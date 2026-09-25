//! Editing a runtime's config file with the smallest textual change.
//!
//! Every edit is made on the parsed tree first. The file text is then
//! spliced at the edited setting only, so comments and formatting elsewhere
//! stay as the owner wrote them. Each splice is checked by parsing the result
//! and comparing it with the edited tree; when a splice can't be placed (or
//! doesn't reproduce the tree) the enclosing setting is re-rendered instead,
//! up to the whole document. The written file therefore always means exactly
//! the edited tree.

pub(crate) mod json5;
pub(crate) mod yaml;

use serde::{Deserialize, Serialize};

/// A parsed config value that can be navigated by string keys.
pub(crate) trait Tree: Clone + PartialEq {
    fn empty_map() -> Self;
    fn is_map(&self) -> bool;
    fn get(&self, key: &str) -> Option<&Self>;
    fn get_mut(&mut self, key: &str) -> Option<&mut Self>;
    /// Inserts or replaces `key`; `self` is a map.
    fn insert(&mut self, key: &str, value: Self);
    /// Removes `key`, keeping the order of the remaining keys.
    fn remove(&mut self, key: &str);
}

/// A config file format.
pub(crate) trait Format {
    type Value: Tree;
    /// The text a config file that doesn't exist yet starts from.
    const EMPTY: &'static str;
    /// Parses a document whose root is a map (an empty document is an empty map).
    fn parse(text: &str) -> Result<Self::Value, String>;
    /// Renders a whole document.
    fn render(root: &Self::Value) -> String;
    /// `text` with the setting at `path` replaced by its value in `root`
    /// (inserted, or removed when `root` has none), or `None` when the
    /// setting can't be located textually.
    fn splice(text: &str, root: &Self::Value, path: &[String]) -> Option<String>;
    /// A single value as text, for the journal.
    fn to_text(value: &Self::Value) -> String;
    fn from_text(text: &str) -> Result<Self::Value, String>;
    fn from_json(value: serde_json::Value) -> Self::Value;
}

/// A desired setting: `value` at `path`, or the setting absent.
#[derive(Debug, Clone)]
pub(crate) struct Edit {
    pub path: Vec<String>,
    pub value: Option<serde_json::Value>,
}

impl Edit {
    pub fn set(path: &[&str], value: serde_json::Value) -> Self {
        Self {
            path: path.iter().map(|key| key.to_string()).collect(),
            value: Some(value),
        }
    }
}

/// One journaled edit: the value at `path` before and after, as text in the
/// file's own format (`None` = the setting was absent).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Record {
    pub path: Vec<String>,
    pub prior: Option<String>,
    pub applied: Option<String>,
}

pub(crate) fn get_path<'a, T: Tree>(root: &'a T, path: &[String]) -> Option<&'a T> {
    path.iter().try_fold(root, |node, key| node.get(key))
}

/// Applies `edits` to `text`, returning the new text and a record of every
/// setting that actually changed.
pub(crate) fn apply<F: Format>(
    text: &str,
    edits: &[Edit],
) -> Result<(String, Vec<Record>), String> {
    let mut root = F::parse(text)?;
    let mut text = text.to_owned();
    let mut records = Vec::new();
    for edit in edits {
        let value = edit.value.clone().map(F::from_json);
        if let Some((path, prior, applied)) = set_path(&mut root, &edit.path, value) {
            text = splice::<F>(&text, &root, &path);
            records.push(Record {
                path,
                prior: prior.as_ref().map(F::to_text),
                applied: applied.as_ref().map(F::to_text),
            });
        }
    }
    Ok((text, records))
}

/// Restores the prior value of each record, newest first. A setting whose
/// current value is no longer the one the record applied was changed since;
/// it is left alone and reported by dotted path.
pub(crate) fn revert<F: Format>(
    text: &str,
    records: &[Record],
) -> Result<(String, Vec<String>), String> {
    let mut root = F::parse(text)?;
    let mut text = text.to_owned();
    let mut conflicts = Vec::new();
    for record in records.iter().rev() {
        let applied = record.applied.as_deref().map(F::from_text).transpose()?;
        if get_path(&root, &record.path) != applied.as_ref() {
            conflicts.push(record.path.join("."));
            continue;
        }
        let prior = record.prior.as_deref().map(F::from_text).transpose()?;
        if let Some((path, _, _)) = set_path(&mut root, &record.path, prior) {
            text = splice::<F>(&text, &root, &path);
        }
    }
    Ok((text, conflicts))
}

/// Whether applying `edits` to `text` would change nothing.
pub(crate) fn is_applied<F: Format>(text: &str, edits: &[Edit]) -> Result<bool, String> {
    let mut root = F::parse(text)?;
    Ok(edits.iter().all(|edit| {
        set_path(&mut root, &edit.path, edit.value.clone().map(F::from_json)).is_none()
    }))
}

/// What happened at one path: the path actually written (an ancestor of the
/// requested one when that ancestor was missing or not a map), and the value
/// there before and after.
type Written<T> = (Vec<String>, Option<T>, Option<T>);

/// Sets (or removes) the value at `path`. Returns `None` when nothing
/// changed. Missing ancestors are created, and an ancestor that isn't a map is
/// replaced; the write is then recorded at that ancestor so a revert puts the
/// original back whole.
fn set_path<T: Tree>(root: &mut T, path: &[String], value: Option<T>) -> Option<Written<T>> {
    let (last, parents) = path.split_last()?;
    let mut node = root;
    for (depth, key) in parents.iter().enumerate() {
        let child_is_map = node.get(key).map(Tree::is_map);
        if child_is_map == Some(true) {
            node = node.get_mut(key)?;
            continue;
        }
        let nested = nest(value?, &path[depth + 1..]);
        let prior = node.get(key).cloned();
        node.insert(key, nested.clone());
        return Some((path[..=depth].to_vec(), prior, Some(nested)));
    }
    let prior = node.get(last).cloned();
    if prior == value {
        return None;
    }
    match &value {
        Some(value) => node.insert(last, value.clone()),
        None => node.remove(last),
    }
    Some((path.to_vec(), prior, value))
}

fn nest<T: Tree>(value: T, path: &[String]) -> T {
    path.iter().rev().fold(value, |inner, key| {
        let mut map = T::empty_map();
        map.insert(key, inner);
        map
    })
}

/// Splices the setting at `path`, climbing to an enclosing setting when the
/// splice can't be placed or doesn't reproduce `root` exactly.
fn splice<F: Format>(text: &str, root: &F::Value, path: &[String]) -> String {
    (1..=path.len())
        .rev()
        .find_map(|depth| {
            F::splice(text, root, &path[..depth])
                .filter(|out| F::parse(out).is_ok_and(|parsed| &parsed == root))
        })
        .unwrap_or_else(|| F::render(root))
}
