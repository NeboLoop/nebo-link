//! YAML documents (Hermes `config.yaml`).
//!
//! Values are parsed with `yaml_serde`. For splicing, block mappings are
//! located by indentation: a member is its `key:` line plus every following
//! line indented deeper. A member can be replaced, inserted after its last
//! sibling, or removed; everything else in the file is kept byte for byte.
//! Flow-style mappings and other shapes the line scanner doesn't follow are
//! re-rendered at the nearest enclosing member instead (see [`super`]).

use yaml_serde::{Mapping, Value};

use super::{Format, Tree, get_path};

pub(crate) struct Yaml;

fn key_value(key: &str) -> Value {
    Value::String(key.to_owned())
}

impl Tree for Value {
    fn empty_map() -> Self {
        Value::Mapping(Mapping::new())
    }

    fn is_map(&self) -> bool {
        self.is_mapping()
    }

    fn get(&self, key: &str) -> Option<&Self> {
        self.as_mapping()?.get(key_value(key))
    }

    fn get_mut(&mut self, key: &str) -> Option<&mut Self> {
        self.as_mapping_mut()?.get_mut(key_value(key))
    }

    fn insert(&mut self, key: &str, value: Self) {
        if let Some(map) = self.as_mapping_mut() {
            map.insert(key_value(key), value);
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some(map) = self.as_mapping_mut() {
            map.shift_remove(key_value(key));
        }
    }
}

impl Format for Yaml {
    type Value = Value;
    const EMPTY: &'static str = "";

    fn parse(text: &str) -> Result<Value, String> {
        match yaml_serde::from_str(text).map_err(|error| error.to_string())? {
            Value::Null => Ok(Value::empty_map()),
            value @ Value::Mapping(_) => Ok(value),
            _ => Err("the top level is not a mapping".to_owned()),
        }
    }

    fn render(root: &Value) -> String {
        yaml_serde::to_string(root).unwrap_or_default()
    }

    fn splice(text: &str, root: &Value, path: &[String]) -> Option<String> {
        let (key, parents) = path.split_last()?;
        let lines = Line::split(text);
        if lines
            .iter()
            .any(|line| line.text.starts_with("---") || line.text.starts_with("..."))
        {
            return None;
        }
        let mut block = 0..lines.len();
        for parent in parents {
            let member = members(&lines, block)?
                .into_iter()
                .rev()
                .find(|m| &m.key == parent)?;
            if member.inline {
                return None;
            }
            block = member.line + 1..member.end;
        }
        let siblings = members(&lines, block.clone());
        let found = siblings
            .as_ref()
            .and_then(|s| s.iter().rev().find(|m| &m.key == key));
        let span = |member: &Member| lines[member.line].start..lines[member.end - 1].end;
        match (found, get_path(root, path)) {
            (Some(member), Some(value)) => Some(replace(
                text,
                span(member),
                &render_member(key, value, member.indent),
            )),
            (Some(member), None) => Some(replace(text, span(member), "")),
            (None, Some(value)) => {
                let (indent, at) = match siblings?.last() {
                    Some(last) => (last.indent, lines[last.end - 1].end),
                    // An empty root gets its first member; an empty nested
                    // mapping is re-rendered by the enclosing member.
                    None if parents.is_empty() => (0, text.len()),
                    None => return None,
                };
                let newline = if text[..at].is_empty() || text[..at].ends_with('\n') {
                    ""
                } else {
                    "\n"
                };
                Some(replace(
                    text,
                    at..at,
                    &format!("{newline}{}", render_member(key, value, indent)),
                ))
            }
            (None, None) => Some(text.to_owned()),
        }
    }

    fn to_text(value: &Value) -> String {
        yaml_serde::to_string(value).unwrap_or_default()
    }

    fn from_text(text: &str) -> Result<Value, String> {
        yaml_serde::from_str(text).map_err(|error| error.to_string())
    }

    fn from_json(value: serde_json::Value) -> Value {
        yaml_serde::to_value(value).unwrap_or(Value::Null)
    }
}

fn replace(text: &str, range: std::ops::Range<usize>, with: &str) -> String {
    format!("{}{with}{}", &text[..range.start], &text[range.end..])
}

/// `key: value` as block YAML, every line indented by `indent` spaces.
fn render_member(key: &str, value: &Value, indent: usize) -> String {
    let mut map = Mapping::new();
    map.insert(key_value(key), value.clone());
    let rendered = yaml_serde::to_string(&map).unwrap_or_default();
    let pad = " ".repeat(indent);
    rendered
        .lines()
        .map(|line| format!("{pad}{line}\n"))
        .collect()
}

struct Line<'a> {
    start: usize,
    /// Just past the line break.
    end: usize,
    /// Without the line break.
    text: &'a str,
}

impl<'a> Line<'a> {
    fn split(text: &'a str) -> Vec<Line<'a>> {
        let mut lines = Vec::new();
        let mut start = 0;
        for piece in text.split_inclusive('\n') {
            lines.push(Line {
                start,
                end: start + piece.len(),
                text: piece.trim_end_matches(['\n', '\r']),
            });
            start += piece.len();
        }
        lines
    }

    fn indent(&self) -> usize {
        self.text.len() - self.text.trim_start_matches(' ').len()
    }

    fn is_blank(&self) -> bool {
        self.text.trim().is_empty()
    }

    fn is_comment(&self) -> bool {
        self.text.trim_start().starts_with('#')
    }
}

/// A member of a block mapping: lines `line..end`.
struct Member {
    key: String,
    indent: usize,
    line: usize,
    end: usize,
    /// The value is written on the key's line (a scalar or flow collection).
    inline: bool,
}

/// The members of the block mapping made of `block`'s lines, or `None` when
/// those lines aren't a plain block mapping.
fn members(lines: &[Line], block: std::ops::Range<usize>) -> Option<Vec<Member>> {
    let content = |i: &usize| !lines[*i].is_blank() && !lines[*i].is_comment();
    let Some(first) = block.clone().find(content) else {
        return Some(Vec::new());
    };
    let indent = lines[first].indent();
    let mut members = Vec::new();
    for i in block.clone().filter(content) {
        match lines[i].indent() {
            n if n == indent => {
                let (key, inline) = parse_key(&lines[i].text[indent..])?;
                members.push(Member {
                    key,
                    indent,
                    line: i,
                    end: i + 1,
                    inline,
                });
            }
            n if n < indent => return None,
            _ => {}
        }
    }
    // A member runs until the next non-blank line at or left of its indent
    // (comments there belong to what follows), minus trailing blank lines.
    for member in &mut members {
        let mut end = (member.line + 1..block.end)
            .find(|&i| !lines[i].is_blank() && lines[i].indent() <= indent)
            .unwrap_or(block.end);
        while end > member.line + 1 && lines[end - 1].is_blank() {
            end -= 1;
        }
        member.end = end;
    }
    Some(members)
}

/// The key of a `key: value` line and whether a value follows on the line.
fn parse_key(line: &str) -> Option<(String, bool)> {
    let (key, rest) = match line.chars().next()? {
        '"' => {
            let close = line[1..].find('"')? + 1;
            let key: String = serde_json::from_str(&line[..=close]).ok()?;
            (key, line[close + 1..].strip_prefix(':')?)
        }
        '\'' => {
            let close = line[1..].find('\'')? + 1;
            (
                line[1..close].to_owned(),
                line[close + 1..].strip_prefix(':')?,
            )
        }
        '-' | '?' | '{' | '[' | '#' | '&' | '*' | '!' | '|' | '>' => return None,
        _ => {
            let colon = line
                .match_indices(':')
                .map(|(i, _)| i)
                .find(|&i| line[i + 1..].is_empty() || line[i + 1..].starts_with([' ', '\t']))?;
            (line[..colon].trim_end().to_owned(), &line[colon + 1..])
        }
    };
    let value = rest.trim();
    Some((key, !value.is_empty() && !value.starts_with('#')))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::doc::{Edit, apply, revert};

    const CONFIG: &str = "\
# Hermes config
model:
  # Default model to use
  default: \"anthropic/claude-opus-4.6\"
  provider: \"auto\"
  base_url: \"https://openrouter.ai/api/v1\"

# Named providers
providers:
  local:
    base_url: http://127.0.0.1:1234/v1

platforms:
  api_server:
    enabled: true
    extra:
      port: 8650
channel_overrides:
  123: quiet
";

    #[test]
    fn replace_member_keeps_the_rest() {
        let edits = [Edit::set(
            &["model"],
            json!({ "default": "x", "provider": "neboai" }),
        )];
        let (out, records) = apply::<Yaml>(CONFIG, &edits).unwrap();
        assert!(
            out.starts_with(
                "# Hermes config\nmodel:\n  default: x\n  provider: neboai\n\n# Named providers\n"
            ),
            "{out}"
        );
        assert!(out.ends_with(&CONFIG[CONFIG.find("# Named providers").unwrap()..]));
        let (back, conflicts) = revert::<Yaml>(&out, &records).unwrap();
        assert!(conflicts.is_empty());
        assert_eq!(Yaml::parse(&back).unwrap(), Yaml::parse(CONFIG).unwrap());
    }

    #[test]
    fn nested_insert_and_remove_is_exact() {
        let edits = [Edit::set(
            &["providers", "neboai"],
            json!({ "base_url": "http://127.0.0.1:9/v1" }),
        )];
        let (out, records) = apply::<Yaml>(CONFIG, &edits).unwrap();
        assert!(out.contains("  local:\n    base_url: http://127.0.0.1:1234/v1\n  neboai:\n    base_url: http://127.0.0.1:9/v1\n"), "{out}");
        let (back, _) = revert::<Yaml>(&out, &records).unwrap();
        assert_eq!(back, CONFIG);
    }

    #[test]
    fn integer_keys_elsewhere_are_untouched() {
        let edits = [Edit::set(&["model", "default"], json!("y"))];
        let (out, _) = apply::<Yaml>(CONFIG, &edits).unwrap();
        assert!(out.contains("channel_overrides:\n  123: quiet\n"));
    }

    #[test]
    fn flow_and_empty_documents() {
        for text in [
            "",
            "providers: {}\n",
            "model: gpt\nproviders: {a: {base_url: x}}\n",
        ] {
            let edits = [
                Edit::set(&["providers", "neboai", "base_url"], json!("u")),
                Edit::set(&["model", "provider"], json!("neboai")),
            ];
            let (out, records) = apply::<Yaml>(text, &edits).unwrap();
            let parsed = Yaml::parse(&out).unwrap();
            assert_eq!(
                get_path(
                    &parsed,
                    &["providers".into(), "neboai".into(), "base_url".into()]
                ),
                Some(&key_value("u"))
            );
            let (back, conflicts) = revert::<Yaml>(&out, &records).unwrap();
            assert!(conflicts.is_empty());
            assert_eq!(
                Yaml::parse(&back).unwrap(),
                Yaml::parse(text).unwrap(),
                "{text:?} -> {back:?}"
            );
        }
    }
}
