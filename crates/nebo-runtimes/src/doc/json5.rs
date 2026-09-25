//! JSON5 documents (OpenClaw's `openclaw.json`).
//!
//! Values are parsed with the `json5` crate. For splicing, a small scanner
//! records where each object member sits in the text (comments, strings and
//! trailing commas included), so one member's value can be replaced, a member
//! inserted or removed, and every other byte of the file kept.

use serde_json::{Map, Value};

use super::{Format, Tree, get_path};

pub(crate) struct Json5;

impl Tree for Value {
    fn empty_map() -> Self {
        Value::Object(Map::new())
    }

    fn is_map(&self) -> bool {
        self.is_object()
    }

    fn get(&self, key: &str) -> Option<&Self> {
        self.as_object()?.get(key)
    }

    fn get_mut(&mut self, key: &str) -> Option<&mut Self> {
        self.as_object_mut()?.get_mut(key)
    }

    fn insert(&mut self, key: &str, value: Self) {
        if let Some(map) = self.as_object_mut() {
            map.insert(key.to_owned(), value);
        }
    }

    fn remove(&mut self, key: &str) {
        if let Some(map) = self.as_object_mut() {
            map.shift_remove(key);
        }
    }
}

impl Format for Json5 {
    type Value = Value;
    const EMPTY: &'static str = "{}\n";

    fn parse(text: &str) -> Result<Value, String> {
        let value: Value = json5::from_str(text).map_err(|error| error.to_string())?;
        if value.is_object() {
            Ok(value)
        } else {
            Err("the top level is not an object".to_owned())
        }
    }

    /// Two-space JSON with a trailing newline: what OpenClaw itself writes.
    fn render(root: &Value) -> String {
        format!("{}\n", render(root, ""))
    }

    fn splice(text: &str, root: &Value, path: &[String]) -> Option<String> {
        let (key, parents) = path.split_last()?;
        let top = scan(text).ok()?;
        let parent = parents.iter().try_fold(&top, |object, key| {
            object
                .members
                .iter()
                .rev()
                .find(|m| &m.key == key)?
                .object
                .as_ref()
        })?;
        let index = parent.members.iter().rposition(|m| &m.key == key);
        match (index, get_path(root, path)) {
            (Some(index), Some(value)) => {
                let member = &parent.members[index];
                let indent = line_indent(text, member.key_start);
                Some(replace(
                    text,
                    member.value_start..member.value_end,
                    &render(value, indent),
                ))
            }
            (Some(index), None) => Some(remove(text, parent, index)),
            (None, Some(value)) => Some(insert(text, parent, key, value)),
            (None, None) => Some(text.to_owned()),
        }
    }

    fn to_text(value: &Value) -> String {
        value.to_string()
    }

    fn from_text(text: &str) -> Result<Value, String> {
        serde_json::from_str(text).map_err(|error| error.to_string())
    }

    fn from_json(value: Value) -> Value {
        value
    }
}

/// An object in the text: the offsets of its braces and its members.
struct Object {
    open: usize,
    close: usize,
    members: Vec<Member>,
}

struct Member {
    key: String,
    key_start: usize,
    value_start: usize,
    value_end: usize,
    /// The offset just past this member's comma, when it has one.
    comma_end: Option<usize>,
    /// The member's value, when it is an object.
    object: Option<Object>,
}

fn replace(text: &str, range: std::ops::Range<usize>, with: &str) -> String {
    format!("{}{with}{}", &text[..range.start], &text[range.end..])
}

/// Pretty JSON whose continuation lines are indented to sit under a member
/// at `indent`.
fn render(value: &Value, indent: &str) -> String {
    let pretty = serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string());
    pretty.replace('\n', &format!("\n{indent}"))
}

/// The leading whitespace of the line containing `offset`.
fn line_indent(text: &str, offset: usize) -> &str {
    let start = text[..offset].rfind('\n').map_or(0, |i| i + 1);
    let line = &text[start..];
    &line[..line.len() - line.trim_start_matches([' ', '\t']).len()]
}

fn insert(text: &str, parent: &Object, key: &str, value: &Value) -> String {
    let outer = line_indent(text, parent.open);
    let indent = match parent.members.first() {
        Some(first) => line_indent(text, first.key_start).to_owned(),
        None => format!("{outer}  "),
    };
    let key = serde_json::to_string(key).unwrap_or_default();
    let entry = format!("\n{indent}{key}: {}", render(value, &indent));
    let Some(last) = parent.members.last() else {
        let inside = parent.open + 1..parent.close;
        return if text[inside.clone()].trim().is_empty() {
            replace(text, inside, &format!("{entry}\n{outer}"))
        } else {
            replace(text, inside.start..inside.start, &format!("{entry},"))
        };
    };
    // Place the member on its own line just before the closing brace, and
    // keep the object's trailing-comma style.
    let before_close = text[..parent.close].trim_end_matches([' ', '\t']);
    let at = match before_close.strip_suffix('\n') {
        Some(rest) => rest.strip_suffix('\r').unwrap_or(rest).len(),
        None => parent.close,
    };
    match last.comma_end {
        Some(_) => replace(text, at..at, &format!("{entry},")),
        None => format!(
            "{},{}{entry}{}",
            &text[..last.value_end],
            &text[last.value_end..at],
            &text[at..]
        ),
    }
}

fn remove(text: &str, parent: &Object, index: usize) -> String {
    let member = &parent.members[index];
    // A member on a line of its own goes with its whole line, so comments on
    // the neighbouring lines stay put. JSON5 allows the trailing comma this
    // can leave on the previous member.
    let line_start = text[..member.key_start].rfind('\n').map_or(0, |i| i + 1);
    let end = member.comma_end.unwrap_or(member.value_end);
    let line_end = text[end..].find('\n').map_or(text.len(), |i| end + i + 1);
    if text[line_start..member.key_start].trim().is_empty() && text[end..line_end].trim().is_empty()
    {
        return replace(text, line_start..line_end, "");
    }
    let range = match (
        index.checked_sub(1).map(|i| &parent.members[i]),
        member.comma_end,
    ) {
        // `prev, member,` → `prev,`
        (Some(prev), Some(comma_end)) => prev.comma_end.unwrap_or(prev.value_end)..comma_end,
        // `prev, member` (last) → `prev`
        (Some(prev), None) => prev.value_end..member.value_end,
        // `{ member, next` → `{ next`
        (None, Some(_)) if index + 1 < parent.members.len() => {
            member.key_start..parent.members[index + 1].key_start
        }
        // the only member
        (None, _) => parent.open + 1..parent.close,
    };
    replace(text, range, "")
}

fn scan(text: &str) -> Result<Object, String> {
    let mut scanner = Scanner { text, pos: 0 };
    scanner.trivia()?;
    if scanner.peek() != Some('{') {
        return Err("expected an object".to_owned());
    }
    let object = scanner.object()?;
    scanner.trivia()?;
    match scanner.peek() {
        None => Ok(object),
        Some(c) => Err(format!("unexpected `{c}` after the document")),
    }
}

struct Scanner<'a> {
    text: &'a str,
    pos: usize,
}

impl Scanner<'_> {
    fn rest(&self) -> &str {
        &self.text[self.pos..]
    }

    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }

    fn bump(&mut self) -> Result<char, String> {
        let c = self.peek().ok_or("unexpected end of document")?;
        self.pos += c.len_utf8();
        Ok(c)
    }

    fn expect(&mut self, want: char) -> Result<(), String> {
        match self.bump()? {
            c if c == want => Ok(()),
            c => Err(format!("expected `{want}`, found `{c}`")),
        }
    }

    /// Skips whitespace and comments.
    fn trivia(&mut self) -> Result<(), String> {
        loop {
            let rest = self.rest();
            if rest.starts_with("//") {
                self.pos += rest.find('\n').unwrap_or(rest.len());
            } else if let Some(body) = rest.strip_prefix("/*") {
                self.pos += 4 + body.find("*/").ok_or("unterminated comment")?;
            } else if self
                .peek()
                .is_some_and(|c| c.is_whitespace() || c == '\u{feff}')
            {
                self.bump()?;
            } else {
                return Ok(());
            }
        }
    }

    fn at_delimiter(&self) -> bool {
        let rest = self.rest();
        rest.starts_with("//")
            || rest.starts_with("/*")
            || self
                .peek()
                .is_none_or(|c| matches!(c, ',' | '}' | ']' | ':') || c.is_whitespace())
    }

    /// A quoted string, decoded.
    fn string(&mut self) -> Result<String, String> {
        let quote = self.bump()?;
        let mut out = String::new();
        loop {
            match self.bump()? {
                c if c == quote => return Ok(out),
                '\\' => match self.bump()? {
                    'n' => out.push('\n'),
                    'r' => out.push('\r'),
                    't' => out.push('\t'),
                    'b' => out.push('\u{8}'),
                    'f' => out.push('\u{c}'),
                    'v' => out.push('\u{b}'),
                    '0' => out.push('\0'),
                    'x' => out.push(self.hex(2)?),
                    'u' => out.push(self.hex(4)?),
                    // Line continuation.
                    '\r' => {
                        if self.peek() == Some('\n') {
                            self.bump()?;
                        }
                    }
                    '\n' | '\u{2028}' | '\u{2029}' => {}
                    other => out.push(other),
                },
                '\n' | '\r' => return Err("line break inside a string".to_owned()),
                c => out.push(c),
            }
        }
    }

    fn hex(&mut self, digits: usize) -> Result<char, String> {
        let hex = self.rest().get(..digits).ok_or("short escape")?;
        let code = u32::from_str_radix(hex, 16).map_err(|_| "bad escape")?;
        self.pos += digits;
        // Lone surrogates can't be decoded into a key; they don't occur in
        // the keys this crate looks up.
        Ok(char::from_u32(code).unwrap_or('\u{fffd}'))
    }

    fn value(&mut self) -> Result<Option<Object>, String> {
        match self.peek() {
            Some('{') => self.object().map(Some),
            Some('[') => self.array().map(|()| None),
            Some('"' | '\'') => self.string().map(|_| None),
            Some(_) => {
                let start = self.pos;
                while !self.at_delimiter() {
                    self.bump()?;
                }
                if self.pos == start {
                    Err(format!("unexpected `{}`", self.peek().unwrap_or(' ')))
                } else {
                    Ok(None)
                }
            }
            None => Err("unexpected end of document".to_owned()),
        }
    }

    fn array(&mut self) -> Result<(), String> {
        self.expect('[')?;
        loop {
            self.trivia()?;
            if self.peek() == Some(']') {
                return self.expect(']');
            }
            self.value()?;
            self.trivia()?;
            match self.bump()? {
                ',' => {}
                ']' => return Ok(()),
                c => return Err(format!("expected `,` or `]`, found `{c}`")),
            }
        }
    }

    fn object(&mut self) -> Result<Object, String> {
        let open = self.pos;
        self.expect('{')?;
        let mut members = Vec::new();
        loop {
            self.trivia()?;
            if self.peek() == Some('}') {
                let close = self.pos;
                self.bump()?;
                return Ok(Object {
                    open,
                    close,
                    members,
                });
            }
            let key_start = self.pos;
            let key = match self.peek() {
                Some('"' | '\'') => self.string()?,
                _ => {
                    while !self.at_delimiter() {
                        self.bump()?;
                    }
                    self.text[key_start..self.pos].to_owned()
                }
            };
            if key.is_empty() && self.pos == key_start {
                return Err("expected a key".to_owned());
            }
            self.trivia()?;
            self.expect(':')?;
            self.trivia()?;
            let value_start = self.pos;
            let object = self.value()?;
            let value_end = self.pos;
            self.trivia()?;
            let comma_end = if self.peek() == Some(',') {
                self.bump()?;
                Some(self.pos)
            } else {
                None
            };
            members.push(Member {
                key,
                key_start,
                value_start,
                value_end,
                comma_end,
                object,
            });
            if comma_end.is_none() {
                self.trivia()?;
                if self.peek() != Some('}') {
                    return Err("expected `,` or `}`".to_owned());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::doc::{Edit, apply, revert};

    const COMMENTED: &str = r#"// OpenClaw config
{
  // Gateway settings
  gateway: {
    port: 18789, // the default
    bind: 'loopback',
    auth: {
      mode: "token",
      token: "abc",
    },
  },
  /* agents */
  agents: { defaults: { model: "anthropic/claude-opus-4-6" } },
}
"#;

    #[test]
    fn replace_keeps_every_other_byte() {
        let edits = [Edit::set(
            &["gateway", "auth", "mode"],
            json!("trusted-proxy"),
        )];
        let (out, records) = apply::<Json5>(COMMENTED, &edits).unwrap();
        assert_eq!(
            out,
            COMMENTED.replace(r#"mode: "token""#, r#"mode: "trusted-proxy""#)
        );
        assert_eq!(records.len(), 1);
        let (back, conflicts) = revert::<Json5>(&out, &records).unwrap();
        assert!(conflicts.is_empty());
        assert_eq!(
            Json5::parse(&back).unwrap(),
            Json5::parse(COMMENTED).unwrap()
        );
    }

    #[test]
    fn insert_and_remove_round_trip() {
        let edits = [
            Edit::set(&["gateway", "controlUi", "basePath"], json!("/t/bot")),
            Edit::set(&["gateway", "trustedProxies"], json!(["127.0.0.1", "::1"])),
            Edit::set(
                &["models", "providers", "neboai"],
                json!({ "baseUrl": "http://127.0.0.1:1/v1" }),
            ),
        ];
        let (out, records) = apply::<Json5>(COMMENTED, &edits).unwrap();
        assert!(out.contains("// the default"), "comments survive: {out}");
        assert!(out.contains("/* agents */"));
        let parsed = Json5::parse(&out).unwrap();
        assert_eq!(parsed["gateway"]["controlUi"]["basePath"], "/t/bot");
        assert_eq!(
            parsed["models"]["providers"]["neboai"]["baseUrl"],
            "http://127.0.0.1:1/v1"
        );
        let (back, conflicts) = revert::<Json5>(&out, &records).unwrap();
        assert!(conflicts.is_empty());
        assert_eq!(back, COMMENTED);
    }

    #[test]
    fn inserts_into_objects_without_trailing_commas_and_empty_objects() {
        for text in [
            "{\"a\": 1}",
            "{\n  \"a\": 1\n}\n",
            "{}",
            "{\n}\n",
            "{ /* none */ }",
        ] {
            let edits = [Edit::set(&["b", "c"], json!(true))];
            let (out, records) = apply::<Json5>(text, &edits).unwrap();
            let parsed = Json5::parse(&out).unwrap();
            assert_eq!(parsed["b"]["c"], true, "{text} -> {out}");
            let (back, _) = revert::<Json5>(&out, &records).unwrap();
            assert_eq!(
                Json5::parse(&back).unwrap(),
                Json5::parse(text).unwrap(),
                "{text} -> {back}"
            );
        }
    }

    #[test]
    fn replaces_a_scalar_ancestor_whole() {
        let text = "{ agents: { defaults: { model: 'a/b' } } }";
        let edits = [Edit::set(
            &["agents", "defaults", "model", "primary"],
            json!("neboai/x"),
        )];
        let (out, records) = apply::<Json5>(text, &edits).unwrap();
        assert_eq!(records[0].path, ["agents", "defaults", "model"]);
        assert_eq!(records[0].prior.as_deref(), Some("\"a/b\""));
        let (back, _) = revert::<Json5>(&out, &records).unwrap();
        assert_eq!(Json5::parse(&back).unwrap(), Json5::parse(text).unwrap());
        assert!(
            back.contains("model: \"a/b\"") || back.contains("model: 'a/b'"),
            "{back}"
        );
    }

    #[test]
    fn strings_that_look_like_syntax_are_skipped() {
        let text = r#"{ "a": "} // not a comment", 'b': '/* nor this */', c: [1, { d: 2 }], }"#;
        let edits = [Edit::set(&["e"], json!(1))];
        let (out, _) = apply::<Json5>(text, &edits).unwrap();
        let parsed = Json5::parse(&out).unwrap();
        assert_eq!(parsed["a"], "} // not a comment");
        assert_eq!(parsed["e"], 1);
    }
}
