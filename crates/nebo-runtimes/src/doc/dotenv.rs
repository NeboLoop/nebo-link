//! Dotenv files (a Hermes profile's `.env`).
//!
//! A flat `KEY=value` document parsed the way Hermes reads it
//! (`hermes_cli/env_loader.py` `_load_dotenv_with_fallback`, the python-dotenv
//! parser): blank lines and `#` comments are skipped, an `export ` prefix is
//! allowed, and a value in matching single or double quotes is unquoted. When
//! a key is assigned more than once the last assignment wins. Splicing
//! replaces that one line (or appends or removes it) and keeps every other
//! byte, comments included.

use serde_json::{Map, Value};

use super::Format;

pub(crate) struct Dotenv;

impl Format for Dotenv {
    type Value = Value;
    const EMPTY: &'static str = "";

    fn parse(text: &str) -> Result<Value, String> {
        let mut map = Map::new();
        for line in text.lines() {
            if let Some((key, value)) = assignment(line) {
                map.insert(key.to_owned(), Value::String(value));
            }
        }
        Ok(Value::Object(map))
    }

    fn render(root: &Value) -> String {
        root.as_object()
            .into_iter()
            .flatten()
            .map(|(key, value)| format!("{}\n", render_line(key, value, "")))
            .collect()
    }

    fn splice(text: &str, root: &Value, path: &[String]) -> Option<String> {
        let [key] = path else {
            return None;
        };
        let lines: Vec<&str> = text.split_inclusive('\n').collect();
        let found = lines
            .iter()
            .rposition(|line| assignment(line).is_some_and(|(k, _)| k == key));
        let value = root.get(key);
        let mut out = String::with_capacity(text.len() + 64);
        match (found, value) {
            (Some(index), Some(value)) => {
                let prefix = if lines[index].trim_start().starts_with("export ") {
                    "export "
                } else {
                    ""
                };
                let newline = if lines[index].ends_with('\n') { "\n" } else { "" };
                for (i, line) in lines.iter().enumerate() {
                    if i == index {
                        out.push_str(&render_line(key, value, prefix));
                        out.push_str(newline);
                    } else {
                        out.push_str(line);
                    }
                }
            }
            (Some(index), None) => {
                for (i, line) in lines.iter().enumerate() {
                    if i != index {
                        out.push_str(line);
                    }
                }
            }
            (None, Some(value)) => {
                out.push_str(text);
                if !text.is_empty() && !text.ends_with('\n') {
                    out.push('\n');
                }
                out.push_str(&render_line(key, value, ""));
                out.push('\n');
            }
            (None, None) => out.push_str(text),
        }
        Some(out)
    }

    fn to_text(value: &Value) -> String {
        match value {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        }
    }

    fn from_text(text: &str) -> Result<Value, String> {
        Ok(Value::String(text.to_owned()))
    }

    fn from_json(value: Value) -> Value {
        match value {
            Value::String(_) => value,
            Value::Number(n) => Value::String(n.to_string()),
            Value::Bool(b) => Value::String(b.to_string()),
            other => Value::String(other.to_string()),
        }
    }
}

/// The `(key, value)` of an assignment line; `None` for blank lines,
/// comments and lines without `=`.
fn assignment(line: &str) -> Option<(&str, String)> {
    let line = line.trim();
    let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
    if line.starts_with('#') {
        return None;
    }
    let (key, value) = line.split_once('=')?;
    let key = key.trim();
    if key.is_empty() || key.contains(char::is_whitespace) {
        return None;
    }
    let value = value.trim();
    let value = match value.chars().next() {
        Some(quote @ ('"' | '\'')) => match value[1..].strip_suffix(quote) {
            Some(inner) if quote == '"' => unescape(inner),
            Some(inner) => inner.to_owned(),
            None => value.to_owned(),
        },
        _ => value.to_owned(),
    };
    Some((key, value))
}

fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match (c, chars.clone().next()) {
            ('\\', Some(next @ ('"' | '\\'))) => {
                out.push(next);
                chars.next();
            }
            _ => out.push(c),
        }
    }
    out
}

/// `KEY=value`, double-quoted when the value would not survive a bare
/// assignment.
fn render_line(key: &str, value: &Value, prefix: &str) -> String {
    let text = Dotenv::to_text(value);
    let bare = !text.is_empty()
        && text
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./:@+,".contains(c));
    if bare {
        format!("{prefix}{key}={text}")
    } else {
        let escaped = text.replace('\\', "\\\\").replace('"', "\\\"");
        format!("{prefix}{key}=\"{escaped}\"")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const FILE: &str = "# keys\nexport OPENAI_API_KEY='sk-1'\nAPI_SERVER_PORT=8650\n\nTOKEN=\"a b\"\n";

    #[test]
    fn parses_like_hermes() {
        let root = Dotenv::parse(FILE).unwrap();
        assert_eq!(root["OPENAI_API_KEY"], "sk-1");
        assert_eq!(root["API_SERVER_PORT"], "8650");
        assert_eq!(root["TOKEN"], "a b");
        assert_eq!(Dotenv::parse("A=1\nA=2\n").unwrap()["A"], "2");
        assert_eq!(Dotenv::parse("").unwrap(), json!({}));
    }

    #[test]
    fn splices_one_line_and_keeps_the_rest() {
        let mut root = Dotenv::parse(FILE).unwrap();
        root["API_SERVER_KEY"] = json!("0123456789abcdef0123");
        let out = Dotenv::splice(FILE, &root, &["API_SERVER_KEY".to_owned()]).unwrap();
        assert_eq!(out, format!("{FILE}API_SERVER_KEY=0123456789abcdef0123\n"));
        assert_eq!(Dotenv::parse(&out).unwrap(), root);

        root["OPENAI_API_KEY"] = json!("sk-2");
        let out = Dotenv::splice(&out, &root, &["OPENAI_API_KEY".to_owned()]).unwrap();
        assert!(out.contains("export OPENAI_API_KEY=sk-2\n"));
        assert!(out.starts_with("# keys\n"));

        root.as_object_mut().unwrap().remove("TOKEN");
        let out = Dotenv::splice(&out, &root, &["TOKEN".to_owned()]).unwrap();
        assert!(!out.contains("TOKEN"));
        assert_eq!(Dotenv::parse(&out).unwrap(), root);
    }

    #[test]
    fn quotes_when_needed_and_appends_a_newline() {
        let mut root = json!({});
        root["A"] = json!("x y\"z");
        let out = Dotenv::splice("B=1", &root, &["A".to_owned()]).unwrap();
        assert_eq!(out, "B=1\nA=\"x y\\\"z\"\n");
        assert_eq!(Dotenv::parse(&out).unwrap()["A"], "x y\"z");
        assert_eq!(Dotenv::render(&root), "A=\"x y\\\"z\"\n");
    }
}
