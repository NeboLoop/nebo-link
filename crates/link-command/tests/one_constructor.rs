//! Every child process in the workspace is made by `command::new`
//! (`crates/link-command`), which sets the Windows creation flags from the
//! `Console` its caller names.
//!
//! A raw `Command::new` starts a console program with no flags, and from a
//! process with no console — an app embedding link-core, the link run as a
//! background task — Windows gives it a window of its own: one terminal per
//! agent, adapter, `tasklist` or `taskkill`. `creation_flags` outside the
//! constructor is a second place deciding the same thing. A code review
//! cannot catch the next site; a failing build can. Test code (`tests/`
//! directories) may start what it likes.

use std::path::{Path, PathBuf};

const CONSTRUCTOR: &str = "crates/link-command/src/lib.rs";

fn repo_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates/
    p.pop(); // repo root
    p
}

/// Every `.rs` file under `crates/` outside a `tests/` directory.
fn source_files() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
                if name != "target" && name != "tests" {
                    walk(&path, out);
                }
            } else if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&repo_root().join("crates"), &mut out);
    out
}

/// `needle` where it starts a path segment: `Command::new(` but not
/// `CommandBuilder::new(` or a `FooCommand::new(`.
fn starts_segment(line: &str, needle: &str) -> bool {
    line.match_indices(needle)
        .any(|(i, _)| !line[..i].chars().next_back().is_some_and(|c| c.is_alphanumeric() || c == '_'))
}

#[test]
fn child_processes_are_made_by_the_one_constructor() {
    let files = source_files();
    assert!(
        files.iter().any(|f| f.to_string_lossy().replace('\\', "/").ends_with(CONSTRUCTOR)),
        "the walk found the constructor itself"
    );
    let mut offenders = Vec::new();
    for file in files {
        let path = file.to_string_lossy().replace('\\', "/");
        if path.ends_with(CONSTRUCTOR) {
            continue;
        }
        let Ok(text) = std::fs::read_to_string(&file) else {
            continue;
        };
        for (n, line) in text.lines().enumerate() {
            if line.trim_start().starts_with("//") {
                continue;
            }
            if starts_segment(line, "Command::new(") || line.contains("creation_flags(") {
                offenders.push(format!("{path}:{}: {}", n + 1, line.trim()));
            }
        }
    }
    assert!(
        offenders.is_empty(),
        "Start a child with `command::new(program, command::Console::Hidden)` (or \
         `Console::Inherit`, see crates/link-command), never `Command::new` or \
         `creation_flags` — on Windows a child made any other way opens a console window.\n{}",
        offenders.join("\n")
    );
}
