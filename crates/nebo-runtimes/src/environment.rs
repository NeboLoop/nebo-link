use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The process environment detection reads: the user's home directory and
/// environment variables. [`Environment::current`] captures the real one;
/// tests and embedders build their own to point detection somewhere else.
#[derive(Debug, Clone, Default)]
pub struct Environment {
    /// The OS home directory of the user whose runtimes are detected.
    pub home: Option<PathBuf>,
    /// Environment variables, e.g. `OPENCLAW_STATE_DIR` or `HERMES_HOME`.
    pub vars: BTreeMap<String, String>,
    /// Folders programs are installed in for every user, looked in after
    /// `PATH` and the user's own: Homebrew's.
    pub system_dirs: Vec<PathBuf>,
}

/// Homebrew's program folders: Apple silicon, Intel, Linux.
const HOMEBREW: [&str; 3] = ["/opt/homebrew/bin", "/usr/local/bin", "/home/linuxbrew/.linuxbrew/bin"];

impl Environment {
    /// The current process's home directory and environment variables.
    pub fn current() -> Self {
        Self {
            home: dirs::home_dir(),
            vars: std::env::vars().collect(),
            system_dirs: HOMEBREW.iter().map(PathBuf::from).collect(),
        }
    }

    /// A variable's value with surrounding whitespace removed; `None` when it
    /// is unset or blank (the runtimes treat blank overrides as unset).
    pub(crate) fn var(&self, name: &str) -> Option<&str> {
        self.vars
            .get(name)
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
    }
}

/// Expands a leading `~` against `home`, the way both runtimes treat their
/// path overrides.
pub(crate) fn expand_tilde(value: &str, home: &Path) -> PathBuf {
    match value.strip_prefix('~') {
        Some("") => home.to_path_buf(),
        Some(rest) if rest.starts_with('/') || rest.starts_with('\\') => home.join(&rest[1..]),
        _ => PathBuf::from(value),
    }
}
