use std::path::PathBuf;

/// Everything that can go wrong reading or changing a runtime's config.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A file could not be read or written.
    #[error("could not access {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    /// A config or journal file is not valid for its format.
    #[error("could not parse {path}: {message}")]
    Parse { path: PathBuf, message: String },
    /// An OpenClaw config object the change would edit is assembled with
    /// `$include`; the link never edits across an include boundary.
    #[error("{path} includes another file at `{key}`; edit the included file instead")]
    Include { path: PathBuf, key: String },
    /// The profile named in a change does not exist in the installation.
    #[error("no profile named `{0}` in this installation")]
    UnknownProfile(String),
    /// The change's parameters are not usable as given.
    #[error("invalid change: {0}")]
    InvalidChange(String),
}
