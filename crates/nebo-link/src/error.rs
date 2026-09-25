use std::path::PathBuf;

/// Everything the link reports to the person running it. Each message says
/// what failed and, where there is one, what to do.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("could not access {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("{path} is not valid: {message}")]
    Parse { path: PathBuf, message: String },
    #[error(transparent)]
    Runtime(#[from] nebo_runtimes::Error),
    #[error("credential store: {0}")]
    Credentials(String),
    #[error("service: {0}")]
    Service(String),
    /// A plain explanation for the person at the keyboard.
    #[error("{0}")]
    Message(String),
}

impl Error {
    pub fn io(path: impl Into<PathBuf>, source: std::io::Error) -> Self {
        Self::Io {
            path: path.into(),
            source,
        }
    }
}

pub type Result<T> = std::result::Result<T, Error>;
