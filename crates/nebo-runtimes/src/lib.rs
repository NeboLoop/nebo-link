//! Local agent runtimes Nebo Link can connect: where they live, which ports
//! they serve, and the config changes the link makes to them (recorded so
//! every change can be undone exactly).
//!
//! The crate is a plain library with no I/O beyond the runtimes' own files and
//! the journal file the caller names. It never starts, stops or signals a
//! runtime: when a change needs a restart it returns the runtime's own restart
//! command as data ([`RuntimeCommand`]) for the caller to run.
//!
//! ```no_run
//! use nebo_runtimes::{detect, Change, Environment, Journal, NeboaiModels, Model};
//!
//! let installs = detect(&Environment::current());
//! let mut journal = Journal::open("/path/to/link/journal.json")?;
//! for install in &installs {
//!     let change = Change::NeboaiModels(NeboaiModels {
//!         base_url: "http://127.0.0.1:18800/v1".into(),
//!         api_key: "local-link-key".into(),
//!         models: vec![Model { id: "neboai-default".into(), name: "NeboAI".into() }],
//!         default_model: "neboai-default".into(),
//!     });
//!     let outcome = journal.apply(install, None, &change)?;
//!     if let Some(command) = outcome.restart {
//!         // Run `command` with std::process::Command.
//!         let _ = command;
//!     }
//! }
//! # Ok::<(), nebo_runtimes::Error>(())
//! ```

mod change;
mod detect;
mod doc;
mod environment;
mod error;
pub mod hermes;
mod journal;
mod openclaw;

pub use change::{
    ApiServer, Change, ChangeKind, Model, NeboaiModels, Outcome, PathMode, ProxyAccess,
    ProxyRoute,
};
pub use detect::{Endpoint, Installation, Profile, RuntimeCommand, Service, detect};
pub use environment::Environment;
pub use error::Error;
pub use journal::{AppliedChange, Journal};

/// The runtimes Nebo Link supports. The string form is what the hub stores
/// in `bots.runtime`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Runtime {
    Openclaw,
    Hermes,
}
