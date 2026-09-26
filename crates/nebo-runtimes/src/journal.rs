//! The undo journal: every change the link makes to a runtime's config, with
//! what it replaced.
//!
//! Both runtimes rewrite their own config files (OpenClaw's writer re-emits
//! the file as plain JSON, Hermes re-dumps its YAML), so a byte snapshot alone
//! can't be trusted to undo a change: restoring it would also undo whatever
//! the owner or the runtime changed since. A revert therefore works in two
//! tiers:
//!
//! 1. **Byte for byte** when the file is still exactly what the link wrote:
//!    the original bytes (comments and all) are written back, or the file is
//!    removed if the link created it.
//! 2. **Semantically** otherwise: each setting the link changed is put back
//!    to its recorded prior value, but only where it still holds the value
//!    the link set. A setting changed since is left as it is and reported in
//!    [`Outcome::conflicts`]. Everything else in the file is untouched.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::doc::dotenv::Dotenv;
use crate::doc::json5::Json5;
use crate::doc::yaml::Yaml;
use crate::doc::{self, Edit, Format, Record};
use crate::{Change, ChangeKind, Error, Installation, Outcome, Runtime, hermes, openclaw};

const VERSION: u32 = 1;

/// The persisted record of applied changes, stored in a file the caller
/// chooses (e.g. the link's data directory). Written with owner-only
/// permissions: it holds config text that can contain secrets.
#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    entries: Vec<Entry>,
}

/// A change that is currently applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedChange {
    pub runtime: Runtime,
    /// The config file the change was written to.
    pub config_path: PathBuf,
    pub change: ChangeKind,
}

#[derive(Debug, Serialize, Deserialize)]
struct JournalFile {
    version: u32,
    changes: Vec<Entry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Entry {
    runtime: Runtime,
    config_path: PathBuf,
    change: ChangeKind,
    /// The file before the change; `None` when the link created it.
    before: Option<String>,
    /// The file exactly as the link last wrote it.
    after: String,
    records: Vec<Record>,
}

impl Journal {
    /// Opens the journal at `path`; a missing file is an empty journal.
    pub fn open(path: impl Into<PathBuf>) -> Result<Self, Error> {
        let path = path.into();
        let entries = match read_optional(&path)? {
            None => Vec::new(),
            Some(text) => {
                let file: JournalFile =
                    serde_json::from_str(&text).map_err(|error| Error::Parse {
                        path: path.clone(),
                        message: error.to_string(),
                    })?;
                if file.version != VERSION {
                    return Err(Error::Parse {
                        path,
                        message: format!("unsupported journal version {}", file.version),
                    });
                }
                file.changes
            }
        };
        Ok(Self { path, entries })
    }

    /// The changes currently applied, oldest first.
    pub fn applied(&self) -> Vec<AppliedChange> {
        self.entries
            .iter()
            .map(|entry| AppliedChange {
                runtime: entry.runtime,
                config_path: entry.config_path.clone(),
                change: entry.change,
            })
            .collect()
    }

    /// Applies `change` to `install` (`profile` names a Hermes profile;
    /// `None` is the default profile) and records it. Re-applying with the
    /// same parameters changes nothing; with new parameters it replaces the
    /// earlier application and keeps the owner's original values as the ones
    /// a revert restores.
    pub fn apply(
        &mut self,
        install: &Installation,
        profile: Option<&str>,
        change: &Change,
    ) -> Result<Outcome, Error> {
        change.validate()?;
        match (install.runtime, change.kind()) {
            (Runtime::Acp(agent), _) => Err(Error::InvalidChange(format!(
                "{} has no settings the link changes",
                agent.name()
            ))),
            (Runtime::Openclaw, _) => self.apply_in::<Json5>(install, profile, change),
            (Runtime::Hermes, ChangeKind::ApiServer) => {
                self.apply_in::<Dotenv>(install, profile, change)
            }
            (Runtime::Hermes, _) => self.apply_in::<Yaml>(install, profile, change),
        }
    }

    /// Reverts the change of `kind` on `install`/`profile`. Reverting a
    /// change that isn't applied does nothing.
    pub fn revert(
        &mut self,
        install: &Installation,
        profile: Option<&str>,
        kind: ChangeKind,
    ) -> Result<Outcome, Error> {
        match (install.runtime, kind) {
            // Nothing is ever applied to an ACP agent.
            (Runtime::Acp(_), _) => Ok(Outcome {
                changed: false,
                restart: None,
                conflicts: Vec::new(),
            }),
            (Runtime::Openclaw, _) => self.revert_in::<Json5>(install, profile, kind),
            (Runtime::Hermes, ChangeKind::ApiServer) => {
                self.revert_in::<Dotenv>(install, profile, kind)
            }
            (Runtime::Hermes, _) => self.revert_in::<Yaml>(install, profile, kind),
        }
    }

    fn apply_in<F: Format>(
        &mut self,
        install: &Installation,
        profile: Option<&str>,
        change: &Change,
    ) -> Result<Outcome, Error> {
        let file = config_file(install, profile, change.kind())?;
        let current = read_optional(&file)?;
        let text = current.clone().unwrap_or_else(|| F::EMPTY.to_owned());
        let edits = edits_for(install.runtime, &file, &text, change)?;
        if edits.is_empty()
            || doc::is_applied::<F>(&text, &edits).map_err(|m| parse_error(&file, m))?
        {
            return Ok(unchanged());
        }

        // Undo an earlier application first, so the new record's prior
        // values are the owner's and not the link's.
        let existing = self.position(&file, change.kind());
        let (base, before) = match existing.map(|i| &self.entries[i]) {
            Some(entry) if current.as_deref() == Some(entry.after.as_str()) => (
                entry.before.clone().unwrap_or_else(|| F::EMPTY.to_owned()),
                entry.before.clone(),
            ),
            Some(entry) => {
                let (base, _) =
                    doc::revert::<F>(&text, &entry.records).map_err(|m| parse_error(&file, m))?;
                (base.clone(), Some(base))
            }
            None => (text.clone(), current.clone()),
        };
        let edits = edits_for(install.runtime, &file, &base, change)?;
        let (after, records) = doc::apply::<F>(&base, &edits).map_err(|m| parse_error(&file, m))?;
        let entry = Entry {
            runtime: install.runtime,
            config_path: file.clone(),
            change: change.kind(),
            before,
            after: after.clone(),
            records,
        };
        match existing {
            Some(i) => self.entries[i] = entry,
            None => self.entries.push(entry),
        }
        // Journal first: a crash between the two writes leaves a record of a
        // change that never landed, which reverts to a no-op.
        self.save()?;
        write_atomic(&file, &after)?;
        Ok(Outcome {
            changed: true,
            restart: restart_for(install, change.kind(), &text, &after),
            conflicts: Vec::new(),
        })
    }

    fn revert_in<F: Format>(
        &mut self,
        install: &Installation,
        profile: Option<&str>,
        kind: ChangeKind,
    ) -> Result<Outcome, Error> {
        let file = config_file(install, profile, kind)?;
        let Some(index) = self.position(&file, kind) else {
            return Ok(unchanged());
        };
        let entry = self.entries[index].clone();
        let current = read_optional(&file)?;
        let (restored, conflicts) = match &current {
            Some(text) if *text == entry.after => (entry.before.clone(), Vec::new()),
            Some(text) => {
                let (restored, conflicts) =
                    doc::revert::<F>(text, &entry.records).map_err(|m| parse_error(&file, m))?;
                (Some(restored), conflicts)
            }
            // The file is gone; there is nothing left to restore.
            None => (None, Vec::new()),
        };
        let changed = restored != current;
        if changed {
            match &restored {
                Some(text) => write_atomic(&file, text)?,
                None => remove_file(&file)?,
            }
        }
        self.entries.remove(index);
        self.save()?;
        let empty = F::EMPTY.to_owned();
        Ok(Outcome {
            changed,
            restart: changed
                .then(|| {
                    let before = current.as_ref().unwrap_or(&empty);
                    restart_for(install, kind, before, restored.as_ref().unwrap_or(&empty))
                })
                .flatten(),
            conflicts,
        })
    }

    fn position(&self, file: &Path, kind: ChangeKind) -> Option<usize> {
        self.entries
            .iter()
            .position(|entry| entry.config_path == file && entry.change == kind)
    }

    fn save(&self) -> Result<(), Error> {
        let file = JournalFile {
            version: VERSION,
            changes: self.entries.clone(),
        };
        let text = serde_json::to_string_pretty(&file).map_err(|error| Error::Parse {
            path: self.path.clone(),
            message: error.to_string(),
        })?;
        write_atomic(&self.path, &text)
    }
}

fn unchanged() -> Outcome {
    Outcome {
        changed: false,
        restart: None,
        conflicts: Vec::new(),
    }
}

fn parse_error(path: &Path, message: String) -> Error {
    Error::Parse {
        path: path.to_path_buf(),
        message,
    }
}

/// The file a change of `kind` is written to.
fn config_file(
    install: &Installation,
    profile: Option<&str>,
    kind: ChangeKind,
) -> Result<PathBuf, Error> {
    match (install.runtime, kind) {
        (Runtime::Acp(agent), _) => Err(Error::InvalidChange(format!(
            "{} has no settings the link changes",
            agent.name()
        ))),
        (Runtime::Openclaw, _) => openclaw::config_file(install, profile),
        (Runtime::Hermes, ChangeKind::ApiServer) => hermes::env_file(install, profile),
        (Runtime::Hermes, _) => hermes::config_file(install, profile),
    }
}

fn edits_for(
    runtime: Runtime,
    file: &Path,
    text: &str,
    change: &Change,
) -> Result<Vec<Edit>, Error> {
    match runtime {
        Runtime::Openclaw => {
            let config = Json5::parse(text).map_err(|m| parse_error(file, m))?;
            let edits = match change {
                Change::ProxyAccess(access) => openclaw::proxy_access(&config, access),
                Change::NeboaiModels(models) => openclaw::neboai_models(&config, models),
                Change::ApiServer(_) => Vec::new(),
            };
            openclaw::check_includes(&config, &edits, file)?;
            Ok(edits)
        }
        Runtime::Acp(_) => Ok(Vec::new()),
        Runtime::Hermes => Ok(match change {
            Change::ProxyAccess(_) => Vec::new(),
            Change::NeboaiModels(models) => hermes::neboai_models(models),
            Change::ApiServer(api) => hermes::api_server_key(api),
        }),
    }
}

/// The restart the caller must run after the file went from `before` to
/// `after`. OpenClaw reloads live except for the cases in
/// [`openclaw::needs_restart`]. Hermes reads its model per new session, so
/// the gateway is restarted to move running sessions onto (or off) the
/// endpoint; it reads `.env` at start, so the API server key needs one too.
fn restart_for(
    install: &Installation,
    kind: ChangeKind,
    before: &str,
    after: &str,
) -> Option<crate::RuntimeCommand> {
    let needed = match install.runtime {
        Runtime::Openclaw => match (Json5::parse(before), Json5::parse(after)) {
            (Ok(before), Ok(after)) => openclaw::needs_restart(&before, &after),
            _ => true,
        },
        Runtime::Hermes => matches!(kind, ChangeKind::NeboaiModels | ChangeKind::ApiServer),
        Runtime::Acp(_) => false,
    };
    needed.then(|| install.restart.clone())
}

fn read_optional(path: &Path) -> Result<Option<String>, Error> {
    match std::fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(Error::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

fn remove_file(path: &Path) -> Result<(), Error> {
    std::fs::remove_file(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })
}

/// Replaces `path` with `text` through a temporary file in the same
/// directory. A symlinked config is written at its target, keeping the link.
/// An existing file keeps its permissions; a new one is owner-only.
fn write_atomic(path: &Path, text: &str) -> Result<(), Error> {
    let io = |source| Error::Io {
        path: path.to_path_buf(),
        source,
    };
    let target = std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    let dir = target.parent().unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir).map_err(io)?;
    let name = target
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let temp = dir.join(format!(".{name}.nebo-link.tmp"));
    std::fs::write(&temp, text).map_err(io)?;
    let permissions = match std::fs::metadata(&target) {
        Ok(metadata) => metadata.permissions(),
        Err(_) => owner_only(std::fs::metadata(&temp).map_err(io)?.permissions()),
    };
    std::fs::set_permissions(&temp, permissions).map_err(io)?;
    std::fs::rename(&temp, &target).map_err(io)
}

#[cfg(unix)]
fn owner_only(mut permissions: std::fs::Permissions) -> std::fs::Permissions {
    use std::os::unix::fs::PermissionsExt;
    permissions.set_mode(0o600);
    permissions
}

#[cfg(not(unix))]
fn owner_only(permissions: std::fs::Permissions) -> std::fs::Permissions {
    permissions
}
