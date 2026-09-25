//! The link's own files. Each linked bot has one directory:
//!
//! ```text
//! <data dir>/nebo-link/<bot id>/
//!   link.json      what this bot links: runtime, install, owner, models endpoint
//!   journal.json   every config change made to the runtime, with prior values
//!   offsets.json   acked hub stream offsets
//!   status.json    the running service's connection state
//!   removed.json   only this, once NeboAI removed the bot and the service
//!                  unlinked it (what `nebo-link status` reports)
//!   token          the bot token (0600)
//!   logs/          rotating service logs
//! ```
//!
//! `<data dir>` is the platform data directory; `--home` (or
//! `NEBO_LINK_HOME`) replaces `<data dir>/nebo-link`.

use std::path::{Path, PathBuf};

use nebo_runtimes::Runtime;
use serde::{Deserialize, Serialize};

use crate::endpoints::Endpoints;
use crate::error::{Error, Result};

/// The directory holding every linked bot.
#[derive(Debug, Clone)]
pub struct Root(PathBuf);

impl Root {
    /// `home` when given (`--home` / `NEBO_LINK_HOME`), otherwise the
    /// platform default.
    pub fn resolve(home: Option<PathBuf>) -> Result<Self> {
        if let Some(home) = home {
            return Ok(Self(home));
        }
        dirs::data_dir()
            .map(|dir| Self(dir.join("nebo-link")))
            .ok_or_else(|| Error::Message("this system has no user data directory".into()))
    }

    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self(path.into())
    }

    pub fn path(&self) -> &Path {
        &self.0
    }

    pub fn bot(&self, bot_id: &str) -> BotDir {
        BotDir(self.0.join(bot_id))
    }

    /// Every linked bot, ordered by name.
    pub fn links(&self) -> Result<Vec<Link>> {
        let entries = match std::fs::read_dir(&self.0) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::io(&self.0, e)),
        };
        let mut links = Vec::new();
        for entry in entries.flatten() {
            let dir = BotDir(entry.path());
            if dir.link_file().is_file() {
                links.push(dir.load()?);
            }
        }
        links.sort_by(|a, b| a.name.cmp(&b.name).then(a.bot_id.cmp(&b.bot_id)));
        Ok(links)
    }

    /// The bots NeboAI removed and their services unlinked, ordered by name.
    pub fn removed(&self) -> Result<Vec<Removed>> {
        let entries = match std::fs::read_dir(&self.0) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(Error::io(&self.0, e)),
        };
        let mut removed = Vec::new();
        for entry in entries.flatten() {
            let file = BotDir(entry.path()).removed_file();
            if file.is_file() {
                removed.push(read_json::<Removed>(&file)?);
            }
        }
        removed.sort_by(|a, b| a.name.cmp(&b.name).then(a.bot_id.cmp(&b.bot_id)));
        Ok(removed)
    }

    /// The link `bot` names, or the only one when `bot` is `None`.
    pub fn select(&self, bot: Option<&str>) -> Result<Link> {
        let links = self.links()?;
        if let Some(id) = bot {
            return links
                .into_iter()
                .find(|l| l.bot_id == id)
                .ok_or_else(|| Error::Message(format!("no linked bot with id {id}")));
        }
        match links.len() {
            0 => Err(Error::Message(
                "nothing is linked yet; run `nebo-link <code>` with a code from the NeboAI app".into(),
            )),
            1 => Ok(links.into_iter().next().expect("one link")),
            _ => Err(Error::Message(format!(
                "several bots are linked; choose one with --bot:\n{}",
                links
                    .iter()
                    .map(|l| format!("  --bot {}  ({})", l.bot_id, l.name))
                    .collect::<Vec<_>>()
                    .join("\n")
            ))),
        }
    }
}

/// One linked bot's directory.
#[derive(Debug, Clone)]
pub struct BotDir(PathBuf);

impl BotDir {
    pub fn path(&self) -> &Path {
        &self.0
    }
    pub fn link_file(&self) -> PathBuf {
        self.0.join("link.json")
    }
    pub fn journal_file(&self) -> PathBuf {
        self.0.join("journal.json")
    }
    pub fn offsets_file(&self) -> PathBuf {
        self.0.join("offsets.json")
    }
    pub fn status_file(&self) -> PathBuf {
        self.0.join("status.json")
    }
    pub fn removed_file(&self) -> PathBuf {
        self.0.join("removed.json")
    }
    pub fn token_file(&self) -> PathBuf {
        self.0.join("token")
    }
    pub fn logs_dir(&self) -> PathBuf {
        self.0.join("logs")
    }

    pub fn create(&self) -> Result<()> {
        std::fs::create_dir_all(&self.0).map_err(|e| Error::io(&self.0, e))?;
        restrict_dir(&self.0)
    }

    pub fn load(&self) -> Result<Link> {
        read_json(&self.link_file())
    }

    pub fn save(&self, link: &Link) -> Result<()> {
        write_json(&self.link_file(), link)
    }

    pub fn load_status(&self) -> Option<Status> {
        read_json(&self.status_file()).ok()
    }

    pub fn save_status(&self, status: &Status) -> Result<()> {
        write_json(&self.status_file(), status)
    }

    /// Removes the status: the service has stopped.
    pub fn clear_status(&self) {
        let _ = std::fs::remove_file(self.status_file());
    }

    /// Removes the directory and everything in it.
    pub fn remove(&self) -> Result<()> {
        match std::fs::remove_dir_all(&self.0) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(Error::io(&self.0, e)),
            _ => Ok(()),
        }
    }
}

/// What one linked bot links.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Link {
    pub bot_id: String,
    /// The bot's name in NeboAI.
    pub name: String,
    pub runtime: Runtime,
    /// The NeboAI account that owns the bot: the identity the link presents
    /// to a runtime that reads one.
    pub owner_id: String,
    /// The installation's home directory, which identifies it among the
    /// detected ones.
    pub home: PathBuf,
    /// The environment overrides that selected the installation when it was
    /// linked (e.g. `OPENCLAW_STATE_DIR`), so the service finds the same one.
    pub env: Vec<(String, String)>,
    /// The NeboAI services the bot was paired with.
    pub endpoints: Endpoints,
    /// The local password the link set on the runtime so the owner's own
    /// tools keep working beside the proxy (OpenClaw `gateway.auth.password`).
    pub local_password: String,
    pub models: ModelsEndpoint,
}

impl Link {
    /// The path prefix the hub serves this bot's UI under.
    pub fn base_path(&self) -> String {
        format!("/t/{}", self.bot_id)
    }
}

/// A bot NeboAI removed, kept after its service unlinked it so
/// `nebo-link status` can say what happened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Removed {
    pub bot_id: String,
    pub name: String,
    pub runtime: Runtime,
    /// The installation it linked; linking it again clears this record.
    pub home: PathBuf,
}

/// The local NeboAI models endpoint the runtime is pointed at when models
/// are on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelsEndpoint {
    /// Loopback port; fixed at pairing because it is written into the
    /// runtime's config.
    pub port: u16,
    /// The key the runtime presents to the endpoint, so no other local
    /// program spends the owner's NeboAI balance.
    pub key: String,
    pub enabled: bool,
}

/// How often the running service rewrites its status.
pub const STATUS_EVERY: std::time::Duration = std::time::Duration::from_secs(15);

/// The running service's connection state, for `nebo-link status`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub pid: u32,
    /// When the service last wrote this (Unix seconds); it writes every
    /// [`STATUS_EVERY`], so an older file means the service is not running.
    pub updated: u64,
    /// Connected to the comms gateway (the bot shows online).
    pub online: bool,
    /// The tunnel is up (the UI is reachable from the app).
    pub tunnel: bool,
    /// The last connection failure, while not connected.
    pub error: Option<String>,
}

pub fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T> {
    let text = std::fs::read_to_string(path).map_err(|e| Error::io(path, e))?;
    serde_json::from_str(&text).map_err(|e| Error::Parse {
        path: path.to_path_buf(),
        message: e.to_string(),
    })
}

/// Writes `value` as JSON through a temporary file, so a crash never leaves
/// half a file. Readable by the owner only.
pub fn write_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let text = serde_json::to_string_pretty(value).expect("state serializes");
    write_private(path, text.as_bytes())
}

/// Writes `bytes` to `path` atomically with owner-only permissions.
pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp).map_err(|e| Error::io(&tmp, e))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|e| Error::io(&tmp, e))?;
    std::fs::rename(&tmp, path).map_err(|e| Error::io(path, e))
}

fn restrict_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .map_err(|e| Error::io(path, e))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn link(id: &str, name: &str) -> Link {
        Link {
            bot_id: id.into(),
            name: name.into(),
            runtime: Runtime::Hermes,
            owner_id: "owner".into(),
            home: "/home/u/.hermes".into(),
            env: vec![],
            endpoints: Endpoints::from_env(),
            local_password: "pw".into(),
            models: ModelsEndpoint {
                port: 18801,
                key: "k".into(),
                enabled: false,
            },
        }
    }

    #[test]
    fn links_round_trip_and_select() {
        let tmp = tempfile::tempdir().unwrap();
        let root = Root::at(tmp.path());
        assert!(root.links().unwrap().is_empty());
        assert!(root.select(None).is_err());

        let a = root.bot("a");
        a.create().unwrap();
        a.save(&link("a", "alpha")).unwrap();
        assert_eq!(root.select(None).unwrap().bot_id, "a");

        let b = root.bot("b");
        b.create().unwrap();
        b.save(&link("b", "beta")).unwrap();
        let err = root.select(None).unwrap_err().to_string();
        assert!(err.contains("--bot a") && err.contains("--bot b"), "{err}");
        assert_eq!(root.select(Some("b")).unwrap().name, "beta");
        assert!(root.select(Some("c")).is_err());

        b.remove().unwrap();
        assert_eq!(root.links().unwrap().len(), 1);
    }

    #[cfg(unix)]
    #[test]
    fn private_files_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("secret");
        write_private(&path, b"x").unwrap();
        let mode = std::fs::metadata(&path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
}
