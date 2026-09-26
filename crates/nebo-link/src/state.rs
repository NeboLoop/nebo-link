//! The link's own files. A machine's link is one bot hosting any number of
//! agents; the bot has one directory:
//!
//! ```text
//! <data dir>/nebo-link/<bot id>/
//!   link.json      the bot (owner, NeboAI endpoints) and every agent it
//!                  hosts: its id, label, runtime and how it is run
//!   offsets.json   acked hub stream offsets
//!   status.json    the running service's connection state
//!   removed.json   only this, once NeboAI removed the bot and the service
//!                  unlinked it (what `nebo-link status` reports)
//!   token          the bot token (0600)
//!   agents/<agent id>/
//!     journal.json   every config change made to an OpenClaw or Hermes
//!                    install, with prior values
//!     processes.json the install's processes the link started (pid, when)
//!     openclaw-device.json
//!                    the keypair the link's own OpenClaw gateway socket
//!                    proves itself with (chat contract)
//!     acp-chats.json the chats an ACP agent that can't list its sessions
//!                    was given (chat contract)
//!   logs/          rotating service logs, and each agent's own output
//!                  (`<agent id>.log`, `<agent id>-<process>.log`)
//! ```
//!
//! `<data dir>` is the platform data directory; `--home` (or
//! `NEBO_LINK_HOME`) replaces `<data dir>/nebo-link`.

use std::path::{Path, PathBuf};

use nebo_runtimes::Runtime;
use serde::{Deserialize, Serialize};

pub use crate::contract::PRIMARY;
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

    /// The directory of the agent `id` hosts.
    pub fn agent(&self, id: &str) -> AgentDir {
        AgentDir {
            bot: self.clone(),
            id: id.to_owned(),
        }
    }

    pub fn create(&self) -> Result<()> {
        std::fs::create_dir_all(&self.0).map_err(|e| Error::io(&self.0, e))?;
        restrict_dir(&self.0)
    }

    /// The bot's link. One saved before a bot could host several agents is
    /// moved to this shape first, its one agent becoming [`PRIMARY`].
    pub fn load(&self) -> Result<Link> {
        let path = self.link_file();
        let value: serde_json::Value = read_json(&path)?;
        if value.get("agents").is_some() {
            return serde_json::from_value(value).map_err(|e| Error::Parse {
                path,
                message: e.to_string(),
            });
        }
        let single: SingleAgent = serde_json::from_value(value).map_err(|e| Error::Parse {
            path: path.clone(),
            message: e.to_string(),
        })?;
        let link = single.into_link();
        self.move_single_agent_files()?;
        self.save(&link)?;
        tracing::info!(bot = %link.bot_id, "the link now hosts its agent as one of several");
        Ok(link)
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

    /// The one agent's files of a link saved before several agents could
    /// share a bot, moved into its agent directory.
    fn move_single_agent_files(&self) -> Result<()> {
        let agent = self.agent(PRIMARY);
        agent.create()?;
        for (old, new) in [
            ("journal.json", agent.journal_file()),
            ("processes.json", agent.processes_file()),
            ("openclaw-device.json", agent.device_file()),
            ("acp-chats.json", agent.acp_chats_file()),
        ] {
            let old = self.0.join(old);
            if old.is_file() {
                std::fs::rename(&old, &new).map_err(|e| Error::io(&old, e))?;
            }
        }
        Ok(())
    }
}

/// One hosted agent's directory, inside its bot's.
#[derive(Debug, Clone)]
pub struct AgentDir {
    bot: BotDir,
    id: String,
}

impl AgentDir {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn bot(&self) -> &BotDir {
        &self.bot
    }
    pub fn path(&self) -> PathBuf {
        self.bot.0.join("agents").join(&self.id)
    }
    pub fn journal_file(&self) -> PathBuf {
        self.path().join("journal.json")
    }
    pub fn processes_file(&self) -> PathBuf {
        self.path().join("processes.json")
    }
    pub fn device_file(&self) -> PathBuf {
        self.path().join("openclaw-device.json")
    }
    pub fn acp_chats_file(&self) -> PathBuf {
        self.path().join("acp-chats.json")
    }
    /// Where the output of the agent's process or command `name` goes;
    /// `None` is the agent's own (an ACP agent's stderr, a runtime's
    /// commands).
    pub fn log(&self, name: Option<&str>) -> PathBuf {
        let file = match name {
            Some(name) => format!("{}-{name}.log", self.id),
            None => format!("{}.log", self.id),
        };
        self.bot.logs_dir().join(file)
    }

    pub fn create(&self) -> Result<()> {
        let path = self.path();
        std::fs::create_dir_all(&path).map_err(|e| Error::io(&path, e))?;
        restrict_dir(&path)
    }

    /// Removes the agent's files.
    pub fn remove(&self) -> Result<()> {
        let path = self.path();
        match std::fs::remove_dir_all(&path) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(Error::io(&path, e)),
            _ => Ok(()),
        }
    }

    /// The runtime's services the link installed for this agent (OpenClaw,
    /// Hermes); empty for any other.
    pub fn services(&self) -> Vec<String> {
        self.bot
            .load()
            .ok()
            .and_then(|link| link.agent(&self.id).and_then(|a| a.install().map(|i| i.services.clone())))
            .unwrap_or_default()
    }

    /// Records that the link installed the runtime's service `name` for
    /// this agent, so `unlink` removes exactly that.
    pub fn record_service(&self, name: &str) -> Result<()> {
        let mut link = self.bot.load()?;
        let Some(install) = link.agent_mut(&self.id).and_then(Hosted::install_mut) else {
            return Ok(());
        };
        if !install.services.iter().any(|s| s == name) {
            install.services.push(name.to_owned());
            self.bot.save(&link)?;
        }
        Ok(())
    }
}

/// A machine's link: one bot, and the agents it hosts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Link {
    pub bot_id: String,
    /// The bot's name in NeboAI.
    pub name: String,
    /// The NeboAI account that owns the bot: the identity the link presents
    /// to a runtime that reads one.
    pub owner_id: String,
    /// The NeboAI services the bot was paired with.
    pub endpoints: Endpoints,
    /// Every agent the bot hosts, the first as [`PRIMARY`]; each is its own
    /// employee on the roster.
    pub agents: Vec<Hosted>,
}

/// One agent a link hosts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Hosted {
    /// Its contract id, fixed when it is added: hired employees name it
    /// (`linked/<bot>/<agent id>`), so it never changes.
    pub id: String,
    /// Its name on the roster: "Claude Code", "Codex · api".
    pub label: String,
    pub runtime: Runtime,
    /// How it is run.
    pub via: Via,
}

/// How a hosted agent is run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Via {
    /// A coding agent the link starts itself, speaking ACP.
    Acp(AcpLink),
    /// An OpenClaw or Hermes install the link opens to NeboAI.
    Install(InstallLink),
}

impl Hosted {
    pub fn acp(&self) -> Option<&AcpLink> {
        match &self.via {
            Via::Acp(acp) => Some(acp),
            Via::Install(_) => None,
        }
    }

    pub fn install(&self) -> Option<&InstallLink> {
        match &self.via {
            Via::Install(install) => Some(install),
            Via::Acp(_) => None,
        }
    }

    pub fn install_mut(&mut self) -> Option<&mut InstallLink> {
        match &mut self.via {
            Via::Install(install) => Some(install),
            Via::Acp(_) => None,
        }
    }
}

/// An ACP agent's settings, fixed when it is added: a service's `PATH` is
/// not the owner's shell's, so the command is kept with absolute paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AcpLink {
    pub program: String,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    /// The folder its conversations work in.
    pub workdir: PathBuf,
}

impl AcpLink {
    pub fn command(&self) -> nebo_runtimes::RuntimeCommand {
        nebo_runtimes::RuntimeCommand {
            program: self.program.clone(),
            args: self.args.clone(),
            env: self.env.clone(),
        }
    }
}

/// An OpenClaw or Hermes install's settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InstallLink {
    /// The installation's home directory, which identifies it among the
    /// detected ones.
    pub home: PathBuf,
    /// The environment overrides that selected the installation when it was
    /// linked (e.g. `OPENCLAW_STATE_DIR`), so the service finds the same one.
    pub env: Vec<(String, String)>,
    /// The local password the link set on the runtime so the owner's own
    /// tools keep working beside the proxy (OpenClaw `gateway.auth.password`).
    pub local_password: String,
    pub models: ModelsEndpoint,
    /// The key the link wrote into the runtime's API server for its chat
    /// contract ([`nebo_runtimes::ApiServer`]).
    pub api_server_key: String,
    /// The runtime processes (by [`nebo_runtimes::ManagedProcess::name`])
    /// whose service the link installed with the runtime's own command, so
    /// `unlink` removes those and never one the owner installed.
    #[serde(default)]
    pub services: Vec<String>,
}

impl Link {
    /// The path prefix the hub serves this bot's UI under.
    pub fn base_path(&self) -> String {
        format!("/t/{}", self.bot_id)
    }

    pub fn agent(&self, id: &str) -> Option<&Hosted> {
        self.agents.iter().find(|a| a.id == id)
    }

    pub fn agent_mut(&mut self, id: &str) -> Option<&mut Hosted> {
        self.agents.iter_mut().find(|a| a.id == id)
    }

    /// The runtime the bot is, as the hub records it: its first OpenClaw or
    /// Hermes install (whose UI the bot opens), else its first agent's.
    pub fn runtime(&self) -> Option<Runtime> {
        self.agents
            .iter()
            .find(|a| a.install().is_some())
            .or(self.agents.first())
            .map(|a| a.runtime)
    }
}

/// A link as saved before a bot could host several agents: one runtime.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SingleAgent {
    bot_id: String,
    name: String,
    runtime: Runtime,
    owner_id: String,
    home: PathBuf,
    env: Vec<(String, String)>,
    endpoints: Endpoints,
    local_password: String,
    models: ModelsEndpoint,
    #[serde(default)]
    api_server_key: String,
    #[serde(default)]
    services: Vec<String>,
    #[serde(default)]
    acp: Option<SingleAcp>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct SingleAcp {
    name: String,
    program: String,
    args: Vec<String>,
    env: Vec<(String, String)>,
    workdir: PathBuf,
}

impl SingleAgent {
    fn into_link(self) -> Link {
        let (label, via) = match self.acp {
            Some(acp) => (
                acp.name,
                Via::Acp(AcpLink {
                    program: acp.program,
                    args: acp.args,
                    env: acp.env,
                    workdir: acp.workdir,
                }),
            ),
            None => (
                crate::install::runtime_name(self.runtime).to_owned(),
                Via::Install(InstallLink {
                    home: self.home,
                    env: self.env,
                    local_password: self.local_password,
                    models: self.models,
                    // A link made before the chat contract had no key yet.
                    api_server_key: match self.api_server_key {
                        key if key.is_empty() => secret(),
                        key => key,
                    },
                    services: self.services,
                }),
            ),
        };
        Link {
            bot_id: self.bot_id,
            name: self.name,
            owner_id: self.owner_id,
            endpoints: self.endpoints,
            agents: vec![Hosted {
                id: PRIMARY.to_owned(),
                label,
                runtime: self.runtime,
                via,
            }],
        }
    }
}

/// A bot NeboAI removed, kept after its service unlinked it so
/// `nebo-link status` can say what happened.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Removed {
    pub bot_id: String,
    pub name: String,
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
    /// The chat contract is announced: an agent answers the link.
    #[serde(default)]
    pub chat: bool,
    /// Why the chat contract is not announced, when it is not.
    #[serde(default)]
    pub chat_error: Option<String>,
    /// The installs' processes the link keeps up, as the supervisors see
    /// them.
    #[serde(default)]
    pub processes: Vec<crate::supervise::ProcessStatus>,
}

/// 256 random bits as hex: the link's keys and passwords.
pub fn secret() -> String {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("os rng");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir).map_err(|e| Error::io(dir, e))?;
    }
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
            owner_id: "owner".into(),
            endpoints: Endpoints::from_env(),
            agents: vec![Hosted {
                id: PRIMARY.into(),
                label: "Hermes".into(),
                runtime: Runtime::Hermes,
                via: Via::Install(InstallLink {
                    home: "/home/u/.hermes".into(),
                    env: vec![],
                    local_password: "pw".into(),
                    models: ModelsEndpoint {
                        port: 18801,
                        key: "k".into(),
                        enabled: false,
                    },
                    api_server_key: String::new(),
                    services: vec![],
                }),
            }],
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

    /// A link saved with one agent (before several could share a bot) loads
    /// as a bot hosting that agent as `assistant`, its files moved into the
    /// agent's directory, and is saved in the new shape.
    #[test]
    fn a_single_agent_link_becomes_its_primary_agent() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = Root::at(tmp.path()).bot("b1");
        dir.create().unwrap();
        std::fs::write(
            dir.link_file(),
            r#"{ "botId": "b1", "name": "Mac · Claude Code", "runtime": { "acp": "claude-code" },
                 "ownerId": "o1", "home": "/usr/local/bin/claude", "env": [],
                 "endpoints": { "api": "a", "comms": "c", "tunnel": "t", "janus": "j" },
                 "localPassword": "pw", "models": { "port": 5, "key": "k", "enabled": false },
                 "apiServerKey": "s", "services": [],
                 "acp": { "name": "Claude Code", "program": "/usr/bin/npx", "args": ["--yes", "adapter"],
                          "env": [["PATH", "/usr/bin"]], "workdir": "/home/u/NeboAI/claude-code" } }"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("acp-chats.json"), "[]").unwrap();

        let link = dir.load().unwrap();
        assert_eq!(link.agents.len(), 1);
        let agent = &link.agents[0];
        assert_eq!((agent.id.as_str(), agent.label.as_str()), (PRIMARY, "Claude Code"));
        assert_eq!(agent.runtime, Runtime::Acp(nebo_runtimes::acp::Agent::ClaudeCode));
        let acp = agent.acp().unwrap();
        assert_eq!(acp.workdir, PathBuf::from("/home/u/NeboAI/claude-code"));
        assert_eq!(acp.args, ["--yes", "adapter"]);
        assert!(dir.agent(PRIMARY).acp_chats_file().is_file(), "its chats moved with it");
        assert!(!dir.path().join("acp-chats.json").exists());
        // Saved in the new shape: the next load reads it as it is.
        assert_eq!(dir.load().unwrap(), link);

        let hermes = Root::at(tmp.path()).bot("b2");
        hermes.create().unwrap();
        std::fs::write(
            hermes.link_file(),
            r#"{ "botId": "b2", "name": "Mac · Hermes", "runtime": "hermes", "ownerId": "o1",
                 "home": "/home/u/.hermes", "env": [["HERMES_HOME", "/home/u/.hermes"]],
                 "endpoints": { "api": "a", "comms": "c", "tunnel": "t", "janus": "j" },
                 "localPassword": "pw", "models": { "port": 5, "key": "k", "enabled": true },
                 "apiServerKey": "s", "services": ["gateway"] }"#,
        )
        .unwrap();
        std::fs::write(hermes.path().join("journal.json"), "{}").unwrap();
        let link = hermes.load().unwrap();
        let install = link.agents[0].install().unwrap();
        assert_eq!(link.agents[0].label, "Hermes");
        assert_eq!(install.services, ["gateway"]);
        assert!(install.models.enabled);
        assert!(hermes.agent(PRIMARY).journal_file().is_file());
        assert_eq!(link.runtime(), Some(Runtime::Hermes));
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
