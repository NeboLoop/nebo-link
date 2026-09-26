//! The link's operations: pairing the machine, adding and removing the
//! agents its bot hosts, NeboAI models on and off, the chat contract, and
//! unlinking. Each is one function the CLI and the running service share.
//!
//! A machine is paired once, as one bot; every agent on it (Claude Code and
//! Codex in their own folders, an OpenClaw or Hermes install) is added to
//! that bot and is its own employee on the roster.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use nebo_runtimes::{
    acp, ApiServer, Change, ChangeKind, Environment, Installation, Journal, NeboaiModels, ProxyAccess, Runtime,
    RuntimeCommand, Service, detect, openclaw,
};
use tokio::sync::watch;

use crate::contract::roster::{Member, Roster};
use crate::contract::{self, Contract, Inbox};
use crate::credentials::Credentials;
use crate::endpoints::{Endpoints, WEB_ORIGIN};
use crate::error::{Error, Result};
use crate::install::{self, runtime_key, runtime_name};
use crate::janus::{self, Janus};
use crate::proxy;
use crate::service;
use crate::state::{
    AcpLink, AgentDir, BotDir, Hosted, InstallLink, Link, ModelsEndpoint, PRIMARY, Removed, Root, Via, secret,
    write_json,
};
use crate::supervise::{self, Supervisor};

/// The bot's purpose as the hub records it.
const PURPOSE: &str = "linked";

/// The header OpenClaw reads the signed-in owner from (trusted-proxy auth).
const USER_HEADER: &str = "x-nebo-user";

/// The agent to host: a runtime by name (OpenClaw, Hermes, Claude Code,
/// Codex, ...), or any other agent that speaks ACP by the command that
/// starts it. Without either, the one OpenClaw or Hermes install found.
#[derive(Debug, Clone, Default)]
pub struct Wanted {
    pub runtime: Option<Runtime>,
    pub acp_command: Option<String>,
    /// The folder a coding agent works in (default `~/NeboAI/<agent>`).
    pub dir: Option<PathBuf>,
    /// Its name on the roster (default: the runtime's, with the folder's
    /// name when the bot already hosts one of that runtime).
    pub label: Option<String>,
}

/// An agent found and proven to run, not yet added.
struct Chosen {
    install: Installation,
    /// An ACP agent's settings; `None` for OpenClaw and Hermes.
    acp: Option<AcpLink>,
    /// What an ACP agent called itself when it is not one the link knows.
    title: Option<String>,
    label: Option<String>,
}

/// What adding an agent did.
pub struct Added {
    pub agent: Hosted,
    /// The install's processes that were not running and the link started.
    pub started: Vec<String>,
    /// Set when the install could not be restarted to pick up its new
    /// settings (for one, a gateway started by hand in a terminal): the
    /// owner restarts it; everything else is in place.
    pub restart_failed: Option<String>,
}

/// What pairing did.
pub struct Paired {
    pub link: Link,
    pub added: Added,
}

/// Pairs this machine with the owner's NeboAI account using `code`, as one
/// bot hosting the agent `wanted` names, and installs and starts the
/// service. More agents are added to the same bot with [`add`].
///
/// The agent is found (and a coding agent started once) before the code is
/// spent, so an agent that can't run is refused while the code is good.
pub async fn pair(root: &Root, code: &str, wanted: Wanted, name: Option<String>, exe: PathBuf) -> Result<Paired> {
    if let Some(linked) = root.links()?.first() {
        return Err(Error::Message(format!(
            "This computer is already linked as \"{}\". Add an agent to it with `nebo-link add`.",
            linked.name
        )));
    }
    let chosen = choose(None, wanted).await?;
    let endpoints = Endpoints::from_env();
    let bot_id = uuid::Uuid::new_v4().to_string();
    let name = name.unwrap_or_else(|| {
        let host = host_label();
        if host.is_empty() { runtime_name(chosen.install.runtime).to_owned() } else { host }
    });

    let resp = nebo_comm::api::redeem_code(
        &endpoints.api,
        code.trim(),
        &name,
        PURPOSE,
        &bot_id,
        runtime_key(chosen.install.runtime),
    )
    .await
    .map_err(|e| Error::Message(format!("Could not link with that code: {e}")))?;

    let dir = root.bot(&bot_id);
    dir.create()?;
    let mut link = Link {
        bot_id: bot_id.clone(),
        name: resp.name.clone(),
        owner_id: resp.id.clone(),
        endpoints,
        agents: Vec::new(),
    };
    dir.save(&link)?;
    // Linked again: what `status` said about a removal no longer applies.
    for removed in root.removed()? {
        root.bot(&removed.bot_id).remove()?;
    }

    let finish = async {
        Credentials::open(&dir).save(&resp.connection_token)?;
        let added = attach(&dir, &mut link, chosen).await?;
        service::install(&service::Spec {
            bot_id: bot_id.clone(),
            exe,
            home: root_override(root),
            path: std::env::var("PATH").ok(),
        })?;
        Ok::<_, Error>(added)
    };
    match finish.await {
        Ok(added) => Ok(Paired {
            link: dir.load()?,
            added,
        }),
        Err(e) => Err(Error::Message(format!(
            "{e}\nThe bot is paired but not running. Fix the problem above and run `nebo-link run --bot {bot_id}`, or undo it with `nebo-link unlink --bot {bot_id}`."
        ))),
    }
}

/// Adds the agent `wanted` names to the linked bot (`bot`, or the only
/// one). A coding agent joins the running service as it is (no new code,
/// no new service); an OpenClaw or Hermes install restarts it, to open the
/// install to NeboAI.
pub async fn add(root: &Root, bot: Option<&str>, wanted: Wanted) -> Result<Added> {
    let mut link = root.select(bot)?;
    let chosen = choose(Some(&link), wanted).await?;
    let dir = root.bot(&link.bot_id);
    let added = attach(&dir, &mut link, chosen).await?;
    if added.agent.install().is_some() && service::installed(&link.bot_id) {
        service::restart(&link.bot_id)?;
    }
    Ok(added)
}

/// What removing an agent did.
pub struct Removal {
    pub agent: Hosted,
    /// For an OpenClaw or Hermes install: what releasing it did.
    pub released: Option<Released>,
}

/// Removes the hosted agent `agent_id` from the linked bot. A coding
/// agent's process ends with it; an OpenClaw or Hermes install gets its
/// config back and the service restarts without it. The bot's last agent
/// is not removed: that is `unlink`.
pub async fn remove(root: &Root, bot: Option<&str>, agent_id: &str) -> Result<Removal> {
    let mut link = root.select(bot)?;
    let agent = link
        .agent(agent_id)
        .cloned()
        .ok_or_else(|| Error::Message(format!("{} hosts no agent {agent_id}. `nebo-link status` lists them.", link.name)))?;
    if link.agents.len() == 1 {
        return Err(Error::Message(format!(
            "{} is the only agent of {}. To unlink the bot, run `nebo-link unlink`.",
            agent.label, link.name
        )));
    }
    let dir = root.bot(&link.bot_id);
    let released = match agent.install() {
        Some(install) => Some(release(&dir.agent(&agent.id), agent.runtime, install).await?),
        None => None,
    };
    link.agents.retain(|a| a.id != agent.id);
    dir.save(&link)?;
    dir.agent(&agent.id).remove()?;
    if released.is_some() && service::installed(&link.bot_id) {
        service::restart(&link.bot_id)?;
    }
    Ok(Removal { agent, released })
}

/// Finds the agent `wanted` names and proves it can run: a coding agent is
/// started once in its folder. `link` is the bot it joins, whose agents it
/// must not repeat.
async fn choose(link: Option<&Link>, wanted: Wanted) -> Result<Chosen> {
    let env = Environment::current();
    let (installs, runtime) = match &wanted.acp_command {
        Some(command) => (
            vec![acp::custom(command, &env).map_err(Error::Message)?],
            Some(Runtime::Acp(acp::Agent::Other)),
        ),
        None => (detect(&env), wanted.runtime),
    };
    // An install is linked once; a coding agent as often as the owner has
    // folders for it.
    let linked: Vec<PathBuf> = link
        .map(|l| l.agents.iter().filter_map(|a| a.install().map(|i| i.home.clone())).collect())
        .unwrap_or_default();
    let install = install::choose(installs, runtime, &linked)?;
    let Some(agent) = install.runtime.acp() else {
        return Ok(Chosen {
            install,
            acp: None,
            title: None,
            label: wanted.label,
        });
    };
    let workdir = workdir(agent, wanted.dir, &env)?;
    if let Some(same) = link.and_then(|l| {
        l.agents
            .iter()
            .find(|a| a.runtime == install.runtime && a.acp().is_some_and(|acp| acp.workdir == workdir))
    }) {
        return Err(Error::Message(format!(
            "{} already works in {} as \"{}\". Choose another folder with --dir.",
            agent.name(),
            workdir.display(),
            same.label
        )));
    }
    let title = probe(agent, &install, &workdir).await?;
    Ok(Chosen {
        acp: Some(AcpLink {
            program: install.restart.program.clone(),
            args: install.restart.args.clone(),
            env: install.restart.env.clone(),
            workdir,
        }),
        install,
        title,
        label: wanted.label,
    })
}

/// Adds `chosen` to `link` and saves it; an OpenClaw or Hermes install is
/// opened to the link (its config journaled), what of it was not running
/// is started, and it is restarted onto its new settings.
async fn attach(dir: &BotDir, link: &mut Link, chosen: Chosen) -> Result<Added> {
    let Chosen {
        install,
        acp,
        title,
        label,
    } = chosen;
    let runtime = install.runtime;
    let folder = match &acp {
        Some(acp) => acp.workdir.clone(),
        None => install.home.clone(),
    };
    let name = match (runtime.acp(), title) {
        (Some(acp::Agent::Other), Some(title)) => title,
        _ => runtime_name(runtime).to_owned(),
    };
    let label = label
        .map(|l| l.trim().to_owned())
        .filter(|l| !l.is_empty())
        .unwrap_or_else(|| default_label(&link.agents, runtime, &name, &folder));
    let id = new_id(&link.agents, &label);
    let via = match acp {
        Some(acp) => Via::Acp(acp),
        None => Via::Install(InstallLink {
            home: install.home.clone(),
            env: install.restart.env.clone(),
            local_password: secret(),
            models: ModelsEndpoint {
                port: free_port()?,
                key: secret(),
                enabled: false,
            },
            api_server_key: secret(),
            services: Vec::new(),
        }),
    };
    let agent = Hosted {
        id,
        label,
        runtime,
        via,
    };
    link.agents.push(agent.clone());
    dir.save(link)?;
    let agent_dir = dir.agent(&agent.id);
    agent_dir.create()?;

    let Some(settings) = agent.install() else {
        // Nothing of a coding agent's to change or keep running: the
        // service starts it when it is first asked for.
        return Ok(Added {
            agent,
            started: Vec::new(),
            restart_failed: None,
        });
    };
    let supervisor = Supervisor::new(&agent_dir, runtime, install.processes.clone());
    let running = supervisor.running().await;
    let mut journal = Journal::open(agent_dir.journal_file())?;
    let outcome = journal.apply(&install, None, &Change::ProxyAccess(proxy_access(link, settings)))?;
    let restart = apply_api_server(&mut journal, &install, settings)?.or(outcome.restart);
    // What was not running is started now, onto the new settings; the
    // service keeps it up from here.
    let started = supervisor.ensure().await;
    // Last, and not fatal: the link connects as soon as the install comes
    // back with its new settings. Only one that was already running needs
    // restarting onto them.
    let was_running = install.processes.first().is_some_and(|p| running.contains(&p.name));
    let restart_failed = match restart.filter(|_| was_running) {
        Some(command) => install::run(&command, install::COMMAND_WAIT, &agent_dir.log(None))
            .await
            .err()
            .map(|e| e.to_string()),
        None => None,
    };
    Ok(Added {
        agent: dir.load()?.agent(agent_dir.id()).cloned().unwrap_or(agent),
        started,
        restart_failed,
    })
}

/// "Claude Code" for the bot's first Claude Code, "Claude Code · api" for
/// one more, named for its folder.
fn default_label(agents: &[Hosted], runtime: Runtime, name: &str, folder: &Path) -> String {
    if !agents.iter().any(|a| a.runtime == runtime) {
        return name.to_owned();
    }
    match folder.file_name().map(|f| f.to_string_lossy().into_owned()) {
        Some(folder) if !folder.is_empty() => format!("{name} · {folder}"),
        _ => format!("{name} {}", agents.iter().filter(|a| a.runtime == runtime).count() + 1),
    }
}

/// The new agent's id: `assistant` for the bot's first, else its label as
/// a slug, made unique. Fixed from here on: hires name it.
fn new_id(agents: &[Hosted], label: &str) -> String {
    if agents.is_empty() {
        return PRIMARY.to_owned();
    }
    let mut slug = String::new();
    for c in label.chars().flat_map(char::to_lowercase) {
        if c.is_ascii_alphanumeric() {
            slug.push(c);
        } else if !slug.ends_with('-') && !slug.is_empty() {
            slug.push('-');
        }
    }
    let base = match slug.trim_end_matches('-') {
        "" => "agent".to_owned(),
        s => s.to_owned(),
    };
    let taken = |id: &str| id == PRIMARY || agents.iter().any(|a| a.id == id);
    if !taken(&base) {
        return base;
    }
    (2..).map(|n| format!("{base}-{n}")).find(|id| !taken(id)).expect("a free id")
}

/// The change that serves an install's UI behind the link.
pub fn proxy_access(link: &Link, install: &InstallLink) -> ProxyAccess {
    ProxyAccess {
        base_path: link.base_path(),
        origin: WEB_ORIGIN.to_string(),
        user_header: USER_HEADER.to_string(),
        identity: link.owner_id.clone(),
        password: install.local_password.clone(),
    }
}

/// Turns on the runtime's API server for the chat contract, for the default
/// profile and every named one (each needs its own key under Hermes'
/// multiplexing). Returns the restart the runtime needs to read it, if any.
pub fn apply_api_server(
    journal: &mut Journal,
    install: &Installation,
    settings: &InstallLink,
) -> Result<Option<RuntimeCommand>> {
    let change = Change::ApiServer(ApiServer {
        key: settings.api_server_key.clone(),
    });
    let mut restart = journal.apply(install, None, &change)?.restart;
    for profile in &install.profiles {
        restart = restart.or(journal.apply(install, Some(&profile.name), &change)?.restart);
    }
    Ok(restart)
}

/// The chat contract for the bot: every hosted agent a member of one
/// [`Roster`], which the running service changes as agents are added and
/// removed. `installs` are the OpenClaw and Hermes installs found for the
/// link's install agents, by agent id; an agent that can't be served is
/// left out, and why is logged.
pub async fn chat(
    dir: &BotDir,
    link: &Link,
    installs: &[(String, Installation)],
    token: watch::Receiver<String>,
) -> (Arc<Contract>, Arc<Roster>) {
    let mut members = Vec::new();
    for agent in &link.agents {
        match member(dir, link, agent, installs).await {
            Ok(member) => members.push(member),
            Err(why) => tracing::info!(agent = %agent.id, why, "this agent's chat is not served"),
        }
    }
    let roster = Arc::new(Roster::new(members));
    let inbox = Inbox::new(&link.endpoints.api, &link.bot_id, token);
    let runtime = link.runtime().unwrap_or(Runtime::Acp(acp::Agent::Other));
    let contract = Contract::new(runtime_key(runtime), runtime_name(runtime), &link.bot_id, roster.clone(), Some(inbox));
    (contract, roster)
}

/// The roster's member for a coding agent: its backend, ready to start the
/// agent when it is first asked for.
pub fn acp_member(dir: &BotDir, agent: &Hosted) -> Option<Member> {
    let (Some(acp), Runtime::Acp(kind)) = (agent.acp(), agent.runtime) else {
        return None;
    };
    let agent_dir = dir.agent(&agent.id);
    let backend = contract::acp::Acp::new(contract::acp::Settings {
        agent: kind,
        name: agent.label.clone(),
        command: acp.command(),
        workdir: acp.workdir.clone(),
        log: agent_dir.log(None),
        chats_file: agent_dir.acp_chats_file(),
    });
    Some(Member {
        id: agent.id.clone(),
        label: agent.label.clone(),
        backend: Arc::new(backend),
    })
}

/// The roster's members once the link changed from `before` to `after`:
/// an agent that stays as it was keeps its member (its running process and
/// its sessions), a coding agent added or changed gets a new one, and one
/// removed is dropped. An install keeps the member it started with: adding
/// or removing one restarts the service.
pub fn reconcile(dir: &BotDir, before: &Link, after: &Link, current: &[Member]) -> Vec<Member> {
    after
        .agents
        .iter()
        .filter_map(|agent| {
            let kept = current.iter().find(|m| m.id == agent.id);
            match kept {
                Some(member) if before.agent(&agent.id) == Some(agent) || agent.install().is_some() => {
                    Some(member.clone())
                }
                _ => acp_member(dir, agent),
            }
        })
        .collect()
}

/// One hosted agent's member of the roster.
async fn member(
    dir: &BotDir,
    link: &Link,
    agent: &Hosted,
    installs: &[(String, Installation)],
) -> std::result::Result<Member, String> {
    if let Some(member) = acp_member(dir, agent) {
        return Ok(member);
    }
    let settings = agent
        .install()
        .ok_or_else(|| format!("{} has no saved command; add it again", agent.label))?;
    let install = installs
        .iter()
        .find(|(id, _)| *id == agent.id)
        .map(|(_, install)| install)
        .ok_or_else(|| format!("{} was not found on this computer", runtime_name(agent.runtime)))?;
    let agent_dir = dir.agent(&agent.id);
    let backend: Arc<dyn contract::backend::Backend> = match agent.runtime {
        Runtime::Hermes => Arc::new(hermes_backend(&agent_dir, agent.runtime, settings, install).await?),
        Runtime::Openclaw => {
            let gateway = install::ui_addr(install).ok_or_else(|| "OpenClaw has no gateway to reach".to_owned())?;
            Arc::new(contract::openclaw::Openclaw::new(
                openclaw::gateway::Connect::new(
                    format!("ws://{gateway}"),
                    &proxy_access(link, settings),
                    proxy::FORWARDED_FOR,
                ),
                openclaw::gateway::FileDeviceStore::new(agent_dir.device_file()),
            ))
        }
        Runtime::Acp(_) => unreachable!("an ACP agent is run by its saved command"),
    };
    Ok(Member {
        id: agent.id.clone(),
        label: agent.label.clone(),
        backend,
    })
}

/// Hermes' API server turned on with the link's key, and a backend on it.
async fn hermes_backend(
    dir: &AgentDir,
    runtime: Runtime,
    settings: &InstallLink,
    install: &Installation,
) -> std::result::Result<contract::hermes::Hermes, String> {
    let mut journal = Journal::open(dir.journal_file()).map_err(|e| e.to_string())?;
    let restart = apply_api_server(&mut journal, install, settings)
        .map_err(|e| format!("could not turn on the {} API server: {e}", runtime_name(runtime)))?;
    if let Some(command) = restart
        && let Err(e) = install::run(&command, install::COMMAND_WAIT, &dir.log(None)).await
    {
        tracing::info!(error = %e, "the runtime was not restarted onto its API server key");
    }
    // Detected again: the API server's endpoint appears once its key is in
    // place, unless the config keeps it off.
    let install = install::find(runtime, settings).map_err(|e| e.to_string())?;
    let default = install
        .endpoints
        .iter()
        .find(|e| {
            e.service
                == Service::HermesApiServer {
                    profile: "default".to_owned(),
                }
        })
        .ok_or_else(|| {
            format!(
                "the Hermes API server is turned off in {} (platforms.api_server.enabled)",
                install.config_path.display()
            )
        })?;
    let profiles = install.profiles.iter().map(|p| p.name.clone()).collect();
    Ok(contract::hermes::Hermes::new(&format!("http://{}", default.addr), &settings.api_server_key, profiles))
}

/// The models endpoint an install is pointed at, as seen from `link` with
/// `token`.
pub fn janus(link: &Link, install: &InstallLink, token: watch::Receiver<String>) -> Janus {
    Janus {
        url: link.endpoints.janus.clone(),
        bot_id: link.bot_id.clone(),
        token,
        key: install.models.key.clone(),
        client: janus::client(),
    }
}

/// What turning models on or off did.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelsChange {
    pub enabled: bool,
    /// An install was restarted to pick the change up.
    pub restarted: bool,
    /// Settings the owner changed since the link set them, left as they are.
    pub conflicts: Vec<String>,
}

/// Points the bot's OpenClaw and Hermes installs' models at NeboAI
/// (`enabled`) or restores the provider each had before. Coding agents run
/// on their own sign-in and are not changed.
pub async fn set_models(dir: &BotDir, token: watch::Receiver<String>, enabled: bool) -> Result<ModelsChange> {
    let mut link = dir.load()?;
    let ids: Vec<String> = link.agents.iter().filter(|a| a.install().is_some()).map(|a| a.id.clone()).collect();
    if ids.is_empty() {
        return Err(Error::Message(
            "The bot's agents run on their own sign-in; NeboAI models are for OpenClaw and Hermes.".into(),
        ));
    }
    let mut change = ModelsChange {
        enabled,
        restarted: false,
        conflicts: Vec::new(),
    };
    for id in ids {
        let agent = link.agent(&id).cloned().expect("agent");
        let settings = agent.install().expect("an install");
        let agent_dir = dir.agent(&id);
        let install = install::find(agent.runtime, settings)?;
        let mut journal = Journal::open(agent_dir.journal_file())?;
        let outcome = if enabled {
            let (models, default_model) = janus::models(&janus(&link, settings, token.clone()))
                .await
                .map_err(Error::Message)?;
            let neboai = Change::NeboaiModels(NeboaiModels {
                base_url: format!("http://127.0.0.1:{}/v1", settings.models.port),
                api_key: settings.models.key.clone(),
                models,
                default_model,
            });
            journal.apply(&install, None, &neboai)?
        } else {
            journal.revert(&install, None, ChangeKind::NeboaiModels)?
        };
        if let Some(command) = &outcome.restart {
            install::run(command, install::COMMAND_WAIT, &agent_dir.log(None)).await?;
            change.restarted = true;
        }
        change.conflicts.extend(outcome.conflicts);
        if let Some(settings) = link.agent_mut(&id).and_then(Hosted::install_mut) {
            settings.models.enabled = enabled;
        }
        dir.save(&link)?;
    }
    Ok(change)
}

/// Whether the bot's installs use NeboAI models.
pub fn models_enabled(link: &Link) -> bool {
    link.agents.iter().filter_map(Hosted::install).any(|i| i.models.enabled)
}

/// Who unlinks a bot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum By {
    /// The owner, with `nebo-link unlink`.
    Owner,
    /// The bot's own service, because NeboAI removed the bot.
    Revocation,
}

/// What releasing an OpenClaw or Hermes install did.
#[derive(Default)]
pub struct Released {
    /// The install's processes the link had started and its services the
    /// link had installed, now stopped and removed.
    pub processes: supervise::Released,
    /// Settings the owner changed since the link set them, left as they are.
    pub conflicts: Vec<String>,
    /// Why the install's config could not be restored, when it could not.
    pub not_restored: Option<String>,
    /// Why the install could not be restarted onto its restored config,
    /// when it could not.
    pub not_restarted: Option<String>,
}

/// Gives an install back: what the link started or installed of it is
/// stopped and removed, and its config restored (and it restarted onto it,
/// unless the link had started it).
async fn release(dir: &AgentDir, runtime: Runtime, settings: &InstallLink) -> Result<Released> {
    let mut released = Released::default();
    let install = match install::find(runtime, settings) {
        Ok(install) => install,
        Err(e) => {
            released.not_restored = Some(e.to_string());
            return Ok(released);
        }
    };
    // What the link started or installed goes first, so nothing of the
    // link's is running on the config being restored.
    released.processes = supervise::release(dir, &settings.services, &install.processes).await;
    let runtime_released = install.processes.first().is_some_and(|p| {
        released.processes.stopped.contains(&p.name) || released.processes.uninstalled.contains(&p.name)
    });
    let mut journal = Journal::open(dir.journal_file())?;
    let mut restart = None;
    for kind in [ChangeKind::NeboaiModels, ChangeKind::ProxyAccess, ChangeKind::ApiServer] {
        let outcome = journal.revert(&install, None, kind)?;
        released.conflicts.extend(outcome.conflicts);
        restart = restart.or(outcome.restart);
    }
    for profile in &install.profiles {
        let outcome = journal.revert(&install, Some(&profile.name), ChangeKind::ApiServer)?;
        released.conflicts.extend(outcome.conflicts);
        restart = restart.or(outcome.restart);
    }
    // An install the link itself had started is not running now; it reads
    // the restored config when the owner next starts it.
    if let Some(command) = restart.filter(|_| !runtime_released)
        && let Err(e) = install::run(&command, install::COMMAND_WAIT, &dir.log(None)).await
    {
        released.not_restarted = Some(e.to_string());
    }
    Ok(released)
}

/// What unlinking did: each install the bot hosted, released.
pub struct Unlinked {
    pub installs: Vec<(Hosted, Released)>,
}

/// Stops and removes the bot's service, gives back every install it
/// hosted, and forgets the bot's credentials and state.
///
/// The owner's unlink stops the service first, so it can't dial NeboAI (and
/// rotate the token) while configs are restored. On revocation this process
/// is the service: removing the service can end it, so that goes last,
/// after the record `nebo-link status` reports the removal from.
pub async fn unlink(root: &Root, link: &Link, by: By) -> Result<Unlinked> {
    let dir = root.bot(&link.bot_id);
    if by == By::Owner {
        service::uninstall(&link.bot_id, false)?;
    }
    let mut unlinked = Unlinked { installs: Vec::new() };
    for agent in &link.agents {
        if let Some(settings) = agent.install() {
            let released = release(&dir.agent(&agent.id), agent.runtime, settings).await?;
            unlinked.installs.push((agent.clone(), released));
        }
    }
    Credentials::open(&dir).forget()?;
    dir.remove()?;
    if by == By::Revocation {
        dir.create()?;
        write_json(
            &dir.removed_file(),
            &Removed {
                bot_id: link.bot_id.clone(),
                name: link.name.clone(),
            },
        )?;
        service::uninstall(&link.bot_id, true)?;
    }
    Ok(unlinked)
}

/// Where a coding agent works: `dir` (relative to where the command runs),
/// or `~/NeboAI/<agent>`, made if missing. Never the whole disk.
fn workdir(agent: acp::Agent, dir: Option<PathBuf>, env: &Environment) -> Result<PathBuf> {
    let workdir = match dir {
        Some(dir) if dir.is_absolute() => dir,
        Some(dir) => std::env::current_dir().map_err(|e| Error::Message(e.to_string()))?.join(dir),
        None => {
            let home = env
                .home
                .clone()
                .ok_or_else(|| Error::Message("this user has no home folder to work in".into()))?;
            default_workdir(&home, agent.name())
        }
    };
    if workdir.parent().is_none() {
        return Err(Error::Message(format!(
            "{} can't work in {}. Choose a project folder with --dir.",
            agent.name(),
            workdir.display()
        )));
    }
    std::fs::create_dir_all(&workdir).map_err(|e| Error::io(&workdir, e))?;
    workdir.canonicalize().map_err(|e| Error::io(&workdir, e))
}

/// Where a coding agent works when the owner names no folder:
/// `~/NeboAI/<agent>`.
pub fn default_workdir(home: &Path, agent: &str) -> PathBuf {
    home.join("NeboAI").join(agent)
}

/// How long a coding agent's first start may take (`npx` may be fetching
/// its adapter).
const ACP_FIRST_START: std::time::Duration = std::time::Duration::from_secs(180);

/// Starts the coding agent once in `workdir`, proving it speaks ACP, and
/// returns what it calls itself.
async fn probe(agent: acp::Agent, install: &Installation, workdir: &Path) -> Result<Option<String>> {
    let mut probe = tokio::process::Command::new(&install.restart.program);
    probe
        .args(&install.restart.args)
        .envs(install.restart.env.iter().cloned())
        .current_dir(workdir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let mut child = probe
        .spawn()
        .map_err(|e| Error::Message(format!("Could not start {}: {e}", agent.name())))?;
    let conn = acp::client::Connection::start(
        child.stdout.take().expect("piped"),
        child.stdin.take().expect("piped"),
        Box::new(|_, _| {}),
    );
    let answered = tokio::time::timeout(
        ACP_FIRST_START,
        conn.request("initialize", acp::protocol::initialize_params("nebo-link", crate::update::VERSION)),
    )
    .await;
    let _ = child.kill().await;
    match answered {
        Ok(Ok(result)) => Ok(acp::protocol::Initialized::parse(&result).title),
        Ok(Err(e)) => Err(Error::Message(format!(
            "{} did not start in ACP mode ({e}). Run `{}` yourself to see why.",
            agent.name(),
            install::shown(&install.restart)
        ))),
        Err(_) => Err(Error::Message(format!(
            "{} did not answer in ACP mode. Run `{}` yourself to see why.",
            agent.name(),
            install::shown(&install.restart)
        ))),
    }
}

/// This machine's name, as Nebo reports it.
pub fn host_label() -> String {
    let from_env = std::env::var("COMPUTERNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .ok()
        .filter(|s| !s.trim().is_empty());
    let name = from_env.or_else(|| {
        std::process::Command::new("hostname")
            .output()
            .ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
            .filter(|s| !s.is_empty())
    });
    name.map(|n| n.trim_end_matches(".local").to_string())
        .unwrap_or_default()
}

/// The `--home` the service must be given: set only when the state root is
/// not the default one.
fn root_override(root: &Root) -> Option<PathBuf> {
    let default = dirs::data_dir().map(|d| d.join("nebo-link"));
    (default.as_deref() != Some(root.path())).then(|| root.path().to_path_buf())
}

/// A loopback port nothing is listening on now.
fn free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| Error::Message(format!("no free local port: {e}")))?;
    listener
        .local_addr()
        .map(|a| a.port())
        .map_err(|e| Error::Message(format!("no free local port: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebo_runtimes::acp::Agent;

    fn acp_agent(id: &str, runtime: Runtime, workdir: &str) -> Hosted {
        Hosted {
            id: id.into(),
            label: runtime_name(runtime).into(),
            runtime,
            via: Via::Acp(AcpLink {
                program: "p".into(),
                args: vec![],
                env: vec![],
                workdir: workdir.into(),
            }),
        }
    }

    #[test]
    fn a_coding_agent_works_in_its_own_folder_under_home() {
        let dir = default_workdir(Path::new("/Users/me"), "Claude Code");
        assert_eq!(dir, PathBuf::from("/Users/me/NeboAI/Claude Code"));
    }

    /// The first agent is `assistant` under the runtime's name; one more of
    /// the same runtime is named for its folder; ids are slugs, unique, and
    /// never `assistant` again.
    #[test]
    fn labels_and_ids_of_added_agents() {
        let claude = Runtime::Acp(Agent::ClaudeCode);
        let codex = Runtime::Acp(Agent::Codex);
        assert_eq!(new_id(&[], "Claude Code"), PRIMARY);
        let one = vec![acp_agent(PRIMARY, claude, "/w/claude-code")];
        assert_eq!(default_label(&one, codex, "Codex", Path::new("/w/codex")), "Codex");
        assert_eq!(new_id(&one, "Codex"), "codex");
        assert_eq!(
            default_label(&one, claude, "Claude Code", Path::new("/w/claude-code-2")),
            "Claude Code · claude-code-2"
        );
        assert_eq!(new_id(&one, "Claude Code · claude-code-2"), "claude-code-claude-code-2");
        let two = vec![acp_agent(PRIMARY, claude, "/w/a"), acp_agent("site", claude, "/w/b")];
        assert_eq!(new_id(&two, "Site"), "site-2");
        assert_eq!(new_id(&two, "Assistant"), "assistant-2");
        assert_eq!(new_id(&two, "···"), "agent");
    }

    #[test]
    fn secrets_are_long_and_distinct() {
        let (a, b) = (secret(), secret());
        assert_eq!(a.len(), 64);
        assert_ne!(a, b);
    }

    #[test]
    fn the_proxy_change_names_the_owner_and_the_bot_prefix() {
        let settings = InstallLink {
            home: "/h".into(),
            env: vec![],
            local_password: "pw".into(),
            models: ModelsEndpoint {
                port: 1,
                key: "k".into(),
                enabled: false,
            },
            api_server_key: String::new(),
            services: vec![],
        };
        let link = Link {
            bot_id: "b1".into(),
            name: "n".into(),
            owner_id: "owner-1".into(),
            endpoints: Endpoints::from_env(),
            agents: vec![Hosted {
                id: PRIMARY.into(),
                label: "OpenClaw".into(),
                runtime: Runtime::Openclaw,
                via: Via::Install(settings.clone()),
            }],
        };
        let access = proxy_access(&link, &settings);
        assert_eq!(access.base_path, "/t/b1");
        assert_eq!(access.identity, "owner-1");
        assert_eq!(access.origin, "https://neboai.com");
        assert_eq!(access.user_header, "x-nebo-user");
        assert_eq!(access.password, "pw");
    }
}
