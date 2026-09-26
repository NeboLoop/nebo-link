//! The link's operations on a bot: pairing, NeboAI models on and off, the
//! chat contract, and unlinking. Each is one function the CLI and the
//! running service share.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use nebo_runtimes::{
    acp, ApiServer, Change, ChangeKind, Environment, Installation, Journal, NeboaiModels, ProxyAccess, Runtime,
    RuntimeCommand, Service, detect, openclaw,
};
use tokio::sync::watch;

use crate::contract::{self, Contract, Inbox};
use crate::credentials::Credentials;
use crate::endpoints::{Endpoints, WEB_ORIGIN};
use crate::error::{Error, Result};
use crate::install::{self, runtime_key, runtime_name};
use crate::janus::{self, Janus};
use crate::proxy;
use crate::service;
use crate::state::{AcpLink, BotDir, Link, ModelsEndpoint, Removed, Root, write_json};
use crate::supervise::{self, Supervisor};

/// The bot's purpose as the hub records it.
const PURPOSE: &str = "linked";

/// The header OpenClaw reads the signed-in owner from (trusted-proxy auth).
const USER_HEADER: &str = "x-nebo-user";

/// What pairing did.
pub struct Paired {
    pub link: Link,
    /// The runtime's processes that were not running and the link started.
    pub started: Vec<String>,
    /// Set when the agent could not be restarted to pick up its new settings
    /// (for one, a gateway started by hand in a terminal): the owner restarts
    /// it; everything else is in place.
    pub restart_failed: Option<String>,
}

/// Pairs the local runtime with the owner's NeboAI account using `code`,
/// opens it to the link, and installs and starts the service.
///
/// An ACP agent is linked when named (`runtime`), or by its command
/// (`acp_command`, any agent that speaks ACP); `workdir` is the folder its
/// conversations work in, [`default_workdir`] when not given. It is started
/// once before the code is spent, so an agent that can't run is refused
/// while the code is still good.
pub async fn pair(
    root: &Root,
    code: &str,
    runtime: Option<Runtime>,
    acp_command: Option<String>,
    workdir: Option<PathBuf>,
    name: Option<String>,
    exe: PathBuf,
) -> Result<Paired> {
    let linked: Vec<PathBuf> = root.links()?.into_iter().map(|l| l.home).collect();
    let env = Environment::current();
    let (installs, runtime) = match &acp_command {
        Some(command) => (
            vec![acp::custom(command, &env).map_err(Error::Message)?],
            Some(Runtime::Acp(acp::Agent::Other)),
        ),
        None => (detect(&env), runtime),
    };
    let install = install::choose(installs, runtime, &linked)?;
    let acp_link = match install.runtime.acp() {
        Some(agent) => Some(acp_link(agent, &install, workdir, &env).await?),
        None => None,
    };
    let endpoints = Endpoints::from_env();
    let bot_id = uuid::Uuid::new_v4().to_string();
    let name = name.unwrap_or_else(|| match &acp_link {
        Some(acp) => default_name_as(&host_label(), &acp.name),
        None => default_name(&host_label(), install.runtime),
    });

    let resp = nebo_comm::api::redeem_code(
        &endpoints.api,
        code.trim(),
        &name,
        PURPOSE,
        &bot_id,
        runtime_key(install.runtime),
    )
    .await
    .map_err(|e| Error::Message(format!("Could not link with that code: {e}")))?;

    let dir = root.bot(&bot_id);
    dir.create()?;
    let link = Link {
        bot_id: bot_id.clone(),
        name: resp.name.clone(),
        runtime: install.runtime,
        owner_id: resp.id.clone(),
        home: install.home.clone(),
        env: install.restart.env.clone(),
        endpoints,
        local_password: secret(),
        models: ModelsEndpoint {
            port: free_port()?,
            key: secret(),
            enabled: false,
        },
        api_server_key: secret(),
        services: Vec::new(),
        acp: acp_link,
    };
    dir.save(&link)?;
    // Linked again: what `status` said about its removal no longer applies.
    for removed in root.removed()?.into_iter().filter(|r| r.home == link.home) {
        root.bot(&removed.bot_id).remove()?;
    }

    let finish = async {
        Credentials::open(&dir).save(&resp.connection_token)?;
        if link.acp.is_some() {
            // Nothing of the agent's to change or keep running: the
            // service starts it and keeps it up.
            service::install(&service::Spec {
                bot_id: bot_id.clone(),
                exe,
                home: root_override(root),
                path: std::env::var("PATH").ok(),
            })?;
            return Ok::<_, Error>((Vec::new(), None));
        }
        let supervisor = Supervisor::new(&dir, install.runtime, install.processes.clone());
        let running = supervisor.running().await;
        let mut journal = Journal::open(dir.journal_file())?;
        let outcome = journal.apply(&install, None, &Change::ProxyAccess(proxy_access(&link)))?;
        let restart = apply_api_server(&mut journal, &install, &link)?.or(outcome.restart);
        // What was not running is started now, onto the new settings; the
        // service keeps it up from here.
        let started = supervisor.ensure().await;
        service::install(&service::Spec {
            bot_id: bot_id.clone(),
            exe,
            home: root_override(root),
            path: std::env::var("PATH").ok(),
        })?;
        // Last, and not fatal: the link is running and connects as soon as
        // the agent comes back with its new settings. Only a runtime that was
        // already running needs restarting onto them.
        let was_running = install.processes.first().is_some_and(|p| running.contains(&p.name));
        let restart_failed = match restart.filter(|_| was_running) {
            Some(command) => install::run(&command, install::COMMAND_WAIT, &dir.runtime_log(runtime_key(install.runtime)))
                .await
                .err()
                .map(|e| e.to_string()),
            None => None,
        };
        Ok::<_, Error>((started, restart_failed))
    };
    match finish.await {
        Ok((started, restart_failed)) => Ok(Paired {
            link: dir.load()?,
            started,
            restart_failed,
        }),
        Err(e) => Err(Error::Message(format!(
            "{e}\nThe bot is paired but not running. Fix the problem above and run `nebo-link run --bot {bot_id}`, or undo it with `nebo-link unlink --bot {bot_id}`."
        ))),
    }
}

/// The change that serves the runtime's UI behind the link.
pub fn proxy_access(link: &Link) -> ProxyAccess {
    ProxyAccess {
        base_path: link.base_path(),
        origin: WEB_ORIGIN.to_string(),
        user_header: USER_HEADER.to_string(),
        identity: link.owner_id.clone(),
        password: link.local_password.clone(),
    }
}

/// Turns on the runtime's API server for the chat contract, for the default
/// profile and every named one (each needs its own key under Hermes'
/// multiplexing). Returns the restart the runtime needs to read it, if any.
pub fn apply_api_server(journal: &mut Journal, install: &Installation, link: &Link) -> Result<Option<RuntimeCommand>> {
    let change = Change::ApiServer(ApiServer {
        key: link.api_server_key.clone(),
    });
    let mut restart = journal.apply(install, None, &change)?.restart;
    for profile in &install.profiles {
        restart = restart.or(journal.apply(install, Some(&profile.name), &change)?.restart);
    }
    Ok(restart)
}

/// The chat contract for `link`: Hermes' API server turned on with the
/// link's key (journaled, restarted when that changed), OpenClaw's gateway
/// reached as the link's own operator socket, or an ACP agent run by its
/// saved command, and a backend on it. `install` is `None` for an ACP agent.
/// `Err` says why the contract can't be served for this install, for
/// `nebo-link status`; nothing is then announced.
pub async fn chat(
    dir: &BotDir,
    link: &mut Link,
    install: Option<&Installation>,
    token: watch::Receiver<String>,
) -> std::result::Result<Arc<Contract>, String> {
    let no_install = || format!("{} was not found on this computer", runtime_name(link.runtime));
    let backend: Arc<dyn contract::backend::Backend> = match link.runtime {
        Runtime::Acp(agent) => {
            let acp = link
                .acp
                .as_ref()
                .ok_or_else(|| format!("{} has no saved command; link it again", runtime_name(link.runtime)))?;
            Arc::new(contract::acp::Acp::new(contract::acp::Settings {
                agent,
                name: acp.name.clone(),
                command: acp.command(),
                workdir: acp.workdir.clone(),
                log: dir.runtime_log(runtime_key(link.runtime)),
                chats_file: dir.acp_chats_file(),
            }))
        }
        Runtime::Hermes => {
            let install = install.ok_or_else(no_install)?;
            Arc::new(hermes_backend(dir, link, install).await?)
        }
        Runtime::Openclaw => {
            let install = install.ok_or_else(no_install)?;
            let gateway = install::ui_addr(install)
                .ok_or_else(|| "OpenClaw has no gateway to reach".to_owned())?;
            Arc::new(contract::openclaw::Openclaw::new(
                openclaw::gateway::Connect::new(format!("ws://{gateway}"), &proxy_access(link), proxy::FORWARDED_FOR),
                openclaw::gateway::FileDeviceStore::new(dir.device_file()),
            ))
        }
    };
    let inbox = Inbox::new(&link.endpoints.api, &link.bot_id, token);
    Ok(Contract::new(
        runtime_key(link.runtime),
        runtime_name(link.runtime),
        &link.bot_id,
        backend,
        Some(inbox),
    ))
}

/// Hermes' API server turned on with the link's key, and a backend on it.
async fn hermes_backend(
    dir: &BotDir,
    link: &mut Link,
    install: &Installation,
) -> std::result::Result<contract::hermes::Hermes, String> {
    if link.api_server_key.is_empty() {
        link.api_server_key = secret();
        dir.save(link).map_err(|e| e.to_string())?;
    }
    let mut journal = Journal::open(dir.journal_file()).map_err(|e| e.to_string())?;
    let restart = apply_api_server(&mut journal, install, link)
        .map_err(|e| format!("could not turn on the {} API server: {e}", runtime_name(link.runtime)))?;
    if let Some(command) = restart
        && let Err(e) = install::run(&command, install::COMMAND_WAIT, &dir.runtime_log(runtime_key(link.runtime))).await
    {
        tracing::info!(error = %e, "the runtime was not restarted onto its API server key");
    }
    // Detected again: the API server's endpoint appears once its key is in
    // place, unless the config keeps it off.
    let install = install::find(link).map_err(|e| e.to_string())?;
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
    Ok(contract::hermes::Hermes::new(&format!("http://{}", default.addr), &link.api_server_key, profiles))
}

/// The models endpoint as seen from `link` with `token`.
pub fn janus(link: &Link, token: watch::Receiver<String>) -> Janus {
    Janus {
        url: link.endpoints.janus.clone(),
        bot_id: link.bot_id.clone(),
        token,
        key: link.models.key.clone(),
        client: janus::client(),
    }
}

/// What turning models on or off did.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelsChange {
    pub enabled: bool,
    /// The runtime was restarted to pick the change up.
    pub restarted: bool,
    /// Settings the owner changed since the link set them, left as they are.
    pub conflicts: Vec<String>,
}

/// Points the runtime's models at NeboAI (`enabled`) or restores the
/// provider it had before.
pub async fn set_models(dir: &BotDir, link: &mut Link, janus: &Janus, enabled: bool) -> Result<ModelsChange> {
    if link.runtime.acp().is_some() {
        return Err(Error::Message(format!(
            "{} runs on its own sign-in; NeboAI models are for OpenClaw and Hermes.",
            runtime_name(link.runtime)
        )));
    }
    let install = install::find(link)?;
    let mut journal = Journal::open(dir.journal_file())?;
    let outcome = if enabled {
        let (models, default_model) = janus::models(janus).await.map_err(Error::Message)?;
        let change = Change::NeboaiModels(NeboaiModels {
            base_url: format!("http://127.0.0.1:{}/v1", link.models.port),
            api_key: link.models.key.clone(),
            models,
            default_model,
        });
        journal.apply(&install, None, &change)?
    } else {
        journal.revert(&install, None, ChangeKind::NeboaiModels)?
    };
    let restarted = match &outcome.restart {
        Some(command) => {
            install::run(command, install::COMMAND_WAIT, &dir.runtime_log(runtime_key(link.runtime))).await?;
            true
        }
        None => false,
    };
    link.models.enabled = enabled;
    dir.save(link)?;
    Ok(ModelsChange {
        enabled,
        restarted,
        conflicts: outcome.conflicts,
    })
}

/// Who unlinks a bot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum By {
    /// The owner, with `nebo-link unlink`.
    Owner,
    /// The bot's own service, because NeboAI removed the bot.
    Revocation,
}

/// What unlinking did.
pub struct Unlinked {
    /// The runtime processes the link had started and its services the
    /// link had installed, now stopped and removed.
    pub released: supervise::Released,
    /// Settings the owner changed since the link set them, left as they are.
    pub conflicts: Vec<String>,
    /// Why the runtime's config could not be restored, when it could not.
    pub not_restored: Option<String>,
    /// Why the runtime could not be restarted onto its restored config, when
    /// it could not.
    pub not_restarted: Option<String>,
}

/// Stops and removes the bot's service, restores the runtime's config,
/// and forgets the bot's credentials and state.
///
/// The owner's unlink stops the service first, so it can't dial NeboAI (and
/// rotate the token) while the config is restored. On revocation this
/// process is the service: removing the service can end it, so that goes
/// last, after the record `nebo-link status` reports the removal from.
pub async fn unlink(root: &Root, link: &Link, by: By) -> Result<Unlinked> {
    let dir = root.bot(&link.bot_id);
    if by == By::Owner {
        service::uninstall(&link.bot_id, false)?;
    }
    let mut unlinked = Unlinked {
        released: supervise::Released::default(),
        conflicts: Vec::new(),
        not_restored: None,
        not_restarted: None,
    };
    // An ACP agent had nothing changed and nothing started outside the
    // service, which is removed below.
    let found = match link.runtime.acp() {
        Some(_) => None,
        None => Some(install::find(link)),
    };
    match found {
        None => {}
        Some(Ok(install)) => {
            // What the link started or installed goes first, so nothing of
            // the link's is running on the config being restored.
            unlinked.released = supervise::release(&dir, link.runtime, &link.services, &install.processes).await;
            let runtime_released = install.processes.first().is_some_and(|p| {
                unlinked.released.stopped.contains(&p.name) || unlinked.released.uninstalled.contains(&p.name)
            });
            let mut journal = Journal::open(dir.journal_file())?;
            let mut restart = None;
            for kind in [ChangeKind::NeboaiModels, ChangeKind::ProxyAccess, ChangeKind::ApiServer] {
                let outcome = journal.revert(&install, None, kind)?;
                unlinked.conflicts.extend(outcome.conflicts);
                restart = restart.or(outcome.restart);
            }
            for profile in &install.profiles {
                let outcome = journal.revert(&install, Some(&profile.name), ChangeKind::ApiServer)?;
                unlinked.conflicts.extend(outcome.conflicts);
                restart = restart.or(outcome.restart);
            }
            // A runtime the link itself had started is not running now;
            // it reads the restored config when the owner next starts it.
            if let Some(command) = restart.filter(|_| !runtime_released)
                && let Err(e) = install::run(&command, install::COMMAND_WAIT, &dir.runtime_log(runtime_key(link.runtime))).await
            {
                unlinked.not_restarted = Some(e.to_string());
            }
        }
        Some(Err(e)) => unlinked.not_restored = Some(e.to_string()),
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
                runtime: link.runtime,
                home: link.home.clone(),
            },
        )?;
        service::uninstall(&link.bot_id, true)?;
    }
    Ok(unlinked)
}

/// "studio-mac · OpenClaw".
pub fn default_name(host: &str, runtime: Runtime) -> String {
    default_name_as(host, runtime_name(runtime))
}

/// "studio-mac · Claude Code": the machine's name and the agent's.
fn default_name_as(host: &str, agent: &str) -> String {
    match host.trim() {
        "" => agent.to_string(),
        host => format!("{host} · {agent}"),
    }
}

/// Where an ACP agent works when the owner names no folder:
/// `~/NeboAI/<agent>`, created at pairing. Never the whole disk.
pub fn default_workdir(home: &Path, agent: &str) -> PathBuf {
    home.join("NeboAI").join(agent)
}

/// How long pairing waits for an ACP agent's first start (`npx` may be
/// fetching its adapter).
const ACP_FIRST_START: std::time::Duration = std::time::Duration::from_secs(180);

/// The settings an ACP agent is linked with: its working folder (made if
/// missing), its saved command, and its name, from its own `initialize`
/// answer when it is not one Nebo Link knows. Starting it here proves it
/// speaks ACP before the pairing code is spent.
async fn acp_link(agent: acp::Agent, install: &Installation, workdir: Option<PathBuf>, env: &Environment) -> Result<AcpLink> {
    let home = env
        .home
        .clone()
        .ok_or_else(|| Error::Message("this user has no home folder to work in".into()))?;
    let workdir = match workdir {
        Some(dir) if dir.is_absolute() => dir,
        Some(dir) => std::env::current_dir().map_err(|e| Error::Message(e.to_string()))?.join(dir),
        None => default_workdir(&home, agent.name()),
    };
    if workdir.parent().is_none() {
        return Err(Error::Message(format!(
            "{} can't work in {}. Choose a project folder with --dir.",
            agent.name(),
            workdir.display()
        )));
    }
    std::fs::create_dir_all(&workdir).map_err(|e| Error::io(&workdir, e))?;
    let workdir = workdir.canonicalize().map_err(|e| Error::io(&workdir, e))?;
    let mut probe = tokio::process::Command::new(&install.restart.program);
    probe
        .args(&install.restart.args)
        .envs(install.restart.env.iter().cloned())
        .current_dir(&workdir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true);
    let title = {
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
            Ok(Ok(result)) => acp::protocol::Initialized::parse(&result).title,
            Ok(Err(e)) => {
                return Err(Error::Message(format!(
                    "{} did not start in ACP mode ({e}). Run `{}` yourself to see why.",
                    agent.name(),
                    install::shown(&install.restart)
                )));
            }
            Err(_) => {
                return Err(Error::Message(format!(
                    "{} did not answer in ACP mode. Run `{}` yourself to see why.",
                    agent.name(),
                    install::shown(&install.restart)
                )));
            }
        }
    };
    Ok(AcpLink {
        name: match agent {
            acp::Agent::Other => title.unwrap_or_else(|| agent.name().to_owned()),
            known => known.name().to_owned(),
        },
        program: install.restart.program.clone(),
        args: install.restart.args.clone(),
        env: install.restart.env.clone(),
        workdir,
    })
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

/// 256 random bits as hex.
fn secret() -> String {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("os rng");
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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

    #[test]
    fn default_names() {
        assert_eq!(default_name("studio-mac", Runtime::Openclaw), "studio-mac · OpenClaw");
        assert_eq!(default_name("  ", Runtime::Hermes), "Hermes");
    }

    #[test]
    fn a_coding_agent_works_in_its_own_folder_under_home() {
        let dir = default_workdir(Path::new("/Users/me"), "Claude Code");
        assert_eq!(dir, PathBuf::from("/Users/me/NeboAI/Claude Code"));
        assert_eq!(default_name_as("studio-mac", "Claude Code"), "studio-mac · Claude Code");
    }

    #[test]
    fn secrets_are_long_and_distinct() {
        let (a, b) = (secret(), secret());
        assert_eq!(a.len(), 64);
        assert_ne!(a, b);
    }

    #[test]
    fn the_proxy_change_names_the_owner_and_the_bot_prefix() {
        let link = Link {
            bot_id: "b1".into(),
            name: "n".into(),
            runtime: Runtime::Openclaw,
            owner_id: "owner-1".into(),
            home: "/h".into(),
            env: vec![],
            endpoints: Endpoints::from_env(),
            local_password: "pw".into(),
            models: ModelsEndpoint {
                port: 1,
                key: "k".into(),
                enabled: false,
            },
            api_server_key: String::new(),
            services: vec![],
            acp: None,
        };
        let access = proxy_access(&link);
        assert_eq!(access.base_path, "/t/b1");
        assert_eq!(access.identity, "owner-1");
        assert_eq!(access.origin, "https://neboai.com");
        assert_eq!(access.user_header, "x-nebo-user");
        assert_eq!(access.password, "pw");
    }
}
