//! The link's operations on a bot: pairing, NeboAI models on and off, the
//! chat contract, and unlinking. Each is one function the CLI and the
//! running service share.

use std::path::PathBuf;
use std::sync::Arc;

use nebo_runtimes::{
    ApiServer, Change, ChangeKind, Environment, Installation, Journal, NeboaiModels, ProxyAccess, Runtime,
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
use crate::state::{BotDir, Link, ModelsEndpoint, Removed, Root, write_json};

/// The bot's purpose as the hub records it.
const PURPOSE: &str = "linked";

/// The header OpenClaw reads the signed-in owner from (trusted-proxy auth).
const USER_HEADER: &str = "x-nebo-user";

/// What pairing did.
pub struct Paired {
    pub link: Link,
    /// Set when the agent could not be restarted to pick up its new settings
    /// (for one, a gateway started by hand in a terminal): the owner restarts
    /// it; everything else is in place.
    pub restart_failed: Option<String>,
}

/// Pairs the local runtime with the owner's NeboAI account using `code`,
/// opens it to the link, and installs and starts the service.
pub async fn pair(
    root: &Root,
    code: &str,
    runtime: Option<Runtime>,
    name: Option<String>,
    exe: PathBuf,
) -> Result<Paired> {
    let linked: Vec<PathBuf> = root.links()?.into_iter().map(|l| l.home).collect();
    let install = install::choose(detect(&Environment::current()), runtime, &linked)?;
    let endpoints = Endpoints::from_env();
    let bot_id = uuid::Uuid::new_v4().to_string();
    let name = name.unwrap_or_else(|| default_name(&host_label(), install.runtime));

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
    };
    dir.save(&link)?;
    // Linked again: what `status` said about its removal no longer applies.
    for removed in root.removed()?.into_iter().filter(|r| r.home == link.home) {
        root.bot(&removed.bot_id).remove()?;
    }

    let finish = async {
        Credentials::open(&dir).save(&resp.connection_token)?;
        let mut journal = Journal::open(dir.journal_file())?;
        let outcome = journal.apply(&install, None, &Change::ProxyAccess(proxy_access(&link)))?;
        let restart = apply_api_server(&mut journal, &install, &link)?.or(outcome.restart);
        service::install(&service::Spec {
            bot_id: bot_id.clone(),
            exe,
            home: root_override(root),
            path: std::env::var("PATH").ok(),
        })?;
        // Last, and not fatal: the link is running and connects as soon as
        // the agent comes back with its new settings.
        let restart_failed = match restart {
            Some(command) => install::restart(&command, install::RESTART_WAIT).await.err().map(|e| e.to_string()),
            None => None,
        };
        Ok::<_, Error>(restart_failed)
    };
    match finish.await {
        Ok(restart_failed) => Ok(Paired { link, restart_failed }),
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
/// link's key (journaled, restarted when that changed), or OpenClaw's
/// gateway reached as the link's own operator socket, and a backend on it.
/// `Err` says why the contract can't be served for this install, for
/// `nebo-link status`; nothing is then announced.
pub async fn chat(
    dir: &BotDir,
    link: &mut Link,
    install: &Installation,
    token: watch::Receiver<String>,
) -> std::result::Result<Arc<Contract>, String> {
    let backend: Arc<dyn contract::backend::Backend> = match link.runtime {
        Runtime::Hermes => Arc::new(hermes_backend(dir, link, install).await?),
        Runtime::Openclaw => {
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
        && let Err(e) = install::restart(&command, install::RESTART_WAIT).await
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
            install::restart(command, install::RESTART_WAIT).await?;
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
        conflicts: Vec::new(),
        not_restored: None,
        not_restarted: None,
    };
    match install::find(link) {
        Ok(install) => {
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
            if let Some(command) = restart
                && let Err(e) = install::restart(&command, install::RESTART_WAIT).await
            {
                unlinked.not_restarted = Some(e.to_string());
            }
        }
        Err(e) => unlinked.not_restored = Some(e.to_string()),
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
    match host.trim() {
        "" => runtime_name(runtime).to_string(),
        host => format!("{host} · {}", runtime_name(runtime)),
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
        };
        let access = proxy_access(&link);
        assert_eq!(access.base_path, "/t/b1");
        assert_eq!(access.identity, "owner-1");
        assert_eq!(access.origin, "https://neboai.com");
        assert_eq!(access.user_header, "x-nebo-user");
        assert_eq!(access.password, "pw");
    }
}
