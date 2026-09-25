//! `nebo-link run --bot <id>`: the service. One process is one bot (the hub's
//! bot lease is per process). It keeps the comms connection (presence) and
//! the tunnel up, serves the local proxy the tunnel delivers to, and serves
//! the NeboAI models endpoint.
//!
//! Reconnects follow Nebo's watchers: a connection that drops is redialed at
//! once, repeated failures back off from 30 s to 10 min, a refused lease is
//! asked for again at the renewal cadence, and a wake from sleep forces a
//! fresh connection.
//!
//! A bot NeboAI removed is refused for good: CONNECT answers that it was
//! revoked, or the hub closes its tunnel with 1008 "revoked". The service
//! then stops retrying and unlinks it the way `nebo-link unlink` does.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use nebo_comm::{CommError, CommPlugin, NeboAIPlugin, lease};
use tokio::sync::watch;

use crate::credentials::Credentials;
use crate::error::{Error, Result};
use crate::install::{self, runtime_key, runtime_name};
use crate::janus::Janus;
use crate::link::{self, By, ModelsChange};
use crate::offsets::FileOffsets;
use crate::proxy::{self, Control, Target};
use crate::state::{BotDir, Link, Root, STATUS_EVERY, Status};

/// Longest wait between failed connection attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(600);
/// First wait after a failed attempt.
const FIRST_BACKOFF: Duration = Duration::from_secs(30);
/// A tick this much later than scheduled means the machine slept.
const SLEEP_DRIFT: Duration = Duration::from_secs(10);

/// Shared by the service's tasks.
struct Service {
    dir: BotDir,
    janus: Janus,
    /// Serializes model toggles.
    toggling: tokio::sync::Mutex<()>,
    online: watch::Receiver<bool>,
    tunnel: Arc<AtomicBool>,
    error: Mutex<Option<String>>,
}

impl Service {
    fn status(&self) -> Status {
        let online = *self.online.borrow();
        Status {
            pid: std::process::id(),
            updated: unix_now(),
            online,
            tunnel: self.tunnel.load(Ordering::Relaxed),
            error: if online { None } else { self.error.lock().expect("error lock").clone() },
        }
    }

    fn write_status(&self) {
        if let Err(e) = self.dir.save_status(&self.status()) {
            tracing::warn!(error = %e, "could not write status");
        }
    }
}

// The link's file is the one record of its settings: the CLI changes it too
// (`nebo-link models`), so every use reads it afresh.
impl Control for Service {
    fn status(&self) -> serde_json::Value {
        let link = match self.dir.load() {
            Ok(link) => link,
            Err(e) => return serde_json::json!({ "error": e.to_string() }),
        };
        let status = Service::status(self);
        serde_json::json!({
            "botId": link.bot_id,
            "name": link.name,
            "runtime": runtime_key(link.runtime),
            "version": env!("CARGO_PKG_VERSION"),
            "online": status.online,
            "tunnel": status.tunnel,
            "models": { "enabled": link.models.enabled },
        })
    }

    async fn set_models(&self, enabled: bool) -> std::result::Result<serde_json::Value, String> {
        let _one_at_a_time = self.toggling.lock().await;
        let change: Result<ModelsChange> = match self.dir.load() {
            Ok(mut link) => link::set_models(&self.dir, &mut link, &self.janus, enabled).await,
            Err(e) => Err(e),
        };
        match change {
            Ok(change) => {
                tracing::info!(enabled, restarted = change.restarted, "models toggled");
                Ok(serde_json::to_value(change).expect("serializes"))
            }
            Err(e) => {
                tracing::warn!(enabled, error = %e, "models toggle failed");
                Err(e.to_string())
            }
        }
    }
}

pub async fn run(root: &Root, bot_id: &str) -> Result<()> {
    let dir = root.bot(bot_id);
    let link = dir.load()?;
    let credentials = Credentials::open(&dir);
    let (token_tx, token_rx) = watch::channel(credentials.load()?);
    let (online_tx, online_rx) = watch::channel(false);
    let tunnel = Arc::new(AtomicBool::new(false));

    let install = install::find(&link)?;
    let upstream = install::ui_addr(&install)
        .ok_or_else(|| Error::Message(format!("{} has no local UI to open", runtime_name(link.runtime))))?;
    let access = link::proxy_access(&link);
    let target = Target {
        upstream,
        base_path: access.base_path.clone(),
        route: access.route(link.runtime),
        identity: access.identity.clone(),
        runtime_name: runtime_name(link.runtime),
    };

    let proxy_listener = proxy::bind_loopback(([127, 0, 0, 1], 0).into())
        .await
        .map_err(|e| Error::Message(format!("could not open the local proxy: {e}")))?;
    let proxy_addr = proxy_listener
        .local_addr()
        .map_err(|e| Error::Message(format!("could not open the local proxy: {e}")))?;
    let models_listener = proxy::bind_loopback(([127, 0, 0, 1], link.models.port).into())
        .await
        .map_err(|e| {
            Error::Message(format!(
                "could not open the NeboAI models endpoint on 127.0.0.1:{}: {e}",
                link.models.port
            ))
        })?;

    let service = Arc::new(Service {
        dir: dir.clone(),
        janus: link::janus(&link, token_rx.clone()),
        toggling: tokio::sync::Mutex::new(()),
        online: online_rx.clone(),
        tunnel: tunnel.clone(),
        error: Mutex::new(None),
    });

    tokio::spawn(proxy::serve(
        proxy_listener,
        target,
        nebo_comm::tunnel::tunnel_auth_secret().to_string(),
        service.clone(),
    ));
    tokio::spawn(crate::janus::serve(models_listener, service.janus.clone()));
    let revoked = Arc::new(tokio::sync::Notify::new());
    tokio::spawn(tunnel_watcher(
        link.endpoints.tunnel.clone(),
        proxy_addr.to_string(),
        token_rx.clone(),
        online_rx,
        tunnel,
        revoked.clone(),
    ));
    let status_writer = service.clone();
    let status_writer = tokio::spawn(async move {
        loop {
            status_writer.write_status();
            tokio::time::sleep(STATUS_EVERY).await;
        }
    });
    tracing::info!(bot = %link.bot_id, runtime = runtime_key(link.runtime), "nebo-link started");

    let plugin = NeboAIPlugin::new(Arc::new(FileOffsets::open(dir.offsets_file())));
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut backoff = FIRST_BACKOFF;
    loop {
        // Built before the match: a `borrow()` in the scrutinee would hold the
        // token's read lock through the arms, and `send_replace` below would
        // wait on it forever (the hub rotates the token on every connect).
        let config = connect_config(&link, &token_rx.borrow());
        let delay = match plugin.connect(config).await {
            Ok(()) => {
                // The hub rotated the token and the old one is dead: save the
                // new one before anything else can dial with it.
                if let Some(token) = plugin.take_rotated_token().await {
                    if let Err(e) = credentials.save(&token) {
                        tracing::error!(error = %e, "could not save the rotated bot token");
                    }
                    token_tx.send_replace(token);
                }
                *service.error.lock().expect("error lock") = None;
                online_tx.send_replace(true);
                service.write_status();
                tracing::info!("connected to NeboAI");
                backoff = FIRST_BACKOFF;
                let ended = connected(&plugin, &mut shutdown, &revoked).await;
                online_tx.send_replace(false);
                service.write_status();
                match ended {
                    Ended::Dropped => Duration::ZERO,
                    Ended::Shutdown => {
                        lease::process().release();
                        let _ = plugin.disconnect().await;
                        return stopped(&dir);
                    }
                    Ended::Revoked => break,
                }
            }
            Err(CommError::Revoked) => break,
            Err(CommError::LeaseHeld) => {
                record_error(&service, "another nebo-link process is running this bot");
                lease::RENEW_EVERY
            }
            Err(e) => {
                record_error(&service, &e.to_string());
                let delay = backoff;
                backoff = (backoff * 2).min(MAX_BACKOFF);
                delay
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = &mut shutdown => return stopped(&dir),
        }
    }

    tracing::warn!("NeboAI removed this bot; unlinking it");
    status_writer.abort();
    let _ = plugin.disconnect().await;
    let unlinked = link::unlink(root, &link, By::Revocation).await?;
    if let Some(reason) = unlinked.not_restored {
        tracing::warn!(reason, "the agent's config was not restored");
    }
    if let Some(reason) = unlinked.not_restarted {
        tracing::warn!(reason, "the agent was not restarted onto its restored config");
    }
    if !unlinked.conflicts.is_empty() {
        tracing::info!(settings = ?unlinked.conflicts, "left as the owner changed them");
    }
    tracing::info!("unlinked");
    Ok(())
}

/// Why the service stopped waiting on a connection.
enum Ended {
    /// The connection dropped or the machine woke from sleep: reconnect.
    Dropped,
    /// The service is shutting down.
    Shutdown,
    /// NeboAI removed the bot.
    Revoked,
}

/// Waits while connected.
async fn connected(
    plugin: &NeboAIPlugin,
    shutdown: &mut std::pin::Pin<&mut impl Future<Output = ()>>,
    revoked: &tokio::sync::Notify,
) -> Ended {
    let tick = STATUS_EVERY;
    loop {
        let before = SystemTime::now();
        tokio::select! {
            _ = plugin.wait_disconnect() => {
                tracing::info!("disconnected from NeboAI; reconnecting");
                return Ended::Dropped;
            }
            _ = tokio::time::sleep(tick) => {
                let slept = SystemTime::now()
                    .duration_since(before)
                    .unwrap_or_default()
                    .saturating_sub(tick)
                    > SLEEP_DRIFT;
                if slept {
                    tracing::info!("woke from sleep; reconnecting");
                    let _ = plugin.disconnect().await;
                    return Ended::Dropped;
                }
                if !plugin.is_connected() {
                    return Ended::Dropped;
                }
            }
            _ = shutdown.as_mut() => return Ended::Shutdown,
            _ = revoked.notified() => return Ended::Revoked,
        }
    }
}

/// Keeps the tunnel up while the comms connection is. Only the process
/// holding the bot's lease may hold its tunnel, and the token is read after
/// the connection that rotated it has saved it. Ends, notifying `revoked`,
/// when the hub says the bot was removed.
async fn tunnel_watcher(
    hub_url: String,
    local_addr: String,
    token: watch::Receiver<String>,
    mut online: watch::Receiver<bool>,
    tunnel: Arc<AtomicBool>,
    revoked: Arc<tokio::sync::Notify>,
) {
    let mut backoff = FIRST_BACKOFF;
    loop {
        if online.wait_for(|on| *on).await.is_err() {
            return;
        }
        lease::process().granted_or_unleased().await;
        let token = token.borrow().clone();
        let started = Instant::now();
        match nebo_comm::tunnel::run(&hub_url, &token, &local_addr, &tunnel).await {
            Ok(()) => tracing::info!("tunnel closed by NeboAI; redialing"),
            Err(nebo_comm::tunnel::TunnelError::Revoked) => {
                tracing::info!("tunnel closed: NeboAI removed this bot");
                revoked.notify_one();
                return;
            }
            Err(e) => tracing::info!(error = %e, "tunnel dropped"),
        }
        // A tunnel that lived a while earns a quick redial; repeated fast
        // failures back off.
        let delay = if started.elapsed() > Duration::from_secs(60) {
            backoff = FIRST_BACKOFF;
            Duration::from_secs(5)
        } else {
            let delay = backoff;
            backoff = (backoff * 2).min(MAX_BACKOFF);
            delay
        };
        tokio::time::sleep(delay).await;
    }
}

fn connect_config(link: &Link, token: &str) -> HashMap<String, String> {
    HashMap::from([
        ("gateway".to_string(), link.endpoints.comms.clone()),
        ("api_server".to_string(), link.endpoints.api.clone()),
        ("bot_id".to_string(), link.bot_id.clone()),
        ("token".to_string(), token.to_string()),
        ("platform".to_string(), std::env::consts::OS.to_string()),
        ("hostname".to_string(), link::host_label()),
        ("runtime".to_string(), runtime_key(link.runtime).to_string()),
    ])
}

fn stopped(dir: &BotDir) -> Result<()> {
    dir.clear_status();
    tracing::info!("nebo-link stopped");
    Ok(())
}

fn record_error(service: &Service, error: &str) {
    tracing::info!(error, "could not connect to NeboAI");
    *service.error.lock().expect("error lock") = Some(error.to_string());
    service.write_status();
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::Endpoints;
    use crate::state::ModelsEndpoint;

    #[test]
    fn connect_announces_the_runtime() {
        let link = Link {
            bot_id: "b1".into(),
            name: "n".into(),
            runtime: nebo_runtimes::Runtime::Hermes,
            owner_id: "o".into(),
            home: "/h".into(),
            env: vec![],
            endpoints: Endpoints::from_env(),
            local_password: "pw".into(),
            models: ModelsEndpoint {
                port: 1,
                key: "k".into(),
                enabled: false,
            },
        };
        let config = connect_config(&link, "jwt");
        assert_eq!(config["runtime"], "hermes");
        assert_eq!(config["bot_id"], "b1");
        assert_eq!(config["token"], "jwt");
        assert_eq!(config["gateway"], link.endpoints.comms);
        assert!(!config.contains_key("data_dir"), "the token is never cached outside the credential store");
    }
}
