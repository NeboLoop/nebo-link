//! `nebo-link run --bot <id>`: the service. One process is one bot (the hub's
//! bot lease is per process), hosting every agent the machine's link has.
//! It keeps the comms connection (presence) and the tunnel up, serves the
//! local proxy the tunnel delivers to (the chat contract for every agent,
//! and the first OpenClaw or Hermes install's UI), and serves each install's
//! NeboAI models endpoint.
//!
//! Reconnects follow Nebo's watchers (`nebo_comm::reconnect`): a hub drain
//! (1012) is redialed within 3 s without advancing the backoff, any other
//! drop or failed dial waits a jittered backoff from under 1 s up to 30 s
//! that starts over once a connection stayed up 10 s, a refused lease is
//! asked for again at the renewal cadence, and a wake from sleep forces a
//! fresh connection.
//!
//! A bot NeboAI removed is refused for good: CONNECT answers that it was
//! revoked, or the hub closes its tunnel with 1008 "revoked". The service
//! then stops retrying and unlinks it the way `nebo-link unlink` does.
//!
//! Each install's own processes (a Hermes gateway and dashboard, an OpenClaw
//! gateway) are kept up by `supervise`. A coding agent's process is started
//! when it is first asked for and started again after it exits. Whether the
//! chat contract can be served is probed on a timer and whenever an
//! install's process comes up; when the answer changes, the service
//! reconnects so CONNECT announces `chat` as it is now.
//!
//! `nebo-link add` and `remove` change the link's file; the service reads it
//! again when it changes, and a coding agent joins or leaves the roster
//! without a restart (an install restarts the service, from the CLI).
//!
//! The service checks for a newer nebo-link every six hours, soon after each
//! (re)connect, and when NeboAI announces a release (a `software_update` on
//! the bot's `installs` stream); when one is out it hands back the bot's
//! lease, disconnects and becomes the new release in place (see `update`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use link_core::host::Host;
use link_core::phone::Contract;
use nebo_comm::reconnect::{Backoff, Disconnect};
use nebo_comm::{CommError, CommPlugin, NeboAIPlugin, lease};
use nebo_runtimes::{Installation, Runtime, acp};
use tokio::sync::watch;

use crate::credentials::Credentials;
use crate::error::{Error, Result};
use crate::install::{self, runtime_key, runtime_name};
use crate::link::{self, By, ModelsChange};
use crate::offsets::FileOffsets;
use crate::proxy::{self, Control, Target};
use crate::state::{BotDir, Hosted, Link, Oal, Root, STATUS_EVERY, Status};
use crate::supervise::Supervisor;
use crate::update::{self, Staged};

/// A tick this much later than scheduled means the machine slept.
const SLEEP_DRIFT: Duration = Duration::from_secs(10);
/// How often the chat contract's readiness is probed between process starts.
const CHAT_PROBE_EVERY: Duration = Duration::from_secs(30);
/// How often the link's file is looked at for agents added or removed.
const RELOAD_EVERY: Duration = Duration::from_secs(2);
/// How long a prompt still running gets to finish when the service stops or
/// restarts, before its agent is stopped anyway (and its session resumes
/// on the next message).
pub const AGENT_GRACE: Duration = Duration::from_secs(60);

/// Shared by the service's tasks.
struct Service {
    dir: BotDir,
    /// The link as last read: what CONNECT announces. The host's keeper
    /// updates it with each agent the host adds or removes.
    link: Arc<Mutex<Link>>,
    token: watch::Receiver<String>,
    /// Serializes model toggles.
    toggling: tokio::sync::Mutex<()>,
    online: watch::Receiver<bool>,
    tunnel: Arc<AtomicBool>,
    error: Mutex<Option<String>>,
    contract: Arc<Contract>,
    host: Arc<Host>,
    /// Why the chat contract is not announced, when it is not.
    chat_error: Mutex<Option<String>>,
    /// What the current connection's CONNECT said about `chat`.
    announced: AtomicBool,
    supervisors: Vec<Arc<Supervisor>>,
    /// The bot's Open Agent Link host, when it could start.
    oal: Option<crate::oal::Service>,
    /// How long a running prompt gets to finish when this run of the service
    /// stops ([`crate::service::refresh`]).
    grace: Duration,
}

impl Service {
    /// Stops the service's work as it stops or restarts: Open Agent Link
    /// first (its connections and listeners close, so no prompt comes in),
    /// then the agents: a prompt still running gets the service's grace to
    /// finish, then every agent pauses, its sessions kept for the next start.
    /// Nothing they started outlives the service. [`Stop::AtOnce`] gives a
    /// running prompt no grace.
    async fn stop(&self, stop: Stop) {
        self.close_oal();
        let grace = match stop {
            Stop::WithGrace => self.grace,
            Stop::AtOnce => Duration::ZERO,
        };
        self.host.shutdown(grace).await;
    }

    /// Closes every Open Agent Link connection with 1001: clients reconnect
    /// once the service is back.
    fn close_oal(&self) {
        if let Some(oal) = &self.oal {
            oal.shutdown();
        }
    }

    fn status(&self) -> Status {
        let online = *self.online.borrow();
        let chat_error = self.chat_error.lock().expect("chat lock").clone();
        Status {
            pid: std::process::id(),
            updated: unix_now(),
            online,
            tunnel: self.tunnel.load(Ordering::Relaxed),
            error: if online { None } else { self.error.lock().expect("error lock").clone() },
            chat: chat_error.is_none(),
            chat_error,
            processes: self.supervisors.iter().flat_map(|s| s.status()).collect(),
            oal: self.oal.as_ref().map(crate::oal::Service::status).unwrap_or_default(),
        }
    }

    /// Whether the chat contract can be announced now: an agent answers the
    /// link with everything the contract needs.
    async fn chat_ready(&self) -> bool {
        let result = self.contract.ready().await;
        let ready = result.is_ok();
        *self.chat_error.lock().expect("chat lock") = result.err();
        ready
    }

    fn write_status(&self) {
        if let Err(e) = self.dir.save_status(&self.status()) {
            tracing::warn!(error = %e, "could not write status");
        }
    }

    /// Reads the link again after it changed: coding agents added join the
    /// roster and those removed leave it; the others keep their backend,
    /// process and sessions. Returns whether what CONNECT announces changed.
    fn reload(&self) -> bool {
        let link = match self.dir.load() {
            Ok(link) => link,
            Err(e) => {
                tracing::warn!(error = %e, "could not read the link again");
                return false;
            }
        };
        let before = self.link.lock().expect("link lock").clone();
        if link == before {
            return false;
        }
        let members = link::reconcile(&self.dir, &before, &link, &self.host.roster().members());
        tracing::info!(
            agents = ?link.agents.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(),
            "the link's agents changed"
        );
        self.host.set_members(members);
        let announce_changed = link.runtime() != before.runtime();
        *self.link.lock().expect("link lock") = link;
        announce_changed
    }
}

// The link's file is the one record of its settings: the CLI changes it too
// (`nebo-link models`, `add`, `remove`), so every use reads it afresh.
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
            "runtime": link.runtime().map(runtime_key),
            "agents": link.agents.iter().map(|a| serde_json::json!({
                "id": a.id,
                "label": a.label,
                "runtime": runtime_key(a.runtime),
                "folder": a.acp().map(|acp| acp.workdir.display().to_string()),
            })).collect::<Vec<_>>(),
            "version": env!("CARGO_PKG_VERSION"),
            "online": status.online,
            "tunnel": status.tunnel,
            "models": { "enabled": link::models_enabled(&link) },
            "chat": { "enabled": status.chat, "error": status.chat_error },
            "processes": status.processes,
        })
    }

    fn oal(&self) -> Option<Arc<oal_host::OalHost>> {
        self.oal.as_ref().map(|oal| oal.host.clone())
    }

    async fn set_models(&self, enabled: bool) -> std::result::Result<serde_json::Value, String> {
        let _one_at_a_time = self.toggling.lock().await;
        match link::set_models(&self.dir, self.token.clone(), enabled).await {
            Ok(change) => {
                let change: ModelsChange = change;
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

/// Runs the bot's service. `oal` overrides the link's Open Agent Link
/// settings where it sets them (the command line's `--relay`, `--lan`).
pub async fn run(root: &Root, bot_id: &str, oal: Oal) -> Result<()> {
    // The binary this service was started as, read before anything can
    // replace it: an update restarts into what is at this path.
    let exe = std::env::current_exe()
        .and_then(|p| p.canonicalize())
        .map_err(|e| Error::Message(format!("could not locate the nebo-link binary: {e}")))?;
    let dir = root.bot(bot_id);
    // The service's definition as this version writes it; how long a
    // running prompt may take when this run stops follows from the one it
    // was started with.
    let grace = crate::service::refresh(&crate::service::Spec {
        bot_id: bot_id.to_owned(),
        exe: exe.clone(),
        home: link::root_override(root),
        path: std::env::var("PATH").ok(),
        logs: dir.logs_dir(),
    });
    let link = dir.load()?;
    let credentials = Credentials::open(&dir);
    let (token_tx, token_rx) = watch::channel(credentials.load()?);
    let (online_tx, online_rx) = watch::channel(false);
    let tunnel = Arc::new(AtomicBool::new(false));

    // Each OpenClaw and Hermes install the bot hosts, found as it was
    // linked; one that is gone is left out, and says so.
    let mut installs: Vec<(String, Installation)> = Vec::new();
    for agent in &link.agents {
        let Some(settings) = agent.install() else { continue };
        match install::find(agent.runtime, settings) {
            Ok(install) => installs.push((agent.id.clone(), install)),
            Err(e) => tracing::warn!(agent = %agent.id, error = %e, "this install is not served"),
        }
    }
    // The UI the bot opens: its first install's that has one. Coding agents
    // have none; their only pages are the chat contract.
    let ui = installs.iter().find_map(|(id, install)| {
        let agent = link.agent(id)?;
        Some((agent.runtime, agent.install()?, install::ui_addr(install)?))
    });
    let target = match ui {
        Some((runtime, settings, upstream)) => {
            let access = link::proxy_access(&link, settings);
            Target {
                upstream: Some(upstream),
                base_path: access.base_path.clone(),
                route: access.route(runtime),
                identity: access.identity.clone(),
                runtime_name: runtime_name(runtime),
            }
        }
        None => {
            let runtime = link.runtime().unwrap_or(Runtime::Acp(acp::Agent::Other));
            Target {
                upstream: None,
                base_path: link.base_path(),
                route: nebo_runtimes::ProxyAccess {
                    base_path: link.base_path(),
                    origin: String::new(),
                    user_header: String::new(),
                    identity: link.owner_id.clone(),
                    password: String::new(),
                }
                .route(runtime),
                identity: link.owner_id.clone(),
                runtime_name: runtime_name(runtime),
            }
        }
    };

    let proxy_listener = proxy::bind_loopback(([127, 0, 0, 1], 0).into())
        .await
        .map_err(|e| Error::Message(format!("could not open the local proxy: {e}")))?;
    let proxy_addr = proxy_listener
        .local_addr()
        .map_err(|e| Error::Message(format!("could not open the local proxy: {e}")))?;
    // Each install's NeboAI models endpoint, on the port its config names.
    for (id, _) in &installs {
        let Some(settings) = link.agent(id).and_then(Hosted::install) else { continue };
        let listener = proxy::bind_loopback(([127, 0, 0, 1], settings.models.port).into())
            .await
            .map_err(|e| {
                Error::Message(format!(
                    "could not open the NeboAI models endpoint on 127.0.0.1:{}: {e}",
                    settings.models.port
                ))
            })?;
        tokio::spawn(crate::janus::serve(listener, link::janus(&link, settings, token_rx.clone())));
    }

    let (contract, host) = link::chat(&dir, &link, &installs, token_rx.clone()).await;
    // Agents added and removed through the host (Open Agent Link, the phone
    // contract) go into the link's file, as the CLI's do.
    let current = Arc::new(Mutex::new(link.clone()));
    host.set_keeper(Arc::new(link::Keeper::new(&dir, Some(current.clone()))));
    let settings = Oal {
        relay: oal.relay.or(link.oal.relay.clone()),
        lan: oal.lan.or(link.oal.lan.clone()),
    };
    let oal = match crate::oal::start(&dir, &link, host.clone(), settings).await {
        Ok(oal) => Some(oal),
        Err(e) => {
            tracing::error!(error = %e, "open agent link could not start; NeboAI still reaches the agents");
            None
        }
    };
    let supervisors: Vec<Arc<Supervisor>> = installs
        .iter()
        .filter_map(|(id, install)| {
            let agent = link.agent(id)?;
            Some(Supervisor::new(&dir.agent(id), agent.runtime, install.processes.clone()))
        })
        .collect();
    for supervisor in &supervisors {
        tokio::spawn(supervisor.clone().run());
    }
    let service = Arc::new(Service {
        dir: dir.clone(),
        link: current,
        token: token_rx.clone(),
        toggling: tokio::sync::Mutex::new(()),
        online: online_rx.clone(),
        tunnel: tunnel.clone(),
        error: Mutex::new(None),
        contract: contract.clone(),
        host,
        chat_error: Mutex::new(None),
        announced: AtomicBool::new(false),
        supervisors: supervisors.clone(),
        oal,
        grace,
    });

    tokio::spawn(proxy::serve(
        proxy_listener,
        target,
        nebo_comm::tunnel::tunnel_auth_secret().to_string(),
        service.clone(),
        Some(contract),
    ));
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
    // Chat readiness, re-probed on a timer and the moment an install's
    // process comes up; a change from what CONNECT announced makes the
    // service reconnect, so the hub learns it without an unlink.
    let chat_changed = Arc::new(tokio::sync::Notify::new());
    let came_up = Arc::new(tokio::sync::Notify::new());
    for supervisor in &supervisors {
        let mut up = supervisor.came_up();
        let came_up = came_up.clone();
        tokio::spawn(async move {
            while up.changed().await.is_ok() {
                came_up.notify_one();
            }
        });
    }
    let prober = service.clone();
    let changed = chat_changed.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(CHAT_PROBE_EVERY) => {}
                _ = came_up.notified() => {}
            }
            let ready = prober.chat_ready().await;
            prober.write_status();
            if ready != prober.announced.load(Ordering::Relaxed) {
                tracing::info!(chat = ready, "chat readiness changed; announcing it");
                changed.notify_one();
            }
        }
    });
    // Agents added and removed: the link's file, looked at for changes.
    let reloader = service.clone();
    let changed = chat_changed.clone();
    let link_file = dir.link_file();
    tokio::spawn(async move {
        let modified = || std::fs::metadata(&link_file).and_then(|m| m.modified()).ok();
        let mut seen = modified();
        loop {
            tokio::time::sleep(RELOAD_EVERY).await;
            let now = modified();
            if now == seen {
                continue;
            }
            seen = now;
            let announce = reloader.reload();
            let ready = reloader.chat_ready().await;
            reloader.write_status();
            if announce || ready != reloader.announced.load(Ordering::Relaxed) {
                changed.notify_one();
            }
        }
    });
    tracing::info!(
        bot = %link.bot_id,
        agents = ?link.agents.iter().map(|a| a.id.as_str()).collect::<Vec<_>>(),
        version = update::VERSION,
        "nebo-link started"
    );
    let (staged_tx, mut staged) = tokio::sync::mpsc::channel(1);
    // Releases NeboAI announces, and each (re)connect, nudge the update check.
    let (nudge_tx, nudges) = tokio::sync::mpsc::unbounded_channel();
    match update::official() {
        Some(feed) => {
            let host = service.host.clone();
            let idle: nebo_updater::IdleFn = Box::new(move || {
                let idle = host.turns().is_empty();
                Box::pin(async move { idle })
            });
            tokio::spawn(update::watch(root.clone(), exe.clone(), feed, staged_tx, nudges, idle));
        }
        None => tracing::info!("this build can't update itself (no release key)"),
    }

    let plugin = NeboAIPlugin::new(Arc::new(FileOffsets::open(dir.offsets_file())));
    plugin.set_message_handler(announcements(nudge_tx.clone()));
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let mut backoff = Backoff::new();
    loop {
        // Probed before every connect, so an agent that came up (or went
        // away) since the last one is announced as it is now.
        let chat = service.chat_ready().await;
        service.announced.store(chat, Ordering::Relaxed);
        // Built before the match: a `borrow()` in the scrutinee would hold the
        // token's read lock through the arms, and `send_replace` below would
        // wait on it forever (the hub rotates the token on every connect).
        let config = connect_config(&service.link.lock().expect("link lock"), &token_rx.borrow(), chat);
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
                let _ = nudge_tx.send(nebo_updater::Nudge::Connected);
                service.write_status();
                tracing::info!("connected to NeboAI");
                let connected_at = Instant::now();
                let ended = connected(&plugin, &mut shutdown, &revoked, &chat_changed, &mut staged).await;
                online_tx.send_replace(false);
                service.write_status();
                match ended {
                    Ended::Dropped(how) => backoff.wait(how, connected_at.elapsed()),
                    Ended::Reconnect => Duration::ZERO,
                    Ended::Shutdown(stop) => {
                        lease::process().release();
                        let _ = plugin.disconnect().await;
                        service.stop(stop).await;
                        return stopped(&dir);
                    }
                    Ended::Revoked => break,
                    Ended::Update(next) => {
                        lease::process().release();
                        let _ = plugin.disconnect().await;
                        service.stop(Stop::WithGrace).await;
                        return Err(restart_failed(update::restart(next, root, &exe)));
                    }
                }
            }
            Err(CommError::Revoked) => break,
            Err(CommError::LeaseHeld) => {
                record_error(&service, "another nebo-link process is running this bot");
                lease::RENEW_EVERY
            }
            Err(e) => {
                record_error(&service, &e.to_string());
                backoff.wait(Disconnect::Dropped, Duration::ZERO)
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            stop = &mut shutdown => {
                service.stop(stop).await;
                return stopped(&dir);
            }
            Some(next) = staged.recv() => {
                service.stop(Stop::WithGrace).await;
                return Err(restart_failed(update::restart(next, root, &exe)));
            }
        }
    }

    tracing::warn!("NeboAI removed this bot; unlinking it");
    status_writer.abort();
    let _ = plugin.disconnect().await;
    service.stop(Stop::WithGrace).await;
    let link = service.link.lock().expect("link lock").clone();
    let unlinked = link::unlink(root, &link, By::Revocation).await?;
    for (agent, released) in unlinked.installs {
        if let Some(reason) = released.not_restored {
            tracing::warn!(agent = %agent.id, reason, "the install's config was not restored");
        }
        if let Some(reason) = released.not_restarted {
            tracing::warn!(agent = %agent.id, reason, "the install was not restarted onto its restored config");
        }
        if !released.conflicts.is_empty() {
            tracing::info!(agent = %agent.id, settings = ?released.conflicts, "left as the owner changed them");
        }
    }
    tracing::info!("unlinked");
    Ok(())
}

/// Why the service stopped waiting on a connection.
enum Ended {
    /// The connection dropped (a hub drain or any other drop): reconnect
    /// after the backoff's wait.
    Dropped(Disconnect),
    /// The machine woke from sleep, or what CONNECT should announce changed:
    /// reconnect at once.
    Reconnect,
    /// The service is shutting down.
    Shutdown(Stop),
    /// NeboAI removed the bot.
    Revoked,
    /// A newer nebo-link is ready to run.
    Update(Staged),
}

/// Waits while connected.
async fn connected(
    plugin: &NeboAIPlugin,
    shutdown: &mut std::pin::Pin<&mut impl Future<Output = Stop>>,
    revoked: &tokio::sync::Notify,
    chat_changed: &tokio::sync::Notify,
    staged: &mut tokio::sync::mpsc::Receiver<Staged>,
) -> Ended {
    let tick = STATUS_EVERY;
    loop {
        let before = SystemTime::now();
        tokio::select! {
            how = plugin.wait_disconnect() => {
                tracing::info!(ended = ?how, "disconnected from NeboAI; reconnecting");
                return Ended::Dropped(how);
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
                    return Ended::Reconnect;
                }
                if !plugin.is_connected() {
                    return Ended::Dropped(Disconnect::Dropped);
                }
            }
            stop = shutdown.as_mut() => return Ended::Shutdown(stop),
            _ = revoked.notified() => return Ended::Revoked,
            _ = chat_changed.notified() => {
                tracing::info!("reconnecting to announce chat as it is now");
                let _ = plugin.disconnect().await;
                return Ended::Reconnect;
            }
            Some(next) = staged.recv() => return Ended::Update(next),
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
    let mut backoff = Backoff::new();
    loop {
        if online.wait_for(|on| *on).await.is_err() {
            return;
        }
        lease::process().granted_or_unleased().await;
        let token = token.borrow().clone();
        let started = Instant::now();
        // A hub drain redials within 3 s and leaves the backoff where it
        // was; a tunnel that ended any other way, or a dial that failed,
        // waits the jittered backoff.
        let delay = match nebo_comm::tunnel::run(&hub_url, &token, &local_addr, &tunnel).await {
            Ok(how) => {
                tracing::info!(ended = ?how, "tunnel closed by NeboAI; redialing");
                backoff.wait(how, started.elapsed())
            }
            Err(nebo_comm::tunnel::TunnelError::Revoked) => {
                tracing::info!("tunnel closed: NeboAI removed this bot");
                revoked.notify_one();
                return;
            }
            Err(e @ nebo_comm::tunnel::TunnelError::Mux(_)) => {
                tracing::info!(error = %e, "tunnel dropped");
                backoff.wait(Disconnect::Dropped, started.elapsed())
            }
            Err(e) => {
                tracing::info!(error = %e, "tunnel dropped");
                backoff.wait(Disconnect::Dropped, Duration::ZERO)
            }
        };
        tokio::time::sleep(delay).await;
    }
}

/// What the service takes from the hub's deliveries: release announcements
/// on the `installs` stream, handed to the update check. Everything else the
/// hub sends a bot is Nebo's to act on, not a link's.
fn announcements(nudges: tokio::sync::mpsc::UnboundedSender<nebo_updater::Nudge>) -> nebo_comm::MessageHandler {
    Arc::new(move |msg: nebo_comm::CommMessage| {
        if msg.topic != "installs" {
            return;
        }
        if let Some(announcement) = nebo_updater::Announcement::parse(&msg.content) {
            tracing::info!(product = %announcement.product, version = %announcement.version, urgent = announcement.urgent, "a release was announced");
            let _ = nudges.send(nebo_updater::Nudge::Announced(announcement));
        }
    })
}

/// `chat` announces the chat contract; a link that can't serve it says
/// nothing, and the phone keeps the runtime's own UI.
fn connect_config(link: &Link, token: &str, chat: bool) -> HashMap<String, String> {
    let mut config = HashMap::from([
        ("gateway".to_string(), link.endpoints.comms.clone()),
        ("api_server".to_string(), link.endpoints.api.clone()),
        ("bot_id".to_string(), link.bot_id.clone()),
        ("token".to_string(), token.to_string()),
        ("platform".to_string(), std::env::consts::OS.to_string()),
        ("hostname".to_string(), link::host_label()),
    ]);
    if let Some(runtime) = link.runtime() {
        config.insert("runtime".to_string(), runtime_key(runtime).to_string());
    }
    if chat {
        config.insert("chat".to_string(), "true".to_string());
    }
    config
}

/// An update that could not restart the service into the new version: its
/// work already stopped, the service exits, and its service manager starts
/// it again (as the version on disk).
fn restart_failed(e: Error) -> Error {
    tracing::error!(error = %e, "could not restart as the new version; exiting for the service manager to start it again");
    e
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

/// How the service stops when told to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// Asked to (Ctrl+C, the service manager's SIGTERM): a running prompt
    /// gets the service's grace.
    WithGrace,
    /// Its console host ended: Task Scheduler ended the task, which it does
    /// at once, and a restart's next run starts right away.
    #[cfg_attr(not(windows), allow(dead_code))]
    AtOnce,
}

async fn shutdown_signal() -> Stop {
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
        Stop::WithGrace
    }
    #[cfg(not(unix))]
    {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => Stop::WithGrace,
            _ = crate::service::console_host_gone() => Stop::AtOnce,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::endpoints::Endpoints;
    use crate::state::{AcpLink, InstallLink, ModelsEndpoint, PRIMARY, Via};

    fn delivery(topic: &str, content: &str) -> nebo_comm::CommMessage {
        serde_json::from_value(serde_json::json!({
            "id": "m1", "from": "", "to": "", "topic": topic, "type": "message", "content": content,
        }))
        .unwrap()
    }

    #[test]
    fn release_announcements_reach_the_update_check_and_nothing_else_does() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let handler = announcements(tx);
        let announced = r#"{"type":"software_update","product":"nebo-link","version":"0.1.3","urgent":true,"window_s":60}"#;
        handler(delivery("installs", announced));
        match rx.try_recv().unwrap() {
            nebo_updater::Nudge::Announced(a) => {
                assert_eq!((a.product.as_str(), a.version.as_str(), a.urgent, a.window_s), ("nebo-link", "0.1.3", true, 60));
            }
            other => panic!("unexpected {other:?}"),
        }
        handler(delivery("installs", r#"{"type":"tool_installed","tool_id":"t"}"#));
        handler(delivery("tasks", announced));
        assert!(rx.try_recv().is_err(), "only an announcement on installs nudges");
    }

    #[test]
    fn connect_announces_the_runtime() {
        let mut link = Link {
            bot_id: "b1".into(),
            name: "n".into(),
            owner_id: "o".into(),
            endpoints: Endpoints::from_env(),
            agents: vec![Hosted {
                id: PRIMARY.into(),
                label: "Codex".into(),
                runtime: Runtime::Acp(acp::Agent::Codex),
                via: Via::Acp(AcpLink {
                    program: "codex-acp".into(),
                    args: vec![],
                    env: vec![],
                    workdir: "/w".into(),
                }),
            }],
            oal: Default::default(),
        };
        let config = connect_config(&link, "jwt", false);
        assert_eq!(config["runtime"], "codex");
        assert_eq!(config["bot_id"], "b1");
        assert_eq!(config["token"], "jwt");
        assert_eq!(config["gateway"], link.endpoints.comms);
        assert!(!config.contains_key("data_dir"), "the token is never cached outside the credential store");
        assert!(!config.contains_key("chat"), "a link without the contract announces nothing");
        assert_eq!(connect_config(&link, "jwt", true)["chat"], "true");

        // A bot hosting an install is that install's runtime (its UI is what
        // the bot opens), wherever it sits among the agents.
        link.agents.push(Hosted {
            id: "hermes".into(),
            label: "Hermes".into(),
            runtime: Runtime::Hermes,
            via: Via::Install(InstallLink {
                home: "/h".into(),
                env: vec![],
                local_password: "pw".into(),
                models: ModelsEndpoint {
                    port: 1,
                    key: "k".into(),
                    enabled: false,
                },
                api_server_key: "s".into(),
                services: vec![],
            }),
        });
        assert_eq!(connect_config(&link, "jwt", false)["runtime"], "hermes");
    }
}
