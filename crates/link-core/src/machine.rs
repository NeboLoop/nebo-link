//! One host per computer per OS user. A computer's agents are hosted by one
//! process for each OS user: the nebo-link daemon, or an app that embeds
//! link-core (Nebo). Two would be two identities for one computer, two
//! connections, and the same folder driven twice.
//!
//! The rule: a nebo-link daemon linked for this OS user is the computer's
//! host, from `nebo-link <code>` until `nebo-link unlink`. An app that embeds
//! link-core hosts agents itself only while no daemon is linked; while one
//! is, the app reaches this computer's agents through the daemon like any
//! other computer's. The daemon's state is the record of which it is, so the
//! answer holds across restarts and never depends on which process started
//! first.
//!
//! The other way round: while an app hosts agents for this OS user, it says
//! so in the daemon's home ([`record_app_host`]), and nebo-link refuses to
//! link or pair ([`hosting_app`]) until the app hosts none.
//!
//! While the daemon runs, it also says in its state where it serves its host
//! on this computer ([`record_direct`]), so an app of the same OS user
//! reaches this computer's agents directly ([`direct`]), with no relay
//! between them to fail.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// The file inside a linked bot's folder that makes it linked.
pub const LINK_FILE: &str = "link.json";

/// The file in the daemon's home an app that embeds link-core writes while
/// it hosts this OS user's agents.
pub const APP_HOST_FILE: &str = "app-host.json";

/// The file in a linked bot's `oal/` folder where its running service says
/// where it serves its host on this computer.
pub const DIRECT_FILE: &str = "direct.json";

/// Where a running daemon serves its host on this computer: Open Agent Link
/// at `wss://<addr>/oal` on loopback, with a self-signed certificate. Every
/// connection is end-to-end encrypted as through a relay; the certificate's
/// fingerprint is what a client pins, read here as it would be from
/// `host/info` on the LAN.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Direct {
    pub addr: SocketAddr,
    /// SHA-256 of the certificate's DER, base64url.
    pub fingerprint: String,
}

/// An app hosting this OS user's agents, as it recorded itself.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AppHost {
    /// The app's name ("Nebo").
    pub app: String,
    /// The agents it hosts, by name ("Claude Code", "Codex").
    pub agents: Vec<String>,
}

/// Records, in the daemon's `home`, that `app` hosts `agents` for this OS
/// user; with no agents, takes the record away. The app calls it whenever
/// the agents it hosts change.
pub fn record_app_host(home: &Path, app: &str, agents: &[String]) -> std::io::Result<()> {
    let file = home.join(APP_HOST_FILE);
    if agents.is_empty() {
        return match std::fs::remove_file(&file) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        };
    }
    std::fs::create_dir_all(home)?;
    let record = AppHost {
        app: app.to_owned(),
        agents: agents.to_vec(),
    };
    let tmp = file.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&record).expect("serializes"))?;
    std::fs::rename(&tmp, &file)
}

/// The app hosting this OS user's agents, when one records that it does.
pub fn hosting_app(home: &Path) -> Option<AppHost> {
    let text = std::fs::read_to_string(home.join(APP_HOST_FILE)).ok()?;
    serde_json::from_str::<AppHost>(&text).ok().filter(|host| !host.agents.is_empty())
}

/// Where a nebo-link daemon keeps its state when `--home` doesn't say:
/// `<data dir>/nebo-link`.
pub fn default_daemon_home() -> Option<PathBuf> {
    dirs::data_dir().map(|dir| dir.join("nebo-link"))
}

/// Where this OS user's nebo-link daemon keeps its state: `NEBO_LINK_HOME`,
/// else [`default_daemon_home`].
pub fn daemon_home() -> Option<PathBuf> {
    std::env::var_os("NEBO_LINK_HOME")
        .filter(|home| !home.is_empty())
        .map(PathBuf::from)
        .or_else(default_daemon_home)
}

/// The name of the bot a nebo-link daemon hosts this computer's agents as,
/// when one is linked in `home` (`<home>/<bot id>/link.json`).
pub fn linked_daemon(home: &Path) -> Option<String> {
    let mut names: Vec<String> = std::fs::read_dir(home)
        .ok()?
        .flatten()
        .filter_map(|entry| {
            let text = std::fs::read_to_string(entry.path().join(LINK_FILE)).ok()?;
            let link: serde_json::Value = serde_json::from_str(&text).ok()?;
            Some(link["name"].as_str().unwrap_or("nebo-link").to_owned())
        })
        .collect();
    names.sort();
    names.into_iter().next()
}

/// The bot `bot_dir` names' record of where it serves its host on this
/// computer.
fn direct_file(bot_dir: &Path) -> PathBuf {
    bot_dir.join("oal").join(DIRECT_FILE)
}

/// Records, in the bot's folder `bot_dir`, that its running service serves
/// its host on this computer as `serving`; with `on` false (the service
/// stops), takes the record away if it is still `serving` (a service that
/// started since keeps its own).
pub fn record_direct(bot_dir: &Path, serving: &Direct, on: bool) -> std::io::Result<()> {
    let file = direct_file(bot_dir);
    if !on {
        let ours = std::fs::read_to_string(&file)
            .ok()
            .and_then(|text| serde_json::from_str::<Direct>(&text).ok())
            .is_some_and(|recorded| recorded == *serving);
        return match ours.then(|| std::fs::remove_file(&file)) {
            Some(Err(e)) if e.kind() != std::io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        };
    }
    std::fs::create_dir_all(file.parent().expect("oal folder"))?;
    let tmp = file.with_extension("tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(serving).expect("serializes"))?;
    std::fs::rename(&tmp, &file)
}

/// Where the daemon in `home` serves the host of the bot `bot_id` on this
/// computer, while it runs. A record a daemon that stopped left behind names
/// a port nothing answers on, or a certificate nothing else has: a client
/// finds out at once and goes the other way.
pub fn direct(home: &Path, bot_id: &str) -> Option<Direct> {
    let text = std::fs::read_to_string(direct_file(&home.join(bot_id))).ok()?;
    serde_json::from_str(&text).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_app_hosting_agents_says_so_until_it_hosts_none() {
        let home = tempfile::tempdir().unwrap();
        assert_eq!(hosting_app(home.path()), None);
        record_app_host(home.path(), "Nebo", &["Claude Code".into(), "Codex".into()]).unwrap();
        let host = hosting_app(home.path()).unwrap();
        assert_eq!(host.app, "Nebo");
        assert_eq!(host.agents, ["Claude Code", "Codex"]);
        // The record isn't a linked bot.
        assert_eq!(linked_daemon(home.path()), None);
        record_app_host(home.path(), "Nebo", &[]).unwrap();
        assert_eq!(hosting_app(home.path()), None);
        record_app_host(home.path(), "Nebo", &[]).unwrap();
        // A home that doesn't exist yet is made.
        let later = home.path().join("later");
        record_app_host(&later, "Nebo", &["Codex".into()]).unwrap();
        assert!(hosting_app(&later).is_some());
    }

    #[test]
    fn a_linked_bot_makes_the_daemon_the_host() {
        let home = tempfile::tempdir().unwrap();
        assert_eq!(linked_daemon(home.path()), None);
        assert_eq!(linked_daemon(&home.path().join("missing")), None);
        // A bot NeboAI removed leaves only removed.json: not linked.
        std::fs::create_dir_all(home.path().join("b0")).unwrap();
        std::fs::write(home.path().join("b0").join("removed.json"), "{}").unwrap();
        assert_eq!(linked_daemon(home.path()), None);
        std::fs::create_dir_all(home.path().join("b1")).unwrap();
        std::fs::write(
            home.path().join("b1").join(LINK_FILE),
            r#"{"botId":"b1","name":"Studio Mac"}"#,
        )
        .unwrap();
        assert_eq!(linked_daemon(home.path()).as_deref(), Some("Studio Mac"));
    }

    #[test]
    fn a_running_daemon_says_where_it_serves_its_host_on_this_computer() {
        let home = tempfile::tempdir().unwrap();
        let bot = home.path().join("b1");
        assert_eq!(direct(home.path(), "b1"), None);
        let first = Direct { addr: "127.0.0.1:4100".parse().unwrap(), fingerprint: "f1".into() };
        record_direct(&bot, &first, true).unwrap();
        assert_eq!(direct(home.path(), "b1"), Some(first.clone()));
        assert_eq!(direct(home.path(), "b2"), None);
        // A service that started since keeps its record when the old one stops.
        let second = Direct { addr: "127.0.0.1:4200".parse().unwrap(), fingerprint: "f1".into() };
        record_direct(&bot, &second, true).unwrap();
        record_direct(&bot, &first, false).unwrap();
        assert_eq!(direct(home.path(), "b1"), Some(second.clone()));
        record_direct(&bot, &second, false).unwrap();
        assert_eq!(direct(home.path(), "b1"), None);
        record_direct(&bot, &second, false).unwrap();
        // The record isn't a linked bot.
        assert_eq!(linked_daemon(home.path()), None);
    }
}
