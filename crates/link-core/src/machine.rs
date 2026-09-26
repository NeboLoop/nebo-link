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

use std::path::{Path, PathBuf};

/// The file inside a linked bot's folder that makes it linked.
pub const LINK_FILE: &str = "link.json";

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

#[cfg(test)]
mod tests {
    use super::*;

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
}
