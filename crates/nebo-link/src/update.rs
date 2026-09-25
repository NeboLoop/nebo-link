//! Self-update, through Nebo's updater (`nebo-updater`) and nebo-link's own
//! signed release feed.
//!
//! Every release publishes `SHA256SUMS` and `SHA256SUMS.sig`, an ed25519
//! signature by the release key. An update is applied only when that
//! signature verifies against the key this build was made with and the
//! binary's hash matches the signed list. A build made without a key (a
//! local build) cannot update itself.
//!
//! `nebo-link update` replaces the binary and restarts every bot's service.
//! The service checks daily and, when a newer release is out, becomes it in
//! place (same process, so launchd and systemd keep supervising it). Bot
//! services share one binary: the update lock in the data root lets only one
//! of them replace it, and one that finds the binary already replaced by
//! another restarts into it instead of replacing it again.

use std::borrow::Cow;
use std::path::{Path, PathBuf};
use std::time::Duration;

use nebo_updater::{ApplyMode, Feed};
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::state::Root;

/// Where nebo-link's releases are published.
pub const CDN: &str = "https://cdn.neboai.com/nebo-link/releases";

/// This build's version.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// How often the service checks for a newer release.
pub const CHECK_EVERY: Duration = Duration::from_secs(24 * 60 * 60);

/// What `nebo-link update` says in a build that has no release key.
pub const NO_KEY: &str = "This build of nebo-link can't update itself. Install it from https://neboai.com/link.sh.";

/// The release public key this build was made with: base64 of the raw 32-byte
/// ed25519 key, from `NEBO_LINK_RELEASE_KEY` at build time.
pub fn release_key() -> Option<[u8; 32]> {
    option_env!("NEBO_LINK_RELEASE_KEY").and_then(decode_key)
}

/// Decodes a base64 raw 32-byte ed25519 public key.
pub fn decode_key(base64_key: &str) -> Option<[u8; 32]> {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD
        .decode(base64_key.trim())
        .ok()?
        .try_into()
        .ok()
}

/// nebo-link's release feed at `base_url`, verified with `key`.
pub fn feed(base_url: impl Into<Cow<'static, str>>, key: [u8; 32]) -> Feed {
    Feed {
        base_url: base_url.into(),
        binary: Cow::Borrowed("nebo-link"),
        checksums: Cow::Borrowed("SHA256SUMS"),
        signing_key: Some(key),
    }
}

/// The official feed, when this build can update itself.
pub fn official() -> Option<Feed> {
    release_key().map(|key| feed(CDN, key))
}

/// Why this install can't replace its own binary, if it can't: installs a
/// package manager owns are updated there.
pub fn not_self_updating() -> Option<&'static str> {
    match nebo_updater::detect_install_method() {
        "direct" => None,
        "homebrew" => Some("nebo-link was installed with Homebrew. Update it with `brew upgrade nebo-link`."),
        _ => Some("nebo-link was installed by your package manager. Update it there."),
    }
}

/// A newer release, downloaded and verified.
#[derive(Debug)]
pub struct Downloaded {
    /// The release tag, e.g. `v0.1.1`.
    pub tag: String,
    /// The verified binary, in the temp directory.
    pub path: PathBuf,
}

impl Downloaded {
    /// The version without the tag's `v`.
    pub fn version(&self) -> &str {
        self.tag.trim_start_matches('v')
    }
}

/// Checks `feed` for a release newer than `current`; downloads and verifies
/// it when there is one. Nothing that fails verification is kept.
pub async fn fetch(feed: &Feed, current: &str) -> Result<Option<Downloaded>> {
    let failed = |e: nebo_updater::UpdateError| Error::Message(format!("could not update nebo-link: {e}"));
    let check = nebo_updater::check(feed, current).await.map_err(failed)?;
    if !check.available {
        return Ok(None);
    }
    let tag = check.latest_version;
    let path = nebo_updater::download(feed, &tag, None).await.map_err(failed)?;
    if let Err(e) = nebo_updater::verify_checksum(feed, &path, &tag).await {
        let _ = std::fs::remove_file(&path);
        return Err(failed(e));
    }
    Ok(Some(Downloaded { tag, path }))
}

/// Replaces the binary with a verified download and returns; the caller
/// restarts what runs it (`nebo-link update`).
pub fn replace(downloaded: &Downloaded, root: &Root) -> Result<()> {
    nebo_updater::apply_update(&downloaded.path, root.path(), ApplyMode::ReplaceOnly)
        .map_err(|e| Error::Message(format!("could not replace the nebo-link binary: {e}")))
}

/// Held while one process updates the binary. Released when dropped, when
/// the process ends and when it becomes the new binary (the lock file is not
/// inherited across exec).
#[derive(Debug)]
pub struct Lock {
    _file: std::fs::File,
}

/// Waits for the update lock in `root`.
pub async fn lock(root: &Root) -> Result<Lock> {
    let dir = root.path().to_path_buf();
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&dir).map_err(|e| Error::io(&dir, e))?;
        let path = dir.join("update.lock");
        let file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&path)
            .map_err(|e| Error::io(&path, e))?;
        file.lock().map_err(|e| Error::io(&path, e))?;
        Ok(Lock { _file: file })
    })
    .await
    .map_err(|e| Error::Message(format!("update lock: {e}")))?
}

/// The version of the `nebo-link` binary at `exe`, from `exe --version`.
pub fn version_of(exe: &Path) -> Option<String> {
    let output = std::process::Command::new(exe).arg("--version").output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .last()
        .map(|v| v.trim_start_matches('v').to_string())
}

/// What the service does to run a newer release.
#[derive(Debug)]
pub enum Staged {
    /// Replace the binary with this verified download, then become it.
    Downloaded(Downloaded, Lock),
    /// Another bot's service (or `nebo-link update`) already replaced the
    /// binary: become the one on disk.
    OnDisk(Lock),
}

/// The service's update check: every [`CHECK_EVERY`], and when a newer
/// release is out, hands the service what to run next. `exe` is the binary
/// the service was started as.
pub async fn watch(root: Root, exe: PathBuf, feed: Feed, staged: mpsc::Sender<Staged>) {
    if let Some(why) = not_self_updating() {
        tracing::info!("{why}");
        return;
    }
    let (available_tx, mut available) = mpsc::unbounded_channel();
    let checker = nebo_updater::BackgroundChecker::new(feed.clone(), VERSION.to_string(), CHECK_EVERY, move |r| {
        let _ = available_tx.send(r.latest_version);
    });
    tokio::spawn(async move { checker.run(tokio_util::sync::CancellationToken::new()).await });
    while let Some(latest) = available.recv().await {
        tracing::info!(current = VERSION, latest, "a newer nebo-link is out");
        match stage(&root, &exe, &feed).await {
            Ok(Some(next)) => {
                if staged.send(next).await.is_err() {
                    return;
                }
            }
            Ok(None) => {}
            Err(e) => tracing::warn!(error = %e, "update failed"),
        }
    }
}

async fn stage(root: &Root, exe: &Path, feed: &Feed) -> Result<Option<Staged>> {
    let lock = lock(root).await?;
    if version_of(exe).is_some_and(|on_disk| on_disk != VERSION) {
        return Ok(Some(Staged::OnDisk(lock)));
    }
    Ok(fetch(feed, VERSION).await?.map(|downloaded| Staged::Downloaded(downloaded, lock)))
}

/// Becomes the new release: this process is replaced by it (on Windows, a new
/// process takes over and this one exits). Returns only on failure. `exe` is
/// the binary the service was started as.
pub fn restart(staged: Staged, root: &Root, exe: &Path) -> Error {
    match staged {
        Staged::Downloaded(downloaded, _lock) => {
            tracing::info!(version = downloaded.version(), "updating nebo-link");
            match nebo_updater::apply_update(&downloaded.path, root.path(), ApplyMode::Restart) {
                Ok(()) => Error::Message("the update returned without restarting".into()),
                Err(e) => Error::Message(format!("could not update nebo-link: {e}")),
            }
        }
        Staged::OnDisk(_lock) => {
            tracing::info!("restarting as the updated nebo-link");
            become_binary(exe)
        }
    }
}

#[cfg(unix)]
fn become_binary(exe: &Path) -> Error {
    use std::os::unix::process::CommandExt;
    let e = std::process::Command::new(exe).args(std::env::args_os().skip(1)).exec();
    Error::io(exe, e)
}

#[cfg(windows)]
fn become_binary(exe: &Path) -> Error {
    match std::process::Command::new(exe).args(std::env::args_os().skip(1)).spawn() {
        Ok(_) => std::process::exit(0),
        Err(e) => Error::io(exe, e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_feed_is_nebo_links_signed_release_feed() {
        let f = feed(CDN, [7; 32]);
        assert_eq!(f.base_url, "https://cdn.neboai.com/nebo-link/releases");
        assert_eq!(f.checksums, "SHA256SUMS");
        assert_eq!(f.signing_key, Some([7; 32]));
        let asset = nebo_updater::asset_name(&f);
        let os = match std::env::consts::OS {
            "macos" => "darwin",
            other => other,
        };
        let arch = match std::env::consts::ARCH {
            "aarch64" => "arm64",
            "x86_64" => "amd64",
            other => other,
        };
        let exe = if cfg!(windows) { ".exe" } else { "" };
        assert_eq!(asset, format!("nebo-link-{os}-{arch}{exe}"));
    }

    #[test]
    fn keys_decode_from_base64_of_32_bytes() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode([9u8; 32]);
        assert_eq!(decode_key(&b64), Some([9; 32]));
        assert_eq!(decode_key(&format!(" {b64}\n")), Some([9; 32]));
        let short = base64::engine::general_purpose::STANDARD.encode([9u8; 31]);
        assert_eq!(decode_key(&short), None);
        assert_eq!(decode_key("not base64!"), None);
    }

    #[test]
    fn a_build_without_a_key_has_no_feed() {
        assert_eq!(official().is_some(), option_env!("NEBO_LINK_RELEASE_KEY").is_some());
    }
}
