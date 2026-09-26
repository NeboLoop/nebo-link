//! Device keys and the peers they are paired with.
//!
//! Each device (a host, or a client installation) has one X25519 static key
//! pair and a list of paired peers, kept in one directory:
//!
//! - `device.key`: the private key, 32 bytes, followed by the previous
//!   private key (32 more) while a rotation is in progress. Mode 0600 on Unix.
//! - `peers.json`: the paired peers (public keys, ids, names). Mode 0600.
//!
//! One process owns a store directory at a time.

use std::fmt;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand_core::OsRng;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use x25519_dalek::StaticSecret;
use zeroize::Zeroizing;

use crate::error::{Error, Result};

/// An X25519 public key. Written as base64url without padding, as OAL writes
/// keys (spec section 6.1).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PublicKey([u8; 32]);

impl PublicKey {
    /// The key from its 32 bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        PublicKey(bytes)
    }

    /// The key's 32 bytes.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub(crate) fn from_slice(bytes: &[u8]) -> Result<Self> {
        let bytes: [u8; 32] = bytes.try_into().map_err(|_| Error::Protocol("a public key is 32 bytes"))?;
        Ok(PublicKey(bytes))
    }

    fn of(secret: &StaticSecret) -> Self {
        PublicKey(x25519_dalek::PublicKey::from(secret).to_bytes())
    }
}

impl fmt::Display for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&URL_SAFE_NO_PAD.encode(self.0))
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PublicKey({self})")
    }
}

impl FromStr for PublicKey {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let bytes = URL_SAFE_NO_PAD.decode(s).map_err(|_| Error::Protocol("a public key is base64url"))?;
        PublicKey::from_slice(&bytes)
    }
}

impl Serialize for PublicKey {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Which side of OAL a peer is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    /// A client device: it connects to hosts.
    Client,
    /// A host: it runs agents and accepts connections.
    Host,
}

/// A device this one is paired with.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Peer {
    /// The peer's id: the host id when the peer is a host; the device id this
    /// host gave it when the peer is a client.
    pub id: String,
    /// What the peer's owner calls it ("Studio Mac", "Alma's phone").
    pub name: String,
    /// Which side the peer is on.
    pub side: Side,
    /// The peer's static public key: who it is, cryptographically.
    pub public_key: PublicKey,
    /// This device's id in the pairing: the host id on a host; the device id
    /// the host gave this device on a client.
    pub local_id: String,
    /// Which of this device's public keys the peer holds. It differs from
    /// [`KeyStore::public_key`] while a rotation is being announced.
    pub pinned: PublicKey,
    /// When the pairing happened, in seconds since the Unix epoch.
    pub paired_at: u64,
}

/// A device's keys and paired peers. Cloning shares one store.
#[derive(Clone)]
pub struct KeyStore {
    inner: Arc<Inner>,
}

struct Inner {
    dir: PathBuf,
    state: Mutex<State>,
    /// Bumped whenever a peer is removed, so open sessions re-check theirs.
    removed: watch::Sender<u64>,
}

struct State {
    current: StaticSecret,
    previous: Option<StaticSecret>,
    peers: Vec<Peer>,
}

#[derive(Serialize, Deserialize)]
struct PeersFile {
    peers: Vec<Peer>,
}

const KEY_FILE: &str = "device.key";
const PEERS_FILE: &str = "peers.json";

impl KeyStore {
    /// Opens the store in `dir`, making the directory and a new key pair the
    /// first time.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        create_private_dir(&dir)?;
        let key_path = dir.join(KEY_FILE);
        let (current, previous) = if key_path.exists() {
            let bytes = Zeroizing::new(fs::read(&key_path).map_err(store_err)?);
            match bytes.len() {
                32 => (secret(&bytes[..32]), None),
                64 => (secret(&bytes[..32]), Some(secret(&bytes[32..]))),
                _ => return Err(Error::Store(format!("{} is damaged", key_path.display()))),
            }
        } else {
            let current = StaticSecret::random_from_rng(OsRng);
            write_private(&key_path, current.as_bytes())?;
            (current, None)
        };
        let peers_path = dir.join(PEERS_FILE);
        let peers = if peers_path.exists() {
            let text = fs::read(&peers_path).map_err(store_err)?;
            serde_json::from_slice::<PeersFile>(&text)
                .map_err(|e| Error::Store(format!("{} is damaged: {e}", peers_path.display())))?
                .peers
        } else {
            Vec::new()
        };
        Ok(KeyStore {
            inner: Arc::new(Inner {
                dir,
                state: Mutex::new(State { current, previous, peers }),
                removed: watch::Sender::new(0),
            }),
        })
    }

    /// This device's current public key: the one new pairings learn.
    pub fn public_key(&self) -> PublicKey {
        PublicKey::of(&self.lock().current)
    }

    /// Every paired peer.
    pub fn peers(&self) -> Vec<Peer> {
        self.lock().peers.clone()
    }

    /// The peer with this public key, if it is paired.
    pub fn peer(&self, key: &PublicKey) -> Option<Peer> {
        self.lock().peers.iter().find(|p| &p.public_key == key).cloned()
    }

    /// Unpairs the peer with this key. New handshakes from or to it fail with
    /// [`Error::UnknownPeer`], and its open sessions end with
    /// [`Error::Revoked`]. Returns false if it was not paired.
    pub fn revoke(&self, key: &PublicKey) -> Result<bool> {
        let mut state = self.lock();
        let before = state.peers.len();
        state.peers.retain(|p| &p.public_key != key);
        if state.peers.len() == before {
            return Ok(false);
        }
        self.save_peers(&state)?;
        drop(state);
        self.inner.removed.send_modify(|n| *n += 1);
        Ok(true)
    }

    /// Starts a key rotation: makes a new key pair and keeps the old one as
    /// the previous key. New pairings learn the new key. Peers that still
    /// hold the old one keep working through it (this device answers and
    /// connects with whichever key each peer holds) until the caller tells
    /// each of them the new key over an open session, records their answer
    /// with [`KeyStore::peer_pinned`], and ends the rotation with
    /// [`KeyStore::retire_previous`]. One rotation at a time.
    pub fn rotate(&self) -> Result<PublicKey> {
        let mut state = self.lock();
        if state.previous.is_some() {
            return Err(Error::Store("a key rotation is already in progress; retire the previous key first".into()));
        }
        let next = StaticSecret::random_from_rng(OsRng);
        let mut bytes = Zeroizing::new([0u8; 64]);
        bytes[..32].copy_from_slice(next.as_bytes());
        bytes[32..].copy_from_slice(state.current.as_bytes());
        write_private(&self.inner.dir.join(KEY_FILE), bytes.as_ref())?;
        let old = std::mem::replace(&mut state.current, next);
        state.previous = Some(old);
        Ok(PublicKey::of(&state.current))
    }

    /// Records that `peer` now holds `ours` for this device (it confirmed the
    /// new key over a session authenticated with the old one).
    pub fn peer_pinned(&self, peer: &PublicKey, ours: PublicKey) -> Result<()> {
        let mut state = self.lock();
        if !state.own_keys().contains(&ours) {
            return Err(Error::Store("that is not one of this device's keys".into()));
        }
        let record = state.peers.iter_mut().find(|p| &p.public_key == peer).ok_or(Error::UnknownPeer)?;
        record.pinned = ours;
        self.save_peers(&state)
    }

    /// Records a peer's new key. Call it only with a key the peer sent over a
    /// session authenticated with `old`.
    pub fn peer_rotated(&self, old: &PublicKey, new: PublicKey) -> Result<()> {
        let mut state = self.lock();
        let record = state.peers.iter_mut().find(|p| &p.public_key == old).ok_or(Error::UnknownPeer)?;
        record.public_key = new;
        self.save_peers(&state)
    }

    /// Ends a rotation: forgets the previous key. Peers that still hold only
    /// the previous key can no longer reach this device, so they are unpaired
    /// and returned; they pair again. Nothing happens if no rotation is in
    /// progress.
    pub fn retire_previous(&self) -> Result<Vec<Peer>> {
        let mut state = self.lock();
        let Some(previous) = state.previous.as_ref().map(PublicKey::of) else {
            return Ok(Vec::new());
        };
        write_private(&self.inner.dir.join(KEY_FILE), state.current.as_bytes())?;
        state.previous = None;
        let (stranded, kept): (Vec<Peer>, Vec<Peer>) =
            std::mem::take(&mut state.peers).into_iter().partition(|p| p.pinned == previous);
        state.peers = kept;
        self.save_peers(&state)?;
        drop(state);
        if !stranded.is_empty() {
            self.inner.removed.send_modify(|n| *n += 1);
        }
        Ok(stranded)
    }

    /// Adds a newly paired peer. A peer with the same key is replaced, and so
    /// is a host with the same id (the same host paired again).
    pub(crate) fn add_peer(&self, peer: Peer) -> Result<()> {
        let mut state = self.lock();
        state
            .peers
            .retain(|p| p.public_key != peer.public_key && !(peer.side == Side::Host && p.side == Side::Host && p.id == peer.id));
        state.peers.push(peer);
        self.save_peers(&state)
    }

    /// Whether the pairing a session was opened under still exists.
    pub(crate) fn still_paired(&self, peer: &Peer) -> bool {
        self.lock().peers.iter().any(|p| p.side == peer.side && p.id == peer.id)
    }

    /// The private key behind one of this device's public keys.
    pub(crate) fn private_for(&self, public: &PublicKey) -> Option<Zeroizing<[u8; 32]>> {
        let state = self.lock();
        [Some(&state.current), state.previous.as_ref()]
            .into_iter()
            .flatten()
            .find(|s| &PublicKey::of(s) == public)
            .map(|s| Zeroizing::new(s.to_bytes()))
    }

    /// This device's key pairs, current first.
    pub(crate) fn key_pairs(&self) -> Vec<(PublicKey, Zeroizing<[u8; 32]>)> {
        let state = self.lock();
        [Some(&state.current), state.previous.as_ref()]
            .into_iter()
            .flatten()
            .map(|s| (PublicKey::of(s), Zeroizing::new(s.to_bytes())))
            .collect()
    }

    pub(crate) fn watch_removals(&self) -> watch::Receiver<u64> {
        self.inner.removed.subscribe()
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        // A panic while holding the lock leaves the state as consistent as the
        // files (every change is written before it is kept), so keep going.
        self.inner.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn save_peers(&self, state: &State) -> Result<()> {
        let text = serde_json::to_vec_pretty(&PeersFile { peers: state.peers.clone() })
            .map_err(|e| Error::Store(e.to_string()))?;
        write_private(&self.inner.dir.join(PEERS_FILE), &text)
    }
}

impl State {
    fn own_keys(&self) -> Vec<PublicKey> {
        [Some(&self.current), self.previous.as_ref()].into_iter().flatten().map(PublicKey::of).collect()
    }
}

pub(crate) fn now() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

fn secret(bytes: &[u8]) -> StaticSecret {
    let mut arr = Zeroizing::new([0u8; 32]);
    arr.copy_from_slice(bytes);
    StaticSecret::from(*arr)
}

fn store_err(e: std::io::Error) -> Error {
    Error::Store(e.to_string())
}

fn create_private_dir(dir: &Path) -> Result<()> {
    fs::create_dir_all(dir).map_err(store_err)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o700)).map_err(store_err)?;
    }
    Ok(())
}

/// Writes `bytes` to `path` through a temporary file and a rename, so a crash
/// leaves the old file or the new one, never half of either.
fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let mut options = fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&tmp).map_err(store_err)?;
    file.write_all(bytes).map_err(store_err)?;
    file.sync_all().map_err(store_err)?;
    drop(file);
    fs::rename(&tmp, path).map_err(store_err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(store: &KeyStore, id: &str, side: Side, key: PublicKey) -> Peer {
        Peer {
            id: id.into(),
            name: id.into(),
            side,
            public_key: key,
            local_id: "me".into(),
            pinned: store.public_key(),
            paired_at: now(),
        }
    }

    #[test]
    fn keys_and_peers_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let store = KeyStore::open(dir.path()).unwrap();
        let key = store.public_key();
        let other = PublicKey::from_bytes([7; 32]);
        store.add_peer(peer(&store, "h1", Side::Host, other)).unwrap();
        drop(store);
        let again = KeyStore::open(dir.path()).unwrap();
        assert_eq!(again.public_key(), key);
        assert_eq!(again.peer(&other).unwrap().id, "h1");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = fs::metadata(dir.path().join(KEY_FILE)).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn public_keys_round_trip_as_base64url() {
        let k = PublicKey::from_bytes([0xfb; 32]);
        let text = k.to_string();
        assert!(!text.contains('=') && !text.contains('+') && !text.contains('/'));
        assert_eq!(text.parse::<PublicKey>().unwrap(), k);
        assert!("short".parse::<PublicKey>().is_err());
    }

    #[test]
    fn rotation_keeps_the_previous_key_until_retired() {
        let dir = tempfile::tempdir().unwrap();
        let store = KeyStore::open(dir.path()).unwrap();
        let old = store.public_key();
        let a = PublicKey::from_bytes([1; 32]);
        let b = PublicKey::from_bytes([2; 32]);
        store.add_peer(peer(&store, "a", Side::Client, a)).unwrap();
        store.add_peer(peer(&store, "b", Side::Client, b)).unwrap();
        let new = store.rotate().unwrap();
        assert_ne!(new, old);
        assert!(store.rotate().is_err());
        assert!(store.private_for(&old).is_some() && store.private_for(&new).is_some());
        // Survives a restart mid-rotation.
        let store = KeyStore::open(dir.path()).unwrap();
        assert_eq!(store.public_key(), new);
        assert!(store.private_for(&old).is_some());
        store.peer_pinned(&a, new).unwrap();
        let stranded = store.retire_previous().unwrap();
        assert_eq!(stranded.iter().map(|p| p.id.as_str()).collect::<Vec<_>>(), ["b"]);
        assert!(store.private_for(&old).is_none());
        assert!(store.peer(&a).is_some() && store.peer(&b).is_none());
        assert!(store.peer_pinned(&a, old).is_err());
    }

    #[test]
    fn a_host_paired_again_replaces_its_old_record() {
        let dir = tempfile::tempdir().unwrap();
        let store = KeyStore::open(dir.path()).unwrap();
        store.add_peer(peer(&store, "h1", Side::Host, PublicKey::from_bytes([1; 32]))).unwrap();
        store.add_peer(peer(&store, "h1", Side::Host, PublicKey::from_bytes([2; 32]))).unwrap();
        store.add_peer(peer(&store, "d1", Side::Client, PublicKey::from_bytes([3; 32]))).unwrap();
        store.add_peer(peer(&store, "d1", Side::Client, PublicKey::from_bytes([4; 32]))).unwrap();
        let peers = store.peers();
        assert_eq!(peers.len(), 3);
        assert_eq!(peers.iter().find(|p| p.id == "h1").unwrap().public_key, PublicKey::from_bytes([2; 32]));
    }

    #[test]
    fn revoke_removes_and_signals() {
        let dir = tempfile::tempdir().unwrap();
        let store = KeyStore::open(dir.path()).unwrap();
        let k = PublicKey::from_bytes([9; 32]);
        store.add_peer(peer(&store, "d", Side::Client, k)).unwrap();
        let mut rx = store.watch_removals();
        assert!(store.revoke(&k).unwrap());
        assert!(rx.has_changed().unwrap());
        rx.mark_unchanged();
        assert!(!store.revoke(&k).unwrap());
        assert!(!rx.has_changed().unwrap());
        assert!(store.peer(&k).is_none());
    }
}
