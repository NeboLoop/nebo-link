//! Encrypted sessions between a paired client and host (spec section 17).
//!
//! The handshake is `Noise_IK_25519_ChaChaPoly_BLAKE2s`: the client already
//! holds the host's static key from pairing and proves its own in the first
//! message; the host looks that key up among its paired devices. The
//! prologue is `OAL-E2E/1 ` and the host id.
//!
//! After the handshake, every OAL frame travels as one or more Noise
//! transport messages. Each plaintext starts with one byte, `0x01` if more
//! parts of the frame follow and `0x00` on the last part. Each side rekeys
//! its sending key (Noise `REKEY`) after every 2^20 messages it sends, and
//! its receiving key after every 2^20 it receives.
//!
//! Noise transport messages use an implicit counter as the nonce, so a
//! message that is replayed, reordered, dropped or altered fails to decrypt.
//! That failure ends the session ([`Error::Authentication`]); OAL runs over
//! an ordered, reliable transport, so it only happens under interference.

use std::io;
use std::sync::{Arc, Mutex};

use futures::stream::{SplitSink, SplitStream};
use futures::{Sink, SinkExt, Stream, StreamExt};
use snow::{Builder, HandshakeState, TransportState};
use tokio::sync::watch;
use zeroize::Zeroizing;

use crate::error::{Error, Result};
use crate::keys::{KeyStore, Peer, PublicKey, Side};
use crate::transport::{MAX_MESSAGE, Transport, next_message};

const NOISE_SESSION: &str = "Noise_IK_25519_ChaChaPoly_BLAKE2s";
const PROLOGUE: &[u8] = b"OAL-E2E/1 ";
const TAG: usize = 16;
const MORE: u8 = 0x01;
const LAST: u8 = 0x00;
/// Frame bytes in one Noise message: the message, less the tag and the part byte.
const PART: usize = MAX_MESSAGE - TAG - 1;
/// Messages per direction between rekeys.
const REKEY_EVERY: u64 = 1 << 20;
/// The largest frame a session accepts unless told otherwise: the minimum
/// every OAL host must accept (spec section 4.1).
pub const DEFAULT_MAX_FRAME: usize = 4 * 1024 * 1024;

/// Opens a session to a paired host. `hello` is the first message's payload
/// (spec: `{"protocol":…,"client":…}`); the host's reply payload is returned
/// with the session.
///
/// `hello` is encrypted to the host but can be replayed by the relay, so it
/// carries negotiation only, never an instruction.
pub async fn connect<T: Transport>(mut transport: T, store: &KeyStore, host: &Peer, hello: &[u8]) -> Result<(Session<T>, Vec<u8>)> {
    if host.side != Side::Host {
        return Err(Error::Protocol("connect is for hosts; that peer is a client"));
    }
    // The store's record, not the caller's copy: it may have been revoked or
    // updated since.
    let host = store.peer(&host.public_key).ok_or(Error::UnknownPeer)?;
    let local = host.pinned;
    let private = store.private_for(&local).ok_or(Error::KeyRetired)?;
    let mut hs = handshake(&host.id, &private, Some(&host.public_key))?;

    let mut buf = Zeroizing::new(vec![0u8; MAX_MESSAGE]);
    let n = hs.write_message(hello, &mut buf).map_err(|_| Error::Protocol("the hello is too long"))?;
    transport.send(buf[..n].to_vec()).await?;
    let msg = next_message(&mut transport).await?;
    let n = hs.read_message(&msg, &mut buf).map_err(|_| Error::Authentication)?;
    let reply = buf[..n].to_vec();
    let session = Session::new(transport, hs, store, Some(host), local)?;
    Ok((session, reply))
}

/// Reads a client's first handshake message. The client must be a paired
/// device; the host inspects its hello, then answers with
/// [`Incoming::finish`].
///
/// A host in the middle of a key rotation accepts clients that hold either
/// its current or its previous key; [`Session::local_key`] says which.
pub async fn accept<T: Transport>(mut transport: T, store: &KeyStore, host_id: &str) -> Result<Incoming<T>> {
    let msg = next_message(&mut transport).await?;
    let mut buf = Zeroizing::new(vec![0u8; MAX_MESSAGE]);
    for (public, private) in store.key_pairs() {
        let mut hs = handshake(host_id, &private, None)?;
        let Ok(n) = hs.read_message(&msg, &mut buf) else { continue };
        let client = PublicKey::from_slice(hs.get_remote_static().ok_or(Error::Authentication)?)?;
        let peer = store.peer(&client).filter(|p| p.side == Side::Client).ok_or(Error::UnknownPeer)?;
        return Ok(Incoming { transport, hs, store: store.clone(), peer, local: public, hello: buf[..n].to_vec() });
    }
    // No key of ours opens it: it was aimed at a key this host does not hold
    // (retired, or another host's), or it was altered.
    Err(Error::Authentication)
}

/// A client's first message, read and authenticated, waiting for the host's
/// answer.
pub struct Incoming<T> {
    transport: T,
    hs: HandshakeState,
    store: KeyStore,
    peer: Peer,
    local: PublicKey,
    hello: Vec<u8>,
}

impl<T: Transport> Incoming<T> {
    /// The paired device that is connecting.
    pub fn peer(&self) -> &Peer {
        &self.peer
    }

    /// The client's hello payload.
    pub fn hello(&self) -> &[u8] {
        &self.hello
    }

    /// Answers with `reply` as the second message's payload (spec:
    /// `{"protocol":…,"device":{"id":…}}`, or an error the host then closes
    /// on) and opens the session.
    ///
    /// A replayed first message also reaches this point, but no one without
    /// the client's keys can read the reply or send a valid frame. Count the
    /// device as present only once its first frame arrives.
    pub async fn finish(mut self, reply: &[u8]) -> Result<Session<T>> {
        let mut buf = Zeroizing::new(vec![0u8; MAX_MESSAGE]);
        let n = self.hs.write_message(reply, &mut buf).map_err(|_| Error::Protocol("the reply is too long"))?;
        self.transport.send(buf[..n].to_vec()).await?;
        Session::new(self.transport, self.hs, &self.store, Some(self.peer), self.local)
    }
}

/// An open encrypted session.
///
/// [`Session::send`] and [`Session::recv`] carry whole OAL frames. To read
/// and write at the same time, [`Session::split`] it.
///
/// `recv` is cancel-safe (a partly received frame is kept for the next call).
/// `send` is not: if a send is dropped half-way, close the session.
pub struct Session<T> {
    transport: T,
    reader: ReadState,
    shared: Arc<Shared>,
}

/// The receiving half of a split session.
pub struct SessionReader<T> {
    stream: SplitStream<T>,
    reader: ReadState,
    shared: Arc<Shared>,
}

/// The sending half of a split session.
pub struct SessionWriter<T: Sink<Vec<u8>>> {
    sink: SplitSink<T, Vec<u8>>,
    shared: Arc<Shared>,
}

struct Shared {
    cipher: Mutex<Cipher>,
    store: KeyStore,
    /// `None` only inside a [`crate::Pairing`], before the peer is recorded.
    peer: Option<Peer>,
    local: PublicKey,
}

struct Cipher {
    ts: TransportState,
    sent: u64,
    received: u64,
    rekey_every: u64,
    failed: bool,
}

struct ReadState {
    partial: Vec<u8>,
    max_frame: usize,
    removals: watch::Receiver<u64>,
    buf: Zeroizing<Vec<u8>>,
}

impl<T: Transport> Session<T> {
    pub(crate) fn new(transport: T, hs: HandshakeState, store: &KeyStore, peer: Option<Peer>, local: PublicKey) -> Result<Self> {
        let ts = hs.into_transport_mode().map_err(|_| Error::Authentication)?;
        let removals = store.watch_removals();
        Ok(Session {
            transport,
            reader: ReadState { partial: Vec::new(), max_frame: DEFAULT_MAX_FRAME, removals, buf: Zeroizing::new(vec![0u8; MAX_MESSAGE]) },
            shared: Arc::new(Shared {
                cipher: Mutex::new(Cipher { ts, sent: 0, received: 0, rekey_every: REKEY_EVERY, failed: false }),
                store: store.clone(),
                peer,
                local,
            }),
        })
    }

    /// Sends one frame.
    pub async fn send(&mut self, frame: &[u8]) -> Result<()> {
        send_frame(&mut self.transport, &self.shared, frame).await
    }

    /// Receives the next frame, or `None` when the peer closed cleanly
    /// between frames.
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        recv_frame(&mut self.transport, &mut self.reader, &self.shared).await
    }

    /// Closes the transport.
    pub async fn close(&mut self) -> Result<()> {
        self.transport.close().await.map_err(Error::Io)
    }

    /// Sets the largest frame this side accepts (what a host advertises as
    /// `maxFrameBytes`). The default is [`DEFAULT_MAX_FRAME`].
    pub fn set_max_frame(&mut self, bytes: usize) {
        self.reader.max_frame = bytes;
    }

    /// The peer, as recorded when the session opened.
    pub fn peer(&self) -> &Peer {
        recorded(&self.shared)
    }

    /// Which of this device's keys the session was opened with. When it is
    /// not [`KeyStore::public_key`], the peer holds an older key and should
    /// be told the new one.
    pub fn local_key(&self) -> PublicKey {
        self.shared.local
    }

    /// Splits the session into halves that can be used from two tasks.
    pub fn split(self) -> (SessionReader<T>, SessionWriter<T>) {
        let (sink, stream) = self.transport.split();
        (
            SessionReader { stream, reader: self.reader, shared: self.shared.clone() },
            SessionWriter { sink, shared: self.shared },
        )
    }

    /// Records the peer of a pairing (see [`crate::Pairing::finish`]).
    pub(crate) fn record(&mut self, peer: Peer) {
        Arc::get_mut(&mut self.shared).expect("a pairing is never split").peer = Some(peer);
    }

    #[cfg(test)]
    fn set_rekey_every(&mut self, n: u64) {
        self.shared.cipher.lock().unwrap().rekey_every = n;
    }
}

impl<T: Transport> SessionReader<T> {
    /// Receives the next frame; see [`Session::recv`].
    pub async fn recv(&mut self) -> Result<Option<Vec<u8>>> {
        recv_frame(&mut self.stream, &mut self.reader, &self.shared).await
    }

    /// See [`Session::set_max_frame`].
    pub fn set_max_frame(&mut self, bytes: usize) {
        self.reader.max_frame = bytes;
    }

    /// The peer, as recorded when the session opened.
    pub fn peer(&self) -> &Peer {
        recorded(&self.shared)
    }
}

impl<T: Transport> SessionWriter<T> {
    /// Sends one frame; see [`Session::send`].
    pub async fn send(&mut self, frame: &[u8]) -> Result<()> {
        send_frame(&mut self.sink, &self.shared, frame).await
    }

    /// Closes the transport.
    pub async fn close(&mut self) -> Result<()> {
        self.sink.close().await.map_err(Error::Io)
    }
}

/// The IK handshake for `host_id`: the client's when it knows the host's
/// key (`host_key`), the host's otherwise.
fn handshake(host_id: &str, private: &[u8; 32], host_key: Option<&PublicKey>) -> Result<HandshakeState> {
    let mut prologue = PROLOGUE.to_vec();
    prologue.extend_from_slice(host_id.as_bytes());
    let setup = |_| Error::Protocol("Noise setup");
    let builder = Builder::new(NOISE_SESSION.parse().expect("a valid Noise pattern"))
        .local_private_key(private)
        .and_then(|b| b.prologue(&prologue))
        .map_err(setup)?;
    match host_key {
        Some(key) => builder.remote_public_key(key.as_bytes()).and_then(|b| b.build_initiator()),
        None => builder.build_responder(),
    }
    .map_err(setup)
}

fn recorded(shared: &Shared) -> &Peer {
    shared.peer.as_ref().expect("a session outside a pairing always has its peer")
}

/// Refuses to go on once the session failed or the peer was unpaired.
fn check_paired(shared: &Shared) -> Result<()> {
    if shared.cipher.lock().unwrap_or_else(|e| e.into_inner()).failed {
        return Err(Error::SessionFailed);
    }
    match &shared.peer {
        Some(peer) if !shared.store.still_paired(peer) => {
            fail(shared);
            Err(Error::Revoked)
        }
        _ => Ok(()),
    }
}

fn fail(shared: &Shared) {
    shared.cipher.lock().unwrap_or_else(|e| e.into_inner()).failed = true;
}

async fn send_frame<S>(sink: &mut S, shared: &Shared, frame: &[u8]) -> Result<()>
where
    S: Sink<Vec<u8>, Error = io::Error> + Unpin,
{
    check_paired(shared)?;
    let mut buf = Zeroizing::new(vec![0u8; MAX_MESSAGE]);
    let parts = frame.len().div_ceil(PART).max(1);
    for (i, chunk) in (0..parts).map(|i| (i, &frame[i * PART..((i + 1) * PART).min(frame.len())])) {
        let mut plain = Vec::with_capacity(1 + chunk.len());
        plain.push(if i + 1 == parts { LAST } else { MORE });
        plain.extend_from_slice(chunk);
        let n = {
            let mut c = shared.cipher.lock().unwrap_or_else(|e| e.into_inner());
            if c.failed {
                return Err(Error::SessionFailed);
            }
            let n = match c.ts.write_message(&plain, &mut buf) {
                Ok(n) => n,
                Err(_) => {
                    c.failed = true;
                    return Err(Error::Protocol("the sending counter is exhausted"));
                }
            };
            c.sent += 1;
            if c.sent.is_multiple_of(c.rekey_every) {
                c.ts.rekey_outgoing();
            }
            n
        };
        if let Err(e) = sink.feed(buf[..n].to_vec()).await {
            fail(shared);
            return Err(Error::Io(e));
        }
    }
    sink.flush().await.map_err(|e| {
        fail(shared);
        Error::Io(e)
    })
}

async fn recv_frame<S>(stream: &mut S, reader: &mut ReadState, shared: &Shared) -> Result<Option<Vec<u8>>>
where
    S: Stream<Item = io::Result<Vec<u8>>> + Unpin,
{
    check_paired(shared)?;
    loop {
        let next = tokio::select! {
            biased;
            changed = reader.removals.changed() => {
                if changed.is_ok() {
                    check_paired(shared)?;
                }
                continue;
            }
            next = stream.next() => next,
        };
        let msg = match next {
            None if reader.partial.is_empty() => return Ok(None),
            None => {
                fail(shared);
                return Err(Error::Closed);
            }
            Some(Err(e)) => {
                fail(shared);
                return Err(Error::Io(e));
            }
            Some(Ok(msg)) => msg,
        };
        let n = {
            let mut c = shared.cipher.lock().unwrap_or_else(|e| e.into_inner());
            if c.failed {
                return Err(Error::SessionFailed);
            }
            let n = match c.ts.read_message(&msg, &mut reader.buf) {
                Ok(n) => n,
                Err(_) => {
                    c.failed = true;
                    return Err(Error::Authentication);
                }
            };
            c.received += 1;
            if c.received.is_multiple_of(c.rekey_every) {
                c.ts.rekey_incoming();
            }
            n
        };
        let plain = &reader.buf[..n];
        let (last, data) = match plain.split_first() {
            Some((&LAST, data)) => (true, data),
            Some((&MORE, data)) => (false, data),
            _ => {
                fail(shared);
                return Err(Error::Protocol("a message part without a valid part byte"));
            }
        };
        if reader.partial.len() + data.len() > reader.max_frame {
            fail(shared);
            return Err(Error::FrameTooLarge { limit: reader.max_frame });
        }
        reader.partial.extend_from_slice(data);
        if last {
            return Ok(Some(std::mem::take(&mut reader.partial)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::code::PairingCode;
    use crate::pair::pair;
    use crate::transport::framed;

    async fn paired() -> (tempfile::TempDir, KeyStore, KeyStore, Peer) {
        let dir = tempfile::tempdir().unwrap();
        let client = KeyStore::open(dir.path().join("c")).unwrap();
        let host = KeyStore::open(dir.path().join("h")).unwrap();
        let (a, b) = tokio::io::duplex(1 << 16);
        let code = PairingCode::generate(None).unwrap();
        let (c, h) = tokio::join!(pair(framed(a), &code, &client, Side::Client), pair(framed(b), &code, &host, Side::Host));
        let (c, h) = (c.unwrap(), h.unwrap());
        let (ck, hk) = (c.peer_key(), h.peer_key());
        h.finish(&hk, "d1", "phone", "h1").unwrap();
        let session = c.finish(&ck, "h1", "Mac", "d1").unwrap();
        (dir, client, host, session.peer().clone())
    }

    async fn open(client: &KeyStore, host: &KeyStore, peer: &Peer) -> (Session<impl Transport>, Session<impl Transport>) {
        let (a, b) = tokio::io::duplex(1 << 20);
        let (c, h) = tokio::join!(connect(framed(a), client, peer, b"hi"), async {
            accept(framed(b), host, "h1").await.unwrap().finish(b"ok").await
        });
        (c.unwrap().0, h.unwrap())
    }

    #[tokio::test]
    async fn both_sides_rekey_on_the_same_message() {
        let (_dir, client, host, peer) = paired().await;
        let (mut c, mut h) = open(&client, &host, &peer).await;
        c.set_rekey_every(3);
        h.set_rekey_every(3);
        for i in 0..20u8 {
            c.send(&[i]).await.unwrap();
            assert_eq!(h.recv().await.unwrap().unwrap(), [i]);
            h.send(&[i, i]).await.unwrap();
            assert_eq!(c.recv().await.unwrap().unwrap(), [i, i]);
        }
        let c = c.shared.cipher.lock().unwrap();
        assert_eq!((c.sent, c.received), (20, 20));
    }

    #[tokio::test]
    async fn a_side_that_does_not_rekey_cannot_read_past_the_rekey() {
        let (_dir, client, host, peer) = paired().await;
        let (mut c, mut h) = open(&client, &host, &peer).await;
        c.set_rekey_every(3);
        for i in 0..3u8 {
            c.send(&[i]).await.unwrap();
            assert_eq!(h.recv().await.unwrap().unwrap(), [i]);
        }
        c.send(b"after").await.unwrap();
        assert!(matches!(h.recv().await, Err(Error::Authentication)));
        assert!(matches!(h.recv().await, Err(Error::SessionFailed)));
    }
}
