//! Pairing and sessions end to end: over in-memory pipes, through relays that
//! try to read or change the traffic, and over a real WebSocket.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use futures::{Sink, SinkExt, Stream, StreamExt};
use oal_secure::{Error, KeyStore, PairingCode, Peer, PublicKey, Session, Side, Transport, accept, connect, framed, pair};
use serde_json::{Value, json};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::codec::Framed;

type Pipe = Framed<DuplexStream, oal_secure::MessageCodec>;

fn pipe() -> (Pipe, Pipe) {
    let (a, b) = tokio::io::duplex(1 << 20);
    (framed(a), framed(b))
}

struct Devices {
    _dir: tempfile::TempDir,
    client: KeyStore,
    host: KeyStore,
}

fn devices() -> Devices {
    let dir = tempfile::tempdir().unwrap();
    let client = KeyStore::open(dir.path().join("client")).unwrap();
    let host = KeyStore::open(dir.path().join("host")).unwrap();
    Devices { _dir: dir, client, host }
}

fn store(dir: &tempfile::TempDir, name: &str) -> KeyStore {
    KeyStore::open(dir.path().join(name)).unwrap()
}

/// The client's side of a pairing: the handshake, then `host/pair` inside it.
async fn pair_client<T: Transport>(transport: T, code: &PairingCode, store: &KeyStore) -> Result<Session<T>, Error> {
    let mut p = pair(transport, code, store, Side::Client).await?;
    let request = json!({"jsonrpc":"2.0","id":1,"method":"host/pair","params":{
        "code": code.to_string(),
        "device": {"name": "Alma's phone", "publicKey": store.public_key().to_string()}
    }});
    p.send(request.to_string().as_bytes()).await?;
    let reply: Value = serde_json::from_slice(&p.recv().await?.ok_or(Error::Closed)?).unwrap();
    let result = &reply["result"];
    let host_key: PublicKey = result["info"]["host"]["publicKey"].as_str().unwrap().parse()?;
    p.finish(
        &host_key,
        result["info"]["host"]["id"].as_str().unwrap(),
        result["info"]["host"]["name"].as_str().unwrap(),
        result["device"]["id"].as_str().unwrap(),
    )
}

/// The host's side: the handshake, then answer `host/pair`.
async fn pair_host<T: Transport>(transport: T, code: &PairingCode, store: &KeyStore) -> Result<Session<T>, Error> {
    let mut p = pair(transport, code, store, Side::Host).await?;
    let request: Value = serde_json::from_slice(&p.recv().await?.ok_or(Error::Closed)?).unwrap();
    let device = &request["params"]["device"];
    let claimed: PublicKey = device["publicKey"].as_str().unwrap().parse()?;
    let result = json!({"jsonrpc":"2.0","id":1,"result":{
        "device": {"id": "d1", "name": device["name"], "token": "t"},
        "info": {"host": {"id": "h1", "name": "Studio Mac", "publicKey": store.public_key().to_string()}}
    }});
    p.send(result.to_string().as_bytes()).await?;
    p.finish(&claimed, "d1", device["name"].as_str().unwrap(), "h1")
}

async fn pair_both(d: &Devices) -> (Session<Pipe>, Session<Pipe>) {
    let code = PairingCode::generate(None).unwrap();
    let (a, b) = pipe();
    let (c, h) = tokio::join!(pair_client(a, &code, &d.client), pair_host(b, &code, &d.host));
    (c.unwrap(), h.unwrap())
}

async fn open(d: &Devices) -> (Session<Pipe>, Session<Pipe>) {
    let host_peer = d.client.peers().into_iter().find(|p| p.side == Side::Host).unwrap();
    let (a, b) = pipe();
    let (c, h) = tokio::join!(connect(a, &d.client, &host_peer, b"{\"protocol\":{\"min\":\"0.2\",\"max\":\"0.2\"}}"), async {
        let incoming = accept(b, &d.host, "h1").await?;
        assert_eq!(incoming.peer().name, "Alma's phone");
        assert_eq!(incoming.hello(), b"{\"protocol\":{\"min\":\"0.2\",\"max\":\"0.2\"}}");
        incoming.finish(b"{\"protocol\":\"0.2\",\"device\":{\"id\":\"d1\"}}").await
    });
    let (c, reply) = c.unwrap();
    assert_eq!(reply, b"{\"protocol\":\"0.2\",\"device\":{\"id\":\"d1\"}}");
    (c, h.unwrap())
}

fn host_peer(d: &Devices) -> Peer {
    d.client.peers().into_iter().find(|p| p.side == Side::Host).unwrap()
}

/// Forwards messages between two pipes, handing each to `edit` with its index
/// and direction (true: client to host). `edit` returns what to forward.
fn relay<F>(client_side: Pipe, host_side: Pipe, edit: F) -> tokio::task::JoinHandle<()>
where
    F: FnMut(usize, bool, Vec<u8>) -> Vec<Vec<u8>> + Send + 'static,
{
    tokio::spawn(async move {
        let (mut c_tx, mut c_rx) = client_side.split();
        let (mut h_tx, mut h_rx) = host_side.split();
        let mut edit = edit;
        let mut n = 0;
        loop {
            tokio::select! {
                m = c_rx.next() => match m {
                    Some(Ok(m)) => { for out in edit(n, true, m) { if h_tx.send(out).await.is_err() { return; } } n += 1; }
                    _ => return,
                },
                m = h_rx.next() => match m {
                    Some(Ok(m)) => { for out in edit(n, false, m) { if c_tx.send(out).await.is_err() { return; } } n += 1; }
                    _ => return,
                },
            }
        }
    })
}

#[tokio::test]
async fn pairing_records_each_side_and_leaves_an_encrypted_session() {
    let d = devices();
    let (mut c, mut h) = pair_both(&d).await;
    let host = host_peer(&d);
    assert_eq!((host.id.as_str(), host.name.as_str(), host.local_id.as_str()), ("h1", "Studio Mac", "d1"));
    assert_eq!(host.public_key, d.host.public_key());
    assert_eq!(host.pinned, d.client.public_key());
    let device = d.host.peer(&d.client.public_key()).unwrap();
    assert_eq!((device.id.as_str(), device.side), ("d1", Side::Client));
    assert_eq!(c.peer().id, "h1");
    // The pairing connection goes on as an authenticated OAL connection.
    c.send(b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"host/agents\"}").await.unwrap();
    assert_eq!(h.recv().await.unwrap().unwrap(), b"{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"host/agents\"}");
}

#[tokio::test]
async fn a_wrong_code_fails_on_both_sides_and_records_nothing() {
    let d = devices();
    let right = PairingCode::parse("K7Q2-ABCD").unwrap();
    let wrong = PairingCode::parse("K7Q2-ABCE").unwrap();
    let (a, b) = pipe();
    let (c, h) = tokio::join!(pair(a, &wrong, &d.client, Side::Client), pair(b, &right, &d.host, Side::Host));
    assert!(matches!(h, Err(Error::PairingFailed)), "host: {:?}", h.err());
    assert!(c.is_err());
    assert!(d.client.peers().is_empty() && d.host.peers().is_empty());
}

/// The relay knows the nameplate (it routes by it) and plays host to the
/// client and client to the host, guessing the secret half.
#[tokio::test]
async fn a_relay_that_knows_the_nameplate_cannot_pair_in_the_middle() {
    let d = devices();
    let code = PairingCode::parse("K7Q2-ABCD").unwrap();
    let guess = PairingCode::parse("K7Q2-0000").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (relay_as_host, relay_as_client) = (store(&dir, "r1"), store(&dir, "r2"));
    let (c_end, r1_end) = pipe();
    let (r2_end, h_end) = pipe();
    let (c, h, r1, r2) = tokio::join!(
        pair_client(c_end, &code, &d.client),
        pair_host(h_end, &code, &d.host),
        pair_host(r1_end, &guess, &relay_as_host),
        pair_client(r2_end, &guess, &relay_as_client),
    );
    // Each genuine side is refused: the host by its own check, the client by
    // the relay's (which then drops the connection).
    assert!(matches!(h, Err(Error::PairingFailed)), "host: {:?}", h.err());
    assert!(c.is_err());
    assert!(matches!(r1, Err(Error::PairingFailed)), "relay as host: {:?}", r1.err());
    assert!(r2.is_err());
    assert!(d.client.peers().is_empty() && d.host.peers().is_empty());
}

/// The boundary of the design: a relay that learns the secret half as well
/// can pair in the middle. That is why the secret half never goes to it.
#[tokio::test]
async fn a_relay_that_knows_the_whole_code_can_pair_in_the_middle() {
    let d = devices();
    let code = PairingCode::parse("K7Q2-ABCD").unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (relay_as_host, relay_as_client) = (store(&dir, "r1"), store(&dir, "r2"));
    let (c_end, r1_end) = pipe();
    let (r2_end, h_end) = pipe();
    let (c, h, r1, r2) = tokio::join!(
        pair_client(c_end, &code, &d.client),
        pair_host(h_end, &code, &d.host),
        pair_host(r1_end, &code, &relay_as_host),
        pair_client(r2_end, &code, &relay_as_client),
    );
    assert!(c.is_ok() && h.is_ok() && r1.is_ok() && r2.is_ok());
    assert_eq!(host_peer(&d).public_key, relay_as_host.public_key());
}

/// A relay that changes any one message of the pairing, in either direction,
/// makes it fail, and neither side records the other.
#[tokio::test]
async fn a_relay_that_changes_any_pairing_message_makes_it_fail() {
    for target in 0..7 {
        let d = devices();
        let code = PairingCode::generate(None).unwrap();
        let (c_end, r1) = pipe();
        let (r2, h_end) = pipe();
        let relay = relay(r1, r2, move |i, _, mut m| {
            if i == target {
                let last = m.len() - 1;
                m[last] ^= 0x01;
            }
            vec![m]
        });
        let (c, h) = tokio::time::timeout(
            Duration::from_secs(10),
            async { tokio::join!(pair_client(c_end, &code, &d.client), pair_host(h_end, &code, &d.host)) },
        )
        .await
        .unwrap_or_else(|_| panic!("pairing hung with message {target} changed"));
        assert!(c.is_err(), "message {target} changed and the client still paired");
        assert!(d.client.peers().is_empty(), "message {target}: the client recorded a host");
        // Only the last message (the host/pair result) comes after the host
        // records the device, and then the key it recorded is the genuine one.
        if target < 6 {
            assert!(h.is_err() && d.host.peers().is_empty(), "message {target}: the host recorded a device");
        }
        assert!(d.host.peers().iter().all(|p| p.public_key == d.client.public_key()));
        relay.abort();
    }
}

#[tokio::test]
async fn a_claimed_key_that_differs_from_the_handshake_is_refused() {
    let d = devices();
    let code = PairingCode::generate(None).unwrap();
    let (a, b) = pipe();
    let (c, h) = tokio::join!(pair(a, &code, &d.client, Side::Client), pair(b, &code, &d.host, Side::Host));
    let (_c, h) = (c.unwrap(), h.unwrap());
    let other = PublicKey::from_bytes([9; 32]);
    assert!(matches!(h.finish(&other, "d1", "phone", "h1"), Err(Error::PairingFailed)));
    assert!(d.host.peers().is_empty());
}

#[tokio::test]
async fn frames_of_every_size_round_trip_over_an_in_memory_pipe() {
    let d = devices();
    pair_both(&d).await;
    let (c, h) = open(&d).await;
    let (mut c_rx, mut c_tx) = c.split();
    let (mut h_rx, mut h_tx) = h.split();
    let big: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
    let frames: Vec<Vec<u8>> = vec![b"{}".to_vec(), Vec::new(), big.clone(), vec![7; 65518], vec![8; 65519]];
    let sent = frames.clone();
    let writer = tokio::spawn(async move {
        for f in &sent {
            c_tx.send(f).await.unwrap();
        }
        c_tx
    });
    for f in &frames {
        assert_eq!(&h_rx.recv().await.unwrap().unwrap(), f);
    }
    // Both directions at once.
    let echo = tokio::spawn(async move {
        while let Some(f) = h_rx.recv().await.unwrap() {
            h_tx.send(&f).await.unwrap();
        }
    });
    let mut c_tx = writer.await.unwrap();
    for i in 0..50u32 {
        c_tx.send(&i.to_be_bytes()).await.unwrap();
    }
    for i in 0..50u32 {
        assert_eq!(c_rx.recv().await.unwrap().unwrap(), i.to_be_bytes());
    }
    c_tx.close().await.unwrap();
    echo.await.unwrap();
}

#[tokio::test]
async fn a_replayed_message_is_rejected_and_ends_the_session() {
    let d = devices();
    pair_both(&d).await;
    let host_peer = host_peer(&d);
    let (c_end, r1) = pipe();
    let (r2, h_end) = pipe();
    // Messages 0 and 1 are the handshake; 2 is the first frame. Send it twice.
    let _relay = relay(r1, r2, |i, _, m| if i == 2 { vec![m.clone(), m] } else { vec![m] });
    let (c, h) = tokio::join!(connect(c_end, &d.client, &host_peer, b"{}"), async {
        accept(h_end, &d.host, "h1").await?.finish(b"{}").await
    });
    let (mut c, _) = c.unwrap();
    let mut h = h.unwrap();
    c.send(b"once").await.unwrap();
    assert_eq!(h.recv().await.unwrap().unwrap(), b"once");
    assert!(matches!(h.recv().await, Err(Error::Authentication)));
    assert!(matches!(h.recv().await, Err(Error::SessionFailed)));
    assert!(matches!(h.send(b"x").await, Err(Error::SessionFailed)));
}

#[tokio::test]
async fn a_changed_message_is_rejected() {
    let d = devices();
    pair_both(&d).await;
    let host_peer = host_peer(&d);
    let (c_end, r1) = pipe();
    let (r2, h_end) = pipe();
    let _relay = relay(r1, r2, |i, _, mut m| {
        if i == 3 {
            m[5] ^= 0x80;
        }
        vec![m]
    });
    let (c, h) = tokio::join!(connect(c_end, &d.client, &host_peer, b"{}"), async {
        accept(h_end, &d.host, "h1").await?.finish(b"{}").await
    });
    let (mut c, _) = c.unwrap();
    let mut h = h.unwrap();
    c.send(b"one").await.unwrap();
    c.send(b"two").await.unwrap();
    assert_eq!(h.recv().await.unwrap().unwrap(), b"one");
    assert!(matches!(h.recv().await, Err(Error::Authentication)));
}

#[tokio::test]
async fn a_host_with_other_keys_cannot_answer_for_the_paired_host() {
    let d = devices();
    pair_both(&d).await;
    let dir = tempfile::tempdir().unwrap();
    let impostor = store(&dir, "impostor");
    let (a, b) = pipe();
    let host = host_peer(&d);
    let (c, i) = tokio::join!(connect(a, &d.client, &host, b"{}"), accept(b, &impostor, "h1"));
    assert!(matches!(i.err(), Some(Error::Authentication)));
    assert!(c.is_err());
}

#[tokio::test]
async fn a_revoked_device_is_refused() {
    let d = devices();
    pair_both(&d).await;
    assert!(d.host.revoke(&d.client.public_key()).unwrap());
    let (a, b) = pipe();
    let host = host_peer(&d);
    let (c, h) = tokio::join!(connect(a, &d.client, &host, b"{}"), accept(b, &d.host, "h1"));
    assert!(matches!(h.err(), Some(Error::UnknownPeer)));
    assert!(c.is_err());
}

#[tokio::test]
async fn a_client_that_unpaired_the_host_refuses_to_connect() {
    let d = devices();
    pair_both(&d).await;
    let host = host_peer(&d);
    d.client.revoke(&host.public_key).unwrap();
    let (a, _b) = pipe();
    assert!(matches!(connect(a, &d.client, &host, b"{}").await.err(), Some(Error::UnknownPeer)));
}

#[tokio::test]
async fn revoking_a_device_ends_its_open_session() {
    let d = devices();
    pair_both(&d).await;
    let (_c, h) = open(&d).await;
    let (mut h_rx, mut h_tx) = h.split();
    let waiting = tokio::spawn(async move { h_rx.recv().await });
    tokio::task::yield_now().await;
    d.host.revoke(&d.client.public_key()).unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), waiting).await.unwrap().unwrap();
    assert!(matches!(got, Err(Error::Revoked)), "{got:?}");
    assert_eq!(Error::Revoked.close_code(), 4003);
    assert!(h_tx.send(b"x").await.is_err());
}

#[tokio::test]
async fn a_frame_over_the_limit_is_refused() {
    let d = devices();
    pair_both(&d).await;
    let (mut c, mut h) = open(&d).await;
    h.set_max_frame(100_000);
    c.send(&vec![1; 100_001]).await.unwrap();
    assert!(matches!(h.recv().await, Err(Error::FrameTooLarge { limit: 100_000 })));
}

#[tokio::test]
async fn host_key_rotation_reaches_the_client_and_retires_the_old_key() {
    let d = devices();
    pair_both(&d).await;
    let old = d.host.public_key();
    let new = d.host.rotate().unwrap();

    // The client still holds the old key and still gets in.
    let (mut c, mut h) = open(&d).await;
    assert_eq!(h.local_key(), old);
    // The host tells it the new key (an OAL message; the shape is the caller's).
    h.send(new.to_string().as_bytes()).await.unwrap();
    let announced: PublicKey = String::from_utf8(c.recv().await.unwrap().unwrap()).unwrap().parse().unwrap();
    d.client.peer_rotated(&old, announced).unwrap();
    c.send(b"ok").await.unwrap();
    assert_eq!(h.recv().await.unwrap().unwrap(), b"ok");
    d.host.peer_pinned(&d.client.public_key(), new).unwrap();
    assert!(d.host.retire_previous().unwrap().is_empty());

    let (_c, h) = open(&d).await;
    assert_eq!(h.local_key(), new);
}

#[tokio::test]
async fn a_client_that_missed_a_retired_host_key_must_pair_again() {
    let d = devices();
    pair_both(&d).await;
    d.host.rotate().unwrap();
    let stranded = d.host.retire_previous().unwrap();
    assert_eq!(stranded.len(), 1);
    let (a, b) = pipe();
    let host = host_peer(&d);
    let (c, h) = tokio::join!(connect(a, &d.client, &host, b"{}"), accept(b, &d.host, "h1"));
    assert!(matches!(h.err(), Some(Error::Authentication)));
    assert!(c.is_err());
}

#[tokio::test]
async fn client_key_rotation_reaches_the_host() {
    let d = devices();
    pair_both(&d).await;
    let old = d.client.public_key();
    let new = d.client.rotate().unwrap();
    let (mut c, mut h) = open(&d).await;
    assert_eq!(c.local_key(), old);
    c.send(new.to_string().as_bytes()).await.unwrap();
    let announced: PublicKey = String::from_utf8(h.recv().await.unwrap().unwrap()).unwrap().parse().unwrap();
    d.host.peer_rotated(&old, announced).unwrap();
    h.send(b"ok").await.unwrap();
    c.recv().await.unwrap().unwrap();
    d.client.peer_pinned(&d.host.public_key(), new).unwrap();
    assert!(d.client.retire_previous().unwrap().is_empty());
    let (c, h) = open(&d).await;
    assert_eq!(c.local_key(), new);
    assert_eq!(h.peer().public_key, new);
}

/// A WebSocket adapted to [`Transport`]: binary messages carry Noise
/// messages; a text message is an error (spec section 17.2).
struct Ws<S>(WebSocketStream<S>);

impl<S: AsyncRead + AsyncWrite + Unpin> Stream for Ws<S> {
    type Item = io::Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return Poll::Ready(match ready!(Pin::new(&mut self.0).poll_next(cx)) {
                Some(Ok(Message::Binary(b))) => Some(Ok(b.to_vec())),
                Some(Ok(Message::Text(_))) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, "a text message"))),
                Some(Ok(Message::Close(_))) | None => None,
                Some(Ok(_)) => continue,
                Some(Err(e)) => Some(Err(io::Error::other(e))),
            });
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Sink<Vec<u8>> for Ws<S> {
    type Error = io::Error;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_ready(cx).map_err(io::Error::other)
    }
    fn start_send(mut self: Pin<&mut Self>, item: Vec<u8>) -> io::Result<()> {
        Pin::new(&mut self.0).start_send(Message::Binary(item.into())).map_err(io::Error::other)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx).map_err(io::Error::other)
    }
    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_close(cx).map_err(io::Error::other)
    }
}

#[tokio::test]
async fn pairing_and_a_session_over_a_real_websocket() {
    let d = devices();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/oal", listener.local_addr().unwrap());
    let host_store = d.host.clone();
    let server = tokio::spawn(async move {
        // The pairing connection, then a session connection.
        let code = PairingCode::parse("K7Q2-ABCD").unwrap();
        let (tcp, _) = listener.accept().await.unwrap();
        let ws = Ws(tokio_tungstenite::accept_async(tcp).await.unwrap());
        drop(pair_host(ws, &code, &host_store).await.unwrap());
        let (tcp, _) = listener.accept().await.unwrap();
        let ws = Ws(tokio_tungstenite::accept_async(tcp).await.unwrap());
        let mut session = accept(ws, &host_store, "h1").await.unwrap().finish(b"{}").await.unwrap();
        while let Some(frame) = session.recv().await.unwrap() {
            session.send(&frame).await.unwrap();
        }
    });

    let code = PairingCode::parse("k7q2 abcd").unwrap();
    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    drop(pair_client(Ws(ws), &code, &d.client).await.unwrap());

    let (ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let (mut session, _) = connect(Ws(ws), &d.client, &host_peer(&d), b"{}").await.unwrap();
    let big = vec![0x5a; 200_000];
    for frame in [&b"{\"agent\":\"app\",\"acp\":{}}"[..], &big] {
        session.send(frame).await.unwrap();
        assert_eq!(session.recv().await.unwrap().unwrap(), frame);
    }
    session.close().await.unwrap();
    server.await.unwrap();
}
