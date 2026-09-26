# oal-secure

End-to-end encryption for [Open Agent Link](https://openagent.link) (OAL). A client and a host pair once with a short code; from then on every connection between them is encrypted and mutually authenticated with keys set up at pairing. A relay between them forwards ciphertext. It can't read the traffic, and it can't insert itself, at pairing or afterwards.

This crate implements OAL spec sections 17.2 to 17.5 (`spec/oal-0.1.md`). Nebo Link is the reference host, and a Rust client uses the same crate.

## Design

### Keys

Each device (a host, or a client installation) has one X25519 static key pair and a list of paired peers, kept in a `KeyStore` directory:

- `device.key`: the private key (32 bytes), followed by the previous private key while a rotation is in progress. Mode 0600, directory 0700 (Unix).
- `peers.json`: each peer's public key, id, name, side, the id this device has at that peer, which of this device's keys the peer holds, and when they paired.

Writes go through a temporary file and a rename. One process owns a store at a time.

### Pairing

A code is `XXXX-XXXX`. The first four characters are the **nameplate**: the relay routes the pairing by it (`wss://<relay>/oal/pair/<nameplate>`), so the relay may issue it and always sees it. The last four are the **secret**: made on the device that shows the code, typed on the other device, and never sent to the relay.

The pairing connection carries five handshake messages, then the ordinary `host/pair` request and result, encrypted:

| # | Direction | Message |
|---|---|---|
| 1 | client → host | CPace `MSGa` (34 bytes) |
| 2 | host → client | CPace `MSGb` (34 bytes) |
| 3 | client → host | `Noise_XXpsk0` message 1, `-> psk, e` |
| 4 | host → client | message 2, `<- e, ee, s, es` |
| 5 | client → host | message 3, `-> s, se` |
| 6 | client → host | `host/pair` request (encrypted frame) |
| 7 | host → client | `host/pair` result (encrypted frame) |

CPace (draft-irtf-cfrg-cpace-21, `CPACE-RISTR255-SHA512`) runs with the whole code as the password, `CI = "OAL-PAIR/1"`, and empty `sid` and associated data. Its output keys the PSK of the Noise handshake, which carries both static keys, encrypted, and proves each side holds its private key. Each side then checks that the key the other names in `host/pair` is the key the handshake authenticated (`Pairing::finish` does this) before recording the peer. The spec section has every byte.

A wrong code fails the pairing on the host at message 3. A relay that changes any message fails it at the next message the other side reads. Either way nothing is recorded.

### Sessions

`Noise_IK_25519_ChaChaPoly_BLAKE2s`, prologue `OAL-E2E/1 <host id>`. The client knows the host's key from pairing and sends its own, encrypted, in message 1; the host looks it up among its paired devices. Message 1's payload is the client's hello (version negotiation) and message 2's is the host's reply.

After the handshake, each OAL frame is one or more Noise transport messages, each a binary WebSocket message. The first plaintext byte of each is `0x01` (more parts follow) or `0x00` (last part), so a part carries up to 65518 bytes of the frame. Frames are joined and limited to `maxFrameBytes` (4 MiB unless the host sets more). The agent id is inside the frame (`{"agent":…,"acp":…}`), so multiplexing by agent happens inside the encrypted channel and the relay never sees agent ids.

- **Rekeying.** Each side rekeys its sending key (Noise `REKEY`) after every 2^20 messages it sends, and its receiving key after every 2^20 it receives. No message is needed to agree on it.
- **Replay, reordering, loss, tampering.** Noise transport messages use an implicit counter as the nonce. Any message that is replayed, reordered, dropped or changed fails to decrypt, and that ends the session. Every later call returns `SessionFailed`.
- **Revocation.** `KeyStore::revoke` removes the peer. New handshakes from or to it fail with `UnknownPeer`, and its open sessions end with `Revoked` at once, including a `recv` that is waiting.
- **Rotation.** `KeyStore::rotate` makes a new key and keeps the old one. While both exist, this device answers with either key and connects with whichever key each peer holds, so nothing breaks. The caller tells each peer the new key over an open session (the OAL message for this is open in spec 17.6), records the peer's answer with `KeyStore::peer_pinned` (the peer records it with `KeyStore::peer_rotated`), and ends the rotation with `KeyStore::retire_previous`, which unpairs and returns any peer that never learned the new key.

### Primitives, and why

- **Sessions: Noise IK** (the `snow` crate), fixed by the spec. After pairing the client knows the host's key, which is what IK assumes: one round trip, the client's identity hidden from the relay, mutual authentication by static keys.
- **Pairing: CPace feeding a Noise PSK.** The code has 20 secret bits, so it can't be a Noise PSK itself: anyone who sees an `XXpsk0` first message can test every code against it offline in well under a second. A fingerprint comparison needs a screen, and hosts are often servers. A PAKE gives an attacker one online guess per attempt. CPace is the CFRG's recommended balanced PAKE. Its output keys a standard Noise handshake, as the draft recommends, so OAL adds no key confirmation of its own.
- **CPace itself** is about 100 lines in `src/cpace.rs`, written from the draft on curve25519-dalek's ristretto255 and the `sha2` crate's SHA-512, and tested against all of the draft's ristretto255 test vectors (appendix B.3). The existing Rust CPace crates were not used: `pake-cpace` implements an early draft with a non-standard encoding, and the others are young single-author crates. Clients in other languages must interoperate, which needs the standard encoding.
- **Keys: X25519** (`x25519-dalek`), which Noise uses.
- **Randomness:** `rand_core`'s `OsRng` only.

No primitive is implemented here. The dependencies that do cryptography are `snow`, `curve25519-dalek`, `x25519-dalek`, `sha2` and `zeroize`.

## Threat model

**Protected against:**

- **A relay that reads traffic** (hosted or self-hosted, honest or compromised). It sees ciphertext only.
- **A relay that changes, drops, reorders, replays or injects messages.** Every such change is detected and ends the session or the pairing.
- **A relay that tries to pair in the middle.** It knows the nameplate but not the secret half. Each attempt is one guess in 2^20. With the spec's 5 failed attempts per code, its chance per code is at most 5 in 2^20, and every attempt shows as a failed pairing.
- **A relay that tries to answer as a host, or connect as a device,** after pairing. It holds neither static private key.
- **A device that is lost or retired.** Revoking its key on the host refuses it and ends its open sessions.
- **Theft of keys after the fact.** Session keys come from ephemeral keys, so a static key stolen later does not reveal past sessions (message 1's payload excepted, below). Rekeying is one-way, so a session key taken mid-session does not reveal traffic from before the last rekey.

**Not protected against:**

- **Metadata.** See below.
- **A relay that learns the whole code.** If the secret half reaches the relay (a code shown on the relay's own web page, or typed into a relay form), the relay can pair in the middle. The spec forbids this, and the test `a_relay_that_knows_the_whole_code_can_pair_in_the_middle` shows why.
- **A compromised endpoint.** Malware on the host or the client device has the keys and the plaintext.
- **Denial of service.** A relay can refuse to carry traffic, or burn a code with failed attempts.
- **Message 1 replay.** The relay can replay a client's first handshake message. Nothing is learned (only the real device can read the reply), but the host must not act on message 1's payload and must count the device as present only after its first frame decrypts. The payload of message 1 is not forward-secret against a later theft of the host's static key.
- **Traffic analysis.** Messages are not padded.

## What the relay still sees

Which host each connection goes to, the client's IP address, when connections open and close, and the size and timing of every message. Each Noise message is its part of the frame plus 17 bytes. With relay-issued identity it also sees the account. At pairing it sees the nameplate and that a pairing happened.

It never sees the secret half of a code, a static key in the clear, device names, tokens, agent ids, session ids, prompts, replies, tool calls, permission requests or file names.

## API

```rust
use oal_secure::{accept, connect, pair, KeyStore, PairingCode, Side};

let store = KeyStore::open(dir)?; // this device's keys and peers

// Pairing, on the device that shows the code:
let code = PairingCode::generate(Some(&nameplate_from_relay))?; // or None: make both halves here
println!("{code}"); // K7QM-3XRD

// Pairing, on the device the code is typed on:
let code = PairingCode::parse("k7qm 3xrd")?;

// Client: pair, then host/pair inside, then keep the connection.
let mut p = pair(ws, &code, &store, Side::Client).await?;
p.send(host_pair_request).await?;
let result = p.recv().await?; // parse info.host.{id,name,publicKey}, device.id
let session = p.finish(&host_public_key, &host_id, &host_name, &device_id)?;

// Host: pair, answer host/pair, keep the connection.
let mut p = pair(ws, &code, &store, Side::Host).await?;
let request = p.recv().await?; // parse device.{name,publicKey}
p.send(host_pair_result).await?;
let session = p.finish(&device_public_key, &device_id, &device_name, &host_id)?;

// Later connections. Client:
let (mut session, reply) = connect(ws, &store, &host_peer, hello_json).await?;
// Host:
let incoming = accept(ws, &store, &host_id).await?; // incoming.peer(), incoming.hello()
let mut session = incoming.finish(reply_json).await?;

session.send(frame).await?;
while let Some(frame) = session.recv().await? { /* one OAL frame */ }
let (reader, writer) = session.split(); // read and write from two tasks
```

`recv` is cancel-safe. `send` is not: if a send is dropped half-way, close the session.

### Transports

Everything runs over a `Transport`: a `Stream` of `io::Result<Vec<u8>>` and a `Sink<Vec<u8>>`, one item per Noise message. A byte stream becomes one with `framed(io)` (two-byte length prefix). A WebSocket needs a small adapter; this one is for axum's `WebSocket` (tokio-tungstenite's is the same shape, in `tests/e2e.rs`):

```rust
struct Ws(axum::extract::ws::WebSocket);

impl Stream for Ws {
    type Item = io::Result<Vec<u8>>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            return Poll::Ready(match ready!(Pin::new(&mut self.0).poll_next(cx)) {
                Some(Ok(Message::Binary(b))) => Some(Ok(b.to_vec())),
                // A text message on an encrypted connection: the host closes with 4001.
                Some(Ok(Message::Text(_))) => Some(Err(io::Error::new(io::ErrorKind::InvalidData, "text message"))),
                Some(Ok(Message::Close(_))) | None => None,
                Some(Ok(_)) => continue, // ping, pong
                Some(Err(e)) => Some(Err(io::Error::other(e))),
            });
        }
    }
}

impl Sink<Vec<u8>> for Ws {
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
```

### Errors

| Error | Meaning | WebSocket close |
|---|---|---|
| `PairingFailed` | Wrong code, or something in the middle; the two can't be told apart. The host counts it against the code. | 4001 |
| `UnknownPeer` | The key is not paired (never, or revoked). | 4001 |
| `Authentication` | A message failed to decrypt: tampering, replay, reordering, loss, or the wrong peer. | 4001 |
| `KeyRetired` | The only key of ours the peer knows was retired: pair again. | 4001 |
| `Revoked` | The peer was unpaired during the session. | 4003 |
| `FrameTooLarge` | A frame over this side's limit. | 1009 |
| `Protocol` | A message of the wrong shape. | 1002 |
| `InvalidCode` | The text entered is not a code. | (before connecting) |
| `Io`, `Closed`, `Store`, `SessionFailed` | Transport, storage, or an earlier failure. | 1011 |

`Error::close_code()` returns the code.

## How each party uses it

### nebo-link (the host)

1. One `KeyStore` per daemon, in the daemon's state directory (for example `<root>/oal-keys/`). The host id is the daemon's host id.
2. `nebo-link pair` makes a `PairingCode` (the nameplate from the relay or made locally; the secret always local), shows it, and registers the nameplate with the relay. The relay's pairing connection arrives through the tunnel at the local proxy; the proxy adapts the WebSocket, calls `pair(ws, &code, &store, Side::Host)`, runs the existing `host/pair` handling over `Pairing::send`/`recv`, and calls `finish` with `device.publicKey`. `PairingFailed` counts against the code's limits (spec 6.2).
3. On `/oal`, a binary first message means an encrypted connection: `accept`, check the hello's version, `finish` with the reply, then run the same OAL connection loop over `Session::recv`/`send` instead of text frames. A text first message is the 0.1 path until the host requires encryption, then close 4001.
4. `host/unpair` and removing a device locally call `KeyStore::revoke` with the device's key; its sessions end with `Revoked`, and the loop closes them with 4003.
5. Key rotation is a command that calls `rotate`, tells each device the new key as it connects (`Session::local_key` differs from `KeyStore::public_key`), and retires the previous key when every device has it.

### A client SDK

One `KeyStore` per client installation. Pair through `wss://<relay>/oal/pair/<nameplate>` with `Side::Client`, send `host/pair` inside, and `finish` with `info.host.publicKey`. Later, `connect` through `wss://<relay>/oal/hosts/<hostId>` (or the host's LAN address) with the stored host `Peer` and the hello JSON. A client that shows a code (the NeboAI app today) gets the nameplate from the relay and makes the secret half itself.

SDKs in other languages implement the same spec: CPace needs ristretto255 and SHA-512 (libsodium has both, so TypeScript, Python and Go all have them), and Noise libraries exist for each (for example `flynn/noise` in Go, `noiseprotocol` in Python, and `noise-c`, which any language can bind). The CPace test vectors in the draft's appendix B.3 check the one part that is written by hand.

### The relay

The relay decrypts nothing and links against nothing here. It must:

- route `wss://<relay>/oal/pair/<nameplate>` by the four-character nameplate, and never ask for, log or show the secret half;
- issue only nameplates, if it issues codes at all;
- pass binary WebSocket messages unchanged, whole and in order (never split or join them).

## For security review

- `src/cpace.rs`: the one protocol written from a specification here. It passes the draft's test vectors; a reviewer should check it against the draft's text, especially scalar sampling, `scalar_mult_vfy`'s abort conditions and the generator string's padding.
- The composition in `src/pair.rs`: CPace's ISK through SHA-512 with a label as the `XXpsk0` PSK, empty CPace `sid`, `CI = "OAL-PAIR/1"`.
- The code split: 20 secret bits and 5 attempts per code give a relay at most 5 in 2^20 per code. Ten characters (30 secret bits) would cost the owner two more characters to type.
- Zeroization: this crate zeroizes the keys and secrets it holds. `snow` 0.10 does not zeroize its internal copies of the static key, handshake state or cipher keys, and `curve25519-dalek` scalars are zeroized only where this crate owns them.
- Keys are files (0600), not the OS keychain.
