# oal-relay

The self-hostable relay for [Open Agent Link](https://openagent.link) (OAL). A host (the process that runs a computer's agents, such as Nebo Link) dials out to the relay and keeps one tunnel open. A client that paired with that host connects to the relay and is carried to the host through the tunnel. Nothing on the host's computer listens on a port.

It is one static binary with a SQLite file beside it. No accounts: every host and every client is an X25519 key, and every connection proves it holds its key before the relay routes it. NeboAI runs a hosted relay for people with a free NeboAI account; this is the one you run yourself, with no account at all.

## Run it

Binary (Rust 1.88 or later):

```sh
cargo install --locked --git https://github.com/NeboLoop/nebo-link oal-relay
oal-relay serve
```

Docker:

```sh
docker build -t oal-relay -f crates/oal-relay/Dockerfile https://github.com/NeboLoop/nebo-link.git#main
docker run -d --name oal-relay -p 8480:8480 -v oal-relay:/data oal-relay
```

It listens on port 8480 over plain HTTP. Put it behind anything that terminates TLS (Caddy: `caddy reverse-proxy --from relay.example.com --to localhost:8480`), or give it a certificate and let it serve HTTPS itself:

```sh
oal-relay serve --listen 0.0.0.0:443 --tls-cert fullchain.pem --tls-key privkey.pem
```

Hosts and clients refuse a relay that is not `https://`, except one on their own machine.

## Pair a device

A pairing code is 8 characters, `K7QM-3XRD`. The first four are the **nameplate**: the relay routes the pairing by it. The last four are the **secret**: the device that shows the code makes it, and it never reaches the relay, in a URL, a header, a log or an API (OAL spec section 6.2). So the relay can't pair in the middle: with OAL 0.2 the whole code is the password of the pairing key exchange (CPace, then Noise; spec section 17.5, the `oal-secure` crate), and the relay knows only half of it.

1. The host holds a nameplate at the relay (a fresh one the relay picks, or the one it chose), adds its own secret half, and shows the code. Nameplates expire after 5 minutes.
2. The client opens `wss://relay.example.com/oal/pair/K7QM`. The relay carries that connection to the host, stamped with the client key the relay verified. Inside it the two run the pairing and `host/pair`, which the relay never reads.
3. When the host accepts, it tells the relay, and from then on the client connects to `wss://relay.example.com/oal/hosts/studio`. Only clients a host has paired get through.

The host checks the code and counts failures against it; the relay routes every attempt until the nameplate expires, and limits unknown nameplates to 10 a minute per address. The operator can't issue codes: a code shown by the relay would be a code the relay knows.

## Point a host at it

A host registers under its OAL host id the first time it opens its tunnel; after that the id belongs to its key.

- **In Rust**, with this crate as a library (`default-features = false` leaves the server out):

  ```rust
  let relay = oal_relay::RelayClient::new("https://relay.example.com", host_key)?;
  let mut tunnel = relay.host("studio").await?;
  let handle = tunnel.handle();
  let nameplate = handle.nameplate(None).await?;                        // or Some("K7QM")
  let code = oal_secure::PairingCode::generate(Some(&nameplate.nameplate))?; // show it to the owner
  while let Some(event) = tunnel.next().await {
      match event {
          // The client keys the relay lets through, when the tunnel comes up.
          HostEvent::Registered { pairings, .. } => {}
          // conn.client_key is the key the relay verified; conn.rx and
          // conn.tx carry the client's WebSocket messages, whole. With
          // conn.nameplate set it is a pairing connection: run the pairing,
          // and when host/pair succeeds call handle.paired(&conn.client_key)
          // before answering it.
          HostEvent::Client(conn) => {}
          HostEvent::Unpaired { .. } => {}
      }
  }
  ```

  When `next()` returns `None` the tunnel is gone; reconnect with backoff. Pairings live on the relay and survive. A host whose OAL server is already a WebSocket endpoint can hand each connection to `oal_relay::host::forward(conn, "ws://127.0.0.1:7878/oal")`.

- **Any other host** that serves OAL on a local WebSocket, in any language:

  ```sh
  oal-relay host --relay https://relay.example.com --id studio --key-file ~/.oal/host.key \
      --forward ws://127.0.0.1:7878/oal --nameplate K7QM
  ```

  It keeps the tunnel up (reconnecting with backoff) and carries every client connection to `--forward`. `--nameplate` routes pairing to it for a code the host shows: pass only the code's first four characters. The bridge can't see inside a pairing, so it lets through the relay every device that opened a pairing connection with a live nameplate; the host's own pairing check is what admits it.

## Point a client at it

In Rust:

```rust
let relay = oal_relay::RelayClient::new("https://relay.example.com", device_key)?;
let code = oal_secure::PairingCode::parse("K7QM-3XRD")?;             // typed by the owner
let (ws, pairing) = relay.pair(code.nameplate()).await?;             // only the nameplate goes to the relay
// ... oal_secure::pair(ws, &code, ...) and host/pair inside it ...
let ws = relay.connect(&pairing.host_id).await?;                     // every later connection
let hosts = relay.presence().await?;                                 // online? which agents?
```

`ws` is an ordinary WebSocket to the host: send and receive OAL frames (or `oal-secure`'s binary messages) on it exactly as on a direct connection. `relay.pair` refuses a whole code before sending anything. Clients in other languages follow [the protocol](#protocol) below; it is two HTTP requests per connection. The TypeScript and Python SDKs (`sdk/`) do it for you, and their tests pair and prompt through this relay.

## Operate it

| Command | What it does |
|---|---|
| `oal-relay serve` | Runs the relay. |
| `oal-relay hosts` | Lists hosts, whether each is online, and its pairings. |
| `oal-relay revoke --host <id> --client <key>` | Removes one pairing. The device's connections close with 4003; it can pair again with a new code. |
| `oal-relay revoke --client <key>` | Bans a device key on this relay: every pairing goes and it can never pair again. |
| `oal-relay revoke --host <id>` | Bans a host's key and deletes the host and its pairings. The id is free again. |

`hosts` and `revoke` talk to the running relay's admin API (`--relay`, default `http://127.0.0.1:8480`) with the admin token, which they read from the data directory when run on the relay's machine (in Docker: `docker exec oal-relay /oal-relay hosts`).

| Flag | Environment | Default |
|---|---|---|
| `--listen` | `OAL_RELAY_LISTEN` | `0.0.0.0:8480` |
| `--data-dir` | `OAL_RELAY_DATA_DIR` | the platform data directory, `oal-relay/` (Docker: `/data`) |
| `--tls-cert`, `--tls-key` | `OAL_RELAY_TLS_CERT`, `OAL_RELAY_TLS_KEY` | none: plain HTTP |
| `--admin-token` | `OAL_RELAY_ADMIN_TOKEN` | generated into `<data-dir>/admin.token` on first run |
| `--allow-host` (comma-separated keys) | `OAL_RELAY_ALLOW_HOSTS` | any key may register a free host id |
| `--max-message-bytes` | `OAL_RELAY_MAX_MESSAGE_BYTES` | 16 MiB |
| | `OAL_RELAY_LOG` | `info` |

`GET /health` answers `{"status":"ok"}`. `GET /metrics` has Prometheus counters: hosts online, client connections, pairings, pairing connections, unknown nameplates, failed proofs, refusals, nameplates issued, revocations, and messages and bytes relayed each way. Logs are JSON lines on stdout.

## What the operator can and can't see

The relay forwards every WebSocket message unchanged and never parses one. With end-to-end encryption on (Noise, OAL spec section 17, the `oal-secure` crate), it carries ciphertext.

The operator **can** see:

- each host's id and public key, and when it was last online;
- each pairing: the client's public key and when it paired;
- live nameplates, and which host holds each;
- which client key connects to which host, when, from which IP address, and how many messages and bytes pass each way;
- the agent ids and online states a host chooses to publish for presence.

The operator **can't** see the secret half of any pairing code, the device's name, its token, or prompts, replies, tool calls, permission requests, file names or anything else inside the frames, once they are end-to-end encrypted. Until then (plain OAL 0.1 traffic, `host/pair` included), whoever runs the relay can read and alter it, which is exactly why you might run your own.

The store holds no content and no code secrets, and the relay never learns a device's private key. The relay's own key and the admin token are in the data directory; keep it private.

## Protocol

For SDK authors. Keys are X25519 public keys, 32 bytes, base64url without padding. Every JSON field is camelCase. Every refusal is HTTP 4xx/5xx with `{"code": "...", "message": "..."}`: branch on `code`, show `message`.

### Proving a key

1. `GET /oal/challenge` returns `{"nonce", "relayKey", "expiresIn": 60}`. The nonce works once, for 60 seconds. It and `GET /oal/presence` answer a web page on any origin (`Access-Control-Allow-Origin: *`), so a browser client can reach the relay.
2. Compute
   ```text
   shared = X25519(your_secret, relayKey)
   prk    = HMAC-SHA256(key = "oal-relay-auth/1", message = shared)
   proof  = HMAC-SHA256(key = prk, message =
              "oal-relay-auth/1\n" + role + "\n" + yourKey + "\n" + relayKey + "\n" + nonce + "\n" + target + "\n")
   ```
   `role` is `host` or `client`. `target` names the request: `host:<hostId>` (open a tunnel), `connect:<hostId>`, `pair:<NAMEPLATE>` (the nameplate's 4 characters: uppercase, O read as 0 and I or L as 1) or `presence`.
3. Send `key`, `nonce` and `proof` (base64url) as query parameters. They ride in the URL because a browser cannot set headers on a WebSocket; a proof is single-use and bound to its request, so a logged URL is worthless.

X25519 keys cannot sign, so this is how Noise itself proves a static key. The key a device proves to the relay is the same static key OAL's end-to-end encryption uses.

### Endpoints

| Request | Refusals |
|---|---|
| `GET /oal/hosts/<hostId>` (WebSocket, offer subprotocol `oal`): connect to a host | 401 `unauthenticated`, 403 `not_paired` / `revoked`, 503 `host_offline` |
| `GET /oal/pair/<nameplate>` (WebSocket): a pairing connection to the host holding the nameplate. The 101 carries `OAL-Host-Id` and `OAL-Host-Key` (where the relay routed it; OAL 0.2's handshake authenticates the key). | 400 `bad_nameplate` (more than the nameplate, never routed), 404 `unknown_nameplate` (none held, or expired), 429 `too_many_attempts` (10 a minute per address), 503 `host_offline` (the nameplate stays good) |
| `GET /oal/presence`: `{"hosts": [{"hostId", "online", "lastSeenAt", "agents": [{"id", "online"}]}]}` for the hosts this key paired with | 401 |
| `GET /oal/tunnel/<hostId>` (WebSocket): a host's tunnel | 401, 403 `revoked` / `not_allowed`, 409 `host_id_taken` / `host_key_taken` |
| `GET /admin/hosts`, `POST /admin/revoke {"hostId"?, "clientKey"?}`, with `Authorization: Bearer <admin token>` | 401, 404 |

On a client connection the relay passes every message through whole, in order and unchanged, text and binary (never split or joined), and passes close codes through both ways. It closes a client with 1001 when the host's tunnel drops or the relay shuts down (reconnect), 4003 when the pairing is removed (don't reconnect with that key), 4008 after 60 seconds of silence (it pings every 20), and 1009 for a message over the limit.

### The tunnel

A host's tunnel is a WebSocket whose binary messages carry a [yamux](https://github.com/hashicorp/yamux/blob/master/spec.md) session; the relay is the yamux client. Every stream starts with an `open` frame from the relay; frames are `kind (u8) | length (u32, big-endian) | payload`:

| kind | payload |
|---|---|
| 0 open | JSON: `{"type":"control"}`, or `{"type":"client","clientKey","nameplate"?}` |
| 1 text | one WebSocket text message |
| 2 binary | one WebSocket binary message |
| 3 close | close code (u16, 0 for none), then the reason |

`clientKey` is the key the relay verified that connection holds; `nameplate` is set on a pairing connection. The first stream is the control channel, carrying JSON in text frames. This is the relay-to-host interface the spec leaves to each relay.

Host to relay (each with a `request` number the answer repeats, except `presence`):

| Message | Answer | What it does |
|---|---|---|
| `nameplate_request {request, nameplate?}` | `nameplate {request, nameplate, expiresAt}` | Holds a nameplate for this host: `nameplate` if given and free (or already this host's), else a fresh one. Refused with `nameplate_taken` or `bad_nameplate`. |
| `paired {request, clientKey}` | `done {request}` | The host paired this client; let it through from now on. Send it when `host/pair` succeeds and answer `host/pair` after `done`. |
| `unpair {request, clientKey}` | `done {request}` | Remove the pairing and close the client's connections with 4003. |
| `presence {agents: [{id, online}]}` | none | Replaces the host's published agents. |

Relay to host: `registered {hostId, pairings: [{clientKey, pairedAt}]}` when the tunnel comes up (reconcile it with the host's own devices), `unpaired {clientKey}` when the operator removes a pairing, and `error {request, code, message}` for a refused request.

Both ends ping every 20 seconds and drop a tunnel that has been silent for 60.

## Limits

One process with one SQLite file: it does not share state across replicas. A host may have about 500 connections open at once (yamux's stream limit on its tunnel). Messages up to 16 MiB by default (OAL hosts must accept 4 MiB).

License: Apache-2.0.
