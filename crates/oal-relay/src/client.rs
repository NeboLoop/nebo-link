//! Reaching a relay, as a client or as a host.
//!
//! Every request starts with a challenge (`GET /oal/challenge`) and carries a
//! proof that the caller holds its key ([`crate::auth`]). The proof rides in
//! the query string, because a browser cannot set headers on a WebSocket; it
//! is single-use and bound to its request, so a logged URL is worthless.

use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::Response;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::auth::{self, Keypair, Role, Target};
use crate::host::{self, HostTunnel};
use crate::wire::{AgentPresence, DEFAULT_MAX_MESSAGE_BYTES, ErrorBody, normalize_nameplate};

/// A WebSocket to a host, through the relay. Send and receive OAL frames (or
/// `oal-secure`'s ciphertext) on it exactly as on a direct connection.
pub type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// The response header carrying the paired host's id, on a pairing upgrade.
pub const HOST_ID_HEADER: &str = "oal-host-id";
/// The response header carrying the paired host's X25519 static public key.
pub const HOST_KEY_HEADER: &str = "oal-host-key";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The relay refused, in plain words (`message` is for people; branch on
    /// `code`: `unauthenticated`, `unknown_nameplate`, `not_paired`,
    /// `host_offline`, `revoked`, `not_allowed`, `host_id_taken`, ...).
    #[error("{message}")]
    Refused { code: String, message: String },
    /// The relay URL cannot be used.
    #[error("{0}")]
    Url(String),
    /// The relay answered something this client cannot use.
    #[error("{0}")]
    Protocol(String),
    #[error("could not reach the relay: {0}")]
    Http(#[from] reqwest::Error),
    #[error("could not reach the relay: {0}")]
    Ws(Box<tokio_tungstenite::tungstenite::Error>),
    #[error("the relay's tunnel closed")]
    Closed,
}

impl From<tokio_tungstenite::tungstenite::Error> for Error {
    fn from(e: tokio_tungstenite::tungstenite::Error) -> Self {
        Error::Ws(Box::new(e))
    }
}

/// Where the relay routed a pairing connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pairing {
    pub host_id: String,
    /// The host's X25519 static public key (base64url, no padding), as the
    /// host registered it with the relay.
    pub host_key: String,
}

/// A paired host's presence (`GET /oal/presence`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HostPresence {
    pub host_id: String,
    pub online: bool,
    /// RFC 3339: when the relay last heard from the host.
    pub last_seen_at: String,
    /// What the host published; empty while it is offline.
    pub agents: Vec<AgentPresence>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Challenge {
    nonce: String,
    relay_key: String,
}

#[derive(Deserialize)]
struct PresenceBody {
    hosts: Vec<HostPresence>,
}

/// A relay, reached as the holder of `key`.
pub struct RelayClient {
    base: reqwest::Url,
    http: reqwest::Client,
    key: Keypair,
}

impl RelayClient {
    /// `url` is the relay's base URL: `https://relay.example.com`. Plain
    /// `http://` is accepted only for a relay on this machine.
    pub fn new(url: &str, key: Keypair) -> Result<Self, Error> {
        let mut base = reqwest::Url::parse(url.trim_end_matches('/'))
            .map_err(|e| Error::Url(format!("{url} is not a URL: {e}")))?;
        let host = base.host_str().unwrap_or("");
        let loopback = host == "localhost"
            || host
                .trim_start_matches('[')
                .trim_end_matches(']')
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        match base.scheme() {
            "https" => {}
            "http" if loopback => {}
            "http" => {
                return Err(Error::Url(format!(
                    "{url} is not encrypted. Use https:// (or http:// only for a relay on this machine)."
                )));
            }
            other => {
                return Err(Error::Url(format!(
                    "{url}: expected https://, got {other}://"
                )));
            }
        }
        base.set_query(None);
        base.set_fragment(None);
        Ok(Self {
            base,
            http: reqwest::Client::new(),
            key,
        })
    }

    /// The key this client proves it holds.
    pub fn key(&self) -> &Keypair {
        &self.key
    }

    /// Opens a pairing connection to the host that holds `nameplate`
    /// (`/oal/pair/<nameplate>`): the first four characters of the code
    /// (`oal_secure::PairingCode::nameplate`). Only the nameplate: a whole
    /// code is refused here, because its second half is the secret and never
    /// goes to the relay. Run the pairing (spec section 17.5, or `host/pair`
    /// in 0.1) on the returned socket; once the host accepts it, this key can
    /// [`connect`](Self::connect) to that host.
    ///
    /// [`Pairing`] is where the relay routed the connection. In OAL 0.2 the
    /// pairing handshake authenticates the host's key; trust that one.
    pub async fn pair(&self, nameplate: &str) -> Result<(WsStream, Pairing), Error> {
        let nameplate = normalize_nameplate(nameplate).ok_or_else(|| {
            Error::Url(
                "pair takes the nameplate: the first 4 characters of the code, never the whole code"
                    .into(),
            )
        })?;
        let (ws, response) = self
            .dial(
                &format!("/oal/pair/{nameplate}"),
                Role::Client,
                Target::Pair(&nameplate),
            )
            .await?;
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        match (header(HOST_ID_HEADER), header(HOST_KEY_HEADER)) {
            (Some(host_id), Some(host_key)) => Ok((ws, Pairing { host_id, host_key })),
            _ => Err(Error::Protocol(
                "the relay opened a pairing connection but did not say to which host".into(),
            )),
        }
    }

    /// Opens a connection to a host this key paired with (`/oal/hosts/<id>`).
    pub async fn connect(&self, host_id: &str) -> Result<WsStream, Error> {
        let (ws, _) = self
            .dial(
                &format!("/oal/hosts/{host_id}"),
                Role::Client,
                Target::Connect(host_id),
            )
            .await?;
        Ok(ws)
    }

    /// The presence of every host this key paired with.
    pub async fn presence(&self) -> Result<Vec<HostPresence>, Error> {
        let challenge = self.challenge().await?;
        let proof = self.prove(Role::Client, &challenge, Target::Presence)?;
        let mut url = self.url("/oal/presence");
        url.query_pairs_mut()
            .append_pair("key", &self.key.public_b64())
            .append_pair("nonce", &challenge.nonce)
            .append_pair("proof", &proof);
        let response = self.http.get(url).send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.bytes().await.unwrap_or_default();
            return Err(refused(status, &body));
        }
        Ok(response.json::<PresenceBody>().await?.hosts)
    }

    /// Opens this host's tunnel under `host_id`. The first time, the relay
    /// registers the id to this key; afterwards only this key may use it.
    pub async fn host(&self, host_id: &str) -> Result<HostTunnel, Error> {
        let (ws, _) = self
            .dial(
                &format!("/oal/tunnel/{host_id}"),
                Role::Host,
                Target::Host(host_id),
            )
            .await?;
        Ok(host::start(ws, DEFAULT_MAX_MESSAGE_BYTES))
    }

    async fn challenge(&self) -> Result<Challenge, Error> {
        let response = self.http.get(self.url("/oal/challenge")).send().await?;
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let body = response.bytes().await.unwrap_or_default();
            return Err(refused(status, &body));
        }
        Ok(response.json().await?)
    }

    fn prove(&self, role: Role, c: &Challenge, target: Target<'_>) -> Result<String, Error> {
        auth::prove(&self.key, role, &c.relay_key, &c.nonce, target)
            .ok_or_else(|| Error::Protocol("the relay's key is not a usable X25519 key".into()))
    }

    fn url(&self, path: &str) -> reqwest::Url {
        let mut url = self.base.clone();
        let joined = format!("{}{path}", url.path().trim_end_matches('/'));
        url.set_path(&joined);
        url
    }

    async fn dial(
        &self,
        path: &str,
        role: Role,
        target: Target<'_>,
    ) -> Result<(WsStream, Response<Option<Vec<u8>>>), Error> {
        let challenge = self.challenge().await?;
        let proof = self.prove(role, &challenge, target)?;
        let mut url = self.url(path);
        let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
        url.set_scheme(scheme)
            .expect("ws and wss are valid schemes");
        url.query_pairs_mut()
            .append_pair("key", &self.key.public_b64())
            .append_pair("nonce", &challenge.nonce)
            .append_pair("proof", &proof);
        let mut request = url.as_str().into_client_request()?;
        if role == Role::Client {
            request
                .headers_mut()
                .insert("sec-websocket-protocol", "oal".parse().expect("header"));
        }
        match tokio_tungstenite::connect_async(request).await {
            Ok(ok) => Ok(ok),
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                let status = response.status().as_u16();
                Err(refused(
                    status,
                    response.body().as_deref().unwrap_or_default(),
                ))
            }
            Err(e) => Err(e.into()),
        }
    }
}

fn refused(status: u16, body: &[u8]) -> Error {
    match serde_json::from_slice::<ErrorBody>(body) {
        Ok(ErrorBody { code, message }) => Error::Refused { code, message },
        Err(_) => Error::Refused {
            code: format!("http_{status}"),
            message: format!("The relay answered {status}."),
        },
    }
}
