//! LAN direct (spec 4.5): the host's own `wss://<address>:<port>/oal`, off
//! unless the owner turns it on. It serves a self-signed certificate, kept
//! with the host's state so its fingerprint stays the same across restarts;
//! `host/info` names the fingerprint (`tlsFingerprint`: SHA-256 of the
//! certificate's DER, base64url), and a client pins it. The host is found by
//! DNS-SD as `_oal._tcp` with `id=<hostId>` and `v=<version>`.
//!
//! Every connection is end-to-end encrypted as through a relay: TLS here is
//! the transport the spec fixes, not what the host trusts.
//!
//! The same listener serves this computer alone ([`Reach::Machine`]): on
//! loopback, never announced or named in `host/info`, for an app of the
//! host's own OS user, which reads where it is and its fingerprint in the
//! host's state (`link_core::machine::direct`). An app on the same computer
//! then never depends on a relay to reach it.
//!
//! The client's side is here too: [`dial`] opens the pinned connection, and
//! a [`Browser`] finds hosts on the LAN by their id.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
use rustls::{DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::server::{Callback, ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::{MAX_FRAME, OalHost, PROTOCOL, Via, wire};

const CERT_FILE: &str = "lan-cert.pem";
const KEY_FILE: &str = "lan-key.pem";
const SERVICE: &str = "_oal._tcp.local.";

/// LAN direct, serving.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lan {
    pub addr: SocketAddr,
    /// The certificate's fingerprint, as `host/info` names it.
    pub fingerprint: String,
}

/// Who a listener serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reach {
    /// The local network: `host/info` names the fingerprint, and with
    /// `advertise`, DNS-SD announces the host.
    Lan { advertise: bool },
    /// This computer alone, on loopback: its apps read where it is in the
    /// host's state, so nothing names or announces it.
    Machine,
}

/// Serves `/oal` on `listen` over TLS with the certificate kept in `dir`
/// (made on first use), to `reach`, until the host shuts down.
pub async fn serve(oal: Arc<OalHost>, listen: SocketAddr, dir: &Path, reach: Reach) -> Result<Lan, String> {
    if reach == Reach::Machine && !listen.ip().is_loopback() {
        return Err(format!("{listen} isn't on loopback: this computer alone is served there"));
    }
    let (certs, key) = certificate(dir)?;
    let fingerprint = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(certs[0].as_ref()));
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS: {e}"))?
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("the LAN certificate can't be used: {e}"))?;
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| format!("could not listen on {listen}: {e}"))?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let lan = matches!(reach, Reach::Lan { .. });
    if lan {
        oal.set_fingerprint(Some(fingerprint.clone()));
    }
    let mdns = match reach {
        Reach::Lan { advertise: true } => announce(&oal, addr.port()),
        _ => None,
    };
    let mut closing = oal.closing();
    let host = oal.clone();
    tokio::spawn(async move {
        loop {
            let accepted = tokio::select! {
                accepted = listener.accept() => accepted,
                _ = crate::stopped(&mut closing) => break,
            };
            let Ok((tcp, peer)) = accepted else { continue };
            let acceptor = acceptor.clone();
            let oal = host.clone();
            tokio::spawn(async move {
                let Ok(tls) = acceptor.accept(tcp).await else {
                    tracing::debug!(%peer, "oal: a LAN connection failed its TLS handshake");
                    return;
                };
                let config = WebSocketConfig::default()
                    .max_message_size(Some(MAX_FRAME + 1024))
                    .max_frame_size(Some(MAX_FRAME + 1024));
                let Ok(ws) = tokio_tungstenite::accept_hdr_async_with_config(tls, Upgrade, Some(config)).await else {
                    return;
                };
                oal.serve(wire::websocket(ws), Via::Lan).await;
            });
        }
        if lan {
            oal.set_fingerprint(None);
        }
        if let Some(mdns) = mdns {
            let _ = mdns.shutdown();
        }
    });
    match reach {
        Reach::Lan { .. } => tracing::info!(%addr, "oal: LAN direct is on"),
        Reach::Machine => tracing::info!(%addr, "oal: this computer's apps reach the host directly"),
    }
    Ok(Lan { addr, fingerprint })
}

/// The certificate and key in `dir`, made on first use.
fn certificate(dir: &Path) -> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>), String> {
    let cert_path = dir.join(CERT_FILE);
    let key_path = dir.join(KEY_FILE);
    if !cert_path.exists() || !key_path.exists() {
        let made = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .map_err(|e| format!("could not make the LAN certificate: {e}"))?;
        std::fs::create_dir_all(dir).map_err(|e| format!("could not use {}: {e}", dir.display()))?;
        crate::write_private(&key_path, made.key_pair.serialize_pem().as_bytes())
            .map_err(|e| format!("could not save {}: {e}", key_path.display()))?;
        crate::write_private(&cert_path, made.cert.pem().as_bytes())
            .map_err(|e| format!("could not save {}: {e}", cert_path.display()))?;
    }
    let certs = CertificateDer::pem_file_iter(&cert_path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("{} is damaged: {e}", cert_path.display()))?;
    let key = PrivateKeyDer::from_pem_file(&key_path).map_err(|e| format!("{} is damaged: {e}", key_path.display()))?;
    if certs.is_empty() {
        return Err(format!("{} holds no certificate", cert_path.display()));
    }
    Ok((certs, key))
}

/// Announces the host on the LAN (`_oal._tcp`, `id=<hostId>`, `v=<version>`).
fn announce(oal: &OalHost, port: u16) -> Option<mdns_sd::ServiceDaemon> {
    let daemon = match mdns_sd::ServiceDaemon::new() {
        Ok(daemon) => daemon,
        Err(e) => {
            tracing::info!(error = %e, "oal: DNS-SD is not available; clients reach LAN direct by address");
            return None;
        }
    };
    let label: String = oal
        .host_name()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '-' })
        .collect();
    let label = label.trim_matches('-');
    let label = if label.is_empty() { "oal-host" } else { label };
    let properties = [("id", oal.host_id()), ("v", PROTOCOL)];
    let service = mdns_sd::ServiceInfo::new(SERVICE, oal.host_name(), &format!("{label}.local."), "", port, &properties[..])
        .map(mdns_sd::ServiceInfo::enable_addr_auto);
    match service.and_then(|service| daemon.register(service)) {
        Ok(()) => Some(daemon),
        Err(e) => {
            tracing::info!(error = %e, "oal: could not announce LAN direct by DNS-SD");
            let _ = daemon.shutdown();
            None
        }
    }
}

/// The upgrade: only `/oal`, and the `oal` subprotocol selected when the
/// client offers it (spec 4.1).
struct Upgrade;

impl Callback for Upgrade {
    fn on_request(self, req: &Request, mut resp: Response) -> Result<Response, ErrorResponse> {
        if req.uri().path() != "/oal" {
            let mut refused = ErrorResponse::new(Some("Open Agent Link is at /oal.".to_owned()));
            *refused.status_mut() = StatusCode::NOT_FOUND;
            return Err(refused);
        }
        let offered = req
            .headers()
            .get("sec-websocket-protocol")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(',').any(|p| p.trim() == "oal"));
        if offered {
            resp.headers_mut().insert("sec-websocket-protocol", "oal".parse().expect("header"));
        }
        Ok(resp)
    }
}

/// A client's connection to a host's own listener.
pub type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Opens `wss://<addr>/oal`, accepting only the certificate with
/// `fingerprint` (spec 4.5: learned from an authenticated `host/info`, or
/// on this computer from the host's state), and offering the `oal`
/// subprotocol.
pub async fn dial(addr: SocketAddr, fingerprint: &str) -> Result<Socket, String> {
    let mut request = format!("wss://{addr}/oal").into_client_request().map_err(|e| e.to_string())?;
    request.headers_mut().insert("sec-websocket-protocol", "oal".parse().expect("header"));
    let config = WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME + 1024))
        .max_frame_size(Some(MAX_FRAME + 1024));
    let connector = tokio_tungstenite::Connector::Rustls(Arc::new(pinned(fingerprint)));
    let (socket, _) = tokio_tungstenite::connect_async_tls_with_config(request, Some(config), false, Some(connector))
        .await
        .map_err(|e| e.to_string())?;
    Ok(socket)
}

/// TLS that accepts only the certificate with `fingerprint`.
fn pinned(fingerprint: &str) -> rustls::ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .expect("the ring provider supports the default versions")
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Pinned {
            fingerprint: fingerprint.to_owned(),
            provider,
        }))
        .with_no_client_auth()
}

#[derive(Debug)]
struct Pinned {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fingerprint = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(Sha256::digest(end_entity.as_ref()));
        if fingerprint == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("the host's certificate isn't the one it named".into()))
        }
    }

    fn verify_tls12_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn verify_tls13_signature(&self, message: &[u8], cert: &CertificateDer<'_>, dss: &DigitallySignedStruct) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.provider.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider.signature_verification_algorithms.supported_schemes()
    }
}

/// Each host a [`Browser`] heard, by its DNS-SD name: its id and where it
/// listens.
type Heard = Arc<Mutex<HashMap<String, (String, Vec<SocketAddr>)>>>;

/// Finds hosts that serve LAN direct, by DNS-SD (`_oal._tcp`, `id=<hostId>`),
/// for as long as it lives: what it has heard is at hand at once.
pub struct Browser {
    daemon: mdns_sd::ServiceDaemon,
    started: Instant,
    heard: Heard,
    changed: Arc<tokio::sync::Notify>,
}

impl Browser {
    /// Starts listening for hosts; `Err` when DNS-SD isn't available here.
    pub fn start() -> Result<Self, String> {
        let daemon = mdns_sd::ServiceDaemon::new().map_err(|e| e.to_string())?;
        let events = daemon.browse(SERVICE).map_err(|e| e.to_string())?;
        let heard: Heard = Arc::default();
        let changed = Arc::new(tokio::sync::Notify::new());
        let (hosts, notify) = (heard.clone(), changed.clone());
        std::thread::spawn(move || {
            while let Ok(event) = events.recv() {
                match event {
                    mdns_sd::ServiceEvent::ServiceResolved(info) => {
                        let Some(id) = info.get_property_val_str("id").map(str::to_owned) else { continue };
                        let mut addrs: Vec<SocketAddr> = info
                            .get_addresses()
                            .iter()
                            .filter(|ip| dialable(ip))
                            .map(|ip| SocketAddr::new(*ip, info.get_port()))
                            .collect();
                        addrs.sort_by_key(|a| (a.is_ipv6(), *a));
                        hosts.lock().expect("hosts heard").insert(info.get_fullname().to_owned(), (id, addrs));
                        notify.notify_waiters();
                    }
                    mdns_sd::ServiceEvent::ServiceRemoved(_, fullname) => {
                        hosts.lock().expect("hosts heard").remove(&fullname);
                    }
                    _ => {}
                }
            }
        });
        Ok(Self {
            daemon,
            started: Instant::now(),
            heard,
            changed,
        })
    }

    /// Where the host `host_id` listens on the LAN, as last heard. Until the
    /// browser has listened for `wait`, waits for it to be heard; after, a
    /// host not heard is not on this network.
    pub async fn find(&self, host_id: &str, wait: Duration) -> Vec<SocketAddr> {
        let deadline = tokio::time::Instant::from_std(self.started + wait);
        loop {
            let changed = self.changed.notified();
            let found: Vec<SocketAddr> = self
                .heard
                .lock()
                .expect("hosts heard")
                .values()
                .filter(|(id, _)| id == host_id)
                .flat_map(|(_, addrs)| addrs.iter().copied())
                .collect();
            if !found.is_empty() || tokio::time::Instant::now() >= deadline {
                return found;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return Vec::new();
            }
        }
    }
}

/// An address a client can dial as DNS-SD gives it: a link-local IPv6
/// address comes without the interface it is on.
fn dialable(ip: &std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(_) => true,
        std::net::IpAddr::V6(v6) => v6.segments()[0] & 0xffc0 != 0xfe80,
    }
}

impl Drop for Browser {
    fn drop(&mut self) {
        let _ = self.daemon.shutdown();
    }
}
