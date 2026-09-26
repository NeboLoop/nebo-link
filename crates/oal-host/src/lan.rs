//! LAN direct (spec 4.5): the host's own `wss://<address>:<port>/oal`, off
//! unless the owner turns it on. It serves a self-signed certificate, kept
//! with the host's state so its fingerprint stays the same across restarts;
//! `host/info` names the fingerprint (`tlsFingerprint`: SHA-256 of the
//! certificate's DER, base64url), and a client pins it. The host is found by
//! DNS-SD as `_oal._tcp` with `id=<hostId>` and `v=<version>`.
//!
//! Every connection is end-to-end encrypted as through a relay: TLS here is
//! the transport the spec fixes, not what the host trusts.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use base64::Engine;
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::handshake::server::{Callback, ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::StatusCode;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;

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

/// Serves `/oal` on `listen` over TLS with the certificate kept in `dir`
/// (made on first use), until the host shuts down; with `advertise`, DNS-SD
/// announces it.
pub async fn serve(oal: Arc<OalHost>, listen: SocketAddr, dir: &Path, advertise: bool) -> Result<Lan, String> {
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
    oal.set_fingerprint(Some(fingerprint.clone()));
    let mdns = if advertise { announce(&oal, addr.port()) } else { None };
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
        oal.set_fingerprint(None);
        if let Some(mdns) = mdns {
            let _ = mdns.shutdown();
        }
    });
    tracing::info!(%addr, "oal: LAN direct is on");
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
