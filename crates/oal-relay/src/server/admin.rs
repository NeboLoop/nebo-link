//! The operator's API, behind the admin token (`Authorization: Bearer ...`).
//! The operator issues no pairing codes: a code's secret half is made by the
//! device that shows it and never reaches the relay (spec section 6.2).
//!
//! - `GET /admin/hosts`: every host, online or not, its key, its pairings and
//!   the agents it published.
//! - `POST /admin/revoke`: `{"hostId","clientKey"}` removes one pairing;
//!   `{"clientKey"}` bans a client key relay-wide; `{"hostId"}` bans a host's
//!   key and deletes the host (its id becomes free).

use std::sync::Arc;

use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, StatusCode};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::metrics::inc;
use super::relay::{CLOSE_UNPAIRED, fp};
use super::{Resp, State, json, refuse};
use crate::time::{now, rfc3339};

const MAX_BODY: usize = 64 * 1024;

pub(crate) fn digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RevokeRequest {
    host_id: Option<String>,
    client_key: Option<String>,
}

pub(crate) async fn route(state: Arc<State>, req: Request<Incoming>, path: &str) -> Resp {
    let token = req
        .headers()
        .get(hyper::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));
    // Compared as digests, so the comparison's timing says nothing about the
    // token.
    if token.map(digest) != Some(state.admin_digest) {
        return refuse(
            StatusCode::UNAUTHORIZED,
            "unauthenticated",
            "The admin token is missing or wrong.",
        );
    }
    let method = req.method().clone();
    match (method, path) {
        (Method::GET, "hosts") => hosts(&state),
        (Method::POST, "revoke") => match body::<RevokeRequest>(req).await {
            Ok(r) => revoke(&state, r).await,
            Err(resp) => *resp,
        },
        _ => refuse(StatusCode::NOT_FOUND, "not_found", "There's nothing here."),
    }
}

async fn body<T: serde::de::DeserializeOwned>(req: Request<Incoming>) -> Result<T, Box<Resp>> {
    let bytes = Limited::new(req.into_body(), MAX_BODY)
        .collect()
        .await
        .map_err(|_| {
            Box::new(refuse(
                StatusCode::PAYLOAD_TOO_LARGE,
                "bad_request",
                "The request body is too large.",
            ))
        })?
        .to_bytes();
    serde_json::from_slice(&bytes).map_err(|e| {
        Box::new(refuse(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("The request body is not valid: {e}"),
        ))
    })
}

fn hosts(state: &State) -> Resp {
    let rows = match state.store.hosts() {
        Ok(rows) => rows,
        Err(e) => {
            return super::refuse(
                StatusCode::SERVICE_UNAVAILABLE,
                "relay_unavailable",
                e.to_string(),
            );
        }
    };
    let mut hosts = Vec::with_capacity(rows.len());
    for h in rows {
        let session = state.host_session(&h.id);
        let pairings = state.store.pairings(&h.id).unwrap_or_default();
        hosts.push(serde_json::json!({
            "hostId": h.id,
            "hostKey": h.public_key,
            "online": session.is_some(),
            "lastSeenAt": rfc3339(if session.is_some() { now() } else { h.last_seen_at }),
            "agents": session.map(|s| s.agents()).unwrap_or_default(),
            "pairings": pairings,
        }));
    }
    json(StatusCode::OK, &serde_json::json!({ "hosts": hosts }))
}

async fn revoke(state: &State, r: RevokeRequest) -> Resp {
    let now = now();
    let unavailable = |e: rusqlite::Error| {
        refuse(
            StatusCode::SERVICE_UNAVAILABLE,
            "relay_unavailable",
            e.to_string(),
        )
    };
    if r.client_key
        .as_deref()
        .is_some_and(|k| crate::auth::decode_key(k).is_none())
    {
        return refuse(
            StatusCode::BAD_REQUEST,
            "bad_request",
            "That isn't a client key.",
        );
    }
    let result = match (r.host_id.as_deref(), r.client_key.as_deref()) {
        (Some(host), Some(client)) => match state.store.unpair(host, client) {
            Ok(true) => {
                state.kill_clients(Some(host), Some(client), CLOSE_UNPAIRED);
                state.notify_unpaired(host, client).await;
                tracing::info!(host = %host, client = %fp(client), "operator removed a pairing");
                serde_json::json!({"revoked": "pairing", "hostId": host, "clientKey": client})
            }
            Ok(false) => {
                return refuse(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "That device isn't paired with that host.",
                );
            }
            Err(e) => return unavailable(e),
        },
        (None, Some(client)) => match state.store.revoke_client(client, now) {
            Ok(hosts) => {
                state.kill_clients(None, Some(client), CLOSE_UNPAIRED);
                for host in &hosts {
                    state.notify_unpaired(host, client).await;
                }
                tracing::info!(client = %fp(client), hosts = hosts.len(), "operator revoked a client key");
                serde_json::json!({"revoked": "client", "clientKey": client, "hosts": hosts})
            }
            Err(e) => return unavailable(e),
        },
        (Some(host), None) => match state.store.revoke_host(host, now) {
            Ok(Some(_)) => {
                state.kill_clients(Some(host), None, CLOSE_UNPAIRED);
                if let Some(session) = state.host_session(host) {
                    session.close_tunnel();
                }
                tracing::info!(host = %host, "operator revoked a host");
                serde_json::json!({"revoked": "host", "hostId": host})
            }
            Ok(None) => {
                return refuse(
                    StatusCode::NOT_FOUND,
                    "unknown_host",
                    format!("No host called {host} has registered with this relay."),
                );
            }
            Err(e) => return unavailable(e),
        },
        (None, None) => {
            return refuse(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "Name a hostId, a clientKey, or both.",
            );
        }
    };
    inc(&state.metrics.revocations);
    json(StatusCode::OK, &result)
}
