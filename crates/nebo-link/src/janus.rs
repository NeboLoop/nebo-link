//! NeboAI models for the linked runtime: an OpenAI-compatible endpoint on
//! loopback (`/v1/chat/completions`, `/v1/models`) that forwards to Janus
//! with the bot's token. The runtime presents the link's local key; the bot
//! token never leaves the link.

use std::convert::Infallible;
use std::sync::Arc;

use futures::TryStreamExt;
use http_body_util::{BodyDataStream, BodyExt, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper::header::{self, HeaderValue};
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tokio::sync::watch;

use crate::proxy::{Body, BoxError, constant_time_eq, json};

/// What Janus records the calls as.
const PURPOSE: &str = "linked_runtime";

/// The model the runtime starts on when Janus lists it.
const PREFERRED_MODEL: &str = "nebo-1";

#[derive(Clone)]
pub struct Janus {
    /// Janus root (no `/v1`).
    pub url: String,
    pub bot_id: String,
    /// The current bot token; the hub rotates it on every connect.
    pub token: watch::Receiver<String>,
    /// The key the runtime must present.
    pub key: String,
    pub client: reqwest::Client,
}

impl Janus {
    fn request(&self, method: reqwest::Method, path_and_query: &str) -> reqwest::RequestBuilder {
        self.client
            .request(method, format!("{}{}", self.url, path_and_query))
            .bearer_auth(self.token.borrow().as_str())
            .header("X-Bot-ID", &self.bot_id)
            .header("X-Purpose", PURPOSE)
    }
}

pub fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()
        .expect("http client")
}

/// Serves the endpoint on `listener` until the task is dropped.
pub async fn serve(listener: TcpListener, janus: Janus) {
    let janus = Arc::new(janus);
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            // Out of file descriptors and the like: let it clear.
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            continue;
        };
        let janus = janus.clone();
        tokio::spawn(async move {
            let service = hyper::service::service_fn(move |req| handle(req, janus.clone()));
            let _ = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

async fn handle(req: Request<Incoming>, janus: Arc<Janus>) -> Result<Response<Body>, Infallible> {
    let presented = req
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .is_some_and(|key| constant_time_eq(key.trim().as_bytes(), janus.key.as_bytes()));
    if !presented {
        return Ok(error(StatusCode::UNAUTHORIZED, "invalid api key"));
    }
    let method = match (req.method(), req.uri().path()) {
        (&Method::GET, "/v1/models") => reqwest::Method::GET,
        (&Method::POST, "/v1/chat/completions") => reqwest::Method::POST,
        _ => return Ok(error(StatusCode::NOT_FOUND, "not found")),
    };
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_default();
    let mut upstream = janus.request(method, &path_and_query);
    for name in [header::CONTENT_TYPE, header::ACCEPT] {
        if let Some(value) = req.headers().get(&name) {
            upstream = upstream.header(name, value);
        }
    }
    let body = BodyDataStream::new(req.into_body());
    let resp = match upstream.body(reqwest::Body::wrap_stream(body)).send().await {
        Ok(resp) => resp,
        Err(e) => {
            tracing::info!(error = %e.without_url(), "janus: request failed");
            return Ok(error(StatusCode::BAD_GATEWAY, "Could not connect to NeboAI. Try again."));
        }
    };

    let mut out = Response::builder().status(resp.status().as_u16());
    for name in [header::CONTENT_TYPE, header::CACHE_CONTROL] {
        if let Some(value) = resp.headers().get(name.as_str())
            && let Ok(value) = HeaderValue::from_bytes(value.as_bytes())
        {
            out = out.header(name, value);
        }
    }
    let stream = resp
        .bytes_stream()
        .map_ok(Frame::data)
        .map_err(|e| BoxError::from(e.without_url()));
    Ok(out
        .body(BodyExt::boxed(StreamBody::new(stream)))
        .expect("valid response"))
}

/// The models Janus serves to this bot, NeboAI's own first.
pub async fn models(janus: &Janus) -> Result<(Vec<nebo_runtimes::Model>, String), String> {
    #[derive(serde::Deserialize)]
    struct Listing {
        data: Vec<Entry>,
    }
    #[derive(serde::Deserialize)]
    struct Entry {
        id: String,
        #[serde(default)]
        name: String,
        #[serde(default)]
        owned_by: String,
    }
    let resp = janus
        .request(reqwest::Method::GET, "/v1/models")
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await
        .map_err(|e| format!("Could not connect to NeboAI: {}", e.without_url()))?;
    if !resp.status().is_success() {
        return Err(format!("NeboAI refused the model list ({})", resp.status()));
    }
    let listing: Listing = resp
        .json()
        .await
        .map_err(|e| format!("NeboAI sent an unreadable model list: {}", e.without_url()))?;
    let models: Vec<nebo_runtimes::Model> = listing
        .data
        .into_iter()
        .filter(|m| m.owned_by == "neboai")
        .map(|m| nebo_runtimes::Model {
            name: if m.name.is_empty() { m.id.clone() } else { m.name },
            id: m.id,
        })
        .collect();
    let default = models
        .iter()
        .find(|m| m.id == PREFERRED_MODEL)
        .or(models.first())
        .map(|m| m.id.clone())
        .ok_or_else(|| "NeboAI listed no models for this bot".to_string())?;
    Ok((models, default))
}

fn error(status: StatusCode, message: &str) -> Response<Body> {
    json(status, &serde_json::json!({ "error": { "message": message } }))
}
