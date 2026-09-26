//! The chat contract on the link's loopback listener beside `/_link/`:
//! link-core's phone contract ([`link_core::phone`]) served over HTTP and the
//! `/ws` socket, for every agent the bot hosts. The proxy hands it
//! `/health`, `/api/v1/*` and `/ws`. An approval the runtime stops for is
//! also an item in the owner's hub inbox (`POST /api/v1/bots/self/inbox`),
//! resolved there when it is answered anywhere.

mod ws;

use std::sync::Arc;

use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use hyper::{Method, Request, Response, StatusCode};
use link_core::phone::{self, Contract, InboxItem};
use nebo_comm::api::NeboAIApi;
use serde_json::{Value, json};
use tokio::sync::watch;

use crate::proxy::{Body, json};

/// The paths the contract serves; everything else on the listener is the
/// runtime's UI.
pub fn routes(path: &str) -> bool {
    path == "/health" || path == "/ws" || path == "/api/v1" || path.starts_with("/api/v1/")
}

/// The owner's hub inbox, reached with the bot's token.
pub struct Inbox {
    api: Arc<NeboAIApi>,
    bot_id: String,
    /// The current bot token; the hub rotates it on every connect.
    token: watch::Receiver<String>,
}

impl Inbox {
    pub fn new(api_url: &str, bot_id: &str, token: watch::Receiver<String>) -> Self {
        let api = NeboAIApi::new(
            api_url.to_owned(),
            bot_id.to_owned(),
            token.borrow().clone(),
        );
        Self {
            api: Arc::new(api),
            bot_id: bot_id.to_owned(),
            token,
        }
    }
}

impl phone::Inbox for Inbox {
    /// Tells the hub off the socket's path: a hub that is slow or down never
    /// holds a turn.
    fn post(&self, item: InboxItem) {
        let item = match item {
            InboxItem::Approval {
                id,
                title,
                body,
                agent_id,
                chat_id,
            } => json!({
                "id": id,
                "type": "approval",
                "title": title,
                "body": body,
                "link": format!("/t/{}/{agent_id}/threads/{chat_id}", self.bot_id),
                "agentId": agent_id,
                "chatId": chat_id,
            }),
            InboxItem::Resolved { id } => json!({ "id": id, "resolved": true }),
        };
        let api = self.api.clone();
        let token = self.token.borrow().clone();
        tokio::spawn(async move {
            api.set_token(token);
            if let Err(e) = api.push_inbox_item(&item).await {
                tracing::info!(error = %e, "the hub did not take the inbox item");
            }
        });
    }
}

/// Serves one request on a contract path ([`routes`]).
pub async fn handle(contract: &Arc<Contract>, mut req: Request<Incoming>) -> Response<Body> {
    let path = req.uri().path().to_owned();
    if path == "/ws" {
        return ws::upgrade(contract.clone(), &mut req);
    }
    let result = if req.method() == Method::GET && path == "/health" {
        Ok(health(contract).await)
    } else {
        contract.rest(req.method().as_str(), &path).await
    };
    // Bodies are read for nothing: every write the contract takes is a
    // frame on the socket or an id in the path.
    let _ = Limited::new(req.into_body(), 64 << 10).collect().await;
    match result {
        Ok(value) => json(StatusCode::OK, &value),
        Err(refusal) => json(
            StatusCode::from_u16(refusal.status).unwrap_or(StatusCode::BAD_GATEWAY),
            &json!({ "error": refusal.message }),
        ),
    }
}

async fn health(contract: &Contract) -> Value {
    json!({
        "version": crate::update::VERSION,
        "runtime": contract.runtime_key(),
        "chat": contract.ready().await.is_ok(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_paths() {
        assert!(routes("/health"));
        assert!(routes("/ws"));
        assert!(routes("/api/v1/agents"));
        assert!(!routes("/api/ws"));
        assert!(!routes("/"));
        assert!(!routes("/healthz"));
    }
}
