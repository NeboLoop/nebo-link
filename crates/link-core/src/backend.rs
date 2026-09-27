//! What the host needs from a runtime: its agents, and ACP to each of them.
//! Every runtime speaks ACP to the host: a coding agent natively
//! ([`crate::acp`]), OpenClaw and Hermes through [`crate::adapter`], which
//! maps them into ACP as Open Agent Link's Appendix A says. So the host keeps
//! one record of every session, in ACP's own messages, whoever reads it: an
//! Open Agent Link client, or the phone contract.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;
use tokio::sync::oneshot;

pub use crate::model::{
    ErrorObject, PermissionOption, SessionModeState, StopReason, ToolCallUpdate, Usage, Words,
};

/// A boxed future, so the trait is object-safe and one host can hold any
/// runtime's backend.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Why a runtime could not be asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The runtime's API is not answering; the owner reads "Could not
    /// connect to <runtime>. Try again."
    Unavailable(String),
    /// The agent, chat or run named does not exist.
    NotFound(String),
    /// The runtime answered, but refused or failed the request.
    Failed(String),
}

impl Error {
    /// The message the owner sees, with the runtime's name in it.
    pub fn message(&self, runtime_name: &str) -> String {
        match self {
            Error::Unavailable(_) => format!("Could not connect to {runtime_name}. Try again."),
            Error::NotFound(what) => format!("{what} was not found."),
            Error::Failed(why) => why.clone(),
        }
    }
}

/// One agent of the runtime: an OpenClaw agent, a Hermes profile, a coding
/// agent in its folder.
#[derive(Debug, Clone, PartialEq)]
pub struct Agent {
    /// The runtime's own id.
    pub id: String,
    pub name: String,
    pub description: String,
    /// The runtime's default agent, which the roster names after its
    /// member.
    pub is_default: bool,
    /// Where its sessions work, for runtimes that have a folder.
    pub folder: Option<String>,
    /// ACP `AgentCapabilities`, as the agent last answered them or as the
    /// adapter provides them.
    pub capabilities: Value,
    /// The modes a new session starts in, when it has modes.
    pub modes: Option<SessionModeState>,
    /// Why the agent can't take a prompt, when its last start failed.
    pub offline_reason: Option<String>,
}

/// What one of the runtime's agents sent the host, beyond its answers.
#[derive(Debug)]
pub struct FromAgent {
    /// The runtime's id of the agent.
    pub agent: String,
    pub message: AgentMessage,
}

#[derive(Debug)]
pub enum AgentMessage {
    /// ACP `session/update`: `update` is its `SessionUpdate`, as sent.
    Update { session_id: String, update: Value },
    /// ACP `session/request_permission`: `params` as sent. The host answers
    /// through `reply`; a reply dropped unanswered is `cancelled`.
    Permission {
        session_id: String,
        params: Value,
        /// The request in the owner's words, where the runtime has its own
        /// (an adapted runtime's question); `None` for an ACP agent's.
        words: Option<Words>,
        reply: Reply,
    },
    /// The agent took back a permission request it sent (ACP
    /// `$/cancel_request`, or an adapted runtime whose question was answered
    /// in its own interface): the host resolves it with nobody's answer.
    Withdrawn { reply: u64 },
}

/// Where the answer to an agent's permission request goes.
#[derive(Debug)]
pub struct Reply {
    /// Unique within the backend: what [`AgentMessage::Withdrawn`] names.
    pub id: u64,
    pub tx: oneshot::Sender<Value>,
}

impl Reply {
    /// A reply and where its answer arrives: the ACP
    /// `RequestPermissionResponse` (`{"outcome": …}`).
    pub fn new(id: u64) -> (Self, oneshot::Receiver<Value>) {
        let (tx, rx) = oneshot::channel();
        (Self { id, tx }, rx)
    }

    pub fn send(self, response: Value) {
        let _ = self.tx.send(response);
    }
}

/// Where a backend hands what its agents send. Called in the order the
/// agents sent it, and before the answer to any request they sent after it
/// resolves: a `session/prompt` that returns has had every update of its turn
/// handed over.
pub type Inbox = Arc<dyn Fn(FromAgent) + Send + Sync>;

/// A runtime behind the host.
pub trait Backend: Send + Sync + 'static {
    /// Whether the runtime can serve its agents now: reachable, and every
    /// feature the host needs present. The error says what is missing.
    fn ready(&self) -> BoxFuture<'_, Result<(), String>>;

    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>>;

    /// Where what the agents send goes from now on. The host calls it when
    /// the backend joins it.
    fn connect(&self, inbox: Inbox);

    /// One ACP request to `agent` (the runtime's id), answered with its
    /// result or its JSON-RPC error: `session/new`, `session/load`,
    /// `session/resume`, `session/list`, `session/prompt`,
    /// `session/set_mode`, `session/set_config_option`, `session/close`,
    /// `session/delete`. An agent that is not running and can't be started
    /// answers `agent_unavailable` with the reason, in plain words.
    fn request<'a>(
        &'a self,
        agent: &'a str,
        method: &'a str,
        params: Value,
    ) -> BoxFuture<'a, Result<Value, ErrorObject>>;

    /// One ACP notification to `agent`: `session/cancel`.
    fn notify(&self, agent: &str, method: &str, params: Value);

    /// Whether the runtime's sessions also change outside the host (its own
    /// interface, its other channels): the host then reads a session afresh
    /// from the runtime whenever a client loads it with nothing running,
    /// rather than from its record. A coding agent's sessions have one
    /// client, the host, so its record is the session.
    fn shared_sessions(&self) -> bool {
        false
    }

    /// The model `session` runs on, or the agent's current model without
    /// one, for the phone contract's `/chats/{id}` and `/models`.
    fn model<'a>(
        &'a self,
        agent: &'a str,
        session: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, Error>>;

    /// Moves the conversation `session` of `agent` to work in `folder`: the
    /// agent starts a new session there (with `mcp`, the MCP servers the
    /// host gives it), and the conversation continues in it from its next
    /// prompt, whose first context is `handoff`. The conversation keeps its
    /// id: what names it reaches the new session, and what the new session
    /// sends names it. A runtime without folders can't.
    fn move_session<'a>(
        &'a self,
        agent: &'a str,
        session: &'a str,
        folder: &'a Path,
        handoff: &'a str,
        mcp: Vec<Value>,
    ) -> BoxFuture<'a, Result<(), ErrorObject>> {
        let _ = (agent, session, folder, handoff, mcp);
        Box::pin(async {
            Err(ErrorObject::new(
                crate::model::code::NOT_PERMITTED,
                "This agent works without a folder, so it can't move to one.",
            ))
        })
    }

    /// The folder the conversation `session` works in, once it moved
    /// ([`Backend::move_session`]); `None` is the agent's own.
    fn session_folder(&self, agent: &str, session: &str) -> Option<PathBuf> {
        let _ = (agent, session);
        None
    }
}
