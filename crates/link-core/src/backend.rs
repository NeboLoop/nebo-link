//! What the host needs from a runtime: its agents, their chats and
//! transcripts, and one streamed turn at a time. Each runtime implements
//! this once ([`crate::acp`], [`crate::hermes`], [`crate::openclaw`]);
//! everything a client sees is built from these types by the host, so no
//! runtime shape leaks past this file.

use std::future::Future;
use std::pin::Pin;

use serde_json::Value;
use tokio::sync::mpsc;

pub use crate::model::{PermissionOption, SessionModeState, StopReason, ToolCallUpdate, Usage, Words};

/// A boxed future, so the trait is object-safe and one link can hold any
/// runtime's backend.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Why a backend call failed.
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

/// One conversation: a runtime session.
#[derive(Debug, Clone, PartialEq)]
pub struct Chat {
    /// The runtime's session id.
    pub id: String,
    pub title: String,
    pub preview: String,
    /// Unix seconds of the last activity.
    pub last_active: Option<f64>,
    pub message_count: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    User,
    Assistant,
    /// A tool's output, answering one of an assistant message's calls.
    Tool,
}

/// A stored message of a chat.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    pub id: String,
    pub role: Role,
    pub text: String,
    /// Unix seconds.
    pub created_at: Option<f64>,
    /// The calls an assistant message made, in order.
    pub tool_calls: Vec<ToolCall>,
    /// The result a tool message carries.
    pub tool_result: Option<ToolResult>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub input: Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolResult {
    pub tool_call_id: String,
    pub content: String,
    pub is_error: bool,
}

/// The runtime stopped for the owner's decision: ACP's
/// `session/request_permission`, and the same in the owner's words.
#[derive(Debug, Clone, PartialEq)]
pub struct Ask {
    /// The runtime's id for the request, when it gives one.
    pub request_id: Option<String>,
    /// The call it asks about.
    pub tool_call: ToolCallUpdate,
    /// The answers it offers; an option's id is what it is answered with.
    pub options: Vec<PermissionOption>,
    pub words: Words,
}

/// What a turn emits, in order, ending with exactly one of `Completed`,
/// `Failed` or `Cancelled`.
#[derive(Debug, Clone, PartialEq)]
pub enum TurnEvent {
    Text(String),
    Thinking(String),
    ToolStart {
        id: String,
        name: String,
        input: Value,
    },
    ToolResult {
        id: String,
        name: String,
        result: String,
        is_error: bool,
        duration_ms: Option<u64>,
    },
    Ask(Box<Ask>),
    /// An ask was answered, from anywhere (the runtime's own UI included).
    AskAnswered {
        request_id: Option<String>,
    },
    Completed {
        stop_reason: StopReason,
        usage: Option<Usage>,
    },
    Failed(String),
    Cancelled,
}

/// What the host sends a running turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    Cancel,
    Answer {
        request_id: Option<String>,
        /// A [`PermissionOption::option_id`] of the ask.
        choice: String,
    },
}

/// How much the owner lets the employee do without asking: Nebo's
/// permission mode for it (`types::permissions::Mode`, as Nebo names it on
/// the `chat` frame). A runtime with modes of its own runs the turn in the
/// one this maps to; one without ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    /// Every change asks.
    Ask,
    /// Acts inside its job (its folder); anything else asks.
    Automatic,
    /// Reads and plans; changes nothing.
    Plan,
    /// Nothing asks.
    FullAccess,
}

impl Permission {
    /// Nebo's name for the mode: `ask`, `automatic`, `plan`, `full_access`.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "ask" => Some(Permission::Ask),
            "automatic" => Some(Permission::Automatic),
            "plan" => Some(Permission::Plan),
            "full_access" => Some(Permission::FullAccess),
            _ => None,
        }
    }
}

/// A turn in progress: its events, and the channel to steer it.
pub struct Turn {
    pub events: mpsc::Receiver<TurnEvent>,
    pub control: mpsc::Sender<Control>,
}

/// A runtime behind the host.
pub trait Backend: Send + Sync + 'static {
    /// Whether the runtime can serve chats now: reachable, and every feature
    /// the host needs present. The error says what is missing.
    fn ready(&self) -> BoxFuture<'_, Result<(), String>>;
    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>>;
    /// The agent's chats, most recent first.
    fn chats<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Vec<Chat>, Error>>;
    fn create_chat<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Chat, Error>>;
    /// The chat's transcript, oldest first.
    fn messages<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Message>, Error>>;
    /// The model `chat` runs on, or the agent's current model without one.
    fn model<'a>(
        &'a self,
        agent: &'a str,
        chat: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, Error>>;
    /// Sends `prompt` on `chat` and starts streaming the turn. The runtime
    /// holds the transcript; only the new message is sent. `permission` is
    /// the owner's choice for the employee, when Nebo sent one.
    fn turn<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
        prompt: String,
        permission: Option<Permission>,
    ) -> BoxFuture<'a, Result<Turn, Error>>;
}
