//! What the contract server needs from a runtime: its agents, their chats
//! and transcripts, and one streamed turn at a time. Each runtime implements
//! this once ([`super::hermes`]); everything the phone sees is rendered from
//! these types by the contract, so no runtime shape leaks past this file.

use std::future::Future;
use std::pin::Pin;

use serde_json::Value;
use tokio::sync::mpsc;

/// A boxed future, so the trait is object-safe and one link can hold any
/// runtime's backend.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Why a backend call failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// The runtime's API is not answering; the phone reads "Could not
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

/// One agent of the runtime: an OpenClaw agent, a Hermes profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Agent {
    /// The runtime's own id.
    pub id: String,
    pub name: String,
    pub description: String,
    /// The runtime's default agent, which the contract exposes as
    /// `assistant`.
    pub is_default: bool,
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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
}

/// One answer the owner can give to an [`Ask`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Choice {
    /// What the runtime is answered with.
    pub value: String,
    /// What the owner reads on the card.
    pub label: String,
}

/// The runtime stopped for the owner's decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ask {
    /// The runtime's id for the request, when it gives one.
    pub request_id: Option<String>,
    /// What is being asked, as the card shows it.
    pub prompt: String,
    /// What the runtime wants to do, for the inbox item's title.
    pub summary: String,
    pub choices: Vec<Choice>,
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
    Ask(Ask),
    /// An ask was answered, from anywhere (the runtime's own UI included).
    AskAnswered {
        request_id: Option<String>,
    },
    Completed {
        usage: Option<Usage>,
    },
    Failed(String),
    Cancelled,
}

/// What the contract sends a running turn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Control {
    Cancel,
    Answer {
        request_id: Option<String>,
        /// A [`Choice::value`] of the ask.
        choice: String,
    },
}

/// A turn in progress: its events, and the channel to steer it.
pub struct Turn {
    pub events: mpsc::Receiver<TurnEvent>,
    pub control: mpsc::Sender<Control>,
}

/// A runtime behind the contract.
pub trait Backend: Send + Sync + 'static {
    /// Whether the runtime can serve chats now: reachable, and every feature
    /// the contract needs present. The error says what is missing.
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
    /// holds the transcript; only the new message is sent.
    fn turn<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
        prompt: String,
    ) -> BoxFuture<'a, Result<Turn, Error>>;
}
