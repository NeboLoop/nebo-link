//! link-core: hosting coding agents (Claude Code, Codex, Gemini CLI,
//! OpenCode, any command that speaks ACP) and agent runtimes (OpenClaw,
//! Hermes) on the computer it runs on. It knows nothing of a hub, a tunnel
//! or a relay: the nebo-link daemon carries it to its clients (Open Agent
//! Link, NeboAI), and Nebo embeds it to be the host on its own computer.
//!
//! - [`host::Host`] is the core: the hosted agents, spoken to in ACP, the
//!   record of each open session, one turn at a time per session, and the
//!   permission requests turns stop for, told to clients as [`host::Event`]s
//!   in Open Agent Link's types ([`model`]).
//! - [`roster::Roster`] holds the hosted agents, each a [`roster::Member`]
//!   with its own [`backend::Backend`]: [`acp::Acp`] (which starts and
//!   restarts its agent process), or a runtime adapted into ACP
//!   ([`adapter::Adapted`]: [`openclaw::Openclaw`], [`hermes::Hermes`]).
//! - [`phone::Contract`] serves Nebo's phone contract over a host, until
//!   Nebo's clients speak Open Agent Link.
//! - [`machine`] says who hosts this computer's agents: one host per
//!   computer per OS user.
//!
//! ```no_run
//! # async fn example(member: link_core::roster::Member) {
//! use std::sync::Arc;
//! use link_core::host::{Event, Host};
//! use link_core::roster::Roster;
//! use serde_json::json;
//!
//! let host = Host::new(Arc::new(Roster::new(vec![member])));
//! let mut events = host.subscribe();
//! let agent = host.agents().await.remove(0);
//! let cwd = agent.folder.clone().unwrap_or_else(|| "/".into());
//! let session = host.new_session(&agent.id, json!({ "cwd": cwd, "mcpServers": [] })).await.unwrap();
//! let session = session["sessionId"].as_str().unwrap();
//! let prompt = vec![json!({ "type": "text", "text": "Run the tests" })];
//! host.prompt(&agent.id, session, prompt, None, None).unwrap();
//! while let Ok(stamped) = events.recv().await {
//!     match stamped.event {
//!         Event::Pending(update) => println!("asks about {}", update.request.tool_call.tool_call_id),
//!         Event::Turn(turn) if turn.state == link_core::model::TurnState::Ended => break,
//!         _ => {}
//!     }
//! }
//! # }
//! ```

pub mod acp;
pub mod adapter;
pub mod backend;
pub mod hermes;
pub mod host;
pub mod machine;
pub mod model;
pub mod openclaw;
pub mod phone;
pub mod roster;

/// The id of a host's first agent, which Nebo's phone finds its primary
/// employee by.
pub const PRIMARY: &str = "assistant";
