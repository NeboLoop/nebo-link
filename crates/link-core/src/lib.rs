//! link-core: hosting coding agents (Claude Code, Codex, Gemini CLI,
//! OpenCode, any command that speaks ACP) and agent runtimes (OpenClaw,
//! Hermes) on the computer it runs on. It knows nothing of a hub, a tunnel
//! or a relay: the nebo-link daemon carries it to NeboAI, and Nebo embeds it
//! to be the host on its own computer.
//!
//! - [`host::Host`] is the core: the hosted agents, their sessions, one turn
//!   at a time per session, and the permission requests turns stop for, told
//!   to clients as [`host::Event`]s in Open Agent Link's types ([`model`]).
//! - [`roster::Roster`] holds the hosted agents, each a [`roster::Member`]
//!   with its own runtime [`backend::Backend`]: [`acp::Acp`] (which starts
//!   and restarts its agent process), [`openclaw::Openclaw`],
//!   [`hermes::Hermes`].
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
//!
//! let host = Host::new(Arc::new(Roster::new(vec![member])));
//! let mut events = host.subscribe();
//! let agent = host.agents().await.remove(0);
//! let session = host.new_session(&agent.id).await.unwrap();
//! host.prompt(&agent.id, &session.id, "Run the tests".into(), None, None).unwrap();
//! while let Ok(event) = events.recv().await {
//!     match event {
//!         Event::Pending(update) => println!("asks: {}", update.request.words.question),
//!         Event::Turn(turn) if turn.state == link_core::model::TurnState::Ended => break,
//!         _ => {}
//!     }
//! }
//! # }
//! ```

pub mod acp;
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
