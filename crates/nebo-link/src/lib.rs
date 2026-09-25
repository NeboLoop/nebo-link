//! Nebo Link: connects an OpenClaw or Hermes install to NeboAI. The binary
//! is `nebo-link`; this library is its implementation, split out so the
//! integration tests can drive the proxy directly.

pub mod credentials;
pub mod endpoints;
pub mod error;
pub mod install;
pub mod janus;
pub mod link;
pub mod offsets;
pub mod proxy;
pub mod run;
pub mod service;
pub mod state;
pub mod update;
