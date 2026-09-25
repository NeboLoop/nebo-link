//! Local agent runtimes Nebo Link can connect: where they live, which ports
//! they serve, and the config changes the link makes to them (recorded so
//! every change can be undone exactly).

/// The runtimes Nebo Link supports. The string form is what the hub stores
/// in `bots.runtime`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Runtime {
    Openclaw,
    Hermes,
}
