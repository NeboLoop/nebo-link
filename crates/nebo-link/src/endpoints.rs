//! Where NeboAI lives. The production defaults and the environment overrides
//! are the ones Nebo itself uses (`NEBOAI_API_URL`, `NEBOAI_COMMS_URL`,
//! `NEBOAI_TUNNEL_URL`, `NEBOAI_JANUS_URL`). They are read when a bot is
//! linked and stored with it, so its service talks to the same NeboAI.

/// The browser origin the owner opens a linked UI from. The hub removes the
/// browser's `Origin` before the tunnel, so the link states this one to
/// runtimes that check it, and the runtime is configured to allow it.
pub const WEB_ORIGIN: &str = "https://neboai.com";

/// The NeboAI services the link talks to.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Endpoints {
    /// REST API: pairing.
    pub api: String,
    /// Comms gateway WebSocket: presence and the bot lease.
    pub comms: String,
    /// Tunnel WebSocket: the owner's requests to the linked UI.
    pub tunnel: String,
    /// Janus: NeboAI models (no `/v1`).
    pub janus: String,
}

impl Endpoints {
    /// The production endpoints, each overridable by its environment variable.
    pub fn from_env() -> Self {
        Self::resolve(|name| std::env::var(name).ok())
    }

    fn resolve(var: impl Fn(&str) -> Option<String>) -> Self {
        let pick = |name: &str, default: &str| {
            var(name)
                .map(|v| v.trim().trim_end_matches('/').to_string())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        Self {
            api: pick("NEBOAI_API_URL", "https://api.neboai.com"),
            comms: pick("NEBOAI_COMMS_URL", "wss://comms.neboai.com/ws"),
            tunnel: pick("NEBOAI_TUNNEL_URL", "wss://api.neboai.com/tunnel/connect"),
            janus: pick("NEBOAI_JANUS_URL", "https://janus.neboai.com"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_defaults() {
        let e = Endpoints::resolve(|_| None);
        assert_eq!(e.api, "https://api.neboai.com");
        assert_eq!(e.comms, "wss://comms.neboai.com/ws");
        assert_eq!(e.tunnel, "wss://api.neboai.com/tunnel/connect");
        assert_eq!(e.janus, "https://janus.neboai.com");
    }

    #[test]
    fn overrides_win_and_blank_is_unset() {
        let e = Endpoints::resolve(|name| match name {
            "NEBOAI_JANUS_URL" => Some("http://127.0.0.1:9000/".into()),
            "NEBOAI_API_URL" => Some("  ".into()),
            _ => None,
        });
        assert_eq!(e.janus, "http://127.0.0.1:9000");
        assert_eq!(e.api, "https://api.neboai.com");
    }
}
