use crate::{Error, Runtime, RuntimeCommand};

/// A named set of config changes the link applies to a runtime, and later
/// reverts through the [`Journal`](crate::Journal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Change {
    /// Let the link serve the runtime's UI behind a base path with no
    /// sign-in or pairing prompt of the runtime's own.
    ProxyAccess(ProxyAccess),
    /// Point the runtime's default model at a local OpenAI-compatible
    /// endpoint.
    NeboaiModels(NeboaiModels),
}

impl Change {
    /// The journal key of this change.
    pub fn kind(&self) -> ChangeKind {
        match self {
            Change::ProxyAccess(_) => ChangeKind::ProxyAccess,
            Change::NeboaiModels(_) => ChangeKind::NeboaiModels,
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        let invalid = |message: &str| Err(Error::InvalidChange(message.to_owned()));
        match self {
            Change::ProxyAccess(access) => {
                let path = &access.base_path;
                if !path.starts_with('/') || path.len() < 2 || path.ends_with('/') {
                    return invalid("base_path must look like /t/<botId>");
                }
                for (name, value) in [
                    ("origin", &access.origin),
                    ("user_header", &access.user_header),
                    ("identity", &access.identity),
                    ("password", &access.password),
                ] {
                    if value.trim().is_empty() {
                        return Err(Error::InvalidChange(format!("{name} is empty")));
                    }
                }
                Ok(())
            }
            Change::NeboaiModels(models) => {
                if models.base_url.trim().is_empty() {
                    return invalid("base_url is empty");
                }
                if !models
                    .models
                    .iter()
                    .any(|model| model.id == models.default_model)
                {
                    return invalid("default_model is not one of models");
                }
                Ok(())
            }
        }
    }
}

/// The name of a [`Change`], used to revert it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeKind {
    ProxyAccess,
    NeboaiModels,
}

/// Parameters of [`Change::ProxyAccess`].
///
/// OpenClaw: serves the Control UI under `base_path`, switches
/// `gateway.auth.mode` to `trusted-proxy` with loopback trusted, grants
/// `identity` admin through `gateway.auth.identityScopes`, auto-approves
/// browser devices that arrive through the proxy, and sets
/// `gateway.auth.password` so the owner's own CLI and direct loopback browser
/// keep working (trusted-proxy mode would lock them out otherwise).
///
/// Hermes: nothing to write. The dashboard takes its prefix per request from
/// `X-Forwarded-Prefix` and needs no sign-in on loopback; setting
/// `dashboard.public_url` would turn its auth gate on. Applying the change to
/// Hermes is a no-op; gate on [`Installation::proxy_supported`](crate::Installation::proxy_supported).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyAccess {
    /// The path prefix the browser sees, e.g. `/t/<botId>`.
    pub base_path: String,
    /// The browser origin the UI is opened from, e.g. `https://neboai.com`
    /// (added to `gateway.controlUi.allowedOrigins`).
    pub origin: String,
    /// The header that carries the signed-in identity
    /// (`gateway.auth.trustedProxy.userHeader`).
    pub user_header: String,
    /// The identity the link sends in `user_header`; it is granted
    /// `operator.admin` in `gateway.auth.identityScopes`.
    pub identity: String,
    /// A local secret for `gateway.auth.password`.
    pub password: String,
}

impl ProxyAccess {
    /// How the link must forward requests to `runtime` once this change is
    /// applied.
    pub fn route(&self, runtime: Runtime) -> ProxyRoute {
        match runtime {
            Runtime::Openclaw => ProxyRoute {
                path_mode: PathMode::ReaddPrefix,
                origin: Some(self.origin.clone()),
                identity_header: Some(self.user_header.clone()),
            },
            Runtime::Hermes => ProxyRoute {
                path_mode: PathMode::StripWithForwardedPrefix,
                origin: None,
                identity_header: None,
            },
        }
    }
}

/// How the link forwards a proxied request to a runtime's UI.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyRoute {
    pub path_mode: PathMode,
    /// `Some`: set `Origin` to this value. `None`: remove `Origin`.
    pub origin: Option<String>,
    /// The header to carry [`ProxyAccess::identity`], when the runtime reads
    /// one.
    pub identity_header: Option<String>,
}

/// What the runtime expects of the request path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathMode {
    /// Forward the path with the base path in front (the runtime serves
    /// under it).
    ReaddPrefix,
    /// Forward the path without the base path and send the base path in
    /// `X-Forwarded-Prefix`.
    StripWithForwardedPrefix,
}

/// Parameters of [`Change::NeboaiModels`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NeboaiModels {
    /// The OpenAI-compatible base URL, ending in `/v1`.
    pub base_url: String,
    /// The key the runtime sends to the endpoint.
    pub api_key: String,
    /// The models the endpoint serves.
    pub models: Vec<Model>,
    /// The id (from `models`) to select as the runtime's default model.
    pub default_model: String,
}

/// A model served by the NeboAI endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Model {
    pub id: String,
    /// Display name.
    pub name: String,
}

/// What applying or reverting a change did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outcome {
    /// Whether the config file changed.
    pub changed: bool,
    /// The runtime's own restart command when the change takes effect only
    /// after it runs; `None` when the runtime picks the change up by itself.
    pub restart: Option<RuntimeCommand>,
    /// Settings a revert left alone because they were changed after the link
    /// applied them (dotted paths). The owner's later edits win.
    pub conflicts: Vec<String>,
}
