//! OpenClaw: `~/.openclaw/openclaw.json` (JSON5) and its gateway.

pub mod gateway;

use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::detect::version_at_least;
use crate::doc::json5::Json5;
use crate::doc::{Edit, Format};
use crate::environment::expand_tilde;
use crate::{
    Endpoint, Environment, Error, HealthCheck, Installation, ManagedProcess, NeboaiModels,
    ProxyAccess, Runtime, RuntimeCommand, Service, ServiceCommand,
};

/// `src/config/paths.ts` `DEFAULT_GATEWAY_PORT`.
const DEFAULT_GATEWAY_PORT: u16 = 18789;
/// First release with `gateway.auth.identityScopes` and
/// `gateway.auth.trustedProxy.deviceAutoApprove` (both needed for a
/// prompt-free proxied Control UI).
const PROXY_MIN_VERSION: [u64; 3] = [2026, 8, 1];
/// The provider id the link's endpoint is registered under.
const PROVIDER: &str = "neboai";
/// The variables that select an OpenClaw install; passed on to its restart
/// command so it restarts the same one.
const SELECTOR_VARS: [&str; 4] = [
    "OPENCLAW_HOME",
    "OPENCLAW_PROFILE",
    "OPENCLAW_STATE_DIR",
    "OPENCLAW_CONFIG_PATH",
];

pub(crate) fn detect(env: &Environment) -> Option<Installation> {
    // `src/infra/home-dir.ts`: OPENCLAW_HOME replaces the OS home.
    let os_home = env.home.clone()?;
    let home = env
        .var("OPENCLAW_HOME")
        .map_or(os_home.clone(), |value| expand_tilde(value, &os_home));
    // `src/config/state-dir.ts` and `src/cli/profile-utils.ts`: a named
    // profile lives in `~/.openclaw-<profile>`; `~/.clawdbot` is the legacy
    // default when `~/.openclaw` doesn't exist.
    let profile = env
        .var("OPENCLAW_PROFILE")
        .filter(|profile| !profile.eq_ignore_ascii_case("default"));
    let state_dir = match (env.var("OPENCLAW_STATE_DIR"), profile) {
        (Some(dir), _) => expand_tilde(dir, &home),
        (None, Some(profile)) => home.join(format!(".openclaw-{profile}")),
        (None, None) => {
            let current = home.join(".openclaw");
            let legacy = home.join(".clawdbot");
            if !current.exists() && legacy.exists() {
                legacy
            } else {
                current
            }
        }
    };
    let config_path = match env.var("OPENCLAW_CONFIG_PATH") {
        Some(path) => expand_tilde(path, &home),
        None => ["openclaw.json", "clawdbot.json"]
            .iter()
            .map(|name| state_dir.join(name))
            .find(|path| path.is_file())
            .unwrap_or_else(|| state_dir.join("openclaw.json")),
    };
    if !state_dir.is_dir() && !config_path.is_file() {
        return None;
    }

    let (config, config_error) = read_config(&config_path);
    let config = config.unwrap_or_else(|| json!({}));
    let gateway = &config["gateway"];
    let version = config["meta"]["lastTouchedVersion"]
        .as_str()
        .map(str::to_owned);
    let command_env: Vec<(String, String)> = SELECTOR_VARS
        .iter()
        .filter_map(|name| Some((name.to_string(), env.var(name)?.to_owned())))
        .collect();
    let openclaw = |args: &[&str]| RuntimeCommand {
        program: "openclaw".to_owned(),
        args: args.iter().map(|a| (*a).to_owned()).collect(),
        env: command_env.clone(),
    };
    // `gateway.mode: "remote"` means this machine only connects to a gateway
    // elsewhere; there is nothing local to proxy or to keep running.
    let (endpoints, processes) = if gateway["mode"] == "remote" {
        (Vec::new(), Vec::new())
    } else {
        // Every bind mode also listens on 127.0.0.1 (`gateway.bind` docs
        // in docs/gateway/config-gateway.md).
        let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, gateway_port(&config, env, profile)));
        let endpoint = Endpoint {
            service: Service::OpenclawGateway {
                bind: gateway["bind"].as_str().unwrap_or("loopback").to_owned(),
                auth_mode: auth_mode(gateway, env),
            },
            addr,
            base_path: gateway["controlUi"]["basePath"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        };
        // One process serves the WebSocket API, the HTTP probes and the
        // Control UI; `/healthz` is answered before auth
        // (`src/gateway/server-http.ts`). `openclaw gateway` with no
        // subcommand runs it in the foreground (`src/cli/gateway-cli/register.ts`).
        let process = ManagedProcess {
            name: "gateway".to_owned(),
            health: HealthCheck {
                url: format!("http://{addr}/healthz"),
                pid_file: None,
            },
            service: gateway_service(env, &home, profile).map(|definition| ServiceCommand {
                definition,
                install: openclaw(&["gateway", "install"]),
                start: openclaw(&["gateway", "start"]),
                uninstall: openclaw(&["gateway", "uninstall"]),
            }),
            run: openclaw(&["gateway"]),
        };
        (vec![endpoint], vec![process])
    };
    Some(Installation {
        runtime: Runtime::Openclaw,
        home: state_dir,
        config_path,
        proxy_supported: version
            .as_deref()
            .and_then(|version| version_at_least(version, &PROXY_MIN_VERSION)),
        version,
        config_error,
        endpoints,
        profiles: Vec::new(),
        restart: openclaw(&["gateway", "restart"]),
        processes,
    })
}

/// Where `openclaw gateway install` writes the gateway's service definition:
/// a launchd agent `ai.openclaw.gateway` (`ai.openclaw.<profile>` for a named
/// profile) or a systemd user unit `openclaw-gateway[-<profile>].service`
/// (`src/daemon/constants.ts`), under the home `OPENCLAW_HOME` selects
/// (`src/infra/home-dir.ts`). OpenClaw registers a scheduled task on Windows,
/// which has no definition file to look for.
fn gateway_service(env: &Environment, home: &Path, profile: Option<&str>) -> Option<PathBuf> {
    if cfg!(target_os = "macos") {
        let label = profile.map_or("ai.openclaw.gateway".to_owned(), |p| format!("ai.openclaw.{p}"));
        Some(home.join("Library/LaunchAgents").join(format!("{label}.plist")))
    } else if cfg!(unix) {
        let unit = profile.map_or("openclaw-gateway".to_owned(), |p| format!("openclaw-gateway-{p}"));
        let config = env
            .var("XDG_CONFIG_HOME")
            .map_or_else(|| home.join(".config"), |dir| expand_tilde(dir, home));
        Some(config.join("systemd/user").join(format!("{unit}.service")))
    } else {
        None
    }
}

fn read_config(path: &Path) -> (Option<Value>, Option<String>) {
    match std::fs::read_to_string(path) {
        Ok(text) => match Json5::parse(&text) {
            Ok(value) => (Some(value), None),
            Err(error) => (None, Some(error)),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (None, None),
        Err(error) => (None, Some(error.to_string())),
    }
}

/// `src/gateway/auth-resolve.ts`: an unset mode means password when a
/// password is configured, token otherwise.
fn auth_mode(gateway: &Value, env: &Environment) -> String {
    let auth = &gateway["auth"];
    match auth["mode"].as_str() {
        Some(mode) if !mode.trim().is_empty() => mode.to_owned(),
        _ if !auth["password"].is_null() || env.var("OPENCLAW_GATEWAY_PASSWORD").is_some() => {
            "password".to_owned()
        }
        _ => "token".to_owned(),
    }
}

/// `src/config/paths.ts` `resolveGatewayPort`: `OPENCLAW_GATEWAY_PORT`, then
/// `gateway.port`, then a per-profile port, then 18789.
fn gateway_port(config: &Value, env: &Environment, profile: Option<&str>) -> u16 {
    if let Some(port) = env.var("OPENCLAW_GATEWAY_PORT").and_then(parse_port_env) {
        return port;
    }
    if let Some(port) = config["gateway"]["port"]
        .as_u64()
        .and_then(|port| u16::try_from(port).ok())
        .filter(|port| *port > 0)
    {
        return port;
    }
    match profile {
        // FNV-1a over the profile name, kept in step with the macOS app's
        // `AppProfile.defaultGatewayPort`.
        Some(profile) => {
            let hash = profile.bytes().fold(2_166_136_261_u32, |hash, byte| {
                (hash ^ u32::from(byte)).wrapping_mul(16_777_619)
            });
            // 20000 + (< 40000) always fits in a u16.
            (20_000 + hash % 40_000) as u16
        }
        None => DEFAULT_GATEWAY_PORT,
    }
}

/// `parseGatewayPortEnvValue`: a bare port, or `host:port` / `[v6]:port`
/// leaked from a Docker publish string.
fn parse_port_env(value: &str) -> Option<u16> {
    let port = if value.bytes().all(|b| b.is_ascii_digit()) {
        value
    } else if let Some(rest) = value.strip_prefix('[') {
        rest.split_once("]:")?.1
    } else {
        let (host, port) = value.split_once(':')?;
        if host.is_empty() || port.contains(':') {
            return None;
        }
        port
    };
    port.parse().ok().filter(|port| *port > 0)
}

/// The edits for [`crate::Change::ProxyAccess`]; lists are extended, never
/// replaced.
pub(crate) fn proxy_access(config: &Value, access: &ProxyAccess) -> Vec<Edit> {
    let gateway = &config["gateway"];
    vec![
        Edit::set(
            &["gateway", "trustedProxies"],
            union(&gateway["trustedProxies"], &["127.0.0.1", "::1"]),
        ),
        Edit::set(
            &["gateway", "controlUi", "basePath"],
            json!(access.base_path),
        ),
        Edit::set(
            &["gateway", "controlUi", "allowedOrigins"],
            union(
                &gateway["controlUi"]["allowedOrigins"],
                &[access.origin.as_str()],
            ),
        ),
        // Trusted-proxy and a shared token are mutually exclusive: the gateway
        // refuses to start with both ("remove gateway.auth.token"). The token
        // is journaled like every other edit, so a revert puts it back.
        Edit::remove(&["gateway", "auth", "token"]),
        Edit::set(&["gateway", "auth", "mode"], json!("trusted-proxy")),
        Edit::set(&["gateway", "auth", "password"], json!(access.password)),
        // Admin through a session-only identity grant, never through
        // `deviceAutoApprove.scopes` (that is a CRITICAL audit finding,
        // docs/gateway/trusted-proxy-auth.md "Automatic device approval").
        Edit::set(
            &["gateway", "auth", "identityScopes", &access.identity],
            json!(["operator.admin"]),
        ),
        Edit::set(
            &["gateway", "auth", "trustedProxy", "userHeader"],
            json!(access.user_header),
        ),
        Edit::set(
            &["gateway", "auth", "trustedProxy", "allowLoopback"],
            json!(true),
        ),
        Edit::set(
            &[
                "gateway",
                "auth",
                "trustedProxy",
                "deviceAutoApprove",
                "enabled",
            ],
            json!(true),
        ),
    ]
}

/// The edits for [`crate::Change::NeboaiModels`]: a `neboai` provider
/// (docs/providers/sglang.md "Explicit configuration") selected as the default
/// agent model. An object-form `agents.defaults.model` keeps its fallbacks.
pub(crate) fn neboai_models(config: &Value, models: &NeboaiModels) -> Vec<Edit> {
    let primary = json!(format!("{PROVIDER}/{}", models.default_model));
    let model = if config["agents"]["defaults"]["model"].is_object() {
        Edit::set(&["agents", "defaults", "model", "primary"], primary)
    } else {
        Edit::set(&["agents", "defaults", "model"], primary)
    };
    vec![
        Edit::set(
            &["models", "providers", PROVIDER],
            json!({
                "baseUrl": models.base_url,
                "apiKey": models.api_key,
                "api": "openai-completions",
                "models": models
                    .models
                    .iter()
                    .map(|model| json!({ "id": model.id, "name": model.name }))
                    .collect::<Vec<_>>(),
            }),
        ),
        model,
    ]
}

fn union(existing: &Value, add: &[&str]) -> Value {
    let mut list: Vec<Value> = existing.as_array().cloned().unwrap_or_default();
    for item in add {
        if !list.iter().any(|value| value == item) {
            list.push(json!(item));
        }
    }
    Value::Array(list)
}

/// Refuses edits that pass through an object assembled with `$include`: its
/// keys may live in the included file, and OpenClaw's own writer respects
/// that boundary too.
pub(crate) fn check_includes(config: &Value, edits: &[Edit], path: &Path) -> Result<(), Error> {
    for edit in edits {
        let mut node = config;
        let mut at = Vec::new();
        for key in std::iter::once(None).chain(edit.path.iter().map(Some)) {
            if let Some(key) = key {
                node = &node[key.as_str()];
                at.push(key.as_str());
            }
            if node.get("$include").is_some() {
                return Err(Error::Include {
                    path: path.to_path_buf(),
                    key: if at.is_empty() {
                        "(top level)".to_owned()
                    } else {
                        at.join(".")
                    },
                });
            }
        }
    }
    Ok(())
}

/// Whether OpenClaw needs `openclaw gateway restart` to pick up the change
/// from `before` to `after`. With live reload (`gateway.reload.mode`
/// `hybrid`, the default) the gateway applies or self-restarts on its own,
/// except for a new Control UI base path.
pub(crate) fn needs_restart(before: &Value, after: &Value) -> bool {
    let reload_off = |config: &Value| config["gateway"]["reload"]["mode"] == "off";
    let base_path = |config: &Value| config["gateway"]["controlUi"]["basePath"].clone();
    before != after
        && (reload_off(before) || reload_off(after) || base_path(before) != base_path(after))
}

/// The config file a change targets; OpenClaw has no named profiles here.
pub(crate) fn config_file(install: &Installation, profile: Option<&str>) -> Result<PathBuf, Error> {
    match profile {
        None => Ok(install.config_path.clone()),
        Some(name) => Err(Error::UnknownProfile(name.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn port_env_forms() {
        assert_eq!(parse_port_env("19001"), Some(19001));
        assert_eq!(parse_port_env("127.0.0.1:18790"), Some(18790));
        assert_eq!(parse_port_env("[::1]:18791"), Some(18791));
        assert_eq!(parse_port_env("a:b:1"), None);
        assert_eq!(parse_port_env("0"), None);
    }

    #[test]
    fn profile_port_is_stable() {
        let env = Environment::default();
        let port = gateway_port(&json!({}), &env, Some("work"));
        assert!((20_000..60_000).contains(&port));
        assert_eq!(port, gateway_port(&json!({}), &env, Some("work")));
        assert_eq!(gateway_port(&json!({}), &env, None), DEFAULT_GATEWAY_PORT);
    }
}
