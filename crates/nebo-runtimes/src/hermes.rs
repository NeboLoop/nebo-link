//! Hermes: `~/.hermes` (the default profile), `~/.hermes/profiles/<name>`,
//! each with `config.yaml` and `.env`; and the client of its API server
//! ([`runs`]).

pub mod runs;
mod sse;

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};

use serde_json::json;
use yaml_serde::Value;

use crate::detect::version_at_least;
use crate::doc::yaml::Yaml;
use crate::doc::{Edit, Format, Tree};
use crate::environment::expand_tilde;
use crate::{
    ApiServer, Endpoint, Environment, Error, HealthCheck, Installation, ManagedProcess,
    NeboaiModels, Profile, Runtime, RuntimeCommand, Service, ServiceCommand,
};

/// `hermes_cli/web_server.py` `start_server(port=9119)`.
const DEFAULT_DASHBOARD_PORT: u16 = 9119;
/// `gateway/platforms/api_server.py` `DEFAULT_PORT`.
const DEFAULT_API_PORT: u16 = 8642;
/// First release whose dashboard honours `X-Forwarded-Prefix` (v2026.5.7).
const PROXY_MIN_VERSION: [u64; 3] = [0, 13, 0];
/// First release whose gateway service name is scoped to a custom root
/// (`hermes_cli/gateway.py` `_native_service_homes`, 2026-09-12): before
/// it, any root gets the bare name.
const SCOPED_SERVICE_MIN_VERSION: [u64; 3] = [0, 21, 5];
/// The named provider the link's endpoint is registered under.
const PROVIDER: &str = "neboai";
/// `hermes_constants.py` `_HERMES_HOME_MARKERS`.
const ROOT_MARKERS: [&str; 3] = ["config.yaml", ".env", "state.db"];
/// `hermes_constants.py` `_PROFILE_IDENTITY_MARKERS`.
const PROFILE_MARKERS: [&str; 6] = [
    "config.yaml",
    ".env",
    "SOUL.md",
    "profile.yaml",
    "auth.json",
    "state.db",
];

pub(crate) fn detect(env: &Environment) -> Option<Installation> {
    let (root, native) = root(env)?;
    let is_install = root.join("hermes-agent").is_dir()
        || root.join("profiles").is_dir()
        || ROOT_MARKERS.iter().any(|marker| root.join(marker).exists());
    if !is_install {
        return None;
    }
    let config_path = root.join("config.yaml");
    let (config, config_error) = read_config(&config_path);
    let profiles = named_profiles(&root);

    let dashboard = dashboard(&root);
    let mut endpoints = vec![dashboard.clone()];
    // Multiplexing (the default, `gateway.multiplex_profiles`) binds only the
    // default profile's API server; a named profile is mirrored on it at
    // `/p/<name>` (`gateway/config.py` `SHARED_LISTENER_MIRROR_PATHS`).
    let listener = api_server(&config, &read_env_file(&root.join(".env")));
    if listener.enabled {
        let addr = listener.addr;
        endpoints.push(Endpoint {
            service: Service::HermesApiServer {
                profile: "default".to_owned(),
            },
            addr,
            base_path: String::new(),
        });
        for profile in &profiles {
            let (config, _) = read_config(&profile.config_path);
            if api_server(&config, &read_env_file(&profile.home.join(".env"))).enabled {
                endpoints.push(Endpoint {
                    service: Service::HermesApiServer {
                        profile: profile.name.clone(),
                    },
                    addr,
                    base_path: format!("/p/{}", profile.name),
                });
            }
        }
    }

    let version = version(&root.join("hermes-agent"));
    let command_env: Vec<(String, String)> = env
        .var("HERMES_HOME")
        .map(|_| ("HERMES_HOME".to_owned(), root.display().to_string()))
        .into_iter()
        .collect();
    let hermes = |args: &[&str]| RuntimeCommand {
        program: "hermes".to_owned(),
        args: args.iter().map(|a| (*a).to_owned()).collect(),
        env: command_env.clone(),
    };
    let processes = vec![
        // The gateway hosts the API server (`gateway/platforms/api_server.py`
        // runs inside it); `/health` answers without the key.
        ManagedProcess {
            name: "gateway".to_owned(),
            health: HealthCheck {
                url: format!("http://{}/health", listener.addr),
                pid_file: Some(root.join("gateway.pid")),
            },
            service: gateway_service(env, &root, &native, version.as_deref()).map(|definition| ServiceCommand {
                definition,
                // Explicit answers to the questions `install` asks on a
                // terminal (`hermes_cli/gateway.py` `_install_systemd_from_cli`).
                install: hermes(&["gateway", "install", "--start-now", "--start-on-login"]),
                start: hermes(&["gateway", "start"]),
                uninstall: hermes(&["gateway", "uninstall"]),
            }),
            run: hermes(&["gateway", "run"]),
        },
        // The dashboard is its own process and has no service of its own
        // (`hermes_cli/subcommands/dashboard.py`: "No service manager / PID
        // file"); `hermes dashboard` runs it in the foreground.
        ManagedProcess {
            name: "dashboard".to_owned(),
            health: HealthCheck {
                url: format!("http://{}/", dashboard.addr),
                pid_file: None,
            },
            service: None,
            run: hermes(&[
                "dashboard",
                "--host",
                &dashboard.addr.ip().to_string(),
                "--port",
                &dashboard.addr.port().to_string(),
                "--no-open",
            ]),
        },
    ];

    Some(Installation {
        runtime: Runtime::Hermes,
        restart: hermes(&["gateway", "restart"]),
        processes,
        home: root,
        config_path,
        proxy_supported: version
            .as_deref()
            .and_then(|version| version_at_least(version, &PROXY_MIN_VERSION)),
        version,
        config_error,
        endpoints,
        profiles,
    })
}

/// The Hermes root and the platform's default one: `hermes_constants.py`
/// `get_default_hermes_root`. A `HERMES_HOME` pointing at
/// `<root>/profiles/<name>` still means `<root>`.
fn root(env: &Environment) -> Option<(PathBuf, PathBuf)> {
    let home = env.home.clone()?;
    let suffix = env
        .vars
        .get("HERMES_DATA_DIR_SUFFIX")
        .map_or("", String::as_str);
    let native = if cfg!(windows) {
        env.var("LOCALAPPDATA")
            .map_or_else(|| home.join("AppData").join("Local"), PathBuf::from)
            .join(format!("hermes{suffix}"))
    } else {
        home.join(format!(".hermes{suffix}"))
    };
    let Some(value) = env.var("HERMES_HOME") else {
        return Some((native.clone(), native));
    };
    let path = expand_tilde(value, &home);
    if path.starts_with(&native) {
        return Some((native.clone(), native));
    }
    let root = match path.parent() {
        Some(parent) if parent.file_name().is_some_and(|name| name == "profiles") => {
            parent.parent().map(Path::to_path_buf)?
        }
        _ => path,
    };
    Some((root, native))
}

/// Where `hermes gateway install` writes the gateway's service definition
/// for `root`: a launchd agent or a systemd user unit named
/// `hermes_cli/gateway.py` `get_service_name` / `gateway_launchd.py`
/// `launchd_label`: the bare name for the platform's default root, else the
/// name with `-<sha256(path)[:8]>` (`_profile_suffix`) from
/// [`SCOPED_SERVICE_MIN_VERSION`] on, and the bare name before it. Hermes
/// has no service manager on Windows the link can name a file for.
fn gateway_service(env: &Environment, root: &Path, native: &Path, version: Option<&str>) -> Option<PathBuf> {
    let home = env.home.as_deref()?;
    let scoped = version.and_then(|v| version_at_least(v, &SCOPED_SERVICE_MIN_VERSION)) != Some(false);
    let suffix = if root == native || !scoped {
        String::new()
    } else {
        use sha2::{Digest, Sha256};
        let resolved = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
        let digest = Sha256::digest(resolved.display().to_string().as_bytes());
        format!("-{}", &format!("{digest:x}")[..8])
    };
    if cfg!(target_os = "macos") {
        Some(home.join("Library/LaunchAgents").join(format!("ai.hermes.gateway{suffix}.plist")))
    } else if cfg!(unix) {
        let config = env
            .var("XDG_CONFIG_HOME")
            .map_or_else(|| home.join(".config"), |dir| expand_tilde(dir, home));
        Some(config.join("systemd/user").join(format!("hermes-gateway{suffix}.service")))
    } else {
        None
    }
}

/// `hermes_cli/profiles.py` `_iter_named_profile_dirs`: live profile
/// directories with a valid id and an identity marker.
fn named_profiles(root: &Path) -> Vec<Profile> {
    let profiles_dir = root.join("profiles");
    let Ok(entries) = std::fs::read_dir(&profiles_dir) else {
        return Vec::new();
    };
    let mut profiles: Vec<Profile> = entries
        .filter_map(Result::ok)
        .filter(|entry| entry.path().is_dir())
        .filter_map(|entry| {
            let name = entry.file_name().into_string().ok()?;
            let home = entry.path();
            let has_identity = PROFILE_MARKERS
                .iter()
                .any(|marker| home.join(marker).is_file() || home.join(marker).is_symlink());
            let deleted = profiles_dir.join(".deleted").join(&name).exists();
            (valid_profile_id(&name) && name != "default" && has_identity && !deleted).then(|| {
                Profile {
                    config_path: home.join("config.yaml"),
                    name,
                    home,
                }
            })
        })
        .collect();
    profiles.sort_by(|a, b| a.name.cmp(&b.name));
    profiles
}

/// `hermes_constants.py` `PROFILE_ID_RE`: `^[a-z0-9][a-z0-9_-]{0,63}$`.
fn valid_profile_id(name: &str) -> bool {
    let mut chars = name.chars();
    name.len() <= 64
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
}

fn read_config(path: &Path) -> (Value, Option<String>) {
    match std::fs::read_to_string(path) {
        Ok(text) => match Yaml::parse(&text) {
            Ok(value) => (value, None),
            Err(error) => (Value::empty_map(), Some(error)),
        },
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (Value::empty_map(), None),
        Err(error) => (Value::empty_map(), Some(error.to_string())),
    }
}

/// `KEY=value` lines of a profile's `.env` (which Hermes loads into the
/// profile's process environment).
fn read_env_file(path: &Path) -> BTreeMap<String, String> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return BTreeMap::new();
    };
    text.lines()
        .filter_map(|line| {
            let line = line.trim();
            let line = line.strip_prefix("export ").unwrap_or(line);
            if line.starts_with('#') {
                return None;
            }
            let (key, value) = line.split_once('=')?;
            let value = value.trim();
            let value = [('"', '"'), ('\'', '\'')]
                .iter()
                .find_map(|(open, close)| value.strip_prefix(*open)?.strip_suffix(*close))
                .unwrap_or(value);
            Some((key.trim().to_owned(), value.to_owned()))
        })
        .collect()
}

/// The web dashboard's address. The dashboard records its real bind in the
/// spawn ledger (`hermes_cli/process_identity.py`, `spawn-ledger.json` in the
/// root); without a record it is the default `127.0.0.1:9119`.
fn dashboard(root: &Path) -> Endpoint {
    let recorded = std::fs::read_to_string(root.join("spawn-ledger.json"))
        .ok()
        .and_then(|text| {
            serde_json::from_str::<Vec<serde_json::Value>>(text.trim_start_matches('\u{feff}')).ok()
        })
        .and_then(|entries| {
            entries.iter().rev().find_map(|entry| {
                let port = entry["port"]
                    .as_u64()
                    .and_then(|port| u16::try_from(port).ok())?;
                (entry["purpose"] == "dashboard" && port > 0).then(|| {
                    SocketAddr::new(dial_ip(entry["host"].as_str().unwrap_or_default()), port)
                })
            })
        });
    Endpoint {
        service: Service::HermesDashboard,
        addr: recorded.unwrap_or(SocketAddr::from((
            Ipv4Addr::LOCALHOST,
            DEFAULT_DASHBOARD_PORT,
        ))),
        base_path: String::new(),
    }
}

/// A profile's API server: whether it is enabled, and its address either way
/// (the gateway's health is read there once it has a key).
struct ApiServerSetting {
    enabled: bool,
    addr: SocketAddr,
}

/// The API server's setting for a profile.
///
/// `gateway/config.py` `PlatformConfig.from_dict` merges the keys of
/// `platforms.api_server` with its `extra` map (extra wins), and
/// `gateway/config_env.py` `_api_server` then lets the profile's environment
/// enable it (a usable `API_SERVER_KEY`, 16+ characters, unless the config
/// says `enabled: false`) and override host and port.
fn api_server(config: &Value, env: &BTreeMap<String, String>) -> ApiServerSetting {
    let platform = config.get("platforms").and_then(|p| p.get("api_server"));
    let setting = |key: &str| {
        platform.and_then(|p| {
            p.get("extra")
                .and_then(|extra| extra.get(key))
                .or_else(|| p.get(key))
        })
    };
    let env_var = |key: &str| env.get(key).map(|v| v.trim()).filter(|v| !v.is_empty());
    let enabled = platform
        .and_then(|p| p.get("enabled"))
        .and_then(Value::as_bool);
    let env_key = env_var("API_SERVER_KEY").is_some_and(|key| key.len() >= 16);
    let enabled = enabled == Some(true) || (env_key && enabled != Some(false));
    let port = env_var("API_SERVER_PORT")
        .and_then(|port| port.parse().ok())
        .or_else(|| {
            setting("port").and_then(|port| match port {
                Value::Number(n) => n.as_u64().and_then(|n| u16::try_from(n).ok()),
                Value::String(s) => s.trim().parse().ok(),
                _ => None,
            })
        })
        .unwrap_or(DEFAULT_API_PORT);
    let host = env_var("API_SERVER_HOST")
        .map(str::to_owned)
        .or_else(|| setting("host").and_then(Value::as_str).map(str::to_owned))
        .unwrap_or_default();
    ApiServerSetting {
        enabled,
        addr: SocketAddr::new(dial_ip(&host), port),
    }
}

/// The address to dial for a bind host: wildcard and name binds are reached
/// on loopback.
fn dial_ip(host: &str) -> IpAddr {
    match host.trim().trim_start_matches('[').trim_end_matches(']') {
        "::" | "::1" => IpAddr::V6(Ipv6Addr::LOCALHOST),
        host => match host.parse::<IpAddr>() {
            Ok(ip) if !ip.is_unspecified() => ip,
            _ => IpAddr::V4(Ipv4Addr::LOCALHOST),
        },
    }
}

/// The installed code's version: the install stamp's `baseVersion`
/// (`hermes_cli/__init__.py`), or the `__version__` literal older releases
/// carried in that file.
fn version(code: &Path) -> Option<String> {
    let stamp = std::fs::read_to_string(code.join("install-stamp.json"))
        .ok()
        .and_then(|text| {
            serde_json::from_str::<serde_json::Value>(text.trim_start_matches('\u{feff}')).ok()
        })
        .and_then(|stamp| stamp["baseVersion"].as_str().map(str::to_owned));
    stamp.or_else(|| {
        let init = std::fs::read_to_string(code.join("hermes_cli").join("__init__.py")).ok()?;
        init.lines().find_map(|line| {
            let value = line
                .trim()
                .strip_prefix("__version__")?
                .trim()
                .strip_prefix('=')?
                .trim();
            Some(value.trim_matches(['"', '\'']).to_owned())
        })
    })
}

/// The edits for [`crate::Change::NeboaiModels`]: a named `neboai` provider
/// (`providers:` in cli-config.yaml.example) and a `model` block selecting
/// it. The whole `model` block is replaced so no setting of the previous
/// provider (its `api_mode`, `context_length`, …) applies to the new one; a
/// revert puts the original block back.
pub(crate) fn neboai_models(models: &NeboaiModels) -> Vec<Edit> {
    vec![
        Edit::set(
            &["providers", PROVIDER],
            json!({
                "name": "NeboAI",
                "base_url": models.base_url,
                "api_key": models.api_key,
                "transport": "chat_completions",
                "default_model": models.default_model,
            }),
        ),
        Edit::set(
            &["model"],
            json!({
                "default": models.default_model,
                "provider": PROVIDER,
                "base_url": models.base_url,
                "api_key": models.api_key,
            }),
        ),
    ]
}

/// The edits for [`crate::Change::ApiServer`]: `API_SERVER_KEY` in the
/// profile's `.env` (`gateway/config_env.py` `_api_server` enables the API
/// server on a usable key; `api_server.py` `_check_auth` compares the bearer
/// token with it).
pub(crate) fn api_server_key(api: &ApiServer) -> Vec<Edit> {
    vec![Edit::set(&["API_SERVER_KEY"], json!(api.key))]
}

/// The config file of `profile` (`None` or `"default"`: the root's).
pub(crate) fn config_file(install: &Installation, profile: Option<&str>) -> Result<PathBuf, Error> {
    profile_home(install, profile).map(|home| home.join("config.yaml"))
}

/// The `.env` of `profile` (`None` or `"default"`: the root's).
pub(crate) fn env_file(install: &Installation, profile: Option<&str>) -> Result<PathBuf, Error> {
    profile_home(install, profile).map(|home| home.join(".env"))
}

fn profile_home(install: &Installation, profile: Option<&str>) -> Result<PathBuf, Error> {
    match profile {
        None | Some("default") => Ok(install.home.clone()),
        Some(name) => install
            .profiles
            .iter()
            .find(|profile| profile.name == name)
            .map(|profile| profile.home.clone())
            .ok_or_else(|| Error::UnknownProfile(name.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn profile_ids() {
        assert!(valid_profile_id("coder"));
        assert!(valid_profile_id("a1_b-2"));
        assert!(!valid_profile_id("Coder"));
        assert!(!valid_profile_id("-x"));
        assert!(!valid_profile_id(""));
    }

    #[test]
    fn dial_addresses() {
        assert_eq!(dial_ip(""), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(dial_ip("0.0.0.0"), IpAddr::V4(Ipv4Addr::LOCALHOST));
        assert_eq!(dial_ip("::"), IpAddr::V6(Ipv6Addr::LOCALHOST));
        assert_eq!(
            dial_ip("100.64.0.5"),
            "100.64.0.5".parse::<IpAddr>().unwrap()
        );
    }
}
