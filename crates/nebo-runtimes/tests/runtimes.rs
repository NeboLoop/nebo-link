//! Detection and apply/revert against temporary homes seeded from each
//! runtime's documented config examples. Nothing here reads the real home.

use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use nebo_runtimes::{
    ApiServer, Change, ChangeKind, Environment, Error, Installation, Journal, Model, NeboaiModels, PathMode,
    ProxyAccess, Runtime, Service, detect,
};
use serde_json::Value;

const OPENCLAW_FIXTURE: &str = include_str!("fixtures/openclaw.json");
const HERMES_FIXTURE: &str = include_str!("fixtures/hermes-config.yaml");

struct Home {
    dir: tempfile::TempDir,
    vars: BTreeMap<String, String>,
}

impl Home {
    fn new() -> Self {
        Self {
            dir: tempfile::tempdir().unwrap(),
            vars: BTreeMap::new(),
        }
    }

    fn path(&self, relative: &str) -> PathBuf {
        self.dir.path().join(relative)
    }

    fn write(&self, relative: &str, text: &str) -> PathBuf {
        let path = self.path(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, text).unwrap();
        path
    }

    fn var(mut self, name: &str, value: &str) -> Self {
        self.vars.insert(name.to_owned(), value.to_owned());
        self
    }

    fn env(&self) -> Environment {
        Environment {
            home: Some(self.dir.path().to_path_buf()),
            vars: self.vars.clone(),
        }
    }

    fn install(&self, runtime: Runtime) -> Installation {
        detect(&self.env())
            .into_iter()
            .find(|install| install.runtime == runtime)
            .expect("installation detected")
    }

    fn journal(&self) -> Journal {
        Journal::open(self.path("link/journal.json")).unwrap()
    }
}

fn proxy_access() -> ProxyAccess {
    ProxyAccess {
        base_path: "/t/bot-1".into(),
        origin: "https://neboai.com".into(),
        user_header: "x-nebo-user".into(),
        identity: "owner-1".into(),
        password: "local-secret".into(),
    }
}

fn models(default: &str) -> NeboaiModels {
    NeboaiModels {
        base_url: "http://127.0.0.1:18800/v1".into(),
        api_key: "link-key".into(),
        models: vec![
            Model {
                id: "neboai-fast".into(),
                name: "NeboAI Fast".into(),
            },
            Model {
                id: "neboai-smart".into(),
                name: "NeboAI Smart".into(),
            },
        ],
        default_model: default.into(),
    }
}

fn json5(path: &Path) -> Value {
    json5::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn yaml(path: &Path) -> yaml_serde::Value {
    yaml_serde::from_str(&fs::read_to_string(path).unwrap()).unwrap()
}

fn addr(text: &str) -> SocketAddr {
    text.parse().unwrap()
}

// ---------------------------------------------------------------- detection

#[test]
fn nothing_installed() {
    assert!(detect(&Home::new().env()).is_empty());
}

#[test]
fn openclaw_from_documented_config() {
    let home = Home::new();
    home.write(".openclaw/openclaw.json", OPENCLAW_FIXTURE);
    let install = home.install(Runtime::Openclaw);
    assert_eq!(install.home, home.path(".openclaw"));
    assert_eq!(install.config_path, home.path(".openclaw/openclaw.json"));
    assert_eq!(install.version.as_deref(), Some("2026.9.6"));
    assert_eq!(install.proxy_supported, Some(true));
    assert_eq!(install.config_error, None);
    assert_eq!(install.endpoints.len(), 1);
    let gateway = &install.endpoints[0];
    assert_eq!(gateway.addr, addr("127.0.0.1:18789"));
    assert_eq!(gateway.base_path, "");
    assert_eq!(
        gateway.service,
        Service::OpenclawGateway {
            bind: "loopback".into(),
            auth_mode: "token".into()
        }
    );
    assert_eq!(install.restart.program, "openclaw");
    assert_eq!(install.restart.args, ["gateway", "restart"]);
    assert!(install.restart.env.is_empty());
}

#[test]
fn openclaw_without_a_config_file_uses_defaults() {
    let home = Home::new();
    fs::create_dir_all(home.path(".openclaw")).unwrap();
    let install = home.install(Runtime::Openclaw);
    assert_eq!(install.endpoints[0].addr, addr("127.0.0.1:18789"));
    assert_eq!(install.version, None);
    assert_eq!(install.proxy_supported, None);
}

#[test]
fn openclaw_honours_its_overrides() {
    let home = Home::new()
        .var("OPENCLAW_STATE_DIR", "~/state")
        .var("OPENCLAW_CONFIG_PATH", "~/elsewhere/oc.json5")
        .var("OPENCLAW_GATEWAY_PORT", "127.0.0.1:19999");
    fs::create_dir_all(home.path("state")).unwrap();
    home.write(
        "elsewhere/oc.json5",
        "{ gateway: { port: 1234, auth: { password: 'p' }, controlUi: { basePath: '/oc' } } }",
    );
    let install = home.install(Runtime::Openclaw);
    assert_eq!(install.home, home.path("state"));
    assert_eq!(install.config_path, home.path("elsewhere/oc.json5"));
    assert_eq!(install.endpoints[0].addr, addr("127.0.0.1:19999"));
    assert_eq!(install.endpoints[0].base_path, "/oc");
    assert!(
        matches!(&install.endpoints[0].service, Service::OpenclawGateway { auth_mode, .. } if auth_mode == "password")
    );
    let env: Vec<&str> = install
        .restart
        .env
        .iter()
        .map(|(name, _)| name.as_str())
        .collect();
    assert_eq!(env, ["OPENCLAW_STATE_DIR", "OPENCLAW_CONFIG_PATH"]);
}

#[test]
fn openclaw_named_profile_and_remote_mode() {
    let home = Home::new().var("OPENCLAW_PROFILE", "work");
    home.write(
        ".openclaw-work/openclaw.json",
        "{ meta: { lastTouchedVersion: '2026.7.2' } }",
    );
    let install = home.install(Runtime::Openclaw);
    assert_eq!(install.home, home.path(".openclaw-work"));
    assert_ne!(install.endpoints[0].addr.port(), 18789);
    assert_eq!(install.proxy_supported, Some(false));

    let remote = Home::new();
    remote.write(".openclaw/openclaw.json", "{ gateway: { mode: 'remote' } }");
    assert!(remote.install(Runtime::Openclaw).endpoints.is_empty());
}

#[test]
fn openclaw_broken_config_is_reported() {
    let home = Home::new();
    home.write(".openclaw/openclaw.json", "{ gateway: ");
    let install = home.install(Runtime::Openclaw);
    assert!(install.config_error.is_some());
    assert_eq!(install.endpoints[0].addr, addr("127.0.0.1:18789"));
}

fn hermes_home() -> Home {
    let home = Home::new();
    home.write(".hermes/config.yaml", HERMES_FIXTURE);
    home.write(
        ".hermes/.env",
        "# keys\nAPI_SERVER_KEY=\"0123456789abcdef0123\"\n",
    );
    home.write(
        ".hermes/hermes-agent/install-stamp.json",
        r#"{"baseVersion": "0.19.0"}"#,
    );
    home.write(
        ".hermes/spawn-ledger.json",
        r#"[{"pid": 1, "purpose": "dashboard", "host": "127.0.0.1", "port": 9200},
            {"pid": 2, "purpose": "serve", "host": "127.0.0.1", "port": 0}]"#,
    );
    // A live profile with its API server enabled through its .env.
    home.write(".hermes/profiles/coder/config.yaml", "model: gpt-5\n");
    home.write(
        ".hermes/profiles/coder/.env",
        "export API_SERVER_KEY=abcdefabcdefabcdef\n",
    );
    // A profile with the API server off.
    home.write(".hermes/profiles/writer/SOUL.md", "writer\n");
    // Not profiles: no identity marker, a tombstone, an invalid id.
    fs::create_dir_all(home.path(".hermes/profiles/ghost/logs")).unwrap();
    home.write(".hermes/profiles/gone/config.yaml", "");
    fs::create_dir_all(home.path(".hermes/profiles/.deleted/gone")).unwrap();
    home.write(".hermes/profiles/Bad Name/config.yaml", "");
    home
}

#[test]
fn hermes_with_profiles() {
    let home = hermes_home();
    let install = home.install(Runtime::Hermes);
    assert_eq!(install.home, home.path(".hermes"));
    assert_eq!(install.config_path, home.path(".hermes/config.yaml"));
    assert_eq!(install.version.as_deref(), Some("0.19.0"));
    assert_eq!(install.proxy_supported, Some(true));
    let names: Vec<&str> = install.profiles.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, ["coder", "writer"]);
    assert_eq!(
        install.profiles[0].config_path,
        home.path(".hermes/profiles/coder/config.yaml")
    );

    let endpoints: Vec<(Service, SocketAddr, &str)> = install
        .endpoints
        .iter()
        .map(|e| (e.service.clone(), e.addr, e.base_path.as_str()))
        .collect();
    assert_eq!(
        endpoints,
        [
            (Service::HermesDashboard, addr("127.0.0.1:9200"), ""),
            (
                Service::HermesApiServer {
                    profile: "default".into()
                },
                addr("127.0.0.1:8650"),
                ""
            ),
            (
                Service::HermesApiServer {
                    profile: "coder".into()
                },
                addr("127.0.0.1:8650"),
                "/p/coder"
            ),
        ]
    );
    assert_eq!(install.restart.program, "hermes");
    assert_eq!(install.restart.args, ["gateway", "restart"]);
}

#[test]
fn hermes_defaults_and_old_versions() {
    let home = Home::new();
    home.write(".hermes/config.yaml", "model: gpt-5\n");
    home.write(
        ".hermes/hermes-agent/hermes_cli/__init__.py",
        "\"\"\"Hermes CLI\"\"\"\n__version__ = \"0.10.0\"\n",
    );
    let install = home.install(Runtime::Hermes);
    assert_eq!(install.version.as_deref(), Some("0.10.0"));
    assert_eq!(install.proxy_supported, Some(false));
    assert_eq!(
        install.endpoints.len(),
        1,
        "API server is off without a key"
    );
    assert_eq!(install.endpoints[0].addr, addr("127.0.0.1:9119"));
    assert!(install.profiles.is_empty());
}

#[test]
fn hermes_home_pointing_at_a_profile_means_its_root() {
    let home = Home::new().var("HERMES_HOME", "~/custom/profiles/coder");
    home.write("custom/config.yaml", "");
    home.write("custom/profiles/coder/config.yaml", "");
    let install = home.install(Runtime::Hermes);
    assert_eq!(install.home, home.path("custom"));
    assert_eq!(
        install.restart.env,
        [(
            "HERMES_HOME".to_owned(),
            home.path("custom").display().to_string()
        )]
    );
}

// ------------------------------------------------------------ processes

/// Where the runtime's own service definition lands for `label` (launchd)
/// or `unit` (systemd) under `home`; `None` on Windows, where neither
/// runtime has a definition file to look for.
fn service_definition(home: &Path, label: &str, unit: &str) -> Option<PathBuf> {
    if cfg!(target_os = "macos") {
        Some(home.join("Library/LaunchAgents").join(format!("{label}.plist")))
    } else if cfg!(unix) {
        Some(home.join(".config/systemd/user").join(format!("{unit}.service")))
    } else {
        None
    }
}

#[test]
fn hermes_processes_are_the_gateway_and_the_dashboard() {
    let home = hermes_home();
    let install = home.install(Runtime::Hermes);
    let names: Vec<&str> = install.processes.iter().map(|p| p.name.as_str()).collect();
    assert_eq!(names, ["gateway", "dashboard"], "the gateway first: it is what `restart` restarts");

    let gateway = &install.processes[0];
    assert_eq!(gateway.health.url, "http://127.0.0.1:8650/health");
    assert_eq!(gateway.health.pid_file, Some(home.path(".hermes/gateway.pid")));
    assert_eq!(gateway.run.program, "hermes");
    assert_eq!(gateway.run.args, ["gateway", "run"]);
    assert!(gateway.run.env.is_empty(), "the default root needs no HERMES_HOME");
    match (&gateway.service, service_definition(home.dir.path(), "ai.hermes.gateway", "hermes-gateway")) {
        (Some(service), Some(definition)) => {
            assert_eq!(service.definition, definition);
            assert_eq!(service.install.args, ["gateway", "install", "--start-now", "--start-on-login"]);
            assert_eq!(service.start.args, ["gateway", "start"]);
            assert_eq!(service.uninstall.args, ["gateway", "uninstall"]);
        }
        (None, None) => {}
        (service, definition) => panic!("service {service:?} for definition {definition:?}"),
    }

    let dashboard = &install.processes[1];
    assert_eq!(dashboard.health.url, "http://127.0.0.1:9200/");
    assert_eq!(dashboard.health.pid_file, None);
    assert_eq!(dashboard.service, None, "Hermes has no service for its dashboard");
    assert_eq!(
        dashboard.run.args,
        ["dashboard", "--host", "127.0.0.1", "--port", "9200", "--no-open"]
    );
}

#[test]
fn hermes_gateway_health_is_known_before_its_key_is_written() {
    let home = Home::new();
    home.write(".hermes/config.yaml", "model: gpt-5\n");
    let install = home.install(Runtime::Hermes);
    assert_eq!(install.endpoints.len(), 1, "no API server endpoint without a key");
    assert_eq!(install.processes[0].health.url, "http://127.0.0.1:8642/health");
    assert_eq!(install.processes[1].health.url, "http://127.0.0.1:9119/");
}

#[test]
fn a_custom_hermes_home_gets_its_own_service_name_and_env() {
    let home = Home::new().var("HERMES_HOME", "~/custom");
    home.write("custom/config.yaml", "");
    home.write("custom/hermes-agent/install-stamp.json", r#"{"baseVersion": "0.21.5"}"#);
    let install = home.install(Runtime::Hermes);
    let root = home.path("custom");
    let env = [("HERMES_HOME".to_owned(), root.display().to_string())];
    for command in install
        .processes
        .iter()
        .flat_map(|p| {
            std::iter::once(&p.run).chain(
                p.service
                    .iter()
                    .flat_map(|s| [&s.install, &s.start, &s.uninstall]),
            )
        })
        .chain(std::iter::once(&install.restart))
    {
        assert_eq!(command.env, env, "{}", command.args.join(" "));
    }
    // `hermes_cli/gateway.py` `_profile_suffix`: sha256 of the resolved path.
    use sha2::{Digest, Sha256};
    let resolved = root.canonicalize().unwrap();
    let hash = format!("{:x}", Sha256::digest(resolved.display().to_string().as_bytes()));
    let suffix = format!("-{}", &hash[..8]);
    let expected = service_definition(
        home.dir.path(),
        &format!("ai.hermes.gateway{suffix}"),
        &format!("hermes-gateway{suffix}"),
    );
    assert_eq!(
        install.processes[0].service.as_ref().map(|s| s.definition.clone()),
        expected
    );

    // Before 0.21.5 Hermes named any root's service the bare name.
    home.write("custom/hermes-agent/install-stamp.json", r#"{"baseVersion": "0.19.0"}"#);
    assert_eq!(
        home.install(Runtime::Hermes).processes[0].service.as_ref().map(|s| s.definition.clone()),
        service_definition(home.dir.path(), "ai.hermes.gateway", "hermes-gateway")
    );
}

#[test]
fn openclaw_process_is_its_gateway() {
    let home = Home::new();
    home.write(".openclaw/openclaw.json", OPENCLAW_FIXTURE);
    let install = home.install(Runtime::Openclaw);
    assert_eq!(install.processes.len(), 1);
    let gateway = &install.processes[0];
    assert_eq!(gateway.name, "gateway");
    assert_eq!(gateway.health.url, "http://127.0.0.1:18789/healthz");
    assert_eq!(gateway.health.pid_file, None);
    assert_eq!(gateway.run.program, "openclaw");
    assert_eq!(gateway.run.args, ["gateway"], "no subcommand runs it in the foreground");
    assert_eq!(
        gateway.service.as_ref().map(|s| s.definition.clone()),
        service_definition(home.dir.path(), "ai.openclaw.gateway", "openclaw-gateway")
    );
    if let Some(service) = &gateway.service {
        assert_eq!(service.install.args, ["gateway", "install"]);
        assert_eq!(service.start.args, ["gateway", "start"]);
        assert_eq!(service.uninstall.args, ["gateway", "uninstall"]);
    }

    let profile = Home::new().var("OPENCLAW_PROFILE", "work");
    profile.write(".openclaw-work/openclaw.json", "{}");
    let install = profile.install(Runtime::Openclaw);
    assert_eq!(
        install.processes[0].service.as_ref().map(|s| s.definition.clone()),
        service_definition(profile.dir.path(), "ai.openclaw.work", "openclaw-gateway-work")
    );
    assert_eq!(
        install.processes[0].run.env,
        [("OPENCLAW_PROFILE".to_owned(), "work".to_owned())]
    );

    let remote = Home::new();
    remote.write(".openclaw/openclaw.json", "{ gateway: { mode: 'remote' } }");
    assert!(remote.install(Runtime::Openclaw).processes.is_empty(), "nothing local to keep running");
}

// ------------------------------------------------------------ apply/revert

#[test]
fn openclaw_proxy_access_round_trip() {
    let home = Home::new();
    let path = home.write(".openclaw/openclaw.json", OPENCLAW_FIXTURE);
    let install = home.install(Runtime::Openclaw);
    let mut journal = home.journal();

    let outcome = journal
        .apply(&install, None, &Change::ProxyAccess(proxy_access()))
        .unwrap();
    assert!(outcome.changed);
    // A new Control UI base path needs the gateway's own restart.
    assert_eq!(outcome.restart.as_ref(), Some(&install.restart));

    let config = json5(&path);
    let gateway = &config["gateway"];
    assert_eq!(
        gateway["trustedProxies"],
        serde_json::json!(["127.0.0.1", "::1"])
    );
    assert_eq!(gateway["controlUi"]["basePath"], "/t/bot-1");
    assert_eq!(
        gateway["controlUi"]["allowedOrigins"],
        serde_json::json!(["https://neboai.com"])
    );
    assert_eq!(gateway["controlUi"]["enabled"], true);
    assert_eq!(gateway["auth"]["mode"], "trusted-proxy");
    assert_eq!(gateway["auth"]["password"], "local-secret");
    assert!(
        gateway["auth"].get("token").is_none(),
        "the gateway refuses to start with trusted-proxy and a token; the revert restores it"
    );
    assert_eq!(
        gateway["auth"]["identityScopes"]["owner-1"],
        serde_json::json!(["operator.admin"])
    );
    assert_eq!(gateway["auth"]["trustedProxy"]["userHeader"], "x-nebo-user");
    assert_eq!(gateway["auth"]["trustedProxy"]["allowLoopback"], true);
    assert_eq!(
        gateway["auth"]["trustedProxy"]["deviceAutoApprove"]["enabled"],
        true
    );
    assert!(
        gateway["auth"]["trustedProxy"]["deviceAutoApprove"]
            .get("scopes")
            .is_none()
    );
    let text = fs::read_to_string(&path).unwrap();
    assert!(
        text.contains("// none | token | password | trusted-proxy"),
        "untouched comments survive"
    );
    assert!(text.contains("// Derived from OpenClaw"));

    let route = proxy_access().route(Runtime::Openclaw);
    assert_eq!(route.path_mode, PathMode::ReaddPrefix);
    assert_eq!(route.origin.as_deref(), Some("https://neboai.com"));
    assert_eq!(route.identity_header.as_deref(), Some("x-nebo-user"));

    let outcome = journal
        .revert(&install, None, ChangeKind::ProxyAccess)
        .unwrap();
    assert!(outcome.changed);
    assert!(outcome.conflicts.is_empty());
    assert_eq!(outcome.restart.as_ref(), Some(&install.restart));
    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        OPENCLAW_FIXTURE,
        "byte for byte"
    );
    assert!(journal.applied().is_empty());
}

#[test]
fn reapply_is_idempotent_and_new_parameters_keep_the_owners_originals() {
    let home = Home::new();
    let path = home.write(".openclaw/openclaw.json", OPENCLAW_FIXTURE);
    let install = home.install(Runtime::Openclaw);
    let mut journal = home.journal();
    let change = Change::ProxyAccess(proxy_access());

    assert!(journal.apply(&install, None, &change).unwrap().changed);
    let once = fs::read_to_string(&path).unwrap();
    let again = journal.apply(&install, None, &change).unwrap();
    assert!(!again.changed);
    assert_eq!(again.restart, None);
    assert_eq!(fs::read_to_string(&path).unwrap(), once);
    assert_eq!(journal.applied().len(), 1);

    let moved = Change::ProxyAccess(ProxyAccess {
        base_path: "/t/bot-2".into(),
        ..proxy_access()
    });
    assert!(journal.apply(&install, None, &moved).unwrap().changed);
    assert_eq!(json5(&path)["gateway"]["controlUi"]["basePath"], "/t/bot-2");
    assert_eq!(journal.applied().len(), 1);

    journal
        .revert(&install, None, ChangeKind::ProxyAccess)
        .unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), OPENCLAW_FIXTURE);
}

#[test]
fn revert_after_the_runtime_rewrote_its_file_is_semantic() {
    let home = Home::new();
    let path = home.write(".openclaw/openclaw.json", OPENCLAW_FIXTURE);
    let install = home.install(Runtime::Openclaw);
    let mut journal = home.journal();
    journal
        .apply(&install, None, &Change::ProxyAccess(proxy_access()))
        .unwrap();

    // OpenClaw rewrites its file as plain JSON (comments gone), and the owner
    // changes one setting the link had set plus one it never touched.
    let mut config = json5(&path);
    config["gateway"]["auth"]["trustedProxy"]["userHeader"] = "x-owner-choice".into();
    config["gateway"]["port"] = 18800.into();
    fs::write(
        &path,
        format!("{}\n", serde_json::to_string_pretty(&config).unwrap()),
    )
    .unwrap();

    let outcome = journal
        .revert(&install, None, ChangeKind::ProxyAccess)
        .unwrap();
    assert!(outcome.changed);
    // The link created `trustedProxy`, so that whole object is what it
    // leaves alone once the owner has edited inside it.
    assert_eq!(outcome.conflicts, ["gateway.auth.trustedProxy"]);
    let config = json5(&path);
    let gateway = &config["gateway"];
    assert_eq!(gateway["port"], 18800, "the owner's later edit stays");
    assert_eq!(gateway["auth"]["mode"], "token");
    assert!(gateway["auth"].get("password").is_none());
    assert!(gateway["auth"].get("identityScopes").is_none());
    assert!(gateway.get("trustedProxies").is_none());
    assert!(gateway["controlUi"].get("basePath").is_none());
    assert!(gateway["controlUi"].get("allowedOrigins").is_none());
    assert_eq!(
        gateway["auth"]["trustedProxy"],
        serde_json::json!({ "userHeader": "x-owner-choice" })
    );
}

#[test]
fn openclaw_models_keep_fallbacks_and_restore_the_previous_model() {
    let home = Home::new();
    let path = home.write(".openclaw/openclaw.json", OPENCLAW_FIXTURE);
    let install = home.install(Runtime::Openclaw);
    let mut journal = home.journal();

    let outcome = journal
        .apply(
            &install,
            None,
            &Change::NeboaiModels(models("neboai-smart")),
        )
        .unwrap();
    assert!(outcome.changed);
    assert_eq!(outcome.restart, None, "live reload picks up models");
    let config = json5(&path);
    let provider = &config["models"]["providers"]["neboai"];
    assert_eq!(provider["baseUrl"], "http://127.0.0.1:18800/v1");
    assert_eq!(provider["apiKey"], "link-key");
    assert_eq!(provider["api"], "openai-completions");
    assert_eq!(
        provider["models"][1],
        serde_json::json!({ "id": "neboai-smart", "name": "NeboAI Smart" })
    );
    assert_eq!(
        config["agents"]["defaults"]["model"]["primary"],
        "neboai/neboai-smart"
    );
    assert_eq!(
        config["agents"]["defaults"]["model"]["fallbacks"],
        serde_json::json!(["minimax/MiniMax-M2.7"])
    );

    journal
        .revert(&install, None, ChangeKind::NeboaiModels)
        .unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), OPENCLAW_FIXTURE);
}

#[test]
fn two_change_sets_revert_in_either_order() {
    let home = Home::new();
    let path = home.write(".openclaw/openclaw.json", OPENCLAW_FIXTURE);
    let install = home.install(Runtime::Openclaw);
    let mut journal = home.journal();
    journal
        .apply(&install, None, &Change::ProxyAccess(proxy_access()))
        .unwrap();
    journal
        .apply(&install, None, &Change::NeboaiModels(models("neboai-fast")))
        .unwrap();

    // Reverting the older one first can't be byte-exact; it is semantic and
    // leaves the newer change in place.
    let outcome = journal
        .revert(&install, None, ChangeKind::ProxyAccess)
        .unwrap();
    assert!(outcome.conflicts.is_empty());
    let config = json5(&path);
    assert_eq!(config["gateway"]["auth"]["mode"], "token");
    assert_eq!(
        config["agents"]["defaults"]["model"]["primary"],
        "neboai/neboai-fast"
    );

    // A journal reopened from disk still knows the remaining change.
    let mut reopened = home.journal();
    assert_eq!(reopened.applied().len(), 1);
    reopened
        .revert(&install, None, ChangeKind::NeboaiModels)
        .unwrap();
    assert_eq!(
        json5(&path),
        json5::from_str::<Value>(OPENCLAW_FIXTURE).unwrap()
    );
    assert!(
        fs::read_to_string(&path)
            .unwrap()
            .contains("// none | token | password | trusted-proxy")
    );
}

#[test]
fn string_model_is_replaced_and_restored() {
    let home = Home::new();
    let original =
        "{\n  agents: { defaults: { model: 'anthropic/claude-opus-4-6' } }, // strict\n}\n";
    let path = home.write(".openclaw/openclaw.json", original);
    let install = home.install(Runtime::Openclaw);
    let mut journal = home.journal();
    journal
        .apply(&install, None, &Change::NeboaiModels(models("neboai-fast")))
        .unwrap();
    assert_eq!(
        json5(&path)["agents"]["defaults"]["model"],
        "neboai/neboai-fast"
    );
    journal
        .revert(&install, None, ChangeKind::NeboaiModels)
        .unwrap();
    assert_eq!(fs::read_to_string(&path).unwrap(), original);
}

#[test]
fn missing_config_file_is_created_then_removed() {
    let home = Home::new();
    fs::create_dir_all(home.path(".openclaw")).unwrap();
    let install = home.install(Runtime::Openclaw);
    let mut journal = home.journal();
    journal
        .apply(&install, None, &Change::NeboaiModels(models("neboai-fast")))
        .unwrap();
    assert_eq!(
        json5(&install.config_path)["agents"]["defaults"]["model"],
        "neboai/neboai-fast"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = fs::metadata(&install.config_path)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "a created config is owner-only");
        let mode = fs::metadata(home.path("link/journal.json"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the journal is owner-only");
    }
    journal
        .revert(&install, None, ChangeKind::NeboaiModels)
        .unwrap();
    assert!(!install.config_path.exists());
}

#[test]
fn includes_are_refused() {
    let home = Home::new();
    home.write(
        ".openclaw/openclaw.json",
        "{ gateway: { $include: './gateway.json5' } }",
    );
    let install = home.install(Runtime::Openclaw);
    let error = home
        .journal()
        .apply(&install, None, &Change::ProxyAccess(proxy_access()))
        .unwrap_err();
    assert!(
        matches!(error, Error::Include { ref key, .. } if key == "gateway"),
        "{error}"
    );
}

#[test]
fn hermes_models_per_profile() {
    let home = hermes_home();
    let install = home.install(Runtime::Hermes);
    let mut journal = home.journal();
    let default_path = home.path(".hermes/config.yaml");
    let coder_path = home.path(".hermes/profiles/coder/config.yaml");

    let outcome = journal
        .apply(
            &install,
            None,
            &Change::NeboaiModels(models("neboai-smart")),
        )
        .unwrap();
    assert!(outcome.changed);
    assert_eq!(outcome.restart.as_ref(), Some(&install.restart));
    let config = yaml(&default_path);
    assert_eq!(config["model"]["provider"], "neboai");
    assert_eq!(config["model"]["default"], "neboai-smart");
    assert_eq!(config["model"]["base_url"], "http://127.0.0.1:18800/v1");
    assert_eq!(
        config["providers"]["neboai"]["base_url"],
        "http://127.0.0.1:18800/v1"
    );
    assert_eq!(
        config["providers"]["neboai"]["transport"],
        "chat_completions"
    );
    assert_eq!(config["platforms"]["api_server"]["extra"]["port"], 8650);
    let text = fs::read_to_string(&default_path).unwrap();
    assert!(
        text.contains("# providers:\n#   my-gateway:"),
        "comments outside the edited blocks survive"
    );

    journal
        .apply(
            &install,
            Some("coder"),
            &Change::NeboaiModels(models("neboai-fast")),
        )
        .unwrap();
    assert_eq!(yaml(&coder_path)["model"]["default"], "neboai-fast");
    assert_eq!(journal.applied().len(), 2);

    journal
        .revert(&install, Some("coder"), ChangeKind::NeboaiModels)
        .unwrap();
    assert_eq!(fs::read_to_string(&coder_path).unwrap(), "model: gpt-5\n");
    journal
        .revert(&install, None, ChangeKind::NeboaiModels)
        .unwrap();
    assert_eq!(fs::read_to_string(&default_path).unwrap(), HERMES_FIXTURE);
}

#[test]
fn hermes_semantic_revert_restores_the_whole_model_block() {
    let home = hermes_home();
    let path = home.path(".hermes/config.yaml");
    let install = home.install(Runtime::Hermes);
    let mut journal = home.journal();
    journal
        .apply(&install, None, &Change::NeboaiModels(models("neboai-fast")))
        .unwrap();

    // Hermes re-dumps its YAML (comments gone) and the owner changes a theme.
    let mut config = yaml(&path);
    config["dashboard"]["theme"] = "midnight".into();
    fs::write(&path, yaml_serde::to_string(&config).unwrap()).unwrap();

    let outcome = journal
        .revert(&install, None, ChangeKind::NeboaiModels)
        .unwrap();
    assert!(outcome.conflicts.is_empty());
    let config = yaml(&path);
    let original: yaml_serde::Value = yaml_serde::from_str(HERMES_FIXTURE).unwrap();
    assert_eq!(config["model"], original["model"]);
    assert!(config.get("providers").is_none());
    assert_eq!(config["dashboard"]["theme"], "midnight");
}

#[test]
fn hermes_proxy_access_needs_no_config() {
    let home = hermes_home();
    let install = home.install(Runtime::Hermes);
    let mut journal = home.journal();
    let outcome = journal
        .apply(&install, None, &Change::ProxyAccess(proxy_access()))
        .unwrap();
    assert!(!outcome.changed);
    assert!(journal.applied().is_empty());
    assert_eq!(
        fs::read_to_string(home.path(".hermes/config.yaml")).unwrap(),
        HERMES_FIXTURE
    );
    let route = proxy_access().route(Runtime::Hermes);
    assert_eq!(route.path_mode, PathMode::StripWithForwardedPrefix);
    assert_eq!(route.origin, None);
    assert_eq!(route.identity_header, None);
}

#[test]
fn bad_targets_and_parameters_are_errors() {
    let home = hermes_home();
    let install = home.install(Runtime::Hermes);
    let mut journal = home.journal();
    let change = Change::NeboaiModels(models("neboai-fast"));
    assert!(matches!(
        journal.apply(&install, Some("ghost"), &change),
        Err(Error::UnknownProfile(_))
    ));
    assert!(matches!(
        journal.apply(&install, None, &Change::NeboaiModels(models("missing"))),
        Err(Error::InvalidChange(_))
    ));
    let slash = ProxyAccess {
        base_path: "/t/bot/".into(),
        ..proxy_access()
    };
    assert!(matches!(
        journal.apply(&install, None, &Change::ProxyAccess(slash)),
        Err(Error::InvalidChange(_))
    ));
    // Reverting something never applied does nothing.
    assert!(
        !journal
            .revert(&install, None, ChangeKind::NeboaiModels)
            .unwrap()
            .changed
    );
}

#[test]
fn hermes_api_server_key_per_profile_round_trip() {
    let home = hermes_home();
    let install = home.install(Runtime::Hermes);
    let mut journal = home.journal();
    let default_env = home.path(".hermes/.env");
    let writer_env = home.path(".hermes/profiles/writer/.env");
    let original = fs::read_to_string(&default_env).unwrap();
    let change = Change::ApiServer(ApiServer {
        key: "fedcba9876543210fedcba98".into(),
    });

    // The default profile already has a key: the one line is replaced, the
    // comment above it stays, and the gateway must restart to read it.
    let outcome = journal.apply(&install, None, &change).unwrap();
    assert!(outcome.changed);
    assert_eq!(outcome.restart.as_ref(), Some(&install.restart));
    assert_eq!(
        fs::read_to_string(&default_env).unwrap(),
        "# keys\nAPI_SERVER_KEY=fedcba9876543210fedcba98\n"
    );
    assert!(!journal.apply(&install, None, &change).unwrap().changed);

    // A profile without a `.env` gets one, owner-only, and the API server
    // is then detected for it at `/p/writer`.
    assert!(!writer_env.exists());
    let outcome = journal.apply(&install, Some("writer"), &change).unwrap();
    assert!(outcome.changed);
    assert_eq!(
        fs::read_to_string(&writer_env).unwrap(),
        "API_SERVER_KEY=fedcba9876543210fedcba98\n"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&writer_env).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let detected = home.install(Runtime::Hermes);
    let writer = detected
        .endpoints
        .iter()
        .find(|e| {
            e.service
                == Service::HermesApiServer {
                    profile: "writer".into(),
                }
        })
        .expect("writer's API server");
    assert_eq!(writer.base_path, "/p/writer");
    assert_eq!(writer.addr, addr("127.0.0.1:8650"));
    assert_eq!(journal.applied().len(), 2);
    assert_eq!(journal.applied()[1].config_path, writer_env);

    // Reverts restore the file exactly, or remove the one the link created.
    journal
        .revert(&install, Some("writer"), ChangeKind::ApiServer)
        .unwrap();
    assert!(!writer_env.exists());
    let outcome = journal
        .revert(&install, None, ChangeKind::ApiServer)
        .unwrap();
    assert!(outcome.changed);
    assert_eq!(outcome.restart.as_ref(), Some(&install.restart));
    assert_eq!(fs::read_to_string(&default_env).unwrap(), original);
    assert!(journal.applied().is_empty());
}

#[test]
fn hermes_api_server_key_revert_keeps_the_owners_later_edits() {
    let home = hermes_home();
    let install = home.install(Runtime::Hermes);
    let mut journal = home.journal();
    let env = home.path(".hermes/.env");
    journal
        .apply(
            &install,
            None,
            &Change::ApiServer(ApiServer {
                key: "fedcba9876543210fedcba98".into(),
            }),
        )
        .unwrap();
    // The owner adds a variable afterwards.
    let mut text = fs::read_to_string(&env).unwrap();
    text.push_str("OPENAI_API_KEY=sk-live\n");
    fs::write(&env, &text).unwrap();
    let outcome = journal
        .revert(&install, None, ChangeKind::ApiServer)
        .unwrap();
    assert!(outcome.conflicts.is_empty());
    // A semantic revert restores the value, not the owner's quoting.
    assert_eq!(
        fs::read_to_string(&env).unwrap(),
        "# keys\nAPI_SERVER_KEY=0123456789abcdef0123\nOPENAI_API_KEY=sk-live\n"
    );

    // The owner changed the key itself: it is left alone and reported.
    journal
        .apply(
            &install,
            None,
            &Change::ApiServer(ApiServer {
                key: "fedcba9876543210fedcba98".into(),
            }),
        )
        .unwrap();
    fs::write(&env, "# keys\nAPI_SERVER_KEY=owners-own-key-0000\n").unwrap();
    let outcome = journal
        .revert(&install, None, ChangeKind::ApiServer)
        .unwrap();
    assert_eq!(outcome.conflicts, vec!["API_SERVER_KEY".to_owned()]);
    assert_eq!(
        fs::read_to_string(&env).unwrap(),
        "# keys\nAPI_SERVER_KEY=owners-own-key-0000\n"
    );
}

#[test]
fn api_server_keys_are_checked_and_openclaw_needs_none() {
    let home = hermes_home();
    let install = home.install(Runtime::Hermes);
    let mut journal = home.journal();
    for key in ["short", "xxxx-xxxx-xxxx-xxxx-xx", "has space in it 0123", "quote\"0123456789abcdef"] {
        assert!(
            matches!(
                journal.apply(
                    &install,
                    None,
                    &Change::ApiServer(ApiServer { key: key.into() })
                ),
                Err(Error::InvalidChange(_))
            ),
            "{key}"
        );
    }

    let home = Home::new();
    home.write(".openclaw/openclaw.json", OPENCLAW_FIXTURE);
    let install = home.install(Runtime::Openclaw);
    let outcome = home
        .journal()
        .apply(
            &install,
            None,
            &Change::ApiServer(ApiServer {
                key: "fedcba9876543210fedcba98".into(),
            }),
        )
        .unwrap();
    assert!(!outcome.changed);
    assert_eq!(
        fs::read_to_string(home.path(".openclaw/openclaw.json")).unwrap(),
        OPENCLAW_FIXTURE
    );
}
