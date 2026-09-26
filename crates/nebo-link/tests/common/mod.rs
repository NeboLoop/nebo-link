//! A linked OpenClaw install in a temporary home, for driving the service
//! (`nebo_link::run::run`) end to end against the fake hub in [`hub`]. Each
//! test file that uses this runs one service: the bot lease is per process.

pub mod fake_http;
pub mod hub;

use std::path::PathBuf;

use nebo_link::credentials::Credentials;
use nebo_link::endpoints::Endpoints;
use nebo_link::link;
use nebo_link::state::{Link, ModelsEndpoint, Root};
use nebo_runtimes::{Change, Journal, Runtime};

/// The install's config, with `{port}` the gateway's port.
const OPENCLAW_CONFIG: &str = r#"{
  meta: { lastTouchedVersion: "2026.9.6" },
  gateway: { mode: "local", port: {port}, bind: "loopback", auth: { mode: "token", token: "t" } },
}
"#;

/// An OpenClaw install linked the way pairing links one: its config
/// changed through the journal, the token stored, the link saved, and its
/// gateway running (the owner's, answering `/healthz`: the service has
/// nothing to start).
pub struct Linked {
    _tmp: tempfile::TempDir,
    pub root: Root,
    pub bot_id: String,
    pub config: PathBuf,
    /// The config as it was before the link changed it.
    pub original: String,
    /// Every `openclaw` command the service ran, one per line.
    pub commands: PathBuf,
    gateway: std::process::Child,
}

impl Linked {
    /// Links against `hub`. Puts a recording `openclaw` first on `PATH`, so
    /// call it before the runtime starts any threads.
    pub fn new(hub_comms: &str, hub_tunnel: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join(".openclaw");
        std::fs::create_dir_all(&state_dir).unwrap();
        let config = state_dir.join("openclaw.json");
        let gateway_port = free_port();
        let original = OPENCLAW_CONFIG.replace("{port}", &gateway_port.to_string());
        std::fs::write(&config, &original).unwrap();
        // The owner's gateway: answers `/healthz` with 200 before the
        // service looks, so it finds it running and leaves it be.
        let gateway = std::process::Command::new(fake_http::build(tmp.path()))
            .arg(gateway_port.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("the fake gateway runs");
        let started = std::time::Instant::now();
        while std::net::TcpStream::connect(("127.0.0.1", gateway_port)).is_err() {
            assert!(started.elapsed() < std::time::Duration::from_secs(30), "the fake gateway did not start");
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        let bin = tmp.path().join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let commands = tmp.path().join("commands");
        let script = bin.join("openclaw");
        std::fs::write(
            &script,
            format!(
                "#!/bin/sh\necho \"openclaw $* OPENCLAW_STATE_DIR=$OPENCLAW_STATE_DIR\" >> '{}'\n",
                commands.display()
            ),
        )
        .unwrap();
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let path = format!("{}:{}", bin.display(), std::env::var("PATH").unwrap_or_default());
        // SAFETY: called before the test starts any other thread.
        unsafe { std::env::set_var("PATH", path) };

        let root = Root::at(tmp.path().join("nebo-link"));
        let bot_id = uuid::Uuid::new_v4().to_string();
        let link = Link {
            bot_id: bot_id.clone(),
            name: "test-mac · OpenClaw".into(),
            runtime: Runtime::Openclaw,
            owner_id: "owner-1".into(),
            home: state_dir.clone(),
            env: vec![
                // The temporary home, so OpenClaw's service definition is
                // looked for there and never in the real one.
                ("OPENCLAW_HOME".into(), tmp.path().display().to_string()),
                ("OPENCLAW_STATE_DIR".into(), state_dir.display().to_string()),
            ],
            endpoints: Endpoints {
                api: "http://127.0.0.1:9".into(),
                comms: hub_comms.into(),
                tunnel: hub_tunnel.into(),
                janus: "http://127.0.0.1:9".into(),
            },
            local_password: "local-pw".into(),
            models: ModelsEndpoint {
                port: free_port(),
                key: "k".into(),
                enabled: false,
            },
            api_server_key: String::new(),
            services: vec![],
            acp: None,
        };
        let dir = root.bot(&bot_id);
        dir.create().unwrap();
        dir.save(&link).unwrap();
        Credentials::open(&dir).save("bot-token").unwrap();
        let install = nebo_link::install::find(&link).unwrap();
        Journal::open(dir.journal_file())
            .unwrap()
            .apply(&install, None, &Change::ProxyAccess(link::proxy_access(&link)))
            .unwrap();
        assert_ne!(std::fs::read_to_string(&config).unwrap(), original);

        Self {
            _tmp: tmp,
            root,
            bot_id,
            config,
            original,
            commands,
            gateway,
        }
    }

    /// Still linked, with the link's changes in the agent's config.
    pub fn assert_linked(&self) {
        assert_eq!(self.root.links().unwrap().len(), 1);
        assert!(self.root.removed().unwrap().is_empty());
        assert_ne!(std::fs::read_to_string(&self.config).unwrap(), self.original);
        assert!(self.root.bot(&self.bot_id).token_file().exists());
    }

    /// Unlinked after NeboAI removed the bot: the agent's config restored and
    /// the agent restarted onto it, the token and state gone, no service, and
    /// `nebo-link status` saying what happened.
    pub fn assert_removed(&self) {
        assert!(self.root.links().unwrap().is_empty());
        let removed = self.root.removed().unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].bot_id, self.bot_id);

        assert_eq!(std::fs::read_to_string(&self.config).unwrap(), self.original);
        let commands = std::fs::read_to_string(&self.commands).unwrap();
        let state_dir = self.config.parent().unwrap().display().to_string();
        // The owner's gateway was running: nothing started, only restarted.
        assert_eq!(commands.trim(), format!("openclaw gateway restart OPENCLAW_STATE_DIR={state_dir}"));
        assert!(!self.root.bot(&self.bot_id).processes_file().exists());

        let dir = self.root.bot(&self.bot_id);
        assert!(!dir.token_file().exists());
        assert!(!dir.link_file().exists());
        assert!(!dir.journal_file().exists());
        assert!(!nebo_link::service::installed(&self.bot_id));

        let status = std::process::Command::new(env!("CARGO_BIN_EXE_nebo-link"))
            .arg("--home")
            .arg(self.root.path())
            .arg("status")
            .output()
            .unwrap();
        assert!(status.status.success());
        assert_eq!(
            String::from_utf8(status.stdout).unwrap(),
            "test-mac · OpenClaw\n  Removed from NeboAI. Run nebo-link <code> to link again.\n"
        );
    }
}

impl Drop for Linked {
    fn drop(&mut self) {
        let _ = self.gateway.kill();
        let _ = self.gateway.wait();
    }
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}
