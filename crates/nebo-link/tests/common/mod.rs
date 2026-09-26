//! A linked OpenClaw install in a temporary home and a fake NeboAI hub, for
//! driving the service (`nebo_link::run::run`) end to end. Each test file
//! that uses this runs one service: the bot lease is per process.

use std::path::PathBuf;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nebo_comm::frame;
use nebo_link::credentials::Credentials;
use nebo_link::endpoints::Endpoints;
use nebo_link::link;
use nebo_link::state::{Link, ModelsEndpoint, Root};
use nebo_runtimes::{Change, Journal, Runtime};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

pub type Ws = WebSocketStream<TcpStream>;

/// How long the hub waits for the service to do the next thing.
pub const PATIENCE: Duration = Duration::from_secs(20);

/// The hub's comms gateway and tunnel endpoints. Every connection the
/// service makes is handed to the test.
pub struct FakeHub {
    pub comms: String,
    pub tunnel: String,
    comms_rx: mpsc::UnboundedReceiver<Ws>,
    tunnel_rx: mpsc::UnboundedReceiver<Ws>,
}

impl FakeHub {
    pub async fn start() -> Self {
        let (comms, comms_rx) = accept_all("/ws").await;
        let (tunnel, tunnel_rx) = accept_all("/tunnel/connect").await;
        Self {
            comms,
            tunnel,
            comms_rx,
            tunnel_rx,
        }
    }

    /// The next comms connection, with its CONNECT read.
    pub async fn next_connect(&mut self) -> Ws {
        let mut ws = tokio::time::timeout(PATIENCE, self.comms_rx.recv())
            .await
            .expect("the service dialed the comms gateway")
            .unwrap();
        let message = tokio::time::timeout(PATIENCE, ws.next())
            .await
            .expect("the service sent CONNECT")
            .unwrap()
            .unwrap();
        let data = message.into_data();
        let (header, _) = frame::decode(&data).unwrap();
        assert_eq!(header.frame_type, frame::TYPE_CONNECT);
        ws
    }

    /// Whether the service dialed the comms gateway again.
    pub fn redialed(&mut self) -> bool {
        self.comms_rx.try_recv().is_ok()
    }

    pub async fn next_tunnel(&mut self) -> Ws {
        tokio::time::timeout(PATIENCE, self.tunnel_rx.recv())
            .await
            .expect("the service dialed the tunnel")
            .unwrap()
    }
}

async fn accept_all(path: &str) -> (String, mpsc::UnboundedReceiver<Ws>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}{path}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            if let Ok(ws) = tokio_tungstenite::accept_async(sock).await {
                let _ = tx.send(ws);
            }
        }
    });
    (url, rx)
}

/// The token the fake hub rotates to on every AUTH_OK, as the real hub does.
pub const ROTATED_TOKEN: &str = "rotated-bot-token";

/// Answers CONNECT: AUTH_OK carrying a rotated token, or AUTH_FAIL with
/// `reason`.
pub async fn answer(ws: &mut Ws, refused: Option<&str>) {
    let (frame_type, payload) = match refused {
        None => (frame::TYPE_AUTH_OK, serde_json::json!({ "ok": true, "token": ROTATED_TOKEN })),
        Some(reason) => (frame::TYPE_AUTH_FAIL, serde_json::json!({ "ok": false, "reason": reason })),
    };
    let data = frame::encode(
        frame::Header {
            frame_type,
            ..Default::default()
        },
        &serde_json::to_vec(&payload).unwrap(),
    )
    .unwrap();
    ws.send(Message::Binary(data.into())).await.unwrap();
}

const OPENCLAW_CONFIG: &str = r#"{
  meta: { lastTouchedVersion: "2026.9.6" },
  gateway: { mode: "local", port: 18789, bind: "loopback", auth: { mode: "token", token: "t" } },
}
"#;

/// An OpenClaw install linked the way pairing links one: its config
/// changed through the journal, the token stored, the link saved.
pub struct Linked {
    _tmp: tempfile::TempDir,
    pub root: Root,
    pub bot_id: String,
    pub config: PathBuf,
    /// Every `openclaw` command the service ran, one per line.
    pub commands: PathBuf,
}

impl Linked {
    /// Links against `hub`. Puts a recording `openclaw` first on `PATH`, so
    /// call it before the runtime starts any threads.
    pub fn new(hub_comms: &str, hub_tunnel: &str) -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let state_dir = tmp.path().join(".openclaw");
        std::fs::create_dir_all(&state_dir).unwrap();
        let config = state_dir.join("openclaw.json");
        std::fs::write(&config, OPENCLAW_CONFIG).unwrap();

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
            env: vec![("OPENCLAW_STATE_DIR".into(), state_dir.display().to_string())],
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
        assert_ne!(std::fs::read_to_string(&config).unwrap(), OPENCLAW_CONFIG);

        Self {
            _tmp: tmp,
            root,
            bot_id,
            config,
            commands,
        }
    }

    /// Still linked, with the link's changes in the agent's config.
    pub fn assert_linked(&self) {
        assert_eq!(self.root.links().unwrap().len(), 1);
        assert!(self.root.removed().unwrap().is_empty());
        assert_ne!(std::fs::read_to_string(&self.config).unwrap(), OPENCLAW_CONFIG);
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

        assert_eq!(std::fs::read_to_string(&self.config).unwrap(), OPENCLAW_CONFIG);
        let commands = std::fs::read_to_string(&self.commands).unwrap();
        let state_dir = self.config.parent().unwrap().display().to_string();
        assert_eq!(commands.trim(), format!("openclaw gateway restart OPENCLAW_STATE_DIR={state_dir}"));

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

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}
