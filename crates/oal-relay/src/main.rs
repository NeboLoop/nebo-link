//! `oal-relay`: the self-hostable Open Agent Link relay.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use oal_relay::host::HostEvent;
use oal_relay::server::{self, ADMIN_TOKEN_FILE, Config, TlsFiles};
use oal_relay::{Keypair, RelayClient};

#[derive(Parser)]
#[command(
    name = "oal-relay",
    version,
    about = "The self-hostable relay for Open Agent Link (https://openagent.link)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the relay.
    Serve {
        /// Address to listen on.
        #[arg(long, env = "OAL_RELAY_LISTEN", default_value = "0.0.0.0:8480")]
        listen: SocketAddr,
        #[command(flatten)]
        data: DataDir,
        /// PEM certificate chain: serve HTTPS directly (with --tls-key).
        #[arg(long, env = "OAL_RELAY_TLS_CERT", requires = "tls_key")]
        tls_cert: Option<PathBuf>,
        /// PEM private key for --tls-cert.
        #[arg(long, env = "OAL_RELAY_TLS_KEY", requires = "tls_cert")]
        tls_key: Option<PathBuf>,
        /// The operator's admin token. Default: <data-dir>/admin.token,
        /// created on first run.
        #[arg(long, env = "OAL_RELAY_ADMIN_TOKEN", hide_env_values = true)]
        admin_token: Option<String>,
        /// Only these host keys may register (comma-separated). Default: any.
        #[arg(long, env = "OAL_RELAY_ALLOW_HOSTS", value_delimiter = ',')]
        allow_host: Vec<String>,
        /// The largest WebSocket message relayed, in bytes.
        #[arg(long, env = "OAL_RELAY_MAX_MESSAGE_BYTES", default_value_t = oal_relay::wire::DEFAULT_MAX_MESSAGE_BYTES)]
        max_message_bytes: usize,
    },
    /// Remove a pairing (--host and --client), ban a client key everywhere
    /// (--client), or ban a host and delete it (--host).
    Revoke {
        #[arg(long, required_unless_present = "client")]
        host: Option<String>,
        /// The client's public key.
        #[arg(long)]
        client: Option<String>,
        #[command(flatten)]
        admin: Admin,
    },
    /// List hosts, whether each is online, and its pairings.
    Hosts {
        #[command(flatten)]
        admin: Admin,
    },
    /// Put an OAL host that serves a local WebSocket behind a relay: keep
    /// the host's tunnel open and carry every client connection to --forward.
    Host {
        /// The relay's URL.
        #[arg(long, env = "OAL_RELAY_URL")]
        relay: String,
        /// The host id to register (the host's OAL host id).
        #[arg(long)]
        id: String,
        /// Where the host's key lives; created on first run.
        #[arg(long)]
        key_file: PathBuf,
        /// The host's own OAL WebSocket.
        #[arg(long, default_value = "ws://127.0.0.1:7878/oal")]
        forward: String,
        /// Route pairing to this host for a code it shows: the code's first
        /// four characters (its nameplate). Only the nameplate; the rest of
        /// the code never goes to the relay.
        #[arg(long)]
        nameplate: Option<String>,
    },
}

#[derive(Args)]
struct DataDir {
    /// Where the relay keeps its store and admin token.
    #[arg(long = "data-dir", env = "OAL_RELAY_DATA_DIR")]
    path: Option<PathBuf>,
}

impl DataDir {
    fn resolve(&self) -> PathBuf {
        self.path.clone().unwrap_or_else(|| {
            dirs::data_dir()
                .map(|d| d.join("oal-relay"))
                .unwrap_or_else(|| PathBuf::from("oal-relay-data"))
        })
    }
}

#[derive(Args)]
struct Admin {
    /// The running relay (its admin API).
    #[arg(long, env = "OAL_RELAY_URL", default_value = "http://127.0.0.1:8480")]
    relay: String,
    /// Default: read from <data-dir>/admin.token.
    #[arg(long, env = "OAL_RELAY_ADMIN_TOKEN", hide_env_values = true)]
    admin_token: Option<String>,
    #[command(flatten)]
    data: DataDir,
}

impl Admin {
    fn token(&self) -> Result<String, String> {
        if let Some(t) = &self.admin_token {
            return Ok(t.clone());
        }
        let path = self.data.resolve().join(ADMIN_TOKEN_FILE);
        std::fs::read_to_string(&path)
            .map(|t| t.trim().to_owned())
            .map_err(|e| {
                format!(
                    "No admin token: pass --admin-token or OAL_RELAY_ADMIN_TOKEN, or run this where the relay's data is ({}: {e}).",
                    path.display()
                )
            })
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> Result<serde_json::Value, String> {
        let token = self.token()?;
        let url = format!("{}/admin/{path}", self.relay.trim_end_matches('/'));
        let mut request = reqwest::Client::new()
            .request(method, &url)
            .bearer_auth(token);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("Could not reach the relay at {}: {e}", self.relay))?;
        let ok = response.status().is_success();
        let value: serde_json::Value = response.json().await.map_err(|e| e.to_string())?;
        if ok {
            Ok(value)
        } else {
            Err(value["message"]
                .as_str()
                .unwrap_or("The relay refused.")
                .to_owned())
        }
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    let result = match Cli::parse().command {
        Command::Serve {
            listen,
            data,
            tls_cert,
            tls_key,
            admin_token,
            allow_host,
            max_message_bytes,
        } => {
            init_logs();
            let mut config = Config::new(listen, data.resolve());
            config.tls = tls_cert
                .zip(tls_key)
                .map(|(cert, key)| TlsFiles { cert, key });
            config.admin_token = admin_token;
            config.allow_hosts = allow_host.into_iter().filter(|k| !k.is_empty()).collect();
            config.max_message_bytes = max_message_bytes;
            serve(config).await
        }
        Command::Revoke {
            host,
            client,
            admin,
        } => admin
            .call(
                reqwest::Method::POST,
                "revoke",
                Some(serde_json::json!({"hostId": host, "clientKey": client})),
            )
            .await
            .map(|v| match v["revoked"].as_str() {
                Some("pairing") => {
                    println!("Removed the pairing. That device's connections are closed.")
                }
                Some("client") => println!("Revoked that device everywhere on this relay."),
                Some("host") => println!("Revoked the host and deleted its pairings."),
                _ => println!("{v}"),
            }),
        Command::Hosts { admin } => {
            admin
                .call(reqwest::Method::GET, "hosts", None)
                .await
                .map(|v| {
                    let hosts = v["hosts"].as_array().cloned().unwrap_or_default();
                    if hosts.is_empty() {
                        println!("No hosts have registered yet.");
                    }
                    for h in hosts {
                        println!(
                            "{}  {}  key {}  last seen {}",
                            h["hostId"].as_str().unwrap_or(""),
                            if h["online"] == true {
                                "online "
                            } else {
                                "offline"
                            },
                            h["hostKey"].as_str().unwrap_or(""),
                            h["lastSeenAt"].as_str().unwrap_or("")
                        );
                        for p in h["pairings"].as_array().into_iter().flatten() {
                            println!(
                                "    {}  paired {}",
                                p["clientKey"].as_str().unwrap_or(""),
                                p["pairedAt"].as_str().unwrap_or("")
                            );
                        }
                    }
                })
        }
        Command::Host {
            relay,
            id,
            key_file,
            forward,
            nameplate,
        } => {
            init_logs();
            host(&relay, &id, &key_file, &forward, nameplate.as_deref()).await
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

fn init_logs() {
    let filter = tracing_subscriber::EnvFilter::try_from_env("OAL_RELAY_LOG")
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .init();
}

async fn serve(config: Config) -> Result<(), String> {
    let relay = server::start(config).await.map_err(|e| e.to_string())?;
    shutdown_signal().await;
    tracing::info!("shutting down");
    relay.shutdown().await;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("installing the SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

/// The host's key from `path`, created (owner-only) the first time.
fn host_key(path: &Path) -> Result<Keypair, String> {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    if let Ok(text) = std::fs::read_to_string(path) {
        let bytes: [u8; 32] = b64
            .decode(text.trim())
            .ok()
            .and_then(|v| v.try_into().ok())
            .ok_or_else(|| format!("{} does not hold a key", path.display()))?;
        return Ok(Keypair::from_secret(bytes));
    }
    let key = Keypair::generate();
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir).map_err(|e| e.to_string())?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    let mut file = options
        .open(path)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    std::io::Write::write_all(&mut file, b64.encode(key.secret_bytes()).as_bytes())
        .map_err(|e| e.to_string())?;
    Ok(key)
}

async fn host(
    relay: &str,
    id: &str,
    key_file: &Path,
    forward: &str,
    nameplate: Option<&str>,
) -> Result<(), String> {
    let key = host_key(key_file)?;
    tracing::info!(host = %id, key = %key.public_b64(), relay = %relay, forward = %forward, "host bridge starting");
    let client = RelayClient::new(relay, key).map_err(|e| e.to_string())?;
    let mut backoff = Duration::from_secs(1);
    let mut nameplate = nameplate.map(str::to_owned);
    loop {
        match client.host(id).await {
            Ok(mut tunnel) => {
                backoff = Duration::from_secs(1);
                while let Some(event) = tunnel.next().await {
                    match event {
                        HostEvent::Registered { pairings, .. } => {
                            tracing::info!(host = %id, pairings = pairings.len(), "tunnel up");
                            if let Some(wanted) = nameplate.take() {
                                let handle = tunnel.handle();
                                tokio::spawn(async move {
                                    match handle.nameplate(Some(&wanted)).await {
                                        Ok(n) => println!(
                                            "Pairing through the relay for codes starting {} until {}",
                                            n.nameplate, n.expires_at
                                        ),
                                        Err(e) => eprintln!("Could not register {wanted}: {e}"),
                                    }
                                });
                            }
                        }
                        HostEvent::Client(conn) => {
                            let forward = forward.to_owned();
                            let handle = tunnel.handle();
                            tokio::spawn(async move {
                                // The bridge can't see inside the pairing (it
                                // is the local host's, and encrypted in 0.2),
                                // so it lets every device that reached the
                                // host through a live nameplate through the
                                // relay. The local host's own pairing check
                                // is what admits a device.
                                if conn.nameplate.is_some()
                                    && let Err(e) = handle.paired(&conn.client_key).await
                                {
                                    tracing::warn!(error = %e, "could not record the pairing at the relay");
                                }
                                if let Err(e) = oal_relay::host::forward(conn, &forward).await {
                                    tracing::warn!(error = %e, "could not reach the local host");
                                }
                            });
                        }
                        HostEvent::Unpaired { .. } => {}
                    }
                }
                tracing::info!(host = %id, "tunnel closed; reconnecting");
            }
            Err(oal_relay::Error::Refused { code, message })
                if matches!(
                    code.as_str(),
                    "revoked" | "not_allowed" | "host_id_taken" | "host_key_taken"
                ) =>
            {
                return Err(message);
            }
            Err(e) => tracing::warn!(error = %e, retry_in = ?backoff, "could not open the tunnel"),
        }
        tokio::time::sleep(backoff + jitter()).await;
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}

fn jitter() -> Duration {
    let mut b = [0u8; 2];
    let _ = getrandom::getrandom(&mut b);
    Duration::from_millis(u64::from(u16::from_be_bytes(b)) % 1000)
}
