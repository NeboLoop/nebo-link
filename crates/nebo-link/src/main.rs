//! `nebo-link`: link an OpenClaw or Hermes agent to NeboAI.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use nebo_runtimes::Runtime;
use tokio::sync::watch;

use nebo_link::credentials::Credentials;
use nebo_link::error::{Error, Result};
use nebo_link::install::{runtime_key, runtime_name};
use nebo_link::state::{Root, STATUS_EVERY};
use nebo_link::{link, run, service, update};

#[derive(Debug, Parser)]
#[command(
    name = "nebo-link",
    version,
    about = "Link the OpenClaw or Hermes agent on this machine to NeboAI."
)]
struct Cli {
    /// The one-time code from the NeboAI app (Connect OpenClaw or Hermes).
    code: Option<String>,

    /// Which agent to link when both are installed.
    #[arg(long, value_enum)]
    runtime: Option<RuntimeArg>,

    /// The bot's name in NeboAI (default: this machine's name and the agent).
    #[arg(long)]
    name: Option<String>,

    /// Where the link keeps its state (default: the platform data directory).
    #[arg(long, env = "NEBO_LINK_HOME", global = true)]
    home: Option<PathBuf>,

    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run a linked bot's connection (what the installed service runs).
    Run {
        #[arg(long)]
        bot: String,
    },
    /// Show what is linked and whether it is connected.
    Status,
    /// Unlink: restore the agent's config, remove the service, forget the bot.
    Unlink {
        #[arg(long)]
        bot: Option<String>,
    },
    /// Print the service's recent log.
    Logs {
        #[arg(long)]
        bot: Option<String>,
        /// How many lines.
        #[arg(long, default_value_t = 100)]
        lines: usize,
    },
    /// Use NeboAI models in the linked agent, or restore its own.
    Models {
        state: Toggle,
        #[arg(long)]
        bot: Option<String>,
    },
    /// Update nebo-link to the latest release and restart the linked bots.
    Update,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum RuntimeArg {
    Openclaw,
    Hermes,
}

impl From<RuntimeArg> for Runtime {
    fn from(arg: RuntimeArg) -> Self {
        match arg {
            RuntimeArg::Openclaw => Runtime::Openclaw,
            RuntimeArg::Hermes => Runtime::Hermes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum Toggle {
    On,
    Off,
}

impl Cli {
    /// Parses the command line. Pairing (a code with its options) and the
    /// subcommands are exclusive; `--home` goes with either.
    fn parse_from_args<I: IntoIterator<Item = String>>(args: I) -> std::result::Result<Self, clap::Error> {
        let cli = Self::try_parse_from(args)?;
        if cli.command.is_some() && (cli.code.is_some() || cli.runtime.is_some() || cli.name.is_some()) {
            return Err(Self::command().error(
                clap::error::ErrorKind::ArgumentConflict,
                "a code, --runtime and --name are for pairing and can't be combined with a command",
            ));
        }
        Ok(cli)
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse_from_args(std::env::args()).unwrap_or_else(|e| e.exit());
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    match runtime.block_on(dispatch(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}

async fn dispatch(cli: Cli) -> Result<()> {
    let root = Root::resolve(cli.home)?;
    let Some(command) = cli.command else {
        let _log = init_logging(None);
        return match cli.code {
            Some(code) => pair(&root, &code, cli.runtime.map(Into::into), cli.name).await,
            None => status(&root),
        };
    };
    match command {
        Command::Run { bot } => {
            let _log = init_logging(Some(root.bot(&bot).logs_dir()));
            run::run(&root, &bot).await
        }
        Command::Status => status(&root),
        Command::Update => {
            let _log = init_logging(None);
            self_update(&root).await
        }
        Command::Unlink { bot } => {
            let _log = init_logging(None);
            let link = root.select(bot.as_deref())?;
            let unlinked = link::unlink(&root, &link, link::By::Owner).await?;
            println!("Unlinked {} ({}).", link.name, link.bot_id);
            if let Some(reason) = unlinked.not_restored {
                println!("Its config was not restored: {reason}");
            }
            if let Some(reason) = unlinked.not_restarted {
                println!("Its config was restored, but restarting it failed: {reason}");
            }
            if !unlinked.conflicts.is_empty() {
                println!("Left as you changed them: {}", unlinked.conflicts.join(", "));
            }
            println!("To remove the bot from your account too, remove it in the NeboAI app.");
            Ok(())
        }
        Command::Logs { bot, lines } => {
            let link = root.select(bot.as_deref())?;
            print!("{}", tail(&root.bot(&link.bot_id).logs_dir(), lines)?);
            Ok(())
        }
        Command::Models { state, bot } => {
            let _log = init_logging(None);
            let mut link = root.select(bot.as_deref())?;
            let dir = root.bot(&link.bot_id);
            let token = Credentials::open(&dir).load()?;
            let (_token_tx, token_rx) = watch::channel(token);
            let janus = link::janus(&link, token_rx);
            let change = link::set_models(&dir, &mut link, &janus, state == Toggle::On).await?;
            println!(
                "{} {} NeboAI models.{}",
                runtime_name(link.runtime),
                if change.enabled { "now uses" } else { "no longer uses" },
                if change.restarted { " It was restarted to pick this up." } else { "" }
            );
            if !change.conflicts.is_empty() {
                println!("Left as you changed them: {}", change.conflicts.join(", "));
            }
            Ok(())
        }
    }
}

async fn pair(root: &Root, code: &str, runtime: Option<Runtime>, name: Option<String>) -> Result<()> {
    let exe = std::env::current_exe()
        .and_then(|p| p.canonicalize())
        .map_err(|e| Error::Message(format!("could not locate the nebo-link binary: {e}")))?;
    let paired = link::pair(root, code, runtime, name, exe).await?;
    let link = &paired.link;
    println!(
        "Linked {} at {} to NeboAI as \"{}\".",
        runtime_name(link.runtime),
        link.home.display(),
        link.name
    );
    println!("Bot id: {}", link.bot_id);
    println!("nebo-link now runs in the background and starts with this computer.");
    println!("Open the NeboAI app to reach it. Check it any time with `nebo-link status`.");
    if let Some(problem) = &paired.restart_failed {
        println!(
            "Restart {} to finish: {problem}\nIf you started it yourself in a terminal, stop it and start it again.",
            runtime_name(link.runtime)
        );
    }
    Ok(())
}

/// `nebo-link update`: replace the binary with the latest verified release,
/// then restart every bot's service so each runs it.
async fn self_update(root: &Root) -> Result<()> {
    let feed = update::official().ok_or_else(|| Error::Message(update::NO_KEY.into()))?;
    if let Some(why) = update::not_self_updating() {
        return Err(Error::Message(why.into()));
    }
    let _lock = update::lock(root).await?;
    let Some(downloaded) = update::fetch(&feed, update::VERSION).await? else {
        println!("nebo-link is up to date ({}).", update::VERSION);
        return Ok(());
    };
    update::replace(&downloaded, root)?;
    println!("Updated nebo-link {} → {}.", update::VERSION, downloaded.version());
    for link in root.links()? {
        if !service::installed(&link.bot_id) {
            continue;
        }
        if let Err(e) = service::restart(&link.bot_id) {
            println!("{} is still running the old version: {e}", link.name);
        }
    }
    Ok(())
}

fn status(root: &Root) -> Result<()> {
    let links = root.links()?;
    let removed = root.removed()?;
    if links.is_empty() && removed.is_empty() {
        println!("Nothing is linked. Run `nebo-link <code>` with a code from the NeboAI app.");
        return Ok(());
    }
    for bot in removed {
        println!("{}", bot.name);
        println!("  Removed from NeboAI. Run nebo-link <code> to link again.");
    }
    for link in links {
        let dir = root.bot(&link.bot_id);
        let running = dir
            .load_status()
            .filter(|s| unix_age(s.updated) <= 3 * STATUS_EVERY.as_secs());
        let state = match &running {
            None if !service::installed(&link.bot_id) => "not running (no service installed)".to_string(),
            None => "not running".to_string(),
            Some(s) if s.online && s.tunnel => "online".to_string(),
            Some(s) if s.online => "online, connecting the tunnel".to_string(),
            Some(s) => match &s.error {
                Some(error) => format!("offline: {error}"),
                None => "offline".to_string(),
            },
        };
        println!("{}", link.name);
        println!("  bot:     {}", link.bot_id);
        println!("  agent:   {} ({}) at {}", runtime_name(link.runtime), runtime_key(link.runtime), link.home.display());
        println!("  status:  {state}");
        if let Some(s) = &running {
            let chat = match (&s.chat, &s.chat_error) {
                (true, _) => "on".to_string(),
                (false, Some(why)) => format!("off: {why}"),
                (false, None) => "off".to_string(),
            };
            println!("  chat:    {chat}");
        }
        println!("  models:  {}", if link.models.enabled { "NeboAI" } else { "the agent's own" });
        println!("  state:   {}", dir.path().display());
    }
    Ok(())
}

fn unix_age(then: u64) -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
        .saturating_sub(then)
}

/// The last `lines` lines of the newest log file in `dir`.
fn tail(dir: &std::path::Path, lines: usize) -> Result<String> {
    let newest = std::fs::read_dir(dir)
        .map_err(|e| Error::io(dir, e))?
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("nebo-link"))
        .max_by_key(|e| e.metadata().and_then(|m| m.modified()).ok())
        .ok_or_else(|| Error::Message(format!("no logs yet in {}", dir.display())))?;
    let text = std::fs::read_to_string(newest.path()).map_err(|e| Error::io(newest.path(), e))?;
    let all: Vec<&str> = text.lines().collect();
    let start = all.len().saturating_sub(lines);
    Ok(all[start..].iter().map(|l| format!("{l}\n")).collect())
}

/// Logging: the service logs to a daily-rotated file in its state directory
/// (a week kept); commands log warnings to stderr. Nothing logs tokens,
/// headers or message bodies.
fn init_logging(dir: Option<PathBuf>) -> Option<tracing_appender::non_blocking::WorkerGuard> {
    use tracing_subscriber::EnvFilter;
    let filter = |default: &str| EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default));
    match dir.and_then(|dir| {
        tracing_appender::rolling::Builder::new()
            .rotation(tracing_appender::rolling::Rotation::DAILY)
            .filename_prefix("nebo-link")
            .filename_suffix("log")
            .max_log_files(7)
            .build(dir)
            .ok()
    }) {
        Some(appender) => {
            let (writer, guard) = tracing_appender::non_blocking(appender);
            tracing_subscriber::fmt()
                .with_env_filter(filter("info"))
                .with_writer(writer)
                .with_ansi(false)
                .init();
            Some(guard)
        }
        None => {
            tracing_subscriber::fmt()
                .with_env_filter(filter("warn"))
                .with_writer(std::io::stderr)
                .init();
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> std::result::Result<Cli, clap::Error> {
        Cli::parse_from_args(std::iter::once("nebo-link").chain(args.iter().copied()).map(String::from))
    }

    #[test]
    fn a_code_pairs() {
        let cli = parse(&["ABCD-1234"]).unwrap();
        assert_eq!(cli.code.as_deref(), Some("ABCD-1234"));
        assert!(cli.command.is_none() && cli.runtime.is_none());

        let cli = parse(&["ABCD-1234", "--runtime", "hermes", "--name", "Home"]).unwrap();
        assert_eq!(cli.runtime, Some(RuntimeArg::Hermes));
        assert_eq!(cli.name.as_deref(), Some("Home"));
        assert!(parse(&["ABCD-1234", "--runtime", "other"]).is_err());
    }

    #[test]
    fn no_arguments_shows_status() {
        let cli = parse(&[]).unwrap();
        assert!(cli.code.is_none() && cli.command.is_none());
    }

    #[test]
    fn subcommands() {
        assert!(matches!(
            parse(&["run", "--bot", "b1"]).unwrap().command,
            Some(Command::Run { bot }) if bot == "b1"
        ));
        assert!(parse(&["run"]).is_err(), "run needs --bot");
        assert!(matches!(parse(&["status"]).unwrap().command, Some(Command::Status)));
        assert!(matches!(
            parse(&["unlink"]).unwrap().command,
            Some(Command::Unlink { bot: None })
        ));
        assert!(matches!(
            parse(&["logs", "--bot", "b1", "--lines", "5"]).unwrap().command,
            Some(Command::Logs { bot: Some(b), lines: 5 }) if b == "b1"
        ));
        assert!(matches!(
            parse(&["models", "on", "--bot", "b1"]).unwrap().command,
            Some(Command::Models { state: Toggle::On, bot: Some(b) }) if b == "b1"
        ));
        assert!(matches!(
            parse(&["models", "off"]).unwrap().command,
            Some(Command::Models { state: Toggle::Off, bot: None })
        ));
        assert!(parse(&["models", "maybe"]).is_err());
        assert!(matches!(parse(&["update"]).unwrap().command, Some(Command::Update)));
        assert!(parse(&["update", "--bot", "b1"]).is_err(), "update is for the binary, not a bot");
    }

    #[test]
    fn a_code_is_not_a_subcommand() {
        assert!(parse(&["ABCD-1234", "status"]).is_err());
        assert!(parse(&["status", "ABCD-1234"]).is_err());
        assert!(parse(&["status", "--runtime", "hermes"]).is_err());
        assert!(parse(&["--home", "/tmp/x", "status"]).is_ok());
    }

    #[test]
    fn home_applies_everywhere() {
        let cli = parse(&["--home", "/tmp/x", "run", "--bot", "b"]).unwrap();
        assert_eq!(cli.home, Some(PathBuf::from("/tmp/x")));
        let cli = parse(&["run", "--bot", "b", "--home", "/tmp/y"]).unwrap();
        assert_eq!(cli.home, Some(PathBuf::from("/tmp/y")));
    }

    #[test]
    fn tail_reads_the_newest_log() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("nebo-link.2026-09-24.log"), "old\n").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        std::fs::write(tmp.path().join("nebo-link.2026-09-25.log"), "a\nb\nc\n").unwrap();
        assert_eq!(tail(tmp.path(), 2).unwrap(), "b\nc\n");
    }
}
