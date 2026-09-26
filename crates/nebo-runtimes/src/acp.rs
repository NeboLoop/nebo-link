//! ACP agents: coding agents that speak the Agent Client Protocol
//! (<https://agentclientprotocol.com>, protocol version 1) as JSON-RPC over
//! stdio. One adapter drives them all; what differs per agent is only how it
//! is started in ACP mode and how its owner signs in.
//!
//! | Agent | Detected by | Started as |
//! |---|---|---|
//! | Claude Code | `claude` | `claude-agent-acp`, else `npx --yes @agentclientprotocol/claude-agent-acp` |
//! | Codex | `codex` | `codex-acp`, else `npx --yes @agentclientprotocol/codex-acp` |
//! | Gemini CLI | `gemini` | `gemini --acp` |
//! | OpenCode | `opencode` | `opencode acp` |
//! | any other | the command the owner gives | that command |
//!
//! The Claude Code and Codex adapters were published by Zed as
//! `@zed-industries/claude-code-acp` and `@zed-industries/codex-acp`; both are
//! deprecated in favour of the `@agentclientprotocol` packages named above
//! (npm, 2026-09). Each adapter bundles its agent and reads the agent's own
//! sign-in (`~/.claude`, `~/.codex`), so the link never sees a credential.
//!
//! An agent runs under its owner's own login. One that is not signed in
//! answers `session/new` (Codex) or `session/prompt` (Claude Code) with
//! JSON-RPC error `-32000` "Authentication required"
//! ([`client::AUTH_REQUIRED`]); [`Agent::sign_in`] says what to do.
//!
//! Detection finds the agent's program on `PATH` and in the per-user
//! folders installers use (`~/.local/bin`, nvm, Volta, Bun, npm's global
//! prefix), and records the command with absolute paths and the `PATH` its
//! launcher needs (`npx` runs `node` from its own folder), so a background
//! service with a bare `PATH` starts it the same way.

pub mod client;
pub mod protocol;

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::{Environment, Installation, Runtime, RuntimeCommand};

/// The ACP agents Nebo Link knows by name, and `Other` for any command that
/// speaks ACP.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Agent {
    ClaudeCode,
    Codex,
    Gemini,
    Opencode,
    Other,
}

/// How an agent is started in ACP mode.
enum Start {
    /// The agent's own program with these arguments.
    Native(&'static [&'static str]),
    /// An adapter package: its program when installed, else through `npx`.
    Adapter {
        program: &'static str,
        package: &'static str,
    },
}

impl Agent {
    /// The agents detected by name.
    pub const KNOWN: [Agent; 4] = [
        Agent::ClaudeCode,
        Agent::Codex,
        Agent::Gemini,
        Agent::Opencode,
    ];

    /// The key the hub stores in `bots.runtime`.
    pub fn key(self) -> &'static str {
        match self {
            Agent::ClaudeCode => "claude-code",
            Agent::Codex => "codex",
            Agent::Gemini => "gemini",
            Agent::Opencode => "opencode",
            Agent::Other => "acp",
        }
    }

    /// The agent's name as its owner knows it.
    pub fn name(self) -> &'static str {
        match self {
            Agent::ClaudeCode => "Claude Code",
            Agent::Codex => "Codex",
            Agent::Gemini => "Gemini CLI",
            Agent::Opencode => "OpenCode",
            Agent::Other => "ACP agent",
        }
    }

    /// The agent's own program, whose presence means it is installed.
    fn program(self) -> Option<&'static str> {
        match self {
            Agent::ClaudeCode => Some("claude"),
            Agent::Codex => Some("codex"),
            Agent::Gemini => Some("gemini"),
            Agent::Opencode => Some("opencode"),
            Agent::Other => None,
        }
    }

    fn start(self) -> Start {
        match self {
            Agent::ClaudeCode => Start::Adapter {
                program: "claude-agent-acp",
                package: "@agentclientprotocol/claude-agent-acp",
            },
            Agent::Codex => Start::Adapter {
                program: "codex-acp",
                package: "@agentclientprotocol/codex-acp",
            },
            // `packages/cli/src/config/config.ts`: `--acp`
            // (`--experimental-acp` is its deprecated spelling).
            Agent::Gemini => Start::Native(&["--acp"]),
            // `packages/opencode/src/cli/cmd/acp.ts`.
            Agent::Opencode => Start::Native(&["acp"]),
            Agent::Other => Start::Native(&[]),
        }
    }

    /// What the owner does when the agent is not signed in, in one sentence
    /// after the fact: "Claude Code isn't signed in on this computer. Run
    /// `claude` once to sign in."
    pub fn sign_in(self, name: &str) -> String {
        let how = match self {
            Agent::ClaudeCode => "Run `claude` once to sign in.",
            Agent::Codex => "Run `codex login` to sign in.",
            Agent::Gemini => "Run `gemini` once to sign in.",
            Agent::Opencode => "Run `opencode auth login` to sign in.",
            Agent::Other => "Sign in to it in a terminal, then try again.",
        };
        format!("{name} isn't signed in on this computer. {how}")
    }
}

/// The installed ACP agents of the user `env` describes, one installation
/// each: `home` is the agent's program (what identifies it among the linked
/// ones), `restart` the command that starts it in ACP mode (starting it again
/// is how it is restarted). An agent that is installed but can't be started
/// in ACP mode (no adapter and no `npx`) carries the reason in
/// `config_error`.
pub(crate) fn detect(env: &Environment) -> Vec<Installation> {
    Agent::KNOWN
        .iter()
        .filter_map(|&agent| {
            let program = which(agent.program()?, env)?;
            let (start, problem) = match agent.start() {
                Start::Native(args) => (Some(command(&program, args, &[], env)), None),
                Start::Adapter { program: adapter, package } => match (which(adapter, env), which("npx", env)) {
                    (Some(adapter), _) => (Some(command(&adapter, &[], &[&program], env)), None),
                    (None, Some(npx)) => (Some(command(&npx, &["--yes", package], &[&program], env)), None),
                    (None, None) => (
                        None,
                        Some(format!(
                            "{} needs Node.js to run for NeboAI: install Node.js (it includes npx), or `npm install -g {package}`, then try again.",
                            agent.name()
                        )),
                    ),
                },
            };
            Some(installation(agent, program, start, problem))
        })
        .collect()
}

/// The installation for an ACP command the owner names (`goose acp`, a
/// path to an agent, ...): its program is found as detection finds programs.
pub fn custom(command_line: &str, env: &Environment) -> Result<Installation, String> {
    let mut words = command_line.split_whitespace();
    let first = words
        .next()
        .ok_or_else(|| "The ACP command is empty.".to_owned())?;
    let program = if first.contains(['/', '\\']) {
        let path = PathBuf::from(first);
        path.is_file().then_some(path)
    } else {
        which(first, env)
    }
    .ok_or_else(|| format!("Could not find `{first}` on this computer."))?;
    let args: Vec<&str> = words.collect();
    let start = command(&program, &args, &[], env);
    Ok(installation(Agent::Other, program, Some(start), None))
}

fn installation(
    agent: Agent,
    program: PathBuf,
    start: Option<RuntimeCommand>,
    problem: Option<String>,
) -> Installation {
    Installation {
        runtime: Runtime::Acp(agent),
        home: program.clone(),
        config_path: program,
        version: None,
        proxy_supported: None,
        config_error: problem,
        endpoints: Vec::new(),
        profiles: Vec::new(),
        restart: start.unwrap_or(RuntimeCommand {
            program: String::new(),
            args: Vec::new(),
            env: Vec::new(),
        }),
        processes: Vec::new(),
    }
}

/// `program args…`, with `PATH` led by the folders of the program and of
/// `beside` (a launcher's interpreter, the agent an adapter wraps).
fn command(program: &Path, args: &[&str], beside: &[&Path], env: &Environment) -> RuntimeCommand {
    let mut dirs: Vec<PathBuf> = Vec::new();
    for path in std::iter::once(program).chain(beside.iter().copied()) {
        if let Some(dir) = path.parent().map(Path::to_path_buf)
            && !dirs.contains(&dir)
        {
            dirs.push(dir);
        }
    }
    dirs.extend(
        env.var("PATH")
            .map(|p| std::env::split_paths(p).collect::<Vec<_>>())
            .unwrap_or_default(),
    );
    let path = std::env::join_paths(dirs)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    RuntimeCommand {
        program: program.to_string_lossy().into_owned(),
        args: args.iter().map(|a| (*a).to_owned()).collect(),
        env: vec![("PATH".to_owned(), path)],
    }
}

/// Finds `name` on `PATH`, then in the per-user folders installers put
/// programs in (a service's `PATH` often lacks them).
pub fn which(name: &str, env: &Environment) -> Option<PathBuf> {
    let mut dirs: Vec<PathBuf> = env
        .var("PATH")
        .map(|p| std::env::split_paths(p).collect())
        .unwrap_or_default();
    if let Some(home) = &env.home {
        for dir in [
            ".local/bin",
            ".claude/local",
            ".npm-global/bin",
            ".volta/bin",
            ".bun/bin",
            ".asdf/shims",
        ] {
            dirs.push(home.join(dir));
        }
        dirs.extend(newest_nvm_bin(home));
    }
    let names: Vec<String> = if cfg!(windows) {
        ["", ".exe", ".cmd"]
            .iter()
            .map(|ext| format!("{name}{ext}"))
            .collect()
    } else {
        vec![name.to_owned()]
    };
    dirs.iter()
        .flat_map(|dir| names.iter().map(move |n| dir.join(n)))
        .find(|path| is_executable(path))
}

/// `~/.nvm/versions/node/<newest>/bin`.
fn newest_nvm_bin(home: &Path) -> Option<PathBuf> {
    let versions = std::fs::read_dir(home.join(".nvm/versions/node")).ok()?;
    versions
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            let parts: Vec<u64> = name
                .trim_start_matches('v')
                .split('.')
                .map_while(|p| p.parse().ok())
                .collect();
            (!parts.is_empty()).then(|| (parts, entry.path().join("bin")))
        })
        .max_by(|a, b| a.0.cmp(&b.0))
        .map(|(_, bin)| bin)
}

fn is_executable(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.is_file() && meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        meta.is_file()
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    fn program(dir: &Path, name: &str) -> PathBuf {
        std::fs::create_dir_all(dir).unwrap();
        let path = dir.join(name);
        std::fs::write(&path, "#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn env(home: &Path, path: &[&Path]) -> Environment {
        let mut env = Environment {
            home: Some(home.to_path_buf()),
            vars: Default::default(),
        };
        let joined = std::env::join_paths(path).unwrap();
        env.vars
            .insert("PATH".into(), joined.to_string_lossy().into_owned());
        env
    }

    #[test]
    fn claude_code_runs_through_npx_with_node_on_its_path() {
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("bin");
        let claude = program(&tmp.path().join(".local/bin"), "claude");
        let node_bin = tmp.path().join(".nvm/versions/node/v22.19.0/bin");
        program(&tmp.path().join(".nvm/versions/node/v9.1.0/bin"), "npx");
        let npx = program(&node_bin, "npx");
        let found = detect(&env(tmp.path(), &[&bin]));
        assert_eq!(found.len(), 1, "{found:?}");
        let install = &found[0];
        assert_eq!(install.runtime, Runtime::Acp(Agent::ClaudeCode));
        assert_eq!(install.home, claude);
        assert_eq!(install.restart.program, npx.to_string_lossy());
        assert_eq!(
            install.restart.args,
            ["--yes", "@agentclientprotocol/claude-agent-acp"]
        );
        let path = &install.restart.env[0].1;
        assert!(path.starts_with(&*node_bin.to_string_lossy()), "{path}");
        assert!(
            path.contains(".local/bin") && path.ends_with("/bin"),
            "{path}"
        );
        assert!(install.config_error.is_none());
    }

    #[test]
    fn an_installed_adapter_is_preferred_and_native_agents_take_their_flag() {
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("bin");
        program(&bin, "codex");
        let adapter = program(&bin, "codex-acp");
        program(&bin, "gemini");
        program(&bin, "npx");
        let found = detect(&env(tmp.path(), &[&bin]));
        let codex = found
            .iter()
            .find(|i| i.runtime == Runtime::Acp(Agent::Codex))
            .unwrap();
        assert_eq!(codex.restart.program, adapter.to_string_lossy());
        assert!(codex.restart.args.is_empty());
        let gemini = found
            .iter()
            .find(|i| i.runtime == Runtime::Acp(Agent::Gemini))
            .unwrap();
        assert_eq!(gemini.restart.args, ["--acp"]);
    }

    #[test]
    fn without_node_the_agent_says_what_it_needs() {
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("bin");
        program(&bin, "claude");
        let found = detect(&env(tmp.path(), &[&bin]));
        assert!(
            found[0]
                .config_error
                .as_deref()
                .unwrap()
                .contains("needs Node.js")
        );
    }

    #[test]
    fn a_custom_command_is_found_like_any_program() {
        let tmp = tempfile::tempdir().unwrap();
        let bin = tmp.path().join("bin");
        let goose = program(&bin, "goose");
        let install = custom("goose acp", &env(tmp.path(), &[&bin])).unwrap();
        assert_eq!(install.runtime, Runtime::Acp(Agent::Other));
        assert_eq!(install.home, goose);
        assert_eq!(install.restart.args, ["acp"]);
        assert!(custom("nowhere-to-be-found acp", &env(tmp.path(), &[&bin])).is_err());
        assert!(custom("  ", &env(tmp.path(), &[&bin])).is_err());
    }

    #[test]
    fn keys_and_sign_in_words() {
        assert_eq!(Agent::ClaudeCode.key(), "claude-code");
        assert_eq!(
            serde_json::to_string(&Agent::ClaudeCode).unwrap(),
            "\"claude-code\""
        );
        assert_eq!(
            Agent::ClaudeCode.sign_in("Claude Code"),
            "Claude Code isn't signed in on this computer. Run `claude` once to sign in."
        );
    }
}
