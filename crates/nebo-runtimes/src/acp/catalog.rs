//! The agent programs Nebo can find on a computer and drive over ACP, as
//! data: how each is found, how it starts speaking ACP, how to tell its
//! version and whether its owner is signed in, and how it updates.
//!
//! The catalog is data so a new agent program is added without a release.
//! Nebo's hub serves the full one; an owner may add or replace entries in a
//! file of their own; and [`Catalog::builtin`] ([`catalog.json`](./catalog.json))
//! holds the few this crate knows by name ([`super::Agent`]), so detection
//! works without the hub. [`Catalog::merge`] layers them, later entries
//! replacing earlier ones with the same id.
//!
//! ```json
//! { "programs": [{
//!     "id": "codex",
//!     "name": "Codex",
//!     "find": [{ "program": "codex" }],
//!     "acp": { "adapter": { "program": "codex-acp", "package": "@agentclientprotocol/codex-acp" } },
//!     "version": { "args": ["--version"] },
//!     "signIn": { "files": ["~/.codex/auth.json"], "command": ["{program}", "login"] },
//!     "update": {
//!       "latest": { "npm": "@openai/codex" },
//!       "commands": [{ "pathContains": "/node_modules/", "command": ["npm", "install", "-g", "@openai/codex@latest"] }]
//!     }
//! }] }
//! ```
//!
//! - `find`: the program's file names, looked for on `PATH`, in the per-user
//!   and Homebrew folders installers use ([`super::which`]), and in `dirs`.
//!   `pathContains` keeps only a program whose real path (links followed)
//!   contains it: `agent` is two different programs' name.
//! - `acp`: the program with `args`, or an `adapter` program (run through
//!   `npx` when it isn't installed).
//! - `version`, `update.latest`: a [`Source`].
//! - `signIn`: signed in when one of `env` is set, one of `files` exists, or
//!   `status` says so; `command` signs the owner in.
//! - `update.commands`: the first whose `pathContains` matches the program's
//!   real path (or has none) updates it.
//!
//! In a command, `{program}` is the program found; any other first word is a
//! program looked for like the agent's own.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::{Environment, RuntimeCommand};

/// Stands for the program found, in a catalog command.
pub const PROGRAM: &str = "{program}";

/// A catalog of agent programs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Catalog {
    #[serde(default)]
    pub programs: Vec<Program>,
}

/// One agent program.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Program {
    /// Its id: `codex`, and the runtime a new agent of it is added as.
    pub id: String,
    /// Its name as its owner knows it: "Codex".
    pub name: String,
    pub find: Vec<Find>,
    pub acp: Start,
    /// Where its version is read; none is the ACP `initialize` answer's
    /// `agentInfo.version`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<Source>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sign_in: Option<SignIn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update: Option<Update>,
}

/// A file name the program is installed as.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Find {
    /// A bare file name (`codex`), never a path.
    pub program: String,
    /// Folders of its own installer, looked in first (`~/.tool/bin`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dirs: Vec<String>,
    /// Kept only when its real path contains this (`/.tool/`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_contains: Option<String>,
}

/// How the program starts speaking ACP on stdio.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Start {
    /// The arguments it takes (the program's own, or the adapter's).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// An adapter that speaks ACP for it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adapter: Option<Adapter>,
}

/// An ACP adapter: its program when installed, else its package through
/// `npx`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Adapter {
    pub program: String,
    pub package: String,
}

/// Where a version is read, the first that is set: `npm`, the latest
/// version of that package on the npm registry; `file`, a JSON file's value
/// at `pointer` (or the file's text); `args`, the first version in what the
/// program prints when run with them; else the ACP `initialize` answer's
/// value at `pointer` (`/agentInfo/version` when none).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Source {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub npm: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pointer: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
}

/// Whether its owner is signed in, and how they sign in.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SignIn {
    /// Its sign-in is kept in one of these files.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub files: Vec<String>,
    /// A key in one of these variables signs it in.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
    /// Its own status command.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<Status>,
    /// The command that signs its owner in.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    /// What its owner does to sign in, in one sentence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

/// A status command: signed out when it fails, or prints `signedOut`
/// (any case).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Status {
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_out: Option<String>,
}

/// How the program updates.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Update {
    /// Where its latest version is read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest: Option<Source>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub commands: Vec<UpdateCommand>,
}

/// An update command, for a program installed where `pathContains` says.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateCommand {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_contains: Option<String>,
    pub command: Vec<String>,
}

/// An agent program found on this computer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub program: Program,
    /// The program's file, as found (a link is not followed).
    pub path: PathBuf,
    /// Starts it speaking ACP; `None` when it can't be (`problem` says why).
    pub command: Option<RuntimeCommand>,
    /// `command` runs an adapter through `npx`, which fetches it when it is
    /// not in npm's cache.
    pub via_npx: bool,
    pub problem: Option<String>,
}

impl Catalog {
    /// The programs this crate knows by name ([`super::Agent`]).
    pub fn builtin() -> &'static Catalog {
        static BUILTIN: OnceLock<Catalog> = OnceLock::new();
        BUILTIN.get_or_init(|| {
            let catalog = Catalog::parse(include_str!("catalog.json")).expect("the built-in catalog parses");
            assert_eq!(catalog.programs.len(), 4, "every built-in entry is valid");
            catalog
        })
    }

    /// A catalog from its JSON, without the entries that aren't valid
    /// ([`Program::valid`]).
    pub fn parse(json: &str) -> Result<Catalog, String> {
        let mut catalog: Catalog = serde_json::from_str(json).map_err(|e| format!("not an agent catalog: {e}"))?;
        catalog.programs.retain(Program::valid);
        Ok(catalog)
    }

    /// This catalog with `over`'s entries: one with an id this catalog has
    /// replaces it where it stands, a new one is added at the end.
    pub fn merge(mut self, over: Catalog) -> Catalog {
        for program in over.programs {
            match self.programs.iter_mut().find(|p| p.id == program.id) {
                Some(same) => *same = program,
                None => self.programs.push(program),
            }
        }
        self
    }

    pub fn get(&self, id: &str) -> Option<&Program> {
        self.programs.iter().find(|p| p.id == id)
    }

    /// The catalog's programs installed for the user `env` describes, each
    /// with the command that starts it speaking ACP. A program two entries
    /// find is the first's.
    pub fn find(&self, env: &Environment) -> Vec<Found> {
        let mut found: Vec<Found> = Vec::new();
        for program in &self.programs {
            let Some(path) = program.find.iter().find_map(|f| find(f, env)) else {
                continue;
            };
            let real_path = real(&path);
            if found.iter().any(|f| real(&f.path) == real_path) {
                continue;
            }
            let (command, via_npx, problem) = start(program, &path, env);
            found.push(Found {
                program: program.clone(),
                path,
                command,
                via_npx,
                problem,
            });
        }
        found
    }
}

impl Program {
    /// An entry detection can use safely: an id that is an agent id, bare
    /// program names, and an ACP start.
    pub fn valid(&self) -> bool {
        let bare = |name: &str| !name.is_empty() && !name.contains(['/', '\\']) && name != "." && name != "..";
        let id_ok = !self.id.is_empty()
            && self.id.len() <= 63
            && self.id.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
            && !self.id.starts_with('-');
        id_ok
            && !self.name.trim().is_empty()
            && !self.find.is_empty()
            && self.find.iter().all(|f| bare(&f.program))
            && self.acp.adapter.as_ref().is_none_or(|a| bare(&a.program) && !a.package.is_empty())
    }

    /// The update command for the program at `path`, as a command to run.
    pub fn update_command(&self, path: &Path, env: &Environment) -> Option<RuntimeCommand> {
        let real = real(path).to_string_lossy().into_owned();
        let chosen = self
            .update
            .as_ref()?
            .commands
            .iter()
            .find(|c| c.path_contains.as_deref().is_none_or(|part| real.contains(part)))?;
        resolve(&chosen.command, path, env)
    }

    /// The sign-in command for the program at `path`, as a command to run.
    pub fn sign_in_command(&self, path: &Path, env: &Environment) -> Option<RuntimeCommand> {
        resolve(&self.sign_in.as_ref()?.command, path, env)
    }
}

/// `words` as a command: `{program}` is the program at `path`, another
/// first word a program found like an agent's; `None` when it isn't found.
pub fn resolve(words: &[String], path: &Path, env: &Environment) -> Option<RuntimeCommand> {
    let (first, rest) = words.split_first()?;
    let program = if first == PROGRAM {
        path.to_path_buf()
    } else if first.contains(['/', '\\']) {
        return None;
    } else {
        super::which(first, env)?
    };
    let args: Vec<&str> = rest.iter().map(|w| if w == PROGRAM { path.to_str().unwrap_or(w) } else { w.as_str() }).collect();
    Some(super::command(&program, &args, &[path], env))
}

/// Where `find` finds its program: its own folders first, then where
/// programs are installed; the first whose real path passes.
fn find(find: &Find, env: &Environment) -> Option<PathBuf> {
    let own: Vec<PathBuf> = match &env.home {
        Some(home) => find.dirs.iter().map(|d| crate::environment::expand_tilde(d, home)).collect(),
        None => find.dirs.iter().filter(|d| !d.starts_with('~')).map(PathBuf::from).collect(),
    };
    super::candidates(&find.program, &own, env).find(|path| {
        find.path_contains.as_deref().is_none_or(|part| {
            let part = match (&env.home, part.strip_prefix("~/")) {
                (Some(home), Some(rest)) => home.join(rest).to_string_lossy().into_owned(),
                _ => part.to_owned(),
            };
            real(path).to_string_lossy().contains(&part)
        })
    })
}

/// The command that starts `program` (found at `path`) speaking ACP.
fn start(program: &Program, path: &Path, env: &Environment) -> (Option<RuntimeCommand>, bool, Option<String>) {
    let args: Vec<&str> = program.acp.args.iter().map(String::as_str).collect();
    let Some(adapter) = &program.acp.adapter else {
        return (Some(super::command(path, &args, &[], env)), false, None);
    };
    if let Some(installed) = super::which(&adapter.program, env) {
        return (Some(super::command(&installed, &args, &[path], env)), false, None);
    }
    match super::which("npx", env) {
        Some(npx) => {
            let npx_args: Vec<&str> = ["--yes", adapter.package.as_str()].into_iter().chain(args).collect();
            (Some(super::command(&npx, &npx_args, &[path], env)), true, None)
        }
        None => (
            None,
            false,
            Some(format!(
                "{} needs Node.js to run for NeboAI: install Node.js (it includes npx), or `npm install -g {}`, then try again.",
                program.name, adapter.package
            )),
        ),
    }
}

/// `path` with every link followed; `path` itself when it can't be.
fn real(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
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
            ..Default::default()
        };
        let joined = std::env::join_paths(path).unwrap();
        env.vars.insert("PATH".into(), joined.to_string_lossy().into_owned());
        env
    }

    /// Two programs installed under one name, told apart by where they
    /// really live, as a catalog from the hub describes them.
    const TWO_AGENTS: &str = r#"{ "programs": [
        { "id": "tool-a", "name": "Tool A", "find": [{ "program": "agent", "dirs": ["~/.tool-a/bin"], "pathContains": "~/.tool-a/" }],
          "acp": { "args": ["agent", "stdio"] } },
        { "id": "tool-b", "name": "Tool B",
          "find": [{ "program": "tool-b-agent" }, { "program": "agent", "pathContains": "/tool-b/" }],
          "acp": { "args": ["acp"] } }
    ] }"#;

    #[test]
    fn a_program_is_found_where_its_own_installer_put_it_and_told_apart_by_its_real_path() {
        let tmp = tempfile::tempdir().unwrap();
        let a = program(&tmp.path().join(".tool-a/downloads"), "tool-a-1.0");
        std::fs::create_dir_all(tmp.path().join(".tool-a/bin")).unwrap();
        std::os::unix::fs::symlink(&a, tmp.path().join(".tool-a/bin/agent")).unwrap();
        let catalog = Catalog::parse(TWO_AGENTS).unwrap();
        // Not on PATH: its installer's own folder finds it.
        let found = catalog.find(&env(tmp.path(), &[]));
        assert_eq!(found.len(), 1, "{found:?}");
        assert_eq!(found[0].program.id, "tool-a");
        assert_eq!(found[0].path, tmp.path().join(".tool-a/bin/agent"));
        let start = found[0].command.as_ref().unwrap();
        assert_eq!(start.program, found[0].path.to_string_lossy());
        assert_eq!(start.args, ["agent", "stdio"]);

        // `~/.local/bin/agent` is the other program: only its real path says so.
        let b = program(&tmp.path().join(".local/share/tool-b/versions/2"), "tool-b-agent");
        std::fs::create_dir_all(tmp.path().join(".local/bin")).unwrap();
        std::os::unix::fs::symlink(&b, tmp.path().join(".local/bin/agent")).unwrap();
        let found = catalog.find(&env(tmp.path(), &[]));
        let ids: Vec<&str> = found.iter().map(|f| f.program.id.as_str()).collect();
        assert_eq!(ids, ["tool-a", "tool-b"]);
        assert_eq!(found[1].path, tmp.path().join(".local/bin/agent"));
        assert_eq!(found[1].command.as_ref().unwrap().args, ["acp"]);
    }

    #[test]
    fn a_service_with_a_bare_path_finds_homebrew_and_npm_installs_with_absolute_paths() {
        let tmp = tempfile::tempdir().unwrap();
        let prefix = tmp.path().join("npm-prefix");
        std::fs::write(tmp.path().join(".npmrc"), format!("prefix={}\n", prefix.display())).unwrap();
        let gemini = program(&prefix.join("bin"), "gemini");
        let brew_bin = tmp.path().join("homebrew/bin");
        let opencode = program(&brew_bin, "opencode");
        let mut env = env(tmp.path(), &[Path::new("/usr/bin")]);
        env.system_dirs = vec![brew_bin];
        let found = Catalog::builtin().find(&env);
        let ids: Vec<&str> = found.iter().map(|f| f.program.id.as_str()).collect();
        assert_eq!(ids, ["gemini", "opencode"]);
        assert_eq!((&found[0].path, &found[1].path), (&gemini, &opencode));
        let start = found[0].command.as_ref().unwrap();
        assert!(Path::new(&start.program).is_absolute());
        assert_eq!(start.args, ["--acp"]);
        assert!(start.env[0].1.starts_with(&*prefix.join("bin").to_string_lossy()), "{:?}", start.env);
    }

    #[test]
    fn the_hub_replaces_and_adds_entries_and_invalid_ones_are_dropped() {
        let hub = Catalog::parse(
            r#"{ "programs": [
                { "id": "gemini", "name": "Gemini CLI", "find": [{ "program": "gemini" }], "acp": { "args": ["--experimental-acp"] } },
                { "id": "tool-c", "name": "Tool C", "find": [{ "program": "tool-c" }], "acp": { "args": ["acp"] } },
                { "id": "Bad Id", "name": "x", "find": [{ "program": "x" }], "acp": {} },
                { "id": "sh-anything", "name": "x", "find": [{ "program": "/bin/sh" }], "acp": { "args": ["-c", "true"] } }
            ] }"#,
        )
        .unwrap();
        assert_eq!(hub.programs.len(), 2);
        let merged = Catalog::builtin().clone().merge(hub);
        let ids: Vec<&str> = merged.programs.iter().map(|p| p.id.as_str()).collect();
        assert_eq!(ids, ["claude-code", "codex", "gemini", "opencode", "tool-c"]);
        assert_eq!(merged.get("gemini").unwrap().acp.args, ["--experimental-acp"]);
    }

    #[test]
    fn the_update_command_follows_how_the_program_was_installed() {
        let tmp = tempfile::tempdir().unwrap();
        let cask = program(&tmp.path().join("homebrew/Caskroom/codex/1/bin"), "codex");
        let npm_codex = program(&tmp.path().join("node/lib/node_modules/@openai/codex/bin"), "codex");
        let brew = program(&tmp.path().join("homebrew/bin"), "brew");
        let npm = program(&tmp.path().join("node/bin"), "npm");
        let env = env(tmp.path(), &[&tmp.path().join("homebrew/bin"), &tmp.path().join("node/bin")]);
        let codex = Catalog::builtin().get("codex").unwrap();
        let by_brew = codex.update_command(&cask, &env).unwrap();
        assert_eq!((by_brew.program.as_str(), by_brew.args.as_slice()), (&*brew.to_string_lossy(), &["upgrade".to_owned(), "codex".to_owned()][..]));
        let by_npm = codex.update_command(&npm_codex, &env).unwrap();
        assert_eq!(by_npm.program, npm.to_string_lossy());
        assert_eq!(by_npm.args, ["install", "-g", "@openai/codex@latest"]);
        // Installed some other way: no command it could be sure of.
        assert!(codex.update_command(&program(&tmp.path().join("elsewhere"), "codex"), &env).is_none());
        // `{program}` is the program itself.
        let claude = program(&tmp.path().join("bin"), "claude");
        let own = Catalog::builtin().get("claude-code").unwrap().update_command(&claude, &env).unwrap();
        assert_eq!((own.program, own.args), (claude.to_string_lossy().into_owned(), vec!["update".to_owned()]));
    }
}
