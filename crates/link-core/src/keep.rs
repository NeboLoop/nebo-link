//! Adding and removing a host's coding agents. The host does it
//! ([`crate::host::Host::add_agent`], [`crate::host::Host::remove_agent`]),
//! for every client alike (Open Agent Link's `host/agents/add`, the phone
//! contract, `nebo-link add`, Nebo's hire); what it keeps of them is the
//! embedder's, behind a [`Keeper`] (nebo-link's `link.json`, Nebo's
//! `agents.json`).
//!
//! - **Which.** A coding agent installed on this computer ([`installed`]:
//!   Claude Code, Codex, Gemini CLI, OpenCode; the keeper says,
//!   [`Keeper::addable`]), or any command that speaks ACP, given by the
//!   computer's owner on the computer itself.
//! - **Its name.** The agent's own ("Claude Code"); one more of it is
//!   "Claude Code 2", or named for the folder the owner chose for it
//!   ("Claude Code · api").
//! - **Its folder.** One of its own, `~/NeboAI/<its id>`, made now: nobody
//!   is asked for a folder when an agent is added. The owner on the computer
//!   may name one instead.
//! - **Proof.** It is started once in its folder and must answer in ACP
//!   before it is kept.
//! - **Removal** ends its process and forgets it. Its folder, and everything
//!   in it, stays.

use std::path::{Path, PathBuf};

use nebo_runtimes::acp::Agent as AcpAgent;
use nebo_runtimes::{Environment, Runtime, RuntimeCommand};
use serde::{Deserialize, Serialize};

use crate::acp::{AcpLink, Client};
use crate::roster::Member;

/// A coding agent a host runs, as its keeper records it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CodingAgent {
    /// Its id, fixed when it is added: clients name it.
    pub id: String,
    /// Its name: "Claude Code", "Claude Code 2".
    pub label: String,
    pub agent: AcpAgent,
    /// How it starts, and the folder it works in.
    pub acp: AcpLink,
}

/// One agent a keeper holds, of any kind: what a new agent's id, name and
/// folder must not repeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Kept {
    pub id: String,
    pub label: String,
    /// The runtime's id: `claude-code`, `openclaw`, ...
    pub runtime: String,
    /// The folder it works in, for an agent that has one.
    pub folder: Option<PathBuf>,
}

/// What the host keeps its added agents in: the embedder's record.
pub trait Keeper: Send + Sync {
    /// Who drives the agents, as ACP's `initialize` introduces the host.
    fn client(&self) -> Client;

    /// The coding agents that can be added, and how each starts: those
    /// installed on this computer.
    fn addable(&self) -> Vec<Installable> {
        installed()
    }

    /// Every agent the record holds.
    fn agents(&self) -> Vec<Kept>;

    /// Records `agent`, and returns the roster member that runs it.
    fn keep(&self, agent: &CodingAgent) -> Result<Member, String>;

    /// Forgets the agent `id`; its folder stays. The error, in the owner's
    /// words, says why it can't be removed here.
    fn forget(&self, id: &str) -> Result<(), String>;
}

/// The agent to add.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Add {
    /// `claude-code`, `codex`, `gemini`, `opencode`, or `acp` with
    /// `command`.
    pub runtime: String,
    /// Any other agent that speaks ACP, by the command that starts it. Only
    /// the owner on the computer names one.
    pub command: Option<RuntimeCommand>,
    /// The folder it works in; `None` is one of its own under `~/NeboAI`.
    /// Only the owner on the computer names one.
    pub folder: Option<PathBuf>,
    /// Its name; `None` is the agent's own ([`label`]).
    pub label: Option<String>,
}

/// A coding agent that can be added, as clients list it (`host/info`
/// `runtimes`, the phone contract's `runtimes`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Addable {
    /// `claude-code`, `codex`, `gemini`, `opencode`.
    pub id: String,
    pub name: String,
}

/// A coding agent that can be added, and how it starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Installable {
    /// `claude-code`, `codex`, `gemini`, `opencode`.
    pub id: String,
    pub name: String,
    pub agent: AcpAgent,
    /// Starts it speaking ACP on stdio.
    pub command: RuntimeCommand,
}

/// The coding agents installed on this computer, each addable any number of
/// times.
pub fn installed() -> Vec<Installable> {
    let installs = nebo_runtimes::detect(&Environment::current());
    AcpAgent::KNOWN
        .into_iter()
        .filter_map(|agent| {
            let install = installs.iter().find(|i| i.runtime == Runtime::Acp(agent))?;
            Some(Installable {
                id: agent.key().to_owned(),
                name: agent.name().to_owned(),
                agent,
                command: install.restart.clone(),
            })
        })
        .collect()
}

/// The addable agent `runtime` names, or why it can't be added.
pub(crate) fn find(addable: Vec<Installable>, runtime: &str) -> Result<Installable, String> {
    if let Some(found) = addable.into_iter().find(|a| a.id == runtime) {
        return Ok(found);
    }
    Err(match AcpAgent::KNOWN.into_iter().find(|a| a.key() == runtime) {
        Some(agent) => format!("{} isn't installed on this computer.", agent.name()),
        None => format!("{runtime} isn't a coding agent this computer can add."),
    })
}

/// A new agent's name: `name` for the first of its runtime; one more is
/// named for the folder the owner chose for it ("Claude Code · api"), or
/// numbered ("Claude Code 2").
pub(crate) fn label(kept: &[Kept], runtime: &str, name: &str, chosen: Option<&Path>) -> String {
    let same = kept.iter().filter(|k| k.runtime == runtime).count();
    if same == 0 {
        return name.to_owned();
    }
    match chosen.and_then(Path::file_name).map(|f| f.to_string_lossy().into_owned()) {
        Some(folder) if !folder.is_empty() => format!("{name} · {folder}"),
        _ => format!("{name} {}", same + 1),
    }
}

/// A new agent's id: its label as an agent id, unique among the kept ones,
/// and never the first agent's (`assistant`).
pub(crate) fn id(kept: &[Kept], label: &str) -> String {
    let taken: Vec<&str> = std::iter::once(crate::PRIMARY)
        .chain(kept.iter().map(|k| k.id.as_str()))
        .collect();
    crate::roster::new_id(label, &taken)
}

/// Where a new agent works when nobody names a folder: `~/NeboAI/<id>`.
pub fn default_folder(home: &Path, id: &str) -> PathBuf {
    home.join("NeboAI").join(id)
}

/// Makes `folder` (never the whole disk) and returns it as the computer
/// names it.
pub(crate) fn make(folder: &Path, name: &str) -> Result<PathBuf, String> {
    if folder.parent().is_none() {
        return Err(format!("{name} can't work in {}. Choose a project folder.", folder.display()));
    }
    std::fs::create_dir_all(folder).map_err(|e| format!("Could not make the folder {}: {e}", folder.display()))?;
    Ok(folder.canonicalize().unwrap_or_else(|_| folder.to_path_buf()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn kept(id: &str, runtime: &str) -> Kept {
        Kept {
            id: id.into(),
            label: id.into(),
            runtime: runtime.into(),
            folder: None,
        }
    }

    #[test]
    fn a_new_agent_is_named_for_its_runtime_then_numbered() {
        assert_eq!(label(&[], "claude-code", "Claude Code", None), "Claude Code");
        let one = [kept("assistant", "claude-code")];
        assert_eq!(label(&one, "codex", "Codex", None), "Codex");
        assert_eq!(label(&one, "claude-code", "Claude Code", None), "Claude Code 2");
        assert_eq!(label(&one, "claude-code", "Claude Code", Some(Path::new("/w/api"))), "Claude Code · api");
        assert_eq!(id(&one, "Claude Code 2"), "claude-code-2");
        assert_eq!(id(&[], "Assistant"), "assistant-2", "never the first agent's id");
        assert_eq!(id(&[kept("claude-code", "claude-code")], "Claude Code"), "claude-code-2");
    }

    #[test]
    fn a_new_agent_works_in_a_folder_of_its_own() {
        assert_eq!(default_folder(Path::new("/Users/me"), "codex-2"), PathBuf::from("/Users/me/NeboAI/codex-2"));
        let home = tempfile::tempdir().unwrap();
        let made = make(&default_folder(home.path(), "codex"), "Codex").unwrap();
        assert!(made.is_dir() && made.ends_with("NeboAI/codex"));
        assert!(make(Path::new("/"), "Codex").unwrap_err().starts_with("Codex can't work in /."));
    }
}
