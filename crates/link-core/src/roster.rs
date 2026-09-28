//! Every agent a computer's host runs. Each hosted
//! agent (a Claude Code in one folder, a Codex in another, an OpenClaw
//! install) is a [`Member`] with its own backend, so two instances of one
//! runtime share nothing: not a process, a session, a question or a folder.
//!
//! - **Agents.** Every agent has one id, the same for every client (Open
//!   Agent Link, the phone contract, Nebo's `linked/<bot>/<agent>`): lowercase
//!   letters, digits and hyphens, at most 63 characters, starting with a
//!   letter or digit ([`agent_id`]). A member's default agent takes the
//!   member's id (the first member's is `assistant`); any other agent of it
//!   (an OpenClaw agent, a Hermes profile) is `<member>-<agent>`, except on
//!   the first member, whose other agents keep their own ids as they did when
//!   a bot had one. Ids saved before this rule (`<member>.<agent>`, a
//!   runtime's own id with capitals or underscores) still name their agent:
//!   an id that names none is read as [`agent_id`] of itself.
//! - **Sessions** are the runtime's own ids, scoped by the agent they
//!   belong to.
//! - **Membership** changes while the host runs ([`Roster::set`]): a member
//!   that stays keeps its backend, running process and sessions.

use std::sync::{Arc, RwLock};

use crate::PRIMARY;
use crate::backend::{Agent, Backend, Error};

/// One hosted agent.
#[derive(Clone)]
pub struct Member {
    /// The hosted agent's id, fixed when it was added: clients name it.
    pub id: String,
    /// Its name as the owner reads it in "Could not connect to <label>".
    pub label: String,
    /// Its runtime's id: `claude-code`, `codex`, `openclaw`, `hermes`, ...
    pub runtime: String,
    pub backend: Arc<dyn Backend>,
}

/// The longest agent id (Open Agent Link's `^[a-z0-9][a-z0-9-]{0,62}$`).
pub const MAX_ID: usize = 63;

/// `text` as an agent id: lowercase letters and digits, every other run of
/// characters one hyphen, no hyphen first or last, at most [`MAX_ID`]
/// characters; `agent` when nothing is left ("Claude Code · api" →
/// `claude-code-api`, `openclaw.research` → `openclaw-research`). An id
/// already in that form is its own.
pub fn agent_id(text: &str) -> String {
    let mut slug = String::new();
    for c in text.chars().flat_map(char::to_lowercase) {
        if c.is_ascii_alphanumeric() {
            slug.push(c);
        } else if !slug.ends_with('-') && !slug.is_empty() {
            slug.push('-');
        }
    }
    slug.truncate(MAX_ID);
    match slug.trim_end_matches('-') {
        "" => "agent".to_owned(),
        s => s.to_owned(),
    }
}

/// Whether `id` is in the form every agent id takes ([`agent_id`]).
pub fn is_agent_id(id: &str) -> bool {
    agent_id(id) == id
}

/// `base` made unique against `taken` with `-2`, `-3`, ..., still at most
/// [`MAX_ID`] characters.
fn unique(base: String, taken: &[&str]) -> String {
    if !taken.contains(&base.as_str()) {
        return base;
    }
    (2..)
        .map(|n| {
            let suffix = format!("-{n}");
            let mut stem = base.clone();
            stem.truncate(MAX_ID - suffix.len());
            format!("{}{suffix}", stem.trim_end_matches('-'))
        })
        .find(|id| !taken.contains(&id.as_str()))
        .expect("a free id")
}

/// A new member's id: `label` as an agent id ([`agent_id`]), made unique
/// against `taken` with `-2`, `-3`, ...
pub fn new_id(label: &str, taken: &[&str]) -> String {
    unique(agent_id(label), taken)
}

/// The hosted agents, in the link's order.
#[derive(Default)]
pub struct Roster {
    members: RwLock<Vec<Member>>,
}

impl Roster {
    pub fn new(members: Vec<Member>) -> Self {
        Self {
            members: RwLock::new(members),
        }
    }

    /// Replaces the members: the link added or removed an agent.
    pub fn set(&self, members: Vec<Member>) {
        *self.members.write().expect("roster") = members;
    }

    pub fn members(&self) -> Vec<Member> {
        self.members.read().expect("roster").clone()
    }

    /// The member an agent id names, and that agent's id in the member's
    /// runtime. An id saved before the ids took their one form is read as
    /// [`agent_id`] of itself.
    pub async fn locate(&self, agent: &str) -> Result<(Member, String), Error> {
        if let Some(found) = self.find(agent).await? {
            return Ok(found);
        }
        let canonical = agent_id(agent);
        if canonical != agent
            && let Some(found) = self.find(&canonical).await?
        {
            return Ok(found);
        }
        Err(Error::NotFound(format!("The agent {agent}")))
    }

    /// The member and runtime agent `agent` names exactly.
    async fn find(&self, agent: &str) -> Result<Option<(Member, String)>, Error> {
        let members = self.members();
        let ids: Vec<&str> = members.iter().map(|m| m.id.as_str()).collect();
        // A member's own id names its default agent; `<member>-<agent>` one of
        // its others (the longest member id first, so `site-2-docs` is
        // `site-2`'s before `site`'s); any other id one of the first member's
        // others.
        let mut candidates: Vec<(&Member, bool)> = Vec::new();
        if let Some(member) = members.iter().find(|m| m.id == agent) {
            candidates.push((member, true));
        }
        let mut prefixed: Vec<&Member> = members
            .iter()
            .filter(|m| m.id != PRIMARY)
            .filter(|m| agent.strip_prefix(m.id.as_str()).is_some_and(|rest| rest.starts_with('-')))
            .collect();
        prefixed.sort_by_key(|m| std::cmp::Reverse(m.id.len()));
        candidates.extend(prefixed.into_iter().map(|m| (m, false)));
        if let Some(primary) = members.iter().find(|m| m.id == PRIMARY) {
            candidates.push((primary, false));
        }
        for (member, default) in candidates {
            let agents = member.backend.agents().await.map_err(|e| unreachable(member, e))?;
            let named = agent_ids(member, &agents, &ids);
            let found = agents.into_iter().zip(named).find(|(a, id)| match default {
                true => a.is_default,
                false => !a.is_default && id == agent,
            });
            if let Some((a, _)) = found {
                return Ok(Some((member.clone(), a.id)));
            }
        }
        Ok(None)
    }

    /// The id `agent` names its agent by: itself, or for an id saved before
    /// the ids took their one form, the agent's id now.
    pub async fn canonical(&self, agent: &str) -> Result<String, Error> {
        let (member, sub) = self.locate(agent).await?;
        let ids: Vec<String> = self.members().iter().map(|m| m.id.clone()).collect();
        let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
        let agents = member.backend.agents().await.map_err(|e| unreachable(&member, e))?;
        let named = agent_ids(&member, &agents, &ids);
        agents
            .iter()
            .zip(named)
            .find(|(a, _)| a.id == sub)
            .map(|(_, id)| id)
            .ok_or_else(|| Error::NotFound(format!("The agent {agent}")))
    }
}

/// The id of each of `member`'s agents ([`agent_id`]), in order: its default
/// agent by the member's id, another by `<member>-<agent>` (on the first
/// member, by its own id), made unique against every member's id and the
/// member's other agents. `members` is every member's id.
pub(crate) fn agent_ids(member: &Member, agents: &[Agent], members: &[&str]) -> Vec<String> {
    let mut taken: Vec<String> = members.iter().map(|m| (*m).to_owned()).collect();
    agents
        .iter()
        .map(|agent| {
            if agent.is_default {
                return member.id.clone();
            }
            let base = match member.id == PRIMARY {
                true => agent_id(&agent.id),
                false => agent_id(&format!("{}-{}", member.id, agent.id)),
            };
            let refs: Vec<&str> = taken.iter().map(String::as_str).collect();
            let id = unique(base, &refs);
            taken.push(id.clone());
            id
        })
        .collect()
}

/// A member's agents as the roster names them: ids by [`agent_ids`], names
/// by [`agent_names`].
pub(crate) fn member_agents(member: &Member, agents: Vec<Agent>, members: &[&str]) -> Vec<Agent> {
    let ids = agent_ids(member, &agents, members);
    let names = agent_names(member, &agents);
    agents
        .into_iter()
        .zip(ids)
        .zip(names)
        .map(|((agent, id), name)| Agent {
            is_default: id == PRIMARY,
            id,
            name,
            ..agent
        })
        .collect()
}

/// Names a runtime gives an agent nobody named: OpenClaw's "Assistant" and
/// "main", Hermes' "default".
const GENERIC_NAMES: [&str; 4] = ["assistant", "main", "default", "agent"];

/// The name of each of `member`'s agents, in order: an agent with a generic
/// name ([`GENERIC_NAMES`]) is named after its app, the member's label
/// ("OpenClaw"), the default agent first; another one is "<label> · <its
/// id>". Named agents keep their names.
pub(crate) fn agent_names(member: &Member, agents: &[Agent]) -> Vec<String> {
    let generic = |a: &Agent| GENERIC_NAMES.contains(&a.name.trim().to_lowercase().as_str());
    let plain = agents
        .iter()
        .position(|a| a.is_default && generic(a))
        .or_else(|| agents.iter().position(generic));
    agents
        .iter()
        .enumerate()
        .map(|(i, a)| match () {
            _ if Some(i) == plain => member.label.clone(),
            _ if generic(a) => format!("{} · {}", member.label, a.id),
            _ => a.name.clone(),
        })
        .collect()
}

/// A member that doesn't answer reads as its own name, not the host's.
fn unreachable(member: &Member, error: Error) -> Error {
    match error {
        Error::Unavailable(why) => {
            tracing::info!(agent = %member.id, why, "contract: the agent is not answering");
            Error::Failed(format!("Could not connect to {}. Try again.", member.label))
        }
        other => other,
    }
}

impl Roster {
    /// Ready when one agent is: the members are asked in order, and the
    /// first that answers ends it, so the others start only when used.
    pub async fn ready(&self) -> Result<(), String> {
        let members = self.members();
        let mut first_error = None;
        for member in &members {
            match member.backend.ready().await {
                Ok(()) => return Ok(()),
                Err(why) => {
                    first_error.get_or_insert(why);
                }
            }
        }
        Err(first_error.unwrap_or_else(|| "no agent is linked; add one with `nebo-link add`".to_owned()))
    }

    /// Every member's agents; a member whose runtime doesn't answer is left
    /// out, unless none answers.
    pub async fn agents(&self) -> Result<Vec<Agent>, Error> {
        Ok(self.listed().await?.into_iter().map(|(_, agent)| agent).collect())
    }

    /// [`Roster::agents`], each with its member's runtime (`claude-code`,
    /// `openclaw`, ...).
    pub async fn listed(&self) -> Result<Vec<(String, Agent)>, Error> {
        let mut all = Vec::new();
        let mut failed = None;
        let members = self.members();
        let ids: Vec<&str> = members.iter().map(|m| m.id.as_str()).collect();
        for member in &members {
            match member.backend.agents().await {
                Ok(agents) => all.extend(member_agents(member, agents, &ids).into_iter().map(|a| (member.runtime.clone(), a))),
                Err(e) => {
                    failed.get_or_insert(unreachable(member, e));
                }
            }
        }
        match failed {
            Some(e) if all.is_empty() => Err(e),
            _ => Ok(all),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::BoxFuture;

    fn agent(id: &str, is_default: bool) -> Agent {
        Agent {
            id: id.into(),
            name: id.into(),
            description: String::new(),
            is_default,
            folder: None,
            capabilities: serde_json::json!({}),
            modes: None,
            offline_reason: None,
        }
    }

    struct Nothing;

    impl Backend for Nothing {
        fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
            Box::pin(async { Ok(()) })
        }
        fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
            Box::pin(async { Ok(Vec::new()) })
        }
        fn connect(&self, _: crate::backend::Inbox) {}
        fn request<'a>(&'a self, _: &'a str, _: &'a str, _: serde_json::Value) -> BoxFuture<'a, Result<serde_json::Value, crate::model::ErrorObject>> {
            Box::pin(async { Ok(serde_json::json!({})) })
        }
        fn notify(&self, _: &str, _: &str, _: serde_json::Value) {}
        fn model<'a>(&'a self, _: &'a str, _: Option<&'a str>) -> BoxFuture<'a, Result<String, Error>> {
            Box::pin(async { Ok(String::new()) })
        }
    }

    fn member(id: &str) -> Member {
        Member {
            id: id.into(),
            label: id.into(),
            runtime: "openclaw".into(),
            backend: Arc::new(Nothing),
        }
    }

    #[test]
    fn agent_ids_are_one_form() {
        assert_eq!(agent_id("openclaw.research"), "openclaw-research");
        assert_eq!(agent_id("Research_Bot"), "research-bot");
        assert_eq!(agent_id("--x--"), "x");
        assert_eq!(agent_id("é"), "agent");
        let long = agent_id(&"a".repeat(80));
        assert_eq!(long.len(), MAX_ID);
        assert!(is_agent_id(&long));
        assert!(!agent_id(&format!("{}-b", "a".repeat(62))).ends_with('-'));
        assert!(is_agent_id("claude-code-2") && !is_agent_id("openclaw.main") && !is_agent_id("Main"));
    }

    #[test]
    fn every_agent_of_a_member_has_an_id_in_that_form() {
        let members = ["assistant", "openclaw", "openclaw-research"];
        let others = agent_ids(
            &member("openclaw"),
            &[agent("main", true), agent("research", false), agent("Night_Owl", false)],
            &members,
        );
        // `openclaw-research` is another member's: the agent takes the next.
        assert_eq!(others, ["openclaw", "openclaw-research-2", "openclaw-night-owl"]);
        let first = agent_ids(
            &member("assistant"),
            &[agent("main", true), agent("research", false), agent("openclaw", false)],
            &members,
        );
        assert_eq!(first, ["assistant", "research", "openclaw-2"]);
        assert!(first.iter().chain(&others).all(|id| is_agent_id(id)));
    }

    /// An agent the runtime left with a generic name is named after its app:
    /// OpenClaw's default "Assistant" reads "OpenClaw", a second generic one
    /// "OpenClaw · <id>"; named agents keep their names.
    #[test]
    fn generic_agent_names_are_the_apps() {
        let mut openclaw = member("openclaw");
        openclaw.label = "OpenClaw".into();
        let named = |id: &str, name: &str, is_default: bool| Agent { name: name.into(), ..agent(id, is_default) };
        let agents = [
            named("research", "Research", false),
            named("work", "main", false),
            named("main", "Assistant", true),
        ];
        assert_eq!(agent_names(&openclaw, &agents), ["Research", "OpenClaw · work", "OpenClaw"]);
        let listed = member_agents(&openclaw, agents.to_vec(), &["assistant", "openclaw"]);
        assert_eq!(listed[2].name, "OpenClaw");
        let mut hermes = member("hermes");
        hermes.label = "Hermes".into();
        assert_eq!(agent_names(&hermes, &[named("default", "Default", false), named("coder", "coder", false)]), ["Hermes", "coder"]);
        assert_eq!(agent_names(&hermes, &[named("default", "Hermes", true)]), ["Hermes"]);
    }

    #[test]
    fn ids_are_unique_slugs_of_labels() {
        assert_eq!(new_id("Claude Code", &[]), "claude-code");
        assert_eq!(new_id("Claude Code · api", &[]), "claude-code-api");
        assert_eq!(new_id("Site", &["site"]), "site-2");
        assert_eq!(new_id("Site", &["site", "site-2"]), "site-3");
        assert_eq!(new_id("···", &[]), "agent");
        assert_eq!(new_id("Assistant", &[PRIMARY]), "assistant-2");
        let long = "x".repeat(70);
        let first = new_id(&long, &[]);
        let taken = [first.as_str()];
        let second = new_id(&long, &taken);
        assert_eq!(second.len(), MAX_ID);
        assert!(second.ends_with("-2") && is_agent_id(&second));
    }
}
