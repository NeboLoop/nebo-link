//! Every agent a machine's link hosts, behind one contract: the bot's one
//! [`Backend`]. Each hosted agent (a Claude Code in one folder, a Codex in
//! another, an OpenClaw install) is a [`Member`] with its own backend, so
//! two instances of one runtime share nothing: not a process, a session, a
//! question or a folder.
//!
//! - **Agents.** A member's default agent takes the member's id (the first
//!   member's is `assistant`); any other agent of it (an OpenClaw agent, a
//!   Hermes profile) is `<member>.<agent>`, except on the first member,
//!   whose other agents keep their own ids as they did when a bot had one.
//! - **Chats.** A chat's id is its runtime session's, prefixed
//!   `<member>~` on every member but the first, so a chat id names its
//!   agent even where the REST paths give no agent, and two agents whose
//!   runtimes number sessions alike never share one.
//! - **Membership** changes while the bot runs ([`Roster::set`]): a member
//!   that stays keeps its backend, running process and sessions.

use std::sync::{Arc, RwLock};

use super::PRIMARY;
use super::backend::{Agent, Backend, BoxFuture, Chat, Error, Message, Permission, Turn};

/// What separates a member's id from its runtime's session id in a chat id.
const SEP: char = '~';

/// One hosted agent.
#[derive(Clone)]
pub struct Member {
    /// The hosted agent's id ([`crate::state::Hosted::id`]).
    pub id: String,
    /// Its name as the owner reads it in "Could not connect to <label>".
    pub label: String,
    pub backend: Arc<dyn Backend>,
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

    /// The member a contract agent id names, and that agent's id in the
    /// member's runtime.
    async fn locate(&self, agent: &str) -> Result<(Member, String), Error> {
        let members = self.members();
        // A member's own id names its default agent; `<member>.<agent>` one
        // of its others; any other id, one of the first member's others.
        let named = members
            .iter()
            .find(|m| m.id == agent)
            .map(|m| (m, None))
            .or_else(|| {
                members.iter().filter(|m| m.id != PRIMARY).find_map(|m| {
                    let sub = agent.strip_prefix(m.id.as_str())?.strip_prefix('.')?;
                    Some((m, Some(sub)))
                })
            })
            .or_else(|| members.iter().find(|m| m.id == PRIMARY).map(|m| (m, Some(agent))));
        let Some((member, sub)) = named else {
            return Err(Error::NotFound(format!("The agent {agent}")));
        };
        let agents = member.backend.agents().await.map_err(|e| unreachable(member, e))?;
        agents
            .into_iter()
            .find(|a| match sub {
                None => a.is_default,
                Some(sub) => !a.is_default && a.id == sub,
            })
            .map(|a| (member.clone(), a.id))
            .ok_or_else(|| Error::NotFound(format!("The agent {agent}")))
    }

    /// A member's chat as the contract names it.
    fn exposed(member: &Member, chat: &str) -> String {
        if member.id == PRIMARY {
            chat.to_owned()
        } else {
            format!("{}{SEP}{chat}", member.id)
        }
    }

    /// The member's own session id for a contract chat id, when the chat is
    /// that member's.
    fn session(&self, member: &Member, chat: &str) -> Result<String, Error> {
        let not_found = || Error::NotFound("That conversation".to_owned());
        if member.id != PRIMARY {
            return chat
                .strip_prefix(member.id.as_str())
                .and_then(|rest| rest.strip_prefix(SEP))
                .map(str::to_owned)
                .ok_or_else(not_found);
        }
        let another = chat
            .split_once(SEP)
            .is_some_and(|(prefix, _)| self.members().iter().any(|m| m.id != PRIMARY && m.id == prefix));
        if another { Err(not_found()) } else { Ok(chat.to_owned()) }
    }
}

/// A member that doesn't answer reads as its own name, not the bot's.
fn unreachable(member: &Member, error: Error) -> Error {
    match error {
        Error::Unavailable(why) => {
            tracing::info!(agent = %member.id, why, "contract: the agent is not answering");
            Error::Failed(format!("Could not connect to {}. Try again.", member.label))
        }
        other => other,
    }
}

impl Backend for Roster {
    /// Ready when one agent is: the members are asked in order, and the
    /// first that answers ends it, so the others start only when used.
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
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
        })
    }

    /// Every member's agents; a member whose runtime doesn't answer is left
    /// out, unless none answers.
    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        Box::pin(async move {
            let mut all = Vec::new();
            let mut failed = None;
            for member in self.members() {
                match member.backend.agents().await {
                    Ok(agents) => all.extend(agents.into_iter().map(|a| {
                        let id = match (a.is_default, member.id == PRIMARY) {
                            (true, _) => member.id.clone(),
                            (false, true) => a.id,
                            (false, false) => format!("{}.{}", member.id, a.id),
                        };
                        Agent {
                            is_default: id == PRIMARY,
                            id,
                            ..a
                        }
                    })),
                    Err(e) => {
                        failed.get_or_insert(unreachable(&member, e));
                    }
                }
            }
            match failed {
                Some(e) if all.is_empty() => Err(e),
                _ => Ok(all),
            }
        })
    }

    fn chats<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Vec<Chat>, Error>> {
        Box::pin(async move {
            let (member, sub) = self.locate(agent).await?;
            let chats = member.backend.chats(&sub).await.map_err(|e| unreachable(&member, e))?;
            Ok(chats
                .into_iter()
                .map(|c| Chat {
                    id: Self::exposed(&member, &c.id),
                    ..c
                })
                .collect())
        })
    }

    fn create_chat<'a>(&'a self, agent: &'a str) -> BoxFuture<'a, Result<Chat, Error>> {
        Box::pin(async move {
            let (member, sub) = self.locate(agent).await?;
            let chat = member.backend.create_chat(&sub).await.map_err(|e| unreachable(&member, e))?;
            Ok(Chat {
                id: Self::exposed(&member, &chat.id),
                ..chat
            })
        })
    }

    fn messages<'a>(&'a self, agent: &'a str, chat: &'a str) -> BoxFuture<'a, Result<Vec<Message>, Error>> {
        Box::pin(async move {
            let (member, sub) = self.locate(agent).await?;
            let session = self.session(&member, chat)?;
            member.backend.messages(&sub, &session).await.map_err(|e| unreachable(&member, e))
        })
    }

    fn model<'a>(&'a self, agent: &'a str, chat: Option<&'a str>) -> BoxFuture<'a, Result<String, Error>> {
        Box::pin(async move {
            let (member, sub) = self.locate(agent).await?;
            let session = chat.map(|c| self.session(&member, c)).transpose()?;
            member
                .backend
                .model(&sub, session.as_deref())
                .await
                .map_err(|e| unreachable(&member, e))
        })
    }

    fn turn<'a>(
        &'a self,
        agent: &'a str,
        chat: &'a str,
        prompt: String,
        permission: Option<Permission>,
    ) -> BoxFuture<'a, Result<Turn, Error>> {
        Box::pin(async move {
            let (member, sub) = self.locate(agent).await?;
            let session = self.session(&member, chat)?;
            member
                .backend
                .turn(&sub, &session, prompt, permission)
                .await
                .map_err(|e| unreachable(&member, e))
        })
    }
}
