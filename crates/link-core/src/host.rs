//! The host core: every agent a computer runs (its [`Roster`]), their
//! sessions, one turn at a time per session, the permission requests those
//! turns stop for, and what clients are told about all of it, in Open Agent
//! Link's types (`spec/oal-0.1.md` §7–§10). No transport: whoever embeds the
//! host (the nebo-link daemon, Nebo itself) carries it to clients.
//!
//! - **Turns.** [`Host::prompt`] starts one on a session and says so with
//!   [`TurnUpdate`] `running`; every turn it accepts ends with exactly one
//!   `ended`, carrying the stop reason and this turn's usage, or the error.
//!   A second prompt while one runs is refused with `turn_in_progress`.
//! - **Permission requests.** A question the runtime stops for becomes a
//!   [`PendingRequest`] on the host-wide list and a [`PendingUpdate`]
//!   `added`. The first answer wins ([`Host::answer`], or the runtime's own
//!   interface); a turn that ends resolves what it left open as `cancelled`.
//! - **Agents.** [`Host::agents`] lists every hosted agent, one whose
//!   runtime doesn't answer included, offline with the reason;
//!   [`Host::set_members`] and [`Host::refresh`] announce what changed with
//!   [`AgentUpdate`].

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc};

use crate::backend::{Backend, Chat, Control, Error, Message, Permission, TurnEvent};
use crate::model::{
    self, Agent, AgentChange, AgentUpdate, DeviceRef, ErrorObject, Outcome, PendingChange,
    PendingRequest, PendingUpdate, StopReason, TurnState, TurnUpdate, code,
};
use crate::roster::{Member, Roster, member_agent};

/// How many resolved request ids are remembered, so a late answer reads
/// `already_answered` rather than `unknown_request`.
const RESOLVED_REMEMBERED: usize = 256;

/// What the host tells its clients, in order.
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    /// `host/turn`: a turn started or ended.
    Turn(TurnUpdate),
    /// What a running turn produced.
    Update(SessionUpdate),
    /// `host/pending_update`: a permission request was added or resolved.
    Pending(Box<PendingUpdate>),
    /// `host/agent_update`: an agent was added, changed or removed.
    Agent(AgentUpdate),
}

/// Something a running turn produced, on its session.
#[derive(Debug, Clone, PartialEq)]
pub struct SessionUpdate {
    pub agent: String,
    pub session_id: String,
    pub turn_id: String,
    pub update: Update,
}

/// A turn's output as it streams.
#[derive(Debug, Clone, PartialEq)]
pub enum Update {
    /// Reply text (ACP `agent_message_chunk`).
    Text(String),
    /// Thinking, or a plan as steps (ACP `agent_thought_chunk`, `plan`).
    Thinking(String),
    /// A tool call, once the runtime says what it is (ACP `tool_call`).
    ToolStart {
        id: String,
        name: String,
        input: Value,
    },
    /// A tool call finished (ACP `tool_call_update` `completed`/`failed`).
    ToolResult {
        id: String,
        name: String,
        result: String,
        is_error: bool,
        duration_ms: Option<u64>,
    },
}

/// Every agent on the computer, and their turns.
pub struct Host {
    roster: Arc<Roster>,
    events: broadcast::Sender<Event>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// The turn running on each session.
    turns: HashMap<String, Running>,
    /// Every pending permission request, oldest first.
    pending: Vec<PendingRequest>,
    /// Requests resolved lately, newest last.
    resolved: VecDeque<String>,
    /// The agents as last announced; `None` until first listed.
    known: Option<Vec<Agent>>,
}

struct Running {
    turn: TurnUpdate,
    /// `None` while the runtime is still starting the turn.
    control: Option<mpsc::Sender<Control>>,
    /// The runtime's own id of each of the turn's pending requests, by the
    /// host's id.
    asks: Vec<(String, Option<String>)>,
}

impl Host {
    pub fn new(roster: Arc<Roster>) -> Arc<Self> {
        let (events, _) = broadcast::channel(1024);
        Arc::new(Self {
            roster,
            events,
            state: Mutex::new(State::default()),
        })
    }

    pub fn roster(&self) -> &Arc<Roster> {
        &self.roster
    }

    /// Every event from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    fn emit(&self, event: Event) {
        let _ = self.events.send(event);
    }

    /// Whether any agent can take a prompt now; the error says why not.
    pub async fn ready(&self) -> Result<(), String> {
        self.roster.ready().await
    }

    // -- Agents ---------------------------------------------------------------

    /// Every hosted agent (`host/agents`): one whose runtime doesn't answer
    /// is listed offline, with the reason.
    pub async fn agents(&self) -> Vec<Agent> {
        let now = self.current().await;
        let mut state = self.state.lock().expect("host state");
        state.known.get_or_insert_with(|| now.clone());
        now
    }

    async fn current(&self) -> Vec<Agent> {
        let mut all = Vec::new();
        for member in self.roster.members() {
            match member.backend.agents().await {
                Ok(agents) => all.extend(agents.into_iter().map(|a| {
                    let a = member_agent(&member, a);
                    Agent {
                        online: a.offline_reason.is_none(),
                        id: a.id,
                        label: a.name,
                        runtime: member.runtime.clone(),
                        folder: a.folder,
                        offline_reason: a.offline_reason,
                        capabilities: a.capabilities,
                        modes: a.modes,
                    }
                })),
                Err(e) => all.push(Agent {
                    id: member.id.clone(),
                    label: member.label.clone(),
                    runtime: member.runtime.clone(),
                    folder: None,
                    online: false,
                    offline_reason: Some(match e {
                        Error::Unavailable(_) => {
                            format!("Could not connect to {}. Try again.", member.label)
                        }
                        other => other.message(&member.label),
                    }),
                    capabilities: json!({}),
                    modes: None,
                }),
            }
        }
        all
    }

    /// Replaces the hosted agents: one that stays keeps its backend (its
    /// running process and sessions). What changed is announced.
    pub fn set_members(self: &Arc<Self>, members: Vec<Member>) {
        self.roster.set(members);
        let host = self.clone();
        tokio::spawn(async move { host.refresh().await });
    }

    /// Lists the agents again and announces each that was added, changed
    /// (came online, went offline, was renamed) or removed since last time.
    pub async fn refresh(&self) {
        // Nothing was announced yet, so nothing can have changed for anyone.
        if self.state.lock().expect("host state").known.is_none() {
            return;
        }
        let now = self.current().await;
        let updates = {
            let mut state = self.state.lock().expect("host state");
            let Some(before) = state.known.replace(now.clone()) else {
                return;
            };
            let mut updates = Vec::new();
            for agent in &now {
                match before.iter().find(|b| b.id == agent.id) {
                    None => updates.push((AgentChange::Added, agent.clone())),
                    Some(b) if b != agent => updates.push((AgentChange::Updated, agent.clone())),
                    Some(_) => {}
                }
            }
            for gone in before
                .into_iter()
                .filter(|b| !now.iter().any(|a| a.id == b.id))
            {
                updates.push((AgentChange::Removed, gone));
            }
            updates
        };
        for (change, agent) in updates {
            self.emit(Event::Agent(AgentUpdate { change, agent }));
        }
    }

    // -- Sessions -------------------------------------------------------------

    /// The agent's sessions, most recent first.
    pub async fn sessions(&self, agent: &str) -> Result<Vec<Chat>, Error> {
        self.roster.chats(agent).await
    }

    pub async fn new_session(&self, agent: &str) -> Result<Chat, Error> {
        self.roster.create_chat(agent).await
    }

    /// The session's transcript, oldest first.
    pub async fn transcript(&self, agent: &str, session: &str) -> Result<Vec<Message>, Error> {
        self.roster.messages(agent, session).await
    }

    /// The model the session runs on, or the agent's current one.
    pub async fn model(&self, agent: &str, session: Option<&str>) -> Result<String, Error> {
        self.roster.model(agent, session).await
    }

    // -- Turns ----------------------------------------------------------------

    /// The turn running on `session`, if any.
    pub fn turn(&self, session: &str) -> Option<TurnUpdate> {
        let state = self.state.lock().expect("host state");
        state.turns.get(session).map(|r| r.turn.clone())
    }

    /// Every running turn.
    pub fn turns(&self) -> Vec<TurnUpdate> {
        let state = self.state.lock().expect("host state");
        state.turns.values().map(|r| r.turn.clone()).collect()
    }

    /// Sends `text` to `agent` on `session` and starts the turn, in the
    /// agent's mode for `permission` when one is given. The turn is
    /// announced `running` now and `ended` when it ends, however it ends;
    /// the only refusal is `turn_in_progress`.
    pub fn prompt(
        self: &Arc<Self>,
        agent: &str,
        session: &str,
        text: String,
        permission: Option<Permission>,
        by: Option<DeviceRef>,
    ) -> Result<TurnUpdate, ErrorObject> {
        let turn = TurnUpdate {
            agent: agent.to_owned(),
            session_id: session.to_owned(),
            turn_id: uuid::Uuid::new_v4().to_string(),
            state: TurnState::Running,
            started_at: model::now(),
            by,
            stop_reason: None,
            error: None,
            usage: None,
        };
        {
            let mut state = self.state.lock().expect("host state");
            if state.turns.contains_key(session) {
                return Err(ErrorObject {
                    code: code::TURN_IN_PROGRESS,
                    message: format!(
                        "{} is still working on the last message. Wait for it or stop it.",
                        self.label(agent)
                    ),
                });
            }
            state.turns.insert(
                session.to_owned(),
                Running {
                    turn: turn.clone(),
                    control: None,
                    asks: Vec::new(),
                },
            );
        }
        self.emit(Event::Turn(turn.clone()));
        let host = self.clone();
        let started = turn.clone();
        tokio::spawn(async move { host.run(started, text, permission).await });
        Ok(turn)
    }

    /// Runs one accepted turn to its end.
    async fn run(self: Arc<Self>, turn: TurnUpdate, text: String, permission: Option<Permission>) {
        let end = match self
            .roster
            .turn(&turn.agent, &turn.session_id, text, permission)
            .await
        {
            Ok(started) => {
                {
                    let mut state = self.state.lock().expect("host state");
                    if let Some(running) = state.turns.get_mut(&turn.session_id) {
                        running.control = Some(started.control);
                    }
                }
                self.relay(started.events, &turn).await
            }
            Err(e) => Ended::Error(ErrorObject {
                code: match e {
                    Error::NotFound(_) => code::UNKNOWN_AGENT,
                    Error::Unavailable(_) => code::AGENT_UNAVAILABLE,
                    Error::Failed(_) => code::TURN_FAILED,
                },
                message: e.message(&self.label(&turn.agent)),
            }),
        };
        self.end(turn, end);
    }

    /// Announces a turn's events until it ends, and how it ended.
    async fn relay(&self, mut events: mpsc::Receiver<TurnEvent>, turn: &TurnUpdate) -> Ended {
        let update = |update: Update| {
            Event::Update(SessionUpdate {
                agent: turn.agent.clone(),
                session_id: turn.session_id.clone(),
                turn_id: turn.turn_id.clone(),
                update,
            })
        };
        while let Some(event) = events.recv().await {
            match event {
                TurnEvent::Text(text) => self.emit(update(Update::Text(text))),
                TurnEvent::Thinking(text) => self.emit(update(Update::Thinking(text))),
                TurnEvent::ToolStart { id, name, input } => {
                    self.emit(update(Update::ToolStart { id, name, input }))
                }
                TurnEvent::ToolResult {
                    id,
                    name,
                    result,
                    is_error,
                    duration_ms,
                } => self.emit(update(Update::ToolResult {
                    id,
                    name,
                    result,
                    is_error,
                    duration_ms,
                })),
                TurnEvent::Ask(ask) => self.ask(turn, *ask),
                TurnEvent::AskAnswered { request_id } => {
                    let taken = {
                        let mut state = self.state.lock().expect("host state");
                        match state.turns.get_mut(&turn.session_id) {
                            Some(running) => take_ask(&mut running.asks, request_id.as_deref()),
                            None => None,
                        }
                    };
                    if let Some(id) = taken {
                        self.resolve(&id, None, None);
                    }
                }
                TurnEvent::Completed { stop_reason, usage } => {
                    return Ended::Stopped(stop_reason, usage);
                }
                TurnEvent::Failed(message) => {
                    return Ended::Error(ErrorObject {
                        code: code::TURN_FAILED,
                        message,
                    });
                }
                TurnEvent::Cancelled => return Ended::Stopped(StopReason::Cancelled, None),
            }
        }
        // The runtime stopped without saying how.
        Ended::Error(ErrorObject {
            code: code::AGENT_UNAVAILABLE,
            message: format!(
                "Could not connect to {}. Try again.",
                self.label(&turn.agent)
            ),
        })
    }

    /// Frees the turn's session, resolves what it left pending, and
    /// announces its end.
    fn end(&self, turn: TurnUpdate, how: Ended) {
        let left = {
            let mut state = self.state.lock().expect("host state");
            state
                .turns
                .remove(&turn.session_id)
                .map(|r| r.asks.into_iter().map(|(id, _)| id).collect::<Vec<_>>())
                .unwrap_or_default()
        };
        for id in left {
            self.resolve(&id, Some(Outcome::Cancelled), None);
        }
        let (stop_reason, usage, error) = match how {
            Ended::Stopped(reason, usage) => (Some(reason), usage, None),
            Ended::Error(error) => (None, None, Some(error)),
        };
        self.emit(Event::Turn(TurnUpdate {
            state: TurnState::Ended,
            stop_reason,
            usage,
            error,
            ..turn
        }));
    }

    /// Stops the running turns on `session`, or every turn of `agent`, or
    /// every turn. Returns how many were told to stop; a turn still starting
    /// is not among them.
    pub fn cancel(&self, agent: Option<&str>, session: Option<&str>) -> usize {
        let controls: Vec<mpsc::Sender<Control>> = {
            let state = self.state.lock().expect("host state");
            state
                .turns
                .iter()
                .filter(|(id, running)| match (session, agent) {
                    (Some(session), _) => id.as_str() == session,
                    (None, Some(agent)) => running.turn.agent == agent,
                    (None, None) => true,
                })
                .filter_map(|(_, running)| running.control.clone())
                .collect()
        };
        let count = controls.len();
        for control in controls {
            tokio::spawn(async move {
                let _ = control.send(Control::Cancel).await;
            });
        }
        count
    }

    // -- Permission requests --------------------------------------------------

    /// Every pending permission request, oldest first (`host/pending`).
    pub fn pending(&self) -> Vec<PendingRequest> {
        self.state.lock().expect("host state").pending.clone()
    }

    /// A question the runtime stopped for: pending now, and announced.
    fn ask(&self, turn: &TurnUpdate, ask: crate::backend::Ask) {
        let request = {
            let mut state = self.state.lock().expect("host state");
            let Some(running) = state.turns.get(&turn.session_id) else {
                return;
            };
            // The runtime's id, unless another pending request already has it
            // (two agents numbering their tool calls alike): the answer must
            // reach the one that asked.
            let mut id = match &ask.request_id {
                Some(id) => id.clone(),
                None => format!("{}-ask-{}", turn.turn_id, running.asks.len() + 1),
            };
            while state.pending.iter().any(|p| p.id == id) {
                id.push('+');
            }
            let request = PendingRequest {
                id: id.clone(),
                agent: turn.agent.clone(),
                session_id: turn.session_id.clone(),
                turn_id: Some(turn.turn_id.clone()),
                tool_call: ask.tool_call,
                options: ask.options,
                created_at: model::now(),
                words: ask.words,
            };
            if let Some(running) = state.turns.get_mut(&turn.session_id) {
                running.asks.push((id, ask.request_id));
            }
            state.pending.push(request.clone());
            request
        };
        self.emit(Event::Pending(Box::new(PendingUpdate {
            change: PendingChange::Added,
            request,
            outcome: None,
            answered_by: None,
        })));
    }

    /// Answers the pending request `id` with `option_id`, one of its
    /// options (`host/answer`); `by` is the device that answered.
    pub fn answer(
        &self,
        id: &str,
        option_id: &str,
        by: Option<DeviceRef>,
    ) -> Result<(), ErrorObject> {
        let (control, backend_id) = {
            let mut state = self.state.lock().expect("host state");
            let Some(request) = state.pending.iter().find(|p| p.id == id) else {
                let answered = state.resolved.iter().any(|r| r == id);
                return Err(if answered {
                    ErrorObject {
                        code: code::ALREADY_ANSWERED,
                        message: "This was already answered.".to_owned(),
                    }
                } else {
                    ErrorObject {
                        code: code::UNKNOWN_REQUEST,
                        message: "That request is no longer waiting.".to_owned(),
                    }
                });
            };
            if !request.options.iter().any(|o| o.option_id == option_id) {
                return Err(ErrorObject {
                    code: code::INVALID_PARAMS,
                    message: "That isn't one of the answers this request offers.".to_owned(),
                });
            }
            let session = request.session_id.clone();
            let Some(running) = state.turns.get_mut(&session) else {
                return Err(ErrorObject {
                    code: code::UNKNOWN_REQUEST,
                    message: "That request is no longer waiting.".to_owned(),
                });
            };
            let Some(control) = running.control.clone() else {
                return Err(ErrorObject {
                    code: code::UNKNOWN_REQUEST,
                    message: "That request is no longer waiting.".to_owned(),
                });
            };
            let backend_id = match running.asks.iter().position(|(ask, _)| ask == id) {
                Some(i) => running.asks.remove(i).1,
                None => None,
            };
            (control, backend_id)
        };
        let choice = option_id.to_owned();
        tokio::spawn(async move {
            let _ = control
                .send(Control::Answer {
                    request_id: backend_id,
                    choice,
                })
                .await;
        });
        self.resolve(
            id,
            Some(Outcome::Selected {
                option_id: option_id.to_owned(),
            }),
            by,
        );
        Ok(())
    }

    /// Takes `id` off the pending list and announces how it was resolved.
    fn resolve(&self, id: &str, outcome: Option<Outcome>, by: Option<DeviceRef>) {
        let request = {
            let mut state = self.state.lock().expect("host state");
            let Some(i) = state.pending.iter().position(|p| p.id == id) else {
                return;
            };
            let request = state.pending.remove(i);
            state.resolved.push_back(request.id.clone());
            if state.resolved.len() > RESOLVED_REMEMBERED {
                state.resolved.pop_front();
            }
            request
        };
        self.emit(Event::Pending(Box::new(PendingUpdate {
            change: PendingChange::Resolved,
            request,
            outcome,
            answered_by: by,
        })));
    }

    /// The name of the member hosting `agent`, for the owner's messages.
    fn label(&self, agent: &str) -> String {
        let members = self.roster.members();
        members
            .iter()
            .find(|m| m.id == agent)
            .or_else(|| {
                members.iter().find(|m| {
                    agent
                        .strip_prefix(m.id.as_str())
                        .is_some_and(|rest| rest.starts_with('.'))
                })
            })
            .or_else(|| members.iter().find(|m| m.id == crate::PRIMARY))
            .map(|m| m.label.clone())
            .unwrap_or_else(|| agent.to_owned())
    }
}

/// How a turn ended.
enum Ended {
    /// With an answer (cancelled included).
    Stopped(StopReason, Option<model::Usage>),
    Error(ErrorObject),
}

/// The host's id of the ask `request_id` names: that one, or the oldest
/// when the runtime gave no id.
fn take_ask(asks: &mut Vec<(String, Option<String>)>, request_id: Option<&str>) -> Option<String> {
    let i = match request_id {
        Some(id) => asks
            .iter()
            .position(|(_, backend)| backend.as_deref() == Some(id)),
        None => (!asks.is_empty()).then_some(0),
    };
    i.map(|i| asks.remove(i).0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn asks_are_taken_by_the_runtimes_id_or_oldest_first() {
        let mut asks = vec![
            ("a".to_owned(), None),
            ("b".to_owned(), Some("b".to_owned())),
        ];
        assert_eq!(take_ask(&mut asks, Some("b")), Some("b".to_owned()));
        assert_eq!(take_ask(&mut asks, Some("zz")), None);
        assert_eq!(take_ask(&mut asks, None), Some("a".to_owned()));
        assert_eq!(take_ask(&mut asks, None), None);
    }
}
