//! A runtime in memory, for the host's tests.

use std::sync::{Arc, Mutex};

use link_core::adapter::{Adapted, Ask, Chat, Control, Message, Runtime, Turn, TurnEvent};
use link_core::backend::{
    Agent, BoxFuture, Error, PermissionOption, StopReason, ToolCallUpdate, Usage, Words,
};
use link_core::roster::Member;
use serde_json::json;
use tokio::sync::mpsc;

/// An answer a turn got: the request's id and the option chosen.
pub type Answer = (Option<String>, String);

/// A runtime whose every turn replies, asks to run `ls`, and finishes once
/// answered; `down` makes it unreachable.
#[derive(Default)]
pub struct Fake {
    pub down: bool,
    /// The prompt each turn was sent.
    pub prompts: Mutex<Vec<String>>,
    /// What each turn was answered with.
    pub answers: Arc<Mutex<Vec<Answer>>>,
    /// The runtime's other agents, by their runtime ids (an OpenClaw's
    /// agents, Hermes profiles).
    pub others: Vec<&'static str>,
}

impl Runtime for Fake {
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        let down = self.down;
        Box::pin(async move { if down { Err("down".into()) } else { Ok(()) } })
    }

    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        let down = self.down;
        let others = self.others.clone();
        Box::pin(async move {
            if down {
                return Err(Error::Unavailable("not answering".into()));
            }
            let agent = |id: &str, is_default: bool| Agent {
                id: id.into(),
                name: "Fake".into(),
                description: String::new(),
                is_default,
                folder: Some("/work".into()),
                capabilities: json!({ "loadSession": true }),
                modes: None,
                offline_reason: None,
            };
            Ok(std::iter::once(agent("fake", true))
                .chain(others.iter().map(|id| agent(id, false)))
                .collect())
        })
    }

    fn chats<'a>(&'a self, _agent: &'a str) -> BoxFuture<'a, Result<Vec<Chat>, Error>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn create_chat<'a>(&'a self, _agent: &'a str) -> BoxFuture<'a, Result<Chat, Error>> {
        Box::pin(async {
            Ok(Chat {
                id: "s1".into(),
                title: String::new(),
                preview: String::new(),
                last_active: None,
                message_count: 0,
            })
        })
    }

    fn messages<'a>(
        &'a self,
        _agent: &'a str,
        _chat: &'a str,
    ) -> BoxFuture<'a, Result<Vec<Message>, Error>> {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn model<'a>(
        &'a self,
        _agent: &'a str,
        _chat: Option<&'a str>,
    ) -> BoxFuture<'a, Result<String, Error>> {
        Box::pin(async { Ok("fake-model".into()) })
    }

    fn turn<'a>(
        &'a self,
        _agent: &'a str,
        _chat: &'a str,
        prompt: String,
    ) -> BoxFuture<'a, Result<Turn, Error>> {
        self.prompts.lock().unwrap().push(prompt.clone());
        let answers = self.answers.clone();
        Box::pin(async move {
            let (events, events_rx) = mpsc::channel(16);
            let (control, mut control_rx) = mpsc::channel(4);
            tokio::spawn(async move {
                let _ = events
                    .send(TurnEvent::Text(format!("You said {prompt}.")))
                    .await;
                let _ = events
                    .send(TurnEvent::Ask(Box::new(Ask {
                        request_id: Some("call_1".into()),
                        tool_call: ToolCallUpdate {
                            tool_call_id: "call_1".into(),
                            title: Some("ls".into()),
                            kind: Some("execute".into()),
                            ..ToolCallUpdate::default()
                        },
                        options: vec![
                            PermissionOption {
                                option_id: "allow".into(),
                                name: "Allow".into(),
                                kind: "allow_once".into(),
                            },
                            PermissionOption {
                                option_id: "reject".into(),
                                name: "Reject".into(),
                                kind: "reject_once".into(),
                            },
                        ],
                        words: Words {
                            question: "ls".into(),
                            summary: "run `ls`".into(),
                            labels: vec!["Allow once".into(), "Deny".into()],
                        },
                    })))
                    .await;
                match control_rx.recv().await {
                    Some(Control::Answer { request_id, choice }) => {
                        answers.lock().unwrap().push((request_id, choice));
                        let _ = events.send(TurnEvent::Text("Done.".into())).await;
                        let _ = events
                            .send(TurnEvent::Completed {
                                stop_reason: StopReason::EndTurn,
                                usage: Some(Usage {
                                    input_tokens: 10,
                                    output_tokens: 3,
                                    ..Usage::default()
                                }),
                            })
                            .await;
                    }
                    Some(Control::Cancel) => {
                        let _ = events.send(TurnEvent::Cancelled).await;
                    }
                    None => {}
                }
            });
            Ok(Turn {
                events: events_rx,
                control,
            })
        })
    }
}

pub fn member(id: &str, label: &str, fake: Arc<Fake>) -> Member {
    Member {
        id: id.into(),
        label: label.into(),
        runtime: "claude-code".into(),
        backend: Arc::new(Adapted::new(label, fake)),
    }
}
