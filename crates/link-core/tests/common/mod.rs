//! A runtime in memory, for the host's tests.

use std::sync::{Arc, Mutex};

use link_core::backend::{
    Agent, Ask, Backend, BoxFuture, Chat, Control, Error, Message, Permission, PermissionOption,
    StopReason, ToolCallUpdate, Turn, TurnEvent, Usage, Words,
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
    /// The permission each turn was sent with.
    pub permissions: Mutex<Vec<Option<Permission>>>,
    /// What each turn was answered with.
    pub answers: Arc<Mutex<Vec<Answer>>>,
}

impl Backend for Fake {
    fn ready(&self) -> BoxFuture<'_, Result<(), String>> {
        let down = self.down;
        Box::pin(async move { if down { Err("down".into()) } else { Ok(()) } })
    }

    fn agents(&self) -> BoxFuture<'_, Result<Vec<Agent>, Error>> {
        let down = self.down;
        Box::pin(async move {
            if down {
                return Err(Error::Unavailable("not answering".into()));
            }
            Ok(vec![Agent {
                id: "fake".into(),
                name: "Fake".into(),
                description: String::new(),
                is_default: true,
                folder: Some("/work".into()),
                capabilities: json!({ "loadSession": true }),
                modes: None,
                offline_reason: None,
            }])
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
        permission: Option<Permission>,
    ) -> BoxFuture<'a, Result<Turn, Error>> {
        self.permissions.lock().unwrap().push(permission);
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
        backend: fake,
    }
}
