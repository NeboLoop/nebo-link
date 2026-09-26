//! The host core against an in-memory runtime adapted into ACP: a session
//! opened, a turn from prompt to end, a permission request answered (first
//! answer wins), one turn per session, cancel, the record a client attaching
//! later gets, and what the host says about its agents.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Fake, member};
use link_core::backend::StopReason;
use link_core::host::{Event, Host, Open, Stamped};
use link_core::model::{AgentChange, Outcome, PendingChange, PendingRequest, TurnState, TurnUpdate, code};
use link_core::roster::Roster;
use serde_json::json;
use tokio::sync::broadcast;

async fn next(events: &mut broadcast::Receiver<Stamped>) -> Event {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("an event in time")
        .expect("the host is up")
        .event
}

/// Events until the next permission request is added.
async fn until_pending(events: &mut broadcast::Receiver<Stamped>) -> PendingRequest {
    loop {
        if let Event::Pending(update) = next(events).await
            && update.change == PendingChange::Added
        {
            return update.request;
        }
    }
}

/// Events until the turn ends.
async fn until_ended(events: &mut broadcast::Receiver<Stamped>) -> (Vec<Event>, TurnUpdate) {
    let mut seen = Vec::new();
    loop {
        match next(events).await {
            Event::Turn(turn) if turn.state == TurnState::Ended => return (seen, turn),
            other => seen.push(other),
        }
    }
}

fn text(t: &str) -> Vec<serde_json::Value> {
    vec![json!({ "type": "text", "text": t })]
}

fn host_of(fake: Arc<Fake>) -> Arc<Host> {
    Host::new(Arc::new(Roster::new(vec![member(link_core::PRIMARY, "Claude Code", fake)])))
}

#[tokio::test]
async fn a_turn_runs_asks_is_answered_and_ends() {
    let fake = Arc::new(Fake::default());
    let host = host_of(fake.clone());
    let mut events = host.subscribe();

    // A prompt needs its session open first.
    assert_eq!(host.prompt("assistant", "s1", text("hi"), None, None).unwrap_err().code, code::NOT_FOUND);
    let created = host.new_session("assistant", json!({ "cwd": "/work", "mcpServers": [] })).await.unwrap();
    assert_eq!(created["sessionId"], "s1");
    assert_eq!(created["models"]["currentModelId"], "fake-model");

    let client = host.client();
    let running = host.prompt("assistant", "s1", text("hello"), None, Some(client)).unwrap();
    assert_eq!(running.state, TurnState::Running);
    match next(&mut events).await {
        Event::Turn(turn) => assert_eq!(turn, running),
        other => panic!("expected the turn to be announced first, got {other:?}"),
    }
    match next(&mut events).await {
        Event::Update(u) => {
            assert_eq!(u.update, json!({ "sessionUpdate": "user_message_chunk", "content": { "type": "text", "text": "hello" } }));
            assert_eq!(u.skip, Some(client), "the client that sent it already has it");
        }
        other => panic!("expected the prompt's echo, got {other:?}"),
    }
    assert_eq!(host.turn("assistant", "s1").map(|t| t.turn_id), Some(running.turn_id.clone()));

    // One turn per session.
    let refused = host.prompt("assistant", "s1", text("again"), None, None).unwrap_err();
    assert_eq!(refused.code, code::TURN_IN_PROGRESS);

    let request = until_pending(&mut events).await;
    assert_eq!(request.id, "call_1", "the tool call's id, when no other request has it");
    assert_eq!(request.agent, "assistant");
    assert_eq!(request.session_id, "s1");
    assert_eq!(request.turn_id.as_deref(), Some(running.turn_id.as_str()));
    assert_eq!(request.tool_call.title.as_deref(), Some("ls"));
    assert_eq!(request.params["toolCall"]["toolCallId"], "call_1", "the agent's params, unchanged");
    assert_eq!(request.words.labels, ["Allow once", "Deny"], "the runtime's own words");
    assert_eq!(host.pending(), vec![request.clone()]);

    // An option the request doesn't offer is refused; one it does is taken.
    let selected = |id: &str| Outcome::Selected { option_id: id.into() };
    assert_eq!(host.answer("call_1", selected("maybe"), None).unwrap_err().code, code::INVALID_PARAMS);
    assert_eq!(host.answer("nope", selected("allow"), None).unwrap_err().code, code::UNKNOWN_REQUEST);
    host.answer("call_1", selected("allow"), None).unwrap();
    assert_eq!(host.answer("call_1", selected("allow"), None).unwrap_err().code, code::ALREADY_ANSWERED);
    assert!(host.pending().is_empty());

    let (seen, ended) = until_ended(&mut events).await;
    assert!(seen.iter().any(|e| matches!(
        e,
        Event::Pending(u) if u.change == PendingChange::Resolved && u.outcome == Some(selected("allow"))
    )));
    assert!(seen.iter().any(|e| matches!(e, Event::Update(u)
        if u.update["sessionUpdate"] == "agent_message_chunk" && u.update["content"]["text"] == "Done.")));
    match seen.last() {
        Some(Event::Answered(answered)) => {
            assert_eq!(answered.client, Some(client));
            assert_eq!(answered.response.as_ref().unwrap()["stopReason"], "end_turn");
        }
        other => panic!("the answer comes just before the end, got {other:?}"),
    }
    assert_eq!(ended.turn_id, running.turn_id);
    assert_eq!(ended.stop_reason, Some(StopReason::EndTurn));
    assert_eq!(ended.usage.as_ref().map(|u| (u.input_tokens, u.output_tokens)), Some((10, 3)));
    assert_eq!(host.turn("assistant", "s1"), None);
    assert_eq!(*fake.answers.lock().unwrap(), vec![(Some("call_1".to_owned()), "allow".to_owned())]);
    assert_eq!(*fake.prompts.lock().unwrap(), vec!["hello".to_owned()]);

    // A client attaching now gets the session as the runtime keeps it (an
    // adapted runtime's transcript is its own: this one keeps none), and the
    // last turn's end.
    let opened = host.open_session("assistant", Open::Load, json!({ "sessionId": "s1" })).await.unwrap();
    assert!(opened.record.is_empty(), "read afresh from the runtime: {:?}", opened.record);
    assert_eq!(opened.turn.as_ref().map(|t| t.state), Some(TurnState::Ended));
    assert!(opened.pending.is_empty());
    let resumed = host.open_session("assistant", Open::Resume, json!({ "sessionId": "s1" })).await.unwrap();
    assert!(resumed.record.is_empty(), "a resume sends no record");

    // The session is free again.
    host.prompt("assistant", "s1", text("more"), None, None).unwrap();
}

#[tokio::test]
async fn a_cancelled_turn_resolves_its_question_as_cancelled() {
    let host = host_of(Arc::default());
    let mut events = host.subscribe();
    assert_eq!(host.cancel(None, None), 0, "nothing running");
    host.new_session("assistant", json!({})).await.unwrap();
    host.prompt("assistant", "s1", text("hello"), None, None).unwrap();
    until_pending(&mut events).await;
    assert_eq!(host.cancel(None, Some("s1")), 1);
    let (seen, ended) = until_ended(&mut events).await;
    assert_eq!(ended.stop_reason, Some(StopReason::Cancelled));
    assert!(seen.iter().any(|e| matches!(
        e,
        Event::Pending(u) if u.change == PendingChange::Resolved && u.outcome == Some(Outcome::Cancelled)
    )));
    assert!(host.pending().is_empty());
}

#[tokio::test]
async fn a_session_is_opened_in_its_agent_once_and_then_served_from_the_record() {
    let host = host_of(Arc::default());
    assert_eq!(
        host.new_session("nobody", json!({})).await.unwrap_err().code,
        code::UNKNOWN_AGENT
    );
    // A session the host hasn't opened is opened in its agent.
    let opened = host.open_session("assistant", Open::Load, json!({ "sessionId": "old" })).await.unwrap();
    assert_eq!(opened.response["models"]["currentModelId"], "fake-model", "the agent's own answer");
    assert!(host.is_open("assistant", "old"));
    assert_eq!(opened.turn, None);
}

#[tokio::test]
async fn agents_are_listed_offline_with_the_reason_and_changes_are_announced() {
    let up = Arc::new(Fake::default());
    let down = Arc::new(Fake {
        down: true,
        ..Fake::default()
    });
    let host = Host::new(Arc::new(Roster::new(vec![
        member(link_core::PRIMARY, "Claude Code", up.clone()),
        member("codex", "Codex", down.clone()),
    ])));
    let agents = host.agents().await;
    assert_eq!(agents.len(), 2);
    assert_eq!(agents[0].id, "assistant");
    assert_eq!(agents[0].label, "Fake");
    assert_eq!(agents[0].runtime, "claude-code");
    assert_eq!(agents[0].folder.as_deref(), Some("/work"));
    assert!(agents[0].online);
    assert_eq!(agents[1].id, "codex");
    assert!(!agents[1].online);
    assert_eq!(agents[1].offline_reason.as_deref(), Some("Could not connect to Codex. Try again."));

    let mut events = host.subscribe();
    host.set_members(vec![
        member(link_core::PRIMARY, "Claude Code", up),
        member("site", "Site", Arc::default()),
    ]);
    let mut changes = Vec::new();
    while changes.len() < 2 {
        if let Event::Agent(update) = next(&mut events).await {
            changes.push((update.change, update.agent.id));
        }
    }
    changes.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(changes, vec![(AgentChange::Removed, "codex".into()), (AgentChange::Added, "site".into())]);
}
