//! The host core against an in-memory runtime: a turn from prompt to end,
//! a permission request answered (first answer wins), one turn per session,
//! cancel, and what the host says about its agents.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{Fake, member};
use link_core::backend::{Permission, StopReason};
use link_core::host::{Event, Host, Update};
use link_core::model::{AgentChange, Outcome, PendingChange, TurnState, code};
use link_core::roster::Roster;
use tokio::sync::broadcast;

async fn next(events: &mut broadcast::Receiver<Event>) -> Event {
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .expect("an event in time")
        .expect("the host is up")
}

/// Events until the next permission request is added.
async fn until_pending(
    events: &mut broadcast::Receiver<Event>,
) -> link_core::model::PendingRequest {
    loop {
        if let Event::Pending(update) = next(events).await
            && update.change == PendingChange::Added
        {
            return update.request;
        }
    }
}

/// Events until the turn ends.
async fn until_ended(
    events: &mut broadcast::Receiver<Event>,
) -> (Vec<Event>, link_core::model::TurnUpdate) {
    let mut seen = Vec::new();
    loop {
        match next(events).await {
            Event::Turn(turn) if turn.state == TurnState::Ended => return (seen, turn),
            other => seen.push(other),
        }
    }
}

#[tokio::test]
async fn a_turn_runs_asks_is_answered_and_ends() {
    let fake = Arc::new(Fake::default());
    let host = Host::new(Arc::new(Roster::new(vec![member(
        link_core::PRIMARY,
        "Claude Code",
        fake.clone(),
    )])));
    let mut events = host.subscribe();

    let running = host
        .prompt(
            "assistant",
            "s1",
            "hello".into(),
            Some(Permission::Automatic),
            None,
        )
        .unwrap();
    assert_eq!(running.state, TurnState::Running);
    match next(&mut events).await {
        Event::Turn(turn) => assert_eq!(turn, running),
        other => panic!("expected the turn to be announced first, got {other:?}"),
    }
    assert_eq!(
        host.turn("s1").map(|t| t.turn_id),
        Some(running.turn_id.clone())
    );

    // One turn per session.
    let refused = host
        .prompt("assistant", "s1", "again".into(), None, None)
        .unwrap_err();
    assert_eq!(refused.code, code::TURN_IN_PROGRESS);

    let request = until_pending(&mut events).await;
    assert_eq!(
        request.id, "call_1",
        "the runtime's own id, when it gives one"
    );
    assert_eq!(request.agent, "assistant");
    assert_eq!(request.session_id, "s1");
    assert_eq!(request.turn_id.as_deref(), Some(running.turn_id.as_str()));
    assert_eq!(request.tool_call.title.as_deref(), Some("ls"));
    assert_eq!(host.pending(), vec![request.clone()]);

    // An option the request doesn't offer is refused; one it does is taken.
    assert_eq!(
        host.answer("call_1", "maybe", None).unwrap_err().code,
        code::INVALID_PARAMS
    );
    assert_eq!(
        host.answer("nope", "allow", None).unwrap_err().code,
        code::UNKNOWN_REQUEST
    );
    host.answer("call_1", "allow", None).unwrap();
    assert_eq!(
        host.answer("call_1", "allow", None).unwrap_err().code,
        code::ALREADY_ANSWERED
    );
    assert!(host.pending().is_empty());

    let (seen, ended) = until_ended(&mut events).await;
    assert!(seen.iter().any(|e| matches!(
        e,
        Event::Pending(u) if u.change == PendingChange::Resolved
            && u.outcome == Some(Outcome::Selected { option_id: "allow".into() })
    )));
    assert!(
        seen.iter()
            .any(|e| matches!(e, Event::Update(u) if u.update == Update::Text("Done.".into())))
    );
    assert_eq!(ended.turn_id, running.turn_id);
    assert_eq!(ended.stop_reason, Some(StopReason::EndTurn));
    assert_eq!(
        ended
            .usage
            .as_ref()
            .map(|u| (u.input_tokens, u.output_tokens)),
        Some((10, 3))
    );
    assert_eq!(host.turn("s1"), None);
    assert_eq!(
        *fake.answers.lock().unwrap(),
        vec![(Some("call_1".to_owned()), "allow".to_owned())]
    );
    assert_eq!(
        *fake.permissions.lock().unwrap(),
        vec![Some(Permission::Automatic)]
    );

    // The session is free again.
    host.prompt("assistant", "s1", "more".into(), None, None)
        .unwrap();
}

#[tokio::test]
async fn a_cancelled_turn_resolves_its_question_as_cancelled() {
    let host = Host::new(Arc::new(Roster::new(vec![member(
        link_core::PRIMARY,
        "Claude Code",
        Arc::default(),
    )])));
    let mut events = host.subscribe();
    assert_eq!(host.cancel(None, None), 0, "nothing running");
    host.prompt("assistant", "s1", "hello".into(), None, None)
        .unwrap();
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
async fn a_turn_for_an_unknown_agent_ends_with_its_error() {
    let host = Host::new(Arc::new(Roster::new(vec![member(
        link_core::PRIMARY,
        "Claude Code",
        Arc::default(),
    )])));
    let mut events = host.subscribe();
    host.prompt("nobody", "s1", "hello".into(), None, None)
        .unwrap();
    let (_, ended) = until_ended(&mut events).await;
    let error = ended.error.expect("an error");
    assert_eq!(error.code, code::UNKNOWN_AGENT);
    assert_eq!(error.message, "The agent nobody was not found.");
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
    assert_eq!(
        agents[1].offline_reason.as_deref(),
        Some("Could not connect to Codex. Try again.")
    );

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
    assert_eq!(
        changes,
        vec![
            (AgentChange::Removed, "codex".into()),
            (AgentChange::Added, "site".into())
        ]
    );
}
