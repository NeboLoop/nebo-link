//! The phone contract with no transport: frames in, frames out, and the
//! owner's inbox told about the question and its answer.

mod common;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::{Fake, member};
use link_core::host::Host;
use link_core::phone::{Contract, Inbox, InboxItem, Outbound};
use link_core::roster::Roster;
use serde_json::{Value, json};
use tokio::sync::broadcast;

#[derive(Default)]
struct Recorded(Mutex<Vec<InboxItem>>);

impl Inbox for Recorded {
    fn post(&self, item: InboxItem) {
        self.0.lock().unwrap().push(item);
    }
}

async fn until(frames: &mut broadcast::Receiver<Outbound>, kind: &str) -> Vec<Outbound> {
    let mut seen = Vec::new();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(5), frames.recv())
            .await
            .unwrap()
            .unwrap();
        let done = frame.kind == kind;
        seen.push(frame);
        if done {
            return seen;
        }
    }
}

#[tokio::test]
async fn a_chat_frame_asks_is_answered_and_completes() {
    let fake = Arc::new(Fake::default());
    let host = Host::new(Arc::new(Roster::new(vec![member(
        link_core::PRIMARY,
        "Claude Code",
        fake.clone(),
    )])));
    let inbox = Arc::new(Recorded::default());
    let contract = Contract::new("claude-code", "Claude Code", host, Some(inbox.clone()));
    let mut frames = contract.subscribe();

    assert_eq!(
        contract.inbound(&json!({ "type": "auth", "data": { "token": "t" } })),
        Some(json!({ "type": "auth_ok" }))
    );
    let created = contract
        .rest("POST", "/api/v1/agents/assistant/chats")
        .await
        .unwrap();
    assert_eq!(created["chat"]["id"], "s1");

    let session = "agent:assistant:thread:s1";
    contract.inbound(&json!({
        "type": "chat",
        "message_id": "m1",
        "data": { "prompt": "hi", "agent_id": "assistant", "session_id": session, "permission_mode": "plan" },
    }));
    let asked = until(&mut frames, "ask_request").await;
    assert_eq!(asked[0].kind, "chat_stream");
    assert_eq!(asked[0].data["content"], "You said hi.");
    let ask = &asked.last().unwrap().data;
    assert_eq!(ask["session_id"], session);
    assert_eq!(ask["request_id"], "call_1");
    assert_eq!(ask["widgets"][0]["options"], json!(["Allow once", "Deny"]));
    let notices = contract.rest("GET", "/api/v1/notifications").await.unwrap();
    assert_eq!(
        notices["notifications"][0]["title"],
        "Fake asks to run `ls`"
    );
    let messages = contract
        .rest("GET", "/api/v1/chats/s1/messages")
        .await
        .unwrap();
    assert_eq!(messages["pendingAsk"]["request_id"], "call_1");

    // The phone answers with the label it showed.
    contract.inbound(&json!({ "type": "ask_response", "data": { "request_id": "call_1", "value": "Allow once" } }));
    let done = until(&mut frames, "chat_complete").await;
    let kinds: Vec<&str> = done.iter().map(|f| f.kind.as_str()).collect();
    assert_eq!(kinds, ["chat_stream", "usage", "chat_complete"]);
    assert_eq!(done[1].data["input_tokens"], 10);
    assert_eq!(done[2].data["stop_reason"], "end_turn");
    assert_eq!(
        *fake.answers.lock().unwrap(),
        vec![(Some("call_1".to_owned()), "allow".to_owned())]
    );
    assert_eq!(
        *fake.prompts.lock().unwrap(),
        vec!["hi".to_owned()],
        "a runtime without modes of its own takes the turn as it is"
    );
    assert_eq!(
        contract
            .rest("GET", "/api/v1/notifications/unread-count")
            .await
            .unwrap(),
        json!({ "count": 0 })
    );
    let items = inbox.0.lock().unwrap().clone();
    assert!(
        matches!(&items[0], InboxItem::Approval { id, title, .. } if id == "approval:call_1" && title == "Fake asks to run `ls`")
    );
    assert_eq!(
        items[1],
        InboxItem::Resolved {
            id: "approval:call_1".into()
        }
    );

    // A frame the phone re-sends after a reconnect runs once.
    contract.inbound(&json!({ "type": "chat", "message_id": "m1", "data": { "prompt": "hi", "session_id": session } }));
    contract.inbound(&json!({ "type": "cancel", "data": { "session_id": session } }));
    let cancelled = until(&mut frames, "chat_cancelled").await;
    assert_eq!(
        cancelled.len(),
        1,
        "nothing ran, so nothing streamed: {:?}",
        cancelled.iter().map(|f| &f.kind).collect::<Vec<_>>()
    );
    assert_eq!(cancelled[0].data["session_id"], Value::from(session));
}

/// Every agent has one id in one form; a hire saved with an agent's id in an
/// older form (`<member>.<agent>`) still reaches it, and its chat's frames
/// carry the id it knows.
#[tokio::test]
async fn an_agent_id_saved_in_an_older_form_still_names_its_agent() {
    let openclaw = Arc::new(Fake {
        others: vec!["Research_Bot"],
        ..Fake::default()
    });
    let host = Host::new(Arc::new(Roster::new(vec![
        member(link_core::PRIMARY, "Claude Code", Arc::new(Fake::default())),
        member("openclaw", "OpenClaw", openclaw.clone()),
    ])));
    let contract = Contract::new("openclaw", "OpenClaw", host, None);
    let mut frames = contract.subscribe();

    let agents = contract.rest("GET", "/api/v1/agents").await.unwrap();
    let ids: Vec<&str> = agents["agents"].as_array().unwrap().iter().map(|a| a["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["assistant", "openclaw", "openclaw-research-bot"]);

    let legacy = "openclaw.Research_Bot";
    let agent = contract.rest("GET", &format!("/api/v1/agents/{legacy}")).await.unwrap();
    assert_eq!(agent["agent"]["id"], legacy);
    let created = contract.rest("POST", &format!("/api/v1/agents/{legacy}/chats")).await.unwrap();
    let chat = created["chat"]["id"].as_str().unwrap().to_owned();
    let session = format!("agent:{legacy}:thread:{chat}");
    contract.inbound(&json!({
        "type": "chat",
        "message_id": "m1",
        "data": { "prompt": "hi", "agent_id": legacy, "session_id": session },
    }));
    let asked = until(&mut frames, "ask_request").await;
    assert!(asked.iter().all(|f| f.data["session_id"] == session.as_str() && f.data["agent_id"] == legacy));
    contract.inbound(&json!({ "type": "ask_response", "data": { "request_id": "call_1", "value": "Allow once" } }));
    let done = until(&mut frames, "chat_complete").await;
    assert!(done.iter().all(|f| f.data["session_id"] == session.as_str()));
    assert_eq!(openclaw.answers.lock().unwrap().len(), 1, "the turn ran on the agent the old id names");
    assert!(contract.rest("GET", "/api/v1/agents/openclaw.nobody").await.is_err());
}
