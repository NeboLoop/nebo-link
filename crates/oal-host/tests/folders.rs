//! Adding an agent over Open Agent Link, moving its conversation to another
//! folder through the host's own tool, and removing it: the agent is the
//! conformance suite's scripted agent, as a process.

mod common;

use std::path::Path;

use link_core::model::DeviceRef;
use oal_host::LocalConnection;
use serde_json::{Value, json};

async fn next(conn: &mut LocalConnection) -> Value {
    let bytes = tokio::time::timeout(std::time::Duration::from_secs(30), conn.rx.recv())
        .await
        .expect("a frame in time")
        .expect("the connection is open");
    serde_json::from_slice(&bytes).unwrap()
}

fn send(conn: &LocalConnection, frame: Value) {
    conn.tx.send(frame.to_string().into_bytes()).unwrap();
}

/// Frames until `pick` finds the one wanted, the rest kept in order.
async fn until(conn: &mut LocalConnection, seen: &mut Vec<Value>, pick: impl Fn(&Value) -> bool) -> Value {
    loop {
        let frame = next(conn).await;
        if pick(&frame) {
            return frame;
        }
        seen.push(frame);
    }
}

/// Sends `text` to the session and returns what the agent said, every
/// update of the turn, and the turn's end.
async fn turn(conn: &mut LocalConnection, agent: &str, session: &str, id: u64, text: &str) -> (String, Vec<Value>) {
    send(conn, json!({ "agent": agent, "acp": { "jsonrpc": "2.0", "id": id, "method": "session/prompt",
        "params": { "sessionId": session, "prompt": [{ "type": "text", "text": text }] } } }));
    let mut seen = Vec::new();
    until(conn, &mut seen, |f| f["method"] == "host/turn" && f["params"]["state"] == "ended").await;
    let updates: Vec<Value> = seen
        .into_iter()
        .filter(|f| f["agent"] == agent && f["acp"]["method"] == "session/update")
        .inspect(|f| assert_eq!(f["acp"]["params"]["sessionId"], session, "every update names the conversation"))
        .map(|f| f["acp"]["params"]["update"].clone())
        .collect();
    let said = updates
        .iter()
        .filter(|u| u["sessionUpdate"] == "agent_message_chunk")
        .filter_map(|u| u["content"]["text"].as_str())
        .collect();
    (said, updates)
}

fn canonical(path: &Path) -> String {
    path.canonicalize().unwrap().display().to_string()
}

#[tokio::test]
async fn a_device_adds_an_agent_which_moves_where_the_owner_asks_and_is_removed() {
    let dir = tempfile::tempdir().unwrap();
    let oal = common::host(dir.path()).await;
    let home = dir.path().join("home").canonicalize().unwrap();
    let phone = DeviceRef { device_id: "d-phone".into(), name: "Phone".into() };
    let mut conn = oal.connect_local(phone);
    let mut seen = Vec::new();

    // The scripted agent is addable; adding one gives it a folder of its own.
    send(&conn, json!({ "jsonrpc": "2.0", "id": 1, "method": "host/info", "params": {} }));
    let info = until(&mut conn, &mut seen, |f| f["id"] == 1).await;
    assert_eq!(info["result"]["runtimes"], json!([{ "id": "oal-fake-agent", "name": "Fake Agent", "kind": "acp", "addable": true }]));
    send(&conn, json!({ "jsonrpc": "2.0", "id": 2, "method": "host/agents/add", "params": { "runtime": "oal-fake-agent" } }));
    let added = until(&mut conn, &mut seen, |f| f["id"] == 2).await;
    let agent = added["result"]["agent"].clone();
    let id = agent["id"].as_str().unwrap().to_owned();
    let folder = home.join("NeboAI").join(&id);
    assert_eq!(agent["folder"], canonical(&folder), "{added}");
    assert!(folder.is_dir());
    assert_eq!(agent["label"], "Fake Agent", "an agent the host doesn't know by name is named for itself");
    until(&mut conn, &mut seen, |f| f["method"] == "host/agent_update" && f["params"]["change"] == "added").await;

    // A conversation in its folder.
    let acp = |n: u64, method: &str, params: Value| json!({ "agent": id, "acp": { "jsonrpc": "2.0", "id": n, "method": method, "params": params } });
    send(&conn, acp(3, "initialize", json!({ "protocolVersion": 1, "clientCapabilities": {} })));
    let init = until(&mut conn, &mut seen, |f| f["acp"]["id"] == 3).await;
    assert_eq!(init["acp"]["result"]["agentCapabilities"]["mcpCapabilities"]["http"], true);
    send(&conn, acp(4, "session/new", json!({ "cwd": agent["folder"], "mcpServers": [] })));
    let created = until(&mut conn, &mut seen, |f| f["acp"]["id"] == 4).await;
    let session = created["acp"]["result"]["sessionId"].as_str().unwrap().to_owned();
    let (said, _) = turn(&mut conn, &id, &session, 5, "where").await;
    assert_eq!(said, format!("Working in {}.", canonical(&folder)));

    // Asked for a folder that isn't there, the agent can't move, and nothing
    // is made; outside the home folder it can't either, outside Full access.
    let (said, _) = turn(&mut conn, &id, &session, 6, "work in ~/workspaces/foo").await;
    assert!(said.starts_with("Couldn't move: There's no folder"), "{said}");
    assert!(!home.join("workspaces/foo").exists());
    let (said, _) = turn(&mut conn, &id, &session, 7, &format!("work in {}", dir.path().display())).await;
    assert!(said.contains("outside the home folder"), "{said}");

    // Asked to work in a new folder, it moves there: the conversation keeps
    // its id, clients are told the folder, and the owner reads where.
    let (said, updates) = turn(&mut conn, &id, &session, 8, "work in new ~/workspaces/foo").await;
    let foo = home.join("workspaces/foo");
    assert!(foo.is_dir(), "made, as the owner asked");
    assert!(said.contains("Now working in ~/workspaces/foo."), "{said}");
    assert!(said.ends_with("Moved."), "{said}");
    let info = updates.iter().find(|u| u["sessionUpdate"] == "session_info_update").expect("the folder is told");
    assert_eq!(info["_meta"]["oal/cwd"], canonical(&foo));
    let call = updates.iter().find(|u| u["sessionUpdate"] == "tool_call").unwrap();
    assert_eq!(call["title"], "move_to_folder");

    // The next message runs in the new folder, starting with the handoff.
    let (said, _) = turn(&mut conn, &id, &session, 9, "where").await;
    assert!(said.starts_with(&format!("Working in {}.", canonical(&foo))), "{said}");
    assert!(said.contains(&format!("Was working in {}.", canonical(&folder))), "the handoff came first: {said}");
    let (said, _) = turn(&mut conn, &id, &session, 10, "where").await;
    assert_eq!(said, format!("Working in {}.", canonical(&foo)), "the handoff is sent once");

    // Loaded again, the conversation says where it works.
    send(&conn, acp(11, "session/load", json!({ "sessionId": session, "cwd": agent["folder"], "mcpServers": [] })));
    let loaded = until(&mut conn, &mut seen, |f| f["acp"]["id"] == 11).await;
    assert_eq!(loaded["acp"]["result"]["_meta"]["oal/cwd"], canonical(&foo));

    // Removed: no longer hosted, and both folders stay.
    send(&conn, json!({ "jsonrpc": "2.0", "id": 12, "method": "host/agents/remove", "params": { "agentId": id } }));
    let removed = until(&mut conn, &mut seen, |f| f["id"] == 12).await;
    assert_eq!(removed["result"], json!({}), "{removed}");
    until(&mut conn, &mut seen, |f| f["method"] == "host/agent_update" && f["params"]["change"] == "removed").await;
    assert!(folder.is_dir() && foo.is_dir(), "removal never deletes a folder");
    send(&conn, json!({ "jsonrpc": "2.0", "id": 13, "method": "host/agents", "params": {} }));
    let agents = until(&mut conn, &mut seen, |f| f["id"] == 13).await;
    assert!(agents["result"]["agents"].as_array().unwrap().iter().all(|a| a["id"] != id.as_str()));
    // The agent the keeper doesn't hold is not removed from a device.
    send(&conn, json!({ "jsonrpc": "2.0", "id": 14, "method": "host/agents/remove", "params": { "agentId": common::AGENT } }));
    let refused = until(&mut conn, &mut seen, |f| f["id"] == 14).await;
    assert_eq!(refused["error"]["code"], -33010);
}
