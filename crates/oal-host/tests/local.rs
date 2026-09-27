//! A client in the host's own process: no handshake and nothing to encrypt,
//! and the same host channel, agent channels and rules as any connection.

mod common;

use link_core::model::DeviceRef;
use oal_host::LocalConnection;
use serde_json::{Value, json};

async fn next(conn: &mut LocalConnection) -> Value {
    let bytes = tokio::time::timeout(std::time::Duration::from_secs(10), conn.rx.recv())
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

#[tokio::test]
async fn an_in_process_client_runs_a_turn_through_a_permission_request() {
    let dir = tempfile::tempdir().unwrap();
    let oal = common::host(dir.path()).await;
    let nebo = DeviceRef { device_id: "local".into(), name: "Nebo".into() };
    let mut conn = oal.connect_local(nebo);
    let mut seen = Vec::new();

    send(&conn, json!({ "jsonrpc": "2.0", "id": 1, "method": "host/hello", "params": {} }));
    let hello = until(&mut conn, &mut seen, |f| f["id"] == 1).await;
    assert_eq!(hello["result"]["device"], json!({ "id": "local", "name": "Nebo" }));
    send(&conn, json!({ "jsonrpc": "2.0", "id": 2, "method": "host/agents", "params": {} }));
    let agents = until(&mut conn, &mut seen, |f| f["id"] == 2).await;
    assert_eq!(agents["result"]["agents"][0]["id"], common::AGENT);

    let acp = |id: u64, method: &str, params: Value| json!({ "agent": common::AGENT, "acp": { "jsonrpc": "2.0", "id": id, "method": method, "params": params } });
    send(&conn, acp(3, "initialize", json!({ "protocolVersion": 1, "clientCapabilities": {} })));
    until(&mut conn, &mut seen, |f| f["acp"]["id"] == 3).await;
    send(&conn, acp(4, "session/new", json!({ "cwd": oal_conformance::fake_host::FOLDER, "mcpServers": [] })));
    let created = until(&mut conn, &mut seen, |f| f["acp"]["id"] == 4).await;
    let session = created["acp"]["result"]["sessionId"].as_str().unwrap().to_owned();
    send(&conn, acp(5, "session/prompt", json!({ "sessionId": session, "prompt": [{ "type": "text", "text": "run: echo hi" }] })));

    let asked = until(&mut conn, &mut seen, |f| f["acp"]["method"] == "session/request_permission").await;
    assert_eq!(asked["acp"]["params"]["toolCall"]["title"], "echo hi");
    let answer = json!({ "agent": common::AGENT, "acp": { "jsonrpc": "2.0", "id": asked["acp"]["id"], "result": { "outcome": { "outcome": "selected", "optionId": "allow-once" } } } });
    send(&conn, answer);
    let ended = until(&mut conn, &mut seen, |f| f["method"] == "host/turn" && f["params"]["state"] == "ended").await;
    assert_eq!(ended["params"]["stopReason"], "end_turn");
    assert_eq!(ended["params"]["by"], json!({ "deviceId": "local", "name": "Nebo" }));
    assert_eq!(ended["params"]["usage"]["outputTokens"], 5);
    let text: String = seen
        .iter()
        .filter(|f| f["acp"]["params"]["update"]["sessionUpdate"] == "agent_message_chunk")
        .filter_map(|f| f["acp"]["params"]["update"]["content"]["text"].as_str())
        .collect();
    assert_eq!(text, "Done.");
    assert!(seen.iter().any(|f| f["acp"]["id"] == 5 && f["acp"]["result"]["stopReason"] == "end_turn"), "the prompt's answer came before the end");
    assert!(oal.devices().is_empty(), "an in-process client is no paired device");

    // Dropping the client's end closes the connection.
    drop(conn.tx);
    assert!(tokio::time::timeout(std::time::Duration::from_secs(5), async { while conn.rx.recv().await.is_some() {} })
        .await
        .is_ok());
}
