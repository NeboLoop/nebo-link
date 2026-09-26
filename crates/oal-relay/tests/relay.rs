//! The relay end to end: a real relay on a local port, real hosts and
//! clients over WebSockets.

mod common;

use std::time::Duration;

use bytes::Bytes;
use common::*;
use futures::SinkExt;
use oal_relay::auth::{self, Role, Target};
use oal_relay::host::HostEvent;
use oal_relay::wire::{AgentPresence, Message};
use oal_relay::{Keypair, Pairing};
use oal_secure::PairingCode;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;

#[tokio::test]
async fn a_host_registers_a_client_pairs_and_frames_flow_both_ways() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let host_key = Keypair::generate();
    let (mut tunnel, pairings) = host_up(&relay, &host_key, "studio").await;
    assert!(pairings.is_empty());

    // The host makes the code: a nameplate from the relay, the secret half
    // its own. Only the nameplate ever reaches the relay.
    let handle = tunnel.handle();
    let nameplate = handle.nameplate(None).await.unwrap();
    let code = PairingCode::generate(Some(&nameplate.nameplate)).unwrap();
    assert_eq!(code.nameplate(), nameplate.nameplate);

    let phone = client(&relay, Keypair::generate());
    let (mut ws, pairing) = phone.pair(code.nameplate()).await.unwrap();
    assert_eq!(
        pairing,
        Pairing {
            host_id: "studio".into(),
            host_key: host_key.public_b64()
        }
    );
    let mut conn = next_client(&mut tunnel).await;
    assert_eq!(conn.client_key, phone.key().public_b64());
    assert_eq!(conn.nameplate.as_deref(), Some(code.nameplate()));

    // Pairing messages are binary (CPace, then Noise): each arrives whole,
    // unchanged and in order, both ways.
    for size in [34usize, 48, 64] {
        let msg: Vec<u8> = (0..size).map(|i| (i * 7 + size) as u8).collect();
        ws.send(WsMessage::binary(msg.clone())).await.unwrap();
        assert_eq!(
            conn.rx.recv().await.unwrap().unwrap(),
            Message::Binary(msg.into())
        );
    }
    for size in [34usize, 96] {
        let msg: Vec<u8> = (0..size).map(|i| (i * 3 + size) as u8).collect();
        conn.tx
            .send(Message::Binary(msg.clone().into()))
            .await
            .unwrap();
        assert_eq!(ws_next(&mut ws).await, WsMessage::Binary(msg.into()));
    }

    // Until the host says it paired this client, the relay lets it no
    // further than the pairing connection.
    assert_eq!(refusal(phone.connect("studio").await), "not_paired");
    handle.paired(&conn.client_key).await.unwrap();

    // Client to host: text, unchanged.
    let hello = r#"{"jsonrpc":"2.0","id":1,"method":"host/hello","params":{}}"#;
    ws.send(WsMessage::text(hello)).await.unwrap();
    assert_eq!(
        conn.rx.recv().await.unwrap().unwrap(),
        Message::Text(hello.into())
    );

    // Host to client: a large binary message, whole, past yamux's initial
    // window.
    let big: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    conn.tx
        .send(Message::Binary(Bytes::from(big.clone())))
        .await
        .unwrap();
    assert_eq!(ws_next(&mut ws).await, WsMessage::Binary(big.into()));

    // Order holds both ways.
    for i in 0..200 {
        ws.send(WsMessage::text(format!("c{i}"))).await.unwrap();
        conn.tx.send(Message::Text(format!("h{i}"))).await.unwrap();
    }
    for i in 0..200 {
        assert_eq!(
            conn.rx.recv().await.unwrap().unwrap(),
            Message::Text(format!("c{i}"))
        );
        assert_eq!(ws_next(&mut ws).await, WsMessage::text(format!("h{i}")));
    }

    // The host closes with an OAL code; the client sees the same code.
    conn.tx
        .send(Message::Close {
            code: Some(4002),
            reason: String::new(),
        })
        .await
        .unwrap();
    assert_eq!(ws_close_code(&mut ws).await, Some(4002));

    // A later connection, not a pairing one.
    let mut ws = phone.connect("studio").await.unwrap();
    let mut conn = next_client(&mut tunnel).await;
    assert_eq!(conn.nameplate, None);
    assert_eq!(conn.client_key, phone.key().public_b64());
    // The client closes with a code; the host sees it.
    ws.close(Some(CloseFrame {
        code: 4001.into(),
        reason: "".into(),
    }))
    .await
    .unwrap();
    assert!(matches!(
        conn.rx.recv().await.unwrap().unwrap(),
        Message::Close {
            code: Some(4001),
            ..
        }
    ));
    relay.shutdown().await;
}

#[tokio::test]
async fn the_secret_half_of_a_code_never_reaches_the_relay() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let (tunnel, _) = host_up(&relay, &Keypair::generate(), "studio").await;
    let nameplate = tunnel.handle().nameplate(None).await.unwrap();
    let code = PairingCode::generate(Some(&nameplate.nameplate)).unwrap();

    // The client library takes only a nameplate and refuses a whole code
    // before anything is sent.
    let phone = client(&relay, Keypair::generate());
    match phone.pair(&code.to_string()).await {
        Err(oal_relay::Error::Url(message)) => assert!(message.contains("nameplate"), "{message}"),
        Err(e) => panic!("expected a local refusal, got {e}"),
        Ok(_) => panic!("a whole code must not be sent"),
    }
    // And the relay refuses a pairing path that carries more than the
    // nameplate, before anything else (spec section 4.4).
    for whole in [code.to_string(), code.to_string().replace('-', "")] {
        let r = reqwest::get(format!("{}/oal/pair/{whole}", url(&relay)))
            .await
            .unwrap();
        assert_eq!(r.status(), 400);
        let body: serde_json::Value = r.json().await.unwrap();
        assert_eq!(body["code"], "bad_nameplate");
    }

    // A host can't register a whole code as a nameplate either.
    assert_eq!(
        refusal(tunnel.handle().nameplate(Some(&code.to_string())).await),
        "bad_nameplate"
    );
    relay.shutdown().await;
}

#[tokio::test]
async fn nameplates_route_until_they_expire_and_belong_to_one_host() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.nameplate_ttl = Duration::from_secs(1);
    let relay = relay_with(config).await;
    let (mut tunnel, _) = host_up(&relay, &Keypair::generate(), "studio").await;
    let (laptop, _) = host_up(&relay, &Keypair::generate(), "laptop").await;

    let phone = client(&relay, Keypair::generate());
    assert_eq!(refusal(phone.pair("ZZZZ").await), "unknown_nameplate");

    // A nameplate the host chose (a code it, or a client, made).
    let chosen = tunnel.handle().nameplate(Some("k7qm")).await.unwrap();
    assert_eq!(chosen.nameplate, "K7QM");
    assert_eq!(
        refusal(laptop.handle().nameplate(Some("K7QM")).await),
        "nameplate_taken"
    );
    // The same host asking again renews it.
    tunnel.handle().nameplate(Some("K7QM")).await.unwrap();

    // Not used up by an attempt: the host judges the code and counts
    // failures; every attempt is carried to it.
    let (_first, _) = phone.pair("K7QM").await.unwrap();
    assert!(next_client(&mut tunnel).await.nameplate.is_some());
    let other = client(&relay, Keypair::generate());
    let (_second, _) = other.pair("k7qm").await.unwrap();
    assert!(next_client(&mut tunnel).await.nameplate.is_some());

    // Expired: refused, and free for another host.
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(refusal(other.pair("K7QM").await), "unknown_nameplate");
    assert_eq!(
        laptop
            .handle()
            .nameplate(Some("K7QM"))
            .await
            .unwrap()
            .nameplate,
        "K7QM"
    );
    relay.shutdown().await;
}

#[tokio::test]
async fn guessing_nameplates_is_rate_limited() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let (tunnel, _) = host_up(&relay, &Keypair::generate(), "studio").await;
    let phone = client(&relay, Keypair::generate());
    let nameplate = tunnel.handle().nameplate(None).await.unwrap();
    let wrong = if nameplate.nameplate == "ZZZZ" {
        "YYYY"
    } else {
        "ZZZZ"
    };
    for _ in 0..10 {
        assert_eq!(refusal(phone.pair(wrong).await), "unknown_nameplate");
    }
    // Even the right one waits now.
    assert_eq!(
        refusal(phone.pair(&nameplate.nameplate).await),
        "too_many_attempts"
    );
    relay.shutdown().await;
}

#[tokio::test]
async fn an_unpaired_or_impersonating_client_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let (mut tunnel, _) = host_up(&relay, &Keypair::generate(), "studio").await;
    let _laptop = host_up(&relay, &Keypair::generate(), "laptop").await;
    let phone = client(&relay, Keypair::generate());
    let (_ws, _conn) = pair(&mut tunnel, &phone).await;

    let stranger = client(&relay, Keypair::generate());
    assert_eq!(refusal(stranger.connect("studio").await), "not_paired");
    assert_eq!(refusal(stranger.connect("nowhere").await), "not_paired");
    // Paired with studio is not paired with laptop.
    assert_eq!(refusal(phone.connect("laptop").await), "not_paired");

    // Claiming the phone's key without its secret fails the proof.
    let base = url(&relay);
    let response = reqwest::get(format!("{base}/oal/challenge")).await.unwrap();
    // Browser clients fetch the challenge from their own origin.
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    let challenge: serde_json::Value = response.json().await.unwrap();
    let (nonce, relay_key) = (
        challenge["nonce"].as_str().unwrap(),
        challenge["relayKey"].as_str().unwrap(),
    );
    assert_eq!(relay_key, relay.relay_key());
    let forged = auth::prove(
        stranger.key(),
        Role::Client,
        relay_key,
        nonce,
        Target::Connect("studio"),
    )
    .unwrap();
    let ws_base = base.replace("http://", "ws://");
    let url = format!(
        "{ws_base}/oal/hosts/studio?key={}&nonce={nonce}&proof={forged}",
        phone.key().public_b64()
    );
    match tokio_tungstenite::connect_async(url).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => assert_eq!(r.status(), 401),
        other => panic!("expected 401, got {other:?}"),
    }

    // A good proof works once; replaying it (same nonce) does not.
    let challenge: serde_json::Value = reqwest::get(format!("{base}/oal/challenge"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let nonce = challenge["nonce"].as_str().unwrap();
    let proof = auth::prove(
        phone.key(),
        Role::Client,
        relay_key,
        nonce,
        Target::Connect("studio"),
    )
    .unwrap();
    let url = format!(
        "{ws_base}/oal/hosts/studio?key={}&nonce={nonce}&proof={proof}",
        phone.key().public_b64()
    );
    let (_first, _) = tokio_tungstenite::connect_async(url.clone()).await.unwrap();
    match tokio_tungstenite::connect_async(url).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(r)) => assert_eq!(r.status(), 401),
        other => panic!("expected 401 on replay, got {other:?}"),
    }
    relay.shutdown().await;
}

#[tokio::test]
async fn a_host_id_belongs_to_its_key_and_the_allowlist_holds() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let owner = Keypair::generate();
    let _studio = host_up(&relay, &owner, "studio").await;
    let squatter = client(&relay, Keypair::generate());
    assert_eq!(refusal(squatter.host("studio").await), "host_id_taken");
    assert_eq!(
        refusal(client(&relay, owner.clone()).host("other").await),
        "host_key_taken"
    );
    relay.shutdown().await;

    let dir = tempfile::tempdir().unwrap();
    let allowed = Keypair::generate();
    let mut config = config(dir.path());
    config.allow_hosts = vec![allowed.public_b64()];
    let relay = relay_with(config).await;
    let _ok = host_up(&relay, &allowed, "studio").await;
    assert_eq!(
        refusal(client(&relay, Keypair::generate()).host("laptop").await),
        "not_allowed"
    );
    relay.shutdown().await;
}

#[tokio::test]
async fn a_host_reconnect_keeps_pairings() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let host_key = Keypair::generate();
    let (mut tunnel, _) = host_up(&relay, &host_key, "studio").await;
    let phone = client(&relay, Keypair::generate());
    let (mut ws, _conn) = pair(&mut tunnel, &phone).await;

    // The host goes away: its clients are told to reconnect (1001) and new
    // connections hear it is offline.
    tunnel.handle().close();
    assert_eq!(ws_close_code(&mut ws).await, Some(1001));
    let mut offline = false;
    for _ in 0..50 {
        match phone.connect("studio").await {
            Err(oal_relay::Error::Refused { code, message }) if code == "host_offline" => {
                assert_eq!(message, "studio is offline.");
                offline = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(100)).await,
        }
    }
    assert!(offline);

    let (mut tunnel, pairings) = host_up(&relay, &host_key, "studio").await;
    assert_eq!(pairings.len(), 1);
    assert_eq!(pairings[0].client_key, phone.key().public_b64());
    let mut ws = phone.connect("studio").await.unwrap();
    let mut conn = next_client(&mut tunnel).await;
    ws.send(WsMessage::text("back")).await.unwrap();
    assert_eq!(
        conn.rx.recv().await.unwrap().unwrap(),
        Message::Text("back".into())
    );
    relay.shutdown().await;
}

#[tokio::test]
async fn a_relay_restart_keeps_hosts_pairings_and_its_key() {
    let dir = tempfile::tempdir().unwrap();
    let relay1 = relay(dir.path()).await;
    let relay_key = relay1.relay_key().to_owned();
    let host_key = Keypair::generate();
    let phone_key = Keypair::generate();
    let (mut tunnel, _) = host_up(&relay1, &host_key, "studio").await;
    let (mut ws, _conn) = pair(&mut tunnel, &client(&relay1, phone_key.clone())).await;
    relay1.shutdown().await;
    // Everyone is told to come back.
    assert_eq!(ws_close_code(&mut ws).await, Some(1001));
    // The host's tunnel ends (after any notice still queued on it).
    while tokio::time::timeout(WAIT, tunnel.next())
        .await
        .unwrap()
        .is_some()
    {}

    let relay2 = relay(dir.path()).await;
    assert_eq!(relay2.relay_key(), relay_key);
    let (mut tunnel, pairings) = host_up(&relay2, &host_key, "studio").await;
    assert_eq!(pairings.len(), 1);
    let phone = client(&relay2, phone_key);
    let mut ws = phone.connect("studio").await.unwrap();
    let mut conn = next_client(&mut tunnel).await;
    conn.tx
        .send(Message::Text("still here".into()))
        .await
        .unwrap();
    assert_eq!(ws_next(&mut ws).await, WsMessage::text("still here"));
    // Another key still cannot take the host's id.
    assert_eq!(
        refusal(client(&relay2, Keypair::generate()).host("studio").await),
        "host_id_taken"
    );
    relay2.shutdown().await;
}

#[tokio::test]
async fn many_concurrent_streams() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let (mut tunnel, _) = host_up(&relay, &Keypair::generate(), "studio").await;
    let mut phones = Vec::new();
    for _ in 0..4 {
        let phone = client(&relay, Keypair::generate());
        let (_ws, _conn) = pair(&mut tunnel, &phone).await;
        phones.push(std::sync::Arc::new(phone));
    }
    // The host echoes every connection.
    tokio::spawn(async move {
        while let Some(event) = tunnel.next().await {
            if let HostEvent::Client(mut conn) = event {
                tokio::spawn(async move {
                    while let Some(Ok(msg)) = conn.rx.recv().await {
                        if matches!(msg, Message::Close { .. }) || conn.tx.send(msg).await.is_err()
                        {
                            break;
                        }
                    }
                });
            }
        }
    });

    const STREAMS: usize = 64;
    const MESSAGES: usize = 25;
    let mut tasks = Vec::new();
    for n in 0..STREAMS {
        let phone = phones[n % phones.len()].clone();
        tasks.push(tokio::spawn(async move {
            let mut ws = phone.connect("studio").await.unwrap();
            for i in 0..MESSAGES {
                ws.send(WsMessage::text(format!("{n}:{i}"))).await.unwrap();
            }
            for i in 0..MESSAGES {
                assert_eq!(ws_next(&mut ws).await, WsMessage::text(format!("{n}:{i}")));
            }
            ws.close(None).await.unwrap();
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let metrics = reqwest::get(format!("{}/metrics", url(&relay)))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let to_host = metrics
        .lines()
        .find(|l| l.starts_with("oal_relay_messages_total{direction=\"to_host\"}"))
        .unwrap();
    let count: usize = to_host.rsplit(' ').next().unwrap().parse().unwrap();
    assert!(count >= STREAMS * MESSAGES, "{to_host}");
    relay.shutdown().await;
}

#[tokio::test]
async fn presence_is_for_paired_clients() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let (mut tunnel, _) = host_up(&relay, &Keypair::generate(), "studio").await;
    let phone = client(&relay, Keypair::generate());
    let (_ws, _conn) = pair(&mut tunnel, &phone).await;
    let agents = vec![
        AgentPresence {
            id: "app".into(),
            online: true,
        },
        AgentPresence {
            id: "api".into(),
            online: false,
        },
    ];
    tunnel
        .handle()
        .publish_presence(agents.clone())
        .await
        .unwrap();

    let mut seen = Vec::new();
    for _ in 0..50 {
        seen = phone.presence().await.unwrap();
        if seen.len() == 1 && seen[0].agents == agents {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].host_id, "studio");
    assert!(seen[0].online);
    assert_eq!(seen[0].agents, agents);

    assert!(
        client(&relay, Keypair::generate())
            .presence()
            .await
            .unwrap()
            .is_empty()
    );
    // A browser client reads presence, refusals included, from its own origin.
    let refused = reqwest::get(format!("{}/oal/presence", url(&relay)))
        .await
        .unwrap();
    assert_eq!(refused.status(), 401);
    assert_eq!(refused.headers()["access-control-allow-origin"], "*");

    tunnel.handle().close();
    for _ in 0..50 {
        seen = phone.presence().await.unwrap();
        if !seen[0].online {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(!seen[0].online);
    assert!(seen[0].agents.is_empty());
    relay.shutdown().await;
}

async fn admin(
    relay: &oal_relay::server::RelayHandle,
    token: &str,
    path: &str,
    body: serde_json::Value,
) -> (u16, serde_json::Value) {
    let r = reqwest::Client::new()
        .post(format!("{}/admin/{path}", url(relay)))
        .bearer_auth(token)
        .json(&body)
        .send()
        .await
        .unwrap();
    (r.status().as_u16(), r.json().await.unwrap())
}

#[tokio::test]
async fn the_operator_revokes() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let host_key = Keypair::generate();
    let (mut tunnel, _) = host_up(&relay, &host_key, "studio").await;

    let (status, _) = admin(
        &relay,
        "wrong",
        "revoke",
        serde_json::json!({"hostId": "studio"}),
    )
    .await;
    assert_eq!(status, 401);
    // The operator issues no codes: a code's secret half is the host's.
    let (status, _) = admin(
        &relay,
        ADMIN,
        "codes",
        serde_json::json!({"hostId": "studio"}),
    )
    .await;
    assert_eq!(status, 404);

    let phone = client(&relay, Keypair::generate());
    let (_pair_ws, _pair_conn) = pair(&mut tunnel, &phone).await;

    // Removing the pairing closes the phone's live connections with 4003,
    // tells the host, and refuses the phone from then on.
    let mut ws = phone.connect("studio").await.unwrap();
    let _conn = next_client(&mut tunnel).await;
    let phone_key = phone.key().public_b64();
    let (status, _) = admin(
        &relay,
        ADMIN,
        "revoke",
        serde_json::json!({"hostId": "studio", "clientKey": phone_key}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(ws_close_code(&mut ws).await, Some(4003));
    loop {
        match event(&mut tunnel).await {
            HostEvent::Unpaired { client_key } => {
                assert_eq!(client_key, phone_key);
                break;
            }
            _ => continue,
        }
    }
    assert_eq!(refusal(phone.connect("studio").await), "not_paired");

    // A revoked client key can never pair again.
    let nameplate = tunnel.handle().nameplate(None).await.unwrap();
    let (status, _) = admin(
        &relay,
        ADMIN,
        "revoke",
        serde_json::json!({"clientKey": phone_key}),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(refusal(phone.pair(&nameplate.nameplate).await), "revoked");

    // A revoked host loses its tunnel and cannot come back with that key.
    let (status, _) = admin(
        &relay,
        ADMIN,
        "revoke",
        serde_json::json!({"hostId": "studio"}),
    )
    .await;
    assert_eq!(status, 200);
    loop {
        match tokio::time::timeout(WAIT, tunnel.next()).await.unwrap() {
            None => break,
            Some(_) => continue,
        }
    }
    assert_eq!(
        refusal(client(&relay, host_key).host("studio").await),
        "revoked"
    );

    // The operator's view.
    let r = reqwest::Client::new()
        .get(format!("{}/admin/hosts", url(&relay)))
        .bearer_auth(ADMIN)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = r.json().await.unwrap();
    assert_eq!(body["hosts"], serde_json::json!([]));
    relay.shutdown().await;
}

#[tokio::test]
async fn the_host_unpairs_a_client() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let (mut tunnel, _) = host_up(&relay, &Keypair::generate(), "studio").await;
    let phone = client(&relay, Keypair::generate());
    let (mut ws, _conn) = pair(&mut tunnel, &phone).await;
    tunnel
        .handle()
        .unpair(&phone.key().public_b64())
        .await
        .unwrap();
    assert_eq!(ws_close_code(&mut ws).await, Some(4003));
    assert_eq!(refusal(phone.connect("studio").await), "not_paired");
    relay.shutdown().await;
}

#[tokio::test]
async fn a_nameplate_for_an_offline_host_waits_for_it() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let host_key = Keypair::generate();
    let (tunnel, _) = host_up(&relay, &host_key, "studio").await;
    let nameplate = tunnel.handle().nameplate(None).await.unwrap().nameplate;
    tunnel.handle().close();
    drop(tunnel);
    let phone = client(&relay, Keypair::generate());
    let mut refused = String::new();
    for _ in 0..50 {
        refused = refusal(phone.pair(&nameplate).await);
        if refused == "host_offline" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(refused, "host_offline");
    let (mut tunnel, _) = host_up(&relay, &host_key, "studio").await;
    let (_ws, _) = phone.pair(&nameplate).await.unwrap();
    let conn = next_client(&mut tunnel).await;
    assert_eq!(conn.nameplate, Some(nameplate));
    relay.shutdown().await;
}

#[tokio::test]
async fn health_and_metrics_carry_no_identities() {
    let dir = tempfile::tempdir().unwrap();
    let relay = relay(dir.path()).await;
    let host_key = Keypair::generate();
    let (_tunnel, _) = host_up(&relay, &host_key, "studio").await;
    let health: serde_json::Value = reqwest::get(format!("{}/health", url(&relay)))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(health["status"], "ok");
    let metrics = reqwest::get(format!("{}/metrics", url(&relay)))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains("oal_relay_hosts_online 1"), "{metrics}");
    assert!(!metrics.contains("studio"));
    assert!(!metrics.contains(&host_key.public_b64()));
    relay.shutdown().await;
}
