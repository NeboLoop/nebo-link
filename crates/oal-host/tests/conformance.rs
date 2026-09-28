//! The OAL conformance suite against the host, every connection end-to-end
//! encrypted: on the LAN (TLS with the host's pinned certificate) and
//! through a relay on this machine. Every example must pass.

mod common;

use std::time::Duration;

use oal_conformance::spec;
use oal_conformance::transcript::{self, Target};
use serde_json::json;

async fn passes(target: &Target) {
    let examples: Vec<_> = spec::EXAMPLES.iter().collect();
    let outcomes = transcript::run(target, &examples).await;
    for outcome in &outcomes {
        if let Err(e) = &outcome.result {
            panic!("{} failed: {e}", outcome.example);
        }
    }
    assert_eq!(outcomes.len(), examples.len(), "every example ran");
}

#[tokio::test]
async fn every_example_passes_on_the_lan() {
    let dir = tempfile::tempdir().unwrap();
    let oal = common::host(dir.path()).await;
    let lan = oal_host::lan::serve(oal.clone(), "127.0.0.1:0".parse().unwrap(), dir.path(), oal_host::lan::Reach::Lan { advertise: false })
        .await
        .unwrap();
    assert_eq!(oal.info()["host"]["tlsFingerprint"], lan.fingerprint.as_str());
    let code = oal.pairing_code().await.unwrap().to_string();
    let target = Target {
        url: format!("wss://127.0.0.1:{}/oal", lan.addr.port()),
        pair_url: None,
        headers: Vec::new(),
        known: vec![("code".into(), json!(code)), ("agent".into(), json!(common::AGENT)), ("runtime".into(), json!(common::RUNTIME))],
        e2e: true,
        relay: false,
        tls_fingerprint: Some(lan.fingerprint),
    };
    passes(&target).await;
    oal.shutdown();
}

#[tokio::test]
async fn every_example_passes_through_a_relay() {
    let dir = tempfile::tempdir().unwrap();
    let relay = oal_relay::server::start(oal_relay::server::Config::new(
        "127.0.0.1:0".parse().unwrap(),
        dir.path().join("relay"),
    ))
    .await
    .unwrap();
    let url = format!("http://{}", relay.local_addr());
    let oal = common::host(dir.path()).await;
    tokio::spawn(oal_host::relay::run(oal.clone(), url.clone()));
    for _ in 0..100 {
        if oal.relay_connected() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(oal.relay_connected(), "the tunnel came up");
    let code = oal.pairing_code().await.unwrap().to_string();
    let target = Target {
        url,
        pair_url: None,
        headers: Vec::new(),
        known: vec![("code".into(), json!(code)), ("agent".into(), json!(common::AGENT)), ("runtime".into(), json!(common::RUNTIME))],
        e2e: true,
        relay: true,
        tls_fingerprint: None,
    };
    passes(&target).await;
    oal.shutdown();
    relay.shutdown().await;
}
