//! The host's own listeners, as a client dials them: this computer's apps
//! reach it on loopback, where nothing names or announces it; LAN direct is
//! named in `host/info`; either takes only a client that pins its
//! certificate.

mod common;

use oal_host::lan::{self, Reach};

#[tokio::test]
async fn this_computer_is_served_on_loopback_and_named_nowhere() {
    let dir = tempfile::tempdir().unwrap();
    let oal = common::host(dir.path()).await;
    let refused = lan::serve(oal.clone(), "0.0.0.0:0".parse().unwrap(), dir.path(), Reach::Machine).await;
    assert!(refused.is_err(), "this computer alone is served on loopback");

    let machine = lan::serve(oal.clone(), "127.0.0.1:0".parse().unwrap(), dir.path(), Reach::Machine).await.unwrap();
    assert!(machine.addr.ip().is_loopback());
    assert!(oal.info()["host"].get("tlsFingerprint").is_none(), "host/info names LAN direct only");
    lan::dial(machine.addr, &machine.fingerprint).await.expect("the pinned certificate is accepted");
    let wrong = "A".repeat(machine.fingerprint.len());
    assert!(lan::dial(machine.addr, &wrong).await.is_err(), "another certificate is refused");

    // LAN direct, from the same certificate, is named.
    let on_lan = lan::serve(oal.clone(), "127.0.0.1:0".parse().unwrap(), dir.path(), Reach::Lan { advertise: false }).await.unwrap();
    assert_eq!(on_lan.fingerprint, machine.fingerprint);
    assert_eq!(oal.info()["host"]["tlsFingerprint"], machine.fingerprint.as_str());

    // Shutting down closes both: nothing answers there any more.
    oal.shutdown();
    let closed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while lan::dial(machine.addr, &machine.fingerprint).await.is_ok() {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(closed.is_ok(), "the listener stops with the host");
}
