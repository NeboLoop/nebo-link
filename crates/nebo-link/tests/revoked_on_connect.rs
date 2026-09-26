//! NeboAI drops a removed bot's connection and refuses its next CONNECT
//! (AUTH_FAIL "bot has been revoked"). The drop alone is routine and the
//! service redials; the refusal ends it: the service unlinks the bot and
//! stops retrying.
#![cfg(unix)]

mod common;

use common::Linked;
use common::hub::{FakeHub, PATIENCE, answer};

#[test]
fn a_dropped_connection_is_redialed_and_a_revoked_connect_unlinks() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut hub = runtime.block_on(FakeHub::start());
    let linked = Linked::new(&hub.comms, &hub.tunnel);

    runtime.block_on(async {
        let service = tokio::time::timeout(PATIENCE, nebo_link::run::run(&linked.root, &linked.bot_id));
        let hub_side = async {
            let (mut ws, _) = hub.next_connect().await;
            answer(&mut ws, None).await;
            let tunnel = hub.next_tunnel().await;

            // The hub drops the connection, as a pod roll (or a revoke) does.
            drop(ws);
            let (mut ws, _) = hub.next_connect().await;
            linked.assert_linked();

            answer(&mut ws, Some("bot has been revoked")).await;
            (ws, tunnel)
        };
        let (ended, _connections) = tokio::join!(service, hub_side);
        ended.expect("the service ended").unwrap();
    });
    linked.assert_removed();
    assert!(!hub.redialed(), "a removed bot does not dial again");
}
