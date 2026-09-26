//! NeboAI closes a removed bot's tunnel with 1008 "revoked". The service
//! unlinks the bot at once, without waiting for its comms connection to
//! be refused.
#![cfg(unix)]

mod common;

use common::Linked;
use common::hub::{FakeHub, PATIENCE, ROTATED_TOKEN, answer};
use futures::SinkExt;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

#[test]
fn a_tunnel_closed_as_revoked_unlinks() {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut hub = runtime.block_on(FakeHub::start());
    let linked = Linked::new(&hub.comms, &hub.tunnel);

    runtime.block_on(async {
        let service = tokio::time::timeout(PATIENCE, nebo_link::run::run(&linked.root, &linked.bot_id));
        let hub_side = async {
            let (mut comms, _) = hub.next_connect().await;
            answer(&mut comms, None).await;
            // The tunnel only dials once the rotated token is saved.
            let mut tunnel = hub.next_tunnel().await;
            linked.assert_linked();
            let saved = std::fs::read_to_string(linked.root.bot(&linked.bot_id).token_file()).unwrap();
            assert_eq!(saved.trim(), ROTATED_TOKEN);

            tunnel
                .send(Message::Close(Some(CloseFrame {
                    code: CloseCode::Policy,
                    reason: "revoked".into(),
                })))
                .await
                .unwrap();
            // The comms connection stays up: the tunnel's word is enough.
            comms
        };
        let (ended, _comms) = tokio::join!(service, hub_side);
        ended.expect("the service ended").unwrap();
    });
    linked.assert_removed();
    assert!(!hub.redialed(), "a removed bot does not dial again");
}
