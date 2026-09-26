//! The host's tunnel to a relay (spec 4.4): a self-hosted `oal-relay`, or
//! any relay that speaks its protocol. The host dials out and keeps the
//! tunnel up; every client connection arrives through it, a pairing one
//! with the nameplate it came by. The relay proves nothing about a client
//! beyond its key: every connection is still end-to-end encrypted, and the
//! relay reads none of it.
//!
//! The host proves itself to the relay with its own static key, the one its
//! encryption uses, under its host id. The host's devices are the record of
//! who may come through: when the tunnel comes up, each device is paired at
//! the relay and every other key the relay lists is unpaired there.

use std::sync::Arc;
use std::time::{Duration, Instant};

use oal_relay::host::{HostEvent, HostHandle};
use oal_relay::wire::PairedClient;
use oal_relay::{Keypair, RelayClient};

use crate::{OalHost, Via, wire};

/// First wait after a tunnel fails; it doubles up to [`MAX_BACKOFF`].
const FIRST_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
/// A tunnel that lived this long earns a quick redial.
const STABLE: Duration = Duration::from_secs(60);

/// Keeps the host's tunnel to the relay at `url` up (redialing with backoff
/// and jitter) until the host shuts down.
pub async fn run(oal: Arc<OalHost>, url: String) {
    let key = Keypair::from_secret(*oal.keys().secret());
    let client = match RelayClient::new(&url, key) {
        Ok(client) => client,
        Err(e) => {
            tracing::error!(url, error = %e, "oal: the relay's address can't be used");
            return;
        }
    };
    let mut closing = oal.closing();
    let mut backoff = FIRST_BACKOFF;
    loop {
        let started = Instant::now();
        match client.host(oal.host_id()).await {
            Ok(mut tunnel) => {
                let handle = tunnel.handle();
                loop {
                    let event = tokio::select! {
                        event = tunnel.next() => event,
                        _ = crate::stopped(&mut closing) => {
                            handle.close();
                            oal.set_relay(None);
                            return;
                        }
                    };
                    let Some(event) = event else { break };
                    match event {
                        HostEvent::Registered { pairings, .. } => {
                            tracing::info!(url, "oal: the relay's tunnel is up");
                            oal.set_relay(Some(handle.clone()));
                            // Answers to requests arrive behind events: never
                            // wait for one here.
                            tokio::spawn(reconcile(oal.clone(), handle.clone(), pairings));
                        }
                        HostEvent::Client(conn) => {
                            let via = Via::Relay {
                                client_key: conn.client_key.clone(),
                                nameplate: conn.nameplate.clone(),
                            };
                            let oal = oal.clone();
                            tokio::spawn(async move { oal.serve(wire::relay(conn), via).await });
                        }
                        HostEvent::Unpaired { client_key } => {
                            tracing::info!(client_key, "oal: the relay's operator removed a device's way through the relay");
                        }
                    }
                }
                oal.set_relay(None);
                tracing::info!(url, "oal: the relay's tunnel closed; redialing");
            }
            Err(e) => tracing::info!(url, error = %e, "oal: could not open the relay's tunnel"),
        }
        let delay = if started.elapsed() > STABLE {
            backoff = FIRST_BACKOFF;
            FIRST_BACKOFF
        } else {
            let delay = backoff;
            backoff = (backoff * 2).min(MAX_BACKOFF);
            delay
        };
        tokio::select! {
            _ = tokio::time::sleep(jitter(delay)) => {}
            _ = crate::stopped(&mut closing) => return,
        }
    }
}

/// Makes the relay's pairings the host's devices: each device let through,
/// every other key the relay lists removed.
async fn reconcile(oal: Arc<OalHost>, handle: HostHandle, pairings: Vec<PairedClient>) {
    let devices: Vec<String> = oal.devices().into_iter().map(|d| d.key).collect();
    for key in devices.iter().filter(|key| !pairings.iter().any(|p| &p.client_key == *key)) {
        if let Err(e) = handle.paired(key).await {
            tracing::info!(error = %e, "oal: the relay did not take a device");
        }
    }
    for pairing in pairings.iter().filter(|p| !devices.contains(&p.client_key)) {
        if let Err(e) = handle.unpair(&pairing.client_key).await {
            tracing::info!(error = %e, "oal: the relay did not let a stale pairing go");
        }
    }
}

/// `delay`, give or take a quarter, so hosts that lost one relay don't all
/// come back at once.
fn jitter(delay: Duration) -> Duration {
    let mut byte = [0u8; 1];
    let _ = getrandom::getrandom(&mut byte);
    let quarter = delay / 4;
    delay - quarter + quarter * 2 * u32::from(byte[0]) / 255
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_within_a_quarter() {
        for _ in 0..100 {
            let d = jitter(Duration::from_secs(8));
            assert!(d >= Duration::from_secs(6) && d <= Duration::from_secs(10), "{d:?}");
        }
    }
}
