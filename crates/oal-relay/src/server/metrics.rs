//! Counters for `/metrics`, in the Prometheus text format. Counts only: no
//! host ids, no keys, no content.

use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::pump::Traffic;

#[derive(Default)]
pub(crate) struct Metrics {
    pub(crate) hosts_online: AtomicU64,
    pub(crate) clients_connected: AtomicU64,
    pub(crate) host_connections: AtomicU64,
    pub(crate) client_connections: AtomicU64,
    pub(crate) pairings: AtomicU64,
    pub(crate) pairing_failures: AtomicU64,
    pub(crate) auth_failures: AtomicU64,
    pub(crate) refused_unpaired: AtomicU64,
    pub(crate) refused_offline: AtomicU64,
    pub(crate) nameplates_issued: AtomicU64,
    pub(crate) pairing_connections: AtomicU64,
    pub(crate) revocations: AtomicU64,
    /// Client WebSocket traffic: "from ws" is client to host.
    pub(crate) traffic: Arc<Traffic>,
}

pub(crate) fn inc(c: &AtomicU64) {
    c.fetch_add(1, Ordering::Relaxed);
}

pub(crate) fn dec(c: &AtomicU64) {
    c.fetch_sub(1, Ordering::Relaxed);
}

impl Metrics {
    pub(crate) fn render(&self) -> String {
        let g = |c: &AtomicU64| c.load(Ordering::Relaxed);
        let t = &self.traffic;
        let mut out = String::new();
        let mut metric = |name: &str, kind: &str, help: &str, samples: &[(&str, u64)]| {
            let _ = writeln!(out, "# HELP oal_relay_{name} {help}");
            let _ = writeln!(out, "# TYPE oal_relay_{name} {kind}");
            for (labels, value) in samples {
                let _ = writeln!(out, "oal_relay_{name}{labels} {value}");
            }
        };
        metric(
            "hosts_online",
            "gauge",
            "Hosts with a tunnel open.",
            &[("", g(&self.hosts_online))],
        );
        metric(
            "clients_connected",
            "gauge",
            "Client connections being relayed.",
            &[("", g(&self.clients_connected))],
        );
        metric(
            "host_connections_total",
            "counter",
            "Host tunnels opened.",
            &[("", g(&self.host_connections))],
        );
        metric(
            "client_connections_total",
            "counter",
            "Client connections relayed to a host, pairing connections included.",
            &[("", g(&self.client_connections))],
        );
        metric(
            "pairings_total",
            "counter",
            "Clients hosts reported as paired.",
            &[("", g(&self.pairings))],
        );
        metric(
            "pairing_failures_total",
            "counter",
            "Pairing attempts with an unknown or expired nameplate.",
            &[("", g(&self.pairing_failures))],
        );
        metric(
            "auth_failures_total",
            "counter",
            "Requests whose proof of key possession failed.",
            &[("", g(&self.auth_failures))],
        );
        metric(
            "refused_total",
            "counter",
            "Client connections refused after authentication.",
            &[
                ("{reason=\"not_paired\"}", g(&self.refused_unpaired)),
                ("{reason=\"host_offline\"}", g(&self.refused_offline)),
            ],
        );
        metric(
            "nameplates_issued_total",
            "counter",
            "Nameplates held for hosts.",
            &[("", g(&self.nameplates_issued))],
        );
        metric(
            "pairing_connections_total",
            "counter",
            "Pairing connections carried to a host.",
            &[("", g(&self.pairing_connections))],
        );
        metric(
            "revocations_total",
            "counter",
            "Pairings, clients and hosts revoked.",
            &[("", g(&self.revocations))],
        );
        metric(
            "messages_total",
            "counter",
            "WebSocket messages relayed.",
            &[
                ("{direction=\"to_host\"}", g(&t.from_ws_messages)),
                ("{direction=\"to_client\"}", g(&t.to_ws_messages)),
            ],
        );
        metric(
            "bytes_total",
            "counter",
            "WebSocket message bytes relayed.",
            &[
                ("{direction=\"to_host\"}", g(&t.from_ws_bytes)),
                ("{direction=\"to_client\"}", g(&t.to_ws_bytes)),
            ],
        );
        out
    }
}
