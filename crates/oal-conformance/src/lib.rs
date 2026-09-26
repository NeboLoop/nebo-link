//! The Open Agent Link (OAL) conformance suite.
//!
//! - [`fake_agent`]: a scripted ACP agent. A host under test runs it as an
//!   ACP command (`oal-conformance agent`); the fake host runs it in process.
//! - [`fake_host`]: an OAL host serving the fake agent, for testing clients.
//! - [`transcript`]: a fake client that drives the recorded examples in
//!   `spec/examples/` against any host and reports where it differs.
//! - [`schema`]: checks frames against `spec/schemas/`.
//!
//! The spec is `spec/oal-0.1.md`.

pub mod fake_agent;
pub mod fake_host;
pub mod schema;
pub mod spec;
pub mod transcript;

/// The OAL version this suite tests.
pub const PROTOCOL: &str = "0.1";

/// Now, as RFC 3339 in UTC to the second.
pub fn now() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let (days, rem) = (secs / 86_400, secs % 86_400);
    // Civil date from days since 1970-01-01 (Howard Hinnant's algorithm).
    let z = days as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn now_is_rfc3339() {
        let now = super::now();
        assert_eq!(now.len(), 20, "{now}");
        assert!(now.starts_with("20") && now.ends_with('Z'), "{now}");
    }
}
