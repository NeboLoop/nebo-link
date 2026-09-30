//! The processes a host starts, as the operating system sees them: whether
//! one runs, the tree under it and how much it works, and stopping all of
//! it. A host starts each agent in a process group of its own, so what the
//! agent starts goes with it; a stop reaches the group and every process
//! still under the agent, asked first and made to after a grace period.
//! Nothing a host started outlives it.
//!
//! A process the host started is recorded with when it started and when the
//! computer booted ([`Started`]), and is only ever stopped through that
//! record ([`stop_recorded`]): a pid the operating system gave to another
//! process since, after the process ended or the computer restarted, is
//! never signalled.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::{Duration, SystemTime};

/// How far apart two readings of one start time may be: the operating
/// system reports it to the second.
const SAME_SECOND: u64 = 2;
/// How far apart two readings of one boot time may be (Linux derives it
/// from the clock and the uptime).
const SAME_BOOT: u64 = 5;

/// Whether a process with this pid runs: it exists, and hasn't exited (one
/// that exited and its parent hasn't reaped yet runs nothing).
pub fn alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: kill with signal 0 only checks that the process exists.
        let rc = unsafe { libc::kill(pid as libc::pid_t, 0) };
        let exists = rc == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM);
        exists
            && !std::process::Command::new("ps")
                .args(["-o", "stat=", "-p", &pid.to_string()])
                .output()
                .is_ok_and(|o| String::from_utf8_lossy(&o.stdout).trim_start().starts_with('Z'))
    }
    #[cfg(not(unix))]
    {
        std::process::Command::new("tasklist")
            .args(["/FI", &format!("PID eq {pid}"), "/NH"])
            .output()
            .is_ok_and(|o| String::from_utf8_lossy(&o.stdout).contains(&format!(" {pid} ")))
    }
}

/// One process in the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Row {
    pid: u32,
    ppid: u32,
    pgid: u32,
    /// CPU time used so far.
    cpu: Duration,
    /// When it started, Unix seconds.
    started: u64,
    /// Exited, and not yet reaped by its parent.
    zombie: bool,
}

/// Every process on the computer: `ps` on Unix; `None` where there is none
/// to ask.
fn table() -> Option<Vec<Row>> {
    #[cfg(unix)]
    {
        let out = std::process::Command::new("ps")
            .args(["-A", "-o", "pid=,ppid=,pgid=,time=,etime=,stat="])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        let now = unix_now();
        Some(String::from_utf8_lossy(&out.stdout).lines().filter_map(|line| row(line, now)).collect())
    }
    #[cfg(not(unix))]
    {
        None
    }
}

fn row(line: &str, now: u64) -> Option<Row> {
    let mut fields = line.split_whitespace();
    Some(Row {
        pid: fields.next()?.parse().ok()?,
        ppid: fields.next()?.parse().ok()?,
        pgid: fields.next()?.parse().ok()?,
        cpu: clock(fields.next()?)?,
        started: now.saturating_sub(clock(fields.next()?)?.as_secs()),
        zombie: fields.next().is_some_and(|stat| stat.starts_with('Z')),
    })
}

/// `ps`'s `time` and `etime`: `[DD-]HH:MM:SS`, `MM:SS`, or with hundredths
/// (`M:SS.cc`, macOS's `time`).
fn clock(text: &str) -> Option<Duration> {
    let (days, rest) = match text.split_once('-') {
        Some((days, rest)) => (days.parse::<u64>().ok()?, rest),
        None => (0, text),
    };
    let mut seconds = 0.0f64;
    for part in rest.split(':') {
        seconds = seconds * 60.0 + part.parse::<f64>().ok()?;
    }
    Some(Duration::from_secs_f64(seconds + (days * 86_400) as f64))
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// When the computer booted, Unix seconds.
fn boot_time() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string("/proc/stat").ok()?;
        stat.lines().find_map(|l| l.strip_prefix("btime ")).and_then(|s| s.trim().parse().ok())
    }
    #[cfg(target_os = "macos")]
    {
        // `{ sec = 1790000000, usec = 123456 } Mon Sep 28 …`
        let out = std::process::Command::new("sysctl").args(["-n", "kern.boottime"]).output().ok()?;
        let text = String::from_utf8_lossy(&out.stdout).into_owned();
        let sec = text.split("sec = ").nth(1)?;
        sec.split(|c: char| !c.is_ascii_digit()).next()?.parse().ok()
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        None
    }
}

/// A process and everything under it, as it is now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tree {
    /// Every process under the root (its descendants, not the root itself).
    pub descendants: BTreeSet<u32>,
    /// What each process of the tree, the root included, has used so far.
    pub usage: BTreeMap<u32, Usage>,
}

/// What one process has used so far.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Usage {
    /// Its CPU time.
    pub cpu: Duration,
    /// When it started, Unix seconds.
    pub started: u64,
}

impl Row {
    fn usage(&self) -> Usage {
        Usage { cpu: self.cpu, started: self.started }
    }
}

/// The tree under `root`, with every process of its process group; `None`
/// when the root is gone or the process table can't be read here.
pub fn tree(root: u32) -> Option<Tree> {
    tree_in(&table()?, root)
}

fn tree_in(rows: &[Row], root: u32) -> Option<Tree> {
    let root_row = rows.iter().find(|r| r.pid == root)?;
    let mut children: HashMap<u32, Vec<&Row>> = HashMap::new();
    for row in rows {
        children.entry(row.ppid).or_default().push(row);
    }
    let mut tree = Tree {
        usage: BTreeMap::from([(root, root_row.usage())]),
        ..Tree::default()
    };
    let mut stack = vec![root];
    // What left the tree (its parent exited) but not the group is still the
    // agent's.
    stack.extend(rows.iter().filter(|r| r.pgid == root && r.pid != root).map(|r| r.pid));
    while let Some(pid) = stack.pop() {
        if pid != root {
            // An exited process its parent has not reaped yet runs nothing.
            if rows.iter().any(|r| r.pid == pid && r.zombie) {
                continue;
            }
            if !tree.descendants.insert(pid) {
                continue;
            }
            if let Some(row) = rows.iter().find(|r| r.pid == pid) {
                tree.usage.insert(pid, row.usage());
            }
        }
        stack.extend(children.get(&pid).into_iter().flatten().map(|c| c.pid).filter(|p| *p != root));
    }
    Some(tree)
}

/// Every process of the process group `pgid`.
fn group(rows: &[Row], pgid: u32) -> Vec<u32> {
    rows.iter().filter(|r| r.pgid == pgid).map(|r| r.pid).collect()
}

/// A pid no stop may ever signal: none, init, this process, and this
/// process's own group.
fn protected(pid: u32) -> bool {
    #[cfg(unix)]
    {
        // SAFETY: getpgrp has no preconditions and cannot fail.
        let own_group = unsafe { libc::getpgrp() } as u32;
        pid <= 1 || pid == std::process::id() || pid == own_group
    }
    #[cfg(not(unix))]
    {
        pid <= 4 || pid == std::process::id()
    }
}

/// Stops the process `pid`, which the caller started and still holds (so
/// the pid is still its: a child the caller hasn't reaped), leading a
/// process group of its own, and every process under it: each is asked to
/// end (SIGTERM), and after `grace` made to (SIGKILL). A process the caller
/// no longer holds is stopped through its record ([`stop_recorded`]).
pub async fn stop(pid: u32, grace: Duration) {
    if protected(pid) {
        tracing::warn!(pid, "process: refusing to stop this process, its group or init");
        return;
    }
    #[cfg(unix)]
    {
        let targets = table().map_or_else(
            || BTreeSet::from([pid]),
            |rows| {
                let mut all: BTreeSet<u32> = group(&rows, pid).into_iter().collect();
                if let Some(tree) = tree_in(&rows, pid) {
                    all.extend(tree.descendants);
                    all.insert(pid);
                }
                all
            },
        );
        end(Some(pid), targets, grace).await;
    }
    #[cfg(not(unix))]
    {
        let _ = grace;
        let _ = tokio::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T", "/F"])
            .output()
            .await;
    }
}

/// Asks `pids` (and the group `group`) to end, waits `grace`, makes what
/// still runs end, and waits for it to be gone.
#[cfg(unix)]
async fn end(group: Option<u32>, pids: BTreeSet<u32>, grace: Duration) {
    let pids: BTreeSet<u32> = pids.into_iter().filter(|p| !protected(*p)).collect();
    let group = group.filter(|g| !protected(*g));
    let signal = |pids: &BTreeSet<u32>, sig: libc::c_int| {
        if let Some(group) = group {
            // SAFETY: signalling a process group by id has no memory effects.
            unsafe { libc::kill(-(group as libc::pid_t), sig) };
        }
        for p in pids {
            // SAFETY: as above, for one process.
            unsafe { libc::kill(*p as libc::pid_t, sig) };
        }
    };
    // What still runs: an exited process its parent hasn't reaped yet runs
    // nothing.
    let left = |pids: &BTreeSet<u32>| match table() {
        Some(rows) => rows.iter().filter(|r| !r.zombie && pids.contains(&r.pid)).map(|r| r.pid).collect::<BTreeSet<u32>>(),
        None => pids.iter().copied().filter(|p| alive(*p)).collect(),
    };
    signal(&pids, libc::SIGTERM);
    let deadline = tokio::time::Instant::now() + grace;
    let mut remaining = left(&pids);
    while !remaining.is_empty() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
        remaining = left(&remaining);
    }
    if !remaining.is_empty() {
        tracing::info!(left = remaining.len(), "process: still running after the grace period; killed");
        signal(&remaining, libc::SIGKILL);
        // Killed is gone in a moment: wait for it, so what comes after (a
        // new start) never overlaps it.
        let killed = tokio::time::Instant::now() + Duration::from_secs(2);
        while !remaining.is_empty() && tokio::time::Instant::now() < killed {
            tokio::time::sleep(Duration::from_millis(20)).await;
            remaining = left(&remaining);
        }
    }
}

/// Kills the process `pid` leads and everything under it at once, with no
/// grace: what a host does with an agent it drops without stopping it. As
/// with [`stop`], the caller still holds the process.
pub fn kill_now(pid: u32) {
    if protected(pid) {
        tracing::warn!(pid, "process: refusing to kill this process, its group or init");
        return;
    }
    #[cfg(unix)]
    {
        let mut all: BTreeSet<u32> = BTreeSet::from([pid]);
        if let Some(rows) = table() {
            all.extend(group(&rows, pid));
            if let Some(tree) = tree_in(&rows, pid) {
                all.extend(tree.descendants);
            }
        }
        // SAFETY: signalling a process group and processes by id has no
        // memory effects.
        unsafe { libc::kill(-(pid as libc::pid_t), libc::SIGKILL) };
        for p in all.into_iter().filter(|p| !protected(*p)) {
            // SAFETY: as above.
            unsafe { libc::kill(p as libc::pid_t, libc::SIGKILL) };
        }
    }
    #[cfg(not(unix))]
    {
        let _ = std::process::Command::new("taskkill").args(["/PID", &pid.to_string(), "/T", "/F"]).output();
    }
}

/// A process a host started, as it records it: the pid, when the process
/// started and when the computer booted, so the record names that process
/// and no other, even after the pid is given to another.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Started {
    /// The process, and its process group.
    pub pid: u32,
    /// When it started, Unix seconds.
    pub started: u64,
    /// When the computer booted, Unix seconds; `None` in a record made where
    /// that can't be told (or by an earlier version), whose process group is
    /// then never stopped once its leader is gone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub boot: Option<u64>,
}

impl Started {
    /// The record of the process `pid` the caller just started.
    pub fn now(pid: u32) -> Self {
        let started = table()
            .and_then(|rows| rows.iter().find(|r| r.pid == pid).map(|r| r.started))
            .unwrap_or_else(unix_now);
        Self { pid, started, boot: boot_time() }
    }

    /// Whether the process it names still runs: the same one, started when
    /// recorded. Where the process table can't be read, whether the pid
    /// runs.
    pub fn running(&self) -> bool {
        match table() {
            Some(rows) => rows.iter().any(|r| self.is(r)),
            None => alive(self.pid),
        }
    }

    /// Whether `row` is the process this record names.
    fn is(&self, row: &Row) -> bool {
        row.pid == self.pid && !row.zombie && row.started.abs_diff(self.started) <= SAME_SECOND
    }

    /// Whether the computer hasn't restarted since the record was made.
    fn this_boot(&self) -> bool {
        matches!((self.boot, boot_time()), (Some(then), Some(now)) if then.abs_diff(now) <= SAME_BOOT)
    }
}

/// Stops what the record names, if it is still there: the process itself
/// (the same one: started when recorded) and everything under it; or, once
/// it exited, what is left of its process group, when the computer hasn't
/// restarted since and only those of the group that started after it did.
/// A pid given to another process is never signalled. Returns whether
/// anything was stopped.
pub async fn stop_recorded(record: &Started, grace: Duration) -> bool {
    if protected(record.pid) {
        return false;
    }
    #[cfg(unix)]
    {
        let Some(rows) = table() else {
            return false;
        };
        if rows.iter().any(|r| record.is(r)) {
            stop(record.pid, grace).await;
            return true;
        }
        if rows.iter().any(|r| r.pid == record.pid) || !record.this_boot() {
            return false;
        }
        let left: BTreeSet<u32> = rows
            .iter()
            .filter(|r| r.pgid == record.pid && !r.zombie && r.started + SAME_SECOND >= record.started)
            .map(|r| r.pid)
            .collect();
        if left.is_empty() {
            return false;
        }
        let mut all = left.clone();
        for pid in &left {
            if let Some(tree) = tree_in(&rows, *pid) {
                all.extend(tree.descendants.into_iter().filter(|d| rows.iter().any(|r| r.pid == *d && r.started + SAME_SECOND >= record.started)));
            }
        }
        end(None, all, grace).await;
        true
    }
    #[cfg(not(unix))]
    {
        if !alive(record.pid) {
            return false;
        }
        stop(record.pid, grace).await;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(table: &[(u32, u32, u32, u64)]) -> Vec<Row> {
        table
            .iter()
            .map(|&(pid, ppid, pgid, ms)| Row { pid, ppid, pgid, cpu: Duration::from_millis(ms), started: 0, zombie: false })
            .collect()
    }

    #[test]
    fn clocks_read_every_format() {
        assert_eq!(clock("0:03.04"), Some(Duration::from_millis(3040)));
        assert_eq!(clock("12:01.50"), Some(Duration::from_millis(721_500)));
        assert_eq!(clock("00:01:02"), Some(Duration::from_secs(62)));
        assert_eq!(clock("03:04"), Some(Duration::from_secs(184)));
        assert_eq!(clock("2-01:00:00"), Some(Duration::from_secs(2 * 86_400 + 3600)));
        assert_eq!(clock("x"), None);
        assert_eq!(row("  42  1  42  0:01.00  01:00 Ss", 1000).map(|r| (r.pid, r.started, r.zombie)), Some((42, 940, false)));
        assert_eq!(row("43 42 42 0:00.00 00:05 Z", 1000).map(|r| r.zombie), Some(true));
    }

    #[test]
    fn a_tree_is_everything_under_its_root_and_its_group() {
        let table = rows(&[
            (1, 0, 1, 0),
            (100, 1, 100, 1000),  // the agent, leading its group
            (101, 100, 100, 500), // its adapter
            (102, 101, 100, 200), // a session's process
            (103, 102, 103, 300), // a tool that made a group of its own
            (104, 1, 100, 50),    // left the tree when its parent exited
            (200, 1, 200, 9999),  // someone else's
        ]);
        let tree = tree_in(&table, 100).unwrap();
        assert_eq!(tree.descendants, BTreeSet::from([101, 102, 103, 104]));
        assert_eq!(tree.usage.keys().copied().collect::<Vec<_>>(), [100, 101, 102, 103, 104]);
        assert_eq!(tree.usage.values().map(|u| u.cpu).sum::<Duration>(), Duration::from_millis(2050));
        assert_eq!(tree_in(&table, 999), None);
        assert_eq!(group(&table, 100), vec![100, 101, 102, 104]);

        // A session's tool server that exited, not yet reaped, is not the agent's.
        let mut table = table;
        table.push(Row { pid: 105, ppid: 101, pgid: 100, cpu: Duration::ZERO, started: 0, zombie: true });
        assert_eq!(tree_in(&table, 100).unwrap().descendants, BTreeSet::from([101, 102, 103, 104]));
    }

    #[test]
    fn init_this_process_and_its_group_are_never_signalled() {
        assert!(protected(0) && protected(1) && protected(std::process::id()));
        #[cfg(unix)]
        {
            // SAFETY: getpgrp has no preconditions.
            assert!(protected(unsafe { libc::getpgrp() } as u32));
        }
        kill_now(std::process::id());
        kill_now(1);
        assert!(alive(std::process::id()));
    }

    #[tokio::test]
    async fn a_stop_ends_the_whole_tree_and_a_record_names_one_process_only() {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            // A shell leading its own group, with a child that ignores
            // SIGTERM: asked, then made to.
            let mut child = std::process::Command::new("sh")
                .args(["-c", "trap '' TERM; sleep 60 & wait"])
                .process_group(0)
                .spawn()
                .unwrap();
            let pid = child.id();
            let record = Started::now(pid);
            assert!(record.running());
            tokio::time::sleep(Duration::from_millis(200)).await;
            let under = tree(pid).unwrap().descendants;
            assert!(!under.is_empty(), "the sleep runs under the shell");
            // The same pid, started at another time, is another process.
            let other = Started { started: record.started - 60, ..record.clone() };
            assert!(!other.running());
            assert!(!stop_recorded(&other, Duration::from_millis(10)).await);
            assert!(alive(pid));

            assert!(stop_recorded(&record, Duration::from_millis(300)).await);
            let _ = child.wait();
            for p in under.iter().chain([&pid]) {
                assert!(!alive(*p), "{p} still runs");
            }
            assert!(!stop_recorded(&record, Duration::from_millis(300)).await, "nothing is left");
        }
    }

    /// The leader exited and left its group running: stopped only on the
    /// boot it was recorded on.
    #[tokio::test]
    async fn what_a_leader_left_is_stopped_on_its_own_boot_only() {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            let mut child = std::process::Command::new("sh")
                .args(["-c", "sleep 60 & exit 0"])
                .process_group(0)
                .spawn()
                .unwrap();
            let pid = child.id();
            let record = Started::now(pid);
            let _ = child.wait();
            tokio::time::sleep(Duration::from_millis(200)).await;
            let rows = table().unwrap();
            let left: Vec<u32> = group(&rows, pid);
            assert_eq!(left.len(), 1, "the sleep is left in the group");
            let before_boot = Started { boot: record.boot.map(|b| b - 3600), ..record.clone() };
            assert!(!stop_recorded(&before_boot, Duration::from_millis(10)).await, "another boot's record stops nothing");
            let unknown_boot = Started { boot: None, ..record.clone() };
            assert!(!stop_recorded(&unknown_boot, Duration::from_millis(10)).await);
            assert!(alive(left[0]));
            assert!(stop_recorded(&record, Duration::from_millis(300)).await);
            assert!(!alive(left[0]));
        }
    }
}
