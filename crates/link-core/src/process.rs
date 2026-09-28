//! The processes a host starts, as the operating system sees them: whether
//! one is alive, the tree under it and how much it works, and stopping all
//! of it. A host starts each agent in a process group of its own, so what the
//! agent starts goes with it; a stop reaches the group and every process
//! still under the agent, asked first and made to after a grace period.
//! Nothing a host started outlives it.

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

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
    /// Exited, and not yet reaped by its parent.
    zombie: bool,
}

/// Every process on the computer: `ps` on Unix; `None` where there is none
/// to ask.
fn table() -> Option<Vec<Row>> {
    #[cfg(unix)]
    {
        let out = std::process::Command::new("ps")
            .args(["-A", "-o", "pid=,ppid=,pgid=,time=,stat="])
            .output()
            .ok()?;
        if !out.status.success() {
            return None;
        }
        Some(String::from_utf8_lossy(&out.stdout).lines().filter_map(row).collect())
    }
    #[cfg(not(unix))]
    {
        None
    }
}

fn row(line: &str) -> Option<Row> {
    let mut fields = line.split_whitespace();
    Some(Row {
        pid: fields.next()?.parse().ok()?,
        ppid: fields.next()?.parse().ok()?,
        pgid: fields.next()?.parse().ok()?,
        cpu: cpu_time(fields.next()?)?,
        zombie: fields.next().is_some_and(|stat| stat.starts_with('Z')),
    })
}

/// `ps`'s `time`: `[DD-]HH:MM:SS` (Linux) or `M:SS.cc` (macOS).
fn cpu_time(text: &str) -> Option<Duration> {
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

/// A process and everything under it, as it is now.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Tree {
    /// Every process under the root (its descendants, not the root itself).
    pub descendants: BTreeSet<u32>,
    /// The CPU time the root and its descendants have used so far.
    pub cpu: Duration,
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
        cpu: root_row.cpu,
        ..Tree::default()
    };
    let mut stack = vec![root];
    // What left the tree (its parent exited) but not the group is still the
    // agent's.
    stack.extend(rows.iter().filter(|r| r.pgid == root && r.pid != root).map(|r| r.pid));
    while let Some(pid) = stack.pop() {
        if pid != root {
            if !tree.descendants.insert(pid) {
                continue;
            }
            tree.cpu += rows.iter().find(|r| r.pid == pid).map_or(Duration::ZERO, |r| r.cpu);
        }
        stack.extend(children.get(&pid).into_iter().flatten().map(|c| c.pid).filter(|p| *p != root));
    }
    Some(tree)
}

/// Every process of the process group `pgid`.
fn group(rows: &[Row], pgid: u32) -> Vec<u32> {
    rows.iter().filter(|r| r.pgid == pgid).map(|r| r.pid).collect()
}

/// When a process started, as the operating system says: with its pid, what
/// tells it apart from a later process that reuses the pid.
pub fn started(pid: u32) -> Option<String> {
    #[cfg(unix)]
    {
        let out = std::process::Command::new("ps")
            .args(["-o", "lstart=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&out.stdout).trim().to_owned();
        (out.status.success() && !text.is_empty()).then_some(text)
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        None
    }
}

/// Stops the process `pid` leads (a process group of its own) and every
/// process under it: each is asked to end (SIGTERM), and after `grace` made
/// to (SIGKILL).
pub async fn stop(pid: u32, grace: Duration) {
    #[cfg(unix)]
    {
        let targets = |rows: &[Row]| -> BTreeSet<u32> {
            let mut all: BTreeSet<u32> = group(rows, pid).into_iter().collect();
            if let Some(tree) = tree_in(rows, pid) {
                all.extend(tree.descendants);
            }
            if rows.iter().any(|r| r.pid == pid) {
                all.insert(pid);
            }
            all
        };
        let signal = |pids: &BTreeSet<u32>, sig: libc::c_int| {
            // SAFETY: signalling a process group and processes by id has no
            // memory effects.
            unsafe { libc::kill(-(pid as libc::pid_t), sig) };
            for p in pids {
                // SAFETY: as above.
                unsafe { libc::kill(*p as libc::pid_t, sig) };
            }
        };
        let first = table().map(|rows| targets(&rows)).unwrap_or_else(|| BTreeSet::from([pid]));
        signal(&first, libc::SIGTERM);
        let deadline = tokio::time::Instant::now() + grace;
        // What still runs: an exited process its parent hasn't reaped yet
        // runs nothing.
        let left = |pids: &BTreeSet<u32>| match table() {
            Some(rows) => rows.iter().filter(|r| !r.zombie && pids.contains(&r.pid)).map(|r| r.pid).collect::<BTreeSet<u32>>(),
            None => pids.iter().copied().filter(|p| alive(*p)).collect(),
        };
        let mut remaining = left(&first);
        while !remaining.is_empty() && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
            remaining = left(&remaining);
        }
        if !remaining.is_empty() {
            tracing::info!(pid, left = remaining.len(), "process: still running after the grace period; killed");
            signal(&remaining, libc::SIGKILL);
            // Killed is gone in a moment: wait for it, so what comes after
            // (a new start) never overlaps it.
            let killed = tokio::time::Instant::now() + Duration::from_secs(2);
            while !remaining.is_empty() && tokio::time::Instant::now() < killed {
                tokio::time::sleep(Duration::from_millis(20)).await;
                remaining = left(&remaining);
            }
        }
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

/// Kills the process `pid` leads and everything under it at once, with no
/// grace: what a host does with an agent it drops without stopping it.
pub fn kill_now(pid: u32) {
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
        for p in all {
            // SAFETY: as above.
            unsafe { libc::kill(p as libc::pid_t, libc::SIGKILL) };
        }
    }
    #[cfg(not(unix))]
    {
        let _ = std::process::Command::new("taskkill").args(["/PID", &pid.to_string(), "/T", "/F"]).output();
    }
}

/// A process a host started, as it records it, so the next run of the host
/// stops what a run that crashed left behind ([`stop_left_behind`]).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Started {
    /// The process, and its process group.
    pub pid: u32,
    /// When it started ([`started`]).
    pub at: String,
}

impl Started {
    /// The record of the process `pid` just started, when the operating
    /// system says when it did.
    pub fn now(pid: u32) -> Option<Self> {
        Some(Self { pid, at: started(pid)? })
    }
}

/// Stops what the record `left` names, if it is still there: the process
/// itself (the same one: it started when recorded), or what is left of its
/// process group once it exited (no other process can take the group's id
/// while any of it runs). Returns whether anything was stopped.
pub async fn stop_left_behind(left: &Started, grace: Duration) -> bool {
    let Some(rows) = table() else {
        return false;
    };
    let leader = rows.iter().any(|r| r.pid == left.pid);
    let ours = match leader {
        true => started(left.pid).as_deref() == Some(left.at.as_str()),
        false => !group(&rows, left.pid).is_empty(),
    };
    if ours {
        stop(left.pid, grace).await;
    }
    ours
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(table: &[(u32, u32, u32, u64)]) -> Vec<Row> {
        table
            .iter()
            .map(|&(pid, ppid, pgid, ms)| Row { pid, ppid, pgid, cpu: Duration::from_millis(ms), zombie: false })
            .collect()
    }

    #[test]
    fn cpu_time_reads_both_formats() {
        assert_eq!(cpu_time("0:03.04"), Some(Duration::from_millis(3040)));
        assert_eq!(cpu_time("12:01.50"), Some(Duration::from_millis(721_500)));
        assert_eq!(cpu_time("00:01:02"), Some(Duration::from_secs(62)));
        assert_eq!(cpu_time("2-01:00:00"), Some(Duration::from_secs(2 * 86_400 + 3600)));
        assert_eq!(cpu_time("x"), None);
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
        assert_eq!(tree.cpu, Duration::from_millis(2050));
        assert_eq!(tree_in(&table, 999), None);
        assert_eq!(group(&table, 100), vec![100, 101, 102, 104]);
    }

    #[tokio::test]
    async fn a_stop_ends_the_whole_tree_and_a_record_finds_what_was_left() {
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
            let record = Started::now(pid).expect("recorded");
            tokio::time::sleep(Duration::from_millis(200)).await;
            let under = tree(pid).unwrap().descendants;
            assert!(!under.is_empty(), "the sleep runs under the shell");
            assert!(stop_left_behind(&record, Duration::from_millis(300)).await);
            let _ = child.wait();
            for p in under.iter().chain([&pid]) {
                assert!(!alive(*p), "{p} still runs");
            }
            assert!(!stop_left_behind(&record, Duration::from_millis(300)).await, "nothing is left");
            // A record whose pid now names another process stops nothing.
            let other = Started { pid: std::process::id(), at: "never".into() };
            assert!(!stop_left_behind(&other, Duration::from_millis(10)).await);
            assert!(alive(std::process::id()));
        }
    }
}
