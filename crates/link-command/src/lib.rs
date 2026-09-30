//! Every child process the link starts is made here: [`new`] is the one
//! constructor, for `std::process::Command` and `tokio::process::Command`
//! alike. There is no other way.
//!
//! Why this exists: on Windows, a console program started by a process that
//! has no console of its own (an app embedding link-core, or the link run as
//! a background task) gets a NEW console window — one terminal flashing open
//! per agent, `npx` adapter, `tasklist` or `taskkill` the link starts. The fix
//! is one creation flag, `CREATE_NO_WINDOW`, and no site set it. Here it is
//! not something a caller can forget: the caller names how the child relates
//! to the console, and `tests/one_constructor.rs` fails the build on a
//! `Command::new` or `creation_flags` anywhere else.
//!
//! On macOS and Linux nothing changes: a child gets no window of its own
//! there, and the choice is a no-op.

use std::ffi::OsStr;

/// How a child relates to the console. Only Windows acts on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Console {
    /// No console window (`CREATE_NO_WINDOW`): the child gets a hidden
    /// console of its own, which what it starts in turn shares, so none of
    /// them opens a window either. Every child the link starts, unless it
    /// takes the link's own place (below). Its piped stdio still works.
    Hidden,
    /// Shares the link's own console: no flag at all. Only for the link
    /// starting itself over after an update, which takes this process's
    /// place (what `exec` does on Unix) and keeps its terminal.
    Inherit,
}

impl Console {
    /// The Windows process-creation flags for this choice.
    pub const fn creation_flags(self) -> u32 {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        match self {
            Console::Hidden => CREATE_NO_WINDOW,
            Console::Inherit => 0,
        }
    }
}

/// A command for `program`, set up for `console`. `C` is
/// `std::process::Command` or `tokio::process::Command`:
///
/// ```
/// use link_command::{self as command, Console};
/// let sync: std::process::Command = command::new("npx", Console::Hidden);
/// let not_sync = command::new::<tokio::process::Command>("npx", Console::Hidden);
/// # let _ = (sync, not_sync);
/// ```
pub fn new<C: From<std::process::Command>>(program: impl AsRef<OsStr>, console: Console) -> C {
    let mut cmd = std::process::Command::new(program);
    set_creation_flags(&mut cmd, console.creation_flags());
    C::from(cmd)
}

#[cfg(windows)]
fn set_creation_flags(cmd: &mut std::process::Command, flags: u32) {
    use std::os::windows::process::CommandExt;
    cmd.creation_flags(flags);
}

/// Creation flags are Windows's own; a child elsewhere never gets a window,
/// so there is nothing to set.
#[cfg(not(windows))]
fn set_creation_flags(_cmd: &mut std::process::Command, _flags: u32) {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_child_is_hidden_unless_it_takes_the_links_place() {
        assert_eq!(Console::Hidden.creation_flags(), 0x0800_0000);
        assert_eq!(Console::Inherit.creation_flags(), 0);
    }

    #[test]
    fn a_hidden_child_still_runs_and_its_output_is_read() {
        let (program, args): (&str, &[&str]) =
            if cfg!(windows) { ("cmd", &["/C", "echo link"]) } else { ("sh", &["-c", "echo link"]) };
        let out = new::<std::process::Command>(program, Console::Hidden).args(args).output().expect("runs");
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "link");
    }

    #[tokio::test]
    async fn the_tokio_command_keeps_what_was_set() {
        let (program, args): (&str, &[&str]) =
            if cfg!(windows) { ("cmd", &["/C", "echo link"]) } else { ("sh", &["-c", "echo link"]) };
        let out = new::<tokio::process::Command>(program, Console::Hidden)
            .args(args)
            .kill_on_drop(true)
            .output()
            .await
            .expect("runs");
        assert!(out.status.success());
        assert_eq!(String::from_utf8_lossy(&out.stdout).trim(), "link");
    }
}
