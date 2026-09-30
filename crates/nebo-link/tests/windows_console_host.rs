//! The Windows service runs `nebo-link` under the windowless console host
//! (`conhost.exe --headless`, see `service::windows_task`). This starts the
//! host exactly as the task's action says, with this test binary standing in
//! for `nebo-link run --bot …`, and checks what the service depends on:
//!
//! - the service runs, with a console of its own and no visible window;
//! - the host runs as long as the service does, and ends with it;
//! - a service that updates itself in place (starts its successor on the
//!   same console and exits) is still the host's, so still the task's run;
//! - ending the host (`schtasks /End`) ends the service.
//!
//! Its own `main` (`harness = false`): the stand-in is started with the
//! service's arguments, which the test harness would not accept.

#[cfg(not(windows))]
fn main() {}

#[cfg(windows)]
fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--bot") {
        windows::service();
    } else {
        windows::run_all();
    }
}

#[cfg(windows)]
mod windows {
    use std::ffi::c_void;
    use std::os::windows::process::CommandExt;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use nebo_link::service::{Spec, windows_task};

    const DIR: &str = "NEBO_LINK_HOST_TEST_DIR";
    const MODE: &str = "NEBO_LINK_HOST_TEST_MODE";
    const WAIT: Duration = Duration::from_secs(20);

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetConsoleWindow() -> *mut c_void;
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut c_void;
        fn WaitForSingleObject(handle: *mut c_void, millis: u32) -> u32;
        fn TerminateProcess(handle: *mut c_void, code: u32) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
    }

    #[link(name = "user32")]
    unsafe extern "system" {
        fn IsWindowVisible(window: *mut c_void) -> i32;
    }

    /// The stand-in for `nebo-link run --bot …`: says it started, whether it
    /// has a console and whether that console's window shows, then runs
    /// until told to stop. In `handover` it starts its successor on its own
    /// console instead, the way the service updates itself on Windows.
    pub fn service() {
        let dir = PathBuf::from(std::env::var_os(DIR).expect("test dir"));
        // SAFETY: no preconditions; a null window means no console.
        let window = unsafe { GetConsoleWindow() };
        // SAFETY: a null or console window handle is a valid argument.
        let visible = !window.is_null() && unsafe { IsWindowVisible(window) } != 0;
        let pid = std::process::id();
        std::fs::write(dir.join(format!("started-{pid}")), format!("console={} visible={visible}", !window.is_null()))
            .expect("write started");
        if std::env::var(MODE).as_deref() == Ok("handover") {
            // The successor outlives this process, as the updated service does.
            #[allow(clippy::zombie_processes)]
            let _successor = command::new::<std::process::Command>(std::env::current_exe().expect("exe"), command::Console::Inherit)
                .args(std::env::args_os().skip(1))
                .env(MODE, "stay")
                .spawn()
                .expect("successor starts");
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        while !dir.join("stop").exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn run_all() {
        runs_without_a_window_and_its_host_ends_with_it();
        a_service_that_updates_itself_is_still_the_hosts();
        ending_the_host_ends_the_service();
        println!("windows_console_host: all passed");
    }

    fn runs_without_a_window_and_its_host_ends_with_it() {
        let dir = fresh("plain");
        let mut host = start_host(&dir, "stay");
        let started = wait_started(&dir, 1);
        assert_eq!(started[0].1, "console=true visible=false", "the service has a console, and no window shows");
        std::thread::sleep(Duration::from_secs(1));
        assert!(host.try_wait().expect("host").is_none(), "the host runs while the service does");
        std::fs::write(dir.join("stop"), "").expect("stop");
        assert!(exits_within(&mut host, WAIT), "the host ends when the service does");
        println!("ok: the service runs without a window, and its host ends with it");
    }

    fn a_service_that_updates_itself_is_still_the_hosts() {
        let dir = fresh("handover");
        let mut host = start_host(&dir, "handover");
        let started = wait_started(&dir, 2);
        for (_, report) in &started {
            assert_eq!(report, "console=true visible=false", "neither the service nor its successor shows a window");
        }
        std::thread::sleep(Duration::from_secs(1));
        assert!(host.try_wait().expect("host").is_none(), "the host runs while the successor does");
        std::fs::write(dir.join("stop"), "").expect("stop");
        assert!(exits_within(&mut host, WAIT), "the host ends when the successor does");
        println!("ok: a service that updates itself in place is still the host's run");
    }

    fn ending_the_host_ends_the_service() {
        let dir = fresh("end");
        let mut host = start_host(&dir, "stay");
        let started = wait_started(&dir, 1);
        let pid = started[0].0;
        host.kill().expect("end the host");
        let _ = host.wait();
        // SAFETY: OpenProcess has no preconditions; SYNCHRONIZE | PROCESS_TERMINATE.
        let handle = unsafe { OpenProcess(0x0010_0000 | 0x0001, 0, pid) };
        if handle.is_null() {
            println!("ok: ending the host ended the service (already gone)");
            return;
        }
        // SAFETY: a process handle opened with SYNCHRONIZE.
        let ended = unsafe { WaitForSingleObject(handle, WAIT.as_millis() as u32) } == 0;
        if !ended {
            // SAFETY: opened with PROCESS_TERMINATE.
            unsafe { TerminateProcess(handle, 1) };
        }
        // SAFETY: a handle this function opened.
        unsafe { CloseHandle(handle) };
        assert!(ended, "ending the host ends the service");
        println!("ok: ending the host ends the service");
    }

    fn fresh(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("nebo-link-host-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("test dir");
        dir
    }

    /// Starts the console host as the task's action says, for this binary.
    fn start_host(dir: &Path, mode: &str) -> std::process::Child {
        let spec = Spec {
            bot_id: "b1".into(),
            exe: std::env::current_exe().expect("test binary"),
            home: Some(dir.join("home state")),
            path: None,
            logs: dir.join("logs"),
        };
        let task = windows_task(&spec, "owner");
        let host = expand(&unescape(between(&task, "<Command>", "</Command>")));
        let arguments = unescape(between(&task, "<Arguments>", "</Arguments>"));
        // Task Scheduler starts the action with no creation flags; the
        // console host opens no window of its own.
        command::new::<std::process::Command>(&host, command::Console::Inherit)
            .raw_arg(&arguments)
            .env(DIR, dir)
            .env(MODE, mode)
            .spawn()
            .unwrap_or_else(|e| panic!("start {host} {arguments}: {e}"))
    }

    /// The first `count` services to start: (pid, report).
    fn wait_started(dir: &Path, count: usize) -> Vec<(u32, String)> {
        let deadline = Instant::now() + WAIT;
        loop {
            let mut started: Vec<(u32, String)> = std::fs::read_dir(dir)
                .expect("read test dir")
                .filter_map(|entry| {
                    let entry = entry.ok()?;
                    let pid = entry.file_name().to_str()?.strip_prefix("started-")?.parse().ok()?;
                    Some((pid, std::fs::read_to_string(entry.path()).ok()?))
                })
                .filter(|(_, report)| !report.is_empty())
                .collect();
            if started.len() >= count {
                started.sort();
                return started;
            }
            assert!(Instant::now() < deadline, "the service did not start under the console host");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    fn exits_within(child: &mut std::process::Child, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        while Instant::now() < deadline {
            if child.try_wait().expect("host").is_some() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let _ = child.kill();
        false
    }

    fn between<'a>(text: &'a str, open: &str, close: &str) -> &'a str {
        let start = text.find(open).expect("open tag") + open.len();
        let end = start + text[start..].find(close).expect("close tag");
        &text[start..end]
    }

    fn unescape(text: &str) -> String {
        text.replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    }

    /// `%SystemRoot%`, as Task Scheduler expands it.
    fn expand(command: &str) -> String {
        let root = std::env::var("SystemRoot").expect("SystemRoot");
        command.replace("%SystemRoot%", &root)
    }
}
