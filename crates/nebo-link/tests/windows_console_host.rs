//! The Windows service runs `nebo-link` under the windowless console host
//! (`conhost.exe --headless`, see `service::windows_task`). This runs the
//! task's action with this test binary standing in for `nebo-link run --bot
//! …`, and checks what the service depends on:
//!
//! - the service runs, with a console of its own and no visible window;
//! - the host runs as long as the service does, and ends with it;
//! - a service that updates itself in place (starts its successor on the
//!   same console and exits) is still the host's, so still the task's run;
//! - ending the host ends the service;
//! - registered with Task Scheduler as `service::windows_task` writes it:
//!   the task runs without a window, `schtasks /End` ends the service, and
//!   a run asked for while one runs starts when it ends (`Queue`).
//!
//! The host is started the way Task Scheduler starts it: a program with no
//! standard handles. (Given handles, the console host takes them for a
//! pseudoconsole's pipes, which is not how the task runs it.)
//!
//! Its own `main` (`harness = false`): the stand-in is started with the
//! service's arguments, which the test harness would not accept. The
//! stand-in reads its test folder from `--home` (its parent) and what to do
//! from `--bot`: Task Scheduler passes no environment of the test's.

#[cfg(not(windows))]
fn main() {}

#[cfg(windows)]
fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.iter().any(|a| a == "--bot") {
        windows::service(&args);
    } else {
        windows::run_all();
    }
}

#[cfg(windows)]
mod windows {
    use std::collections::BTreeMap;
    use std::ffi::c_void;
    use std::path::{Path, PathBuf};
    use std::time::{Duration, Instant};

    use nebo_link::service::{Spec, windows_task};

    const WAIT: Duration = Duration::from_secs(30);
    const SYNCHRONIZE: u32 = 0x0010_0000;
    const PROCESS_TERMINATE: u32 = 0x0001;
    const STARTF_USESTDHANDLES: u32 = 0x0000_0100;

    #[repr(C)]
    struct StartupInfo {
        cb: u32,
        reserved: *mut u16,
        desktop: *mut u16,
        title: *mut u16,
        x: u32,
        y: u32,
        x_size: u32,
        y_size: u32,
        x_count_chars: u32,
        y_count_chars: u32,
        fill_attribute: u32,
        flags: u32,
        show_window: u16,
        reserved2_size: u16,
        reserved2: *mut u8,
        std_input: *mut c_void,
        std_output: *mut c_void,
        std_error: *mut c_void,
    }

    #[repr(C)]
    struct ProcessInformation {
        process: *mut c_void,
        thread: *mut c_void,
        pid: u32,
        tid: u32,
    }

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetConsoleWindow() -> *mut c_void;
        fn GetConsoleProcessList(pids: *mut u32, count: u32) -> u32;
        fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut c_void;
        fn WaitForSingleObject(handle: *mut c_void, millis: u32) -> u32;
        fn TerminateProcess(handle: *mut c_void, code: u32) -> i32;
        fn CloseHandle(handle: *mut c_void) -> i32;
        fn CreateProcessW(
            application: *const u16,
            command_line: *mut u16,
            process_attributes: *const c_void,
            thread_attributes: *const c_void,
            inherit_handles: i32,
            creation_flags: u32,
            environment: *const c_void,
            current_directory: *const u16,
            startup_info: *const StartupInfo,
            process_information: *mut ProcessInformation,
        ) -> i32;
    }

    #[link(name = "user32")]
    unsafe extern "system" {
        fn IsWindowVisible(window: *mut c_void) -> i32;
    }

    fn arg_after<'a>(args: &'a [String], flag: &str) -> &'a str {
        let at = args.iter().position(|a| a == flag).unwrap_or_else(|| panic!("{flag} in {args:?}"));
        &args[at + 1]
    }

    /// The stand-in for `nebo-link run --bot …`: says it started, whether it
    /// has a console and whether that console's window shows, then runs
    /// until `stop-<pid>` appears. As `--bot handover` it starts its
    /// successor on its own console instead, the way the service updates
    /// itself on Windows.
    pub fn service(args: &[String]) {
        let dir = Path::new(arg_after(args, "--home")).parent().expect("test dir").to_path_buf();
        let mode = arg_after(args, "--bot");
        let mut sharing = [0u32; 16];
        // SAFETY: a buffer of the length given; 0 means no console.
        let console = unsafe { GetConsoleProcessList(sharing.as_mut_ptr(), sharing.len() as u32) } != 0;
        // SAFETY: no preconditions. A headless host's window, where it makes
        // one, is never shown; off a desktop it makes none (null).
        let window = unsafe { GetConsoleWindow() };
        // SAFETY: a null or console window handle is a valid argument.
        let visible = !window.is_null() && unsafe { IsWindowVisible(window) } != 0;
        let pid = std::process::id();
        let report = format!("console={console} visible={visible}");
        std::fs::write(dir.join(format!("started-{pid}")), report).expect("write started");
        if mode == "handover" {
            let mut next: Vec<String> = args[1..].to_vec();
            let at = next.iter().position(|a| a == "handover").expect("mode");
            next[at] = "stay".into();
            // The successor outlives this process, as the updated service does.
            #[allow(clippy::zombie_processes)]
            let _successor =
                command::new::<std::process::Command>(std::env::current_exe().expect("exe"), command::Console::Inherit)
                    .args(next)
                    .spawn()
                    .expect("successor starts");
            return;
        }
        let deadline = Instant::now() + Duration::from_secs(120);
        while !dir.join(format!("stop-{pid}")).exists() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn run_all() {
        runs_without_a_window_and_its_host_ends_with_it();
        a_service_that_updates_itself_is_still_the_hosts();
        ending_the_host_ends_the_service();
        the_registered_task_runs_it_without_a_window();
        println!("windows_console_host: all passed");
    }

    fn runs_without_a_window_and_its_host_ends_with_it() {
        let dir = fresh("plain");
        let host = Process::start_host(&dir, "stay");
        let started = wait_started(&dir, 1);
        let (pid, report) = started.iter().next().expect("one");
        assert_eq!(report, "console=true visible=false", "the service has a console, and no window shows");
        assert!(!host.ended_within(Duration::from_secs(1)), "the host runs while the service does");
        stop(&dir, *pid);
        assert!(host.ended_within(WAIT), "the host ends when the service does");
        println!("ok: the service runs without a window, and its host ends with it");
    }

    fn a_service_that_updates_itself_is_still_the_hosts() {
        let dir = fresh("handover");
        let host = Process::start_host(&dir, "handover");
        let started = wait_started(&dir, 2);
        for report in started.values() {
            assert_eq!(report, "console=true visible=false", "neither the service nor its successor shows a window");
        }
        assert!(!host.ended_within(Duration::from_secs(1)), "the host runs while the successor does");
        for pid in started.keys() {
            stop(&dir, *pid);
        }
        assert!(host.ended_within(WAIT), "the host ends when the successor does");
        println!("ok: a service that updates itself in place is still the host's run");
    }

    fn ending_the_host_ends_the_service() {
        let dir = fresh("end");
        let host = Process::start_host(&dir, "stay");
        let started = wait_started(&dir, 1);
        let service = Process::open(*started.keys().next().expect("one"));
        host.terminate();
        let ended = service.as_ref().is_none_or(|s| s.ended_within(WAIT));
        if let Some(service) = service.filter(|_| !ended) {
            service.terminate();
        }
        assert!(ended, "ending the host ends the service");
        println!("ok: ending the host ends the service");
    }

    /// The task exactly as `service::windows_task` writes it, registered
    /// and run by Task Scheduler.
    fn the_registered_task_runs_it_without_a_window() {
        let dir = fresh("task");
        let task = format!("NeboLinkTest-{}", std::process::id());
        let file = dir.join("task.xml");
        let user = match (std::env::var("USERDOMAIN"), std::env::var("USERNAME")) {
            (Ok(domain), Ok(user)) => format!("{domain}\\{user}"),
            (_, user) => user.expect("USERNAME"),
        };
        let mut bytes = vec![0xFF, 0xFE];
        bytes.extend(windows_task(&spec(&dir, "stay"), &user).encode_utf16().flat_map(u16::to_le_bytes));
        std::fs::write(&file, bytes).expect("task file");
        schtasks(&["/Create", "/TN", &task, "/XML", &file.display().to_string(), "/F"]);
        let result = std::panic::catch_unwind(|| {
            schtasks(&["/Run", "/TN", &task]);
            let first = wait_started(&dir, 1);
            let (&pid, report) = first.iter().next().expect("one");
            assert_eq!(report, "console=true visible=false", "the task's service has a console, and no window shows");

            // Queue: a run asked for while one runs starts when it ends.
            schtasks(&["/Run", "/TN", &task]);
            std::thread::sleep(Duration::from_secs(3));
            assert_eq!(started_now(&dir).len(), 1, "a run asked for while one runs waits");
            stop(&dir, pid);
            let second = wait_started(&dir, 2);
            let (&next, report) = second.iter().find(|(p, _)| **p != pid).expect("the queued run");
            assert_eq!(report, "console=true visible=false");
            println!("ok: a run asked for while the task runs starts when it ends, without a window");

            // `schtasks /End` ends the service.
            let service = Process::open(next);
            schtasks(&["/End", "/TN", &task]);
            let ended = service.as_ref().is_none_or(|s| s.ended_within(WAIT));
            if let Some(service) = service.filter(|_| !ended) {
                service.terminate();
            }
            assert!(ended, "schtasks /End ends the service");
            println!("ok: the registered task runs the service without a window, and /End ends it");
        });
        let _ = command::new::<std::process::Command>("schtasks", command::Console::Hidden)
            .args(["/Delete", "/TN", &task, "/F"])
            .output();
        if let Err(panic) = result {
            std::panic::resume_unwind(panic);
        }
    }

    fn spec(dir: &Path, mode: &str) -> Spec {
        Spec {
            bot_id: mode.into(),
            exe: std::env::current_exe().expect("test binary"),
            home: Some(dir.join("home state")),
            path: None,
            logs: dir.join("logs"),
        }
    }

    fn schtasks(args: &[&str]) {
        let out = command::new::<std::process::Command>("schtasks", command::Console::Hidden)
            .args(args)
            .output()
            .expect("schtasks runs");
        assert!(
            out.status.success(),
            "schtasks {args:?}: {} {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn fresh(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("nebo-link-host-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("test dir");
        dir
    }

    fn stop(dir: &Path, pid: u32) {
        std::fs::write(dir.join(format!("stop-{pid}")), "").expect("stop");
    }

    fn started_now(dir: &Path) -> BTreeMap<u32, String> {
        std::fs::read_dir(dir)
            .expect("read test dir")
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let pid = entry.file_name().to_str()?.strip_prefix("started-")?.parse().ok()?;
                Some((pid, std::fs::read_to_string(entry.path()).ok()?))
            })
            .filter(|(_, report)| !report.is_empty())
            .collect()
    }

    /// The services started so far, once there are `count`: pid → report.
    fn wait_started(dir: &Path, count: usize) -> BTreeMap<u32, String> {
        let deadline = Instant::now() + WAIT;
        loop {
            let started = started_now(dir);
            if started.len() >= count {
                return started;
            }
            assert!(Instant::now() < deadline, "the service did not start under the console host ({started:?})");
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    struct Process {
        handle: *mut c_void,
    }

    impl Process {
        /// Runs the task's action (the console host and its arguments) as
        /// Task Scheduler does: no standard handles, none inherited.
        fn start_host(dir: &Path, mode: &str) -> Process {
            let task = windows_task(&spec(dir, mode), "owner");
            let host = unescape(between(&task, "<Command>", "</Command>"))
                .replace("%SystemRoot%", &std::env::var("SystemRoot").expect("SystemRoot"));
            let arguments = unescape(between(&task, "<Arguments>", "</Arguments>"));
            let mut line: Vec<u16> = format!("\"{host}\" {arguments}").encode_utf16().chain([0]).collect();
            let info = StartupInfo {
                cb: std::mem::size_of::<StartupInfo>() as u32,
                reserved: std::ptr::null_mut(),
                desktop: std::ptr::null_mut(),
                title: std::ptr::null_mut(),
                x: 0,
                y: 0,
                x_size: 0,
                y_size: 0,
                x_count_chars: 0,
                y_count_chars: 0,
                fill_attribute: 0,
                flags: STARTF_USESTDHANDLES,
                show_window: 0,
                reserved2_size: 0,
                reserved2: std::ptr::null_mut(),
                std_input: std::ptr::null_mut(),
                std_output: std::ptr::null_mut(),
                std_error: std::ptr::null_mut(),
            };
            let mut process = ProcessInformation {
                process: std::ptr::null_mut(),
                thread: std::ptr::null_mut(),
                pid: 0,
                tid: 0,
            };
            // SAFETY: a NUL-terminated, writable command line; valid startup
            // and process information structs; no handles inherited.
            let ok = unsafe {
                CreateProcessW(
                    std::ptr::null(),
                    line.as_mut_ptr(),
                    std::ptr::null(),
                    std::ptr::null(),
                    0,
                    0,
                    std::ptr::null(),
                    std::ptr::null(),
                    &info,
                    &mut process,
                )
            };
            assert!(ok != 0, "start {host} {arguments}: {}", std::io::Error::last_os_error());
            // SAFETY: a thread handle CreateProcessW returned.
            unsafe { CloseHandle(process.thread) };
            Process { handle: process.process }
        }

        fn open(pid: u32) -> Option<Process> {
            // SAFETY: OpenProcess has no preconditions.
            let handle = unsafe { OpenProcess(SYNCHRONIZE | PROCESS_TERMINATE, 0, pid) };
            (!handle.is_null()).then_some(Process { handle })
        }

        fn ended_within(&self, wait: Duration) -> bool {
            // SAFETY: a process handle with SYNCHRONIZE access.
            unsafe { WaitForSingleObject(self.handle, wait.as_millis() as u32) == 0 }
        }

        fn terminate(&self) {
            // SAFETY: a process handle with PROCESS_TERMINATE access.
            unsafe { TerminateProcess(self.handle, 1) };
            let _ = self.ended_within(WAIT);
        }
    }

    impl Drop for Process {
        fn drop(&mut self) {
            // SAFETY: a handle this struct owns.
            unsafe { CloseHandle(self.handle) };
        }
    }

    fn between<'a>(text: &'a str, open: &str, close: &str) -> &'a str {
        let start = text.find(open).expect("open tag") + open.len();
        let end = start + text[start..].find(close).expect("close tag");
        &text[start..end]
    }

    fn unescape(text: &str) -> String {
        text.replace("&quot;", "\"").replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")
    }
}
