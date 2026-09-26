//! A stand-in for a runtime's process: an HTTP server answering every
//! request with 200, as its own program (the supervisor starts programs).
//! Built with `rustc` into a temporary directory when a test needs it, so
//! the tests depend on nothing the machine may lack (a CI runner's
//! `python3 -m http.server` did not come up).

use std::path::{Path, PathBuf};

const SOURCE: &str = r#"
fn main() {
    let port: u16 = std::env::args().nth(1).expect("port").parse().expect("port");
    let listener = std::net::TcpListener::bind(("127.0.0.1", port)).expect("bind");
    for stream in listener.incoming().flatten() {
        std::thread::spawn(move || {
            let mut stream = stream;
            let mut request = [0u8; 4096];
            let _ = std::io::Read::read(&mut stream, &mut request);
            let _ = std::io::Write::write_all(
                &mut stream,
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
            );
        });
    }
}
"#;

/// Builds the server into `dir` and returns its path.
pub fn build(dir: &Path) -> PathBuf {
    let source = dir.join("fake_http.rs");
    std::fs::write(&source, SOURCE).unwrap();
    let exe = dir.join("fake-http");
    let output = std::process::Command::new("rustc")
        .args(["--edition", "2021", "-o"])
        .arg(&exe)
        .arg(&source)
        .output()
        .expect("rustc is installed");
    assert!(output.status.success(), "rustc: {}", String::from_utf8_lossy(&output.stderr));
    exe
}
