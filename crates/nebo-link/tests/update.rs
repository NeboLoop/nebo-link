//! Self-update against a fake release CDN signed with a throwaway key: a
//! good release downloads and verifies; a tampered binary or a signature by
//! any other key is refused and nothing is kept.

use std::sync::Arc;

use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
use nebo_link::update;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const RELEASE: &[u8] = b"nebo-link v9.9.9";

/// Serves `files` (path, body) over HTTP on a local port; anything else is a
/// 404. Returns the base URL.
async fn serve(files: Vec<(String, Vec<u8>)>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let files = Arc::new(files);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let files = files.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                    match socket.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => request.extend_from_slice(&buf[..n]),
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split_whitespace().nth(1).unwrap_or_default();
                let (status, body) = match files.iter().find(|(p, _)| p == path) {
                    Some((_, body)) => ("200 OK", body.clone()),
                    None => ("404 Not Found", Vec::new()),
                };
                let head = format!("HTTP/1.1 {status}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
                let _ = socket.write_all(head.as_bytes()).await;
                let _ = socket.write_all(&body).await;
            });
        }
    });
    format!("http://{addr}")
}

/// A throwaway key for this test run only.
fn test_key() -> SigningKey {
    let mut seed = [0u8; 32];
    seed[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    seed[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    SigningKey::from_bytes(&seed)
}

/// Publishes v9.9.9: `served` is the binary the CDN hands out, SHA256SUMS
/// lists the hash of the real release, and `signer` signs SHA256SUMS.
/// Returns the base URL.
async fn publish(signer: &SigningKey, served: &[u8]) -> String {
    let asset = nebo_updater::asset_name(&update::feed("", [0; 32]));
    let sums = format!(
        "{:x}  nebo-link-other-platform\n{:x}  {asset}\n",
        Sha256::digest(b"other"),
        Sha256::digest(RELEASE)
    )
    .into_bytes();
    let sig = base64::engine::general_purpose::STANDARD.encode(signer.sign(&sums).to_bytes());
    let version = serde_json::json!({
        "version": "v9.9.9",
        "release_url": "https://github.com/NeboLoop/nebo-link/releases/tag/v9.9.9",
        "platforms": { "darwin": "v9.9.9", "linux": "v9.9.9", "windows": "v9.9.9" },
    });
    serve(vec![
        ("/version.json".into(), version.to_string().into_bytes()),
        (format!("/v9.9.9/{asset}"), served.to_vec()),
        ("/v9.9.9/SHA256SUMS".into(), sums),
        ("/v9.9.9/SHA256SUMS.sig".into(), sig.into_bytes()),
    ])
    .await
}

#[tokio::test]
async fn a_signed_release_downloads_and_verifies() {
    let key = test_key();
    let base = publish(&key, RELEASE).await;
    let feed = update::feed(base, key.verifying_key().to_bytes());

    let downloaded = update::fetch(&feed, "0.1.0").await.unwrap().expect("v9.9.9 is newer");
    assert_eq!(downloaded.tag, "v9.9.9");
    assert_eq!(downloaded.version(), "9.9.9");
    assert_eq!(std::fs::read(&downloaded.path).unwrap(), RELEASE);
    std::fs::remove_file(&downloaded.path).unwrap();

    assert!(update::fetch(&feed, "9.9.9").await.unwrap().is_none(), "already up to date");
}

#[tokio::test]
async fn a_tampered_binary_is_refused() {
    let key = test_key();
    let base = publish(&key, b"not the release").await;
    let feed = update::feed(base, key.verifying_key().to_bytes());

    let err = update::fetch(&feed, "0.1.0").await.unwrap_err().to_string();
    assert!(err.contains("checksum mismatch"), "{err}");
}

#[tokio::test]
async fn a_release_signed_by_another_key_is_refused() {
    let key = test_key();
    let base = publish(&test_key(), RELEASE).await;
    let feed = update::feed(base, key.verifying_key().to_bytes());

    let err = update::fetch(&feed, "0.1.0").await.unwrap_err().to_string();
    assert!(err.contains("SHA256SUMS.sig does not match"), "{err}");
}
