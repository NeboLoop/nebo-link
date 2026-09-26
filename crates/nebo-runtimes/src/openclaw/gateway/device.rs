//! The device identity a gateway client proves in `connect.params.device`
//! (`packages/gateway-protocol/src/schema/frames.ts:80-88`): an Ed25519
//! keypair whose public key names the device.

use std::io;
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use ed25519_dalek::{Signer, SigningKey};
use sha2::{Digest, Sha256};

/// An Ed25519 device keypair.
#[derive(Clone)]
pub struct DeviceKey {
    signing: SigningKey,
}

impl std::fmt::Debug for DeviceKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeviceKey")
            .field("device_id", &self.device_id())
            .finish_non_exhaustive()
    }
}

impl DeviceKey {
    /// A fresh keypair from the OS random source.
    pub fn generate() -> io::Result<Self> {
        let mut secret = [0u8; 32];
        getrandom::getrandom(&mut secret).map_err(|error| io::Error::other(error.to_string()))?;
        Ok(Self::from_secret(secret))
    }

    /// The keypair for a stored 32-byte secret.
    pub fn from_secret(secret: [u8; 32]) -> Self {
        Self {
            signing: SigningKey::from_bytes(&secret),
        }
    }

    /// The 32-byte secret, for a store to keep.
    pub fn secret(&self) -> [u8; 32] {
        self.signing.to_bytes()
    }

    /// `connect.params.device.id`: the SHA-256 of the raw public key, hex
    /// (`src/infra/device-identity.ts:176-187` `deriveDeviceIdFromPublicKey`);
    /// the gateway recomputes it and refuses a mismatch
    /// (`src/gateway/server/ws-connection/connect-device-proof.ts:64-67`).
    pub fn device_id(&self) -> String {
        let digest = Sha256::digest(self.signing.verifying_key().as_bytes());
        digest.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// `connect.params.device.publicKey`: the raw 32-byte public key,
    /// unpadded base64url (`src/infra/ed25519-signature.ts:156-168`).
    pub fn public_key(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.signing.verifying_key().as_bytes())
    }

    /// `connect.params.device.signature` over `payload`, unpadded base64url
    /// (`src/infra/ed25519-signature.ts:170-174` `signEd25519Payload`).
    pub fn sign(&self, payload: &str) -> String {
        URL_SAFE_NO_PAD.encode(self.signing.sign(payload.as_bytes()).to_bytes())
    }
}

/// Where a client keeps its [`DeviceKey`] between connections. The gateway
/// pairs the key once; a new key every start would be a new device every
/// start.
pub trait DeviceStore: Send + Sync {
    /// The stored key, or `None` when this client has none yet.
    fn load(&self) -> io::Result<Option<DeviceKey>>;
    /// Keep `key` for the next connection.
    fn save(&self, key: &DeviceKey) -> io::Result<()>;
}

/// A [`DeviceStore`] in one JSON file, owner-readable only on Unix.
#[derive(Debug, Clone)]
pub struct FileDeviceStore {
    path: PathBuf,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredDevice {
    version: u32,
    /// The 32-byte secret, unpadded base64url.
    secret_key: String,
}

impl FileDeviceStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl DeviceStore for FileDeviceStore {
    fn load(&self) -> io::Result<Option<DeviceKey>> {
        let text = match std::fs::read_to_string(&self.path) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let stored: StoredDevice = serde_json::from_str(&text).map_err(io::Error::other)?;
        let secret = URL_SAFE_NO_PAD
            .decode(stored.secret_key)
            .map_err(io::Error::other)?;
        let secret: [u8; 32] = secret
            .try_into()
            .map_err(|_| io::Error::other("device secret key is not 32 bytes"))?;
        Ok(Some(DeviceKey::from_secret(secret)))
    }

    fn save(&self, key: &DeviceKey) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let stored = StoredDevice {
            version: 1,
            secret_key: URL_SAFE_NO_PAD.encode(key.secret()),
        };
        let text = serde_json::to_string_pretty(&stored).map_err(io::Error::other)?;
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create(true).truncate(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&self.path)?;
        io::Write::write_all(&mut file, text.as_bytes())
    }
}

/// The signed connect proof, payload version 3
/// (`packages/gateway-client/src/device-auth.ts:44-64`
/// `buildDeviceAuthPayloadV3`; the gateway tries v3 then v2,
/// `src/gateway/server/ws-connection/handshake-auth-helpers.ts:288-325`).
/// `platform` and `device_family` are lowercased before joining, byte for
/// byte as the gateway does. `scopes` is joined in the order sent.
pub(crate) fn auth_payload(input: &AuthPayload<'_>) -> String {
    let platform = normalize_metadata(input.platform);
    let device_family = normalize_metadata(input.device_family);
    [
        "v3",
        input.device_id,
        input.client_id,
        input.client_mode,
        input.role,
        &input.scopes.join(","),
        &input.signed_at_ms.to_string(),
        input.token,
        input.nonce,
        &platform,
        &device_family,
    ]
    .join("|")
}

pub(crate) struct AuthPayload<'a> {
    pub device_id: &'a str,
    pub client_id: &'a str,
    pub client_mode: &'a str,
    pub role: &'a str,
    pub scopes: &'a [&'a str],
    pub signed_at_ms: u64,
    /// `auth.token ?? auth.deviceToken ?? auth.bootstrapToken ?? ""`
    /// (`handshake-auth-helpers.ts:262-269`); empty for trusted-proxy auth.
    pub token: &'a str,
    pub nonce: &'a str,
    pub platform: &'a str,
    pub device_family: &'a str,
}

/// `normalizeDeviceMetadataForAuth` (`device-auth.ts:1-10`): trim, then
/// ASCII-lowercase only.
fn normalize_metadata(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixed_key() -> DeviceKey {
        DeviceKey::from_secret([7u8; 32])
    }

    #[test]
    fn device_id_is_sha256_hex_of_raw_public_key() {
        let key = fixed_key();
        let raw = URL_SAFE_NO_PAD.decode(key.public_key()).unwrap();
        assert_eq!(raw.len(), 32);
        let expected: String = Sha256::digest(&raw)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(key.device_id(), expected);
        assert_eq!(key.device_id().len(), 64);
    }

    #[test]
    fn signature_verifies_with_dalek() {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        let key = fixed_key();
        let signature = key.sign("hello");
        let raw_key: [u8; 32] = URL_SAFE_NO_PAD
            .decode(key.public_key())
            .unwrap()
            .try_into()
            .unwrap();
        let raw_sig: [u8; 64] = URL_SAFE_NO_PAD
            .decode(signature)
            .unwrap()
            .try_into()
            .unwrap();
        VerifyingKey::from_bytes(&raw_key)
            .unwrap()
            .verify(b"hello", &Signature::from_bytes(&raw_sig))
            .unwrap();
    }

    #[test]
    fn auth_payload_matches_build_device_auth_payload_v3() {
        let payload = auth_payload(&AuthPayload {
            device_id: "dev",
            client_id: "openclaw-control-ui",
            client_mode: "ui",
            role: "operator",
            scopes: &["operator.read", "operator.write"],
            signed_at_ms: 1737264000000,
            token: "",
            nonce: "n0nce",
            platform: " Darwin ",
            device_family: "",
        });
        assert_eq!(
            payload,
            "v3|dev|openclaw-control-ui|ui|operator|operator.read,operator.write|1737264000000||n0nce|darwin|"
        );
    }

    #[test]
    fn file_store_round_trips_and_is_private() {
        let dir = tempfile::tempdir().unwrap();
        let store = FileDeviceStore::new(dir.path().join("nested").join("device.json"));
        assert!(store.load().unwrap().is_none());
        let key = DeviceKey::generate().unwrap();
        store.save(&key).unwrap();
        let loaded = store.load().unwrap().unwrap();
        assert_eq!(loaded.secret(), key.secret());
        assert_eq!(loaded.device_id(), key.device_id());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(store.path())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }
}
