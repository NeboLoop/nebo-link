//! The bot token, in an owner-only file in the bot's state directory. The
//! hub rotates the token on every connect and the old one stops working at
//! once, so every save must land before the next connect.
//!
//! One store on every platform, and never the OS keychain: the link runs as
//! a background service, and a keychain call from a service can block with
//! no one there to answer it (seen on macOS in the first live run), while a
//! headless Linux server has no keychain at all.

use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::state::{BotDir, write_private};

/// Where one bot's token is kept.
pub struct Credentials {
    file: PathBuf,
}

impl Credentials {
    /// The store for the bot whose state directory is `dir`.
    pub fn open(dir: &BotDir) -> Self {
        Self {
            file: dir.token_file(),
        }
    }

    /// Where the token is kept.
    pub fn path(&self) -> &std::path::Path {
        &self.file
    }

    /// Saves `token` (mode 0600), replacing the one stored before.
    pub fn save(&self, token: &str) -> Result<()> {
        write_private(&self.file, token.as_bytes())
    }

    /// The stored token.
    pub fn load(&self) -> Result<String> {
        match std::fs::read_to_string(&self.file) {
            Ok(token) => Ok(token.trim().to_string()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Err(Error::Credentials(
                "no bot token is stored for this bot".into(),
            )),
            Err(e) => Err(Error::io(&self.file, e)),
        }
    }

    /// Deletes the token.
    pub fn forget(&self) -> Result<()> {
        match std::fs::remove_file(&self.file) {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(Error::io(&self.file, e)),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Root;

    #[test]
    fn owner_only_file_round_trip() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = Root::at(tmp.path()).bot("b");
        dir.create().unwrap();
        let creds = Credentials::open(&dir);
        assert!(creds.load().is_err());
        creds.save("t1").unwrap();
        creds.save("t2").unwrap();
        assert_eq!(creds.load().unwrap(), "t2");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.token_file())
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        creds.forget().unwrap();
        creds.forget().unwrap();
        assert!(creds.load().is_err());
    }
}
