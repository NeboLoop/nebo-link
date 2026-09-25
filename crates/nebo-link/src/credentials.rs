//! The bot token. It lives in the OS keychain where one is available, and
//! otherwise in an owner-only file in the bot's state directory. The hub
//! rotates the token on every connect and the old one stops working at once,
//! so every save must land before the next connect.

use std::path::PathBuf;

use crate::error::{Error, Result};
use crate::state::{BotDir, write_private};

/// The keychain service name entries are stored under (account = bot id).
const KEYCHAIN_SERVICE: &str = "nebo-link";

/// Where one bot's token is kept.
pub struct Credentials {
    keychain: Option<keyring::Entry>,
    file: PathBuf,
}

/// Where a saved token landed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stored {
    Keychain,
    File,
}

impl Credentials {
    /// The store for `bot_id`: the OS keychain when it can be opened, with
    /// the fallback file in `dir`.
    pub fn open(dir: &BotDir, bot_id: &str) -> Self {
        Self::with_keychain(dir, keyring::Entry::new(KEYCHAIN_SERVICE, bot_id).ok())
    }

    /// The store with an explicit keychain entry (`None`: file only).
    pub fn with_keychain(dir: &BotDir, keychain: Option<keyring::Entry>) -> Self {
        Self {
            keychain,
            file: dir.token_file(),
        }
    }

    /// Saves `token`, replacing the one stored before.
    pub fn save(&self, token: &str) -> Result<Stored> {
        if let Some(entry) = &self.keychain {
            match entry.set_password(token) {
                Ok(()) => {
                    // The keychain now holds the newest token; a file left by
                    // an earlier fallback would shadow it.
                    remove_file(&self.file)?;
                    return Ok(Stored::Keychain);
                }
                Err(e) => tracing::info!(error = %e, "keychain unavailable; storing the bot token in the state directory"),
            }
        }
        write_private(&self.file, token.as_bytes())?;
        Ok(Stored::File)
    }

    /// The stored token. The file wins over the keychain: it is only written
    /// when the keychain refused the newest token.
    pub fn load(&self) -> Result<String> {
        match std::fs::read_to_string(&self.file) {
            Ok(token) => return Ok(token.trim().to_string()),
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(Error::io(&self.file, e));
            }
            Err(_) => {}
        }
        let entry = self
            .keychain
            .as_ref()
            .ok_or_else(|| Error::Credentials("no bot token is stored for this bot".into()))?;
        entry.get_password().map_err(|e| match e {
            keyring::Error::NoEntry => Error::Credentials("no bot token is stored for this bot".into()),
            other => Error::Credentials(other.to_string()),
        })
    }

    /// Deletes the token from both places. The file goes first, so a
    /// keychain that can't be reached still leaves nothing on disk.
    pub fn forget(&self) -> Result<()> {
        remove_file(&self.file)?;
        match self.keychain.as_ref().map(keyring::Entry::delete_credential) {
            None | Some(Ok(())) | Some(Err(keyring::Error::NoEntry)) => Ok(()),
            Some(Err(e)) => Err(Error::Credentials(e.to_string())),
        }
    }
}

fn remove_file(path: &std::path::Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(Error::io(path, e)),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Root;

    fn mock_entry() -> keyring::Entry {
        keyring::Entry::new_with_credential(Box::new(keyring::mock::MockCredential::default()))
    }

    #[test]
    fn keychain_first_and_nothing_on_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = Root::at(tmp.path()).bot("b");
        dir.create().unwrap();
        let creds = Credentials::with_keychain(&dir, Some(mock_entry()));
        assert_eq!(creds.save("t1").unwrap(), Stored::Keychain);
        assert!(!dir.token_file().exists());
        assert_eq!(creds.load().unwrap(), "t1");
        creds.forget().unwrap();
        assert!(creds.load().is_err());
    }

    #[test]
    fn falls_back_to_an_owner_only_file() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = Root::at(tmp.path()).bot("b");
        dir.create().unwrap();
        let entry = mock_entry();
        entry
            .get_credential()
            .downcast_ref::<keyring::mock::MockCredential>()
            .unwrap()
            .set_error(keyring::Error::NoStorageAccess("locked".into()));
        let creds = Credentials::with_keychain(&dir, Some(entry));
        assert_eq!(creds.save("t1").unwrap(), Stored::File);
        assert_eq!(creds.load().unwrap(), "t1");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(dir.token_file()).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }

        // The keychain works again: the next rotation goes there and the
        // stale file is gone, so it can never shadow the newer token.
        assert_eq!(creds.save("t2").unwrap(), Stored::Keychain);
        assert!(!dir.token_file().exists());
        assert_eq!(creds.load().unwrap(), "t2");
    }

    #[test]
    fn file_only_without_a_keychain() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = Root::at(tmp.path()).bot("b");
        dir.create().unwrap();
        let creds = Credentials::with_keychain(&dir, None);
        assert!(creds.load().is_err());
        assert_eq!(creds.save("t1").unwrap(), Stored::File);
        assert_eq!(creds.load().unwrap(), "t1");
        creds.forget().unwrap();
        assert!(creds.load().is_err());
    }
}
