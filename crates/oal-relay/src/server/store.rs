//! The relay's state on disk: one SQLite file. Hosts and the keys they
//! registered, pairings, live nameplates, revoked keys, and the relay's own
//! key. No content and no code secrets, ever: the relay never sees either.

use std::path::Path;
use std::sync::Mutex;

use rusqlite::{Connection, OptionalExtension, params};

use crate::time::rfc3339;
use crate::wire::PairedClient;

const SCHEMA_VERSION: i64 = 1;

const SCHEMA: &str = "
CREATE TABLE meta (
    k TEXT PRIMARY KEY,
    v BLOB NOT NULL
);
CREATE TABLE hosts (
    id TEXT PRIMARY KEY,
    public_key TEXT NOT NULL UNIQUE,
    created_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL
);
CREATE TABLE pairings (
    host_id TEXT NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    client_key TEXT NOT NULL,
    paired_at INTEGER NOT NULL,
    PRIMARY KEY (host_id, client_key)
);
CREATE INDEX pairings_by_client ON pairings(client_key);
CREATE TABLE nameplates (
    nameplate TEXT PRIMARY KEY,
    host_id TEXT NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    expires_at INTEGER NOT NULL
);
CREATE TABLE revocations (
    public_key TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('host', 'client')),
    revoked_at INTEGER NOT NULL
);
";

/// A registered host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HostRow {
    pub(crate) id: String,
    pub(crate) public_key: String,
    pub(crate) last_seen_at: i64,
}

/// Why a host could not register.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RegisterError {
    /// The id belongs to another key.
    IdTaken,
    /// This key is registered under another id.
    KeyTaken(String),
}

pub(crate) struct Store {
    db: Mutex<Connection>,
}

type Result<T> = std::result::Result<T, rusqlite::Error>;

impl Store {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        let db = Connection::open(path)?;
        db.pragma_update(None, "journal_mode", "WAL")?;
        db.pragma_update(None, "synchronous", "NORMAL")?;
        db.pragma_update(None, "foreign_keys", "ON")?;
        db.busy_timeout(std::time::Duration::from_secs(5))?;
        let version: i64 = db.pragma_query_value(None, "user_version", |r| r.get(0))?;
        if version == 0 {
            db.execute_batch(&format!(
                "BEGIN; {SCHEMA} PRAGMA user_version = {SCHEMA_VERSION}; COMMIT;"
            ))?;
        }
        Ok(Self { db: Mutex::new(db) })
    }

    fn db(&self) -> std::sync::MutexGuard<'_, Connection> {
        self.db.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn ping(&self) -> Result<()> {
        self.db().query_row("SELECT 1", [], |_| Ok(()))
    }

    /// The relay's own X25519 secret, created on first use.
    pub(crate) fn relay_secret(&self) -> Result<[u8; 32]> {
        let db = self.db();
        let found: Option<Vec<u8>> = db
            .query_row("SELECT v FROM meta WHERE k = 'relay_key'", [], |r| r.get(0))
            .optional()?;
        if let Some(bytes) = found.and_then(|v| <[u8; 32]>::try_from(v).ok()) {
            return Ok(bytes);
        }
        let fresh = crate::auth::Keypair::generate().secret_bytes();
        db.execute(
            "INSERT OR REPLACE INTO meta (k, v) VALUES ('relay_key', ?1)",
            params![fresh.to_vec()],
        )?;
        Ok(fresh)
    }

    /// Registers `id` to `key` the first time; afterwards, only `key` may
    /// use `id`. Refreshes `last_seen_at`.
    pub(crate) fn register_host(
        &self,
        id: &str,
        key: &str,
        now: i64,
    ) -> Result<std::result::Result<(), RegisterError>> {
        let db = self.db();
        let owner: Option<String> = db
            .query_row("SELECT public_key FROM hosts WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .optional()?;
        match owner {
            Some(k) if k == key => {
                db.execute(
                    "UPDATE hosts SET last_seen_at = ?2 WHERE id = ?1",
                    params![id, now],
                )?;
                Ok(Ok(()))
            }
            Some(_) => Ok(Err(RegisterError::IdTaken)),
            None => {
                let other: Option<String> = db
                    .query_row("SELECT id FROM hosts WHERE public_key = ?1", [key], |r| {
                        r.get(0)
                    })
                    .optional()?;
                if let Some(other) = other {
                    return Ok(Err(RegisterError::KeyTaken(other)));
                }
                db.execute(
                    "INSERT INTO hosts (id, public_key, created_at, last_seen_at) VALUES (?1, ?2, ?3, ?3)",
                    params![id, key, now],
                )?;
                Ok(Ok(()))
            }
        }
    }

    pub(crate) fn touch_host(&self, id: &str, now: i64) -> Result<()> {
        self.db().execute(
            "UPDATE hosts SET last_seen_at = ?2 WHERE id = ?1",
            params![id, now],
        )?;
        Ok(())
    }

    pub(crate) fn host(&self, id: &str) -> Result<Option<HostRow>> {
        self.db()
            .query_row(
                "SELECT id, public_key, last_seen_at FROM hosts WHERE id = ?1",
                [id],
                host_row,
            )
            .optional()
    }

    pub(crate) fn hosts(&self) -> Result<Vec<HostRow>> {
        let db = self.db();
        let mut stmt = db.prepare("SELECT id, public_key, last_seen_at FROM hosts ORDER BY id")?;
        stmt.query_map([], host_row)?.collect()
    }

    /// Holds `nameplate` for `host_id` until `expires_at`. A nameplate
    /// another host holds (and has not let expire) is refused with `false`;
    /// the same host asking again renews it. Expired nameplates are dropped.
    pub(crate) fn hold_nameplate(
        &self,
        nameplate: &str,
        host_id: &str,
        expires_at: i64,
        now: i64,
    ) -> Result<bool> {
        let db = self.db();
        db.execute("DELETE FROM nameplates WHERE expires_at <= ?1", [now])?;
        let changed = db.execute(
            "INSERT INTO nameplates (nameplate, host_id, expires_at) VALUES (?1, ?2, ?3)
             ON CONFLICT (nameplate) DO UPDATE SET expires_at = excluded.expires_at
             WHERE nameplates.host_id = excluded.host_id",
            params![nameplate, host_id, expires_at],
        )?;
        Ok(changed > 0)
    }

    /// The host a live nameplate routes to.
    pub(crate) fn nameplate_host(&self, nameplate: &str, now: i64) -> Result<Option<String>> {
        self.db()
            .query_row(
                "SELECT host_id FROM nameplates WHERE nameplate = ?1 AND expires_at > ?2",
                params![nameplate, now],
                |r| r.get(0),
            )
            .optional()
    }

    /// Records that `host_id` paired `client_key` (the host said so).
    pub(crate) fn add_pairing(&self, host_id: &str, client_key: &str, now: i64) -> Result<()> {
        self.db().execute(
            "INSERT INTO pairings (host_id, client_key, paired_at) VALUES (?1, ?2, ?3)
             ON CONFLICT (host_id, client_key) DO NOTHING",
            params![host_id, client_key, now],
        )?;
        Ok(())
    }

    pub(crate) fn pairing(&self, host_id: &str, client_key: &str) -> Result<Option<PairedClient>> {
        self.db()
            .query_row(
                "SELECT client_key, paired_at FROM pairings WHERE host_id = ?1 AND client_key = ?2",
                [host_id, client_key],
                paired_client,
            )
            .optional()
    }

    pub(crate) fn pairings(&self, host_id: &str) -> Result<Vec<PairedClient>> {
        let db = self.db();
        let mut stmt = db.prepare(
            "SELECT client_key, paired_at FROM pairings WHERE host_id = ?1 ORDER BY paired_at, client_key",
        )?;
        stmt.query_map([host_id], paired_client)?.collect()
    }

    /// The hosts `client_key` is paired with.
    pub(crate) fn paired_hosts(&self, client_key: &str) -> Result<Vec<HostRow>> {
        let db = self.db();
        let mut stmt = db.prepare(
            "SELECT h.id, h.public_key, h.last_seen_at FROM hosts h
             JOIN pairings p ON p.host_id = h.id WHERE p.client_key = ?1 ORDER BY h.id",
        )?;
        stmt.query_map([client_key], host_row)?.collect()
    }

    /// Removes one pairing. True if there was one.
    pub(crate) fn unpair(&self, host_id: &str, client_key: &str) -> Result<bool> {
        Ok(self.db().execute(
            "DELETE FROM pairings WHERE host_id = ?1 AND client_key = ?2",
            [host_id, client_key],
        )? > 0)
    }

    /// Revokes a client key relay-wide: every pairing goes, and the key can
    /// never pair or connect again. Returns the hosts it was paired with.
    pub(crate) fn revoke_client(&self, client_key: &str, now: i64) -> Result<Vec<String>> {
        let mut db = self.db();
        let tx = db.transaction()?;
        let hosts: Vec<String> = {
            let mut stmt =
                tx.prepare("DELETE FROM pairings WHERE client_key = ?1 RETURNING host_id")?;
            stmt.query_map([client_key], |r| r.get(0))?
                .collect::<Result<_>>()?
        };
        tx.execute(
            "INSERT OR REPLACE INTO revocations (public_key, kind, revoked_at) VALUES (?1, 'client', ?2)",
            params![client_key, now],
        )?;
        tx.commit()?;
        Ok(hosts)
    }

    /// Revokes a host: its key can never register again, and the host, its
    /// pairings and its nameplates are deleted (the id becomes free). Returns the
    /// host's key, or `None` if there was no such host.
    pub(crate) fn revoke_host(&self, host_id: &str, now: i64) -> Result<Option<String>> {
        let mut db = self.db();
        let tx = db.transaction()?;
        let key: Option<String> = tx
            .query_row(
                "DELETE FROM hosts WHERE id = ?1 RETURNING public_key",
                [host_id],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(key) = &key {
            tx.execute(
                "INSERT OR REPLACE INTO revocations (public_key, kind, revoked_at) VALUES (?1, 'host', ?2)",
                params![key, now],
            )?;
        }
        tx.commit()?;
        Ok(key)
    }

    pub(crate) fn is_revoked(&self, key: &str) -> Result<bool> {
        self.db()
            .query_row(
                "SELECT 1 FROM revocations WHERE public_key = ?1",
                [key],
                |_| Ok(()),
            )
            .optional()
            .map(|r| r.is_some())
    }
}

fn host_row(r: &rusqlite::Row<'_>) -> Result<HostRow> {
    Ok(HostRow {
        id: r.get(0)?,
        public_key: r.get(1)?,
        last_seen_at: r.get(2)?,
    })
}

fn paired_client(r: &rusqlite::Row<'_>) -> Result<PairedClient> {
    Ok(PairedClient {
        client_key: r.get(0)?,
        paired_at: rfc3339(r.get(1)?),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> (tempfile::TempDir, Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("relay.db")).unwrap();
        (dir, store)
    }

    #[test]
    fn a_host_id_belongs_to_its_first_key() {
        let (_d, s) = store();
        assert_eq!(s.register_host("studio", "k1", 1).unwrap(), Ok(()));
        assert_eq!(s.register_host("studio", "k1", 2).unwrap(), Ok(()));
        assert_eq!(
            s.register_host("studio", "k2", 3).unwrap(),
            Err(RegisterError::IdTaken)
        );
        assert_eq!(
            s.register_host("laptop", "k1", 3).unwrap(),
            Err(RegisterError::KeyTaken("studio".into()))
        );
        assert_eq!(s.host("studio").unwrap().unwrap().last_seen_at, 2);
    }

    #[test]
    fn a_nameplate_routes_to_one_host_until_it_expires() {
        let (_d, s) = store();
        s.register_host("studio", "hk", 0).unwrap().unwrap();
        s.register_host("laptop", "lk", 0).unwrap().unwrap();
        assert!(s.hold_nameplate("K7QM", "studio", 100, 0).unwrap());
        // Not consumed by use: it routes until it expires.
        assert_eq!(
            s.nameplate_host("K7QM", 50).unwrap().as_deref(),
            Some("studio")
        );
        assert_eq!(
            s.nameplate_host("K7QM", 60).unwrap().as_deref(),
            Some("studio")
        );
        // Another host can't take it; the same host renews it.
        assert!(!s.hold_nameplate("K7QM", "laptop", 200, 70).unwrap());
        assert!(s.hold_nameplate("K7QM", "studio", 200, 70).unwrap());
        assert!(s.nameplate_host("K7QM", 150).unwrap().is_some());
        assert!(s.nameplate_host("K7QM", 200).unwrap().is_none());
        // Once expired it is free for anyone.
        assert!(s.hold_nameplate("K7QM", "laptop", 400, 300).unwrap());
        assert_eq!(
            s.nameplate_host("K7QM", 301).unwrap().as_deref(),
            Some("laptop")
        );
    }

    #[test]
    fn revoking_removes_pairings_and_bans_the_key() {
        let (_d, s) = store();
        s.register_host("studio", "hk", 0).unwrap().unwrap();
        s.add_pairing("studio", "ck", 1).unwrap();
        assert!(s.pairing("studio", "ck").unwrap().is_some());
        assert_eq!(
            s.revoke_client("ck", 2).unwrap(),
            vec!["studio".to_string()]
        );
        assert!(s.is_revoked("ck").unwrap());
        assert!(s.pairings("studio").unwrap().is_empty());

        assert_eq!(s.revoke_host("studio", 3).unwrap().as_deref(), Some("hk"));
        assert!(s.is_revoked("hk").unwrap());
        assert!(s.host("studio").unwrap().is_none());
        // The id is free again, for another key.
        assert_eq!(s.register_host("studio", "hk2", 4).unwrap(), Ok(()));
    }

    #[test]
    fn state_survives_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("relay.db");
        let secret = {
            let s = Store::open(&path).unwrap();
            s.register_host("studio", "hk", 0).unwrap().unwrap();
            s.add_pairing("studio", "ck", 1).unwrap();
            s.hold_nameplate("K7QM", "studio", i64::MAX, 0).unwrap();
            s.relay_secret().unwrap()
        };
        let s = Store::open(&path).unwrap();
        assert_eq!(s.relay_secret().unwrap(), secret);
        assert_eq!(s.pairings("studio").unwrap().len(), 1);
        assert_eq!(s.paired_hosts("ck").unwrap()[0].id, "studio");
        assert_eq!(
            s.nameplate_host("K7QM", 5).unwrap().as_deref(),
            Some("studio")
        );
    }
}
