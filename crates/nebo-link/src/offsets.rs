//! Acked hub stream offsets, persisted so a restarted service resumes each
//! stream where it left off (`nebo_comm::StreamOffsets`).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Mutex;

use crate::state::{read_json, write_json};

pub struct FileOffsets {
    path: PathBuf,
    acked: Mutex<HashMap<String, u64>>,
}

impl FileOffsets {
    /// Offsets stored at `path`; a missing or unreadable file starts every
    /// stream from 0, as a first run does.
    pub fn open(path: PathBuf) -> Self {
        let acked = read_json(&path).unwrap_or_default();
        Self {
            path,
            acked: Mutex::new(acked),
        }
    }
}

impl nebo_comm::StreamOffsets for FileOffsets {
    fn acked(&self, _bot_id: &str, stream: &str) -> u64 {
        self.acked
            .lock()
            .expect("offsets lock")
            .get(stream)
            .copied()
            .unwrap_or(0)
    }

    fn record(&self, _bot_id: &str, stream: &str, seq: u64) {
        let mut acked = self.acked.lock().expect("offsets lock");
        let slot = acked.entry(stream.to_string()).or_insert(0);
        if seq <= *slot {
            return;
        }
        *slot = seq;
        if let Err(e) = write_json(&self.path, &*acked) {
            tracing::warn!(error = %e, "could not persist stream offsets");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nebo_comm::StreamOffsets;

    #[test]
    fn offsets_persist_and_never_move_back() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("offsets.json");
        let offsets = FileOffsets::open(path.clone());
        assert_eq!(offsets.acked("b", "s"), 0);
        offsets.record("b", "s", 7);
        offsets.record("b", "s", 3);
        assert_eq!(offsets.acked("b", "s"), 7);
        assert_eq!(FileOffsets::open(path).acked("b", "s"), 7);
    }
}
