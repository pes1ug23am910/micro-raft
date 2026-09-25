//! Durable storage for Raft's persistent state.
//!
//! This module contains no consensus logic; it only executes durable writes
//! requested by the driver.
//! Layout per node data dir:
//! - `hardstate.json` — atomic-swap-updated JSON of [`HardState`]
//! - `log.jsonl` — append-only, one CRC-framed JSON line per [`Entry`]:
//!   `{"crc":<u32>,"entry":{...}}\n`, `crc` over the serialized entry bytes
//!
//! Renames are not followed by a directory fsync. Full rename durability
//! differs by platform, and this implementation targets a local Windows demo.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use raft_core::{Entry, HardState, LogIndex};
use serde::Deserialize;
use tracing::warn;

use crate::crc::crc32;

const HARDSTATE: &str = "hardstate.json";
const HARDSTATE_NEW: &str = "hardstate.json.new";
const LOG: &str = "log.jsonl";
const LOG_NEW: &str = "log.jsonl.new";

/// One framed log line: `{"crc":N,"entry":{...}}`.
#[derive(Deserialize)]
struct Frame {
    crc: u32,
    entry: Entry,
}

/// Serialize one CRC-framed log line. The CRC covers exactly the serialized
/// entry bytes embedded in the frame, so recovery can recompute it from the
/// re-serialized parsed entry (serde_json emits canonical bytes for our types).
fn frame_line(e: &Entry) -> io::Result<String> {
    let entry_json = serde_json::to_string(e)?;
    let crc = crc32(entry_json.as_bytes());
    Ok(format!("{{\"crc\":{crc},\"entry\":{entry_json}}}\n"))
}

pub struct Storage {
    dir: PathBuf,
}

impl Storage {
    /// Opens (creating if needed) a node data dir; returns recovered state.
    /// Missing `hardstate.json` recovers as the default `{0, null}`; the log
    /// is replayed with CRC + index-contiguity checks and torn-tail truncation.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<(Storage, HardState, Vec<Entry>)> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;

        // A leftover .new means a crash between write and rename. The swap
        // never completed, so hardstate.json is authoritative and the orphan
        // must not be trusted (hardstate_persist_roundtrip).
        let orphan = dir.join(HARDSTATE_NEW);
        if orphan.exists() {
            warn!(path = %orphan.display(), "removing stale hardstate.json.new from interrupted swap");
            fs::remove_file(&orphan)?;
        }

        let hard = match fs::read(dir.join(HARDSTATE)) {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("hardstate.json corrupt: {e}"),
                )
            })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HardState::default(),
            Err(e) => return Err(e),
        };

        let entries = recover_log(&dir)?;
        Ok((Storage { dir }, hard, entries))
    }

    /// Atomic swap: truncate-write `hardstate.json.new`, fsync, rename
    /// over `hardstate.json`. A crash leaves either the old or the new file,
    /// never a hybrid. `std::fs::rename` replaces the destination on Windows
    /// (MoveFileEx semantics) as long as the destination is not open.
    pub fn save_hard_state(&mut self, hs: &HardState) -> io::Result<()> {
        let tmp = self.dir.join(HARDSTATE_NEW);
        let mut f = File::create(&tmp)?;
        f.write_all(serde_json::to_string(hs)?.as_bytes())?;
        f.sync_all()?;
        drop(f); // Windows requires closing the handle before the rename.
        fs::rename(&tmp, self.dir.join(HARDSTATE))
    }

    /// Optionally truncate the durable log from an index (conflict repair,
    /// rewrite-based because logs in this project are deliberately small),
    /// then append. Every mutation ends in `sync_all` before returning:
    /// nothing may be acknowledged upstream that the disk hasn't confirmed.
    pub fn append_entries(
        &mut self,
        truncate_from: Option<LogIndex>,
        entries: &[Entry],
    ) -> io::Result<()> {
        if let Some(from) = truncate_from {
            self.truncate_by_rewrite(from)?;
        }
        if entries.is_empty() {
            return Ok(());
        }
        let mut buf = String::new();
        for e in entries {
            buf.push_str(&frame_line(e)?);
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(LOG))?;
        f.write_all(buf.as_bytes())?;
        f.sync_all()?;
        Ok(())
    }

    /// Rewrite the log as its surviving prefix (< `from`): write a complete
    /// `.new`, fsync, close, rename over `log.jsonl`.
    fn truncate_by_rewrite(&mut self, from: LogIndex) -> io::Result<()> {
        let survivors: Vec<Entry> = recover_log(&self.dir)?
            .into_iter()
            .filter(|e| e.index < from)
            .collect();
        let tmp = self.dir.join(LOG_NEW);
        let mut f = File::create(&tmp)?;
        let mut buf = String::new();
        for e in &survivors {
            buf.push_str(&frame_line(e)?);
        }
        f.write_all(buf.as_bytes())?;
        f.sync_all()?;
        drop(f); // Windows requires closing the handle before the rename.
        fs::rename(&tmp, self.dir.join(LOG))
    }
}

/// Torn-tail recovery scans `log.jsonl`, verifying CRC and index contiguity.
/// Only an unterminated final tail can result from an interrupted append and
/// is truncated. Every newline-terminated invalid frame is durable corruption:
/// recovery fails without modifying the file, even when it is the final frame.
fn recover_log(dir: &Path) -> io::Result<Vec<Entry>> {
    let path = dir.join(LOG);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut entries: Vec<Entry> = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let rest = &bytes[offset..];
        let Some(nl) = rest.iter().position(|&b| b == b'\n') else {
            truncate_at(&path, offset, "partial line (no trailing newline)")?;
            break;
        };
        let prev_index = entries.last().map_or(0, |e| e.index);
        match parse_frame(&rest[..nl], prev_index) {
            Ok(entry) => entries.push(entry),
            Err(why) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "log.jsonl corrupt at byte offset {offset}: {why}; refusing to modify newline-terminated data"
                    ),
                ));
            }
        }
        offset += nl + 1;
    }
    Ok(entries)
}

fn parse_frame(line: &[u8], prev_index: LogIndex) -> Result<Entry, String> {
    let frame: Frame =
        serde_json::from_slice(line).map_err(|e| format!("unparseable frame: {e}"))?;
    let entry_json =
        serde_json::to_string(&frame.entry).map_err(|e| format!("re-serialize failed: {e}"))?;
    let computed = crc32(entry_json.as_bytes());
    if computed != frame.crc {
        return Err(format!(
            "crc mismatch: stored {}, computed {computed}",
            frame.crc
        ));
    }
    if frame.entry.index != prev_index + 1 {
        return Err(format!(
            "index gap: {} follows {prev_index}",
            frame.entry.index
        ));
    }
    Ok(frame.entry)
}

fn truncate_at(path: &Path, offset: usize, why: &str) -> io::Result<()> {
    warn!(
        path = %path.display(),
        offset,
        why,
        "torn tail: truncating unterminated final log data"
    );
    let f = OpenOptions::new().write(true).open(path)?;
    f.set_len(offset as u64)?;
    f.sync_all()
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::path::Path;
    use std::sync::atomic::{AtomicU32, Ordering};

    use raft_core::Command;

    use super::*;

    static TEST_DIR_SEQ: AtomicU32 = AtomicU32::new(0);

    /// Per-test data dir under the workspace-root `data/` (gitignored).
    /// Declare it BEFORE the `Storage` in each test so the `Storage` (and any
    /// file handles) drop first: Windows refuses to remove a directory whose
    /// files are still open.
    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let n = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
            let path = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../data")
                .join(format!("test-{}-{n}", std::process::id()));
            fs::create_dir_all(&path).expect("create test dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            if let Err(e) = fs::remove_dir_all(&self.0) {
                eprintln!(
                    "warning: failed to remove test dir {}: {e}",
                    self.0.display()
                );
            }
        }
    }

    /// Deterministic entry content so recovered logs can be compared exactly.
    fn entry(index: LogIndex, term: u64) -> Entry {
        Entry {
            index,
            term,
            command: Command::Put {
                key: format!("k{index}"),
                value: format!("v{index}-t{term}"),
            },
        }
    }

    fn entries(range: std::ops::RangeInclusive<u64>, term: u64) -> Vec<Entry> {
        range.map(|i| entry(i, term)).collect()
    }

    /// Byte offset where the last line of the file starts.
    fn last_line_start(bytes: &[u8]) -> usize {
        bytes[..bytes.len() - 1]
            .iter()
            .rposition(|&b| b == b'\n')
            .map_or(0, |p| p + 1)
    }

    fn assert_invalid_recovery_preserves_file(
        dir: &TestDir,
        expected_bytes: &[u8],
        expected_reason: &str,
    ) {
        let error = match Storage::open(dir.path()) {
            Ok(_) => panic!("newline-terminated corruption must fail recovery"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error.to_string().contains(expected_reason),
            "unexpected recovery error: {error}"
        );
        assert!(
            error
                .to_string()
                .contains("refusing to modify newline-terminated data"),
            "unexpected recovery error: {error}"
        );
        assert_eq!(
            fs::read(dir.path().join(LOG)).unwrap(),
            expected_bytes,
            "failed recovery must leave the corrupt file untouched"
        );
    }

    #[test]
    fn log_roundtrip_replay() {
        let dir = TestDir::new();
        let (mut s, hard, log) = Storage::open(dir.path()).unwrap();
        assert_eq!(hard, HardState::default(), "fresh dir recovers {{0, null}}");
        assert!(log.is_empty());
        s.append_entries(None, &entries(1..=34, 1)).unwrap();
        drop(s);

        let (mut s, _, log) = Storage::open(dir.path()).unwrap();
        assert_eq!(log.len(), 34);
        s.append_entries(None, &entries(35..=67, 2)).unwrap();
        drop(s);

        let (mut s, _, log) = Storage::open(dir.path()).unwrap();
        assert_eq!(log.len(), 67);
        s.append_entries(None, &entries(68..=100, 3)).unwrap();
        drop(s);

        let (_s, _, log) = Storage::open(dir.path()).unwrap();
        assert_eq!(
            log.len(),
            100,
            "all 100 entries survive 3 open/append cycles"
        );
        for (i, e) in log.iter().enumerate() {
            assert_eq!(e.index, i as u64 + 1, "strictly ordered, contiguous");
            assert_eq!(*e, entry(e.index, e.term), "content roundtrips exactly");
        }
    }

    #[test]
    fn unterminated_tail_is_truncated_on_recovery() {
        // Crash mid-append: the file ends in a partial line.
        let dir = TestDir::new();
        let (mut s, _, _) = Storage::open(dir.path()).unwrap();
        s.append_entries(None, &entries(1..=10, 1)).unwrap();
        drop(s);
        let log_path = dir.path().join("log.jsonl");
        let bytes = fs::read(&log_path).unwrap();
        let lls = last_line_start(&bytes);
        let cut = lls + (bytes.len() - lls) / 2;
        OpenOptions::new()
            .write(true)
            .open(&log_path)
            .unwrap()
            .set_len(cut as u64)
            .unwrap();
        let (_s, _, log) = Storage::open(dir.path()).unwrap();
        assert_eq!(log.len(), 9, "partial last line must be cut");
        assert_eq!(log.last().unwrap().index, 9);
        assert_eq!(
            fs::metadata(&log_path).unwrap().len(),
            lls as u64,
            "file must be physically truncated at the bad line's offset"
        );
    }

    #[test]
    fn newline_terminated_final_crc_error_fails_without_modifying_log() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).unwrap();
        storage.append_entries(None, &entries(1..=10, 1)).unwrap();
        drop(storage);

        let log_path = dir.path().join(LOG);
        let mut corrupted = fs::read(&log_path).unwrap();
        let last_start = last_line_start(&corrupted);
        let payload_at = last_start
            + corrupted[last_start..]
                .windows(8)
                .position(|w| w == b"\"entry\":")
                .expect("frame format carries an entry field")
            + 8
            + 10;
        corrupted[payload_at] ^= 0x01;
        fs::write(&log_path, &corrupted).unwrap();

        assert_invalid_recovery_preserves_file(&dir, &corrupted, "crc mismatch");
    }

    #[test]
    fn newline_terminated_final_json_error_fails_without_modifying_log() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).unwrap();
        storage.append_entries(None, &entries(1..=2, 1)).unwrap();
        drop(storage);

        let log_path = dir.path().join(LOG);
        let mut corrupted = fs::read(&log_path).unwrap();
        corrupted.extend_from_slice(b"{not-json}\n");
        fs::write(&log_path, &corrupted).unwrap();

        assert_invalid_recovery_preserves_file(&dir, &corrupted, "unparseable frame");
    }

    #[test]
    fn newline_terminated_final_index_gap_fails_without_modifying_log() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).unwrap();
        storage.append_entries(None, &entries(1..=2, 1)).unwrap();
        drop(storage);

        let log_path = dir.path().join(LOG);
        let mut corrupted = fs::read(&log_path).unwrap();
        corrupted.extend_from_slice(frame_line(&entry(4, 1)).unwrap().as_bytes());
        fs::write(&log_path, &corrupted).unwrap();

        assert_invalid_recovery_preserves_file(&dir, &corrupted, "index gap: 4 follows 2");
    }

    #[test]
    fn interior_corruption_fails_without_modifying_log() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).unwrap();
        storage.append_entries(None, &entries(1..=3, 1)).unwrap();
        drop(storage);

        let log_path = dir.path().join(LOG);
        let mut corrupted = fs::read(&log_path).unwrap();
        let first_end = corrupted.iter().position(|&b| b == b'\n').unwrap() + 1;
        let second_end = first_end
            + corrupted[first_end..]
                .iter()
                .position(|&b| b == b'\n')
                .unwrap()
            + 1;
        let payload_at = first_end
            + corrupted[first_end..second_end]
                .windows(8)
                .position(|w| w == b"\"entry\":")
                .expect("frame format carries an entry field")
            + 8
            + 10;
        corrupted[payload_at] ^= 0x01;
        fs::write(&log_path, &corrupted).unwrap();

        let error = match Storage::open(dir.path()) {
            Ok(_) => panic!("interior corruption must fail recovery"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error
            .to_string()
            .contains("refusing to modify newline-terminated data"));
        assert_eq!(
            fs::read(&log_path).unwrap(),
            corrupted,
            "failed recovery must leave the corrupt file untouched"
        );
    }

    #[test]
    fn hardstate_persist_roundtrip() {
        let dir = TestDir::new();
        let (mut s, hard, _) = Storage::open(dir.path()).unwrap();
        assert_eq!(
            hard,
            HardState::default(),
            "missing file recovers {{0, null}}"
        );
        s.save_hard_state(&HardState {
            current_term: 7,
            voted_for: Some(2),
        })
        .unwrap();
        drop(s);

        let (s, hard, _) = Storage::open(dir.path()).unwrap();
        assert_eq!(
            hard,
            HardState {
                current_term: 7,
                voted_for: Some(2)
            }
        );
        drop(s);

        // A crash between write and rename leaves a stale .new; recovery must
        // trust hardstate.json and ignore/remove the orphan.
        let stale = dir.path().join("hardstate.json.new");
        fs::write(&stale, br#"{"current_term":9999,"voted_for":3}"#).unwrap();
        let (_s, hard, _) = Storage::open(dir.path()).unwrap();
        assert_eq!(
            hard,
            HardState {
                current_term: 7,
                voted_for: Some(2)
            },
            "the orphaned .new must never be trusted"
        );
        assert!(!stale.exists(), "stale .new is removed on open");
    }

    #[test]
    fn hardstate_replaces_existing_file_repeatedly() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).unwrap();
        for term in 1..=5 {
            storage
                .save_hard_state(&HardState {
                    current_term: term,
                    voted_for: Some((term % 3 + 1) as u8),
                })
                .unwrap();
        }
        drop(storage);

        let (_storage, hard, _) = Storage::open(dir.path()).unwrap();
        assert_eq!(hard.current_term, 5);
        assert_eq!(hard.voted_for, Some(3));
    }

    #[test]
    fn failed_hardstate_swap_preserves_previous_state() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).unwrap();
        let original = HardState {
            current_term: 4,
            voted_for: Some(2),
        };
        storage.save_hard_state(&original).unwrap();

        let blocked_temp = dir.path().join(HARDSTATE_NEW);
        fs::create_dir(&blocked_temp).unwrap();
        let result = storage.save_hard_state(&HardState {
            current_term: 5,
            voted_for: Some(3),
        });
        assert!(result.is_err(), "the filesystem failure must be returned");
        fs::remove_dir(&blocked_temp).unwrap();
        drop(storage);

        let (_storage, recovered, _) = Storage::open(dir.path()).unwrap();
        assert_eq!(recovered, original, "the last completed swap remains valid");
    }

    #[test]
    fn truncate_from_rewrites_suffix() {
        let dir = TestDir::new();
        let (mut s, _, _) = Storage::open(dir.path()).unwrap();
        s.append_entries(None, &entries(1..=10, 1)).unwrap();
        // Conflict repair drops 6..=10 and installs a newer-term tail.
        s.append_entries(Some(6), &[entry(6, 2), entry(7, 2)])
            .unwrap();
        drop(s);

        let (_s, _, log) = Storage::open(dir.path()).unwrap();
        assert_eq!(log.len(), 7, "recovery yields indices 1..=7");
        assert_eq!(
            log.iter().map(|e| e.index).collect::<Vec<_>>(),
            (1..=7).collect::<Vec<_>>()
        );
        assert_eq!(
            log[4].term, 1,
            "prefix below the truncation point untouched"
        );
        assert_eq!(
            (log[5].term, log[6].term),
            (2, 2),
            "new tail carries the new term"
        );
    }
}
