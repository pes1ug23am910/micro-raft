//! Durable storage for Raft's persistent state.
//!
//! This module contains no consensus logic; it only executes durable writes
//! requested by the driver.
//! Layout per node data dir:
//! - `hardstate.json` — atomic-swap-updated JSON of [`HardState`]
//! - `log.jsonl` — append-only, one CRC-framed JSON line per [`Entry`]:
//!   `{"crc":<u32>,"entry":{...}}\n`, `crc` over the serialized entry bytes
//! - After compaction, `CURRENT` selects one immutable generation snapshot and
//!   its mutable generation WAL; both files are synced before selection.
//!
//! Content is synced before rename; directory metadata is synced after name
//! creation, replacement and orphan removal. Unsupported barriers return errors.

use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use raft_core::{Entry, HardState, LogIndex};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::crc::crc32;
use crate::durability;
use crate::snapshot::{
    invalid, LimitedWriter, SnapshotDescriptor, SnapshotImage, MAX_SNAPSHOT_BYTES,
    MAX_SNAPSHOT_WAL_BYTES,
};

const HARDSTATE: &str = "hardstate.json";
const HARDSTATE_NEW: &str = "hardstate.json.new";
const LOG: &str = "log.jsonl";
const LOG_NEW: &str = "log.jsonl.new";
const CURRENT: &str = "CURRENT";
const CURRENT_NEW: &str = "CURRENT.new";

// The outer version intentionally lacks raw HardState fields. Old binaries fail
// decoding it instead of silently ignoring dynamic membership authority.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HardStateEnvelope {
    version: u32,
    crc: u32,
    hard_state: HardState,
}
fn encode_hard_state(hard: &HardState) -> io::Result<Vec<u8>> {
    if hard.membership.is_none() {
        return serde_json::to_vec(hard).map_err(io::Error::other);
    }
    let encoded = serde_json::to_vec(hard)?;
    serde_json::to_vec(&HardStateEnvelope {
        version: 2,
        crc: crc32(&encoded),
        hard_state: hard.clone(),
    })
    .map_err(io::Error::other)
}
fn decode_hard_state(bytes: &[u8]) -> io::Result<HardState> {
    let value: serde_json::Value = serde_json::from_slice(bytes)?;
    if value.get("version").is_some() || value.get("hard_state").is_some() {
        let frame: HardStateEnvelope = serde_json::from_value(value)?;
        if frame.version != 2
            || frame.hard_state.membership.is_none()
            || frame.crc != crc32(&serde_json::to_vec(&frame.hard_state)?)
        {
            return Err(invalid("hard-state envelope version or checksum mismatch"));
        }
        frame
            .hard_state
            .membership
            .as_ref()
            .expect("checked")
            .validate()
            .map_err(invalid)?;
        Ok(frame.hard_state)
    } else {
        if value.get("membership").is_some() {
            return Err(invalid("membership requires hard-state version 2 envelope"));
        }
        serde_json::from_value(value).map_err(|e| invalid(e.to_string()))
    }
}

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

/// Durable operations owned exclusively by the driver. The trait permits
/// deterministic I/O failure and ordering tests without a second disk writer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct StorageMetrics {
    /// Actual sync_all calls for appending WAL records. Excludes rewrites,
    /// snapshots, hard state, directory barriers and the final shutdown flush.
    pub wal_append_sync_attempts: u64,
    pub wal_append_sync_completed: u64,
    pub wal_append_bytes: u64,
    pub wal_append_records: u64,
}

pub trait DurableStore {
    fn metrics(&self) -> StorageMetrics {
        StorageMetrics::default()
    }
    fn save_hard_state(&mut self, hard: &HardState) -> io::Result<()>;
    fn append_entries(
        &mut self,
        truncate_from: Option<LogIndex>,
        entries: &[Entry],
    ) -> io::Result<()>;
    fn flush(&mut self) -> io::Result<()>;
    fn publish_snapshot(
        &mut self,
        _image: &SnapshotImage,
        _retained_suffix: &[Entry],
    ) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "this durable store does not support snapshot publication",
        ))
    }
}
pub struct RecoveredState {
    pub hard_state: HardState,
    pub entries: Vec<Entry>,
    pub snapshot: Option<SnapshotImage>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    generation: u64,
    snapshot: SnapshotDescriptor,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestFrame {
    crc: u32,
    manifest: Manifest,
}

pub struct Storage {
    dir: PathBuf,
    log_name: String,
    manifest: Option<Manifest>,
    poisoned: bool,
    metrics: StorageMetrics,
}

impl Storage {
    /// Opens (creating if needed) a node data dir; returns recovered state.
    /// Missing `hardstate.json` recovers as the default `{0, null}`; the log
    /// is replayed with CRC + index-contiguity checks and torn-tail truncation.
    pub fn open(dir: impl Into<PathBuf>) -> io::Result<(Storage, HardState, Vec<Entry>)> {
        let (storage, recovered) = Self::open_recovered(dir)?;
        if recovered.snapshot.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "snapshot state requires Storage::open_recovered",
            ));
        }
        Ok((storage, recovered.hard_state, recovered.entries))
    }

    pub fn open_recovered(dir: impl Into<PathBuf>) -> io::Result<(Storage, RecoveredState)> {
        let dir = dir.into();
        durability::create_dir_all(&dir)?;
        durability::sync_directory(&dir)?;

        // A leftover .new means a crash between write and rename. The swap
        // never completed, so hardstate.json is authoritative and the orphan
        // must not be trusted (hardstate_persist_roundtrip).
        for name in [HARDSTATE_NEW, LOG_NEW] {
            let orphan = dir.join(name);
            match fs::remove_file(&orphan) {
                Ok(()) => {
                    warn!(path = %orphan.display(), "removed stale temporary file from interrupted publication");
                    durability::sync_directory(&dir)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }

        let hard: HardState = match fs::read(dir.join(HARDSTATE)) {
            Ok(bytes) => decode_hard_state(&bytes).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("hardstate.json corrupt: {e}"),
                )
            })?,
            Err(e) if e.kind() == io::ErrorKind::NotFound => HardState::default(),
            Err(e) => return Err(e),
        };

        let manifest = read_manifest(&dir)?;
        let (snapshot, log_name, base) = if let Some(manifest) = &manifest {
            let image = SnapshotImage::decode(read_bounded(
                &dir.join(snapshot_name(manifest.generation)),
                MAX_SNAPSHOT_BYTES,
            )?)?;
            if image.descriptor() != &manifest.snapshot {
                return Err(invalid("CURRENT descriptor differs from snapshot image"));
            }
            if hard.current_term < manifest.snapshot.metadata.last_included_term {
                return Err(invalid("snapshot term exceeds durable hard state"));
            }
            (
                Some(image),
                wal_name(manifest.generation),
                manifest.snapshot.metadata.last_included_index,
            )
        } else {
            (None, LOG.to_owned(), 0)
        };
        let entries = recover_log_at(&dir.join(&log_name), base, manifest.is_some())?;
        validate_terms(
            &entries,
            manifest
                .as_ref()
                .map_or(0, |m| m.snapshot.metadata.last_included_term),
            hard.current_term,
        )?;
        let mut storage = Storage {
            dir,
            log_name,
            manifest,
            poisoned: false,
            metrics: StorageMetrics::default(),
        };
        // Only validated active state permits deletion of unpublished or obsolete files.
        storage.reclaim_with_hook(|_| Ok(()))?;
        Ok((
            storage,
            RecoveredState {
                hard_state: hard,
                entries,
                snapshot,
            },
        ))
    }

    /// Atomic swap: truncate-write `hardstate.json.new`, fsync, rename
    /// over `hardstate.json`, then directory-sync. A crash leaves either the old or new file,
    /// never a hybrid. `std::fs::rename` replaces the destination on Windows
    /// (MoveFileEx semantics) as long as the destination is not open.
    pub fn save_hard_state(&mut self, hs: &HardState) -> io::Result<()> {
        self.ensure_usable()?;
        if hs.current_term
            < self
                .manifest
                .as_ref()
                .map_or(0, |m| m.snapshot.metadata.last_included_term)
        {
            return Err(invalid("hard state cannot precede snapshot term"));
        }
        let result =
            durability::publish_file(&self.dir, HARDSTATE_NEW, HARDSTATE, &encode_hard_state(hs)?);
        self.mutation_result(result)
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
        self.ensure_usable()?;
        if truncate_from.is_some_and(|from| from <= self.base_index())
            || entries
                .first()
                .is_some_and(|entry| entry.index <= self.base_index())
        {
            return Err(invalid("cannot rewrite compacted snapshot prefix"));
        }
        let result = self.append_inner(truncate_from, entries);
        self.mutation_result(result)
    }
    fn append_inner(
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
        let log_existed = self.dir.join(&self.log_name).try_exists()?;
        if !log_existed && self.manifest.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "selected generation WAL is missing",
            ));
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.dir.join(&self.log_name))?;
        f.write_all(buf.as_bytes())?;
        self.metrics.wal_append_bytes += buf.len() as u64;
        self.metrics.wal_append_records += entries.len() as u64;
        self.metrics.wal_append_sync_attempts += 1;
        f.sync_all()?;
        self.metrics.wal_append_sync_completed += 1;
        if !log_existed {
            durability::sync_directory(&self.dir)?;
        }
        Ok(())
    }

    /// Final content-sync barrier for files that exist at shutdown.
    ///
    /// Writes already sync before returning; this explicit boundary lets the
    /// driver report a final I/O failure. Directory metadata is also synced.
    /// This is not a physical power-loss test. An unused store creates no files.
    pub fn flush(&mut self) -> io::Result<()> {
        self.ensure_usable()?;
        let result = self.flush_inner();
        self.mutation_result(result)
    }
    fn flush_inner(&mut self) -> io::Result<()> {
        let mut names = vec![HARDSTATE.to_owned(), self.log_name.clone()];
        if let Some(manifest) = &self.manifest {
            names.push(CURRENT.to_owned());
            names.push(snapshot_name(manifest.generation));
        }
        for name in names {
            let path = self.dir.join(name);
            match OpenOptions::new().write(true).open(&path) {
                Ok(file) => file.sync_all().map_err(|error| {
                    io::Error::new(
                        error.kind(),
                        format!("shutdown sync {}: {error}", path.display()),
                    )
                })?,
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound && self.manifest.is_none() => {}
                Err(error) => {
                    return Err(io::Error::new(
                        error.kind(),
                        format!("shutdown sync open {}: {error}", path.display()),
                    ));
                }
            }
        }
        durability::sync_directory(&self.dir)
    }
    /// Rewrite the log as its surviving prefix (< `from`): write a complete
    /// `.new`, fsync, close, rename over `log.jsonl`.
    fn truncate_by_rewrite(&mut self, from: LogIndex) -> io::Result<()> {
        let survivors: Vec<Entry> = recover_log_at(
            &self.dir.join(&self.log_name),
            self.base_index(),
            self.manifest.is_some(),
        )?
        .into_iter()
        .filter(|e| e.index < from)
        .collect();
        let mut buf = String::new();
        for e in &survivors {
            buf.push_str(&frame_line(e)?);
        }
        durability::publish_file(
            &self.dir,
            &format!("{}.new", self.log_name),
            &self.log_name,
            buf.as_bytes(),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SnapshotPublicationStep {
    SnapshotWritten,
    SnapshotSynced,
    WalWritten,
    WalSynced,
    PreparedDirectorySynced,
    CurrentWritten,
    CurrentSynced,
    CurrentRenamed,
    CurrentDirectorySynced,
    PriorFileRemoved,
    ReclaimDirectorySynced,
}
impl Storage {
    fn ensure_usable(&self) -> io::Result<()> {
        if self.poisoned {
            Err(io::Error::other(
                "store requires reopen after failed durable mutation",
            ))
        } else {
            Ok(())
        }
    }
    fn mutation_result<T>(&mut self, result: io::Result<T>) -> io::Result<T> {
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn base_index(&self) -> LogIndex {
        self.manifest
            .as_ref()
            .map_or(0, |manifest| manifest.snapshot.metadata.last_included_index)
    }
    /// Driver fences application/commit and supplies the core's exact suffix.
    /// Storage independently verifies the suffix before preparing either file.
    pub fn publish_snapshot(
        &mut self,
        image: &SnapshotImage,
        retained_suffix: &[Entry],
    ) -> io::Result<()> {
        self.publish_snapshot_with_hook(image, retained_suffix, |_| Ok(()))
    }
    pub(crate) fn publish_snapshot_with_hook(
        &mut self,
        image: &SnapshotImage,
        retained_suffix: &[Entry],
        mut hook: impl FnMut(SnapshotPublicationStep) -> io::Result<()>,
    ) -> io::Result<()> {
        self.ensure_usable()?;
        let descriptor = image.descriptor();
        descriptor.validate().map_err(invalid)?;
        let boundary = &descriptor.metadata;
        if boundary.last_included_index <= self.base_index() {
            return Err(invalid("snapshot is stale"));
        }
        let hard = decode_hard_state(&fs::read(self.dir.join(HARDSTATE))?)?;
        if let Some(previous) = self
            .manifest
            .as_ref()
            .map(|manifest| &manifest.snapshot.metadata)
        {
            match (&previous.membership, &boundary.membership) {
                (Some(before), Some(after)) if after.is_consistent_extension_of(before) => {}
                (None, Some(after)) if previous.members == after.state.genesis_voters => {}
                (None, None) if previous.members == boundary.members => {}
                _ => {
                    return Err(invalid(
                        "snapshot membership history is not a consistent extension",
                    ))
                }
            }
        }
        if let Some(before) = &hard.membership {
            match &boundary.membership {
                Some(after) if after.is_consistent_extension_of(before) => {}
                None if before.index == 0 && boundary.members == before.state.genesis_voters => {}
                _ => return Err(invalid("snapshot contradicts durable committed membership")),
            }
        }
        if hard.current_term < boundary.last_included_term {
            return Err(invalid("snapshot term exceeds durable hard state"));
        }
        let entries = recover_log_at(
            &self.dir.join(&self.log_name),
            self.base_index(),
            self.manifest.is_some(),
        )?;
        validate_terms(
            &entries,
            self.manifest
                .as_ref()
                .map_or(0, |m| m.snapshot.metadata.last_included_term),
            hard.current_term,
        )?;
        let matched = entries.iter().any(|entry| {
            entry.index == boundary.last_included_index && entry.term == boundary.last_included_term
        });
        let derived: Vec<_> = if matched {
            entries
                .into_iter()
                .filter(|entry| entry.index > boundary.last_included_index)
                .collect()
        } else {
            Vec::new()
        };
        if derived != retained_suffix {
            return Err(invalid(
                "retained WAL suffix differs from matching-boundary rule",
            ));
        }
        let mut wal = LimitedWriter::new(MAX_SNAPSHOT_WAL_BYTES);
        for entry in retained_suffix {
            wal.write_all(frame_line(entry)?.as_bytes())?;
        }
        let wal = wal.into_inner();
        let generation = self
            .manifest
            .as_ref()
            .map_or(Some(1), |manifest| manifest.generation.checked_add(1))
            .ok_or_else(|| invalid("snapshot generation exhausted"))?;
        let manifest = Manifest {
            version: 1,
            generation,
            snapshot: descriptor.clone(),
        };
        let result = (|| {
            write_immutable(
                &self.dir.join(snapshot_name(generation)),
                image.bytes(),
                || hook(SnapshotPublicationStep::SnapshotWritten),
            )?;
            hook(SnapshotPublicationStep::SnapshotSynced)?;
            write_immutable(&self.dir.join(wal_name(generation)), &wal, || {
                hook(SnapshotPublicationStep::WalWritten)
            })?;
            hook(SnapshotPublicationStep::WalSynced)?;
            durability::sync_directory(&self.dir)?;
            hook(SnapshotPublicationStep::PreparedDirectorySynced)?;
            let manifest_bytes = serde_json::to_vec(&manifest)?;
            let frame = serde_json::to_vec(&ManifestFrame {
                crc: crc32(&manifest_bytes),
                manifest: manifest.clone(),
            })?;
            durability::publish_with_hook(&self.dir, CURRENT_NEW, CURRENT, &frame, |step| {
                hook(match step {
                    durability::PublicationStep::ContentWritten => {
                        SnapshotPublicationStep::CurrentWritten
                    }
                    durability::PublicationStep::ContentSynced => {
                        SnapshotPublicationStep::CurrentSynced
                    }
                    durability::PublicationStep::Renamed => SnapshotPublicationStep::CurrentRenamed,
                    durability::PublicationStep::DirectorySynced => {
                        SnapshotPublicationStep::CurrentDirectorySynced
                    }
                })
            })?;
            self.log_name = wal_name(generation);
            self.manifest = Some(manifest);
            self.reclaim_with_hook(&mut hook)
        })();
        self.mutation_result(result)
    }

    /// Exact reserved filenames only; no recursive deletion or metadata paths.
    /// Run after active state validation, and after CURRENT's directory barrier.
    fn reclaim_with_hook(
        &mut self,
        mut hook: impl FnMut(SnapshotPublicationStep) -> io::Result<()>,
    ) -> io::Result<()> {
        let selected = self
            .manifest
            .as_ref()
            .map(|manifest| snapshot_name(manifest.generation));
        let result = (|| {
            for item in fs::read_dir(&self.dir)? {
                let item = item?;
                let filename = item.file_name();
                let Some(name) = filename.to_str() else {
                    continue;
                };
                let reserved = name == CURRENT_NEW
                    || is_generation_file(name)
                    || (name == LOG && self.manifest.is_some());
                if reserved && name != self.log_name && selected.as_deref() != Some(name) {
                    fs::remove_file(item.path())?;
                    hook(SnapshotPublicationStep::PriorFileRemoved)?;
                }
            }
            durability::sync_directory(&self.dir)?;
            hook(SnapshotPublicationStep::ReclaimDirectorySynced)
        })();
        self.mutation_result(result)
    }
}
fn snapshot_name(generation: u64) -> String {
    format!("generation-{generation:020}.snapshot")
}
fn wal_name(generation: u64) -> String {
    format!("generation-{generation:020}.wal")
}
fn is_generation_file(name: &str) -> bool {
    let Some(tail) = name.strip_prefix("generation-") else {
        return false;
    };
    let Some((digits, extension)) = tail.split_once('.') else {
        return false;
    };
    digits.len() == 20
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && matches!(extension, "snapshot" | "wal" | "wal.new")
}
fn write_immutable(
    path: &Path,
    bytes: &[u8],
    written: impl FnOnce() -> io::Result<()>,
) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(bytes)?;
    written()?;
    file.sync_all()
}
fn read_manifest(dir: &Path) -> io::Result<Option<Manifest>> {
    if !dir.join(CURRENT).try_exists()? {
        return Ok(None);
    }
    let frame: ManifestFrame = serde_json::from_slice(&read_bounded(&dir.join(CURRENT), 8192)?)?;
    if frame.manifest.version != 1
        || frame.manifest.generation == 0
        || frame.crc != crc32(&serde_json::to_vec(&frame.manifest)?)
    {
        return Err(invalid(
            "CURRENT manifest version, generation or checksum invalid",
        ));
    }
    frame.manifest.snapshot.validate().map_err(invalid)?;
    Ok(Some(frame.manifest))
}
fn read_bounded(path: &Path, maximum: usize) -> io::Result<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    fs::File::open(path)?
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > maximum {
        return Err(invalid("snapshot file exceeds byte bound"));
    }
    Ok(bytes)
}

fn validate_terms(entries: &[Entry], base_term: u64, current_term: u64) -> io::Result<()> {
    let mut previous = base_term;
    for entry in entries {
        if entry.term < previous || entry.term > current_term {
            return Err(invalid(format!(
                "log term {} at index {} is outside [{previous}, {current_term}]",
                entry.term, entry.index
            )));
        }
        previous = entry.term;
    }
    Ok(())
}

impl DurableStore for Storage {
    fn metrics(&self) -> StorageMetrics {
        self.metrics
    }
    fn publish_snapshot(
        &mut self,
        image: &SnapshotImage,
        retained_suffix: &[Entry],
    ) -> io::Result<()> {
        Storage::publish_snapshot(self, image, retained_suffix)
    }

    fn save_hard_state(&mut self, hard: &HardState) -> io::Result<()> {
        Storage::save_hard_state(self, hard)
    }

    fn append_entries(
        &mut self,
        truncate_from: Option<LogIndex>,
        entries: &[Entry],
    ) -> io::Result<()> {
        Storage::append_entries(self, truncate_from, entries)
    }

    fn flush(&mut self) -> io::Result<()> {
        Storage::flush(self)
    }
}
/// Torn-tail recovery scans `log.jsonl`, verifying CRC and index contiguity.
/// Only an unterminated final tail can result from an interrupted append and
/// is truncated. Every newline-terminated invalid frame is durable corruption:
/// recovery fails without modifying the file, even when it is the final frame.
fn recover_log_at(path: &Path, base: LogIndex, required: bool) -> io::Result<Vec<Entry>> {
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound && !required => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut entries: Vec<Entry> = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let rest = &bytes[offset..];
        let Some(nl) = rest.iter().position(|&b| b == b'\n') else {
            truncate_at(path, offset, "partial line (no trailing newline)")?;
            break;
        };
        let prev_index = entries.last().map_or(base, |e| e.index);
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
    if Some(frame.entry.index) != prev_index.checked_add(1) {
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
mod snapshot_tests {
    use super::*;
    use crate::application::StateMachine;
    use crate::snapshot::SnapshotMetadata;
    use raft_core::Command;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct TestDir(PathBuf);
    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "micro-raft-generation-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            durability::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn log() -> Vec<Entry> {
        vec![
            Command::RegisterSession {
                nonce: "registered".into(),
            },
            Command::SessionPut {
                session_id: 1,
                key: "key".into(),
                sequence: 1,
                value: "retained".into(),
            },
            Command::SessionDelete {
                session_id: 1,
                key: "key".into(),
                sequence: 2,
            },
            Command::CloseSession { session_id: 1 },
            Command::Put {
                key: "after-close".into(),
                value: "local".into(),
            },
            Command::NoOp,
        ]
        .into_iter()
        .enumerate()
        .map(|(position, command)| Entry {
            index: position as u64 + 1,
            term: 2,
            command,
        })
        .collect()
    }
    fn snapshot(entries: &[Entry], term: u64) -> SnapshotImage {
        let mut application = StateMachine::default();
        for entry in entries {
            application.apply(entry).unwrap();
        }
        SnapshotImage::new(
            SnapshotMetadata {
                membership: None,
                last_included_index: application.last_applied(),
                last_included_term: term,
                members: vec![1, 2, 3],
            },
            &application,
        )
        .unwrap()
    }
    fn setup(dir: &TestDir) -> (Storage, Vec<Entry>) {
        let (mut storage, _, _) = Storage::open(&dir.0).unwrap();
        storage
            .save_hard_state(&HardState {
                membership: None,
                current_term: 5,
                voted_for: Some(2),
            })
            .unwrap();
        let entries = log();
        storage.append_entries(None, &entries).unwrap();
        (storage, entries)
    }
    #[test]
    fn publication_reopens_snapshot_sessions_and_matching_suffix_then_appends() {
        let dir = TestDir::new();
        let (mut storage, entries) = setup(&dir);
        let image = snapshot(&entries[..4], 2);
        storage.publish_snapshot(&image, &entries[4..]).unwrap();
        storage.flush().unwrap();
        drop(storage);
        assert!(!dir.0.join(LOG).exists());
        assert!(Storage::open(&dir.0).is_err());
        let (mut storage, recovered) = Storage::open_recovered(&dir.0).unwrap();
        assert_eq!(recovered.entries, entries[4..]);
        assert_eq!(
            recovered.snapshot.as_ref().unwrap().application(),
            image.application()
        );
        assert_eq!(
            recovered.hard_state,
            HardState {
                membership: None,
                current_term: 5,
                voted_for: Some(2)
            }
        );
        storage
            .append_entries(
                None,
                &[Entry {
                    index: 7,
                    term: 5,
                    command: Command::NoOp,
                }],
            )
            .unwrap();
        drop(storage);
        let (_, recovered) = Storage::open_recovered(&dir.0).unwrap();
        assert_eq!(recovered.entries.last().unwrap().index, 7);
    }
    #[test]
    fn publication_at_every_boundary_recovers_one_coherent_generation() {
        use SnapshotPublicationStep::*;
        for failure_at in [
            SnapshotWritten,
            SnapshotSynced,
            WalWritten,
            WalSynced,
            PreparedDirectorySynced,
            CurrentWritten,
            CurrentSynced,
            CurrentRenamed,
            CurrentDirectorySynced,
            PriorFileRemoved,
            ReclaimDirectorySynced,
        ] {
            let dir = TestDir::new();
            let (mut storage, entries) = setup(&dir);
            let old = snapshot(&entries[..2], 2);
            storage.publish_snapshot(&old, &entries[2..]).unwrap();
            // Incoming committed history disagrees with the old boundary term;
            // retaining any old suffix would be an invalid hybrid generation.
            let new = snapshot(&entries[..4], 3);
            let error = storage
                .publish_snapshot_with_hook(&new, &[], |step| {
                    if step == CurrentRenamed {
                        assert!(dir.0.join(snapshot_name(1)).exists());
                        assert!(dir.0.join(wal_name(1)).exists());
                    }
                    if step == failure_at {
                        Err(io::Error::other("injected publication interruption"))
                    } else {
                        Ok(())
                    }
                })
                .unwrap_err();
            assert!(error.to_string().contains("injected"));
            assert!(storage.flush().is_err());
            assert!(storage.append_entries(None, &[]).is_err());
            drop(storage);
            let (_, recovered) = Storage::open_recovered(&dir.0).unwrap();
            let published = matches!(
                failure_at,
                CurrentRenamed | CurrentDirectorySynced | PriorFileRemoved | ReclaimDirectorySynced
            );
            let expected = if published { &new } else { &old };
            assert_eq!(
                recovered.snapshot.as_ref().unwrap().descriptor(),
                expected.descriptor(),
                "{failure_at:?}"
            );
            assert_eq!(
                recovered.snapshot.as_ref().unwrap().application(),
                expected.application(),
                "{failure_at:?}"
            );
            assert_eq!(
                recovered.entries,
                if published {
                    vec![]
                } else {
                    entries[2..].to_vec()
                },
                "{failure_at:?}"
            );
            assert_eq!(
                recovered.hard_state,
                HardState {
                    membership: None,
                    current_term: 5,
                    voted_for: Some(2)
                }
            );
        }
    }
    #[test]
    fn first_publication_interruption_preserves_legacy_log_and_can_retry() {
        let dir = TestDir::new();
        let (mut storage, entries) = setup(&dir);
        let image = snapshot(&entries[..3], 2);
        storage
            .publish_snapshot_with_hook(&image, &entries[3..], |step| {
                if step == SnapshotPublicationStep::PreparedDirectorySynced {
                    Err(io::Error::other("interrupt"))
                } else {
                    Ok(())
                }
            })
            .unwrap_err();
        drop(storage);
        assert!(dir.0.join(LOG).exists());
        let (mut storage, recovered) = Storage::open_recovered(&dir.0).unwrap();
        assert!(recovered.snapshot.is_none());
        assert_eq!(recovered.entries, entries);
        assert!(!dir.0.join(snapshot_name(1)).exists());
        storage.publish_snapshot(&image, &entries[3..]).unwrap();
    }
    #[test]
    fn wrong_suffix_stale_boundary_term_and_membership_reject_before_publication() {
        let dir = TestDir::new();
        let (mut storage, entries) = setup(&dir);
        let image = snapshot(&entries[..3], 2);
        assert!(storage.publish_snapshot(&image, &[]).is_err());
        assert!(!dir.0.join(CURRENT).exists());
        assert!(storage
            .publish_snapshot(&snapshot(&entries[..3], 6), &[])
            .is_err());
        storage.publish_snapshot(&image, &entries[3..]).unwrap();
        assert!(storage.publish_snapshot(&image, &entries[3..]).is_err());
        let changed = SnapshotImage::new(
            SnapshotMetadata {
                membership: None,
                last_included_index: 4,
                last_included_term: 2,
                members: vec![1, 2],
            },
            snapshot(&entries[..4], 2).application(),
        )
        .unwrap();
        assert!(storage.publish_snapshot(&changed, &entries[4..]).is_err());
        assert!(storage.append_entries(Some(3), &[]).is_err());
        assert!(storage
            .save_hard_state(&HardState {
                membership: None,
                current_term: 1,
                voted_for: None
            })
            .is_err());
        storage.flush().unwrap();
    }
    #[test]
    fn selected_file_absence_or_corruption_fails_without_falling_back() {
        for kind in 0..4 {
            let dir = TestDir::new();
            let (mut storage, entries) = setup(&dir);
            storage
                .publish_snapshot(&snapshot(&entries[..3], 2), &entries[3..])
                .unwrap();
            drop(storage);
            let path = dir.0.join(match kind {
                0 => wal_name(1),
                1 => snapshot_name(1),
                _ => CURRENT.to_owned(),
            });
            if kind < 2 {
                fs::remove_file(path).unwrap();
            } else {
                let mut bytes = fs::read(&path).unwrap();
                if kind == 2 {
                    bytes[0] ^= 1;
                } else {
                    bytes.push(0);
                }
                fs::write(path, bytes).unwrap();
            }
            assert!(Storage::open_recovered(&dir.0).is_err());
        }
    }
    #[test]
    fn selected_suffix_torn_tail_is_repaired_but_complete_corruption_is_not() {
        let dir = TestDir::new();
        let (mut storage, entries) = setup(&dir);
        storage
            .publish_snapshot(&snapshot(&entries[..3], 2), &entries[3..])
            .unwrap();
        drop(storage);
        let path = dir.0.join(wal_name(1));
        let original = fs::read(&path).unwrap();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"partial")
            .unwrap();
        let (_, recovered) = Storage::open_recovered(&dir.0).unwrap();
        assert_eq!(recovered.entries, entries[3..]);
        assert_eq!(fs::read(&path).unwrap(), original);
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"corrupt\n")
            .unwrap();
        let corrupt = fs::read(&path).unwrap();
        assert!(Storage::open_recovered(&dir.0).is_err());
        assert_eq!(fs::read(&path).unwrap(), corrupt);
    }
    #[test]
    fn obsolete_reclamation_is_scoped_and_active_generation_survives_conflict_rewrite() {
        let dir = TestDir::new();
        let (mut storage, entries) = setup(&dir);
        fs::write(dir.0.join("generation-not-owned.snapshot"), b"keep").unwrap();
        storage
            .publish_snapshot(&snapshot(&entries[..2], 2), &entries[2..])
            .unwrap();
        storage
            .publish_snapshot(&snapshot(&entries[..4], 2), &entries[4..])
            .unwrap();
        assert!(!dir.0.join(snapshot_name(1)).exists());
        assert!(!dir.0.join(wal_name(1)).exists());
        assert_eq!(
            fs::read(dir.0.join("generation-not-owned.snapshot")).unwrap(),
            b"keep"
        );
        storage
            .append_entries(
                Some(5),
                &[Entry {
                    index: 5,
                    term: 5,
                    command: Command::NoOp,
                }],
            )
            .unwrap();
        drop(storage);
        let (_, recovered) = Storage::open_recovered(&dir.0).unwrap();
        assert_eq!(recovered.entries.len(), 1);
        assert_eq!(recovered.entries[0].term, 5);
    }

    #[test]
    fn reclamation_io_failure_poisoning_preserves_published_generation() {
        let dir = TestDir::new();
        let (mut storage, entries) = setup(&dir);
        let blocked = dir.0.join(snapshot_name(99));
        fs::create_dir(&blocked).unwrap();
        let image = snapshot(&entries[..3], 2);
        assert!(storage.publish_snapshot(&image, &entries[3..]).is_err());
        assert!(storage.save_hard_state(&HardState::default()).is_err());
        drop(storage);
        assert!(dir.0.join(CURRENT).exists());
        assert!(Storage::open_recovered(&dir.0).is_err());
        fs::remove_dir(blocked).unwrap();
        let (_, recovered) = Storage::open_recovered(&dir.0).unwrap();
        assert_eq!(recovered.snapshot.unwrap().descriptor(), image.descriptor());
        assert_eq!(recovered.entries, entries[3..]);
    }

    #[test]
    fn checksum_rejection_never_reclaims_unselected_evidence() {
        for corrupt_manifest in [true, false] {
            let dir = TestDir::new();
            let (mut storage, entries) = setup(&dir);
            storage
                .publish_snapshot(&snapshot(&entries[..3], 2), &entries[3..])
                .unwrap();
            drop(storage);
            let orphan = dir.0.join(snapshot_name(99));
            fs::write(&orphan, b"preserve until active state validates").unwrap();
            if corrupt_manifest {
                let path = dir.0.join(CURRENT);
                let mut frame: ManifestFrame =
                    serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                frame.crc ^= 1;
                fs::write(path, serde_json::to_vec(&frame).unwrap()).unwrap();
            } else {
                let path = dir.0.join(snapshot_name(1));
                let mut bytes = fs::read(&path).unwrap();
                let end = bytes.len() - 1;
                bytes[end] ^= 1;
                fs::write(path, bytes).unwrap();
            }
            assert!(Storage::open_recovered(&dir.0).is_err());
            assert!(orphan.exists());
        }
    }

    #[test]
    fn missing_selected_wal_is_never_recreated_by_append() {
        let dir = TestDir::new();
        let (mut storage, entries) = setup(&dir);
        storage
            .publish_snapshot(&snapshot(&entries[..3], 2), &entries[3..])
            .unwrap();
        let path = dir.0.join(wal_name(1));
        fs::remove_file(&path).unwrap();
        let error = storage
            .append_entries(
                None,
                &[Entry {
                    index: 7,
                    term: 5,
                    command: Command::NoOp,
                }],
            )
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert!(!path.exists());
        assert!(storage.flush().is_err());
    }

    #[test]
    fn invalid_suffix_terms_reject_before_reclaim_or_publication() {
        for selected in [false, true] {
            for corrupt_term in [1, 6] {
                let dir = TestDir::new();
                let (mut storage, mut entries) = setup(&dir);
                if selected {
                    storage
                        .publish_snapshot(&snapshot(&entries[..2], 2), &entries[2..])
                        .unwrap();
                }
                let log_path = dir.0.join(&storage.log_name);
                // Preserve valid framing/CRC/index sequence while violating only
                // the boundary monotonicity or durable current-term constraint.
                entries[3].term = corrupt_term;
                let begin = if selected { 2 } else { 0 };
                let bytes = entries[begin..]
                    .iter()
                    .map(|entry| frame_line(entry).unwrap())
                    .collect::<String>();
                fs::write(&log_path, bytes).unwrap();
                let orphan = dir.0.join(snapshot_name(99));
                fs::write(&orphan, b"must survive failed active-state validation").unwrap();
                let image = snapshot(&log()[..3], 2);
                assert!(storage.publish_snapshot(&image, &entries[3..]).is_err());
                assert!(orphan.exists());
                drop(storage);
                assert!(Storage::open_recovered(&dir.0).is_err());
                assert!(orphan.exists());
            }
        }
    }
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
    fn final_flush_preserves_existing_state_and_does_not_create_unused_files() {
        let dir = TestDir::new();
        let (mut store, _, _) = Storage::open(dir.path()).unwrap();
        store.flush().unwrap();
        assert!(!dir.path().join(HARDSTATE).exists());
        assert!(!dir.path().join(LOG).exists());
        let hard = HardState {
            membership: None,
            current_term: 4,
            voted_for: Some(2),
        };
        store.save_hard_state(&hard).unwrap();
        let expected = entries(1..=3, 4);
        store.append_entries(None, &expected).unwrap();
        let log_bytes = fs::read(dir.path().join(LOG)).unwrap();
        store.flush().unwrap();
        drop(store);
        let (_, recovered_hard, recovered_entries) = Storage::open(dir.path()).unwrap();
        assert_eq!(recovered_hard, hard);
        assert_eq!(recovered_entries, expected);
        assert_eq!(fs::read(dir.path().join(LOG)).unwrap(), log_bytes);
    }

    #[test]
    fn final_flush_propagates_an_existing_file_open_failure() {
        let dir = TestDir::new();
        let (mut store, _, _) = Storage::open(dir.path()).unwrap();
        fs::create_dir(dir.path().join(LOG)).unwrap();
        let error = store.flush().unwrap_err();
        assert!(error.to_string().contains("shutdown sync open"));
    }
    #[test]
    fn log_roundtrip_replay() {
        let dir = TestDir::new();
        let (mut s, hard, log) = Storage::open(dir.path()).unwrap();
        assert_eq!(hard, HardState::default(), "fresh dir recovers {{0, null}}");
        assert!(log.is_empty());
        s.save_hard_state(&HardState {
            membership: None,
            current_term: 3,
            voted_for: None,
        })
        .unwrap();
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
        s.save_hard_state(&HardState {
            membership: None,
            current_term: 2,
            voted_for: None,
        })
        .unwrap();
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
            membership: None,
            current_term: 7,
            voted_for: Some(2),
        })
        .unwrap();
        drop(s);

        let (s, hard, _) = Storage::open(dir.path()).unwrap();
        assert_eq!(
            hard,
            HardState {
                membership: None,
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
                membership: None,
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
                    membership: None,
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
            membership: None,
            current_term: 4,
            voted_for: Some(2),
        };
        storage.save_hard_state(&original).unwrap();

        let blocked_temp = dir.path().join(HARDSTATE_NEW);
        fs::create_dir(&blocked_temp).unwrap();
        let result = storage.save_hard_state(&HardState {
            membership: None,
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
        s.save_hard_state(&HardState {
            membership: None,
            current_term: 2,
            voted_for: None,
        })
        .unwrap();
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

    #[test]
    fn append_sync_metrics_count_real_group_barriers_not_records_or_other_syncs() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).unwrap();
        storage
            .save_hard_state(&HardState {
                membership: None,
                current_term: 1,
                voted_for: None,
            })
            .unwrap();
        assert_eq!(storage.metrics(), StorageMetrics::default());
        storage.append_entries(None, &entries(1..=16, 1)).unwrap();
        let first = storage.metrics();
        assert_eq!(first.wal_append_sync_attempts, 1);
        assert_eq!(first.wal_append_sync_completed, 1);
        assert_eq!(first.wal_append_records, 16);
        assert_eq!(
            first.wal_append_bytes,
            fs::metadata(dir.path().join(LOG)).unwrap().len()
        );
        storage.append_entries(None, &[]).unwrap();
        storage.flush().unwrap();
        assert_eq!(
            storage.metrics(),
            first,
            "final flush is intentionally a separate barrier class"
        );
        storage.append_entries(None, &entries(17..=17, 1)).unwrap();
        assert_eq!(storage.metrics().wal_append_sync_completed, 2);
        assert_eq!(storage.metrics().wal_append_records, 17);
    }
}
