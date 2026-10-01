//! Versioned, bounded application snapshots. No partial image is usable state.
use std::io::{self, Write};

use raft_core::{LogIndex, NodeId};
pub use raft_core::{
    SnapshotDescriptor, SnapshotMetadata, SnapshotTransferId, MAX_SNAPSHOT_BYTES,
    MAX_SNAPSHOT_CHUNK_BYTES,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::application::{ApplicationSnapshot, StateMachine};

pub const MAX_SNAPSHOT_WAL_BYTES: usize = 64 * 1024 * 1024;
const MAGIC: &[u8; 8] = b"MRFTSN01";
const MAGIC_V2: &[u8; 8] = b"MRFTSN02";
const HEADER_BYTES: usize = 48;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    version: u32,
    metadata: SnapshotMetadata,
    application: ApplicationSnapshot,
}

/// Construction and decoding always validate both application and framing.
#[derive(Clone, Debug)]
pub struct SnapshotImage {
    descriptor: SnapshotDescriptor,
    bytes: Vec<u8>,
    application: StateMachine,
}

impl SnapshotImage {
    pub fn new(metadata: SnapshotMetadata, application: &StateMachine) -> io::Result<Self> {
        metadata.validate().map_err(invalid)?;
        if metadata.last_included_index != application.last_applied() {
            return Err(invalid(
                "snapshot boundary differs from applied application state",
            ));
        }
        let payload = Payload {
            version: if metadata.membership.is_some() { 2 } else { 1 },
            metadata,
            application: application.export_snapshot(),
        };
        let mut encoded = LimitedWriter::new(MAX_SNAPSHOT_BYTES - HEADER_BYTES);
        serde_json::to_writer(&mut encoded, &payload)?;
        let body = encoded.into_inner();
        let mut bytes = Vec::with_capacity(HEADER_BYTES + body.len());
        bytes.extend_from_slice(if payload.version == 2 {
            MAGIC_V2
        } else {
            MAGIC
        });
        bytes.extend_from_slice(&(body.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&Sha256::digest(&body));
        bytes.extend_from_slice(&body);
        Self::decode(bytes)
    }

    pub fn decode(bytes: Vec<u8>) -> io::Result<Self> {
        if bytes.len() < HEADER_BYTES
            || bytes.len() > MAX_SNAPSHOT_BYTES
            || (&bytes[..8] != MAGIC && &bytes[..8] != MAGIC_V2)
        {
            return Err(invalid(
                "snapshot frame version, length or magic is invalid",
            ));
        }
        let declared = u64::from_be_bytes(bytes[8..16].try_into().expect("fixed header"));
        if declared != (bytes.len() - HEADER_BYTES) as u64 {
            return Err(invalid("snapshot payload length mismatch"));
        }
        if Sha256::digest(&bytes[HEADER_BYTES..])[..] != bytes[16..48] {
            return Err(invalid("snapshot payload checksum mismatch"));
        }
        let payload: Payload = serde_json::from_slice(&bytes[HEADER_BYTES..])?;
        if (payload.version != 1 && payload.version != 2)
            || (payload.version == 2) != payload.metadata.membership.is_some()
            || (payload.version == 2) != (&bytes[..8] == MAGIC_V2)
        {
            return Err(invalid("unsupported snapshot payload version"));
        }
        payload.metadata.validate().map_err(invalid)?;
        let application = StateMachine::import_snapshot(
            payload.application,
            payload.metadata.last_included_index,
        )
        .map_err(|error| invalid(error.to_string()))?;
        let descriptor = SnapshotDescriptor {
            metadata: payload.metadata,
            total_len: bytes.len() as u64,
            sha256: Sha256::digest(&bytes).into(),
        };
        Ok(Self {
            descriptor,
            bytes,
            application,
        })
    }

    pub fn descriptor(&self) -> &SnapshotDescriptor {
        &self.descriptor
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn application(&self) -> &StateMachine {
        &self.application
    }
    pub fn into_application(self) -> StateMachine {
        self.application
    }
}

pub(crate) fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Serialization stops at the bound rather than allocating an unlimited buffer.
pub(crate) struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl LimitedWriter {
    pub(crate) fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
    pub(crate) fn into_inner(self) -> Vec<u8> {
        self.bytes
    }
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot serialization exceeds configured byte bound",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StagedChunk {
    pub next_offset: u64,
    pub complete: bool,
}

/// Content rejection is recoverable; filesystem failure stops the receiver.
#[derive(Debug)]
pub enum SnapshotReceiveError {
    Rejected(String),
    Io(io::Error),
}
impl std::fmt::Display for SnapshotReceiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Rejected(message) => f.write_str(message),
            Self::Io(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for SnapshotReceiveError {}
impl From<io::Error> for SnapshotReceiveError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
fn rejected(message: impl std::fmt::Display) -> SnapshotReceiveError {
    SnapshotReceiveError::Rejected(message.to_string())
}

struct ActiveTransfer {
    identity: SnapshotTransferId,
    descriptor: SnapshotDescriptor,
    file: std::fs::File,
    next_offset: u64,
    complete: Option<SnapshotImage>,
}

/// One disk-backed transfer slot. A process restart intentionally discards its
/// progress; the sender restarts at zero. Cancellation truncates this dedicated
/// temporary file and never touches published snapshots or a WAL.
pub struct SnapshotReceiver {
    path: std::path::PathBuf,
    active: Option<ActiveTransfer>,
    poisoned: bool,
}

impl SnapshotReceiver {
    pub fn new(dir: &std::path::Path) -> io::Result<Self> {
        crate::durability::create_dir_all(dir)?;
        let path = dir.join("snapshot-install.tmp");
        // Transfer progress is volatile. Reopening retires a prior interrupted
        // transfer without interpreting it as an active or published image.
        if path.try_exists()? {
            let stale = std::fs::OpenOptions::new().write(true).open(&path)?;
            stale.set_len(0)?;
            stale.sync_all()?;
        }
        Ok(Self {
            path,
            active: None,
            poisoned: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn stage_chunk(
        &mut self,
        identity: SnapshotTransferId,
        descriptor: &SnapshotDescriptor,
        offset: u64,
        bytes: &[u8],
        minimum_index: LogIndex,
        members: &[NodeId],
        installed: Option<&SnapshotImage>,
    ) -> Result<StagedChunk, SnapshotReceiveError> {
        self.stage_inner(
            identity,
            descriptor,
            offset,
            bytes,
            minimum_index,
            members,
            installed,
            false,
        )
    }

    /// Only the single writer calls this after the core validated the exact
    /// descriptor, sender's group/history and transfer identity. A newer leader
    /// need not belong to an older snapshot's configuration.
    #[allow(clippy::too_many_arguments)]
    pub fn stage_authorized_chunk(
        &mut self,
        identity: SnapshotTransferId,
        descriptor: &SnapshotDescriptor,
        offset: u64,
        bytes: &[u8],
        minimum_index: LogIndex,
        members: &[NodeId],
        installed: Option<&SnapshotImage>,
    ) -> Result<StagedChunk, SnapshotReceiveError> {
        self.stage_inner(
            identity,
            descriptor,
            offset,
            bytes,
            minimum_index,
            members,
            installed,
            true,
        )
    }
    #[allow(clippy::too_many_arguments)]
    fn stage_inner(
        &mut self,
        identity: SnapshotTransferId,
        descriptor: &SnapshotDescriptor,
        offset: u64,
        bytes: &[u8],
        minimum_index: LogIndex,
        members: &[NodeId],
        installed: Option<&SnapshotImage>,
        core_authorized: bool,
    ) -> Result<StagedChunk, SnapshotReceiveError> {
        use std::io::{Read, Seek, SeekFrom};
        if self.poisoned {
            return Err(
                io::Error::other("snapshot receiver requires reopen after I/O failure").into(),
            );
        }
        descriptor.validate().map_err(rejected)?;
        if identity.sequence == 0
            || identity.term < descriptor.metadata.last_included_term
            || (!core_authorized && !members.contains(&identity.leader_id))
            || descriptor.metadata.members != members
        {
            return Err(rejected(
                "snapshot transfer authority or membership is invalid",
            ));
        }
        let remaining = descriptor
            .total_len
            .checked_sub(offset)
            .ok_or_else(|| rejected("snapshot chunk offset beyond end"))?;
        let expected = remaining.min(MAX_SNAPSHOT_CHUNK_BYTES as u64) as usize;
        if expected == 0
            || bytes.len() != expected
            || !offset.is_multiple_of(MAX_SNAPSHOT_CHUNK_BYTES as u64)
        {
            return Err(rejected(
                "snapshot chunk is not a canonical bounded segment",
            ));
        }
        if let Some(installed) = installed.filter(|image| image.descriptor() == descriptor) {
            if installed.bytes()[offset as usize..offset as usize + bytes.len()] != *bytes {
                return Err(rejected("installed snapshot duplicate chunk differs"));
            }
            return Ok(StagedChunk {
                next_offset: descriptor.total_len,
                complete: true,
            });
        }
        if descriptor.metadata.last_included_index <= minimum_index {
            return Err(rejected("snapshot transfer is stale"));
        }
        let same = self
            .active
            .as_ref()
            .is_some_and(|active| active.identity == identity);
        if same
            && self
                .active
                .as_ref()
                .is_some_and(|active| active.descriptor != *descriptor)
        {
            return Err(rejected("snapshot descriptor changed within one transfer"));
        }
        if !same {
            if offset != 0 {
                return Err(rejected("new snapshot transfer must begin at offset zero"));
            }
            if let Some(active) = &self.active {
                let newer = identity.term > active.identity.term
                    || (identity.term == active.identity.term
                        && identity.leader_id == active.identity.leader_id
                        && identity.incarnation == active.identity.incarnation
                        && identity.sequence > active.identity.sequence);
                if !newer
                    || descriptor.metadata.last_included_index
                        < active.descriptor.metadata.last_included_index
                {
                    return Err(rejected("snapshot transfer identity is stale"));
                }
            }
            let result = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&self.path);
            let file = match result {
                Ok(file) => file,
                Err(error) => {
                    self.poisoned = true;
                    return Err(error.into());
                }
            };
            self.active = Some(ActiveTransfer {
                identity,
                descriptor: descriptor.clone(),
                file,
                next_offset: 0,
                complete: None,
            });
        }
        let active = self.active.as_mut().expect("active transfer created above");
        if offset > active.next_offset {
            return Err(rejected("snapshot chunk gap"));
        }
        let result: Result<StagedChunk, SnapshotReceiveError> = (|| {
            active.file.seek(SeekFrom::Start(offset))?;
            if offset < active.next_offset {
                let mut previous = vec![0; bytes.len()];
                active.file.read_exact(&mut previous)?;
                if previous != bytes {
                    return Err(rejected("duplicate snapshot chunk differs"));
                }
            } else {
                active.file.write_all(bytes)?;
                active.file.sync_all()?;
                active.next_offset += bytes.len() as u64;
            }
            if active.next_offset == descriptor.total_len && active.complete.is_none() {
                active.file.seek(SeekFrom::Start(0))?;
                let mut complete = Vec::with_capacity(descriptor.total_len as usize);
                (&mut active.file)
                    .take(descriptor.total_len + 1)
                    .read_to_end(&mut complete)?;
                if complete.len() as u64 != descriptor.total_len
                    || Sha256::digest(&complete)[..] != descriptor.sha256
                {
                    return Err(rejected("complete transfer checksum or length mismatch"));
                }
                let image = SnapshotImage::decode(complete).map_err(rejected)?;
                if image.descriptor() != descriptor {
                    return Err(rejected(
                        "transfer descriptor differs from snapshot content",
                    ));
                }
                active.complete = Some(image);
            }
            Ok(StagedChunk {
                next_offset: active.next_offset,
                complete: active.complete.is_some(),
            })
        })();
        if matches!(&result, Err(SnapshotReceiveError::Io(_))) {
            self.poisoned = true;
        }
        result
    }

    pub fn completed(&self, identity: SnapshotTransferId) -> Option<&SnapshotImage> {
        if self.poisoned {
            return None;
        }
        self.active
            .as_ref()
            .filter(|active| active.identity == identity)
            .and_then(|active| active.complete.as_ref())
    }

    /// Shutdown retires the one active transfer before the final storage flush.
    pub fn cancel_active(&mut self) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "snapshot receiver requires reopen after I/O failure",
            ));
        }
        if let Some(identity) = self.active.as_ref().map(|active| active.identity) {
            self.cancel(identity)?;
        }
        Ok(())
    }

    /// A stale cancellation cannot cancel another transfer. Explicit sync errors
    /// propagate; dropping the object merely closes its handle.
    pub fn cancel(&mut self, identity: SnapshotTransferId) -> io::Result<bool> {
        if self.poisoned {
            return Err(io::Error::other(
                "snapshot receiver requires reopen after I/O failure",
            ));
        }
        let Some(active) = self
            .active
            .as_ref()
            .filter(|active| active.identity == identity)
        else {
            return Ok(false);
        };
        let result = active.file.set_len(0).and_then(|()| active.file.sync_all());
        if let Err(error) = result {
            self.poisoned = true;
            return Err(error);
        }
        self.active = None;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft_core::{Command, Entry};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct TestDir(std::path::PathBuf);
    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "micro-raft-snapshot-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            crate::durability::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn image(large: bool) -> SnapshotImage {
        let mut state = StateMachine::default();
        state
            .apply(&Entry {
                index: 1,
                term: 2,
                command: Command::RegisterSession {
                    nonce: "retained".into(),
                },
            })
            .unwrap();
        state
            .apply(&Entry {
                index: 2,
                term: 2,
                command: Command::SessionPut {
                    session_id: 1,
                    key: "k".into(),
                    sequence: 1,
                    value: if large {
                        "x".repeat(60 * 1024)
                    } else {
                        "value".into()
                    },
                },
            })
            .unwrap();
        SnapshotImage::new(
            SnapshotMetadata {
                membership: None,
                last_included_index: 2,
                last_included_term: 2,
                members: vec![1, 2, 3],
            },
            &state,
        )
        .unwrap()
    }
    fn identity(sequence: u64) -> SnapshotTransferId {
        SnapshotTransferId {
            leader_id: 1,
            term: 3,
            incarnation: 42,
            sequence,
        }
    }
    fn stage(
        receiver: &mut SnapshotReceiver,
        id: SnapshotTransferId,
        image: &SnapshotImage,
        offset: usize,
    ) -> Result<StagedChunk, SnapshotReceiveError> {
        receiver.stage_chunk(
            id,
            image.descriptor(),
            offset as u64,
            &image.bytes()[offset..(offset + MAX_SNAPSHOT_CHUNK_BYTES).min(image.bytes().len())],
            0,
            &[1, 2, 3],
            None,
        )
    }

    #[test]
    fn image_roundtrip_preserves_application_and_transfer_descriptor() {
        let original = image(false);
        let decoded = SnapshotImage::decode(original.bytes().to_vec()).unwrap();
        assert_eq!(original.descriptor(), decoded.descriptor());
        assert_eq!(original.application(), decoded.application());
        let mut restored = decoded.into_application();
        let result = restored
            .apply(&Entry {
                index: 3,
                term: 3,
                command: Command::SessionPut {
                    session_id: 1,
                    key: "k".into(),
                    sequence: 1,
                    value: "value".into(),
                },
            })
            .unwrap();
        assert!(
            matches!(result, crate::application::ApplyOutcome::Session(response) if response.index == Some(2))
        );
    }
    #[test]
    fn image_rejects_corrupt_truncated_extended_unknown_format_and_mismatched_boundary() {
        let original = image(false);
        for position in [0, 8, 16, HEADER_BYTES, original.bytes().len() - 1] {
            let mut corrupt = original.bytes().to_vec();
            corrupt[position] ^= 1;
            assert!(SnapshotImage::decode(corrupt).is_err());
        }
        assert!(
            SnapshotImage::decode(original.bytes()[..original.bytes().len() - 1].to_vec()).is_err()
        );
        let mut extended = original.bytes().to_vec();
        extended.push(0);
        assert!(SnapshotImage::decode(extended).is_err());
        let mut metadata = original.descriptor().metadata.clone();
        metadata.last_included_index += 1;
        assert!(SnapshotImage::new(metadata, original.application()).is_err());
        let mut metadata = original.descriptor().metadata.clone();
        metadata.members = vec![1, 1];
        assert!(SnapshotImage::new(metadata, original.application()).is_err());
        let mut limited = LimitedWriter::new(2);
        limited.write_all(b"ok").unwrap();
        assert!(limited.write_all(b"!").is_err());
    }
    #[test]
    fn chunks_reassemble_on_disk_and_identical_duplicates_are_idempotent() {
        let dir = TestDir::new();
        let mut receiver = SnapshotReceiver::new(&dir.0).unwrap();
        let image = image(true);
        assert!(image.bytes().len() > MAX_SNAPSHOT_CHUNK_BYTES);
        let first = stage(&mut receiver, identity(1), &image, 0).unwrap();
        assert!(!first.complete);
        assert_eq!(stage(&mut receiver, identity(1), &image, 0).unwrap(), first);
        for offset in
            (MAX_SNAPSHOT_CHUNK_BYTES..image.bytes().len()).step_by(MAX_SNAPSHOT_CHUNK_BYTES)
        {
            stage(&mut receiver, identity(1), &image, offset).unwrap();
        }
        assert_eq!(
            receiver.completed(identity(1)).unwrap().bytes(),
            image.bytes()
        );
        let final_offset =
            (image.bytes().len() - 1) / MAX_SNAPSHOT_CHUNK_BYTES * MAX_SNAPSHOT_CHUNK_BYTES;
        assert!(
            stage(&mut receiver, identity(1), &image, final_offset)
                .unwrap()
                .complete
        );
        assert_eq!(
            std::fs::metadata(&receiver.path).unwrap().len(),
            image.bytes().len() as u64
        );
    }
    #[test]
    fn changed_duplicates_gaps_stale_identity_and_descriptors_cannot_replace_progress() {
        let dir = TestDir::new();
        let mut receiver = SnapshotReceiver::new(&dir.0).unwrap();
        let image = image(true);
        let first = stage(&mut receiver, identity(2), &image, 0).unwrap();
        let mut changed = image.bytes()[..MAX_SNAPSHOT_CHUNK_BYTES].to_vec();
        changed[20] ^= 1;
        assert!(receiver
            .stage_chunk(
                identity(2),
                image.descriptor(),
                0,
                &changed,
                0,
                &[1, 2, 3],
                None
            )
            .is_err());
        assert!(stage(&mut receiver, identity(1), &image, 0).is_err());
        assert!(stage(
            &mut receiver,
            identity(2),
            &image,
            MAX_SNAPSHOT_CHUNK_BYTES * 2
        )
        .is_err());
        let mut descriptor = image.descriptor().clone();
        descriptor.sha256[0] ^= 1;
        assert!(receiver
            .stage_chunk(
                identity(2),
                &descriptor,
                0,
                &image.bytes()[..MAX_SNAPSHOT_CHUNK_BYTES],
                0,
                &[1, 2, 3],
                None
            )
            .is_err());
        assert_eq!(stage(&mut receiver, identity(2), &image, 0).unwrap(), first);
        assert!(!receiver.cancel(identity(1)).unwrap());
        assert!(receiver.cancel(identity(2)).unwrap());
        assert_eq!(std::fs::metadata(&receiver.path).unwrap().len(), 0);
    }
    #[test]
    fn incomplete_restart_is_inactive_and_requires_offset_zero() {
        let dir = TestDir::new();
        let image = image(true);
        {
            let mut receiver = SnapshotReceiver::new(&dir.0).unwrap();
            stage(&mut receiver, identity(1), &image, 0).unwrap();
        }
        let mut receiver = SnapshotReceiver::new(&dir.0).unwrap();
        assert!(receiver.completed(identity(1)).is_none());
        assert_eq!(
            std::fs::metadata(&receiver.path).unwrap().len(),
            0,
            "reopen reclaims interrupted transfer bytes"
        );
        assert!(stage(&mut receiver, identity(1), &image, MAX_SNAPSHOT_CHUNK_BYTES).is_err());
        stage(&mut receiver, identity(1), &image, 0).unwrap();
        receiver.cancel_active().unwrap();
        assert_eq!(
            std::fs::metadata(&receiver.path).unwrap().len(),
            0,
            "shutdown reclaims staged bytes"
        );
    }
    #[test]
    fn installed_final_duplicate_is_verified_without_republishing() {
        let dir = TestDir::new();
        let mut receiver = SnapshotReceiver::new(&dir.0).unwrap();
        let image = image(false);
        let result = receiver
            .stage_chunk(
                identity(1),
                image.descriptor(),
                0,
                image.bytes(),
                2,
                &[1, 2, 3],
                Some(&image),
            )
            .unwrap();
        assert!(result.complete);
        assert_eq!(result.next_offset, image.bytes().len() as u64);
        assert!(!receiver.path.exists());
        let mut changed = image.bytes().to_vec();
        changed[48] ^= 1;
        assert!(receiver
            .stage_chunk(
                identity(1),
                image.descriptor(),
                0,
                &changed,
                2,
                &[1, 2, 3],
                Some(&image)
            )
            .is_err());
        assert!(receiver
            .stage_chunk(
                identity(1),
                image.descriptor(),
                0,
                image.bytes(),
                2,
                &[1, 2, 3],
                None
            )
            .is_err());
    }
    #[test]
    fn invalid_complete_checksum_never_yields_installable_state() {
        let dir = TestDir::new();
        let mut receiver = SnapshotReceiver::new(&dir.0).unwrap();
        let image = image(false);
        let mut changed = image.bytes().to_vec();
        changed[48] ^= 1;
        assert!(receiver
            .stage_chunk(
                identity(1),
                image.descriptor(),
                0,
                &changed,
                0,
                &[1, 2, 3],
                None
            )
            .is_err());
        assert!(receiver.completed(identity(1)).is_none());
        let mut descriptor = image.descriptor().clone();
        descriptor.total_len = MAX_SNAPSHOT_BYTES as u64 + 1;
        assert!(receiver
            .stage_chunk(
                identity(2),
                &descriptor,
                0,
                image.bytes(),
                0,
                &[1, 2, 3],
                None
            )
            .is_err());
    }
}
