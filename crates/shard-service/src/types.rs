pub use raft_core::membership::{AdminOperation, AdminRecord};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub type GroupId = u16;
pub type ShardId = u16;
pub const CONTROLLER: GroupId = 0;
pub const MAX_SHARDS: usize = 64;
pub const MAX_GROUPS: usize = 16;
pub const MAX_TRANSFERS: usize = 1024;
pub const MAX_ACTOR_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_TRANSFER_BYTES: usize = 4 * 1024 * 1024;
pub const CHUNK_BYTES: usize = 32 * 1024;
pub const MAX_KEY_BYTES: usize = 1024;
pub const MAX_VALUE_BYTES: usize = 64 * 1024;
pub const MAX_SESSIONS: usize = 1024;
pub const MAX_RETRY_KEYS: usize = 8192;
pub const MAX_RETAINED_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Origin {
    pub group: GroupId,
    pub index: u64,
}
impl Origin {
    pub fn key(self) -> String {
        format!("{}:{}", self.group, self.index)
    }
    pub fn valid(self) -> bool {
        self.index > 0
    }
}
pub type SessionId = Origin;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransferId {
    pub shard: ShardId,
    pub epoch: u64,
    pub controller_index: u64,
}
impl TransferId {
    pub fn key(&self) -> String {
        format!("{}:{}:{}", self.shard, self.epoch, self.controller_index)
    }
    pub fn valid(&self) -> bool {
        usize::from(self.shard) < MAX_SHARDS && self.epoch > 1 && self.controller_index > 0
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Move {
    pub id: TransferId,
    pub source: GroupId,
    pub destination: GroupId,
    pub previous_epoch: u64,
}
impl Move {
    pub fn valid(&self) -> bool {
        self.id.valid()
            && self.source != CONTROLLER
            && self.destination != CONTROLLER
            && self.source != self.destination
            && self.previous_epoch.checked_add(1) == Some(self.id.epoch)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retention {
    pub sessions: usize,
    pub retry_keys: usize,
    pub payload_bytes: usize,
}
impl Retention {
    pub fn valid(&self) -> bool {
        self.sessions <= MAX_SESSIONS
            && self.retry_keys <= MAX_RETRY_KEYS
            && self.payload_bytes <= MAX_RETAINED_PAYLOAD_BYTES
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageProof {
    pub movement: Move,
    pub fence: Origin,
    pub bytes: u64,
    pub sha256: [u8; 32],
    pub retention: Retention,
}
impl ImageProof {
    pub fn valid(&self) -> bool {
        self.movement.valid()
            && self.retention.valid()
            && self.fence.valid()
            && self.fence.group == self.movement.source
            && (1..=MAX_TRANSFER_BYTES as u64).contains(&self.bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstalledProof {
    pub image: ImageProof,
    pub installed: Origin,
}
impl InstalledProof {
    pub fn valid(&self) -> bool {
        self.image.valid()
            && self.installed.valid()
            && self.installed.group == self.image.movement.destination
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OwnershipProof {
    pub installation: InstalledProof,
    pub controller: Origin,
}
impl OwnershipProof {
    pub fn valid(&self) -> bool {
        self.installation.valid()
            && self.controller.valid()
            && self.controller.group == CONTROLLER
            && self.controller.index > self.installation.image.movement.id.controller_index
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActivationProof {
    pub ownership: OwnershipProof,
    pub activated: Origin,
}
impl ActivationProof {
    pub fn valid(&self) -> bool {
        self.ownership.valid()
            && self.activated.valid()
            && self.activated.group == self.ownership.installation.image.movement.destination
            && self.activated.index > self.ownership.installation.installed.index
    }
}

/// A controller-committed decision made before ownership changes. Endpoints
/// must independently obtain this exact proof through a checked controller read.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbortProof {
    pub movement: Move,
    pub controller: Origin,
}
impl AbortProof {
    pub fn valid(&self) -> bool {
        self.movement.valid()
            && self.controller.group == CONTROLLER
            && self.controller.index > self.movement.id.controller_index
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbortRecord {
    pub abort: AbortProof,
    pub recovered: Origin,
}
/// All transfer history is retained until database reset. Reserve a bounded
/// serialized tombstone for every possible controller transfer, so even a full
/// destination can record an abort without requiring a capacity-releasing write.
pub const ABORT_RECORD_RESERVE: usize = 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub origin: Origin,
    pub shard: ShardId,
    pub epoch: u64,
    pub session: Option<SessionId>,
    pub sequence: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Error {
    InvalidInput,
    WrongGroup,
    NotInitialized,
    AlreadyInitialized,
    StaleRoute,
    Unavailable,
    Busy,
    StaleGeneration,
    WrongTransfer,
    WrongPhase,
    Aborted,
    PayloadMismatch,
    ChunkOffset,
    Capacity,
    UnknownSession,
    SessionClosed,
    StaleSequence,
    SequenceGap,
}

pub fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

pub fn key_shard(key: &str, shard_count: u16) -> Option<ShardId> {
    if key.is_empty()
        || key.len() > MAX_KEY_BYTES
        || shard_count == 0
        || usize::from(shard_count) > MAX_SHARDS
    {
        return None;
    }
    let hash = sha256(key.as_bytes());
    Some(
        (u64::from_be_bytes(hash[..8].try_into().expect("fixed digest")) % u64::from(shard_count))
            as u16,
    )
}

pub fn canonical_bytes<T: Serialize>(value: &T, max: usize) -> Result<Vec<u8>, Error> {
    struct Bounded {
        bytes: Vec<u8>,
        max: usize,
        exceeded: bool,
    }
    impl std::io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            if bytes.len() > self.max.saturating_sub(self.bytes.len()) {
                self.exceeded = true;
                return Err(std::io::Error::other("encoded state exceeds limit"));
            }
            self.bytes.extend(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded {
        bytes: Vec::new(),
        max,
        exceeded: false,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| {
        if writer.exceeded {
            Error::Capacity
        } else {
            Error::InvalidInput
        }
    })?;
    Ok(writer.bytes)
}

pub const MAX_MAINTENANCE_TICKETS: usize = 1024;
pub const MAX_MAINTENANCE_OPERATION_BYTES: usize = 4096;
pub const MAINTENANCE_COMPLETION_RESERVE: usize = 8192;

pub fn maintenance_request_id_valid(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id.is_ascii()
        && !id
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
}
pub fn maintenance_operation_valid(operation: &AdminOperation) -> bool {
    let structure = match operation {
        AdminOperation::AddLearner { endpoints, .. } => endpoints.validate().is_ok(),
        AdminOperation::Remove { .. } => true,
        AdminOperation::SetVoters { voters } => raft_core::membership::VoterConfig::Stable {
            voters: voters.clone(),
        }
        .validate()
        .is_ok(),
    };
    structure && canonical_bytes(operation, MAX_MAINTENANCE_OPERATION_BYTES).is_ok()
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceCompletion {
    pub record: AdminRecord,
    pub observed_applied: Origin,
    pub released: Origin,
}

/// One globally serialized membership intent. Absence, local rejection and
/// timeouts never complete it; only the exact applied core outcome can do so.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MaintenanceTicket {
    pub request_id: String,
    pub target: GroupId,
    pub operation: AdminOperation,
    pub origin: Origin,
    pub previous_generation: Option<Origin>,
    pub completion: Option<MaintenanceCompletion>,
}
impl MaintenanceTicket {
    pub fn core_request_id(&self) -> String {
        format!("l7-maintenance-{}", self.origin.index)
    }
    pub fn record_matches(&self, record: &AdminRecord, observed: Origin) -> bool {
        let (Some(final_index), Some(final_term)) = (record.final_index, record.final_term) else {
            return false;
        };
        record.operation == self.operation
            && record.first_index > 0
            && record.first_term > 0
            && final_index >= record.first_index
            && final_term >= record.first_term
            && if record.joint {
                final_index > record.first_index
                    && !matches!(record.operation, AdminOperation::AddLearner { .. })
            } else {
                final_index == record.first_index
                    && final_term == record.first_term
                    && !matches!(record.operation, AdminOperation::SetVoters { .. })
            }
            && observed.group == self.target
            && observed.index >= final_index
            && (self.target != CONTROLLER || record.first_index > self.origin.index)
    }
    pub fn valid(&self) -> bool {
        maintenance_request_id_valid(&self.request_id)
            && maintenance_operation_valid(&self.operation)
            && self.origin.group == CONTROLLER
            && self.origin.index > 0
            && self.previous_generation.is_none_or(|previous| {
                previous.group == CONTROLLER
                    && previous.index > 0
                    && previous.index < self.origin.index
            })
            && self.completion.as_ref().is_none_or(|completion| {
                self.record_matches(&completion.record, completion.observed_applied)
                    && completion.released.group == CONTROLLER
                    && completion.released.index > self.origin.index
                    && (self.target != CONTROLLER
                        || completion.observed_applied.index < completion.released.index)
                    && canonical_bytes(completion, MAINTENANCE_COMPLETION_RESERVE).is_ok()
            })
    }
}
