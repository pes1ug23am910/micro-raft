//! Core data types shared by the state machine and its drivers.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

/// Node identifier: 1, 2, 3, …
pub type NodeId = u8;
/// Logical epoch. Starts at 0; the first election produces term 1.
pub type Term = u64;
/// 1-based log position; 0 means "no entries" / "before the log".
pub type LogIndex = u64;

/// A state-machine command carried by a log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Put {
        key: String,
        value: String,
    },
    Delete {
        key: String,
    },
    /// Appended by a leader on election win (R16) so it can promptly commit
    /// something from its own term — the practical answer to Figure 8 (R19b).
    NoOp,
    /// Consensus-owned configuration entry; ordinary client proposals reject it.
    Configuration(crate::membership::ConfigurationEntry),
    RegisterSession {
        nonce: String,
    },
    CloseSession {
        session_id: LogIndex,
    },
    SessionPut {
        session_id: LogIndex,
        key: String,
        sequence: u64,
        value: String,
    },
    SessionDelete {
        session_id: LogIndex,
        key: String,
        sequence: u64,
    },
}

/// One replicated log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    pub index: LogIndex,
    pub term: Term,
    pub command: Command,
}

/// The state that MUST survive a crash (Figure 2, "Persistent state"; R1).
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct HardState {
    pub current_term: Term,
    pub voted_for: Option<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub membership: Option<crate::membership::CommittedMembership>,
}

/// One pre-election attempt. Incarnation must be fresh on every boot; sequence
/// is checked and never reused within that boot. This is correlation, not auth.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CampaignId {
    pub incarnation: u64,
    pub sequence: u64,
}

/// The node's role, carrying exactly the volatile per-role state Figure 2
/// assigns. `BTreeSet`/`BTreeMap`, never `HashMap`: iteration order must not
/// be able to influence deterministic behavior.
#[derive(Clone, Debug, PartialEq)]
pub enum Role {
    Follower,
    PreCandidate {
        prospective_term: Term,
        campaign_id: CampaignId,
        votes_received: BTreeSet<NodeId>,
    },
    Candidate {
        votes_received: BTreeSet<NodeId>,
    },
    Leader {
        next_index: BTreeMap<NodeId, LogIndex>,
        match_index: BTreeMap<NodeId, LogIndex>,
    },
}

/// Terminal read-barrier admission or cancellation outcome. No read rejection
/// implies anything about an unacknowledged write's final outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadRejectReason {
    NotLeader,
    NotReady,
    Capacity,
    ContextExhausted,
    Cancelled,
    LeadershipLost,
}

impl ReadRejectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotLeader => "not_leader",
            Self::NotReady => "read_not_ready",
            Self::Capacity => "read_capacity",
            Self::ContextExhausted => "read_context_exhausted",
            Self::Cancelled => "read_cancelled",
            Self::LeadershipLost => "leadership_lost",
        }
    }
}

/// Maximum complete opaque snapshot image and one canonical transfer chunk.
pub const MAX_SNAPSHOT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;
pub const MIN_SNAPSHOT_BYTES: usize = 48;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotMetadata {
    pub last_included_index: LogIndex,
    pub last_included_term: Term,
    pub members: Vec<NodeId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub membership: Option<crate::membership::CommittedMembership>,
}

impl SnapshotMetadata {
    pub fn validate(&self) -> Result<(), String> {
        if let Some(membership) = &self.membership {
            membership.validate()?;
            if membership.index > self.last_included_index
                || membership.term > self.last_included_term
                || membership
                    .state
                    .participants()
                    .into_iter()
                    .collect::<Vec<_>>()
                    != self.members
            {
                return Err("snapshot membership boundary or participant set mismatch".into());
            }
        }
        if self.last_included_index == 0 || self.last_included_term == 0 {
            return Err("snapshot boundary index and term must be positive".into());
        }
        if self.last_included_index == LogIndex::MAX {
            return Err("snapshot boundary must leave room for a following entry".into());
        }
        if self.members.is_empty()
            || self.members.len() > 256
            || self.members.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err("snapshot voters must be nonempty, sorted, and unique".into());
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotDescriptor {
    pub metadata: SnapshotMetadata,
    pub total_len: u64,
    pub sha256: [u8; 32],
}

impl SnapshotDescriptor {
    pub fn validate(&self) -> Result<(), String> {
        self.metadata.validate()?;
        if !(MIN_SNAPSHOT_BYTES as u64..=MAX_SNAPSHOT_BYTES as u64).contains(&self.total_len) {
            return Err("snapshot image size must be between 48 bytes and 64 MiB".into());
        }
        Ok(())
    }
}

/// A transfer attempt, separate from immutable snapshot content identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SnapshotTransferId {
    pub leader_id: NodeId,
    pub term: Term,
    pub incarnation: u64,
    pub sequence: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotStageResult {
    Accepted { next_offset: u64, complete: bool },
    Rejected,
}
