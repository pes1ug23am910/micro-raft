//! Messages, inputs, and effects for the pure state-machine boundary.
//!
//! This is the core's entire interface with the world: everything the node
//! ever learns arrives as an [`Input`]; everything it ever wants done comes
//! back as an [`Effect`]. Drivers execute effects strictly in emitted order,
//! and every `Persist*` effect completes durably (fsync) before any subsequent
//! `Send` in the same batch is transmitted.

use serde::{Deserialize, Serialize};

use crate::types::{
    CampaignId, Command, Entry, HardState, LogIndex, NodeId, ReadRejectReason, SnapshotDescriptor,
    SnapshotStageResult, SnapshotTransferId, Term,
};

/// The Raft wire protocol.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RaftMessage {
    /// Explicit groups require this one-level envelope on every wire message.
    GroupMessage {
        group_id: String,
        genesis_voters: Vec<NodeId>,
        message: Box<RaftMessage>,
    },
    /// Durable-log configuration hint; not a certificate of commitment.
    MembershipAdvertisement {
        term: Term,
        configuration: crate::membership::CommittedMembership,
    },
    RequestVote {
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    RequestVoteReply {
        term: Term,
        vote_granted: bool,
    },
    AppendEntries {
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        /// Empty = heartbeat.
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        /// Term-scoped freshness challenge. Zero carries no quorum evidence.
        #[serde(default)]
        contact_round: u64,
    },
    AppendEntriesReply {
        term: Term,
        success: bool,
        /// On success: index of the last entry the follower now matches.
        /// On failure: the follower's last log index. The leader retries from
        /// the following index, while still backing off at least one position.
        match_index: LogIndex,
        #[serde(default)]
        contact_round: u64,
    },
    PreVote {
        prospective_term: Term,
        campaign_id: CampaignId,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    PreVoteReply {
        /// Responder's actual term; never the requested prospective term.
        term: Term,
        prospective_term: Term,
        campaign_id: CampaignId,
        vote_granted: bool,
    },
    /// A fresh, per-request authority challenge. It carries no log mutation.
    ReadProbe {
        term: Term,
        leader_id: NodeId,
        context: u64,
    },
    ReadProbeReply {
        term: Term,
        context: u64,
        accepted: bool,
    },
    /// A follower has compacted past an AppendEntries anchor. The leader must
    /// verify this retained boundary term before using the progress hint.
    CompactedPrefix {
        term: Term,
        last_included_index: LogIndex,
        last_included_term: Term,
        contact_round: u64,
    },
    InstallSnapshot {
        transfer: SnapshotTransferId,
        descriptor: SnapshotDescriptor,
        offset: u64,
        data: Vec<u8>,
        done: bool,
        contact_round: u64,
    },
    InstallSnapshotReply {
        term: Term,
        transfer: SnapshotTransferId,
        next_offset: u64,
        match_index: LogIndex,
        accepted: bool,
        installed: bool,
        contact_round: u64,
    },
}

/// Everything the node ever learns.
#[derive(Clone, Debug, PartialEq)]
pub enum Input {
    /// Execute the recovery durability/apply barrier before accepting ordinary inputs.
    Recover,
    MembershipChange {
        request_id: String,
        operation: crate::membership::AdminOperation,
    },
    /// A message arrived from a peer.
    Message {
        from: NodeId,
        msg: RaftMessage,
    },
    /// Logical time advanced. The driver sends this every `TICK_MS`. Deadlines
    /// are computed from `now_ms` inside the core: drivers never manage
    /// timers on the core's behalf, they only deliver ticks.
    Tick {
        now_ms: u64,
    },
    /// A client asked to execute a command (leader-only; see R20).
    ClientPropose {
        command: Command,
    },
    /// Ordered bounded admission, with one shared durability effect and one
    /// acceptance/rejection for every input command. Empty batches do nothing.
    ClientProposeBatch {
        commands: Vec<Command>,
    },
    /// Caller IDs identify local waiters; the core creates its own wire context.
    /// An active duplicate ID is ignored; drivers must not reuse completed IDs.
    ReadIndex {
        request_id: u64,
    },
    CancelRead {
        request_id: u64,
    },
    /// The shell prepared a durable image at an already applied boundary.
    Compact {
        descriptor: SnapshotDescriptor,
    },
    SnapshotChunkRead {
        to: NodeId,
        transfer: SnapshotTransferId,
        descriptor: SnapshotDescriptor,
        offset: u64,
        data: Vec<u8>,
    },
    SnapshotChunkStaged {
        transfer: SnapshotTransferId,
        descriptor: SnapshotDescriptor,
        offset: u64,
        result: SnapshotStageResult,
    },
    /// Must follow publication in the same serialized driver effect batch.
    SnapshotPublished {
        transfer: Option<SnapshotTransferId>,
        descriptor: SnapshotDescriptor,
    },
}

/// Everything the node ever wants done.
#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
    /// A group/history-validated new sender needs a reply route. The shell must
    /// still validate/resolve endpoints; this does not grant voter membership.
    MembershipRouteHint {
        id: NodeId,
        term: Term,
        endpoints: crate::membership::MemberEndpoints,
    },
    MembershipResult {
        request_id: String,
        outcome: crate::membership_protocol::MembershipOutcome,
    },
    MembershipChanged {
        committed: crate::membership::CommittedMembership,
    },
    /// Durably persist hard state BEFORE executing any later effect in this batch.
    PersistHardState(HardState),
    /// Durably append these entries BEFORE executing any later effect in this
    /// batch. `truncate_from`: if Some(i), first truncate the durable log from
    /// index i (inclusive) — conflict repair per R13 — then append.
    PersistLogEntries {
        truncate_from: Option<LogIndex>,
        entries: Vec<Entry>,
    },
    /// Hand this message to the transport (fire-and-forget; loss is tolerated).
    Send {
        to: NodeId,
        msg: RaftMessage,
    },
    /// Apply this committed entry to the state machine, in index order (R4).
    Apply(Entry),
    /// The node's role changed (Follower/PreCandidate/Candidate/Leader) — for logging/metrics
    /// and for a driver to fail pending client requests on step-down.
    RoleChanged {
        role_name: &'static str,
        term: Term,
    },
    /// Leader accepted a ClientPropose and assigned it this log index; kv-node
    /// uses it to register the pending client response.
    ProposeAccepted {
        index: LogIndex,
    },
    /// Not-leader proposals yield this instead, with the last known leader if any.
    ProposeRejected {
        leader_hint: Option<NodeId>,
    },
    /// Fresh quorum authority established. The driver must wait for its applied
    /// state to reach index and recheck this leader term before publishing data.
    ReadReady {
        request_id: u64,
        context: u64,
        index: LogIndex,
        term: Term,
    },
    ReadRejected {
        request_id: u64,
        leader_hint: Option<NodeId>,
        reason: ReadRejectReason,
    },
    ReadSnapshotChunk {
        to: NodeId,
        transfer: SnapshotTransferId,
        descriptor: SnapshotDescriptor,
        offset: u64,
        max_len: usize,
    },
    StageSnapshotChunk {
        transfer: SnapshotTransferId,
        descriptor: SnapshotDescriptor,
        offset: u64,
        data: Vec<u8>,
        done: bool,
    },
    /// Atomic snapshot/WAL publication; the shell independently verifies suffix.
    PublishSnapshot {
        transfer: Option<SnapshotTransferId>,
        descriptor: SnapshotDescriptor,
        retained_entries: Vec<Entry>,
    },
    /// Install application state before executing any subsequent final ACK.
    ApplySnapshot {
        descriptor: SnapshotDescriptor,
    },
    CancelSnapshot {
        transfer: SnapshotTransferId,
    },
    SnapshotRejected {
        reason: &'static str,
    },
}
