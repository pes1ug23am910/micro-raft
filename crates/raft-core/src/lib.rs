//! # raft-core — a pure, deterministic Raft state machine (sans-I/O)
//!
//! No networking, no disk access, no clocks, no threads, no async, and no
//! dependencies beyond `serde`. Everything the node
//! ever learns arrives as an [`Input`]; everything it ever wants done comes
//! back as an [`Effect`]. Two interchangeable shells execute those effects:
//! the real driver (`kv-node`: tokio, TCP, disk) and the simulator (`sim`:
//! virtual clock, virtual network, virtual disk).
//!
//! Timing is core-owned: election/heartbeat deadlines are
//! computed from the `now_ms` carried by [`Input::Tick`]. There is no timer
//! effect, so driver and core can never disagree about time — and the sim
//! controls time trivially.

pub mod election;
pub mod membership;
pub mod membership_protocol;
pub mod message;
pub mod read;
pub mod replication;
pub mod rng;
pub mod snapshot;
pub mod types;

pub use election::{decide_vote, RequestVoteRq};
pub use message::{AppendConflictHint, Effect, Input, RaftMessage};
pub use read::MAX_PENDING_READS;
pub use replication::commit_advance;
pub use types::{
    CampaignId, Command, Entry, HardState, LogIndex, NodeId, ReadRejectReason, Role,
    SnapshotDescriptor, SnapshotMetadata, SnapshotStageResult, SnapshotTransferId, Term,
    MAX_SNAPSHOT_BYTES, MAX_SNAPSHOT_CHUNK_BYTES, MIN_SNAPSHOT_BYTES,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use rng::Pcg32;

/// Drivers deliver `Input::Tick` every this-many logical milliseconds.
pub const TICK_MS: u64 = 10;
/// Leader heartbeat cadence. Must stay well below `ELECTION_MIN_MS`.
pub const HEARTBEAT_MS: u64 = 50;
/// Election timeout lower bound.
pub const ELECTION_MIN_MS: u64 = 150;
/// Election timeout upper bound: deadline = now + rng.range_inclusive(150, 300).
pub const ELECTION_MAX_MS: u64 = 300;
/// A leader must hear fresh replies from a majority in each logical window.
pub const CHECK_QUORUM_MS: u64 = ELECTION_MAX_MS;
/// Maximum entries carried by one `AppendEntries` RPC. With 64 KiB values
/// and worst-case JSON escaping, sixteen entries stay below the driver's
/// 8 MiB frame cap while allowing bounded, steady catch-up. Drivers embedding
/// the core must ensure that a single entry is frame-encodable.
pub const MAX_APPEND_ENTRIES: usize = 16;
/// Maximum commands admitted atomically into one log persistence effect.
pub const MAX_PROPOSAL_BATCH: usize = 16;

/// A persisted log that cannot have been produced by a correct Raft node.
///
/// Storage validates framing, CRCs, and contiguous indices while reading.
/// The core repeats the structural checks at its trust boundary so a caller
/// cannot accidentally boot with state that would invalidate 1-based log
/// indexing throughout the algorithm.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RestoreError {
    InvalidSnapshot(String),
    NonContiguousIndex {
        expected: LogIndex,
        found: LogIndex,
    },
    TermRegression {
        index: LogIndex,
        previous: Term,
        found: Term,
    },
    EntryTermExceedsCurrent {
        index: LogIndex,
        entry_term: Term,
        current_term: Term,
    },
}

impl fmt::Display for RestoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RestoreError::InvalidSnapshot(reason) => write!(f, "invalid recovered snapshot: {reason}"),
            RestoreError::NonContiguousIndex { expected, found } => {
                write!(f, "recovered log index {found} is not expected index {expected}")
            }
            RestoreError::TermRegression {
                index,
                previous,
                found,
            } => write!(
                f,
                "recovered log term regresses at index {index}: {previous} -> {found}"
            ),
            RestoreError::EntryTermExceedsCurrent {
                index,
                entry_term,
                current_term,
            } => write!(
                f,
                "recovered entry at index {index} has term {entry_term} above current term {current_term}"
            ),
        }
    }
}

impl std::error::Error for RestoreError {}

/// One Raft node as a pure state machine.
#[derive(Debug)]
pub struct RaftNode {
    pub id: NodeId,
    /// Replication destinations: other voters, learners and retained retired identities.
    pub peers: Vec<NodeId>,
    pub hard: HardState,
    /// Retained suffix after snapshot_index(); durability follows ordered effects.
    pub log: Vec<Entry>,
    /// Rebuilt at the selected snapshot boundary (zero without a snapshot).
    pub commit_index: LogIndex,
    /// Scheduled apply watermark, initially the selected snapshot boundary.
    pub last_applied: LogIndex,
    pub role: Role,
    /// The `leader_id` of the most recent valid-term `AppendEntries` — the
    /// "last known leader" R20 serves as `ProposeRejected`'s hint. Best-effort
    /// by design: cleared on campaigning or quorum-loss stepdown.
    pub(crate) leader_hint: Option<NodeId>,
    // -- timing, all in logical milliseconds fed via Tick (core-owned) --
    pub(crate) now_ms: u64,
    /// When to start/restart an election (R5). 0 = not yet drawn (first Tick).
    pub(crate) election_deadline_ms: u64,
    /// Leader only: when to send the next heartbeats (R17).
    pub(crate) heartbeat_due_ms: u64,
    /// Seeded at construction; the ONLY randomness in the core.
    pub(crate) rng: Pcg32,
    pub(crate) campaign_incarnation: u64,
    pub(crate) campaign_sequence: u64,
    pub(crate) campaign_exhausted: bool,
    pub(crate) last_leader_contact_ms: Option<u64>,
    pub(crate) contact_round: u64,
    pub(crate) quorum_deadline_ms: u64,
    pub(crate) quorum_contacts: BTreeSet<NodeId>,
    pub(crate) read_context: u64,
    #[cfg(test)]
    pub(crate) read_fault: read::ReadFaultMode,
    #[cfg(test)]
    pub(crate) read_fault_hits: u64,
    pub(crate) snapshot: Option<SnapshotDescriptor>,
    pub(crate) snapshot_sequence: u64,
    pub(crate) outgoing_snapshots: BTreeMap<NodeId, snapshot::OutgoingSnapshot>,
    pub(crate) incoming_snapshot: Option<snapshot::IncomingSnapshot>,
    pub(crate) last_snapshot_transfer: Option<(SnapshotTransferId, SnapshotDescriptor)>,
    pub(crate) publishing_snapshot: Option<snapshot::PublishingSnapshot>,
    pub(crate) pending_reads: BTreeMap<u64, read::PendingRead>,
    pub(crate) genesis_membership: membership::CommittedMembership,
    pub(crate) effective_membership: membership::MembershipState,
    pub(crate) recovery_pending: bool,
    #[cfg(test)]
    pub(crate) membership_union_fault: bool,
    #[cfg(test)]
    pub(crate) membership_fault_hits: u64,
    pub(crate) advertisement_last: BTreeMap<NodeId, (Term, LogIndex, u64)>,
    pub(crate) promotion_fence: Option<(String, membership::AdminOperation, LogIndex)>,
    pub(crate) recovery_refresh_hard: bool,
    pub(crate) membership_advertisements: BTreeMap<NodeId, (Term, membership::CommittedMembership)>,
}

impl RaftNode {
    /// A fresh, never-booted node: empty log, term 0, `Follower` (R1).
    /// Recovered `HardState` + log from storage are supplied to [`Self::restore`].
    /// `seed` also identifies this boot's pre-campaigns: supply a fresh random
    /// value on each real boot (the deterministic simulator uses a boot counter).
    /// Legacy bootstrap; panics for duplicate or self peers. Explicit groups and
    /// fresh nonvoting joiners should use `new_for_group` with canonical genesis IDs.
    pub fn new(id: NodeId, peers: Vec<NodeId>, seed: u64) -> Self {
        assert!(!peers.contains(&id), "self must not appear in peers");
        assert_eq!(
            peers.iter().copied().collect::<BTreeSet<_>>().len(),
            peers.len(),
            "duplicate peer"
        );
        let mut members = peers.clone();
        members.push(id);
        members.sort_unstable();
        let genesis_membership =
            membership::CommittedMembership::bootstrap(members).expect("valid genesis");
        RaftNode {
            effective_membership: genesis_membership.state.clone(),
            genesis_membership,
            recovery_pending: false,
            #[cfg(test)]
            membership_union_fault: false,
            #[cfg(test)]
            membership_fault_hits: 0,
            promotion_fence: None,
            advertisement_last: BTreeMap::new(),
            recovery_refresh_hard: false,
            membership_advertisements: BTreeMap::new(),
            id,
            peers,
            hard: HardState::default(),
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            role: Role::Follower,
            leader_hint: None,
            now_ms: 0,
            // The real deadline is drawn from `rng` on the first Tick.
            election_deadline_ms: 0,
            heartbeat_due_ms: 0,
            rng: Pcg32::new(seed),
            campaign_incarnation: seed,
            campaign_sequence: 0,
            campaign_exhausted: false,
            last_leader_contact_ms: None,
            contact_round: 0,
            quorum_deadline_ms: 0,
            quorum_contacts: BTreeSet::new(),
            read_context: 0,
            #[cfg(test)]
            read_fault: read::ReadFaultMode::None,
            #[cfg(test)]
            read_fault_hits: 0,
            snapshot: None,
            snapshot_sequence: 0,
            outgoing_snapshots: BTreeMap::new(),
            incoming_snapshot: None,
            last_snapshot_transfer: None,
            publishing_snapshot: None,
            pending_reads: BTreeMap::new(),
        }
    }

    /// Rebuild a node from exactly Raft's persistent state (R1).
    ///
    /// The node returns as a follower with fresh logical timers and no leader
    /// hint. Legacy fixed groups relearn commitment from the leader. Explicit
    /// groups also recover their proven committed membership prefix and must
    /// execute `Input::Recover` before service when `recovery_required()` is true.
    pub fn restore(
        id: NodeId,
        peers: Vec<NodeId>,
        seed: u64,
        hard: HardState,
        log: Vec<Entry>,
    ) -> Result<Self, RestoreError> {
        Self::restore_with_snapshot(id, peers, seed, hard, None, log)
    }

    /// Restore a selected snapshot and its contiguous retained suffix. The
    /// caller installs the image's application state before serving requests.
    pub fn restore_with_snapshot(
        id: NodeId,
        peers: Vec<NodeId>,
        seed: u64,
        hard: HardState,
        snapshot: Option<SnapshotDescriptor>,
        log: Vec<Entry>,
    ) -> Result<Self, RestoreError> {
        let mut node = Self::new(id, peers, seed);
        if let Some(image) = &snapshot {
            image.validate().map_err(RestoreError::InvalidSnapshot)?;
            if image.metadata.last_included_term > hard.current_term {
                return Err(RestoreError::InvalidSnapshot(
                    "membership or hard term mismatch".into(),
                ));
            }
        }
        node.snapshot = snapshot;
        let mut previous_term = node.snapshot_term();
        let base = node.snapshot_index();
        for (offset, entry) in log.iter().enumerate() {
            let expected = base
                .checked_add(offset as u64)
                .and_then(|v| v.checked_add(1))
                .ok_or_else(|| RestoreError::InvalidSnapshot("log index overflow".into()))?;
            if entry.index != expected {
                return Err(RestoreError::NonContiguousIndex {
                    expected,
                    found: entry.index,
                });
            }
            if entry.term < previous_term {
                return Err(RestoreError::TermRegression {
                    index: entry.index,
                    previous: previous_term,
                    found: entry.term,
                });
            }
            if entry.term > hard.current_term {
                return Err(RestoreError::EntryTermExceedsCurrent {
                    index: entry.index,
                    entry_term: entry.term,
                    current_term: hard.current_term,
                });
            }
            previous_term = entry.term;
        }
        let recovery = membership_protocol::recover_membership(
            &hard,
            node.snapshot.as_ref(),
            &log,
            &node.genesis_membership.state.genesis_voters,
        )
        .map_err(RestoreError::InvalidSnapshot)?;
        node.genesis_membership = membership::CommittedMembership::bootstrap_with_group(
            recovery.committed.state.group_id.clone(),
            recovery.committed.state.genesis_voters.clone(),
        )
        .map_err(RestoreError::InvalidSnapshot)?;
        node.hard = hard;
        if recovery.refresh_hard {
            node.hard.membership = Some(recovery.committed);
        }
        node.effective_membership = recovery.effective;
        node.log = log;
        node.commit_index = recovery.commit_index;
        node.last_applied = base;
        node.recovery_refresh_hard = recovery.refresh_hard;
        node.recovery_pending = recovery.refresh_hard || node.commit_index > base;
        node.update_replication_peers();
        Ok(node)
    }

    /// Validated leader hint from the accepted protocol state, never raw wire data.
    pub fn known_leader(&self) -> Option<NodeId> {
        if self.is_leader() {
            Some(self.id)
        } else {
            self.leader_hint
        }
    }

    /// Restore an independently validated durable application/engine watermark.
    /// Call only before service or ordinary inputs. The embedding shell validates
    /// group identity and the state-store transaction before supplying this proof.
    pub fn restore_applied_watermark(
        &mut self,
        index: LogIndex,
        term: Term,
    ) -> Result<(), RestoreError> {
        if self.now_ms != 0
            || self.election_deadline_ms != 0
            || !matches!(self.role, Role::Follower)
            || index < self.snapshot_index()
            || index < self.last_applied
            || index > self.last_log_index()
            || self.log_term(index) != Some(term)
            || (index > self.commit_index
                && self.membership_at(index).as_ref() != Ok(self.committed_membership()))
        {
            return Err(RestoreError::InvalidSnapshot(
                "invalid durable application watermark or initialization phase".into(),
            ));
        }
        self.commit_index = self.commit_index.max(index);
        self.last_applied = index;
        self.recovery_pending = self.recovery_refresh_hard || self.commit_index > self.last_applied;
        Ok(())
    }

    /// Feed one input; get back an ordered list of effects the driver must
    /// execute — in the exact order emitted: every `Persist*`
    /// precedes the `Send`s that depend on it).
    pub fn step(&mut self, input: Input) -> Vec<Effect> {
        let mut effects = Vec::new();
        if self.recovery_pending && !matches!(input, Input::Recover) {
            return effects;
        }
        match input {
            Input::Recover => self.on_recover(&mut effects),
            Input::MembershipChange {
                request_id,
                operation,
            } => self.on_membership_change(request_id, operation, &mut effects),
            Input::Tick { now_ms } => self.on_tick(now_ms, &mut effects),
            Input::Message { from, msg } => self.on_message(from, msg, &mut effects),
            Input::ClientPropose { command } => self.on_client_propose(command, &mut effects),
            Input::ClientProposeBatch { commands } => {
                self.on_client_propose_batch(commands, &mut effects)
            }
            Input::ReadIndex { request_id } => self.on_read_index(request_id, &mut effects),
            Input::CancelRead { request_id } => self.cancel_read(request_id, &mut effects),
            Input::Compact { descriptor } => self.on_compact(descriptor, &mut effects),
            Input::SnapshotChunkRead {
                to,
                transfer,
                descriptor,
                offset,
                data,
            } => self.on_snapshot_chunk_read(to, transfer, descriptor, offset, data, &mut effects),
            Input::SnapshotChunkStaged {
                transfer,
                descriptor,
                offset,
                result,
            } => self.on_snapshot_chunk_staged(transfer, descriptor, offset, result, &mut effects),
            Input::SnapshotPublished {
                transfer,
                descriptor,
            } => self.on_snapshot_published(transfer, descriptor, &mut effects),
        }
        self.wrap_group_effects(&mut effects);
        effects
    }

    fn on_tick(&mut self, now_ms: u64, effects: &mut Vec<Effect>) {
        // Neither clock regression nor duplicate ticks can renew a window.
        if now_ms < self.now_ms || (now_ms == self.now_ms && self.election_deadline_ms != 0) {
            return;
        }
        self.now_ms = now_ms;
        // There is no representable future deadline at the end of logical time.
        // Fail closed instead of retaining authority behind a saturated timer.
        if now_ms.checked_add(CHECK_QUORUM_MS).is_none() {
            self.campaign_exhausted = true;
            self.become_follower(effects);
            self.leader_hint = None;
            return;
        }
        if self.election_deadline_ms == 0 {
            self.reset_election_deadline();
        }
        match self.role {
            Role::Leader { .. } => {
                if now_ms >= self.quorum_deadline_ms {
                    // A long scheduling pause cannot recycle a historical quorum.
                    let missed_window =
                        now_ms.saturating_sub(self.quorum_deadline_ms) >= CHECK_QUORUM_MS;
                    let next_round = self.contact_round.checked_add(1);
                    if missed_window
                        || !self.voter_config().has_quorum(&self.quorum_contacts)
                        || next_round.is_none()
                    {
                        self.become_follower(effects);
                        self.leader_hint = None;
                        self.reset_election_deadline();
                        return;
                    }
                    self.contact_round = next_round.expect("checked above");
                    self.quorum_contacts = BTreeSet::from([self.id]);
                    self.quorum_deadline_ms = now_ms.saturating_add(CHECK_QUORUM_MS);
                    self.heartbeat_due_ms = now_ms;
                }
                self.maybe_finalize_membership(effects);
                if !self.is_leader() {
                    return;
                }
                if self.heartbeat_due_ms <= now_ms {
                    self.send_append_entries(effects);
                    self.heartbeat_due_ms = now_ms.saturating_add(HEARTBEAT_MS);
                }
            }
            Role::Follower | Role::PreCandidate { .. } | Role::Candidate { .. } => {
                if now_ms >= self.election_deadline_ms
                    && !self.campaign_exhausted
                    && self.is_voter(self.id)
                {
                    self.start_pre_vote(effects);
                }
            }
        }
    }

    fn on_message(&mut self, from: NodeId, msg: RaftMessage, effects: &mut Vec<Effect>) {
        let Some(msg) = self.unwrap_group_message(msg) else {
            return;
        };
        if let RaftMessage::MembershipAdvertisement {
            term,
            configuration,
        } = msg
        {
            self.on_membership_advertisement(from, term, configuration, effects);
            return;
        }
        if from == self.id {
            return;
        }
        let eligible = match &msg {
            RaftMessage::RequestVote {
                term,
                last_log_index,
                last_log_term,
                ..
            } => self.may_vote_for(from, *term, false, *last_log_index, *last_log_term),
            RaftMessage::PreVote {
                prospective_term,
                last_log_index,
                last_log_term,
                ..
            } => self.may_vote_for(
                from,
                *prospective_term,
                true,
                *last_log_index,
                *last_log_term,
            ),
            RaftMessage::AppendEntries { term, .. } => self.may_replicate_from(from, *term),
            RaftMessage::InstallSnapshot { transfer, .. } => {
                self.may_replicate_from(from, transfer.term)
            }
            RaftMessage::AppendEntriesReply { .. }
            | RaftMessage::InstallSnapshotReply { .. }
            | RaftMessage::CompactedPrefix { .. } => self.peers.contains(&from),
            _ => self.is_voter(from),
        };
        if !eligible {
            return;
        }
        match &msg {
            RaftMessage::RequestVote {
                candidate_id,
                term,
                last_log_term,
                ..
            } if *candidate_id != from || last_log_term > term => return,
            RaftMessage::PreVote {
                candidate_id,
                prospective_term,
                last_log_term,
                ..
            } if *candidate_id != from || last_log_term >= prospective_term => return,
            RaftMessage::AppendEntries { leader_id, .. } if *leader_id != from => return,
            RaftMessage::ReadProbe {
                leader_id, context, ..
            } if *leader_id != from || *context == 0 => return,
            RaftMessage::ReadProbeReply { context: 0, .. } => return,
            RaftMessage::InstallSnapshot {
                transfer,
                descriptor,
                offset,
                data,
                done,
                ..
            } if transfer.leader_id != from
                || transfer.sequence == 0
                || descriptor.metadata.last_included_term > transfer.term
                || !self.valid_snapshot_membership(descriptor)
                || !snapshot::valid_chunk(descriptor, *offset, data.len(), *done) =>
            {
                return
            }
            RaftMessage::CompactedPrefix {
                term,
                last_included_index,
                last_included_term,
                ..
            } if *last_included_index == 0
                || *last_included_term == 0
                || last_included_term > term =>
            {
                return
            }
            RaftMessage::InstallSnapshotReply { transfer, .. }
                if transfer.leader_id != self.id || transfer.sequence == 0 =>
            {
                return
            }
            _ => {}
        }
        // Reject malformed AppendEntries before adopting its term, resetting
        // timers, or touching persistent/volatile state. The sender receives
        // the same ordinary failure shape used for a consistency mismatch.
        let append_last_index = match &msg {
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                ..
            } => match replication::validate_append_entries(
                *term,
                *prev_log_index,
                *prev_log_term,
                entries,
            ) {
                Some(last_index) => Some(last_index),
                None => {
                    effects.push(Effect::Send {
                        to: *leader_id,
                        msg: RaftMessage::AppendEntriesReply {
                            conflict: None,
                            term: self.hard.current_term,
                            success: false,
                            match_index: self.last_log_index(),
                            contact_round: 0,
                        },
                    });
                    return;
                }
            },
            _ => None,
        };
        let msg_term = match &msg {
            RaftMessage::RequestVote { term, .. }
            | RaftMessage::RequestVoteReply { term, .. }
            | RaftMessage::AppendEntries { term, .. }
            | RaftMessage::AppendEntriesReply { term, .. }
            | RaftMessage::PreVoteReply { term, .. }
            | RaftMessage::ReadProbe { term, .. }
            | RaftMessage::ReadProbeReply { term, .. } => *term,
            RaftMessage::InstallSnapshot { transfer, .. } => transfer.term,
            RaftMessage::InstallSnapshotReply { term, .. }
            | RaftMessage::CompactedPrefix { term, .. } => *term,
            RaftMessage::PreVote { .. } => self.hard.current_term,
            RaftMessage::GroupMessage { .. } | RaftMessage::MembershipAdvertisement { .. } => {
                unreachable!("handled envelope")
            }
        };
        // R2: any structurally valid message (request OR reply) with a newer
        // term is adopted before its semantic content is processed.
        if msg_term > self.hard.current_term {
            self.hard.current_term = msg_term;
            self.hard.voted_for = None;
            // Term durability precedes externally visible stepdown.
            effects.push(Effect::PersistHardState(self.hard.clone()));
            self.become_follower(effects);
            self.last_leader_contact_ms = None;
            self.cancel_incoming_snapshot(effects);
        }
        match msg {
            RaftMessage::GroupMessage { .. } | RaftMessage::MembershipAdvertisement { .. } => {
                unreachable!("handled envelope")
            }
            RaftMessage::CompactedPrefix {
                term,
                last_included_index,
                last_included_term,
                contact_round,
            } => {
                if self.log_term(last_included_index) == Some(last_included_term) {
                    self.on_append_entries_reply(
                        from,
                        term,
                        true,
                        last_included_index,
                        contact_round,
                        None,
                        effects,
                    );
                }
            }
            RaftMessage::InstallSnapshot {
                transfer,
                descriptor,
                offset,
                data,
                done,
                contact_round,
            } => self.on_install_snapshot(
                transfer,
                descriptor,
                offset,
                data,
                done,
                contact_round,
                effects,
            ),
            RaftMessage::InstallSnapshotReply {
                term,
                transfer,
                next_offset,
                match_index,
                accepted,
                installed,
                contact_round,
            } => self.on_install_snapshot_reply(
                from,
                term,
                transfer,
                next_offset,
                match_index,
                accepted,
                installed,
                contact_round,
                effects,
            ),
            RaftMessage::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.on_request_vote(term, candidate_id, last_log_index, last_log_term, effects),
            RaftMessage::RequestVoteReply { term, vote_granted } => {
                self.on_request_vote_reply(from, term, vote_granted, effects);
            }
            RaftMessage::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                contact_round,
            } => {
                let Some(payload_last_index) = append_last_index else {
                    return;
                };
                self.on_append_entries(
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    payload_last_index,
                    leader_commit,
                    contact_round,
                    effects,
                );
            }
            RaftMessage::AppendEntriesReply {
                term,
                success,
                match_index,
                contact_round,
                conflict,
            } => self.on_append_entries_reply(
                from,
                term,
                success,
                match_index,
                contact_round,
                conflict,
                effects,
            ),
            RaftMessage::ReadProbe {
                term,
                leader_id,
                context,
            } => {
                self.on_read_probe(term, leader_id, context, effects);
            }
            RaftMessage::ReadProbeReply {
                term,
                context,
                accepted,
            } => {
                self.on_read_probe_reply(from, term, context, accepted, effects);
            }
            RaftMessage::PreVote {
                prospective_term,
                campaign_id,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                self.on_pre_vote(
                    prospective_term,
                    campaign_id,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                    effects,
                );
            }
            RaftMessage::PreVoteReply {
                term,
                prospective_term,
                campaign_id,
                vote_granted,
            } => {
                self.on_pre_vote_reply(
                    from,
                    term,
                    prospective_term,
                    campaign_id,
                    vote_granted,
                    effects,
                );
            }
        }
    }

    /// R7: on becoming Follower for any reason, candidate/leader-only state
    /// is dropped with the role. Emits `RoleChanged` only on a real change.
    pub(crate) fn become_follower(&mut self, effects: &mut Vec<Effect>) {
        self.outgoing_snapshots.clear();
        self.promotion_fence = None;
        self.advertisement_last.clear();
        self.contact_round = 0;
        self.quorum_contacts.clear();
        if !matches!(self.role, Role::Follower) {
            self.role = Role::Follower;
            effects.push(Effect::RoleChanged {
                role_name: "Follower",
                term: self.hard.current_term,
            });
        }
        self.reject_pending_reads(ReadRejectReason::LeadershipLost, effects);
    }

    /// Reset on first boot, campaigning, quorum-loss stepdown, granting a
    /// real vote, or valid leader contact. PreVote requests never reset it.
    pub(crate) fn reset_election_deadline(&mut self) {
        self.election_deadline_ms = self
            .now_ms
            .saturating_add(self.rng.range_inclusive(ELECTION_MIN_MS, ELECTION_MAX_MS));
    }

    pub fn last_log_index(&self) -> LogIndex {
        self.log.last().map_or(self.snapshot_index(), |e| e.index)
    }

    pub(crate) fn last_log_term(&self) -> Term {
        self.log.last().map_or(self.snapshot_term(), |e| e.term)
    }

    /// R4: whenever `commit_index > last_applied`, emit `Apply` for the next
    /// unapplied entry and advance — strictly in index order, exactly once
    /// per boot per index. Runs after every commit_index change (R14, R19).
    pub(crate) fn apply_committed(&mut self, effects: &mut Vec<Effect>) {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            let entry = self
                .entry_at(self.last_applied)
                .expect("R19/R14 never advance commit_index past the log")
                .clone();
            effects.push(Effect::Apply(entry));
        }
    }

    /// A compacted-away index has no available term; index zero is available
    /// only before the first snapshot. The boundary retains its exact term.
    pub fn log_term(&self, index: LogIndex) -> Option<Term> {
        if index == self.snapshot_index() {
            return Some(self.snapshot_term());
        }
        self.entry_at(index).map(|entry| entry.term)
    }

    pub fn entry_at(&self, index: LogIndex) -> Option<&Entry> {
        let offset = index.checked_sub(self.snapshot_index())?.checked_sub(1)?;
        self.log.get(usize::try_from(offset).ok()?)
    }

    pub(crate) fn term_at(&self, index: LogIndex) -> Term {
        self.log_term(index).unwrap_or(0)
    }
    pub fn snapshot_descriptor(&self) -> Option<&SnapshotDescriptor> {
        self.snapshot.as_ref()
    }
    pub fn snapshot_index(&self) -> LogIndex {
        self.snapshot
            .as_ref()
            .map_or(0, |s| s.metadata.last_included_index)
    }
    pub fn snapshot_term(&self) -> Term {
        self.snapshot
            .as_ref()
            .map_or(0, |s| s.metadata.last_included_term)
    }
    pub fn members(&self) -> Vec<NodeId> {
        self.effective_membership
            .participants()
            .into_iter()
            .collect()
    }

    /// Read-only introspection for tests/logging (the field stays core-owned).
    pub fn election_deadline(&self) -> u64 {
        self.election_deadline_ms
    }

    /// Current term-scoped quorum challenge, only while leading.
    pub fn contact_round(&self) -> Option<u64> {
        self.is_leader().then_some(self.contact_round)
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }
}

#[cfg(test)]
mod restore_tests {
    use super::*;

    fn entry(index: LogIndex, term: Term) -> Entry {
        Entry {
            index,
            term,
            command: Command::NoOp,
        }
    }

    #[test]
    fn restore_keeps_only_persistent_state() {
        let hard = HardState {
            membership: None,
            current_term: 7,
            voted_for: Some(2),
        };
        let log = vec![entry(1, 2), entry(2, 7)];
        let node = RaftNode::restore(1, vec![2, 3], 99, hard.clone(), log.clone())
            .expect("valid recovered state");

        assert_eq!(node.hard, hard);
        assert_eq!(node.log, log);
        assert_eq!(node.commit_index, 0);
        assert_eq!(node.last_applied, 0);
        assert_eq!(node.role, Role::Follower);
        assert_eq!(node.leader_hint, None);
        assert_eq!(node.now_ms, 0);
        assert_eq!(node.election_deadline_ms, 0);
        assert_eq!(node.heartbeat_due_ms, 0);
    }

    #[test]
    fn restore_rejects_structurally_impossible_logs() {
        let hard = HardState {
            membership: None,
            current_term: 4,
            voted_for: None,
        };
        assert!(matches!(
            RaftNode::restore(1, vec![2, 3], 1, hard.clone(), vec![entry(2, 1)]),
            Err(RestoreError::NonContiguousIndex {
                expected: 1,
                found: 2
            })
        ));
        assert!(matches!(
            RaftNode::restore(
                1,
                vec![2, 3],
                1,
                hard.clone(),
                vec![entry(1, 3), entry(2, 2)]
            ),
            Err(RestoreError::TermRegression { index: 2, .. })
        ));
        assert!(matches!(
            RaftNode::restore(1, vec![2, 3], 1, hard, vec![entry(1, 5)]),
            Err(RestoreError::EntryTermExceedsCurrent { index: 1, .. })
        ));
    }
}

#[cfg(test)]
mod election_extensions;

#[cfg(test)]
mod snapshot_tests;

#[cfg(test)]
mod batch_tests;

#[cfg(test)]
mod membership_protocol_tests;

#[cfg(test)]
mod applied_watermark_tests;

#[cfg(test)]
mod backtracking_tests;
