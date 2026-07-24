//! # raft-core — a pure, deterministic Raft state machine (sans-I/O)
//!
//! No networking, no disk access, no clocks, no threads, no async, and no
//! dependencies beyond `serde`. Everything the node
//! ever learns arrives as an [`Input`]; everything it ever wants done comes
//! back as an [`Effect`]. Two interchangeable shells execute those effects:
//! the real driver (`kv-node`: tokio, TCP, disk) and the simulator (`sim`:
//! virtual clock, virtual network, virtual disk).
//!
//! Timing is core-owned (decision D-000): election/heartbeat deadlines are
//! computed from the `now_ms` carried by [`Input::Tick`]. There is no timer
//! effect, so driver and core can never disagree about time — and the sim
//! controls time trivially.

pub mod election;
pub mod message;
pub mod replication;
pub mod rng;
pub mod types;

pub use election::{decide_vote, RequestVoteRq};
pub use message::{Effect, Input, RaftMessage};
pub use types::{Command, Entry, HardState, LogIndex, NodeId, Role, Term};

use rng::Pcg32;

/// Drivers deliver `Input::Tick` every this-many logical milliseconds.
pub const TICK_MS: u64 = 10;
/// Leader heartbeat cadence. Must stay ≪ `ELECTION_MIN_MS` (Gate 1 probe 5).
pub const HEARTBEAT_MS: u64 = 50;
/// Election timeout lower bound.
pub const ELECTION_MIN_MS: u64 = 150;
/// Election timeout upper bound: deadline = now + rng.range_inclusive(150, 300).
pub const ELECTION_MAX_MS: u64 = 300;

/// One Raft node as a pure state machine; drivers execute its returned effects.
#[derive(Debug)]
pub struct RaftNode {
    pub id: NodeId,
    /// The OTHER nodes (len 2 in a 3-node cluster).
    pub peers: Vec<NodeId>,
    pub hard: HardState,
    /// In-core copy; durability happens via `Persist*` effects.
    pub log: Vec<Entry>,
    /// Volatile (R1: rebuilt as 0 on every boot).
    pub commit_index: LogIndex,
    /// Volatile (R1: rebuilt as 0 on every boot).
    pub last_applied: LogIndex,
    pub role: Role,
    /// The `leader_id` of the most recent valid-term `AppendEntries` — the
    /// "last known leader" R20 serves as `ProposeRejected`'s hint. Best-effort
    /// by design: never cleared, only overwritten by a fresher sighting.
    pub(crate) leader_hint: Option<NodeId>,
    // -- timing, all in logical milliseconds fed via Tick (core-owned; D-000) --
    pub(crate) now_ms: u64,
    /// When to start/restart an election (R5). 0 = not yet drawn (first Tick).
    pub(crate) election_deadline_ms: u64,
    /// Leader only: when to send the next heartbeats (R17).
    pub(crate) heartbeat_due_ms: u64,
    /// Seeded at construction; the ONLY randomness in the core.
    pub(crate) rng: Pcg32,
}

impl RaftNode {
    /// A fresh, never-booted node: empty log, term 0, `Follower` (R1).
    /// M5: recovered `HardState` + log from storage become parameters here.
    pub fn new(id: NodeId, peers: Vec<NodeId>, seed: u64) -> Self {
        RaftNode {
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
        }
    }

    /// Feed one input; get back an ordered list of effects the driver must
    /// execute — in the exact order emitted (§2.4 contract: every `Persist*`
    /// precedes the `Send`s that depend on it).
    pub fn step(&mut self, input: Input) -> Vec<Effect> {
        let mut effects = Vec::new();
        match input {
            Input::Tick { now_ms } => self.on_tick(now_ms, &mut effects),
            Input::Message { from, msg } => self.on_message(from, msg, &mut effects),
            Input::ClientPropose { command } => self.on_client_propose(command, &mut effects),
        }
        effects
    }

    fn on_tick(&mut self, now_ms: u64, effects: &mut Vec<Effect>) {
        self.now_ms = now_ms;
        // First tick after boot: draw the initial randomized deadline (D-000).
        if self.election_deadline_ms == 0 {
            self.reset_election_deadline();
        }
        match self.role {
            // R5: a Leader ignores the election deadline.
            Role::Leader { .. } => {
                if self.heartbeat_due_ms <= now_ms {
                    self.send_append_entries(effects); // R17
                    self.heartbeat_due_ms = now_ms + HEARTBEAT_MS;
                }
            }
            // R5: a Follower or Candidate whose deadline passes campaigns.
            Role::Follower | Role::Candidate { .. } => {
                if now_ms >= self.election_deadline_ms {
                    self.start_election(effects); // R8
                }
            }
        }
    }

    fn on_message(&mut self, from: NodeId, msg: RaftMessage, effects: &mut Vec<Effect>) {
        let msg_term = match &msg {
            RaftMessage::RequestVote { term, .. }
            | RaftMessage::RequestVoteReply { term, .. }
            | RaftMessage::AppendEntries { term, .. }
            | RaftMessage::AppendEntriesReply { term, .. } => *term,
        };
        // R2: any message (request OR reply) with a newer term — adopt it,
        // clear the vote, convert to Follower, persist — all BEFORE the
        // message content is processed.
        if msg_term > self.hard.current_term {
            self.hard.current_term = msg_term;
            self.hard.voted_for = None;
            self.become_follower(effects);
            effects.push(Effect::PersistHardState(self.hard.clone()));
        }
        match msg {
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
            } => self.on_append_entries(
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                effects,
            ),
            RaftMessage::AppendEntriesReply {
                term,
                success,
                match_index,
            } => self.on_append_entries_reply(from, term, success, match_index),
        }
    }

    /// R7: on becoming Follower for any reason, candidate/leader-only state
    /// is dropped with the role. Emits `RoleChanged` only on a real change.
    pub(crate) fn become_follower(&mut self, effects: &mut Vec<Effect>) {
        if !matches!(self.role, Role::Follower) {
            self.role = Role::Follower;
            effects.push(Effect::RoleChanged {
                role_name: "Follower",
                term: self.hard.current_term,
            });
        }
    }

    /// R6: the deadline is reset in EXACTLY three situations — (a) granting a
    /// vote, (b) accepting AppendEntries from the current leader, (c) starting
    /// an election. Callers are those three sites (plus first-boot draw).
    pub(crate) fn reset_election_deadline(&mut self) {
        self.election_deadline_ms =
            self.now_ms + self.rng.range_inclusive(ELECTION_MIN_MS, ELECTION_MAX_MS);
    }

    pub(crate) fn last_log_index(&self) -> LogIndex {
        self.log.last().map_or(0, |e| e.index)
    }

    pub(crate) fn last_log_term(&self) -> Term {
        self.log.last().map_or(0, |e| e.term)
    }

    /// R4: whenever `commit_index > last_applied`, emit `Apply` for the next
    /// unapplied entry and advance — strictly in index order, exactly once
    /// per boot per index. Runs after every commit_index change (R14, R19).
    pub(crate) fn apply_committed(&mut self, effects: &mut Vec<Effect>) {
        while self.commit_index > self.last_applied {
            self.last_applied += 1;
            let entry = self
                .log
                .get(usize::try_from(self.last_applied - 1).expect("log index fits usize"))
                .expect("R19/R14 never advance commit_index past the log")
                .clone();
            effects.push(Effect::Apply(entry));
        }
    }

    /// Explicit convention (§4/M4): `term_at(0) == 0` — "before the log".
    pub(crate) fn term_at(&self, index: LogIndex) -> Term {
        if index == 0 {
            return 0;
        }
        self.log.get(usize::try_from(index - 1).expect("log index fits usize"))
            .map_or(0, |e| e.term)
    }

    /// Strict majority of the whole cluster (2 of 3, 3 of 5).
    pub(crate) fn majority(&self) -> usize {
        let cluster_size = self.peers.len() + 1;
        cluster_size / 2 + 1
    }

    /// Read-only introspection for tests/logging (the field stays core-owned).
    pub fn election_deadline(&self) -> u64 {
        self.election_deadline_ms
    }

    pub fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }
}
