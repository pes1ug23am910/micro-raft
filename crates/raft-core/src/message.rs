//! Messages, inputs, and effects for the pure state-machine boundary.
//!
//! This is the core's entire interface with the world: everything the node
//! ever learns arrives as an [`Input`]; everything it ever wants done comes
//! back as an [`Effect`]. Drivers execute effects strictly in emitted order,
//! and every `Persist*` effect completes durably (fsync) before any subsequent
//! `Send` in the same batch is transmitted.

use serde::{Deserialize, Serialize};

use crate::types::{Command, Entry, HardState, LogIndex, NodeId, Term};

/// The Raft wire protocol.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum RaftMessage {
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
    },
    AppendEntriesReply {
        term: Term,
        success: bool,
        /// On success: index of the last entry the follower now matches.
        /// On failure: the follower's last log index. The leader retries from
        /// the following index, while still backing off at least one position.
        match_index: LogIndex,
    },
}

/// Everything the node ever learns.
#[derive(Clone, Debug, PartialEq)]
pub enum Input {
    /// A message arrived from a peer.
    Message { from: NodeId, msg: RaftMessage },
    /// Logical time advanced. The driver sends this every `TICK_MS`. Deadlines
    /// are computed from `now_ms` inside the core: drivers never manage
    /// timers on the core's behalf, they only deliver ticks.
    Tick { now_ms: u64 },
    /// A client asked to execute a command (leader-only; see R20).
    ClientPropose { command: Command },
}

/// Everything the node ever wants done.
#[derive(Clone, Debug, PartialEq)]
pub enum Effect {
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
    Send { to: NodeId, msg: RaftMessage },
    /// Apply this committed entry to the state machine, in index order (R4).
    Apply(Entry),
    /// The node's role changed (Follower/Candidate/Leader) — for logging/metrics
    /// and for a driver to fail pending client requests on step-down.
    RoleChanged { role_name: &'static str, term: Term },
    /// Leader accepted a ClientPropose and assigned it this log index; kv-node
    /// uses it to register the pending client response.
    ProposeAccepted { index: LogIndex },
    /// Not-leader proposals yield this instead, with the last known leader if any.
    ProposeRejected { leader_hint: Option<NodeId> },
}
