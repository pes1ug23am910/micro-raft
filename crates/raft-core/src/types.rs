//! Stable core data types shared by the state machine, drivers, and simulator.

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
    Put { key: String, value: String },
    Delete { key: String },
    /// Appended by a leader on election win (R16) so it can promptly commit
    /// something from its own term — the practical answer to Figure 8 (R19b).
    NoOp,
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
}

/// The node's role, carrying exactly the volatile per-role state Figure 2
/// assigns. `BTreeSet` and `BTreeMap` keep iteration order deterministic.
#[derive(Clone, Debug, PartialEq)]
pub enum Role {
    Follower,
    Candidate {
        votes_received: BTreeSet<NodeId>,
    },
    Leader {
        next_index: BTreeMap<NodeId, LogIndex>,
        match_index: BTreeMap<NodeId, LogIndex>,
    },
}
