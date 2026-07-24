//! Raft safety invariants asserted after every simulator step.
//!
//! M3 shipped **ElectionSafety** (at most one node ever reaches Leader in a
//! given term) and **TermMonotonicity** (a node's current_term never
//! decreases). M4 adds **LogMatching**, **StateMachineSafety**, and the
//! **CommittedRegistry** with LeaderCompleteness/CommittedDurability v1;
//! M5–M6 extend the registry checks across crash schedules.
//!
//! Every panic carries the run's seed: `seed=N` replays the exact failure.

use std::collections::BTreeMap;

use raft_core::{Command, Entry, LogIndex, NodeId, Term};

#[derive(Debug, Default)]
pub struct Invariants {
    leaders_by_term: BTreeMap<Term, NodeId>,
}

impl Invariants {
    /// ElectionSafety: called the moment any node reaches Leader. Panics on a
    /// conflicting insert — two leaders in one term is the split-brain Raft's
    /// vote rules exist to prevent.
    pub fn on_leader_elected(&mut self, seed: u64, term: Term, id: NodeId) {
        if let Some(&existing) = self.leaders_by_term.get(&term) {
            assert!(
                existing == id,
                "seed={seed}: ElectionSafety violated — term {term} has two leaders: n{existing} and n{id}"
            );
        } else {
            self.leaders_by_term.insert(term, id);
        }
    }

    /// TermMonotonicity: checked after every step of the node that stepped.
    pub fn check_term_monotonic(seed: u64, id: NodeId, prev: Term, current: Term) {
        assert!(
            current >= prev,
            "seed={seed}: TermMonotonicity violated on n{id}: term went {prev} -> {current}"
        );
    }

    /// Every (term → leader) pair ever observed — the `leaders_by_term()`
    /// accessor of §4/M3.
    pub fn leaders_by_term(&self) -> &BTreeMap<Term, NodeId> {
        &self.leaders_by_term
    }
}

/// LogMatching (§4/M4): for every pair of nodes, wherever both logs hold an
/// entry with the same index and term, the logs must be identical up through
/// that index. Per the inductive property it suffices to find the LAST
/// common index with equal terms and assert prefix equality there.
pub fn check_log_matching(seed: u64, a_id: NodeId, a: &[Entry], b_id: NodeId, b: &[Entry]) {
    let mut i = a.len().min(b.len());
    while i > 0 && a[i - 1].term != b[i - 1].term {
        i -= 1;
    }
    if i > 0 {
        assert!(
            a[..i] == b[..i],
            "seed={seed}: LogMatching violated — n{a_id} and n{b_id} agree on term at \
             index {i} but their prefixes differ"
        );
    }
}

/// StateMachineSafety (§4/M4): no two nodes' applied histories may disagree
/// at any index. Histories only grow, so comparing overlapping prefixes
/// after every application keeps the check incremental and complete.
pub fn check_applied_agreement(
    seed: u64,
    a_id: NodeId,
    a: &[(LogIndex, Command)],
    b_id: NodeId,
    b: &[(LogIndex, Command)],
) {
    let overlap = a.len().min(b.len());
    assert!(
        a[..overlap] == b[..overlap],
        "seed={seed}: StateMachineSafety violated — n{a_id} and n{b_id} applied \
         different commands inside their common prefix (first {overlap} applications)"
    );
}

/// The committed-entry registry (§4/M4): an entry is recorded the moment its
/// index becomes `<= commit_index` on a leader committing in its own term
/// (R19 — the only place commitment is ever DEFINED; followers merely learn
/// of it). LeaderCompleteness/CommittedDurability v1 then demands every
/// registered entry exist in the log of every subsequent leader.
#[derive(Debug, Default)]
pub struct CommittedRegistry {
    entries: BTreeMap<LogIndex, Entry>,
}

impl CommittedRegistry {
    /// Record a committed entry. Two different entries committed at one
    /// index is the exact disaster consensus exists to prevent — panic.
    pub fn record(&mut self, seed: u64, entry: &Entry) {
        if let Some(existing) = self.entries.get(&entry.index) {
            assert!(
                existing == entry,
                "seed={seed}: CommittedRegistry conflict — index {} was committed as \
                 {existing:?} but a leader now commits {entry:?}",
                entry.index
            );
        } else {
            self.entries.insert(entry.index, entry.clone());
        }
    }

    /// LeaderCompleteness/CommittedDurability v1: asserted the moment any
    /// node becomes Leader — every entry ever committed must already be in
    /// its log, byte-identical.
    pub fn assert_all_in_leader_log(&self, seed: u64, leader: NodeId, term: Term, log: &[Entry]) {
        for (&index, entry) in &self.entries {
            let held = log
                .get(usize::try_from(index - 1).expect("log index fits usize"))
                .is_some_and(|e| e == entry);
            assert!(
                held,
                "seed={seed}: LeaderCompleteness violated — n{leader} elected for term \
                 {term} without committed entry {entry:?} (index {index})"
            );
        }
    }

    pub fn get(&self, index: LogIndex) -> Option<&Entry> {
        self.entries.get(&index)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}
