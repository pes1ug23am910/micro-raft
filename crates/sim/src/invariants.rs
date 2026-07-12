//! Raft safety invariants asserted after every simulator step.
//!
//! M3 ships the first two: **ElectionSafety** (at most one node ever reaches
//! Leader in a given term) and **TermMonotonicity** (a node's current_term
//! never decreases). M4 adds LogMatching, StateMachineSafety and the
//! CommittedRegistry; M5–M6 complete CommittedDurability.
//!
//! Every panic carries the run's seed: `seed=N` replays the exact failure.

use std::collections::BTreeMap;

use raft_core::{NodeId, Term};

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
