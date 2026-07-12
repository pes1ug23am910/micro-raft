//! Leader election (M3): election start (R8), the vote decision (R9), reply
//! counting (R10), and the transition to leadership (R16's election half).
//!
//! `decide_vote` stays pure so its vote rule can be tested independently.

use crate::message::{Effect, RaftMessage};
use crate::types::{HardState, LogIndex, NodeId, Role, Term};
use crate::{RaftNode, HEARTBEAT_MS};

use std::collections::{BTreeMap, BTreeSet};

/// The fields of `RaftMessage::RequestVote`, as a plain struct so the pure
/// vote decision has a stable, diffable signature.
#[derive(Clone, Debug, PartialEq)]
pub struct RequestVoteRq {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

/// R9 — grant iff BOTH hold: (a) `voted_for` is free for this candidate
/// (None, or already exactly this candidate — one vote per term, ever);
/// (b) the candidate's log is *at least as up-to-date*: LAST TERMS compared
/// first, lengths only as the tie-break. Comparing by index alone elects
/// data-losing leaders (Gate 1 probe 2).
///
/// Assumes R2/R3 already ran, so `req.term == hard.current_term`.
pub fn decide_vote(hard: &HardState, my_last: (LogIndex, Term), req: &RequestVoteRq) -> bool {
    let vote_free = match hard.voted_for {
        None => true,
        Some(already) => already == req.candidate_id,
    };
    if !vote_free {
        return false;
    }
    let (my_last_index, my_last_term) = my_last;
    req.last_log_term > my_last_term
        || (req.last_log_term == my_last_term && req.last_log_index >= my_last_index)
}

impl RaftNode {
    /// R8: increment term, vote for self, become Candidate, reset the
    /// deadline (R6c), persist BEFORE soliciting, then solicit every peer.
    pub(crate) fn start_election(&mut self, effects: &mut Vec<Effect>) {
        self.hard.current_term += 1;
        self.hard.voted_for = Some(self.id);
        let mut votes_received = BTreeSet::new();
        votes_received.insert(self.id);
        self.role = Role::Candidate { votes_received };
        self.reset_election_deadline(); // R6c
        // §2.4 contract: the persist precedes the RequestVote sends — a
        // candidate that solicits before its term/vote are durable can
        // double-vote after a crash (Gate 3 material).
        effects.push(Effect::PersistHardState(self.hard.clone()));
        effects.push(Effect::RoleChanged {
            role_name: "Candidate",
            term: self.hard.current_term,
        });
        let (last_log_index, last_log_term) = (self.last_log_index(), self.last_log_term());
        for &peer in &self.peers {
            effects.push(Effect::Send {
                to: peer,
                msg: RaftMessage::RequestVote {
                    term: self.hard.current_term,
                    candidate_id: self.id,
                    last_log_index,
                    last_log_term,
                },
            });
        }
    }

    /// R9 handling (R2/R3 already ran in `on_message`).
    pub(crate) fn on_request_vote(
        &mut self,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
        effects: &mut Vec<Effect>,
    ) {
        // R3: stale request → reject with the current term, touch nothing.
        if term < self.hard.current_term {
            effects.push(Effect::Send {
                to: candidate_id,
                msg: RaftMessage::RequestVoteReply {
                    term: self.hard.current_term,
                    vote_granted: false,
                },
            });
            return;
        }
        let req = RequestVoteRq {
            term,
            candidate_id,
            last_log_index,
            last_log_term,
        };
        let grant = decide_vote(&self.hard, (self.last_log_index(), self.last_log_term()), &req);
        if grant {
            self.hard.voted_for = Some(candidate_id);
            self.reset_election_deadline(); // R6a: granting resets the timer…
            effects.push(Effect::PersistHardState(self.hard.clone()));
        }
        // …and R6 forbids resetting it on a refusal: resetting there lets a
        // disruptive candidate suppress everyone else's elections forever.
        effects.push(Effect::Send {
            to: candidate_id,
            msg: RaftMessage::RequestVoteReply {
                term: self.hard.current_term,
                vote_granted: grant,
            },
        });
    }

    /// R10: count grants for the current term in a set (duplicates cannot
    /// double-count); strict cluster majority → Leader.
    pub(crate) fn on_request_vote_reply(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
        effects: &mut Vec<Effect>,
    ) {
        // R3: stale replies — an older term, or arriving in a role that no
        // longer expects them — are ignored entirely.
        if term != self.hard.current_term || !vote_granted {
            return;
        }
        let majority = self.majority();
        let won = {
            let Role::Candidate { votes_received } = &mut self.role else {
                return;
            };
            votes_received.insert(from);
            votes_received.len() >= majority
        };
        if won {
            self.become_leader(effects);
        }
    }

    /// R16 (election half — the NoOp/log half lands in M4): initialize
    /// per-peer replication state, then heartbeat immediately so the claim
    /// on the term goes out on this very step.
    pub(crate) fn become_leader(&mut self, effects: &mut Vec<Effect>) {
        let next = self.last_log_index() + 1;
        let mut next_index = BTreeMap::new();
        let mut match_index = BTreeMap::new();
        for &peer in &self.peers {
            next_index.insert(peer, next);
            match_index.insert(peer, 0);
        }
        self.role = Role::Leader {
            next_index,
            match_index,
        };
        effects.push(Effect::RoleChanged {
            role_name: "Leader",
            term: self.hard.current_term,
        });
        // M4: R16's NoOp entry + PersistLogEntries are appended here.
        self.send_append_entries(effects);
        self.heartbeat_due_ms = self.now_ms + HEARTBEAT_MS;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(candidate_id: NodeId, last_log_index: LogIndex, last_log_term: Term) -> RequestVoteRq {
        RequestVoteRq {
            term: 5,
            candidate_id,
            last_log_index,
            last_log_term,
        }
    }

    fn hard(voted_for: Option<NodeId>) -> HardState {
        HardState {
            current_term: 5,
            voted_for,
        }
    }

    /// R9b's load-bearing detail: a LONGER log with an OLDER last term is
    /// LESS up-to-date (the Gate 1 worked example). Index-only comparison
    /// would elect a data-losing leader.
    #[test]
    fn decide_vote_term_outranks_length() {
        // Mine: 5 entries ending in term 3. Candidate: 9 entries ending in term 2.
        assert!(!decide_vote(&hard(None), (5, 3), &req(2, 9, 2)));
        // Candidate: 1 entry ending in term 4 — shorter but newer wins.
        assert!(decide_vote(&hard(None), (5, 3), &req(2, 1, 4)));
    }

    /// R9a: one vote per term — a second candidate is refused, but the same
    /// candidate is re-granted (idempotent for duplicate/retried requests).
    #[test]
    fn decide_vote_one_vote_per_term() {
        assert!(!decide_vote(&hard(Some(3)), (0, 0), &req(2, 10, 5)));
        assert!(decide_vote(&hard(Some(2)), (0, 0), &req(2, 10, 5)));
    }

    /// R9b tie-break: equal last terms compare by length; equal-or-longer
    /// is granted, shorter is refused.
    #[test]
    fn decide_vote_equal_term_compares_length() {
        assert!(!decide_vote(&hard(None), (5, 3), &req(2, 4, 3)));
        assert!(decide_vote(&hard(None), (5, 3), &req(2, 5, 3)));
        assert!(decide_vote(&hard(None), (5, 3), &req(2, 6, 3)));
    }
}
