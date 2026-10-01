//! Leader election: election start (R8), the vote decision (R9), reply
//! counting (R10), and the transition to leadership (R16's election half).
//!
//! `decide_vote` is a pure function so its safety-critical rule can be tested
//! independently of node state transitions.

use crate::message::{Effect, RaftMessage};
use crate::types::{CampaignId, Command, Entry, HardState, LogIndex, NodeId, Role, Term};
use crate::{RaftNode, CHECK_QUORUM_MS, HEARTBEAT_MS};

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
/// data-losing leaders.
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
    /// Probe reachability and log eligibility without changing durable state.
    pub(crate) fn start_pre_vote(&mut self, effects: &mut Vec<Effect>) {
        if !self.is_voter(self.id) {
            return;
        }
        self.cancel_incoming_snapshot(effects);
        let (Some(prospective_term), Some(sequence)) = (
            self.hard.current_term.checked_add(1),
            self.campaign_sequence.checked_add(1),
        ) else {
            // Counter exhaustion is passive, never wrap or reuse correlation.
            self.campaign_exhausted = true;
            self.become_follower(effects);
            return;
        };
        self.campaign_sequence = sequence;
        self.leader_hint = None;
        let campaign_id = CampaignId {
            incarnation: self.campaign_incarnation,
            sequence,
        };
        self.role = Role::PreCandidate {
            prospective_term,
            campaign_id,
            votes_received: BTreeSet::from([self.id]),
        };
        self.reset_election_deadline();
        if self.self_quorum() {
            self.start_election(effects);
            return;
        }
        effects.push(Effect::RoleChanged {
            role_name: "PreCandidate",
            term: self.hard.current_term,
        });
        for &peer in &self.peers {
            if !self.is_voter(peer) {
                continue;
            }
            self.advertise_membership(peer, effects);
            effects.push(Effect::Send {
                to: peer,
                msg: RaftMessage::PreVote {
                    prospective_term,
                    campaign_id,
                    candidate_id: self.id,
                    last_log_index: self.last_log_index(),
                    last_log_term: self.last_log_term(),
                },
            });
        }
    }

    pub(crate) fn on_pre_vote(
        &mut self,
        prospective_term: Term,
        campaign_id: CampaignId,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
        effects: &mut Vec<Effect>,
    ) {
        let recent_leader = self
            .last_leader_contact_ms
            .is_some_and(|at| self.now_ms.saturating_sub(at) < CHECK_QUORUM_MS);
        let fresh_log =
            (last_log_term, last_log_index) >= (self.last_log_term(), self.last_log_index());
        let vote_granted = self.may_vote_for(
            candidate_id,
            prospective_term,
            true,
            last_log_index,
            last_log_term,
        ) && prospective_term > self.hard.current_term
            && fresh_log
            && !self.is_leader()
            && !recent_leader;
        effects.push(Effect::Send {
            to: candidate_id,
            msg: RaftMessage::PreVoteReply {
                term: self.hard.current_term,
                prospective_term,
                campaign_id,
                vote_granted,
            },
        });
    }

    pub(crate) fn on_pre_vote_reply(
        &mut self,
        from: NodeId,
        term: Term,
        prospective_term: Term,
        campaign_id: CampaignId,
        vote_granted: bool,
        effects: &mut Vec<Effect>,
    ) {
        if !vote_granted || term >= prospective_term || !self.is_voter(from) {
            return;
        }
        let quorum = self.voter_config().clone();
        let Role::PreCandidate {
            prospective_term: expected_term,
            campaign_id: expected_id,
            votes_received,
        } = &mut self.role
        else {
            return;
        };
        if prospective_term != *expected_term || campaign_id != *expected_id {
            return;
        }
        votes_received.insert(from);
        if quorum.has_quorum(votes_received) {
            self.start_election(effects);
        }
    }

    /// R8: increment term, vote for self, become Candidate, reset the
    /// deadline (R6c), persist BEFORE soliciting, then solicit every peer.
    pub(crate) fn start_election(&mut self, effects: &mut Vec<Effect>) {
        if !self.is_voter(self.id) {
            return;
        }
        let Some(term) = self.hard.current_term.checked_add(1) else {
            self.campaign_exhausted = true;
            self.become_follower(effects);
            return;
        };
        self.hard.current_term = term;
        self.hard.voted_for = Some(self.id);
        let mut votes_received = BTreeSet::new();
        votes_received.insert(self.id);
        self.role = Role::Candidate { votes_received };
        self.reset_election_deadline();
        // Persist before soliciting votes. Otherwise a crash could erase the
        // candidate's self-vote and allow it to vote twice in one term.
        effects.push(Effect::PersistHardState(self.hard.clone()));
        if self.self_quorum() {
            self.become_leader(effects);
            return;
        }
        effects.push(Effect::RoleChanged {
            role_name: "Candidate",
            term: self.hard.current_term,
        });
        let (last_log_index, last_log_term) = (self.last_log_index(), self.last_log_term());
        for &peer in &self.peers {
            if !self.is_voter(peer) {
                continue;
            }
            self.advertise_membership(peer, effects);
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
        let grant = self.may_vote_for(candidate_id, term, false, last_log_index, last_log_term)
            && decide_vote(
                &self.hard,
                (self.last_log_index(), self.last_log_term()),
                &req,
            );
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
        if term != self.hard.current_term || !vote_granted || !self.is_voter(from) {
            return;
        }
        let quorum = self.voter_config().clone();
        let won = {
            let Role::Candidate { votes_received } = &mut self.role else {
                return;
            };
            votes_received.insert(from);
            quorum.has_quorum(votes_received)
        };
        if won {
            self.become_leader(effects);
        }
    }

    /// R16: initialize per-peer replication state anchored to the PRE-NoOp
    /// last index, immediately append + persist the current-term NoOp, then
    /// heartbeat on this very step — so the NoOp rides the first
    /// AppendEntries out. The NoOp is what lets R19(b) commit something from
    /// this term promptly, unlocking everything before it (Figure 8).
    pub(crate) fn become_leader(&mut self, effects: &mut Vec<Effect>) {
        let Some(next) = self
            .last_log_index()
            .checked_add(1)
            .filter(|next| *next < LogIndex::MAX)
        else {
            self.campaign_exhausted = true;
            self.become_follower(effects);
            self.leader_hint = None;
            return;
        };
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
        self.read_context = 0;
        debug_assert!(
            self.pending_reads.is_empty(),
            "old leader reads must be cancelled"
        );
        self.contact_round = 1;
        self.quorum_deadline_ms = self.now_ms.saturating_add(CHECK_QUORUM_MS);
        self.quorum_contacts = BTreeSet::from([self.id]);
        let noop = Entry {
            index: next,
            term: self.hard.current_term,
            command: Command::NoOp,
        };
        self.log.push(noop.clone());
        // The append's persist precedes the Sends that carry it.
        effects.push(Effect::PersistLogEntries {
            truncate_from: None,
            entries: vec![noop],
        });
        let mut committed = Vec::new();
        if self.self_quorum() {
            self.advance_commit(self.last_log_index(), &mut committed);
        }
        for effect in &committed {
            if matches!(effect, Effect::PersistHardState(_)) {
                effects.push(effect.clone());
            }
        }
        effects.push(Effect::RoleChanged {
            role_name: "Leader",
            term: self.hard.current_term,
        });
        self.send_append_entries(effects);
        self.heartbeat_due_ms = self.now_ms.saturating_add(HEARTBEAT_MS);
        effects.extend(
            committed
                .into_iter()
                .filter(|effect| !matches!(effect, Effect::PersistHardState(_))),
        );
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
            membership: None,
            current_term: 5,
            voted_for,
        }
    }

    /// R9b's load-bearing detail: a LONGER log with an OLDER last term is
    /// LESS up-to-date. Index-only comparison
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

    #[test]
    fn unknown_and_duplicate_vote_replies_do_not_form_a_majority() {
        let mut node = RaftNode::new(1, vec![2, 3, 4, 5], 7);
        let mut effects = Vec::new();
        node.start_election(&mut effects);
        let term = node.hard.current_term;

        node.on_request_vote_reply(99, term, true, &mut effects);
        assert!(matches!(node.role, Role::Candidate { .. }));

        node.on_request_vote_reply(2, term, true, &mut effects);
        node.on_request_vote_reply(2, term, true, &mut effects);
        assert!(matches!(node.role, Role::Candidate { .. }));

        node.on_request_vote_reply(3, term, true, &mut effects);
        assert!(
            node.is_leader(),
            "two distinct peers plus the self-vote is a majority"
        );
    }
}
