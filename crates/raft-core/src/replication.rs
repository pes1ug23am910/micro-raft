//! Log replication & commitment. M3 ships only what elections need:
//! AppendEntries as heartbeats (R17 with empty entries, real `prev_log_*`),
//! follower timing acceptance (R6b) and candidate step-down (R11).
//!
//! M4: the consistency check (R12), conflict truncation + append (R13),
//! follower commit update (R14), leader bookkeeping and backoff (R18), the
//! pure `commit_advance` function (R19 — kept pure for the Gate 2 hand-diff),
//! client proposals (R20), and the leader NoOp completing R16.

use crate::message::{Effect, RaftMessage};
use crate::types::{Entry, LogIndex, NodeId, Role, Term};
use crate::RaftNode;

impl RaftNode {
    /// R17: to every peer, entries from `next_index[p]` onward with the real
    /// `prev_log_index/term` anchor. M3: `entries` is always empty — a pure
    /// heartbeat is enough to HOLD leadership; payloads ride here in M4.
    pub(crate) fn send_append_entries(&mut self, effects: &mut Vec<Effect>) {
        let Role::Leader { next_index, .. } = &self.role else {
            return;
        };
        for &peer in &self.peers {
            let prev_log_index = next_index.get(&peer).map_or(self.last_log_index(), |&n| n - 1);
            let prev_log_term = self.term_at(prev_log_index);
            effects.push(Effect::Send {
                to: peer,
                msg: RaftMessage::AppendEntries {
                    term: self.hard.current_term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    // M4: log[next_index[peer]..] rides along here.
                    entries: Vec::new(),
                    leader_commit: self.commit_index,
                },
            });
        }
    }

    /// M3 follower half: timing + role rules only (R6b, R11, R3).
    /// M4: R12 consistency check, R13 conflict repair, R14 commit update.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        _prev_log_term: Term,
        _entries: Vec<Entry>,
        _leader_commit: LogIndex,
        effects: &mut Vec<Effect>,
    ) {
        // R3: a stale leader gets the current term and a rejection.
        if term < self.hard.current_term {
            effects.push(Effect::Send {
                to: leader_id,
                msg: RaftMessage::AppendEntriesReply {
                    term: self.hard.current_term,
                    success: false,
                    match_index: self.last_log_index() + 1,
                },
            });
            return;
        }
        // term == current_term here (R2 equalized anything newer): someone
        // legitimately leads this term. R11: a candidate steps down.
        self.become_follower(effects);
        // R6b: ANY AppendEntries from the current leader resets the election
        // timer — including, in M4, one whose consistency check fails.
        self.reset_election_deadline();
        // M4: R12/R13/R14 land here; the persist effect precedes this reply.
        effects.push(Effect::Send {
            to: leader_id,
            msg: RaftMessage::AppendEntriesReply {
                term: self.hard.current_term,
                success: true,
                match_index: prev_log_index,
            },
        });
    }

    /// M3: only the stale-reply guard (R3). M4: R18 bookkeeping + R19 commit.
    pub(crate) fn on_append_entries_reply(
        &mut self,
        _from: NodeId,
        term: Term,
        _success: bool,
        _match_index: LogIndex,
    ) {
        if term != self.hard.current_term {
            // R3: stale replies are dropped — a slow network must not be able
            // to resurrect dead bookkeeping.
        }
        // M4: match_index/next_index updates (R18) and commit advancement (R19).
    }
}
