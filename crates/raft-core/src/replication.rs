//! Log replication & commitment. M3 ships only what elections need:
//! AppendEntries as heartbeats (R17 with empty entries, real `prev_log_*`),
//! follower timing acceptance (R6b) and candidate step-down (R11).
//!
//! M4: the consistency check (R12), conflict truncation + append (R13),
//! follower commit update (R14), leader bookkeeping and backoff (R18), the
//! pure `commit_advance` function (R19 — kept pure for the Gate 2 hand-diff),
//! client proposals (R20), and the leader NoOp completing R16.

use crate::message::{Effect, RaftMessage};
use crate::types::{Command, Entry, LogIndex, NodeId, Role, Term};
use crate::RaftNode;

impl RaftNode {
    /// R17: to every peer, the entries from `next_index[p]` onward (empty =
    /// pure heartbeat), anchored at `prev_log_index/term`. Entries piggyback
    /// on the heartbeat cadence rather than being pushed on propose — a
    /// documented simplification costing at most one HEARTBEAT_MS of latency.
    pub(crate) fn send_append_entries(&mut self, effects: &mut Vec<Effect>) {
        let Role::Leader { next_index, .. } = &self.role else {
            return;
        };
        for &peer in &self.peers {
            let next = next_index.get(&peer).map_or(self.last_log_index() + 1, |&n| n);
            let prev_log_index = next - 1;
            let prev_log_term = self.term_at(prev_log_index);
            let from = usize::try_from(prev_log_index).expect("log index fits usize");
            let entries = self.log.get(from..).map_or_else(Vec::new, <[Entry]>::to_vec);
            effects.push(Effect::Send {
                to: peer,
                msg: RaftMessage::AppendEntries {
                    term: self.hard.current_term,
                    leader_id: self.id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: self.commit_index,
                },
            });
        }
    }

    /// R20: a Leader appends the command at the next index in its own term,
    /// persists it, and acknowledges with the assigned index — replication
    /// then rides the next heartbeat (R17). Anyone else refuses, hinting at
    /// the last known leader so the client can retry there.
    pub(crate) fn on_client_propose(&mut self, command: Command, effects: &mut Vec<Effect>) {
        if !matches!(self.role, Role::Leader { .. }) {
            effects.push(Effect::ProposeRejected {
                leader_hint: self.leader_hint,
            });
            return;
        }
        let entry = Entry {
            index: self.last_log_index() + 1,
            term: self.hard.current_term,
            command,
        };
        self.log.push(entry.clone());
        // §2.4 contract: the persist precedes the accept — nothing may act on
        // an entry the leader itself hasn't made durable.
        effects.push(Effect::PersistLogEntries {
            truncate_from: None,
            entries: vec![entry.clone()],
        });
        effects.push(Effect::ProposeAccepted { index: entry.index });
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
        // R20: this sender is the freshest leader sighting we have.
        self.leader_hint = Some(leader_id);
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
