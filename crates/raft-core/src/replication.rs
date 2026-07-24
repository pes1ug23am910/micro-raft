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

    /// The follower's accept path: R12 consistency check, R13 conflict
    /// repair + append, R14 commit update, R4 apply — after the M3 rules
    /// (R3 stale rejection, R11 candidate step-down, R6b timing).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
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
        // timer — including one whose consistency check fails (R12).
        self.reset_election_deadline();
        // R20: this sender is the freshest leader sighting we have.
        self.leader_hint = Some(leader_id);

        // R12: the consistency check — do we hold the leader's anchor entry?
        // Failure still counted the sender as leader above; the reply carries
        // my log length as a backoff hint for R18's next_index walk.
        if prev_log_index > 0
            && (self.last_log_index() < prev_log_index
                || self.term_at(prev_log_index) != prev_log_term)
        {
            effects.push(Effect::Send {
                to: leader_id,
                msg: RaftMessage::AppendEntriesReply {
                    term: self.hard.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                },
            });
            return;
        }

        // R13: walk the payload against the existing log. Truncate ONLY at
        // the first same-index/different-term conflict — the sole situation
        // in which entries are ever deleted, and only here, on a non-leader
        // (R15). Entries already present are skipped, never re-appended, so
        // a duplicate or reordered AppendEntries can never shrink the log.
        let entries_len = entries.len() as u64;
        let mut truncate_from = None;
        let mut appended = Vec::new();
        for entry in entries {
            if entry.index > self.last_log_index() {
                appended.push(entry); // past my end — genuinely new
            } else if self.term_at(entry.index) == entry.term {
                // Same index + same term ⇒ same entry (Log Matching): keep.
            } else {
                // First real conflict: everything from here on is wreckage
                // from a dead leader; cut it and take the leader's suffix.
                truncate_from = Some(entry.index);
                self.log
                    .truncate(usize::try_from(entry.index - 1).expect("log index fits usize"));
                appended.push(entry);
            }
        }
        if !appended.is_empty() {
            self.log.extend(appended.iter().cloned());
            // One persist effect, emitted only because the log changed, and
            // before the success reply below (§2.4 contract).
            effects.push(Effect::PersistLogEntries {
                truncate_from,
                entries: appended,
            });
        }
        effects.push(Effect::Send {
            to: leader_id,
            msg: RaftMessage::AppendEntriesReply {
                term: self.hard.current_term,
                success: true,
                match_index: prev_log_index + entries_len,
            },
        });

        // R14: lift commit_index toward the leader's, bounded by what this
        // RPC covered — never backwards — then apply in order (R4).
        let new_commit = leader_commit.min(prev_log_index + entries_len);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed(effects);
        }
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
