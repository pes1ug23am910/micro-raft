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

/// R19 as a pure function (the Gate 2 hand-diff target, like `decide_vote`
/// was for Gate 1).
///
/// `match_indexes` carries one replication watermark per cluster member —
/// the leader's own log length included. Returns the largest `N > current`
/// such that (a) a strict majority of the cluster has `match >= N`, AND
/// (b) `log[N].term == current_term`. Condition (b) is Figure 8's lesson and
/// is not optional: majority replication of a *prior-term* entry alone must
/// never advance the commit index — older entries commit only as a side
/// effect of a current-term entry committing above them (R19b).
pub fn commit_advance(
    current: LogIndex,
    current_term: Term,
    log: &[Entry],
    match_indexes: &[LogIndex],
) -> Option<LogIndex> {
    let majority = match_indexes.len() / 2 + 1;
    let mut n = log.last().map_or(0, |e| e.index);
    while n > current {
        let replicated = match_indexes.iter().filter(|&&m| m >= n).count();
        if replicated >= majority
            && log[usize::try_from(n - 1).expect("log index fits usize")].term == current_term
        {
            return Some(n);
        }
        n -= 1;
    }
    None
}

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

    /// R18: success raises this peer's watermark monotonically and probes
    /// commit advancement (R19); failure walks `next_index` back — hint-
    /// accelerated, floored at 1, with plain decrement as the guaranteed-
    /// terminating fallback. Retry rides the next heartbeat (R17).
    pub(crate) fn on_append_entries_reply(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: LogIndex,
        effects: &mut Vec<Effect>,
    ) {
        // R3: stale replies — an older term, or arriving in a role that no
        // longer expects them — must not mutate current bookkeeping. A slow
        // network cannot be allowed to resurrect dead state.
        if term != self.hard.current_term {
            return;
        }
        let own_last = self.last_log_index();
        let advanced = {
            let Role::Leader {
                next_index,
                match_index: peer_match,
            } = &mut self.role
            else {
                return;
            };
            let (Some(next), Some(matched)) = (next_index.get_mut(&from), peer_match.get_mut(&from))
            else {
                return; // not a peer of this cluster
            };
            if success {
                // R18: monotone — a duplicate or reordered success can never
                // move the watermark backwards.
                *matched = (*matched).max(match_index);
                *next = *matched + 1;
                // R19: the leader counts itself via its own log length.
                let mut all: Vec<LogIndex> = peer_match.values().copied().collect();
                all.push(own_last);
                commit_advance(self.commit_index, self.hard.current_term, &self.log, &all)
            } else {
                *next = 1.max((*next - 1).min(match_index + 1));
                None
            }
        };
        if let Some(n) = advanced {
            self.commit_index = n;
            self.apply_committed(effects); // R4
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log(terms: &[Term]) -> Vec<Entry> {
        terms
            .iter()
            .enumerate()
            .map(|(i, &term)| Entry {
                index: i as u64 + 1,
                term,
                command: Command::NoOp,
            })
            .collect()
    }

    /// R19b's load-bearing clause: a majority-replicated PRIOR-term entry
    /// must never advance the commit index by count alone — the Figure 8
    /// disaster. It commits only underneath a current-term entry.
    #[test]
    fn commit_advance_blocks_prior_term_majority() {
        // Term-4 leader; idx 2 (term 2) sits on a majority (self=3, 2, 0);
        // idx 3 is the leader's own term-4 NoOp.
        let l = log(&[1, 2, 4]);
        // NoOp not yet on a majority: only the prior-term entry is.
        assert_eq!(commit_advance(0, 4, &l, &[3, 2, 0]), None);
        // The moment the term-4 entry reaches a majority, everything below
        // commits with it — carried, not counted.
        assert_eq!(commit_advance(0, 4, &l, &[3, 3, 0]), Some(3));
    }

    /// R19a: a strict majority of the CLUSTER (leader included) — half is
    /// not enough, and the bar is ⌊n/2⌋+1 for even and odd cluster sizes.
    #[test]
    fn commit_advance_requires_strict_majority() {
        let l = log(&[1, 1, 1]);
        // 5-node cluster: 2 of 5 at index 3 is not a majority...
        assert_eq!(commit_advance(0, 1, &l, &[3, 3, 0, 0, 0]), None);
        // ...3 of 5 is.
        assert_eq!(commit_advance(0, 1, &l, &[3, 3, 3, 0, 0]), Some(3));
    }

    /// R19 takes the LARGEST qualifying N, and returns None when nothing
    /// above `current` qualifies (no regression, no busywork).
    #[test]
    fn commit_advance_takes_largest_qualifying_n() {
        let l = log(&[1, 1, 1, 1]);
        // Majority holds up to 3; one straggler at 4.
        assert_eq!(commit_advance(1, 1, &l, &[4, 3, 3]), Some(3));
        // Already committed there: nothing new.
        assert_eq!(commit_advance(3, 1, &l, &[4, 3, 3]), None);
    }
}
