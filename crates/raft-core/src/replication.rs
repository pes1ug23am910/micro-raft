//! Log replication and commitment: consistency checks, conflict repair,
//! follower commit updates, leader progress tracking, client proposals, and
//! the current-term rule for advancing `commit_index`.

use crate::message::{Effect, RaftMessage};
use crate::types::{Command, Entry, LogIndex, NodeId, Role, Term};
use crate::{RaftNode, MAX_APPEND_ENTRIES};

/// Validate the structural framing of an `AppendEntries` payload and return
/// the last index covered by the RPC. An empty heartbeat covers its anchor.
/// Indices must be contiguous without overflow, and terms must be
/// nondecreasing from the anchor without exceeding the leader's current term.
pub(crate) fn validate_append_entries(
    message_term: Term,
    prev_log_index: LogIndex,
    prev_log_term: Term,
    entries: &[Entry],
) -> Option<LogIndex> {
    if prev_log_term > message_term {
        return None;
    }
    let entries_len = LogIndex::try_from(entries.len()).ok()?;
    let last_index = prev_log_index.checked_add(entries_len)?;
    let mut previous_term = prev_log_term;
    for (offset, entry) in entries.iter().enumerate() {
        let offset = LogIndex::try_from(offset).ok()?;
        let expected = prev_log_index.checked_add(1)?.checked_add(offset)?;
        if entry.index != expected || entry.term < previous_term || entry.term > message_term {
            return None;
        }
        previous_term = entry.term;
    }
    Some(last_index)
}

/// R19 as a pure function so the commitment rule is independently testable.
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
    /// R17: to every peer, a bounded batch beginning at `next_index[p]`
    /// (empty = pure heartbeat), anchored at `prev_log_index/term`. Entries
    /// piggyback on the heartbeat cadence rather than being pushed on propose,
    /// costing at most one HEARTBEAT_MS of latency per catch-up batch.
    pub(crate) fn send_append_entries(&mut self, effects: &mut Vec<Effect>) {
        let Role::Leader { next_index, .. } = &self.role else {
            return;
        };
        for &peer in &self.peers {
            let next = next_index
                .get(&peer)
                .map_or(self.last_log_index() + 1, |&n| n);
            let prev_log_index = next - 1;
            let prev_log_term = self.term_at(prev_log_index);
            let from = usize::try_from(prev_log_index).expect("log index fits usize");
            let to = from.saturating_add(MAX_APPEND_ENTRIES).min(self.log.len());
            let entries = self
                .log
                .get(from..to)
                .map_or_else(Vec::new, <[Entry]>::to_vec);
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
        // The persist precedes the accept — nothing may act on
        // an entry the leader itself hasn't made durable.
        effects.push(Effect::PersistLogEntries {
            truncate_from: None,
            entries: vec![entry.clone()],
        });
        effects.push(Effect::ProposeAccepted { index: entry.index });
    }

    /// The follower's accept path: R12 consistency check, R13 conflict
    /// repair + append, R14 commit update, R4 apply — after the term rules
    /// (R3 stale rejection, R11 candidate step-down, R6b timing).
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_append_entries(
        &mut self,
        term: Term,
        leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        payload_last_index: LogIndex,
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
                    match_index: self.last_log_index(),
                },
            });
            return;
        }
        // term == current_term here (R2 equalized anything newer): someone
        // legitimately leads this term. R11: a candidate steps down.
        self.become_follower(effects);
        // Any structurally valid AppendEntries in the current term confirms
        // a leader and resets the election timer, even if its anchor fails.
        self.reset_election_deadline();
        // R20: this sender is the freshest leader sighting we have.
        self.leader_hint = Some(leader_id);

        // R12: the consistency check — do we hold the leader's anchor entry?
        // Failure still counted the sender as leader above; the reply carries
        // my last log index so the leader can retry from the following index.
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
            // before the success reply below.
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
                match_index: payload_last_index,
            },
        });

        // R14: lift commit_index toward the leader's, bounded by what this
        // RPC covered — never backwards — then apply in order (R4).
        let new_commit = leader_commit.min(payload_last_index);
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_committed(effects);
        }
    }

    /// R18: success raises this peer's watermark monotonically and probes
    /// commit advancement (R19); failure walks `next_index` back — hint-
    /// accelerated, floored at 1, with plain decrement as the terminating
    /// fallback. Retry rides the next heartbeat.
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
            let (Some(next), Some(matched)) =
                (next_index.get_mut(&from), peer_match.get_mut(&from))
            else {
                return; // not a peer of this cluster
            };
            if success {
                // R18: monotone — a duplicate or reordered success can never
                // move the watermark backwards.
                *matched = (*matched).max(match_index);
                *next = matched.saturating_add(1);
                // R19: the leader counts itself via its own log length.
                let mut all: Vec<LogIndex> = peer_match.values().copied().collect();
                all.push(own_last);
                commit_advance(self.commit_index, self.hard.current_term, &self.log, &all)
            } else {
                *next = 1.max(next.saturating_sub(1).min(match_index.saturating_add(1)));
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
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;
    use crate::Input;

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

    fn append_entries(prev_log_index: LogIndex, indexes: &[LogIndex]) -> RaftMessage {
        append_entries_with_terms(
            9,
            prev_log_index,
            0,
            &indexes.iter().map(|&index| (index, 9)).collect::<Vec<_>>(),
        )
    }

    fn append_entries_with_terms(
        message_term: Term,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: &[(LogIndex, Term)],
    ) -> RaftMessage {
        RaftMessage::AppendEntries {
            term: message_term,
            leader_id: 2,
            prev_log_index,
            prev_log_term,
            entries: entries
                .iter()
                .map(|&(index, term)| Entry {
                    index,
                    term,
                    command: Command::NoOp,
                })
                .collect(),
            leader_commit: LogIndex::MAX,
        }
    }

    fn assert_malformed_append_is_side_effect_free(msg: RaftMessage) {
        let mut node = RaftNode::new(1, vec![2, 3], 7);
        node.hard.current_term = 3;
        node.hard.voted_for = Some(1);
        node.log = log(&[2]);
        node.role = Role::Candidate {
            votes_received: BTreeSet::from([1]),
        };
        node.election_deadline_ms = 123;
        node.leader_hint = Some(3);

        let hard_before = node.hard.clone();
        let log_before = node.log.clone();
        let role_before = node.role.clone();
        let effects = node.step(Input::Message { from: 2, msg });

        assert_eq!(node.hard, hard_before);
        assert_eq!(node.log, log_before);
        assert_eq!(node.role, role_before);
        assert_eq!(node.commit_index, 0);
        assert_eq!(node.last_applied, 0);
        assert_eq!(node.election_deadline_ms, 123);
        assert_eq!(node.leader_hint, Some(3));
        assert_eq!(
            effects,
            vec![Effect::Send {
                to: 2,
                msg: RaftMessage::AppendEntriesReply {
                    term: 3,
                    success: false,
                    match_index: 1,
                },
            }]
        );
    }

    #[test]
    fn malformed_append_indices_are_rejected_before_state_changes() {
        assert_malformed_append_is_side_effect_free(append_entries(0, &[0]));
        assert_malformed_append_is_side_effect_free(append_entries(0, &[1, 3]));
        assert_malformed_append_is_side_effect_free(append_entries(
            LogIndex::MAX,
            &[LogIndex::MAX],
        ));
        assert_malformed_append_is_side_effect_free(append_entries(
            LogIndex::MAX - 1,
            &[LogIndex::MAX, 0],
        ));
    }

    #[test]
    fn malformed_append_terms_are_rejected_before_state_changes() {
        assert_malformed_append_is_side_effect_free(append_entries_with_terms(9, 1, 5, &[(2, 4)]));
        assert_malformed_append_is_side_effect_free(append_entries_with_terms(
            9,
            0,
            0,
            &[(1, 4), (2, 3)],
        ));
        assert_malformed_append_is_side_effect_free(append_entries_with_terms(9, 0, 0, &[(1, 10)]));
        assert_malformed_append_is_side_effect_free(append_entries_with_terms(9, 1, 10, &[]));
    }

    #[test]
    fn overflow_adjacent_valid_shapes_fail_consistency_without_panicking() {
        for (prev_log_index, indexes) in [
            (LogIndex::MAX, Vec::new()),
            (LogIndex::MAX - 1, vec![LogIndex::MAX]),
        ] {
            let mut node = RaftNode::new(1, vec![2, 3], 7);
            node.hard.current_term = 9;
            node.log = log(&[2]);
            let log_before = node.log.clone();

            let effects = node.step(Input::Message {
                from: 2,
                msg: append_entries(prev_log_index, &indexes),
            });

            assert_eq!(node.log, log_before);
            assert!(!effects.iter().any(|effect| matches!(
                effect,
                Effect::PersistHardState(_) | Effect::PersistLogEntries { .. } | Effect::Apply(_)
            )));
            assert!(effects.iter().any(|effect| matches!(
                effect,
                Effect::Send {
                    msg: RaftMessage::AppendEntriesReply { success: false, .. },
                    ..
                }
            )));
        }
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

    #[test]
    fn lagging_follower_advances_in_bounded_batches() {
        let mut node = RaftNode::new(1, vec![2], 7);
        node.hard.current_term = 4;
        node.log = log(&vec![4; MAX_APPEND_ENTRIES * 2 + 5]);
        node.role = Role::Leader {
            next_index: BTreeMap::from([(2, 1)]),
            match_index: BTreeMap::from([(2, 0)]),
        };

        let mut batch_sizes = Vec::new();
        let mut matched = 0;
        while matched < node.last_log_index() {
            let mut effects = Vec::new();
            node.send_append_entries(&mut effects);
            let (prev, entries) = effects
                .iter()
                .find_map(|effect| match effect {
                    Effect::Send {
                        to: 2,
                        msg:
                            RaftMessage::AppendEntries {
                                prev_log_index,
                                entries,
                                ..
                            },
                    } => Some((*prev_log_index, entries)),
                    _ => None,
                })
                .expect("leader sends to its follower");

            assert_eq!(prev, matched);
            assert!(!entries.is_empty());
            assert!(entries.len() <= MAX_APPEND_ENTRIES);
            batch_sizes.push(entries.len());
            matched += u64::try_from(entries.len()).expect("batch length fits");

            let mut reply_effects = Vec::new();
            node.on_append_entries_reply(2, 4, true, matched, &mut reply_effects);
        }

        assert_eq!(batch_sizes, vec![MAX_APPEND_ENTRIES, MAX_APPEND_ENTRIES, 5]);
        let Role::Leader {
            next_index,
            match_index,
        } = &node.role
        else {
            panic!("node stopped leading");
        };
        assert_eq!(match_index.get(&2), Some(&node.last_log_index()));
        assert_eq!(next_index.get(&2), Some(&(node.last_log_index() + 1)));
    }
}
