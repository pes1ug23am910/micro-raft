//! Log replication and commitment: consistency checks, conflict repair,
//! follower commit updates, leader progress tracking, client proposals, and
//! the current-term rule for advancing `commit_index`.

use crate::message::{AppendConflictHint, Effect, RaftMessage};
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
    for entry in log.iter().rev().take_while(|entry| entry.index > current) {
        let replicated = match_indexes.iter().filter(|&&m| m >= entry.index).count();
        if replicated >= majority && entry.term == current_term {
            return Some(entry.index);
        }
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
        let next_index = next_index.clone();
        for peer in self.peers.clone() {
            self.advertise_replication_membership(peer, effects);
            let next = next_index
                .get(&peer)
                .map_or(self.last_log_index().saturating_add(1), |&n| n);
            if next <= self.snapshot_index() {
                self.send_snapshot_chunk(peer, effects);
                continue;
            }
            self.outgoing_snapshots.remove(&peer);
            let prev_log_index = next - 1;
            let prev_log_term = self.term_at(prev_log_index);
            let from = usize::try_from(prev_log_index - self.snapshot_index())
                .expect("log offset fits usize");
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
                    contact_round: self.contact_round,
                },
            });
        }
    }

    /// R20: a Leader appends the command at the next index in its own term,
    /// persists it, and acknowledges with the assigned index — replication
    /// then rides the next heartbeat (R17). Anyone else refuses, hinting at
    /// the last known leader so the client can retry there.
    pub(crate) fn on_client_propose(&mut self, command: Command, effects: &mut Vec<Effect>) {
        self.on_client_propose_batch(vec![command], effects);
    }

    pub(crate) fn on_client_propose_batch(
        &mut self,
        commands: Vec<Command>,
        effects: &mut Vec<Effect>,
    ) {
        if commands.is_empty() {
            return;
        }
        let last = self.last_log_index();
        let final_index = last
            .checked_add(commands.len() as u64)
            .filter(|index| *index < LogIndex::MAX);
        if !self.is_leader()
            || !self.is_voter(self.id)
            || commands
                .iter()
                .any(|command| matches!(command, Command::Configuration(_)))
            || commands.len() > crate::MAX_PROPOSAL_BATCH
            || final_index.is_none()
        {
            effects.extend(commands.iter().map(|_| Effect::ProposeRejected {
                leader_hint: self.leader_hint,
            }));
            return;
        }
        let entries: Vec<_> = commands
            .into_iter()
            .enumerate()
            .map(|(offset, command)| Entry {
                index: last + offset as u64 + 1,
                term: self.hard.current_term,
                command,
            })
            .collect();
        self.log.extend(entries.iter().cloned());
        // All durable bytes precede the first acceptance, including in a
        // singleton where this batch can immediately commit and apply.
        effects.push(Effect::PersistLogEntries {
            truncate_from: None,
            entries: entries.clone(),
        });
        effects.extend(
            entries
                .iter()
                .map(|entry| Effect::ProposeAccepted { index: entry.index }),
        );
        if self.self_quorum() {
            self.advance_commit(final_index.expect("checked range"), effects);
        }
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
        contact_round: u64,
        effects: &mut Vec<Effect>,
    ) {
        // R3: a stale leader gets the current term and a rejection.
        if term < self.hard.current_term {
            effects.push(Effect::Send {
                to: leader_id,
                msg: RaftMessage::AppendEntriesReply {
                    conflict: None,
                    term: self.hard.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                    contact_round,
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
        self.last_leader_contact_ms = Some(self.now_ms);

        if prev_log_index < self.snapshot_index() {
            effects.push(Effect::Send {
                to: leader_id,
                msg: RaftMessage::CompactedPrefix {
                    term: self.hard.current_term,
                    last_included_index: self.snapshot_index(),
                    last_included_term: self.snapshot_term(),
                    contact_round,
                },
            });
            return;
        }

        // R12: the consistency check — do we hold the leader's anchor entry?
        // The last-index fallback remains available to older decoders. The
        // optional hint skips an entire conflicting term in upgraded leaders.
        if self.log_term(prev_log_index) != Some(prev_log_term) {
            let conflict = self.append_conflict_hint(prev_log_index);
            effects.push(Effect::Send {
                to: leader_id,
                msg: RaftMessage::AppendEntriesReply {
                    term: self.hard.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                    contact_round,
                    conflict,
                },
            });
            return;
        }

        // Refuse any conflict inside the already committed prefix.
        if entries.iter().any(|entry| {
            entry.index <= self.commit_index && self.log_term(entry.index) != Some(entry.term)
        }) {
            effects.push(Effect::Send {
                to: leader_id,
                msg: RaftMessage::AppendEntriesReply {
                    conflict: None,
                    term: self.hard.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                    contact_round,
                },
            });
            return;
        }

        let mut candidate = self.log.clone();
        for entry in &entries {
            let offset = (entry.index - self.snapshot_index() - 1) as usize;
            if candidate
                .get(offset)
                .is_some_and(|old| old.term == entry.term)
            {
                continue;
            }
            candidate.truncate(offset);
            candidate.push(entry.clone());
        }
        if !self.valid_configuration_suffix(&candidate) {
            effects.push(Effect::Send {
                to: leader_id,
                msg: RaftMessage::AppendEntriesReply {
                    conflict: None,
                    term: self.hard.current_term,
                    success: false,
                    match_index: self.last_log_index(),
                    contact_round: 0,
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
                self.log.truncate(
                    usize::try_from(entry.index - self.snapshot_index() - 1)
                        .expect("log offset fits usize"),
                );
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
        // Persist membership commitment before replying or applying commands.
        let new_commit = leader_commit.min(payload_last_index);
        if new_commit > self.commit_index {
            self.advance_commit(new_commit, effects);
        } else {
            self.refresh_effective_membership(effects);
        }
        effects.push(Effect::Send {
            to: leader_id,
            msg: RaftMessage::AppendEntriesReply {
                conflict: None,
                term: self.hard.current_term,
                success: true,
                match_index: payload_last_index,
                contact_round,
            },
        });
    }

    /// First *available* index of the conflicting term. Terms are monotonic,
    /// so binary search avoids scanning a long same-term suffix per rejection.
    /// A snapshot retains its boundary term but cannot reveal compacted entries.
    fn append_conflict_hint(&self, rejected_index: LogIndex) -> Option<AppendConflictHint> {
        let term = self.log_term(rejected_index);
        let first_index = match term {
            Some(0) => return None, // index-zero framing error, not a real term
            Some(term) if self.snapshot_term() == term => self.snapshot_index(),
            Some(term) => self.log[self.log.partition_point(|entry| entry.term < term)].index,
            None => self.last_log_index().checked_add(1)?,
        };
        Some(AppendConflictHint {
            rejected_index,
            term,
            first_index,
        })
    }

    fn last_index_in_term(&self, term: Term) -> Option<LogIndex> {
        let end = self.log.partition_point(|entry| entry.term <= term);
        if let Some(entry) = end.checked_sub(1).and_then(|i| self.log.get(i)) {
            if entry.term == term {
                return Some(entry.index);
            }
        }
        (self.snapshot_term() == term).then_some(self.snapshot_index())
    }

    /// R18: successes raise replication watermarks; rejection hints only move
    /// the retry cursor backwards, never below confirmed replication. A term
    /// found locally skips to its end; an absent term skips to the follower's
    /// first available index for it. Retry rides the next heartbeat.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn on_append_entries_reply(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: LogIndex,
        contact_round: u64,
        conflict: Option<AppendConflictHint>,
        effects: &mut Vec<Effect>,
    ) {
        // R3: stale replies — an older term, or arriving in a role that no
        // longer expects them — must not mutate current bookkeeping. A slow
        // network cannot be allowed to resurrect dead state.
        if term != self.hard.current_term {
            return;
        }
        let own_last = self.last_log_index();
        if success && match_index > own_last {
            return;
        }
        let hinted_next = if let Some(hint) = conflict {
            let valid = !success
                && hint.first_index > 0
                && hint.first_index <= hint.rejected_index
                && match hint.term {
                    Some(conflict_term) => {
                        conflict_term > 0
                            && conflict_term <= term
                            && hint.rejected_index <= match_index
                            && self.log_term(hint.rejected_index) != Some(conflict_term)
                    }
                    None => match_index.checked_add(1) == Some(hint.first_index),
                };
            if !valid {
                return; // Malformed hints cannot count as fresh quorum contact.
            }
            Some(
                hint.term
                    .and_then(|term| self.last_index_in_term(term))
                    .map_or(hint.first_index, |index| index.saturating_add(1)),
            )
        } else {
            None
        };
        let successful = {
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
            if conflict.is_some_and(|hint| {
                hint.rejected_index != next.saturating_sub(1) || hint.rejected_index <= *matched
            }) {
                return; // A newer reply already changed this request's cursor.
            }
            // A consistency rejection still proves the peer heard this round.
            // Old-round replication information remains useful, but cannot renew
            // leadership. The exact round is term-scoped and never reused.
            if contact_round != 0 && contact_round == self.contact_round {
                self.quorum_contacts.insert(from);
            }
            if success {
                // R18: monotone — a duplicate or reordered success can never
                // move the watermark backwards.
                *matched = (*matched).max(match_index);
                *next = matched.saturating_add(1);
                true
            } else {
                let floor = matched.saturating_add(1).max(1);
                let candidate = hinted_next.unwrap_or_else(|| match_index.saturating_add(1));
                *next = candidate.min(next.saturating_sub(1)).max(floor);
                false
            }
        };
        if successful {
            if let Some(n) = self.quorum_commit_candidate() {
                self.advance_commit(n, effects);
            }
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
            contact_round: 0,
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
                    conflict: None,
                    contact_round: 0,
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
                    msg: RaftMessage::AppendEntriesReply {
                        contact_round: 0,
                        success: false,
                        ..
                    },
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
            node.on_append_entries_reply(2, 4, true, matched, 0, None, &mut reply_effects);
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
