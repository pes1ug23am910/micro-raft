//! ReadIndex: a new quorum challenge for each local read, never a time lease.
use crate::{Effect, LogIndex, NodeId, RaftMessage, RaftNode, ReadRejectReason, Term};
use std::collections::BTreeSet;

/// Bounded local waiters. Drivers cancel disconnected or timed-out requests.
pub const MAX_PENDING_READS: usize = 128;

#[derive(Debug)]
pub(crate) struct PendingRead {
    pub(crate) context: u64,
    pub(crate) term: Term,
    pub(crate) index: LogIndex,
    pub(crate) voters: BTreeSet<NodeId>,
}

/// Deliberate protocol defects compiled only into the core's unit-test binary.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum ReadFaultMode {
    None,
    BypassQuorum,
    AcceptWrongContext,
}

impl RaftNode {
    pub(crate) fn on_read_index(&mut self, request_id: u64, effects: &mut Vec<Effect>) {
        // An accidental duplicate cannot replace the first waiter/context or
        // generate a rejection that the driver could mistake for its original.
        if self.pending_reads.contains_key(&request_id) {
            return;
        }
        let reason = if !self.is_leader() || !self.is_voter(self.id) {
            Some(ReadRejectReason::NotLeader)
        } else if self.commit_index == 0
            || self.term_at(self.commit_index) != self.hard.current_term
        {
            // A majority must first commit something in this leader's term.
            Some(ReadRejectReason::NotReady)
        } else if self.pending_reads.len() >= MAX_PENDING_READS {
            Some(ReadRejectReason::Capacity)
        } else if self.read_context == u64::MAX {
            Some(ReadRejectReason::ContextExhausted)
        } else {
            None
        };
        if let Some(reason) = reason {
            effects.push(Effect::ReadRejected {
                request_id,
                leader_hint: self.leader_hint,
                reason,
            });
            return;
        }
        self.read_context += 1; // checked exhaustion above, never reuse in a term
        let context = self.read_context;
        let term = self.hard.current_term;
        let index = self.commit_index;
        let has_quorum = self.self_quorum();
        #[cfg(test)]
        let has_quorum = if self.read_fault == ReadFaultMode::BypassQuorum {
            self.read_fault_hits += 1;
            true
        } else {
            has_quorum
        };
        if has_quorum {
            effects.push(Effect::ReadReady {
                request_id,
                context,
                index,
                term,
            });
            return;
        }
        self.pending_reads.insert(
            request_id,
            PendingRead {
                context,
                term,
                index,
                voters: BTreeSet::from([self.id]),
            },
        );
        for &peer in &self.peers {
            if !self.is_voter(peer) {
                continue;
            }
            effects.push(Effect::Send {
                to: peer,
                msg: RaftMessage::ReadProbe {
                    term,
                    leader_id: self.id,
                    context,
                },
            });
        }
    }

    pub(crate) fn on_read_probe(
        &mut self,
        term: Term,
        leader_id: NodeId,
        context: u64,
        effects: &mut Vec<Effect>,
    ) {
        let accepted =
            term == self.hard.current_term && self.is_voter(self.id) && self.is_voter(leader_id);
        if accepted {
            // Like an empty AppendEntries, a valid current-term leader probe
            // establishes contact, but advances neither commit nor apply state.
            self.become_follower(effects);
            self.reset_election_deadline();
            self.last_leader_contact_ms = Some(self.now_ms);
            self.leader_hint = Some(leader_id);
        }
        effects.push(Effect::Send {
            to: leader_id,
            msg: RaftMessage::ReadProbeReply {
                term: self.hard.current_term,
                context,
                accepted,
            },
        });
    }

    pub(crate) fn on_read_probe_reply(
        &mut self,
        from: NodeId,
        term: Term,
        context: u64,
        accepted: bool,
        effects: &mut Vec<Effect>,
    ) {
        if !self.is_leader() || !accepted || term != self.hard.current_term || !self.is_voter(from)
        {
            return;
        }
        let quorum = self.voter_config().clone();
        #[cfg(test)]
        let accept_wrong_context = self.read_fault == ReadFaultMode::AcceptWrongContext;
        let Some((&request_id, pending)) = self.pending_reads.iter_mut().find(|(_, pending)| {
            let context_matches = pending.context == context;
            #[cfg(test)]
            let context_matches = context_matches || accept_wrong_context;
            context_matches && pending.term == term
        }) else {
            return;
        };
        #[cfg(test)]
        if pending.context != context {
            self.read_fault_hits += 1;
        }
        pending.voters.insert(from);
        if quorum.has_quorum(&pending.voters) {
            let pending = self.pending_reads.remove(&request_id).expect("active read");
            effects.push(Effect::ReadReady {
                request_id,
                context,
                index: pending.index,
                term,
            });
        }
    }

    pub(crate) fn cancel_read(&mut self, request_id: u64, effects: &mut Vec<Effect>) {
        if self.pending_reads.remove(&request_id).is_some() {
            effects.push(Effect::ReadRejected {
                request_id,
                leader_hint: self.leader_hint,
                reason: ReadRejectReason::Cancelled,
            });
        }
    }

    pub(crate) fn reject_pending_reads(
        &mut self,
        reason: ReadRejectReason,
        effects: &mut Vec<Effect>,
    ) {
        for request_id in std::mem::take(&mut self.pending_reads).into_keys() {
            effects.push(Effect::ReadRejected {
                request_id,
                leader_hint: self.leader_hint,
                reason,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Command, Entry, HardState, Input, Role, CHECK_QUORUM_MS};

    fn leader(peers: Vec<NodeId>, committed: bool) -> RaftNode {
        let mut node = RaftNode::new(1, peers, 111);
        node.step(Input::Tick { now_ms: 10 });
        node.start_election(&mut Vec::new());
        for peer in node.peers.clone() {
            node.on_request_vote_reply(peer, node.hard.current_term, true, &mut Vec::new());
        }
        if committed {
            for peer in node.peers.clone() {
                node.on_append_entries_reply(
                    peer,
                    node.hard.current_term,
                    true,
                    1,
                    1,
                    &mut Vec::new(),
                );
            }
        }
        assert!(node.is_leader());
        node
    }

    fn context(effects: &[Effect]) -> u64 {
        effects
            .iter()
            .find_map(|effect| match effect {
                Effect::Send {
                    msg: RaftMessage::ReadProbe { context, .. },
                    ..
                } => Some(*context),
                _ => None,
            })
            .expect("read sent a probe")
    }

    fn reply(
        node: &mut RaftNode,
        from: NodeId,
        term: Term,
        context: u64,
        accepted: bool,
    ) -> Vec<Effect> {
        node.step(Input::Message {
            from,
            msg: RaftMessage::ReadProbeReply {
                term,
                context,
                accepted,
            },
        })
    }

    #[test]
    fn reads_require_a_committed_current_term_entry_and_do_not_append_or_persist() {
        let mut node = leader(vec![2, 3], false);
        let log = node.log.clone();
        let hard = node.hard.clone();
        let refused = node.step(Input::ReadIndex { request_id: 7 });
        assert!(matches!(
            refused.as_slice(),
            [Effect::ReadRejected {
                request_id: 7,
                reason: ReadRejectReason::NotReady,
                ..
            }]
        ));
        node.on_append_entries_reply(2, 1, true, 1, 1, &mut Vec::new());
        let admitted = node.step(Input::ReadIndex { request_id: 8 });
        assert!(admitted.iter().all(|e| matches!(
            e,
            Effect::Send {
                msg: RaftMessage::ReadProbe { .. },
                ..
            }
        )));
        assert_eq!(node.hard, hard);
        assert_eq!(node.log, log);
        let barrier = reply(&mut node, 2, 1, context(&admitted), true);
        assert!(matches!(
            barrier.as_slice(),
            [Effect::ReadReady {
                request_id: 8,
                index: 1,
                term: 1,
                ..
            }]
        ));
        assert_eq!(node.log, log);
    }

    #[test]
    fn prior_term_commit_does_not_open_read_admission() {
        let mut node = RaftNode::restore(
            1,
            vec![2, 3],
            111,
            HardState {
                membership: None,
                current_term: 2,
                voted_for: None,
            },
            vec![Entry {
                index: 1,
                term: 1,
                command: Command::NoOp,
            }],
        )
        .unwrap();
        node.commit_index = 1;
        node.last_applied = 1;
        node.start_election(&mut Vec::new());
        node.on_request_vote_reply(2, 3, true, &mut Vec::new());
        assert!(node.is_leader());
        assert_eq!(node.log.last().unwrap().term, 3);
        assert!(matches!(
            node.step(Input::ReadIndex { request_id: 1 }).as_slice(),
            [Effect::ReadRejected {
                reason: ReadRejectReason::NotReady,
                ..
            }]
        ));
    }

    #[test]
    fn exact_context_term_and_distinct_configured_majority_are_required() {
        let mut node = leader(vec![2, 3, 4, 5], true);
        let c = context(&node.step(Input::ReadIndex { request_id: 10 }));
        for (from, term, got, accepted) in [
            (2, 0, c, true),
            (2, 1, c + 1, true),
            (99, 1, c, true),
            (1, 1, c, true),
            (2, 1, c, false),
            (2, 1, 0, true),
        ] {
            assert!(reply(&mut node, from, term, got, accepted).is_empty());
        }
        assert!(reply(&mut node, 2, 1, c, true).is_empty());
        assert!(reply(&mut node, 2, 1, c, true).is_empty());
        assert_eq!(node.pending_reads.len(), 1);
        assert!(
            matches!(reply(&mut node, 3, 1, c, true).as_slice(), [Effect::ReadReady { request_id: 10, context, .. }] if *context == c)
        );
        assert!(reply(&mut node, 4, 1, c, true).is_empty());
    }

    #[test]
    fn every_request_has_new_context_even_after_success_or_cancellation() {
        let mut node = leader(vec![2, 3], true);
        let first = context(&node.step(Input::ReadIndex { request_id: 1 }));
        assert!(node.step(Input::ReadIndex { request_id: 1 }).is_empty());
        assert_eq!(node.pending_reads[&1].context, first);
        assert!(matches!(
            node.step(Input::CancelRead { request_id: 1 }).as_slice(),
            [Effect::ReadRejected {
                reason: ReadRejectReason::Cancelled,
                ..
            }]
        ));
        assert!(node.step(Input::CancelRead { request_id: 1 }).is_empty());
        let second = context(&node.step(Input::ReadIndex { request_id: 2 }));
        assert_ne!(first, second);
        assert!(reply(&mut node, 2, 1, first, true).is_empty());
        assert!(matches!(
            reply(&mut node, 2, 1, second, true).as_slice(),
            [Effect::ReadReady { request_id: 2, .. }]
        ));
        let third = context(&node.step(Input::ReadIndex { request_id: 3 }));
        assert_ne!(third, second);
        assert!(reply(&mut node, 2, 1, second, true).is_empty());
    }

    #[test]
    fn admission_is_bounded_and_cancel_frees_capacity() {
        let mut node = leader(vec![2, 3], true);
        for request_id in 0..MAX_PENDING_READS as u64 {
            context(&node.step(Input::ReadIndex { request_id }));
        }
        assert_eq!(node.pending_reads.len(), MAX_PENDING_READS);
        assert!(matches!(
            node.step(Input::ReadIndex { request_id: 999 }).as_slice(),
            [Effect::ReadRejected {
                reason: ReadRejectReason::Capacity,
                ..
            }]
        ));
        node.step(Input::CancelRead { request_id: 1 });
        context(&node.step(Input::ReadIndex { request_id: 1000 }));
        assert_eq!(node.pending_reads.len(), MAX_PENDING_READS);
        node.step(Input::CancelRead { request_id: 2 });
        node.read_context = u64::MAX;
        assert!(matches!(
            node.step(Input::ReadIndex { request_id: 1001 }).as_slice(),
            [Effect::ReadRejected {
                reason: ReadRejectReason::ContextExhausted,
                ..
            }]
        ));
    }

    #[test]
    fn higher_actual_term_cancels_once_after_persist_and_old_callbacks_cannot_complete() {
        let mut node = leader(vec![2, 3], true);
        let c = context(&node.step(Input::ReadIndex { request_id: 7 }));
        let effects = reply(&mut node, 2, 2, c, false);
        assert!(matches!(
            effects.first(),
            Some(Effect::PersistHardState(HardState {
                current_term: 2,
                ..
            }))
        ));
        assert_eq!(
            effects
                .iter()
                .filter(|e| matches!(
                    e,
                    Effect::ReadRejected {
                        request_id: 7,
                        reason: ReadRejectReason::LeadershipLost,
                        ..
                    }
                ))
                .count(),
            1
        );
        assert_eq!(node.role, Role::Follower);
        assert!(node.pending_reads.is_empty());
        assert!(reply(&mut node, 3, 1, c, true).is_empty());
        assert!(node.step(Input::CancelRead { request_id: 7 }).is_empty());
        assert!(matches!(
            node.step(Input::ReadIndex { request_id: 8 }).as_slice(),
            [Effect::ReadRejected {
                reason: ReadRejectReason::NotLeader,
                ..
            }]
        ));
    }

    #[test]
    fn check_quorum_loss_cancels_pending_read_without_a_success() {
        let mut node = leader(vec![2, 3], true);
        let c = context(&node.step(Input::ReadIndex { request_id: 44 }));
        let first_boundary = node.quorum_deadline_ms;
        node.step(Input::Tick {
            now_ms: first_boundary,
        });
        let effects = node.step(Input::Tick {
            now_ms: first_boundary + CHECK_QUORUM_MS,
        });
        assert!(effects.iter().any(|e| matches!(
            e,
            Effect::ReadRejected {
                request_id: 44,
                reason: ReadRejectReason::LeadershipLost,
                ..
            }
        )));
        assert!(!effects
            .iter()
            .any(|e| matches!(e, Effect::ReadReady { .. })));
        assert!(reply(&mut node, 2, 1, c, true).is_empty());
    }

    #[test]
    fn read_fence_is_the_committed_index_captured_at_admission() {
        let mut node = leader(vec![2, 3], true);
        let c = context(&node.step(Input::ReadIndex { request_id: 1 }));
        node.step(Input::ClientPropose {
            command: Command::Put {
                key: "k".into(),
                value: "v".into(),
            },
        });
        node.on_append_entries_reply(2, 1, true, 2, 1, &mut Vec::new());
        assert_eq!(node.commit_index, 2);
        assert!(matches!(
            reply(&mut node, 2, 1, c, true).as_slice(),
            [Effect::ReadReady { index: 1, .. }]
        ));
    }

    #[test]
    fn valid_probe_confirms_term_without_mutating_log_or_commit() {
        let mut node = RaftNode::new(2, vec![1, 3], 5);
        let effects = node.step(Input::Message {
            from: 1,
            msg: RaftMessage::ReadProbe {
                term: 3,
                leader_id: 1,
                context: 9,
            },
        });
        assert!(matches!(
            effects.first(),
            Some(Effect::PersistHardState(HardState {
                current_term: 3,
                ..
            }))
        ));
        assert!(matches!(
            effects.last(),
            Some(Effect::Send {
                msg: RaftMessage::ReadProbeReply {
                    term: 3,
                    context: 9,
                    accepted: true
                },
                ..
            })
        ));
        assert!(node.log.is_empty());
        assert_eq!(node.commit_index, 0);
        let hard = node.hard.clone();
        assert!(node
            .step(Input::Message {
                from: 1,
                msg: RaftMessage::ReadProbe {
                    term: 99,
                    leader_id: 3,
                    context: 1
                }
            })
            .is_empty());
        assert!(node
            .step(Input::Message {
                from: 1,
                msg: RaftMessage::ReadProbe {
                    term: 99,
                    leader_id: 1,
                    context: 0
                }
            })
            .is_empty());
        assert_eq!(node.hard, hard);
        assert!(matches!(
            node.step(Input::Message {
                from: 1,
                msg: RaftMessage::ReadProbe {
                    term: 2,
                    leader_id: 1,
                    context: 10
                }
            })
            .as_slice(),
            [Effect::Send {
                msg: RaftMessage::ReadProbeReply {
                    accepted: false,
                    term: 3,
                    ..
                },
                ..
            }]
        ));
    }

    #[test]
    fn singleton_read_is_immediate_but_still_requires_committed_noop() {
        let mut node = leader(vec![], true);
        let effects = node.step(Input::ReadIndex { request_id: 1 });
        assert!(matches!(
            effects.as_slice(),
            [Effect::ReadReady {
                request_id: 1,
                context: 1,
                index: 1,
                term: 1
            }]
        ));
        assert!(node.pending_reads.is_empty());
    }

    // A separate witness counts only actual reply inputs for the expected
    // request context, never the implementation's pending voter set.
    fn verify_witness(
        expected_context: u64,
        replies: &[(NodeId, Term, u64)],
        completion: &Effect,
    ) -> Result<(), &'static str> {
        let Effect::ReadReady {
            request_id: 9,
            context,
            term: 1,
            index: 1,
        } = completion
        else {
            return Err("wrong-request-term-or-index");
        };
        if *context != expected_context {
            return Err("wrong-context");
        }
        let mut witnessed = BTreeSet::from([1]);
        for &(from, term, context) in replies {
            if [2, 3].contains(&from) && term == 1 && context == expected_context {
                witnessed.insert(from);
            }
        }
        if witnessed.len() < 2 {
            return Err("no-quorum");
        }
        Ok(())
    }

    #[test]
    fn executed_no_quorum_mutant_is_reached_and_rejected_by_independent_witness() {
        let mut normal = leader(vec![2, 3], true);
        let normal_effects = normal.step(Input::ReadIndex { request_id: 9 });
        assert!(!normal_effects
            .iter()
            .any(|e| matches!(e, Effect::ReadReady { .. })));
        assert_eq!(normal.read_fault_hits, 0);

        let mut mutant = leader(vec![2, 3], true);
        mutant.read_fault = ReadFaultMode::BypassQuorum;
        // Execute the actual ReadIndex admission/completion path with the
        // deliberate defect; this is not a forged completion appended later.
        let effects = mutant.step(Input::ReadIndex { request_id: 9 });
        assert_eq!(
            mutant.read_fault_hits, 1,
            "faulty protocol branch was reached"
        );
        let unsafe_success = effects
            .iter()
            .find(|e| matches!(e, Effect::ReadReady { .. }))
            .expect("mutant returned a false success");
        assert_eq!(verify_witness(1, &[], unsafe_success), Err("no-quorum"));
    }

    #[test]
    fn executed_wrong_context_mutant_is_reached_and_rejected_by_independent_witness() {
        for faulty in [false, true] {
            let mut node = leader(vec![2, 3], true);
            let c = context(&node.step(Input::ReadIndex { request_id: 9 }));
            if faulty {
                node.read_fault = ReadFaultMode::AcceptWrongContext;
            }
            let trace = [(2, 1, c + 1)];
            let effects = reply(&mut node, 2, 1, c + 1, true);
            if faulty {
                assert_eq!(
                    node.read_fault_hits, 1,
                    "mismatched-context branch was reached"
                );
                let unsafe_success = effects
                    .iter()
                    .find(|e| matches!(e, Effect::ReadReady { .. }))
                    .expect("mutant used unrelated callback");
                assert_eq!(
                    verify_witness(c, &trace, unsafe_success),
                    Err("wrong-context")
                );
            } else {
                assert!(effects.is_empty());
                assert_eq!(node.read_fault_hits, 0);
                let valid = reply(&mut node, 2, 1, c, true);
                assert_eq!(verify_witness(c, &[(2, 1, c)], &valid[0]), Ok(()));
            }
        }
    }

    #[test]
    fn overlapping_reads_complete_only_their_own_context_out_of_order() {
        let mut node = leader(vec![2, 3], true);
        let a = context(&node.step(Input::ReadIndex { request_id: 17 }));
        let b = context(&node.step(Input::ReadIndex { request_id: 18 }));
        assert_ne!(a, b);
        assert!(matches!(
            reply(&mut node, 2, 1, b, true).as_slice(),
            [Effect::ReadReady { request_id: 18, .. }]
        ));
        assert!(node.pending_reads.contains_key(&17));
        assert!(!node.pending_reads.contains_key(&18));
        assert!(matches!(
            reply(&mut node, 3, 1, a, true).as_slice(),
            [Effect::ReadReady { request_id: 17, .. }]
        ));
        assert!(node.pending_reads.is_empty());
    }

    #[test]
    fn context_reuse_in_new_term_cannot_consume_old_term_reply() {
        let mut node = leader(vec![2, 3], true);
        let old_context = context(&node.step(Input::ReadIndex { request_id: 1 }));
        reply(&mut node, 2, 2, old_context, false);
        assert!(node.pending_reads.is_empty());
        node.start_election(&mut Vec::new());
        node.on_request_vote_reply(2, 3, true, &mut Vec::new());
        node.on_append_entries_reply(2, 3, true, 2, 1, &mut Vec::new());
        assert_eq!(node.commit_index, 2);
        let new_context = context(&node.step(Input::ReadIndex { request_id: 2 }));
        assert_eq!(old_context, new_context, "wire identity is (term, context)");
        assert!(reply(&mut node, 2, 1, old_context, true).is_empty());
        assert!(matches!(
            reply(&mut node, 2, 3, new_context, true).as_slice(),
            [Effect::ReadReady {
                request_id: 2,
                term: 3,
                index: 2,
                ..
            }]
        ));
    }
}
