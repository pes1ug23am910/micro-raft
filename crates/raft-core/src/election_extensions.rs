//! Counterexamples for pre-election correlation and fresh quorum windows.
use crate::*;

fn campaign(node: &mut RaftNode) -> (Term, CampaignId, Vec<Effect>) {
    let mut effects = Vec::new();
    node.start_pre_vote(&mut effects);
    let Role::PreCandidate {
        prospective_term,
        campaign_id,
        ..
    } = node.role
    else {
        panic!("expected pre-candidate");
    };
    (prospective_term, campaign_id, effects)
}

fn grant(
    node: &mut RaftNode,
    from: NodeId,
    prospective_term: Term,
    campaign_id: CampaignId,
) -> Vec<Effect> {
    node.step(Input::Message {
        from,
        msg: RaftMessage::PreVoteReply {
            term: node.hard.current_term,
            prospective_term,
            campaign_id,
            vote_granted: true,
        },
    })
}

fn leader(peers: Vec<NodeId>) -> RaftNode {
    let mut node = RaftNode::new(1, peers, 77);
    node.step(Input::Tick { now_ms: 10 });
    node.start_election(&mut Vec::new());
    for peer in node.peers.clone() {
        node.on_request_vote_reply(peer, node.hard.current_term, true, &mut Vec::new());
    }
    assert!(node.is_leader());
    node
}

fn contact(node: &mut RaftNode, from: NodeId, round: u64, success: bool, index: LogIndex) {
    node.step(Input::Message {
        from,
        msg: RaftMessage::AppendEntriesReply {
            conflict: None,
            term: node.hard.current_term,
            success,
            match_index: index,
            contact_round: round,
        },
    });
}

#[test]
fn precampaign_is_volatile_and_only_exact_distinct_members_can_win() {
    let mut node = RaftNode::new(1, vec![2, 3, 4, 5], 123);
    let (_, old, _) = campaign(&mut node);
    let (term, current, effects) = campaign(&mut node);
    assert_ne!(old, current);
    assert_eq!(node.hard, HardState::default());
    assert!(effects.iter().all(|e| !matches!(
        e,
        Effect::PersistHardState(_) | Effect::PersistLogEntries { .. }
    )));
    for (from, requested, id) in [
        (2, term, old),
        (2, term + 1, current),
        (
            2,
            term,
            CampaignId {
                incarnation: 124,
                ..current
            },
        ),
        (99, term, current),
        (1, term, current),
    ] {
        assert!(grant(&mut node, from, requested, id).is_empty());
    }
    grant(&mut node, 2, term, current);
    grant(&mut node, 2, term, current);
    assert!(matches!(node.role, Role::PreCandidate { .. }));
    let won = grant(&mut node, 3, term, current);
    assert!(matches!(node.role, Role::Candidate { .. }));
    assert_eq!(
        node.hard,
        HardState {
            membership: None,
            current_term: 1,
            voted_for: Some(1)
        }
    );
    assert!(matches!(won.first(), Some(Effect::PersistHardState(_))));
    assert!(won.iter().any(|e| matches!(
        e,
        Effect::Send {
            msg: RaftMessage::RequestVote { term: 1, .. },
            ..
        }
    )));
}

#[test]
fn restored_incarnation_rejects_delayed_prevote_grant() {
    let mut old = RaftNode::new(1, vec![2, 3], 123);
    let (term, old_id, _) = campaign(&mut old);
    let mut rebooted =
        RaftNode::restore(1, vec![2, 3], 124, old.hard.clone(), old.log.clone()).unwrap();
    let (_, new_id, _) = campaign(&mut rebooted);
    assert_eq!(old_id.sequence, new_id.sequence);
    assert!(grant(&mut rebooted, 2, term, old_id).is_empty());
    assert_eq!(rebooted.hard.current_term, 0);
    grant(&mut rebooted, 2, term, new_id);
    assert_eq!(rebooted.hard.current_term, 1);
}

#[test]
fn huge_prospective_term_does_not_adopt_term_vote_or_reset_timer() {
    let mut node = RaftNode::new(1, vec![2, 3], 5);
    node.step(Input::Tick { now_ms: 10 });
    let deadline = node.election_deadline();
    let effects = node.step(Input::Message {
        from: 2,
        msg: RaftMessage::PreVote {
            prospective_term: Term::MAX,
            campaign_id: CampaignId {
                incarnation: 8,
                sequence: 1,
            },
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
    });
    assert_eq!(node.hard, HardState::default());
    assert_eq!(node.election_deadline(), deadline);
    assert!(matches!(
        effects.as_slice(),
        [Effect::Send {
            msg: RaftMessage::PreVoteReply {
                term: 0,
                prospective_term: Term::MAX,
                vote_granted: true,
                ..
            },
            ..
        }]
    ));
}

#[test]
fn stale_log_prevote_and_recent_leader_contact_are_independent_guards() {
    let mut node = RaftNode::restore(
        1,
        vec![2, 3],
        5,
        HardState {
            membership: None,
            current_term: 3,
            voted_for: Some(3),
        },
        vec![Entry {
            index: 1,
            term: 3,
            command: Command::NoOp,
        }],
    )
    .unwrap();
    node.step(Input::Tick { now_ms: 10 });
    let ask = |index, term| Input::Message {
        from: 2,
        msg: RaftMessage::PreVote {
            prospective_term: 4,
            campaign_id: CampaignId {
                incarnation: 8,
                sequence: 1,
            },
            candidate_id: 2,
            last_log_index: index,
            last_log_term: term,
        },
    };
    let granted = |effects: Vec<Effect>| {
        matches!(
            effects.as_slice(),
            [Effect::Send {
                msg: RaftMessage::PreVoteReply {
                    vote_granted: true,
                    ..
                },
                ..
            }]
        )
    };
    assert!(!granted(node.step(ask(100, 2))));
    assert!(granted(node.step(ask(1, 3))));
    campaign(&mut node);
    node.step(Input::Message {
        from: 3,
        msg: RaftMessage::AppendEntries {
            term: 3,
            leader_id: 3,
            prev_log_index: 1,
            prev_log_term: 3,
            entries: vec![],
            leader_commit: 1,
            contact_round: 9,
        },
    });
    assert_eq!(node.role, Role::Follower);
    assert!(!granted(node.step(ask(1, 3))));
    node.step(Input::Tick { now_ms: 309 });
    assert!(!granted(node.step(ask(1, 3))));
    node.step(Input::Tick { now_ms: 310 });
    assert!(granted(node.step(ask(1, 3))));
    assert_eq!(node.hard.current_term, 3);
    assert_eq!(node.hard.voted_for, Some(3));
}

#[test]
fn only_known_matching_envelopes_can_change_hard_state() {
    let mut node = leader(vec![2, 3]);
    let hard = node.hard.clone();
    for (from, candidate_id) in [(99, 99), (2, 3), (1, 1)] {
        assert!(node
            .step(Input::Message {
                from,
                msg: RaftMessage::RequestVote {
                    term: 100,
                    candidate_id,
                    last_log_index: 0,
                    last_log_term: 0,
                }
            })
            .is_empty());
    }
    assert!(node
        .step(Input::Message {
            from: 2,
            msg: RaftMessage::AppendEntries {
                term: 100,
                leader_id: 3,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
                contact_round: 1,
            }
        })
        .is_empty());
    assert_eq!(node.hard, hard);
    assert!(node.is_leader());
}

#[test]
fn higher_actual_prevote_reply_is_durable_before_stepdown() {
    let mut node = leader(vec![2, 3]);
    let effects = node.step(Input::Message {
        from: 2,
        msg: RaftMessage::PreVoteReply {
            term: 7,
            prospective_term: Term::MAX,
            campaign_id: CampaignId {
                incarnation: 0,
                sequence: 0,
            },
            vote_granted: false,
        },
    });
    assert_eq!(node.hard.current_term, 7);
    assert_eq!(node.role, Role::Follower);
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::PersistHardState(HardState {
                current_term: 7,
                ..
            }),
            Effect::RoleChanged {
                role_name: "Follower",
                ..
            }
        ]
    ));
}

#[test]
fn old_future_unknown_duplicate_and_malformed_contacts_cannot_renew_leadership() {
    let mut node = leader(vec![2, 3, 4, 5]);
    let boundary = node.quorum_deadline_ms;
    let round = node.contact_round;
    contact(&mut node, 2, round, false, 0);
    contact(&mut node, 2, round, false, 0); // same peer twice
    contact(&mut node, 3, round - 1, true, 1);
    contact(&mut node, 3, round + 1, true, 1);
    contact(&mut node, 99, round, true, 1);
    contact(&mut node, 3, round, true, 999); // impossible success watermark
    let effects = node.step(Input::Tick { now_ms: boundary });
    assert_eq!(node.role, Role::Follower);
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::RoleChanged {
            role_name: "Follower",
            ..
        }
    )));
    assert!(matches!(
        node.step(Input::ClientPropose {
            command: Command::NoOp
        })
        .as_slice(),
        [Effect::ProposeRejected { .. }]
    ));
}

#[test]
fn fresh_consistency_rejections_count_but_cannot_be_replayed_next_window() {
    let mut node = leader(vec![2, 3]);
    let boundary = node.quorum_deadline_ms;
    let round = node.contact_round;
    contact(&mut node, 2, round, false, 0);
    node.step(Input::Tick { now_ms: boundary });
    assert_eq!(node.contact_round, round + 1);
    assert!(node.is_leader());
    for _ in 0..20 {
        contact(&mut node, 2, round, false, 0);
    }
    node.step(Input::Tick {
        now_ms: boundary + CHECK_QUORUM_MS,
    });
    assert_eq!(node.role, Role::Follower);
}

#[test]
fn malformed_append_never_echoes_fresh_quorum_challenge() {
    let mut follower = RaftNode::new(2, vec![1, 3], 3);
    let effects = follower.step(Input::Message {
        from: 1,
        msg: RaftMessage::AppendEntries {
            term: 99,
            leader_id: 1,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![Entry {
                index: 2,
                term: 99,
                command: Command::NoOp,
            }],
            leader_commit: 0,
            contact_round: 100,
        },
    });
    assert!(matches!(
        effects.as_slice(),
        [Effect::Send {
            msg: RaftMessage::AppendEntriesReply {
                contact_round: 0,
                success: false,
                ..
            },
            ..
        }]
    ));
    assert_eq!(follower.hard.current_term, 0);
}

#[test]
fn clock_regression_and_long_pause_cannot_extend_old_contacts() {
    let mut node = leader(vec![2, 3]);
    let boundary = node.quorum_deadline_ms;
    contact(&mut node, 2, 1, false, 0);
    assert!(node.step(Input::Tick { now_ms: 9 }).is_empty());
    assert_eq!(node.now_ms, 10);
    node.step(Input::Tick {
        now_ms: boundary + CHECK_QUORUM_MS,
    });
    assert!(!node.is_leader());
}

#[test]
fn term_campaign_and_quorum_counter_exhaustion_never_wraps() {
    for which in 0..2 {
        let mut node = RaftNode::new(1, vec![2, 3], 7);
        if which == 0 {
            node.hard.current_term = Term::MAX;
        } else {
            node.campaign_sequence = u64::MAX;
        }
        node.start_pre_vote(&mut Vec::new());
        assert!(node.campaign_exhausted);
        assert_eq!(node.role, Role::Follower);
        assert!(node.step(Input::Tick { now_ms: u64::MAX }).is_empty());
    }
    let mut node = leader(vec![2, 3]);
    node.contact_round = u64::MAX;
    contact(&mut node, 2, u64::MAX, false, 0);
    node.step(Input::Tick {
        now_ms: node.quorum_deadline_ms,
    });
    assert!(!node.is_leader());
}

#[test]
fn singleton_self_quorum_persists_before_noop_and_client_apply() {
    let mut node = RaftNode::new(1, vec![], 4);
    node.step(Input::Tick { now_ms: 1 });
    let effects = node.step(Input::Tick {
        now_ms: node.election_deadline(),
    });
    assert!(node.is_leader());
    assert_eq!(node.commit_index, 1);
    assert!(matches!(effects.first(), Some(Effect::PersistHardState(_))));
    assert!(matches!(
        effects.get(1),
        Some(Effect::PersistLogEntries { .. })
    ));
    assert!(matches!(
        effects.last(),
        Some(Effect::Apply(Entry { index: 1, .. }))
    ));
    let proposal = node.step(Input::ClientPropose {
        command: Command::Put {
            key: "a".into(),
            value: "b".into(),
        },
    });
    assert!(matches!(
        proposal.as_slice(),
        [
            Effect::PersistLogEntries { .. },
            Effect::ProposeAccepted { index: 2 },
            Effect::Apply(Entry { index: 2, .. })
        ]
    ));
    for _ in 0..3 {
        node.step(Input::Tick {
            now_ms: node.quorum_deadline_ms,
        });
        assert!(node.is_leader());
    }
}

#[test]
fn healthy_leader_refuses_prevote_without_touching_quorum_or_term() {
    let mut node = leader(vec![2, 3]);
    let deadline = node.quorum_deadline_ms;
    let hard = node.hard.clone();
    let effects = node.step(Input::Message {
        from: 2,
        msg: RaftMessage::PreVote {
            prospective_term: 9,
            campaign_id: CampaignId {
                incarnation: 999,
                sequence: 1,
            },
            candidate_id: 2,
            last_log_index: 1,
            last_log_term: hard.current_term,
        },
    });
    assert!(matches!(
        effects.as_slice(),
        [Effect::Send {
            msg: RaftMessage::PreVoteReply {
                vote_granted: false,
                ..
            },
            ..
        }]
    ));
    assert_eq!(node.hard, hard);
    assert_eq!(node.quorum_deadline_ms, deadline);
    assert!(node.is_leader());
}

#[test]
fn logical_time_exhaustion_relinquishes_authority() {
    let mut node = leader(vec![2, 3]);
    contact(&mut node, 2, 1, false, 0);
    node.step(Input::Tick { now_ms: u64::MAX });
    assert_eq!(node.role, Role::Follower);
    assert!(node.campaign_exhausted);
    assert!(node.step(Input::Tick { now_ms: u64::MAX }).is_empty());
}
