use crate::membership::{
    AdminOperation, CommittedMembership, ConfigurationEntry, ConfigurationPhase, MemberEndpoints,
    VoterConfig,
};
use crate::membership_protocol::MembershipOutcome;
use crate::*;
use std::collections::BTreeSet;

fn add(id: NodeId) -> AdminOperation {
    AdminOperation::AddLearner {
        id,
        endpoints: MemberEndpoints {
            raft: format!("node-{id}:7000"),
            http: format!("node-{id}:8000"),
        },
    }
}
fn config(index: u64, id: &str, op: AdminOperation, phase: ConfigurationPhase) -> Entry {
    Entry {
        index,
        term: 1,
        command: Command::Configuration(ConfigurationEntry {
            request_id: id.into(),
            operation: op,
            phase,
        }),
    }
}
fn history() -> (CommittedMembership, Vec<Entry>, CommittedMembership) {
    let genesis =
        CommittedMembership::bootstrap_with_group("group-a".into(), vec![1, 2, 3]).unwrap();
    let entries = vec![
        Entry {
            index: 1,
            term: 1,
            command: Command::NoOp,
        },
        config(2, "add-4", add(4), ConfigurationPhase::Apply),
        Entry {
            index: 3,
            term: 1,
            command: Command::NoOp,
        },
        config(
            4,
            "swap",
            AdminOperation::SetVoters {
                voters: vec![2, 3, 4],
            },
            ConfigurationPhase::Joint,
        ),
        config(
            5,
            "swap",
            AdminOperation::SetVoters {
                voters: vec![2, 3, 4],
            },
            ConfigurationPhase::Final,
        ),
    ];
    let mut state = genesis.clone();
    for entry in &entries {
        if let Command::Configuration(change) = &entry.command {
            state = state.advanced(entry.index, entry.term, change).unwrap();
        }
    }
    (genesis, entries, state)
}
fn image(membership: CommittedMembership) -> SnapshotDescriptor {
    SnapshotDescriptor {
        metadata: SnapshotMetadata {
            last_included_index: 5,
            last_included_term: 1,
            members: membership.state.participants().into_iter().collect(),
            membership: Some(membership),
        },
        total_len: 100,
        sha256: [1; 32],
    }
}
fn group(msg: RaftMessage) -> RaftMessage {
    RaftMessage::GroupMessage {
        group_id: "group-a".into(),
        genesis_voters: vec![1, 2, 3],
        message: Box::new(msg),
    }
}
fn leader() -> RaftNode {
    let mut node = RaftNode::new(1, vec![2, 3], 31);
    node.initialize_group("group-a".into(), vec![1, 2, 3])
        .unwrap();
    node.start_election(&mut Vec::new());
    node.on_request_vote_reply(2, 1, true, &mut Vec::new());
    node.on_append_entries_reply(2, 1, true, 1, node.contact_round, None, &mut Vec::new());
    assert_eq!(node.commit_index, 1);
    node
}
fn admin(node: &mut RaftNode, id: &str, operation: AdminOperation) -> Vec<Effect> {
    node.step(Input::MembershipChange {
        request_id: id.into(),
        operation,
    })
}
fn accepted(effects: &[Effect]) -> u64 {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::MembershipResult {
                outcome: MembershipOutcome::Accepted { index },
                ..
            } => Some(*index),
            _ => None,
        })
        .expect("admin accepted")
}

#[test]
fn selected_snapshot_publication_gap_requires_durable_refresh_before_any_input() {
    let (genesis, _, final_state) = history();
    let old_hard = HardState {
        current_term: 2,
        voted_for: Some(2),
        membership: Some(genesis),
    };
    let selected = image(final_state.clone());
    // Reached crash point: CURRENT selects the image; hard state is still old.
    let mut node = RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        5,
        old_hard.clone(),
        Some(selected.clone()),
        vec![],
    )
    .unwrap();
    assert!(node.recovery_required());
    assert!(node.step(Input::Tick { now_ms: 10_000 }).is_empty());
    assert!(node
        .step(Input::ClientPropose {
            command: Command::NoOp
        })
        .is_empty());
    assert_eq!(node.hard.current_term, 2);
    assert_eq!(node.hard.voted_for, Some(2));
    let refresh = node.step(Input::Recover);
    let [Effect::PersistHardState(refreshed)] = refresh.as_slice() else {
        panic!("{refresh:?}")
    };
    assert_eq!(refreshed.membership, Some(final_state));
    // Crash before executing refresh repeats the barrier; crash after it does not.
    assert!(RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        6,
        old_hard,
        Some(selected.clone()),
        vec![]
    )
    .unwrap()
    .recovery_required());
    let mut reopened = RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        7,
        refreshed.clone(),
        Some(selected),
        vec![],
    )
    .unwrap();
    assert!(!reopened.recovery_required());
    for now_ms in [1, 1_000, 10_000] {
        reopened.step(Input::Tick { now_ms });
    }
    assert_eq!(
        reopened.role,
        Role::Follower,
        "removed directory cannot campaign"
    );
    assert_eq!(reopened.hard.current_term, 2);
}

#[test]
fn durable_membership_requires_exact_selected_suffix_and_replays_it_before_service() {
    let (_, entries, final_state) = history();
    let hard = HardState {
        current_term: 2,
        voted_for: None,
        membership: Some(final_state),
    };
    assert!(RaftNode::restore(2, vec![1, 3], 1, hard.clone(), entries[..4].to_vec()).is_err());
    let mut conflicting = entries.clone();
    conflicting[1] = config(2, "other-add", add(4), ConfigurationPhase::Apply);
    assert!(RaftNode::restore(2, vec![1, 3], 1, hard.clone(), conflicting).is_err());
    let mut node = RaftNode::restore(2, vec![1, 3], 1, hard, entries.clone()).unwrap();
    assert_eq!(node.commit_index, 5);
    assert_eq!(node.last_applied, 0);
    assert!(node.recovery_required());
    let effects = node.step(Input::Recover);
    assert_eq!(
        effects,
        entries.into_iter().map(Effect::Apply).collect::<Vec<_>>()
    );
    assert!(!node.recovery_required());
}

#[test]
fn snapshot_history_contradiction_or_legacy_image_cannot_hide_durable_configuration() {
    let (_, _, final_state) = history();
    let mut other = final_state.clone();
    other.state.group_id = "other".into();
    let hard = HardState {
        current_term: 2,
        voted_for: None,
        membership: Some(other),
    };
    assert!(RaftNode::restore_with_snapshot(
        2,
        vec![1, 3],
        1,
        hard,
        Some(image(final_state.clone())),
        vec![]
    )
    .is_err());
    let mut legacy = image(final_state.clone());
    legacy.metadata.membership = None;
    legacy.metadata.members = vec![1, 2, 3];
    let hard = HardState {
        current_term: 2,
        voted_for: None,
        membership: Some(final_state),
    };
    assert!(RaftNode::restore_with_snapshot(2, vec![1, 3], 1, hard, Some(legacy), vec![]).is_err());
}

#[test]
fn uncommitted_final_restores_new_effective_quorum_and_conflict_rollback_restores_old_voters() {
    let (genesis, entries, _) = history();
    let mut node = RaftNode::restore(
        2,
        vec![1, 3],
        5,
        HardState {
            current_term: 1,
            voted_for: None,
            membership: Some(genesis),
        },
        entries,
    )
    .unwrap();
    assert_eq!(node.commit_index, 0);
    assert_eq!(
        node.voter_config(),
        &VoterConfig::Stable {
            voters: vec![2, 3, 4]
        }
    );
    assert!(!node.voter_config().has_quorum(&BTreeSet::from([1, 2])));
    let effects = node.step(Input::Message {
        from: 3,
        msg: group(RaftMessage::AppendEntries {
            term: 2,
            leader_id: 3,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![Entry {
                index: 1,
                term: 2,
                command: Command::NoOp,
            }],
            leader_commit: 1,
            contact_round: 1,
        }),
    });
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::PersistLogEntries {
            truncate_from: Some(1),
            ..
        }
    )));
    assert_eq!(
        node.voter_config(),
        &VoterConfig::Stable {
            voters: vec![1, 2, 3]
        }
    );
    assert_eq!(node.commit_index, 1);
}

#[test]
fn promotion_requires_both_quorums_for_joint_and_new_quorum_for_final() {
    let mut node = leader();
    let add_index = accepted(&admin(&mut node, "add-4", add(4)));
    node.on_append_entries_reply(
        2,
        1,
        true,
        add_index,
        node.contact_round,
        None,
        &mut Vec::new(),
    );
    assert!(node.effective_membership.learners.contains(&4));
    let op = AdminOperation::SetVoters {
        voters: vec![2, 3, 4],
    };
    assert!(
        matches!(admin(&mut node,"swap",op.clone()).last(),Some(Effect::MembershipResult {
        outcome:MembershipOutcome::Rejected { reason,.. },..
    }) if reason=="learner_not_caught_up")
    );
    node.on_append_entries_reply(
        4,
        1,
        true,
        add_index,
        node.contact_round,
        None,
        &mut Vec::new(),
    );
    let joint = accepted(&admin(&mut node, "swap", op.clone()));
    assert!(matches!(node.voter_config(), VoterConfig::Joint { .. }));
    let round = node.contact_round;
    node.on_append_entries_reply(2, 1, true, joint, round, None, &mut Vec::new());
    assert_eq!(
        node.commit_index, add_index,
        "old-only quorum cannot commit joint"
    );
    node.on_append_entries_reply(4, 1, true, joint, round, None, &mut Vec::new());
    assert_eq!(node.commit_index, joint);
    let mut effects = Vec::new();
    node.maybe_finalize_membership(&mut effects);
    let final_index = node.last_log_index();
    assert_eq!(final_index, joint + 1);
    node.on_append_entries_reply(2, 1, true, final_index, round, None, &mut Vec::new());
    assert_eq!(node.commit_index, joint, "old-only is not a new majority");
    node.on_append_entries_reply(4, 1, true, final_index, round, None, &mut effects);
    assert_eq!(node.commit_index, final_index);
    assert_eq!(
        node.role,
        Role::Follower,
        "removed leader steps down at committed final"
    );
    assert!(node.committed_membership().state.retired.contains(&1));
    assert_eq!(
        node.committed_membership().state.records["swap"].final_index,
        Some(final_index)
    );
}

#[test]
fn joint_read_requires_both_quorums_and_configuration_change_cancels_old_context() {
    let mut node = leader();
    let index = accepted(&admin(&mut node, "add-4", add(4)));
    node.on_append_entries_reply(2, 1, true, index, node.contact_round, None, &mut Vec::new());
    node.on_append_entries_reply(4, 1, true, index, node.contact_round, None, &mut Vec::new());
    node.step(Input::ReadIndex { request_id: 7 });
    let effects = admin(
        &mut node,
        "swap",
        AdminOperation::SetVoters {
            voters: vec![2, 3, 4],
        },
    );
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::ReadRejected { request_id: 7, .. })));
    node.step(Input::ReadIndex { request_id: 8 });
    let context = node.pending_reads[&8].context;
    let mut reply = Vec::new();
    node.on_read_probe_reply(2, 1, context, true, &mut reply);
    assert!(reply.is_empty());
    node.on_read_probe_reply(4, 1, context, true, &mut reply);
    assert!(matches!(
        reply.as_slice(),
        [Effect::ReadReady { request_id: 8, .. }]
    ));
}

#[test]
fn explicit_group_and_advertisement_checks_precede_any_term_or_quorum_change() {
    let (genesis, _, final_state) = history();
    let mut stale = RaftNode::new(1, vec![2, 3], 1);
    stale
        .initialize_group("group-a".into(), vec![1, 2, 3])
        .unwrap();
    let append = RaftMessage::AppendEntries {
        term: 10,
        leader_id: 4,
        prev_log_index: 0,
        prev_log_term: 0,
        entries: vec![],
        leader_commit: 0,
        contact_round: 1,
    };
    assert!(stale
        .step(Input::Message {
            from: 4,
            msg: group(append.clone())
        })
        .is_empty());
    let mut wrong = final_state.clone();
    wrong.state.group_id = "wrong".into();
    stale.step(Input::Message {
        from: 4,
        msg: group(RaftMessage::MembershipAdvertisement {
            term: 10,
            configuration: wrong,
        }),
    });
    assert!(stale.membership_advertisements.is_empty());
    stale.step(Input::Message {
        from: 1,
        msg: group(RaftMessage::MembershipAdvertisement {
            term: 10,
            configuration: final_state.clone(),
        }),
    });
    assert!(
        stale.membership_advertisements.is_empty(),
        "retired sender cannot advertise authority"
    );
    let route_hint = stale.step(Input::Message {
        from: 4,
        msg: group(RaftMessage::MembershipAdvertisement {
            term: 10,
            configuration: final_state,
        }),
    });
    assert!(
        matches!(route_hint.as_slice(), [Effect::MembershipRouteHint { id:4,endpoints, .. }]
        if endpoints.raft == "node-4:7000" && endpoints.http == "node-4:8000")
    );
    assert_eq!(
        stale.hard.current_term, 0,
        "advertisement grants no term authority"
    );
    assert_eq!(stale.committed_membership(), &genesis);
    assert!(!stale.is_voter(4));
    stale.step(Input::Message {
        from: 4,
        msg: group(append),
    });
    assert_eq!(
        stale.hard.current_term, 10,
        "validated advertised leader can enter replication path"
    );
    assert!(
        !stale.is_voter(4),
        "replication permission cannot install configuration"
    );
    let hard = stale.hard.clone();
    for msg in [
        RaftMessage::RequestVote {
            term: 99,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        },
        RaftMessage::GroupMessage {
            group_id: "wrong".into(),
            genesis_voters: vec![1, 2, 3],
            message: Box::new(RaftMessage::RequestVote {
                term: 99,
                candidate_id: 2,
                last_log_index: 0,
                last_log_term: 0,
            }),
        },
        group(group(RaftMessage::RequestVote {
            term: 99,
            candidate_id: 2,
            last_log_index: 0,
            last_log_term: 0,
        })),
    ] {
        assert!(stale.step(Input::Message { from: 2, msg }).is_empty());
        assert_eq!(stale.hard, hard);
    }
}

#[test]
fn exact_committed_admin_retry_is_cached_and_changed_payload_rejected() {
    let mut node = leader();
    let index = accepted(&admin(&mut node, "add-4", add(4)));
    let before = node.log.clone();
    assert_eq!(accepted(&admin(&mut node, "add-4", add(4))), index);
    assert_eq!(node.log, before);
    node.on_append_entries_reply(2, 1, true, index, node.contact_round, None, &mut Vec::new());
    let record = node.committed_membership().state.records["add-4"].clone();
    assert!(
        matches!(admin(&mut node,"add-4",add(4)).as_slice(),[Effect::MembershipResult {
        outcome:MembershipOutcome::Recorded { record:actual },..
    }] if *actual==record)
    );
    assert!(
        matches!(admin(&mut node,"add-4",add(5)).as_slice(),[Effect::MembershipResult {
        outcome:MembershipOutcome::Rejected { reason,.. },..
    }] if reason=="request_payload_changed")
    );
    assert_eq!(node.log, before);
}

fn five_actor_joint() -> (RaftNode, u64) {
    let mut node = leader();
    for id in [4, 5] {
        let index = accepted(&admin(&mut node, &format!("add-{id}"), add(id)));
        node.on_append_entries_reply(2, 1, true, index, node.contact_round, None, &mut Vec::new());
    }
    let fence = node.last_log_index();
    for id in [4, 5] {
        node.on_append_entries_reply(
            id,
            1,
            true,
            fence,
            node.contact_round,
            None,
            &mut Vec::new(),
        );
    }
    let joint = accepted(&admin(
        &mut node,
        "joint",
        AdminOperation::SetVoters {
            voters: vec![3, 4, 5],
        },
    ));
    (node, joint)
}

#[test]
fn executed_union_quorum_commit_mutant_is_reached_and_rejected_by_independent_witness() {
    for faulty in [false, true] {
        let (mut node, joint) = five_actor_joint();
        node.membership_union_fault = faulty;
        for id in [2, 4] {
            node.on_append_entries_reply(
                id,
                1,
                true,
                joint,
                node.contact_round,
                None,
                &mut Vec::new(),
            );
        }
        // This witness is deliberately a separate count over observed durable
        // reply identities; it never calls production quorum/replay helpers.
        let witnessed = BTreeSet::from([1, 2, 4]);
        let old_majority = [1, 2, 3].iter().filter(|id| witnessed.contains(id)).count() >= 2;
        let new_majority = [3, 4, 5].iter().filter(|id| witnessed.contains(id)).count() >= 2;
        if faulty {
            assert!(
                node.membership_fault_hits > 0,
                "defective commit branch reached"
            );
            assert_eq!(
                node.commit_index, joint,
                "mutant committed using union majority"
            );
            assert!(
                !(old_majority && new_majority),
                "independent witness rejects reached false commit"
            );
        } else {
            assert_eq!(node.membership_fault_hits, 0);
            assert!(node.commit_index < joint);
        }
    }
}

#[test]
fn joint_elections_and_check_quorum_require_each_separate_majority() {
    let (mut node, _) = five_actor_joint();
    node.start_election(&mut Vec::new());
    let term = node.hard.current_term;
    for id in [2, 4] {
        node.on_request_vote_reply(id, term, true, &mut Vec::new());
    }
    assert!(
        matches!(node.role, Role::Candidate { .. }),
        "union majority cannot elect"
    );
    node.on_request_vote_reply(5, term, true, &mut Vec::new());
    assert!(node.is_leader());
    node.quorum_contacts = BTreeSet::from([1, 2, 4]);
    let deadline = node.quorum_deadline_ms;
    node.step(Input::Tick { now_ms: deadline });
    assert_eq!(
        node.role,
        Role::Follower,
        "union majority cannot retain leadership"
    );
}

#[test]
fn engine_applied_watermark_preserves_later_membership_recovery_and_rejects_wrong_boundaries() {
    let (_, entries, final_state) = history();
    let hard = HardState {
        current_term: 2,
        voted_for: Some(2),
        membership: Some(final_state),
    };
    let mut node = RaftNode::restore(2, vec![1, 3], 1, hard, entries.clone()).unwrap();
    assert!(node.restore_applied_watermark(6, 1).is_err());
    assert!(node.restore_applied_watermark(3, 99).is_err());
    node.restore_applied_watermark(3, 1).unwrap();
    assert_eq!(node.commit_index, 5);
    assert_eq!(node.last_applied, 3);
    assert!(node.recovery_required());
    assert_eq!(
        node.step(Input::Recover),
        entries[3..]
            .iter()
            .cloned()
            .map(Effect::Apply)
            .collect::<Vec<_>>()
    );
    assert!(node.restore_applied_watermark(2, 1).is_err());
    node.step(Input::Tick { now_ms: 1 });
    assert!(node.restore_applied_watermark(5, 1).is_err());
}

#[test]
fn learner_vote_requires_group_bound_promoting_history_and_fresh_candidate_log() {
    let (genesis, entries, _) = history();
    let Command::Configuration(add) = &entries[1].command else {
        unreachable!()
    };
    let learner = genesis.advanced(2, 1, add).unwrap();
    let mut node = RaftNode::restore(
        4,
        vec![1, 2, 3],
        2,
        HardState {
            current_term: 1,
            voted_for: None,
            membership: Some(learner.clone()),
        },
        entries[..3].to_vec(),
    )
    .unwrap();
    node.step(Input::Recover);
    let request = RaftMessage::RequestVote {
        term: 2,
        candidate_id: 1,
        last_log_index: 4,
        last_log_term: 1,
    };
    assert!(node
        .step(Input::Message {
            from: 1,
            msg: group(request.clone())
        })
        .is_empty());
    let Command::Configuration(joint) = &entries[3].command else {
        unreachable!()
    };
    let promoted = learner.advanced(4, 1, joint).unwrap();
    node.step(Input::Message {
        from: 1,
        msg: group(RaftMessage::MembershipAdvertisement {
            term: 2,
            configuration: promoted,
        }),
    });
    assert_eq!(node.hard.current_term, 1);
    assert!(!node.is_voter(4));
    let stale = RaftMessage::RequestVote {
        term: 2,
        candidate_id: 1,
        last_log_index: 3,
        last_log_term: 1,
    };
    assert!(node
        .step(Input::Message {
            from: 1,
            msg: group(stale)
        })
        .is_empty());
    let effects = node.step(Input::Message {
        from: 1,
        msg: group(request),
    });
    assert!(matches!(effects.first(), Some(Effect::PersistHardState(_))));
    assert!(effects.iter().any(
        |effect| matches!(effect,Effect::Send { msg:RaftMessage::GroupMessage { message,.. },.. }
        if matches!(**message,RaftMessage::RequestVoteReply { vote_granted:true,.. }))
    ));
    assert_eq!(node.hard.voted_for, Some(1));
    assert!(
        !node.is_voter(4),
        "vote permission never installs candidate configuration"
    );
    assert_eq!(node.effective_membership.learners, BTreeSet::from([4]));
}

#[test]
fn promotion_fence_does_not_move_with_later_writes_and_old_contacts_do_not_qualify() {
    let mut node = leader();
    let added = accepted(&admin(&mut node, "add-4", add(4)));
    node.on_append_entries_reply(2, 1, true, added, node.contact_round, None, &mut Vec::new());
    let op = AdminOperation::SetVoters {
        voters: vec![1, 2, 3, 4],
    };
    admin(&mut node, "promote", op.clone());
    let fence = node.promotion_fence.as_ref().unwrap().2;
    node.step(Input::ClientPropose {
        command: Command::NoOp,
    });
    assert!(node.last_log_index() > fence);
    node.on_append_entries_reply(4, 1, true, fence, 0, None, &mut Vec::new());
    assert!(accepted_or_none(&admin(&mut node, "promote", op.clone())).is_none());
    node.on_append_entries_reply(4, 1, true, fence, node.contact_round, None, &mut Vec::new());
    assert!(accepted_or_none(&admin(&mut node, "promote", op)).is_some());
}
fn accepted_or_none(effects: &[Effect]) -> Option<u64> {
    effects.iter().find_map(|e| match e {
        Effect::MembershipResult {
            outcome: MembershipOutcome::Accepted { index },
            ..
        } => Some(*index),
        _ => None,
    })
}

#[test]
fn advertisement_retry_is_rate_bounded_and_stops_after_peer_has_configuration() {
    let mut node = leader();
    let index = accepted(&admin(&mut node, "add-4", add(4)));
    let mut effects = Vec::new();
    for _ in 0..100 {
        node.advertise_replication_membership(2, &mut effects);
    }
    assert_eq!(effects.len(), 1);
    node.now_ms = CHECK_QUORUM_MS;
    node.advertise_replication_membership(2, &mut effects);
    assert_eq!(effects.len(), 2);
    node.on_append_entries_reply(2, 1, true, index, node.contact_round, None, &mut Vec::new());
    node.now_ms += CHECK_QUORUM_MS;
    node.advertise_replication_membership(2, &mut effects);
    assert_eq!(effects.len(), 2);
}

#[test]
fn maximum_field_size_history_and_chunk_fit_real_eight_mib_frame_budget() {
    use crate::membership::{
        AdminRecord, MAX_ADMIN_ID_BYTES, MAX_ADMIN_RECORDS, MAX_MEMBERSHIP_JSON_BYTES,
        MAX_MEMBER_ENDPOINT_BYTES,
    };
    let ids: Vec<u8> = (0..=255).collect();
    let mut history =
        CommittedMembership::bootstrap_with_group("x".repeat(MAX_ADMIN_ID_BYTES), ids.clone())
            .unwrap();
    // A deliberately overfilled field-size fixture. Canonical histories have
    // fewer combinations, so this is a conservative encoding ceiling, not a
    // claim that this synthetic history is an admissible transition chain.
    history.index = u64::MAX;
    history.term = u64::MAX;
    for id in &ids {
        history.state.endpoints.insert(
            *id,
            MemberEndpoints {
                raft: "\\".repeat(MAX_MEMBER_ENDPOINT_BYTES),
                http: "\\".repeat(MAX_MEMBER_ENDPOINT_BYTES),
            },
        );
    }
    for id in 0..MAX_ADMIN_RECORDS {
        let prefix = format!("{id:04}");
        let key = prefix + &"\\".repeat(MAX_ADMIN_ID_BYTES - 4);
        history.state.records.insert(
            key,
            AdminRecord {
                operation: AdminOperation::SetVoters {
                    voters: ids.clone(),
                },
                first_index: u64::MAX,
                first_term: u64::MAX,
                joint: true,
                final_index: Some(u64::MAX),
                final_term: Some(u64::MAX),
            },
        );
    }
    let history_bytes = serde_json::to_vec(&history).unwrap().len();
    assert!(history_bytes < MAX_MEMBERSHIP_JSON_BYTES, "{history_bytes}");
    let advertised = group(RaftMessage::MembershipAdvertisement {
        term: u64::MAX,
        configuration: history.clone(),
    });
    assert!(serde_json::to_vec(&advertised).unwrap().len() < 8 * 1024 * 1024);
    let mut descriptor = image(history);
    descriptor.total_len = MAX_SNAPSHOT_CHUNK_BYTES as u64;
    let chunk = group(RaftMessage::InstallSnapshot {
        transfer: SnapshotTransferId {
            leader_id: 255,
            term: u64::MAX,
            incarnation: u64::MAX,
            sequence: u64::MAX,
        },
        descriptor,
        offset: 0,
        data: vec![255; MAX_SNAPSHOT_CHUNK_BYTES],
        done: true,
        contact_round: u64::MAX,
    });
    assert!(serde_json::to_vec(&chunk).unwrap().len() < 8 * 1024 * 1024);
}

#[test]
fn retired_learner_stale_and_contradictory_advertisements_cannot_authorize_messages() {
    let (genesis, entries, final_state) = history();
    let mut node = RaftNode::new(2, vec![1, 3], 11);
    node.initialize_group("group-a".into(), vec![1, 2, 3])
        .unwrap();
    let Command::Configuration(change) = &entries[1].command else {
        unreachable!()
    };
    let learner_state = genesis.advanced(2, 1, change).unwrap();
    for (from, configuration) in [(1, final_state), (4, learner_state.clone())] {
        assert!(node
            .step(Input::Message {
                from,
                msg: group(RaftMessage::MembershipAdvertisement {
                    term: 10,
                    configuration,
                })
            })
            .is_empty());
        assert!(node.membership_advertisements.is_empty());
        assert_eq!(node.hard.current_term, 0);
    }
    node.hard.current_term = 3;
    node.hard.membership = Some(learner_state);
    let conflict = genesis
        .advanced(
            2,
            1,
            &ConfigurationEntry {
                request_id: "different-history".into(),
                operation: add(5),
                phase: ConfigurationPhase::Apply,
            },
        )
        .unwrap();
    for (term, configuration) in [(2, node.committed_membership().clone()), (3, conflict)] {
        node.step(Input::Message {
            from: 1,
            msg: group(RaftMessage::MembershipAdvertisement {
                term,
                configuration,
            }),
        });
        assert!(node.membership_advertisements.is_empty());
        assert_eq!(node.hard.current_term, 3);
    }
}

#[test]
fn fresh_joiner_constructor_keeps_genesis_distinct_from_routes_and_local_identity() {
    let (mut node, effects) =
        RaftNode::new_for_group(4, vec![1, 2, 3], 7, "group-a".into()).unwrap();
    assert!(matches!(effects.as_slice(), [Effect::PersistHardState(_)]));
    assert_eq!(node.members(), vec![1, 2, 3]);
    assert_eq!(node.peers, vec![1, 2, 3]);
    assert!(!node.is_voter(4));
    assert!(
        !node.effective_membership.learners.contains(&4),
        "bootstrapping does not self-admit"
    );
    for now_ms in [0, 1_000, 10_000] {
        assert!(node.step(Input::Tick { now_ms }).is_empty());
    }
    assert_eq!(node.role, Role::Follower);
    assert_eq!(node.hard.current_term, 0);
    assert!(RaftNode::new_for_group(4, vec![3, 1, 2], 7, "group-a".into()).is_err());
    assert!(RaftNode::new_for_group(4, vec![1, 1, 2], 7, "group-a".into()).is_err());
}

#[test]
fn installed_snapshot_duplicate_after_new_configuration_never_rolls_membership_back() {
    let (genesis, entries, _) = history();
    let Command::Configuration(add_four) = &entries[1].command else {
        unreachable!()
    };
    let old = genesis.advanced(2, 1, add_four).unwrap();
    let selected = image(old.clone());
    let later = config(6, "add-5", add(5), ConfigurationPhase::Apply);
    let Command::Configuration(add_five) = &later.command else {
        unreachable!()
    };
    let current = old.advanced(6, 1, add_five).unwrap();
    let mut node = RaftNode::restore_with_snapshot(
        2,
        vec![1, 3],
        4,
        HardState {
            current_term: 2,
            voted_for: None,
            membership: Some(current.clone()),
        },
        Some(selected.clone()),
        vec![later],
    )
    .unwrap();
    node.step(Input::Recover);
    let transfer = SnapshotTransferId {
        leader_id: 1,
        term: 2,
        incarnation: 42,
        sequence: 1,
    };
    let effects = node.step(Input::Message {
        from: 1,
        msg: group(RaftMessage::InstallSnapshot {
            transfer,
            descriptor: selected.clone(),
            offset: 0,
            data: vec![0; 100],
            done: true,
            contact_round: 1,
        }),
    });
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::StageSnapshotChunk { .. })));
    let effects = node.step(Input::SnapshotChunkStaged {
        transfer,
        descriptor: selected,
        offset: 0,
        result: SnapshotStageResult::Accepted {
            next_offset: 100,
            complete: true,
        },
    });
    assert!(!effects.iter().any(|e| matches!(
        e,
        Effect::PublishSnapshot { .. } | Effect::ApplySnapshot { .. } | Effect::PersistHardState(_)
    )));
    assert_eq!(node.committed_membership(), &current);
    assert_eq!((node.commit_index, node.last_applied), (6, 6));
}
