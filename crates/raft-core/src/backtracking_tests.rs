//! Rejection hints accelerate repair without becoming replication evidence.

use super::*;

fn entries(start: LogIndex, terms: &[Term]) -> Vec<Entry> {
    terms
        .iter()
        .enumerate()
        .map(|(offset, &term)| Entry {
            index: start + offset as u64,
            term,
            command: Command::NoOp,
        })
        .collect()
}

fn image(index: LogIndex, term: Term) -> SnapshotDescriptor {
    SnapshotDescriptor {
        metadata: SnapshotMetadata {
            membership: None,
            last_included_index: index,
            last_included_term: term,
            members: vec![1, 2, 3],
        },
        total_len: 48,
        sha256: [7; 32],
    }
}

fn leader() -> RaftNode {
    let mut node = RaftNode::new(1, vec![2, 3], 42);
    node.hard.current_term = 6;
    node.log = entries(1, &[1, 1, 2, 2, 4, 4]);
    node.commit_index = 1;
    node.last_applied = 1;
    node.role = Role::Leader {
        next_index: BTreeMap::from([(2, 7), (3, 7)]),
        match_index: BTreeMap::from([(2, 0), (3, 0)]),
    };
    node.contact_round = 17;
    node
}

fn compacted_node() -> RaftNode {
    RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        42,
        HardState {
            current_term: 6,
            voted_for: None,
            membership: None,
        },
        Some(image(5, 2)),
        entries(6, &[2, 4]),
    )
    .unwrap()
}

fn progress(node: &RaftNode) -> (LogIndex, LogIndex) {
    let Role::Leader {
        next_index,
        match_index,
    } = &node.role
    else {
        panic!("expected leader");
    };
    (next_index[&2], match_index[&2])
}

fn reply(
    term: Term,
    success: bool,
    matched: LogIndex,
    conflict: Option<AppendConflictHint>,
) -> RaftMessage {
    RaftMessage::AppendEntriesReply {
        term,
        success,
        match_index: matched,
        contact_round: 17,
        conflict,
    }
}

fn absent_term_hint() -> AppendConflictHint {
    AppendConflictHint {
        rejected_index: 6,
        term: Some(3),
        first_index: 3,
    }
}

fn append(prev_log_index: LogIndex, prev_log_term: Term) -> RaftMessage {
    RaftMessage::AppendEntries {
        term: 6,
        leader_id: 2,
        prev_log_index,
        prev_log_term,
        entries: vec![],
        leader_commit: 0,
        contact_round: 17,
    }
}

#[test]
fn follower_conflict_run_includes_its_retained_snapshot_boundary() {
    for anchor in [5, 6] {
        let mut node = compacted_node();
        let effects = node.step(Input::Message {
            from: 2,
            msg: append(anchor, 3),
        });
        assert_eq!(
            effects,
            vec![Effect::Send {
                to: 2,
                msg: reply(
                    6,
                    false,
                    7,
                    Some(AppendConflictHint {
                        rejected_index: anchor,
                        term: Some(2),
                        first_index: 5,
                    }),
                ),
            }]
        );
        assert_eq!(node.log, entries(6, &[2, 4]));
        assert_eq!((node.commit_index, node.last_applied), (5, 5));
    }
}

#[test]
fn leader_finds_conflict_term_only_in_its_snapshot_boundary() {
    let mut node = compacted_node();
    node.log = entries(6, &[4, 4]);
    node.role = Role::Leader {
        next_index: BTreeMap::from([(2, 8), (3, 8)]),
        match_index: BTreeMap::from([(2, 0), (3, 0)]),
    };
    node.contact_round = 17;
    let effects = node.step(Input::Message {
        from: 2,
        msg: reply(
            6,
            false,
            7,
            Some(AppendConflictHint {
                rejected_index: 7,
                term: Some(2),
                first_index: 3,
            }),
        ),
    });
    assert!(effects.is_empty());
    assert_eq!(progress(&node), (6, 0));
    assert_eq!(node.commit_index, 5);
    let mut sends = vec![];
    node.send_append_entries(&mut sends);
    assert!(sends.iter().any(|effect| matches!(
        effect,
        Effect::Send {
            to: 2,
            msg: RaftMessage::AppendEntries {
                prev_log_index: 5,
                prev_log_term: 2,
                ..
            },
        }
    )));
}

#[test]
fn short_follower_hint_can_cross_the_leaders_committed_snapshot_boundary() {
    let mut node = compacted_node();
    node.role = Role::Leader {
        next_index: BTreeMap::from([(2, 8), (3, 8)]),
        match_index: BTreeMap::from([(2, 0), (3, 0)]),
    };
    node.contact_round = 17;
    node.step(Input::Message {
        from: 2,
        msg: reply(
            6,
            false,
            2,
            Some(AppendConflictHint {
                rejected_index: 7,
                term: None,
                first_index: 3,
            }),
        ),
    });
    assert_eq!(progress(&node), (3, 0));
    assert_eq!((node.commit_index, node.last_applied), (5, 5));
    let mut sends = vec![];
    node.send_append_entries(&mut sends);
    assert!(sends.iter().any(|effect| matches!(
        effect,
        Effect::ReadSnapshotChunk {
            to: 2,
            descriptor,
            offset: 0,
            ..
        } if descriptor == &image(5, 2)
    )));
    assert!(!sends.iter().any(|effect| matches!(
        effect,
        Effect::Send {
            to: 2,
            msg: RaftMessage::AppendEntries { .. },
        }
    )));
}

#[test]
fn compacted_away_anchor_keeps_the_verified_boundary_protocol() {
    let mut node = compacted_node();
    let effects = node.step(Input::Message {
        from: 2,
        msg: append(4, 1),
    });
    assert_eq!(
        effects,
        vec![Effect::Send {
            to: 2,
            msg: RaftMessage::CompactedPrefix {
                term: 6,
                last_included_index: 5,
                last_included_term: 2,
                contact_round: 17,
            },
        }]
    );
}

#[test]
fn malformed_hints_change_neither_replication_nor_quorum_evidence() {
    let valid = absent_term_hint();
    let invalid = [
        (
            false,
            6,
            AppendConflictHint {
                first_index: 0,
                ..valid
            },
        ),
        (
            false,
            6,
            AppendConflictHint {
                first_index: 7,
                ..valid
            },
        ),
        (
            false,
            6,
            AppendConflictHint {
                term: Some(0),
                ..valid
            },
        ),
        (
            false,
            6,
            AppendConflictHint {
                term: Some(7),
                ..valid
            },
        ),
        (false, 5, valid), // A held anchor cannot exceed the follower's tail.
        (
            false,
            6,
            AppendConflictHint {
                term: Some(4),
                ..valid
            },
        ),
        (
            false,
            6,
            AppendConflictHint {
                term: None,
                ..valid
            },
        ),
        (
            false,
            LogIndex::MAX,
            AppendConflictHint {
                term: None,
                ..valid
            },
        ),
        (true, 6, valid),
    ];
    for (success, matched, hint) in invalid {
        let mut node = leader();
        let before = node.role.clone();
        let effects = node.step(Input::Message {
            from: 2,
            msg: reply(6, success, matched, Some(hint)),
        });
        assert!(effects.is_empty(), "hint={hint:?}");
        assert_eq!(node.role, before, "hint={hint:?}");
        assert_eq!(node.commit_index, 1, "hint={hint:?}");
        assert!(node.quorum_contacts.is_empty(), "hint={hint:?}");
    }
}

#[test]
fn duplicate_hint_and_rejection_after_success_cannot_undo_progress() {
    let mut node = leader();
    let rejection = reply(6, false, 6, Some(absent_term_hint()));
    node.step(Input::Message {
        from: 2,
        msg: rejection.clone(),
    });
    assert_eq!(progress(&node), (3, 0));
    assert!(node.quorum_contacts.contains(&2));
    node.quorum_contacts.clear();
    node.step(Input::Message {
        from: 2,
        msg: rejection.clone(),
    });
    assert_eq!(progress(&node), (3, 0));
    assert!(node.quorum_contacts.is_empty());

    node.step(Input::Message {
        from: 2,
        msg: reply(6, true, 6, None),
    });
    assert_eq!(progress(&node), (7, 6));
    node.quorum_contacts.clear();
    node.step(Input::Message {
        from: 2,
        msg: rejection,
    });
    assert_eq!(progress(&node), (7, 6));
    assert!(node.quorum_contacts.is_empty());
    // A legacy rejection lacks correlation but must respect known matches.
    node.step(Input::Message {
        from: 2,
        msg: reply(6, false, 0, None),
    });
    assert_eq!(progress(&node), (7, 6));
}

#[test]
fn hint_and_legacy_fallback_never_cross_a_confirmed_replication_floor() {
    for conflict in [Some(absent_term_hint()), None] {
        let mut node = leader();
        if let Role::Leader { match_index, .. } = &mut node.role {
            match_index.insert(2, 4);
        }
        let effects = node.step(Input::Message {
            from: 2,
            msg: reply(6, false, 6, conflict),
        });
        assert!(effects.is_empty());
        assert_eq!(progress(&node), (if conflict.is_some() { 5 } else { 6 }, 4));
        assert_eq!(node.commit_index, 1);
    }
}

#[test]
fn old_term_unknown_peer_and_follower_state_ignore_retry_hints() {
    for (from, term) in [(2, 5), (9, 6), (9, 7)] {
        let mut node = leader();
        let before = node.role.clone();
        let effects = node.step(Input::Message {
            from,
            msg: reply(term, false, 6, Some(absent_term_hint())),
        });
        assert!(effects.is_empty());
        assert_eq!(node.role, before);
        assert_eq!(node.hard.current_term, 6);
        assert!(node.quorum_contacts.is_empty());
    }
    let mut node = leader();
    node.role = Role::Follower;
    let effects = node.step(Input::Message {
        from: 2,
        msg: reply(6, false, 6, Some(absent_term_hint())),
    });
    assert!(effects.is_empty());
    assert_eq!(node.role, Role::Follower);
    assert!(node.quorum_contacts.is_empty());
}

#[test]
fn newer_term_reply_still_persists_term_before_stepping_down() {
    let mut node = leader();
    node.hard.voted_for = Some(1);
    let effects = node.step(Input::Message {
        from: 2,
        msg: reply(7, false, 6, Some(absent_term_hint())),
    });
    assert_eq!(node.hard.current_term, 7);
    assert_eq!(node.hard.voted_for, None);
    assert_eq!(node.role, Role::Follower);
    assert!(matches!(
        effects.first(),
        Some(Effect::PersistHardState(HardState {
            current_term: 7,
            voted_for: None,
            ..
        }))
    ));
    assert!(matches!(
        effects.get(1),
        Some(Effect::RoleChanged {
            role_name: "Follower",
            term: 7
        })
    ));
    assert!(node.quorum_contacts.is_empty());
}

#[test]
fn committed_prefix_conflict_does_not_become_a_backtracking_hint() {
    let mut node = leader();
    node.role = Role::Follower;
    node.commit_index = 3;
    node.last_applied = 3;
    let before = node.log.clone();
    let effects = node.step(Input::Message {
        from: 2,
        msg: RaftMessage::AppendEntries {
            term: 6,
            leader_id: 2,
            prev_log_index: 2,
            prev_log_term: 1,
            entries: entries(3, &[3]),
            leader_commit: 3,
            contact_round: 17,
        },
    });
    assert_eq!(node.log, before);
    assert_eq!((node.commit_index, node.last_applied), (3, 3));
    assert_eq!(
        effects,
        vec![Effect::Send {
            to: 2,
            msg: reply(6, false, 6, None)
        }]
    );
}

#[test]
fn legacy_reply_json_remains_compatible_and_empty_hints_are_omitted() {
    let legacy =
        r#"{"AppendEntriesReply":{"term":6,"success":false,"match_index":6,"contact_round":17}}"#;
    let decoded: RaftMessage = serde_json::from_str(legacy).unwrap();
    assert_eq!(decoded, reply(6, false, 6, None));
    assert_eq!(serde_json::to_string(&decoded).unwrap(), legacy);
    let old_without_round = r#"{"AppendEntriesReply":{"term":6,"success":false,"match_index":6}}"#;
    assert!(matches!(
        serde_json::from_str::<RaftMessage>(old_without_round).unwrap(),
        RaftMessage::AppendEntriesReply {
            conflict: None,
            contact_round: 0,
            ..
        }
    ));
    for hint in [
        absent_term_hint(),
        AppendConflictHint {
            rejected_index: 6,
            term: None,
            first_index: 3,
        },
    ] {
        let message = reply(6, false, 6, Some(hint));
        let encoded = serde_json::to_vec(&message).unwrap();
        assert_eq!(
            serde_json::from_slice::<RaftMessage>(&encoded).unwrap(),
            message
        );
    }
}
