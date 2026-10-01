use super::*;
fn leader(peers: Vec<NodeId>) -> RaftNode {
    let mut node = RaftNode::new(1, peers.clone(), 91);
    node.hard.current_term = 5;
    node.role = Role::Leader {
        next_index: peers.iter().map(|id| (*id, 1)).collect(),
        match_index: peers.iter().map(|id| (*id, 0)).collect(),
    };
    node
}
fn commands(count: usize) -> Vec<Command> {
    (0..count)
        .map(|i| Command::Put {
            key: format!("k{i}"),
            value: format!("v{i}"),
        })
        .collect()
}

#[test]
fn batch_one_persist_then_ordered_individual_acceptance_before_singleton_applies() {
    let mut node = leader(vec![]);
    let commands = commands(3);
    let effects = node.step(Input::ClientProposeBatch {
        commands: commands.clone(),
    });
    assert_eq!(effects.len(), 7);
    assert!(
        matches!(&effects[0],Effect::PersistLogEntries{truncate_from:None,entries} if entries.len()==3)
    );
    for offset in 0..3 {
        assert_eq!(
            effects[offset + 1],
            Effect::ProposeAccepted {
                index: offset as u64 + 1
            }
        );
        assert_eq!(
            effects[offset + 4],
            Effect::Apply(Entry {
                index: offset as u64 + 1,
                term: 5,
                command: commands[offset].clone()
            })
        );
    }
    assert_eq!(node.commit_index, 3);
    assert_eq!(node.last_applied, 3);
}

#[test]
fn follower_oversized_empty_and_overflow_batches_never_partially_append() {
    let mut follower = RaftNode::new(1, vec![2, 3], 92);
    let rejected = follower.step(Input::ClientProposeBatch {
        commands: commands(3),
    });
    assert_eq!(rejected.len(), 3);
    assert!(rejected
        .iter()
        .all(|e| matches!(e, Effect::ProposeRejected { .. })));
    assert!(follower.log.is_empty());
    let mut node = leader(vec![2, 3]);
    let rejected = node.step(Input::ClientProposeBatch {
        commands: commands(MAX_PROPOSAL_BATCH + 1),
    });
    assert_eq!(rejected.len(), MAX_PROPOSAL_BATCH + 1);
    assert!(node.log.is_empty());
    assert!(node
        .step(Input::ClientProposeBatch { commands: vec![] })
        .is_empty());
    node.snapshot = Some(SnapshotDescriptor {
        metadata: SnapshotMetadata {
            membership: None,
            last_included_index: u64::MAX - 3,
            last_included_term: 5,
            members: vec![1, 2, 3],
        },
        total_len: 48,
        sha256: [1; 32],
    });
    let rejected = node.step(Input::ClientProposeBatch {
        commands: commands(3),
    });
    assert_eq!(rejected.len(), 3);
    assert!(rejected
        .iter()
        .all(|e| matches!(e, Effect::ProposeRejected { .. })));
    assert!(node.log.is_empty());
    let accepted = node.step(Input::ClientProposeBatch {
        commands: commands(2),
    });
    assert_eq!(node.log.len(), 2);
    assert!(matches!(accepted[2],Effect::ProposeAccepted{index} if index==u64::MAX-1));
}

#[test]
fn maximum_batch_preserves_duplicate_session_commands_for_committed_deduplication() {
    let mut node = leader(vec![2, 3]);
    let command = Command::SessionPut {
        session_id: 1,
        key: "k".into(),
        sequence: 1,
        value: "v".into(),
    };
    let effects = node.step(Input::ClientProposeBatch {
        commands: vec![command.clone(); MAX_PROPOSAL_BATCH],
    });
    assert_eq!(node.log.len(), MAX_PROPOSAL_BATCH);
    assert_eq!(node.commit_index, 0);
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::PersistLogEntries { .. }))
            .count(),
        1
    );
    assert_eq!(
        effects
            .iter()
            .filter(|e| matches!(e, Effect::ProposeAccepted { .. }))
            .count(),
        MAX_PROPOSAL_BATCH
    );
    assert!(node.log.iter().all(|entry| entry.command == command));
    assert!(!effects
        .iter()
        .any(|e| matches!(e, Effect::Send { .. } | Effect::Apply(_))));
}

#[test]
fn legacy_single_proposal_is_identical_to_one_element_batch() {
    let mut one = leader(vec![]);
    let mut batch = leader(vec![]);
    let command = commands(1).pop().unwrap();
    assert_eq!(
        one.step(Input::ClientPropose {
            command: command.clone()
        }),
        batch.step(Input::ClientProposeBatch {
            commands: vec![command]
        })
    );
    assert_eq!(one.log, batch.log);
    assert_eq!(one.commit_index, batch.commit_index);
}
