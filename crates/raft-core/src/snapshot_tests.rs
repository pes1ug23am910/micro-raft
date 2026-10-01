use super::*;

fn entry(index: u64, term: u64) -> Entry {
    Entry {
        index,
        term,
        command: Command::NoOp,
    }
}
fn image(index: u64, term: u64, bytes: u64) -> SnapshotDescriptor {
    SnapshotDescriptor {
        metadata: SnapshotMetadata {
            membership: None,
            last_included_index: index,
            last_included_term: term,
            members: vec![1, 2, 3],
        },
        total_len: bytes,
        sha256: [7; 32],
    }
}
fn transfer(sequence: u64) -> SnapshotTransferId {
    SnapshotTransferId {
        leader_id: 2,
        term: 5,
        incarnation: 42,
        sequence,
    }
}
fn follower() -> RaftNode {
    let mut node = RaftNode::new(1, vec![2, 3], 19);
    node.hard.current_term = 5;
    node.log = vec![
        entry(1, 1),
        entry(2, 2),
        entry(3, 2),
        entry(4, 4),
        entry(5, 5),
    ];
    node.commit_index = 2;
    node.last_applied = 2;
    node
}
fn chunk(
    node: &mut RaftNode,
    id: SnapshotTransferId,
    descriptor: &SnapshotDescriptor,
    offset: u64,
) -> Vec<Effect> {
    let len = (descriptor.total_len - offset).min(MAX_SNAPSHOT_CHUNK_BYTES as u64);
    node.step(Input::Message {
        from: id.leader_id,
        msg: RaftMessage::InstallSnapshot {
            transfer: id,
            descriptor: descriptor.clone(),
            offset,
            data: vec![1; len as usize],
            done: offset + len == descriptor.total_len,
            contact_round: 9,
        },
    })
}
fn staged(
    node: &mut RaftNode,
    id: SnapshotTransferId,
    descriptor: &SnapshotDescriptor,
    offset: u64,
    next: u64,
    complete: bool,
) -> Vec<Effect> {
    node.step(Input::SnapshotChunkStaged {
        transfer: id,
        descriptor: descriptor.clone(),
        offset,
        result: SnapshotStageResult::Accepted {
            next_offset: next,
            complete,
        },
    })
}
fn publish(
    node: &mut RaftNode,
    id: Option<SnapshotTransferId>,
    descriptor: &SnapshotDescriptor,
) -> Vec<Effect> {
    node.step(Input::SnapshotPublished {
        transfer: id,
        descriptor: descriptor.clone(),
    })
}
fn accepted(effects: &[Effect], installed: bool) -> bool {
    effects.iter().any(|effect| {
        matches!(effect, Effect::Send { msg: RaftMessage::InstallSnapshotReply {
        accepted: true, installed: found, .. }, .. } if *found == installed)
    })
}

#[test]
fn snapshot_restore_validates_membership_terms_and_suffix_offsets() {
    let hard = HardState {
        membership: None,
        current_term: 5,
        voted_for: Some(2),
    };
    let descriptor = image(3, 2, 48);
    let node = RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        8,
        hard.clone(),
        Some(descriptor.clone()),
        vec![entry(4, 4), entry(5, 5)],
    )
    .unwrap();
    assert_eq!(
        (
            node.snapshot_index(),
            node.snapshot_term(),
            node.commit_index,
            node.last_applied
        ),
        (3, 2, 3, 3)
    );
    assert_eq!(node.log_term(2), None);
    assert_eq!(node.log_term(0), None);
    assert_eq!(node.log_term(3), Some(2));
    assert_eq!(node.entry_at(4), Some(&entry(4, 4)));
    assert_eq!(node.last_log_index(), 5);
    assert!(RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        8,
        hard.clone(),
        Some(descriptor.clone()),
        vec![entry(5, 4)]
    )
    .is_err());
    assert!(RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        8,
        hard.clone(),
        Some(descriptor.clone()),
        vec![entry(4, 1)]
    )
    .is_err());
    assert!(RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        8,
        HardState::default(),
        Some(descriptor.clone()),
        vec![]
    )
    .is_err());
    assert!(
        RaftNode::restore_with_snapshot(1, vec![2], 8, hard, Some(descriptor), vec![]).is_err()
    );
}

#[test]
fn malformed_chunk_never_adopts_term_or_stages_unbounded_bytes() {
    for kind in 0..6 {
        let mut node = follower();
        let mut descriptor = image(3, 2, 48);
        let mut id = transfer(1);
        id.term = 99;
        let mut offset = 0;
        let mut data = vec![1; 48];
        let mut done = true;
        match kind {
            0 => descriptor.metadata.members = vec![1, 2],
            1 => data.push(1),
            2 => offset = 1,
            3 => done = false,
            4 => descriptor.total_len = MAX_SNAPSHOT_BYTES as u64 + 1,
            _ => id.leader_id = 3,
        }
        let effects = node.step(Input::Message {
            from: 2,
            msg: RaftMessage::InstallSnapshot {
                transfer: id,
                descriptor,
                offset,
                data,
                done,
                contact_round: 1,
            },
        });
        assert!(effects.is_empty());
        assert_eq!(node.hard.current_term, 5);
    }
}

#[test]
fn local_compaction_is_two_phase_and_preserves_later_applied_suffix() {
    let mut node = follower();
    node.commit_index = 5;
    node.last_applied = 5;
    let descriptor = image(3, 2, 48);
    let effects = node.step(Input::Compact {
        descriptor: descriptor.clone(),
    });
    assert!(
        matches!(&effects[..],[Effect::PublishSnapshot{transfer:None,retained_entries,..}] if retained_entries==&vec![entry(4,4),entry(5,5)])
    );
    assert_eq!(node.snapshot_index(), 0);
    assert_eq!(node.log.len(), 5);
    assert!(publish(&mut node, None, &descriptor).is_empty());
    assert_eq!(node.snapshot_index(), 3);
    assert_eq!(node.last_applied, 5);
    assert_eq!(node.commit_index, 5);
    assert_eq!(node.log, vec![entry(4, 4), entry(5, 5)]);
    assert!(matches!(
        &node.step(Input::Compact { descriptor })[..],
        [Effect::SnapshotRejected { .. }]
    ));
}

#[test]
fn local_compaction_rejects_unapplied_wrong_term_and_busy_boundaries() {
    for descriptor in [image(3, 2, 48), image(2, 4, 48), image(0, 0, 48)] {
        assert!(matches!(
            &follower().step(Input::Compact { descriptor })[..],
            [Effect::SnapshotRejected { .. }]
        ));
    }
    let mut node = follower();
    let descriptor = image(3, 2, 48);
    chunk(&mut node, transfer(1), &descriptor, 0);
    assert!(matches!(
        &node.step(Input::Compact {
            descriptor: image(2, 2, 48)
        })[..],
        [Effect::SnapshotRejected {
            reason: "snapshot_busy"
        }]
    ));
}

#[test]
fn final_ack_waits_for_publication_and_application_install_matching_suffix() {
    let mut node = follower();
    let descriptor = image(3, 2, 48);
    let id = transfer(1);
    let receipt = chunk(&mut node, id, &descriptor, 0);
    assert!(!accepted(&receipt, true));
    assert!(receipt
        .iter()
        .any(|e| matches!(e, Effect::StageSnapshotChunk { .. })));
    let complete = staged(&mut node, id, &descriptor, 0, 48, true);
    assert!(
        matches!(&complete[..],[Effect::PublishSnapshot{retained_entries,..}] if retained_entries==&vec![entry(4,4),entry(5,5)])
    );
    assert_eq!(node.snapshot_index(), 0);
    assert!(!accepted(&complete, true));
    let published = publish(&mut node, Some(id), &descriptor);
    assert!(matches!(
        &published[..],
        [
            Effect::ApplySnapshot { .. },
            Effect::Send {
                msg: RaftMessage::InstallSnapshotReply {
                    installed: true,
                    accepted: true,
                    match_index: 3,
                    ..
                },
                ..
            }
        ]
    ));
    assert_eq!(node.last_applied, 3);
    assert_eq!(node.commit_index, 3);
    assert_eq!(node.log, vec![entry(4, 4), entry(5, 5)]);
}

#[test]
fn conflicting_uncommitted_suffix_is_discarded_but_committed_floor_is_protected() {
    let mut node = follower();
    let descriptor = image(3, 3, 48);
    let id = transfer(1);
    chunk(&mut node, id, &descriptor, 0);
    assert!(
        matches!(&staged(&mut node,id,&descriptor,0,48,true)[..],[Effect::PublishSnapshot{retained_entries,..}] if retained_entries.is_empty())
    );
    publish(&mut node, Some(id), &descriptor);
    assert!(node.log.is_empty());
    assert_eq!(node.last_log_index(), 3);
    let mut node = follower();
    node.commit_index = 4;
    node.last_applied = 4;
    assert!(!chunk(&mut node, id, &descriptor, 0)
        .iter()
        .any(|e| matches!(e, Effect::StageSnapshotChunk { .. })));
}

#[test]
fn application_advancement_during_staging_cancels_obsolete_image() {
    let mut node = follower();
    let descriptor = image(3, 2, 48);
    let id = transfer(1);
    chunk(&mut node, id, &descriptor, 0);
    node.commit_index = 4;
    node.last_applied = 4;
    let effects = staged(&mut node, id, &descriptor, 0, 48, true);
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::CancelSnapshot { .. })));
    assert!(!effects
        .iter()
        .any(|e| matches!(e, Effect::PublishSnapshot { .. })));
    assert_eq!(node.last_applied, 4);
}

#[test]
fn duplicate_chunks_are_rechecked_by_shell_and_gaps_never_stage() {
    let mut node = follower();
    let descriptor = image(3, 2, MAX_SNAPSHOT_CHUNK_BYTES as u64 * 3);
    let id = transfer(1);
    chunk(&mut node, id, &descriptor, 0);
    let first = staged(
        &mut node,
        id,
        &descriptor,
        0,
        MAX_SNAPSHOT_CHUNK_BYTES as u64,
        false,
    );
    assert!(accepted(&first, false));
    let gap = chunk(
        &mut node,
        id,
        &descriptor,
        MAX_SNAPSHOT_CHUNK_BYTES as u64 * 2,
    );
    assert!(!gap
        .iter()
        .any(|e| matches!(e, Effect::StageSnapshotChunk { .. })));
    let duplicate = chunk(&mut node, id, &descriptor, 0);
    assert!(duplicate
        .iter()
        .any(|e| matches!(e, Effect::StageSnapshotChunk { offset: 0, .. })));
    let changed = node.step(Input::SnapshotChunkStaged {
        transfer: id,
        descriptor: descriptor.clone(),
        offset: 0,
        result: SnapshotStageResult::Rejected,
    });
    assert!(!accepted(&changed, false));
    assert!(!accepted(&changed, true));
    assert_eq!(node.snapshot_index(), 0);
    chunk(&mut node, id, &descriptor, MAX_SNAPSHOT_CHUNK_BYTES as u64);
    assert!(accepted(
        &staged(
            &mut node,
            id,
            &descriptor,
            MAX_SNAPSHOT_CHUNK_BYTES as u64,
            MAX_SNAPSHOT_CHUNK_BYTES as u64 * 2,
            false
        ),
        false
    ));
}

#[test]
fn supersession_term_change_and_campaign_cancel_staging_and_late_callbacks() {
    let descriptor = image(3, 2, 48);
    let mut node = follower();
    chunk(&mut node, transfer(1), &descriptor, 0);
    let effects = chunk(&mut node, transfer(2), &descriptor, 0);
    assert!(effects
        .iter()
        .any(|e| matches!(e,Effect::CancelSnapshot{transfer:t} if *t==transfer(1))));
    assert!(staged(&mut node, transfer(1), &descriptor, 0, 48, true).is_empty());
    assert!(!chunk(&mut node, transfer(1), &descriptor, 0)
        .iter()
        .any(|e| matches!(e, Effect::StageSnapshotChunk { .. })));
    let newer = node.step(Input::Message {
        from: 3,
        msg: RaftMessage::RequestVoteReply {
            term: 6,
            vote_granted: false,
        },
    });
    assert!(matches!(newer.first(), Some(Effect::PersistHardState(_))));
    assert!(newer
        .iter()
        .any(|e| matches!(e, Effect::CancelSnapshot { .. })));
    assert!(staged(&mut node, transfer(2), &descriptor, 0, 48, true).is_empty());
    let mut node = follower();
    chunk(&mut node, transfer(1), &descriptor, 0);
    node.step(Input::Tick { now_ms: 0 });
    let deadline = node.election_deadline();
    assert!(node
        .step(Input::Tick { now_ms: deadline })
        .iter()
        .any(|e| matches!(e, Effect::CancelSnapshot { .. })));
}

#[test]
fn same_transfer_cannot_change_descriptor_or_incarnation() {
    let mut node = follower();
    let descriptor = image(3, 2, 48);
    chunk(&mut node, transfer(1), &descriptor, 0);
    let mut changed = descriptor.clone();
    changed.sha256 = [9; 32];
    assert!(!chunk(&mut node, transfer(1), &changed, 0)
        .iter()
        .any(|e| matches!(e, Effect::StageSnapshotChunk { .. })));
    let mut other = transfer(2);
    other.incarnation += 1;
    assert!(!chunk(&mut node, other, &descriptor, 0)
        .iter()
        .any(|e| matches!(e, Effect::StageSnapshotChunk { .. })));
}

#[test]
fn invalid_stage_callback_cannot_publish_or_move_progress() {
    for (next, complete) in [(49, true), (48, false), (0, false)] {
        let mut node = follower();
        let descriptor = image(3, 2, 48);
        chunk(&mut node, transfer(1), &descriptor, 0);
        let effects = staged(&mut node, transfer(1), &descriptor, 0, next, complete);
        assert!(!effects
            .iter()
            .any(|e| matches!(e, Effect::PublishSnapshot { .. })));
        assert_eq!(node.snapshot_index(), 0);
    }
}

#[test]
fn final_duplicate_is_verified_and_acknowledged_without_rollback_or_republication() {
    let descriptor = image(3, 2, 48);
    let mut node = follower();
    let id = transfer(1);
    chunk(&mut node, id, &descriptor, 0);
    staged(&mut node, id, &descriptor, 0, 48, true);
    publish(&mut node, Some(id), &descriptor);
    node.commit_index = 5;
    node.last_applied = 5;
    chunk(&mut node, id, &descriptor, 0);
    let duplicate = staged(&mut node, id, &descriptor, 0, 48, true);
    assert!(accepted(&duplicate, true));
    assert_eq!(node.last_applied, 5);
    assert!(!duplicate.iter().any(|e| matches!(
        e,
        Effect::ApplySnapshot { .. } | Effect::PublishSnapshot { .. }
    )));
}

fn compacted_leader(bytes: u64) -> RaftNode {
    let mut node = RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        123,
        HardState {
            membership: None,
            current_term: 5,
            voted_for: Some(1),
        },
        Some(image(3, 2, bytes)),
        vec![entry(4, 5)],
    )
    .unwrap();
    node.role = Role::Leader {
        next_index: BTreeMap::from([(2, 1), (3, 4)]),
        match_index: BTreeMap::from([(2, 0), (3, 3)]),
    };
    node.contact_round = 4;
    node.quorum_contacts = BTreeSet::from([1]);
    node
}

#[test]
fn leader_chunks_are_bounded_correlated_and_delayed_replies_do_not_renew_quorum() {
    let mut node = compacted_leader(MAX_SNAPSHOT_CHUNK_BYTES as u64 + 48);
    let mut effects = Vec::new();
    node.send_append_entries(&mut effects);
    let (id, descriptor) = effects
        .iter()
        .find_map(|e| match e {
            Effect::ReadSnapshotChunk {
                transfer,
                descriptor,
                offset: 0,
                max_len: MAX_SNAPSHOT_CHUNK_BYTES,
                to: 2,
            } => Some((*transfer, descriptor.clone())),
            _ => None,
        })
        .unwrap();
    assert!(effects.iter().any(|e|matches!(e,Effect::Send{to:3,msg:RaftMessage::AppendEntries{prev_log_index:3,prev_log_term:2,entries,..}} if entries==&vec![entry(4,5)])));
    let send = node.step(Input::SnapshotChunkRead {
        to: 2,
        transfer: id,
        descriptor: descriptor.clone(),
        offset: 0,
        data: vec![1; MAX_SNAPSHOT_CHUNK_BYTES],
    });
    assert!(matches!(
        &send[..],
        [Effect::Send {
            msg: RaftMessage::InstallSnapshot {
                done: false,
                contact_round: 4,
                ..
            },
            ..
        }]
    ));
    let reply = RaftMessage::InstallSnapshotReply {
        term: 5,
        transfer: id,
        next_offset: MAX_SNAPSHOT_CHUNK_BYTES as u64,
        match_index: 0,
        accepted: true,
        installed: false,
        contact_round: 3,
    };
    let progress = node.step(Input::Message {
        from: 2,
        msg: reply.clone(),
    });
    assert!(
        matches!(&progress[..],[Effect::ReadSnapshotChunk{offset,..}] if *offset==MAX_SNAPSHOT_CHUNK_BYTES as u64)
    );
    assert!(!node.quorum_contacts.contains(&2));
    assert!(node
        .step(Input::Message {
            from: 2,
            msg: reply
        })
        .is_empty());
    node.step(Input::SnapshotChunkRead {
        to: 2,
        transfer: id,
        descriptor: descriptor.clone(),
        offset: MAX_SNAPSHOT_CHUNK_BYTES as u64,
        data: vec![1; 48],
    });
    let final_reply = node.step(Input::Message {
        from: 2,
        msg: RaftMessage::InstallSnapshotReply {
            term: 5,
            transfer: id,
            next_offset: descriptor.total_len,
            match_index: 3,
            accepted: true,
            installed: true,
            contact_round: 4,
        },
    });
    assert!(node.quorum_contacts.contains(&2));
    assert!(node.outgoing_snapshots.is_empty());
    assert!(final_reply.iter().any(|e| matches!(
        e,
        Effect::Send {
            to: 2,
            msg: RaftMessage::AppendEntries {
                prev_log_index: 3,
                ..
            }
        }
    )));
}

#[test]
fn stepdown_discards_outgoing_chunk_callbacks_and_term_scoped_transfer_replies() {
    let mut node = compacted_leader(48);
    let mut effects = Vec::new();
    node.send_snapshot_chunk(2, &mut effects);
    let Effect::ReadSnapshotChunk {
        transfer,
        descriptor,
        ..
    } = effects.pop().unwrap()
    else {
        panic!()
    };
    node.step(Input::Message {
        from: 3,
        msg: RaftMessage::RequestVoteReply {
            term: 6,
            vote_granted: false,
        },
    });
    assert!(node
        .step(Input::SnapshotChunkRead {
            to: 2,
            transfer,
            descriptor: descriptor.clone(),
            offset: 0,
            data: vec![1; 48]
        })
        .is_empty());
    assert!(node
        .step(Input::Message {
            from: 2,
            msg: RaftMessage::InstallSnapshotReply {
                term: 5,
                transfer,
                next_offset: 48,
                match_index: 3,
                accepted: true,
                installed: true,
                contact_round: 4
            }
        })
        .is_empty());
    assert_eq!(node.snapshot_index(), 3);
}

#[test]
fn compacted_follower_hint_requires_exact_local_boundary_term() {
    let mut follower = compacted_leader(48);
    follower.role = Role::Follower;
    let response = follower.step(Input::Message {
        from: 2,
        msg: RaftMessage::AppendEntries {
            term: 5,
            leader_id: 2,
            prev_log_index: 1,
            prev_log_term: 1,
            entries: vec![],
            leader_commit: 1,
            contact_round: 7,
        },
    });
    assert!(matches!(
        &response[..],
        [Effect::Send {
            msg: RaftMessage::CompactedPrefix {
                last_included_index: 3,
                last_included_term: 2,
                ..
            },
            ..
        }]
    ));
    let mut leader = RaftNode::new(2, vec![1, 3], 7);
    leader.hard.current_term = 5;
    leader.log = vec![entry(1, 1), entry(2, 2), entry(3, 2), entry(4, 5)];
    leader.role = Role::Leader {
        next_index: BTreeMap::from([(1, 2), (3, 5)]),
        match_index: BTreeMap::from([(1, 0), (3, 0)]),
    };
    leader.step(Input::Message {
        from: 1,
        msg: RaftMessage::CompactedPrefix {
            term: 5,
            last_included_index: 3,
            last_included_term: 4,
            contact_round: 0,
        },
    });
    assert!(matches!(&leader.role,Role::Leader{next_index,..} if next_index[&1]==2));
    leader.step(Input::Message {
        from: 1,
        msg: RaftMessage::CompactedPrefix {
            term: 5,
            last_included_index: 3,
            last_included_term: 2,
            contact_round: 0,
        },
    });
    assert!(
        matches!(&leader.role,Role::Leader{next_index,match_index} if next_index[&1]==4 && match_index[&1]==3)
    );
}

#[test]
fn compacted_suffix_commit_and_application_use_absolute_indices() {
    let mut node = compacted_leader(48);
    let effects = node.step(Input::Message {
        from: 2,
        msg: RaftMessage::AppendEntriesReply {
            conflict: None,
            term: 5,
            success: true,
            match_index: 4,
            contact_round: 4,
        },
    });
    assert_eq!(node.commit_index, 4);
    assert_eq!(node.last_applied, 4);
    assert!(effects
        .iter()
        .any(|e| matches!(e,Effect::Apply(entry) if entry.index==4)));
    assert_eq!(
        commit_advance(3, 5, &[entry(4, 5), entry(5, 5)], &[5, 5, 0]),
        Some(5)
    );
}

#[test]
fn receiver_restart_rejection_starts_new_transfer_at_zero() {
    let mut node = compacted_leader(MAX_SNAPSHOT_CHUNK_BYTES as u64 + 48);
    let mut effects = Vec::new();
    node.send_snapshot_chunk(2, &mut effects);
    let Effect::ReadSnapshotChunk {
        transfer,
        descriptor,
        ..
    } = effects.pop().unwrap()
    else {
        panic!()
    };
    node.step(Input::SnapshotChunkRead {
        to: 2,
        transfer,
        descriptor: descriptor.clone(),
        offset: 0,
        data: vec![1; MAX_SNAPSHOT_CHUNK_BYTES],
    });
    node.step(Input::Message {
        from: 2,
        msg: RaftMessage::InstallSnapshotReply {
            term: 5,
            transfer,
            next_offset: MAX_SNAPSHOT_CHUNK_BYTES as u64,
            match_index: 0,
            accepted: true,
            installed: false,
            contact_round: 4,
        },
    });
    node.step(Input::SnapshotChunkRead {
        to: 2,
        transfer,
        descriptor: descriptor.clone(),
        offset: MAX_SNAPSHOT_CHUNK_BYTES as u64,
        data: vec![1; 48],
    });
    node.step(Input::Message {
        from: 2,
        msg: RaftMessage::InstallSnapshotReply {
            term: 5,
            transfer,
            next_offset: 0,
            match_index: 0,
            accepted: false,
            installed: false,
            contact_round: 0,
        },
    });
    let mut effects = Vec::new();
    node.send_snapshot_chunk(2, &mut effects);
    assert!(
        matches!(&effects[..],[Effect::ReadSnapshotChunk{offset:0,transfer:new,..}] if new.sequence>transfer.sequence)
    );
}

#[test]
fn compacted_log_index_exhaustion_never_wraps_or_appends_max_index() {
    let mut descriptor = image(u64::MAX - 1, 5, 48);
    descriptor.metadata.members = vec![1];
    let mut node = RaftNode::restore_with_snapshot(
        1,
        vec![],
        9,
        HardState {
            membership: None,
            current_term: 5,
            voted_for: None,
        },
        Some(descriptor),
        vec![],
    )
    .unwrap();
    node.step(Input::Tick { now_ms: 0 });
    let deadline = node.election_deadline();
    let effects = node.step(Input::Tick { now_ms: deadline });
    assert!(!node.is_leader());
    assert!(node.campaign_exhausted);
    assert!(node.log.is_empty());
    assert!(!effects
        .iter()
        .any(|e| matches!(e, Effect::PersistLogEntries { .. })));
}

#[test]
fn newer_sequence_cannot_replace_stage_with_an_older_boundary_in_same_term() {
    let mut node = follower();
    let newer = image(4, 4, 48);
    let older = image(3, 2, 48);
    chunk(&mut node, transfer(1), &newer, 0);
    let effects = chunk(&mut node, transfer(2), &older, 0);
    assert!(!effects.iter().any(|e| matches!(
        e,
        Effect::StageSnapshotChunk { .. } | Effect::CancelSnapshot { .. }
    )));
    assert!(staged(&mut node, transfer(1), &newer, 0, 48, true)
        .iter()
        .any(|e| matches!(e, Effect::PublishSnapshot { .. })));
}

#[test]
fn stage_receipt_cannot_skip_unreceived_chunks() {
    let mut node = follower();
    let descriptor = image(3, 2, MAX_SNAPSHOT_CHUNK_BYTES as u64 * 3);
    let id = transfer(1);
    chunk(&mut node, id, &descriptor, 0);
    let effects = staged(
        &mut node,
        id,
        &descriptor,
        0,
        MAX_SNAPSHOT_CHUNK_BYTES as u64 * 2,
        false,
    );
    assert!(!accepted(&effects, false));
    assert!(effects
        .iter()
        .any(|e| matches!(e, Effect::CancelSnapshot { .. })));
}

#[test]
fn new_transfer_of_installed_image_finishes_from_first_chunk_without_reapply() {
    let descriptor = image(3, 2, MAX_SNAPSHOT_CHUNK_BYTES as u64 + 48);
    let id = transfer(1);
    let mut node = follower();
    chunk(&mut node, id, &descriptor, 0);
    staged(
        &mut node,
        id,
        &descriptor,
        0,
        MAX_SNAPSHOT_CHUNK_BYTES as u64,
        false,
    );
    chunk(&mut node, id, &descriptor, MAX_SNAPSHOT_CHUNK_BYTES as u64);
    staged(
        &mut node,
        id,
        &descriptor,
        MAX_SNAPSHOT_CHUNK_BYTES as u64,
        descriptor.total_len,
        true,
    );
    publish(&mut node, Some(id), &descriptor);
    node.commit_index = 5;
    node.last_applied = 5;
    let newer = transfer(2);
    chunk(&mut node, newer, &descriptor, 0);
    let effects = staged(&mut node, newer, &descriptor, 0, descriptor.total_len, true);
    assert!(accepted(&effects, true));
    assert_eq!(node.last_applied, 5);
    assert!(!effects.iter().any(|e| matches!(
        e,
        Effect::PublishSnapshot { .. } | Effect::ApplySnapshot { .. }
    )));
    let mut leader = compacted_leader(descriptor.total_len);
    let mut outgoing = Vec::new();
    leader.send_snapshot_chunk(2, &mut outgoing);
    let Effect::ReadSnapshotChunk {
        transfer,
        descriptor,
        ..
    } = outgoing.pop().unwrap()
    else {
        panic!()
    };
    leader.step(Input::SnapshotChunkRead {
        to: 2,
        transfer,
        descriptor: descriptor.clone(),
        offset: 0,
        data: vec![1; MAX_SNAPSHOT_CHUNK_BYTES],
    });
    let effects = leader.step(Input::Message {
        from: 2,
        msg: RaftMessage::InstallSnapshotReply {
            term: 5,
            transfer,
            next_offset: descriptor.total_len,
            match_index: 3,
            accepted: true,
            installed: true,
            contact_round: 4,
        },
    });
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::Send {
            to: 2,
            msg: RaftMessage::AppendEntries {
                prev_log_index: 3,
                ..
            }
        }
    )));
}

#[test]
fn retained_suffix_replays_after_snapshot_without_skipping_application() {
    let mut node = follower();
    let descriptor = image(3, 2, 48);
    let id = transfer(1);
    chunk(&mut node, id, &descriptor, 0);
    staged(&mut node, id, &descriptor, 0, 48, true);
    publish(&mut node, Some(id), &descriptor);
    let effects = node.step(Input::Message {
        from: 2,
        msg: RaftMessage::AppendEntries {
            term: 5,
            leader_id: 2,
            prev_log_index: 5,
            prev_log_term: 5,
            entries: vec![],
            leader_commit: 5,
            contact_round: 10,
        },
    });
    let applied: Vec<_> = effects
        .iter()
        .filter_map(|e| match e {
            Effect::Apply(entry) => Some(entry.index),
            _ => None,
        })
        .collect();
    assert_eq!(applied, vec![4, 5]);
    assert_eq!(node.last_applied, 5);
    let mut restored = RaftNode::restore_with_snapshot(
        1,
        vec![2, 3],
        55,
        node.hard.clone(),
        node.snapshot_descriptor().cloned(),
        node.log.clone(),
    )
    .unwrap();
    assert_eq!(restored.last_applied, 3);
    let replay = restored.step(Input::Message {
        from: 2,
        msg: RaftMessage::AppendEntries {
            term: 5,
            leader_id: 2,
            prev_log_index: 5,
            prev_log_term: 5,
            entries: vec![],
            leader_commit: 5,
            contact_round: 11,
        },
    });
    let applied: Vec<_> = replay
        .iter()
        .filter_map(|e| match e {
            Effect::Apply(entry) => Some(entry.index),
            _ => None,
        })
        .collect();
    assert_eq!(applied, vec![4, 5]);
}
