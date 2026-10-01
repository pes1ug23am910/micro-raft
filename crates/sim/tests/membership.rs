use raft_core::membership::{AdminOperation, MemberEndpoints, VoterConfig};
use raft_core::membership_protocol::MembershipOutcome;
use raft_core::{Effect, Role, MAX_SNAPSHOT_CHUNK_BYTES};
use sim::{Sim, SnapshotFault};
use std::collections::BTreeSet;

fn cluster(seed: u64) -> Sim {
    let mut sim = Sim::new(5, seed);
    sim.configure_group("membership-test", vec![1, 2, 3]);
    sim.run_ms_ticking_only(&[1], 800);
    assert_eq!(sim.leader(), Some(1));
    assert!(sim.node(1).commit_index > 0);
    sim
}
fn add(id: u8) -> AdminOperation {
    AdminOperation::AddLearner {
        id,
        endpoints: MemberEndpoints {
            raft: format!("node-{id}:7000"),
            http: format!("node-{id}:8000"),
        },
    }
}
fn accepted(effects: &[Effect]) -> Option<u64> {
    effects.iter().find_map(|e| match e {
        Effect::MembershipResult {
            outcome: MembershipOutcome::Accepted { index },
            ..
        } => Some(*index),
        _ => None,
    })
}
fn change(sim: &mut Sim, id: &str, op: AdminOperation) -> u64 {
    let mut admitted = None;
    for _ in 0..150 {
        if let Some(leader) = sim.leader() {
            let effects = sim.membership_change(leader, id, op.clone());
            admitted = accepted(&effects).or(admitted);
            if let Some(record) = sim
                .node(leader)
                .committed_membership()
                .state
                .records
                .get(id)
            {
                if record.final_index.is_some() {
                    return record.first_index;
                }
            }
        }
        sim.run_ms(50);
    }
    for actor in 1..=5 {
        let n = sim.node(actor);
        eprintln!("admin={id} actor={actor} alive={} role={:?} term={} commit={} last={} committed={:?} effective={:?}",sim.is_alive(actor),n.role,n.hard.current_term,n.commit_index,n.last_log_index(),n.committed_membership().state.voters,n.effective_membership().voters);
    }
    panic!("admin did not complete; last admission={admitted:?}");
}

fn admit(sim: &mut Sim, id: &str, operation: AdminOperation) -> (u8, u64) {
    for _ in 0..100 {
        if let Some(leader) = sim.leader() {
            if let Some(index) = accepted(&sim.membership_change(leader, id, operation.clone())) {
                return (leader, index);
            }
        }
        sim.run_ms(50);
    }
    panic!("no checked admission for {id}");
}

fn put(sim: &mut Sim, key: &str) -> u64 {
    assert!(sim.run_until(|s| s.leader().is_some(), 3000));
    let leader = sim.leader().unwrap();
    let effects = sim.client_put(leader, key, "ack");
    let index = effects
        .iter()
        .find_map(|e| match e {
            Effect::ProposeAccepted { index } => Some(*index),
            _ => None,
        })
        .unwrap();
    assert!(sim.run_until(|s| s.node(leader).commit_index >= index, 3000));
    index
}

#[test]
fn joining_learner_installs_snapshot_before_promotion_then_all_recover() {
    let mut sim = cluster(8101);
    for n in 0..12 {
        put(&mut sim, &format!("key-{n}"));
    }
    let leader = sim.leader().unwrap();
    let boundary = sim.node(leader).last_applied;
    sim.compact(leader, MAX_SNAPSHOT_CHUNK_BYTES * 2 + 13);
    assert!(
        !sim.node(leader)
            .snapshot_descriptor()
            .unwrap()
            .metadata
            .members
            .contains(&4),
        "the installed image must predate learner admission"
    );
    assert!(!sim
        .node(4)
        .effective_membership()
        .participants()
        .contains(&4));
    change(&mut sim, "add-4", add(4));
    assert!(sim.run_until(|s| s.node(4).last_applied > boundary, 4000));
    assert!(sim.snapshots_installed() > 0);
    assert_eq!(sim.node(4).snapshot_index(), boundary);
    change(
        &mut sim,
        "promote-4",
        AdminOperation::SetVoters {
            voters: vec![1, 2, 3, 4],
        },
    );
    sim.run_ms(300);
    let final_index = sim.node(sim.leader().unwrap()).committed_membership().index;
    for id in 1..=5 {
        sim.crash(id);
    }
    for id in 1..=5 {
        sim.restart(id);
    }
    assert!(sim.run_until(
        |s| s.leader().is_some() && (1..=4).all(|id| s.node(id).last_applied >= final_index),
        5000
    ));
    assert_eq!(
        sim.node(5).role,
        Role::Follower,
        "unadmitted actor never votes"
    );
    for id in 1..=4 {
        assert_eq!(
            sim.node(id).committed_membership().state.voters,
            VoterConfig::Stable {
                voters: vec![1, 2, 3, 4]
            }
        );
    }
}

#[test]
fn old_voter_offline_across_leader_removal_learns_new_leader_and_snapshot_configuration() {
    let mut sim = cluster(8111);
    sim.crash(3);
    change(&mut sim, "add-4", add(4));
    change(&mut sim, "add-5", add(5));
    change(
        &mut sim,
        "replace-old",
        AdminOperation::SetVoters {
            voters: vec![3, 4, 5],
        },
    );
    assert!(sim.run_until(|s| s.leader().is_some_and(|id| id == 4 || id == 5), 4000));
    let leader = sim.leader().unwrap();
    let boundary = put(&mut sim, "new-voters-write");
    sim.compact(leader, MAX_SNAPSHOT_CHUNK_BYTES + 17);
    sim.set_snapshot_fault(3, SnapshotFault::CrashAfterPublication);
    sim.restart(3);
    assert!(
        sim.run_until(|s| !s.is_alive(3), 5000),
        "old voter reaches new leader's snapshot publication"
    );
    assert_eq!(sim.snapshot_fault_hits(), 1);
    assert!(sim.durable_hard_state(3).membership.as_ref().unwrap().index < boundary);
    sim.restart(3);
    assert!(sim.run_until(|s| s.node(3).last_applied >= boundary, 5000));
    assert_eq!(
        sim.node(3).committed_membership().state.voters,
        VoterConfig::Stable {
            voters: vec![3, 4, 5]
        }
    );
    // Removed directories learn their tombstone and stay passive on restart.
    assert!(sim.run_until(
        |s| (1..=2).all(|id| s
            .node(id)
            .committed_membership()
            .state
            .retired
            .contains(&id)),
        4000
    ));
    for id in 1..=2 {
        sim.crash(id);
        sim.restart(id);
    }
    sim.run_ms(2000);
    assert_eq!(sim.node(1).role, Role::Follower);
    assert_eq!(sim.node(2).role, Role::Follower);
    assert!(sim.leader().is_some_and(|id| [3, 4, 5].contains(&id)));
}

#[test]
fn pending_joint_configuration_survives_all_restart_and_exact_admin_retry() {
    let mut sim = cluster(8123);
    change(&mut sim, "add-4", add(4));
    change(&mut sim, "add-5", add(5));
    sim.run_ms(300);
    // Initial admission is possible; then isolate the old-majority partition.
    let op = AdminOperation::SetVoters {
        voters: vec![3, 4, 5],
    };
    let (leader, first) = admit(&mut sim, "joint", op.clone());
    let other_old = (1..=3).find(|id| *id != leader).unwrap();
    let old_side = BTreeSet::from([leader, other_old]);
    let remaining = (1..=5).filter(|id| !old_side.contains(id)).collect();
    sim.set_partitions(vec![old_side, remaining]);
    sim.run_ms(200);
    assert!(
        sim.node(leader).commit_index < first,
        "old-only cannot commit new joint entry"
    );
    for id in 1..=5 {
        sim.crash(id);
    }
    sim.heal_partitions();
    for id in 1..=5 {
        sim.restart(id);
    }
    let elected = sim.run_until(|s| s.leader().is_some(), 5000);
    if !elected {
        for actor in 1..=5 {
            let n = sim.node(actor);
            eprintln!(
                "actor={actor} role={:?} term={} commit={} last={} committed={:?} effective={:?}",
                n.role,
                n.hard.current_term,
                n.commit_index,
                n.last_log_index(),
                n.committed_membership().state.voters,
                n.effective_membership().voters
            );
        }
    }
    assert!(elected);
    let mut completed = false;
    for _ in 0..100 {
        if let Some(leader) = sim.leader() {
            sim.membership_change(leader, "joint", op.clone());
            completed = sim
                .node(leader)
                .committed_membership()
                .state
                .records
                .get("joint")
                .is_some_and(|record| record.final_index.is_some());
            if completed {
                break;
            }
        }
        sim.run_ms(100);
    }
    assert!(completed);
    let leader = sim.leader().unwrap();
    let before = sim.node(leader).last_log_index();
    let effects = sim.membership_change(leader, "joint", op);
    assert!(effects.iter().any(|e| matches!(
        e,
        Effect::MembershipResult {
            outcome: MembershipOutcome::Recorded { .. },
            ..
        }
    )));
    assert_eq!(sim.node(leader).last_log_index(), before);
}

#[test]
fn repeated_loss_compaction_changes_and_restart_keep_independent_history_witness() {
    for seed in 8200..8210 {
        let mut sim = cluster(seed);
        sim.faults.drop_prob = 0.05;
        change(&mut sim, "add-4", add(4));
        change(&mut sim, "add-5", add(5));
        for n in 0..5 {
            put(&mut sim, &format!("before-{n}"));
        }
        change(
            &mut sim,
            "promote",
            AdminOperation::SetVoters {
                voters: vec![1, 2, 3, 4, 5],
            },
        );
        let leader = sim.leader().unwrap();
        sim.compact(leader, MAX_SNAPSHOT_CHUNK_BYTES + 11);
        change(&mut sim, "remove-2", AdminOperation::Remove { id: 2 });
        sim.run_ms(300);
        sim.crash(2);
        sim.restart(2);
        for n in 0..5 {
            put(&mut sim, &format!("after-{n}"));
        }
        sim.run_ms(500);
        assert_eq!(sim.node(2).role, Role::Follower);
        assert!(sim.registry().len() >= 17);
    }
}

#[test]
fn joint_snapshot_with_uncommitted_final_suffix_recovers_and_finishes_after_all_crash() {
    let mut sim = cluster(8161);
    change(&mut sim, "add-4", add(4));
    change(&mut sim, "add-5", add(5));
    sim.run_ms(300);
    let operation = AdminOperation::SetVoters {
        voters: vec![3, 4, 5],
    };
    let (leader, _) = admit(&mut sim, "joint-snapshot", operation.clone());
    assert!(sim.run_until(
        |s| matches!(
            s.node(leader).committed_membership().state.voters,
            VoterConfig::Joint { .. }
        ),
        3000
    ));
    let boundary = sim.node(leader).commit_index;
    sim.compact(leader, MAX_SNAPSHOT_CHUNK_BYTES + 23);
    assert!(matches!(
        sim.node(leader)
            .snapshot_descriptor()
            .unwrap()
            .metadata
            .membership
            .as_ref()
            .unwrap()
            .state
            .voters,
        VoterConfig::Joint { .. }
    ));
    assert!(
        sim.node(leader).last_log_index() > boundary,
        "final remains in retained suffix"
    );
    for id in 1..=5 {
        sim.crash(id);
    }
    for id in 1..=5 {
        sim.restart(id);
    }
    assert_eq!(sim.node(leader).snapshot_index(), boundary);
    assert_eq!(
        sim.node(leader).effective_membership().voters,
        VoterConfig::Stable {
            voters: vec![3, 4, 5]
        }
    );
    assert!(sim.run_until(|s| s.leader().is_some(), 5000));
    change(&mut sim, "joint-snapshot", operation);
    assert_eq!(
        sim.node(sim.leader().unwrap())
            .committed_membership()
            .state
            .voters,
        VoterConfig::Stable {
            voters: vec![3, 4, 5]
        }
    );
}
