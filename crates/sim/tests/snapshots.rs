use raft_core::{Command, Effect, MAX_SNAPSHOT_CHUNK_BYTES};
use sim::{Sim, SnapshotFault};
use std::panic::{catch_unwind, AssertUnwindSafe};

fn elected(seed: u64) -> (Sim, u8) {
    let mut sim = Sim::new(3, seed);
    assert!(sim.run_until(|s| s.leader().is_some() && !s.registry().is_empty(), 3000));
    sim.run_ms(200);
    let leader = sim.leader().unwrap();
    (sim, leader)
}
fn put(sim: &mut Sim, leader: u8, key: &str, value: &str) -> u64 {
    let effects = sim.client_put(leader, key, value);
    let index = effects
        .iter()
        .find_map(|e| match e {
            Effect::ProposeAccepted { index } => Some(*index),
            _ => None,
        })
        .expect("leader accepts");
    assert!(sim.run_until(|s| s.node(leader).commit_index >= index, 2000));
    index
}

#[test]
fn lagging_follower_installs_multiple_chunks_then_appends_and_all_nodes_reopen() {
    let (mut sim, leader) = elected(3101);
    let lagger = (1..=3).find(|id| *id != leader).unwrap();
    let early = sim.applied(lagger).to_vec();
    assert!(sim.crash(lagger));
    let index = put(&mut sim, leader, "snapshot-key", "acknowledged");
    sim.compact(leader, MAX_SNAPSHOT_CHUNK_BYTES * 3 + 17);
    assert_eq!(sim.node(leader).snapshot_index(), index);
    assert!(sim.node(leader).log.is_empty());
    let after = put(&mut sim, leader, "suffix-key", "after-compaction");
    assert!(sim.restart(lagger));
    assert!(sim.run_until(|s| s.node(lagger).last_applied >= after, 4000));
    assert_eq!(sim.node(lagger).snapshot_index(), index);
    assert_eq!(&sim.applied(lagger)[..early.len()], early.as_slice());
    assert!(sim.snapshots_installed() >= 1);
    for id in 1..=3 {
        assert!(sim.crash(id));
    }
    for id in 1..=3 {
        assert!(sim.restart(id));
    }
    assert!(sim.run_until(
        |s| s.leader().is_some() && (1..=3).all(|id| s.node(id).last_applied >= after),
        4000
    ));
    for id in 1..=3 {
        assert_eq!(
            sim.applied(id)[index as usize - 1].1,
            Command::Put {
                key: "snapshot-key".into(),
                value: "acknowledged".into()
            }
        );
        assert_eq!(
            sim.applied(id)[after as usize - 1].1,
            Command::Put {
                key: "suffix-key".into(),
                value: "after-compaction".into()
            }
        );
    }
}

#[test]
fn local_publication_crashes_select_exactly_old_or_new_generation() {
    for fault in [
        SnapshotFault::CrashBeforePublication,
        SnapshotFault::CrashAfterPublication,
    ] {
        let (mut sim, leader) = elected(3113);
        let index = put(&mut sim, leader, "stable", "value");
        let prefix = sim.applied(leader).to_vec();
        sim.set_snapshot_fault(leader, fault);
        sim.compact(leader, 48);
        assert!(!sim.is_alive(leader));
        assert_eq!(sim.snapshot_fault_hits(), 1);
        sim.restart(leader);
        let expected = if fault == SnapshotFault::CrashAfterPublication {
            index
        } else {
            0
        };
        assert_eq!(sim.node(leader).snapshot_index(), expected);
        assert!(sim.run_until(|s| s.node(leader).last_applied >= index, 4000));
        assert_eq!(&sim.applied(leader)[..prefix.len()], prefix.as_slice());
    }
}

#[test]
fn staged_follower_crash_discards_partial_image_and_retries_from_zero() {
    let (mut sim, leader) = elected(3127);
    let lagger = (1..=3).find(|id| *id != leader).unwrap();
    sim.crash(lagger);
    let index = put(&mut sim, leader, "staged", "safe");
    sim.compact(leader, MAX_SNAPSHOT_CHUNK_BYTES * 4 + 17);
    sim.restart(lagger);
    assert!(sim.run_until(|s| s.staged_snapshot_bytes(lagger) > 0, 2000));
    sim.set_snapshot_fault(lagger, SnapshotFault::CrashAfterStage);
    assert!(sim.run_until(|s| !s.is_alive(lagger), 1000));
    assert_eq!(sim.snapshot_fault_hits(), 1);
    assert_eq!(sim.staged_snapshot_bytes(lagger), 0);
    sim.restart(lagger);
    assert!(sim.run_until(|s| s.node(lagger).last_applied >= index, 5000));
    assert_eq!(sim.node(lagger).snapshot_index(), index);
}

#[test]
fn follower_publication_crash_recovers_image_without_final_ack() {
    let (mut sim, leader) = elected(3137);
    let lagger = (1..=3).find(|id| *id != leader).unwrap();
    sim.crash(lagger);
    let index = put(&mut sim, leader, "published", "safe");
    sim.compact(leader, MAX_SNAPSHOT_CHUNK_BYTES + 17);
    sim.set_snapshot_fault(lagger, SnapshotFault::CrashAfterPublication);
    sim.restart(lagger);
    assert!(sim.run_until(|s| !s.is_alive(lagger), 3000));
    assert_eq!(sim.snapshot_fault_hits(), 1);
    sim.restart(lagger);
    assert_eq!(sim.node(lagger).snapshot_index(), index);
    let suffix = put(&mut sim, leader, "later", "still-safe");
    assert!(sim.run_until(|s| s.node(lagger).last_applied >= suffix, 3000));
}

fn panic_message(value: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = value.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = value.downcast_ref::<&str>() {
        message.to_string()
    } else {
        "non-string panic".into()
    }
}

#[test]
fn executed_missing_snapshot_durability_fault_is_reached_and_rejected() {
    let (mut sim, leader) = elected(3151);
    put(&mut sim, leader, "keep", "me");
    sim.set_snapshot_fault(leader, SnapshotFault::SkipDurablePublication);
    let failure = catch_unwind(AssertUnwindSafe(|| sim.compact(leader, 48)))
        .expect_err("missing durable publication must fail");
    assert_eq!(sim.snapshot_fault_hits(), 1);
    assert!(panic_message(failure).contains("durable"));
}

#[test]
fn executed_compacted_history_corruption_is_reached_and_original_oracle_rejects_it() {
    let (mut sim, leader) = elected(3163);
    put(&mut sim, leader, "keep", "history");
    sim.run_ms(200);
    sim.set_snapshot_fault(leader, SnapshotFault::CorruptPublishedHistory);
    let failure = catch_unwind(AssertUnwindSafe(|| sim.compact(leader, 48)))
        .expect_err("compaction must not erase safety evidence");
    assert_eq!(sim.snapshot_fault_hits(), 1);
    let message = panic_message(failure);
    assert!(
        message.contains("LogMatching") || message.contains("applied command differs"),
        "unexpected oracle: {message}"
    );
}

#[test]
fn repeated_compaction_with_loss_partitions_and_leader_change_preserves_full_history() {
    for seed in 3200..3220 {
        let (mut sim, leader) = elected(seed);
        let lagger = (1..=3).find(|id| *id != leader).unwrap();
        sim.crash(lagger);
        let first = put(&mut sim, leader, "first", "1");
        sim.compact(leader, MAX_SNAPSHOT_CHUNK_BYTES + 17);
        put(&mut sim, leader, "second", "2");
        sim.compact(leader, MAX_SNAPSHOT_CHUNK_BYTES * 2 + 17);
        sim.faults.drop_prob = 0.15;
        sim.restart(lagger);
        assert!(
            sim.run_until(|s| s.node(lagger).snapshot_index() > first, 6000),
            "seed={seed}"
        );
        sim.faults.drop_prob = 0.0;
        sim.run_ms(200);
        sim.crash(leader);
        assert!(sim.run_until(|s| s.leader().is_some_and(|id| id != leader), 4000));
        let new = sim.leader().unwrap();
        let last = put(&mut sim, new, "after-failover", "3");
        sim.restart(leader);
        assert!(sim.run_until(|s| (1..=3).all(|id| s.node(id).last_applied >= last), 5000));
    }
}
