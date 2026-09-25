//! Deterministic crash, partition, delay, and message-loss schedules.

use std::collections::BTreeSet;

use raft_core::rng::Pcg32;
use raft_core::{Command, Effect, NodeId};
use sim::Sim;

fn node_id(rng: &mut Pcg32) -> NodeId {
    u8::try_from(rng.range_inclusive(1, 3)).expect("generated node id fits")
}

fn accepted(effects: &[Effect]) -> bool {
    effects
        .iter()
        .any(|effect| matches!(effect, Effect::ProposeAccepted { .. }))
}

fn fully_converged(sim: &Sim) -> bool {
    (2..=3).all(|id| sim.node(1).log == sim.node(id).log && sim.applied(1) == sim.applied(id))
}

fn assert_registered_entry_present_and_applied(sim: &Sim, id: NodeId, seed: u64) {
    for (&index, entry) in sim.registry().iter() {
        let offset = usize::try_from(index - 1).expect("log index fits usize");
        assert_eq!(
            sim.node(id).log.get(offset),
            Some(entry),
            "seed={seed}: n{id} is missing committed entry {index}"
        );
        assert_eq!(
            sim.applied(id).get(offset),
            Some(&(index, entry.command.clone())),
            "seed={seed}: n{id} has not applied committed entry {index}"
        );
    }
}

#[test]
fn kill_the_leader_and_assert() {
    for seed in 0..10 {
        let mut sim = Sim::new(3, seed);
        assert!(
            sim.run_until(|state| state.leader().is_some(), 2_000),
            "seed={seed}: cold-start cluster did not elect a leader"
        );
        let old_leader = sim.leader().expect("leader just observed");

        for number in 1..=20 {
            let effects = sim.client_put(old_leader, &format!("k{number}"), &format!("v{number}"));
            assert!(
                accepted(&effects),
                "seed={seed}: leader n{old_leader} rejected k{number}"
            );
        }
        assert!(
            sim.run_until(
                |state| state.registry().len() >= 21
                    && (1..=3).all(|id| state.applied(id).len() >= 21),
                3_000,
            ),
            "seed={seed}: k1..k20 did not fully commit and apply"
        );
        assert_registered_entry_present_and_applied(&sim, old_leader, seed);

        assert!(sim.crash(old_leader));
        assert!(
            sim.run_until(
                |state| state.leader().is_some_and(|leader| leader != old_leader),
                2_000,
            ),
            "seed={seed}: no replacement leader within 2000 virtual ms"
        );
        let new_leader = sim.leader().expect("replacement leader just observed");
        assert_ne!(new_leader, old_leader);
        assert_registered_entry_present_and_applied(&sim, new_leader, seed);

        for number in 21..=30 {
            let effects = sim.client_put(new_leader, &format!("k{number}"), &format!("v{number}"));
            assert!(
                accepted(&effects),
                "seed={seed}: replacement leader n{new_leader} rejected k{number}"
            );
        }
        assert!(
            sim.run_until(
                |state| {
                    state.registry().len() >= 32
                        && (1..=3)
                            .filter(|&id| state.is_alive(id))
                            .all(|id| state.applied(id).len() >= 32)
                },
                3_000,
            ),
            "seed={seed}: k21..k30 did not commit under n{new_leader}"
        );
        assert_registered_entry_present_and_applied(&sim, new_leader, seed);

        assert!(sim.restart(old_leader));
        assert!(
            sim.run_until(fully_converged, 3_000),
            "seed={seed}: restarted n{old_leader} did not converge within 3000 virtual ms"
        );
        for id in 1..=3 {
            assert_registered_entry_present_and_applied(&sim, id, seed);
        }
    }
}

fn run_chaos_seed(seed: u64, duration_ms: u64) {
    let mut sim = Sim::new(3, seed);
    sim.faults.drop_prob = 0.05;
    sim.faults.delay_min_ms = 1;
    sim.faults.delay_max_ms = 50;

    let mut rng = Pcg32::new(seed ^ 0xC4A0_5EED_D15C_A11E);
    let mut next_partition_action = rng.range_inclusive(5_000, 10_000);
    let mut partition_active = false;
    let mut next_crash = rng.range_inclusive(8_000, 15_000);
    let mut down: Option<(NodeId, u64)> = None;
    let mut proposal = 0_u64;

    while sim.now() < duration_ms {
        if sim.now() >= next_partition_action {
            if partition_active {
                sim.heal_partitions();
            } else {
                sim.set_partitions(vec![BTreeSet::from([node_id(&mut rng)])]);
            }
            partition_active = !partition_active;
            next_partition_action = sim.now() + rng.range_inclusive(5_000, 10_000);
        }

        if let Some((id, restart_at)) = down {
            if sim.now() >= restart_at {
                assert!(sim.restart(id), "seed={seed}: n{id} did not restart");
                down = None;
                next_crash = sim.now() + rng.range_inclusive(8_000, 15_000);
            }
        } else if sim.now() >= next_crash {
            let id = node_id(&mut rng);
            assert!(sim.crash(id), "seed={seed}: n{id} did not crash");
            down = Some((id, sim.now() + rng.range_inclusive(250, 750)));
        }

        let target = node_id(&mut rng);
        let key = format!("k{}", rng.range_inclusive(0, 31));
        if rng.range_inclusive(0, 4) == 0 {
            sim.client_delete(target, &key);
        } else {
            sim.client_put(target, &key, &format!("v{proposal}"));
        }
        proposal += 1;
        sim.run_ms(100);
    }

    if let Some((id, _)) = down {
        assert!(
            sim.restart(id),
            "seed={seed}: final restart of n{id} failed"
        );
    }
    sim.heal_partitions();
    sim.faults.drop_prob = 0.0;
    sim.faults.delay_min_ms = 1;
    sim.faults.delay_max_ms = 20;
    sim.run_ms(5_000);

    assert!(
        fully_converged(&sim),
        "seed={seed}: logs or applied histories did not converge after cooldown"
    );
    assert!(
        !sim.registry().is_empty(),
        "seed={seed}: chaos schedule committed no entries"
    );
    assert!(
        sim.stats().messages_dropped_randomly > 0,
        "seed={seed}: random message loss was never exercised"
    );
    assert!(
        sim.stats().partitions_imposed > 0 && sim.stats().partitions_healed > 0,
        "seed={seed}: partition schedule was never fully exercised"
    );
    assert!(
        sim.stats().crashes > 0 && sim.stats().restarts > 0,
        "seed={seed}: crash/restart schedule was never exercised"
    );

    for id in 1..=3 {
        assert_registered_entry_present_and_applied(&sim, id, seed);
    }
}

#[test]
fn soak_200_seeds_all_invariants() {
    for seed in 0..200 {
        run_chaos_seed(seed, 60_000);
    }
}

#[test]
#[ignore = "extended deterministic soak; run explicitly before release"]
fn soak_extended() {
    for seed in 0..1_000 {
        run_chaos_seed(seed, 60_000);
    }
}

#[test]
fn chaos_schedule_replays_identically() {
    fn outcome(seed: u64) -> (Vec<Command>, sim::FaultStats) {
        let mut sim = Sim::new(3, seed);
        sim.faults.drop_prob = 0.05;
        sim.faults.delay_min_ms = 1;
        sim.faults.delay_max_ms = 50;
        let mut rng = Pcg32::new(seed ^ 0x0051_A7E5);
        for proposal in 0..100 {
            let id = node_id(&mut rng);
            sim.client_put(id, &format!("k{proposal}"), "v");
            sim.run_ms(100);
        }
        sim.faults.drop_prob = 0.0;
        sim.run_ms(2_000);
        (
            sim.applied(1)
                .iter()
                .map(|(_, command)| command.clone())
                .collect(),
            sim.stats().clone(),
        )
    }

    assert_eq!(outcome(137), outcome(137));
}
