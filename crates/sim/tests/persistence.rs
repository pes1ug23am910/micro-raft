//! Persistence and crash/restart acceptance tests.

use std::collections::BTreeSet;

use raft_core::rng::Pcg32;
use raft_core::{Effect, HardState, RaftMessage};
use sim::{audit_persist_before_send, PersistenceMode, Sim};

fn elect(sim: &mut Sim, seed: u64) -> u8 {
    assert!(
        sim.run_until(|s| s.leader().is_some(), 2_000),
        "seed={seed}: no leader within 2000 virtual ms"
    );
    sim.leader().expect("leader just observed")
}

fn vote_request(term: u64, candidate_id: u8) -> RaftMessage {
    RaftMessage::RequestVote {
        term,
        candidate_id,
        last_log_index: 0,
        last_log_term: 0,
    }
}

fn granted(effects: &[Effect]) -> bool {
    effects.iter().any(|effect| {
        matches!(
            effect,
            Effect::Send {
                msg: RaftMessage::RequestVoteReply {
                    vote_granted: true,
                    ..
                },
                ..
            }
        )
    })
}

#[test]
fn persist_ordered_before_send() {
    let invalid = vec![
        Effect::Send {
            to: 2,
            msg: vote_request(1, 1),
        },
        Effect::PersistHardState(HardState {
            current_term: 1,
            voted_for: Some(1),
        }),
    ];
    assert!(audit_persist_before_send(&invalid).is_err());

    let already_durable_idempotent_reply = vec![Effect::Send {
        to: 1,
        msg: RaftMessage::RequestVoteReply {
            term: 5,
            vote_granted: true,
        },
    }];
    assert!(
        audit_persist_before_send(&already_durable_idempotent_reply).is_ok(),
        "an idempotent reply needs no fresh write when its vote is already durable"
    );

    for seed in 0..50 {
        let mut sim = Sim::new(3, seed);
        sim.faults.drop_prob = 0.05;
        let leader = elect(&mut sim, seed);
        for i in 0..10 {
            sim.client_put(leader, &format!("k{i}"), "v");
        }
        sim.set_partitions(vec![BTreeSet::from([leader])]);
        sim.run_ms(500);
        sim.heal_partitions();
        sim.run_ms(1_000);

        assert!(
            sim.stats().effect_batches_audited > 0,
            "seed={seed}: no effect batch was audited"
        );
        assert!(
            sim.stats().external_effects_checked > 0,
            "seed={seed}: no durable-state boundary was checked"
        );
    }
}

#[test]
fn higher_term_is_durable_before_role_change() {
    let seed = 91;
    let mut sim = Sim::new(3, seed);
    let leader = elect(&mut sim, seed);
    let peer = *sim.node(leader).peers.first().expect("leader has peers");
    let higher = sim.node(leader).hard.current_term + 1;
    let effects = sim.deliver_now(peer, leader, vote_request(higher, peer));

    let persisted = effects
        .iter()
        .position(|effect| matches!(effect, Effect::PersistHardState(_)))
        .expect("higher term is persisted");
    let changed = effects
        .iter()
        .position(|effect| {
            matches!(
                effect,
                Effect::RoleChanged {
                    role_name: "Follower",
                    ..
                }
            )
        })
        .expect("leader steps down");
    assert!(
        persisted < changed,
        "term persistence must precede RoleChanged"
    );
}

#[test]
fn no_double_vote_after_crash_restart() {
    let hard = HardState {
        current_term: 5,
        voted_for: None,
    };

    let mut durable = Sim::new(3, 7);
    durable.seed_hard_state(3, hard.clone());
    assert!(granted(&durable.deliver_now(1, 3, vote_request(5, 1))));
    assert_eq!(durable.durable_hard_state(3).voted_for, Some(1));
    assert!(durable.crash(3));
    assert!(durable.restart(3));
    assert!(
        !granted(&durable.deliver_now(2, 3, vote_request(5, 2))),
        "persisted voted_for must prevent a second vote in term 5"
    );

    let mut negligent = Sim::new(3, 7);
    negligent.seed_hard_state(3, hard);
    negligent.set_persistence_mode(PersistenceMode::Negligent);
    assert!(granted(&negligent.deliver_now(1, 3, vote_request(5, 1))));
    assert_eq!(negligent.durable_hard_state(3).voted_for, None);
    assert!(negligent.crash(3));
    assert!(negligent.restart(3));
    assert!(
        granted(&negligent.deliver_now(2, 3, vote_request(5, 2))),
        "negative control must demonstrate the double vote when persistence is skipped"
    );
}

#[test]
fn committed_survives_follower_crash() {
    for seed in 0..10 {
        let mut sim = Sim::new(3, seed);
        let leader = elect(&mut sim, seed);
        for i in 0..10 {
            sim.client_put(leader, &format!("k{i}"), &format!("v{i}"));
        }
        assert!(
            sim.run_until(|s| (1..=3).all(|id| s.applied(id).len() >= 11), 3_000),
            "seed={seed}: entries did not commit and apply before the crash"
        );
        let follower = (1..=3).find(|&id| id != leader).expect("has follower");
        let durable_log = sim.durable_log(follower).to_vec();

        assert!(sim.crash(follower));
        assert!(sim.restart(follower));
        assert_eq!(sim.node(follower).log, durable_log);
        assert_eq!(sim.node(follower).commit_index, 0);
        assert!(sim.applied(follower).is_empty());

        assert!(
            sim.run_until(
                |s| {
                    s.node(1).log == s.node(2).log
                        && s.node(2).log == s.node(3).log
                        && s.applied(1) == s.applied(2)
                        && s.applied(2) == s.applied(3)
                },
                3_000,
            ),
            "seed={seed}: restarted follower did not replay and converge"
        );
    }
}

#[test]
fn minority_kills_never_lose_committed() {
    for seed in 0..50 {
        let mut sim = Sim::new(3, seed);
        sim.faults.drop_prob = 0.05;
        let mut rng = Pcg32::new(seed ^ 0xC0DE_5157);
        let mut down: Option<(u8, u64)> = None;

        for round in 0..300_u64 {
            if let Some((id, restart_at)) = down {
                if sim.now() >= restart_at {
                    assert!(sim.restart(id));
                    down = None;
                }
            }
            if down.is_none() && round > 0 && round % 40 == 0 {
                let id = rng.range_inclusive(1, 3) as u8;
                assert!(sim.crash(id));
                down = Some((id, sim.now() + 500));
            }

            let target = rng.range_inclusive(1, 3) as u8;
            sim.client_put(target, &format!("k{}", round % 11), &format!("v{round}"));
            sim.run_ms(100);
        }

        if let Some((id, _)) = down {
            assert!(sim.restart(id));
        }
        sim.faults.drop_prob = 0.0;
        sim.heal_partitions();
        sim.run_ms(5_000);

        for id in 2..=3 {
            assert_eq!(
                sim.node(1).log,
                sim.node(id).log,
                "seed={seed}: logs differ"
            );
            assert_eq!(
                sim.applied(1),
                sim.applied(id),
                "seed={seed}: applied histories differ"
            );
        }
        assert!(
            sim.stats().crashes > 0,
            "seed={seed}: crash schedule was inert"
        );
        assert!(!sim.registry().is_empty(), "seed={seed}: nothing committed");
    }
}
