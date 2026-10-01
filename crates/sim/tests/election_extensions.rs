//! Election extensions exercised through durable effects and a virtual network.
use raft_core::{Command, Effect, RaftMessage, Role, CHECK_QUORUM_MS};
use sim::Sim;
use std::collections::BTreeSet;

#[test]
fn isolated_follower_keeps_term_and_rejoins_without_disrupting_majority() {
    for seed in 0..40 {
        let mut sim = Sim::new(3, seed);
        assert!(
            sim.run_until(|s| s.leader().is_some(), 1_000),
            "seed={seed}"
        );
        sim.run_ms(200);
        let leader = sim.leader().unwrap();
        let isolated = *sim.node(leader).peers.first().unwrap();
        let term = sim.node(leader).hard.current_term;
        sim.set_partitions(vec![BTreeSet::from([isolated])]);
        sim.run_ms(5_000);
        assert_eq!(
            sim.node(isolated).hard.current_term,
            term,
            "seed={seed}: isolated term inflated"
        );
        assert_eq!(sim.durable_hard_state(isolated).current_term, term);
        assert!(matches!(sim.node(isolated).role, Role::PreCandidate { .. }));
        assert_eq!(
            sim.leader(),
            Some(leader),
            "seed={seed}: healthy majority disrupted"
        );
        sim.heal_partitions();
        sim.run_ms(2_000);
        assert_eq!(
            sim.leader(),
            Some(leader),
            "seed={seed}: rejoin forced replacement"
        );
        for id in 1..=3 {
            assert_eq!(sim.node(id).hard.current_term, term, "seed={seed}, id={id}");
        }
    }
}

#[test]
fn captured_old_round_replies_do_not_keep_partitioned_leader_authoritative() {
    for seed in 0..30 {
        let mut sim = Sim::new(3, seed);
        assert!(
            sim.run_until(|s| s.leader().is_some(), 1_000),
            "seed={seed}"
        );
        sim.run_ms(200);
        let old = sim.leader().unwrap();
        let peer = sim.node(old).peers[0];
        let term = sim.node(old).hard.current_term;
        let round = sim.node(old).contact_round().unwrap();
        // Obtain a genuine reply through the follower, then retain a copy as
        // if the network had duplicated and delayed it before the partition.
        let effects = sim.deliver_now(
            old,
            peer,
            RaftMessage::AppendEntries {
                term,
                leader_id: old,
                prev_log_index: 0,
                prev_log_term: 0,
                entries: vec![],
                leader_commit: 0,
                contact_round: round,
            },
        );
        let captured = effects
            .into_iter()
            .find_map(|effect| match effect {
                Effect::Send {
                    to,
                    msg: msg @ RaftMessage::AppendEntriesReply { .. },
                } if to == old => Some(msg),
                _ => None,
            })
            .expect("follower replied");
        sim.set_partitions(vec![BTreeSet::from([old])]);
        let pending = sim.client_put(old, "pending", "minority");
        assert!(pending
            .iter()
            .any(|e| matches!(e, Effect::ProposeAccepted { .. })));
        let isolated_at = sim.now();
        while sim.now() - isolated_at < 2 * CHECK_QUORUM_MS + 50 {
            sim.run_ms(50);
            sim.deliver_now(peer, old, captured.clone());
        }
        assert!(
            !sim.node(old).is_leader(),
            "seed={seed}: stale replies preserved leader"
        );
        assert!(sim
            .client_put(old, "late", "must-refuse")
            .iter()
            .any(|e| matches!(e, Effect::ProposeRejected { .. })));
        assert!(!sim.registry().iter().any(
            |(_, entry)| matches!(&entry.command, Command::Put { key, .. } if key == "pending")
        ));
        assert!(
            sim.run_until(|s| s.leader().is_some_and(|id| id != old), 2_000),
            "seed={seed}: no majority leader"
        );
        let replacement = sim.leader().unwrap();
        sim.client_put(replacement, "after", "majority");
        assert!(sim.run_until(
            |s| s.registry().iter().any(
                |(_, entry)| matches!(&entry.command, Command::Put { key, .. } if key == "after")
            ),
            1_000
        ));
        sim.heal_partitions();
        assert!(
            sim.run_until(
                |s| (1..=3).all(|id| s.node(id).log == s.node(replacement).log
                    && s.applied(id) == s.applied(replacement)),
                3_000
            ),
            "seed={seed}: no convergence"
        );
    }
}

#[test]
fn single_node_commits_and_recovers_via_its_current_term_noop() {
    let mut sim = Sim::new(1, 111);
    assert!(sim.run_until(|s| s.leader() == Some(1), 1_000));
    sim.client_put(1, "single", "durable");
    assert_eq!(sim.node(1).commit_index, 2);
    assert_eq!(sim.applied(1).len(), 2);
    sim.crash(1);
    sim.restart(1);
    assert_eq!(sim.node(1).commit_index, 0);
    assert!(sim.run_until(|s| s.leader() == Some(1), 1_000));
    assert_eq!(sim.node(1).commit_index, 3);
    assert_eq!(sim.applied(1).len(), 3);
}
