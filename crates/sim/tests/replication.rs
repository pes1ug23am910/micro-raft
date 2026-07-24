//! Log-replication, repair, and Figure 8 commitment acceptance tests under the
//! deterministic simulator. Every failure
//! message carries its seed.

use raft_core::{Command, Effect, Entry, HardState, LogIndex, RaftMessage, Term};
use sim::Sim;

fn entries(range: std::ops::RangeInclusive<u64>, term_of: impl Fn(LogIndex) -> Term) -> Vec<Entry> {
    range
        .map(|index| Entry {
            index,
            term: term_of(index),
            command: Command::NoOp,
        })
        .collect()
}

/// The applied history with NoOps filtered out — NoOps are present in logs
/// and applied histories but have no KV effect (§4/M4).
fn applied_commands(sim: &Sim, id: u8) -> Vec<Command> {
    sim.applied(id)
        .iter()
        .filter(|(_, c)| !matches!(c, Command::NoOp))
        .map(|(_, c)| c.clone())
        .collect()
}

#[test]
fn entries_replicate_and_apply() {
    for seed in 0..10 {
        let mut sim = Sim::new(3, seed);
        assert!(
            sim.run_until(|s| s.leader().is_some(), 1_000),
            "seed={seed}: no leader within 1000 virtual ms"
        );
        let leader = sim.leader().expect("just elected");
        let mut proposed = Vec::new();
        for i in 0..20 {
            let (k, v) = (format!("k{i}"), format!("v{i}"));
            let effects = sim.client_put(leader, &k, &v);
            assert!(
                effects.iter().any(|e| matches!(e, Effect::ProposeAccepted { .. })),
                "seed={seed}: leader must accept proposal {i}"
            );
            proposed.push(Command::Put { key: k, value: v });
        }
        // Within 2,000 virtual ms all three applied histories equal the
        // proposed sequence (NoOps excluded from KV effect, present in logs).
        let converged = sim.run_until(
            |s| (1..=3).all(|id| applied_commands(s, id) == proposed),
            2_000,
        );
        assert!(
            converged,
            "seed={seed}: applied histories did not converge to the 20 proposed puts; \
             histories: {:?}",
            (1..=3).map(|id| sim.applied(id).len()).collect::<Vec<_>>()
        );
    }
}

#[test]
fn noop_committed_on_election() {
    for seed in 0..20 {
        let mut sim = Sim::new(3, seed);
        assert!(
            sim.run_until(|s| s.leader().is_some(), 1_000),
            "seed={seed}: no leader within 1000 virtual ms"
        );
        let leader = sim.leader().expect("just elected");
        // R16: the new leader appended its current-term NoOp immediately.
        let noop_index = sim.node(leader).log.last().map_or(0, |e| e.index);
        assert!(
            noop_index >= 1
                && matches!(
                    sim.node(leader).log.last().map(|e| &e.command),
                    Some(Command::NoOp)
                ),
            "seed={seed}: new leader must append its NoOp on election (R16)"
        );
        // R16 + R19 interlock: it reaches commit_index within 1,000 virtual ms.
        let committed = sim.run_until(|s| s.node(leader).commit_index >= noop_index, 1_000);
        assert!(
            committed,
            "seed={seed}: leader NoOp (index {noop_index}) not committed within 1000 ms; \
             commit_index={}",
            sim.node(leader).commit_index
        );
    }
}

#[test]
fn not_leader_propose_rejected() {
    for seed in 0..10 {
        let mut sim = Sim::new(3, seed);
        assert!(
            sim.run_until(|s| s.leader().is_some(), 1_000),
            "seed={seed}: no leader within 1000 virtual ms"
        );
        let leader = sim.leader().expect("just elected");
        // Let heartbeats flow so every follower has accepted an AppendEntries
        // from the current leader — the source of the hint (R20).
        sim.run_ms(200);
        let follower = *sim
            .node(leader)
            .peers
            .first()
            .expect("3-node cluster has peers");
        let effects = sim.client_put(follower, "k", "v");
        let rejected = effects.iter().find_map(|e| match e {
            Effect::ProposeRejected { leader_hint } => Some(*leader_hint),
            _ => None,
        });
        assert_eq!(
            rejected,
            Some(Some(leader)),
            "seed={seed}: proposing to follower n{follower} must yield ProposeRejected \
             with leader_hint Some(n{leader}); got {rejected:?}"
        );
        assert!(
            !effects.iter().any(|e| matches!(e, Effect::ProposeAccepted { .. })),
            "seed={seed}: a follower must never accept a proposal (R20)"
        );
    }
}

#[test]
fn divergent_follower_converges() {
    for seed in 0..10 {
        let mut sim = Sim::new(3, seed);
        assert!(
            sim.run_until(|s| s.leader().is_some(), 1_000),
            "seed={seed}: no leader within 1000 virtual ms"
        );
        let all_logs_equal =
            |s: &Sim| s.node(1).log == s.node(2).log && s.node(2).log == s.node(3).log;
        let followers: Vec<u8> = (1..=3)
            .filter(|&id| Some(id) != sim.leader())
            .collect();

        for (phase, &isolated) in followers.iter().enumerate() {
            // Partition one follower away; the remaining two are a majority.
            sim.faults.partitions = vec![std::collections::BTreeSet::from([isolated])];
            // Feed the (current) leader 5 entries and let them commit.
            for i in 0..5 {
                assert!(
                    sim.run_until(|s| s.leader().is_some(), 2_000),
                    "seed={seed}: no leader during phase {phase}"
                );
                let leader = sim.leader().expect("just checked");
                let effects = sim.client_put(leader, &format!("p{phase}k{i}"), "v");
                assert!(
                    effects.iter().any(|e| matches!(e, Effect::ProposeAccepted { .. })),
                    "seed={seed}: leader must accept phase-{phase} proposal {i}"
                );
                sim.run_ms(100);
            }
            // Heal fully. The isolated node's ratcheted term may force a
            // re-election; convergence must still win out (R9b guarantees
            // the winner holds every committed entry).
            sim.faults.partitions.clear();
        }
        let converged = sim.run_until(all_logs_equal, 3_000);
        assert!(
            converged,
            "seed={seed}: logs not byte-identical within 3000 ms of the final heal; \
             lengths: {:?}",
            (1..=3).map(|id| sim.node(id).log.len()).collect::<Vec<_>>()
        );
    }
}

#[test]
fn deep_divergence_repaired() {
    for seed in 0..10 {
        let mut sim = Sim::new(3, seed);
        // The Figure 7 family: a common committed-era prefix (1..=3, term 1);
        // node 1 holds two entries from term 3; node 2 holds FOUR extra
        // uncommitted entries from dead term 2; node 3 has the bare prefix.
        sim.seed_log(1, entries(1..=5, |i| if i <= 3 { 1 } else { 3 }));
        sim.seed_log(2, entries(1..=7, |i| if i <= 3 { 1 } else { 2 }));
        sim.seed_log(3, entries(1..=3, |_| 1));
        for id in 1..=3 {
            sim.seed_hard_state(
                id,
                HardState {
                    current_term: 3,
                    voted_for: None,
                },
            );
        }
        // Only node 1's clock runs: it campaigns (term 4) and must win —
        // its last term (3) outranks node 2's dead-term tail (R9b).
        let mut elected = false;
        for _ in 0..200 {
            sim.run_ms_ticking_only(&[1], 10);
            if sim.node(1).is_leader() {
                elected = true;
                break;
            }
        }
        assert!(elected, "seed={seed}: node 1 never won its term-4 election");

        // Repair rides the heartbeat cadence: next_index walks back to the
        // common prefix, node 2's wreckage is truncated (R13), the leader's
        // suffix installed. Node clocks 2–3 stay frozen — repair is entirely
        // message-driven.
        let repaired = (0..60).any(|_| {
            sim.run_ms_ticking_only(&[1], 10);
            sim.node(2).log == sim.node(1).log && sim.node(3).log == sim.node(1).log
        });
        assert!(
            repaired,
            "seed={seed}: divergent follower logs did not converge to the leader's; \
             n1={} n2={} n3={} entries",
            sim.node(1).log.len(),
            sim.node(2).log.len(),
            sim.node(3).log.len()
        );
        assert!(
            !sim.node(2).log.iter().any(|e| e.term == 2),
            "seed={seed}: R13 truncation must have removed every dead term-2 entry"
        );
    }
}

/// The flagship regression (§4/M4): a majority-replicated PRIOR-term entry
/// must not commit by count alone — R19(b), the exact clause Figure 8 of the
/// paper exists to justify. Scripted on 5 nodes with the paper's history
/// seeded and the decisive term-4 phase played out organically.
#[test]
fn figure8_prior_term_not_committed_by_count() {
    for seed in 0..5 {
        let mut sim = Sim::new(5, seed);
        let e1 = Entry {
            index: 1,
            term: 1,
            command: Command::NoOp,
        };
        let e2 = Entry {
            index: 2,
            term: 2,
            command: Command::Put {
                key: "x".into(),
                value: "1".into(),
            },
        };
        // History as of Figure 8(c): S1 led term 2 and replicated e2 to S2
        // only. S5 then won term 3 with votes from S3/S4 and appended its
        // own term-3 entry locally (R16) before being cut off.
        sim.seed_log(1, vec![e1.clone(), e2.clone()]);
        sim.seed_log(2, vec![e1.clone(), e2.clone()]);
        sim.seed_log(3, vec![e1.clone()]);
        sim.seed_log(4, vec![e1.clone()]);
        sim.seed_log(
            5,
            vec![
                e1.clone(),
                Entry {
                    index: 2,
                    term: 3,
                    command: Command::NoOp,
                },
            ],
        );
        for (id, term, voted) in [(1, 2, 1), (2, 2, 1), (3, 3, 5), (4, 3, 5), (5, 3, 5)] {
            sim.seed_hard_state(
                id,
                HardState {
                    current_term: term,
                    voted_for: Some(voted),
                },
            );
        }
        // S4 and S5 stay unreachable; S1's electorate is {S1, S2, S3}.
        sim.faults.partitions = vec![
            std::collections::BTreeSet::from([1, 2, 3]),
            std::collections::BTreeSet::from([4, 5]),
        ];

        // S1 campaigns. Term 3 must fail (S3's vote is spent on S5); the
        // retry wins term 4 with S2 and S3 — exactly the paper's timeline.
        let mut elected = false;
        for _ in 0..250 {
            sim.run_ms_ticking_only(&[1], 10);
            if sim.node(1).is_leader() {
                elected = true;
                break;
            }
        }
        assert!(elected, "seed={seed}: S1 never regained leadership");
        assert_eq!(
            sim.node(1).hard.current_term,
            4,
            "seed={seed}: S1 must win at term 4 (term 3 is spent on S5)"
        );

        // Freeze every clock: S1's win-step AppendEntries land (S2 takes the
        // term-4 NoOp; S3 fails the consistency check) and the replies come
        // back — but no further heartbeats fire.
        sim.run_ms_ticking_only(&[], 60);
        assert_eq!(
            sim.node(1).commit_index,
            0,
            "seed={seed}: nothing may commit yet"
        );

        // A delayed/retransmitted AppendEntries carrying ONLY the term-2
        // entry reaches S3 — legal under Raft's asynchronous network; this
        // is R16's NoOp having its *replication* delayed, never its append.
        sim.deliver_now(
            1,
            3,
            RaftMessage::AppendEntries {
                term: 4,
                leader_id: 1,
                prev_log_index: 1,
                prev_log_term: 1,
                entries: vec![e2.clone()],
                leader_commit: 0,
            },
        );
        // Let S3's success reply reach S1's bookkeeping (clocks still frozen).
        sim.run_ms_ticking_only(&[], 60);

        // THE moment: S1, S2, S3 — a strict majority of five — now hold the
        // term-2 entry at index 2. R19(b) must refuse to commit it by count.
        for id in [1, 2, 3] {
            assert_eq!(
                sim.node(id).log.get(1),
                Some(&e2),
                "seed={seed}: S{id} must hold the term-2 entry — majority premise"
            );
        }
        assert!(
            sim.node(1).commit_index <= 1,
            "seed={seed}: R19(b) violated — a majority-replicated prior-term entry \
             advanced commit_index to {} by count alone (the Figure 8 data-loss bug)",
            sim.node(1).commit_index
        );
        assert!(
            sim.registry().is_empty(),
            "seed={seed}: nothing may be registered committed yet"
        );

        // Resume S1's clock alone: the next heartbeat ships its term-4 NoOp
        // to S3, and THAT commit — a current-term majority — carries the
        // term-2 entry with it (R19b's 'committed as a side effect').
        let committed = (0..100).any(|_| {
            sim.run_ms_ticking_only(&[1], 10);
            sim.node(1).commit_index >= 2
        });
        assert!(
            committed,
            "seed={seed}: idx 2 must commit once the term-4 NoOp replicates"
        );
        assert_eq!(
            sim.node(1).commit_index,
            3,
            "seed={seed}: the term-4 NoOp (idx 3) is what advanced commitment"
        );
        assert_eq!(
            sim.registry().get(2),
            Some(&e2),
            "seed={seed}: the term-2 entry commits only under the term-4 entry"
        );
    }
}

#[test]
fn soak_replication_20_seeds() {
    for seed in 0..20 {
        let mut sim = Sim::new(3, seed);
        sim.faults.drop_prob = 0.1;
        // The proposal schedule is seeded and independent of the sim's RNG.
        let mut rng = raft_core::rng::Pcg32::new(seed ^ 0x5EED_CAFE);
        for _ in 0..300 {
            sim.run_ms(100);
            let node = u8::try_from(rng.range_inclusive(1, 3)).expect("node id");
            let key = format!("k{}", rng.range_inclusive(0, 8));
            if rng.range_inclusive(0, 3) == 0 {
                sim.client_delete(node, &key);
            } else {
                sim.client_put(node, &key, &format!("v{}", rng.next_u32()));
            }
        }
        // Fault-free cooldown, then full convergence.
        sim.faults.drop_prob = 0.0;
        sim.run_ms(5_000);
        for id in 2..=3 {
            assert_eq!(
                sim.node(1).log,
                sim.node(id).log,
                "seed={seed}: logs did not converge after cooldown"
            );
            assert_eq!(
                sim.applied(1),
                sim.applied(id),
                "seed={seed}: applied histories did not converge after cooldown"
            );
        }
        assert!(
            !sim.registry().is_empty(),
            "seed={seed}: 30s of proposals must commit something"
        );
        for id in 1..=3 {
            let log = &sim.node(id).log;
            for index in 1..=sim.registry().len() as u64 {
                let registered = sim.registry().get(index);
                assert_eq!(
                    registered,
                    log.get(usize::try_from(index - 1).expect("fits")),
                    "seed={seed}: committed entry {index} missing/differs on n{id} \
                     after convergence"
                );
            }
        }
    }
}
