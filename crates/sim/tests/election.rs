//! Leader-election acceptance tests under the deterministic simulator.
//! Every failure message carries its seed.

use raft_core::{Command, Entry, HardState, LogIndex, RaftMessage, Role, Term};
use sim::Sim;

fn noop_entries(
    range: std::ops::RangeInclusive<u64>,
    term_of: impl Fn(LogIndex) -> Term,
) -> Vec<Entry> {
    range
        .map(|index| Entry {
            index,
            term: term_of(index),
            command: Command::NoOp,
        })
        .collect()
}

#[test]
fn elects_leader_from_cold_start() {
    for seed in 0..100 {
        let mut sim = Sim::new(3, seed);
        let elected = sim.run_until(|s| s.leader().is_some(), 1_000);
        assert!(elected, "seed={seed}: no leader within 1000 virtual ms");
    }
}

#[test]
fn single_leader_per_term_100_seeds() {
    for seed in 0..100 {
        let mut sim = Sim::new(3, seed);
        let elected = sim.run_until(|s| s.leader().is_some(), 1_000);
        assert!(elected, "seed={seed}: no leader within 1000 virtual ms");
        // ElectionSafety is asserted inside every step; 10,000 further ms of
        // steady state give it every chance to fire.
        sim.run_ms(10_000);
        assert!(
            !sim.leaders_by_term().is_empty(),
            "seed={seed}: no leadership was ever observed"
        );
    }
}

#[test]
fn split_vote_resolves() {
    for seed in 0..50 {
        let mut sim = Sim::new(3, seed);
        // Black out the network for one full initial timeout window: every
        // node times out and campaigns into the void — a forced split.
        sim.faults.drop_prob = 1.0;
        sim.run_ms(310);
        for id in 1..=3 {
            assert!(
                sim.node(id).hard.current_term >= 1,
                "seed={seed}: n{id} never campaigned during the blackout"
            );
        }
        // Heal. Randomized timeouts must desynchronize the candidates.
        sim.faults.drop_prob = 0.0;
        let elected = sim.run_until(|s| s.leader().is_some(), 2_000);
        assert!(
            elected,
            "seed={seed}: split vote did not resolve within 2000 ms after heal"
        );
    }
}

#[test]
fn stale_log_candidate_rejected() {
    for seed in 0..10 {
        let mut sim = Sim::new(3, seed);
        // Node 1: entries to index 3, term 2. Nodes 2–3: entries to index 5,
        // last term 3 — strictly more up-to-date.
        sim.seed_log(1, noop_entries(1..=3, |_| 2));
        for id in [2, 3] {
            sim.seed_log(id, noop_entries(1..=5, |i| if i <= 3 { 2 } else { 3 }));
        }
        for id in 1..=3 {
            sim.seed_hard_state(
                id,
                HardState {
                    current_term: 3,
                    voted_for: None,
                },
            );
        }
        // Force node 1 to campaign: only its clock runs; 2 and 3 still
        // receive and answer its solicitations (rejecting on R9b).
        sim.run_ms_ticking_only(&[1], 700);
        assert!(
            sim.node(1).hard.current_term > 3,
            "seed={seed}: node 1 never campaigned"
        );
        assert!(
            !sim.node(1).is_leader(),
            "seed={seed}: stale-log candidate must never win"
        );
        // Unfreeze everyone: a node holding the newer log must win instead.
        let elected = sim.run_until(|s| s.leader().is_some_and(|l| l != 1), 2_000);
        assert!(
            elected,
            "seed={seed}: no up-to-date node won after the stale candidate was rejected"
        );
    }
}

#[test]
fn higher_term_causes_stepdown() {
    let seed = 7;
    let mut sim = Sim::new(3, seed);
    assert!(
        sim.run_until(|s| s.leader().is_some(), 1_000),
        "seed={seed}: no leader within 1000 virtual ms"
    );
    let leader = sim.leader().expect("just elected");
    let other = *sim.node(leader).peers.first().expect("has peers");
    let t5 = sim.node(leader).hard.current_term + 3;

    // Any message carrying a newer term must dethrone the leader (R2) —
    // here a RequestVote whose empty-log candidacy it can even vote for.
    let effects = sim.deliver_now(
        other,
        leader,
        RaftMessage::RequestVote {
            term: t5,
            candidate_id: other,
            last_log_index: sim.node(other).log.last().map_or(0, |e| e.index),
            last_log_term: sim.node(other).log.last().map_or(0, |e| e.term),
        },
    );

    assert_eq!(
        sim.node(leader).hard.current_term,
        t5,
        "seed={seed}: higher term must be adopted"
    );
    assert!(
        matches!(sim.node(leader).role, Role::Follower),
        "seed={seed}: leader must step down to Follower"
    );
    let persist_at = effects
        .iter()
        .position(|e| matches!(e, raft_core::Effect::PersistHardState(_)));
    let first_send_at = effects
        .iter()
        .position(|e| matches!(e, raft_core::Effect::Send { .. }));
    assert!(
        persist_at.is_some(),
        "seed={seed}: R2 must persist the adopted term"
    );
    if let (Some(p), Some(s)) = (persist_at, first_send_at) {
        assert!(
            p < s,
            "seed={seed}: PersistHardState must be emitted before any reply Send"
        );
    }
}

#[test]
fn rejected_vote_does_not_reset_timer() {
    let seed = 11;
    let mut sim = Sim::new(3, seed);
    // Node 1 gets a stale log so nodes 2–3 must refuse its votes (R9b).
    sim.seed_log(1, noop_entries(1..=3, |_| 2));
    for id in [2, 3] {
        sim.seed_log(id, noop_entries(1..=5, |i| if i <= 3 { 2 } else { 3 }));
    }
    for id in 1..=3 {
        sim.seed_hard_state(
            id,
            HardState {
                current_term: 3,
                voted_for: None,
            },
        );
    }
    // One global step so every node draws its initial election deadline.
    sim.run_ms(raft_core::TICK_MS);
    let deadline_2 = sim.node(2).election_deadline();
    let deadline_3 = sim.node(3).election_deadline();
    assert!(
        deadline_2 > 0 && deadline_3 > 0,
        "seed={seed}: deadlines drawn"
    );

    // Only node 1's clock runs; it campaigns (repeatedly). 2 and 3 receive
    // the solicitations while frozen and refuse them.
    sim.run_ms_ticking_only(&[1], 700);
    assert!(
        sim.node(2).hard.current_term > 3,
        "seed={seed}: node 2 never saw a solicitation"
    );
    assert_eq!(
        sim.node(2).hard.voted_for,
        None,
        "seed={seed}: node 2 must have refused (stale candidate log)"
    );
    // R6: the refusals must not have touched their election schedules.
    assert_eq!(
        sim.node(2).election_deadline(),
        deadline_2,
        "seed={seed}: R6 violated — node 2's timer was reset by a rejected vote"
    );
    assert_eq!(
        sim.node(3).election_deadline(),
        deadline_3,
        "seed={seed}: R6 violated — node 3's timer was reset by a rejected vote"
    );
}

#[test]
fn no_election_while_heartbeats_flow() {
    let seed = 5;
    let mut sim = Sim::new(3, seed);
    assert!(
        sim.run_until(|s| s.leader().is_some(), 1_000),
        "seed={seed}: no leader within 1000 virtual ms"
    );
    let leader = sim.leader().expect("just elected");
    let term = sim.node(leader).hard.current_term;
    // Healthy heartbeats, zero drops, for 60,000 virtual ms.
    sim.run_ms(60_000);
    assert_eq!(
        sim.leader(),
        Some(leader),
        "seed={seed}: leadership must be stable under healthy heartbeats"
    );
    for id in 1..=3 {
        assert_eq!(
            sim.node(id).hard.current_term,
            term,
            "seed={seed}: n{id}'s term changed — an election happened despite heartbeats"
        );
    }
}
