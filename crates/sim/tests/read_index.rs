//! Independent read-barrier witness checks and deterministic isolation schedules.
use raft_core::{Effect, NodeId, RaftMessage, ReadRejectReason};
use sim::Sim;
use std::collections::BTreeSet;

#[derive(Debug, PartialEq)]
enum InvalidProof {
    WrongRequest,
    WrongContext,
    WrongTerm,
    WrongIndex,
    NoQuorum,
}

struct Proof {
    request_id: u64,
    context: u64,
    term: u64,
    index: u64,
    members: BTreeSet<NodeId>,
    witnessed: BTreeSet<NodeId>,
}

impl Proof {
    // This oracle uses the independently witnessed request/reply trace, never
    // the core's private pending-read state or its membership tally.
    fn observe_reply(&mut self, from: NodeId, msg: &RaftMessage) {
        if let RaftMessage::ReadProbeReply {
            term,
            context,
            accepted: true,
        } = msg
        {
            if *term == self.term && *context == self.context && self.members.contains(&from) {
                self.witnessed.insert(from);
            }
        }
    }

    fn check(&self, effect: &Effect) -> Result<(), InvalidProof> {
        let Effect::ReadReady {
            request_id,
            context,
            term,
            index,
        } = effect
        else {
            panic!("expected read completion");
        };
        if *request_id != self.request_id {
            return Err(InvalidProof::WrongRequest);
        }
        if *context != self.context {
            return Err(InvalidProof::WrongContext);
        }
        if *term != self.term {
            return Err(InvalidProof::WrongTerm);
        }
        if *index != self.index {
            return Err(InvalidProof::WrongIndex);
        }
        if self.witnessed.len() <= self.members.len() / 2 {
            return Err(InvalidProof::NoQuorum);
        }
        Ok(())
    }
}

fn probe(effects: &[Effect]) -> RaftMessage {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Send {
                msg: msg @ RaftMessage::ReadProbe { .. },
                ..
            } => Some(msg.clone()),
            _ => None,
        })
        .expect("admitted read emitted a probe")
}

#[test]
fn independent_proof_rejects_forged_callbacks_and_validates_actual_quorum() {
    let mut sim = Sim::new(5, 300);
    assert!(sim.run_until(
        |s| s.leader().is_some_and(|id| s.node(id).commit_index > 0),
        2_000
    ));
    sim.run_ms(100);
    let leader = sim.leader().unwrap();
    let before_log = sim.node(leader).log.clone();
    let peers = sim.node(leader).peers.clone();
    sim.faults.drop_prob = 1.0;
    let request = probe(&sim.read_index(leader, 91));
    let RaftMessage::ReadProbe { term, context, .. } = request else {
        unreachable!()
    };
    let mut proof = Proof {
        request_id: 91,
        context,
        term,
        index: sim.node(leader).commit_index,
        members: (1..=5).collect(),
        witnessed: BTreeSet::from([leader]),
    };
    let forged = Effect::ReadReady {
        request_id: 91,
        context,
        term,
        index: proof.index,
    };
    assert_eq!(
        proof.check(&forged),
        Err(InvalidProof::NoQuorum),
        "planted no-quorum success must be detected"
    );
    assert!(sim.take_read_outcomes().is_empty());

    // Wrong context, duplicates and nonmembers do not change either witness.
    for (from, got) in [(peers[0], context + 1), (99, context)] {
        let msg = RaftMessage::ReadProbeReply {
            term,
            context: got,
            accepted: true,
        };
        proof.observe_reply(from, &msg);
        sim.deliver_now(from, leader, msg);
    }
    assert_eq!(proof.check(&forged), Err(InvalidProof::NoQuorum));
    assert!(sim.take_read_outcomes().is_empty());

    for (offset, peer) in peers.iter().take(2).copied().enumerate() {
        let actual = sim
            .deliver_now(leader, peer, request.clone())
            .into_iter()
            .find_map(|effect| match effect {
                Effect::Send {
                    msg: msg @ RaftMessage::ReadProbeReply { .. },
                    ..
                } => Some(msg),
                _ => None,
            })
            .unwrap();
        proof.observe_reply(peer, &actual);
        sim.deliver_now(peer, leader, actual.clone());
        sim.deliver_now(peer, leader, actual); // duplicated actual delivery
        let outcomes = sim.take_read_outcomes();
        if offset == 0 {
            assert!(
                outcomes.is_empty(),
                "self+one peer is not a majority of five"
            );
            assert_eq!(proof.check(&forged), Err(InvalidProof::NoQuorum));
        } else {
            assert_eq!(outcomes.len(), 1, "one terminal callback per read");
            assert_eq!(proof.check(&outcomes[0].1), Ok(()));
        }
    }
    let wrong = Effect::ReadReady {
        request_id: 91,
        context: context + 1,
        term,
        index: proof.index,
    };
    assert_eq!(
        proof.check(&wrong),
        Err(InvalidProof::WrongContext),
        "planted wrong-context success must be detected even with quorum"
    );
    assert_eq!(
        sim.node(leader).log,
        before_log,
        "read barriers append no log entries"
    );
}

#[test]
fn isolated_former_leader_cannot_return_new_read_and_majority_fence_covers_write() {
    for seed in 0..30 {
        let mut sim = Sim::new(3, seed);
        assert!(sim.run_until(
            |s| s.leader().is_some_and(|id| s.node(id).commit_index > 0),
            2_000
        ));
        sim.run_ms(100);
        let old = sim.leader().unwrap();
        sim.set_partitions(vec![BTreeSet::from([old])]);
        let request = probe(&sim.read_index(old, 1));
        let RaftMessage::ReadProbe {
            term: old_term,
            context: old_context,
            ..
        } = request
        else {
            unreachable!()
        };
        sim.run_ms(700);
        let outcomes = sim.take_read_outcomes();
        assert!(
            outcomes.iter().any(|(id, e)| *id == old
                && matches!(
                    e,
                    Effect::ReadRejected {
                        request_id: 1,
                        reason: ReadRejectReason::LeadershipLost,
                        ..
                    }
                )),
            "seed={seed}: old read not cancelled"
        );
        assert!(
            !outcomes
                .iter()
                .any(|(_, e)| matches!(e, Effect::ReadReady { .. })),
            "seed={seed}: isolated read succeeded"
        );
        let peer = sim.node(old).peers[0];
        sim.deliver_now(
            peer,
            old,
            RaftMessage::ReadProbeReply {
                term: old_term,
                context: old_context,
                accepted: true,
            },
        );
        assert!(
            sim.take_read_outcomes().is_empty(),
            "seed={seed}: delayed callback revived cancelled read"
        );
        assert!(sim.run_until(
            |s| s
                .leader()
                .is_some_and(|id| id != old && s.node(id).commit_index > 0),
            2_000
        ));
        let current = sim.leader().unwrap();
        let effects = sim.client_put(current, "before-read", "committed");
        let index = effects
            .iter()
            .find_map(|e| {
                if let Effect::ProposeAccepted { index } = e {
                    Some(*index)
                } else {
                    None
                }
            })
            .unwrap();
        assert!(sim.run_until(|s| s.node(current).last_applied >= index, 1_000));
        sim.read_index(current, 2);
        sim.run_ms(100);
        let outcomes = sim.take_read_outcomes();
        assert!(outcomes.iter().any(|(id,e)| *id == current && matches!(e, Effect::ReadReady { request_id: 2, index: fence, term, .. } if *fence >= index && *term == sim.node(current).hard.current_term)), "seed={seed}: majority read missing fence");
    }
}

#[test]
fn higher_term_append_cancels_reads_before_repair_without_violating_durability_audit() {
    let mut sim = Sim::new(3, 77);
    assert!(sim.run_until(
        |s| s.leader().is_some_and(|id| s.node(id).commit_index > 0),
        2_000
    ));
    sim.run_ms(100);
    let old = sim.leader().unwrap();
    let peer = sim.node(old).peers[0];
    sim.faults.drop_prob = 1.0;
    sim.read_index(old, 55);
    let term = sim.node(old).hard.current_term + 1;
    let last = sim.node(old).log.last().unwrap().clone();
    let effects = sim.deliver_now(
        peer,
        old,
        RaftMessage::AppendEntries {
            term,
            leader_id: peer,
            prev_log_index: last.index,
            prev_log_term: last.term,
            entries: vec![raft_core::Entry {
                index: last.index + 1,
                term,
                command: raft_core::Command::NoOp,
            }],
            leader_commit: last.index,
            contact_round: 1,
        },
    );
    assert!(matches!(effects.first(), Some(Effect::PersistHardState(_))));
    assert_eq!(sim.durable_hard_state(old).current_term, term);
    let outcomes = sim.take_read_outcomes();
    assert!(
        matches!(outcomes.as_slice(), [(id, Effect::ReadRejected { request_id: 55, reason: ReadRejectReason::LeadershipLost, .. })] if *id == old)
    );
}
