//! Explicit message schedules for term-aware log repair and delayed replies.

use std::collections::BTreeSet;

use raft_core::{
    AppendConflictHint, Command, Effect, Entry, HardState, Input, LogIndex, RaftMessage, RaftNode,
    Role, Term, HEARTBEAT_MS, MAX_APPEND_ENTRIES,
};

fn log(terms: &[Term]) -> Vec<Entry> {
    terms
        .iter()
        .enumerate()
        .map(|(offset, &term)| Entry {
            index: offset as LogIndex + 1,
            term,
            command: Command::NoOp,
        })
        .collect()
}

fn append_request(effects: Vec<Effect>) -> RaftMessage {
    let mut requests = effects.into_iter().filter_map(|effect| match effect {
        Effect::Send {
            to: 2,
            msg: msg @ RaftMessage::AppendEntries { .. },
        } => Some(msg),
        _ => None,
    });
    let request = requests.next().expect("one append to the follower");
    assert!(requests.next().is_none(), "only one append per heartbeat");
    request
}

fn reply(follower: &mut RaftNode, request: RaftMessage) -> RaftMessage {
    let mut replies = follower
        .step(Input::Message {
            from: 1,
            msg: request,
        })
        .into_iter()
        .filter_map(|effect| match effect {
            Effect::Send {
                to: 1,
                msg: msg @ RaftMessage::AppendEntriesReply { .. },
            } => Some(msg),
            _ => None,
        });
    let response = replies.next().expect("follower replies to append");
    assert!(replies.next().is_none(), "only one append reply");
    response
}

fn progress(leader: &RaftNode) -> (LogIndex, LogIndex) {
    let Role::Leader {
        next_index,
        match_index,
    } = &leader.role
    else {
        panic!("responsive follower must keep the leader in office");
    };
    (next_index[&2], match_index[&2])
}

/// Seed a reachable election boundary, then obtain a real follower vote. The
/// leader's newer last term makes it eligible despite the divergent suffix.
fn elect(leader_terms: &[Term], follower_terms: &[Term]) -> (RaftNode, RaftNode, RaftMessage) {
    let hard = HardState {
        current_term: 5,
        voted_for: None,
        membership: None,
    };
    let mut leader = RaftNode::restore(1, vec![2], 1, hard.clone(), log(leader_terms))
        .expect("valid leader history");
    let mut follower = RaftNode::restore(2, vec![1], 2, hard, log(follower_terms))
        .expect("valid follower history");
    leader.hard.voted_for = Some(1);
    leader.role = Role::Candidate {
        votes_received: BTreeSet::from([1]),
    };
    let vote = follower
        .step(Input::Message {
            from: 1,
            msg: RaftMessage::RequestVote {
                term: 5,
                candidate_id: 1,
                last_log_index: leader_terms.len() as LogIndex,
                last_log_term: *leader_terms.last().expect("nonempty leader log"),
            },
        })
        .into_iter()
        .find_map(|effect| match effect {
            Effect::Send {
                to: 1,
                msg: msg @ RaftMessage::RequestVoteReply { .. },
            } => Some(msg),
            _ => None,
        })
        .expect("follower's election reply");
    assert!(matches!(
        vote,
        RaftMessage::RequestVoteReply {
            vote_granted: true,
            ..
        }
    ));
    let initial = append_request(leader.step(Input::Message { from: 2, msg: vote }));
    (leader, follower, initial)
}

fn divergent_histories(suffix_len: usize, follower_term: Term) -> (Vec<Term>, Vec<Term>) {
    let mut leader = vec![1, 1, 1, 2, 2];
    let mut follower = leader.clone();
    leader.extend(std::iter::repeat_n(4, suffix_len));
    follower.extend(std::iter::repeat_n(follower_term, suffix_len));
    (leader, follower)
}

/// Count rejected probes separately from successful, bounded catch-up batches.
/// Clearing only the optional hint emulates an older follower's reply format.
fn repair(suffix_len: usize, follower_term: Term, legacy: bool) -> (usize, usize) {
    let (leader_terms, follower_terms) = divergent_histories(suffix_len, follower_term);
    let (mut leader, mut follower, mut request) = elect(&leader_terms, &follower_terms);
    let mut rejected = 0;
    let mut successful = 0;
    let mut now_ms = 0;
    let final_index = leader.log.len() as LogIndex;
    let max_rounds = suffix_len + (suffix_len + 1).div_ceil(MAX_APPEND_ENTRIES) + 2;

    for _ in 0..max_rounds {
        let mut response = reply(&mut follower, request);
        let RaftMessage::AppendEntriesReply {
            success, conflict, ..
        } = &mut response
        else {
            unreachable!("reply helper only returns AppendEntriesReply");
        };
        if *success {
            successful += 1;
            assert!(conflict.is_none(), "success must carry no conflict hint");
        } else {
            rejected += 1;
            assert!(conflict.is_some(), "actual mismatches carry a hint");
        }
        if legacy {
            *conflict = None;
        }
        leader.step(Input::Message {
            from: 2,
            msg: response,
        });
        if progress(&leader).1 == final_index {
            assert_eq!(
                follower.log, leader.log,
                "repair must replace the entire suffix"
            );
            assert_eq!(
                leader.commit_index, final_index,
                "current-term no-op commits"
            );
            now_ms += HEARTBEAT_MS;
            let commit = append_request(leader.step(Input::Tick { now_ms }));
            let response = reply(&mut follower, commit);
            leader.step(Input::Message {
                from: 2,
                msg: response,
            });
            assert_eq!(follower.commit_index, final_index);
            return (rejected, successful);
        }
        now_ms += HEARTBEAT_MS;
        request = append_request(leader.step(Input::Tick { now_ms }));
    }
    panic!("repair did not converge: suffix={suffix_len}, term={follower_term}, legacy={legacy}");
}

#[test]
fn leader_skips_to_its_last_entry_in_the_conflicting_term() {
    for suffix_len in [1_usize, 64, 257] {
        let batches = (suffix_len + 1).div_ceil(MAX_APPEND_ENTRIES);
        assert_eq!(repair(suffix_len, 2, false), (1, batches));
        assert_eq!(repair(suffix_len, 2, true), (suffix_len, batches));
    }
}

#[test]
fn absent_conflicting_term_skips_to_followers_first_index() {
    for suffix_len in [1_usize, 64, 257] {
        let batches = (suffix_len + 1).div_ceil(MAX_APPEND_ENTRIES);
        assert_eq!(repair(suffix_len, 3, false), (1, batches));
        assert_eq!(repair(suffix_len, 3, true), (suffix_len, batches));
    }
}

#[test]
fn missing_anchor_skips_directly_to_the_followers_log_end() {
    let (leader_terms, _) = divergent_histories(64, 3);
    let follower_terms = &leader_terms[..3];
    let (mut leader, mut follower, initial) = elect(&leader_terms, follower_terms);
    let response = reply(&mut follower, initial);
    assert!(matches!(
        response,
        RaftMessage::AppendEntriesReply {
            success: false,
            conflict: Some(AppendConflictHint {
                rejected_index: 69,
                term: None,
                first_index: 4,
            }),
            ..
        }
    ));
    leader.step(Input::Message {
        from: 2,
        msg: response,
    });
    assert_eq!(progress(&leader), (4, 0));
    let retry = append_request(leader.step(Input::Tick {
        now_ms: HEARTBEAT_MS,
    }));
    assert!(matches!(
        retry,
        RaftMessage::AppendEntries {
            prev_log_index: 3,
            ..
        }
    ));
    assert!(matches!(
        reply(&mut follower, retry),
        RaftMessage::AppendEntriesReply { success: true, .. }
    ));
}

#[test]
fn delayed_rejection_cannot_undo_successful_catch_up() {
    for legacy in [false, true] {
        let (leader_terms, follower_terms) = divergent_histories(64, 2);
        let (mut leader, mut follower, initial) = elect(&leader_terms, &follower_terms);
        let mut delayed = reply(&mut follower, initial);
        assert!(matches!(
            delayed,
            RaftMessage::AppendEntriesReply { success: false, .. }
        ));
        leader.step(Input::Message {
            from: 2,
            msg: delayed.clone(),
        });
        let retry = append_request(leader.step(Input::Tick {
            now_ms: HEARTBEAT_MS,
        }));
        let accepted = reply(&mut follower, retry);
        assert!(matches!(
            accepted,
            RaftMessage::AppendEntriesReply { success: true, .. }
        ));
        leader.step(Input::Message {
            from: 2,
            msg: accepted,
        });
        let proven = progress(&leader);
        assert_eq!(proven, (22, 21), "first batch proves the repaired prefix");
        if legacy {
            if let RaftMessage::AppendEntriesReply { conflict, .. } = &mut delayed {
                *conflict = None;
            }
        }
        leader.step(Input::Message {
            from: 2,
            msg: delayed,
        });
        assert_eq!(
            progress(&leader),
            proven,
            "delayed failure crossed the match floor"
        );
        let next = append_request(leader.step(Input::Tick {
            now_ms: 2 * HEARTBEAT_MS,
        }));
        assert!(matches!(
            next,
            RaftMessage::AppendEntries {
                prev_log_index: 21,
                ..
            }
        ));
        assert!(matches!(
            reply(&mut follower, next),
            RaftMessage::AppendEntriesReply { success: true, .. }
        ));
    }
}
