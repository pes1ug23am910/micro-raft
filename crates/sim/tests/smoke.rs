//! M0 placeholder: proves `sim` links against `raft-core` and pins the R1 boot
//! state (volatile indices start at 0; a fresh node is a Follower at term 0).
//! The real simulator arrives in M3.

use raft_core::{HardState, RaftNode, Role};

#[test]
fn sim_crate_links_against_raft_core() {
    let node = RaftNode::new(1, vec![2, 3], 42);
    assert_eq!(node.id, 1);
    assert_eq!(node.peers, vec![2, 3]);
    assert_eq!(
        node.hard,
        HardState {
            current_term: 0,
            voted_for: None
        }
    );
    assert_eq!(node.role, Role::Follower);
    assert_eq!(node.commit_index, 0);
    assert_eq!(node.last_applied, 0);
    assert!(node.log.is_empty());
}
