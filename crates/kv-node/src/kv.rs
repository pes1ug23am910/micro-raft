//! Applied key-value state and the read-only node snapshot shared with HTTP.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use raft_core::{Command, Entry, LogIndex, NodeId, RaftNode, Role, Term};
use serde::Serialize;

/// The externally visible role of a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

impl NodeRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Follower => "follower",
            Self::Candidate => "candidate",
            Self::Leader => "leader",
        }
    }
}

impl From<&Role> for NodeRole {
    fn from(role: &Role) -> Self {
        match role {
            Role::Follower => Self::Follower,
            Role::Candidate { .. } => Self::Candidate,
            Role::Leader { .. } => Self::Leader,
        }
    }
}

/// A point-in-time status view returned by `GET /status`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeStatus {
    pub node_id: NodeId,
    pub role: NodeRole,
    pub term: Term,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub leader_hint: Option<NodeId>,
}

/// A consistent read snapshot used by the HTTP handlers.
#[derive(Clone, Debug)]
pub struct ReadSnapshot {
    pub status: NodeStatus,
    pub values: BTreeMap<String, String>,
}

#[derive(Debug)]
struct ReadState {
    status: NodeStatus,
    values: BTreeMap<String, String>,
}

/// Cheaply cloneable state shared between the single-writer driver and HTTP.
#[derive(Clone, Debug)]
pub struct SharedReadState {
    inner: Arc<RwLock<ReadState>>,
}

impl SharedReadState {
    pub fn from_node(node: &RaftNode) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ReadState {
                status: status_from_node(node, None),
                values: BTreeMap::new(),
            })),
        }
    }

    pub fn snapshot(&self) -> ReadSnapshot {
        let state = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ReadSnapshot {
            status: state.status.clone(),
            values: state.values.clone(),
        }
    }

    pub fn status(&self) -> NodeStatus {
        self.inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .status
            .clone()
    }

    /// Reads one value and its status watermark under the same lock.
    pub fn get(&self, key: &str) -> (NodeStatus, Option<String>) {
        let state = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (state.status.clone(), state.values.get(key).cloned())
    }

    pub(crate) fn apply(&self, entry: &Entry) {
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match &entry.command {
            Command::Put { key, value } => {
                state.values.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                state.values.remove(key);
            }
            Command::NoOp => {}
        }
        // Keep the value snapshot and its staleness watermark atomic for GET.
        state.status.last_applied = entry.index;
        state.status.commit_index = state.status.commit_index.max(entry.index);
    }

    pub(crate) fn refresh_status(&self, node: &RaftNode, last_known_leader: Option<NodeId>) {
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.status = status_from_node(node, last_known_leader);
    }
}

fn status_from_node(node: &RaftNode, last_known_leader: Option<NodeId>) -> NodeStatus {
    let role = NodeRole::from(&node.role);
    NodeStatus {
        node_id: node.id,
        role,
        term: node.hard.current_term,
        commit_index: node.commit_index,
        last_applied: node.last_applied,
        leader_hint: if role == NodeRole::Leader {
            Some(node.id)
        } else {
            last_known_leader
        },
    }
}

#[cfg(test)]
mod tests {
    use raft_core::{Command, Entry, RaftNode};

    use super::*;

    #[test]
    fn commands_update_the_local_state_machine() {
        let node = RaftNode::new(1, vec![2, 3], 7);
        let state = SharedReadState::from_node(&node);

        state.apply(&Entry {
            index: 1,
            term: 1,
            command: Command::Put {
                key: "answer".into(),
                value: "42".into(),
            },
        });
        let snapshot = state.snapshot();
        assert_eq!(snapshot.values.get("answer"), Some(&"42".into()));
        assert_eq!(snapshot.status.last_applied, 1);
        assert_eq!(snapshot.status.commit_index, 1);

        state.apply(&Entry {
            index: 2,
            term: 1,
            command: Command::Delete {
                key: "answer".into(),
            },
        });
        assert!(!state.snapshot().values.contains_key("answer"));
    }
}
