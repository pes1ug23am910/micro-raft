//! Applied key-value state and the read-only node snapshot shared with HTTP.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, RwLock};

use raft_core::{Entry, LogIndex, NodeId, RaftNode, Role, Term};
use serde::Serialize;

use crate::application::{ApplyError, ApplyOutcome, StateMachine};
use crate::snapshot::{SnapshotImage, SnapshotMetadata};

/// The externally visible role of a node.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeRole {
    Follower,
    Candidate,
    #[serde(rename = "pre_candidate")]
    PreCandidate,
    Leader,
}

impl NodeRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Follower => "follower",
            Self::Candidate => "candidate",
            Self::PreCandidate => "pre_candidate",
            Self::Leader => "leader",
        }
    }
}

impl From<&Role> for NodeRole {
    fn from(role: &Role) -> Self {
        match role {
            Role::Follower => Self::Follower,
            Role::Candidate { .. } => Self::Candidate,
            Role::PreCandidate { .. } => Self::PreCandidate,
            Role::Leader { .. } => Self::Leader,
        }
    }
}

/// A point-in-time status view returned by `GET /status`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct NodeStatus {
    pub committed_membership: raft_core::membership::CommittedMembership,
    pub effective_membership: raft_core::membership::MembershipState,
    pub node_id: NodeId,
    pub role: NodeRole,
    pub term: Term,
    pub commit_index: LogIndex,
    pub last_applied: LogIndex,
    pub leader_hint: Option<NodeId>,
    pub snapshot_index: LogIndex,
    pub snapshot_term: Term,
    pub retained_log_entries: usize,
    pub snapshot_threshold: u64,
    pub snapshot_error: Option<String>,
    pub batching: crate::driver::BatchingStatus,
    pub storage: crate::storage::StorageMetrics,
    pub applied_store: Option<crate::applied_store::AppliedStoreStatus>,
    pub route_error: Option<String>,
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
    application: StateMachine,
}

/// Cheaply cloneable state shared between the single-writer driver and HTTP.
#[derive(Clone, Debug)]
pub struct SharedReadState {
    inner: Arc<RwLock<ReadState>>,
}

impl SharedReadState {
    /// Restore only after validating the engine's cells against Raft recovery.
    pub fn from_application(node: &RaftNode, application: StateMachine) -> io::Result<Self> {
        if application.last_applied() != node.last_applied || node.last_applied > node.commit_index
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "engine and core application watermark differ",
            ));
        }
        Ok(Self {
            inner: Arc::new(RwLock::new(ReadState {
                status: status_from_node(node, None, application.last_applied()),
                application,
            })),
        })
    }

    /// One committed prefix becomes visible only after one atomic engine barrier.
    /// Holding the write lock prevents any provisional application read. The undo
    /// guard restores every touched component on error before releasing that lock.
    pub(crate) fn apply_group(
        &self,
        entries: &[Entry],
        store: &mut crate::applied_store::AppliedStore,
    ) -> io::Result<Vec<(LogIndex, ApplyOutcome)>> {
        if entries.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "empty applied range",
            ));
        }
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut transaction = state.application.transaction();
        let mut cells = BTreeMap::new();
        let mut bytes = 0;
        let mut outcomes = Vec::new();
        let mut term = 0;
        for entry in entries.iter().take(state_store::MAX_APPLY_RANGE as usize) {
            let outcome = transaction
                .apply(entry)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            let changes = store.cells(transaction.state(), entry)?;
            if !crate::applied_store::merge_cells(&mut cells, &mut bytes, changes)? {
                transaction.undo_last();
                if outcomes.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "one applied entry exceeds engine transaction budget",
                    ));
                }
                break;
            }
            outcomes.push((entry.index, outcome));
            term = entry.term;
        }
        let last = outcomes
            .last()
            .expect("a nonempty applied range was prepared")
            .0;
        let changes: Vec<_> = cells
            .into_iter()
            .map(|(key, value)| state_store::Mutation { key, value })
            .collect();
        store.commit(entries[0].index, last, term, &changes)?;
        transaction.commit();
        state.status.last_applied = last;
        state.status.commit_index = state.status.commit_index.max(last);
        state.status.applied_store = Some(store.status()?);
        Ok(outcomes)
    }

    pub(crate) fn applied_store_status(&self, status: crate::applied_store::AppliedStoreStatus) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .status
            .applied_store = Some(status);
    }

    pub fn from_node(node: &RaftNode) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ReadState {
                status: status_from_node(node, None, 0),
                application: StateMachine::default(),
            })),
        }
    }

    /// Restores the application only after the core validated the same durable boundary.
    pub fn from_snapshot(node: &RaftNode, image: SnapshotImage) -> io::Result<Self> {
        let boundary = &image.descriptor().metadata;
        if boundary.last_included_index != node.last_applied
            || boundary.last_included_index > node.commit_index
            || boundary.last_included_term > node.hard.current_term
            || boundary.last_included_term != node.snapshot_term()
            || node.snapshot_descriptor() != Some(image.descriptor())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "application snapshot and restored consensus boundary differ",
            ));
        }
        let shared = Self::from_node(node);
        shared.install_snapshot(image)?;
        Ok(shared)
    }

    /// The image's application and watermark come from one lock acquisition.
    pub(crate) fn snapshot_image(&self, metadata: SnapshotMetadata) -> io::Result<SnapshotImage> {
        let state = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        SnapshotImage::new(metadata, &state.application)
    }

    /// A published image can only move application state forward. Transfer
    /// duplicates are acknowledged by the protocol without reapplying the image.
    pub(crate) fn install_snapshot(&self, image: SnapshotImage) -> io::Result<()> {
        let boundary = image.descriptor().metadata.last_included_index;
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if boundary <= state.application.last_applied() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "snapshot installation must advance the application boundary",
            ));
        }
        state.application = image.into_application();
        state.status.last_applied = boundary;
        state.status.commit_index = state.status.commit_index.max(boundary);
        Ok(())
    }
    pub fn snapshot(&self) -> ReadSnapshot {
        let state = self
            .inner
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ReadSnapshot {
            status: state.status.clone(),
            values: state.application.values().clone(),
        }
    }

    pub fn with_route_error(self, error: Option<String>) -> Self {
        self.route_error(error);
        self
    }

    pub(crate) fn route_error(&self, error: Option<String>) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .status
            .route_error = error;
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
        (
            state.status.clone(),
            state.application.values().get(key).cloned(),
        )
    }

    pub(crate) fn apply(&self, entry: &Entry) -> Result<ApplyOutcome, ApplyError> {
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let outcome = state.application.apply(entry)?;
        // Values, retry records, and the read watermark change under one lock.
        state.status.last_applied = entry.index;
        state.status.commit_index = state.status.commit_index.max(entry.index);
        Ok(outcome)
    }

    pub(crate) fn update_batching(&self, update: impl FnOnce(&mut crate::driver::BatchingStatus)) {
        update(
            &mut self
                .inner
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .status
                .batching,
        );
    }
    pub(crate) fn storage_metrics(&self, metrics: crate::storage::StorageMetrics) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .status
            .storage = metrics;
    }

    pub(crate) fn configure_snapshots(&self, threshold: u64) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .status
            .snapshot_threshold = threshold;
    }
    pub(crate) fn snapshot_error(&self, error: Option<String>) {
        self.inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .status
            .snapshot_error = error;
    }

    pub(crate) fn refresh_status(&self, node: &RaftNode, last_known_leader: Option<NodeId>) {
        let mut state = self
            .inner
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let threshold = state.status.snapshot_threshold;
        let error = state.status.snapshot_error.take();
        let batching = state.status.batching.clone();
        let storage = state.status.storage;
        let applied_store = state.status.applied_store.take();
        let route_error = state.status.route_error.take();
        state.status = status_from_node(node, last_known_leader, state.application.last_applied());
        state.status.snapshot_threshold = threshold;
        state.status.snapshot_error = error;
        state.status.batching = batching;
        state.status.storage = storage;
        state.status.applied_store = applied_store;
        state.status.route_error = route_error;
    }
}

fn status_from_node(
    node: &RaftNode,
    last_known_leader: Option<NodeId>,
    applied: LogIndex,
) -> NodeStatus {
    let role = NodeRole::from(&node.role);
    NodeStatus {
        committed_membership: node.committed_membership().clone(),
        effective_membership: node.effective_membership().clone(),
        node_id: node.id,
        role,
        term: node.hard.current_term,
        commit_index: node.commit_index,
        last_applied: applied,
        snapshot_index: node.snapshot_index(),
        snapshot_term: node.snapshot_term(),
        retained_log_entries: node.log.len(),
        snapshot_threshold: 0,
        snapshot_error: None,
        batching: Default::default(),
        storage: Default::default(),
        applied_store: None,
        route_error: None,
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

        state
            .apply(&Entry {
                index: 1,
                term: 1,
                command: Command::Put {
                    key: "answer".into(),
                    value: "42".into(),
                },
            })
            .unwrap();
        let snapshot = state.snapshot();
        assert_eq!(snapshot.values.get("answer"), Some(&"42".into()));
        assert_eq!(snapshot.status.last_applied, 1);
        assert_eq!(snapshot.status.commit_index, 1);

        state
            .apply(&Entry {
                index: 2,
                term: 1,
                command: Command::Delete {
                    key: "answer".into(),
                },
            })
            .unwrap();
        assert!(!state.snapshot().values.contains_key("answer"));
    }

    #[test]
    fn snapshot_capture_install_and_reopen_preserve_actual_watermark() {
        let mut node = RaftNode::new(1, vec![2, 3], 7);
        node.hard.current_term = 1;
        let state = SharedReadState::from_node(&node);
        state
            .apply(&Entry {
                index: 1,
                term: 1,
                command: Command::Put {
                    key: "captured".into(),
                    value: "one".into(),
                },
            })
            .unwrap();
        let metadata = SnapshotMetadata {
            membership: None,
            last_included_index: 1,
            last_included_term: 1,
            members: vec![1, 2, 3],
        };
        let image = state.snapshot_image(metadata.clone()).unwrap();
        let mut ahead = metadata;
        ahead.last_included_index = 2;
        assert!(
            state.snapshot_image(ahead).is_err(),
            "cannot snapshot scheduled but unapplied work"
        );
        assert!(
            SharedReadState::from_snapshot(&node, image.clone()).is_err(),
            "core has not restored the snapshot boundary"
        );
        node = RaftNode::restore_with_snapshot(
            1,
            vec![2, 3],
            8,
            node.hard.clone(),
            Some(image.descriptor().clone()),
            vec![],
        )
        .unwrap();
        let reopened = SharedReadState::from_snapshot(&node, image.clone()).unwrap();
        assert_eq!(reopened.get("captured").1.as_deref(), Some("one"));
        assert_eq!(reopened.status().last_applied, 1);
        reopened
            .apply(&Entry {
                index: 2,
                term: 1,
                command: Command::Put {
                    key: "captured".into(),
                    value: "two".into(),
                },
            })
            .unwrap();
        assert!(
            reopened.install_snapshot(image).is_err(),
            "stale snapshot cannot roll back applied state"
        );
        assert_eq!(reopened.get("captured").1.as_deref(), Some("two"));
    }
}
