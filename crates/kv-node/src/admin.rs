//! Bounded administrative requests; log admission is never a completion ACK.
use raft_core::{
    membership::{AdminOperation, AdminRecord},
    NodeId,
};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::oneshot;

pub const ADMIN_CHANNEL_CAPACITY: usize = 32;
pub const MAX_ADMIN_WAITERS: usize = 64;
pub const ADMIN_TIMEOUT: Duration = Duration::from_secs(7);
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdminCommand {
    pub request_id: String,
    pub operation: AdminOperation,
}
#[derive(Debug)]
pub struct AdminRequest {
    pub command: AdminCommand,
    /// SHA-256 of canonical complete effective membership validated with DNS.
    /// Fixed-size proof avoids multiplying the bounded history by queue length.
    pub validated_membership: Option<[u8; 32]>,
    pub respond_to: oneshot::Sender<AdminResult>,
}
pub fn membership_revision(state: &raft_core::membership::MembershipState) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::digest(serde_json::to_vec(state).expect("membership is serializable")).into()
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum AdminResult {
    Completed {
        request_id: String,
        record: AdminRecord,
    },
    Rejected {
        reason: String,
        leader_hint: Option<NodeId>,
    },
    Unknown {
        request_id: String,
        leader_hint: Option<NodeId>,
    },
}
