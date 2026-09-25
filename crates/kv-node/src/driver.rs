//! Single-writer driver for the pure Raft state machine.
//!
//! Effects are executed in the exact order emitted by the core. Durable
//! effects complete before processing continues, so a dependent network reply
//! can never leave the process before the corresponding state reaches disk.

use std::collections::BTreeMap;
use std::io;
use std::time::Duration;

use raft_core::{Command, Effect, Input, LogIndex, NodeId, RaftMessage, RaftNode, TICK_MS};
use tokio::sync::{mpsc, oneshot};
use tokio::time::MissedTickBehavior;
use tracing::{debug, info};

use crate::kv::SharedReadState;
use crate::storage::Storage;
use crate::transport::Transport;

pub const PROPOSAL_CHANNEL_CAPACITY: usize = 256;

fn message_kind(message: &RaftMessage) -> &'static str {
    match message {
        RaftMessage::RequestVote { .. } => "request_vote",
        RaftMessage::RequestVoteReply { .. } => "request_vote_reply",
        RaftMessage::AppendEntries { .. } => "append_entries",
        RaftMessage::AppendEntriesReply { .. } => "append_entries_reply",
    }
}

/// One client command waiting to enter the replicated log.
#[derive(Debug)]
pub struct ProposalRequest {
    pub command: Command,
    pub respond_to: oneshot::Sender<ProposalResult>,
}

/// Completion observed by the HTTP request that submitted a proposal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposalResult {
    Applied {
        index: LogIndex,
    },
    NotLeader {
        leader_hint: Option<NodeId>,
    },
    /// The proposal was accepted, but the driver can no longer prove whether
    /// it committed and applied. Retrying the command may therefore repeat it.
    OutcomeUnknown {
        leader_hint: Option<NodeId>,
    },
}

type PendingWrites = BTreeMap<LogIndex, oneshot::Sender<ProposalResult>>;

/// Runs the node until a durable effect fails.
///
/// Disk operations are intentionally synchronous. The driver is a single
/// writer, and preserving the persist-before-send boundary is more important
/// here than maximizing throughput.
pub async fn run_driver<T: Transport>(
    mut node: RaftNode,
    mut storage: Storage,
    transport: T,
    mut inbound_rx: mpsc::Receiver<(NodeId, RaftMessage)>,
    mut proposal_rx: mpsc::Receiver<ProposalRequest>,
    shared: SharedReadState,
) -> io::Result<()> {
    let started = tokio::time::Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(TICK_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut pending = PendingWrites::new();
    let mut last_known_leader = None;
    shared.refresh_status(&node, last_known_leader);

    loop {
        // A timed-out HTTP request drops its receiver. Pruning here prevents a
        // leader without quorum from retaining abandoned proposals forever.
        pending.retain(|_, respond_to| !respond_to.is_closed());

        tokio::select! {
            _ = tick.tick() => {
                let input = Input::Tick {
                    now_ms: started.elapsed().as_millis() as u64,
                };
                step_and_execute(
                    &mut node,
                    &mut storage,
                    &transport,
                    &shared,
                    &mut pending,
                    &mut last_known_leader,
                    input,
                    None,
                )?;
            }
            Some((from, msg)) = inbound_rx.recv() => {
                debug!(from, kind = message_kind(&msg), "message received");
                if let RaftMessage::AppendEntries { term, leader_id, .. } = &msg {
                    if *term >= node.hard.current_term {
                        last_known_leader = Some(*leader_id);
                    }
                }
                step_and_execute(
                    &mut node,
                    &mut storage,
                    &transport,
                    &shared,
                    &mut pending,
                    &mut last_known_leader,
                    Input::Message { from, msg },
                    None,
                )?;
            }
            Some(request) = proposal_rx.recv() => {
                process_proposal_request(
                    &mut node,
                    &mut storage,
                    &transport,
                    &shared,
                    &mut pending,
                    &mut last_known_leader,
                    request,
                )?;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn process_proposal_request(
    node: &mut RaftNode,
    storage: &mut Storage,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingWrites,
    last_known_leader: &mut Option<NodeId>,
    request: ProposalRequest,
) -> io::Result<()> {
    // HTTP drops this receiver when its deadline expires. If that happened
    // while the request waited in the bounded queue, it is still safe to skip
    // the proposal entirely because the core has not seen it yet.
    if request.respond_to.is_closed() {
        debug!("expired client request skipped before proposal");
        return Ok(());
    }

    step_and_execute(
        node,
        storage,
        transport,
        shared,
        pending,
        last_known_leader,
        Input::ClientPropose {
            command: request.command,
        },
        Some(request.respond_to),
    )
}

#[allow(clippy::too_many_arguments)]
fn step_and_execute(
    node: &mut RaftNode,
    storage: &mut Storage,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingWrites,
    last_known_leader: &mut Option<NodeId>,
    input: Input,
    proposal_response: Option<oneshot::Sender<ProposalResult>>,
) -> io::Result<()> {
    let effects = node.step(input);
    execute_in_order(
        storage,
        transport,
        shared,
        pending,
        last_known_leader,
        effects,
        proposal_response,
    )?;
    shared.refresh_status(node, *last_known_leader);
    Ok(())
}

/// Checks the ordering constraints that can be established from one effect
/// batch without access to the core's previous state.
///
/// A vote solicitation always follows a hard-state change. Granted vote
/// replies and successful append replies may omit a persistence effect only
/// for idempotent re-grants and no-change appends respectively. If the batch
/// contains the relevant persistence effect, it must precede the send.
pub fn audit_persist_before_send(batch: &[Effect]) -> Result<(), String> {
    let first_hard = batch
        .iter()
        .position(|effect| matches!(effect, Effect::PersistHardState(_)));
    let first_log = batch
        .iter()
        .position(|effect| matches!(effect, Effect::PersistLogEntries { .. }));

    for (send_at, effect) in batch.iter().enumerate() {
        let Effect::Send { msg, .. } = effect else {
            continue;
        };
        match msg {
            RaftMessage::RequestVote { .. } => require_prior(
                first_hard,
                send_at,
                "RequestVote must follow PersistHardState",
            )?,
            RaftMessage::RequestVoteReply {
                vote_granted: true, ..
            } if first_hard.is_some() => require_prior(
                first_hard,
                send_at,
                "granted RequestVoteReply precedes PersistHardState",
            )?,
            RaftMessage::AppendEntriesReply { success: true, .. } if first_log.is_some() => {
                require_prior(
                    first_log,
                    send_at,
                    "successful AppendEntriesReply precedes PersistLogEntries",
                )?;
            }
            _ => {}
        }
    }
    Ok(())
}

fn require_prior(
    persist_at: Option<usize>,
    send_at: usize,
    message: &'static str,
) -> Result<(), String> {
    match persist_at {
        Some(at) if at < send_at => Ok(()),
        _ => Err(message.to_owned()),
    }
}

#[allow(clippy::too_many_arguments)]
fn execute_in_order(
    storage: &mut Storage,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingWrites,
    last_known_leader: &mut Option<NodeId>,
    effects: Vec<Effect>,
    mut proposal_response: Option<oneshot::Sender<ProposalResult>>,
) -> io::Result<()> {
    audit_persist_before_send(&effects)
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;

    for effect in effects {
        match effect {
            Effect::PersistHardState(hard) => storage.save_hard_state(&hard)?,
            Effect::PersistLogEntries {
                truncate_from,
                entries,
            } => storage.append_entries(truncate_from, &entries)?,
            Effect::Send { to, msg } => {
                debug!(to, kind = message_kind(&msg), "sending");
                transport.send(to, msg);
            }
            Effect::RoleChanged { role_name, term } => {
                info!(role = role_name, term, "role changed");
                if role_name != "Leader" {
                    mark_pending_outcomes_unknown(pending, *last_known_leader);
                }
            }
            Effect::Apply(entry) => {
                let index = entry.index;
                shared.apply(&entry);
                if let Some(respond_to) = pending.remove(&index) {
                    let _ = respond_to.send(ProposalResult::Applied { index });
                }
            }
            Effect::ProposeAccepted { index } => {
                let Some(respond_to) = proposal_response.take() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "proposal accepted without an active client request",
                    ));
                };
                if !respond_to.is_closed() && pending.insert(index, respond_to).is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("duplicate pending proposal index {index}"),
                    ));
                }
            }
            Effect::ProposeRejected { leader_hint } => {
                if leader_hint.is_some() {
                    *last_known_leader = leader_hint;
                }
                let Some(respond_to) = proposal_response.take() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "proposal rejected without an active client request",
                    ));
                };
                let _ = respond_to.send(ProposalResult::NotLeader { leader_hint });
            }
        }
    }

    if proposal_response.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "client proposal produced no acceptance or rejection effect",
        ));
    }
    Ok(())
}

fn mark_pending_outcomes_unknown(pending: &mut PendingWrites, leader_hint: Option<NodeId>) {
    for (_, respond_to) in std::mem::take(pending) {
        let _ = respond_to.send(ProposalResult::OutcomeUnknown { leader_hint });
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::{Arc, Mutex};

    use raft_core::{Entry, HardState, Role};

    use super::*;

    static TEST_DIR_SEQ: AtomicU32 = AtomicU32::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let sequence = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "micro-raft-driver-test-{}-{sequence}",
                std::process::id()
            ));
            fs::create_dir_all(&path).expect("create test dir");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Default)]
    struct RecordingTransport(Arc<Mutex<Vec<(NodeId, RaftMessage)>>>);

    impl Transport for RecordingTransport {
        fn send(&self, to: NodeId, msg: RaftMessage) {
            self.0.lock().expect("transport lock").push((to, msg));
        }
    }

    fn append_reply() -> RaftMessage {
        RaftMessage::AppendEntriesReply {
            term: 1,
            success: true,
            match_index: 1,
        }
    }

    #[test]
    fn persistence_auditor_rejects_send_before_persist() {
        let bad_vote = vec![
            Effect::Send {
                to: 2,
                msg: RaftMessage::RequestVote {
                    term: 1,
                    candidate_id: 1,
                    last_log_index: 0,
                    last_log_term: 0,
                },
            },
            Effect::PersistHardState(HardState {
                current_term: 1,
                voted_for: Some(1),
            }),
        ];
        assert!(audit_persist_before_send(&bad_vote).is_err());

        let bad_append = vec![
            Effect::Send {
                to: 1,
                msg: append_reply(),
            },
            Effect::PersistLogEntries {
                truncate_from: None,
                entries: Vec::new(),
            },
        ];
        assert!(audit_persist_before_send(&bad_append).is_err());
    }

    #[test]
    fn persistence_auditor_accepts_ordered_and_no_change_batches() {
        let ordered = vec![
            Effect::PersistLogEntries {
                truncate_from: None,
                entries: Vec::new(),
            },
            Effect::Send {
                to: 1,
                msg: append_reply(),
            },
        ];
        assert!(audit_persist_before_send(&ordered).is_ok());
        assert!(audit_persist_before_send(&[Effect::Send {
            to: 1,
            msg: append_reply(),
        }])
        .is_ok());
    }

    #[test]
    fn storage_failure_prevents_later_send() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).expect("open storage");
        fs::create_dir(dir.path().join("log.jsonl")).expect("block log file creation");
        let node = RaftNode::new(1, vec![2, 3], 3);
        let shared = SharedReadState::from_node(&node);
        let transport = RecordingTransport::default();
        let effects = vec![
            Effect::PersistLogEntries {
                truncate_from: None,
                entries: vec![Entry {
                    index: 1,
                    term: 1,
                    command: Command::NoOp,
                }],
            },
            Effect::Send {
                to: 2,
                msg: append_reply(),
            },
        ];

        let result = execute_in_order(
            &mut storage,
            &transport,
            &shared,
            &mut PendingWrites::new(),
            &mut None,
            effects,
            None,
        );
        assert!(result.is_err());
        assert!(transport.0.lock().expect("transport lock").is_empty());
    }

    #[tokio::test]
    async fn storage_failure_leaves_in_flight_proposal_outcome_unresolved() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).expect("open storage");
        fs::create_dir(dir.path().join("log.jsonl")).expect("block log file creation");
        let mut node = RaftNode::new(1, vec![2, 3], 3);
        node.hard.current_term = 1;
        node.role = Role::Leader {
            next_index: BTreeMap::from([(2, 1), (3, 1)]),
            match_index: BTreeMap::from([(2, 0), (3, 0)]),
        };
        let shared = SharedReadState::from_node(&node);
        let transport = RecordingTransport::default();
        let mut pending = PendingWrites::new();
        let mut last_known_leader = None;
        let (respond_to, response) = oneshot::channel();

        let result = process_proposal_request(
            &mut node,
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut last_known_leader,
            ProposalRequest {
                command: Command::Put {
                    key: "k".into(),
                    value: "v".into(),
                },
                respond_to,
            },
        );

        assert!(result.is_err(), "the storage error terminates the driver");
        assert_eq!(node.log.len(), 1, "the core already saw the proposal");
        assert!(pending.is_empty(), "acceptance was not completed");
        assert!(
            response.await.is_err(),
            "the lost completion maps to outcome_unknown at the HTTP boundary"
        );
    }

    #[tokio::test]
    async fn client_completes_only_when_its_entry_is_applied() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).expect("open storage");
        let node = RaftNode::new(1, vec![2, 3], 3);
        let shared = SharedReadState::from_node(&node);
        let transport = RecordingTransport::default();
        let mut pending = PendingWrites::new();
        let (respond_to, mut response) = oneshot::channel();

        execute_in_order(
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut None,
            vec![Effect::ProposeAccepted { index: 1 }],
            Some(respond_to),
        )
        .expect("accept proposal");
        assert!(matches!(
            response.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        execute_in_order(
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut None,
            vec![Effect::Apply(Entry {
                index: 1,
                term: 1,
                command: Command::Put {
                    key: "k".into(),
                    value: "v".into(),
                },
            })],
            None,
        )
        .expect("apply proposal");
        assert_eq!(
            response.await.expect("completion"),
            ProposalResult::Applied { index: 1 }
        );
        assert_eq!(
            shared.snapshot().values.get("k").map(String::as_str),
            Some("v")
        );
    }

    #[test]
    fn cancelled_client_is_not_retained() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).expect("open storage");
        let node = RaftNode::new(1, vec![2, 3], 3);
        let shared = SharedReadState::from_node(&node);
        let transport = RecordingTransport::default();
        let mut pending = PendingWrites::new();
        let (respond_to, response) = oneshot::channel();
        drop(response);

        execute_in_order(
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut None,
            vec![Effect::ProposeAccepted { index: 4 }],
            Some(respond_to),
        )
        .expect("accept cancelled proposal");
        assert!(pending.is_empty());
    }

    #[test]
    fn expired_queued_client_is_skipped_before_proposal() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).expect("open storage");
        let mut node = RaftNode::new(1, vec![2, 3], 3);
        node.hard.current_term = 1;
        node.role = Role::Leader {
            next_index: BTreeMap::from([(2, 1), (3, 1)]),
            match_index: BTreeMap::from([(2, 0), (3, 0)]),
        };
        let shared = SharedReadState::from_node(&node);
        let transport = RecordingTransport::default();
        let mut pending = PendingWrites::new();
        let mut last_known_leader = None;
        let (respond_to, response) = oneshot::channel();
        drop(response);

        process_proposal_request(
            &mut node,
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut last_known_leader,
            ProposalRequest {
                command: Command::Put {
                    key: "k".into(),
                    value: "v".into(),
                },
                respond_to,
            },
        )
        .expect("skip expired proposal");

        assert!(node.log.is_empty(), "the command never entered the core");
        assert!(pending.is_empty());
        assert!(transport.0.lock().expect("transport lock").is_empty());
        drop(storage);
        let (_storage, _, recovered) = Storage::open(dir.path()).expect("reopen storage");
        assert!(recovered.is_empty(), "the command was not persisted");
    }

    #[tokio::test]
    async fn stepping_down_marks_pending_outcomes_unknown() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).expect("open storage");
        let node = RaftNode::new(1, vec![2, 3], 3);
        let shared = SharedReadState::from_node(&node);
        let transport = RecordingTransport::default();
        let mut pending = PendingWrites::new();
        let (respond_to, response) = oneshot::channel();
        pending.insert(2, respond_to);

        execute_in_order(
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut Some(3),
            vec![Effect::RoleChanged {
                role_name: "Follower",
                term: 2,
            }],
            None,
        )
        .expect("step down");

        assert!(pending.is_empty());
        assert_eq!(
            response.await.expect("completion"),
            ProposalResult::OutcomeUnknown {
                leader_hint: Some(3)
            }
        );
    }
}
