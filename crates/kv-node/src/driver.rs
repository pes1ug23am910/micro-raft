//! Single-writer driver for the pure Raft state machine.
//!
//! Effects are executed in the exact order emitted by the core. Durable
//! effects complete before processing continues, so a dependent network reply
//! can never leave the process before the corresponding state reaches disk.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::Path;
use std::time::Duration;

use raft_core::{Command, Effect, Input, LogIndex, NodeId, RaftMessage, RaftNode, TICK_MS};
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior};
use tracing::{debug, info};

use crate::admin::{AdminRequest, AdminResult, ADMIN_TIMEOUT, MAX_ADMIN_WAITERS};
use crate::application::{ApplyOutcome, SessionResult};
use crate::kv::{NodeRole, SharedReadState};
use crate::shutdown::{self, ShutdownRx, DRAIN_TIMEOUT};
use crate::snapshot::{SnapshotImage, SnapshotMetadata, SnapshotReceiveError, SnapshotReceiver};
use crate::storage::{DurableStore, Storage};
use crate::transport::Transport;
use raft_core::membership::{AdminOperation, MembershipState};
use raft_core::membership_protocol::MembershipOutcome;

pub const PROPOSAL_CHANNEL_CAPACITY: usize = 256;
pub const READ_CHANNEL_CAPACITY: usize = 64;
pub const MAX_BATCH_BYTES: usize = 1024 * 1024;
pub const MAX_BATCH_DELAY_MS: u64 = 10;

#[derive(Clone, Copy, Debug)]
pub struct BatchOptions {
    pub records: usize,
    pub delay: Duration,
}
impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            records: raft_core::MAX_PROPOSAL_BATCH,
            delay: Duration::from_millis(2),
        }
    }
}
impl BatchOptions {
    pub fn validate(self) -> io::Result<Self> {
        if self.records == 0
            || self.records > raft_core::MAX_PROPOSAL_BATCH
            || self.delay > Duration::from_millis(MAX_BATCH_DELAY_MS)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "batch records must be 1..=16 and delay at most 10ms",
            ));
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize)]
pub struct BatchingStatus {
    pub record_limit: usize,
    pub byte_limit: usize,
    pub delay_ms: u64,
    pub admitted_batches: u64,
    pub admitted_records: u64,
    pub admitted_command_bytes: u64,
    pub max_batch_records: usize,
    pub max_batch_command_bytes: usize,
    pub proposal_queue_peak: usize,
    pub cancelled_before_admission: u64,
    pub oversized_rejected: u64,
}

pub const READ_TIMEOUT: Duration = Duration::from_secs(2);

fn message_kind(message: &RaftMessage) -> &'static str {
    match message {
        RaftMessage::GroupMessage { message, .. } => message_kind(message),
        RaftMessage::MembershipAdvertisement { .. } => "membership_advertisement",
        RaftMessage::InstallSnapshot { .. } => "install_snapshot",
        RaftMessage::InstallSnapshotReply { .. } => "install_snapshot_reply",
        RaftMessage::CompactedPrefix { .. } => "compacted_prefix",
        RaftMessage::ReadProbe { .. } => "read_probe",
        RaftMessage::ReadProbeReply { .. } => "read_probe_reply",
        RaftMessage::PreVote { .. } => "pre_vote",
        RaftMessage::PreVoteReply { .. } => "pre_vote_reply",
        RaftMessage::RequestVote { .. } => "request_vote",
        RaftMessage::RequestVoteReply { .. } => "request_vote_reply",
        RaftMessage::AppendEntries { .. } => "append_entries",
        RaftMessage::AppendEntriesReply { .. } => "append_entries_reply",
    }
}

/// Snapshot state belongs to the same serialized writer as ordinary effects.
/// One active transfer and one prepared capture bound temporary state.
pub struct SnapshotRuntime {
    administration: Option<mpsc::Receiver<AdminRequest>>,
    routes: Option<tokio::sync::watch::Sender<crate::membership_runtime::RouteUpdate>>,
    active: Option<SnapshotImage>,
    prepared: Option<SnapshotImage>,
    receiver: SnapshotReceiver,
    threshold: u64,
    last_attempt: LogIndex,
    batching: BatchOptions,
    application_store: Option<crate::applied_store::AppliedStore>,
}
impl SnapshotRuntime {
    pub fn with_administration(
        mut self,
        receiver: mpsc::Receiver<AdminRequest>,
        routes: tokio::sync::watch::Sender<crate::membership_runtime::RouteUpdate>,
    ) -> Self {
        self.administration = Some(receiver);
        self.routes = Some(routes);
        self
    }
    pub fn with_application_store(mut self, store: crate::applied_store::AppliedStore) -> Self {
        self.application_store = Some(store);
        self
    }
    pub fn with_batching(mut self, options: BatchOptions) -> io::Result<Self> {
        self.batching = options.validate()?;
        Ok(self)
    }
    pub fn new(
        data_dir: impl AsRef<Path>,
        active: Option<SnapshotImage>,
        threshold: u64,
    ) -> io::Result<Self> {
        let last_attempt = active
            .as_ref()
            .map_or(0, |image| image.descriptor().metadata.last_included_index);
        Ok(Self {
            administration: None,
            routes: None,
            active,
            prepared: None,
            receiver: SnapshotReceiver::new(data_dir.as_ref())?,
            threshold,
            last_attempt,
            batching: BatchOptions::default(),
            application_store: None,
        })
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
    Session(SessionResult),
    Applied {
        index: LogIndex,
    },
    /// The request was rejected before it reached the core.
    ShuttingDown,
    AdmissionRejected {
        reason: &'static str,
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

/// A read waits for a current-term quorum and application, through the same writer.
#[derive(Debug)]
pub struct ReadRequest {
    pub key: String,
    pub respond_to: oneshot::Sender<ReadResult>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadResult {
    Ready {
        value: Option<String>,
        index: LogIndex,
        applied_index: LogIndex,
        term: u64,
        context: u64,
    },
    Rejected {
        reason: &'static str,
        leader_hint: Option<NodeId>,
    },
}

#[derive(Clone, Copy, Debug)]
struct ReadBarrier {
    index: LogIndex,
    term: u64,
    context: u64,
}
struct PendingRead {
    key: String,
    respond_to: oneshot::Sender<ReadResult>,
    deadline: Instant,
    barrier: Option<ReadBarrier>,
}

struct PendingAdmin {
    operation: AdminOperation,
    respond_to: oneshot::Sender<AdminResult>,
    deadline: Instant,
}
#[derive(Default)]
struct PendingRequests {
    administration: BTreeMap<String, Vec<PendingAdmin>>,
    routes: Option<tokio::sync::watch::Sender<crate::membership_runtime::RouteUpdate>>,
    routing_state: Option<(u64, MembershipState)>,
    writes: BTreeMap<LogIndex, oneshot::Sender<ProposalResult>>,
    reads: BTreeMap<u64, PendingRead>,
    next_read_id: u64,
    snapshots: Option<SnapshotRuntime>,
    application_store: Option<crate::applied_store::AppliedStore>,
}

// Existing write operations remain explicit through their map interface. Read
// completion is separate: it never consumes or changes a write's log-index slot.
impl std::ops::Deref for PendingRequests {
    type Target = BTreeMap<LogIndex, oneshot::Sender<ProposalResult>>;
    fn deref(&self) -> &Self::Target {
        &self.writes
    }
}
impl std::ops::DerefMut for PendingRequests {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.writes
    }
}
impl PendingRequests {
    fn new() -> Self {
        Self::default()
    }
}

/// Completion of the local shutdown protocol, not a guarantee that every
/// attempted write committed. Uncommitted durable log entries may remain.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DrainReport {
    pub queued_rejected: usize,
    pub pending_unknown: usize,
    pub deadline_expired: bool,
    pub uncommitted_entries: u64,
}

/// Compatibility entry point without an external shutdown request.
pub async fn run_driver<T: Transport>(
    node: RaftNode,
    storage: Storage,
    transport: T,
    inbound_rx: mpsc::Receiver<(NodeId, RaftMessage)>,
    proposal_rx: mpsc::Receiver<ProposalRequest>,
    shared: SharedReadState,
) -> io::Result<()> {
    let (_controller, shutdown) = shutdown::channel();
    run_driver_with_shutdown(
        node,
        storage,
        transport,
        inbound_rx,
        proposal_rx,
        shared,
        shutdown,
    )
    .await
    .map(|_| ())
}

/// Drain accepted requests through the same writer after admission closes.
///
/// Durable batches contain no await and are never canceled midway. The deadline
/// bounds quorum/asynchronous waiting; it cannot preempt an OS call stuck in sync.
pub async fn run_driver_with_shutdown<T: Transport, S: DurableStore>(
    node: RaftNode,
    storage: S,
    transport: T,
    inbound_rx: mpsc::Receiver<(NodeId, RaftMessage)>,
    proposal_rx: mpsc::Receiver<ProposalRequest>,
    shared: SharedReadState,
    shutdown: ShutdownRx,
) -> io::Result<DrainReport> {
    run_driver_with_limit(
        node,
        storage,
        transport,
        inbound_rx,
        proposal_rx,
        shared,
        shutdown,
        DRAIN_TIMEOUT,
    )
    .await
}

/// Runtime entry point with bounded linearizable-read admission.
#[allow(clippy::too_many_arguments)]
pub async fn run_driver_with_reads_and_shutdown<T: Transport, S: DurableStore>(
    node: RaftNode,
    storage: S,
    transport: T,
    inbound_rx: mpsc::Receiver<(NodeId, RaftMessage)>,
    proposal_rx: mpsc::Receiver<ProposalRequest>,
    read_rx: mpsc::Receiver<ReadRequest>,
    shared: SharedReadState,
    shutdown: ShutdownRx,
) -> io::Result<DrainReport> {
    run_driver_with_limit_and_reads(
        node,
        storage,
        transport,
        inbound_rx,
        proposal_rx,
        read_rx,
        shared,
        shutdown,
        DRAIN_TIMEOUT,
        None,
    )
    .await
}

/// Full runtime entry point; zero threshold keeps transfer/recovery enabled.
#[allow(clippy::too_many_arguments)]
pub async fn run_driver_with_snapshots<T: Transport, S: DurableStore>(
    node: RaftNode,
    storage: S,
    transport: T,
    inbound_rx: mpsc::Receiver<(NodeId, RaftMessage)>,
    proposal_rx: mpsc::Receiver<ProposalRequest>,
    read_rx: mpsc::Receiver<ReadRequest>,
    shared: SharedReadState,
    shutdown: ShutdownRx,
    snapshots: SnapshotRuntime,
) -> io::Result<DrainReport> {
    if node.snapshot_descriptor() != snapshots.active.as_ref().map(SnapshotImage::descriptor) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "driver snapshot differs from recovered core boundary",
        ));
    }
    shared.configure_snapshots(snapshots.threshold);
    run_driver_with_limit_and_reads(
        node,
        storage,
        transport,
        inbound_rx,
        proposal_rx,
        read_rx,
        shared,
        shutdown,
        DRAIN_TIMEOUT,
        Some(snapshots),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_driver_with_limit<T: Transport, S: DurableStore>(
    node: RaftNode,
    storage: S,
    transport: T,
    inbound_rx: mpsc::Receiver<(NodeId, RaftMessage)>,
    proposal_rx: mpsc::Receiver<ProposalRequest>,
    shared: SharedReadState,
    shutdown: ShutdownRx,
    drain_timeout: Duration,
) -> io::Result<DrainReport> {
    let (sender, reads) = mpsc::channel(1);
    drop(sender);
    run_driver_with_limit_and_reads(
        node,
        storage,
        transport,
        inbound_rx,
        proposal_rx,
        reads,
        shared,
        shutdown,
        drain_timeout,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn run_driver_with_limit_and_reads<T: Transport, S: DurableStore>(
    mut node: RaftNode,
    mut storage: S,
    transport: T,
    mut inbound_rx: mpsc::Receiver<(NodeId, RaftMessage)>,
    mut proposal_rx: mpsc::Receiver<ProposalRequest>,
    mut read_rx: mpsc::Receiver<ReadRequest>,
    shared: SharedReadState,
    mut shutdown: ShutdownRx,
    drain_timeout: Duration,
    mut snapshots: Option<SnapshotRuntime>,
) -> io::Result<DrainReport> {
    let batching = snapshots
        .as_ref()
        .map_or_else(BatchOptions::default, |s| s.batching)
        .validate()?;
    shared.update_batching(|status| {
        status.record_limit = batching.records;
        status.byte_limit = MAX_BATCH_BYTES;
        status.delay_ms = batching.delay.as_millis() as u64;
    });
    let mut deferred = None;
    let mut fairness_due = false;
    let started = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_millis(TICK_MS));
    tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut pending = PendingRequests::new();
    pending.application_store = snapshots
        .as_mut()
        .and_then(|runtime| runtime.application_store.take());
    if let Some(store) = &pending.application_store {
        shared.applied_store_status(store.status()?);
    }
    let (closed_admin_tx, closed_admin_rx) = mpsc::channel(1);
    drop(closed_admin_tx);
    let mut admin_rx = snapshots
        .as_mut()
        .and_then(|runtime| runtime.administration.take())
        .unwrap_or(closed_admin_rx);
    pending.routes = snapshots.as_mut().and_then(|runtime| runtime.routes.take());
    pending.snapshots = snapshots;
    let mut last_known_leader = None;
    let mut deadline = None;
    let mut report = DrainReport::default();
    shared.refresh_status(&node, last_known_leader);
    // Startup recovery follows engine watermark restoration and precedes ticks.
    step_and_execute(
        &mut node,
        &mut storage,
        &transport,
        &shared,
        &mut pending,
        &mut last_known_leader,
        Input::Recover,
        None,
    )?;

    let outcome = loop {
        // This boundary is reached only after an entire effect batch completes.
        if deadline.is_none() {
            if let Some(requested_at) = shutdown.requested_at() {
                proposal_rx.close();
                read_rx.close();
                admin_rx.close();
                while let Ok(request) = admin_rx.try_recv() {
                    let _ = request.respond_to.send(AdminResult::Rejected {
                        reason: "shutting_down".into(),
                        leader_hint: last_known_leader,
                    });
                    report.queued_rejected += 1;
                }
                reject_queued_reads(&mut read_rx, "shutting_down");
                reject_pending_reads(&mut pending, "shutting_down", last_known_leader);
                deadline = Some(requested_at + drain_timeout);
                info!("shutdown drain started; proposal admission closed");
            }
        }
        // A closed HTTP waiter already has an unknown outcome at its boundary.
        pending.retain(|_, respond_to| !respond_to.is_closed());
        reconcile_admin(&node, &shared, &mut pending, last_known_leader);
        if let Err(error) = cancel_expired_reads(
            &mut node,
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut last_known_leader,
        ) {
            break Err(error);
        }
        if let Some(drain_deadline) = deadline {
            if let Some(request) = deferred.take() {
                let request: ProposalRequest = request;
                let _ = request.respond_to.send(ProposalResult::ShuttingDown);
                report.queued_rejected += 1;
            }
            report.queued_rejected += reject_queued_requests(&mut proposal_rx);
            if pending.is_empty() && pending.administration.is_empty() {
                break Ok(());
            }
            if Instant::now() >= drain_deadline {
                report.deadline_expired = true;
                break Ok(());
            }
        }

        // A busy proposal queue cannot win another admission before one queued
        // peer input and one queued read get a service opportunity. Refresh the
        // logical clock first, including after a slow synchronous disk barrier.
        if fairness_due && deadline.is_none() {
            fairness_due = false;
            if let Err(error) = service_fair_turn(
                &mut node,
                &mut storage,
                &transport,
                &shared,
                &mut pending,
                &mut last_known_leader,
                &mut inbound_rx,
                &mut read_rx,
                &shutdown,
                started,
            ) {
                break Err(error);
            }
            continue;
        }
        let wake_deadline = deadline.unwrap_or_else(Instant::now);
        let result = tokio::select! {
            _ = shutdown.requested(), if deadline.is_none() => Ok(()),
            _ = tokio::time::sleep_until(wake_deadline), if deadline.is_some() => {
                report.deadline_expired = true;
                break Ok(());
            }
            _ = tick.tick() => {
                let input = Input::Tick {
                    now_ms: started.elapsed().as_millis() as u64,
                };
                step_and_execute(
                    &mut node, &mut storage, &transport, &shared, &mut pending,
                    &mut last_known_leader, input, None,
                )
            }
            Some((from, msg)) = inbound_rx.recv() => {
                process_peer_message(&mut node, &mut storage, &transport, &shared,
                    &mut pending, &mut last_known_leader, from, msg)
            }
            Some(request) = admin_rx.recv(), if deadline.is_none() => {
                if shutdown.is_requested() {
                    let _ = request.respond_to.send(AdminResult::Rejected { reason: "shutting_down".into(), leader_hint: last_known_leader });
                    Ok(())
                } else {
                    process_admin_request(&mut node, &mut storage, &transport, &shared,
                        &mut pending, &mut last_known_leader, request)
                }
            }
            Some(request) = read_rx.recv(), if deadline.is_none() => {
                if shutdown.is_requested() {
                    let _ = request.respond_to.send(ReadResult::Rejected { reason: "shutting_down", leader_hint: last_known_leader });
                    Ok(())
                } else {
                    process_read_request(&mut node, &mut storage, &transport, &shared,
                        &mut pending, &mut last_known_leader, request)
                }
            }
            Some(request) = next_proposal(&mut deferred, &mut proposal_rx), if deadline.is_none() => {
                shared.update_batching(|status| status.proposal_queue_peak = status.proposal_queue_peak.max(proposal_rx.len() + 1));
                let requests = collect_proposals(request, &mut proposal_rx, &mut deferred,
                    &mut shutdown, batching, &shared).await;
                if shutdown.is_requested() {
                    report.queued_rejected += requests.len();
                    for request in requests { let _ = request.respond_to.send(ProposalResult::ShuttingDown); }
                    Ok(())
                } else {
                    fairness_due = true;
                    step_and_execute(&mut node, &mut storage, &transport, &shared, &mut pending,
                        &mut last_known_leader, Input::Tick { now_ms: started.elapsed().as_millis() as u64 }, None)
                    .and_then(|()| process_proposal_batch(
                        &mut node, &mut storage, &transport, &shared, &mut pending,
                        &mut last_known_leader, requests))
                }
            }
        };
        if let Err(error) = result {
            break Err(error);
        }
        if deadline.is_none() && !shutdown.is_requested() {
            if let Err(error) = maybe_compact(
                &mut node,
                &mut storage,
                &transport,
                &shared,
                &mut pending,
                &mut last_known_leader,
            ) {
                break Err(error);
            }
            if let Some(store) = pending.application_store.as_mut() {
                match store.maintain() {
                    Ok(true) => match store.status() {
                        Ok(status) => shared.applied_store_status(status),
                        Err(error) => break Err(error),
                    },
                    Ok(false) => {}
                    Err(error) => break Err(error),
                }
            }
        }
    };

    // Fatal effects also close admission. Queued requests never entered Raft;
    // accepted requests cannot be retroactively called rejected.
    proposal_rx.close();
    read_rx.close();
    reject_queued_reads(&mut read_rx, "unavailable");
    reject_pending_reads(&mut pending, "unavailable", last_known_leader);
    if let Some(request) = deferred.take() {
        let _ = request.respond_to.send(ProposalResult::ShuttingDown);
        report.queued_rejected += 1;
    }
    report.queued_rejected += reject_queued_requests(&mut proposal_rx);
    report.pending_unknown =
        pending.len() + pending.administration.values().map(Vec::len).sum::<usize>();
    unknown_admin(&mut pending, last_known_leader);
    mark_pending_outcomes_unknown(&mut pending, last_known_leader);
    report.uncommitted_entries = node.last_log_index().saturating_sub(node.commit_index);
    let cleanup = pending
        .snapshots
        .as_mut()
        .map_or(Ok(()), |runtime| runtime.receiver.cancel_active());
    if let Err(error) = &cleanup {
        tracing::error!(%error, "snapshot transfer cleanup failed");
    }
    let engine_flushed = pending
        .application_store
        .as_mut()
        .map_or(Ok(()), |store| store.flush());
    if let Err(error) = &engine_flushed {
        tracing::error!(%error,"final application engine flush failed");
    }
    let outcome = outcome.and(cleanup).and(engine_flushed);
    let flushed = storage.flush();
    match (outcome, flushed) {
        (Err(error), Err(flush_error)) => {
            tracing::error!(%flush_error, "final sync also failed after driver error");
            Err(error)
        }
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => {
            info!(
                queued_rejected = report.queued_rejected,
                pending_unknown = report.pending_unknown,
                deadline_expired = report.deadline_expired,
                uncommitted_entries = report.uncommitted_entries,
                "shutdown storage synced"
            );
            Ok(report)
        }
    }
}

fn unknown_admin(pending: &mut PendingRequests, leader_hint: Option<NodeId>) {
    for (request_id, waiters) in std::mem::take(&mut pending.administration) {
        for waiter in waiters {
            let _ = waiter.respond_to.send(AdminResult::Unknown {
                request_id: request_id.clone(),
                leader_hint,
            });
        }
    }
}
fn reconcile_admin(
    node: &RaftNode,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader_hint: Option<NodeId>,
) {
    let applied = shared.status().last_applied;
    let now = Instant::now();
    let committed = node.committed_membership();
    let mut keep = BTreeMap::new();
    for (request_id, waiters) in std::mem::take(&mut pending.administration) {
        for waiter in waiters {
            if waiter.respond_to.is_closed() {
                continue;
            }
            let record = committed.state.records.get(&request_id).filter(|record| {
                record.operation == waiter.operation
                    && record.final_index.is_some_and(|index| index <= applied)
            });
            if let Some(record) = record {
                let _ = waiter.respond_to.send(AdminResult::Completed {
                    request_id: request_id.clone(),
                    record: record.clone(),
                });
            } else if !matches!(node.role, raft_core::Role::Leader { .. }) || now >= waiter.deadline
            {
                let _ = waiter.respond_to.send(AdminResult::Unknown {
                    request_id: request_id.clone(),
                    leader_hint,
                });
            } else {
                keep.entry(request_id.clone())
                    .or_insert_with(Vec::new)
                    .push(waiter);
            }
        }
    }
    pending.administration = keep;
}
fn update_routes(node: &RaftNode, pending: &mut PendingRequests) {
    let next = (node.hard.current_term, node.effective_membership().clone());
    if pending.routing_state.as_ref() != Some(&next) {
        if let Some(routes) = &pending.routes {
            routes.send_modify(|update| {
                update
                    .update_authority(node.hard.current_term, node.effective_membership().clone());
            });
        }
        pending.routing_state = Some(next);
    }
}
#[allow(clippy::too_many_arguments)]
fn process_admin_request(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader: &mut Option<NodeId>,
    request: AdminRequest,
) -> io::Result<()> {
    if request.respond_to.is_closed() {
        return Ok(());
    }
    let request_id = request.command.request_id;
    let operation = request.command.operation;
    let retry = node
        .effective_membership()
        .records
        .get(&request_id)
        .is_some_and(|record| record.operation == operation);
    let reason = if matches!(operation, AdminOperation::AddLearner { .. })
        && !retry
        && request.validated_membership
            != Some(crate::admin::membership_revision(
                node.effective_membership(),
            )) {
        Some("stale_endpoint_validation")
    } else if pending.administration.values().map(Vec::len).sum::<usize>() >= MAX_ADMIN_WAITERS {
        Some("admin_capacity")
    } else if pending
        .administration
        .get(&request_id)
        .is_some_and(|waiters| waiters.iter().any(|waiter| waiter.operation != operation))
    {
        Some("request_payload_changed")
    } else {
        None
    };
    if let Some(reason) = reason {
        let _ = request.respond_to.send(AdminResult::Rejected {
            reason: reason.into(),
            leader_hint: *leader,
        });
        return Ok(());
    }
    pending
        .administration
        .entry(request_id.clone())
        .or_default()
        .push(PendingAdmin {
            operation: operation.clone(),
            respond_to: request.respond_to,
            deadline: Instant::now() + ADMIN_TIMEOUT,
        });
    step_and_execute(
        node,
        storage,
        transport,
        shared,
        pending,
        leader,
        Input::MembershipChange {
            request_id,
            operation,
        },
        None,
    )
}

fn reject_queued_requests(proposals: &mut mpsc::Receiver<ProposalRequest>) -> usize {
    let mut rejected = 0;
    while let Ok(request) = proposals.try_recv() {
        rejected += 1;
        let _ = request.respond_to.send(ProposalResult::ShuttingDown);
    }
    rejected
}
async fn next_proposal(
    deferred: &mut Option<ProposalRequest>,
    proposals: &mut mpsc::Receiver<ProposalRequest>,
) -> Option<ProposalRequest> {
    match deferred.take() {
        Some(request) => Some(request),
        None => proposals.recv().await,
    }
}

// Counting serialization bounds the canonical command bytes without allocating
// an unbounded temporary buffer for a caller outside the HTTP API.
fn command_bytes(command: &Command) -> Option<usize> {
    struct Counter(usize);
    impl io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let size = self.0.saturating_add(bytes.len());
            if size > MAX_BATCH_BYTES {
                return Err(io::Error::other("batch byte capacity"));
            }
            self.0 = size;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, command).ok()?;
    Some(counter.0)
}

async fn collect_proposals(
    first: ProposalRequest,
    proposals: &mut mpsc::Receiver<ProposalRequest>,
    deferred: &mut Option<ProposalRequest>,
    shutdown: &mut ShutdownRx,
    options: BatchOptions,
    shared: &SharedReadState,
) -> Vec<ProposalRequest> {
    let deadline = Instant::now() + options.delay;
    let mut batch = Vec::new();
    let mut bytes = 0;
    let mut next = Some(first);
    // Canceled/invalid queued work counts toward the scan bound too. Otherwise
    // a producer of canceled requests could starve ticks and shutdown forever.
    let mut scanned = 0;
    while let Some(request) = next.take() {
        scanned += 1;
        if request.respond_to.is_closed() {
            shared.update_batching(|status| status.cancelled_before_admission += 1);
        } else if let Some(size) = command_bytes(&request.command) {
            if bytes + size > MAX_BATCH_BYTES {
                *deferred = Some(request);
                break;
            }
            bytes += size;
            batch.push(request);
        } else {
            shared.update_batching(|status| status.oversized_rejected += 1);
            let _ = request.respond_to.send(ProposalResult::AdmissionRejected {
                reason: "proposal_too_large",
            });
        }
        if batch.len() >= options.records
            || scanned >= raft_core::MAX_PROPOSAL_BATCH
            || shutdown.is_requested()
        {
            break;
        }
        // An already queued request is admitted without waiting. Delay zero is
        // still a no-wait group; records=1 is the single-request baseline.
        match proposals.try_recv() {
            Ok(request) => next = Some(request),
            Err(mpsc::error::TryRecvError::Disconnected) => break,
            Err(mpsc::error::TryRecvError::Empty) => {
                if Instant::now() >= deadline {
                    break;
                }
                next = tokio::select! {
                    biased;
                    _ = shutdown.requested() => None,
                    _ = tokio::time::sleep_until(deadline) => None,
                    request = proposals.recv() => request,
                };
            }
        }
    }
    batch
}

#[allow(clippy::too_many_arguments)]
fn process_peer_message(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader: &mut Option<NodeId>,
    from: NodeId,
    msg: RaftMessage,
) -> io::Result<()> {
    debug!(from, kind = message_kind(&msg), "message received");
    // The core alone validates group, membership, sender, and term. Its accepted
    // leader hint is reflected by the effect/result path, never by raw wire data.
    step_and_execute(
        node,
        storage,
        transport,
        shared,
        pending,
        leader,
        Input::Message { from, msg },
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn service_fair_turn(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader: &mut Option<NodeId>,
    inbound: &mut mpsc::Receiver<(NodeId, RaftMessage)>,
    reads: &mut mpsc::Receiver<ReadRequest>,
    shutdown: &ShutdownRx,
    started: Instant,
) -> io::Result<()> {
    step_and_execute(
        node,
        storage,
        transport,
        shared,
        pending,
        leader,
        Input::Tick {
            now_ms: started.elapsed().as_millis() as u64,
        },
        None,
    )?;
    if let Ok((from, msg)) = inbound.try_recv() {
        process_peer_message(node, storage, transport, shared, pending, leader, from, msg)?;
    }
    if !shutdown.is_requested() {
        if let Ok(request) = reads.try_recv() {
            process_read_request(node, storage, transport, shared, pending, leader, request)?;
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn process_proposal_request(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader: &mut Option<NodeId>,
    request: ProposalRequest,
) -> io::Result<()> {
    process_proposal_batch(
        node,
        storage,
        transport,
        shared,
        pending,
        leader,
        vec![request],
    )
}

#[allow(clippy::too_many_arguments)]
fn process_proposal_batch(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader: &mut Option<NodeId>,
    requests: Vec<ProposalRequest>,
) -> io::Result<()> {
    let mut commands = Vec::new();
    let mut responses = ProposalResponses::default();
    let mut bytes = 0;
    for request in requests {
        if request.respond_to.is_closed() {
            shared.update_batching(|status| status.cancelled_before_admission += 1);
            continue;
        }
        let size = command_bytes(&request.command)
            .ok_or_else(|| io::Error::other("unchecked proposal size"))?;
        bytes += size;
        commands.push(request.command);
        responses.0.push_back(request.respond_to);
    }
    if commands.is_empty() {
        return Ok(());
    }
    if commands.len() > raft_core::MAX_PROPOSAL_BATCH || bytes > MAX_BATCH_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unchecked proposal batch bounds",
        ));
    }
    let count = commands.len();
    let effects = node.step(Input::ClientProposeBatch { commands });
    if effects
        .iter()
        .any(|effect| matches!(effect, Effect::ProposeAccepted { .. }))
    {
        shared.update_batching(|status| {
            status.admitted_batches += 1;
            status.admitted_records += count as u64;
            status.admitted_command_bytes += bytes as u64;
            status.max_batch_records = status.max_batch_records.max(count);
            status.max_batch_command_bytes = status.max_batch_command_bytes.max(bytes);
        });
    }
    let result = execute_batch_responses(
        Some(node),
        storage,
        transport,
        shared,
        pending,
        leader,
        effects,
        responses,
    );
    shared.storage_metrics(storage.metrics());
    result?;
    *leader = node.known_leader();
    shared.refresh_status(node, *leader);
    reconcile_admin(node, shared, pending, *leader);
    update_routes(node, pending);
    complete_fenced_reads(shared, pending);
    Ok(())
}

// Failure before ProposeAccepted still follows core admission and may leave a
// partial WAL. Every remaining waiter must receive an explicit unknown outcome.
#[derive(Default)]
struct ProposalResponses(VecDeque<oneshot::Sender<ProposalResult>>);
impl Drop for ProposalResponses {
    fn drop(&mut self) {
        for response in self.0.drain(..) {
            let _ = response.send(ProposalResult::OutcomeUnknown { leader_hint: None });
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn step_and_execute(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    last_known_leader: &mut Option<NodeId>,
    input: Input,
    proposal_response: Option<oneshot::Sender<ProposalResult>>,
) -> io::Result<()> {
    let effects = node.step(input);
    *last_known_leader = node.known_leader();
    execute_batch(
        Some(node),
        storage,
        transport,
        shared,
        pending,
        last_known_leader,
        effects,
        proposal_response,
    )?;
    shared.refresh_status(node, *last_known_leader);
    reconcile_admin(node, shared, pending, *last_known_leader);
    update_routes(node, pending);
    shared.storage_metrics(storage.metrics());
    complete_fenced_reads(shared, pending);
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
        let msg = match msg {
            RaftMessage::GroupMessage { message, .. } => message.as_ref(),
            other => other,
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
fn execute_batch(
    node: Option<&mut RaftNode>,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader: &mut Option<NodeId>,
    effects: Vec<Effect>,
    response: Option<oneshot::Sender<ProposalResult>>,
) -> io::Result<()> {
    execute_batch_responses(
        node,
        storage,
        transport,
        shared,
        pending,
        leader,
        effects,
        ProposalResponses(response.into_iter().collect()),
    )
}

#[allow(clippy::too_many_arguments)]
fn execute_batch_responses(
    mut node: Option<&mut RaftNode>,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    last_known_leader: &mut Option<NodeId>,
    effects: Vec<Effect>,
    mut proposal_responses: ProposalResponses,
) -> io::Result<()> {
    audit_persist_before_send(&effects)
        .map_err(|message| io::Error::new(io::ErrorKind::InvalidData, message))?;

    let mut queue: VecDeque<_> = effects.into();
    while let Some(effect) = queue.pop_front() {
        let mut callback = None;
        match effect {
            Effect::MembershipRouteHint {
                id,
                term,
                endpoints,
            } => {
                if let Some(routes) = &pending.routes {
                    routes.send_if_modified(|update| {
                        let hint = crate::membership_runtime::RouteHint { term, endpoints };
                        if term >= update.term
                            && !update.membership.endpoints.contains_key(&id)
                            && !update.membership.retired.contains(&id)
                            && update.hints.get(&id) != Some(&hint)
                        {
                            update.hints.insert(id, hint);
                            true
                        } else {
                            false
                        }
                    });
                }
            }
            Effect::MembershipResult {
                request_id,
                outcome,
            } => {
                if let MembershipOutcome::Rejected {
                    reason,
                    leader_hint,
                } = outcome
                {
                    if let Some(waiters) = pending.administration.remove(&request_id) {
                        for waiter in waiters {
                            let _ = waiter.respond_to.send(AdminResult::Rejected {
                                reason: reason.clone(),
                                leader_hint,
                            });
                        }
                    }
                }
                // Accepted and pending Recorded are not success; the post-batch
                // applied committed record is the only completion authority.
            }
            Effect::MembershipChanged { committed: _ } => {}
            Effect::ReadSnapshotChunk {
                to,
                transfer,
                descriptor,
                offset,
                max_len,
            } => {
                let runtime = pending
                    .snapshots
                    .as_ref()
                    .ok_or_else(|| io::Error::other("snapshot runtime is disabled"))?;
                let image = runtime
                    .active
                    .as_ref()
                    .filter(|image| image.descriptor() == &descriptor)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "outbound snapshot image unavailable",
                        )
                    })?;
                if max_len == 0
                    || max_len > raft_core::MAX_SNAPSHOT_CHUNK_BYTES
                    || offset >= descriptor.total_len
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "invalid outbound chunk request",
                    ));
                }
                let end = (offset as usize)
                    .saturating_add(max_len)
                    .min(image.bytes().len());
                callback = Some(Input::SnapshotChunkRead {
                    to,
                    transfer,
                    descriptor,
                    offset,
                    data: image.bytes()[offset as usize..end].to_vec(),
                });
            }
            Effect::StageSnapshotChunk {
                transfer,
                descriptor,
                offset,
                data,
                done,
            } => {
                node.as_deref()
                    .ok_or_else(|| io::Error::other("snapshot callback requires core"))?;
                let runtime = pending
                    .snapshots
                    .as_mut()
                    .ok_or_else(|| io::Error::other("snapshot runtime is disabled"))?;
                let result =
                    if !raft_core::snapshot::valid_chunk(&descriptor, offset, data.len(), done) {
                        raft_core::SnapshotStageResult::Rejected
                    } else {
                        match runtime.receiver.stage_authorized_chunk(
                            transfer,
                            &descriptor,
                            offset,
                            &data,
                            shared.status().last_applied,
                            &descriptor.metadata.members,
                            runtime.active.as_ref(),
                        ) {
                            Ok(chunk) => raft_core::SnapshotStageResult::Accepted {
                                next_offset: chunk.next_offset,
                                complete: chunk.complete,
                            },
                            Err(SnapshotReceiveError::Rejected(reason)) => {
                                tracing::warn!(%reason, "snapshot chunk rejected");
                                raft_core::SnapshotStageResult::Rejected
                            }
                            Err(SnapshotReceiveError::Io(error)) => return Err(error),
                        }
                    };
                callback = Some(Input::SnapshotChunkStaged {
                    transfer,
                    descriptor,
                    offset,
                    result,
                });
            }
            Effect::PublishSnapshot {
                transfer,
                descriptor,
                retained_entries,
            } => {
                let runtime = pending
                    .snapshots
                    .as_mut()
                    .ok_or_else(|| io::Error::other("snapshot runtime is disabled"))?;
                let image = match transfer {
                    Some(identity) => runtime.receiver.completed(identity).cloned(),
                    None => runtime.prepared.take(),
                }
                .filter(|image| image.descriptor() == &descriptor)
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "publication image differs from accepted descriptor",
                    )
                })?;
                storage.publish_snapshot(&image, &retained_entries)?;
                runtime.active = Some(image);
                if let Some(identity) = transfer {
                    runtime.receiver.cancel(identity)?;
                }
                shared.snapshot_error(None);
                info!(
                    index = descriptor.metadata.last_included_index,
                    bytes = descriptor.total_len,
                    retained_entries = retained_entries.len(),
                    "snapshot generation published"
                );
                callback = Some(Input::SnapshotPublished {
                    transfer,
                    descriptor,
                });
            }
            Effect::ApplySnapshot { descriptor } => {
                let runtime = pending
                    .snapshots
                    .as_mut()
                    .ok_or_else(|| io::Error::other("snapshot runtime is disabled"))?;
                let image = runtime
                    .active
                    .as_ref()
                    .filter(|image| image.descriptor() == &descriptor)
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "applied snapshot differs from published image",
                        )
                    })?;
                if let Some(store) = pending.application_store.as_mut() {
                    store.install(image)?;
                }
                shared.install_snapshot(image.clone())?;
                if let Some(store) = &pending.application_store {
                    shared.applied_store_status(store.status()?);
                }
                // Pending proposal identities covered by an installed image have
                // no retained client waiter proof of their original outcome.
                let covered: Vec<_> = pending
                    .writes
                    .keys()
                    .copied()
                    .take_while(|index| *index <= descriptor.metadata.last_included_index)
                    .collect();
                for index in covered {
                    if let Some(waiter) = pending.writes.remove(&index) {
                        let _ = waiter.send(ProposalResult::OutcomeUnknown {
                            leader_hint: *last_known_leader,
                        });
                    }
                }
            }
            Effect::CancelSnapshot { transfer } => {
                if let Some(runtime) = pending.snapshots.as_mut() {
                    runtime.receiver.cancel(transfer)?;
                }
            }
            Effect::SnapshotRejected { reason } => {
                if let Some(runtime) = pending.snapshots.as_mut() {
                    runtime.prepared = None;
                }
                shared.snapshot_error(Some(reason.to_owned()));
                tracing::warn!(reason, "snapshot request rejected; durable log retained");
            }
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
                    reject_pending_reads(pending, "leadership_lost", *last_known_leader);
                }
            }
            Effect::Apply(entry) => {
                let outcomes = if let Some(store) = pending.application_store.as_mut() {
                    let mut entries = vec![entry];
                    while entries.len() < state_store::MAX_APPLY_RANGE as usize
                        && matches!(queue.front(), Some(Effect::Apply(_)))
                    {
                        let Some(Effect::Apply(entry)) = queue.pop_front() else {
                            unreachable!()
                        };
                        entries.push(entry);
                    }
                    let outcomes = shared.apply_group(&entries, store)?;
                    // A changed-cell byte bound can split a prefix below the
                    // record limit. Return the untouched suffix to the front.
                    for entry in entries.into_iter().skip(outcomes.len()).rev() {
                        queue.push_front(Effect::Apply(entry));
                    }
                    outcomes
                } else {
                    let index = entry.index;
                    let outcome = shared
                        .apply(&entry)
                        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    vec![(index, outcome)]
                };
                for (index, outcome) in outcomes {
                    if let Some(respond_to) = pending.remove(&index) {
                        let result = match outcome {
                            ApplyOutcome::Applied { index } => ProposalResult::Applied { index },
                            ApplyOutcome::Session(result) => ProposalResult::Session(result),
                        };
                        let _ = respond_to.send(result);
                    }
                }
            }
            Effect::ReadReady {
                request_id,
                context,
                index,
                term,
            } => {
                if let Some(read) = pending.reads.get_mut(&request_id) {
                    read.barrier = Some(ReadBarrier {
                        index,
                        term,
                        context,
                    });
                }
            }
            Effect::ReadRejected {
                request_id,
                leader_hint,
                reason,
            } => {
                if let Some(read) = pending.reads.remove(&request_id) {
                    let _ = read.respond_to.send(ReadResult::Rejected {
                        reason: reason.as_str(),
                        leader_hint,
                    });
                }
            }
            Effect::ProposeAccepted { index } => {
                let Some(respond_to) = proposal_responses.0.pop_front() else {
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
                let Some(respond_to) = proposal_responses.0.pop_front() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "proposal rejected without an active client request",
                    ));
                };
                let _ = respond_to.send(ProposalResult::NotLeader { leader_hint });
            }
        }
        if let Some(input) = callback {
            let core = node
                .as_deref_mut()
                .ok_or_else(|| io::Error::other("durable snapshot callback requires core"))?;
            let next = core.step(input);
            audit_persist_before_send(&next)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            for effect in next.into_iter().rev() {
                queue.push_front(effect);
            }
        }
    }

    if !proposal_responses.0.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "client proposal produced no acceptance or rejection effect",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
fn execute_in_order(
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader: &mut Option<NodeId>,
    effects: Vec<Effect>,
    response: Option<oneshot::Sender<ProposalResult>>,
) -> io::Result<()> {
    execute_batch(
        None, storage, transport, shared, pending, leader, effects, response,
    )
}

fn maybe_compact(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader: &mut Option<NodeId>,
) -> io::Result<()> {
    maybe_compact_with(
        node,
        storage,
        transport,
        shared,
        pending,
        leader,
        |metadata| shared.snapshot_image(metadata),
    )
}

#[allow(clippy::too_many_arguments)]
fn maybe_compact_with(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader: &mut Option<NodeId>,
    capture: impl FnOnce(SnapshotMetadata) -> io::Result<SnapshotImage>,
) -> io::Result<()> {
    let Some(runtime) = pending.snapshots.as_mut() else {
        return Ok(());
    };
    let applied = shared.status().last_applied;
    if runtime.threshold == 0
        || applied.saturating_sub(runtime.last_attempt.max(node.snapshot_index()))
            < runtime.threshold
    {
        return Ok(());
    }
    runtime.last_attempt = applied;
    let term = node.log_term(applied).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "applied boundary has no retained term",
        )
    })?;
    let metadata = SnapshotMetadata {
        membership: node
            .hard
            .membership
            .as_ref()
            .map(|_| node.committed_membership().clone()),
        last_included_index: applied,
        last_included_term: term,
        members: node
            .committed_membership()
            .state
            .participants()
            .into_iter()
            .collect(),
    };
    let image = match capture(metadata) {
        Ok(image) => image,
        Err(error) => {
            shared.snapshot_error(Some(error.to_string()));
            tracing::error!(%error, applied, "snapshot capture refused; WAL retained until a later bounded attempt");
            return Ok(());
        }
    };
    let descriptor = image.descriptor().clone();
    runtime.prepared = Some(image);
    step_and_execute(
        node,
        storage,
        transport,
        shared,
        pending,
        leader,
        Input::Compact { descriptor },
        None,
    )
}

fn mark_pending_outcomes_unknown(pending: &mut PendingRequests, leader_hint: Option<NodeId>) {
    for (_, respond_to) in std::mem::take(&mut pending.writes) {
        let _ = respond_to.send(ProposalResult::OutcomeUnknown { leader_hint });
    }
}

fn reject_queued_reads(reads: &mut mpsc::Receiver<ReadRequest>, reason: &'static str) {
    while let Ok(read) = reads.try_recv() {
        let _ = read.respond_to.send(ReadResult::Rejected {
            reason,
            leader_hint: None,
        });
    }
}
fn reject_pending_reads(
    pending: &mut PendingRequests,
    reason: &'static str,
    leader_hint: Option<NodeId>,
) {
    for (_, read) in std::mem::take(&mut pending.reads) {
        let _ = read.respond_to.send(ReadResult::Rejected {
            reason,
            leader_hint,
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn process_read_request(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader_hint: &mut Option<NodeId>,
    request: ReadRequest,
) -> io::Result<()> {
    if request.respond_to.is_closed() {
        return Ok(());
    }
    let Some(request_id) = pending.next_read_id.checked_add(1) else {
        let _ = request.respond_to.send(ReadResult::Rejected {
            reason: "read_context_exhausted",
            leader_hint: *leader_hint,
        });
        return Ok(());
    };
    if pending.reads.len() >= raft_core::MAX_PENDING_READS {
        let _ = request.respond_to.send(ReadResult::Rejected {
            reason: "read_capacity",
            leader_hint: *leader_hint,
        });
        return Ok(());
    }
    pending.next_read_id = request_id;
    pending.reads.insert(
        request_id,
        PendingRead {
            key: request.key,
            respond_to: request.respond_to,
            deadline: Instant::now() + READ_TIMEOUT,
            barrier: None,
        },
    );
    step_and_execute(
        node,
        storage,
        transport,
        shared,
        pending,
        leader_hint,
        Input::ReadIndex { request_id },
        None,
    )
}

fn cancel_expired_reads(
    node: &mut RaftNode,
    storage: &mut impl DurableStore,
    transport: &impl Transport,
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    leader_hint: &mut Option<NodeId>,
) -> io::Result<()> {
    let expired: Vec<_> = pending
        .reads
        .iter()
        .filter_map(|(id, read)| {
            (read.respond_to.is_closed() || Instant::now() >= read.deadline).then_some(*id)
        })
        .collect();
    for request_id in expired {
        if let Some(read) = pending.reads.remove(&request_id) {
            let _ = read.respond_to.send(ReadResult::Rejected {
                reason: "read_timeout",
                leader_hint: *leader_hint,
            });
        }
        step_and_execute(
            node,
            storage,
            transport,
            shared,
            pending,
            leader_hint,
            Input::CancelRead { request_id },
            None,
        )?;
    }
    Ok(())
}

fn complete_fenced_reads(shared: &SharedReadState, pending: &mut PendingRequests) {
    complete_reads_with_fence(shared, pending, |applied, required| applied >= required);
}

fn complete_reads_with_fence(
    shared: &SharedReadState,
    pending: &mut PendingRequests,
    mut applied_through: impl FnMut(LogIndex, LogIndex) -> bool,
) {
    let mut completed = Vec::new();
    for (&request_id, read) in &pending.reads {
        let Some(barrier) = read.barrier else {
            continue;
        };
        let (status, value) = shared.get(&read.key);
        let result = if Instant::now() >= read.deadline {
            Some(ReadResult::Rejected {
                reason: "read_timeout",
                leader_hint: status.leader_hint,
            })
        } else if status.role != NodeRole::Leader || status.term != barrier.term {
            Some(ReadResult::Rejected {
                reason: "leadership_lost",
                leader_hint: status.leader_hint,
            })
        } else if applied_through(status.last_applied, barrier.index) {
            Some(ReadResult::Ready {
                value,
                index: barrier.index,
                applied_index: status.last_applied,
                term: barrier.term,
                context: barrier.context,
            })
        } else {
            None
        };
        if let Some(result) = result {
            completed.push((request_id, result));
        }
    }
    for (request_id, result) in completed {
        if let Some(read) = pending.reads.remove(&request_id) {
            let _ = read.respond_to.send(result);
        }
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

    #[derive(Default)]
    struct ProbeStore {
        events: Arc<Mutex<Vec<&'static str>>>,
        appended: Option<oneshot::Sender<()>>,
        request_on_append: Option<shutdown::ShutdownHandle>,
        fail_append: bool,
        fail_flush: bool,
    }

    impl DurableStore for ProbeStore {
        fn save_hard_state(&mut self, _: &HardState) -> io::Result<()> {
            self.events.lock().unwrap().push("hard");
            Ok(())
        }

        fn append_entries(&mut self, _: Option<LogIndex>, _: &[Entry]) -> io::Result<()> {
            self.events.lock().unwrap().push("persist");
            if let Some(controller) = self.request_on_append.take() {
                controller.request();
                self.events.lock().unwrap().push("request");
            }
            if let Some(appended) = self.appended.take() {
                let _ = appended.send(());
            }
            if self.fail_append {
                return Err(io::Error::other("injected append failure"));
            }
            Ok(())
        }

        fn flush(&mut self) -> io::Result<()> {
            self.events.lock().unwrap().push("flush");
            if self.fail_flush {
                Err(io::Error::other("injected final flush failure"))
            } else {
                Ok(())
            }
        }
    }

    struct OrderedTransport(Arc<Mutex<Vec<&'static str>>>);

    impl Transport for OrderedTransport {
        fn send(&self, _: NodeId, _: RaftMessage) {
            self.0.lock().unwrap().push("send");
        }
    }

    struct SnapshotStoreProbe {
        events: Arc<Mutex<Vec<&'static str>>>,
        shared: SharedReadState,
        fail_publication: bool,
    }
    impl DurableStore for SnapshotStoreProbe {
        fn save_hard_state(&mut self, _: &HardState) -> io::Result<()> {
            self.events.lock().unwrap().push("hard");
            Ok(())
        }
        fn append_entries(&mut self, _: Option<LogIndex>, _: &[Entry]) -> io::Result<()> {
            Ok(())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn publish_snapshot(&mut self, _: &SnapshotImage, retained: &[Entry]) -> io::Result<()> {
            assert!(retained.is_empty());
            assert_eq!(
                self.shared.status().last_applied,
                0,
                "application cannot precede publication"
            );
            self.events.lock().unwrap().push("publish");
            if self.fail_publication {
                Err(io::Error::other("injected snapshot publication failure"))
            } else {
                Ok(())
            }
        }
    }
    struct SnapshotAckProbe {
        events: Arc<Mutex<Vec<&'static str>>>,
        shared: SharedReadState,
    }
    impl Transport for SnapshotAckProbe {
        fn send(&self, _: NodeId, message: RaftMessage) {
            if matches!(
                message,
                RaftMessage::InstallSnapshotReply {
                    installed: true,
                    ..
                }
            ) {
                assert!(self.events.lock().unwrap().contains(&"publish"));
                assert!(
                    self.shared.status().last_applied >= 1,
                    "final ACK cannot precede application"
                );
                self.events.lock().unwrap().push("installed_ack");
            }
        }
    }
    fn install_fixture() -> SnapshotImage {
        let mut application = crate::application::StateMachine::default();
        application
            .apply(&Entry {
                index: 1,
                term: 1,
                command: Command::Put {
                    key: "installed".into(),
                    value: "value".into(),
                },
            })
            .unwrap();
        SnapshotImage::new(
            SnapshotMetadata {
                membership: None,
                last_included_index: 1,
                last_included_term: 1,
                members: vec![1, 2, 3],
            },
            &application,
        )
        .unwrap()
    }
    fn install_input(image: &SnapshotImage) -> Input {
        Input::Message {
            from: 1,
            msg: RaftMessage::InstallSnapshot {
                transfer: raft_core::SnapshotTransferId {
                    leader_id: 1,
                    term: 2,
                    incarnation: 9,
                    sequence: 1,
                },
                descriptor: image.descriptor().clone(),
                offset: 0,
                data: image.bytes().to_vec(),
                done: true,
                contact_round: 1,
            },
        }
    }
    #[test]
    fn snapshot_publication_callback_installs_before_ack_and_failure_sends_no_success() {
        for fail_publication in [false, true] {
            let dir = TestDir::new();
            let mut node = RaftNode::new(2, vec![1, 3], 7);
            let shared = SharedReadState::from_node(&node);
            let events = Arc::new(Mutex::new(Vec::new()));
            let mut storage = SnapshotStoreProbe {
                events: events.clone(),
                shared: shared.clone(),
                fail_publication,
            };
            let transport = SnapshotAckProbe {
                events: events.clone(),
                shared: shared.clone(),
            };
            let mut pending = PendingRequests::new();
            pending.snapshots = Some(SnapshotRuntime::new(dir.path(), None, 0).unwrap());
            let image = install_fixture();
            let result = step_and_execute(
                &mut node,
                &mut storage,
                &transport,
                &shared,
                &mut pending,
                &mut None,
                install_input(&image),
                None,
            );
            if fail_publication {
                assert!(result.is_err());
                assert_eq!(node.snapshot_index(), 0);
                assert_eq!(shared.status().last_applied, 0);
                assert_eq!(*events.lock().unwrap(), vec!["hard", "publish"]);
            } else {
                result.unwrap();
                assert_eq!(node.snapshot_index(), 1);
                assert_eq!(shared.get("installed").1.as_deref(), Some("value"));
                assert_eq!(
                    *events.lock().unwrap(),
                    vec!["hard", "publish", "installed_ack"]
                );
                // A final-chunk retransmission after the receiver slot was cleaned
                // verifies active immutable bytes without publishing/applying again.
                step_and_execute(
                    &mut node,
                    &mut storage,
                    &transport,
                    &shared,
                    &mut pending,
                    &mut None,
                    install_input(&image),
                    None,
                )
                .unwrap();
                assert_eq!(
                    *events.lock().unwrap(),
                    vec!["hard", "publish", "installed_ack", "installed_ack"]
                );
                assert_eq!(
                    fs::metadata(dir.path().join("snapshot-install.tmp"))
                        .unwrap()
                        .len(),
                    0
                );
            }
        }
    }
    #[test]
    fn snapshot_stage_filesystem_failure_is_fatal_without_publish_or_ack() {
        let dir = TestDir::new();
        let mut node = RaftNode::new(2, vec![1, 3], 7);
        let shared = SharedReadState::from_node(&node);
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut storage = SnapshotStoreProbe {
            events: events.clone(),
            shared: shared.clone(),
            fail_publication: false,
        };
        let transport = SnapshotAckProbe {
            events: events.clone(),
            shared: shared.clone(),
        };
        let mut pending = PendingRequests::new();
        pending.snapshots = Some(SnapshotRuntime::new(dir.path(), None, 0).unwrap());
        fs::create_dir(dir.path().join("snapshot-install.tmp")).unwrap();
        assert!(step_and_execute(
            &mut node,
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut None,
            install_input(&install_fixture()),
            None
        )
        .is_err());
        assert_eq!(*events.lock().unwrap(), vec!["hard"]);
        assert_eq!(shared.status().last_applied, 0);
    }
    #[test]
    fn refused_capture_retains_wal_and_retries_only_after_another_threshold() {
        let dir = TestDir::new();
        let entries: Vec<_> = (1..=4)
            .map(|index| Entry {
                index,
                term: 1,
                command: Command::NoOp,
            })
            .collect();
        let mut node = RaftNode::restore(
            1,
            vec![2, 3],
            1,
            HardState {
                membership: None,
                current_term: 1,
                voted_for: None,
            },
            entries.clone(),
        )
        .unwrap();
        let shared = SharedReadState::from_node(&node);
        for entry in &entries[..2] {
            shared.apply(entry).unwrap();
        }
        node.commit_index = 2;
        node.last_applied = 2;
        let mut pending = PendingRequests::new();
        pending.snapshots = Some(SnapshotRuntime::new(dir.path(), None, 2).unwrap());
        let mut storage = ProbeStore::default();
        let transport = RecordingTransport::default();
        let attempts = std::cell::Cell::new(0);
        let capture = |_: SnapshotMetadata| {
            attempts.set(attempts.get() + 1);
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot exceeds capacity",
            ))
        };
        for _ in 0..3 {
            maybe_compact_with(
                &mut node,
                &mut storage,
                &transport,
                &shared,
                &mut pending,
                &mut None,
                capture,
            )
            .unwrap();
        }
        assert_eq!(attempts.get(), 1);
        assert!(shared.status().snapshot_error.unwrap().contains("capacity"));
        assert_eq!(node.log, entries);
        assert_eq!(node.snapshot_index(), 0);
        assert!(storage.events.lock().unwrap().is_empty());
        for entry in &entries[2..] {
            shared.apply(entry).unwrap();
        }
        node.commit_index = 4;
        node.last_applied = 4;
        maybe_compact_with(
            &mut node,
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut None,
            capture,
        )
        .unwrap();
        assert_eq!(attempts.get(), 2);
        assert_eq!(node.log, entries);
    }

    fn shutdown_leader() -> (RaftNode, SharedReadState) {
        let mut node = RaftNode::new(1, vec![2, 3], 3);
        node.step(Input::Tick { now_ms: 0 });
        let effects = node.step(Input::Tick {
            now_ms: node.election_deadline(),
        });
        let (prospective_term, campaign_id) = effects
            .into_iter()
            .find_map(|effect| {
                if let Effect::Send {
                    msg:
                        RaftMessage::PreVote {
                            prospective_term,
                            campaign_id,
                            ..
                        },
                    ..
                } = effect
                {
                    Some((prospective_term, campaign_id))
                } else {
                    None
                }
            })
            .expect("real pre-campaign");
        node.step(Input::Message {
            from: 2,
            msg: RaftMessage::PreVoteReply {
                term: 0,
                prospective_term,
                campaign_id,
                vote_granted: true,
            },
        });
        node.step(Input::Message {
            from: 2,
            msg: RaftMessage::RequestVoteReply {
                term: prospective_term,
                vote_granted: true,
            },
        });
        assert!(node.is_leader());
        // The elected leader has its real contact window and initial no-op.
        // The fixture begins before the no-op receives a replication reply.
        assert_eq!(node.log.len(), 1);
        let shared = SharedReadState::from_node(&node);
        (node, shared)
    }

    fn shutdown_proposal() -> (ProposalRequest, oneshot::Receiver<ProposalResult>) {
        let (respond_to, response) = oneshot::channel();
        (
            ProposalRequest {
                command: Command::Put {
                    key: "shutdown-key".into(),
                    value: "value".into(),
                },
                respond_to,
            },
            response,
        )
    }

    #[tokio::test]
    async fn shutdown_rejects_queued_requests_before_the_core_sees_them() {
        let (node, shared) = shutdown_leader();
        let store = ProbeStore::default();
        let events = store.events.clone();
        let transport = OrderedTransport(events.clone());
        let (_inbound, inbound_rx) = mpsc::channel(4);
        let (proposals, proposal_rx) = mpsc::channel(4);
        let (first, first_response) = shutdown_proposal();
        let (second, second_response) = shutdown_proposal();
        proposals.try_send(first).unwrap();
        proposals.try_send(second).unwrap();
        let (controller, shutdown) = shutdown::channel();
        controller.request();

        let report = run_driver_with_shutdown(
            node,
            store,
            transport,
            inbound_rx,
            proposal_rx,
            shared,
            shutdown,
        )
        .await
        .unwrap();
        assert_eq!(report.queued_rejected, 2);
        assert_eq!(report.pending_unknown, 0);
        assert_eq!(report.uncommitted_entries, 1);
        assert_eq!(first_response.await.unwrap(), ProposalResult::ShuttingDown);
        assert_eq!(second_response.await.unwrap(), ProposalResult::ShuttingDown);
        assert!(proposals.is_closed());
        assert_eq!(*events.lock().unwrap(), vec!["flush"]);
    }

    #[tokio::test]
    async fn shutdown_finishes_started_batch_then_bounds_quorum_loss_with_unknown_outcome() {
        let (node, shared) = shutdown_leader();
        let (controller, shutdown) = shutdown::channel();
        let store = ProbeStore {
            request_on_append: Some(controller),
            ..ProbeStore::default()
        };
        let events = store.events.clone();
        let transport = OrderedTransport(events.clone());
        let (_inbound, inbound_rx) = mpsc::channel(4);
        let (proposals, proposal_rx) = mpsc::channel(4);
        let (request, response) = shutdown_proposal();
        proposals.try_send(request).unwrap();

        let report = tokio::time::timeout(
            Duration::from_secs(1),
            run_driver_with_limit(
                node,
                store,
                transport,
                inbound_rx,
                proposal_rx,
                shared,
                shutdown,
                Duration::from_millis(20),
            ),
        )
        .await
        .expect("drain is bounded without quorum")
        .unwrap();
        assert!(report.deadline_expired);
        assert_eq!(report.pending_unknown, 1);
        assert_eq!(report.uncommitted_entries, 2);
        assert_eq!(
            response.await.unwrap(),
            ProposalResult::OutcomeUnknown {
                leader_hint: Some(1)
            }
        );
        let events = events.lock().unwrap();
        let requested = events.iter().position(|event| *event == "request").unwrap();
        assert_eq!(events[requested - 1], "persist");
        // pending_unknown == 1 above proves ProposeAccepted ran after the
        // persistence operation requested shutdown; the batch was not cut short.
        assert!(requested < events.len() - 1);
        assert_eq!(events.last(), Some(&"flush"));
    }

    #[tokio::test]
    async fn shutdown_can_finish_an_accepted_write_using_existing_peer_replies() {
        let (node, shared) = shutdown_leader();
        let (controller, shutdown) = shutdown::channel();
        let (appended, append_seen) = oneshot::channel();
        let store = ProbeStore {
            request_on_append: Some(controller),
            appended: Some(appended),
            ..ProbeStore::default()
        };
        let events = store.events.clone();
        let transport = OrderedTransport(events.clone());
        let (inbound, inbound_rx) = mpsc::channel(4);
        let (proposals, proposal_rx) = mpsc::channel(4);
        let (request, response) = shutdown_proposal();
        proposals.try_send(request).unwrap();
        let driver = tokio::spawn(run_driver_with_limit(
            node,
            store,
            transport,
            inbound_rx,
            proposal_rx,
            shared.clone(),
            shutdown,
            Duration::from_secs(1),
        ));
        append_seen.await.unwrap();
        inbound
            .send((
                2,
                RaftMessage::AppendEntriesReply {
                    conflict: None,
                    contact_round: 0,
                    term: 1,
                    success: true,
                    match_index: 2,
                },
            ))
            .await
            .unwrap();
        assert_eq!(
            response.await.unwrap(),
            ProposalResult::Applied { index: 2 }
        );
        let report = tokio::time::timeout(Duration::from_secs(2), driver)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!report.deadline_expired);
        assert_eq!(report.pending_unknown, 0);
        assert_eq!(report.uncommitted_entries, 0);
        assert_eq!(shared.get("shutdown-key").1, Some("value".into()));
        assert_eq!(events.lock().unwrap().last(), Some(&"flush"));
    }

    #[tokio::test]
    async fn required_final_flush_failure_is_returned_to_the_process_boundary() {
        let (node, shared) = shutdown_leader();
        let store = ProbeStore {
            fail_flush: true,
            ..ProbeStore::default()
        };
        let events = store.events.clone();
        let (_inbound, inbound_rx) = mpsc::channel(4);
        let (_proposals, proposal_rx) = mpsc::channel(4);
        let (controller, shutdown) = shutdown::channel();
        controller.request();
        let error = run_driver_with_shutdown(
            node,
            store,
            OrderedTransport(events.clone()),
            inbound_rx,
            proposal_rx,
            shared,
            shutdown,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("injected final flush failure"));
        assert_eq!(*events.lock().unwrap(), vec!["flush"]);
    }

    #[tokio::test]
    async fn a_later_flush_error_does_not_hide_the_first_persistence_failure() {
        let (node, shared) = shutdown_leader();
        let store = ProbeStore {
            fail_append: true,
            fail_flush: true,
            ..ProbeStore::default()
        };
        let events = store.events.clone();
        let (_inbound, inbound_rx) = mpsc::channel(4);
        let (proposals, proposal_rx) = mpsc::channel(4);
        let (request, response) = shutdown_proposal();
        proposals.try_send(request).unwrap();
        let (_controller, shutdown) = shutdown::channel();
        let error = run_driver_with_shutdown(
            node,
            store,
            OrderedTransport(events.clone()),
            inbound_rx,
            proposal_rx,
            shared,
            shutdown,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("injected append failure"));
        assert_eq!(
            response.await.unwrap(),
            ProposalResult::OutcomeUnknown { leader_hint: None }
        );
        let events = events.lock().unwrap();
        let persisted = events.iter().position(|event| *event == "persist").unwrap();
        assert_eq!(
            &events[persisted..],
            &["persist", "flush"],
            "no dependent send may follow the failed persistence operation"
        );
    }
    fn append_reply() -> RaftMessage {
        RaftMessage::AppendEntriesReply {
            conflict: None,
            contact_round: 0,
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
                membership: None,
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
            &mut PendingRequests::new(),
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
        let mut pending = PendingRequests::new();
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
        assert_eq!(
            response.await.unwrap(),
            ProposalResult::OutcomeUnknown { leader_hint: None }
        );
    }

    #[tokio::test]
    async fn client_completes_only_when_its_entry_is_applied() {
        let dir = TestDir::new();
        let (mut storage, _, _) = Storage::open(dir.path()).expect("open storage");
        let node = RaftNode::new(1, vec![2, 3], 3);
        let shared = SharedReadState::from_node(&node);
        let transport = RecordingTransport::default();
        let mut pending = PendingRequests::new();
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
        let mut pending = PendingRequests::new();
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
        let mut pending = PendingRequests::new();
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
        let mut pending = PendingRequests::new();
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
    #[tokio::test]
    async fn session_completion_replays_original_result_after_duplicate_apply() {
        let node = RaftNode::new(1, vec![2, 3], 1);
        let shared = SharedReadState::from_node(&node);
        let mut storage = ProbeStore::default();
        let transport = RecordingTransport::default();
        let mut pending = PendingRequests::new();
        let (first_tx, first_rx) = oneshot::channel();
        let (retry_tx, retry_rx) = oneshot::channel();
        pending.insert(2, first_tx);
        pending.insert(4, retry_tx);
        let command = Command::SessionPut {
            session_id: 1,
            key: "protected".into(),
            sequence: 1,
            value: "first".into(),
        };
        let commands = [
            Command::RegisterSession {
                nonce: "driver-replay".into(),
            },
            command.clone(),
            Command::Put {
                key: "protected".into(),
                value: "later".into(),
            },
            command,
        ];
        let effects = commands
            .into_iter()
            .enumerate()
            .map(|(offset, command)| {
                Effect::Apply(Entry {
                    index: offset as u64 + 1,
                    term: 1,
                    command,
                })
            })
            .collect();
        execute_in_order(
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut None,
            effects,
            None,
        )
        .unwrap();
        let first = first_rx.await.unwrap();
        let retry = retry_rx.await.unwrap();
        assert_eq!(retry, first);
        let ProposalResult::Session(result) = retry else {
            panic!("session response");
        };
        assert_eq!(result.index, Some(2));
        assert_eq!(shared.get("protected").1.as_deref(), Some("later"));
        assert_eq!(shared.status().last_applied, 4);
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn session_state_survives_a_canceled_waiter_and_replays_when_retried() {
        let node = RaftNode::new(1, vec![2, 3], 1);
        let shared = SharedReadState::from_node(&node);
        let mut storage = ProbeStore::default();
        let transport = RecordingTransport::default();
        let mut pending = PendingRequests::new();
        let (canceled_tx, canceled_rx) = oneshot::channel();
        drop(canceled_rx);
        pending.insert(2, canceled_tx);
        let command = Command::SessionPut {
            session_id: 1,
            key: "key".into(),
            sequence: 1,
            value: "value".into(),
        };
        execute_in_order(
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut None,
            vec![
                Effect::Apply(Entry {
                    index: 1,
                    term: 1,
                    command: Command::RegisterSession {
                        nonce: "canceled".into(),
                    },
                }),
                Effect::Apply(Entry {
                    index: 2,
                    term: 1,
                    command: command.clone(),
                }),
            ],
            None,
        )
        .unwrap();
        let (retry_tx, retry_rx) = oneshot::channel();
        pending.insert(3, retry_tx);
        execute_in_order(
            &mut storage,
            &transport,
            &shared,
            &mut pending,
            &mut None,
            vec![Effect::Apply(Entry {
                index: 3,
                term: 1,
                command,
            })],
            None,
        )
        .unwrap();
        let ProposalResult::Session(result) = retry_rx.await.unwrap() else {
            panic!()
        };
        assert_eq!(result.index, Some(2));
        assert_eq!(shared.get("key").1.as_deref(), Some("value"));
    }

    struct ReadRig {
        node: RaftNode,
        shared: SharedReadState,
        store: ProbeStore,
        transport: RecordingTransport,
        pending: PendingRequests,
        hint: Option<NodeId>,
    }
    impl ReadRig {
        fn singleton() -> Self {
            let node = RaftNode::new(1, vec![], 23);
            let shared = SharedReadState::from_node(&node);
            let mut rig = Self {
                node,
                shared,
                store: ProbeStore::default(),
                transport: RecordingTransport::default(),
                pending: PendingRequests::new(),
                hint: None,
            };
            rig.step(Input::Tick { now_ms: 0 });
            rig.step(Input::Tick {
                now_ms: rig.node.election_deadline(),
            });
            assert!(rig.node.is_leader());
            assert_eq!(rig.shared.status().last_applied, 1);
            rig
        }
        fn step(&mut self, input: Input) {
            step_and_execute(
                &mut self.node,
                &mut self.store,
                &self.transport,
                &self.shared,
                &mut self.pending,
                &mut self.hint,
                input,
                None,
            )
            .unwrap();
        }
        fn read(&mut self) -> oneshot::Receiver<ReadResult> {
            let (respond_to, response) = oneshot::channel();
            process_read_request(
                &mut self.node,
                &mut self.store,
                &self.transport,
                &self.shared,
                &mut self.pending,
                &mut self.hint,
                ReadRequest {
                    key: "fenced".into(),
                    respond_to,
                },
            )
            .unwrap();
            response
        }
        fn commit_with_held_application(&mut self) -> Entry {
            let effects = self.node.step(Input::ClientPropose {
                command: Command::Put {
                    key: "fenced".into(),
                    value: "new".into(),
                },
            });
            let entry = effects
                .iter()
                .find_map(|effect| match effect {
                    Effect::Apply(entry) => Some(entry.clone()),
                    _ => None,
                })
                .expect("singleton actually committed the command");
            let held = effects
                .into_iter()
                .filter(|effect| !matches!(effect, Effect::Apply(_)))
                .collect();
            let (respond_to, _response) = oneshot::channel();
            execute_in_order(
                &mut self.store,
                &self.transport,
                &self.shared,
                &mut self.pending,
                &mut self.hint,
                held,
                Some(respond_to),
            )
            .unwrap();
            self.shared.refresh_status(&self.node, self.hint);
            assert_eq!(self.node.commit_index, entry.index);
            assert_eq!(self.node.last_applied, entry.index);
            assert_eq!(self.shared.status().last_applied, entry.index - 1);
            entry
        }
    }

    #[test]
    fn read_index_waits_for_actual_application_and_does_not_append() {
        let mut rig = ReadRig::singleton();
        let entry = rig.commit_with_held_application();
        let log_len = rig.node.log.len();
        let mut response = rig.read();
        assert_eq!(
            rig.node.log.len(),
            log_len,
            "ReadIndex creates no log entry"
        );
        assert!(matches!(
            response.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let barrier = rig.pending.reads.values().next().unwrap().barrier.unwrap();
        assert_eq!(
            barrier.index, entry.index,
            "real core quorum confirmation reached the driver"
        );
        complete_fenced_reads(&rig.shared, &mut rig.pending);
        assert!(matches!(
            response.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        execute_in_order(
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            vec![Effect::Apply(entry.clone())],
            None,
        )
        .unwrap();
        rig.shared.refresh_status(&rig.node, rig.hint);
        complete_fenced_reads(&rig.shared, &mut rig.pending);
        assert!(matches!(response.try_recv().unwrap(), ReadResult::Ready {
            value: Some(value), index, applied_index, context, ..
        } if value == "new" && index == entry.index && applied_index >= index && context > 0));
    }

    #[test]
    fn executed_missing_apply_fence_mutant_returns_stale_state_and_is_detected() {
        let mut rig = ReadRig::singleton();
        let committed = rig.commit_with_held_application();
        let mut response = rig.read();
        let mut fault_hits = 0;
        complete_reads_with_fence(&rig.shared, &mut rig.pending, |applied, required| {
            assert!(
                applied < required,
                "defect must reach a lagging application"
            );
            fault_hits += 1;
            true
        });
        assert_eq!(
            fault_hits, 1,
            "executed completion path bypassed its apply fence"
        );
        let ReadResult::Ready {
            value,
            index,
            applied_index,
            ..
        } = response.try_recv().unwrap()
        else {
            panic!("the planted defect must expose a successful stale read");
        };
        // Independent committed-register witness, not the completion predicate:
        // the confirmed prefix contains this Put and cannot describe absence.
        let expected = match committed.command {
            Command::Put { value, .. } => Some(value),
            _ => unreachable!(),
        };
        assert_ne!(
            value, expected,
            "register witness detects the executed defect"
        );
        assert!(applied_index < index);
    }

    #[test]
    fn read_deadline_and_closed_waiter_remove_state_without_reusing_identity() {
        let mut rig = ReadRig::singleton();
        let _held = rig.commit_with_held_application();
        let mut expired = rig.read();
        let first_id = rig.pending.next_read_id;
        rig.pending.reads.get_mut(&first_id).unwrap().deadline =
            Instant::now() - Duration::from_millis(1);
        complete_fenced_reads(&rig.shared, &mut rig.pending);
        assert!(matches!(
            expired.try_recv().unwrap(),
            ReadResult::Rejected {
                reason: "read_timeout",
                ..
            }
        ));
        let cancelled = rig.read();
        let second_id = rig.pending.next_read_id;
        assert!(second_id > first_id);
        drop(cancelled);
        cancel_expired_reads(
            &mut rig.node,
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
        )
        .unwrap();
        assert!(rig.pending.reads.is_empty());
        rig.pending.next_read_id = u64::MAX;
        assert!(matches!(
            rig.read().try_recv().unwrap(),
            ReadResult::Rejected {
                reason: "read_context_exhausted",
                ..
            }
        ));
    }

    #[test]
    fn bounded_read_waiters_reject_capacity_and_role_loss_rejects_fenced_waiters() {
        let mut rig = ReadRig::singleton();
        let _held = rig.commit_with_held_application();
        let mut waiting = Vec::new();
        for _ in 0..raft_core::MAX_PENDING_READS {
            waiting.push(rig.read());
        }
        assert_eq!(rig.pending.reads.len(), raft_core::MAX_PENDING_READS);
        assert!(matches!(
            rig.read().try_recv().unwrap(),
            ReadResult::Rejected {
                reason: "read_capacity",
                ..
            }
        ));
        execute_in_order(
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            vec![Effect::RoleChanged {
                role_name: "Follower",
                term: 2,
            }],
            None,
        )
        .unwrap();
        for mut response in waiting {
            assert!(matches!(
                response.try_recv().unwrap(),
                ReadResult::Rejected {
                    reason: "leadership_lost",
                    ..
                }
            ));
        }
        assert!(rig.pending.reads.is_empty());
    }

    fn batch_request(command: Command) -> (ProposalRequest, oneshot::Receiver<ProposalResult>) {
        let (respond_to, response) = oneshot::channel();
        (
            ProposalRequest {
                command,
                respond_to,
            },
            response,
        )
    }
    fn batch_put(key: &str, value: String) -> (ProposalRequest, oneshot::Receiver<ProposalResult>) {
        batch_request(Command::Put {
            key: key.into(),
            value,
        })
    }

    #[tokio::test]
    async fn batch_collector_preserves_fifo_across_byte_limit_and_caps_cancel_scan() {
        let node = RaftNode::new(1, vec![], 3);
        let shared = SharedReadState::from_node(&node);
        let (_control, mut shutdown) = shutdown::channel();
        let (tx, mut rx) = mpsc::channel(32);
        let (first, _first_response) = batch_put("first", "x".repeat(600_000));
        let (second, _second_response) = batch_put("second", "y".repeat(600_000));
        let (third, _third_response) = batch_put("third", "last".into());
        tx.try_send(second).unwrap();
        tx.try_send(third).unwrap();
        let mut deferred = None;
        let options = BatchOptions {
            records: 16,
            delay: Duration::ZERO,
        };
        let first_batch = collect_proposals(
            first,
            &mut rx,
            &mut deferred,
            &mut shutdown,
            options,
            &shared,
        )
        .await;
        assert_eq!(first_batch.len(), 1);
        assert!(matches!(&first_batch[0].command, Command::Put { key, .. } if key == "first"));
        assert_eq!(rx.len(), 1);
        let second = next_proposal(&mut deferred, &mut rx).await.unwrap();
        let next = collect_proposals(
            second,
            &mut rx,
            &mut deferred,
            &mut shutdown,
            options,
            &shared,
        )
        .await;
        assert_eq!(next.len(), 2);
        assert!(matches!(&next[0].command, Command::Put { key, .. } if key == "second"));
        assert!(matches!(&next[1].command, Command::Put { key, .. } if key == "third"));
        assert!(
            next.iter()
                .map(|request| command_bytes(&request.command).unwrap())
                .sum::<usize>()
                <= MAX_BATCH_BYTES
        );
        for _ in 0..20 {
            let (request, response) = shutdown_proposal();
            drop(response);
            tx.try_send(request).unwrap();
        }
        let first = rx.recv().await.unwrap();
        assert!(collect_proposals(
            first,
            &mut rx,
            &mut deferred,
            &mut shutdown,
            options,
            &shared
        )
        .await
        .is_empty());
        assert_eq!(rx.len(), 4, "canceled work has a finite scan budget");
        assert_eq!(shared.status().batching.cancelled_before_admission, 16);
    }

    #[tokio::test]
    async fn batch_sparse_deadline_shutdown_and_oversized_admission_are_bounded() {
        let node = RaftNode::new(1, vec![], 3);
        let shared = SharedReadState::from_node(&node);
        let (control, mut shutdown) = shutdown::channel();
        let (_tx, mut rx) = mpsc::channel(2);
        let mut deferred = None;
        let (first, _response) = shutdown_proposal();
        let start = Instant::now();
        let batch = tokio::time::timeout(
            Duration::from_secs(1),
            collect_proposals(
                first,
                &mut rx,
                &mut deferred,
                &mut shutdown,
                BatchOptions::default(),
                &shared,
            ),
        )
        .await
        .unwrap();
        assert_eq!(batch.len(), 1);
        assert!(start.elapsed() >= Duration::from_millis(2));
        control.request();
        let (second, _response) = shutdown_proposal();
        assert_eq!(
            collect_proposals(
                second,
                &mut rx,
                &mut deferred,
                &mut shutdown,
                BatchOptions::default(),
                &shared
            )
            .await
            .len(),
            1
        );
        let (oversized, response) = batch_put("too-big", "x".repeat(MAX_BATCH_BYTES));
        assert!(collect_proposals(
            oversized,
            &mut rx,
            &mut deferred,
            &mut shutdown,
            BatchOptions::default(),
            &shared
        )
        .await
        .is_empty());
        assert_eq!(
            response.await.unwrap(),
            ProposalResult::AdmissionRejected {
                reason: "proposal_too_large"
            }
        );
        assert_eq!(shared.status().batching.oversized_rejected, 1);
    }

    #[tokio::test]
    async fn batch_persists_once_and_duplicate_session_response_does_not_repeat_mutation() {
        let mut rig = ReadRig::singleton();
        let (registration, response) = batch_request(Command::RegisterSession {
            nonce: "batch-nonce".into(),
        });
        process_proposal_batch(
            &mut rig.node,
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            vec![registration],
        )
        .unwrap();
        assert!(matches!(
            response.await.unwrap(),
            ProposalResult::Session(_)
        ));
        rig.store.events.lock().unwrap().clear();
        let command = Command::SessionPut {
            session_id: 2,
            key: "protected".into(),
            sequence: 1,
            value: "original".into(),
        };
        let (first, first_response) = batch_request(command.clone());
        let (other, other_response) = batch_put("protected", "independent".into());
        let (retry, retry_response) = batch_request(command);
        process_proposal_batch(
            &mut rig.node,
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            vec![first, other, retry],
        )
        .unwrap();
        assert_eq!(*rig.store.events.lock().unwrap(), vec!["persist"]);
        let first = first_response.await.unwrap();
        assert_eq!(first, retry_response.await.unwrap());
        assert!(matches!(
            first,
            ProposalResult::Session(SessionResult { index: Some(3), .. })
        ));
        assert_eq!(
            other_response.await.unwrap(),
            ProposalResult::Applied { index: 4 }
        );
        assert_eq!(
            rig.shared.get("protected").1.as_deref(),
            Some("independent")
        );
        assert_eq!(rig.shared.status().last_applied, 5);
    }

    #[tokio::test]
    async fn batch_sync_failure_marks_every_admitted_request_unknown_without_application() {
        let mut rig = ReadRig::singleton();
        rig.store.fail_append = true;
        let (a, a_response) = shutdown_proposal();
        let (b, b_response) = shutdown_proposal();
        assert!(process_proposal_batch(
            &mut rig.node,
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            vec![a, b]
        )
        .is_err());
        for response in [a_response, b_response] {
            assert_eq!(
                response.await.unwrap(),
                ProposalResult::OutcomeUnknown { leader_hint: None }
            );
        }
        assert_eq!(rig.shared.status().last_applied, 1);
        assert_eq!(rig.node.last_log_index(), 3);
        assert!(rig.pending.is_empty());
    }

    #[tokio::test]
    async fn batch_cancel_before_admission_skips_but_after_admission_still_applies() {
        let (mut node, shared) = shutdown_leader();
        let mut store = ProbeStore::default();
        let transport = RecordingTransport::default();
        let mut pending = PendingRequests::new();
        let mut hint = None;
        let (skipped, skipped_response) = batch_put("skipped", "never".into());
        drop(skipped_response);
        let (accepted, accepted_response) = batch_put("accepted", "durable".into());
        let (last, last_response) = batch_put("last", "value".into());
        process_proposal_batch(
            &mut node,
            &mut store,
            &transport,
            &shared,
            &mut pending,
            &mut hint,
            vec![skipped, accepted, last],
        )
        .unwrap();
        drop(accepted_response);
        assert_eq!(node.last_log_index(), 3);
        let term = node.hard.current_term;
        step_and_execute(
            &mut node,
            &mut store,
            &transport,
            &shared,
            &mut pending,
            &mut hint,
            Input::Message {
                from: 2,
                msg: RaftMessage::AppendEntriesReply {
                    conflict: None,
                    term,
                    success: true,
                    match_index: 3,
                    contact_round: 0,
                },
            },
            None,
        )
        .unwrap();
        assert_eq!(
            last_response.await.unwrap(),
            ProposalResult::Applied { index: 3 }
        );
        assert_eq!(shared.get("skipped").1, None);
        assert_eq!(shared.get("accepted").1.as_deref(), Some("durable"));
    }

    #[tokio::test]
    async fn batch_shutdown_preserves_started_group_and_rejects_the_remaining_queue() {
        let (node, shared) = shutdown_leader();
        let (control, shutdown) = shutdown::channel();
        let store = ProbeStore {
            request_on_append: Some(control),
            ..ProbeStore::default()
        };
        let events = store.events.clone();
        let (_inbound, inbound_rx) = mpsc::channel(1);
        let (tx, rx) = mpsc::channel(32);
        let mut replies = Vec::new();
        for _ in 0..20 {
            let (request, response) = shutdown_proposal();
            tx.try_send(request).unwrap();
            replies.push(response);
        }
        let report = run_driver_with_limit(
            node,
            store,
            RecordingTransport::default(),
            inbound_rx,
            rx,
            shared.clone(),
            shutdown,
            Duration::from_millis(20),
        )
        .await
        .unwrap();
        assert!(report.deadline_expired);
        assert_eq!(report.pending_unknown, 16);
        assert_eq!(report.queued_rejected, 4);
        for (index, response) in replies.into_iter().enumerate() {
            let outcome = response.await.unwrap();
            if index < 16 {
                assert!(matches!(outcome, ProposalResult::OutcomeUnknown { .. }));
            } else {
                assert_eq!(outcome, ProposalResult::ShuttingDown);
            }
        }
        assert_eq!(
            events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| **event == "persist")
                .count(),
            1
        );
        assert_eq!(shared.status().last_applied, 0);
    }

    #[tokio::test]
    async fn batch_busy_queue_services_a_peer_and_a_read_before_next_admission() {
        struct FairStore {
            peer: mpsc::Sender<(NodeId, RaftMessage)>,
            reads: mpsc::Sender<ReadRequest>,
            read: Option<ReadRequest>,
            calls: Arc<Mutex<usize>>,
        }
        impl DurableStore for FairStore {
            fn save_hard_state(&mut self, _: &HardState) -> io::Result<()> {
                Ok(())
            }
            fn append_entries(&mut self, _: Option<LogIndex>, entries: &[Entry]) -> io::Result<()> {
                *self.calls.lock().unwrap() += 1;
                assert_eq!(entries.len(), 16);
                self.peer
                    .try_send((
                        2,
                        RaftMessage::RequestVote {
                            term: 2,
                            candidate_id: 2,
                            last_log_index: 100,
                            last_log_term: 2,
                        },
                    ))
                    .unwrap();
                self.reads.try_send(self.read.take().unwrap()).unwrap();
                Ok(())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let (node, shared) = shutdown_leader();
        let (peer, inbound) = mpsc::channel(2);
        let (reads, read_rx) = mpsc::channel(2);
        let (respond_to, read_response) = oneshot::channel();
        let calls = Arc::new(Mutex::new(0));
        let store = FairStore {
            peer,
            reads,
            calls: calls.clone(),
            read: Some(ReadRequest {
                key: "k".into(),
                respond_to,
            }),
        };
        let (tx, proposals) = mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
        let mut responses = Vec::new();
        for _ in 0..PROPOSAL_CHANNEL_CAPACITY {
            let (request, response) = shutdown_proposal();
            tx.try_send(request).unwrap();
            responses.push(response);
        }
        let (control, shutdown) = shutdown::channel();
        let driver = tokio::spawn(run_driver_with_reads_and_shutdown(
            node,
            store,
            RecordingTransport::default(),
            inbound,
            proposals,
            read_rx,
            shared,
            shutdown,
        ));
        let read = tokio::time::timeout(Duration::from_secs(1), read_response)
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            read,
            ReadResult::Rejected {
                reason: "not_leader",
                ..
            }
        ));
        assert_eq!(
            *calls.lock().unwrap(),
            1,
            "peer step-down preceded the second batch"
        );
        for response in responses.drain(..16) {
            assert!(matches!(
                response.await.unwrap(),
                ProposalResult::OutcomeUnknown { .. }
            ));
        }
        control.request();
        tokio::time::timeout(Duration::from_secs(1), driver)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    struct AppliedProbe {
        index: u64,
        events: Arc<Mutex<Vec<&'static str>>>,
        ranges: Arc<Mutex<Vec<(u64, u64)>>>,
        fail_commit: bool,
        fail_install: bool,
        fail_flush: bool,
    }
    impl state_store::StateStore for AppliedProbe {
        fn applied_index(&self) -> u64 {
            self.index
        }
        fn get(&mut self, _: &[u8]) -> io::Result<Option<Vec<u8>>> {
            unreachable!()
        }
        fn scan(&mut self) -> io::Result<state_store::Rows> {
            unreachable!()
        }
        fn commit_range(
            &mut self,
            first: u64,
            last: u64,
            _: &[state_store::Mutation],
        ) -> io::Result<state_store::CommitOutcome> {
            self.events.lock().unwrap().push("engine_commit");
            self.ranges.lock().unwrap().push((first, last));
            if self.fail_commit {
                return Err(io::Error::other("injected engine commit failure"));
            }
            self.index = last;
            Ok(state_store::CommitOutcome::Applied)
        }
        fn install(&mut self, index: u64, _: &[state_store::Mutation]) -> io::Result<()> {
            self.events.lock().unwrap().push("engine_install");
            if self.fail_install {
                return Err(io::Error::other("injected engine install failure"));
            }
            self.index = index;
            Ok(())
        }
        fn flush(&mut self) -> io::Result<()> {
            self.events.lock().unwrap().push("engine_flush");
            if self.fail_flush {
                Err(io::Error::other("injected engine flush failure"))
            } else {
                Ok(())
            }
        }
        fn statistics(&self) -> io::Result<state_store::Statistics> {
            Ok(Default::default())
        }
    }
    fn applied_probe(node: &RaftNode, events: Arc<Mutex<Vec<&'static str>>>) -> AppliedProbe {
        AppliedProbe {
            index: node.last_applied,
            events,
            ranges: Arc::new(Mutex::new(Vec::new())),
            fail_commit: false,
            fail_install: false,
            fail_flush: false,
        }
    }

    #[test]
    fn engine_group_commit_splits_at_every_non_apply_effect() {
        let mut rig = ReadRig::singleton();
        let events = Arc::new(Mutex::new(Vec::new()));
        let probe = applied_probe(&rig.node, events.clone());
        let ranges = probe.ranges.clone();
        rig.pending.application_store =
            Some(crate::applied_store::injected(Box::new(probe), &rig.node));
        let apply = |index| {
            Effect::Apply(Entry {
                index,
                term: 1,
                command: Command::Put {
                    key: format!("key{index}"),
                    value: "value".into(),
                },
            })
        };
        execute_batch(
            None,
            &mut rig.store,
            &OrderedTransport(events.clone()),
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            vec![
                apply(2),
                apply(3),
                Effect::Send {
                    to: 2,
                    msg: RaftMessage::RequestVoteReply {
                        term: 1,
                        vote_granted: false,
                    },
                },
                apply(4),
                apply(5),
            ],
            None,
        )
        .unwrap();
        assert_eq!(*ranges.lock().unwrap(), vec![(2, 3), (4, 5)]);
        assert_eq!(
            *events.lock().unwrap(),
            vec!["engine_commit", "send", "engine_commit"]
        );
        assert_eq!(rig.shared.status().last_applied, 5);
    }

    #[tokio::test]
    async fn engine_commit_failure_returns_unknown_for_whole_admitted_batch_without_visibility() {
        let rig = ReadRig::singleton();
        let dir = TestDir::new();
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut probe = applied_probe(&rig.node, events.clone());
        probe.fail_commit = true;
        let runtime = SnapshotRuntime::new(dir.path(), None, 0)
            .unwrap()
            .with_application_store(crate::applied_store::injected(Box::new(probe), &rig.node));
        let (_peer_tx, peer_rx) = mpsc::channel(1);
        let (proposal_tx, proposal_rx) = mpsc::channel(4);
        let (_read_tx, read_rx) = mpsc::channel(1);
        let (a, first) = batch_put("first", "one".into());
        let (b, second) = batch_put("second", "two".into());
        proposal_tx.send(a).await.unwrap();
        proposal_tx.send(b).await.unwrap();
        let (_shutdown, receive) = shutdown::channel();
        let outcome = tokio::time::timeout(
            Duration::from_secs(2),
            run_driver_with_snapshots(
                rig.node,
                rig.store,
                rig.transport,
                peer_rx,
                proposal_rx,
                read_rx,
                rig.shared.clone(),
                receive,
                runtime,
            ),
        )
        .await
        .expect("fatal engine error must stop the writer");
        assert!(outcome.is_err());
        assert!(rig.shared.snapshot().values.is_empty());
        assert_eq!(rig.shared.status().last_applied, 1);
        for response in [first, second] {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(1), response)
                    .await
                    .unwrap()
                    .unwrap(),
                ProposalResult::OutcomeUnknown {
                    leader_hint: Some(1)
                }
            );
        }
        assert_eq!(*events.lock().unwrap(), vec!["engine_commit"]);
    }

    #[test]
    fn engine_snapshot_install_precedes_visibility_and_final_ack_and_failure_is_fatal() {
        for fail_install in [false, true] {
            let dir = TestDir::new();
            let mut node = RaftNode::new(2, vec![1, 3], 7);
            let shared = SharedReadState::from_node(&node);
            let events = Arc::new(Mutex::new(Vec::new()));
            let mut storage = SnapshotStoreProbe {
                events: events.clone(),
                shared: shared.clone(),
                fail_publication: false,
            };
            let transport = SnapshotAckProbe {
                events: events.clone(),
                shared: shared.clone(),
            };
            let mut pending = PendingRequests::new();
            pending.snapshots = Some(SnapshotRuntime::new(dir.path(), None, 0).unwrap());
            let mut probe = applied_probe(&node, events.clone());
            probe.fail_install = fail_install;
            pending.application_store =
                Some(crate::applied_store::injected(Box::new(probe), &node));
            let result = step_and_execute(
                &mut node,
                &mut storage,
                &transport,
                &shared,
                &mut pending,
                &mut None,
                install_input(&install_fixture()),
                None,
            );
            if fail_install {
                assert!(result.is_err());
                assert_eq!(shared.status().last_applied, 0);
                assert_eq!(
                    *events.lock().unwrap(),
                    vec!["hard", "publish", "engine_install"]
                );
            } else {
                result.unwrap();
                assert_eq!(shared.get("installed").1.as_deref(), Some("value"));
                assert_eq!(
                    *events.lock().unwrap(),
                    vec!["hard", "publish", "engine_install", "installed_ack"]
                );
            }
        }
    }

    #[tokio::test]
    async fn engine_flush_failure_still_flushes_consensus_store_and_returns_failure() {
        let dir = TestDir::new();
        let node = RaftNode::new(1, vec![], 7);
        let shared = SharedReadState::from_node(&node);
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut probe = applied_probe(&node, events.clone());
        probe.fail_flush = true;
        let runtime = SnapshotRuntime::new(dir.path(), None, 0)
            .unwrap()
            .with_application_store(crate::applied_store::injected(Box::new(probe), &node));
        let store = ProbeStore {
            events: events.clone(),
            ..Default::default()
        };
        let (_peer_tx, peer_rx) = mpsc::channel(1);
        let (_proposal_tx, proposal_rx) = mpsc::channel(1);
        let (_read_tx, read_rx) = mpsc::channel(1);
        let (shutdown, receive) = shutdown::channel();
        shutdown.request();
        let error = run_driver_with_snapshots(
            node,
            store,
            RecordingTransport::default(),
            peer_rx,
            proposal_rx,
            read_rx,
            shared,
            receive,
            runtime,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("engine flush"));
        assert_eq!(*events.lock().unwrap(), vec!["engine_flush", "flush"]);
    }
    fn explicit_admin_rig() -> ReadRig {
        let (node, effects) =
            RaftNode::new_for_group(1, vec![1], 23, "admin-driver".into()).unwrap();
        let shared = SharedReadState::from_node(&node);
        let mut rig = ReadRig {
            node,
            shared,
            store: ProbeStore::default(),
            transport: RecordingTransport::default(),
            pending: PendingRequests::new(),
            hint: None,
        };
        execute_batch(
            Some(&mut rig.node),
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            effects,
            None,
        )
        .unwrap();
        rig.step(Input::Tick { now_ms: 0 });
        rig.step(Input::Tick {
            now_ms: rig.node.election_deadline(),
        });
        assert!(rig.node.is_leader());
        rig
    }
    fn learner_operation() -> AdminOperation {
        AdminOperation::AddLearner {
            id: 2,
            endpoints: raft_core::membership::MemberEndpoints {
                raft: "127.0.0.1:19002".into(),
                http: "127.0.0.1:18002".into(),
            },
        }
    }
    fn admin_request(
        rig: &mut ReadRig,
        id: &str,
        operation: AdminOperation,
    ) -> oneshot::Receiver<AdminResult> {
        let (respond_to, response) = oneshot::channel();
        let revision = crate::admin::membership_revision(rig.node.effective_membership());
        process_admin_request(
            &mut rig.node,
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            AdminRequest {
                command: crate::admin::AdminCommand {
                    request_id: id.into(),
                    operation,
                },
                validated_membership: Some(revision),
                respond_to,
            },
        )
        .unwrap();
        response
    }
    #[test]
    fn administration_completion_waits_for_actual_apply_and_retries_exact_record() {
        let mut rig = explicit_admin_rig();
        let operation = learner_operation();
        let (respond_to, mut response) = oneshot::channel();
        rig.pending.administration.insert(
            "add2".into(),
            vec![PendingAdmin {
                operation: operation.clone(),
                respond_to,
                deadline: Instant::now() + ADMIN_TIMEOUT,
            }],
        );
        let effects = rig.node.step(Input::MembershipChange {
            request_id: "add2".into(),
            operation: operation.clone(),
        });
        let (apply, held): (Vec<_>, Vec<_>) = effects
            .into_iter()
            .partition(|effect| matches!(effect, Effect::Apply(_)));
        execute_batch(
            Some(&mut rig.node),
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            held,
            None,
        )
        .unwrap();
        reconcile_admin(&rig.node, &rig.shared, &mut rig.pending, rig.hint);
        assert!(
            matches!(
                response.try_recv(),
                Err(oneshot::error::TryRecvError::Empty)
            ),
            "durable core membership without application is not an ACK"
        );
        execute_batch(
            Some(&mut rig.node),
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            apply,
            None,
        )
        .unwrap();
        reconcile_admin(&rig.node, &rig.shared, &mut rig.pending, rig.hint);
        let completed = response.try_recv().unwrap();
        assert!(
            matches!(&completed,AdminResult::Completed{record,..} if record.final_index==Some(2))
        );
        assert_eq!(
            admin_request(&mut rig, "add2", operation)
                .try_recv()
                .unwrap(),
            completed
        );
        assert!(
            matches!(admin_request(&mut rig,"add2",AdminOperation::Remove{id:2}).try_recv().unwrap(),AdminResult::Rejected{reason,..} if reason=="request_payload_changed")
        );
    }
    #[test]
    fn admitted_joint_change_rejects_overlap_and_closed_waiters_do_not_erase_durable_request() {
        let mut rig = explicit_admin_rig();
        assert!(matches!(
            admin_request(&mut rig, "add2", learner_operation())
                .try_recv()
                .unwrap(),
            AdminResult::Completed { .. }
        ));
        rig.step(Input::Tick {
            now_ms: rig.node.election_deadline() + 1,
        });
        let contact = rig
            .transport
            .0
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|(to, msg)| {
                let msg = match msg {
                    RaftMessage::GroupMessage { message, .. } => message.as_ref(),
                    other => other,
                };
                if let RaftMessage::AppendEntries { contact_round, .. } = msg {
                    (*to == 2).then_some(*contact_round)
                } else {
                    None
                }
            })
            .expect("learner heartbeat sent");
        rig.step(Input::Message {
            from: 2,
            msg: RaftMessage::GroupMessage {
                group_id: "admin-driver".into(),
                genesis_voters: vec![1],
                message: Box::new(RaftMessage::AppendEntriesReply {
                    conflict: None,
                    term: rig.node.hard.current_term,
                    success: true,
                    match_index: 2,
                    contact_round: contact,
                }),
            },
        });
        let mut pending = admin_request(
            &mut rig,
            "promote2",
            AdminOperation::SetVoters { voters: vec![1, 2] },
        );
        assert!(matches!(
            pending.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(matches!(
            rig.node.effective_membership().voters,
            raft_core::membership::VoterConfig::Joint { .. }
        ));
        assert!(
            matches!(admin_request(&mut rig,"overlap",AdminOperation::Remove{id:2}).try_recv().unwrap(),AdminResult::Rejected{reason,..} if reason=="membership_change_pending")
        );
        drop(pending);
        reconcile_admin(&rig.node, &rig.shared, &mut rig.pending, rig.hint);
        assert!(rig.pending.administration.is_empty());
        let mut retry = admin_request(
            &mut rig,
            "promote2",
            AdminOperation::SetVoters { voters: vec![1, 2] },
        );
        assert!(matches!(
            retry.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        unknown_admin(&mut rig.pending, rig.hint);
        assert!(
            matches!(retry.try_recv().unwrap(),AdminResult::Unknown{request_id,..} if request_id=="promote2")
        );
        assert!(rig.node.log.iter().any(|entry|matches!(&entry.command,Command::Configuration(change) if change.request_id=="promote2")));
    }

    #[tokio::test]
    async fn administrative_shutdown_preserves_admitted_batch_drains_or_reports_unknown() {
        for complete_during_drain in [false, true] {
            let mut rig = explicit_admin_rig();
            assert!(matches!(
                admin_request(&mut rig, "add2", learner_operation())
                    .try_recv()
                    .unwrap(),
                AdminResult::Completed { .. }
            ));
            rig.step(Input::Tick {
                now_ms: rig.node.election_deadline() + 1,
            });
            let contact = rig
                .transport
                .0
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find_map(|(to, msg)| {
                    let RaftMessage::GroupMessage { message, .. } = msg else {
                        return None;
                    };
                    if let RaftMessage::AppendEntries { contact_round, .. } = message.as_ref() {
                        (*to == 2).then_some(*contact_round)
                    } else {
                        None
                    }
                })
                .expect("learner must receive a real catch-up probe");
            let term = rig.node.hard.current_term;
            let reply = |index| RaftMessage::GroupMessage {
                group_id: "admin-driver".into(),
                genesis_voters: vec![1],
                message: Box::new(RaftMessage::AppendEntriesReply {
                    conflict: None,
                    term,
                    success: true,
                    match_index: index,
                    contact_round: contact,
                }),
            };
            rig.step(Input::Message {
                from: 2,
                msg: reply(2),
            });
            let dir = TestDir::new();
            let (controller, shutdown) = shutdown::channel();
            let (appended, append_seen) = oneshot::channel();
            let store = ProbeStore {
                request_on_append: Some(controller),
                appended: Some(appended),
                ..ProbeStore::default()
            };
            let events = store.events.clone();
            let (inbound, inbound_rx) = mpsc::channel(4);
            let (_proposals, proposal_rx) = mpsc::channel(4);
            let (_reads, read_rx) = mpsc::channel(4);
            let (admins, admin_rx) = mpsc::channel(4);
            let mut responses = Vec::new();
            for (request_id, operation) in [
                ("accepted", AdminOperation::SetVoters { voters: vec![1, 2] }),
                ("still-queued", AdminOperation::Remove { id: 2 }),
            ] {
                let (respond_to, response) = oneshot::channel();
                admins
                    .try_send(AdminRequest {
                        command: crate::admin::AdminCommand {
                            request_id: request_id.into(),
                            operation,
                        },
                        validated_membership: None,
                        respond_to,
                    })
                    .unwrap();
                responses.push(response);
            }
            let (routes, _route_rx) =
                tokio::sync::watch::channel(crate::membership_runtime::RouteUpdate {
                    term,
                    membership: rig.node.effective_membership().clone(),
                    hints: BTreeMap::new(),
                });
            let runtime = SnapshotRuntime::new(&dir.0, None, 0)
                .unwrap()
                .with_administration(admin_rx, routes);
            let shared = rig.shared.clone();
            let driver = tokio::spawn(run_driver_with_limit_and_reads(
                rig.node,
                store,
                OrderedTransport(events.clone()),
                inbound_rx,
                proposal_rx,
                read_rx,
                shared.clone(),
                shutdown,
                if complete_during_drain {
                    Duration::from_secs(2)
                } else {
                    Duration::from_millis(40)
                },
                Some(runtime),
            ));
            tokio::time::timeout(Duration::from_secs(1), append_seen)
                .await
                .unwrap()
                .unwrap();
            if complete_during_drain {
                // The joint and final entries each require learner 2's durable
                // receipt. These existing peer replies remain admitted while
                // new administrative requests are closed by shutdown.
                inbound.send((2, reply(3))).await.unwrap();
                // Final is appended on a subsequent Tick. A receipt for index
                // 4 before its append would be an impossible future ACK and
                // must be rejected. Also let the new driver's elapsed clock
                // catch up with the fixture's already-advanced logical clock.
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if events
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|event| **event == "persist")
                            .count()
                            == 2
                        {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .expect("joint commitment must lead to a durable final append");
                inbound.send((2, reply(4))).await.unwrap();
            }
            let report = tokio::time::timeout(Duration::from_secs(3), driver)
                .await
                .expect("administrative drain must be bounded")
                .unwrap()
                .unwrap();
            let queued = responses.pop().unwrap().await.unwrap();
            assert!(
                matches!(queued, AdminResult::Rejected { reason, .. } if reason == "shutting_down")
            );
            assert!(admins.is_closed());
            assert_eq!(report.queued_rejected, 1);
            let accepted = responses.pop().unwrap().await.unwrap();
            if complete_during_drain {
                assert!(
                    matches!(&accepted, AdminResult::Completed { record, .. } if record.final_index == Some(4)),
                    "{accepted:?}"
                );
                assert_eq!(shared.status().last_applied, 4);
                assert!(!report.deadline_expired);
                assert_eq!(report.pending_unknown, 0);
                assert_eq!(report.uncommitted_entries, 0);
            } else {
                assert!(
                    matches!(accepted, AdminResult::Unknown { request_id, .. } if request_id == "accepted")
                );
                assert!(report.deadline_expired);
                assert_eq!(report.pending_unknown, 1);
                assert_eq!(report.uncommitted_entries, 1);
            }
            let events = events.lock().unwrap();
            let requested = events.iter().position(|event| *event == "request").unwrap();
            assert_eq!(events[requested - 1], "persist");
            assert_eq!(events.last(), Some(&"flush"));
        }
    }

    #[test]
    fn endpoint_validation_race_is_rejected_before_second_configuration_append() {
        let mut rig = explicit_admin_rig();
        let revision = crate::admin::membership_revision(rig.node.effective_membership());
        let first = admin_request(&mut rig, "first", learner_operation())
            .try_recv()
            .unwrap();
        assert!(matches!(first, AdminResult::Completed { .. }));
        let before = rig.node.last_log_index();
        let (respond_to, mut response) = oneshot::channel();
        let operation = AdminOperation::AddLearner {
            id: 3,
            endpoints: raft_core::membership::MemberEndpoints {
                raft: "127.0.0.1:19002".into(),
                http: "127.0.0.1:18003".into(),
            },
        };
        process_admin_request(
            &mut rig.node,
            &mut rig.store,
            &rig.transport,
            &rig.shared,
            &mut rig.pending,
            &mut rig.hint,
            AdminRequest {
                command: crate::admin::AdminCommand {
                    request_id: "second".into(),
                    operation,
                },
                validated_membership: Some(revision),
                respond_to,
            },
        )
        .unwrap();
        assert!(
            matches!(response.try_recv().unwrap(),AdminResult::Rejected{reason,..} if reason=="stale_endpoint_validation")
        );
        assert_eq!(rig.node.last_log_index(), before);
        assert!(!rig.node.effective_membership().learners.contains(&3));
    }
}
