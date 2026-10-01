//! One serialized durable Raft actor per controller/data group.
use crate::machine::{Machine, Operation, Reply};
use crate::query::{Observed, Query};
use crate::types::{Error, GroupId};
use kv_node::driver::audit_persist_before_send;
use kv_node::membership_runtime::{RouteHint, RouteUpdate};
use kv_node::shutdown::{ShutdownRx, DRAIN_TIMEOUT};
use kv_node::snapshot::{SnapshotImage, SnapshotMetadata, SnapshotReceiveError, SnapshotReceiver};
use kv_node::storage::Storage;
use kv_node::transport::Transport;
use raft_core::membership::{AdminOperation, AdminRecord, MembershipState};
use raft_core::membership_protocol::MembershipOutcome;
use raft_core::{Effect, Input, NodeId, RaftMessage, RaftNode};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::Path;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{Instant, MissedTickBehavior};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_WRITES: usize = 256;
const MAX_READS: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Status {
    pub group: GroupId,
    pub node: NodeId,
    pub role: String,
    pub term: u64,
    pub commit_index: u64,
    pub applied_index: u64,
    pub snapshot_index: u64,
    pub last_log_index: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case", deny_unknown_fields)]
pub enum Response {
    Applied {
        group: GroupId,
        index: u64,
        reply: Box<Reply>,
    },
    MembershipApplied {
        group: GroupId,
        request_id: String,
        record: Box<AdminRecord>,
        applied_index: u64,
    },
    Checked {
        group: GroupId,
        node: NodeId,
        term: u64,
        index: u64,
        applied_index: u64,
        context: u64,
        observed: Box<Observed>,
    },
    Rejected {
        error: Error,
    },
    NotLeader {
        leader_hint: Option<NodeId>,
    },
    Unknown,
    Unavailable {
        reason: String,
    },
}
enum Request {
    Membership {
        request_id: String,
        operation: AdminOperation,
        validated_revision: Option<[u8; 32]>,
        reply: oneshot::Sender<Response>,
        deadline: Instant,
    },
    Write {
        operation: Operation,
        reply: oneshot::Sender<Response>,
        deadline: Instant,
    },
    Read {
        query: Query,
        reply: oneshot::Sender<Response>,
        deadline: Instant,
    },
}
#[derive(Clone)]
pub struct GroupHandle {
    group: GroupId,
    tx: mpsc::Sender<Request>,
    status: watch::Receiver<Status>,
}
impl GroupHandle {
    pub(crate) async fn membership(
        &self,
        request_id: String,
        operation: AdminOperation,
        validated_revision: Option<[u8; 32]>,
    ) -> Response {
        let (reply, received) = oneshot::channel();
        if self
            .tx
            .try_send(Request::Membership {
                request_id,
                operation,
                validated_revision,
                reply,
                deadline: Instant::now() + REQUEST_TIMEOUT,
            })
            .is_err()
        {
            return Response::Unavailable {
                reason: "administration admission closed or full".into(),
            };
        }
        tokio::time::timeout(REQUEST_TIMEOUT, received)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(Response::Unknown)
    }
    pub fn status(&self) -> Status {
        self.status.borrow().clone()
    }
    pub async fn propose(&self, operation: Operation) -> Response {
        if !operation.targets(self.group) {
            return Response::Rejected {
                error: Error::WrongGroup,
            };
        }
        if let Err(error) = operation.encode(self.group) {
            return Response::Rejected { error };
        }
        let (reply, received) = oneshot::channel();
        if self
            .tx
            .try_send(Request::Write {
                operation,
                reply,
                deadline: Instant::now() + REQUEST_TIMEOUT,
            })
            .is_err()
        {
            return Response::Unavailable {
                reason: "proposal admission closed or full".into(),
            };
        }
        tokio::time::timeout(REQUEST_TIMEOUT, received)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(Response::Unknown)
    }
    pub async fn query(&self, query: Query) -> Response {
        let (reply, received) = oneshot::channel();
        if self
            .tx
            .try_send(Request::Read {
                query,
                reply,
                deadline: Instant::now() + REQUEST_TIMEOUT,
            })
            .is_err()
        {
            return Response::Unavailable {
                reason: "read admission closed or full".into(),
            };
        }
        tokio::time::timeout(REQUEST_TIMEOUT, received)
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or(Response::Unavailable {
                reason: "read deadline".into(),
            })
    }
}
struct PendingWrite {
    reply: oneshot::Sender<Response>,
    deadline: Instant,
}
struct PendingRead {
    query: Query,
    reply: oneshot::Sender<Response>,
    deadline: Instant,
    barrier: Option<(u64, u64, u64)>,
}
struct PendingAdmin {
    operation: AdminOperation,
    reply: oneshot::Sender<Response>,
    deadline: Instant,
}

pub struct Actor<T: Transport> {
    node: RaftNode,
    storage: Storage,
    transport: T,
    machine: Machine,
    active: Option<SnapshotImage>,
    prepared: Option<SnapshotImage>,
    receiver: SnapshotReceiver,
    snapshot_threshold: u64,
    snapshot_retry_at: Instant,
    writes: BTreeMap<u64, PendingWrite>,
    administration: BTreeMap<String, Vec<PendingAdmin>>,
    routes: Option<watch::Sender<RouteUpdate>>,
    reads: BTreeMap<u64, PendingRead>,
    read_id: u64,
    rx: mpsc::Receiver<Request>,
    status: watch::Sender<Status>,
    last_known_leader: Option<NodeId>,
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

impl<T: Transport> Actor<T> {
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        group: GroupId,
        node_id: NodeId,
        genesis: Vec<NodeId>,
        group_identity: String,
        seed: u64,
        path: &Path,
        transport: T,
        snapshot_threshold: u64,
    ) -> io::Result<(Self, GroupHandle)> {
        let (mut storage, recovered) = Storage::open_recovered(path)?;
        let machine = match &recovered.snapshot {
            Some(image) => crate::snapshot::restore(image, group)?,
            None => Machine::new(group).map_err(|e| invalid(format!("invalid group: {e:?}")))?,
        };
        let fresh = recovered.hard_state == raft_core::HardState::default()
            && recovered.entries.is_empty()
            && recovered.snapshot.is_none();
        let node = if fresh {
            let (node, effects) =
                RaftNode::new_for_group(node_id, genesis.clone(), seed, group_identity.clone())
                    .map_err(invalid)?;
            for effect in effects {
                match effect {
                    Effect::PersistHardState(hard) => storage.save_hard_state(&hard)?,
                    _ => return Err(invalid("unexpected explicit bootstrap effect")),
                }
            }
            node
        } else {
            RaftNode::restore_with_snapshot(
                node_id,
                genesis
                    .iter()
                    .copied()
                    .filter(|id| *id != node_id)
                    .collect(),
                seed,
                recovered.hard_state,
                recovered.snapshot.as_ref().map(|s| s.descriptor().clone()),
                recovered.entries,
            )
            .map_err(|e| invalid(e.to_string()))?
        };
        let authority = &node.committed_membership().state;
        if node.hard.membership.is_none()
            || authority.group_id != group_identity
            || authority.genesis_voters != genesis
        {
            return Err(invalid("actor durable group identity/genesis mismatch; implicit legacy migration is forbidden"));
        }
        let (tx, rx) = mpsc::channel(MAX_WRITES + MAX_READS);
        let status_value = Status {
            group,
            node: node_id,
            role: "follower".into(),
            term: node.hard.current_term,
            commit_index: node.commit_index,
            applied_index: machine.applied(),
            snapshot_index: node.snapshot_index(),
            last_log_index: node.last_log_index(),
        };
        let (status, status_rx) = watch::channel(status_value);
        let mut actor = Self {
            node,
            storage,
            transport,
            machine,
            active: recovered.snapshot,
            prepared: None,
            receiver: SnapshotReceiver::new(path)?,
            snapshot_threshold,
            snapshot_retry_at: Instant::now(),
            writes: BTreeMap::new(),
            administration: BTreeMap::new(),
            routes: None,
            reads: BTreeMap::new(),
            read_id: 0,
            rx,
            status,
            last_known_leader: None,
        };
        // Durable membership recovery and configuration application precede
        // listeners, ordinary inputs and any externally visible state.
        actor.step(Input::Recover, None)?;
        actor.update_status();
        Ok((
            actor,
            GroupHandle {
                group,
                tx,
                status: status_rx,
            },
        ))
    }
    pub fn effective_membership(&self) -> &MembershipState {
        self.node.effective_membership()
    }
    pub fn with_routes(mut self, routes: watch::Sender<RouteUpdate>) -> Self {
        self.routes = Some(routes);
        self.update_routes();
        self
    }
    pub async fn run(
        mut self,
        mut inbound: mpsc::Receiver<(NodeId, RaftMessage)>,
        mut shutdown: ShutdownRx,
    ) -> io::Result<()> {
        let mut result = self.run_inner(&mut inbound, &mut shutdown).await;
        self.rx.close();
        self.fail_pending();
        // Cleanup and the final durability barrier are attempted even after a
        // fatal input/storage error; the first failure remains authoritative.
        for cleanup in [self.receiver.cancel_active(), self.storage.flush()] {
            if let Err(error) = cleanup {
                if result.is_ok() {
                    result = Err(error);
                } else {
                    tracing::error!(%error, "additional actor cleanup failure");
                }
            }
        }
        result
    }
    async fn run_inner(
        &mut self,
        inbound: &mut mpsc::Receiver<(NodeId, RaftMessage)>,
        shutdown: &mut ShutdownRx,
    ) -> io::Result<()> {
        let started = Instant::now();
        let mut tick = tokio::time::interval(Duration::from_millis(raft_core::TICK_MS));
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut draining = None;
        loop {
            if draining.is_none() {
                if let Some(requested_at) = shutdown.requested_at() {
                    draining = Some(requested_at + DRAIN_TIMEOUT);
                    self.begin_drain()?;
                }
            }
            if draining.is_some_and(|deadline| {
                (self.writes.is_empty() && self.administration.is_empty())
                    || Instant::now() >= deadline
            }) {
                return Ok(());
            }
            tokio::select! {
                _=shutdown.requested(),if draining.is_none()=>{continue;}
                _=tick.tick()=>{
                    self.step(Input::Tick{now_ms:started.elapsed().as_millis() as u64},None)?;
                    self.expire()?;
                }
                received=inbound.recv()=>{
                    let Some((from,msg))=received else{return Err(io::Error::other("peer listener closed"));};
                    self.step(Input::Message{from,msg},None)?;
                }
                request=self.rx.recv(),if draining.is_none()=>{
                    let Some(request)=request else{draining=Some(Instant::now()+DRAIN_TIMEOUT);self.begin_drain()?;continue;};
                    if shutdown.is_requested() {
                        match request { Request::Write{reply,..}|Request::Read{reply,..}|Request::Membership{reply,..}=>{let _=reply.send(Response::Unavailable{reason:"shutting down before admission".into()});} }
                        continue;
                    }
                    match request{
                        Request::Membership{request_id,operation,validated_revision,reply,deadline}=>{
                            if reply.is_closed() { continue; }
                            let retry=self.node.effective_membership().records.get(&request_id).is_some_and(|r|r.operation==operation);
                            if deadline<=Instant::now() || self.administration.values().map(Vec::len).sum::<usize>()>=64 {
                                let _=reply.send(Response::Unavailable{reason:"administration deadline/capacity".into()}); continue;
                            }
                            if matches!(operation,AdminOperation::AddLearner{..}) && !retry && validated_revision!=Some(kv_node::admin::membership_revision(self.node.effective_membership())) {
                                let _=reply.send(Response::Unavailable{reason:"stale endpoint validation".into()}); continue;
                            }
                            if self.administration.get(&request_id).is_some_and(|w|w.iter().any(|w|w.operation!=operation)) {
                                let _=reply.send(Response::Rejected{error:Error::PayloadMismatch}); continue;
                            }
                            self.administration.entry(request_id.clone()).or_default().push(PendingAdmin{operation:operation.clone(),reply,deadline});
                            self.step(Input::MembershipChange{request_id,operation},None)?;
                        }
                        Request::Write{operation,reply,deadline}=>{
                            if reply.is_closed(){continue;}
                            if deadline<=Instant::now() || self.writes.len()>=MAX_WRITES{
                                let _=reply.send(Response::Unavailable{reason:"proposal admission deadline/capacity".into()});continue;
                            }
                            match operation.encode(self.machine.group()){
                                Ok(command)=>self.step(Input::ClientPropose{command},Some(PendingWrite{reply,deadline}))?,
                                Err(error)=>{let _=reply.send(Response::Rejected{error});}
                            }
                        }
                        Request::Read{query,reply,deadline}=>{
                            if reply.is_closed(){continue;}
                            if deadline<=Instant::now() || self.reads.len()>=MAX_READS || self.read_id==u64::MAX{
                                let _=reply.send(Response::Unavailable{reason:"read admission deadline/capacity".into()});continue;
                            }
                            self.read_id+=1;let id=self.read_id;
                            self.reads.insert(id,PendingRead{query,reply,deadline,barrier:None});
                            self.step(Input::ReadIndex{request_id:id},None)?;
                        }
                    }
                }
            }
            self.finish_reads();
            self.finish_administration();
            if draining.is_none() && !shutdown.is_requested() {
                self.maybe_snapshot()?;
            }
            self.update_status();
        }
    }
    fn begin_drain(&mut self) -> io::Result<()> {
        self.rx.close();
        while let Ok(request) = self.rx.try_recv() {
            match request {
                Request::Write { reply, .. }
                | Request::Read { reply, .. }
                | Request::Membership { reply, .. } => {
                    let _ = reply.send(Response::Unavailable {
                        reason: "shutting down before admission".into(),
                    });
                }
            }
        }
        let ids: Vec<_> = self.reads.keys().copied().collect();
        for id in ids {
            self.cancel_read(id, "shutting down")?;
        }
        Ok(())
    }
    fn step(&mut self, input: Input, write: Option<PendingWrite>) -> io::Result<()> {
        let effects = self.node.step(input);
        self.execute(effects, write)?;
        self.update_routes();
        Ok(())
    }
    fn execute(&mut self, effects: Vec<Effect>, mut write: Option<PendingWrite>) -> io::Result<()> {
        audit_persist_before_send(&effects).map_err(invalid)?;
        let mut queue: VecDeque<_> = effects.into();
        while let Some(effect) = queue.pop_front() {
            let mut callback = None;
            match effect {
                Effect::MembershipResult {
                    request_id,
                    outcome,
                } => {
                    if let MembershipOutcome::Rejected {
                        reason,
                        leader_hint: _,
                    } = outcome
                    {
                        if let Some(waiters) = self.administration.remove(&request_id) {
                            for waiter in waiters {
                                let _ = waiter.reply.send(Response::Unavailable {
                                    reason: reason.clone(),
                                });
                            }
                        }
                    }
                }
                Effect::MembershipChanged { .. } => {}
                Effect::MembershipRouteHint {
                    id,
                    term,
                    endpoints,
                } => {
                    if let Some(routes) = &self.routes {
                        routes.send_if_modified(|update| {
                            let hint = RouteHint { term, endpoints };
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
                Effect::PersistHardState(hard) => self.storage.save_hard_state(&hard)?,
                Effect::PersistLogEntries {
                    truncate_from,
                    entries,
                } => self.storage.append_entries(truncate_from, &entries)?,
                Effect::Send { to, msg } => self.transport.send(to, msg),
                Effect::Apply(entry) => {
                    let reply = self.machine.apply(&entry).map_err(invalid)?;
                    if let Some(pending) = self.writes.remove(&entry.index) {
                        let _ = pending.reply.send(Response::Applied {
                            group: self.machine.group(),
                            index: entry.index,
                            reply: Box::new(reply),
                        });
                    }
                }
                Effect::RoleChanged { role_name, .. } => {
                    if role_name != "Leader" {
                        self.fail_pending();
                    }
                }
                Effect::ProposeAccepted { index } => {
                    let request = write
                        .take()
                        .ok_or_else(|| invalid("proposal accepted without request"))?;
                    if self.writes.insert(index, request).is_some() {
                        return Err(invalid("duplicate proposal waiter"));
                    }
                }
                Effect::ProposeRejected { leader_hint } => {
                    self.last_known_leader = leader_hint;
                    let request = write
                        .take()
                        .ok_or_else(|| invalid("proposal rejected without request"))?;
                    let _ = request.reply.send(Response::NotLeader { leader_hint });
                }
                Effect::ReadReady {
                    request_id,
                    context,
                    index,
                    term,
                } => {
                    if let Some(read) = self.reads.get_mut(&request_id) {
                        read.barrier = Some((index, term, context));
                    }
                }
                Effect::ReadRejected {
                    request_id,
                    leader_hint,
                    reason,
                } => {
                    self.last_known_leader = leader_hint;
                    if let Some(read) = self.reads.remove(&request_id) {
                        let _ = read.reply.send(Response::Unavailable {
                            reason: reason.as_str().into(),
                        });
                    }
                }
                Effect::ReadSnapshotChunk {
                    to,
                    transfer,
                    descriptor,
                    offset,
                    max_len,
                } => {
                    let image = self
                        .active
                        .as_ref()
                        .filter(|s| s.descriptor() == &descriptor)
                        .ok_or_else(|| invalid("snapshot not available for send"))?;
                    if max_len == 0
                        || max_len > raft_core::MAX_SNAPSHOT_CHUNK_BYTES
                        || offset >= descriptor.total_len
                    {
                        return Err(invalid("invalid outbound snapshot segment"));
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
                    let result =
                        if !raft_core::snapshot::valid_chunk(&descriptor, offset, data.len(), done)
                        {
                            raft_core::SnapshotStageResult::Rejected
                        } else {
                            match self.receiver.stage_authorized_chunk(
                                transfer,
                                &descriptor,
                                offset,
                                &data,
                                self.machine.applied(),
                                &descriptor.metadata.members,
                                self.active.as_ref(),
                            ) {
                                Ok(chunk) => {
                                    let actor_valid =
                                        self.receiver.completed(transfer).is_none_or(|image| {
                                            crate::snapshot::restore(image, self.machine.group())
                                                .is_ok()
                                        });
                                    if !actor_valid {
                                        self.receiver.cancel(transfer)?;
                                        raft_core::SnapshotStageResult::Rejected
                                    } else {
                                        raft_core::SnapshotStageResult::Accepted {
                                            next_offset: chunk.next_offset,
                                            complete: chunk.complete,
                                        }
                                    }
                                }
                                Err(SnapshotReceiveError::Rejected(_)) => {
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
                    let image = match transfer {
                        Some(id) => self.receiver.completed(id).cloned(),
                        None => self.prepared.take(),
                    }
                    .filter(|image| image.descriptor() == &descriptor)
                    .ok_or_else(|| invalid("publication image mismatch"))?;
                    crate::snapshot::restore(&image, self.machine.group())?;
                    self.storage.publish_snapshot(&image, &retained_entries)?;
                    self.active = Some(image);
                    if let Some(id) = transfer {
                        self.receiver.cancel(id)?;
                    }
                    callback = Some(Input::SnapshotPublished {
                        transfer,
                        descriptor,
                    });
                }
                Effect::ApplySnapshot { descriptor } => {
                    let image = self
                        .active
                        .as_ref()
                        .filter(|image| image.descriptor() == &descriptor)
                        .ok_or_else(|| invalid("applied unpublished snapshot"))?;
                    self.machine = crate::snapshot::restore(image, self.machine.group())?;
                    let covered: Vec<_> = self
                        .writes
                        .keys()
                        .copied()
                        .take_while(|i| *i <= self.machine.applied())
                        .collect();
                    for index in covered {
                        if let Some(pending) = self.writes.remove(&index) {
                            let _ = pending.reply.send(Response::Unknown);
                        }
                    }
                }
                Effect::CancelSnapshot { transfer } => {
                    self.receiver.cancel(transfer)?;
                }
                Effect::SnapshotRejected { .. } => self.prepared = None,
            }
            if let Some(input) = callback {
                let next = self.node.step(input);
                audit_persist_before_send(&next).map_err(invalid)?;
                for effect in next.into_iter().rev() {
                    queue.push_front(effect);
                }
            }
        }
        if write.is_some() {
            return Err(invalid("proposal produced no acceptance/rejection"));
        }
        Ok(())
    }
    fn finish_reads(&mut self) {
        let ready: Vec<_> = self
            .reads
            .iter()
            .filter_map(|(id, read)| {
                read.barrier
                    .filter(|(index, _, _)| self.machine.applied() >= *index)
                    .map(|_| *id)
            })
            .collect();
        for id in ready {
            let read = self.reads.remove(&id).expect("selected read");
            let (index, term, context) = read.barrier.expect("selected barrier");
            let result = if Instant::now() >= read.deadline
                || !self.node.is_leader()
                || self.node.hard.current_term != term
            {
                Response::Unavailable {
                    reason: "read authority/deadline changed".into(),
                }
            } else {
                let observed = match &read.query {
                    Query::Membership { request_id } => Ok(Observed::Membership {
                        group: self.machine.group(),
                        committed: Box::new(self.node.committed_membership().clone()),
                        effective: Box::new(self.node.effective_membership().clone()),
                        record: request_id
                            .as_ref()
                            .and_then(|id| self.node.committed_membership().state.records.get(id))
                            .cloned(),
                    }),
                    query => self.machine.query(query),
                };
                match observed {
                    Ok(observed) => Response::Checked {
                        group: self.machine.group(),
                        node: self.node.id,
                        term,
                        index,
                        applied_index: self.machine.applied(),
                        context,
                        observed: Box::new(observed),
                    },
                    Err(error) => Response::Rejected { error },
                }
            };
            let _ = read.reply.send(result);
        }
    }
    fn cancel_read(&mut self, id: u64, reason: &str) -> io::Result<()> {
        if let Some(read) = self.reads.remove(&id) {
            let _ = read.reply.send(Response::Unavailable {
                reason: reason.into(),
            });
        }
        self.step(Input::CancelRead { request_id: id }, None)
    }
    fn expire(&mut self) -> io::Result<()> {
        let now = Instant::now();
        let reads: Vec<_> = self
            .reads
            .iter()
            .filter(|(_, r)| r.deadline <= now || r.reply.is_closed())
            .map(|(id, _)| *id)
            .collect();
        for id in reads {
            self.cancel_read(id, "read deadline/cancelled")?;
        }
        let writes: Vec<_> = self
            .writes
            .iter()
            .filter(|(_, w)| w.deadline <= now || w.reply.is_closed())
            .map(|(id, _)| *id)
            .collect();
        for id in writes {
            if let Some(write) = self.writes.remove(&id) {
                let _ = write.reply.send(Response::Unknown);
            }
        }
        Ok(())
    }
    fn maybe_snapshot(&mut self) -> io::Result<()> {
        let index = self.machine.applied();
        if Instant::now() < self.snapshot_retry_at
            || self.snapshot_threshold == 0
            || index.saturating_sub(self.node.snapshot_index()) < self.snapshot_threshold
            || self.prepared.is_some()
        {
            return Ok(());
        }
        self.snapshot_retry_at = Instant::now() + Duration::from_secs(1);
        let term = self
            .node
            .log_term(index)
            .ok_or_else(|| invalid("applied snapshot term unavailable"))?;
        let image = crate::snapshot::capture(
            SnapshotMetadata {
                last_included_index: index,
                last_included_term: term,
                members: self
                    .node
                    .committed_membership()
                    .state
                    .participants()
                    .into_iter()
                    .collect(),
                membership: Some(self.node.committed_membership().clone()),
            },
            &self.machine,
        )?;
        let descriptor = image.descriptor().clone();
        self.prepared = Some(image);
        self.step(Input::Compact { descriptor }, None)
    }
    fn fail_pending(&mut self) {
        for (_, waiters) in std::mem::take(&mut self.administration) {
            for waiter in waiters {
                let _ = waiter.reply.send(Response::Unknown);
            }
        }
        for (_, write) in std::mem::take(&mut self.writes) {
            let _ = write.reply.send(Response::Unknown);
        }
        for (_, read) in std::mem::take(&mut self.reads) {
            let _ = read.reply.send(Response::Unavailable {
                reason: "leadership lost or actor stopped".into(),
            });
        }
    }
    fn finish_administration(&mut self) {
        let now = Instant::now();
        let mut keep = BTreeMap::new();
        for (request_id, waiters) in std::mem::take(&mut self.administration) {
            for waiter in waiters {
                if waiter.reply.is_closed() {
                    continue;
                }
                if let Some(record) = self
                    .node
                    .committed_membership()
                    .state
                    .records
                    .get(&request_id)
                    .filter(|record| {
                        record.operation == waiter.operation
                            && record
                                .final_index
                                .is_some_and(|index| index <= self.machine.applied())
                    })
                {
                    let _ = waiter.reply.send(Response::MembershipApplied {
                        group: self.machine.group(),
                        request_id: request_id.clone(),
                        record: Box::new(record.clone()),
                        applied_index: self.machine.applied(),
                    });
                } else if !self.node.is_leader() || now >= waiter.deadline {
                    let _ = waiter.reply.send(Response::Unknown);
                } else {
                    keep.entry(request_id.clone())
                        .or_insert_with(Vec::new)
                        .push(waiter);
                }
            }
        }
        self.administration = keep;
    }
    fn update_routes(&self) {
        if let Some(routes) = &self.routes {
            routes.send_if_modified(|update| {
                if update.term == self.node.hard.current_term
                    && update.membership == *self.node.effective_membership()
                {
                    return false;
                }
                update.update_authority(
                    self.node.hard.current_term,
                    self.node.effective_membership().clone(),
                );
                true
            });
        }
    }
    fn update_status(&self) {
        let role = match self.node.role {
            raft_core::Role::Follower => "follower",
            raft_core::Role::PreCandidate { .. } => "pre_candidate",
            raft_core::Role::Candidate { .. } => "candidate",
            raft_core::Role::Leader { .. } => "leader",
        };
        self.status.send_replace(Status {
            group: self.machine.group(),
            node: self.node.id,
            role: role.into(),
            term: self.node.hard.current_term,
            commit_index: self.node.commit_index,
            applied_index: self.machine.applied(),
            snapshot_index: self.node.snapshot_index(),
            last_log_index: self.node.last_log_index(),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    struct Dir(std::path::PathBuf);
    impl Dir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "micro-raft-group-actor-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            assert!(!path.exists());
            Self(path)
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            if std::thread::panicking() {
                eprintln!("retained actor fixture {}", self.0.display());
            } else {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }
    struct Sink;
    impl Transport for Sink {
        fn send(&self, _: NodeId, _: RaftMessage) {}
    }

    #[test]
    fn passive_actor_persists_exact_genesis_without_acquiring_a_vote() {
        let dir = Dir::new();
        let (mut actor, _) = Actor::open(
            1,
            4,
            vec![1, 2, 3],
            "actor-passive".into(),
            7,
            &dir.0,
            Sink,
            0,
        )
        .unwrap();
        for n in 0..100 {
            actor.step(Input::Tick { now_ms: n * 100 }, None).unwrap();
        }
        assert!(matches!(actor.node.role, raft_core::Role::Follower));
        assert_eq!(
            actor.node.committed_membership().state.genesis_voters,
            vec![1, 2, 3]
        );
        assert!(!actor
            .node
            .effective_membership()
            .participants()
            .contains(&4));
        drop(actor);
        let (reopened, _) = Actor::open(
            1,
            4,
            vec![1, 2, 3],
            "actor-passive".into(),
            8,
            &dir.0,
            Sink,
            0,
        )
        .unwrap();
        assert!(!reopened.node.recovery_required());
        assert!(!reopened
            .node
            .effective_membership()
            .participants()
            .contains(&4));
    }

    #[test]
    fn membership_carrier_reopens_exact_authority_and_application_watermark() {
        let dir = Dir::new();
        let (mut actor, _) =
            Actor::open(0, 1, vec![1], "actor-recovery".into(), 9, &dir.0, Sink, 1).unwrap();
        for n in 0..100 {
            actor.step(Input::Tick { now_ms: n * 100 }, None).unwrap();
            if actor.node.is_leader() {
                break;
            }
        }
        assert!(actor.node.is_leader());
        actor
            .step(
                Input::MembershipChange {
                    request_id: "recovery-add2".into(),
                    operation: AdminOperation::AddLearner {
                        id: 2,
                        endpoints: raft_core::membership::MemberEndpoints {
                            raft: "127.0.0.1:6122".into(),
                            http: "127.0.0.1:7122".into(),
                        },
                    },
                },
                None,
            )
            .unwrap();
        let committed = actor.node.committed_membership().clone();
        assert!(committed.state.learners.contains(&2));
        assert_eq!(
            committed.state.records["recovery-add2"].final_index,
            Some(actor.machine.applied())
        );
        actor.maybe_snapshot().unwrap();
        let snapshot = actor
            .active
            .as_ref()
            .expect("reached real snapshot publication");
        assert_eq!(
            snapshot.descriptor().metadata.membership.as_ref(),
            Some(&committed)
        );
        let watermark = actor.machine.applied();
        drop(actor);
        let (reopened, _) =
            Actor::open(0, 1, vec![1], "actor-recovery".into(), 10, &dir.0, Sink, 1).unwrap();
        assert_eq!(reopened.node.committed_membership(), &committed);
        assert_eq!(reopened.machine.applied(), watermark);
        assert!(!reopened.node.recovery_required());
        drop(reopened);
        assert!(Actor::open(0, 1, vec![1], "wrong-group".into(), 11, &dir.0, Sink, 1).is_err());
        assert!(Actor::open(
            0,
            1,
            vec![1, 2],
            "actor-recovery".into(),
            11,
            &dir.0,
            Sink,
            1
        )
        .is_err());
    }
}
