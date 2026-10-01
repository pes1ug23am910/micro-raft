//! Checked network intents: callers provide decisions, never remote commit proofs.
use crate::controller::{ControllerCommand as C, Phase, Transfer};
use crate::data::DataCommand as D;
use crate::machine::{Operation, Reply};
use crate::query::{Observed, Query, ShardView};
use crate::runtime::{GroupHandle, Response};
use crate::topology::Topology;
use crate::types::*;
use axum::{
    extract::{DefaultBodyLimit, State},
    routing::{get, post},
    Json, Router,
};
use raft_core::membership::{AdminOperation, AdminRecord, MembershipState};
use raft_core::NodeId;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, net::SocketAddr, sync::Arc, time::Duration};
use tokio::{sync::Semaphore, task::JoinSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Wire<T> {
    pub cluster: String,
    pub body: T,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadRequest {
    pub group: GroupId,
    pub query: Query,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteRequest {
    pub group: GroupId,
    pub action: Action,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Step {
    Fence,
    RecordFence,
    BeginInstall,
    PullChunk,
    FinishInstall,
    RecordInstalled,
    Own,
    Activate,
    RecordActivated,
    Cleanup,
    RecordCleanup,
    Complete,
    Abort,
    AbortSource,
    AbortDestination,
    RecordSourceAbort,
    RecordDestinationAbort,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Action {
    BeginMaintenance {
        request_id: String,
        target: GroupId,
        operation: AdminOperation,
    },
    ApplyMaintenance {
        request_id: String,
    },
    ReconcileMaintenance {
        request_id: String,
    },
    Bootstrap,
    Register {
        shard: ShardId,
        epoch: u64,
        nonce: String,
    },
    Close {
        shard: ShardId,
        epoch: u64,
        session: SessionId,
    },
    Mutate {
        shard: ShardId,
        epoch: u64,
        session: SessionId,
        key: String,
        sequence: u64,
        value: Option<String>,
    },
    BeginMove {
        request_id: String,
        shard: ShardId,
        epoch: u64,
        destination: GroupId,
    },
    Advance {
        request_id: String,
        step: Step,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReconcileRequest {
    pub request_id: String,
}

pub struct Service {
    pub topology: Arc<Topology>,
    pub node: NodeId,
    pub groups: BTreeMap<GroupId, GroupHandle>,
    pub routes: Arc<crate::routing::RouteRegistry>,
    fingerprint: String,
    admission: Semaphore,
}
fn rejected(error: Error) -> Response {
    Response::Rejected { error }
}
fn unavailable(reason: impl Into<String>) -> Response {
    Response::Unavailable {
        reason: reason.into(),
    }
}
fn capacity(response: &Response) -> bool {
    matches!(
        response,
        Response::Rejected {
            error: Error::Capacity
        }
    ) || matches!(response,Response::Applied{reply,..} if matches!(reply.as_ref(),Reply::Rejected(Error::Capacity)))
}

impl Service {
    pub fn new(
        topology: Arc<Topology>,
        node: NodeId,
        groups: BTreeMap<GroupId, GroupHandle>,
        routes: Arc<crate::routing::RouteRegistry>,
    ) -> Arc<Self> {
        Arc::new(Self {
            fingerprint: topology.fingerprint(),
            topology,
            node,
            groups,
            routes,
            admission: Semaphore::new(64),
        })
    }
    pub fn router(self: &Arc<Self>) -> Router {
        Router::new()
            .route("/status", get(status))
            .route("/v1/query", post(query_local))
            .route("/v1/execute", post(execute_local))
            .route("/v1/checked", post(query_routed))
            .route("/v1/dispatch", post(execute_routed))
            .route("/v1/reconcile", post(reconcile))
            .layer(DefaultBodyLimit::max(crate::network::MAX_BODY))
            .with_state(Arc::clone(self))
    }
    pub async fn query_group(self: &Arc<Self>, group: GroupId, query: Query) -> Response {
        self.query_authority(group, query).await.1
    }
    async fn query_authority(
        self: &Arc<Self>,
        group: GroupId,
        query: Query,
    ) -> (Option<(NodeId, SocketAddr)>, Response) {
        let mut pending = JoinSet::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
        for (id, http) in self.routes.http_routes(group) {
            let service = Arc::clone(self);
            let query = query.clone();
            pending.spawn(async move {
                if id == service.node {
                    return (
                        id,
                        http,
                        match service.groups.get(&group) {
                            Some(handle) => handle.query(query).await,
                            None => rejected(Error::WrongGroup),
                        },
                    );
                }
                let request = Wire {
                    cluster: service.fingerprint.clone(),
                    body: ReadRequest { group, query },
                };
                let response = match crate::network::post::<_, Wire<Response>>(
                    http,
                    "/v1/query",
                    &request,
                    deadline.saturating_duration_since(tokio::time::Instant::now()),
                )
                .await
                {
                    Ok(reply) if reply.cluster == service.fingerprint => reply.body,
                    Ok(_) => unavailable("query cluster identity mismatch"),
                    Err(error) => unavailable(error.to_string()),
                };
                (id, http, response)
            });
        }
        while let Ok(Some(result)) = tokio::time::timeout_at(deadline, pending.join_next()).await {
            match result {
                Ok((
                    node,
                    http,
                    response @ Response::Checked {
                        group: actual,
                        node: actual_node,
                        term,
                        index,
                        applied_index,
                        context,
                        ..
                    },
                )) if actual == group
                    && actual_node == node
                    && term > 0
                    && context > 0
                    && index > 0
                    && applied_index >= index =>
                {
                    return (Some((node, http)), response)
                }
                Ok((_, _, response @ Response::Rejected { .. })) => return (None, response),
                _ => {}
            }
        }
        (
            None,
            unavailable("no checked group observation within bounded peer requests"),
        )
    }
    pub async fn checked(
        self: &Arc<Self>,
        group: GroupId,
        query: Query,
    ) -> Result<Observed, Response> {
        match self.query_group(group, query).await {
            Response::Checked { observed, .. } => Ok(*observed),
            other => Err(other),
        }
    }
    pub async fn execute_group(self: &Arc<Self>, request: ExecuteRequest) -> Response {
        // Discover via a completed group read fence. A dead low-numbered route
        // cannot monopolize retries, and diagnostic status is never authority.
        let (authority, observed) = self
            .query_authority(request.group, Query::Configuration)
            .await;
        let Some((id, http)) = authority else {
            return observed;
        };
        if id == self.node {
            return self.apply_intent(request).await;
        }
        let wire = Wire {
            cluster: self.fingerprint.clone(),
            body: request,
        };
        match crate::network::post::<_, Wire<Response>>(
            http,
            "/v1/execute",
            &wire,
            Duration::from_secs(12),
        )
        .await
        {
            Ok(reply) if reply.cluster == self.fingerprint => reply.body,
            Ok(_) => Response::Unknown,
            Err(error) => {
                tracing::warn!(node=id,%error,"control request completion unknown");
                Response::Unknown
            }
        }
    }
    async fn transfer(self: &Arc<Self>, request_id: &str) -> Result<Transfer, Response> {
        match self
            .checked(
                CONTROLLER,
                Query::Transfer {
                    request_id: request_id.into(),
                },
            )
            .await?
        {
            Observed::Transfer {
                transfer: Some(transfer),
            } => Ok(transfer),
            _ => Err(rejected(Error::WrongTransfer)),
        }
    }
    async fn maintenance(
        self: &Arc<Self>,
        request_id: &str,
    ) -> Result<MaintenanceTicket, Response> {
        match self
            .checked(
                CONTROLLER,
                Query::Maintenance {
                    request_id: request_id.into(),
                },
            )
            .await?
        {
            Observed::Maintenance {
                ticket: Some(ticket),
            } => Ok(ticket),
            _ => Err(rejected(Error::WrongTransfer)),
        }
    }
    async fn membership_view(
        self: &Arc<Self>,
        group: GroupId,
        request_id: Option<String>,
    ) -> Result<(Origin, MembershipState, Option<AdminRecord>), Response> {
        match self
            .query_group(group, Query::Membership { request_id })
            .await
        {
            Response::Checked {
                group: actual,
                applied_index,
                observed,
                ..
            } if actual == group => match *observed {
                Observed::Membership {
                    group: observed_group,
                    committed,
                    effective,
                    record,
                } if observed_group == group && committed.index <= applied_index => Ok((
                    Origin {
                        group,
                        index: applied_index,
                    },
                    *effective,
                    record,
                )),
                _ => Err(unavailable("invalid checked membership authority")),
            },
            other => Err(other),
        }
    }
    async fn validate_membership_endpoints(
        self: &Arc<Self>,
        group: GroupId,
        operation: &AdminOperation,
        membership: &MembershipState,
    ) -> Result<[u8; 32], Response> {
        self.routes
            .validate_operation(group, operation, membership)
            .await
            .map_err(unavailable)
    }
    async fn apply_maintenance(self: &Arc<Self>, group: GroupId, request_id: &str) -> Response {
        let ticket = match self.maintenance(request_id).await {
            Ok(ticket) => ticket,
            Err(response) => return response,
        };
        if ticket.target != group {
            return rejected(Error::WrongGroup);
        }
        let core_id = ticket.core_request_id();
        let (_, membership, _) = match self.membership_view(group, Some(core_id.clone())).await {
            Ok(view) => view,
            Err(response) => return response,
        };
        let retry = membership
            .records
            .get(&core_id)
            .is_some_and(|record| record.operation == ticket.operation);
        let revision = if retry {
            None
        } else {
            match self
                .validate_membership_endpoints(group, &ticket.operation, &membership)
                .await
            {
                Ok(revision) => Some(revision),
                Err(response) => return response,
            }
        };
        let Some(handle) = self.groups.get(&group) else {
            return rejected(Error::WrongGroup);
        };
        handle.membership(core_id, ticket.operation, revision).await
    }
    async fn reconcile_maintenance(self: &Arc<Self>, request_id: &str) -> Response {
        let ticket = match self.maintenance(request_id).await {
            Ok(ticket) => ticket,
            Err(response) => return response,
        };
        if ticket.completion.is_some() {
            return self
                .query_group(
                    CONTROLLER,
                    Query::Maintenance {
                        request_id: request_id.into(),
                    },
                )
                .await;
        }
        let core_request_id = ticket.core_request_id();
        let (observed_applied, _, record) = match self
            .membership_view(ticket.target, Some(core_request_id.clone()))
            .await
        {
            Ok(view) => view,
            Err(response) => return response,
        };
        if let Some(record) = record.filter(|record| {
            record.operation == ticket.operation
                && record
                    .final_index
                    .is_some_and(|index| index <= observed_applied.index)
        }) {
            let Some(handle) = self.groups.get(&CONTROLLER) else {
                return rejected(Error::WrongGroup);
            };
            return handle
                .propose(Operation::Controller(C::CompleteMaintenance {
                    request_id: request_id.into(),
                    ticket_origin: ticket.origin,
                    core_request_id,
                    record,
                    observed_applied,
                }))
                .await;
        }
        Box::pin(self.execute_group(ExecuteRequest {
            group: ticket.target,
            action: Action::ApplyMaintenance {
                request_id: request_id.into(),
            },
        }))
        .await
    }
    async fn shard(
        self: &Arc<Self>,
        group: GroupId,
        shard: ShardId,
    ) -> Result<ShardView, Response> {
        match self.checked(group, Query::Shard { shard }).await? {
            Observed::Shard {
                group: actual,
                shard: actual_shard,
                ownership,
            } if actual == group && actual_shard == shard => Ok(ownership),
            _ => Err(unavailable("checked shard identity mismatch")),
        }
    }
    async fn apply_intent(self: &Arc<Self>, request: ExecuteRequest) -> Response {
        let Some(handle) = self.groups.get(&request.group) else {
            return rejected(Error::WrongGroup);
        };
        if handle.status().role != "leader" {
            return Response::NotLeader { leader_hint: None };
        }
        match &request.action {
            Action::ApplyMaintenance { request_id } => {
                return self.apply_maintenance(request.group, request_id).await
            }
            Action::ReconcileMaintenance { request_id } if request.group == CONTROLLER => {
                return self.reconcile_maintenance(request_id).await
            }
            _ => {}
        }
        let operation = match self.prepare(request.group, request.action).await {
            Ok(op) => op,
            Err(response) => return response,
        };
        handle.propose(operation).await
    }
    async fn prepare(
        self: &Arc<Self>,
        group: GroupId,
        action: Action,
    ) -> Result<Operation, Response> {
        let data = |command| {
            if group == CONTROLLER {
                Err(rejected(Error::WrongGroup))
            } else {
                Ok(Operation::Data(command))
            }
        };
        let control = |command| {
            if group != CONTROLLER {
                Err(rejected(Error::WrongGroup))
            } else {
                Ok(Operation::Controller(command))
            }
        };
        match action {
            Action::BeginMaintenance {
                request_id,
                target,
                operation,
            } => {
                if group != CONTROLLER || !self.topology.all_groups().any(|id| id == target) {
                    return Err(rejected(Error::WrongGroup));
                }
                if let Observed::Maintenance {
                    ticket: Some(ticket),
                } = self
                    .checked(
                        CONTROLLER,
                        Query::Maintenance {
                            request_id: request_id.clone(),
                        },
                    )
                    .await?
                {
                    // Exact retained retries need no fresh route or liveness prerequisite.
                    return control(C::AcquireMaintenance {
                        request_id,
                        target,
                        operation,
                        expected_generation: ticket.previous_generation,
                    });
                }
                let generation = match self.checked(CONTROLLER, Query::MaintenanceGate).await? {
                    Observed::MaintenanceGate {
                        generation,
                        active: None,
                        handoff_pending: false,
                    } => generation,
                    Observed::MaintenanceGate { .. } => return Err(rejected(Error::Busy)),
                    _ => return Err(unavailable("invalid checked maintenance gate")),
                };
                let (_, effective, _) = self.membership_view(target, None).await?;
                effective
                    .first_phase("l7-maintenance-preflight", &operation)
                    .map_err(unavailable)?;
                self.validate_membership_endpoints(target, &operation, &effective)
                    .await?;
                control(C::AcquireMaintenance {
                    request_id,
                    target,
                    operation,
                    expected_generation: generation,
                })
            }
            Action::ApplyMaintenance { .. } | Action::ReconcileMaintenance { .. } => {
                Err(rejected(Error::WrongGroup))
            }
            Action::Bootstrap => {
                if group == CONTROLLER {
                    return control(C::Initialize {
                        groups: self.topology.groups.clone(),
                        owners: self.topology.owners.clone(),
                    });
                }
                match self.checked(CONTROLLER, Query::Configuration).await? {
                    Observed::Configuration {
                        initialized_at: Some(_),
                        groups,
                        shard_count,
                        bootstrap_safe: true,
                        ..
                    } if groups == self.topology.groups
                        && usize::from(shard_count) == self.topology.owners.len() => {}
                    _ => {
                        return Err(unavailable(
                            "controller cannot authorize fresh data bootstrap",
                        ))
                    }
                }
                data(D::Initialize {
                    shard_count: self.topology.owners.len() as u16,
                    groups: self.topology.groups.clone(),
                    owned: self
                        .topology
                        .owners
                        .iter()
                        .enumerate()
                        .filter(|(_, g)| **g == group)
                        .map(|(s, _)| s as u16)
                        .collect(),
                })
            }
            Action::Register {
                shard,
                epoch,
                nonce,
            } => data(D::Register {
                shard,
                epoch,
                nonce,
            }),
            Action::Close {
                shard,
                epoch,
                session,
            } => data(D::Close {
                shard,
                epoch,
                session,
            }),
            Action::Mutate {
                shard,
                epoch,
                session,
                key,
                sequence,
                value,
            } => data(D::Mutate {
                shard,
                epoch,
                session,
                key,
                sequence,
                value,
            }),
            Action::BeginMove {
                request_id,
                shard,
                epoch,
                destination,
            } => {
                if group != CONTROLLER {
                    return Err(rejected(Error::WrongGroup));
                }
                if let Observed::Transfer { transfer: Some(_) } = self
                    .checked(
                        CONTROLLER,
                        Query::Transfer {
                            request_id: request_id.clone(),
                        },
                    )
                    .await?
                {
                    return control(C::Begin {
                        request_id,
                        shard,
                        expected_epoch: epoch,
                        destination,
                    });
                }
                match self.checked(CONTROLLER, Query::MaintenanceGate).await? {
                    Observed::MaintenanceGate {
                        active: Some(_), ..
                    } => return Err(rejected(Error::Busy)),
                    Observed::MaintenanceGate { active: None, .. } => {}
                    _ => return Err(unavailable("invalid checked maintenance gate")),
                }
                let route = match self.checked(CONTROLLER, Query::Route { shard }).await? {
                    Observed::Route { route, .. } if route.epoch == epoch => route,
                    _ => return Err(rejected(Error::StaleRoute)),
                };
                for endpoint in [route.owner, destination] {
                    match self.checked(endpoint, Query::Configuration).await? {
                        Observed::Configuration {
                            group: actual,
                            initialized_at: Some(_),
                            groups,
                            shard_count,
                            ..
                        } if actual == endpoint
                            && groups == self.topology.groups
                            && usize::from(shard_count) == self.topology.owners.len() => {}
                        _ => {
                            return Err(unavailable(
                                "both data groups must be initialized before beginning a move",
                            ))
                        }
                    }
                }
                control(C::Begin {
                    request_id,
                    shard,
                    expected_epoch: epoch,
                    destination,
                })
            }
            Action::Advance { request_id, step } => {
                let transfer = self.transfer(&request_id).await?;
                let movement = &transfer.movement;
                let shard = movement.id.shard;
                let expected = match step {
                    Step::Fence | Step::Cleanup | Step::AbortSource => movement.source,
                    Step::BeginInstall
                    | Step::PullChunk
                    | Step::FinishInstall
                    | Step::Activate
                    | Step::AbortDestination => movement.destination,
                    _ => CONTROLLER,
                };
                if group != expected {
                    return Err(rejected(Error::WrongGroup));
                }
                match (step, &transfer.phase) {
                    (Step::Fence, Phase::Started) => data(D::Fence {
                        movement: movement.clone(),
                    }),
                    (Step::RecordFence, Phase::Started) => {
                        let ShardView::Fenced { image } =
                            self.shard(movement.source, shard).await?
                        else {
                            return Err(rejected(Error::WrongPhase));
                        };
                        if image.movement != *movement {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        control(C::Fenced { image })
                    }
                    (Step::BeginInstall, Phase::Fenced { image }) => {
                        if self.shard(movement.source, shard).await?
                            != (ShardView::Fenced {
                                image: image.clone(),
                            })
                        {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        data(D::BeginInstall {
                            image: image.clone(),
                        })
                    }
                    (Step::PullChunk, Phase::Fenced { image }) => {
                        let ShardView::Installing {
                            image: target,
                            next_offset,
                        } = self.shard(movement.destination, shard).await?
                        else {
                            return Err(rejected(Error::WrongPhase));
                        };
                        if target != *image || next_offset >= image.bytes {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        match self
                            .checked(
                                movement.source,
                                Query::Export {
                                    id: movement.id.clone(),
                                    offset: next_offset,
                                },
                            )
                            .await?
                        {
                            Observed::Export {
                                image: source,
                                offset,
                                bytes,
                            } if source == *image
                                && offset == next_offset
                                && !bytes.is_empty()
                                && bytes.len() <= CHUNK_BYTES =>
                            {
                                data(D::Chunk {
                                    id: movement.id.clone(),
                                    offset,
                                    bytes,
                                })
                            }
                            _ => Err(rejected(Error::WrongTransfer)),
                        }
                    }
                    (Step::FinishInstall, Phase::Fenced { image }) => {
                        if self.shard(movement.source, shard).await?
                            != (ShardView::Fenced {
                                image: image.clone(),
                            })
                        {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        data(D::FinishInstall {
                            id: movement.id.clone(),
                        })
                    }
                    (Step::RecordInstalled, Phase::Fenced { image }) => {
                        let ShardView::Installed { installation } =
                            self.shard(movement.destination, shard).await?
                        else {
                            return Err(rejected(Error::WrongPhase));
                        };
                        if installation.image != *image {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        control(C::Installed { installation })
                    }
                    (Step::Own, Phase::Installed { installation }) => {
                        if self.shard(movement.destination, shard).await?
                            != (ShardView::Installed {
                                installation: installation.clone(),
                            })
                        {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        control(C::Own {
                            id: movement.id.clone(),
                        })
                    }
                    (Step::Activate, Phase::Owned { ownership }) => data(D::Activate {
                        ownership: ownership.clone(),
                    }),
                    (Step::RecordActivated, Phase::Owned { ownership }) => {
                        let ShardView::Active {
                            epoch,
                            activation: Some(activation),
                        } = self.shard(movement.destination, shard).await?
                        else {
                            return Err(rejected(Error::WrongPhase));
                        };
                        if epoch != movement.id.epoch || activation.ownership != *ownership {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        control(C::Activated { activation })
                    }
                    (Step::Cleanup, Phase::Activated { activation }) => {
                        if self.shard(movement.destination, shard).await?
                            != (ShardView::Active {
                                epoch: movement.id.epoch,
                                activation: Some(activation.clone()),
                            })
                        {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        data(D::Cleanup {
                            activation: activation.clone(),
                        })
                    }
                    (Step::RecordCleanup, Phase::Activated { activation }) => {
                        let ShardView::Retired {
                            activation: source,
                            cleaned,
                        } = self.shard(movement.source, shard).await?
                        else {
                            return Err(rejected(Error::WrongPhase));
                        };
                        if source != *activation {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        control(C::Cleaned {
                            id: movement.id.clone(),
                            cleaned,
                        })
                    }
                    (
                        Step::Complete,
                        Phase::Cleaned {
                            activation,
                            cleaned,
                        },
                    ) => {
                        if self.shard(movement.source, shard).await?
                            != (ShardView::Retired {
                                activation: activation.clone(),
                                cleaned: *cleaned,
                            })
                            || self.shard(movement.destination, shard).await?
                                != (ShardView::Active {
                                    epoch: movement.id.epoch,
                                    activation: Some(activation.clone()),
                                })
                        {
                            return Err(rejected(Error::WrongTransfer));
                        }
                        control(C::Complete {
                            id: movement.id.clone(),
                        })
                    }
                    (
                        Step::Abort,
                        Phase::Started | Phase::Fenced { .. } | Phase::Installed { .. },
                    ) => control(C::Abort {
                        id: movement.id.clone(),
                    }),
                    (Step::AbortSource | Step::AbortDestination, Phase::Aborted { abort, .. }) => {
                        data(D::AbortTransfer {
                            abort: abort.clone(),
                        })
                    }
                    (
                        Step::RecordSourceAbort | Step::RecordDestinationAbort,
                        Phase::Aborted { abort, .. },
                    ) => {
                        let endpoint = if matches!(step, Step::RecordSourceAbort) {
                            movement.source
                        } else {
                            movement.destination
                        };
                        match self
                            .checked(
                                endpoint,
                                Query::Aborted {
                                    id: movement.id.clone(),
                                },
                            )
                            .await?
                        {
                            Observed::Aborted {
                                group: actual,
                                record: Some(record),
                            } if actual == endpoint && record.abort == *abort => {
                                control(C::EndpointAborted {
                                    abort: abort.clone(),
                                    recovered: record.recovered,
                                })
                            }
                            _ => Err(rejected(Error::WrongTransfer)),
                        }
                    }
                    _ => Err(rejected(Error::WrongPhase)),
                }
            }
        }
    }
    pub async fn reconcile_once(self: &Arc<Self>, request_id: &str) -> Response {
        let transfer = match self.transfer(request_id).await {
            Ok(t) => t,
            Err(r) => return r,
        };
        if transfer.complete() {
            return self
                .query_group(
                    CONTROLLER,
                    Query::Transfer {
                        request_id: request_id.into(),
                    },
                )
                .await;
        }
        let movement = &transfer.movement;
        let shard = movement.id.shard;
        let choice: Result<(GroupId, Step), Response> = async {
            Ok(match &transfer.phase {
                Phase::Started => match self.shard(movement.source, shard).await? {
                    ShardView::Fenced { image } if image.movement == *movement => {
                        (CONTROLLER, Step::RecordFence)
                    }
                    _ => (movement.source, Step::Fence),
                },
                Phase::Fenced { image } => match self.shard(movement.destination, shard).await? {
                    ShardView::Installing {
                        image: target,
                        next_offset,
                    } if target == *image => (
                        movement.destination,
                        if next_offset == image.bytes {
                            Step::FinishInstall
                        } else {
                            Step::PullChunk
                        },
                    ),
                    ShardView::Installed { installation } if installation.image == *image => {
                        (CONTROLLER, Step::RecordInstalled)
                    }
                    _ => (movement.destination, Step::BeginInstall),
                },
                Phase::Installed { .. } => (CONTROLLER, Step::Own),
                Phase::Owned { ownership } => {
                    match self.shard(movement.destination, shard).await? {
                        ShardView::Active {
                            activation: Some(a),
                            ..
                        } if a.ownership == *ownership => (CONTROLLER, Step::RecordActivated),
                        _ => (movement.destination, Step::Activate),
                    }
                }
                Phase::Activated { activation } => {
                    match self.shard(movement.source, shard).await? {
                        ShardView::Retired { activation: a, .. } if a == *activation => {
                            (CONTROLLER, Step::RecordCleanup)
                        }
                        _ => (movement.source, Step::Cleanup),
                    }
                }
                Phase::Cleaned { .. } => (CONTROLLER, Step::Complete),
                Phase::Aborted {
                    abort,
                    source_recovered,
                    destination_recovered,
                    ..
                } => {
                    let (endpoint, apply, record) = if source_recovered.is_none() {
                        (movement.source, Step::AbortSource, Step::RecordSourceAbort)
                    } else if destination_recovered.is_none() {
                        (
                            movement.destination,
                            Step::AbortDestination,
                            Step::RecordDestinationAbort,
                        )
                    } else {
                        return Err(unavailable("terminal abort unexpectedly pending"));
                    };
                    match self
                        .checked(
                            endpoint,
                            Query::Aborted {
                                id: movement.id.clone(),
                            },
                        )
                        .await?
                    {
                        Observed::Aborted {
                            group,
                            record: Some(r),
                        } if group == endpoint && r.abort == *abort => (CONTROLLER, record),
                        _ => (endpoint, apply),
                    }
                }
                Phase::Complete { .. } => {
                    return Err(unavailable("completed transfer unexpectedly pending"))
                }
            })
        }
        .await;
        let (group, step) = match choice {
            Ok(v) => v,
            Err(r) => return r,
        };
        let response = self
            .execute_group(ExecuteRequest {
                group,
                action: Action::Advance {
                    request_id: request_id.into(),
                    step,
                },
            })
            .await;
        tracing::info!(request_id, group, ?step, ?response, "handoff phase attempt");
        if matches!(step, Step::Fence | Step::BeginInstall) && capacity(&response) {
            let abort = self
                .execute_group(ExecuteRequest {
                    group: CONTROLLER,
                    action: Action::Advance {
                        request_id: request_id.into(),
                        step: Step::Abort,
                    },
                })
                .await;
            tracing::info!(
                request_id,
                ?abort,
                "handoff capacity refusal requests recoverable abort"
            );
        }
        response
    }
    pub async fn background(
        self: Arc<Self>,
        mut shutdown: kv_node::shutdown::ShutdownRx,
        automatic: bool,
    ) {
        let mut cadence = tokio::time::interval(Duration::from_millis(100));
        cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {_=shutdown.requested()=>return,_=cadence.tick()=>{}}
            for (group, handle) in &self.groups {
                if shutdown.is_requested() {
                    return;
                }
                if handle.status().role != "leader" {
                    continue;
                }
                let needs_bootstrap = match handle.query(Query::Configuration).await {
                    Response::Checked { observed, .. } => matches!(
                        *observed,
                        Observed::Configuration {
                            initialized_at: None,
                            ..
                        }
                    ),
                    _ => false,
                };
                if needs_bootstrap {
                    let response = self
                        .apply_intent(ExecuteRequest {
                            group: *group,
                            action: Action::Bootstrap,
                        })
                        .await;
                    tracing::debug!(group, ?response, "group bootstrap attempt");
                }
            }
            if automatic
                && self
                    .groups
                    .get(&CONTROLLER)
                    .is_some_and(|g| g.status().role == "leader")
            {
                if let Ok(Observed::MaintenanceGate {
                    active: Some(ticket),
                    ..
                }) = self.checked(CONTROLLER, Query::MaintenanceGate).await
                {
                    let response = self.reconcile_maintenance(&ticket.request_id).await;
                    tracing::debug!(?response, "maintenance reconciliation attempt");
                }
                if let Ok(Observed::PendingTransfers { transfers }) =
                    self.checked(CONTROLLER, Query::PendingTransfers).await
                {
                    for transfer in transfers {
                        if shutdown.is_requested() {
                            return;
                        }
                        self.reconcile_once(&transfer.request_id).await;
                    }
                }
            }
        }
    }
}

async fn status(State(service): State<Arc<Service>>) -> Json<serde_json::Value> {
    Json(
        serde_json::json!({"cluster":service.fingerprint,"node":service.node,"groups":service.groups.values().map(GroupHandle::status).collect::<Vec<_>>(),"route_errors":service.routes.errors()}),
    )
}
fn wrap(service: &Service, response: Response) -> Json<Wire<Response>> {
    Json(Wire {
        cluster: service.fingerprint.clone(),
        body: response,
    })
}
async fn query_local(
    State(s): State<Arc<Service>>,
    Json(wire): Json<Wire<ReadRequest>>,
) -> Json<Wire<Response>> {
    let Ok(_permit) = s.admission.try_acquire() else {
        return wrap(&s, unavailable("control admission full"));
    };
    if wire.cluster != s.fingerprint {
        return wrap(&s, rejected(Error::WrongGroup));
    }
    let response = match s.groups.get(&wire.body.group) {
        Some(handle) => handle.query(wire.body.query).await,
        None => rejected(Error::WrongGroup),
    };
    wrap(&s, response)
}
async fn execute_local(
    State(s): State<Arc<Service>>,
    Json(wire): Json<Wire<ExecuteRequest>>,
) -> Json<Wire<Response>> {
    let Ok(_permit) = s.admission.try_acquire() else {
        return wrap(&s, unavailable("control admission full"));
    };
    if wire.cluster != s.fingerprint {
        return wrap(&s, rejected(Error::WrongGroup));
    }
    let response = tokio::time::timeout(Duration::from_secs(10), s.apply_intent(wire.body))
        .await
        .unwrap_or(Response::Unknown);
    wrap(&s, response)
}
async fn query_routed(
    State(s): State<Arc<Service>>,
    Json(wire): Json<Wire<ReadRequest>>,
) -> Json<Wire<Response>> {
    let Ok(_permit) = s.admission.try_acquire() else {
        return wrap(&s, unavailable("control admission full"));
    };
    if wire.cluster != s.fingerprint {
        return wrap(&s, rejected(Error::WrongGroup));
    }
    wrap(&s, s.query_group(wire.body.group, wire.body.query).await)
}
async fn execute_routed(
    State(s): State<Arc<Service>>,
    Json(wire): Json<Wire<ExecuteRequest>>,
) -> Json<Wire<Response>> {
    let Ok(_permit) = s.admission.try_acquire() else {
        return wrap(&s, unavailable("control admission full"));
    };
    if wire.cluster != s.fingerprint {
        return wrap(&s, rejected(Error::WrongGroup));
    }
    wrap(&s, s.execute_group(wire.body).await)
}
async fn reconcile(
    State(s): State<Arc<Service>>,
    Json(wire): Json<Wire<ReconcileRequest>>,
) -> Json<Wire<Response>> {
    let Ok(_permit) = s.admission.try_acquire() else {
        return wrap(&s, unavailable("control admission full"));
    };
    if wire.cluster != s.fingerprint {
        return wrap(&s, rejected(Error::WrongGroup));
    }
    wrap(&s, s.reconcile_once(&wire.body.request_id).await)
}
