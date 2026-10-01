//! Local HTTP API for writes, local reads, and node status.

use std::time::Duration;

use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{header, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use raft_core::{Command, LogIndex, NodeId};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::admin::{AdminCommand, AdminRequest, AdminResult, ADMIN_TIMEOUT};
use crate::application::SessionError;
pub use crate::application::{MAX_KEY_BYTES, MAX_VALUE_BYTES};
use crate::driver::{ProposalRequest, ProposalResult, ReadRequest, ReadResult, READ_TIMEOUT};
use crate::kv::SharedReadState;
use crate::shutdown::ShutdownRx;

pub const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct ApiState {
    shared: SharedReadState,
    proposals: mpsc::Sender<ProposalRequest>,
    reads: Option<mpsc::Sender<ReadRequest>>,
    administration: Option<mpsc::Sender<AdminRequest>>,
    route_validator: Option<crate::membership_runtime::RouteValidator>,
    shutdown: Option<ShutdownRx>,
}

impl ApiState {
    pub fn new(shared: SharedReadState, proposals: mpsc::Sender<ProposalRequest>) -> Self {
        Self {
            shared,
            proposals,
            reads: None,
            administration: None,
            route_validator: None,
            shutdown: None,
        }
    }

    pub fn with_administration(
        mut self,
        sender: mpsc::Sender<AdminRequest>,
        validator: crate::membership_runtime::RouteValidator,
    ) -> Self {
        self.administration = Some(sender);
        self.route_validator = Some(validator);
        self
    }

    pub fn with_reads(mut self, reads: mpsc::Sender<ReadRequest>) -> Self {
        self.reads = Some(reads);
        self
    }

    pub fn with_shutdown(mut self, shutdown: ShutdownRx) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    fn is_shutting_down(&self) -> bool {
        self.shutdown.as_ref().is_some_and(ShutdownRx::is_requested)
    }
}

async fn admit(State(state): State<ApiState>, request: Request, next: Next) -> Response {
    if state.is_shutting_down() {
        return api_error("shutting_down");
    }
    next.run(request).await
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/kv/{key}", put(put_key).delete(delete_key).get(get_key))
        .route("/status", get(status))
        .route("/admin/membership", post(change_membership))
        .route("/admin/membership/{request_id}", get(membership_result))
        .route("/sessions", post(register_session))
        .route("/sessions/{session_id}", delete(close_session))
        .route(
            "/sessions/{session_id}/kv/{key}",
            put(session_put).delete(session_delete),
        )
        .layer(DefaultBodyLimit::max(MAX_VALUE_BYTES))
        .layer(middleware::from_fn_with_state(state.clone(), admit))
        .with_state(state)
}

pub async fn serve(listener: TcpListener, state: ApiState) -> std::io::Result<()> {
    axum::serve(listener, router(state)).await
}

/// Stop accepts on shutdown; already accepted requests remain bounded by their
/// normal request deadlines and the binary's final connection-cleanup budget.
pub async fn serve_with_shutdown(
    listener: TcpListener,
    state: ApiState,
    mut shutdown: ShutdownRx,
) -> std::io::Result<()> {
    let state = state.with_shutdown(shutdown.clone());
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            shutdown.requested().await;
        })
        .await
}

#[derive(Serialize)]
struct WriteOk {
    ok: bool,
    index: LogIndex,
}

#[derive(Serialize)]
struct ProposalError {
    error: &'static str,
    leader_hint: Option<NodeId>,
}

#[derive(Serialize)]
struct ApiError {
    error: &'static str,
}

async fn membership_result(
    State(state): State<ApiState>,
    Path(request_id): Path<String>,
) -> Response {
    let status = state.shared.status();
    let record = status.committed_membership.state.records.get(&request_id);
    let code = if record.is_some() {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    };
    (code, Json(serde_json::json!({ "read_mode": "local", "membership_index": status.committed_membership.index,
        "last_applied": status.last_applied, "request_id": request_id, "record": record }))).into_response()
}
async fn change_membership(
    State(state): State<ApiState>,
    Json(command): Json<AdminCommand>,
) -> Response {
    if state.is_shutting_down() {
        return api_error("shutting_down");
    }
    let Some(sender) = &state.administration else {
        return api_error("administration_unavailable");
    };
    let deadline = tokio::time::Instant::now() + ADMIN_TIMEOUT;
    let request_id = command.request_id.clone();
    // The replicated cached result is authoritative even when an old endpoint
    // is no longer resolvable. Exact retries bypass only this pre-admission DNS.
    let status = state.shared.status();
    let retry = status
        .effective_membership
        .records
        .get(&request_id)
        .is_some_and(|record| record.operation == command.operation);
    if !retry {
        if let raft_core::membership::AdminOperation::AddLearner { id, endpoints } =
            &command.operation
        {
            let Some(validator) = &state.route_validator else {
                return api_error("routing_unavailable");
            };
            match tokio::time::timeout_at(
                deadline,
                validator.validate(*id, endpoints, &status.effective_membership),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(reason)) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(AdminResult::Rejected {
                            reason,
                            leader_hint: status.leader_hint,
                        }),
                    )
                        .into_response()
                }
                Err(_) => return api_error("endpoint_validation_timeout"),
            }
        }
    }
    let (respond_to, response) = oneshot::channel();
    match tokio::time::timeout_at(
        deadline,
        sender.send(AdminRequest {
            command,
            validated_membership: Some(crate::admin::membership_revision(
                &status.effective_membership,
            )),
            respond_to,
        }),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return api_error("administration_unavailable"),
        Err(_) => return api_error("admin_queue_timeout"),
    }
    let result = match tokio::time::timeout_at(deadline, response).await {
        Ok(Ok(result)) => result,
        _ => AdminResult::Unknown {
            request_id,
            leader_hint: state.shared.status().leader_hint,
        },
    };
    let code = match &result {
        AdminResult::Completed { .. } => StatusCode::OK,
        AdminResult::Rejected { reason, .. }
            if reason != "not_leader" && reason != "shutting_down" =>
        {
            StatusCode::CONFLICT
        }
        _ => StatusCode::SERVICE_UNAVAILABLE,
    };
    (code, Json(result)).into_response()
}

async fn put_key(
    State(state): State<ApiState>,
    Path(key): Path<String>,
    value: String,
) -> Response {
    if key.len() > MAX_KEY_BYTES {
        return key_too_large();
    }
    submit(&state, Command::Put { key, value }).await
}

async fn delete_key(State(state): State<ApiState>, Path(key): Path<String>) -> Response {
    if key.len() > MAX_KEY_BYTES {
        return key_too_large();
    }
    submit(&state, Command::Delete { key }).await
}

#[derive(Deserialize)]
struct Sequence {
    sequence: u64,
}

/// The body is a caller-retained nonce. Retrying it never creates a new token.
async fn register_session(State(state): State<ApiState>, nonce: String) -> Response {
    submit(&state, Command::RegisterSession { nonce }).await
}

async fn close_session(
    State(state): State<ApiState>,
    Path(session_id): Path<LogIndex>,
) -> Response {
    submit(&state, Command::CloseSession { session_id }).await
}

async fn session_put(
    State(state): State<ApiState>,
    Path((session_id, key)): Path<(LogIndex, String)>,
    Query(Sequence { sequence }): Query<Sequence>,
    value: String,
) -> Response {
    if key.len() > MAX_KEY_BYTES {
        return key_too_large();
    }
    submit(
        &state,
        Command::SessionPut {
            session_id,
            key,
            sequence,
            value,
        },
    )
    .await
}

async fn session_delete(
    State(state): State<ApiState>,
    Path((session_id, key)): Path<(LogIndex, String)>,
    Query(Sequence { sequence }): Query<Sequence>,
) -> Response {
    if key.len() > MAX_KEY_BYTES {
        return key_too_large();
    }
    submit(
        &state,
        Command::SessionDelete {
            session_id,
            key,
            sequence,
        },
    )
    .await
}

async fn submit(state: &ApiState, command: Command) -> Response {
    if state.is_shutting_down() {
        return api_error("shutting_down");
    }
    let deadline = tokio::time::Instant::now() + WRITE_TIMEOUT;
    let (respond_to, response) = oneshot::channel();
    let request = ProposalRequest {
        command,
        respond_to,
    };

    match tokio::time::timeout_at(deadline, state.proposals.send(request)).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return api_error("unavailable"),
        // Cancelling a pending Tokio mpsc send leaves the request out of the
        // queue, so this timeout is known not to have reached the driver.
        Err(_) => return api_error("timeout"),
    }

    match tokio::time::timeout_at(deadline, response).await {
        Ok(Ok(ProposalResult::Applied { index })) => {
            (StatusCode::OK, Json(WriteOk { ok: true, index })).into_response()
        }
        Ok(Ok(ProposalResult::Session(result))) => {
            let status = match result.error {
                None => StatusCode::OK,
                Some(
                    SessionError::InvalidNonce
                    | SessionError::InvalidKey
                    | SessionError::InvalidSequence,
                ) => StatusCode::BAD_REQUEST,
                Some(SessionError::ValueTooLarge) => StatusCode::PAYLOAD_TOO_LARGE,
                Some(SessionError::UnknownSession) => StatusCode::NOT_FOUND,
                Some(SessionError::SessionClosed) => StatusCode::GONE,
                Some(
                    SessionError::SessionCapacity
                    | SessionError::KeyCapacity
                    | SessionError::PayloadCapacity,
                ) => StatusCode::INSUFFICIENT_STORAGE,
                Some(_) => StatusCode::CONFLICT,
            };
            (status, Json(result)).into_response()
        }
        Ok(Ok(ProposalResult::NotLeader { leader_hint })) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ProposalError {
                error: "not_leader",
                leader_hint,
            }),
        )
            .into_response(),
        Ok(Ok(ProposalResult::AdmissionRejected { reason })) => api_error(reason),
        Ok(Ok(ProposalResult::ShuttingDown)) => api_error("shutting_down"),
        Ok(Ok(ProposalResult::OutcomeUnknown { leader_hint })) => outcome_unknown(leader_hint),
        // The driver accepted the queue item, so a lost sender or elapsed
        // deadline cannot safely be treated as a rejection: it may commit.
        Ok(Err(_)) | Err(_) => outcome_unknown(state.shared.status().leader_hint),
    }
}

fn api_error(error: &'static str) -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(ApiError { error })).into_response()
}

fn outcome_unknown(leader_hint: Option<NodeId>) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ProposalError {
            error: "outcome_unknown",
            leader_hint,
        }),
    )
        .into_response()
}

#[derive(Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ReadConsistency {
    #[default]
    Local,
    Linearizable,
}
#[derive(Default, Deserialize)]
struct ReadOptions {
    #[serde(default)]
    consistency: ReadConsistency,
}

async fn get_key(
    State(state): State<ApiState>,
    Path(key): Path<String>,
    Query(options): Query<ReadOptions>,
) -> Response {
    if key.len() > MAX_KEY_BYTES {
        return key_too_large();
    }
    if state.is_shutting_down() {
        return api_error("shutting_down");
    }
    if matches!(options.consistency, ReadConsistency::Local) {
        let (status, value) = state.shared.get(&key);
        return read_response(value, status.role.as_str(), status.last_applied, "local");
    }
    let Some(reads) = &state.reads else {
        return api_error("read_unavailable");
    };
    let deadline = tokio::time::Instant::now() + READ_TIMEOUT;
    let (respond_to, response) = oneshot::channel();
    let request = ReadRequest { key, respond_to };
    match tokio::time::timeout_at(deadline, reads.send(request)).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return api_error("read_unavailable"),
        Err(_) => return api_error("read_timeout"),
    }
    match tokio::time::timeout_at(deadline, response).await {
        Ok(Ok(ReadResult::Ready {
            value,
            index,
            applied_index,
            term,
            context,
        })) => {
            if index == 0 || term == 0 || context == 0 || applied_index < index {
                return api_error("invalid_read_barrier");
            }
            let mut response = read_response(value, "leader", applied_index, "linearizable");
            for (name, value) in [
                ("x-raft-read-index", index),
                ("x-raft-term", term),
                ("x-raft-read-context", context),
            ] {
                response.headers_mut().insert(
                    HeaderName::from_static(name),
                    HeaderValue::from_str(&value.to_string())
                        .expect("decimal u64 is a header value"),
                );
            }
            response
        }
        Ok(Ok(ReadResult::Rejected {
            reason,
            leader_hint,
        })) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ProposalError {
                error: reason,
                leader_hint,
            }),
        )
            .into_response(),
        Ok(Err(_)) => api_error("read_unavailable"),
        Err(_) => api_error("read_timeout"),
    }
}

fn read_response(
    value: Option<String>,
    role: &'static str,
    applied: LogIndex,
    mode: &'static str,
) -> Response {
    let mut response = match value {
        Some(value) => (StatusCode::OK, value).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    };
    let is_success = response.status() == StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        HeaderName::from_static("x-raft-role"),
        HeaderValue::from_static(role),
    );
    headers.insert(
        HeaderName::from_static("x-raft-read-mode"),
        HeaderValue::from_static(mode),
    );
    headers.insert(
        HeaderName::from_static("x-raft-last-applied"),
        HeaderValue::from_str(&applied.to_string()).expect("decimal u64 is a header value"),
    );
    if !headers.contains_key(header::CONTENT_TYPE) && is_success {
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
    }
    response
}
fn key_too_large() -> Response {
    (
        StatusCode::PAYLOAD_TOO_LARGE,
        Json(ApiError {
            error: "key_too_large",
        }),
    )
        .into_response()
}

async fn status(State(state): State<ApiState>) -> impl IntoResponse {
    Json(state.shared.status())
}

#[cfg(test)]
mod tests {
    use axum::body::to_bytes;
    use raft_core::RaftNode;
    use serde_json::Value;

    use super::*;

    fn api() -> (ApiState, mpsc::Receiver<ProposalRequest>) {
        let node = RaftNode::new(1, vec![2, 3], 9);
        let shared = SharedReadState::from_node(&node);
        let (tx, rx) = mpsc::channel(4);
        (ApiState::new(shared, tx), rx)
    }

    #[tokio::test]
    async fn shutdown_rejects_write_before_it_enters_proposal_queue() {
        let (api, mut proposals) = api();
        let (handle, shutdown) = crate::shutdown::channel();
        let api = api.with_shutdown(shutdown);
        handle.request();
        let response = put_key(State(api), Path("k".into()), "v".into()).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap()["error"],
            "shutting_down"
        );
        assert!(proposals.try_recv().is_err());
    }

    #[tokio::test]
    async fn queued_shutdown_rejection_is_definite_not_an_acknowledgment() {
        let (api, mut proposals) = api();
        let responder = tokio::spawn(async move {
            let request = proposals.recv().await.unwrap();
            request
                .respond_to
                .send(ProposalResult::ShuttingDown)
                .unwrap();
        });
        let response = put_key(State(api), Path("k".into()), "v".into()).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            serde_json::json!({"error":"shutting_down"})
        );
        responder.await.unwrap();
    }

    #[tokio::test]
    async fn put_returns_only_after_apply_confirmation() {
        let (api, mut proposals) = api();
        let responder = tokio::spawn(async move {
            let request = proposals.recv().await.expect("proposal");
            assert_eq!(
                request.command,
                Command::Put {
                    key: "k".into(),
                    value: "v".into()
                }
            );
            request
                .respond_to
                .send(ProposalResult::Applied { index: 7 })
                .expect("handler waiting");
        });

        let response = put_key(State(api), Path("k".into()), "v".into()).await;
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 1024).await.expect("body");
        let json: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(json, serde_json::json!({"ok": true, "index": 7}));
        responder.await.expect("responder");
    }

    #[tokio::test]
    async fn not_leader_response_includes_hint() {
        let (api, mut proposals) = api();
        let responder = tokio::spawn(async move {
            let request = proposals.recv().await.expect("proposal");
            request
                .respond_to
                .send(ProposalResult::NotLeader {
                    leader_hint: Some(2),
                })
                .expect("handler waiting");
        });

        let response = delete_key(State(api), Path("k".into())).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 1024).await.expect("body");
        let json: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(
            json,
            serde_json::json!({"error": "not_leader", "leader_hint": 2})
        );
        responder.await.expect("responder");
    }

    #[tokio::test]
    async fn accepted_but_unresolved_proposal_reports_outcome_unknown() {
        let (api, mut proposals) = api();
        let responder = tokio::spawn(async move {
            let request = proposals.recv().await.expect("proposal");
            request
                .respond_to
                .send(ProposalResult::OutcomeUnknown {
                    leader_hint: Some(3),
                })
                .expect("handler waiting");
        });

        let response = delete_key(State(api), Path("k".into())).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 1024).await.expect("body");
        let json: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(
            json,
            serde_json::json!({"error": "outcome_unknown", "leader_hint": 3})
        );
        responder.await.expect("responder");
    }

    #[tokio::test]
    async fn lost_completion_after_enqueue_reports_outcome_unknown() {
        let (api, mut proposals) = api();
        let responder = tokio::spawn(async move {
            let request = proposals.recv().await.expect("proposal");
            drop(request.respond_to);
        });

        let response = delete_key(State(api), Path("k".into())).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 1024).await.expect("body");
        let json: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(
            json,
            serde_json::json!({"error": "outcome_unknown", "leader_hint": null})
        );
        responder.await.expect("responder");
    }

    #[tokio::test]
    async fn closed_proposal_queue_reports_unavailable() {
        let (api, proposals) = api();
        drop(proposals);

        let response = delete_key(State(api), Path("k".into())).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = to_bytes(response.into_body(), 1024).await.expect("body");
        let json: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(json, serde_json::json!({"error": "unavailable"}));
    }

    #[tokio::test]
    async fn local_reads_expose_staleness_headers() {
        let node = RaftNode::new(1, vec![2, 3], 9);
        let shared = SharedReadState::from_node(&node);
        shared
            .apply(&raft_core::Entry {
                index: 1,
                term: 1,
                command: Command::Put {
                    key: "k".into(),
                    value: "v".into(),
                },
            })
            .unwrap();
        let (tx, _rx) = mpsc::channel(1);
        let response = get_key(
            State(ApiState::new(shared, tx)),
            Path("k".to_owned()),
            Query(ReadOptions::default()),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["x-raft-role"], "follower");
        assert_eq!(response.headers()["x-raft-last-applied"], "1");
        let body = to_bytes(response.into_body(), 1024).await.expect("body");
        assert_eq!(&body[..], b"v");
    }

    #[tokio::test]
    async fn oversized_key_is_rejected_before_proposal() {
        let (api, mut proposals) = api();
        let response = put_key(State(api), Path("k".repeat(MAX_KEY_BYTES + 1)), "v".into()).await;

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = to_bytes(response.into_body(), 1024).await.expect("body");
        let json: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(json, serde_json::json!({"error": "key_too_large"}));
        assert!(proposals.try_recv().is_err(), "no proposal was enqueued");
    }

    fn checked_read_options() -> Query<ReadOptions> {
        Query(ReadOptions {
            consistency: ReadConsistency::Linearizable,
        })
    }

    #[tokio::test]
    async fn checked_read_waits_for_driver_and_exposes_barrier_for_value_and_absence() {
        for value in [Some("value".to_owned()), None] {
            let (api, _proposals) = self::api();
            let (read_tx, mut read_rx) = mpsc::channel::<ReadRequest>(1);
            let handler = tokio::spawn(get_key(
                State(api.with_reads(read_tx)),
                Path("k".into()),
                checked_read_options(),
            ));
            let request = read_rx.recv().await.unwrap();
            assert_eq!(request.key, "k");
            assert!(
                !handler.is_finished(),
                "local state alone cannot complete a checked read"
            );
            request
                .respond_to
                .send(ReadResult::Ready {
                    value: value.clone(),
                    index: 4,
                    applied_index: 5,
                    term: 2,
                    context: 9,
                })
                .unwrap();
            let response = handler.await.unwrap();
            assert_eq!(
                response.status(),
                if value.is_some() {
                    StatusCode::OK
                } else {
                    StatusCode::NOT_FOUND
                }
            );
            for (name, expected) in [
                ("x-raft-read-mode", "linearizable"),
                ("x-raft-read-index", "4"),
                ("x-raft-last-applied", "5"),
                ("x-raft-term", "2"),
                ("x-raft-read-context", "9"),
            ] {
                assert_eq!(response.headers()[name], expected);
            }
            let body = to_bytes(response.into_body(), 1024).await.unwrap();
            assert_eq!(
                body.as_ref(),
                value.as_deref().unwrap_or_default().as_bytes()
            );
        }
    }

    #[tokio::test]
    async fn checked_read_never_labels_an_invalid_barrier_as_linearizable() {
        for (index, applied_index, term, context) in
            [(2, 1, 1, 1), (0, 1, 1, 1), (1, 1, 0, 1), (1, 1, 1, 0)]
        {
            let (api, _proposals) = self::api();
            let (read_tx, mut read_rx) = mpsc::channel::<ReadRequest>(1);
            let responder = tokio::spawn(async move {
                read_rx
                    .recv()
                    .await
                    .unwrap()
                    .respond_to
                    .send(ReadResult::Ready {
                        value: Some("stale".into()),
                        index,
                        applied_index,
                        term,
                        context,
                    })
                    .unwrap();
            });
            let response = get_key(
                State(api.with_reads(read_tx)),
                Path("k".into()),
                checked_read_options(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
            assert!(!response.headers().contains_key("x-raft-read-mode"));
            let body = to_bytes(response.into_body(), 1024).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&body).unwrap()["error"],
                "invalid_read_barrier"
            );
            responder.await.unwrap();
        }
    }

    #[tokio::test]
    async fn checked_read_rejection_and_unavailable_channel_do_not_fall_back_to_local() {
        let (api, _proposals) = self::api();
        let response = get_key(State(api), Path("k".into()), checked_read_options()).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let (api, _proposals) = self::api();
        let (read_tx, mut read_rx) = mpsc::channel::<ReadRequest>(1);
        let responder = tokio::spawn(async move {
            read_rx
                .recv()
                .await
                .unwrap()
                .respond_to
                .send(ReadResult::Rejected {
                    reason: "not_leader",
                    leader_hint: Some(3),
                })
                .unwrap();
        });
        let response = get_key(
            State(api.with_reads(read_tx)),
            Path("k".into()),
            checked_read_options(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!response.headers().contains_key("x-raft-read-mode"));
        let body = to_bytes(response.into_body(), 1024).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            serde_json::json!({ "error": "not_leader", "leader_hint": 3 })
        );
        responder.await.unwrap();
    }
}
