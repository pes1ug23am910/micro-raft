//! Local HTTP API for writes, local reads, and node status.

use std::time::Duration;

use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{header, HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, put};
use axum::{Json, Router};
use raft_core::{Command, LogIndex, NodeId};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};

use crate::driver::{ProposalRequest, ProposalResult};
use crate::kv::SharedReadState;

/// Values are intentionally kept well below the peer protocol's frame cap.
pub const MAX_VALUE_BYTES: usize = 64 * 1024;
pub const MAX_KEY_BYTES: usize = 1024;
pub const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct ApiState {
    shared: SharedReadState,
    proposals: mpsc::Sender<ProposalRequest>,
}

impl ApiState {
    pub fn new(shared: SharedReadState, proposals: mpsc::Sender<ProposalRequest>) -> Self {
        Self { shared, proposals }
    }
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/kv/{key}", put(put_key).delete(delete_key).get(get_key))
        .route("/status", get(status))
        .layer(DefaultBodyLimit::max(MAX_VALUE_BYTES))
        .with_state(state)
}

pub async fn serve(listener: TcpListener, state: ApiState) -> std::io::Result<()> {
    axum::serve(listener, router(state)).await
}

#[derive(Serialize)]
struct WriteOk {
    ok: bool,
    index: LogIndex,
}

#[derive(Serialize)]
struct NotLeader {
    error: &'static str,
    leader_hint: Option<NodeId>,
}

#[derive(Serialize)]
struct TimeoutError {
    error: &'static str,
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

async fn submit(state: &ApiState, command: Command) -> Response {
    let (respond_to, response) = oneshot::channel();
    let request = ProposalRequest {
        command,
        respond_to,
    };
    let result = tokio::time::timeout(WRITE_TIMEOUT, async {
        state.proposals.send(request).await.map_err(|_| ())?;
        response.await.map_err(|_| ())
    })
    .await;

    match result {
        Ok(Ok(ProposalResult::Applied { index })) => {
            (StatusCode::OK, Json(WriteOk { ok: true, index })).into_response()
        }
        Ok(Ok(ProposalResult::NotLeader { leader_hint })) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(NotLeader {
                error: "not_leader",
                leader_hint,
            }),
        )
            .into_response(),
        Ok(Err(())) | Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(TimeoutError { error: "timeout" }),
        )
            .into_response(),
    }
}

async fn get_key(State(state): State<ApiState>, Path(key): Path<String>) -> Response {
    if key.len() > MAX_KEY_BYTES {
        return key_too_large();
    }
    let (status, value) = state.shared.get(&key);
    let mut response = match value {
        Some(value) => (StatusCode::OK, value).into_response(),
        None => StatusCode::NOT_FOUND.into_response(),
    };
    let is_success = response.status() == StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        HeaderName::from_static("x-raft-role"),
        HeaderValue::from_static(status.role.as_str()),
    );
    if let Ok(value) = HeaderValue::from_str(&status.last_applied.to_string()) {
        headers.insert(HeaderName::from_static("x-raft-last-applied"), value);
    }
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
        Json(TimeoutError {
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
    async fn local_reads_expose_staleness_headers() {
        let node = RaftNode::new(1, vec![2, 3], 9);
        let shared = SharedReadState::from_node(&node);
        shared.apply(&raft_core::Entry {
            index: 1,
            term: 1,
            command: Command::Put {
                key: "k".into(),
                value: "v".into(),
            },
        });
        let (tx, _rx) = mpsc::channel(1);
        let response = get_key(State(ApiState::new(shared, tx)), Path("k".to_owned())).await;

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
}
