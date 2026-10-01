use std::collections::BTreeMap;
use std::fs;
use std::net::{SocketAddr, TcpListener as PortReservation};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use kv_node::driver::{
    run_driver_with_snapshots, SnapshotRuntime, PROPOSAL_CHANNEL_CAPACITY, READ_CHANNEL_CAPACITY,
};
use kv_node::http::{self, ApiState, MAX_KEY_BYTES, MAX_VALUE_BYTES};
use kv_node::kv::{NodeRole, SharedReadState};
use kv_node::shutdown;
use kv_node::storage::Storage;
use kv_node::transport::{spawn_listener, TcpTransport, INBOUND_QUEUE_CAPACITY};
use raft_core::{Command, Entry, HardState, NodeId, RaftNode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};
use tracing::Instrument;

static TEST_DIR_SEQ: AtomicU32 = AtomicU32::new(0);
static NEXT_BOOT_SEED: AtomicU64 = AtomicU64::new(1);

fn fresh_boot_seed() -> u64 {
    NEXT_BOOT_SEED
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .expect("test boot seed counter exhausted")
}

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let sequence = TEST_DIR_SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "micro-raft-live-test-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create test dir");
        Self(path)
    }

    fn node(&self, id: NodeId) -> PathBuf {
        self.0.join(format!("n{id}"))
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn kv_node_recovers_from_disk() {
    let dir = TestDir::new();
    let node_dir = dir.node(1);
    let expected_hard = HardState {
        membership: None,
        current_term: 7,
        voted_for: Some(2),
    };
    let expected_log = vec![Entry {
        index: 1,
        term: 6,
        command: Command::Put {
            key: "durable".into(),
            value: "yes".into(),
        },
    }];

    let (mut storage, _, _) = Storage::open(&node_dir).expect("open storage");
    storage
        .save_hard_state(&expected_hard)
        .expect("persist hard state");
    storage
        .append_entries(None, &expected_log)
        .expect("persist log");
    drop(storage);

    let (_storage, hard, log) = Storage::open(&node_dir).expect("reopen storage");
    let node = RaftNode::restore(1, vec![2, 3], 99, hard, log).expect("restore node");
    assert_eq!(node.hard, expected_hard);
    assert_eq!(node.log, expected_log);
    assert_eq!(node.commit_index, 0);
    assert_eq!(node.last_applied, 0);
    assert_eq!(NodeRole::from(&node.role), NodeRole::Follower);
}

struct RunningNode {
    id: NodeId,
    raft_port: u16,
    http_port: u16,
    shared: SharedReadState,
    raft_listener: JoinHandle<()>,
    driver: JoinHandle<std::io::Result<()>>,
    http: JoinHandle<std::io::Result<()>>,
    recovered_hard: HardState,
    recovered_log: Vec<Entry>,
    recovered_snapshot: Option<raft_core::SnapshotDescriptor>,
}

impl RunningNode {
    async fn shutdown(mut self) {
        self.raft_listener.abort();
        self.driver.abort();
        self.http.abort();
        let _ = (&mut self.raft_listener).await;
        let _ = (&mut self.driver).await;
        let _ = (&mut self.http).await;
    }
}

// Panic or an outer timeout must not leave a detached cluster running.
impl Drop for RunningNode {
    fn drop(&mut self) {
        self.raft_listener.abort();
        self.driver.abort();
        self.http.abort();
    }
}

struct StartupTasks(Vec<tokio::task::AbortHandle>);
impl Drop for StartupTasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

async fn start_node(
    id: NodeId,
    raft_ports: &[u16; 3],
    http_port: Option<u16>,
    root: &TestDir,
) -> RunningNode {
    start_node_with_snapshots(id, raft_ports, http_port, root, 0).await
}

async fn start_node_with_snapshots(
    id: NodeId,
    raft_ports: &[u16; 3],
    http_port: Option<u16>,
    root: &TestDir,
    threshold: u64,
) -> RunningNode {
    let peers: Vec<(NodeId, SocketAddr)> = (1..=3)
        .filter(|&peer| peer != id)
        .map(|peer| {
            (
                peer,
                SocketAddr::from(([127, 0, 0, 1], raft_ports[usize::from(peer - 1)])),
            )
        })
        .collect();
    let peer_ids = peers.iter().map(|(peer, _)| *peer).collect();
    let (storage, recovered) = Storage::open_recovered(root.node(id)).expect("open storage");
    let recovered_hard = recovered.hard_state.clone();
    let recovered_log = recovered.entries.clone();
    let recovered_snapshot = recovered
        .snapshot
        .as_ref()
        .map(|image| image.descriptor().clone());
    let node = RaftNode::restore_with_snapshot(
        id,
        peer_ids,
        fresh_boot_seed(),
        recovered.hard_state,
        recovered_snapshot.clone(),
        recovered.entries,
    )
    .expect("restore node");
    let shared = match &recovered.snapshot {
        Some(image) => {
            SharedReadState::from_snapshot(&node, image.clone()).expect("restore applied snapshot")
        }
        None => SharedReadState::from_node(&node),
    };
    let snapshots = SnapshotRuntime::new(root.node(id), recovered.snapshot, threshold)
        .expect("snapshot runtime");
    let (proposal_tx, proposal_rx) = tokio::sync::mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
    let (read_tx, read_rx) = tokio::sync::mpsc::channel(READ_CHANNEL_CAPACITY);
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(INBOUND_QUEUE_CAPACITY);
    let mut startup = StartupTasks(Vec::new());
    let raft_port = raft_ports[usize::from(id - 1)];
    let raft_listener = spawn_listener(raft_port, inbound_tx)
        .await
        .expect("bind raft listener");
    startup.0.push(raft_listener.abort_handle());
    let transport = TcpTransport::spawn(id, &peers);

    let http_listener = TcpListener::bind(("127.0.0.1", http_port.unwrap_or(0)))
        .await
        .expect("bind HTTP listener");
    let http_port = http_listener.local_addr().expect("HTTP address").port();
    let api = ApiState::new(shared.clone(), proposal_tx).with_reads(read_tx);
    let http = tokio::spawn(http::serve(http_listener, api));
    startup.0.push(http.abort_handle());
    let driver_shared = shared.clone();
    let driver = tokio::spawn(
        async move {
            let (_controller, shutdown) = shutdown::channel();
            run_driver_with_snapshots(
                node,
                storage,
                transport,
                inbound_rx,
                proposal_rx,
                read_rx,
                driver_shared,
                shutdown,
                snapshots,
            )
            .await
            .map(|_| ())
        }
        .instrument(tracing::info_span!("driver", id)),
    );

    startup.0.clear();
    RunningNode {
        id,
        raft_port,
        http_port,
        shared,
        raft_listener,
        driver,
        http,
        recovered_hard,
        recovered_log,
        recovered_snapshot,
    }
}

fn reserve_raft_ports() -> ([u16; 3], Vec<PortReservation>) {
    let mut reservations = Vec::new();
    let mut ports = [0; 3];
    for port in &mut ports {
        let listener = PortReservation::bind(("127.0.0.1", 0)).expect("reserve port");
        *port = listener.local_addr().expect("reserved address").port();
        reservations.push(listener);
    }
    (ports, reservations)
}

async fn wait_for_leader(nodes: &[Option<RunningNode>], within: Duration) -> usize {
    timeout(within, async {
        loop {
            let leaders: Vec<usize> = nodes
                .iter()
                .enumerate()
                .filter_map(|(index, node)| {
                    node.as_ref()
                        .filter(|node| node.shared.snapshot().status.role == NodeRole::Leader)
                        .map(|_| index)
                })
                .collect();
            if leaders.len() == 1 {
                return leaders[0];
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("leader elected before timeout")
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

async fn request(port: u16, method: &str, path: &str, body: &[u8]) -> HttpResponse {
    timeout(
        Duration::from_secs(4),
        request_inner(port, method, path, body),
    )
    .await
    .unwrap_or_else(|_| panic!("HTTP {method} {path} on port {port} exceeded4s"))
}

async fn request_inner(port: u16, method: &str, path: &str, body: &[u8]) -> HttpResponse {
    let mut stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect HTTP");
    let head = format!(
        "{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await.expect("write head");
    stream.write_all(body).await.expect("write body");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");

    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("HTTP header terminator");
    let header_text = std::str::from_utf8(&raw[..split]).expect("UTF-8 headers");
    let mut lines = header_text.split("\r\n");
    let status = lines
        .next()
        .expect("status line")
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status");
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    HttpResponse {
        status,
        headers,
        body: raw[(split + 4)..].to_vec(),
    }
}

async fn wait_for_value(port: u16, key: &str, expected: &str, within: Duration) {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(response) = timeout(
            Duration::from_millis(300),
            request(port, "GET", &format!("/kv/{key}"), &[]),
        )
        .await
        {
            if response.status == 200 && response.body == expected.as_bytes() {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "value {key:?} did not converge on HTTP port {port} in {within:?}"
        );
        sleep(Duration::from_millis(30)).await;
    }
}

async fn wait_for_missing(port: u16, key: &str, within: Duration) -> HttpResponse {
    let deadline = Instant::now() + within;
    loop {
        if let Ok(response) = timeout(
            Duration::from_millis(300),
            request(port, "GET", &format!("/kv/{key}"), &[]),
        )
        .await
        {
            if response.status == 404 {
                return response;
            }
        }
        assert!(
            Instant::now() < deadline,
            "deletion did not converge in time"
        );
        sleep(Duration::from_millis(30)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn live_driver_serves_writes_and_recovers_after_restart() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "warn".into()),
        )
        .with_test_writer()
        .try_init();
    let root = TestDir::new();
    let (raft_ports, reservations) = reserve_raft_ports();
    drop(reservations);

    let mut nodes: Vec<Option<RunningNode>> = Vec::new();
    for id in 1..=3 {
        nodes.push(Some(start_node(id, &raft_ports, None, &root).await));
    }

    let leader_index = wait_for_leader(&nodes, Duration::from_secs(5)).await;
    eprintln!("initial leader index={leader_index}");
    let leader = nodes[leader_index].as_ref().expect("leader alive");
    let status = request(leader.http_port, "GET", "/status", &[]).await;
    assert_eq!(status.status, 200);
    let status_json: serde_json::Value = serde_json::from_slice(&status.body).expect("status JSON");
    assert_eq!(status_json["node_id"], leader.id);
    assert_eq!(status_json["role"], "leader");

    let follower = nodes
        .iter()
        .flatten()
        .find(|node| node.id != leader.id)
        .expect("follower");
    let rejected = request(follower.http_port, "PUT", "/kv/rejected", b"value").await;
    assert_eq!(rejected.status, 503);
    let rejected_json: serde_json::Value =
        serde_json::from_slice(&rejected.body).expect("rejection JSON");
    assert_eq!(rejected_json["error"], "not_leader");

    let oversized = vec![b'x'; MAX_VALUE_BYTES + 1];
    let too_large = request(leader.http_port, "PUT", "/kv/large", &oversized).await;
    assert_eq!(too_large.status, 413);

    let oversized_key = format!("/kv/{}", "k".repeat(MAX_KEY_BYTES + 1));
    let key_too_large = request(leader.http_port, "PUT", &oversized_key, b"value").await;
    assert_eq!(key_too_large.status, 413);

    let written = request(leader.http_port, "PUT", "/kv/alpha", b"one").await;
    assert_eq!(written.status, 200, "body={:?}", written.body);
    let write_json: serde_json::Value =
        serde_json::from_slice(&written.body).expect("write response JSON");
    assert_eq!(write_json["ok"], true);
    assert!(write_json["index"].as_u64().is_some());

    for node in nodes.iter().flatten() {
        wait_for_value(node.http_port, "alpha", "one", Duration::from_secs(3)).await;
        let read = request(node.http_port, "GET", "/kv/alpha", &[]).await;
        assert!(read.headers.contains_key("x-raft-role"));
        assert!(read.headers.contains_key("x-raft-last-applied"));
    }

    let old = nodes[leader_index].take().expect("take old leader");
    let old_id = old.id;
    let old_http_port = old.http_port;
    let old_raft_port = old.raft_port;
    old.shutdown().await;
    assert_eq!(old_raft_port, raft_ports[leader_index]);

    let new_leader_index = wait_for_leader(&nodes, Duration::from_secs(5)).await;
    assert_ne!(new_leader_index, leader_index);

    let restarted = start_node(old_id, &raft_ports, Some(old_http_port), &root).await;
    assert!(restarted.recovered_hard.current_term >= 1);
    assert!(restarted.recovered_log.iter().any(|entry| {
        matches!(
            &entry.command,
            Command::Put { key, value } if key == "alpha" && value == "one"
        )
    }));
    let restarted_http = restarted.http_port;
    eprintln!(
        "reopened node={old_id} HTTP={restarted_http} raft={old_raft_port}; cluster={:?}",
        nodes
            .iter()
            .flatten()
            .map(|node| node.shared.status())
            .collect::<Vec<_>>()
    );
    nodes[leader_index] = Some(restarted);
    wait_for_value(restarted_http, "alpha", "one", Duration::from_secs(5)).await;

    let current_leader = wait_for_leader(&nodes, Duration::from_secs(5)).await;
    let deleted = request(
        nodes[current_leader].as_ref().unwrap().http_port,
        "DELETE",
        "/kv/alpha",
        &[],
    )
    .await;
    assert_eq!(deleted.status, 200, "body={:?}", deleted.body);
    let delete_json: serde_json::Value =
        serde_json::from_slice(&deleted.body).expect("delete response JSON");
    assert_eq!(delete_json["ok"], true);
    assert!(delete_json["index"].as_u64().is_some());

    for node in nodes.iter().flatten() {
        let missing = wait_for_missing(node.http_port, "alpha", Duration::from_secs(5)).await;
        assert!(missing.headers.contains_key("x-raft-role"));
        assert!(missing.headers.contains_key("x-raft-last-applied"));
    }

    for node in nodes.into_iter().flatten() {
        node.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn sessions_replay_unknown_writes_across_leader_change_and_wal_reopen() {
    timeout(Duration::from_secs(25), async {
        let root = TestDir::new();
        let (raft_ports, reservations) = reserve_raft_ports();
        drop(reservations);
        let mut nodes = Vec::new();
        for id in 1..=3 {
            nodes.push(Some(start_node(id, &raft_ports, None, &root).await));
        }
        let initial = wait_for_leader(&nodes, Duration::from_secs(5)).await;
        let port = nodes[initial].as_ref().unwrap().http_port;
        let registration = request(port, "POST", "/sessions", b"retained-client-nonce").await;
        assert_eq!(registration.status, 200, "{:?}", registration.body);
        let registered: serde_json::Value = serde_json::from_slice(&registration.body).unwrap();
        let session_id = registered["session_id"].as_u64().unwrap();
        let path = format!("/sessions/{session_id}/kv/protected?sequence=1");

        // Send an operation but deliberately never observe its HTTP response.
        let mut lost_response = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        let body = b"committed-with-lost-response";
        let head = format!("PUT {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
        lost_response.write_all(head.as_bytes()).await.unwrap();
        lost_response.write_all(body).await.unwrap();
        timeout(Duration::from_secs(3), async {
            loop {
                if nodes[initial].as_ref().unwrap().shared.get("protected").1.as_deref()
                    == Some("committed-with-lost-response") {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("first operation actually committed before losing leader");
        drop(lost_response);
        let old = nodes[initial].take().unwrap();
        let old_id = old.id;
        old.shutdown().await;
        let (_disk, _, log) = Storage::open(root.node(old_id)).unwrap();
        let original_index = log.iter().find_map(|entry| match &entry.command {
            Command::SessionPut { session_id: id, sequence: 1, key, .. }
                if *id == session_id && key == "protected" => Some(entry.index),
            _ => None,
        }).expect("original operation was durably logged");
        let next = wait_for_leader(&nodes, Duration::from_secs(5)).await;
        assert_ne!(next, initial);
        let next_port = nodes[next].as_ref().unwrap().http_port;
        let retry = request(next_port, "PUT", &path, body).await;
        assert_eq!(retry.status, 200, "{:?}", retry.body);
        let replay: serde_json::Value = serde_json::from_slice(&retry.body).unwrap();
        assert_eq!(replay["index"], original_index);
        assert_eq!(replay["sequence"], 1);

        // A duplicate must not repeat its mutation over a later independent write.
        assert_eq!(request(next_port, "PUT", "/kv/protected", b"later-independent-write").await.status, 200);
        let repeated = request(next_port, "PUT", &path, body).await;
        assert_eq!(repeated.body, retry.body);
        assert_eq!(request(next_port, "GET", "/kv/protected", b"").await.body, b"later-independent-write");
        let mismatch = request(next_port, "PUT", &path, b"changed-payload").await;
        assert_eq!(mismatch.status, 409);
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&mismatch.body).unwrap()["error"], "payload_mismatch");
        let gap = request(next_port, "PUT", &format!("/sessions/{session_id}/kv/protected?sequence=3"), b"gap").await;
        assert_eq!(gap.status, 409);

        // Reopen every replica from WAL; retry metadata must replay with values.
        for node in &mut nodes {
            if let Some(node) = node.take() { node.shutdown().await; }
        }
        for (offset, slot) in nodes.iter_mut().enumerate() {
            *slot = Some(start_node(offset as u8 + 1, &raft_ports, None, &root).await);
        }
        let reopened = wait_for_leader(&nodes, Duration::from_secs(5)).await;
        let reopened_port = nodes[reopened].as_ref().unwrap().http_port;
        let nonce_retry = request(reopened_port, "POST", "/sessions", b"retained-client-nonce").await;
        assert_eq!(nonce_retry.body, registration.body);
        let replay = request(reopened_port, "PUT", &path, body).await;
        assert_eq!(replay.body, retry.body);
        assert_eq!(request(reopened_port, "GET", "/kv/protected", b"").await.body, b"later-independent-write");
        let next_path = format!("/sessions/{session_id}/kv/protected?sequence=2");
        let advance = request(reopened_port, "PUT", &next_path, b"next-sequence").await;
        assert_eq!(advance.status, 200);
        for node in nodes.iter().flatten() {
            wait_for_value(node.http_port, "protected", "next-sequence", Duration::from_secs(3)).await;
        }
        let delete_path = format!("/sessions/{session_id}/kv/protected?sequence=3");
        let deleted = request(reopened_port, "DELETE", &delete_path, b"").await;
        assert_eq!(deleted.status, 200);
        for node in nodes.iter().flatten() {
            wait_for_missing(node.http_port, "protected", Duration::from_secs(3)).await;
        }
        assert_eq!(request(reopened_port, "PUT", "/kv/protected", b"after-delete").await.status, 200);
        assert_eq!(request(reopened_port, "DELETE", &delete_path, b"").await.body, deleted.body);
        assert_eq!(request(reopened_port, "GET", "/kv/protected", b"").await.body, b"after-delete");
        let close_path = format!("/sessions/{session_id}");
        let close = request(reopened_port, "DELETE", &close_path, b"").await;
        assert_eq!(close.status, 200);
        assert_eq!(request(reopened_port, "DELETE", &close_path, b"").await.body, close.body);
        assert_eq!(request(reopened_port, "PUT", &next_path, b"next-sequence").await.status, 410);
        assert_eq!(request(reopened_port, "POST", "/sessions", b"retained-client-nonce").await.body, registration.body);
        assert_eq!(request(reopened_port, "PUT", &next_path, b"next-sequence").await.status, 410);
        for node in nodes.into_iter().flatten() { node.shutdown().await; }
    }).await.expect("bounded session HTTP, failover, and restart scenario");
}

fn checked_read_metadata(response: &HttpResponse, minimum_index: u64) -> (u64, u64) {
    assert_eq!(
        response.headers.get("x-raft-read-mode").map(String::as_str),
        Some("linearizable"),
        "{response:?}"
    );
    assert_eq!(
        response.headers.get("x-raft-role").map(String::as_str),
        Some("leader"),
        "{response:?}"
    );
    let numeric = |name: &str| {
        response
            .headers
            .get(name)
            .unwrap_or_else(|| panic!("missing {name}: {response:?}"))
            .parse::<u64>()
            .unwrap_or_else(|_| panic!("invalid {name}: {response:?}"))
    };
    let index = numeric("x-raft-read-index");
    let applied = numeric("x-raft-last-applied");
    let term = numeric("x-raft-term");
    let context = numeric("x-raft-read-context");
    assert!(
        index >= minimum_index && index > 0,
        "read fence predates ACK: {response:?}"
    );
    assert!(
        applied >= index,
        "application has not reached read fence: {response:?}"
    );
    assert!(
        term > 0 && context > 0,
        "invalid authority identity: {response:?}"
    );
    (term, context)
}

fn assert_read_unavailable(response: &HttpResponse) {
    assert_eq!(
        response.status, 503,
        "checked read must require quorum: {response:?}"
    );
    let body: serde_json::Value = serde_json::from_slice(&response.body).expect("read error JSON");
    assert!(
        matches!(
            body["error"].as_str(),
            Some("not_leader" | "leadership_lost" | "read_timeout")
        ),
        "unexpected failure: {response:?}"
    );
    assert!(
        !response.headers.contains_key("x-raft-read-index"),
        "failure advertised successful read fence: {response:?}"
    );
}

async fn wait_for_checked_value(
    port: u16,
    key: &str,
    value: &str,
    within: Duration,
) -> HttpResponse {
    let deadline = Instant::now() + within;
    loop {
        let response = request(
            port,
            "GET",
            &format!("/kv/{key}?consistency=linearizable"),
            &[],
        )
        .await;
        if response.status == 200 {
            assert_eq!(
                response.body,
                value.as_bytes(),
                "checked read returned wrong committed value: {response:?}"
            );
            return response;
        }
        assert_eq!(
            response.status, 503,
            "unexpected read during election: {response:?}"
        );
        assert!(
            Instant::now() < deadline,
            "checked read never became ready on port{port}: {response:?}"
        );
        sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn checked_reads_require_quorum_and_preserve_acked_value_across_leader_failure() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("warn")
        .with_test_writer()
        .try_init();
    timeout(Duration::from_secs(25), async {
        let root = TestDir::new();
        let (raft_ports, reservations) = reserve_raft_ports();
        drop(reservations);
        let mut nodes = Vec::new();
        for id in 1..=3 { nodes.push(Some(start_node(id, &raft_ports, None, &root).await)); }
        let initial = wait_for_leader(&nodes, Duration::from_secs(5)).await;
        let port = nodes[initial].as_ref().unwrap().http_port;
        let ack = request(port, "PUT", "/kv/checked", b"durable-before-failover").await;
        assert_eq!(ack.status, 200, "{ack:?}");
        let ack_body: serde_json::Value = serde_json::from_slice(&ack.body).unwrap();
        assert_eq!(ack_body["ok"], true);
        let ack_index = ack_body["index"].as_u64().filter(|index| *index > 0).expect("positive ACK index");

        let invalid_mode = request(port, "GET", "/kv/checked?consistency=stale", &[]).await;
        assert_eq!(invalid_mode.status, 400, "invalid mode must not fall back to local: {invalid_mode:?}");
        assert!(!invalid_mode.headers.contains_key("x-raft-read-index"));
        let leader_local = request(port, "GET", "/kv/checked?consistency=local", &[]).await;
        assert_eq!(leader_local.status, 200, "{leader_local:?}");
        assert_eq!(leader_local.body, b"durable-before-failover");
        assert_eq!(leader_local.headers.get("x-raft-read-mode").map(String::as_str), Some("local"));
        assert!(!leader_local.headers.contains_key("x-raft-read-index"));

        let first = request(port, "GET", "/kv/checked?consistency=linearizable", &[]).await;
        assert_eq!(first.status, 200, "{first:?}");
        assert_eq!(first.body, b"durable-before-failover");
        let first_authority = checked_read_metadata(&first, ack_index);
        let missing = request(port, "GET", "/kv/absent?consistency=linearizable", &[]).await;
        assert_eq!(missing.status, 404, "missing checked read still needs quorum: {missing:?}");
        let missing_authority = checked_read_metadata(&missing, ack_index);
        assert_ne!(first_authority, missing_authority, "each checked read must obtain a fresh context");

        let follower = nodes.iter().enumerate().find(|(index, _)| *index != initial).unwrap().1.as_ref().unwrap();
        wait_for_value(follower.http_port, "checked", "durable-before-failover", Duration::from_secs(3)).await;
        let refused = request(follower.http_port, "GET", "/kv/checked?consistency=linearizable", &[]).await;
        assert_read_unavailable(&refused);
        let refused_body: serde_json::Value = serde_json::from_slice(&refused.body).unwrap();
        assert_eq!(refused_body["error"], "not_leader");
        let local = request(follower.http_port, "GET", "/kv/checked", &[]).await;
        assert_eq!(local.status, 200, "{local:?}");
        assert_eq!(local.body, b"durable-before-failover");
        assert_eq!(local.headers.get("x-raft-read-mode").map(String::as_str), Some("local"));
        assert!(!local.headers.contains_key("x-raft-read-index"));

        // Abort all three tasks of the elected leader: real TCP connections
        // close, and only the two remaining durable replicas can elect again.
        let old_id = nodes[initial].as_ref().unwrap().id;
        nodes[initial].take().unwrap().shutdown().await;
        let replacement = wait_for_leader(&nodes, Duration::from_secs(5)).await;
        assert_ne!(replacement, initial);
        let replacement_port = nodes[replacement].as_ref().unwrap().http_port;
        let recovered = wait_for_checked_value(replacement_port, "checked", "durable-before-failover", Duration::from_secs(5)).await;
        let new_authority = checked_read_metadata(&recovered, ack_index);
        assert!(new_authority.0 > first_authority.0, "failover must use a new term: {recovered:?}");
        eprintln!("checked-read failover old_node={old_id} new_node={} ack_index={ack_index} old_term={} new_term={} read_context={}", nodes[replacement].as_ref().unwrap().id, first_authority.0, new_authority.0, new_authority.1);

        // Remove the last peer. A request invoked after both peers stopped has
        // no fresh quorum, even if this node has not noticed the loss yet.
        for (index, slot) in nodes.iter_mut().enumerate() {
            if index != replacement { if let Some(node) = slot.take() { node.shutdown().await; } }
        }
        for key in ["checked", "absent"] {
            let unavailable = request(replacement_port, "GET", &format!("/kv/{key}?consistency=linearizable"), &[]).await;
            assert_read_unavailable(&unavailable);
            eprintln!("quorum-loss checked read key={key} status={} body={}", unavailable.status, String::from_utf8_lossy(&unavailable.body));
        }
        let local = request(replacement_port, "GET", "/kv/checked?consistency=local", &[]).await;
        assert_eq!(local.status, 200, "local state remains observable: {local:?}");
        assert_eq!(local.body, b"durable-before-failover");
        assert_eq!(local.headers.get("x-raft-read-mode").map(String::as_str), Some("local"));
        nodes[replacement].take().unwrap().shutdown().await;
    }).await.expect("bounded checked-read, actual failover, and quorum-loss scenario");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn snapshot_chunks_catch_up_a_lagging_replica_and_preserve_retries_after_restart() {
    timeout(Duration::from_secs(40), async {
        let root = TestDir::new();
        let (ports, reservations) = reserve_raft_ports(); drop(reservations);
        let mut nodes = Vec::new();
        for id in 1..=3 { nodes.push(Some(start_node_with_snapshots(id, &ports, None, &root, 8).await)); }
        let leader = wait_for_leader(&nodes, Duration::from_secs(5)).await;
        let lag = (leader + 1) % 3;
        nodes[lag].take().unwrap().shutdown().await;
        let port = nodes[leader].as_ref().unwrap().http_port;
        let registered = request(port, "POST", "/sessions", b"snapshot-client").await;
        assert_eq!(registered.status, 200, "{registered:?}");
        let session = serde_json::from_slice::<serde_json::Value>(&registered.body).unwrap()["session_id"].as_u64().unwrap();
        let put_path = format!("/sessions/{session}/kv/protected?sequence=1");
        let large = "snapshot-payload-".repeat(3000);
        let put = request(port, "PUT", &put_path, large.as_bytes()).await;
        assert_eq!(put.status, 200, "{put:?}");
        let delete_path = format!("/sessions/{session}/kv/deleted?sequence=1");
        let deleted = request(port, "DELETE", &delete_path, b"").await;
        assert_eq!(deleted.status, 200, "{deleted:?}");
        assert_eq!(request(port,"PUT","/kv/protected",b"independent-overwrite").await.status,200);
        assert_eq!(request(port,"PUT","/kv/deleted",b"reinserted").await.status,200);
        let closed_registration = request(port,"POST","/sessions",b"closed-snapshot-client").await;
        let closed_id = serde_json::from_slice::<serde_json::Value>(&closed_registration.body).unwrap()["session_id"].as_u64().unwrap();
        assert_eq!(request(port,"DELETE",&format!("/sessions/{closed_id}"),b"").await.status,200);
        for index in 0..16 {
            let reply=request(port,"PUT",&format!("/kv/workload-{index}"),b"acknowledged").await;
            assert_eq!(reply.status,200,"{reply:?}");
        }
        timeout(Duration::from_secs(3), async {
            loop {
                let status = nodes[leader].as_ref().unwrap().shared.status();
                if status.snapshot_index >= 16 && status.retained_log_entries < 8 { break; }
                sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("automatic compaction completes after the acknowledged workload");
        let leader_status = nodes[leader].as_ref().unwrap().shared.status();
        assert!(leader_status.snapshot_index >= 16, "{leader_status:?}");
        assert!(leader_status.retained_log_entries < 8, "{leader_status:?}");
        let installed_boundary = leader_status.snapshot_index;
        let lag_id = lag as u8 + 1;
        let restarted = start_node_with_snapshots(lag_id,&ports,None,&root,0).await;
        assert!(restarted.recovered_snapshot.is_none(), "lagging replica must need network snapshot");
        nodes[lag] = Some(restarted);
        timeout(Duration::from_secs(10), async {
            loop {
                let lag = nodes[lag].as_ref().unwrap();
                let status=lag.shared.status();
                if status.snapshot_index >= installed_boundary && lag.shared.get("workload-15").1.as_deref()==Some("acknowledged") { break; }
                assert!(!lag.driver.is_finished(),"lagging driver terminated during snapshot transfer");
                sleep(Duration::from_millis(20)).await;
            }
        }).await.expect("lagging follower installs snapshot and retained suffix");
        nodes[lag].take().unwrap().shutdown().await;
        let (disk,recovered)=Storage::open_recovered(root.node(lag_id)).unwrap();
        let image=recovered.snapshot.as_ref().expect("installed snapshot durable");
        assert!(image.bytes().len()>raft_core::MAX_SNAPSHOT_CHUNK_BYTES,"must exercise multiple chunks");
        assert_eq!(image.application().values().get("protected").map(String::as_str),Some("independent-overwrite"));
        let snapshot_bytes=image.bytes().len(); let boundary=image.descriptor().metadata.last_included_index;
        let suffix_entries=recovered.entries.len(); drop(disk); drop(recovered);
        assert!(!root.node(lag_id).join("log.jsonl").exists(),"legacy WAL reclaimed after selected generation sync");
        let files:Vec<_>=fs::read_dir(root.node(lag_id)).unwrap().map(|item|item.unwrap().path()).collect();
        let snapshots=files.iter().filter(|path|path.extension().is_some_and(|extension|extension=="snapshot")).count();
        let wals:Vec<_>=files.iter().filter(|path|path.extension().is_some_and(|extension|extension=="wal")).collect();
        assert_eq!(snapshots,1); assert_eq!(wals.len(),1);
        let wal_bytes=fs::metadata(wals[0]).unwrap().len();
        println!("snapshot_measurements {}",serde_json::json!({"workload":"one51000Bprotectedput,protecteddelete,closed-session,16uniquelegacywrites","snapshot_bytes":snapshot_bytes,"snapshot_index":boundary,"retained_entries":suffix_entries,"retained_wal_bytes":wal_bytes,"retained_snapshot_files":snapshots,"chunks_required":snapshot_bytes.div_ceil(raft_core::MAX_SNAPSHOT_CHUNK_BYTES)}));
        let reopened=start_node_with_snapshots(lag_id,&ports,None,&root,0).await;
        assert_eq!(reopened.recovered_snapshot.as_ref().unwrap().metadata.last_included_index,boundary);
        assert_eq!(reopened.shared.get("protected").1.as_deref(),Some("independent-overwrite"));
        nodes[lag]=Some(reopened);
        wait_for_value(nodes[lag].as_ref().unwrap().http_port,"workload-15","acknowledged",Duration::from_secs(5)).await;
        nodes[leader].take().unwrap().shutdown().await;
        let next=wait_for_leader(&nodes,Duration::from_secs(5)).await;
        let next_port=nodes[next].as_ref().unwrap().http_port;
        assert_eq!(request(next_port,"POST","/sessions",b"snapshot-client").await.body,registered.body);
        assert_eq!(request(next_port,"PUT",&put_path,large.as_bytes()).await.body,put.body);
        assert_eq!(request(next_port,"GET","/kv/protected?consistency=linearizable",b"").await.body,b"independent-overwrite");
        assert_eq!(request(next_port,"DELETE",&delete_path,b"").await.body,deleted.body);
        assert_eq!(request(next_port,"GET","/kv/deleted",b"").await.body,b"reinserted");
        assert_eq!(request(next_port,"POST","/sessions",b"closed-snapshot-client").await.body,closed_registration.body);
        assert_eq!(request(next_port,"PUT",&format!("/sessions/{closed_id}/kv/closed?sequence=1"),b"must-reject").await.status,410);
        for node in nodes.into_iter().flatten() { node.shutdown().await; }
    }).await.expect("bounded snapshot catch-up, replay and restart scenario");
}
