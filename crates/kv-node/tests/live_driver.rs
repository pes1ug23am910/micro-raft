use std::collections::BTreeMap;
use std::fs;
use std::net::{SocketAddr, TcpListener as PortReservation};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use kv_node::driver::{run_driver, PROPOSAL_CHANNEL_CAPACITY};
use kv_node::http::{self, ApiState, MAX_KEY_BYTES, MAX_VALUE_BYTES};
use kv_node::kv::{NodeRole, SharedReadState};
use kv_node::storage::Storage;
use kv_node::transport::{spawn_listener, TcpTransport, INBOUND_QUEUE_CAPACITY};
use raft_core::{Command, Entry, HardState, NodeId, RaftNode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout, Instant};

static TEST_DIR_SEQ: AtomicU32 = AtomicU32::new(0);

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
}

impl RunningNode {
    async fn shutdown(self) {
        self.raft_listener.abort();
        self.driver.abort();
        self.http.abort();
        let _ = self.raft_listener.await;
        let _ = self.driver.await;
        let _ = self.http.await;
    }
}

async fn start_node(
    id: NodeId,
    raft_ports: &[u16; 3],
    http_port: Option<u16>,
    root: &TestDir,
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
    let (storage, hard, log) = Storage::open(root.node(id)).expect("open storage");
    let recovered_hard = hard.clone();
    let recovered_log = log.clone();
    let node =
        RaftNode::restore(id, peer_ids, u64::from(id) * 101, hard, log).expect("restore node");
    let shared = SharedReadState::from_node(&node);
    let (proposal_tx, proposal_rx) = tokio::sync::mpsc::channel(PROPOSAL_CHANNEL_CAPACITY);
    let (inbound_tx, inbound_rx) = tokio::sync::mpsc::channel(INBOUND_QUEUE_CAPACITY);
    let raft_port = raft_ports[usize::from(id - 1)];
    let raft_listener = spawn_listener(raft_port, inbound_tx)
        .await
        .expect("bind raft listener");
    let transport = TcpTransport::spawn(id, &peers);

    let http_listener = TcpListener::bind(("127.0.0.1", http_port.unwrap_or(0)))
        .await
        .expect("bind HTTP listener");
    let http_port = http_listener.local_addr().expect("HTTP address").port();
    let api = ApiState::new(shared.clone(), proposal_tx);
    let http = tokio::spawn(http::serve(http_listener, api));
    let driver_shared = shared.clone();
    let driver = tokio::spawn(run_driver(
        node,
        storage,
        transport,
        inbound_rx,
        proposal_rx,
        driver_shared,
    ));

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

struct HttpResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

async fn request(port: u16, method: &str, path: &str, body: &[u8]) -> HttpResponse {
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
        assert!(Instant::now() < deadline, "value did not converge in time");
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
    let root = TestDir::new();
    let (raft_ports, reservations) = reserve_raft_ports();
    drop(reservations);

    let mut nodes: Vec<Option<RunningNode>> = Vec::new();
    for id in 1..=3 {
        nodes.push(Some(start_node(id, &raft_ports, None, &root).await));
    }

    let leader_index = wait_for_leader(&nodes, Duration::from_secs(5)).await;
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
