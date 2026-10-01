//! Real isolated binaries exercising membership, dynamic routes and both engines.
use serde_json::Value;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

static NEXT: AtomicU64 = AtomicU64::new(0);
static SERIAL: Mutex<()> = Mutex::new(());
const POLL: Duration = Duration::from_millis(20);

struct Cluster {
    children: [Option<Child>; 5],
    root: PathBuf,
    ports: [(u16, u16); 5],
    reservations: Vec<Option<TcpListener>>,
    backend: &'static str,
    boots: [u64; 5],
    unavailable_seed: Option<usize>,
}
impl Cluster {
    fn new(backend: &'static str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "micro-raft-membership-process-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let reservations: Vec<_> = (0..10)
            .map(|_| Some(TcpListener::bind(("127.0.0.1", 0)).unwrap()))
            .collect();
        let ports = std::array::from_fn(|i| {
            (
                reservations[i * 2]
                    .as_ref()
                    .unwrap()
                    .local_addr()
                    .unwrap()
                    .port(),
                reservations[i * 2 + 1]
                    .as_ref()
                    .unwrap()
                    .local_addr()
                    .unwrap()
                    .port(),
            )
        });
        Self {
            children: std::array::from_fn(|_| None),
            root,
            ports,
            reservations,
            backend,
            boots: [0; 5],
            unavailable_seed: None,
        }
    }
    fn start(&mut self, i: usize) {
        assert!(self.children[i].is_none());
        self.reservations[i * 2].take();
        self.reservations[i * 2 + 1].take();
        self.boots[i] += 1;
        let output = fs::File::create(self.root.join(format!(
            "n{}-boot{}.log",
            i + 1,
            self.boots[i]
        )))
        .unwrap();
        let peers = (0..3)
            .filter(|&p| p != i)
            .map(|p| {
                format!(
                    "{}@{}:{}",
                    p + 1,
                    if self.unavailable_seed == Some(p) {
                        "removed-seed.invalid"
                    } else {
                        "127.0.0.1"
                    },
                    self.ports[p].0
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        self.children[i] = Some(
            Command::new(env!("CARGO_BIN_EXE_kv-node"))
                .args([
                    "--group-id",
                    "membership-process",
                    "--genesis-voters",
                    "1,2,3",
                    "--id",
                    &(i + 1).to_string(),
                    "--raft-port",
                    &self.ports[i].0.to_string(),
                    "--http-port",
                    &self.ports[i].1.to_string(),
                    "--peers",
                    &peers,
                    "--snapshot-threshold",
                    "16",
                    "--state-backend",
                    self.backend,
                    "--data-dir",
                ])
                .arg(self.root.join(format!("n{}", i + 1)))
                .stdin(Stdio::null())
                .stdout(output.try_clone().unwrap())
                .stderr(output)
                .spawn()
                .unwrap(),
        );
    }
    fn alive(&mut self) {
        for (i, child) in self.children.iter_mut().enumerate() {
            if let Some(child) = child {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "node{} exited prematurely; logs={}",
                    i + 1,
                    self.root.display()
                );
            }
        }
    }
    fn crash(&mut self, i: usize) {
        let mut child = self.children[i].take().unwrap();
        assert!(
            child.try_wait().unwrap().is_none(),
            "node must be alive before crash"
        );
        child.kill().unwrap();
        reap(&mut child).unwrap();
    }
    fn request(&self, i: usize, method: &str, path: &str, body: &str) -> io::Result<(u16, String)> {
        let address = SocketAddr::from(([127, 0, 0, 1], self.ports[i].1));
        let mut socket = TcpStream::connect_timeout(&address, Duration::from_millis(250))?;
        socket.set_read_timeout(Some(Duration::from_secs(8)))?;
        socket.set_write_timeout(Some(Duration::from_secs(8)))?;
        write!(socket,"{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len())?;
        let mut bytes = Vec::new();
        socket.take(256 * 1024).read_to_end(&mut bytes)?;
        let text = String::from_utf8(bytes).map_err(io::Error::other)?;
        let (head, body) = text
            .split_once("\r\n\r\n")
            .ok_or_else(|| io::Error::other("incomplete HTTP response"))?;
        let status = head
            .split_whitespace()
            .nth(1)
            .ok_or_else(|| io::Error::other("missing status"))?
            .parse()
            .map_err(io::Error::other)?;
        Ok((status, body.to_owned()))
    }
    fn status(&self, i: usize) -> Option<Value> {
        let (code, body) = self.request(i, "GET", "/status", "").ok()?;
        (code == 200)
            .then(|| serde_json::from_str(&body).ok())
            .flatten()
    }
    fn leader(&mut self) -> usize {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            self.alive();
            let candidates: Vec<_> = (0..5)
                .filter(|&i| {
                    self.children[i].is_some()
                        && self.status(i).is_some_and(|s| s["role"] == "leader")
                })
                .collect();
            if candidates.len() == 1 {
                let leader = candidates[0];
                // Role publication precedes the current-term no-op commitment.
                // Require a real ReadIndex fence before treating this leader as
                // ready; do not retry mutations with ambiguous outcomes.
                if self
                    .request(
                        leader,
                        "GET",
                        "/kv/readiness-probe?consistency=linearizable",
                        "",
                    )
                    .is_ok_and(|(status, _)| matches!(status, 200 | 404))
                {
                    return leader;
                }
            }
            assert!(Instant::now() < deadline, "leader election timed out");
            thread::sleep(POLL);
        }
    }
    fn ack(&self, i: usize, method: &str, path: &str, body: &str) -> Value {
        let (status, body) = self.request(i, method, path, body).unwrap();
        assert_eq!(status, 200, "{body}");
        let result: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(result["ok"], true);
        assert!(result["index"].as_u64().is_some_and(|index| index > 0));
        result
    }
    fn admin(&mut self, request_id: &str, operation: Value) -> Value {
        let deadline = Instant::now() + Duration::from_secs(25);
        let command =
            serde_json::json!({"request_id":request_id,"operation":operation}).to_string();
        loop {
            let leader = self.leader();
            match self.request(leader, "POST", "/admin/membership", &command) {
                Ok((200, body)) => {
                    let result: Value = serde_json::from_str(&body).unwrap();
                    assert_eq!(result["outcome"], "completed", "{body}");
                    assert!(result["record"]["final_index"]
                        .as_u64()
                        .is_some_and(|index| index > 0));
                    return result;
                }
                Ok((code, body)) => {
                    let result: Value = serde_json::from_str(&body).unwrap();
                    assert!(matches!(code, 409 | 503), "admin {code}: {body}");
                    assert!(
                        result["outcome"] == "unknown"
                            || matches!(
                                result["reason"].as_str(),
                                Some(
                                    "learner_not_caught_up"
                                        | "not_leader"
                                        | "current_term_not_committed"
                                )
                            ),
                        "admin cannot retry rejection {body}"
                    );
                }
                Err(_) => {} // The same durable request id/payload resolves unknown completion.
            }
            assert!(Instant::now() < deadline, "admin {request_id} timed out");
            thread::sleep(POLL);
        }
    }
    fn add(&mut self, id: usize) -> Value {
        self.admin(&format!("add-{id}"), serde_json::json!({"kind":"add_learner","id":id,
            "endpoints":{"raft":format!("127.0.0.1:{}",self.ports[id-1].0),"http":format!("127.0.0.1:{}",self.ports[id-1].1)}}))
    }
    fn wait_retired(&mut self, i: usize) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            self.alive();
            if self.status(i).is_some_and(|status| {
                status["role"] == "follower"
                    && status["committed_membership"]["state"]["retired"]
                        .as_array()
                        .is_some_and(|ids| ids.contains(&serde_json::json!(i + 1)))
            }) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "removed node{} did not learn durable removal",
                i + 1
            );
            thread::sleep(POLL);
        }
    }
    fn wait_applied(&mut self, i: usize, index: u64, snapshot_floor: u64) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            self.alive();
            if self.status(i).is_some_and(|status| {
                status["last_applied"].as_u64().unwrap_or(0) >= index
                    && status["snapshot_index"].as_u64().unwrap_or(0) >= snapshot_floor
            }) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "node{} catch-up timed out at index{index}/snapshot{snapshot_floor}",
                i + 1
            );
            thread::sleep(POLL);
        }
    }
    fn checked_value(&self, i: usize, key: &str, expected: &str) {
        let (status, body) = self
            .request(i, "GET", &format!("/kv/{key}?consistency=linearizable"), "")
            .unwrap();
        assert_eq!(status, 200, "{body}");
        assert_eq!(body, expected);
    }
}
fn reap(child: &mut Child) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other("owned child cleanup deadline exceeded"));
        }
        thread::sleep(POLL);
    }
}
impl Drop for Cluster {
    fn drop(&mut self) {
        let mut errors = Vec::new();
        for child in &mut self.children {
            if let Some(mut child) = child.take() {
                if let Err(error) = child.kill().and_then(|()| reap(&mut child)) {
                    errors.push(error.to_string());
                }
            }
        }
        if thread::panicking() || !errors.is_empty() {
            for item in fs::read_dir(&self.root).unwrap().flatten() {
                if item.path().extension().is_some_and(|x| x == "log") {
                    let bytes = fs::read(item.path()).unwrap();
                    eprintln!(
                        "{}:\n{}",
                        item.path().display(),
                        String::from_utf8_lossy(&bytes[bytes.len().saturating_sub(24 * 1024)..])
                    );
                }
            }
            eprintln!(
                "retained engine process evidence: {} cleanup_errors={errors:?}",
                self.root.display()
            );
            if !thread::panicking() {
                panic!("owned process cleanup failed");
            }
        } else {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }
}

fn membership_process_matrix(backend: &'static str) {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut cluster = Cluster::new(backend);
    for node in 0..5 {
        cluster.start(node);
    }
    let leader = cluster.leader();
    let removed_leader = leader;
    // Booting a reachable process outside genesis cannot create a voter.
    for learner in [3, 4] {
        let status = cluster.status(learner).unwrap();
        assert_eq!(status["role"], "follower");
        assert_eq!(status["commit_index"], 0);
        assert_eq!(
            status["effective_membership"]["voters"]["voters"],
            serde_json::json!([1, 2, 3])
        );
    }
    cluster.crash(3); // keep learner4 behind through its eventual admission
    let offline_old = (leader + 1) % 3;
    let surviving_old = (0..3)
        .find(|&id| id != leader && id != offline_old)
        .unwrap();
    cluster.crash(offline_old);
    let registration = cluster.ack(leader, "POST", "/sessions", "membership-session");
    let session = registration["session_id"].as_u64().unwrap();
    let retry_path = format!("/sessions/{session}/kv/protected?sequence=1");
    let original = cluster.ack(leader, "PUT", &retry_path, "before-membership");
    cluster.ack(leader, "PUT", "/kv/protected", "independent");
    let add5_early = cluster.add(5);
    cluster.wait_applied(4, add5_early["record"]["final_index"].as_u64().unwrap(), 0);
    assert_eq!(
        cluster.status(4).unwrap()["snapshot_index"],
        0,
        "learner5 must catch up through WAL before any snapshot"
    );
    let value = "x".repeat(2600);
    let mut last = 0;
    for index in 0..48 {
        last = cluster.ack(leader, "PUT", &format!("/kv/bulk{index}"), &value)["index"]
            .as_u64()
            .unwrap();
    }
    cluster.wait_applied(leader, last, 32);
    let snapshot_floor = cluster.status(leader).unwrap()["snapshot_index"]
        .as_u64()
        .unwrap();
    let add4 = cluster.add(4);
    let too_early=serde_json::json!({"request_id":"behind-promotion","operation":{"kind":"set_voters","voters":[1,2,3,4]}}).to_string();
    let (code, body) = cluster
        .request(leader, "POST", "/admin/membership", &too_early)
        .unwrap();
    assert_eq!(code, 409, "{body}");
    assert!(body.contains("learner_not_caught_up"));
    cluster.start(3);
    cluster.wait_applied(
        3,
        add4["record"]["final_index"].as_u64().unwrap(),
        snapshot_floor,
    );
    let add5 = cluster.add(5);
    assert_eq!(
        add5, add5_early,
        "exact admission retry keeps the original record"
    );
    // Learner5 is the WAL-catch-up path. Local snapshot schedules can differ:
    // a busy incoming transfer may defer its optional capture until another
    // threshold of entries. Require the actual acknowledged application fence,
    // while learner4 alone must prove the lagging multi-chunk snapshot path.
    cluster.wait_applied(
        4,
        last.max(add4["record"]["final_index"].as_u64().unwrap()),
        0,
    );
    {
        let learner = 3;
        let manifest: Value = serde_json::from_slice(
            &fs::read(
                cluster
                    .root
                    .join(format!("n{}", learner + 1))
                    .join("CURRENT"),
            )
            .unwrap(),
        )
        .unwrap();
        assert!(
            manifest["manifest"]["snapshot"]["total_len"]
                .as_u64()
                .unwrap()
                > raft_core::MAX_SNAPSHOT_CHUNK_BYTES as u64
        );
        assert!(manifest["manifest"]["snapshot"]["metadata"]["membership"].is_object());
    }
    let voters = vec![surviving_old + 1, 4, 5];
    let operation = serde_json::json!({"kind":"set_voters","voters":voters});
    let final_record = cluster.admin("replace-old-majority", operation.clone());
    cluster.wait_retired(leader);
    let final_index = final_record["record"]["final_index"].as_u64().unwrap();
    for node in [surviving_old, 3, 4] {
        cluster.wait_applied(node, final_index, 0);
    }
    // Force the next leader to be a formerly unregistered process. Its group-
    // bound advertisement must let the offline old voter learn its removal.
    cluster.crash(surviving_old);
    let new_leader = cluster.leader();
    assert!([3, 4].contains(&new_leader));
    cluster.unavailable_seed = Some(removed_leader);
    cluster.start(offline_old);
    cluster.wait_retired(offline_old);
    cluster.start(surviving_old);
    cluster.wait_applied(surviving_old, final_index, 0);
    for node in 0..5 {
        cluster.crash(node);
    }
    for node in 0..5 {
        cluster.start(node);
    }
    let leader = cluster.leader();
    assert!(voters.contains(&(leader + 1)));
    cluster.checked_value(leader, "protected", "independent");
    cluster.checked_value(leader, "bulk47", &value);
    assert_eq!(
        cluster.admin("replace-old-majority", operation),
        final_record
    );
    assert_eq!(
        cluster.ack(leader, "POST", "/sessions", "membership-session"),
        registration
    );
    assert_eq!(
        cluster.ack(leader, "PUT", &retry_path, "before-membership"),
        original
    );
    cluster.checked_value(leader, "protected", "independent");
    for removed in (0..3).filter(|&id| id != surviving_old) {
        cluster.wait_retired(removed);
    }
    let changed=serde_json::json!({"request_id":"replace-old-majority","operation":{"kind":"remove","id":4}}).to_string();
    let (code, body) = cluster
        .request(leader, "POST", "/admin/membership", &changed)
        .unwrap();
    assert_eq!(code, 409, "{body}");
    assert!(body.contains("request_payload_changed"));
    eprintln!("backend={backend} removed_leader={} offline_old={} new_voters={voters:?} committed_admin_index={final_index} snapshot_floor={snapshot_floor}",removed_leader+1,offline_old+1);
}
#[test]
fn membership_lsm_processes_cover_passive_join_snapshot_leader_removal_and_reopen() {
    membership_process_matrix("lsm");
}
#[test]
fn membership_redb_processes_cover_passive_join_snapshot_leader_removal_and_reopen() {
    membership_process_matrix("redb");
}

#[test]
fn real_group_frames_reject_cross_wiring_and_wal_proven_unknown_admin_retries_safely() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut cluster = Cluster::new("memory");
    for node in 0..3 {
        cluster.start(node);
    }
    let leader = cluster.leader();
    let sender = ((leader + 1) % 3 + 1) as u8;
    let forged = raft_core::RaftMessage::RequestVote {
        term: 10000,
        candidate_id: sender,
        last_log_index: 10000,
        last_log_term: 10000,
    };
    let scope = kv_node::transport::TransportScope::new("membership-process".into(), vec![1, 2, 3])
        .unwrap();
    let wrong_scope =
        kv_node::transport::TransportScope::new("unrelated-group".into(), vec![1, 2, 3]).unwrap();
    let wrong_genesis_scope =
        kv_node::transport::TransportScope::new("membership-process".into(), vec![1, 2, 4])
            .unwrap();
    let envelope = |group: &str, genesis_voters: Vec<u8>| raft_core::RaftMessage::GroupMessage {
        group_id: group.into(),
        genesis_voters,
        message: Box::new(forged.clone()),
    };
    let frames = [
        kv_node::transport::encode_scoped_frame(
            sender,
            &envelope("unrelated-group", vec![1, 2, 3]),
            &scope,
        )
        .unwrap(),
        kv_node::transport::encode_scoped_frame(
            sender,
            &envelope("membership-process", vec![1, 2, 3]),
            &wrong_scope,
        )
        .unwrap(),
        kv_node::transport::encode_scoped_frame(
            sender,
            &envelope("membership-process", vec![1, 2, 4]),
            &scope,
        )
        .unwrap(),
        kv_node::transport::encode_scoped_frame(
            sender,
            &envelope("membership-process", vec![1, 2, 3]),
            &wrong_genesis_scope,
        )
        .unwrap(),
        kv_node::transport::encode_scoped_frame(sender, &forged, &scope).unwrap(),
        kv_node::transport::encode_frame(sender, &envelope("membership-process", vec![1, 2, 3]))
            .unwrap(),
    ];
    for frame in frames {
        let address = SocketAddr::from(([127, 0, 0, 1], cluster.ports[leader].0));
        let mut socket = TcpStream::connect_timeout(&address, Duration::from_millis(250)).unwrap();
        socket
            .set_write_timeout(Some(Duration::from_secs(1)))
            .unwrap();
        socket.write_all(&frame).unwrap();
    }
    thread::sleep(Duration::from_millis(150));
    assert!(cluster.status(leader).unwrap()["term"].as_u64().unwrap() < 10000);
    let mut accepted = None;
    for trial in 0..5 {
        let leader = cluster.leader();
        let followers: Vec<_> = (0..3).filter(|&node| node != leader).collect();
        for &node in &followers {
            cluster.crash(node);
        }
        let request_id = format!("quorum-loss-{trial}");
        let operation = serde_json::json!({"kind":"add_learner","id":4,
            "endpoints":{"raft":format!("127.0.0.1:{}",cluster.ports[3].0),"http":format!("127.0.0.1:{}",cluster.ports[3].1)}});
        let command =
            serde_json::json!({"request_id":request_id,"operation":operation}).to_string();
        let started = Instant::now();
        let result = cluster.request(leader, "POST", "/admin/membership", &command);
        assert!(started.elapsed() < Duration::from_secs(9));
        assert!(
            !result.as_ref().is_ok_and(|(code, _)| *code == 200),
            "lost quorum must not ACK administration"
        );
        let wal = fs::read_to_string(
            cluster
                .root
                .join(format!("n{}", leader + 1))
                .join("log.jsonl"),
        )
        .unwrap();
        let admitted = wal.lines().any(|line| line.contains(&request_id));
        for &node in &followers {
            cluster.start(node);
        }
        if admitted {
            if let Ok((code, body)) = &result {
                assert_eq!(*code, 503, "{body}");
                assert_eq!(
                    serde_json::from_str::<Value>(body).unwrap()["outcome"],
                    "unknown"
                );
            }
            accepted = Some((request_id, operation));
            break;
        }
        // CheckQuorum may expire before admission on an OS scheduling boundary;
        // retain the rejected trial and require a later actually durable attempt.
        eprintln!("trial={trial} lost leader before admission; response={result:?}");
    }
    let (request_id, operation) =
        accepted.expect("test must reach a WAL-proven accepted pending administration");
    let completed = cluster.admin(&request_id, operation.clone());
    assert_eq!(cluster.admin(&request_id, operation), completed);
    let leader = cluster.leader();
    assert!(
        cluster.status(leader).unwrap()["committed_membership"]["state"]["learners"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!(4))
    );
    eprintln!(
        "WAL-proven unknown request={request_id} completed_index={}",
        completed["record"]["final_index"]
    );
}
