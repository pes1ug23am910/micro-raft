//! Unix child-process shutdown checks. Local reads are only catch-up observations.
//! SIGKILL is used solely to remove quorum and to clean up owned test children.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::os::unix::process::ExitStatusExt;
use std::path::PathBuf;
use std::process::{Child, Command as ProcessCommand, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use kv_node::storage::Storage;
use raft_core::{Command, Entry, HardState};
use serde_json::Value;

static SEQUENCE: AtomicU32 = AtomicU32::new(0);
static SERIAL: Mutex<()> = Mutex::new(());
const POLL: Duration = Duration::from_millis(20);
// Five-second quorum drain, 250ms cleanup, and scheduling allowance under normal I/O.
const EXIT_BOUND: Duration = Duration::from_secs(6);
const HTTP_BOUND: Duration = Duration::from_secs(3);
const PROBE_BOUND: Duration = Duration::from_millis(300);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        for _ in 0..16 {
            let path = std::env::temp_dir().join(format!(
                "micro-raft-signals-{}-{}",
                std::process::id(),
                SEQUENCE.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => panic!("create isolated directory: {error}"),
            }
        }
        panic!("could not allocate an isolated signal-test directory");
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if thread::panicking() {
            if let Ok(entries) = fs::read_dir(&self.0) {
                for entry in entries.flatten() {
                    let path = entry.path();
                    if path.extension().is_some_and(|extension| extension == "log") {
                        if let Ok(bytes) = fs::read(&path) {
                            let tail = &bytes[bytes.len().saturating_sub(64 * 1024)..];
                            eprintln!(
                                "{} (last 64KiB):\n{}",
                                path.display(),
                                String::from_utf8_lossy(tail)
                            );
                        }
                    }
                }
            }
        }
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct NodeProcess(Option<Child>);

impl NodeProcess {
    fn wait(&mut self, within: Duration) -> ExitStatus {
        let deadline = Instant::now() + within;
        loop {
            let child = self.0.as_mut().expect("live owned child");
            if let Some(status) = child.try_wait().expect("poll child") {
                self.0.take();
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "child {} failed to exit within {within:?}",
                child.id()
            );
            thread::sleep(POLL);
        }
    }

    fn signal_and_wait(&mut self, signal: &str) -> Duration {
        let child = self.0.as_mut().expect("live owned child");
        assert!(
            child.try_wait().expect("poll before signal").is_none(),
            "child exited before signal"
        );
        let pid = child.id();
        let started = Instant::now();
        let output = ProcessCommand::new("kill")
            .args(["-s", signal, &pid.to_string()])
            .output()
            .expect("run Unix kill utility for owned child");
        assert!(
            output.status.success(),
            "signal {signal} pid={pid}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let remaining = EXIT_BOUND.saturating_sub(started.elapsed());
        let status = self.wait(remaining);
        let elapsed = started.elapsed();
        assert_eq!(
            status.code(),
            Some(0),
            "{signal} must complete the clean shutdown path: {status}"
        );
        assert!(elapsed < EXIT_BOUND, "{signal} exit took {elapsed:?}");
        eprintln!("signal={signal} pid={pid} exit={status} elapsed={elapsed:?}");
        elapsed
    }

    fn crash(&mut self) {
        self.0
            .as_mut()
            .expect("live owned child")
            .kill()
            .expect("kill owned child");
        let status = self.wait(Duration::from_secs(3));
        assert_eq!(
            status.signal(),
            Some(9),
            "quorum removal requires observed SIGKILL exit"
        );
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let deadline = Instant::now() + Duration::from_secs(3);
            let failure = loop {
                match child.try_wait() {
                    Ok(Some(_)) => break None,
                    Err(error) => break Some(format!("cleanup poll pid={}: {error}", child.id())),
                    Ok(None) if Instant::now() < deadline => thread::sleep(POLL),
                    Ok(None) => {
                        break Some(format!(
                            "cleanup could not reap pid={} after SIGKILL",
                            child.id()
                        ))
                    }
                }
            };
            if let Some(failure) = failure {
                if thread::panicking() {
                    eprintln!("{failure}");
                } else {
                    panic!("{failure}");
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Ports {
    raft: u16,
    http: u16,
}

struct Cluster {
    // Children must be stopped before the directory guard dumps/removes their logs.
    nodes: Vec<Option<NodeProcess>>,
    root: TestDir,
    ports: [Ports; 3],
    reservations: Vec<Option<TcpListener>>,
    generations: [u32; 3],
}

impl Cluster {
    fn new() -> Self {
        let reservations: Vec<_> = (0..6)
            .map(|_| {
                Some(TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve listener port"))
            })
            .collect();
        let ports = std::array::from_fn(|index| Ports {
            raft: reservations[index * 2]
                .as_ref()
                .unwrap()
                .local_addr()
                .unwrap()
                .port(),
            http: reservations[index * 2 + 1]
                .as_ref()
                .unwrap()
                .local_addr()
                .unwrap()
                .port(),
        });
        Self {
            nodes: (0..3).map(|_| None).collect(),
            root: TestDir::new(),
            ports,
            reservations,
            generations: [0; 3],
        }
    }

    fn data_dir(&self, index: usize) -> PathBuf {
        self.root.0.join(format!("node-{}", index + 1))
    }

    fn address(&self, index: usize) -> SocketAddr {
        SocketAddr::from((Ipv4Addr::LOCALHOST, self.ports[index].http))
    }

    fn start(&mut self, index: usize) {
        assert!(self.nodes[index]
            .as_ref()
            .is_none_or(|node| node.0.is_none()));
        let peers = (0..3)
            .filter(|&other| other != index)
            .map(|other| format!("{}@127.0.0.1:{}", other + 1, self.ports[other].raft))
            .collect::<Vec<_>>()
            .join(",");
        self.generations[index] += 1;
        let log = fs::File::create(self.root.0.join(format!(
            "node-{}-{}.log",
            index + 1,
            self.generations[index]
        )))
        .unwrap();
        drop(self.reservations[index * 2].take());
        drop(self.reservations[index * 2 + 1].take());
        let mut command = ProcessCommand::new(env!("CARGO_BIN_EXE_kv-node"));
        command
            .args([
                "--id",
                &(index + 1).to_string(),
                "--peers",
                &peers,
                "--data-dir",
            ])
            .arg(self.data_dir(index))
            .args([
                "--raft-port",
                &self.ports[index].raft.to_string(),
                "--http-port",
                &self.ports[index].http.to_string(),
            ])
            .env("RUST_LOG", "info")
            .env("TOKIO_WORKER_THREADS", "2")
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log));
        eprintln!("launch: {command:?}");
        self.nodes[index] = Some(NodeProcess(Some(
            command.spawn().expect("spawn actual kv-node binary"),
        )));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            self.assert_alive();
            if matches!(request(self.address(index), "GET", "/status", b"", PROBE_BOUND), Ok(response) if response.status == 200)
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "node {} HTTP endpoint did not start",
                index + 1
            );
            thread::sleep(POLL);
        }
    }

    fn start_all(&mut self) {
        for index in 0..3 {
            self.start(index);
        }
    }

    fn assert_alive(&mut self) {
        for node in self.nodes.iter_mut().flatten() {
            if let Some(child) = &mut node.0 {
                assert!(
                    child.try_wait().expect("poll running node").is_none(),
                    "node pid={} exited unexpectedly",
                    child.id()
                );
            }
        }
    }

    fn acknowledged_write(&mut self, key: &str, value: &str) -> (usize, u64, u64) {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            self.assert_alive();
            for index in 0..3 {
                if self.nodes[index]
                    .as_ref()
                    .is_none_or(|node| node.0.is_none())
                {
                    continue;
                }
                let Ok(status) = request(self.address(index), "GET", "/status", b"", PROBE_BOUND)
                else {
                    continue;
                };
                if status.status != 200 {
                    continue;
                }
                let status: Value = serde_json::from_slice(&status.body).expect("status JSON");
                if status["role"] != "leader" {
                    continue;
                }
                let response = request(
                    self.address(index),
                    "PUT",
                    &format!("/kv/{key}"),
                    value.as_bytes(),
                    HTTP_BOUND,
                )
                .expect("write response");
                let json: Value = serde_json::from_slice(&response.body).expect("write JSON");
                if response.status == 503 && json["error"] == "not_leader" {
                    continue;
                }
                assert_eq!(
                    response.status, 200,
                    "healthy write was not acknowledged: {json}"
                );
                assert_eq!(json["ok"], true);
                let applied = json["index"]
                    .as_u64()
                    .filter(|index| *index > 0)
                    .expect("positive acknowledged index");
                let term = status["term"].as_u64().expect("status term");
                eprintln!(
                    "ack node={} key={key} index={applied} term={term}",
                    index + 1
                );
                return (index, applied, term);
            }
            assert!(
                Instant::now() < deadline,
                "no leader acknowledged the healthy write"
            );
            thread::sleep(POLL);
        }
    }

    fn wait_local_value(&mut self, index: usize, key: &str, value: &str, watermark: u64) {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            self.assert_alive();
            if let Ok(response) = request(
                self.address(index),
                "GET",
                &format!("/kv/{key}"),
                b"",
                PROBE_BOUND,
            ) {
                let applied = response
                    .headers
                    .get("x-raft-last-applied")
                    .and_then(|value| value.parse::<u64>().ok());
                if response.status == 200
                    && response.body == value.as_bytes()
                    && applied.is_some_and(|index| index >= watermark)
                {
                    return;
                }
            }
            assert!(
                Instant::now() < deadline,
                "node {} did not apply {key} through {watermark}",
                index + 1
            );
            thread::sleep(POLL);
        }
    }

    fn assert_listeners_closed(&self, index: usize) {
        for port in [self.ports[index].raft, self.ports[index].http] {
            assert!(
                TcpStream::connect_timeout(
                    &SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
                    PROBE_BOUND
                )
                .is_err(),
                "listener {port} survived process exit"
            );
        }
    }
}

struct HttpResponse {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

fn connect(address: SocketAddr) -> io::Result<TcpStream> {
    let stream = TcpStream::connect_timeout(&address, PROBE_BOUND)?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    Ok(stream)
}

fn send_request(stream: &mut TcpStream, method: &str, path: &str, body: &[u8]) -> io::Result<()> {
    write!(stream, "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n", body.len())?;
    stream.write_all(body)
}

fn response(stream: &mut TcpStream, within: Duration) -> io::Result<HttpResponse> {
    let deadline = Instant::now() + within;
    let mut raw = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        // Return as soon as a complete response arrives. A later TCP reset must
        // not hide an already-observed successful acknowledgment from the oracle.
        if let Some(reply) = parse_response(&raw)? {
            return Ok(reply);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP response deadline",
            ));
        }
        stream.set_read_timeout(Some(remaining))?;
        match stream.read(&mut buffer) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "connection closed before complete HTTP response",
                ))
            }
            Ok(bytes) => raw.extend_from_slice(&buffer[..bytes]),
            Err(error) => return Err(error),
        }
        if raw.len() > 128 * 1024 {
            return Err(io::Error::other("oversized test HTTP response"));
        }
    }
}

fn parse_response(raw: &[u8]) -> io::Result<Option<HttpResponse>> {
    let Some(split) = raw.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
        return Ok(None);
    };
    let head = std::str::from_utf8(&raw[..split]).map_err(io::Error::other)?;
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .ok_or_else(|| io::Error::other("invalid HTTP status"))?;
    let headers: BTreeMap<String, String> = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.to_ascii_lowercase(), value.trim().to_owned()))
        .collect();
    let expected = headers
        .get("content-length")
        .and_then(|length| length.parse::<usize>().ok())
        .ok_or_else(|| io::Error::other("test requires an exact-length HTTP response"))?;
    if expected > 128 * 1024 {
        return Err(io::Error::other("oversized declared HTTP body"));
    }
    let body = &raw[split + 4..];
    if body.len() < expected {
        return Ok(None);
    }
    if body.len() != expected {
        return Err(io::Error::other("unexpected bytes after HTTP body"));
    }
    Ok(Some(HttpResponse {
        status,
        headers,
        body: body.to_vec(),
    }))
}

fn request(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &[u8],
    within: Duration,
) -> io::Result<HttpResponse> {
    let mut stream = connect(address)?;
    send_request(&mut stream, method, path, body)?;
    response(&mut stream, within)
}

fn wal_put_index(path: PathBuf, key: &str, value: &str) -> Option<u64> {
    let bytes = fs::read(path).ok()?;
    bytes
        .split_inclusive(|byte| *byte == b'\n')
        .filter(|line| line.ends_with(b"\n"))
        .filter_map(|line| serde_json::from_slice::<Value>(line).ok())
        .filter_map(|frame| serde_json::from_value::<Entry>(frame["entry"].clone()).ok())
        .find_map(|entry| match entry.command {
            Command::Put {
                key: found_key,
                value: found_value,
            } if found_key == key && found_value == value => Some(entry.index),
            _ => None,
        })
}

#[test]
fn sigterm_leader_exits_cleanly_and_reopens_acknowledged_state() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut cluster = Cluster::new();
    cluster.start_all();
    let (leader, acknowledged, term) = cluster.acknowledged_write("before-term", "durable");
    for index in 0..3 {
        cluster.wait_local_value(index, "before-term", "durable", acknowledged);
    }
    let before: HardState = serde_json::from_slice(
        &fs::read(cluster.data_dir(leader).join("hardstate.json"))
            .expect("read atomically published hardstate"),
    )
    .expect("hardstate JSON");
    assert!(before.current_term >= term);
    cluster.nodes[leader]
        .as_mut()
        .unwrap()
        .signal_and_wait("TERM");
    cluster.assert_listeners_closed(leader);
    // Inspect the stopped node before its peers can repair anything on restart.
    let (storage, hard, entries) =
        Storage::open(cluster.data_dir(leader)).expect("reopen stopped storage");
    assert!(hard.current_term >= before.current_term);
    if hard.current_term == before.current_term {
        assert_eq!(hard.voted_for, before.voted_for);
    }
    assert!(entries.iter().any(|entry| entry.index == acknowledged && matches!(&entry.command, Command::Put { key, value } if key == "before-term" && value == "durable")));
    drop(storage);
    let (replacement, _, _) = cluster.acknowledged_write("survivor-write", "majority");
    assert_ne!(replacement, leader);
    cluster.start(leader);
    cluster.wait_local_value(leader, "before-term", "durable", acknowledged);
    let (_, after, _) = cluster.acknowledged_write("after-restart", "working");
    cluster.wait_local_value(leader, "after-restart", "working", after);
}

#[test]
fn sigterm_without_quorum_preserves_unknown_outcome_and_bounded_exit() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut cluster = Cluster::new();
    cluster.start_all();
    let (leader, _, _) = cluster.acknowledged_write("quorum-anchor", "committed");
    for index in 0..3 {
        if index != leader {
            cluster.nodes[index].as_mut().unwrap().crash();
        }
    }
    let mut pending = connect(cluster.address(leader)).expect("connect pending write");
    send_request(
        &mut pending,
        "PUT",
        "/kv/pending-without-quorum",
        b"uncertain",
    )
    .expect("send pending write");
    let deadline = Instant::now() + Duration::from_secs(1);
    let pending_index = loop {
        if let Some(index) = wal_put_index(
            cluster.data_dir(leader).join("log.jsonl"),
            "pending-without-quorum",
            "uncertain",
        ) {
            break index;
        }
        assert!(
            Instant::now() < deadline,
            "pending request never reached the WAL before its HTTP deadline"
        );
        thread::sleep(Duration::from_millis(5));
    };
    // A complete WAL record proves the proposal reached the storage path;
    // only the later stopped-state reopen below validates the persisted frame.
    let state = request(cluster.address(leader), "GET", "/status", b"", PROBE_BOUND)
        .expect("isolated status");
    let state: Value = serde_json::from_slice(&state.body).unwrap();
    assert!(state["last_applied"].as_u64().unwrap() < pending_index);
    pending.set_nonblocking(true).unwrap();
    let mut peek = [0u8; 1];
    let before_signal = pending.peek(&mut peek);
    pending.set_nonblocking(false).unwrap();
    assert!(
        matches!(before_signal, Err(ref error) if error.kind() == io::ErrorKind::WouldBlock),
        "request must still be pending when signaled: {before_signal:?}"
    );
    eprintln!("quorum removed; pending WAL index={pending_index}; no response before SIGTERM");
    cluster.nodes[leader]
        .as_mut()
        .unwrap()
        .signal_and_wait("TERM");
    cluster.assert_listeners_closed(leader);
    match response(&mut pending, Duration::from_secs(1)) {
        Ok(reply) => {
            assert_eq!(
                reply.status, 503,
                "a quorumless pending write must never be acknowledged"
            );
            let json: Value = serde_json::from_slice(&reply.body).expect("unknown outcome JSON");
            assert_eq!(json["error"], "outcome_unknown");
            assert_ne!(json["ok"], true);
            eprintln!("pending response: {json}");
        }
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::UnexpectedEof
                    | io::ErrorKind::ConnectionReset
                    | io::ErrorKind::ConnectionAborted
            ) =>
        {
            eprintln!("pending connection closed: outcome remains unknown ({error})");
        }
        Err(error) => panic!("pending connection did not finish after child exit: {error}"),
    }
    let (_storage, _, entries) =
        Storage::open(cluster.data_dir(leader)).expect("reopen isolated node");
    assert!(entries.iter().any(|entry| entry.index == pending_index && matches!(&entry.command, Command::Put { key, value } if key == "pending-without-quorum" && value == "uncertain")));
    // The existing HTTP deadline is two seconds, shorter than the five-second
    // quorum-drain budget. It can end the waiter early; driver tests cover the
    // full drain deadline without falsely treating this duration as its proof.
}

#[test]
fn sigint_exits_with_an_incomplete_http_body() {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut cluster = Cluster::new();
    cluster.start(0);
    let mut incomplete = connect(cluster.address(0)).expect("connect partial HTTP request");
    incomplete.write_all(b"PUT /kv/incomplete-body HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/plain\r\nContent-Length: 4096\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "server did not start reading the incomplete body"
        );
        incomplete.set_read_timeout(Some(remaining)).unwrap();
        let mut byte = [0u8; 1];
        incomplete
            .read_exact(&mut byte)
            .expect("100 Continue response");
        head.push(byte[0]);
        assert!(head.len() <= 4096, "oversized interim response");
    }
    assert!(
        head.starts_with(b"HTTP/1.1 100 Continue\r\n"),
        "unexpected interim response: {}",
        String::from_utf8_lossy(&head)
    );
    incomplete.write_all(b"x").unwrap();
    // Hold this client open through shutdown; ordinary HTTP graceful waiting
    // alone would otherwise wait forever for the remaining 4095 body bytes.
    cluster.nodes[0].as_mut().unwrap().signal_and_wait("INT");
    cluster.assert_listeners_closed(0);
    let (_storage, _, entries) =
        Storage::open(cluster.data_dir(0)).expect("reopen SIGINT-stopped node");
    assert!(!entries.iter().any(
        |entry| matches!(&entry.command, Command::Put { key, .. } if key == "incomplete-body")
    ));
    drop(incomplete);
}
