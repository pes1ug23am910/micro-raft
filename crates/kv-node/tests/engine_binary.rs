//! Real owned processes: persistent applied state, chunk catch-up, abrupt crash,
//! and retry recovery. These are process-crash checks, not device power-loss tests.
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
    children: [Option<Child>; 3],
    root: PathBuf,
    ports: [(u16, u16); 3],
    reservations: Vec<Option<TcpListener>>,
    backend: &'static str,
    boots: [u64; 3],
}
impl Cluster {
    fn new(backend: &'static str) -> Self {
        let root = std::env::temp_dir().join(format!(
            "micro-raft-engine-process-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let reservations: Vec<_> = (0..6)
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
            children: [None, None, None],
            root,
            ports,
            reservations,
            backend,
            boots: [0; 3],
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
            .map(|p| format!("{}@127.0.0.1:{}", p + 1, self.ports[p].0))
            .collect::<Vec<_>>()
            .join(",");
        self.children[i] = Some(
            Command::new(env!("CARGO_BIN_EXE_kv-node"))
                .args([
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
        socket.set_read_timeout(Some(Duration::from_secs(3)))?;
        socket.set_write_timeout(Some(Duration::from_secs(3)))?;
        write!(socket,"{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len())?;
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
            let candidates: Vec<_> = (0..3)
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
fn exercise(backend: &'static str) {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut cluster = Cluster::new(backend);
    for i in 0..3 {
        cluster.start(i);
    }
    let leader = cluster.leader();
    let registered = cluster.ack(leader, "POST", "/sessions", "crash-retry");
    let session = registered["session_id"].as_u64().unwrap();
    let route = format!("/sessions/{session}/kv/protected?sequence=1");
    let original = cluster.ack(leader, "PUT", &route, "original");
    cluster.ack(leader, "PUT", "/kv/protected", "independent");
    assert_eq!(cluster.ack(leader, "PUT", &route, "original"), original);
    let laggard = (leader + 1) % 3;
    cluster.crash(laggard);
    let value = "x".repeat(2400);
    let mut last = 0;
    for index in 0..48 {
        last = cluster.ack(leader, "PUT", &format!("/kv/bulk{index}"), &value)["index"]
            .as_u64()
            .unwrap();
    }
    cluster.wait_applied(leader, last, 32);
    let floor = cluster.status(leader).unwrap()["snapshot_index"]
        .as_u64()
        .unwrap();
    cluster.start(laggard);
    cluster.wait_applied(laggard, last, floor);
    // Inspect only the atomically selected manifest while the process is live;
    // opening Storage here would create a second writer/reclaimer.
    let manifest: Value = serde_json::from_slice(
        &fs::read(
            cluster
                .root
                .join(format!("n{}", laggard + 1))
                .join("CURRENT"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(
        manifest["manifest"]["snapshot"]["total_len"]
            .as_u64()
            .unwrap()
            > raft_core::MAX_SNAPSHOT_CHUNK_BYTES as u64,
        "catch-up must transfer several chunks"
    );
    for i in 0..3 {
        cluster.wait_applied(i, last, 0);
        cluster.crash(i);
    }
    for i in 0..3 {
        cluster.start(i);
    }
    let leader = cluster.leader();
    cluster.checked_value(leader, "protected", "independent");
    cluster.checked_value(leader, "bulk47", &value);
    assert_eq!(
        cluster.ack(leader, "POST", "/sessions", "crash-retry"),
        registered
    );
    assert_eq!(
        cluster.ack(leader, "PUT", &route, "original"),
        original,
        "retry response must retain its pre-crash index"
    );
    cluster.checked_value(leader, "protected", "independent");
    let status = cluster.status(leader).unwrap();
    assert_eq!(status["applied_store"]["backend"], backend);
    assert_eq!(status["applied_store"]["durable"], true);
    assert!(status["applied_store"]["applied_index"].as_u64().unwrap() >= last);
    eprintln!("backend={backend} acknowledged_boundary={last} transferred_snapshot_floor={floor} recovered_retry_index={}",original["index"]);
}
#[test]
fn lsm_chunk_catchup_and_process_crash_preserve_application_retry_state() {
    exercise("lsm");
}
#[test]
fn redb_chunk_catchup_and_process_crash_preserve_application_retry_state() {
    exercise("redb");
}
