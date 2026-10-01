//! Interrupt real membership entries at TCP boundaries, then reopen their WALs.
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

static NEXT: AtomicU64 = AtomicU64::new(0);
static SERIAL: Mutex<()> = Mutex::new(());
const REQUEST_ID: &str = "interrupted-voter-replacement";
const POLL: Duration = Duration::from_millis(20);
const FRAME_LIMIT: usize = 8 * 1024 * 1024;
const TRACE_LIMIT: usize = 50_000;

#[derive(Clone, Copy, Debug, PartialEq)]
enum Phase {
    Joint,
    Final,
}
impl Phase {
    fn text(self) -> &'static str {
        match self {
            Self::Joint => "joint",
            Self::Final => "final",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum Mode {
    Open,
    Armed,
    Held,
    NewPair,
    NewJoint,
}
struct Gate {
    started: Instant,
    mode: Mode,
    phase: Phase,
    joint_all: bool,
    leader: u64,
    partner: u64,
    new_voters: Vec<u64>,
    joint: Option<u64>,
    joint_term: Option<u64>,
    boundary: Option<u64>,
    boundary_term: Option<u64>,
    joint_durable: BTreeSet<u64>,
    boundary_durable: BTreeSet<u64>,
    trace: Vec<Value>,
    errors: Vec<String>,
}
impl Gate {
    fn new(phase: Phase) -> Self {
        Self {
            started: Instant::now(),
            mode: Mode::Open,
            phase,
            joint_all: false,
            leader: 0,
            partner: 0,
            new_voters: vec![],
            joint: None,
            joint_term: None,
            boundary: None,
            boundary_term: None,
            joint_durable: BTreeSet::new(),
            boundary_durable: BTreeSet::new(),
            trace: vec![],
            errors: vec![],
        }
    }
    fn record(&mut self, item: Value) {
        if self.trace.len() < TRACE_LIMIT {
            self.trace.push(item);
        } else if self.errors.is_empty() {
            self.errors.push("bounded gate trace exhausted".into());
        }
    }
    fn error(&mut self, error: String) {
        if self.errors.len() < 16 {
            self.errors.push(error);
        }
    }
    fn decide(&mut self, to: u64, bytes: &[u8]) -> bool {
        let frame: Value = match serde_json::from_slice(bytes) {
            Ok(value) => value,
            Err(error) => {
                self.error(format!("gate JSON: {error}"));
                return false;
            }
        };
        let Some(from) = frame["from"].as_u64() else {
            self.error("missing sender in scoped frame".into());
            return false;
        };
        let group = &frame["msg"]["GroupMessage"];
        if frame["scope"]["group_id"] != "membership-gates"
            || frame["scope"]["genesis_voters"] != json!([1, 2, 3])
            || group["group_id"] != "membership-gates"
            || group["genesis_voters"] != json!([1, 2, 3])
        {
            self.error("unexpected frame scope".into());
            return false;
        }
        let Some(message) = group["message"].as_object() else {
            self.error("missing group message".into());
            return false;
        };
        let Some((kind, body)) = message.iter().next() else {
            self.error("empty group message".into());
            return false;
        };
        let mut configurations = vec![];
        if kind == "AppendEntries" {
            for entry in body["entries"].as_array().into_iter().flatten() {
                let config = &entry["command"]["Configuration"];
                if config["request_id"] != REQUEST_ID {
                    continue;
                }
                let Some(index) = entry["index"].as_u64() else {
                    self.error("configuration has no log index".into());
                    return false;
                };
                let phase = config["phase"].as_str().unwrap_or("");
                let Some(term) = entry["term"].as_u64() else {
                    self.error("configuration has no term".into());
                    return false;
                };
                configurations.push(json!({"phase":phase,"index":index,"term":term}));
                if phase == "joint" {
                    if self.joint.is_none() {
                        self.joint = Some(index);
                        self.joint_term = Some(term);
                    } else if matches!(self.mode, Mode::Armed | Mode::Held)
                        && (self.joint != Some(index) || self.joint_term != Some(term))
                    {
                        self.error("armed joint entry was replaced".into());
                        return false;
                    }
                }
                // Serialize the switch with the decision for every connection.
                // No final/joint frame can escape the selected boundary first.
                if self.mode == Mode::Armed && phase == self.phase.text() {
                    if from != self.leader {
                        self.error("leader changed before the armed boundary".into());
                        return false;
                    }
                    self.boundary = Some(index);
                    self.boundary_term = Some(term);
                    self.mode = Mode::Held;
                }
            }
        }
        let matched = if kind == "AppendEntriesReply" && body["success"] == true {
            body["match_index"].as_u64()
        } else {
            None
        };
        if to == self.leader {
            if matched
                .zip(self.joint)
                .is_some_and(|(seen, index)| seen >= index)
                && body["term"].as_u64() == self.joint_term
            {
                self.joint_durable.insert(from);
            }
            if matched
                .zip(self.boundary)
                .is_some_and(|(seen, index)| seen >= index)
                && body["term"].as_u64() == self.boundary_term
            {
                self.boundary_durable.insert(from);
            }
        }
        let mut allowed = match self.mode {
            Mode::Open | Mode::Armed => true,
            Mode::Held => {
                if self.joint_all {
                    from == self.leader
                } else {
                    ([self.leader, self.partner].contains(&from)
                        && [self.leader, self.partner].contains(&to))
                        || (self.phase == Phase::Final && from == self.leader && to == 4)
                }
            }
            Mode::NewPair => [4, 5].contains(&from) && [4, 5].contains(&to),
            Mode::NewJoint => self.new_voters.contains(&from) && self.new_voters.contains(&to),
        };
        // For the final-entry case ensure all future voters durably received
        // joint history before permitting the joint quorum to acknowledge it.
        // This makes node5's exact pre-final state an observed boundary.
        if self.mode == Mode::Armed
            && self.phase == Phase::Final
            && to == self.leader
            && self.new_voters.contains(&from)
            && matched
                .zip(self.joint)
                .is_some_and(|(seen, index)| seen >= index)
            && !self
                .new_voters
                .iter()
                .all(|id| self.joint_durable.contains(id))
        {
            allowed = false;
        }
        if !self.errors.is_empty() {
            allowed = false;
        }
        self.record(json!({"elapsed_us":self.started.elapsed().as_micros(),
            "from":from,"to":to,"kind":kind,"term":body["term"],
            "configs":configurations,"successful_match":matched,
            "mode":format!("{:?}",self.mode),"allowed":allowed}));
        allowed
    }
}

async fn proxy_connection(
    mut incoming: tokio::net::TcpStream,
    backend: SocketAddr,
    to: u64,
    gate: Arc<Mutex<Gate>>,
) {
    let mut outgoing: Option<tokio::net::TcpStream> = None;
    loop {
        let mut size = [0; 4];
        if incoming.read_exact(&mut size).await.is_err() {
            return;
        }
        let length = u32::from_be_bytes(size) as usize;
        if length == 0 || length > FRAME_LIMIT {
            gate.lock()
                .unwrap()
                .error(format!("invalid frame length {length}"));
            return;
        }
        let mut bytes = vec![0; length];
        if incoming.read_exact(&mut bytes).await.is_err() {
            return;
        }
        let allowed = gate.lock().unwrap().decide(to, &bytes);
        if !allowed {
            continue;
        }
        if outgoing.is_none() {
            match tokio::time::timeout(
                Duration::from_secs(1),
                tokio::net::TcpStream::connect(backend),
            )
            .await
            {
                Ok(Ok(stream)) => outgoing = Some(stream),
                _ => return, // A crashed backend is an intended network failure.
            }
        }
        let stream = outgoing.as_mut().unwrap();
        if !tokio::time::timeout(Duration::from_secs(1), async {
            stream.write_all(&size).await?;
            stream.write_all(&bytes).await
        })
        .await
        .is_ok_and(|result| result.is_ok())
        {
            return;
        }
    }
}

struct Cluster {
    children: [Option<Child>; 5],
    root: PathBuf,
    retain: bool,
    host: Ipv4Addr,
    ports: [(u16, u16, u16); 5], // Backend Raft, gated Raft, HTTP.
    reservations: Vec<Option<TcpListener>>,
    boots: [u64; 5],
    runtime: Option<tokio::runtime::Runtime>,
    gate: Arc<Mutex<Gate>>,
}
impl Cluster {
    fn new(phase: Phase) -> Self {
        let retain = std::env::var_os("MICRO_RAFT_MEMBERSHIP_FAULT_DIR").is_some();
        let parent = std::env::var_os("MICRO_RAFT_MEMBERSHIP_FAULT_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let root = parent.join(format!(
            "membership-gate-{}-{}-{}",
            phase.text(),
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        // UDP connect only asks the local routing table; it sends no packet.
        // Explicit advertisements require an assigned, nonloopback address.
        let lookup = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap();
        lookup.connect((Ipv4Addr::new(192, 0, 2, 1), 9)).unwrap();
        let std::net::IpAddr::V4(host) = lookup.local_addr().unwrap().ip() else {
            panic!("process gate requires a routable IPv4 adapter");
        };
        assert!(!host.is_loopback() && !host.is_unspecified());
        let mut reservations: Vec<_> = (0..15)
            .map(|_| Some(TcpListener::bind((Ipv4Addr::UNSPECIFIED, 0)).unwrap()))
            .collect();
        let ports = std::array::from_fn(|i| {
            (
                reservations[3 * i]
                    .as_ref()
                    .unwrap()
                    .local_addr()
                    .unwrap()
                    .port(),
                reservations[3 * i + 1]
                    .as_ref()
                    .unwrap()
                    .local_addr()
                    .unwrap()
                    .port(),
                reservations[3 * i + 2]
                    .as_ref()
                    .unwrap()
                    .local_addr()
                    .unwrap()
                    .port(),
            )
        });
        let gate = Arc::new(Mutex::new(Gate::new(phase)));
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        for i in 0..5 {
            let listener = reservations[3 * i + 1].take().unwrap();
            listener.set_nonblocking(true).unwrap();
            let gate = gate.clone();
            let backend = SocketAddr::from((Ipv4Addr::LOCALHOST, ports[i].0));
            runtime.spawn(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let permits = Arc::new(tokio::sync::Semaphore::new(64));
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let Ok(permit) = permits.clone().try_acquire_owned() else {
                        gate.lock()
                            .unwrap()
                            .error("proxy connection bound exceeded".into());
                        continue;
                    };
                    let gate = gate.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        proxy_connection(stream, backend, (i + 1) as u64, gate).await;
                    });
                }
            });
        }
        fs::write(
            root.join("fixture.json"),
            serde_json::to_vec_pretty(&json!({
                "phase":phase.text(),"host":host.to_string(),"ports":ports,
                "binary":env!("CARGO_BIN_EXE_kv-node"),"snapshots":false,
                "genesis_voters":[1,2,3],"backend":"memory"
            }))
            .unwrap(),
        )
        .unwrap();
        Self {
            children: std::array::from_fn(|_| None),
            root,
            retain,
            host,
            ports,
            reservations,
            boots: [0; 5],
            runtime: Some(runtime),
            gate,
        }
    }
    fn start(&mut self, i: usize) {
        assert!(self.children[i].is_none());
        self.reservations[3 * i].take();
        self.reservations[3 * i + 2].take();
        self.boots[i] += 1;
        let log = fs::File::create(
            self.root
                .join(format!("n{}-boot{}.log", i + 1, self.boots[i])),
        )
        .unwrap();
        let peers = (0..3)
            .filter(|&p| p != i)
            .map(|p| format!("{}@{}:{}", p + 1, self.host, self.ports[p].1))
            .collect::<Vec<_>>()
            .join(",");
        self.children[i] = Some(
            Command::new(env!("CARGO_BIN_EXE_kv-node"))
                .args([
                    "--group-id",
                    "membership-gates",
                    "--genesis-voters",
                    "1,2,3",
                    "--id",
                    &(i + 1).to_string(),
                    "--raft-listen",
                    &format!("127.0.0.1:{}", self.ports[i].0),
                    "--raft-advertise",
                    &format!("{}:{}", self.host, self.ports[i].1),
                    "--http-listen",
                    &format!("0.0.0.0:{}", self.ports[i].2),
                    "--http-advertise",
                    &format!("{}:{}", self.host, self.ports[i].2),
                    "--peers",
                    &peers,
                    "--snapshot-threshold",
                    "0",
                    "--data-dir",
                ])
                .arg(self.root.join(format!("n{}", i + 1)))
                .stdin(Stdio::null())
                .stdout(log.try_clone().unwrap())
                .stderr(log)
                .spawn()
                .unwrap(),
        );
    }
    fn check(&mut self) {
        for (i, child) in self.children.iter_mut().enumerate() {
            if let Some(child) = child {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "node{} exited; logs={}",
                    i + 1,
                    self.root.display()
                );
            }
        }
        let gate = self.gate.lock().unwrap();
        assert!(gate.errors.is_empty(), "gate errors: {:?}", gate.errors);
    }
    fn crash(&mut self, i: usize) {
        let mut child = self.children[i].take().unwrap();
        assert!(child.try_wait().unwrap().is_none());
        child.kill().unwrap();
        reap(&mut child).unwrap();
    }
    fn request(&self, i: usize, method: &str, path: &str, body: &str) -> io::Result<(u16, String)> {
        request(self.ports[i].2, method, path, body)
    }
    fn status(&self, i: usize) -> Option<Value> {
        let (code, body) = self.request(i, "GET", "/status", "").ok()?;
        (code == 200)
            .then(|| serde_json::from_str(&body).ok())
            .flatten()
    }
    fn wait(&mut self, label: &str, mut ready: impl FnMut(&mut Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            self.check();
            if ready(self) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "{label} timed out; {}",
                self.root.display()
            );
            thread::sleep(POLL);
        }
    }
    fn leader(&mut self) -> usize {
        let mut found = None;
        self.wait("checked leader readiness", |c| {
            for i in 0..5 {
                if c.children[i].is_some()
                    && c.status(i).is_some_and(|s| s["role"] == "leader")
                    && c.request(i, "GET", "/kv/readiness-probe?consistency=linearizable", "")
                        .is_ok_and(|(code, _)| matches!(code, 200 | 404))
                {
                    found = Some(i);
                    return true;
                }
            }
            false
        });
        found.unwrap()
    }
    fn admin(&mut self, id: &str, operation: Value) -> Value {
        let command = json!({"request_id":id,"operation":operation}).to_string();
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let leader = self.leader();
            if let Ok((code, body)) = self.request(leader, "POST", "/admin/membership", &command) {
                let value: Value = serde_json::from_str(&body).unwrap();
                if code == 200 {
                    assert_eq!(value["outcome"], "completed");
                    assert!(value["record"]["final_index"].as_u64().is_some());
                    return value;
                }
                assert!(matches!(code, 409 | 503), "admin {code}: {body}");
                assert!(
                    value["outcome"] == "unknown"
                        || matches!(
                            value["reason"].as_str(),
                            Some(
                                "learner_not_caught_up"
                                    | "not_leader"
                                    | "current_term_not_committed"
                                    | "stale_endpoint_validation"
                            )
                        ),
                    "unexpected admin rejection: {body}"
                );
            }
            assert!(Instant::now() < deadline, "exact admin retry timed out");
            thread::sleep(POLL);
        }
    }
    fn ack(&self, i: usize, key: &str, value: &str) -> u64 {
        let (code, body) = self
            .request(i, "PUT", &format!("/kv/{key}"), value)
            .unwrap();
        assert_eq!(code, 200, "{body}");
        let value: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["ok"], true);
        value["index"].as_u64().unwrap()
    }
    fn checked(&self, i: usize, key: &str, expected: &str) {
        let (code, body) = self
            .request(i, "GET", &format!("/kv/{key}?consistency=linearizable"), "")
            .unwrap();
        assert_eq!(code, 200, "{body}");
        assert_eq!(body, expected);
    }
    fn checkpoint(&self, name: &str) {
        let statuses: Vec<_> = (0..5)
            .map(|i| json!({"node":i+1,"status":self.status(i)}))
            .collect();
        fs::write(
            self.root.join(format!("{name}.json")),
            serde_json::to_vec_pretty(&statuses).unwrap(),
        )
        .unwrap();
    }
}
fn request(port: u16, method: &str, path: &str, body: &str) -> io::Result<(u16, String)> {
    let mut socket = TcpStream::connect_timeout(
        &SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        Duration::from_millis(250),
    )?;
    socket.set_read_timeout(Some(Duration::from_secs(8)))?;
    socket.set_write_timeout(Some(Duration::from_secs(8)))?;
    write!(socket,"{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len())?;
    let mut bytes = vec![];
    socket.take(256 * 1024).read_to_end(&mut bytes)?;
    let text = String::from_utf8(bytes).map_err(io::Error::other)?;
    let (header, body) = text
        .split_once("\r\n\r\n")
        .ok_or_else(|| io::Error::other("incomplete HTTP header"))?;
    let code = header
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| io::Error::other("missing status"))?
        .parse()
        .map_err(io::Error::other)?;
    let declared = header
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find(|(key, _)| key.eq_ignore_ascii_case("content-length"))
        .ok_or_else(|| io::Error::other("missing content length"))?
        .1
        .trim()
        .parse::<usize>()
        .map_err(io::Error::other)?;
    if declared != body.len() {
        return Err(io::Error::other("incomplete HTTP body"));
    }
    Ok((code, body.into()))
}
struct Pending(Option<thread::JoinHandle<io::Result<(u16, String)>>>);
impl Pending {
    fn start(port: u16, command: String) -> Self {
        Self(Some(thread::spawn(move || {
            request(port, "POST", "/admin/membership", &command)
        })))
    }
    fn finish(&mut self) -> io::Result<(u16, String)> {
        self.0.take().unwrap().join().unwrap()
    }
}
impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(task) = self.0.take() {
            let _ = task.join();
        }
    }
}
fn reap(child: &mut Child) -> io::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        if child.try_wait()?.is_some() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(io::Error::other("child cleanup timed out"));
        }
        thread::sleep(POLL);
    }
}
fn stop_owned_child(child: &mut Child) -> io::Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }
    let killed = child.kill();
    // Always reap, including the race where the child exits before kill.
    reap(child).map_err(|error| io::Error::other(format!("kill={killed:?}; reap={error}")))
}
impl Drop for Cluster {
    fn drop(&mut self) {
        let mut errors = vec![];
        for child in &mut self.children {
            if let Some(mut child) = child.take() {
                if let Err(error) = stop_owned_child(&mut child) {
                    errors.push(error.to_string());
                }
            }
        }
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_timeout(Duration::from_secs(2));
        }
        let gate = self.gate.lock().unwrap();
        let trace = json!({"events":gate.trace,"errors":gate.errors,"cleanup_errors":errors,
            "joint_index":gate.joint,"joint_term":gate.joint_term,
            "boundary_index":gate.boundary,"boundary_term":gate.boundary_term,
            "joint_durable":gate.joint_durable,"boundary_durable":gate.boundary_durable,
            "assertions_passed":!thread::panicking() && errors.is_empty() && gate.errors.is_empty()});
        fs::write(
            self.root.join("gate-trace.json"),
            serde_json::to_vec_pretty(&trace).unwrap(),
        )
        .unwrap();
        if self.retain || thread::panicking() || !errors.is_empty() || !gate.errors.is_empty() {
            eprintln!("retained membership gate evidence: {}", self.root.display());
        } else {
            fs::remove_dir_all(&self.root).unwrap();
        }
        if !thread::panicking() {
            assert!(errors.is_empty(), "cleanup: {errors:?}");
            assert!(
                gate.errors.is_empty(),
                "late gate errors: {:?}",
                gate.errors
            );
        }
    }
}

fn interrupt(phase: Phase, joint_all: bool) {
    let _serial = SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let mut c = Cluster::new(phase);
    for i in 0..5 {
        c.start(i);
    }
    for id in [4, 5] {
        c.admin(
            &format!("add-{id}"),
            json!({"kind":"add_learner","id":id,
            "endpoints":{"raft":format!("{}:{}",c.host,c.ports[id-1].1),
                "http":format!("{}:{}",c.host,c.ports[id-1].2)}}),
        );
    }
    let leader = c.leader();
    assert!(leader < 3, "only genesis members can be initial leader");
    let partner = (leader + 1) % 3;
    let survivor = (0..3).find(|id| *id != leader && *id != partner).unwrap();
    let new_voters = vec![(survivor + 1) as u64, 4, 5];
    let anchor = c.ack(leader, "anchor", "acknowledged-before-membership");
    c.wait("all replicas applied anchor", |c| {
        (0..5).all(|i| {
            c.status(i)
                .is_some_and(|s| s["last_applied"].as_u64().unwrap_or(0) >= anchor)
        })
    });
    c.checkpoint("before-boundary");
    {
        let mut gate = c.gate.lock().unwrap();
        gate.leader = (leader + 1) as u64;
        gate.partner = (partner + 1) as u64;
        gate.new_voters = new_voters.clone();
        gate.joint_all = joint_all;
        gate.mode = Mode::Armed;
    }
    let operation = json!({"kind":"set_voters","voters":new_voters});
    let mut pending = Pending::start(
        c.ports[leader].2,
        json!({"request_id":REQUEST_ID,"operation":operation}).to_string(),
    );
    c.wait("durable interrupted configuration replies", |c| {
        let gate = c.gate.lock().unwrap();
        gate.boundary.is_some()
            && gate.boundary_durable.contains(&((partner + 1) as u64))
            && (phase == Phase::Joint || gate.boundary_durable.contains(&4))
            && (!joint_all
                || (1..=5)
                    .filter(|&id| id != gate.leader)
                    .all(|id| gate.boundary_durable.contains(&id)))
    });
    let boundary = c.gate.lock().unwrap().boundary.unwrap();
    c.checkpoint("held-boundary");
    for i in [leader, partner] {
        let status = c.status(i).unwrap();
        assert!(status["commit_index"].as_u64().unwrap() < boundary);
        assert_eq!(
            status["effective_membership"]["voters"]["phase"],
            if phase == Phase::Joint {
                "joint"
            } else {
                "stable"
            }
        );
        assert!(
            status["committed_membership"]["state"]["records"][REQUEST_ID]["final_index"].is_null()
        );
    }
    let result = pending.finish();
    fs::write(
        c.root.join("interrupted-admin.json"),
        serde_json::to_vec_pretty(&match &result {
            Ok((code, body)) => json!({"code":code,"body":body}),
            Err(error) => json!({"transport_error":error.to_string()}),
        })
        .unwrap(),
    )
    .unwrap();
    assert!(
        !result.is_ok_and(|(code, _)| code == 200),
        "uncommitted admin cannot complete"
    );
    for (method, path, body) in [
        ("PUT", "/kv/isolated-attempt", "unknown"),
        ("GET", "/kv/anchor?consistency=linearizable", ""),
    ] {
        let result = c.request(leader, method, path, body);
        assert!(
            !result.is_ok_and(|(code, _)| code == 200 || (method == "GET" && code == 404)),
            "isolated operation succeeded"
        );
    }
    if joint_all {
        c.gate.lock().unwrap().mode = Mode::NewJoint;
        let new_nodes: Vec<_> = new_voters.iter().map(|id| (*id - 1) as usize).collect();
        for &i in &new_nodes {
            c.crash(i);
        }
        for &i in &new_nodes {
            c.start(i);
        }
        c.wait("new-only joint voters reopened", |c| {
            new_nodes.iter().all(|&i| {
                c.status(i).is_some_and(|s| {
                    s["effective_membership"]["voters"]["phase"] == "joint"
                        && s["commit_index"].as_u64().unwrap() < boundary
                })
            })
        });
        c.wait("new-only joint election attempts", |c| {
            let gate = c.gate.lock().unwrap();
            new_voters.iter().all(|id| {
                gate.trace.iter().any(|event| {
                    event["mode"] == "NewJoint"
                        && event["kind"] == "PreVote"
                        && event["from"] == *id
                        && event["allowed"] == true
                })
            })
        });
        for &i in &new_nodes {
            assert!(c.status(i).unwrap()["commit_index"].as_u64().unwrap() < boundary);
            for (method, path, body) in [
                ("GET", "/kv/anchor?consistency=linearizable", ""),
                ("PUT", "/kv/new-only-attempt", "unknown"),
            ] {
                let result = c.request(i, method, path, body);
                assert!(
                    !result.is_ok_and(|(code, _)| code == 200 || (method == "GET" && code == 404)),
                    "joint new-only component acquired authority"
                );
            }
        }
        c.checkpoint("new-only-joint-reopened-no-authority");
        c.gate.lock().unwrap().mode = Mode::Open;
    } else if phase == Phase::Joint {
        c.crash(leader);
        c.crash(partner);
        c.start(leader);
        c.start(partner);
        c.wait("durable joint recovered without quorum", |c| {
            [leader, partner].iter().all(|&i| {
                c.status(i).is_some_and(|s| {
                    s["effective_membership"]["voters"]["phase"] == "joint"
                        && s["commit_index"].as_u64().unwrap() < boundary
                })
            })
        });
        c.checkpoint("reopened-joint");
        c.gate.lock().unwrap().mode = Mode::Open;
    } else {
        let gate = c.gate.lock().unwrap();
        assert!(new_voters.iter().all(|id| gate.joint_durable.contains(id)));
        drop(gate);
        let node5 = c.status(4).unwrap();
        assert_eq!(node5["effective_membership"]["voters"]["phase"], "joint");
        assert!(node5["commit_index"].as_u64().unwrap() < boundary);
        for i in 0..3 {
            c.crash(i);
        }
        // Both original majorities have disappeared. Only two of the final
        // three voters can communicate; final must authorize their recovery.
        c.gate.lock().unwrap().mode = Mode::NewPair;
    }
    let completed = c.admin(REQUEST_ID, operation.clone());
    assert!(completed["record"]["final_index"].as_u64().unwrap() >= boundary);
    let new_leader = c.leader();
    assert!(new_voters.contains(&((new_leader + 1) as u64)));
    if phase == Phase::Final {
        assert!([3, 4].contains(&new_leader));
    }
    c.checked(new_leader, "anchor", "acknowledged-before-membership");
    c.ack(new_leader, "after-recovery", "new-majority-ack");
    c.checked(new_leader, "after-recovery", "new-majority-ack");
    assert_eq!(
        c.admin(REQUEST_ID, operation)["record"],
        completed["record"]
    );
    c.checkpoint("recovered-quorum");
    if phase == Phase::Final {
        c.gate.lock().unwrap().mode = Mode::Open;
        for i in 0..3 {
            c.start(i);
        }
    }
    let final_index = completed["record"]["final_index"].as_u64().unwrap();
    c.wait("all replicas learn committed final configuration", |c| {
        (0..5).all(|i| {
            c.status(i).is_some_and(|s| {
                s["committed_membership"]["state"]["records"][REQUEST_ID]["final_index"]
                    == final_index
                    && s["effective_membership"]["voters"]["voters"] == json!(new_voters)
            })
        })
    });
    c.checkpoint("final-all-replicas");
}

#[test]
fn joint_old_majority_cannot_commit_and_reopens_before_healing() {
    interrupt(Phase::Joint, false);
}

#[test]
fn uncommitted_final_recovers_with_new_majority_after_all_old_voters_crash() {
    interrupt(Phase::Final, false);
}

#[test]
fn joint_new_majority_cannot_elect_or_commit_after_reopening() {
    interrupt(Phase::Joint, true);
}
