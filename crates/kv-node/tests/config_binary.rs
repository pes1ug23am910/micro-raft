//! Actual executable checks for configuration diagnostics and endpoint wiring.
//! Cluster cleanup uses process termination, not coordinated-signal validation.

use std::fs;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

static SEQUENCE: AtomicU32 = AtomicU32::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "micro-raft-binary-{}-{}",
            std::process::id(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("create isolated test directory");
        Self(path)
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        if thread::panicking() {
            if let Ok(entries) = fs::read_dir(&self.0) {
                for entry in entries.flatten() {
                    if entry
                        .path()
                        .extension()
                        .is_some_and(|extension| extension == "log")
                    {
                        if let Ok(log) = fs::read_to_string(entry.path()) {
                            eprintln!("{}:\n{log}", entry.path().display());
                        }
                    }
                }
            }
        }
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Process(Option<Child>);

impl Process {
    fn bounded_output(mut self) -> Output {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if self.0.as_mut().unwrap().try_wait().unwrap().is_some() {
                return self.0.take().unwrap().wait_with_output().unwrap();
            }
            assert!(
                Instant::now() < deadline,
                "configuration command exceeded 10s"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

fn config_output(root: &TestDir, arguments: &[&str]) -> Output {
    Process(Some(
        Command::new(env!("CARGO_BIN_EXE_kv-node"))
            .args(["--id", "1", "--data-dir"])
            .arg(root.0.join("unopened-state"))
            .args(arguments)
            .arg("--check-config")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    ))
    .bounded_output()
}

fn diagnostics(output: Output) -> Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("stdout contains only diagnostic JSON")
}

#[test]
fn legacy_hostname_diagnostics_do_not_open_storage_or_listeners() {
    let root = TestDir::new();
    let http = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let raft = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let http_port = http.local_addr().unwrap().port().to_string();
    let raft_port = raft.local_addr().unwrap().port().to_string();
    let json = diagnostics(config_output(
        &root,
        &[
            "--http-port",
            &http_port,
            "--raft-port",
            &raft_port,
            "--peers",
            "2@localhost:1",
        ],
    ));
    assert_eq!(json["schema_version"], 1);
    assert_eq!(json["mode"], "legacy");
    assert_eq!(json["snapshot_threshold"], 256);
    assert_eq!(json["peers"][0]["endpoint"], "localhost:1");
    assert!(!json["peers"][0]["candidates"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(!root.0.join("unopened-state").exists());
    // Both ports remain occupied by the test. A diagnostic invocation must
    // not try to bind them, and it must leave those listeners untouched.
    assert!(TcpStream::connect(http.local_addr().unwrap()).is_ok());
    assert!(TcpStream::connect(raft.local_addr().unwrap()).is_ok());
}

#[test]
fn explicit_ipv4_and_ipv6_diagnostics_preserve_bind_advertise_separation() {
    let root = TestDir::new();
    for (listen_raft, advertise_raft, listen_http, advertise_http, peer) in [
        (
            "0.0.0.0:7100",
            "192.0.2.1:7100",
            "0.0.0.0:8100",
            "192.0.2.1:8100",
            "2@192.0.2.2:7100",
        ),
        (
            "[::]:7100",
            "[2001:db8::1]:7100",
            "[::]:8100",
            "[2001:db8::1]:8100",
            "2@[2001:db8::2]:7100",
        ),
    ] {
        let json = diagnostics(config_output(
            &root,
            &[
                "--raft-listen",
                listen_raft,
                "--raft-advertise",
                advertise_raft,
                "--http-listen",
                listen_http,
                "--http-advertise",
                advertise_http,
                "--peers",
                peer,
            ],
        ));
        assert_eq!(json["mode"], "explicit");
        assert_eq!(json["raft_listen"], listen_raft);
        assert_eq!(json["raft_advertise"], advertise_raft);
        assert_eq!(json["http_listen"], listen_http);
        assert_eq!(json["http_advertise"], advertise_http);
        assert_eq!(json["raft_candidates"][0], advertise_raft);
        assert!(!root.0.join("unopened-state").exists());
    }
}

#[test]
fn invalid_binary_configuration_exits_two_before_creating_state() {
    let root = TestDir::new();
    for arguments in [
        vec![
            "--http-port",
            "8100",
            "--raft-port",
            "7100",
            "--raft-listen",
            "0.0.0.0:7100",
            "--peers",
            "2@127.0.0.1:7102",
        ],
        vec![
            "--raft-listen",
            "0.0.0.0:7100",
            "--raft-advertise",
            "0.0.0.0:7100",
            "--http-listen",
            "0.0.0.0:8100",
            "--http-advertise",
            "192.0.2.1:8100",
            "--peers",
            "2@192.0.2.2:7100",
        ],
        vec![
            "--raft-listen",
            "0.0.0.0:7100",
            "--raft-advertise",
            "192.0.2.1:7100",
            "--http-listen",
            "0.0.0.0:8100",
            "--http-advertise",
            "192.0.2.1:8100",
            "--peers",
            "2@127.0.0.1:7102",
        ],
    ] {
        let output = config_output(&root, &arguments);
        assert_eq!(output.status.code(), Some(2));
        assert!(!root.0.join("unopened-state").exists());
    }
}

fn request(
    addr: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect_timeout(&addr, Duration::from_millis(300))?;
    stream.set_read_timeout(Some(Duration::from_secs(3)))?;
    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
    write!(stream, "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n{body}", body.len())?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| std::io::Error::other("missing HTTP headers"))?;
    let code = head
        .split_whitespace()
        .nth(1)
        .and_then(|x| x.parse().ok())
        .ok_or_else(|| std::io::Error::other("invalid HTTP status"))?;
    Ok((code, body.to_owned()))
}

fn exercise_cluster(ip: Ipv4Addr, explicit: bool) {
    let root = TestDir::new();
    let reservations: Vec<_> = (0..6)
        .map(|_| TcpListener::bind((ip, 0)).unwrap())
        .collect();
    let ports: Vec<_> = reservations
        .iter()
        .map(|x| x.local_addr().unwrap().port())
        .collect();
    let mut children = Vec::new();
    let mut logs = Vec::new();
    drop(reservations);
    for index in 0..3 {
        let id = index + 1;
        let peers = (0..3)
            .filter(|&other| other != index)
            .map(|other| {
                let host = if explicit {
                    ip.to_string()
                } else {
                    "localhost".into()
                };
                format!("{}@{host}:{}", other + 1, ports[other * 2])
            })
            .collect::<Vec<_>>()
            .join(",");
        let path = root.0.join(format!("node-{id}.log"));
        let mut command = Command::new(env!("CARGO_BIN_EXE_kv-node"));
        command
            .args(["--id", &id.to_string(), "--peers", &peers, "--data-dir"])
            .arg(root.0.join(format!("node-{id}")))
            .stdout(Stdio::null())
            .stderr(Stdio::from(fs::File::create(&path).unwrap()));
        if explicit {
            command.args([
                "--raft-listen",
                &format!("0.0.0.0:{}", ports[index * 2]),
                "--raft-advertise",
                &format!("{ip}:{}", ports[index * 2]),
                "--http-listen",
                &format!("0.0.0.0:{}", ports[index * 2 + 1]),
                "--http-advertise",
                &format!("{ip}:{}", ports[index * 2 + 1]),
            ]);
        } else {
            command.args([
                "--raft-port",
                &ports[index * 2].to_string(),
                "--http-port",
                &ports[index * 2 + 1].to_string(),
            ]);
        }
        eprintln!("launch: {command:?}");
        children.push(Process(Some(command.spawn().unwrap())));
        logs.push(path);
    }
    let addresses: Vec<_> = (0..3)
        .map(|i| SocketAddr::new(IpAddr::V4(ip), ports[i * 2 + 1]))
        .collect();
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut acknowledged = false;
    while Instant::now() < deadline && !acknowledged {
        for child in &mut children {
            assert!(
                child.0.as_mut().unwrap().try_wait().unwrap().is_none(),
                "node exited before cluster formed"
            );
        }
        for address in &addresses {
            if let Ok((200, body)) = request(*address, "GET", "/status", "") {
                let status: Value = serde_json::from_str(&body).unwrap();
                if status["role"] == "leader" {
                    if let Ok((200, body)) =
                        request(*address, "PUT", "/kv/endpoint-proof", "replicated")
                    {
                        let ack: Value = serde_json::from_str(&body).unwrap();
                        assert_eq!(ack["ok"], true);
                        assert!(ack["index"].as_u64().unwrap() > 0);
                        eprintln!("acknowledged at {address}: {ack}");
                        acknowledged = true;
                        break;
                    }
                }
            }
        }
        if !acknowledged {
            thread::sleep(Duration::from_millis(50));
        }
    }
    assert!(acknowledged, "no leader acknowledged a write within 20s");
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let caught_up = addresses.iter().all(|address| matches!(request(*address, "GET", "/kv/endpoint-proof", ""), Ok((200, body)) if body == "replicated"));
        if caught_up {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "replicas did not apply acknowledged value"
        );
        thread::sleep(Duration::from_millis(50));
    }
    drop(children);
    for path in logs {
        eprintln!(
            "{}:\n{}",
            path.file_name().unwrap().to_string_lossy(),
            fs::read_to_string(&path).unwrap()
        );
    }
}

#[test]
fn three_binaries_with_legacy_hostname_peers_replicate() {
    exercise_cluster(Ipv4Addr::LOCALHOST, false);
}

#[test]
#[ignore = "requires MICRO_RAFT_TEST_IPV4 set to an assigned trusted non-loopback IPv4 address"]
fn three_binaries_with_explicit_endpoints_replicate() {
    let ip: Ipv4Addr = std::env::var("MICRO_RAFT_TEST_IPV4")
        .expect("set MICRO_RAFT_TEST_IPV4")
        .parse()
        .unwrap();
    assert!(!ip.is_loopback() && !ip.is_unspecified() && !ip.is_multicast());
    exercise_cluster(ip, true);
}

#[test]
fn three_binaries_recover_selected_snapshots_and_cached_session_responses() {
    let root = TestDir::new();
    let reservations: Vec<_> = (0..6)
        .map(|_| TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap())
        .collect();
    let ports: Vec<_> = reservations
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect();
    drop(reservations);
    let addresses: Vec<_> = (0..3)
        .map(|index| SocketAddr::from(([127, 0, 0, 1], ports[index * 2 + 1])))
        .collect();
    let launch = |boot: usize, threshold: &str| {
        (0..3)
            .map(|index| {
                let peers = (0..3)
                    .filter(|other| *other != index)
                    .map(|other| format!("{}@127.0.0.1:{}", other + 1, ports[other * 2]))
                    .collect::<Vec<_>>()
                    .join(",");
                let log =
                    fs::File::create(root.0.join(format!("snapshot-{boot}-{}.log", index + 1)))
                        .unwrap();
                Process(Some(
                    Command::new(env!("CARGO_BIN_EXE_kv-node"))
                        .args([
                            "--id",
                            &(index + 1).to_string(),
                            "--peers",
                            &peers,
                            "--raft-port",
                            &ports[index * 2].to_string(),
                            "--http-port",
                            &ports[index * 2 + 1].to_string(),
                            "--snapshot-threshold",
                            threshold,
                            "--data-dir",
                        ])
                        .arg(root.0.join(format!("snapshot-node-{}", index + 1)))
                        .stdout(Stdio::null())
                        .stderr(Stdio::from(log))
                        .spawn()
                        .unwrap(),
                ))
            })
            .collect::<Vec<_>>()
    };
    let leader = || {
        let deadline = Instant::now() + Duration::from_secs(12);
        loop {
            for address in &addresses {
                if let Ok((200, body)) = request(*address, "GET", "/status", "") {
                    let status: Value = serde_json::from_str(&body).unwrap();
                    if status["role"] == "leader" {
                        return *address;
                    }
                }
            }
            assert!(
                Instant::now() < deadline,
                "binary cluster did not elect leader"
            );
            thread::sleep(Duration::from_millis(25));
        }
    };
    let mut children = launch(1, "4");
    let initial = leader();
    let (code, registration) =
        request(initial, "POST", "/sessions", "binary-snapshot-session").unwrap();
    assert_eq!(code, 200);
    let session = serde_json::from_str::<Value>(&registration).unwrap()["session_id"]
        .as_u64()
        .unwrap();
    let path = format!("/sessions/{session}/kv/protected?sequence=1");
    let (code, cached) = request(initial, "PUT", &path, "once").unwrap();
    assert_eq!(code, 200);
    assert_eq!(
        request(initial, "PUT", "/kv/protected", "after-cached-write")
            .unwrap()
            .0,
        200
    );
    for index in 0..10 {
        assert_eq!(
            request(
                initial,
                "PUT",
                &format!("/kv/binary-{index}"),
                "acknowledged"
            )
            .unwrap()
            .0,
            200
        );
    }
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let complete = addresses.iter().all(|address| {
            let Ok((200, body)) = request(*address, "GET", "/status", "") else {
                return false;
            };
            let status: Value = serde_json::from_str(&body).unwrap();
            status["snapshot_threshold"] == 4
                && status["snapshot_index"]
                    .as_u64()
                    .is_some_and(|index| index >= 8)
        });
        if complete {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "all binaries must publish before process interruption"
        );
        thread::sleep(Duration::from_millis(25));
    }
    for process in &mut children {
        let child = process.0.as_mut().unwrap();
        child.kill().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(!status.success());
                break;
            }
            assert!(
                Instant::now() < deadline,
                "owned interrupted process did not exit"
            );
            thread::sleep(Duration::from_millis(10));
        }
        process.0.take();
    }
    let mut boundaries = Vec::new();
    for id in 1..=3 {
        let (_storage, recovered) =
            kv_node::storage::Storage::open_recovered(root.0.join(format!("snapshot-node-{id}")))
                .unwrap();
        let image = recovered.snapshot.unwrap();
        boundaries.push(image.descriptor().metadata.last_included_index);
        assert_eq!(
            image
                .application()
                .values()
                .get("protected")
                .map(String::as_str),
            Some("after-cached-write")
        );
    }
    children = launch(2, "0");
    let reopened = leader();
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Ok((200, value)) = request(
            reopened,
            "GET",
            "/kv/protected?consistency=linearizable",
            "",
        ) {
            assert_eq!(value, "after-cached-write");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "reopened leader must establish current-term read authority"
        );
        thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        request(
            reopened,
            "GET",
            "/kv/protected?consistency=linearizable",
            ""
        )
        .unwrap(),
        (200, "after-cached-write".into())
    );
    assert_eq!(
        request(reopened, "POST", "/sessions", "binary-snapshot-session").unwrap(),
        (200, registration)
    );
    assert_eq!(
        request(reopened, "PUT", &path, "once").unwrap(),
        (200, cached)
    );
    assert_eq!(
        request(
            reopened,
            "GET",
            "/kv/protected?consistency=linearizable",
            ""
        )
        .unwrap(),
        (200, "after-cached-write".into())
    );
    let status: Value =
        serde_json::from_str(&request(reopened, "GET", "/status", "").unwrap().1).unwrap();
    assert_eq!(status["snapshot_threshold"], 0);
    println!(
        "binary_snapshot_recovery {}",
        serde_json::json!({"recovered_snapshot_indices":boundaries,"restart_capture_threshold":0,"replay_preserved":true})
    );
    drop(children);
}
