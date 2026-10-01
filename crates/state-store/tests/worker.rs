use serde_json::{json, Value};
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    mpsc,
};
use std::time::Duration;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "micro-raft-worker-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct Worker {
    child: Child,
    responses: mpsc::Receiver<String>,
}
impl Worker {
    fn start(engine: &str, path: &std::path::Path) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_state-worker"))
            .args(["--engine", engine, "--directory"])
            .arg(path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let output = child.stdout.take().unwrap();
        let (sender, responses) = mpsc::sync_channel(8);
        std::thread::spawn(move || {
            for line in BufReader::new(output).lines() {
                match line {
                    Ok(line) => {
                        if sender.send(line).is_err() {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        let worker = Self { child, responses };
        assert_eq!(worker.receive()["ready"], true);
        worker
    }
    fn receive(&self) -> Value {
        serde_json::from_str(
            &self
                .responses
                .recv_timeout(Duration::from_secs(10))
                .expect("bounded worker response"),
        )
        .unwrap()
    }
    fn call(&mut self, request: Value) -> Value {
        let stdin = self.child.stdin.as_mut().unwrap();
        serde_json::to_writer(&mut *stdin, &request).unwrap();
        stdin.write_all(b"\n").unwrap();
        stdin.flush().unwrap();
        self.receive()
    }
    fn kill(&mut self) {
        self.child.kill().unwrap();
        self.child.wait().unwrap();
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[test]
fn abrupt_process_termination_after_ack_preserves_cells_and_progress() {
    for engine in ["lsm", "redb"] {
        let dir = Temp::new();
        let mut worker = Worker::start(engine, &dir.0);
        let mutation = json!({"op":"commit_range","first":1,"index":7,"changes":[{"key":[1],"value":[42]},{"key":[2],"value":[7]}]});
        let ack = worker.call(mutation.clone());
        assert_eq!(ack["outcome"], "ok");
        assert_eq!(ack["applied_index"], 7);
        // The observer has received the complete response before killing the
        // child. No exit request or destructor flush runs in the worker.
        worker.kill();
        drop(worker);
        let mut worker = Worker::start(engine, &dir.0);
        assert_eq!(
            worker.call(json!({"op":"get","key":[1]}))["value"],
            json!([42])
        );
        assert_eq!(
            worker.call(json!({"op":"get","key":[2]}))["value"],
            json!([7])
        );
        let replay = worker.call(mutation);
        assert_eq!(replay["commit"], "Replay");
        assert_eq!(replay["applied_index"], 7);
        assert_eq!(worker.call(json!({"op":"exit"}))["exited"], true);
        assert!(worker.child.wait().unwrap().success());
    }
}

#[test]
fn rejected_input_does_not_become_unknown_or_kill_a_healthy_worker() {
    for engine in ["lsm", "redb"] {
        let dir = Temp::new();
        let mut worker = Worker::start(engine, &dir.0);
        let rejected = worker.call(json!({"op":"commit","index":2,"changes":[]}));
        assert_eq!(rejected["outcome"], "rejected");
        assert_eq!(rejected["fatal"], false);
        assert_eq!(
            worker.call(json!({"op":"commit","index":1,"changes":[]}))["outcome"],
            "ok"
        );
        assert_eq!(
            worker.call(json!({"op":"get","key":[9]}))["outcome"],
            "absent"
        );
        assert_eq!(worker.call(json!({"op":"unknown"}))["outcome"], "rejected");
        assert_eq!(worker.call(json!({"op":"statistics"}))["outcome"], "ok");
    }
}
