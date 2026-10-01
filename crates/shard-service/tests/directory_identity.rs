use shard_service::topology::{DirectoryGuard, Topology};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Dir(PathBuf);
impl Dir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "micro-raft-shard-identity-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        assert!(!p.exists());
        Self(p)
    }
}
impl Drop for Dir {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            let _ = fs::remove_dir_all(&self.0);
        } else {
            eprintln!("retained failed fixture {}", self.0.display());
        }
    }
}
fn topology() -> Topology {
    serde_json::from_value(serde_json::json!({"version":1,"cluster":"identity-test","groups":[1,2],"owners":[1,2],"genesis_voters":{"0":[1],"1":[1],"2":[1]},"nodes":[{"id":1,"http":"127.0.0.1:5010","raft":{"0":"127.0.0.1:5011","1":"127.0.0.1:5012","2":"127.0.0.1:5013"}}]})).unwrap()
}

#[test]
fn exclusive_directory_identity_survives_reopen_and_advances_boot() {
    let dir = Dir::new();
    let topology = topology();
    let first = DirectoryGuard::open(&dir.0, 1, &topology).unwrap();
    assert_eq!(first.boot, 1);
    first.mark_ready(&dir.0).unwrap();
    assert!(DirectoryGuard::open(&dir.0, 1, &topology).is_err());
    drop(first);
    let second = DirectoryGuard::open(&dir.0, 1, &topology).unwrap();
    assert_eq!(second.boot, 2);
    drop(second);
    let mut other = topology.clone();
    other.owners.swap(0, 1);
    assert!(DirectoryGuard::open(&dir.0, 1, &other).is_err());
}

#[test]
fn missing_payload_wrong_group_or_interrupted_initialization_fails_closed() {
    for failure in [
        "missing-wal",
        "missing-hardstate",
        "group-swap",
        "incomplete",
    ] {
        let dir = Dir::new();
        let topology = topology();
        let guard = DirectoryGuard::open(&dir.0, 1, &topology).unwrap();
        if failure != "incomplete" {
            guard.mark_ready(&dir.0).unwrap();
        }
        drop(guard);
        match failure {
            "missing-wal" => fs::remove_file(dir.0.join("group-1/log.jsonl")).unwrap(),
            "missing-hardstate" => fs::remove_file(dir.0.join("group-1/hardstate.json")).unwrap(),
            "group-swap" => {
                fs::copy(dir.0.join("group-2/GROUP"), dir.0.join("group-1/GROUP")).unwrap();
            }
            _ => {}
        }
        assert!(
            DirectoryGuard::open(&dir.0, 1, &topology).is_err(),
            "{failure}"
        );
    }
}

#[test]
fn interrupted_boot_counter_publication_recovers_from_the_validated_current_counter() {
    use std::io::Write;
    let dir = Dir::new();
    let topology = topology();
    let guard = DirectoryGuard::open(&dir.0, 1, &topology).unwrap();
    guard.mark_ready(&dir.0).unwrap();
    drop(guard);
    let mut stage = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dir.0.join("BOOT.new"))
        .unwrap();
    stage.write_all(b"2").unwrap();
    stage.sync_all().unwrap();
    drop(stage);
    let reopened = DirectoryGuard::open(&dir.0, 1, &topology).unwrap();
    assert_eq!(reopened.boot, 2);
    assert!(!dir.0.join("BOOT.new").exists());
}
#[test]
fn numeric_topology_rejects_mapped_alias_broadcast_and_scoped_ipv6() {
    let mut topology = topology();
    topology.nodes[0].http = "[::ffff:127.0.0.1]:5011".parse().unwrap();
    assert!(topology.validate().is_err());
    topology.nodes[0].http = "255.255.255.255:5010".parse().unwrap();
    assert!(topology.validate().is_err());
    topology.nodes[0].http = std::net::SocketAddr::V6(std::net::SocketAddrV6::new(
        "fe80::1".parse().unwrap(),
        5010,
        0,
        2,
    ));
    assert!(topology.validate().is_err());
}

#[test]
fn adding_routable_hosts_or_changing_addresses_does_not_change_group_authority() {
    let dir = Dir::new();
    let mut topology = topology();
    let fingerprint = topology.fingerprint();
    let scope = topology.scope(1).unwrap();
    let guard = DirectoryGuard::open(&dir.0, 1, &topology).unwrap();
    guard.mark_ready(&dir.0).unwrap();
    drop(guard);
    topology.nodes[0].http = "127.0.0.1:6010".parse().unwrap();
    let mut learner = topology.nodes[0].clone();
    learner.id = 2;
    learner.http = "127.0.0.1:6020".parse().unwrap();
    for (group, addr) in &mut learner.raft {
        *addr = format!("127.0.0.1:{}", 6021 + group).parse().unwrap();
    }
    topology.nodes.push(learner);
    topology.validate().unwrap();
    assert_eq!(topology.fingerprint(), fingerprint);
    assert_eq!(topology.scope(1).unwrap(), scope);
    assert_eq!(scope.genesis_voters(), &[1]);
    let reopened = DirectoryGuard::open(&dir.0, 1, &topology).unwrap();
    assert_eq!(reopened.boot, 2);
}

#[test]
fn historical_genesis_identity_survives_removing_its_retired_route() {
    let mut topology = topology();
    let fingerprint = topology.fingerprint();
    let scope = topology.scope(1).unwrap();
    topology.nodes[0].id = 2;
    topology.validate().unwrap();
    assert_eq!(topology.fingerprint(), fingerprint);
    assert_eq!(topology.scope(1).unwrap(), scope);
    assert_eq!(topology.genesis_voters[&1], vec![1]);
}
