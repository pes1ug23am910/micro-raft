use state_store::{LsmOptions, LsmStore, Mutation, RedbStore, StateStore};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "micro-raft-engine-{}-{}",
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
fn open(kind: &str, path: &Path) -> io::Result<Box<dyn StateStore>> {
    if kind == "lsm" {
        Ok(Box::new(LsmStore::open(path, LsmOptions::default())?))
    } else {
        Ok(Box::new(RedbStore::open(path)?))
    }
}
fn files(path: &Path) -> Vec<(String, Vec<u8>)> {
    let mut result: Vec<_> = fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (
                entry.file_name().to_string_lossy().into_owned(),
                fs::read(entry.path()).unwrap(),
            )
        })
        .collect();
    result.sort();
    result
}
fn rejection(kind: &str, path: &Path) {
    let before = files(path);
    let error = match open(kind, path) {
        Ok(_) => panic!("unexpected initialization"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        files(path),
        before,
        "failed admission must preserve existing files"
    );
}

#[test]
fn sequential_backend_switch_is_rejected_without_hiding_existing_state() {
    for kind in ["lsm", "redb"] {
        let dir = Temp::new();
        let mut store = open(kind, &dir.0).unwrap();
        store
            .commit(1, &[Mutation::put(b"key", b"acknowledged")])
            .unwrap();
        drop(store);
        rejection(if kind == "lsm" { "redb" } else { "lsm" }, &dir.0);
        let mut store = open(kind, &dir.0).unwrap();
        assert_eq!(store.applied_index(), 1);
        assert_eq!(store.get(b"key").unwrap(), Some(b"acknowledged".to_vec()));
    }
}

#[test]
fn deleted_selected_payload_never_becomes_an_empty_store() {
    for kind in ["lsm", "redb"] {
        let dir = Temp::new();
        let mut store = open(kind, &dir.0).unwrap();
        store
            .commit(1, &[Mutation::put(b"key", b"acknowledged")])
            .unwrap();
        drop(store);
        fs::remove_file(dir.0.join(if kind == "lsm" {
            "CURRENT"
        } else {
            "state.redb"
        }))
        .unwrap();
        rejection(kind, &dir.0);
    }
}

#[test]
fn missing_or_corrupt_marker_rejects_preexisting_state() {
    for kind in ["lsm", "redb"] {
        for corrupt in [false, true] {
            let dir = Temp::new();
            drop(open(kind, &dir.0).unwrap());
            if corrupt {
                fs::write(dir.0.join("ENGINE"), b"damaged identity").unwrap();
            } else {
                fs::remove_file(dir.0.join("ENGINE")).unwrap();
            }
            rejection(kind, &dir.0);
        }
    }
}

#[test]
fn interrupted_first_creation_fails_closed_with_no_payload() {
    for kind in ["lsm", "redb"] {
        for staged in [false, true] {
            let dir = Temp::new();
            fs::write(dir.0.join("LOCK"), []).unwrap();
            fs::write(
                dir.0.join(if staged { "ENGINE.new" } else { "ENGINE" }),
                format!("micro-raft-state-store\nversion=1\nengine={kind}\n"),
            )
            .unwrap();
            rejection(kind, &dir.0);
        }
    }
}
