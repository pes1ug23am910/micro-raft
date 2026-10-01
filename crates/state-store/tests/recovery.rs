use state_store::{
    CommitOutcome, FaultPoint, LsmOptions, LsmStore, Mutation, RedbStore, StateStore,
};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "micro-raft-state-{}-{}",
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

fn options() -> LsmOptions {
    LsmOptions {
        memtable_bytes: 512,
        target_table_bytes: 512,
        l0_trigger: 2,
        level1_bytes: 2048,
        max_compaction_input_bytes: 8 * 1024 * 1024,
    }
}
fn open(kind: &str, path: &Path) -> Box<dyn StateStore> {
    if kind == "lsm" {
        Box::new(LsmStore::open(path, options()).unwrap())
    } else {
        Box::new(RedbStore::open(path).unwrap())
    }
}
fn cells(value: &[u8], session: &[u8]) -> Vec<Mutation> {
    vec![
        Mutation::put(b"value/key", value),
        Mutation::put(b"session/7/key", session),
    ]
}
fn assert_state(store: &mut dyn StateStore, index: u64, value: &[u8], session: &[u8]) {
    assert_eq!(store.applied_index(), index);
    assert_eq!(store.get(b"value/key").unwrap(), Some(value.to_vec()));
    assert_eq!(store.get(b"session/7/key").unwrap(), Some(session.to_vec()));
}

#[test]
fn contiguous_ranges_reopen_atomically_and_replay_binds_both_ends() {
    for kind in ["lsm", "redb"] {
        let dir = Temp::new();
        let mut store = open(kind, &dir.0);
        let final_cells = cells(b"last-value", b"cached-original-index-2");
        assert_eq!(
            store.commit_range(1, 7, &final_cells).unwrap(),
            CommitOutcome::Applied
        );
        assert_state(&mut *store, 7, b"last-value", b"cached-original-index-2");
        assert_eq!(store.statistics().unwrap().commits, 1);
        assert_eq!(
            store.commit_range(1, 7, &final_cells).unwrap(),
            CommitOutcome::Replay
        );
        assert!(store.commit_range(2, 7, &final_cells).is_err());
        assert!(store.commit_range(7, 8, &final_cells).is_err());
        assert!(store.commit_range(9, 10, &final_cells).is_err());
        assert!(store.commit_range(8, 7, &final_cells).is_err());
        assert!(store.commit_range(8, 264, &final_cells).is_err());
        drop(store);
        let mut store = open(kind, &dir.0);
        assert_state(&mut *store, 7, b"last-value", b"cached-original-index-2");
        assert_eq!(
            store.commit_range(1, 7, &final_cells).unwrap(),
            CommitOutcome::Replay
        );
        store.commit_range(8, 263, &[]).unwrap();
        drop(store);
        let mut store = open(kind, &dir.0);
        assert_state(&mut *store, 263, b"last-value", b"cached-original-index-2");
    }
}

#[test]
fn both_engines_atomically_reopen_values_retry_cells_and_watermark() {
    for kind in ["lsm", "redb"] {
        let dir = Temp::new();
        let mut store = open(kind, &dir.0);
        let first = cells(b"first", b"seq1/cached-response1");
        assert_eq!(store.commit(1, &first).unwrap(), CommitOutcome::Applied);
        assert_eq!(store.commit(1, &first).unwrap(), CommitOutcome::Replay);
        assert!(store.commit(1, &cells(b"changed", b"seq1")).is_err());
        assert!(store.commit(3, &first).is_err());
        store
            .commit(
                2,
                &[
                    Mutation::delete(b"value/key"),
                    Mutation::put(b"session/7/key", b"seq2/deleted"),
                ],
            )
            .unwrap();
        store.commit(3, &[]).unwrap();
        drop(store);
        let mut store = open(kind, &dir.0);
        assert_eq!(store.applied_index(), 3);
        assert_eq!(store.get(b"value/key").unwrap(), None);
        assert_eq!(
            store.get(b"session/7/key").unwrap(),
            Some(b"seq2/deleted".to_vec())
        );
        assert_eq!(store.commit(3, &[]).unwrap(), CommitOutcome::Replay);
        store
            .install(20, &cells(b"snapshot", b"seq7/from-snapshot"))
            .unwrap();
        drop(store);
        let mut store = open(kind, &dir.0);
        assert_state(&mut *store, 20, b"snapshot", b"seq7/from-snapshot");
        store.commit(21, &cells(b"next", b"seq8")).unwrap();
    }
}

#[test]
fn maintenance_drains_level_zero_instead_of_replacing_one_run_forever() {
    let dir = Temp::new();
    let mut store = LsmStore::open(
        &dir.0,
        LsmOptions {
            memtable_bytes: 4096,
            target_table_bytes: 4096,
            l0_trigger: 2,
            level1_bytes: 64 * 1024,
            max_compaction_input_bytes: 1024 * 1024,
        },
    )
    .unwrap();
    for index in 1..=2 {
        store
            .commit(index, &[Mutation::put(format!("key/{index}"), b"value")])
            .unwrap();
        store.flush().unwrap();
    }
    assert_eq!(store.statistics().unwrap().tables_per_level[0], 2);
    assert!(store.statistics().unwrap().pending_compaction);
    store
        .commit(3, &[Mutation::put(b"key/3", b"value")])
        .unwrap();
    assert!(store.maintain().unwrap());
    let stats = store.statistics().unwrap();
    assert_eq!(
        stats.tables_per_level[0], 0,
        "maintenance must drain the triggered level"
    );
    assert!(!stats.pending_compaction);
    assert_eq!(stats.compactions, 1);
    store
        .commit(4, &[Mutation::put(b"key/4", b"value")])
        .unwrap();
    assert!(
        !store.maintain().unwrap(),
        "one new write must not perpetuate maintenance"
    );
    drop(store);
    let mut store = LsmStore::open(&dir.0, LsmOptions::default()).unwrap();
    assert_eq!(store.applied_index(), 4);
    assert_eq!(store.scan().unwrap().len(), 4);
}

#[test]
fn continuing_writes_allow_level_one_to_progress_to_level_two() {
    let dir = Temp::new();
    let mut store = LsmStore::open(&dir.0, options()).unwrap();
    for index in 1..=64 {
        store
            .commit(
                index,
                &[Mutation::put(
                    format!("key/{index:03}"),
                    vec![index as u8; 128],
                )],
            )
            .unwrap();
        store.maintain().unwrap();
    }
    assert!(
        store.statistics().unwrap().tables_per_level[2] > 0,
        "level one must not starve behind repeated level-zero work"
    );
    drop(store);
    let mut store = LsmStore::open(&dir.0, options()).unwrap();
    assert_eq!(store.applied_index(), 64);
    assert_eq!(store.scan().unwrap().len(), 64);
    for index in 1..=64 {
        assert_eq!(
            store.get(format!("key/{index:03}").as_bytes()).unwrap(),
            Some(vec![index as u8; 128])
        );
    }
}

#[test]
fn bounded_seeded_reference_map_survives_reopen_and_compaction() {
    for seed in 1u64..=4 {
        let dir = Temp::new();
        let mut lsm = LsmStore::open(&dir.0, options()).unwrap();
        let mut reference = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut random = seed;
        for index in 1u64..=180 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let key = format!("key/{:03}", random % 31).into_bytes();
            let mutation = if random % 5 == 0 {
                reference.remove(&key);
                Mutation::delete(key)
            } else {
                let value = vec![(random >> 32) as u8; 60 + (random % 91) as usize];
                reference.insert(key.clone(), value.clone());
                Mutation::put(key, value)
            };
            lsm.commit(index, &[mutation]).unwrap();
            if index % 7 == 0 {
                let _ = lsm.compact_once().unwrap();
            }
            if index % 23 == 0 {
                drop(lsm);
                lsm = LsmStore::open(&dir.0, options()).unwrap();
            }
            assert_eq!(
                lsm.scan().unwrap(),
                reference.clone().into_iter().collect::<Vec<_>>(),
                "seed={seed}, index={index}"
            );
        }
        lsm.flush().unwrap();
        while lsm.compact_once().unwrap() {}
        assert!(lsm.statistics().unwrap().compactions > 0);
        drop(lsm);
        let mut lsm = LsmStore::open(&dir.0, options()).unwrap();
        assert_eq!(
            lsm.scan().unwrap(),
            reference.into_iter().collect::<Vec<_>>()
        );
        assert_eq!(lsm.applied_index(), 180);
    }
}

#[test]
fn publication_failures_recover_whole_state_and_poison_the_open_handle() {
    let points = [
        FaultPoint::TableWritten,
        FaultPoint::TableSynced,
        FaultPoint::NewWalSynced,
        FaultPoint::FilesDirectorySynced,
        FaultPoint::ManifestWritten,
        FaultPoint::ManifestSynced,
        FaultPoint::ManifestRenamed,
        FaultPoint::ManifestDirectorySynced,
        FaultPoint::ObsoleteRemoved,
        FaultPoint::ReclaimDirectorySynced,
    ];
    for point in points {
        let dir = Temp::new();
        let mut lsm = LsmStore::open(&dir.0, options()).unwrap();
        lsm.commit(1, &cells(b"old", b"old-retry")).unwrap();
        lsm.inject_failure(point);
        assert!(
            lsm.install(10, &cells(b"new", b"new-retry")).is_err(),
            "unreached {point:?}"
        );
        assert!(lsm.commit(2, &[]).is_err());
        assert!(lsm.get(b"value/key").is_err());
        drop(lsm);
        let mut reopened = LsmStore::open(&dir.0, options()).unwrap();
        match reopened.applied_index() {
            1 => assert_state(&mut reopened, 1, b"old", b"old-retry"),
            10 => assert_state(&mut reopened, 10, b"new", b"new-retry"),
            other => panic!("incoherent publication {point:?}: {other}"),
        }
    }
}

#[test]
fn sync_failures_never_report_success_and_reopen_keeps_atomic_batches() {
    for point in [FaultPoint::WalWritten, FaultPoint::WalSynced] {
        let dir = Temp::new();
        let mut lsm = LsmStore::open(&dir.0, options()).unwrap();
        lsm.commit(1, &cells(b"old", b"old-retry")).unwrap();
        lsm.inject_failure(point);
        assert!(lsm
            .commit_range(2, 8, &cells(b"new", b"new-retry"))
            .is_err());
        assert!(lsm.get(b"value/key").is_err());
        drop(lsm);
        let mut reopened = LsmStore::open(&dir.0, options()).unwrap();
        match reopened.applied_index() {
            1 => assert_state(&mut reopened, 1, b"old", b"old-retry"),
            8 => assert_state(&mut reopened, 8, b"new", b"new-retry"),
            other => panic!("bad index {other}"),
        }
    }
    let dir = Temp::new();
    let mut redb = RedbStore::open(&dir.0).unwrap();
    redb.commit(1, &cells(b"old", b"old-retry")).unwrap();
    redb.inject_sync_failure();
    assert!(redb
        .commit_range(2, 8, &cells(b"new", b"new-retry"))
        .is_err());
    assert!(redb.get(b"value/key").is_err());
    drop(redb);
    let mut reopened = RedbStore::open(&dir.0).unwrap();
    match reopened.applied_index() {
        1 => assert_state(&mut reopened, 1, b"old", b"old-retry"),
        8 => assert_state(&mut reopened, 8, b"new", b"new-retry"),
        other => panic!("bad index {other}"),
    }
}

fn selected_file(dir: &Path, suffix: &str) -> PathBuf {
    fs::read_dir(dir)
        .unwrap()
        .map(Result::unwrap)
        .find(|entry| entry.file_name().to_string_lossy().ends_with(suffix))
        .unwrap()
        .path()
}

#[test]
fn incomplete_final_wal_frame_is_trimmed_but_complete_corruption_fails() {
    let original = Temp::new();
    let mut store = LsmStore::open(&original.0, LsmOptions::default()).unwrap();
    store.commit(1, &cells(b"old", b"old-retry")).unwrap();
    let wal = selected_file(&original.0, ".wal");
    let first = fs::metadata(&wal).unwrap().len() as usize;
    store.commit(2, &cells(b"new", b"new-retry")).unwrap();
    drop(store);
    let raw = fs::read(&wal).unwrap();
    let second = raw.len() - first;
    for cut in [1, 7, 11, 12, 15, 43, 44, 45, second - 32, second - 1] {
        let dir = Temp::new();
        fs::copy(original.0.join("CURRENT"), dir.0.join("CURRENT")).unwrap();
        fs::copy(original.0.join("ENGINE"), dir.0.join("ENGINE")).unwrap();
        fs::write(dir.0.join(wal.file_name().unwrap()), &raw[..first + cut]).unwrap();
        let mut reopened = LsmStore::open(&dir.0, options()).unwrap();
        assert_state(&mut reopened, 1, b"old", b"old-retry");
        assert_eq!(
            fs::metadata(dir.0.join(wal.file_name().unwrap()))
                .unwrap()
                .len(),
            first as u64
        );
    }
    // Includes upward length corruption that used to look like an incomplete
    // tail, plus downward length, authenticated header, payload and digest.
    for offset in [8, 9, 10, 11, 20, 44, second - 1] {
        let mut corrupt = raw.clone();
        corrupt[first + offset] ^= 0x01;
        fs::write(&wal, &corrupt).unwrap();
        assert!(
            LsmStore::open(&original.0, options()).is_err(),
            "offset {offset}"
        );
        assert_eq!(
            fs::read(&wal).unwrap(),
            corrupt,
            "corruption must not be repaired as a tail"
        );
    }
}

#[test]
fn selected_table_manifest_and_missing_wal_corruption_fail_closed() {
    for kind in ["table", "manifest", "missing-table", "missing-wal"] {
        let dir = Temp::new();
        let mut store = LsmStore::open(&dir.0, options()).unwrap();
        store.commit(1, &cells(b"durable", b"retry")).unwrap();
        store.flush().unwrap();
        drop(store);
        let path = match kind {
            "manifest" => dir.0.join("CURRENT"),
            "missing-wal" => selected_file(&dir.0, ".wal"),
            _ => selected_file(&dir.0, ".sst"),
        };
        if kind.starts_with("missing") {
            fs::remove_file(&path).unwrap();
        } else {
            let mut bytes = fs::read(&path).unwrap();
            bytes[20] ^= 0x80;
            fs::write(path, bytes).unwrap();
        }
        assert!(LsmStore::open(&dir.0, options()).is_err(), "{kind}");
    }
}

#[test]
fn tombstones_do_not_resurrect_values_in_older_levels() {
    let dir = Temp::new();
    let mut lsm = LsmStore::open(&dir.0, options()).unwrap();
    for index in 1..=80u64 {
        let key = format!("k/{:02}", index % 12).into_bytes();
        lsm.commit(index, &[Mutation::put(key, vec![index as u8; 180])])
            .unwrap();
        lsm.flush().unwrap();
        while lsm.compact_once().unwrap() {}
    }
    assert!(lsm.statistics().unwrap().tables_per_level[2] > 0);
    lsm.commit(81, &[Mutation::delete(b"k/04")]).unwrap();
    lsm.flush().unwrap();
    for index in 82..=100 {
        lsm.commit(index, &[Mutation::put(b"other", vec![index as u8; 300])])
            .unwrap();
        lsm.flush().unwrap();
        while lsm.compact_once().unwrap() {}
        assert_eq!(lsm.get(b"k/04").unwrap(), None);
    }
    drop(lsm);
    let mut reopened = LsmStore::open(&dir.0, options()).unwrap();
    assert_eq!(reopened.get(b"k/04").unwrap(), None);
}

#[test]
fn concurrent_directory_open_and_invalid_batches_do_not_mutate_state() {
    for kind in ["lsm", "redb"] {
        let dir = Temp::new();
        let mut store = open(kind, &dir.0);
        assert!(LsmStore::open(&dir.0, options()).is_err());
        assert!(RedbStore::open(&dir.0).is_err());
        assert!(store
            .commit(
                1,
                &[Mutation::put(b"same", b"a"), Mutation::put(b"same", b"b")]
            )
            .is_err());
        assert!(store
            .commit(1, &[Mutation::put(Vec::new(), b"value")])
            .is_err());
        assert!(store
            .commit(
                1,
                &[Mutation::put(
                    b"large",
                    vec![0; state_store::MAX_VALUE_BYTES + 1]
                )]
            )
            .is_err());
        assert_eq!(store.applied_index(), 0);
        assert!(store.scan().unwrap().is_empty());
        store.commit(1, &cells(b"valid", b"retry")).unwrap();
    }
}

#[test]
fn point_lookup_filter_counters_include_absent_in_range_keys() -> io::Result<()> {
    let dir = Temp::new();
    let mut store = LsmStore::open(&dir.0, LsmOptions::default())?;
    let changes: Vec<_> = (0..1000)
        .map(|n| Mutation::put(format!("k/{:06}", n * 2).into_bytes(), b"v"))
        .collect();
    store.commit(1, &changes)?;
    store.flush()?;
    for n in 0..1999 {
        assert_eq!(
            store.get(format!("k/{n:06}").as_bytes())?.is_some(),
            n % 2 == 0
        );
    }
    let stats = store.statistics()?;
    assert_eq!(stats.bloom_probes, 1999);
    assert!(stats.bloom_positive >= 1000);
    assert_eq!(stats.bloom_positive - 1000, stats.bloom_false_positive);
    assert!(stats.bloom_false_positive < 100);
    assert!(stats.point_read_block_bytes > 0);
    Ok(())
}
