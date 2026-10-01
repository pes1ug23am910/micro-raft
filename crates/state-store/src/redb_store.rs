use crate::codec::encode_batch;
use crate::durable::{sync_directory, Directory, Engine};
use crate::{
    digest, invalid, rejected, validate_changes, validate_install, CommitOutcome, Mutation, Rows,
    StateStore, Statistics,
};
use redb::backends::FileBackend;
use redb::{
    BackendError, Database, Durability, ReadableDatabase, ReadableTable, StorageBackend,
    TableDefinition,
};
use std::fs::OpenOptions;
use std::io;
use std::ops::Bound;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

const CELLS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("cells_v1");
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("progress_v1");

#[derive(Debug, Default)]
struct Counters {
    writes: AtomicU64,
    syncs: AtomicU64,
    fail_sync: AtomicBool,
}

#[derive(Debug)]
struct CountedBackend {
    inner: FileBackend,
    counters: Arc<Counters>,
}

impl StorageBackend for CountedBackend {
    fn len(&self) -> io::Result<u64> {
        self.inner.len()
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(offset, out)
    }
    fn set_len(&self, length: u64) -> io::Result<()> {
        self.inner.set_len(length)
    }
    fn sync_data(&self) -> io::Result<()> {
        if self.counters.fail_sync.swap(false, Ordering::Relaxed) {
            return Err(io::Error::other("injected redb sync failure"));
        }
        self.inner.sync_data()?;
        self.counters.syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.inner.write(offset, data)?;
        self.counters
            .writes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }
    fn close(&self) -> io::Result<()> {
        self.inner.close()
    }
    fn try_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.inner.try_lock_range(start, end)
    }
    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> Result<bool, BackendError> {
        self.inner.try_lock_shared_range(start, end)
    }
    fn lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.lock_range(start, end)
    }
    fn lock_shared_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.lock_shared_range(start, end)
    }
    fn unlock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<(), BackendError> {
        self.inner.unlock_range(start, end)
    }
    fn query_lock_range(&self, start: Bound<u64>, end: Bound<u64>) -> Result<bool, BackendError> {
        self.inner.query_lock_range(start, end)
    }
}

pub struct RedbStore {
    database: Database,
    dir: Directory,
    counters: Arc<Counters>,
    applied: u64,
    last_digest: [u8; 32],
    stats: Statistics,
    poisoned: bool,
}

fn metadata(index: u64, hash: [u8; 32]) -> Vec<u8> {
    let mut bytes = 1u32.to_le_bytes().to_vec();
    bytes.extend(index.to_le_bytes());
    bytes.extend(hash);
    bytes
}
fn parse_metadata(bytes: &[u8]) -> io::Result<(u64, [u8; 32])> {
    if bytes.len() != 44 || bytes[..4] != 1u32.to_le_bytes() {
        return Err(invalid("redb application metadata invalid"));
    }
    Ok((
        u64::from_le_bytes(bytes[4..12].try_into().expect("checked")),
        bytes[12..].try_into().expect("checked"),
    ))
}

impl RedbStore {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let dir = Directory::open(path.as_ref(), Engine::Redb)?;
        let mut stats = Statistics {
            directory_syncs: dir.open_directory_syncs,
            content_syncs: dir.open_content_syncs,
            metadata_bytes: dir.open_metadata_bytes,
            ..Statistics::default()
        };
        let data_path = dir.path.join("state.redb");
        let exists = !dir.fresh;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(dir.fresh)
            .truncate(false)
            .open(&data_path)?;
        if exists && file.metadata()?.len() == 0 {
            return Err(invalid("existing redb file is empty"));
        }
        let counters = Arc::new(Counters::default());
        let backend = CountedBackend {
            inner: FileBackend::new(file).map_err(io::Error::other)?,
            counters: counters.clone(),
        };
        let database = Database::builder()
            .set_cache_size(8 * 1024 * 1024)
            .create_with_backend(backend)
            .map_err(io::Error::other)?;
        if !exists {
            let mut transaction = database.begin_write().map_err(io::Error::other)?;
            transaction
                .set_durability(Durability::Immediate)
                .map_err(io::Error::other)?;
            {
                let _cells = transaction.open_table(CELLS).map_err(io::Error::other)?;
                let mut meta = transaction.open_table(META).map_err(io::Error::other)?;
                meta.insert("state", metadata(0, [0; 32]).as_slice())
                    .map_err(io::Error::other)?;
            }
            transaction.commit().map_err(io::Error::other)?;
            sync_directory(&dir.path)?;
            stats.directory_syncs += 1;
        }
        let (applied, last_digest) = {
            let transaction = database.begin_read().map_err(io::Error::other)?;
            let table = transaction.open_table(META).map_err(io::Error::other)?;
            let value = table
                .get("state")
                .map_err(io::Error::other)?
                .ok_or_else(|| invalid("redb progress missing"))?;
            parse_metadata(value.value())?
        };
        Ok(Self {
            database,
            dir,
            counters,
            applied,
            last_digest,
            stats,
            poisoned: false,
        })
    }
    fn healthy(&self) -> io::Result<()> {
        if self.poisoned {
            Err(io::Error::other(
                "redb adapter requires reopen after transaction failure",
            ))
        } else {
            Ok(())
        }
    }
    pub fn inject_sync_failure(&mut self) {
        self.counters.fail_sync.store(true, Ordering::Relaxed);
    }
    fn write(
        &mut self,
        index: u64,
        changes: &[Mutation],
        hash: [u8; 32],
        replace: bool,
    ) -> io::Result<()> {
        self.healthy()?;
        let write = || -> io::Result<()> {
            let mut transaction = self.database.begin_write().map_err(io::Error::other)?;
            transaction
                .set_durability(Durability::Immediate)
                .map_err(io::Error::other)?;
            {
                let mut cells = transaction.open_table(CELLS).map_err(io::Error::other)?;
                if replace {
                    cells.retain(|_, _| false).map_err(io::Error::other)?;
                }
                for change in changes {
                    if let Some(value) = &change.value {
                        cells
                            .insert(change.key.as_slice(), value.as_slice())
                            .map_err(io::Error::other)?;
                    } else {
                        cells
                            .remove(change.key.as_slice())
                            .map_err(io::Error::other)?;
                    }
                }
                let mut meta = transaction.open_table(META).map_err(io::Error::other)?;
                meta.insert("state", metadata(index, hash).as_slice())
                    .map_err(io::Error::other)?;
            }
            transaction.commit().map_err(io::Error::other)
        };
        if let Err(error) = write() {
            self.poisoned = true;
            return Err(error);
        }
        self.applied = index;
        self.last_digest = hash;
        Ok(())
    }
}

impl StateStore for RedbStore {
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.healthy()?;
        let transaction = self.database.begin_read().map_err(io::Error::other)?;
        let table = transaction.open_table(CELLS).map_err(io::Error::other)?;
        let value = table
            .get(key)
            .map_err(io::Error::other)?
            .map(|value| value.value().to_vec());
        Ok(value)
    }
    fn scan(&mut self) -> io::Result<Rows> {
        self.healthy()?;
        let transaction = self.database.begin_read().map_err(io::Error::other)?;
        let table = transaction.open_table(CELLS).map_err(io::Error::other)?;
        let mut result = Vec::new();
        let mut bytes = 0;
        for row in table.iter().map_err(io::Error::other)? {
            let (key, value) = row.map_err(io::Error::other)?;
            bytes += key.value().len() + value.value().len();
            if bytes > 256 * 1024 * 1024 {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "materialized scan exceeds 256 MiB",
                ));
            }
            result.push((key.value().to_vec(), value.value().to_vec()));
        }
        Ok(result)
    }
    fn commit_range(
        &mut self,
        first: u64,
        index: u64,
        changes: &[Mutation],
    ) -> io::Result<CommitOutcome> {
        self.healthy()?;
        let logical = validate_changes(changes)?;
        let hash = digest(&encode_batch(first, index, changes)?);
        if index == self.applied && index > 0 && hash == self.last_digest {
            return Ok(CommitOutcome::Replay);
        }
        if self.applied.checked_add(1) != Some(first) {
            return Err(rejected("applied range must start at the next index"));
        }
        self.write(index, changes, hash, false)?;
        self.stats.commits += 1;
        self.stats.logical_bytes += logical;
        Ok(CommitOutcome::Applied)
    }
    fn install(&mut self, index: u64, cells: &[Mutation]) -> io::Result<()> {
        validate_install(cells)?;
        if cells.iter().any(|cell| cell.value.is_none()) || (index == 0 && !cells.is_empty()) {
            return Err(rejected(
                "snapshot cells must be live at a nonzero boundary",
            ));
        }
        self.write(index, cells, [0; 32], true)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.healthy()
    }
    fn statistics(&self) -> io::Result<Statistics> {
        let mut stats = self.stats.clone();
        stats.redb_write_bytes = self.counters.writes.load(Ordering::Relaxed);
        stats.content_syncs += self.counters.syncs.load(Ordering::Relaxed);
        stats.disk_bytes = self.dir.disk_bytes()?;
        Ok(stats)
    }
}
