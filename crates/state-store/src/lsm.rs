use crate::codec::{decode_batch, encode_batch, frame, read_frame};
use crate::durable::{sync_directory, Directory, Engine};
use crate::merge::{Merge, Source};
use crate::table::{write_table, Table, TableMeta};
use crate::{
    digest, invalid, rejected, validate_changes, validate_install, CommitOutcome, Mutation, Record,
    Rows, StateStore, Statistics, MAX_BATCH_RECORDS,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;

const WAL_MAGIC: &[u8; 8] = b"MRSWAL01";
const MANIFEST_MAGIC: &[u8; 8] = b"MRSMAN01";
const MAX_TABLES: usize = 256;
const MAX_MANIFEST_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LsmOptions {
    pub memtable_bytes: usize,
    pub target_table_bytes: usize,
    pub l0_trigger: usize,
    pub level1_bytes: u64,
    pub max_compaction_input_bytes: u64,
}
impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            memtable_bytes: 1024 * 1024,
            target_table_bytes: 512 * 1024,
            l0_trigger: 4,
            level1_bytes: 8 * 1024 * 1024,
            max_compaction_input_bytes: 64 * 1024 * 1024,
        }
    }
}
impl LsmOptions {
    fn validate(&self) -> io::Result<()> {
        if !(256..=16 * 1024 * 1024).contains(&self.memtable_bytes)
            || !(256..=16 * 1024 * 1024).contains(&self.target_table_bytes)
            || !(2..=32).contains(&self.l0_trigger)
            || self.level1_bytes < self.target_table_bytes as u64
            || !(1024..=256 * 1024 * 1024).contains(&self.max_compaction_input_bytes)
        {
            return Err(invalid("invalid LSM work bounds"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    WalWritten,
    WalSynced,
    TableWritten,
    TableSynced,
    NewWalSynced,
    FilesDirectorySynced,
    ManifestWritten,
    ManifestSynced,
    ManifestRenamed,
    ManifestDirectorySynced,
    ObsoleteRemoved,
    ReclaimDirectorySynced,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    applied: u64,
    last_digest: [u8; 32],
    next_id: u64,
    wal_id: u64,
    tables: Vec<TableMeta>,
}
impl Manifest {
    fn wal_name(&self) -> String {
        format!("wal-{:020}.wal", self.wal_id)
    }
    fn allocate(&mut self) -> io::Result<u64> {
        let id = self.next_id;
        self.next_id = id
            .checked_add(1)
            .ok_or_else(|| invalid("file identity exhausted"))?;
        Ok(id)
    }
    fn validate(&self) -> io::Result<()> {
        if self.version != 1
            || self.wal_id == 0
            || self.next_id <= self.wal_id
            || self.tables.len() > MAX_TABLES
        {
            return Err(invalid("invalid selected manifest"));
        }
        let mut ids = BTreeSet::new();
        ids.insert(self.wal_id);
        for table in &self.tables {
            if !ids.insert(table.id)
                || table.id >= self.next_id
                || table.max_sequence > self.applied
                || table.level > 2
            {
                return Err(invalid("manifest table identities/bounds invalid"));
            }
        }
        for level in 1..=2 {
            let mut tables: Vec<_> = self
                .tables
                .iter()
                .filter(|table| table.level == level)
                .collect();
            tables.sort_by(|left, right| left.first.cmp(&right.first));
            if tables.windows(2).any(|pair| pair[0].last >= pair[1].first) {
                return Err(invalid("leveled runs overlap"));
            }
        }
        Ok(())
    }
}

pub struct LsmStore {
    dir: Directory,
    options: LsmOptions,
    manifest: Manifest,
    tables: Vec<Table>,
    wal: File,
    wal_len: u64,
    mem: BTreeMap<Vec<u8>, Record>,
    mem_bytes: usize,
    applied: u64,
    last_digest: [u8; 32],
    stats: Statistics,
    poisoned: bool,
    fault: Option<FaultPoint>,
}

impl LsmStore {
    pub fn open(path: impl AsRef<Path>, options: LsmOptions) -> io::Result<Self> {
        options.validate()?;
        let dir = Directory::open(path.as_ref(), Engine::Lsm)?;
        let mut stats = Statistics {
            directory_syncs: dir.open_directory_syncs,
            content_syncs: dir.open_content_syncs,
            metadata_bytes: dir.open_metadata_bytes,
            ..Statistics::default()
        };
        let current = dir.path.join("CURRENT");
        if dir.fresh {
            if fs::read_dir(&dir.path)?
                .any(|entry| entry.is_ok_and(|entry| owned(&entry.file_name().to_string_lossy())))
            {
                return Err(invalid(
                    "owned files exist without CURRENT; refusing stale/new ambiguity",
                ));
            }
            let initial = Manifest {
                version: 1,
                applied: 0,
                last_digest: [0; 32],
                next_id: 2,
                wal_id: 1,
                tables: vec![],
            };
            let wal = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.path.join(initial.wal_name()))?;
            wal.sync_all()?;
            stats.content_syncs += 1;
            drop(wal);
            sync_directory(&dir.path)?;
            stats.directory_syncs += 1;
            let bytes = frame(
                MANIFEST_MAGIC,
                &serde_json::to_vec(&initial).map_err(io::Error::other)?,
            )?;
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(dir.path.join("CURRENT.new"))?;
            file.write_all(&bytes)?;
            stats.metadata_bytes += bytes.len() as u64;
            file.sync_all()?;
            stats.content_syncs += 1;
            drop(file);
            fs::rename(dir.path.join("CURRENT.new"), &current)?;
            sync_directory(&dir.path)?;
            stats.directory_syncs += 1;
        }
        let mut file = File::open(&current)?;
        let length = file.metadata()?.len();
        if length > MAX_MANIFEST_BYTES {
            return Err(invalid("manifest too large"));
        }
        let (payload, used) = read_frame(&mut file, MANIFEST_MAGIC)?
            .ok_or_else(|| invalid("incomplete selected manifest"))?;
        if used as u64 != length {
            return Err(invalid("manifest trailing bytes"));
        }
        let manifest: Manifest =
            serde_json::from_slice(&payload).map_err(|error| invalid(error.to_string()))?;
        manifest.validate()?;
        let mut tables = Vec::new();
        for meta in &manifest.tables {
            tables.push(Table::open(dir.path.join(meta.filename()), meta.clone())?);
        }
        let wal = OpenOptions::new()
            .read(true)
            .write(true)
            .open(dir.path.join(manifest.wal_name()))?;
        let mut store = Self {
            applied: manifest.applied,
            last_digest: manifest.last_digest,
            dir,
            options,
            manifest,
            tables,
            wal,
            wal_len: 0,
            mem: BTreeMap::new(),
            mem_bytes: 0,
            stats,
            poisoned: false,
            fault: None,
        };
        store.recover_wal()?;
        store.reclaim()?;
        Ok(store)
    }

    /// Deterministic syscall-boundary failure injection; the next matching
    /// boundary fails once and the handle must be reopened.
    pub fn inject_failure(&mut self, point: FaultPoint) {
        self.fault = Some(point);
    }
    fn hit(&mut self, point: FaultPoint) -> io::Result<()> {
        if self.fault == Some(point) {
            self.fault = None;
            return Err(io::Error::other(format!("injected LSM failure: {point:?}")));
        }
        Ok(())
    }
    fn healthy(&self) -> io::Result<()> {
        if self.poisoned {
            Err(io::Error::other(
                "state store requires reopen after durable mutation failure",
            ))
        } else {
            Ok(())
        }
    }
    fn mutation<T>(&mut self, action: impl FnOnce(&mut Self) -> io::Result<T>) -> io::Result<T> {
        self.healthy()?;
        match action(self) {
            Ok(value) => Ok(value),
            Err(error) => {
                self.poisoned = true;
                Err(error)
            }
        }
    }
    fn apply_mem(&mut self, sequence: u64, changes: &[Mutation]) {
        for change in changes {
            let record = Record {
                key: change.key.clone(),
                value: change.value.clone(),
                sequence,
            };
            self.mem_bytes += record.size();
            if let Some(previous) = self.mem.insert(record.key.clone(), record) {
                self.mem_bytes -= previous.size();
            }
        }
    }
    fn recover_wal(&mut self) -> io::Result<()> {
        let original = self.wal.metadata()?.len();
        if original > 64 * 1024 * 1024 {
            return Err(invalid("WAL recovery byte bound exceeded"));
        }
        while let Some((payload, bytes)) = read_frame(&mut self.wal, WAL_MAGIC)? {
            let (first, index, changes) = decode_batch(&payload)?;
            if self.applied.checked_add(1) != Some(first) {
                return Err(invalid("WAL applied-index discontinuity"));
            }
            self.apply_mem(index, &changes);
            self.applied = index;
            self.last_digest = digest(&payload);
            self.wal_len += bytes as u64;
        }
        if self.wal_len != original {
            self.wal.set_len(self.wal_len)?;
            self.wal.sync_all()?;
            self.stats.content_syncs += 1;
        }
        self.wal.seek(SeekFrom::End(0))?;
        Ok(())
    }
    fn sources(
        &self,
        selected: Option<&BTreeSet<u64>>,
        include_mem: bool,
    ) -> io::Result<Vec<Source>> {
        let mut sources: Vec<Source> = Vec::new();
        if include_mem {
            sources.push(Box::new(
                self.mem
                    .values()
                    .cloned()
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(Ok),
            ));
        }
        for table in &self.tables {
            if selected.is_none_or(|ids| ids.contains(&table.meta.id)) {
                sources.push(Box::new(table.iter()?));
            }
        }
        Ok(sources)
    }
    fn create_tables(
        &mut self,
        manifest: &mut Manifest,
        level: usize,
        rows: impl Iterator<Item = io::Result<Record>>,
        compaction: bool,
    ) -> io::Result<Vec<TableMeta>> {
        let mut result = Vec::new();
        let mut batch = Vec::new();
        let mut size = 0;
        for row in rows {
            let row = row?;
            if !batch.is_empty()
                && (size + row.size() > self.options.target_table_bytes
                    || batch.len() == MAX_BATCH_RECORDS)
            {
                result.push(self.create_table(manifest, level, &batch, compaction)?);
                batch.clear();
                size = 0;
            }
            size += row.size();
            batch.push(row);
            if result.len() >= MAX_TABLES {
                return Err(invalid("table count capacity exceeded"));
            }
        }
        if !batch.is_empty() {
            result.push(self.create_table(manifest, level, &batch, compaction)?);
        }
        Ok(result)
    }
    fn create_table(
        &mut self,
        manifest: &mut Manifest,
        level: usize,
        records: &[Record],
        compaction: bool,
    ) -> io::Result<TableMeta> {
        let id = manifest.allocate()?;
        let path = self.dir.path.join(format!("sst-{id:020}.sst"));
        let (meta, file) = write_table(&path, id, level, records)?;
        if compaction {
            self.stats.compaction_bytes += meta.bytes;
        } else {
            self.stats.flush_table_bytes += meta.bytes;
        }
        self.hit(FaultPoint::TableWritten)?;
        file.sync_all()?;
        self.stats.content_syncs += 1;
        self.hit(FaultPoint::TableSynced)?;
        Ok(meta)
    }
    fn publish(&mut self, mut manifest: Manifest) -> io::Result<()> {
        manifest.wal_id = manifest.allocate()?;
        manifest.applied = self.applied;
        manifest.last_digest = self.last_digest;
        manifest.validate()?;
        let new_wal = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(self.dir.path.join(manifest.wal_name()))?;
        new_wal.sync_all()?;
        self.stats.content_syncs += 1;
        self.hit(FaultPoint::NewWalSynced)?;
        let mut tables = Vec::new();
        for meta in &manifest.tables {
            tables.push(Table::open(
                self.dir.path.join(meta.filename()),
                meta.clone(),
            )?);
        }
        sync_directory(&self.dir.path)?;
        self.stats.directory_syncs += 1;
        self.hit(FaultPoint::FilesDirectorySynced)?;
        let bytes = frame(
            MANIFEST_MAGIC,
            &serde_json::to_vec(&manifest).map_err(io::Error::other)?,
        )?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(invalid("manifest capacity exceeded"));
        }
        let mut current = File::create(self.dir.path.join("CURRENT.new"))?;
        current.write_all(&bytes)?;
        self.stats.metadata_bytes += bytes.len() as u64;
        self.hit(FaultPoint::ManifestWritten)?;
        current.sync_all()?;
        self.stats.content_syncs += 1;
        self.hit(FaultPoint::ManifestSynced)?;
        drop(current);
        fs::rename(
            self.dir.path.join("CURRENT.new"),
            self.dir.path.join("CURRENT"),
        )?;
        self.hit(FaultPoint::ManifestRenamed)?;
        sync_directory(&self.dir.path)?;
        self.stats.directory_syncs += 1;
        self.hit(FaultPoint::ManifestDirectorySynced)?;
        self.wal = new_wal;
        self.wal_len = 0;
        self.manifest = manifest;
        self.tables = tables;
        self.mem.clear();
        self.mem_bytes = 0;
        self.reclaim()
    }
    fn reclaim(&mut self) -> io::Result<()> {
        let mut live = BTreeSet::new();
        live.insert(self.manifest.wal_name());
        live.extend(self.manifest.tables.iter().map(TableMeta::filename));
        let mut changed = false;
        for entry in fs::read_dir(&self.dir.path)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if owned(&name) && !live.contains(&name) {
                if !entry.file_type()?.is_file() {
                    return Err(invalid("reserved store name is not a regular file"));
                }
                fs::remove_file(entry.path())?;
                changed = true;
                self.hit(FaultPoint::ObsoleteRemoved)?;
            }
        }
        if changed {
            sync_directory(&self.dir.path)?;
            self.stats.directory_syncs += 1;
            self.hit(FaultPoint::ReclaimDirectorySynced)?;
        }
        Ok(())
    }
    fn flush_inner(&mut self) -> io::Result<()> {
        if self.applied == self.manifest.applied {
            return Ok(());
        }
        let mut next = self.manifest.clone();
        let records = self
            .mem
            .values()
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .map(Ok);
        let added = self.create_tables(&mut next, 0, records, false)?;
        next.tables.extend(added);
        self.publish(next)
    }
    fn compaction_plan(&self) -> Option<(usize, BTreeSet<u64>)> {
        let l0: Vec<_> = self
            .tables
            .iter()
            .filter(|table| table.meta.level == 0)
            .collect();
        let level1: u64 = self
            .tables
            .iter()
            .filter(|table| table.meta.level == 1)
            .map(|table| table.meta.bytes)
            .sum();
        let (sources, destination) = if l0.len() >= self.options.l0_trigger {
            // Publishing a compaction first flushes the active memtable. Drain
            // the selected level-zero set together: removing only one run can
            // replace it with that flush forever and starve lower levels.
            (l0, 1)
        } else if level1 > self.options.level1_bytes {
            (
                vec![self
                    .tables
                    .iter()
                    .filter(|table| table.meta.level == 1)
                    .min_by_key(|table| table.meta.id)?],
                2,
            )
        } else {
            return None;
        };
        let first = sources.iter().map(|table| &table.meta.first).min()?;
        let last = sources.iter().map(|table| &table.meta.last).max()?;
        let mut ids: BTreeSet<_> = sources.iter().map(|table| table.meta.id).collect();
        for table in &self.tables {
            if table.meta.level == destination && table.meta.overlaps(first, last) {
                ids.insert(table.meta.id);
            }
        }
        Some((destination, ids))
    }
    /// Execute one bounded leveled job. Returns false when no level is over its
    /// trigger. Compaction is synchronous; workloads measure its active cost.
    pub fn compact_once(&mut self) -> io::Result<bool> {
        self.mutation(|store| {
            store.flush_inner()?;
            let Some((destination, selected)) = store.compaction_plan() else {
                return Ok(false);
            };
            let input: u64 = store
                .tables
                .iter()
                .filter(|table| selected.contains(&table.meta.id))
                .map(|table| table.meta.bytes)
                .sum();
            if input > store.options.max_compaction_input_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "compaction input work limit exceeded",
                ));
            }
            let unselected: Vec<_> = store
                .tables
                .iter()
                .filter(|table| !selected.contains(&table.meta.id))
                .map(|table| table.meta.clone())
                .collect();
            let merged = Merge::new(store.sources(Some(&selected), false)?)?;
            let visible = merged.filter(move |result| match result {
                Err(_) => true,
                Ok(record) => {
                    record.value.is_some()
                        || unselected.iter().any(|table| table.contains(&record.key))
                }
            });
            let mut next = store.manifest.clone();
            next.tables.retain(|table| !selected.contains(&table.id));
            let added = store.create_tables(&mut next, destination, visible, true)?;
            next.tables.extend(added);
            store.publish(next)?;
            store.stats.compactions += 1;
            store.stats.compaction_input_bytes += input;
            Ok(true)
        })
    }
}

fn owned(name: &str) -> bool {
    if name == "CURRENT.new" {
        return true;
    }
    [("sst-", ".sst"), ("wal-", ".wal")]
        .iter()
        .any(|(prefix, suffix)| {
            name.strip_prefix(prefix)
                .and_then(|rest| rest.strip_suffix(suffix))
                .is_some_and(|digits| {
                    digits.len() == 20 && digits.bytes().all(|byte| byte.is_ascii_digit())
                })
        })
}

impl StateStore for LsmStore {
    fn applied_index(&self) -> u64 {
        self.applied
    }
    fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.healthy()?;
        let mut newest = self.mem.get(key).cloned();
        for table in &self.tables {
            if !table.meta.contains(key)
                || newest
                    .as_ref()
                    .is_some_and(|record| record.sequence > table.meta.max_sequence)
            {
                continue;
            }
            let lookup = table.lookup(key)?;
            self.stats.bloom_probes += 1;
            self.stats.bloom_positive += u64::from(lookup.bloom_positive);
            self.stats.bloom_false_positive +=
                u64::from(lookup.bloom_positive && lookup.record.is_none());
            self.stats.point_read_block_bytes += lookup.block_bytes;
            if let Some(record) = lookup.record {
                if let Some(previous) = &newest {
                    if previous.sequence == record.sequence && previous.value != record.value {
                        return Err(invalid("conflicting record version"));
                    }
                }
                if newest
                    .as_ref()
                    .is_none_or(|previous| previous.sequence < record.sequence)
                {
                    newest = Some(record);
                }
            }
        }
        Ok(newest.and_then(|record| record.value))
    }
    fn scan(&mut self) -> io::Result<Rows> {
        self.healthy()?;
        let mut result = Vec::new();
        let mut bytes = 0;
        for record in Merge::new(self.sources(None, true)?)? {
            let record = record?;
            if let Some(value) = record.value {
                bytes += record.key.len() + value.len();
                if bytes > 256 * 1024 * 1024 {
                    return Err(io::Error::new(
                        io::ErrorKind::OutOfMemory,
                        "materialized scan exceeds 256 MiB",
                    ));
                }
                result.push((record.key, value));
            }
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
        let payload = encode_batch(first, index, changes)?;
        let hash = digest(&payload);
        if index == self.applied && index > 0 && hash == self.last_digest {
            return Ok(CommitOutcome::Replay);
        }
        if self.applied.checked_add(1) != Some(first) {
            return Err(rejected("applied range must start at the next index"));
        }
        self.mutation(|store| {
            if store.mem_bytes >= store.options.memtable_bytes
                || store.wal_len >= (store.options.memtable_bytes * 4).min(32 * 1024 * 1024) as u64
            {
                store.flush_inner()?;
            }
            let named_wal = fs::metadata(store.dir.path.join(store.manifest.wal_name()))?;
            if named_wal.len() != store.wal_len || store.wal.metadata()?.len() != store.wal_len {
                return Err(invalid("active WAL changed outside its writer"));
            }
            let bytes = frame(WAL_MAGIC, &payload)?;
            store.wal.write_all(&bytes)?;
            store.wal_len += bytes.len() as u64;
            store.stats.wal_bytes += bytes.len() as u64;
            store.hit(FaultPoint::WalWritten)?;
            store.wal.sync_all()?;
            store.stats.content_syncs += 1;
            store.hit(FaultPoint::WalSynced)?;
            store.apply_mem(index, changes);
            store.applied = index;
            store.last_digest = hash;
            store.stats.commits += 1;
            store.stats.logical_bytes += logical;
            Ok(CommitOutcome::Applied)
        })
    }
    fn install(&mut self, index: u64, cells: &[Mutation]) -> io::Result<()> {
        validate_install(cells)?;
        if cells.iter().any(|cell| cell.value.is_none()) || (index == 0 && !cells.is_empty()) {
            return Err(rejected(
                "snapshot cells must be live at a nonzero boundary",
            ));
        }
        self.mutation(|store| {
            let mut rows: Vec<_> = cells
                .iter()
                .map(|cell| Record {
                    key: cell.key.clone(),
                    value: cell.value.clone(),
                    sequence: index,
                })
                .collect();
            rows.sort_by(|left, right| left.key.cmp(&right.key));
            let mut next = store.manifest.clone();
            next.tables.clear();
            next.tables = store.create_tables(&mut next, 2, rows.into_iter().map(Ok), false)?;
            store.applied = index;
            store.last_digest = [0; 32];
            store.publish(next)
        })
    }
    fn flush(&mut self) -> io::Result<()> {
        self.mutation(Self::flush_inner)
    }
    fn statistics(&self) -> io::Result<Statistics> {
        let mut stats = self.stats.clone();
        stats.disk_bytes = self.dir.disk_bytes()?;
        for table in &self.tables {
            stats.tables_per_level[table.meta.level] += 1;
        }
        stats.pending_compaction = self.compaction_plan().is_some();
        Ok(stats)
    }
    fn maintain(&mut self) -> io::Result<bool> {
        self.healthy()?;
        if self.compaction_plan().is_some() {
            self.compact_once()
        } else {
            Ok(false)
        }
    }
}
