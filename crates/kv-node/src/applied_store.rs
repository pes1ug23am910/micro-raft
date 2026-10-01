//! Atomic derived application cells, separate from the consensus WAL.
//!
//! Reads use the explicitly retained in-memory application cache. These engine
//! statistics therefore do not describe the HTTP read path or its Bloom behavior.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use raft_core::{Command, Entry, LogIndex, NodeId, RaftNode, Term};
use serde::{Deserialize, Serialize};
use state_store::{LsmOptions, LsmStore, Mutation, RedbStore, StateStore};

use crate::application::{
    ApplicationSnapshot, SessionKeySnapshot, SessionResult, SessionSnapshot, StateMachine,
};
use crate::durability;
use crate::snapshot::SnapshotImage;

const IDENTITY_FILE: &str = "application-identity.json";
const IDENTITY_TEMP: &str = "application-identity.json.new";
const META_KEY: &[u8] = b"m/state";

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, clap::ValueEnum)]
#[serde(rename_all = "lowercase")]
pub enum StateBackend {
    #[default]
    Memory,
    Lsm,
    Redb,
}
impl StateBackend {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Lsm => "lsm",
            Self::Redb => "redb",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationIdentity {
    pub group: String,
    pub genesis_voters: Vec<NodeId>,
}
impl ApplicationIdentity {
    /// Compatibility name for the durable consensus identity. Legacy application
    /// directories retain their original "legacy" label; explicit groups use the
    /// exact persisted group id and immutable genesis, never dynamic peer routes.
    pub fn legacy(node: &RaftNode) -> Self {
        let authority = &node.committed_membership().state;
        Self {
            group: if authority.group_id == raft_core::membership::LEGACY_GROUP_ID {
                "legacy".into()
            } else {
                authority.group_id.clone()
            },
            genesis_voters: authority.genesis_voters.clone(),
        }
    }
    fn validate(&self) -> io::Result<()> {
        if self.group.is_empty()
            || self.group.len() > 128
            || !self.group.bytes().all(|byte| byte.is_ascii_graphic())
            || self.genesis_voters.is_empty()
            || !self.genesis_voters.windows(2).all(|pair| pair[0] < pair[1])
        {
            return Err(invalid("invalid application group/genesis identity"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    version: u32,
    backend: StateBackend,
    identity: ApplicationIdentity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    version: u32,
    identity: ApplicationIdentity,
    applied_index: LogIndex,
    applied_term: Term,
    retained_keys: usize,
    retained_payload_bytes: usize,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionHeader {
    registered: SessionResult,
    closed: Option<SessionResult>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct AppliedStoreStatus {
    pub backend: StateBackend,
    pub applied_index: LogIndex,
    pub durable: bool,
    pub statistics: serde_json::Value,
}

pub struct AppliedStore {
    backend: StateBackend,
    identity: ApplicationIdentity,
    engine: Option<Box<dyn StateStore>>,
    index: LogIndex,
    term: Term,
    poisoned: bool,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn json<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(io::Error::other)
}
fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> io::Result<T> {
    serde_json::from_slice(bytes).map_err(|error| invalid(error.to_string()))
}
fn text(bytes: &[u8]) -> io::Result<String> {
    String::from_utf8(bytes.to_vec()).map_err(|_| invalid("non-UTF8 application cell"))
}
fn named(prefix: &[u8], key: &str) -> Vec<u8> {
    [prefix, key.as_bytes()].concat()
}
fn session_key(prefix: &[u8], id: u64, key: Option<&str>) -> Vec<u8> {
    let mut encoded = [prefix, &id.to_be_bytes()].concat();
    if let Some(key) = key {
        encoded.extend_from_slice(key.as_bytes());
    }
    encoded
}
fn session_id(bytes: &[u8]) -> io::Result<u64> {
    Ok(u64::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| invalid("invalid session identity cell"))?,
    ))
}

fn metadata(app: &StateMachine, identity: &ApplicationIdentity, term: Term) -> Metadata {
    let (retained_keys, retained_payload_bytes) = app.retained_counts();
    Metadata {
        version: 1,
        identity: identity.clone(),
        applied_index: app.last_applied(),
        applied_term: term,
        retained_keys,
        retained_payload_bytes,
    }
}

/// Only the cells an entry can affect are encoded. Metadata is always written,
/// including for rejected commands and retry replays, to advance atomically.
fn entry_cells(
    app: &StateMachine,
    identity: &ApplicationIdentity,
    entry: &Entry,
) -> io::Result<Vec<Mutation>> {
    let mut cells = vec![Mutation::put(
        META_KEY,
        json(&metadata(app, identity, entry.term))?,
    )];
    let mut value = |key: &str| {
        cells.push(Mutation {
            key: named(b"v/", key),
            value: app.values().get(key).map(|value| value.as_bytes().to_vec()),
        });
    };
    match &entry.command {
        Command::Put { key, .. } | Command::Delete { key } => value(key),
        Command::SessionPut {
            session_id, key, ..
        }
        | Command::SessionDelete {
            session_id, key, ..
        } => {
            if let Some(record) = app.session_key(*session_id, key) {
                value(key);
                cells.push(Mutation::put(
                    session_key(b"k/", *session_id, Some(key)),
                    json(&record)?,
                ));
            }
        }
        Command::RegisterSession { nonce } => {
            if let Some(id) = app.registration_id(nonce) {
                cells.push(Mutation::put(named(b"r/", nonce), id.to_be_bytes()));
                let (registered, closed) = app
                    .session_header(id)
                    .ok_or_else(|| invalid("registration lacks session header"))?;
                cells.push(Mutation::put(
                    session_key(b"s/", id, None),
                    json(&SessionHeader { registered, closed })?,
                ));
            }
        }
        Command::CloseSession { session_id } => {
            if let Some((registered, closed)) = app.session_header(*session_id) {
                cells.push(Mutation::put(
                    session_key(b"s/", *session_id, None),
                    json(&SessionHeader { registered, closed })?,
                ));
            }
        }
        Command::NoOp | Command::Configuration(_) => {}
    }
    Ok(cells)
}

fn full_cells(
    app: &StateMachine,
    identity: &ApplicationIdentity,
    term: Term,
) -> io::Result<Vec<Mutation>> {
    let snapshot = app.export_snapshot();
    let mut cells = vec![Mutation::put(
        META_KEY,
        json(&metadata(app, identity, term))?,
    )];
    for (key, value) in snapshot.values {
        cells.push(Mutation::put(named(b"v/", &key), value.into_bytes()));
    }
    for session in snapshot.sessions {
        cells.push(Mutation::put(
            named(b"r/", &session.nonce),
            session.session_id.to_be_bytes(),
        ));
        cells.push(Mutation::put(
            session_key(b"s/", session.session_id, None),
            json(&SessionHeader {
                registered: session.registered,
                closed: session.closed,
            })?,
        ));
        for record in session.keys {
            cells.push(Mutation::put(
                session_key(b"k/", session.session_id, Some(&record.key)),
                json(&record)?,
            ));
        }
    }
    Ok(cells)
}

fn decode_cells(
    rows: state_store::Rows,
    applied: u64,
    identity: &ApplicationIdentity,
) -> io::Result<(StateMachine, Term)> {
    if applied == 0 {
        if !rows.is_empty() {
            return Err(invalid("zero engine watermark has application cells"));
        }
        return Ok((StateMachine::default(), 0));
    }
    let mut meta = None;
    let mut values = Vec::new();
    let mut registrations = BTreeMap::new();
    let mut headers = BTreeMap::new();
    let mut keys: BTreeMap<u64, Vec<SessionKeySnapshot>> = BTreeMap::new();
    let mut seen = std::collections::BTreeSet::new();
    for (key, value) in rows {
        if !seen.insert(key.clone()) {
            return Err(invalid("duplicate application cell"));
        }
        if key == META_KEY {
            meta = Some(parse::<Metadata>(&value)?);
        } else if let Some(key) = key.strip_prefix(b"v/") {
            values.push((text(key)?, text(&value)?));
        } else if let Some(key) = key.strip_prefix(b"r/") {
            if registrations
                .insert(session_id(&value)?, text(key)?)
                .is_some()
            {
                return Err(invalid("two nonces share one session"));
            }
        } else if let Some(key) = key.strip_prefix(b"s/") {
            headers.insert(session_id(key)?, parse::<SessionHeader>(&value)?);
        } else if let Some(key) = key.strip_prefix(b"k/") {
            if key.len() <= 8 {
                return Err(invalid("incomplete retry record key"));
            }
            let id = session_id(&key[..8])?;
            let record = parse::<SessionKeySnapshot>(&value)?;
            if record.key != text(&key[8..])? {
                return Err(invalid("retry record key differs from its cell key"));
            }
            keys.entry(id).or_default().push(record);
        } else {
            return Err(invalid("unknown application namespace"));
        }
    }
    let meta = meta.ok_or_else(|| invalid("engine lacks application metadata"))?;
    if meta.version != 1
        || meta.applied_index != applied
        || &meta.identity != identity
        || meta.applied_term == 0
    {
        return Err(invalid("engine application identity/watermark mismatch"));
    }
    let mut sessions = Vec::new();
    for (id, header) in headers {
        let nonce = registrations
            .remove(&id)
            .ok_or_else(|| invalid("session header lacks registration"))?;
        sessions.push(SessionSnapshot {
            nonce,
            session_id: id,
            registered: header.registered,
            closed: header.closed,
            keys: keys.remove(&id).unwrap_or_default(),
        });
    }
    if !registrations.is_empty() || !keys.is_empty() {
        return Err(invalid("orphan registration/retry record"));
    }
    let app = StateMachine::import_snapshot(
        ApplicationSnapshot {
            schema_version: 1,
            last_applied: applied,
            values,
            sessions,
            retained_keys: meta.retained_keys,
            retained_payload_bytes: meta.retained_payload_bytes,
        },
        applied,
    )
    .map_err(|error| invalid(error.to_string()))?;
    Ok((app, meta.applied_term))
}

/// Read-only preflight before publishing a new core identity. Neither endpoint
/// routes nor an application marker may override persisted consensus identity.
pub fn preflight_identity(
    dir: &Path,
    backend: StateBackend,
    identity: &ApplicationIdentity,
    fresh: bool,
    migrate_legacy: bool,
) -> io::Result<()> {
    identity.validate()?;
    match std::fs::read(dir.join(IDENTITY_FILE)) {
        Ok(bytes) => {
            let existing: Binding = parse(&bytes)?;
            let wanted = Binding {
                version: 1,
                backend,
                identity: identity.clone(),
            };
            if existing != wanted
                && !(migrate_legacy
                    && existing.version == 1
                    && existing.backend == backend
                    && existing.identity.group == "legacy"
                    && existing.identity.genesis_voters == identity.genesis_voters
                    && identity.group != "legacy")
            {
                return Err(invalid(
                    "application binding differs from requested core identity/backend",
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if !fresh && backend != StateBackend::Memory {
                return Err(invalid(
                    "existing directory cannot silently adopt an unbound persistent backend",
                ));
            }
        }
        Err(error) => return Err(error),
    }
    Ok(())
}

impl AppliedStore {
    /// Bind backend selection before listeners open. Existing initialized legacy
    /// directories can adopt memory; a persistent engine starts in a fresh node
    /// directory. Backend migration is deliberately an explicit future operation.
    pub fn open(
        dir: &Path,
        backend: StateBackend,
        identity: ApplicationIdentity,
        node: &mut RaftNode,
        snapshot: Option<&SnapshotImage>,
    ) -> io::Result<(Self, StateMachine)> {
        Self::open_for_group(dir, backend, identity, node, snapshot, false, false)
    }

    /// Startup-only explicit group adoption. `fresh_directory` is captured before
    /// publishing core identity; `migrate_legacy` is an explicit operator flag.
    /// The core barrier must precede this engine transaction and binding update.
    #[allow(clippy::too_many_arguments)]
    pub fn open_for_group(
        dir: &Path,
        backend: StateBackend,
        identity: ApplicationIdentity,
        node: &mut RaftNode,
        snapshot: Option<&SnapshotImage>,
        fresh_directory: bool,
        migrate_legacy: bool,
    ) -> io::Result<(Self, StateMachine)> {
        identity.validate()?;
        if identity != ApplicationIdentity::legacy(node) {
            return Err(invalid("identity differs from durable consensus authority"));
        }
        let initialized = ["hardstate.json", "log.jsonl", "CURRENT"]
            .iter()
            .try_fold(false, |found, name| {
                dir.join(name).try_exists().map(|exists| found || exists)
            })?;
        let wanted = Binding {
            version: 1,
            backend,
            identity: identity.clone(),
        };
        let legacy_identity = ApplicationIdentity {
            group: "legacy".into(),
            genesis_voters: identity.genesis_voters.clone(),
        };
        let mut finish_migration = false;
        let mut source_identity = identity.clone();
        match std::fs::read(dir.join(IDENTITY_FILE)) {
            Ok(bytes) => {
                let existing: Binding = parse(&bytes)?;
                if existing != wanted {
                    if !migrate_legacy
                        || identity.group == "legacy"
                        || existing.version != 1
                        || existing.backend != backend
                        || existing.identity != legacy_identity
                        || !node.committed_membership().state.records.is_empty()
                    {
                        return Err(invalid(
                            "application backend/group identity changed without migration",
                        ));
                    }
                    source_identity = legacy_identity;
                    finish_migration = true;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                if initialized && !fresh_directory && backend != StateBackend::Memory {
                    return Err(invalid("persistent backend adoption requires a fresh node directory or explicit migration"));
                }
                durability::publish_file(dir, IDENTITY_TEMP, IDENTITY_FILE, &json(&wanted)?)?;
            }
            Err(error) => return Err(error),
        }
        let mut engine: Option<Box<dyn StateStore>> = match backend {
            StateBackend::Memory => None,
            StateBackend::Lsm | StateBackend::Redb => {
                let path = dir.join(format!("state-{}", backend.as_str()));
                if initialized && !fresh_directory && !path.try_exists()? {
                    return Err(invalid("selected persistent engine directory is missing"));
                }
                Some(match backend {
                    StateBackend::Lsm => Box::new(LsmStore::open(path, LsmOptions::default())?),
                    StateBackend::Redb => Box::new(RedbStore::open(path)?),
                    StateBackend::Memory => unreachable!(),
                })
            }
        };
        let mut app =
            snapshot.map_or_else(StateMachine::default, |image| image.application().clone());
        let mut term = node.snapshot_term();
        if let Some(engine) = engine.as_mut() {
            let applied = engine.applied_index();
            let rows = engine.scan()?;
            // A crash may leave the new engine transaction selected while the
            // old binding remains. Only the exact explicit migration may resume.
            if finish_migration && applied > 0 {
                let meta: Metadata = parse(
                    rows.iter()
                        .find(|(key, _)| key == META_KEY)
                        .ok_or_else(|| invalid("engine lacks migration metadata"))?
                        .1
                        .as_slice(),
                )?;
                if meta.identity == identity {
                    source_identity = identity.clone();
                }
            }
            let (recovered, recovered_term) = decode_cells(rows, applied, &source_identity)?;
            if applied < node.snapshot_index() {
                // CURRENT may have published the snapshot before the engine
                // installation barrier. That selected image is authoritative.
                engine.install(app.last_applied(), &full_cells(&app, &identity, term)?)?;
            } else {
                if node.log_term(applied) != Some(recovered_term) {
                    return Err(invalid(
                        "engine applied term/index differs from recovered Raft log",
                    ));
                }
                for index in node.snapshot_index() + 1..=applied {
                    let entry = node
                        .entry_at(index)
                        .ok_or_else(|| invalid("engine watermark crosses absent Raft entry"))?;
                    app.apply(entry)
                        .map_err(|error| invalid(error.to_string()))?;
                }
                if app != recovered {
                    return Err(invalid(
                        "engine state differs from deterministic snapshot/WAL replay",
                    ));
                }
                app = recovered;
                term = recovered_term;
            }
            node.restore_applied_watermark(app.last_applied(), term)
                .map_err(|error| invalid(error.to_string()))?;
            if finish_migration && source_identity != identity && app.last_applied() > 0 {
                engine.install(app.last_applied(), &full_cells(&app, &identity, term)?)?;
            }
        }
        if finish_migration {
            durability::publish_file(dir, IDENTITY_TEMP, IDENTITY_FILE, &json(&wanted)?)?;
        }
        Ok((
            Self {
                backend,
                identity,
                engine,
                index: app.last_applied(),
                term,
                poisoned: false,
            },
            app,
        ))
    }

    pub(crate) fn cells(&self, app: &StateMachine, entry: &Entry) -> io::Result<Vec<Mutation>> {
        entry_cells(app, &self.identity, entry)
    }
    pub(crate) fn commit(
        &mut self,
        first: LogIndex,
        index: LogIndex,
        term: Term,
        cells: &[Mutation],
    ) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "application engine requires reopen after I/O failure",
            ));
        }
        if self.index.checked_add(1) != Some(first) {
            return Err(invalid(
                "application range does not follow engine watermark",
            ));
        }
        if let Some(engine) = self.engine.as_mut() {
            if let Err(error) = engine.commit_range(first, index, cells) {
                self.poisoned = true;
                return Err(error);
            }
        }
        self.index = index;
        self.term = term;
        Ok(())
    }
    pub(crate) fn install(&mut self, image: &SnapshotImage) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "application engine requires reopen after I/O failure",
            ));
        }
        let descriptor = image.descriptor();
        let matches_identity = match &descriptor.metadata.membership {
            Some(membership) => {
                let group = if membership.state.group_id == raft_core::membership::LEGACY_GROUP_ID {
                    "legacy"
                } else {
                    &membership.state.group_id
                };
                group == self.identity.group
                    && membership.state.genesis_voters == self.identity.genesis_voters
            }
            None => descriptor.metadata.members == self.identity.genesis_voters,
        };
        if !matches_identity || descriptor.metadata.last_included_index <= self.index {
            return Err(invalid(
                "snapshot does not advance this application's identity/boundary",
            ));
        }
        if let Some(engine) = self.engine.as_mut() {
            let cells = full_cells(
                image.application(),
                &self.identity,
                descriptor.metadata.last_included_term,
            )?;
            if let Err(error) = engine.install(descriptor.metadata.last_included_index, &cells) {
                self.poisoned = true;
                return Err(error);
            }
        }
        self.index = descriptor.metadata.last_included_index;
        self.term = descriptor.metadata.last_included_term;
        Ok(())
    }
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        if self.poisoned {
            return Err(io::Error::other(
                "application engine requires reopen after I/O failure",
            ));
        }
        self.engine.as_mut().map_or(Ok(()), |engine| engine.flush())
    }
    pub(crate) fn maintain(&mut self) -> io::Result<bool> {
        if self.poisoned {
            return Err(io::Error::other(
                "application engine requires reopen after I/O failure",
            ));
        }
        self.engine
            .as_mut()
            .map_or(Ok(false), |engine| engine.maintain())
    }
    pub(crate) fn status(&self) -> io::Result<AppliedStoreStatus> {
        let statistics = match &self.engine {
            Some(engine) => serde_json::to_value(engine.statistics()?).map_err(io::Error::other)?,
            None => serde_json::Value::Null,
        };
        Ok(AppliedStoreStatus {
            backend: self.backend,
            applied_index: self.index,
            durable: self.engine.is_some(),
            statistics,
        })
    }
}

/// Check/coalesce a candidate entry without mutating the existing prefix when
/// the next entry would exceed the engine transaction budget.
pub(crate) fn merge_cells(
    current: &mut BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    bytes: &mut usize,
    changes: Vec<Mutation>,
) -> io::Result<bool> {
    let mut size = *bytes;
    let mut seen = std::collections::BTreeSet::new();
    let mut count = current.len();
    for change in &changes {
        if !seen.insert(&change.key)
            || change.key.is_empty()
            || change.key.len() > state_store::MAX_KEY_BYTES
            || change
                .value
                .as_ref()
                .is_some_and(|value| value.len() > state_store::MAX_VALUE_BYTES)
        {
            return Err(invalid("application cell exceeds engine bounds"));
        }
        if let Some(previous) = current.get(&change.key) {
            size = size
                .checked_sub(change.key.len() + previous.as_ref().map_or(0, Vec::len))
                .ok_or_else(|| invalid("inconsistent accumulated cell size"))?;
        } else {
            count += 1;
        }
        size = size
            .checked_add(change.key.len() + change.value.as_ref().map_or(0, Vec::len))
            .ok_or_else(|| invalid("cell byte count overflow"))?;
    }
    if size > state_store::MAX_BATCH_BYTES || count > state_store::MAX_BATCH_RECORDS {
        return Ok(false);
    }
    for change in changes {
        current.insert(change.key, change.value);
    }
    *bytes = size;
    Ok(true)
}

#[cfg(test)]
pub(crate) fn injected(engine: Box<dyn StateStore>, node: &RaftNode) -> AppliedStore {
    AppliedStore {
        backend: StateBackend::Lsm,
        identity: ApplicationIdentity::legacy(node),
        index: engine.applied_index(),
        term: node.log_term(engine.applied_index()).unwrap_or(0),
        engine: Some(engine),
        poisoned: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::application::ApplyOutcome;
    use crate::kv::SharedReadState;
    use crate::storage::Storage;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "micro-raft-engine-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
    fn entry(index: u64, command: Command) -> Entry {
        Entry {
            index,
            term: 1,
            command,
        }
    }
    fn put(index: u64, key: &str, value: &str) -> Entry {
        entry(
            index,
            Command::Put {
                key: key.into(),
                value: value.into(),
            },
        )
    }
    fn commands() -> Vec<Entry> {
        vec![
            entry(
                1,
                Command::RegisterSession {
                    nonce: "retry".into(),
                },
            ),
            entry(
                2,
                Command::SessionPut {
                    session_id: 1,
                    key: "key".into(),
                    sequence: 1,
                    value: "original".into(),
                },
            ),
            put(3, "key", "independent"),
            entry(
                4,
                Command::SessionPut {
                    session_id: 1,
                    key: "key".into(),
                    sequence: 1,
                    value: "original".into(),
                },
            ),
            entry(
                5,
                Command::SessionDelete {
                    session_id: 1,
                    key: "deleted".into(),
                    sequence: 1,
                },
            ),
            entry(6, Command::CloseSession { session_id: 1 }),
        ]
    }
    fn restored(entries: Vec<Entry>, snapshot: Option<&SnapshotImage>) -> RaftNode {
        RaftNode::restore_with_snapshot(
            1,
            vec![],
            7,
            raft_core::HardState {
                membership: None,
                current_term: 1,
                voted_for: None,
            },
            snapshot.map(|image| image.descriptor().clone()),
            entries,
        )
        .unwrap()
    }
    fn open(
        dir: &Path,
        backend: StateBackend,
        node: &mut RaftNode,
        snapshot: Option<&SnapshotImage>,
    ) -> (AppliedStore, StateMachine) {
        AppliedStore::open(
            dir,
            backend,
            ApplicationIdentity::legacy(node),
            node,
            snapshot,
        )
        .unwrap()
    }

    #[test]
    fn both_engines_atomically_reopen_values_sessions_delete_closed_metadata_and_watermark() {
        for backend in [StateBackend::Lsm, StateBackend::Redb] {
            let dir = Directory::new();
            let entries = commands();
            let mut node = restored(entries.clone(), None);
            let (mut store, app) = open(&dir.0, backend, &mut node, None);
            let shared = SharedReadState::from_application(&node, app).unwrap();
            let outcomes = shared.apply_group(&entries, &mut store).unwrap();
            assert_eq!(outcomes.len(), 6);
            assert_eq!(
                outcomes[1].1, outcomes[3].1,
                "retry preserves original response index"
            );
            assert_eq!(shared.get("key").1.as_deref(), Some("independent"));
            assert_eq!(store.status().unwrap().statistics["commits"], 1);
            drop(store);
            let mut reopened = restored(entries, None);
            let (store, app) = open(&dir.0, backend, &mut reopened, None);
            assert_eq!((reopened.commit_index, reopened.last_applied), (6, 6));
            assert_eq!(app.session_header(1).unwrap().1.unwrap().index, Some(6));
            assert!(app.session_key(1, "deleted").is_some());
            assert_eq!(
                app.values().get("key").map(String::as_str),
                Some("independent")
            );
            assert_eq!(store.status().unwrap().applied_index, 6);
        }
    }

    #[test]
    fn backend_and_genesis_cannot_change_and_legacy_persistent_adoption_is_explicitly_rejected() {
        let dir = Directory::new();
        let mut node = RaftNode::new(1, vec![], 7);
        let (store, _) = open(&dir.0, StateBackend::Lsm, &mut node, None);
        drop(store);
        assert!(AppliedStore::open(
            &dir.0,
            StateBackend::Redb,
            ApplicationIdentity::legacy(&node),
            &mut node,
            None
        )
        .is_err());
        let mut other = RaftNode::new(1, vec![2], 8);
        assert!(AppliedStore::open(
            &dir.0,
            StateBackend::Lsm,
            ApplicationIdentity::legacy(&other),
            &mut other,
            None
        )
        .is_err());
        let legacy = Directory::new();
        let (mut disk, _, _) = Storage::open(&legacy.0).unwrap();
        disk.save_hard_state(&raft_core::HardState::default())
            .unwrap();
        assert!(AppliedStore::open(
            &legacy.0,
            StateBackend::Lsm,
            ApplicationIdentity::legacy(&node),
            &mut node,
            None
        )
        .is_err());
        assert!(!legacy.0.join(IDENTITY_FILE).exists());
        open(&legacy.0, StateBackend::Memory, &mut node, None);
    }

    #[test]
    fn current_snapshot_repairs_crash_between_raft_publication_and_engine_install() {
        for backend in [StateBackend::Lsm, StateBackend::Redb] {
            let dir = Directory::new();
            let entries = commands();
            let mut old = restored(entries.clone(), None);
            let (mut store, app) = open(&dir.0, backend, &mut old, None);
            let shared = SharedReadState::from_application(&old, app).unwrap();
            shared.apply_group(&entries[..2], &mut store).unwrap();
            drop(store);
            let mut app = StateMachine::default();
            for entry in &entries {
                app.apply(entry).unwrap();
            }
            let image = SnapshotImage::new(
                raft_core::SnapshotMetadata {
                    membership: None,
                    last_included_index: 6,
                    last_included_term: 1,
                    members: vec![1],
                },
                &app,
            )
            .unwrap();
            let mut node = restored(vec![], Some(&image));
            let (store, recovered) = open(&dir.0, backend, &mut node, Some(&image));
            assert_eq!(recovered, app);
            assert_eq!(store.index, 6);
            drop(store);
            let mut again = restored(vec![], Some(&image));
            let (_, recovered) = open(&dir.0, backend, &mut again, Some(&image));
            assert_eq!(recovered, app);
        }
    }

    #[test]
    fn engine_watermark_and_state_must_match_raft_not_merely_decode() {
        for backend in [StateBackend::Lsm, StateBackend::Redb] {
            let dir = Directory::new();
            let mut node = restored(vec![put(1, "key", "one")], None);
            let (mut store, app) = open(&dir.0, backend, &mut node, None);
            let shared = SharedReadState::from_application(&node, app).unwrap();
            shared.apply_group(&node.log, &mut store).unwrap();
            drop(store);
            for entries in [
                vec![],
                vec![put(1, "key", "changed")],
                vec![Entry {
                    index: 1,
                    term: 2,
                    command: Command::NoOp,
                }],
            ] {
                let mut wrong = RaftNode::restore(
                    1,
                    vec![],
                    8,
                    raft_core::HardState {
                        membership: None,
                        current_term: 2,
                        voted_for: None,
                    },
                    entries,
                )
                .unwrap();
                assert!(AppliedStore::open(
                    &dir.0,
                    backend,
                    ApplicationIdentity::legacy(&wrong),
                    &mut wrong,
                    None
                )
                .is_err());
            }
            let mut correct = restored(vec![put(1, "key", "one")], None);
            open(&dir.0, backend, &mut correct, None);
        }
    }

    #[test]
    fn canonical_cells_reject_orphans_bad_counters_key_mismatch_and_unknown_namespaces() {
        let mut app = StateMachine::default();
        for entry in commands() {
            app.apply(&entry).unwrap();
        }
        let identity = ApplicationIdentity::legacy(&RaftNode::new(1, vec![], 7));
        let rows: Vec<_> = full_cells(&app, &identity, 1)
            .unwrap()
            .into_iter()
            .map(|cell| (cell.key, cell.value.unwrap()))
            .collect();
        assert_eq!(decode_cells(rows.clone(), 6, &identity).unwrap().0, app);
        let mut variants = Vec::new();
        let mut bad = rows.clone();
        bad.retain(|(key, _)| !key.starts_with(b"r/"));
        variants.push(bad);
        let mut bad = rows.clone();
        bad.push((b"foreign".to_vec(), vec![]));
        variants.push(bad);
        let mut bad = rows.clone();
        let cell = bad.iter_mut().find(|(key, _)| key == META_KEY).unwrap();
        let mut meta: Metadata = parse(&cell.1).unwrap();
        meta.retained_keys += 1;
        cell.1 = json(&meta).unwrap();
        variants.push(bad);
        let mut bad = rows.clone();
        bad.iter_mut()
            .find(|(key, _)| key.starts_with(b"k/"))
            .unwrap()
            .0
            .push(b'X');
        variants.push(bad);
        let mut bad = rows;
        bad.push(bad[0].clone());
        variants.push(bad);
        for bad in variants {
            assert!(decode_cells(bad, 6, &identity).is_err());
        }
    }

    #[derive(Default)]
    struct Probe {
        calls: Vec<(u64, u64)>,
        fail: bool,
    }
    struct Engine(Arc<Mutex<Probe>>);
    impl StateStore for Engine {
        fn applied_index(&self) -> u64 {
            0
        }
        fn get(&mut self, _: &[u8]) -> io::Result<Option<Vec<u8>>> {
            unreachable!()
        }
        fn scan(&mut self) -> io::Result<state_store::Rows> {
            unreachable!()
        }
        fn commit_range(
            &mut self,
            first: u64,
            last: u64,
            _: &[Mutation],
        ) -> io::Result<state_store::CommitOutcome> {
            let mut probe = self.0.lock().unwrap();
            probe.calls.push((first, last));
            if probe.fail {
                Err(io::Error::other("injected atomic engine failure"))
            } else {
                Ok(state_store::CommitOutcome::Applied)
            }
        }
        fn install(&mut self, _: u64, _: &[Mutation]) -> io::Result<()> {
            unreachable!()
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
        fn statistics(&self) -> io::Result<state_store::Statistics> {
            Ok(Default::default())
        }
    }

    #[test]
    fn failed_engine_commit_rolls_back_whole_range_and_poisons_further_mutations() {
        let node = RaftNode::new(1, vec![], 7);
        let shared = SharedReadState::from_node(&node);
        let probe = Arc::new(Mutex::new(Probe {
            fail: true,
            ..Default::default()
        }));
        let mut store = injected(Box::new(Engine(probe.clone())), &node);
        assert!(shared.apply_group(&commands(), &mut store).is_err());
        assert_eq!(shared.status().last_applied, 0);
        assert!(shared.snapshot().values.is_empty());
        assert!(shared.apply_group(&commands(), &mut store).is_err());
        assert_eq!(probe.lock().unwrap().calls, vec![(1, 6)]);
        assert!(store.flush().is_err());
    }

    #[test]
    fn apply_range_splits_on_actual_coalesced_byte_budget_without_exposing_next_entry() {
        let node = RaftNode::new(1, vec![], 7);
        let shared = SharedReadState::from_node(&node);
        let probe = Arc::new(Mutex::new(Probe::default()));
        let mut store = injected(Box::new(Engine(probe.clone())), &node);
        let mut entries = vec![entry(
            1,
            Command::RegisterSession {
                nonce: "large".into(),
            },
        )];
        for index in 2..=100 {
            entries.push(entry(
                index,
                Command::SessionPut {
                    session_id: 1,
                    key: format!("key{index}"),
                    sequence: 1,
                    value: "z".repeat(crate::application::MAX_VALUE_BYTES),
                },
            ));
        }
        let first = shared.apply_group(&entries, &mut store).unwrap();
        assert!(
            first.len() > 1 && first.len() < entries.len(),
            "JSON retry payload cells must reach the byte budget"
        );
        let split = first.len();
        assert_eq!(shared.status().last_applied, split as u64);
        assert!(shared.get(&format!("key{}", split + 1)).1.is_none());
        let second = shared.apply_group(&entries[split..], &mut store).unwrap();
        assert_eq!(split + second.len(), entries.len());
        assert_eq!(
            probe.lock().unwrap().calls,
            vec![(1, split as u64), (split as u64 + 1, 100)]
        );
    }

    #[test]
    fn invalid_session_command_advances_only_metadata_without_creating_unbounded_cell_keys() {
        let node = RaftNode::new(1, vec![], 7);
        let shared = SharedReadState::from_node(&node);
        let probe = Arc::new(Mutex::new(Probe::default()));
        let mut store = injected(Box::new(Engine(probe)), &node);
        let result = shared
            .apply_group(
                &[entry(
                    1,
                    Command::SessionPut {
                        session_id: 9,
                        key: "x".repeat(5000),
                        sequence: 1,
                        value: "no".into(),
                    },
                )],
                &mut store,
            )
            .unwrap();
        assert!(matches!(&result[0].1,ApplyOutcome::Session(result) if !result.ok));
        assert_eq!(shared.status().last_applied, 1);
    }

    #[test]
    fn cell_merging_rejects_duplicates_and_preserves_prefix_when_budget_is_exceeded() {
        let mut current = BTreeMap::new();
        let mut bytes = 0;
        assert!(merge_cells(&mut current, &mut bytes, vec![Mutation::put(b"a", b"b")]).unwrap());
        let before = current.clone();
        let before_bytes = bytes;
        assert!(merge_cells(
            &mut current,
            &mut bytes,
            vec![Mutation::put(b"a", b"c"), Mutation::delete(b"a")]
        )
        .is_err());
        assert_eq!(current, before);
        assert_eq!(bytes, before_bytes);
        let changes = (0..17)
            .map(|index| {
                Mutation::put(format!("key{index}"), vec![0; state_store::MAX_VALUE_BYTES])
            })
            .collect();
        assert!(!merge_cells(&mut current, &mut bytes, changes).unwrap());
        assert_eq!(current, before);
        assert_eq!(bytes, before_bytes);
    }
    #[test]
    fn legacy_v2_snapshot_uses_the_existing_application_identity_mapping() {
        for backend in [StateBackend::Memory, StateBackend::Lsm, StateBackend::Redb] {
            let dir = Directory::new();
            let mut node = RaftNode::new(1, vec![2, 3], 7);
            let identity = ApplicationIdentity::legacy(&node);
            let (mut store, mut app) =
                AppliedStore::open(&dir.0, backend, identity, &mut node, None).unwrap();
            app.apply(&entry(1, Command::NoOp)).unwrap();
            let image = SnapshotImage::new(
                raft_core::SnapshotMetadata {
                    last_included_index: 1,
                    last_included_term: 1,
                    members: vec![1, 2, 3],
                    membership: Some(
                        raft_core::membership::CommittedMembership::bootstrap(vec![1, 2, 3])
                            .unwrap(),
                    ),
                },
                &app,
            )
            .unwrap();
            store.install(&image).unwrap();
            assert_eq!(store.index, 1);
        }
    }
}
