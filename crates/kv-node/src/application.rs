//! Deterministic committed application, including retained client retry state.
//!
//! Session records and closed-session tombstones live until database reset.
//! Admission is decided in log order; no local timer or cache eviction changes
//! the retry contract. Log replay reconstructs values and retry state together.

use std::collections::BTreeMap;
use std::fmt;

use raft_core::{Command, Entry, LogIndex};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_SESSIONS: usize = 1024;
pub const MAX_KEYS_PER_SESSION: usize = 256;
pub const MAX_SESSION_KEYS: usize = 8192;
pub const MAX_RETAINED_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_NONCE_BYTES: usize = 128;
pub const MAX_KEY_BYTES: usize = 1024;
pub const MAX_VALUE_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionError {
    InvalidNonce,
    InvalidKey,
    ValueTooLarge,
    InvalidSequence,
    UnknownSession,
    SessionClosed,
    SessionCapacity,
    KeyCapacity,
    PayloadCapacity,
    PayloadMismatch,
    StaleSequence,
    SequenceGap,
    SequenceExhausted,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionResult {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<LogIndex>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub index: Option<LogIndex>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<SessionError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_sequence: Option<u64>,
}

impl SessionResult {
    fn success(session_id: LogIndex, sequence: Option<u64>, index: LogIndex) -> Self {
        Self {
            ok: true,
            session_id: Some(session_id),
            sequence,
            index: Some(index),
            error: None,
            expected_sequence: None,
        }
    }

    fn rejected(error: SessionError, expected_sequence: Option<u64>) -> Self {
        Self {
            ok: false,
            session_id: None,
            sequence: None,
            index: None,
            error: Some(error),
            expected_sequence,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied { index: LogIndex },
    Session(SessionResult),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyError {
    pub previous: LogIndex,
    pub received: LogIndex,
}

impl fmt::Display for ApplyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "application index {} does not follow {}",
            self.received, self.previous
        )
    }
}

impl std::error::Error for ApplyError {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct KeyRecord {
    sequence: u64,
    digest: [u8; 32],
    // Exact equality protects the contract even against a digest collision.
    payload: Vec<u8>,
    result: SessionResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Session {
    registered: SessionResult,
    closed: Option<SessionResult>,
    keys: BTreeMap<String, KeyRecord>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StateMachine {
    values: BTreeMap<String, String>,
    registrations: BTreeMap<String, LogIndex>,
    sessions: BTreeMap<LogIndex, Session>,
    last_applied: LogIndex,
    retained_keys: usize,
    retained_payload_bytes: usize,
}

/// Explicit application format; independent of the Raft/storage snapshot frame.
/// Lists preserve duplicate identities until import can reject them.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationSnapshot {
    pub schema_version: u32,
    pub last_applied: LogIndex,
    pub values: Vec<(String, String)>,
    pub sessions: Vec<SessionSnapshot>,
    pub retained_keys: usize,
    pub retained_payload_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionSnapshot {
    pub nonce: String,
    pub session_id: LogIndex,
    pub registered: SessionResult,
    pub closed: Option<SessionResult>,
    pub keys: Vec<SessionKeySnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionKeySnapshot {
    pub key: String,
    pub sequence: u64,
    pub digest: [u8; 32],
    pub payload: Vec<u8>,
    pub result: SessionResult,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotError(pub String);

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for SnapshotError {}

fn snapshot_require(condition: bool, message: &str) -> Result<(), SnapshotError> {
    if condition {
        Ok(())
    } else {
        Err(SnapshotError(message.into()))
    }
}

/// A provisional application range. The runtime holds its read-state write lock
/// throughout this guard's lifetime; dropping without commit restores every
/// touched component before that lock can expose application state again.
pub(crate) struct ApplicationTransaction<'a> {
    state: &'a mut StateMachine,
    undo: Vec<ApplicationUndo>,
    committed: bool,
}

struct ApplicationUndo {
    last_applied: LogIndex,
    retained_keys: usize,
    retained_payload_bytes: usize,
    value: Option<(String, Option<String>)>,
    registration: Option<(String, Option<LogIndex>)>,
    session: SessionUndo,
}
enum SessionUndo {
    Unchanged,
    Registered {
        session_id: LogIndex,
    },
    Closed {
        session_id: LogIndex,
        previous: Option<SessionResult>,
    },
    Key {
        session_id: LogIndex,
        key: String,
        previous: Option<KeyRecord>,
    },
}

impl ApplicationUndo {
    fn capture(state: &StateMachine, entry: &Entry) -> Self {
        let mut undo = Self {
            last_applied: state.last_applied,
            retained_keys: state.retained_keys,
            retained_payload_bytes: state.retained_payload_bytes,
            value: None,
            registration: None,
            session: SessionUndo::Unchanged,
        };
        match &entry.command {
            Command::Put { key, .. } | Command::Delete { key } => {
                undo.value = Some((key.clone(), state.values.get(key).cloned()));
            }
            Command::RegisterSession { nonce } => {
                undo.registration = Some((nonce.clone(), state.registrations.get(nonce).copied()));
                // Session identities come from prior applied registration entries.
                // The next entry's index cannot name an existing session.
                undo.session = SessionUndo::Registered {
                    session_id: entry.index,
                };
            }
            Command::CloseSession { session_id } => {
                if let Some(session) = state.sessions.get(session_id) {
                    undo.session = SessionUndo::Closed {
                        session_id: *session_id,
                        previous: session.closed.clone(),
                    };
                }
            }
            Command::SessionPut {
                session_id, key, ..
            }
            | Command::SessionDelete {
                session_id, key, ..
            } => {
                undo.value = Some((key.clone(), state.values.get(key).cloned()));
                if let Some(session) = state.sessions.get(session_id) {
                    undo.session = SessionUndo::Key {
                        session_id: *session_id,
                        key: key.clone(),
                        previous: session.keys.get(key).cloned(),
                    };
                }
            }
            Command::NoOp | Command::Configuration(_) => {}
        }
        undo
    }

    fn restore(self, state: &mut StateMachine) {
        fn restore_cell<K: Ord, V>(map: &mut BTreeMap<K, V>, key: K, value: Option<V>) {
            if let Some(value) = value {
                map.insert(key, value);
            } else {
                map.remove(&key);
            }
        }
        if let Some((key, value)) = self.value {
            restore_cell(&mut state.values, key, value);
        }
        if let Some((nonce, id)) = self.registration {
            restore_cell(&mut state.registrations, nonce, id);
        }
        match self.session {
            SessionUndo::Unchanged => {}
            SessionUndo::Registered { session_id } => {
                state.sessions.remove(&session_id);
            }
            SessionUndo::Closed {
                session_id,
                previous,
            } => {
                state
                    .sessions
                    .get_mut(&session_id)
                    .expect("undo retains prior session")
                    .closed = previous;
            }
            SessionUndo::Key {
                session_id,
                key,
                previous,
            } => {
                restore_cell(
                    &mut state
                        .sessions
                        .get_mut(&session_id)
                        .expect("undo retains prior session")
                        .keys,
                    key,
                    previous,
                );
            }
        }
        state.last_applied = self.last_applied;
        state.retained_keys = self.retained_keys;
        state.retained_payload_bytes = self.retained_payload_bytes;
    }
}

impl ApplicationTransaction<'_> {
    pub(crate) fn state(&self) -> &StateMachine {
        self.state
    }
    pub(crate) fn apply(&mut self, entry: &Entry) -> Result<ApplyOutcome, ApplyError> {
        // Validate before capturing a registration undo: a stale index may name
        // an existing session and must never remove that session on rollback.
        if self.state.last_applied.checked_add(1) != Some(entry.index) {
            return Err(ApplyError {
                previous: self.state.last_applied,
                received: entry.index,
            });
        }
        let undo = ApplicationUndo::capture(self.state, entry);
        self.undo.push(undo);
        match self.state.apply(entry) {
            Ok(result) => Ok(result),
            Err(error) => {
                self.undo_last();
                Err(error)
            }
        }
    }
    pub(crate) fn undo_last(&mut self) {
        self.undo
            .pop()
            .expect("undo applies only to a prepared entry")
            .restore(self.state);
    }
    pub(crate) fn commit(mut self) {
        self.committed = true;
    }
}
impl Drop for ApplicationTransaction<'_> {
    fn drop(&mut self) {
        if !self.committed {
            for undo in self.undo.drain(..).rev() {
                undo.restore(self.state);
            }
        }
    }
}

impl StateMachine {
    pub(crate) fn transaction(&mut self) -> ApplicationTransaction<'_> {
        ApplicationTransaction {
            state: self,
            undo: Vec::new(),
            committed: false,
        }
    }
    pub(crate) fn retained_counts(&self) -> (usize, usize) {
        (self.retained_keys, self.retained_payload_bytes)
    }
    pub(crate) fn registration_id(&self, nonce: &str) -> Option<LogIndex> {
        self.registrations.get(nonce).copied()
    }
    pub(crate) fn session_header(
        &self,
        id: LogIndex,
    ) -> Option<(SessionResult, Option<SessionResult>)> {
        self.sessions
            .get(&id)
            .map(|session| (session.registered.clone(), session.closed.clone()))
    }
    pub(crate) fn session_key(&self, id: LogIndex, key: &str) -> Option<SessionKeySnapshot> {
        let record = self.sessions.get(&id)?.keys.get(key)?;
        Some(SessionKeySnapshot {
            key: key.to_owned(),
            sequence: record.sequence,
            digest: record.digest,
            payload: record.payload.clone(),
            result: record.result.clone(),
        })
    }
}

impl StateMachine {
    pub fn values(&self) -> &BTreeMap<String, String> {
        &self.values
    }

    pub fn last_applied(&self) -> LogIndex {
        self.last_applied
    }

    /// Consume the next committed entry, even when its application is rejected.
    /// A reject changes no session sequence or value, but still advances apply.
    pub fn apply(&mut self, entry: &Entry) -> Result<ApplyOutcome, ApplyError> {
        if self.last_applied.checked_add(1) != Some(entry.index) {
            return Err(ApplyError {
                previous: self.last_applied,
                received: entry.index,
            });
        }
        let outcome = match &entry.command {
            Command::Put { key, value } => {
                self.values.insert(key.clone(), value.clone());
                ApplyOutcome::Applied { index: entry.index }
            }
            Command::Delete { key } => {
                self.values.remove(key);
                ApplyOutcome::Applied { index: entry.index }
            }
            Command::NoOp | Command::Configuration(_) => {
                ApplyOutcome::Applied { index: entry.index }
            }
            Command::RegisterSession { nonce } => {
                ApplyOutcome::Session(self.register(nonce, entry.index))
            }
            Command::CloseSession { session_id } => {
                ApplyOutcome::Session(self.close(*session_id, entry.index))
            }
            Command::SessionPut {
                session_id,
                key,
                sequence,
                value,
            } => ApplyOutcome::Session(self.mutate(
                *session_id,
                key,
                *sequence,
                Some(value),
                entry.index,
            )),
            Command::SessionDelete {
                session_id,
                key,
                sequence,
            } => ApplyOutcome::Session(self.mutate(*session_id, key, *sequence, None, entry.index)),
        };
        self.last_applied = entry.index;
        Ok(outcome)
    }

    /// Export the complete application at its current applied watermark.
    /// The caller must frame/version/bound storage or network bytes separately.
    pub fn export_snapshot(&self) -> ApplicationSnapshot {
        ApplicationSnapshot {
            schema_version: 1,
            last_applied: self.last_applied,
            values: self
                .values
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            sessions: self
                .registrations
                .iter()
                .map(|(nonce, id)| {
                    let session = &self.sessions[id];
                    SessionSnapshot {
                        nonce: nonce.clone(),
                        session_id: *id,
                        registered: session.registered.clone(),
                        closed: session.closed.clone(),
                        keys: session
                            .keys
                            .iter()
                            .map(|(key, record)| SessionKeySnapshot {
                                key: key.clone(),
                                sequence: record.sequence,
                                digest: record.digest,
                                payload: record.payload.clone(),
                                result: record.result.clone(),
                            })
                            .collect(),
                    }
                })
                .collect(),
            retained_keys: self.retained_keys,
            retained_payload_bytes: self.retained_payload_bytes,
        }
    }

    /// Rebuild from validated metadata tied to the enclosing Raft snapshot index.
    /// StateMachine deliberately has no Deserialize implementation that bypasses
    /// this trust boundary. Construct the replacement fully before publishing it.
    pub fn import_snapshot(
        snapshot: ApplicationSnapshot,
        expected_last_applied: LogIndex,
    ) -> Result<Self, SnapshotError> {
        snapshot_require(
            snapshot.schema_version == 1,
            "unsupported application snapshot version",
        )?;
        snapshot_require(
            snapshot.last_applied == expected_last_applied,
            "application/Raft snapshot index mismatch",
        )?;
        snapshot_require(
            snapshot.sessions.len() <= MAX_SESSIONS,
            "snapshot exceeds retained session capacity",
        )?;
        snapshot_require(
            snapshot.retained_keys <= MAX_SESSION_KEYS,
            "snapshot key counter exceeds capacity",
        )?;
        snapshot_require(
            snapshot.retained_payload_bytes <= MAX_RETAINED_PAYLOAD_BYTES,
            "snapshot payload counter exceeds capacity",
        )?;
        snapshot_require(
            snapshot.last_applied > 0
                || (snapshot.values.is_empty() && snapshot.sessions.is_empty()),
            "zero-index snapshot contains application state",
        )?;
        let mut state = Self {
            last_applied: snapshot.last_applied,
            ..Self::default()
        };
        for (key, value) in snapshot.values {
            snapshot_require(
                !key.is_empty() && key.len() <= MAX_KEY_BYTES,
                "invalid snapshot value key",
            )?;
            snapshot_require(
                value.len() <= MAX_VALUE_BYTES,
                "snapshot value exceeds byte limit",
            )?;
            snapshot_require(
                state.values.insert(key, value).is_none(),
                "duplicate snapshot value key",
            )?;
        }
        // Each retained registration, close and latest mutation originated in
        // its own log entry. One index cannot justify two different operations.
        let mut operation_indices = std::collections::BTreeSet::new();
        for saved in snapshot.sessions {
            let id = saved.session_id;
            snapshot_require(
                !saved.nonce.is_empty()
                    && saved.nonce.len() <= MAX_NONCE_BYTES
                    && !saved.nonce.chars().any(char::is_control),
                "invalid snapshot registration nonce",
            )?;
            snapshot_require(
                id > 0 && id <= snapshot.last_applied,
                "invalid snapshot session identity",
            )?;
            snapshot_require(
                saved.registered == SessionResult::success(id, None, id),
                "registration result does not match its identity",
            )?;
            snapshot_require(
                operation_indices.insert(id),
                "duplicate snapshot operation index",
            )?;
            snapshot_require(
                saved.keys.len() <= MAX_KEYS_PER_SESSION,
                "snapshot exceeds per-session key capacity",
            )?;
            let close_index = if let Some(closed) = &saved.closed {
                let index = closed
                    .index
                    .ok_or_else(|| SnapshotError("closed result has no index".into()))?;
                snapshot_require(
                    index > id
                        && index <= snapshot.last_applied
                        && *closed == SessionResult::success(id, None, index),
                    "invalid closed-session result",
                )?;
                snapshot_require(
                    operation_indices.insert(index),
                    "duplicate snapshot operation index",
                )?;
                Some(index)
            } else {
                None
            };
            let mut session = Session {
                registered: saved.registered,
                closed: saved.closed,
                keys: BTreeMap::new(),
            };
            for record in saved.keys {
                snapshot_require(
                    !record.key.is_empty() && record.key.len() <= MAX_KEY_BYTES,
                    "invalid snapshot retry key",
                )?;
                snapshot_require(record.sequence > 0, "snapshot retry sequence is zero")?;
                let value = decode_payload(&record.key, &record.payload)?;
                snapshot_require(
                    canonical_payload(&record.key, value) == record.payload,
                    "snapshot retry payload is not canonical",
                )?;
                let digest: [u8; 32] = Sha256::digest(&record.payload).into();
                snapshot_require(digest == record.digest, "snapshot payload digest mismatch")?;
                let index = record
                    .result
                    .index
                    .ok_or_else(|| SnapshotError("retry result has no index".into()))?;
                snapshot_require(
                    index > id
                        && index <= snapshot.last_applied
                        && record.sequence <= index - id
                        && close_index.is_none_or(|closed| index < closed)
                        && record.result
                            == SessionResult::success(id, Some(record.sequence), index),
                    "retry result is inconsistent with its sequence or applied index",
                )?;
                snapshot_require(
                    operation_indices.insert(index),
                    "duplicate snapshot operation index",
                )?;
                state.retained_keys += 1;
                state.retained_payload_bytes = state
                    .retained_payload_bytes
                    .checked_add(record.payload.len())
                    .ok_or_else(|| {
                        SnapshotError("snapshot payload byte counter overflow".into())
                    })?;
                snapshot_require(
                    state.retained_keys <= MAX_SESSION_KEYS
                        && state.retained_payload_bytes <= MAX_RETAINED_PAYLOAD_BYTES,
                    "snapshot retained retry state exceeds capacity",
                )?;
                snapshot_require(
                    session
                        .keys
                        .insert(
                            record.key,
                            KeyRecord {
                                sequence: record.sequence,
                                digest: record.digest,
                                payload: record.payload,
                                result: record.result,
                            },
                        )
                        .is_none(),
                    "duplicate snapshot session key",
                )?;
            }
            snapshot_require(
                state.registrations.insert(saved.nonce, id).is_none(),
                "duplicate snapshot registration nonce",
            )?;
            snapshot_require(
                state.sessions.insert(id, session).is_none(),
                "duplicate snapshot session identity",
            )?;
        }
        snapshot_require(
            state.retained_keys == snapshot.retained_keys,
            "snapshot retained-key counter mismatch",
        )?;
        snapshot_require(
            state.retained_payload_bytes == snapshot.retained_payload_bytes,
            "snapshot retained-payload byte counter mismatch",
        )?;
        Ok(state)
    }

    fn register(&mut self, nonce: &str, index: LogIndex) -> SessionResult {
        if nonce.is_empty() || nonce.len() > MAX_NONCE_BYTES || nonce.chars().any(char::is_control)
        {
            return SessionResult::rejected(SessionError::InvalidNonce, None);
        }
        if let Some(id) = self.registrations.get(nonce) {
            return self.sessions[id].registered.clone();
        }
        if self.sessions.len() == MAX_SESSIONS {
            return SessionResult::rejected(SessionError::SessionCapacity, None);
        }
        let result = SessionResult::success(index, None, index);
        self.registrations.insert(nonce.to_owned(), index);
        self.sessions.insert(
            index,
            Session {
                registered: result.clone(),
                closed: None,
                keys: BTreeMap::new(),
            },
        );
        result
    }

    fn close(&mut self, id: LogIndex, index: LogIndex) -> SessionResult {
        let Some(session) = self.sessions.get_mut(&id) else {
            return SessionResult::rejected(SessionError::UnknownSession, None);
        };
        let result = session
            .closed
            .get_or_insert_with(|| SessionResult::success(id, None, index));
        result.clone()
    }

    fn mutate(
        &mut self,
        id: LogIndex,
        key: &str,
        sequence: u64,
        value: Option<&String>,
        index: LogIndex,
    ) -> SessionResult {
        if key.is_empty() || key.len() > MAX_KEY_BYTES {
            return SessionResult::rejected(SessionError::InvalidKey, None);
        }
        if value.is_some_and(|value| value.len() > MAX_VALUE_BYTES) {
            return SessionResult::rejected(SessionError::ValueTooLarge, None);
        }
        let Some(session) = self.sessions.get_mut(&id) else {
            return SessionResult::rejected(SessionError::UnknownSession, None);
        };
        if session.closed.is_some() {
            return SessionResult::rejected(SessionError::SessionClosed, None);
        }
        if sequence == 0 {
            if session
                .keys
                .get(key)
                .is_some_and(|record| record.sequence == u64::MAX)
            {
                return SessionResult::rejected(SessionError::SequenceExhausted, None);
            }
            return SessionResult::rejected(SessionError::InvalidSequence, Some(1));
        }
        let payload = canonical_payload(key, value.map(String::as_str));
        let digest: [u8; 32] = Sha256::digest(&payload).into();
        let previous = session.keys.get(key);
        let expected = if let Some(previous) = previous {
            if sequence == previous.sequence {
                return if digest == previous.digest && payload == previous.payload {
                    previous.result.clone()
                } else {
                    SessionResult::rejected(SessionError::PayloadMismatch, None)
                };
            }
            if sequence < previous.sequence {
                return SessionResult::rejected(SessionError::StaleSequence, None);
            }
            let Some(next) = previous.sequence.checked_add(1) else {
                return SessionResult::rejected(SessionError::SequenceExhausted, None);
            };
            next
        } else {
            1
        };
        if sequence != expected {
            return SessionResult::rejected(SessionError::SequenceGap, Some(expected));
        }
        if previous.is_none()
            && (session.keys.len() == MAX_KEYS_PER_SESSION
                || self.retained_keys == MAX_SESSION_KEYS)
        {
            return SessionResult::rejected(SessionError::KeyCapacity, None);
        }
        let previous_bytes = previous.map_or(0, |record| record.payload.len());
        let retained_bytes = self.retained_payload_bytes - previous_bytes + payload.len();
        if retained_bytes > MAX_RETAINED_PAYLOAD_BYTES {
            return SessionResult::rejected(SessionError::PayloadCapacity, None);
        }
        let result = SessionResult::success(id, Some(sequence), index);
        if previous.is_none() {
            self.retained_keys += 1;
        }
        self.retained_payload_bytes = retained_bytes;
        session.keys.insert(
            key.to_owned(),
            KeyRecord {
                sequence,
                digest,
                payload,
                result: result.clone(),
            },
        );
        match value {
            Some(value) => {
                self.values.insert(key.to_owned(), value.clone());
            }
            None => {
                self.values.remove(key);
            }
        }
        result
    }
}

fn decode_payload<'a>(key: &str, payload: &'a [u8]) -> Result<Option<&'a str>, SnapshotError> {
    let prefix = b"micro-raft/session-payload/v1";
    let invalid = || SnapshotError("malformed canonical retry payload".into());
    let mut tail = payload
        .strip_prefix(prefix.as_slice())
        .ok_or_else(invalid)?;
    let tag = *tail.first().ok_or_else(invalid)?;
    tail = &tail[1..];
    fn field<'a>(tail: &mut &'a [u8], maximum: usize) -> Result<&'a [u8], SnapshotError> {
        let invalid = || SnapshotError("malformed canonical retry payload length".into());
        let length = tail.get(..8).ok_or_else(invalid)?;
        let length = u64::from_be_bytes(length.try_into().map_err(|_| invalid())?);
        let length = usize::try_from(length).map_err(|_| invalid())?;
        if length > maximum {
            return Err(invalid());
        }
        *tail = &tail[8..];
        let bytes = tail.get(..length).ok_or_else(invalid)?;
        *tail = &tail[length..];
        Ok(bytes)
    }
    snapshot_require(
        field(&mut tail, MAX_KEY_BYTES)? == key.as_bytes(),
        "snapshot retry payload key mismatch",
    )?;
    let value = match tag {
        0 => None,
        1 => Some(std::str::from_utf8(field(&mut tail, MAX_VALUE_BYTES)?).map_err(|_| invalid())?),
        _ => return Err(invalid()),
    };
    snapshot_require(tail.is_empty(), "snapshot retry payload has trailing bytes")?;
    Ok(value)
}

fn canonical_payload(key: &str, value: Option<&str>) -> Vec<u8> {
    let mut payload = b"micro-raft/session-payload/v1".to_vec();
    payload.push(u8::from(value.is_some()));
    payload.extend_from_slice(&(key.len() as u64).to_be_bytes());
    payload.extend_from_slice(key.as_bytes());
    if let Some(value) = value {
        payload.extend_from_slice(&(value.len() as u64).to_be_bytes());
        payload.extend_from_slice(value.as_bytes());
    }
    payload
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(state: &mut StateMachine, command: Command) -> ApplyOutcome {
        state
            .apply(&Entry {
                index: state.last_applied + 1,
                term: 1,
                command,
            })
            .unwrap()
    }

    fn session(state: &mut StateMachine, nonce: &str) -> LogIndex {
        let ApplyOutcome::Session(result) = apply(
            state,
            Command::RegisterSession {
                nonce: nonce.into(),
            },
        ) else {
            panic!("registration result");
        };
        assert!(result.ok);
        result.session_id.unwrap()
    }

    fn put(id: LogIndex, key: &str, sequence: u64, value: &str) -> Command {
        Command::SessionPut {
            session_id: id,
            key: key.into(),
            sequence,
            value: value.into(),
        }
    }

    fn rejected(outcome: ApplyOutcome, expected: SessionError) {
        let ApplyOutcome::Session(result) = outcome else {
            panic!("expected session rejection");
        };
        assert_eq!(result.error, Some(expected));
        assert!(!result.ok);
    }

    #[test]
    fn duplicates_at_different_log_indices_return_the_original_response() {
        let mut state = StateMachine::default();
        let id = session(&mut state, "client");
        let first = apply(&mut state, put(id, "k", 1, "original"));
        // Another client or legacy write may legitimately change the same key.
        apply(
            &mut state,
            Command::Put {
                key: "k".into(),
                value: "newer".into(),
            },
        );
        let replay = apply(&mut state, put(id, "k", 1, "original"));
        assert_eq!(replay, first);
        assert_eq!(
            state.values()["k"],
            "newer",
            "duplicate must not mutate again"
        );
        assert_eq!(state.last_applied(), 4);
    }

    #[test]
    fn registration_and_close_are_idempotent_without_reopening_a_session() {
        let mut state = StateMachine::default();
        let id = session(&mut state, "nonce");
        let closed = apply(&mut state, Command::CloseSession { session_id: id });
        assert_eq!(
            apply(&mut state, Command::CloseSession { session_id: id }),
            closed
        );
        assert_eq!(session(&mut state, "nonce"), id);
        rejected(
            apply(&mut state, put(id, "k", 1, "v")),
            SessionError::SessionClosed,
        );
        assert_eq!(state.sessions.len(), 1);
        assert_eq!(state.registrations.len(), 1);
    }

    #[test]
    fn changed_stale_gap_and_invalid_sequences_do_not_consume_progress() {
        let mut state = StateMachine::default();
        let id = session(&mut state, "sequence");
        rejected(
            apply(&mut state, put(id, "k", 0, "v")),
            SessionError::InvalidSequence,
        );
        rejected(
            apply(&mut state, put(id, "k", 2, "v")),
            SessionError::SequenceGap,
        );
        let first = apply(&mut state, put(id, "k", 1, "v"));
        rejected(
            apply(&mut state, put(id, "k", 1, "different")),
            SessionError::PayloadMismatch,
        );
        assert_eq!(apply(&mut state, put(id, "k", 1, "v")), first);
        apply(&mut state, put(id, "k", 2, "next"));
        rejected(
            apply(&mut state, put(id, "k", 1, "v")),
            SessionError::StaleSequence,
        );
        assert_eq!(state.values()["k"], "next");
    }

    #[test]
    fn independent_keys_and_sessions_have_independent_sequences() {
        let mut state = StateMachine::default();
        let a = session(&mut state, "a");
        let b = session(&mut state, "b");
        for command in [
            put(a, "x", 1, "a"),
            put(a, "y", 1, "a"),
            put(b, "x", 1, "b"),
        ] {
            let ApplyOutcome::Session(result) = apply(&mut state, command) else {
                panic!()
            };
            assert!(result.ok);
        }
        assert_eq!(state.values()["x"], "b");
        assert_eq!(state.values()["y"], "a");
    }

    #[test]
    fn delete_retains_retry_state_and_cannot_delete_a_later_value() {
        let mut state = StateMachine::default();
        let id = session(&mut state, "delete");
        apply(&mut state, put(id, "k", 1, "old"));
        let delete = Command::SessionDelete {
            session_id: id,
            key: "k".into(),
            sequence: 2,
        };
        let first = apply(&mut state, delete.clone());
        assert!(!state.values().contains_key("k"));
        apply(
            &mut state,
            Command::Put {
                key: "k".into(),
                value: "later".into(),
            },
        );
        assert_eq!(apply(&mut state, delete), first);
        assert_eq!(state.values()["k"], "later");
        assert_eq!(state.retained_keys, 1);
        rejected(
            apply(&mut state, put(id, "k", 2, "")),
            SessionError::PayloadMismatch,
        );
    }

    #[test]
    fn last_sequence_replays_without_overflow_and_never_wraps_to_zero() {
        let mut state = StateMachine::default();
        let id = session(&mut state, "overflow");
        apply(&mut state, put(id, "k", 1, "v"));
        let record = state
            .sessions
            .get_mut(&id)
            .unwrap()
            .keys
            .get_mut("k")
            .unwrap();
        record.sequence = u64::MAX;
        record.result.sequence = Some(u64::MAX);
        let expected = ApplyOutcome::Session(record.result.clone());
        assert_eq!(apply(&mut state, put(id, "k", u64::MAX, "v")), expected);
        rejected(
            apply(&mut state, put(id, "k", 0, "v")),
            SessionError::SequenceExhausted,
        );
        rejected(
            apply(&mut state, put(id, "k", 1, "v")),
            SessionError::StaleSequence,
        );
    }

    #[test]
    fn capacities_are_replicated_and_closed_sessions_still_consume_slots() {
        let mut state = StateMachine::default();
        for n in 0..MAX_SESSIONS {
            let id = session(&mut state, &format!("session-{n}"));
            apply(&mut state, Command::CloseSession { session_id: id });
        }
        rejected(
            apply(
                &mut state,
                Command::RegisterSession {
                    nonce: "one-more".into(),
                },
            ),
            SessionError::SessionCapacity,
        );
        assert_eq!(state.sessions.len(), MAX_SESSIONS);
        assert_eq!(
            session(&mut state, "session-0"),
            1,
            "admitted nonce still replays at capacity"
        );
    }

    #[test]
    fn key_capacity_preserves_existing_replays_and_allows_next_sequence() {
        let mut state = StateMachine::default();
        let id = session(&mut state, "keys");
        let first = apply(&mut state, put(id, "k0", 1, "v"));
        for n in 1..MAX_KEYS_PER_SESSION {
            apply(&mut state, put(id, &format!("k{n}"), 1, "v"));
        }
        rejected(
            apply(&mut state, put(id, "overflow", 1, "v")),
            SessionError::KeyCapacity,
        );
        assert_eq!(apply(&mut state, put(id, "k0", 1, "v")), first);
        let ApplyOutcome::Session(next) = apply(&mut state, put(id, "k0", 2, "v2")) else {
            panic!()
        };
        assert!(next.ok);
        assert_eq!(state.retained_keys, MAX_KEYS_PER_SESSION);
    }

    #[test]
    fn byte_capacity_rejection_does_not_advance_sequence_or_mutate_value() {
        let mut state = StateMachine::default();
        let value = "x".repeat(MAX_VALUE_BYTES);
        let a = session(&mut state, "bytes-a");
        let b = session(&mut state, "bytes-b");
        let mut rejected_key = None;
        for n in 0..MAX_KEYS_PER_SESSION {
            let outcome = apply(&mut state, put(a, &format!("k{n}"), 1, &value));
            if let ApplyOutcome::Session(result) = outcome {
                if result.error == Some(SessionError::PayloadCapacity) {
                    rejected_key = Some(format!("k{n}"));
                    break;
                }
            }
        }
        let key = rejected_key.expect("payload cap reached before all 256 full-size records");
        assert!(!state.values().contains_key(&key));
        assert!(!state.sessions[&a].keys.contains_key(&key));
        assert!(state.retained_payload_bytes <= MAX_RETAINED_PAYLOAD_BYTES);
        let ApplyOutcome::Session(small) = apply(&mut state, put(b, "small", 1, "v")) else {
            panic!()
        };
        assert!(
            small.ok,
            "small record can use remaining declared byte budget"
        );
    }

    #[test]
    fn replay_rebuilds_values_sessions_and_stable_responses_together() {
        let commands = [
            Command::RegisterSession {
                nonce: "recovered".into(),
            },
            put(1, "k", 1, "first"),
            put(1, "k", 1, "first"),
            put(1, "k", 2, "second"),
            Command::SessionDelete {
                session_id: 1,
                key: "k".into(),
                sequence: 3,
            },
            Command::CloseSession { session_id: 1 },
        ];
        let mut original = StateMachine::default();
        let results: Vec<_> = commands
            .iter()
            .cloned()
            .map(|c| apply(&mut original, c))
            .collect();
        let mut recovered = StateMachine::default();
        let replays: Vec<_> = commands
            .into_iter()
            .map(|c| apply(&mut recovered, c))
            .collect();
        assert_eq!(results, replays);
        assert_eq!(original, recovered);
        rejected(
            apply(&mut recovered, put(1, "k", 3, "")),
            SessionError::SessionClosed,
        );
    }

    #[test]
    fn application_rejects_noncontiguous_replay_without_partial_changes() {
        let mut state = StateMachine::default();
        let entry = Entry {
            index: 2,
            term: 1,
            command: Command::RegisterSession { nonce: "n".into() },
        };
        assert!(state.apply(&entry).is_err());
        assert_eq!(state, StateMachine::default());
    }

    #[test]
    fn payload_encoding_separates_operations_lengths_and_exact_bytes() {
        assert_ne!(
            canonical_payload("ab", Some("c")),
            canonical_payload("a", Some("bc"))
        );
        assert_ne!(
            canonical_payload("k", None),
            canonical_payload("k", Some(""))
        );
        let mut state = StateMachine::default();
        let id = session(&mut state, "digest");
        apply(&mut state, put(id, "k", 1, "v"));
        // Planted digest collision: exact canonical payload still refuses replay.
        let wrong = canonical_payload("k", Some("different"));
        state
            .sessions
            .get_mut(&id)
            .unwrap()
            .keys
            .get_mut("k")
            .unwrap()
            .digest = Sha256::digest(wrong).into();
        rejected(
            apply(&mut state, put(id, "k", 1, "different")),
            SessionError::PayloadMismatch,
        );
    }

    #[test]
    fn invalid_session_commands_are_explicit_without_allocating_state() {
        let mut state = StateMachine::default();
        rejected(
            apply(&mut state, Command::RegisterSession { nonce: "".into() }),
            SessionError::InvalidNonce,
        );
        rejected(
            apply(&mut state, Command::CloseSession { session_id: 99 }),
            SessionError::UnknownSession,
        );
        rejected(
            apply(&mut state, put(99, "k", 1, "v")),
            SessionError::UnknownSession,
        );
        let id = session(&mut state, "valid");
        rejected(
            apply(&mut state, put(id, "", 1, "v")),
            SessionError::InvalidKey,
        );
        rejected(
            apply(
                &mut state,
                put(id, "k", 1, &"v".repeat(MAX_VALUE_BYTES + 1)),
            ),
            SessionError::ValueTooLarge,
        );
        assert_eq!(state.retained_keys, 0);
        assert!(state.values().is_empty());
    }
    #[test]
    fn global_key_capacity_is_enforced_across_sessions_in_application_order() {
        let mut state = StateMachine::default();
        for group in 0..(MAX_SESSION_KEYS / MAX_KEYS_PER_SESSION) {
            let id = session(&mut state, &format!("global-{group}"));
            for key in 0..MAX_KEYS_PER_SESSION {
                let ApplyOutcome::Session(result) =
                    apply(&mut state, put(id, &format!("k{key}"), 1, "v"))
                else {
                    panic!()
                };
                assert!(result.ok);
            }
        }
        let extra = session(&mut state, "extra");
        rejected(
            apply(&mut state, put(extra, "key", 1, "v")),
            SessionError::KeyCapacity,
        );
        assert_eq!(state.retained_keys, MAX_SESSION_KEYS);
        assert!(state.sessions[&extra].keys.is_empty());
    }
    fn snapshot_fixture() -> StateMachine {
        let mut state = StateMachine::default();
        let a = session(&mut state, "snapshot-a");
        apply(&mut state, put(a, "kept", 1, "original"));
        let b = session(&mut state, "snapshot-b");
        apply(&mut state, put(b, "deleted", 1, "old"));
        apply(
            &mut state,
            Command::SessionDelete {
                session_id: b,
                key: "deleted".into(),
                sequence: 2,
            },
        );
        apply(&mut state, Command::CloseSession { session_id: b });
        // Cached original response remains valid although another write changed the value.
        apply(
            &mut state,
            Command::Put {
                key: "kept".into(),
                value: "later".into(),
            },
        );
        state
    }

    #[test]
    fn snapshot_roundtrip_preserves_closed_deleted_and_retry_state() {
        let original = snapshot_fixture();
        let raw = serde_json::to_vec(&original.export_snapshot()).unwrap();
        let snapshot: ApplicationSnapshot = serde_json::from_slice(&raw).unwrap();
        let mut imported =
            StateMachine::import_snapshot(snapshot, original.last_applied()).unwrap();
        assert_eq!(original, imported);
        let replay = apply(&mut imported, put(1, "kept", 1, "original"));
        let ApplyOutcome::Session(result) = replay else {
            panic!()
        };
        assert_eq!(result.index, Some(2));
        assert_eq!(imported.values()["kept"], "later");
        rejected(
            apply(&mut imported, put(3, "deleted", 3, "resurrect")),
            SessionError::SessionClosed,
        );
        assert!(!imported.values().contains_key("deleted"));
        assert_eq!(session(&mut imported, "snapshot-b"), 3);
    }

    #[test]
    fn snapshot_rejects_version_watermark_and_counter_mismatches() {
        let state = snapshot_fixture();
        let base = state.export_snapshot();
        assert!(StateMachine::import_snapshot(base.clone(), state.last_applied() + 1).is_err());
        for (version, keys, bytes) in [
            (2, base.retained_keys, base.retained_payload_bytes),
            (1, base.retained_keys + 1, base.retained_payload_bytes),
            (1, base.retained_keys, base.retained_payload_bytes + 1),
            (1, MAX_SESSION_KEYS + 1, base.retained_payload_bytes),
            (1, base.retained_keys, MAX_RETAINED_PAYLOAD_BYTES + 1),
        ] {
            let mut snapshot = base.clone();
            snapshot.schema_version = version;
            snapshot.retained_keys = keys;
            snapshot.retained_payload_bytes = bytes;
            assert!(StateMachine::import_snapshot(snapshot, state.last_applied()).is_err());
        }
    }

    #[test]
    fn snapshot_rejects_forged_payload_digest_and_cached_results() {
        let state = snapshot_fixture();
        let base = state.export_snapshot();
        for variant in 0..8 {
            let mut snapshot = base.clone();
            let record = &mut snapshot.sessions[0].keys[0];
            match variant {
                0 => record.digest[0] ^= 1,
                1 => record.payload.push(0),
                2 => record.sequence = 0,
                3 => record.result.index = Some(state.last_applied() + 1),
                4 => record.result.session_id = Some(99),
                5 => record.result.error = Some(SessionError::UnknownSession),
                6 => record.result.ok = false,
                7 => record.key = "different".into(),
                _ => unreachable!(),
            }
            assert!(
                StateMachine::import_snapshot(snapshot, state.last_applied()).is_err(),
                "variant {variant}"
            );
        }
    }

    #[test]
    fn snapshot_rejects_duplicate_names_ids_keys_and_operation_indices() {
        let state = snapshot_fixture();
        let base = state.export_snapshot();
        for variant in 0..6 {
            let mut snapshot = base.clone();
            match variant {
                0 => snapshot.values.push(snapshot.values[0].clone()),
                1 => snapshot.sessions.push(snapshot.sessions[0].clone()),
                2 => snapshot.sessions[1].nonce = snapshot.sessions[0].nonce.clone(),
                3 => {
                    let duplicate = snapshot.sessions[0].keys[0].clone();
                    snapshot.sessions[0].keys.push(duplicate);
                }
                4 => {
                    snapshot.sessions[1].session_id = 2;
                    snapshot.sessions[1].registered = SessionResult::success(2, None, 2);
                }
                5 => snapshot.sessions[1].closed = Some(SessionResult::success(3, None, 4)),
                _ => unreachable!(),
            }
            assert!(
                StateMachine::import_snapshot(snapshot, state.last_applied()).is_err(),
                "variant {variant}"
            );
        }
    }

    #[test]
    fn snapshot_rejects_malformed_lengths_and_unknown_fields() {
        let state = snapshot_fixture();
        let base = state.export_snapshot();
        for payload in [
            vec![],
            b"other-version".to_vec(),
            [
                b"micro-raft/session-payload/v1".as_slice(),
                &[1],
                &u64::MAX.to_be_bytes(),
            ]
            .concat(),
            canonical_payload("kept", Some("x"))
                .into_iter()
                .take(32)
                .collect(),
        ] {
            let mut snapshot = base.clone();
            snapshot.sessions[0].keys[0].payload = payload;
            assert!(StateMachine::import_snapshot(snapshot, state.last_applied()).is_err());
        }
        let mut json = serde_json::to_value(base).unwrap();
        json["unexpected"] = serde_json::json!(true);
        assert!(serde_json::from_value::<ApplicationSnapshot>(json).is_err());
    }

    #[test]
    fn snapshot_rejects_capacity_nonce_key_and_registration_corruption() {
        let state = snapshot_fixture();
        let base = state.export_snapshot();
        for variant in 0..6 {
            let mut snapshot = base.clone();
            match variant {
                0 => snapshot.sessions[0].nonce.clear(),
                1 => snapshot.sessions[0].nonce = "n".repeat(MAX_NONCE_BYTES + 1),
                2 => snapshot.values[0].0 = "k".repeat(MAX_KEY_BYTES + 1),
                3 => snapshot.values[0].1 = "v".repeat(MAX_VALUE_BYTES + 1),
                4 => snapshot.sessions[0].registered.sequence = Some(1),
                5 => {
                    snapshot.sessions[0].keys =
                        vec![snapshot.sessions[0].keys[0].clone(); MAX_KEYS_PER_SESSION + 1]
                }
                _ => unreachable!(),
            }
            assert!(
                StateMachine::import_snapshot(snapshot, state.last_applied()).is_err(),
                "variant {variant}"
            );
        }
    }

    #[test]
    fn snapshot_empty_boundary_is_valid_and_nonempty_zero_boundary_is_not() {
        let empty = StateMachine::default();
        assert_eq!(
            StateMachine::import_snapshot(empty.export_snapshot(), 0).unwrap(),
            empty
        );
        let mut invalid = empty.export_snapshot();
        invalid.values.push(("key".into(), "value".into()));
        assert!(StateMachine::import_snapshot(invalid, 0).is_err());
    }

    #[test]
    fn snapshot_install_replacement_is_constructed_before_publication() {
        let mut live = snapshot_fixture();
        let before = live.clone();
        let mut invalid = live.export_snapshot();
        invalid.retained_keys += 1;
        if let Ok(replacement) = StateMachine::import_snapshot(invalid, live.last_applied()) {
            live = replacement;
        }
        assert_eq!(live, before);
    }
}
#[cfg(test)]
mod transaction_tests {
    use super::*;
    fn log(index: u64, command: Command) -> Entry {
        Entry {
            index,
            term: 1,
            command,
        }
    }
    fn put(session_id: u64, key: &str, sequence: u64, value: &str) -> Command {
        Command::SessionPut {
            session_id,
            key: key.into(),
            sequence,
            value: value.into(),
        }
    }
    fn baseline() -> StateMachine {
        let mut app = StateMachine::default();
        app.apply(&log(
            1,
            Command::RegisterSession {
                nonce: "first".into(),
            },
        ))
        .unwrap();
        app.apply(&log(2, put(1, "key", 1, "original"))).unwrap();
        app
    }
    #[test]
    fn dropped_range_restores_values_registration_headers_retry_records_and_counters() {
        let mut app = baseline();
        let before = app.clone();
        {
            let mut transaction = app.transaction();
            let commands = [
                Command::RegisterSession {
                    nonce: "second".into(),
                },
                put(3, "other", 1, "created"),
                Command::Put {
                    key: "key".into(),
                    value: "independent".into(),
                },
                Command::SessionDelete {
                    session_id: 1,
                    key: "key".into(),
                    sequence: 2,
                },
                Command::CloseSession { session_id: 1 },
                put(1, "key", 3, "rejected because closed"),
                Command::RegisterSession {
                    nonce: "first".into(),
                },
                Command::Delete {
                    key: "other".into(),
                },
                Command::NoOp,
            ];
            for (offset, command) in commands.into_iter().enumerate() {
                transaction.apply(&log(offset as u64 + 3, command)).unwrap();
            }
            assert_ne!(transaction.state(), &before);
            // Simulate returning an engine commit error: the transaction guard
            // is dropped before the enclosing read-state write lock is released.
        }
        assert_eq!(app, before);
        assert_eq!(
            StateMachine::import_snapshot(app.export_snapshot(), app.last_applied()).unwrap(),
            before
        );
    }
    #[test]
    fn byte_boundary_undo_of_last_entry_preserves_earlier_prepared_range() {
        let mut app = baseline();
        {
            let mut transaction = app.transaction();
            transaction
                .apply(&log(3, put(1, "key", 2, "next")))
                .unwrap();
            transaction
                .apply(&log(4, Command::CloseSession { session_id: 1 }))
                .unwrap();
            transaction.undo_last();
            assert_eq!(transaction.state().last_applied(), 3);
            transaction.commit();
        }
        let outcome = app.apply(&log(4, put(1, "key", 2, "next"))).unwrap();
        assert!(matches!(
            outcome,
            ApplyOutcome::Session(SessionResult {
                index: Some(3),
                error: None,
                ..
            })
        ));
        assert_eq!(app.values().get("key").map(String::as_str), Some("next"));
        StateMachine::import_snapshot(app.export_snapshot(), app.last_applied()).unwrap();
    }
    #[test]
    fn stale_registration_index_cannot_remove_an_existing_session_during_rollback() {
        let mut app = baseline();
        let before = app.clone();
        {
            let mut transaction = app.transaction();
            assert!(transaction
                .apply(&log(
                    1,
                    Command::RegisterSession {
                        nonce: "wrong".into()
                    }
                ))
                .is_err());
        }
        assert_eq!(app, before);
    }
    #[test]
    fn panic_during_external_commit_rolls_back_before_state_can_be_reused() {
        let mut app = baseline();
        let before = app.clone();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut transaction = app.transaction();
            transaction
                .apply(&log(3, put(1, "key", 2, "provisional")))
                .unwrap();
            panic!("injected persistence panic");
        }));
        assert!(result.is_err());
        assert_eq!(app, before);
    }
}
