//! Strict carrier adapter; durable publication remains owned by kv-node Storage.
use crate::machine::Machine;
use crate::types::{canonical_bytes, sha256, GroupId, MAX_ACTOR_BYTES};
use kv_node::application::{ApplicationSnapshot, StateMachine};
use kv_node::snapshot::{SnapshotImage, SnapshotMetadata};
use serde::{Deserialize, Serialize};
use std::io;

const PREFIX: &str = "@micro-raft/shard-snapshot/v1/";
const RAW_CHUNK: usize = 16 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    version: u32,
    group: GroupId,
    bytes: usize,
    chunks: usize,
    sha256: [u8; 32],
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(DIGITS[usize::from(byte >> 4)] as char);
        result.push(DIGITS[usize::from(byte & 15)] as char);
    }
    result
}
fn unhex(text: &str) -> io::Result<Vec<u8>> {
    if !text.len().is_multiple_of(2) || text.len() > RAW_CHUNK * 2 {
        return Err(invalid("invalid carrier chunk length"));
    }
    fn digit(byte: u8) -> io::Result<u8> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(invalid("noncanonical carrier hex")),
        }
    }
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| Ok(digit(pair[0])? * 16 + digit(pair[1])?))
        .collect()
}

pub fn capture(metadata: SnapshotMetadata, machine: &Machine) -> io::Result<SnapshotImage> {
    machine.validate().map_err(invalid)?;
    if metadata.last_included_index != machine.applied() {
        return Err(invalid("actor snapshot boundary mismatch"));
    }
    let bytes = canonical_bytes(machine, MAX_ACTOR_BYTES)
        .map_err(|e| invalid(format!("actor encoding: {e:?}")))?;
    let header = Header {
        version: 1,
        group: machine.group(),
        bytes: bytes.len(),
        chunks: bytes.len().div_ceil(RAW_CHUNK),
        sha256: sha256(&bytes),
    };
    let mut values = vec![(
        format!("{PREFIX}header"),
        serde_json::to_string(&header).map_err(io::Error::other)?,
    )];
    values.extend(
        bytes
            .chunks(RAW_CHUNK)
            .enumerate()
            .map(|(number, chunk)| (format!("{PREFIX}chunk/{number:06}"), hex(chunk))),
    );
    values.sort_by(|a, b| a.0.cmp(&b.0));
    let carrier = StateMachine::import_snapshot(
        ApplicationSnapshot {
            schema_version: 1,
            last_applied: machine.applied(),
            values,
            sessions: vec![],
            retained_keys: 0,
            retained_payload_bytes: 0,
        },
        machine.applied(),
    )
    .map_err(|e| invalid(e.to_string()))?;
    SnapshotImage::new(metadata, &carrier)
}

pub fn restore(image: &SnapshotImage, expected_group: GroupId) -> io::Result<Machine> {
    let carrier = image.application().export_snapshot();
    if !carrier.sessions.is_empty()
        || carrier.retained_keys != 0
        || carrier.retained_payload_bytes != 0
        || carrier.last_applied != image.descriptor().metadata.last_included_index
    {
        return Err(invalid("invalid actor carrier state"));
    }
    let values = &image.application().values();
    let raw_header = values
        .get(&format!("{PREFIX}header"))
        .ok_or_else(|| invalid("actor carrier header missing"))?;
    let header: Header = serde_json::from_str(raw_header).map_err(|e| invalid(e.to_string()))?;
    if header.version != 1
        || header.group != expected_group
        || header.bytes == 0
        || header.bytes > MAX_ACTOR_BYTES
        || header.chunks != header.bytes.div_ceil(RAW_CHUNK)
        || values.len() != header.chunks + 1
        || serde_json::to_string(&header).map_err(io::Error::other)? != *raw_header
    {
        return Err(invalid("actor carrier identity/count invalid"));
    }
    let mut bytes = Vec::with_capacity(header.bytes);
    for number in 0..header.chunks {
        let raw = values
            .get(&format!("{PREFIX}chunk/{number:06}"))
            .ok_or_else(|| invalid("actor carrier chunk missing"))?;
        let chunk = unhex(raw)?;
        if chunk.len() != RAW_CHUNK.min(header.bytes - bytes.len()) {
            return Err(invalid("actor carrier chunk boundary invalid"));
        }
        bytes.extend(chunk);
    }
    if bytes.len() != header.bytes || sha256(&bytes) != header.sha256 {
        return Err(invalid("actor carrier digest mismatch"));
    }
    let machine: Machine = serde_json::from_slice(&bytes).map_err(|e| invalid(e.to_string()))?;
    if machine.group() != expected_group
        || machine.applied() != carrier.last_applied
        || canonical_bytes(&machine, MAX_ACTOR_BYTES).map_err(|_| invalid("actor exceeds bound"))?
            != bytes
    {
        return Err(invalid("actor payload identity/boundary invalid"));
    }
    machine.validate().map_err(invalid)?;
    Ok(machine)
}
