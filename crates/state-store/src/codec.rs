use crate::{
    digest, invalid, validate_changes, validate_range, Mutation, Record, MAX_BATCH_BYTES,
    MAX_BATCH_RECORDS, MAX_KEY_BYTES, MAX_VALUE_BYTES,
};
use std::io::{self, Read};

pub(crate) const MAX_FRAME: usize = MAX_BATCH_BYTES + MAX_BATCH_RECORDS * 16 + 24;

pub(crate) fn encode_record(record: &Record, out: &mut Vec<u8>) {
    out.extend(record.sequence.to_le_bytes());
    out.extend((record.key.len() as u32).to_le_bytes());
    out.extend(
        record
            .value
            .as_ref()
            .map_or(u32::MAX, |v| v.len() as u32)
            .to_le_bytes(),
    );
    out.extend(&record.key);
    if let Some(value) = &record.value {
        out.extend(value);
    }
}

pub(crate) fn decode_records(mut bytes: &[u8], count: usize) -> io::Result<Vec<Record>> {
    if count > MAX_BATCH_RECORDS {
        return Err(invalid("record count exceeds limit"));
    }
    let mut records = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let mut header = [0; 16];
        bytes
            .read_exact(&mut header)
            .map_err(|_| invalid("short record header"))?;
        let sequence = u64::from_le_bytes(header[0..8].try_into().expect("fixed header"));
        let key_len = u32::from_le_bytes(header[8..12].try_into().expect("fixed header")) as usize;
        let value_len = u32::from_le_bytes(header[12..16].try_into().expect("fixed header"));
        let length = if value_len == u32::MAX {
            0
        } else {
            value_len as usize
        };
        if key_len == 0
            || key_len > MAX_KEY_BYTES
            || length > MAX_VALUE_BYTES
            || key_len + length > bytes.len()
        {
            return Err(invalid("record bounds invalid"));
        }
        let key = bytes[..key_len].to_vec();
        let value = (value_len != u32::MAX).then(|| bytes[key_len..key_len + length].to_vec());
        bytes = &bytes[key_len + length..];
        records.push(Record {
            key,
            value,
            sequence,
        });
    }
    if !bytes.is_empty() {
        return Err(invalid("trailing record data"));
    }
    Ok(records)
}

pub(crate) fn encode_batch(first: u64, index: u64, changes: &[Mutation]) -> io::Result<Vec<u8>> {
    validate_range(first, index)?;
    validate_changes(changes)?;
    let mut bytes = Vec::new();
    bytes.extend(first.to_le_bytes());
    bytes.extend(index.to_le_bytes());
    bytes.extend((changes.len() as u32).to_le_bytes());
    for change in changes {
        encode_record(
            &Record {
                key: change.key.clone(),
                value: change.value.clone(),
                sequence: index,
            },
            &mut bytes,
        );
    }
    Ok(bytes)
}

pub(crate) fn decode_batch(bytes: &[u8]) -> io::Result<(u64, u64, Vec<Mutation>)> {
    if bytes.len() < 20 {
        return Err(invalid("short batch"));
    }
    let first = u64::from_le_bytes(bytes[..8].try_into().expect("checked size"));
    let index = u64::from_le_bytes(bytes[8..16].try_into().expect("checked size"));
    validate_range(first, index).map_err(|error| invalid(error.to_string()))?;
    let count = u32::from_le_bytes(bytes[16..20].try_into().expect("checked size")) as usize;
    let records = decode_records(&bytes[20..], count)?;
    if records.iter().any(|record| record.sequence != index) {
        return Err(invalid("mixed batch sequence"));
    }
    let changes: Vec<_> = records
        .into_iter()
        .map(|record| Mutation {
            key: record.key,
            value: record.value,
        })
        .collect();
    validate_changes(&changes).map_err(|error| invalid(error.to_string()))?;
    Ok((first, index, changes))
}

pub(crate) fn frame(magic: &[u8; 8], payload: &[u8]) -> io::Result<Vec<u8>> {
    if payload.len() > MAX_FRAME {
        return Err(invalid("frame too large"));
    }
    let mut bytes = Vec::with_capacity(payload.len() + 76);
    bytes.extend(magic);
    bytes.extend((payload.len() as u32).to_le_bytes());
    // Check the fixed header before trusting its length. Corruption must not
    // turn a complete acknowledged frame into an apparently incomplete tail.
    bytes.extend(digest(&bytes));
    bytes.extend(payload);
    bytes.extend(digest(&bytes));
    Ok(bytes)
}

/// None means EOF or an incomplete final frame. Invalid complete bytes fail.
pub(crate) fn read_frame(
    reader: &mut impl Read,
    magic: &[u8; 8],
) -> io::Result<Option<(Vec<u8>, usize)>> {
    let mut header = [0; 44];
    let mut got = 0;
    while got < header.len() {
        match reader.read(&mut header[got..]) {
            Ok(0) => return Ok(None),
            Ok(n) => got += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    if &header[..8] != magic {
        return Err(invalid("frame magic/version mismatch"));
    }
    if digest(&header[..12]).as_slice() != &header[12..] {
        return Err(invalid("frame header checksum mismatch"));
    }
    let length = u32::from_le_bytes(header[8..12].try_into().expect("fixed header")) as usize;
    if length > MAX_FRAME {
        return Err(invalid("frame length exceeds limit"));
    }
    let mut rest = vec![0; length + 32];
    match reader.read_exact(&mut rest) {
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
        Ok(()) => {}
    }
    let mut check = header.to_vec();
    check.extend(&rest[..length]);
    if digest(&check).as_slice() != &rest[length..] {
        return Err(invalid("frame checksum mismatch"));
    }
    rest.truncate(length);
    Ok(Some((rest, length + 76)))
}
