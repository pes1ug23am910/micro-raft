use crate::bloom::Bloom;
use crate::codec::{decode_records, encode_record, frame, read_frame};
use crate::{invalid, Record, MAX_BATCH_RECORDS};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const HEADER: &[u8; 8] = b"MRSTBL01";
const BLOCK: &[u8; 8] = b"MRSBLK01";
const FOOTER: &[u8; 8] = b"MRSIDX01";
const END: &[u8; 8] = b"MRSEND01";
const BLOCK_TARGET: usize = 16 * 1024;
const MAX_TABLE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_FOOTER_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TableMeta {
    pub id: u64,
    pub level: usize,
    pub bytes: u64,
    pub first: Vec<u8>,
    pub last: Vec<u8>,
    pub min_sequence: u64,
    pub max_sequence: u64,
    pub records: usize,
}

impl TableMeta {
    pub fn filename(&self) -> String {
        format!("sst-{:020}.sst", self.id)
    }
    pub fn contains(&self, key: &[u8]) -> bool {
        self.first.as_slice() <= key && key <= self.last.as_slice()
    }
    pub fn overlaps(&self, first: &[u8], last: &[u8]) -> bool {
        self.first.as_slice() <= last && first <= self.last.as_slice()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BlockMeta {
    offset: u64,
    bytes: u64,
    first: Vec<u8>,
    last: Vec<u8>,
    records: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Footer {
    version: u32,
    blocks: Vec<BlockMeta>,
    bloom: Bloom,
    records: usize,
}

/// Caller syncs the new file and containing directory before manifest selection.
pub(crate) fn write_table(
    path: &Path,
    id: u64,
    level: usize,
    records: &[Record],
) -> io::Result<(TableMeta, File)> {
    if records.is_empty()
        || records.len() > MAX_BATCH_RECORDS
        || records.windows(2).any(|pair| pair[0].key >= pair[1].key)
        || records.iter().any(|record| record.sequence == 0)
    {
        return Err(invalid(
            "table records must be nonempty, ordered and versioned",
        ));
    }
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(HEADER)?;
    let mut blocks = Vec::new();
    let mut start = 0;
    while start < records.len() {
        let mut end = start;
        let mut payload = vec![0; 4];
        while end < records.len() && (end == start || payload.len() < BLOCK_TARGET) {
            encode_record(&records[end], &mut payload);
            end += 1;
        }
        payload[..4].copy_from_slice(&((end - start) as u32).to_le_bytes());
        let encoded = frame(BLOCK, &payload)?;
        let offset = file.stream_position()?;
        file.write_all(&encoded)?;
        blocks.push(BlockMeta {
            offset,
            bytes: encoded.len() as u64,
            first: records[start].key.clone(),
            last: records[end - 1].key.clone(),
            records: end - start,
        });
        start = end;
    }
    let keys: Vec<_> = records.iter().map(|record| record.key.as_slice()).collect();
    let footer = Footer {
        version: 1,
        blocks,
        bloom: Bloom::build(&keys),
        records: records.len(),
    };
    let bytes = frame(
        FOOTER,
        &serde_json::to_vec(&footer).map_err(io::Error::other)?,
    )?;
    if bytes.len() as u64 > MAX_FOOTER_BYTES {
        return Err(invalid("table index exceeds limit"));
    }
    file.write_all(&bytes)?;
    file.write_all(&(bytes.len() as u64).to_le_bytes())?;
    file.write_all(END)?;
    let length = file.stream_position()?;
    if length > MAX_TABLE_BYTES {
        return Err(invalid("table byte limit exceeded"));
    }
    Ok((
        TableMeta {
            id,
            level,
            bytes: length,
            first: records[0].key.clone(),
            last: records[records.len() - 1].key.clone(),
            min_sequence: records
                .iter()
                .map(|record| record.sequence)
                .min()
                .expect("nonempty"),
            max_sequence: records
                .iter()
                .map(|record| record.sequence)
                .max()
                .expect("nonempty"),
            records: records.len(),
        },
        file,
    ))
}

#[derive(Clone)]
pub(crate) struct Table {
    path: PathBuf,
    pub meta: TableMeta,
    footer: Footer,
}

pub(crate) struct Lookup {
    pub record: Option<Record>,
    pub bloom_positive: bool,
    pub block_bytes: u64,
}

impl Table {
    pub fn open(path: PathBuf, meta: TableMeta) -> io::Result<Self> {
        let mut file = File::open(&path)?;
        let length = file.metadata()?.len();
        if length != meta.bytes
            || !(68..=MAX_TABLE_BYTES).contains(&length)
            || meta.level > 2
            || meta.id == 0
            || meta.first.is_empty()
            || meta.first > meta.last
            || meta.min_sequence == 0
            || meta.min_sequence > meta.max_sequence
            || meta.records == 0
            || meta.records > MAX_BATCH_RECORDS
        {
            return Err(invalid("table metadata invalid"));
        }
        let mut header = [0; 8];
        file.read_exact(&mut header)?;
        if &header != HEADER {
            return Err(invalid("table header/version mismatch"));
        }
        file.seek(SeekFrom::End(-16))?;
        let mut trailer = [0; 16];
        file.read_exact(&mut trailer)?;
        if &trailer[8..] != END {
            return Err(invalid("table footer marker missing"));
        }
        let footer_len = u64::from_le_bytes(trailer[..8].try_into().expect("fixed trailer"));
        if footer_len > MAX_FOOTER_BYTES || footer_len + 24 > length {
            return Err(invalid("footer length invalid"));
        }
        let footer_at = length - 16 - footer_len;
        file.seek(SeekFrom::Start(footer_at))?;
        let (payload, consumed) =
            read_frame(&mut file, FOOTER)?.ok_or_else(|| invalid("incomplete table index"))?;
        if consumed as u64 != footer_len {
            return Err(invalid("table index length mismatch"));
        }
        let footer: Footer = serde_json::from_slice(&payload).map_err(invalid_json)?;
        if footer.version != 1
            || footer.records != meta.records
            || footer.blocks.is_empty()
            || footer.blocks.len() > meta.records
        {
            return Err(invalid("invalid table index"));
        }
        footer.bloom.validate(meta.records)?;
        let mut next = 8;
        let mut count = 0usize;
        for (number, block) in footer.blocks.iter().enumerate() {
            if block.offset != next
                || block.bytes < 48
                || block.bytes > MAX_TABLE_BYTES
                || block.first > block.last
                || block.records == 0
                || block.records > MAX_BATCH_RECORDS
                || (number > 0 && footer.blocks[number - 1].last >= block.first)
            {
                return Err(invalid("invalid block index/order"));
            }
            next = next
                .checked_add(block.bytes)
                .ok_or_else(|| invalid("block offset overflow"))?;
            count = count
                .checked_add(block.records)
                .ok_or_else(|| invalid("block record count overflow"))?;
            if next > footer_at || count > meta.records {
                return Err(invalid("block index exceeds table"));
            }
        }
        if next != footer_at
            || count != meta.records
            || footer.blocks[0].first != meta.first
            || footer.blocks.last().expect("nonempty").last != meta.last
        {
            return Err(invalid("table index bounds mismatch"));
        }
        let table = Self { path, meta, footer };
        let mut low = u64::MAX;
        let mut high = 0;
        for item in table.iter()? {
            let record = item?;
            if !table.footer.bloom.contains(&record.key) {
                return Err(invalid("Bloom false negative in stored table"));
            }
            low = low.min(record.sequence);
            high = high.max(record.sequence);
        }
        if low != table.meta.min_sequence || high != table.meta.max_sequence {
            return Err(invalid("table sequence range mismatch"));
        }
        Ok(table)
    }

    pub fn lookup(&self, key: &[u8]) -> io::Result<Lookup> {
        if !self.footer.bloom.contains(key) {
            return Ok(Lookup {
                record: None,
                bloom_positive: false,
                block_bytes: 0,
            });
        }
        let number = self
            .footer
            .blocks
            .partition_point(|block| block.last.as_slice() < key);
        let Some(block) = self
            .footer
            .blocks
            .get(number)
            .filter(|block| block.first.as_slice() <= key)
        else {
            return Ok(Lookup {
                record: None,
                bloom_positive: true,
                block_bytes: 0,
            });
        };
        let records = read_block(&mut File::open(&self.path)?, block)?;
        let record = records
            .binary_search_by(|record| record.key.as_slice().cmp(key))
            .ok()
            .map(|i| records[i].clone());
        Ok(Lookup {
            record,
            bloom_positive: true,
            block_bytes: block.bytes,
        })
    }

    pub fn iter(&self) -> io::Result<TableIterator> {
        Ok(TableIterator {
            file: File::open(&self.path)?,
            blocks: self.footer.blocks.clone().into(),
            records: VecDeque::new(),
            failed: false,
        })
    }
}

fn invalid_json(error: serde_json::Error) -> io::Error {
    invalid(error.to_string())
}

fn read_block(file: &mut File, block: &BlockMeta) -> io::Result<Vec<Record>> {
    file.seek(SeekFrom::Start(block.offset))?;
    let (payload, consumed) =
        read_frame(file, BLOCK)?.ok_or_else(|| invalid("incomplete data block"))?;
    if consumed as u64 != block.bytes || payload.len() < 4 {
        return Err(invalid("data block size mismatch"));
    }
    let count = u32::from_le_bytes(payload[..4].try_into().expect("checked")) as usize;
    let records = decode_records(&payload[4..], count)?;
    if count != block.records
        || records.is_empty()
        || records.windows(2).any(|pair| pair[0].key >= pair[1].key)
        || records[0].key != block.first
        || records[count - 1].key != block.last
    {
        return Err(invalid("data block order/index mismatch"));
    }
    Ok(records)
}

pub(crate) struct TableIterator {
    file: File,
    blocks: VecDeque<BlockMeta>,
    records: VecDeque<Record>,
    failed: bool,
}
impl Iterator for TableIterator {
    type Item = io::Result<Record>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        if let Some(record) = self.records.pop_front() {
            return Some(Ok(record));
        }
        let block = self.blocks.pop_front()?;
        match read_block(&mut self.file, &block) {
            Ok(records) => {
                self.records = records.into();
                self.records.pop_front().map(Ok)
            }
            Err(error) => {
                self.failed = true;
                Some(Err(error))
            }
        }
    }
}
