//! Exhaustive comparison of derived entries with their captured source columns.
//! This never establishes Ethereum provenance; verified ingestion establishes
//! that separately. Every physical row is checked, including retained fork rows
//! that query execution subsequently masks with the canonical bitmap.
use std::{io, path::Path};

use alloy_primitives::{Address, B256};
use logex_storage::SegmentReader;
use roaring::RoaringBitmap;

use crate::{btree::BTreeIndexReader, transfer_bloom};

#[derive(Clone, Copy)]
enum Field {
    Address,
    Topic(u8),
    BlockNumber,
    Timestamp,
    BlockHash,
}

impl Field {
    fn name(self) -> &'static str {
        match self {
            Self::Address => "address",
            Self::Topic(0) => "topic0",
            Self::Topic(1) => "topic1",
            Self::Topic(2) => "topic2",
            Self::Topic(_) => unreachable!("only indexed topic fields are declared"),
            Self::BlockNumber => "block_number",
            Self::Timestamp => "timestamp",
            Self::BlockHash => "block_hash",
        }
    }

    fn width(self) -> usize {
        match self {
            Self::Address => 20,
            Self::Topic(_) | Self::BlockHash => 32,
            Self::BlockNumber | Self::Timestamp => 8,
        }
    }
}

enum Column {
    Addresses(Vec<Address>),
    Topics(Vec<Option<B256>>),
    Hashes(Vec<B256>),
    Numbers(Vec<u64>),
}

impl Column {
    fn read(reader: &SegmentReader, field: Field, rows: usize) -> io::Result<Self> {
        let column = match field {
            Field::Address => Self::Addresses(reader.read_address(None)?),
            Field::Topic(_) => Self::Topics(reader.read_nullable_b256(field.name(), None)?),
            Field::BlockHash => Self::Hashes(reader.read_b256(field.name(), None)?),
            Field::BlockNumber | Field::Timestamp => {
                Self::Numbers(reader.read_u64(field.name(), None)?)
            }
        };
        let actual = match &column {
            Self::Addresses(values) => values.len(),
            Self::Topics(values) => values.len(),
            Self::Hashes(values) => values.len(),
            Self::Numbers(values) => values.len(),
        };
        if actual != rows {
            return Err(invalid(format!(
                "source column {} has {actual} rows, expected {rows}",
                field.name()
            )));
        }
        Ok(column)
    }

    fn write_key(&self, row: usize, output: &mut [u8]) -> bool {
        match self {
            Self::Addresses(values) => output.copy_from_slice(values[row].as_slice()),
            Self::Hashes(values) => output.copy_from_slice(values[row].as_slice()),
            Self::Numbers(values) => output.copy_from_slice(&values[row].to_be_bytes()),
            Self::Topics(values) => match values[row] {
                Some(value) => output.copy_from_slice(value.as_slice()),
                None => return false,
            },
        }
        true
    }
}

/// Called while the caller holds either the exclusive build checkpoint or the
/// published checkpoint's shared lock. Source ownership must prevent mutation.
/// One index and its selected source columns are retained at a time; this has
/// the same per-segment row bound as building, not a total-RSS guarantee.
pub(crate) fn verify_artifact(
    source: &Path,
    path: &Path,
    name: &str,
    file_id: [u8; 16],
) -> io::Result<()> {
    let reader = SegmentReader::open_projected(source, source_columns(name)?)?;
    verify_captured_artifact(&reader, path, name, file_id, &|| false)
}

/// Read only retained source artifacts. Publication ownership keeps the index
/// files stable; later source appends/compaction cannot change this reader's
/// captured prefix. The owner must re-admit its storage view before completion.
pub(crate) fn verify_captured_artifact(
    reader: &SegmentReader,
    path: &Path,
    name: &str,
    file_id: [u8; 16],
    cancelled: &dyn Fn() -> bool,
) -> io::Result<()> {
    check_cancelled(cancelled)?;
    if name == crate::EVENT_BLOOM_FILE {
        return crate::event_bloom::verify_captured_membership(reader, path, file_id, cancelled);
    }
    if matches!(
        name,
        transfer_bloom::ERC20_EVENTS_BLOOM_FILE | transfer_bloom::TRANSFER_BLOOM_FILE
    ) {
        return transfer_bloom::verify_captured_membership(reader, path, name, file_id, cancelled);
    }
    use Field::*;
    let fields: &[Field] = match name {
        "address.bptree" => &[Address],
        "topic0.bptree" => &[Topic(0)],
        "block_number.bptree" => &[BlockNumber],
        "timestamp.bptree" => &[Timestamp],
        "block_hash.bptree" => &[BlockHash],
        "address_topic0.bptree" => &[Address, Topic(0)],
        "address_topic0_block.bptree" => &[Address, Topic(0), BlockNumber],
        "topic0_topic1.bptree" => &[Topic(0), Topic(1)],
        "address_topic0_topic1.bptree" => &[Address, Topic(0), Topic(1)],
        "address_topic0_topic2.bptree" => &[Address, Topic(0), Topic(2)],
        _ => return Err(invalid(format!("unknown derived index {name}"))),
    };
    let rows = u32::try_from(reader.read_row_count()?)
        .map_err(|_| invalid("source exceeds index row-ID limit"))?;
    let columns = fields
        .iter()
        .map(|&field| {
            check_cancelled(cancelled)?;
            Column::read(reader, field, rows as usize)
        })
        .collect::<io::Result<Vec<_>>>()?;
    let width = fields.iter().map(|field| field.width()).sum::<usize>();
    let index = BTreeIndexReader::open_bound(path, file_id)?;
    if index.key_size() != width {
        return Err(invalid("index key width differs from its source fields"));
    }
    let mut scratch = [0; 84];
    let mut seen = RoaringBitmap::new();
    for (key, bitmap) in index.entries() {
        check_cancelled(cancelled)?;
        if bitmap.is_empty() {
            return Err(invalid("index contains an empty key entry"));
        }
        for row in bitmap {
            if row.is_multiple_of(16_384) {
                check_cancelled(cancelled)?;
            }
            if row >= rows {
                return Err(invalid("index row ID exceeds source boundary"));
            }
            if !seen.insert(row) {
                return Err(invalid("index repeats a source row under multiple keys"));
            }
            if source_key(row as usize, fields, &columns, &mut scratch) != Some(key) {
                return Err(invalid(format!("index key differs from source row {row}")));
            }
        }
    }
    for row in 0..rows {
        if row.is_multiple_of(16_384) {
            check_cancelled(cancelled)?;
        }
        if source_key(row as usize, fields, &columns, &mut scratch).is_some() != seen.contains(row)
        {
            return Err(invalid(format!("index omits required source row {row}")));
        }
    }
    check_cancelled(cancelled)
}

pub(crate) fn source_columns(name: &str) -> io::Result<&'static [&'static str]> {
    Ok(match name {
        crate::EVENT_BLOOM_FILE => &["address", "topic0", "topic1", "topic2", "topic3"],
        transfer_bloom::ERC20_EVENTS_BLOOM_FILE | transfer_bloom::TRANSFER_BLOOM_FILE => {
            &["address", "topic0", "topic1", "topic2"]
        }
        "address.bptree" => &["address"],
        "topic0.bptree" => &["topic0"],
        "block_number.bptree" => &["block_number"],
        "timestamp.bptree" => &["timestamp"],
        "block_hash.bptree" => &["block_hash"],
        "address_topic0.bptree" => &["address", "topic0"],
        "address_topic0_block.bptree" => &["address", "topic0", "block_number"],
        "topic0_topic1.bptree" => &["topic0", "topic1"],
        "address_topic0_topic1.bptree" => &["address", "topic0", "topic1"],
        "address_topic0_topic2.bptree" => &["address", "topic0", "topic2"],
        _ => return Err(invalid(format!("unknown derived index {name}"))),
    })
}

pub(crate) fn check_cancelled(cancelled: &dyn Fn() -> bool) -> io::Result<()> {
    if cancelled() {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "index verification cancelled",
        ));
    }
    Ok(())
}

fn source_key<'a>(
    row: usize,
    fields: &[Field],
    columns: &[Column],
    scratch: &'a mut [u8; 84],
) -> Option<&'a [u8]> {
    let mut offset = 0;
    for (field, column) in fields.iter().zip(columns) {
        let end = offset + field.width();
        if !column.write_key(row, &mut scratch[offset..end]) {
            return None;
        }
        offset = end;
    }
    Some(&scratch[..offset])
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
