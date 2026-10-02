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
    if name == crate::EVENT_BLOOM_FILE {
        return crate::event_bloom::verify_source_membership(source, path, file_id);
    }
    if matches!(
        name,
        transfer_bloom::ERC20_EVENTS_BLOOM_FILE | transfer_bloom::TRANSFER_BLOOM_FILE
    ) {
        return transfer_bloom::verify_source_membership(source, path, name, file_id);
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
    let names: Vec<_> = fields.iter().map(|field| field.name()).collect();
    let reader = SegmentReader::open_projected(source, &names)?;
    let rows = u32::try_from(reader.read_row_count()?)
        .map_err(|_| invalid("source exceeds index row-ID limit"))?;
    let columns = fields
        .iter()
        .map(|&field| Column::read(&reader, field, rows as usize))
        .collect::<io::Result<Vec<_>>>()?;
    let width = fields.iter().map(|field| field.width()).sum::<usize>();
    let index = BTreeIndexReader::open_bound(path, file_id)?;
    if index.key_size() != width {
        return Err(invalid("index key width differs from its source fields"));
    }
    let mut scratch = [0; 84];
    let mut seen = RoaringBitmap::new();
    for (key, bitmap) in index.entries() {
        if bitmap.is_empty() {
            return Err(invalid("index contains an empty key entry"));
        }
        for row in bitmap {
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
        if source_key(row as usize, fields, &columns, &mut scratch).is_some() != seen.contains(row)
        {
            return Err(invalid(format!("index omits required source row {row}")));
        }
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
