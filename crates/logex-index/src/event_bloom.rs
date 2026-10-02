//! Compact emitter/event presence and indexed-argument skip filters.
//!
//! Topic values are indexed as bytes, without interpreting an ABI. Anonymous
//! events work too when the query constrains their first topic. A filter only
//! excludes segments; possible matches still require exact column predicates.
use std::{
    io::{self, Read},
    path::Path,
};

use alloy_primitives::{Address, B256, keccak256};
use logex_storage::SegmentReader;
use logex_types::QueryMemoryBudget;

use crate::{
    bloom::{HASH_ROUNDS, HEADER_LEN, filter_bytes, key_bits, open_bloom, read_key},
    builder::validate_source_rows,
    index_file::{IndexFile, write_index_file},
};

const MAGIC: &[u8; 8] = b"LXEVBF1\0";
pub const EVENT_BLOOM_FILE: &str = "events.bloom";

/// At most 2 MiB of bits per segment, regardless of event type or row count.
pub struct EventBloom;

impl EventBloom {
    pub fn build(source: &Path, index_dir: &Path) -> io::Result<()> {
        let reader = source_reader(source)?;
        let rows = source_rows(&reader)?;
        let mut bits = vec![0; filter_bytes(rows)];
        // Only one argument column is decoded at a time.
        visit_source_keys(&reader, |_, hashes| {
            let mask = bits.len() as u64 * 8 - 1;
            for bit in key_bits(hashes, mask) {
                bits[(bit / 8) as usize] |= 1 << (bit % 8);
            }
            Ok(())
        })?;
        write_index_file(
            &index_dir.join(EVENT_BLOOM_FILE),
            HEADER_LEN + bits.len() as u64,
            |writer| {
                writer.write_all(MAGIC)?;
                writer.write_all(&(bits.len() as u64 * 8).to_le_bytes())?;
                writer.write_all(&(HASH_ROUNDS as u32).to_le_bytes())?;
                writer.write_all(&bits)
            },
        )
    }
}

pub struct EventBloomReader {
    reader: IndexFile,
    mask: u64,
}

impl EventBloomReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let (reader, mask) = open_bloom(IndexFile::open(path)?, MAGIC)?;
        Ok(Self { reader, mask })
    }

    /// Keep the source publication checkpoint guard alive for this reader.
    pub fn open_bound(path: &Path, expected_file_id: [u8; 16]) -> io::Result<Self> {
        let (reader, mask) = open_bloom(IndexFile::open_bound(path, expected_file_id)?, MAGIC)?;
        Ok(Self { reader, mask })
    }

    /// Charge protected page/checksum caches to the query's existing budget.
    pub fn open_bound_with_memory(
        path: &Path,
        expected_file_id: [u8; 16],
        memory: &QueryMemoryBudget,
    ) -> io::Result<Self> {
        let (reader, mask) = open_bloom(
            IndexFile::open_bound_with_memory(path, expected_file_id, Some(memory))?,
            MAGIC,
        )?;
        Ok(Self { reader, mask })
    }

    pub fn may_contain_event(&mut self, topic0: &B256, address: &Address) -> io::Result<bool> {
        read_key(
            &mut self.reader,
            self.mask,
            key_hashes(topic0, address, 0, &B256::ZERO),
        )
    }

    pub fn may_contain_topic(
        &mut self,
        topic0: &B256,
        address: &Address,
        position: usize,
        value: &B256,
    ) -> io::Result<bool> {
        if !(1..=3).contains(&position) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "indexed topic position must be 1, 2 or 3",
            ));
        }
        read_key(
            &mut self.reader,
            self.mask,
            key_hashes(topic0, address, position as u8, value),
        )
    }
}

/// Check every source membership before publication and during explicit scrub.
/// Reading the whole bounded vector also validates every protected file page.
pub(crate) fn verify_source_membership(
    source: &Path,
    path: &Path,
    file_id: [u8; 16],
) -> io::Result<()> {
    let (mut file, mask) = open_bloom(IndexFile::open_bound(path, file_id)?, MAGIC)?;
    let mut bits = vec![0; ((mask + 1) / 8) as usize];
    file.read_exact(&mut bits)?;
    let reader = source_reader(source)?;
    visit_source_keys(&reader, |row, hashes| {
        if key_bits(hashes, mask).any(|bit| bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("bloom omits source row {row}"),
            ));
        }
        Ok(())
    })
}

fn source_reader(source: &Path) -> io::Result<SegmentReader> {
    SegmentReader::open_projected(source, &["address", "topic0", "topic1", "topic2", "topic3"])
}

fn source_rows(reader: &SegmentReader) -> io::Result<usize> {
    u32::try_from(reader.read_row_count()?)
        .map(|rows| rows as usize)
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "source exceeds index row-ID limit",
            )
        })
}

fn visit_source_keys(
    reader: &SegmentReader,
    mut visit: impl FnMut(usize, (u64, u64)) -> io::Result<()>,
) -> io::Result<()> {
    source_rows(reader)?;
    let addresses = reader.read_address(None)?;
    let events = reader.read_nullable_b256("topic0", None)?;
    validate_source_rows(
        reader,
        &[("address", addresses.len()), ("topic0", events.len())],
    )?;
    for (row, event) in events.iter().enumerate() {
        if let Some(event) = event {
            visit(row, key_hashes(event, &addresses[row], 0, &B256::ZERO))?;
        }
    }
    for (position, name) in [(1, "topic1"), (2, "topic2"), (3, "topic3")] {
        let values = reader.read_nullable_b256(name, None)?;
        validate_source_rows(reader, &[(name, values.len())])?;
        for (row, (event, value)) in events.iter().zip(&values).enumerate() {
            if let (Some(event), Some(value)) = (event, value) {
                visit(row, key_hashes(event, &addresses[row], position, value))?;
            }
        }
    }
    Ok(())
}

fn key_hashes(topic0: &B256, address: &Address, position: u8, value: &B256) -> (u64, u64) {
    // Position zero is a separate presence domain, including for zero arguments.
    let mut key = [0; 8 + 32 + 20 + 1 + 32];
    key[..8].copy_from_slice(MAGIC);
    key[8..40].copy_from_slice(topic0.as_slice());
    key[40..60].copy_from_slice(address.as_slice());
    key[60] = position;
    key[61..].copy_from_slice(value.as_slice());
    let hash = keccak256(key);
    let h1 = u64::from_le_bytes(hash[..8].try_into().expect("eight hash bytes"));
    let h2 = u64::from_le_bytes(hash[8..16].try_into().expect("eight hash bytes")) | 1;
    (h1, h2)
}

#[cfg(test)]
mod tests {
    use super::*;
    use logex_storage::ColumnFile;
    use logex_types::{LogRow, QueryMemoryError, QueryMemoryLimit, Source};
    use tempfile::TempDir;

    fn rows() -> Vec<LogRow> {
        [
            (
                Some(keccak256(b"Deposit(address,uint256)")),
                [Some(B256::ZERO), None, None],
            ),
            (
                Some(keccak256(b"Withdrawal(address,uint256)")),
                [Some(B256::repeat_byte(1)), None, None],
            ),
            (
                Some(crate::transfer_topic0()),
                [
                    Some(B256::repeat_byte(2)),
                    Some(B256::repeat_byte(3)),
                    Some(B256::repeat_byte(4)),
                ],
            ),
            (
                Some(keccak256(
                    b"TransferSingle(address,address,address,uint256,uint256)",
                )),
                [
                    Some(B256::repeat_byte(5)),
                    Some(B256::repeat_byte(6)),
                    Some(B256::repeat_byte(7)),
                ],
            ),
            (Some(B256::repeat_byte(8)), [None, None, None]),
            (None, [None, None, None]),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, (event, topics))| LogRow {
            address: Address::repeat_byte(index as u8),
            topic0: event,
            topic1: topics[0],
            topic2: topics[1],
            topic3: topics[2],
            block_number: 10,
            block_hash: B256::repeat_byte(10),
            timestamp: 20,
            tx_hash: B256::repeat_byte(11),
            tx_index: 0,
            log_index: index as u32,
            data: Default::default(),
            data_len: 0,
            source: Source::Receipt,
        })
        .collect()
    }

    fn build(source: &Path, rows: &[LogRow]) -> std::path::PathBuf {
        ColumnFile::write_batch(source, rows).unwrap();
        let indexes = source.join("indexes");
        std::fs::create_dir_all(&indexes).unwrap();
        EventBloom::build(source, &indexes).unwrap();
        indexes.join(EVENT_BLOOM_FILE)
    }

    #[test]
    fn arbitrary_events_all_positions_and_presence_match_independent_bits() {
        let source = TempDir::new().unwrap();
        let rows = rows();
        let path = build(source.path(), &rows);
        let mut raw = Vec::new();
        IndexFile::open(&path)
            .unwrap()
            .read_to_end(&mut raw)
            .unwrap();
        let mut expected = vec![0u8; 256 * 1024];
        let mut reader = EventBloomReader::open(&path).unwrap();
        for row in &rows {
            let Some(event) = row.topic0 else { continue };
            assert!(reader.may_contain_event(&event, &row.address).unwrap());
            for (position, value) in [Some(B256::ZERO), row.topic1, row.topic2, row.topic3]
                .into_iter()
                .enumerate()
            {
                let Some(value) = value else { continue };
                if position > 0 {
                    assert!(
                        reader
                            .may_contain_topic(&event, &row.address, position, &value)
                            .unwrap()
                    );
                }
                // Independent serialization and wide modulo arithmetic check
                // the source traversal, key domains and wrapping bit layout.
                let mut key = b"LXEVBF1\0".to_vec();
                key.extend_from_slice(event.as_slice());
                key.extend_from_slice(row.address.as_slice());
                key.push(position as u8);
                key.extend_from_slice(value.as_slice());
                let digest = keccak256(&key);
                let first = u128::from(u64::from_le_bytes(digest[..8].try_into().unwrap()));
                let step = u128::from(u64::from_le_bytes(digest[8..16].try_into().unwrap()) | 1);
                for round in 0..4u128 {
                    let bit = ((first + round * step) % (expected.len() as u128 * 8)) as usize;
                    expected[bit / 8] |= 1 << (bit % 8);
                }
            }
            assert!(
                !reader
                    .may_contain_event(&event, &Address::repeat_byte(255))
                    .unwrap()
            );
        }
        assert_eq!(&raw[20..], expected);
        assert!(
            !reader
                .may_contain_topic(&rows[4].topic0.unwrap(), &rows[4].address, 1, &B256::ZERO)
                .unwrap()
        );
        for position in [0, 4, usize::MAX] {
            assert_eq!(
                reader
                    .may_contain_topic(&B256::ZERO, &Address::ZERO, position, &B256::ZERO)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidInput
            );
        }
        let id = IndexFile::protected_file_id(&path).unwrap();
        verify_source_membership(source.path(), &path, id).unwrap();
        assert!(crate::Erc20EventBloomReader::open(&path).is_err());
        assert!(EventBloomReader::open_bound(&path, [0; 16]).is_err());
    }

    #[test]
    fn verifier_rejects_each_missing_membership_even_with_valid_checksums() {
        let source = TempDir::new().unwrap();
        let rows = rows();
        let path = build(source.path(), &rows);
        let mut valid = Vec::new();
        IndexFile::open(&path)
            .unwrap()
            .read_to_end(&mut valid)
            .unwrap();
        let row = &rows[2];
        for (position, value) in [
            B256::ZERO,
            row.topic1.unwrap(),
            row.topic2.unwrap(),
            row.topic3.unwrap(),
        ]
        .into_iter()
        .enumerate()
        {
            let mut corrupt = valid.clone();
            let mask = ((corrupt.len() - 20) * 8 - 1) as u64;
            let bit = key_bits(
                key_hashes(&row.topic0.unwrap(), &row.address, position as u8, &value),
                mask,
            )
            .next()
            .unwrap();
            corrupt[20 + (bit / 8) as usize] &= !(1 << (bit % 8));
            write_index_file(&path, corrupt.len() as u64, |writer| {
                writer.write_all(&corrupt)
            })
            .unwrap();
            let id = IndexFile::protected_file_id(&path).unwrap();
            let error = verify_source_membership(source.path(), &path, id).unwrap_err();
            assert!(
                error.to_string().contains("bloom omits source row"),
                "position {position}: {error}"
            );
        }
    }

    #[test]
    fn reader_budget_is_retained_released_and_enforced() {
        let source = TempDir::new().unwrap();
        let rows = rows();
        let path = build(source.path(), &rows);
        let id = IndexFile::protected_file_id(&path).unwrap();
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
        let mut reader = EventBloomReader::open_bound_with_memory(&path, id, &memory).unwrap();
        for row in &rows {
            if let Some(event) = row.topic0 {
                assert!(reader.may_contain_event(&event, &row.address).unwrap());
            }
        }
        let used = memory.used() as usize;
        assert!(used > 0);
        let pressure = memory
            .reserve(memory.limit() - used, "other query")
            .unwrap();
        let error = EventBloomReader::open_bound_with_memory(&path, id, &memory)
            .err()
            .unwrap();
        assert!(error.get_ref().unwrap().is::<QueryMemoryError>());
        drop(pressure);
        drop(reader);
        assert_eq!(memory.used(), 0);
    }
}
