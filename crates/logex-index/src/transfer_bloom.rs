use crate::builder::validate_source_rows;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use alloy_primitives::{Address, B256, keccak256};
use logex_storage::SegmentReader;
use logex_types::QueryMemoryBudget;

use crate::index_file::{IndexFile, write_index_file};

const TRANSFER_MAGIC: &[u8; 8] = b"LXTRBF1\0";
const ERC20_EVENTS_MAGIC: &[u8; 8] = b"LXE2BF1\0";
const HEADER_LEN: u64 = 8 + 8 + 4;
const MIN_FILTER_BYTES: usize = 256 * 1024;
const MAX_FILTER_BYTES: usize = 2 * 1024 * 1024;
const MAX_FILTER_BITS: u64 = (MAX_FILTER_BYTES as u64) * 8;
const HASH_ROUNDS: u64 = 4;

pub const ERC20_EVENTS_BLOOM_FILE: &str = "erc20_events.bloom";
pub const TRANSFER_BLOOM_FILE: &str = "erc20_transfer.bloom";

pub fn transfer_topic0() -> B256 {
    keccak256(b"Transfer(address,address,uint256)")
}

pub fn approval_topic0() -> B256 {
    keccak256(b"Approval(address,address,uint256)")
}

pub fn is_common_erc20_event_topic0(topic0: &B256) -> bool {
    *topic0 == transfer_topic0() || *topic0 == approval_topic0()
}

/// Bounded, row-sized per-segment presence filter for common ERC20 event topic keys.
/// It is intentionally only a segment skip index: query execution still
/// materializes and rechecks matching rows before returning or aggregating.
#[derive(Debug, Clone)]
pub struct Erc20EventBloom {
    bits: Vec<u8>,
}

pub struct Erc20EventBloomReader {
    reader: IndexFile,
    bit_mask: u64,
}

impl Erc20EventBloom {
    pub fn build(partition_dir: &Path, index_dir: &Path) -> io::Result<()> {
        let reader = SegmentReader::open_projected(
            partition_dir,
            &["address", "topic0", "topic1", "topic2"],
        )?;
        let addresses = reader.read_address(None)?;
        let topic0s = reader.read_nullable_b256("topic0", None)?;
        let topic1s = reader.read_nullable_b256("topic1", None)?;
        let topic2s = reader.read_nullable_b256("topic2", None)?;
        validate_source_rows(
            &reader,
            &[
                ("address", addresses.len()),
                ("topic0", topic0s.len()),
                ("topic1", topic1s.len()),
                ("topic2", topic2s.len()),
            ],
        )?;
        let transfer_topic0 = transfer_topic0();
        let approval_topic0 = approval_topic0();
        let mut bloom = Self::new(addresses.len());

        for (((address, topic0), topic1), topic2) in addresses
            .iter()
            .zip(topic0s.iter())
            .zip(topic1s.iter())
            .zip(topic2s.iter())
        {
            let Some(topic0) = topic0 else {
                continue;
            };
            if *topic0 != transfer_topic0 && *topic0 != approval_topic0 {
                continue;
            }
            if let Some(topic1) = topic1 {
                bloom.insert(topic0, address, 1, topic1);
            }
            if let Some(topic2) = topic2 {
                bloom.insert(topic0, address, 2, topic2);
            }
        }

        bloom.write_to_file(&index_dir.join(ERC20_EVENTS_BLOOM_FILE))
    }

    pub fn may_contain_from_file(
        path: &Path,
        topic0: &B256,
        address: &Address,
        topic_index: usize,
        topic: &B256,
    ) -> io::Result<bool> {
        let mut reader = Erc20EventBloomReader::open(path)?;
        reader.may_contain(topic0, address, topic_index, topic)
    }

    fn new(row_count: usize) -> Self {
        Self {
            bits: vec![0; filter_bytes(row_count)],
        }
    }

    fn insert(&mut self, topic0: &B256, address: &Address, topic_index: u8, topic: &B256) {
        let (h1, h2) = erc20_event_key_hashes(topic0, address, topic_index, topic);
        let bit_mask = self.bits.len() as u64 * 8 - 1;
        for round in 0..HASH_ROUNDS {
            let bit = h1.wrapping_add(round.wrapping_mul(h2)) & bit_mask;
            self.bits[(bit / 8) as usize] |= 1u8 << (bit % 8);
        }
    }

    fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let bit_len = self.bits.len() as u64 * 8;
        write_index_file(path, HEADER_LEN + self.bits.len() as u64, |writer| {
            writer.write_all(ERC20_EVENTS_MAGIC)?;
            writer.write_all(&bit_len.to_le_bytes())?;
            writer.write_all(&(HASH_ROUNDS as u32).to_le_bytes())?;
            writer.write_all(&self.bits)
        })
    }
}

impl Erc20EventBloomReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let (reader, bit_mask) = open_bloom(IndexFile::open(path)?, ERC20_EVENTS_MAGIC)?;
        Ok(Self { reader, bit_mask })
    }

    /// Open a published bloom after matching the ID from a caller-held
    /// publication checkpoint guard.
    pub fn open_bound(path: &Path, expected_file_id: [u8; 16]) -> io::Result<Self> {
        let (reader, bit_mask) = open_bloom(
            IndexFile::open_bound(path, expected_file_id)?,
            ERC20_EVENTS_MAGIC,
        )?;
        Ok(Self { reader, bit_mask })
    }

    /// Account the selected-page and checksum caches of a published bloom.
    /// Keep the caller's publication checkpoint guard for the reader lifetime.
    pub fn open_bound_with_memory(
        path: &Path,
        expected_file_id: [u8; 16],
        memory: &QueryMemoryBudget,
    ) -> io::Result<Self> {
        let (reader, bit_mask) = open_bloom(
            IndexFile::open_bound_with_memory(path, expected_file_id, Some(memory))?,
            ERC20_EVENTS_MAGIC,
        )?;
        Ok(Self { reader, bit_mask })
    }

    pub fn may_contain(
        &mut self,
        topic0: &B256,
        address: &Address,
        topic_index: usize,
        topic: &B256,
    ) -> io::Result<bool> {
        let topic_index = u8::try_from(topic_index).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "topic index is out of range")
        })?;

        let (h1, h2) = erc20_event_key_hashes(topic0, address, topic_index, topic);
        for round in 0..HASH_ROUNDS {
            let bit = h1.wrapping_add(round.wrapping_mul(h2)) & self.bit_mask;
            let byte_offset = HEADER_LEN + bit / 8;
            self.reader.seek(SeekFrom::Start(byte_offset))?;
            let mut byte = [0u8; 1];
            self.reader.read_exact(&mut byte)?;
            if byte[0] & (1u8 << (bit % 8)) == 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

/// Legacy per-segment presence filter for ERC20 Transfer
/// sender/receiver keys. New syncs build `erc20_events.bloom`; this reader is
/// kept for integrity-protected Transfer indexes. Raw legacy bloom files must be
/// rebuilt because they cannot detect changes to presence bits.
#[derive(Debug, Clone)]
pub struct TransferBloom {
    bits: Vec<u8>,
}

pub struct TransferBloomReader {
    reader: IndexFile,
    bit_mask: u64,
}

impl TransferBloom {
    pub fn build(partition_dir: &Path, index_dir: &Path) -> io::Result<()> {
        let reader = SegmentReader::open_projected(
            partition_dir,
            &["address", "topic0", "topic1", "topic2"],
        )?;
        let addresses = reader.read_address(None)?;
        let topic0s = reader.read_nullable_b256("topic0", None)?;
        let topic1s = reader.read_nullable_b256("topic1", None)?;
        let topic2s = reader.read_nullable_b256("topic2", None)?;
        validate_source_rows(
            &reader,
            &[
                ("address", addresses.len()),
                ("topic0", topic0s.len()),
                ("topic1", topic1s.len()),
                ("topic2", topic2s.len()),
            ],
        )?;
        let transfer_topic0 = transfer_topic0();
        let mut bloom = Self::new(addresses.len());

        for (((address, topic0), topic1), topic2) in addresses
            .iter()
            .zip(topic0s.iter())
            .zip(topic1s.iter())
            .zip(topic2s.iter())
        {
            if *topic0 != Some(transfer_topic0) {
                continue;
            }
            if let Some(topic1) = topic1 {
                bloom.insert(address, 1, topic1);
            }
            if let Some(topic2) = topic2 {
                bloom.insert(address, 2, topic2);
            }
        }

        bloom.write_to_file(&index_dir.join(TRANSFER_BLOOM_FILE))
    }

    pub fn may_contain_from_file(
        path: &Path,
        address: &Address,
        topic_index: usize,
        topic: &B256,
    ) -> io::Result<bool> {
        let mut reader = TransferBloomReader::open(path)?;
        reader.may_contain(address, topic_index, topic)
    }

    fn new(row_count: usize) -> Self {
        Self {
            bits: vec![0; filter_bytes(row_count)],
        }
    }

    fn insert(&mut self, address: &Address, topic_index: u8, topic: &B256) {
        let (h1, h2) = transfer_key_hashes(address, topic_index, topic);
        let bit_mask = self.bits.len() as u64 * 8 - 1;
        for round in 0..HASH_ROUNDS {
            let bit = h1.wrapping_add(round.wrapping_mul(h2)) & bit_mask;
            self.bits[(bit / 8) as usize] |= 1u8 << (bit % 8);
        }
    }

    fn write_to_file(&self, path: &Path) -> io::Result<()> {
        let bit_len = self.bits.len() as u64 * 8;
        write_index_file(path, HEADER_LEN + self.bits.len() as u64, |writer| {
            writer.write_all(TRANSFER_MAGIC)?;
            writer.write_all(&bit_len.to_le_bytes())?;
            writer.write_all(&(HASH_ROUNDS as u32).to_le_bytes())?;
            writer.write_all(&self.bits)
        })
    }
}

impl TransferBloomReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let (reader, bit_mask) = open_bloom(IndexFile::open(path)?, TRANSFER_MAGIC)?;
        Ok(Self { reader, bit_mask })
    }

    /// Open a published bloom after matching the ID from a caller-held
    /// publication checkpoint guard.
    pub fn open_bound(path: &Path, expected_file_id: [u8; 16]) -> io::Result<Self> {
        let (reader, bit_mask) = open_bloom(
            IndexFile::open_bound(path, expected_file_id)?,
            TRANSFER_MAGIC,
        )?;
        Ok(Self { reader, bit_mask })
    }

    /// Account the selected-page and checksum caches of a published bloom.
    /// Keep the caller's publication checkpoint guard for the reader lifetime.
    pub fn open_bound_with_memory(
        path: &Path,
        expected_file_id: [u8; 16],
        memory: &QueryMemoryBudget,
    ) -> io::Result<Self> {
        let (reader, bit_mask) = open_bloom(
            IndexFile::open_bound_with_memory(path, expected_file_id, Some(memory))?,
            TRANSFER_MAGIC,
        )?;
        Ok(Self { reader, bit_mask })
    }

    pub fn may_contain(
        &mut self,
        address: &Address,
        topic_index: usize,
        topic: &B256,
    ) -> io::Result<bool> {
        let topic_index = u8::try_from(topic_index).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "topic index is out of range")
        })?;

        let (h1, h2) = transfer_key_hashes(address, topic_index, topic);
        for round in 0..HASH_ROUNDS {
            let bit = h1.wrapping_add(round.wrapping_mul(h2)) & self.bit_mask;
            let byte_offset = HEADER_LEN + bit / 8;
            self.reader.seek(SeekFrom::Start(byte_offset))?;
            let mut byte = [0u8; 1];
            self.reader.read_exact(&mut byte)?;
            if byte[0] & (1u8 << (bit % 8)) == 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

pub(crate) fn encoded_logical_size_for_rows(rows: u64) -> io::Result<u64> {
    let rows = usize::try_from(rows).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "bloom row count exceeds address space",
        )
    })?;
    HEADER_LEN
        .checked_add(filter_bytes(rows) as u64)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "bloom size bound overflow"))
}

// Two indexed topic positions per source row need at most two insertions.
// Keep at least 128 bits per insertion through 65,536 rows, then retain the
// previous 2 MiB ceiling. Clamp before multiplication and power-of-two rounding.
fn filter_bytes(row_count: usize) -> usize {
    (row_count.min(MAX_FILTER_BYTES / 32) * 32)
        .next_power_of_two()
        .max(MIN_FILTER_BYTES)
}

fn open_bloom(mut reader: IndexFile, expected_magic: &[u8; 8]) -> io::Result<(IndexFile, u64)> {
    if !reader.is_protected() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "legacy bloom has no integrity checks; rebuild derived indexes",
        ));
    }
    let mut header = [0; HEADER_LEN as usize];
    reader.read_exact(&mut header)?;
    let bit_len = u64::from_le_bytes(header[8..16].try_into().unwrap());
    if &header[..8] != expected_magic
        || !bit_len.is_power_of_two()
        || !(MIN_FILTER_BYTES as u64 * 8..=MAX_FILTER_BITS).contains(&bit_len)
        || header[16..20] != (HASH_ROUNDS as u32).to_le_bytes()
        || reader.logical_len() != HEADER_LEN + bit_len / 8
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid bloom file geometry",
        ));
    }
    Ok((reader, bit_len - 1))
}

fn transfer_key_hashes(address: &Address, topic_index: u8, topic: &B256) -> (u64, u64) {
    let mut key = [0u8; 1 + 20 + 32];
    key[0] = topic_index;
    key[1..21].copy_from_slice(address.as_slice());
    key[21..].copy_from_slice(topic.as_slice());
    let hash = keccak256(key);
    let bytes = hash.as_slice();
    let h1 = u64::from_le_bytes(bytes[0..8].try_into().expect("hash slice has 8 bytes"));
    let h2 = u64::from_le_bytes(bytes[8..16].try_into().expect("hash slice has 8 bytes")) | 1;
    (h1, h2)
}

fn erc20_event_key_hashes(
    topic0: &B256,
    address: &Address,
    topic_index: u8,
    topic: &B256,
) -> (u64, u64) {
    let mut key = [0u8; 32 + 1 + 20 + 32];
    key[..32].copy_from_slice(topic0.as_slice());
    key[32] = topic_index;
    key[33..53].copy_from_slice(address.as_slice());
    key[53..].copy_from_slice(topic.as_slice());
    let hash = keccak256(key);
    let bytes = hash.as_slice();
    let h1 = u64::from_le_bytes(bytes[0..8].try_into().expect("hash slice has 8 bytes"));
    let h2 = u64::from_le_bytes(bytes[8..16].try_into().expect("hash slice has 8 bytes")) | 1;
    (h1, h2)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::bytes;
    use logex_storage::ColumnFile;
    use logex_types::{LogRow, Source};
    use tempfile::TempDir;

    use super::*;

    fn topic_address(byte: u8) -> B256 {
        let mut topic = [0u8; 32];
        topic[12..].copy_from_slice(Address::repeat_byte(byte).as_slice());
        B256::from(topic)
    }

    #[test]
    fn accounted_bloom_readers_charge_caches_without_loading_the_bitset() {
        use logex_types::{QueryMemoryError, QueryMemoryLimit};
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("events.bloom");
        let address = Address::repeat_byte(7);
        let topic = topic_address(11);
        let topic0 = transfer_topic0();
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(32 * 1024).unwrap());
        let mut bloom = Erc20EventBloom::new(1000);
        bloom.insert(&topic0, &address, 1, &topic);
        bloom.write_to_file(&path).unwrap();
        let id = IndexFile::protected_file_id(&path).unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > memory.limit() as u64);
        let mut reader = Erc20EventBloomReader::open_bound_with_memory(&path, id, &memory).unwrap();
        let retained = memory.used();
        assert!(retained > 0 && retained < memory.limit() as u128);
        assert!(reader.may_contain(&topic0, &address, 1, &topic).unwrap());
        assert_eq!(memory.used(), retained);
        let pressure = memory
            .reserve(memory.limit() - retained as usize, "other query")
            .unwrap();
        let error = Erc20EventBloomReader::open_bound_with_memory(&path, id, &memory)
            .err()
            .unwrap();
        assert!(error.get_ref().unwrap().is::<QueryMemoryError>());
        assert_eq!(memory.used(), retained + pressure.bytes());
        drop(pressure);
        drop(reader);
        assert_eq!(memory.used(), 0);

        let mut bloom = TransferBloom::new(1000);
        bloom.insert(&address, 2, &topic);
        bloom.write_to_file(&path).unwrap();
        let id = IndexFile::protected_file_id(&path).unwrap();
        let mut reader = TransferBloomReader::open_bound_with_memory(&path, id, &memory).unwrap();
        assert!(reader.may_contain(&address, 2, &topic).unwrap());
        let retained = memory.used();
        let pressure = memory
            .reserve(memory.limit() - retained as usize, "other query")
            .unwrap();
        let error = TransferBloomReader::open_bound_with_memory(&path, id, &memory)
            .err()
            .unwrap();
        assert!(error.get_ref().unwrap().is::<QueryMemoryError>());
        drop(pressure);
        drop(reader);
        assert_eq!(memory.used(), 0);
        assert!(TransferBloomReader::open_bound_with_memory(&path, [0; 16], &memory).is_err());
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn adaptive_filter_sizes_are_bounded_at_every_transition() {
        for (rows, bytes) in [
            (0, 256 * 1024),
            (1, 256 * 1024),
            (8192, 256 * 1024),
            (8193, 512 * 1024),
            (16384, 512 * 1024),
            (16385, 1024 * 1024),
            (32768, 1024 * 1024),
            (32769, 2 * 1024 * 1024),
            (65536, 2 * 1024 * 1024),
            (65537, 2 * 1024 * 1024),
            (usize::MAX, 2 * 1024 * 1024),
        ] {
            assert_eq!(filter_bytes(rows), bytes, "rows {rows}");
        }
    }

    fn insert_reference_bits(bits: &mut [u8], key: &[u8]) {
        let hash = keccak256(key);
        let first = u128::from(u64::from_le_bytes(hash[..8].try_into().unwrap()));
        let step = u128::from(u64::from_le_bytes(hash[8..16].try_into().unwrap()) | 1);
        for round in 0..4u128 {
            // Wide arithmetic and modulo independently check the writer's
            // wrapping arithmetic and mask for each permitted power-of-two size.
            let bit = ((first + round * step) % (bits.len() as u128 * 8)) as usize;
            bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    #[test]
    fn adaptive_bloom_bits_match_independent_modulo_oracle() {
        for rows in [8192, 16384, 32768, 65536] {
            let mut common = Erc20EventBloom::new(rows);
            let mut transfer = TransferBloom::new(rows);
            let mut common_expected = vec![0; filter_bytes(rows)];
            let mut transfer_expected = vec![0; filter_bytes(rows)];
            for index in 0..64u64 {
                let address = Address::repeat_byte(index as u8);
                let topic = keccak256(index.to_be_bytes());
                let event = if index % 3 == 0 {
                    transfer_topic0()
                } else {
                    approval_topic0()
                };
                let position = (index % 2 + 1) as u8;
                common.insert(&event, &address, position, &topic);
                let mut common_key = event.as_slice().to_vec();
                common_key.push(position);
                common_key.extend_from_slice(address.as_slice());
                common_key.extend_from_slice(topic.as_slice());
                insert_reference_bits(&mut common_expected, &common_key);
                if event == transfer_topic0() {
                    transfer.insert(&address, position, &topic);
                    let mut transfer_key = vec![position];
                    transfer_key.extend_from_slice(address.as_slice());
                    transfer_key.extend_from_slice(topic.as_slice());
                    insert_reference_bits(&mut transfer_expected, &transfer_key);
                }
            }
            assert_eq!(common.bits, common_expected, "common, rows {rows}");
            assert_eq!(transfer.bits, transfer_expected, "transfer, rows {rows}");
        }
    }

    #[test]
    fn protected_blooms_preserve_all_inserted_keys_across_reopen() {
        let dir = TempDir::new().unwrap();
        let common_path = dir.path().join("events.bloom");
        let transfer_path = dir.path().join("transfer.bloom");
        for row_count in [8192, 16384, 32768, 65536] {
            let mut common = Erc20EventBloom::new(row_count);
            let mut transfer = TransferBloom::new(row_count);
            let mut expected = Vec::new();
            let mut seed = 0x32c7_97db_1205_68a1u64;
            for i in 0..256 {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                let address = Address::from_word(keccak256(seed.to_le_bytes()));
                let topic = keccak256(seed.to_be_bytes());
                let event = if i % 2 == 0 {
                    transfer_topic0()
                } else {
                    approval_topic0()
                };
                let position = if i % 3 == 0 { 1 } else { 2 };
                common.insert(&event, &address, position, &topic);
                if event == transfer_topic0() {
                    transfer.insert(&address, position, &topic);
                }
                expected.push((event, address, position, topic));
            }
            common.write_to_file(&common_path).unwrap();
            transfer.write_to_file(&transfer_path).unwrap();
            for _ in 0..2 {
                let mut common = Erc20EventBloomReader::open(&common_path).unwrap();
                let mut transfer = TransferBloomReader::open(&transfer_path).unwrap();
                for &(event, address, position, topic) in &expected {
                    assert!(
                        common
                            .may_contain(&event, &address, usize::from(position), &topic)
                            .unwrap()
                    );
                    if event == transfer_topic0() {
                        assert!(
                            transfer
                                .may_contain(&address, usize::from(position), &topic)
                                .unwrap()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn bloom_readers_reject_inconsistent_file_geometry() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("presence.bloom");
        let mut accepted = Vec::new();
        for common in [false, true] {
            let magic = if common {
                ERC20_EVENTS_MAGIC
            } else {
                TRANSFER_MAGIC
            };
            let open = |path: &Path| {
                if common {
                    Erc20EventBloomReader::open(path).map(|_| ())
                } else {
                    TransferBloomReader::open(path).map(|_| ())
                }
            };
            for (bits, rounds, payload_bytes) in [
                (8, HASH_ROUNDS, MAX_FILTER_BYTES),
                (0, HASH_ROUNDS, 0),
                (
                    MIN_FILTER_BYTES as u64 * 4,
                    HASH_ROUNDS,
                    MIN_FILTER_BYTES / 2,
                ),
                (
                    MIN_FILTER_BYTES as u64 * 24,
                    HASH_ROUNDS,
                    MIN_FILTER_BYTES * 3,
                ),
                (MAX_FILTER_BITS * 2, HASH_ROUNDS, 0),
                (MAX_FILTER_BITS, HASH_ROUNDS + 1, MAX_FILTER_BYTES),
                (MAX_FILTER_BITS, HASH_ROUNDS, MAX_FILTER_BYTES - 1),
                (MAX_FILTER_BITS, HASH_ROUNDS, MAX_FILTER_BYTES + 1),
            ] {
                let mut bytes = magic.to_vec();
                bytes.extend_from_slice(&bits.to_le_bytes());
                bytes.extend_from_slice(&(rounds as u32).to_le_bytes());
                bytes.resize(HEADER_LEN as usize + payload_bytes, 0);
                for protected in [false, true] {
                    if protected {
                        write_index_file(&path, bytes.len() as u64, |writer| {
                            writer.write_all(&bytes)
                        })
                        .unwrap();
                    } else {
                        std::fs::write(&path, &bytes).unwrap();
                    }
                    if open(&path).is_ok() {
                        accepted.push((common, protected, bits, rounds, payload_bytes));
                    }
                }
            }
        }
        assert!(
            accepted.is_empty(),
            "inconsistent bloom geometry: {accepted:?}"
        );
    }

    #[test]
    fn changed_bloom_payload_is_an_error_instead_of_a_missing_match() {
        // A known present value must not disappear silently if its persisted
        // presence bit changes. Both formats use small, disposable local files.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("presence.bloom");
        let address = Address::repeat_byte(0x31);
        let topic = topic_address(0x72);
        let event = transfer_topic0();
        let mut missing_errors = Vec::new();
        for common in [false, true] {
            let (magic, hash) = if common {
                let mut bloom = Erc20EventBloom::new(65536);
                bloom.insert(&event, &address, 1, &topic);
                bloom.write_to_file(&path).unwrap();
                (
                    ERC20_EVENTS_MAGIC,
                    erc20_event_key_hashes(&event, &address, 1, &topic).0,
                )
            } else {
                let mut bloom = TransferBloom::new(65536);
                bloom.insert(&address, 1, &topic);
                bloom.write_to_file(&path).unwrap();
                (TRANSFER_MAGIC, transfer_key_hashes(&address, 1, &topic).0)
            };
            let lookup = |path: &Path| {
                if common {
                    Erc20EventBloom::may_contain_from_file(path, &event, &address, 1, &topic)
                } else {
                    TransferBloom::may_contain_from_file(path, &address, 1, &topic)
                }
            };
            assert!(lookup(&path).unwrap());

            let mut bytes = std::fs::read(&path).unwrap();
            let starts: Vec<_> = bytes
                .windows(magic.len())
                .enumerate()
                .filter_map(|(offset, window)| (window == magic).then_some(offset))
                .collect();
            assert_eq!(starts.len(), 1, "fixture has one logical bloom header");
            let bit = hash % MAX_FILTER_BITS;
            let byte = starts[0] + HEADER_LEN as usize + (bit / 8) as usize;
            let mask = 1u8 << (bit % 8);
            assert_ne!(bytes[byte] & mask, 0);
            bytes[byte] &= !mask;
            std::fs::write(&path, bytes).unwrap();
            if lookup(&path).is_ok() {
                missing_errors.push(common);
            }
        }
        assert!(
            missing_errors.is_empty(),
            "changed presence data silently reported a missing value: {missing_errors:?}"
        );
    }

    #[test]
    fn transfer_bloom_indexes_transfer_sender_and_receiver_presence() {
        let tmp = TempDir::new().unwrap();
        let partition = tmp.path().join("partition");
        let indexes = partition.join("indexes");
        std::fs::create_dir_all(&indexes).unwrap();
        let token = Address::repeat_byte(0xAA);
        let from = topic_address(0x01);
        let to = topic_address(0x02);
        ColumnFile::write_batch(
            &partition,
            &[
                LogRow {
                    block_number: 1,
                    block_hash: B256::repeat_byte(0x11),
                    timestamp: 100,
                    tx_hash: B256::repeat_byte(0x22),
                    tx_index: 0,
                    log_index: 0,
                    address: token,
                    topic0: Some(transfer_topic0()),
                    topic1: Some(from),
                    topic2: Some(to),
                    topic3: None,
                    data: bytes!(""),
                    data_len: 0,
                    source: Source::Receipt,
                },
                LogRow {
                    block_number: 2,
                    block_hash: B256::repeat_byte(0x33),
                    timestamp: 112,
                    tx_hash: B256::repeat_byte(0x44),
                    tx_index: 0,
                    log_index: 0,
                    address: token,
                    topic0: Some(B256::repeat_byte(0x99)),
                    topic1: Some(topic_address(0x03)),
                    topic2: Some(topic_address(0x04)),
                    topic3: None,
                    data: bytes!(""),
                    data_len: 0,
                    source: Source::Receipt,
                },
            ],
        )
        .unwrap();

        TransferBloom::build(&partition, &indexes).unwrap();
        let path = indexes.join(TRANSFER_BLOOM_FILE);
        assert!(TransferBloom::may_contain_from_file(&path, &token, 1, &from).unwrap());
        assert!(TransferBloom::may_contain_from_file(&path, &token, 2, &to).unwrap());
        assert!(
            !TransferBloom::may_contain_from_file(&path, &token, 1, &topic_address(0x05)).unwrap()
        );
        assert!(
            !TransferBloom::may_contain_from_file(&path, &token, 2, &topic_address(0x04)).unwrap()
        );
    }

    #[test]
    fn erc20_event_bloom_indexes_transfer_and_approval_presence() {
        let tmp = TempDir::new().unwrap();
        let partition = tmp.path().join("partition");
        let indexes = partition.join("indexes");
        std::fs::create_dir_all(&indexes).unwrap();
        let token = Address::repeat_byte(0xAA);
        let from = topic_address(0x01);
        let to = topic_address(0x02);
        let owner = topic_address(0x03);
        let spender = topic_address(0x04);
        ColumnFile::write_batch(
            &partition,
            &[
                LogRow {
                    block_number: 1,
                    block_hash: B256::repeat_byte(0x11),
                    timestamp: 100,
                    tx_hash: B256::repeat_byte(0x22),
                    tx_index: 0,
                    log_index: 0,
                    address: token,
                    topic0: Some(transfer_topic0()),
                    topic1: Some(from),
                    topic2: Some(to),
                    topic3: None,
                    data: bytes!(""),
                    data_len: 0,
                    source: Source::Receipt,
                },
                LogRow {
                    block_number: 2,
                    block_hash: B256::repeat_byte(0x33),
                    timestamp: 112,
                    tx_hash: B256::repeat_byte(0x44),
                    tx_index: 0,
                    log_index: 0,
                    address: token,
                    topic0: Some(approval_topic0()),
                    topic1: Some(owner),
                    topic2: Some(spender),
                    topic3: None,
                    data: bytes!(""),
                    data_len: 0,
                    source: Source::Receipt,
                },
                LogRow {
                    block_number: 3,
                    block_hash: B256::repeat_byte(0x55),
                    timestamp: 124,
                    tx_hash: B256::repeat_byte(0x66),
                    tx_index: 0,
                    log_index: 0,
                    address: token,
                    topic0: Some(B256::repeat_byte(0x99)),
                    topic1: Some(topic_address(0x05)),
                    topic2: Some(topic_address(0x06)),
                    topic3: None,
                    data: bytes!(""),
                    data_len: 0,
                    source: Source::Receipt,
                },
            ],
        )
        .unwrap();

        Erc20EventBloom::build(&partition, &indexes).unwrap();
        let path = indexes.join(ERC20_EVENTS_BLOOM_FILE);
        assert!(
            Erc20EventBloom::may_contain_from_file(&path, &transfer_topic0(), &token, 1, &from)
                .unwrap()
        );
        assert!(
            Erc20EventBloom::may_contain_from_file(&path, &transfer_topic0(), &token, 2, &to)
                .unwrap()
        );
        assert!(
            Erc20EventBloom::may_contain_from_file(&path, &approval_topic0(), &token, 1, &owner)
                .unwrap()
        );
        assert!(
            Erc20EventBloom::may_contain_from_file(&path, &approval_topic0(), &token, 2, &spender)
                .unwrap()
        );
        assert!(
            !Erc20EventBloom::may_contain_from_file(&path, &approval_topic0(), &token, 1, &from)
                .unwrap()
        );
        assert!(
            !Erc20EventBloom::may_contain_from_file(&path, &transfer_topic0(), &token, 2, &spender)
                .unwrap()
        );
    }
}
