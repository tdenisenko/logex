use std::fs;
use std::io;
use std::path::Path;

use alloy_primitives::{Address, B256, Bytes};

use crate::column::{COLUMN_VERSION, ColumnFileHeader, NullBitmap};

/// Read a little-endian u64 from a byte slice at the given offset.
fn read_le_u64(data: &[u8], offset: usize) -> io::Result<u64> {
    let end = offset + 8;
    let bytes: [u8; 8] = data
        .get(offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "read out of bounds"))?
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "slice conversion failed"))?;
    Ok(u64::from_le_bytes(bytes))
}

/// Read a little-endian u32 from a byte slice at the given offset.
fn read_le_u32(data: &[u8], offset: usize) -> io::Result<u32> {
    let end = offset + 4;
    let bytes: [u8; 4] = data
        .get(offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "read out of bounds"))?
        .try_into()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "slice conversion failed"))?;
    Ok(u32::from_le_bytes(bytes))
}

fn checked_slice(data: &[u8], offset: usize, len: usize) -> io::Result<&[u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "column offset overflow"))?;
    data.get(offset..end)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "column read out of bounds"))
}

fn checked_range(data: &[u8], start: usize, end: usize) -> io::Result<&[u8]> {
    if end < start {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "column offsets are not monotonic",
        ));
    }
    checked_slice(data, start, end - start)
}

/// Validated raw offset table with payloads borrowed from one owned file buffer.
/// Compaction borrows a page at a time; public query results still own each value.
pub(crate) struct RawBytesColumn {
    data: Vec<u8>,
    row_count: usize,
    blob_start: usize,
}

impl RawBytesColumn {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        let invalid = |reason: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid variable column {}: {reason}", path.display()),
            )
        };
        let data = fs::read(path)?;
        let header = ColumnFileHeader::read_from(&data).ok_or_else(|| invalid("corrupt header"))?;
        if header.version != COLUMN_VERSION || header.compression != 0 {
            return Err(invalid("unsupported raw column version or compression"));
        }
        let row_count =
            usize::try_from(header.row_count).map_err(|_| invalid("row count overflow"))?;
        let blob_start = row_count
            .checked_add(1)
            .and_then(|count| count.checked_mul(8))
            .and_then(|size| size.checked_add(ColumnFileHeader::SIZE))
            .filter(|&size| size <= data.len())
            .ok_or_else(|| invalid("overflowing or truncated offset table"))?;
        // Validate before allocating row results, including unselected offsets.
        // The file buffer bounds both the count and every later offset conversion.
        let blob_len = (data.len() - blob_start) as u64;
        let mut previous = 0;
        for (index, bytes) in data[ColumnFileHeader::SIZE..blob_start]
            .as_chunks::<8>()
            .0
            .iter()
            .enumerate()
        {
            let offset = u64::from_le_bytes(*bytes);
            if (index == 0 && offset != 0) || offset < previous || offset > blob_len {
                return Err(invalid(
                    "offsets must start at zero and remain within the payload",
                ));
            }
            previous = offset;
        }
        if previous != blob_len {
            return Err(invalid("final offset does not match the payload length"));
        }
        Ok(Self {
            data,
            row_count,
            blob_start,
        })
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    pub(crate) fn row(&self, row: usize) -> io::Result<&[u8]> {
        if row >= self.row_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "variable column row id out of bounds",
            ));
        }
        // open() proved the complete table fits in this immutable buffer.
        let position = ColumnFileHeader::SIZE + row * 8;
        let start = read_le_u64(&self.data, position)? as usize;
        let end = read_le_u64(&self.data, position + 8)? as usize;
        checked_range(&self.data[self.blob_start..], start, end)
    }
}

/// Typed column data returned from reads.
#[derive(Debug, Clone)]
pub enum ColumnData {
    Address(Vec<Address>),
    B256(Vec<Option<B256>>),
    U64(Vec<u64>),
    U32(Vec<u32>),
    U8(Vec<u8>),
    Bytes(Vec<Bytes>),
}

impl ColumnData {
    pub fn len(&self) -> usize {
        match self {
            Self::Address(v) => v.len(),
            Self::B256(v) => v.len(),
            Self::U64(v) => v.len(),
            Self::U32(v) => v.len(),
            Self::U8(v) => v.len(),
            Self::Bytes(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Reads column files from a partition directory.
pub struct ColumnReader;

impl ColumnReader {
    /// Read the address column, returning values at the given row indices.
    /// If `row_ids` is None, returns all rows.
    pub fn read_address(dir: &Path, row_ids: Option<&[u32]>) -> std::io::Result<Vec<Address>> {
        let data = fs::read(dir.join("address.col"))?;
        let header = ColumnFileHeader::read_from(&data).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt header")
        })?;

        let body = &data[ColumnFileHeader::SIZE..];
        let item_size = 20; // Address is 20 bytes

        match row_ids {
            Some(ids) => {
                let mut result = Vec::with_capacity(ids.len());
                for &id in ids {
                    let offset = id as usize * item_size;
                    result.push(Address::from_slice(checked_slice(body, offset, item_size)?));
                }
                Ok(result)
            }
            None => {
                let count = header.row_count as usize;
                let mut result = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = i * item_size;
                    result.push(Address::from_slice(checked_slice(body, offset, item_size)?));
                }
                Ok(result)
            }
        }
    }

    /// Read a 32-byte hash column (block_hash, tx_hash) — non-nullable.
    pub fn read_b256(
        dir: &Path,
        name: &str,
        row_ids: Option<&[u32]>,
    ) -> std::io::Result<Vec<B256>> {
        let data = fs::read(dir.join(name))?;
        let header = ColumnFileHeader::read_from(&data).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt header")
        })?;

        let body = &data[ColumnFileHeader::SIZE..];
        let item_size = 32;

        match row_ids {
            Some(ids) => {
                let mut result = Vec::with_capacity(ids.len());
                for &id in ids {
                    let offset = id as usize * item_size;
                    result.push(B256::from_slice(checked_slice(body, offset, item_size)?));
                }
                Ok(result)
            }
            None => {
                let count = header.row_count as usize;
                let mut result = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = i * item_size;
                    result.push(B256::from_slice(checked_slice(body, offset, item_size)?));
                }
                Ok(result)
            }
        }
    }

    /// Read a nullable 32-byte topic column with its null bitmap.
    pub fn read_nullable_b256(
        dir: &Path,
        base_name: &str,
        row_ids: Option<&[u32]>,
    ) -> std::io::Result<Vec<Option<B256>>> {
        let col_data = fs::read(dir.join(format!("{base_name}.col")))?;
        let null_data = fs::read(dir.join(format!("{base_name}.null")))?;

        let header = ColumnFileHeader::read_from(&col_data).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt header")
        })?;
        let nulls = NullBitmap::read_from(&null_data).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt null bitmap")
        })?;

        let body = &col_data[ColumnFileHeader::SIZE..];
        let item_size = 32;

        match row_ids {
            Some(ids) => {
                let mut result = Vec::with_capacity(ids.len());
                for &id in ids {
                    let offset = id as usize * item_size;
                    if nulls.is_present(id as u64) {
                        result.push(Some(B256::from_slice(checked_slice(
                            body, offset, item_size,
                        )?)));
                    } else {
                        result.push(None);
                    }
                }
                Ok(result)
            }
            None => {
                let count = header.row_count as usize;
                let mut result = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = i * item_size;
                    if nulls.is_present(i as u64) {
                        result.push(Some(B256::from_slice(checked_slice(
                            body, offset, item_size,
                        )?)));
                    } else {
                        result.push(None);
                    }
                }
                Ok(result)
            }
        }
    }

    /// Read a u64 column (block_number, timestamp).
    pub fn read_u64(dir: &Path, name: &str, row_ids: Option<&[u32]>) -> std::io::Result<Vec<u64>> {
        let data = fs::read(dir.join(name))?;
        let header = ColumnFileHeader::read_from(&data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt header"))?;

        let body = &data[ColumnFileHeader::SIZE..];

        match row_ids {
            Some(ids) => ids
                .iter()
                .map(|&id| read_le_u64(body, id as usize * 8))
                .collect(),
            None => (0..header.row_count as usize)
                .map(|i| read_le_u64(body, i * 8))
                .collect(),
        }
    }

    /// Read a u32 column (tx_index, log_index, data_len).
    pub fn read_u32(dir: &Path, name: &str, row_ids: Option<&[u32]>) -> std::io::Result<Vec<u32>> {
        let data = fs::read(dir.join(name))?;
        let header = ColumnFileHeader::read_from(&data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt header"))?;

        let body = &data[ColumnFileHeader::SIZE..];

        match row_ids {
            Some(ids) => ids
                .iter()
                .map(|&id| read_le_u32(body, id as usize * 4))
                .collect(),
            None => (0..header.row_count as usize)
                .map(|i| read_le_u32(body, i * 4))
                .collect(),
        }
    }

    /// Read the source column (u8).
    pub fn read_u8(dir: &Path, name: &str, row_ids: Option<&[u32]>) -> std::io::Result<Vec<u8>> {
        let data = fs::read(dir.join(name))?;
        let header = ColumnFileHeader::read_from(&data).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt header")
        })?;

        let body = &data[ColumnFileHeader::SIZE..];

        match row_ids {
            Some(ids) => {
                let mut result = Vec::with_capacity(ids.len());
                for &id in ids {
                    result.push(*body.get(id as usize).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "row id out of bounds")
                    })?);
                }
                Ok(result)
            }
            None => {
                let count = header.row_count as usize;
                Ok(checked_slice(body, 0, count)?.to_vec())
            }
        }
    }

    /// Read the variable-length data column.
    pub fn read_var_bytes(
        dir: &Path,
        name: &str,
        row_ids: Option<&[u32]>,
    ) -> std::io::Result<Vec<Bytes>> {
        let column = RawBytesColumn::open(&dir.join(name))?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(row_ids.map_or(column.row_count(), <[u32]>::len))
            .map_err(io::Error::other)?;
        match row_ids {
            Some(ids) => {
                for &row in ids {
                    values.push(Bytes::copy_from_slice(column.row(row as usize)?));
                }
            }
            None => {
                for row in 0..column.row_count() {
                    values.push(Bytes::copy_from_slice(column.row(row)?));
                }
            }
        }
        Ok(values)
    }

    /// Read the canonical bitmap.
    pub fn read_canonical(dir: &Path) -> std::io::Result<NullBitmap> {
        let data = fs::read(dir.join("canonical.bitmap"))?;
        NullBitmap::read_from(&data).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt canonical bitmap")
        })
    }

    /// Read the row count from any column file header.
    pub fn read_row_count(dir: &Path) -> std::io::Result<u64> {
        // Use address.col as the reference (always present)
        let data = fs::read(dir.join("address.col"))?;
        let header = ColumnFileHeader::read_from(&data).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt header")
        })?;
        Ok(header.row_count)
    }

    /// Reconstruct full LogRows from a partition directory.
    /// Only loads columns needed based on the projection.
    /// If `row_ids` is None, reads all rows.
    pub fn read_log_rows(
        dir: &Path,
        row_ids: Option<&[u32]>,
    ) -> std::io::Result<Vec<logex_types::LogRow>> {
        let addresses = Self::read_address(dir, row_ids)?;
        let block_numbers = Self::read_u64(dir, "block_number.col", row_ids)?;
        let block_hashes = Self::read_b256(dir, "block_hash.col", row_ids)?;
        let timestamps = Self::read_u64(dir, "timestamp.col", row_ids)?;
        let tx_hashes = Self::read_b256(dir, "tx_hash.col", row_ids)?;
        let tx_indices = Self::read_u32(dir, "tx_index.col", row_ids)?;
        let log_indices = Self::read_u32(dir, "log_index.col", row_ids)?;
        let topic0s = Self::read_nullable_b256(dir, "topic0", row_ids)?;
        let topic1s = Self::read_nullable_b256(dir, "topic1", row_ids)?;
        let topic2s = Self::read_nullable_b256(dir, "topic2", row_ids)?;
        let topic3s = Self::read_nullable_b256(dir, "topic3", row_ids)?;
        let datas = Self::read_var_bytes(dir, "data.col", row_ids)?;
        let data_lens = Self::read_u32(dir, "data_len.col", row_ids)?;
        let sources = Self::read_u8(dir, "source.col", row_ids)?;

        let count = addresses.len();
        let mut rows = Vec::with_capacity(count);

        for i in 0..count {
            rows.push(logex_types::LogRow {
                block_number: block_numbers[i],
                block_hash: block_hashes[i],
                timestamp: timestamps[i],
                tx_hash: tx_hashes[i],
                tx_index: tx_indices[i],
                log_index: log_indices[i],
                address: addresses[i],
                topic0: topic0s[i],
                topic1: topic1s[i],
                topic2: topic2s[i],
                topic3: topic3s[i],
                data: datas[i].clone(),
                data_len: data_lens[i],
                source: logex_types::Source::from_u8(sources[i]).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid source byte: {}", sources[i]),
                    )
                })?,
            });
        }

        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::column::ColumnFile;
    use alloy_primitives::{Address, B256, bytes};
    use logex_types::{LogRow, Source};
    use tempfile::TempDir;

    fn make_test_rows(count: usize) -> Vec<LogRow> {
        (0..count)
            .map(|i| LogRow {
                block_number: 1000 + i as u64,
                block_hash: B256::repeat_byte((i % 256) as u8),
                timestamp: 1_700_000_000 + i as u64 * 12,
                tx_hash: B256::repeat_byte(((i + 50) % 256) as u8),
                tx_index: (i % 100) as u32,
                log_index: i as u32,
                address: Address::repeat_byte((i % 256) as u8),
                topic0: Some(B256::repeat_byte(0x10)),
                topic1: if i % 2 == 0 {
                    Some(B256::repeat_byte(0x20))
                } else {
                    None
                },
                topic2: None,
                topic3: None,
                data: bytes!("deadbeef"),
                data_len: 4,
                source: Source::Receipt,
            })
            .collect()
    }

    #[test]
    fn test_read_all_rows_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows(50);

        ColumnFile::write_batch(&dir, &rows).unwrap();
        let read_back = ColumnReader::read_log_rows(&dir, None).unwrap();

        assert_eq!(read_back.len(), 50);
        assert_eq!(read_back[0], rows[0]);
        assert_eq!(read_back[49], rows[49]);
    }

    #[test]
    fn test_read_specific_rows() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows(100);

        ColumnFile::write_batch(&dir, &rows).unwrap();

        let ids = vec![0, 10, 50, 99];
        let read_back = ColumnReader::read_log_rows(&dir, Some(&ids)).unwrap();

        assert_eq!(read_back.len(), 4);
        assert_eq!(read_back[0], rows[0]);
        assert_eq!(read_back[1], rows[10]);
        assert_eq!(read_back[2], rows[50]);
        assert_eq!(read_back[3], rows[99]);
    }

    #[test]
    fn test_read_individual_columns() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows(20);

        ColumnFile::write_batch(&dir, &rows).unwrap();

        // Read just addresses
        let addresses = ColumnReader::read_address(&dir, None).unwrap();
        assert_eq!(addresses.len(), 20);
        assert_eq!(addresses[0], rows[0].address);

        // Read just block numbers
        let blocks = ColumnReader::read_u64(&dir, "block_number.col", None).unwrap();
        assert_eq!(blocks.len(), 20);
        assert_eq!(blocks[0], 1000);
        assert_eq!(blocks[19], 1019);

        // Read nullable topic1 — alternating present/null
        let topic1s = ColumnReader::read_nullable_b256(&dir, "topic1", None).unwrap();
        assert_eq!(topic1s.len(), 20);
        assert!(topic1s[0].is_some()); // even index = present
        assert!(topic1s[1].is_none()); // odd index = null
    }

    #[test]
    fn test_read_variable_length_data() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows(10);

        ColumnFile::write_batch(&dir, &rows).unwrap();

        let datas = ColumnReader::read_var_bytes(&dir, "data.col", None).unwrap();
        assert_eq!(datas.len(), 10);
        assert_eq!(datas[0], bytes!("deadbeef"));

        // Read specific rows
        let datas = ColumnReader::read_var_bytes(&dir, "data.col", Some(&[0, 5, 9])).unwrap();
        assert_eq!(datas.len(), 3);
        assert_eq!(datas[0], bytes!("deadbeef"));
    }

    #[test]
    fn test_read_canonical_bitmap() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows(10);

        ColumnFile::write_batch(&dir, &rows).unwrap();

        let canonical = ColumnReader::read_canonical(&dir).unwrap();
        assert_eq!(canonical.len(), 10);
        for i in 0..10 {
            assert!(canonical.is_present(i));
        }
    }

    #[test]
    fn test_read_after_append() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");

        let rows1 = make_test_rows(10);
        ColumnFile::write_batch(&dir, &rows1).unwrap();

        let rows2 = make_test_rows(5);
        ColumnFile::append_batch(&dir, &rows2, 10).unwrap();

        let all = ColumnReader::read_log_rows(&dir, None).unwrap();
        assert_eq!(all.len(), 15);
        assert_eq!(all[0], rows1[0]);
        assert_eq!(all[10], rows2[0]);
    }

    #[test]
    fn test_read_row_count() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows(42);

        ColumnFile::write_batch(&dir, &rows).unwrap();

        let count = ColumnReader::read_row_count(&dir).unwrap();
        assert_eq!(count, 42);
    }

    #[test]
    fn truncated_fixed_column_returns_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows(4);

        ColumnFile::write_batch(&dir, &rows).unwrap();
        let path = dir.join("block_hash.col");
        let mut data = fs::read(&path).unwrap();
        data.truncate(data.len() - 20);
        fs::write(&path, data).unwrap();

        let error = ColumnReader::read_b256(&dir, "block_hash.col", None).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn truncated_variable_column_returns_error() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("partition");
        let rows = make_test_rows(4);

        ColumnFile::write_batch(&dir, &rows).unwrap();
        let path = dir.join("data.col");
        let mut data = fs::read(&path).unwrap();
        data.truncate(data.len() - 3);
        fs::write(&path, data).unwrap();

        let error = ColumnReader::read_var_bytes(&dir, "data.col", None).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
    #[test]
    fn variable_column_rejects_malformed_layout_before_allocation() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("data.col");
        let mut valid = Vec::new();
        ColumnFileHeader {
            version: 1,
            row_count: 2,
            compression: 0,
        }
        .write_to(&mut valid)
        .unwrap();
        for offset in [0u64, 1, 3] {
            valid.extend_from_slice(&offset.to_le_bytes());
        }
        valid.extend_from_slice(b"abc");
        for damage in 0..9 {
            let mut bytes = valid.clone();
            match damage {
                0 => bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes()),
                1 => bytes[4..8].copy_from_slice(&2u32.to_le_bytes()),
                2 => bytes[16] = 1,
                3 => bytes[17..25].copy_from_slice(&1u64.to_le_bytes()),
                4 => bytes[25..33].copy_from_slice(&4u64.to_le_bytes()),
                5 => bytes.truncate(ColumnFileHeader::SIZE + 8),
                6 => bytes[33..41].copy_from_slice(&2u64.to_le_bytes()),
                7 => bytes[25..33].copy_from_slice(&u64::MAX.to_le_bytes()),
                8 => {
                    bytes[8..16].copy_from_slice(&3u64.to_le_bytes());
                    bytes.splice(33..33, 0u64.to_le_bytes());
                }
                _ => unreachable!(),
            }
            fs::write(&path, bytes).unwrap();
            for selection in [None, Some(&[0][..]), Some(&[][..])] {
                let result = std::panic::catch_unwind(|| {
                    ColumnReader::read_var_bytes(tmp.path(), "data.col", selection)
                });
                assert!(
                    result.is_ok(),
                    "damage {damage} panicked before rejecting its layout"
                );
                assert_eq!(
                    result.unwrap().unwrap_err().kind(),
                    io::ErrorKind::InvalidData,
                    "damage {damage}"
                );
            }
        }
    }

    #[test]
    fn variable_column_preserves_empty_payloads_and_selected_order() {
        let tmp = TempDir::new().unwrap();
        let mut bytes = Vec::new();
        ColumnFileHeader {
            version: 1,
            row_count: 3,
            compression: 0,
        }
        .write_to(&mut bytes)
        .unwrap();
        for offset in [0u64, 1, 1, 3] {
            bytes.extend_from_slice(&offset.to_le_bytes());
        }
        bytes.extend_from_slice(b"abc");
        fs::write(tmp.path().join("data.col"), bytes).unwrap();
        let expected = vec![
            Bytes::from_static(b"a"),
            Bytes::new(),
            Bytes::from_static(b"bc"),
        ];
        assert_eq!(
            ColumnReader::read_var_bytes(tmp.path(), "data.col", None).unwrap(),
            expected
        );
        assert_eq!(
            ColumnReader::read_var_bytes(tmp.path(), "data.col", Some(&[2, 0, 2, 1])).unwrap(),
            vec![
                expected[2].clone(),
                expected[0].clone(),
                expected[2].clone(),
                expected[1].clone()
            ]
        );
    }
    #[test]
    fn all_null_column_extensions_preserve_new_and_replaced_file_bytes() {
        for publication in [
            crate::durability::Publication::Ordered,
            crate::durability::Publication::Deferred,
        ] {
            for replace in [false, true] {
                for count in [0, 1, 9, 513] {
                    let tmp = TempDir::new().unwrap();
                    if replace {
                        ColumnFile::write_batch(tmp.path(), &make_test_rows(1024)).unwrap();
                    }
                    let mut rows = make_test_rows(count);
                    for row in &mut rows {
                        row.topic0 = None;
                        row.topic1 = None;
                        row.topic2 = None;
                        row.topic3 = None;
                    }
                    ColumnFile::write_batch_with_publication(tmp.path(), &rows, None, publication)
                        .unwrap();
                    for name in ["topic0", "topic1", "topic2", "topic3"] {
                        let data = fs::read(tmp.path().join(format!("{name}.col"))).unwrap();
                        assert_eq!(
                            ColumnFileHeader::read_from(&data).unwrap().row_count,
                            count as u64
                        );
                        assert_eq!(&data[ColumnFileHeader::SIZE..], vec![0; count * 32]);
                        assert_eq!(
                            ColumnReader::read_nullable_b256(tmp.path(), name, None).unwrap(),
                            vec![None; count]
                        );
                    }
                    assert_eq!(ColumnReader::read_log_rows(tmp.path(), None).unwrap(), rows);
                    let restored = make_test_rows(3);
                    ColumnFile::write_batch_with_publication(
                        tmp.path(),
                        &restored,
                        None,
                        publication,
                    )
                    .unwrap();
                    assert_eq!(
                        ColumnReader::read_log_rows(tmp.path(), None).unwrap(),
                        restored
                    );
                }
            }
        }
    }
}
