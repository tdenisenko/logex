use std::fs;
use std::io;
use std::path::Path;

use alloy_primitives::{Address, B256, Bytes};

use crate::column::{ColumnFileHeader, NullBitmap};

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
                    if offset + item_size > body.len() {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "row id out of bounds",
                        ));
                    }
                    result.push(Address::from_slice(&body[offset..offset + item_size]));
                }
                Ok(result)
            }
            None => {
                let count = header.row_count as usize;
                let mut result = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = i * item_size;
                    result.push(Address::from_slice(&body[offset..offset + item_size]));
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
                    result.push(B256::from_slice(&body[offset..offset + item_size]));
                }
                Ok(result)
            }
            None => {
                let count = header.row_count as usize;
                let mut result = Vec::with_capacity(count);
                for i in 0..count {
                    let offset = i * item_size;
                    result.push(B256::from_slice(&body[offset..offset + item_size]));
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
                        result.push(Some(B256::from_slice(&body[offset..offset + item_size])));
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
                        result.push(Some(B256::from_slice(&body[offset..offset + item_size])));
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
                    result.push(body[id as usize]);
                }
                Ok(result)
            }
            None => {
                let count = header.row_count as usize;
                Ok(body[..count].to_vec())
            }
        }
    }

    /// Read the variable-length data column.
    pub fn read_var_bytes(
        dir: &Path,
        name: &str,
        row_ids: Option<&[u32]>,
    ) -> std::io::Result<Vec<Bytes>> {
        let data = fs::read(dir.join(name))?;
        let header = ColumnFileHeader::read_from(&data).ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "corrupt header")
        })?;

        let row_count = header.row_count as usize;
        let offsets_start = ColumnFileHeader::SIZE;
        let offsets_size = (row_count + 1) * 8;
        let blob_start = offsets_start + offsets_size;

        // Parse offset array
        let mut offsets = Vec::with_capacity(row_count + 1);
        for i in 0..=row_count {
            offsets.push(read_le_u64(&data, offsets_start + i * 8)?);
        }

        let blob = &data[blob_start..];

        match row_ids {
            Some(ids) => {
                let mut result = Vec::with_capacity(ids.len());
                for &id in ids {
                    let id = id as usize;
                    let start = offsets[id] as usize;
                    let end = offsets[id + 1] as usize;
                    result.push(Bytes::copy_from_slice(&blob[start..end]));
                }
                Ok(result)
            }
            None => {
                let mut result = Vec::with_capacity(row_count);
                for i in 0..row_count {
                    let start = offsets[i] as usize;
                    let end = offsets[i + 1] as usize;
                    result.push(Bytes::copy_from_slice(&blob[start..end]));
                }
                Ok(result)
            }
        }
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
}
