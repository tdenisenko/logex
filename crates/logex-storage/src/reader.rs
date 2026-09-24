use std::fs;
use std::io::{self, Read};
use std::path::Path;

use alloy_primitives::{Address, B256, Bytes};
use logex_types::{QueryBuffer, QueryMemoryBudget};

use crate::column::{COLUMN_VERSION, ColumnFileHeader, NullBitmap};

/// Validate an address header and the length of the same captured raw file.
pub(crate) fn raw_address_row_count(header_bytes: &[u8], file_len: u64) -> io::Result<u64> {
    let header = ColumnFileHeader::read_from(header_bytes)
        .filter(|header| {
            header.version == COLUMN_VERSION
                && header.compression == 0
                && header.row_count <= u64::from(u32::MAX)
        })
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "corrupt captured row-count header",
            )
        })?;
    // The u32 row bound above makes the address-column byte count fit in u64.
    let expected = ColumnFileHeader::SIZE as u64 + header.row_count * 20;
    if file_len != expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "raw row count does not match its address column",
        ));
    }
    Ok(header.row_count)
}

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

/// A validated whole fixed-width column, or a bounds-checked selected prefix.
/// The file buffer owns the bytes; page encoders can borrow them directly.
pub(crate) struct RawFixedColumn<const WIDTH: usize> {
    data: QueryBuffer<u8>,
}

impl<const WIDTH: usize> RawFixedColumn<WIDTH> {
    pub(crate) fn open(path: &Path) -> io::Result<Self> {
        Self::open_prefix(path, None)
    }

    fn open_for_read(path: &Path, row_ids: Option<&[u32]>) -> io::Result<Self> {
        let prefix = row_ids
            .and_then(|ids| ids.iter().max())
            .map(|&row| {
                usize::try_from(u64::from(row) + 1).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "selected column prefix exceeds address space",
                    )
                })
            })
            .transpose()?;
        Self::open_prefix(path, prefix)
    }

    fn open_prefix(path: &Path, prefix: Option<usize>) -> io::Result<Self> {
        Self::from_bytes(path, fs::read(path)?, prefix)
    }

    pub(crate) fn from_bytes(
        path: &Path,
        data: Vec<u8>,
        prefix: Option<usize>,
    ) -> io::Result<Self> {
        Self::from_accounted(path, QueryBuffer::unaccounted(data), prefix)
    }

    pub(crate) fn from_accounted(
        path: &Path,
        mut data: QueryBuffer<u8>,
        prefix: Option<usize>,
    ) -> io::Result<Self> {
        const { assert!(WIDTH > 0, "raw column widths must be positive") };
        let invalid = |reason: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid fixed column {}: {reason}", path.display()),
            )
        };
        let header = ColumnFileHeader::read_from(&data).ok_or_else(|| invalid("corrupt header"))?;
        if header.version != COLUMN_VERSION || header.compression != 0 {
            return Err(invalid("unsupported raw column version or compression"));
        }
        let rows = usize::try_from(header.row_count).map_err(|_| invalid("row count overflow"))?;
        let byte_len = |rows: usize| {
            rows.checked_mul(WIDTH)
                .and_then(|len| len.checked_add(ColumnFileHeader::SIZE))
        };
        let declared_len = byte_len(rows).ok_or_else(|| invalid("column length overflow"))?;
        if let Some(prefix) = prefix {
            // Fixed columns append in place. A query selecting an already
            // published prefix can race with a later append's header/tail.
            // Validate every requested slot without requiring that unrelated
            // tail to be complete. Whole reads and compaction stay strict.
            let prefix_len = byte_len(prefix).ok_or_else(|| invalid("selected prefix overflow"))?;
            if prefix > rows || prefix_len > data.len() {
                return Err(invalid("selected rows exceed the column bounds"));
            }
            data.truncate(prefix_len);
        } else if declared_len != data.len() {
            return Err(invalid("row count does not match the complete file body"));
        }
        Ok(Self { data })
    }

    pub(crate) fn values(&self) -> &[[u8; WIDTH]] {
        self.data[ColumnFileHeader::SIZE..].as_chunks::<WIDTH>().0
    }

    pub(crate) fn values_mut(&mut self) -> &mut [[u8; WIDTH]] {
        self.data[ColumnFileHeader::SIZE..]
            .as_chunks_mut::<WIDTH>()
            .0
    }

    pub(crate) fn read_nulls(&self, path: &Path) -> io::Result<NullBitmap> {
        self.read_nulls_for_read(path, None)
    }

    fn read_nulls_for_read(&self, path: &Path, row_ids: Option<&[u32]>) -> io::Result<NullBitmap> {
        self.read_nulls_from_bytes(
            path,
            &fs::read(path)?,
            row_ids.is_some_and(|ids| !ids.is_empty()),
        )
    }

    pub(crate) fn read_nulls_from_bytes(
        &self,
        path: &Path,
        data: &[u8],
        selected_prefix: bool,
    ) -> io::Result<NullBitmap> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("null bitmap {} does not match its column", path.display()),
            )
        };
        validate_null_bytes(
            data,
            data.len() as u64,
            self.values().len() as u64,
            selected_prefix,
        )?;
        NullBitmap::read_from(data).ok_or_else(invalid)
    }

    pub(crate) fn materialize<T>(
        &self,
        row_ids: Option<&[u32]>,
        decode: impl FnMut(usize, &[u8; WIDTH]) -> T,
    ) -> io::Result<Vec<T>> {
        self.materialize_accounted(row_ids, None, decode)
            .map(|buffer| buffer.into_parts().0)
    }

    pub(crate) fn materialize_accounted<T>(
        &self,
        row_ids: Option<&[u32]>,
        memory: Option<&QueryMemoryBudget>,
        mut decode: impl FnMut(usize, &[u8; WIDTH]) -> T,
    ) -> io::Result<QueryBuffer<T>> {
        let values = self.values();
        let mut result = QueryBuffer::try_with_capacity(
            row_ids.map_or(values.len(), <[u32]>::len),
            memory,
            "fixed column output",
        )?;
        match row_ids {
            Some(ids) => {
                for &id in ids {
                    let row = id as usize;
                    let value = values.get(row).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            "fixed column row id out of bounds",
                        )
                    })?;
                    result.try_push(decode(row, value))?;
                }
            }
            None => {
                result.try_extend(
                    values
                        .iter()
                        .enumerate()
                        .map(|(row, value)| decode(row, value)),
                )?;
            }
        }
        Ok(result)
    }
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
        Self::from_bytes(path, fs::read(path)?)
    }

    pub(crate) fn from_bytes(path: &Path, data: Vec<u8>) -> io::Result<Self> {
        let invalid = |reason: &str| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid variable column {}: {reason}", path.display()),
            )
        };
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

    pub(crate) fn materialize(
        &self,
        row_ids: Option<&[u32]>,
        visible_rows: Option<u64>,
    ) -> io::Result<Vec<Bytes>> {
        let visible =
            usize::try_from(visible_rows.unwrap_or(self.row_count() as u64)).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "visible raw row count exceeds address space",
                )
            })?;
        if visible > self.row_count()
            || row_ids.is_some_and(|ids| ids.iter().any(|&id| id as usize >= visible))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raw selection exceeds captured rows",
            ));
        }
        let mut values = Vec::new();
        values
            .try_reserve_exact(row_ids.map_or(visible, <[u32]>::len))
            .map_err(io::Error::other)?;
        match row_ids {
            Some(ids) => {
                for &row in ids {
                    values.push(Bytes::copy_from_slice(self.row(row as usize)?));
                }
            }
            None => {
                for row in 0..visible {
                    values.push(Bytes::copy_from_slice(self.row(row)?));
                }
            }
        }
        Ok(values)
    }

    pub(crate) fn row_count(&self) -> usize {
        self.row_count
    }

    /// Validated little-endian offsets, excluding the final payload sentinel.
    pub(crate) fn encoded_row_offsets(&self) -> &[u8] {
        &self.data[ColumnFileHeader::SIZE..self.blob_start - 8]
    }

    pub(crate) fn payload(&self) -> &[u8] {
        &self.data[self.blob_start..]
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

/// Reads column files from a partition directory.
pub struct ColumnReader;

impl ColumnReader {
    /// Read the address column, preserving the requested row order.
    /// If `row_ids` is None, returns all rows.
    pub fn read_address(dir: &Path, row_ids: Option<&[u32]>) -> io::Result<Vec<Address>> {
        RawFixedColumn::<20>::open_for_read(&dir.join("address.col"), row_ids)?
            .materialize(row_ids, |_, value| Address::from(*value))
    }

    /// Read a non-nullable 32-byte hash column (block_hash, tx_hash).
    pub fn read_b256(dir: &Path, name: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<B256>> {
        RawFixedColumn::<32>::open_for_read(&dir.join(name), row_ids)?
            .materialize(row_ids, |_, value| B256::from(*value))
    }

    /// Read a nullable 32-byte topic column with its matching null bitmap.
    pub fn read_nullable_b256(
        dir: &Path,
        base_name: &str,
        row_ids: Option<&[u32]>,
    ) -> io::Result<Vec<Option<B256>>> {
        let column =
            RawFixedColumn::<32>::open_for_read(&dir.join(format!("{base_name}.col")), row_ids)?;
        let nulls = column.read_nulls_for_read(&dir.join(format!("{base_name}.null")), row_ids)?;
        column.materialize(row_ids, |row, value| {
            nulls.is_present(row as u64).then(|| B256::from(*value))
        })
    }

    /// Read a u64 column (block_number, timestamp).
    pub fn read_u64(dir: &Path, name: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u64>> {
        RawFixedColumn::<8>::open_for_read(&dir.join(name), row_ids)?
            .materialize(row_ids, |_, value| u64::from_le_bytes(*value))
    }

    /// Read a u32 column (tx_index, log_index, data_len).
    pub fn read_u32(dir: &Path, name: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u32>> {
        RawFixedColumn::<4>::open_for_read(&dir.join(name), row_ids)?
            .materialize(row_ids, |_, value| u32::from_le_bytes(*value))
    }

    /// Read the source column (u8).
    pub fn read_u8(dir: &Path, name: &str, row_ids: Option<&[u32]>) -> io::Result<Vec<u8>> {
        RawFixedColumn::<1>::open_for_read(&dir.join(name), row_ids)?
            .materialize(row_ids, |_, value| value[0])
    }

    /// Read the variable-length data column.
    pub fn read_var_bytes(
        dir: &Path,
        name: &str,
        row_ids: Option<&[u32]>,
    ) -> std::io::Result<Vec<Bytes>> {
        let column = RawBytesColumn::open(&dir.join(name))?;
        column.materialize(row_ids, None)
    }

    /// Read the canonical bitmap.
    pub fn read_canonical(dir: &Path) -> std::io::Result<NullBitmap> {
        let data = fs::read(dir.join("canonical.bitmap"))?;
        crate::column::read_canonical_bitmap(&data, None)
    }

    /// Read a raw directory's row count from its validated address-column header.
    pub fn read_row_count(dir: &Path) -> std::io::Result<u64> {
        let mut file = fs::File::open(dir.join("address.col"))?;
        let mut header = [0; ColumnFileHeader::SIZE];
        file.read_exact(&mut header).map_err(|error| {
            if error.kind() == io::ErrorKind::UnexpectedEof {
                io::Error::new(io::ErrorKind::InvalidData, "truncated row-count header")
            } else {
                error
            }
        })?;
        raw_address_row_count(&header, file.metadata()?.len())
    }

    /// Reconstruct full rows using the segment reader's captured publication.
    /// If `row_ids` is `None`, reads all captured rows. A manifestless directory
    /// must have a coherent raw layout; standalone selected-column reads can
    /// inspect an existing prefix during an unfinished append.
    pub fn read_log_rows(
        dir: &Path,
        row_ids: Option<&[u32]>,
    ) -> std::io::Result<Vec<logex_types::LogRow>> {
        crate::SegmentReader::open(dir)?.read_log_rows(row_ids)
    }
}

pub(crate) fn validate_null_bytes(
    data: &[u8],
    complete_len: u64,
    required_rows: u64,
    prefix: bool,
) -> io::Result<u64> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "null bitmap does not match its column",
        )
    };
    let header: [u8; 8] = data
        .get(..8)
        .ok_or_else(invalid)?
        .try_into()
        .map_err(|_| invalid())?;
    let rows = u64::from_le_bytes(header);
    if (prefix && rows < required_rows)
        || (!prefix && rows != required_rows)
        || rows.div_ceil(8).checked_add(8) != Some(complete_len)
        || required_rows
            .div_ceil(8)
            .checked_add(8)
            .is_none_or(|n| n > data.len() as u64)
    {
        return Err(invalid());
    }
    Ok(rows)
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
        for count in [0, 42] {
            ColumnFile::write_batch(&dir, &make_test_rows(count)).unwrap();
            assert_eq!(ColumnReader::read_row_count(&dir).unwrap(), count as u64);
        }
    }

    #[test]
    fn legacy_full_rows_reject_inconsistent_column_counts() {
        let dir = TempDir::new().unwrap();
        let rows = make_test_rows(2);
        ColumnFile::write_batch(dir.path(), &rows).unwrap();
        let path = dir.path().join("block_number.col");
        let mut bytes = fs::read(&path).unwrap();
        bytes[8..16].copy_from_slice(&1u64.to_le_bytes());
        bytes.truncate(ColumnFileHeader::SIZE + 8);
        fs::write(&path, &bytes).unwrap();

        assert_eq!(
            ColumnReader::read_log_rows(dir.path(), None)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn legacy_full_rows_reject_inconsistent_payload_lengths() {
        let dir = TempDir::new().unwrap();
        ColumnFile::write_batch(dir.path(), &make_test_rows(2)).unwrap();
        let path = dir.path().join("data_len.col");
        let mut bytes = fs::read(&path).unwrap();
        let start = ColumnFileHeader::SIZE;
        bytes[start..start + 4].copy_from_slice(&3u32.to_le_bytes());
        bytes[start + 4..start + 8].copy_from_slice(&5u32.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        for ids in [None, Some(&[0][..])] {
            assert_eq!(
                ColumnReader::read_log_rows(dir.path(), ids)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::InvalidData
            );
        }
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }

    #[test]
    fn legacy_row_count_rejects_invalid_header_and_length() {
        for case in 0..6 {
            let dir = TempDir::new().unwrap();
            ColumnFile::write_batch(dir.path(), &make_test_rows(2)).unwrap();
            let path = dir.path().join("address.col");
            let mut bytes = fs::read(&path).unwrap();
            match case {
                0 => bytes[4..8].copy_from_slice(&(COLUMN_VERSION + 1).to_le_bytes()),
                1 => bytes[16] = 1,
                2 => {
                    bytes.pop();
                }
                3 => bytes.push(0),
                4 => bytes[8..16].copy_from_slice(&u64::MAX.to_le_bytes()),
                5 => bytes.truncate(ColumnFileHeader::SIZE - 1),
                _ => unreachable!(),
            }
            fs::write(&path, &bytes).unwrap();
            assert_eq!(
                ColumnReader::read_row_count(dir.path()).unwrap_err().kind(),
                io::ErrorKind::InvalidData,
                "case {case}"
            );
            assert_eq!(fs::read(&path).unwrap(), bytes);
        }
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
    #[test]
    fn fixed_columns_validate_layout_before_materializing_rows() {
        let dir = TempDir::new().unwrap();
        type ReadColumn = fn(&Path, Option<&[u32]>) -> io::Result<()>;
        let readers: [(&str, usize, ReadColumn); 6] = [
            ("address.col", 20, |dir, ids| {
                ColumnReader::read_address(dir, ids).map(drop)
            }),
            ("block_hash.col", 32, |dir, ids| {
                ColumnReader::read_b256(dir, "block_hash.col", ids).map(drop)
            }),
            ("block_number.col", 8, |dir, ids| {
                ColumnReader::read_u64(dir, "block_number.col", ids).map(drop)
            }),
            ("tx_index.col", 4, |dir, ids| {
                ColumnReader::read_u32(dir, "tx_index.col", ids).map(drop)
            }),
            ("source.col", 1, |dir, ids| {
                ColumnReader::read_u8(dir, "source.col", ids).map(drop)
            }),
            ("topic0.col", 32, |dir, ids| {
                ColumnReader::read_nullable_b256(dir, "topic0", ids).map(drop)
            }),
        ];
        let mut failures = Vec::new();
        for (name, width, read) in readers {
            for damage in [
                "count-overflow",
                "version",
                "compression",
                "truncated",
                "trailing",
            ] {
                let mut bytes = Vec::new();
                ColumnFileHeader {
                    version: if damage == "version" {
                        COLUMN_VERSION + 1
                    } else {
                        COLUMN_VERSION
                    },
                    row_count: if damage == "count-overflow" {
                        u64::MAX
                    } else {
                        1
                    },
                    compression: u8::from(damage == "compression"),
                }
                .write_to(&mut bytes)
                .unwrap();
                bytes.extend(vec![
                    0;
                    match damage {
                        "count-overflow" => 0,
                        "truncated" => width - 1,
                        "trailing" => width + 1,
                        _ => width,
                    }
                ]);
                fs::write(dir.path().join(name), bytes).unwrap();
                fs::write(
                    dir.path().join("topic0.null"),
                    [1u64.to_le_bytes().as_slice(), &[0]].concat(),
                )
                .unwrap();
                for ids in [None, Some([0].as_slice()), Some([].as_slice())] {
                    if damage == "trailing" && ids.is_some_and(|ids| !ids.is_empty()) {
                        assert!(read(dir.path(), ids).is_ok());
                        continue;
                    }
                    let result = std::panic::catch_unwind(|| read(dir.path(), ids));
                    if !matches!(result, Ok(Err(ref error)) if error.kind() == io::ErrorKind::InvalidData)
                    {
                        failures.push(format!("{name}: {damage}, selection={ids:?}: {result:?}"));
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }

    #[test]
    fn nullable_columns_reject_mismatched_bitmaps_and_out_of_bounds_nulls() {
        let dir = TempDir::new().unwrap();
        let mut rows = make_test_rows(1);
        rows[0].topic0 = None;
        ColumnFile::write_batch(dir.path(), &rows).unwrap();
        assert_eq!(
            ColumnReader::read_nullable_b256(dir.path(), "topic0", Some(&[1]))
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
        for len in [0u64, 2] {
            fs::write(
                dir.path().join("topic0.null"),
                [len.to_le_bytes().as_slice(), &[0][..usize::from(len > 0)]].concat(),
            )
            .unwrap();
            for ids in [None, Some([0].as_slice()), Some([].as_slice())] {
                if len == 2 && ids.is_some_and(|ids| !ids.is_empty()) {
                    assert_eq!(
                        ColumnReader::read_nullable_b256(dir.path(), "topic0", ids).unwrap(),
                        vec![None]
                    );
                    continue;
                }
                assert_eq!(
                    ColumnReader::read_nullable_b256(dir.path(), "topic0", ids)
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::InvalidData
                );
            }
        }
    }
    #[test]
    fn fixed_columns_preserve_empty_and_repeated_selection_order() {
        let dir = TempDir::new().unwrap();
        let mut rows = make_test_rows(17);
        for (index, row) in rows.iter_mut().enumerate() {
            if index % 2 == 0 {
                row.topic0 = None;
            }
        }
        ColumnFile::write_batch(dir.path(), &rows).unwrap();
        for ids in [&[][..], &[16, 0, 16, 8, 1][..]] {
            assert_eq!(
                ColumnReader::read_log_rows(dir.path(), Some(ids)).unwrap(),
                ids.iter()
                    .map(|&i| rows[i as usize].clone())
                    .collect::<Vec<_>>()
            );
        }
    }
    #[test]
    fn selected_fixed_prefix_survives_an_unfinished_append() {
        let dir = TempDir::new().unwrap();
        let rows = make_test_rows(2);
        let ids = [1, 0, 1];
        let expected: Vec<_> = ids.iter().map(|&i| rows[i as usize].clone()).collect();
        for stage in [
            "header-ahead",
            "partial-tail",
            "body-ahead",
            "bitmap-lags",
            "bitmap-ahead",
        ] {
            ColumnFile::write_batch(dir.path(), &rows).unwrap();
            for (name, width) in [
                ("address.col", 20),
                ("block_hash.col", 32),
                ("block_number.col", 8),
                ("tx_index.col", 4),
                ("source.col", 1),
                ("topic0.col", 32),
            ] {
                let path = dir.path().join(name);
                let mut bytes = fs::read(&path).unwrap();
                let declared = if matches!(stage, "body-ahead" | "bitmap-ahead") {
                    2u64
                } else {
                    3
                };
                bytes[8..16].copy_from_slice(&declared.to_le_bytes());
                let extra = match stage {
                    "partial-tail" => width - 1,
                    "body-ahead" | "bitmap-lags" => width,
                    _ => 0,
                };
                bytes.extend(vec![0; extra]);
                fs::write(path, bytes).unwrap();
            }
            if stage == "bitmap-ahead" {
                let path = dir.path().join("topic0.null");
                let mut bytes = fs::read(&path).unwrap();
                bytes[..8].copy_from_slice(&3u64.to_le_bytes());
                fs::write(path, bytes).unwrap();
            }
            assert_eq!(
                ColumnReader::read_address(dir.path(), Some(&ids)).unwrap(),
                expected.iter().map(|row| row.address).collect::<Vec<_>>(),
                "{stage}"
            );
            assert_eq!(
                ColumnReader::read_b256(dir.path(), "block_hash.col", Some(&ids)).unwrap(),
                expected
                    .iter()
                    .map(|row| row.block_hash)
                    .collect::<Vec<_>>(),
                "{stage}"
            );
            assert_eq!(
                ColumnReader::read_u64(dir.path(), "block_number.col", Some(&ids)).unwrap(),
                expected
                    .iter()
                    .map(|row| row.block_number)
                    .collect::<Vec<_>>(),
                "{stage}"
            );
            assert_eq!(
                ColumnReader::read_u32(dir.path(), "tx_index.col", Some(&ids)).unwrap(),
                expected.iter().map(|row| row.tx_index).collect::<Vec<_>>(),
                "{stage}"
            );
            assert_eq!(
                ColumnReader::read_u8(dir.path(), "source.col", Some(&ids)).unwrap(),
                expected
                    .iter()
                    .map(|row| row.source as u8)
                    .collect::<Vec<_>>(),
                "{stage}"
            );
            assert_eq!(
                ColumnReader::read_nullable_b256(dir.path(), "topic0", Some(&ids)).unwrap(),
                expected.iter().map(|row| row.topic0).collect::<Vec<_>>(),
                "{stage}"
            );
            // Full-row reads capture a coherent directory boundary. These
            // manifestless partial layouts cannot establish that boundary.
            let full = ColumnReader::read_log_rows(dir.path(), Some(&ids));
            if stage == "bitmap-ahead" {
                assert_eq!(full.unwrap(), expected);
            } else {
                assert_eq!(full.unwrap_err().kind(), io::ErrorKind::InvalidData);
            }
            assert!(
                ColumnReader::read_nullable_b256(dir.path(), "topic0", None).is_err(),
                "{stage}"
            );
        }
    }
}

// Validate a borrowed bitmap prefix without allocating another bit vector.
// The captured complete extent still validates the declared raw bitmap shape.
