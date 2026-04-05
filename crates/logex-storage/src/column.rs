use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::Path;

use logex_types::LogRow;

/// Magic bytes identifying a LogEx column file.
const COLUMN_MAGIC: &[u8; 4] = b"LXCL";

/// Current column file format version.
const COLUMN_VERSION: u32 = 1;

/// Header written at the start of every `.col` file.
#[derive(Debug, Clone, Copy)]
pub struct ColumnFileHeader {
    pub version: u32,
    pub row_count: u64,
    /// 0 = no compression (used in write path; compression added later).
    pub compression: u8,
}

impl ColumnFileHeader {
    pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(COLUMN_MAGIC)?;
        w.write_all(&self.version.to_le_bytes())?;
        w.write_all(&self.row_count.to_le_bytes())?;
        w.write_all(&[self.compression])?;
        Ok(())
    }

    pub fn read_from(data: &[u8]) -> Option<Self> {
        if data.len() < Self::SIZE || &data[0..4] != COLUMN_MAGIC {
            return None;
        }
        let version = u32::from_le_bytes(data[4..8].try_into().ok()?);
        let row_count = u64::from_le_bytes(data[8..16].try_into().ok()?);
        let compression = data[16];
        Some(Self {
            version,
            row_count,
            compression,
        })
    }

    /// Total byte size of the header on disk.
    pub const SIZE: usize = 4 + 4 + 8 + 1; // magic + version + row_count + compression
}

/// A bitmap tracking which rows have null values (used for optional topic columns).
#[derive(Debug, Clone, Default)]
pub struct NullBitmap {
    /// One bit per row. `true` = value present, `false` = null.
    bits: Vec<u8>,
    len: u64,
}

impl NullBitmap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn push(&mut self, present: bool) {
        let byte_idx = (self.len / 8) as usize;
        let bit_idx = (self.len % 8) as u32;
        if byte_idx >= self.bits.len() {
            self.bits.push(0);
        }
        if present {
            self.bits[byte_idx] |= 1 << bit_idx;
        }
        self.len += 1;
    }

    pub fn is_present(&self, row: u64) -> bool {
        let byte_idx = (row / 8) as usize;
        let bit_idx = (row % 8) as u32;
        if byte_idx >= self.bits.len() {
            return false;
        }
        (self.bits[byte_idx] >> bit_idx) & 1 == 1
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&self.len.to_le_bytes())?;
        w.write_all(&self.bits)?;
        Ok(())
    }

    pub fn read_from(data: &[u8]) -> Option<Self> {
        if data.len() < 8 {
            return None;
        }
        let len = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let byte_count = len.div_ceil(8) as usize;
        if data.len() < 8 + byte_count {
            return None;
        }
        let bits = data[8..8 + byte_count].to_vec();
        Some(Self { bits, len })
    }
}

/// Handles writing column files for a partition directory.
pub struct ColumnFile;

impl ColumnFile {
    /// Write all fixed-size and variable-length column files for a batch of rows.
    pub fn write_batch(dir: &Path, rows: &[LogRow]) -> io::Result<()> {
        fs::create_dir_all(dir)?;
        let row_count = rows.len() as u64;

        // Write all columns in parallel (sequentially here, could be parallelized later)
        Self::write_fixed_col(dir, "address.col", row_count, rows, |r| {
            r.address.as_slice().to_vec()
        })?;
        Self::write_fixed_col(dir, "block_number.col", row_count, rows, |r| {
            r.block_number.to_le_bytes().to_vec()
        })?;
        Self::write_fixed_col(dir, "block_hash.col", row_count, rows, |r| {
            r.block_hash.as_slice().to_vec()
        })?;
        Self::write_fixed_col(dir, "tx_hash.col", row_count, rows, |r| {
            r.tx_hash.as_slice().to_vec()
        })?;
        Self::write_fixed_col(dir, "tx_index.col", row_count, rows, |r| {
            r.tx_index.to_le_bytes().to_vec()
        })?;
        Self::write_fixed_col(dir, "log_index.col", row_count, rows, |r| {
            r.log_index.to_le_bytes().to_vec()
        })?;
        Self::write_fixed_col(dir, "timestamp.col", row_count, rows, |r| {
            r.timestamp.to_le_bytes().to_vec()
        })?;
        Self::write_fixed_col(dir, "data_len.col", row_count, rows, |r| {
            r.data_len.to_le_bytes().to_vec()
        })?;
        Self::write_fixed_col(dir, "source.col", row_count, rows, |r| vec![r.source as u8])?;

        // Nullable topic columns: write column file + null bitmap
        Self::write_nullable_col(dir, "topic0", row_count, rows, |r| {
            r.topic0.map(|t| t.as_slice().to_vec())
        })?;
        Self::write_nullable_col(dir, "topic1", row_count, rows, |r| {
            r.topic1.map(|t| t.as_slice().to_vec())
        })?;
        Self::write_nullable_col(dir, "topic2", row_count, rows, |r| {
            r.topic2.map(|t| t.as_slice().to_vec())
        })?;
        Self::write_nullable_col(dir, "topic3", row_count, rows, |r| {
            r.topic3.map(|t| t.as_slice().to_vec())
        })?;

        // Variable-length data column: offset array + data blob
        Self::write_var_col(dir, "data.col", row_count, rows)?;

        // Canonical bitmap: all rows start as canonical
        Self::write_canonical_bitmap(dir, row_count)?;

        Ok(())
    }

    /// Append rows to existing column files (for the hot partition).
    pub fn append_batch(dir: &Path, rows: &[LogRow], existing_rows: u64) -> io::Result<()> {
        if !dir.exists() {
            return Self::write_batch(dir, rows);
        }

        let new_row_count = existing_rows + rows.len() as u64;

        Self::append_fixed_col(dir, "address.col", new_row_count, rows, |r| {
            r.address.as_slice().to_vec()
        })?;
        Self::append_fixed_col(dir, "block_number.col", new_row_count, rows, |r| {
            r.block_number.to_le_bytes().to_vec()
        })?;
        Self::append_fixed_col(dir, "block_hash.col", new_row_count, rows, |r| {
            r.block_hash.as_slice().to_vec()
        })?;
        Self::append_fixed_col(dir, "tx_hash.col", new_row_count, rows, |r| {
            r.tx_hash.as_slice().to_vec()
        })?;
        Self::append_fixed_col(dir, "tx_index.col", new_row_count, rows, |r| {
            r.tx_index.to_le_bytes().to_vec()
        })?;
        Self::append_fixed_col(dir, "log_index.col", new_row_count, rows, |r| {
            r.log_index.to_le_bytes().to_vec()
        })?;
        Self::append_fixed_col(dir, "timestamp.col", new_row_count, rows, |r| {
            r.timestamp.to_le_bytes().to_vec()
        })?;
        Self::append_fixed_col(dir, "data_len.col", new_row_count, rows, |r| {
            r.data_len.to_le_bytes().to_vec()
        })?;
        Self::append_fixed_col(dir, "source.col", new_row_count, rows, |r| {
            vec![r.source as u8]
        })?;

        Self::append_nullable_col(dir, "topic0", new_row_count, rows, |r| {
            r.topic0.map(|t| t.as_slice().to_vec())
        })?;
        Self::append_nullable_col(dir, "topic1", new_row_count, rows, |r| {
            r.topic1.map(|t| t.as_slice().to_vec())
        })?;
        Self::append_nullable_col(dir, "topic2", new_row_count, rows, |r| {
            r.topic2.map(|t| t.as_slice().to_vec())
        })?;
        Self::append_nullable_col(dir, "topic3", new_row_count, rows, |r| {
            r.topic3.map(|t| t.as_slice().to_vec())
        })?;

        Self::append_var_col(dir, "data.col", new_row_count, rows, existing_rows)?;
        Self::append_canonical_bitmap(dir, new_row_count, rows.len() as u64)?;

        Ok(())
    }

    fn write_fixed_col(
        dir: &Path,
        name: &str,
        row_count: u64,
        rows: &[LogRow],
        extract: impl Fn(&LogRow) -> Vec<u8>,
    ) -> io::Result<()> {
        let path = dir.join(name);
        let file = File::create(&path)?;
        let mut w = BufWriter::new(file);

        let header = ColumnFileHeader {
            version: COLUMN_VERSION,
            row_count,
            compression: 0,
        };
        header.write_to(&mut w)?;

        for row in rows {
            w.write_all(&extract(row))?;
        }
        w.flush()?;
        Ok(())
    }

    fn append_fixed_col(
        dir: &Path,
        name: &str,
        new_row_count: u64,
        rows: &[LogRow],
        extract: impl Fn(&LogRow) -> Vec<u8>,
    ) -> io::Result<()> {
        let path = dir.join(name);
        let mut data = fs::read(&path)?;

        // Update row count in header
        data[8..16].copy_from_slice(&new_row_count.to_le_bytes());

        // Append new data
        let mut file = File::create(&path)?;
        file.write_all(&data)?;
        for row in rows {
            file.write_all(&extract(row))?;
        }
        file.flush()?;
        Ok(())
    }

    fn write_nullable_col(
        dir: &Path,
        base_name: &str,
        row_count: u64,
        rows: &[LogRow],
        extract: impl Fn(&LogRow) -> Option<Vec<u8>>,
    ) -> io::Result<()> {
        let col_path = dir.join(format!("{base_name}.col"));
        let null_path = dir.join(format!("{base_name}.null"));

        let col_file = File::create(&col_path)?;
        let mut w = BufWriter::new(col_file);
        let mut nulls = NullBitmap::new();

        let header = ColumnFileHeader {
            version: COLUMN_VERSION,
            row_count,
            compression: 0,
        };
        header.write_to(&mut w)?;

        // For nullable columns, write 32 zero bytes when null, actual value when present.
        let zero = vec![0u8; 32];
        for row in rows {
            if let Some(val) = extract(row) {
                w.write_all(&val)?;
                nulls.push(true);
            } else {
                w.write_all(&zero)?;
                nulls.push(false);
            }
        }
        w.flush()?;

        // Write null bitmap
        let null_file = File::create(&null_path)?;
        let mut nw = BufWriter::new(null_file);
        nulls.write_to(&mut nw)?;
        nw.flush()?;

        Ok(())
    }

    fn append_nullable_col(
        dir: &Path,
        base_name: &str,
        new_row_count: u64,
        rows: &[LogRow],
        extract: impl Fn(&LogRow) -> Option<Vec<u8>>,
    ) -> io::Result<()> {
        let col_path = dir.join(format!("{base_name}.col"));
        let null_path = dir.join(format!("{base_name}.null"));

        // Read existing column data and update header
        let mut col_data = fs::read(&col_path)?;
        col_data[8..16].copy_from_slice(&new_row_count.to_le_bytes());

        let mut col_file = File::create(&col_path)?;
        col_file.write_all(&col_data)?;

        // Read existing null bitmap and append
        let null_data = fs::read(&null_path)?;
        let mut nulls = NullBitmap::read_from(&null_data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt null bitmap"))?;

        let zero = vec![0u8; 32];
        for row in rows {
            if let Some(val) = extract(row) {
                col_file.write_all(&val)?;
                nulls.push(true);
            } else {
                col_file.write_all(&zero)?;
                nulls.push(false);
            }
        }
        col_file.flush()?;

        let null_file = File::create(&null_path)?;
        let mut nw = BufWriter::new(null_file);
        nulls.write_to(&mut nw)?;
        nw.flush()?;

        Ok(())
    }

    /// Variable-length column: 4-byte offset array (one per row + 1 sentinel) + data blob.
    fn write_var_col(dir: &Path, name: &str, row_count: u64, rows: &[LogRow]) -> io::Result<()> {
        let path = dir.join(name);
        let file = File::create(&path)?;
        let mut w = BufWriter::new(file);

        let header = ColumnFileHeader {
            version: COLUMN_VERSION,
            row_count,
            compression: 0,
        };
        header.write_to(&mut w)?;

        // Compute offsets
        let mut offset: u64 = 0;
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        for row in rows {
            offsets.push(offset);
            offset += row.data.len() as u64;
        }
        offsets.push(offset); // sentinel

        // Write offset array
        for o in &offsets {
            w.write_all(&o.to_le_bytes())?;
        }

        // Write data blob
        for row in rows {
            w.write_all(&row.data)?;
        }
        w.flush()?;
        Ok(())
    }

    fn append_var_col(
        dir: &Path,
        name: &str,
        new_row_count: u64,
        rows: &[LogRow],
        _existing_rows: u64,
    ) -> io::Result<()> {
        let path = dir.join(name);
        let data = fs::read(&path)?;

        let header = ColumnFileHeader::read_from(&data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt data column"))?;
        let old_count = header.row_count as usize;

        // Read existing offsets
        let offset_start = ColumnFileHeader::SIZE;
        let offsets_size = (old_count + 1) * 8;
        let data_start = offset_start + offsets_size;

        let mut old_offsets = Vec::with_capacity(old_count + 1);
        for i in 0..=old_count {
            let pos = offset_start + i * 8;
            let o = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            old_offsets.push(o);
        }
        let existing_data = &data[data_start..];
        let existing_data_len = *old_offsets.last().unwrap();

        // Compute new offsets
        let mut new_offsets = Vec::with_capacity(rows.len() + 1);
        let mut off = existing_data_len;
        for row in rows {
            new_offsets.push(off);
            off += row.data.len() as u64;
        }
        new_offsets.push(off);

        // Write everything fresh
        let file = File::create(&path)?;
        let mut w = BufWriter::new(file);

        let new_header = ColumnFileHeader {
            version: COLUMN_VERSION,
            row_count: new_row_count,
            compression: 0,
        };
        new_header.write_to(&mut w)?;

        // All offsets: old (without sentinel) + new (with sentinel)
        for o in &old_offsets[..old_count] {
            w.write_all(&o.to_le_bytes())?;
        }
        for o in &new_offsets {
            w.write_all(&o.to_le_bytes())?;
        }

        // All data
        w.write_all(existing_data)?;
        for row in rows {
            w.write_all(&row.data)?;
        }
        w.flush()?;
        Ok(())
    }

    /// Write a canonical bitmap where all rows are marked canonical (all 1s).
    fn write_canonical_bitmap(dir: &Path, row_count: u64) -> io::Result<()> {
        let path = dir.join("canonical.bitmap");
        let file = File::create(&path)?;
        let mut w = BufWriter::new(file);

        let mut bitmap = NullBitmap::new();
        for _ in 0..row_count {
            bitmap.push(true);
        }
        bitmap.write_to(&mut w)?;
        w.flush()?;
        Ok(())
    }

    fn append_canonical_bitmap(dir: &Path, _new_row_count: u64, new_rows: u64) -> io::Result<()> {
        let path = dir.join("canonical.bitmap");
        let data = fs::read(&path)?;
        let mut bitmap = NullBitmap::read_from(&data).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "corrupt canonical bitmap")
        })?;

        for _ in 0..new_rows {
            bitmap.push(true);
        }

        let file = File::create(&path)?;
        let mut w = BufWriter::new(file);
        bitmap.write_to(&mut w)?;
        w.flush()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_header_roundtrip() {
        let header = ColumnFileHeader {
            version: 1,
            row_count: 42,
            compression: 0,
        };
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();
        assert_eq!(buf.len(), ColumnFileHeader::SIZE);

        let parsed = ColumnFileHeader::read_from(&buf).unwrap();
        assert_eq!(parsed.version, 1);
        assert_eq!(parsed.row_count, 42);
        assert_eq!(parsed.compression, 0);
    }

    #[test]
    fn test_null_bitmap_roundtrip() {
        let mut bitmap = NullBitmap::new();
        bitmap.push(true);
        bitmap.push(false);
        bitmap.push(true);
        bitmap.push(true);
        bitmap.push(false);

        assert!(bitmap.is_present(0));
        assert!(!bitmap.is_present(1));
        assert!(bitmap.is_present(2));
        assert!(bitmap.is_present(3));
        assert!(!bitmap.is_present(4));
        assert_eq!(bitmap.len(), 5);

        let mut buf = Vec::new();
        bitmap.write_to(&mut buf).unwrap();

        let parsed = NullBitmap::read_from(&buf).unwrap();
        assert_eq!(parsed.len(), 5);
        assert!(parsed.is_present(0));
        assert!(!parsed.is_present(1));
        assert!(parsed.is_present(2));
        assert!(parsed.is_present(3));
        assert!(!parsed.is_present(4));
    }
}
