use crate::durability;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::thread;

use logex_types::LogRow;

/// Magic bytes identifying a LogEx column file.
const COLUMN_MAGIC: &[u8; 4] = b"LXCL";

/// Current column file format version.
const COLUMN_VERSION: u32 = 1;
const ZERO_B256: [u8; 32] = [0; 32];

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

    /// Set the value at position `row`.
    pub fn set(&mut self, row: u64, present: bool) {
        let byte_idx = (row / 8) as usize;
        let bit_idx = (row % 8) as u32;
        if byte_idx < self.bits.len() {
            if present {
                self.bits[byte_idx] |= 1 << bit_idx;
            } else {
                self.bits[byte_idx] &= !(1 << bit_idx);
            }
        }
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
/// Column replacements order their contents before rename. The storage caller
/// must synchronize the complete segment before publishing its manifest.
pub struct ColumnFile;

fn join_write_worker(handle: thread::ScopedJoinHandle<'_, io::Result<()>>) -> io::Result<()> {
    handle
        .join()
        .map_err(|_| io::Error::other("column write worker panicked"))?
}

impl ColumnFile {
    /// Write all fixed-size and variable-length column files for a batch of rows.
    pub fn write_batch(dir: &Path, rows: &[LogRow]) -> io::Result<()> {
        Self::write_batch_with_canonical(dir, rows, None)
    }

    pub(crate) fn write_batch_with_canonical(
        dir: &Path,
        rows: &[LogRow],
        canonical: Option<&NullBitmap>,
    ) -> io::Result<()> {
        if canonical.is_some_and(|bitmap| bitmap.len() != rows.len() as u64) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "canonical bitmap length differs from replacement rows",
            ));
        }
        fs::create_dir_all(dir)?;
        let row_count = rows.len() as u64;

        thread::scope(|scope| {
            let address = scope.spawn(|| {
                Self::write_fixed_col(dir, "address.col", row_count, rows, |w, r| {
                    w.write_all(r.address.as_slice())
                })
            });
            let block_number = scope.spawn(|| {
                Self::write_fixed_col(dir, "block_number.col", row_count, rows, |w, r| {
                    w.write_all(&r.block_number.to_le_bytes())
                })
            });
            let block_hash = scope.spawn(|| {
                Self::write_fixed_col(dir, "block_hash.col", row_count, rows, |w, r| {
                    w.write_all(r.block_hash.as_slice())
                })
            });
            let tx_hash = scope.spawn(|| {
                Self::write_fixed_col(dir, "tx_hash.col", row_count, rows, |w, r| {
                    w.write_all(r.tx_hash.as_slice())
                })
            });
            let tx_index = scope.spawn(|| {
                Self::write_fixed_col(dir, "tx_index.col", row_count, rows, |w, r| {
                    w.write_all(&r.tx_index.to_le_bytes())
                })
            });
            let log_index = scope.spawn(|| {
                Self::write_fixed_col(dir, "log_index.col", row_count, rows, |w, r| {
                    w.write_all(&r.log_index.to_le_bytes())
                })
            });
            let timestamp = scope.spawn(|| {
                Self::write_fixed_col(dir, "timestamp.col", row_count, rows, |w, r| {
                    w.write_all(&r.timestamp.to_le_bytes())
                })
            });
            let data_len = scope.spawn(|| {
                Self::write_fixed_col(dir, "data_len.col", row_count, rows, |w, r| {
                    w.write_all(&r.data_len.to_le_bytes())
                })
            });
            let source = scope.spawn(|| {
                Self::write_fixed_col(dir, "source.col", row_count, rows, |w, r| {
                    w.write_all(&[r.source as u8])
                })
            });
            let topic0 = scope.spawn(|| {
                Self::write_nullable_col(dir, "topic0", row_count, rows, |w, r| {
                    write_optional_b256(w, r.topic0.as_ref())
                })
            });
            let topic1 = scope.spawn(|| {
                Self::write_nullable_col(dir, "topic1", row_count, rows, |w, r| {
                    write_optional_b256(w, r.topic1.as_ref())
                })
            });
            let topic2 = scope.spawn(|| {
                Self::write_nullable_col(dir, "topic2", row_count, rows, |w, r| {
                    write_optional_b256(w, r.topic2.as_ref())
                })
            });
            let topic3 = scope.spawn(|| {
                Self::write_nullable_col(dir, "topic3", row_count, rows, |w, r| {
                    write_optional_b256(w, r.topic3.as_ref())
                })
            });
            let data = scope.spawn(|| Self::write_var_col(dir, "data.col", row_count, rows));
            let canonical = scope.spawn(|| match canonical {
                Some(bitmap) => Self::replace_canonical_bitmap(dir, bitmap),
                None => Self::write_canonical_bitmap(dir, row_count),
            });

            join_write_worker(address)?;
            join_write_worker(block_number)?;
            join_write_worker(block_hash)?;
            join_write_worker(tx_hash)?;
            join_write_worker(tx_index)?;
            join_write_worker(log_index)?;
            join_write_worker(timestamp)?;
            join_write_worker(data_len)?;
            join_write_worker(source)?;
            join_write_worker(topic0)?;
            join_write_worker(topic1)?;
            join_write_worker(topic2)?;
            join_write_worker(topic3)?;
            join_write_worker(data)?;
            join_write_worker(canonical)?;
            Ok(())
        })
    }

    /// Append rows to existing column files (for the hot partition).
    pub fn append_batch(dir: &Path, rows: &[LogRow], existing_rows: u64) -> io::Result<()> {
        if !dir.exists() {
            return Self::write_batch(dir, rows);
        }

        let new_row_count = existing_rows + rows.len() as u64;

        Self::append_fixed_col(
            dir,
            "address.col",
            existing_rows,
            new_row_count,
            rows,
            |w, r| w.write_all(r.address.as_slice()),
        )?;
        Self::append_fixed_col(
            dir,
            "block_number.col",
            existing_rows,
            new_row_count,
            rows,
            |w, r| w.write_all(&r.block_number.to_le_bytes()),
        )?;
        Self::append_fixed_col(
            dir,
            "block_hash.col",
            existing_rows,
            new_row_count,
            rows,
            |w, r| w.write_all(r.block_hash.as_slice()),
        )?;
        Self::append_fixed_col(
            dir,
            "tx_hash.col",
            existing_rows,
            new_row_count,
            rows,
            |w, r| w.write_all(r.tx_hash.as_slice()),
        )?;
        Self::append_fixed_col(
            dir,
            "tx_index.col",
            existing_rows,
            new_row_count,
            rows,
            |w, r| w.write_all(&r.tx_index.to_le_bytes()),
        )?;
        Self::append_fixed_col(
            dir,
            "log_index.col",
            existing_rows,
            new_row_count,
            rows,
            |w, r| w.write_all(&r.log_index.to_le_bytes()),
        )?;
        Self::append_fixed_col(
            dir,
            "timestamp.col",
            existing_rows,
            new_row_count,
            rows,
            |w, r| w.write_all(&r.timestamp.to_le_bytes()),
        )?;
        Self::append_fixed_col(
            dir,
            "data_len.col",
            existing_rows,
            new_row_count,
            rows,
            |w, r| w.write_all(&r.data_len.to_le_bytes()),
        )?;
        Self::append_fixed_col(
            dir,
            "source.col",
            existing_rows,
            new_row_count,
            rows,
            |w, r| w.write_all(&[r.source as u8]),
        )?;

        Self::append_nullable_col(dir, "topic0", existing_rows, new_row_count, rows, |w, r| {
            write_optional_b256(w, r.topic0.as_ref())
        })?;
        Self::append_nullable_col(dir, "topic1", existing_rows, new_row_count, rows, |w, r| {
            write_optional_b256(w, r.topic1.as_ref())
        })?;
        Self::append_nullable_col(dir, "topic2", existing_rows, new_row_count, rows, |w, r| {
            write_optional_b256(w, r.topic2.as_ref())
        })?;
        Self::append_nullable_col(dir, "topic3", existing_rows, new_row_count, rows, |w, r| {
            write_optional_b256(w, r.topic3.as_ref())
        })?;

        Self::append_var_col(dir, "data.col", new_row_count, rows, existing_rows)?;
        Self::append_canonical_bitmap(dir, existing_rows, rows.len() as u64)?;

        Ok(())
    }

    fn write_fixed_col(
        dir: &Path,
        name: &str,
        row_count: u64,
        rows: &[LogRow],
        mut write_value: impl FnMut(&mut BufWriter<File>, &LogRow) -> io::Result<()>,
    ) -> io::Result<()> {
        durability::atomic_replace_ordered(&dir.join(name), |writer| {
            ColumnFileHeader {
                version: COLUMN_VERSION,
                row_count,
                compression: 0,
            }
            .write_to(writer)?;
            for row in rows {
                write_value(writer, row)?;
            }
            Ok(())
        })
    }

    fn append_fixed_col(
        dir: &Path,
        name: &str,
        existing_rows: u64,
        new_row_count: u64,
        rows: &[LogRow],
        mut write_value: impl FnMut(&mut BufWriter<File>, &LogRow) -> io::Result<()>,
    ) -> io::Result<()> {
        let path = dir.join(name);
        let file = Self::open_col_for_append(&path, existing_rows, new_row_count)?;
        let mut file = BufWriter::new(file);
        for row in rows {
            write_value(&mut file, row)?;
        }
        file.flush()?;
        Ok(())
    }

    fn write_nullable_col(
        dir: &Path,
        base_name: &str,
        row_count: u64,
        rows: &[LogRow],
        mut write_value: impl FnMut(&mut BufWriter<File>, &LogRow) -> io::Result<bool>,
    ) -> io::Result<()> {
        let mut nulls = NullBitmap::new();
        durability::atomic_replace_ordered(&dir.join(format!("{base_name}.col")), |writer| {
            ColumnFileHeader {
                version: COLUMN_VERSION,
                row_count,
                compression: 0,
            }
            .write_to(writer)?;
            for row in rows {
                nulls.push(write_value(writer, row)?);
            }
            Ok(())
        })?;
        durability::atomic_replace_ordered(&dir.join(format!("{base_name}.null")), |writer| {
            nulls.write_to(writer)
        })
    }

    fn append_nullable_col(
        dir: &Path,
        base_name: &str,
        existing_rows: u64,
        new_row_count: u64,
        rows: &[LogRow],
        mut write_value: impl FnMut(&mut BufWriter<File>, &LogRow) -> io::Result<bool>,
    ) -> io::Result<()> {
        let col_path = dir.join(format!("{base_name}.col"));
        let null_path = dir.join(format!("{base_name}.null"));

        let col_file = Self::open_col_for_append(&col_path, existing_rows, new_row_count)?;
        let mut col_file = BufWriter::new(col_file);

        // Read existing null bitmap and append
        let null_data = fs::read(&null_path)?;
        let mut nulls = NullBitmap::read_from(&null_data)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "corrupt null bitmap"))?;
        if nulls.len() != existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "null bitmap row count mismatch for {base_name}: expected {existing_rows}, got {}",
                    nulls.len()
                ),
            ));
        }

        for row in rows {
            nulls.push(write_value(&mut col_file, row)?);
        }
        col_file.flush()?;

        durability::atomic_replace_ordered(&null_path, |nw| nulls.write_to(nw))?;

        Ok(())
    }

    /// Variable-length column: 8-byte offsets (one per row plus sentinel), then data.
    fn write_var_col(dir: &Path, name: &str, row_count: u64, rows: &[LogRow]) -> io::Result<()> {
        durability::atomic_replace_ordered(&dir.join(name), |writer| {
            ColumnFileHeader {
                version: COLUMN_VERSION,
                row_count,
                compression: 0,
            }
            .write_to(writer)?;
            let mut offset = 0u64;
            for row in rows {
                writer.write_all(&offset.to_le_bytes())?;
                offset = offset.checked_add(row.data.len() as u64).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "data column size overflow")
                })?;
            }
            writer.write_all(&offset.to_le_bytes())?;
            for row in rows {
                writer.write_all(&row.data)?;
            }
            Ok(())
        })
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
        if header.row_count != _existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "data column row count mismatch: expected {_existing_rows}, got {}",
                    header.row_count
                ),
            ));
        }

        // Read existing offsets
        let offset_start = ColumnFileHeader::SIZE;
        let offsets_size = (old_count + 1) * 8;
        let data_start = offset_start + offsets_size;

        let mut old_offsets = Vec::with_capacity(old_count + 1);
        for i in 0..=old_count {
            let pos = offset_start + i * 8;
            let end = pos + 8;
            let o = u64::from_le_bytes(
                data.get(pos..end)
                    .ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "truncated offset array")
                    })?
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid offset"))?,
            );
            old_offsets.push(o);
        }
        let existing_data = &data[data_start..];
        let existing_data_len = old_offsets.last().copied().unwrap_or(0);

        // Compute new offsets
        let mut new_offsets = Vec::with_capacity(rows.len() + 1);
        let mut off = existing_data_len;
        for row in rows {
            new_offsets.push(off);
            off += row.data.len() as u64;
        }
        new_offsets.push(off);

        durability::atomic_replace_ordered(&path, |w| {
            let new_header = ColumnFileHeader {
                version: COLUMN_VERSION,
                row_count: new_row_count,
                compression: 0,
            };
            new_header.write_to(w)?;

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
            Ok(())
        })?;
        Ok(())
    }

    /// Write a canonical bitmap where all rows are marked canonical (all 1s).
    pub(crate) fn write_canonical_bitmap(dir: &Path, row_count: u64) -> io::Result<()> {
        let mut bitmap = NullBitmap::new();
        for _ in 0..row_count {
            bitmap.push(true);
        }
        Self::replace_canonical_bitmap(dir, &bitmap)
    }

    pub(crate) fn replace_canonical_bitmap(dir: &Path, bitmap: &NullBitmap) -> io::Result<()> {
        durability::atomic_write(&dir.join("canonical.bitmap"), |writer| {
            bitmap.write_to(writer)
        })
    }

    fn append_canonical_bitmap(dir: &Path, existing_rows: u64, new_rows: u64) -> io::Result<()> {
        let path = dir.join("canonical.bitmap");
        let data = fs::read(&path)?;
        let mut bitmap = NullBitmap::read_from(&data).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "corrupt canonical bitmap")
        })?;
        if bitmap.len() != existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "canonical bitmap row count mismatch: expected {existing_rows}, got {}",
                    bitmap.len()
                ),
            ));
        }

        for _ in 0..new_rows {
            bitmap.push(true);
        }

        durability::atomic_replace_ordered(&path, |w| bitmap.write_to(w))?;
        Ok(())
    }

    fn open_col_for_append(
        path: &Path,
        existing_rows: u64,
        new_row_count: u64,
    ) -> io::Result<File> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut header_buf = [0u8; ColumnFileHeader::SIZE];
        file.read_exact(&mut header_buf)?;
        let header = ColumnFileHeader::read_from(&header_buf).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("corrupt column header in {}", path.display()),
            )
        })?;
        if header.row_count != existing_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "column row count mismatch for {}: expected {existing_rows}, got {}",
                    path.display(),
                    header.row_count
                ),
            ));
        }

        file.seek(SeekFrom::Start(8))?;
        file.write_all(&new_row_count.to_le_bytes())?;
        file.seek(SeekFrom::End(0))?;
        Ok(file)
    }
}

fn write_optional_b256(
    writer: &mut BufWriter<File>,
    value: Option<&alloy_primitives::B256>,
) -> io::Result<bool> {
    if let Some(value) = value {
        writer.write_all(value.as_slice())?;
        Ok(true)
    } else {
        writer.write_all(&ZERO_B256)?;
        Ok(false)
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
