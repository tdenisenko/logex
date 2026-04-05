use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;

use logex_types::LogRow;

/// Write-ahead log for crash recovery.
///
/// Each WAL entry consists of:
/// - 4 bytes: row count (u32, little-endian)
/// - N bytes: JSON-serialized Vec<LogRow>
/// - 4 bytes: CRC32 checksum of the serialized data
///
/// On startup, any valid entries are replayed into the hot partition,
/// then the WAL is truncated.
pub struct WriteAheadLog {
    path: PathBuf,
}

impl WriteAheadLog {
    pub fn open(path: PathBuf) -> io::Result<Self> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(Self { path })
    }

    /// Append a batch of rows to the WAL.
    pub fn append(&mut self, rows: &[LogRow]) -> io::Result<()> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut w = BufWriter::new(file);

        let serialized = serde_json::to_vec(rows)?;
        let row_count = rows.len() as u32;
        let checksum = crc32fast::hash(&serialized);

        w.write_all(&row_count.to_le_bytes())?;
        w.write_all(&(serialized.len() as u32).to_le_bytes())?;
        w.write_all(&serialized)?;
        w.write_all(&checksum.to_le_bytes())?;
        w.flush()?;

        // fsync for durability
        w.get_ref().sync_all()?;

        Ok(())
    }

    /// Read all valid entries from the WAL.
    pub fn read_all(&self) -> io::Result<Vec<LogRow>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();
        if file_len == 0 {
            return Ok(Vec::new());
        }

        let mut reader = BufReader::new(file);
        let mut all_rows = Vec::new();

        loop {
            // Try to read an entry header
            let mut header_buf = [0u8; 8]; // row_count (4) + data_len (4)
            match reader.read_exact(&mut header_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            let _row_count = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
            let data_len = u32::from_le_bytes(header_buf[4..8].try_into().unwrap()) as usize;

            // Read serialized data
            let mut data = vec![0u8; data_len];
            match reader.read_exact(&mut data) {
                Ok(()) => {}
                Err(_) => {
                    tracing::warn!("WAL entry truncated, skipping remaining entries");
                    break;
                }
            }

            // Read and verify checksum
            let mut crc_buf = [0u8; 4];
            match reader.read_exact(&mut crc_buf) {
                Ok(()) => {}
                Err(_) => {
                    tracing::warn!("WAL checksum missing, skipping entry");
                    break;
                }
            }

            let stored_crc = u32::from_le_bytes(crc_buf);
            let computed_crc = crc32fast::hash(&data);
            if stored_crc != computed_crc {
                tracing::warn!(
                    stored = stored_crc,
                    computed = computed_crc,
                    "WAL checksum mismatch, skipping entry"
                );
                break;
            }

            // Deserialize rows
            match serde_json::from_slice::<Vec<LogRow>>(&data) {
                Ok(rows) => all_rows.extend(rows),
                Err(e) => {
                    tracing::warn!(error = %e, "WAL entry deserialization failed, skipping");
                    break;
                }
            }
        }

        Ok(all_rows)
    }

    /// Truncate the WAL (called after successful commit to storage).
    pub fn truncate(&mut self) -> io::Result<()> {
        if self.path.exists() {
            File::create(&self.path)?; // truncates to zero
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use logex_types::Source;
    use tempfile::TempDir;

    fn make_test_rows(count: usize) -> Vec<LogRow> {
        (0..count)
            .map(|i| LogRow {
                block_number: 1000 + i as u64,
                block_hash: B256::repeat_byte(0xAA),
                timestamp: 1_700_000_000 + i as u64 * 12,
                tx_hash: B256::repeat_byte(0xBB),
                tx_index: 0,
                log_index: i as u32,
                address: Address::repeat_byte(0x01),
                topic0: Some(B256::repeat_byte(0x10)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!("cafe"),
                data_len: 2,
                source: Source::Receipt,
            })
            .collect()
    }

    #[test]
    fn test_wal_write_and_read() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        let mut wal = WriteAheadLog::open(wal_path).unwrap();
        let rows = make_test_rows(10);
        wal.append(&rows).unwrap();

        let recovered = wal.read_all().unwrap();
        assert_eq!(recovered.len(), 10);
        assert_eq!(recovered[0].block_number, 1000);
        assert_eq!(recovered[9].block_number, 1009);
    }

    #[test]
    fn test_wal_multiple_appends() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        let mut wal = WriteAheadLog::open(wal_path).unwrap();
        wal.append(&make_test_rows(5)).unwrap();
        wal.append(&make_test_rows(3)).unwrap();

        let recovered = wal.read_all().unwrap();
        assert_eq!(recovered.len(), 8);
    }

    #[test]
    fn test_wal_truncate() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        let mut wal = WriteAheadLog::open(wal_path).unwrap();
        wal.append(&make_test_rows(10)).unwrap();
        wal.truncate().unwrap();

        let recovered = wal.read_all().unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_wal_empty_read() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("nonexistent.wal");

        let wal = WriteAheadLog::open(wal_path).unwrap();
        let recovered = wal.read_all().unwrap();
        assert!(recovered.is_empty());
    }

    #[test]
    fn test_wal_corrupt_data_stops_at_corruption() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        // Write valid entries
        let mut wal = WriteAheadLog::open(wal_path.clone()).unwrap();
        wal.append(&make_test_rows(5)).unwrap();

        // Append garbage
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(b"garbage data that is not valid").unwrap();

        // Should recover the valid entries
        let wal = WriteAheadLog::open(wal_path).unwrap();
        let recovered = wal.read_all().unwrap();
        assert_eq!(recovered.len(), 5);
    }
}
