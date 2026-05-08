use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Cursor, Read, Write};
use std::path::PathBuf;

use alloy_primitives::{Address, B256, Bytes};
use logex_types::{LogRow, Source};

/// Write-ahead log for crash recovery.
///
/// Each WAL entry consists of:
/// - 4 bytes: row count (u32, little-endian)
/// - 4 bytes: payload byte length (u32, little-endian)
/// - N bytes: compact binary row payload
/// - 4 bytes: CRC32 checksum of the payload
///
/// On startup, any valid entries are replayed into the hot partition,
/// then the WAL is truncated.
pub struct WriteAheadLog {
    path: PathBuf,
}

const WAL_BINARY_MAGIC: &[u8; 4] = b"LXWL";
const WAL_BINARY_VERSION: u32 = 1;

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

        let serialized = encode_rows(rows)?;
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

            let _row_count =
                u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
            let data_len =
                u32::from_le_bytes([header_buf[4], header_buf[5], header_buf[6], header_buf[7]])
                    as usize;

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

            match decode_rows(&data, _row_count as usize) {
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

fn encode_rows(rows: &[LogRow]) -> io::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(rows.len().saturating_mul(160));
    out.extend_from_slice(WAL_BINARY_MAGIC);
    out.extend_from_slice(&WAL_BINARY_VERSION.to_le_bytes());

    for row in rows {
        out.extend_from_slice(&row.block_number.to_le_bytes());
        out.extend_from_slice(row.block_hash.as_slice());
        out.extend_from_slice(&row.timestamp.to_le_bytes());
        out.extend_from_slice(row.tx_hash.as_slice());
        out.extend_from_slice(&row.tx_index.to_le_bytes());
        out.extend_from_slice(&row.log_index.to_le_bytes());
        out.extend_from_slice(row.address.as_slice());
        out.push(topic_presence_mask(row));
        write_optional_b256(&mut out, row.topic0.as_ref());
        write_optional_b256(&mut out, row.topic1.as_ref());
        write_optional_b256(&mut out, row.topic2.as_ref());
        write_optional_b256(&mut out, row.topic3.as_ref());
        out.extend_from_slice(&row.data_len.to_le_bytes());
        let data_len = u32::try_from(row.data.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "log row data is too large to encode in WAL",
            )
        })?;
        out.extend_from_slice(&data_len.to_le_bytes());
        out.extend_from_slice(&row.data);
        out.push(row.source as u8);
    }

    Ok(out)
}

fn decode_rows(data: &[u8], row_count: usize) -> io::Result<Vec<LogRow>> {
    if data.len() < WAL_BINARY_MAGIC.len() + 4 || &data[..4] != WAL_BINARY_MAGIC {
        return serde_json::from_slice::<Vec<LogRow>>(data).map_err(io::Error::other);
    }

    let mut cursor = Cursor::new(data);
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic)?;
    let version = read_u32(&mut cursor)?;
    if version != WAL_BINARY_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported WAL binary version {version}"),
        ));
    }

    let mut rows = Vec::with_capacity(row_count);
    for _ in 0..row_count {
        let block_number = read_u64(&mut cursor)?;
        let block_hash = read_b256(&mut cursor)?;
        let timestamp = read_u64(&mut cursor)?;
        let tx_hash = read_b256(&mut cursor)?;
        let tx_index = read_u32(&mut cursor)?;
        let log_index = read_u32(&mut cursor)?;
        let address = read_address(&mut cursor)?;
        let topics = read_u8(&mut cursor)?;
        let topic0 = read_optional_b256(&mut cursor, topics, 0)?;
        let topic1 = read_optional_b256(&mut cursor, topics, 1)?;
        let topic2 = read_optional_b256(&mut cursor, topics, 2)?;
        let topic3 = read_optional_b256(&mut cursor, topics, 3)?;
        let data_len = read_u32(&mut cursor)?;
        let encoded_data_len = read_u32(&mut cursor)? as usize;
        let mut data = vec![0u8; encoded_data_len];
        cursor.read_exact(&mut data)?;
        let source = Source::from_u8(read_u8(&mut cursor)?)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid WAL row source"))?;

        rows.push(LogRow {
            block_number,
            block_hash,
            timestamp,
            tx_hash,
            tx_index,
            log_index,
            address,
            topic0,
            topic1,
            topic2,
            topic3,
            data: Bytes::from(data),
            data_len,
            source,
        });
    }

    if cursor.position() != data.len() as u64 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "WAL binary payload has trailing bytes",
        ));
    }

    Ok(rows)
}

fn topic_presence_mask(row: &LogRow) -> u8 {
    u8::from(row.topic0.is_some())
        | (u8::from(row.topic1.is_some()) << 1)
        | (u8::from(row.topic2.is_some()) << 2)
        | (u8::from(row.topic3.is_some()) << 3)
}

fn write_optional_b256(out: &mut Vec<u8>, value: Option<&B256>) {
    if let Some(value) = value {
        out.extend_from_slice(value.as_slice());
    }
}

fn read_optional_b256(cursor: &mut Cursor<&[u8]>, mask: u8, index: u8) -> io::Result<Option<B256>> {
    if mask & (1 << index) == 0 {
        return Ok(None);
    }
    read_b256(cursor).map(Some)
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> io::Result<u8> {
    let mut value = [0u8; 1];
    cursor.read_exact(&mut value)?;
    Ok(value[0])
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> io::Result<u32> {
    let mut value = [0u8; 4];
    cursor.read_exact(&mut value)?;
    Ok(u32::from_le_bytes(value))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> io::Result<u64> {
    let mut value = [0u8; 8];
    cursor.read_exact(&mut value)?;
    Ok(u64::from_le_bytes(value))
}

fn read_b256(cursor: &mut Cursor<&[u8]>) -> io::Result<B256> {
    let mut value = [0u8; 32];
    cursor.read_exact(&mut value)?;
    Ok(B256::from(value))
}

fn read_address(cursor: &mut Cursor<&[u8]>) -> io::Result<Address> {
    let mut value = [0u8; 20];
    cursor.read_exact(&mut value)?;
    Ok(Address::from(value))
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
    fn test_wal_reads_legacy_json_payload() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");
        let rows = make_test_rows(2);
        let serialized = serde_json::to_vec(&rows).unwrap();
        let checksum = crc32fast::hash(&serialized);

        {
            let file = File::create(&wal_path).unwrap();
            let mut writer = BufWriter::new(file);
            writer
                .write_all(&(rows.len() as u32).to_le_bytes())
                .unwrap();
            writer
                .write_all(&(serialized.len() as u32).to_le_bytes())
                .unwrap();
            writer.write_all(&serialized).unwrap();
            writer.write_all(&checksum.to_le_bytes()).unwrap();
            writer.flush().unwrap();
        }

        let wal = WriteAheadLog::open(wal_path).unwrap();
        let recovered = wal.read_all().unwrap();
        assert_eq!(recovered, rows);
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
