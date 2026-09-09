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
/// Startup replays entries only after the entire WAL passes recovery validation.
/// Complete corruption and ambiguous truncated payloads are errors; only a partial
/// final header or a valid payload with an incomplete matching checksum is ignored.
pub struct WriteAheadLog {
    path: PathBuf,
}

const WAL_BINARY_MAGIC: &[u8; 4] = b"LXWL";
const WAL_BINARY_VERSION: u32 = 1;
// All fixed row fields, including the topic mask, both data lengths and source.
const WAL_MIN_ROW_BYTES: usize = 118;

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
        // Validate and encode before opening the file, so invalid input cannot
        // create a WAL or leave an incomplete entry after a valid prefix.
        let row_count = u32::try_from(rows.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "too many rows for a WAL entry")
        })?;
        let serialized = encode_rows(rows)?;
        let payload_len = u32::try_from(serialized.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL payload exceeds u32 length",
            )
        })?;
        let checksum = crc32fast::hash(&serialized);

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let mut w = BufWriter::new(file);

        w.write_all(&row_count.to_le_bytes())?;
        w.write_all(&payload_len.to_le_bytes())?;
        w.write_all(&serialized)?;
        w.write_all(&checksum.to_le_bytes())?;
        w.flush()?;

        // fsync for durability
        w.get_ref().sync_all()?;

        Ok(())
    }

    /// Read recoverable entries without modifying the WAL. An error anywhere
    /// prevents the caller from receiving a prefix and discarding later entries.
    pub fn read_all(&self) -> io::Result<Vec<LogRow>> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        let file_len = file.metadata()?.len();
        read_entries(&mut BufReader::new(file), file_len).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("cannot recover WAL {}: {error}", self.path.display()),
            )
        })
    }

    /// Truncate the WAL (called after successful commit to storage).
    pub fn truncate(&mut self) -> io::Result<()> {
        match OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)
        {
            Ok(_) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

/// The length is a snapshot, not a substitute for read errors or exclusivity.
fn read_entries(reader: &mut impl Read, file_len: u64) -> io::Result<Vec<LogRow>> {
    let mut all_rows = Vec::new();
    let mut offset = 0;
    while offset < file_len {
        let entry = read_entry(reader, file_len - offset).map_err(|error| {
            io::Error::new(error.kind(), format!("entry at byte {offset}: {error}"))
        })?;
        let Some((rows, bytes_read)) = entry else {
            break;
        };
        all_rows.try_reserve(rows.len()).map_err(io::Error::other)?;
        all_rows.extend(rows);
        offset += bytes_read;
    }
    // Fail if the WAL grew during recovery instead of allowing startup to erase
    // bytes that were never inspected. Cross-process exclusion is still required.
    let mut extra = [0];
    if reader.read(&mut extra)? != 0 {
        return Err(invalid_data("WAL changed during recovery"));
    }
    Ok(all_rows)
}

fn read_entry(reader: &mut impl Read, remaining: u64) -> io::Result<Option<(Vec<LogRow>, u64)>> {
    let mut header = [0; 8];
    if remaining < header.len() as u64 {
        reader.read_exact(&mut header[..remaining as usize])?;
        return Ok(None);
    }
    reader.read_exact(&mut header)?;
    let row_count = u32::from_le_bytes([header[0], header[1], header[2], header[3]]) as usize;
    let data_len = u32::from_le_bytes([header[4], header[5], header[6], header[7]]) as usize;
    let payload_remaining = remaining - header.len() as u64;
    if data_len as u64 > payload_remaining {
        // The outer header is not checksummed. A corrupted length could conceal
        // later complete entries, so this cannot be classified as a safe tail.
        return Err(invalid_data(
            "payload length exceeds remaining WAL bytes; preserve the WAL for inspection",
        ));
    }
    let mut data = Vec::new();
    data.try_reserve_exact(data_len).map_err(io::Error::other)?;
    data.resize(data_len, 0);
    reader.read_exact(&mut data)?;

    let crc_len = (payload_remaining - data_len as u64).min(4) as usize;
    let mut stored_crc = [0; 4];
    reader.read_exact(&mut stored_crc[..crc_len])?;
    let computed_crc = crc32fast::hash(&data).to_le_bytes();
    if stored_crc[..crc_len] != computed_crc[..crc_len] {
        return Err(invalid_data("WAL checksum mismatch"));
    }
    let rows = decode_rows(&data, row_count)?;
    if crc_len < 4 {
        // A full, valid payload and matching checksum prefix establish an
        // incomplete final entry. Never replay an entry without its full CRC.
        return Ok(None);
    }
    Ok(Some((rows, 8 + data_len as u64 + 4)))
}

fn invalid_data(message: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn validate_row_data(row: &LogRow) -> io::Result<()> {
    if usize::try_from(row.data_len).ok() != Some(row.data.len()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "WAL row data_len does not match data bytes",
        ));
    }
    Ok(())
}

fn encode_rows(rows: &[LogRow]) -> io::Result<Vec<u8>> {
    let mut payload_len = 8usize;
    for row in rows {
        validate_row_data(row)?;
        let fixed_len = WAL_MIN_ROW_BYTES + topic_presence_mask(row).count_ones() as usize * 32;
        payload_len = payload_len
            .checked_add(fixed_len)
            .and_then(|len| len.checked_add(row.data.len()))
            .filter(|&len| u32::try_from(len).is_ok())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "WAL payload exceeds u32 length",
                )
            })?;
    }
    let mut out = Vec::new();
    out.try_reserve_exact(payload_len)
        .map_err(io::Error::other)?;
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
        // Both lengths are retained for compatibility; equality was checked above.
        out.extend_from_slice(&row.data_len.to_le_bytes());
        out.extend_from_slice(&row.data);
        out.push(row.source as u8);
    }

    Ok(out)
}

fn decode_rows(data: &[u8], row_count: usize) -> io::Result<Vec<LogRow>> {
    if data.len() < WAL_BINARY_MAGIC.len() + 4 || &data[..4] != WAL_BINARY_MAGIC {
        let rows = serde_json::from_slice::<Vec<LogRow>>(data).map_err(invalid_data)?;
        if rows.len() != row_count {
            return Err(invalid_data(
                "WAL JSON row count does not match entry header",
            ));
        }
        for row in &rows {
            validate_row_data(row).map_err(invalid_data)?;
        }
        return Ok(rows);
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

    if row_count > (data.len() - 8) / WAL_MIN_ROW_BYTES {
        return Err(invalid_data("WAL row count exceeds available payload"));
    }
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count)
        .map_err(io::Error::other)?;
    for _ in 0..row_count {
        let block_number = read_u64(&mut cursor)?;
        let block_hash = read_b256(&mut cursor)?;
        let timestamp = read_u64(&mut cursor)?;
        let tx_hash = read_b256(&mut cursor)?;
        let tx_index = read_u32(&mut cursor)?;
        let log_index = read_u32(&mut cursor)?;
        let address = read_address(&mut cursor)?;
        let topics = read_u8(&mut cursor)?;
        if topics & !0x0f != 0 {
            return Err(invalid_data("invalid WAL topic mask"));
        }
        let topic0 = read_optional_b256(&mut cursor, topics, 0)?;
        let topic1 = read_optional_b256(&mut cursor, topics, 1)?;
        let topic2 = read_optional_b256(&mut cursor, topics, 2)?;
        let topic3 = read_optional_b256(&mut cursor, topics, 3)?;
        let data_len = read_u32(&mut cursor)?;
        let encoded_data_len = read_u32(&mut cursor)? as usize;
        if data_len as usize != encoded_data_len {
            return Err(invalid_data("WAL row data lengths disagree"));
        }
        let remaining = data.len() - cursor.position() as usize;
        if encoded_data_len >= remaining {
            return Err(invalid_data(
                "WAL row data exceeds payload or leaves no source byte",
            ));
        }
        let mut row_data = Vec::new();
        row_data
            .try_reserve_exact(encoded_data_len)
            .map_err(io::Error::other)?;
        // The checked slice is already initialized. Copy it once instead of
        // zero-filling a row buffer and then overwriting it through Read.
        let start = cursor.position() as usize;
        let end = start + encoded_data_len;
        row_data.extend_from_slice(&data[start..end]);
        cursor.set_position(end as u64);
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
            data: Bytes::from(row_data),
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
    fn complete_corrupt_wal_entry_is_an_error_not_a_successful_prefix() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.wal");
        let mut wal = WriteAheadLog::open(path.clone()).unwrap();
        wal.append(&make_test_rows(1)).unwrap();
        let first_end = fs::metadata(&path).unwrap().len() as usize;
        wal.append(&make_test_rows(1)).unwrap();
        wal.append(&make_test_rows(1)).unwrap();
        let mut bytes = fs::read(&path).unwrap();
        bytes[first_end + 16] ^= 1; // payload byte in a complete middle entry
        fs::write(&path, &bytes).unwrap();
        assert!(wal.read_all().is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
    }

    #[test]
    fn wal_legacy_row_count_must_match_the_payload() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.wal");
        let payload = serde_json::to_vec(&make_test_rows(1)).unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        fs::write(&path, bytes).unwrap();
        assert!(WriteAheadLog::open(path).unwrap().read_all().is_err());
    }

    #[test]
    fn wal_binary_row_shape_must_not_be_silently_normalized() {
        let mut rows = make_test_rows(1);
        rows[0].topic0 = None;
        let bytes = encode_rows(&rows).unwrap();
        let mut bad_mask = bytes.clone();
        bad_mask[8 + 108] = 0x80;
        assert!(decode_rows(&bad_mask, 1).is_err());
        let mut bad_length = bytes;
        bad_length[8 + 109..8 + 113].copy_from_slice(&999u32.to_le_bytes());
        assert!(decode_rows(&bad_length, 1).is_err());
    }

    #[test]
    fn append_invalid_row_preserves_existing_wal() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("test.wal");
        let mut wal = WriteAheadLog::open(path.clone()).unwrap();
        let mut rows = make_test_rows(1);
        wal.append(&rows).unwrap();
        let original = fs::read(&path).unwrap();
        rows[0].data_len += 1;
        assert!(wal.append(&rows).is_err());
        assert_eq!(fs::read(path).unwrap(), original);
    }

    fn frame(rows: &[LogRow]) -> Vec<u8> {
        let payload = encode_rows(rows).unwrap();
        let mut bytes = (rows.len() as u32).to_le_bytes().to_vec();
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        bytes
    }

    #[test]
    fn wal_truncation_policy_at_every_byte() {
        let rows = make_test_rows(2);
        let prefix = frame(&rows);
        let tail = frame(&rows[..1]);
        for cut in 0..=tail.len() {
            let bytes = [prefix.as_slice(), &tail[..cut]].concat();
            let result = read_entries(&mut bytes.as_slice(), bytes.len() as u64);
            if cut < 8 || (tail.len() - 4..tail.len()).contains(&cut) {
                assert_eq!(result.unwrap(), rows, "discard incomplete tail at {cut}");
            } else if cut == tail.len() {
                assert_eq!(result.unwrap(), [rows.as_slice(), &rows[..1]].concat());
            } else {
                assert!(result.is_err(), "ambiguous partial payload at {cut}");
            }
        }
    }

    #[test]
    fn wal_complete_frame_bit_mutations_never_return_a_prefix() {
        let rows = make_test_rows(1);
        let original = frame(&rows);
        for index in 0..original.len() {
            for bit in 0..8 {
                let mut corrupted = original.clone();
                corrupted[index] ^= 1 << bit;
                let bytes = [original.as_slice(), &corrupted, original.as_slice()].concat();
                assert!(
                    read_entries(&mut bytes.as_slice(), bytes.len() as u64).is_err(),
                    "accepted corruption at {index}, bit {bit}"
                );
            }
        }
    }

    #[test]
    fn wal_lengths_are_bounded_before_allocation() {
        let mut header = 1u32.to_le_bytes().to_vec();
        header.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(read_entries(&mut header.as_slice(), 8).is_err());

        let mut rows = make_test_rows(1);
        rows[0].topic0 = None;
        let mut payload = encode_rows(&rows).unwrap();
        assert!(decode_rows(&payload, usize::MAX).is_err());
        // Matching lengths still cannot allocate beyond the payload's bounds.
        payload[8 + 109..8 + 113].copy_from_slice(&u32::MAX.to_le_bytes());
        payload[8 + 113..8 + 117].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_rows(&payload, 1).is_err());
    }

    #[test]
    fn wal_payload_validation_covers_legacy_and_binary_shapes() {
        let mut rows = make_test_rows(1);
        let payload = encode_rows(&rows).unwrap();
        for cut in 0..payload.len() {
            assert!(decode_rows(&payload[..cut], 1).is_err(), "cut {cut}");
        }
        let mut bad_version = payload.clone();
        bad_version[4..8].copy_from_slice(&2u32.to_le_bytes());
        assert!(decode_rows(&bad_version, 1).is_err());
        let mut bad_source = payload.clone();
        *bad_source.last_mut().unwrap() = 2;
        assert!(decode_rows(&bad_source, 1).is_err());
        let mut trailing = payload;
        trailing.push(0);
        assert!(decode_rows(&trailing, 1).is_err());
        rows[0].data_len += 1;
        assert!(decode_rows(&serde_json::to_vec(&rows).unwrap(), 1).is_err());

        // Preserve every existing optional-topic pattern and both source tags.
        for mask in 0..16 {
            rows[0].topic0 = (mask & 1 != 0).then_some(B256::repeat_byte(1));
            rows[0].topic1 = (mask & 2 != 0).then_some(B256::repeat_byte(2));
            rows[0].topic2 = (mask & 4 != 0).then_some(B256::repeat_byte(3));
            rows[0].topic3 = (mask & 8 != 0).then_some(B256::repeat_byte(4));
            for source in [Source::Receipt, Source::Trace] {
                rows[0].source = source;
                for data in [Bytes::new(), bytes!("123456")] {
                    rows[0].data_len = data.len() as u32;
                    rows[0].data = data;
                    assert_eq!(decode_rows(&encode_rows(&rows).unwrap(), 1).unwrap(), rows);
                    assert_eq!(
                        decode_rows(&serde_json::to_vec(&rows).unwrap(), 1).unwrap(),
                        rows
                    );
                }
            }
        }
        assert!(
            decode_rows(&encode_rows(&[]).unwrap(), 0)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn wal_partial_checksum_requires_valid_payload_and_matching_prefix() {
        let original = frame(&make_test_rows(1));
        for crc_len in 1..4 {
            let mut partial = original[..original.len() - 4 + crc_len].to_vec();
            *partial.last_mut().unwrap() ^= 1;
            assert!(read_entries(&mut partial.as_slice(), partial.len() as u64).is_err());
        }
        let mut partial = original[..original.len() - 4].to_vec();
        // Complete payload, missing checksum, invalid binary version.
        partial[12] = 2;
        assert!(read_entries(&mut partial.as_slice(), partial.len() as u64).is_err());
    }

    #[test]
    fn wal_read_errors_and_length_changes_are_not_treated_as_incomplete_tails() {
        struct FailAfter<'a> {
            data: &'a [u8],
        }
        impl Read for FailAfter<'_> {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.data.is_empty() {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "injected read failure",
                    ))
                } else {
                    self.data.read(buf)
                }
            }
        }
        let bytes = frame(&make_test_rows(1));
        for cut in 0..=bytes.len() {
            let error = read_entries(
                &mut FailAfter {
                    data: &bytes[..cut],
                },
                bytes.len() as u64,
            )
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::PermissionDenied, "cut {cut}");
        }
        assert!(read_entries(&mut bytes.as_slice(), bytes.len() as u64 - 1).is_err());
        assert!(read_entries(&mut bytes.as_slice(), bytes.len() as u64 + 1).is_err());
    }

    #[test]
    fn wal_invalid_append_does_not_create_a_file() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("absent.wal");
        let mut wal = WriteAheadLog::open(path.clone()).unwrap();
        let mut rows = make_test_rows(1);
        rows[0].data_len += 1;
        assert!(wal.append(&rows).is_err());
        assert!(!path.exists());
        wal.truncate().unwrap();
        assert!(!path.exists());
        fs::create_dir(&path).unwrap();
        assert!(wal.read_all().is_err());
        assert!(wal.truncate().is_err());
    }

    #[test]
    #[ignore = "explicit release WAL benchmark; see docs/audit/wal-recovery.md"]
    fn wal_release_baseline() {
        use std::hint::black_box;
        use std::time::Instant;

        let codec_iterations = std::env::var("LOGEX_WAL_CODEC_ITERATIONS")
            .map(|value| {
                value
                    .parse::<u32>()
                    .expect("positive codec iteration count")
            })
            .unwrap_or(10);
        assert!(codec_iterations > 0);
        let mut rows = make_test_rows(4096);
        for (index, row) in rows.iter_mut().enumerate() {
            row.topic1 = (index % 2 == 0).then_some(B256::repeat_byte(2));
            row.topic2 = (index % 3 == 0).then_some(B256::repeat_byte(3));
            row.topic3 = (index % 5 == 0).then_some(B256::repeat_byte(4));
            row.data = Bytes::from(vec![index as u8; (index % 4) * 128]);
            row.data_len = row.data.len() as u32;
        }
        let payload = encode_rows(&rows).unwrap();
        assert_eq!(decode_rows(&payload, rows.len()).unwrap(), rows);
        let tmp = TempDir::new().unwrap();
        let mut wal = WriteAheadLog::open(tmp.path().join("bench.wal")).unwrap();
        wal.append(&rows).unwrap();
        assert_eq!(wal.read_all().unwrap(), rows);
        println!(
            "{}",
            serde_json::json!({
                "kind": "config", "fixture": "wal-v1", "rows": rows.len(),
                "payload_bytes": payload.len(), "payload_crc32": crc32fast::hash(&payload),
                "samples": 15, "codec_iterations": codec_iterations, "io_iterations": 1
            })
        );
        for sample in 0..15 {
            let start = Instant::now();
            for _ in 0..codec_iterations {
                black_box(encode_rows(black_box(&rows)).unwrap());
            }
            let encode_ms = start.elapsed().as_secs_f64() * 1000.0 / f64::from(codec_iterations);
            let start = Instant::now();
            for _ in 0..codec_iterations {
                black_box(decode_rows(black_box(&payload), rows.len()).unwrap());
            }
            let decode_ms = start.elapsed().as_secs_f64() * 1000.0 / f64::from(codec_iterations);
            wal.truncate().unwrap();
            let start = Instant::now();
            wal.append(black_box(&rows)).unwrap();
            let append_ms = start.elapsed().as_secs_f64() * 1000.0;
            let start = Instant::now();
            let recovered = wal.read_all().unwrap();
            let read_ms = start.elapsed().as_secs_f64() * 1000.0;
            assert_eq!(recovered, rows);
            println!(
                "{}",
                serde_json::json!({
                    "kind": "sample", "sample": sample, "encode_ms": encode_ms,
                    "decode_ms": decode_ms, "append_ms": append_ms, "read_ms": read_ms
                })
            );
        }
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
    fn test_wal_garbage_tail_requires_inspection() {
        let tmp = TempDir::new().unwrap();
        let wal_path = tmp.path().join("test.wal");

        // Write valid entries
        let mut wal = WriteAheadLog::open(wal_path.clone()).unwrap();
        wal.append(&make_test_rows(5)).unwrap();

        // Append garbage
        let mut file = OpenOptions::new().append(true).open(&wal_path).unwrap();
        file.write_all(b"garbage data that is not valid").unwrap();

        // An unchecked length in the garbage could hide complete later entries.
        let wal = WriteAheadLog::open(wal_path).unwrap();
        assert!(wal.read_all().is_err());
    }
}
