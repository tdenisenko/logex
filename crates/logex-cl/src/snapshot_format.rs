use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};

use sha2::{Digest, Sha256};

use crate::ConsensusSnapshot;

const MAGIC: &[u8; 8] = b"LXCLSN01";
const HEADER_LEN: usize = 8 + 8 + 32;

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn json_error(error: serde_json::Error) -> io::Error {
    io::Error::new(
        error.io_error_kind().unwrap_or(io::ErrorKind::InvalidData),
        error,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Integrity {
    pub(super) encoded_len: u64,
    pub(super) payload_digest: [u8; 32],
}

pub(super) fn read_with_integrity(file: File) -> io::Result<(ConsensusSnapshot, Integrity)> {
    let length = file.metadata()?.len();
    read_from_with_integrity(file, length)
}

#[cfg(test)]
fn read_from(source: impl Read, file_length: u64) -> io::Result<ConsensusSnapshot> {
    read_from_with_integrity(source, file_length).map(|(snapshot, _)| snapshot)
}

fn read_from_with_integrity(
    mut source: impl Read,
    file_length: u64,
) -> io::Result<(ConsensusSnapshot, Integrity)> {
    if file_length < HEADER_LEN as u64 {
        return Err(invalid("consensus snapshot header is truncated"));
    }
    let mut header = [0; HEADER_LEN];
    source.read_exact(&mut header)?;
    if &header[..8] != MAGIC {
        return Err(invalid("unsupported or damaged consensus snapshot format"));
    }
    let mut length_bytes = [0; 8];
    length_bytes.copy_from_slice(&header[8..16]);
    let payload_length = u64::from_le_bytes(length_bytes);
    if file_length.checked_sub(HEADER_LEN as u64) != Some(payload_length) {
        return Err(invalid(
            "consensus snapshot length does not match its header",
        ));
    }

    // Buffer outside the hasher so serde's byte-sized reads hash whole chunks.
    // Take prevents the checksum from including bytes beyond the declared body.
    let mut reader = BufReader::new(HashReader {
        inner: source.take(payload_length),
        hash: Sha256::new(),
    });
    let snapshot = serde_json::from_reader(&mut reader).map_err(json_error)?;
    let payload = reader.into_inner();
    if payload.inner.limit() != 0 {
        return Err(invalid("consensus snapshot payload is truncated"));
    }
    if payload.hash.finalize()[..] != header[16..] {
        return Err(invalid("consensus snapshot checksum mismatch"));
    }
    // Also detect bytes appended after the opened file's initial length check.
    let mut source = payload.inner.into_inner();
    let mut trailing = [0];
    loop {
        match source.read(&mut trailing) {
            Ok(0) => {
                return Ok((
                    snapshot,
                    Integrity {
                        encoded_len: file_length,
                        payload_digest: header[16..48].try_into().expect("fixed checksum width"),
                    },
                ));
            }
            Ok(_) => return Err(invalid("consensus snapshot has trailing bytes")),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        }
    }
}

/// Write to a new, empty staging file. The caller owns synchronization and
/// atomic publication; no incomplete header is ever a published snapshot.
#[cfg(test)]
pub(super) fn write(target: impl Write + Seek, snapshot: &ConsensusSnapshot) -> io::Result<()> {
    write_with_integrity(target, snapshot).map(|_| ())
}

pub(super) fn write_with_integrity(
    mut target: impl Write + Seek,
    snapshot: &ConsensusSnapshot,
) -> io::Result<Integrity> {
    target.write_all(&[0; HEADER_LEN])?;
    let (length, checksum) = {
        let mut writer = BufWriter::new(HashWriter {
            inner: &mut target,
            hash: Sha256::new(),
            length: 0,
        });
        serde_json::to_writer_pretty(&mut writer, snapshot).map_err(json_error)?;
        writer.flush()?;
        let payload = writer.into_inner().map_err(|error| error.into_error())?;
        (payload.length, payload.hash.finalize())
    };
    let mut header = [0; HEADER_LEN];
    header[..8].copy_from_slice(MAGIC);
    header[8..16].copy_from_slice(&length.to_le_bytes());
    header[16..].copy_from_slice(&checksum);
    target.seek(SeekFrom::Start(0))?;
    target.write_all(&header)?;
    target.flush()?;
    Ok(Integrity {
        encoded_len: length
            .checked_add(HEADER_LEN as u64)
            .ok_or_else(|| invalid("consensus snapshot total length exceeds u64"))?,
        payload_digest: checksum.into(),
    })
}

struct HashReader<R> {
    inner: R,
    hash: Sha256,
}

impl<R: Read> Read for HashReader<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(bytes)?;
        self.hash.update(&bytes[..count]);
        Ok(count)
    }
}

struct HashWriter<W> {
    inner: W,
    hash: Sha256,
    length: u64,
}

impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = self.inner.write(bytes)?;
        self.length = self
            .length
            .checked_add(
                u64::try_from(count)
                    .map_err(|_| invalid("consensus snapshot write length exceeds u64"))?,
            )
            .ok_or_else(|| invalid("consensus snapshot write length exceeds u64"))?;
        self.hash.update(&bytes[..count]);
        Ok(count)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use logex_types::{ChainAnchors, ConsensusLightClientStatus, WeakSubjectivityCheckpoint};

    fn sample() -> ConsensusSnapshot {
        ConsensusSnapshot {
            checkpoint: WeakSubjectivityCheckpoint {
                beacon_root: B256::repeat_byte(1),
                beacon_slot: None,
            },
            anchors: ChainAnchors::default(),
            ordered_anchors: Vec::new(),
            anchor_gap_count: 0,
            light_client: ConsensusLightClientStatus::default(),
            light_client_payloads: Default::default(),
            verified_light_client_store: None,
        }
    }

    fn encoded() -> Vec<u8> {
        let mut output = io::Cursor::new(Vec::new());
        write(&mut output, &sample()).unwrap();
        output.into_inner()
    }

    #[test]
    fn snapshot_integrity_envelope_matches_payload_and_whitespace() {
        let bytes = encoded();
        let payload = serde_json::to_vec_pretty(&sample()).unwrap();
        assert_eq!(&bytes[..8], b"LXCLSN01");
        assert_eq!(&bytes[8..16], &(payload.len() as u64).to_le_bytes());
        assert_eq!(bytes[16..48], Sha256::digest(&payload)[..]);
        assert_eq!(&bytes[48..], payload);
        assert_eq!(
            read_from(bytes.as_slice(), bytes.len() as u64).unwrap(),
            sample()
        );

        let mut with_space = bytes;
        with_space.extend_from_slice(b"\n \t");
        let length = (with_space.len() - 48) as u64;
        with_space[8..16].copy_from_slice(&length.to_le_bytes());
        assert!(
            read_from(with_space.as_slice(), with_space.len() as u64)
                .unwrap_err()
                .to_string()
                .contains("checksum")
        );
        let checksum = Sha256::digest(&with_space[48..]);
        with_space[16..48].copy_from_slice(&checksum);
        assert_eq!(
            read_from(with_space.as_slice(), with_space.len() as u64).unwrap(),
            sample()
        );
    }

    #[test]
    fn snapshot_integrity_rejects_incomplete_or_inconsistent_envelopes() {
        let original = encoded();
        for length in 0..48 {
            assert!(read_from(&original[..length], length as u64).is_err());
        }
        for offset in [0, 7, 8, 16, 47] {
            let mut bytes = original.clone();
            bytes[offset] ^= 1;
            assert!(read_from(bytes.as_slice(), bytes.len() as u64).is_err());
        }
        let mut oversized = original.clone();
        oversized[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(read_from(oversized.as_slice(), oversized.len() as u64).is_err());
        let shortened = &original[..original.len() - 1];
        assert!(read_from(shortened, shortened.len() as u64).is_err());

        let mut extra = original.clone();
        extra.push(b' ');
        assert!(read_from(extra.as_slice(), extra.len() as u64).is_err());
        assert!(
            read_from(extra.as_slice(), original.len() as u64)
                .unwrap_err()
                .to_string()
                .contains("trailing")
        );

        // Model truncation after the initial metadata read: the JSON itself is
        // complete, but eight promised bytes are missing from the opened stream.
        let mut short_after_metadata = original.clone();
        short_after_metadata[8..16]
            .copy_from_slice(&((original.len() - 48 + 8) as u64).to_le_bytes());
        assert!(
            read_from(short_after_metadata.as_slice(), (original.len() + 8) as u64)
                .unwrap_err()
                .to_string()
                .contains("truncated")
        );
    }

    struct PartialIo {
        cursor: io::Cursor<Vec<u8>>,
        remaining_writes: Option<usize>,
        fail_seek: bool,
    }

    impl Read for PartialIo {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            let length = bytes.len().min(5);
            self.cursor.read(&mut bytes[..length])
        }
    }

    impl Write for PartialIo {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.remaining_writes == Some(0) {
                return Err(io::Error::other("simulated staging write failure"));
            }
            let length = bytes
                .len()
                .min(3)
                .min(self.remaining_writes.unwrap_or(usize::MAX));
            let written = self.cursor.write(&bytes[..length])?;
            if let Some(remaining) = &mut self.remaining_writes {
                *remaining -= written;
            }
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl Seek for PartialIo {
        fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
            if self.fail_seek {
                return Err(io::Error::other("simulated staging seek failure"));
            }
            self.cursor.seek(position)
        }
    }

    #[test]
    fn snapshot_integrity_short_io_preserves_length_and_checksum() {
        let mut output = PartialIo {
            cursor: io::Cursor::new(Vec::new()),
            remaining_writes: None,
            fail_seek: false,
        };
        write(&mut output, &sample()).unwrap();
        assert_eq!(output.cursor.get_ref(), &encoded());
        let length = output.cursor.get_ref().len() as u64;
        output.cursor.set_position(0);
        assert_eq!(read_from(output, length).unwrap(), sample());
    }

    #[test]
    fn snapshot_integrity_failed_staging_never_has_a_complete_header() {
        for (remaining_writes, fail_seek) in [(Some(48 + 32), false), (None, true)] {
            let mut output = PartialIo {
                cursor: io::Cursor::new(Vec::new()),
                remaining_writes,
                fail_seek,
            };
            let error = write(&mut output, &sample()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Other);
            let bytes = output.cursor.into_inner();
            assert_eq!(&bytes[..48], &[0; 48]);
            assert!(read_from(bytes.as_slice(), bytes.len() as u64).is_err());
        }
    }
}
