//! Disposable authenticated checkpoint headers. The key and replay counters never
//! leave memory. This is scratch, not recovery state: no fsync or restart replay.
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use alloy_consensus::Header;
use alloy_primitives::{B256, keccak256};
use alloy_rlp::{Decodable, Encodable};
use eyre::Result;
use logex_types::ExecutionAnchor;
use rand::RngCore;
use tokio_util::task::AbortOnDropHandle;

use crate::validation::{
    HeaderValidationError, validate_downloaded_headers, validate_header_matches_anchor,
};

pub(super) const MAX_SPOOL_CHUNK_HEADERS: usize = 1024;

const BUFFER_BYTES: usize = 64 * 1024;
const DOMAIN: &[u8] = b"logex/checkpoint-header-spool/v1/one-header";

#[derive(Debug)]
pub(super) enum AppendError {
    InvalidHeaders(HeaderValidationError),
    Storage(eyre::Report),
}

pub(super) struct HeaderSpoolWriter {
    file: BufWriter<File>,
    key: [u8; 32],
    count: u64,
    max_record_bytes: usize,
    previous: Header,
}

pub(super) struct SpoolChunk {
    pub(super) headers: Vec<Header>,
    pub(super) hashes: Vec<B256>,
}

/// Only `seal` can construct a reader, after the complete downloaded chain's
/// terminal header matches the consensus anchor. Frames remain authenticated
/// during replay, including their position and length, before RLP decoding.
pub(super) struct HeaderSpoolReader {
    file: BufReader<File>,
    key: [u8; 32],
    count: u64,
    index: u64,
    max_record_bytes: usize,
}

#[derive(Clone, Copy)]
pub(super) struct SpoolPosition {
    offset: u64,
    index: u64,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn record_tag(key: &[u8; 32], index: u64, bytes: &[u8]) -> blake3::Hash {
    let mut hash = blake3::Hasher::new_keyed(key);
    hash.update(DOMAIN);
    hash.update(&index.to_le_bytes());
    hash.update(&(bytes.len() as u64).to_le_bytes());
    hash.update(bytes);
    hash.finalize()
}

impl HeaderSpoolWriter {
    pub(super) async fn new(directory: PathBuf, previous: Header) -> Result<Self> {
        AbortOnDropHandle::new(tokio::task::spawn_blocking(move || {
            // Keep the supervised relative namespace unchanged. Never create a
            // parent or fall back to a system temporary directory. Unlink while
            // open so cancellation/process death cannot leave a replay artifact.
            let staged = logex_fs::StagedFile::new_in(&directory, ".checkpoint-headers-")?;
            let file = staged.as_file().try_clone()?;
            staged.discard()?;
            let mut key = [0; 32];
            rand::rngs::OsRng
                .try_fill_bytes(&mut key)
                .map_err(io::Error::other)?;
            Ok(Self {
                file: BufWriter::with_capacity(BUFFER_BYTES, file),
                key,
                count: 0,
                max_record_bytes: 0,
                previous,
            })
        }))
        .await?
    }

    pub(super) fn last_header(&self) -> &Header {
        &self.previous
    }

    pub(super) async fn append(
        mut self,
        headers: Vec<Header>,
    ) -> std::result::Result<Self, AppendError> {
        if headers.len() > MAX_SPOOL_CHUNK_HEADERS {
            return Err(AppendError::Storage(
                invalid("checkpoint header page exceeds bounded chunk").into(),
            ));
        }
        let expected = self.previous.number.checked_add(1).ok_or_else(|| {
            AppendError::Storage(invalid("checkpoint header number overflow").into())
        })?;
        validate_downloaded_headers(expected, Some(&self.previous), &headers)
            .map_err(AppendError::InvalidHeaders)?;
        AbortOnDropHandle::new(tokio::task::spawn_blocking(move || -> Result<Self> {
            for header in headers {
                let mut encoded = Vec::new();
                encoded
                    .try_reserve_exact(header.length())
                    .map_err(io::Error::other)?;
                header.encode(&mut encoded);
                self.file.write_all(&(encoded.len() as u64).to_le_bytes())?;
                self.file.write_all(&encoded)?;
                self.file
                    .write_all(record_tag(&self.key, self.count, &encoded).as_bytes())?;
                self.max_record_bytes = self.max_record_bytes.max(encoded.len());
                self.count = self
                    .count
                    .checked_add(1)
                    .ok_or_else(|| invalid("checkpoint header count overflow"))?;
                self.previous = header;
            }
            // Never return a writer whose Drop would flush on the async worker.
            self.file.flush()?;
            Ok(self)
        }))
        .await
        .map_err(|error| AppendError::Storage(error.into()))?
        .map_err(AppendError::Storage)
    }

    pub(super) fn validate_anchor(&self, anchor: &ExecutionAnchor) -> Result<()> {
        if self.count == 0 {
            return Err(invalid("empty checkpoint header spool").into());
        }
        validate_header_matches_anchor(anchor, &self.previous, self.previous.hash_slow())
            .map_err(|error| eyre::eyre!("checkpoint terminal header: {error}"))?;
        Ok(())
    }

    pub(super) async fn seal(mut self, anchor: ExecutionAnchor) -> Result<HeaderSpoolReader> {
        self.validate_anchor(&anchor)?;
        AbortOnDropHandle::new(tokio::task::spawn_blocking(
            move || -> Result<HeaderSpoolReader> {
                self.file.flush()?;
                let mut file = self.file.into_inner().map_err(|error| error.into_error())?;
                file.seek(SeekFrom::Start(0))?;
                Ok(HeaderSpoolReader {
                    file: BufReader::with_capacity(BUFFER_BYTES, file),
                    key: self.key,
                    count: self.count,
                    index: 0,
                    max_record_bytes: self.max_record_bytes,
                })
            },
        ))
        .await?
    }
}

impl HeaderSpoolReader {
    pub(super) fn remaining(&self) -> u64 {
        self.count - self.index
    }

    /// Return a bounded replay checkpoint along with the chunk. A declined
    /// parallel request can rewind just this candidate before sequential fallback.
    pub(super) async fn read_chunk(
        mut self,
        limit: usize,
    ) -> Result<(Self, SpoolPosition, SpoolChunk)> {
        AbortOnDropHandle::new(tokio::task::spawn_blocking(move || -> Result<_> {
            let position = SpoolPosition {
                offset: self.file.stream_position()?,
                index: self.index,
            };
            let rows = self.read_chunk_sync(limit)?;
            Ok((self, position, rows))
        }))
        .await?
    }

    fn read_chunk_sync(&mut self, limit: usize) -> io::Result<SpoolChunk> {
        if limit == 0 {
            return Err(invalid("zero checkpoint chunk size"));
        }
        let count = self
            .remaining()
            .min(limit.min(MAX_SPOOL_CHUNK_HEADERS) as u64) as usize;
        let mut headers = Vec::new();
        headers.try_reserve_exact(count).map_err(io::Error::other)?;
        let mut hashes = Vec::new();
        hashes.try_reserve_exact(count).map_err(io::Error::other)?;
        for _ in 0..count {
            let mut length = [0; 8];
            self.file.read_exact(&mut length)?;
            let length = usize::try_from(u64::from_le_bytes(length))
                .map_err(|_| invalid("checkpoint frame length overflow"))?;
            // Trusted bound collected during validated original encoding, not
            // from scratch contents. Never allocate an attacker-selected length.
            if length == 0 || length > self.max_record_bytes {
                return Err(invalid("checkpoint frame length out of bounds"));
            }
            let mut bytes = Vec::new();
            bytes.try_reserve_exact(length).map_err(io::Error::other)?;
            bytes.resize(length, 0);
            self.file.read_exact(&mut bytes)?;
            let mut tag = [0; 32];
            self.file.read_exact(&mut tag)?;
            if record_tag(&self.key, self.index, &bytes) != blake3::Hash::from(tag) {
                return Err(invalid("checkpoint frame authentication failed"));
            }
            let mut encoded = bytes.as_slice();
            let header = Header::decode(&mut encoded)
                .map_err(|_| invalid("invalid checkpoint header encoding"))?;
            if !encoded.is_empty() {
                return Err(invalid("trailing checkpoint header bytes"));
            }
            hashes.push(keccak256(&bytes));
            headers.push(header);
            self.index += 1; // index < count, whose checked writer count fits u64.
        }
        if self.remaining() == 0 {
            let mut extra = [0];
            if self.file.read(&mut extra)? != 0 {
                return Err(invalid("trailing checkpoint spool bytes"));
            }
        }
        Ok(SpoolChunk { headers, hashes })
    }

    pub(super) async fn rewind(mut self, position: SpoolPosition) -> Result<Self> {
        AbortOnDropHandle::new(tokio::task::spawn_blocking(move || -> Result<_> {
            if position.index > self.index || position.offset > self.file.stream_position()? {
                return Err(invalid("checkpoint rewind position is ahead of reader").into());
            }
            self.file.seek(SeekFrom::Start(position.offset))?;
            self.index = position.index;
            Ok(self)
        }))
        .await?
    }
}

#[cfg(test)]
mod tests;
