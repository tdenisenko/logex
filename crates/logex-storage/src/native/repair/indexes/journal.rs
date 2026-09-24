//! Bounded intent for replacing derived index trees while the catalog is fixed.
use std::{
    collections::BTreeSet,
    fs,
    io::{self, Read, Write},
};

use alloy_primitives::FixedBytes;
use serde::{Deserialize, Serialize};

use super::{JOURNAL_FILE, invalid};
use crate::native::{NativeStorageCatalog, StorageCatalogPaths};
use crate::{
    durability,
    index_checkpoint::{INDEX_CHECKPOINT_FILE, MAX_ARTIFACTS, validate_artifact_name},
};

const MAGIC: &[u8; 8] = b"LXIXRP01";
const HEADER: usize = 28;
const MAX_METADATA: usize = 1024 * 1024;
const MAX_CATALOG: usize = super::super::journal::MAX_CATALOG;
pub(super) const MAX_BYTES: usize = HEADER + MAX_METADATA + MAX_CATALOG;
pub(super) const MAX_ENTRIES: usize = super::super::journal::MAX_ENTRIES;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    pub id: u64,
    pub had_indexes: bool,
    pub prepared: Option<FixedBytes<32>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Metadata {
    pub operation: FixedBytes<16>,
    pub artifacts: Vec<String>,
    pub entries: Vec<Entry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Journal {
    pub catalog: NativeStorageCatalog,
    pub metadata: Metadata,
}

impl Journal {
    fn validate(&self) -> io::Result<()> {
        self.catalog.validate()?;
        let metadata = &self.metadata;
        if metadata.operation == FixedBytes::ZERO
            || metadata.entries.is_empty()
            || metadata.entries.len() > MAX_ENTRIES
            || metadata.artifacts.is_empty()
            || metadata.artifacts.len() > MAX_ARTIFACTS
            || metadata
                .entries
                .windows(2)
                .any(|pair| pair[0].id >= pair[1].id)
            || metadata.artifacts.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(invalid("invalid index repair operation or selection"));
        }
        for name in &metadata.artifacts {
            validate_artifact_name(name)
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
            if name == INDEX_CHECKPOINT_FILE {
                return Err(invalid("index checkpoint is not a derived artifact"));
            }
        }
        let ids: BTreeSet<_> = self
            .catalog
            .segments
            .iter()
            .map(|segment| segment.id)
            .collect();
        if metadata
            .entries
            .iter()
            .any(|entry| !ids.contains(&entry.id))
        {
            return Err(invalid("index repair selects an unknown segment"));
        }
        Ok(())
    }

    pub fn encode(&self) -> io::Result<Vec<u8>> {
        self.validate()?;
        let mut metadata = LimitedBytes(Vec::new());
        serde_json::to_writer(&mut metadata, &self.metadata).map_err(io::Error::other)?;
        let catalog = self.catalog.encode()?;
        let total = checked_lengths(metadata.0.len(), catalog.len())?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(total).map_err(io::Error::other)?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&(metadata.0.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(catalog.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&[0; 4]);
        bytes.extend_from_slice(&metadata.0);
        bytes.extend_from_slice(&catalog);
        let crc = checksum(&bytes);
        bytes[24..28].copy_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < HEADER || bytes.len() > MAX_BYTES || &bytes[..8] != MAGIC {
            return Err(invalid("invalid index repair journal envelope"));
        }
        let metadata_len = usize::try_from(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
            .map_err(|_| invalid("index repair metadata length is not representable"))?;
        let catalog_len = usize::try_from(u64::from_le_bytes(bytes[16..24].try_into().unwrap()))
            .map_err(|_| invalid("index repair catalog length is not representable"))?;
        if checked_lengths(metadata_len, catalog_len)? != bytes.len()
            || checksum(bytes) != u32::from_le_bytes(bytes[24..28].try_into().unwrap())
        {
            return Err(invalid("index repair journal length or checksum mismatch"));
        }
        let split = HEADER + metadata_len;
        let journal = Self {
            metadata: serde_json::from_slice(&bytes[HEADER..split])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?,
            catalog: NativeStorageCatalog::decode(&bytes[split..])?,
        };
        journal.validate()?;
        Ok(journal)
    }

    pub fn load(paths: &StorageCatalogPaths) -> io::Result<Option<Self>> {
        let path = paths.root().join(JOURNAL_FILE);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
            Ok(metadata)
                if !metadata.file_type().is_file() || metadata.len() > MAX_BYTES as u64 =>
            {
                return Err(invalid("index repair journal file type or size is invalid"));
            }
            Ok(_) => {}
        }
        let mut bytes = Vec::new();
        fs::File::open(path)?
            .take(MAX_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        Self::decode(&bytes).map(Some)
    }

    pub fn persist(&self, paths: &StorageCatalogPaths) -> io::Result<()> {
        durability::write_bytes(&paths.root().join(JOURNAL_FILE), &self.encode()?)
    }
}

struct LimitedBytes(Vec<u8>);
impl Write for LimitedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_METADATA.saturating_sub(self.0.len()) {
            return Err(invalid("index repair metadata exceeds limit"));
        }
        self.0.try_reserve(bytes.len()).map_err(io::Error::other)?;
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn checked_lengths(metadata: usize, catalog: usize) -> io::Result<usize> {
    if metadata == 0 || metadata > MAX_METADATA || catalog == 0 || catalog > MAX_CATALOG {
        return Err(invalid("index repair journal component exceeds bounds"));
    }
    HEADER
        .checked_add(metadata)
        .and_then(|length| length.checked_add(catalog))
        .ok_or_else(|| invalid("index repair journal length overflows"))
}

fn checksum(bytes: &[u8]) -> u32 {
    let mut checksum = crc32fast::Hasher::new();
    checksum.update(&bytes[..24]);
    checksum.update(&bytes[HEADER..]);
    checksum.finalize()
}
