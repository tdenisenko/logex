//! Bounded repair intent encoding. Valid structure is not publication authority.
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read, Write},
    path::PathBuf,
};

use super::super::{
    catalog::{NativeStorageCatalog, SegmentManifest, StorageCatalogPaths},
    segment,
};
use super::overlap;
use alloy_primitives::FixedBytes;
use serde::{Deserialize, Serialize};

pub(in crate::native) const JOURNAL_FILE: &str = "repair.journal";
const MAGIC: &[u8; 8] = b"LXRPJR01";
const HEADER: usize = 36;
pub(super) const MAX_CATALOG: usize = 64 * 1024 * 1024;
const MAX_METADATA: usize = 8 * 1024 * 1024;
pub(super) const MAX_BYTES: usize = 2 * MAX_CATALOG + MAX_METADATA;
pub(super) const MAX_ENTRIES: usize = 4096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RepairJournal {
    pub operation: FixedBytes<16>,
    pub before: NativeStorageCatalog,
    pub after: Option<NativeStorageCatalog>,
    pub seeds: Vec<u64>,
    pub entries: Vec<RepairEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RepairEntry {
    pub original_id: u64,
    pub replacement_id: u64,
    pub canonical_digest: FixedBytes<32>,
    pub staged_manifest: Option<SegmentManifest>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Metadata {
    operation: FixedBytes<16>,
    seeds: Vec<u64>,
    entries: Vec<RepairEntry>,
}

#[derive(Serialize)]
struct MetadataRef<'a> {
    operation: FixedBytes<16>,
    seeds: &'a [u64],
    entries: &'a [RepairEntry],
}

fn invalid(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}

struct LimitedBytes(Vec<u8>);
impl Write for LimitedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > MAX_METADATA.saturating_sub(self.0.len()) {
            return Err(invalid("repair journal metadata exceeds limit"));
        }
        self.0.try_reserve(bytes.len()).map_err(io::Error::other)?;
        self.0.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl RepairJournal {
    pub fn validate(&self) -> io::Result<()> {
        if self.operation == FixedBytes::ZERO
            || self.seeds.is_empty()
            || self.seeds.len() > MAX_ENTRIES
            || self.entries.is_empty()
            || self.entries.len() > MAX_ENTRIES
        {
            return Err(invalid("invalid repair operation or selection count"));
        }
        // Catalog validation also checks active append-state and header windows.
        self.before.validate()?;
        let selected = overlap::select(&self.before, &self.seeds, MAX_ENTRIES, u64::MAX)?;
        if !selected
            .segment_ids
            .iter()
            .copied()
            .eq(self.entries.iter().map(|e| e.original_id))
        {
            return Err(invalid("repair entries differ from selected owners"));
        }
        let next_id = self
            .before
            .next_segment_id
            .checked_add(self.entries.len() as u64)
            .ok_or_else(|| invalid("repair replacement IDs overflow"))?;
        let mut mapping = BTreeMap::new();
        for (index, entry) in self.entries.iter().enumerate() {
            if entry.replacement_id
                != self
                    .before
                    .next_segment_id
                    .checked_add(index as u64)
                    .ok_or_else(|| invalid("repair replacement ID overflows"))?
            {
                return Err(invalid(
                    "repair replacement IDs are not the reserved sequence",
                ));
            }
            mapping.insert(entry.original_id, entry);
        }
        let Some(after) = &self.after else {
            if self
                .entries
                .iter()
                .any(|entry| entry.staged_manifest.is_some())
            {
                return Err(invalid("unprepared repair has staged manifests"));
            }
            return Ok(());
        };
        let mut expected = self.before.clone();
        expected.next_segment_id = next_id;
        for descriptor in &mut expected.segments {
            let Some(entry) = mapping.get(&descriptor.id) else {
                continue;
            };
            let manifest = entry
                .staged_manifest
                .as_ref()
                .ok_or_else(|| invalid("prepared repair lacks a staged manifest"))?;
            manifest.validate_read_bounds()?;
            let reference = manifest
                .column_bundle
                .as_ref()
                .ok_or_else(|| invalid("repair replacement is not bundled"))?;
            reference.end()?;
            if reference.row_count != descriptor.row_count {
                return Err(invalid("repair bundle row count differs"));
            }
            descriptor.id = entry.replacement_id;
            descriptor.generation = 0;
            descriptor.relative_path =
                PathBuf::from("segments").join(format!("s_{:016}", descriptor.id));
            descriptor.manifest_relative_path = descriptor.relative_path.join("segment.json");
            descriptor.column_bundle = Some(reference.clone());
            validate_columns(manifest)?;
            if *manifest != segment::manifest_with_columns(descriptor, manifest.columns.clone()) {
                return Err(invalid(
                    "repair staged manifest differs from original logical identity",
                ));
            }
        }
        for active in [
            &mut expected.active_hot_segment,
            &mut expected.active_historical_segment,
        ] {
            if let Some(entry) = active.and_then(|id| mapping.get(&id)) {
                *active = Some(entry.replacement_id);
            }
        }
        if *after != expected {
            return Err(invalid(
                "repair after catalog differs from exact replacement derivation",
            ));
        }
        after.validate()?;
        Ok(())
    }

    pub fn encode(&self) -> io::Result<Vec<u8>> {
        self.validate()?;
        let mut metadata = LimitedBytes(Vec::new());
        serde_json::to_writer(
            &mut metadata,
            &MetadataRef {
                operation: self.operation,
                seeds: &self.seeds,
                entries: &self.entries,
            },
        )
        .map_err(io::Error::other)?;
        let before = self.before.encode()?;
        let after = self
            .after
            .as_ref()
            .map(NativeStorageCatalog::encode)
            .transpose()?
            .unwrap_or_default();
        let total = checked_lengths(metadata.0.len(), before.len(), after.len())?;
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(total).map_err(io::Error::other)?;
        bytes.extend_from_slice(MAGIC);
        for length in [metadata.0.len(), before.len(), after.len()] {
            bytes.extend_from_slice(&(length as u64).to_le_bytes());
        }
        bytes.extend_from_slice(&[0; 4]);
        bytes.extend_from_slice(&metadata.0);
        bytes.extend_from_slice(&before);
        bytes.extend_from_slice(&after);
        let crc = checksum(&bytes);
        bytes[32..36].copy_from_slice(&crc.to_le_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() < HEADER || bytes.len() > MAX_BYTES || &bytes[..8] != MAGIC {
            return Err(invalid("invalid repair journal envelope"));
        }
        let mut lengths = [0usize; 3];
        for (index, length) in lengths.iter_mut().enumerate() {
            *length = usize::try_from(u64::from_le_bytes(
                bytes[8 + index * 8..16 + index * 8].try_into().unwrap(),
            ))
            .map_err(|_| invalid("repair journal length is not representable"))?;
        }
        let [metadata_len, before_len, after_len] = lengths;
        if checked_lengths(metadata_len, before_len, after_len)? != bytes.len()
            || checksum(bytes) != u32::from_le_bytes(bytes[32..36].try_into().unwrap())
        {
            return Err(invalid("repair journal length or checksum mismatch"));
        }
        let metadata_end = HEADER + metadata_len;
        let before_end = metadata_end + before_len;
        let metadata: Metadata =
            serde_json::from_slice(&bytes[HEADER..metadata_end]).map_err(io::Error::other)?;
        let journal = Self {
            operation: metadata.operation,
            seeds: metadata.seeds,
            entries: metadata.entries,
            before: NativeStorageCatalog::decode(&bytes[metadata_end..before_end])?,
            after: if after_len == 0 {
                None
            } else {
                Some(NativeStorageCatalog::decode(&bytes[before_end..])?)
            },
        };
        journal.validate()?;
        Ok(journal)
    }

    pub fn load(paths: &StorageCatalogPaths) -> io::Result<Option<Self>> {
        let path = paths.root().join(JOURNAL_FILE);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
            Ok(metadata) if !metadata.file_type().is_file() => {
                return Err(invalid("repair journal is not a regular file"));
            }
            Ok(_) => {}
        }
        let file = fs::File::open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() > MAX_BYTES as u64 {
            return Err(invalid("repair journal file type or size is invalid"));
        }
        let mut bytes = Vec::new();
        file.take(MAX_BYTES as u64 + 1).read_to_end(&mut bytes)?;
        Self::decode(&bytes).map(Some)
    }

    pub fn persist(&self, paths: &StorageCatalogPaths) -> io::Result<()> {
        crate::durability::write_bytes(&paths.root().join(JOURNAL_FILE), &self.encode()?)
    }
}

fn checked_lengths(metadata: usize, before: usize, after: usize) -> io::Result<usize> {
    if metadata == 0
        || metadata > MAX_METADATA
        || before == 0
        || before > MAX_CATALOG
        || after > MAX_CATALOG
    {
        return Err(invalid("repair journal component exceeds bounds"));
    }
    HEADER
        .checked_add(metadata)
        .and_then(|v| v.checked_add(before))
        .and_then(|v| v.checked_add(after))
        .filter(|&total| total <= MAX_BYTES)
        .ok_or_else(|| invalid("repair journal total exceeds bounds"))
}
fn checksum(bytes: &[u8]) -> u32 {
    let mut hash = crc32fast::Hasher::new();
    hash.update(&bytes[..32]);
    hash.update(&bytes[HEADER..]);
    hash.finalize()
}
fn validate_columns(manifest: &SegmentManifest) -> io::Result<()> {
    let profile = segment::current_column_profile();
    if manifest.columns.len() != profile.len() {
        return Err(invalid("repair column set differs"));
    }
    for (column, &(name, codec)) in manifest.columns.iter().zip(profile) {
        let nulls = name
            .starts_with("topic")
            .then(|| format!("columns/{name}.null"));
        if column.name != name
            || column.codec != codec
            || column.page_rows != crate::page::MAX_PAGE_ROWS
            || column.data_path != format!("columns/{name}.pages")
            || column.page_index_path.as_deref()
                != Some(format!("columns/{name}.pages.idx").as_str())
            || column.null_bitmap_path != nulls
        {
            return Err(invalid(
                "repair column identity differs from standalone encoding",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
