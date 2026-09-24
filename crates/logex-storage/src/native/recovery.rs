use std::fs::File;
use std::io::{self, Read};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use super::catalog::{SegmentDescriptor, SegmentKind, StorageCatalogPaths};
use crate::durability;
use crate::wal::EncodedWalBatch;

pub(super) struct MaintenanceWork {
    pub rows: Vec<logex_types::LogRow>,
    pub journal: Option<RecoveryJournal>,
}

/// Bound maintenance inputs before invoking the unchanged startup state machine.
/// Raw tails can be sized without requiring a complete current publication.
/// Compressed artifacts are sized at both catalog-pinned and published positions
/// before recovery can read either source representation.
pub(super) fn preflight_maintenance(
    paths: &StorageCatalogPaths,
    catalog: &super::catalog::NativeStorageCatalog,
    limits: super::inspection::NativeRecoveryLimits,
) -> io::Result<MaintenanceWork> {
    use std::fs;
    let root = paths.root();
    for name in [
        super::repair::journal::JOURNAL_FILE,
        super::repair::indexes::JOURNAL_FILE,
        "wal/ingestion.json",
    ] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(_) => {
                return Err(io::Error::new(
                    if name == "wal/ingestion.json" {
                        io::ErrorKind::Unsupported
                    } else {
                        io::ErrorKind::WouldBlock
                    },
                    format!(
                        "recovery blocked by {name}; preserve evidence and use its explicit recovery workflow (legacy ingestion journals have no supported decoder)"
                    ),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    super::storage::verify_recent_headers(&catalog.state)?;
    if let Some(intent) = &catalog.state.canonical_reorg {
        intent.validate(&catalog.state)?;
    }
    // Check the directory and evidence path shapes again instead of trusting
    // mutable report fields or following an unsupported recovery artifact.
    super::inspection::recovery_prerequisites(paths, catalog)?;
    let journal_path = RecoveryJournal::path(paths);
    match fs::symlink_metadata(&journal_path) {
        Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "recovery journal must be an ordinary file",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let journal = RecoveryJournal::load(paths)?;
    let wal =
        crate::WriteAheadLog::read_existing_bounded(&root.join("wal/pending.wal"), limits.wal)?;
    let wal_rows = wal.len() as u64;
    let wal_payload = wal.iter().try_fold(0u64, |bytes, row| {
        bytes
            .checked_add(row.data.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "WAL payload size overflow"))
    })?;
    // Replay may encode each row in its own variable-byte page. Reserve its
    // two eight-byte offsets and four-byte framing as well as the actual data,
    // before a subsequent recovery/reorg read can inspect generated pages.
    let wal_payload = wal_rows
        .checked_mul(20)
        .and_then(|framing| framing.checked_add(wal_payload))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL decoded framing size overflow",
            )
        })?;
    maintenance_limit("replayed rows", wal_rows, limits.primary.max_segment_rows)?;
    maintenance_limit(
        "replayed payload bytes",
        wal_payload,
        limits.primary.max_decoded_payload_bytes,
    )?;
    if catalog.state.canonical_reorg.is_some() && !wal.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "pending WAL rows and reorg intent cannot be admitted together; preserve both artifacts for explicit recovery",
        ));
    }
    for descriptor in &catalog.segments {
        let dir = paths.segment_dir(descriptor.id);
        let result = (|| {
            maintenance_limit(
                "committed segment rows",
                descriptor.row_count,
                limits.primary.max_segment_rows,
            )?;
            // Conservative allowance for any segment receiving this WAL suffix,
            // including a later reorg's full hash/bitmap scan after replay.
            let possible_rows = descriptor.row_count.checked_add(wal_rows).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "recovery row count overflow")
            })?;
            maintenance_limit(
                "segment rows including WAL",
                possible_rows,
                limits.primary.max_segment_rows,
            )?;
            let mut artifacts = 0;
            size_raw_recovery_tree(
                &dir,
                &dir,
                &mut artifacts,
                limits.primary.max_retained_artifact_bytes,
                0,
            )?;
            let manifest =
                super::catalog::SegmentManifest::load(&paths.segment_manifest_path(descriptor.id))
                    .map_err(io::Error::from)?;
            if let Some(manifest) = &manifest {
                maintenance_limit(
                    "published segment rows",
                    manifest.row_count,
                    limits.primary.max_segment_rows,
                )?;
            }
            if descriptor.column_bundle.is_some() {
                // Startup restores this exact immutable table even when a later
                // manifest describes an interrupted append. Never infer its
                // column layout or canonical state from the advanced manifest.
                let columns = super::segment::current_compacted_columns();
                let pinned = super::segment::manifest_with_columns(descriptor, columns);
                preflight_compressed_reader(
                    crate::SegmentReader::open_for_inspection_manifest(&dir, pinned.clone())?,
                    limits.primary,
                    wal_payload,
                )?;
                if let Some(published) = manifest
                    && published != pinned
                {
                    preflight_compressed_reader(
                        crate::SegmentReader::open_for_inspection_manifest(&dir, published)?,
                        limits.primary,
                        wal_payload,
                    )?;
                }
                return Ok(());
            }
            if let Some(manifest) = &manifest
                && (manifest.column_bundle.is_some()
                    || manifest
                        .columns
                        .iter()
                        .any(|column| column.page_index_path.is_some()))
            {
                // Paged sources retain the raw publication fence. A matching
                // interrupted prefix rewrite can only be captured with the
                // existing catalog-bound recovery capability (never zero-row
                // initialization, which would create directories in preflight).
                match crate::SegmentReader::open_for_inspection(&dir) {
                    Ok(reader) => preflight_compressed_reader(reader, limits.primary, wal_payload)?,
                    Err(error) => {
                        let Some(namespace) = descriptor
                            .source_namespace
                            .filter(|_| descriptor.row_count != 0)
                        else {
                            return Err(error);
                        };
                        let owner = crate::column::begin_prefix_recovery(
                            &dir,
                            namespace.0,
                            descriptor.row_count,
                            descriptor.generation,
                            descriptor.id,
                            descriptor.kind,
                            descriptor.source_commitment,
                        )?;
                        let reader = crate::SegmentReader::open_recovering_prefix(&owner)?;
                        preflight_compressed_reader(reader, limits.primary, wal_payload)?;
                    }
                }
                return Ok(());
            }
            let payload = match fs::metadata(dir.join("data.col")) {
                Ok(metadata) => metadata.len(),
                Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
                Err(error) => return Err(error),
            };
            maintenance_limit(
                "raw payload artifact including framing and WAL",
                payload.checked_add(wal_payload).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "recovery payload size overflow")
                })?,
                limits.primary.max_decoded_payload_bytes,
            )?;
            if descriptor.row_count != 0 {
                for entry in fs::read_dir(&dir)? {
                    let entry = entry?;
                    if entry
                        .path()
                        .extension()
                        .is_some_and(|extension| extension == "col")
                    {
                        let mut header = [0; crate::column::ColumnFileHeader::SIZE];
                        File::open(entry.path())?.read_exact(&mut header)?;
                        let header = crate::column::ColumnFileHeader::read_from(&header)
                            .ok_or_else(|| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "invalid recovery column header",
                                )
                            })?;
                        maintenance_limit(
                            "physical raw column rows",
                            header.row_count,
                            limits.primary.max_segment_rows,
                        )?;
                        if header.compression != 0 {
                            return Err(io::Error::new(
                                io::ErrorKind::Unsupported,
                                "compressed raw columns require bounded recovery support",
                            ));
                        }
                    }
                }
            }
            Ok(())
        })();
        result.map_err(|error: io::Error| {
            io::Error::new(
                error.kind(),
                format!("preflight recovery segment {}: {error}", dir.display()),
            )
        })?;
    }
    Ok(MaintenanceWork { rows: wal, journal })
}

fn preflight_compressed_reader(
    mut reader: crate::SegmentReader,
    limits: super::inspection::InspectionLimits,
    wal_payload: u64,
) -> io::Result<()> {
    maintenance_limit(
        "compressed source rows",
        reader.read_row_count()?,
        limits.max_segment_rows,
    )?;
    reader.inspection_preflight(limits.max_retained_artifact_bytes, limits.max_decoded_payload_bytes - wal_payload).map_err(|error| match error {
        crate::segment_reader::InspectionPreflightError::Io(error) => error,
        crate::segment_reader::InspectionPreflightError::LimitExceeded { resource, required, limit } => io::Error::new(io::ErrorKind::InvalidInput, format!("recovery {resource} limit exceeded: required={required}, remaining allowance={limit}")),
    })
}

fn maintenance_limit(resource: &str, required: u64, limit: u64) -> io::Result<()> {
    if required > limit {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("recovery {resource} limit exceeded: required={required}, limit={limit}"),
        ));
    }
    Ok(())
}

fn size_raw_recovery_tree(
    root: &std::path::Path,
    path: &std::path::Path,
    total: &mut u64,
    limit: u64,
    depth: usize,
) -> io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || (!metadata.is_file() && !metadata.is_dir()) {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            format!("recovery requires ordinary artifacts: {}", path.display()),
        ));
    }
    if metadata.is_file() {
        *total = total.checked_add(metadata.len()).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "recovery artifact size overflow",
            )
        })?;
        return maintenance_limit("source artifact bytes", *total, limit);
    }
    if depth > 8 {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "unexpected nesting in recovery source artifacts",
        ));
    }
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        if path == root && entry.file_name() == "indexes" {
            continue;
        }
        size_raw_recovery_tree(root, &entry.path(), total, limit, depth + 1)?;
    }
    Ok(())
}

const JOURNAL_VERSION: u32 = 1;
const CHECKPOINT_JOURNAL_VERSION: u32 = 2;
pub(super) const MAX_JOURNAL_BYTES: u64 = 16 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum IngestRoute {
    Live,
    Historical,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckpointState {
    route: IngestRoute,
    complete: bool,
}

/// A WAL batch or checkpoint origin, ordered before the WAL or segments change.
/// Existing segments between `start.id` and `next_segment_id` can be historical
/// and must never be counted as part of the batch's newly allocated segments.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryJournal {
    version: u32,
    pub(super) start: SegmentDescriptor,
    pub(super) next_segment_id: u64,
    pub(super) row_count: u32,
    pub(super) payload_checksum: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    checkpoint: Option<CheckpointState>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CheckedJournal {
    journal: RecoveryJournal,
    checksum: u32,
}

impl RecoveryJournal {
    pub(super) fn new(
        start: SegmentDescriptor,
        next_segment_id: u64,
        batch: &EncodedWalBatch,
    ) -> io::Result<Self> {
        let journal = Self {
            version: JOURNAL_VERSION,
            start,
            next_segment_id,
            row_count: batch.row_count,
            payload_checksum: batch.checksum,
            checkpoint: None,
        };
        journal.validate()?;
        Ok(journal)
    }

    fn validate(&self) -> io::Result<()> {
        crate::commitment::validate_state(
            self.start.source_namespace,
            self.start.row_count,
            self.start.source_commitment,
            self.start.source_state.as_ref(),
        )?;
        let valid_shape = match (self.version, self.checkpoint) {
            (JOURNAL_VERSION, None) => self.start.kind == SegmentKind::Hot && self.row_count > 0,
            (CHECKPOINT_JOURNAL_VERSION, Some(checkpoint)) => {
                (checkpoint.route == IngestRoute::Historical || self.start.kind == SegmentKind::Hot)
                    && if checkpoint.complete {
                        self.row_count > 0
                    } else {
                        self.row_count == 0 && self.payload_checksum == 0
                    }
            }
            _ => false,
        };
        if !valid_shape || self.start.id >= self.next_segment_id {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid or unsupported WAL recovery journal",
            ));
        }
        Ok(())
    }

    pub(super) fn new_checkpoint(
        start: SegmentDescriptor,
        next_segment_id: u64,
        route: IngestRoute,
    ) -> io::Result<Self> {
        let journal = Self {
            version: CHECKPOINT_JOURNAL_VERSION,
            start,
            next_segment_id,
            row_count: 0,
            payload_checksum: 0,
            checkpoint: Some(CheckpointState {
                route,
                complete: false,
            }),
        };
        journal.validate()?;
        Ok(journal)
    }

    pub(super) fn is_active_checkpoint(&self) -> bool {
        self.checkpoint
            .is_some_and(|checkpoint| !checkpoint.complete)
    }

    pub(super) fn is_complete_checkpoint(&self) -> bool {
        self.checkpoint
            .is_some_and(|checkpoint| checkpoint.complete)
    }

    pub(super) fn route(&self) -> IngestRoute {
        self.checkpoint
            .map_or(IngestRoute::Live, |checkpoint| checkpoint.route)
    }

    pub(super) fn complete_checkpoint(&mut self, row_count: u32, checksum: u32) -> io::Result<()> {
        let checkpoint = self
            .checkpoint
            .as_mut()
            .ok_or_else(|| io::Error::other("not a checkpoint journal"))?;
        checkpoint.complete = true;
        self.row_count = row_count;
        self.payload_checksum = checksum;
        self.validate()
    }

    pub(super) fn path(paths: &StorageCatalogPaths) -> PathBuf {
        paths.root().join("wal/recovery.json")
    }

    pub(super) fn persist(&self, paths: &StorageCatalogPaths) -> io::Result<()> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(io::Error::other)?;
        let checked = CheckedJournal {
            journal: self.clone(),
            checksum: crc32fast::hash(&bytes),
        };
        let bytes = serde_json::to_vec(&checked).map_err(io::Error::other)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "WAL recovery journal exceeds size limit",
            ));
        }
        durability::write_bytes_ordered(&Self::path(paths), &bytes)
    }

    pub(super) fn load(paths: &StorageCatalogPaths) -> io::Result<Option<Self>> {
        let path = Self::path(paths);
        let file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        let mut bytes = Vec::new();
        file.take(MAX_JOURNAL_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_JOURNAL_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL recovery journal exceeds size limit",
            ));
        }
        let checked: CheckedJournal = serde_json::from_slice(&bytes).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid recovery journal {}: {error}", path.display()),
            )
        })?;
        let payload = serde_json::to_vec(&checked.journal).map_err(io::Error::other)?;
        if crc32fast::hash(&payload) != checked.checksum {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "WAL recovery journal checksum mismatch",
            ));
        }
        checked.journal.validate()?;
        Ok(Some(checked.journal))
    }

    pub(super) fn remove(paths: &StorageCatalogPaths) -> io::Result<()> {
        durability::remove_file(&Self::path(paths))
    }
}

#[cfg(test)]
mod tests {
    use super::super::catalog::{NativeStorageCatalog, NativeStorageConfig};
    use super::*;
    use crate::native::storage::NativeStorage;

    #[test]
    fn valid_checksum_does_not_bypass_journal_version_or_position_checks() {
        let dir = tempfile::tempdir().unwrap();
        let config = NativeStorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..NativeStorageConfig::default()
        };
        let storage = NativeStorage::open(config.clone()).unwrap();
        drop(storage);
        let (catalog, paths) = NativeStorageCatalog::open_or_create(&config).unwrap();
        let valid = RecoveryJournal {
            version: JOURNAL_VERSION,
            start: catalog.active_hot_segment().unwrap().clone(),
            next_segment_id: catalog.next_segment_id,
            row_count: 3,
            payload_checksum: 123,
            checkpoint: None,
        };
        for damage in 0..4 {
            let mut journal = valid.clone();
            match damage {
                0 => journal.version += 1,
                1 => journal.start.kind = SegmentKind::Sealed,
                2 => journal.next_segment_id = journal.start.id,
                3 => journal.row_count = 0,
                _ => unreachable!(),
            }
            let checksum = crc32fast::hash(&serde_json::to_vec(&journal).unwrap());
            let bytes = serde_json::to_vec(&CheckedJournal { journal, checksum }).unwrap();
            std::fs::write(RecoveryJournal::path(&paths), &bytes).unwrap();
            assert!(RecoveryJournal::load(&paths).is_err(), "damage {damage}");
            assert_eq!(std::fs::read(RecoveryJournal::path(&paths)).unwrap(), bytes);
        }
    }

    #[test]
    fn checkpoint_shape_is_validated_even_with_a_matching_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let config = NativeStorageConfig {
            data_dir: dir.path().to_path_buf(),
            ..NativeStorageConfig::default()
        };
        let storage = NativeStorage::open(config.clone()).unwrap();
        drop(storage);
        let (catalog, paths) = NativeStorageCatalog::open_or_create(&config).unwrap();
        let active = RecoveryJournal::new_checkpoint(
            catalog.active_hot_segment().unwrap().clone(),
            catalog.next_segment_id,
            IngestRoute::Live,
        )
        .unwrap();
        active.persist(&paths).unwrap();
        assert!(
            RecoveryJournal::load(&paths)
                .unwrap()
                .unwrap()
                .is_active_checkpoint()
        );
        for damage in 0..7 {
            let mut journal = active.clone();
            match damage {
                0 => journal.version = JOURNAL_VERSION,
                1 => journal.checkpoint = None,
                2 => journal.row_count = 1,
                3 => journal.payload_checksum = 1,
                4 => journal.checkpoint.as_mut().unwrap().complete = true,
                5 => journal.start.kind = SegmentKind::Sealed,
                6 => journal.next_segment_id = journal.start.id,
                _ => unreachable!(),
            }
            let checksum = crc32fast::hash(&serde_json::to_vec(&journal).unwrap());
            let bytes = serde_json::to_vec(&CheckedJournal { journal, checksum }).unwrap();
            std::fs::write(RecoveryJournal::path(&paths), &bytes).unwrap();
            assert!(RecoveryJournal::load(&paths).is_err(), "damage {damage}");
            assert_eq!(std::fs::read(RecoveryJournal::path(&paths)).unwrap(), bytes);
        }
        let mut historical = active;
        historical.start.kind = SegmentKind::Sealed;
        historical.checkpoint.as_mut().unwrap().route = IngestRoute::Historical;
        historical.complete_checkpoint(3, 123).unwrap();
        historical.persist(&paths).unwrap();
        let recovered = RecoveryJournal::load(&paths).unwrap().unwrap();
        assert_eq!(recovered.route(), IngestRoute::Historical);
        assert!(!recovered.is_active_checkpoint());
    }
}
