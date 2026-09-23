//! Exclusive, read-only inspection of catalog-selected primary data.
//!
//! This is not canonical-chain authentication or a replacement authorization.
//! Derived indexes and pending recovery transactions are deliberately not verified.
use std::{
    io,
    path::{Path, PathBuf},
};

use super::{
    catalog::{NativeStorageCatalog, SegmentDescriptor, SegmentKind, StorageCatalogPaths},
    directory_lock::DataDirectoryLock,
};
use crate::{SegmentReader, commitment::PrefixState};

/// Caller-selected per-segment work limits, independent of query admission.
#[derive(Debug, Clone, Copy)]
pub struct InspectionLimits {
    /// Maximum catalog-selected rows in any one segment.
    pub max_segment_rows: u64,
    /// Captured source artifact budget checked before whole-buffer reads.
    pub max_retained_artifact_bytes: u64,
    /// Payload decode-work allowance, including compressed-page framing.
    /// This is not a bound on total allocations or process RSS.
    pub max_decoded_payload_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InspectedSegmentRole {
    ActiveHot,
    ActiveHistorical,
    CompletedSealed,
}

#[derive(Debug)]
pub enum PrimaryDataDisposition {
    /// All logical rows matched the local published commitment. This does not
    /// authenticate canonical flags, chain membership or completeness of blocks.
    CommitmentVerified,
    /// Structure was checked, but a published logical identity was absent.
    Unbound,
    /// Recomputed logical content differs from the published local commitment.
    LogicalCommitmentMismatch,
    LimitExceeded {
        resource: &'static str,
        required: u64,
        limit: u64,
    },
    /// No corruption inference is made from an unavailable or failed check.
    Incomplete {
        stage: &'static str,
        path: PathBuf,
        kind: io::ErrorKind,
        message: String,
    },
    RecoveryRequired,
}

#[derive(Debug)]
pub struct SegmentInspection {
    pub id: u64,
    pub role: InspectedSegmentRole,
    pub disposition: PrimaryDataDisposition,
}

/// Holds exclusive ownership until dropped. Even a fully verified local scan
/// is not evidence authorizing replacement or deletion of stored data.
#[derive(Debug)]
pub struct PrimaryDataInspection {
    _owner: DataDirectoryLock,
    pub(super) paths: StorageCatalogPaths,
    pub catalog: NativeStorageCatalog,
    pub recovery_prerequisites: Vec<PathBuf>,
    pub segments: Vec<SegmentInspection>,
    /// Always false for this primary-data-only inspection.
    pub derived_indexes_inspected: bool,
}

fn contextual(stage: &str, path: &Path, error: io::Error) -> io::Error {
    io::Error::new(error.kind(), format!("{stage} {}: {error}", path.display()))
}

/// Inspect existing storage without creating directories, replaying WAL,
/// publishing metadata, rebuilding indexes or removing any artifacts.
///
/// Limits are checked before row-ID allocation and whole-buffer source reads.
/// They bound specified inputs, not total process RSS. Segments are scanned
/// serially; captured metadata and one payload batch can coexist.
pub fn inspect_primary_data(
    path: &Path,
    limits: InspectionLimits,
) -> io::Result<PrimaryDataInspection> {
    let owner = DataDirectoryLock::acquire_existing(path)
        .map_err(|error| contextual("acquire inspection ownership", path, error))?;
    inspect_owned(
        owner,
        StorageCatalogPaths::new(std::path::absolute(path)?),
        limits,
    )
}

pub(super) fn inspect_owned(
    owner: DataDirectoryLock,
    paths: StorageCatalogPaths,
    limits: InspectionLimits,
) -> io::Result<PrimaryDataInspection> {
    let catalog = NativeStorageCatalog::load_existing(&paths)
        .map_err(|error| contextual("read existing catalog", &paths.catalog_path(), error))?;
    super::storage::verify_recent_headers(&catalog.state).map_err(|error| {
        contextual("verify catalog header window", &paths.catalog_path(), error)
    })?;
    let recovery_prerequisites = recovery_prerequisites(&paths, &catalog)?;
    let segments = catalog
        .segments
        .iter()
        .map(|descriptor| {
            let role = if catalog.active_hot_segment == Some(descriptor.id) {
                InspectedSegmentRole::ActiveHot
            } else if catalog.active_historical_segment == Some(descriptor.id) {
                InspectedSegmentRole::ActiveHistorical
            } else {
                debug_assert_eq!(descriptor.kind, SegmentKind::Sealed);
                InspectedSegmentRole::CompletedSealed
            };
            let disposition = if recovery_prerequisites.is_empty() {
                inspect_segment(&paths.segment_dir(descriptor.id), descriptor, limits)
            } else {
                PrimaryDataDisposition::RecoveryRequired
            };
            SegmentInspection {
                id: descriptor.id,
                role,
                disposition,
            }
        })
        .collect();
    Ok(PrimaryDataInspection {
        _owner: owner,
        paths,
        catalog,
        recovery_prerequisites,
        segments,
        derived_indexes_inspected: false,
    })
}

pub(super) fn recovery_prerequisites(
    paths: &StorageCatalogPaths,
    catalog: &NativeStorageCatalog,
) -> io::Result<Vec<PathBuf>> {
    let path = paths.root();
    for (directory, optional) in [(path.join("segments"), false), (path.join("wal"), true)] {
        match std::fs::symlink_metadata(&directory) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(contextual(
                    "inspect storage directory",
                    &directory,
                    io::Error::new(
                        io::ErrorKind::Unsupported,
                        "inspection requires an ordinary directory",
                    ),
                ));
            }
            Err(error) if optional && error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(contextual("inspect storage directory", &directory, error)),
        }
    }
    let mut recovery_prerequisites = Vec::new();
    for (relative, empty_regular_allowed) in [
        (super::repair::journal::JOURNAL_FILE, false),
        ("wal/recovery.json", false),
        ("wal/ingestion.json", false),
        ("wal/pending.wal", true),
    ] {
        let artifact = path.join(relative);
        match std::fs::symlink_metadata(&artifact) {
            Ok(metadata)
                if empty_regular_allowed
                    && metadata.is_file()
                    && !metadata.file_type().is_symlink()
                    && metadata.len() == 0 => {}
            Ok(_) => recovery_prerequisites.push(artifact),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(contextual("inspect recovery evidence", &artifact, error)),
        }
    }
    if catalog.state.canonical_reorg.is_some() {
        recovery_prerequisites.push(paths.catalog_path());
    }
    Ok(recovery_prerequisites)
}

fn incomplete(stage: &'static str, path: &Path, error: io::Error) -> PrimaryDataDisposition {
    PrimaryDataDisposition::Incomplete {
        stage,
        path: path.to_owned(),
        kind: error.kind(),
        message: error.to_string(),
    }
}

pub(super) fn inspect_segment(
    path: &Path,
    descriptor: &SegmentDescriptor,
    limits: InspectionLimits,
) -> PrimaryDataDisposition {
    if descriptor.row_count > limits.max_segment_rows {
        return PrimaryDataDisposition::LimitExceeded {
            resource: "segment rows",
            required: descriptor.row_count,
            limit: limits.max_segment_rows,
        };
    }
    for (entry, directory) in [(path.to_owned(), true), (path.join("segment.json"), false)] {
        match std::fs::symlink_metadata(&entry) {
            Ok(metadata)
                if !metadata.file_type().is_symlink()
                    && if directory {
                        metadata.is_dir()
                    } else {
                        metadata.is_file()
                    } => {}
            Ok(_) => {
                return incomplete(
                    "inspect source file type",
                    &entry,
                    io::Error::new(
                        io::ErrorKind::Unsupported,
                        "inspection requires ordinary source directories and files",
                    ),
                );
            }
            Err(error) if !directory && error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return incomplete("inspect source file type", &entry, error),
        }
    }
    let mut reader = match SegmentReader::open_for_inspection(path) {
        Ok(reader) => reader,
        Err(error) => return incomplete("capture source", path, error),
    };
    if let Err(error) = verify_identity(&reader, descriptor) {
        return incomplete("compare captured source with catalog", path, error);
    }
    match reader.inspection_preflight(
        limits.max_retained_artifact_bytes,
        limits.max_decoded_payload_bytes,
    ) {
        Ok(()) => {}
        Err(crate::segment_reader::InspectionPreflightError::LimitExceeded {
            resource,
            required,
            limit,
        }) => {
            return PrimaryDataDisposition::LimitExceeded {
                resource,
                required,
                limit,
            };
        }
        Err(crate::segment_reader::InspectionPreflightError::Io(error)) => {
            return incomplete("source resource preflight", path, error);
        }
    }
    match verify_rows(&reader, descriptor) {
        Ok(disposition) => disposition,
        Err(error) => incomplete("verify logical rows and metadata", path, error),
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

pub(super) fn verify_identity(
    reader: &SegmentReader,
    descriptor: &SegmentDescriptor,
) -> io::Result<()> {
    if reader
        .captured_manifest_identity()
        .is_some_and(|identity| identity != (descriptor.id, descriptor.kind))
        || reader.read_row_count()? != descriptor.row_count
        || reader.generation() != descriptor.generation
        || reader.source_namespace() != descriptor.source_namespace.map(|value| value.0)
        || reader.source_commitment()? != descriptor.source_commitment.map(|value| value.0)
        || reader.bundle_reference() != descriptor.column_bundle.as_ref()
    {
        return Err(invalid("captured source differs from catalog identity"));
    }
    Ok(())
}

fn verify_rows(
    reader: &SegmentReader,
    descriptor: &SegmentDescriptor,
) -> io::Result<PrimaryDataDisposition> {
    match reader.read_canonical_len() {
        Ok(rows) if rows == descriptor.row_count => {
            // Envelope/length checks alone do not validate actual bitmap data.
            drop(reader.read_canonical()?);
        }
        Ok(_) => return Err(invalid("canonical bitmap row count differs")),
        // A fresh empty raw segment publishes only its manifest. This exception
        // does not apply to bundled sources or a present malformed bitmap.
        Err(error)
            if descriptor.row_count == 0
                && descriptor.column_bundle.is_none()
                && error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let rows = u32::try_from(descriptor.row_count)
        .map_err(|_| invalid("segment row addressing exceeds u32"))?;
    let mut ids = Vec::new();
    ids.try_reserve_exact(rows as usize)
        .map_err(io::Error::other)?;
    ids.extend(0..rows);
    let mut state = descriptor
        .source_namespace
        .map(|namespace| PrefixState::empty(namespace.0));
    let mut count = 0u64;
    let mut min_block = None;
    let mut max_block = None;
    let mut min_timestamp = None;
    let mut max_timestamp = None;
    for batch in reader.log_row_batches(&ids)? {
        let batch = batch?;
        count = count
            .checked_add(batch.len() as u64)
            .ok_or_else(|| invalid("row count overflow"))?;
        for row in &batch {
            min_block =
                Some(min_block.map_or(row.block_number, |value: u64| value.min(row.block_number)));
            max_block =
                Some(max_block.map_or(row.block_number, |value: u64| value.max(row.block_number)));
            min_timestamp =
                Some(min_timestamp.map_or(row.timestamp, |value: u64| value.min(row.timestamp)));
            max_timestamp =
                Some(max_timestamp.map_or(row.timestamp, |value: u64| value.max(row.timestamp)));
        }
        if let Some(previous) = state {
            state = Some(previous.extend(&batch)?);
        }
    }
    if count != descriptor.row_count
        || min_block != descriptor.min_block
        || max_block != descriptor.max_block
        || min_timestamp != descriptor.min_timestamp
        || max_timestamp != descriptor.max_timestamp
    {
        return Err(invalid("logical row count or bounds differ from catalog"));
    }
    Ok(match (state, descriptor.source_commitment) {
        (Some(state), Some(expected)) if state.commitment() != expected => {
            PrimaryDataDisposition::LogicalCommitmentMismatch
        }
        (Some(_), Some(_)) => PrimaryDataDisposition::CommitmentVerified,
        _ => PrimaryDataDisposition::Unbound,
    })
}

#[cfg(test)]
mod tests;
