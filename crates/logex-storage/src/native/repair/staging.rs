//! Fresh replacement artifacts, without catalog publication or journal recovery.
use std::{fs, io, path::Path};

use super::{VerifiedRepairCandidate, invalid};
use crate::{
    SegmentReader,
    native::{InspectionLimits, SegmentDescriptor, SegmentManifest, StorageCatalogPaths},
    segment_reader::InspectionPreflightError,
};
use logex_types::LogRow;

use super::super::{inspection, segment};

/// A verified staging snapshot retaining the original exclusive storage owner.
/// Standalone staging retains the original ID; a repair transaction reserves a
/// new ID before encoding. Both use a fresh generation-zero bundle. Journal
/// ownership, disk headroom, publication and quarantine belong to the coordinator.
/// Any later metadata transformation must
/// be verified again, including its index bindings.
///
/// Artifacts are retained on drop or failure. A returned handle is not permission
/// to publish: verify again after subsequent staging writes and admit the complete
/// fetch transcript against current consensus before committing a journaled repair.
#[derive(Debug)]
pub struct StagedRepairCandidate<'a> {
    pub(super) source: VerifiedRepairCandidate<'a>,
    pub(super) paths: StorageCatalogPaths,
    pub(super) descriptor: SegmentDescriptor,
    pub(super) manifest: SegmentManifest,
    pub(super) limits: InspectionLimits,
}

impl<'a> VerifiedRepairCandidate<'a> {
    /// Create a new staging root under an existing parent. An existing root is
    /// always refused; no source artifact or caller-owned ancestor is replaced.
    /// Verify the supplied rows again because this proof does not retain them.
    ///
    /// Read limits apply to the independent staged-file verification. Encoding
    /// also uses the plan's row/payload limits; these do not bound total RSS,
    /// caller-owned rows, concurrent column scratch or disk consumption.
    pub fn stage(
        &self,
        destination: &Path,
        rows: &[LogRow],
        limits: InspectionLimits,
    ) -> io::Result<StagedRepairCandidate<'a>> {
        self.stage_as(destination, rows, limits, self.descriptor().id)
    }

    pub(super) fn stage_as(
        &self,
        destination: &Path,
        rows: &[LogRow],
        limits: InspectionLimits,
        replacement_id: u64,
    ) -> io::Result<StagedRepairCandidate<'a>> {
        if self.descriptor().row_count > limits.max_segment_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "repair source exceeds staging verification row limit",
            ));
        }
        u32::try_from(self.descriptor().row_count)
            .map_err(|_| invalid("repair source exceeds segment row addressing"))?;
        let mut verifier = self.verifier()?;
        verifier.append(rows)?;
        let source = verifier.finish()?;
        let destination = std::path::absolute(destination)?;
        fs::create_dir(&destination)?;
        let build = || {
            let paths = StorageCatalogPaths::new(destination.clone());
            fs::create_dir(paths.segments_dir())?;
            let mut descriptor = source.descriptor().clone();
            descriptor.id = replacement_id;
            descriptor.relative_path =
                std::path::PathBuf::from("segments").join(format!("s_{replacement_id:016}"));
            descriptor.manifest_relative_path = descriptor.relative_path.join("segment.json");
            descriptor.generation = 0;
            let columns = segment::write_repair_bundle(
                &paths.segment_dir(descriptor.id),
                rows,
                source.canonical(),
            )?
            .apply_to(&mut descriptor);
            let manifest = segment::manifest_with_columns(&descriptor, columns.clone());
            segment::persist_segment_manifest_with_columns(&paths, &descriptor, columns)?;
            let staged = StagedRepairCandidate {
                source,
                paths,
                descriptor,
                manifest,
                limits,
            };
            staged.verify()?;
            Ok(staged)
        };
        build().map_err(|error: io::Error| {
            io::Error::new(
                error.kind(),
                format!(
                    "repair staging failed at {}; retain the partial stage: {error}",
                    destination.display()
                ),
            )
        })
    }
}

impl StagedRepairCandidate<'_> {
    pub fn segment_dir(&self) -> std::path::PathBuf {
        self.paths.segment_dir(self.descriptor.id)
    }

    /// Unpublished metadata; only the repair transaction reserves a catalog ID.
    pub fn descriptor(&self) -> &SegmentDescriptor {
        &self.descriptor
    }

    /// Reread manifest, all logical rows and canonical flags independently.
    /// Derived indexes require their own verification by the caller.
    pub fn verify(&self) -> io::Result<()> {
        let dir = self.segment_dir();
        let manifest_path = self.paths.segment_manifest_path(self.descriptor.id);
        for (path, directory) in [
            (self.paths.root().to_owned(), true),
            (self.paths.segments_dir(), true),
            (dir.clone(), true),
            (manifest_path.clone(), false),
        ] {
            let metadata = fs::symlink_metadata(path)?;
            if metadata.file_type().is_symlink()
                || if directory {
                    !metadata.is_dir()
                } else {
                    !metadata.is_file()
                }
            {
                return Err(invalid(
                    "repair stage requires ordinary directories and files",
                ));
            }
        }
        let manifest = SegmentManifest::load(&manifest_path)
            .map_err(io::Error::from)?
            .ok_or_else(|| invalid("repair stage manifest is missing"))?;
        if manifest != self.manifest {
            return Err(invalid(
                "repair stage manifest differs from encoded metadata",
            ));
        }
        let mut reader = SegmentReader::open_for_inspection(&dir)?;
        inspection::verify_identity(&reader, &self.descriptor)?;
        reader
            .inspection_preflight(
                self.limits.max_retained_artifact_bytes,
                self.limits.max_decoded_payload_bytes,
            )
            .map_err(|error| match error {
                InspectionPreflightError::Io(error) => error,
                InspectionPreflightError::LimitExceeded {
                    resource,
                    required,
                    limit,
                } => io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("repair stage {resource} requires {required}; limit is {limit}"),
                ),
            })?;
        let canonical = reader
            .read_canonical_for_inspection(self.source.plan.limits.max_canonical_artifact_bytes)?;
        if canonical.len() != self.source.canonical().len()
            || (0..canonical.len())
                .any(|row| canonical.is_present(row) != self.source.canonical().is_present(row))
        {
            return Err(invalid("repair stage canonical flags differ from original"));
        }
        let count = u32::try_from(self.descriptor.row_count)
            .map_err(|_| invalid("repair stage exceeds segment row addressing"))?;
        let mut ids = Vec::new();
        ids.try_reserve_exact(usize::try_from(count).map_err(io::Error::other)?)
            .map_err(io::Error::other)?;
        ids.extend(0..count);
        let mut verifier = self.source.verifier()?;
        for batch in reader.log_row_batches(&ids)? {
            verifier.append(&batch?)?;
        }
        verifier.finish()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
