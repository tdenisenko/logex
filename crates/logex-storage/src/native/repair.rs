//! Read-only ownership planning and exact local reconstruction checks.
//!
//! A successful check preserves published local contents; it does not prove
//! chain membership or block completeness. The repair coordinator must still
//! authenticate fetched blocks, verify staged artifacts and guard publication.
mod overlap;

use std::{collections::BTreeMap, io};

use logex_types::LogRow;

use super::{
    catalog::{NativeStorageCatalog, SegmentDescriptor},
    inspection::{self, PrimaryDataInspection},
};
use crate::{NullBitmap, SegmentReader, commitment::PrefixState, row_bounds::RowBounds};

/// Work limits for the selected overlap closure and each candidate segment.
/// These do not bound the already-loaded catalog, caller-owned input batches,
/// total allocations or process RSS. Candidates retain one canonical bitmap.
#[derive(Debug, Clone, Copy)]
pub struct RepairPlanLimits {
    pub max_segments: usize,
    /// Total inclusive block count across selected overlap components.
    pub max_blocks: u64,
    pub max_segment_rows: u64,
    pub max_canonical_artifact_bytes: u64,
    /// Sum of actual log-data bytes submitted for one segment.
    pub max_candidate_data_bytes: u64,
}

/// All catalog segments connected by intersecting inclusive block ranges.
/// Adjacency alone does not connect groups. Empty segments have no range.
#[derive(Debug, PartialEq, Eq)]
pub struct RepairOwnershipGroup {
    segment_ids: Vec<u64>,
    range: Option<(u64, u64)>,
}

impl RepairOwnershipGroup {
    pub fn segment_ids(&self) -> &[u64] {
        &self.segment_ids
    }

    /// Necessary replacement range, not evidence of complete block coverage.
    pub fn block_range(&self) -> Option<(u64, u64)> {
        self.range
    }
}

/// Retains the inspection's exclusive directory owner without dropping and
/// reacquiring it. Original descriptors, progress and chain anchors are immutable.
#[derive(Debug)]
pub struct RepairOwnershipPlan {
    inspection: PrimaryDataInspection,
    groups: Vec<RepairOwnershipGroup>,
    selected: BTreeMap<u64, usize>,
    limits: RepairPlanLimits,
}

impl PrimaryDataInspection {
    /// Select complete overlap components without modifying any files. Selection
    /// is explicit: an inspection error or resource limit is not proof of damage.
    pub fn into_repair_plan(
        self,
        segment_ids: &[u64],
        limits: RepairPlanLimits,
    ) -> io::Result<RepairOwnershipPlan> {
        // Inspection fields are public reports. Re-read authoritative metadata
        // under the same owner instead of trusting a caller-modified report.
        let catalog = NativeStorageCatalog::load_existing(&self.paths)?;
        if catalog != self.catalog {
            return Err(invalid(
                "catalog differs from the inspected snapshot; inspect again",
            ));
        }
        super::storage::verify_recent_headers(&catalog.state)?;
        if !inspection::recovery_prerequisites(&self.paths, &catalog)?.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "verified WAL or reorg recovery is required before segment repair planning",
            ));
        }
        let groups = overlap::select(
            &catalog,
            segment_ids,
            limits.max_segments,
            limits.max_blocks,
        )?;
        let mut selected: BTreeMap<_, _> = groups
            .iter()
            .flat_map(|group| group.segment_ids.iter().map(|&id| (id, 0)))
            .collect();
        for (index, segment) in catalog.segments.iter().enumerate() {
            if let Some(position) = selected.get_mut(&segment.id) {
                *position = index;
            }
        }
        Ok(RepairOwnershipPlan {
            inspection: self,
            groups,
            selected,
            limits,
        })
    }
}

impl RepairOwnershipPlan {
    pub fn catalog(&self) -> &NativeStorageCatalog {
        &self.inspection.catalog
    }

    pub fn groups(&self) -> &[RepairOwnershipGroup] {
        &self.groups
    }

    /// Start a streaming check in original physical row order. Only canonical
    /// metadata is captured, so unrelated damaged payloads need not be readable.
    /// Missing ownership evidence is a blocker; flags are never reconstructed by
    /// assuming that all replacement rows belong to the current canonical chain.
    pub fn begin_candidate(&self, segment_id: u64) -> io::Result<RepairCandidateVerifier<'_>> {
        let index = *self.selected.get(&segment_id).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment is not selected for repair",
            )
        })?;
        let descriptor = &self.catalog().segments[index];
        let namespace = descriptor.source_namespace.ok_or_else(|| {
            invalid(
                "repair requires the original source namespace; preserve artifacts for recovery",
            )
        })?;
        if descriptor.source_commitment.is_none() {
            return Err(invalid(
                "repair requires the original logical commitment; preserve artifacts for recovery",
            ));
        }
        if descriptor.row_count > self.limits.max_segment_rows {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "segment {} requires {} rows; limit is {}",
                    descriptor.id, descriptor.row_count, self.limits.max_segment_rows
                ),
            ));
        }
        let dir = self.inspection.paths.segment_dir(segment_id);
        let metadata = std::fs::symlink_metadata(&dir)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "repair inspection requires an ordinary segment directory",
            ));
        }
        let mut reader = SegmentReader::open_for_inspection_projected(&dir, Some(&[]))?;
        inspection::verify_identity(&reader, descriptor)?;
        let canonical = match reader
            .read_canonical_for_inspection(self.limits.max_canonical_artifact_bytes)
        {
            Ok(bitmap) => bitmap,
            // A fresh empty raw segment can contain only its manifest.
            Err(error)
                if descriptor.row_count == 0
                    && descriptor.column_bundle.is_none()
                    && error.kind() == io::ErrorKind::NotFound =>
            {
                NullBitmap::new()
            }
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "cannot preserve canonical flags for segment {segment_id}: {error}; retain original artifacts"
                    ),
                ));
            }
        };
        if canonical.len() != descriptor.row_count {
            return Err(invalid(
                "canonical bitmap row count differs from repair source",
            ));
        }
        Ok(RepairCandidateVerifier {
            plan: self,
            index,
            canonical,
            state: PrefixState::empty(namespace.0),
            bounds: None,
            data_bytes: 0,
            failed: false,
        })
    }
}

/// Bounded retained state for exact candidate verification. Caller-owned input
/// batches are hashed and then released; payloads are not retained or staged.
/// After any rejected batch this verifier cannot issue a successful result.
#[derive(Debug)]
pub struct RepairCandidateVerifier<'a> {
    plan: &'a RepairOwnershipPlan,
    index: usize,
    canonical: NullBitmap,
    state: PrefixState,
    bounds: Option<RowBounds>,
    data_bytes: u64,
    failed: bool,
}

impl<'a> RepairCandidateVerifier<'a> {
    pub fn append(&mut self, rows: &[LogRow]) -> io::Result<()> {
        let result = self.append_checked(rows);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    fn append_checked(&mut self, rows: &[LogRow]) -> io::Result<()> {
        if self.failed {
            return Err(invalid(
                "candidate verifier previously failed; start a new candidate",
            ));
        }
        let count = self
            .bounds
            .map_or(0, |bounds| bounds.row_count)
            .checked_add(u64::try_from(rows.len()).map_err(io::Error::other)?)
            .ok_or_else(|| invalid("candidate row count overflow"))?;
        if count > self.plan.catalog().segments[self.index].row_count {
            return Err(invalid(
                "candidate contains more rows than the original segment",
            ));
        }
        let mut data_bytes = self.data_bytes;
        let mut bounds = self.bounds;
        for row in rows {
            let length = u64::try_from(row.data.len()).map_err(io::Error::other)?;
            // The published identity includes data_len independently of the
            // actual payload. Preserve both fields; charge actual bytes here.
            data_bytes = data_bytes
                .checked_add(length)
                .ok_or_else(|| invalid("candidate payload byte count overflow"))?;
            if data_bytes > self.plan.limits.max_candidate_data_bytes {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "candidate exceeds the per-segment log-data byte budget",
                ));
            }
            match &mut bounds {
                Some(bounds) => bounds.include(row),
                None => bounds = Some(RowBounds::from_row(row)),
            }
        }
        self.state = self.state.extend(rows)?;
        self.bounds = bounds;
        self.data_bytes = data_bytes;
        Ok(())
    }

    pub fn finish(self) -> io::Result<VerifiedRepairCandidate<'a>> {
        if self.failed {
            return Err(invalid(
                "candidate verifier previously failed; start a new candidate",
            ));
        }
        let descriptor = &self.plan.catalog().segments[self.index];
        if self.bounds.map_or(0, |bounds| bounds.row_count) != descriptor.row_count
            || self.bounds.map(|bounds| bounds.min_block) != descriptor.min_block
            || self.bounds.map(|bounds| bounds.max_block) != descriptor.max_block
            || self.bounds.map(|bounds| bounds.min_timestamp) != descriptor.min_timestamp
            || self.bounds.map(|bounds| bounds.max_timestamp) != descriptor.max_timestamp
        {
            return Err(invalid(
                "candidate row count or bounds differ from the original segment",
            ));
        }
        if Some(self.state.commitment()) != descriptor.source_commitment {
            return Err(invalid(
                "candidate rows differ from the original logical commitment",
            ));
        }
        Ok(VerifiedRepairCandidate {
            plan: self.plan,
            index: self.index,
            canonical: self.canonical,
        })
    }
}

/// Exact local row equivalence plus the original integrity-checked canonical
/// flags, kept under the plan's directory lock. This result does not authorize
/// publication: it neither authenticates a chain nor checks a staged file.
#[derive(Debug)]
pub struct VerifiedRepairCandidate<'a> {
    plan: &'a RepairOwnershipPlan,
    index: usize,
    canonical: NullBitmap,
}

impl VerifiedRepairCandidate<'_> {
    pub fn descriptor(&self) -> &SegmentDescriptor {
        &self.plan.catalog().segments[self.index]
    }

    pub fn canonical(&self) -> &NullBitmap {
        &self.canonical
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;
