//! Ownership planning, exact reconstruction and journaled offline publication.
//!
//! A successful check preserves published local contents; it does not prove
//! chain membership or block completeness. Staging independently checks encoded
//! primary data; the sync coordinator retains authenticated fetch transcripts
//! and guards the catalog switch. Pending journal evidence blocks ordinary open
//! until explicit repair resumption completes original-artifact quarantine.
mod input;
pub(super) mod journal;
mod overlap;
pub(super) mod publication;
mod staging;
pub use input::{RepairReadLimits, RepairRowInput};
pub use publication::{
    CommittedRepairPublication, PendingRepair, PreparedRepairPublication, RepairCatalogState,
    RepairInspection, RepairPublication, inspect_pending_repair, inspect_repair,
};
pub use staging::StagedRepairCandidate;

use std::{collections::BTreeMap, io};

use logex_types::LogRow;

use super::{
    catalog::{NativeStorageCatalog, SegmentDescriptor},
    inspection::{self, PrimaryDataInspection},
};
use crate::{NullBitmap, SegmentReader, commitment::PrefixState, row_bounds::RowBounds};

/// Work limits for the affected block ranges and each candidate segment.
/// These do not bound the already-loaded catalog, caller-owned input batches,
/// total allocations or process RSS. Candidates retain one canonical bitmap.
#[derive(Debug, Clone, Copy)]
pub struct RepairPlanLimits {
    pub max_segments: usize,
    /// Total inclusive block count in the union of explicit seed ranges.
    pub max_blocks: u64,
    pub max_segment_rows: u64,
    pub max_canonical_artifact_bytes: u64,
    /// Sum of actual log-data bytes submitted for one segment.
    pub max_candidate_data_bytes: u64,
}

/// Retains the inspection's exclusive directory owner without dropping and
/// reacquiring it. Original descriptors, progress and chain anchors are immutable.
#[derive(Debug)]
pub struct RepairOwnershipPlan {
    inspection: PrimaryDataInspection,
    selection: overlap::Selection,
    selected: BTreeMap<u64, usize>,
    limits: RepairPlanLimits,
    seeds: Vec<u64>,
    pending: Option<journal::RepairJournal>,
    publication_active: std::sync::atomic::AtomicBool,
}

impl PrimaryDataInspection {
    /// Select every owner of the explicit seed block ranges without modifying
    /// files. Healthy neighbors do not expand the affected ranges: their other
    /// rows must be verified and preserved locally. Unavailable carry-forward
    /// evidence blocks reconstruction or requires an explicitly revised plan.
    /// An inspection error or resource limit alone is not proof of damage.
    pub fn into_repair_plan(
        self,
        segment_ids: &[u64],
        limits: RepairPlanLimits,
    ) -> io::Result<RepairOwnershipPlan> {
        self.into_repair_plan_with_journal(segment_ids, limits, None)
    }

    fn into_repair_plan_with_journal(
        self,
        segment_ids: &[u64],
        limits: RepairPlanLimits,
        pending: Option<journal::RepairJournal>,
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
        let mut prerequisites = inspection::recovery_prerequisites(&self.paths, &catalog)?;
        if let Some(journal) = &pending {
            journal.validate()?;
            if catalog != journal.before {
                return Err(invalid(
                    "repair resumption requires the exact original catalog",
                ));
            }
            prerequisites.retain(|path| *path != self.paths.root().join(journal::JOURNAL_FILE));
        }
        if !prerequisites.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "verified WAL or reorg recovery is required before segment repair planning",
            ));
        }
        let selection = overlap::select(
            &catalog,
            segment_ids,
            limits.max_segments,
            limits.max_blocks,
        )?;
        let mut selected: BTreeMap<_, _> =
            selection.segment_ids.iter().map(|&id| (id, 0)).collect();
        for (index, segment) in catalog.segments.iter().enumerate() {
            if let Some(position) = selected.get_mut(&segment.id) {
                *position = index;
            }
        }
        Ok(RepairOwnershipPlan {
            inspection: self,
            selection,
            selected,
            limits,
            seeds: segment_ids.to_vec(),
            pending,
            publication_active: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

impl RepairOwnershipPlan {
    pub fn catalog(&self) -> &NativeStorageCatalog {
        &self.inspection.catalog
    }

    /// Unique owners intersecting the affected ranges, plus explicit empty
    /// seeds. Every selected segment still needs an exact whole-segment check.
    pub fn segment_ids(&self) -> &[u64] {
        &self.selection.segment_ids
    }

    /// Sorted disjoint inclusive seed ranges, without gaps added by healthy
    /// neighbors. These are reconstruction inputs, not evidence of completeness
    /// or permission to insert fetched rows absent from the original segments.
    pub fn block_ranges(&self) -> &[(u64, u64)] {
        &self.selection.block_ranges
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

impl<'a> VerifiedRepairCandidate<'a> {
    pub fn descriptor(&self) -> &SegmentDescriptor {
        &self.plan.catalog().segments[self.index]
    }

    pub fn canonical(&self) -> &NullBitmap {
        &self.canonical
    }

    fn verifier(&self) -> io::Result<RepairCandidateVerifier<'a>> {
        let namespace = self
            .descriptor()
            .source_namespace
            .ok_or_else(|| invalid("verified repair source has no namespace"))?;
        Ok(RepairCandidateVerifier {
            plan: self.plan,
            index: self.index,
            canonical: self.canonical.clone(),
            state: PrefixState::empty(namespace.0),
            bounds: None,
            data_bytes: 0,
            failed: false,
        })
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod range_tests;
