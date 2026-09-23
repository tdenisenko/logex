//! Original-position reconstruction under one exclusive storage owner.
//!
//! The assembler drives its own verified fetch cursor for each fixed range, so
//! completion cannot be supplied separately from the blocks actually consumed.
//! Anchors come from the retained consensus store. Every completion must still
//! be admitted against one current snapshot immediately before publication.
//! These objects do not verify staged files or implement durable publication.
use std::collections::{BTreeMap, BTreeSet};

use alloy_primitives::B256;
use eyre::{Result, WrapErr};
use logex_cl::ConsensusStore;
use logex_storage::{
    NullBitmap,
    native::{
        RepairCandidateVerifier, RepairOwnershipPlan, RepairReadLimits, SegmentDescriptor,
        VerifiedRepairCandidate,
    },
};
use logex_types::{LogRow, Source};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::{
    RepairCompletion, RepairFetchErrorKind, RepairFetchLimits, RepairFetchStep, RepairFetcher,
    RepairRange, RepairSource, VerifiedRepairBlock, failure,
};

/// Bounds the complete reconstruction working set, in addition to the plan's
/// per-segment limits and the fetcher's per-block work limits. Row count bounds
/// routing/slot metadata; actual payload bytes count each stored occurrence.
/// Captured file buffers, decoder scratch and network decoding have separate
/// allowances. These are not a total process RSS or allocation guarantee.
#[derive(Debug, Clone, Copy)]
pub struct RepairReconstructionLimits {
    pub max_total_rows: u64,
    pub max_total_data_bytes: u64,
    pub read: RepairReadLimits,
}

#[derive(Debug)]
struct PendingSegment<'a> {
    verifier: RepairCandidateVerifier<'a>,
    rows: Vec<Option<LogRow>>,
}

#[derive(Debug)]
struct ReceiptSlot {
    block_hash: B256,
    log_index: u32,
    segment: usize,
    position: usize,
}

/// Keep the maintenance operation on a blocking worker that owns the plan and
/// drives fetching through its runtime. Preparation and finish perform bounded
/// synchronous reads or hashing; fetching performs no storage I/O and follows
/// RepairSource's runtime requirements. One absolute deadline includes all ranges and
/// time between calls; cancellation is observed between finite CPU/I/O steps.
/// A failed or dropped in-progress fetch permanently poisons this assembler.
#[derive(Debug)]
pub struct RepairReconstruction<'a> {
    plan: &'a RepairOwnershipPlan,
    segments: Vec<PendingSegment<'a>>,
    pending: BTreeMap<u64, Vec<ReceiptSlot>>,
    completions: Vec<RepairCompletion>,
    fetch_limits: RepairFetchLimits,
    cancellation: CancellationToken,
    max_data_bytes: u64,
    data_bytes: u64,
    failed: bool,
}

fn check_active(deadline: Instant, cancellation: &CancellationToken) -> Result<()> {
    ensure_repair!(
        !cancellation.is_cancelled(),
        Cancelled,
        "repair reconstruction cancelled"
    );
    ensure_repair!(
        Instant::now() < deadline,
        Deadline,
        "repair reconstruction deadline exceeded"
    );
    Ok(())
}

fn charge_data(total: &mut u64, bytes: usize, limit: u64) -> Result<()> {
    let next = u64::try_from(bytes)
        .ok()
        .and_then(|bytes| total.checked_add(bytes))
        .ok_or_else(|| {
            failure(
                RepairFetchErrorKind::LimitExceeded,
                eyre::eyre!("repair reconstruction log-data byte count overflows"),
            )
        })?;
    ensure_repair!(
        next <= limit,
        LimitExceeded,
        "repair reconstruction exceeds total log-data byte budget"
    );
    *total = next;
    Ok(())
}

impl<'a> RepairReconstruction<'a> {
    pub fn new(
        plan: &'a RepairOwnershipPlan,
        limits: RepairReconstructionLimits,
        fetch_limits: RepairFetchLimits,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        check_active(fetch_limits.deadline, &cancellation)?;
        // Charge all selected rows before opening their routing or payload files.
        let selected: BTreeSet<_> = plan.segment_ids().iter().copied().collect();
        let mut total_rows = 0u64;
        for descriptor in &plan.catalog().segments {
            if selected.contains(&descriptor.id) {
                total_rows = total_rows
                    .checked_add(descriptor.row_count)
                    .ok_or_else(|| {
                        failure(
                            RepairFetchErrorKind::LimitExceeded,
                            eyre::eyre!("repair reconstruction row count overflows"),
                        )
                    })?;
                ensure_repair!(
                    total_rows <= limits.max_total_rows,
                    LimitExceeded,
                    "repair reconstruction exceeds total row budget"
                );
            }
        }
        let mut segments = Vec::new();
        segments
            .try_reserve_exact(selected.len())
            .wrap_err("reserve selected repair owners")?;
        let mut pending: BTreeMap<u64, Vec<ReceiptSlot>> = BTreeMap::new();
        let mut data_bytes = 0;
        for &id in plan.segment_ids() {
            check_active(fetch_limits.deadline, &cancellation)?;
            let mut read_limits = limits.read;
            read_limits.max_carry_data_bytes = read_limits
                .max_carry_data_bytes
                .min(limits.max_total_data_bytes - data_bytes);
            let (verifier, inputs) = plan
                .prepare_candidate(id, read_limits)
                .wrap_err_with(|| format!("prepare original row ownership for segment {id}"))
                .map_err(|error| failure(RepairFetchErrorKind::Local, error))?;
            let mut rows = Vec::new();
            rows.try_reserve_exact(inputs.len())
                .wrap_err("reserve original repair row positions")?;
            for (position, input) in inputs.into_iter().enumerate() {
                if let Some(row) = &input.preserved {
                    charge_data(&mut data_bytes, row.data.len(), limits.max_total_data_bytes)?;
                } else {
                    pending
                        .entry(input.block_number)
                        .or_default()
                        .push(ReceiptSlot {
                            block_hash: input.block_hash,
                            log_index: input.log_index,
                            segment: segments.len(),
                            position,
                        });
                }
                rows.push(input.preserved);
            }
            segments.push(PendingSegment { verifier, rows });
        }
        check_active(fetch_limits.deadline, &cancellation)?;
        Ok(Self {
            plan,
            segments,
            pending,
            completions: Vec::new(),
            fetch_limits,
            cancellation,
            max_data_bytes: limits.max_total_data_bytes,
            data_bytes,
            failed: false,
        })
    }

    pub fn next_range(&self) -> Option<RepairRange> {
        self.plan
            .block_ranges()
            .get(self.completions.len())
            .map(|&(start, end)| RepairRange { start, end })
    }

    /// Fetch the next entire affected range using the nearest admissible retained
    /// consensus anchor within the header budget. Returns false once all ranges
    /// have completed. Selection can change during fetching; the completed result
    /// therefore requires a fresh whole-transcript check before publication.
    pub async fn fetch_next_range(
        &mut self,
        source: &mut impl RepairSource,
        consensus: &ConsensusStore,
    ) -> Result<bool> {
        ensure_repair!(
            !self.failed,
            Terminal,
            "repair reconstruction is terminal after failed or cancelled fetching"
        );
        // Set before any await, including the cursor's first request. Dropping a
        // polled future cannot later turn its partially filled rows into success.
        self.failed = true;
        check_active(self.fetch_limits.deadline, &self.cancellation)?;
        let Some(range) = self.next_range() else {
            self.failed = false;
            return Ok(false);
        };
        let anchor = consensus
            .nearest_current_anchor(range.start, range.end, self.fetch_limits.max_headers)
            .ok_or_else(|| {
                failure(
                    RepairFetchErrorKind::Unavailable,
                    eyre::eyre!(
                        "no current consensus anchor covers repair range {}..={} within the {}-header budget; retain the original artifacts and retry with available trusted history or an appropriate bridge budget",
                        range.start,
                        range.end,
                        self.fetch_limits.max_headers
                    ),
                )
            })?;
        let mut fetch =
            RepairFetcher::new(range, anchor, self.fetch_limits, self.cancellation.clone())?;
        let expected = range
            .end
            .checked_sub(range.start)
            .and_then(|count| count.checked_add(1))
            .ok_or_else(|| {
                failure(
                    RepairFetchErrorKind::InvalidInput,
                    eyre::eyre!("repair reconstruction block count overflows"),
                )
            })?;
        let mut received = 0;
        loop {
            match fetch.next_block(source).await? {
                RepairFetchStep::Block(block) => {
                    ensure_repair!(
                        received < expected && block.header().number == range.end - received,
                        InvalidData,
                        "repair reconstruction requires exact descending block delivery"
                    );
                    self.apply_block(&block)?;
                    received += 1;
                }
                RepairFetchStep::Complete(completion) => {
                    ensure_repair!(
                        completion.range() == range
                            && completion.anchor() == anchor
                            && completion.delivered_blocks() == expected
                            && received == expected,
                        InvalidData,
                        "repair completion differs from the consumed block range"
                    );
                    ensure_repair!(
                        self.pending.range(range.start..=range.end).next().is_none(),
                        Local,
                        "repair range still has unfilled original receipt positions"
                    );
                    check_active(self.fetch_limits.deadline, &self.cancellation)?;
                    self.completions.push(completion);
                    self.failed = false;
                    return Ok(true);
                }
            }
        }
    }

    fn apply_block(&mut self, block: &VerifiedRepairBlock) -> Result<()> {
        let Some(slots) = self.pending.remove(&block.header().number) else {
            // Whole-block delivery, including empty blocks, remains part of the
            // transcript even when the original dataset has no matching rows.
            return Ok(());
        };
        let hash = block.header().hash_slow();
        for slot in slots {
            ensure_repair!(
                slot.block_hash == hash,
                Local,
                "original canonical receipt hash differs from the verified repair block"
            );
            let row = usize::try_from(slot.log_index)
                .ok()
                .and_then(|index| block.rows().get(index))
                .ok_or_else(|| {
                    failure(
                        RepairFetchErrorKind::Local,
                        eyre::eyre!("original receipt log index is absent from the verified block"),
                    )
                })?;
            ensure_repair!(
                row.block_number == block.header().number
                    && row.block_hash == hash
                    && row.log_index == slot.log_index
                    && row.source == Source::Receipt,
                InvalidData,
                "verified repair row differs from its block position"
            );
            charge_data(&mut self.data_bytes, row.data.len(), self.max_data_bytes)?;
            let target = self
                .segments
                .get_mut(slot.segment)
                .and_then(|segment| segment.rows.get_mut(slot.position))
                .ok_or_else(|| {
                    failure(
                        RepairFetchErrorKind::Local,
                        eyre::eyre!("original repair row position is unavailable"),
                    )
                })?;
            ensure_repair!(
                target.is_none(),
                Local,
                "repair row position was filled twice"
            );
            *target = Some(row.clone());
        }
        Ok(())
    }

    /// Check complete block coverage and exact original segments before returning
    /// immutable rows and canonical flags. Staging, index rebuilding and journal
    /// publication remain separate work. The result retains the operation lifetime
    /// for a fresh current-anchor and cancellation check at publication admission.
    pub fn finish(self) -> Result<ReconstructedRepair<'a>> {
        ensure_repair!(
            !self.failed,
            Terminal,
            "repair reconstruction previously failed"
        );
        check_active(self.fetch_limits.deadline, &self.cancellation)?;
        ensure_repair!(
            self.completions.len() == self.plan.block_ranges().len() && self.pending.is_empty(),
            Local,
            "repair reconstruction has incomplete block coverage or original rows"
        );
        let mut verified = Vec::new();
        verified
            .try_reserve_exact(self.segments.len())
            .wrap_err("reserve verified repair owners")?;
        for mut segment in self.segments {
            check_active(self.fetch_limits.deadline, &self.cancellation)?;
            let rows = segment
                .rows
                .into_iter()
                .map(|row| {
                    row.ok_or_else(|| {
                        failure(
                            RepairFetchErrorKind::Local,
                            eyre::eyre!("repair reconstruction has an unfilled original row"),
                        )
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            segment
                .verifier
                .append(&rows)
                .wrap_err("check reconstructed original row order and contents")
                .map_err(|error| failure(RepairFetchErrorKind::Local, error))?;
            let proof = segment
                .verifier
                .finish()
                .wrap_err("verify exact reconstructed segment commitment")
                .map_err(|error| failure(RepairFetchErrorKind::Local, error))?;
            verified.push(ReconstructedSegment { proof, rows });
        }
        check_active(self.fetch_limits.deadline, &self.cancellation)?;
        Ok(ReconstructedRepair {
            plan: self.plan,
            segments: verified,
            completions: self.completions,
            deadline: self.fetch_limits.deadline,
            cancellation: self.cancellation,
        })
    }
}

/// Exact local equivalence and complete fetch transcripts, under the original
/// directory owner. The caller remains responsible for staged-file verification
/// and durable publication inside a fresh current-anchor admission.
#[derive(Debug)]
pub struct ReconstructedRepair<'a> {
    plan: &'a RepairOwnershipPlan,
    segments: Vec<ReconstructedSegment<'a>>,
    completions: Vec<RepairCompletion>,
    deadline: Instant,
    cancellation: CancellationToken,
}

impl ReconstructedRepair<'_> {
    pub fn plan(&self) -> &RepairOwnershipPlan {
        self.plan
    }
    pub fn segments(&self) -> &[ReconstructedSegment<'_>] {
        &self.segments
    }
    pub fn completions(&self) -> &[RepairCompletion] {
        &self.completions
    }

    /// Admit one synchronous operation only while every completion anchor is
    /// current under one consensus snapshot. Prepare and verify staging before
    /// calling; keep the existing storage owner throughout. The callback must not
    /// await, re-enter consensus APIs, or call peers, subscribers or progress hooks.
    ///
    /// Cancellation and deadline are checked again after acquiring that snapshot,
    /// immediately before the callback. Cancellation after admission cannot undo
    /// its writes; the callback must finish or leave recoverable journal evidence.
    /// Empty transcripts are valid only for the already-proven empty range union
    /// and make no chain claim. This is not finality or cross-file durability.
    pub fn with_current_anchors<R>(
        &self,
        consensus: &ConsensusStore,
        publish: impl FnOnce() -> Result<R>,
    ) -> Result<R> {
        check_active(self.deadline, &self.cancellation)?;
        let mut anchors = Vec::new();
        anchors
            .try_reserve_exact(self.completions.len())
            .wrap_err("reserve repair admission anchors")?;
        anchors.extend(self.completions.iter().map(RepairCompletion::anchor));
        consensus
            .with_current_anchors(&anchors, || {
                check_active(self.deadline, &self.cancellation)?;
                publish()
            })
            .ok_or_else(|| {
                failure(
                    RepairFetchErrorKind::Unavailable,
                    eyre::eyre!(
                        "repair completion anchors are no longer current; retain staged and original artifacts for a fresh verified repair attempt"
                    ),
                )
            })?
    }
}

#[derive(Debug)]
pub struct ReconstructedSegment<'a> {
    proof: VerifiedRepairCandidate<'a>,
    rows: Vec<LogRow>,
}

impl ReconstructedSegment<'_> {
    pub fn descriptor(&self) -> &SegmentDescriptor {
        self.proof.descriptor()
    }
    pub fn canonical(&self) -> &NullBitmap {
        self.proof.canonical()
    }
    pub fn rows(&self) -> &[LogRow] {
        &self.rows
    }
}

#[cfg(test)]
mod tests;
