//! Finite maintenance coordinator consuming one exclusively owned assessment.
use std::{
    collections::BTreeSet,
    io,
    path::{Path, PathBuf},
};

use eyre::{Result, WrapErr, ensure};
use logex_cl::ConsensusStore;
use logex_index::{IndexBuildProfile, IndexBuilder, IndexVerificationError};
use logex_storage::{
    WalReadLimits,
    native::{
        NativeRecoveryLimits, PrimaryDataInspection, RepairCatalogState, RepairInspection,
        RepairOwnershipPlan, RepairPlanLimits,
    },
};
use tokio_util::sync::CancellationToken;

use super::{
    RepairAssessment, RepairAssessmentLimits, RepairAssessmentReport, RepairFetchLimits,
    RepairReconstruction, RepairReconstructionLimits, RepairSource, SegmentRepairDisposition,
    assessment::assess_owned,
};

#[derive(Debug, Clone, Copy)]
pub struct RepairExecutionLimits {
    pub assessment: RepairAssessmentLimits,
    pub wal: WalReadLimits,
    pub plan: RepairPlanLimits,
    pub reconstruction: RepairReconstructionLimits,
    /// One absolute deadline spans local recovery, every range and staging.
    pub fetch: RepairFetchLimits,
}

#[derive(Debug)]
pub struct RepairExecutionOutcome {
    /// A final verified report retaining the original exclusive directory owner.
    pub assessment: RepairAssessment,
    pub quarantine_dirs: Vec<PathBuf>,
    pub recovered_pending_storage: bool,
}

/// Execute local recovery, index rebuild or authenticated reconstruction as
/// classified by the retained assessment. No ordinary ingestion handle is exposed.
///
/// This future performs bounded synchronous file/CPU work. Run it on a dedicated
/// blocking maintenance worker driving its network runtime, never an async runtime
/// worker. The caller owns transport timeout/shutdown supervision. Cancellation
/// is observed between bounded operations; started publication/quarantine finishes
/// or leaves its durable journal. No new network requests occur for index-only work.
pub async fn execute_repair(
    mut assessment: RepairAssessment,
    limits: RepairExecutionLimits,
    profile: IndexBuildProfile,
    source: &mut impl RepairSource,
    consensus: &ConsensusStore,
    cancellation: CancellationToken,
) -> Result<RepairExecutionOutcome> {
    ensure!(
        assessment.uses_limits(limits.assessment),
        "repair execution limits differ from the assessment; reassess under the chosen limits before executing"
    );
    let mut quarantine_dirs = Vec::new();
    let mut recovered = false;
    let mut reconstructed = false;
    let mut rebuilt = false;
    loop {
        active(&limits, &cancellation)?;
        // Reports are privately held, but each underlying operation independently
        // validates its catalog, source evidence and pending transaction.
        let report = assessment.report().clone();
        let inspection = match report {
            RepairAssessmentReport::PendingIndexes { .. } => {
                ensure!(
                    !rebuilt,
                    "index repair did not converge; retain its evidence"
                );
                let RepairInspection::PendingIndexes(pending) = assessment.into_inspection() else {
                    unreachable!("assessment owns matching inspection")
                };
                let mut required_names = IndexBuilder::required_index_files(profile).to_vec();
                required_names.sort_unstable();
                ensure!(
                    pending
                        .required_artifacts()
                        .iter()
                        .map(String::as_str)
                        .eq(required_names),
                    "resume index repair with its recorded index profile before selecting a different profile"
                );
                let required = index_headroom(
                    pending.estimate_metadata_bytes(),
                    pending.segment_row_counts().map(|(_, rows)| rows),
                    profile,
                )?;
                let mut repair = pending.resume(limits.assessment.primary)?;
                let quarantine = repair.execute(
                    required,
                    |source, stage| {
                        active(&limits, &cancellation)?;
                        IndexBuilder::build_fresh_indexes_at(source, stage, profile)
                    },
                    |source, stage| {
                        active(&limits, &cancellation)?;
                        verify_indexes(source, stage, profile, limits.assessment)
                    },
                )?;
                quarantine_dirs.push(quarantine);
                RepairInspection::Primary(Box::new(repair.into_inspection()?))
            }
            RepairAssessmentReport::RecoveryRequired { .. } => {
                ensure!(
                    !recovered,
                    "verified startup recovery left pending evidence; preserve artifacts for inspection"
                );
                let RepairInspection::Primary(primary) = assessment.into_inspection() else {
                    unreachable!("assessment owns matching inspection")
                };
                let primary = primary.recover(NativeRecoveryLimits { primary: limits.assessment.primary, wal: limits.wal })
                    .wrap_err("verify and recover retained startup work; committed-row damage may require separate reconstruction")?;
                recovered = true;
                RepairInspection::Primary(Box::new(primary))
            }
            RepairAssessmentReport::PendingPublication { state, .. } => {
                ensure!(
                    !reconstructed,
                    "primary repair did not converge; retain the publication journal"
                );
                let RepairInspection::Pending(pending) = assessment.into_inspection() else {
                    unreachable!("assessment owns matching inspection")
                };
                let (primary, quarantine) = match state {
                    RepairCatalogState::BeforePublication => {
                        let plan = pending.into_plan(limits.assessment.primary, limits.plan)?;
                        reconstruct(plan, limits, profile, source, consensus, &cancellation).await?
                    }
                    RepairCatalogState::AfterPublication => {
                        // The committed catalog is authoritative. Completing
                        // its quarantine requires no new chain admission.
                        let required = index_headroom(
                            pending.estimate_completion_metadata_bytes()?,
                            pending.replacement_row_counts(),
                            profile,
                        )?;
                        pending.check_completion_headroom(required)?;
                        pending.finish_inspected(limits.assessment.primary, |path| {
                            match verify_indexes(
                                path,
                                &path.join("indexes"),
                                profile,
                                limits.assessment,
                            ) {
                                Ok(()) => Ok(()),
                                Err(error) if repairable_index(&error) => {
                                    IndexBuilder::build_indexes(path, profile)?;
                                    verify_indexes(
                                        path,
                                        &path.join("indexes"),
                                        profile,
                                        limits.assessment,
                                    )
                                }
                                Err(error) => Err(error),
                            }
                        })?
                    }
                };
                quarantine_dirs.push(quarantine);
                RepairInspection::Primary(Box::new(primary))
            }
            RepairAssessmentReport::Inspected {
                index_profile,
                segments,
            } => {
                ensure!(
                    index_profile == profile,
                    "repair assessment used a different index profile; reassess before execution"
                );
                let mut primary_ids = Vec::new();
                let mut index_ids = Vec::new();
                for segment in segments {
                    match segment.disposition {
                        SegmentRepairDisposition::Verified => {}
                        SegmentRepairDisposition::PrimaryRepairRequired(_) => primary_ids.push(segment.id),
                        SegmentRepairDisposition::IndexRebuildRequired(_) => index_ids.push(segment.id),
                        SegmentRepairDisposition::Blocked(issue) => return Err(io::Error::new(issue.kind, format!("repair segment {} blocked at {} ({}): {}", segment.id, issue.stage, issue.path.display(), issue.message)).into()),
                        SegmentRepairDisposition::LimitExceeded { resource, required, limit } => return Err(io::Error::new(io::ErrorKind::InvalidInput, format!("repair segment {} exceeds {resource}: required={required}, limit={limit}", segment.id)).into()),
                    }
                }
                if primary_ids.is_empty() && index_ids.is_empty() {
                    return Ok(RepairExecutionOutcome {
                        assessment,
                        quarantine_dirs,
                        recovered_pending_storage: recovered,
                    });
                }
                let RepairInspection::Primary(primary) = assessment.into_inspection() else {
                    unreachable!("assessment owns matching inspection")
                };
                if !primary_ids.is_empty() {
                    ensure!(
                        !reconstructed,
                        "primary verification still fails after repair; retain all artifacts"
                    );
                    let plan = primary.into_repair_plan(&primary_ids, limits.plan)?;
                    let (primary, quarantine) =
                        reconstruct(plan, limits, profile, source, consensus, &cancellation)
                            .await?;
                    quarantine_dirs.push(quarantine);
                    reconstructed = true;
                    RepairInspection::Primary(Box::new(primary))
                } else {
                    ensure!(
                        !rebuilt,
                        "derived index verification still fails after repair; retain all artifacts"
                    );
                    let selected: BTreeSet<_> = index_ids.iter().copied().collect();
                    let rows = primary
                        .catalog
                        .segments
                        .iter()
                        .filter(|segment| selected.contains(&segment.id))
                        .map(|segment| segment.row_count);
                    let required = index_headroom(
                        primary.estimate_index_repair_metadata_bytes(),
                        rows,
                        profile,
                    )?;
                    let mut repair = primary.begin_index_repair(
                        &index_ids,
                        IndexBuilder::required_index_files(profile),
                        limits.assessment.primary,
                        required,
                    )?;
                    // The journal now consumes baseline free bytes. Recheck the
                    // complete fresh staging bound; reserving the original fresh
                    // estimate here is conservative about metadata already written.
                    let quarantine = repair.execute(
                        required,
                        |source, stage| {
                            active(&limits, &cancellation)?;
                            IndexBuilder::build_fresh_indexes_at(source, stage, profile)
                        },
                        |source, stage| {
                            active(&limits, &cancellation)?;
                            verify_indexes(source, stage, profile, limits.assessment)
                        },
                    )?;
                    quarantine_dirs.push(quarantine);
                    rebuilt = true;
                    RepairInspection::Primary(Box::new(repair.into_inspection()?))
                }
            }
        };
        assessment = assess_owned(inspection, limits.assessment, profile)?;
    }
}

async fn reconstruct(
    plan: RepairOwnershipPlan,
    limits: RepairExecutionLimits,
    profile: IndexBuildProfile,
    source: &mut impl RepairSource,
    consensus: &ConsensusStore,
    cancellation: &CancellationToken,
) -> Result<(PrimaryDataInspection, PathBuf)> {
    let mut reconstruction = RepairReconstruction::new(
        &plan,
        limits.reconstruction,
        limits.fetch,
        cancellation.clone(),
    )?;
    while reconstruction.fetch_next_range(source, consensus).await? {}
    let reconstruction = reconstruction.finish()?;
    let mut publication = reconstruction.begin_publication(profile)?;
    let mut stages = Vec::new();
    for &id in plan.segment_ids() {
        stages.push(reconstruction.stage_publication_segment(
            &mut publication,
            id,
            limits.assessment.primary,
            profile,
            limits.assessment.max_index_logical_bytes_per_segment,
        )?);
    }
    let quarantine = reconstruction.publish_replacements(
        publication,
        stages,
        consensus,
        limits.assessment.primary,
        profile,
        limits.assessment.max_index_logical_bytes_per_segment,
    )?;
    drop(reconstruction);
    Ok((plan.into_inspection(limits.assessment.primary)?, quarantine))
}

fn index_headroom(
    metadata: u64,
    mut rows: impl Iterator<Item = u64>,
    profile: IndexBuildProfile,
) -> io::Result<u64> {
    rows.try_fold(metadata, |total, rows| {
        total
            .checked_add(IndexBuilder::estimate_fresh_index_bytes(rows, profile)?)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "index repair size estimate overflows",
                )
            })
    })
}

fn verify_indexes(
    source: &Path,
    indexes: &Path,
    profile: IndexBuildProfile,
    limits: RepairAssessmentLimits,
) -> io::Result<()> {
    IndexBuilder::verify_indexes_at_with_limit(
        source,
        indexes,
        profile,
        limits.max_index_logical_bytes_per_segment,
    )
    .map_err(|error| match error {
        IndexVerificationError::Io(error) => error,
        IndexVerificationError::LimitExceeded { required, limit } => io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "index verification exceeds logical byte limit: required={required}, limit={limit}"
            ),
        ),
    })
}

fn repairable_index(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof | io::ErrorKind::NotFound
    )
}

fn active(limits: &RepairExecutionLimits, cancellation: &CancellationToken) -> io::Result<()> {
    if cancellation.is_cancelled() {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "offline repair cancelled; retained journals can be resumed",
        ));
    }
    if tokio::time::Instant::now() >= limits.fetch.deadline {
        return Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "offline repair deadline expired; retained journals can be resumed",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
