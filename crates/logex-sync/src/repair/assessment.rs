//! Read-only maintenance assessment under one continuously retained data owner.
//!
//! Findings select the next verification step, not permission to replace data.
//! Primary reconstruction still requires the existing overlap plan, intact routing
//! and canonical metadata, authenticated fetches and exact replacement verification.
use std::{
    io,
    path::{Path, PathBuf},
};

use alloy_primitives::FixedBytes;
use logex_index::{IndexBuildProfile, IndexBuilder, IndexVerificationError};
use logex_storage::native::{
    InspectedSegmentRole, InspectionLimits, PrimaryDataDisposition, RepairCatalogState,
    RepairInspection, StorageCatalogPaths, inspect_repair,
};

/// Offline scan allowances, distinct from ingestion and query resource limits.
#[derive(Debug, Clone, Copy)]
pub struct RepairAssessmentLimits {
    pub primary: InspectionLimits,
    /// Required index logical payload bytes per segment. Opening an index can
    /// prefetch bytes before accounting. This is not a physical I/O or RSS cap.
    pub max_index_logical_bytes_per_segment: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepairIssue {
    pub stage: &'static str,
    pub path: PathBuf,
    pub kind: io::ErrorKind,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SegmentRepairDisposition {
    /// Primary rows match their local commitment; required index artifacts pass
    /// binding and payload checks. Neither check authenticates chain completeness
    /// or proves semantic correspondence of every index entry to its source.
    Verified,
    /// Primary rows passed. The selected index profile is absent, stale or fails
    /// artifact verification. A rebuild still needs exclusive index ownership.
    IndexRebuildRequired(RepairIssue),
    /// Local primary verification failed. Reconstruction is only a candidate:
    /// original identity, routing, canonical flags, ranges and chain anchors must
    /// all pass their existing independent checks before any replacement.
    PrimaryRepairRequired(RepairIssue),
    /// An unavailable resource or unsupported/unbound source is not proof of
    /// corruption. Resolve this issue before choosing a repair action.
    Blocked(RepairIssue),
    LimitExceeded {
        resource: &'static str,
        required: u64,
        limit: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRepairAssessment {
    pub id: u64,
    pub role: InspectedSegmentRole,
    pub disposition: SegmentRepairDisposition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairAssessmentReport {
    /// A checked journal/catalog relation takes precedence over a new scan.
    /// BeforePublication needs fresh reconstruction; AfterPublication needs
    /// committed replacement verification and quarantine completion.
    PendingPublication {
        operation: FixedBytes<16>,
        state: RepairCatalogState,
        quarantine_dir: PathBuf,
    },
    PendingIndexes {
        operation: FixedBytes<16>,
        segments: Vec<u64>,
        required_artifacts: Vec<String>,
        quarantine_dir: PathBuf,
    },
    /// Pending WAL/ingestion/reorg evidence must be verified before inspecting
    /// stable primary rows. This does not assert that replay can repair damage.
    RecoveryRequired { artifacts: Vec<PathBuf> },
    Inspected {
        index_profile: IndexBuildProfile,
        segments: Vec<SegmentRepairAssessment>,
    },
}

/// Keeps the same exclusive owner from inspection until consumed or dropped.
/// Reports are immutable through this handle and never authorize storage writes.
#[derive(Debug)]
pub struct RepairAssessment {
    inspection: RepairInspection,
    report: RepairAssessmentReport,
    limits: RepairAssessmentLimits,
}

impl RepairAssessment {
    pub(super) fn uses_limits(&self, limits: RepairAssessmentLimits) -> bool {
        self.limits.primary.max_segment_rows == limits.primary.max_segment_rows
            && self.limits.primary.max_retained_artifact_bytes
                == limits.primary.max_retained_artifact_bytes
            && self.limits.primary.max_decoded_payload_bytes
                == limits.primary.max_decoded_payload_bytes
            && self.limits.max_index_logical_bytes_per_segment
                == limits.max_index_logical_bytes_per_segment
    }

    pub fn report(&self) -> &RepairAssessmentReport {
        &self.report
    }

    /// Transfer the owner to the repair coordinator without opening storage or
    /// reacquiring its lock. Subsequent operations must revalidate their own
    /// prerequisites; this report is not a substitute for a repair plan.
    pub fn into_inspection(self) -> RepairInspection {
        self.inspection
    }
}

/// Inspect existing storage and the required derived index profile without writes,
/// replay, network requests or file creation. Synchronous, finite scans belong on
/// the maintenance worker. Per-segment budgets do not bound total elapsed time.
pub fn assess_repair(
    root: &Path,
    limits: RepairAssessmentLimits,
    index_profile: IndexBuildProfile,
) -> io::Result<RepairAssessment> {
    let inspection = inspect_repair(root, limits.primary)?;
    assess_owned(inspection, limits, index_profile)
}

pub(super) fn assess_owned(
    inspection: RepairInspection,
    limits: RepairAssessmentLimits,
    index_profile: IndexBuildProfile,
) -> io::Result<RepairAssessment> {
    let report = match &inspection {
        RepairInspection::Pending(pending) => RepairAssessmentReport::PendingPublication {
            operation: pending.operation_id(),
            state: pending.state(),
            quarantine_dir: pending.quarantine_dir(),
        },
        RepairInspection::PendingIndexes(pending) => RepairAssessmentReport::PendingIndexes {
            operation: pending.operation_id(),
            segments: pending.segment_ids().collect(),
            required_artifacts: pending.required_artifacts().to_vec(),
            quarantine_dir: pending.quarantine_dir(),
        },
        RepairInspection::Primary(primary) if !primary.recovery_prerequisites.is_empty() => {
            RepairAssessmentReport::RecoveryRequired {
                artifacts: primary.recovery_prerequisites.clone(),
            }
        }
        RepairInspection::Primary(primary) => {
            let paths = StorageCatalogPaths::new(primary.root().to_owned());
            let segments = primary
                .segments
                .iter()
                .map(|segment| {
                    let path = paths.segment_dir(segment.id);
                    let disposition = match &segment.disposition {
                        PrimaryDataDisposition::CommitmentVerified => {
                            assess_indexes(&path, index_profile, limits)
                        }
                        PrimaryDataDisposition::LogicalCommitmentMismatch => {
                            SegmentRepairDisposition::PrimaryRepairRequired(RepairIssue {
                                stage: "verify primary commitment",
                                path,
                                kind: io::ErrorKind::InvalidData,
                                message: "primary rows differ from their published local commitment"
                                    .to_owned(),
                            })
                        }
                        PrimaryDataDisposition::Unbound => {
                            SegmentRepairDisposition::Blocked(RepairIssue {
                                stage: "verify primary identity",
                                path,
                                kind: io::ErrorKind::Unsupported,
                                message: "published primary identity is absent; automatic repair cannot establish the original contents".to_owned(),
                            })
                        }
                        PrimaryDataDisposition::LimitExceeded { resource, required, limit } => {
                            SegmentRepairDisposition::LimitExceeded {
                                resource,
                                required: *required,
                                limit: *limit,
                            }
                        }
                        PrimaryDataDisposition::Incomplete { stage, path, kind, message } => {
                            let issue = RepairIssue {
                                stage,
                                path: path.clone(),
                                kind: *kind,
                                message: message.clone(),
                            };
                            if data_verification_error(*kind) {
                                SegmentRepairDisposition::PrimaryRepairRequired(issue)
                            } else {
                                SegmentRepairDisposition::Blocked(issue)
                            }
                        }
                        PrimaryDataDisposition::RecoveryRequired => {
                            SegmentRepairDisposition::Blocked(RepairIssue {
                                stage: "verify recovery prerequisites",
                                path,
                                kind: io::ErrorKind::WouldBlock,
                                message: "pending recovery must finish before stable primary verification".to_owned(),
                            })
                        }
                    };
                    SegmentRepairAssessment { id: segment.id, role: segment.role, disposition }
                })
                .collect();
            RepairAssessmentReport::Inspected {
                index_profile,
                segments,
            }
        }
    };
    Ok(RepairAssessment {
        inspection,
        report,
        limits,
    })
}

fn assess_indexes(
    path: &Path,
    profile: IndexBuildProfile,
    limits: RepairAssessmentLimits,
) -> SegmentRepairDisposition {
    match IndexBuilder::verify_indexes_with_limit(
        path,
        profile,
        limits.max_index_logical_bytes_per_segment,
    ) {
        Ok(()) => SegmentRepairDisposition::Verified,
        Err(IndexVerificationError::LimitExceeded { required, limit }) => {
            SegmentRepairDisposition::LimitExceeded {
                resource: "index logical payload bytes",
                required,
                limit,
            }
        }
        Err(IndexVerificationError::Io(error)) => {
            let issue = RepairIssue {
                stage: "verify required derived indexes",
                path: path.join("indexes"),
                kind: error.kind(),
                message: error.to_string(),
            };
            if data_verification_error(error.kind()) {
                SegmentRepairDisposition::IndexRebuildRequired(issue)
            } else {
                SegmentRepairDisposition::Blocked(issue)
            }
        }
    }
}

fn data_verification_error(kind: io::ErrorKind) -> bool {
    matches!(
        kind,
        io::ErrorKind::InvalidData | io::ErrorKind::UnexpectedEof | io::ErrorKind::NotFound
    )
}

#[cfg(test)]
mod tests;
