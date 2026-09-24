//! Pure presentation of local assessment evidence; no recovery authorization.
use logex_index::IndexBuildProfile;
use logex_storage::native::{InspectedSegmentRole, RepairCatalogState};
use logex_sync::repair::{RepairAssessmentReport, RepairIssue, SegmentRepairDisposition};
use serde_json::{Value, json};

pub(super) fn report_exit_code(report: &RepairAssessmentReport) -> i32 {
    match report {
        RepairAssessmentReport::Inspected { segments, .. } => {
            if segments.iter().any(|segment| {
                matches!(
                    segment.disposition,
                    SegmentRepairDisposition::Blocked(_)
                        | SegmentRepairDisposition::LimitExceeded { .. }
                )
            }) {
                1
            } else if segments
                .iter()
                .all(|segment| matches!(segment.disposition, SegmentRepairDisposition::Verified))
            {
                0
            } else {
                2
            }
        }
        _ => 2,
    }
}

pub(super) fn assessment_report(report: &RepairAssessmentReport) -> Value {
    let exit_code = report_exit_code(report);
    let mut value = match report {
        RepairAssessmentReport::PendingPublication {
            operation,
            state,
            quarantine_dir,
        } => json!({
            "kind": "pending_publication",
            "summary": match state {
                RepairCatalogState::BeforePublication => "Pending primary publication requires fresh reconstruction and verification before replacement.",
                RepairCatalogState::AfterPublication => "Published replacement requires verification and quarantine completion.",
            },
            "operation": operation.to_string(),
            "publication_state": match state {
                RepairCatalogState::BeforePublication => "before_publication",
                RepairCatalogState::AfterPublication => "after_publication",
            },
            "quarantine_dir": quarantine_dir,
        }),
        RepairAssessmentReport::PendingIndexes {
            operation,
            segments,
            required_artifacts,
            quarantine_dir,
        } => json!({
            "kind": "pending_indexes",
            "summary": "Pending index publication requires source verification and resumed staging or installation.",
            "operation": operation.to_string(),
            "segment_ids": segments,
            "required_artifacts": required_artifacts,
            "quarantine_dir": quarantine_dir,
        }),
        RepairAssessmentReport::RecoveryRequired { artifacts } => json!({
            "kind": "recovery_required",
            "summary": "Recovery evidence must be verified before stable primary inspection; its presence does not establish that replay can repair damaged rows.",
            "recovery_prerequisites": artifacts,
        }),
        RepairAssessmentReport::Inspected {
            index_profile,
            segments,
        } => {
            let segments: Vec<_> = segments.iter().map(|segment| {
                let mut value = match &segment.disposition {
                    SegmentRepairDisposition::Verified => json!({
                        "status": "verified",
                        "summary": "Primary rows match the local commitment and required index artifacts passed binding and payload checks.",
                    }),
                    SegmentRepairDisposition::IndexRebuildRequired(issue) => json!({
                        "status": "index_rebuild_required",
                        "summary": "Primary rows passed; index rebuilding requires exclusive ownership and publication verification.",
                        "diagnostic": diagnostic(issue),
                    }),
                    SegmentRepairDisposition::PrimaryRepairRequired(issue) => json!({
                        "status": "primary_repair_required",
                        "summary": "Primary verification failed; reconstruction remains conditional on independent identity, routing, canonical metadata, range and anchor checks.",
                        "diagnostic": diagnostic(issue),
                    }),
                    SegmentRepairDisposition::Blocked(issue) => json!({
                        "status": "blocked",
                        "summary": "Resolve the unavailable or unsupported evidence before choosing a repair action; this finding does not establish corruption.",
                        "diagnostic": diagnostic(issue),
                    }),
                    SegmentRepairDisposition::LimitExceeded { resource, required, limit } => json!({
                        "status": "limit_exceeded",
                        "summary": "Assessment could not complete within its maintenance work allowance.",
                        "resource": resource,
                        "required": required,
                        "limit": limit,
                    }),
                };
                value["id"] = json!(segment.id);
                value["role"] = json!(match segment.role {
                    InspectedSegmentRole::ActiveHot => "active_hot",
                    InspectedSegmentRole::ActiveHistorical => "active_historical",
                    InspectedSegmentRole::CompletedSealed => "completed_sealed",
                });
                value
            }).collect();
            json!({
                "kind": "inspected",
                "summary": match exit_code {
                    0 => "All selected segments passed local primary and required index artifact checks.",
                    1 => "Assessment contains blocked or limit-exceeded segments; resolve them before repair.",
                    _ => "Assessment found segments requiring further repair verification or index rebuilding.",
                },
                "index_profile": match index_profile {
                    IndexBuildProfile::All => "all",
                    IndexBuildProfile::LogQuery => "log_query",
                    IndexBuildProfile::Erc20Transfer => "erc20_transfer",
                },
                "segments": segments,
            })
        }
    };
    value["requires_repair"] = json!(exit_code != 0);
    value["blocked"] = json!(exit_code == 1);
    value["exit_code"] = json!(exit_code);
    value["scope"] = json!(
        "Local assessment only; does not establish chain membership, source completeness, canonical correctness, or authorization to replace data."
    );
    value
}

fn diagnostic(issue: &RepairIssue) -> Value {
    use std::io::ErrorKind;
    json!({
        "stage": issue.stage,
        "path": issue.path,
        "kind": match issue.kind {
            ErrorKind::NotFound => "not_found",
            ErrorKind::PermissionDenied => "permission_denied",
            ErrorKind::InvalidData => "invalid_data",
            ErrorKind::InvalidInput => "invalid_input",
            ErrorKind::Unsupported => "unsupported",
            ErrorKind::WouldBlock => "would_block",
            ErrorKind::UnexpectedEof => "unexpected_eof",
            ErrorKind::TimedOut => "timed_out",
            ErrorKind::Interrupted => "interrupted",
            ErrorKind::OutOfMemory => "out_of_memory",
            ErrorKind::StorageFull => "storage_full",
            ErrorKind::ReadOnlyFilesystem => "read_only_filesystem",
            _ => "other",
        },
        "message": issue.message,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logex_sync::repair::SegmentRepairAssessment;

    fn inspected(dispositions: Vec<SegmentRepairDisposition>) -> RepairAssessmentReport {
        RepairAssessmentReport::Inspected {
            index_profile: IndexBuildProfile::All,
            segments: dispositions
                .into_iter()
                .enumerate()
                .map(|(id, disposition)| SegmentRepairAssessment {
                    id: id as u64,
                    role: InspectedSegmentRole::ActiveHot,
                    disposition,
                })
                .collect(),
        }
    }

    fn issue() -> RepairIssue {
        RepairIssue {
            stage: "verify source",
            path: "segments/s_1/data.col".into(),
            kind: std::io::ErrorKind::InvalidData,
            message: "checksum mismatch".into(),
        }
    }

    #[test]
    fn mixed_findings_preserve_status_diagnostics_and_blocker_precedence() {
        let report = inspected(vec![
            SegmentRepairDisposition::Verified,
            SegmentRepairDisposition::IndexRebuildRequired(issue()),
            SegmentRepairDisposition::PrimaryRepairRequired(issue()),
            SegmentRepairDisposition::Blocked(issue()),
            SegmentRepairDisposition::LimitExceeded {
                resource: "rows",
                required: 10,
                limit: 5,
            },
        ]);
        let value = assessment_report(&report);
        assert_eq!(report_exit_code(&report), 1);
        assert_eq!(value["blocked"], true);
        assert_eq!(value["requires_repair"], true);
        let statuses: Vec<_> = value["segments"]
            .as_array()
            .unwrap()
            .iter()
            .map(|segment| segment["status"].as_str().unwrap())
            .collect();
        assert_eq!(
            statuses,
            [
                "verified",
                "index_rebuild_required",
                "primary_repair_required",
                "blocked",
                "limit_exceeded"
            ]
        );
        assert_eq!(value["segments"][2]["diagnostic"]["stage"], "verify source");
        assert_eq!(
            value["segments"][2]["diagnostic"]["path"],
            "segments/s_1/data.col"
        );
        assert_eq!(value["segments"][4]["required"], 10);
        assert_eq!(
            report_exit_code(&inspected(vec![SegmentRepairDisposition::LimitExceeded {
                resource: "rows",
                required: 2,
                limit: 1
            }])),
            1
        );
    }

    #[test]
    fn success_requires_all_verified_and_actionable_findings_exit_two() {
        for dispositions in [vec![], vec![SegmentRepairDisposition::Verified]] {
            let report = inspected(dispositions);
            assert_eq!(report_exit_code(&report), 0);
            let value = assessment_report(&report);
            assert_eq!(value["requires_repair"], false);
            assert_eq!(value["blocked"], false);
        }
        for disposition in [
            SegmentRepairDisposition::IndexRebuildRequired(issue()),
            SegmentRepairDisposition::PrimaryRepairRequired(issue()),
        ] {
            assert_eq!(
                report_exit_code(&inspected(vec![
                    SegmentRepairDisposition::Verified,
                    disposition
                ])),
                2
            );
        }
    }

    #[test]
    fn pending_reports_do_not_claim_verified_repairability() {
        let operation = alloy_primitives::FixedBytes::repeat_byte(1);
        let reports = [
            RepairAssessmentReport::RecoveryRequired {
                artifacts: vec!["wal/pending.wal".into()],
            },
            RepairAssessmentReport::PendingPublication {
                operation,
                state: RepairCatalogState::BeforePublication,
                quarantine_dir: "repair/quarantine".into(),
            },
            RepairAssessmentReport::PendingPublication {
                operation,
                state: RepairCatalogState::AfterPublication,
                quarantine_dir: "repair/quarantine".into(),
            },
            RepairAssessmentReport::PendingIndexes {
                operation,
                segments: vec![2],
                required_artifacts: vec!["address.idx".into()],
                quarantine_dir: "indexes/quarantine".into(),
            },
        ];
        for report in &reports {
            assert_eq!(report_exit_code(report), 2);
            let value = assessment_report(report);
            assert_eq!(value["requires_repair"], true);
            assert_eq!(value["blocked"], false);
            assert!(value.get("segments").is_none());
        }
        assert_eq!(
            assessment_report(&reports[0])["recovery_prerequisites"][0],
            "wal/pending.wal"
        );
        assert_eq!(
            assessment_report(&reports[1])["publication_state"],
            "before_publication"
        );
        assert_eq!(
            assessment_report(&reports[2])["publication_state"],
            "after_publication"
        );
        assert_eq!(
            assessment_report(&reports[3])["required_artifacts"][0],
            "address.idx"
        );
    }
}
