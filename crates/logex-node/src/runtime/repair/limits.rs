use std::time::Duration;

use eyre::{Result, ensure};
use logex_storage::{
    WalReadLimits,
    native::{InspectionLimits, RepairPlanLimits, RepairReadLimits},
};
use logex_sync::repair::{
    RepairAssessmentLimits, RepairExecutionLimits, RepairFetchLimits, RepairReconstructionLimits,
};
use tokio::time::Instant;

use crate::cli::RepairLimitsArgs;

/// These allowances bound individual maintenance inputs and retained row/data
/// work. Decoder/network scratch and filesystem allocation are separate; this
/// is deliberately not advertised as a process-memory or disk reservation cap.
pub(super) fn execution_limits(args: &RepairLimitsArgs) -> Result<RepairExecutionLimits> {
    let bytes = args.repair_max_segment_bytes;
    let rows = args.repair_max_segment_rows;
    ensure!(
        [
            bytes,
            rows,
            args.repair_max_total_rows,
            args.repair_max_total_data_bytes,
            args.repair_max_blocks,
            args.repair_max_headers,
            args.repair_timeout_secs,
            args.repair_request_timeout_secs
        ]
        .into_iter()
        .all(|limit| limit > 0)
            && args.repair_max_segments > 0
            && args.repair_max_attempts > 0,
        "repair limits must be positive"
    );
    let deadline = Instant::now()
        .checked_add(Duration::from_secs(args.repair_timeout_secs))
        .ok_or_else(|| eyre::eyre!("repair timeout is too large"))?;
    Ok(RepairExecutionLimits {
        assessment: RepairAssessmentLimits {
            primary: InspectionLimits {
                max_segment_rows: rows,
                max_retained_artifact_bytes: bytes,
                max_decoded_payload_bytes: bytes,
            },
            max_index_logical_bytes_per_segment: bytes,
        },
        wal: WalReadLimits {
            max_bytes: bytes,
            max_rows: rows,
        },
        plan: RepairPlanLimits {
            max_segments: args.repair_max_segments,
            max_blocks: args.repair_max_blocks,
            max_segment_rows: rows,
            max_canonical_artifact_bytes: bytes,
            max_candidate_data_bytes: bytes,
        },
        reconstruction: RepairReconstructionLimits {
            max_total_rows: args.repair_max_total_rows,
            max_total_data_bytes: args.repair_max_total_data_bytes,
            read: RepairReadLimits {
                max_routing_artifact_bytes: bytes,
                max_carry_artifact_bytes: bytes,
                max_carry_decoded_payload_bytes: bytes,
                max_carry_data_bytes: bytes,
            },
        },
        fetch: RepairFetchLimits {
            header_page_size: 192,
            max_headers: args.repair_max_headers,
            // Ethereum's execution rules also validate each accepted block.
            // These finite maintenance allowances reject oversized work before
            // extraction, without changing normal ingestion validation limits.
            max_transactions_per_block: 1_000_000,
            max_encoded_body_bytes: 64 * 1024 * 1024,
            max_rows_per_block: 1_000_000,
            max_log_data_bytes_per_block: 64 * 1024 * 1024,
            request_timeout: Duration::from_secs(args.repair_request_timeout_secs),
            max_attempts: args.repair_max_attempts,
            deadline,
        },
    })
}
