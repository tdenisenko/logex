//! Explicit invocation budgets. These are not normal startup/config defaults.
use alloy_primitives::B256;
use logex_storage::native::{InspectionLimits, PrimaryAuditLimits};
use logex_sync::{
    history_audit::{AuditJournalLimits, AuditManifestLimits},
    repair::RepairFetchLimits,
};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{self, Read},
    path::Path,
    time::Duration,
};

const MAX_PLAN_BYTES: u64 = 16 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Plan {
    pub version: u32,
    /// Stable owner-selected identifier. Repeating start cannot create a new job.
    pub request_id: B256,
    pub scope: Scope,
    /// Stop this invocation after this many NEW blocks, preserving a checkpoint.
    pub new_blocks: u64,
    pub deadline_secs: u64,
    pub network_batch_blocks: usize,
    pub minimum_free_bytes: u64,
    pub source: SourceLimits,
    pub manifest: ManifestLimits,
    pub journal: AuditJournalLimits,
    pub fetch: FetchLimits,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Scope {
    /// Validate recent payload commitments and measure fetch cost, without a
    /// whole-primary scan or any assertion of complete stored event integrity.
    Pilot,
    /// Compare every stored occurrence from here to a frozen finalized anchor.
    Audit { from_block: u64 },
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SourceLimits {
    pub segments: usize,
    pub total_rows: u64,
    pub segment_rows: u64,
    pub retained_segment_bytes: u64,
    pub decoded_segment_bytes: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ManifestLimits {
    pub sort_records: usize,
    pub merge_fan_in: usize,
    pub scratch_bytes: u64,
    pub runs: u64,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FetchLimits {
    pub header_page: u64,
    pub headers: u64,
    pub transactions_per_block: usize,
    pub encoded_body_bytes: usize,
    pub events_per_block: usize,
    pub event_data_bytes: usize,
    pub request_timeout_secs: u64,
    pub attempts: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct Invocation {
    pub(super) plan: Plan,
    pub(super) resume: bool,
}
impl Invocation {
    pub(crate) fn network_batch_blocks(&self) -> usize {
        self.plan.network_batch_blocks
    }
    /// Read before the supervised volume changes cwd. Only the parsed numeric
    /// plan enters the runtime; output paths stay in the owned data namespace.
    pub(crate) fn read(path: &Path, resume: bool) -> io::Result<Self> {
        let file = fs::File::open(path)?;
        if !file.metadata()?.is_file() || file.metadata()?.len() > MAX_PLAN_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "audit plan must be a bounded regular JSON file",
            ));
        }
        let mut bytes = Vec::new();
        file.take(MAX_PLAN_BYTES + 1).read_to_end(&mut bytes)?;
        if bytes.len() as u64 > MAX_PLAN_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "audit plan exceeds byte allowance",
            ));
        }
        let plan: Plan = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        plan.validate()?;
        if resume && matches!(plan.scope, Scope::Pilot) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fetch-cost pilots do not resume; use a new request ID",
            ));
        }
        Ok(Self { plan, resume })
    }
}
impl Plan {
    fn validate(&self) -> io::Result<()> {
        let valid = self.version == 1
            && self.request_id != B256::ZERO
            && self.new_blocks > 0
            && (1..=7 * 24 * 3600).contains(&self.deadline_secs)
            && (1..=32).contains(&self.network_batch_blocks)
            && self.minimum_free_bytes > 0
            && self.source.segments > 0
            && self.source.total_rows > 0
            && (1..=u64::from(u32::MAX)).contains(&self.source.segment_rows)
            && self.source.retained_segment_bytes > 0
            && self.source.decoded_segment_bytes > 0
            && self.manifest.sort_records > 0
            && (2..=64).contains(&self.manifest.merge_fan_in)
            && self.manifest.scratch_bytes > 0
            && self.manifest.runs > 0
            && self.journal.max_bytes > 0
            && (1..=1_000_000).contains(&self.journal.max_chunks)
            && (1..=1024).contains(&self.journal.checkpoint_blocks)
            && (1..=1024).contains(&self.fetch.header_page)
            && self.fetch.headers > 0
            && self.fetch.transactions_per_block > 0
            && self.fetch.encoded_body_bytes > 0
            && self.fetch.events_per_block > 0
            && self.fetch.event_data_bytes > 0
            && (1..=60).contains(&self.fetch.request_timeout_secs)
            && (1..=4).contains(&self.fetch.attempts);
        if !valid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid or unsupported explicit audit work budget",
            ));
        }
        Ok(())
    }
    pub(super) fn source_limits(&self) -> PrimaryAuditLimits {
        PrimaryAuditLimits {
            max_segments: self.source.segments,
            max_total_rows: self.source.total_rows,
            segment: InspectionLimits {
                max_segment_rows: self.source.segment_rows,
                max_retained_artifact_bytes: self.source.retained_segment_bytes,
                max_decoded_payload_bytes: self.source.decoded_segment_bytes,
            },
        }
    }
    pub(super) fn manifest_limits(&self) -> AuditManifestLimits {
        AuditManifestLimits {
            sort_records: self.manifest.sort_records,
            merge_fan_in: self.manifest.merge_fan_in,
            max_scratch_bytes: self.manifest.scratch_bytes,
            max_runs: self.manifest.runs,
        }
    }
    pub(super) fn fetch_limits(&self, deadline: tokio::time::Instant) -> RepairFetchLimits {
        RepairFetchLimits {
            header_page_size: self.fetch.header_page,
            max_headers: self.fetch.headers,
            max_transactions_per_block: self.fetch.transactions_per_block,
            max_encoded_body_bytes: self.fetch.encoded_body_bytes,
            max_rows_per_block: self.fetch.events_per_block,
            max_log_data_bytes_per_block: self.fetch.event_data_bytes,
            request_timeout: Duration::from_secs(self.fetch.request_timeout_secs),
            max_attempts: self.fetch.attempts,
            deadline,
        }
    }
}
