//! Explicit finite verification of published indexes while normal service runs.
//! This never rebuilds an index, downloads Ethereum data, or changes primary rows.
use super::maintenance_files::{create_private_directory, ensure_directory, read_json, save_json};
use alloy_primitives::B256;
use logex_index::{CapturedIndexVerification, IndexBuildProfile, IndexBuilder};
use logex_server::AppState;
use logex_storage::native::{IndexAuditLimits, IndexAuditSnapshot, InspectionLimits};
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Plan {
    version: u32,
    request_id: B256,
    deadline_secs: u64,
    minimum_free_bytes: u64,
    max_segments: usize,
    max_total_rows: u64,
    max_segment_rows: u64,
    max_retained_source_bytes: u64,
    max_decoded_payload_bytes: u64,
    max_index_logical_bytes_per_segment: u64,
    max_manifest_bytes: u64,
}

impl Plan {
    pub(crate) fn read(path: &Path) -> io::Result<Self> {
        let plan: Self = read_json(path)?;
        plan.validate()?;
        Ok(plan)
    }

    fn validate(&self) -> io::Result<()> {
        if self.version != 1
            || self.request_id == B256::ZERO
            || !(1..=7 * 24 * 3600).contains(&self.deadline_secs)
            || self.minimum_free_bytes == 0
            || !(1..=1_000_000).contains(&self.max_segments)
            || self.max_total_rows == 0
            || !(1..=u64::from(u32::MAX)).contains(&self.max_segment_rows)
            || self.max_retained_source_bytes == 0
            || self.max_decoded_payload_bytes == 0
            || self.max_index_logical_bytes_per_segment == 0
            || self.max_manifest_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid explicit index audit budget",
            ));
        }
        Ok(())
    }

    fn limits(&self) -> IndexAuditLimits {
        IndexAuditLimits {
            max_segments: self.max_segments,
            max_total_rows: self.max_total_rows,
            segment: InspectionLimits {
                max_segment_rows: self.max_segment_rows,
                max_retained_artifact_bytes: self.max_retained_source_bytes,
                max_decoded_payload_bytes: self.max_decoded_payload_bytes,
            },
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cancellation {
    version: u32,
    request_id: B256,
}

fn request_directory(data: &Path, id: B256) -> PathBuf {
    data.join("index-audits").join(format!("request-{id:x}"))
}

/// Record an explicit cancellation for an existing, matching job only. This is
/// cooperative and does not signal the node or claim its worker has stopped.
pub(crate) fn cancel_request(data: &Path, id: B256) -> io::Result<()> {
    let directory = request_directory(data, id);
    if !fs::symlink_metadata(data.join("index-audits"))?.is_dir()
        || !fs::symlink_metadata(&directory)?.is_dir()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index audit directory is not ordinary",
        ));
    }
    let plan = Plan::read(&directory.join("plan.json"))?;
    if plan.request_id != id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index audit request identity changed",
        ));
    }
    let path = directory.join("cancel.json");
    match fs::symlink_metadata(&path) {
        Ok(_) => {
            let recorded: Cancellation = read_json(&path)?;
            if recorded.version != 1 || recorded.request_id != id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "index audit cancellation identity changed",
                ));
            }
            Ok(())
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => save_json(
            &directory,
            "cancel.json",
            &Cancellation {
                version: 1,
                request_id: id,
            },
            false,
        ),
        Err(e) => Err(e),
    }
}

fn now_ms() -> io::Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_millis(),
    )
    .map_err(io::Error::other)
}

#[derive(Serialize)]
struct Report {
    version: u32,
    request_id: B256,
    phase: &'static str,
    started_unix_ms: u64,
    updated_unix_ms: u64,
    elapsed_seconds: f64,
    source_prefix_fingerprint: Option<B256>,
    captured_segments: usize,
    captured_physical_rows: u64,
    verified_nonempty_segments: usize,
    minimum_rows_covered: u64,
    verified_rows_in_opened_prefixes: u64,
    verified_index_logical_bytes: u64,
    manifest_bytes: u64,
    manifest_blake3: Option<B256>,
    complete: bool,
    error: Option<String>,
    limitations: [&'static str; 4],
}

impl Report {
    fn new(plan: &Plan, stamp: u64) -> Self {
        Self {
            version: 1,
            request_id: plan.request_id,
            phase: "waiting_for_verified_coverage",
            started_unix_ms: stamp,
            updated_unix_ms: stamp,
            elapsed_seconds: 0.0,
            source_prefix_fingerprint: None,
            captured_segments: 0,
            captured_physical_rows: 0,
            verified_nonempty_segments: 0,
            minimum_rows_covered: 0,
            verified_rows_in_opened_prefixes: 0,
            verified_index_logical_bytes: 0,
            manifest_bytes: 0,
            manifest_blake3: None,
            complete: false,
            error: None,
            limitations: [
                "Every artifact in each admitted publication is checked against its retained local source. This is not independent Ethereum authentication.",
                "The finite catalog selection includes every nonempty captured prefix. Later appends may be included per source; later new segments require tail reconciliation.",
                "Row/source/index/report limits bound work inputs, not total process RSS, elapsed I/O latency or filesystem allocation.",
                "Cancellation is cooperative between finite reads/decodes/membership steps. No indexes are rebuilt and no Ethereum data is downloaded.",
            ],
        }
    }
}

#[derive(Serialize)]
struct ArtifactRecord<'a> {
    name: &'a str,
    file_id: alloy_primitives::FixedBytes<16>,
    logical_bytes: u64,
}

#[derive(Serialize)]
struct SegmentRecord<'a> {
    segment_id: u64,
    minimum_rows: u64,
    verified_rows: u64,
    source_namespace: alloy_primitives::FixedBytes<16>,
    source_commitment: B256,
    artifacts: Vec<ArtifactRecord<'a>>,
}

struct Control {
    cancellation: CancellationToken,
    deadline: Instant,
    last_filesystem_check: Cell<Instant>,
    calls: Cell<u64>,
    error: RefCell<Option<String>>,
    directory: PathBuf,
    request_id: B256,
    minimum_free_bytes: u64,
}

impl Control {
    fn check(&self, source: Option<&IndexAuditSnapshot>) -> io::Result<()> {
        if let Some(error) = self.error.borrow().as_ref() {
            return Err(io::Error::other(error.clone()));
        }
        if self.cancellation.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "index audit cancelled",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "index audit deadline exceeded",
            ));
        }
        if let Some(source) = source {
            source.validate()?;
        }
        if self.last_filesystem_check.get().elapsed() >= Duration::from_secs(1) {
            self.last_filesystem_check.set(Instant::now());
            let path = self.directory.join("cancel.json");
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    let record: Cancellation = read_json(&path)?;
                    if record.version != 1 || record.request_id != self.request_id {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "index audit cancellation identity changed",
                        ));
                    }
                    self.cancellation.cancel();
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "index audit cancellation requested",
                    ));
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            if let Some(source) = source {
                source.check_available_space(self.minimum_free_bytes)?;
            }
        }
        Ok(())
    }

    fn cancelled(&self, source: &IndexAuditSnapshot) -> bool {
        if self.cancellation.is_cancelled() || self.error.borrow().is_some() {
            return true;
        }
        let calls = self.calls.get().wrapping_add(1);
        self.calls.set(calls);
        if calls.is_multiple_of(4096)
            && let Err(error) = self.check(Some(source))
        {
            self.error.replace(Some(error.to_string()));
            return true;
        }
        false
    }
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

pub(super) fn spawn(
    plan: Plan,
    data: PathBuf,
    state: Arc<AppState>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        if *shutdown.borrow() {
            return;
        }
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancellation.clone());
        let worker_cancel = cancellation.clone();
        let mut worker = tokio::task::spawn_blocking(move || run(plan, data, state, worker_cancel));
        let result = tokio::select! {
            biased;
            result = &mut worker => result,
            _ = async { while !*shutdown.borrow_and_update() { if shutdown.changed().await.is_err() { break; } } } => {
                cancellation.cancel(); worker.await
            }
        };
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(%error, "explicit index audit stopped"),
            Err(error) => tracing::error!(%error, "explicit index audit worker failed"),
        }
    })
}

fn run(
    plan: Plan,
    data: PathBuf,
    state: Arc<AppState>,
    cancellation: CancellationToken,
) -> io::Result<()> {
    plan.validate()?;
    let started = Instant::now();
    let parent = data.join("index-audits");
    ensure_directory(&parent)?;
    let directory = request_directory(&data, plan.request_id);
    create_private_directory(&directory)?; // Existing request IDs never rerun implicitly.
    File::open(&parent)?.sync_all()?;
    save_json(&directory, "plan.json", &plan, false)?;
    let control = Control {
        cancellation,
        deadline: started + Duration::from_secs(plan.deadline_secs),
        last_filesystem_check: Cell::new(started),
        calls: Cell::new(0),
        error: RefCell::new(None),
        directory: directory.clone(),
        request_id: plan.request_id,
        minimum_free_bytes: plan.minimum_free_bytes,
    };
    let mut report = Report::new(&plan, now_ms()?);
    save_json(&directory, "progress.json", &report, false)?;
    let result = execute(&plan, &directory, &state, &control, started, &mut report);
    report.updated_unix_ms = now_ms()?;
    report.elapsed_seconds = started.elapsed().as_secs_f64();
    if let Err(error) = &result {
        report.phase = "failed";
        report.error = Some(error.to_string().chars().take(2048).collect());
        report.complete = false;
    }
    save_json(&directory, "result.json", &report, false)?;
    save_json(&directory, "progress.json", &report, true)?;
    tracing::info!(request_id=%plan.request_id, phase=report.phase, segments=report.verified_nonempty_segments, "explicit index audit ended");
    result
}

fn execute(
    plan: &Plan,
    directory: &Path,
    state: &AppState,
    control: &Control,
    started: Instant,
    report: &mut Report,
) -> io::Result<()> {
    let source = loop {
        control.check(None)?;
        let healthy = {
            let s = state.sync_status.lock().unwrap();
            s.consensus_head_fresh == Some(true)
                && s.connected_peers > 0
                && s.current_block.saturating_add(2) >= s.target_block
        };
        if healthy {
            let storage = state.storage.blocking_read();
            if storage
                .verified_log_coverage()
                .is_some_and(|v| v.from.block_number == 0)
            {
                break storage.index_audit_snapshot(plan.limits())?;
            }
        }
        std::thread::sleep(Duration::from_millis(250));
    };
    verify_snapshot(plan, directory, state, control, started, report, source)
}

fn verify_snapshot(
    plan: &Plan,
    directory: &Path,
    state: &AppState,
    control: &Control,
    started: Instant,
    report: &mut Report,
    source: IndexAuditSnapshot,
) -> io::Result<()> {
    control.check(Some(&source))?;
    source.check_available_space(
        plan.minimum_free_bytes
            .checked_add(plan.max_manifest_bytes)
            .ok_or_else(|| io::Error::other("index audit headroom overflow"))?,
    )?;
    report.phase = "verifying_captured_publications";
    report.source_prefix_fingerprint = Some(source.source_prefix_fingerprint());
    report.captured_segments = source.segment_count();
    report.captured_physical_rows = source.physical_rows();
    save_json(directory, "progress.json", report, true)?;
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(directory.join("segments.jsonl"))?;
    let mut digest = blake3::Hasher::new();
    digest.update(b"logex.index-audit.segments.v1\0");
    source.visit_index_sources(&|| control.cancelled(&source), |item| {
        control.check(Some(&source))?;
        let proof = IndexBuilder::verify_captured_indexes(
            item.reader,
            item.index_directory,
            IndexBuildProfile::Events,
            plan.max_index_logical_bytes_per_segment,
            &|| control.cancelled(&source),
        )
        .map_err(io::Error::other)?;
        control.check(Some(&source))?;
        append_segment(
            &mut file,
            &mut digest,
            report,
            plan.max_manifest_bytes,
            item.segment_id,
            item.minimum_rows,
            &proof,
        )?;
        report.updated_unix_ms = now_ms()?;
        report.elapsed_seconds = started.elapsed().as_secs_f64();
        save_json(directory, "progress.json", report, true)
    })?;
    control.check(Some(&source))?;
    if report.minimum_rows_covered != source.physical_rows() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index audit did not cover every selected row",
        ));
    }
    file.sync_all()?;
    File::open(directory)?.sync_all()?;
    // Re-admit under the ordinary storage read guard only after slow verification
    // and report I/O finish. The report describes this admission instant.
    let _storage = state.storage.blocking_read();
    source.validate()?;
    report.manifest_blake3 = Some(B256::from(*digest.finalize().as_bytes()));
    report.complete = true;
    report.phase = "captured_publications_verified";
    Ok(())
}

fn append_segment(
    file: &mut File,
    digest: &mut blake3::Hasher,
    report: &mut Report,
    limit: u64,
    id: u64,
    minimum_rows: u64,
    proof: &CapturedIndexVerification,
) -> io::Result<()> {
    if proof.rows < minimum_rows {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "verified index source omits selected rows",
        ));
    }
    let record = SegmentRecord {
        segment_id: id,
        minimum_rows,
        verified_rows: proof.rows,
        source_namespace: proof.source_namespace.into(),
        source_commitment: proof.source_commitment.into(),
        artifacts: proof
            .artifacts
            .iter()
            .map(|a| ArtifactRecord {
                name: &a.name,
                file_id: a.file_id.into(),
                logical_bytes: a.logical_bytes,
            })
            .collect(),
    };
    let mut bytes = serde_json::to_vec(&record).map_err(io::Error::other)?;
    bytes.push(b'\n');
    let length = report
        .manifest_bytes
        .checked_add(bytes.len() as u64)
        .filter(|n| *n <= limit)
        .ok_or_else(|| io::Error::other("index audit manifest byte allowance exceeded"))?;
    let covered = report
        .minimum_rows_covered
        .checked_add(minimum_rows)
        .ok_or_else(|| io::Error::other("index audit selected row count overflow"))?;
    let rows = report
        .verified_rows_in_opened_prefixes
        .checked_add(proof.rows)
        .ok_or_else(|| io::Error::other("index audit opened row count overflow"))?;
    let logical = report
        .verified_index_logical_bytes
        .checked_add(proof.logical_bytes)
        .ok_or_else(|| io::Error::other("index audit logical byte count overflow"))?;
    file.write_all(&bytes)?;
    digest.update(&bytes);
    report.manifest_bytes = length;
    report.minimum_rows_covered = covered;
    report.verified_rows_in_opened_prefixes = rows;
    report.verified_index_logical_bytes = logical;
    report.verified_nonempty_segments += 1;
    Ok(())
}

#[cfg(test)]
mod tests;
