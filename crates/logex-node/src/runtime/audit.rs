//! Explicit finite audit work. Ordinary startup never creates or resumes a job.
//! Audit errors are reported without stopping the healthy sync/query service.
use alloy_primitives::B256;
use logex_cl::{ConsensusStore, FinalizedAuditAnchor};
use logex_server::AppState;
use logex_storage::native::PrimaryAuditSnapshot;
use logex_sync::{
    history_audit::{
        AuditManifest, AuditNetworkClient, AuditNetworkMetrics, AuditRange, AuditSession,
    },
    repair::{RepairFetchError, RepairFetchErrorKind, RepairFetchStep, RepairFetcher, RepairRange},
};
use logex_types::{ExecutionAnchor, WeakSubjectivityCheckpoint};
use reth_chainspec::{EthChainSpec, MAINNET};
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    fs::{self, File},
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

mod files;
mod plan;
pub(crate) use files::cancel_request;
use files::{
    cancel_path, cancellation_recorded, create_private_directory, ensure_directory,
    ordinary_directory, read_json, save_json, session_directory,
};
pub(crate) use plan::Invocation;
use plan::Scope;

#[cfg(test)]
mod tests;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    version: u32,
    request_id: B256,
    scope: Scope,
    range: AuditRange,
    anchor: ExecutionAnchor,
    checkpoint: WeakSubjectivityCheckpoint,
    journal: logex_sync::history_audit::AuditJournalLimits,
}

#[derive(Serialize)]
struct TransientFailure {
    unix_ms: u64,
    next_block: Option<u64>,
    error: String,
}

#[derive(Serialize)]
struct Report {
    request_id: B256,
    phase: &'static str,
    started_unix_ms: u64,
    updated_unix_ms: u64,
    elapsed_seconds: f64,
    source_scan_seconds: Option<f64>,
    restored_blocks: u64,
    compared_or_validated_blocks: u64,
    /// Durable journal prefix, excluding any in-memory comparisons. No pilot journal.
    checkpointed_blocks: Option<u64>,
    events: u64,
    transient_retries: u32,
    transient_failures: Vec<TransientFailure>,
    next_block: Option<u64>,
    network: AuditNetworkMetrics,
    node_payload_bytes_before: Option<u64>,
    node_payload_bytes_after: Option<u64>,
    descriptor: Option<Descriptor>,
    manifest: Option<logex_sync::history_audit::AuditManifestSummary>,
    comparison: Option<logex_sync::history_audit::ReceiptComparisonReport>,
    finalized_anchor_readmitted: bool,
    complete_dataset_acceptance: bool,
    error: Option<String>,
    limitations: Vec<&'static str>,
}

struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// Own both the finite blocking worker and its shutdown observation. A normal
/// audit failure/completion is not a fatal long-lived node-worker failure.
pub(super) fn spawn(
    invocation: Invocation,
    data_dir: PathBuf,
    state: Arc<AppState>,
    consensus: Arc<ConsensusStore>,
    client: AuditNetworkClient,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let cancellation = CancellationToken::new();
        let _cancel = CancelOnDrop(cancellation.clone());
        if *shutdown.borrow() {
            return;
        }
        let worker_cancel = cancellation.clone();
        let runtime = tokio::runtime::Handle::current();
        let mut worker = tokio::task::spawn_blocking(move || {
            run(
                invocation,
                data_dir,
                state,
                consensus,
                client,
                worker_cancel,
                runtime,
            )
        });
        let result = tokio::select! {
            biased;
            result = &mut worker => result,
            _ = async {
                while !*shutdown.borrow_and_update() {
                    if shutdown.changed().await.is_err() { break; }
                }
            } => {
                cancellation.cancel();
                worker.await
            }
        };
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                tracing::warn!(%error, "one-time history audit stopped")
            }
            Err(error) => {
                tracing::error!(%error, "one-time history audit worker failed")
            }
        }
    })
}

struct Control {
    cancellation: CancellationToken,
    deadline: Instant,
    source: PrimaryAuditSnapshot,
    minimum_free_bytes: u64,
    cancel_path: PathBuf,
    request_id: B256,
    last_space: Cell<Instant>,
    calls: Cell<usize>,
    resource_error: RefCell<Option<String>>,
}
impl Control {
    fn check(&self) -> io::Result<()> {
        self.source.validate()?;
        if let Some(error) = self.resource_error.borrow().as_ref() {
            return Err(io::Error::other(error.clone()));
        }
        if self.cancellation.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "audit invocation cancelled",
            ));
        }
        if Instant::now() >= self.deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "audit invocation deadline exceeded",
            ));
        }
        if self.last_space.get().elapsed() >= Duration::from_secs(5) {
            self.last_space.set(Instant::now());
            if cancellation_recorded(&self.cancel_path, self.request_id)? {
                self.cancellation.cancel();
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "audit cancellation requested",
                ));
            }
            if let Err(error) = self.source.check_available_space(self.minimum_free_bytes) {
                self.resource_error.replace(Some(error.to_string()));
                return Err(error);
            }
        }
        Ok(())
    }
    fn retry_pause(&self, runtime: &tokio::runtime::Handle, delay: Duration) -> io::Result<()> {
        let until = Instant::now()
            .checked_add(delay)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "retry delay overflow"))?
            .min(self.deadline);
        loop {
            self.check()?;
            let remaining = until.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(());
            }
            runtime.block_on(async {
                tokio::select! {
                    _ = self.cancellation.cancelled() => {},
                    _ = tokio::time::sleep(remaining.min(Duration::from_secs(5))) => {},
                }
            });
        }
    }

    fn cancelled(&self) -> bool {
        if self.cancellation.is_cancelled() {
            return true;
        }
        let calls = self.calls.get().wrapping_add(1);
        self.calls.set(calls);
        // Physical scans invoke this for every row. Check the clock/filesystem
        // periodically, without making billions of time/syscall requests.
        if calls.is_multiple_of(16_384) {
            return self.check().is_err();
        }
        self.resource_error.borrow().is_some()
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
fn node_bytes(state: &AppState) -> Option<u64> {
    state
        .sync_status
        .lock()
        .unwrap()
        .execution_network
        .as_ref()
        .map(|n| n.p2p_downloaded_payload_bytes)
}
fn unavailable(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::WouldBlock, message)
}

fn run(
    invocation: Invocation,
    data_dir: PathBuf,
    state: Arc<AppState>,
    consensus: Arc<ConsensusStore>,
    mut client: AuditNetworkClient,
    cancellation: CancellationToken,
    runtime: tokio::runtime::Handle,
) -> eyre::Result<()> {
    let started = Instant::now();
    let started_ms = now_ms()?;
    let deadline = started
        .checked_add(Duration::from_secs(invocation.plan.deadline_secs))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "audit deadline overflow"))?;
    let mut report = Report {
        request_id: invocation.plan.request_id,
        phase: "waiting_for_verified_history",
        started_unix_ms: started_ms,
        updated_unix_ms: started_ms,
        elapsed_seconds: 0.0,
        source_scan_seconds: None,
        restored_blocks: 0,
        compared_or_validated_blocks: 0,
        checkpointed_blocks: None,
        events: 0,
        transient_retries: 0,
        transient_failures: Vec::new(),
        next_block: None,
        network: client.metrics(),
        node_payload_bytes_before: node_bytes(&state),
        node_payload_bytes_after: None,
        descriptor: None,
        manifest: None,
        comparison: None,
        finalized_anchor_readmitted: false,
        complete_dataset_acceptance: false,
        error: None,
        limitations: vec![
            "An explicit pilot validates payload commitments and measures fetch work; it does not audit every stored event.",
            "A comparison covers its frozen finalized range only. Derived indexes, advancing-tail reconciliation and overall release acceptance remain separate.",
            "Returned-prefix RLP counters exclude transport, requests, discarded retries and residuals; receipt blooms may be reconstructed. Node counters also include concurrent normal sync. Neither is billed wire traffic.",
            "Source/artifact budgets are finite work allowances, not whole-process RSS or filesystem allocation reservations.",
            "Resume reuses prior private local verified evidence after source/ancestry checks. Journal checksums are not portable Ethereum receipt proofs.",
        ],
    };
    let mut output = None;
    let outcome = (|| -> eyre::Result<()> {
        let (source, anchor, descriptor, root) = prepare(
            &invocation,
            &data_dir,
            &state,
            &consensus,
            &cancellation,
            deadline,
            &runtime,
        )?;
        output = Some(root.clone());
        report.descriptor = Some(descriptor.clone());
        report.next_block = Some(descriptor.range.through);
        report.node_payload_bytes_before = node_bytes(&state);
        let control = Control {
            cancellation: cancellation.clone(),
            deadline,
            source,
            minimum_free_bytes: invocation.plan.minimum_free_bytes,
            cancel_path: cancel_path(&data_dir, invocation.plan.request_id),
            request_id: invocation.plan.request_id,
            last_space: Cell::new(Instant::now()),
            calls: Cell::new(0),
            resource_error: RefCell::new(None),
        };
        control
            .source
            .check_available_space(control.minimum_free_bytes)?;
        let work = Work {
            invocation: &invocation,
            descriptor: &descriptor,
            root: &root,
            state: &state,
            consensus: &consensus,
            anchor,
            control: &control,
            runtime: &runtime,
            started,
        };
        let result = match invocation.plan.scope {
            Scope::Pilot => work.pilot(&mut client, &mut report),
            Scope::Audit { .. } => work.audit(&mut client, &mut report),
        };
        // Expose a resource/deadline cause rather than just the scanner's generic
        // cooperative-interruption error. A changed source has precedence.
        control.check()?;
        result
    })();
    report.updated_unix_ms = now_ms()?;
    report.elapsed_seconds = started.elapsed().as_secs_f64();
    report.network = client.metrics();
    report.node_payload_bytes_after = node_bytes(&state);
    if let Err(error) = &outcome {
        report.phase = if cancellation.is_cancelled() {
            "cancelled"
        } else {
            "failed"
        };
        report.error = Some(format!("{error:#}").chars().take(4096).collect());
    }
    if let Some(root) = output {
        save_json(
            &root,
            &format!("invocation-{started_ms}.json"),
            &report,
            false,
        )?;
        save_json(&root, "progress.json", &report, true)?;
        tracing::info!(request_id=%report.request_id,phase=report.phase,blocks=report.compared_or_validated_blocks,path=%root.display(),"one-time history audit invocation ended");
    }
    outcome
}

fn prepare(
    invocation: &Invocation,
    data_dir: &Path,
    state: &AppState,
    consensus: &ConsensusStore,
    cancel: &CancellationToken,
    deadline: Instant,
    runtime: &tokio::runtime::Handle,
) -> eyre::Result<(
    PrimaryAuditSnapshot,
    FinalizedAuditAnchor,
    Descriptor,
    PathBuf,
)> {
    let parent = data_dir.join("history-audits");
    ensure_directory(&parent)?;
    let root = parent.join(format!("request-{:x}", invocation.plan.request_id));
    let saved = if invocation.resume {
        ordinary_directory(&root)?;
        let descriptor: Descriptor = read_json(&root.join("request.json"))?;
        validate_resume(&descriptor, &invocation.plan)?;
        Some(descriptor)
    } else {
        match fs::symlink_metadata(&root) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(_) => {
                eyre::bail!("audit request already exists; use explicit resume to continue it");
            }
        }
        None
    };
    let cancellation_path = cancel_path(data_dir, invocation.plan.request_id);
    if invocation.resume && cancellation_recorded(&cancellation_path, invocation.plan.request_id)? {
        // Explicit resume revokes only this request's validated cancel marker.
        fs::remove_file(&cancellation_path)?;
        File::open(&parent)?.sync_all()?;
    }
    loop {
        if cancellation_recorded(&cancellation_path, invocation.plan.request_id)? {
            cancel.cancel();
        }
        if cancel.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "audit cancelled before preparation",
            )
            .into());
        }
        eyre::ensure!(Instant::now() < deadline, "audit startup deadline exceeded");
        let healthy = {
            let s = state.sync_status.lock().unwrap();
            s.consensus_head_fresh == Some(true)
                && s.connected_peers > 0
                && s.current_block.saturating_add(2) >= s.target_block
        };
        if healthy {
            let storage = state.storage.blocking_read();
            if let Some(coverage) = storage.verified_log_coverage()
                && coverage.from.block_number == 0
                && coverage.from.block_hash == MAINNET.genesis_hash()
            {
                let anchor = match &saved {
                    Some(d) => consensus.restore_finalized_audit_anchor(d.checkpoint, d.anchor),
                    None => consensus.finalized_audit_anchor(),
                };
                if let Some(anchor) = anchor
                    && coverage.to.block_number >= anchor.execution().block_number
                {
                    let descriptor = match saved.clone() {
                        Some(d) => d,
                        None => {
                            let through = anchor.execution().block_number;
                            let from = match invocation.plan.scope {
                                Scope::Pilot => through
                                    .checked_add(1)
                                    .and_then(|v| v.checked_sub(invocation.plan.new_blocks))
                                    .ok_or_else(|| {
                                        io::Error::new(
                                            io::ErrorKind::InvalidInput,
                                            "pilot exceeds available history",
                                        )
                                    })?,
                                Scope::Audit { from_block } => from_block,
                            };
                            let headers = through
                                .checked_sub(from)
                                .and_then(|v| v.checked_add(1))
                                .ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        "invalid frozen audit range",
                                    )
                                })?;
                            eyre::ensure!(
                                headers <= invocation.plan.fetch.headers,
                                "frozen audit range exceeds the explicit header allowance"
                            );
                            Descriptor {
                                version: 1,
                                request_id: invocation.plan.request_id,
                                scope: invocation.plan.scope.clone(),
                                range: AuditRange { from, through },
                                anchor: anchor.execution(),
                                checkpoint: anchor.checkpoint(),
                                journal: invocation.plan.journal,
                            }
                        }
                    };
                    eyre::ensure!(
                        descriptor.range.through == anchor.execution().block_number,
                        "audit descriptor upper bound is not the frozen anchor"
                    );
                    let source = storage.primary_audit_snapshot(invocation.plan.source_limits())?;
                    drop(storage);
                    source.check_available_space(invocation.plan.minimum_free_bytes)?;
                    if !invocation.resume {
                        ensure_directory(&parent)?;
                        create_private_directory(&root)?;
                        File::open(&parent)?.sync_all()?;
                        save_json(&root, "request.json", &descriptor, false)?;
                        save_json(&root, "initial-plan.json", &invocation.plan, false)?;
                    }
                    return Ok((source, anchor, descriptor, root));
                }
            }
        }
        runtime.block_on(async {
            tokio::select! {
                _ = cancel.cancelled() => {},
                _ = tokio::time::sleep(Duration::from_millis(500)) => {},
            }
        });
    }
}

struct Work<'a> {
    invocation: &'a Invocation,
    descriptor: &'a Descriptor,
    root: &'a Path,
    state: &'a AppState,
    consensus: &'a ConsensusStore,
    anchor: FinalizedAuditAnchor,
    control: &'a Control,
    runtime: &'a tokio::runtime::Handle,
    started: Instant,
}

impl Work<'_> {
    fn pilot(&self, client: &mut AuditNetworkClient, report: &mut Report) -> eyre::Result<()> {
        let Self {
            invocation,
            descriptor,
            root,
            state,
            consensus,
            anchor,
            control,
            runtime,
            started,
        } = *self;
        let plan = &invocation.plan;
        report.phase = "fetch_cost_pilot";
        progress(root, report, client, state, started)?;
        let limits = plan.fetch_limits(tokio::time::Instant::from_std(control.deadline));
        let mut fetcher = RepairFetcher::new(
            RepairRange {
                start: descriptor.range.from,
                end: descriptor.range.through,
            },
            descriptor.anchor,
            limits,
            control.cancellation.clone(),
        )?;
        loop {
            control.check()?;
            match runtime.block_on(fetcher.next_block(client))? {
                RepairFetchStep::Block(block) => {
                    report.compared_or_validated_blocks += 1;
                    report.events = report
                        .events
                        .checked_add(block.rows().len() as u64)
                        .ok_or_else(|| io::Error::other("pilot event count overflow"))?;
                    report.next_block = block
                        .header()
                        .number
                        .checked_sub(1)
                        .filter(|n| *n >= descriptor.range.from);
                }
                RepairFetchStep::Complete(completed) => {
                    eyre::ensure!(
                        completed.anchor() == descriptor.anchor
                            && completed.delivered_blocks() == plan.new_blocks,
                        "pilot completion does not cover its declared range"
                    );
                    break;
                }
            }
            if report
                .compared_or_validated_blocks
                .is_multiple_of(plan.journal.checkpoint_blocks as u64)
            {
                progress(root, report, client, state, started)?;
            }
        }
        let _storage = state.storage.blocking_read();
        control.source.validate()?;
        consensus
            .with_finalized_audit_anchor(&anchor, || control.source.validate())
            .ok_or_else(|| {
                unavailable("audit finalized anchor changed before pilot admission")
            })??;
        report.finalized_anchor_readmitted = true;
        report.phase = "pilot_complete";
        Ok(())
    }

    fn audit(&self, client: &mut AuditNetworkClient, report: &mut Report) -> eyre::Result<()> {
        let Self {
            invocation,
            descriptor,
            root,
            state,
            consensus,
            anchor,
            control,
            runtime,
            started,
        } = *self;
        let plan = &invocation.plan;
        report.phase = "scanning_complete_physical_source";
        progress(root, report, client, state, started)?;
        let reserve = plan
            .minimum_free_bytes
            .checked_add(plan.manifest.scratch_bytes)
            .ok_or_else(|| io::Error::other("audit headroom allowance overflow"))?;
        control.source.check_available_space(reserve)?;
        let scan_started = Instant::now();
        let result = AuditManifest::build(
            control.source.clone(),
            descriptor.range,
            root,
            plan.manifest_limits(),
            &|| control.cancelled(),
        );
        control.check()?;
        let manifest = result?;
        report.source_scan_seconds = Some(scan_started.elapsed().as_secs_f64());
        report.manifest = Some(manifest.summary().clone());
        let existing = session_directory(root)?;
        let mut session = match existing {
            Some(path) => {
                eyre::ensure!(invocation.resume, "audit session already exists");
                AuditSession::resume(&path, &manifest, anchor, &|| control.cancelled())?
            }
            None => AuditSession::create(root, &manifest, anchor, plan.journal)?,
        };
        report.restored_blocks = session.compared_blocks();
        report.checkpointed_blocks = Some(session.checkpointed_blocks());
        report.compared_or_validated_blocks = session.compared_blocks();
        report.events = session.compared_events();
        report.next_block = session.next_block_number();
        report.phase = "comparing_complete_receipts";
        progress(root, report, client, state, started)?;
        let mut fetcher = session.fetcher(
            plan.fetch_limits(tokio::time::Instant::from_std(control.deadline)),
            control.cancellation.clone(),
        )?;
        let result = (|| -> eyre::Result<()> {
            while session.next_block_number().is_some()
                && session.compared_blocks() - report.restored_blocks < plan.new_blocks
            {
                control.check()?;
                let step = match runtime.block_on(fetcher.next_block(client)) {
                    Ok(step) => step,
                    Err(error) => {
                        control.check()?;
                        if report.transient_retries >= plan.fetch.max_transient_retries
                            || !error
                                .downcast_ref::<RepairFetchError>()
                                .is_some_and(|cause| {
                                    cause.kind == RepairFetchErrorKind::Unavailable
                                })
                        {
                            return Err(error);
                        }
                        let failure = TransientFailure {
                            unix_ms: now_ms()?,
                            next_block: session.next_block_number(),
                            error: format!("{error:#}").chars().take(2048).collect(),
                        };
                        fetcher = session.retry_unavailable(
                            error,
                            plan.fetch_limits(tokio::time::Instant::from_std(control.deadline)),
                            control.cancellation.clone(),
                        )?;
                        report.transient_retries += 1;
                        report.transient_failures.push(failure);
                        report.checkpointed_blocks = Some(session.checkpointed_blocks());
                        report.phase = "waiting_to_retry_unavailable_peer";
                        progress(root, report, client, state, started)?;
                        control.retry_pause(
                            runtime,
                            Duration::from_secs(plan.fetch.retry_delay_secs),
                        )?;
                        report.phase = "comparing_complete_receipts";
                        continue;
                    }
                };
                let RepairFetchStep::Block(block) = step else {
                    eyre::bail!("audit fetch ended before physical comparison");
                };
                session.compare(&block)?;
                report.compared_or_validated_blocks = session.compared_blocks();
                report.checkpointed_blocks = Some(session.checkpointed_blocks());
                report.events = session.compared_events();
                report.next_block = session.next_block_number();
                if (session.compared_blocks() - report.restored_blocks)
                    .is_multiple_of(plan.journal.checkpoint_blocks as u64)
                {
                    progress(root, report, client, state, started)?;
                }
            }
            Ok(())
        })();
        // Preserve complete comparisons even on a bounded fetch failure/deadline.
        // A source change or checkpoint I/O failure prevents further reuse here.
        let saved = session.checkpoint();
        report.checkpointed_blocks = Some(session.checkpointed_blocks());
        if let Err(error) = result {
            if let Err(checkpoint) = saved {
                return Err(error.wrap_err(format!("audit checkpoint also failed: {checkpoint}")));
            }
            return Err(error);
        }
        saved?;
        if session.next_block_number().is_none() {
            let report_result = session.finish()?;
            let _storage = state.storage.blocking_read();
            manifest.validate()?;
            consensus
                .with_finalized_audit_anchor(&anchor, || manifest.validate())
                .ok_or_else(|| {
                    unavailable("audit finalized anchor changed before completion admission")
                })??;
            report.comparison = Some(report_result);
            report.finalized_anchor_readmitted = true;
            report.phase = "frozen_range_compared";
        } else {
            report.phase = "paused_at_invocation_budget";
        }
        Ok(())
    }
}

fn validate_resume(descriptor: &Descriptor, plan: &plan::Plan) -> eyre::Result<()> {
    eyre::ensure!(
        descriptor.version == 1
            && descriptor.request_id == plan.request_id
            && descriptor.scope == plan.scope
            && descriptor.journal == plan.journal,
        "audit resume plan does not match its frozen request"
    );
    let Scope::Audit { from_block } = descriptor.scope else {
        eyre::bail!("fetch-cost pilots are finite; use a new request ID for another pilot");
    };
    eyre::ensure!(
        from_block == descriptor.range.from
            && descriptor.range.through == descriptor.anchor.block_number,
        "audit descriptor bounds do not match its frozen request"
    );
    let headers = descriptor
        .range
        .through
        .checked_sub(from_block)
        .and_then(|n| n.checked_add(1))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid frozen audit range"))?;
    eyre::ensure!(
        headers <= plan.fetch.headers,
        "frozen audit range exceeds the explicit header allowance"
    );
    Ok(())
}

fn progress(
    root: &Path,
    report: &mut Report,
    client: &AuditNetworkClient,
    state: &AppState,
    started: Instant,
) -> io::Result<()> {
    report.updated_unix_ms = now_ms()?;
    report.elapsed_seconds = started.elapsed().as_secs_f64();
    report.network = client.metrics();
    report.node_payload_bytes_after = node_bytes(state);
    save_json(root, "progress.json", report, true)
}
