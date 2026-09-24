//! Exclusive offline maintenance before normal storage, ingestion or query startup.
use std::{
    future::Future,
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use eyre::{Result, WrapErr, ensure};
use futures_util::FutureExt;
use logex_index::IndexBuildProfile;
use logex_server::{HttpServerConfig, MaintenanceState, RepairPhase};
use logex_storage::native::StorageCatalogPaths;
use logex_sync::{
    repair::{
        RepairAssessment, RepairExecutionOutcome, RepairExecutionPhase, assess_repair,
        execute_repair_with_provider,
    },
    tasks::TaskMonitor,
};
use tokio::{sync::watch, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

use crate::{
    cli::RepairLimitsArgs,
    volume::{ExpectedVolume, StorageMonitor},
};

mod limits;
mod network;
mod report;
#[cfg(test)]
mod tests;

pub(crate) use network::RepairNetworkOptions;

const PROFILE: IndexBuildProfile = IndexBuildProfile::All;
const WORKER_STOP_TIMEOUT: Duration = Duration::from_secs(120);
const HTTP_STOP_TIMEOUT: Duration = Duration::from_secs(30);

pub(crate) struct RunRepairOptions {
    pub root: PathBuf,
    pub limits: RepairLimitsArgs,
    pub http_address: SocketAddr,
    pub http: HttpServerConfig,
    pub network: RepairNetworkOptions,
}

/// Absence is fresh only if there is no initialized data or pending evidence.
/// This probe never creates a path; ordinary startup remains its exclusive owner.
pub(super) fn needs_startup_assessment(root: &Path) -> io::Result<bool> {
    let metadata = match std::fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "automatic repair requires a real data directory",
        ));
    }
    let catalog = StorageCatalogPaths::new(root.to_owned()).catalog_path();
    match std::fs::symlink_metadata(catalog) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            if std::fs::read_dir(root)?.next().transpose()?.is_some() {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "data directory has artifacts but no catalog; repair cannot determine original ranges; preserve its contents for inspection",
                ))
            } else {
                Ok(false)
            }
        }
        Err(error) => Err(error),
    }
}

/// No ordinary storage open, network initialization, write probe or file creation.
/// The report is an assessment of local evidence, not proof of chain completeness.
pub(crate) fn inspect(root: &Path, args: &RepairLimitsArgs) -> Result<RepairAssessment> {
    let limits = limits::execution_limits(args)?;
    let assessment = assess_repair(root, limits.assessment, PROFILE)
        .wrap_err("inspect existing storage without modification")?;
    ensure!(
        Instant::now() < limits.fetch.deadline,
        "repair inspection exceeded its time allowance"
    );
    Ok(assessment)
}

pub(crate) fn run_dry_run(
    root: &Path,
    args: &RepairLimitsArgs,
    volume: Option<&ExpectedVolume>,
) -> Result<i32> {
    let assessment = inspect(root, args);
    // Recheck even after an assessment error. Never report a stale successful
    // identity if the expected mount disappeared while files were being read.
    if let Some(volume) = volume {
        volume
            .check_read_only()
            .wrap_err("recheck expected volume after inspection")?;
    }
    let assessment = assessment?;
    let report = report::assessment_report(assessment.report());
    print_report(&report)?;
    Ok(report::report_exit_code(assessment.report()))
}

pub(crate) fn print_outcome(outcome: &RepairExecutionOutcome) -> Result<()> {
    let mut report = report::assessment_report(outcome.assessment.report());
    report["quarantine_dirs"] = serde_json::to_value(&outcome.quarantine_dirs)?;
    report["recovered_pending_storage"] = outcome.recovered_pending_storage.into();
    print_report(&report)
}

fn print_report(report: &serde_json::Value) -> Result<()> {
    use io::Write;
    let mut output = io::stdout().lock();
    serde_json::to_writer_pretty(&mut output, report)?;
    writeln!(output)?;
    Ok(())
}

/// The same coordinator backs manual repair and opt-in startup repair. All CPU,
/// file and exclusive storage work lives on one blocking worker. Its runtime
/// handle drives network futures while HTTP and shutdown remain responsive.
pub(crate) async fn run_maintenance(
    options: RunRepairOptions,
    storage_monitor: &mut Option<StorageMonitor>,
    mut signal: std::pin::Pin<&mut impl Future<Output = io::Result<&'static str>>>,
) -> Result<RepairExecutionOutcome> {
    let limits = limits::execution_limits(&options.limits)?;
    ensure!(
        needs_startup_assessment(&options.root)?,
        "repair requires an existing initialized data directory; no storage was created"
    );
    let state = Arc::new(MaintenanceState::new(RepairPhase::Inspecting));
    let listener = tokio::net::TcpListener::bind(options.http_address)
        .await
        .wrap_err("bind maintenance HTTP listener before repair")?;

    let (failures, failure_rx) = watch::channel(None);
    let monitor =
        super::storage_health::monitor_initialized_storage(storage_monitor, &options.root)?;
    // Create every fallible independent watchdog before spawning HTTP or the
    // blocking writer. Setup errors must not detach already-running work.
    let workers = TaskMonitor::default();
    let _server_watchdog = super::start_runtime_failure_watchdog(
        workers.subscribe(),
        super::RUNTIME_FAILURE_CLEANUP_GRACE,
        || std::process::exit(1),
    )?;
    let _failure_watchdog = super::start_runtime_failure_watchdog(
        failure_rx.clone(),
        super::RUNTIME_FAILURE_CLEANUP_GRACE,
        || std::process::exit(1),
    )?;
    // This stage guard disarms only after maintenance workers and HTTP join.
    // Manual callers retain their whole-runtime guard through owner destruction.
    let (stopping, stop_rx) = watch::channel(None);
    let _stage_watchdog = super::start_runtime_failure_watchdog(
        stop_rx,
        super::RUNTIME_FAILURE_CLEANUP_GRACE,
        || std::process::exit(1),
    )?;
    let failed_state = Arc::clone(&state);
    let storage_failures = failures.clone();
    monitor.set_failure_handler(move |reason| {
        latch_failure(&storage_failures, Arc::from(reason));
        failed_state.fail(reason);
    })?;

    let (http_shutdown, http_rx) = watch::channel(false);
    let http = workers.spawn_result(
        "maintenance HTTP server",
        logex_server::serve_maintenance(Arc::clone(&state), listener, http_rx, options.http),
    );

    let cancellation = CancellationToken::new();
    let work_cancel = cancellation.clone();
    let work_state = Arc::clone(&state);
    let runtime = tokio::runtime::Handle::current();
    let mut worker = tokio::task::spawn_blocking(move || {
        let assessment = assess_repair(&options.root, limits.assessment, PROFILE)
            .wrap_err("assess repair under exclusive directory ownership")?;
        // Execution consumes its assessment even on error. Peer shutdown may
        // still persist cache state, so retain the same directory owner until
        // that cleanup has acknowledged completion.
        let _directory = assessment.retain_directory();
        let mut consensus = network::RetainedConsensus::new(options.root.clone());
        let mut source = network::LazyRepairSource::new(
            options.root,
            options.network,
            consensus.shared(),
            failures,
        );
        runtime.block_on(async {
            let result = catch_repair_unwind(execute_repair_with_provider(
                assessment,
                limits,
                PROFILE,
                &mut source,
                &mut consensus,
                work_cancel,
                |phase| work_state.set_phase(server_phase(phase)),
            ))
            .await;
            // No `?` may bypass network cleanup, including absent anchors,
            // cancellation, verification errors or partially completed repair.
            let cleanup = match std::panic::AssertUnwindSafe(source.shutdown())
                .catch_unwind()
                .await
            {
                Ok(cleanup) => cleanup,
                // A cleanup unwind cannot prove all peer writers stopped.
                // Terminate while the directory guard still owns the lock.
                Err(_) => std::process::exit(1),
            };
            combine_cleanup(result, cleanup)
        })
    });

    let mut server_failure = Some(workers.subscribe());
    let mut failure = Some(failure_rx);
    let mut result = supervise_worker(
        &mut worker,
        WorkerSupervision {
            cancellation: &cancellation,
            stopping: &stopping,
            state: &state,
            deadline: limits.fetch.deadline,
            failure: &mut failure,
            server_failure: &mut server_failure,
        },
        signal.as_mut(),
    )
    .await;
    workers.begin_shutdown();
    http_shutdown.send_replace(true);
    if let Err(error) = finish_http(http).await {
        result = combine_cleanup(result, Err(error));
    }
    // A concurrently latched failure always overrides worker success, including
    // failures arriving while HTTP drains. This also atomically checks the
    // monitor before handing its callback to ordinary startup.
    for receiver in [&failure, &server_failure].into_iter().flatten() {
        if let Some(error) = receiver.borrow().clone() {
            result = combine_cleanup(result, Err(eyre::eyre!(error.to_string())));
        }
    }
    result = combine_cleanup(result, monitor.clear_failure_handler().map_err(Into::into));
    // Keep the same registered signal future through listener teardown. A stop
    // delivered during HTTP drain must not disappear at the auto-start handoff.
    if result.is_ok() {
        if let Some(signal) = signal.as_mut().now_or_never() {
            result = Err(signal_stop_error(signal));
        } else if Instant::now() >= limits.fetch.deadline {
            result = Err(eyre::eyre!(
                "repair time allowance expired during maintenance cleanup"
            ));
        }
    }
    if let Err(error) = &result {
        state.fail(format!("{error:#}"));
    }
    result
}

fn server_phase(phase: RepairExecutionPhase) -> RepairPhase {
    match phase {
        RepairExecutionPhase::Recovering => RepairPhase::Recovering,
        RepairExecutionPhase::RebuildingIndexes => RepairPhase::RebuildingIndexes,
        RepairExecutionPhase::Reconstructing => RepairPhase::Reconstructing,
        RepairExecutionPhase::Verifying => RepairPhase::Verifying,
    }
}

fn latch_failure(sender: &watch::Sender<Option<Arc<str>>>, reason: Arc<str>) {
    sender.send_if_modified(|current| {
        if current.is_some() {
            false
        } else {
            *current = Some(reason);
            true
        }
    });
}

fn combine_cleanup<T>(result: Result<T>, cleanup: Result<()>) -> Result<T> {
    match (result, cleanup) {
        (result, Ok(())) => result,
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => {
            Err(error.wrap_err(format!("repair cleanup also failed: {cleanup:#}")))
        }
    }
}

async fn catch_repair_unwind<T>(work: impl Future<Output = Result<T>>) -> Result<T> {
    match std::panic::AssertUnwindSafe(work).catch_unwind().await {
        Ok(result) => result,
        Err(_) => Err(eyre::eyre!(
            "repair execution panicked; retain recovery evidence and inspect before retrying"
        )),
    }
}

struct WorkerSupervision<'a> {
    cancellation: &'a CancellationToken,
    stopping: &'a watch::Sender<Option<Arc<str>>>,
    state: &'a MaintenanceState,
    deadline: Instant,
    failure: &'a mut Option<watch::Receiver<Option<Arc<str>>>>,
    server_failure: &'a mut Option<watch::Receiver<Option<Arc<str>>>>,
}

fn signal_stop_error(signal: io::Result<&'static str>) -> eyre::Report {
    match signal {
        Ok(signal) => eyre::eyre!(
            "repair interrupted by {signal}; retain journals and rerun repair to resume"
        ),
        Err(error) => eyre::Report::new(error).wrap_err("cannot supervise repair shutdown signals"),
    }
}

async fn supervise_worker<T>(
    worker: &mut JoinHandle<Result<T>>,
    supervision: WorkerSupervision<'_>,
    signal: impl Future<Output = io::Result<&'static str>>,
) -> Result<T> {
    let WorkerSupervision {
        cancellation,
        stopping,
        state,
        deadline,
        failure,
        server_failure,
    } = supervision;
    let interrupted = tokio::select! {
        biased;
        reason = super::wait_for_runtime_failure(failure) => Some(eyre::eyre!("repair runtime failure: {reason}")),
        reason = super::wait_for_runtime_failure(server_failure) => Some(eyre::eyre!("maintenance HTTP failure: {reason}")),
        signal = signal => Some(signal_stop_error(signal)),
        _ = tokio::time::sleep_until(deadline) => Some(eyre::eyre!("repair time allowance expired; retain journals and rerun with --repair-timeout-secs if needed")),
        result = &mut *worker => {
            // Arm before logging, status locks or cleanup, including clean
            // completion. The stage guard disarms only after joined teardown.
            stopping.send_replace(Some(Arc::from("repair maintenance finishing")));
            let result = result.wrap_err("maintenance worker failed").and_then(|result| result);
            if let Err(error) = &result { state.fail(format!("{error:#}")); }
            return result;
        }
    };
    let error = interrupted.expect("only the joined worker branch returns early");
    let reason = format!("{error:#}");
    stopping.send_replace(Some(Arc::from(reason.as_str())));
    cancellation.cancel();
    state.fail(&reason);
    // Aborting a running blocking task cannot stop its writes. Never release
    // ownership or start queries while it may still publish a durable stage.
    match tokio::time::timeout(WORKER_STOP_TIMEOUT, worker).await {
        Ok(Ok(Ok(_))) => Err(error),
        Ok(Ok(Err(worker_error))) => {
            Err(error.wrap_err(format!("repair worker also failed: {worker_error:#}")))
        }
        Ok(Err(join_error)) => {
            Err(error.wrap_err(format!("repair worker also failed: {join_error}")))
        }
        Err(_) => std::process::exit(1),
    }
}

async fn finish_http(mut http: JoinHandle<()>) -> Result<()> {
    match tokio::time::timeout(HTTP_STOP_TIMEOUT, &mut http).await {
        Ok(result) => result.wrap_err("maintenance HTTP task failed"),
        Err(_) => {
            // HTTP owns no data-directory writes. Unlike the maintenance
            // worker it can be aborted, then joined before releasing its port.
            http.abort();
            let result = http.await;
            ensure!(
                result.as_ref().is_err_and(|error| error.is_cancelled()),
                "maintenance HTTP shutdown failed: {result:?}"
            );
            Err(eyre::eyre!(
                "maintenance HTTP graceful shutdown exceeded {HTTP_STOP_TIMEOUT:?}"
            ))
        }
    }
}
