use super::{LowDiskSpace, mark_sync_stopped_for_runtime_failure, wait_for_runtime_failure};
use futures_util::FutureExt;
use logex_types::SyncStatus;
use std::fmt::Display;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::pin;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::watch;

pub(super) struct SyncSupervisor<'a> {
    pub(super) shutdown_tx: &'a watch::Sender<bool>,
    pub(super) sync_status: &'a Mutex<SyncStatus>,
    pub(super) consensus_storage_failure: &'a mut Option<watch::Receiver<Option<Arc<str>>>>,
    pub(super) execution_network_failure: &'a mut Option<watch::Receiver<Option<Arc<str>>>>,
    pub(super) shutdown_timeout: Duration,
}

enum StopTrigger {
    Engine(Result<(), String>),
    Signal(&'static str),
    LowDisk(LowDiskSpace),
    RuntimeFailure {
        component: &'static str,
        error: Arc<str>,
        consensus_unavailable: bool,
    },
}

impl SyncSupervisor<'_> {
    pub(super) async fn run<E: Display>(
        self,
        engine: impl Future<Output = Result<(), E>>,
        signal: impl Future<Output = &'static str>,
        low_disk: impl Future<Output = LowDiskSpace>,
        mut on_failure: impl FnMut(&str),
    ) -> ExitCode {
        // Treat an engine unwind as terminal: never poll that future again.
        // The caller only retires owned work, closes services and exits; continued
        // ingestion must not rely on partially updated engine state.
        let mut engine = pin!(async {
            match AssertUnwindSafe(engine).catch_unwind().await {
                Ok(result) => result.map_err(|error| error.to_string()),
                Err(payload) => {
                    let message = payload
                        .downcast_ref::<&str>()
                        .copied()
                        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
                        .unwrap_or("non-string panic payload");
                    Err(format!("panicked: {message}"))
                }
            }
        });
        let trigger = tokio::select! {
            biased;
            error = wait_for_runtime_failure(self.consensus_storage_failure) => StopTrigger::RuntimeFailure {
                component: "consensus storage", error, consensus_unavailable: true,
            },
            error = wait_for_runtime_failure(self.execution_network_failure) => StopTrigger::RuntimeFailure {
                component: "execution network", error, consensus_unavailable: false,
            },
            result = &mut engine => StopTrigger::Engine(result),
            signal = signal => StopTrigger::Signal(signal),
            low_disk = low_disk => StopTrigger::LowDisk(low_disk),
        };

        let mut exit = ExitCode::SUCCESS;
        let mut consensus_unavailable = false;
        let engine_stopped = match trigger {
            StopTrigger::Engine(result) => {
                if let Err(error) = result {
                    record_failure(
                        &mut exit,
                        &mut on_failure,
                        format!("sync engine error: {error}"),
                    );
                }
                true
            }
            StopTrigger::Signal(signal) => {
                tracing::info!(signal, "shutdown requested, stopping node gracefully");
                false
            }
            StopTrigger::LowDisk(low_disk) => {
                record_failure(
                    &mut exit,
                    &mut on_failure,
                    format!(
                        "disk space below safety threshold at {}: {} free bytes, minimum {}",
                        low_disk.path.display(),
                        low_disk.free_bytes,
                        low_disk.min_free_bytes,
                    ),
                );
                false
            }
            StopTrigger::RuntimeFailure {
                component,
                error,
                consensus_unavailable: unavailable,
            } => {
                consensus_unavailable = unavailable;
                record_failure(
                    &mut exit,
                    &mut on_failure,
                    format!("{component} failed: {error}"),
                );
                false
            }
        };

        // Notify other workers before taking status locks or awaiting the engine.
        // For failures, record_failure already armed the outer cleanup watchdog.
        let _ = self.shutdown_tx.send(true);
        if exit == ExitCode::FAILURE {
            mark_sync_stopped_for_runtime_failure(self.sync_status, consensus_unavailable);
        }
        if !engine_stopped {
            match tokio::time::timeout(self.shutdown_timeout, &mut engine).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => record_failure(
                    &mut exit,
                    &mut on_failure,
                    format!("sync engine error during shutdown: {error}"),
                ),
                Err(_) => record_failure(
                    &mut exit,
                    &mut on_failure,
                    format!(
                        "sync engine did not stop within {:?}",
                        self.shutdown_timeout
                    ),
                ),
            }
            // The engine may publish final progress while honoring shutdown.
            // Reassert the failure state after it stops (or its grace expires).
            if exit == ExitCode::FAILURE {
                mark_sync_stopped_for_runtime_failure(self.sync_status, consensus_unavailable);
            }
        }
        exit
    }
}

fn record_failure(exit: &mut ExitCode, on_failure: &mut impl FnMut(&str), message: String) {
    if *exit == ExitCode::SUCCESS {
        *exit = ExitCode::FAILURE;
        // Arm once, before logging, status locks and the shared cleanup. A second
        // failure must not replace the guard and restart its cleanup deadline.
        on_failure(&message);
    }
    tracing::error!(error = %message, "sync runtime failed, stopping node");
}

#[cfg(test)]
mod tests;
