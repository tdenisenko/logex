use std::fmt::Display;
use std::process::ExitCode;
use std::sync::{Mutex, mpsc};
use std::time::{Duration, Instant};

use logex_types::SyncStatus;

/// A whole-shutdown deadline requiring explicit successful runtime teardown.
#[must_use = "finish the Tokio runtime while retaining its shutdown guard"]
pub struct RuntimeShutdown {
    completed: mpsc::Sender<Instant>,
    deadline: Instant,
}

pub(super) fn start_shutdown_watchdog(
    grace: Duration,
    on_failure: impl FnOnce() + Send + 'static,
) -> std::io::Result<RuntimeShutdown> {
    let deadline = Instant::now().checked_add(grace).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "shutdown grace is too large",
        )
    })?;
    let (completed, receiver) = mpsc::channel();
    std::thread::Builder::new()
        .name("shutdown-watchdog".to_owned())
        .spawn(move || {
            // Disconnection is also failure: an unwind or accidental early drop
            // must not disarm protection before blocking workers have stopped.
            if !completed_in_time(receiver, deadline) {
                on_failure();
            }
        })?;
    Ok(RuntimeShutdown {
        completed,
        deadline,
    })
}

/// Keep the deadline alive through Tokio and remaining process-owner teardown.
pub fn finish_runtime_shutdown(
    runtime: tokio::runtime::Runtime,
    guard: RuntimeShutdown,
    finish_owners: impl FnOnce(),
) -> std::io::Result<()> {
    drop(runtime);
    finish_owners();
    let completed = Instant::now();
    let expired = || {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "runtime shutdown deadline expired",
        )
    };
    if completed > guard.deadline {
        return Err(expired());
    }
    guard.completed.send(completed).map_err(|_| expired())
}

fn completed_in_time(receiver: mpsc::Receiver<Instant>, deadline: Instant) -> bool {
    matches!(
        receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())),
        Ok(completed) if completed <= deadline
    )
}

pub(super) fn record_failure(exit: &mut ExitCode, status: &Mutex<SyncStatus>, error: impl Display) {
    *exit = ExitCode::FAILURE;
    // The whole-shutdown guard is already armed before any of these operations.
    super::mark_sync_stopped_for_runtime_failure(status, false);
    tracing::error!(%error, "node cleanup failed");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn blocked_runtime() -> (tokio::runtime::Runtime, mpsc::Sender<()>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (release, blocked) = mpsc::channel::<()>();
        let (started, observed) = mpsc::channel();
        runtime.spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = blocked.recv();
        });
        observed.recv_timeout(Duration::from_secs(5)).unwrap();
        (runtime, release)
    }

    #[test]
    fn shutdown_deadline_remains_armed_through_runtime_drop() {
        let (runtime, release) = blocked_runtime();
        let (failed, observed) = mpsc::channel();
        let guard = start_shutdown_watchdog(Duration::from_millis(20), move || {
            let _ = failed.send(());
        })
        .unwrap();
        let cleanup = std::thread::spawn(move || finish_runtime_shutdown(runtime, guard, || {}));
        let outcome = observed.recv_timeout(Duration::from_secs(5));
        // Release this test's blocking worker even when the observation failed.
        drop(release);
        assert!(cleanup.join().unwrap().is_err());
        assert_eq!(outcome, Ok(()));
    }

    #[test]
    fn completed_runtime_teardown_disarms_deadline() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (failed, observed) = mpsc::channel();
        let guard = start_shutdown_watchdog(Duration::from_secs(30), move || {
            let _ = failed.send(());
        })
        .unwrap();
        finish_runtime_shutdown(runtime, guard, || {}).unwrap();
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(5)),
            Err(mpsc::RecvTimeoutError::Disconnected)
        );
    }

    #[test]
    fn process_owner_teardown_remains_inside_the_shutdown_deadline() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (failed, observed) = mpsc::channel();
        let (release, blocked) = mpsc::channel::<()>();
        let guard = start_shutdown_watchdog(Duration::from_millis(20), move || {
            let _ = failed.send(());
        })
        .unwrap();
        let cleanup = std::thread::spawn(move || {
            finish_runtime_shutdown(runtime, guard, || {
                let _ = blocked.recv();
            })
        });
        let outcome = observed.recv_timeout(Duration::from_secs(5));
        drop(release);
        assert!(cleanup.join().unwrap().is_err());
        assert_eq!(outcome, Ok(()));
    }

    #[test]
    fn dropping_uncompleted_guard_reports_failure() {
        let (failed, observed) = mpsc::channel();
        let guard = start_shutdown_watchdog(Duration::from_secs(30), move || {
            let _ = failed.send(());
        })
        .unwrap();
        drop(guard);
        assert_eq!(observed.recv_timeout(Duration::from_secs(5)), Ok(()));
    }

    #[test]
    fn cleanup_unwind_does_not_disarm_runtime_protection() {
        let (runtime, release) = blocked_runtime();
        let (failed, observed) = mpsc::channel();
        let guard = start_shutdown_watchdog(Duration::from_secs(30), move || {
            let _ = failed.send(());
        })
        .unwrap();
        let cleanup = std::thread::spawn(move || {
            let _runtime = runtime;
            let _guard = guard;
            panic!("isolated cleanup unwind control");
        });
        let outcome = observed.recv_timeout(Duration::from_secs(5));
        drop(release);
        assert!(cleanup.join().is_err());
        assert_eq!(outcome, Ok(()));
    }

    #[test]
    fn cleanup_error_preserves_failure_and_stopped_status() {
        let mut exit = ExitCode::SUCCESS;
        let status = Mutex::new(SyncStatus {
            syncing: true,
            eta_seconds: Some(5.0),
            ..Default::default()
        });
        record_failure(&mut exit, &status, "owned worker failed");
        assert_eq!(exit, ExitCode::FAILURE);
        record_failure(&mut exit, &status, "secondary cleanup failure");
        assert_eq!(exit, ExitCode::FAILURE);
        let status = status.lock().unwrap();
        assert!(!status.syncing);
        assert!(status.eta_seconds.is_none());
    }

    #[test]
    fn delayed_watchdog_checks_completion_time_instead_of_queue_presence() {
        let deadline = Instant::now();
        for completed_in_budget in [false, true] {
            let (complete, receiver) = mpsc::channel();
            complete
                .send(if completed_in_budget {
                    deadline
                } else {
                    deadline + Duration::from_secs(1)
                })
                .unwrap();
            drop(complete);
            assert_eq!(completed_in_time(receiver, deadline), completed_in_budget);
        }
    }

    #[test]
    fn expired_runtime_teardown_cannot_return_success() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (failed, observed) = mpsc::channel();
        let guard = start_shutdown_watchdog(Duration::ZERO, move || {
            let _ = failed.send(());
        })
        .unwrap();
        let error = finish_runtime_shutdown(runtime, guard, || {}).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
        assert_eq!(observed.recv_timeout(Duration::from_secs(5)), Ok(()));
    }
}
