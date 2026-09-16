//! Lifetime supervision for long-lived sync and node workers.
//!
//! Call `begin_shutdown` before notifying workers to stop. Ordinary completion
//! or cancellation is then expected; explicit errors and unwinds remain fatal.
//! A failure latch survives until the monitor is dropped.
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::watch;
use tokio::task::JoinHandle;

const RUNNING: u8 = 0;
const STOPPING: u8 = 1;
const FAILED: u8 = 2;

/// Retains the first unexpected worker exit, even before subscription.
#[derive(Default)]
pub struct TaskMonitor {
    inner: Arc<MonitorState>,
}

struct MonitorState {
    lifecycle: AtomicU8,
    failure: watch::Sender<Option<Arc<str>>>,
}

impl Default for MonitorState {
    fn default() -> Self {
        Self {
            lifecycle: AtomicU8::new(RUNNING),
            failure: watch::channel(None).0,
        }
    }
}

impl TaskMonitor {
    /// Subscribe to the permanent first-failure latch.
    pub fn subscribe(&self) -> watch::Receiver<Option<Arc<str>>> {
        self.inner.failure.subscribe()
    }

    /// Arbitrate intentional shutdown against concurrent worker completion.
    pub fn begin_shutdown(&self) {
        // One transition arbitrates a shutdown racing with a worker exit. An
        // already recorded failure must survive intentional cleanup.
        let _ = self.inner.lifecycle.compare_exchange(
            RUNNING,
            STOPPING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    /// Spawn a worker whose normal return is unexpected until shutdown.
    pub fn spawn(
        &self,
        name: &'static str,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> JoinHandle<()> {
        // Construct outside the future so cancellation before its first poll
        // still reports the worker's disappearance.
        let task = GuardedTask {
            future,
            guard: TaskExitGuard {
                state: Arc::clone(&self.inner),
                name,
            },
        };
        tokio::spawn(async move {
            let (future, guard) = task.into_parts();
            let _guard = guard;
            future.await;
        })
    }

    /// Spawn a fallible worker, preserving its error even during shutdown.
    pub fn spawn_result<E: std::fmt::Display + Send + 'static>(
        &self,
        name: &'static str,
        future: impl Future<Output = Result<(), E>> + Send + 'static,
    ) -> JoinHandle<()> {
        let task = GuardedTask {
            future,
            guard: TaskExitGuard {
                state: Arc::clone(&self.inner),
                name,
            },
        };
        tokio::spawn(async move {
            let (future, guard) = task.into_parts();
            if let Err(error) = future.await {
                guard.report_failure(format!("{name} failed: {error}"), true);
            }
            // Keep the guard until completion (including cancellation/unwind).
            drop(guard);
        })
    }
}

// Field order keeps the guard alive while the unpolled future is dropped. A
// cleanup unwind must still report failure after intentional shutdown begins.
struct GuardedTask<F> {
    future: F,
    guard: TaskExitGuard,
}

impl<F> GuardedTask<F> {
    fn into_parts(self) -> (F, TaskExitGuard) {
        // Consuming self makes the spawned closure capture the complete owner,
        // rather than independently capturing its fields in use order.
        (self.future, self.guard)
    }
}

struct TaskExitGuard {
    state: Arc<MonitorState>,
    name: &'static str,
}

impl TaskExitGuard {
    fn report_failure(&self, message: String, during_shutdown: bool) {
        if self
            .state
            .lifecycle
            .try_update(
                Ordering::AcqRel,
                Ordering::Acquire,
                |current| match current {
                    RUNNING => Some(FAILED),
                    STOPPING if during_shutdown => Some(FAILED),
                    _ => None,
                },
            )
            .is_ok()
        {
            // send_replace retains the first failure even before subscription.
            self.state.failure.send_replace(Some(Arc::from(message)));
        }
    }
}

impl Drop for TaskExitGuard {
    fn drop(&mut self) {
        let unwinding = std::thread::panicking();
        self.report_failure(
            format!(
                "{} {}",
                self.name,
                if unwinding {
                    "panicked"
                } else {
                    "exited unexpectedly"
                }
            ),
            unwinding,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn normal_worker_return_is_latched_before_subscription() {
        let monitor = TaskMonitor::default();
        monitor.spawn("execution worker", async {}).await.unwrap();
        let receiver = monitor.subscribe();
        assert_eq!(
            receiver.borrow().as_deref(),
            Some("execution worker exited unexpectedly")
        );
    }

    #[tokio::test]
    async fn worker_unwind_keeps_join_error_and_reports_failure() {
        let monitor = TaskMonitor::default();
        let mut receiver = monitor.subscribe();
        let task = monitor.spawn("execution worker", async {
            panic!("isolated worker exit control");
        });
        assert!(task.await.unwrap_err().is_panic());
        receiver.changed().await.unwrap();
        assert!(receiver.borrow().is_some());
    }

    #[tokio::test]
    async fn cancellation_before_first_poll_reports_failure() {
        let monitor = TaskMonitor::default();
        let task = monitor.spawn("execution worker", std::future::pending());
        // This current-thread test has not yielded since spawning the task.
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(monitor.subscribe().borrow().is_some());
    }

    #[tokio::test]
    async fn intentional_shutdown_suppresses_return_and_cancellation() {
        for abort in [false, true] {
            let monitor = TaskMonitor::default();
            let task = monitor.spawn("execution worker", async {});
            monitor.begin_shutdown();
            if abort {
                task.abort();
            }
            let _ = task.await;
            assert!(monitor.subscribe().borrow().is_none());
        }
    }

    #[tokio::test]
    async fn first_failure_survives_other_exits_and_shutdown() {
        let monitor = TaskMonitor::default();
        monitor.spawn("first worker", async {}).await.unwrap();
        monitor.begin_shutdown();
        monitor.spawn("second worker", async {}).await.unwrap();
        assert_eq!(
            monitor.subscribe().borrow().as_deref(),
            Some("first worker exited unexpectedly")
        );
    }
    #[tokio::test]
    async fn worker_unwind_during_shutdown_reports_failure() {
        let monitor = TaskMonitor::default();
        monitor.begin_shutdown();
        let task = monitor.spawn("shutdown worker", async {
            panic!("isolated shutdown failure control");
        });
        assert!(task.await.unwrap_err().is_panic());
        assert!(monitor.subscribe().borrow().is_some());
    }

    #[tokio::test]
    async fn explicit_error_is_preserved_before_and_during_shutdown() {
        for stopping in [false, true] {
            let monitor = TaskMonitor::default();
            if stopping {
                monitor.begin_shutdown();
            }
            monitor
                .spawn_result("service", async { Err::<(), _>("local bind failed") })
                .await
                .unwrap();
            assert_eq!(
                monitor.subscribe().borrow().as_deref(),
                Some("service failed: local bind failed")
            );
        }
    }

    #[tokio::test]
    async fn fallible_worker_success_requires_requested_shutdown() {
        for stopping in [false, true] {
            let monitor = TaskMonitor::default();
            if stopping {
                monitor.begin_shutdown();
            }
            monitor
                .spawn_result("service", async { Ok::<(), String>(()) })
                .await
                .unwrap();
            assert_eq!(monitor.subscribe().borrow().is_none(), stopping);
        }
    }

    #[tokio::test]
    async fn fallible_worker_cancellation_before_poll_respects_shutdown() {
        for stopping in [false, true] {
            let monitor = TaskMonitor::default();
            let task =
                monitor.spawn_result("service", std::future::pending::<Result<(), String>>());
            if stopping {
                monitor.begin_shutdown();
            }
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert_eq!(monitor.subscribe().borrow().is_none(), stopping);
        }
    }

    #[tokio::test]
    async fn fallible_worker_unwind_during_shutdown_is_retained() {
        let monitor = TaskMonitor::default();
        monitor.begin_shutdown();
        let task = monitor.spawn_result("service", async {
            panic!("isolated service failure control");
            #[expect(unreachable_code)]
            Ok::<(), String>(())
        });
        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(
            monitor.subscribe().borrow().as_deref(),
            Some("service panicked")
        );
    }

    #[tokio::test]
    async fn first_error_survives_later_error_and_success() {
        let monitor = TaskMonitor::default();
        monitor
            .spawn_result("first", async { Err::<(), _>("initial failure") })
            .await
            .unwrap();
        monitor.begin_shutdown();
        monitor
            .spawn_result("second", async { Err::<(), _>("later failure") })
            .await
            .unwrap();
        monitor.spawn("third", async {}).await.unwrap();
        assert_eq!(
            monitor.subscribe().borrow().as_deref(),
            Some("first failed: initial failure")
        );
    }

    #[tokio::test]
    async fn cancellation_drop_unwind_during_shutdown_is_retained() {
        struct DropFailure(Option<tokio::sync::oneshot::Sender<()>>);
        impl Future for DropFailure {
            type Output = Result<(), &'static str>;
            fn poll(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                if let Some(polled) = self.get_mut().0.take() {
                    let _ = polled.send(());
                }
                std::task::Poll::Pending
            }
        }
        impl Drop for DropFailure {
            fn drop(&mut self) {
                panic!("isolated worker cleanup failure control");
            }
        }
        for fallible in [false, true] {
            for poll_first in [false, true] {
                let monitor = TaskMonitor::default();
                let (started, polled) = tokio::sync::oneshot::channel();
                let future = DropFailure(Some(started));
                let task = if fallible {
                    monitor.spawn_result("service", future)
                } else {
                    monitor.spawn("service", async move {
                        let _ = future.await;
                    })
                };
                if poll_first {
                    polled.await.unwrap();
                }
                monitor.begin_shutdown();
                // Without the explicit await above, this current-thread runtime
                // has not yet polled the spawned future.
                task.abort();
                assert!(task.await.unwrap_err().is_panic());
                assert_eq!(
                    monitor.subscribe().borrow().as_deref(),
                    Some("service panicked")
                );
            }
        }
    }
}
