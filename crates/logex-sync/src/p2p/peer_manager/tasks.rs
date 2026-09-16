//! Lifetime supervision for the execution network's long-lived workers.
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::watch;
use tokio::task::JoinHandle;

const RUNNING: u8 = 0;
const STOPPING: u8 = 1;
const FAILED: u8 = 2;

#[derive(Default)]
pub(super) struct TaskMonitor {
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
    pub(super) fn subscribe(&self) -> watch::Receiver<Option<Arc<str>>> {
        self.inner.failure.subscribe()
    }

    pub(super) fn begin_shutdown(&self) {
        // One transition arbitrates a shutdown racing with a worker exit. An
        // already recorded failure must survive intentional cleanup.
        let _ = self.inner.lifecycle.compare_exchange(
            RUNNING,
            STOPPING,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(super) fn spawn(
        &self,
        name: &'static str,
        future: impl Future<Output = ()> + Send + 'static,
    ) -> JoinHandle<()> {
        // Construct outside the future so cancellation before its first poll
        // still reports the worker's disappearance.
        let guard = TaskExitGuard {
            state: Arc::clone(&self.inner),
            name,
        };
        tokio::spawn(async move {
            let _guard = guard;
            future.await;
        })
    }
}

struct TaskExitGuard {
    state: Arc<MonitorState>,
    name: &'static str,
}

impl Drop for TaskExitGuard {
    fn drop(&mut self) {
        if self
            .state
            .lifecycle
            .compare_exchange(RUNNING, FAILED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            // send_replace retains the first failure even before subscription.
            self.state.failure.send_replace(Some(Arc::from(format!(
                "{} exited unexpectedly",
                self.name
            ))));
        }
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
}
