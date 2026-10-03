//! Bounded synchronous projection work for DataFusion's async streams.
use std::{io, sync::Arc};

use datafusion::error::{DataFusionError, Result};
use tokio::sync::{Semaphore, oneshot};

use super::{QueryCancelCheck, check_query_canceled};

/// Shared across every table scan and partition in one SQL execution. This
/// bounds filesystem/decode work independently of DataFusion's partition count
/// and Tokio's blocking-pool limit. The caller may already occupy that pool
/// while driving the SQL future, so projection uses bounded native threads
/// rather than queuing work behind its own caller. No runtime or persistent
/// source/cache is created.
#[derive(Clone)]
pub(super) struct ProjectionWorkers {
    permits: Arc<Semaphore>,
}

impl ProjectionWorkers {
    pub(super) fn new() -> Self {
        let width = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .clamp(1, 8);
        Self {
            permits: Arc::new(Semaphore::new(width)),
        }
    }

    pub(super) async fn run<T, F>(&self, cancel: Option<QueryCancelCheck>, work: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce() -> io::Result<T> + Send + 'static,
    {
        check_query_canceled(cancel.as_ref())?;
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| DataFusionError::Execution("query projection workers closed".into()))?;
        check_query_canceled(cancel.as_ref())?;
        let (sender, receiver) = oneshot::channel();
        std::thread::Builder::new()
            .name("logex-sql-projection".to_owned())
            .spawn(move || {
                // Started I/O cannot be forcibly interrupted. Retain capacity,
                // request ownership and captured buffers even if DataFusion
                // drops the receiver. Waiting work has not spawned a thread.
                let result = (|| {
                    check_query_canceled(cancel.as_ref())?;
                    let result = work().map_err(DataFusionError::IoError);
                    check_query_canceled(cancel.as_ref())?;
                    result
                })();
                // I/O and decoding are finished before a successful receiver
                // can start its next batch. Keep request ownership until the
                // result has either been delivered or discarded as well.
                drop(permit);
                // Transfer the request owner with the result: an abandoned
                // receiver must drop any batch before releasing admission.
                let _ = sender.send(ProjectionResult {
                    result,
                    _request_owner: cancel,
                });
            })
            .map_err(DataFusionError::IoError)?;
        let ProjectionResult {
            result,
            _request_owner,
        } = receiver.await.map_err(|_| {
            DataFusionError::Execution("query projection worker failed before returning".into())
        })?;
        result
    }
}

struct ProjectionResult<T> {
    // Field order also protects an unreceived result: buffers go first.
    result: Result<T>,
    _request_owner: Option<QueryCancelCheck>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use logex_types::{QueryMemoryBudget, QueryMemoryLimit};
    use std::{
        future::Future,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        task::Poll,
        time::Duration,
    };

    fn workers(width: usize) -> ProjectionWorkers {
        ProjectionWorkers {
            permits: Arc::new(Semaphore::new(width)),
        }
    }

    struct DropNotice(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropNotice {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[test]
    fn projection_progresses_from_an_occupied_serving_blocking_pool() {
        // The server drives the SQL future from its blocking pool. A projection
        // must not queue behind that same caller when the pool has one slot.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let handle = tokio::runtime::Handle::current();
            let result = tokio::task::spawn_blocking(move || {
                handle.block_on(async {
                    tokio::time::timeout(Duration::from_secs(2), workers(1).run(None, || Ok(17)))
                        .await
                })
            })
            .await
            .unwrap();
            assert_eq!(result.unwrap().unwrap(), 17);
        });
    }

    #[tokio::test]
    async fn blocked_projection_keeps_runtime_responsive_and_bounds_other_partitions() {
        let workers = workers(1);
        let runtime_thread = std::thread::current().id();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let worker = workers.clone();
        let first = tokio::spawn(async move {
            worker
                .run(None, move || {
                    entered_tx.send(std::thread::current().id()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    Ok(7)
                })
                .await
        });
        assert_ne!(entered_rx.await.unwrap(), runtime_thread);
        assert_eq!(workers.permits.available_permits(), 0);

        let started = Arc::new(AtomicBool::new(false));
        let seen = Arc::clone(&started);
        let mut second = Box::pin(workers.run(None, move || {
            seen.store(true, Ordering::SeqCst);
            Ok(9)
        }));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(second.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        // These assertions run on the same single async thread while the
        // first filesystem/decode stand-in remains deliberately blocked.
        assert!(!started.load(Ordering::SeqCst));
        assert!(tokio::spawn(async { true }).await.unwrap());
        resume_tx.send(()).unwrap();
        assert_eq!(first.await.unwrap().unwrap(), 7);
        assert_eq!(second.await.unwrap(), 9);
        assert_eq!(workers.permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn abandoned_running_projection_retains_request_memory_and_capacity() {
        let workers = workers(1);
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024).unwrap());
        let retained = memory.reserve(128, "projection ownership control").unwrap();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let owner = Arc::new(DropNotice(Some(dropped_tx)));
        let weak = Arc::downgrade(&owner);
        let canceled = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&canceled);
        let cancel: QueryCancelCheck = Arc::new(move || {
            let _keep = &owner;
            flag.load(Ordering::SeqCst)
        });
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let worker = workers.clone();
        let first = tokio::spawn(async move {
            worker
                .run(Some(cancel), move || {
                    let _retained = retained;
                    entered_tx.send(()).unwrap();
                    resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                    Ok(())
                })
                .await
        });
        entered_rx.await.unwrap();
        canceled.store(true, Ordering::SeqCst);
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        assert!(weak.upgrade().is_some());
        assert_eq!(memory.used(), 128);
        assert_eq!(workers.permits.available_permits(), 0);
        resume_tx.send(()).unwrap();
        let permit = workers.permits.acquire().await.unwrap();
        drop(permit);
        assert_eq!(memory.used(), 0);
        // Observe the actual owner drop, not a scheduler-dependent delay after
        // the worker releases its permit.
        dropped_rx.await.unwrap();
        assert!(weak.upgrade().is_none());
    }

    #[tokio::test]
    async fn unreceived_batch_releases_its_memory_before_request_admission() {
        struct Owner {
            memory: QueryMemoryBudget,
            dropped: Option<tokio::sync::oneshot::Sender<u128>>,
        }
        impl Drop for Owner {
            fn drop(&mut self) {
                if let Some(sender) = self.dropped.take() {
                    let _ = sender.send(self.memory.used());
                }
            }
        }
        let workers = workers(1);
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024).unwrap());
        let batch = memory
            .reserve(128, "unreceived projection control")
            .unwrap();
        let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel();
        let owner = Owner {
            memory: memory.clone(),
            dropped: Some(dropped_tx),
        };
        let cancel: QueryCancelCheck = Arc::new(move || {
            let _keep = &owner;
            false
        });
        let (finished_tx, finished_rx) = tokio::sync::oneshot::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let mut request = Box::pin(workers.run(Some(cancel), move || {
            resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            finished_tx.send(()).unwrap();
            Ok(batch)
        }));
        // Poll once to start work, then abandon without receiving its result.
        // The worker can finish before or after this receiver is dropped.
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(request.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        resume_tx.send(()).unwrap();
        finished_rx.await.unwrap();
        drop(request);
        assert_eq!(dropped_rx.await.unwrap(), 0);
        assert_eq!(memory.used(), 0);
    }

    #[tokio::test]
    async fn canceled_waiter_never_starts_projection_and_errors_release_capacity() {
        let workers = workers(1);
        let occupied = workers.permits.acquire().await.unwrap();
        let canceled = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&canceled);
        let cancel: QueryCancelCheck = Arc::new(move || flag.load(Ordering::SeqCst));
        let starts = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&starts);
        let mut pending = Box::pin(workers.run(Some(cancel), move || {
            seen.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(pending.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        canceled.store(true, Ordering::SeqCst);
        drop(occupied);
        assert!(
            pending
                .await
                .unwrap_err()
                .to_string()
                .contains("query canceled")
        );
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        assert_eq!(workers.permits.available_permits(), 1);

        let error = workers
            .run::<(), _>(None, || {
                Err(io::Error::new(io::ErrorKind::InvalidData, "bad column"))
            })
            .await
            .unwrap_err();
        assert!(
            matches!(error, DataFusionError::IoError(error) if error.kind() == io::ErrorKind::InvalidData)
        );
        let panic = workers
            .run::<(), _>(None, || panic!("projection panic control"))
            .await
            .unwrap_err();
        assert!(panic.to_string().contains("query projection worker failed"));
        assert_eq!(workers.permits.available_permits(), 1);
    }
}
