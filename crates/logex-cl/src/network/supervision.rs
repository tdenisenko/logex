use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use logex_types::SyncStatus;
use tokio::sync::watch;
use tokio::task::JoinSet;

use super::{ConsensusNetworkError, mark_consensus_network_unavailable, wait_for_shutdown};

pub(super) async fn supervise<F>(
    mut network: F,
    mut rebuild: impl FnMut() -> Result<F, ConsensusNetworkError>,
    sync_status: Arc<Mutex<SyncStatus>>,
    mut shutdown: watch::Receiver<bool>,
    restart_delay: Duration,
) -> Result<(), ConsensusNetworkError>
where
    F: Future<Output = Result<(), ConsensusNetworkError>> + Send + 'static,
{
    // Retain the child task until it completes. Dropping a bare JoinHandle would
    // detach it when this supervisor is canceled; JoinSet aborts owned tasks.
    let mut workers = JoinSet::new();
    loop {
        // Do not start another network after a stop observed during rebuild.
        if *shutdown.borrow() || shutdown.has_changed().is_err() {
            return Ok(());
        }
        workers.spawn(network);
        let outcome = workers
            .join_next()
            .await
            .expect("the supervisor owns exactly one network worker");
        if *shutdown.borrow() || shutdown.has_changed().is_err() {
            return outcome.map_err(ConsensusNetworkError::WorkerShutdown)?;
        }

        mark_consensus_network_unavailable(&sync_status);
        match outcome {
            Ok(Ok(())) => {
                tracing::error!("consensus network exited unexpectedly; restarting");
            }
            Ok(Err(error)) => {
                tracing::error!(%error, "consensus network exited with an error; restarting");
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    panicked = error.is_panic(),
                    "consensus network task failed; restarting"
                );
            }
        }

        loop {
            tokio::select! {
                biased;
                _ = wait_for_shutdown(&mut shutdown) => return Ok(()),
                _ = tokio::time::sleep(restart_delay) => {}
            }
            match rebuild() {
                Ok(next_network) => {
                    network = next_network;
                    tracing::info!("consensus network supervisor restarted the network task");
                    break;
                }
                Err(error) => {
                    tracing::error!(%error, "failed to reconstruct consensus network; retrying");
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    type Worker = Pin<Box<dyn Future<Output = Result<(), ConsensusNetworkError>> + Send>>;

    struct Dropped(Option<oneshot::Sender<()>>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    #[tokio::test]
    async fn supervisor_cancellation_retires_its_worker() {
        let (_shutdown, receiver) = watch::channel(false);
        let (started, observed_start) = oneshot::channel();
        let (release, released) = oneshot::channel::<()>();
        let (dropped, mut observed_drop) = oneshot::channel();
        let worker = async move {
            let _owned = Dropped(Some(dropped));
            let _ = started.send(());
            let _ = released.await;
            Ok(())
        };
        let supervisor = tokio::spawn(supervise(
            worker,
            || unreachable!("no restart before cancellation"),
            Arc::new(Mutex::new(SyncStatus::default())),
            receiver,
            Duration::from_secs(60),
        ));
        observed_start.await.unwrap();
        supervisor.abort();
        assert!(supervisor.await.unwrap_err().is_cancelled());
        let retired = tokio::time::timeout(Duration::from_secs(5), &mut observed_drop).await;
        // Always release this control's worker, including on the original code.
        drop(release);
        if retired.is_err() {
            observed_drop.await.unwrap();
        }
        assert!(retired.is_ok(), "supervisor detached its worker");
    }

    #[tokio::test]
    async fn worker_error_during_shutdown_is_preserved() {
        let (shutdown, receiver) = watch::channel(false);
        let worker = async move {
            shutdown.send(true).unwrap();
            Err(ConsensusNetworkError::StartDiscovery(
                "local startup control".into(),
            ))
        };
        let result = supervise(
            worker,
            || unreachable!("shutdown does not restart"),
            Arc::new(Mutex::new(SyncStatus::default())),
            receiver,
            Duration::from_secs(60),
        )
        .await;
        assert!(matches!(
            result,
            Err(ConsensusNetworkError::StartDiscovery(_))
        ));
    }

    #[tokio::test]
    async fn worker_unwind_during_shutdown_is_preserved() {
        let (shutdown, receiver) = watch::channel(false);
        let worker = async move {
            shutdown.send(true).unwrap();
            worker_panic()
        };
        let result = supervise(
            worker,
            || unreachable!("shutdown does not restart"),
            Arc::new(Mutex::new(SyncStatus::default())),
            receiver,
            Duration::from_secs(60),
        )
        .await;
        assert!(
            matches!(result, Err(ConsensusNetworkError::WorkerShutdown(error)) if error.is_panic())
        );
    }

    fn worker_panic() -> Result<(), ConsensusNetworkError> {
        panic!("isolated consensus worker completion control");
    }

    #[tokio::test]
    async fn closed_shutdown_channel_preserves_worker_error() {
        let (shutdown, receiver) = watch::channel(false);
        let result = supervise(
            async move {
                drop(shutdown);
                Err(ConsensusNetworkError::EventStream(
                    "local completion control".into(),
                ))
            },
            || unreachable!("closed owner does not restart"),
            Arc::new(Mutex::new(SyncStatus::default())),
            receiver,
            Duration::ZERO,
        )
        .await;
        assert!(matches!(result, Err(ConsensusNetworkError::EventStream(_))));
    }

    #[tokio::test]
    async fn recovery_restarts_after_return_error_and_unwind() {
        for outcome in 0..3 {
            let (shutdown, receiver) = watch::channel(false);
            let status = Arc::new(Mutex::new(SyncStatus {
                syncing: true,
                consensus_head_fresh: Some(true),
                ..Default::default()
            }));
            let first: Worker = Box::pin(async move {
                match outcome {
                    0 => Ok(()),
                    1 => Err(ConsensusNetworkError::StartDiscovery(
                        "local startup control".into(),
                    )),
                    _ => worker_panic(),
                }
            });
            let attempts = Arc::new(AtomicUsize::new(0));
            let rebuild_attempts = Arc::clone(&attempts);
            let restarted_status = Arc::clone(&status);
            let restarted_receiver = receiver.clone();
            let (restarted, observed) = oneshot::channel();
            let mut restarted = Some(restarted);
            let rebuild = move || -> Result<Worker, ConsensusNetworkError> {
                let attempt = rebuild_attempts.fetch_add(1, Ordering::SeqCst);
                if attempt == 0 {
                    return Err(ConsensusNetworkError::ConstructDiscovery(
                        "local reconstruction control".into(),
                    ));
                }
                assert_eq!(attempt, 1);
                let status = Arc::clone(&restarted_status);
                let mut receiver = restarted_receiver.clone();
                let restarted = restarted.take().unwrap();
                Ok(Box::pin(async move {
                    assert_eq!(status.lock().unwrap().consensus_head_fresh, Some(false));
                    restarted.send(()).unwrap();
                    wait_for_shutdown(&mut receiver).await;
                    Ok(())
                }))
            };
            let task = tokio::spawn(supervise(first, rebuild, status, receiver, Duration::ZERO));
            let recovered = tokio::time::timeout(Duration::from_secs(5), observed).await;
            shutdown.send(true).unwrap();
            let stopped = tokio::time::timeout(Duration::from_secs(5), task).await;
            recovered.unwrap().unwrap();
            stopped.unwrap().unwrap().unwrap();
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
        }
    }

    #[tokio::test]
    async fn stop_before_first_poll_does_not_start_a_worker() {
        for closed in [false, true] {
            let (shutdown, receiver) = watch::channel(!closed);
            if closed {
                drop(shutdown);
            }
            supervise(
                async { panic!("stopped supervisor polled a worker") },
                || unreachable!("stopped supervisor rebuilt a worker"),
                Arc::new(Mutex::new(SyncStatus::default())),
                receiver,
                Duration::ZERO,
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn stop_during_reconstruction_drops_the_new_worker_unpolled() {
        let (shutdown, receiver) = watch::channel(false);
        let (dropped, observed) = oneshot::channel();
        let mut owned = Some(Dropped(Some(dropped)));
        let first: Worker = Box::pin(async { Ok(()) });
        supervise(
            first,
            move || -> Result<Worker, ConsensusNetworkError> {
                shutdown.send(true).unwrap();
                let owned = owned.take().unwrap();
                Ok(Box::pin(async move {
                    drop(owned);
                    panic!("stopped supervisor polled the replacement");
                }))
            },
            Arc::new(Mutex::new(SyncStatus::default())),
            receiver,
            Duration::ZERO,
        )
        .await
        .unwrap();
        observed.await.unwrap();
    }

    #[tokio::test]
    async fn cancellation_before_first_poll_drops_owned_future() {
        let (_shutdown, receiver) = watch::channel(false);
        let (dropped, observed) = oneshot::channel();
        let owned = Dropped(Some(dropped));
        let task = tokio::spawn(supervise(
            async move {
                drop(owned);
                panic!("unpolled supervisor started its worker");
            },
            || unreachable!("unpolled supervisor rebuilt a worker"),
            Arc::new(Mutex::new(SyncStatus::default())),
            receiver,
            Duration::ZERO,
        ));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        observed.await.unwrap();
    }

    #[tokio::test]
    async fn outer_reconstruction_unwind_is_observable_by_owner() {
        let (_shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(supervise(
            async { Ok(()) },
            || panic!("isolated supervisor reconstruction control"),
            Arc::new(Mutex::new(SyncStatus::default())),
            receiver,
            Duration::ZERO,
        ));
        assert!(task.await.unwrap_err().is_panic());
    }

    #[tokio::test]
    async fn shutdown_interrupts_restart_wait() {
        let (shutdown, receiver) = watch::channel(false);
        let (finished, observed) = oneshot::channel();
        let status = Arc::new(Mutex::new(SyncStatus {
            consensus_head_fresh: Some(true),
            ..Default::default()
        }));
        let task = tokio::spawn(supervise(
            async move {
                finished.send(()).unwrap();
                Ok(())
            },
            || unreachable!("shutdown must interrupt the restart delay"),
            Arc::clone(&status),
            receiver,
            Duration::from_secs(60),
        ));
        observed.await.unwrap();
        let entered_retry = tokio::time::timeout(Duration::from_secs(5), async {
            while status.lock().unwrap().consensus_head_fresh != Some(false) {
                tokio::task::yield_now().await;
            }
        })
        .await;
        shutdown.send(true).unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(5), task).await;
        entered_retry.unwrap();
        stopped.unwrap().unwrap().unwrap();
    }
}
