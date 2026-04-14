use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use logex_index::IndexBuilder;
use logex_server::AppState;

const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Background task that periodically rebuilds indexes on the hot partition.
pub async fn run_background_indexer(
    state: Arc<AppState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut last_indexed: Option<HotIndexState> = None;
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = wait_for_shutdown(&mut shutdown) => {
                tracing::info!("background indexer shutting down");
                break;
            }
            _ = ticker.tick() => {}
        }

        let current = {
            let storage = state.storage.read().await;
            HotIndexState {
                partition_id: storage.hot_partition().meta.id,
                row_count: storage.hot_partition().meta.row_count,
                path: storage.hot_partition().meta.path.clone(),
            }
        };

        if should_rebuild_hot_indexes(last_indexed.as_ref(), &current) {
            let path = current.path.clone();
            match tokio::task::spawn_blocking(move || IndexBuilder::build_all_indexes(&path)).await
            {
                Ok(Ok(())) => {
                    tracing::debug!(
                        partition_id = current.partition_id,
                        rows = current.row_count,
                        "indexes rebuilt"
                    );
                    last_indexed = Some(current);
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "failed to build indexes");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "index build task panicked");
                }
            }
        }
    }
}

pub async fn log_task_exit(name: &str, handle: tokio::task::JoinHandle<()>) {
    let mut handle = handle;
    match tokio::time::timeout(TASK_SHUTDOWN_TIMEOUT, &mut handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            tracing::warn!(task = name, error = %e, "task exited unexpectedly");
        }
        Err(_) => {
            tracing::warn!(
                task = name,
                ?TASK_SHUTDOWN_TIMEOUT,
                "task did not stop in time, aborting it"
            );
            handle.abort();
            let _ = handle.await;
        }
    }
}

async fn wait_for_shutdown(shutdown: &mut tokio::sync::watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
}

#[derive(Debug, Clone)]
struct HotIndexState {
    partition_id: u64,
    row_count: u64,
    path: PathBuf,
}

fn should_rebuild_hot_indexes(
    last_indexed: Option<&HotIndexState>,
    current: &HotIndexState,
) -> bool {
    if current.row_count == 0 {
        return false;
    }

    match last_indexed {
        None => true,
        Some(last) if last.partition_id != current.partition_id => true,
        Some(last) => current.row_count > last.row_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rebuild_hot_indexes_when_partition_rotates() {
        let last = HotIndexState {
            partition_id: 1,
            row_count: 150,
            path: PathBuf::from("/tmp/old"),
        };
        let current = HotIndexState {
            partition_id: 2,
            row_count: 1,
            path: PathBuf::from("/tmp/new"),
        };

        assert!(should_rebuild_hot_indexes(Some(&last), &current));
    }

    #[test]
    fn skip_rebuild_when_hot_partition_is_unchanged() {
        let last = HotIndexState {
            partition_id: 7,
            row_count: 200,
            path: PathBuf::from("/tmp/hot"),
        };
        let current = HotIndexState {
            partition_id: 7,
            row_count: 200,
            path: PathBuf::from("/tmp/hot"),
        };

        assert!(!should_rebuild_hot_indexes(Some(&last), &current));
    }
}
