use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use logex_index::{IndexBuildProfile, IndexBuilder};
use logex_server::AppState;
use logex_storage::PartitionManager;
use logex_types::{EXECUTION_HISTORY_TARGET_BLOCK, SyncStatus};

const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);
const ACTIVE_SYNC_COMPACTION_SEGMENT_LIMIT: usize = 1;
const ACTIVE_SYNC_COMPACTION_CATCH_UP_LIMIT: usize = 4;
const ACTIVE_SYNC_COMPACTION_HIGH_CATCH_UP_LIMIT: usize = 8;
const ACTIVE_SYNC_COMPACTION_CATCH_UP_BACKLOG: usize = 1_024;
const ACTIVE_SYNC_COMPACTION_HIGH_BACKLOG: usize = 4_096;
const ACTIVE_SYNC_PROFILE_REWRITE_SEGMENT_LIMIT: usize = 2;
const ACTIVE_SYNC_SEALED_INDEX_SEGMENT_LIMIT: usize = 2;
const BACKGROUND_COMPACTION_SEGMENT_LIMIT: usize = 24;
const BACKGROUND_SEALED_INDEX_SEGMENT_LIMIT: usize = 8;
const BACKGROUND_COMPACTION_INTERVAL: Duration = Duration::from_secs(10);
const ACTIVE_SYNC_BACKLOG_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
const BYTES_PER_KIB: u64 = 1024;
const BYTES_PER_GIB: u64 = 1024 * 1024 * 1024;
const ACTIVE_SYNC_COMPACTION_MIN_AVAILABLE_MEMORY_BYTES: u64 = 2 * BYTES_PER_GIB;

#[derive(Debug, Clone, Copy)]
struct CompactionReport {
    compacted: usize,
    raw_backlog: Option<usize>,
    profile_rewrite_backlog: Option<usize>,
}

/// Background task that periodically rebuilds indexes on the hot partition.
pub async fn run_background_indexer(
    state: Arc<AppState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    let mut last_indexed: Option<HotIndexState> = None;
    let mut sealed_index_scan_complete_for_max_id: Option<u64> = None;
    let mut last_active_backlog_refresh: Option<std::time::Instant> = None;
    let mut last_active_raw_backlog: Option<usize> = None;
    let mut ticker = tokio::time::interval(BACKGROUND_COMPACTION_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = wait_for_shutdown(&mut shutdown) => {
                tracing::info!("background indexer shutting down");
                break;
            }
            _ = ticker.tick() => {}
        }

        {
            let active_sync = sync_is_active(&state) || historical_sync_is_incomplete(&state).await;
            if active_sync
                && let Some(available_memory_bytes) = active_sync_compaction_memory_pressure()
            {
                let storage = Arc::clone(&state.storage);
                match tokio::task::spawn_blocking(move || -> io::Result<CompactionReport> {
                    let storage = storage.blocking_read();
                    Ok(CompactionReport {
                        compacted: 0,
                        raw_backlog: Some(storage.raw_compaction_backlog_count()?),
                        profile_rewrite_backlog: None,
                    })
                })
                .await
                {
                    Ok(Ok(report)) => update_compaction_status(&state, report),
                    Ok(Err(error)) => {
                        tracing::warn!(
                            %error,
                            "failed to refresh compaction backlog under memory pressure"
                        );
                    }
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "compaction backlog refresh task failed under memory pressure"
                        );
                    }
                }
                tracing::debug!(
                    available_memory_bytes,
                    "skipping active-sync storage compaction under memory pressure"
                );
                continue;
            }
            let compaction_limit = if active_sync {
                ACTIVE_SYNC_COMPACTION_SEGMENT_LIMIT
            } else {
                BACKGROUND_COMPACTION_SEGMENT_LIMIT
            };
            let refresh_active_backlog = active_sync
                && last_active_backlog_refresh.is_none_or(|refreshed_at| {
                    refreshed_at.elapsed() >= ACTIVE_SYNC_BACKLOG_REFRESH_INTERVAL
                });
            let active_raw_backlog_hint = if active_sync {
                last_active_raw_backlog
            } else {
                None
            };
            let storage = Arc::clone(&state.storage);
            match tokio::task::spawn_blocking(move || -> io::Result<CompactionReport> {
                if active_sync {
                    let (
                        raw_plan,
                        profile_plan,
                        raw_backlog_before,
                        profile_rewrite_backlog,
                        compaction_limit,
                    ) = {
                        let storage = storage.blocking_read();
                        let raw_backlog_before = if refresh_active_backlog {
                            Some(storage.raw_compaction_backlog_count()?)
                        } else {
                            None
                        };
                        let profile_rewrite_backlog = if refresh_active_backlog {
                            Some(storage.profile_rewrite_backlog_count()?)
                        } else {
                            None
                        };
                        let raw_backlog_for_limits =
                            raw_backlog_before.or(active_raw_backlog_hint).unwrap_or(1);
                        let (compaction_limit, profile_rewrite_limit) =
                            active_sync_compaction_limits(raw_backlog_for_limits, compaction_limit);
                        (
                            storage.recent_raw_segment_compaction_plan(compaction_limit)?,
                            storage.profile_rewrite_compaction_plan(profile_rewrite_limit)?,
                            raw_backlog_before,
                            profile_rewrite_backlog,
                            compaction_limit,
                        )
                    };

                    debug_assert!(raw_plan.len() <= compaction_limit);
                    let raw_plan_len = raw_plan.len();
                    let compacted = raw_plan.compact()? + profile_plan.compact()?;
                    let raw_backlog = raw_backlog_before
                        .or(active_raw_backlog_hint)
                        .map(|backlog| backlog.saturating_sub(raw_plan_len));
                    return Ok(CompactionReport {
                        compacted,
                        raw_backlog,
                        profile_rewrite_backlog,
                    });
                }

                let (plan, compaction_limit) = {
                    let storage = storage.blocking_read();
                    let backlog = storage.compaction_backlog_count()?;
                    let compaction_limit = if backlog >= ACTIVE_SYNC_COMPACTION_CATCH_UP_BACKLOG {
                        ACTIVE_SYNC_COMPACTION_CATCH_UP_LIMIT
                    } else {
                        compaction_limit
                    };
                    (
                        storage.segment_compaction_plan(compaction_limit)?,
                        compaction_limit,
                    )
                };

                debug_assert!(plan.len() <= compaction_limit);
                let compacted = plan.compact()?;
                let storage = storage.blocking_read();
                let raw_backlog = storage.raw_compaction_backlog_count()?;
                let total_backlog = storage.compaction_backlog_count()?;
                Ok(CompactionReport {
                    compacted,
                    raw_backlog: Some(raw_backlog),
                    profile_rewrite_backlog: Some(total_backlog.saturating_sub(raw_backlog)),
                })
            })
            .await
            {
                Ok(Ok(report)) => {
                    let refreshed_active_backlog = active_sync && report.raw_backlog.is_some();
                    update_compaction_status(&state, report);
                    if active_sync {
                        if let Some(raw_backlog) = report.raw_backlog {
                            last_active_raw_backlog = Some(raw_backlog);
                        }
                    } else {
                        last_active_raw_backlog = report.raw_backlog;
                    }
                    if refreshed_active_backlog {
                        last_active_backlog_refresh = Some(std::time::Instant::now());
                    }
                    if report.compacted > 0 {
                        tracing::info!(
                            segments = report.compacted,
                            raw_backlog = report.raw_backlog,
                            profile_rewrite_backlog = report.profile_rewrite_backlog,
                            "compacted sealed segments behind the safety margin"
                        );
                    }
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "failed to compact eligible sealed segments");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "background compaction task failed");
                }
            }
        }

        let active_sync = sync_is_active(&state) || historical_sync_is_incomplete(&state).await;
        let sealed_index_limit = sealed_index_segment_limit(active_sync);

        let (sealed_max_id, sealed_targets) = {
            let storage = state.storage.read().await;
            sealed_query_index_targets(&storage, sealed_index_limit)
        };
        if sealed_index_scan_complete_for_max_id != sealed_max_id {
            if sealed_targets.is_empty() {
                sealed_index_scan_complete_for_max_id = sealed_max_id;
            } else {
                let target_count = sealed_targets.len();
                let result = tokio::task::spawn_blocking(move || -> io::Result<Vec<u64>> {
                    let mut indexed = Vec::with_capacity(sealed_targets.len());
                    for target in sealed_targets {
                        IndexBuilder::build_missing_indexes(
                            &target.path,
                            IndexBuildProfile::Erc20Transfer,
                        )?;
                        indexed.push(target.segment_id);
                    }
                    Ok(indexed)
                })
                .await;

                match result {
                    Ok(Ok(indexed_segment_ids)) => {
                        let indexed = indexed_segment_ids.len();
                        {
                            let mut storage = state.storage.write().await;
                            for segment_id in indexed_segment_ids {
                                if let Err(e) = storage.refresh_segment_manifest(segment_id) {
                                    tracing::warn!(
                                        error = %e,
                                        segment_id,
                                        "failed to refresh segment manifest after sealed index build"
                                    );
                                }
                            }
                        }
                        tracing::info!(
                            indexed,
                            target_count,
                            "built missing query indexes for sealed segments"
                        );
                        sealed_index_scan_complete_for_max_id = None;
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(
                            error = %e,
                            "failed to build missing sealed query indexes"
                        );
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "sealed query index task panicked");
                    }
                }
            }
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
                    {
                        let mut storage = state.storage.write().await;
                        if let Err(e) = storage.refresh_segment_indexes(current.partition_id) {
                            tracing::warn!(
                                error = %e,
                                partition_id = current.partition_id,
                                "failed to refresh segment manifest after hot index rebuild"
                            );
                        }
                    }
                    tracing::debug!(
                        partition_id = current.partition_id,
                        rows = current.row_count,
                        "indexes rebuilt"
                    );
                    last_indexed = Some(current);
                }
                Ok(Err(e)) => {
                    let latest = {
                        let storage = state.storage.read().await;
                        HotIndexState {
                            partition_id: storage.hot_partition().meta.id,
                            row_count: storage.hot_partition().meta.row_count,
                            path: storage.hot_partition().meta.path.clone(),
                        }
                    };
                    if index_build_error_is_transient(&e, &current, &latest) {
                        tracing::debug!(
                            error = %e,
                            partition_id = current.partition_id,
                            previous_rows = current.row_count,
                            current_rows = latest.row_count,
                            "hot partition changed during index rebuild, retrying later"
                        );
                    } else {
                        tracing::warn!(error = %e, "failed to build indexes");
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "index build task panicked");
                }
            }
        }
    }
}

fn sync_is_active(state: &AppState) -> bool {
    let status = state
        .sync_status
        .lock()
        .expect("sync status mutex poisoned");
    should_defer_background_indexing(&status)
}

fn update_compaction_status(state: &AppState, report: CompactionReport) {
    let mut status = state
        .sync_status
        .lock()
        .expect("sync status mutex poisoned");
    if let Some(raw_backlog) = report.raw_backlog {
        status.raw_log_segment_backlog = Some(raw_backlog);
    }
    if let Some(profile_rewrite_backlog) = report.profile_rewrite_backlog {
        status.storage_profile_rewrite_backlog = Some(profile_rewrite_backlog);
    }
}

fn should_defer_background_indexing(status: &SyncStatus) -> bool {
    status.historical_eta_seconds.is_some()
}

fn sealed_index_segment_limit(active_sync: bool) -> usize {
    if active_sync {
        ACTIVE_SYNC_SEALED_INDEX_SEGMENT_LIMIT
    } else {
        BACKGROUND_SEALED_INDEX_SEGMENT_LIMIT
    }
}

fn active_sync_compaction_limits(raw_backlog: usize, base_limit: usize) -> (usize, usize) {
    let raw_limit = if raw_backlog >= ACTIVE_SYNC_COMPACTION_HIGH_BACKLOG {
        ACTIVE_SYNC_COMPACTION_HIGH_CATCH_UP_LIMIT
    } else if raw_backlog >= ACTIVE_SYNC_COMPACTION_CATCH_UP_BACKLOG {
        ACTIVE_SYNC_COMPACTION_CATCH_UP_LIMIT
    } else {
        base_limit
    };
    let profile_rewrite_limit = if raw_backlog == 0 {
        ACTIVE_SYNC_PROFILE_REWRITE_SEGMENT_LIMIT
    } else {
        0
    };

    (raw_limit, profile_rewrite_limit)
}

fn active_sync_compaction_memory_pressure() -> Option<u64> {
    let available_memory_bytes = linux_available_memory_bytes()?;
    (available_memory_bytes < ACTIVE_SYNC_COMPACTION_MIN_AVAILABLE_MEMORY_BYTES)
        .then_some(available_memory_bytes)
}

fn linux_available_memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        let Some(rest) = line.strip_prefix("MemAvailable:") else {
            continue;
        };
        let kib = rest
            .split_whitespace()
            .next()
            .and_then(|value| value.parse::<u64>().ok())?;
        return kib.checked_mul(BYTES_PER_KIB);
    }
    None
}

async fn historical_sync_is_incomplete(state: &AppState) -> bool {
    let historical_sync_disabled = state
        .sync_status
        .lock()
        .expect("sync status mutex poisoned")
        .historical_sync_disabled;
    if historical_sync_disabled {
        return false;
    }

    let storage = state.storage.read().await;
    should_defer_for_historical_floor(storage.historical_floor().map(|marker| marker.block_number))
}

fn should_defer_for_historical_floor(floor_block: Option<u64>) -> bool {
    floor_block.is_some_and(|block| block > EXECUTION_HISTORY_TARGET_BLOCK)
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct HotIndexState {
    partition_id: u64,
    row_count: u64,
    path: PathBuf,
}

fn index_build_error_is_transient(
    error: &io::Error,
    before: &HotIndexState,
    after: &HotIndexState,
) -> bool {
    error.kind() == io::ErrorKind::InvalidData && before != after
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct SealedIndexTarget {
    segment_id: u64,
    path: PathBuf,
}

fn sealed_query_index_targets(
    storage: &PartitionManager,
    limit: usize,
) -> (Option<u64>, Vec<SealedIndexTarget>) {
    let max_segment_id = storage
        .sealed_partitions()
        .iter()
        .map(|partition| partition.meta.id)
        .max();
    if limit == 0 {
        return (max_segment_id, Vec::new());
    }

    let targets = storage
        .sealed_partitions()
        .iter()
        .filter(|partition| partition.meta.row_count > 0)
        .filter(|partition| query_indexes_missing(&partition.meta.path))
        .take(limit)
        .map(|partition| SealedIndexTarget {
            segment_id: partition.meta.id,
            path: partition.meta.path.clone(),
        })
        .collect();

    (max_segment_id, targets)
}

fn query_indexes_missing(path: &Path) -> bool {
    let index_dir = path.join("indexes");
    IndexBuilder::required_index_files(IndexBuildProfile::Erc20Transfer)
        .iter()
        .any(|file_name| !index_dir.join(file_name).is_file())
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

    #[test]
    fn transient_index_build_errors_require_invalid_data_and_changed_hot_state() {
        let before = HotIndexState {
            partition_id: 7,
            row_count: 200,
            path: PathBuf::from("/tmp/hot"),
        };
        let after = HotIndexState {
            partition_id: 7,
            row_count: 220,
            path: PathBuf::from("/tmp/hot"),
        };
        let invalid = io::Error::new(io::ErrorKind::InvalidData, "corrupt header");
        let other = io::Error::new(io::ErrorKind::NotFound, "missing");

        assert!(index_build_error_is_transient(&invalid, &before, &after));
        assert!(!index_build_error_is_transient(&invalid, &before, &before));
        assert!(!index_build_error_is_transient(&other, &before, &after));
    }

    #[test]
    fn defer_background_indexing_only_while_historical_eta_is_active() {
        let live_following = SyncStatus {
            syncing: true,
            ..Default::default()
        };
        let historical = SyncStatus {
            historical_eta_seconds: Some(120.0),
            ..Default::default()
        };
        let idle = SyncStatus::default();

        assert!(!should_defer_background_indexing(&live_following));
        assert!(should_defer_background_indexing(&historical));
        assert!(!should_defer_background_indexing(&idle));
    }

    #[test]
    fn sealed_query_indexing_continues_during_active_sync_at_lower_batch_size() {
        assert_eq!(
            sealed_index_segment_limit(true),
            ACTIVE_SYNC_SEALED_INDEX_SEGMENT_LIMIT
        );
        assert_eq!(
            sealed_index_segment_limit(false),
            BACKGROUND_SEALED_INDEX_SEGMENT_LIMIT
        );
        assert!(sealed_index_segment_limit(true) < sealed_index_segment_limit(false));
    }

    #[test]
    fn query_index_missing_requires_every_common_erc20_event_index() {
        let tmp = tempfile::TempDir::new().unwrap();
        let indexes = tmp.path().join("indexes");
        std::fs::create_dir_all(&indexes).unwrap();
        assert!(query_indexes_missing(tmp.path()));

        for file_name in IndexBuilder::required_index_files(IndexBuildProfile::Erc20Transfer) {
            std::fs::write(indexes.join(file_name), []).unwrap();
        }
        assert!(!query_indexes_missing(tmp.path()));
    }

    #[test]
    fn defer_background_indexing_until_historical_floor_reaches_target() {
        assert!(should_defer_for_historical_floor(Some(
            EXECUTION_HISTORY_TARGET_BLOCK + 1
        )));
        assert!(!should_defer_for_historical_floor(Some(
            EXECUTION_HISTORY_TARGET_BLOCK
        )));
        assert!(!should_defer_for_historical_floor(None));
    }

    #[test]
    fn active_sync_compaction_defers_profile_rewrites_until_raw_backlog_clears() {
        assert_eq!(
            active_sync_compaction_limits(1, ACTIVE_SYNC_COMPACTION_SEGMENT_LIMIT),
            (ACTIVE_SYNC_COMPACTION_SEGMENT_LIMIT, 0)
        );
        assert_eq!(
            active_sync_compaction_limits(0, ACTIVE_SYNC_COMPACTION_SEGMENT_LIMIT),
            (
                ACTIVE_SYNC_COMPACTION_SEGMENT_LIMIT,
                ACTIVE_SYNC_PROFILE_REWRITE_SEGMENT_LIMIT
            )
        );
    }

    #[test]
    fn active_sync_compaction_uses_catch_up_limit_for_large_raw_backlog() {
        assert_eq!(
            active_sync_compaction_limits(
                ACTIVE_SYNC_COMPACTION_CATCH_UP_BACKLOG,
                ACTIVE_SYNC_COMPACTION_SEGMENT_LIMIT
            ),
            (ACTIVE_SYNC_COMPACTION_CATCH_UP_LIMIT, 0)
        );
        assert_eq!(
            active_sync_compaction_limits(
                ACTIVE_SYNC_COMPACTION_HIGH_BACKLOG,
                ACTIVE_SYNC_COMPACTION_SEGMENT_LIMIT
            ),
            (ACTIVE_SYNC_COMPACTION_HIGH_CATCH_UP_LIMIT, 0)
        );
    }
}
