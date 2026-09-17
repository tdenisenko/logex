use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use logex_index::{IndexBuildProfile, IndexBuilder};
use logex_server::AppState;
use logex_storage::PartitionManager;
use logex_storage::native::{CompactionMode, SegmentCompactionTask};
use logex_types::{EXECUTION_HISTORY_TARGET_BLOCK, SyncStatus};

const TASK_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(60);
const ACTIVE_SYNC_COMPACTION_SEGMENT_LIMIT: usize = 1;
const ACTIVE_SYNC_COMPACTION_CATCH_UP_LIMIT: usize = 4;
const ACTIVE_SYNC_COMPACTION_HIGH_CATCH_UP_LIMIT: usize = 8;
const ACTIVE_SYNC_COMPACTION_CATCH_UP_BACKLOG: usize = 1_024;
const ACTIVE_SYNC_COMPACTION_HIGH_BACKLOG: usize = 4_096;
const ACTIVE_SYNC_PROFILE_REWRITE_SEGMENT_LIMIT: usize = 2;
const BACKGROUND_COMPACTION_SEGMENT_LIMIT: usize = 24;
const BACKGROUND_SEALED_INDEX_SEGMENT_LIMIT: usize = 8;
const INDEX_CANDIDATE_BATCH_SIZE: usize = 64;
const COMPACTION_CANDIDATE_BATCH_SIZE: usize = 64;
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

/// Checkpoint idle ingestion and periodically compact/build derived indexes.
pub async fn run_background_indexer(
    state: Arc<AppState>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> io::Result<()> {
    let mut last_indexed: Option<HotIndexState> = None;
    let mut last_active_backlog_refresh: Option<std::time::Instant> = None;
    let mut last_active_raw_backlog: Option<usize> = None;
    let mut ticker = tokio::time::interval(BACKGROUND_COMPACTION_INTERVAL);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = wait_for_shutdown(&mut shutdown) => {
                tracing::info!("background indexer shutting down");
                break;
            }
            _ = ticker.tick() => {}
        }

        // Small live epochs must also checkpoint when the node becomes idle.
        // Keep filesystem work off async workers and run before compaction/index
        // selection so newly durable sealed segments become eligible together.
        let storage = Arc::clone(&state.storage);
        join_background_worker(
            "storage checkpoint",
            tokio::task::spawn_blocking(move || storage.blocking_write().checkpoint_if_due()),
        )
        .await?
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("storage checkpoint failed; writes require recovery: {error}"),
            )
        })?;

        {
            let active_sync_status = sync_is_active(&state);
            let historical_incomplete = historical_sync_is_incomplete(&state).await;
            let active_sync = active_sync_status || historical_incomplete;
            if should_defer_compaction_for_historical_sync(historical_incomplete) {
                tracing::debug!(
                    "skipping background compaction while historical sync is incomplete"
                );
                continue;
            }
            if active_sync
                && let Some(available_memory_bytes) = active_sync_compaction_memory_pressure()
            {
                let storage = Arc::clone(&state.storage);
                let worker =
                    tokio::task::spawn_blocking(move || -> io::Result<CompactionReport> {
                        Ok(CompactionReport {
                            compacted: 0,
                            raw_backlog: Some(compaction_backlog(
                                &storage,
                                CompactionMode::RawOnly,
                            )?),
                            profile_rewrite_backlog: None,
                        })
                    });
                match join_background_worker("compaction backlog refresh", worker).await? {
                    Ok(report) => update_compaction_status(&state, report),
                    Err(error) => {
                        tracing::warn!(
                            %error,
                            "failed to refresh compaction backlog under memory pressure"
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
            let worker = tokio::task::spawn_blocking(move || -> io::Result<CompactionReport> {
                if active_sync {
                    let (
                        raw_plan,
                        profile_plan,
                        raw_backlog_before,
                        profile_rewrite_backlog,
                        compaction_limit,
                    ) = {
                        let raw_backlog_before = if refresh_active_backlog {
                            Some(compaction_backlog(&storage, CompactionMode::RawOnly)?)
                        } else {
                            None
                        };
                        let profile_rewrite_backlog = if refresh_active_backlog {
                            Some(compaction_backlog(
                                &storage,
                                CompactionMode::ProfileRewrite,
                            )?)
                        } else {
                            None
                        };
                        let raw_backlog_for_limits =
                            raw_backlog_before.or(active_raw_backlog_hint).unwrap_or(1);
                        let (compaction_limit, profile_rewrite_limit) =
                            active_sync_compaction_limits(raw_backlog_for_limits, compaction_limit);
                        (
                            compaction_plan(
                                &storage,
                                compaction_limit,
                                CompactionMode::RawOnly,
                                true,
                            )?,
                            compaction_plan(
                                &storage,
                                profile_rewrite_limit,
                                CompactionMode::ProfileRewrite,
                                false,
                            )?,
                            raw_backlog_before,
                            profile_rewrite_backlog,
                            compaction_limit,
                        )
                    };

                    debug_assert!(raw_plan.len() <= compaction_limit);
                    let raw_plan_len = raw_plan.len();
                    let compacted = compact_tasks(&raw_plan)? + compact_tasks(&profile_plan)?;
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
                    let backlog = compaction_backlog(&storage, CompactionMode::CurrentProfile)?;
                    let compaction_limit = if backlog >= ACTIVE_SYNC_COMPACTION_CATCH_UP_BACKLOG {
                        ACTIVE_SYNC_COMPACTION_CATCH_UP_LIMIT
                    } else {
                        compaction_limit
                    };
                    (
                        compaction_plan(
                            &storage,
                            compaction_limit,
                            CompactionMode::CurrentProfile,
                            false,
                        )?,
                        compaction_limit,
                    )
                };

                debug_assert!(plan.len() <= compaction_limit);
                let compacted = compact_tasks(&plan)?;
                let raw_backlog = compaction_backlog(&storage, CompactionMode::RawOnly)?;
                let total_backlog = compaction_backlog(&storage, CompactionMode::CurrentProfile)?;
                Ok(CompactionReport {
                    compacted,
                    raw_backlog: Some(raw_backlog),
                    profile_rewrite_backlog: Some(total_backlog.saturating_sub(raw_backlog)),
                })
            });
            match join_background_worker("storage compaction", worker).await? {
                Ok(report) => {
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
                Err(e) => {
                    tracing::warn!(error = %e, "failed to compact eligible sealed segments");
                }
            }
        }

        let active_sync = sync_is_active(&state) || historical_sync_is_incomplete(&state).await;
        if should_defer_query_indexing(active_sync) {
            continue;
        }
        let storage = Arc::clone(&state.storage);
        let worker = tokio::task::spawn_blocking(move || {
            build_sealed_query_indexes(
                &storage,
                BACKGROUND_SEALED_INDEX_SEGMENT_LIMIT,
                query_indexes_missing,
                |path| IndexBuilder::build_missing_indexes(path, IndexBuildProfile::Erc20Transfer),
            )
        });
        match join_background_worker("sealed query index", worker).await? {
            Ok(indexed) if indexed > 0 => {
                tracing::info!(indexed, "built missing query indexes for sealed segments");
            }
            Ok(_) => {}
            Err(error) => {
                tracing::warn!(%error, "failed to build missing sealed query indexes");
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

        if current.row_count > 0 {
            let rebuild_for_rows = should_rebuild_hot_indexes(last_indexed.as_ref(), &current);
            let path = current.path.clone();
            // Opening source/checkpoint/index files is filesystem work too.
            // Keep freshness checks on the same blocking worker as the build.
            let worker = tokio::task::spawn_blocking(move || {
                rebuild_hot_query_indexes(&path, rebuild_for_rows)
            });
            match join_background_worker("hot query index", worker).await? {
                Ok(true) => {
                    tracing::debug!(
                        partition_id = current.partition_id,
                        rows = current.row_count,
                        "indexes rebuilt"
                    );
                    last_indexed = Some(current);
                }
                Ok(false) => {}
                Err(e) => {
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
            }
        }
    }
    Ok(())
}

/// A failed blocking worker terminates the owning indexer and reaches its node
/// supervisor. Keep the operation's own result separate: optional maintenance
/// I/O errors retain their existing retry/logging policy at each caller.
async fn join_background_worker<T>(
    name: &str,
    handle: tokio::task::JoinHandle<T>,
) -> io::Result<T> {
    handle
        .await
        .map_err(|error| io::Error::other(format!("{name} worker failed: {error}")))
}

/// Visit a bounded metadata batch at a time without retaining the ingestion
/// lock during filesystem work. Catalog positions keep their original order;
/// additions after the initial length are left to the next maintenance pass.
fn visit_compaction_candidates(
    storage: &tokio::sync::RwLock<PartitionManager>,
    newest_first: bool,
    mut visit: impl FnMut(SegmentCompactionTask) -> io::Result<bool>,
) -> io::Result<()> {
    let end = storage.blocking_read().compaction_candidate_count();
    let mut cursor = if newest_first { end } else { 0 };
    while if newest_first {
        cursor > 0
    } else {
        cursor < end
    } {
        let range = if newest_first {
            cursor.saturating_sub(COMPACTION_CANDIDATE_BATCH_SIZE)..cursor
        } else {
            cursor
                ..cursor
                    .saturating_add(COMPACTION_CANDIDATE_BATCH_SIZE)
                    .min(end)
        };
        cursor = if newest_first { range.start } else { range.end };
        let mut candidates = storage.blocking_read().compaction_candidates(range)?;
        if newest_first {
            candidates.reverse();
        }
        for candidate in candidates {
            if !visit(candidate)? {
                return Ok(());
            }
        }
    }
    Ok(())
}

fn compaction_backlog(
    storage: &tokio::sync::RwLock<PartitionManager>,
    mode: CompactionMode,
) -> io::Result<usize> {
    let mut count = 0;
    visit_compaction_candidates(storage, false, |candidate| {
        if candidate.needs_compaction(mode)? {
            count += 1;
        }
        Ok(true)
    })?;
    Ok(count)
}

fn compaction_plan(
    storage: &tokio::sync::RwLock<PartitionManager>,
    limit: usize,
    mode: CompactionMode,
    newest_first: bool,
) -> io::Result<Vec<SegmentCompactionTask>> {
    let mut plan = Vec::new();
    if limit != 0 {
        visit_compaction_candidates(storage, newest_first, |candidate| {
            if candidate.needs_compaction(mode)? {
                plan.push(candidate);
            }
            Ok(plan.len() < limit)
        })?;
    }
    Ok(plan)
}

fn compact_tasks(tasks: &[SegmentCompactionTask]) -> io::Result<usize> {
    for task in tasks {
        task.compact()?;
    }
    Ok(tasks.len())
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

fn should_defer_query_indexing(active_sync: bool) -> bool {
    active_sync
}

fn should_defer_compaction_for_historical_sync(historical_incomplete: bool) -> bool {
    historical_incomplete
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

pub async fn join_task(name: &str, handle: tokio::task::JoinHandle<()>) -> io::Result<()> {
    join_task_with_timeout(name, handle, TASK_SHUTDOWN_TIMEOUT).await
}

async fn join_task_with_timeout(
    name: &str,
    mut handle: tokio::task::JoinHandle<()>,
    shutdown_timeout: Duration,
) -> io::Result<()> {
    match tokio::time::timeout(shutdown_timeout, &mut handle).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(io::Error::other(format!(
            "{name} failed during cleanup: {error}"
        ))),
        Err(_) => {
            handle.abort();
            // Started blocking work cannot be forcibly canceled. Do not await
            // it again here; the node's independent shutdown deadline remains
            // armed through runtime destruction and this result forces failure.
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("{name} did not stop within {shutdown_timeout:?}"),
            ))
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
    error.kind() == io::ErrorKind::WouldBlock
        || (error.kind() == io::ErrorKind::InvalidData && before != after)
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

/// Capture only bounded metadata under the storage lock; every filesystem
/// freshness check and index build runs after the guard is released. The caller
/// runs this synchronous helper on a blocking worker and retains storage ownership.
fn build_sealed_query_indexes(
    storage: &tokio::sync::RwLock<PartitionManager>,
    limit: usize,
    mut missing: impl FnMut(&Path) -> Option<bool>,
    mut build: impl FnMut(&Path) -> io::Result<()>,
) -> io::Result<usize> {
    if limit == 0 {
        return Ok(0);
    }
    // This is an advisory maintenance pass, not a query/coverage snapshot.
    // Bound it to the initial list length so concurrent rotation cannot extend
    // it forever. Changed positions/new segments are revisited on the next tick;
    // each actual build independently validates its current source publication.
    let end = storage.blocking_read().sealed_partitions().len();
    let mut cursor = 0;
    let mut indexed = 0;
    while cursor < end {
        let candidates = {
            let storage = storage.blocking_read();
            let next = cursor.saturating_add(INDEX_CANDIDATE_BATCH_SIZE).min(end);
            let candidates = sealed_index_candidates(&storage, cursor..next);
            cursor = next;
            candidates
        };
        for path in candidates {
            if missing(&path) == Some(true) {
                build(&path)?;
                indexed += 1;
                if indexed == limit {
                    return Ok(indexed);
                }
            }
        }
    }
    Ok(indexed)
}

fn sealed_index_candidates(
    storage: &PartitionManager,
    range: std::ops::Range<usize>,
) -> Vec<PathBuf> {
    let active_historical_segment = storage.active_historical_segment_id();
    storage
        .sealed_partitions()
        .iter()
        .skip(range.start)
        .take(range.len())
        .filter(|partition| partition.meta.row_count > 0)
        .filter(|partition| Some(partition.meta.id) != active_historical_segment)
        .map(|partition| partition.meta.path.clone())
        .collect()
}

fn rebuild_hot_query_indexes(path: &Path, rebuild_for_rows: bool) -> io::Result<bool> {
    match query_indexes_missing(path) {
        Some(missing) if missing || rebuild_for_rows => {
            IndexBuilder::build_all_indexes(path)?;
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn query_indexes_missing(path: &Path) -> Option<bool> {
    match IndexBuilder::indexes_missing(path, IndexBuildProfile::Erc20Transfer) {
        Ok(missing) => Some(missing),
        Err(error) if error.kind() == io::ErrorKind::Unsupported => {
            tracing::debug!(path = %path.display(), %error, "source is not eligible for indexes");
            None
        }
        // Other failures are handled by the subsequent build's error path.
        Err(_) => Some(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index_row(number: u64) -> logex_types::LogRow {
        logex_types::LogRow {
            block_number: number,
            block_hash: alloy_primitives::B256::ZERO,
            timestamp: number,
            tx_hash: alloy_primitives::B256::ZERO,
            tx_index: 0,
            log_index: 0,
            address: alloy_primitives::Address::ZERO,
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: alloy_primitives::Bytes::new(),
            data_len: 0,
            source: logex_types::Source::Receipt,
        }
    }

    fn index_storage(path: &Path, segments: usize) -> PartitionManager {
        let mut storage = PartitionManager::open(logex_storage::PartitionManagerConfig {
            data_dir: path.to_owned(),
            partition_target_rows: 1,
            ..Default::default()
        })
        .unwrap();
        storage
            .write_batch(
                &(0..segments)
                    .map(|i| index_row(i as u64 + 1))
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        assert_eq!(storage.sealed_partitions().len(), segments);
        storage
    }

    fn compaction_storage(path: &Path, segments: usize) -> PartitionManager {
        let mut storage = index_storage(path, segments);
        storage
            .record_sync_head(10_000, alloy_primitives::B256::ZERO, 10_000)
            .unwrap();
        storage.checkpoint_durable().unwrap();
        storage
    }

    #[test]
    fn compaction_inspection_and_execution_release_the_ingestion_lock() {
        let root = tempfile::tempdir().unwrap();
        let storage = tokio::sync::RwLock::new(compaction_storage(root.path(), 3));
        let mut compacted = 0;
        visit_compaction_candidates(&storage, false, |candidate| {
            // Keep the writer throughout the actual filesystem operations, so
            // this checks both the visit boundary and the task's independence.
            let _writer = storage
                .try_write()
                .expect("candidate retained storage guard");
            assert!(candidate.needs_compaction(CompactionMode::RawOnly)?);
            candidate.compact()?;
            compacted += 1;
            assert!(!candidate.needs_compaction(CompactionMode::CurrentProfile)?);
            Ok(true)
        })
        .unwrap();
        assert_eq!(compacted, 3);
        assert_eq!(
            compaction_backlog(&storage, CompactionMode::RawOnly).unwrap(),
            0
        );
    }

    #[test]
    fn compaction_selection_preserves_order_modes_and_limits() {
        let root = tempfile::tempdir().unwrap();
        let storage = tokio::sync::RwLock::new(compaction_storage(root.path(), 4));
        let expected = storage
            .blocking_read()
            .sealed_partitions()
            .iter()
            .map(|partition| partition.meta.id)
            .collect::<Vec<_>>();
        let oldest = compaction_plan(&storage, 2, CompactionMode::RawOnly, false).unwrap();
        assert_eq!(
            oldest
                .iter()
                .map(|task| task.segment_id())
                .collect::<Vec<_>>(),
            expected[..2]
        );
        let newest = compaction_plan(&storage, 2, CompactionMode::RawOnly, true).unwrap();
        assert_eq!(
            newest
                .iter()
                .map(|task| task.segment_id())
                .collect::<Vec<_>>(),
            expected[2..].iter().rev().copied().collect::<Vec<_>>()
        );
        assert!(
            compaction_plan(&storage, 8, CompactionMode::ProfileRewrite, false)
                .unwrap()
                .is_empty()
        );
        assert_eq!(compact_tasks(&oldest).unwrap(), 2);
        for mode in [CompactionMode::RawOnly, CompactionMode::CurrentProfile] {
            assert_eq!(compaction_backlog(&storage, mode).unwrap(), 2);
            let tasks = compaction_plan(&storage, 8, mode, false).unwrap();
            assert_eq!(
                tasks
                    .iter()
                    .map(|task| task.segment_id())
                    .collect::<Vec<_>>(),
                expected[2..]
            );
        }
        assert_eq!(
            compaction_backlog(&storage, CompactionMode::ProfileRewrite).unwrap(),
            0
        );
        let _writer = storage.blocking_write();
        assert!(
            compaction_plan(&storage, 0, CompactionMode::RawOnly, false)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn compaction_scan_batches_metadata_and_bounds_concurrent_growth() {
        let root = tempfile::tempdir().unwrap();
        let initial = COMPACTION_CANDIDATE_BATCH_SIZE + 1;
        let storage = tokio::sync::RwLock::new(compaction_storage(root.path(), initial));
        let mut visited = Vec::new();
        visit_compaction_candidates(&storage, true, |candidate| {
            if visited.is_empty() {
                let mut writer = storage.try_write().unwrap();
                writer.write_batch(&[index_row(1_000)])?;
                writer.checkpoint_durable()?;
            }
            assert!(candidate.needs_compaction(CompactionMode::RawOnly)?);
            visited.push(candidate.segment_id());
            Ok(true)
        })
        .unwrap();
        let expected = storage
            .blocking_read()
            .sealed_partitions()
            .iter()
            .map(|partition| partition.meta.id)
            .collect::<Vec<_>>();
        assert_eq!(
            visited,
            expected[..initial]
                .iter()
                .rev()
                .copied()
                .collect::<Vec<_>>()
        );
        assert_eq!(
            compaction_backlog(&storage, CompactionMode::RawOnly).unwrap(),
            initial + 1
        );
    }

    #[test]
    fn sealed_index_probe_and_build_release_the_ingestion_lock() {
        let root = tempfile::tempdir().unwrap();
        let storage = tokio::sync::RwLock::new(index_storage(root.path(), 3));
        let indexed = build_sealed_query_indexes(
            &storage,
            2,
            |path| {
                assert!(
                    storage.try_write().is_ok(),
                    "freshness probe holds storage lock"
                );
                query_indexes_missing(path)
            },
            |path| {
                assert!(
                    storage.try_write().is_ok(),
                    "index build holds storage lock"
                );
                IndexBuilder::build_missing_indexes(path, IndexBuildProfile::Erc20Transfer)
            },
        )
        .unwrap();
        assert_eq!(indexed, 2);
        let paths = sealed_index_candidates(&storage.blocking_read(), 0..3);
        assert_eq!(query_indexes_missing(&paths[0]), Some(false));
        assert_eq!(query_indexes_missing(&paths[1]), Some(false));
        assert_eq!(query_indexes_missing(&paths[2]), Some(true));
    }

    #[test]
    fn sealed_index_scan_batches_metadata_and_defers_concurrent_growth() {
        let root = tempfile::tempdir().unwrap();
        let initial = INDEX_CANDIDATE_BATCH_SIZE + 1;
        let storage = tokio::sync::RwLock::new(index_storage(root.path(), initial));
        let batch =
            sealed_index_candidates(&storage.blocking_read(), 0..INDEX_CANDIDATE_BATCH_SIZE);
        assert_eq!(batch.len(), INDEX_CANDIDATE_BATCH_SIZE);
        let mut checked = 0;
        let indexed = build_sealed_query_indexes(
            &storage,
            8,
            |_| {
                checked += 1;
                if checked == 1 {
                    // Real append/rotation during freshness inspection. The pass
                    // remains finite and the new candidate is seen on the next pass.
                    storage
                        .blocking_write()
                        .write_batch(&[index_row(1_000)])
                        .unwrap();
                }
                Some(false)
            },
            |_| panic!("current indexes must not rebuild"),
        )
        .unwrap();
        assert_eq!(indexed, 0);
        assert_eq!(checked, initial);
        let mut revisited = 0;
        build_sealed_query_indexes(
            &storage,
            8,
            |_| {
                revisited += 1;
                None
            },
            |_| panic!("ineligible source must not build"),
        )
        .unwrap();
        assert_eq!(revisited, initial + 1);
    }

    #[test]
    fn disabled_sealed_indexing_does_not_lock_or_probe_storage() {
        let root = tempfile::tempdir().unwrap();
        let storage = tokio::sync::RwLock::new(index_storage(root.path(), 1));
        let _writer = storage.blocking_write();
        assert_eq!(
            build_sealed_query_indexes(
                &storage,
                0,
                |_| panic!("disabled indexing must not probe"),
                |_| panic!("disabled indexing must not build")
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn hot_index_freshness_keeps_existing_build_policy() {
        let root = tempfile::tempdir().unwrap();
        logex_storage::ColumnFile::write_batch(root.path(), &[index_row(1)]).unwrap();
        assert!(rebuild_hot_query_indexes(root.path(), false).unwrap());
        assert!(!IndexBuilder::indexes_missing(root.path(), IndexBuildProfile::All).unwrap());
        assert!(!rebuild_hot_query_indexes(root.path(), false).unwrap());
        assert!(rebuild_hot_query_indexes(root.path(), true).unwrap());
        assert!(!rebuild_hot_query_indexes(root.path(), false).unwrap());
    }

    #[tokio::test]
    async fn cleanup_deadline_does_not_await_started_blocking_work_again() {
        let (release, blocked) = std::sync::mpsc::channel::<()>();
        let (started, observed) = tokio::sync::oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            started.send(()).unwrap();
            let _ = blocked.recv();
        });
        observed.await.unwrap();
        let outcome = tokio::time::timeout(
            Duration::from_millis(200),
            join_task_with_timeout("owned blocking control", task, Duration::ZERO),
        )
        .await;
        // Always release our own worker before asserting, including a failed
        // control. No test-runtime teardown or external process remains stuck.
        drop(release);
        let error = outcome
            .expect("the post-abort join exceeded its deadline")
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn cleanup_join_preserves_normal_completion_and_worker_failure() {
        join_task_with_timeout(
            "completed worker",
            tokio::spawn(async {}),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let task = tokio::spawn(async {
            panic!("isolated worker unwind control");
        });
        let error = join_task_with_timeout("failed worker", task, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("failed worker"));
        assert!(error.to_string().contains("panicked"));
    }

    #[tokio::test]
    async fn cleanup_join_reports_unexpected_cancellation() {
        let task = tokio::spawn(std::future::pending::<()>());
        task.abort();
        let error = join_task_with_timeout("canceled worker", task, Duration::from_secs(2))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("canceled worker"));
    }

    #[tokio::test]
    async fn indexer_stops_when_checkpoint_requires_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = PartitionManager::open(logex_storage::PartitionManagerConfig {
            data_dir: dir.path().to_owned(),
            ..Default::default()
        })
        .unwrap();
        let row = logex_types::LogRow {
            block_number: 1,
            block_hash: alloy_primitives::B256::repeat_byte(1),
            timestamp: 1,
            tx_hash: alloy_primitives::B256::repeat_byte(2),
            tx_index: 0,
            log_index: 0,
            address: alloy_primitives::Address::ZERO,
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: alloy_primitives::Bytes::new(),
            data_len: 0,
            source: logex_types::Source::Receipt,
        };
        storage.write_batch(&[row]).unwrap();
        // Only this temporary fixture is changed. Preserve its catalog, then
        // occupy the publication destination to force a real checkpoint error.
        let catalog = dir.path().join("catalog.json");
        std::fs::rename(&catalog, dir.path().join("saved-catalog.json")).unwrap();
        std::fs::create_dir(&catalog).unwrap();
        assert!(storage.checkpoint_durable().is_err());
        let error = storage.checkpoint_if_due().unwrap_err();
        assert!(error.to_string().contains("unfinished transaction"));
        let state = Arc::new(AppState::new(storage, None, SyncStatus::default()));
        let (_shutdown, receiver) = tokio::sync::watch::channel(false);
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            run_background_indexer(state, receiver),
        )
        .await;
        let error = result
            .expect("checkpoint failure must stop the indexer")
            .unwrap_err();
        assert!(error.to_string().contains("unfinished transaction"));
    }

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
        let busy = io::Error::new(io::ErrorKind::WouldBlock, "index reader or newer source");

        assert!(index_build_error_is_transient(&invalid, &before, &after));
        assert!(!index_build_error_is_transient(&invalid, &before, &before));
        assert!(!index_build_error_is_transient(&other, &before, &after));
        assert!(index_build_error_is_transient(&busy, &before, &before));
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
    fn sealed_query_indexing_is_deferred_during_active_sync() {
        assert!(should_defer_query_indexing(true));
        assert!(!should_defer_query_indexing(false));
    }

    #[test]
    fn compaction_is_deferred_while_historical_sync_is_incomplete() {
        assert!(should_defer_compaction_for_historical_sync(true));
        assert!(!should_defer_compaction_for_historical_sync(false));
    }

    #[test]
    fn query_index_missing_requires_every_common_erc20_event_index() {
        let tmp = tempfile::TempDir::new().unwrap();
        let indexes = tmp.path().join("indexes");
        std::fs::create_dir_all(&indexes).unwrap();
        assert_eq!(query_indexes_missing(tmp.path()), Some(true));

        for file_name in IndexBuilder::required_index_files(IndexBuildProfile::Erc20Transfer) {
            std::fs::write(indexes.join(file_name), []).unwrap();
        }
        assert_eq!(query_indexes_missing(tmp.path()), Some(true));
        logex_storage::ColumnFile::write_batch(tmp.path(), &[]).unwrap();
        IndexBuilder::build_indexes(tmp.path(), IndexBuildProfile::Erc20Transfer).unwrap();
        assert_eq!(query_indexes_missing(tmp.path()), Some(false));
    }

    #[test]
    fn sealed_query_indexing_skips_active_historical_staging_segment() {
        use alloy_primitives::{Address, B256, Bytes};
        use logex_storage::{PartitionManager, PartitionManagerConfig};
        use logex_types::{LogRow, Source};

        let tmp = tempfile::TempDir::new().unwrap();
        let mut storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 10,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        let rows = (0..5)
            .map(|index| LogRow {
                block_number: 100 + index,
                block_hash: B256::repeat_byte(0xAA),
                timestamp: 1_700_000_000 + index,
                tx_hash: B256::repeat_byte(0xBB),
                tx_index: 0,
                log_index: index as u32,
                address: Address::repeat_byte(0xCC),
                topic0: Some(B256::repeat_byte(0xDD)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: Bytes::new(),
                data_len: 0,
                source: Source::Receipt,
            })
            .collect::<Vec<_>>();
        storage.write_historical_batch(&rows).unwrap();

        let targets = sealed_index_candidates(&storage, 0..INDEX_CANDIDATE_BATCH_SIZE);

        assert!(storage.active_historical_segment_id().is_some());
        assert!(targets.is_empty());
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

#[cfg(test)]
mod maintenance_failure_tests {
    use super::*;

    #[tokio::test]
    async fn blocking_maintenance_worker_failure_is_fatal() {
        let worker = tokio::task::spawn_blocking(|| -> io::Result<()> {
            panic!("isolated maintenance worker failure");
        });
        let error = join_background_worker("fixture maintenance", worker)
            .await
            .expect_err("a failed worker must leave the maintenance loop");
        assert!(error.to_string().contains("fixture maintenance"));
    }

    #[tokio::test]
    async fn blocking_maintenance_failure_reaches_node_monitor() {
        let monitor = logex_sync::tasks::TaskMonitor::default();
        let mut failure = monitor.subscribe();
        let task = monitor.spawn_result("background indexer", async {
            let worker = tokio::task::spawn_blocking(|| -> io::Result<()> {
                panic!("isolated monitored maintenance worker failure");
            });
            let _operation = join_background_worker("fixture maintenance", worker).await?;
            // The original error-and-continue policy leaves the loop running.
            std::future::pending::<io::Result<()>>().await
        });
        let observed = tokio::time::timeout(Duration::from_secs(2), failure.changed()).await;
        monitor.begin_shutdown();
        task.abort();
        let _ = task.await;
        observed
            .expect("worker failure must notify the supervisor")
            .unwrap();
        let message = failure.borrow().clone().unwrap();
        assert!(message.contains("background indexer"));
        assert!(message.contains("fixture maintenance"));
    }

    #[tokio::test]
    async fn completed_maintenance_preserves_value_and_operation_error() {
        let value =
            join_background_worker("fixture maintenance", tokio::task::spawn_blocking(|| 7))
                .await
                .unwrap();
        assert_eq!(value, 7);
        let operation = join_background_worker(
            "fixture maintenance",
            tokio::task::spawn_blocking(|| {
                Err::<(), _>(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "fixture source changed",
                ))
            }),
        )
        .await
        .unwrap();
        let error = operation.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(error.to_string(), "fixture source changed");
    }

    #[tokio::test]
    async fn canceled_maintenance_worker_is_not_a_successful_operation() {
        let handle = tokio::spawn(std::future::pending::<io::Result<()>>());
        handle.abort();
        assert!(
            join_background_worker("fixture maintenance", handle)
                .await
                .is_err()
        );
    }
}
