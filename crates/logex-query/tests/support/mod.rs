//! Explicit raw-data execution for relational, storage and resource fixtures.
//! These helpers do not assert that generated/captured rows form Ethereum history.
#![allow(dead_code)] // Shared by independent integration-test targets.
use logex_query::{
    NativeStorageSnapshot, QueryCancelCheck, SqlQueryError, SqlQueryPage, SqlQueryResult,
};
use logex_storage::{PartitionManager, native::NativeLogFilter};
use logex_types::LogRow;

pub fn execute_log_filter(
    storage: &PartitionManager,
    filter: &NativeLogFilter,
) -> std::io::Result<Vec<LogRow>> {
    execute_log_filter_with_cancel(storage, filter, None)
}
pub fn execute_log_filter_with_cancel(
    storage: &PartitionManager,
    filter: &NativeLogFilter,
    cancel: Option<&QueryCancelCheck>,
) -> std::io::Result<Vec<LogRow>> {
    logex_query::execute_log_filter_on_snapshot_with_cancel(
        &NativeStorageSnapshot::for_unverified_inspection(storage),
        filter,
        cancel,
    )
}
pub async fn execute_sql(
    sql: &str,
    storage: &PartitionManager,
    head: Option<u64>,
) -> Result<SqlQueryResult, SqlQueryError> {
    execute_sql_page(sql, storage, head, SqlQueryPage::default()).await
}
pub async fn execute_sql_page(
    sql: &str,
    storage: &PartitionManager,
    head: Option<u64>,
    page: SqlQueryPage,
) -> Result<SqlQueryResult, SqlQueryError> {
    execute_sql_page_with_cancel(sql, storage, head, page, None).await
}
pub async fn execute_sql_page_with_cancel(
    sql: &str,
    storage: &PartitionManager,
    head: Option<u64>,
    page: SqlQueryPage,
    cancel: Option<QueryCancelCheck>,
) -> Result<SqlQueryResult, SqlQueryError> {
    logex_query::execute_sql_page_on_snapshot(
        sql,
        NativeStorageSnapshot::for_unverified_inspection(storage),
        head.or_else(|| storage.head_block()).unwrap_or(0),
        page,
        cancel,
    )
    .await
}

/// An actually verified empty block, for zero-row execution/memory controls.
pub fn empty_verified_storage() -> (tempfile::TempDir, PartitionManager) {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(logex_storage::PartitionManagerConfig {
        data_dir: dir.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    let header = alloy_consensus::Header::default();
    let block = logex_storage::VerifiedBlockLogs::from_empty_header(&header).unwrap();
    storage
        .ingest_verified_canonical_batch(&[], &[block], &[header], None)
        .unwrap();
    (dir, storage)
}
