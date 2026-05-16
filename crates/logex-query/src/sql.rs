use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use alloy_primitives::{Address, B256, keccak256};
use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, LargeStringArray,
    ListArray, ListBuilder, StringArray, StringBuilder, UInt32Array, UInt64Array, new_empty_array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use datafusion::catalog::Session;
use datafusion::common::ScalarValue;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::logical_expr::expr::InList;
use datafusion::logical_expr::{
    Between, BinaryExpr, Expr as DataFusionExpr, Operator, TableProviderFilterPushDown, TableType,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion::prelude::SessionContext;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator as SqlBinaryOperator, DuplicateTreatment, Expr as SqlAstExpr, FunctionArg,
    FunctionArgExpr, FunctionArguments, GroupByExpr, LimitClause, OrderByKind, SelectItem, SetExpr,
    Statement as SqlStatement, TableFactor, Value as SqlValue,
};
use num_bigint::{BigInt, BigUint};
use roaring::RoaringBitmap;
use serde_json::{Map, Value};

use logex_storage::native::{NativeLogFilter, TopicConstraint};
use logex_storage::{PartitionManager, SegmentReader};

use crate::lexer::{Token, tokenize};
use crate::native::{
    StorageSnapshot, candidate_row_ids, matches_native_filter, partition_matches_filter,
};

const DATAFUSION_BATCH_SIZE: usize = 4_096;
pub const DEFAULT_QUERY_PAGE_SIZE: usize = 50;

#[derive(Debug, thiserror::Error)]
pub enum SqlQueryError {
    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),
    #[error("sql error: {0}")]
    DataFusion(#[from] DataFusionError),
    #[error("legacy LogEx syntax error: {0}")]
    LegacySyntax(String),
}

#[derive(Debug)]
pub struct SqlQueryResult {
    pub rows: Vec<Value>,
    pub total_scanned: u64,
}

pub type QueryCancelCheck = Arc<dyn Fn() -> bool + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, Default)]
pub struct SqlQueryPage {
    pub limit: Option<usize>,
    pub offset: usize,
}

impl SqlQueryPage {
    pub fn new(limit: Option<usize>, offset: usize) -> Self {
        Self { limit, offset }
    }
}

struct LogexTableProvider {
    schema: SchemaRef,
    snapshot: StorageSnapshot,
    total_scanned: Arc<AtomicU64>,
    cancel_check: Option<QueryCancelCheck>,
}

impl std::fmt::Debug for LogexTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogexTableProvider")
            .field("schema", &self.schema)
            .field("snapshot", &self.snapshot)
            .field("total_scanned", &self.total_scanned)
            .finish_non_exhaustive()
    }
}

impl LogexTableProvider {
    fn new(
        snapshot: StorageSnapshot,
        total_scanned: Arc<AtomicU64>,
        cancel_check: Option<QueryCancelCheck>,
    ) -> Self {
        Self {
            schema: Arc::new(log_rows_schema()),
            snapshot,
            total_scanned,
            cancel_check,
        }
    }
}

#[async_trait]
impl TableProvider for LogexTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&DataFusionExpr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if supports_exact_pushdown(expr) {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[DataFusionExpr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        let filter = build_native_pushdown_filter(filters)?;
        let projected_schema = projected_schema(self.schema(), projection)?;
        let projected_columns = projected_column_names(self.schema(), projection);
        let mut scanned_rows = 0u64;
        let mut remaining_limit = limit;
        let mut generators: Vec<Arc<parking_lot::RwLock<dyn LazyBatchGenerator>>> = Vec::new();

        for partition in self.snapshot.partitions_in_order(filter.order) {
            check_query_canceled(self.cancel_check.as_ref())?;
            if !partition_matches_filter(&partition, &filter) {
                continue;
            }

            let mut row_ids = candidate_row_ids(&partition.path, &filter, true)
                .map_err(DataFusionError::IoError)?;
            if row_ids.is_empty() {
                continue;
            }
            if has_native_constraints(&filter) {
                row_ids = exact_candidate_row_ids(
                    &partition.path,
                    &filter,
                    row_ids,
                    self.cancel_check.as_ref(),
                )
                .map_err(DataFusionError::IoError)?;
                if row_ids.is_empty() {
                    continue;
                }
            }

            scanned_rows += row_ids.len() as u64;

            if let Some(limit) = remaining_limit {
                if row_ids.len() > limit {
                    row_ids.truncate(limit);
                }
                remaining_limit = Some(limit.saturating_sub(row_ids.len()));
            }

            generators.push(Arc::new(parking_lot::RwLock::new(
                LogSegmentBatchGenerator::new(
                    partition.path.clone(),
                    projected_schema.clone(),
                    projected_columns.clone(),
                    row_ids,
                    self.cancel_check.clone(),
                ),
            )));

            if remaining_limit == Some(0) {
                break;
            }
        }

        self.total_scanned.store(scanned_rows, Ordering::Relaxed);
        if generators.is_empty() {
            generators.push(Arc::new(parking_lot::RwLock::new(
                EmptyLogBatchGenerator::new(projected_schema.clone()),
            )));
        }

        Ok(Arc::new(LazyMemoryExec::try_new(
            projected_schema,
            generators,
        )?))
    }
}

fn has_native_constraints(filter: &NativeLogFilter) -> bool {
    filter.block_hash.is_some()
        || filter.from_block.is_some()
        || filter.to_block.is_some()
        || filter.from_timestamp.is_some()
        || filter.to_timestamp.is_some()
        || filter.data_len.is_some()
        || filter.data_min.is_some()
        || filter.data_max.is_some()
        || !filter.addresses.is_empty()
        || filter
            .topics
            .iter()
            .any(|constraint| !matches!(constraint, TopicConstraint::Any))
}

fn check_query_canceled(cancel_check: Option<&QueryCancelCheck>) -> DataFusionResult<()> {
    if cancel_check.is_some_and(|is_canceled| is_canceled()) {
        return Err(DataFusionError::Execution("query canceled".to_owned()));
    }
    Ok(())
}

fn exact_candidate_row_ids(
    dir: &std::path::Path,
    filter: &NativeLogFilter,
    row_ids: Vec<u32>,
    cancel_check: Option<&QueryCancelCheck>,
) -> std::io::Result<Vec<u32>> {
    if cancel_check.is_some_and(|is_canceled| is_canceled()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "query canceled",
        ));
    }
    let reader = SegmentReader::open(dir)?;
    let rows = reader.read_log_rows(Some(&row_ids))?;
    if cancel_check.is_some_and(|is_canceled| is_canceled()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "query canceled",
        ));
    }
    Ok(row_ids
        .into_iter()
        .zip(rows)
        .filter_map(|(row_id, row)| matches_native_filter(&row, filter).then_some(row_id))
        .collect())
}

#[derive(Debug)]
struct EmptyLogBatchGenerator {
    schema: SchemaRef,
    yielded: bool,
}

impl EmptyLogBatchGenerator {
    fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            yielded: false,
        }
    }
}

impl std::fmt::Display for EmptyLogBatchGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "empty log batch generator")
    }
}

impl LazyBatchGenerator for EmptyLogBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        if self.yielded {
            return Ok(None);
        }
        self.yielded = true;
        Ok(Some(empty_projected_batch(self.schema.clone())?))
    }
}

struct LogSegmentBatchGenerator {
    dir: std::path::PathBuf,
    schema: SchemaRef,
    projected_columns: Vec<String>,
    row_ids: Vec<u32>,
    offset: usize,
    cancel_check: Option<QueryCancelCheck>,
}

impl std::fmt::Debug for LogSegmentBatchGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogSegmentBatchGenerator")
            .field("dir", &self.dir)
            .field("projected_columns", &self.projected_columns)
            .field("row_ids", &self.row_ids.len())
            .field("offset", &self.offset)
            .finish_non_exhaustive()
    }
}

impl LogSegmentBatchGenerator {
    fn new(
        dir: std::path::PathBuf,
        schema: SchemaRef,
        projected_columns: Vec<String>,
        row_ids: Vec<u32>,
        cancel_check: Option<QueryCancelCheck>,
    ) -> Self {
        Self {
            dir,
            schema,
            projected_columns,
            row_ids,
            offset: 0,
            cancel_check,
        }
    }
}

impl std::fmt::Display for LogSegmentBatchGenerator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "log segment batch generator")
    }
}

impl LazyBatchGenerator for LogSegmentBatchGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> DataFusionResult<Option<RecordBatch>> {
        check_query_canceled(self.cancel_check.as_ref())?;
        if self.offset >= self.row_ids.len() {
            return Ok(None);
        }

        let end = (self.offset + DATAFUSION_BATCH_SIZE).min(self.row_ids.len());
        let row_ids = &self.row_ids[self.offset..end];
        self.offset = end;

        let batch = build_projected_batch(
            self.schema.clone(),
            &self.dir,
            row_ids,
            &self.projected_columns,
        )
        .map_err(DataFusionError::from)?;
        Ok(Some(batch))
    }
}

pub async fn execute_sql(
    sql: &str,
    storage: &PartitionManager,
    head_block: Option<u64>,
) -> Result<SqlQueryResult, SqlQueryError> {
    execute_sql_page(sql, storage, head_block, SqlQueryPage::default()).await
}

pub async fn execute_sql_page(
    sql: &str,
    storage: &PartitionManager,
    head_block: Option<u64>,
    page: SqlQueryPage,
) -> Result<SqlQueryResult, SqlQueryError> {
    execute_sql_page_with_cancel(sql, storage, head_block, page, None).await
}

pub async fn execute_sql_page_with_cancel(
    sql: &str,
    storage: &PartitionManager,
    head_block: Option<u64>,
    page: SqlQueryPage,
    cancel_check: Option<QueryCancelCheck>,
) -> Result<SqlQueryResult, SqlQueryError> {
    let snapshot = StorageSnapshot::from_storage(storage);
    let head_block = head_block.unwrap_or_else(|| storage.head_block().unwrap_or(0));
    execute_sql_page_on_snapshot(sql, snapshot, head_block, page, cancel_check).await
}

pub async fn execute_sql_page_on_snapshot(
    sql: &str,
    snapshot: StorageSnapshot,
    head_block: u64,
    page: SqlQueryPage,
    cancel_check: Option<QueryCancelCheck>,
) -> Result<SqlQueryResult, SqlQueryError> {
    let SqlQueryPage { limit, offset } = page;
    if let Some(message) = unsupported_from_alias_sort_shorthand(sql) {
        return Err(SqlQueryError::DataFusion(DataFusionError::Plan(message)));
    }
    let sql = rewrite_legacy_sql(sql, head_block)?;
    enforce_read_only_sql(&sql)?;
    if let Some(result) = try_execute_native_data_sum(&sql, &snapshot, page, cancel_check.clone())?
    {
        return Ok(result);
    }
    if let Some(result) = try_execute_native_select(&sql, &snapshot, page, cancel_check.clone())? {
        return Ok(result);
    }
    let total_scanned = Arc::new(AtomicU64::new(0));
    let table = LogexTableProvider::new(snapshot, Arc::clone(&total_scanned), cancel_check.clone());

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(table))?;

    let dataframe = ctx.sql(&sql).await?;
    let dataframe = if offset > 0 || limit.is_some() {
        dataframe.limit(offset, limit)?
    } else {
        dataframe
    };
    check_query_canceled(cancel_check.as_ref()).map_err(SqlQueryError::DataFusion)?;
    let batches = dataframe.collect().await?;
    check_query_canceled(cancel_check.as_ref()).map_err(SqlQueryError::DataFusion)?;
    let rows = record_batches_to_json(&batches);

    Ok(SqlQueryResult {
        rows,
        total_scanned: total_scanned.load(Ordering::Relaxed),
    })
}

fn try_execute_native_select(
    sql: &str,
    snapshot: &StorageSnapshot,
    page: SqlQueryPage,
    cancel_check: Option<QueryCancelCheck>,
) -> Result<Option<SqlQueryResult>, SqlQueryError> {
    let Some(mut native_query) = parse_native_select_query(sql)? else {
        return Ok(None);
    };

    let page_limit = page.limit.map(|limit| limit.saturating_add(page.offset));
    native_query.filter.limit = match (native_query.sql_limit, page_limit) {
        (Some(sql_limit), Some(page_limit)) => Some(sql_limit.min(page_limit)),
        (Some(sql_limit), None) => Some(sql_limit),
        (None, Some(page_limit)) => Some(page_limit),
        (None, None) => None,
    };
    native_query.filter.offset = page.offset;

    let (rows, total_scanned) =
        execute_native_sql_filter(snapshot, &native_query.filter, cancel_check.as_ref())?;
    let rows = rows
        .iter()
        .map(|row| native_log_row_to_json(row, &native_query.columns))
        .collect();

    Ok(Some(SqlQueryResult {
        rows,
        total_scanned,
    }))
}

fn execute_native_sql_filter(
    snapshot: &StorageSnapshot,
    filter: &NativeLogFilter,
    cancel_check: Option<&QueryCancelCheck>,
) -> Result<(Vec<logex_types::LogRow>, u64), SqlQueryError> {
    check_query_canceled(cancel_check).map_err(SqlQueryError::DataFusion)?;
    let partitions: Vec<_> = snapshot
        .partitions_in_order(filter.order)
        .into_iter()
        .filter(|partition| partition_matches_filter(partition, filter))
        .collect();
    let worker_count = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .clamp(1, 8);
    let window_size = worker_count.saturating_mul(4).max(1);
    let scan_limit = filter
        .limit
        .map(|limit| limit.saturating_add(filter.offset));
    let mut rows = Vec::new();
    let mut total_scanned = 0u64;

    'outer: for chunk in partitions.chunks(window_size) {
        check_query_canceled(cancel_check).map_err(SqlQueryError::DataFusion)?;
        let chunk_results = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(chunk.len());
            for partition in chunk {
                let path = partition.path.clone();
                let filter = filter.clone();
                let cancel_check = cancel_check.cloned();
                let limit = scan_limit;
                handles.push(scope.spawn(move || {
                    scan_native_sql_partition(&path, &filter, limit, cancel_check.as_ref())
                }));
            }
            handles
                .into_iter()
                .map(|handle| {
                    handle
                        .join()
                        .unwrap_or_else(|_| Err(std::io::Error::other("query worker panicked")))
                })
                .collect::<Vec<_>>()
        });

        for result in chunk_results {
            let mut partition_rows = result.map_err(|err| {
                if err.kind() == std::io::ErrorKind::Interrupted {
                    SqlQueryError::DataFusion(DataFusionError::Execution(
                        "query canceled".to_owned(),
                    ))
                } else {
                    SqlQueryError::Storage(err)
                }
            })?;
            total_scanned += partition_rows.len() as u64;
            rows.append(&mut partition_rows);
            if let Some(limit) = scan_limit
                && rows.len() >= limit
            {
                rows.truncate(limit);
                break 'outer;
            }
        }
    }

    sort_native_rows(&mut rows, filter.order);
    if filter.offset > 0 {
        if filter.offset >= rows.len() {
            rows.clear();
        } else {
            rows.drain(..filter.offset);
        }
    }
    if let Some(limit) = filter.limit {
        rows.truncate(limit);
    }

    Ok((rows, total_scanned))
}

fn scan_native_sql_partition(
    path: &std::path::Path,
    filter: &NativeLogFilter,
    limit: Option<usize>,
    cancel_check: Option<&QueryCancelCheck>,
) -> std::io::Result<Vec<logex_types::LogRow>> {
    if cancel_check.is_some_and(|is_canceled| is_canceled()) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "query canceled",
        ));
    }
    let mut row_ids = candidate_row_ids(path, filter, true)?;
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }
    order_and_truncate_row_ids(path, &mut row_ids, filter.order, limit)?;
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }
    let reader = SegmentReader::open(path)?;
    let mut rows = reader.read_log_rows(Some(&row_ids))?;
    rows.retain(|row| matches_native_filter(row, filter));
    sort_native_rows(&mut rows, filter.order);
    Ok(rows)
}

fn order_and_truncate_row_ids(
    path: &std::path::Path,
    row_ids: &mut Vec<u32>,
    order: logex_storage::native::LogOrder,
    limit: Option<usize>,
) -> std::io::Result<()> {
    if row_ids.is_empty() {
        return Ok(());
    }
    let reader = SegmentReader::open(path)?;
    let blocks = reader.read_u64("block_number", Some(row_ids))?;
    let tx_indices = reader.read_u32("tx_index", Some(row_ids))?;
    let log_indices = reader.read_u32("log_index", Some(row_ids))?;
    let mut keyed: Vec<_> = row_ids
        .iter()
        .copied()
        .zip(blocks.into_iter().zip(tx_indices).zip(log_indices))
        .map(|(row_id, ((block, tx_index), log_index))| (row_id, block, tx_index, log_index))
        .collect();
    if matches!(order, logex_storage::native::LogOrder::Descending) {
        keyed.sort_by_key(|(_, block, tx_index, log_index)| {
            std::cmp::Reverse((*block, *tx_index, *log_index))
        });
    } else {
        keyed.sort_by_key(|(_, block, tx_index, log_index)| (*block, *tx_index, *log_index));
    }
    if let Some(limit) = limit {
        keyed.truncate(limit);
    }
    *row_ids = keyed.into_iter().map(|(row_id, _, _, _)| row_id).collect();
    Ok(())
}

fn sort_native_rows(rows: &mut [logex_types::LogRow], order: logex_storage::native::LogOrder) {
    if matches!(order, logex_storage::native::LogOrder::Descending) {
        rows.sort_by_key(|row| std::cmp::Reverse((row.block_number, row.tx_index, row.log_index)));
    } else {
        rows.sort_by_key(|row| (row.block_number, row.tx_index, row.log_index));
    }
}

struct NativeSqlQuery {
    columns: Vec<String>,
    filter: NativeLogFilter,
    sql_limit: Option<usize>,
}

struct NativeDataSumQuery {
    projections: Vec<NativeAggregateProjection>,
    sums: Vec<NativeRowValueExpr>,
    filter: NativeLogFilter,
    candidate_filters: Vec<NativeLogFilter>,
    selection: Option<SqlAstExpr>,
    sql_limit: Option<usize>,
}

struct NativeAggregateProjection {
    output_column: String,
    expr: NativeAggregateExpr,
}

#[derive(Clone)]
enum NativeAggregateExpr {
    Sum(usize),
    Add(Box<NativeAggregateExpr>, Box<NativeAggregateExpr>),
    Sub(Box<NativeAggregateExpr>, Box<NativeAggregateExpr>),
}

#[derive(Clone)]
enum NativeRowValueExpr {
    Data,
    Literal(BigInt),
    Case {
        branches: Vec<(SqlAstExpr, NativeRowValueExpr)>,
        else_expr: Option<Box<NativeRowValueExpr>>,
    },
}

struct NativeSumState {
    expr: NativeRowValueExpr,
    sum: BigInt,
    count: u64,
}

fn parse_native_select_query(sql: &str) -> Result<Option<NativeSqlQuery>, SqlQueryError> {
    let mut statements = DFParser::parse_sql(sql).map_err(SqlQueryError::DataFusion)?;
    if statements.len() != 1 {
        return Ok(None);
    }
    let Some(DFStatement::Statement(statement)) = statements.pop_front() else {
        return Ok(None);
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return Ok(None);
    };
    if query.with.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Ok(None);
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty())
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
    {
        return Ok(None);
    }
    if !select_has_logs_from(&select.from) {
        return Ok(None);
    }

    let Some(columns) = native_projection_columns(&select.projection) else {
        return Ok(None);
    };
    let Some(order) = native_order(query.order_by.as_ref()) else {
        return Ok(None);
    };
    let Some(sql_limit) = native_limit(query.limit_clause.as_ref()) else {
        return Ok(None);
    };

    let mut filter = NativeLogFilter::new();
    filter.order = order;
    filter.limit = sql_limit;
    if let Some(selection) = &select.selection
        && !apply_sql_ast_filter(&mut filter, selection)?
    {
        return Ok(None);
    }

    Ok(Some(NativeSqlQuery {
        columns,
        filter,
        sql_limit,
    }))
}

fn try_execute_native_data_sum(
    sql: &str,
    snapshot: &StorageSnapshot,
    page: SqlQueryPage,
    cancel_check: Option<QueryCancelCheck>,
) -> Result<Option<SqlQueryResult>, SqlQueryError> {
    let Some(native_query) = parse_native_data_sum_query(sql)? else {
        return Ok(None);
    };

    if native_query.sql_limit == Some(0) || page.limit == Some(0) || page.offset > 0 {
        return Ok(Some(SqlQueryResult {
            rows: Vec::new(),
            total_scanned: 0,
        }));
    }

    let (values, total_scanned) = execute_native_data_sum(
        snapshot,
        &native_query.filter,
        &native_query.candidate_filters,
        native_query.selection.as_ref(),
        &native_query.projections,
        &native_query.sums,
        cancel_check.as_ref(),
    )?;
    let mut row = Map::with_capacity(values.len());
    for (projection, value) in native_query.projections.iter().zip(values) {
        row.insert(
            projection.output_column.clone(),
            value
                .map(|value| Value::String(value.to_string()))
                .unwrap_or(Value::Null),
        );
    }

    Ok(Some(SqlQueryResult {
        rows: vec![Value::Object(row)],
        total_scanned,
    }))
}

fn parse_native_data_sum_query(sql: &str) -> Result<Option<NativeDataSumQuery>, SqlQueryError> {
    let mut statements = DFParser::parse_sql(sql).map_err(SqlQueryError::DataFusion)?;
    if statements.len() != 1 {
        return Ok(None);
    }
    let Some(DFStatement::Statement(statement)) = statements.pop_front() else {
        return Ok(None);
    };
    let SqlStatement::Query(query) = statement.as_ref() else {
        return Ok(None);
    };
    if query.with.is_some()
        || query.order_by.is_some()
        || query.fetch.is_some()
        || !query.locks.is_empty()
        || query.for_clause.is_some()
        || query.settings.is_some()
        || query.format_clause.is_some()
        || !query.pipe_operators.is_empty()
    {
        return Ok(None);
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return Ok(None);
    };
    if select.distinct.is_some()
        || select.top.is_some()
        || select.into.is_some()
        || !select.lateral_views.is_empty()
        || select.prewhere.is_some()
        || !matches!(&select.group_by, GroupByExpr::Expressions(exprs, modifiers) if exprs.is_empty() && modifiers.is_empty())
        || !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
        || select.having.is_some()
        || !select.named_window.is_empty()
        || select.qualify.is_some()
        || select.value_table_mode.is_some()
        || select.connect_by.is_some()
        || !select_has_logs_from(&select.from)
    {
        return Ok(None);
    }

    let Some((projections, sums)) = native_aggregate_projections(&select.projection) else {
        return Ok(None);
    };
    let Some(sql_limit) = native_limit(query.limit_clause.as_ref()) else {
        return Ok(None);
    };

    let mut filter = NativeLogFilter::new();
    let mut selection = None;
    if let Some(selection_expr) = &select.selection {
        let mut exact_filter = NativeLogFilter::new();
        if apply_sql_ast_filter(&mut exact_filter, selection_expr)? {
            filter = exact_filter;
        } else {
            apply_sql_ast_filter_conjuncts(&mut filter, selection_expr)?;
            selection = Some(selection_expr.clone());
        }
    }
    let candidate_filters = selection
        .as_ref()
        .and_then(|selection| topic_or_candidate_filters(&filter, selection))
        .unwrap_or_else(|| vec![filter.clone()]);

    Ok(Some(NativeDataSumQuery {
        projections,
        sums,
        filter,
        candidate_filters,
        selection,
        sql_limit,
    }))
}

fn apply_sql_ast_filter_conjuncts(
    filter: &mut NativeLogFilter,
    expr: &SqlAstExpr,
) -> Result<(), SqlQueryError> {
    match expr {
        SqlAstExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::And => {
            apply_sql_ast_filter_conjuncts(filter, left)?;
            apply_sql_ast_filter_conjuncts(filter, right)?;
        }
        other => {
            let _ = apply_sql_ast_filter(filter, other)?;
        }
    }
    Ok(())
}

fn native_aggregate_projections(
    projection: &[SelectItem],
) -> Option<(Vec<NativeAggregateProjection>, Vec<NativeRowValueExpr>)> {
    let mut projections = Vec::with_capacity(projection.len());
    let mut sums = Vec::new();
    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                projections.push(NativeAggregateProjection {
                    output_column: native_aggregate_output_name(expr),
                    expr: parse_native_aggregate_expr(expr, &mut sums)?,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                projections.push(NativeAggregateProjection {
                    output_column: alias.value.clone(),
                    expr: parse_native_aggregate_expr(expr, &mut sums)?,
                });
            }
            _ => return None,
        }
    }
    (!projections.is_empty()).then_some((projections, sums))
}

fn native_aggregate_output_name(expr: &SqlAstExpr) -> String {
    if is_plain_sum_data_expr(expr) {
        "sum".to_owned()
    } else {
        expr.to_string()
    }
}

fn parse_native_aggregate_expr(
    expr: &SqlAstExpr,
    sums: &mut Vec<NativeRowValueExpr>,
) -> Option<NativeAggregateExpr> {
    match expr {
        SqlAstExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::Plus => {
            Some(NativeAggregateExpr::Add(
                Box::new(parse_native_aggregate_expr(left, sums)?),
                Box::new(parse_native_aggregate_expr(right, sums)?),
            ))
        }
        SqlAstExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::Minus => {
            Some(NativeAggregateExpr::Sub(
                Box::new(parse_native_aggregate_expr(left, sums)?),
                Box::new(parse_native_aggregate_expr(right, sums)?),
            ))
        }
        SqlAstExpr::Nested(expr) => parse_native_aggregate_expr(expr, sums),
        _ => parse_native_sum_expr(expr, sums),
    }
}

fn parse_native_sum_expr(
    expr: &SqlAstExpr,
    sums: &mut Vec<NativeRowValueExpr>,
) -> Option<NativeAggregateExpr> {
    let SqlAstExpr::Function(function) = expr else {
        return None;
    };
    if !function.name.to_string().eq_ignore_ascii_case("sum")
        || !matches!(&function.parameters, FunctionArguments::None)
        || function.filter.is_some()
        || function.null_treatment.is_some()
        || function.over.is_some()
        || !function.within_group.is_empty()
    {
        return None;
    }
    let FunctionArguments::List(args) = &function.args else {
        return None;
    };
    if matches!(args.duplicate_treatment, Some(DuplicateTreatment::Distinct))
        || !args.clauses.is_empty()
        || args.args.len() != 1
    {
        return None;
    }
    let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = &args.args[0] else {
        return None;
    };
    let index = sums.len();
    sums.push(parse_native_row_value_expr(expr)?);
    Some(NativeAggregateExpr::Sum(index))
}

fn is_plain_sum_data_expr(expr: &SqlAstExpr) -> bool {
    let mut sums = Vec::new();
    matches!(
        parse_native_sum_expr(expr, &mut sums),
        Some(NativeAggregateExpr::Sum(0))
    ) && matches!(sums.first(), Some(NativeRowValueExpr::Data))
}

fn parse_native_row_value_expr(expr: &SqlAstExpr) -> Option<NativeRowValueExpr> {
    match expr {
        SqlAstExpr::Identifier(_) | SqlAstExpr::CompoundIdentifier(_) => sql_identifier(expr)
            .is_some_and(|column| column == "data")
            .then_some(NativeRowValueExpr::Data),
        SqlAstExpr::Nested(expr) => parse_native_row_value_expr(expr),
        SqlAstExpr::Cast {
            expr, data_type, ..
        } => {
            if is_integer_cast_target(data_type) {
                parse_native_row_value_expr(expr)
            } else {
                None
            }
        }
        SqlAstExpr::Value(value) => sql_bigint_value(value).map(NativeRowValueExpr::Literal),
        SqlAstExpr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            if operand.is_some() {
                return None;
            }
            let mut branches = Vec::with_capacity(conditions.len());
            for condition in conditions {
                branches.push((
                    condition.condition.clone(),
                    parse_native_row_value_expr(&condition.result)?,
                ));
            }
            Some(NativeRowValueExpr::Case {
                branches,
                else_expr: match else_result.as_deref() {
                    Some(expr) => Some(Box::new(parse_native_row_value_expr(expr)?)),
                    None => None,
                },
            })
        }
        _ => None,
    }
}

fn is_integer_cast_target(data_type: &datafusion::sql::sqlparser::ast::DataType) -> bool {
    let name = data_type.to_string().to_ascii_lowercase();
    name.contains("int") || name.contains("numeric") || name.contains("decimal")
}

fn topic_or_candidate_filters(
    base_filter: &NativeLogFilter,
    selection: &SqlAstExpr,
) -> Option<Vec<NativeLogFilter>> {
    let terms = find_topic_or_terms(selection)?;
    if terms.len() < 2 {
        return None;
    }
    let mut filters = Vec::with_capacity(terms.len());
    for (index, topic) in terms {
        let mut filter = base_filter.clone();
        merge_topic_constraint(&mut filter.topics[index], TopicConstraint::One(topic));
        filters.push(filter);
    }
    Some(filters)
}

fn find_topic_or_terms(expr: &SqlAstExpr) -> Option<Vec<(usize, B256)>> {
    match expr {
        SqlAstExpr::Nested(expr) => find_topic_or_terms(expr),
        SqlAstExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::And => {
            find_topic_or_terms(left).or_else(|| find_topic_or_terms(right))
        }
        SqlAstExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::Or => {
            let mut terms = collect_topic_or_terms(left)?;
            terms.extend(collect_topic_or_terms(right)?);
            Some(terms)
        }
        _ => None,
    }
}

fn collect_topic_or_terms(expr: &SqlAstExpr) -> Option<Vec<(usize, B256)>> {
    match expr {
        SqlAstExpr::Nested(expr) => collect_topic_or_terms(expr),
        SqlAstExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::Or => {
            let mut terms = collect_topic_or_terms(left)?;
            terms.extend(collect_topic_or_terms(right)?);
            Some(terms)
        }
        _ => parse_topic_eq_term(expr).map(|term| vec![term]),
    }
}

fn parse_topic_eq_term(expr: &SqlAstExpr) -> Option<(usize, B256)> {
    let SqlAstExpr::BinaryOp { left, op, right } = expr else {
        return None;
    };
    if *op != SqlBinaryOperator::Eq {
        return None;
    }
    let (column, literal, reversed) = normalize_sql_binary(left, right)?;
    if reversed {
        return None;
    }
    let index = topic_column_index(&column)?;
    let topic = sql_string(literal).and_then(parse_b256)?;
    Some((index, topic))
}

fn execute_native_data_sum(
    snapshot: &StorageSnapshot,
    filter: &NativeLogFilter,
    candidate_filters: &[NativeLogFilter],
    selection: Option<&SqlAstExpr>,
    projections: &[NativeAggregateProjection],
    sum_inputs: &[NativeRowValueExpr],
    cancel_check: Option<&QueryCancelCheck>,
) -> Result<(Vec<Option<BigInt>>, u64), SqlQueryError> {
    check_query_canceled(cancel_check).map_err(SqlQueryError::DataFusion)?;
    let mut states = sum_inputs
        .iter()
        .cloned()
        .map(|expr| NativeSumState {
            expr,
            sum: BigInt::default(),
            count: 0,
        })
        .collect::<Vec<_>>();
    let data_only = selection.is_none()
        && sum_inputs
            .iter()
            .all(|expr| matches!(expr, NativeRowValueExpr::Data));
    let mut total_scanned = 0u64;

    for partition in snapshot.partitions_in_order(filter.order) {
        check_query_canceled(cancel_check).map_err(SqlQueryError::DataFusion)?;
        if !partition_matches_filter(&partition, filter) {
            continue;
        }
        let mut row_bitmap = RoaringBitmap::new();
        for candidate_filter in candidate_filters {
            if !partition_matches_filter(&partition, candidate_filter) {
                continue;
            }
            let row_ids = candidate_row_ids(&partition.path, candidate_filter, true)
                .map_err(map_native_query_io_error)?;
            row_bitmap.extend(row_ids);
        }
        let row_ids = row_bitmap.iter().collect::<Vec<_>>();
        if row_ids.is_empty() {
            continue;
        }

        let reader = SegmentReader::open(&partition.path).map_err(map_native_query_io_error)?;
        if data_only {
            let values = reader
                .read_var_bytes("data", Some(&row_ids))
                .map_err(map_native_query_io_error)?;
            for value in values {
                let value = BigInt::from(BigUint::from_bytes_be(value.as_ref()));
                for state in &mut states {
                    state.sum += value.clone();
                    state.count += 1;
                }
            }
            total_scanned += row_ids.len() as u64;
            continue;
        }

        let rows = reader
            .read_log_rows(Some(&row_ids))
            .map_err(map_native_query_io_error)?;
        for row in rows {
            if let Some(selection) = selection
                && !eval_sql_predicate(&row, selection)
            {
                continue;
            }
            for state in &mut states {
                if let Some(value) = eval_native_row_value(&row, &state.expr) {
                    state.sum += value;
                    state.count += 1;
                }
            }
            total_scanned += 1;
        }
    }

    Ok((
        projections
            .iter()
            .map(|projection| eval_native_aggregate_expr(&projection.expr, &states))
            .collect(),
        total_scanned,
    ))
}

fn eval_native_aggregate_expr(
    expr: &NativeAggregateExpr,
    states: &[NativeSumState],
) -> Option<BigInt> {
    match expr {
        NativeAggregateExpr::Sum(index) => {
            let state = states.get(*index)?;
            (state.count > 0).then(|| state.sum.clone())
        }
        NativeAggregateExpr::Add(left, right) => Some(
            eval_native_aggregate_expr(left, states)? + eval_native_aggregate_expr(right, states)?,
        ),
        NativeAggregateExpr::Sub(left, right) => Some(
            eval_native_aggregate_expr(left, states)? - eval_native_aggregate_expr(right, states)?,
        ),
    }
}

fn eval_native_row_value(row: &logex_types::LogRow, expr: &NativeRowValueExpr) -> Option<BigInt> {
    match expr {
        NativeRowValueExpr::Data => Some(BigInt::from(BigUint::from_bytes_be(row.data.as_ref()))),
        NativeRowValueExpr::Literal(value) => Some(value.clone()),
        NativeRowValueExpr::Case {
            branches,
            else_expr,
        } => {
            for (condition, result) in branches {
                if eval_sql_predicate(row, condition) {
                    return eval_native_row_value(row, result);
                }
            }
            else_expr
                .as_deref()
                .and_then(|expr| eval_native_row_value(row, expr))
        }
    }
}

fn eval_sql_predicate(row: &logex_types::LogRow, expr: &SqlAstExpr) -> bool {
    match expr {
        SqlAstExpr::Nested(expr) => eval_sql_predicate(row, expr),
        SqlAstExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::And => {
            eval_sql_predicate(row, left) && eval_sql_predicate(row, right)
        }
        SqlAstExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::Or => {
            eval_sql_predicate(row, left) || eval_sql_predicate(row, right)
        }
        SqlAstExpr::BinaryOp { left, op, right } => {
            eval_sql_comparison(row, left, op.clone(), right)
        }
        SqlAstExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let Some(column) = sql_identifier(expr) else {
                return false;
            };
            let Some(value) = row_numeric_value(row, &column) else {
                return false;
            };
            let Some(low) = sql_u64(low) else {
                return false;
            };
            let Some(high) = sql_u64(high) else {
                return false;
            };
            let matches = value >= low && value <= high;
            if *negated { !matches } else { matches }
        }
        SqlAstExpr::InList {
            expr,
            list,
            negated,
        } => {
            let Some(column) = sql_identifier(expr) else {
                return false;
            };
            let matches = list
                .iter()
                .any(|literal| eval_column_literal_eq(row, &column, literal));
            if *negated { !matches } else { matches }
        }
        _ => false,
    }
}

fn eval_sql_comparison(
    row: &logex_types::LogRow,
    left: &SqlAstExpr,
    operator: SqlBinaryOperator,
    right: &SqlAstExpr,
) -> bool {
    if let Some(column) = sql_identifier(left) {
        return eval_column_literal_comparison(row, &column, operator, right, false);
    }
    if let Some(column) = sql_identifier(right) {
        return eval_column_literal_comparison(row, &column, operator, left, true);
    }
    false
}

fn eval_column_literal_comparison(
    row: &logex_types::LogRow,
    column: &str,
    operator: SqlBinaryOperator,
    literal: &SqlAstExpr,
    reversed: bool,
) -> bool {
    match column {
        "block_number" | "timestamp" | "data_len" | "source" => {
            let Some(row_value) = row_numeric_value(row, column) else {
                return false;
            };
            let Some(literal_value) = sql_u64(literal) else {
                return false;
            };
            compare_u64(row_value, operator, literal_value, reversed)
        }
        "block_hash" => {
            let Some(value) = sql_string(literal).and_then(parse_b256) else {
                return false;
            };
            compare_eq(row.block_hash == value, operator, reversed)
        }
        "address" => {
            let Some(value) = sql_string(literal).and_then(parse_address) else {
                return false;
            };
            compare_eq(row.address == value, operator, reversed)
        }
        column if topic_column_index(column).is_some() => {
            let Some(value) = sql_string(literal).and_then(parse_b256) else {
                return false;
            };
            let topic = row_topic(row, topic_column_index(column).unwrap());
            compare_eq(topic == Some(value), operator, reversed)
        }
        "data" => {
            let Some(value) = sql_string(literal).and_then(parse_hex_bytes) else {
                return false;
            };
            compare_bytes(row.data.as_ref(), operator, value.as_slice(), reversed)
        }
        _ => false,
    }
}

fn eval_column_literal_eq(row: &logex_types::LogRow, column: &str, literal: &SqlAstExpr) -> bool {
    eval_column_literal_comparison(row, column, SqlBinaryOperator::Eq, literal, false)
}

fn row_numeric_value(row: &logex_types::LogRow, column: &str) -> Option<u64> {
    match column {
        "block_number" => Some(row.block_number),
        "timestamp" => Some(row.timestamp),
        "data_len" => Some(row.data_len as u64),
        "source" => Some(row.source as u8 as u64),
        _ => None,
    }
}

fn row_topic(row: &logex_types::LogRow, index: usize) -> Option<B256> {
    match index {
        0 => row.topic0,
        1 => row.topic1,
        2 => row.topic2,
        3 => row.topic3,
        _ => None,
    }
}

fn compare_eq(matches: bool, operator: SqlBinaryOperator, _reversed: bool) -> bool {
    match operator {
        SqlBinaryOperator::Eq => matches,
        SqlBinaryOperator::NotEq => !matches,
        _ => false,
    }
}

fn compare_u64(left: u64, operator: SqlBinaryOperator, right: u64, reversed: bool) -> bool {
    if reversed {
        return compare_u64(right, operator, left, false);
    }
    match operator {
        SqlBinaryOperator::Eq => left == right,
        SqlBinaryOperator::NotEq => left != right,
        SqlBinaryOperator::Gt => left > right,
        SqlBinaryOperator::GtEq => left >= right,
        SqlBinaryOperator::Lt => left < right,
        SqlBinaryOperator::LtEq => left <= right,
        _ => false,
    }
}

fn compare_bytes(left: &[u8], operator: SqlBinaryOperator, right: &[u8], reversed: bool) -> bool {
    if reversed {
        return compare_bytes(right, operator, left, false);
    }
    match operator {
        SqlBinaryOperator::Eq => left == right,
        SqlBinaryOperator::NotEq => left != right,
        SqlBinaryOperator::Gt => left > right,
        SqlBinaryOperator::GtEq => left >= right,
        SqlBinaryOperator::Lt => left < right,
        SqlBinaryOperator::LtEq => left <= right,
        _ => false,
    }
}

fn sql_bigint_value(value: &datafusion::sql::sqlparser::ast::ValueWithSpan) -> Option<BigInt> {
    match &value.value {
        SqlValue::Number(value, _) => value.parse().ok(),
        SqlValue::SingleQuotedString(value)
        | SqlValue::DoubleQuotedString(value)
        | SqlValue::TripleSingleQuotedString(value)
        | SqlValue::TripleDoubleQuotedString(value) => value.parse().ok(),
        _ => None,
    }
}

fn map_native_query_io_error(err: std::io::Error) -> SqlQueryError {
    if err.kind() == std::io::ErrorKind::Interrupted {
        SqlQueryError::DataFusion(DataFusionError::Execution("query canceled".to_owned()))
    } else {
        SqlQueryError::Storage(err)
    }
}

fn select_has_logs_from(from: &[datafusion::sql::sqlparser::ast::TableWithJoins]) -> bool {
    if from.len() != 1 || !from[0].joins.is_empty() {
        return false;
    }
    match &from[0].relation {
        TableFactor::Table {
            name,
            alias,
            args,
            with_hints,
            version,
            with_ordinality,
            partitions,
            json_path,
            sample,
            index_hints,
            ..
        } => {
            name.to_string().eq_ignore_ascii_case("logs")
                && alias.is_none()
                && args.is_none()
                && with_hints.is_empty()
                && version.is_none()
                && !*with_ordinality
                && partitions.is_empty()
                && json_path.is_none()
                && sample.is_none()
                && index_hints.is_empty()
        }
        _ => false,
    }
}

fn native_projection_columns(projection: &[SelectItem]) -> Option<Vec<String>> {
    let mut columns = Vec::new();
    for item in projection {
        match item {
            SelectItem::Wildcard(_) => return Some(all_log_columns()),
            SelectItem::UnnamedExpr(expr) => columns.push(sql_identifier(expr)?),
            SelectItem::ExprWithAlias { expr, .. } => columns.push(sql_identifier(expr)?),
            SelectItem::QualifiedWildcard(_, _) => return None,
        }
    }
    Some(columns)
}

fn all_log_columns() -> Vec<String> {
    [
        "block_number",
        "block_hash",
        "timestamp",
        "tx_hash",
        "tx_index",
        "log_index",
        "address",
        "topic0",
        "topic1",
        "topic2",
        "topic3",
        "topics",
        "data",
        "data_len",
        "source",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn native_order(
    order_by: Option<&datafusion::sql::sqlparser::ast::OrderBy>,
) -> Option<logex_storage::native::LogOrder> {
    let Some(order_by) = order_by else {
        return Some(logex_storage::native::LogOrder::Ascending);
    };
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return None;
    };
    let expected = ["block_number", "tx_index", "log_index"];
    if expressions.len() != expected.len() {
        return None;
    }
    let mut descending = None;
    for (expr, expected) in expressions.iter().zip(expected) {
        if sql_identifier(&expr.expr)?.eq_ignore_ascii_case(expected) {
            let is_desc = !expr.options.asc.unwrap_or(true);
            if let Some(existing) = descending {
                if existing != is_desc {
                    return None;
                }
            } else {
                descending = Some(is_desc);
            }
            if expr.options.nulls_first.is_some() || expr.with_fill.is_some() {
                return None;
            }
        } else {
            return None;
        }
    }
    if descending.unwrap_or(false) {
        Some(logex_storage::native::LogOrder::Descending)
    } else {
        Some(logex_storage::native::LogOrder::Ascending)
    }
}

fn native_limit(limit_clause: Option<&LimitClause>) -> Option<Option<usize>> {
    let Some(limit_clause) = limit_clause else {
        return Some(None);
    };
    match limit_clause {
        LimitClause::LimitOffset {
            limit: Some(limit),
            offset: None,
            limit_by,
        } if limit_by.is_empty() => sql_usize(limit).map(Some),
        LimitClause::LimitOffset {
            limit: None,
            offset: None,
            limit_by,
        } if limit_by.is_empty() => Some(None),
        _ => None,
    }
}

fn apply_sql_ast_filter(
    filter: &mut NativeLogFilter,
    expr: &SqlAstExpr,
) -> Result<bool, SqlQueryError> {
    match expr {
        SqlAstExpr::BinaryOp { left, op, right } if *op == SqlBinaryOperator::And => {
            Ok(apply_sql_ast_filter(filter, left)? && apply_sql_ast_filter(filter, right)?)
        }
        SqlAstExpr::BinaryOp { left, op, right } => {
            apply_sql_binary_filter(filter, left, op.clone(), right)
        }
        SqlAstExpr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            if *negated {
                return Ok(false);
            }
            let Some(column) = sql_identifier(expr) else {
                return Ok(false);
            };
            let (Some(low), Some(high)) = (sql_u64(low), sql_u64(high)) else {
                return Ok(false);
            };
            match column.as_str() {
                "block_number" => {
                    filter.from_block =
                        Some(filter.from_block.map_or(low, |current| current.max(low)));
                    filter.to_block =
                        Some(filter.to_block.map_or(high, |current| current.min(high)));
                    Ok(true)
                }
                "timestamp" => {
                    filter.from_timestamp = Some(
                        filter
                            .from_timestamp
                            .map_or(low, |current| current.max(low)),
                    );
                    filter.to_timestamp = Some(
                        filter
                            .to_timestamp
                            .map_or(high, |current| current.min(high)),
                    );
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        SqlAstExpr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                return Ok(false);
            }
            let Some(column) = sql_identifier(expr) else {
                return Ok(false);
            };
            match column.as_str() {
                "address" => {
                    let mut addresses = Vec::with_capacity(list.len());
                    for item in list {
                        let Some(address) = sql_string(item).and_then(parse_address) else {
                            return Ok(false);
                        };
                        addresses.push(address);
                    }
                    merge_addresses(filter, addresses);
                    Ok(true)
                }
                column if topic_column_index(column).is_some() => {
                    let index = topic_column_index(column).unwrap();
                    let mut topics = Vec::with_capacity(list.len());
                    for item in list {
                        let Some(topic) = sql_string(item).and_then(parse_b256) else {
                            return Ok(false);
                        };
                        topics.push(topic);
                    }
                    merge_topic_constraint(
                        &mut filter.topics[index],
                        TopicConstraint::AnyOf(topics),
                    );
                    Ok(true)
                }
                _ => Ok(false),
            }
        }
        _ => Ok(false),
    }
}

fn apply_sql_binary_filter(
    filter: &mut NativeLogFilter,
    left: &SqlAstExpr,
    operator: SqlBinaryOperator,
    right: &SqlAstExpr,
) -> Result<bool, SqlQueryError> {
    let Some((column, literal, reversed)) = normalize_sql_binary(left, right) else {
        return Ok(false);
    };

    match column.as_str() {
        "block_number" => {
            let Some(value) = sql_u64(literal) else {
                return Ok(false);
            };
            apply_block_number_constraint(filter, sql_binary_operator(operator), value, reversed);
            Ok(true)
        }
        "timestamp" => {
            let Some(value) = sql_u64(literal) else {
                return Ok(false);
            };
            apply_timestamp_constraint(filter, sql_binary_operator(operator), value, reversed);
            Ok(true)
        }
        "data_len" if !reversed && operator == SqlBinaryOperator::Eq => {
            let Some(value) = sql_u64(literal) else {
                return Ok(false);
            };
            filter.data_len = Some(value.min(u32::MAX as u64) as u32);
            Ok(true)
        }
        "data" if !reversed => {
            let Some(value) = sql_string(literal).and_then(parse_hex_bytes) else {
                return Ok(false);
            };
            match operator {
                SqlBinaryOperator::Eq => apply_data_constraint(filter, Operator::Eq, value),
                SqlBinaryOperator::GtEq => apply_data_constraint(filter, Operator::GtEq, value),
                SqlBinaryOperator::LtEq => apply_data_constraint(filter, Operator::LtEq, value),
                _ => return Ok(false),
            }
            Ok(true)
        }
        "block_hash" if !reversed && operator == SqlBinaryOperator::Eq => {
            let Some(block_hash) = sql_string(literal).and_then(parse_b256) else {
                return Ok(false);
            };
            filter.block_hash = Some(block_hash);
            Ok(true)
        }
        "address" if !reversed && operator == SqlBinaryOperator::Eq => {
            let Some(address) = sql_string(literal).and_then(parse_address) else {
                return Ok(false);
            };
            merge_addresses(filter, vec![address]);
            Ok(true)
        }
        column
            if !reversed
                && operator == SqlBinaryOperator::Eq
                && topic_column_index(column).is_some() =>
        {
            let Some(topic) = sql_string(literal).and_then(parse_b256) else {
                return Ok(false);
            };
            let index = topic_column_index(column).unwrap();
            merge_topic_constraint(&mut filter.topics[index], TopicConstraint::One(topic));
            Ok(true)
        }
        _ => Ok(false),
    }
}

fn normalize_sql_binary<'a>(
    left: &'a SqlAstExpr,
    right: &'a SqlAstExpr,
) -> Option<(String, &'a SqlAstExpr, bool)> {
    if let Some(column) = sql_identifier(left) {
        return Some((column, right, false));
    }
    if let Some(column) = sql_identifier(right) {
        return Some((column, left, true));
    }
    None
}

fn sql_binary_operator(operator: SqlBinaryOperator) -> Operator {
    match operator {
        SqlBinaryOperator::Eq => Operator::Eq,
        SqlBinaryOperator::Gt => Operator::Gt,
        SqlBinaryOperator::GtEq => Operator::GtEq,
        SqlBinaryOperator::Lt => Operator::Lt,
        SqlBinaryOperator::LtEq => Operator::LtEq,
        _ => Operator::Eq,
    }
}

fn sql_identifier(expr: &SqlAstExpr) -> Option<String> {
    match expr {
        SqlAstExpr::Identifier(ident) => Some(ident.value.to_ascii_lowercase()),
        SqlAstExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
            (parts[0].value.eq_ignore_ascii_case("logs"))
                .then(|| parts[1].value.to_ascii_lowercase())
        }
        _ => None,
    }
}

fn sql_string(expr: &SqlAstExpr) -> Option<&str> {
    match expr {
        SqlAstExpr::Value(value) => match &value.value {
            SqlValue::SingleQuotedString(value)
            | SqlValue::DoubleQuotedString(value)
            | SqlValue::TripleSingleQuotedString(value)
            | SqlValue::TripleDoubleQuotedString(value) => Some(value.as_str()),
            _ => None,
        },
        _ => None,
    }
}

fn sql_u64(expr: &SqlAstExpr) -> Option<u64> {
    match expr {
        SqlAstExpr::Value(value) => match &value.value {
            SqlValue::Number(value, _) => value.parse().ok(),
            _ => None,
        },
        _ => None,
    }
}

fn sql_usize(expr: &SqlAstExpr) -> Option<usize> {
    sql_u64(expr).and_then(|value| usize::try_from(value).ok())
}

fn native_log_row_to_json(row: &logex_types::LogRow, columns: &[String]) -> Value {
    let mut out = Map::with_capacity(columns.len());
    for column in columns {
        let value = match column.as_str() {
            "block_number" => Value::Number(row.block_number.into()),
            "block_hash" => Value::String(to_hex_hash(row.block_hash)),
            "timestamp" => Value::Number(row.timestamp.into()),
            "tx_hash" => Value::String(to_hex_hash(row.tx_hash)),
            "tx_index" => Value::Number((row.tx_index as u64).into()),
            "log_index" => Value::Number((row.log_index as u64).into()),
            "address" => Value::String(to_hex_address(row.address.as_slice())),
            "topic0" => optional_topic_to_json(row.topic0),
            "topic1" => optional_topic_to_json(row.topic1),
            "topic2" => optional_topic_to_json(row.topic2),
            "topic3" => optional_topic_to_json(row.topic3),
            "topics" => Value::Array(
                [row.topic0, row.topic1, row.topic2, row.topic3]
                    .into_iter()
                    .flatten()
                    .map(|topic| Value::String(to_hex_hash(topic)))
                    .collect(),
            ),
            "data" => Value::String(to_hex_bytes(row.data.as_ref())),
            "data_len" => Value::Number((row.data_len as u64).into()),
            "source" => Value::Number((row.source as u8 as u64).into()),
            _ => Value::Null,
        };
        out.insert(column.clone(), value);
    }
    Value::Object(out)
}

fn optional_topic_to_json(topic: Option<B256>) -> Value {
    topic
        .map(|topic| Value::String(to_hex_hash(topic)))
        .unwrap_or(Value::Null)
}

fn enforce_read_only_sql(sql: &str) -> Result<(), SqlQueryError> {
    let mut statements = DFParser::parse_sql(sql).map_err(SqlQueryError::DataFusion)?;
    if statements.len() != 1 {
        return Err(SqlQueryError::DataFusion(DataFusionError::Plan(
            "only a single read-only SQL statement is allowed".to_owned(),
        )));
    }

    let statement = statements.pop_front().ok_or_else(|| {
        SqlQueryError::DataFusion(DataFusionError::Plan(
            "only a single read-only SQL statement is allowed".to_owned(),
        ))
    })?;

    if !matches!(statement, DFStatement::Statement(stmt) if matches!(stmt.as_ref(), SqlStatement::Query(_)))
    {
        return Err(SqlQueryError::DataFusion(DataFusionError::Plan(
            "LogEx SQL is read-only; only SELECT and WITH queries are allowed".to_owned(),
        )));
    }

    Ok(())
}

fn unsupported_from_alias_sort_shorthand(sql: &str) -> Option<String> {
    let tokens = tokenize(sql).ok()?;
    let from_pos = tokens.iter().position(|token| *token == Token::From)?;
    let table = tokens.get(from_pos + 1)?;
    let next = tokens.get(from_pos + 2)?;
    let has_order_by = tokens.contains(&Token::OrderBy);

    if !matches!(table, Token::Ident(name) if name.eq_ignore_ascii_case("logs")) || has_order_by {
        return None;
    }

    match next {
        Token::Desc | Token::Asc => Some(
            "unexpected ASC/DESC after FROM logs; use ORDER BY block_number DESC, tx_index DESC, log_index DESC".to_owned(),
        ),
        _ => None,
    }
}

fn log_rows_schema() -> Schema {
    Schema::new(vec![
        Field::new("block_number", DataType::UInt64, false),
        Field::new("block_hash", DataType::Utf8, false),
        Field::new("timestamp", DataType::UInt64, false),
        Field::new("tx_hash", DataType::Utf8, false),
        Field::new("tx_index", DataType::UInt64, false),
        Field::new("log_index", DataType::UInt64, false),
        Field::new("address", DataType::Utf8, false),
        Field::new("topic0", DataType::Utf8, true),
        Field::new("topic1", DataType::Utf8, true),
        Field::new("topic2", DataType::Utf8, true),
        Field::new("topic3", DataType::Utf8, true),
        Field::new(
            "topics",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        ),
        Field::new("data", DataType::Utf8, false),
        Field::new("data_len", DataType::UInt64, false),
        Field::new("source", DataType::UInt64, false),
    ])
}

fn projected_schema(
    schema: SchemaRef,
    projection: Option<&Vec<usize>>,
) -> DataFusionResult<SchemaRef> {
    match projection {
        Some(indices) => Ok(Arc::new(schema.project(indices)?)),
        None => Ok(schema),
    }
}

fn projected_column_names(schema: SchemaRef, projection: Option<&Vec<usize>>) -> Vec<String> {
    match projection {
        Some(indices) => indices
            .iter()
            .map(|index| schema.field(*index).name().clone())
            .collect(),
        None => schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect(),
    }
}

fn build_projected_batch(
    schema: SchemaRef,
    dir: &std::path::Path,
    row_ids: &[u32],
    projected_columns: &[String],
) -> std::io::Result<RecordBatch> {
    if projected_columns.is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(row_ids.len()));
        return RecordBatch::try_new_with_options(schema, Vec::new(), &options)
            .map_err(std::io::Error::other);
    }

    let reader = SegmentReader::open(dir)?;
    let mut arrays = Vec::with_capacity(projected_columns.len());

    for column in projected_columns {
        arrays.push(read_column_as_array(&reader, row_ids, column)?);
    }

    RecordBatch::try_new(schema, arrays).map_err(std::io::Error::other)
}

fn empty_projected_batch(schema: SchemaRef) -> DataFusionResult<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new().with_row_count(Some(0));
        return Ok(RecordBatch::try_new_with_options(
            schema,
            Vec::new(),
            &options,
        )?);
    }

    let arrays = schema
        .fields()
        .iter()
        .map(|field| new_empty_array(field.data_type()))
        .collect();
    Ok(RecordBatch::try_new(schema, arrays)?)
}

fn read_column_as_array(
    reader: &SegmentReader,
    row_ids: &[u32],
    column: &str,
) -> std::io::Result<ArrayRef> {
    let array: ArrayRef = match column {
        "block_number" => Arc::new(UInt64Array::from(
            reader.read_u64("block_number", Some(row_ids))?,
        )),
        "block_hash" => Arc::new(StringArray::from_iter_values(
            reader
                .read_b256("block_hash", Some(row_ids))?
                .into_iter()
                .map(to_hex_hash),
        )),
        "timestamp" => Arc::new(UInt64Array::from(
            reader.read_u64("timestamp", Some(row_ids))?,
        )),
        "tx_hash" => Arc::new(StringArray::from_iter_values(
            reader
                .read_b256("tx_hash", Some(row_ids))?
                .into_iter()
                .map(to_hex_hash),
        )),
        "tx_index" => Arc::new(UInt64Array::from_iter_values(
            reader
                .read_u32("tx_index", Some(row_ids))?
                .into_iter()
                .map(|value| value as u64),
        )),
        "log_index" => Arc::new(UInt64Array::from_iter_values(
            reader
                .read_u32("log_index", Some(row_ids))?
                .into_iter()
                .map(|value| value as u64),
        )),
        "address" => Arc::new(StringArray::from_iter_values(
            reader
                .read_address(Some(row_ids))?
                .into_iter()
                .map(|address| to_hex_address(address.as_slice())),
        )),
        "topic0" => Arc::new(StringArray::from(
            reader
                .read_nullable_b256("topic0", Some(row_ids))?
                .into_iter()
                .map(|topic| topic.map(to_hex_hash))
                .collect::<Vec<_>>(),
        )),
        "topic1" => Arc::new(StringArray::from(
            reader
                .read_nullable_b256("topic1", Some(row_ids))?
                .into_iter()
                .map(|topic| topic.map(to_hex_hash))
                .collect::<Vec<_>>(),
        )),
        "topic2" => Arc::new(StringArray::from(
            reader
                .read_nullable_b256("topic2", Some(row_ids))?
                .into_iter()
                .map(|topic| topic.map(to_hex_hash))
                .collect::<Vec<_>>(),
        )),
        "topic3" => Arc::new(StringArray::from(
            reader
                .read_nullable_b256("topic3", Some(row_ids))?
                .into_iter()
                .map(|topic| topic.map(to_hex_hash))
                .collect::<Vec<_>>(),
        )),
        "topics" => build_topics_array(reader, row_ids)?,
        "data" => Arc::new(StringArray::from_iter_values(
            reader
                .read_var_bytes("data", Some(row_ids))?
                .into_iter()
                .map(|bytes| to_hex_bytes(&bytes)),
        )),
        "data_len" => Arc::new(UInt64Array::from_iter_values(
            reader
                .read_u32("data_len", Some(row_ids))?
                .into_iter()
                .map(|value| value as u64),
        )),
        "source" => Arc::new(UInt64Array::from_iter_values(
            reader
                .read_u8("source", Some(row_ids))?
                .into_iter()
                .map(|value| value as u64),
        )),
        other => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unsupported projected column: {other}"),
            ));
        }
    };

    Ok(array)
}

fn build_topics_array(reader: &SegmentReader, row_ids: &[u32]) -> std::io::Result<ArrayRef> {
    let topic0 = reader.read_nullable_b256("topic0", Some(row_ids))?;
    let topic1 = reader.read_nullable_b256("topic1", Some(row_ids))?;
    let topic2 = reader.read_nullable_b256("topic2", Some(row_ids))?;
    let topic3 = reader.read_nullable_b256("topic3", Some(row_ids))?;

    let mut builder = ListBuilder::new(StringBuilder::new());
    for index in 0..row_ids.len() {
        for topic in [topic0[index], topic1[index], topic2[index], topic3[index]]
            .into_iter()
            .flatten()
        {
            builder.values().append_value(to_hex_hash(topic));
        }
        builder.append(true);
    }

    Ok(Arc::new(builder.finish()))
}

fn supports_exact_pushdown(expr: &DataFusionExpr) -> bool {
    match expr {
        DataFusionExpr::BinaryExpr(binary) => supports_binary_pushdown(binary),
        DataFusionExpr::Between(between) => supports_between_pushdown(between),
        DataFusionExpr::InList(in_list) => supports_in_list_pushdown(in_list),
        _ => false,
    }
}

fn supports_binary_pushdown(binary: &BinaryExpr) -> bool {
    let Some((column, literal, reversed)) = normalize_binary(binary) else {
        return false;
    };

    match column.as_str() {
        "block_number" => numeric_scalar(literal).is_some(),
        "timestamp" => numeric_scalar(literal).is_some(),
        "data_len" if !reversed => {
            matches!(binary.op, Operator::Eq) && numeric_scalar(literal).is_some()
        }
        "data" if !reversed => {
            matches!(binary.op, Operator::Eq | Operator::GtEq | Operator::LtEq)
                && parse_bytes_scalar(literal).is_some()
        }
        "block_hash" if !reversed => {
            matches!(binary.op, Operator::Eq) && parse_b256_scalar(literal).is_some()
        }
        "address" if !reversed => {
            matches!(binary.op, Operator::Eq) && parse_address_scalar(literal).is_some()
        }
        column if !reversed && topic_column_index(column).is_some() => {
            matches!(binary.op, Operator::Eq) && parse_b256_scalar(literal).is_some()
        }
        _ => false,
    }
}

fn supports_between_pushdown(between: &Between) -> bool {
    matches!(between.expr.as_ref(), DataFusionExpr::Column(column) if column.name == "block_number" || column.name == "timestamp")
        && !between.negated
        && scalar_literal(between.low.as_ref())
            .and_then(numeric_scalar)
            .is_some()
        && scalar_literal(between.high.as_ref())
            .and_then(numeric_scalar)
            .is_some()
}

fn supports_in_list_pushdown(in_list: &InList) -> bool {
    if in_list.negated {
        return false;
    }
    let DataFusionExpr::Column(column) = in_list.expr.as_ref() else {
        return false;
    };

    match column.name.as_str() {
        "address" => in_list.list.iter().all(|expr| {
            scalar_literal(expr)
                .and_then(parse_address_scalar)
                .is_some()
        }),
        column if topic_column_index(column).is_some() => in_list
            .list
            .iter()
            .all(|expr| scalar_literal(expr).and_then(parse_b256_scalar).is_some()),
        _ => false,
    }
}

fn build_native_pushdown_filter(filters: &[DataFusionExpr]) -> DataFusionResult<NativeLogFilter> {
    let mut filter = NativeLogFilter::new();

    for expr in filters {
        apply_pushdown_expr(&mut filter, expr)?;
    }

    Ok(filter)
}

fn apply_pushdown_expr(
    filter: &mut NativeLogFilter,
    expr: &DataFusionExpr,
) -> DataFusionResult<()> {
    match expr {
        DataFusionExpr::BinaryExpr(binary) => apply_binary_pushdown(filter, binary),
        DataFusionExpr::Between(between) => apply_between_pushdown(filter, between),
        DataFusionExpr::InList(in_list) => apply_in_list_pushdown(filter, in_list),
        other => Err(DataFusionError::Plan(format!(
            "unsupported pushed filter: {}",
            other.human_display()
        ))),
    }
}

fn apply_binary_pushdown(
    filter: &mut NativeLogFilter,
    binary: &BinaryExpr,
) -> DataFusionResult<()> {
    let Some((column, literal, reversed)) = normalize_binary(binary) else {
        return Err(DataFusionError::Plan(format!(
            "unable to normalize pushed filter {}",
            binary
        )));
    };

    match column.as_str() {
        "block_number" => {
            let value = numeric_scalar(literal)
                .ok_or_else(|| DataFusionError::Plan("invalid block_number literal".to_owned()))?;
            apply_block_number_constraint(filter, binary.op, value, reversed);
        }
        "timestamp" => {
            let value = numeric_scalar(literal)
                .ok_or_else(|| DataFusionError::Plan("invalid timestamp literal".to_owned()))?;
            apply_timestamp_constraint(filter, binary.op, value, reversed);
        }
        "data_len" if !reversed && binary.op == Operator::Eq => {
            let value = numeric_scalar(literal)
                .ok_or_else(|| DataFusionError::Plan("invalid data_len literal".to_owned()))?;
            filter.data_len = Some(value.min(u32::MAX as u64) as u32);
        }
        "data" if !reversed => {
            let value = parse_bytes_scalar(literal)
                .ok_or_else(|| DataFusionError::Plan("invalid data literal".to_owned()))?;
            apply_data_constraint(filter, binary.op, value);
        }
        "block_hash" if !reversed && binary.op == Operator::Eq => {
            filter.block_hash =
                Some(parse_b256_scalar(literal).ok_or_else(|| {
                    DataFusionError::Plan("invalid block_hash literal".to_owned())
                })?);
        }
        "address" if !reversed && binary.op == Operator::Eq => {
            merge_addresses(
                filter,
                vec![
                    parse_address_scalar(literal).ok_or_else(|| {
                        DataFusionError::Plan("invalid address literal".to_owned())
                    })?,
                ],
            );
        }
        column
            if !reversed && binary.op == Operator::Eq && topic_column_index(column).is_some() =>
        {
            let index = topic_column_index(column).unwrap();
            merge_topic_constraint(
                &mut filter.topics[index],
                TopicConstraint::One(
                    parse_b256_scalar(literal).ok_or_else(|| {
                        DataFusionError::Plan(format!("invalid {column} literal"))
                    })?,
                ),
            );
        }
        _ => {}
    }

    Ok(())
}

fn apply_between_pushdown(filter: &mut NativeLogFilter, between: &Between) -> DataFusionResult<()> {
    let DataFusionExpr::Column(column) = between.expr.as_ref() else {
        return Err(DataFusionError::Plan(
            "between pushdown requires a column".to_owned(),
        ));
    };
    if !matches!(column.name.as_str(), "block_number" | "timestamp") || between.negated {
        return Err(DataFusionError::Plan(
            "unsupported BETWEEN pushdown".to_owned(),
        ));
    }

    let low = scalar_literal(between.low.as_ref())
        .and_then(numeric_scalar)
        .ok_or_else(|| DataFusionError::Plan("invalid BETWEEN low bound".to_owned()))?;
    let high = scalar_literal(between.high.as_ref())
        .and_then(numeric_scalar)
        .ok_or_else(|| DataFusionError::Plan("invalid BETWEEN high bound".to_owned()))?;

    if column.name == "block_number" {
        filter.from_block = Some(
            filter
                .from_block
                .map(|current| current.max(low))
                .unwrap_or(low),
        );
        filter.to_block = Some(
            filter
                .to_block
                .map(|current| current.min(high))
                .unwrap_or(high),
        );
    } else {
        filter.from_timestamp = Some(
            filter
                .from_timestamp
                .map(|current| current.max(low))
                .unwrap_or(low),
        );
        filter.to_timestamp = Some(
            filter
                .to_timestamp
                .map(|current| current.min(high))
                .unwrap_or(high),
        );
    }
    Ok(())
}

fn apply_in_list_pushdown(filter: &mut NativeLogFilter, in_list: &InList) -> DataFusionResult<()> {
    let DataFusionExpr::Column(column) = in_list.expr.as_ref() else {
        return Err(DataFusionError::Plan(
            "IN pushdown requires a column".to_owned(),
        ));
    };
    if in_list.negated {
        return Err(DataFusionError::Plan(
            "NOT IN pushdown is unsupported".to_owned(),
        ));
    }

    match column.name.as_str() {
        "address" => {
            let mut addresses = Vec::with_capacity(in_list.list.len());
            for expr in &in_list.list {
                let literal = scalar_literal(expr).ok_or_else(|| {
                    DataFusionError::Plan("address IN requires string literals".to_owned())
                })?;
                addresses.push(
                    parse_address_scalar(literal).ok_or_else(|| {
                        DataFusionError::Plan("invalid address literal".to_owned())
                    })?,
                );
            }
            merge_addresses(filter, addresses);
        }
        column if topic_column_index(column).is_some() => {
            let index = topic_column_index(column).unwrap();
            let mut topics = Vec::with_capacity(in_list.list.len());
            for expr in &in_list.list {
                let literal = scalar_literal(expr).ok_or_else(|| {
                    DataFusionError::Plan(format!("{column} IN requires string literals"))
                })?;
                topics.push(
                    parse_b256_scalar(literal).ok_or_else(|| {
                        DataFusionError::Plan(format!("invalid {column} literal"))
                    })?,
                );
            }
            merge_topic_constraint(&mut filter.topics[index], TopicConstraint::AnyOf(topics));
        }
        _ => {}
    }

    Ok(())
}

fn apply_block_number_constraint(
    filter: &mut NativeLogFilter,
    operator: Operator,
    value: u64,
    reversed: bool,
) {
    match (operator, reversed) {
        (Operator::Eq, false) | (Operator::Eq, true) => {
            filter.from_block = Some(
                filter
                    .from_block
                    .map(|current| current.max(value))
                    .unwrap_or(value),
            );
            filter.to_block = Some(
                filter
                    .to_block
                    .map(|current| current.min(value))
                    .unwrap_or(value),
            );
        }
        (Operator::Gt, false) | (Operator::Lt, true) => {
            let bound = value.saturating_add(1);
            filter.from_block = Some(
                filter
                    .from_block
                    .map(|current| current.max(bound))
                    .unwrap_or(bound),
            );
        }
        (Operator::GtEq, false) | (Operator::LtEq, true) => {
            filter.from_block = Some(
                filter
                    .from_block
                    .map(|current| current.max(value))
                    .unwrap_or(value),
            );
        }
        (Operator::Lt, false) | (Operator::Gt, true) => {
            let bound = value.saturating_sub(1);
            filter.to_block = Some(
                filter
                    .to_block
                    .map(|current| current.min(bound))
                    .unwrap_or(bound),
            );
        }
        (Operator::LtEq, false) | (Operator::GtEq, true) => {
            filter.to_block = Some(
                filter
                    .to_block
                    .map(|current| current.min(value))
                    .unwrap_or(value),
            );
        }
        _ => {}
    }
}

fn apply_timestamp_constraint(
    filter: &mut NativeLogFilter,
    operator: Operator,
    value: u64,
    reversed: bool,
) {
    match (operator, reversed) {
        (Operator::Eq, false) | (Operator::Eq, true) => {
            filter.from_timestamp = Some(
                filter
                    .from_timestamp
                    .map(|current| current.max(value))
                    .unwrap_or(value),
            );
            filter.to_timestamp = Some(
                filter
                    .to_timestamp
                    .map(|current| current.min(value))
                    .unwrap_or(value),
            );
        }
        (Operator::Gt, false) | (Operator::Lt, true) => {
            let bound = value.saturating_add(1);
            filter.from_timestamp = Some(
                filter
                    .from_timestamp
                    .map(|current| current.max(bound))
                    .unwrap_or(bound),
            );
        }
        (Operator::GtEq, false) | (Operator::LtEq, true) => {
            filter.from_timestamp = Some(
                filter
                    .from_timestamp
                    .map(|current| current.max(value))
                    .unwrap_or(value),
            );
        }
        (Operator::Lt, false) | (Operator::Gt, true) => {
            let bound = value.saturating_sub(1);
            filter.to_timestamp = Some(
                filter
                    .to_timestamp
                    .map(|current| current.min(bound))
                    .unwrap_or(bound),
            );
        }
        (Operator::LtEq, false) | (Operator::GtEq, true) => {
            filter.to_timestamp = Some(
                filter
                    .to_timestamp
                    .map(|current| current.min(value))
                    .unwrap_or(value),
            );
        }
        _ => {}
    }
}

fn apply_data_constraint(filter: &mut NativeLogFilter, operator: Operator, value: Vec<u8>) {
    match operator {
        Operator::Eq => {
            filter.data_min = Some(match filter.data_min.take() {
                Some(current) => current.max(value.clone()),
                None => value.clone(),
            });
            filter.data_max = Some(match filter.data_max.take() {
                Some(current) => current.min(value),
                None => value,
            });
        }
        Operator::GtEq => {
            filter.data_min = Some(match filter.data_min.take() {
                Some(current) => current.max(value),
                None => value,
            });
        }
        Operator::LtEq => {
            filter.data_max = Some(match filter.data_max.take() {
                Some(current) => current.min(value),
                None => value,
            });
        }
        _ => {}
    }
}

fn merge_addresses(filter: &mut NativeLogFilter, next: Vec<Address>) {
    if filter.addresses.is_empty() {
        filter.addresses = next;
    } else {
        filter.addresses.retain(|address| next.contains(address));
    }
}

fn merge_topic_constraint(current: &mut TopicConstraint, next: TopicConstraint) {
    *current = match (&*current, next) {
        (TopicConstraint::Any, next) => next,
        (TopicConstraint::One(current), TopicConstraint::Any) => TopicConstraint::One(*current),
        (TopicConstraint::AnyOf(current), TopicConstraint::Any) => {
            TopicConstraint::AnyOf(current.clone())
        }
        (TopicConstraint::One(current), TopicConstraint::One(next)) => {
            if *current == next {
                TopicConstraint::One(*current)
            } else {
                TopicConstraint::AnyOf(Vec::new())
            }
        }
        (TopicConstraint::One(current), TopicConstraint::AnyOf(next)) => {
            if next.contains(current) {
                TopicConstraint::One(*current)
            } else {
                TopicConstraint::AnyOf(Vec::new())
            }
        }
        (TopicConstraint::AnyOf(current), TopicConstraint::One(next)) => {
            if current.contains(&next) {
                TopicConstraint::One(next)
            } else {
                TopicConstraint::AnyOf(Vec::new())
            }
        }
        (TopicConstraint::AnyOf(current), TopicConstraint::AnyOf(next)) => TopicConstraint::AnyOf(
            current
                .iter()
                .copied()
                .filter(|topic| next.contains(topic))
                .collect(),
        ),
    };
}

fn topic_column_index(column: &str) -> Option<usize> {
    match column {
        "topic0" => Some(0),
        "topic1" => Some(1),
        "topic2" => Some(2),
        "topic3" => Some(3),
        _ => None,
    }
}

fn normalize_binary(binary: &BinaryExpr) -> Option<(String, &ScalarValue, bool)> {
    match (binary.left.as_ref(), binary.right.as_ref()) {
        (DataFusionExpr::Column(column), DataFusionExpr::Literal(literal, _)) => {
            Some((column.name.clone(), literal, false))
        }
        (DataFusionExpr::Literal(literal, _), DataFusionExpr::Column(column)) => {
            Some((column.name.clone(), literal, true))
        }
        _ => None,
    }
}

fn scalar_literal(expr: &DataFusionExpr) -> Option<&ScalarValue> {
    match expr {
        DataFusionExpr::Literal(literal, _) => Some(literal),
        _ => None,
    }
}

fn numeric_scalar(value: &ScalarValue) -> Option<u64> {
    match value {
        ScalarValue::UInt64(Some(value)) => Some(*value),
        ScalarValue::UInt32(Some(value)) => Some(*value as u64),
        ScalarValue::UInt16(Some(value)) => Some(*value as u64),
        ScalarValue::UInt8(Some(value)) => Some(*value as u64),
        ScalarValue::Int64(Some(value)) if *value >= 0 => Some(*value as u64),
        ScalarValue::Int32(Some(value)) if *value >= 0 => Some(*value as u64),
        ScalarValue::Int16(Some(value)) if *value >= 0 => Some(*value as u64),
        ScalarValue::Int8(Some(value)) if *value >= 0 => Some(*value as u64),
        _ => None,
    }
}

fn parse_address_scalar(value: &ScalarValue) -> Option<Address> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => parse_address(value.trim()),
        _ => None,
    }
}

fn parse_b256_scalar(value: &ScalarValue) -> Option<B256> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => parse_b256(value.trim()),
        _ => None,
    }
}

fn parse_bytes_scalar(value: &ScalarValue) -> Option<Vec<u8>> {
    match value {
        ScalarValue::Utf8(Some(value))
        | ScalarValue::Utf8View(Some(value))
        | ScalarValue::LargeUtf8(Some(value)) => parse_hex_bytes(value.trim()),
        _ => None,
    }
}

fn parse_address(value: &str) -> Option<Address> {
    let hex = value.strip_prefix("0x").unwrap_or(value);
    let bytes = hex::decode(hex).ok()?;
    (bytes.len() == 20).then(|| Address::from_slice(&bytes))
}

fn parse_b256(value: &str) -> Option<B256> {
    let hex = value.strip_prefix("0x").unwrap_or(value);
    let bytes = hex::decode(hex).ok()?;
    (bytes.len() == 32).then(|| B256::from_slice(&bytes))
}

fn parse_hex_bytes(value: &str) -> Option<Vec<u8>> {
    let hex = value.strip_prefix("0x").unwrap_or(value);
    hex.len().is_multiple_of(2).then_some(())?;
    hex::decode(hex).ok()
}

fn rewrite_legacy_sql(sql: &str, head_block: u64) -> Result<String, SqlQueryError> {
    let sql = rewrite_missing_select_projection(sql);
    let sql = replace_latest_keyword(&sql, head_block);
    let sql = rewrite_event_literals(&sql)?;
    rewrite_address_literals(&sql)
}

fn rewrite_missing_select_projection(sql: &str) -> String {
    let tokens = match tokenize(sql) {
        Ok(tokens) => tokens,
        Err(_) => return sql.to_owned(),
    };

    if !matches!(tokens.as_slice(), [Token::Select, Token::From, ..]) {
        return sql.to_owned();
    }

    let Some(select_end) = find_leading_select_end(sql) else {
        return sql.to_owned();
    };

    let mut rewritten = String::with_capacity(sql.len() + 2);
    rewritten.push_str(&sql[..select_end]);
    rewritten.push_str(" *");
    rewritten.push_str(&sql[select_end..]);
    rewritten
}

fn find_leading_select_end(sql: &str) -> Option<usize> {
    let chars: Vec<(usize, char)> = sql.char_indices().collect();
    let mut index = 0;

    while index < chars.len() {
        let (_, ch) = chars[index];
        if ch.is_whitespace() {
            index += 1;
            continue;
        }

        if ch == '-' && chars.get(index + 1).is_some_and(|(_, next)| *next == '-') {
            index += 2;
            while index < chars.len() && chars[index].1 != '\n' {
                index += 1;
            }
            continue;
        }

        break;
    }

    let start = index;
    const SELECT: &str = "select";
    for expected in SELECT.chars() {
        let (_, actual) = *chars.get(index)?;
        if !actual.eq_ignore_ascii_case(&expected) {
            return None;
        }
        index += 1;
    }

    let boundary_ok = start == 0 || !is_identifier_char(chars[start.saturating_sub(1)].1);
    let next_is_boundary = chars
        .get(index)
        .is_none_or(|(_, ch)| !is_identifier_char(*ch));

    (boundary_ok && next_is_boundary).then(|| {
        chars
            .get(index)
            .map(|(offset, _)| *offset)
            .unwrap_or_else(|| sql.len())
    })
}

fn replace_latest_keyword(sql: &str, head_block: u64) -> String {
    let mut rewritten = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut index = 0;
    let mut in_string = false;

    while index < chars.len() {
        let ch = chars[index];
        if ch == '\'' {
            in_string = !in_string;
            rewritten.push(ch);
            index += 1;
            continue;
        }

        if !in_string
            && index + 6 <= chars.len()
            && chars[index..index + 6]
                .iter()
                .collect::<String>()
                .eq_ignore_ascii_case("latest")
        {
            let prev_ok = index == 0 || !is_identifier_char(chars[index - 1]);
            let next_ok = index + 6 == chars.len() || !is_identifier_char(chars[index + 6]);
            if prev_ok && next_ok {
                rewritten.push_str(&head_block.to_string());
                index += 6;
                continue;
            }
        }

        rewritten.push(ch);
        index += 1;
    }

    rewritten
}

fn rewrite_event_literals(sql: &str) -> Result<String, SqlQueryError> {
    rewrite_prefixed_literals(sql, "event", |literal| {
        Ok(format!("'{}'", to_hex_hash(keccak256(literal.as_bytes()))))
    })
}

fn rewrite_address_literals(sql: &str) -> Result<String, SqlQueryError> {
    rewrite_prefixed_literals(sql, "address", |literal| {
        let address = parse_address(literal).ok_or_else(|| {
            SqlQueryError::LegacySyntax(format!("invalid address literal: {literal}"))
        })?;
        let mut padded = [0u8; 32];
        padded[12..].copy_from_slice(address.as_slice());
        Ok(format!("'{}'", to_hex_hash(B256::from(padded))))
    })
}

fn rewrite_prefixed_literals(
    sql: &str,
    prefix: &str,
    mut rewrite: impl FnMut(&str) -> Result<String, SqlQueryError>,
) -> Result<String, SqlQueryError> {
    let mut out = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut index = 0;
    let prefix_chars: Vec<char> = prefix.chars().collect();

    while index < chars.len() {
        let matches_prefix = index + prefix_chars.len() + 1 < chars.len()
            && chars[index..index + prefix_chars.len()]
                .iter()
                .collect::<String>()
                .eq_ignore_ascii_case(prefix)
            && chars[index + prefix_chars.len()] == '\'';

        if matches_prefix {
            let start = index + prefix_chars.len() + 1;
            let mut end = start;
            while end < chars.len() && chars[end] != '\'' {
                end += 1;
            }
            if end >= chars.len() {
                return Err(SqlQueryError::LegacySyntax(format!(
                    "unterminated {prefix} literal"
                )));
            }

            let literal = chars[start..end].iter().collect::<String>();
            out.push_str(&rewrite(&literal)?);
            index = end + 1;
            continue;
        }

        out.push(chars[index]);
        index += 1;
    }

    Ok(out)
}

fn is_identifier_char(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_'
}

fn record_batches_to_json(batches: &[RecordBatch]) -> Vec<Value> {
    let mut rows = Vec::new();
    for batch in batches {
        let fields = batch.schema().fields().clone();
        for row_index in 0..batch.num_rows() {
            let mut row = Map::with_capacity(batch.num_columns());
            for (field, column) in fields.iter().zip(batch.columns()) {
                row.insert(
                    field.name().clone(),
                    array_value_to_json(column.as_ref(), row_index),
                );
            }
            rows.push(Value::Object(row));
        }
    }
    rows
}

fn array_value_to_json(array: &dyn Array, row_index: usize) -> Value {
    if array.is_null(row_index) {
        return Value::Null;
    }

    match array.data_type() {
        DataType::Utf8 => array
            .as_any()
            .downcast_ref::<StringArray>()
            .map(|array| Value::String(array.value(row_index).to_owned()))
            .unwrap_or(Value::Null),
        DataType::LargeUtf8 => array
            .as_any()
            .downcast_ref::<LargeStringArray>()
            .map(|array| Value::String(array.value(row_index).to_owned()))
            .unwrap_or(Value::Null),
        DataType::UInt64 => array
            .as_any()
            .downcast_ref::<UInt64Array>()
            .map(|array| Value::Number(array.value(row_index).into()))
            .unwrap_or(Value::Null),
        DataType::UInt32 => array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|array| Value::Number((array.value(row_index) as u64).into()))
            .unwrap_or(Value::Null),
        DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|array| Value::Number(array.value(row_index).into()))
            .unwrap_or(Value::Null),
        DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|array| Value::Number((array.value(row_index) as i64).into()))
            .unwrap_or(Value::Null),
        DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .and_then(|array| serde_json::Number::from_f64(array.value(row_index)))
            .map(Value::Number)
            .unwrap_or(Value::Null),
        DataType::Boolean => array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .map(|array| Value::Bool(array.value(row_index)))
            .unwrap_or(Value::Null),
        DataType::List(_) => array
            .as_any()
            .downcast_ref::<ListArray>()
            .map(|array| {
                let values = array.value(row_index);
                let strings = values.as_any().downcast_ref::<StringArray>();
                match strings {
                    Some(strings) => Value::Array(
                        (0..strings.len())
                            .map(|index| {
                                if strings.is_null(index) {
                                    Value::Null
                                } else {
                                    Value::String(strings.value(index).to_owned())
                                }
                            })
                            .collect(),
                    ),
                    None => Value::Null,
                }
            })
            .unwrap_or(Value::Null),
        _ => Value::String(format!("{:?}", array.data_type())),
    }
}

fn to_hex_hash(hash: impl AsRef<[u8]>) -> String {
    format!("0x{}", hex::encode(hash))
}

fn to_hex_address(address: &[u8]) -> String {
    format!("0x{}", hex::encode(address))
}

fn to_hex_bytes(data: &[u8]) -> String {
    format!("0x{}", hex::encode(data))
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, bytes, keccak256};
    use logex_index::IndexBuilder;
    use logex_storage::PartitionManagerConfig;
    use logex_types::{LogRow, Source};
    use tempfile::TempDir;

    use super::*;

    fn make_test_rows() -> Vec<LogRow> {
        vec![
            LogRow {
                block_number: 100,
                block_hash: B256::repeat_byte(0x01),
                timestamp: 1_700_000_000,
                tx_hash: B256::repeat_byte(0x11),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xAA),
                topic0: Some(B256::repeat_byte(0xDD)),
                topic1: Some(B256::repeat_byte(0x99)),
                topic2: None,
                topic3: None,
                data: bytes!(""),
                data_len: 0,
                source: Source::Receipt,
            },
            LogRow {
                block_number: 200,
                block_hash: B256::repeat_byte(0x02),
                timestamp: 1_700_001_200,
                tx_hash: B256::repeat_byte(0x22),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xBB),
                topic0: Some(B256::repeat_byte(0xEE)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!("cafe"),
                data_len: 2,
                source: Source::Receipt,
            },
        ]
    }

    fn setup_storage() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let mut storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        storage.write_batch(&make_test_rows()).unwrap();
        IndexBuilder::build_all_indexes(&storage.hot_partition().meta.path).unwrap();
        storage
            .refresh_segment_indexes(storage.hot_partition().meta.id)
            .unwrap();
        (tmp, storage)
    }

    fn setup_unindexed_storage() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let mut storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        storage.write_batch(&make_test_rows()).unwrap();
        (tmp, storage)
    }

    fn setup_amount_storage() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let mut storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        storage
            .write_batch(&[
                LogRow {
                    block_number: 300,
                    block_hash: B256::repeat_byte(0x03),
                    timestamp: 1_700_003_000,
                    tx_hash: B256::repeat_byte(0x33),
                    tx_index: 0,
                    log_index: 0,
                    address: Address::repeat_byte(0xCC),
                    topic0: Some(B256::repeat_byte(0xDD)),
                    topic1: None,
                    topic2: None,
                    topic3: None,
                    data: bytes!(
                        "0000000000000000000000000000000100000000000000000000000000000000"
                    ),
                    data_len: 32,
                    source: Source::Receipt,
                },
                LogRow {
                    block_number: 301,
                    block_hash: B256::repeat_byte(0x04),
                    timestamp: 1_700_003_012,
                    tx_hash: B256::repeat_byte(0x44),
                    tx_index: 0,
                    log_index: 0,
                    address: Address::repeat_byte(0xCC),
                    topic0: Some(B256::repeat_byte(0xDD)),
                    topic1: None,
                    topic2: None,
                    topic3: None,
                    data: bytes!(
                        "0000000000000000000000000000000000000000000000000000000000000005"
                    ),
                    data_len: 32,
                    source: Source::Receipt,
                },
            ])
            .unwrap();
        IndexBuilder::build_all_indexes(&storage.hot_partition().meta.path).unwrap();
        storage
            .refresh_segment_indexes(storage.hot_partition().meta.id)
            .unwrap();
        (tmp, storage)
    }

    fn setup_transfer_balance_storage() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let token = parse_address("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap();
        let target = padded_address_topic("0xE6c031F4C63e76e453d9A0aAe566D06236d11F95");
        let other = padded_address_topic("0x99C7ec507e16489F901214aB4ed558737B5e9BDe");
        let transfer = keccak256(b"Transfer(address,address,uint256)");
        let mut storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        storage
            .write_batch(&[
                transfer_row(25_000_001, token, other, target, 100),
                transfer_row(25_000_002, token, target, other, 30),
                transfer_row(25_000_002, token, target, target, 20),
                transfer_row(25_000_003, token, other, target, 5),
                transfer_row(24_999_999, token, other, target, 1_000),
            ])
            .unwrap();
        IndexBuilder::build_all_indexes(&storage.hot_partition().meta.path).unwrap();
        storage
            .refresh_segment_indexes(storage.hot_partition().meta.id)
            .unwrap();

        let rows = storage
            .hot_partition()
            .meta
            .path
            .join("indexes")
            .join("topic0.bptree");
        assert!(rows.exists());
        assert_eq!(transfer, keccak256(b"Transfer(address,address,uint256)"));
        (tmp, storage)
    }

    fn padded_address_topic(address: &str) -> B256 {
        let address = parse_address(address).unwrap();
        let mut padded = [0u8; 32];
        padded[12..].copy_from_slice(address.as_slice());
        B256::from(padded)
    }

    fn transfer_row(
        block_number: u64,
        token: Address,
        from: B256,
        to: B256,
        amount: u64,
    ) -> LogRow {
        let mut data = [0u8; 32];
        data[24..].copy_from_slice(&amount.to_be_bytes());
        LogRow {
            block_number,
            block_hash: B256::repeat_byte((block_number % 255) as u8),
            timestamp: 1_700_000_000 + block_number,
            tx_hash: B256::repeat_byte((block_number % 251) as u8),
            tx_index: 0,
            log_index: 0,
            address: token,
            topic0: Some(keccak256(b"Transfer(address,address,uint256)")),
            topic1: Some(from),
            topic2: Some(to),
            topic3: None,
            data: data.into(),
            data_len: 32,
            source: Source::Receipt,
        }
    }

    #[tokio::test]
    async fn executes_aggregate_sql_against_native_storage() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql(
            "SELECT COUNT(*) AS total FROM logs WHERE block_number <= latest",
            &storage,
            storage.head_block(),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["total"], 2);
    }

    #[tokio::test]
    async fn sums_hex_data_as_exact_decimal_bigint() {
        let (_tmp, storage) = setup_amount_storage();
        let result = execute_sql_page(
            "SELECT SUM(data) AS total FROM logs WHERE data_len = 32",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            result.rows[0]["total"],
            "340282366920938463463374607431768211461"
        );
        assert_eq!(result.total_scanned, 2);
    }

    #[tokio::test]
    async fn sums_cast_hex_data_as_numeric() {
        let (_tmp, storage) = setup_amount_storage();
        let result = execute_sql_page(
            "SELECT SUM(CAST(data AS NUMERIC)) AS total FROM logs WHERE data_len = 32",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(
            result.rows[0]["total"],
            "340282366920938463463374607431768211461"
        );
        assert_eq!(result.total_scanned, 2);
    }

    #[tokio::test]
    async fn sums_postgres_style_cast_hex_data_as_numeric() {
        let (_tmp, storage) = setup_amount_storage();
        let result = execute_sql_page(
            "SELECT SUM(data::NUMERIC) AS total FROM logs WHERE data_len = 32",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(
            result.rows[0]["total"],
            "340282366920938463463374607431768211461"
        );
        assert_eq!(result.total_scanned, 2);
    }

    #[tokio::test]
    async fn sum_data_returns_null_when_no_rows_match() {
        let (_tmp, storage) = setup_amount_storage();
        let result = execute_sql_page(
            "SELECT SUM(data) AS total FROM logs WHERE block_number = 999",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert!(result.rows[0]["total"].is_null());
        assert_eq!(result.total_scanned, 0);
    }

    #[tokio::test]
    async fn sums_case_expressions_and_subtracts_exact_uint256_values() {
        let (_tmp, storage) = setup_transfer_balance_storage();
        let result = execute_sql_page(
            "SELECT
               SUM(CASE WHEN topic2 = address'0xE6c031F4C63e76e453d9A0aAe566D06236d11F95'
                        THEN data ELSE 0 END) AS total_received,
               SUM(CASE WHEN topic1 = address'0xE6c031F4C63e76e453d9A0aAe566D06236d11F95'
                        THEN data ELSE 0 END) AS total_sent,
               SUM(CASE WHEN topic2 = address'0xE6c031F4C63e76e453d9A0aAe566D06236d11F95'
                        THEN data ELSE 0 END) -
               SUM(CASE WHEN topic1 = address'0xE6c031F4C63e76e453d9A0aAe566D06236d11F95'
                        THEN data ELSE 0 END) AS net_balance
             FROM logs
             WHERE topic0 = event'Transfer(address,address,uint256)'
               AND address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
               AND (
                 topic2 = address'0xE6c031F4C63e76e453d9A0aAe566D06236d11F95'
                 OR
                 topic1 = address'0xE6c031F4C63e76e453d9A0aAe566D06236d11F95'
               )
               AND block_number BETWEEN 25000000 AND 25108000
               AND data_len = 32
               AND data >= '0x000000000000000000000000000000000000000000000000000000000000000a'",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["total_received"], "120");
        assert_eq!(result.rows[0]["total_sent"], "50");
        assert_eq!(result.rows[0]["net_balance"], "70");
        assert_eq!(result.total_scanned, 3);
    }

    #[tokio::test]
    async fn supports_regular_sql_order_by_desc() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql(
            "SELECT block_number AS bn FROM logs ORDER BY block_number DESC LIMIT 1",
            &storage,
            storage.head_block(),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["bn"], 200);
    }

    #[tokio::test]
    async fn returns_empty_rows_when_filter_matches_no_segments() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql_page(
            "SELECT block_number, tx_hash FROM logs WHERE block_number = 999 ORDER BY block_number DESC, tx_index DESC, log_index DESC",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert!(result.rows.is_empty());
        assert_eq!(result.total_scanned, 0);
    }

    #[tokio::test]
    async fn applies_pushed_filters_when_segment_indexes_are_missing() {
        let (_tmp, storage) = setup_unindexed_storage();
        let result = execute_sql_page(
            "SELECT block_number, address FROM logs WHERE address = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' AND block_number BETWEEN 100 AND 100",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["block_number"], 100);
        assert_eq!(
            result.rows[0]["address"],
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[tokio::test]
    async fn pushes_timestamp_between_into_native_scan() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql_page(
            "SELECT block_number FROM logs WHERE timestamp BETWEEN 1700001001 AND 1700001200",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["block_number"], 200);
        assert_eq!(result.total_scanned, 1);
    }

    #[tokio::test]
    async fn pushes_topic_in_predicates_beyond_topic0() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql_page(
            "SELECT block_number FROM logs WHERE topic0 = '0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' AND topic1 IN ('0x9999999999999999999999999999999999999999999999999999999999999999')",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["block_number"], 100);
        assert_eq!(result.total_scanned, 1);
    }

    #[tokio::test]
    async fn pushes_address_topic_and_timestamp_filters_together() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql_page(
            "SELECT block_number FROM logs \
             WHERE address = '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' \
               AND topic0 = '0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd' \
               AND topic1 IN ('0x9999999999999999999999999999999999999999999999999999999999999999') \
               AND timestamp BETWEEN 1700000000 AND 1700000000",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["block_number"], 100);
        assert_eq!(result.total_scanned, 1);
    }

    #[tokio::test]
    async fn pushes_data_len_and_data_range_into_native_scan() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql_page(
            "SELECT block_number FROM logs \
             WHERE data_len = 2 \
               AND data >= '0xca00' \
               AND data <= '0xcb00'",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["block_number"], 200);
        assert_eq!(result.total_scanned, 1);
    }

    #[tokio::test]
    async fn native_executes_ordered_limited_log_queries() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql_page(
            "SELECT block_number, data FROM logs \
             WHERE data_len = 2 \
               AND data >= '0xca00' \
               AND data <= '0xcb00' \
             ORDER BY block_number DESC, tx_index DESC, log_index DESC \
             LIMIT 1",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["block_number"], 200);
        assert_eq!(result.rows[0]["data"], "0xcafe");
        assert_eq!(result.total_scanned, 1);
    }

    #[test]
    fn parses_transfer_query_shape_for_native_execution() {
        let sql = rewrite_legacy_sql(
            "SELECT block_number, tx_hash, log_index, address, topic0, topic1, topic2, data
             FROM logs
             WHERE topic0 = event'Transfer(address,address,uint256)'
               AND address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
               AND topic1 IN (address'0xE6c031F4C63e76e453d9A0aAe566D06236d11F95')
               AND timestamp BETWEEN 1767214800 AND 1778924400
               AND data_len = 32
               AND data >= '0x00000000000000000000000000000000000000000000000000000000004c4b40'
               AND data <= '0x00000000000000000000000000000000000000000000000000000002540be400'
             ORDER BY block_number DESC, tx_index DESC, log_index DESC
             LIMIT 500",
            25_100_000,
        )
        .unwrap();

        assert!(parse_native_select_query(&sql).unwrap().is_some());
    }

    #[tokio::test]
    async fn aggregates_over_empty_log_matches() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql(
            "SELECT COUNT(*) AS total FROM logs WHERE block_number = 999",
            &storage,
            storage.head_block(),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["total"], 0);
        assert_eq!(result.total_scanned, 0);
    }

    #[tokio::test]
    async fn accepts_large_explicit_query_page_limits() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql_page(
            "SELECT * FROM logs",
            &storage,
            storage.head_block(),
            SqlQueryPage::new(Some(10_001), 0),
        )
        .await
        .expect("large query page limits should be accepted");

        assert_eq!(result.rows.len(), 2);
    }

    #[tokio::test]
    async fn rewrites_legacy_event_literals() {
        let (_tmp, storage) = setup_storage();
        let sql = "SELECT * FROM logs WHERE topic0 = event'Transfer(address,address,uint256)'";
        let rewritten = rewrite_legacy_sql(sql, storage.head_block().unwrap_or(0)).unwrap();
        assert!(rewritten.contains("0xddf252ad"));
    }

    #[tokio::test]
    async fn rewrites_legacy_address_literals() {
        let (_tmp, storage) = setup_storage();
        let sql =
            "SELECT * FROM logs WHERE topic1 = address'0xdAC17F958D2ee523a2206206994597C13D831ec7'";
        let rewritten = rewrite_legacy_sql(sql, storage.head_block().unwrap_or(0)).unwrap();
        assert!(
            rewritten
                .contains("0x000000000000000000000000dac17f958d2ee523a2206206994597c13d831ec7")
        );
    }

    #[tokio::test]
    async fn rewrites_missing_projection_to_select_star() {
        let (_tmp, storage) = setup_storage();
        let sql = "select from logs where block_number = 100";
        let rewritten = rewrite_legacy_sql(sql, storage.head_block().unwrap_or(0)).unwrap();
        assert_eq!(rewritten, "select * from logs where block_number = 100");
    }

    #[tokio::test]
    async fn executes_missing_projection_shorthand() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql(
            "select from logs where block_number = 100",
            &storage,
            storage.head_block(),
        )
        .await
        .unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["block_number"], 100);
        assert_eq!(
            result.rows[0]["address"],
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
    }

    #[tokio::test]
    async fn rejects_mutating_sql_statements() {
        let (_tmp, storage) = setup_storage();

        for sql in [
            "UPDATE logs SET block_number = 1",
            "DELETE FROM logs",
            "INSERT INTO logs VALUES (1)",
            "CREATE TABLE nope AS SELECT * FROM logs",
            "SELECT * FROM logs; DELETE FROM logs",
        ] {
            let error = execute_sql(sql, &storage, storage.head_block())
                .await
                .expect_err("mutating SQL must be rejected");
            assert!(matches!(error, SqlQueryError::DataFusion(_)));
        }
    }
}
