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
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
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

#[derive(Debug, Clone, Copy)]
pub struct SqlQueryPage {
    pub limit: Option<usize>,
    pub offset: usize,
}

impl Default for SqlQueryPage {
    fn default() -> Self {
        Self {
            limit: None,
            offset: 0,
        }
    }
}

impl SqlQueryPage {
    pub fn new(limit: Option<usize>, offset: usize) -> Self {
        Self { limit, offset }
    }
}

#[derive(Debug)]
struct LogexTableProvider {
    schema: SchemaRef,
    snapshot: StorageSnapshot,
    total_scanned: Arc<AtomicU64>,
}

impl LogexTableProvider {
    fn new(snapshot: StorageSnapshot, total_scanned: Arc<AtomicU64>) -> Self {
        Self {
            schema: Arc::new(log_rows_schema()),
            snapshot,
            total_scanned,
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
            if !partition_matches_filter(&partition, &filter) {
                continue;
            }

            let mut row_ids = candidate_row_ids(&partition.path, &filter, true)
                .map_err(DataFusionError::IoError)?;
            if row_ids.is_empty() {
                continue;
            }
            if has_native_constraints(&filter) {
                row_ids = exact_candidate_row_ids(&partition.path, &filter, row_ids)
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
        || !filter.addresses.is_empty()
        || filter
            .topics
            .iter()
            .any(|constraint| !matches!(constraint, TopicConstraint::Any))
}

fn exact_candidate_row_ids(
    dir: &std::path::Path,
    filter: &NativeLogFilter,
    row_ids: Vec<u32>,
) -> std::io::Result<Vec<u32>> {
    let reader = SegmentReader::open(dir)?;
    let rows = reader.read_log_rows(Some(&row_ids))?;
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

#[derive(Debug)]
struct LogSegmentBatchGenerator {
    dir: std::path::PathBuf,
    schema: SchemaRef,
    projected_columns: Vec<String>,
    row_ids: Vec<u32>,
    offset: usize,
}

impl LogSegmentBatchGenerator {
    fn new(
        dir: std::path::PathBuf,
        schema: SchemaRef,
        projected_columns: Vec<String>,
        row_ids: Vec<u32>,
    ) -> Self {
        Self {
            dir,
            schema,
            projected_columns,
            row_ids,
            offset: 0,
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
    let SqlQueryPage { limit, offset } = page;
    let head_block = head_block.unwrap_or_else(|| storage.head_block().unwrap_or(0));
    if let Some(message) = unsupported_from_alias_sort_shorthand(sql) {
        return Err(SqlQueryError::DataFusion(DataFusionError::Plan(message)));
    }
    let sql = rewrite_legacy_sql(sql, head_block)?;
    enforce_read_only_sql(&sql)?;
    let total_scanned = Arc::new(AtomicU64::new(0));
    let table = LogexTableProvider::new(
        StorageSnapshot::from_storage(storage),
        Arc::clone(&total_scanned),
    );

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(table))?;

    let dataframe = ctx.sql(&sql).await?;
    let dataframe = if offset > 0 || limit.is_some() {
        dataframe.limit(offset, limit)?
    } else {
        dataframe
    };
    let batches = dataframe.collect().await?;
    let rows = record_batches_to_json(&batches);

    Ok(SqlQueryResult {
        rows,
        total_scanned: total_scanned.load(Ordering::Relaxed),
    })
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
        "block_hash" if !reversed => {
            matches!(binary.op, Operator::Eq) && parse_b256_scalar(literal).is_some()
        }
        "address" if !reversed => {
            matches!(binary.op, Operator::Eq) && parse_address_scalar(literal).is_some()
        }
        "topic0" if !reversed => {
            matches!(binary.op, Operator::Eq) && parse_b256_scalar(literal).is_some()
        }
        _ => false,
    }
}

fn supports_between_pushdown(between: &Between) -> bool {
    matches!(between.expr.as_ref(), DataFusionExpr::Column(column) if column.name == "block_number")
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
        "topic0" => in_list
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
        "topic0" if !reversed && binary.op == Operator::Eq => {
            merge_topic_constraint(
                &mut filter.topics[0],
                TopicConstraint::One(
                    parse_b256_scalar(literal).ok_or_else(|| {
                        DataFusionError::Plan("invalid topic0 literal".to_owned())
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
    if column.name != "block_number" || between.negated {
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
        "topic0" => {
            let mut topics = Vec::with_capacity(in_list.list.len());
            for expr in &in_list.list {
                let literal = scalar_literal(expr).ok_or_else(|| {
                    DataFusionError::Plan("topic0 IN requires string literals".to_owned())
                })?;
                topics.push(
                    parse_b256_scalar(literal).ok_or_else(|| {
                        DataFusionError::Plan("invalid topic0 literal".to_owned())
                    })?,
                );
            }
            merge_topic_constraint(&mut filter.topics[0], TopicConstraint::AnyOf(topics));
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
    use alloy_primitives::{Address, B256, bytes};
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
