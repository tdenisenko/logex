use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, LargeStringArray,
    ListArray, ListBuilder, StringArray, StringBuilder, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionContext;
use serde_json::{Map, Value};

use logex_storage::PartitionManager;
use logex_types::LogRow;

use crate::{BinOp, Expr, Query, SelectItem, execute as execute_legacy, parse};

const DATAFUSION_BATCH_SIZE: usize = 4_096;

#[derive(Debug, thiserror::Error)]
pub enum SqlQueryError {
    #[error("storage error: {0}")]
    Storage(#[from] std::io::Error),
    #[error("sql error: {0}")]
    DataFusion(#[from] DataFusionError),
}

#[derive(Debug)]
pub struct SqlQueryResult {
    pub rows: Vec<Value>,
    pub total_scanned: u64,
}

pub async fn execute_sql(
    sql: &str,
    storage: &PartitionManager,
    head_block: Option<u64>,
) -> Result<SqlQueryResult, SqlQueryError> {
    let head_block = head_block.unwrap_or_else(|| storage.head_block().unwrap_or(0));

    let (candidate_rows, total_scanned, normalized_sql) = match parse(sql) {
        Ok(query) => {
            let seed_query = Query {
                select: vec![SelectItem::Star],
                where_clause: query.where_clause.clone(),
                group_by: vec![],
                order_by: vec![],
                limit: None,
            };
            let seed_result = execute_legacy(&seed_query, storage, Some(head_block))?;
            (
                seed_result.rows,
                seed_result.total_scanned,
                compile_query(&query, head_block),
            )
        }
        Err(_) => {
            let seed_query = Query {
                select: vec![SelectItem::Star],
                where_clause: None,
                group_by: vec![],
                order_by: vec![],
                limit: None,
            };
            let seed_result = execute_legacy(&seed_query, storage, Some(head_block))?;
            (seed_result.rows, seed_result.total_scanned, sql.to_owned())
        }
    };

    let schema = Arc::new(log_rows_schema());
    let batches = rows_to_batches(Arc::clone(&schema), &candidate_rows)?;
    let table = MemTable::try_new(schema, vec![batches])?;

    let ctx = SessionContext::new();
    ctx.register_table("logs", Arc::new(table))?;

    let dataframe = ctx.sql(&normalized_sql).await?;
    let result_batches = dataframe.collect().await?;
    let rows = record_batches_to_json(&result_batches);

    Ok(SqlQueryResult {
        rows,
        total_scanned,
    })
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

fn rows_to_batches(
    schema: Arc<Schema>,
    rows: &[LogRow],
) -> Result<Vec<RecordBatch>, DataFusionError> {
    if rows.is_empty() {
        return Ok(vec![RecordBatch::new_empty(schema)]);
    }

    rows.chunks(DATAFUSION_BATCH_SIZE)
        .map(|chunk| rows_to_batch(Arc::clone(&schema), chunk))
        .collect()
}

fn rows_to_batch(schema: Arc<Schema>, rows: &[LogRow]) -> Result<RecordBatch, DataFusionError> {
    let block_number: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|row| row.block_number),
    ));
    let block_hash: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|row| to_hex_hash(row.block_hash)),
    ));
    let timestamp: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|row| row.timestamp),
    ));
    let tx_hash: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|row| to_hex_hash(row.tx_hash)),
    ));
    let tx_index: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|row| row.tx_index as u64),
    ));
    let log_index: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|row| row.log_index as u64),
    ));
    let address: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter()
            .map(|row| to_hex_address(row.address.as_slice())),
    ));
    let topic0: ArrayRef = Arc::new(StringArray::from(
        rows.iter()
            .map(|row| row.topic0.map(to_hex_hash))
            .collect::<Vec<_>>(),
    ));
    let topic1: ArrayRef = Arc::new(StringArray::from(
        rows.iter()
            .map(|row| row.topic1.map(to_hex_hash))
            .collect::<Vec<_>>(),
    ));
    let topic2: ArrayRef = Arc::new(StringArray::from(
        rows.iter()
            .map(|row| row.topic2.map(to_hex_hash))
            .collect::<Vec<_>>(),
    ));
    let topic3: ArrayRef = Arc::new(StringArray::from(
        rows.iter()
            .map(|row| row.topic3.map(to_hex_hash))
            .collect::<Vec<_>>(),
    ));

    let mut topics_builder = ListBuilder::new(StringBuilder::new());
    for row in rows {
        for topic in [row.topic0, row.topic1, row.topic2, row.topic3]
            .into_iter()
            .flatten()
        {
            topics_builder.values().append_value(to_hex_hash(topic));
        }
        topics_builder.append(true);
    }
    let topics: ArrayRef = Arc::new(topics_builder.finish());

    let data: ArrayRef = Arc::new(StringArray::from_iter_values(
        rows.iter().map(|row| to_hex_bytes(&row.data)),
    ));
    let data_len: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|row| row.data_len as u64),
    ));
    let source: ArrayRef = Arc::new(UInt64Array::from_iter_values(
        rows.iter().map(|row| row.source as u64),
    ));

    Ok(RecordBatch::try_new(
        schema,
        vec![
            block_number,
            block_hash,
            timestamp,
            tx_hash,
            tx_index,
            log_index,
            address,
            topic0,
            topic1,
            topic2,
            topic3,
            topics,
            data,
            data_len,
            source,
        ],
    )?)
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

fn compile_query(query: &Query, head_block: u64) -> String {
    let mut sql = format!(
        "SELECT {} FROM logs",
        query
            .select
            .iter()
            .map(|item| compile_select_item(item, head_block))
            .collect::<Vec<_>>()
            .join(", ")
    );

    if let Some(where_clause) = &query.where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(&compile_expr(where_clause, head_block));
    }

    if !query.group_by.is_empty() {
        sql.push_str(" GROUP BY ");
        sql.push_str(&query.group_by.join(", "));
    }

    if !query.order_by.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(
            &query
                .order_by
                .iter()
                .map(|item| {
                    let mut expr = compile_expr(&item.expr, head_block);
                    if item.desc {
                        expr.push_str(" DESC");
                    }
                    expr
                })
                .collect::<Vec<_>>()
                .join(", "),
        );
    }

    if let Some(limit) = query.limit {
        sql.push_str(" LIMIT ");
        sql.push_str(&limit.to_string());
    }

    sql
}

fn compile_select_item(item: &SelectItem, head_block: u64) -> String {
    match item {
        SelectItem::Star => "*".to_owned(),
        SelectItem::Column { name, alias } => alias
            .as_ref()
            .map(|alias| format!("{name} AS {alias}"))
            .unwrap_or_else(|| name.clone()),
        SelectItem::Function { name, args, alias } => {
            let args = args
                .iter()
                .map(|expr| compile_expr(expr, head_block))
                .collect::<Vec<_>>()
                .join(", ");
            alias
                .as_ref()
                .map(|alias| format!("{name}({args}) AS {alias}"))
                .unwrap_or_else(|| format!("{name}({args})"))
        }
    }
}

fn compile_expr(expr: &Expr, head_block: u64) -> String {
    match expr {
        Expr::Column(name) => name.clone(),
        Expr::Number(value) => value.to_string(),
        Expr::StringLit(value) => quote_sql_string(&normalize_hex_literal(value)),
        Expr::EventHash(hash) => quote_sql_string(&to_hex_hash(*hash)),
        Expr::AddressPadded(hash) => quote_sql_string(&to_hex_hash(*hash)),
        Expr::Latest => head_block.to_string(),
        Expr::BinaryOp { left, op, right } => format!(
            "({} {} {})",
            compile_expr(left, head_block),
            compile_bin_op(*op),
            compile_expr(right, head_block)
        ),
        Expr::Between { expr, low, high } => format!(
            "({} BETWEEN {} AND {})",
            compile_expr(expr, head_block),
            compile_expr(low, head_block),
            compile_expr(high, head_block)
        ),
        Expr::InList {
            expr,
            list,
            negated,
        } => format!(
            "({} {}IN ({}))",
            compile_expr(expr, head_block),
            if *negated { "NOT " } else { "" },
            list.iter()
                .map(|expr| compile_expr(expr, head_block))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Not(expr) => format!("(NOT {})", compile_expr(expr, head_block)),
        Expr::Function { name, args } => format!(
            "{name}({})",
            args.iter()
                .map(|expr| compile_expr(expr, head_block))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Expr::Star => "*".to_owned(),
    }
}

fn compile_bin_op(op: BinOp) -> &'static str {
    match op {
        BinOp::Eq => "=",
        BinOp::Ne => "!=",
        BinOp::Lt => "<",
        BinOp::Gt => ">",
        BinOp::Le => "<=",
        BinOp::Ge => ">=",
        BinOp::And => "AND",
        BinOp::Or => "OR",
        BinOp::Add => "+",
        BinOp::Sub => "-",
    }
}

fn quote_sql_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn normalize_hex_literal(value: &str) -> String {
    if value.starts_with("0x") && value[2..].chars().all(|ch| ch.is_ascii_hexdigit()) {
        value.to_ascii_lowercase()
    } else {
        value.to_owned()
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
    use logex_types::Source;
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
        let tmp = TempDir::new().expect("tempdir");
        let config = PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
        };
        let mut mgr = PartitionManager::open(config).expect("storage opens");
        mgr.write_batch(&make_test_rows()).expect("rows persist");
        IndexBuilder::build_all_indexes(&mgr.hot_partition().meta.path).expect("indexes build");
        (tmp, mgr)
    }

    #[tokio::test]
    async fn executes_aggregate_sql() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql(
            "SELECT COUNT(*) AS total FROM logs WHERE block_number <= latest",
            &storage,
            storage.head_block(),
        )
        .await
        .expect("query executes");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["total"], 2);
    }

    #[tokio::test]
    async fn preserves_order_alias_and_limit() {
        let (_tmp, storage) = setup_storage();
        let result = execute_sql(
            "SELECT block_number AS bn FROM logs ORDER BY block_number DESC LIMIT 1",
            &storage,
            storage.head_block(),
        )
        .await
        .expect("query executes");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0]["bn"], 200);
    }
}
