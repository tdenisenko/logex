use std::path::Path;

use roaring::RoaringBitmap;

use logex_index::{BTreeIndexReader, CompositeQuery};
use logex_storage::{ColumnReader, PartitionManager};
use logex_types::LogRow;

use crate::ast::{BinOp, Expr, OrderByItem, Query, SelectItem};
use crate::planner::{self, QueryPlan};

/// Query execution result.
#[derive(Debug)]
pub struct QueryResult {
    pub rows: Vec<LogRow>,
    pub total_scanned: u64,
}

/// Execute a parsed query against the storage engine.
pub fn execute(
    query: &Query,
    storage: &PartitionManager,
    head_block: Option<u64>,
) -> std::io::Result<QueryResult> {
    let head_block = head_block.unwrap_or_else(|| storage.head_block().unwrap_or(0));
    let mut plan = match &query.where_clause {
        Some(expr) => planner::plan_where(expr),
        None => QueryPlan::default(),
    };

    // Resolve `latest` if needed
    if plan.uses_latest {
        planner::resolve_latest(&mut plan, head_block);
    }

    let mut all_rows = Vec::new();
    let mut total_scanned = 0u64;

    // Scan sealed partitions
    for partition in storage.sealed_partitions() {
        if !partition_matches_range(&partition.meta, &plan) {
            continue;
        }
        let rows = scan_partition(&partition.meta.path, &plan)?;
        total_scanned += rows.len() as u64;
        all_rows.extend(rows);
    }

    // Scan hot partition
    let hot = storage.hot_partition();
    if hot.meta.row_count > 0 && partition_matches_range(&hot.meta, &plan) {
        let rows = scan_partition(&hot.meta.path, &plan)?;
        total_scanned += rows.len() as u64;
        all_rows.extend(rows);
    }

    // Apply the full WHERE clause after index lookups. This keeps query results
    // correct even when a hot partition has not been re-indexed yet.
    if let Some(expr) = &query.where_clause {
        all_rows.retain(|row| eval_filter(expr, row, head_block));
    }

    // Apply ORDER BY
    if !query.order_by.is_empty() {
        apply_order_by(&mut all_rows, &query.order_by);
    }

    // Apply LIMIT
    if let Some(limit) = query.limit {
        all_rows.truncate(limit as usize);
    }

    Ok(QueryResult {
        rows: all_rows,
        total_scanned,
    })
}

/// Check if a partition's block range overlaps with the plan's range.
fn partition_matches_range(meta: &logex_types::PartitionMeta, plan: &QueryPlan) -> bool {
    if let Some(from) = plan.block_from
        && meta.max_block < from
    {
        return false;
    }
    if let Some(to) = plan.block_to
        && meta.min_block >= to
    {
        return false;
    }
    true
}

/// Scan a single partition using available indexes, then read matching rows.
fn scan_partition(dir: &Path, plan: &QueryPlan) -> std::io::Result<Vec<LogRow>> {
    let row_count = ColumnReader::read_row_count(dir)?;
    if row_count == 0 {
        return Ok(Vec::new());
    }

    // Try to use indexes to narrow down rows
    let bitmap = build_index_bitmap(dir, plan, row_count)?;

    let row_ids: Vec<u32> = bitmap.iter().collect();
    if row_ids.is_empty() {
        return Ok(Vec::new());
    }

    // Read matching rows, filtering out non-canonical ones
    let canonical = ColumnReader::read_canonical(dir)?;
    let canonical_ids: Vec<u32> = row_ids
        .into_iter()
        .filter(|&id| canonical.is_present(id as u64))
        .collect();

    if canonical_ids.is_empty() {
        return Ok(Vec::new());
    }

    ColumnReader::read_log_rows(dir, Some(&canonical_ids))
}

/// Build a bitmap of candidate row IDs using available indexes.
fn build_index_bitmap(
    dir: &Path,
    plan: &QueryPlan,
    row_count: u64,
) -> std::io::Result<RoaringBitmap> {
    let index_dir = dir.join("indexes");
    let mut result: Option<RoaringBitmap> = None;

    // Try composite index first: (address, topic0)
    if let (Some(addr_bytes), Some(topic0)) = (&plan.address, &plan.topic0) {
        let composite_path = index_dir.join("address_topic0.bptree");
        if composite_path.exists() && addr_bytes.len() == 20 {
            let reader = BTreeIndexReader::open(&composite_path)?;
            let addr: &[u8; 20] = addr_bytes.as_slice().try_into().map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid address length")
            })?;
            let bm = CompositeQuery::get_address_topic0(
                &reader,
                addr,
                topic0.as_slice().try_into().map_err(|_| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid topic0 length")
                })?,
            );
            if let Some(bm) = bm {
                result = Some(intersect_optional(result, bm));
            } else {
                return Ok(RoaringBitmap::new());
            }

            // Skip individual address/topic0 lookups since composite covers both
            return apply_block_range(dir, &index_dir, plan, result, row_count);
        }
    }

    // Individual address index
    if let Some(addr_bytes) = &plan.address {
        let addr_path = index_dir.join("address.bptree");
        if addr_path.exists() {
            let reader = BTreeIndexReader::open(&addr_path)?;
            if let Some(bm) = reader.get(addr_bytes) {
                result = Some(intersect_optional(result, bm.clone()));
            } else {
                return Ok(RoaringBitmap::new());
            }
        }
    }

    // Individual topic0 index
    if let Some(topic0) = &plan.topic0 {
        let topic_path = index_dir.join("topic0.bptree");
        if topic_path.exists() {
            let reader = BTreeIndexReader::open(&topic_path)?;
            if let Some(bm) = reader.get(topic0.as_slice()) {
                result = Some(intersect_optional(result, bm.clone()));
            } else {
                return Ok(RoaringBitmap::new());
            }
        }
    }

    apply_block_range(dir, &index_dir, plan, result, row_count)
}

/// Apply block_number range filter using the block_number index.
fn apply_block_range(
    _dir: &Path,
    index_dir: &Path,
    plan: &QueryPlan,
    current: Option<RoaringBitmap>,
    row_count: u64,
) -> std::io::Result<RoaringBitmap> {
    if plan.block_from.is_some() || plan.block_to.is_some() {
        let block_path = index_dir.join("block_number.bptree");
        if block_path.exists() {
            let reader = BTreeIndexReader::open(&block_path)?;
            let from = plan.block_from.unwrap_or(0);
            let to = plan.block_to.unwrap_or(u64::MAX);
            let range_bm = reader.range(&from.to_be_bytes(), &to.to_be_bytes());
            return Ok(intersect_optional(current, range_bm));
        }
    }

    // No block range filter or no index — return current or full scan
    Ok(current.unwrap_or_else(|| (0..row_count as u32).collect()))
}

fn intersect_optional(existing: Option<RoaringBitmap>, new: RoaringBitmap) -> RoaringBitmap {
    match existing {
        Some(e) => e & new,
        None => new,
    }
}

/// Apply ORDER BY to results (in-place sort).
fn apply_order_by(rows: &mut [LogRow], order_by: &[OrderByItem]) {
    if order_by.is_empty() {
        return;
    }

    rows.sort_by(|a, b| {
        for item in order_by {
            let cmp = compare_by_expr(a, b, &item.expr);
            let cmp = if item.desc { cmp.reverse() } else { cmp };
            if cmp != std::cmp::Ordering::Equal {
                return cmp;
            }
        }
        std::cmp::Ordering::Equal
    });
}

fn compare_by_expr(a: &LogRow, b: &LogRow, expr: &Expr) -> std::cmp::Ordering {
    match expr {
        Expr::Column(name) => match name.as_str() {
            "block_number" => a.block_number.cmp(&b.block_number),
            "timestamp" => a.timestamp.cmp(&b.timestamp),
            "tx_index" => a.tx_index.cmp(&b.tx_index),
            "log_index" => a.log_index.cmp(&b.log_index),
            "address" => a.address.cmp(&b.address),
            _ => std::cmp::Ordering::Equal,
        },
        _ => std::cmp::Ordering::Equal,
    }
}

/// Evaluate a residual filter against a single row.
fn eval_filter(expr: &Expr, row: &LogRow, head_block: u64) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinOp::And,
            right,
        } => eval_filter(left, row, head_block) && eval_filter(right, row, head_block),
        Expr::BinaryOp {
            left,
            op: BinOp::Or,
            right,
        } => eval_filter(left, row, head_block) || eval_filter(right, row, head_block),
        Expr::BinaryOp {
            left,
            op: BinOp::Eq,
            right,
        } => compare_values(
            eval_value(left, row, head_block),
            eval_value(right, row, head_block),
            BinOp::Eq,
        ),
        Expr::BinaryOp {
            left,
            op: BinOp::Ne,
            right,
        } => compare_values(
            eval_value(left, row, head_block),
            eval_value(right, row, head_block),
            BinOp::Ne,
        ),
        Expr::BinaryOp {
            left,
            op: BinOp::Lt,
            right,
        } => compare_values(
            eval_value(left, row, head_block),
            eval_value(right, row, head_block),
            BinOp::Lt,
        ),
        Expr::BinaryOp {
            left,
            op: BinOp::Gt,
            right,
        } => compare_values(
            eval_value(left, row, head_block),
            eval_value(right, row, head_block),
            BinOp::Gt,
        ),
        Expr::BinaryOp {
            left,
            op: BinOp::Le,
            right,
        } => compare_values(
            eval_value(left, row, head_block),
            eval_value(right, row, head_block),
            BinOp::Le,
        ),
        Expr::BinaryOp {
            left,
            op: BinOp::Ge,
            right,
        } => compare_values(
            eval_value(left, row, head_block),
            eval_value(right, row, head_block),
            BinOp::Ge,
        ),
        Expr::Between { expr, low, high } => {
            let value = eval_value(expr, row, head_block);
            let low = eval_value(low, row, head_block);
            let high = eval_value(high, row, head_block);
            compare_values(value.clone(), low, BinOp::Ge) && compare_values(value, high, BinOp::Le)
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let value = eval_value(expr, row, head_block);
            let contains = list.iter().any(|candidate| {
                compare_values(
                    value.clone(),
                    eval_value(candidate, row, head_block),
                    BinOp::Eq,
                )
            });
            if *negated { !contains } else { contains }
        }
        Expr::Not(inner) => !eval_filter(inner, row, head_block),
        _ => eval_value(expr, row, head_block)
            .and_then(|value| match value {
                QueryValue::Bool(result) => Some(result),
                _ => None,
            })
            .unwrap_or(false),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum QueryValue {
    Number(i128),
    Bytes(Vec<u8>),
    String(String),
    Bool(bool),
}

fn eval_value(expr: &Expr, row: &LogRow, head_block: u64) -> Option<QueryValue> {
    match expr {
        Expr::Column(name) => column_value(name, row),
        Expr::Number(value) => Some(QueryValue::Number(*value as i128)),
        Expr::StringLit(value) => Some(string_literal_value(value)),
        Expr::EventHash(hash) | Expr::AddressPadded(hash) => {
            Some(QueryValue::Bytes(hash.as_slice().to_vec()))
        }
        Expr::Latest => Some(QueryValue::Number(head_block as i128)),
        Expr::BinaryOp {
            left,
            op: BinOp::Add,
            right,
        } => {
            let left = eval_numeric(left, row, head_block)?;
            let right = eval_numeric(right, row, head_block)?;
            Some(QueryValue::Number(left + right))
        }
        Expr::BinaryOp {
            left,
            op: BinOp::Sub,
            right,
        } => {
            let left = eval_numeric(left, row, head_block)?;
            let right = eval_numeric(right, row, head_block)?;
            Some(QueryValue::Number(left - right))
        }
        Expr::BinaryOp { .. } | Expr::Between { .. } | Expr::InList { .. } | Expr::Not(_) => {
            Some(QueryValue::Bool(eval_filter(expr, row, head_block)))
        }
        Expr::Function { .. } | Expr::Star => None,
    }
}

fn eval_numeric(expr: &Expr, row: &LogRow, head_block: u64) -> Option<i128> {
    match eval_value(expr, row, head_block)? {
        QueryValue::Number(value) => Some(value),
        _ => None,
    }
}

fn column_value(name: &str, row: &LogRow) -> Option<QueryValue> {
    match name {
        "block_number" => Some(QueryValue::Number(row.block_number as i128)),
        "block_hash" => Some(QueryValue::Bytes(row.block_hash.as_slice().to_vec())),
        "timestamp" => Some(QueryValue::Number(row.timestamp as i128)),
        "tx_hash" => Some(QueryValue::Bytes(row.tx_hash.as_slice().to_vec())),
        "tx_index" => Some(QueryValue::Number(row.tx_index as i128)),
        "log_index" => Some(QueryValue::Number(row.log_index as i128)),
        "address" => Some(QueryValue::Bytes(row.address.as_slice().to_vec())),
        "topic0" => row
            .topic0
            .map(|value| QueryValue::Bytes(value.as_slice().to_vec())),
        "topic1" => row
            .topic1
            .map(|value| QueryValue::Bytes(value.as_slice().to_vec())),
        "topic2" => row
            .topic2
            .map(|value| QueryValue::Bytes(value.as_slice().to_vec())),
        "topic3" => row
            .topic3
            .map(|value| QueryValue::Bytes(value.as_slice().to_vec())),
        "data" => Some(QueryValue::Bytes(row.data.to_vec())),
        "data_len" => Some(QueryValue::Number(row.data_len as i128)),
        "source" => Some(QueryValue::Number(row.source as u8 as i128)),
        _ => None,
    }
}

fn string_literal_value(value: &str) -> QueryValue {
    let stripped = value.strip_prefix("0x").unwrap_or(value);
    if stripped.len().is_multiple_of(2)
        && !stripped.is_empty()
        && stripped.chars().all(|ch| ch.is_ascii_hexdigit())
        && let Ok(bytes) = hex::decode(stripped)
    {
        return QueryValue::Bytes(bytes);
    }

    QueryValue::String(value.to_owned())
}

fn compare_values(left: Option<QueryValue>, right: Option<QueryValue>, op: BinOp) -> bool {
    let Some(left) = left else {
        return false;
    };
    let Some(right) = right else {
        return false;
    };

    match (left, right) {
        (QueryValue::Number(left), QueryValue::Number(right)) => {
            compare_ordering(left.cmp(&right), op)
        }
        (QueryValue::Bytes(left), QueryValue::Bytes(right)) => {
            compare_ordering(left.cmp(&right), op)
        }
        (QueryValue::String(left), QueryValue::String(right)) => {
            compare_ordering(left.cmp(&right), op)
        }
        (QueryValue::Bool(left), QueryValue::Bool(right)) => compare_ordering(left.cmp(&right), op),
        _ => false,
    }
}

fn compare_ordering(ordering: std::cmp::Ordering, op: BinOp) -> bool {
    match op {
        BinOp::Eq => ordering == std::cmp::Ordering::Equal,
        BinOp::Ne => ordering != std::cmp::Ordering::Equal,
        BinOp::Lt => ordering == std::cmp::Ordering::Less,
        BinOp::Gt => ordering == std::cmp::Ordering::Greater,
        BinOp::Le => ordering != std::cmp::Ordering::Greater,
        BinOp::Ge => ordering != std::cmp::Ordering::Less,
        _ => false,
    }
}

/// Check if a query is a simple SELECT * (no aggregation, no decode).
pub fn is_simple_select(query: &Query) -> bool {
    query
        .select
        .iter()
        .all(|item| matches!(item, SelectItem::Star | SelectItem::Column { .. }))
        && query.group_by.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;
    use alloy_primitives::{Address, B256, bytes};
    use logex_index::IndexBuilder;
    use logex_storage::PartitionManagerConfig;
    use logex_types::Source;
    use tempfile::TempDir;

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
                topic1: None,
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
                topic1: Some(B256::repeat_byte(0x99)),
                topic2: None,
                topic3: None,
                data: bytes!("cafe"),
                data_len: 2,
                source: Source::Receipt,
            },
            LogRow {
                block_number: 300,
                block_hash: B256::repeat_byte(0x03),
                timestamp: 1_700_002_400,
                tx_hash: B256::repeat_byte(0x33),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xAA),
                topic0: Some(B256::repeat_byte(0xDD)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!("beef"),
                data_len: 2,
                source: Source::Receipt,
            },
        ]
    }

    fn setup_storage() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let config = PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
        };
        let mut mgr = PartitionManager::open(config).unwrap();
        mgr.write_batch(&make_test_rows()).unwrap();

        // Build indexes on the hot partition
        IndexBuilder::build_all_indexes(&mgr.hot_partition().meta.path).unwrap();

        (tmp, mgr)
    }

    fn setup_storage_without_indexes() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let config = PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
        };
        let mut mgr = PartitionManager::open(config).unwrap();
        mgr.write_batch(&make_test_rows()).unwrap();
        (tmp, mgr)
    }

    #[test]
    fn test_select_star() {
        let (_tmp, storage) = setup_storage();
        let q = parse("SELECT * FROM logs").unwrap();
        let result = execute(&q, &storage, None).unwrap();
        assert_eq!(result.rows.len(), 3);
    }

    #[test]
    fn test_filter_by_address() {
        let (_tmp, storage) = setup_storage();
        let addr_hex = hex::encode(Address::repeat_byte(0xAA).as_slice());
        let q = parse(&format!(
            "SELECT * FROM logs WHERE address = '0x{addr_hex}'"
        ))
        .unwrap();
        let result = execute(&q, &storage, None).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert!(
            result
                .rows
                .iter()
                .all(|r| r.address == Address::repeat_byte(0xAA))
        );
    }

    #[test]
    fn test_filter_by_block_range() {
        let (_tmp, storage) = setup_storage();
        let q = parse("SELECT * FROM logs WHERE block_number BETWEEN 100 AND 200").unwrap();
        let result = execute(&q, &storage, None).unwrap();
        assert_eq!(result.rows.len(), 2); // blocks 100 and 200
    }

    #[test]
    fn test_order_by_desc_limit() {
        let (_tmp, storage) = setup_storage();
        let q = parse("SELECT * FROM logs ORDER BY block_number DESC LIMIT 2").unwrap();
        let result = execute(&q, &storage, None).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert_eq!(result.rows[0].block_number, 300);
        assert_eq!(result.rows[1].block_number, 200);
    }

    /// Reproduce the block range query bug with large (real Ethereum) block numbers.
    fn make_large_block_rows() -> Vec<LogRow> {
        (0..100)
            .map(|i| LogRow {
                block_number: 22_100_000 + i,
                block_hash: B256::repeat_byte((i % 256) as u8),
                timestamp: 1_700_000_000 + i * 12,
                tx_hash: B256::repeat_byte(((i + 1) % 256) as u8),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xAA),
                topic0: Some(B256::repeat_byte(0xDD)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: bytes!(""),
                data_len: 0,
                source: Source::Receipt,
            })
            .collect()
    }

    fn setup_large_block_storage() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let config = PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
        };
        let mut mgr = PartitionManager::open(config).unwrap();
        mgr.write_batch(&make_large_block_rows()).unwrap();
        IndexBuilder::build_all_indexes(&mgr.hot_partition().meta.path).unwrap();
        (tmp, mgr)
    }

    #[test]
    fn test_block_range_large_numbers_ge() {
        let (_tmp, storage) = setup_large_block_storage();
        let q = parse("SELECT * FROM logs WHERE block_number >= 22100050").unwrap();
        let result = execute(&q, &storage, None).unwrap();
        assert_eq!(
            result.rows.len(),
            50,
            "block_number >= 22100050 should return 50 rows"
        );
    }

    #[test]
    fn test_block_range_large_numbers_between() {
        let (_tmp, storage) = setup_large_block_storage();
        let q =
            parse("SELECT * FROM logs WHERE block_number BETWEEN 22100000 AND 22100009").unwrap();
        let result = execute(&q, &storage, None).unwrap();
        assert_eq!(
            result.rows.len(),
            10,
            "BETWEEN 22100000 AND 22100009 should return 10 rows"
        );
    }

    #[test]
    fn test_block_range_large_numbers_eq() {
        let (_tmp, storage) = setup_large_block_storage();
        let q = parse("SELECT * FROM logs WHERE block_number = 22100050").unwrap();
        let result = execute(&q, &storage, None).unwrap();
        assert_eq!(
            result.rows.len(),
            1,
            "block_number = 22100050 should return 1 row"
        );
    }

    #[test]
    fn test_latest_resolution() {
        let (_tmp, storage) = setup_storage();
        let q = parse("SELECT * FROM logs WHERE block_number >= latest - 100").unwrap();
        // Head block is 300, so latest - 100 = 200
        let result = execute(&q, &storage, Some(300)).unwrap();
        assert_eq!(result.rows.len(), 2); // blocks 200, 300
    }

    #[test]
    fn test_filter_by_topic1_without_indexes() {
        let (_tmp, storage) = setup_storage_without_indexes();
        let topic = hex::encode(B256::repeat_byte(0x99));
        let q = parse(&format!("SELECT * FROM logs WHERE topic1 = '0x{topic}'")).unwrap();
        let result = execute(&q, &storage, None).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].block_number, 200);
    }

    #[test]
    fn test_filter_by_block_hash_without_indexes() {
        let (_tmp, storage) = setup_storage_without_indexes();
        let hash = hex::encode(B256::repeat_byte(0x03));
        let q = parse(&format!("SELECT * FROM logs WHERE block_hash = '0x{hash}'")).unwrap();
        let result = execute(&q, &storage, None).unwrap();

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0].block_number, 300);
    }
}
