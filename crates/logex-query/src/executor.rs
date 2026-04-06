use std::path::Path;

use roaring::RoaringBitmap;

use logex_index::{BTreeIndexReader, CompositeQuery};
use logex_storage::{ColumnReader, PartitionManager};
use logex_types::LogRow;

use crate::ast::{Expr, OrderByItem, Query, SelectItem};
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
    let mut plan = match &query.where_clause {
        Some(expr) => planner::plan_where(expr),
        None => QueryPlan::default(),
    };

    // Resolve `latest` if needed
    if plan.uses_latest {
        let head = head_block.unwrap_or_else(|| storage.head_block().unwrap_or(0));
        planner::resolve_latest(&mut plan, head);
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

    // Apply residual filters
    if !plan.residual_filters.is_empty() {
        all_rows.retain(|row| plan.residual_filters.iter().all(|f| eval_filter(f, row)));
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
fn eval_filter(expr: &Expr, row: &LogRow) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: crate::ast::BinOp::Eq,
            right,
        } => {
            if let Expr::Column(name) = left.as_ref() {
                return match_eq(name, right, row);
            }
            true
        }
        Expr::BinaryOp {
            left,
            op: crate::ast::BinOp::Ne,
            right,
        } => {
            if let Expr::Column(name) = left.as_ref() {
                return !match_eq(name, right, row);
            }
            true
        }
        Expr::BinaryOp {
            left,
            op: crate::ast::BinOp::And,
            right,
        } => eval_filter(left, row) && eval_filter(right, row),
        Expr::BinaryOp {
            left,
            op: crate::ast::BinOp::Or,
            right,
        } => eval_filter(left, row) || eval_filter(right, row),
        Expr::Not(inner) => !eval_filter(inner, row),
        _ => true, // Unknown filters pass through
    }
}

fn match_eq(column: &str, value: &Expr, row: &LogRow) -> bool {
    match column {
        "address" => match value {
            Expr::StringLit(s) => {
                let hex = s.strip_prefix("0x").unwrap_or(s);
                hex::decode(hex)
                    .ok()
                    .is_some_and(|bytes| bytes == row.address.as_slice())
            }
            _ => false,
        },
        "source" => match value {
            Expr::Number(n) => row.source as u8 == *n as u8,
            _ => false,
        },
        _ => true,
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
                topic1: None,
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
}
