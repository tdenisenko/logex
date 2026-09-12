use alloy_primitives::{Address, B256, Bytes};
use logex_index::IndexBuilder;
use logex_query::{SqlQueryPage, execute_log_filter, execute_sql_page};
use logex_storage::native::{LogOrder, NativeLogFilter};
use logex_storage::{PartitionManager, PartitionManagerConfig};
use logex_types::{LogRow, Source};
use serde_json::{Value, json};
use tempfile::TempDir;

fn row(block: u64) -> LogRow {
    LogRow {
        block_number: block,
        block_hash: alloy_primitives::keccak256(block.to_le_bytes()),
        timestamp: block * 12,
        tx_hash: B256::repeat_byte(1),
        tx_index: 0,
        log_index: 0,
        address: Address::repeat_byte(0xaa),
        topic0: None,
        topic1: None,
        topic2: None,
        topic3: None,
        data: Bytes::new(),
        data_len: 0,
        source: Source::Receipt,
    }
}

fn storage(batches: &[&[u64]], target: u64) -> (TempDir, PartitionManager) {
    let tmp = TempDir::new().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: tmp.path().to_owned(),
        partition_target_rows: target,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    for batch in batches {
        storage
            .write_batch(&batch.iter().copied().map(row).collect::<Vec<_>>())
            .unwrap();
    }
    storage.checkpoint().unwrap();
    (tmp, storage)
}

#[test]
fn native_pagination_sorts_reverse_ingestion_before_limiting() {
    let (_tmp, storage) = storage(&[&[30, 40], &[10, 20]], 100);
    let mut filter = NativeLogFilter::new();
    filter.limit = Some(2);
    let actual = execute_log_filter(&storage, &filter).unwrap();
    assert_eq!(actual, vec![row(10), row(20)]);
}

#[test]
fn native_pagination_merges_overlapping_segment_ranges() {
    let (_tmp, storage) = storage(&[&[10, 40, 70], &[20, 50, 80], &[30, 60, 90]], 3);
    assert_eq!(storage.sealed_count(), 3);
    for (order, expected) in [
        (LogOrder::Ascending, vec![row(10), row(20)]),
        (LogOrder::Descending, vec![row(90), row(80)]),
    ] {
        let mut filter = NativeLogFilter::new();
        filter.order = order;
        filter.limit = Some(2);
        assert_eq!(execute_log_filter(&storage, &filter).unwrap(), expected);
    }
}

#[tokio::test]
async fn sql_pagination_merges_overlapping_segment_ranges() {
    let (_tmp, storage) = storage(&[&[10, 40, 70], &[20, 50, 80], &[30, 60, 90]], 3);
    for (direction, expected) in [("ASC", vec![10, 20]), ("DESC", vec![90, 80])] {
        let sql = format!(
            "SELECT block_number FROM logs ORDER BY block_number {direction}, tx_index {direction}, log_index {direction} LIMIT 2"
        );
        let actual = execute_sql_page(
            &sql,
            &storage,
            storage.head_block(),
            SqlQueryPage::new(None, 0),
        )
        .await
        .unwrap();
        let expected: Vec<Value> = expected
            .into_iter()
            .map(|n| json!({"block_number": n}))
            .collect();
        assert_eq!(actual.rows, expected);
    }
}

#[tokio::test]
async fn sql_pagination_applies_api_page_after_sql_limit() {
    let (_tmp, storage) = storage(&[&[10, 20, 30, 40, 50, 60]], 100);
    let actual = execute_sql_page(
        "SELECT block_number FROM logs ORDER BY block_number ASC, tx_index ASC, log_index ASC LIMIT 3",
        &storage,
        storage.head_block(),
        SqlQueryPage::new(Some(2), 2),
    )
    .await
    .unwrap();
    assert_eq!(actual.rows, vec![json!({"block_number": 30})]);
}

fn projected(rows: &[LogRow]) -> Vec<Value> {
    rows.iter()
        .map(|r| json!({"block_number": r.block_number, "tx_index": r.tx_index, "log_index": r.log_index}))
        .collect()
}

fn reference_page(rows: &[LogRow], limit: Option<usize>, offset: usize) -> &[LogRow] {
    let start = offset.min(rows.len());
    let end = limit.map_or(rows.len(), |limit| {
        start.saturating_add(limit).min(rows.len())
    });
    &rows[start..end]
}

#[tokio::test]
async fn pagination_matches_reference_across_storage_layouts_and_equal_block_bounds() {
    // Each segment covers the same blocks, with earlier transaction/log keys
    // arriving in later segments. Bounds equal to the last result cannot prune.
    let input: Vec<_> = [8, 2, 5, 7, 1, 4, 6, 0, 3]
        .into_iter()
        .map(|i| {
            let mut row = row(1000 + u64::from(i / 3));
            row.tx_index = i % 3 / 2;
            row.log_index = i % 3;
            row.address = Address::repeat_byte(if i % 4 == 0 { 0xbb } else { 0xaa });
            row
        })
        .collect();
    for historical in [false, true] {
        for compact in [false, true] {
            for indexed in [false, true] {
                let (_tmp, mut storage) = storage(&[], 3);
                for batch in input.chunks(3) {
                    let mut batch = batch.to_vec();
                    batch.sort_by_key(|r| (r.block_number, r.tx_index, r.log_index));
                    if historical {
                        storage.write_historical_batch(&batch).unwrap();
                    } else {
                        storage.write_batch(&batch).unwrap();
                    }
                }
                storage.checkpoint().unwrap();
                assert_eq!(storage.sealed_count(), 3);
                if compact {
                    let count = storage.compact_eligible_segments().unwrap();
                    if !historical {
                        assert!(count > 0, "exercise rewritten raw segments");
                    }
                }
                if indexed {
                    for partition in storage.sealed_partitions() {
                        IndexBuilder::build_all_indexes(&partition.meta.path).unwrap();
                    }
                }
                for (order, direction) in
                    [(LogOrder::Ascending, "ASC"), (LogOrder::Descending, "DESC")]
                {
                    let mut reference: Vec<_> = input
                        .iter()
                        .filter(|r| r.address == Address::repeat_byte(0xaa))
                        .cloned()
                        .collect();
                    reference.sort_by_key(|r| (r.block_number, r.tx_index, r.log_index));
                    if order == LogOrder::Descending {
                        reference.reverse();
                    }
                    let sql = format!(
                        "SELECT block_number, tx_index, log_index FROM logs WHERE address = '{}' ORDER BY block_number {direction}, tx_index {direction}, log_index {direction}",
                        Address::repeat_byte(0xaa)
                    );
                    for limit in [None, Some(0), Some(1), Some(2), Some(5), Some(100)] {
                        for offset in [0, 1, 4, 9, usize::MAX] {
                            let mut filter = NativeLogFilter::new()
                                .with_addresses(vec![Address::repeat_byte(0xaa)]);
                            filter.order = order;
                            filter.limit = limit;
                            filter.offset = offset;
                            let expected = reference_page(&reference, limit, offset);
                            assert_eq!(
                                execute_log_filter(&storage, &filter).unwrap(),
                                expected,
                                "historical={historical} compact={compact} indexed={indexed} order={order:?} limit={limit:?} offset={offset}"
                            );
                            let actual = execute_sql_page(
                                &sql,
                                &storage,
                                storage.head_block(),
                                SqlQueryPage::new(limit, offset),
                            )
                            .await
                            .unwrap();
                            assert_eq!(
                                actual.rows,
                                projected(expected),
                                "historical={historical} compact={compact} indexed={indexed} order={order:?} limit={limit:?} offset={offset}"
                            );
                        }
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn sql_limit_and_api_page_match_reference_and_datafusion() {
    let blocks = [10, 20, 30, 40, 50, 60];
    let (_tmp, storage) = storage(&[&blocks], 100);
    for direction in ["ASC", "DESC"] {
        let mut reference: Vec<_> = blocks.into_iter().map(row).collect();
        if direction == "DESC" {
            reference.reverse();
        }
        for sql_limit in [None, Some(0), Some(1), Some(3), Some(20)] {
            for page_limit in [None, Some(0), Some(1), Some(2), Some(20)] {
                for offset in [0, 1, 2, 3, 6, usize::MAX] {
                    let clause = sql_limit.map_or_else(String::new, |n| format!(" LIMIT {n}"));
                    let sql = format!(
                        "SELECT block_number, tx_index, log_index FROM logs ORDER BY block_number {direction}, tx_index {direction}, log_index {direction}{clause}"
                    );
                    let expected = projected(reference_page(
                        reference_page(&reference, sql_limit, 0),
                        page_limit,
                        offset,
                    ));
                    let page = SqlQueryPage::new(page_limit, offset);
                    let actual = execute_sql_page(&sql, &storage, storage.head_block(), page)
                        .await
                        .unwrap();
                    assert_eq!(actual.rows, expected, "{sql}, {page:?}");
                    if offset != usize::MAX {
                        // Equivalent expression deliberately bypasses the native
                        // ORDER BY recognizer and exercises DataFusion execution.
                        let fallback =
                            sql.replace("ORDER BY block_number ", "ORDER BY block_number + 0 ");
                        let actual =
                            execute_sql_page(&fallback, &storage, storage.head_block(), page)
                                .await
                                .unwrap();
                        assert_eq!(actual.rows, expected, "{fallback}, {page:?}");
                    }
                }
            }
        }
    }
}

#[tokio::test]
async fn pagination_prunes_ranges_that_cannot_improve_the_page() {
    // More than the maximum SQL scan window (8 workers * 4 partitions).
    let blocks: Vec<_> = (1..=65).collect();
    let (_tmp, storage) = storage(&[&blocks], 1);
    let middle = storage
        .sealed_partitions()
        .iter()
        .find(|p| p.meta.min_block == 33)
        .unwrap();
    let path = middle.meta.path.join("segment.json");
    let original = std::fs::read(&path).unwrap();
    std::fs::write(&path, b"{").unwrap();
    for (order, direction, expected) in [
        (LogOrder::Ascending, "ASC", row(1)),
        (LogOrder::Descending, "DESC", row(65)),
    ] {
        let mut filter = NativeLogFilter::new();
        filter.order = order;
        filter.limit = Some(1);
        assert_eq!(
            execute_log_filter(&storage, &filter).unwrap(),
            vec![expected.clone()]
        );
        let sql = format!(
            "SELECT block_number, tx_index, log_index FROM logs ORDER BY block_number {direction}, tx_index {direction}, log_index {direction} LIMIT 1"
        );
        let actual = execute_sql_page(
            &sql,
            &storage,
            storage.head_block(),
            SqlQueryPage::default(),
        )
        .await
        .unwrap();
        assert_eq!(actual.rows, projected(&[expected]));
    }
    // Without LIMIT, that same unreadable range is required and must error.
    assert!(execute_log_filter(&storage, &NativeLogFilter::new()).is_err());
    assert!(
        execute_sql_page(
            "SELECT block_number FROM logs",
            &storage,
            storage.head_block(),
            SqlQueryPage::default()
        )
        .await
        .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"{");
    std::fs::write(&path, original).unwrap();
}

#[tokio::test]
async fn pagination_merges_equal_block_keys_across_sql_windows() {
    let (_tmp, mut storage) = storage(&[], 1);
    let mut reference = Vec::new();
    for index in (0..65).rev() {
        let mut row = row(100);
        row.tx_index = index / 2;
        row.log_index = index;
        storage.write_batch(&[row.clone()]).unwrap();
        reference.push(row);
    }
    storage.checkpoint().unwrap();
    reference.reverse();
    for (order, direction) in [(LogOrder::Ascending, "ASC"), (LogOrder::Descending, "DESC")] {
        let mut ordered = reference.clone();
        if order == LogOrder::Descending {
            ordered.reverse();
        }
        for offset in [0, 35] {
            let mut filter = NativeLogFilter::new();
            filter.order = order;
            filter.limit = Some(3);
            filter.offset = offset;
            let expected = reference_page(&ordered, Some(3), offset);
            assert_eq!(execute_log_filter(&storage, &filter).unwrap(), expected);
            let sql = format!(
                "SELECT block_number, tx_index, log_index FROM logs ORDER BY block_number {direction}, tx_index {direction}, log_index {direction}"
            );
            let actual = execute_sql_page(
                &sql,
                &storage,
                storage.head_block(),
                SqlQueryPage::new(Some(3), offset),
            )
            .await
            .unwrap();
            assert_eq!(actual.rows, projected(expected));
        }
    }
}

#[tokio::test]
async fn pagination_matches_reference_for_seeded_ingestion_permutations() {
    let reference: Vec<_> = (0..39)
        .map(|index| {
            let mut row = row(100 + index / 3);
            row.tx_index = (index % 3 / 2) as u32;
            row.log_index = (index % 3) as u32;
            row
        })
        .collect();
    for seed in 0..12u64 {
        let (_tmp, mut storage) = storage(&[], if seed % 2 == 0 { 7 } else { 100 });
        let mut input = reference.clone();
        let mut state = seed + 1;
        for i in (1..input.len()).rev() {
            // Deterministic Fisher–Yates fixtures, independent of query code.
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            input.swap(i, (state % (i as u64 + 1)) as usize);
        }
        for batch in input.chunks(3) {
            let mut batch = batch.to_vec();
            batch.sort_by_key(|r| (r.block_number, r.tx_index, r.log_index));
            if seed % 2 == 0 {
                storage.write_batch(&batch).unwrap();
            } else {
                storage.write_historical_batch(&batch).unwrap();
            }
        }
        storage.checkpoint().unwrap();
        for (order, direction) in [(LogOrder::Ascending, "ASC"), (LogOrder::Descending, "DESC")] {
            let mut ordered = reference.clone();
            if order == LogOrder::Descending {
                ordered.reverse();
            }
            for (limit, offset) in [(1, 0), (3, 5), (7, 31)] {
                let mut filter = NativeLogFilter::new();
                filter.order = order;
                filter.limit = Some(limit);
                filter.offset = offset;
                let expected = reference_page(&ordered, Some(limit), offset);
                assert_eq!(
                    execute_log_filter(&storage, &filter).unwrap(),
                    expected,
                    "seed={seed} {filter:?}"
                );
                let sql = format!(
                    "SELECT block_number, tx_index, log_index FROM logs ORDER BY block_number {direction}, tx_index {direction}, log_index {direction}"
                );
                let actual = execute_sql_page(
                    &sql,
                    &storage,
                    storage.head_block(),
                    SqlQueryPage::new(Some(limit), offset),
                )
                .await
                .unwrap();
                assert_eq!(actual.rows, projected(expected), "seed={seed} {filter:?}");
            }
        }
    }
}
