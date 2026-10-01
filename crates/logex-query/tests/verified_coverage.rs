//! Admission uses real receipt/root verification and durable publication, never
//! forged catalog coverage. These small unit fixtures are not mainnet benchmarks.
use alloy_consensus::{
    Eip658Value, Header, Receipt, ReceiptEnvelope, SignableTransaction, TxEnvelope, TxLegacy,
    TxReceipt, proofs,
};
use alloy_primitives::{Address, B256, Bytes, Log, Signature};
use logex_query::{
    NativeStorageSnapshot, QueryCoverageError, SqlQueryError, SqlQueryPage, execute_log_filter,
    execute_log_filter_on_snapshot_with_cancel, execute_log_filter_on_snapshot_with_memory,
    execute_sql, execute_sql_page, execute_sql_page_on_snapshot, query_coverage_error,
};
use logex_storage::{
    PartitionManager, PartitionManagerConfig, VerifiedBlockLogs, native::NativeLogFilter,
};
use logex_types::{LogRow, QueryMemoryBudget, QueryMemoryLimit};

struct Chain {
    headers: Vec<Header>,
    blocks: Vec<VerifiedBlockLogs>,
    rows: Vec<Vec<LogRow>>,
}

fn chain() -> Chain {
    let mut chain = Chain {
        headers: Vec::new(),
        blocks: Vec::new(),
        rows: Vec::new(),
    };
    for number in 0..=13 {
        let parent_hash = chain
            .headers
            .last()
            .map(Header::hash_slow)
            .unwrap_or_default();
        let mut header = Header {
            number,
            parent_hash,
            timestamp: number + 1,
            ..Default::default()
        };
        let logs = if number == 10 {
            2
        } else {
            usize::from(number >= 12)
        };
        let mut rows = Vec::new();
        let proof = if logs == 0 {
            VerifiedBlockLogs::from_empty_header(&header).unwrap()
        } else {
            let tx: TxEnvelope = TxLegacy {
                nonce: number,
                gas_limit: 100_000,
                ..Default::default()
            }
            .into_signed(Signature::test_signature())
            .into();
            let mut data = vec![0; 32];
            data[31] = 1;
            let receipt = ReceiptEnvelope::Legacy(
                Receipt {
                    status: Eip658Value::Eip658(true),
                    cumulative_gas_used: 21_000,
                    logs: (0..logs)
                        .map(|_| {
                            Log::new(
                                Address::repeat_byte(0x51),
                                vec![B256::repeat_byte(0x33)],
                                Bytes::copy_from_slice(&data),
                            )
                            .unwrap()
                        })
                        .collect(),
                }
                .with_bloom(),
            );
            header.gas_used = 21_000;
            header.transactions_root =
                proofs::calculate_transaction_root(std::slice::from_ref(&tx));
            header.receipts_root = proofs::calculate_receipt_root(std::slice::from_ref(&receipt));
            header.logs_bloom = receipt.bloom();
            VerifiedBlockLogs::verify_and_append(&header, &[tx], &[receipt], &mut rows).unwrap()
        };
        chain.headers.push(header);
        chain.blocks.push(proof);
        chain.rows.push(rows);
    }
    chain
}

fn setup() -> (tempfile::TempDir, PartitionManager, Chain) {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: dir.path().to_owned(),
        partition_target_rows: 2,
        ..Default::default()
    })
    .unwrap();
    let chain = chain();
    let rows: Vec<_> = chain.rows[10..=12].concat();
    storage
        .ingest_verified_canonical_batch(
            &rows,
            &chain.blocks[10..=12],
            &chain.headers[10..=12],
            None,
        )
        .unwrap();
    (dir, storage, chain)
}

#[tokio::test]
async fn legacy_rows_and_head_do_not_certify_queries() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(PartitionManagerConfig {
        data_dir: dir.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    let chain = chain();
    storage
        .ingest_canonical_batch(
            &chain.rows[10],
            &chain.headers[10],
            &chain.headers[10..=10],
            None,
        )
        .unwrap();
    assert_eq!(storage.head_block(), Some(10));
    let filter = NativeLogFilter::new().with_block_range(Some(10), Some(10));
    let error = execute_log_filter(&storage, &filter).unwrap_err();
    assert_eq!(
        query_coverage_error(&error),
        Some(QueryCoverageError::Unverified)
    );
    for sql in [
        "SELECT * FROM logs WHERE block_number = 10",
        "SELECT COUNT(*) FROM logs",
        "SELECT DISTINCT address FROM logs",
    ] {
        assert!(
            matches!(
                execute_sql(sql, &storage, None).await,
                Err(SqlQueryError::Coverage(QueryCoverageError::Unverified))
            ),
            "{sql}"
        );
    }
    let inspected = execute_log_filter_on_snapshot_with_cancel(
        &NativeStorageSnapshot::for_unverified_inspection(&storage),
        &filter,
        None,
    )
    .unwrap();
    assert_eq!(inspected, chain.rows[10]);
}

#[test]
fn native_queries_admit_complete_ranges_and_explicit_empty_blocks() {
    let (_dir, storage, chain) = setup();
    let filter = NativeLogFilter::new().with_block_range(Some(10), Some(12));
    assert_eq!(
        execute_log_filter(&storage, &filter).unwrap(),
        chain.rows[10..=12].concat()
    );
    assert!(
        execute_log_filter(
            &storage,
            &NativeLogFilter::new().with_block_range(Some(11), Some(11))
        )
        .unwrap()
        .is_empty()
    );
    assert!(
        execute_log_filter(
            &storage,
            &NativeLogFilter::new().with_block_hash(chain.headers[11].hash_slow())
        )
        .unwrap()
        .is_empty()
    );
    let snapshot = NativeStorageSnapshot::from_storage(&storage);
    let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024 * 1024).unwrap());
    for filter in [
        NativeLogFilter::new(),
        NativeLogFilter::new().with_block_range(Some(9), Some(12)),
        NativeLogFilter::new().with_block_range(Some(10), Some(13)),
        NativeLogFilter::new().with_block_hash(B256::repeat_byte(0xff)),
        NativeLogFilter::default(),
    ] {
        assert!(
            query_coverage_error(&execute_log_filter(&storage, &filter).unwrap_err()).is_some()
        );
        let error = execute_log_filter_on_snapshot_with_memory(&snapshot, &filter, None, &memory)
            .unwrap_err();
        assert!(query_coverage_error(&error).is_some());
        assert_eq!(memory.used(), 0);
    }
}

#[tokio::test]
async fn every_sql_scan_requires_complete_coverage_even_with_empty_candidates() {
    let (_dir, storage, _) = setup();
    for sql in [
        "SELECT block_number FROM logs LIMIT 1",
        "SELECT COUNT(*) FROM logs",
        "SELECT SUM(data) FROM logs",
        "SELECT topic0, SUM(data) FROM logs GROUP BY topic0",
        "SELECT DISTINCT address FROM logs",
        "SELECT COUNT(block_number + 0) FROM logs WHERE block_number >= 9",
        "SELECT * FROM logs WHERE block_number BETWEEN 10 AND 13",
        "SELECT * FROM logs WHERE block_number = 9 AND address = '0xffffffffffffffffffffffffffffffffffffffff'",
        "SELECT block_number FROM logs WHERE block_number = 10 OR block_number = 9",
        "WITH bounded AS (SELECT * FROM logs WHERE block_number >= 10) SELECT (SELECT COUNT(*) FROM logs) FROM bounded",
        "SELECT a.block_number FROM logs a JOIN logs b ON a.address = b.address WHERE a.block_number = 10",
        "SELECT block_number FROM logs WHERE block_number = 10 UNION ALL SELECT block_number FROM logs WHERE block_number = 9",
        "SELECT COUNT(*) FROM logs WHERE block_number + 0 >= 9",
    ] {
        let error = execute_sql(sql, &storage, None).await.unwrap_err();
        assert!(
            matches!(error, SqlQueryError::Coverage(_)),
            "{sql}: {error}"
        );
    }
    for sql in [
        "SELECT block_number FROM logs WHERE block_number BETWEEN 10 AND 12",
        "SELECT COUNT(*) FROM logs WHERE block_number >= 10",
        "SELECT SUM(data) FROM logs WHERE block_number >= 10",
        "SELECT DISTINCT address FROM logs WHERE block_number >= 10",
        "WITH bounded AS (SELECT * FROM logs WHERE block_number >= 10) SELECT COUNT(*) FROM bounded",
        "SELECT a.block_number FROM logs a JOIN logs b ON a.address = b.address WHERE a.block_number = 10 AND b.block_number >= 10",
        "SELECT block_number FROM logs WHERE block_number = 10 UNION ALL SELECT block_number FROM logs WHERE block_number = 12",
    ] {
        execute_sql(sql, &storage, None)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
    for sql in [
        "SELECT COUNT(*) FROM logs",
        "SELECT SUM(data) FROM logs",
        "SELECT * FROM logs",
    ] {
        assert!(
            matches!(
                execute_sql_page(sql, &storage, None, SqlQueryPage::new(Some(0), 0)).await,
                Err(SqlQueryError::Coverage(_))
            ),
            "{sql}"
        );
    }
}

#[tokio::test]
async fn backfill_and_append_do_not_expand_an_existing_snapshot_and_reorg_invalidates_it() {
    let (_dir, mut storage, chain) = setup();
    let snapshot = NativeStorageSnapshot::from_storage(&storage);
    storage
        .ingest_verified_historical_batch(&chain.rows[..10].concat(), &chain.blocks[..10])
        .unwrap();
    assert!(
        execute_sql("SELECT COUNT(*) FROM logs", &storage, None)
            .await
            .is_ok()
    );
    assert!(matches!(
        execute_sql_page_on_snapshot(
            "SELECT COUNT(*) FROM logs",
            snapshot.clone(),
            12,
            SqlQueryPage::default(),
            None
        )
        .await,
        Err(SqlQueryError::Coverage(_))
    ));
    storage
        .ingest_verified_canonical_batch(
            &chain.rows[13],
            &chain.blocks[13..=13],
            &chain.headers[10..=13],
            None,
        )
        .unwrap();
    assert!(
        execute_log_filter(
            &storage,
            &NativeLogFilter::new().with_block_range(Some(13), Some(13))
        )
        .is_ok()
    );
    assert!(
        execute_log_filter_on_snapshot_with_cancel(
            &snapshot,
            &NativeLogFilter::new().with_block_range(Some(13), Some(13)),
            None
        )
        .is_err()
    );
    storage
        .apply_canonical_reorg(
            &[chain.headers[12].hash_slow(), chain.headers[13].hash_slow()],
            &chain.headers[10..=11],
            None,
        )
        .unwrap();
    let error = execute_log_filter_on_snapshot_with_cancel(
        &snapshot,
        &NativeLogFilter::new().with_block_range(Some(10), Some(10)),
        None,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert!(
        execute_log_filter(
            &storage,
            &NativeLogFilter::new().with_block_range(Some(12), Some(12))
        )
        .is_err()
    );
}

#[tokio::test]
async fn queries_without_event_reads_work_before_sync() {
    for sql in [
        "SELECT 1 AS value",
        "SELECT table_name FROM information_schema.tables",
        "SELECT * FROM logs WHERE false",
    ] {
        execute_sql_page_on_snapshot(
            sql,
            NativeStorageSnapshot::default(),
            0,
            SqlQueryPage::default(),
            None,
        )
        .await
        .unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}
