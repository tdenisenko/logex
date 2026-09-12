// Diagnostic only: append to the identical audit_harness.rs in both isolated
// revision archives. This separates history from the concurrent-query workload
// and repeats warm reopen/SQL paths on one fixed indexed/compacted dataset.
#[tokio::test]
#[ignore = "isolated release diagnostic; see sql-predicates acceptance record"]
async fn benchmark_predicate_phase_isolation() {
    let config = Config::from_env();
    let rows = fixture(config.rows, config.profile);
    let expected = expected_matches(&rows);
    println!("{}", json!({"kind":"config", "profile":config.profile.name(),
        "rows":config.rows, "repeats":config.repeats,
        "fixture_digest":keccak256(serde_json::to_vec(&rows).unwrap()).to_string(),
        "isolation":"fresh history first; then fixed indexed/compacted warm reopen and queries"}));
    let mut samples = Samples { enabled: true, ..Default::default() };
    for iteration in 0..config.repeats {
        let history = tempfile::tempdir().unwrap();
        let mut storage = PartitionManager::open(config.storage(history.path())).unwrap();
        let start = Instant::now();
        for batch in rows.rchunks(config.batch_rows) {
            storage.write_historical_batch(batch).unwrap();
        }
        storage.finalize_historical_segment().unwrap();
        samples.record("historical_storage_ingest", iteration, start.elapsed(), rows.len());
        assert_eq!(storage.total_rows(), rows.len() as u64);
        assert_native(&storage, &expected);
        drop(storage);
        let storage = PartitionManager::open(config.storage(history.path())).unwrap();
        assert_eq!(storage.total_rows(), rows.len() as u64);
        assert_native(&storage, &expected);
    }
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = PartitionManager::open(config.storage(tmp.path())).unwrap();
    for batch in rows.chunks(config.batch_rows) {
        storage.write_batch(batch).unwrap();
    }
    storage.checkpoint().unwrap();
    for partition in storage.sealed_partitions().iter().chain(std::iter::once(storage.hot_partition())) {
        if partition.meta.row_count > 0 {
            IndexBuilder::build_all_indexes(&partition.meta.path).unwrap();
        }
    }
    assert!(storage.compact_eligible_segments().unwrap() > 0);
    drop(storage);
    for iteration in 0..config.repeats {
        let start = Instant::now();
        let storage = PartitionManager::open(config.storage(tmp.path())).unwrap();
        samples.record("reopen", iteration, start.elapsed(), rows.len());
        assert_eq!(storage.total_rows(), rows.len() as u64);
        query_cases(&storage, &expected, iteration, &mut Samples::default()).await;
        query_cases(&storage, &expected, iteration, &mut samples).await;
    }
    samples.summarize();
}
