use super::*;
use crate::verified_block::tests::block;

fn storage(directory: &Path) -> NativeStorage {
    NativeStorage::open(NativeStorageConfig {
        data_dir: directory.to_owned(),
        hot_target_rows: 2,
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn canonical_publication_rejects_replay_and_persists_exact_rows_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = storage(dir.path());
    let (header, rows, proof) = block(10, B256::ZERO, 5);
    storage
        .ingest_verified_canonical_batch(
            &rows,
            std::slice::from_ref(&proof),
            std::slice::from_ref(&header),
            None,
        )
        .unwrap();
    let total: u64 = storage
        .catalog
        .segments
        .iter()
        .map(|segment| segment.row_count)
        .sum();
    assert_eq!(total, 5);
    assert!(storage.catalog.segments.len() > 1);
    assert!(
        storage
            .ingest_verified_canonical_batch(&rows, &[proof], std::slice::from_ref(&header), None)
            .is_err()
    );
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let mut reopened = self::storage(dir.path());
    assert_eq!(
        reopened.verified_log_coverage().unwrap().from.block_number,
        10
    );
    assert_eq!(
        reopened.verified_log_coverage().unwrap().to.block_number,
        10
    );
    let (next, next_rows, next_proof) = block(11, header.hash_slow(), 0);
    reopened
        .ingest_verified_canonical_batch(&next_rows, &[next_proof], &[header, next.clone()], None)
        .unwrap();
    assert_eq!(reopened.catalog.state.sync_head.unwrap().block_number, 11);
    assert_eq!(
        reopened.verified_log_coverage().unwrap().to.block_number,
        11
    );
    assert_eq!(
        reopened
            .catalog
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum::<u64>(),
        total
    );
}

#[test]
fn historical_publication_checks_every_block_and_rejects_overlap() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = storage(dir.path());
    let (first, mut rows, first_proof) = block(0, B256::ZERO, 3);
    let (second, second_rows, second_proof) = block(1, first.hash_slow(), 0);
    let (third, third_rows, third_proof) = block(2, second.hash_slow(), 3);
    let (anchor, anchor_rows, anchor_proof) = block(3, third.hash_slow(), 1);
    rows.extend(second_rows);
    rows.extend(third_rows);
    let execution = ExecutionAnchor {
        block_number: anchor.number,
        block_hash: anchor.hash_slow(),
        beacon_root: B256::repeat_byte(1),
        beacon_slot: 1,
        receipts_root: anchor.receipts_root,
    };
    storage
        .ingest_verified_canonical_batch(
            &anchor_rows,
            &[anchor_proof],
            std::slice::from_ref(&anchor),
            Some(&execution),
        )
        .unwrap();
    let proof = [first_proof, second_proof, third_proof];
    assert!(
        storage
            .ingest_verified_historical_batch(&rows, &[proof[0].clone(), proof[2].clone()])
            .is_err()
    );
    let mut duplicate = rows.clone();
    duplicate.push(rows[0].clone());
    assert!(
        storage
            .ingest_verified_historical_batch(&duplicate, &proof)
            .is_err()
    );
    assert_eq!(
        storage
            .catalog
            .state
            .historical_floor_header
            .as_ref()
            .unwrap()
            .number,
        3
    );
    storage
        .ingest_verified_historical_batch(&rows, &proof)
        .unwrap();
    assert_eq!(
        storage
            .catalog
            .state
            .historical_floor_header
            .as_ref()
            .unwrap()
            .number,
        0
    );
    assert!(
        storage
            .ingest_verified_historical_batch(&rows, &proof)
            .is_err()
    );
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let reopened = self::storage(dir.path());
    assert_eq!(
        reopened.verified_log_coverage().unwrap().from.block_number,
        0
    );
    assert_eq!(reopened.verified_log_coverage().unwrap().to.block_number, 3);
    assert_eq!(
        reopened
            .catalog
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum::<u64>(),
        7
    );
    assert_eq!(
        reopened
            .catalog
            .state
            .historical_floor_header
            .as_ref()
            .unwrap()
            .number,
        0
    );
}

#[test]
fn rejects_mutated_event_before_any_storage_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = storage(dir.path());
    let (header, mut rows, proof) = block(10, B256::ZERO, 1);
    rows[0].log_index = 1;
    assert!(
        storage
            .ingest_verified_canonical_batch(&rows, &[proof], &[header], None)
            .is_err()
    );
    assert!(
        storage
            .catalog
            .segments
            .iter()
            .all(|segment| segment.row_count == 0)
    );
    assert!(storage.catalog.state.sync_head.is_none());
    assert!(!storage.recovery_required);
}

#[test]
fn verified_publication_rejects_a_divergent_restart_header_window() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = self::storage(dir.path());
    let (base, rows, proof) = block(0, B256::ZERO, 1);
    storage
        .ingest_verified_canonical_batch(&rows, &[proof], std::slice::from_ref(&base), None)
        .unwrap();
    let (tip, tip_rows, tip_proof) = block(1, base.hash_slow(), 1);
    let mut divergent = base;
    divergent.timestamp += 1;
    assert!(
        storage
            .ingest_verified_canonical_batch(&tip_rows, &[tip_proof], &[divergent, tip], None)
            .is_err()
    );
    assert_eq!(storage.verified_log_coverage().unwrap().to.block_number, 0);
    assert_eq!(canonical_rows(&storage), rows);
}

fn canonical_rows(storage: &NativeStorage) -> Vec<LogRow> {
    let mut rows = Vec::new();
    for segment in &storage.catalog.segments {
        if segment.row_count == 0 {
            continue;
        }
        let reader = SegmentReader::open(&storage.paths.segment_dir(segment.id)).unwrap();
        let flags = reader.read_canonical().unwrap();
        rows.extend(
            reader
                .read_log_rows(None)
                .unwrap()
                .into_iter()
                .enumerate()
                .filter_map(|(index, row)| flags.is_present(index as u64).then_some(row)),
        );
    }
    rows.sort_by_key(|row| (row.block_number, row.log_index));
    rows
}

#[test]
fn unchecked_mutations_cannot_weaken_verified_coverage() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = storage(dir.path());
    let (header, rows, proof) = block(10, B256::ZERO, 3);
    storage
        .ingest_verified_canonical_batch(&rows, &[proof], std::slice::from_ref(&header), None)
        .unwrap();
    storage.checkpoint_durable().unwrap();
    let expected = storage.verified_log_coverage();
    let next = block(11, header.hash_slow(), 1).0;
    assert!(storage.write_batch(&rows).is_err());
    assert!(storage.write_historical_batch(&rows).is_err());
    assert!(
        storage
            .ingest_canonical_batch(&rows, &next, std::slice::from_ref(&next), None)
            .is_err()
    );
    assert!(storage.ingest_historical_batch(&rows, &header).is_err());
    assert!(
        storage
            .record_sync_head(11, next.hash_slow(), next.timestamp)
            .is_err()
    );
    assert!(
        storage
            .record_canonical_state(&next, std::slice::from_ref(&next))
            .is_err()
    );
    assert!(
        storage
            .record_historical_floor(&Header {
                number: 9,
                ..header.clone()
            })
            .is_err()
    );
    assert!(storage.mark_non_canonical(header.hash_slow()).is_err());
    storage.write_batch(&[]).unwrap();
    storage.write_historical_batch(&[]).unwrap();
    assert_eq!(storage.verified_log_coverage(), expected);
    assert_eq!(canonical_rows(&storage), rows);
    drop(storage);
    let reopened = self::storage(dir.path());
    assert_eq!(reopened.verified_log_coverage(), expected);
    assert_eq!(canonical_rows(&reopened), rows);
}

#[test]
fn legacy_rows_and_metadata_do_not_certify_earlier_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = storage(dir.path());
    let (older, older_rows, older_proof) = block(9, B256::ZERO, 1);
    let (legacy, legacy_rows, _) = block(10, older.hash_slow(), 1);
    let (next, next_rows, next_proof) = block(11, legacy.hash_slow(), 0);
    let anchor = ExecutionAnchor {
        block_number: legacy.number,
        block_hash: legacy.hash_slow(),
        beacon_root: B256::repeat_byte(1),
        beacon_slot: 1,
        receipts_root: legacy.receipts_root,
    };
    storage
        .ingest_canonical_batch(
            &legacy_rows,
            &legacy,
            std::slice::from_ref(&legacy),
            Some(&anchor),
        )
        .unwrap();
    assert!(storage.verified_log_coverage().is_none());
    storage
        .ingest_verified_canonical_batch(&next_rows, &[next_proof], &[legacy, next], None)
        .unwrap();
    storage
        .ingest_verified_historical_batch(&older_rows, &[older_proof])
        .unwrap();
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let reopened = self::storage(dir.path());
    let coverage = reopened.verified_log_coverage().unwrap();
    assert_eq!(
        (coverage.from.block_number, coverage.to.block_number),
        (11, 11)
    );
    assert_eq!(reopened.historical_floor().unwrap().block_number, 9);
}

#[test]
fn verified_batches_recover_every_publication_failure_without_duplicate_replay() {
    for historical in [false, true] {
        let mut steps = 0;
        for failure in std::iter::once(usize::MAX).chain(0..) {
            if failure != usize::MAX && failure >= steps {
                break;
            }
            let dir = tempfile::tempdir().unwrap();
            let mut storage = self::storage(dir.path());
            let (first, first_rows, first_proof) = block(0, B256::ZERO, 3);
            let empty = Header {
                number: 1,
                parent_hash: first.hash_slow(),
                timestamp: 2,
                ..Default::default()
            };
            let empty_proof = VerifiedBlockLogs::from_empty_header(&empty).unwrap();
            let (last, last_rows, last_proof) = block(2, empty.hash_slow(), 4);
            let initial = if historical { &last } else { &first };
            let initial_rows = if historical { &last_rows } else { &first_rows };
            let initial_proof = if historical {
                &last_proof
            } else {
                &first_proof
            };
            let anchor = ExecutionAnchor {
                block_number: initial.number,
                block_hash: initial.hash_slow(),
                beacon_root: B256::repeat_byte(1),
                beacon_slot: 1,
                receipts_root: initial.receipts_root,
            };
            storage
                .ingest_verified_canonical_batch(
                    initial_rows,
                    std::slice::from_ref(initial_proof),
                    std::slice::from_ref(initial),
                    Some(&anchor),
                )
                .unwrap();
            storage.checkpoint_durable().unwrap();
            let append = |storage: &mut NativeStorage| {
                if historical {
                    storage.ingest_verified_historical_batch(
                        &first_rows,
                        &[empty_proof.clone(), first_proof.clone()],
                    )
                } else {
                    storage.ingest_verified_canonical_batch(
                        &last_rows,
                        &[empty_proof.clone(), last_proof.clone()],
                        &[first.clone(), empty.clone(), last.clone()],
                        None,
                    )
                }
            };
            durability::inject_failure(failure);
            let result = append(&mut storage).and_then(|()| storage.checkpoint_durable());
            let events = durability::take_events();
            if failure == usize::MAX {
                result.unwrap();
                steps = events.len();
                assert!(steps > 0 && steps < 512);
            } else {
                assert!(result.is_err(), "{historical}/{failure}: {events:?}");
            }
            drop(storage);
            let mut reopened = self::storage(dir.path());
            let coverage = reopened.verified_log_coverage().unwrap();
            let completed = coverage.from.block_number == 0 && coverage.to.block_number == 2;
            let mut all_rows = first_rows.clone();
            all_rows.extend(last_rows.clone());
            assert_eq!(
                canonical_rows(&reopened),
                if completed {
                    all_rows.clone()
                } else {
                    initial_rows.clone()
                },
                "{historical}/{failure}: {events:?}"
            );
            if completed {
                assert!(append(&mut reopened).is_err());
            } else {
                append(&mut reopened).unwrap();
            }
            reopened.checkpoint_durable().unwrap();
            drop(reopened);
            let twice = self::storage(dir.path());
            assert_eq!(canonical_rows(&twice), all_rows);
            let coverage = twice.verified_log_coverage().unwrap();
            assert_eq!(
                (coverage.from.block_number, coverage.to.block_number),
                (0, 2)
            );
        }
    }
}

#[test]
fn verified_reorg_retires_old_events_and_replacement_publishes_once() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = storage(dir.path());
    let (base, base_rows, base_proof) = block(0, B256::ZERO, 2);
    let (old, old_rows, old_proof) = block(1, base.hash_slow(), 4);
    storage
        .ingest_verified_canonical_batch(
            &base_rows,
            &[base_proof],
            std::slice::from_ref(&base),
            None,
        )
        .unwrap();
    storage
        .ingest_verified_canonical_batch(
            &old_rows,
            &[old_proof],
            &[base.clone(), old.clone()],
            None,
        )
        .unwrap();
    storage.checkpoint_durable().unwrap();
    assert_eq!(
        storage
            .apply_canonical_reorg(&[old.hash_slow()], std::slice::from_ref(&base), None)
            .unwrap(),
        4
    );
    assert_eq!(storage.verified_log_coverage().unwrap().to.block_number, 0);
    let (replacement, replacement_rows, replacement_proof) = block(1, base.hash_slow(), 3);
    storage
        .ingest_verified_canonical_batch(
            &replacement_rows,
            std::slice::from_ref(&replacement_proof),
            &[base.clone(), replacement.clone()],
            None,
        )
        .unwrap();
    assert!(
        storage
            .ingest_verified_canonical_batch(
                &replacement_rows,
                &[replacement_proof],
                &[base, replacement],
                None
            )
            .is_err()
    );
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let reopened = self::storage(dir.path());
    let mut wanted = base_rows;
    wanted.extend(replacement_rows);
    assert_eq!(canonical_rows(&reopened), wanted);
    assert_eq!(reopened.verified_log_coverage().unwrap().to.block_number, 1);
}

#[test]
fn verified_reorg_recovers_each_failure_and_clips_legacy_starting_floor() {
    for legacy_base in [false, true] {
        let mut steps = 0;
        for failure in std::iter::once(usize::MAX).chain(0..) {
            if failure != usize::MAX && failure >= steps {
                break;
            }
            let dir = tempfile::tempdir().unwrap();
            let mut storage = self::storage(dir.path());
            let (base, base_rows, base_proof) = block(0, B256::ZERO, 2);
            let (old, old_rows, old_proof) = block(1, base.hash_slow(), 4);
            if legacy_base {
                storage
                    .ingest_canonical_batch(&base_rows, &base, std::slice::from_ref(&base), None)
                    .unwrap();
            } else {
                storage
                    .ingest_verified_canonical_batch(
                        &base_rows,
                        &[base_proof],
                        std::slice::from_ref(&base),
                        None,
                    )
                    .unwrap();
            }
            storage
                .ingest_verified_canonical_batch(
                    &old_rows,
                    &[old_proof],
                    &[base.clone(), old.clone()],
                    None,
                )
                .unwrap();
            storage.checkpoint_durable().unwrap();
            durability::inject_failure(failure);
            let result = storage.apply_canonical_reorg(
                &[old.hash_slow()],
                std::slice::from_ref(&base),
                None,
            );
            let events = durability::take_events();
            if failure == usize::MAX {
                assert_eq!(result.unwrap(), 4);
                steps = events.len();
                assert!(steps > 0 && steps < 512);
            } else {
                assert!(result.is_err(), "{legacy_base}/{failure}: {events:?}");
            }
            drop(storage);
            let mut reopened = self::storage(dir.path());
            if reopened.sync_head().unwrap().block_number == 1 {
                reopened
                    .apply_canonical_reorg(&[old.hash_slow()], std::slice::from_ref(&base), None)
                    .unwrap();
            }
            assert_eq!(
                canonical_rows(&reopened),
                base_rows,
                "{legacy_base}/{failure}: {events:?}"
            );
            assert_eq!(reopened.historical_floor_header(), Some(&base));
            if legacy_base {
                assert!(reopened.verified_log_coverage().is_none());
            } else {
                assert_eq!(reopened.verified_log_coverage().unwrap().to.block_number, 0);
            }
            let (replacement, rows, proof) = block(1, base.hash_slow(), 3);
            reopened
                .ingest_verified_canonical_batch(&rows, &[proof], &[base, replacement], None)
                .unwrap();
            reopened.checkpoint_durable().unwrap();
            drop(reopened);
            let twice = self::storage(dir.path());
            let mut expected = base_rows;
            expected.extend(rows);
            assert_eq!(canonical_rows(&twice), expected);
            let coverage = twice.verified_log_coverage().unwrap();
            assert_eq!(coverage.from.block_number, u64::from(legacy_base));
            assert_eq!(coverage.to.block_number, 1);
        }
    }
}

#[test]
fn legacy_events_outside_progress_cannot_be_duplicated_by_verified_ingestion() {
    for historical in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = self::storage(dir.path());
        let (first, first_rows, first_proof) = block(0, B256::ZERO, 2);
        let (last, last_rows, last_proof) = block(1, first.hash_slow(), 2);
        if historical {
            storage
                .ingest_historical_batch(&first_rows, &last)
                .unwrap_err();
            storage.write_historical_batch(&first_rows).unwrap();
            storage.record_historical_floor(&last).unwrap();
            assert!(
                storage
                    .ingest_verified_historical_batch(&first_rows, &[first_proof])
                    .unwrap_err()
                    .to_string()
                    .contains("already contains canonical events")
            );
        } else {
            storage.write_batch(&last_rows).unwrap();
            storage
                .record_canonical_state(&first, std::slice::from_ref(&first))
                .unwrap();
            assert!(
                storage
                    .ingest_verified_canonical_batch(
                        &last_rows,
                        &[last_proof],
                        &[first, last],
                        None
                    )
                    .unwrap_err()
                    .to_string()
                    .contains("already contains canonical events")
            );
        }
        assert_eq!(storage.total_rows(), 2);
        assert!(storage.verified_log_coverage().is_none());
    }
}
