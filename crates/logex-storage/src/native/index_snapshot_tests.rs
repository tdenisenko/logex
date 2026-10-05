use super::*;
use alloy_primitives::{Address, Bytes};
use logex_types::Source;
use std::cell::Cell;

fn limits() -> IndexAuditLimits {
    IndexAuditLimits {
        max_segments: 100,
        max_total_rows: 10_000,
        segment: InspectionLimits {
            max_segment_rows: 1_000,
            max_retained_artifact_bytes: 16 * 1024 * 1024,
            max_decoded_payload_bytes: 16 * 1024 * 1024,
        },
    }
}

fn config(root: &Path) -> NativeStorageConfig {
    NativeStorageConfig {
        data_dir: root.to_owned(),
        hot_target_rows: 4,
        compaction_safety_margin_blocks: 0,
    }
}

fn rows(count: u32) -> Vec<LogRow> {
    (0..count)
        .map(|i| LogRow {
            block_number: 100 + u64::from(i / 3),
            block_hash: B256::repeat_byte((100 + i / 3) as u8),
            timestamp: 1_000 + u64::from(i / 3),
            tx_hash: B256::repeat_byte(i as u8),
            tx_index: i % 3,
            log_index: i % 3,
            address: Address::repeat_byte(i as u8),
            topic0: Some(B256::repeat_byte(10)),
            topic1: (i % 2 == 0).then_some(B256::repeat_byte(11)),
            topic2: None,
            topic3: None,
            data: Bytes::from(vec![i as u8; (i + 1) as usize]),
            data_len: i + 1,
            source: Source::Receipt,
        })
        .collect()
}

fn write(storage: &mut NativeStorage, rows: &[LogRow], bundled: bool) {
    if bundled {
        storage.write_historical_batch(rows).unwrap();
    } else {
        storage.write_batch(rows).unwrap();
    }
    storage.checkpoint_durable().unwrap();
}

fn tree(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, out);
            } else {
                out.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}
use std::collections::BTreeMap;

#[test]
fn index_sources_retain_finite_owned_prefixes_across_appends() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        hot_target_rows: 1_000,
        ..config(dir.path())
    })
    .unwrap();
    write(&mut storage, &rows(2), false);
    let snapshot = storage.index_audit_snapshot(limits()).unwrap();
    write(&mut storage, &rows(3), false);
    let mut visited = 0;
    snapshot
        .visit_index_sources(&|| false, |source| {
            assert_eq!(source.minimum_rows, 2);
            assert_eq!(source.reader.read_row_count()?, 5);
            let captured = source.reader.read_address(None)?;
            write(&mut storage, &rows(40), false);
            assert_eq!(source.reader.read_address(None)?, captured);
            assert_eq!(source.reader.read_nullable_b256("topic0", None)?.len(), 5);
            assert_eq!(source.reader.read_nullable_b256("topic1", None)?.len(), 5);
            assert_eq!(source.reader.read_row_count()?, 5);
            visited += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(visited, 1);
    assert_eq!(snapshot.physical_rows(), 2);
    drop(storage);
    assert_eq!(
        snapshot
            .visit_index_sources(&|| false, |_| Ok(()))
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn index_source_capture_rejects_shape_budget_and_view_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(6), true);
    let snapshot = storage.index_audit_snapshot(limits()).unwrap();
    assert_eq!(
        snapshot
            .visit_index_sources(&|| true, |_| panic!("cancelled source visited"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    let mut bound = limits();
    bound.segment.max_retained_artifact_bytes = 1;
    let bounded = storage.index_audit_snapshot(bound).unwrap();
    assert_eq!(
        bounded
            .visit_index_sources(&|| false, |_| panic!("oversized source visited"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    let hash = rows(1)[0].block_hash;
    assert_eq!(
        snapshot
            .visit_index_sources(&|| false, |_| {
                storage.mark_non_canonical(hash)?;
                Ok(())
            })
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn online_index_source_bounds_reject_shrink_and_excess_opened_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        hot_target_rows: 1_000,
        ..config(dir.path())
    })
    .unwrap();
    write(&mut storage, &rows(3), false);
    let snapshot = storage.index_audit_snapshot(limits()).unwrap();
    let error = snapshot
        .visit_index_sources(&|| false, |source| {
            let path = source.index_directory.parent().unwrap().join("address.col");
            let file = fs::OpenOptions::new().write(true).open(path)?;
            file.set_len(0)?;
            source.reader.read_address(None).map(|_| ())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);

    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        hot_target_rows: 1_000,
        ..config(dir.path())
    })
    .unwrap();
    write(&mut storage, &rows(2), false);
    let mut limit = limits();
    limit.max_total_rows = 3;
    let snapshot = storage.index_audit_snapshot(limit).unwrap();
    write(&mut storage, &rows(2), false);
    assert_eq!(
        snapshot
            .visit_index_sources(&|| false, |_| panic!("excess source visited"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn capture_and_open_limits_fail_before_visiting_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(3), false);
    let mut small = limits();
    small.max_segments = 0;
    assert_eq!(
        storage.index_audit_snapshot(small).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    small = limits();
    small.max_total_rows = 2;
    assert_eq!(
        storage.index_audit_snapshot(small).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    small = limits();
    small.segment.max_segment_rows = 2;
    assert_eq!(
        storage.index_audit_snapshot(small).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    for retained in [false, true] {
        small = limits();
        if retained {
            small.segment.max_retained_artifact_bytes = 0;
        } else {
            small.segment.max_decoded_payload_bytes = 0;
        }
        let snapshot = storage.index_audit_snapshot(small).unwrap();
        let error = snapshot
            .visit_index_sources(&|| false, |_| panic!("limited snapshot visited sources"))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
    small = limits();
    small.segment.max_segment_rows = 3;
    let snapshot = storage.index_audit_snapshot(small).unwrap();
    write(&mut storage, &rows(1), false);
    assert_eq!(
        snapshot
            .visit_index_sources(&|| false, |_| panic!("grown segment exceeds work limit"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn missing_source_never_gets_created_or_repaired_by_a_visit() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(3), true);
    let snapshot = storage.index_audit_snapshot(limits()).unwrap();
    let id = storage
        .segments()
        .iter()
        .find(|s| s.row_count > 0)
        .unwrap()
        .id;
    fs::remove_dir_all(storage.segment_path(id)).unwrap();
    let before = tree(dir.path());
    assert!(snapshot.visit_index_sources(&|| false, |_| Ok(())).is_err());
    assert_eq!(tree(dir.path()), before);
}

#[test]
fn recovery_required_capture_is_rejected_without_storage_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    storage.recovery_required = true;
    let before = tree(dir.path());
    assert!(storage.index_audit_snapshot(limits()).is_err());
    assert_eq!(tree(dir.path()), before);
}

#[test]
fn headroom_checks_preserve_storage_and_reject_stale_source() {
    let dir = tempfile::tempdir().unwrap();
    let storage = NativeStorage::open(config(dir.path())).unwrap();
    let snapshot = storage.index_audit_snapshot(limits()).unwrap();
    let before = tree(dir.path());
    snapshot.check_available_space(0).unwrap();
    assert!(snapshot.check_available_space(u64::MAX).is_err());
    assert_eq!(tree(dir.path()), before);
    drop(storage);
    assert_eq!(
        snapshot.check_available_space(0).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn retained_owner_survives_compaction_and_blocks_reopen_until_release() {
    for bundled in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = config(dir.path());
        let mut storage = NativeStorage::open(cfg.clone()).unwrap();
        write(&mut storage, &rows(6), bundled);
        let snapshot = storage.index_audit_snapshot(limits()).unwrap();
        let fingerprint = snapshot.source_prefix_fingerprint();
        write(&mut storage, &rows(9), bundled);
        storage.compact_raw_segments_limit(100).unwrap();
        let mut covered = 0;
        snapshot
            .visit_index_sources(&|| false, |source| {
                assert!(source.reader.read_row_count()? >= source.minimum_rows);
                covered += source.minimum_rows;
                Ok(())
            })
            .unwrap();
        assert_eq!(covered, 6);
        assert_eq!(snapshot.physical_rows(), 6);
        assert_eq!(snapshot.source_prefix_fingerprint(), fingerprint);
        assert_eq!(
            storage
                .index_audit_snapshot(limits())
                .unwrap()
                .physical_rows(),
            15
        );
        drop(storage);
        assert_eq!(
            snapshot.validate().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        assert!(NativeStorage::open(cfg.clone()).is_err());
        drop(snapshot);
        assert!(NativeStorage::open(cfg).is_ok());
    }
}

#[test]
fn cancellation_and_callback_failure_preserve_storage_and_invalidation() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(10), true);
    let snapshot = storage.index_audit_snapshot(limits()).unwrap();
    let before = tree(dir.path());
    let cancelled = Cell::new(false);
    assert_eq!(
        snapshot
            .visit_index_sources(&|| cancelled.get(), |_| {
                cancelled.set(true);
                Ok(())
            })
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    let error = snapshot
        .visit_index_sources(&|| false, |_| Err(io::Error::other("sink unavailable")))
        .unwrap_err();
    assert_eq!(error.to_string(), "sink unavailable");
    assert_eq!(tree(dir.path()), before);
    let error = snapshot
        .visit_index_sources(&|| false, |_| {
            storage.mark_non_canonical(rows(1)[0].block_hash)?;
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "source changed during reorg",
            ))
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn empty_selection_excludes_first_append_and_changed_source_binding_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    let empty = storage.index_audit_snapshot(limits()).unwrap();
    write(&mut storage, &rows(3), true);
    empty
        .visit_index_sources(&|| false, |_| panic!("empty selection included later rows"))
        .unwrap();
    assert_eq!(empty.physical_rows(), 0);
    let index = storage
        .catalog
        .segments
        .iter()
        .position(|d| d.row_count > 0)
        .unwrap();
    let original = storage.catalog.segments[index].clone();
    storage.catalog.segments[index].source_commitment = Some(B256::repeat_byte(0xff));
    assert_eq!(
        storage
            .index_audit_snapshot(limits())
            .unwrap()
            .visit_index_sources(&|| false, |_| panic!("changed source admitted"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    storage.catalog.segments[index] = original;
    storage.catalog.segments[index].source_namespace = None;
    storage.catalog.segments[index].source_commitment = None;
    assert_eq!(
        storage.index_audit_snapshot(limits()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn retry_reopens_grown_prefix_and_charges_only_the_admitted_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        hot_target_rows: 1_000,
        ..config(dir.path())
    })
    .unwrap();
    write(&mut storage, &rows(2), false);
    let mut budget = limits();
    budget.max_total_rows = 3;
    let source = storage.index_audit_snapshot(budget).unwrap();
    let mut attempts = 0;
    source
        .visit_index_sources_with_retry(&|| false, |item| {
            attempts += 1;
            assert_eq!(item.minimum_rows, 2);
            if attempts == 1 {
                assert_eq!(item.reader.read_row_count()?, 2);
                write(&mut storage, &rows(1), false);
                // The retained reader cannot absorb the concurrent append.
                assert_eq!(item.reader.read_row_count()?, 2);
                Ok(IndexAuditSourceAction::Retry)
            } else {
                assert_eq!(item.reader.read_row_count()?, 3);
                assert_eq!(item.reader.read_address(None)?.len(), 3);
                Ok(IndexAuditSourceAction::Complete)
            }
        })
        .unwrap();
    assert_eq!(attempts, 2);
    assert_eq!(source.physical_rows(), 2);
}

#[test]
fn retry_rechecks_cancellation_view_and_opened_source_limits() {
    for cause in ["cancel", "reorg", "total", "segment"] {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            hot_target_rows: 1_000,
            ..config(dir.path())
        })
        .unwrap();
        write(&mut storage, &rows(2), false);
        let mut budget = limits();
        if cause == "total" {
            budget.max_total_rows = 3;
        }
        if cause == "segment" {
            budget.segment.max_segment_rows = 3;
        }
        let source = storage.index_audit_snapshot(budget).unwrap();
        let cancelled = Cell::new(false);
        let mut visits = 0;
        let error = source
            .visit_index_sources_with_retry(&|| cancelled.get(), |_| {
                visits += 1;
                assert_eq!(visits, 1, "unavailable source was admitted: {cause}");
                match cause {
                    "cancel" => cancelled.set(true),
                    "reorg" => {
                        storage.mark_non_canonical(rows(1)[0].block_hash)?;
                    }
                    _ => write(&mut storage, &rows(2), false),
                }
                Ok(IndexAuditSourceAction::Retry)
            })
            .unwrap_err();
        let expected = match cause {
            "cancel" => io::ErrorKind::Interrupted,
            "reorg" => io::ErrorKind::WouldBlock,
            _ => io::ErrorKind::InvalidInput,
        };
        assert_eq!(error.kind(), expected, "{cause}: {error}");
        assert_eq!(visits, 1);
    }
}
