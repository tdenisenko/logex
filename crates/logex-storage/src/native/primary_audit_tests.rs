use super::*;
use alloy_primitives::{Address, Bytes};
use logex_types::Source;
use std::cell::Cell;

fn limits() -> PrimaryAuditLimits {
    PrimaryAuditLimits {
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

type CapturedRows = Vec<(u64, u32, bool, LogRow)>;

fn collect(snapshot: &PrimaryAuditSnapshot) -> io::Result<(PrimaryAuditReport, CapturedRows)> {
    let mut rows = Vec::new();
    let report = snapshot.scan(&|| false, |batch| {
        rows.extend(
            batch
                .rows()
                .map(|v| (v.segment_id, v.row_id, v.canonical, v.row.clone())),
        );
        Ok(())
    })?;
    Ok((report, rows))
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
fn every_physical_row_survives_across_segments_including_duplicates_and_forks() {
    for bundled in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = NativeStorage::open(config(dir.path())).unwrap();
        let mut expected = rows(9);
        expected.push(expected[0].clone());
        expected.push(expected[0].clone());
        write(&mut storage, &expected, bundled);
        let before_reorg = storage.primary_audit_snapshot(limits()).unwrap();
        let (all, captured) = collect(&before_reorg).unwrap();
        assert_eq!(all.physical_rows, 11);
        assert_eq!(all.canonical_rows, 11);
        assert_eq!(captured.iter().filter(|v| v.3 == expected[0]).count(), 3);
        assert_eq!(
            captured.iter().map(|v| v.3.clone()).collect::<Vec<_>>(),
            expected
        );
        let hash = expected[3].block_hash;
        assert_eq!(storage.mark_non_canonical(hash).unwrap(), 3);
        assert_eq!(
            collect(&before_reorg).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        let before = tree(dir.path());
        let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
        let (report, captured) = collect(&snapshot).unwrap();
        assert_eq!(tree(dir.path()), before, "read-only scan changed a file");
        assert_eq!(report.physical_rows, 11);
        assert_eq!(report.canonical_rows, 8);
        assert_eq!(
            captured.iter().map(|v| v.3.clone()).collect::<Vec<_>>(),
            expected
        );
        assert!(captured.iter().all(|v| v.2 == (v.3.block_hash != hash)));
        assert_eq!(
            all.source_prefix_fingerprint,
            report.source_prefix_fingerprint
        );
        assert!(
            all.segments
                .iter()
                .zip(&report.segments)
                .any(|(a, b)| a.canonical_digest != b.canonical_digest)
        );
        let unique: BTreeSet<_> = captured.iter().map(|v| (v.0, v.1)).collect();
        assert_eq!(unique.len(), 11);
    }
}

#[test]
fn later_appends_new_segments_and_compaction_do_not_change_the_captured_prefix() {
    for bundled in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = NativeStorage::open(config(dir.path())).unwrap();
        write(&mut storage, &rows(6), bundled);
        let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
        let (before, captured) = collect(&snapshot).unwrap();
        write(&mut storage, &rows(9), bundled);
        storage.compact_raw_segments_limit(100).unwrap();
        let (after, current) = collect(&snapshot).unwrap();
        assert_eq!(after, before);
        assert_eq!(current, captured);
        assert_eq!(
            collect(&storage.primary_audit_snapshot(limits()).unwrap())
                .unwrap()
                .0
                .physical_rows,
            15
        );
    }
}

#[test]
fn an_empty_capture_stays_empty_after_first_append() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
    let (empty, _) = collect(&snapshot).unwrap();
    write(&mut storage, &rows(3), false);
    let (after, captured) = collect(&snapshot).unwrap();
    assert_eq!(after, empty);
    assert!(captured.is_empty());
}

#[test]
fn a_reorg_during_callback_prevents_successful_completion() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(10), true);
    let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
    let error = snapshot
        .scan(&|| false, |batch| {
            let hash = batch.rows().next().unwrap().row.block_hash;
            storage.mark_non_canonical(hash)?;
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
}

#[test]
fn close_invalidates_snapshot_and_retained_owner_prevents_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = config(dir.path());
    let mut storage = NativeStorage::open(cfg.clone()).unwrap();
    write(&mut storage, &rows(4), false);
    let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
    let expected = collect(&snapshot).unwrap();
    drop(storage);
    assert_eq!(
        collect(&snapshot).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(NativeStorage::open(cfg.clone()).is_err());
    drop(snapshot);
    let storage = NativeStorage::open(cfg).unwrap();
    assert_eq!(
        collect(&storage.primary_audit_snapshot(limits()).unwrap()).unwrap(),
        expected
    );
}

#[test]
fn cancellation_and_callback_failure_never_return_partial_success_or_mutate_storage() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(10), true);
    let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
    let before = tree(dir.path());
    let cancelled = Cell::new(false);
    let error = snapshot
        .scan(&|| cancelled.get(), |_| {
            cancelled.set(true);
            Ok(())
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert_eq!(
        snapshot
            .scan(&|| true, |_| panic!("cancelled scan decoded rows"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::Interrupted
    );
    let error = snapshot
        .scan(&|| false, |_| Err(io::Error::other("sink unavailable")))
        .unwrap_err();
    assert_eq!(error.to_string(), "sink unavailable");
    assert_eq!(tree(dir.path()), before);
    assert_eq!(collect(&snapshot).unwrap().0.physical_rows, 10);
}

#[test]
fn capture_and_open_limits_fail_before_visiting_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(3), false);
    let mut small = limits();
    small.max_segments = 0;
    assert_eq!(
        storage.primary_audit_snapshot(small).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    small = limits();
    small.max_total_rows = 2;
    assert_eq!(
        storage.primary_audit_snapshot(small).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    small = limits();
    small.segment.max_segment_rows = 2;
    assert_eq!(
        storage.primary_audit_snapshot(small).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    for retained in [false, true] {
        small = limits();
        if retained {
            small.segment.max_retained_artifact_bytes = 0;
        } else {
            small.segment.max_decoded_payload_bytes = 0;
        }
        let snapshot = storage.primary_audit_snapshot(small).unwrap();
        let error = snapshot
            .scan(&|| false, |_| panic!("limited scan visited rows"))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
    small = limits();
    small.segment.max_segment_rows = 3;
    let snapshot = storage.primary_audit_snapshot(small).unwrap();
    write(&mut storage, &rows(1), false);
    assert_eq!(
        snapshot
            .scan(&|| false, |_| panic!("grown segment exceeds work limit"))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn incorrect_bounds_never_silently_prune_rows_and_changed_commitments_fail() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(3), true);
    let index = storage
        .catalog
        .segments
        .iter()
        .position(|d| d.row_count > 0)
        .unwrap();
    let original = storage.catalog.segments[index].clone();
    storage.catalog.segments[index].min_block = Some(999_999);
    storage.catalog.segments[index].max_block = Some(999_999);
    let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
    let mut visited = 0;
    let error = snapshot
        .scan(&|| false, |batch| {
            visited += batch.rows().len();
            Ok(())
        })
        .unwrap_err();
    assert_eq!(visited, 3);
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    storage.catalog.segments[index] = original;
    storage.catalog.segments[index].source_commitment = Some(B256::repeat_byte(0xff));
    assert_eq!(
        collect(&storage.primary_audit_snapshot(limits()).unwrap())
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    storage.catalog.segments[index].source_namespace = None;
    storage.catalog.segments[index].source_commitment = None;
    assert_eq!(
        storage.primary_audit_snapshot(limits()).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn missing_source_never_gets_created_or_repaired_by_a_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(3), true);
    let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
    let id = storage
        .segments()
        .iter()
        .find(|s| s.row_count > 0)
        .unwrap()
        .id;
    fs::remove_dir_all(storage.segment_path(id)).unwrap();
    let before = tree(dir.path());
    assert!(collect(&snapshot).is_err());
    assert_eq!(tree(dir.path()), before);
}

#[test]
fn recovery_required_capture_is_rejected_without_storage_changes() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    storage.recovery_required = true;
    let before = tree(dir.path());
    assert!(storage.primary_audit_snapshot(limits()).is_err());
    assert_eq!(tree(dir.path()), before);
}

#[test]
fn callback_append_preserves_existing_rows_without_extending_the_scan() {
    for bundled in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = NativeStorage::open(config(dir.path())).unwrap();
        write(&mut storage, &rows(10), bundled);
        let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
        let mut captured = Vec::new();
        let mut appended = false;
        let report = snapshot
            .scan(&|| false, |batch| {
                captured.extend(batch.rows().map(|v| v.row.clone()));
                if !appended {
                    write(&mut storage, &rows(9), bundled);
                    appended = true;
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(report.physical_rows, 10);
        assert_eq!(captured, rows(10));
        assert_eq!(storage.total_rows(), 19);
    }
}

#[test]
fn derived_index_files_are_never_a_source_or_filter_for_audit_rows() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(10), true);
    for segment in storage.segments() {
        let indexes = storage.segment_path(segment.id).join("indexes");
        fs::create_dir_all(&indexes).unwrap();
        fs::write(
            indexes.join("block_number.btree"),
            b"deliberately invalid derived index",
        )
        .unwrap();
        fs::write(indexes.join("address.btree"), []).unwrap();
    }
    let before = tree(dir.path());
    let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
    assert_eq!(
        collect(&snapshot)
            .unwrap()
            .1
            .iter()
            .map(|v| v.3.clone())
            .collect::<Vec<_>>(),
        rows(10)
    );
    assert_eq!(tree(dir.path()), before);
}

#[test]
fn source_corruption_after_capture_is_rejected_without_repair() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(3), false);
    let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
    let id = storage
        .segments()
        .iter()
        .find(|d| d.row_count > 0)
        .unwrap()
        .id;
    let path = storage.segment_path(id).join("address.col");
    let mut bytes = fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&path, bytes).unwrap();
    let before = tree(dir.path());
    assert_eq!(
        collect(&snapshot).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(tree(dir.path()), before);
}

#[test]
fn failed_callback_after_reorg_is_reported_as_invalidated_view() {
    let dir = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(config(dir.path())).unwrap();
    write(&mut storage, &rows(3), false);
    let snapshot = storage.primary_audit_snapshot(limits()).unwrap();
    let error = snapshot
        .scan(&|| false, |batch| {
            storage.mark_non_canonical(batch.rows().next().unwrap().row.block_hash)?;
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "source unavailable during reorg",
            ))
        })
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
}
