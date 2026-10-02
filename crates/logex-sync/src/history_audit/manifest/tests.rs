use super::*;
use alloy_primitives::{Address, Bytes};
use logex_storage::native::{
    InspectionLimits, NativeStorage, NativeStorageConfig, PrimaryAuditLimits,
};
use logex_types::Source;
use std::{cell::Cell, collections::BTreeMap};

fn source_limits() -> PrimaryAuditLimits {
    PrimaryAuditLimits {
        max_segments: 1000,
        max_total_rows: 10_000,
        segment: InspectionLimits {
            max_segment_rows: 1000,
            max_retained_artifact_bytes: 16 * 1024 * 1024,
            max_decoded_payload_bytes: 16 * 1024 * 1024,
        },
    }
}
fn limits() -> AuditManifestLimits {
    AuditManifestLimits {
        sort_records: 2,
        merge_fan_in: 2,
        max_scratch_bytes: 1024 * 1024,
        max_runs: 10_000,
    }
}
fn storage(path: &Path) -> NativeStorage {
    NativeStorage::open(NativeStorageConfig {
        data_dir: path.to_owned(),
        hot_target_rows: 3,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap()
}
fn row(block: u64, index: u32) -> LogRow {
    LogRow {
        block_number: block,
        block_hash: B256::repeat_byte(block as u8),
        timestamp: block * 10,
        tx_hash: B256::repeat_byte(index as u8),
        tx_index: index / 2,
        log_index: index,
        address: Address::repeat_byte(42),
        topic0: Some(B256::repeat_byte(1)),
        topic1: Some(B256::repeat_byte(2)),
        topic2: None,
        topic3: None,
        data: Bytes::from_static(b"event"),
        data_len: 5,
        source: Source::Receipt,
    }
}
fn write(storage: &mut NativeStorage, rows: &[LogRow]) {
    storage.write_batch(rows).unwrap();
    storage.checkpoint_durable().unwrap();
}
fn build(storage: &NativeStorage, scratch: &Path) -> io::Result<AuditManifest> {
    AuditManifest::build(
        storage.primary_audit_snapshot(source_limits())?,
        AuditRange {
            from: 0,
            through: 200,
        },
        scratch,
        limits(),
        &|| false,
    )
}
fn tree(root: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Vec<u8>>) {
        for entry in fs::read_dir(dir).unwrap() {
            let p = entry.unwrap().path();
            if p.is_dir() {
                visit(root, &p, out);
            } else {
                out.insert(
                    p.strip_prefix(root).unwrap().to_owned(),
                    fs::read(p).unwrap(),
                );
            }
        }
    }
    let mut out = BTreeMap::new();
    visit(root, root, &mut out);
    out
}
fn rows_digest(rows: &[LogRow]) -> B256 {
    let mut hash = run_hasher();
    for row in rows {
        hash_row(&mut hash, row).unwrap();
    }
    B256::from(*hash.finalize().as_bytes())
}

#[test]
fn external_merge_keeps_every_occurrence_and_actual_row_fields_without_source_writes() {
    let data = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut storage = storage(data.path());
    // Deliberately different physical/segment ordering, with block runs split.
    let physical = vec![
        row(101, 2),
        row(100, 1),
        row(102, 0),
        row(100, 0),
        row(101, 0),
        row(101, 1),
        row(100, 2),
        row(102, 1),
    ];
    write(&mut storage, &physical);
    let before = tree(data.path());
    let manifest = build(&storage, scratch.path()).unwrap();
    assert_eq!(tree(data.path()), before);
    assert_eq!(manifest.summary().physical_rows, 8);
    assert_eq!(manifest.summary().canonical_rows, 8);
    assert_eq!(manifest.summary().selected_rows, 8);
    assert_eq!(manifest.summary().nonempty_blocks, 3);
    let expected: BTreeMap<_, _> = physical
        .iter()
        .map(|r| ((r.block_number, r.log_index), r))
        .collect();
    let mut reader = manifest.reader().unwrap();
    let mut seen = Vec::new();
    while let Some(run) = reader.next_run().unwrap() {
        let mut hash = run_hasher();
        for index in u64::from(run.first_log)..run.end_log().unwrap() {
            let r = expected[&(run.block, index as u32)];
            hash_row(&mut hash, r).unwrap();
            seen.push((run.block, index as u32));
        }
        assert_eq!(run.digest, B256::from(*hash.finalize().as_bytes()));
    }
    assert_eq!(
        seen,
        vec![
            (102, 0),
            (102, 1),
            (101, 0),
            (101, 1),
            (101, 2),
            (100, 0),
            (100, 1),
            (100, 2)
        ]
    );
    assert!(manifest.summary().peak_scratch_bytes <= limits().max_scratch_bytes);
    assert_eq!(fs::read_dir(manifest._scratch.path()).unwrap().count(), 1);
    drop(manifest);
    assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
}

#[test]
fn duplicate_gap_nonzero_start_and_two_canonical_hashes_fail_closed_across_segments() {
    let mut other_hash = row(100, 1);
    other_hash.block_hash = B256::repeat_byte(255);
    for rows in [
        vec![row(100, 0), row(100, 1), row(100, 0)],
        vec![row(100, 0), row(100, 2)],
        vec![row(100, 1)],
        vec![row(100, 0), other_hash],
    ] {
        let data = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let mut storage = storage(data.path());
        write(&mut storage, &rows);
        let before = tree(data.path());
        assert_eq!(
            build(&storage, scratch.path()).err().unwrap().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(tree(data.path()), before);
        assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
    }
}

#[test]
fn canonical_membership_and_actual_bounds_are_explicit_without_deduplication() {
    let data = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut storage = storage(data.path());
    let rows = vec![row(99, 0), row(100, 0), row(101, 0), row(102, 0)];
    write(&mut storage, &rows);
    storage.mark_non_canonical(rows[2].block_hash).unwrap();
    let manifest = AuditManifest::build(
        storage.primary_audit_snapshot(source_limits()).unwrap(),
        AuditRange {
            from: 100,
            through: 101,
        },
        scratch.path(),
        limits(),
        &|| false,
    )
    .unwrap();
    assert_eq!(manifest.summary().physical_rows, 4);
    assert_eq!(manifest.summary().canonical_rows, 3);
    assert_eq!(manifest.summary().selected_rows, 1);
    assert_eq!(manifest.summary().nonempty_blocks, 1);
    let run = manifest.reader().unwrap().next_run().unwrap().unwrap();
    assert_eq!(run.block, 100);
    assert_eq!(run.digest, rows_digest(&[row(100, 0)]));
}

#[test]
fn empty_storage_has_a_complete_empty_local_manifest_but_no_receipt_claim() {
    let data = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let storage = storage(data.path());
    let manifest = build(&storage, scratch.path()).unwrap();
    assert_eq!(manifest.summary().selected_rows, 0);
    assert_eq!(manifest.summary().runs, 0);
    assert!(manifest.reader().unwrap().next_run().unwrap().is_none());
}

#[test]
fn cancellations_limits_and_invalidated_sources_leave_no_partial_manifest() {
    let data = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut storage = storage(data.path());
    write(
        &mut storage,
        &(100..110).map(|b| row(b, 0)).collect::<Vec<_>>(),
    );
    for limit in [
        AuditManifestLimits {
            max_runs: 1,
            ..limits()
        },
        AuditManifestLimits {
            max_scratch_bytes: RECORD_BYTES,
            ..limits()
        },
        AuditManifestLimits {
            merge_fan_in: 1,
            ..limits()
        },
        AuditManifestLimits {
            sort_records: 0,
            ..limits()
        },
    ] {
        let error = AuditManifest::build(
            storage.primary_audit_snapshot(source_limits()).unwrap(),
            AuditRange {
                from: 0,
                through: 200,
            },
            scratch.path(),
            limit,
            &|| false,
        )
        .err()
        .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
    }
    let polls = Cell::new(0);
    let result = AuditManifest::build(
        storage.primary_audit_snapshot(source_limits()).unwrap(),
        AuditRange {
            from: 0,
            through: 200,
        },
        scratch.path(),
        limits(),
        &|| {
            polls.set(polls.get() + 1);
            polls.get() > 12
        },
    );
    assert_eq!(result.err().unwrap().kind(), io::ErrorKind::Interrupted);
    assert_eq!(fs::read_dir(scratch.path()).unwrap().count(), 0);
    let old = storage.primary_audit_snapshot(source_limits()).unwrap();
    storage.mark_non_canonical(row(100, 0).block_hash).unwrap();
    assert_eq!(
        AuditManifest::build(
            old,
            AuditRange {
                from: 0,
                through: 200
            },
            scratch.path(),
            limits(),
            &|| false
        )
        .err()
        .unwrap()
        .kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn later_appends_are_excluded_but_reorg_and_close_invalidate_admission() {
    let data = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut storage = storage(data.path());
    write(&mut storage, &[row(100, 0)]);
    let snapshot = storage.primary_audit_snapshot(source_limits()).unwrap();
    write(&mut storage, &[row(101, 0)]);
    let manifest = AuditManifest::build(
        snapshot,
        AuditRange {
            from: 0,
            through: 200,
        },
        scratch.path(),
        limits(),
        &|| false,
    )
    .unwrap();
    assert_eq!(manifest.summary().selected_rows, 1);
    storage.mark_non_canonical(row(100, 0).block_hash).unwrap();
    assert_eq!(
        manifest.validate().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    let current = build(&storage, scratch.path()).unwrap();
    drop(storage);
    assert_eq!(
        current.validate().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn every_persisted_field_and_nullable_topic_changes_the_event_commitment() {
    let original = row(100, 0);
    let digest = rows_digest(std::slice::from_ref(&original));
    let mut variants = Vec::new();
    macro_rules! changed {
        ($field:ident, $value:expr) => {{
            let mut row = original.clone();
            row.$field = $value;
            variants.push(row);
        }};
    }
    changed!(block_number, 101);
    changed!(block_hash, B256::repeat_byte(255));
    changed!(timestamp, 1001);
    changed!(tx_hash, B256::repeat_byte(254));
    changed!(tx_index, 1);
    changed!(log_index, 1);
    changed!(address, Address::repeat_byte(43));
    changed!(topic0, None);
    changed!(topic1, None);
    changed!(topic2, Some(B256::ZERO));
    changed!(topic3, Some(B256::ZERO));
    changed!(data, Bytes::from_static(b"evEnt"));
    changed!(source, Source::Trace);
    for variant in variants {
        assert_ne!(rows_digest(&[variant]), digest);
    }
    let mut invalid = original;
    invalid.data_len += 1;
    assert_eq!(
        hash_row(&mut run_hasher(), &invalid).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}

#[test]
fn corrupt_truncated_appended_and_unsorted_runs_never_recover_after_error() {
    for mode in 0..4 {
        let data = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let mut storage = storage(data.path());
        write(&mut storage, &[row(100, 0), row(101, 0)]);
        let manifest = build(&storage, scratch.path()).unwrap();
        let original = fs::read(&manifest.part.path).unwrap();
        let mut bytes = original.clone();
        match mode {
            0 => bytes[64] ^= 1,
            1 => {
                bytes.pop();
            }
            2 => bytes.push(0),
            _ => bytes.rotate_left(RECORD_BYTES as usize),
        }
        fs::write(&manifest.part.path, &bytes).unwrap();
        match manifest.reader() {
            Err(_) => assert!(mode == 1 || mode == 2),
            Ok(mut reader) => {
                loop {
                    match reader.next_run() {
                        Ok(Some(_)) => {}
                        Ok(None) => panic!("corrupt scratch accepted"),
                        Err(_) => break,
                    }
                }
                fs::write(&manifest.part.path, &original).unwrap();
                assert!(reader.next_run().is_err(), "failure must poison the reader");
            }
        }
    }
}

#[test]
fn incremental_merging_bounds_live_scratch_files_even_with_one_record_buffers() {
    let dir = tempfile::tempdir().unwrap();
    let mut sorter = Sorter::new(
        dir.path().to_owned(),
        AuditManifestLimits {
            sort_records: 1,
            ..limits()
        },
    )
    .unwrap();
    for block in 0..200 {
        let r = row(block, 0);
        let mut pending = PendingRun::new(block, 0, &r);
        pending.append(&r).unwrap();
        sorter.push(pending.finish(), &|| false).unwrap();
        assert!(sorter.parts.len() <= 8);
        assert_eq!(
            fs::read_dir(dir.path()).unwrap().count(),
            sorter.parts.len()
        );
    }
    let (part, _) = sorter.finish(&|| false).unwrap();
    assert_eq!(
        validate_order(
            &part,
            AuditRange {
                from: 0,
                through: 199
            },
            &|| false
        )
        .unwrap(),
        (200, 200)
    );
}
