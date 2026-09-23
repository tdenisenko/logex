//! Writer-backed local identity controls, not chain-authentication fixtures.
use super::*;
use crate::native::{
    InspectionLimits, NativeStorage, NativeStorageConfig, PrimaryDataDisposition,
    inspect_primary_data,
};
use alloy_primitives::{Address, B256, Bytes};
use logex_types::Source;
use std::{
    fs,
    path::{Path, PathBuf},
    time::SystemTime,
};

fn inspection_limits() -> InspectionLimits {
    InspectionLimits {
        max_segment_rows: 100,
        max_retained_artifact_bytes: 16 * 1024 * 1024,
        max_decoded_payload_bytes: 1024 * 1024,
    }
}
fn limits() -> RepairPlanLimits {
    RepairPlanLimits {
        max_segments: 20,
        max_blocks: 100,
        max_segment_rows: 100,
        max_canonical_artifact_bytes: 1024 * 1024,
        max_candidate_data_bytes: 1024,
    }
}
fn rows() -> Vec<LogRow> {
    (0..6)
        .map(|index| LogRow {
            block_number: 10 + index / 2,
            block_hash: B256::repeat_byte((10 + index / 2) as u8),
            timestamp: 100 + index / 2,
            tx_hash: B256::repeat_byte(index as u8),
            tx_index: index as u32,
            log_index: (index % 2) as u32,
            address: Address::repeat_byte(7),
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Bytes::from(vec![index as u8; 4]),
            data_len: 4,
            source: if index == 4 {
                Source::Trace
            } else {
                Source::Receipt
            },
        })
        .collect()
}
fn fixture(bundled: bool, target: u64, noncanonical: bool) -> (tempfile::TempDir, Vec<LogRow>) {
    let tmp = tempfile::tempdir().unwrap();
    let data = rows();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        hot_target_rows: target,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    if bundled {
        storage.write_historical_batch(&data).unwrap();
    } else {
        storage.write_batch(&data).unwrap();
    }
    if noncanonical {
        assert_eq!(storage.mark_non_canonical(data[0].block_hash).unwrap(), 2);
    }
    storage.checkpoint_durable().unwrap();
    drop(storage);
    (tmp, data)
}
fn inspect(path: &Path) -> PrimaryDataInspection {
    inspect_primary_data(path, inspection_limits()).unwrap()
}
fn data_id(report: &PrimaryDataInspection) -> u64 {
    report
        .catalog
        .segments
        .iter()
        .find(|s| s.row_count > 0)
        .unwrap()
        .id
}
type Tree = BTreeMap<PathBuf, (SystemTime, Option<Vec<u8>>)>;
fn tree(root: &Path) -> Tree {
    fn walk(root: &Path, p: &Path, out: &mut Tree) {
        let m = fs::metadata(p).unwrap();
        out.insert(
            p.strip_prefix(root).unwrap().to_owned(),
            (
                m.modified().unwrap(),
                m.is_file().then(|| fs::read(p).unwrap()),
            ),
        );
        if m.is_dir() {
            for child in fs::read_dir(p).unwrap() {
                walk(root, &child.unwrap().path(), out);
            }
        }
    }
    let mut out = Tree::new();
    walk(root, root, &mut out);
    out
}

// The small fixture stores this stream contiguously. Locate the encoded bytes
// rather than assuming physical stream order; assert this fixture property.
fn unique_stream_offset(path: &Path, reference: &crate::BundleReference, stream: &str) -> usize {
    let reader = crate::bundle::BundleReader::open(path, reference).unwrap();
    let encoded = reader
        .read_stream(crate::column_artifact::stream_id(stream).unwrap())
        .unwrap();
    assert!(!encoded.is_empty());
    let bytes = fs::read(path).unwrap();
    let matches: Vec<_> = bytes[..reference.table_offset as usize]
        .windows(encoded.len())
        .enumerate()
        .filter_map(|(offset, data)| (data == encoded.as_slice()).then_some(offset))
        .collect();
    assert_eq!(
        matches.len(),
        1,
        "fixture stream must have one contiguous physical occurrence"
    );
    matches[0]
}

#[test]
fn repair_exact_candidate_survives_damaged_raw_and_bundled_columns_without_writes() {
    for bundled in [false, true] {
        let (tmp, data) = fixture(bundled, if bundled { 6 } else { 100 }, false);
        let initial = inspect(tmp.path());
        let id = data_id(&initial);
        let d = initial
            .catalog
            .segments
            .iter()
            .find(|s| s.id == id)
            .unwrap();
        assert_eq!(d.row_count, data.len() as u64);
        assert_eq!(d.column_bundle.is_some(), bundled);
        assert!(initial.segments.iter().all(|segment| matches!(
            segment.disposition,
            PrimaryDataDisposition::CommitmentVerified
        )));
        let dir = initial.paths.segment_dir(id);
        let damaged = if bundled {
            crate::column_artifact::bundle_path(&dir, d.generation)
        } else {
            dir.join("block_hash.col")
        };
        let offset = if bundled {
            unique_stream_offset(
                &damaged,
                d.column_bundle.as_ref().unwrap(),
                "columns/block_hash.pages",
            )
        } else {
            crate::ColumnFileHeader::SIZE + 32 * 2
        };
        drop(initial);
        let mut bytes = fs::read(&damaged).unwrap();
        bytes[offset] ^= 0x80;
        fs::write(&damaged, bytes).unwrap();
        let before = tree(tmp.path());
        let report = inspect(tmp.path());
        assert!(!matches!(
            report
                .segments
                .iter()
                .find(|s| s.id == id)
                .unwrap()
                .disposition,
            PrimaryDataDisposition::CommitmentVerified
        ));
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        let mut check = plan.begin_candidate(id).unwrap();
        check.append(&data[..2]).unwrap();
        check.append(&data[2..]).unwrap();
        let exact = check.finish().unwrap();
        assert_eq!(exact.descriptor().row_count, 6);
        assert!((0..6).all(|row| exact.canonical().is_present(row)));
        assert_eq!(
            inspect_primary_data(tmp.path(), inspection_limits())
                .unwrap_err()
                .kind(),
            io::ErrorKind::WouldBlock
        );
        assert_eq!(tree(tmp.path()), before);
        drop(exact);
        drop(plan);
        assert_eq!(tree(tmp.path()), before);
        drop(inspect(tmp.path()));
    }
}

#[test]
fn repair_wrong_rows_fail_and_rejected_append_is_terminal() {
    let (tmp, data) = fixture(false, 100, false);
    let report = inspect(tmp.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    for variant in 0..4 {
        let mut altered = data.clone();
        match variant {
            0 => altered.swap(0, 1),
            1 => altered[2].source = Source::Trace,
            2 => altered[2].data = Bytes::from(vec![99; 4]),
            _ => {
                altered.pop();
            }
        }
        let mut check = plan.begin_candidate(id).unwrap();
        check.append(&altered).unwrap();
        assert!(check.finish().is_err());
    }
    let mut check = plan.begin_candidate(id).unwrap();
    check.append(&data).unwrap();
    assert!(check.append(&data[..1]).is_err());
    assert!(check.append(&[]).is_err());
    assert!(check.finish().is_err());
    let mut altered_length = data.clone();
    altered_length[0].data_len = 3;
    let mut check = plan.begin_candidate(id).unwrap();
    check.append(&altered_length).unwrap();
    assert!(check.finish().is_err());
}

#[test]
fn repair_preserves_original_noncanonical_flags() {
    for bundled in [false, true] {
        let (tmp, data) = fixture(bundled, if bundled { 6 } else { 100 }, true);
        let report = inspect(tmp.path());
        let id = data_id(&report);
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        let mut check = plan.begin_candidate(id).unwrap();
        check.append(&data).unwrap();
        let exact = check.finish().unwrap();
        for row in 0..6 {
            assert_eq!(exact.canonical().is_present(row), row >= 2);
        }
    }
}

#[test]
fn repair_candidate_limits_are_enforced_without_success_after_failure() {
    let (tmp, data) = fixture(false, 100, false);
    for case in 0..3 {
        let report = inspect(tmp.path());
        let id = data_id(&report);
        let mut budget = limits();
        match case {
            0 => budget.max_segment_rows = 5,
            1 => budget.max_canonical_artifact_bytes = 0,
            _ => budget.max_candidate_data_bytes = 23,
        }
        let plan = report.into_repair_plan(&[id], budget).unwrap();
        if case < 2 {
            assert!(plan.begin_candidate(id).is_err());
        } else {
            let mut check = plan.begin_candidate(id).unwrap();
            check.append(&data[..3]).unwrap();
            assert!(check.append(&data[3..]).is_err());
            assert!(check.finish().is_err());
        }
    }
    let report = inspect(tmp.path());
    let id = data_id(&report);
    let mut budget = limits();
    budget.max_candidate_data_bytes = 24;
    let plan = report.into_repair_plan(&[id], budget).unwrap();
    let mut check = plan.begin_candidate(id).unwrap();
    check.append(&data).unwrap();
    check.finish().unwrap();
}

#[test]
fn repair_report_tampering_and_pending_recovery_cannot_authorize_plan() {
    let (tmp, _) = fixture(false, 100, false);
    let mut report = inspect(tmp.path());
    let id = data_id(&report);
    report
        .catalog
        .segments
        .iter_mut()
        .find(|s| s.id == id)
        .unwrap()
        .row_count += 1;
    assert!(report.into_repair_plan(&[id], limits()).is_err());
    let wal = tmp.path().join("wal/ingestion.json");
    fs::write(&wal, b"retained pending transaction").unwrap();
    let before = tree(tmp.path());
    let mut report = inspect(tmp.path());
    report.recovery_prerequisites.clear();
    report.segments.clear();
    assert_eq!(
        report.into_repair_plan(&[id], limits()).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn repair_real_row_splits_expand_only_overlapping_owners() {
    let (tmp, _) = fixture(false, 1, false);
    let report = inspect(tmp.path());
    let owners: Vec<_> = report
        .catalog
        .segments
        .iter()
        .filter(|s| s.min_block == Some(10))
        .map(|s| s.id)
        .collect();
    assert_eq!(owners.len(), 2);
    let plan = report.into_repair_plan(&owners[..1], limits()).unwrap();
    assert_eq!(plan.groups().len(), 1);
    assert_eq!(plan.groups()[0].block_range(), Some((10, 10)));
    assert_eq!(plan.groups()[0].segment_ids(), owners.as_slice());
}

#[test]
fn repair_empty_segment_has_no_invented_block_coverage() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    drop(storage);
    let before = tree(tmp.path());
    let report = inspect(tmp.path());
    let catalog = report.catalog.clone();
    let id = catalog.segments[0].id;
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    assert_eq!(plan.groups()[0].block_range(), None);
    plan.begin_candidate(id).unwrap().finish().unwrap();
    assert_eq!(plan.catalog(), &catalog);
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn repair_zero_log_progress_is_preserved_without_inventing_segment_range() {
    let tmp = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    let floor = alloy_consensus::Header {
        number: 50,
        timestamp: 100,
        ..Default::default()
    };
    // Exercise storage's zero-row progress contract, not consensus authentication.
    storage.ingest_historical_batch(&[], &floor).unwrap();
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let before = tree(tmp.path());
    let report = inspect(tmp.path());
    assert_eq!(
        report.catalog.state.historical_floor_header.as_ref(),
        Some(&floor)
    );
    let original = report.catalog.clone();
    let ids: Vec<_> = original.segments.iter().map(|s| s.id).collect();
    let plan = report.into_repair_plan(&ids, limits()).unwrap();
    for group in plan.groups() {
        assert_eq!(group.block_range(), None);
    }
    assert_eq!(plan.catalog(), &original);
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn repair_missing_or_corrupt_canonical_evidence_blocks_candidate_without_writes() {
    for case in 0..3 {
        let bundled = case == 2;
        let (tmp, _) = fixture(bundled, if bundled { 6 } else { 100 }, false);
        let initial = inspect(tmp.path());
        let id = data_id(&initial);
        let descriptor = initial
            .catalog
            .segments
            .iter()
            .find(|segment| segment.id == id)
            .unwrap();
        let dir = initial.paths.segment_dir(id);
        let path = if bundled {
            crate::column_artifact::bundle_path(&dir, descriptor.generation)
        } else {
            dir.join("canonical.bitmap")
        };
        let offset = if bundled {
            unique_stream_offset(
                &path,
                descriptor.column_bundle.as_ref().unwrap(),
                "canonical.bitmap",
            )
        } else {
            fs::read(&path).unwrap().len() - 1
        };
        drop(initial);
        if case == 0 {
            fs::remove_file(&path).unwrap();
        } else {
            let mut bytes = fs::read(&path).unwrap();
            bytes[offset] ^= 0x80;
            fs::write(&path, bytes).unwrap();
        }
        let before = tree(tmp.path());
        let report = inspect(tmp.path());
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        assert!(plan.begin_candidate(id).is_err());
        assert_eq!(tree(tmp.path()), before);
        drop(plan);
        assert_eq!(tree(tmp.path()), before);
    }
}

#[test]
fn repair_candidate_cannot_escape_selected_component() {
    let (tmp, _) = fixture(false, 1, false);
    let report = inspect(tmp.path());
    let selected = report
        .catalog
        .segments
        .iter()
        .find(|segment| segment.min_block == Some(10))
        .unwrap()
        .id;
    let other = report
        .catalog
        .segments
        .iter()
        .find(|segment| segment.min_block == Some(11))
        .unwrap()
        .id;
    let plan = report.into_repair_plan(&[selected], limits()).unwrap();
    assert!(plan.begin_candidate(other).is_err());
    assert!(plan.begin_candidate(u64::MAX).is_err());
}
