//! Disposable storage fixtures; original directories are never replaced.
use super::super::tests::{data_id, fixture, inspect, inspection_limits, limits, tree};
use super::*;
use crate::native::{NativeStorage, NativeStorageConfig};

#[test]
fn staging_preserves_raw_and_bundled_rows_flags_identity_and_original_files() {
    for bundled in [false, true] {
        let (original, rows) = fixture(bundled, if bundled { 6 } else { 100 }, true);
        let before = tree(original.path());
        let report = inspect(original.path());
        let id = data_id(&report);
        let plan = report.into_repair_plan(&[id], limits()).unwrap();
        let mut verifier = plan.begin_candidate(id).unwrap();
        verifier.append(&rows).unwrap();
        let candidate = verifier.finish().unwrap();
        let parent = tempfile::tempdir().unwrap();
        let destination = parent.path().join("replacement");
        let parent_before = tree(parent.path());
        let estimate = candidate.estimate_staging_bytes(&rows).unwrap();
        assert!(!destination.exists());
        assert_eq!(tree(parent.path()), parent_before);
        assert_eq!(tree(original.path()), before);
        let staged = candidate
            .stage(&destination, &rows, inspection_limits())
            .unwrap();
        assert!(stage_file_bytes(&destination) <= estimate);
        assert_bundle_estimate(&staged, estimate);
        assert_eq!(
            staged.descriptor.source_namespace,
            candidate.descriptor().source_namespace
        );
        assert_eq!(
            staged.descriptor.source_commitment,
            candidate.descriptor().source_commitment
        );
        assert_eq!(
            staged.descriptor.source_state,
            candidate.descriptor().source_state
        );
        assert_eq!(staged.descriptor.id, id);
        assert_eq!(staged.descriptor.kind, candidate.descriptor().kind);
        assert_eq!(staged.descriptor.generation, 0);
        assert!(staged.descriptor.column_bundle.is_some());
        let reader = SegmentReader::open_for_inspection(&staged.segment_dir()).unwrap();
        let flags = reader.read_canonical().unwrap();
        let ids: Vec<_> = (0..rows.len() as u32).collect();
        let actual: Vec<_> = reader
            .log_row_batches(&ids)
            .unwrap()
            .flat_map(Result::unwrap)
            .collect();
        assert_eq!(actual, rows);
        assert_eq!(
            (0..flags.len())
                .map(|i| flags.is_present(i))
                .collect::<Vec<_>>(),
            vec![false, false, true, true, true, true]
        );
        drop(candidate);
        staged.verify().unwrap();
        assert!(
            NativeStorage::open(NativeStorageConfig {
                data_dir: original.path().to_owned(),
                ..Default::default()
            })
            .is_err()
        );
        assert!(!destination.join("catalog.json").exists());
        assert_eq!(tree(original.path()), before);
        drop(staged);
        assert!(
            destination.exists(),
            "staging retention belongs to the coordinator"
        );
    }
}

#[test]
fn staging_rechecks_detached_input_before_creating_a_destination() {
    let (original, rows) = fixture(false, 100, false);
    let before = tree(original.path());
    let report = inspect(original.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    for case in 0..3 {
        let mut altered = rows.clone();
        match case {
            0 => {
                altered.pop();
            }
            1 => altered.swap(0, 1),
            _ => altered[0].timestamp += 1,
        }
        let destination = parent.path().join(format!("case-{case}"));
        assert!(candidate.estimate_staging_bytes(&altered).is_err());
        assert!(!destination.exists());
        assert!(
            candidate
                .stage(&destination, &altered, inspection_limits())
                .is_err()
        );
        assert!(!destination.exists());
    }
    assert_eq!(tree(original.path()), before);
}

#[test]
fn staging_refuses_existing_destinations_and_does_not_create_ancestors() {
    let (original, rows) = fixture(false, 100, false);
    let report = inspect(original.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    fs::create_dir(parent.path().join("directory")).unwrap();
    fs::write(parent.path().join("directory/retained"), b"existing stage").unwrap();
    fs::write(parent.path().join("file"), b"existing file").unwrap();
    let before = tree(parent.path());
    for name in ["directory", "file", "missing/child"] {
        assert!(
            candidate
                .stage(&parent.path().join(name), &rows, inspection_limits())
                .is_err()
        );
        assert_eq!(tree(parent.path()), before);
    }
}

#[test]
fn staging_read_limits_refuse_work_and_retain_partial_outputs() {
    let (original, rows) = fixture(false, 100, false);
    let before = tree(original.path());
    let report = inspect(original.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    for case in 0..3 {
        let mut read = inspection_limits();
        match case {
            0 => read.max_segment_rows = rows.len() as u64 - 1,
            1 => read.max_retained_artifact_bytes = 0,
            _ => read.max_decoded_payload_bytes = 0,
        }
        let destination = parent.path().join(format!("case-{case}"));
        assert!(candidate.stage(&destination, &rows, read).is_err());
        assert_eq!(destination.exists(), case != 0);
        if case != 0 {
            let retained = tree(&destination);
            assert!(
                candidate
                    .stage(&destination, &rows, inspection_limits())
                    .is_err()
            );
            assert_eq!(tree(&destination), retained);
        }
    }
    assert_eq!(tree(original.path()), before);
}

#[test]
fn staging_reread_detects_changed_manifest_bounds_and_incomplete_bundle() {
    let (original, rows) = fixture(false, 100, false);
    let before = tree(original.path());
    let report = inspect(original.path());
    let id = data_id(&report);
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    let staged = candidate
        .stage(
            &parent.path().join("replacement"),
            &rows,
            inspection_limits(),
        )
        .unwrap();
    let manifest_path = staged.paths.segment_manifest_path(id);
    let original_manifest = fs::read(&manifest_path).unwrap();
    let mut manifest = staged.manifest.clone();
    manifest.max_block = Some(100);
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
    assert!(
        staged
            .verify()
            .unwrap_err()
            .to_string()
            .contains("manifest differs")
    );
    fs::write(&manifest_path, original_manifest).unwrap();
    staged.verify().unwrap();
    let bundle = crate::column_artifact::bundle_path(&staged.segment_dir(), 0);
    let file = fs::OpenOptions::new().write(true).open(bundle).unwrap();
    file.set_len(file.metadata().unwrap().len() - 1).unwrap();
    assert!(staged.verify().is_err());
    assert_eq!(tree(original.path()), before);
}

#[test]
fn staging_an_empty_original_segment_keeps_its_empty_identity() {
    let original = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: original.path().to_owned(),
        ..Default::default()
    })
    .unwrap();
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let before = tree(original.path());
    let report = inspect(original.path());
    let id = report.catalog.active_hot_segment.unwrap();
    let plan = report.into_repair_plan(&[id], limits()).unwrap();
    let candidate = plan.begin_candidate(id).unwrap().finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    let parent_before = tree(parent.path());
    let estimate = candidate.estimate_staging_bytes(&[]).unwrap();
    assert_eq!(tree(parent.path()), parent_before);
    assert_eq!(tree(original.path()), before);
    let staged = candidate
        .stage(&parent.path().join("empty"), &[], inspection_limits())
        .unwrap();
    staged.verify().unwrap();
    assert_eq!(staged.descriptor.row_count, 0);
    assert!(staged.descriptor.min_block.is_none());
    assert!(staged.descriptor.max_block.is_none());
    assert!(stage_file_bytes(&parent.path().join("empty")) <= estimate);
    assert_bundle_estimate(&staged, estimate);
    assert_eq!(tree(original.path()), before);
}

fn assert_bundle_estimate(staged: &StagedRepairCandidate<'_>, estimate: u64) {
    let bundle = crate::column_artifact::bundle_path(&staged.segment_dir(), 0);
    assert!(fs::metadata(bundle).unwrap().len() <= estimate - SegmentManifest::MAX_BYTES as u64);
}

fn stage_file_bytes(path: &Path) -> u64 {
    tree(path)
        .values()
        .filter_map(|(_, bytes)| bytes.as_ref())
        .map(|bytes| bytes.len() as u64)
        .sum()
}

#[test]
fn staging_estimate_covers_multiple_pages_variable_payload_and_mixed_flags() {
    use alloy_primitives::{Address, B256, Bytes};
    use logex_types::Source;
    let count = crate::page::MAX_PAGE_ROWS as usize + 1;
    let mut seed = 0x1234_5678_9abc_def0u64;
    // An incompressible payload spans more than one bundle extent. The total
    // dataset remains small; the row count crosses exactly one page boundary.
    let large_data: Vec<_> = (0..crate::bundle::MAX_EXTENT_BYTES + 257)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed as u8
        })
        .collect();
    let large_data = Bytes::from(large_data);
    let rows: Vec<_> = (0..count)
        .map(|index| {
            let block = 10 + (index / 10000) as u64;
            let data = if index == 0 {
                large_data.clone()
            } else {
                Bytes::from(vec![index as u8; index % 41])
            };
            LogRow {
                block_number: block,
                block_hash: B256::repeat_byte(block as u8),
                timestamp: 100 + block,
                tx_hash: B256::repeat_byte(index as u8),
                tx_index: 0,
                log_index: index as u32,
                address: Address::repeat_byte(index as u8),
                topic0: (index % 2 == 0).then(|| B256::repeat_byte(1)),
                topic1: (index % 3 == 0).then(|| B256::repeat_byte(2)),
                topic2: (index % 5 == 0).then(|| B256::repeat_byte(3)),
                topic3: (index % 7 == 0).then(|| B256::repeat_byte(4)),
                data_len: data.len() as u32,
                data,
                source: if index % 2 == 0 {
                    Source::Receipt
                } else {
                    Source::Trace
                },
            }
        })
        .collect();
    let original = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: original.path().to_owned(),
        hot_target_rows: count as u64 + 1,
        ..Default::default()
    })
    .unwrap();
    storage.write_batch(&rows).unwrap();
    assert_eq!(
        storage.mark_non_canonical(rows[0].block_hash).unwrap(),
        10000
    );
    storage.checkpoint_durable().unwrap();
    drop(storage);
    let read_limits = InspectionLimits {
        max_segment_rows: count as u64,
        max_retained_artifact_bytes: 32 * 1024 * 1024,
        max_decoded_payload_bytes: 16 * 1024 * 1024,
    };
    let before = tree(original.path());
    let inspection = crate::native::inspect_primary_data(original.path(), read_limits).unwrap();
    let id = data_id(&inspection);
    let plan = inspection
        .into_repair_plan(
            &[id],
            crate::native::RepairPlanLimits {
                max_segment_rows: count as u64,
                max_candidate_data_bytes: 4 * 1024 * 1024,
                ..limits()
            },
        )
        .unwrap();
    let mut verifier = plan.begin_candidate(id).unwrap();
    verifier.append(&rows).unwrap();
    let candidate = verifier.finish().unwrap();
    let parent = tempfile::tempdir().unwrap();
    let destination = parent.path().join("not-created-by-estimate");
    let parent_before = tree(parent.path());
    let estimate = candidate.estimate_staging_bytes(&rows).unwrap();
    assert!(!destination.exists());
    assert_eq!(tree(parent.path()), parent_before);
    assert_eq!(tree(original.path()), before);
    let staged = candidate.stage(&destination, &rows, read_limits).unwrap();
    staged.verify().unwrap();
    assert!(stage_file_bytes(&destination) <= estimate);
    assert_bundle_estimate(&staged, estimate);
    let reference = staged.descriptor.column_bundle.as_ref().unwrap();
    let bundle = crate::bundle::BundleReader::open(
        &crate::column_artifact::bundle_path(&staged.segment_dir(), 0),
        reference,
    )
    .unwrap();
    assert!(
        bundle
            .read_stream(crate::column_artifact::stream_id("columns/data.pages").unwrap())
            .unwrap()
            .len()
            > crate::bundle::MAX_EXTENT_BYTES
    );
    let reader = SegmentReader::open_for_inspection(&staged.segment_dir()).unwrap();
    let flags = reader.read_canonical().unwrap();
    assert!(!flags.is_present(0));
    assert!(flags.is_present(10000));
    assert_eq!(tree(original.path()), before);
}
