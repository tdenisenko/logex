//! Actual row-boundary fixture for bounded repair ownership, not chain proof.
use super::*;
use crate::native::{
    InspectionLimits, NativeStorage, NativeStorageConfig, PrimaryDataDisposition,
    inspect_primary_data,
};
use alloy_primitives::{Address, B256, Bytes};
use logex_types::{LogRow, Source};

fn boundary_fixture(preserve_orphan_trace: bool) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let mut rows = Vec::new();
    for (offset, count) in [2, 3, 3, 3, 1].into_iter().enumerate() {
        let height = 10 + offset as u64;
        for index in 0..count {
            rows.push(LogRow {
                block_number: height,
                block_hash: B256::repeat_byte(height as u8),
                timestamp: 100 + height,
                tx_hash: B256::repeat_byte(height as u8),
                tx_index: 0,
                log_index: index,
                address: Address::repeat_byte(7),
                topic0: None,
                topic1: None,
                topic2: None,
                topic3: None,
                data: Bytes::from(vec![height as u8, index as u8]),
                data_len: 2,
                source: if preserve_orphan_trace && height == 13 && index == 0 {
                    Source::Trace
                } else {
                    Source::Receipt
                },
            });
        }
    }
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        hot_target_rows: 3,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(&rows).unwrap();
    if preserve_orphan_trace {
        assert_eq!(
            storage.mark_non_canonical(B256::repeat_byte(13)).unwrap(),
            3
        );
    }
    storage.checkpoint_durable().unwrap();
    drop(storage);
    tmp
}

fn inspect(path: &std::path::Path) -> PrimaryDataInspection {
    inspect_primary_data(
        path,
        InspectionLimits {
            max_segment_rows: 100,
            max_retained_artifact_bytes: 16 * 1024 * 1024,
            max_decoded_payload_bytes: 1024 * 1024,
        },
    )
    .unwrap()
}

#[test]
fn repair_seed_range_does_not_cascade_through_healthy_neighbor_rows() {
    let tmp = boundary_fixture(false);
    let inspected = inspect(tmp.path());
    assert!(inspected.recovery_prerequisites.is_empty());
    assert!(inspected.segments.iter().all(|segment| matches!(
        segment.disposition,
        PrimaryDataDisposition::CommitmentVerified
    )));
    let descriptors: Vec<_> = inspected
        .catalog
        .segments
        .iter()
        .filter(|segment| segment.row_count != 0)
        .collect();
    assert_eq!(
        descriptors
            .iter()
            .map(|segment| (segment.min_block, segment.max_block, segment.row_count))
            .collect::<Vec<_>>(),
        vec![
            (Some(10), Some(11), 3),
            (Some(11), Some(12), 3),
            (Some(12), Some(13), 3),
            (Some(13), Some(14), 3),
        ]
    );
    let seed = descriptors[1].id;
    let expected_owners: Vec<_> = descriptors[..3].iter().map(|segment| segment.id).collect();
    // Designate the middle segment for reconstruction without corrupting it:
    // all originals remain available to prove this is selection-policy behavior.
    // Neighbor rows at heights 10/13 can be preserved locally; selecting a neighbor
    // must not turn its unaffected height 13 into another fetch/ownership seed.
    let plan = inspected
        .into_repair_plan(
            &[seed],
            RepairPlanLimits {
                max_segments: 10,
                max_blocks: 100,
                max_segment_rows: 100,
                max_canonical_artifact_bytes: 1024 * 1024,
                max_candidate_data_bytes: 1024,
            },
        )
        .unwrap();
    assert_eq!(plan.segment_ids(), expected_owners);
    assert_eq!(plan.block_ranges(), &[(11, 12)]);
}

#[test]
fn bounded_seed_plan_preserves_local_rows_and_flags_outside_fetch_ranges() {
    let tmp = boundary_fixture(true);
    let before = super::tests::tree(tmp.path());
    let inspected = inspect(tmp.path());
    let seed = inspected.catalog.segments[1].id;
    let plan = inspected
        .into_repair_plan(
            &[seed],
            RepairPlanLimits {
                max_segments: 3,
                max_blocks: 2,
                max_segment_rows: 3,
                max_canonical_artifact_bytes: 1024 * 1024,
                max_candidate_data_bytes: 6,
            },
        )
        .unwrap();
    assert_eq!(plan.block_ranges(), &[(11, 12)]);
    assert_eq!(plan.segment_ids().len(), 3);

    // This tests exact local preservation, not an authenticated assembler.
    // A future assembler must carry these unaffected rows forward and prove
    // the same complete original segment, including noncanonical Trace rows.
    let mut outside = Vec::new();
    for &id in plan.segment_ids() {
        let reader =
            SegmentReader::open_for_inspection(&plan.inspection.paths.segment_dir(id)).unwrap();
        let rows = reader.read_log_rows(None).unwrap();
        let mut candidate = plan.begin_candidate(id).unwrap();
        candidate.append(&rows).unwrap();
        let verified = candidate.finish().unwrap();
        assert_eq!(verified.descriptor().row_count, 3);
        for (position, row) in rows.iter().enumerate() {
            if !(11..=12).contains(&row.block_number) {
                outside.push((
                    row.block_number,
                    row.source,
                    verified.canonical().is_present(position as u64),
                ));
            }
        }
    }
    assert_eq!(
        outside,
        vec![
            (10, Source::Receipt, true),
            (10, Source::Receipt, true),
            (13, Source::Trace, false),
        ]
    );
    assert_eq!(super::tests::tree(tmp.path()), before);
    drop(plan);
    assert_eq!(super::tests::tree(tmp.path()), before);
}
