use super::super::tests::tree;
use super::*;
use crate::native::{InspectionLimits, NativeStorage, NativeStorageConfig, inspect_primary_data};
use alloy_primitives::{Address, Bytes};
use std::{fs, path::Path};

fn reads() -> RepairReadLimits {
    RepairReadLimits {
        max_routing_artifact_bytes: 1 << 20,
        max_carry_artifact_bytes: 1 << 20,
        max_carry_decoded_payload_bytes: 1 << 20,
        max_carry_data_bytes: 100,
    }
}
fn inspection(path: &Path) -> PrimaryDataInspection {
    inspect_primary_data(
        path,
        InspectionLimits {
            max_segment_rows: 100,
            max_retained_artifact_bytes: 1 << 20,
            max_decoded_payload_bytes: 1 << 20,
        },
    )
    .unwrap()
}
fn plan_limits() -> super::super::RepairPlanLimits {
    super::super::RepairPlanLimits {
        max_segments: 10,
        max_blocks: 100,
        max_segment_rows: 100,
        max_canonical_artifact_bytes: 1 << 20,
        max_candidate_data_bytes: 100,
    }
}
fn fixture(bundle: bool, mixed: bool) -> (tempfile::TempDir, Vec<LogRow>) {
    let tmp = tempfile::tempdir().unwrap();
    let mut rows: Vec<_> = (0..6)
        .map(|i| LogRow {
            block_number: 10 + i / 2,
            block_hash: B256::repeat_byte((10 + i / 2) as u8),
            timestamp: 100 + i / 2,
            tx_hash: B256::repeat_byte(i as u8),
            tx_index: 0,
            log_index: (i % 2) as u32,
            address: Address::ZERO,
            topic0: None,
            topic1: None,
            topic2: None,
            topic3: None,
            data: Bytes::from(vec![i as u8; 4]),
            data_len: 4,
            source: Source::Receipt,
        })
        .collect();
    if mixed {
        rows[2].source = Source::Trace;
    }
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: tmp.path().to_owned(),
        hot_target_rows: if mixed {
            3
        } else if bundle {
            6
        } else {
            100
        },
        ..Default::default()
    })
    .unwrap();
    if bundle {
        storage.write_historical_batch(&rows).unwrap();
    } else {
        storage.write_batch(&rows).unwrap();
    }
    if mixed {
        storage.mark_non_canonical(rows[0].block_hash).unwrap();
    }
    storage.checkpoint_durable().unwrap();
    drop(storage);
    (tmp, rows)
}
#[test]
fn preparation_skips_replaceable_raw_and_bundle_payload_damage() {
    for bundled in [false, true] {
        let (tmp, rows) = fixture(bundled, false);
        let report = inspection(tmp.path());
        let d = report
            .catalog
            .segments
            .iter()
            .find(|s| s.row_count > 0)
            .unwrap();
        let id = d.id;
        let dir = report.paths.segment_dir(id);
        let path = if bundled {
            crate::column_artifact::bundle_path(&dir, d.generation)
        } else {
            dir.join("data.col")
        };
        let mut bytes = fs::read(&path).unwrap();
        let offset = if let Some(reference) = &d.column_bundle {
            let reader = crate::bundle::BundleReader::open(&path, reference).unwrap();
            let payload = reader
                .read_stream(crate::column_artifact::stream_id("columns/data.pages").unwrap())
                .unwrap();
            assert!(!payload.is_empty());
            let matches: Vec<_> = bytes[..reference.table_offset as usize]
                .windows(payload.len())
                .enumerate()
                .filter_map(|(i, b)| (b == payload.as_slice()).then_some(i))
                .collect();
            assert_eq!(matches.len(), 1);
            matches[0]
        } else {
            bytes.len() - 1
        };
        drop(report);
        bytes[offset] ^= 0x80;
        fs::write(path, bytes).unwrap();
        let before = tree(tmp.path());
        let report = inspection(tmp.path());
        let plan = report.into_repair_plan(&[id], plan_limits()).unwrap();
        let (mut verifier, inputs) = plan.prepare_candidate(id, reads()).unwrap();
        assert_eq!(inputs.len(), rows.len());
        assert!(inputs.iter().all(|r| r.preserved.is_none()));
        for (input, row) in inputs.iter().zip(&rows) {
            assert_eq!(
                (input.block_number, input.block_hash, input.log_index),
                (row.block_number, row.block_hash, row.log_index)
            );
        }
        verifier.append(&rows).unwrap();
        verifier.finish().unwrap();
        assert_eq!(tree(tmp.path()), before);
    }
}
#[test]
fn preparation_carries_trace_noncanonical_and_outside_seed_rows() {
    let (tmp, rows) = fixture(false, true);
    let before = tree(tmp.path());
    let report = inspection(tmp.path());
    let id = report
        .catalog
        .segments
        .iter()
        .find(|s| s.row_count > 0)
        .unwrap()
        .id;
    let plan = report.into_repair_plan(&[id], plan_limits()).unwrap();
    assert_eq!(plan.block_ranges(), &[(10, 11)]);
    for &owner in plan.segment_ids() {
        let (mut verifier, inputs) = plan.prepare_candidate(owner, reads()).unwrap();
        let mut candidate = Vec::new();
        for input in inputs {
            let original = rows
                .iter()
                .find(|row| row.block_hash == input.block_hash && row.log_index == input.log_index)
                .unwrap();
            let must_carry = original.block_number == 10
                || original.source == Source::Trace
                || original.block_number == 12;
            assert_eq!(input.preserved.is_some(), must_carry);
            if let Some(preserved) = input.preserved {
                assert_eq!(&preserved, original);
                candidate.push(preserved);
            } else {
                candidate.push(original.clone());
            }
        }
        verifier.append(&candidate).unwrap();
        verifier.finish().unwrap();
    }
    assert_eq!(tree(tmp.path()), before);
}
#[test]
fn preparation_missing_routing_or_required_carry_blocks_and_limits_apply() {
    for routing in [true, false] {
        let (tmp, _) = fixture(false, true);
        let report = inspection(tmp.path());
        let d = report
            .catalog
            .segments
            .iter()
            .find(|s| s.row_count > 0)
            .unwrap();
        let id = d.id;
        let path =
            report
                .paths
                .segment_dir(id)
                .join(if routing { "log_index.col" } else { "data.col" });
        drop(report);
        fs::remove_file(path).unwrap();
        let before = tree(tmp.path());
        let plan = inspection(tmp.path())
            .into_repair_plan(&[id], plan_limits())
            .unwrap();
        assert!(plan.prepare_candidate(id, reads()).is_err());
        assert_eq!(tree(tmp.path()), before);
    }
    let (tmp, _) = fixture(false, true);
    let report = inspection(tmp.path());
    let id = report
        .catalog
        .segments
        .iter()
        .find(|s| s.row_count > 0)
        .unwrap()
        .id;
    let plan = report.into_repair_plan(&[id], plan_limits()).unwrap();
    for case in 0..4 {
        let mut limits = reads();
        match case {
            0 => limits.max_routing_artifact_bytes = 0,
            1 => limits.max_carry_artifact_bytes = 0,
            2 => limits.max_carry_decoded_payload_bytes = 0,
            _ => limits.max_carry_data_bytes = 11,
        }
        assert!(plan.prepare_candidate(id, limits).is_err());
    }
    let mut exact = reads();
    exact.max_carry_data_bytes = 12;
    assert!(plan.prepare_candidate(id, exact).is_ok());
}
#[test]
fn preparation_empty_owner_needs_no_routing_columns() {
    let tmp = tempfile::tempdir().unwrap();
    drop(
        NativeStorage::open(NativeStorageConfig {
            data_dir: tmp.path().to_owned(),
            ..Default::default()
        })
        .unwrap(),
    );
    let before = tree(tmp.path());
    let report = inspection(tmp.path());
    let id = report.catalog.segments[0].id;
    let plan = report.into_repair_plan(&[id], plan_limits()).unwrap();
    let (verifier, inputs) = plan
        .prepare_candidate(
            id,
            RepairReadLimits {
                max_routing_artifact_bytes: 0,
                max_carry_artifact_bytes: 0,
                max_carry_decoded_payload_bytes: 0,
                max_carry_data_bytes: 0,
            },
        )
        .unwrap();
    assert!(inputs.is_empty());
    verifier.finish().unwrap();
    assert_eq!(tree(tmp.path()), before);
}

#[test]
fn preparation_raw_routing_budget_matches_captured_artifacts() {
    let (tmp, _) = fixture(false, false);
    let report = inspection(tmp.path());
    let id = report
        .catalog
        .segments
        .iter()
        .find(|s| s.row_count > 0)
        .unwrap()
        .id;
    let dir = report.paths.segment_dir(id);
    let required: u64 = [
        "block_number.col",
        "block_hash.col",
        "log_index.col",
        "source.col",
        "canonical.bitmap",
    ]
    .iter()
    .map(|name| fs::metadata(dir.join(name)).unwrap().len())
    .sum();
    let plan = report.into_repair_plan(&[id], plan_limits()).unwrap();
    let mut limit = reads();
    limit.max_routing_artifact_bytes = required - 1;
    assert!(plan.prepare_candidate(id, limit).is_err());
    limit.max_routing_artifact_bytes = required;
    assert!(plan.prepare_candidate(id, limit).is_ok());
}

#[test]
fn preparation_bundle_routing_budget_excludes_unread_columns() {
    let (tmp, _) = fixture(true, false);
    let report = inspection(tmp.path());
    let descriptor = report
        .catalog
        .segments
        .iter()
        .find(|s| s.row_count > 0)
        .unwrap();
    let id = descriptor.id;
    let dir = report.paths.segment_dir(id);
    let path = crate::column_artifact::bundle_path(&dir, descriptor.generation);
    let reference = descriptor.column_bundle.as_ref().unwrap();
    let bundle = crate::bundle::BundleReader::open(&path, reference).unwrap();
    let manifest = crate::native::SegmentManifest::load(&dir.join("segment.json"))
        .unwrap()
        .unwrap();
    let mut required = u64::from(reference.chain_bytes);
    for column in manifest.columns.iter().filter(|c| {
        ["block_number", "block_hash", "log_index", "source"].contains(&c.name.as_str())
    }) {
        for stream in [&column.data_path, column.page_index_path.as_ref().unwrap()] {
            required += bundle
                .stream_len(crate::column_artifact::stream_id(stream).unwrap())
                .unwrap();
        }
        required += crate::page::PAGE_INDEX_HEADER_BYTES as u64;
    }
    // The allowance includes decoded table metadata and reconstructed index
    // framing, so it cannot be compared with compressed physical file size.
    // Prove this fixture has an unrelated payload, then admit at the exact
    // routing-only allowance; charging that payload would reject this call.
    assert!(
        bundle
            .stream_len(crate::column_artifact::stream_id("columns/data.pages").unwrap())
            .unwrap()
            > 0
    );
    let plan = report.into_repair_plan(&[id], plan_limits()).unwrap();
    let mut limit = reads();
    limit.max_routing_artifact_bytes = required - 1;
    assert!(plan.prepare_candidate(id, limit).is_err());
    limit.max_routing_artifact_bytes = required;
    assert!(plan.prepare_candidate(id, limit).is_ok());
}
