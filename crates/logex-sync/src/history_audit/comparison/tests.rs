//! Isolated comparison fault controls. Scripted anchors are not CL proofs.
use super::*;
use crate::history_audit::AuditManifestLimits;
use alloy_primitives::{Address, Bytes};
use logex_storage::native::{
    InspectionLimits, NativeStorage, NativeStorageConfig, PrimaryAuditLimits,
};
use logex_types::Source;

fn chain() -> (Vec<Header>, Vec<LogRow>, ExecutionAnchor) {
    let mut headers = Vec::new();
    let mut rows = Vec::new();
    for number in 0..4 {
        let header = Header {
            number,
            timestamp: number * 12,
            parent_hash: headers.last().map(Header::hash_slow).unwrap_or_default(),
            receipts_root: B256::repeat_byte(number as u8),
            ..Default::default()
        };
        if number == 1 || number == 3 {
            for index in 0..3 {
                rows.push(LogRow {
                    block_number: number,
                    block_hash: header.hash_slow(),
                    timestamp: header.timestamp,
                    tx_hash: B256::repeat_byte(index as u8),
                    tx_index: index,
                    log_index: index,
                    address: Address::repeat_byte(7),
                    topic0: Some(B256::repeat_byte(8)),
                    topic1: None,
                    topic2: None,
                    topic3: None,
                    data: Bytes::from_static(b"real shape, isolated fixture"),
                    data_len: 28,
                    source: Source::Receipt,
                });
            }
        }
        headers.push(header);
    }
    let tip = headers.last().unwrap();
    let anchor = ExecutionAnchor {
        block_number: tip.number,
        block_hash: tip.hash_slow(),
        receipts_root: tip.receipts_root,
        beacon_root: B256::repeat_byte(9),
        beacon_slot: 10,
    };
    (headers, rows, anchor)
}
fn local(
    rows: &[LogRow],
) -> (
    tempfile::TempDir,
    tempfile::TempDir,
    NativeStorage,
    AuditManifest,
) {
    let dir = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let mut storage = NativeStorage::open(NativeStorageConfig {
        data_dir: dir.path().to_owned(),
        hot_target_rows: 2,
        compaction_safety_margin_blocks: 0,
    })
    .unwrap();
    storage.write_batch(rows).unwrap();
    storage.checkpoint_durable().unwrap();
    let snapshot = storage
        .primary_audit_snapshot(PrimaryAuditLimits {
            max_segments: 100,
            max_total_rows: 1000,
            segment: InspectionLimits {
                max_segment_rows: 100,
                max_retained_artifact_bytes: 1024 * 1024,
                max_decoded_payload_bytes: 1024 * 1024,
            },
        })
        .unwrap();
    let manifest = AuditManifest::build(
        snapshot,
        AuditRange {
            from: 0,
            through: 3,
        },
        scratch.path(),
        AuditManifestLimits {
            sort_records: 1,
            merge_fan_in: 2,
            max_scratch_bytes: 1024 * 1024,
            max_runs: 1000,
        },
        &|| false,
    )
    .unwrap();
    (dir, scratch, storage, manifest)
}
fn block_rows(rows: &[LogRow], number: u64) -> Vec<LogRow> {
    rows.iter()
        .filter(|row| row.block_number == number)
        .cloned()
        .collect()
}

#[test]
fn full_parent_linked_traversal_includes_empty_blocks_and_all_events() {
    let (headers, mut rows, anchor) = chain();
    rows.reverse();
    let (_dir, _scratch, _storage, manifest) = local(&rows);
    let mut compare = ReceiptComparison::new(&manifest, anchor).unwrap();
    for header in headers.iter().rev() {
        let mut expected = block_rows(&rows, header.number);
        expected.sort_by_key(|r| r.log_index);
        compare.compare_rows(header, &expected).unwrap();
    }
    let report = compare.finish().unwrap();
    assert_eq!(
        (report.blocks, report.empty_blocks, report.events),
        (4, 2, 6)
    );
}

#[test]
fn missing_whole_block_and_tail_events_are_not_hidden_by_a_valid_local_manifest() {
    for omit in [1, 2] {
        let (headers, rows, anchor) = chain();
        let filtered: Vec<_> = rows
            .iter()
            .filter(|r| {
                if omit == 1 {
                    r.block_number != 1
                } else {
                    r.log_index != 2
                }
            })
            .cloned()
            .collect();
        let (_dir, _scratch, _storage, manifest) = local(&filtered);
        let mut compare = ReceiptComparison::new(&manifest, anchor).unwrap();
        let failed = headers.iter().rev().any(|h| {
            compare
                .compare_rows(h, &block_rows(&rows, h.number))
                .is_err()
        });
        assert!(failed);
        assert!(compare.finish().is_err());
    }
}

#[test]
fn extra_rows_altered_fields_and_rows_at_empty_blocks_fail_before_completion() {
    let (headers, rows, anchor) = chain();
    for mode in 0..3 {
        let mut changed = rows.clone();
        match mode {
            0 => {
                let mut extra = rows.last().unwrap().clone();
                extra.log_index = 3;
                changed.push(extra);
            }
            1 => changed.last_mut().unwrap().address = Address::repeat_byte(22),
            _ => {
                let mut extra = rows[0].clone();
                extra.block_number = 2;
                extra.block_hash = headers[2].hash_slow();
                extra.timestamp = headers[2].timestamp;
                extra.log_index = 0;
                changed.push(extra);
            }
        }
        let (_dir, _scratch, _storage, manifest) = local(&changed);
        let mut compare = ReceiptComparison::new(&manifest, anchor).unwrap();
        assert!(headers.iter().rev().any(|h| {
            compare
                .compare_rows(h, &block_rows(&rows, h.number))
                .is_err()
        }));
        assert!(compare.finish().is_err());
    }
}

#[test]
fn skip_replay_wrong_anchor_and_early_finish_cannot_certify_a_subset() {
    let (headers, rows, anchor) = chain();
    let (_dir, _scratch, _storage, manifest) = local(&rows);
    assert!(
        ReceiptComparison::new(&manifest, anchor)
            .unwrap()
            .finish()
            .is_err()
    );
    let mut compare = ReceiptComparison::new(&manifest, anchor).unwrap();
    assert!(compare.compare_rows(&headers[2], &[]).is_err());
    assert!(
        compare
            .compare_rows(&headers[3], &block_rows(&rows, 3))
            .is_err()
    );
    let mut compare = ReceiptComparison::new(&manifest, anchor).unwrap();
    compare
        .compare_rows(&headers[3], &block_rows(&rows, 3))
        .unwrap();
    assert!(
        compare
            .compare_rows(&headers[3], &block_rows(&rows, 3))
            .is_err()
    );
    let mut wrong = anchor;
    wrong.block_hash = B256::ZERO;
    let mut compare = ReceiptComparison::new(&manifest, wrong).unwrap();
    assert!(
        compare
            .compare_rows(&headers[3], &block_rows(&rows, 3))
            .is_err()
    );
    let mut wrong = anchor;
    wrong.receipts_root = B256::ZERO;
    let mut compare = ReceiptComparison::new(&manifest, wrong).unwrap();
    assert!(
        compare
            .compare_rows(&headers[3], &block_rows(&rows, 3))
            .is_err()
    );
}

#[test]
fn source_invalidation_wins_over_comparison_errors_and_rejects_final_admission() {
    let (headers, rows, anchor) = chain();
    let (_dir, _scratch, mut storage, manifest) = local(&rows);
    let mut compare = ReceiptComparison::new(&manifest, anchor).unwrap();
    for header in headers.iter().rev() {
        compare
            .compare_rows(header, &block_rows(&rows, header.number))
            .unwrap();
    }
    storage.mark_non_canonical(headers[1].hash_slow()).unwrap();
    assert_eq!(
        compare.finish().unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}
