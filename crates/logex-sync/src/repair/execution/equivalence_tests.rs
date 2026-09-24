//! Independent row expectations over scripted execution transport. Test-populated
//! retained anchors exercise admission, not a new consensus proof or live chain.
use super::tests::{consensus, limits, tree};
use super::*;
use crate::repair::{
    assess_repair,
    tests::{anchor, fixture_with_logs},
};
use alloy_consensus::{Header, SignableTransaction, TxLegacy};
use alloy_primitives::{Address, B256, Bytes, Signature, U256};
use logex_query::execute_log_filter;
use logex_storage::{
    PartitionManager, PartitionManagerConfig, SegmentReader,
    native::{
        LogOrder, NativeLogFilter, NativeStorage, NativeStorageCatalog, NativeStorageConfig,
        SegmentKind, StorageCatalogPaths, TopicConstraint,
    },
};
use logex_types::{LogRow, Source};
use std::{collections::BTreeMap, fs};

/// Fixture expectations are declared directly: two logs, in transactions 1/2,
/// with block-global log ordinals 0/1. No fetcher or extraction code supplies rows.
pub(super) fn expected_rows(headers: &[Header], logged_blocks: &[u64]) -> Vec<LogRow> {
    let mut result = Vec::new();
    for &block in logged_blocks {
        let header = &headers[block as usize];
        for tx_index in 1..=2 {
            let transaction = TxLegacy {
                nonce: tx_index,
                gas_limit: 21_000,
                ..Default::default()
            }
            .into_signed(Signature::new(U256::from(1), U256::from(2), false));
            result.push(LogRow {
                block_number: header.number,
                block_hash: header.hash_slow(),
                timestamp: header.timestamp,
                tx_hash: *transaction.hash(),
                tx_index: tx_index as u32,
                log_index: tx_index as u32 - 1,
                address: Address::repeat_byte(7),
                topic0: Some(B256::repeat_byte(8)),
                topic1: None,
                topic2: None,
                topic3: None,
                data: Bytes::from(vec![tx_index as u8; 4]),
                data_len: 4,
                source: Source::Receipt,
            });
        }
    }
    result
}

fn catalog(root: &Path) -> NativeStorageCatalog {
    let inspection = assess_repair(root, limits().assessment, IndexBuildProfile::All).unwrap();
    let RepairInspection::Primary(primary) = inspection.into_inspection() else {
        panic!("expected ordinary inspection")
    };
    primary.catalog.clone()
}

struct Dataset {
    live: Vec<LogRow>,
    historical: Vec<LogRow>,
    gap: Vec<LogRow>,
    retired: Vec<B256>,
    target: u64,
}
impl Dataset {
    fn new(headers: &[Header], disjoint: bool) -> Self {
        let rows = expected_rows(headers, if disjoint { &[0, 2, 3] } else { &[0, 3] });
        let at = |block: u64, tx| {
            rows.iter()
                .find(|r| r.block_number == headers[block as usize].number && r.tx_index == tx)
                .unwrap()
                .clone()
        };
        let mut trace = at(0, 1);
        trace.block_number = headers[1].number;
        trace.block_hash = headers[1].hash_slow();
        trace.timestamp = headers[1].timestamp;
        trace.tx_index = 8;
        trace.log_index = 8;
        trace.source = Source::Trace;
        trace.address = Address::repeat_byte(9);
        trace.topic0 = Some(B256::repeat_byte(9));
        trace.topic1 = Some(B256::ZERO);
        trace.data = Bytes::from(vec![0x55; 3]);
        trace.data_len = 3;
        let mut orphan = at(3, 1);
        orphan.block_hash = B256::repeat_byte(0x99);
        orphan.tx_index = 9;
        orphan.log_index = 9;
        orphan.address = Address::repeat_byte(10);
        orphan.data = Bytes::from(vec![0x99; 7]);
        orphan.data_len = 7;
        let mut orphan_trace = at(0, 1);
        orphan_trace.block_hash = B256::repeat_byte(0x98);
        orphan_trace.tx_index = 10;
        orphan_trace.log_index = 10;
        orphan_trace.source = Source::Trace;
        orphan_trace.address = Address::repeat_byte(11);
        orphan_trace.topic0 = None;
        orphan_trace.data = Bytes::new();
        orphan_trace.data_len = 0;
        let retired = vec![orphan.block_hash, orphan_trace.block_hash];
        let mut historical = vec![at(0, 2), trace, orphan, orphan_trace];
        let (live, gap, target) = if disjoint {
            historical.insert(1, at(2, 1));
            (
                vec![
                    at(0, 2),
                    at(0, 1),
                    at(0, 2),
                    at(0, 1),
                    at(0, 2),
                    at(3, 2),
                    at(3, 1),
                ],
                vec![at(2, 2)],
                5,
            )
        } else {
            (
                vec![at(3, 2), at(0, 2), at(3, 1), at(0, 1)],
                Vec::new(),
                100,
            )
        };
        Self {
            live,
            historical,
            gap,
            retired,
            target,
        }
    }

    fn all_rows(&self) -> Vec<LogRow> {
        self.live
            .iter()
            .chain(&self.historical)
            .chain(&self.gap)
            .cloned()
            .collect()
    }

    fn write(&self, headers: &[Header]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = NativeStorage::open(NativeStorageConfig {
            data_dir: dir.path().to_owned(),
            hot_target_rows: self.target,
            compaction_safety_margin_blocks: 0,
        })
        .unwrap();
        storage.write_batch(&self.live).unwrap();
        storage.write_historical_batch(&self.historical).unwrap();
        storage.write_historical_batch(&self.gap).unwrap();
        for hash in &self.retired {
            assert_eq!(storage.mark_non_canonical(*hash).unwrap(), 1);
        }
        storage
            .record_verified_canonical_state(&anchor(&headers[3]), &headers[3], headers)
            .unwrap();
        storage.record_historical_floor(&headers[0]).unwrap();
        storage.checkpoint_durable().unwrap();
        drop(storage);
        for segment in &catalog(dir.path()).segments {
            IndexBuilder::build_indexes(
                &dir.path().join(&segment.relative_path),
                IndexBuildProfile::All,
            )
            .unwrap();
        }
        dir
    }
}

fn owner_rows(
    root: &Path,
    catalog: &NativeStorageCatalog,
) -> BTreeMap<u64, (Vec<LogRow>, Vec<bool>)> {
    catalog
        .segments
        .iter()
        .map(|segment| {
            let reader = SegmentReader::open(&root.join(&segment.relative_path)).unwrap();
            let rows = reader.read_log_rows(None).unwrap();
            let bitmap = reader.read_canonical().unwrap();
            let canonical = (0..rows.len())
                .map(|index| bitmap.is_present(index as u64))
                .collect();
            (segment.id, (rows, canonical))
        })
        .collect()
}

// Each case supplies a separate, small expected selection. This does not call the
// query engine's row matcher, indexes, extractor, or repaired segment reader.
fn assert_queries(
    repaired: &PartitionManager,
    reference: &PartitionManager,
    dataset: &Dataset,
    headers: &[Header],
) {
    let all = dataset.all_rows();
    let canonical: Vec<_> = all
        .iter()
        .filter(|r| !dataset.retired.contains(&r.block_hash))
        .cloned()
        .collect();
    let mut cases = vec![
        ("canonical", NativeLogFilter::new(), canonical.clone()),
        ("including orphans", NativeLogFilter::default(), all.clone()),
    ];
    cases.push((
        "empty receipt block and trace",
        NativeLogFilter::new().with_block_range(Some(headers[1].number), Some(headers[1].number)),
        canonical
            .iter()
            .filter(|r| r.source == Source::Trace)
            .cloned()
            .collect(),
    ));
    cases.push((
        "timestamp",
        NativeLogFilter::new()
            .with_timestamp_range(Some(headers[2].timestamp), Some(headers[3].timestamp)),
        canonical
            .iter()
            .filter(|r| r.block_number >= headers[2].number)
            .cloned()
            .collect(),
    ));
    cases.push((
        "block hash",
        NativeLogFilter::new().with_block_hash(headers[0].hash_slow()),
        canonical
            .iter()
            .filter(|r| r.block_number == headers[0].number)
            .cloned()
            .collect(),
    ));
    cases.push((
        "address union",
        NativeLogFilter::default()
            .with_addresses(vec![Address::repeat_byte(9), Address::repeat_byte(11)]),
        all.iter()
            .filter(|r| r.source == Source::Trace)
            .cloned()
            .collect(),
    ));
    cases.push((
        "zero is present",
        NativeLogFilter::new().with_topic(1, TopicConstraint::One(B256::ZERO)),
        canonical
            .iter()
            .filter(|r| r.source == Source::Trace)
            .cloned()
            .collect(),
    ));
    cases.push((
        "null is not zero",
        NativeLogFilter::default().with_topic(2, TopicConstraint::One(B256::ZERO)),
        Vec::new(),
    ));
    let mut two_topics = NativeLogFilter::new();
    two_topics.min_topic_count = 2;
    cases.push((
        "topic count",
        two_topics,
        canonical
            .iter()
            .filter(|r| r.source == Source::Trace)
            .cloned()
            .collect(),
    ));
    cases.push((
        "topic union",
        NativeLogFilter::default().with_topic(
            0,
            TopicConstraint::AnyOf(vec![B256::repeat_byte(8), B256::repeat_byte(9)]),
        ),
        all.iter().filter(|r| r.topic0.is_some()).cloned().collect(),
    ));
    let data_len = NativeLogFilter {
        data_len: Some(0),
        ..NativeLogFilter::default()
    };
    cases.push((
        "empty data",
        data_len,
        all.iter()
            .filter(|r| r.address == Address::repeat_byte(11))
            .cloned()
            .collect(),
    ));
    let mut data_range = NativeLogFilter::new();
    data_range.data_min = Some(vec![1; 4]);
    data_range.data_max = Some(vec![2; 4]);
    data_range.data_not_equals = vec![vec![1; 4]];
    cases.push((
        "data bounds and exclusion",
        data_range,
        canonical
            .iter()
            .filter(|r| r.source == Source::Receipt && r.tx_index == 2)
            .cloned()
            .collect(),
    ));
    for (name, filter, expected) in cases {
        for order in [LogOrder::Ascending, LogOrder::Descending] {
            let mut ordered = expected.clone();
            match order {
                LogOrder::Ascending => {
                    ordered.sort_by_key(|r| (r.block_number, r.tx_index, r.log_index))
                }
                LogOrder::Descending => ordered
                    .sort_by_key(|r| std::cmp::Reverse((r.block_number, r.tx_index, r.log_index))),
            }
            for (offset, limit) in [
                (0, None),
                (0, Some(0)),
                (0, Some(1)),
                (1, Some(2)),
                (3, Some(4)),
                (100, Some(2)),
            ] {
                let mut page_filter = filter.clone();
                page_filter.order = order;
                page_filter.offset = offset;
                page_filter.limit = limit;
                let wanted: Vec<_> = ordered
                    .iter()
                    .skip(offset)
                    .take(limit.unwrap_or(usize::MAX))
                    .cloned()
                    .collect();
                assert_eq!(
                    execute_log_filter(reference, &page_filter).unwrap(),
                    wanted,
                    "reference {name} {order:?} offset={offset} limit={limit:?}"
                );
                assert_eq!(
                    execute_log_filter(repaired, &page_filter).unwrap(),
                    wanted,
                    "repaired {name} {order:?} offset={offset} limit={limit:?}"
                );
            }
        }
    }
}

#[tokio::test]
async fn repaired_hot_and_sealed_owners_match_independent_reference_queries() {
    for disjoint in [true, false] {
        let (mut source, headers) = fixture_with_logs(if disjoint { &[0, 2, 3] } else { &[0, 3] });
        let dataset = Dataset::new(&headers, disjoint);
        let damaged = dataset.write(&headers);
        let reference = dataset.write(&headers);
        let before = catalog(damaged.path());
        let owners = owner_rows(damaged.path(), &before);
        let paths = StorageCatalogPaths::new(damaged.path().to_owned());
        let seeds: Vec<_> = before
            .segments
            .iter()
            .filter(|segment| {
                segment.column_bundle.is_none()
                    && segment.row_count > 0
                    && (segment.id == before.active_hot_segment.unwrap()
                        || disjoint
                            && segment.kind == SegmentKind::Sealed
                            && segment.min_block == Some(headers[0].number)
                            && segment.max_block == Some(headers[0].number))
            })
            .map(|segment| segment.id)
            .collect();
        assert_eq!(seeds.len(), if disjoint { 2 } else { 1 });
        if disjoint {
            assert!(
                before
                    .segments
                    .iter()
                    .any(|s| seeds.contains(&s.id) && s.kind == SegmentKind::Sealed)
            );
        }
        assert!(
            before
                .segments
                .iter()
                .any(|s| seeds.contains(&s.id) && s.kind == SegmentKind::Hot)
        );
        for id in &seeds {
            fs::remove_file(paths.segment_dir(*id).join("data.col")).unwrap();
        }
        let before_trees: BTreeMap<_, _> = before
            .segments
            .iter()
            .map(|s| (s.id, tree(&paths.segment_dir(s.id))))
            .collect();
        let assessment =
            assess_repair(damaged.path(), limits().assessment, IndexBuildProfile::All).unwrap();
        let (_trust_dir, trust) = consensus(&[anchor(&headers[3])]);
        let result = execute_repair(
            assessment,
            limits(),
            IndexBuildProfile::All,
            &mut source,
            &trust,
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let RepairAssessmentReport::Inspected { segments, .. } = result.assessment.report() else {
            panic!("expected verified inspection")
        };
        assert!(
            segments
                .iter()
                .all(|s| s.disposition == SegmentRepairDisposition::Verified)
        );
        assert_eq!(result.quarantine_dirs.len(), 1);
        let quarantine = result.quarantine_dirs[0].clone();
        drop(result);
        assert_eq!(
            source.body_calls,
            if disjoint {
                vec![headers[0].number, headers[3].number]
            } else {
                vec![
                    headers[3].number,
                    headers[2].number,
                    headers[1].number,
                    headers[0].number,
                ]
            }
        );
        assert_eq!(source.receipt_calls, source.body_calls);
        let after = catalog(damaged.path());
        assert_eq!(after.state, before.state);
        assert_eq!(after.anchors, before.anchors);
        assert_eq!(after.segments.len(), before.segments.len());
        let restored = owner_rows(damaged.path(), &after);
        let mut selected = 0;
        for original in &before.segments {
            let replacement = after
                .segments
                .iter()
                .find(|s| s.source_namespace == original.source_namespace)
                .unwrap();
            assert_eq!(restored[&replacement.id], owners[&original.id]);
            assert_eq!(replacement.kind, original.kind);
            assert_eq!(replacement.row_count, original.row_count);
            assert_eq!(replacement.source_commitment, original.source_commitment);
            if replacement.id != original.id {
                selected += 1;
                assert!(replacement.id >= before.next_segment_id);
                assert_eq!(
                    tree(&quarantine.join(format!("s_{:016}", original.id))),
                    before_trees[&original.id]
                );
            } else {
                assert_eq!(replacement, original);
                assert_eq!(
                    tree(&paths.segment_dir(original.id)),
                    before_trees[&original.id]
                );
            }
        }
        assert_eq!(selected, if disjoint { 3 } else { 2 });
        assert_eq!(fs::read_dir(&quarantine).unwrap().count(), selected);
        for _ in 0..3 {
            let open = |root: &Path| {
                PartitionManager::open(PartitionManagerConfig {
                    data_dir: root.to_owned(),
                    partition_target_rows: dataset.target,
                    compaction_safety_margin_blocks: 0,
                })
                .unwrap()
            };
            let repaired = open(damaged.path());
            let clean = open(reference.path());
            assert_queries(&repaired, &clean, &dataset, &headers);
            drop((repaired, clean));
            let reopened = catalog(damaged.path());
            assert_eq!(reopened.state, before.state);
            assert_eq!(reopened.anchors, before.anchors);
            assert_eq!(owner_rows(damaged.path(), &reopened), restored);
        }
    }
}
