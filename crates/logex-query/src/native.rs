use std::path::Path;

use alloy_primitives::{Address, B256};
use roaring::RoaringBitmap;

use logex_index::{BTreeIndexReader, CompositeQuery};
use logex_storage::native::{LogOrder, NativeLogFilter, TopicConstraint};
use logex_storage::{PartitionManager, SegmentReader};
use logex_types::{LogRow, PartitionMeta};

#[derive(Debug, Clone, Default)]
pub struct StorageSnapshot {
    sealed_partitions: Vec<PartitionMeta>,
    hot_partition: Option<PartitionMeta>,
}

impl StorageSnapshot {
    pub fn from_storage(storage: &PartitionManager) -> Self {
        let sealed_partitions = storage
            .sealed_partitions()
            .iter()
            .map(|partition| partition.meta.clone())
            .collect();

        let hot_partition = {
            let hot = storage.hot_partition();
            (hot.meta.row_count > 0).then(|| hot.meta.clone())
        };

        Self {
            sealed_partitions,
            hot_partition,
        }
    }

    pub fn partitions_in_order(&self, order: LogOrder) -> Vec<PartitionMeta> {
        let mut partitions = self.sealed_partitions.clone();
        if let Some(hot) = &self.hot_partition {
            partitions.push(hot.clone());
        }
        partitions.sort_by_key(|partition| partition.min_block);
        if matches!(order, LogOrder::Descending) {
            partitions.reverse();
        }
        partitions
    }
}

pub fn execute_log_filter(
    storage: &PartitionManager,
    filter: &NativeLogFilter,
) -> std::io::Result<Vec<LogRow>> {
    let snapshot = StorageSnapshot::from_storage(storage);
    let mut rows = Vec::new();
    let scan_limit = filter
        .limit
        .map(|limit| limit.saturating_add(filter.offset));

    for partition in snapshot.partitions_in_order(filter.order) {
        if !partition_matches_filter(&partition, filter) {
            continue;
        }

        let candidate_ids = candidate_row_ids(&partition.path, filter, true)?;
        if candidate_ids.is_empty() {
            continue;
        }

        let reader = SegmentReader::open(&partition.path)?;
        let mut partition_rows = reader.read_log_rows(Some(&candidate_ids))?;
        partition_rows.retain(|row| matches_native_filter(row, filter));

        if matches!(filter.order, LogOrder::Descending) {
            partition_rows.sort_by_key(|row| std::cmp::Reverse(native_log_sort_key(row)));
        }

        rows.extend(partition_rows);

        if let Some(limit) = scan_limit
            && rows.len() >= limit
        {
            rows.truncate(limit);
            break;
        }
    }

    if matches!(filter.order, LogOrder::Ascending) {
        rows.sort_by_key(native_log_sort_key);
    } else {
        rows.sort_by_key(|row| std::cmp::Reverse(native_log_sort_key(row)));
    }

    if filter.offset > 0 {
        if filter.offset >= rows.len() {
            rows.clear();
        } else {
            rows.drain(..filter.offset);
        }
    }

    if let Some(limit) = filter.limit {
        rows.truncate(limit);
    }

    Ok(rows)
}

pub fn partition_matches_filter(meta: &PartitionMeta, filter: &NativeLogFilter) -> bool {
    if let Some(block_hash) = filter.block_hash {
        return meta.row_count > 0
            && (meta.min_block <= meta.max_block || block_hash != B256::ZERO);
    }
    if let Some(from) = filter.from_block
        && meta.max_block < from
    {
        return false;
    }
    if let Some(to) = filter.to_block
        && meta.min_block > to
    {
        return false;
    }
    true
}

pub fn candidate_row_ids(
    dir: &Path,
    filter: &NativeLogFilter,
    use_indexes: bool,
) -> std::io::Result<Vec<u32>> {
    let reader = SegmentReader::open(dir)?;
    let row_count = reader.read_row_count()?;
    if row_count == 0 {
        return Ok(Vec::new());
    }

    let bitmap = if use_indexes {
        build_candidate_bitmap(dir, filter, row_count)?
    } else {
        (0..row_count as u32).collect()
    };

    let canonical = reader.read_canonical()?;
    let row_ids = bitmap
        .iter()
        .filter(|&row_id| !filter.canonical_only || canonical.is_present(row_id as u64))
        .collect();

    Ok(row_ids)
}

pub fn matches_native_filter(row: &LogRow, filter: &NativeLogFilter) -> bool {
    if filter.canonical_only {
        // Canonical-only enforcement is handled by the canonical bitmap before
        // rows are materialized.
    }

    if let Some(block_hash) = filter.block_hash
        && row.block_hash != block_hash
    {
        return false;
    }
    if let Some(from_block) = filter.from_block
        && row.block_number < from_block
    {
        return false;
    }
    if let Some(to_block) = filter.to_block
        && row.block_number > to_block
    {
        return false;
    }
    if !filter.addresses.is_empty() && !filter.addresses.contains(&row.address) {
        return false;
    }

    let row_topics = [row.topic0, row.topic1, row.topic2, row.topic3];
    for (topic, constraint) in row_topics.into_iter().zip(filter.topics.iter()) {
        match constraint {
            TopicConstraint::Any => {}
            TopicConstraint::One(expected) => {
                if topic != Some(*expected) {
                    return false;
                }
            }
            TopicConstraint::AnyOf(candidates) => {
                if let Some(topic) = topic {
                    if !candidates.contains(&topic) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
    }

    true
}

fn build_candidate_bitmap(
    dir: &Path,
    filter: &NativeLogFilter,
    row_count: u64,
) -> std::io::Result<RoaringBitmap> {
    let index_dir = dir.join("indexes");
    let mut result: Option<RoaringBitmap> = None;

    if let Some(block_hash) = filter.block_hash {
        let block_hash_path = index_dir.join("block_hash.bptree");
        if block_hash_path.exists() {
            let reader = BTreeIndexReader::open(&block_hash_path)?;
            if let Some(bitmap) = reader.get(block_hash.as_slice()) {
                result = Some(intersect_optional(result, bitmap.clone()));
            } else {
                return Ok(RoaringBitmap::new());
            }
        }
    }

    match (
        single_address(&filter.addresses),
        single_topic(&filter.topics[0]),
        filter.from_block,
        filter.to_block,
    ) {
        (Some(address), Some(topic0), Some(from), Some(to)) => {
            let composite_path = index_dir.join("address_topic0_block.bptree");
            if composite_path.exists() {
                let reader = BTreeIndexReader::open(&composite_path)?;
                let from_inclusive = from;
                let to_exclusive = to.saturating_add(1);
                let bitmap = CompositeQuery::range_address_topic0_blocks(
                    &reader,
                    &address,
                    &topic0,
                    from_inclusive,
                    to_exclusive,
                );
                result = Some(intersect_optional(result, bitmap));
            }
        }
        (Some(address), Some(topic0), _, _) => {
            let composite_path = index_dir.join("address_topic0.bptree");
            if composite_path.exists() {
                let reader = BTreeIndexReader::open(&composite_path)?;
                if let Some(bitmap) = CompositeQuery::get_address_topic0(&reader, &address, &topic0)
                {
                    result = Some(intersect_optional(result, bitmap));
                } else {
                    return Ok(RoaringBitmap::new());
                }
            }
        }
        _ => {}
    }

    if let (Some(topic0), Some(topic1)) = (
        single_topic(&filter.topics[0]),
        single_topic(&filter.topics[1]),
    ) {
        let composite_path = index_dir.join("topic0_topic1.bptree");
        if composite_path.exists() {
            let reader = BTreeIndexReader::open(&composite_path)?;
            if let Some(bitmap) = CompositeQuery::get_topic0_topic1(&reader, &topic0, &topic1) {
                result = Some(intersect_optional(result, bitmap));
            } else {
                return Ok(RoaringBitmap::new());
            }
        }
    }

    if !filter.addresses.is_empty() {
        let address_path = index_dir.join("address.bptree");
        if address_path.exists() {
            let reader = BTreeIndexReader::open(&address_path)?;
            let mut union = RoaringBitmap::new();
            for address in &filter.addresses {
                if let Some(bitmap) = reader.get(address.as_slice()) {
                    union |= bitmap;
                }
            }
            if union.is_empty() {
                return Ok(RoaringBitmap::new());
            }
            result = Some(intersect_optional(result, union));
        }
    }

    if let Some(topic_bitmap) = build_topic0_bitmap(&index_dir, &filter.topics[0])? {
        if topic_bitmap.is_empty() {
            return Ok(RoaringBitmap::new());
        }
        result = Some(intersect_optional(result, topic_bitmap));
    }

    if filter.from_block.is_some() || filter.to_block.is_some() {
        let block_path = index_dir.join("block_number.bptree");
        if block_path.exists() {
            let reader = BTreeIndexReader::open(&block_path)?;
            let from = filter.from_block.unwrap_or(0);
            let to_exclusive = filter.to_block.unwrap_or(u64::MAX - 1).saturating_add(1);
            let bitmap = reader.range(&from.to_be_bytes(), &to_exclusive.to_be_bytes());
            result = Some(intersect_optional(result, bitmap));
        }
    }

    Ok(result.unwrap_or_else(|| (0..row_count as u32).collect()))
}

fn build_topic0_bitmap(
    index_dir: &Path,
    constraint: &TopicConstraint,
) -> std::io::Result<Option<RoaringBitmap>> {
    let topic_path = index_dir.join("topic0.bptree");
    if !topic_path.exists() {
        return Ok(None);
    }

    let reader = BTreeIndexReader::open(&topic_path)?;
    let bitmap = match constraint {
        TopicConstraint::Any => return Ok(None),
        TopicConstraint::One(topic) => reader.get(topic.as_slice()).cloned().unwrap_or_default(),
        TopicConstraint::AnyOf(topics) => {
            let mut union = RoaringBitmap::new();
            for topic in topics {
                if let Some(bitmap) = reader.get(topic.as_slice()) {
                    union |= bitmap;
                }
            }
            union
        }
    };

    Ok(Some(bitmap))
}

fn intersect_optional(existing: Option<RoaringBitmap>, new: RoaringBitmap) -> RoaringBitmap {
    match existing {
        Some(existing) => existing & new,
        None => new,
    }
}

fn single_address(addresses: &[Address]) -> Option<[u8; 20]> {
    (addresses.len() == 1).then(|| addresses[0].0.0)
}

fn single_topic(constraint: &TopicConstraint) -> Option<[u8; 32]> {
    match constraint {
        TopicConstraint::One(topic) => topic.as_slice().try_into().ok(),
        _ => None,
    }
}

fn native_log_sort_key(row: &LogRow) -> (u64, u32, u32) {
    (row.block_number, row.tx_index, row.log_index)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, bytes};
    use logex_index::IndexBuilder;
    use logex_storage::PartitionManagerConfig;
    use logex_types::Source;
    use tempfile::TempDir;

    use super::*;

    fn make_test_rows() -> Vec<LogRow> {
        vec![
            LogRow {
                block_number: 100,
                block_hash: B256::repeat_byte(0x01),
                timestamp: 1_700_000_000,
                tx_hash: B256::repeat_byte(0x11),
                tx_index: 0,
                log_index: 0,
                address: Address::repeat_byte(0xAA),
                topic0: Some(B256::repeat_byte(0x10)),
                topic1: Some(B256::repeat_byte(0x20)),
                topic2: None,
                topic3: None,
                data: bytes!(""),
                data_len: 0,
                source: Source::Receipt,
            },
            LogRow {
                block_number: 101,
                block_hash: B256::repeat_byte(0x02),
                timestamp: 1_700_000_012,
                tx_hash: B256::repeat_byte(0x12),
                tx_index: 0,
                log_index: 1,
                address: Address::repeat_byte(0xBB),
                topic0: Some(B256::repeat_byte(0x10)),
                topic1: Some(B256::repeat_byte(0x21)),
                topic2: None,
                topic3: None,
                data: bytes!("cafe"),
                data_len: 2,
                source: Source::Receipt,
            },
        ]
    }

    fn setup_storage() -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let mut storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        storage.write_batch(&make_test_rows()).unwrap();
        IndexBuilder::build_all_indexes(&storage.hot_partition().meta.path).unwrap();
        storage
            .refresh_segment_indexes(storage.hot_partition().meta.id)
            .unwrap();
        (tmp, storage)
    }

    #[test]
    fn executes_native_log_filter() {
        let (_tmp, storage) = setup_storage();
        let filter = NativeLogFilter::new()
            .with_block_range(Some(100), Some(101))
            .with_addresses(vec![Address::repeat_byte(0xAA)])
            .with_topic(0, TopicConstraint::One(B256::repeat_byte(0x10)));

        let rows = execute_log_filter(&storage, &filter).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].block_number, 100);
    }

    #[test]
    fn uses_block_hash_index_when_available() {
        let (_tmp, storage) = setup_storage();
        let filter = NativeLogFilter::new().with_block_hash(B256::repeat_byte(0x02));

        let rows = execute_log_filter(&storage, &filter).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].block_number, 101);
    }

    #[test]
    fn applies_offset_after_native_sort_order() {
        let (_tmp, storage) = setup_storage();
        let mut filter = NativeLogFilter::new();
        filter.limit = Some(1);
        filter.offset = 1;

        let rows = execute_log_filter(&storage, &filter).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].block_number, 101);
    }
}
