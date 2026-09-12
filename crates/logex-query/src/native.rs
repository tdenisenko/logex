use std::io;
use std::path::Path;

use alloy_primitives::{Address, B256};
use roaring::RoaringBitmap;

use logex_index::{
    BTreeIndexReader, CompositeQuery, ERC20_EVENTS_BLOOM_FILE, Erc20EventBloomReader,
    TRANSFER_BLOOM_FILE, TransferBloomReader, is_common_erc20_event_topic0, transfer_topic0,
};
use logex_storage::native::{LogOrder, NativeLogFilter, ReadViewToken, TopicConstraint};
use logex_storage::{IndexReadCheckpoint, PartitionManager, SegmentReader};
use logex_types::{LogRow, PartitionMeta};

/// A bounded optimistic query view. Later appends/new segments are excluded;
/// representation-only compaction preserves rows. A reorg or storage close
/// invalidates the view, so execution must fail and retry on a fresh snapshot.
#[derive(Debug, Clone, Default)]
pub struct StorageSnapshot {
    sealed_partitions: Vec<PartitionMeta>,
    hot_partition: Option<PartitionMeta>,
    pub(crate) validity: Option<ReadViewToken>,
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
            validity: Some(storage.read_view_token()),
        }
    }

    fn validate(&self) -> io::Result<()> {
        if self
            .validity
            .as_ref()
            .is_some_and(|token| !token.is_valid())
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "query snapshot changed during a reorg or storage restart; retry",
            ));
        }
        Ok(())
    }

    pub fn partitions_in_order(&self, order: LogOrder) -> Vec<PartitionMeta> {
        let mut partitions = self.sealed_partitions.clone();
        if let Some(hot) = &self.hot_partition {
            partitions.push(hot.clone());
        }
        match order {
            LogOrder::Ascending => partitions.sort_by_key(|partition| partition.min_block),
            LogOrder::Descending => {
                partitions.sort_by_key(|partition| std::cmp::Reverse(partition.max_block));
            }
        }
        partitions
    }
}

pub fn execute_log_filter(
    storage: &PartitionManager,
    filter: &NativeLogFilter,
) -> std::io::Result<Vec<LogRow>> {
    if filter.limit == Some(0) {
        return Ok(Vec::new());
    }
    let snapshot = StorageSnapshot::from_storage(storage);
    snapshot.validate()?;
    let mut rows = Vec::new();
    let scan_limit = filter
        .limit
        .map(|limit| limit.saturating_add(filter.offset));

    for partition in snapshot.partitions_in_order(filter.order) {
        if ordered_page_is_complete(&partition, &rows, filter.order, scan_limit) {
            break;
        }
        if !partition_matches_filter(&partition, filter) {
            continue;
        }

        let candidate_ids = candidate_row_ids(&partition.path, filter, true, partition.row_count)?;
        if candidate_ids.is_empty() {
            continue;
        }

        let reader = SegmentReader::open(&partition.path)?;
        let mut partition_rows = reader.read_log_rows(Some(&candidate_ids))?;
        partition_rows.retain(|row| matches_native_filter(row, filter));

        rows.extend(partition_rows);
        retain_ordered_prefix(&mut rows, filter.order, scan_limit);
    }

    sort_native_rows(&mut rows, filter.order);

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

    snapshot.validate()?;
    Ok(rows)
}

pub(crate) fn sort_native_rows(rows: &mut [LogRow], order: LogOrder) {
    match order {
        LogOrder::Ascending => rows.sort_by_key(native_log_sort_key),
        LogOrder::Descending => {
            rows.sort_by_key(|row| std::cmp::Reverse(native_log_sort_key(row)));
        }
    }
}

pub(crate) fn retain_ordered_prefix(rows: &mut Vec<LogRow>, order: LogOrder, limit: Option<usize>) {
    if let Some(limit) = limit {
        // Physical row order can run backwards after historical ingestion.
        // Keep the global best rows, including matches from overlapping ranges.
        sort_native_rows(rows, order);
        rows.truncate(limit);
    }
}

/// `next` must be the first remaining partition in `partitions_in_order`, and
/// `rows` the retained sorted prefix. Equal block boundaries must still be read:
/// transaction/log ordering can improve the page within that same block.
pub(crate) fn ordered_page_is_complete(
    next: &PartitionMeta,
    rows: &[LogRow],
    order: LogOrder,
    limit: Option<usize>,
) -> bool {
    let Some(limit) = limit else {
        return false;
    };
    if rows.len() < limit {
        return false;
    }
    let Some(last) = rows.last() else {
        return false;
    };
    match order {
        LogOrder::Ascending => next.min_block > last.block_number,
        LogOrder::Descending => next.max_block < last.block_number,
    }
}

pub fn partition_matches_filter(meta: &PartitionMeta, filter: &NativeLogFilter) -> bool {
    if filter
        .from_block
        .zip(filter.to_block)
        .is_some_and(|(from, to)| from > to)
        || filter
            .from_timestamp
            .zip(filter.to_timestamp)
            .is_some_and(|(from, to)| from > to)
    {
        return false;
    }
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
    if let Some(from) = filter.from_timestamp
        && meta.max_timestamp.is_some_and(|max| max < from)
    {
        return false;
    }
    if let Some(to) = filter.to_timestamp
        && meta.min_timestamp.is_some_and(|min| min > to)
    {
        return false;
    }
    true
}

pub fn candidate_row_ids(
    dir: &Path,
    filter: &NativeLogFilter,
    use_indexes: bool,
    visible_rows: u64,
) -> std::io::Result<Vec<u32>> {
    // Include every refinement column even when an index is currently available:
    // a stale or busy index must be able to fall back to this same captured source.
    let mut columns = Vec::new();
    for (name, needed) in [
        ("address", !filter.addresses.is_empty()),
        ("block_hash", filter.block_hash.is_some()),
        (
            "block_number",
            filter.from_block.is_some() || filter.to_block.is_some(),
        ),
        (
            "timestamp",
            filter.from_timestamp.is_some() || filter.to_timestamp.is_some(),
        ),
        ("data_len", filter.data_len.is_some()),
        (
            "data",
            filter.data_min.is_some()
                || filter.data_max.is_some()
                || !filter.data_not_equals.is_empty(),
        ),
    ] {
        if needed {
            columns.push(name);
        }
    }
    for (name, constraint) in ["topic0", "topic1", "topic2", "topic3"]
        .into_iter()
        .zip(&filter.topics)
    {
        if !matches!(constraint, TopicConstraint::Any) {
            columns.push(name);
        }
    }
    let reader = SegmentReader::open_projected(dir, &columns)?;
    candidate_row_ids_for_reader(dir, &reader, filter, use_indexes, false, visible_rows)
}

pub(crate) fn candidate_row_ids_for_reader(
    dir: &Path,
    reader: &SegmentReader,
    filter: &NativeLogFilter,
    use_indexes: bool,
    event_bloom_prechecked: bool,
    row_count: u64,
) -> std::io::Result<Vec<u32>> {
    let physical_rows = reader.read_row_count()?;
    if row_count > physical_rows {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment is missing rows from the captured query snapshot",
        ));
    }
    let refine_filter = use_indexes;
    let checkpoint = if use_indexes {
        IndexReadCheckpoint::open(dir, reader)?
    } else {
        None
    };
    let use_indexes = checkpoint.is_some();
    if use_indexes
        && !event_bloom_prechecked
        && erc20_event_bloom_excludes(&dir.join("indexes"), filter)?
    {
        return Ok(Vec::new());
    }

    if row_count == 0 {
        return Ok(Vec::new());
    }

    let bitmap = if use_indexes {
        build_candidate_bitmap(dir, reader, filter, row_count)?
    } else if refine_filter {
        refine_candidate_bitmap_from_columns(reader, filter, row_count, None)?
            .unwrap_or_else(|| (0..row_count as u32).collect())
    } else {
        (0..row_count as u32).collect()
    };
    if bitmap.is_empty() {
        return Ok(Vec::new());
    }
    if bitmap
        .max()
        .is_some_and(|row| u64::from(row) >= physical_rows)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "index row exceeds its source segment",
        ));
    }

    let canonical = reader.read_canonical()?;
    let row_ids = bitmap
        .iter()
        .filter(|&row_id| u64::from(row_id) < row_count)
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
    if let Some(from_timestamp) = filter.from_timestamp
        && row.timestamp < from_timestamp
    {
        return false;
    }
    if let Some(to_timestamp) = filter.to_timestamp
        && row.timestamp > to_timestamp
    {
        return false;
    }
    if !filter.addresses.is_empty() && !filter.addresses.contains(&row.address) {
        return false;
    }
    if let Some(data_len) = filter.data_len
        && row.data_len != data_len
    {
        return false;
    }
    if let Some(data_min) = &filter.data_min
        && row.data.as_ref() < data_min.as_slice()
    {
        return false;
    }
    if let Some(data_max) = &filter.data_max
        && row.data.as_ref() > data_max.as_slice()
    {
        return false;
    }
    if filter
        .data_not_equals
        .iter()
        .any(|value| row.data.as_ref() == value.as_slice())
    {
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
    segment_reader: &SegmentReader,
    filter: &NativeLogFilter,
    row_count: u64,
) -> std::io::Result<RoaringBitmap> {
    let index_dir = dir.join("indexes");
    let mut result: Option<RoaringBitmap> = None;
    let mut covered_addresses = false;
    let mut covered_topics = [false; 4];
    let mut covered_block_range = false;

    if let Some(block_hash) = filter.block_hash {
        let block_hash_path = index_dir.join("block_hash.bptree");
        if block_hash_path.exists() {
            if let Some(bitmap) =
                BTreeIndexReader::get_from_file(&block_hash_path, block_hash.as_slice())?
            {
                result = Some(intersect_optional(result, bitmap));
            } else {
                return Ok(RoaringBitmap::new());
            }
        }
    }

    if let (Some(address), Some(topic0), Some(topic1_values)) = (
        single_address(&filter.addresses),
        single_topic(&filter.topics[0]),
        topic_values(&filter.topics[1]),
    ) {
        let composite_path = index_dir.join("address_topic0_topic1.bptree");
        if composite_path.exists() {
            let mut union = RoaringBitmap::new();
            for topic1 in topic1_values {
                if let Some(bitmap) = CompositeQuery::get_address_topic0_topic1_from_file(
                    &composite_path,
                    &address,
                    &topic0,
                    &topic1,
                )? {
                    union |= bitmap;
                }
            }
            if union.is_empty() {
                return Ok(RoaringBitmap::new());
            }
            result = Some(intersect_optional(result, union));
            covered_addresses = true;
            covered_topics[0] = true;
            covered_topics[1] = true;
        }
    }

    if let (Some(address), Some(topic0), Some(topic2_values)) = (
        single_address(&filter.addresses),
        single_topic(&filter.topics[0]),
        topic_values(&filter.topics[2]),
    ) {
        let composite_path = index_dir.join("address_topic0_topic2.bptree");
        if composite_path.exists() {
            let mut union = RoaringBitmap::new();
            for topic2 in topic2_values {
                if let Some(bitmap) = CompositeQuery::get_address_topic0_topic2_from_file(
                    &composite_path,
                    &address,
                    &topic0,
                    &topic2,
                )? {
                    union |= bitmap;
                }
            }
            if union.is_empty() {
                return Ok(RoaringBitmap::new());
            }
            result = Some(intersect_optional(result, union));
            covered_addresses = true;
            covered_topics[0] = true;
            covered_topics[2] = true;
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
                let bitmap = CompositeQuery::range_address_topic0_blocks_inclusive(
                    &reader, &address, &topic0, from, to,
                );
                result = Some(intersect_optional(result, bitmap));
                covered_addresses = true;
                covered_topics[0] = true;
                covered_block_range = true;
            }
        }
        (Some(address), Some(topic0), _, _) => {
            let composite_path = index_dir.join("address_topic0.bptree");
            if composite_path.exists() {
                if let Some(bitmap) = CompositeQuery::get_address_topic0_from_file(
                    &composite_path,
                    &address,
                    &topic0,
                )? {
                    result = Some(intersect_optional(result, bitmap));
                    covered_addresses = true;
                    covered_topics[0] = true;
                } else {
                    return Ok(RoaringBitmap::new());
                }
            }
        }
        _ => {}
    }

    if let (Some(topic0), Some(topic1_values)) = (
        single_topic(&filter.topics[0]),
        topic_values(&filter.topics[1]),
    ) {
        let composite_path = index_dir.join("topic0_topic1.bptree");
        if composite_path.exists() && (!covered_topics[0] || !covered_topics[1]) {
            let mut union = RoaringBitmap::new();
            for topic1 in topic1_values {
                if let Some(bitmap) =
                    CompositeQuery::get_topic0_topic1_from_file(&composite_path, &topic0, &topic1)?
                {
                    union |= bitmap;
                }
            }
            if union.is_empty() {
                return Ok(RoaringBitmap::new());
            }
            result = Some(intersect_optional(result, union));
            covered_topics[0] = true;
            covered_topics[1] = true;
        }
    }

    if !filter.addresses.is_empty() && !covered_addresses {
        let address_path = index_dir.join("address.bptree");
        if address_path.exists() {
            let mut union = RoaringBitmap::new();
            for address in &filter.addresses {
                if let Some(bitmap) =
                    BTreeIndexReader::get_from_file(&address_path, address.as_slice())?
                {
                    union |= bitmap;
                }
            }
            if union.is_empty() {
                return Ok(RoaringBitmap::new());
            }
            result = Some(intersect_optional(result, union));
        }
    }

    if !covered_topics[0]
        && let Some(topic_bitmap) = build_topic0_bitmap(&index_dir, &filter.topics[0])?
    {
        if topic_bitmap.is_empty() {
            return Ok(RoaringBitmap::new());
        }
        result = Some(intersect_optional(result, topic_bitmap));
    }

    if !covered_block_range && (filter.from_block.is_some() || filter.to_block.is_some()) {
        let block_path = index_dir.join("block_number.bptree");
        if block_path.exists() {
            let reader = BTreeIndexReader::open(&block_path)?;
            let from = filter.from_block.unwrap_or(0);
            let to = filter.to_block.unwrap_or(u64::MAX);
            let bitmap = reader.range_inclusive(&from.to_be_bytes(), &to.to_be_bytes());
            result = Some(intersect_optional(result, bitmap));
            if result.as_ref().is_some_and(RoaringBitmap::is_empty) {
                return Ok(RoaringBitmap::new());
            }
        }
    }

    if filter.from_timestamp.is_some() || filter.to_timestamp.is_some() {
        let timestamp_path = index_dir.join("timestamp.bptree");
        if timestamp_path.exists() {
            let reader = BTreeIndexReader::open(&timestamp_path)?;
            let from = filter.from_timestamp.unwrap_or(0);
            let to = filter.to_timestamp.unwrap_or(u64::MAX);
            let bitmap = reader.range_inclusive(&from.to_be_bytes(), &to.to_be_bytes());
            result = Some(intersect_optional(result, bitmap));
            if result.as_ref().is_some_and(RoaringBitmap::is_empty) {
                return Ok(RoaringBitmap::new());
            }
        }
    }

    result = refine_candidate_bitmap_from_columns(segment_reader, filter, row_count, result)?;

    Ok(result.unwrap_or_else(|| (0..row_count as u32).collect()))
}

fn erc20_event_bloom_excludes(index_dir: &Path, filter: &NativeLogFilter) -> io::Result<bool> {
    let common_bloom_path = index_dir.join(ERC20_EVENTS_BLOOM_FILE);
    if common_bloom_path.is_file() {
        let mut reader = Erc20EventBloomReader::open(&common_bloom_path)?;
        return erc20_event_bloom_reader_excludes(&mut reader, filter);
    }

    let legacy_transfer_bloom_path = index_dir.join(TRANSFER_BLOOM_FILE);
    if legacy_transfer_bloom_path.is_file() {
        let mut reader = TransferBloomReader::open(&legacy_transfer_bloom_path)?;
        return legacy_transfer_bloom_reader_excludes(&mut reader, filter);
    }

    Ok(false)
}

pub(crate) fn erc20_event_bloom_exclusions(
    index_dir: &Path,
    filters: &[NativeLogFilter],
) -> io::Result<Option<Vec<bool>>> {
    let common_bloom_path = index_dir.join(ERC20_EVENTS_BLOOM_FILE);
    if common_bloom_path.is_file() {
        let mut reader = Erc20EventBloomReader::open(&common_bloom_path)?;
        return filters
            .iter()
            .map(|filter| erc20_event_bloom_reader_excludes(&mut reader, filter))
            .collect::<io::Result<Vec<_>>>()
            .map(Some);
    }

    let legacy_transfer_bloom_path = index_dir.join(TRANSFER_BLOOM_FILE);
    if legacy_transfer_bloom_path.is_file() {
        let mut reader = TransferBloomReader::open(&legacy_transfer_bloom_path)?;
        return filters
            .iter()
            .map(|filter| legacy_transfer_bloom_reader_excludes(&mut reader, filter))
            .collect::<io::Result<Vec<_>>>()
            .map(Some);
    }

    Ok(None)
}

fn erc20_event_bloom_reader_excludes(
    reader: &mut Erc20EventBloomReader,
    filter: &NativeLogFilter,
) -> io::Result<bool> {
    let (Some(address), Some(topic0)) = (
        single_address(&filter.addresses),
        single_topic(&filter.topics[0]),
    ) else {
        return Ok(false);
    };
    let topic0 = B256::from(topic0);
    if !is_common_erc20_event_topic0(&topic0) {
        return Ok(false);
    }
    let address = Address::from(address);
    for topic_index in [1usize, 2usize] {
        let Some(topics) = topic_values(&filter.topics[topic_index]) else {
            continue;
        };
        let mut any_present = false;
        for topic in topics {
            if reader.may_contain(&topic0, &address, topic_index, &B256::from(topic))? {
                any_present = true;
                break;
            }
        }
        if !any_present {
            return Ok(true);
        }
    }
    Ok(false)
}

fn legacy_transfer_bloom_reader_excludes(
    reader: &mut TransferBloomReader,
    filter: &NativeLogFilter,
) -> io::Result<bool> {
    let (Some(address), Some(topic0)) = (
        single_address(&filter.addresses),
        single_topic(&filter.topics[0]),
    ) else {
        return Ok(false);
    };
    if B256::from(topic0) != transfer_topic0() {
        return Ok(false);
    }
    let address = Address::from(address);
    for topic_index in [1usize, 2usize] {
        let Some(topics) = topic_values(&filter.topics[topic_index]) else {
            continue;
        };
        let mut any_present = false;
        for topic in topics {
            if reader.may_contain(&address, topic_index, &B256::from(topic))? {
                any_present = true;
                break;
            }
        }
        if !any_present {
            return Ok(true);
        }
    }
    Ok(false)
}

fn refine_candidate_bitmap_from_columns(
    reader: &SegmentReader,
    filter: &NativeLogFilter,
    row_count: u64,
    mut result: Option<RoaringBitmap>,
) -> std::io::Result<Option<RoaringBitmap>> {
    if !filter.addresses.is_empty() {
        let row_ids = row_ids_from_bitmap(result.as_ref());
        let values = reader.read_address(row_ids.as_deref())?;
        result = Some(bitmap_from_values(
            row_ids.as_deref(),
            row_count,
            values,
            |address| filter.addresses.contains(address),
        ));
        if result.as_ref().is_some_and(RoaringBitmap::is_empty) {
            return Ok(result);
        }
    }

    if let Some(block_hash) = filter.block_hash {
        let row_ids = row_ids_from_bitmap(result.as_ref());
        let values = reader.read_b256("block_hash", row_ids.as_deref())?;
        result = Some(bitmap_from_values(
            row_ids.as_deref(),
            row_count,
            values,
            |value| *value == block_hash,
        ));
        if result.as_ref().is_some_and(RoaringBitmap::is_empty) {
            return Ok(result);
        }
    }

    for (index, constraint) in filter.topics.iter().enumerate() {
        if matches!(constraint, TopicConstraint::Any) {
            continue;
        }
        let row_ids = row_ids_from_bitmap(result.as_ref());
        let values = reader.read_nullable_b256(&format!("topic{index}"), row_ids.as_deref())?;
        result = Some(bitmap_from_values(
            row_ids.as_deref(),
            row_count,
            values,
            |topic| topic_matches_constraint(*topic, constraint),
        ));
        if result.as_ref().is_some_and(RoaringBitmap::is_empty) {
            return Ok(result);
        }
    }

    if filter.from_block.is_some() || filter.to_block.is_some() {
        let row_ids = row_ids_from_bitmap(result.as_ref());
        let values = reader.read_u64("block_number", row_ids.as_deref())?;
        result = Some(bitmap_from_values(
            row_ids.as_deref(),
            row_count,
            values,
            |block| {
                filter.from_block.is_none_or(|from| *block >= from)
                    && filter.to_block.is_none_or(|to| *block <= to)
            },
        ));
        if result.as_ref().is_some_and(RoaringBitmap::is_empty) {
            return Ok(result);
        }
    }

    if filter.from_timestamp.is_some() || filter.to_timestamp.is_some() {
        let row_ids = row_ids_from_bitmap(result.as_ref());
        let values = reader.read_u64("timestamp", row_ids.as_deref())?;
        result = Some(bitmap_from_values(
            row_ids.as_deref(),
            row_count,
            values,
            |timestamp| {
                filter.from_timestamp.is_none_or(|from| *timestamp >= from)
                    && filter.to_timestamp.is_none_or(|to| *timestamp <= to)
            },
        ));
    }

    if let Some(data_len) = filter.data_len {
        let row_ids = row_ids_from_bitmap(result.as_ref());
        let values = reader.read_u32("data_len", row_ids.as_deref())?;
        result = Some(bitmap_from_values(
            row_ids.as_deref(),
            row_count,
            values,
            |value| *value == data_len,
        ));
        if result.as_ref().is_some_and(RoaringBitmap::is_empty) {
            return Ok(result);
        }
    }

    if filter.data_min.is_some() || filter.data_max.is_some() || !filter.data_not_equals.is_empty()
    {
        let row_ids = row_ids_from_bitmap(result.as_ref());
        let values = reader.read_var_bytes("data", row_ids.as_deref())?;
        result = Some(bitmap_from_values(
            row_ids.as_deref(),
            row_count,
            values,
            |data| {
                filter
                    .data_min
                    .as_ref()
                    .is_none_or(|min| data.as_ref() >= min.as_slice())
                    && filter
                        .data_max
                        .as_ref()
                        .is_none_or(|max| data.as_ref() <= max.as_slice())
                    && !filter
                        .data_not_equals
                        .iter()
                        .any(|value| data.as_ref() == value.as_slice())
            },
        ));
    }

    Ok(result)
}

fn row_ids_from_bitmap(bitmap: Option<&RoaringBitmap>) -> Option<Vec<u32>> {
    bitmap.map(|bitmap| bitmap.iter().collect())
}

fn bitmap_from_values<T>(
    row_ids: Option<&[u32]>,
    row_count: u64,
    values: Vec<T>,
    mut matches: impl FnMut(&T) -> bool,
) -> RoaringBitmap {
    let mut bitmap = RoaringBitmap::new();
    match row_ids {
        Some(row_ids) => {
            for (row_id, value) in row_ids.iter().copied().zip(values.iter()) {
                if matches(value) {
                    bitmap.insert(row_id);
                }
            }
        }
        None => {
            for (row_id, value) in (0..row_count as u32).zip(values.iter()) {
                if matches(value) {
                    bitmap.insert(row_id);
                }
            }
        }
    }
    bitmap
}

fn topic_matches_constraint(topic: Option<B256>, constraint: &TopicConstraint) -> bool {
    match constraint {
        TopicConstraint::Any => true,
        TopicConstraint::One(expected) => topic == Some(*expected),
        TopicConstraint::AnyOf(candidates) => {
            topic.is_some_and(|topic| candidates.contains(&topic))
        }
    }
}

fn build_topic0_bitmap(
    index_dir: &Path,
    constraint: &TopicConstraint,
) -> std::io::Result<Option<RoaringBitmap>> {
    let topic_path = index_dir.join("topic0.bptree");
    if !topic_path.exists() {
        return Ok(None);
    }

    let bitmap = match constraint {
        TopicConstraint::Any => return Ok(None),
        TopicConstraint::One(topic) => {
            BTreeIndexReader::get_from_file(&topic_path, topic.as_slice())?.unwrap_or_default()
        }
        TopicConstraint::AnyOf(topics) => {
            let mut union = RoaringBitmap::new();
            for topic in topics {
                if let Some(bitmap) =
                    BTreeIndexReader::get_from_file(&topic_path, topic.as_slice())?
                {
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
        TopicConstraint::AnyOf(topics) if topics.len() == 1 => topics[0].as_slice().try_into().ok(),
        _ => None,
    }
}

fn topic_values(constraint: &TopicConstraint) -> Option<Vec<[u8; 32]>> {
    match constraint {
        TopicConstraint::One(topic) => topic.as_slice().try_into().ok().map(|topic| vec![topic]),
        TopicConstraint::AnyOf(topics) if !topics.is_empty() => topics
            .iter()
            .map(|topic| topic.as_slice().try_into().ok())
            .collect(),
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
        storage.checkpoint().unwrap();
        (tmp, storage)
    }

    #[test]
    fn captured_boundaries_do_not_hide_missing_rows_or_invalid_index_ids() {
        let (_tmp, storage) = setup_storage();
        let path = &storage.hot_partition().meta.path;
        let filter = NativeLogFilter::new();
        let error = candidate_row_ids(path, &filter, true, 3).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("missing rows"));
        assert_eq!(candidate_row_ids(path, &filter, true, 1).unwrap(), vec![0]);
        let address = Address::repeat_byte(0xaa);
        let mut index = logex_index::BTreeIndex::new(20);
        index.insert(address.as_slice(), 0);
        index.insert(address.as_slice(), 2); // Beyond physical rows, not a later append.
        index
            .write_to_file(&path.join("indexes/address.bptree"))
            .unwrap();
        let filter = NativeLogFilter::new().with_addresses(vec![address]);
        assert_eq!(
            candidate_row_ids(path, &filter, true, 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
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
    fn uses_transfer_composite_indexes_for_topic_in_filters() {
        let (_tmp, storage) = setup_storage();
        let filter = NativeLogFilter::new()
            .with_addresses(vec![Address::repeat_byte(0xAA)])
            .with_topic(0, TopicConstraint::One(B256::repeat_byte(0x10)))
            .with_topic(
                1,
                TopicConstraint::AnyOf(vec![B256::repeat_byte(0x20), B256::repeat_byte(0x21)]),
            );

        let rows = execute_log_filter(&storage, &filter).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].block_number, 100);
        assert_eq!(rows[0].topic1, Some(B256::repeat_byte(0x20)));
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

    #[test]
    fn rejects_missing_canonical_bits_instead_of_omitting_rows() {
        let (_tmp, storage) = setup_storage();
        let filter = NativeLogFilter::new();
        let expected = execute_log_filter(&storage, &filter).unwrap();
        assert!(expected.len() > 1);
        let path = storage.hot_partition().meta.path.join("canonical.bitmap");
        let mut short = logex_storage::NullBitmap::new();
        for _ in 1..expected.len() {
            short.push(true);
        }
        let mut bytes = Vec::new();
        short.write_to(&mut bytes).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        assert_eq!(
            execute_log_filter(&storage, &filter).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(std::fs::read(path).unwrap(), bytes);
    }
}
