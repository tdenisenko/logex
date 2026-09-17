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
    execute_log_filter_with_cancel(storage, filter, None)
}

/// Scan with cooperative cancellation between filesystem operations.
/// An operating-system I/O already in progress cannot be interrupted here.
pub fn execute_log_filter_with_cancel(
    storage: &PartitionManager,
    filter: &NativeLogFilter,
    cancel: Option<&crate::QueryCancelCheck>,
) -> std::io::Result<Vec<LogRow>> {
    let snapshot = StorageSnapshot::from_storage(storage);
    execute_log_filter_on_snapshot_with_cancel(&snapshot, filter, cancel)
}

/// Execute a captured view without borrowing the live storage manager.
///
/// Callers may release their storage lock after capturing the snapshot. Appends
/// stay outside the captured row boundaries; reorg or close invalidation is
/// checked before execution and after both successful and failed execution.
/// Even an empty requested page must refer to a valid view.
pub fn execute_log_filter_on_snapshot_with_cancel(
    snapshot: &StorageSnapshot,
    filter: &NativeLogFilter,
    cancel: Option<&crate::QueryCancelCheck>,
) -> std::io::Result<Vec<LogRow>> {
    snapshot.validate()?;
    let result = execute_log_filter_snapshot_inner(snapshot, filter, cancel);
    snapshot.validate()?;
    result
}

fn execute_log_filter_snapshot_inner(
    snapshot: &StorageSnapshot,
    filter: &NativeLogFilter,
    cancel: Option<&crate::QueryCancelCheck>,
) -> std::io::Result<Vec<LogRow>> {
    let check = || {
        if cancel.is_some_and(|check| check()) {
            Err(io::Error::new(io::ErrorKind::Interrupted, "query canceled"))
        } else {
            snapshot.validate()
        }
    };
    check()?;
    if filter.limit == Some(0) {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    let scan_limit = filter
        .limit
        .map(|limit| limit.saturating_add(filter.offset));

    for partition in snapshot.partitions_in_order(filter.order) {
        check()?;
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

        check()?;
        let reader = SegmentReader::open(&partition.path)?;
        check()?;
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

    check()?;
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
    if let Some(checkpoint) = checkpoint.as_ref()
        && !event_bloom_prechecked
        && erc20_event_bloom_excludes(&dir.join("indexes"), checkpoint, filter)?
    {
        return Ok(Vec::new());
    }

    if row_count == 0 {
        return Ok(Vec::new());
    }

    let bitmap = if let Some(checkpoint) = checkpoint.as_ref() {
        build_candidate_bitmap(dir, reader, checkpoint, filter, row_count)?
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
    checkpoint: &IndexReadCheckpoint,
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
        if let Some(file_id) = checkpoint.artifact_id("block_hash.bptree") {
            if let Some(bitmap) = BTreeIndexReader::get_from_file_bound(
                &block_hash_path,
                file_id,
                block_hash.as_slice(),
            )? {
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
        if let Some(file_id) = checkpoint.artifact_id("address_topic0_topic1.bptree") {
            let mut union = RoaringBitmap::new();
            for topic1 in topic1_values {
                if let Some(bitmap) = CompositeQuery::get_address_topic0_topic1_from_file_bound(
                    &composite_path,
                    file_id,
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
        if let Some(file_id) = checkpoint.artifact_id("address_topic0_topic2.bptree") {
            let mut union = RoaringBitmap::new();
            for topic2 in topic2_values {
                if let Some(bitmap) = CompositeQuery::get_address_topic0_topic2_from_file_bound(
                    &composite_path,
                    file_id,
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
            if let Some(file_id) = checkpoint.artifact_id("address_topic0_block.bptree") {
                let reader = BTreeIndexReader::open_bound(&composite_path, file_id)?;
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
            if let Some(file_id) = checkpoint.artifact_id("address_topic0.bptree") {
                if let Some(bitmap) = CompositeQuery::get_address_topic0_from_file_bound(
                    &composite_path,
                    file_id,
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
        if let Some(file_id) = checkpoint.artifact_id("topic0_topic1.bptree")
            && (!covered_topics[0] || !covered_topics[1])
        {
            let mut union = RoaringBitmap::new();
            for topic1 in topic1_values {
                if let Some(bitmap) = CompositeQuery::get_topic0_topic1_from_file_bound(
                    &composite_path,
                    file_id,
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
            covered_topics[0] = true;
            covered_topics[1] = true;
        }
    }

    if !filter.addresses.is_empty() && !covered_addresses {
        let address_path = index_dir.join("address.bptree");
        if let Some(file_id) = checkpoint.artifact_id("address.bptree") {
            let mut union = RoaringBitmap::new();
            for address in &filter.addresses {
                if let Some(bitmap) = BTreeIndexReader::get_from_file_bound(
                    &address_path,
                    file_id,
                    address.as_slice(),
                )? {
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
        && let Some(topic_bitmap) = build_topic0_bitmap(&index_dir, checkpoint, &filter.topics[0])?
    {
        if topic_bitmap.is_empty() {
            return Ok(RoaringBitmap::new());
        }
        result = Some(intersect_optional(result, topic_bitmap));
    }

    if !covered_block_range && (filter.from_block.is_some() || filter.to_block.is_some()) {
        let block_path = index_dir.join("block_number.bptree");
        if let Some(file_id) = checkpoint.artifact_id("block_number.bptree") {
            let reader = BTreeIndexReader::open_bound(&block_path, file_id)?;
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
        if let Some(file_id) = checkpoint.artifact_id("timestamp.bptree") {
            let reader = BTreeIndexReader::open_bound(&timestamp_path, file_id)?;
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

fn erc20_event_bloom_excludes(
    index_dir: &Path,
    checkpoint: &IndexReadCheckpoint,
    filter: &NativeLogFilter,
) -> io::Result<bool> {
    let common_bloom_path = index_dir.join(ERC20_EVENTS_BLOOM_FILE);
    if let Some(file_id) = checkpoint.artifact_id(ERC20_EVENTS_BLOOM_FILE) {
        let mut reader = Erc20EventBloomReader::open_bound(&common_bloom_path, file_id)?;
        return erc20_event_bloom_reader_excludes(&mut reader, filter);
    }

    let legacy_transfer_bloom_path = index_dir.join(TRANSFER_BLOOM_FILE);
    if let Some(file_id) = checkpoint.artifact_id(TRANSFER_BLOOM_FILE) {
        let mut reader = TransferBloomReader::open_bound(&legacy_transfer_bloom_path, file_id)?;
        return legacy_transfer_bloom_reader_excludes(&mut reader, filter);
    }

    Ok(false)
}

pub(crate) fn erc20_event_bloom_exclusions(
    index_dir: &Path,
    checkpoint: &IndexReadCheckpoint,
    filters: &[NativeLogFilter],
) -> io::Result<Option<Vec<bool>>> {
    let common_bloom_path = index_dir.join(ERC20_EVENTS_BLOOM_FILE);
    if let Some(file_id) = checkpoint.artifact_id(ERC20_EVENTS_BLOOM_FILE) {
        let mut reader = Erc20EventBloomReader::open_bound(&common_bloom_path, file_id)?;
        return filters
            .iter()
            .map(|filter| erc20_event_bloom_reader_excludes(&mut reader, filter))
            .collect::<io::Result<Vec<_>>>()
            .map(Some);
    }

    let legacy_transfer_bloom_path = index_dir.join(TRANSFER_BLOOM_FILE);
    if let Some(file_id) = checkpoint.artifact_id(TRANSFER_BLOOM_FILE) {
        let mut reader = TransferBloomReader::open_bound(&legacy_transfer_bloom_path, file_id)?;
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
    checkpoint: &IndexReadCheckpoint,
    constraint: &TopicConstraint,
) -> std::io::Result<Option<RoaringBitmap>> {
    let topic_path = index_dir.join("topic0.bptree");
    let Some(file_id) = checkpoint.artifact_id("topic0.bptree") else {
        return Ok(None);
    };

    let bitmap = match constraint {
        TopicConstraint::Any => return Ok(None),
        TopicConstraint::One(topic) => {
            BTreeIndexReader::get_from_file_bound(&topic_path, file_id, topic.as_slice())?
                .unwrap_or_default()
        }
        TopicConstraint::AnyOf(topics) => {
            let mut union = RoaringBitmap::new();
            for topic in topics {
                if let Some(bitmap) =
                    BTreeIndexReader::get_from_file_bound(&topic_path, file_id, topic.as_slice())?
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
    use std::fs;
    use std::path::Path;

    use alloy_primitives::{Address, B256, bytes};
    use logex_index::IndexBuilder;
    use logex_storage::{ColumnFile, PartitionManagerConfig};
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

    fn make_alternate_rows() -> Vec<LogRow> {
        let mut rows = make_test_rows();
        rows[0].block_hash = B256::repeat_byte(0x31);
        rows[0].address = Address::repeat_byte(0xCC);
        rows[1].block_hash = B256::repeat_byte(0x32);
        rows[1].address = Address::repeat_byte(0xDD);
        rows
    }

    fn write_legacy_source(dir: &Path, rows: &[LogRow]) {
        ColumnFile::write_batch(dir, rows).unwrap();
        assert!(
            !dir.join("segment.json").exists(),
            "fixture must exercise the legacy source identity"
        );
    }

    fn full_scan_row_ids(dir: &Path, filter: &NativeLogFilter) -> Vec<u32> {
        SegmentReader::open(dir)
            .unwrap()
            .read_log_rows(None)
            .unwrap()
            .into_iter()
            .enumerate()
            .filter_map(|(row_id, row)| {
                matches_native_filter(&row, filter).then_some(row_id as u32)
            })
            .collect()
    }

    fn assert_indexed_result_matches_scan_or_errors(
        dir: &Path,
        filter: &NativeLogFilter,
        context: &str,
    ) {
        let expected = full_scan_row_ids(dir, filter);
        assert!(!expected.is_empty(), "the scan oracle must find a row");
        let visible_rows = SegmentReader::open(dir).unwrap().read_row_count().unwrap();
        if let Ok(actual) = candidate_row_ids(dir, filter, true, visible_rows) {
            assert_eq!(
                actual, expected,
                "{context} must produce the scan result or an explicit integrity error"
            );
        }
    }

    fn copy_index_directory(source: &Path, target: &Path) {
        fs::create_dir_all(target).unwrap();
        for entry in fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            fs::copy(entry.path(), target.join(entry.file_name())).unwrap();
        }
    }

    fn setup_storage() -> (TempDir, PartitionManager) {
        setup_storage_with_rows(&make_test_rows())
    }

    fn setup_storage_with_rows(rows: &[LogRow]) -> (TempDir, PartitionManager) {
        let tmp = TempDir::new().unwrap();
        let mut storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            partition_target_rows: 1_000_000,
            compaction_safety_margin_blocks: 2_048,
        })
        .unwrap();
        storage.write_batch(rows).unwrap();
        IndexBuilder::build_all_indexes(&storage.hot_partition().meta.path).unwrap();
        storage.checkpoint().unwrap();
        (tmp, storage)
    }

    #[test]
    fn divergent_native_clones_do_not_share_index_publications() {
        fn copy_fixture_tree(source: &Path, target: &Path) {
            fs::create_dir_all(target).unwrap();
            for entry in fs::read_dir(source).unwrap() {
                let entry = entry.unwrap();
                let kind = entry.file_type().unwrap();
                let destination = target.join(entry.file_name());
                if kind.is_dir() {
                    copy_fixture_tree(&entry.path(), &destination);
                } else {
                    assert!(kind.is_file(), "fixture contains only regular files");
                    fs::copy(entry.path(), destination).unwrap();
                }
            }
        }

        let (source_tmp, mut source) = setup_storage();
        source.checkpoint_durable().unwrap();
        drop(source);
        let target_tmp = TempDir::new().unwrap();
        copy_fixture_tree(source_tmp.path(), target_tmp.path());
        let reopen = |path: &Path| {
            PartitionManager::open(PartitionManagerConfig {
                data_dir: path.to_path_buf(),
                partition_target_rows: 1_000_000,
                compaction_safety_margin_blocks: 2_048,
            })
            .unwrap()
        };
        let mut source = reopen(source_tmp.path());
        let mut target = reopen(target_tmp.path());
        assert_eq!(
            SegmentReader::open(&source.hot_partition().meta.path)
                .unwrap()
                .source_namespace(),
            SegmentReader::open(&target.hot_partition().meta.path)
                .unwrap()
                .source_namespace(),
            "both copies must descend from the same source incarnation"
        );
        for (storage, mut rows) in [
            (&mut source, make_test_rows()),
            (&mut target, make_alternate_rows()),
        ] {
            for row in &mut rows {
                row.block_number += 2;
                row.timestamp += 24;
            }
            storage.write_batch(&rows).unwrap();
            storage.checkpoint_durable().unwrap();
            IndexBuilder::build_all_indexes(&storage.hot_partition().meta.path).unwrap();
        }
        let source_dir = source.hot_partition().meta.path.clone();
        let target_dir = target.hot_partition().meta.path.clone();
        let filter = NativeLogFilter::new().with_addresses(vec![Address::repeat_byte(0xCC)]);
        let expected = full_scan_row_ids(&target_dir, &filter);
        assert_eq!(expected, vec![2]);
        assert_eq!(
            candidate_row_ids(&target_dir, &filter, true, 4).unwrap(),
            expected
        );
        drop(source);
        drop(target);
        copy_index_directory(&source_dir.join("indexes"), &target_dir.join("indexes"));
        assert_indexed_result_matches_scan_or_errors(
            &target_dir,
            &filter,
            "a complete index set from a divergently appended database copy",
        );
    }

    #[test]
    fn complete_native_checkpoint_set_copied_across_datasets_is_not_silently_trusted() {
        let (_source_tmp, source) = setup_storage_with_rows(&make_test_rows());
        let (_target_tmp, target) = setup_storage_with_rows(&make_alternate_rows());
        let source_dir = &source.hot_partition().meta.path;
        let target_dir = &target.hot_partition().meta.path;
        copy_index_directory(&source_dir.join("indexes"), &target_dir.join("indexes"));
        let filter = NativeLogFilter::new().with_addresses(vec![Address::repeat_byte(0xCC)]);

        assert_indexed_result_matches_scan_or_errors(
            target_dir,
            &filter,
            "a complete native checkpoint set from another dataset",
        );
    }

    #[test]
    fn complete_same_kind_index_copied_across_sources_is_not_silently_trusted() {
        let tmp = TempDir::new().unwrap();
        let source = tmp.path().join("source");
        let target = tmp.path().join("target");
        write_legacy_source(&source, &make_test_rows());
        write_legacy_source(&target, &make_alternate_rows());
        IndexBuilder::build_all_indexes(&source).unwrap();
        IndexBuilder::build_all_indexes(&target).unwrap();

        fs::copy(
            source.join("indexes/block_hash.bptree"),
            target.join("indexes/block_hash.bptree"),
        )
        .unwrap();
        let filter = NativeLogFilter::new().with_block_hash(B256::repeat_byte(0x32));

        assert_indexed_result_matches_scan_or_errors(
            &target,
            &filter,
            "a complete block-hash index from another source",
        );
    }

    #[test]
    fn complete_same_width_different_kind_index_is_not_silently_trusted() {
        let tmp = TempDir::new().unwrap();
        write_legacy_source(tmp.path(), &make_test_rows());
        IndexBuilder::build_all_indexes(tmp.path()).unwrap();

        fs::copy(
            tmp.path().join("indexes/timestamp.bptree"),
            tmp.path().join("indexes/block_number.bptree"),
        )
        .unwrap();
        let filter = NativeLogFilter::new().with_block_range(Some(100), Some(100));

        assert_indexed_result_matches_scan_or_errors(
            tmp.path(),
            &filter,
            "a complete timestamp index substituted for the block-number index",
        );
    }

    #[test]
    fn complete_same_width_composite_index_is_not_silently_trusted() {
        let tmp = TempDir::new().unwrap();
        write_legacy_source(tmp.path(), &make_test_rows());
        IndexBuilder::build_all_indexes(tmp.path()).unwrap();

        fs::copy(
            tmp.path().join("indexes/address_topic0_topic2.bptree"),
            tmp.path().join("indexes/address_topic0_topic1.bptree"),
        )
        .unwrap();
        let filter = NativeLogFilter::new()
            .with_addresses(vec![Address::repeat_byte(0xAA)])
            .with_topic(0, TopicConstraint::One(B256::repeat_byte(0x10)))
            .with_topic(1, TopicConstraint::One(B256::repeat_byte(0x20)));

        assert_indexed_result_matches_scan_or_errors(
            tmp.path(),
            &filter,
            "a complete topic2 composite substituted for the same-width topic1 composite",
        );
    }

    #[test]
    fn complete_bloom_copied_across_sources_is_not_silently_trusted() {
        let tmp = TempDir::new().unwrap();
        let source = tmp.path().join("source");
        let target = tmp.path().join("target");
        let mut source_rows = make_test_rows();
        let mut target_rows = make_alternate_rows();
        for rows in [&mut source_rows, &mut target_rows] {
            rows[0].topic0 = Some(transfer_topic0());
        }
        write_legacy_source(&source, &source_rows);
        write_legacy_source(&target, &target_rows);
        IndexBuilder::build_all_indexes(&source).unwrap();
        IndexBuilder::build_all_indexes(&target).unwrap();

        fs::copy(
            source.join("indexes").join(ERC20_EVENTS_BLOOM_FILE),
            target.join("indexes").join(ERC20_EVENTS_BLOOM_FILE),
        )
        .unwrap();
        let filter = NativeLogFilter::new()
            .with_addresses(vec![Address::repeat_byte(0xCC)])
            .with_topic(0, TopicConstraint::One(transfer_topic0()))
            .with_topic(1, TopicConstraint::One(B256::repeat_byte(0x20)));

        let segment = SegmentReader::open(&target).unwrap();
        let checkpoint = IndexReadCheckpoint::open(&target, &segment)
            .unwrap()
            .unwrap();
        assert_eq!(
            erc20_event_bloom_exclusions(
                &target.join("indexes"),
                &checkpoint,
                std::slice::from_ref(&filter),
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidData
        );

        assert_indexed_result_matches_scan_or_errors(
            &target,
            &filter,
            "a complete ERC-20 bloom from another source",
        );
    }

    #[test]
    fn complete_checkpoint_and_indexes_copied_across_legacy_sources_are_not_silently_trusted() {
        let tmp = TempDir::new().unwrap();
        let source = tmp.path().join("source");
        let target = tmp.path().join("target");
        write_legacy_source(&source, &make_test_rows());
        write_legacy_source(&target, &make_alternate_rows());
        IndexBuilder::build_all_indexes(&source).unwrap();

        copy_index_directory(&source.join("indexes"), &target.join("indexes"));
        let filter = NativeLogFilter::new().with_addresses(vec![Address::repeat_byte(0xCC)]);

        assert_indexed_result_matches_scan_or_errors(
            &target,
            &filter,
            "a complete checkpoint and index set from an equal-row legacy source",
        );
    }

    #[test]
    fn equal_row_legacy_source_replacement_does_not_silently_reuse_old_indexes() {
        let tmp = TempDir::new().unwrap();
        write_legacy_source(tmp.path(), &make_test_rows());
        IndexBuilder::build_all_indexes(tmp.path()).unwrap();

        // Replace every source column while preserving the row boundary. The
        // independently scanned source now differs from the published indexes.
        write_legacy_source(tmp.path(), &make_alternate_rows());
        let filter = NativeLogFilter::new().with_addresses(vec![Address::repeat_byte(0xCC)]);

        assert_indexed_result_matches_scan_or_errors(
            tmp.path(),
            &filter,
            "indexes published before an equal-row legacy source replacement",
        );
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
