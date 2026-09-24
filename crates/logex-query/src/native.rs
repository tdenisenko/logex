use std::io;
use std::path::Path;

use alloy_primitives::{Address, B256};
use roaring::RoaringBitmap;

use logex_index::{
    BTreeIndexReader, CompositeQuery, ERC20_EVENTS_BLOOM_FILE, Erc20EventBloomReader, QueryBitmap,
    TRANSFER_BLOOM_FILE, TransferBloomReader, is_common_erc20_event_topic0, transfer_topic0,
};
use logex_storage::native::{LogOrder, NativeLogFilter, ReadViewToken, TopicConstraint};
use logex_storage::{IndexReadCheckpoint, PartitionManager, SegmentReader};
use logex_types::{LogRow, PartitionMeta, QueryBuffer, QueryMemoryBudget};

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

    /// Check whether reorg or storage-close invalidation changed this view.
    /// Returns `WouldBlock` when callers must retry with a fresh snapshot.
    /// Call again after response conversion, including failed execution, to
    /// prevent an invalidated view from becoming a successful response.
    pub fn validate(&self) -> io::Result<()> {
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

/// Execute a native query with source, candidate, row and sorting allocations
/// charged to a shared budget. Returned rows and payload aliases retain their
/// own backing charges. Snapshot/path metadata remains planning overhead.
pub fn execute_log_filter_on_snapshot_with_memory(
    snapshot: &StorageSnapshot,
    filter: &NativeLogFilter,
    cancel: Option<&crate::QueryCancelCheck>,
    memory: &QueryMemoryBudget,
) -> io::Result<QueryBuffer<LogRow>> {
    snapshot.validate()?;
    let result = (|| {
        check_candidate_canceled(cancel)?;
        let mut rows = QueryBuffer::try_with_capacity(0, Some(memory), "native query rows")?;
        if filter.limit == Some(0) || filter.min_topic_count > filter.topics.len() {
            return Ok(rows);
        }
        let scan_limit = filter
            .limit
            .map(|limit| limit.saturating_add(filter.offset));
        for partition in snapshot.partitions_in_order(filter.order) {
            snapshot.validate()?;
            check_candidate_canceled(cancel)?;
            if ordered_page_is_complete(&partition, &rows, filter.order, scan_limit) {
                break;
            }
            if !partition_matches_filter(&partition, filter) {
                continue;
            }
            let mut selected = scan_native_partition_with_memory(
                &partition.path,
                filter,
                scan_limit,
                cancel,
                partition.row_count,
                memory,
            )?;
            rows.try_append(&mut selected)?;
            drop(selected);
            retain_ordered_prefix_with_memory(&mut rows, filter.order, scan_limit, memory, cancel)?;
        }
        sort_native_rows_with_memory(&mut rows, filter.order, memory, cancel)?;
        rows.remove_prefix(filter.offset);
        if let Some(limit) = filter.limit {
            rows.truncate(limit);
        }
        check_candidate_canceled(cancel)?;
        Ok(rows)
    })();
    // Invalidation takes priority even if allocation or cancellation also failed.
    snapshot.validate()?;
    result
}

pub(crate) fn scan_native_partition_with_memory(
    path: &Path,
    filter: &NativeLogFilter,
    limit: Option<usize>,
    cancel: Option<&crate::QueryCancelCheck>,
    visible_rows: u64,
    memory: &QueryMemoryBudget,
) -> io::Result<QueryBuffer<LogRow>> {
    check_candidate_canceled(cancel)?;
    if limit == Some(0) {
        return QueryBuffer::try_with_capacity(0, Some(memory), "native log rows");
    }
    let mut ids = candidate_row_ids_with_memory(path, filter, true, visible_rows, memory, cancel)?;
    if ids.is_empty() {
        return QueryBuffer::try_with_capacity(0, Some(memory), "native log rows");
    }
    order_native_row_ids_with_memory(path, &mut ids, filter.order, limit, memory, cancel)?;
    const COLUMNS: &[&str] = &[
        "block_number",
        "block_hash",
        "timestamp",
        "tx_hash",
        "tx_index",
        "log_index",
        "address",
        "topic0",
        "topic1",
        "topic2",
        "topic3",
        "data",
        "data_len",
        "source",
    ];
    let reader = SegmentReader::open_projected_with_memory(path, COLUMNS, memory.clone())?;
    let canceled = || cancel.is_some_and(|check| check());
    // One admitted selection avoids repeatedly reading raw column prefixes.
    // Exact candidates allow prefix selection before payload materialization.
    let mut rows = reader.read_log_rows_with_memory(&ids, Some(&canceled))?;
    let mut position = 0usize;
    let mut interrupted = false;
    rows.retain(|row| {
        if position.is_multiple_of(256) {
            interrupted |= canceled();
        }
        position += 1;
        interrupted || matches_native_filter(row, filter)
    });
    if interrupted {
        return Err(io::Error::new(io::ErrorKind::Interrupted, "query canceled"));
    }
    check_candidate_canceled(cancel)?;
    Ok(rows)
}

fn order_native_row_ids_with_memory(
    path: &Path,
    ids: &mut QueryBuffer<u32>,
    order: LogOrder,
    limit: Option<usize>,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> io::Result<()> {
    if ids.len() < 2 {
        if let Some(limit) = limit {
            ids.truncate(limit);
        }
        return check_candidate_canceled(cancel);
    }
    let reader = SegmentReader::open_projected_with_memory(
        path,
        &["block_number", "tx_index", "log_index"],
        memory.clone(),
    )?;
    check_candidate_canceled(cancel)?;
    let blocks = reader.read_u64_with_memory("block_number", Some(ids))?;
    check_candidate_canceled(cancel)?;
    let transactions = reader.read_u32_with_memory("tx_index", Some(ids))?;
    check_candidate_canceled(cancel)?;
    let logs = reader.read_u32_with_memory("log_index", Some(ids))?;
    if [blocks.len(), transactions.len(), logs.len()]
        .into_iter()
        .any(|len| len != ids.len())
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "native ordering columns differ in length",
        ));
    }
    let mut keyed =
        QueryBuffer::try_with_capacity(ids.len(), Some(memory), "native ordering keys")?;
    for (position, &id) in ids.iter().enumerate() {
        if position.is_multiple_of(256) {
            check_candidate_canceled(cancel)?;
        }
        keyed.try_push((
            (blocks[position], transactions[position], logs[position]),
            position,
            id,
        ))?;
    }
    drop((blocks, transactions, logs, reader));
    // Original position makes equal log keys stable without allocating opaque
    // stable-sort scratch. Only this explicitly owned key buffer is sorted.
    keyed.sort_unstable_by(|left, right| {
        compare_log_keys(left.0, right.0, order).then_with(|| left.1.cmp(&right.1))
    });
    check_candidate_canceled(cancel)?;
    if let Some(limit) = limit {
        keyed.truncate(limit);
    }
    ids.truncate(keyed.len());
    for (output, &(_, _, id)) in ids.iter_mut().zip(keyed.iter()) {
        *output = id;
    }
    Ok(())
}

fn compare_log_keys(
    left: (u64, u32, u32),
    right: (u64, u32, u32),
    order: LogOrder,
) -> std::cmp::Ordering {
    match order {
        LogOrder::Ascending => left.cmp(&right),
        LogOrder::Descending => right.cmp(&left),
    }
}

/// Keep stable tie behavior using an explicitly owned permutation, then move
/// rows in place. The index sort itself uses no heap scratch. Cancellation is
/// cooperative before/after that admitted sort and during the permutation.
pub(crate) fn sort_native_rows_with_memory(
    rows: &mut [LogRow],
    order: LogOrder,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> io::Result<()> {
    check_candidate_canceled(cancel)?;
    if rows.len() < 2 {
        return Ok(());
    }
    let mut ordered = true;
    for (position, pair) in rows.windows(2).enumerate() {
        if position.is_multiple_of(256) {
            check_candidate_canceled(cancel)?;
        }
        if compare_log_keys(
            native_log_sort_key(&pair[0]),
            native_log_sort_key(&pair[1]),
            order,
        )
        .is_gt()
        {
            ordered = false;
            break;
        }
    }
    if ordered {
        return check_candidate_canceled(cancel);
    }
    let mut positions =
        QueryBuffer::try_with_capacity(rows.len(), Some(memory), "native sort positions")?;
    positions.try_extend(0..rows.len())?;
    positions.sort_unstable_by(|&left, &right| {
        compare_log_keys(
            native_log_sort_key(&rows[left]),
            native_log_sort_key(&rows[right]),
            order,
        )
        .then_with(|| left.cmp(&right))
    });
    check_candidate_canceled(cancel)?;
    let mut moved = 0usize;
    for start in 0..positions.len() {
        if start.is_multiple_of(256) {
            check_candidate_canceled(cancel)?;
        }
        let mut destination = start;
        while positions[destination] != start {
            if moved.is_multiple_of(256) {
                check_candidate_canceled(cancel)?;
            }
            moved += 1;
            let source = positions[destination];
            rows.swap(destination, source);
            positions[destination] = destination;
            destination = source;
        }
        positions[destination] = destination;
    }
    check_candidate_canceled(cancel)
}

pub(crate) fn retain_ordered_prefix_with_memory(
    rows: &mut QueryBuffer<LogRow>,
    order: LogOrder,
    limit: Option<usize>,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> io::Result<()> {
    if let Some(limit) = limit {
        sort_native_rows_with_memory(rows, order, memory, cancel)?;
        rows.truncate(limit);
    }
    Ok(())
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
    if filter.limit == Some(0) || filter.min_topic_count > filter.topics.len() {
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
    let columns = candidate_refinement_columns(filter);
    let reader = SegmentReader::open_projected(dir, &columns)?;
    candidate_row_ids_for_reader(dir, &reader, filter, use_indexes, false, visible_rows)
}

/// Build an exact, query-owned SQL candidate selection. Index and source
/// allocations share `memory`; the returned buffer retains its charge through
/// physical-plan execution.
pub(crate) fn candidate_row_ids_with_memory(
    dir: &Path,
    filter: &NativeLogFilter,
    use_indexes: bool,
    visible_rows: u64,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> std::io::Result<QueryBuffer<u32>> {
    candidate_row_ids_for_filters_with_memory(
        dir,
        std::slice::from_ref(filter),
        use_indexes,
        visible_rows,
        memory,
        cancel,
    )
}

/// Open one accounted source capture and build the sorted, unique union for
/// every filter. The returned row IDs retain their own charge; the projected
/// source owner is released before this function returns.
pub(crate) fn candidate_row_ids_for_filters_with_memory(
    dir: &Path,
    filters: &[NativeLogFilter],
    use_indexes: bool,
    visible_rows: u64,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> std::io::Result<QueryBuffer<u32>> {
    check_candidate_canceled(cancel)?;
    if filters.is_empty() {
        return QueryBuffer::try_with_capacity(0, Some(memory), "query candidate row ids");
    }
    let columns = candidate_refinement_columns_for_filters(filters);
    let reader = SegmentReader::open_projected_with_memory(dir, &columns, memory.clone())?;
    candidate_row_ids_for_filters_on_reader_with_memory(
        dir,
        &reader,
        filters,
        use_indexes,
        visible_rows,
        memory,
        cancel,
    )
}

/// Build a sorted, unique candidate union while borrowing a caller-owned,
/// accounted source capture. This lets grouped queries retain the same source
/// owner for later projection without reopening or recapturing the segment.
pub(crate) fn candidate_row_ids_for_filters_on_reader_with_memory(
    dir: &Path,
    reader: &SegmentReader,
    filters: &[NativeLogFilter],
    use_indexes: bool,
    visible_rows: u64,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> std::io::Result<QueryBuffer<u32>> {
    check_candidate_canceled(cancel)?;
    let physical_rows = reader.read_row_count()?;
    if visible_rows > physical_rows {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment is missing rows from the captured query snapshot",
        ));
    }
    if visible_rows > u64::from(u32::MAX) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment row count exceeds candidate address space",
        ));
    }
    if filters.is_empty() || visible_rows == 0 {
        return QueryBuffer::try_with_capacity(0, Some(memory), "query candidate row ids");
    }
    if filters
        .iter()
        .all(|filter| filter.min_topic_count > filter.topics.len())
    {
        return QueryBuffer::try_with_capacity(0, Some(memory), "query candidate row ids");
    }

    let checkpoint = if use_indexes {
        IndexReadCheckpoint::open(dir, reader)?
    } else {
        None
    };
    let bloom_exclusions = match checkpoint.as_ref() {
        Some(checkpoint) => erc20_event_bloom_exclusions_with_memory(
            &dir.join("indexes"),
            checkpoint,
            filters,
            memory,
            cancel,
        )?,
        None => AccountedBloomExclusions::None,
    };
    check_candidate_canceled(cancel)?;

    let common_canonical =
        filters
            .first()
            .map(|filter| filter.canonical_only)
            .filter(|canonical| {
                filters
                    .iter()
                    .all(|filter| filter.canonical_only == *canonical)
            });
    let hoist_canonical = filters.len() > 1 && common_canonical == Some(true);
    let mut union = QueryBuffer::try_with_capacity(0, Some(memory), "query candidate row ids")?;
    for (index, filter) in filters.iter().enumerate() {
        check_candidate_canceled(cancel)?;
        let bloom_excluded = bloom_exclusions.excludes(index);
        let row_ids = candidate_row_ids_for_filter_on_accounted_reader(
            dir,
            reader,
            checkpoint.as_ref(),
            filter,
            bloom_excluded,
            !hoist_canonical,
            physical_rows,
            visible_rows,
            memory,
            cancel,
        )?;
        union = merge_sorted_candidate_ids(union, row_ids, memory, cancel)?;
    }
    drop(bloom_exclusions);
    if hoist_canonical && !union.is_empty() {
        union = retain_canonical_candidate_ids(reader, visible_rows, Some(union), memory, cancel)?;
    }
    check_candidate_canceled(cancel)?;
    Ok(union)
}

#[allow(clippy::too_many_arguments)]
fn candidate_row_ids_for_filter_on_accounted_reader(
    dir: &Path,
    reader: &SegmentReader,
    checkpoint: Option<&IndexReadCheckpoint>,
    filter: &NativeLogFilter,
    bloom_excluded: bool,
    apply_canonical: bool,
    physical_rows: u64,
    visible_rows: u64,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> io::Result<QueryBuffer<u32>> {
    if filter.min_topic_count > filter.topics.len() || bloom_excluded {
        return QueryBuffer::try_with_capacity(0, Some(memory), "query candidate row ids");
    }

    let mut row_ids = if let Some(checkpoint) = checkpoint {
        build_index_candidate_set(dir, checkpoint, filter, Some(memory), cancel)?
            .map(|candidate| candidate.into_row_ids(memory))
            .transpose()?
    } else {
        None
    };
    if let Some(ids) = &mut row_ids {
        if ids
            .last()
            .is_some_and(|row| u64::from(*row) >= physical_rows)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "index row exceeds its source segment",
            ));
        }
        ids.truncate(ids.partition_point(|row| u64::from(*row) < visible_rows));
        if ids.is_empty() {
            return Ok(row_ids.expect("candidate selection exists"));
        }
    }

    refine_candidate_ids_from_columns(reader, filter, visible_rows, memory, cancel, &mut row_ids)?;
    check_candidate_canceled(cancel)?;
    if apply_canonical && filter.canonical_only {
        row_ids = Some(retain_canonical_candidate_ids(
            reader,
            visible_rows,
            row_ids,
            memory,
            cancel,
        )?);
    }
    check_candidate_canceled(cancel)?;
    row_ids.map_or_else(|| query_row_id_range(visible_rows, memory), Ok)
}

fn retain_canonical_candidate_ids(
    reader: &SegmentReader,
    visible_rows: u64,
    mut row_ids: Option<QueryBuffer<u32>>,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> io::Result<QueryBuffer<u32>> {
    check_candidate_canceled(cancel)?;
    let canonical = reader.read_canonical_accounted()?;
    if canonical.len() < visible_rows {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "canonical bitmap is shorter than the captured query snapshot",
        ));
    }
    if let Some(mut ids) = row_ids.take() {
        let mut retained = 0usize;
        for position in 0..ids.len() {
            if position.is_multiple_of(256) {
                check_candidate_canceled(cancel)?;
            }
            let row = ids[position];
            if canonical.is_present(u64::from(row)) {
                ids[retained] = row;
                retained += 1;
            }
        }
        ids.truncate(retained);
        return Ok(ids);
    }

    let mut matches = 0usize;
    for row in 0..visible_rows {
        if row.is_multiple_of(256) {
            check_candidate_canceled(cancel)?;
        }
        if canonical.is_present(row) {
            matches = matches.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "canonical candidate count exceeds address space",
                )
            })?;
        }
    }
    let mut ids = QueryBuffer::try_with_capacity(matches, Some(memory), "query candidate row ids")?;
    for row in 0..visible_rows {
        if row.is_multiple_of(256) {
            check_candidate_canceled(cancel)?;
        }
        if canonical.is_present(row) {
            ids.try_push(row as u32)?;
        }
    }
    Ok(ids)
}

fn candidate_refinement_columns(filter: &NativeLogFilter) -> Vec<&'static str> {
    candidate_refinement_columns_for_filters(std::slice::from_ref(filter))
}

/// Return the deterministic physical-column union needed to refine every
/// filter from one projected segment capture.
pub(crate) fn candidate_refinement_columns_for_filters(
    filters: &[NativeLogFilter],
) -> Vec<&'static str> {
    let mut columns = Vec::new();
    for (name, needed) in [
        (
            "address",
            filters.iter().any(|filter| !filter.addresses.is_empty()),
        ),
        (
            "block_hash",
            filters.iter().any(|filter| filter.block_hash.is_some()),
        ),
        (
            "block_number",
            filters
                .iter()
                .any(|filter| filter.from_block.is_some() || filter.to_block.is_some()),
        ),
        (
            "timestamp",
            filters
                .iter()
                .any(|filter| filter.from_timestamp.is_some() || filter.to_timestamp.is_some()),
        ),
        (
            "data_len",
            filters.iter().any(|filter| filter.data_len.is_some()),
        ),
        (
            "data",
            filters.iter().any(|filter| {
                filter.data_min.is_some()
                    || filter.data_max.is_some()
                    || !filter.data_not_equals.is_empty()
            }),
        ),
    ] {
        if needed {
            columns.push(name);
        }
    }
    for (index, name) in ["topic0", "topic1", "topic2", "topic3"]
        .into_iter()
        .enumerate()
    {
        if filters.iter().any(|filter| {
            index < filter.min_topic_count || !matches!(&filter.topics[index], TopicConstraint::Any)
        }) {
            columns.push(name);
        }
    }
    columns
}

fn merge_sorted_candidate_ids(
    left: QueryBuffer<u32>,
    right: QueryBuffer<u32>,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> io::Result<QueryBuffer<u32>> {
    if left.is_empty() {
        return Ok(right);
    }
    if right.is_empty() {
        return Ok(left);
    }

    let mut left_index = 0usize;
    let mut right_index = 0usize;
    let mut union_len = 0usize;
    let mut steps = 0usize;
    while left_index < left.len() || right_index < right.len() {
        if steps.is_multiple_of(256) {
            check_candidate_canceled(cancel)?;
        }
        match (left.get(left_index), right.get(right_index)) {
            (Some(left_value), Some(right_value)) if left_value == right_value => {
                left_index += 1;
                right_index += 1;
            }
            (Some(left_value), Some(right_value)) if left_value < right_value => left_index += 1,
            (Some(_), Some(_)) => right_index += 1,
            (Some(_), None) => left_index += 1,
            (None, Some(_)) => right_index += 1,
            (None, None) => break,
        }
        union_len = union_len.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "candidate union exceeds address space",
            )
        })?;
        steps += 1;
    }
    check_candidate_canceled(cancel)?;
    if union_len == left.len() {
        return Ok(left);
    }
    if union_len == right.len() {
        return Ok(right);
    }

    let mut union =
        QueryBuffer::try_with_capacity(union_len, Some(memory), "query candidate row ids")?;
    left_index = 0;
    right_index = 0;
    steps = 0;
    while left_index < left.len() || right_index < right.len() {
        if steps.is_multiple_of(256) {
            check_candidate_canceled(cancel)?;
        }
        let value = match (left.get(left_index), right.get(right_index)) {
            (Some(left_value), Some(right_value)) if left_value == right_value => {
                left_index += 1;
                right_index += 1;
                *left_value
            }
            (Some(left_value), Some(right_value)) if left_value < right_value => {
                left_index += 1;
                *left_value
            }
            (Some(_), Some(right_value)) => {
                right_index += 1;
                *right_value
            }
            (Some(left_value), None) => {
                left_index += 1;
                *left_value
            }
            (None, Some(right_value)) => {
                right_index += 1;
                *right_value
            }
            (None, None) => break,
        };
        union.try_push(value)?;
        steps += 1;
    }
    debug_assert_eq!(union.len(), union_len);
    check_candidate_canceled(cancel)?;
    Ok(union)
}

fn check_candidate_canceled(cancel: Option<&crate::QueryCancelCheck>) -> io::Result<()> {
    if cancel.is_some_and(|check| check()) {
        Err(io::Error::new(io::ErrorKind::Interrupted, "query canceled"))
    } else {
        Ok(())
    }
}

fn query_row_id_range(row_count: u64, memory: &QueryMemoryBudget) -> io::Result<QueryBuffer<u32>> {
    let capacity = usize::try_from(row_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "candidate row count exceeds address space",
        )
    })?;
    let mut result =
        QueryBuffer::try_with_capacity(capacity, Some(memory), "query candidate row ids")?;
    result.try_extend(0..row_count as u32)?;
    Ok(result)
}

fn refine_candidate_ids_with_values<T>(
    row_ids: &mut Option<QueryBuffer<u32>>,
    row_count: u64,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
    values: &QueryBuffer<T>,
    matches: impl Fn(&T) -> bool,
) -> io::Result<()> {
    if let Some(ids) = row_ids {
        if values.len() != ids.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "selected candidate column returned the wrong row count",
            ));
        }
        let mut retained = 0usize;
        for position in 0..ids.len() {
            if position.is_multiple_of(256) {
                check_candidate_canceled(cancel)?;
            }
            if matches(&values[position]) {
                let row = ids[position];
                ids[retained] = row;
                retained += 1;
            }
        }
        ids.truncate(retained);
        return Ok(());
    }

    let expected = usize::try_from(row_count).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "candidate row count exceeds address space",
        )
    })?;
    if values.len() < expected {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "candidate column is shorter than the captured row count",
        ));
    }
    // Appends after snapshot capture may make the pinned physical column longer
    // than this query's visible prefix. Never admit those later rows.
    let visible = &values[..expected];
    let mut count = 0usize;
    for (position, value) in visible.iter().enumerate() {
        if position.is_multiple_of(256) {
            check_candidate_canceled(cancel)?;
        }
        if matches(value) {
            count = count.checked_add(1).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "candidate row count exceeds address space",
                )
            })?;
        }
    }
    let mut ids = QueryBuffer::try_with_capacity(count, Some(memory), "query candidate row ids")?;
    for (row, value) in visible.iter().enumerate() {
        if row.is_multiple_of(256) {
            check_candidate_canceled(cancel)?;
        }
        if matches(value) {
            ids.try_push(row as u32)?;
        }
    }
    *row_ids = Some(ids);
    Ok(())
}

fn refine_candidate_ids_from_columns(
    reader: &SegmentReader,
    filter: &NativeLogFilter,
    row_count: u64,
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
    row_ids: &mut Option<QueryBuffer<u32>>,
) -> io::Result<()> {
    macro_rules! refine {
        ($read:expr, $matches:expr) => {{
            check_candidate_canceled(cancel)?;
            let values = $read?;
            refine_candidate_ids_with_values(
                row_ids, row_count, memory, cancel, &values, $matches,
            )?;
            if row_ids.as_ref().is_some_and(|ids| ids.is_empty()) {
                return Ok(());
            }
        }};
    }

    if !filter.addresses.is_empty() {
        refine!(
            reader.read_address_with_memory(row_ids.as_deref()),
            |value| filter.addresses.contains(value)
        );
    }
    if let Some(block_hash) = filter.block_hash {
        refine!(
            reader.read_b256_with_memory("block_hash", row_ids.as_deref()),
            |value| *value == block_hash
        );
    }
    for (index, constraint) in filter.topics.iter().enumerate() {
        if index >= filter.min_topic_count && matches!(constraint, TopicConstraint::Any) {
            continue;
        }
        let column = format!("topic{index}");
        refine!(
            reader.read_nullable_b256_with_memory(&column, row_ids.as_deref()),
            |topic| {
                (index >= filter.min_topic_count || topic.is_some())
                    && topic_matches_constraint(*topic, constraint)
            }
        );
    }
    if filter.from_block.is_some() || filter.to_block.is_some() {
        refine!(
            reader.read_u64_with_memory("block_number", row_ids.as_deref()),
            |block| filter.from_block.is_none_or(|from| *block >= from)
                && filter.to_block.is_none_or(|to| *block <= to)
        );
    }
    if filter.from_timestamp.is_some() || filter.to_timestamp.is_some() {
        refine!(
            reader.read_u64_with_memory("timestamp", row_ids.as_deref()),
            |timestamp| filter.from_timestamp.is_none_or(|from| *timestamp >= from)
                && filter.to_timestamp.is_none_or(|to| *timestamp <= to)
        );
    }
    if let Some(data_len) = filter.data_len {
        refine!(
            reader.read_u32_with_memory("data_len", row_ids.as_deref()),
            |value| *value == data_len
        );
    }
    if filter.data_min.is_some() || filter.data_max.is_some() || !filter.data_not_equals.is_empty()
    {
        refine!(
            reader.read_var_bytes_with_memory("data", row_ids.as_deref()),
            |data| filter
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
        );
    }
    Ok(())
}

pub(crate) fn candidate_row_ids_for_reader(
    dir: &Path,
    reader: &SegmentReader,
    filter: &NativeLogFilter,
    use_indexes: bool,
    event_bloom_prechecked: bool,
    row_count: u64,
) -> std::io::Result<Vec<u32>> {
    if filter.min_topic_count > filter.topics.len() {
        return Ok(Vec::new());
    }
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

/// Check predicates carried by a log row. Callers handle canonical bitmap
/// enforcement, result ordering and pagination separately.
pub fn matches_native_filter(row: &LogRow, filter: &NativeLogFilter) -> bool {
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

    if filter.min_topic_count > filter.topics.len() {
        return false;
    }
    let row_topics = [row.topic0, row.topic1, row.topic2, row.topic3];
    row_topics
        .into_iter()
        .zip(&filter.topics)
        .enumerate()
        .all(|(index, (topic, constraint))| {
            (index >= filter.min_topic_count || topic.is_some())
                && topic_matches_constraint(topic, constraint)
        })
}

enum CandidateSet {
    Plain(RoaringBitmap),
    Accounted(QueryBitmap),
}

impl CandidateSet {
    fn is_empty(&self) -> bool {
        match self {
            Self::Plain(bitmap) => bitmap.is_empty(),
            Self::Accounted(bitmap) => bitmap.is_empty(),
        }
    }

    fn union(self, rhs: Self) -> io::Result<Self> {
        match (self, rhs) {
            (Self::Plain(lhs), Self::Plain(rhs)) => Ok(Self::Plain(lhs | rhs)),
            (Self::Accounted(lhs), Self::Accounted(rhs)) => lhs.union(rhs).map(Self::Accounted),
            _ => Err(io::Error::other(
                "candidate sets use different ownership modes",
            )),
        }
    }

    fn intersection(self, rhs: Self) -> io::Result<Self> {
        match (self, rhs) {
            (Self::Plain(lhs), Self::Plain(rhs)) => Ok(Self::Plain(lhs & rhs)),
            (Self::Accounted(lhs), Self::Accounted(rhs)) => {
                lhs.intersection(rhs).map(Self::Accounted)
            }
            _ => Err(io::Error::other(
                "candidate sets use different ownership modes",
            )),
        }
    }

    fn into_plain(self) -> io::Result<RoaringBitmap> {
        match self {
            Self::Plain(bitmap) => Ok(bitmap),
            Self::Accounted(_) => Err(io::Error::other(
                "accounted candidate used by an unaccounted query",
            )),
        }
    }

    fn into_row_ids(self, memory: &QueryMemoryBudget) -> io::Result<QueryBuffer<u32>> {
        match self {
            Self::Accounted(bitmap) => bitmap.into_row_ids(memory),
            Self::Plain(_) => Err(io::Error::other(
                "unaccounted candidate used by an accounted query",
            )),
        }
    }
}

#[derive(Clone, Copy)]
struct CandidateAccess<'a> {
    memory: Option<&'a QueryMemoryBudget>,
    cancel: Option<&'a crate::QueryCancelCheck>,
}

impl CandidateAccess<'_> {
    fn empty(self) -> io::Result<CandidateSet> {
        match self.memory {
            Some(memory) => QueryBitmap::empty(memory).map(CandidateSet::Accounted),
            None => Ok(CandidateSet::Plain(RoaringBitmap::new())),
        }
    }

    fn point(self, path: &Path, file_id: [u8; 16], key: &[u8]) -> io::Result<Option<CandidateSet>> {
        check_candidate_canceled(self.cancel)?;
        match self.memory {
            Some(memory) => {
                BTreeIndexReader::get_from_file_bound_with_memory(path, file_id, key, memory)
                    .map(|value| value.map(CandidateSet::Accounted))
            }
            None => BTreeIndexReader::get_from_file_bound(path, file_id, key)
                .map(|value| value.map(CandidateSet::Plain)),
        }
    }

    fn range_inclusive(
        self,
        path: &Path,
        file_id: [u8; 16],
        start: &[u8],
        end: &[u8],
    ) -> io::Result<CandidateSet> {
        check_candidate_canceled(self.cancel)?;
        match self.memory {
            Some(memory) => BTreeIndexReader::range_inclusive_from_file_bound_with_memory(
                path, file_id, start, end, memory,
            )
            .map(CandidateSet::Accounted),
            None => {
                let reader = BTreeIndexReader::open_bound(path, file_id)?;
                Ok(CandidateSet::Plain(reader.range_inclusive(start, end)))
            }
        }
    }

    fn composite_range(
        self,
        path: &Path,
        file_id: [u8; 16],
        address: &[u8; 20],
        topic0: &[u8; 32],
        from: u64,
        to: u64,
    ) -> io::Result<CandidateSet> {
        check_candidate_canceled(self.cancel)?;
        match self.memory {
            Some(memory) => {
                CompositeQuery::range_address_topic0_blocks_inclusive_from_file_bound_with_memory(
                    path, file_id, address, topic0, from, to, memory,
                )
                .map(CandidateSet::Accounted)
            }
            None => {
                let reader = BTreeIndexReader::open_bound(path, file_id)?;
                Ok(CandidateSet::Plain(
                    CompositeQuery::range_address_topic0_blocks_inclusive(
                        &reader, address, topic0, from, to,
                    ),
                ))
            }
        }
    }

    fn address_topic0_topic1(
        self,
        path: &Path,
        file_id: [u8; 16],
        address: &[u8; 20],
        topic0: &[u8; 32],
        topic1: &[u8; 32],
    ) -> io::Result<Option<CandidateSet>> {
        check_candidate_canceled(self.cancel)?;
        match self.memory {
            Some(memory) => CompositeQuery::get_address_topic0_topic1_from_file_bound_with_memory(
                path, file_id, address, topic0, topic1, memory,
            )
            .map(|value| value.map(CandidateSet::Accounted)),
            None => CompositeQuery::get_address_topic0_topic1_from_file_bound(
                path, file_id, address, topic0, topic1,
            )
            .map(|value| value.map(CandidateSet::Plain)),
        }
    }

    fn address_topic0_topic2(
        self,
        path: &Path,
        file_id: [u8; 16],
        address: &[u8; 20],
        topic0: &[u8; 32],
        topic2: &[u8; 32],
    ) -> io::Result<Option<CandidateSet>> {
        check_candidate_canceled(self.cancel)?;
        match self.memory {
            Some(memory) => CompositeQuery::get_address_topic0_topic2_from_file_bound_with_memory(
                path, file_id, address, topic0, topic2, memory,
            )
            .map(|value| value.map(CandidateSet::Accounted)),
            None => CompositeQuery::get_address_topic0_topic2_from_file_bound(
                path, file_id, address, topic0, topic2,
            )
            .map(|value| value.map(CandidateSet::Plain)),
        }
    }

    fn address_topic0(
        self,
        path: &Path,
        file_id: [u8; 16],
        address: &[u8; 20],
        topic0: &[u8; 32],
    ) -> io::Result<Option<CandidateSet>> {
        check_candidate_canceled(self.cancel)?;
        match self.memory {
            Some(memory) => CompositeQuery::get_address_topic0_from_file_bound_with_memory(
                path, file_id, address, topic0, memory,
            )
            .map(|value| value.map(CandidateSet::Accounted)),
            None => {
                CompositeQuery::get_address_topic0_from_file_bound(path, file_id, address, topic0)
                    .map(|value| value.map(CandidateSet::Plain))
            }
        }
    }

    fn topic0_topic1(
        self,
        path: &Path,
        file_id: [u8; 16],
        topic0: &[u8; 32],
        topic1: &[u8; 32],
    ) -> io::Result<Option<CandidateSet>> {
        check_candidate_canceled(self.cancel)?;
        match self.memory {
            Some(memory) => CompositeQuery::get_topic0_topic1_from_file_bound_with_memory(
                path, file_id, topic0, topic1, memory,
            )
            .map(|value| value.map(CandidateSet::Accounted)),
            None => {
                CompositeQuery::get_topic0_topic1_from_file_bound(path, file_id, topic0, topic1)
                    .map(|value| value.map(CandidateSet::Plain))
            }
        }
    }
}

fn build_candidate_bitmap(
    dir: &Path,
    segment_reader: &SegmentReader,
    checkpoint: &IndexReadCheckpoint,
    filter: &NativeLogFilter,
    row_count: u64,
) -> std::io::Result<RoaringBitmap> {
    let result = build_index_candidate_set(dir, checkpoint, filter, None, None)?
        .map(CandidateSet::into_plain)
        .transpose()?;
    let result = refine_candidate_bitmap_from_columns(segment_reader, filter, row_count, result)?;
    Ok(result.unwrap_or_else(|| (0..row_count as u32).collect()))
}

fn build_index_candidate_set(
    dir: &Path,
    checkpoint: &IndexReadCheckpoint,
    filter: &NativeLogFilter,
    memory: Option<&QueryMemoryBudget>,
    cancel: Option<&crate::QueryCancelCheck>,
) -> std::io::Result<Option<CandidateSet>> {
    let index_dir = dir.join("indexes");
    let access = CandidateAccess { memory, cancel };
    let mut result: Option<CandidateSet> = None;
    let mut covered_addresses = false;
    let mut covered_topics = [false; 4];
    let mut covered_block_range = false;

    if let Some(block_hash) = filter.block_hash {
        let block_hash_path = index_dir.join("block_hash.bptree");
        if let Some(file_id) = checkpoint.artifact_id("block_hash.bptree") {
            if let Some(bitmap) = access.point(&block_hash_path, file_id, block_hash.as_slice())? {
                result = Some(intersect_optional_set(result, bitmap)?);
            } else {
                return access.empty().map(Some);
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
            let mut union = access.empty()?;
            for topic1 in topic1_values {
                let topic1: &[u8; 32] = topic1
                    .as_slice()
                    .try_into()
                    .expect("B256 has a fixed 32-byte width");
                if let Some(bitmap) = access.address_topic0_topic1(
                    &composite_path,
                    file_id,
                    &address,
                    &topic0,
                    topic1,
                )? {
                    union = union.union(bitmap)?;
                }
            }
            if union.is_empty() {
                return Ok(Some(union));
            }
            result = Some(intersect_optional_set(result, union)?);
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
            let mut union = access.empty()?;
            for topic2 in topic2_values {
                let topic2: &[u8; 32] = topic2
                    .as_slice()
                    .try_into()
                    .expect("B256 has a fixed 32-byte width");
                if let Some(bitmap) = access.address_topic0_topic2(
                    &composite_path,
                    file_id,
                    &address,
                    &topic0,
                    topic2,
                )? {
                    union = union.union(bitmap)?;
                }
            }
            if union.is_empty() {
                return Ok(Some(union));
            }
            result = Some(intersect_optional_set(result, union)?);
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
                let bitmap = access.composite_range(
                    &composite_path,
                    file_id,
                    &address,
                    &topic0,
                    from,
                    to,
                )?;
                result = Some(intersect_optional_set(result, bitmap)?);
                covered_addresses = true;
                covered_topics[0] = true;
                covered_block_range = true;
            }
        }
        (Some(address), Some(topic0), _, _) => {
            let composite_path = index_dir.join("address_topic0.bptree");
            if let Some(file_id) = checkpoint.artifact_id("address_topic0.bptree") {
                if let Some(bitmap) =
                    access.address_topic0(&composite_path, file_id, &address, &topic0)?
                {
                    result = Some(intersect_optional_set(result, bitmap)?);
                    covered_addresses = true;
                    covered_topics[0] = true;
                } else {
                    return access.empty().map(Some);
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
            let mut union = access.empty()?;
            for topic1 in topic1_values {
                let topic1: &[u8; 32] = topic1
                    .as_slice()
                    .try_into()
                    .expect("B256 has a fixed 32-byte width");
                if let Some(bitmap) =
                    access.topic0_topic1(&composite_path, file_id, &topic0, topic1)?
                {
                    union = union.union(bitmap)?;
                }
            }
            if union.is_empty() {
                return Ok(Some(union));
            }
            result = Some(intersect_optional_set(result, union)?);
            covered_topics[0] = true;
            covered_topics[1] = true;
        }
    }

    if !filter.addresses.is_empty() && !covered_addresses {
        let address_path = index_dir.join("address.bptree");
        if let Some(file_id) = checkpoint.artifact_id("address.bptree") {
            let mut union = access.empty()?;
            for address in &filter.addresses {
                if let Some(bitmap) = access.point(&address_path, file_id, address.as_slice())? {
                    union = union.union(bitmap)?;
                }
            }
            if union.is_empty() {
                return Ok(Some(union));
            }
            result = Some(intersect_optional_set(result, union)?);
        }
    }

    if !covered_topics[0]
        && let Some(topic_bitmap) =
            build_topic0_candidate_set(&index_dir, checkpoint, &filter.topics[0], access)?
    {
        if topic_bitmap.is_empty() {
            return Ok(Some(topic_bitmap));
        }
        result = Some(intersect_optional_set(result, topic_bitmap)?);
    }

    if !covered_block_range && (filter.from_block.is_some() || filter.to_block.is_some()) {
        let block_path = index_dir.join("block_number.bptree");
        if let Some(file_id) = checkpoint.artifact_id("block_number.bptree") {
            let from = filter.from_block.unwrap_or(0);
            let to = filter.to_block.unwrap_or(u64::MAX);
            let bitmap = access.range_inclusive(
                &block_path,
                file_id,
                &from.to_be_bytes(),
                &to.to_be_bytes(),
            )?;
            result = Some(intersect_optional_set(result, bitmap)?);
            if result.as_ref().is_some_and(CandidateSet::is_empty) {
                return Ok(result);
            }
        }
    }

    if filter.from_timestamp.is_some() || filter.to_timestamp.is_some() {
        let timestamp_path = index_dir.join("timestamp.bptree");
        if let Some(file_id) = checkpoint.artifact_id("timestamp.bptree") {
            let from = filter.from_timestamp.unwrap_or(0);
            let to = filter.to_timestamp.unwrap_or(u64::MAX);
            let bitmap = access.range_inclusive(
                &timestamp_path,
                file_id,
                &from.to_be_bytes(),
                &to.to_be_bytes(),
            )?;
            result = Some(intersect_optional_set(result, bitmap)?);
            if result.as_ref().is_some_and(CandidateSet::is_empty) {
                return Ok(result);
            }
        }
    }

    Ok(result)
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

#[derive(Debug)]
enum AccountedBloomExclusions {
    None,
    Single(bool),
    Multiple(QueryBuffer<u8>),
}

impl AccountedBloomExclusions {
    fn excludes(&self, index: usize) -> bool {
        match self {
            Self::None => false,
            Self::Single(excluded) => {
                debug_assert_eq!(index, 0);
                *excluded
            }
            Self::Multiple(exclusions) => exclusions[index] != 0,
        }
    }
}

fn erc20_event_bloom_exclusions_with_memory(
    index_dir: &Path,
    checkpoint: &IndexReadCheckpoint,
    filters: &[NativeLogFilter],
    memory: &QueryMemoryBudget,
    cancel: Option<&crate::QueryCancelCheck>,
) -> io::Result<AccountedBloomExclusions> {
    let common_bloom_path = index_dir.join(ERC20_EVENTS_BLOOM_FILE);
    if let Some(file_id) = checkpoint.artifact_id(ERC20_EVENTS_BLOOM_FILE) {
        let mut reader =
            Erc20EventBloomReader::open_bound_with_memory(&common_bloom_path, file_id, memory)?;
        if let [filter] = filters {
            check_candidate_canceled(cancel)?;
            return erc20_event_bloom_reader_excludes(&mut reader, filter)
                .map(AccountedBloomExclusions::Single);
        }
        let mut exclusions =
            QueryBuffer::try_with_capacity(filters.len(), Some(memory), "event bloom exclusions")?;
        for filter in filters {
            check_candidate_canceled(cancel)?;
            exclusions.try_push(u8::from(erc20_event_bloom_reader_excludes(
                &mut reader,
                filter,
            )?))?;
        }
        return Ok(AccountedBloomExclusions::Multiple(exclusions));
    }

    let legacy_transfer_bloom_path = index_dir.join(TRANSFER_BLOOM_FILE);
    if let Some(file_id) = checkpoint.artifact_id(TRANSFER_BLOOM_FILE) {
        let mut reader = TransferBloomReader::open_bound_with_memory(
            &legacy_transfer_bloom_path,
            file_id,
            memory,
        )?;
        if let [filter] = filters {
            check_candidate_canceled(cancel)?;
            return legacy_transfer_bloom_reader_excludes(&mut reader, filter)
                .map(AccountedBloomExclusions::Single);
        }
        let mut exclusions =
            QueryBuffer::try_with_capacity(filters.len(), Some(memory), "event bloom exclusions")?;
        for filter in filters {
            check_candidate_canceled(cancel)?;
            exclusions.try_push(u8::from(legacy_transfer_bloom_reader_excludes(
                &mut reader,
                filter,
            )?))?;
        }
        return Ok(AccountedBloomExclusions::Multiple(exclusions));
    }

    Ok(AccountedBloomExclusions::None)
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
            if reader.may_contain(&topic0, &address, topic_index, topic)? {
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
            if reader.may_contain(&address, topic_index, topic)? {
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
        if index >= filter.min_topic_count && matches!(constraint, TopicConstraint::Any) {
            continue;
        }
        let row_ids = row_ids_from_bitmap(result.as_ref());
        let values = reader.read_nullable_b256(&format!("topic{index}"), row_ids.as_deref())?;
        result = Some(bitmap_from_values(
            row_ids.as_deref(),
            row_count,
            values,
            |topic| {
                (index >= filter.min_topic_count || topic.is_some())
                    && topic_matches_constraint(*topic, constraint)
            },
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

fn build_topic0_candidate_set(
    index_dir: &Path,
    checkpoint: &IndexReadCheckpoint,
    constraint: &TopicConstraint,
    access: CandidateAccess<'_>,
) -> std::io::Result<Option<CandidateSet>> {
    let topic_path = index_dir.join("topic0.bptree");
    let Some(file_id) = checkpoint.artifact_id("topic0.bptree") else {
        return Ok(None);
    };

    let bitmap = match constraint {
        TopicConstraint::Any => return Ok(None),
        TopicConstraint::One(topic) => access
            .point(&topic_path, file_id, topic.as_slice())?
            .map_or_else(|| access.empty(), Ok)?,
        TopicConstraint::AnyOf(topics) => {
            let mut union = access.empty()?;
            for topic in topics {
                if let Some(bitmap) = access.point(&topic_path, file_id, topic.as_slice())? {
                    union = union.union(bitmap)?;
                }
            }
            union
        }
    };

    Ok(Some(bitmap))
}

fn intersect_optional_set(
    existing: Option<CandidateSet>,
    new: CandidateSet,
) -> io::Result<CandidateSet> {
    match existing {
        Some(existing) => existing.intersection(new),
        None => Ok(new),
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

fn topic_values(constraint: &TopicConstraint) -> Option<&[B256]> {
    match constraint {
        TopicConstraint::One(topic) => Some(std::slice::from_ref(topic)),
        TopicConstraint::AnyOf(topics) if !topics.is_empty() => Some(topics),
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
    use logex_index::{IndexBuildProfile, IndexBuilder};
    use logex_storage::{ColumnFile, PartitionManagerConfig};
    use logex_types::{QueryMemoryLimit, Source};
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

    #[test]
    fn required_topic_presence_refines_indexed_and_unindexed_candidates() {
        let dir = tempfile::tempdir().unwrap();
        let prototype = make_test_rows()[0].clone();
        let shapes = [
            [false, false, false, false],
            [true, false, false, false],
            [true, true, false, false],
            [false, true, false, false],
            [true, false, true, false],
            [true, true, true, true],
        ];
        let rows: Vec<_> = shapes
            .into_iter()
            .enumerate()
            .map(|(index, shape)| {
                let mut row = prototype.clone();
                row.log_index = index as u32;
                row.topic0 = shape[0].then_some(B256::ZERO);
                row.topic1 = shape[1].then_some(B256::ZERO);
                row.topic2 = shape[2].then_some(B256::ZERO);
                row.topic3 = shape[3].then_some(B256::ZERO);
                row
            })
            .collect();
        write_legacy_source(dir.path(), &rows);
        for indexed in [false, true] {
            if indexed {
                IndexBuilder::build_all_indexes(dir.path()).unwrap();
            }
            let reader = SegmentReader::open(dir.path()).unwrap();
            let checkpoint = IndexReadCheckpoint::open(dir.path(), &reader).unwrap();
            assert_eq!(
                checkpoint
                    .as_ref()
                    .and_then(|checkpoint| checkpoint.artifact_id("topic0.bptree"))
                    .is_some(),
                indexed
            );
            for count in 0..=5 {
                let filter = NativeLogFilter {
                    min_topic_count: count,
                    ..NativeLogFilter::new()
                };
                let expected: Vec<u32> = shapes
                    .iter()
                    .enumerate()
                    .filter(|(_, shape)| {
                        count <= 4 && shape.iter().take(count).all(|present| *present)
                    })
                    .map(|(index, _)| index as u32)
                    .collect();
                let actual =
                    candidate_row_ids(dir.path(), &filter, true, rows.len() as u64).unwrap();
                assert_eq!(actual, expected, "indexed={indexed},arity={count}");
                let direct: Vec<_> = rows
                    .iter()
                    .enumerate()
                    .filter(|(_, row)| matches_native_filter(row, &filter))
                    .map(|(index, _)| index as u32)
                    .collect();
                assert_eq!(direct, expected);
                // Without index refinement, candidates remain a conservative superset.
                let unrefined =
                    candidate_row_ids(dir.path(), &filter, false, rows.len() as u64).unwrap();
                let refined: Vec<_> = unrefined
                    .into_iter()
                    .filter(|&index| matches_native_filter(&rows[index as usize], &filter))
                    .collect();
                assert_eq!(refined, expected);
            }
            let impossible =
                NativeLogFilter::new().with_topic(0, TopicConstraint::AnyOf(Vec::new()));
            assert!(
                candidate_row_ids(dir.path(), &impossible, true, rows.len() as u64)
                    .unwrap()
                    .is_empty()
            );
            assert!(
                rows.iter()
                    .all(|row| !matches_native_filter(row, &impossible))
            );
            let combined = NativeLogFilter {
                min_topic_count: 2,
                ..NativeLogFilter::new().with_topic(1, TopicConstraint::One(B256::ZERO))
            };
            assert_eq!(
                candidate_row_ids(dir.path(), &combined, true, rows.len() as u64).unwrap(),
                vec![2, 5]
            );
        }
    }

    #[test]
    fn accounted_candidates_match_full_row_oracle_for_pushed_predicates() {
        let topic_x = B256::repeat_byte(0x10);
        let topic_y = B256::repeat_byte(0x20);
        let topic_z = B256::repeat_byte(0x21);
        let topic_q = B256::repeat_byte(0x30);
        let address_a = Address::repeat_byte(0xaa);
        let address_b = Address::repeat_byte(0xbb);
        let address_c = Address::repeat_byte(0xcc);
        let template = make_test_rows().remove(0);
        let rows = vec![
            template.clone(),
            LogRow {
                block_number: 101,
                block_hash: B256::repeat_byte(2),
                timestamp: template.timestamp + 10,
                log_index: 1,
                address: address_b,
                topic0: Some(topic_x),
                topic1: Some(topic_z),
                data: bytes!("cafe"),
                data_len: 2,
                source: Source::Trace,
                ..template.clone()
            },
            LogRow {
                block_number: 102,
                block_hash: B256::repeat_byte(3),
                timestamp: template.timestamp + 20,
                log_index: 2,
                address: address_a,
                topic0: Some(topic_x),
                topic1: Some(topic_y),
                topic2: Some(topic_q),
                data: bytes!("beef"),
                data_len: 2,
                ..template.clone()
            },
            LogRow {
                block_number: 103,
                block_hash: B256::repeat_byte(4),
                timestamp: template.timestamp + 30,
                log_index: 3,
                address: address_a,
                topic0: Some(B256::repeat_byte(0x40)),
                topic1: None,
                data: bytes!("01"),
                data_len: 1,
                ..template.clone()
            },
            LogRow {
                block_number: 104,
                block_hash: B256::repeat_byte(5),
                timestamp: template.timestamp + 40,
                log_index: 4,
                address: address_c,
                topic0: Some(topic_x),
                topic1: Some(topic_y),
                topic2: Some(topic_q),
                data: bytes!(""),
                data_len: 0,
                source: Source::Trace,
                ..template.clone()
            },
            LogRow {
                block_number: 105,
                block_hash: B256::repeat_byte(6),
                timestamp: template.timestamp + 50,
                log_index: 5,
                address: address_a,
                topic0: Some(topic_x),
                topic1: Some(topic_z),
                topic2: Some(topic_q),
                data: bytes!("ff"),
                data_len: 1,
                ..template
            },
        ];
        let canonical = [true, false, true, true, false, true];
        let setup = || {
            let tmp = tempfile::tempdir().unwrap();
            let mut storage = PartitionManager::open(PartitionManagerConfig {
                data_dir: tmp.path().to_path_buf(),
                partition_target_rows: 1_000_000,
                compaction_safety_margin_blocks: 2_048,
            })
            .unwrap();
            storage.write_batch(&rows).unwrap();
            assert_eq!(storage.mark_non_canonical(rows[1].block_hash).unwrap(), 1);
            assert_eq!(storage.mark_non_canonical(rows[4].block_hash).unwrap(), 1);
            storage.checkpoint().unwrap();
            (tmp, storage)
        };
        let (_tmp, storage) = setup();
        let path = storage.hot_partition().meta.path.clone();
        let filters = vec![
            NativeLogFilter::new().with_block_hash(rows[1].block_hash),
            NativeLogFilter::new().with_block_range(Some(101), Some(102)),
            NativeLogFilter::new()
                .with_timestamp_range(Some(rows[0].timestamp), Some(rows[1].timestamp)),
            NativeLogFilter {
                data_len: Some(rows[1].data_len),
                ..NativeLogFilter::new()
            },
            NativeLogFilter {
                data_min: Some(rows[0].data.to_vec()),
                data_max: Some(rows[1].data.to_vec()),
                data_not_equals: vec![rows[0].data.to_vec()],
                ..NativeLogFilter::new()
            },
            NativeLogFilter {
                data_min: Some(Vec::new()),
                data_max: Some(Vec::new()),
                ..NativeLogFilter::new()
            },
            NativeLogFilter::new().with_addresses(vec![rows[0].address, rows[1].address]),
            NativeLogFilter::new()
                .with_addresses(vec![address_a])
                .with_topic(0, TopicConstraint::One(topic_x))
                .with_topic(1, TopicConstraint::AnyOf(vec![topic_y, topic_z])),
            NativeLogFilter::new()
                .with_addresses(vec![address_a])
                .with_topic(0, TopicConstraint::One(topic_x))
                .with_topic(2, TopicConstraint::One(topic_q)),
            NativeLogFilter::new()
                .with_addresses(vec![address_a])
                .with_topic(0, TopicConstraint::One(topic_x))
                .with_block_range(Some(102), Some(105)),
            NativeLogFilter::new()
                .with_addresses(vec![address_a])
                .with_topic(0, TopicConstraint::One(topic_x)),
            NativeLogFilter::new()
                .with_topic(0, TopicConstraint::One(topic_x))
                .with_topic(1, TopicConstraint::One(topic_y)),
            NativeLogFilter::new().with_topic(
                0,
                TopicConstraint::AnyOf(vec![topic_x, B256::repeat_byte(0x40)]),
            ),
            NativeLogFilter::new().with_topic(0, TopicConstraint::AnyOf(Vec::new())),
            NativeLogFilter {
                min_topic_count: 2,
                from_timestamp: Some(rows[2].timestamp),
                to_timestamp: Some(rows[5].timestamp),
                data_min: Some(vec![0xbe]),
                data_max: Some(vec![0xff]),
                topics: [
                    TopicConstraint::One(topic_x),
                    TopicConstraint::AnyOf(vec![topic_y, topic_z]),
                    TopicConstraint::Any,
                    TopicConstraint::Any,
                ],
                ..NativeLogFilter::new()
            },
            NativeLogFilter::new().with_topic(2, TopicConstraint::One(B256::repeat_byte(0xff))),
            NativeLogFilter {
                canonical_only: false,
                addresses: vec![address_c],
                ..NativeLogFilter::default()
            },
        ];

        for indexed in [false, true] {
            if indexed {
                IndexBuilder::build_all_indexes(&path).unwrap();
            }
            for filter in &filters {
                let expected: Vec<u32> = rows
                    .iter()
                    .enumerate()
                    .filter_map(|(row, value)| {
                        (matches_native_filter(value, filter)
                            && (!filter.canonical_only || canonical[row]))
                            .then_some(row as u32)
                    })
                    .collect();
                let memory =
                    QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024 * 1024).unwrap());
                let actual = candidate_row_ids_with_memory(
                    &path,
                    filter,
                    indexed,
                    rows.len() as u64,
                    &memory,
                    None,
                )
                .unwrap();
                assert_eq!(&*actual, expected, "indexed={indexed}, filter={filter:?}");
                drop(actual);
                assert_eq!(memory.used(), 0);
            }
        }

        let (_partial_tmp, partial) = setup();
        let partial_path = partial.hot_partition().meta.path.clone();
        IndexBuilder::build_indexes(&partial_path, IndexBuildProfile::Erc20Transfer).unwrap();
        let filter = NativeLogFilter::new().with_addresses(vec![address_a]);
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024 * 1024).unwrap());
        let actual = candidate_row_ids_with_memory(
            &partial_path,
            &filter,
            true,
            rows.len() as u64,
            &memory,
            None,
        )
        .unwrap();
        assert_eq!(&*actual, &[0, 2, 3, 5]);
        drop(actual);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn accounted_multi_filter_candidates_share_capture_and_match_oracle() {
        #[derive(Clone, Copy)]
        enum Indexes {
            None,
            Partial,
            Full,
        }

        let prototype = make_test_rows()[0].clone();
        let rows: Vec<_> = (0..4)
            .map(|index| LogRow {
                block_number: 100 + index,
                block_hash: B256::repeat_byte(index as u8 + 1),
                timestamp: prototype.timestamp + index,
                log_index: index as u32,
                address: [
                    Address::repeat_byte(0xaa),
                    Address::repeat_byte(0xbb),
                    Address::repeat_byte(0xaa),
                    Address::repeat_byte(0xcc),
                ][index as usize],
                ..prototype.clone()
            })
            .collect();
        let canonical = [true, false, true, true];
        let filters = vec![
            NativeLogFilter {
                canonical_only: true,
                addresses: vec![Address::repeat_byte(0xaa)],
                ..NativeLogFilter::default()
            },
            NativeLogFilter {
                canonical_only: true,
                from_block: Some(102),
                to_block: Some(103),
                ..NativeLogFilter::default()
            },
        ];
        assert_eq!(
            candidate_refinement_columns_for_filters(&filters),
            vec!["address", "block_number"]
        );

        for indexes in [Indexes::None, Indexes::Partial, Indexes::Full] {
            let tmp = tempfile::tempdir().unwrap();
            let mut storage = PartitionManager::open(PartitionManagerConfig {
                data_dir: tmp.path().to_path_buf(),
                partition_target_rows: 1_000_000,
                compaction_safety_margin_blocks: 2_048,
            })
            .unwrap();
            storage.write_batch(&rows).unwrap();
            assert_eq!(storage.mark_non_canonical(rows[1].block_hash).unwrap(), 1);
            let path = storage.hot_partition().meta.path.clone();
            match indexes {
                Indexes::None => {}
                Indexes::Partial => {
                    IndexBuilder::build_indexes(&path, IndexBuildProfile::Erc20Transfer).unwrap();
                }
                Indexes::Full => IndexBuilder::build_all_indexes(&path).unwrap(),
            }
            storage.checkpoint().unwrap();

            let expected: Vec<u32> = rows
                .iter()
                .enumerate()
                .filter_map(|(row, value)| {
                    (filters.iter().any(|filter| {
                        matches_native_filter(value, filter)
                            && (!filter.canonical_only || canonical[row])
                    }))
                    .then_some(row as u32)
                })
                .collect();
            let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024 * 1024).unwrap());
            let columns = candidate_refinement_columns_for_filters(&filters);
            let reader =
                SegmentReader::open_projected_with_memory(&path, &columns, memory.clone()).unwrap();
            let ids = candidate_row_ids_for_filters_on_reader_with_memory(
                &path,
                &reader,
                &filters,
                !matches!(indexes, Indexes::None),
                rows.len() as u64,
                &memory,
                None,
            )
            .unwrap();
            assert_eq!(&*ids, expected);
            assert_eq!(
                memory.used(),
                (ids.capacity() * size_of::<u32>()) as u128,
                "the returned candidate allocation must retain its own charge"
            );
            drop(ids);
            assert_eq!(
                memory.used(),
                0,
                "a raw captured reader has no scalable buffer until a read"
            );

            let mixed_filters = vec![
                NativeLogFilter {
                    canonical_only: false,
                    addresses: vec![Address::repeat_byte(0xbb)],
                    ..NativeLogFilter::default()
                },
                filters[0].clone(),
            ];
            let ids = candidate_row_ids_for_filters_on_reader_with_memory(
                &path,
                &reader,
                &mixed_filters,
                !matches!(indexes, Indexes::None),
                rows.len() as u64,
                &memory,
                None,
            )
            .unwrap();
            assert_eq!(&*ids, &[0, 1, 2]);
            drop(ids);
            assert_eq!(memory.used(), 0);
            drop(reader);
            assert_eq!(memory.used(), 0);
        }
    }

    #[test]
    fn accounted_candidate_union_charges_overlap_and_releases_on_failure() {
        fn ids(values: &[u32], memory: &QueryMemoryBudget) -> QueryBuffer<u32> {
            let mut ids = QueryBuffer::try_with_capacity(
                values.len(),
                Some(memory),
                "query candidate row ids",
            )
            .unwrap();
            ids.try_extend(values.iter().copied()).unwrap();
            ids
        }

        let constrained =
            QueryMemoryBudget::new(QueryMemoryLimit::new(size_of::<[u32; 8]>() - 1).unwrap());
        let error = merge_sorted_candidate_ids(
            ids(&[0, 2], &constrained),
            ids(&[1, 3], &constrained),
            &constrained,
            None,
        )
        .unwrap_err();
        assert!(
            error
                .get_ref()
                .unwrap()
                .is::<logex_types::QueryMemoryError>()
        );
        assert_eq!(constrained.used(), 0);

        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024).unwrap());
        let union = merge_sorted_candidate_ids(
            ids(&[0, 2], &memory),
            ids(&[1, 2, 3], &memory),
            &memory,
            None,
        )
        .unwrap();
        assert_eq!(&*union, &[0, 1, 2, 3]);
        assert!(memory.used() > 0);
        drop(union);
        assert_eq!(memory.used(), 0);

        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let cancel: crate::QueryCancelCheck = {
            let calls = std::sync::Arc::clone(&calls);
            std::sync::Arc::new(move || {
                calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed) >= 1
            })
        };
        let left: Vec<_> = (0..600).step_by(2).collect();
        let right: Vec<_> = (1..600).step_by(2).collect();
        let error = merge_sorted_candidate_ids(
            ids(&left, &memory),
            ids(&right, &memory),
            &memory,
            Some(&cancel),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn singleton_canonical_filter_does_not_materialize_full_row_range() {
        let dir = tempfile::tempdir().unwrap();
        let prototype = make_test_rows()[0].clone();
        let rows: Vec<_> = (0..512)
            .map(|index| LogRow {
                block_number: prototype.block_number + index,
                block_hash: B256::repeat_byte((index % 255 + 1) as u8),
                log_index: index as u32,
                ..prototype.clone()
            })
            .collect();
        write_legacy_source(dir.path(), &rows);
        let mut canonical = logex_storage::NullBitmap::new();
        for _ in &rows {
            canonical.push(false);
        }
        let mut canonical_bytes = Vec::new();
        canonical.write_to(&mut canonical_bytes).unwrap();
        fs::write(dir.path().join("canonical.bitmap"), canonical_bytes).unwrap();

        let probe = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
        let reader =
            SegmentReader::open_projected_with_memory(dir.path(), &[], probe.clone()).unwrap();
        let capture_bytes = probe.used();
        let canonical = reader.read_canonical_accounted().unwrap();
        let exact_limit = usize::try_from(probe.used()).unwrap();
        assert!(exact_limit < probe.limit());
        assert!(
            (rows.len() * size_of::<u32>()) as u128 > exact_limit as u128 - capture_bytes,
            "the fixture budget must reject a full candidate range"
        );
        drop(canonical);
        drop(reader);
        assert_eq!(probe.used(), 0);

        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(exact_limit).unwrap());
        let reader =
            SegmentReader::open_projected_with_memory(dir.path(), &[], memory.clone()).unwrap();
        let ids = candidate_row_ids_for_filters_on_reader_with_memory(
            dir.path(),
            &reader,
            std::slice::from_ref(&NativeLogFilter::new()),
            false,
            rows.len() as u64,
            &memory,
            None,
        )
        .unwrap();
        assert!(ids.is_empty());
        drop(ids);
        drop(reader);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn impossible_multi_filter_union_skips_index_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let rows = make_test_rows();
        write_legacy_source(dir.path(), &rows);
        IndexBuilder::build_all_indexes(dir.path()).unwrap();
        fs::write(dir.path().join("indexes/index-checkpoint"), b"corrupt").unwrap();
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
        let reader =
            SegmentReader::open_projected_with_memory(dir.path(), &[], memory.clone()).unwrap();
        let impossible = NativeLogFilter {
            min_topic_count: 5,
            ..NativeLogFilter::new()
        };
        let ids = candidate_row_ids_for_filters_on_reader_with_memory(
            dir.path(),
            &reader,
            &[impossible.clone(), impossible],
            true,
            rows.len() as u64,
            &memory,
            None,
        )
        .unwrap();
        assert!(ids.is_empty());
        drop(ids);
        drop(reader);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn singleton_bloom_exclusion_keeps_scalar_working_set() {
        let dir = tempfile::tempdir().unwrap();
        let mut rows = make_test_rows();
        rows[0].topic0 = Some(transfer_topic0());
        write_legacy_source(dir.path(), &rows);
        IndexBuilder::build_all_indexes(dir.path()).unwrap();
        let reader = SegmentReader::open(dir.path()).unwrap();
        let checkpoint = IndexReadCheckpoint::open(dir.path(), &reader)
            .unwrap()
            .unwrap();
        let file_id = checkpoint.artifact_id(ERC20_EVENTS_BLOOM_FILE).unwrap();
        let bloom_path = dir.path().join("indexes").join(ERC20_EVENTS_BLOOM_FILE);

        let probe = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
        let bloom =
            Erc20EventBloomReader::open_bound_with_memory(&bloom_path, file_id, &probe).unwrap();
        let exact_limit = usize::try_from(probe.used()).unwrap();
        drop(bloom);
        assert_eq!(probe.used(), 0);

        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(exact_limit).unwrap());
        let filter =
            NativeLogFilter::new().with_topic(0, TopicConstraint::One(B256::repeat_byte(0xee)));
        assert!(matches!(
            erc20_event_bloom_exclusions_with_memory(
                &dir.path().join("indexes"),
                &checkpoint,
                std::slice::from_ref(&filter),
                &memory,
                None,
            )
            .unwrap(),
            AccountedBloomExclusions::Single(false)
        ));
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn accounted_candidate_cancellation_releases_every_owner() {
        let dir = tempfile::tempdir().unwrap();
        let rows = make_test_rows();
        write_legacy_source(dir.path(), &rows);
        IndexBuilder::build_all_indexes(dir.path()).unwrap();
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
        let cancel: crate::QueryCancelCheck = std::sync::Arc::new(|| true);
        let error = candidate_row_ids_with_memory(
            dir.path(),
            &NativeLogFilter::new().with_addresses(vec![rows[0].address]),
            true,
            rows.len() as u64,
            &memory,
            Some(&cancel),
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn accounted_refinement_ignores_rows_appended_after_snapshot_capture() {
        let (_tmp, mut storage) = setup_storage();
        let path = storage.hot_partition().meta.path.clone();
        let visible_rows = storage.hot_partition().meta.row_count;
        let mut appended = make_test_rows().remove(0);
        appended.block_number += 10;
        appended.log_index += 10;
        storage
            .write_batch(std::slice::from_ref(&appended))
            .unwrap();
        assert!(storage.hot_partition().meta.row_count > visible_rows);

        let filters = [
            NativeLogFilter::new().with_addresses(vec![appended.address]),
            NativeLogFilter::new().with_block_range(Some(appended.block_number), None),
        ];
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024 * 1024).unwrap());
        let actual = candidate_row_ids_for_filters_with_memory(
            &path,
            &filters,
            true,
            visible_rows,
            &memory,
            None,
        )
        .unwrap();
        assert_eq!(&*actual, &[0]);
        drop(actual);
        assert_eq!(memory.used(), 0);
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
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024 * 1024).unwrap());
        assert_eq!(
            erc20_event_bloom_exclusions_with_memory(
                &target.join("indexes"),
                &checkpoint,
                std::slice::from_ref(&filter),
                &memory,
                None,
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::InvalidData
        );
        assert_eq!(memory.used(), 0);

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
    fn accounted_native_sort_preserves_stable_ties_across_permutations() {
        let prototype = make_test_rows()[0].clone();
        let base: Vec<_> = (0..7)
            .map(|index| {
                let mut row = prototype.clone();
                row.block_number = index % 3;
                row.log_index = 0;
                row.block_hash = B256::repeat_byte(index as u8);
                row
            })
            .collect();
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(1024).unwrap());
        let mut seed = 7u64;
        for _ in 0..96 {
            let mut input = base.clone();
            for index in (1..input.len()).rev() {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                input.swap(index, (seed as usize) % (index + 1));
            }
            for order in [LogOrder::Ascending, LogOrder::Descending] {
                let mut expected = input.clone();
                match order {
                    LogOrder::Ascending => {
                        expected.sort_by_key(|row| (row.block_number, row.tx_index, row.log_index))
                    }
                    LogOrder::Descending => expected.sort_by_key(|row| {
                        std::cmp::Reverse((row.block_number, row.tx_index, row.log_index))
                    }),
                }
                let mut actual = input.clone();
                sort_native_rows_with_memory(&mut actual, order, &memory, None).unwrap();
                assert_eq!(actual, expected);
                assert_eq!(memory.used(), 0);
            }
        }
        let mut unordered = vec![base[2].clone(), base[0].clone(), base[1].clone()];
        let original = unordered.clone();
        let constrained =
            QueryMemoryBudget::new(QueryMemoryLimit::new(3 * size_of::<usize>() - 1).unwrap());
        let error =
            sort_native_rows_with_memory(&mut unordered, LogOrder::Ascending, &constrained, None)
                .unwrap_err();
        assert!(
            error
                .get_ref()
                .unwrap()
                .is::<logex_types::QueryMemoryError>()
        );
        assert_eq!(unordered, original);
        assert_eq!(constrained.used(), 0);
        unordered.sort_by_key(|row| row.block_number);
        let held = constrained
            .reserve(constrained.limit(), "other query")
            .unwrap();
        sort_native_rows_with_memory(&mut unordered, LogOrder::Ascending, &constrained, None)
            .unwrap();
        assert_eq!(constrained.used(), held.bytes());
    }

    #[test]
    fn accounted_native_query_matches_pages_and_retains_payload_aliases() {
        let (_tmp, storage) = setup_storage();
        let snapshot = StorageSnapshot::from_storage(&storage);
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024 * 1024).unwrap());
        for order in [LogOrder::Ascending, LogOrder::Descending] {
            for offset in 0..4 {
                for limit in [None, Some(0), Some(1), Some(3)] {
                    let filter = NativeLogFilter {
                        order,
                        offset,
                        limit,
                        ..NativeLogFilter::new()
                    };
                    let expected = execute_log_filter(&storage, &filter).unwrap();
                    let rows = execute_log_filter_on_snapshot_with_memory(
                        &snapshot, &filter, None, &memory,
                    )
                    .unwrap();
                    assert_eq!(&*rows, expected);
                    assert!(memory.used() >= (rows.capacity() * size_of::<LogRow>()) as u128);
                    drop(rows);
                    assert_eq!(memory.used(), 0);
                }
            }
        }
        let rows = execute_log_filter_on_snapshot_with_memory(
            &snapshot,
            &NativeLogFilter::new(),
            None,
            &memory,
        )
        .unwrap();
        let payload = rows
            .iter()
            .find(|row| !row.data.is_empty())
            .unwrap()
            .data
            .clone();
        let expected = payload.to_vec();
        drop(rows);
        assert!(memory.used() > 0);
        assert_eq!(&payload[..], expected);
        drop(payload);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn accounted_native_pressure_and_cancellation_release_partial_results() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        let (_tmp, storage) = setup_storage();
        let snapshot = StorageSnapshot::from_storage(&storage);
        let memory = QueryMemoryBudget::new(QueryMemoryLimit::new(16 * 1024 * 1024).unwrap());
        let held = memory.reserve(memory.limit(), "other query").unwrap();
        let error = execute_log_filter_on_snapshot_with_memory(
            &snapshot,
            &NativeLogFilter::new(),
            None,
            &memory,
        )
        .unwrap_err();
        assert!(
            error
                .get_ref()
                .unwrap()
                .is::<logex_types::QueryMemoryError>()
        );
        assert_eq!(memory.used(), held.bytes());
        drop(held);
        let calls = Arc::new(AtomicUsize::new(0));
        let probe: crate::QueryCancelCheck = {
            let calls = Arc::clone(&calls);
            Arc::new(move || {
                calls.fetch_add(1, Ordering::Relaxed);
                false
            })
        };
        drop(
            execute_log_filter_on_snapshot_with_memory(
                &snapshot,
                &NativeLogFilter::new(),
                Some(&probe),
                &memory,
            )
            .unwrap(),
        );
        let total = calls.load(Ordering::Relaxed);
        assert!(total > 6);
        for at in [1, total / 2, total] {
            calls.store(0, Ordering::Relaxed);
            let cancel: crate::QueryCancelCheck = {
                let calls = Arc::clone(&calls);
                Arc::new(move || calls.fetch_add(1, Ordering::Relaxed) + 1 >= at)
            };
            let error = execute_log_filter_on_snapshot_with_memory(
                &snapshot,
                &NativeLogFilter::new(),
                Some(&cancel),
                &memory,
            )
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::Interrupted);
            assert_eq!(memory.used(), 0);
        }
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
