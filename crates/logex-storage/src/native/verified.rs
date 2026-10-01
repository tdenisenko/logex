use super::*;
use crate::VerifiedBlockLogs;

pub(super) struct VerifiedAppendStart {
    active: Option<(u64, u64)>,
    next_segment_id: u64,
}

impl NativeStorage {
    /// Unchecked import and metadata helpers must never weaken a certified
    /// dataset, including at crash boundaries before the next catalog write.
    pub(super) fn require_unverified_storage(&self) -> io::Result<()> {
        self.ensure_writable()?;
        if self.catalog.state.verified_log_coverage.is_some() {
            return Err(invalid(
                "unchecked mutation of verified event storage is forbidden",
            ));
        }
        Ok(())
    }

    pub fn verified_log_coverage(&self) -> Option<crate::VerifiedLogCoverage> {
        self.catalog.state.verified_log_coverage
    }

    /// Publish complete authenticated blocks only as a strict extension of the
    /// existing canonical tip. A retry of an already published block is rejected
    /// before mutation; the caller must resume from the committed tip.
    pub fn ingest_verified_canonical_batch(
        &mut self,
        rows: &[LogRow],
        blocks: &[VerifiedBlockLogs],
        recent_headers: &[Header],
        anchor: Option<&ExecutionAnchor>,
    ) -> io::Result<()> {
        self.ensure_writable()?;
        let (first, last) = VerifiedBlockLogs::verify_batch(blocks, rows)?;
        if recent_headers.windows(2).any(|pair| {
            pair[0].number.checked_add(1) != Some(pair[1].number)
                || pair[0].hash_slow() != pair[1].parent_hash
        }) {
            return Err(invalid("verified canonical header cache is not contiguous"));
        }
        if anchor.is_some_and(|anchor| anchor.receipts_root != last.header().receipts_root) {
            return Err(invalid(
                "verified canonical anchor receipt root differs from header",
            ));
        }
        if let Some(previous) = self.catalog.state.sync_head {
            if previous.block_number.checked_add(1) != Some(first.header().number)
                || previous.block_hash != first.header().parent_hash
            {
                return Err(invalid(
                    "verified canonical batch does not extend stored tip",
                ));
            }
        } else {
            // A full reorg can leave only retired fork rows behind. They are
            // allowed; unowned canonical rows must never seed a verified tip.
            self.ensure_canonical_range_empty(0, u64::MAX)?;
        }
        self.ensure_canonical_range_empty(first.header().number, last.header().number)?;
        let mut coverage = crate::VerifiedLogCoverage::from_blocks(first, last);
        if let Some(previous) = self.catalog.state.verified_log_coverage {
            coverage.from = previous.from;
        }
        self.apply_canonical_batch(
            rows,
            last.header(),
            recent_headers,
            anchor,
            Some((first.header(), coverage)),
        )
    }

    /// The full authenticated header sequence, including empty blocks, must
    /// immediately precede the persisted floor. Check actual parent hashes, not
    /// scheduler sequence numbers or a caller-supplied block count.
    pub fn ingest_verified_historical_batch(
        &mut self,
        rows: &[LogRow],
        blocks: &[VerifiedBlockLogs],
    ) -> io::Result<()> {
        self.ensure_writable()?;
        let (first, last) = VerifiedBlockLogs::verify_batch(blocks, rows)?;
        let child = self
            .catalog
            .state
            .historical_floor_header
            .as_ref()
            .ok_or_else(|| invalid("verified history requires an authenticated starting floor"))?;
        if last.header().number.checked_add(1) != Some(child.number)
            || last.block_hash() != child.parent_hash
        {
            return Err(invalid(
                "verified historical batch does not precede stored floor",
            ));
        }
        self.ensure_canonical_range_empty(first.header().number, last.header().number)?;
        let coverage = self
            .catalog
            .state
            .verified_log_coverage
            .map(|mut previous| {
                // Never bridge an unaudited legacy gap between two checked ranges.
                if previous.from.block_number == child.number
                    && previous.from.block_hash == child.hash_slow()
                {
                    previous.from = crate::VerifiedLogCoverage::from_blocks(first, last).from;
                }
                previous
            });
        self.apply_historical_batch(rows, first.header(), true, coverage)
    }

    /// Exact physical overlap guard, independent of query indexes. Usually all
    /// segment bounds exclude a new range. Legacy rows outside their progress
    /// markers and retained fork rows require inspecting canonical positions.
    fn ensure_canonical_range_empty(&self, from: u64, to: u64) -> io::Result<()> {
        for segment in self.catalog.segments.iter().filter(|segment| {
            segment.row_count != 0
                && segment.min_block.is_none_or(|min| min <= to)
                && segment.max_block.is_none_or(|max| max >= from)
        }) {
            let reader = SegmentReader::open(&self.paths.segment_dir(segment.id))?;
            let canonical = reader.read_canonical()?;
            if canonical.len() < segment.row_count {
                return Err(invalid("overlap check has missing canonical row positions"));
            }
            let end = u32::try_from(segment.row_count).map_err(io::Error::other)?;
            let mut offset = 0_u32;
            while offset < end {
                let next = offset.saturating_add(8192).min(end);
                let ids = (offset..next)
                    .filter(|&row| canonical.is_present(u64::from(row)))
                    .collect::<Vec<_>>();
                if !ids.is_empty()
                    && reader
                        .read_u64("block_number", Some(&ids))?
                        .iter()
                        .any(|&number| (from..=to).contains(&number))
                {
                    return Err(invalid(
                        "verified block range already contains canonical events",
                    ));
                }
                offset = next;
            }
        }
        Ok(())
    }

    pub(super) fn capture_verified_append(&self, route: IngestRoute) -> VerifiedAppendStart {
        let active = match route {
            IngestRoute::Live => self.catalog.active_hot_segment,
            IngestRoute::Historical => self.catalog.active_historical_segment,
        }
        .and_then(|id| {
            self.catalog
                .segments
                .iter()
                .find(|segment| segment.id == id)
        })
        .map(|segment| (segment.id, segment.row_count));
        VerifiedAppendStart {
            active,
            next_segment_id: self.catalog.next_segment_id,
        }
    }

    /// Read every newly written canonical row through the real storage decoder
    /// before publishing coverage. Work is bounded by the incoming batch and
    /// each comparison buffer is at most 8192 rows. No sampling or deduplication.
    pub(super) fn verify_appended_events(
        &self,
        start: VerifiedAppendStart,
        expected: &[LogRow],
    ) -> io::Result<()> {
        let mut consumed = 0usize;
        for segment in self.catalog.segments.iter().filter(|segment| {
            Some(segment.id) == start.active.map(|(id, _)| id)
                || segment.id >= start.next_segment_id
        }) {
            let first = start
                .active
                .filter(|(id, _)| *id == segment.id)
                .map_or(0, |(_, count)| count);
            let count = segment
                .row_count
                .checked_sub(first)
                .ok_or_else(|| invalid("verified append shortened its original segment"))?;
            if count > expected.len().saturating_sub(consumed) as u64 {
                return Err(invalid("verified append wrote unexpected extra rows"));
            }
            if count == 0 {
                continue;
            }
            let reader = SegmentReader::open(&self.paths.segment_dir(segment.id))?;
            if reader.source_namespace() != segment.source_namespace.map(|value| value.0)
                || reader.source_commitment()? != segment.source_commitment.map(|value| value.0)
            {
                return Err(invalid("verified append source identity mismatch"));
            }
            let canonical = reader.read_canonical()?;
            if canonical.len() < segment.row_count
                || (first..segment.row_count).any(|row| !canonical.is_present(row))
            {
                return Err(invalid(
                    "verified append has missing canonical row positions",
                ));
            }
            let mut offset = u32::try_from(first).map_err(io::Error::other)?;
            let end = u32::try_from(segment.row_count).map_err(io::Error::other)?;
            while offset < end {
                let chunk_end = offset.saturating_add(8192).min(end);
                let ids = (offset..chunk_end).collect::<Vec<_>>();
                let actual = reader.read_log_rows(Some(&ids))?;
                let next = consumed
                    .checked_add(ids.len())
                    .ok_or_else(|| invalid("verified row offset overflow"))?;
                if expected.get(consumed..next) != Some(actual.as_slice()) {
                    return Err(invalid("persisted events differ from authenticated input"));
                }
                consumed = next;
                offset = chunk_end;
            }
        }
        if consumed != expected.len() {
            return Err(invalid("verified append did not persist all events"));
        }
        Ok(())
    }
}

fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
#[path = "verified_tests.rs"]
mod tests;
