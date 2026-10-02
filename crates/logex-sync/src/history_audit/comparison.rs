use std::io;

use alloy_consensus::Header;
use alloy_primitives::B256;
use logex_types::{ExecutionAnchor, LogRow};
use serde::Serialize;

use super::manifest::{EventRun, RunReader, hash_row, run_hasher};
use super::{AuditManifest, AuditRange};
use crate::repair::VerifiedRepairBlock;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

/// Complete physical/receipt correspondence for one range and caller-supplied
/// authenticated anchor. The coordinator must establish finalized consensus
/// provenance and re-admit the anchor/source before publishing acceptance.
/// This report alone is not a portable receipt proof or a finality certificate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ReceiptComparisonReport {
    pub range: AuditRange,
    pub anchor: ExecutionAnchor,
    pub blocks: u64,
    pub empty_blocks: u64,
    pub events: u64,
    pub source_prefix_fingerprint: B256,
    pub records_digest: B256,
}

/// Consumes every whole, receipt-validated block in descending height order.
/// The first block must be the anchor; every following parent hash must link.
/// No block (including zero-row blocks), physical occurrence or field can be
/// skipped. Failure is terminal. Source reorg/close invalidates all completion.
pub struct ReceiptComparison<'a> {
    manifest: &'a AuditManifest,
    reader: RunReader,
    pending: Option<EventRun>,
    anchor: ExecutionAnchor,
    next_block: Option<u64>,
    next_hash: B256,
    blocks: u64,
    empty_blocks: u64,
    events: u64,
    failed: bool,
}

impl<'a> ReceiptComparison<'a> {
    /// The anchor must already be authenticated by the maintenance coordinator.
    /// Arbitrary execution-peer data must not supply this trust boundary.
    pub fn new(manifest: &'a AuditManifest, anchor: ExecutionAnchor) -> io::Result<Self> {
        let range = manifest.summary().range;
        if anchor.block_number != range.through
            || range
                .through
                .checked_sub(range.from)
                .and_then(|n| n.checked_add(1))
                .is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "audit anchor must equal the finite manifest upper bound",
            ));
        }
        let result = (|| {
            let mut reader = manifest.reader()?;
            let pending = reader.next_run()?;
            Ok(Self {
                manifest,
                reader,
                pending,
                anchor,
                next_block: Some(range.through),
                next_hash: anchor.block_hash,
                blocks: 0,
                empty_blocks: 0,
                events: 0,
                failed: false,
            })
        })();
        manifest.validate()?;
        result
    }

    pub fn next_block_number(&self) -> Option<u64> {
        self.next_block
    }

    pub fn compare(&mut self, block: &VerifiedRepairBlock) -> io::Result<()> {
        self.compare_rows(block.header(), block.rows())
    }

    fn compare_rows(&mut self, header: &Header, rows: &[LogRow]) -> io::Result<()> {
        if self.failed {
            return Err(invalid("receipt comparison is terminal after failure"));
        }
        self.failed = true;
        self.manifest.validate()?;
        let result = self.compare_inner(header, rows);
        // Do not misclassify a changed view as established primary corruption.
        self.manifest.validate()?;
        if result.is_ok() {
            self.failed = false;
        }
        result
    }

    fn compare_inner(&mut self, header: &Header, rows: &[LogRow]) -> io::Result<()> {
        let hash = header.hash_slow();
        if self.next_block != Some(header.number)
            || hash != self.next_hash
            || (self.blocks == 0 && header.receipts_root != self.anchor.receipts_root)
        {
            return Err(invalid(format!(
                "audit block {} does not match the next authenticated header",
                header.number
            )));
        }
        if self.pending.is_some_and(|run| run.block > header.number) {
            return Err(invalid(
                "audit contains an unconsumed earlier physical block",
            ));
        }
        let mut offset = 0usize;
        while let Some(run) = self.pending.filter(|run| run.block == header.number) {
            if run.block_hash != hash || u64::from(run.first_log) != offset as u64 {
                return Err(invalid(format!(
                    "audit physical identity mismatch at block {}, segment {}, row {}",
                    header.number, run.segment, run.first_physical_row
                )));
            }
            let end = usize::try_from(run.end_log()?).map_err(io::Error::other)?;
            let selected = rows.get(offset..end).ok_or_else(|| invalid(format!(
                "extra physical events at audit block {}: physical end {end}, receipt events {}", header.number, rows.len())))?;
            let mut digest = run_hasher();
            for (index, row) in selected.iter().enumerate() {
                if row.block_number != header.number
                    || row.block_hash != hash
                    || row.timestamp != header.timestamp
                    || row.log_index as usize != offset + index
                {
                    return Err(invalid(
                        "receipt-derived audit row is not in canonical block order",
                    ));
                }
                hash_row(&mut digest, row)?;
            }
            if digest.finalize().as_bytes() != run.digest.as_slice() {
                return Err(invalid(format!(
                    "audit event fields differ at block {}, log indices {}..{}, segment {}, physical row {}",
                    header.number, offset, end, run.segment, run.first_physical_row
                )));
            }
            offset = end;
            self.pending = self.reader.next_run()?;
        }
        if offset != rows.len() {
            return Err(invalid(format!(
                "missing physical events at audit block {}: found {offset}, receipt events {}",
                header.number,
                rows.len()
            )));
        }
        self.blocks = self
            .blocks
            .checked_add(1)
            .ok_or_else(|| invalid("audit block count overflow"))?;
        self.events = self
            .events
            .checked_add(rows.len() as u64)
            .ok_or_else(|| invalid("audit event count overflow"))?;
        if rows.is_empty() {
            self.empty_blocks += 1;
        }
        self.next_hash = header.parent_hash;
        self.next_block = if header.number == self.manifest.summary().range.from {
            None
        } else {
            header.number.checked_sub(1)
        };
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<ReceiptComparisonReport> {
        self.manifest.validate()?;
        let result = (|| {
            let summary = self.manifest.summary();
            let expected_blocks = summary.range.through - summary.range.from + 1;
            if self.failed || self.next_block.is_some() || self.blocks != expected_blocks {
                return Err(invalid("audit receipt traversal is failed or incomplete"));
            }
            if self.pending.is_some()
                || self.reader.next_run()?.is_some()
                || self.events != summary.selected_rows
            {
                return Err(invalid(
                    "audit physical event stream was not completely consumed",
                ));
            }
            Ok(ReceiptComparisonReport {
                range: summary.range,
                anchor: self.anchor,
                blocks: self.blocks,
                empty_blocks: self.empty_blocks,
                events: self.events,
                source_prefix_fingerprint: summary.source_prefix_fingerprint,
                records_digest: summary.records_digest,
            })
        })();
        self.manifest.validate()?;
        result
    }
}

#[cfg(test)]
mod tests;
