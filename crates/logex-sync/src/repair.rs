//! Finite repair fetching and reconstruction without storage publication.
//!
//! The low-level fetcher requires an authentic execution anchor from its caller;
//! the anchor type alone does not prove consensus provenance or finality. The
//! reconstruction assembler selects retained anchors from ConsensusStore and can
//! admit its whole transcript against one current snapshot. A separate coordinator
//! must durably bind verified staged replacements and complete block coverage,
//! including empty blocks, before publishing a repaired catalog.
//!
//! Cancellation drops local waits, not already queued network requests. The owning
//! network runtime remains responsible for transport timeout/teardown and may persist
//! its ordinary peer cache. This module writes no repair data, sync state or notifications.
//! CPU validation is synchronous and bounded by caller work limits. Deadline and
//! cancellation are observed between finite CPU steps, not hard CPU preemption; a
//! coordinator needing runtime responsiveness must provide appropriate worker ownership.
use std::{collections::VecDeque, future::Future, time::Duration};

use alloy_consensus::{Header, TxReceipt};
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::B256;
use eyre::{Result, WrapErr};
use logex_types::{ExecutionAnchor, LogRow};
use reth_network_peers::PeerId;
use reth_primitives_traits::BlockBody;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{
    extract,
    p2p::peer_manager::{PeerManager, ReceiptRequestContext, SourcedBlockBody, SourcedReceiptSet},
    validation::{
        receipts_match_transaction_count, validate_block_pre_execution,
        validate_downloaded_headers, validate_header_matches_anchor, validate_receipts_for_header,
        validate_reverse_downloaded_headers_with_hashes,
    },
};

/// Downcast the returned eyre report to this error; callers need not parse text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RepairFetchErrorKind {
    InvalidInput,
    Unavailable,
    InvalidData,
    LimitExceeded,
    Cancelled,
    Deadline,
    Terminal,
    Local,
}

#[derive(Debug)]
pub struct RepairFetchError {
    pub kind: RepairFetchErrorKind,
    cause: eyre::Report,
}
impl std::fmt::Display for RepairFetchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{:?}: {:#}", self.kind, self.cause)
    }
}
impl std::error::Error for RepairFetchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause.as_ref())
    }
}
fn failure(kind: RepairFetchErrorKind, cause: eyre::Report) -> eyre::Report {
    eyre::Report::new(RepairFetchError { kind, cause })
}
macro_rules! ensure_repair {
    ($condition:expr, $kind:ident, $($message:tt)*) => {
        if !$condition { return Err(failure(RepairFetchErrorKind::$kind, eyre::eyre!($($message)*))); }
    };
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairRange {
    pub start: u64,
    pub end: u64,
}

/// No production defaults: the exclusive maintenance coordinator chooses budgets.
/// Payload limits constrain validation/extraction work after existing network decode;
/// they are not wire-allocation or total-RSS limits.
#[derive(Debug, Clone, Copy)]
pub struct RepairFetchLimits {
    pub header_page_size: u64,
    /// Includes the anchor itself and every header bridging down to range.start.
    pub max_headers: u64,
    pub max_transactions_per_block: usize,
    pub max_encoded_body_bytes: usize,
    pub max_rows_per_block: usize,
    pub max_log_data_bytes_per_block: usize,
    pub request_timeout: Duration,
    /// Peer-selection attempts per request, not a whole-cursor wire-exchange cap.
    pub max_attempts: usize,
    /// One absolute deadline across all cursor calls, including time spent staging.
    pub deadline: Instant,
}

#[derive(Debug)]
pub struct VerifiedRepairBlock {
    header: Header,
    rows: Vec<LogRow>,
}
impl VerifiedRepairBlock {
    pub fn header(&self) -> &Header {
        &self.header
    }
    pub fn rows(&self) -> &[LogRow] {
        &self.rows
    }
    pub fn into_parts(self) -> (Header, Vec<LogRow>) {
        (self.header, self.rows)
    }
}

#[derive(Debug, Clone)]
pub struct RepairCompletion {
    range: RepairRange,
    anchor: ExecutionAnchor,
    delivered_blocks: u64,
}

impl RepairCompletion {
    pub fn range(&self) -> RepairRange {
        self.range
    }
    pub fn anchor(&self) -> ExecutionAnchor {
        self.anchor
    }
    pub fn delivered_blocks(&self) -> u64 {
        self.delivered_blocks
    }
}

#[derive(Debug)]
pub enum RepairFetchStep {
    Block(Box<VerifiedRepairBlock>),
    Complete(RepairCompletion),
}

/// Narrow scripted-test seam; production uses the PeerManager implementation.
/// Responses remain untrusted until the cursor's existing consensus validators pass.
/// Await within the owning runtime task, as with the existing PeerManager APIs;
/// their opaque request futures do not provide a portable `tokio::spawn` Send
/// guarantee. Source ownership itself is Send; no task is detached here.
pub trait RepairSource: Send {
    fn headers(
        &mut self,
        start: B256,
        count: u64,
        timeout: Duration,
        attempts: usize,
    ) -> impl Future<Output = Result<(PeerId, Vec<Header>)>>;
    fn body(
        &mut self,
        hash: B256,
        number: u64,
        timeout: Duration,
        attempts: usize,
    ) -> impl Future<Output = Result<Vec<SourcedBlockBody>>>;
    fn receipts(
        &mut self,
        header: &Header,
        preferred: PeerId,
        timeout: Duration,
        attempts: usize,
    ) -> impl Future<Output = Result<Vec<SourcedReceiptSet>>>;
}

impl RepairSource for PeerManager {
    async fn headers(
        &mut self,
        start: B256,
        count: u64,
        timeout: Duration,
        attempts: usize,
    ) -> Result<(PeerId, Vec<Header>)> {
        self.get_headers_reverse_with_limits(
            BlockHashOrNumber::Hash(start),
            count,
            timeout,
            attempts,
        )
        .await
    }
    async fn body(
        &mut self,
        hash: B256,
        number: u64,
        timeout: Duration,
        attempts: usize,
    ) -> Result<Vec<SourcedBlockBody>> {
        self.get_bodies_prefer_peers_with_limits(vec![hash], number, &[], timeout, attempts)
            .await
    }
    async fn receipts(
        &mut self,
        header: &Header,
        preferred: PeerId,
        timeout: Duration,
        attempts: usize,
    ) -> Result<Vec<SourcedReceiptSet>> {
        self.get_receipts_prefer_peers_with_limits(
            vec![ReceiptRequestContext::from_header(header)],
            header.number,
            &[preferred],
            timeout,
            attempts,
        )
        .await
    }
}

/// Yields whole blocks in descending height order, including zero-row blocks.
/// Owns at most one authenticated header page and one block payload at a time.
/// A failed or dropped next_block future permanently poisons this cursor. Restart
/// requires an explicit new plan/cursor; partial delivery never implies completion.
pub struct RepairFetcher {
    range: RepairRange,
    anchor: ExecutionAnchor,
    limits: RepairFetchLimits,
    cancellation: CancellationToken,
    child: Option<Header>,
    pending: VecDeque<Header>,
    delivered: u64,
    expected: u64,
    poisoned: bool,
}

impl RepairFetcher {
    pub fn new(
        range: RepairRange,
        anchor: ExecutionAnchor,
        limits: RepairFetchLimits,
        cancellation: CancellationToken,
    ) -> Result<Self> {
        ensure_repair!(
            range.start <= range.end,
            InvalidInput,
            "repair range is inverted"
        );
        ensure_repair!(
            anchor.block_number >= range.end,
            InvalidInput,
            "repair anchor does not cover range end"
        );
        let expected = range
            .end
            .checked_sub(range.start)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| {
                failure(
                    RepairFetchErrorKind::InvalidInput,
                    eyre::eyre!("inclusive repair block count overflows"),
                )
            })?;
        let headers = anchor
            .block_number
            .checked_sub(range.start)
            .and_then(|distance| distance.checked_add(1))
            .ok_or_else(|| {
                failure(
                    RepairFetchErrorKind::InvalidInput,
                    eyre::eyre!("repair bridge header count overflows"),
                )
            })?;
        ensure_repair!(
            limits.header_page_size > 0 && limits.header_page_size <= 1024,
            InvalidInput,
            "repair header page must be between 1 and 1024"
        );
        ensure_repair!(
            headers <= limits.max_headers,
            LimitExceeded,
            "repair bridge exceeds header budget"
        );
        ensure_repair!(
            limits.max_attempts > 0 && !limits.request_timeout.is_zero(),
            InvalidInput,
            "repair request limits must be positive"
        );
        ensure_repair!(
            limits.max_transactions_per_block > 0
                && limits.max_encoded_body_bytes > 0
                && limits.max_rows_per_block > 0
                && limits.max_log_data_bytes_per_block > 0,
            InvalidInput,
            "repair block work limits must be positive"
        );
        let cursor = Self {
            range,
            anchor,
            limits,
            cancellation,
            child: None,
            pending: VecDeque::new(),
            delivered: 0,
            expected,
            poisoned: false,
        };
        cursor.check_active()?;
        Ok(cursor)
    }

    fn check_active(&self) -> Result<()> {
        ensure_repair!(
            !self.cancellation.is_cancelled(),
            Cancelled,
            "repair fetching cancelled"
        );
        ensure_repair!(
            Instant::now() < self.limits.deadline,
            Deadline,
            "repair fetching deadline exceeded"
        );
        Ok(())
    }

    pub async fn next_block(&mut self, source: &mut impl RepairSource) -> Result<RepairFetchStep> {
        ensure_repair!(
            !self.poisoned,
            Terminal,
            "repair cursor is terminal after failed or cancelled work"
        );
        self.poisoned = true;
        self.check_active()?;
        let cancellation = self.cancellation.clone();
        let deadline = self.limits.deadline;
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(failure(RepairFetchErrorKind::Cancelled, eyre::eyre!("repair fetching cancelled"))),
            _ = tokio::time::sleep_until(deadline) => Err(failure(RepairFetchErrorKind::Deadline, eyre::eyre!("repair fetching deadline exceeded"))),
            result = self.step(source) => result.map_err(|error| {
                if error.downcast_ref::<RepairFetchError>().is_some() { error } else { failure(RepairFetchErrorKind::Local, error) }
            }),
        }?;
        self.check_active()?;
        self.poisoned = false;
        Ok(result)
    }

    async fn step(&mut self, source: &mut impl RepairSource) -> Result<RepairFetchStep> {
        if self.delivered == self.expected {
            return Ok(RepairFetchStep::Complete(RepairCompletion {
                range: self.range,
                anchor: self.anchor,
                delivered_blocks: self.delivered,
            }));
        }
        if self.child.is_none() {
            let (peer, mut headers) = source
                .headers(
                    self.anchor.block_hash,
                    1,
                    self.limits.request_timeout,
                    self.limits.max_attempts,
                )
                .await
                .wrap_err_with(|| {
                    format!(
                        "request anchor header {} ({})",
                        self.anchor.block_number, self.anchor.block_hash
                    )
                })
                .map_err(|error| failure(RepairFetchErrorKind::Unavailable, error))?;
            self.check_active()?;
            ensure_repair!(
                !headers.is_empty(),
                Unavailable,
                "anchor header unavailable from {peer}"
            );
            ensure_repair!(
                headers.len() == 1,
                InvalidData,
                "anchor header response from {peer} must contain exactly one header"
            );
            let header = headers.pop().expect("length checked");
            validate_header_matches_anchor(&self.anchor, &header, header.hash_slow())
                .map_err(|error| eyre::eyre!("{error}"))
                .wrap_err_with(|| format!("anchor header from {peer}"))
                .map_err(|error| failure(RepairFetchErrorKind::InvalidData, error))?;
            validate_downloaded_headers(header.number, None, std::slice::from_ref(&header))
                .map_err(|error| eyre::eyre!("{error}"))
                .wrap_err_with(|| format!("standalone anchor header from {peer}"))
                .map_err(|error| failure(RepairFetchErrorKind::InvalidData, error))?;
            self.check_active()?;
            if header.number <= self.range.end {
                self.pending.push_back(header.clone());
            }
            self.child = Some(header);
        }
        while self.pending.is_empty() {
            self.check_active()?;
            let child = self.child.as_ref().expect("anchor initialized");
            ensure_repair!(
                child.number > self.range.start,
                InvalidData,
                "repair header coverage ended before completion"
            );
            let count = self
                .limits
                .header_page_size
                .min(child.number - self.range.start);
            let (peer, headers) = source
                .headers(
                    child.parent_hash,
                    count,
                    self.limits.request_timeout,
                    self.limits.max_attempts,
                )
                .await
                .wrap_err_with(|| {
                    format!(
                        "request {} reverse headers below {} starting at {}",
                        count, child.number, child.parent_hash
                    )
                })
                .map_err(|error| failure(RepairFetchErrorKind::Unavailable, error))?;
            self.check_active()?;
            ensure_repair!(
                !headers.is_empty(),
                Unavailable,
                "reverse header history unavailable from {peer}"
            );
            ensure_repair!(
                headers.len() as u64 <= count,
                InvalidData,
                "invalid reverse header response length from {peer}"
            );
            validate_reverse_downloaded_headers_with_hashes(child, &headers)
                .map_err(|error| eyre::eyre!("{error}"))
                .wrap_err_with(|| format!("reverse ancestry from {peer}"))
                .map_err(|error| failure(RepairFetchErrorKind::InvalidData, error))?;
            self.check_active()?;
            self.child = headers.last().cloned();
            self.pending.extend(
                headers
                    .into_iter()
                    .filter(|header| header.number <= self.range.end),
            );
        }
        let header = self
            .pending
            .pop_front()
            .expect("nonempty authenticated page");
        ensure_repair!(
            header.number == self.range.end - self.delivered,
            InvalidData,
            "repair delivery is not contiguous"
        );
        let hash = header.hash_slow();
        let mut bodies = source
            .body(
                hash,
                header.number,
                self.limits.request_timeout,
                self.limits.max_attempts,
            )
            .await
            .wrap_err_with(|| format!("request body for {} ({hash})", header.number))
            .map_err(|error| failure(RepairFetchErrorKind::Unavailable, error))?;
        self.check_active()?;
        ensure_repair!(
            !bodies.is_empty(),
            Unavailable,
            "repair body history unavailable"
        );
        ensure_repair!(
            bodies.len() == 1,
            InvalidData,
            "repair body response must contain exactly one block"
        );
        let (body_peer, body) = bodies.pop().expect("length checked");
        ensure_repair!(
            body.transaction_count() <= self.limits.max_transactions_per_block,
            LimitExceeded,
            "repair body exceeds transaction budget"
        );
        ensure_repair!(
            alloy_rlp::Encodable::length(&body) <= self.limits.max_encoded_body_bytes,
            LimitExceeded,
            "repair body exceeds encoded-byte budget"
        );
        validate_block_pre_execution(&header, hash, &body)
            .wrap_err_with(|| format!("body for {} from {body_peer}", header.number))
            .map_err(|error| failure(RepairFetchErrorKind::InvalidData, error))?;
        self.check_active()?;
        let mut sets = source
            .receipts(
                &header,
                body_peer,
                self.limits.request_timeout,
                self.limits.max_attempts,
            )
            .await
            .wrap_err_with(|| format!("request receipts for {} ({hash})", header.number))
            .map_err(|error| failure(RepairFetchErrorKind::Unavailable, error))?;
        self.check_active()?;
        ensure_repair!(
            !sets.is_empty(),
            Unavailable,
            "repair receipt history unavailable"
        );
        ensure_repair!(
            sets.len() == 1,
            InvalidData,
            "repair receipt response must contain exactly one block"
        );
        let (receipt_peer, receipts) = sets.pop().expect("length checked");
        ensure_repair!(
            receipts_match_transaction_count(&body, &receipts),
            InvalidData,
            "receipt count for {} from {receipt_peer} differs from verified transaction count",
            header.number
        );
        let row_count =
            extract::checked_row_count(receipts.iter().map(|receipt| receipt.logs().len()))
                .map_err(|error| failure(RepairFetchErrorKind::LimitExceeded, error))?;
        ensure_repair!(
            row_count
                .checked_sub(1)
                .is_none_or(|last| u32::try_from(last).is_ok())
                && receipts
                    .len()
                    .checked_sub(1)
                    .is_none_or(|last| u32::try_from(last).is_ok()),
            InvalidData,
            "repair block indexes are not representable"
        );
        ensure_repair!(
            row_count <= self.limits.max_rows_per_block,
            LimitExceeded,
            "repair block exceeds row budget"
        );
        let data_bytes = receipts
            .iter()
            .flat_map(|receipt| receipt.logs())
            .try_fold(0usize, |sum, log| {
                ensure_repair!(
                    log.topics().len() <= 4 && u32::try_from(log.data.data.len()).is_ok(),
                    InvalidData,
                    "receipt log from {receipt_peer} is not representable"
                );
                sum.checked_add(log.data.data.len()).ok_or_else(|| {
                    failure(
                        RepairFetchErrorKind::LimitExceeded,
                        eyre::eyre!("repair log data size overflows"),
                    )
                })
            })?;
        ensure_repair!(
            data_bytes <= self.limits.max_log_data_bytes_per_block,
            LimitExceeded,
            "repair block exceeds log-data budget"
        );
        validate_receipts_for_header(&header, &receipts)
            .map_err(|error| eyre::eyre!("{error}"))
            .wrap_err_with(|| format!("receipts for {} from {receipt_peer}", header.number))
            .map_err(|error| failure(RepairFetchErrorKind::InvalidData, error))?;
        self.check_active()?;
        let mut rows = Vec::new();
        rows.try_reserve_exact(row_count)
            .wrap_err("reserve bounded repair rows")?;
        extract::append_from_body_receipts(
            &mut rows,
            header.number,
            hash,
            header.timestamp,
            &body,
            &receipts,
        )?;
        self.check_active()?;
        self.delivered += 1;
        Ok(RepairFetchStep::Block(Box::new(VerifiedRepairBlock {
            header,
            rows,
        })))
    }
}

mod reconstruction;
pub use reconstruction::{
    ReconstructedRepair, ReconstructedSegment, RepairReconstruction, RepairReconstructionLimits,
};

#[cfg(test)]
mod tests;
