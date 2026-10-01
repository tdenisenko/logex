//! Query admission against receipt-verified, durably published block coverage.
use std::{error::Error, io};

use alloy_primitives::B256;
use logex_storage::{PartitionManager, VerifiedLogCoverage, native::NativeLogFilter};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryCoverageError {
    #[error(
        "event history has no verified coverage; wait for verified sync or rebuild legacy data"
    )]
    Unverified,
    #[error(
        "requested blocks {from}..={to} are outside verified event coverage {verified_from}..={verified_to}; wait for sync or use explicit bounds within that coverage"
    )]
    OutsideRange {
        from: u64,
        to: u64,
        verified_from: u64,
        verified_to: u64,
    },
    #[error(
        "block hash {hash} is not in the verified header cache and history starts at {verified_from}; specify a verified block range or wait for genesis coverage"
    )]
    UnknownBlockHash { hash: B256, verified_from: u64 },
    #[error(
        "verified queries support canonical events only; orphan inspection requires an explicit unverified storage view"
    )]
    Noncanonical,
}

/// Retained with the same storage view as partition row boundaries. A later
/// append/backfill cannot extend this snapshot's admitted query range.
#[derive(Debug, Clone, Default)]
pub(crate) struct QueryCoverage {
    verified: Option<VerifiedLogCoverage>,
    headers: Vec<B256>,
}

impl QueryCoverage {
    pub(crate) fn capture(storage: &PartitionManager) -> Self {
        let verified = storage.verified_log_coverage();
        let headers = storage
            .recent_headers()
            .iter()
            .filter(|header| {
                verified.is_some_and(|range| {
                    (range.from.block_number..=range.to.block_number).contains(&header.number)
                })
            })
            .map(|header| header.hash_slow())
            .collect();
        Self { verified, headers }
    }

    pub(crate) fn check(&self, filter: &NativeLogFilter) -> Result<(), QueryCoverageError> {
        if !filter.canonical_only {
            return Err(QueryCoverageError::Noncanonical);
        }
        let range = self.verified.ok_or(QueryCoverageError::Unverified)?;
        if let Some(hash) = filter.block_hash {
            // A known canonical hash identifies exactly one verified block,
            // including an empty block with no searchable event rows. Additional
            // predicates can only reduce its result (possibly to the empty set).
            if hash == range.from.block_hash
                || hash == range.to.block_hash
                || self.headers.contains(&hash)
            {
                return Ok(());
            }
            if filter.from_block.is_none() && range.from.block_number != 0 {
                return Err(QueryCoverageError::UnknownBlockHash {
                    hash,
                    verified_from: range.from.block_number,
                });
            }
        }
        let from = filter.from_block.unwrap_or(0);
        let to = filter.to_block.unwrap_or(range.to.block_number);
        if from < range.from.block_number || to > range.to.block_number {
            return Err(QueryCoverageError::OutsideRange {
                from,
                to,
                verified_from: range.from.block_number,
                verified_to: range.to.block_number,
            });
        }
        Ok(())
    }
}

/// Recover the typed admission failure through I/O and executor wrappers. Avoid
/// classifying disk errors or arbitrary error text as missing chain coverage.
pub fn query_coverage_error(root: &(dyn Error + 'static)) -> Option<QueryCoverageError> {
    let mut pending = vec![root];
    while let Some(error) = pending.pop() {
        if let Some(error) = error.downcast_ref::<QueryCoverageError>() {
            return Some(error.clone());
        }
        let datafusion = error
            .downcast_ref::<datafusion::error::DataFusionError>()
            .or_else(|| {
                error
                    .downcast_ref::<std::sync::Arc<datafusion::error::DataFusionError>>()
                    .map(std::sync::Arc::as_ref)
            });
        if let Some(datafusion::error::DataFusionError::Collection(errors)) = datafusion {
            pending.extend(errors.iter().map(|error| error as &(dyn Error + 'static)));
        } else if let Some(error) = error.downcast_ref::<io::Error>() {
            if let Some(inner) = error.get_ref() {
                pending.push(inner);
            }
        } else if let Some(source) = error.source() {
            pending.push(source);
        }
    }
    None
}
