//! Receipt-derived expectations carried to the storage publication boundary.
//!
//! A header's canonical ancestry is admitted by consensus/sync. This opaque
//! record independently authenticates its complete transaction/receipt payload
//! and binds the exact event sequence, including blocks that emit no events.
use alloy_consensus::{Header, TxReceipt, proofs};
use alloy_eips::Encodable2718;
use alloy_primitives::{B256, Bloom, Log};
use logex_types::ExecutionBlockMarker;
use logex_types::{BlockContext, LogRow};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, io};

use crate::commitment::PrefixState;

const EVENT_NAMESPACE: [u8; 16] = *b"logex.block.v1\0\0";

/// The empty Merkle Patricia trie root, `keccak256(rlp(""))`.
pub const EMPTY_TRIE_ROOT: B256 =
    alloy_primitives::b256!("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421");

#[derive(Debug, Clone)]
pub struct VerifiedBlockLogs {
    header: Header,
    hash: B256,
    rows: u64,
    commitment: B256,
}

/// A contiguous range established by complete receipt validation and storage
/// readback. This is a local integrity record, not a portable Ethereum proof.
/// Legacy head/floor metadata never implicitly creates this record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VerifiedLogCoverage {
    pub from: ExecutionBlockMarker,
    pub to: ExecutionBlockMarker,
}

impl VerifiedLogCoverage {
    pub(crate) fn from_blocks(first: &VerifiedBlockLogs, last: &VerifiedBlockLogs) -> Self {
        let marker = |block: &VerifiedBlockLogs| ExecutionBlockMarker {
            block_number: block.header.number,
            block_hash: block.hash,
            timestamp: block.header.timestamp,
        };
        Self {
            from: marker(first),
            to: marker(last),
        }
    }
}

impl VerifiedBlockLogs {
    /// A canonical header with both empty trie commitments deterministically
    /// proves the absence of transactions and receipts. A zero-log bloom alone
    /// is not sufficient. No network payload can add events to these roots.
    pub fn from_empty_header(header: &Header) -> io::Result<Self> {
        if header.transactions_root != EMPTY_TRIE_ROOT
            || header.receipts_root != EMPTY_TRIE_ROOT
            || header.gas_used != 0
            || header.logs_bloom != Bloom::ZERO
        {
            return Err(invalid("header does not prove an empty block"));
        }
        Ok(Self {
            header: header.clone(),
            hash: header.hash_slow(),
            rows: 0,
            commitment: PrefixState::empty(EVENT_NAMESPACE).commitment(),
        })
    }

    /// Authenticate the complete payload and append all events in receipt order.
    /// On failure the caller's original row prefix is preserved. This does not
    /// attest canonical ancestry: publication also checks the admitted chain.
    pub fn verify_and_append<T, R>(
        header: &Header,
        transactions: &[T],
        receipts: &[R],
        rows: &mut Vec<LogRow>,
    ) -> io::Result<Self>
    where
        T: Encodable2718,
        R: TxReceipt<Log = Log> + Encodable2718,
    {
        if transactions.len() != receipts.len() {
            return Err(invalid("transaction/receipt count mismatch"));
        }
        if proofs::calculate_transaction_root(transactions) != header.transactions_root {
            return Err(invalid("transaction root mismatch"));
        }
        if proofs::calculate_receipt_root(receipts) != header.receipts_root {
            return Err(invalid("receipt root mismatch"));
        }
        if receipts.last().map_or(0, TxReceipt::cumulative_gas_used) != header.gas_used {
            return Err(invalid("receipt cumulative gas mismatch"));
        }
        let bloom = receipts
            .iter()
            .fold(Bloom::ZERO, |bloom, receipt| bloom | receipt.bloom());
        if bloom != header.logs_bloom {
            return Err(invalid("receipt bloom mismatch"));
        }
        let count = receipts.iter().try_fold(0_usize, |count, receipt| {
            count
                .checked_add(receipt.logs().len())
                .ok_or_else(|| invalid("block event count overflow"))
        })?;
        if count
            .checked_sub(1)
            .is_some_and(|last| u32::try_from(last).is_err())
            || transactions
                .len()
                .checked_sub(1)
                .is_some_and(|last| u32::try_from(last).is_err())
        {
            return Err(invalid("block event or transaction index exceeds u32"));
        }
        rows.try_reserve(count).map_err(io::Error::other)?;
        let start = rows.len();
        let hash = header.hash_slow();
        let context = BlockContext {
            block_number: header.number,
            block_hash: hash,
            timestamp: header.timestamp,
        };
        let result = (|| {
            for (transaction_index, (transaction, receipt)) in
                transactions.iter().zip(receipts).enumerate()
            {
                // Derive the metadata from the authenticated wire encoding,
                // rather than trusting a separately cached transaction hash.
                let transaction_hash = alloy_primitives::keccak256(transaction.encoded_2718());
                for log in receipt.logs() {
                    rows.push(
                        LogRow::try_from_primitives_log(
                            log,
                            &context,
                            transaction_hash,
                            transaction_index as u32,
                            (rows.len() - start) as u32,
                        )
                        .map_err(|error| invalid(error.to_string()))?,
                    );
                }
            }
            let commitment = PrefixState::from_rows(EVENT_NAMESPACE, &rows[start..])?.commitment();
            Ok(Self {
                header: header.clone(),
                hash,
                rows: count as u64,
                commitment,
            })
        })();
        if result.is_err() {
            rows.truncate(start);
        }
        result
    }

    pub fn header(&self) -> &Header {
        &self.header
    }
    pub fn block_hash(&self) -> B256 {
        self.hash
    }
    pub fn row_count(&self) -> u64 {
        self.rows
    }

    /// Check every block and every row without deduplicating or ignoring extras.
    /// Blocks can arrive in either direction; each block's events retain their
    /// canonical receipt order. Empty blocks remain explicit in `blocks`.
    pub(crate) fn verify_batch<'a>(
        blocks: &'a [Self],
        rows: &[LogRow],
    ) -> io::Result<(&'a Self, &'a Self)> {
        let mut by_number = BTreeMap::new();
        for block in blocks {
            if by_number.insert(block.header.number, block).is_some() {
                return Err(invalid("duplicate block in verified batch"));
            }
        }
        let first = *by_number
            .first_key_value()
            .ok_or_else(|| invalid("empty verified block batch"))?
            .1;
        let last = *by_number
            .last_key_value()
            .expect("nonempty verified batch")
            .1;
        let mut previous: Option<&Self> = None;
        for block in by_number.values() {
            if let Some(parent) = previous
                && (parent.header.number.checked_add(1) != Some(block.header.number)
                    || parent.hash != block.header.parent_hash)
            {
                return Err(invalid(
                    "verified block batch has a gap or divergent parent",
                ));
            }
            previous = Some(block);
        }
        let mut observed = BTreeMap::new();
        let mut offset = 0;
        while offset < rows.len() {
            let number = rows[offset].block_number;
            let block = by_number
                .get(&number)
                .ok_or_else(|| invalid("row has no verified block"))?;
            let count = rows[offset..]
                .iter()
                .take_while(|row| row.block_number == number)
                .count();
            if observed.insert(number, ()).is_some()
                || block.rows != count as u64
                || PrefixState::from_rows(EVENT_NAMESPACE, &rows[offset..offset + count])?
                    .commitment()
                    != block.commitment
            {
                return Err(invalid(
                    "stored event sequence differs from verified receipts",
                ));
            }
            offset += count;
        }
        if by_number
            .values()
            .any(|block| block.rows != 0 && !observed.contains_key(&block.header.number))
        {
            return Err(invalid("verified block events are missing from batch"));
        }
        Ok((first, last))
    }
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
pub(crate) mod tests;
