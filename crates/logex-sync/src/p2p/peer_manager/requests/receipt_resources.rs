//! Necessary receipt resource bounds for the supported mainnet gas schedule.
//!
//! Intrinsic transaction gas and retained LOG costs imply
//! `21000 * receipts + 375 * logs + 375 * topics + 8 * data_bytes <= 2 * gas_used`.
//! The factor two accommodates the historical half-gas refund cap. This is not
//! receipt-root validation, header authentication, or a process-memory bound:
//! callers retain their existing ancestry, attribution, and final validation.

use alloy_consensus::Header;
use alloy_primitives::B256;

use crate::primitives::LogexReceipt;

/// Request identity and resource authority derived from the same header.
/// Construction binds the fields; it does not authenticate the header.
#[derive(Clone, Copy, Debug)]
pub struct ReceiptRequestContext {
    hash: B256,
    gas_used: u64,
}

impl ReceiptRequestContext {
    pub fn from_header(header: &Header) -> Self {
        Self {
            hash: header.hash_slow(),
            gas_used: header.gas_used,
        }
    }

    pub fn block_hash(&self) -> B256 {
        self.hash
    }

    pub(super) fn gas_used(&self) -> u64 {
        self.gas_used
    }

    #[cfg(test)]
    pub(super) fn test_with_hash(hash: B256, gas_used: u64) -> Self {
        Self { hash, gas_used }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::p2p::peer_manager) struct ReceiptResourceExceeded {
    pub(in crate::p2p::peer_manager) block_hash: B256,
    pub(in crate::p2p::peer_manager) max_weight: u128,
}

/// Check only new receipts, carrying the weight of an unfinished block forward.
/// Receipt-reported cumulative gas is deliberately not an authority here.
pub(super) fn check_receipt_block<'a>(
    context: &ReceiptRequestContext,
    previous_weight: u128,
    receipts: impl IntoIterator<Item = &'a LogexReceipt>,
) -> Result<u128, ReceiptResourceExceeded> {
    // Multiplying a u64 by two is representable in u128, including u64::MAX.
    let max_weight = u128::from(context.gas_used()) * 2;
    let exceeded = || ReceiptResourceExceeded {
        block_hash: context.block_hash(),
        max_weight,
    };
    if previous_weight > max_weight {
        return Err(exceeded());
    }
    let mut weight = previous_weight;
    let mut charge = |amount: u128| {
        weight = weight.checked_add(amount).ok_or_else(exceeded)?;
        if weight > max_weight {
            return Err(exceeded());
        }
        Ok(())
    };
    for receipt in receipts {
        charge(21_000)?;
        for log in &receipt.logs {
            charge(375)?;
            charge(
                (log.topics().len() as u128)
                    .checked_mul(375)
                    .ok_or_else(exceeded)?,
            )?;
            charge(
                (log.data.data.len() as u128)
                    .checked_mul(8)
                    .ok_or_else(exceeded)?,
            )?;
        }
    }
    Ok(weight)
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{Eip658Value, TxType};
    use alloy_primitives::{Address, Bytes, Log};

    use super::*;

    fn context(gas_used: u64) -> ReceiptRequestContext {
        ReceiptRequestContext::test_with_hash(B256::repeat_byte(7), gas_used)
    }

    fn receipt(topics: usize, data_len: usize) -> LogexReceipt {
        LogexReceipt {
            // An untrusted gas claim must not enlarge or reduce the bound.
            cumulative_gas_used: u64::MAX,
            logs: vec![
                Log::new(
                    Address::ZERO,
                    vec![B256::ZERO; topics],
                    Bytes::from(vec![0; data_len]),
                )
                .unwrap(),
            ],
            ..Default::default()
        }
    }

    #[test]
    fn header_constructor_binds_hash_and_gas() {
        let mut header = Header {
            gas_used: 21_000,
            ..Default::default()
        };
        let first = ReceiptRequestContext::from_header(&header);
        assert_eq!(first.block_hash(), header.hash_slow());
        assert_eq!(first.gas_used(), 21_000);
        header.gas_used = 21_001;
        let second = ReceiptRequestContext::from_header(&header);
        assert_eq!(second.block_hash(), header.hash_slow());
        assert_ne!(first.block_hash(), second.block_hash());
        assert_eq!(second.gas_used(), 21_001);
    }

    #[test]
    fn empty_and_carried_weight_obey_limits() {
        assert_eq!(check_receipt_block(&context(0), 0, []).unwrap(), 0);
        assert!(check_receipt_block(&context(0), 1, []).is_err());
        assert!(check_receipt_block(&context(0), 0, [&LogexReceipt::default()]).is_err());
        let receipt = LogexReceipt::default();
        let first = check_receipt_block(&context(21_000), 0, [&receipt]).unwrap();
        assert_eq!(first, 21_000);
        assert_eq!(
            check_receipt_block(&context(21_000), first, [&receipt]).unwrap(),
            42_000
        );
        assert!(check_receipt_block(&context(21_000), 42_000, [&receipt]).is_err());
        assert_eq!(
            check_receipt_block(&context(21_000), 42_000, []).unwrap(),
            42_000
        );
    }

    #[test]
    fn each_log_topic_and_data_term_is_counted() {
        for (topics, data, expected) in [(0, 0, 21_375), (1, 0, 21_750), (4, 3, 22_899)] {
            let receipt = receipt(topics, data);
            assert_eq!(
                check_receipt_block(&context(50_000), 0, [&receipt]).unwrap(),
                expected
            );
            // Add one carried weight unit for odd weights, yielding an exact even bound.
            let previous = expected % 2;
            let gas = ((expected + previous) / 2) as u64;
            assert_eq!(
                check_receipt_block(&context(gas), previous, [&receipt]).unwrap(),
                expected + previous
            );
            assert!(check_receipt_block(&context(gas - 1), previous, [&receipt]).is_err());
        }
        let mut two_logs = receipt(0, 0);
        two_logs.logs.push(two_logs.logs[0].clone());
        assert_eq!(
            check_receipt_block(&context(50_000), 0, [&two_logs]).unwrap(),
            21_750
        );
    }

    #[test]
    fn receipt_envelope_and_status_do_not_change_weight() {
        // Arithmetic fixtures only: no claim these receipts execute in the EVM.
        for tx_type in [
            TxType::Legacy,
            TxType::Eip2930,
            TxType::Eip1559,
            TxType::Eip4844,
            TxType::Eip7702,
        ] {
            let mut receipt = receipt(1, 2);
            receipt.tx_type = tx_type;
            let mut statuses = vec![Eip658Value::Eip658(false), Eip658Value::Eip658(true)];
            if tx_type == TxType::Legacy {
                statuses.push(Eip658Value::PostState(B256::ZERO));
            }
            for status in statuses {
                receipt.status = status;
                receipt.cumulative_gas_used = 0;
                assert_eq!(
                    check_receipt_block(&context(50_000), 0, [&receipt]).unwrap(),
                    21_766
                );
            }
        }
    }

    #[test]
    fn half_refund_boundary_and_large_header_are_overflow_safe() {
        let receipt = LogexReceipt::default();
        assert_eq!(
            check_receipt_block(&context(10_500), 0, [&receipt]).unwrap(),
            21_000
        );
        let error = check_receipt_block(&context(10_499), 0, [&receipt]).unwrap_err();
        assert_eq!(error.block_hash, B256::repeat_byte(7));
        assert_eq!(error.max_weight, 20_998);
        let max_weight = u128::from(u64::MAX) * 2;
        assert_eq!(
            check_receipt_block(&context(u64::MAX), max_weight - 21_000, [&receipt]).unwrap(),
            max_weight
        );
        assert!(check_receipt_block(&context(u64::MAX), max_weight - 20_999, [&receipt]).is_err());
        assert!(check_receipt_block(&context(u64::MAX), u128::MAX, []).is_err());
    }
}
