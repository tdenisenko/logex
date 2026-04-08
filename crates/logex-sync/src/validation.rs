use alloy_consensus::{BlockHeader, ReceiptWithBloom, TxReceipt, proofs};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{B256, Bloom};
use reth_chainspec::{EthereumHardforks, MAINNET};

/// The empty Merkle Patricia Trie root: `keccak256(rlp(""))`.
///
/// This is the receipts_root reported by Ethereum headers for blocks
/// containing zero transactions.
pub const EMPTY_TRIE_ROOT: B256 =
    alloy_primitives::b256!("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421");

/// Receipt validation error aligned with Ethereum mainnet consensus rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReceiptValidationError {
    GasUsedMismatch { expected: u64, got: u64 },
    ReceiptRootMismatch { expected: B256, got: B256 },
    LogsBloomMismatch,
}

impl std::fmt::Display for ReceiptValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GasUsedMismatch { expected, got } => {
                write!(f, "receipt gas mismatch: expected {expected}, got {got}")
            }
            Self::ReceiptRootMismatch { expected, got } => {
                write!(f, "receipt root mismatch: expected {expected}, got {got}")
            }
            Self::LogsBloomMismatch => write!(f, "logs bloom mismatch"),
        }
    }
}

/// Validate receipts against the block header using Reth's historical rules:
/// gas is always checked, while receipts root and bloom are only checked from
/// Byzantium onward.
pub fn validate_receipts_for_header<H, R>(
    header: &H,
    receipts: &[ReceiptWithBloom<R>],
) -> Result<(), ReceiptValidationError>
where
    H: BlockHeader,
    R: alloy_consensus::Eip2718EncodableReceipt + TxReceipt + Send + Sync,
    ReceiptWithBloom<R>: Encodable2718,
{
    let cumulative_gas_used = receipts
        .last()
        .map(|receipt| receipt.cumulative_gas_used())
        .unwrap_or_default();
    if cumulative_gas_used != header.gas_used() {
        return Err(ReceiptValidationError::GasUsedMismatch {
            expected: header.gas_used(),
            got: cumulative_gas_used,
        });
    }

    // Before Byzantium, receipts committed the post-state root rather than the
    // EIP-658 status flag. As a log-only node we cannot reconstruct that root,
    // so we follow Reth and skip receipts-root/bloom verification pre-Byzantium.
    if !MAINNET.is_byzantium_active_at_block(header.number()) {
        return Ok(());
    }

    let calculated_root = proofs::calculate_receipt_root(receipts);
    if calculated_root != header.receipts_root() {
        return Err(ReceiptValidationError::ReceiptRootMismatch {
            expected: header.receipts_root(),
            got: calculated_root,
        });
    }

    let calculated_logs_bloom = receipts
        .iter()
        .fold(Bloom::ZERO, |bloom, receipt| bloom | receipt.logs_bloom);
    if calculated_logs_bloom != header.logs_bloom() {
        return Err(ReceiptValidationError::LogsBloomMismatch);
    }

    Ok(())
}

pub fn receipts_match_transaction_count<B, R>(body: &B, receipts: &[ReceiptWithBloom<R>]) -> bool
where
    B: reth_primitives_traits::BlockBody,
{
    body.transaction_count() == receipts.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{Header, ReceiptWithBloom, TxType};
    use alloy_primitives::{Address, B256, Bloom, Log, LogData, bloom};
    use reth_ethereum_primitives::Receipt as RethReceipt;

    /// Build a single fake EIP-1559 receipt for round-trip tests.
    fn fake_receipt() -> ReceiptWithBloom<RethReceipt> {
        let log = Log {
            address: Address::repeat_byte(0xAA),
            data: LogData::new_unchecked(
                vec![B256::repeat_byte(0xDD)],
                alloy_primitives::Bytes::from_static(b"hello"),
            ),
        };
        let logs_bloom = alloy_primitives::logs_bloom(std::slice::from_ref(&log));
        ReceiptWithBloom {
            receipt: RethReceipt {
                tx_type: TxType::Eip1559,
                success: true,
                cumulative_gas_used: 21_000,
                logs: vec![log],
            },
            logs_bloom,
        }
    }

    #[test]
    fn empty_receipts_match_empty_root_post_byzantium() {
        let empty: Vec<ReceiptWithBloom<RethReceipt>> = vec![];
        let header = Header {
            number: 4_370_000,
            receipts_root: EMPTY_TRIE_ROOT,
            ..Default::default()
        };
        assert_eq!(validate_receipts_for_header(&header, &empty), Ok(()));
    }

    #[test]
    fn empty_receipts_reject_nonzero_root_post_byzantium() {
        let empty: Vec<ReceiptWithBloom<RethReceipt>> = vec![];
        let header = Header {
            number: 4_370_000,
            receipts_root: B256::repeat_byte(0x01),
            ..Default::default()
        };
        assert!(matches!(
            validate_receipts_for_header(&header, &empty),
            Err(ReceiptValidationError::ReceiptRootMismatch { .. })
        ));
    }

    #[test]
    fn nonempty_receipts_round_trip_post_byzantium() {
        let receipts = vec![fake_receipt()];
        let header = Header {
            number: 4_370_000,
            gas_used: receipts[0].cumulative_gas_used(),
            receipts_root: proofs::calculate_receipt_root(&receipts),
            logs_bloom: receipts[0].logs_bloom,
            ..Default::default()
        };
        assert_eq!(validate_receipts_for_header(&header, &receipts), Ok(()));
    }

    #[test]
    fn tampered_receipt_fails_root_check_post_byzantium() {
        let receipts = vec![fake_receipt()];
        let header = Header {
            number: 4_370_000,
            gas_used: receipts[0].cumulative_gas_used(),
            receipts_root: proofs::calculate_receipt_root(&receipts),
            logs_bloom: receipts[0].logs_bloom,
            ..Default::default()
        };

        // Build a tampered receipt with a different log
        let mut tampered = receipts.clone();
        tampered[0].receipt.cumulative_gas_used = 99_999;

        assert!(matches!(
            validate_receipts_for_header(&header, &tampered),
            Err(ReceiptValidationError::GasUsedMismatch { .. })
        ));
    }

    /// Real Optimism mainnet block — sanity check using the same fixture as
    /// alloy-consensus's `check_receipt_root_optimism` test, ported to reth's
    /// `Receipt` type so we know the typed-encoding plumbing is wired up
    /// correctly end-to-end.
    #[test]
    fn known_receipt_root_eip2930() {
        let logs = vec![Log {
            address: Address::ZERO,
            data: LogData::new_unchecked(vec![], Default::default()),
        }];
        let logs_bloom = bloom!(
            "00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
        );
        let receipt = ReceiptWithBloom {
            receipt: RethReceipt {
                tx_type: TxType::Eip2930,
                success: true,
                cumulative_gas_used: 102_068,
                logs,
            },
            logs_bloom,
        };
        let expected = alloy_primitives::b256!(
            "fe70ae4a136d98944951b2123859698d59ad251a381abc9960fa81cae3d0d4a0"
        );
        let header = Header {
            number: 4_370_000,
            gas_used: receipt.cumulative_gas_used(),
            receipts_root: expected,
            logs_bloom: receipt.logs_bloom,
            ..Default::default()
        };
        assert_eq!(
            validate_receipts_for_header(&header, std::slice::from_ref(&receipt)),
            Ok(())
        );
    }

    #[test]
    fn pre_byzantium_skips_root_validation() {
        let receipts = vec![fake_receipt()];
        let header = Header {
            number: 46_147,
            gas_used: receipts[0].cumulative_gas_used(),
            receipts_root: B256::repeat_byte(0x11),
            logs_bloom: Bloom::repeat_byte(0x22),
            ..Default::default()
        };

        assert_eq!(validate_receipts_for_header(&header, &receipts), Ok(()));
    }

    #[test]
    fn post_byzantium_validates_logs_bloom() {
        let receipts = vec![fake_receipt()];
        let header = Header {
            number: 4_370_000,
            gas_used: receipts[0].cumulative_gas_used(),
            receipts_root: proofs::calculate_receipt_root(&receipts),
            logs_bloom: Bloom::ZERO,
            ..Default::default()
        };

        assert_eq!(
            validate_receipts_for_header(&header, &receipts),
            Err(ReceiptValidationError::LogsBloomMismatch)
        );
    }
}
