use alloy_consensus::{ReceiptWithBloom, proofs};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::B256;

/// The empty Merkle Patricia Trie root: `keccak256(rlp(""))`.
///
/// This is the receipts_root reported by Ethereum headers for blocks
/// containing zero transactions.
pub const EMPTY_TRIE_ROOT: B256 =
    alloy_primitives::b256!("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421");

/// Validate that a set of typed receipts (with blooms) hashes to the expected
/// receipts root from the block header.
///
/// This is the core trustless verification step for a light node:
/// - The header's `receipts_root` is consensus-attested (signed by 2/3 of validators).
/// - This function recomputes the Merkle Patricia Trie root from the receipts a
///   peer sent us and rejects them if they don't match.
///
/// Receipts come from the wire as `ReceiptWithBloom<Receipt>` where `Receipt`
/// already carries its own `tx_type`, so EIP-2718 envelope encoding works
/// without any external metadata.
pub fn validate_receipt_root<R>(receipts: &[ReceiptWithBloom<R>], expected_root: B256) -> bool
where
    R: alloy_consensus::Eip2718EncodableReceipt + Send + Sync,
    ReceiptWithBloom<R>: Encodable2718,
{
    if receipts.is_empty() {
        return expected_root == EMPTY_TRIE_ROOT;
    }
    proofs::calculate_receipt_root(receipts) == expected_root
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{ReceiptWithBloom, TxType};
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
        ReceiptWithBloom {
            receipt: RethReceipt {
                tx_type: TxType::Eip1559,
                success: true,
                cumulative_gas_used: 21_000,
                logs: vec![log],
            },
            logs_bloom: Bloom::default(),
        }
    }

    #[test]
    fn empty_receipts_match_empty_root() {
        let empty: Vec<ReceiptWithBloom<RethReceipt>> = vec![];
        assert!(validate_receipt_root(&empty, EMPTY_TRIE_ROOT));
    }

    #[test]
    fn empty_receipts_reject_nonzero_root() {
        let empty: Vec<ReceiptWithBloom<RethReceipt>> = vec![];
        assert!(!validate_receipt_root(&empty, B256::repeat_byte(0x01)));
    }

    #[test]
    fn nonempty_receipts_round_trip() {
        let receipts = vec![fake_receipt()];
        let root = proofs::calculate_receipt_root(&receipts);
        assert!(validate_receipt_root(&receipts, root));
    }

    #[test]
    fn tampered_receipt_fails_root_check() {
        let receipts = vec![fake_receipt()];
        let real_root = proofs::calculate_receipt_root(&receipts);

        // Build a tampered receipt with a different log
        let mut tampered = receipts.clone();
        tampered[0].receipt.cumulative_gas_used = 99_999;

        let tampered_root = proofs::calculate_receipt_root(&tampered);
        assert_ne!(real_root, tampered_root);
        assert!(!validate_receipt_root(&tampered, real_root));
        assert!(validate_receipt_root(&tampered, tampered_root));
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
        assert!(validate_receipt_root(
            std::slice::from_ref(&receipt),
            expected
        ));
    }
}
