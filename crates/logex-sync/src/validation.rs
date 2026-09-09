use std::fmt;
use std::sync::LazyLock;

use alloy_consensus::{BlockHeader, Header, ReceiptWithBloom, TxReceipt, proofs};
use alloy_eips::eip2718::Encodable2718;
use alloy_primitives::{B256, Bloom};
use logex_types::ExecutionAnchor;
use reth_chainspec::{ChainSpec, EthereumHardforks, MAINNET};
use reth_consensus::{ConsensusError, HeaderValidator};
use reth_consensus_common::validation::{
    MAX_RLP_BLOCK_SIZE, validate_body_against_header as validate_reth_body_against_header,
};
use reth_ethereum_consensus::EthBeaconConsensus;
use reth_ethereum_primitives::{Block as EthereumBlock, BlockBody as EthereumBlockBody};
use reth_primitives_traits::{Block, BlockBody, GotExpected, SealedHeader};

static EXECUTION_CONSENSUS: LazyLock<EthBeaconConsensus<ChainSpec>> =
    LazyLock::new(|| EthBeaconConsensus::new(MAINNET.clone()));

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

#[derive(Debug, Clone)]
pub enum HeaderValidationError {
    StartBlockMismatch { expected: u64, got: u64 },
    ReverseStartBlockMismatch { expected: u64, got: u64 },
    BrokenReverseParentLink { child: u64, parent: u64 },
    Standalone(ConsensusError),
    AgainstParent(ConsensusError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorValidationError {
    BlockNumberMismatch { expected: u64, got: u64 },
    BlockHashMismatch { expected: B256, got: B256 },
    ReceiptsRootMismatch { expected: B256, got: B256 },
}

impl fmt::Display for HeaderValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StartBlockMismatch { expected, got } => {
                write!(
                    f,
                    "header batch started at unexpected block: expected {expected}, got {got}"
                )
            }
            Self::ReverseStartBlockMismatch { expected, got } => {
                write!(
                    f,
                    "reverse header batch started at unexpected block: expected {expected}, got {got}"
                )
            }
            Self::BrokenReverseParentLink { child, parent } => {
                write!(
                    f,
                    "reverse header batch has a broken parent link: block {parent} is not the parent of block {child}"
                )
            }
            Self::Standalone(error) => write!(f, "{error}"),
            Self::AgainstParent(error) => write!(f, "{error}"),
        }
    }
}

impl fmt::Display for AnchorValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlockNumberMismatch { expected, got } => {
                write!(
                    f,
                    "anchored block number mismatch: expected {expected}, got {got}"
                )
            }
            Self::BlockHashMismatch { expected, got } => {
                write!(
                    f,
                    "anchored block hash mismatch: expected {expected}, got {got}"
                )
            }
            Self::ReceiptsRootMismatch { expected, got } => {
                write!(
                    f,
                    "anchored receipts root mismatch: expected {expected}, got {got}"
                )
            }
        }
    }
}

pub fn validate_header_matches_anchor<H>(
    anchor: &ExecutionAnchor,
    header: &H,
    actual_block_hash: B256,
) -> Result<(), AnchorValidationError>
where
    H: BlockHeader,
{
    if header.number() != anchor.block_number {
        return Err(AnchorValidationError::BlockNumberMismatch {
            expected: anchor.block_number,
            got: header.number(),
        });
    }

    if actual_block_hash != anchor.block_hash {
        return Err(AnchorValidationError::BlockHashMismatch {
            expected: anchor.block_hash,
            got: actual_block_hash,
        });
    }

    if header.receipts_root() != anchor.receipts_root {
        return Err(AnchorValidationError::ReceiptsRootMismatch {
            expected: anchor.receipts_root,
            got: header.receipts_root(),
        });
    }

    Ok(())
}

/// Complete the standalone rules that the upstream validator only checks
/// with a parent (minimum gas limit and excess blob gas) or does not reject
/// before London (base fee).
fn validate_execution_header(header: &SealedHeader<Header>) -> Result<(), ConsensusError> {
    if header.gas_limit() < reth_primitives_traits::constants::MINIMUM_GAS_LIMIT {
        return Err(ConsensusError::GasLimitInvalidMinimum {
            child_gas_limit: header.gas_limit(),
        });
    }
    if !MAINNET.is_london_active_at_block(header.number()) && header.base_fee_per_gas().is_some() {
        return Err(ConsensusError::Other(format!(
            "base fee present before London at block {}",
            header.number()
        )));
    }
    if MAINNET.is_cancun_active_at_timestamp(header.timestamp())
        && header.excess_blob_gas().is_none()
    {
        return Err(ConsensusError::ExcessBlobGasMissing);
    }
    EXECUTION_CONSENSUS.validate_header(header)
}

pub fn validate_downloaded_headers(
    expected_start_block: u64,
    previous_header: Option<&Header>,
    headers: &[Header],
) -> Result<(), HeaderValidationError> {
    let Some(first_header) = headers.first() else {
        return Ok(());
    };

    if first_header.number() != expected_start_block {
        return Err(HeaderValidationError::StartBlockMismatch {
            expected: expected_start_block,
            got: first_header.number(),
        });
    }

    let mut parent = previous_header.cloned().map(SealedHeader::seal_slow);
    for header in headers {
        let sealed = SealedHeader::seal_slow(header.clone());
        validate_execution_header(&sealed).map_err(HeaderValidationError::Standalone)?;

        if let Some(ref parent_header) = parent {
            EXECUTION_CONSENSUS
                .validate_header_against_parent(&sealed, parent_header)
                .map_err(HeaderValidationError::AgainstParent)?;
        }

        parent = Some(sealed);
    }

    Ok(())
}

pub fn validate_reverse_downloaded_headers(
    child_header: &Header,
    headers: &[Header],
) -> Result<(), HeaderValidationError> {
    validate_reverse_downloaded_headers_with_hashes(child_header, headers).map(|_| ())
}

pub fn validate_reverse_downloaded_headers_with_hashes(
    child_header: &Header,
    headers: &[Header],
) -> Result<Vec<B256>, HeaderValidationError> {
    let Some(first_header) = headers.first() else {
        return Ok(Vec::new());
    };

    let expected_start_block = child_header.number().saturating_sub(1);
    if first_header.number() != expected_start_block {
        return Err(HeaderValidationError::ReverseStartBlockMismatch {
            expected: expected_start_block,
            got: first_header.number(),
        });
    }

    let mut child = SealedHeader::new_unhashed(child_header.clone());
    let mut hashes = Vec::with_capacity(headers.len());
    for parent_header in headers {
        let parent_hash = parent_header.hash_slow();
        if child.parent_hash() != parent_hash {
            return Err(HeaderValidationError::BrokenReverseParentLink {
                child: child.number(),
                parent: parent_header.number(),
            });
        }

        let parent = SealedHeader::new(parent_header.clone(), parent_hash);
        validate_execution_header(&parent).map_err(HeaderValidationError::Standalone)?;
        EXECUTION_CONSENSUS
            .validate_header_against_parent(&child, &parent)
            .map_err(HeaderValidationError::AgainstParent)?;
        hashes.push(parent_hash);
        child = parent;
    }

    Ok(hashes)
}

pub fn validate_block_pre_execution(
    header: &Header,
    _block_hash: B256,
    body: &EthereumBlockBody,
) -> Result<(), ConsensusError> {
    validate_reth_body_against_header(body, header)?;

    if let Some(header_blob_gas_used) = header.blob_gas_used() {
        let total_blob_gas = body.blob_gas_used();
        if total_blob_gas != header_blob_gas_used {
            return Err(ConsensusError::BlobGasUsedDiff(GotExpected {
                got: header_blob_gas_used,
                expected: total_blob_gas,
            }));
        }
    }

    if EXECUTION_CONSENSUS
        .chain_spec()
        .is_osaka_active_at_timestamp(header.timestamp())
    {
        let rlp_length = EthereumBlock::rlp_length(header, body);
        if rlp_length > MAX_RLP_BLOCK_SIZE {
            return Err(ConsensusError::BlockTooLarge {
                rlp_length,
                max_rlp_length: MAX_RLP_BLOCK_SIZE,
            });
        }
    }

    Ok(())
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

/// Validate receipts against the block header.
///
/// Gas-used, receipt-root, and logs-bloom checks are all enforced. Ancient
/// pre-Byzantium receipts are now verifiable because LogEx preserves the
/// historical `post_state` value on the wire.
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

    if receipts.is_empty() {
        if header.receipts_root() != EMPTY_TRIE_ROOT {
            return Err(ReceiptValidationError::ReceiptRootMismatch {
                expected: header.receipts_root(),
                got: EMPTY_TRIE_ROOT,
            });
        }
        if header.logs_bloom() != Bloom::ZERO {
            return Err(ReceiptValidationError::LogsBloomMismatch);
        }
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
    use alloy_consensus::{Eip658Value, Header, ReceiptWithBloom, TxType};
    use alloy_primitives::{Address, B256, Bloom, Log, LogData, bloom};
    use reth_ethereum_primitives::Receipt as RethReceipt;

    use crate::primitives::LogexReceipt;

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
    fn historical_mainnet_headers_preserve_hashes_and_transition_ancestry() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/execution_headers.json")).unwrap();
        let headers: Vec<Header> = fixture["headers"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| {
                let header: Header = serde_json::from_value(entry["header"].clone()).unwrap();
                let expected: B256 = serde_json::from_value(entry["hash"].clone()).unwrap();
                assert_eq!(header.number, entry["number"].as_u64().unwrap());
                assert_eq!(header.hash_slow(), expected, "block {}", header.number);
                assert!(
                    validate_downloaded_headers(header.number, None, std::slice::from_ref(&header))
                        .is_ok(),
                    "block {}",
                    header.number
                );
                header
            })
            .collect();
        assert_eq!(headers[0], *MAINNET.genesis_header());
        for pair in headers
            .windows(2)
            .filter(|pair| pair[0].number + 1 == pair[1].number)
        {
            assert!(
                validate_downloaded_headers(pair[1].number, Some(&pair[0]), &pair[1..]).is_ok(),
                "block {}",
                pair[1].number
            );
            assert_eq!(
                validate_reverse_downloaded_headers_with_hashes(&pair[1], &pair[..1]).unwrap(),
                vec![pair[0].hash_slow()]
            );
        }
        let london = headers.iter().find(|h| h.number == 12_965_000).unwrap();
        assert_eq!(london.base_fee_per_gas, Some(1_000_000_000));
        let merge = headers.iter().find(|h| h.number == 15_537_394).unwrap();
        for field in 0..3 {
            let mut bad = merge.clone();
            match field {
                0 => bad.difficulty = alloy_primitives::U256::from(1),
                1 => bad.nonce = alloy_primitives::B64::repeat_byte(1),
                2 => bad.ommers_hash = B256::ZERO,
                _ => unreachable!(),
            }
            assert!(validate_downloaded_headers(bad.number, None, &[bad]).is_err());
        }
    }

    #[test]
    fn header_number_boundaries_do_not_wrap_or_walk_before_genesis() {
        let genesis = MAINNET.genesis_header().clone();
        assert!(validate_reverse_downloaded_headers(&genesis, &[]).is_ok());
        let child = Header {
            parent_hash: genesis.hash_slow(),
            gas_limit: genesis.gas_limit,
            timestamp: 1,
            ..Default::default()
        };
        assert!(matches!(
            validate_reverse_downloaded_headers(&child, &[genesis]),
            Err(HeaderValidationError::AgainstParent(
                ConsensusError::ParentBlockNumberMismatch { .. }
            ))
        ));
        let parent = Header {
            number: u64::MAX,
            gas_limit: 5_000,
            base_fee_per_gas: Some(1),
            ..Default::default()
        };
        let wrapped_child = Header {
            parent_hash: parent.hash_slow(),
            gas_limit: 5_000,
            timestamp: 1,
            ..Default::default()
        };
        assert!(matches!(
            validate_downloaded_headers(0, Some(&parent), &[wrapped_child]),
            Err(HeaderValidationError::AgainstParent(
                ConsensusError::ParentBlockNumberMismatch { .. }
            ))
        ));
    }

    #[test]
    fn london_transition_validates_in_both_directions() {
        let before = Header {
            number: 12_964_999,
            gas_limit: 10_000_000,
            timestamp: 1_628_166_810,
            ..Default::default()
        };
        let london = Header {
            number: before.number + 1,
            parent_hash: before.hash_slow(),
            gas_limit: 20_000_000,
            gas_used: 20_000_000,
            timestamp: before.timestamp + 12,
            base_fee_per_gas: Some(1_000_000_000),
            ..Default::default()
        };
        // A full block raises the base fee by 1/8, per the EIP-1559 formula.
        let after = Header {
            number: london.number + 1,
            parent_hash: london.hash_slow(),
            gas_limit: london.gas_limit,
            timestamp: london.timestamp + 12,
            base_fee_per_gas: Some(1_125_000_000),
            ..Default::default()
        };
        assert!(
            validate_downloaded_headers(
                before.number,
                None,
                &[before.clone(), london.clone(), after.clone()]
            )
            .is_ok()
        );
        assert_eq!(
            validate_reverse_downloaded_headers_with_hashes(
                &after,
                &[london.clone(), before.clone()]
            )
            .unwrap(),
            vec![london.hash_slow(), before.hash_slow()]
        );
        for base_fee in [0, 999_999_999, 1_000_000_001] {
            let bad = Header {
                base_fee_per_gas: Some(base_fee),
                ..london.clone()
            };
            assert!(matches!(
                validate_downloaded_headers(bad.number, Some(&before), &[bad]),
                Err(HeaderValidationError::AgainstParent(
                    ConsensusError::BaseFeeDiff(_)
                ))
            ));
        }
        let bad_parent = Header {
            base_fee_per_gas: Some(0),
            ..before.clone()
        };
        let child = Header {
            parent_hash: bad_parent.hash_slow(),
            ..london.clone()
        };
        assert!(matches!(
            validate_reverse_downloaded_headers(&child, &[bad_parent]),
            Err(HeaderValidationError::Standalone(ConsensusError::Other(_)))
        ));
        let missing_fee = Header {
            base_fee_per_gas: None,
            ..london
        };
        assert!(matches!(
            validate_downloaded_headers(missing_fee.number, Some(&before), &[missing_fee]),
            Err(HeaderValidationError::Standalone(
                ConsensusError::BaseFeeMissing
            ))
        ));
    }

    #[test]
    fn standalone_header_gas_and_extra_data_boundaries() {
        for gas_limit in [5_000, 10_000_000, i64::MAX as u64] {
            for extra_len in [0, 32] {
                let header = Header {
                    number: 1,
                    gas_limit,
                    gas_used: gas_limit,
                    extra_data: vec![0; extra_len].into(),
                    ..Default::default()
                };
                assert!(validate_downloaded_headers(1, None, &[header]).is_ok());
            }
        }
        let header = Header {
            number: 1,
            gas_limit: 5_000,
            gas_used: 5_001,
            ..Default::default()
        };
        assert!(matches!(
            validate_downloaded_headers(1, None, &[header]),
            Err(HeaderValidationError::Standalone(
                ConsensusError::HeaderGasUsedExceedsGasLimit { .. }
            ))
        ));
        let header = Header {
            number: 1,
            gas_limit: u64::MAX,
            ..Default::default()
        };
        assert!(matches!(
            validate_downloaded_headers(1, None, &[header]),
            Err(HeaderValidationError::Standalone(
                ConsensusError::HeaderGasLimitExceedsMax { .. }
            ))
        ));
        let header = Header {
            number: 1,
            gas_limit: 5_000,
            extra_data: vec![0; 33].into(),
            ..Default::default()
        };
        assert!(matches!(
            validate_downloaded_headers(1, None, &[header]),
            Err(HeaderValidationError::Standalone(
                ConsensusError::ExtraDataExceedsMax { .. }
            ))
        ));
    }

    #[test]
    fn timestamp_fork_fields_are_required_only_after_activation() {
        // Mainnet activations pinned by Reth 1.11.3. These synthetic headers
        // test field presence, not canonicality or execution validity.
        const SHANGHAI: u64 = 1_681_338_455;
        const CANCUN: u64 = 1_710_338_135;
        const PRAGUE: u64 = 1_746_612_311;
        for activation in [
            SHANGHAI,
            CANCUN,
            PRAGUE,
            1_764_798_551,
            1_765_290_071,
            1_767_747_671,
        ] {
            for timestamp in [activation - 1, activation, activation + 1] {
                let header = Header {
                    number: 24_000_000,
                    gas_limit: 30_000_000,
                    timestamp,
                    base_fee_per_gas: Some(1),
                    withdrawals_root: (timestamp >= SHANGHAI).then_some(B256::ZERO),
                    blob_gas_used: (timestamp >= CANCUN).then_some(0),
                    excess_blob_gas: (timestamp >= CANCUN).then_some(0),
                    parent_beacon_block_root: (timestamp >= CANCUN).then_some(B256::ZERO),
                    requests_hash: (timestamp >= PRAGUE).then_some(B256::ZERO),
                    ..Default::default()
                };
                assert!(
                    validate_downloaded_headers(header.number, None, std::slice::from_ref(&header))
                        .is_ok(),
                    "timestamp={timestamp}"
                );
                for field in 0..5 {
                    let mut bad = header.clone();
                    match field {
                        0 => {
                            bad.withdrawals_root = if bad.withdrawals_root.is_some() {
                                None
                            } else {
                                Some(B256::ZERO)
                            }
                        }
                        1 => {
                            bad.blob_gas_used = if bad.blob_gas_used.is_some() {
                                None
                            } else {
                                Some(0)
                            }
                        }
                        2 => {
                            bad.excess_blob_gas = if bad.excess_blob_gas.is_some() {
                                None
                            } else {
                                Some(0)
                            }
                        }
                        3 => {
                            bad.parent_beacon_block_root = if bad.parent_beacon_block_root.is_some()
                            {
                                None
                            } else {
                                Some(B256::ZERO)
                            }
                        }
                        4 => {
                            bad.requests_hash = if bad.requests_hash.is_some() {
                                None
                            } else {
                                Some(B256::ZERO)
                            }
                        }
                        _ => unreachable!(),
                    }
                    assert!(
                        validate_downloaded_headers(bad.number, None, &[bad]).is_err(),
                        "timestamp={timestamp}, field={field}"
                    );
                }
            }
        }
    }

    #[test]
    fn standalone_header_rejects_base_fee_before_london() {
        let header = Header {
            number: 12_964_999,
            gas_limit: 10_000_000,
            base_fee_per_gas: Some(0),
            ..Default::default()
        };
        assert!(validate_downloaded_headers(header.number, None, &[header]).is_err());
    }

    #[test]
    fn standalone_header_rejects_gas_limit_below_minimum() {
        for gas_limit in [0, 1, 4_999] {
            let header = Header {
                number: 1,
                gas_limit,
                ..Default::default()
            };
            assert!(
                validate_downloaded_headers(header.number, None, &[header]).is_err(),
                "gas_limit={gas_limit}"
            );
        }
    }

    #[test]
    fn reverse_headers_reject_wrong_start_block_before_consensus_validation() {
        let child = Header {
            number: 10,
            parent_hash: B256::repeat_byte(0xAA),
            ..Default::default()
        };
        let wrong_parent = Header {
            number: 8,
            ..Default::default()
        };

        assert!(matches!(
            validate_reverse_downloaded_headers(&child, &[wrong_parent]),
            Err(HeaderValidationError::ReverseStartBlockMismatch {
                expected: 9,
                got: 8
            })
        ));
    }

    #[test]
    fn reverse_headers_reject_broken_parent_link_before_consensus_validation() {
        let child = Header {
            number: 10,
            parent_hash: B256::repeat_byte(0xAA),
            ..Default::default()
        };
        let wrong_parent = Header {
            number: 9,
            parent_hash: B256::repeat_byte(0xBB),
            ..Default::default()
        };

        assert!(matches!(
            validate_reverse_downloaded_headers(&child, &[wrong_parent]),
            Err(HeaderValidationError::BrokenReverseParentLink {
                child: 10,
                parent: 9
            })
        ));
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
    fn empty_receipts_reject_nonzero_logs_bloom() {
        let empty: Vec<ReceiptWithBloom<RethReceipt>> = vec![];
        let header = Header {
            number: 4_370_000,
            receipts_root: EMPTY_TRIE_ROOT,
            logs_bloom: Bloom::repeat_byte(0x01),
            ..Default::default()
        };
        assert_eq!(
            validate_receipts_for_header(&header, &empty),
            Err(ReceiptValidationError::LogsBloomMismatch)
        );
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

    #[test]
    fn receipt_root_binds_each_log_field_status_type_and_transaction_order() {
        let mut second = fake_receipt();
        second.receipt.cumulative_gas_used = 42_000;
        let receipts = vec![fake_receipt(), second];
        let header = Header {
            number: 20_000_000,
            gas_used: 42_000,
            receipts_root: proofs::calculate_receipt_root(&receipts),
            logs_bloom: receipts[0].logs_bloom | receipts[1].logs_bloom,
            ..Default::default()
        };
        assert_eq!(validate_receipts_for_header(&header, &receipts), Ok(()));
        for mutation in 0..8 {
            let mut tampered = receipts.clone();
            let receipt = &mut tampered[0].receipt;
            match mutation {
                0 => receipt.logs[0].address = Address::ZERO,
                1 => receipt.logs[0].data.topics_mut()[0] = B256::ZERO,
                2 => receipt.logs[0].data.data = alloy_primitives::Bytes::new(),
                3 => receipt.logs.clear(),
                4 => receipt.success = false,
                5 => receipt.tx_type = TxType::Legacy,
                6 => receipt.cumulative_gas_used = 20_999,
                _ => tampered.swap(0, 1),
            }
            assert!(
                validate_receipts_for_header(&header, &tampered).is_err(),
                "mutation={mutation}"
            );
        }
    }

    #[test]
    fn anchored_header_must_match_consensus_anchor() {
        let header = Header {
            number: 11,
            receipts_root: B256::repeat_byte(0x55),
            ..Default::default()
        };
        let anchor = ExecutionAnchor {
            beacon_root: B256::repeat_byte(0x11),
            beacon_slot: 999,
            block_number: 11,
            block_hash: header.hash_slow(),
            receipts_root: B256::repeat_byte(0x55),
        };
        assert_eq!(
            validate_header_matches_anchor(&anchor, &header, header.hash_slow()),
            Ok(())
        );

        let wrong_anchor = ExecutionAnchor {
            receipts_root: B256::repeat_byte(0x66),
            ..anchor
        };
        assert!(matches!(
            validate_header_matches_anchor(&wrong_anchor, &header, header.hash_slow()),
            Err(AnchorValidationError::ReceiptsRootMismatch { .. })
        ));
    }

    #[test]
    fn pre_execution_validation_rejects_blob_gas_mismatch() {
        let body = EthereumBlockBody::default();
        let header = Header {
            transactions_root: body.calculate_tx_root(),
            ommers_hash: body.calculate_ommers_root(),
            withdrawals_root: body.calculate_withdrawals_root(),
            blob_gas_used: Some(1),
            ..Default::default()
        };

        assert!(matches!(
            validate_block_pre_execution(&header, header.hash_slow(), &body),
            Err(ConsensusError::BlobGasUsedDiff(_))
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
    fn pre_byzantium_validates_root_and_logs_bloom() {
        let logs = vec![Log {
            address: Address::repeat_byte(0xAA),
            data: LogData::new_unchecked(
                vec![B256::repeat_byte(0xBB)],
                alloy_primitives::Bytes::from_static(b"ancient"),
            ),
        }];
        let logs_bloom = alloy_primitives::logs_bloom(logs.iter());
        let receipts = vec![ReceiptWithBloom {
            receipt: LogexReceipt {
                tx_type: TxType::Legacy,
                status: Eip658Value::PostState(B256::repeat_byte(0x44)),
                cumulative_gas_used: 21_000,
                logs,
            },
            logs_bloom,
        }];
        let header = Header {
            number: 46_147,
            gas_used: receipts[0].cumulative_gas_used(),
            receipts_root: proofs::calculate_receipt_root(&receipts),
            logs_bloom: receipts[0].logs_bloom,
            ..Default::default()
        };

        assert_eq!(validate_receipts_for_header(&header, &receipts), Ok(()));
    }

    #[test]
    fn tampered_pre_byzantium_receipt_fails_root_check() {
        let logs = vec![Log {
            address: Address::repeat_byte(0xAA),
            data: LogData::new_unchecked(
                vec![B256::repeat_byte(0xBB)],
                alloy_primitives::Bytes::from_static(b"ancient"),
            ),
        }];
        let logs_bloom = alloy_primitives::logs_bloom(logs.iter());
        let receipts = vec![ReceiptWithBloom {
            receipt: LogexReceipt {
                tx_type: TxType::Legacy,
                status: Eip658Value::PostState(B256::repeat_byte(0x44)),
                cumulative_gas_used: 21_000,
                logs,
            },
            logs_bloom,
        }];
        let header = Header {
            number: 46_147,
            gas_used: receipts[0].cumulative_gas_used(),
            receipts_root: proofs::calculate_receipt_root(&receipts),
            logs_bloom: receipts[0].logs_bloom,
            ..Default::default()
        };

        let mut tampered = receipts.clone();
        tampered[0].receipt.status = Eip658Value::PostState(B256::repeat_byte(0x99));

        assert!(matches!(
            validate_receipts_for_header(&header, &tampered),
            Err(ReceiptValidationError::ReceiptRootMismatch { .. })
        ));
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
