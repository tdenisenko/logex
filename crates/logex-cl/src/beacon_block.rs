use alloy_primitives::{Address, B256, FixedBytes, U256};
use logex_types::{ConsensusDataFork, ExecutionAnchor};
use ssz::Decode;
use ssz_derive::{Decode, Encode};
use ssz_types::{BitList, BitVector, FixedVector, VariableList};
use thiserror::Error;
use tree_hash::{TreeHash as _, merkle_root, mix_in_length};
use tree_hash_derive::TreeHash;
use typenum::{U1, U2, U8, U16, U32, U33, U64, U4096, U8192, U131072, U1048576, U1073741824};

use crate::{MAINNET_CONSENSUS_CHAIN_SPEC, rpc::RawRpcResponse};

const BLS_PUBKEY_BYTES: usize = 48;
const BLS_SIGNATURE_BYTES: usize = 96;
const KZG_COMMITMENT_BYTES: usize = 48;
const LOGS_BLOOM_BYTES: usize = 256;

// Protocol collection limits live in the SSZ field types, so decoding and
// tree hashing cannot disagree about a list limit or fixed-vector length.
#[derive(Debug, Error)]
pub(crate) enum BeaconBlockError {
    #[error("beacon block response was missing v2 fork context bytes")]
    MissingForkContext,
    #[error("unsupported beacon block fork context 0x{0}")]
    UnsupportedForkContext(String),
    #[error("failed to decode electra/fulu beacon block payload: {0}")]
    DecodeElectra(String),
    #[error(
        "beacon block fork context 0x{actual} does not match slot {slot} (expected 0x{expected})"
    )]
    ForkContextMismatch {
        slot: u64,
        expected: String,
        actual: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct VerifiedBeaconBlock {
    pub fork: ConsensusDataFork,
    pub beacon_root: B256,
    pub parent_root: B256,
    pub slot: u64,
    pub execution_anchor: ExecutionAnchor,
}

type SszAggregationBits = BitList<U131072>;
type SszCommitteeBits = BitVector<U64>;
type SszTransactions = VariableList<VariableList<u8, U1073741824>, U1048576>;

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct Eth1DataSsz {
    deposit_root: B256,
    deposit_count: u64,
    block_hash: B256,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct CheckpointSsz {
    epoch: u64,
    root: B256,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct AttestationDataSsz {
    slot: u64,
    index: u64,
    beacon_block_root: B256,
    source: CheckpointSsz,
    target: CheckpointSsz,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct BeaconBlockHeaderSsz {
    slot: u64,
    proposer_index: u64,
    parent_root: B256,
    state_root: B256,
    body_root: B256,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct SignedBeaconBlockHeaderSsz {
    message: BeaconBlockHeaderSsz,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct ProposerSlashingSsz {
    signed_header_1: SignedBeaconBlockHeaderSsz,
    signed_header_2: SignedBeaconBlockHeaderSsz,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct VoluntaryExitSsz {
    epoch: u64,
    validator_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct SignedVoluntaryExitSsz {
    message: VoluntaryExitSsz,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct SyncAggregateSsz {
    sync_committee_bits: FixedBytes<64>,
    sync_committee_signature: FixedBytes<BLS_SIGNATURE_BYTES>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct DepositDataSsz {
    pubkey: FixedBytes<BLS_PUBKEY_BYTES>,
    withdrawal_credentials: B256,
    amount: u64,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct DepositSsz {
    proof: FixedVector<B256, U33>,
    data: DepositDataSsz,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct BlsToExecutionChangeSsz {
    validator_index: u64,
    from_bls_pubkey: FixedBytes<BLS_PUBKEY_BYTES>,
    to_execution_address: Address,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct SignedBlsToExecutionChangeSsz {
    message: BlsToExecutionChangeSsz,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct DepositRequestSsz {
    pubkey: FixedBytes<BLS_PUBKEY_BYTES>,
    withdrawal_credentials: B256,
    amount: u64,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
    index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct WithdrawalRequestSsz {
    source_address: Address,
    validator_pubkey: FixedBytes<BLS_PUBKEY_BYTES>,
    amount: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct ConsolidationRequestSsz {
    source_address: Address,
    source_pubkey: FixedBytes<BLS_PUBKEY_BYTES>,
    target_pubkey: FixedBytes<BLS_PUBKEY_BYTES>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct ExecutionRequestsSsz {
    deposits: VariableList<DepositRequestSsz, U8192>,
    withdrawals: VariableList<WithdrawalRequestSsz, U16>,
    consolidations: VariableList<ConsolidationRequestSsz, U2>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct IndexedAttestationElectraSsz {
    attesting_indices: VariableList<u64, U131072>,
    data: AttestationDataSsz,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct AttesterSlashingElectraSsz {
    attestation_1: IndexedAttestationElectraSsz,
    attestation_2: IndexedAttestationElectraSsz,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode, TreeHash)]
struct AttestationElectraSsz {
    aggregation_bits: SszAggregationBits,
    data: AttestationDataSsz,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
    committee_bits: SszCommitteeBits,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct WithdrawalSsz {
    index: u64,
    validator_index: u64,
    address: Address,
    amount: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode)]
struct ExecutionPayloadElectraSsz {
    parent_hash: B256,
    fee_recipient: Address,
    state_root: B256,
    receipts_root: B256,
    logs_bloom: FixedBytes<LOGS_BLOOM_BYTES>,
    prev_randao: B256,
    block_number: u64,
    gas_limit: u64,
    gas_used: u64,
    timestamp: u64,
    extra_data: VariableList<u8, U32>,
    base_fee_per_gas: U256,
    block_hash: B256,
    transactions: SszTransactions,
    withdrawals: VariableList<WithdrawalSsz, U16>,
    blob_gas_used: u64,
    excess_blob_gas: u64,
}

// The generic list TreeHash packs each u8 individually. Hash contiguous byte
// slices directly, retaining the same bound for SSZ decoding and tree depth.
fn byte_list_root<N: typenum::Unsigned>(bytes: &VariableList<u8, N>) -> B256 {
    mix_in_length(&merkle_root(bytes, N::USIZE.div_ceil(32)), bytes.len())
}

impl tree_hash::TreeHash for ExecutionPayloadElectraSsz {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        tree_hash::TreeHashType::Container
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        unreachable!("execution payload is a container")
    }

    fn tree_hash_packing_factor() -> usize {
        unreachable!("execution payload is a container")
    }

    fn tree_hash_root(&self) -> B256 {
        let mut transaction_roots = Vec::with_capacity(self.transactions.len() * 32);
        for transaction in &self.transactions {
            transaction_roots.extend_from_slice(byte_list_root(transaction).as_slice());
        }
        let transactions_root = mix_in_length(
            &merkle_root(&transaction_roots, SszTransactions::max_len()),
            self.transactions.len(),
        );
        let fields = [
            self.parent_hash.tree_hash_root(),
            self.fee_recipient.tree_hash_root(),
            self.state_root.tree_hash_root(),
            self.receipts_root.tree_hash_root(),
            self.logs_bloom.tree_hash_root(),
            self.prev_randao.tree_hash_root(),
            self.block_number.tree_hash_root(),
            self.gas_limit.tree_hash_root(),
            self.gas_used.tree_hash_root(),
            self.timestamp.tree_hash_root(),
            byte_list_root(&self.extra_data),
            self.base_fee_per_gas.tree_hash_root(),
            self.block_hash.tree_hash_root(),
            transactions_root,
            self.withdrawals.tree_hash_root(),
            self.blob_gas_used.tree_hash_root(),
            self.excess_blob_gas.tree_hash_root(),
        ];
        let mut bytes = Vec::with_capacity(fields.len() * 32);
        for field in fields {
            bytes.extend_from_slice(field.as_slice());
        }
        merkle_root(&bytes, fields.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct BeaconBlockBodyElectraSsz {
    randao_reveal: FixedBytes<BLS_SIGNATURE_BYTES>,
    eth1_data: Eth1DataSsz,
    graffiti: B256,
    proposer_slashings: VariableList<ProposerSlashingSsz, U16>,
    attester_slashings: VariableList<AttesterSlashingElectraSsz, U1>,
    attestations: VariableList<AttestationElectraSsz, U8>,
    deposits: VariableList<DepositSsz, U16>,
    voluntary_exits: VariableList<SignedVoluntaryExitSsz, U16>,
    sync_aggregate: SyncAggregateSsz,
    execution_payload: ExecutionPayloadElectraSsz,
    bls_to_execution_changes: VariableList<SignedBlsToExecutionChangeSsz, U16>,
    blob_kzg_commitments: VariableList<FixedBytes<KZG_COMMITMENT_BYTES>, U4096>,
    execution_requests: ExecutionRequestsSsz,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct BeaconBlockElectraSsz {
    slot: u64,
    proposer_index: u64,
    parent_root: B256,
    state_root: B256,
    body: BeaconBlockBodyElectraSsz,
}

impl BeaconBlockElectraSsz {
    fn header(&self) -> BeaconBlockHeaderSsz {
        BeaconBlockHeaderSsz {
            slot: self.slot,
            proposer_index: self.proposer_index,
            parent_root: self.parent_root,
            state_root: self.state_root,
            body_root: self.body.tree_hash_root(),
        }
    }

    fn execution_anchor(&self) -> ExecutionAnchor {
        let header = self.header();
        ExecutionAnchor {
            beacon_root: header.tree_hash_root(),
            beacon_slot: self.slot,
            block_number: self.body.execution_payload.block_number,
            block_hash: self.body.execution_payload.block_hash,
            receipts_root: self.body.execution_payload.receipts_root,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode)]
struct SignedBeaconBlockElectraSsz {
    message: BeaconBlockElectraSsz,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
}

pub(crate) fn decode_verified_beacon_block(
    response: &RawRpcResponse,
) -> Result<VerifiedBeaconBlock, BeaconBlockError> {
    if let Some(context_bytes) = response.context_bytes {
        let fork = fork_from_context(context_bytes)?;
        let block = decode_verified_block_for_fork(response, fork)?;
        let expected = MAINNET_CONSENSUS_CHAIN_SPEC
            .fork_digest_for_epoch(MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(block.slot));
        if context_bytes != expected {
            return Err(BeaconBlockError::ForkContextMismatch {
                slot: block.slot,
                expected: hex_context(expected),
                actual: hex_context(context_bytes),
            });
        }
        return Ok(block);
    }

    let block = decode_electra_block(response)?;
    let epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(block.slot);
    if matches!(
        MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(epoch)[0],
        0x05 | 0x06
    ) {
        return Ok(block);
    }

    Err(BeaconBlockError::MissingForkContext)
}

fn fork_from_context(context_bytes: [u8; 4]) -> Result<ConsensusDataFork, BeaconBlockError> {
    let Some(version) = MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_digest(context_bytes) else {
        let digest = hex_context(context_bytes);
        return Err(BeaconBlockError::UnsupportedForkContext(digest));
    };

    match version[0] {
        0x05 | 0x06 => Ok(ConsensusDataFork::Electra),
        0x04 => Ok(ConsensusDataFork::Deneb),
        0x03 => Ok(ConsensusDataFork::Capella),
        _ => {
            let digest = hex_context(context_bytes);
            Err(BeaconBlockError::UnsupportedForkContext(digest))
        }
    }
}

fn decode_verified_block_for_fork(
    response: &RawRpcResponse,
    fork: ConsensusDataFork,
) -> Result<VerifiedBeaconBlock, BeaconBlockError> {
    match fork {
        ConsensusDataFork::Electra => decode_electra_block(response),
        ConsensusDataFork::Capella | ConsensusDataFork::Deneb => {
            let context = response.context_bytes.unwrap_or_default();
            Err(BeaconBlockError::UnsupportedForkContext(hex_context(
                context,
            )))
        }
    }
}

fn decode_electra_block(
    response: &RawRpcResponse,
) -> Result<VerifiedBeaconBlock, BeaconBlockError> {
    let block = SignedBeaconBlockElectraSsz::from_ssz_bytes(&response.bytes)
        .map_err(|error| BeaconBlockError::DecodeElectra(format!("{error:?}")))?;
    let header = block.message.header();
    let actual_root = header.tree_hash_root();
    let execution_anchor = block.message.execution_anchor();
    Ok(VerifiedBeaconBlock {
        fork: ConsensusDataFork::Electra,
        beacon_root: actual_root,
        parent_root: header.parent_root,
        slot: header.slot,
        execution_anchor,
    })
}

fn hex_context(context_bytes: [u8; 4]) -> String {
    format!(
        "{:02x}{:02x}{:02x}{:02x}",
        context_bytes[0], context_bytes[1], context_bytes[2], context_bytes[3]
    )
}

#[cfg(test)]
mod tests {
    use alloy_primitives::b256;
    use ssz::Encode as _;

    use super::*;

    fn fixed<const N: usize>(byte: u8) -> FixedBytes<N> {
        FixedBytes::repeat_byte(byte)
    }

    fn sample_block(slot: u64, parent_root: B256) -> SignedBeaconBlockElectraSsz {
        SignedBeaconBlockElectraSsz {
            message: BeaconBlockElectraSsz {
                slot,
                proposer_index: 7,
                parent_root,
                state_root: B256::repeat_byte(0x02),
                body: BeaconBlockBodyElectraSsz {
                    randao_reveal: fixed::<BLS_SIGNATURE_BYTES>(0x03),
                    eth1_data: Eth1DataSsz {
                        deposit_root: B256::repeat_byte(0x04),
                        deposit_count: 9,
                        block_hash: B256::repeat_byte(0x05),
                    },
                    graffiti: B256::repeat_byte(0x06),
                    proposer_slashings: VariableList::default(),
                    attester_slashings: VariableList::default(),
                    attestations: VariableList::default(),
                    deposits: VariableList::default(),
                    voluntary_exits: VariableList::default(),
                    sync_aggregate: SyncAggregateSsz {
                        sync_committee_bits: fixed::<64>(0x07),
                        sync_committee_signature: fixed::<BLS_SIGNATURE_BYTES>(0x08),
                    },
                    execution_payload: ExecutionPayloadElectraSsz {
                        parent_hash: B256::repeat_byte(0x09),
                        fee_recipient: Address::repeat_byte(0x0a),
                        state_root: B256::repeat_byte(0x0b),
                        receipts_root: B256::repeat_byte(0x0c),
                        logs_bloom: fixed::<LOGS_BLOOM_BYTES>(0x0d),
                        prev_randao: B256::repeat_byte(0x0e),
                        block_number: 12_345,
                        gas_limit: 30_000_000,
                        gas_used: 21_000,
                        timestamp: 1_700_000_000,
                        extra_data: VariableList::new(vec![0xde, 0xad, 0xbe, 0xef]).unwrap(),
                        base_fee_per_gas: U256::from(100u64),
                        block_hash: B256::repeat_byte(0x0f),
                        transactions: VariableList::new(vec![
                            VariableList::new(vec![0x01, 0x02, 0x03]).unwrap(),
                        ])
                        .unwrap(),
                        withdrawals: VariableList::new(vec![WithdrawalSsz {
                            index: 1,
                            validator_index: 2,
                            address: Address::repeat_byte(0x10),
                            amount: 3,
                        }])
                        .unwrap(),
                        blob_gas_used: 4,
                        excess_blob_gas: 5,
                    },
                    bls_to_execution_changes: VariableList::default(),
                    blob_kzg_commitments: VariableList::new(vec![fixed::<KZG_COMMITMENT_BYTES>(
                        0x11,
                    )])
                    .unwrap(),
                    execution_requests: ExecutionRequestsSsz::default(),
                },
            },
            signature: fixed::<BLS_SIGNATURE_BYTES>(0x12),
        }
    }

    #[test]
    fn canonical_fixed_deposit_proof_decodes() {
        // SSZ Vector[Bytes32, 33] is inline, with no offset or length prefix.
        let mut bytes = vec![0x42; 33 * 32];
        let data = DepositDataSsz {
            pubkey: fixed(0x11),
            withdrawal_credentials: fixed(0x22),
            amount: 32_000_000_000,
            signature: fixed(0x33),
        };
        bytes.extend_from_slice(&data.as_ssz_bytes());
        let deposit = DepositSsz::from_ssz_bytes(&bytes).expect("canonical deposit must decode");
        assert_eq!(deposit.data, data);
        assert_eq!(deposit.proof.len(), 33);
        assert_eq!(deposit.as_ssz_bytes(), bytes);
        for count in [0, 32, 34] {
            let mut malformed = vec![0x42; count * 32];
            malformed.extend_from_slice(&data.as_ssz_bytes());
            assert!(DepositSsz::from_ssz_bytes(&malformed).is_err());
        }

        // Insert independently assembled fixed-size deposits into a complete
        // block, adjusting only the SSZ offsets from the protocol layout.
        let block = sample_block(14_132_160, B256::ZERO);
        let mut body = block.message.body.as_ssz_bytes();
        replace_ssz_field(
            &mut body,
            &[200, 204, 208, 212, 216, 380, 384, 388, 392],
            3,
            &bytes,
        );
        let mut message = block.message.as_ssz_bytes();
        replace_ssz_field(&mut message, &[80], 0, &body);
        let mut signed = block.as_ssz_bytes();
        replace_ssz_field(&mut signed, &[0], 0, &message);
        let decoded = SignedBeaconBlockElectraSsz::from_ssz_bytes(&signed).unwrap();
        assert_eq!(decoded.message.body.deposits.len(), 1);
        assert_eq!(decoded.message.body.deposits[0], deposit);
        assert_eq!(decoded.as_ssz_bytes(), signed);
        assert!(
            decode_verified_beacon_block(&RawRpcResponse {
                context_bytes: None,
                bytes: signed
            })
            .is_ok()
        );
    }

    fn replace_ssz_field(bytes: &mut Vec<u8>, offsets: &[usize], field: usize, payload: &[u8]) {
        let read_offset = |position| {
            u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize
        };
        let start = read_offset(offsets[field]);
        let end = offsets
            .get(field + 1)
            .map_or(bytes.len(), |offset| read_offset(*offset));
        let new_offsets = offsets
            .iter()
            .skip(field + 1)
            .map(|offset| {
                (
                    *offset,
                    u32::try_from(read_offset(*offset) - (end - start) + payload.len()).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        bytes.splice(start..end, payload.iter().copied());
        for (offset, value) in new_offsets {
            bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
        }
    }

    #[test]
    fn beacon_body_enforces_all_operation_list_limits() {
        let block = sample_block(14_132_160, B256::ZERO);
        let body = block.message.body.as_ssz_bytes();
        let offsets = [200, 204, 208, 212, 216, 380, 384, 388, 392];
        let attestation = AttestationElectraSsz {
            aggregation_bits: BitList::with_capacity(0).unwrap(),
            data: Default::default(),
            signature: FixedBytes::ZERO,
            committee_bits: BitVector::default(),
        };
        macro_rules! check {
            ($index:expr, $limit:expr, $value:expr) => {
                for count in [0, $limit, $limit + 1] {
                    let mut encoded = body.clone();
                    replace_ssz_field(
                        &mut encoded,
                        &offsets,
                        $index,
                        &vec![$value; count].as_ssz_bytes(),
                    );
                    assert_eq!(
                        BeaconBlockBodyElectraSsz::from_ssz_bytes(&encoded).is_ok(),
                        count <= $limit,
                        "field={}, count={count}",
                        $index
                    );
                }
            };
        }
        check!(0, 16, ProposerSlashingSsz::default());
        check!(1, 1, AttesterSlashingElectraSsz::default());
        check!(2, 8, attestation.clone());
        check!(3, 16, DepositSsz::default());
        check!(4, 16, SignedVoluntaryExitSsz::default());
        check!(6, 16, SignedBlsToExecutionChangeSsz::default());
        check!(7, 4096, FixedBytes::<48>::ZERO);
    }

    #[test]
    fn beacon_nested_lists_enforce_limits() {
        let requests = ExecutionRequestsSsz::default().as_ssz_bytes();
        for (field, limit, item) in [
            (0, 8192, DepositRequestSsz::default().as_ssz_bytes()),
            (1, 16, WithdrawalRequestSsz::default().as_ssz_bytes()),
            (2, 2, ConsolidationRequestSsz::default().as_ssz_bytes()),
        ] {
            for count in [0, limit, limit + 1] {
                let mut encoded = requests.clone();
                replace_ssz_field(&mut encoded, &[0, 4, 8], field, &item.repeat(count));
                assert_eq!(
                    ExecutionRequestsSsz::from_ssz_bytes(&encoded).is_ok(),
                    count <= limit
                );
            }
        }
        for count in [0, 131072, 131073] {
            let mut encoded = IndexedAttestationElectraSsz::default().as_ssz_bytes();
            replace_ssz_field(&mut encoded, &[0], 0, &vec![0u64; count].as_ssz_bytes());
            assert_eq!(
                IndexedAttestationElectraSsz::from_ssz_bytes(&encoded).is_ok(),
                count <= 131072
            );
        }
        let payload = sample_block(14_132_160, B256::ZERO)
            .message
            .body
            .execution_payload
            .as_ssz_bytes();
        for count in [0, 16, 17] {
            let mut encoded = payload.clone();
            replace_ssz_field(
                &mut encoded,
                &[436, 504, 508],
                2,
                &vec![WithdrawalSsz::default(); count].as_ssz_bytes(),
            );
            assert_eq!(
                ExecutionPayloadElectraSsz::from_ssz_bytes(&encoded).is_ok(),
                count <= 16
            );
        }
        for count in [0, 1048576, 1048577] {
            let mut encoded = payload.clone();
            replace_ssz_field(
                &mut encoded,
                &[436, 504, 508],
                1,
                &vec![Vec::<u8>::new(); count].as_ssz_bytes(),
            );
            assert_eq!(
                ExecutionPayloadElectraSsz::from_ssz_bytes(&encoded).is_ok(),
                count <= 1048576
            );
        }
    }

    #[test]
    fn beacon_context_matches_electra_fulu_and_blob_schedule_boundaries() {
        for epoch in [364032, 411392, 412672, 419072] {
            for slot in [epoch * 32 - 1, epoch * 32] {
                let block = sample_block(slot, B256::ZERO);
                let current_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(slot);
                let response = RawRpcResponse {
                    context_bytes: Some(
                        MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(current_epoch),
                    ),
                    bytes: block.as_ssz_bytes(),
                };
                assert_eq!(
                    decode_verified_beacon_block(&response).is_ok(),
                    current_epoch >= 364032
                );
                let wrong_epoch = if current_epoch < epoch {
                    epoch
                } else {
                    epoch - 1
                };
                let wrong = RawRpcResponse {
                    context_bytes: Some(
                        MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(wrong_epoch),
                    ),
                    ..response
                };
                assert!(decode_verified_beacon_block(&wrong).is_err());
            }
        }
        let block = sample_block(364032 * 32 - 1, B256::ZERO);
        assert!(
            decode_verified_beacon_block(&RawRpcResponse {
                context_bytes: None,
                bytes: block.as_ssz_bytes()
            })
            .is_err()
        );
    }

    #[test]
    fn beacon_decode_enforces_execution_extra_data_limit() {
        let block = sample_block(14_132_160, B256::ZERO);
        for length in [0, 31, 32, 33] {
            let mut bytes = block.message.body.execution_payload.as_ssz_bytes();
            replace_ssz_field(&mut bytes, &[436, 504, 508], 0, &vec![0x42; length]);
            assert_eq!(
                ExecutionPayloadElectraSsz::from_ssz_bytes(&bytes).is_ok(),
                length <= 32
            );
        }
    }

    #[test]
    fn beacon_decode_rejects_fork_context_from_another_slot() {
        let block = sample_block(14_132_160, B256::ZERO);
        let response = RawRpcResponse {
            context_bytes: Some(MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(364_032)),
            bytes: block.as_ssz_bytes(),
        };
        assert!(decode_verified_beacon_block(&response).is_err());
    }

    #[test]
    fn decodes_and_verifies_electra_beacon_block_payload() {
        let block = sample_block(14_132_160, B256::repeat_byte(0x55));
        let header = block.message.header();
        let response = RawRpcResponse {
            context_bytes: Some(MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(441_630)),
            bytes: block.as_ssz_bytes(),
        };

        let verified = decode_verified_beacon_block(&response).expect("expected verified block");
        assert_eq!(verified.fork, ConsensusDataFork::Electra);
        assert_eq!(verified.beacon_root, header.tree_hash_root());
        assert_eq!(verified.parent_root, B256::repeat_byte(0x55));
        assert_eq!(verified.slot, 14_132_160);
        assert_eq!(verified.execution_anchor.block_number, 12_345);
        assert_eq!(
            verified.execution_anchor.block_hash,
            B256::repeat_byte(0x0f)
        );
        assert_eq!(
            verified.execution_anchor.receipts_root,
            B256::repeat_byte(0x0c)
        );
    }

    #[test]
    fn decodes_current_blob_schedule_fork_digest() {
        let block = sample_block(14_132_160, B256::repeat_byte(0x55));
        let response = RawRpcResponse {
            context_bytes: Some(MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(441_630)),
            bytes: block.as_ssz_bytes(),
        };

        let verified = decode_verified_beacon_block(&response).expect("expected verified block");
        assert_eq!(verified.fork, ConsensusDataFork::Electra);
        assert_eq!(verified.slot, 14_132_160);
    }

    #[test]
    fn decodes_post_electra_beacon_block_without_context() {
        let block = sample_block(14_132_160, B256::repeat_byte(0x55));
        let response = RawRpcResponse {
            context_bytes: None,
            bytes: block.as_ssz_bytes(),
        };

        let verified = decode_verified_beacon_block(&response).expect("expected verified block");
        assert_eq!(verified.fork, ConsensusDataFork::Electra);
        assert_eq!(verified.slot, 14_132_160);
    }

    #[test]
    fn real_fulu_beacon_block_fixture_matches_canonical_root() {
        let response = RawRpcResponse {
            context_bytes: Some(MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(441_630)),
            bytes: include_bytes!("../tests/fixtures/beacon_block_14132042.ssz").to_vec(),
        };

        let signed = SignedBeaconBlockElectraSsz::from_ssz_bytes(&response.bytes)
            .expect("fixture should decode as electra/fulu block");
        assert_eq!(signed.as_ssz_bytes(), response.bytes);
        let header = signed.message.header();

        let verified = decode_verified_beacon_block(&response).expect("expected verified block");

        assert_eq!(verified.slot, 14_132_042);
        assert_eq!(
            header.state_root,
            b256!("0x3e56421c4e4ca7be9701511ed4a5ffe183f42c1f622300e80cece1ea0b205f1b")
        );
        assert_eq!(
            header.body_root,
            b256!("0xda485436980d8bb8136a1cdc93478a9d3744ac1ae70ad7f9d8997efbb924b3d6")
        );
        assert_eq!(
            verified.parent_root,
            b256!("0x19e0f7fcc6cbb74021d1914fdd743c4fa8aa8ec819a9628e0131ace607d29e99")
        );
        assert_eq!(
            verified.beacon_root,
            b256!("0xbb1015ebd50a5861139aaf7761ab2346a822205bdb2d71c66120bdf7378a535d")
        );
        assert_eq!(verified.execution_anchor.beacon_slot, 14_132_042);
    }

    #[test]
    fn contiguous_byte_hash_matches_generic_ssz_at_chunk_boundaries() {
        for length in [0, 1, 31, 32, 33, 55, 56, 63, 64, 65, 1024] {
            let bytes = VariableList::<u8, U1073741824>::new(
                (0..length).map(|index| (index % 251) as u8).collect(),
            )
            .unwrap();
            assert_eq!(
                byte_list_root(&bytes),
                bytes.tree_hash_root(),
                "length={length}"
            );
        }
    }

    #[test]
    #[ignore = "release audit measurement"]
    fn beacon_decode_release_baseline() {
        let response = RawRpcResponse {
            context_bytes: Some(MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(441630)),
            bytes: include_bytes!("../tests/fixtures/beacon_block_14132042.ssz").to_vec(),
        };
        let expected = b256!("bb1015ebd50a5861139aaf7761ab2346a822205bdb2d71c66120bdf7378a535d");
        for _ in 0..10 {
            assert_eq!(
                decode_verified_beacon_block(std::hint::black_box(&response))
                    .unwrap()
                    .beacon_root,
                expected
            );
        }
        for sample in 0..5 {
            let started = std::time::Instant::now();
            for _ in 0..200 {
                assert_eq!(
                    decode_verified_beacon_block(std::hint::black_box(&response))
                        .unwrap()
                        .beacon_root,
                    expected
                );
            }
            println!(
                "{{\"benchmark\":\"beacon_decode\",\"sample\":{sample},\"iterations\":200,\"elapsed_ns\":{}}}",
                started.elapsed().as_nanos()
            );
        }
    }

    #[test]
    fn ssz_list_roots_use_declared_limits_not_current_lengths() {
        use tree_hash::{merkle_root, mix_in_length};
        let roots = vec![B256::repeat_byte(0x42); 3];
        let list = VariableList::<B256, U16>::new(roots.clone()).unwrap();
        let bytes = roots
            .iter()
            .flat_map(|root| root.to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            list.tree_hash_root(),
            mix_in_length(&merkle_root(&bytes, 16), 3)
        );
        assert_ne!(
            list.tree_hash_root(),
            mix_in_length(&merkle_root(&bytes, 3), 3)
        );
        let indices = VariableList::<u64, U131072>::new(vec![1, 2, 3, 4, 5]).unwrap();
        let bytes = indices
            .iter()
            .flat_map(|index| index.to_le_bytes())
            .collect::<Vec<_>>();
        assert_eq!(
            indices.tree_hash_root(),
            mix_in_length(&merkle_root(&bytes, 131072 * 8 / 32), 5)
        );
    }
}
