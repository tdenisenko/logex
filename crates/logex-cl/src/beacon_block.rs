use alloy_primitives::{Address, B256, FixedBytes, U256};
use logex_types::{ConsensusDataFork, ExecutionAnchor};
use ssz::{Decode, DecodeError, Encode};
use ssz_derive::{Decode, Encode};
use thiserror::Error;
use tree_hash::{TreeHash as _, merkle_root, mix_in_length};
use tree_hash_derive::TreeHash;

use crate::{MAINNET_CONSENSUS_CHAIN_SPEC, rpc::RawRpcResponse};

const BLS_PUBKEY_BYTES: usize = 48;
const BLS_SIGNATURE_BYTES: usize = 96;
const KZG_COMMITMENT_BYTES: usize = 48;
const LOGS_BLOOM_BYTES: usize = 256;
const MAX_COMMITTEES_PER_SLOT: usize = 64;
const MAX_VALIDATORS_PER_COMMITTEE: usize = 2_048;
const MAX_ATTESTATION_BITS_ELECTRA: usize =
    MAX_COMMITTEES_PER_SLOT * MAX_VALIDATORS_PER_COMMITTEE;
const MAX_PROPOSER_SLASHINGS: usize = 16;
const MAX_ATTESTER_SLASHINGS_ELECTRA: usize = 1;
const MAX_ATTESTATIONS_ELECTRA: usize = 8;
const MAX_DEPOSITS: usize = 16;
const MAX_VOLUNTARY_EXITS: usize = 16;
const MAX_BLS_TO_EXECUTION_CHANGES: usize = 16;
const MAX_BYTES_PER_TRANSACTION: usize = 1 << 30;
const MAX_TRANSACTIONS_PER_PAYLOAD: usize = 1 << 20;
const MAX_EXTRA_DATA_BYTES: usize = 32;
const MAX_WITHDRAWALS_PER_PAYLOAD: usize = 16;
const MAX_BLOB_COMMITMENTS_PER_BLOCK: usize = 1 << 12;
const MAX_DEPOSIT_REQUESTS_PER_PAYLOAD: usize = 1 << 13;
const MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD: usize = 16;
const MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD: usize = 2;
const DEPOSIT_CONTRACT_TREE_DEPTH: usize = 32;
const DEPOSIT_PROOF_LEN: usize = DEPOSIT_CONTRACT_TREE_DEPTH + 1;
#[derive(Debug, Error)]
pub(crate) enum BeaconBlockError {
    #[error("beacon block response was missing v2 fork context bytes")]
    MissingForkContext,
    #[error("unsupported beacon block fork context 0x{0}")]
    UnsupportedForkContext(String),
    #[error("failed to decode electra/fulu beacon block payload: {0}")]
    DecodeElectra(String),
    #[error(
        "expected beacon block root {expected} at slot {expected_slot}, got {actual} at slot {actual_slot}"
    )]
    UnexpectedBeaconRoot {
        expected: B256,
        expected_slot: u64,
        actual: B256,
        actual_slot: u64,
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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct SszBitlist<const MAX_BITS: usize> {
    bytes: Vec<u8>,
    len_bits: usize,
}

impl<const MAX_BITS: usize> SszBitlist<MAX_BITS> {
    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }

    const fn len_bits(&self) -> usize {
        self.len_bits
    }
}

impl<const MAX_BITS: usize> Encode for SszBitlist<MAX_BITS> {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        let byte_len = bytes_for_bits(self.len_bits.saturating_add(1));
        let mut encoded = self.bytes.clone();
        encoded.resize(byte_len, 0);
        let byte_index = self.len_bits / 8;
        let bit_index = self.len_bits % 8;
        encoded[byte_index] |= 1 << bit_index;
        buf.extend_from_slice(&encoded);
    }

    fn ssz_bytes_len(&self) -> usize {
        bytes_for_bits(self.len_bits.saturating_add(1))
    }
}

impl<const MAX_BITS: usize> Decode for SszBitlist<MAX_BITS> {
    fn is_ssz_fixed_len() -> bool {
        false
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.is_empty() {
            return Err(DecodeError::BytesInvalid(
                "bitlist encoding must contain a length marker".to_owned(),
            ));
        }

        let last = *bytes.last().expect("checked empty above");
        let Some(msb_index) = highest_set_bit(last) else {
            return Err(DecodeError::BytesInvalid(
                "bitlist encoding must contain a length marker".to_owned(),
            ));
        };

        let len_bits = (bytes.len() - 1) * 8 + msb_index;
        if len_bits > MAX_BITS {
            return Err(DecodeError::BytesInvalid(format!(
                "bitlist length {len_bits} exceeded max {MAX_BITS}"
            )));
        }

        let logical_len = bytes_for_bits(len_bits);
        let mut logical_bytes = bytes[..logical_len].to_vec();
        if let Some(last_byte) = logical_bytes.last_mut() {
            *last_byte &= !(1 << msb_index);
        }

        Ok(Self {
            bytes: logical_bytes,
            len_bits,
        })
    }
}

impl<const MAX_BITS: usize> tree_hash::TreeHash for SszBitlist<MAX_BITS> {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        tree_hash::TreeHashType::List
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        unreachable!("bitlists are never packed directly")
    }

    fn tree_hash_packing_factor() -> usize {
        unreachable!("bitlists are never packed directly")
    }

    fn tree_hash_root(&self) -> B256 {
        let root = bitfield_root(self.as_slice(), MAX_BITS);
        mix_in_length(&root, self.len_bits())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct SszBitvector<const BITS: usize> {
    bytes: Vec<u8>,
}

impl<const BITS: usize> SszBitvector<BITS> {
    fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

impl<const BITS: usize> Encode for SszBitvector<BITS> {
    fn is_ssz_fixed_len() -> bool {
        true
    }

    fn ssz_fixed_len() -> usize {
        bytes_for_bits(BITS)
    }

    fn ssz_append(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.bytes);
    }

    fn ssz_bytes_len(&self) -> usize {
        bytes_for_bits(BITS)
    }
}

impl<const BITS: usize> Decode for SszBitvector<BITS> {
    fn is_ssz_fixed_len() -> bool {
        true
    }

    fn ssz_fixed_len() -> usize {
        bytes_for_bits(BITS)
    }

    fn from_ssz_bytes(bytes: &[u8]) -> Result<Self, DecodeError> {
        let expected = bytes_for_bits(BITS);
        if bytes.len() != expected {
            return Err(DecodeError::InvalidByteLength {
                len: bytes.len(),
                expected,
            });
        }

        if BITS % 8 != 0 {
            let unused_mask = !((1u8 << (BITS % 8)) - 1);
            if bytes.last().is_some_and(|last| last & unused_mask != 0) {
                return Err(DecodeError::BytesInvalid(
                    "bitvector had non-zero unused bits".to_owned(),
                ));
            }
        }

        Ok(Self {
            bytes: bytes.to_vec(),
        })
    }
}

impl<const BITS: usize> tree_hash::TreeHash for SszBitvector<BITS> {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        tree_hash::TreeHashType::Vector
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        unreachable!("bitvectors are never packed directly")
    }

    fn tree_hash_packing_factor() -> usize {
        unreachable!("bitvectors are never packed directly")
    }

    fn tree_hash_root(&self) -> B256 {
        bitfield_root(self.as_slice(), BITS)
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode)]
struct DepositSsz {
    proof: Vec<B256>,
    data: DepositDataSsz,
}

impl tree_hash::TreeHash for DepositSsz {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        tree_hash::TreeHashType::Container
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        unreachable!("deposit is a container")
    }

    fn tree_hash_packing_factor() -> usize {
        unreachable!("deposit is a container")
    }

    fn tree_hash_root(&self) -> B256 {
        let proof_roots = self
            .proof
            .iter()
            .map(|proof_item| proof_item.tree_hash_root())
            .collect::<Vec<_>>();
        container_root_from_roots(&[
            vector_root_from_roots(&proof_roots, DEPOSIT_PROOF_LEN),
            self.data.tree_hash_root(),
        ])
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode)]
struct ExecutionRequestsSsz {
    deposits: Vec<DepositRequestSsz>,
    withdrawals: Vec<WithdrawalRequestSsz>,
    consolidations: Vec<ConsolidationRequestSsz>,
}

impl tree_hash::TreeHash for ExecutionRequestsSsz {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        tree_hash::TreeHashType::Container
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        unreachable!("execution requests are a container")
    }

    fn tree_hash_packing_factor() -> usize {
        unreachable!("execution requests are a container")
    }

    fn tree_hash_root(&self) -> B256 {
        container_root_from_roots(&[
            list_root_from_items(&self.deposits, MAX_DEPOSIT_REQUESTS_PER_PAYLOAD),
            list_root_from_items(&self.withdrawals, MAX_WITHDRAWAL_REQUESTS_PER_PAYLOAD),
            list_root_from_items(&self.consolidations, MAX_CONSOLIDATION_REQUESTS_PER_PAYLOAD),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode)]
struct IndexedAttestationElectraSsz {
    attesting_indices: Vec<u64>,
    data: AttestationDataSsz,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
}

impl tree_hash::TreeHash for IndexedAttestationElectraSsz {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        tree_hash::TreeHashType::Container
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        unreachable!("indexed attestation is a container")
    }

    fn tree_hash_packing_factor() -> usize {
        unreachable!("indexed attestation is a container")
    }

    fn tree_hash_root(&self) -> B256 {
        container_root_from_roots(&[
            packed_u64_list_root(&self.attesting_indices, MAX_ATTESTATION_BITS_ELECTRA),
            self.data.tree_hash_root(),
            self.signature.tree_hash_root(),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct AttesterSlashingElectraSsz {
    attestation_1: IndexedAttestationElectraSsz,
    attestation_2: IndexedAttestationElectraSsz,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, TreeHash)]
struct AttestationElectraSsz {
    aggregation_bits: SszBitlist<MAX_ATTESTATION_BITS_ELECTRA>,
    data: AttestationDataSsz,
    signature: FixedBytes<BLS_SIGNATURE_BYTES>,
    committee_bits: SszBitvector<MAX_COMMITTEES_PER_SLOT>,
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
    extra_data: Vec<u8>,
    base_fee_per_gas: U256,
    block_hash: B256,
    transactions: Vec<Vec<u8>>,
    withdrawals: Vec<WithdrawalSsz>,
    blob_gas_used: u64,
    excess_blob_gas: u64,
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
        let transaction_roots = self
            .transactions
            .iter()
            .map(|transaction| bytes_list_root(transaction, MAX_BYTES_PER_TRANSACTION))
            .collect::<Vec<_>>();
        container_root_from_roots(&[
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
            bytes_list_root(&self.extra_data, MAX_EXTRA_DATA_BYTES),
            self.base_fee_per_gas.tree_hash_root(),
            self.block_hash.tree_hash_root(),
            list_root_from_roots(&transaction_roots, MAX_TRANSACTIONS_PER_PAYLOAD),
            list_root_from_items(&self.withdrawals, MAX_WITHDRAWALS_PER_PAYLOAD),
            self.blob_gas_used.tree_hash_root(),
            self.excess_blob_gas.tree_hash_root(),
        ])
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode)]
struct BeaconBlockBodyElectraSsz {
    randao_reveal: FixedBytes<BLS_SIGNATURE_BYTES>,
    eth1_data: Eth1DataSsz,
    graffiti: B256,
    proposer_slashings: Vec<ProposerSlashingSsz>,
    attester_slashings: Vec<AttesterSlashingElectraSsz>,
    attestations: Vec<AttestationElectraSsz>,
    deposits: Vec<DepositSsz>,
    voluntary_exits: Vec<SignedVoluntaryExitSsz>,
    sync_aggregate: SyncAggregateSsz,
    execution_payload: ExecutionPayloadElectraSsz,
    bls_to_execution_changes: Vec<SignedBlsToExecutionChangeSsz>,
    blob_kzg_commitments: Vec<FixedBytes<KZG_COMMITMENT_BYTES>>,
    execution_requests: ExecutionRequestsSsz,
}

impl tree_hash::TreeHash for BeaconBlockBodyElectraSsz {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        tree_hash::TreeHashType::Container
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        unreachable!("beacon block body is a container")
    }

    fn tree_hash_packing_factor() -> usize {
        unreachable!("beacon block body is a container")
    }

    fn tree_hash_root(&self) -> B256 {
        let blob_commitment_roots = self
            .blob_kzg_commitments
            .iter()
            .map(|commitment| commitment.tree_hash_root())
            .collect::<Vec<_>>();
        container_root_from_roots(&[
            self.randao_reveal.tree_hash_root(),
            self.eth1_data.tree_hash_root(),
            self.graffiti.tree_hash_root(),
            list_root_from_items(&self.proposer_slashings, MAX_PROPOSER_SLASHINGS),
            list_root_from_items(&self.attester_slashings, MAX_ATTESTER_SLASHINGS_ELECTRA),
            list_root_from_items(&self.attestations, MAX_ATTESTATIONS_ELECTRA),
            list_root_from_items(&self.deposits, MAX_DEPOSITS),
            list_root_from_items(&self.voluntary_exits, MAX_VOLUNTARY_EXITS),
            self.sync_aggregate.tree_hash_root(),
            self.execution_payload.tree_hash_root(),
            list_root_from_items(&self.bls_to_execution_changes, MAX_BLS_TO_EXECUTION_CHANGES),
            list_root_from_roots(&blob_commitment_roots, MAX_BLOB_COMMITMENTS_PER_BLOCK),
            self.execution_requests.tree_hash_root(),
        ])
    }
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
        return decode_verified_block_for_fork(response, fork);
    }

    let block = decode_electra_block(response)?;
    let epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(block.slot);
    if MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(epoch)[0] >= 0x05 {
        return Ok(block);
    }

    Err(BeaconBlockError::MissingForkContext)
}

pub(crate) fn verify_trusted_beacon_root(
    block: VerifiedBeaconBlock,
    expected_root: B256,
    expected_slot: u64,
) -> Result<VerifiedBeaconBlock, BeaconBlockError> {
    if block.beacon_root != expected_root || block.slot != expected_slot {
        return Err(BeaconBlockError::UnexpectedBeaconRoot {
            expected: expected_root,
            expected_slot,
            actual: block.beacon_root,
            actual_slot: block.slot,
        });
    }
    Ok(block)
}

fn fork_from_context(context_bytes: [u8; 4]) -> Result<ConsensusDataFork, BeaconBlockError> {
    let Some(version) = MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_digest(context_bytes) else {
        let digest = hex_context(context_bytes);
        return Err(BeaconBlockError::UnsupportedForkContext(digest));
    };

    match version[0] {
        0x05.. => Ok(ConsensusDataFork::Electra),
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
            Err(BeaconBlockError::UnsupportedForkContext(hex_context(context)))
        }
    }
}

fn decode_electra_block(response: &RawRpcResponse) -> Result<VerifiedBeaconBlock, BeaconBlockError> {
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

fn highest_set_bit(byte: u8) -> Option<usize> {
    (0..8).rev().find(|bit_index| byte & (1 << bit_index) != 0)
}

const fn bytes_for_bits(bits: usize) -> usize {
    bits.div_ceil(8)
}

fn bitfield_root(bytes: &[u8], max_bits: usize) -> B256 {
    let byte_size = bytes_for_bits(max_bits);
    let minimum_leaves = byte_size.div_ceil(32);
    merkle_root(bytes, minimum_leaves)
}

fn container_root_from_roots(field_roots: &[B256]) -> B256 {
    let mut bytes = Vec::with_capacity(field_roots.len() * 32);
    for root in field_roots {
        bytes.extend_from_slice(root.as_slice());
    }
    merkle_root(&bytes, field_roots.len())
}

fn vector_root_from_roots(roots: &[B256], length: usize) -> B256 {
    let mut bytes = Vec::with_capacity(roots.len() * 32);
    for root in roots {
        bytes.extend_from_slice(root.as_slice());
    }
    merkle_root(&bytes, length.max(roots.len()))
}

fn list_root_from_roots(roots: &[B256], max_len: usize) -> B256 {
    let root = vector_root_from_roots(roots, max_len);
    mix_in_length(&root, roots.len())
}

fn list_root_from_items<T: tree_hash::TreeHash>(items: &[T], max_len: usize) -> B256 {
    let roots = items
        .iter()
        .map(tree_hash::TreeHash::tree_hash_root)
        .collect::<Vec<_>>();
    list_root_from_roots(&roots, max_len)
}

fn bytes_list_root(bytes: &[u8], max_len_bytes: usize) -> B256 {
    let root = merkle_root(bytes, max_len_bytes.div_ceil(32));
    mix_in_length(&root, bytes.len())
}

fn packed_u64_list_root(values: &[u64], max_len: usize) -> B256 {
    let mut bytes = Vec::with_capacity(values.len() * std::mem::size_of::<u64>());
    for value in values {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    let root = merkle_root(&bytes, (max_len * std::mem::size_of::<u64>()).div_ceil(32));
    mix_in_length(&root, values.len())
}

#[cfg(test)]
mod tests {
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
                    proposer_slashings: Vec::new(),
                    attester_slashings: Vec::new(),
                    attestations: Vec::new(),
                    deposits: Vec::new(),
                    voluntary_exits: Vec::new(),
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
                        extra_data: vec![0xde, 0xad, 0xbe, 0xef],
                        base_fee_per_gas: U256::from(100u64),
                        block_hash: B256::repeat_byte(0x0f),
                        transactions: vec![vec![0x01, 0x02, 0x03]],
                        withdrawals: vec![WithdrawalSsz {
                            index: 1,
                            validator_index: 2,
                            address: Address::repeat_byte(0x10),
                            amount: 3,
                        }],
                        blob_gas_used: 4,
                        excess_blob_gas: 5,
                    },
                    bls_to_execution_changes: Vec::new(),
                    blob_kzg_commitments: vec![fixed::<KZG_COMMITMENT_BYTES>(0x11)],
                    execution_requests: ExecutionRequestsSsz::default(),
                },
            },
            signature: fixed::<BLS_SIGNATURE_BYTES>(0x12),
        }
    }

    #[test]
    fn decodes_and_verifies_electra_beacon_block_payload() {
        let block = sample_block(14_132_160, B256::repeat_byte(0x55));
        let header = block.message.header();
        let response = RawRpcResponse {
            context_bytes: Some(MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(411_392)),
            bytes: block.as_ssz_bytes(),
        };

        let verified = decode_verified_beacon_block(&response).expect("expected verified block");
        assert_eq!(verified.fork, ConsensusDataFork::Electra);
        assert_eq!(verified.beacon_root, header.tree_hash_root());
        assert_eq!(verified.parent_root, B256::repeat_byte(0x55));
        assert_eq!(verified.slot, 14_132_160);
        assert_eq!(verified.execution_anchor.block_number, 12_345);
        assert_eq!(verified.execution_anchor.block_hash, B256::repeat_byte(0x0f));
        assert_eq!(verified.execution_anchor.receipts_root, B256::repeat_byte(0x0c));
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
    fn rejects_trusted_beacon_root_mismatch() {
        let block = sample_block(14_132_160, B256::repeat_byte(0x55));
        let response = RawRpcResponse {
            context_bytes: Some(MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(411_392)),
            bytes: block.as_ssz_bytes(),
        };

        let verified = decode_verified_beacon_block(&response).expect("expected verified block");
        let error =
            verify_trusted_beacon_root(verified, B256::repeat_byte(0xaa), 14_132_160).unwrap_err();
        assert!(matches!(
            error,
            BeaconBlockError::UnexpectedBeaconRoot { .. }
        ));
    }

    #[test]
    fn ssz_list_roots_use_declared_limits_not_current_lengths() {
        let roots = vec![
            B256::repeat_byte(0x42),
            B256::repeat_byte(0x43),
            B256::repeat_byte(0x44),
        ];
        let declared_limit_root = list_root_from_roots(&roots, MAX_PROPOSER_SLASHINGS);
        let buggy_current_length_root = list_root_from_roots(&roots, roots.len());
        assert_ne!(declared_limit_root, buggy_current_length_root);

        let indices = vec![1u64, 2, 3, 4, 5];
        let declared_indices_root = packed_u64_list_root(&indices, MAX_ATTESTATION_BITS_ELECTRA);
        let buggy_indices_root = packed_u64_list_root(&indices, indices.len());
        assert_ne!(declared_indices_root, buggy_indices_root);
    }
}
