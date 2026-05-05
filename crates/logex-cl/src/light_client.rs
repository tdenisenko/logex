use std::time::{SystemTime, UNIX_EPOCH};

use alloy_primitives::{Address, B256, FixedBytes, U256};
use alloy_rpc_types_beacon::{BlsPublicKey, BlsSignature};
use blst::BLST_ERROR;
use blst::min_pk::{PublicKey as BlstPublicKey, Signature as BlstSignature};
use logex_types::{
    ConsensusDataFork, ExecutionAnchor, LightClientBootstrapStatus, LightClientExecutionData,
    LightClientFinalityUpdateStatus, LightClientHeaderSummary, LightClientOptimisticUpdateStatus,
    WeakSubjectivityCheckpoint,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use ssz::Decode;
use ssz_derive::{Decode, Encode};
use thiserror::Error;
use tree_hash::{TreeHash as _, merkle_root, mix_in_length};

use crate::MAINNET_CONSENSUS_CHAIN_SPEC;

const SYNC_COMMITTEE_PUBKEYS: usize = 512;
const BLS_PUBKEY_BYTES: usize = 48;
const SYNC_COMMITTEE_BITS_BYTES: usize = 64;
const LOGS_BLOOM_BYTES: usize = 256;
const EXECUTION_BRANCH_DEPTH: usize = 4;
const PRE_ELECTRA_FINALITY_BRANCH_DEPTH: usize = 6;
const PRE_ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH: usize = 5;
const ELECTRA_FINALITY_BRANCH_DEPTH: usize = 7;
const ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH: usize = 6;
const SYNC_COMMITTEE_PUBKEY_BYTES: usize = SYNC_COMMITTEE_PUBKEYS * BLS_PUBKEY_BYTES;
const EPOCHS_PER_SYNC_COMMITTEE_PERIOD: u64 = 256;
const SLOTS_PER_EPOCH: u64 = 32;
const UPDATE_TIMEOUT: u64 = SLOTS_PER_EPOCH * EPOCHS_PER_SYNC_COMMITTEE_PERIOD;
const SYNC_COMMITTEE_DOMAIN: [u8; 4] = [7, 0, 0, 0];
const SYNC_COMMITTEE_SIGNATURE_DST: &[u8] = b"BLS_SIG_BLS12381G2_XMD:SHA-256_SSWU_RO_POP_";
const MIN_SYNC_COMMITTEE_PARTICIPANTS: usize = 1;
const EXECUTION_PAYLOAD_GINDEX: u64 = 25;
const PRE_ELECTRA_FINALIZED_ROOT_GINDEX: u64 = 105;
const PRE_ELECTRA_CURRENT_SYNC_COMMITTEE_GINDEX: u64 = 54;
const PRE_ELECTRA_NEXT_SYNC_COMMITTEE_GINDEX: u64 = 55;
const ELECTRA_FINALIZED_ROOT_GINDEX: u64 = 169;
const ELECTRA_CURRENT_SYNC_COMMITTEE_GINDEX: u64 = 86;
const ELECTRA_NEXT_SYNC_COMMITTEE_GINDEX: u64 = 87;
const CAPELLA_FORK_VERSION: u8 = 0x03;
const DENEB_FORK_VERSION: u8 = 0x04;
const ELECTRA_FORK_VERSION: u8 = 0x05;

type ExecutionBranch = FixedBytes<{ 32 * EXECUTION_BRANCH_DEPTH }>;
type PreElectraFinalityBranch = FixedBytes<{ 32 * PRE_ELECTRA_FINALITY_BRANCH_DEPTH }>;
type PreElectraSyncCommitteeBranch = FixedBytes<{ 32 * PRE_ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH }>;
type ElectraFinalityBranch = FixedBytes<{ 32 * ELECTRA_FINALITY_BRANCH_DEPTH }>;
type ElectraSyncCommitteeBranch = FixedBytes<{ 32 * ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH }>;

#[derive(Debug, Error)]
pub enum LightClientDecodeError {
    #[error("failed to decode {payload_kind} payload: {details}")]
    UnsupportedFork {
        payload_kind: &'static str,
        details: String,
    },
}

#[derive(Debug, Error)]
pub enum LightClientVerificationError {
    #[error(transparent)]
    Decode(#[from] LightClientDecodeError),
    #[error(
        "trusted checkpoint root mismatch: expected {expected}, bootstrap beacon root was {actual}"
    )]
    TrustedCheckpointMismatch { expected: B256, actual: B256 },
    #[error("invalid execution proof for beacon slot {slot}")]
    InvalidExecutionProof { slot: u64 },
    #[error("invalid current sync committee proof for beacon slot {slot}")]
    InvalidCurrentSyncCommitteeProof { slot: u64 },
    #[error(
        "invalid finalized checkpoint proof: attested slot {attested_slot}, finalized slot {finalized_slot}"
    )]
    InvalidFinalityProof {
        attested_slot: u64,
        finalized_slot: u64,
    },
    #[error("sync committee update had no active participants")]
    NoSyncCommitteeParticipants,
    #[error(
        "light-client update slot ordering is invalid: signature={signature_slot} attested={attested_slot} finalized={finalized_slot}"
    )]
    InvalidSlotOrdering {
        signature_slot: u64,
        attested_slot: u64,
        finalized_slot: u64,
    },
    #[error(
        "light-client update signature slot {signature_slot} is ahead of local slot {current_slot}"
    )]
    SignatureFromFuture {
        signature_slot: u64,
        current_slot: u64,
    },
    #[error(
        "light-client update signature period {signature_period} is not known from store period {store_period}"
    )]
    UnknownSyncCommitteePeriod {
        signature_period: u64,
        store_period: u64,
    },
    #[error(
        "light-client update attested slot {attested_slot} is not newer than finalized store slot {store_finalized_slot}"
    )]
    IrrelevantUpdate {
        attested_slot: u64,
        store_finalized_slot: u64,
    },
    #[error("invalid next sync committee proof for attested slot {attested_slot}")]
    InvalidNextSyncCommitteeProof { attested_slot: u64 },
    #[error("finality update did not include a finalized header")]
    MissingFinalizedHeader,
    #[error("invalid sync committee public key at index {index}: {details}")]
    InvalidCommitteePublicKey { index: usize, details: String },
    #[error("invalid sync committee signature: {0}")]
    InvalidSignature(String),
    #[error("sync committee signature verification failed")]
    InvalidSyncCommitteeSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, Serialize, Deserialize)]
pub(crate) struct BeaconBlockHeaderSsz {
    pub(crate) slot: u64,
    pub(crate) proposer_index: u64,
    pub(crate) parent_root: B256,
    pub(crate) state_root: B256,
    pub(crate) body_root: B256,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode)]
struct SyncCommitteeRaw {
    pubkeys: FixedBytes<SYNC_COMMITTEE_PUBKEY_BYTES>,
    aggregate_pubkey: BlsPublicKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub(crate) struct SyncCommitteeData {
    pub(crate) pubkeys: Vec<BlsPublicKey>,
    pub(crate) aggregate_pubkey: BlsPublicKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, Serialize, Deserialize)]
struct SyncAggregateRaw {
    sync_committee_bits: FixedBytes<SYNC_COMMITTEE_BITS_BYTES>,
    sync_committee_signature: BlsSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, Serialize, Deserialize)]
struct ExecutionPayloadHeaderCapella {
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
    transactions_root: B256,
    withdrawals_root: B256,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, Serialize, Deserialize)]
struct ExecutionPayloadHeaderDeneb {
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
    transactions_root: B256,
    withdrawals_root: B256,
    blob_gas_used: u64,
    excess_blob_gas: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, Serialize, Deserialize)]
struct LightClientHeaderCapella {
    beacon: BeaconBlockHeaderSsz,
    execution: ExecutionPayloadHeaderCapella,
    execution_branch: ExecutionBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Encode, Decode, Serialize, Deserialize)]
struct LightClientHeaderDeneb {
    beacon: BeaconBlockHeaderSsz,
    execution: ExecutionPayloadHeaderDeneb,
    execution_branch: ExecutionBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientBootstrapCapella {
    header: LightClientHeaderCapella,
    current_sync_committee: SyncCommitteeRaw,
    current_sync_committee_branch: PreElectraSyncCommitteeBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientBootstrapDeneb {
    header: LightClientHeaderDeneb,
    current_sync_committee: SyncCommitteeRaw,
    current_sync_committee_branch: PreElectraSyncCommitteeBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientBootstrapElectra {
    header: LightClientHeaderDeneb,
    current_sync_committee: SyncCommitteeRaw,
    current_sync_committee_branch: ElectraSyncCommitteeBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientFinalityUpdateCapella {
    attested_header: LightClientHeaderCapella,
    finalized_header: LightClientHeaderCapella,
    finality_branch: PreElectraFinalityBranch,
    sync_aggregate: SyncAggregateRaw,
    signature_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientFinalityUpdateDeneb {
    attested_header: LightClientHeaderDeneb,
    finalized_header: LightClientHeaderDeneb,
    finality_branch: PreElectraFinalityBranch,
    sync_aggregate: SyncAggregateRaw,
    signature_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientFinalityUpdateElectra {
    attested_header: LightClientHeaderDeneb,
    finalized_header: LightClientHeaderDeneb,
    finality_branch: ElectraFinalityBranch,
    sync_aggregate: SyncAggregateRaw,
    signature_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientOptimisticUpdateCapella {
    attested_header: LightClientHeaderCapella,
    sync_aggregate: SyncAggregateRaw,
    signature_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientOptimisticUpdateDeneb {
    attested_header: LightClientHeaderDeneb,
    sync_aggregate: SyncAggregateRaw,
    signature_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientUpdateCapella {
    attested_header: LightClientHeaderCapella,
    next_sync_committee: SyncCommitteeRaw,
    next_sync_committee_branch: PreElectraSyncCommitteeBranch,
    finalized_header: LightClientHeaderCapella,
    finality_branch: PreElectraFinalityBranch,
    sync_aggregate: SyncAggregateRaw,
    signature_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientUpdateDeneb {
    attested_header: LightClientHeaderDeneb,
    next_sync_committee: SyncCommitteeRaw,
    next_sync_committee_branch: PreElectraSyncCommitteeBranch,
    finalized_header: LightClientHeaderDeneb,
    finality_branch: PreElectraFinalityBranch,
    sync_aggregate: SyncAggregateRaw,
    signature_slot: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientUpdateElectra {
    attested_header: LightClientHeaderDeneb,
    next_sync_committee: SyncCommitteeRaw,
    next_sync_committee_branch: ElectraSyncCommitteeBranch,
    finalized_header: LightClientHeaderDeneb,
    finality_branch: ElectraFinalityBranch,
    sync_aggregate: SyncAggregateRaw,
    signature_slot: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VerifiedExecutionPayloadHeader {
    pub block_number: u64,
    pub block_hash: B256,
    pub receipts_root: B256,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VerifiedLightClientHeader {
    pub fork: ConsensusDataFork,
    pub beacon: BeaconBlockHeaderSsz,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub execution: Option<VerifiedExecutionPayloadHeader>,
}

impl VerifiedLightClientHeader {
    pub(crate) fn beacon_root(&self) -> B256 {
        beacon_block_header_root(&self.beacon)
    }

    pub(crate) fn execution_anchor(&self) -> Option<ExecutionAnchor> {
        self.execution.map(|execution| ExecutionAnchor {
            beacon_root: self.beacon_root(),
            beacon_slot: self.beacon.slot,
            block_number: execution.block_number,
            block_hash: execution.block_hash,
            receipts_root: execution.receipts_root,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VerifiedLightClientStore {
    pub checkpoint_root: B256,
    #[serde(default)]
    pub bootstrap_slot: u64,
    pub current_sync_committee: SyncCommitteeData,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_sync_committee: Option<SyncCommitteeData>,
    pub finalized_header: VerifiedLightClientHeader,
    pub optimistic_header: VerifiedLightClientHeader,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) best_valid_update: Option<VerifiedLightClientUpdate>,
    #[serde(default)]
    pub previous_max_active_participants: usize,
    #[serde(default)]
    pub current_max_active_participants: usize,
}

impl VerifiedLightClientStore {
    pub(crate) fn bootstrap_slot(&self) -> u64 {
        self.bootstrap_slot
    }

    pub(crate) fn finalized_anchor(&self) -> Option<ExecutionAnchor> {
        self.finalized_header.execution_anchor()
    }

    pub(crate) fn optimistic_anchor(&self) -> Option<ExecutionAnchor> {
        self.optimistic_header.execution_anchor()
    }
}

#[derive(Debug, Clone)]
enum DecodedBootstrap {
    Capella(LightClientBootstrapCapella),
    Deneb(LightClientBootstrapDeneb),
    Electra(LightClientBootstrapElectra),
}

#[derive(Debug, Clone)]
enum DecodedFinalityUpdate {
    Capella(LightClientFinalityUpdateCapella),
    Deneb(LightClientFinalityUpdateDeneb),
    Electra(LightClientFinalityUpdateElectra),
}

#[derive(Debug, Clone)]
enum DecodedOptimisticUpdate {
    Capella(LightClientOptimisticUpdateCapella),
    Deneb(LightClientOptimisticUpdateDeneb),
}

#[derive(Debug, Clone)]
enum DecodedUpdate {
    Capella(LightClientUpdateCapella),
    Deneb(LightClientUpdateDeneb),
    Electra(LightClientUpdateElectra),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VerifiedLightClientUpdate {
    attested_header: VerifiedLightClientHeader,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    finalized_header: Option<VerifiedLightClientHeader>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    next_sync_committee: Option<SyncCommitteeData>,
    signature_slot: u64,
    participants: usize,
}

impl VerifiedLightClientUpdate {
    fn finalized_slot(&self) -> u64 {
        self.finalized_header
            .as_ref()
            .map(|header| header.beacon.slot)
            .unwrap_or(0)
    }

    fn has_supermajority(&self) -> bool {
        self.participants * 3 >= SYNC_COMMITTEE_PUBKEYS * 2
    }

    fn has_relevant_sync_committee(&self) -> bool {
        self.next_sync_committee.is_some()
            && sync_committee_period_at_slot(self.attested_header.beacon.slot)
                == sync_committee_period_at_slot(self.signature_slot)
    }

    fn has_sync_committee_finality(&self) -> bool {
        self.finalized_header
            .as_ref()
            .is_some_and(|finalized_header| {
                sync_committee_period_at_slot(finalized_header.beacon.slot)
                    == sync_committee_period_at_slot(self.attested_header.beacon.slot)
            })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AppliedLightClientUpdate {
    pub(crate) store: VerifiedLightClientStore,
    pub(crate) optimistic_status: LightClientOptimisticUpdateStatus,
    pub(crate) finality_status: Option<LightClientFinalityUpdateStatus>,
}

pub fn decode_bootstrap(
    bytes: &[u8],
) -> Result<LightClientBootstrapStatus, LightClientDecodeError> {
    Ok(decode_bootstrap_payload(bytes)?.status())
}

pub fn decode_finality_update(
    bytes: &[u8],
) -> Result<LightClientFinalityUpdateStatus, LightClientDecodeError> {
    Ok(decode_finality_update_payload(bytes)?.status())
}

pub fn decode_optimistic_update(
    bytes: &[u8],
) -> Result<LightClientOptimisticUpdateStatus, LightClientDecodeError> {
    Ok(decode_optimistic_update_payload(bytes)?.status())
}

pub(crate) fn apply_light_client_update_payload(
    bytes: &[u8],
    store: &VerifiedLightClientStore,
) -> Result<AppliedLightClientUpdate, LightClientVerificationError> {
    let decoded = decode_update_payload(bytes)?;
    let update = verify_valid_light_client_update(
        store,
        decoded.verified_update()?,
        decoded.finality_branch_for_verification(),
        decoded.next_sync_committee_branch(),
        decoded.sync_aggregate(),
    )?;
    process_light_client_update(store, update)
}

pub(crate) fn verify_bootstrap_payload(
    bytes: &[u8],
    checkpoint: WeakSubjectivityCheckpoint,
) -> Result<(LightClientBootstrapStatus, VerifiedLightClientStore), LightClientVerificationError> {
    let decoded = decode_bootstrap_payload(bytes)?;
    let header = decoded.verified_header()?;
    let beacon_root = header.beacon_root();
    if beacon_root != checkpoint.beacon_root {
        return Err(LightClientVerificationError::TrustedCheckpointMismatch {
            expected: checkpoint.beacon_root,
            actual: beacon_root,
        });
    }

    let slot = header.beacon.slot;
    let leaf = decoded.current_sync_committee().tree_hash_root();
    let branch = decoded.current_sync_committee_branch();
    let gindex = current_sync_committee_gindex_at_slot(slot);
    if !is_valid_normalized_merkle_branch(leaf, branch, gindex, header.beacon.state_root) {
        return Err(LightClientVerificationError::InvalidCurrentSyncCommitteeProof { slot });
    }

    let status = decoded.status();
    let store = VerifiedLightClientStore {
        checkpoint_root: checkpoint.beacon_root,
        bootstrap_slot: checkpoint.beacon_slot.unwrap_or(slot),
        current_sync_committee: decoded.current_sync_committee().to_persisted(),
        next_sync_committee: None,
        finalized_header: header.clone(),
        optimistic_header: header,
        best_valid_update: None,
        previous_max_active_participants: 0,
        current_max_active_participants: 0,
    };
    Ok((status, store))
}

pub(crate) fn apply_finality_update_payload(
    bytes: &[u8],
    store: &VerifiedLightClientStore,
) -> Result<
    (
        LightClientFinalityUpdateStatus,
        VerifiedLightClientStore,
        VerifiedLightClientHeader,
        VerifiedLightClientHeader,
    ),
    LightClientVerificationError,
> {
    let decoded = decode_finality_update_payload(bytes)?;
    let attested_header = decoded.attested_verified_header()?;
    let finalized_header = decoded.finalized_verified_header()?;
    let update = VerifiedLightClientUpdate {
        attested_header: attested_header.clone(),
        finalized_header: Some(finalized_header.clone()),
        next_sync_committee: None,
        signature_slot: decoded.signature_slot(),
        participants: participant_count(decoded.sync_aggregate()),
    };
    let applied = process_light_client_update(
        store,
        verify_valid_light_client_update(
            store,
            update,
            Some(decoded.finality_branch()),
            None,
            decoded.sync_aggregate(),
        )?,
    )?;
    Ok((
        applied
            .finality_status
            .expect("finality wrapper should always yield a finality status"),
        applied.store,
        attested_header,
        finalized_header,
    ))
}

pub(crate) fn apply_optimistic_update_payload(
    bytes: &[u8],
    store: &VerifiedLightClientStore,
) -> Result<
    (
        LightClientOptimisticUpdateStatus,
        VerifiedLightClientStore,
        VerifiedLightClientHeader,
    ),
    LightClientVerificationError,
> {
    let decoded = decode_optimistic_update_payload(bytes)?;
    let attested_header = decoded.attested_verified_header()?;
    let update = VerifiedLightClientUpdate {
        attested_header: attested_header.clone(),
        finalized_header: None,
        next_sync_committee: None,
        signature_slot: decoded.signature_slot(),
        participants: participant_count(decoded.sync_aggregate()),
    };
    let applied = process_light_client_update(
        store,
        verify_valid_light_client_update(store, update, None, None, decoded.sync_aggregate())?,
    )?;
    Ok((applied.optimistic_status, applied.store, attested_header))
}

impl DecodedBootstrap {
    fn status(&self) -> LightClientBootstrapStatus {
        match self {
            Self::Capella(payload) => LightClientBootstrapStatus {
                fork: ConsensusDataFork::Capella,
                header: header_summary_capella(&payload.header),
                current_sync_committee_pubkeys: SYNC_COMMITTEE_PUBKEYS,
                current_sync_committee_branch_depth: PRE_ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH,
            },
            Self::Deneb(payload) => LightClientBootstrapStatus {
                fork: ConsensusDataFork::Deneb,
                header: header_summary_deneb(&payload.header),
                current_sync_committee_pubkeys: SYNC_COMMITTEE_PUBKEYS,
                current_sync_committee_branch_depth: PRE_ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH,
            },
            Self::Electra(payload) => LightClientBootstrapStatus {
                fork: ConsensusDataFork::Electra,
                header: header_summary_deneb(&payload.header),
                current_sync_committee_pubkeys: SYNC_COMMITTEE_PUBKEYS,
                current_sync_committee_branch_depth: ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH,
            },
        }
    }

    fn verified_header(&self) -> Result<VerifiedLightClientHeader, LightClientVerificationError> {
        match self {
            Self::Capella(payload) => verify_capella_header(&payload.header),
            Self::Deneb(payload) => verify_deneb_header(&payload.header, ConsensusDataFork::Deneb),
            Self::Electra(payload) => {
                verify_deneb_header(&payload.header, ConsensusDataFork::Electra)
            }
        }
    }

    fn current_sync_committee(&self) -> &SyncCommitteeRaw {
        match self {
            Self::Capella(payload) => &payload.current_sync_committee,
            Self::Deneb(payload) => &payload.current_sync_committee,
            Self::Electra(payload) => &payload.current_sync_committee,
        }
    }

    fn current_sync_committee_branch(&self) -> Vec<B256> {
        match self {
            Self::Capella(payload) => branch_from_fixed(&payload.current_sync_committee_branch),
            Self::Deneb(payload) => branch_from_fixed(&payload.current_sync_committee_branch),
            Self::Electra(payload) => branch_from_fixed(&payload.current_sync_committee_branch),
        }
    }
}

impl DecodedFinalityUpdate {
    fn status(&self) -> LightClientFinalityUpdateStatus {
        match self {
            Self::Capella(payload) => LightClientFinalityUpdateStatus {
                fork: ConsensusDataFork::Capella,
                attested_header: header_summary_capella(&payload.attested_header),
                finalized_header: header_summary_capella(&payload.finalized_header),
                signature_slot: payload.signature_slot,
                sync_committee_participants: participant_count(&payload.sync_aggregate),
                finality_branch_depth: PRE_ELECTRA_FINALITY_BRANCH_DEPTH,
            },
            Self::Deneb(payload) => LightClientFinalityUpdateStatus {
                fork: ConsensusDataFork::Deneb,
                attested_header: header_summary_deneb(&payload.attested_header),
                finalized_header: header_summary_deneb(&payload.finalized_header),
                signature_slot: payload.signature_slot,
                sync_committee_participants: participant_count(&payload.sync_aggregate),
                finality_branch_depth: PRE_ELECTRA_FINALITY_BRANCH_DEPTH,
            },
            Self::Electra(payload) => LightClientFinalityUpdateStatus {
                fork: ConsensusDataFork::Electra,
                attested_header: header_summary_deneb(&payload.attested_header),
                finalized_header: header_summary_deneb(&payload.finalized_header),
                signature_slot: payload.signature_slot,
                sync_committee_participants: participant_count(&payload.sync_aggregate),
                finality_branch_depth: ELECTRA_FINALITY_BRANCH_DEPTH,
            },
        }
    }

    fn attested_verified_header(
        &self,
    ) -> Result<VerifiedLightClientHeader, LightClientVerificationError> {
        match self {
            Self::Capella(payload) => verify_capella_header(&payload.attested_header),
            Self::Deneb(payload) => {
                verify_deneb_header(&payload.attested_header, ConsensusDataFork::Deneb)
            }
            Self::Electra(payload) => {
                verify_deneb_header(&payload.attested_header, ConsensusDataFork::Electra)
            }
        }
    }

    fn finalized_verified_header(
        &self,
    ) -> Result<VerifiedLightClientHeader, LightClientVerificationError> {
        match self {
            Self::Capella(payload) => verify_capella_header(&payload.finalized_header),
            Self::Deneb(payload) => {
                verify_deneb_header(&payload.finalized_header, ConsensusDataFork::Deneb)
            }
            Self::Electra(payload) => {
                verify_deneb_header(&payload.finalized_header, ConsensusDataFork::Electra)
            }
        }
    }

    fn sync_aggregate(&self) -> &SyncAggregateRaw {
        match self {
            Self::Capella(payload) => &payload.sync_aggregate,
            Self::Deneb(payload) => &payload.sync_aggregate,
            Self::Electra(payload) => &payload.sync_aggregate,
        }
    }

    fn finality_branch(&self) -> Vec<B256> {
        match self {
            Self::Capella(payload) => branch_from_fixed(&payload.finality_branch),
            Self::Deneb(payload) => branch_from_fixed(&payload.finality_branch),
            Self::Electra(payload) => branch_from_fixed(&payload.finality_branch),
        }
    }

    fn signature_slot(&self) -> u64 {
        match self {
            Self::Capella(payload) => payload.signature_slot,
            Self::Deneb(payload) => payload.signature_slot,
            Self::Electra(payload) => payload.signature_slot,
        }
    }
}

impl DecodedOptimisticUpdate {
    fn status(&self) -> LightClientOptimisticUpdateStatus {
        match self {
            Self::Capella(payload) => LightClientOptimisticUpdateStatus {
                fork: ConsensusDataFork::Capella,
                attested_header: header_summary_capella(&payload.attested_header),
                signature_slot: payload.signature_slot,
                sync_committee_participants: participant_count(&payload.sync_aggregate),
            },
            Self::Deneb(payload) => LightClientOptimisticUpdateStatus {
                fork: fork_for_slot(payload.attested_header.beacon.slot),
                attested_header: header_summary_deneb(&payload.attested_header),
                signature_slot: payload.signature_slot,
                sync_committee_participants: participant_count(&payload.sync_aggregate),
            },
        }
    }

    fn attested_verified_header(
        &self,
    ) -> Result<VerifiedLightClientHeader, LightClientVerificationError> {
        match self {
            Self::Capella(payload) => verify_capella_header(&payload.attested_header),
            Self::Deneb(payload) => verify_deneb_header(
                &payload.attested_header,
                fork_for_slot(payload.attested_header.beacon.slot),
            ),
        }
    }

    fn sync_aggregate(&self) -> &SyncAggregateRaw {
        match self {
            Self::Capella(payload) => &payload.sync_aggregate,
            Self::Deneb(payload) => &payload.sync_aggregate,
        }
    }

    fn signature_slot(&self) -> u64 {
        match self {
            Self::Capella(payload) => payload.signature_slot,
            Self::Deneb(payload) => payload.signature_slot,
        }
    }
}

impl DecodedUpdate {
    fn verified_update(&self) -> Result<VerifiedLightClientUpdate, LightClientVerificationError> {
        match self {
            Self::Capella(payload) => Ok(VerifiedLightClientUpdate {
                attested_header: verify_capella_header(&payload.attested_header)?,
                finalized_header: verified_optional_capella_header(&payload.finalized_header)?,
                next_sync_committee: verified_optional_sync_committee(
                    &payload.next_sync_committee,
                    &payload.next_sync_committee_branch,
                )?,
                signature_slot: payload.signature_slot,
                participants: participant_count(&payload.sync_aggregate),
            }),
            Self::Deneb(payload) => Ok(VerifiedLightClientUpdate {
                attested_header: verify_deneb_header(
                    &payload.attested_header,
                    fork_for_slot(payload.attested_header.beacon.slot),
                )?,
                finalized_header: verified_optional_deneb_header(&payload.finalized_header)?,
                next_sync_committee: verified_optional_sync_committee(
                    &payload.next_sync_committee,
                    &payload.next_sync_committee_branch,
                )?,
                signature_slot: payload.signature_slot,
                participants: participant_count(&payload.sync_aggregate),
            }),
            Self::Electra(payload) => Ok(VerifiedLightClientUpdate {
                attested_header: verify_deneb_header(
                    &payload.attested_header,
                    ConsensusDataFork::Electra,
                )?,
                finalized_header: verified_optional_deneb_header(&payload.finalized_header)?,
                next_sync_committee: verified_optional_sync_committee(
                    &payload.next_sync_committee,
                    &payload.next_sync_committee_branch,
                )?,
                signature_slot: payload.signature_slot,
                participants: participant_count(&payload.sync_aggregate),
            }),
        }
    }

    fn sync_aggregate(&self) -> &SyncAggregateRaw {
        match self {
            Self::Capella(payload) => &payload.sync_aggregate,
            Self::Deneb(payload) => &payload.sync_aggregate,
            Self::Electra(payload) => &payload.sync_aggregate,
        }
    }

    fn finality_branch(&self) -> Vec<B256> {
        match self {
            Self::Capella(payload) => branch_from_fixed(&payload.finality_branch),
            Self::Deneb(payload) => branch_from_fixed(&payload.finality_branch),
            Self::Electra(payload) => branch_from_fixed(&payload.finality_branch),
        }
    }

    fn finality_branch_for_verification(&self) -> Option<Vec<B256>> {
        let branch = self.finality_branch();
        let has_verified_finalized_header = match self {
            Self::Capella(payload) => {
                payload.finalized_header != LightClientHeaderCapella::default()
            }
            Self::Deneb(payload) => payload.finalized_header != LightClientHeaderDeneb::default(),
            Self::Electra(payload) => payload.finalized_header != LightClientHeaderDeneb::default(),
        };
        (has_verified_finalized_header || branch.iter().any(|item| *item != B256::ZERO))
            .then_some(branch)
    }

    fn next_sync_committee_branch(&self) -> Option<Vec<B256>> {
        match self {
            Self::Capella(payload)
                if payload.next_sync_committee != SyncCommitteeRaw::default() =>
            {
                Some(branch_from_fixed(&payload.next_sync_committee_branch))
            }
            Self::Deneb(payload) if payload.next_sync_committee != SyncCommitteeRaw::default() => {
                Some(branch_from_fixed(&payload.next_sync_committee_branch))
            }
            Self::Electra(payload)
                if payload.next_sync_committee != SyncCommitteeRaw::default() =>
            {
                Some(branch_from_fixed(&payload.next_sync_committee_branch))
            }
            _ => None,
        }
    }
}

impl tree_hash::TreeHash for SyncCommitteeRaw {
    fn tree_hash_type() -> tree_hash::TreeHashType {
        tree_hash::TreeHashType::Container
    }

    fn tree_hash_packed_encoding(&self) -> tree_hash::PackedEncoding {
        unreachable!("sync committee is a container")
    }

    fn tree_hash_packing_factor() -> usize {
        unreachable!("sync committee is a container")
    }

    fn tree_hash_root(&self) -> B256 {
        let pubkeys_root = sync_committee_pubkeys_root_from_bytes(&self.pubkeys);
        let aggregate_root = self.aggregate_pubkey.tree_hash_root();
        container_root_from_roots(&[pubkeys_root, aggregate_root])
    }
}

fn decode_bootstrap_payload(bytes: &[u8]) -> Result<DecodedBootstrap, LightClientDecodeError> {
    if let Ok(payload) = LightClientBootstrapElectra::from_ssz_bytes(bytes) {
        return Ok(DecodedBootstrap::Electra(payload));
    }
    if let Ok(payload) = LightClientBootstrapDeneb::from_ssz_bytes(bytes) {
        return Ok(DecodedBootstrap::Deneb(payload));
    }
    if let Ok(payload) = LightClientBootstrapCapella::from_ssz_bytes(bytes) {
        return Ok(DecodedBootstrap::Capella(payload));
    }

    Err(LightClientDecodeError::UnsupportedFork {
        payload_kind: "light-client bootstrap",
        details: candidate_failures(
            bytes,
            &[
                (
                    "electra",
                    LightClientBootstrapElectra::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
                (
                    "deneb",
                    LightClientBootstrapDeneb::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
                (
                    "capella",
                    LightClientBootstrapCapella::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
            ],
        ),
    })
}

fn decode_finality_update_payload(
    bytes: &[u8],
) -> Result<DecodedFinalityUpdate, LightClientDecodeError> {
    if let Ok(payload) = LightClientFinalityUpdateElectra::from_ssz_bytes(bytes) {
        return Ok(DecodedFinalityUpdate::Electra(payload));
    }
    if let Ok(payload) = LightClientFinalityUpdateDeneb::from_ssz_bytes(bytes) {
        return Ok(DecodedFinalityUpdate::Deneb(payload));
    }
    if let Ok(payload) = LightClientFinalityUpdateCapella::from_ssz_bytes(bytes) {
        return Ok(DecodedFinalityUpdate::Capella(payload));
    }

    Err(LightClientDecodeError::UnsupportedFork {
        payload_kind: "light-client finality update",
        details: candidate_failures(
            bytes,
            &[
                (
                    "electra",
                    LightClientFinalityUpdateElectra::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
                (
                    "deneb",
                    LightClientFinalityUpdateDeneb::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
                (
                    "capella",
                    LightClientFinalityUpdateCapella::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
            ],
        ),
    })
}

fn decode_optimistic_update_payload(
    bytes: &[u8],
) -> Result<DecodedOptimisticUpdate, LightClientDecodeError> {
    if let Ok(payload) = LightClientOptimisticUpdateDeneb::from_ssz_bytes(bytes) {
        return Ok(DecodedOptimisticUpdate::Deneb(payload));
    }
    if let Ok(payload) = LightClientOptimisticUpdateCapella::from_ssz_bytes(bytes) {
        return Ok(DecodedOptimisticUpdate::Capella(payload));
    }

    Err(LightClientDecodeError::UnsupportedFork {
        payload_kind: "light-client optimistic update",
        details: candidate_failures(
            bytes,
            &[
                (
                    "deneb_or_electra",
                    LightClientOptimisticUpdateDeneb::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
                (
                    "capella",
                    LightClientOptimisticUpdateCapella::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
            ],
        ),
    })
}

fn decode_update_payload(bytes: &[u8]) -> Result<DecodedUpdate, LightClientDecodeError> {
    if let Ok(payload) = LightClientUpdateElectra::from_ssz_bytes(bytes) {
        return Ok(DecodedUpdate::Electra(payload));
    }
    if let Ok(payload) = LightClientUpdateDeneb::from_ssz_bytes(bytes) {
        return Ok(DecodedUpdate::Deneb(payload));
    }
    if let Ok(payload) = LightClientUpdateCapella::from_ssz_bytes(bytes) {
        return Ok(DecodedUpdate::Capella(payload));
    }

    Err(LightClientDecodeError::UnsupportedFork {
        payload_kind: "light-client update",
        details: candidate_failures(
            bytes,
            &[
                (
                    "electra",
                    LightClientUpdateElectra::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
                (
                    "deneb",
                    LightClientUpdateDeneb::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
                (
                    "capella",
                    LightClientUpdateCapella::from_ssz_bytes(bytes)
                        .err()
                        .map(|error| format!("{error:?}")),
                ),
            ],
        ),
    })
}

fn verify_capella_header(
    header: &LightClientHeaderCapella,
) -> Result<VerifiedLightClientHeader, LightClientVerificationError> {
    let execution_root = execution_payload_header_capella_root(&header.execution);
    let branch = branch_from_fixed(&header.execution_branch);
    if !is_valid_merkle_branch(
        execution_root,
        &branch,
        EXECUTION_BRANCH_DEPTH,
        subtree_index(EXECUTION_PAYLOAD_GINDEX),
        header.beacon.body_root,
    ) {
        return Err(LightClientVerificationError::InvalidExecutionProof {
            slot: header.beacon.slot,
        });
    }

    Ok(VerifiedLightClientHeader {
        fork: ConsensusDataFork::Capella,
        beacon: header.beacon.clone(),
        execution: execution_data_capella(&header.execution).map(|execution| {
            VerifiedExecutionPayloadHeader {
                block_number: execution.block_number,
                block_hash: execution.block_hash,
                receipts_root: execution.receipts_root,
            }
        }),
    })
}

fn verify_deneb_header(
    header: &LightClientHeaderDeneb,
    fork: ConsensusDataFork,
) -> Result<VerifiedLightClientHeader, LightClientVerificationError> {
    let execution_root = execution_payload_header_deneb_root(&header.execution);
    let branch = branch_from_fixed(&header.execution_branch);
    if !is_valid_merkle_branch(
        execution_root,
        &branch,
        EXECUTION_BRANCH_DEPTH,
        subtree_index(EXECUTION_PAYLOAD_GINDEX),
        header.beacon.body_root,
    ) {
        return Err(LightClientVerificationError::InvalidExecutionProof {
            slot: header.beacon.slot,
        });
    }

    Ok(VerifiedLightClientHeader {
        fork,
        beacon: header.beacon.clone(),
        execution: execution_data_deneb(&header.execution).map(|execution| {
            VerifiedExecutionPayloadHeader {
                block_number: execution.block_number,
                block_hash: execution.block_hash,
                receipts_root: execution.receipts_root,
            }
        }),
    })
}

fn verified_optional_capella_header(
    header: &LightClientHeaderCapella,
) -> Result<Option<VerifiedLightClientHeader>, LightClientVerificationError> {
    if header == &LightClientHeaderCapella::default() {
        Ok(None)
    } else {
        verify_capella_header(header).map(Some)
    }
}

fn verified_optional_deneb_header(
    header: &LightClientHeaderDeneb,
) -> Result<Option<VerifiedLightClientHeader>, LightClientVerificationError> {
    if header == &LightClientHeaderDeneb::default() {
        Ok(None)
    } else {
        verify_deneb_header(header, fork_for_slot(header.beacon.slot)).map(Some)
    }
}

fn verified_optional_sync_committee<const N: usize>(
    committee: &SyncCommitteeRaw,
    branch: &FixedBytes<N>,
) -> Result<Option<SyncCommitteeData>, LightClientVerificationError> {
    if committee == &SyncCommitteeRaw::default() {
        if branch_has_nonzero(branch.as_slice()) {
            return Err(
                LightClientVerificationError::InvalidNextSyncCommitteeProof { attested_slot: 0 },
            );
        }
        Ok(None)
    } else {
        Ok(Some(committee.to_persisted()))
    }
}

fn verify_valid_light_client_update(
    store: &VerifiedLightClientStore,
    update: VerifiedLightClientUpdate,
    finality_branch: Option<Vec<B256>>,
    next_sync_committee_branch: Option<Vec<B256>>,
    sync_aggregate: &SyncAggregateRaw,
) -> Result<VerifiedLightClientUpdate, LightClientVerificationError> {
    if update.participants < MIN_SYNC_COMMITTEE_PARTICIPANTS {
        return Err(LightClientVerificationError::NoSyncCommitteeParticipants);
    }

    let finalized_slot = update
        .finalized_header
        .as_ref()
        .map(|header| header.beacon.slot)
        .unwrap_or(0);
    let attested_slot = update.attested_header.beacon.slot;
    let current_slot = wall_clock_slot();
    if update.signature_slot > current_slot {
        return Err(LightClientVerificationError::SignatureFromFuture {
            signature_slot: update.signature_slot,
            current_slot,
        });
    }
    if update.signature_slot <= attested_slot || attested_slot < finalized_slot {
        return Err(LightClientVerificationError::InvalidSlotOrdering {
            signature_slot: update.signature_slot,
            attested_slot,
            finalized_slot,
        });
    }

    let store_period = sync_committee_period_at_slot(store.finalized_header.beacon.slot);
    let signature_period = sync_committee_period_at_slot(update.signature_slot);
    let committee = if signature_period == store_period {
        &store.current_sync_committee
    } else if signature_period == store_period + 1 {
        store.next_sync_committee.as_ref().ok_or(
            LightClientVerificationError::UnknownSyncCommitteePeriod {
                signature_period,
                store_period,
            },
        )?
    } else {
        return Err(LightClientVerificationError::UnknownSyncCommitteePeriod {
            signature_period,
            store_period,
        });
    };

    let attested_period = sync_committee_period_at_slot(attested_slot);
    let update_has_next_sync_committee = store.next_sync_committee.is_none()
        && update.next_sync_committee.is_some()
        && attested_period == store_period;
    if attested_slot <= store.finalized_header.beacon.slot && !update_has_next_sync_committee {
        return Err(LightClientVerificationError::IrrelevantUpdate {
            attested_slot,
            store_finalized_slot: store.finalized_header.beacon.slot,
        });
    }

    if let Some(finality_branch) = finality_branch {
        let finalized_root = update
            .finalized_header
            .as_ref()
            .map(VerifiedLightClientHeader::beacon_root)
            .unwrap_or(B256::ZERO);
        if !is_valid_normalized_merkle_branch(
            finalized_root,
            finality_branch,
            finalized_root_gindex_at_slot(attested_slot),
            update.attested_header.beacon.state_root,
        ) {
            return Err(LightClientVerificationError::InvalidFinalityProof {
                attested_slot,
                finalized_slot,
            });
        }
    }

    if let Some(next_sync_committee) = &update.next_sync_committee {
        if attested_period == store_period
            && let Some(known_next_sync_committee) = &store.next_sync_committee
            && next_sync_committee != known_next_sync_committee
        {
            return Err(
                LightClientVerificationError::InvalidNextSyncCommitteeProof { attested_slot },
            );
        }
        let Some(next_sync_committee_branch) = next_sync_committee_branch else {
            return Err(
                LightClientVerificationError::InvalidNextSyncCommitteeProof { attested_slot },
            );
        };
        if !is_valid_normalized_merkle_branch(
            sync_committee_tree_hash_root(next_sync_committee),
            next_sync_committee_branch,
            next_sync_committee_gindex_at_slot(attested_slot),
            update.attested_header.beacon.state_root,
        ) {
            return Err(
                LightClientVerificationError::InvalidNextSyncCommitteeProof { attested_slot },
            );
        }
    }

    let participant_pubkeys =
        committee.participant_public_keys(&sync_aggregate.sync_committee_bits)?;
    let signature =
        BlstSignature::sig_validate(sync_aggregate.sync_committee_signature.as_slice(), false)
            .map_err(|error| {
                LightClientVerificationError::InvalidSignature(format!("{error:?}"))
            })?;

    let message = compute_signing_root(
        beacon_block_header_root(&update.attested_header.beacon),
        compute_sync_committee_domain(update.signature_slot),
    );
    let participant_refs = participant_pubkeys.iter().collect::<Vec<_>>();
    let result = signature.fast_aggregate_verify(
        false,
        message.as_slice(),
        SYNC_COMMITTEE_SIGNATURE_DST,
        &participant_refs,
    );
    if result != BLST_ERROR::BLST_SUCCESS {
        return Err(LightClientVerificationError::InvalidSyncCommitteeSignature);
    }

    Ok(update)
}

fn process_light_client_update(
    store: &VerifiedLightClientStore,
    update: VerifiedLightClientUpdate,
) -> Result<AppliedLightClientUpdate, LightClientVerificationError> {
    let mut next_store = store.clone();
    if next_store
        .best_valid_update
        .as_ref()
        .is_none_or(|best| is_better_update(&update, best))
    {
        next_store.best_valid_update = Some(update.clone());
    }

    next_store.current_max_active_participants = next_store
        .current_max_active_participants
        .max(update.participants);

    if update.participants > safety_threshold(&next_store)
        && update.attested_header.beacon.slot > next_store.optimistic_header.beacon.slot
    {
        next_store.optimistic_header = update.attested_header.clone();
    }

    let update_has_finalized_next_sync_committee = next_store.next_sync_committee.is_none()
        && update.next_sync_committee.is_some()
        && update.finalized_header.is_some()
        && sync_committee_period_at_slot(update.finalized_slot())
            == sync_committee_period_at_slot(update.attested_header.beacon.slot);
    if update.participants * 3 >= SYNC_COMMITTEE_PUBKEYS * 2
        && (update.finalized_slot() > next_store.finalized_header.beacon.slot
            || update_has_finalized_next_sync_committee)
    {
        apply_validated_light_client_update(&mut next_store, &update);
        next_store.best_valid_update = None;
    }

    let optimistic_status = LightClientOptimisticUpdateStatus {
        fork: update.attested_header.fork,
        attested_header: header_summary_verified(&update.attested_header),
        signature_slot: update.signature_slot,
        sync_committee_participants: update.participants,
    };
    let finality_status =
        update
            .finalized_header
            .as_ref()
            .map(|finalized_header| LightClientFinalityUpdateStatus {
                fork: finalized_header.fork,
                attested_header: header_summary_verified(&update.attested_header),
                finalized_header: header_summary_verified(finalized_header),
                signature_slot: update.signature_slot,
                sync_committee_participants: update.participants,
                finality_branch_depth: finality_branch_depth_at_slot(
                    update.attested_header.beacon.slot,
                ),
            });

    Ok(AppliedLightClientUpdate {
        store: next_store,
        optimistic_status,
        finality_status,
    })
}

pub(crate) fn force_update_light_client_store(
    store: &VerifiedLightClientStore,
) -> Option<VerifiedLightClientStore> {
    if wall_clock_slot()
        <= store
            .finalized_header
            .beacon
            .slot
            .saturating_add(UPDATE_TIMEOUT)
    {
        return None;
    }
    let mut next_store = store.clone();
    let mut best_update = next_store.best_valid_update.clone()?;
    if best_update.finalized_slot() <= next_store.finalized_header.beacon.slot {
        best_update.finalized_header = Some(best_update.attested_header.clone());
    }
    apply_validated_light_client_update(&mut next_store, &best_update);
    next_store.best_valid_update = None;
    Some(next_store)
}

fn apply_validated_light_client_update(
    store: &mut VerifiedLightClientStore,
    update: &VerifiedLightClientUpdate,
) {
    let store_period = sync_committee_period_at_slot(store.finalized_header.beacon.slot);
    let update_finalized_period = sync_committee_period_at_slot(update.finalized_slot());
    if store.next_sync_committee.is_none() {
        store.next_sync_committee = update.next_sync_committee.clone();
    } else if update_finalized_period == store_period + 1 {
        if let Some(next_sync_committee) = store.next_sync_committee.clone() {
            store.current_sync_committee = next_sync_committee;
        }
        store.next_sync_committee = update.next_sync_committee.clone();
        store.previous_max_active_participants = store.current_max_active_participants;
        store.current_max_active_participants = 0;
    }
    if let Some(finalized_header) = &update.finalized_header
        && finalized_header.beacon.slot > store.finalized_header.beacon.slot
    {
        store.finalized_header = finalized_header.clone();
        if store.finalized_header.beacon.slot > store.optimistic_header.beacon.slot {
            store.optimistic_header = store.finalized_header.clone();
        }
    }
}

fn safety_threshold(store: &VerifiedLightClientStore) -> usize {
    store
        .previous_max_active_participants
        .max(store.current_max_active_participants)
        / 2
}

fn header_summary_capella(header: &LightClientHeaderCapella) -> LightClientHeaderSummary {
    LightClientHeaderSummary {
        beacon_slot: header.beacon.slot,
        execution: execution_data_capella(&header.execution),
    }
}

fn header_summary_deneb(header: &LightClientHeaderDeneb) -> LightClientHeaderSummary {
    LightClientHeaderSummary {
        beacon_slot: header.beacon.slot,
        execution: execution_data_deneb(&header.execution),
    }
}

fn execution_data_capella(
    execution: &ExecutionPayloadHeaderCapella,
) -> Option<LightClientExecutionData> {
    (execution.block_hash != B256::ZERO).then_some(LightClientExecutionData {
        block_number: execution.block_number,
        block_hash: execution.block_hash,
        receipts_root: execution.receipts_root,
    })
}

fn execution_data_deneb(
    execution: &ExecutionPayloadHeaderDeneb,
) -> Option<LightClientExecutionData> {
    (execution.block_hash != B256::ZERO).then_some(LightClientExecutionData {
        block_number: execution.block_number,
        block_hash: execution.block_hash,
        receipts_root: execution.receipts_root,
    })
}

fn header_summary_verified(header: &VerifiedLightClientHeader) -> LightClientHeaderSummary {
    LightClientHeaderSummary {
        beacon_slot: header.beacon.slot,
        execution: header.execution.map(|execution| LightClientExecutionData {
            block_number: execution.block_number,
            block_hash: execution.block_hash,
            receipts_root: execution.receipts_root,
        }),
    }
}

fn participant_count(sync_aggregate: &SyncAggregateRaw) -> usize {
    sync_aggregate
        .sync_committee_bits
        .as_slice()
        .iter()
        .map(|byte| byte.count_ones() as usize)
        .sum()
}

fn is_better_update(
    new_update: &VerifiedLightClientUpdate,
    old_update: &VerifiedLightClientUpdate,
) -> bool {
    if new_update.has_supermajority() != old_update.has_supermajority() {
        return new_update.has_supermajority();
    }
    if !new_update.has_supermajority() && new_update.participants != old_update.participants {
        return new_update.participants > old_update.participants;
    }
    if new_update.has_relevant_sync_committee() != old_update.has_relevant_sync_committee() {
        return new_update.has_relevant_sync_committee();
    }
    if new_update.finalized_header.is_some() != old_update.finalized_header.is_some() {
        return new_update.finalized_header.is_some();
    }
    if new_update.finalized_header.is_some()
        && new_update.has_sync_committee_finality() != old_update.has_sync_committee_finality()
    {
        return new_update.has_sync_committee_finality();
    }
    if new_update.participants != old_update.participants {
        return new_update.participants > old_update.participants;
    }
    if new_update.attested_header.beacon.slot != old_update.attested_header.beacon.slot {
        return new_update.attested_header.beacon.slot < old_update.attested_header.beacon.slot;
    }
    new_update.signature_slot < old_update.signature_slot
}

fn branch_has_nonzero(branch: &[u8]) -> bool {
    branch.iter().any(|byte| *byte != 0)
}

fn sync_committee_tree_hash_root(committee: &SyncCommitteeData) -> B256 {
    let mut pubkeys = Vec::with_capacity(SYNC_COMMITTEE_PUBKEY_BYTES);
    for pubkey in &committee.pubkeys {
        pubkeys.extend_from_slice(pubkey.as_slice());
    }
    let raw = SyncCommitteeRaw {
        pubkeys: FixedBytes::from_slice(&pubkeys),
        aggregate_pubkey: committee.aggregate_pubkey,
    };
    raw.tree_hash_root()
}

fn candidate_failures(bytes: &[u8], attempts: &[(&str, Option<String>)]) -> String {
    let details = attempts
        .iter()
        .filter_map(|(label, error)| error.as_ref().map(|error| format!("{label}: {error}")))
        .collect::<Vec<_>>()
        .join("; ");
    format!("{} bytes; {details}", bytes.len())
}

fn sync_committee_period_at_slot(slot: u64) -> u64 {
    MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(slot) / EPOCHS_PER_SYNC_COMMITTEE_PERIOD
}

fn finalized_root_gindex_at_slot(slot: u64) -> u64 {
    if uses_electra_light_client_layout(slot) {
        ELECTRA_FINALIZED_ROOT_GINDEX
    } else {
        PRE_ELECTRA_FINALIZED_ROOT_GINDEX
    }
}

fn current_sync_committee_gindex_at_slot(slot: u64) -> u64 {
    if uses_electra_light_client_layout(slot) {
        ELECTRA_CURRENT_SYNC_COMMITTEE_GINDEX
    } else {
        PRE_ELECTRA_CURRENT_SYNC_COMMITTEE_GINDEX
    }
}

fn finality_branch_depth_at_slot(slot: u64) -> usize {
    if uses_electra_light_client_layout(slot) {
        ELECTRA_FINALITY_BRANCH_DEPTH
    } else {
        PRE_ELECTRA_FINALITY_BRANCH_DEPTH
    }
}

#[allow(dead_code)]
fn next_sync_committee_gindex_at_slot(slot: u64) -> u64 {
    if uses_electra_light_client_layout(slot) {
        ELECTRA_NEXT_SYNC_COMMITTEE_GINDEX
    } else {
        PRE_ELECTRA_NEXT_SYNC_COMMITTEE_GINDEX
    }
}

fn uses_electra_light_client_layout(slot: u64) -> bool {
    let epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(slot);
    MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(epoch)[0] >= ELECTRA_FORK_VERSION
}

fn fork_for_slot(slot: u64) -> ConsensusDataFork {
    let epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(slot);
    match MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(epoch)[0] {
        version if version >= ELECTRA_FORK_VERSION => ConsensusDataFork::Electra,
        DENEB_FORK_VERSION => ConsensusDataFork::Deneb,
        CAPELLA_FORK_VERSION => ConsensusDataFork::Capella,
        _ => ConsensusDataFork::Capella,
    }
}

fn wall_clock_slot() -> u64 {
    let unix_now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(MAINNET_CONSENSUS_CHAIN_SPEC.genesis_time);
    unix_now.saturating_sub(MAINNET_CONSENSUS_CHAIN_SPEC.genesis_time) / 12
}

fn compute_sync_committee_domain(signature_slot: u64) -> [u8; 32] {
    let epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(signature_slot.saturating_sub(1));
    let fork_version = MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(epoch);
    let fork_data_root = fork_data_root(fork_version);
    let mut domain = [0u8; 32];
    domain[..4].copy_from_slice(&SYNC_COMMITTEE_DOMAIN);
    domain[4..].copy_from_slice(&fork_data_root[..28]);
    domain
}

fn fork_data_root(fork_version: [u8; 4]) -> [u8; 32] {
    let mut version_bytes = [0u8; 32];
    version_bytes[..4].copy_from_slice(&fork_version);
    let mut hasher = Sha256::new();
    hasher.update(version_bytes);
    hasher.update(
        MAINNET_CONSENSUS_CHAIN_SPEC
            .genesis_validators_root
            .as_slice(),
    );
    hasher.finalize().into()
}

fn compute_signing_root(object_root: B256, domain: [u8; 32]) -> B256 {
    hash_concat(object_root, B256::from(domain))
}

fn sync_committee_pubkeys_root_from_bytes(
    pubkeys: &FixedBytes<SYNC_COMMITTEE_PUBKEY_BYTES>,
) -> B256 {
    let mut roots = Vec::with_capacity(SYNC_COMMITTEE_PUBKEYS * 32);
    for pubkey in pubkeys.as_slice().chunks_exact(BLS_PUBKEY_BYTES) {
        let root = FixedBytes::<BLS_PUBKEY_BYTES>::from_slice(pubkey).tree_hash_root();
        roots.extend_from_slice(root.as_slice());
    }
    merkle_root(&roots, SYNC_COMMITTEE_PUBKEYS)
}

fn beacon_block_header_root(header: &BeaconBlockHeaderSsz) -> B256 {
    container_root_from_roots(&[
        header.slot.tree_hash_root(),
        header.proposer_index.tree_hash_root(),
        header.parent_root.tree_hash_root(),
        header.state_root.tree_hash_root(),
        header.body_root.tree_hash_root(),
    ])
}

fn execution_payload_header_capella_root(header: &ExecutionPayloadHeaderCapella) -> B256 {
    container_root_from_roots(&[
        header.parent_hash.tree_hash_root(),
        header.fee_recipient.tree_hash_root(),
        header.state_root.tree_hash_root(),
        header.receipts_root.tree_hash_root(),
        header.logs_bloom.tree_hash_root(),
        header.prev_randao.tree_hash_root(),
        header.block_number.tree_hash_root(),
        header.gas_limit.tree_hash_root(),
        header.gas_used.tree_hash_root(),
        header.timestamp.tree_hash_root(),
        bytes_list_root(&header.extra_data),
        header.base_fee_per_gas.tree_hash_root(),
        header.block_hash.tree_hash_root(),
        header.transactions_root.tree_hash_root(),
        header.withdrawals_root.tree_hash_root(),
    ])
}

fn execution_payload_header_deneb_root(header: &ExecutionPayloadHeaderDeneb) -> B256 {
    container_root_from_roots(&[
        header.parent_hash.tree_hash_root(),
        header.fee_recipient.tree_hash_root(),
        header.state_root.tree_hash_root(),
        header.receipts_root.tree_hash_root(),
        header.logs_bloom.tree_hash_root(),
        header.prev_randao.tree_hash_root(),
        header.block_number.tree_hash_root(),
        header.gas_limit.tree_hash_root(),
        header.gas_used.tree_hash_root(),
        header.timestamp.tree_hash_root(),
        bytes_list_root(&header.extra_data),
        header.base_fee_per_gas.tree_hash_root(),
        header.block_hash.tree_hash_root(),
        header.transactions_root.tree_hash_root(),
        header.withdrawals_root.tree_hash_root(),
        header.blob_gas_used.tree_hash_root(),
        header.excess_blob_gas.tree_hash_root(),
    ])
}

fn container_root_from_roots(field_roots: &[B256]) -> B256 {
    let mut bytes = Vec::with_capacity(field_roots.len() * 32);
    for root in field_roots {
        bytes.extend_from_slice(root.as_slice());
    }
    merkle_root(&bytes, 0)
}

fn bytes_list_root(bytes: &[u8]) -> B256 {
    let root = merkle_root(bytes, 0);
    mix_in_length(&root, bytes.len())
}

fn branch_from_fixed<const N: usize>(branch: &FixedBytes<N>) -> Vec<B256> {
    branch
        .as_slice()
        .chunks_exact(32)
        .map(B256::from_slice)
        .collect()
}

fn is_valid_normalized_merkle_branch(
    leaf: B256,
    branch: Vec<B256>,
    gindex: u64,
    root: B256,
) -> bool {
    let depth = floorlog2(gindex);
    let extra = branch.len().saturating_sub(depth);
    if branch[..extra].iter().any(|item| *item != B256::ZERO) {
        return false;
    }
    is_valid_merkle_branch(leaf, &branch[extra..], depth, subtree_index(gindex), root)
}

fn is_valid_merkle_branch(
    mut leaf: B256,
    branch: &[B256],
    depth: usize,
    mut index: u64,
    root: B256,
) -> bool {
    if branch.len() != depth {
        return false;
    }
    for sibling in branch {
        leaf = if index & 1 == 0 {
            hash_concat(leaf, *sibling)
        } else {
            hash_concat(*sibling, leaf)
        };
        index >>= 1;
    }
    leaf == root
}

fn floorlog2(value: u64) -> usize {
    (u64::BITS - 1 - value.leading_zeros()) as usize
}

fn subtree_index(gindex: u64) -> u64 {
    gindex % (1u64 << floorlog2(gindex))
}

fn hash_concat(left: B256, right: B256) -> B256 {
    let mut hasher = Sha256::new();
    hasher.update(left.as_slice());
    hasher.update(right.as_slice());
    B256::from_slice(&hasher.finalize())
}

impl SyncCommitteeRaw {
    fn to_persisted(&self) -> SyncCommitteeData {
        SyncCommitteeData {
            pubkeys: self
                .pubkeys
                .as_slice()
                .chunks_exact(BLS_PUBKEY_BYTES)
                .map(BlsPublicKey::from_slice)
                .collect(),
            aggregate_pubkey: self.aggregate_pubkey,
        }
    }
}

impl SyncCommitteeData {
    fn participant_public_keys(
        &self,
        bits: &FixedBytes<SYNC_COMMITTEE_BITS_BYTES>,
    ) -> Result<Vec<BlstPublicKey>, LightClientVerificationError> {
        let mut active = Vec::new();
        for (index, pubkey) in self.pubkeys.iter().enumerate() {
            let selected = bits.as_slice()[index / 8] & (1 << (index % 8)) != 0;
            if !selected {
                continue;
            }
            let pubkey = BlstPublicKey::key_validate(pubkey.as_slice()).map_err(|error| {
                LightClientVerificationError::InvalidCommitteePublicKey {
                    index,
                    details: format!("{error:?}"),
                }
            })?;
            active.push(pubkey);
        }
        Ok(active)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blst::min_pk::SecretKey;
    use ssz::Encode;

    fn fixed<const N: usize>(byte: u8) -> FixedBytes<N> {
        FixedBytes::from_slice(&vec![byte; N])
    }

    fn beacon_header(
        slot: u64,
        state_root: B256,
        body_root: B256,
        byte: u8,
    ) -> BeaconBlockHeaderSsz {
        BeaconBlockHeaderSsz {
            slot,
            proposer_index: 7,
            parent_root: B256::repeat_byte(byte),
            state_root,
            body_root,
        }
    }

    fn deneb_execution(block_number: u64, byte: u8) -> ExecutionPayloadHeaderDeneb {
        ExecutionPayloadHeaderDeneb {
            parent_hash: B256::repeat_byte(byte),
            fee_recipient: Address::repeat_byte(byte),
            state_root: B256::repeat_byte(byte.wrapping_add(1)),
            receipts_root: B256::repeat_byte(byte.wrapping_add(2)),
            logs_bloom: fixed::<LOGS_BLOOM_BYTES>(byte.wrapping_add(3)),
            prev_randao: B256::repeat_byte(byte.wrapping_add(4)),
            block_number,
            gas_limit: 30_000_000,
            gas_used: 15_000_000,
            timestamp: 1_700_000_000,
            extra_data: vec![byte; 4],
            base_fee_per_gas: U256::from(42u64),
            block_hash: B256::repeat_byte(byte.wrapping_add(5)),
            transactions_root: B256::repeat_byte(byte.wrapping_add(6)),
            withdrawals_root: B256::repeat_byte(byte.wrapping_add(7)),
            blob_gas_used: 131_072,
            excess_blob_gas: 262_144,
        }
    }

    fn branch_root(leaf: B256, siblings: &[B256], index: u64) -> B256 {
        let mut root = leaf;
        let mut path = index;
        for sibling in siblings {
            root = if path & 1 == 0 {
                hash_concat(root, *sibling)
            } else {
                hash_concat(*sibling, root)
            };
            path >>= 1;
        }
        root
    }

    fn sync_committee_from_secret_key(sk: &SecretKey) -> SyncCommitteeRaw {
        let pk = sk.sk_to_pk().compress();
        let mut pubkeys = vec![0u8; SYNC_COMMITTEE_PUBKEY_BYTES];
        pubkeys[..BLS_PUBKEY_BYTES].copy_from_slice(&pk);
        SyncCommitteeRaw {
            pubkeys: FixedBytes::from_slice(&pubkeys),
            aggregate_pubkey: FixedBytes::from_slice(&pk),
        }
    }

    fn signed_sync_aggregate(
        sk: &SecretKey,
        attested_header: &BeaconBlockHeaderSsz,
        signature_slot: u64,
    ) -> SyncAggregateRaw {
        let message = compute_signing_root(
            beacon_block_header_root(attested_header),
            compute_sync_committee_domain(signature_slot),
        );
        let signature = sk.sign(message.as_slice(), SYNC_COMMITTEE_SIGNATURE_DST, &[]);
        let mut bits = [0u8; SYNC_COMMITTEE_BITS_BYTES];
        bits[0] = 0b0000_0001;
        SyncAggregateRaw {
            sync_committee_bits: FixedBytes::from(bits),
            sync_committee_signature: FixedBytes::from_slice(&signature.compress()),
        }
    }

    fn bootstrap_payload(slot: u64) -> (WeakSubjectivityCheckpoint, Vec<u8>, SecretKey) {
        let sk = SecretKey::key_gen(&[7u8; 32], &[]).unwrap();
        let sync_committee = sync_committee_from_secret_key(&sk);
        let sync_committee_root = sync_committee.tree_hash_root();
        let sync_siblings = [
            B256::repeat_byte(0xa1),
            B256::repeat_byte(0xa2),
            B256::repeat_byte(0xa3),
            B256::repeat_byte(0xa4),
            B256::repeat_byte(0xa5),
        ];
        let state_root = branch_root(
            sync_committee_root,
            &sync_siblings,
            subtree_index(current_sync_committee_gindex_at_slot(slot)),
        );

        let execution = deneb_execution(19_000_001, 0x21);
        let execution_root = execution_payload_header_deneb_root(&execution);
        let execution_siblings = [
            B256::repeat_byte(0xb1),
            B256::repeat_byte(0xb2),
            B256::repeat_byte(0xb3),
            B256::repeat_byte(0xb4),
        ];
        let body_root = branch_root(
            execution_root,
            &execution_siblings,
            subtree_index(EXECUTION_PAYLOAD_GINDEX),
        );

        let header = LightClientHeaderDeneb {
            beacon: beacon_header(slot, state_root, body_root, 0x11),
            execution,
            execution_branch: {
                let mut bytes = Vec::with_capacity(32 * EXECUTION_BRANCH_DEPTH);
                for sibling in execution_siblings {
                    bytes.extend_from_slice(sibling.as_slice());
                }
                FixedBytes::from_slice(&bytes)
            },
        };
        let checkpoint = WeakSubjectivityCheckpoint {
            beacon_root: beacon_block_header_root(&header.beacon),
            beacon_slot: Some(slot),
        };
        let payload = LightClientBootstrapDeneb {
            header,
            current_sync_committee: sync_committee.clone(),
            current_sync_committee_branch: {
                let mut bytes = Vec::with_capacity(32 * PRE_ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH);
                for sibling in sync_siblings {
                    bytes.extend_from_slice(sibling.as_slice());
                }
                FixedBytes::from_slice(&bytes)
            },
        };
        (checkpoint, payload.as_ssz_bytes(), sk)
    }

    fn update_payload_with_next_sync_committee(slot: u64) -> (Vec<u8>, SecretKey) {
        let signing_sk = SecretKey::key_gen(&[7u8; 32], &[]).unwrap();
        let next_sk = SecretKey::key_gen(&[9u8; 32], &[]).unwrap();
        let next_sync_committee = sync_committee_from_secret_key(&next_sk);
        let next_sync_committee_root = next_sync_committee.tree_hash_root();
        let next_sync_siblings = [
            B256::repeat_byte(0xd1),
            B256::repeat_byte(0xd2),
            B256::repeat_byte(0xd3),
            B256::repeat_byte(0xd4),
            B256::repeat_byte(0xd5),
        ];
        let state_root = branch_root(
            next_sync_committee_root,
            &next_sync_siblings,
            subtree_index(next_sync_committee_gindex_at_slot(slot + 1)),
        );

        let execution = deneb_execution(19_000_020, 0x52);
        let execution_root = execution_payload_header_deneb_root(&execution);
        let execution_siblings = [
            B256::repeat_byte(0xe1),
            B256::repeat_byte(0xe2),
            B256::repeat_byte(0xe3),
            B256::repeat_byte(0xe4),
        ];
        let attested_header = LightClientHeaderDeneb {
            beacon: beacon_header(
                slot + 1,
                state_root,
                branch_root(
                    execution_root,
                    &execution_siblings,
                    subtree_index(EXECUTION_PAYLOAD_GINDEX),
                ),
                0x41,
            ),
            execution,
            execution_branch: {
                let mut bytes = Vec::with_capacity(32 * EXECUTION_BRANCH_DEPTH);
                for sibling in execution_siblings {
                    bytes.extend_from_slice(sibling.as_slice());
                }
                FixedBytes::from_slice(&bytes)
            },
        };
        let signature_slot = slot + 2;
        let payload = LightClientUpdateDeneb {
            attested_header: attested_header.clone(),
            next_sync_committee: next_sync_committee.clone(),
            next_sync_committee_branch: {
                let mut bytes = Vec::with_capacity(32 * PRE_ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH);
                for sibling in next_sync_siblings {
                    bytes.extend_from_slice(sibling.as_slice());
                }
                FixedBytes::from_slice(&bytes)
            },
            finalized_header: LightClientHeaderDeneb::default(),
            finality_branch: FixedBytes::ZERO,
            sync_aggregate: signed_sync_aggregate(
                &signing_sk,
                &attested_header.beacon,
                signature_slot,
            ),
            signature_slot,
        };
        (payload.as_ssz_bytes(), next_sk)
    }

    #[test]
    fn verifies_deneb_bootstrap_and_builds_store() {
        let slot = 10_000_000;
        let (checkpoint, bytes, _) = bootstrap_payload(slot);
        let (status, store) = verify_bootstrap_payload(&bytes, checkpoint).unwrap();

        assert_eq!(status.fork, ConsensusDataFork::Deneb);
        assert_eq!(status.header.beacon_slot, slot);
        assert_eq!(store.bootstrap_slot(), slot);
        assert_eq!(store.finalized_header.beacon_root(), checkpoint.beacon_root);
        assert_eq!(
            store.optimistic_anchor().map(|anchor| anchor.block_number),
            Some(19_000_001)
        );
    }

    #[test]
    fn rejects_bootstrap_with_invalid_execution_branch() {
        let slot = 10_000_000;
        let (checkpoint, mut bytes, _) = bootstrap_payload(slot);
        let mut payload = LightClientBootstrapDeneb::from_ssz_bytes(&bytes).unwrap();
        payload.header.execution_branch = fixed::<{ 32 * EXECUTION_BRANCH_DEPTH }>(0xff);
        bytes = payload.as_ssz_bytes();

        let error = verify_bootstrap_payload(&bytes, checkpoint).unwrap_err();
        assert!(matches!(
            error,
            LightClientVerificationError::InvalidExecutionProof { .. }
        ));
    }

    #[test]
    fn verifies_optimistic_update_signature_and_advances_head() {
        let slot = 10_000_000;
        let (checkpoint, bootstrap_bytes, sk) = bootstrap_payload(slot);
        let (_, store) = verify_bootstrap_payload(&bootstrap_bytes, checkpoint).unwrap();

        let execution = deneb_execution(19_000_010, 0x44);
        let execution_root = execution_payload_header_deneb_root(&execution);
        let execution_siblings = [
            B256::repeat_byte(0xc1),
            B256::repeat_byte(0xc2),
            B256::repeat_byte(0xc3),
            B256::repeat_byte(0xc4),
        ];
        let attested_header = LightClientHeaderDeneb {
            beacon: beacon_header(
                slot + 1,
                B256::repeat_byte(0xdd),
                branch_root(
                    execution_root,
                    &execution_siblings,
                    subtree_index(EXECUTION_PAYLOAD_GINDEX),
                ),
                0x33,
            ),
            execution,
            execution_branch: {
                let mut bytes = Vec::with_capacity(32 * EXECUTION_BRANCH_DEPTH);
                for sibling in execution_siblings {
                    bytes.extend_from_slice(sibling.as_slice());
                }
                FixedBytes::from_slice(&bytes)
            },
        };
        let signature_slot = slot + 2;
        let payload = LightClientOptimisticUpdateDeneb {
            attested_header: attested_header.clone(),
            sync_aggregate: signed_sync_aggregate(&sk, &attested_header.beacon, signature_slot),
            signature_slot,
        };

        let (status, next_store, verified_header) =
            apply_optimistic_update_payload(&payload.as_ssz_bytes(), &store).unwrap();
        assert_eq!(status.attested_header.beacon_slot, slot + 1);
        assert_eq!(verified_header.beacon.slot, slot + 1);
        assert_eq!(next_store.optimistic_header.beacon.slot, slot + 1);
        assert_eq!(
            next_store
                .optimistic_anchor()
                .map(|anchor| anchor.block_number),
            Some(19_000_010)
        );
    }

    #[test]
    fn applies_light_client_update_and_learns_next_committee_after_force_update() {
        let slot = 10_000_000;
        let (checkpoint, bootstrap_bytes, _) = bootstrap_payload(slot);
        let (_, store) = verify_bootstrap_payload(&bootstrap_bytes, checkpoint).unwrap();
        let (bytes, _next_sk) = update_payload_with_next_sync_committee(slot);

        let applied = apply_light_client_update_payload(&bytes, &store).unwrap();
        assert_eq!(
            applied.optimistic_status.attested_header.beacon_slot,
            slot + 1
        );
        assert!(applied.finality_status.is_none());
        assert_eq!(applied.store.optimistic_header.beacon.slot, slot + 1);
        assert!(applied.store.next_sync_committee.is_none());
        assert!(applied.store.best_valid_update.is_some());

        let forced = force_update_light_client_store(&applied.store).unwrap();
        assert!(forced.next_sync_committee.is_some());
        assert_eq!(forced.finalized_header.beacon.slot, slot + 1);
    }
}
