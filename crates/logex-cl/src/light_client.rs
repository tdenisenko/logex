use alloy_primitives::{Address, B256, FixedBytes, U256};
use alloy_rpc_types_beacon::{BlsPublicKey, BlsSignature};
use logex_types::{
    ConsensusDataFork, LightClientBootstrapStatus, LightClientExecutionData,
    LightClientFinalityUpdateStatus, LightClientHeaderSummary, LightClientOptimisticUpdateStatus,
};
use ssz::Decode;
use ssz_derive::{Decode, Encode};
use thiserror::Error;

const SYNC_COMMITTEE_PUBKEYS: usize = 512;
const BLS_PUBKEY_BYTES: usize = 48;
#[cfg(test)]
const BLS_SIGNATURE_BYTES: usize = 96;
const SYNC_COMMITTEE_BITS_BYTES: usize = 64;
const LOGS_BLOOM_BYTES: usize = 256;

const EXECUTION_BRANCH_DEPTH: usize = 4;
const PRE_ELECTRA_FINALITY_BRANCH_DEPTH: usize = 6;
const PRE_ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH: usize = 5;
const ELECTRA_FINALITY_BRANCH_DEPTH: usize = 7;
const ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH: usize = 6;

const SYNC_COMMITTEE_PUBKEY_BYTES: usize = SYNC_COMMITTEE_PUBKEYS * BLS_PUBKEY_BYTES;

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

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct BeaconBlockHeaderSsz {
    slot: u64,
    proposer_index: u64,
    parent_root: B256,
    state_root: B256,
    body_root: B256,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct SyncCommitteeRaw {
    pubkeys: FixedBytes<SYNC_COMMITTEE_PUBKEY_BYTES>,
    aggregate_pubkey: BlsPublicKey,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct SyncAggregateRaw {
    sync_committee_bits: FixedBytes<SYNC_COMMITTEE_BITS_BYTES>,
    sync_committee_signature: BlsSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
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

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
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

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
struct LightClientHeaderCapella {
    beacon: BeaconBlockHeaderSsz,
    execution: ExecutionPayloadHeaderCapella,
    execution_branch: ExecutionBranch,
}

#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
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

pub fn decode_bootstrap(
    bytes: &[u8],
) -> Result<LightClientBootstrapStatus, LightClientDecodeError> {
    if let Ok(payload) = LightClientBootstrapElectra::from_ssz_bytes(bytes) {
        return Ok(LightClientBootstrapStatus {
            fork: ConsensusDataFork::Electra,
            header: header_summary_deneb(&payload.header),
            current_sync_committee_pubkeys: SYNC_COMMITTEE_PUBKEYS,
            current_sync_committee_branch_depth: ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH,
        });
    }

    if let Ok(payload) = LightClientBootstrapDeneb::from_ssz_bytes(bytes) {
        return Ok(LightClientBootstrapStatus {
            fork: ConsensusDataFork::Deneb,
            header: header_summary_deneb(&payload.header),
            current_sync_committee_pubkeys: SYNC_COMMITTEE_PUBKEYS,
            current_sync_committee_branch_depth: PRE_ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH,
        });
    }

    if let Ok(payload) = LightClientBootstrapCapella::from_ssz_bytes(bytes) {
        return Ok(LightClientBootstrapStatus {
            fork: ConsensusDataFork::Capella,
            header: header_summary_capella(&payload.header),
            current_sync_committee_pubkeys: SYNC_COMMITTEE_PUBKEYS,
            current_sync_committee_branch_depth: PRE_ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH,
        });
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

pub fn decode_finality_update(
    bytes: &[u8],
) -> Result<LightClientFinalityUpdateStatus, LightClientDecodeError> {
    if let Ok(payload) = LightClientFinalityUpdateElectra::from_ssz_bytes(bytes) {
        return Ok(LightClientFinalityUpdateStatus {
            fork: ConsensusDataFork::Electra,
            attested_header: header_summary_deneb(&payload.attested_header),
            finalized_header: header_summary_deneb(&payload.finalized_header),
            signature_slot: payload.signature_slot,
            sync_committee_participants: participant_count(&payload.sync_aggregate),
            finality_branch_depth: ELECTRA_FINALITY_BRANCH_DEPTH,
        });
    }

    if let Ok(payload) = LightClientFinalityUpdateDeneb::from_ssz_bytes(bytes) {
        return Ok(LightClientFinalityUpdateStatus {
            fork: ConsensusDataFork::Deneb,
            attested_header: header_summary_deneb(&payload.attested_header),
            finalized_header: header_summary_deneb(&payload.finalized_header),
            signature_slot: payload.signature_slot,
            sync_committee_participants: participant_count(&payload.sync_aggregate),
            finality_branch_depth: PRE_ELECTRA_FINALITY_BRANCH_DEPTH,
        });
    }

    if let Ok(payload) = LightClientFinalityUpdateCapella::from_ssz_bytes(bytes) {
        return Ok(LightClientFinalityUpdateStatus {
            fork: ConsensusDataFork::Capella,
            attested_header: header_summary_capella(&payload.attested_header),
            finalized_header: header_summary_capella(&payload.finalized_header),
            signature_slot: payload.signature_slot,
            sync_committee_participants: participant_count(&payload.sync_aggregate),
            finality_branch_depth: PRE_ELECTRA_FINALITY_BRANCH_DEPTH,
        });
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

pub fn decode_optimistic_update(
    bytes: &[u8],
) -> Result<LightClientOptimisticUpdateStatus, LightClientDecodeError> {
    if let Ok(payload) = LightClientOptimisticUpdateDeneb::from_ssz_bytes(bytes) {
        return Ok(LightClientOptimisticUpdateStatus {
            fork: ConsensusDataFork::Deneb,
            attested_header: header_summary_deneb(&payload.attested_header),
            signature_slot: payload.signature_slot,
            sync_committee_participants: participant_count(&payload.sync_aggregate),
        });
    }

    if let Ok(payload) = LightClientOptimisticUpdateCapella::from_ssz_bytes(bytes) {
        return Ok(LightClientOptimisticUpdateStatus {
            fork: ConsensusDataFork::Capella,
            attested_header: header_summary_capella(&payload.attested_header),
            signature_slot: payload.signature_slot,
            sync_committee_participants: participant_count(&payload.sync_aggregate),
        });
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

fn participant_count(sync_aggregate: &SyncAggregateRaw) -> usize {
    sync_aggregate
        .sync_committee_bits
        .as_slice()
        .iter()
        .map(|byte| byte.count_ones() as usize)
        .sum()
}

fn candidate_failures(bytes: &[u8], attempts: &[(&str, Option<String>)]) -> String {
    let details = attempts
        .iter()
        .filter_map(|(label, error)| error.as_ref().map(|error| format!("{label}: {error}")))
        .collect::<Vec<_>>()
        .join("; ");
    format!("{} bytes; {details}", bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ssz::Encode;

    fn fixed<const N: usize>(byte: u8) -> FixedBytes<N> {
        FixedBytes::from_slice(&vec![byte; N])
    }

    fn beacon_header(slot: u64, byte: u8) -> BeaconBlockHeaderSsz {
        BeaconBlockHeaderSsz {
            slot,
            proposer_index: 7,
            parent_root: B256::repeat_byte(byte),
            state_root: B256::repeat_byte(byte.wrapping_add(1)),
            body_root: B256::repeat_byte(byte.wrapping_add(2)),
        }
    }

    fn sync_committee() -> SyncCommitteeRaw {
        SyncCommitteeRaw {
            pubkeys: fixed::<SYNC_COMMITTEE_PUBKEY_BYTES>(0x11),
            aggregate_pubkey: fixed::<BLS_PUBKEY_BYTES>(0x22),
        }
    }

    fn sync_aggregate(participants: &[usize]) -> SyncAggregateRaw {
        let mut bits = vec![0u8; SYNC_COMMITTEE_BITS_BYTES];
        for participant in participants {
            bits[participant / 8] |= 1 << (participant % 8);
        }
        SyncAggregateRaw {
            sync_committee_bits: FixedBytes::from_slice(&bits),
            sync_committee_signature: fixed::<BLS_SIGNATURE_BYTES>(0x33),
        }
    }

    fn capella_execution(block_number: u64, byte: u8) -> ExecutionPayloadHeaderCapella {
        ExecutionPayloadHeaderCapella {
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

    #[test]
    fn decodes_electra_bootstrap_summary() {
        let payload = LightClientBootstrapElectra {
            header: LightClientHeaderDeneb {
                beacon: beacon_header(12_345, 0x41),
                execution: deneb_execution(22_222_222, 0x51),
                execution_branch: fixed::<{ 32 * EXECUTION_BRANCH_DEPTH }>(0x61),
            },
            current_sync_committee: sync_committee(),
            current_sync_committee_branch: fixed::<{ 32 * ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH }>(
                0x71,
            ),
        };

        let summary = decode_bootstrap(&payload.as_ssz_bytes()).unwrap();
        assert_eq!(summary.fork, ConsensusDataFork::Electra);
        assert_eq!(summary.header.beacon_slot, 12_345);
        assert_eq!(summary.header.execution.unwrap().block_number, 22_222_222);
        assert_eq!(
            summary.current_sync_committee_branch_depth,
            ELECTRA_SYNC_COMMITTEE_BRANCH_DEPTH
        );
    }

    #[test]
    fn decodes_deneb_finality_update_summary() {
        let payload = LightClientFinalityUpdateDeneb {
            attested_header: LightClientHeaderDeneb {
                beacon: beacon_header(54_321, 0x11),
                execution: deneb_execution(19_000_000, 0x21),
                execution_branch: fixed::<{ 32 * EXECUTION_BRANCH_DEPTH }>(0x31),
            },
            finalized_header: LightClientHeaderDeneb {
                beacon: beacon_header(54_300, 0x12),
                execution: deneb_execution(18_999_990, 0x22),
                execution_branch: fixed::<{ 32 * EXECUTION_BRANCH_DEPTH }>(0x32),
            },
            finality_branch: fixed::<{ 32 * PRE_ELECTRA_FINALITY_BRANCH_DEPTH }>(0x41),
            sync_aggregate: sync_aggregate(&[0, 1, 5, 9, 63]),
            signature_slot: 54_322,
        };

        let summary = decode_finality_update(&payload.as_ssz_bytes()).unwrap();
        assert_eq!(summary.fork, ConsensusDataFork::Deneb);
        assert_eq!(summary.attested_header.beacon_slot, 54_321);
        assert_eq!(summary.finalized_header.beacon_slot, 54_300);
        assert_eq!(summary.sync_committee_participants, 5);
        assert_eq!(
            summary.finality_branch_depth,
            PRE_ELECTRA_FINALITY_BRANCH_DEPTH
        );
    }

    #[test]
    fn decodes_capella_optimistic_update_summary() {
        let payload = LightClientOptimisticUpdateCapella {
            attested_header: LightClientHeaderCapella {
                beacon: beacon_header(77_777, 0x91),
                execution: capella_execution(17_777_777, 0x92),
                execution_branch: fixed::<{ 32 * EXECUTION_BRANCH_DEPTH }>(0x93),
            },
            sync_aggregate: sync_aggregate(&[2, 3, 4]),
            signature_slot: 77_778,
        };

        let summary = decode_optimistic_update(&payload.as_ssz_bytes()).unwrap();
        assert_eq!(summary.fork, ConsensusDataFork::Capella);
        assert_eq!(summary.attested_header.beacon_slot, 77_777);
        assert_eq!(
            summary.attested_header.execution.unwrap().block_number,
            17_777_777
        );
        assert_eq!(summary.sync_committee_participants, 3);
    }
}
