use alloy_primitives::{b256, B256};

/// Mainnet consensus constants that LogEx needs for peer interop and light-client verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusChainSpec {
    pub genesis_block_root: B256,
    pub genesis_validators_root: B256,
    pub genesis_fork_version: [u8; 4],
}

pub const MAINNET_CONSENSUS_CHAIN_SPEC: ConsensusChainSpec = ConsensusChainSpec {
    genesis_block_root: b256!("4d611d5b93fdab69013a7f0a2f961caca0c853f87cfe9595fe50038163079360"),
    genesis_validators_root: b256!(
        "4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
    ),
    genesis_fork_version: [0x00, 0x00, 0x00, 0x00],
};
