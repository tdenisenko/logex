use alloy_primitives::{B256, b256};
use reth_ethereum_forks::{ForkFilter, ForkFilterKey, Head};

/// Mainnet genesis block hash.
pub const MAINNET_GENESIS: B256 =
    b256!("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3");

/// Mainnet genesis timestamp (July 30, 2015).
pub const MAINNET_GENESIS_TIMESTAMP: u64 = 1_438_226_773;

/// Mainnet chain ID.
pub const MAINNET_CHAIN_ID: u64 = 1;

/// Build a ForkFilter for Ethereum mainnet with the given head.
///
/// Fork schedule (block-based and timestamp-based) from:
/// https://github.com/ethereum/execution-specs/tree/master/network-upgrades/mainnet-upgrades
pub fn mainnet_fork_filter(head: Head) -> ForkFilter {
    let forks = [
        // Block-based forks
        ForkFilterKey::Block(1_150_000),  // Homestead
        ForkFilterKey::Block(1_920_000),  // DAO Fork
        ForkFilterKey::Block(2_463_000),  // Tangerine Whistle (EIP-150)
        ForkFilterKey::Block(2_675_000),  // Spurious Dragon
        ForkFilterKey::Block(4_370_000),  // Byzantium
        ForkFilterKey::Block(7_280_000),  // Constantinople/Petersburg
        ForkFilterKey::Block(9_069_000),  // Istanbul
        ForkFilterKey::Block(9_200_000),  // Muir Glacier
        ForkFilterKey::Block(12_244_000), // Berlin
        ForkFilterKey::Block(12_965_000), // London
        ForkFilterKey::Block(13_773_000), // Arrow Glacier
        ForkFilterKey::Block(15_050_000), // Gray Glacier
        // Timestamp-based forks (post-merge)
        ForkFilterKey::Time(1_681_338_455), // Shanghai
        ForkFilterKey::Time(1_710_338_135), // Cancun
        ForkFilterKey::Time(1_746_612_311), // Prague
    ];
    ForkFilter::new(head, MAINNET_GENESIS, MAINNET_GENESIS_TIMESTAMP, forks)
}
