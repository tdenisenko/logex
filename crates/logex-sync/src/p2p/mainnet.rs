use std::time::{SystemTime, UNIX_EPOCH};

use alloy_primitives::{B256, U256, b256};
use reth_chainspec::MAINNET;
use reth_ethereum_forks::{ForkFilter, Head};

/// Mainnet genesis block hash.
pub const MAINNET_GENESIS: B256 =
    b256!("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3");

/// Mainnet genesis timestamp (July 30, 2015).
pub const MAINNET_GENESIS_TIMESTAMP: u64 = 1_438_226_773;

/// Mainnet chain ID.
pub const MAINNET_CHAIN_ID: u64 = 1;

/// Ethereum mainnet genesis difficulty.
pub const MAINNET_GENESIS_DIFFICULTY: u64 = 17_179_869_184;

/// Return a handshake-safe view of our head.
///
/// For fork-ID validation we care about chain compatibility more than our
/// local sync height. A brand-new node at block 0 still needs to present the
/// current mainnet fork schedule or many peers will reject the ETH handshake
/// as if we were on an obsolete pre-fork network.
pub fn handshake_head(base: Head) -> Head {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    Head {
        number: u64::MAX,
        hash: if base.hash.is_zero() {
            MAINNET_GENESIS
        } else {
            base.hash
        },
        difficulty: if base.difficulty.is_zero() {
            U256::from(MAINNET_GENESIS_DIFFICULTY)
        } else {
            base.difficulty
        },
        total_difficulty: if base.total_difficulty.is_zero() {
            U256::from(MAINNET_GENESIS_DIFFICULTY)
        } else {
            base.total_difficulty
        },
        timestamp: base.timestamp.max(now),
    }
}

/// Build a ForkFilter for Ethereum mainnet with the given head.
pub fn mainnet_fork_filter(head: Head) -> ForkFilter {
    MAINNET.fork_filter(head)
}
