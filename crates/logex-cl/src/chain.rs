use std::time::{SystemTime, UNIX_EPOCH};

use alloy_primitives::{B256, b256};
use sha2::{Digest, Sha256};

const SECONDS_PER_SLOT: u64 = 12;
const SLOTS_PER_EPOCH: u64 = 32;
const FAR_FUTURE_EPOCH: u64 = u64::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduledFork {
    pub epoch: u64,
    pub version: [u8; 4],
}

/// Mainnet consensus constants that LogEx needs for peer interop and light-client verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusChainSpec {
    pub genesis_time: u64,
    pub genesis_block_root: B256,
    pub genesis_validators_root: B256,
    pub genesis_fork_version: [u8; 4],
    pub fork_schedule: &'static [ScheduledFork],
}

impl ConsensusChainSpec {
    pub const fn epoch_for_slot(self, slot: u64) -> u64 {
        slot / SLOTS_PER_EPOCH
    }

    pub fn wall_clock_epoch(self) -> u64 {
        let unix_now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs())
            .unwrap_or(self.genesis_time);
        unix_now
            .saturating_sub(self.genesis_time)
            .checked_div(SECONDS_PER_SLOT * SLOTS_PER_EPOCH)
            .unwrap_or(0)
    }

    pub const fn fork_version_for_epoch(self, epoch: u64) -> [u8; 4] {
        let mut version = self.genesis_fork_version;
        let mut index = 0usize;
        while index < self.fork_schedule.len() {
            let scheduled = self.fork_schedule[index];
            if epoch >= scheduled.epoch {
                version = scheduled.version;
                index += 1;
            } else {
                break;
            }
        }
        version
    }

    pub fn fork_digest_for_epoch(self, epoch: u64) -> [u8; 4] {
        self.fork_digest(self.fork_version_for_epoch(epoch))
    }

    pub fn enr_fork_id_for_epoch(self, epoch: u64) -> [u8; 16] {
        let current_version = self.fork_version_for_epoch(epoch);
        let (next_fork_version, next_fork_epoch) = self.next_fork_for_epoch(epoch);
        let mut fork_id = [0u8; 16];
        fork_id[0..4].copy_from_slice(&self.fork_digest(current_version));
        fork_id[4..8].copy_from_slice(&next_fork_version);
        fork_id[8..16].copy_from_slice(&next_fork_epoch.to_le_bytes());
        fork_id
    }

    fn fork_digest(self, version: [u8; 4]) -> [u8; 4] {
        let mut version_chunk = [0u8; 32];
        version_chunk[0..4].copy_from_slice(&version);
        let mut hasher = Sha256::new();
        hasher.update(version_chunk);
        hasher.update(self.genesis_validators_root.as_slice());
        let digest = hasher.finalize();
        let mut fork_digest = [0u8; 4];
        fork_digest.copy_from_slice(&digest[..4]);
        fork_digest
    }

    const fn next_fork_for_epoch(self, epoch: u64) -> ([u8; 4], u64) {
        let mut index = 0usize;
        while index < self.fork_schedule.len() {
            let scheduled = self.fork_schedule[index];
            if scheduled.epoch > epoch {
                return (scheduled.version, scheduled.epoch);
            }
            index += 1;
        }
        (self.fork_version_for_epoch(epoch), FAR_FUTURE_EPOCH)
    }
}

pub const MAINNET_CONSENSUS_CHAIN_SPEC: ConsensusChainSpec = ConsensusChainSpec {
    genesis_time: 1_606_824_023,
    genesis_block_root: b256!("4d611d5b93fdab69013a7f0a2f961caca0c853f87cfe9595fe50038163079360"),
    genesis_validators_root: b256!(
        "4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
    ),
    genesis_fork_version: [0x00, 0x00, 0x00, 0x00],
    fork_schedule: &[
        ScheduledFork {
            epoch: 74_240,
            version: [0x01, 0x00, 0x00, 0x00],
        },
        ScheduledFork {
            epoch: 144_896,
            version: [0x02, 0x00, 0x00, 0x00],
        },
        ScheduledFork {
            epoch: 194_048,
            version: [0x03, 0x00, 0x00, 0x00],
        },
        ScheduledFork {
            epoch: 269_568,
            version: [0x04, 0x00, 0x00, 0x00],
        },
        ScheduledFork {
            epoch: 364_032,
            version: [0x05, 0x00, 0x00, 0x00],
        },
        ScheduledFork {
            epoch: 411_392,
            version: [0x06, 0x00, 0x00, 0x00],
        },
    ],
};

#[cfg(test)]
mod tests {
    use super::MAINNET_CONSENSUS_CHAIN_SPEC;

    #[test]
    fn mainnet_fork_version_tracks_known_boundaries() {
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(0),
            [0x00, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(74_240),
            [0x01, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(144_896),
            [0x02, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(194_048),
            [0x03, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(269_568),
            [0x04, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(364_032),
            [0x05, 0x00, 0x00, 0x00]
        );
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_epoch(411_392),
            [0x06, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn mainnet_enr_fork_id_uses_current_and_next_versions() {
        let electra_epoch = 400_000;
        let fork_id = MAINNET_CONSENSUS_CHAIN_SPEC.enr_fork_id_for_epoch(electra_epoch);
        assert_eq!(&fork_id[4..8], &[0x06, 0x00, 0x00, 0x00]);
        assert_eq!(u64::from_le_bytes(fork_id[8..16].try_into().unwrap()), 411_392);

        let fulu_epoch = 441_630;
        let fork_id = MAINNET_CONSENSUS_CHAIN_SPEC.enr_fork_id_for_epoch(fulu_epoch);
        assert_eq!(&fork_id[4..8], &[0x06, 0x00, 0x00, 0x00]);
        assert_eq!(u64::from_le_bytes(fork_id[8..16].try_into().unwrap()), u64::MAX);
    }
}
