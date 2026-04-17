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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobScheduleEntry {
    pub epoch: u64,
    pub max_blobs_per_block: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobParameters {
    pub epoch: u64,
    pub max_blobs_per_block: u64,
}

/// Mainnet consensus constants that LogEx needs for peer interop and light-client verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsensusChainSpec {
    pub genesis_time: u64,
    pub genesis_block_root: B256,
    pub genesis_validators_root: B256,
    pub genesis_fork_version: [u8; 4],
    pub fork_schedule: &'static [ScheduledFork],
    pub electra_max_blobs_per_block: u64,
    pub blob_schedule: &'static [BlobScheduleEntry],
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
        let base_digest = self.fork_data_root(self.fork_version_for_epoch(epoch));
        let blob_parameters = self.blob_parameters_for_epoch(epoch);
        let mut hasher = Sha256::new();
        hasher.update(blob_parameters.epoch.to_le_bytes());
        hasher.update(blob_parameters.max_blobs_per_block.to_le_bytes());
        let blob_hash = hasher.finalize();

        let mut fork_digest = [0u8; 4];
        for (index, byte) in fork_digest.iter_mut().enumerate() {
            *byte = base_digest[index] ^ blob_hash[index];
        }
        fork_digest
    }

    pub fn plain_fork_digest_for_version(self, version: [u8; 4]) -> [u8; 4] {
        let base_digest = self.fork_data_root(version);
        [base_digest[0], base_digest[1], base_digest[2], base_digest[3]]
    }

    pub fn fork_version_for_digest(self, digest: [u8; 4]) -> Option<[u8; 4]> {
        if digest == self.plain_fork_digest_for_version(self.genesis_fork_version) {
            return Some(self.genesis_fork_version);
        }

        for scheduled in self.fork_schedule {
            if digest == self.plain_fork_digest_for_version(scheduled.version) {
                return Some(scheduled.version);
            }
        }

        if digest == self.fork_digest_for_epoch(0) {
            return Some(self.fork_version_for_epoch(0));
        }

        for scheduled in self.fork_schedule {
            if digest == self.fork_digest_for_epoch(scheduled.epoch) {
                return Some(scheduled.version);
            }
        }

        for scheduled in self.blob_schedule {
            if digest == self.fork_digest_for_epoch(scheduled.epoch) {
                return Some(self.fork_version_for_epoch(scheduled.epoch));
            }
        }

        None
    }

    pub fn enr_fork_id_for_epoch(self, epoch: u64) -> [u8; 16] {
        let current_version = self.fork_version_for_epoch(epoch);
        let next_fork_version = self
            .next_regular_fork_for_epoch(epoch)
            .map(|scheduled| scheduled.version)
            .unwrap_or(current_version);
        let next_fork_epoch = self
            .next_scheduled_epoch_after(epoch)
            .unwrap_or(FAR_FUTURE_EPOCH);
        let mut fork_id = [0u8; 16];
        fork_id[0..4].copy_from_slice(&self.fork_digest_for_epoch(epoch));
        fork_id[4..8].copy_from_slice(&next_fork_version);
        fork_id[8..16].copy_from_slice(&next_fork_epoch.to_le_bytes());
        fork_id
    }

    pub fn next_fork_digest_for_epoch(self, epoch: u64) -> [u8; 4] {
        self.next_scheduled_epoch_after(epoch)
            .map(|next_epoch| self.fork_digest_for_epoch(next_epoch))
            .unwrap_or([0u8; 4])
    }

    pub fn blob_parameters_for_epoch(self, epoch: u64) -> BlobParameters {
        for scheduled in self.blob_schedule.iter().rev() {
            if epoch >= scheduled.epoch {
                return BlobParameters {
                    epoch: scheduled.epoch,
                    max_blobs_per_block: scheduled.max_blobs_per_block,
                };
            }
        }

        BlobParameters {
            epoch: self
                .fork_schedule
                .iter()
                .find(|scheduled| scheduled.version == [0x05, 0x00, 0x00, 0x00])
                .map(|scheduled| scheduled.epoch)
                .unwrap_or(0),
            max_blobs_per_block: self.electra_max_blobs_per_block,
        }
    }

    fn fork_data_root(self, version: [u8; 4]) -> [u8; 32] {
        let mut version_chunk = [0u8; 32];
        version_chunk[0..4].copy_from_slice(&version);
        let mut hasher = Sha256::new();
        hasher.update(version_chunk);
        hasher.update(self.genesis_validators_root.as_slice());
        hasher.finalize().into()
    }

    fn next_regular_fork_for_epoch(self, epoch: u64) -> Option<ScheduledFork> {
        self.fork_schedule
            .iter()
            .copied()
            .find(|scheduled| scheduled.epoch > epoch)
    }

    fn next_scheduled_epoch_after(self, epoch: u64) -> Option<u64> {
        let next_regular_epoch = self
            .next_regular_fork_for_epoch(epoch)
            .map(|fork| fork.epoch);
        let next_blob_epoch = self
            .blob_schedule
            .iter()
            .map(|scheduled| scheduled.epoch)
            .find(|scheduled_epoch| *scheduled_epoch > epoch);

        match (next_regular_epoch, next_blob_epoch) {
            (Some(regular), Some(blob)) => Some(regular.min(blob)),
            (Some(regular), None) => Some(regular),
            (None, Some(blob)) => Some(blob),
            (None, None) => None,
        }
    }
}

pub const MAINNET_CONSENSUS_CHAIN_SPEC: ConsensusChainSpec = ConsensusChainSpec {
    genesis_time: 1_606_824_023,
    genesis_block_root: b256!("4d611d5b93fdab69013a7f0a2f961caca0c853f87cfe9595fe50038163079360"),
    genesis_validators_root: b256!(
        "4b363db94e286120d76eb905340fdd4e54bfe9f06bf33ff6cf5ad27f511bfe95"
    ),
    genesis_fork_version: [0x00, 0x00, 0x00, 0x00],
    electra_max_blobs_per_block: 9,
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
    blob_schedule: &[
        BlobScheduleEntry {
            epoch: 412_672,
            max_blobs_per_block: 15,
        },
        BlobScheduleEntry {
            epoch: 419_072,
            max_blobs_per_block: 21,
        },
    ],
};

#[cfg(test)]
mod tests {
    use alloy_primitives::hex;

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
        assert_eq!(
            u64::from_le_bytes(fork_id[8..16].try_into().unwrap()),
            411_392
        );

        let fulu_epoch = 411_392;
        let fork_id = MAINNET_CONSENSUS_CHAIN_SPEC.enr_fork_id_for_epoch(fulu_epoch);
        assert_eq!(&fork_id[4..8], &[0x06, 0x00, 0x00, 0x00]);
        assert_eq!(
            u64::from_le_bytes(fork_id[8..16].try_into().unwrap()),
            412_672
        );

        let post_bpo_epoch = 441_630;
        let fork_id = MAINNET_CONSENSUS_CHAIN_SPEC.enr_fork_id_for_epoch(post_bpo_epoch);
        assert_eq!(&fork_id[4..8], &[0x06, 0x00, 0x00, 0x00]);
        assert_eq!(
            u64::from_le_bytes(fork_id[8..16].try_into().unwrap()),
            u64::MAX
        );
    }

    #[test]
    fn mainnet_fork_digest_tracks_blob_parameter_only_forks() {
        let pre_bpo = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(412_671);
        let bpo1 = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(412_672);
        let bpo2 = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(419_072);
        let current = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(441_630);

        assert_ne!(pre_bpo, bpo1);
        assert_ne!(bpo1, bpo2);
        assert_eq!(current, bpo2);
        assert_eq!(hex::encode(current), "8c9f62fe");
    }

    #[test]
    fn mainnet_fork_version_lookup_accepts_plain_and_blob_shift_digests() {
        let simple_fulu =
            MAINNET_CONSENSUS_CHAIN_SPEC.plain_fork_digest_for_version([0x06, 0x00, 0x00, 0x00]);
        let shifted_fulu = MAINNET_CONSENSUS_CHAIN_SPEC.fork_digest_for_epoch(441_630);

        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_digest(simple_fulu),
            Some([0x06, 0x00, 0x00, 0x00])
        );
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.fork_version_for_digest(shifted_fulu),
            Some([0x06, 0x00, 0x00, 0x00])
        );
    }

    #[test]
    fn mainnet_next_fork_digest_zeroes_after_last_scheduled_fork() {
        assert_eq!(
            MAINNET_CONSENSUS_CHAIN_SPEC.next_fork_digest_for_epoch(441_630),
            [0u8; 4]
        );
    }
}
