//! Range advertisement change detection, called only on the existing epoch tick.
use reth_eth_wire::BlockRangeUpdate;

pub(super) fn should_send_range_update(
    last_sent: Option<&BlockRangeUpdate>,
    current: &BlockRangeUpdate,
) -> bool {
    last_sent != Some(current)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;

    use super::*;

    fn range(earliest: u64, latest: u64, hash: u8) -> BlockRangeUpdate {
        BlockRangeUpdate { earliest, latest, latest_hash: B256::repeat_byte(hash) }
    }

    #[test]
    fn range_update_first_and_unchanged_tuple() {
        let current = range(50, 100, 1);
        assert!(should_send_range_update(None, &current));
        assert!(!should_send_range_update(Some(&current), &current));
    }

    #[test]
    fn range_update_full_epoch_forward_progress() {
        assert!(should_send_range_update(Some(&range(50, 100, 1)), &range(50, 132, 2)));
    }

    #[test]
    fn range_update_earliest_changes_without_head_progress() {
        let previous = range(50, 100, 1);
        assert!(should_send_range_update(Some(&previous), &range(60, 100, 1)));
        assert!(should_send_range_update(Some(&previous), &range(40, 100, 1)));
    }

    #[test]
    fn range_update_same_height_reorg() {
        assert!(should_send_range_update(Some(&range(50, 100, 1)), &range(50, 100, 2)));
    }

    #[test]
    fn range_update_regression_and_genesis_fallback() {
        let previous = range(50, 100, 1);
        assert!(should_send_range_update(Some(&previous), &range(50, 90, 2)));
        assert!(should_send_range_update(Some(&previous), &range(0, 0, 3)));
    }

    #[test]
    fn range_update_sub_epoch_change_is_not_indefinitely_hidden() {
        assert!(should_send_range_update(Some(&range(50, 100, 1)), &range(50, 101, 2)));
    }
}
