//! Snapshot controls against the exact upstream type source compiled in an
//! isolated test module; no sockets or network tasks. Reth keeps session private.
use super::range_snapshot_types::BlockRangeInfo;
use alloy_primitives::B256;
use reth_eth_wire::BlockRangeUpdate;
use std::sync::{Arc, Barrier};

fn published_ranges() -> [BlockRangeUpdate; 2] {
    [
        BlockRangeUpdate {
            earliest: 1,
            latest: 10,
            latest_hash: B256::repeat_byte(1),
        },
        BlockRangeUpdate {
            earliest: 20,
            latest: 30,
            latest_hash: B256::repeat_byte(2),
        },
    ]
}

#[test]
fn serving_range_snapshot_sequential_updates_are_shared() {
    let [first, second] = published_ranges();
    let state = BlockRangeInfo::new(first.earliest, first.latest, first.latest_hash);
    let clone = state.clone();
    assert_eq!(clone.to_message(), first);
    assert_eq!(clone.range(), 1..=10);
    assert!(clone.contains(5));
    assert!(!clone.contains(20));
    state.update(second.earliest, second.latest, second.latest_hash);
    assert_eq!(clone.to_message(), second);
    assert_eq!(clone.range(), 20..=30);
    assert_eq!(clone.earliest(), 20);
    assert_eq!(clone.latest(), 30);
    assert_eq!(clone.latest_hash(), second.latest_hash);
    assert!(!clone.has_full_history());
    clone.update(0, first.latest, first.latest_hash);
    assert!(state.has_full_history());
}

#[test]
fn serving_range_snapshot_concurrent_public_reads_are_coherent() {
    // Bounded probabilistic race control, not deterministic scheduling proof.
    // Source-level proof is separate: old update stores earliest/latest/hash
    // independently, and old range/to_message load them independently.
    const ROUNDS: usize = 10_000;
    let published = published_ranges();
    let state = BlockRangeInfo::new(
        published[0].earliest,
        published[0].latest,
        published[0].latest_hash,
    );
    let barrier = Arc::new(Barrier::new(2));
    let writer_state = state.clone();
    let writer_barrier = Arc::clone(&barrier);
    let writer = std::thread::spawn(move || {
        let choices = published_ranges();
        writer_barrier.wait();
        for round in 0..ROUNDS {
            let next = &choices[(round + 1) % 2];
            writer_state.update(next.earliest, next.latest, next.latest_hash);
        }
    });
    let mut invalid_message = None;
    let mut invalid_range = None;
    barrier.wait();
    for _ in 0..ROUNDS {
        let message = state.to_message();
        if !published.contains(&message) && invalid_message.is_none() {
            invalid_message = Some(message);
        }
        let range = state.range();
        if range != (1..=10) && range != (20..=30) && invalid_range.is_none() {
            invalid_range = Some(range);
        }
    }
    // Keep all work bounded and join before reporting any observed violation.
    writer.join().unwrap();
    assert!(
        invalid_message.is_none() && invalid_range.is_none(),
        "every individual snapshot must match a complete publication: message={invalid_message:?}, range={invalid_range:?}"
    );
}
