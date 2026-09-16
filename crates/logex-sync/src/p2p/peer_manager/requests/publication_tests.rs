use super::limit_tests::Fixture;
use super::*;
use alloy_consensus::Header;

fn publication_header(number: u64, marker: u8) -> Header {
    Header {
        number,
        extra_data: vec![marker].into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn publication_initial_and_identical_tuple_are_suppressed() {
    let mut fixture = Fixture::new().await;
    assert!(!fixture.manager.sync_advertised_history_range(false));
    assert!(!fixture.manager.sync_advertised_history_range(false));
    assert!(fixture.manager.sync_advertised_history_range(true));
    assert!(!fixture.manager.sync_advertised_history_range(false));
}

#[tokio::test]
async fn publication_detects_earliest_hash_regression_and_genesis_changes() {
    let mut fixture = Fixture::new().await;
    let first = publication_header(10, 1);
    let second = publication_header(11, 2);
    for header in [&first, &second] {
        fixture
            .manager
            .serve_cache
            .insert_block(header, &Default::default(), &[]);
    }
    assert!(fixture.manager.sync_advertised_history_range(false));
    assert!(!fixture.manager.sync_advertised_history_range(false));
    fixture
        .manager
        .serve_cache
        .remove_blocks(&[first.hash_slow()]);
    assert!(
        fixture.manager.sync_advertised_history_range(false),
        "earliest-only advance"
    );
    fixture
        .manager
        .serve_cache
        .insert_block(&first, &Default::default(), &[]);
    assert!(
        fixture.manager.sync_advertised_history_range(false),
        "earliest-only decrease"
    );
    let replacement = publication_header(11, 3);
    fixture
        .manager
        .serve_cache
        .insert_block(&replacement, &Default::default(), &[]);
    assert!(
        fixture.manager.sync_advertised_history_range(false),
        "same-height hash change"
    );
    fixture
        .manager
        .serve_cache
        .remove_blocks(&[replacement.hash_slow()]);
    assert!(
        fixture.manager.sync_advertised_history_range(false),
        "latest regression"
    );
    fixture
        .manager
        .serve_cache
        .remove_blocks(&[first.hash_slow()]);
    assert!(
        fixture.manager.sync_advertised_history_range(false),
        "genesis fallback"
    );
    assert!(!fixture.manager.sync_advertised_history_range(false));
}

#[tokio::test]
async fn publication_set_head_restores_unchanged_cache_availability() {
    let mut fixture = Fixture::new().await;
    let cached = publication_header(10, 1);
    fixture
        .manager
        .cache_canonical_block(&cached, &Default::default(), &[]);
    assert_eq!(fixture.poll_network_status_head(), cached.hash_slow());
    assert!(!fixture.manager.sync_advertised_history_range(false));
    // Avoid activation/dial scheduling: only queued status/range commands are
    // relevant to this contract; fixture has no connected network sessions.
    fixture.manager.network_activated = true;
    let newer = publication_header(100, 2);
    fixture.manager.set_head(Head {
        number: newer.number,
        hash: newer.hash_slow(),
        ..Default::default()
    });
    assert_eq!(fixture.manager.local_head.hash, newer.hash_slow());
    assert_eq!(
        fixture.poll_network_status_head(),
        cached.hash_slow(),
        "set_head must restore body availability after update_status overwrites the range head"
    );
    assert!(!fixture.manager.sync_advertised_history_range(false));
}
