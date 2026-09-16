//! Serving-contract fixtures use synthetic headers and empty bodies, not EVM validation.
use alloy_consensus::{BlockBody, Header};
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::B256;
use reth_chainspec::MAINNET;
use reth_storage_api::{BlockReader, ReceiptProvider};

use super::state::advertised_status_range;
use crate::p2p::serve_cache::ServeCacheProvider;

fn header(number: u64) -> Header {
    Header {
        number,
        ..Default::default()
    }
}

fn assert_servable_endpoints(provider: &ServeCacheProvider, range: (u64, u64, B256)) {
    let (earliest, latest, latest_hash) = range;
    assert!(earliest <= latest);
    for id in [
        BlockHashOrNumber::Number(earliest),
        BlockHashOrNumber::Number(latest),
        BlockHashOrNumber::Hash(latest_hash),
    ] {
        let block = provider
            .block(id)
            .unwrap()
            .expect("advertised endpoint needs a body");
        assert!(
            provider.receipts_by_block(id).unwrap().is_some(),
            "advertised endpoint needs receipts"
        );
        if block.header.number == latest {
            assert_eq!(block.header.hash_slow(), latest_hash);
        }
    }
}

fn assert_genesis_fallback(provider: &ServeCacheProvider) {
    let range = advertised_status_range(provider.advertised_history_range());
    assert_servable_endpoints(provider, range);
    assert_eq!(range, (0, 0, MAINNET.genesis_hash()));
}

#[test]
fn advertisement_empty_cache_claims_only_servable_genesis() {
    assert_genesis_fallback(&ServeCacheProvider::new());
}

#[test]
fn advertisement_header_only_cache_does_not_claim_bodies() {
    let provider = ServeCacheProvider::new();
    let header = header(100);
    provider.insert_headers([header.clone()]);
    assert!(provider.advertised_history_range().is_none());
    assert!(
        provider
            .block(BlockHashOrNumber::Hash(header.hash_slow()))
            .unwrap()
            .is_none()
    );
    assert_genesis_fallback(&provider);
}

#[test]
fn advertisement_final_body_removal_returns_to_servable_genesis() {
    let provider = ServeCacheProvider::new();
    let header = header(100);
    provider.insert_block(&header, &BlockBody::default(), &[]);
    let range = advertised_status_range(provider.advertised_history_range());
    assert_servable_endpoints(&provider, range);
    assert_eq!(range, (100, 100, header.hash_slow()));
    provider.remove_blocks(&[header.hash_slow()]);
    assert!(provider.advertised_history_range().is_none());
    assert_genesis_fallback(&provider);
}

#[test]
fn advertisement_cached_window_matches_provider() {
    let provider = ServeCacheProvider::new();
    for number in [7, 8, 9] {
        provider.insert_block(&header(number), &BlockBody::default(), &[]);
    }
    let range = advertised_status_range(provider.advertised_history_range());
    assert_eq!(range, (7, 9, header(9).hash_slow()));
    assert_servable_endpoints(&provider, range);
}

#[test]
fn advertisement_invalid_window_falls_back_to_servable_genesis() {
    let provider = ServeCacheProvider::new();
    for invalid in [
        Some((11, 9, B256::repeat_byte(9))),
        Some((9, 9, B256::ZERO)),
    ] {
        let range = advertised_status_range(invalid);
        assert_servable_endpoints(&provider, range);
        assert_eq!(range, (0, 0, MAINNET.genesis_hash()));
    }
}
