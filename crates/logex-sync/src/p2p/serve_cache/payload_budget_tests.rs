//! Small structural cache fixtures, not authenticated chain data or throughput tests.
use super::*;
use alloy_consensus::{Eip658Value, SignableTransaction, TxLegacy, TxReceipt as _, TxType};
use alloy_primitives::{Bytes, Log, LogData, Signature, U256};

type Body = BlockBody<reth_ethereum_primitives::TransactionSigned, Header>;

fn provider_with_budget(limit: usize) -> ServeCacheProvider {
    ServeCacheProvider {
        payload_limit: u64::try_from(limit).unwrap(),
        ..ServeCacheProvider::new()
    }
}

#[derive(Clone)]
struct Fixture {
    header: Header,
    body: Body,
    receipts: Vec<ReceiptWithBloom<LogexReceipt>>,
}

fn fixture(number: u64, variable_bytes: usize) -> Fixture {
    Fixture {
        header: Header {
            number,
            extra_data: Bytes::from(vec![7; variable_bytes.min(32)]),
            ..Default::default()
        },
        body: Body {
            transactions: vec![
                TxLegacy {
                    input: Bytes::from(vec![9; variable_bytes]),
                    ..Default::default()
                }
                .into_signed(Signature::new(U256::from(1), U256::from(2), false))
                .into(),
            ],
            ommers: Vec::new(),
            withdrawals: None,
        },
        receipts: vec![ReceiptWithBloom {
            receipt: LogexReceipt {
                tx_type: TxType::Eip1559,
                status: Eip658Value::success(),
                cumulative_gas_used: 21_000,
                logs: vec![Log {
                    address: Address::ZERO,
                    data: LogData::new_unchecked(
                        vec![B256::repeat_byte(3)],
                        Bytes::from(vec![5; variable_bytes]),
                    ),
                }],
            },
            logs_bloom: Bloom::ZERO,
        }],
    }
}

// Independent encoded-byte oracle: actual encoding, not production size helpers.
// Receipts are counted individually; there is deliberately no outer-list prefix.
fn weight(f: &Fixture) -> usize {
    alloy_rlp::encode(&f.header).len()
        + alloy_rlp::encode(&f.body).len()
        + f.receipts
            .iter()
            .map(|r| {
                let mut encoded = Vec::new();
                r.receipt
                    .rlp_encode_with_bloom(&r.receipt.bloom(), &mut encoded);
                encoded.len()
            })
            .sum::<usize>()
}

fn insert(provider: &ServeCacheProvider, f: &Fixture) {
    provider.insert_block(&f.header, &f.body, &f.receipts);
    assert_accounting(provider);
}

fn assert_accounting(provider: &ServeCacheProvider) {
    let state = provider.inner.read().unwrap();
    let mut total = 0u64;
    assert_eq!(state.blocks.len(), state.number_to_hash.len());
    assert_eq!(state.blocks.len(), state.hash_to_number.len());
    assert!(state.blocks.len() <= SERVE_CACHE_BLOCK_LIMIT);
    assert_eq!(state.headers.len(), state.header_number_to_hash.len());
    assert!(state.headers.len() <= SERVE_CACHE_HEADER_LIMIT);
    for (hash, cached) in &state.blocks {
        let block = &cached.block;
        assert_eq!(*hash, block.header.hash_slow());
        assert_eq!(state.number_to_hash.get(&block.header.number), Some(hash));
        assert_eq!(state.hash_to_number.get(hash), Some(&block.header.number));
        let mut bytes =
            alloy_rlp::encode(&block.header).len() + alloy_rlp::encode(&block.body).len();
        for receipt in &cached.receipts {
            let mut encoded = Vec::new();
            receipt.rlp_encode_with_bloom(&receipt.bloom(), &mut encoded);
            bytes += encoded.len();
        }
        let bytes = u64::try_from(bytes).unwrap();
        assert_eq!(cached.payload_bytes, bytes);
        total = total.checked_add(bytes).unwrap();
        if let Some(header_hash) = state.header_number_to_hash.get(&block.header.number) {
            assert_eq!(header_hash, hash);
        }
    }
    for (number, hash) in &state.header_number_to_hash {
        let header = state.headers.get(hash).unwrap();
        assert_eq!(*number, header.number);
        assert_eq!(*hash, header.hash_slow());
    }
    assert_eq!(state.payload_bytes, total);
    assert!(total <= provider.payload_limit);
}

fn has_body(provider: &ServeCacheProvider, f: &Fixture) -> bool {
    provider
        .block_by_hash(f.header.hash_slow())
        .unwrap()
        .is_some()
}

#[test]
fn payload_budget_exact_boundary_and_one_byte_over() {
    let f = fixture(1, 31);
    let exact = provider_with_budget(weight(&f));
    insert(&exact, &f);
    assert!(has_body(&exact, &f));
    let short = provider_with_budget(weight(&f) - 1);
    insert(&short, &f);
    assert!(!has_body(&short, &f));
    assert_eq!(short.header_by_number(1).unwrap(), Some(f.header.clone()));
    assert_eq!(short.advertised_history_range(), None);
}

#[test]
fn payload_budget_variable_body_and_receipts_are_both_charged() {
    let small = fixture(1, 0);
    let mut body_large = small.clone();
    body_large.body = fixture(1, 100).body;
    let mut receipts_large = small.clone();
    receipts_large.receipts = fixture(1, 100).receipts;
    for larger in [body_large, receipts_large] {
        assert!(weight(&larger) > weight(&small));
        let provider = provider_with_budget(weight(&small));
        insert(&provider, &larger);
        assert!(!has_body(&provider, &larger));
    }
}

#[test]
fn payload_budget_evicts_multiple_oldest_and_rejects_too_old() {
    let old: Vec<_> = (1..=3).map(|n| fixture(n, 0)).collect();
    let limit = old.iter().map(weight).sum();
    let provider = provider_with_budget(limit);
    for f in &old {
        insert(&provider, f);
    }
    let newer = fixture(4, 800);
    assert!(weight(&newer) <= limit);
    assert!(weight(&newer) + weight(&old[2]) > limit);
    insert(&provider, &newer);
    assert!(!has_body(&provider, &old[0]));
    assert!(!has_body(&provider, &old[1]));
    assert!(has_body(&provider, &newer));
    let before = provider.advertised_history_range();
    insert(&provider, &old[0]);
    assert!(!has_body(&provider, &old[0]));
    assert_eq!(provider.advertised_history_range(), before);
}

#[test]
fn payload_budget_oversized_replacement_still_invalidates_old_canonical_body() {
    let old = fixture(2, 0);
    let neighbor = fixture(3, 0);
    let provider = provider_with_budget(weight(&old) + weight(&neighbor));
    insert(&provider, &old);
    insert(&provider, &neighbor);
    let replacement = fixture(2, 2000);
    insert(&provider, &replacement);
    assert!(!has_body(&provider, &old));
    assert!(!has_body(&provider, &replacement));
    assert!(has_body(&provider, &neighbor));
    assert_eq!(
        provider.header_by_number(2).unwrap(),
        Some(replacement.header.clone())
    );
    assert!(
        provider
            .receipts_by_block(old.header.hash_slow().into())
            .unwrap()
            .is_none()
    );
    assert_eq!(
        provider.advertised_history_range(),
        Some((3, 3, neighbor.header.hash_slow()))
    );
    provider.remove_blocks(&[old.header.hash_slow()]);
    assert_accounting(&provider);
    assert_eq!(
        provider.header_by_number(2).unwrap(),
        Some(replacement.header)
    );
}

#[test]
fn payload_budget_duplicate_removal_and_header_reorg_release_charge() {
    let first = fixture(1, 0);
    let second = fixture(2, 0);
    let third = fixture(3, 0);
    let provider = provider_with_budget(weight(&first) + weight(&second));
    insert(&provider, &first);
    insert(&provider, &first);
    insert(&provider, &second);
    assert!(has_body(&provider, &first));
    assert!(has_body(&provider, &second));
    provider.remove_blocks(&[first.header.hash_slow()]);
    assert_accounting(&provider);
    insert(&provider, &third);
    assert!(has_body(&provider, &second));
    assert!(has_body(&provider, &third));
    let mut replacement = second.header.clone();
    replacement.timestamp = 1;
    assert!(provider.insert_headers([replacement]));
    assert_accounting(&provider);
    insert(&provider, &first);
    assert!(has_body(&provider, &first));
    assert!(has_body(&provider, &third));
}

#[test]
fn payload_budget_descending_inserts_preserve_newer_window() {
    let entries: Vec<_> = (1..=5).map(|n| fixture(n, 0)).collect();
    let provider = provider_with_budget(weight(&entries[3]) + weight(&entries[4]));
    for f in entries.iter().rev() {
        insert(&provider, f);
    }
    for f in &entries[..3] {
        assert!(!has_body(&provider, f));
    }
    assert!(has_body(&provider, &entries[3]));
    assert!(has_body(&provider, &entries[4]));
    assert_eq!(
        provider.advertised_history_range(),
        Some((4, 5, entries[4].header.hash_slow()))
    );
}

#[test]
fn payload_budget_mixed_operations_match_small_retained_height_model() {
    let limit = weight(&fixture(1, 0)) * 3;
    let provider = provider_with_budget(limit);
    let mut retained: BTreeMap<u64, Fixture> = BTreeMap::new();
    // Repeated heights, nonmonotonic arrival, duplicates, different-size replacements,
    // removals and header-only reorgs. This model uses only independently encoded bytes.
    for step in 0..36u64 {
        let number = (step * 5) % 11 + 1;
        if step % 9 == 8 {
            if let Some(old) = retained.remove(&number) {
                provider.remove_blocks(&[old.header.hash_slow()]);
                assert_accounting(&provider);
            }
        } else if step % 7 == 6 {
            let mut header = fixture(number, 0).header;
            header.timestamp = 100 + step;
            retained.remove(&number);
            provider.insert_headers([header]);
            assert_accounting(&provider);
        } else {
            let mut f = fixture(number, (step as usize % 4) * 190);
            f.header.timestamp = step % 4;
            retained.remove(&number);
            let newer_bytes: usize = retained.range(number..).map(|(_, f)| weight(f)).sum();
            if weight(&f) + newer_bytes <= limit {
                retained.insert(number, f.clone());
                while retained.values().map(weight).sum::<usize>() > limit {
                    retained.pop_first();
                }
            }
            insert(&provider, &f);
            // Same-identity insertion must not accumulate an additional charge.
            insert(&provider, &f);
        }
        let actual: Vec<_> = provider
            .inner
            .read()
            .unwrap()
            .number_to_hash
            .iter()
            .map(|(n, h)| (*n, *h))
            .collect();
        let expected: Vec<_> = retained
            .iter()
            .map(|(n, f)| (*n, f.header.hash_slow()))
            .collect();
        assert_eq!(actual, expected, "operation {step}");
    }
}

#[test]
fn payload_budget_concurrent_admission_keeps_accounting_and_recent_entries() {
    let provider = provider_with_budget(weight(&fixture(1, 0)) * 3);
    let ready = std::sync::Barrier::new(4);
    std::thread::scope(|scope| {
        for worker in 0..4 {
            let provider = &provider;
            let ready = &ready;
            scope.spawn(move || {
                ready.wait();
                for offset in 0..8 {
                    // All 32 small fixtures have equal encoded weight. Interleaved
                    // admission can reject stale preflights, but the newest three
                    // entries must remain and each observed snapshot must balance.
                    insert(provider, &fixture(worker * 8 + offset + 1, 0));
                }
            });
        }
    });
    assert_accounting(&provider);
    assert_eq!(
        provider
            .inner
            .read()
            .unwrap()
            .number_to_hash
            .keys()
            .copied()
            .collect::<Vec<_>>(),
        vec![30, 31, 32]
    );
}
