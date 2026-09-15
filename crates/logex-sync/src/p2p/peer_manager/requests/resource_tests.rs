//! Header-resource controls using ordinary typed receipts, not EVM execution
//! or committed-block fixtures. Original absent-policy failures are retained
//! separately; the guarded merger now receives explicit header context.
use super::*;
use alloy_consensus::{Eip658Value, Header, TxType};

fn receipt(index: u64) -> LogexReceipt {
    LogexReceipt {
        tx_type: TxType::Eip1559,
        status: Eip658Value::success(),
        cumulative_gas_used: index,
        logs: Vec::new(),
    }
}

// Independent explicit arithmetic over the actual retained objects; this must
// not call the eventual production resource-weight helper.
fn retained_weight(merged: &ReceiptBatch) -> u128 {
    let mut weight = 0u128;
    for block in merged {
        for wrapped in block {
            weight += 21_000;
            for log in &wrapped.receipt.logs {
                weight += 375;
                weight += 375 * log.topics().len() as u128;
                weight += 8 * log.data.data.len() as u128;
            }
        }
    }
    weight
}

fn fragment(index: u64) -> Receipts70<LogexReceipt> {
    Receipts70 {
        last_block_incomplete: true,
        receipts: vec![vec![receipt(index)]],
    }
}

#[test]
fn receipt_resource_two_fragments_at_proposed_limit_are_retained() {
    let header = Header {
        gas_used: 21_000,
        ..Default::default()
    };
    let proposed_limit = 2 * u128::from(header.gas_used);
    let contexts = [ReceiptRequestContext::from_header(&header)];
    let mut partial_weight = 0;
    assert_eq!(proposed_limit, 42_000);
    let mut merged = ReceiptBatch::new();
    let mut blooms = ReceiptBloomCache::default();
    let mut cursor = (0, 0);
    for index in 1..=2 {
        cursor = merge_receipts70_response(
            &mut merged,
            cursor.0,
            cursor.1,
            fragment(index),
            &contexts,
            &mut blooms,
            &mut partial_weight,
        )
        .expect("positive cursor progress within proposed resource bound");
        assert_eq!(cursor, (0, index));
        assert_eq!(retained_weight(&merged), 21_000 * u128::from(index));
    }
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].len(), 2);
    assert_eq!(merged[0][0].receipt, receipt(1));
    assert_eq!(merged[0][1].receipt, receipt(2));
    assert_eq!(retained_weight(&merged), proposed_limit);
}

#[test]
fn receipt_resource_third_fragment_must_not_exceed_header_derived_limit() {
    let header = Header {
        gas_used: 21_000,
        ..Default::default()
    };
    let proposed_limit = 2 * u128::from(header.gas_used);
    let contexts = [ReceiptRequestContext::from_header(&header)];
    let mut partial_weight = 0;
    let mut merged = ReceiptBatch::new();
    let mut blooms = ReceiptBloomCache::default();
    let mut cursor = (0, 0);
    for index in 1..=2 {
        cursor = merge_receipts70_response(
            &mut merged,
            cursor.0,
            cursor.1,
            fragment(index),
            &contexts,
            &mut blooms,
            &mut partial_weight,
        )
        .unwrap();
    }
    assert_eq!(cursor, (0, 2));
    assert_eq!(retained_weight(&merged), proposed_limit);
    let result = merge_receipts70_response(
        &mut merged,
        cursor.0,
        cursor.1,
        fragment(3),
        &contexts,
        &mut blooms,
        &mut partial_weight,
    );
    assert!(matches!(
        result,
        Err(Receipts70MergeError::ResourceExceeded(_))
    ));
    assert_eq!(partial_weight, proposed_limit);
    assert_eq!(
        retained_weight(&merged),
        proposed_limit,
        "rejected fragment must not mutate retained prefix"
    );
}

fn context(number: u64, gas: u64) -> ReceiptRequestContext {
    ReceiptRequestContext::from_header(&Header {
        number,
        gas_used: gas,
        ..Default::default()
    })
}

#[test]
fn receipt_resource_multiblock_failure_is_atomic_and_partial_weight_resets() {
    let contexts = [context(1, 21_000), context(2, 10_500)];
    let mut merged = ReceiptBatch::new();
    let mut blooms = ReceiptBloomCache::default();
    let mut partial = 0;
    let cursor = merge_receipts70_response(
        &mut merged,
        0,
        0,
        fragment(1),
        &contexts,
        &mut blooms,
        &mut partial,
    )
    .unwrap();
    assert_eq!(cursor, (0, 1));
    assert_eq!(partial, 21_000);
    let retained_before = merged.clone();
    // First block's remaining allowance cannot subsidize the second block.
    let bad = Receipts70 {
        last_block_incomplete: true,
        receipts: vec![vec![receipt(2)], vec![receipt(3), receipt(4)]],
    };
    let error = merge_receipts70_response(
        &mut merged,
        cursor.0,
        cursor.1,
        bad,
        &contexts,
        &mut blooms,
        &mut partial,
    )
    .unwrap_err();
    assert!(matches!(error, Receipts70MergeError::ResourceExceeded(_)));
    assert_eq!(merged, retained_before);
    assert_eq!(partial, 21_000);
    let good = Receipts70 {
        last_block_incomplete: true,
        receipts: vec![vec![receipt(2)], vec![receipt(3)]],
    };
    let cursor = merge_receipts70_response(
        &mut merged,
        cursor.0,
        cursor.1,
        good,
        &contexts,
        &mut blooms,
        &mut partial,
    )
    .unwrap();
    assert_eq!(cursor, (1, 1));
    assert_eq!(
        partial, 21_000,
        "completed first block's weight must not leak into next block"
    );
    assert_eq!(retained_weight(&merged), 63_000);
    let result = merge_receipts70_response(
        &mut merged,
        cursor.0,
        cursor.1,
        fragment(4),
        &contexts,
        &mut blooms,
        &mut partial,
    );
    assert!(matches!(
        result,
        Err(Receipts70MergeError::ResourceExceeded(_))
    ));
    assert_eq!(retained_weight(&merged), 63_000);
    assert_eq!(partial, 21_000);
}

#[test]
fn receipt_resource_complete_reply_clears_partial_and_shape_precedes_weight() {
    let contexts = [context(1, 21_000), context(2, 0)];
    let mut merged = ReceiptBatch::new();
    let mut blooms = ReceiptBloomCache::default();
    let mut partial = 0;
    let cursor = merge_receipts70_response(
        &mut merged,
        0,
        0,
        fragment(1),
        &contexts,
        &mut blooms,
        &mut partial,
    )
    .unwrap();
    let complete = Receipts70 {
        last_block_incomplete: false,
        receipts: vec![vec![receipt(2)], Vec::new()],
    };
    assert_eq!(
        merge_receipts70_response(
            &mut merged,
            cursor.0,
            cursor.1,
            complete,
            &contexts,
            &mut blooms,
            &mut partial
        )
        .unwrap(),
        (2, 0)
    );
    assert_eq!(partial, 0);
    assert!(merged[1].is_empty());
    let mut empty = ReceiptBatch::new();
    let oversized_shape = Receipts70 {
        last_block_incomplete: false,
        receipts: vec![vec![receipt(1)]; 3],
    };
    assert!(matches!(
        merge_receipts70_response(
            &mut empty,
            0,
            0,
            oversized_shape,
            &contexts,
            &mut blooms,
            &mut partial
        ),
        Err(Receipts70MergeError::ResponseOverflow)
    ));
    assert!(empty.is_empty());
    assert_eq!(partial, 0);
}

fn assert_wire_hashes(request: &PeerRequest<LogexNetworkPrimitives>, expected: &[B256]) {
    let actual = match request {
        PeerRequest::GetReceipts { request, .. } | PeerRequest::GetReceipts69 { request, .. } => {
            &request.0
        }
        PeerRequest::GetReceipts70 { request, .. } => &request.block_hashes,
        _ => panic!("expected receipt request"),
    };
    assert_eq!(actual, expected);
}

fn answer_overweight(request: PeerRequest<LogexNetworkPrimitives>) {
    assert_wire_hashes(&request, &[context(1, 21_000).block_hash()]);
    let receipts = vec![receipt(1), receipt(2), receipt(3)];
    match request {
        PeerRequest::GetReceipts { response, .. } => {
            let receipts = receipts
                .into_iter()
                .map(|receipt| alloy_consensus::ReceiptWithBloom {
                    receipt,
                    logs_bloom: Default::default(),
                })
                .collect();
            response.send(Ok(Receipts(vec![receipts]))).unwrap();
        }
        PeerRequest::GetReceipts69 { response, .. } => {
            response.send(Ok(Receipts69(vec![receipts]))).unwrap();
        }
        PeerRequest::GetReceipts70 { response, .. } => {
            response
                .send(Ok(Receipts70 {
                    last_block_incomplete: false,
                    receipts: vec![receipts],
                }))
                .unwrap();
        }
        _ => panic!("expected receipt request"),
    }
}

#[tokio::test(start_paused = true)]
async fn receipt_resource_public_versions_reject_without_peer_blame() {
    for version in [EthVersion::Eth68, EthVersion::Eth69, EthVersion::Eth70] {
        let mut fixture = limit_tests::Fixture::new().await;
        for peer in fixture.manager.peers.values_mut() {
            peer.version = version;
        }
        let snapshots: Vec<_> = fixture
            .manager
            .peer_order
            .iter()
            .map(|id| {
                let p = &fixture.manager.peers[id];
                (
                    *id,
                    p.consecutive_timeouts,
                    p.receipt_request_limit,
                    p.is_serving,
                )
            })
            .collect();
        let mut future = Box::pin(fixture.manager.get_receipts_prefer_peers_with_limits(
            vec![context(1, 21_000)],
            1,
            &[],
            Duration::from_secs(10),
            1,
        ));
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        let mut requests = limit_tests::take_requests(&mut fixture.receivers);
        assert_eq!(requests.len(), 1);
        answer_overweight(requests.pop().unwrap().1);
        match futures_util::poll!(future.as_mut()) {
            std::task::Poll::Ready(result) => assert!(result.is_err()),
            std::task::Poll::Pending => {
                panic!("resource guard should finish without another wire request")
            }
        }
        drop(future);
        assert!(limit_tests::take_requests(&mut fixture.receivers).is_empty());
        for (id, timeouts, limit, serving) in snapshots {
            let peer = &fixture.manager.peers[&id];
            assert_eq!(peer.consecutive_timeouts, timeouts);
            assert_eq!(peer.receipt_request_limit, limit);
            assert_eq!(peer.is_serving, serving);
            assert!(peer.receipt_paused_until.is_none());
            assert!(peer.receipt_quarantined_until.is_none());
        }
        assert!(fixture.manager.receipt_quarantined_peers.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn receipt_resource_plan_versions_enforce_context_before_returning() {
    for version in [EthVersion::Eth68, EthVersion::Eth69, EthVersion::Eth70] {
        let (mut peer, mut receiver) = ownership_tests::test_session(PeerId::repeat_byte(1));
        peer.version = version;
        let plan = ownership_tests::test_plan(&peer);
        let mut future = Box::pin(
            plan.request_receipts_until_complete(peer.sender.peer_id, vec![context(1, 21_000)]),
        );
        assert!(futures_util::poll!(future.as_mut()).is_pending());
        answer_overweight(receiver.try_recv().expect("one receipt request"));
        match futures_util::poll!(future.as_mut()) {
            std::task::Poll::Ready(result) => assert!(matches!(
                result,
                Err(ChunkFailureKind::Request(
                    RequestAttempt::ReceiptResourcesExceeded(_)
                ))
            )),
            std::task::Poll::Pending => panic!("plan resource guard should reject immediately"),
        }
    }
}

fn answer_legacy_blocks(
    request: PeerRequest<LogexNetworkPrimitives>,
    blocks: Vec<Vec<LogexReceipt>>,
) {
    match request {
        PeerRequest::GetReceipts { response, .. } => {
            let blocks = blocks
                .into_iter()
                .map(|block| {
                    block
                        .into_iter()
                        .map(|receipt| alloy_consensus::ReceiptWithBloom {
                            receipt,
                            logs_bloom: Default::default(),
                        })
                        .collect()
                })
                .collect();
            response.send(Ok(Receipts(blocks))).unwrap();
        }
        PeerRequest::GetReceipts69 { response, .. } => {
            response.send(Ok(Receipts69(blocks))).unwrap();
        }
        _ => panic!("expected legacy receipt response"),
    }
}

#[test]
fn receipt_resource_legacy_shape_precedes_raw_and_bloomed_weight() {
    let blocks = [context(1, 0)];
    // Even the first block is overweight. Extra outer blocks must still retain
    // their protocol-shape classification rather than become neutral capacity.
    let raw = vec![vec![receipt(1)], Vec::new()];
    assert!(matches!(
        check_raw_receipt_resources(&blocks, &raw),
        Err(RequestAttempt::ReceiptResponseOverflow { returned: 2 })
    ));
    let bloomed = raw
        .into_iter()
        .map(|block| {
            block
                .into_iter()
                .map(|receipt| alloy_consensus::ReceiptWithBloom {
                    receipt,
                    logs_bloom: Default::default(),
                })
                .collect()
        })
        .collect();
    assert!(matches!(
        check_bloomed_receipt_resources(&blocks, &bloomed),
        Err(RequestAttempt::ReceiptResponseOverflow { returned: 2 })
    ));
}

#[tokio::test(start_paused = true)]
async fn receipt_resource_legacy_collectors_preserve_overflow_failure_policy() {
    for version in [EthVersion::Eth68, EthVersion::Eth69] {
        for use_plan in [false, true] {
            let mut fixture = limit_tests::Fixture::new().await;
            let id = PeerId::repeat_byte(1);
            fixture.manager.peers.get_mut(&id).unwrap().version = version;
            let plan = ownership_tests::test_plan(&fixture.manager.peers[&id]);
            let mut future = Box::pin(async {
                if use_plan {
                    plan.request_receipts_until_complete(id, vec![context(1, 0)])
                        .await
                } else {
                    fixture
                        .manager
                        .request_receipts_until_complete(id, vec![context(1, 0)])
                        .await
                }
            });
            assert!(futures_util::poll!(future.as_mut()).is_pending());
            let mut requests = limit_tests::take_requests(&mut fixture.receivers);
            assert_eq!(requests.len(), 1);
            let request = requests.pop().unwrap().1;
            assert_wire_hashes(&request, &[context(1, 0).block_hash()]);
            answer_legacy_blocks(request, vec![vec![receipt(1)], Vec::new()]);
            let kind = match futures_util::poll!(future.as_mut()) {
                std::task::Poll::Ready(result) => result.unwrap_err(),
                std::task::Poll::Pending => panic!("outer overflow must fail immediately"),
            };
            assert!(matches!(kind, ChunkFailureKind::Incomplete { returned: 2 }));
            let failure = ChunkRequestFailure {
                peer_id: id,
                role: ChunkRequestRole::Receipts,
                requested: 1,
                kind,
            };
            assert!(chunk_failure_disables_role_peer(&failure));
            drop(future);
            assert!(
                fixture.manager.on_request_error(
                    id,
                    PeerRequestKind::Receipts,
                    &RequestAttempt::ReceiptResponseOverflow { returned: 2 }
                ),
                "direct sequential handler must retain bad-protocol removal decision"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn receipt_resource_legacy_positive_partial_aligns_remaining_context() {
    for version in [EthVersion::Eth68, EthVersion::Eth69] {
        for use_plan in [false, true] {
            let mut fixture = limit_tests::Fixture::new().await;
            let id = PeerId::repeat_byte(1);
            fixture.manager.peers.get_mut(&id).unwrap().version = version;
            let plan = ownership_tests::test_plan(&fixture.manager.peers[&id]);
            let contexts = vec![context(1, 0), context(2, 21_000)];
            let mut future = Box::pin(async {
                if use_plan {
                    plan.request_receipts_until_complete(id, contexts.clone())
                        .await
                } else {
                    fixture
                        .manager
                        .request_receipts_until_complete(id, contexts.clone())
                        .await
                }
            });
            assert!(futures_util::poll!(future.as_mut()).is_pending());
            let mut requests = limit_tests::take_requests(&mut fixture.receivers);
            assert_eq!(requests.len(), 1);
            let request = requests.pop().unwrap().1;
            assert_wire_hashes(
                &request,
                &[contexts[0].block_hash(), contexts[1].block_hash()],
            );
            answer_legacy_blocks(request, vec![Vec::new()]);
            assert!(futures_util::poll!(future.as_mut()).is_pending());
            let mut requests = limit_tests::take_requests(&mut fixture.receivers);
            assert_eq!(requests.len(), 1);
            let request = requests.pop().unwrap().1;
            assert_wire_hashes(&request, &[contexts[1].block_hash()]);
            answer_legacy_blocks(request, vec![vec![receipt(1)]]);
            let receipts = match futures_util::poll!(future.as_mut()) {
                std::task::Poll::Ready(result) => result.unwrap(),
                std::task::Poll::Pending => panic!("positive partial completion must finish"),
            };
            assert_eq!(receipts.len(), 2);
            assert!(receipts[0].is_empty());
            assert_eq!(receipts[1].len(), 1);
            assert_eq!(receipts[1][0].receipt, receipt(1));
        }
    }
}
