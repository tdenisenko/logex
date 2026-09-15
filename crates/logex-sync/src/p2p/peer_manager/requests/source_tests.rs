use super::limit_tests::{Fixture, hashes, receipt_contexts, take_requests};
use super::*;

/// Ordinary decoded receipt marker, not an executed transaction or committed block.
fn marker_receipt(hash: B256) -> LogexReceipt {
    LogexReceipt {
        cumulative_gas_used: u64::from(hash[0]),
        ..Default::default()
    }
}

#[tokio::test(start_paused = true)]
async fn standalone_receipts_preserve_out_of_order_supplier_sources() {
    let mut fixture = Fixture::new().await;
    let requested = hashes();
    let future = fixture
        .manager
        .get_receipts_prefer_peers(receipt_contexts(&requested), 1, &[]);
    tokio::pin!(future);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 2);
    let mut suppliers = HashMap::new();
    // Later range completes first. The returned result must still be in hash order.
    requests.sort_by_key(|(_, request)| match request {
        PeerRequest::GetReceipts69 { request, .. } => request.0[0],
        _ => panic!("expected ETH69 receipt request"),
    });
    while let Some((index, request)) = requests.pop() {
        answer_marked_receipts(index, request, &mut suppliers);
        if !requests.is_empty() {
            // Drive the collector while the earlier range is still unanswered.
            assert!(futures_util::poll!(future.as_mut()).is_pending());
        }
    }
    let receipts = future.await.unwrap();
    assert_eq!(receipts.len(), requested.len());
    assert_eq!(suppliers.len(), requested.len());
    assert_eq!(suppliers.values().copied().collect::<HashSet<_>>().len(), 2);
    for (hash, (_, block_receipts)) in requested.iter().zip(&receipts) {
        assert_eq!(block_receipts.len(), 1);
        assert_eq!(block_receipts[0].receipt, marker_receipt(*hash));
    }
    for (hash, (reported_peer, _)) in requested.iter().zip(&receipts) {
        assert_eq!(
            *reported_peer, suppliers[hash],
            "returned source for hash {hash} must be its supplier"
        );
    }
}

fn answer_marked_receipts(
    index: usize,
    request: PeerRequest<LogexNetworkPrimitives>,
    suppliers: &mut HashMap<B256, PeerId>,
) {
    let PeerRequest::GetReceipts69 { request, response } = request else {
        panic!("expected ETH69 receipt request");
    };
    let supplier = PeerId::repeat_byte(index as u8 + 1);
    for hash in &request.0 {
        assert!(suppliers.insert(*hash, supplier).is_none());
    }
    response
        .send(Ok(Receipts69(
            request
                .0
                .into_iter()
                .map(|hash| vec![marker_receipt(hash)])
                .collect(),
        )))
        .unwrap();
}

#[tokio::test(start_paused = true)]
async fn standalone_receipts_keep_retry_winner_and_completed_chunk_sources() {
    let mut fixture = Fixture::new().await;
    let requested = hashes();
    let future = fixture
        .manager
        .get_receipts(receipt_contexts(&requested), 1);
    tokio::pin!(future);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 2);
    requests.sort_by_key(|(_, request)| match request {
        PeerRequest::GetReceipts69 { request, .. } => request.0[0],
        _ => panic!("expected ETH69 receipt request"),
    });
    let (failed_index, failed) = requests.remove(0);
    let PeerRequest::GetReceipts69 { request, response } = failed else {
        panic!("expected ETH69 receipt request");
    };
    let failed_hashes = request.0;
    response
        .send(Err(reth_network::p2p::error::RequestError::Timeout))
        .unwrap();
    let mut suppliers = HashMap::new();
    let (completed_index, completed) = requests.pop().unwrap();
    answer_marked_receipts(completed_index, completed, &mut suppliers);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut retries = take_requests(&mut fixture.receivers);
    assert_eq!(retries.len(), 1);
    let (winner_index, retry) = retries.pop().unwrap();
    assert_ne!(winner_index, failed_index);
    assert_ne!(winner_index, completed_index);
    let PeerRequest::GetReceipts69 { request, .. } = &retry else {
        panic!("expected ETH69 receipt request");
    };
    assert_eq!(request.0, failed_hashes);
    answer_marked_receipts(winner_index, retry, &mut suppliers);
    let receipts = future.await.unwrap();
    assert_eq!(receipts.len(), requested.len());
    for (hash, (peer, block_receipts)) in requested.iter().zip(receipts) {
        assert_eq!(peer, suppliers[hash]);
        assert_eq!(block_receipts.len(), 1);
        assert_eq!(block_receipts[0].receipt, marker_receipt(*hash));
    }
}

#[tokio::test(start_paused = true)]
async fn standalone_eth69_partial_prefix_keeps_single_supplier_and_empty_sets() {
    let mut fixture = Fixture::new().await;
    let requested = hashes();
    let future = fixture.manager.get_receipts_prefer_peers_with_limits(
        receipt_contexts(&requested),
        1,
        &[],
        Duration::from_secs(2),
        1,
    );
    tokio::pin!(future);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 1);
    let (index, first) = requests.pop().unwrap();
    let PeerRequest::GetReceipts69 { request, response } = first else {
        panic!("expected ETH69 receipt request");
    };
    assert_eq!(request.0, requested);
    // A complete empty inner set is a block, not an empty outer response.
    response.send(Ok(Receipts69(vec![Vec::new(); 32]))).unwrap();
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 1);
    let (next_index, next) = requests.pop().unwrap();
    assert_eq!(next_index, index);
    let PeerRequest::GetReceipts69 { request, response } = next else {
        panic!("expected ETH69 receipt request");
    };
    assert_eq!(request.0, requested[32..]);
    response
        .send(Ok(Receipts69(
            request
                .0
                .into_iter()
                .map(|hash| vec![marker_receipt(hash)])
                .collect(),
        )))
        .unwrap();
    let receipts = future.await.unwrap();
    assert_eq!(receipts.len(), requested.len());
    for (offset, (peer, block_receipts)) in receipts.into_iter().enumerate() {
        assert_eq!(peer, PeerId::repeat_byte(index as u8 + 1));
        if offset < 32 {
            assert!(block_receipts.is_empty());
        } else {
            assert_eq!(block_receipts.len(), 1);
            assert_eq!(block_receipts[0].receipt, marker_receipt(requested[offset]));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn standalone_eth70_continuation_keeps_supplier_and_empty_block() {
    let mut fixture = Fixture::new().await;
    for peer in fixture.manager.peers.values_mut() {
        peer.version = EthVersion::Eth70;
    }
    let requested = vec![B256::repeat_byte(1), B256::repeat_byte(2)];
    let future = fixture
        .manager
        .get_receipts(receipt_contexts(&requested), 1);
    tokio::pin!(future);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 1);
    let (index, first) = requests.pop().unwrap();
    let PeerRequest::GetReceipts70 { request, response } = first else {
        panic!("expected ETH70 receipt request");
    };
    assert_eq!(request.block_hashes, requested);
    assert_eq!(request.first_block_receipt_index, 0);
    response
        .send(Ok(Receipts70 {
            last_block_incomplete: true,
            receipts: vec![Vec::new(), vec![marker_receipt(B256::repeat_byte(3))]],
        }))
        .unwrap();
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let mut requests = take_requests(&mut fixture.receivers);
    assert_eq!(requests.len(), 1);
    let (next_index, next) = requests.pop().unwrap();
    assert_eq!(next_index, index);
    let PeerRequest::GetReceipts70 { request, response } = next else {
        panic!("expected ETH70 receipt request");
    };
    assert_eq!(request.block_hashes, requested[1..]);
    assert_eq!(request.first_block_receipt_index, 1);
    response
        .send(Ok(Receipts70 {
            last_block_incomplete: false,
            receipts: vec![vec![marker_receipt(B256::repeat_byte(4))]],
        }))
        .unwrap();
    let receipts = future.await.unwrap();
    assert_eq!(receipts.len(), 2);
    let supplier = PeerId::repeat_byte(index as u8 + 1);
    assert_eq!(receipts[0].0, supplier);
    assert!(receipts[0].1.is_empty());
    assert_eq!(receipts[1].0, supplier);
    assert_eq!(
        receipts[1]
            .1
            .iter()
            .map(|receipt| receipt.receipt.cumulative_gas_used)
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
}

#[tokio::test(start_paused = true)]
async fn standalone_receipt_empty_requests_have_no_fabricated_supplier() {
    let mut fixture = Fixture::new().await;
    assert!(
        fixture
            .manager
            .get_receipts(Vec::new(), 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .manager
            .get_receipts_prefer_peers(Vec::new(), 1, &[])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        fixture
            .manager
            .get_receipts_prefer_peers_with_limits(Vec::new(), 1, &[], Duration::from_secs(2), 1)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(take_requests(&mut fixture.receivers).is_empty());
}
