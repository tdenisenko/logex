//! Actual collector/public-API controls using the dormant local channel fixture.
use super::limit_tests::{Fixture, receipt_contexts, take_requests};
use super::*;
use std::task::Poll;

type Request = PeerRequest<LogexNetworkPrimitives>;

fn hashes() -> Vec<B256> {
    (1..=4).map(B256::repeat_byte).collect()
}

fn take_one(receivers: &mut [mpsc::Receiver<Request>]) -> (usize, Request) {
    let mut requests = take_requests(receivers);
    assert_eq!(requests.len(), 1);
    requests.pop().unwrap()
}

fn requested_hashes(request: &Request) -> &[B256] {
    match request {
        Request::GetBlockBodies { request, .. } => &request.0,
        Request::GetReceipts { request, .. } | Request::GetReceipts69 { request, .. } => &request.0,
        Request::GetReceipts70 { request, .. } => &request.block_hashes,
        _ => panic!("unexpected request"),
    }
}

fn reply(request: Request, count: usize) {
    // Empty receipt sets are ordinary valid transport fixtures; block content
    // validation is outside this response-cardinality and cleanup control.
    match request {
        Request::GetBlockBodies { response, .. } => {
            response
                .send(Ok(BlockBodies(vec![Default::default(); count])))
                .unwrap();
        }
        Request::GetReceipts { response, .. } => {
            response
                .send(Ok(Receipts(vec![Vec::new(); count])))
                .unwrap();
        }
        Request::GetReceipts69 { response, .. } => {
            response
                .send(Ok(Receipts69(vec![Vec::new(); count])))
                .unwrap();
        }
        Request::GetReceipts70 { response, .. } => {
            response
                .send(Ok(Receipts70 {
                    last_block_incomplete: false,
                    receipts: vec![Vec::new(); count],
                }))
                .unwrap();
        }
        _ => panic!("unexpected request"),
    }
}

async fn tail_overflow(use_plan: bool, bodies: bool, version: EthVersion, returned: usize) {
    let mut fixture = Fixture::new().await;
    let id = PeerId::repeat_byte(1);
    fixture.manager.peers.get_mut(&id).unwrap().version = version;
    let plan = ownership_tests::test_plan(&fixture.manager.peers[&id]);
    let requested = hashes();
    let contexts = receipt_contexts(&requested); // One million gas per block.
    let mut future = Box::pin(async {
        match (use_plan, bodies) {
            (true, true) => plan
                .request_bodies_until_complete(id, requested.clone())
                .await
                .map(|_| ()),
            (false, true) => fixture
                .manager
                .request_bodies_until_complete(id, requested.clone())
                .await
                .map(|_| ()),
            (true, false) => plan
                .request_receipts_until_complete(id, contexts.clone())
                .await
                .map(|_| ()),
            (false, false) => fixture
                .manager
                .request_receipts_until_complete(id, contexts.clone())
                .await
                .map(|_| ()),
        }
    });
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let (index, first) = take_one(&mut fixture.receivers);
    assert_eq!(index, 0);
    assert_eq!(requested_hashes(&first), requested);
    reply(first, 2);
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let (index, tail) = take_one(&mut fixture.receivers);
    assert_eq!(index, 0);
    assert_eq!(requested_hashes(&tail), &requested[2..]);
    reply(tail, returned);
    let kind = match futures_util::poll!(future.as_mut()) {
        Poll::Ready(result) => result.expect_err("tail exceeds its two requested blocks"),
        Poll::Pending => panic!("malformed tail must finish the attempt"),
    };
    drop(future);
    assert!(
        matches!(kind, ChunkFailureKind::ResponseOverflow { requested: 2, returned: actual } if actual == returned)
    );
    let failure = ChunkRequestFailure {
        peer_id: id,
        role: if bodies {
            ChunkRequestRole::Bodies
        } else {
            ChunkRequestRole::Receipts
        },
        requested: 4, // Original chunk telemetry must not replace tail shape context.
        kind,
    };
    assert!(chunk_failure_disables_role_peer(&failure));
    let mut dead = HashSet::new();
    fixture
        .manager
        .apply_parallel_chunk_failures("test tail", vec![failure], &mut dead);
    assert!(
        dead.contains(&id),
        "overflowing tail must retain bad-protocol removal, even when returned <= original chunk size"
    );
}

macro_rules! tail_test {
    ($name:ident, $plan:expr, $bodies:expr, $version:ident, $returned:expr) => {
        #[tokio::test(start_paused = true)]
        async fn $name() {
            tail_overflow($plan, $bodies, EthVersion::$version, $returned).await;
        }
    };
}
tail_test!(manager_body_tail_three, false, true, Eth69, 3);
tail_test!(manager_body_tail_four, false, true, Eth69, 4);
tail_test!(plan_body_tail_three, true, true, Eth69, 3);
tail_test!(plan_body_tail_four, true, true, Eth69, 4);
tail_test!(manager_eth68_tail_three, false, false, Eth68, 3);
tail_test!(manager_eth68_tail_four, false, false, Eth68, 4);
tail_test!(plan_eth68_tail_three, true, false, Eth68, 3);
tail_test!(plan_eth68_tail_four, true, false, Eth68, 4);
tail_test!(manager_eth69_tail_three, false, false, Eth69, 3);
tail_test!(manager_eth69_tail_four, false, false, Eth69, 4);
tail_test!(plan_eth69_tail_three, true, false, Eth69, 3);
tail_test!(plan_eth69_tail_four, true, false, Eth69, 4);

async fn successful_receipt_retry_cleans_dead_peer(version: EthVersion) {
    let mut fixture = Fixture::new().await;
    let first_id = PeerId::repeat_byte(1);
    for peer in fixture.manager.peers.values_mut() {
        peer.version = version;
    }
    let first = fixture.manager.peers.get_mut(&first_id).unwrap();
    first.consecutive_timeouts = 7;
    first.receipt_blocks_per_sec = 1_000.0;
    let requested = hashes();
    let mut future = Box::pin(fixture.manager.get_receipts_prefer_peers_with_limits(
        receipt_contexts(&requested),
        1,
        &[],
        Duration::from_secs(2),
        2,
    ));
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let (first_index, pending) = take_one(&mut fixture.receivers);
    assert_eq!(
        first_index, 0,
        "high measured rate selects the threshold-near peer first"
    );
    assert_eq!(requested_hashes(&pending), requested);
    // Hold its response sender alive so this is a real request deadline, not a
    // disconnected channel. The second supplier succeeds in the same public call.
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(futures_util::poll!(future.as_mut()).is_pending());
    let (winner_index, winner) = take_one(&mut fixture.receivers);
    assert_ne!(winner_index, first_index);
    assert_eq!(requested_hashes(&winner), requested);
    reply(winner, 4);
    let result = match futures_util::poll!(future.as_mut()) {
        Poll::Ready(result) => result.expect("second supplier completes"),
        Poll::Pending => panic!("complete second response must resolve public request"),
    };
    drop(future);
    drop(pending);
    assert_eq!(result.len(), 4);
    let winner_id = PeerId::repeat_byte((winner_index + 1) as u8);
    assert!(
        result
            .iter()
            .all(|(source, receipts)| *source == winner_id && receipts.is_empty())
    );
    assert!(fixture.manager.peers.contains_key(&winner_id));
    assert!(
        !fixture.manager.peers.contains_key(&first_id),
        "successful retry must not skip earlier timeout-threshold cleanup"
    );
}

#[tokio::test(start_paused = true)]
async fn eth68_success_cleans_prior_dead_peer() {
    successful_receipt_retry_cleans_dead_peer(EthVersion::Eth68).await;
}
#[tokio::test(start_paused = true)]
async fn eth69_success_cleans_prior_dead_peer() {
    successful_receipt_retry_cleans_dead_peer(EthVersion::Eth69).await;
}
#[tokio::test(start_paused = true)]
async fn eth70_success_cleans_prior_dead_peer() {
    successful_receipt_retry_cleans_dead_peer(EthVersion::Eth70).await;
}
