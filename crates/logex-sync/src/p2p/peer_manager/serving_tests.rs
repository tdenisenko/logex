//! Public pinned-handler pagination controls. Typed cache fixtures exercise the
//! serving contract; they are not claimed to be executed, valid mainnet blocks.
//! No socket, network manager, discovery, remote peer or service task is used.
use super::*;
use crate::primitives::LogexReceipt;
use alloy_consensus::{Eip658Value, Header, ReceiptWithBloom, TxType};
use alloy_primitives::{Address, Bytes, Log};
use alloy_rlp::Encodable;
use reth_eth_wire::GetReceipts70;
use reth_network::eth_requests::{EthRequestHandler, IncomingEthRequest, SOFT_RESPONSE_LIMIT};
use reth_network_api::test_utils::PeersHandle;

fn receipt_with_encoded_size(size: usize) -> LogexReceipt {
    // Start with a data field exactly as large as the target, measure framing,
    // then remove only that small overhead. Both sizes stay in the same RLP
    // length-prefix class. The independent assertion below establishes size.
    let mut receipt = LogexReceipt {
        tx_type: TxType::Legacy,
        status: Eip658Value::success(),
        cumulative_gas_used: 21_000,
        logs: vec![Log::new_unchecked(
            Address::ZERO,
            Vec::new(),
            Bytes::from(vec![0x42; size]),
        )],
    };
    let overhead = receipt.length() - size;
    receipt.logs[0].data.data = Bytes::from(vec![0x42; size - overhead]);
    assert_eq!(alloy_rlp::encode(&receipt).len(), size);
    receipt
}

struct HandlerFixture {
    hashes: Vec<B256>,
    provider: Arc<ServeCacheProvider>,
    sender: mpsc::Sender<IncomingEthRequest<LogexNetworkPrimitives>>,
    handler: std::pin::Pin<Box<EthRequestHandler<Arc<ServeCacheProvider>, LogexNetworkPrimitives>>>,
}

impl HandlerFixture {
    fn new(receipts: &[LogexReceipt]) -> Self {
        Self::with_blocks(&[receipts])
    }

    fn with_blocks(blocks: &[&[LogexReceipt]]) -> Self {
        let provider = Arc::new(ServeCacheProvider::new());
        let mut hashes = Vec::new();
        for (index, receipts) in blocks.iter().enumerate() {
            let header = Header {
                number: index as u64 + 1,
                ..Default::default()
            };
            hashes.push(header.hash_slow());
            let wrapped: Vec<_> = receipts
                .iter()
                .cloned()
                .map(|receipt| ReceiptWithBloom {
                    receipt,
                    logs_bloom: Default::default(),
                })
                .collect();
            provider.insert_block(&header, &Default::default(), &wrapped);
        }
        let (sender, receiver) = mpsc::channel(1);
        // This unused handle satisfies the handler constructor. Its command
        // receiver is dropped and no peer-management work is run.
        let (peer_commands, _) = mpsc::unbounded_channel();
        let handler = Box::pin(EthRequestHandler::new(
            Arc::clone(&provider),
            PeersHandle::new(peer_commands),
            receiver,
        ));
        Self {
            provider,
            hashes,
            sender,
            handler,
        }
    }

    async fn response(&mut self, index: u64) -> reth_eth_wire::Receipts70<LogexReceipt> {
        self.response_from_block(0, index).await
    }

    async fn response_from_block(
        &mut self,
        block: usize,
        index: u64,
    ) -> reth_eth_wire::Receipts70<LogexReceipt> {
        let (response, mut receiver) = tokio::sync::oneshot::channel();
        self.sender
            .try_send(IncomingEthRequest::GetReceipts70 {
                peer_id: PeerId::repeat_byte(1),
                request: GetReceipts70 {
                    first_block_receipt_index: index,
                    block_hashes: self.hashes[block..].to_vec(),
                },
                response,
            })
            .unwrap();
        assert!(futures_util::poll!(self.handler.as_mut()).is_pending());
        receiver
            .try_recv()
            .expect("handler processed one bounded request")
            .unwrap()
    }
}

#[tokio::test]
async fn serving_eth70_oversized_first_receipt_makes_progress() {
    let receipt = receipt_with_encoded_size(SOFT_RESPONSE_LIMIT + 1);
    let mut fixture = HandlerFixture::new(std::slice::from_ref(&receipt));
    let response = fixture.response(0).await;
    assert!(
        response.receipts == vec![vec![receipt]],
        "a soft response target must not produce an empty incomplete fragment forever"
    );
    assert!(!response.last_block_incomplete);
}

#[tokio::test]
async fn serving_eth70_list_overhead_does_not_invent_remaining_receipts() {
    let receipt = receipt_with_encoded_size(SOFT_RESPONSE_LIMIT);
    let expected = vec![receipt];
    assert!(
        expected.length() > SOFT_RESPONSE_LIMIT,
        "block-list framing alone crosses the target"
    );
    let mut fixture = HandlerFixture::new(&expected);
    let response = fixture.response(0).await;
    assert!(response.receipts == vec![expected]);
    assert!(
        !response.last_block_incomplete,
        "all receipts were emitted; framing overhead cannot mean another receipt remains"
    );
}

#[tokio::test]
async fn serving_eth70_normal_pagination_preserves_exact_cursor_suffix() {
    let first = receipt_with_encoded_size(SOFT_RESPONSE_LIMIT / 2 + 128);
    let mut second = first.clone();
    second.cumulative_gas_used = 42_000;
    let mut fixture = HandlerFixture::new(&[first.clone(), second.clone()]);
    let prefix = fixture.response(0).await;
    assert!(prefix.receipts == vec![vec![first]]);
    assert!(prefix.last_block_incomplete);
    let suffix = fixture.response(1).await;
    assert!(suffix.receipts == vec![vec![second]]);
    assert!(!suffix.last_block_incomplete);
}

#[tokio::test]
async fn serving_eth70_prior_empty_complete_block_omits_unstarted_fragment() {
    let large = receipt_with_encoded_size(SOFT_RESPONSE_LIMIT + 1);
    let large_block = [large.clone()];
    let mut fixture = HandlerFixture::with_blocks(&[&[], &large_block]);
    let first = fixture.response(0).await;
    assert!(first.receipts == vec![Vec::<LogexReceipt>::new()]);
    assert!(!first.last_block_incomplete);
    // Completing an empty block advances the block cursor, not receipt index.
    let next = fixture.response_from_block(1, 0).await;
    assert!(next.receipts == vec![vec![large]]);
    assert!(!next.last_block_incomplete);
}

#[tokio::test]
async fn serving_eth70_prior_nonempty_complete_block_omits_unstarted_fragment() {
    let small = LogexReceipt::default();
    let large = receipt_with_encoded_size(SOFT_RESPONSE_LIMIT + 1);
    let first_block = [small.clone()];
    let second_block = [large.clone()];
    let mut fixture = HandlerFixture::with_blocks(&[&first_block, &second_block]);
    let first = fixture.response(0).await;
    assert!(first.receipts == vec![vec![small]]);
    assert!(!first.last_block_incomplete);
    let next = fixture.response_from_block(1, 0).await;
    assert!(next.receipts == vec![vec![large]]);
    assert!(!next.last_block_incomplete);
}

#[tokio::test]
async fn serving_eth70_resumed_large_receipt_makes_progress() {
    let large = receipt_with_encoded_size(SOFT_RESPONSE_LIMIT + 1);
    let mut fixture = HandlerFixture::new(&[LogexReceipt::default(), large.clone()]);
    let response = fixture.response(1).await;
    assert!(response.receipts == vec![vec![large]]);
    assert!(!response.last_block_incomplete);
}

#[tokio::test]
async fn serving_eth70_large_prefix_then_exact_remaining_receipt() {
    let large = receipt_with_encoded_size(SOFT_RESPONSE_LIMIT + 1);
    let later = LogexReceipt {
        cumulative_gas_used: 42_000,
        ..Default::default()
    };
    let mut fixture = HandlerFixture::new(&[large.clone(), later.clone()]);
    let prefix = fixture.response(0).await;
    assert!(prefix.receipts == vec![vec![large]]);
    assert!(prefix.last_block_incomplete);
    let suffix = fixture.response(1).await;
    assert!(suffix.receipts == vec![vec![later]]);
    assert!(!suffix.last_block_incomplete);
}

// These controls only poll a public handler over in-process channels. The
// provider's cumulative payload counter witnesses a real cache read/copy; it is
// not a claim that response bytes were sent to any network connection.
async fn cancelled_serving_request<R>(
    request: impl Fn(
        B256,
        tokio::sync::oneshot::Sender<reth_network::p2p::error::RequestResult<R>>,
    ) -> IncomingEthRequest<LogexNetworkPrimitives>,
    check_response: impl Fn(R),
) {
    enum ReceiverState {
        Dropped,
        Open,
        Closed,
    }
    let mut fixture = HandlerFixture::new(&[LogexReceipt::default()]);
    for state in [
        ReceiverState::Dropped,
        ReceiverState::Open,
        ReceiverState::Closed,
        ReceiverState::Open,
    ] {
        let before = fixture.provider.p2p_upload_snapshot().1;
        let (response, receiver) = tokio::sync::oneshot::channel();
        let mut receiver = Some(receiver);
        fixture
            .sender
            .try_send(request(fixture.hashes[0], response))
            .unwrap();
        // Cancel after enqueue but before the handler is polled, matching a
        // session closing while an otherwise valid request waits in the queue.
        match state {
            ReceiverState::Dropped => drop(receiver.take()),
            ReceiverState::Closed => receiver.as_mut().unwrap().close(),
            ReceiverState::Open => {}
        }
        assert!(futures_util::poll!(fixture.handler.as_mut()).is_pending());
        let after = fixture.provider.p2p_upload_snapshot().1;
        match state {
            ReceiverState::Open => {
                check_response(receiver.unwrap().try_recv().unwrap().unwrap());
                assert!(after > before, "live request must still read cached data");
            }
            ReceiverState::Dropped | ReceiverState::Closed => {
                assert_eq!(
                    after, before,
                    "closed response must not read/copy cached data"
                );
                if let Some(mut receiver) = receiver {
                    assert!(matches!(
                        receiver.try_recv(),
                        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
                    ));
                }
            }
        }
    }
}

#[tokio::test]
async fn serving_cancellation_skips_headers_and_preserves_live_response() {
    cancelled_serving_request(
        |hash, response| IncomingEthRequest::GetBlockHeaders {
            peer_id: PeerId::repeat_byte(1),
            request: GetBlockHeaders {
                start_block: hash.into(),
                limit: 1,
                skip: 0,
                direction: reth_eth_wire::HeadersDirection::Rising,
            },
            response,
        },
        |headers| {
            assert!(
                headers.0
                    == vec![Header {
                        number: 1,
                        ..Default::default()
                    }]
            );
        },
    )
    .await;
}

#[tokio::test]
async fn serving_cancellation_skips_bodies_and_preserves_live_response() {
    cancelled_serving_request(
        |hash, response| IncomingEthRequest::GetBlockBodies {
            peer_id: PeerId::repeat_byte(1),
            request: GetBlockBodies(vec![hash]),
            response,
        },
        |bodies| assert!(bodies.0 == vec![Default::default()]),
    )
    .await;
}

#[tokio::test]
async fn serving_cancellation_skips_eth68_receipts_and_preserves_live_response() {
    cancelled_serving_request(
        |hash, response| IncomingEthRequest::GetReceipts {
            peer_id: PeerId::repeat_byte(1),
            request: GetReceipts(vec![hash]),
            response,
        },
        |receipts| {
            assert!(receipts.0 == vec![vec![ReceiptWithBloom::from(LogexReceipt::default())]]);
        },
    )
    .await;
}

#[tokio::test]
async fn serving_cancellation_skips_eth69_receipts_and_preserves_live_response() {
    cancelled_serving_request(
        |hash, response| IncomingEthRequest::GetReceipts69 {
            peer_id: PeerId::repeat_byte(1),
            request: GetReceipts(vec![hash]),
            response,
        },
        |receipts| assert!(receipts.0 == vec![vec![LogexReceipt::default()]]),
    )
    .await;
}

#[tokio::test]
async fn serving_cancellation_skips_eth70_receipts_and_preserves_live_response() {
    cancelled_serving_request(
        |hash, response| IncomingEthRequest::GetReceipts70 {
            peer_id: PeerId::repeat_byte(1),
            request: GetReceipts70 {
                first_block_receipt_index: 0,
                block_hashes: vec![hash],
            },
            response,
        },
        |receipts| {
            assert!(receipts.receipts == vec![vec![LogexReceipt::default()]]);
            assert!(!receipts.last_block_incomplete);
        },
    )
    .await;
}
