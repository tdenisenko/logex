use super::*;
use std::task::Poll;

fn header_request() -> HeadersRequest {
    HeadersRequest::falling(BlockHashOrNumber::Number(1), 1)
}

fn fill_request_queue(sender: &PeerRequestSender<PeerRequest<LogexNetworkPrimitives>>) {
    let (response, _receiver) = oneshot::channel();
    sender
        .to_session_tx
        .try_send(PeerRequest::GetBlockHeaders {
            request: GetBlockHeaders {
                start_block: BlockHashOrNumber::Number(0),
                limit: 0,
                skip: 0,
                direction: reth_eth_wire::HeadersDirection::Rising,
            },
            response,
        })
        .unwrap();
}

fn assert_timeout<T>(result: Poll<std::result::Result<T, RequestAttempt>>) {
    assert!(matches!(
        result,
        Poll::Ready(Err(RequestAttempt::Request(
            reth_network::p2p::error::RequestError::Timeout
        )))
    ));
}

#[tokio::test(start_paused = true)]
async fn header_deadline_covers_waiting_for_queue_space() {
    let (peer, mut requests) = ownership_tests::test_session(PeerId::repeat_byte(1));
    fill_request_queue(&peer.sender);
    let mut request = Box::pin(request_headers_with_sender(
        peer.sender.clone(),
        header_request(),
    ));
    assert!(futures_util::poll!(&mut request).is_pending());

    tokio::time::advance(REQUEST_TIMEOUT + Duration::from_millis(1)).await;
    assert_timeout(futures_util::poll!(&mut request));
    drop(request);
    assert!(requests.try_recv().is_ok());
    assert!(matches!(
        requests.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test(start_paused = true)]
async fn header_deadline_is_not_restarted_after_queue_admission() {
    let (peer, mut requests) = ownership_tests::test_session(PeerId::repeat_byte(2));
    fill_request_queue(&peer.sender);
    let mut request = Box::pin(request_headers_with_sender(
        peer.sender.clone(),
        header_request(),
    ));
    assert!(futures_util::poll!(&mut request).is_pending());
    let queue_wait = REQUEST_TIMEOUT / 2;
    tokio::time::advance(queue_wait).await;
    assert!(requests.try_recv().is_ok());
    assert!(futures_util::poll!(&mut request).is_pending());
    let PeerRequest::GetBlockHeaders { response, .. } = requests.try_recv().unwrap() else {
        panic!("expected the queued header request");
    };

    tokio::time::advance(REQUEST_TIMEOUT - queue_wait + Duration::from_millis(1)).await;
    assert_timeout(futures_util::poll!(&mut request));
    assert!(response.is_closed());
}

#[tokio::test(start_paused = true)]
async fn body_plan_deadline_covers_queue_and_response_with_its_custom_budget() {
    let request_timeout = Duration::from_secs(2);
    for admit_after_wait in [false, true] {
        let id = PeerId::repeat_byte(3);
        let (peer, mut requests) = ownership_tests::test_session(id);
        let plan = ownership_tests::test_plan(&peer);
        fill_request_queue(&peer.sender);
        let mut request = Box::pin(plan.request_bodies(id, vec![B256::ZERO], request_timeout));
        assert!(futures_util::poll!(&mut request).is_pending());
        tokio::time::advance(request_timeout / 2).await;
        let response = if admit_after_wait {
            assert!(requests.try_recv().is_ok());
            assert!(futures_util::poll!(&mut request).is_pending());
            let PeerRequest::GetBlockBodies { response, .. } = requests.try_recv().unwrap() else {
                panic!("expected the queued body request");
            };
            Some(response)
        } else {
            None
        };
        tokio::time::advance(request_timeout / 2 + Duration::from_millis(1)).await;
        assert_timeout(futures_util::poll!(&mut request));
        if let Some(response) = response {
            assert!(response.is_closed());
        } else {
            assert!(requests.try_recv().is_ok());
            assert!(matches!(
                requests.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn request_deadline_preserves_response_and_error_mapping() {
    for outcome in 0..4 {
        let (peer, mut requests) = ownership_tests::test_session(PeerId::repeat_byte(4));
        let mut request = Box::pin(request_headers_with_sender(
            peer.sender.clone(),
            header_request(),
        ));
        assert!(futures_util::poll!(&mut request).is_pending());
        let PeerRequest::GetBlockHeaders { response, .. } = requests.try_recv().unwrap() else {
            panic!("expected a header request");
        };
        match outcome {
            0 => {
                let headers = vec![alloy_consensus::Header::default()];
                response.send(Ok(BlockHeaders(headers.clone()))).unwrap();
                assert!(
                    matches!(futures_util::poll!(&mut request), Poll::Ready(Ok(actual)) if actual == headers)
                );
            }
            1 => {
                response
                    .send(Err(reth_network::p2p::error::RequestError::BadResponse))
                    .unwrap();
                assert!(matches!(
                    futures_util::poll!(&mut request),
                    Poll::Ready(Err(RequestAttempt::Request(
                        reth_network::p2p::error::RequestError::BadResponse
                    )))
                ));
            }
            2 => {
                drop(response);
                assert!(matches!(
                    futures_util::poll!(&mut request),
                    Poll::Ready(Err(RequestAttempt::Disconnected))
                ));
            }
            _ => {
                tokio::time::advance(REQUEST_TIMEOUT + Duration::from_millis(1)).await;
                assert_timeout(futures_util::poll!(&mut request));
                assert!(response.is_closed());
            }
        }
    }

    let (peer, requests) = ownership_tests::test_session(PeerId::repeat_byte(5));
    drop(requests);
    assert!(matches!(
        request_headers_with_sender(peer.sender, header_request()).await,
        Err(RequestAttempt::Disconnected)
    ));
}

#[tokio::test(start_paused = true)]
async fn canceled_request_drops_admission_or_response_without_closing_the_session() {
    for queued in [false, true] {
        let (peer, mut requests) = ownership_tests::test_session(PeerId::repeat_byte(6));
        if queued {
            fill_request_queue(&peer.sender);
        }
        let mut request = Box::pin(request_headers_with_sender(
            peer.sender.clone(),
            header_request(),
        ));
        assert!(futures_util::poll!(&mut request).is_pending());
        drop(request);
        let PeerRequest::GetBlockHeaders { response, .. } = requests.try_recv().unwrap() else {
            panic!("expected a header request");
        };
        assert!(response.is_closed());
        assert!(matches!(
            requests.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));

        let mut next = Box::pin(request_headers_with_sender(
            peer.sender.clone(),
            header_request(),
        ));
        assert!(futures_util::poll!(&mut next).is_pending());
        let PeerRequest::GetBlockHeaders { response, .. } = requests.try_recv().unwrap() else {
            panic!("expected the next header request");
        };
        response.send(Ok(BlockHeaders(Vec::new()))).unwrap();
        assert!(
            matches!(futures_util::poll!(&mut next), Poll::Ready(Ok(headers)) if headers.is_empty())
        );
    }
}

#[tokio::test(start_paused = true)]
async fn header_candidate_rotation_continues_after_full_queues_time_out() {
    let mut candidates = Vec::new();
    let mut occupied_queues = Vec::new();
    for index in 0..REVERSE_HEADER_PAGE_PARALLEL_CANDIDATES {
        let id = PeerId::repeat_byte(index as u8);
        let (peer, requests) = ownership_tests::test_session(id);
        fill_request_queue(&peer.sender);
        candidates.push((id, peer.sender));
        occupied_queues.push(requests);
    }
    let available = PeerId::repeat_byte(10);
    let (peer, mut requests) = ownership_tests::test_session(available);
    candidates.push((available, peer.sender));
    let mut page = Box::pin(request_header_page_from_candidates(
        7,
        header_request(),
        candidates,
    ));
    assert!(futures_util::poll!(&mut page).is_pending());
    tokio::time::advance(REQUEST_TIMEOUT + Duration::from_millis(1)).await;
    assert!(futures_util::poll!(&mut page).is_pending());
    let PeerRequest::GetBlockHeaders { response, .. } = requests.try_recv().unwrap() else {
        panic!("expected the next candidate request");
    };
    let headers = vec![alloy_consensus::Header::default()];
    response.send(Ok(BlockHeaders(headers.clone()))).unwrap();
    let result = page.await;
    assert_eq!(result.page_index, 7);
    let (source, actual, _) = result.success.unwrap();
    assert_eq!(source, available);
    assert_eq!(actual, headers);
    assert_eq!(
        result.failures.len(),
        REVERSE_HEADER_PAGE_PARALLEL_CANDIDATES
    );
    for (_, failure) in result.failures {
        assert!(matches!(
            failure,
            RequestAttempt::Request(reth_network::p2p::error::RequestError::Timeout)
        ));
    }
    for mut requests in occupied_queues {
        assert!(requests.try_recv().is_ok());
        assert!(requests.try_recv().is_err());
    }
}
