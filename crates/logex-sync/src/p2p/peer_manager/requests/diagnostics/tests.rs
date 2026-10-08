use super::*;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};
use tracing::instrument::WithSubscriber;

#[derive(Clone, Default)]
struct Output(Arc<Mutex<Vec<u8>>>);

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Output {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
    async fn capture<T>(&self, future: impl std::future::Future<Output = T>) -> T {
        let output = self.clone();
        let subscriber = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .with_ansi(false)
                .without_time()
                .with_max_level(tracing::Level::DEBUG)
                .with_writer(move || output.clone())
                .finish(),
        );
        // With only one registered dispatcher, tracing derives a new callsite's
        // cached interest from the thread that first reaches it. A sibling test
        // without a subscriber can therefore cache `never` for our events. Keep
        // a second dispatcher alive so registration considers both subscribers,
        // while only the scoped capture receives this future's events. Creating
        // it also refreshes callsites already reached by unobserved requests.
        let _interest_guard = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        future.with_subscriber(subscriber).await
    }
}

fn bodies(
    response: oneshot::Sender<
        reth_network::p2p::error::RequestResult<
            BlockBodies<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
        >,
    >,
) -> PeerRequest<LogexNetworkPrimitives> {
    PeerRequest::GetBlockBodies {
        request: GetBlockBodies(vec![B256::repeat_byte(9)]),
        response,
    }
}

#[tokio::test]
async fn diagnostics_distinguish_admitted_receiver_drop_and_later_response() {
    let output = Output::default();
    output
        .capture(async {
            let peer = PeerId::repeat_byte(7);
            let (tx, mut rx) = mpsc::channel(1);
            let sender = PeerRequestSender::new(peer, tx);
            let mut first = Box::pin(request_with_sender::<RawBlockBodies, _, _>(
                &sender,
                &bodies,
                Duration::from_secs(10),
            ));
            assert!(futures_util::poll!(&mut first).is_pending());
            let PeerRequest::GetBlockBodies { response, .. } = rx.try_recv().unwrap() else {
                panic!()
            };
            drop(first);
            // The session still owns the first response even though its consumer left.
            assert!(response.send(Ok(BlockBodies(Vec::new()))).is_err());
            let second = request_with_sender::<RawBlockBodies, _, _>(
                &sender,
                &bodies,
                Duration::from_secs(10),
            );
            let deliver = async {
                let PeerRequest::GetBlockBodies { response, .. } = rx.recv().await.unwrap() else {
                    panic!()
                };
                response.send(Ok(BlockBodies(Vec::new()))).unwrap();
            };
            let (result, ()) = tokio::join!(second, deliver);
            assert!(result.unwrap().is_empty());
        })
        .await;
    let text = output.text();
    let ids: Vec<_> = text
        .lines()
        .filter(|line| line.contains("execution exchange queued"))
        .map(|line| {
            line.split("exchange_id=")
                .nth(1)
                .unwrap()
                .split_whitespace()
                .next()
                .unwrap()
        })
        .collect();
    assert_eq!(ids.len(), 2);
    assert_ne!(ids[0], ids[1]);
    assert!(
        text.lines()
            .any(|line| line.contains(&format!("exchange_id={}", ids[0]))
                && line.contains("local_receiver_dropped")
                && line.contains("session_admitted=true"))
    );
    assert!(
        text.lines()
            .any(|line| line.contains(&format!("exchange_id={}", ids[1]))
                && line.contains("response_received"))
    );
    assert!(!text.contains("execution exchange failed"));
}

#[tokio::test(start_paused = true)]
async fn diagnostics_queue_timeout_is_not_reported_as_admitted_or_cancelled() {
    let output = Output::default();
    output
        .capture(async {
            let (tx, mut rx) = mpsc::channel(1);
            tx.send(bodies(oneshot::channel().0)).await.unwrap();
            let sender = PeerRequestSender::new(PeerId::repeat_byte(7), tx);
            let result = request_with_sender::<RawBlockBodies, _, _>(
                &sender,
                &bodies,
                Duration::from_secs(1),
            )
            .await;
            assert!(matches!(
                result,
                Err(RequestAttempt::Request(
                    reth_network::p2p::error::RequestError::Timeout
                ))
            ));
            assert!(rx.try_recv().is_ok());
            assert!(rx.try_recv().is_err());
        })
        .await;
    let text = output.text();
    assert!(
        text.lines()
            .any(|line| line.contains("execution exchange failed")
                && line.contains("session_admitted=false")
                && line.contains("Timeout"))
    );
    assert!(!text.contains("local_receiver_dropped"));
    assert!(!text.contains("session_admitted=true"));
}

#[tokio::test]
async fn diagnostics_drop_before_admission_never_submits_the_request() {
    let output = Output::default();
    output
        .capture(async {
            let (tx, mut rx) = mpsc::channel(1);
            tx.send(bodies(oneshot::channel().0)).await.unwrap();
            let sender = PeerRequestSender::new(PeerId::repeat_byte(7), tx);
            let mut request = Box::pin(request_with_sender::<RawBlockBodies, _, _>(
                &sender,
                &bodies,
                Duration::from_secs(10),
            ));
            assert!(futures_util::poll!(&mut request).is_pending());
            drop(request);
            assert!(rx.try_recv().is_ok());
            assert!(rx.try_recv().is_err());
        })
        .await;
    assert!(
        output
            .text()
            .lines()
            .any(|line| line.contains("local_receiver_dropped")
                && line.contains("session_admitted=false"))
    );
}

#[test]
fn diagnostics_payload_scope_is_bounded_and_preserves_hash_order_and_offset() {
    let a = B256::repeat_byte(1);
    let b = B256::repeat_byte(2);
    let first = Scope::hashes("receipts70", &[a, b], Some(3)).to_string();
    assert_ne!(
        first,
        Scope::hashes("receipts70", &[b, a], Some(3)).to_string()
    );
    assert_ne!(
        first,
        Scope::hashes("receipts70", &[a, b], Some(4)).to_string()
    );
    assert!(
        Scope::hashes("receipts70", &vec![a; 100_000], Some(3))
            .to_string()
            .len()
            < 160
    );
    let headers = Scope::Headers(GetBlockHeaders {
        start_block: BlockHashOrNumber::Number(123),
        limit: 8,
        skip: 2,
        direction: reth_eth_wire::HeadersDirection::Falling,
    });
    let text = headers.to_string();
    for expected in ["123", "count=8", "skip=2", "Falling"] {
        assert!(text.contains(expected));
    }
}

#[tokio::test]
async fn diagnostics_capture_after_unobserved_exchange_on_another_thread() {
    let output = Output::default();
    output
        .capture(async {
            // Exercise the production callsites from a subscriber-less thread first.
            // Run this test alone in a fresh process to cover cold registration too.
            std::thread::spawn(|| {
                let request = bodies(oneshot::channel().0);
                let _exchange =
                    Exchange::new(PeerId::repeat_byte(7), &request, Duration::from_secs(10));
            })
            .join()
            .unwrap();
            let request = bodies(oneshot::channel().0);
            let _exchange =
                Exchange::new(PeerId::repeat_byte(8), &request, Duration::from_secs(10));
        })
        .await;
    let text = output.text();
    assert_eq!(text.lines().count(), 2, "{text:?}");
    assert!(text.contains("execution exchange queued"), "{text:?}");
    assert!(text.contains("local_receiver_dropped"), "{text:?}");
}
