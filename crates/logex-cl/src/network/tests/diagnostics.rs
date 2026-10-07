use super::*;
use std::io::Write;

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

#[tokio::test]
async fn diagnostics_correlate_ownership_release_late_reply_and_current_failure() {
    let temp = TempDir::new().unwrap();
    let (mut network, _) = request_lifecycle_fixture(&temp);
    let peer = PeerId::random();
    let output = Output::default();
    let writer = output.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_env_filter("info,logex_requests=debug")
        .with_writer(move || writer.clone())
        .finish();
    let (old, current) = tracing::subscriber::with_default(subscriber, || {
        let kind = RpcRequestKind::Status;
        network.ensure_request(peer, kind);
        let old = *network.pending_requests.keys().next().unwrap();
        network.clear_pending_requests_for_peer(peer);
        network.handle_rpc_response(
            kind,
            peer,
            old.request_id,
            Eth2RpcResponse::Status(network.local_status_message()),
        );
        assert!(network.last_rpc_failure.is_none());
        assert!(network.peer_failures.is_empty());
        network.ensure_request(peer, kind);
        let current = *network.pending_requests.keys().next().unwrap();
        network.handle_rpc_event(
            kind,
            request_response::Event::OutboundFailure {
                peer,
                connection_id: libp2p::swarm::ConnectionId::new_unchecked(1),
                request_id: current.request_id,
                error: request_response::OutboundFailure::Timeout,
            },
        );
        assert!(network.pending_requests.is_empty());
        assert_eq!(network.request_failures.status, 1);
        (old, current)
    });
    assert_ne!(old, current);
    let text = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
    let old_id = format!("request_id={:?}", old.request_id);
    let current_id = format!("request_id={:?}", current.request_id);
    assert!(
        text.lines()
            .any(|line| line.contains("peer_state_cleared") && line.contains(&old_id))
    );
    assert!(
        text.lines()
            .any(|line| line.contains("unknown request") && line.contains(&old_id))
    );
    assert!(
        text.lines()
            .any(|line| line.contains("outbound_failure") && line.contains(&current_id))
    );
    assert!(
        text.lines()
            .any(|line| line.contains("consensus RPC request failed")
                && line.contains(&current_id)
                && line.contains(&peer.to_string()))
    );
    let failure = network.last_rpc_failure.unwrap();
    let observed: u64 = failure
        .strip_prefix("observed_unix_ms=")
        .unwrap()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert!(observed > 0 && observed <= unix_time_millis());
    assert!(failure.contains("request=status"));
    assert!(failure.contains(&current_id));
}

#[test]
fn diagnostics_retained_failure_has_observation_time_and_keeps_detail() {
    let before = unix_time_millis();
    let text = timestamped_diagnostic("peer=recorded request=metadata failure=closed".into());
    let (time, detail) = text.split_once(' ').unwrap();
    let at: u64 = time
        .strip_prefix("observed_unix_ms=")
        .unwrap()
        .parse()
        .unwrap();
    assert!(at >= before && at <= unix_time_millis());
    assert_eq!(detail, "peer=recorded request=metadata failure=closed");
}
