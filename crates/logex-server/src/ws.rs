use std::sync::Arc;

use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use serde::Deserialize;
use tokio::sync::broadcast;

use logex_types::LogRow;

use crate::eth_filter::{EthFilter, RpcLog, matches_filter};
use crate::handler::AppState;

/// Capacity of the broadcast channel for new logs.
const BROADCAST_CAPACITY: usize = 4096;

/// Manages WebSocket subscriptions for live log streaming.
pub struct SubscriptionManager {
    sender: broadcast::Sender<Arc<Vec<LogRow>>>,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self { sender }
    }

    /// Notify all subscribers of new logs. Called by the ingestion pipeline.
    pub fn notify(&self, rows: &[LogRow]) {
        if rows.is_empty() || self.sender.receiver_count() == 0 {
            return;
        }
        // Ignore send errors — they just mean no active receivers
        let _ = self.sender.send(Arc::new(rows.to_vec()));
    }

    /// Create a new receiver for the broadcast channel.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Vec<LogRow>>> {
        self.sender.subscribe()
    }
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Subscription request sent by the client over WebSocket.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SubscribeRequest {
    /// Filter to apply to incoming logs (same as eth_getLogs filter).
    #[serde(default)]
    filter: EthFilter,
}

/// Handle WebSocket upgrade for `/ws`.
pub async fn handle_ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> Response {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: Arc<AppState>) {
    // Wait for the client's subscription message
    let filter = match receive_filter(&mut socket).await {
        Some(f) => f,
        None => return,
    };

    tracing::debug!("new WebSocket subscription");

    let subs = match &state.subscriptions {
        Some(s) => s,
        None => {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };

    let mut receiver = subs.subscribe();

    // Send an ack
    let ack = serde_json::json!({"status": "subscribed"});
    if socket
        .send(Message::Text(ack.to_string().into()))
        .await
        .is_err()
    {
        return;
    }

    // Stream matching logs
    loop {
        tokio::select! {
            result = receiver.recv() => {
                match result {
                    Ok(rows) => {
                        let matching: Vec<RpcLog> = rows
                            .iter()
                            .filter(|row| matches_filter(row, &filter))
                            .map(RpcLog::from)
                            .collect();

                        if matching.is_empty() {
                            continue;
                        }

                        let json = match serde_json::to_string(&matching) {
                            Ok(j) => j,
                            Err(_) => continue,
                        };

                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, "WebSocket subscriber lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
            // Check for client disconnect or ping/pong
            msg = socket.recv() => {
                match msg {
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    Some(Ok(Message::Ping(data)))
                        if socket.send(Message::Pong(data.clone())).await.is_err() =>
                    {
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    tracing::debug!("WebSocket subscription closed");
}

/// Receive and parse the initial subscription filter from the client.
async fn receive_filter(socket: &mut WebSocket) -> Option<EthFilter> {
    // Give the client 10 seconds to send their subscription
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(10), socket.recv()).await;

    match timeout {
        Ok(Some(Ok(Message::Text(text)))) => {
            match serde_json::from_str::<SubscribeRequest>(&text) {
                Ok(req) => Some(req.filter),
                Err(e) => {
                    let err = serde_json::json!({"error": format!("invalid filter: {e}")});
                    let _ = socket.send(Message::Text(err.to_string().into())).await;
                    None
                }
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, bytes};
    use logex_types::Source;

    fn make_log(addr_byte: u8, block: u64) -> LogRow {
        LogRow {
            block_number: block,
            block_hash: B256::repeat_byte(0x01),
            timestamp: 1_700_000_000,
            tx_hash: B256::repeat_byte(0x11),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(addr_byte),
            topic0: Some(B256::repeat_byte(0xDD)),
            topic1: None,
            topic2: None,
            topic3: None,
            data: bytes!("cafe"),
            data_len: 2,
            source: Source::Receipt,
        }
    }

    #[test]
    fn test_subscription_manager_no_receivers() {
        let mgr = SubscriptionManager::new();
        // Should not panic even with no receivers
        mgr.notify(&[make_log(0xAA, 100)]);
    }

    #[test]
    fn test_subscription_manager_broadcast() {
        let mgr = SubscriptionManager::new();
        let mut rx1 = mgr.subscribe();
        let mut rx2 = mgr.subscribe();

        let rows = vec![make_log(0xAA, 100), make_log(0xBB, 200)];
        mgr.notify(&rows);

        let received1 = rx1.try_recv().unwrap();
        assert_eq!(received1.len(), 2);

        let received2 = rx2.try_recv().unwrap();
        assert_eq!(received2.len(), 2);
    }

    #[test]
    fn test_subscription_manager_empty_batch() {
        let mgr = SubscriptionManager::new();
        let mut rx = mgr.subscribe();

        mgr.notify(&[]);
        assert!(rx.try_recv().is_err()); // Nothing sent
    }

    #[tokio::test]
    async fn test_subscription_filter_matching() {
        let mgr = SubscriptionManager::new();
        let mut rx = mgr.subscribe();

        let filter = EthFilter {
            address: crate::eth_filter::AddressFilter::Single(Address::repeat_byte(0xAA)),
            ..Default::default()
        };

        let rows = vec![make_log(0xAA, 100), make_log(0xBB, 200)];
        mgr.notify(&rows);

        let batch = rx.recv().await.unwrap();
        let matching: Vec<RpcLog> = batch
            .iter()
            .filter(|row| matches_filter(row, &filter))
            .map(RpcLog::from)
            .collect();

        assert_eq!(matching.len(), 1);
        assert!(
            matching[0]
                .address
                .contains(&hex::encode(Address::repeat_byte(0xAA)))
        );
    }
}
