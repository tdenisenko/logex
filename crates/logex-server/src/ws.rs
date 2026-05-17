use std::collections::HashSet;
use std::sync::{Arc, LazyLock};

use alloy_primitives::{Address, B256, keccak256};
use axum::extract::State;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::response::Response;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use logex_types::LogRow;

use crate::eth_filter::{EthFilter, RpcLog, matches_filter, parse_address};
use crate::handler::AppState;

/// Capacity of the broadcast channel for new logs.
const BROADCAST_CAPACITY: usize = 4096;
static ERC20_TRANSFER_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256(b"Transfer(address,address,uint256)"));

/// Manages WebSocket subscriptions for live log streaming.
#[derive(Clone)]
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
    /// Subscription mode. Defaults to the existing raw log stream.
    #[serde(default, rename = "type")]
    subscription_type: SubscriptionKind,
    /// Filter to apply to incoming logs (same as eth_getLogs filter).
    #[serde(default)]
    filter: EthFilter,
    /// Wallet addresses to watch for ERC20 transfers. Matches sender or recipient.
    #[serde(default, alias = "walletAddresses", deserialize_with = "address_vec")]
    addresses: Vec<Address>,
    /// Optional token contract allow-list for ERC20 transfer subscriptions.
    #[serde(default, deserialize_with = "address_vec")]
    token_addresses: Vec<Address>,
    /// Optional raw uint256 lower bound for ERC20 transfer amount data.
    #[serde(default, alias = "minAmountRaw", deserialize_with = "amount_bound")]
    min_amount: Option<[u8; 32]>,
    /// Optional raw uint256 upper bound for ERC20 transfer amount data.
    #[serde(default, alias = "maxAmountRaw", deserialize_with = "amount_bound")]
    max_amount: Option<[u8; 32]>,
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum SubscriptionKind {
    #[default]
    Logs,
    Erc20Transfers,
}

#[derive(Debug)]
enum Subscription {
    Logs(EthFilter),
    Erc20Transfers(Erc20TransferSubscription),
}

#[derive(Debug)]
struct Erc20TransferSubscription {
    wallet_topics: HashSet<B256>,
    token_addresses: HashSet<Address>,
    min_amount: Option<[u8; 32]>,
    max_amount: Option<[u8; 32]>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Erc20TransferNotification {
    #[serde(rename = "type")]
    notification_type: &'static str,
    token_address: String,
    from: String,
    to: String,
    raw_amount: String,
    block_number: u64,
    block_hash: String,
    timestamp: u64,
    transaction_hash: String,
    transaction_index: u32,
    log_index: u32,
    removed: bool,
}

impl SubscribeRequest {
    fn into_subscription(self) -> Result<Subscription, String> {
        match self.subscription_type {
            SubscriptionKind::Logs => Ok(Subscription::Logs(self.filter)),
            SubscriptionKind::Erc20Transfers => Erc20TransferSubscription::new(
                self.addresses,
                self.token_addresses,
                self.min_amount,
                self.max_amount,
            )
            .map(Subscription::Erc20Transfers),
        }
    }
}

impl Erc20TransferSubscription {
    fn new(
        addresses: Vec<Address>,
        token_addresses: Vec<Address>,
        min_amount: Option<[u8; 32]>,
        max_amount: Option<[u8; 32]>,
    ) -> Result<Self, String> {
        if addresses.is_empty() {
            return Err("erc20Transfers subscriptions require at least one wallet address".into());
        }
        if let (Some(min), Some(max)) = (min_amount, max_amount)
            && min > max
        {
            return Err("minAmount must be less than or equal to maxAmount".into());
        }

        Ok(Self {
            wallet_topics: addresses.into_iter().map(address_topic).collect(),
            token_addresses: token_addresses.into_iter().collect(),
            min_amount,
            max_amount,
        })
    }

    fn notification_for(&self, row: &LogRow) -> Option<Erc20TransferNotification> {
        if row.topic0.as_ref() != Some(&*ERC20_TRANSFER_TOPIC) || row.data_len != 32 {
            return None;
        }
        if !self.token_addresses.is_empty() && !self.token_addresses.contains(&row.address) {
            return None;
        }
        let from_topic = row.topic1?;
        let to_topic = row.topic2?;
        if !self.wallet_topics.contains(&from_topic) && !self.wallet_topics.contains(&to_topic) {
            return None;
        }
        let amount = amount_bytes(row)?;
        if self.min_amount.is_some_and(|min| amount < min) {
            return None;
        }
        if self.max_amount.is_some_and(|max| amount > max) {
            return None;
        }

        Some(Erc20TransferNotification {
            notification_type: "erc20Transfer",
            token_address: format_hex(row.address.as_slice()),
            from: format_hex(topic_address(&from_topic)?.as_slice()),
            to: format_hex(topic_address(&to_topic)?.as_slice()),
            raw_amount: format_hex(&amount),
            block_number: row.block_number,
            block_hash: format_hex(row.block_hash.as_slice()),
            timestamp: row.timestamp,
            transaction_hash: format_hex(row.tx_hash.as_slice()),
            transaction_index: row.tx_index,
            log_index: row.log_index,
            removed: false,
        })
    }
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
    let subscription = match receive_subscription(&mut socket).await {
        Some(subscription) => subscription,
        None => return,
    };

    tracing::debug!(kind = subscription.kind(), "new WebSocket subscription");

    let subs = match &state.subscriptions {
        Some(s) => s,
        None => {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };

    let mut receiver = subs.subscribe();

    // Send an ack
    let ack = serde_json::json!({"status": "subscribed", "type": subscription.kind()});
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
                        let Some(json) = subscription.payload_for(&rows) else {
                            continue;
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

impl Subscription {
    fn kind(&self) -> &'static str {
        match self {
            Self::Logs(_) => "logs",
            Self::Erc20Transfers(_) => "erc20Transfers",
        }
    }

    fn payload_for(&self, rows: &[LogRow]) -> Option<String> {
        match self {
            Self::Logs(filter) => {
                let matching: Vec<RpcLog> = rows
                    .iter()
                    .filter(|row| matches_filter(row, filter))
                    .map(RpcLog::from)
                    .collect();
                if matching.is_empty() {
                    return None;
                }
                serde_json::to_string(&matching).ok()
            }
            Self::Erc20Transfers(subscription) => {
                let matching: Vec<Erc20TransferNotification> = rows
                    .iter()
                    .filter_map(|row| subscription.notification_for(row))
                    .collect();
                if matching.is_empty() {
                    return None;
                }
                serde_json::to_string(&matching).ok()
            }
        }
    }
}

/// Receive and parse the initial subscription request from the client.
async fn receive_subscription(socket: &mut WebSocket) -> Option<Subscription> {
    // Give the client 10 seconds to send their subscription
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(10), socket.recv()).await;

    match timeout {
        Ok(Some(Ok(Message::Text(text)))) => {
            match serde_json::from_str::<SubscribeRequest>(&text) {
                Ok(req) => match req.into_subscription() {
                    Ok(subscription) => Some(subscription),
                    Err(e) => {
                        let err = serde_json::json!({"error": e});
                        let _ = socket.send(Message::Text(err.to_string().into())).await;
                        None
                    }
                },
                Err(e) => {
                    let err = serde_json::json!({"error": format!("invalid subscription: {e}")});
                    let _ = socket.send(Message::Text(err.to_string().into())).await;
                    None
                }
            }
        }
        _ => None,
    }
}

fn address_vec<'de, D>(deserializer: D) -> Result<Vec<Address>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Ok(Vec::new()),
        serde_json::Value::String(text) => parse_address_list(&text)
            .map_err(serde::de::Error::custom)
            .map(|addresses| addresses.into_iter().collect()),
        serde_json::Value::Array(values) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| "expected address hex string".to_string())
                    .and_then(parse_address)
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(serde::de::Error::custom),
        _ => Err(serde::de::Error::custom(
            "expected address string or address array",
        )),
    }
}

fn parse_address_list(text: &str) -> Result<Vec<Address>, String> {
    text.split([',', '\n', '\r', '\t', ' '])
        .filter(|part| !part.trim().is_empty())
        .map(parse_address)
        .collect()
}

fn amount_bound<'de, D>(deserializer: D) -> Result<Option<[u8; 32]>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(text) if text.trim().is_empty() => Ok(None),
        serde_json::Value::String(text) => parse_u256_bound(&text)
            .map(Some)
            .map_err(serde::de::Error::custom),
        serde_json::Value::Number(number) => parse_u256_bound(&number.to_string())
            .map(Some)
            .map_err(serde::de::Error::custom),
        _ => Err(serde::de::Error::custom(
            "amount bounds must be raw uint256 strings",
        )),
    }
}

fn parse_u256_bound(value: &str) -> Result<[u8; 32], String> {
    let text = value.trim();
    if text.is_empty() {
        return Err("amount bound cannot be empty".into());
    }
    if let Some(hex) = text.strip_prefix("0x") {
        if hex.len() > 64 {
            return Err("hex amount bound must fit in uint256".into());
        }
        let padded = format!("{hex:0>64}");
        let bytes = hex::decode(padded).map_err(|e| format!("invalid hex amount bound: {e}"))?;
        return bytes
            .try_into()
            .map_err(|_| "hex amount bound must be 32 bytes".to_string());
    }
    if !text.chars().all(|ch| ch.is_ascii_digit()) {
        return Err("amount bound must be a decimal integer or 0x-prefixed uint256".into());
    }

    let mut out = [0_u8; 32];
    for digit in text.bytes().map(|byte| byte - b'0') {
        mul_small(&mut out, 10)?;
        add_small(&mut out, digit)?;
    }
    Ok(out)
}

fn mul_small(bytes: &mut [u8; 32], multiplier: u8) -> Result<(), String> {
    let mut carry = 0_u16;
    for byte in bytes.iter_mut().rev() {
        let value = u16::from(*byte) * u16::from(multiplier) + carry;
        *byte = value as u8;
        carry = value >> 8;
    }
    if carry == 0 {
        Ok(())
    } else {
        Err("decimal amount bound must fit in uint256".into())
    }
}

fn add_small(bytes: &mut [u8; 32], addend: u8) -> Result<(), String> {
    let mut carry = u16::from(addend);
    for byte in bytes.iter_mut().rev() {
        let value = u16::from(*byte) + carry;
        *byte = value as u8;
        carry = value >> 8;
        if carry == 0 {
            return Ok(());
        }
    }
    Err("decimal amount bound must fit in uint256".into())
}

fn amount_bytes(row: &LogRow) -> Option<[u8; 32]> {
    let bytes: &[u8] = row.data.as_ref();
    (bytes.len() == 32).then(|| bytes.try_into().ok())?
}

fn address_topic(address: Address) -> B256 {
    let mut topic = [0_u8; 32];
    topic[12..].copy_from_slice(address.as_slice());
    B256::from(topic)
}

fn topic_address(topic: &B256) -> Option<Address> {
    let bytes = topic.as_slice();
    bytes[..12]
        .iter()
        .all(|byte| *byte == 0)
        .then(|| Address::from_slice(&bytes[12..]))
}

fn format_hex(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, Bytes, bytes};
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

    fn uint256_bytes(value: u64) -> Bytes {
        let mut bytes = [0_u8; 32];
        bytes[24..].copy_from_slice(&value.to_be_bytes());
        Bytes::copy_from_slice(&bytes)
    }

    fn make_transfer_log(
        token: Address,
        from: Address,
        to: Address,
        amount: u64,
        block: u64,
    ) -> LogRow {
        LogRow {
            block_number: block,
            block_hash: B256::repeat_byte(0x01),
            timestamp: 1_700_000_000,
            tx_hash: B256::repeat_byte(0x11),
            tx_index: 1,
            log_index: 2,
            address: token,
            topic0: Some(*ERC20_TRANSFER_TOPIC),
            topic1: Some(address_topic(from)),
            topic2: Some(address_topic(to)),
            topic3: None,
            data: uint256_bytes(amount),
            data_len: 32,
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

    #[test]
    fn test_erc20_transfer_subscription_matches_sender_or_recipient() {
        let tracked = Address::repeat_byte(0xA1);
        let token = Address::repeat_byte(0xBB);
        let other = Address::repeat_byte(0xC1);
        let subscription =
            Erc20TransferSubscription::new(vec![tracked], vec![token], None, None).unwrap();

        let outgoing = make_transfer_log(token, tracked, other, 100, 10);
        let incoming = make_transfer_log(token, other, tracked, 100, 11);
        let unrelated_wallet = make_transfer_log(token, other, Address::repeat_byte(0xD1), 100, 12);
        let unrelated_token =
            make_transfer_log(Address::repeat_byte(0xEF), tracked, other, 100, 13);

        assert!(subscription.notification_for(&outgoing).is_some());
        assert!(subscription.notification_for(&incoming).is_some());
        assert!(subscription.notification_for(&unrelated_wallet).is_none());
        assert!(subscription.notification_for(&unrelated_token).is_none());
    }

    #[test]
    fn test_erc20_transfer_subscription_filters_amount_bounds() {
        let tracked = Address::repeat_byte(0xA1);
        let token = Address::repeat_byte(0xBB);
        let subscription = Erc20TransferSubscription::new(
            vec![tracked],
            vec![token],
            Some(parse_u256_bound("100").unwrap()),
            Some(parse_u256_bound("200").unwrap()),
        )
        .unwrap();

        assert!(
            subscription
                .notification_for(&make_transfer_log(
                    token,
                    tracked,
                    Address::repeat_byte(0xC1),
                    99,
                    10
                ))
                .is_none()
        );
        assert!(
            subscription
                .notification_for(&make_transfer_log(
                    token,
                    tracked,
                    Address::repeat_byte(0xC1),
                    100,
                    10
                ))
                .is_some()
        );
        assert!(
            subscription
                .notification_for(&make_transfer_log(
                    token,
                    tracked,
                    Address::repeat_byte(0xC1),
                    201,
                    10
                ))
                .is_none()
        );
    }

    #[test]
    fn test_erc20_transfer_payload_serializes_enriched_notification() {
        let tracked = Address::repeat_byte(0xA1);
        let token = Address::repeat_byte(0xBB);
        let subscription = Subscription::Erc20Transfers(
            Erc20TransferSubscription::new(vec![tracked], vec![token], None, None).unwrap(),
        );
        let row = make_transfer_log(token, Address::repeat_byte(0xC1), tracked, 123, 42);
        let payload = subscription.payload_for(&[row]).unwrap();
        let json: serde_json::Value = serde_json::from_str(&payload).unwrap();
        let notification = &json.as_array().unwrap()[0];

        assert_eq!(notification["type"], "erc20Transfer");
        assert_eq!(notification["blockNumber"], 42);
        assert_eq!(
            notification["tokenAddress"],
            format!("0x{}", hex::encode(token.as_slice()))
        );
        assert_eq!(
            notification["to"],
            format!("0x{}", hex::encode(tracked.as_slice()))
        );
        assert_eq!(
            notification["rawAmount"],
            "0x000000000000000000000000000000000000000000000000000000000000007b"
        );
    }

    #[test]
    fn test_erc20_transfer_subscribe_request_deserializes_filters() {
        let tracked = format!("0x{}", hex::encode(Address::repeat_byte(0xA1)));
        let token = format!("0x{}", hex::encode(Address::repeat_byte(0xBB)));
        let request = serde_json::json!({
            "type": "erc20Transfers",
            "walletAddresses": [tracked],
            "tokenAddresses": [token],
            "minAmount": "100",
            "maxAmountRaw": "0xff"
        });

        let request: SubscribeRequest = serde_json::from_value(request).unwrap();
        let subscription = request.into_subscription().unwrap();
        let Subscription::Erc20Transfers(subscription) = subscription else {
            panic!("expected erc20 transfer subscription");
        };

        assert_eq!(subscription.wallet_topics.len(), 1);
        assert_eq!(subscription.token_addresses.len(), 1);
        assert_eq!(
            subscription.min_amount,
            Some(parse_u256_bound("100").unwrap())
        );
        assert_eq!(
            subscription.max_amount,
            Some(parse_u256_bound("0xff").unwrap())
        );
    }

    #[test]
    fn test_erc20_transfer_subscription_rejects_missing_addresses() {
        let err = Erc20TransferSubscription::new(Vec::new(), Vec::new(), None, None)
            .expect_err("missing wallet addresses should fail");
        assert!(err.contains("at least one wallet address"));
    }

    #[test]
    fn test_erc20_transfer_subscription_rejects_inverted_amount_bounds() {
        let tracked = Address::repeat_byte(0xA1);
        let err = Erc20TransferSubscription::new(
            vec![tracked],
            Vec::new(),
            Some(parse_u256_bound("200").unwrap()),
            Some(parse_u256_bound("100").unwrap()),
        )
        .expect_err("inverted amount bounds should fail");
        assert!(err.contains("minAmount"));
    }

    #[test]
    fn test_parse_u256_bound_accepts_decimal_and_hex() {
        assert_eq!(
            parse_u256_bound("255").unwrap(),
            parse_u256_bound("0xff").unwrap()
        );
        assert!(
            parse_u256_bound("0x10000000000000000000000000000000000000000000000000000000000000000")
                .is_err()
        );
        assert!(parse_u256_bound("1.5").is_err());
    }
}
