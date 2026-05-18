use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy_primitives::{Address, B256, keccak256};
use axum::Json;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use logex_types::LogRow;

use crate::eth_filter::{EthFilter, RpcLog, matches_filter, parse_address};
use crate::handler::AppState;

/// Capacity of the broadcast channel for new logs.
const BROADCAST_CAPACITY: usize = 4096;
const LIVE_TRANSFER_CHANNEL_CAPACITY: usize = 1024;
const LIVE_TRANSFER_HISTORY_LIMIT: usize = 10_000;
const DASHBOARD_SESSION_TIMEOUT: Duration = Duration::from_secs(60);
static ERC20_TRANSFER_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256(b"Transfer(address,address,uint256)"));

/// Manages WebSocket subscriptions for live log streaming.
#[derive(Clone)]
pub struct SubscriptionManager {
    inner: Arc<SubscriptionManagerInner>,
}

struct SubscriptionManagerInner {
    sender: broadcast::Sender<Arc<Vec<LogRow>>>,
    live_transfers: Mutex<LiveTransferSessions>,
    next_session_id: AtomicU64,
}

#[derive(Default)]
struct LiveTransferSessions {
    sessions: HashMap<String, LiveTransferSession>,
}

struct LiveTransferSession {
    subscription: Erc20TransferSubscription,
    scope: LiveSubscriptionScope,
    notifications: VecDeque<Erc20TransferNotification>,
    sender: broadcast::Sender<Arc<Vec<Erc20TransferNotification>>>,
    active_connections: usize,
    expires_at: Option<Instant>,
    dropped_notifications: u64,
}

impl SubscriptionManager {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            inner: Arc::new(SubscriptionManagerInner {
                sender,
                live_transfers: Mutex::new(LiveTransferSessions::default()),
                next_session_id: AtomicU64::new(1),
            }),
        }
    }

    /// Notify all subscribers of new logs. Called by the ingestion pipeline.
    pub fn notify(&self, rows: &[LogRow]) {
        if rows.is_empty() {
            return;
        }
        // Ignore send errors — they just mean no active raw-log receivers.
        if self.inner.sender.receiver_count() > 0 {
            let _ = self.inner.sender.send(Arc::new(rows.to_vec()));
        }
        self.notify_live_transfer_sessions(rows);
    }

    /// Create a new receiver for the broadcast channel.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Vec<LogRow>>> {
        self.inner.sender.subscribe()
    }

    fn notify_live_transfer_sessions(&self, rows: &[LogRow]) {
        let now = Instant::now();
        let mut sessions = self.live_transfers();
        sessions.sweep_expired(now);

        for session in sessions.sessions.values_mut() {
            let matching: Vec<Erc20TransferNotification> = rows
                .iter()
                .filter_map(|row| session.subscription.notification_for(row))
                .collect();
            if matching.is_empty() {
                continue;
            }
            session.push_notifications(&matching);
            let _ = session.sender.send(Arc::new(matching));
        }
    }

    fn live_transfers(&self) -> std::sync::MutexGuard<'_, LiveTransferSessions> {
        self.inner
            .live_transfers
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn next_subscription_id(&self) -> String {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        let sequence = self.inner.next_session_id.fetch_add(1, Ordering::Relaxed);
        format!("live-{millis:x}-{sequence:x}")
    }

    fn upsert_live_transfer_session(
        &self,
        requested_id: Option<String>,
        scope: LiveSubscriptionScope,
        subscription: Erc20TransferSubscription,
        attach: bool,
    ) -> LiveTransferSessionAttachment {
        let id = requested_id
            .and_then(normalize_subscription_id)
            .unwrap_or_else(|| self.next_subscription_id());
        let mut sessions = self.live_transfers();
        sessions.sweep_expired(Instant::now());
        let session = sessions
            .sessions
            .entry(id.clone())
            .or_insert_with(|| LiveTransferSession::new(subscription.clone(), scope));
        session.subscription = subscription;
        session.scope = scope;
        if attach {
            session.active_connections = session.active_connections.saturating_add(1);
            session.expires_at = None;
        } else if scope == LiveSubscriptionScope::Dashboard && session.active_connections == 0 {
            session.expires_at = Some(Instant::now() + DASHBOARD_SESSION_TIMEOUT);
        } else {
            session.expires_at = None;
        }
        let snapshot = session.snapshot(&id);
        let receiver = session.sender.subscribe();
        LiveTransferSessionAttachment {
            id,
            snapshot,
            receiver,
        }
    }

    fn live_transfer_session(&self, id: &str) -> Option<LiveTransferSessionSnapshot> {
        let mut sessions = self.live_transfers();
        sessions.sweep_expired(Instant::now());
        let id = normalize_subscription_id(id.to_string())?;
        sessions
            .sessions
            .get(&id)
            .map(|session| session.snapshot(&id))
    }

    fn clear_live_transfer_session(&self, id: &str) -> Option<LiveTransferSessionSnapshot> {
        let mut sessions = self.live_transfers();
        sessions.sweep_expired(Instant::now());
        let id = normalize_subscription_id(id.to_string())?;
        let session = sessions.sessions.get_mut(&id)?;
        session.notifications.clear();
        session.dropped_notifications = 0;
        Some(session.snapshot(&id))
    }

    fn remove_live_transfer_session(&self, id: &str) -> bool {
        let mut sessions = self.live_transfers();
        let Some(id) = normalize_subscription_id(id.to_string()) else {
            return false;
        };
        sessions.sessions.remove(&id).is_some()
    }

    fn detach_live_transfer_session(&self, id: &str) {
        let Some(id) = normalize_subscription_id(id.to_string()) else {
            return;
        };
        let mut sessions = self.live_transfers();
        if let Some(session) = sessions.sessions.get_mut(&id) {
            session.active_connections = session.active_connections.saturating_sub(1);
            let should_expire = session.scope == LiveSubscriptionScope::Dashboard
                && session.active_connections == 0;
            if should_expire {
                session.expires_at = Some(Instant::now() + DASHBOARD_SESSION_TIMEOUT);
            }
        }
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
pub(crate) struct SubscribeRequest {
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
    /// Optional id used to resume server-backed ERC20 transfer sessions.
    #[serde(default, alias = "clientId")]
    subscription_id: Option<String>,
    /// Session lifetime. Dashboard sessions expire after the browser WebSocket disconnects; service sessions do not.
    #[serde(default, alias = "subscriptionScope")]
    scope: LiveSubscriptionScope,
    /// Backward-compatible way for non-UI clients to request a service session.
    #[serde(default)]
    persistent: bool,
}

#[derive(Debug, Default, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum SubscriptionKind {
    #[default]
    Logs,
    Erc20Transfers,
}

#[derive(Debug, Default, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
enum LiveSubscriptionScope {
    #[default]
    Ephemeral,
    Dashboard,
    Service,
}

#[derive(Debug)]
enum Subscription {
    Logs(EthFilter),
    Erc20Transfers(Erc20TransferSubscription),
}

#[derive(Debug, Clone)]
struct Erc20TransferSubscription {
    wallet_topics: HashSet<B256>,
    token_addresses: HashSet<Address>,
    min_amount: Option<[u8; 32]>,
    max_amount: Option<[u8; 32]>,
}

#[derive(Debug, Clone, Serialize)]
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

struct LiveTransferSessionAttachment {
    id: String,
    snapshot: LiveTransferSessionSnapshot,
    receiver: broadcast::Receiver<Arc<Vec<Erc20TransferNotification>>>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LiveTransferSessionSnapshot {
    status: &'static str,
    #[serde(rename = "type")]
    subscription_type: &'static str,
    subscription_id: String,
    scope: LiveSubscriptionScope,
    active_connections: usize,
    notifications: Vec<Erc20TransferNotification>,
    dropped_notifications: u64,
    history_limit: usize,
    expires_in_seconds: Option<u64>,
}

impl SubscribeRequest {
    fn into_subscription(self) -> Result<Subscription, String> {
        match self.subscription_type {
            SubscriptionKind::Logs => Ok(Subscription::Logs(self.filter)),
            SubscriptionKind::Erc20Transfers => self
                .into_erc20_transfer_subscription()
                .map(Subscription::Erc20Transfers),
        }
    }

    fn into_erc20_transfer_subscription(self) -> Result<Erc20TransferSubscription, String> {
        Erc20TransferSubscription::new(
            self.addresses,
            self.token_addresses,
            self.min_amount,
            self.max_amount,
        )
    }

    fn live_session_scope(&self) -> LiveSubscriptionScope {
        if self.persistent && self.scope == LiveSubscriptionScope::Ephemeral {
            LiveSubscriptionScope::Service
        } else {
            self.scope
        }
    }
}

impl LiveTransferSessions {
    fn sweep_expired(&mut self, now: Instant) {
        self.sessions.retain(
            |_, session| !matches!(session.expires_at, Some(expires_at) if expires_at <= now),
        );
    }
}

impl LiveTransferSession {
    fn new(subscription: Erc20TransferSubscription, scope: LiveSubscriptionScope) -> Self {
        let (sender, _) = broadcast::channel(LIVE_TRANSFER_CHANNEL_CAPACITY);
        Self {
            subscription,
            scope,
            notifications: VecDeque::new(),
            sender,
            active_connections: 0,
            expires_at: if scope == LiveSubscriptionScope::Dashboard {
                Some(Instant::now() + DASHBOARD_SESSION_TIMEOUT)
            } else {
                None
            },
            dropped_notifications: 0,
        }
    }

    fn push_notifications(&mut self, notifications: &[Erc20TransferNotification]) {
        for notification in notifications.iter().rev() {
            self.notifications.push_front(notification.clone());
            while self.notifications.len() > LIVE_TRANSFER_HISTORY_LIMIT {
                self.notifications.pop_back();
                self.dropped_notifications = self.dropped_notifications.saturating_add(1);
            }
        }
    }

    fn snapshot(&self, id: &str) -> LiveTransferSessionSnapshot {
        LiveTransferSessionSnapshot {
            status: "subscribed",
            subscription_type: "erc20Transfers",
            subscription_id: id.to_string(),
            scope: self.scope,
            active_connections: self.active_connections,
            notifications: self.notifications.iter().cloned().collect(),
            dropped_notifications: self.dropped_notifications,
            history_limit: LIVE_TRANSFER_HISTORY_LIMIT,
            expires_in_seconds: self.expires_at.map(|expires_at| {
                expires_at
                    .saturating_duration_since(Instant::now())
                    .as_secs()
            }),
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
        if addresses.is_empty() && token_addresses.is_empty() {
            return Err(
                "erc20Transfers subscriptions require at least one wallet address or token address"
                    .into(),
            );
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
        if !self.wallet_topics.is_empty()
            && !self.wallet_topics.contains(&from_topic)
            && !self.wallet_topics.contains(&to_topic)
        {
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

/// Create or update a service-scoped ERC20 transfer subscription.
///
/// This endpoint is intended for non-dashboard clients that need a subscription to keep collecting
/// matching live transfers while the LogEx process is running, even if no WebSocket is currently
/// attached. Dashboard-created sessions use `/ws` and expire after the browser WebSocket
/// disconnects.
pub(crate) async fn handle_live_transfer_subscribe(
    State(state): State<Arc<AppState>>,
    Json(mut request): Json<SubscribeRequest>,
) -> Response {
    request.subscription_type = SubscriptionKind::Erc20Transfers;
    request.scope = LiveSubscriptionScope::Service;
    let requested_id = request.subscription_id.clone();
    let subscription = match request.into_erc20_transfer_subscription() {
        Ok(subscription) => subscription,
        Err(error) => return json_error(StatusCode::BAD_REQUEST, error),
    };
    let Some(subs) = state.subscriptions.as_ref() else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "subscriptions are disabled",
        );
    };

    let attachment = subs.upsert_live_transfer_session(
        requested_id,
        LiveSubscriptionScope::Service,
        subscription,
        false,
    );
    Json(attachment.snapshot).into_response()
}

/// Return a server-backed ERC20 transfer subscription and retained notifications.
pub(crate) async fn handle_live_transfer_get(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(subs) = state.subscriptions.as_ref() else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "subscriptions are disabled",
        );
    };
    match subs.live_transfer_session(&id) {
        Some(snapshot) => Json(snapshot).into_response(),
        None => json_error(StatusCode::NOT_FOUND, "subscription not found"),
    }
}

/// Clear retained notifications for a server-backed ERC20 transfer subscription.
pub(crate) async fn handle_live_transfer_clear(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(subs) = state.subscriptions.as_ref() else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "subscriptions are disabled",
        );
    };
    match subs.clear_live_transfer_session(&id) {
        Some(snapshot) => Json(snapshot).into_response(),
        None => json_error(StatusCode::NOT_FOUND, "subscription not found"),
    }
}

/// Delete a server-backed ERC20 transfer subscription.
pub(crate) async fn handle_live_transfer_delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Response {
    let Some(subs) = state.subscriptions.as_ref() else {
        return json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "subscriptions are disabled",
        );
    };
    if subs.remove_live_transfer_session(&id) {
        Json(serde_json::json!({"status": "deleted", "subscriptionId": id})).into_response()
    } else {
        json_error(StatusCode::NOT_FOUND, "subscription not found")
    }
}

fn json_error(status: StatusCode, error: impl Into<String>) -> Response {
    (status, Json(serde_json::json!({ "error": error.into() }))).into_response()
}

async fn handle_ws(mut socket: WebSocket, state: Arc<AppState>) {
    // Wait for the client's subscription message
    let request = match receive_subscription(&mut socket).await {
        Some(request) => request,
        None => return,
    };

    let subs = match &state.subscriptions {
        Some(s) => s,
        None => {
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };

    if request.subscription_type == SubscriptionKind::Erc20Transfers {
        let scope = request.live_session_scope();
        if scope != LiveSubscriptionScope::Ephemeral || request.subscription_id.is_some() {
            handle_live_transfer_ws(socket, subs.clone(), request, scope).await;
            return;
        }
    }

    let subscription = match request.into_subscription() {
        Ok(subscription) => subscription,
        Err(error) => {
            let err = serde_json::json!({"error": error});
            let _ = socket.send(Message::Text(err.to_string().into())).await;
            return;
        }
    };

    tracing::debug!(kind = subscription.kind(), "new WebSocket subscription");

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

async fn handle_live_transfer_ws(
    mut socket: WebSocket,
    subs: SubscriptionManager,
    request: SubscribeRequest,
    scope: LiveSubscriptionScope,
) {
    let requested_id = request.subscription_id.clone();
    let subscription = match request.into_erc20_transfer_subscription() {
        Ok(subscription) => subscription,
        Err(error) => {
            let err = serde_json::json!({"error": error});
            let _ = socket.send(Message::Text(err.to_string().into())).await;
            return;
        }
    };
    let mut attachment = subs.upsert_live_transfer_session(requested_id, scope, subscription, true);

    tracing::debug!(
        kind = "erc20Transfers",
        subscription_id = %attachment.id,
        ?scope,
        "new server-backed WebSocket subscription"
    );

    if socket
        .send(Message::Text(
            serde_json::to_string(&attachment.snapshot)
                .unwrap_or_else(|_| r#"{"status":"subscribed","type":"erc20Transfers"}"#.into())
                .into(),
        ))
        .await
        .is_err()
    {
        subs.detach_live_transfer_session(&attachment.id);
        return;
    }

    loop {
        tokio::select! {
            result = attachment.receiver.recv() => {
                match result {
                    Ok(notifications) => {
                        if notifications.is_empty() {
                            continue;
                        }
                        if let Ok(json) = serde_json::to_string(&*notifications)
                            && socket.send(Message::Text(json.into())).await.is_err()
                        {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed = n, subscription_id = %attachment.id, "server-backed WebSocket subscriber lagged");
                        continue;
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
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

    subs.detach_live_transfer_session(&attachment.id);
    tracing::debug!(
        subscription_id = %attachment.id,
        "server-backed WebSocket subscription closed"
    );
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
async fn receive_subscription(socket: &mut WebSocket) -> Option<SubscribeRequest> {
    // Give the client 10 seconds to send their subscription
    let timeout = tokio::time::timeout(std::time::Duration::from_secs(10), socket.recv()).await;

    match timeout {
        Ok(Some(Ok(Message::Text(text)))) => {
            match serde_json::from_str::<SubscribeRequest>(&text) {
                Ok(req) => Some(req),
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

fn normalize_subscription_id(id: String) -> Option<String> {
    let trimmed = id.trim();
    if trimmed.is_empty() || trimmed.len() > 128 {
        return None;
    }
    if !trimmed
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return None;
    }
    Some(trimmed.to_string())
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

    #[test]
    fn test_dashboard_live_transfer_session_replays_and_expires_after_detach() {
        let mgr = SubscriptionManager::new();
        let tracked = Address::repeat_byte(0xA1);
        let token = Address::repeat_byte(0xBB);
        let subscription =
            Erc20TransferSubscription::new(vec![tracked], vec![token], None, None).unwrap();
        let mut attachment = mgr.upsert_live_transfer_session(
            Some("dashboard-1".into()),
            LiveSubscriptionScope::Dashboard,
            subscription,
            true,
        );

        mgr.notify(&[make_transfer_log(
            token,
            Address::repeat_byte(0xC1),
            tracked,
            100,
            10,
        )]);

        let live_batch = attachment.receiver.try_recv().unwrap();
        assert_eq!(live_batch.len(), 1);
        let snapshot = mgr.live_transfer_session("dashboard-1").unwrap();
        assert_eq!(snapshot.notifications.len(), 1);
        assert_eq!(snapshot.active_connections, 1);
        assert_eq!(snapshot.expires_in_seconds, None);

        mgr.detach_live_transfer_session("dashboard-1");
        {
            let mut sessions = mgr.live_transfers();
            sessions.sessions.get_mut("dashboard-1").unwrap().expires_at =
                Some(Instant::now() - Duration::from_secs(1));
        }
        mgr.notify(&[make_log(0xAA, 11)]);
        assert!(mgr.live_transfer_session("dashboard-1").is_none());
    }

    #[test]
    fn test_service_live_transfer_session_persists_without_socket() {
        let mgr = SubscriptionManager::new();
        let tracked = Address::repeat_byte(0xA1);
        let token = Address::repeat_byte(0xBB);
        let subscription =
            Erc20TransferSubscription::new(vec![tracked], vec![token], None, None).unwrap();
        let attachment = mgr.upsert_live_transfer_session(
            Some("service-1".into()),
            LiveSubscriptionScope::Service,
            subscription,
            false,
        );

        assert_eq!(attachment.snapshot.active_connections, 0);
        mgr.notify(&[make_transfer_log(
            token,
            tracked,
            Address::repeat_byte(0xC1),
            100,
            10,
        )]);

        let snapshot = mgr.live_transfer_session("service-1").unwrap();
        assert_eq!(snapshot.scope, LiveSubscriptionScope::Service);
        assert_eq!(snapshot.notifications.len(), 1);
        assert_eq!(snapshot.expires_in_seconds, None);
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
    fn test_erc20_transfer_subscription_allows_token_only_filter() {
        let token = Address::repeat_byte(0xBB);
        let other_token = Address::repeat_byte(0xCC);
        let subscription =
            Erc20TransferSubscription::new(Vec::new(), vec![token], None, None).unwrap();

        assert!(
            subscription
                .notification_for(&make_transfer_log(
                    token,
                    Address::repeat_byte(0xA1),
                    Address::repeat_byte(0xA2),
                    100,
                    10
                ))
                .is_some()
        );
        assert!(
            subscription
                .notification_for(&make_transfer_log(
                    other_token,
                    Address::repeat_byte(0xA1),
                    Address::repeat_byte(0xA2),
                    100,
                    10
                ))
                .is_none()
        );
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
            "subscriptionId": "dashboard-1",
            "subscriptionScope": "dashboard",
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

        let request: SubscribeRequest = serde_json::from_value(serde_json::json!({
            "type": "erc20Transfers",
            "persistent": true,
            "walletAddresses": [format!("0x{}", hex::encode(Address::repeat_byte(0xA1)))]
        }))
        .unwrap();
        assert_eq!(request.live_session_scope(), LiveSubscriptionScope::Service);
    }

    #[test]
    fn test_erc20_transfer_subscription_rejects_empty_filters() {
        let err = Erc20TransferSubscription::new(Vec::new(), Vec::new(), None, None)
            .expect_err("empty wallet and token filters should fail");
        assert!(err.contains("at least one wallet address or token address"));
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
