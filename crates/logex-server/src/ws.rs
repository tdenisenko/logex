use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use alloy_primitives::{Address, B256, keccak256};
use axum::Json;
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use logex_types::LogRow;

use crate::eth_filter::{EthFilter, RpcLog, parse_address};
use crate::handler::AppState;

/// Capacity of the broadcast channel for new logs.
const BROADCAST_CAPACITY: usize = 4096;
const LIVE_TRANSFER_CHANNEL_CAPACITY: usize = 1024;
const LIVE_TRANSFER_HISTORY_LIMIT: usize = 10_000;
const DASHBOARD_SESSION_TIMEOUT: Duration = Duration::from_secs(60);
const LAG_CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
static ERC20_TRANSFER_TOPIC: LazyLock<B256> =
    LazyLock::new(|| keccak256(b"Transfer(address,address,uint256)"));

/// Manages WebSocket subscriptions for live log streaming.
#[derive(Clone)]
pub struct SubscriptionManager {
    inner: Arc<SubscriptionManagerInner>,
}

/// A transient canonical log change; stored rows remain independent of delivery state.
#[derive(Debug)]
pub struct LogNotificationBatch {
    pub rows: Vec<LogRow>,
    pub removed: bool,
}

struct SubscriptionManagerInner {
    sender: broadcast::Sender<Arc<LogNotificationBatch>>,
    live_transfers: Mutex<LiveTransferSessions>,
    next_session_id: AtomicU64,
}

#[derive(Default)]
struct LiveTransferSessions {
    sessions: HashMap<String, LiveTransferSession>,
}

struct LiveTransferSession {
    identity: Arc<()>,
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

    /// Publish accepted live logs in chronological order.
    pub fn notify(&self, rows: &[LogRow]) {
        self.publish(rows, false);
    }

    /// Publish logs removed by a successful canonical change, before replacements.
    /// Retained clients can receive removals for unknown identities and must ignore them.
    pub fn notify_removed(&self, rows: &[LogRow]) {
        self.publish(rows, true);
    }

    /// Create a receiver for transient canonical log changes.
    pub fn subscribe(&self) -> broadcast::Receiver<Arc<LogNotificationBatch>> {
        self.inner.sender.subscribe()
    }

    fn publish(&self, rows: &[LogRow], removed: bool) {
        if rows.is_empty() {
            return;
        }
        // One publication boundary orders both channels and retained snapshots.
        let mut sessions = self.live_transfers();
        sessions.sweep_expired(Instant::now());
        if self.inner.sender.receiver_count() > 0 {
            let _ = self.inner.sender.send(Arc::new(LogNotificationBatch {
                rows: rows.to_vec(),
                removed,
            }));
        }
        if sessions.sessions.is_empty() {
            return;
        }
        // Retractions cannot use the current filter: an in-place upsert may have
        // changed it after an earlier delivery, even one already evicted from history.
        if removed {
            let removals: Vec<_> = rows
                .iter()
                .filter_map(Erc20TransferSubscription::unfiltered_notification_for)
                .map(|mut notification| {
                    notification.removed = true;
                    notification
                })
                .collect();
            if removals.is_empty() {
                return;
            }
            let removals = Arc::new(removals);
            for session in sessions.sessions.values_mut() {
                session.push_notifications(&removals);
                let _ = session.sender.send(Arc::clone(&removals));
            }
            return;
        }
        for session in sessions.sessions.values_mut() {
            let matching: Vec<_> = rows
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
            identity: Arc::clone(&session.identity),
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

    fn detach_live_transfer_session(&self, id: &str, identity: &Arc<()>) {
        let Some(id) = normalize_subscription_id(id.to_string()) else {
            return;
        };
        let mut sessions = self.live_transfers();
        if let Some(session) = sessions.sessions.get_mut(&id)
            && Arc::ptr_eq(&session.identity, identity)
        {
            session.active_connections = session.active_connections.saturating_sub(1);
            if session.active_connections == 0 {
                match session.scope {
                    LiveSubscriptionScope::Ephemeral => {
                        sessions.sessions.remove(&id);
                    }
                    LiveSubscriptionScope::Dashboard => {
                        session.expires_at = Some(Instant::now() + DASHBOARD_SESSION_TIMEOUT);
                    }
                    LiveSubscriptionScope::Service => {}
                }
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
    Logs(Box<logex_storage::native::NativeLogFilter>),
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
    identity: Arc<()>,
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
            SubscriptionKind::Logs => self
                .filter
                .to_stream_filter()
                .map(|filter| Subscription::Logs(Box::new(filter))),
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
            identity: Arc::new(()),
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
        if notifications.is_empty() {
            return;
        }
        // Publications have a single change kind. Batch removal avoids scanning
        // the full retained history separately for every orphaned log.
        if notifications
            .iter()
            .all(|notification| notification.removed)
        {
            let identities: HashSet<_> = notifications
                .iter()
                .map(|notification| (notification.block_hash.as_str(), notification.log_index))
                .collect();
            self.notifications.retain(|existing| {
                !identities.contains(&(existing.block_hash.as_str(), existing.log_index))
            });
            return;
        }
        for notification in notifications {
            if notification.removed {
                self.notifications.retain(|existing| {
                    existing.block_hash != notification.block_hash
                        || existing.log_index != notification.log_index
                });
                continue;
            }
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

    fn unfiltered_notification_for(row: &LogRow) -> Option<Erc20TransferNotification> {
        Self {
            wallet_topics: HashSet::new(),
            token_addresses: HashSet::new(),
            min_amount: None,
            max_amount: None,
        }
        .notification_for(row)
    }

    fn notification_for(&self, row: &LogRow) -> Option<Erc20TransferNotification> {
        if row.topic0.as_ref() != Some(&*ERC20_TRANSFER_TOPIC)
            || row.topic3.is_some()
            || row.data_len != 32
        {
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
    if let Some(reason) = state.storage_failure() {
        return crate::rest::storage_unavailable_response(reason);
    }
    ws.on_upgrade(move |socket| async move {
        tokio::select! {
            biased;
            _ = state.storage_unavailable() => {},
            _ = handle_ws(socket, Arc::clone(&state)) => {},
        }
    })
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
    if let Some(reason) = state.storage_failure() {
        return crate::rest::storage_unavailable_response(reason);
    }
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
    if let Some(reason) = state.storage_failure() {
        return crate::rest::storage_unavailable_response(reason);
    }
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
                        let Some(json) = subscription.payload_for_change(&rows.rows, rows.removed) else {
                            continue;
                        };

                        if socket.send(Message::Text(json.into())).await.is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(missed_batches = n, "WebSocket subscriber lagged; closing for resync");
                        close_lagged(|message| socket.send(message)).await;
                        break;
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

/// A gap ends delivery; the close frame is only a best-effort resync diagnostic.
/// Bound this terminal write so a stalled peer cannot keep the socket/session
/// owner alive. Ordinary data, acknowledgement, and pong sends retain backpressure.
async fn close_lagged<F>(send: impl FnOnce(Message) -> F)
where
    F: std::future::Future<Output = Result<(), axum::Error>>,
{
    let message = Message::Close(Some(CloseFrame {
        code: close_code::AGAIN,
        reason: "Live stream fell behind; reconnect and reconcile stored logs.".into(),
    }));
    let _ = tokio::time::timeout(LAG_CLOSE_TIMEOUT, send(message)).await;
}

// Detach even when storage failure cancels the entire socket future.
// This token identifies an entry without keeping its broadcast sender alive.
struct DetachSession<'a>(&'a SubscriptionManager, String, Arc<()>);

impl Drop for DetachSession<'_> {
    fn drop(&mut self) {
        self.0.detach_live_transfer_session(&self.1, &self.2);
    }
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
    let LiveTransferSessionAttachment {
        id,
        identity,
        snapshot,
        mut receiver,
    } = subs.upsert_live_transfer_session(requested_id, scope, subscription, true);
    let _detach = DetachSession(&subs, id.clone(), identity);

    tracing::debug!(
        kind = "erc20Transfers",
        subscription_id = %id,
        ?scope,
        "new server-backed WebSocket subscription"
    );

    let acknowledgement = serde_json::to_string(&snapshot)
        .unwrap_or_else(|_| r#"{"status":"subscribed","type":"erc20Transfers"}"#.into());
    // The acknowledgement owns its serialized data. Release the copied history
    // before any socket await, including a slow acknowledgement send.
    drop(snapshot);
    if socket
        .send(Message::Text(acknowledgement.into()))
        .await
        .is_err()
    {
        return;
    }

    loop {
        tokio::select! {
            result = receiver.recv() => {
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
                        tracing::warn!(missed_batches = n, subscription_id = %id, "server-backed WebSocket subscriber lagged; closing for resync");
                        close_lagged(|message| socket.send(message)).await;
                        break;
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

    tracing::debug!(
        subscription_id = %id,
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

    #[cfg(test)]
    fn payload_for(&self, rows: &[LogRow]) -> Option<String> {
        self.payload_for_change(rows, false)
    }

    fn payload_for_change(&self, rows: &[LogRow], removed: bool) -> Option<String> {
        match self {
            Self::Logs(filter) => {
                let matching: Vec<RpcLog> = rows
                    .iter()
                    .filter(|row| logex_query::matches_native_filter(row, filter))
                    .map(|row| {
                        let mut log = RpcLog::from(row);
                        log.removed = removed;
                        log
                    })
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
                    .map(|mut notification| {
                        notification.removed = removed;
                        notification
                    })
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
    struct AddressListVisitor;

    impl<'de> serde::de::Visitor<'de> for AddressListVisitor {
        type Value = Vec<Address>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("null, an address list string, or an array of address strings")
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(Vec::new())
        }

        fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Self::Value, E> {
            parse_address_list(text).map_err(E::custom)
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut addresses = Vec::new();
            while let Some(text) = sequence.next_element::<String>()? {
                addresses.push(parse_address(&text).map_err(serde::de::Error::custom)?);
            }
            Ok(addresses)
        }
    }

    deserializer.deserialize_any(AddressListVisitor)
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
    struct AmountBoundVisitor;

    impl<'de> serde::de::Visitor<'de> for AmountBoundVisitor {
        type Value = Option<[u8; 32]>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("null, a raw uint256 string, or an unsigned 64-bit integer")
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_str<E: serde::de::Error>(self, text: &str) -> Result<Self::Value, E> {
            if text.trim().is_empty() {
                return Ok(None);
            }
            parse_u256_bound(text).map(Some).map_err(E::custom)
        }

        fn visit_u64<E>(self, number: u64) -> Result<Self::Value, E> {
            let mut amount = [0; 32];
            amount[24..].copy_from_slice(&number.to_be_bytes());
            Ok(Some(amount))
        }

        fn visit_i64<E: serde::de::Error>(self, number: i64) -> Result<Self::Value, E> {
            let number = u64::try_from(number)
                .map_err(|_| E::custom("amount bound must not be negative"))?;
            self.visit_u64(number)
        }
    }

    // Floating-point tokens are deliberately unsupported: uint256 bounds must
    // remain exact. Larger integer amounts are supplied as decimal/hex strings.
    deserializer.deserialize_any(AmountBoundVisitor)
}

fn parse_u256_bound(value: &str) -> Result<[u8; 32], String> {
    let text = value.trim();
    if text.is_empty() {
        return Err("amount bound cannot be empty".into());
    }
    if let Some(hex) = text.strip_prefix("0x") {
        if hex.is_empty() {
            return Err("hex amount bound must contain at least one digit".into());
        }
        if hex.len() > 64 {
            return Err("hex amount bound must fit in uint256".into());
        }
        let mut padded = [b'0'; 64];
        padded[64 - hex.len()..].copy_from_slice(hex.as_bytes());
        let mut amount = [0; 32];
        hex::decode_to_slice(padded, &mut amount)
            .map_err(|e| format!("invalid hex amount bound: {e}"))?;
        return Ok(amount);
    }
    if !text.chars().all(|ch| ch.is_ascii_digit()) {
        return Err("amount bound must be a decimal integer or 0x-prefixed uint256".into());
    }

    // Scan syntax above, but only significant digits need uint256 arithmetic.
    // A uint256 has at most 78 decimal digits; the arithmetic still checks the
    // exact upper bound for 78-digit values. Leading zeroes remain accepted.
    let significant = text.trim_start_matches('0');
    if significant.len() > 78 {
        return Err("decimal amount bound must fit in uint256".into());
    }
    let mut out = [0_u8; 32];
    for digit in significant.bytes().map(|byte| byte - b'0') {
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

    #[tokio::test]
    async fn erc20_inputs_invalid_update_cannot_mutate_existing_session() {
        use tower::ServiceExt;
        let manager = SubscriptionManager::new();
        let tracked = Address::repeat_byte(1);
        let replacement = Address::repeat_byte(2);
        let token = Address::repeat_byte(0xBB);
        let subscription =
            Erc20TransferSubscription::new(vec![tracked], vec![token], None, None).unwrap();
        let attachment = manager.upsert_live_transfer_session(
            Some("atomic-input".into()),
            LiveSubscriptionScope::Dashboard,
            subscription,
            true,
        );
        manager.notify(&[make_transfer_log(token, tracked, Address::ZERO, 5, 10)]);
        let directory = tempfile::tempdir().unwrap();
        let storage =
            logex_storage::PartitionManager::open(logex_storage::PartitionManagerConfig {
                data_dir: directory.path().to_path_buf(),
                partition_target_rows: 100,
                compaction_safety_margin_blocks: 2048,
            })
            .unwrap();
        let state = Arc::new(AppState::new(
            storage,
            Some(manager.clone()),
            Default::default(),
        ));
        let router = axum::Router::new()
            .route(
                "/subscriptions",
                axum::routing::post(handle_live_transfer_subscribe),
            )
            .with_state(state);
        let private = |text: String| serde_json::json!({"$serde_json::private::RawValue":text});
        for (index, (fields, expected)) in [
            (
                serde_json::json!({"addresses":private(serde_json::to_string(&format!("{replacement:#x}")).unwrap())}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                serde_json::json!({"addresses":[format!("{replacement:#x}")],"minAmount":private("1".into())}),
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                serde_json::json!({"addresses":[format!("{replacement:#x}")],"minAmount":"10","maxAmount":"1"}),
                StatusCode::BAD_REQUEST,
            ),
        ].into_iter().enumerate() {
            let prior_history = serde_json::to_value(&manager.live_transfer_session("atomic-input").unwrap().notifications).unwrap();
            let mut request = fields;
            request["subscriptionId"] = serde_json::json!("atomic-input");
            let response = router
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri("/subscriptions")
                        .header("content-type", "application/json")
                        .body(axum::body::Body::from(request.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            let sessions = manager.live_transfers();
            let session = sessions.sessions.get("atomic-input").unwrap();
            assert!(Arc::ptr_eq(&session.identity, &attachment.identity));
            assert_eq!(session.active_connections, 1);
            assert_eq!(session.scope, LiveSubscriptionScope::Dashboard);
            assert_eq!(
                session.subscription.wallet_topics,
                HashSet::from([address_topic(tracked)])
            );
            assert_eq!(session.subscription.token_addresses, HashSet::from([token]));
            assert_eq!(session.subscription.min_amount, None);
            assert_eq!(session.subscription.max_amount, None);
            assert_eq!(serde_json::to_value(&session.notifications).unwrap(), prior_history);
            assert_eq!(session.dropped_notifications, 0);
            drop(sessions);
            let next_block = 11 + index as u64;
            manager.notify(&[make_transfer_log(token, tracked, Address::ZERO, 5, next_block)]);
            let snapshot = manager.live_transfer_session("atomic-input").unwrap();
            assert_eq!(snapshot.notifications.iter().map(|row| row.block_number).collect::<Vec<_>>(), (10..=next_block).rev().collect::<Vec<_>>());
        }
        let invalid_new = serde_json::json!({"subscriptionId":"must-not-exist","addresses":private(serde_json::to_string(&format!("{replacement:#x}")).unwrap())});
        let response = router
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/subscriptions")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(invalid_new.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(manager.live_transfer_session("must-not-exist").is_none());
        let request = serde_json::json!({"subscriptionId":"atomic-input","walletAddresses":format!("{replacement:#x}, {tracked:#x}"),"minAmountRaw":1,"maxAmountRaw":"10"});
        let response = router
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/subscriptions")
                    .header("content-type", "application/json")
                    .body(axum::body::Body::from(request.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        {
            let sessions = manager.live_transfers();
            let session = sessions.sessions.get("atomic-input").unwrap();
            assert!(Arc::ptr_eq(&session.identity, &attachment.identity));
            assert_eq!(session.active_connections, 1);
            assert_eq!(session.scope, LiveSubscriptionScope::Service);
            assert_eq!(session.subscription.wallet_topics.len(), 2);
            assert_eq!(
                session.subscription.min_amount,
                Some(parse_u256_bound("1").unwrap())
            );
            assert_eq!(
                session
                    .notifications
                    .iter()
                    .map(|row| row.block_number)
                    .collect::<Vec<_>>(),
                vec![13, 12, 11, 10]
            );
        }
        manager.detach_live_transfer_session(&attachment.id, &attachment.identity);
    }

    #[test]
    fn erc20_inputs_preserve_address_forms_and_aliases() {
        let first = Address::repeat_byte(1);
        let second = Address::repeat_byte(2);
        for field in ["addresses", "walletAddresses", "tokenAddresses"] {
            for (value, expected) in [
                (serde_json::Value::Null, vec![]),
                (serde_json::json!(""), vec![]),
                (serde_json::json!([]), vec![]),
                (serde_json::json!(format!("{first:#x}")), vec![first]),
                (
                    serde_json::json!(format!(" {first:#x},\n{second:#x}\r\t ")),
                    vec![first, second],
                ),
                (
                    serde_json::json!([format!("{first:#x}"), format!("{second:#x}")]),
                    vec![first, second],
                ),
            ] {
                let wire = serde_json::json!({field:value}).to_string();
                let request: SubscribeRequest = serde_json::from_str(&wire).unwrap();
                let actual = if field == "tokenAddresses" {
                    request.token_addresses
                } else {
                    request.addresses
                };
                assert_eq!(actual, expected, "{field}");
            }
            for value in [
                serde_json::json!(true),
                serde_json::json!(42),
                serde_json::json!({}),
                serde_json::json!([null]),
                serde_json::json!([[]]),
                serde_json::json!([42]),
                serde_json::json!("0x01"),
            ] {
                let wire = serde_json::json!({field:value}).to_string();
                assert!(
                    serde_json::from_str::<SubscribeRequest>(&wire).is_err(),
                    "{wire}"
                );
            }
        }
        let wire = format!(
            r#"{{"type":"erc20Transfers","walletAddresses":["{first:#x}"],"minAmountRaw":"1","maxAmountRaw":2,"subscriptionScope":"dashboard","clientId":"kept-id","unknownExtension":{{"anything":true}}}}"#
        );
        let request: SubscribeRequest = serde_json::from_str(&wire).unwrap();
        assert_eq!(request.subscription_id.as_deref(), Some("kept-id"));
        assert_eq!(
            request.live_session_scope(),
            LiveSubscriptionScope::Dashboard
        );
        assert!(request.into_subscription().is_ok());
    }

    #[test]
    fn erc20_inputs_amount_wire_types_are_exact_and_optional() {
        for field in ["minAmount", "maxAmount", "minAmountRaw", "maxAmountRaw"] {
            for token in ["null", r#""""#, r#""  ""#] {
                let wire = format!("{{\"{field}\":{token}}}");
                let request: SubscribeRequest = serde_json::from_str(&wire).unwrap();
                assert!(request.min_amount.is_none() && request.max_amount.is_none());
            }
            for token in ["0", "100", "18446744073709551615", r#""0xF""#, r#""00015""#] {
                let wire = format!("{{\"{field}\":{token}}}");
                let request: SubscribeRequest = serde_json::from_str(&wire).unwrap();
                let expected = parse_u256_bound(token.trim_matches('"')).unwrap();
                let actual = if field.starts_with("min") {
                    request.min_amount
                } else {
                    request.max_amount
                };
                assert_eq!(actual, Some(expected), "{wire}");
            }
            for token in [
                "-1",
                "-0",
                "1.0",
                "1e2",
                "18446744073709551616",
                "true",
                "[]",
                "{}",
                r#""0x""#,
            ] {
                let wire = format!("{{\"{field}\":{token}}}");
                assert!(
                    serde_json::from_str::<SubscribeRequest>(&wire).is_err(),
                    "{wire}"
                );
            }
        }
    }

    #[test]
    fn erc20_inputs_uint256_boundaries_and_leading_zeroes() {
        const MAX: &str =
            "115792089237316195423570985008687907853269984665640564039457584007913129639935";
        const OVER: &str =
            "115792089237316195423570985008687907853269984665640564039457584007913129639936";
        assert_eq!(parse_u256_bound(MAX).unwrap(), [255; 32]);
        assert_eq!(
            parse_u256_bound(&format!("0x{}", "f".repeat(64))).unwrap(),
            [255; 32]
        );
        assert!(parse_u256_bound(OVER).is_err());
        assert!(parse_u256_bound(&"9".repeat(79)).is_err());
        assert_eq!(parse_u256_bound(&"0".repeat(128)).unwrap(), [0; 32]);
        assert_eq!(
            parse_u256_bound(&format!("{}15", "0".repeat(128))).unwrap(),
            parse_u256_bound("0xf").unwrap()
        );
        assert_eq!(parse_u256_bound(&format!("000{MAX}")).unwrap(), [255; 32]);
        assert!(parse_u256_bound(&format!("000{OVER}")).is_err());
        for invalid in ["", "-1", "+1", "1.1", "1e2", "0X1", "0xG", "00x", "١"] {
            assert!(parse_u256_bound(invalid).is_err(), "{invalid}");
        }
        let mut expected = [0; 32];
        expected[31] = 15;
        assert_eq!(parse_u256_bound("  0xF  ").unwrap(), expected);
        assert_eq!(parse_u256_bound("  00015  ").unwrap(), expected);
    }

    #[test]
    fn erc20_inputs_event_shape_and_zero_transfer_controls() {
        let token = Address::repeat_byte(0xBB);
        let subscription = Erc20TransferSubscription::new(vec![], vec![token], None, None).unwrap();
        let original = make_transfer_log(token, Address::ZERO, Address::ZERO, 0, 100);
        for mask in 0..16 {
            let mut row = original.clone();
            row.topic0 = (mask & 1 != 0).then_some(*ERC20_TRANSFER_TOPIC);
            row.topic1 = (mask & 2 != 0).then_some(B256::ZERO);
            row.topic2 = (mask & 4 != 0).then_some(B256::ZERO);
            row.topic3 = (mask & 8 != 0).then_some(B256::ZERO);
            assert_eq!(
                subscription.notification_for(&row).is_some(),
                mask == 7,
                "mask={mask}"
            );
        }
        for actual in [0, 31, 32, 33] {
            for declared in [0, 31, 32, 33] {
                let mut row = original.clone();
                row.data = Bytes::from(vec![0; actual]);
                row.data_len = declared;
                assert_eq!(
                    subscription.notification_for(&row).is_some(),
                    actual == 32 && declared == 32
                );
            }
        }
        for position in [1, 2] {
            let mut row = original.clone();
            let mut topic = [0; 32];
            topic[0] = 1;
            if position == 1 {
                row.topic1 = Some(B256::from(topic));
            } else {
                row.topic2 = Some(B256::from(topic));
            }
            assert!(subscription.notification_for(&row).is_none());
        }
        let notification = subscription.notification_for(&original).unwrap();
        assert_eq!(notification.from, format!("{:#x}", Address::ZERO));
        assert_eq!(notification.to, format!("{:#x}", Address::ZERO));
        assert_eq!(notification.raw_amount, format!("0x{}", "00".repeat(32)));
    }

    #[test]
    fn erc20_inputs_invalid_event_is_excluded_from_raw_hook_and_retained_history() {
        let token = Address::repeat_byte(0xBB);
        let subscription = Erc20TransferSubscription::new(vec![], vec![token], None, None).unwrap();
        let valid = make_transfer_log(token, Address::ZERO, Address::repeat_byte(1), 10, 100);
        let mut invalid = valid.clone();
        invalid.topic3 = Some(B256::ZERO);
        invalid.block_number = 101;
        let rows = [valid, invalid];
        let payload = Subscription::Erc20Transfers(subscription.clone())
            .payload_for(&rows)
            .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
        assert_eq!(payload.as_array().unwrap().len(), 1);
        assert_eq!(payload[0]["blockNumber"], 100);
        let logs = Subscription::Logs(Box::default())
            .payload_for(&rows)
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&logs)
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let manager = SubscriptionManager::new();
        let mut attachment = manager.upsert_live_transfer_session(
            Some("shape".into()),
            LiveSubscriptionScope::Service,
            subscription,
            false,
        );
        manager.notify(&rows);
        assert_eq!(attachment.receiver.try_recv().unwrap().len(), 1);
        let snapshot = manager.live_transfer_session("shape").unwrap();
        assert_eq!(snapshot.notifications.len(), 1);
        assert_eq!(snapshot.notifications[0].block_number, 100);
    }

    #[test]
    fn erc20_inputs_reject_literal_objects_disguised_as_values() {
        let address = format!("{:#x}", Address::repeat_byte(1));
        let raw = |encoded: String| serde_json::json!({"$serde_json::private::RawValue": encoded});
        let mut accepted = Vec::new();
        for (field, value) in [
            ("addresses", raw(serde_json::to_string(&address).unwrap())),
            (
                "walletAddresses",
                raw(serde_json::to_string(&vec![&address]).unwrap()),
            ),
            (
                "tokenAddresses",
                serde_json::json!([raw(serde_json::to_string(&address).unwrap())]),
            ),
            ("minAmount", raw("\"100\"".into())),
            ("maxAmountRaw", raw("100".into())),
            ("minAmountRaw", raw("null".into())),
        ] {
            let mut request = serde_json::json!({"type":"erc20Transfers"});
            request[field] = value;
            let wire = request.to_string();
            if serde_json::from_str::<SubscribeRequest>(&wire).is_ok() {
                accepted.push(field);
            }
        }
        assert!(
            accepted.is_empty(),
            "literal object accepted in: {accepted:?}"
        );
    }

    #[test]
    fn erc20_inputs_reject_empty_hex_amount_digits() {
        assert!(
            parse_u256_bound("0x").is_err(),
            "hex prefix without digits is not an amount"
        );
    }

    #[test]
    fn erc20_inputs_require_exact_transfer_topic_count() {
        let token = Address::repeat_byte(0xBB);
        let tracked = Address::repeat_byte(1);
        let subscription =
            Erc20TransferSubscription::new(vec![tracked], vec![token], None, None).unwrap();
        let mut row = make_transfer_log(token, tracked, Address::repeat_byte(2), 10, 100);
        assert!(subscription.notification_for(&row).is_some());
        row.topic3 = Some(B256::ZERO);
        assert!(
            subscription.notification_for(&row).is_none(),
            "four-topic row is not the ERC20 Transfer ABI"
        );
    }

    struct CloseDropProbe(Arc<std::sync::atomic::AtomicBool>);

    impl Drop for CloseDropProbe {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn delivery_pending_terminal_close_expires_and_releases_future() {
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = CloseDropProbe(Arc::clone(&dropped));
        tokio::time::timeout(
            Duration::from_secs(3),
            close_lagged(|message| async move {
                let Message::Close(Some(frame)) = message else {
                    panic!("expected close")
                };
                assert_eq!(frame.code, close_code::AGAIN);
                assert_eq!(
                    frame.reason,
                    "Live stream fell behind; reconnect and reconcile stored logs."
                );
                let _probe = probe;
                std::future::pending::<Result<(), axum::Error>>().await
            }),
        )
        .await
        .expect("terminal close must be bounded");
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn delivery_terminal_close_cancellation_detaches_original_session() {
        let manager = SubscriptionManager::new();
        let subscription =
            Erc20TransferSubscription::new(vec![Address::repeat_byte(1)], vec![], None, None)
                .unwrap();
        let attachment = manager.upsert_live_transfer_session(
            Some("cancel-close".into()),
            LiveSubscriptionScope::Dashboard,
            subscription.clone(),
            true,
        );
        let owned = manager.clone();
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let probe = CloseDropProbe(Arc::clone(&dropped));
        let (ready, started) = tokio::sync::oneshot::channel();
        let work = tokio::spawn(async move {
            let _detach = DetachSession(&owned, attachment.id, attachment.identity);
            close_lagged(|_| async move {
                let _probe = probe;
                ready.send(()).unwrap();
                std::future::pending::<Result<(), axum::Error>>().await
            })
            .await;
        });
        started.await.unwrap();
        assert!(manager.remove_live_transfer_session("cancel-close"));
        let replacement = manager.upsert_live_transfer_session(
            Some("cancel-close".into()),
            LiveSubscriptionScope::Dashboard,
            subscription,
            true,
        );
        work.abort();
        assert!(work.await.unwrap_err().is_cancelled());
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(
            manager
                .live_transfer_session("cancel-close")
                .unwrap()
                .active_connections,
            1
        );
        manager.detach_live_transfer_session(&replacement.id, &replacement.identity);
        assert_eq!(
            manager
                .live_transfer_session("cancel-close")
                .unwrap()
                .active_connections,
            0
        );
    }

    #[tokio::test]
    async fn delivery_terminal_close_success_and_failure_finish_without_retry() {
        for fail in [false, true] {
            let calls = std::sync::atomic::AtomicUsize::new(0);
            close_lagged(|message| {
                calls.fetch_add(1, Ordering::SeqCst);
                assert!(matches!(message, Message::Close(Some(_))));
                std::future::ready(if fail {
                    Err(axum::Error::new(std::io::Error::other("closed")))
                } else {
                    Ok(())
                })
            })
            .await;
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn delivery_healthy_raw_batches_remain_chronological() {
        let manager = SubscriptionManager::new();
        let mut client = DeliveryClient::connect(manager.clone(), r#"{"type":"logs"}"#).await;
        assert_eq!(client.frame().await.0, 1);
        manager.notify(&[make_log(1, 10), make_log(1, 11)]);
        let (opcode, data) = client.frame().await;
        assert_eq!(opcode, 1);
        let rows: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(rows[0]["blockNumber"], "0xa");
        assert_eq!(rows[1]["blockNumber"], "0xb");
        assert_eq!(manager.inner.sender.receiver_count(), 1);
        drop(client);
        tokio::time::timeout(Duration::from_secs(3), async {
            while manager.inner.sender.receiver_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn delivery_retained_ack_history_and_live_order_are_preserved() {
        let manager = SubscriptionManager::new();
        let token = Address::repeat_byte(0xBB);
        let tracked = Address::repeat_byte(0xA1);
        let recipient = Address::repeat_byte(2);
        let subscription =
            Erc20TransferSubscription::new(vec![tracked], vec![token], None, None).unwrap();
        drop(manager.upsert_live_transfer_session(
            Some("healthy-service".into()),
            LiveSubscriptionScope::Service,
            subscription,
            false,
        ));
        manager.notify(&[
            make_transfer_log(token, tracked, recipient, 1, 8),
            make_transfer_log(token, tracked, recipient, 2, 9),
        ]);
        let request = format!(
            r#"{{"type":"erc20Transfers","subscriptionId":"healthy-service","scope":"service","addresses":["{tracked:#x}"]}}"#
        );
        let mut client = DeliveryClient::connect(manager.clone(), &request).await;
        let (opcode, ack) = client.frame().await;
        assert_eq!(opcode, 1);
        let ack: serde_json::Value = serde_json::from_slice(&ack).unwrap();
        assert_eq!(ack["activeConnections"], 1);
        assert_eq!(ack["notifications"][0]["blockNumber"], 9);
        assert_eq!(ack["notifications"][1]["blockNumber"], 8);
        manager.notify(&[
            make_transfer_log(token, tracked, recipient, 3, 10),
            make_transfer_log(token, tracked, recipient, 4, 11),
        ]);
        let (opcode, data) = client.frame().await;
        assert_eq!(opcode, 1);
        let rows: serde_json::Value = serde_json::from_slice(&data).unwrap();
        assert_eq!(rows[0]["blockNumber"], 10);
        assert_eq!(rows[1]["blockNumber"], 11);
        drop(client);
        tokio::time::timeout(Duration::from_secs(3), async {
            while manager
                .live_transfer_session("healthy-service")
                .unwrap()
                .active_connections
                != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let snapshot = manager.live_transfer_session("healthy-service").unwrap();
        assert_eq!(snapshot.notifications.len(), 4);
        assert_eq!(snapshot.notifications[0].block_number, 11);
        assert_eq!(snapshot.expires_in_seconds, None);
    }

    // Tiny loopback WebSocket transport: no external client or production listener.
    struct DeliveryClient {
        stream: tokio::net::TcpStream,
        server: tokio::task::JoinHandle<()>,
        _directory: tempfile::TempDir,
    }

    impl Drop for DeliveryClient {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    impl DeliveryClient {
        async fn connect(manager: SubscriptionManager, request: &str) -> Self {
            tokio::time::timeout(
                Duration::from_secs(3),
                Self::connect_inner(manager, request),
            )
            .await
            .expect("bounded local handshake")
        }

        async fn connect_inner(manager: SubscriptionManager, request: &str) -> Self {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let directory = tempfile::tempdir().unwrap();
            let storage =
                logex_storage::PartitionManager::open(logex_storage::PartitionManagerConfig {
                    data_dir: directory.path().to_path_buf(),
                    partition_target_rows: 100,
                    compaction_safety_margin_blocks: 2048,
                })
                .unwrap();
            let state = Arc::new(AppState::new(storage, Some(manager), Default::default()));
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let app = axum::Router::new()
                .route("/ws", axum::routing::get(handle_ws_upgrade))
                .with_state(state);
            let server = tokio::spawn(async move {
                axum::serve(listener, app).await.unwrap();
            });
            let stream = tokio::net::TcpStream::connect(address).await.unwrap();
            let mut client = Self {
                stream,
                server,
                _directory: directory,
            };
            let stream = &mut client.stream;
            stream.write_all(b"GET /ws HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n").await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(stream.read_u8().await.unwrap());
                assert!(header.len() < 4096);
            }
            assert!(header.starts_with(b"HTTP/1.1 101"));
            let mut frame = vec![0x81];
            if request.len() < 126 {
                frame.push(0x80 | request.len() as u8);
            } else {
                frame.push(0x80 | 126);
                frame.extend_from_slice(&(request.len() as u16).to_be_bytes());
            }
            frame.extend_from_slice(&[0; 4]);
            frame.extend_from_slice(request.as_bytes());
            stream.write_all(&frame).await.unwrap();
            client
        }

        async fn frame(&mut self) -> (u8, Vec<u8>) {
            use tokio::io::AsyncReadExt;
            tokio::time::timeout(Duration::from_secs(3), async {
                let opcode = self.stream.read_u8().await.unwrap() & 0x0f;
                let mut length = u64::from(self.stream.read_u8().await.unwrap());
                assert_eq!(length & 0x80, 0);
                if length == 126 {
                    length = u64::from(self.stream.read_u16().await.unwrap());
                } else if length == 127 {
                    length = self.stream.read_u64().await.unwrap();
                }
                assert!(length < 16_384);
                let mut data = vec![0; length as usize];
                self.stream.read_exact(&mut data).await.unwrap();
                (opcode, data)
            })
            .await
            .expect("bounded local frame")
        }
    }

    #[tokio::test]
    async fn delivery_raw_lag_is_terminal_on_wire() {
        let (sender, _) = broadcast::channel(1);
        let manager = SubscriptionManager {
            inner: Arc::new(SubscriptionManagerInner {
                sender,
                live_transfers: Mutex::default(),
                next_session_id: AtomicU64::new(1),
            }),
        };
        let mut client = DeliveryClient::connect(manager.clone(), r#"{"type":"logs"}"#).await;
        assert_eq!(client.frame().await.0, 1);
        // No await between sends: the current-thread receiver must observe a gap.
        manager.notify(&[make_log(1, 10)]);
        manager.notify(&[make_log(1, 11)]);
        let (opcode, data) = client.frame().await;
        assert_eq!(
            opcode, 8,
            "lag must close, not deliver a successful partial batch"
        );
        assert_eq!(u16::from_be_bytes([data[0], data[1]]), 1013);
        assert!(String::from_utf8_lossy(&data[2..]).contains("reconcile"));
        assert_eq!(manager.inner.sender.receiver_count(), 0);
    }

    #[tokio::test]
    async fn delivery_retained_lag_is_terminal_and_detaches_on_wire() {
        let manager = SubscriptionManager::new();
        let tracked = Address::repeat_byte(0xA1);
        let token = Address::repeat_byte(0xBB);
        let subscription =
            Erc20TransferSubscription::new(vec![tracked], vec![token], None, None).unwrap();
        drop(manager.upsert_live_transfer_session(
            Some("lag-session".into()),
            LiveSubscriptionScope::Dashboard,
            subscription,
            false,
        ));
        {
            let mut sessions = manager.inner.live_transfers.lock().unwrap();
            sessions.sessions.get_mut("lag-session").unwrap().sender = broadcast::channel(1).0;
        }
        let request = format!(
            r#"{{"type":"erc20Transfers","subscriptionId":"lag-session","scope":"dashboard","walletAddresses":["{tracked:#x}"],"tokenAddresses":["{token:#x}"]}}"#
        );
        let mut client = DeliveryClient::connect(manager.clone(), &request).await;
        let (opcode, ack) = client.frame().await;
        assert_eq!(opcode, 1);
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&ack).unwrap()["status"],
            "subscribed"
        );
        manager.notify(&[make_transfer_log(
            token,
            tracked,
            Address::repeat_byte(2),
            1,
            10,
        )]);
        manager.notify(&[make_transfer_log(
            token,
            tracked,
            Address::repeat_byte(2),
            2,
            11,
        )]);
        let (opcode, data) = client.frame().await;
        assert_eq!(
            opcode, 8,
            "retained lag must close rather than skip live data"
        );
        assert_eq!(u16::from_be_bytes([data[0], data[1]]), 1013);
        let snapshot = manager.live_transfer_session("lag-session").unwrap();
        assert_eq!(snapshot.active_connections, 0);
        assert!(snapshot.expires_in_seconds.is_some());
        assert_eq!(snapshot.notifications.len(), 2);
        assert_eq!(snapshot.notifications[0].block_number, 11);
    }

    #[test]
    fn raw_log_numeric_bounds_and_empty_addresses_match_expected_rows() {
        let mut mismatches = Vec::new();
        for filter in [
            r#"{"fromBlock":"0xa","toBlock":"0xa"}"#,
            r#"{"fromBlock":"0xa","toBlock":"0xa","address":[]}"#,
        ] {
            let request: SubscribeRequest =
                serde_json::from_str(&format!(r#"{{"type":"logs","filter":{filter}}}"#)).unwrap();
            let subscription = request.into_subscription().unwrap();
            let payload = subscription.payload_for(&[
                make_log(0xAA, 9),
                make_log(0xAA, 10),
                make_log(0xAA, 11),
            ]);
            let rows: Option<serde_json::Value> =
                payload.map(|payload| serde_json::from_str(&payload).unwrap());
            let blocks: Vec<_> = rows
                .as_ref()
                .and_then(|rows| rows.as_array())
                .into_iter()
                .flatten()
                .map(|row| row["blockNumber"].clone())
                .collect();
            if blocks != vec![serde_json::json!("0xa")] {
                mismatches.push(format!("filter={filter}, blocks={blocks:?}"));
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn raw_log_subscription_rejects_conflicts_excess_topics_and_named_bounds() {
        let mut accepted = Vec::new();
        for filter in [
            r#"{"topics":[null,null,null,null,null]}"#.to_owned(),
            format!(
                r#"{{"blockHash":"0x{}","fromBlock":"0xa"}}"#,
                "00".repeat(32)
            ),
            r#"{"fromBlock":"latest"}"#.to_owned(),
        ] {
            let request: SubscribeRequest =
                serde_json::from_str(&format!(r#"{{"type":"logs","filter":{filter}}}"#)).unwrap();
            if request.into_subscription().is_ok() {
                accepted.push(filter);
            }
        }
        assert!(accepted.is_empty(), "accepted: {accepted:?}");
    }

    #[test]
    fn raw_log_wildcard_arity_and_query_pagination_fields_preserve_stream_semantics() {
        let rows: Vec<_> = (0..5)
            .map(|count| {
                let mut row = make_log(0xAA, 10);
                row.log_index = count;
                row.topic0 = (count > 0).then_some(B256::ZERO);
                row.topic1 = (count > 1).then_some(B256::ZERO);
                row.topic2 = (count > 2).then_some(B256::ZERO);
                row.topic3 = (count > 3).then_some(B256::ZERO);
                row
            })
            .collect();
        for (topics, required) in [
            (serde_json::json!(null), 0),
            (serde_json::json!([]), 0),
            (serde_json::json!([null]), 1),
            (serde_json::json!([[]]), 1),
            (serde_json::json!([null, null, null, null]), 4),
            (
                serde_json::json!([[null, format!("{:#x}", B256::repeat_byte(8))]]),
                1,
            ),
        ] {
            let wire = serde_json::json!({"type":"logs","filter":{"topics":topics,"address":[],"fromBlock":"earliest","toBlock":"0xa","limit":0,"offset":10001}}).to_string();
            let request: SubscribeRequest = serde_json::from_str(&wire).unwrap();
            let subscription = request.into_subscription().unwrap();
            let payload = subscription.payload_for(&rows).unwrap();
            let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
            let ids: Vec<_> = payload
                .as_array()
                .unwrap()
                .iter()
                .map(|row| {
                    u32::from_str_radix(
                        row["logIndex"].as_str().unwrap().trim_start_matches("0x"),
                        16,
                    )
                    .unwrap()
                })
                .collect();
            assert_eq!(ids, (required..5).collect::<Vec<_>>());
        }
    }

    #[test]
    fn raw_log_decoder_rejects_literal_nested_invalid_filter_shapes() {
        for filter in [
            r#"[null,null,null,[],null,null,0]"#,
            r#"{"topics":[[null,true]]}"#,
            r#"{"address":{"$serde_json::private::RawValue":"\"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\""}}"#,
            r#"{"topics":[{"$serde_json::private::RawValue":"[]"}]}"#,
            r#"{"fromBlock":"safe"}"#,
            r#"{"fromBlock":"finalized"}"#,
            r#"{"fromBlock":"pending"}"#,
        ] {
            let wire = format!(r#"{{"type":"logs","filter":{filter}}}"#);
            let result = serde_json::from_str::<SubscribeRequest>(&wire)
                .map_err(|error| error.to_string())
                .and_then(SubscribeRequest::into_subscription);
            assert!(result.is_err(), "accepted {filter}");
        }
    }

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
    fn reorg_raw_and_ephemeral_filters_preserve_change_kind() {
        let manager = SubscriptionManager::new();
        let mut receiver = manager.subscribe();
        let wallet = Address::repeat_byte(1);
        let token = Address::repeat_byte(2);
        let old = make_transfer_log(token, wallet, Address::ZERO, 5, 10);
        let other = make_transfer_log(Address::repeat_byte(3), wallet, Address::ZERO, 5, 10);
        manager.notify(std::slice::from_ref(&old));
        manager.notify_removed(&[old.clone(), other]);
        let mut replacement = old.clone();
        replacement.block_hash = B256::repeat_byte(4);
        manager.notify(&[replacement.clone()]);
        assert!(!receiver.try_recv().unwrap().removed);
        let removed = receiver.try_recv().unwrap();
        assert!(removed.removed);
        let added = receiver.try_recv().unwrap();
        assert!(!added.removed);
        assert_eq!(added.rows[0].block_hash, replacement.block_hash);
        let raw = Subscription::Logs(Box::new(
            EthFilter {
                address: crate::eth_filter::AddressFilter::Single(token),
                ..Default::default()
            }
            .to_stream_filter()
            .unwrap(),
        ));
        let transfer = Subscription::Erc20Transfers(
            Erc20TransferSubscription::new(vec![], vec![token], None, None).unwrap(),
        );
        for subscription in [raw, transfer] {
            let payload: serde_json::Value = serde_json::from_str(
                &subscription
                    .payload_for_change(&removed.rows, removed.removed)
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(payload.as_array().unwrap().len(), 1);
            assert_eq!(payload[0]["removed"], true);
            assert_eq!(
                payload[0]["blockHash"],
                format_hex(old.block_hash.as_slice())
            );
        }
        manager.notify_removed(&[]);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn reorg_retained_removes_across_filter_changes_and_eviction() {
        let manager = SubscriptionManager::new();
        let token = Address::repeat_byte(2);
        let original = make_transfer_log(token, Address::repeat_byte(1), Address::ZERO, 5, 10);
        let original_filter =
            Erc20TransferSubscription::new(vec![], vec![token], None, None).unwrap();
        let mut attachment = manager.upsert_live_transfer_session(
            Some("changed".into()),
            LiveSubscriptionScope::Service,
            original_filter,
            true,
        );
        manager.notify(std::slice::from_ref(&original));
        assert!(!attachment.receiver.try_recv().unwrap()[0].removed);
        let changed_filter =
            Erc20TransferSubscription::new(vec![], vec![Address::repeat_byte(3)], None, None)
                .unwrap();
        manager.upsert_live_transfer_session(
            Some("changed".into()),
            LiveSubscriptionScope::Service,
            changed_filter,
            false,
        );
        manager.notify_removed(std::slice::from_ref(&original));
        assert!(attachment.receiver.try_recv().unwrap()[0].removed);
        assert!(
            manager
                .live_transfer_session("changed")
                .unwrap()
                .notifications
                .is_empty()
        );
        // Model a past delivery no longer present in bounded history. Removal
        // delivery must not depend on either history membership or today's filter.
        manager.notify_removed(std::slice::from_ref(&original));
        assert!(attachment.receiver.try_recv().unwrap()[0].removed);
        let mut invalid = original.clone();
        invalid.topic3 = Some(B256::ZERO);
        manager.notify_removed(&[invalid]);
        assert!(attachment.receiver.try_recv().is_err());
        let snapshot = manager.live_transfer_session("changed").unwrap();
        assert!(snapshot.notifications.is_empty());
        assert_eq!(snapshot.dropped_notifications, 0);
        manager.detach_live_transfer_session(&attachment.id, &attachment.identity);
    }

    #[test]
    fn reorg_snapshots_reconcile_disconnected_and_attaching_sessions() {
        for scope in [
            LiveSubscriptionScope::Service,
            LiveSubscriptionScope::Dashboard,
        ] {
            let manager = SubscriptionManager::new();
            let token = Address::repeat_byte(2);
            let subscription =
                Erc20TransferSubscription::new(vec![], vec![token], None, None).unwrap();
            let mut before = manager.upsert_live_transfer_session(
                Some("restore".into()),
                scope,
                subscription.clone(),
                true,
            );
            let old = make_transfer_log(token, Address::ZERO, Address::ZERO, 5, 10);
            let mut replacement = old.clone();
            replacement.block_hash = B256::repeat_byte(4);
            manager.notify(std::slice::from_ref(&old));
            before.receiver.try_recv().unwrap();
            manager.detach_live_transfer_session(&before.id, &before.identity);
            drop(before);
            manager.notify_removed(&[old]);
            manager.notify(&[replacement.clone()]);
            let restored = manager.upsert_live_transfer_session(
                Some("restore".into()),
                scope,
                subscription,
                true,
            );
            assert_eq!(restored.snapshot.notifications.len(), 1);
            assert_eq!(
                restored.snapshot.notifications[0].block_hash,
                format_hex(replacement.block_hash.as_slice())
            );
            assert!(!restored.snapshot.notifications[0].removed);
            assert_eq!(restored.snapshot.dropped_notifications, 0);
            let mut receiver = restored.receiver;
            assert!(receiver.try_recv().is_err());
            manager.notify_removed(&[replacement]);
            assert!(receiver.try_recv().unwrap()[0].removed);
            assert!(
                manager
                    .live_transfer_session("restore")
                    .unwrap()
                    .notifications
                    .is_empty()
            );
            manager.detach_live_transfer_session(&restored.id, &restored.identity);
        }
    }

    #[test]
    fn reorg_history_removal_reconciles_without_tombstone() {
        let subscription =
            Erc20TransferSubscription::new(vec![Address::ZERO], vec![], None, None).unwrap();
        let mut session = LiveTransferSession::new(subscription, LiveSubscriptionScope::Service);
        let first = history_notification(1);
        let mut replacement = first.clone();
        replacement.block_hash = format!("{:#x}", B256::repeat_byte(2));
        session.push_notifications(&[first.clone(), replacement.clone()]);
        let mut removed = first;
        removed.removed = true;
        session.push_notifications(&[removed.clone(), removed]);
        assert_eq!(session.notifications.len(), 1);
        assert_eq!(session.notifications[0].block_hash, replacement.block_hash);
        assert!(!session.notifications[0].removed);
        assert_eq!(session.dropped_notifications, 0);
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
        assert_eq!(received1.rows.len(), 2);

        let received2 = rx2.try_recv().unwrap();
        assert_eq!(received2.rows.len(), 2);
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

        mgr.detach_live_transfer_session("dashboard-1", &attachment.identity);
        {
            let mut sessions = mgr.live_transfers();
            sessions.sessions.get_mut("dashboard-1").unwrap().expires_at =
                Some(Instant::now() - Duration::from_secs(1));
        }
        mgr.notify(&[make_log(0xAA, 11)]);
        assert!(mgr.live_transfer_session("dashboard-1").is_none());
    }

    #[tokio::test]
    async fn dropping_pending_session_work_releases_its_connection() {
        let manager = SubscriptionManager::new();
        let subscription =
            Erc20TransferSubscription::new(vec![Address::repeat_byte(0xA1)], vec![], None, None)
                .unwrap();
        let attachment = manager.upsert_live_transfer_session(
            Some("owned-session".into()),
            LiveSubscriptionScope::Dashboard,
            subscription,
            true,
        );
        let owned = manager.clone();
        let (ready, started) = tokio::sync::oneshot::channel();
        let work = tokio::spawn(async move {
            let _detach = DetachSession(&owned, attachment.id, attachment.identity);
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        started.await.unwrap();
        assert_eq!(
            manager
                .live_transfer_session("owned-session")
                .unwrap()
                .active_connections,
            1
        );
        work.abort();
        assert!(work.await.unwrap_err().is_cancelled());
        let snapshot = manager.live_transfer_session("owned-session").unwrap();
        assert_eq!(snapshot.active_connections, 0);
        assert!(snapshot.expires_in_seconds.is_some());
    }

    #[test]
    fn session_identity_survives_delete_and_recreate() {
        let manager = SubscriptionManager::new();
        let subscription =
            Erc20TransferSubscription::new(vec![Address::repeat_byte(0xA1)], vec![], None, None)
                .unwrap();
        let mut old = manager.upsert_live_transfer_session(
            Some("reused".into()),
            LiveSubscriptionScope::Dashboard,
            subscription.clone(),
            true,
        );
        let old_guard = DetachSession(&manager, old.id.clone(), Arc::clone(&old.identity));
        assert!(manager.remove_live_transfer_session(&old.id));
        assert!(matches!(
            old.receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Closed)
        ));
        let replacement = manager.upsert_live_transfer_session(
            Some(old.id.clone()),
            LiveSubscriptionScope::Dashboard,
            subscription,
            true,
        );
        let replacement_guard = DetachSession(
            &manager,
            replacement.id.clone(),
            Arc::clone(&replacement.identity),
        );
        drop(old_guard);
        let snapshot = manager.live_transfer_session(&replacement.id).unwrap();
        assert_eq!(
            snapshot.active_connections, 1,
            "old owner detached replacement"
        );
        assert!(snapshot.expires_in_seconds.is_none());
        drop(replacement_guard);
        let snapshot = manager.live_transfer_session(&replacement.id).unwrap();
        assert_eq!(snapshot.active_connections, 0);
        assert!(snapshot.expires_in_seconds.is_some());
    }

    #[test]
    fn named_ephemeral_session_ends_with_its_last_owner() {
        let manager = SubscriptionManager::new();
        let request: SubscribeRequest = serde_json::from_value(serde_json::json!({
            "type": "erc20Transfers", "subscriptionId": "named-ephemeral",
            "addresses": [format!("{:#x}", Address::repeat_byte(0xA1))]
        }))
        .unwrap();
        let scope = request.live_session_scope();
        assert_eq!(scope, LiveSubscriptionScope::Ephemeral);
        let id = request.subscription_id.clone();
        let subscription = request.into_erc20_transfer_subscription().unwrap();
        let first = manager.upsert_live_transfer_session(id, scope, subscription.clone(), true);
        let first_guard = DetachSession(&manager, first.id.clone(), Arc::clone(&first.identity));
        let second =
            manager.upsert_live_transfer_session(Some(first.id.clone()), scope, subscription, true);
        let second_guard = DetachSession(&manager, second.id.clone(), Arc::clone(&second.identity));
        assert_eq!(
            manager
                .live_transfer_session(&first.id)
                .unwrap()
                .active_connections,
            2
        );
        drop(first_guard);
        assert_eq!(
            manager
                .live_transfer_session(&second.id)
                .unwrap()
                .active_connections,
            1
        );
        drop(second_guard);
        assert!(
            manager.live_transfer_session(&second.id).is_none(),
            "ephemeral session survived its last owner"
        );
    }

    #[test]
    fn retained_upsert_preserves_owners_history_and_uses_current_configuration() {
        let manager = SubscriptionManager::new();
        let wallet = Address::repeat_byte(0xA1);
        let token = Address::repeat_byte(0xBB);
        let other_token = Address::repeat_byte(0xCC);
        let initial =
            Erc20TransferSubscription::new(vec![wallet], vec![token], None, None).unwrap();
        let mut first = manager.upsert_live_transfer_session(
            Some("updated".into()),
            LiveSubscriptionScope::Dashboard,
            initial,
            true,
        );
        let first_guard = DetachSession(&manager, first.id.clone(), Arc::clone(&first.identity));
        manager.notify(&[make_transfer_log(token, wallet, wallet, 1, 1)]);
        first.receiver.try_recv().unwrap();
        let updated =
            Erc20TransferSubscription::new(vec![wallet], vec![other_token], None, None).unwrap();
        let second = manager.upsert_live_transfer_session(
            Some(first.id.clone()),
            LiveSubscriptionScope::Service,
            updated,
            true,
        );
        let second_guard = DetachSession(&manager, second.id.clone(), Arc::clone(&second.identity));
        assert!(Arc::ptr_eq(&first.identity, &second.identity));
        assert_eq!(second.snapshot.notifications.len(), 1);
        assert_eq!(second.snapshot.active_connections, 2);
        manager.notify(&[make_transfer_log(token, wallet, wallet, 1, 2)]);
        assert!(matches!(
            first.receiver.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        manager.notify(&[
            make_transfer_log(other_token, wallet, wallet, 1, 3),
            make_transfer_log(other_token, wallet, wallet, 1, 4),
        ]);
        let batch = first.receiver.try_recv().unwrap();
        assert_eq!(
            batch.iter().map(|n| n.block_number).collect::<Vec<_>>(),
            vec![3, 4]
        );
        let snapshot = manager.live_transfer_session(&second.id).unwrap();
        assert_eq!(
            snapshot
                .notifications
                .iter()
                .map(|n| n.block_number)
                .collect::<Vec<_>>(),
            vec![4, 3, 1]
        );
        drop(first_guard);
        assert_eq!(
            manager
                .live_transfer_session(&second.id)
                .unwrap()
                .active_connections,
            1
        );
        drop(second_guard);
        let snapshot = manager.live_transfer_session(&second.id).unwrap();
        assert_eq!(snapshot.scope, LiveSubscriptionScope::Service);
        assert_eq!(snapshot.active_connections, 0);
        assert!(snapshot.expires_in_seconds.is_none());
    }

    fn history_session() -> LiveTransferSession {
        LiveTransferSession::new(
            Erc20TransferSubscription::new(vec![Address::repeat_byte(0xA1)], vec![], None, None)
                .unwrap(),
            LiveSubscriptionScope::Service,
        )
    }

    fn history_notification(index: u32) -> Erc20TransferNotification {
        let mut notification =
            Erc20TransferSubscription::new(vec![Address::repeat_byte(0xA1)], vec![], None, None)
                .unwrap()
                .notification_for(&make_transfer_log(
                    Address::repeat_byte(0xBB),
                    Address::repeat_byte(0xA1),
                    Address::repeat_byte(0xC1),
                    1,
                    1,
                ))
                .unwrap();
        notification.log_index = index;
        notification
    }

    #[test]
    fn retained_history_is_independent_of_publication_batches() {
        let mut batched = history_session();
        batched.push_notifications(&[history_notification(0), history_notification(1)]);
        batched.push_notifications(&[history_notification(2)]);
        let mut individual = history_session();
        for index in 0..3 {
            individual.push_notifications(&[history_notification(index)]);
        }
        let indexes = |session: &LiveTransferSession| {
            session
                .notifications
                .iter()
                .map(|n| n.log_index)
                .collect::<Vec<_>>()
        };
        assert_eq!(indexes(&batched), indexes(&individual));
        assert_eq!(indexes(&batched), vec![2, 1, 0]);
    }

    #[test]
    fn retained_history_evicts_oldest_within_and_across_batches() {
        let mut session = history_session();
        let prototype = history_notification(0);
        let notifications: Vec<_> = (0..=LIVE_TRANSFER_HISTORY_LIMIT as u32)
            .map(|index| {
                let mut notification = prototype.clone();
                notification.log_index = index;
                notification
            })
            .collect();
        session.push_notifications(&notifications);
        assert_eq!(session.notifications.len(), LIVE_TRANSFER_HISTORY_LIMIT);
        assert_eq!(session.dropped_notifications, 1);
        assert_eq!(
            session.notifications.front().unwrap().log_index,
            LIVE_TRANSFER_HISTORY_LIMIT as u32
        );
        assert_eq!(session.notifications.back().unwrap().log_index, 1);
        session.push_notifications(&[history_notification(LIVE_TRANSFER_HISTORY_LIMIT as u32 + 1)]);
        assert_eq!(session.dropped_notifications, 2);
        assert_eq!(session.notifications.back().unwrap().log_index, 2);
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
            .rows
            .iter()
            .filter(|row| {
                logex_query::matches_native_filter(row, &filter.to_stream_filter().unwrap())
            })
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
