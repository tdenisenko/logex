use crate::{
    ConsensusLightClientStatus, ExecutionAnchor, ExecutionBlockMarker, WeakSubjectivityCheckpoint,
};
use serde::Serialize;

/// High-level runtime state for the node.
#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeState {
    #[default]
    Starting,
    Discovering,
    Connecting,
    Syncing,
    WaitingForConsensus,
    Synced,
    Disconnected,
    Reconnecting,
}

impl NodeState {
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Starting => "Starting",
            Self::Discovering => "Discovering",
            Self::Connecting => "Connecting",
            Self::Syncing => "Syncing",
            Self::WaitingForConsensus => "Waiting For Consensus",
            Self::Synced => "Synced",
            Self::Disconnected => "Disconnected",
            Self::Reconnecting => "Reconnecting",
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ConsensusNetworkStatus {
    /// Base64-encoded local ENR advertised on the consensus network.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_enr: Option<String>,
    /// Local discovery node id, derived from the ENR public key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_node_id: Option<String>,
    /// UDP discovery port bound for discv5.
    pub discovery_port: u16,
    /// TCP libp2p port advertised in the ENR for future req/resp and gossip sessions.
    pub p2p_port: u16,
    /// Local libp2p peer id derived from the consensus networking key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_peer_id: Option<String>,
    /// Maximum retained dialable consensus peers.
    pub max_peers: usize,
    /// Number of configured bootnodes.
    pub bootnode_count: usize,
    /// Number of unique peers observed by discovery in this run.
    pub discovered_peers: usize,
    /// Number of discovered peers that advertise a TCP port and are eligible as dial candidates.
    pub dialable_peers: usize,
    /// Number of ENRs currently stored in the discovery routing table.
    pub routing_table_peers: usize,
    /// Number of active UDP discovery sessions currently established.
    pub active_sessions: usize,
    /// Number of libp2p consensus peers currently connected over TCP.
    pub connected_peer_sessions: usize,
    /// Number of libp2p consensus peers currently being dialed.
    pub dialing_peer_sessions: usize,
    /// Number of discovered peers remembered as useful CL candidates from prior sessions.
    pub preferred_peers: usize,
    /// Number of peers currently cooling down before another dial attempt.
    pub cooldown_peers: usize,
    /// Number of peers ignored for the current run because identify or RPC behaviour proved them irrelevant.
    pub ignored_peers: usize,
    /// Number of peers deferred until bootstrap has landed because they only advertise post-bootstrap work.
    pub deferred_until_post_bootstrap_peers: usize,
    /// Number of connected peers for which identify information has been recorded.
    pub identified_peers: usize,
    /// Number of identified peers that advertise the Status RPC.
    pub status_capable_peers: usize,
    /// Number of identified peers that advertise the MetaData RPC.
    pub metadata_capable_peers: usize,
    /// Number of identified peers that advertise the light-client bootstrap RPC.
    pub bootstrap_capable_peers: usize,
    /// Number of identified peers that advertise the light-client updates-by-range RPC.
    pub updates_by_range_capable_peers: usize,
    /// Number of identified peers that advertise the light-client finality update RPC.
    pub finality_update_capable_peers: usize,
    /// Number of identified peers that advertise the light-client optimistic update RPC.
    pub optimistic_update_capable_peers: usize,
    /// Number of identified peers that advertise the beacon-blocks-by-range RPC.
    pub beacon_blocks_by_range_capable_peers: usize,
    /// Number of identified peers that advertise the beacon-blocks-by-root RPC.
    pub beacon_blocks_by_root_capable_peers: usize,
    /// Number of peers that answered the initial Status req/resp handshake.
    pub status_peers: usize,
    /// Number of peers that answered the MetaData req/resp handshake.
    pub metadata_peers: usize,
    /// Number of peers that served a light-client bootstrap payload.
    pub bootstrap_peers: usize,
    /// Number of peers that served a light-client updates-by-range response stream.
    pub updates_by_range_peers: usize,
    /// Number of peers that served a light-client finality update payload.
    pub finality_update_peers: usize,
    /// Number of peers that served a light-client optimistic update payload.
    pub optimistic_update_peers: usize,
    /// Number of peers that served beacon-blocks-by-range response streams.
    pub beacon_blocks_by_range_peers: usize,
    /// Number of peers that served beacon-blocks-by-root response streams.
    pub beacon_blocks_by_root_peers: usize,
    /// Number of outbound light-client RPC requests currently in flight.
    pub pending_rpc_requests: usize,
    /// Number of in-flight Status requests.
    pub pending_status_requests: usize,
    /// Number of in-flight MetaData requests.
    pub pending_metadata_requests: usize,
    /// Number of in-flight light-client bootstrap requests.
    pub pending_bootstrap_requests: usize,
    /// Number of in-flight light-client updates-by-range requests.
    pub pending_updates_by_range_requests: usize,
    /// Number of in-flight light-client finality-update requests.
    pub pending_finality_update_requests: usize,
    /// Number of in-flight light-client optimistic-update requests.
    pub pending_optimistic_update_requests: usize,
    /// Number of in-flight beacon-blocks-by-range requests.
    pub pending_beacon_blocks_by_range_requests: usize,
    /// Number of in-flight beacon-blocks-by-range requests above the checkpoint.
    pub pending_forward_beacon_blocks_by_range_requests: usize,
    /// Number of in-flight beacon-blocks-by-root requests.
    pub pending_beacon_blocks_by_root_requests: usize,
    /// Number of failed Status request attempts since startup.
    pub status_request_failures: u64,
    /// Number of failed MetaData request attempts since startup.
    pub metadata_request_failures: u64,
    /// Number of failed light-client bootstrap request attempts since startup.
    pub bootstrap_request_failures: u64,
    /// Number of failed light-client updates-by-range request attempts since startup.
    pub updates_by_range_request_failures: u64,
    /// Number of failed light-client finality-update request attempts since startup.
    pub finality_update_request_failures: u64,
    /// Number of failed light-client optimistic-update request attempts since startup.
    pub optimistic_update_request_failures: u64,
    /// Number of failed beacon-blocks-by-range request attempts since startup.
    pub beacon_blocks_by_range_request_failures: u64,
    /// Number of failed beacon-blocks-by-root request attempts since startup.
    pub beacon_blocks_by_root_request_failures: u64,
    /// Number of active CL gossipsub topic subscriptions.
    pub gossip_subscriptions: usize,
    /// Number of light-client finality-update gossip messages observed since startup.
    pub finality_update_gossip_messages: u64,
    /// Number of light-client optimistic-update gossip messages observed since startup.
    pub optimistic_update_gossip_messages: u64,
    /// Number of malformed or undecodable light-client gossip payloads observed since startup.
    pub gossip_decode_failures: u64,
    /// Recent consensus P2P payload download rate in decoded bytes per second.
    pub p2p_download_bytes_per_sec: u64,
    /// Recent consensus P2P payload upload rate in decoded bytes per second.
    pub p2p_upload_bytes_per_sec: u64,
    /// Cumulative decoded bytes received through consensus RPC and gossip payloads.
    pub p2p_downloaded_payload_bytes: u64,
    /// Cumulative decoded bytes sent through consensus RPC payloads.
    pub p2p_uploaded_payload_bytes: u64,
    /// Most recent noteworthy consensus connection event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_connection_event: Option<String>,
    /// Most recent identify record observed from a connected consensus peer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_identify_event: Option<String>,
    /// Most recent peer-selection or backoff policy decision.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_peer_policy_event: Option<String>,
    /// Most recent consensus RPC failure or error response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rpc_failure: Option<String>,
    /// Most recent failure to send an inbound consensus RPC response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_response_send_failure: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct ExecutionNetworkStatus {
    /// Maximum execution peers configured for the current run.
    pub max_peers: usize,
    /// Execution peer sessions accepted since startup.
    pub accepted_sessions: u64,
    /// Execution peer sessions rejected because the peer did not advertise a usable tip.
    pub rejected_zero_tip_sessions: u64,
    /// Execution peer sessions closed since startup after being accepted.
    pub disconnected_sessions: u64,
    /// Accepted sessions closed with an explicit too-many-peers reason.
    pub saturated_disconnects: u64,
    /// Accepted sessions closed before serving any sync data.
    pub nonserving_disconnects: u64,
    /// Discovered candidates ignored because they did not include an ENR fork ID.
    pub missing_fork_id_candidates: u64,
    /// Discovered candidates ignored because their ENR fork ID was incompatible.
    pub fork_id_rejected_candidates: u64,
    /// Non-DNS execution discovery candidates accepted for dialing.
    pub discovered_candidates: u64,
    /// DNS execution discovery candidates accepted for dialing.
    pub dns_discovered_candidates: u64,
    /// DNS execution discovery candidates ignored because they lacked a dialable address
    /// for the selected P2P address family.
    pub dns_family_rejected_candidates: u64,
    /// Configured execution bootnodes accepted as direct RLPx dial candidates.
    pub configured_bootnode_direct_candidates: usize,
    /// Configured execution bootnodes accepted as signed discovery ENRs.
    pub configured_bootnode_discovery_enrs: usize,
    /// Configured execution bootnodes ignored because they did not match the selected
    /// P2P address family.
    pub configured_bootnode_family_rejections: usize,
    /// Execution peer dials submitted to the underlying network scheduler since startup.
    pub submitted_dials_total: u64,
    /// Submitted dials that aged out without becoming an accepted or closed session event.
    pub submitted_dial_expirations: u64,
    /// Pending execution peer candidates that have not been submitted to the dialer yet.
    pub queued_candidates: usize,
    /// Execution peers currently submitted to the dialer but not yet connected or failed.
    pub pending_dials: usize,
    /// Persisted peers that previously served valid execution data.
    pub productive_peers: usize,
    /// Total persisted execution peers retained across restarts.
    pub known_peers: usize,
    /// Peers temporarily backed off after remote saturation or too-many-peers responses.
    pub saturated_peers: usize,
    /// Peers temporarily excluded from receipt requests after receipt-specific failures.
    pub receipt_quarantined_peers: usize,
    /// Connected peers currently eligible for body requests after pause filtering.
    pub body_request_ready_peers: usize,
    /// Connected peers currently eligible for receipt requests after pause/quarantine filtering.
    pub receipt_request_ready_peers: usize,
    /// Connected peers temporarily paused for body requests.
    pub body_request_paused_peers: usize,
    /// Connected peers temporarily paused for receipt requests.
    pub receipt_request_paused_peers: usize,
    /// Connected peers currently carrying body request load from background plans.
    pub active_body_requests: usize,
    /// Connected peers currently carrying receipt request load from background plans.
    pub active_receipt_requests: usize,
    /// Connected peers with timeout penalties affecting request ranking.
    pub timeout_penalized_peers: usize,
    /// Connected peers that have successfully served at least one body request.
    pub body_proven_peers: usize,
    /// Connected peers that have successfully served at least one receipt request.
    pub receipt_proven_peers: usize,
    /// Average adaptive body request block limit across connected peers.
    pub body_request_limit_avg: usize,
    /// Average adaptive receipt request block limit across connected peers.
    pub receipt_request_limit_avg: usize,
    /// Historical fetch request attempts currently in flight.
    pub historical_fetch_active: usize,
    /// Historical body/receipt fetch plans waiting for scheduler admission.
    pub historical_fetch_ready: usize,
    /// Historical fetch outcomes buffered and waiting for ordered ingest.
    pub historical_fetch_completed: usize,
    /// Historical fetch request attempts plus buffered outcomes.
    pub historical_fetch_pending: usize,
    /// Historical fetch sequence currently required by ordered ingest.
    pub historical_fetch_expected_sequence: u64,
    /// Next historical fetch sequence that will be assigned to a new plan.
    pub historical_fetch_next_sequence: u64,
    /// Whether the required historical fetch sequence is blocking behind later work.
    pub historical_fetch_head_of_line_blocked: bool,
    /// Later historical fetch outcomes buffered while the required sequence is missing.
    pub historical_fetch_head_of_line_completed: usize,
    /// Whether the required historical fetch sequence still has an active request task.
    pub historical_fetch_expected_active: bool,
    /// Milliseconds elapsed since the required historical fetch sequence started blocking.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub historical_fetch_head_of_line_elapsed_ms: Option<u64>,
    /// Historical prepare tasks currently in flight.
    pub historical_prepare_active: usize,
    /// Historical prepare tasks whose join handles are already ready.
    pub historical_prepare_ready: usize,
    /// Historical prepare outcomes buffered and waiting for ordered ingest.
    pub historical_prepare_completed: usize,
    /// Historical prepare tasks plus buffered outcomes.
    pub historical_prepare_pending: usize,
    /// Historical prepare sequence currently required by ordered ingest.
    pub historical_prepare_expected_sequence: u64,
    /// Whether the ordered historical ingest path is currently writing a prepared batch.
    pub historical_ingest_active: bool,
    /// Historical prepare sequence currently being written, when an ingest is active.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub historical_ingest_sequence: Option<u64>,
    /// Milliseconds elapsed since the active historical ingest started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub historical_ingest_elapsed_ms: Option<u64>,
    /// Body request slots available after active and reserved historical work.
    pub historical_scheduler_body_slot_margin: usize,
    /// Receipt request slots available after active and reserved historical work.
    pub historical_scheduler_receipt_slot_margin: usize,
    /// Whether ordered write or prepare backlog is currently blocking scheduler refill.
    pub historical_scheduler_write_backpressure: bool,
    /// Current target depth for the historical fetch pipeline.
    pub historical_scheduler_pipeline_depth: usize,
    /// Current target depth for buffered historical fetch outcomes.
    pub historical_scheduler_buffer_depth: usize,
    /// Current target depth for cheap queued historical fetch plans.
    pub historical_scheduler_ready_plan_depth: usize,
    /// Number of new fetches currently admitted by critical-path refill.
    pub historical_scheduler_critical_refill_limit: usize,
    /// Number of new fetches currently admitted by write-period refill.
    pub historical_scheduler_write_refill_limit: usize,
    /// Cumulative stale body/receipt role retries scheduled by the historical scheduler.
    pub historical_scheduler_stale_role_retries: u64,
    /// Cumulative prefix-critical chunk reassignments scheduled by the historical scheduler.
    pub historical_scheduler_prefix_reassignments: u64,
    /// Cumulative successful historical body request attempts.
    pub historical_scheduler_body_successes: u64,
    /// Cumulative successful historical receipt request attempts.
    pub historical_scheduler_receipt_successes: u64,
    /// Cumulative historical body request failures.
    pub historical_scheduler_body_failures: u64,
    /// Cumulative historical receipt request failures.
    pub historical_scheduler_receipt_failures: u64,
    /// Cumulative blocks returned by successful historical body requests.
    pub historical_scheduler_body_blocks: u64,
    /// Cumulative blocks returned by successful historical receipt requests.
    pub historical_scheduler_receipt_blocks: u64,
    /// Recent successful execution P2P payload download rate in decoded bytes per second.
    pub p2p_download_bytes_per_sec: u64,
    /// Recent successful execution P2P payload upload rate in decoded bytes per second.
    pub p2p_upload_bytes_per_sec: u64,
    /// Cumulative decoded bytes returned by successful execution P2P responses.
    pub p2p_downloaded_payload_bytes: u64,
    /// Cumulative decoded bytes served to execution P2P peers.
    pub p2p_uploaded_payload_bytes: u64,
    /// Connected geth peers.
    pub connected_geth_peers: usize,
    /// Connected Nethermind peers.
    pub connected_nethermind_peers: usize,
    /// Connected Reth peers.
    pub connected_reth_peers: usize,
    /// Connected peers from other client families.
    pub connected_other_peers: usize,
    /// Connected peers reached over IPv4 execution transport.
    pub connected_ipv4_peers: usize,
    /// Connected peers reached over IPv6 execution transport.
    pub connected_ipv6_peers: usize,
    /// Serving geth peers.
    pub serving_geth_peers: usize,
    /// Serving Nethermind peers.
    pub serving_nethermind_peers: usize,
    /// Serving Reth peers.
    pub serving_reth_peers: usize,
    /// Serving peers from other client families.
    pub serving_other_peers: usize,
    /// Serving peers reached over IPv4 execution transport.
    pub serving_ipv4_peers: usize,
    /// Serving peers reached over IPv6 execution transport.
    pub serving_ipv6_peers: usize,
}

/// Live sync progress, updated by the sync task, read by HTTP endpoints.
#[derive(Debug, Clone, Default, Serialize)]
pub struct SyncStatus {
    /// High-level runtime state shown in the UI and status endpoint.
    pub node_state: NodeState,
    /// Whether the node is actively syncing blocks.
    pub syncing: bool,
    /// Number of currently connected peers.
    pub connected_peers: usize,
    /// Number of connected peers that have successfully answered validated
    /// sync requests such as headers, bodies, or receipts.
    pub serving_peers: usize,
    /// Number of pending peer candidates waiting to be dialed.
    pub pending_peers: usize,
    /// Startup P2P address selection mode, such as auto-public-ipv4 or auto-outbound-only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p2p_address_mode: Option<String>,
    /// Local IP address used by EL and CL P2P listeners.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p2p_bind_ip: Option<String>,
    /// Local P2P listener address families.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub p2p_listen_families: Vec<String>,
    /// Outbound P2P address families accepted for direct peer dials.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub p2p_dial_families: Vec<String>,
    /// Public P2P address families advertised to peers.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub p2p_advertised_families: Vec<String>,
    /// Public external IP advertised to peers, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub p2p_external_ip: Option<String>,
    /// Startup P2P reachability warnings.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub p2p_warnings: Vec<String>,
    /// The highest block number ingested so far.
    pub current_block: u64,
    /// The latest known block on the network.
    pub target_block: u64,
    /// Current sync throughput.
    pub blocks_per_sec: f64,
    /// Average sync throughput expressed in blocks per minute.
    pub blocks_per_minute: f64,
    /// Recent live log ingestion throughput from forward sync blocks.
    pub logs_per_sec: f64,
    /// Wall-clock time when live log throughput last advanced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_rate_updated_at_unix_ms: Option<u64>,
    /// Total logs ingested since the node started.
    pub logs_ingested: u64,
    /// Estimated seconds remaining to reach target.
    pub eta_seconds: Option<f64>,
    /// Whether reverse historical execution sync was disabled for this run.
    pub historical_sync_disabled: bool,
    /// Lowest execution block verified by the EL reverse backfill path.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub historical_execution_floor: Option<ExecutionBlockMarker>,
    /// Execution block where the current reverse backfill path started.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub historical_execution_anchor: Option<ExecutionBlockMarker>,
    /// Lowest block the current historical verifier is allowed to target.
    pub historical_target_block: u64,
    /// Historical reverse-sync throughput.
    pub historical_blocks_per_sec: f64,
    /// Historical reverse-sync log ingestion throughput.
    pub historical_logs_per_sec: f64,
    /// Wall-clock time when historical reverse-sync throughput last advanced.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub historical_rate_updated_at_unix_ms: Option<u64>,
    /// Estimated seconds remaining for the historical reverse verifier.
    pub historical_eta_seconds: Option<f64>,
    /// Raw sealed log segments waiting for first-time column compression.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_log_segment_backlog: Option<usize>,
    /// Already compacted segments waiting for an idle-time rewrite into the current storage profile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_profile_rewrite_backlog: Option<usize>,
    /// Weak-subjectivity checkpoint the node bootstrapped from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<WeakSubjectivityCheckpoint>,
    /// Highest indexed execution anchor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub indexed_execution_head: Option<ExecutionAnchor>,
    /// Lowest CL-authenticated execution anchor currently materialized from verified beacon blocks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialized_execution_floor: Option<ExecutionAnchor>,
    /// Highest CL-authenticated execution anchor currently materialized from verified beacon blocks.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub materialized_execution_ceiling: Option<ExecutionAnchor>,
    /// Number of CL-authenticated execution anchors currently materialized in the checkpoint-centered range.
    pub materialized_execution_anchor_count: usize,
    /// Number of detected continuity gaps in the materialized execution anchor range.
    pub materialized_execution_anchor_gap_count: usize,
    /// Highest optimistic execution anchor known from CL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub optimistic_execution_head: Option<ExecutionAnchor>,
    /// Highest finalized execution anchor known from CL.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalized_execution_head: Option<ExecutionAnchor>,
    /// Native consensus-network discovery state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consensus_network: Option<ConsensusNetworkStatus>,
    /// Native execution-network peer state.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_network: Option<ExecutionNetworkStatus>,
    /// Decoded native CL light-client payload summaries learned from peers.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub consensus_light_client: Option<ConsensusLightClientStatus>,
}
