use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
#[cfg(test)]
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy_primitives::U256;
use logex_cl::{
    AnchorCoverage, ConsensusDialAddressFamilies, ConsensusNetworkConfig, ConsensusStateError,
    ConsensusStore, MAINNET_CONSENSUS_CHAIN_SPEC, consensus_state_exists, consensus_state_path,
    prepare_consensus_network,
};
use logex_server::{AppState, SubscriptionManager};
use logex_storage::{PartitionManager, PartitionManagerConfig, SyncHead};
use logex_sync::SyncConfig;
use logex_sync::engine::SyncEngine;
use logex_sync::p2p::{
    peer_manager::{DialAddressFamilies, PeerManager, PeerManagerConfig},
    persistence::{
        discovery_secret_path, known_peers_path, load_known_peers, load_or_create_secret_key,
        persist_known_peers,
    },
};
use logex_sync::tasks::TaskMonitor;
use logex_types::{ChainAnchors, ExecutionAnchor, SyncStatus};
use reth_chainspec::{EthChainSpec, MAINNET};
use reth_discv4::NatResolver;
use reth_ethereum_forks::Head;

use crate::background::{join_task, run_background_indexer};
use crate::checkpoint::{RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG, resolve_checkpoint};

mod cleanup;
mod services;
mod storage_health;
mod supervision;
mod sync_mode;
#[cfg(test)]
mod sync_mode_tests;

use sync_mode::{
    SyncModeState, read_sync_mode_state, remove_sync_mode_state, write_sync_mode_state,
};

pub use cleanup::finish_runtime_shutdown;

const ENGINE_CLEANUP_TIMEOUT: Duration = Duration::from_secs(60);
const SYNC_ENGINE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(120);
// Runtime failures allow the engine's 120-second grace plus 60 seconds
// for shared cleanup. An independent thread enforces this even if startup,
// filesystem calls or post-abort joins block application runtime workers.
const RUNTIME_FAILURE_CLEANUP_GRACE: Duration = Duration::from_secs(180);
const MAINNET_SECONDS_PER_SLOT: u64 = 12;
const MAINNET_SLOTS_PER_EPOCH: u64 = 32;
const IPV4_REACHABILITY_PROBE: (Ipv4Addr, u16) = (Ipv4Addr::new(1, 1, 1, 1), 80);
const IPV6_REACHABILITY_PROBE: (Ipv6Addr, u16) = (
    Ipv6Addr::new(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111),
    80,
);
const P2P_REACHABILITY_PROBE_TIMEOUT: Duration = Duration::from_millis(900);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HistoricalSyncMode {
    Enabled,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum P2pAddressSelectionMode {
    Explicit,
    AutoPublicIpv4,
    AutoPublicIpv6,
    AutoOutboundOnly,
}

impl P2pAddressSelectionMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Explicit => "explicit",
            Self::AutoPublicIpv4 => "auto-public-ipv4",
            Self::AutoPublicIpv6 => "auto-public-ipv6",
            Self::AutoOutboundOnly => "auto-outbound-only",
        }
    }
}

#[derive(Debug, Clone)]
struct P2pAddressSelection {
    nat: NatResolver,
    bind_ip: IpAddr,
    dial_families: DialAddressFamilies,
    advertised_families: DialAddressFamilies,
    external_ip: Option<IpAddr>,
    mode: P2pAddressSelectionMode,
    warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct ConsensusP2pAddressSelection {
    bind_ip: IpAddr,
    dial_families: ConsensusDialAddressFamilies,
    external_ip: Option<IpAddr>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Copy, Default)]
struct LocalP2pAddressCandidates {
    ipv4: Option<Ipv4Addr>,
    ipv6: Option<Ipv6Addr>,
}

impl LocalP2pAddressCandidates {
    fn public_ipv4(self) -> Option<Ipv4Addr> {
        self.ipv4.filter(|ip| is_public_ipv4(*ip))
    }

    fn public_ipv6(self) -> Option<Ipv6Addr> {
        self.ipv6.filter(|ip| is_public_ipv6(*ip))
    }

    fn route_dial_families(self) -> DialAddressFamilies {
        match (self.ipv4, self.ipv6) {
            (Some(_), Some(_)) => DialAddressFamilies::BOTH,
            (Some(_), None) => DialAddressFamilies::IPV4,
            (None, Some(_)) => DialAddressFamilies::IPV6,
            (None, None) => DialAddressFamilies::IPV4,
        }
    }

    fn outbound_only_bind_ip(self) -> IpAddr {
        if self.ipv4.is_some() {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        } else if self.ipv6.is_some() {
            IpAddr::V6(Ipv6Addr::UNSPECIFIED)
        } else {
            IpAddr::V4(Ipv4Addr::UNSPECIFIED)
        }
    }
}

pub struct RunSyncOptions {
    pub pm_config: PartitionManagerConfig,
    pub volume_monitor: Option<crate::volume::MonitorHandle>,
    pub checkpoint: Option<String>,
    pub checkpoint_sync_url: Option<String>,
    pub http_host: IpAddr,
    pub http_port: u16,
    pub grpc_host: IpAddr,
    pub grpc_port: u16,
    pub discovery_port: u16,
    pub p2p_port: u16,
    pub max_peers: usize,
    pub nat: String,
    pub p2p_bind_ip: Option<IpAddr>,
    pub execution_bootnodes: Vec<String>,
    pub execution_discv5_port: u16,
    pub cl_discovery_port: u16,
    pub cl_p2p_port: u16,
    pub cl_max_peers: usize,
    pub dashboard_enabled: bool,
    pub dashboard_password: Option<String>,
    pub disable_historical_sync: bool,
}

pub async fn run_sync(options: RunSyncOptions) -> cleanup::RuntimeShutdown {
    let RunSyncOptions {
        pm_config,
        volume_monitor,
        checkpoint,
        checkpoint_sync_url,
        http_host,
        http_port,
        grpc_host,
        grpc_port,
        discovery_port,
        p2p_port,
        max_peers,
        nat,
        p2p_bind_ip,
        execution_bootnodes,
        execution_discv5_port,
        cl_discovery_port,
        cl_p2p_port,
        cl_max_peers,
        dashboard_enabled,
        dashboard_password,
        disable_historical_sync,
    } = options;
    let local_p2p_candidates = detect_local_p2p_addresses().await;
    let mut p2p_address =
        match select_p2p_address(&nat, p2p_bind_ip, p2p_port, local_p2p_candidates).await {
            Ok(selection) => selection,
            Err(error) => {
                tracing::error!(%error, "invalid EL NAT resolver");
                std::process::exit(1);
            }
        };
    add_runtime_p2p_warnings(&mut p2p_address, &execution_bootnodes);
    let consensus_p2p_address = select_consensus_p2p_address(&p2p_address, local_p2p_candidates);
    let nat = p2p_address.nat.clone();
    let p2p_external_ip = p2p_address.external_ip;
    let p2p_bind_ip = p2p_address.bind_ip;
    let p2p_dial_families = p2p_address.dial_families;
    let p2p_external_ip_label = p2p_external_ip
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unresolved".to_owned());
    tracing::info!(
        bind_ip = %p2p_bind_ip,
        ?p2p_dial_families,
        external_ip = %p2p_external_ip_label,
        nat = %nat,
        mode = p2p_address.mode.as_str(),
        "resolved p2p address selection"
    );
    for warning in &p2p_address.warnings {
        tracing::warn!(warning, "p2p address selection warning");
    }
    let consensus_external_ip_label = consensus_p2p_address
        .external_ip
        .map(|ip| ip.to_string())
        .unwrap_or_else(|| "unresolved".to_owned());
    tracing::info!(
        bind_ip = %consensus_p2p_address.bind_ip,
        ?consensus_p2p_address.dial_families,
        external_ip = %consensus_external_ip_label,
        "resolved consensus p2p address selection"
    );
    for warning in &consensus_p2p_address.warnings {
        tracing::warn!(warning, "consensus p2p address selection warning");
    }

    let data_dir = pm_config.data_dir.clone();
    let discovery_secret_file = discovery_secret_path(&data_dir);
    let known_peers_file = known_peers_path(&data_dir);
    let consensus_state_exists = match consensus_state_exists(&data_dir) {
        Ok(exists) => exists,
        Err(error) => {
            tracing::error!(%error, "failed to inspect consensus state");
            std::process::exit(1);
        }
    };
    let checkpoint_request = checkpoint;
    let mut checkpoint = if consensus_state_exists && checkpoint_request.is_none() {
        None
    } else {
        resolve_checkpoint_or_exit(checkpoint_request, checkpoint_sync_url.as_deref()).await
    };

    let mut storage = match PartitionManager::open(pm_config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    let sync_head = storage.sync_head();
    let historical_sync_mode = match resolve_historical_sync_mode(
        &data_dir,
        &storage,
        consensus_state_exists,
        disable_historical_sync,
    ) {
        Ok(mode) => mode,
        Err(error) => {
            tracing::error!(%error);
            std::process::exit(1);
        }
    };
    let historical_sync_disabled = historical_sync_mode == HistoricalSyncMode::Disabled;
    let head_block = storage.head_block().unwrap_or(0);
    let indexed_head_block = storage.indexed_head_block();
    let resume_block = sync_head
        .map(|head| head.block_number)
        .or(indexed_head_block)
        .unwrap_or(0);
    tracing::info!(
        total_rows = storage.total_rows(),
        head_block,
        indexed_head_block,
        sync_head_block = sync_head.map(|head| head.block_number),
        "storage ready"
    );

    let mut consensus_refreshed = false;
    let mut consensus = match maybe_open_consensus_store(&data_dir, &storage, checkpoint.as_deref())
    {
        Ok(store) => store.map(Arc::new),
        Err(ConsensusStateError::MissingCheckpoint) => {
            tracing::error!(
                data_dir = %data_dir.display(),
                "fresh data directories now require --checkpoint <root-or-descriptor> to start canonical sync"
            );
            std::process::exit(1);
        }
        Err(ConsensusStateError::StaleWeakSubjectivityCheckpoint { .. }) => {
            tracing::warn!(
                data_dir = %data_dir.display(),
                "persisted consensus state is outside the weak-subjectivity window; refreshing from a recent checkpoint"
            );
            match refresh_consensus_state_from_recent_checkpoint(
                &data_dir,
                &mut checkpoint,
                checkpoint_sync_url.as_deref(),
                "weak-subjectivity-stale",
            )
            .await
            {
                Ok(store) => {
                    consensus_refreshed = true;
                    Some(Arc::new(store))
                }
                Err(error) => {
                    tracing::error!(%error, "failed to refresh stale consensus state");
                    std::process::exit(1);
                }
            }
        }
        Err(error) => {
            tracing::error!(%error, "failed to initialize consensus state");
            std::process::exit(1);
        }
    };

    if let Some(store) = consensus.as_ref()
        && let Some(staleness) = recent_consensus_state_staleness(store)
    {
        tracing::warn!(
            trusted_slot = staleness.trusted_slot,
            trusted_epoch = staleness.trusted_epoch,
            current_epoch = staleness.current_epoch,
            max_epochs = staleness.max_epochs,
            "persisted consensus state is older than the recent checkpoint window; refreshing from checkpoint-sync source"
        );
        match refresh_consensus_state_from_recent_checkpoint(
            &data_dir,
            &mut checkpoint,
            checkpoint_sync_url.as_deref(),
            "recent-checkpoint-stale",
        )
        .await
        {
            Ok(store) => {
                consensus = Some(Arc::new(store));
                consensus_refreshed = true;
            }
            Err(error) => {
                tracing::error!(%error, "failed to refresh stale consensus state");
                std::process::exit(1);
            }
        }
    }

    if let Some(store) = consensus.as_ref()
        && let Some(staleness) = local_execution_progress_staleness(sync_head, store)
        && !consensus_refreshed
    {
        tracing::warn!(
            block_number = staleness.block_number,
            timestamp = staleness.timestamp,
            age_secs = staleness.age_secs,
            max_age_secs = staleness.max_age_secs,
            "local execution progress is older than the recent checkpoint window; refreshing consensus checkpoint before resuming"
        );
        match refresh_consensus_state_from_recent_checkpoint(
            &data_dir,
            &mut checkpoint,
            checkpoint_sync_url.as_deref(),
            "execution-progress-stale",
        )
        .await
        {
            Ok(store) => {
                consensus = Some(Arc::new(store));
                consensus_refreshed = true;
            }
            Err(error) => {
                tracing::error!(
                    %error,
                    "failed to refresh checkpoint for stale local execution progress"
                );
                std::process::exit(1);
            }
        }
    }

    if let Some(consensus) = consensus.as_ref() {
        if let Some(staleness) = local_execution_progress_staleness(sync_head, consensus) {
            tracing::warn!(
                block_number = staleness.block_number,
                timestamp = staleness.timestamp,
                age_secs = staleness.age_secs,
                max_age_secs = staleness.max_age_secs,
                checkpoint_refreshed = consensus_refreshed,
                "local execution progress is stale but startup has a recent checkpoint; CL anchors will bridge the gap before live EL sync advances"
            );
        }
        let checkpoint = consensus.checkpoint();
        let mut anchors = consensus.chain_anchors();
        anchors.indexed_head = storage.chain_anchors().indexed_head;
        if let Err(error) = storage.record_chain_anchors(anchors.clone()) {
            tracing::error!(error = %error, "failed to persist consensus anchors into storage");
            std::process::exit(1);
        }
        tracing::info!(
            checkpoint_root = %checkpoint.beacon_root,
            checkpoint_slot = checkpoint.beacon_slot,
            optimistic_head = anchors.optimistic_head.map(|anchor| anchor.block_number),
            finalized_head = anchors.finalized_head.map(|anchor| anchor.block_number),
            "consensus state ready"
        );
    } else {
        tracing::warn!(
            cl_discovery_port,
            cl_p2p_port,
            cl_max_peers,
            "starting without persisted consensus state; EL-only sync mode remains active until checkpointed CL state is configured"
        );
    }

    let known_peers = match load_known_peers(&known_peers_file) {
        Ok(peers) => peers,
        Err(e) => {
            tracing::error!(
                error = %e,
                path = %known_peers_file.display(),
                "failed to read or preserve known peers; check storage access before restarting"
            );
            std::process::exit(1);
        }
    };
    tracing::info!(
        peers = known_peers.len(),
        path = %known_peers_file.display(),
        "loaded known peers"
    );
    let p2p_warning_count_before_known_peers = p2p_address.warnings.len();
    add_known_peer_fallback_warnings(&mut p2p_address, known_peers.len());
    if p2p_address.mode == P2pAddressSelectionMode::AutoOutboundOnly && !known_peers.is_empty() {
        tracing::info!(
            peers = known_peers.len(),
            "outbound-only execution p2p will seed from persisted known peers"
        );
    }
    for warning in p2p_address
        .warnings
        .iter()
        .skip(p2p_warning_count_before_known_peers)
    {
        tracing::warn!(warning, "p2p known-peer fallback warning");
    }

    let storage_anchors = storage.chain_anchors();
    let historical_floor = storage.historical_floor();
    let historical_anchor = storage.historical_anchor();
    let mut sync_status = initial_sync_status(
        resume_block,
        &storage_anchors,
        historical_floor,
        historical_anchor,
        historical_sync_disabled,
        consensus.as_deref(),
    );
    apply_p2p_address_status(&mut sync_status, &p2p_address);
    let state = Arc::new(AppState::new(
        storage,
        Some(SubscriptionManager::new()),
        sync_status,
    ));
    let volume_failure = volume_monitor.map(|monitor| {
        let (failure, receiver) = tokio::sync::watch::channel(None);
        let state = Arc::downgrade(&state);
        monitor
            .set_failure_handler(move |reason| {
                // The monitor arms its independent process deadline first. Notify
                // the supervisor and close admission even while engine I/O blocks.
                failure.send_replace(Some(reason.to_owned()));
                if let Some(state) = state.upgrade() {
                    state.mark_storage_unavailable(reason);
                }
            })
            .unwrap_or_else(|_| std::process::exit(1));
        receiver
    });

    let secret_key = match load_or_create_secret_key(&discovery_secret_file) {
        Ok(secret) => secret,
        Err(e) => {
            tracing::error!(
                error = %e,
                path = %discovery_secret_file.display(),
                "failed to load discovery secret"
            );
            std::process::exit(1);
        }
    };
    tracing::info!(
        path = %discovery_secret_file.display(),
        "loaded discovery secret"
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut consensus_storage_failure = consensus
        .as_ref()
        .map(|store| store.subscribe_storage_failure());
    let _consensus_storage_watchdog = consensus_storage_failure.as_ref().map(|receiver| {
        start_runtime_failure_watchdog(receiver.clone(), RUNTIME_FAILURE_CLEANUP_GRACE, || {
            std::process::exit(1)
        })
        .unwrap_or_else(|error| {
            tracing::error!(%error, "failed to start consensus storage shutdown watchdog");
            std::process::exit(1);
        })
    });

    let node_workers = TaskMonitor::default();
    // Start before spawning services so an early bind failure is retained and
    // bounded even if execution-network initialization is still in progress.
    let _node_worker_watchdog = start_runtime_failure_watchdog(
        node_workers.subscribe(),
        RUNTIME_FAILURE_CLEANUP_GRACE,
        || std::process::exit(1),
    )
    .unwrap_or_else(|error| {
        tracing::error!(%error, "failed to start node worker shutdown watchdog");
        std::process::exit(1);
    });

    let consensus_network_handle = consensus.as_ref().map(|consensus| {
        prepare_consensus_network(
            ConsensusNetworkConfig {
                data_dir: data_dir.clone(),
                checkpoint: consensus.checkpoint(),
                bind_ip: consensus_p2p_address.bind_ip,
                dial_families: consensus_p2p_address.dial_families,
                external_ip: consensus_p2p_address.external_ip,
                discovery_port: cl_discovery_port,
                p2p_port: cl_p2p_port,
                max_peers: cl_max_peers,
            },
            Arc::clone(consensus),
            Arc::clone(&state.sync_status),
            shutdown_rx.clone(),
        )
    });
    let consensus_network_handle = match consensus_network_handle {
        Some(Ok(network)) => {
            Some(node_workers.spawn_result("consensus network supervisor", network))
        }
        Some(Err(error)) => {
            tracing::error!(%error, "failed to start consensus network");
            std::process::exit(1);
        }
        None => None,
    };

    let http_addr = SocketAddr::new(http_host, http_port);
    let http_handle = services::spawn_http(
        &node_workers,
        Arc::clone(&state),
        http_addr,
        shutdown_rx.clone(),
        logex_server::HttpServerConfig {
            dashboard_enabled,
            dashboard_password,
        },
    );
    let grpc_addr = SocketAddr::new(grpc_host, grpc_port);
    let grpc_handle = services::spawn_grpc(
        &node_workers,
        Arc::clone(&state),
        grpc_addr,
        shutdown_rx.clone(),
    );

    let index_state = Arc::clone(&state);
    let index_shutdown = shutdown_rx.clone();
    let index_handle = node_workers.spawn_result(
        "background indexer",
        run_background_indexer(index_state, index_shutdown),
    );

    tracing::info!(
        http = %format!("http://{http_addr}"),
        grpc = %format!("http://{grpc_addr}"),
        "query endpoints starting"
    );

    let our_head = startup_network_head(sync_head, consensus.as_deref());
    let peers = match PeerManager::new(PeerManagerConfig {
        secret_key,
        listener_port: p2p_port,
        discovery_port,
        bind_ip: p2p_bind_ip,
        dial_families: p2p_dial_families,
        max_peers,
        nat_resolver: nat,
        our_head,
        known_peers,
        known_peers_path: known_peers_file.clone(),
        execution_bootnodes,
        execution_discv5_port,
    })
    .await
    {
        Ok(peers) => peers,
        Err(e) => {
            tracing::error!(error = %e, "failed to start p2p networking");
            std::process::exit(1);
        }
    };

    let mut execution_network_failure = Some(peers.task_failure_receiver());
    let _execution_network_watchdog = start_runtime_failure_watchdog(
        peers.task_failure_receiver(),
        RUNTIME_FAILURE_CLEANUP_GRACE,
        || std::process::exit(1),
    )
    .unwrap_or_else(|error| {
        tracing::error!(%error, "failed to start execution network shutdown watchdog");
        std::process::exit(1);
    });

    let sync_config = SyncConfig {
        max_peers,
        disable_historical_sync: historical_sync_disabled,
        ..Default::default()
    };

    let mut engine = SyncEngine::new(
        sync_config,
        peers,
        Arc::clone(&state.storage),
        state.subscriptions.clone(),
        Arc::clone(&state.sync_status),
        consensus,
        shutdown_rx.clone(),
    );

    let mut shutdown_guard = None;
    let mut engine_exit_code = supervision::SyncSupervisor {
        on_shutdown: || {
            shutdown_guard = Some(
                cleanup::start_shutdown_watchdog(RUNTIME_FAILURE_CLEANUP_GRACE, || {
                    std::process::exit(1)
                })
                .unwrap_or_else(|_| {
                    // Without an independent deadline, logging or cleanup
                    // could block indefinitely. Fail before acquiring locks.
                    std::process::exit(1);
                }),
            );
        },
        shutdown_tx: &shutdown_tx,
        node_workers: &node_workers,
        sync_status: &state.sync_status,
        consensus_storage_failure: &mut consensus_storage_failure,
        execution_network_failure: &mut execution_network_failure,
        shutdown_timeout: SYNC_ENGINE_SHUTDOWN_TIMEOUT,
    }
    .run(
        engine.run(),
        wait_for_shutdown_signal(),
        storage_health::wait_for_failure(data_dir.clone(), volume_failure),
        |reason| state.mark_storage_unavailable(reason),
    )
    .await;

    match tokio::time::timeout(ENGINE_CLEANUP_TIMEOUT, engine.shutdown()).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => cleanup::record_failure(&mut engine_exit_code, &state.sync_status, error),
        Err(_) => cleanup::record_failure(
            &mut engine_exit_code,
            &state.sync_status,
            format!("sync engine cleanup exceeded {ENGINE_CLEANUP_TIMEOUT:?}"),
        ),
    }

    let known_peers = engine.known_peers();
    if let Err(e) = persist_known_peers(&known_peers_file, &known_peers) {
        tracing::warn!(
            error = %e,
            path = %known_peers_file.display(),
            "failed to persist known peers"
        );
    } else {
        tracing::info!(
            peers = known_peers.len(),
            path = %known_peers_file.display(),
            "persisted known peers"
        );
    }

    let _ = shutdown_tx.send(true);
    tracing::info!("waiting for HTTP, gRPC, and indexing tasks to stop");
    let mut tasks = vec![
        ("HTTP server", http_handle),
        ("gRPC server", grpc_handle),
        ("background indexer", index_handle),
    ];
    if let Some(handle) = consensus_network_handle {
        tasks.push(("consensus network", handle));
    }
    // These workers are already stopping independently. Await them together so
    // their cleanup windows do not multiply with the number of services.
    for result in futures_util::future::join_all(
        tasks
            .into_iter()
            .map(|(name, handle)| join_task(name, handle)),
    )
    .await
    {
        if let Err(error) = result {
            cleanup::record_failure(&mut engine_exit_code, &state.sync_status, error);
        }
    }
    tracing::info!("shutting down");
    // Inspect the permanent latch again: a failure may also arrive while a
    // different shutdown branch or the shared cleanup is already running.
    if let Some(error) = consensus_storage_failure
        .as_ref()
        .and_then(|receiver| receiver.borrow().clone())
    {
        tracing::error!(%error, "node stopped after fatal consensus storage failure");
        std::process::exit(1);
    }
    if let Some(error) = execution_network_failure
        .as_ref()
        .and_then(|receiver| receiver.borrow().clone())
    {
        tracing::error!(%error, "node stopped after fatal execution network failure");
        std::process::exit(1);
    }
    if let Some(error) = node_workers.subscribe().borrow().clone() {
        tracing::error!(%error, "node stopped after fatal worker failure");
        std::process::exit(1);
    }
    if engine_exit_code != std::process::ExitCode::SUCCESS {
        // Keep the watchdog alive until process exit. Dropping Tokio's runtime
        // instead could wait indefinitely for unfinished blocking work.
        std::process::exit(1);
    }
    shutdown_guard.expect("the supervisor arms shutdown before completing")
}

async fn wait_for_runtime_failure(
    receiver: &mut Option<tokio::sync::watch::Receiver<Option<Arc<str>>>>,
) -> Arc<str> {
    let Some(receiver) = receiver else {
        return std::future::pending().await;
    };
    loop {
        if let Some(error) = receiver.borrow_and_update().clone() {
            return error;
        }
        if receiver.changed().await.is_err() {
            // A closed channel without a failure is not a runtime failure.
            return std::future::pending().await;
        }
    }
}

struct RuntimeFailureWatchdog {
    waiting_complete: Option<tokio::sync::oneshot::Sender<()>>,
    deadline_complete: std::sync::mpsc::Sender<()>,
}

impl Drop for RuntimeFailureWatchdog {
    fn drop(&mut self) {
        if let Some(complete) = self.waiting_complete.take() {
            let _ = complete.send(());
        }
        let _ = self.deadline_complete.send(());
    }
}

fn start_runtime_failure_watchdog(
    receiver: tokio::sync::watch::Receiver<Option<Arc<str>>>,
    grace: Duration,
    on_expiry: impl FnOnce() + Send + 'static,
) -> std::io::Result<RuntimeFailureWatchdog> {
    // Tokio watch has no blocking receiver API. This runtime belongs solely to
    // the watchdog thread and only awaits notifications; it starts no workers.
    let runtime = tokio::runtime::Builder::new_current_thread().build()?;
    let (waiting_complete, mut waiting_done) = tokio::sync::oneshot::channel();
    let (deadline_complete, deadline_done) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("runtime-failure-watchdog".to_owned())
        .spawn(move || {
            let mut receiver = Some(receiver);
            let failed = runtime.block_on(async {
                tokio::select! {
                    biased;
                    _ = &mut waiting_done => false,
                    _ = wait_for_runtime_failure(&mut receiver) => true,
                }
            });
            if failed {
                finish_runtime_failure_watchdog(deadline_done, grace, on_expiry);
            }
        })?;
    // Dropping the guard wakes either notification wait without joining the
    // thread. Normal shutdown never leaves a thread waiting for a future fault.
    Ok(RuntimeFailureWatchdog {
        waiting_complete: Some(waiting_complete),
        deadline_complete,
    })
}

fn finish_runtime_failure_watchdog(
    completed: std::sync::mpsc::Receiver<()>,
    grace: Duration,
    on_expiry: impl FnOnce(),
) {
    if matches!(
        completed.recv_timeout(grace),
        Err(std::sync::mpsc::RecvTimeoutError::Timeout)
    ) {
        // Do not acquire application locks or log before terminating: those
        // facilities may be the reason orderly cleanup has not completed.
        on_expiry();
    }
}

fn mark_sync_stopped_for_runtime_failure(
    sync_status: &std::sync::Mutex<SyncStatus>,
    consensus_unavailable: bool,
) {
    // Reporting a failure must not unwind and disarm its cleanup watchdog if a
    // previous telemetry update was interrupted. Retain the poison indication.
    let mut status = sync_status
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    status.syncing = false;
    status.eta_seconds = None;
    status.historical_eta_seconds = None;
    if consensus_unavailable {
        status.consensus_head_fresh = Some(false);
    }
    status.node_state = logex_types::NodeState::Disconnected;
}

async fn select_p2p_address(
    nat: &str,
    p2p_bind_ip: Option<IpAddr>,
    p2p_port: u16,
    candidates: LocalP2pAddressCandidates,
) -> Result<P2pAddressSelection, String> {
    let parsed = nat
        .parse::<NatResolver>()
        .map_err(|error| error.to_string())?;
    if parsed == NatResolver::Any {
        return Ok(choose_auto_p2p_address(candidates, p2p_bind_ip));
    }

    let resolved = resolve_startup_nat(parsed).await;
    let mut warnings = Vec::new();
    Ok(choose_explicit_p2p_address(
        resolved,
        p2p_bind_ip,
        p2p_port,
        candidates,
        &mut warnings,
    ))
}

async fn resolve_startup_nat(nat_resolver: NatResolver) -> NatResolver {
    if matches!(
        nat_resolver,
        NatResolver::ExternalIp(_) | NatResolver::ExternalAddr(_) | NatResolver::None
    ) {
        return nat_resolver;
    }

    match nat_resolver.clone().external_addr().await {
        Some(ip) => {
            tracing::info!(
                nat = %nat_resolver,
                external_ip = %ip,
                "resolved EL external IP before starting p2p"
            );
            NatResolver::ExternalIp(ip)
        }
        None => nat_resolver,
    }
}

fn default_p2p_bind_ip(nat_resolver: &NatResolver, p2p_port: u16) -> IpAddr {
    match nat_resolver.clone().as_external_ip(p2p_port) {
        Some(IpAddr::V6(_)) => IpAddr::V6(Ipv6Addr::UNSPECIFIED),
        _ => IpAddr::V4(Ipv4Addr::UNSPECIFIED),
    }
}

fn choose_explicit_p2p_address(
    nat: NatResolver,
    p2p_bind_ip: Option<IpAddr>,
    p2p_port: u16,
    candidates: LocalP2pAddressCandidates,
    warnings: &mut Vec<String>,
) -> P2pAddressSelection {
    let external_ip = nat.clone().as_external_ip(p2p_port);
    let bind_ip = narrow_unspecified_ipv6_bind(
        p2p_bind_ip.unwrap_or_else(|| default_p2p_bind_ip(&nat, p2p_port)),
        external_ip,
        candidates.ipv6,
        warnings,
    );
    let advertised_families = external_ip
        .map(DialAddressFamilies::for_bind_ip)
        .unwrap_or(DialAddressFamilies::for_bind_ip(bind_ip));
    let dial_families = p2p_bind_ip
        .map(DialAddressFamilies::for_bind_ip)
        .unwrap_or_else(|| {
            combine_dial_families(advertised_families, candidates.route_dial_families())
        });

    P2pAddressSelection {
        nat,
        bind_ip,
        dial_families,
        advertised_families,
        external_ip,
        mode: P2pAddressSelectionMode::Explicit,
        warnings: warnings.clone(),
    }
}

fn combine_dial_families(
    first: DialAddressFamilies,
    second: DialAddressFamilies,
) -> DialAddressFamilies {
    match (
        first.allows_ipv4() || second.allows_ipv4(),
        first.allows_ipv6() || second.allows_ipv6(),
    ) {
        (true, true) => DialAddressFamilies::BOTH,
        (false, true) => DialAddressFamilies::IPV6,
        _ => DialAddressFamilies::IPV4,
    }
}

fn choose_auto_p2p_address(
    candidates: LocalP2pAddressCandidates,
    p2p_bind_ip: Option<IpAddr>,
) -> P2pAddressSelection {
    let mut warnings = Vec::new();
    let public_ipv4 = candidates.public_ipv4();
    let public_ipv6 = candidates.public_ipv6();
    if p2p_bind_ip.is_none() && public_ipv4.is_some() && public_ipv6.is_some() {
        warnings.push(
            "public IPv4 and IPv6 were both detected; execution advertises IPv4 and dials both routed families. True simultaneous execution IPv4+IPv6 inbound requires a future composite network backend."
                .to_owned(),
        );
    } else if p2p_bind_ip.is_none() && public_ipv6.is_some() && candidates.ipv4.is_some() {
        warnings.push(
            "public IPv6 and outbound IPv4 were detected; execution advertises IPv6 and dials both routed families where possible"
                .to_owned(),
        );
    }

    let selection = match p2p_bind_ip {
        Some(bind_ip @ IpAddr::V4(_)) => public_ipv4.map(|ip| {
            (
                NatResolver::ExternalIp(IpAddr::V4(ip)),
                bind_ip,
                DialAddressFamilies::IPV4,
                Some(IpAddr::V4(ip)),
                P2pAddressSelectionMode::AutoPublicIpv4,
            )
        }),
        Some(bind_ip @ IpAddr::V6(_)) => public_ipv6.map(|ip| {
            let bind_ip = narrow_unspecified_ipv6_bind(
                bind_ip,
                Some(IpAddr::V6(ip)),
                candidates.ipv6,
                &mut warnings,
            );
            (
                NatResolver::ExternalIp(IpAddr::V6(ip)),
                bind_ip,
                DialAddressFamilies::IPV6,
                Some(IpAddr::V6(ip)),
                P2pAddressSelectionMode::AutoPublicIpv6,
            )
        }),
        None => public_ipv4
            .map(|ip| {
                (
                    NatResolver::ExternalIp(IpAddr::V4(ip)),
                    IpAddr::V4(Ipv4Addr::UNSPECIFIED),
                    if public_ipv6.is_some() || candidates.ipv6.is_some() {
                        DialAddressFamilies::BOTH
                    } else {
                        DialAddressFamilies::IPV4
                    },
                    Some(IpAddr::V4(ip)),
                    P2pAddressSelectionMode::AutoPublicIpv4,
                )
            })
            .or_else(|| {
                public_ipv6.map(|ip| {
                    let bind_ip = narrow_unspecified_ipv6_bind(
                        IpAddr::V6(Ipv6Addr::UNSPECIFIED),
                        Some(IpAddr::V6(ip)),
                        candidates.ipv6,
                        &mut warnings,
                    );
                    (
                        NatResolver::ExternalIp(IpAddr::V6(ip)),
                        bind_ip,
                        if candidates.ipv4.is_some() {
                            DialAddressFamilies::BOTH
                        } else {
                            DialAddressFamilies::IPV6
                        },
                        Some(IpAddr::V6(ip)),
                        P2pAddressSelectionMode::AutoPublicIpv6,
                    )
                })
            }),
    };

    let (nat, bind_ip, dial_families, external_ip, mode) = selection.unwrap_or_else(|| {
        warnings.push(
            "no locally owned public IPv4 or IPv6 address with outbound reachability was detected; using outbound-only P2P without advertising a public address"
                .to_owned(),
        );
        let bind_ip = p2p_bind_ip.unwrap_or_else(|| candidates.outbound_only_bind_ip());
        (
            NatResolver::None,
            bind_ip,
            p2p_bind_ip
                .map(DialAddressFamilies::for_bind_ip)
                .unwrap_or_else(|| candidates.route_dial_families()),
            None,
            P2pAddressSelectionMode::AutoOutboundOnly,
        )
    });

    P2pAddressSelection {
        nat,
        bind_ip,
        dial_families,
        advertised_families: external_ip
            .map(DialAddressFamilies::for_bind_ip)
            .unwrap_or_else(|| DialAddressFamilies::for_bind_ip(bind_ip)),
        external_ip,
        mode,
        warnings,
    }
}

async fn detect_local_p2p_addresses() -> LocalP2pAddressCandidates {
    let (ipv4, ipv6) = tokio::join!(default_route_ipv4(), default_route_ipv6());
    LocalP2pAddressCandidates { ipv4, ipv6 }
}

async fn default_route_ipv4() -> Option<Ipv4Addr> {
    let stream = connect_reachability_probe(SocketAddr::new(
        IpAddr::V4(IPV4_REACHABILITY_PROBE.0),
        IPV4_REACHABILITY_PROBE.1,
    ))
    .await?;
    match stream.local_addr().ok()?.ip() {
        IpAddr::V4(ip) => Some(ip),
        IpAddr::V6(_) => None,
    }
}

async fn default_route_ipv6() -> Option<Ipv6Addr> {
    let stream = connect_reachability_probe(SocketAddr::new(
        IpAddr::V6(IPV6_REACHABILITY_PROBE.0),
        IPV6_REACHABILITY_PROBE.1,
    ))
    .await?;
    match stream.local_addr().ok()?.ip() {
        IpAddr::V4(_) => None,
        IpAddr::V6(ip) => Some(ip),
    }
}

async fn connect_reachability_probe(addr: SocketAddr) -> Option<tokio::net::TcpStream> {
    tokio::time::timeout(
        P2P_REACHABILITY_PROBE_TIMEOUT,
        tokio::net::TcpStream::connect(addr),
    )
    .await
    .ok()?
    .ok()
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    if ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_multicast()
    {
        return false;
    }

    match octets {
        [0, _, _, _] => false,
        [100, second, _, _] if (64..=127).contains(&second) => false,
        [192, 0, 0, _] => false,
        [192, 0, 2, _] => false,
        [198, second, _, _] if second == 18 || second == 19 => false,
        [198, 51, 100, _] => false,
        [203, 0, 113, _] => false,
        [first, _, _, _] if first >= 240 => false,
        _ => true,
    }
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    if ip.is_unspecified() || ip.is_loopback() || ip.is_multicast() {
        return false;
    }

    if !ipv6_matches_prefix(ip, Ipv6Addr::new(0x2000, 0, 0, 0, 0, 0, 0, 0), 3) {
        return false;
    }

    for (prefix, bits) in [
        (Ipv6Addr::new(0x2001, 0, 0, 0, 0, 0, 0, 0), 32), // Teredo.
        (Ipv6Addr::new(0x2001, 0x0002, 0, 0, 0, 0, 0, 0), 48), // Benchmarking.
        (Ipv6Addr::new(0x2001, 0x0010, 0, 0, 0, 0, 0, 0), 28), // ORCHIDv1.
        (Ipv6Addr::new(0x2001, 0x0020, 0, 0, 0, 0, 0, 0), 28), // ORCHIDv2.
        (Ipv6Addr::new(0x2001, 0x0db8, 0, 0, 0, 0, 0, 0), 32), // Documentation.
        (Ipv6Addr::new(0x2002, 0, 0, 0, 0, 0, 0, 0), 16), // 6to4.
        (Ipv6Addr::new(0x3fff, 0, 0, 0, 0, 0, 0, 0), 20), // Documentation.
    ] {
        if ipv6_matches_prefix(ip, prefix, bits) {
            return false;
        }
    }

    true
}

fn ipv6_matches_prefix(ip: Ipv6Addr, prefix: Ipv6Addr, prefix_len: u32) -> bool {
    debug_assert!(prefix_len <= 128);
    let mask = if prefix_len == 0 {
        0
    } else {
        u128::MAX << (128 - prefix_len)
    };
    let ip_bits = u128::from_be_bytes(ip.octets());
    let prefix_bits = u128::from_be_bytes(prefix.octets());
    (ip_bits & mask) == (prefix_bits & mask)
}

fn narrow_unspecified_ipv6_bind(
    bind_ip: IpAddr,
    external_ip: Option<IpAddr>,
    local_ipv6: Option<Ipv6Addr>,
    warnings: &mut Vec<String>,
) -> IpAddr {
    let IpAddr::V6(bind_ipv6) = bind_ip else {
        return bind_ip;
    };
    if !bind_ipv6.is_unspecified() {
        return bind_ip;
    }

    let Some(IpAddr::V6(external_ipv6)) = external_ip else {
        return bind_ip;
    };
    if local_ipv6 == Some(external_ipv6) {
        return IpAddr::V6(external_ipv6);
    }

    warnings.push(
        "IPv6 wildcard P2P bind may be dual-stack on some operating systems; bind a concrete local IPv6 address to ensure OS-level IPv6-only listeners"
            .to_owned(),
    );
    bind_ip
}

fn apply_p2p_address_status(status: &mut SyncStatus, selection: &P2pAddressSelection) {
    status.p2p_address_mode = Some(selection.mode.as_str().to_owned());
    status.p2p_bind_ip = Some(selection.bind_ip.to_string());
    status.p2p_listen_families =
        dial_family_labels(DialAddressFamilies::for_bind_ip(selection.bind_ip));
    status.p2p_dial_families = dial_family_labels(selection.dial_families);
    status.p2p_advertised_families = if selection.external_ip.is_some() {
        dial_family_labels(selection.advertised_families)
    } else {
        Vec::new()
    };
    status.p2p_external_ip = selection.external_ip.map(|ip| ip.to_string());
    status.p2p_warnings = selection.warnings.clone();
}

fn add_runtime_p2p_warnings(selection: &mut P2pAddressSelection, execution_bootnodes: &[String]) {
    if selection.dial_families == DialAddressFamilies::IPV6 && execution_bootnodes.is_empty() {
        selection.warnings.push(
            "strict IPv6-only execution sync depends on public IPv6 EL peers; public DNS discovery can be sparse, so configure --execution-bootnode with IPv6 enode:// or enr: records if EL peers stay at zero. Proven serving peers are cached for restart."
                .to_owned(),
        );
    }
}

fn add_known_peer_fallback_warnings(
    selection: &mut P2pAddressSelection,
    loaded_known_peers: usize,
) {
    if selection.mode == P2pAddressSelectionMode::AutoOutboundOnly && loaded_known_peers == 0 {
        selection.warnings.push(
            "outbound-only execution p2p has no persisted known peers yet; startup will rely on bootnodes and DNS until serving peers are learned and cached"
                .to_owned(),
        );
    }
}

fn dial_family_labels(families: DialAddressFamilies) -> Vec<String> {
    let mut labels = Vec::with_capacity(2);
    if families.allows_ipv4() {
        labels.push("ipv4".to_owned());
    }
    if families.allows_ipv6() {
        labels.push("ipv6".to_owned());
    }
    labels
}

fn consensus_dial_families(families: DialAddressFamilies) -> ConsensusDialAddressFamilies {
    match (families.allows_ipv4(), families.allows_ipv6()) {
        (true, true) => ConsensusDialAddressFamilies::BOTH,
        (false, true) => ConsensusDialAddressFamilies::IPV6,
        _ => ConsensusDialAddressFamilies::IPV4,
    }
}

fn select_consensus_p2p_address(
    execution: &P2pAddressSelection,
    candidates: LocalP2pAddressCandidates,
) -> ConsensusP2pAddressSelection {
    if execution.mode == P2pAddressSelectionMode::AutoPublicIpv4
        && execution.dial_families.allows_ipv6()
        && let Some(public_ipv6) = candidates.public_ipv6()
    {
        let mut warnings = vec![
            "public IPv4 and IPv6 were both detected; execution advertises IPv4 by default while consensus advertises IPv6 because the beacon network has stronger IPv6 reachability"
                .to_owned(),
        ];
        let bind_ip = narrow_unspecified_ipv6_bind(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            Some(IpAddr::V6(public_ipv6)),
            candidates.ipv6,
            &mut warnings,
        );
        return ConsensusP2pAddressSelection {
            bind_ip,
            dial_families: ConsensusDialAddressFamilies::IPV6,
            external_ip: Some(IpAddr::V6(public_ipv6)),
            warnings,
        };
    }

    ConsensusP2pAddressSelection {
        bind_ip: execution.bind_ip,
        dial_families: consensus_dial_families(execution.dial_families),
        external_ip: execution.external_ip,
        warnings: Vec::new(),
    }
}

fn initial_sync_status(
    resume_block: u64,
    storage_anchors: &logex_types::ChainAnchors,
    historical_floor: Option<logex_types::ExecutionBlockMarker>,
    historical_anchor: Option<logex_types::ExecutionBlockMarker>,
    historical_sync_disabled: bool,
    consensus: Option<&ConsensusStore>,
) -> SyncStatus {
    let mut status = SyncStatus {
        current_block: resume_block,
        target_block: 0,
        historical_sync_disabled,
        historical_execution_floor: historical_floor,
        historical_execution_anchor: historical_anchor,
        historical_target_block: logex_sync::EXECUTION_HISTORY_TARGET_BLOCK,
        ..Default::default()
    };
    status.indexed_execution_head = storage_anchors.indexed_head;

    if let Some(consensus) = consensus {
        let anchors = consensus.chain_anchors();
        let anchor_coverage = consensus.anchor_coverage();
        status.checkpoint = Some(consensus.checkpoint());
        status.optimistic_execution_head = anchors.optimistic_head;
        status.finalized_execution_head = anchors.finalized_head;
        status.materialized_execution_floor = anchor_coverage.floor;
        status.materialized_execution_ceiling = anchor_coverage.ceiling;
        status.materialized_execution_anchor_count = anchor_coverage.count;
        status.materialized_execution_anchor_gap_count = anchor_coverage.gap_count;
        let light_client = consensus.light_client_status();
        status.consensus_light_client = (!light_client.is_empty()).then_some(light_client);
        if let Some(anchor) = anchors.optimistic_head {
            status.target_block = anchor.block_number;
        }
    }
    status
}

fn resolve_historical_sync_mode(
    data_dir: &Path,
    storage: &PartitionManager,
    consensus_state_exists: bool,
    disable_historical_sync_requested: bool,
) -> Result<HistoricalSyncMode, String> {
    let state = read_sync_mode_state(data_dir)?;
    match (
        disable_historical_sync_requested,
        state
            .as_ref()
            .is_some_and(|state| state.historical_sync_disabled),
    ) {
        (true, true) => Ok(HistoricalSyncMode::Disabled),
        (true, false) => {
            if storage_is_fresh_for_sync_mode(storage, consensus_state_exists) {
                write_sync_mode_state(
                    data_dir,
                    &SyncModeState {
                        historical_sync_disabled: true,
                    },
                )?;
                Ok(HistoricalSyncMode::Disabled)
            } else {
                Err(
                    "--disable-historical-sync can only be used with a fresh LogEx data directory. This data directory was already initialized without the flag; restart without --disable-historical-sync or use a new --data-dir."
                        .to_owned(),
                )
            }
        }
        (false, true) => {
            remove_sync_mode_state(data_dir)?;
            tracing::info!(
                data_dir = %data_dir.display(),
                "historical sync was previously disabled; restarting without --disable-historical-sync enables normal historical backfill"
            );
            Ok(HistoricalSyncMode::Enabled)
        }
        (false, false) => Ok(HistoricalSyncMode::Enabled),
    }
}

fn storage_is_fresh_for_sync_mode(
    storage: &PartitionManager,
    consensus_state_exists: bool,
) -> bool {
    !consensus_state_exists
        && storage.sync_head().is_none()
        && storage.total_rows() == 0
        && storage.head_block().is_none()
        && storage.indexed_head_block().is_none()
        && storage.historical_floor().is_none()
        && storage.historical_anchor().is_none()
}

fn startup_network_head(sync_head: Option<SyncHead>, consensus: Option<&ConsensusStore>) -> Head {
    match sync_head {
        Some(head) if head.block_number == 0 || head.timestamp > 0 => network_head(
            head.block_number,
            head.block_hash,
            if head.block_number == 0 {
                MAINNET.genesis().timestamp
            } else {
                head.timestamp
            },
        ),
        Some(head) => {
            tracing::warn!(
                block_number = head.block_number,
                "sync metadata is missing the block timestamp, starting execution network status from consensus until a new verified block updates it"
            );
            consensus
                .and_then(|consensus| consensus.anchor_coverage().ceiling)
                .map(consensus_anchor_network_head)
                .or_else(|| consensus.map(consensus_network_head))
                .unwrap_or_else(genesis_network_head)
        }
        None => consensus
            .and_then(|consensus| consensus.anchor_coverage().ceiling)
            .map(consensus_anchor_network_head)
            .or_else(|| consensus.map(consensus_network_head))
            .unwrap_or_else(genesis_network_head),
    }
}

fn consensus_anchor_network_head(anchor: logex_types::ExecutionAnchor) -> Head {
    network_head(
        anchor.block_number,
        anchor.block_hash,
        MAINNET_CONSENSUS_CHAIN_SPEC
            .genesis_time
            .saturating_add(anchor.beacon_slot.saturating_mul(MAINNET_SECONDS_PER_SLOT)),
    )
}

fn consensus_network_head(consensus: &ConsensusStore) -> Head {
    if let Some(anchor) =
        consensus_execution_head_anchor(consensus.chain_anchors(), consensus.anchor_coverage())
    {
        return consensus_anchor_network_head(anchor);
    }

    let timestamp = consensus
        .checkpoint()
        .beacon_slot
        .map(consensus_slot_timestamp)
        .unwrap_or_else(current_unix_timestamp);
    network_head(0, MAINNET.genesis_hash(), timestamp)
}

fn consensus_execution_head_anchor(
    anchors: ChainAnchors,
    coverage: AnchorCoverage,
) -> Option<ExecutionAnchor> {
    [
        anchors.optimistic_head,
        anchors.finalized_head,
        coverage.ceiling,
    ]
    .into_iter()
    .flatten()
    .max_by_key(|anchor| anchor.block_number)
}

fn consensus_slot_timestamp(slot: u64) -> u64 {
    MAINNET_CONSENSUS_CHAIN_SPEC
        .genesis_time
        .saturating_add(slot.saturating_mul(MAINNET_SECONDS_PER_SLOT))
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(MAINNET_CONSENSUS_CHAIN_SPEC.genesis_time)
}

fn genesis_network_head() -> Head {
    network_head(0, MAINNET.genesis_hash(), MAINNET.genesis().timestamp)
}

fn network_head(number: u64, hash: alloy_primitives::B256, timestamp: u64) -> Head {
    Head {
        number,
        hash,
        timestamp,
        difficulty: if number == 0 {
            MAINNET.genesis().difficulty
        } else {
            U256::ZERO
        },
        total_difficulty: if number == 0 {
            MAINNET.genesis().difficulty
        } else {
            MAINNET
                .final_paris_total_difficulty()
                .unwrap_or(MAINNET.genesis().difficulty)
        },
    }
}

fn maybe_open_consensus_store(
    data_dir: &std::path::Path,
    storage: &PartitionManager,
    checkpoint: Option<&str>,
) -> Result<Option<ConsensusStore>, ConsensusStateError> {
    if consensus_state_exists(data_dir)? || checkpoint.is_some() {
        return ConsensusStore::open(data_dir, checkpoint).map(Some);
    }

    if storage.sync_head().is_none() && storage.total_rows() == 0 {
        return Err(ConsensusStateError::MissingCheckpoint);
    }

    Ok(None)
}

async fn resolve_checkpoint_or_exit(
    checkpoint: Option<String>,
    checkpoint_sync_url: Option<&str>,
) -> Option<String> {
    match resolve_checkpoint(checkpoint, checkpoint_sync_url).await {
        Ok(checkpoint) => checkpoint,
        Err(error) => {
            tracing::error!(%error, "failed to resolve weak-subjectivity checkpoint");
            std::process::exit(1);
        }
    }
}

async fn refresh_consensus_state_from_recent_checkpoint(
    data_dir: &Path,
    checkpoint: &mut Option<String>,
    checkpoint_sync_url: Option<&str>,
    reason: &str,
) -> Result<ConsensusStore, String> {
    let checkpoint = resolve_checkpoint_for_recovery(checkpoint, checkpoint_sync_url).await?;
    if let Some(archive_path) =
        archive_consensus_state(data_dir, reason).map_err(|error| error.to_string())?
    {
        tracing::warn!(
            path = %archive_path.display(),
            "archived stale consensus state before checkpoint refresh"
        );
    }
    ConsensusStore::open(data_dir, Some(&checkpoint)).map_err(|error| error.to_string())
}

async fn resolve_checkpoint_for_recovery(
    checkpoint: &mut Option<String>,
    checkpoint_sync_url: Option<&str>,
) -> Result<String, String> {
    if let Some(checkpoint) = checkpoint.clone() {
        return Ok(checkpoint);
    }

    let resolved = resolve_checkpoint(None, checkpoint_sync_url)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "checkpoint-sync source did not return a checkpoint".to_owned())?;
    *checkpoint = Some(resolved.clone());
    Ok(resolved)
}

fn archive_consensus_state(
    data_dir: &Path,
    reason: &str,
) -> Result<Option<PathBuf>, ConsensusStateError> {
    let path = consensus_state_path(data_dir);
    match fs::symlink_metadata(&path) {
        Ok(_) => {}
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(ConsensusStateError::ReadState { path, source }),
    }

    let parent = path.parent().unwrap_or(data_dir);
    let archive = logex_fs::StagedDirectory::new_in(parent, &format!(".consensus-state-{reason}-"))
        .map_err(|source| ConsensusStateError::PersistState {
            path: parent.to_path_buf(),
            source,
        })?;
    // Persist the new directory link before removing the original state's name.
    fs::File::open(parent)
        .and_then(|file| file.sync_all())
        .map_err(|source| ConsensusStateError::PersistState {
            path: parent.to_path_buf(),
            source,
        })?;
    let archive_path = archive.path().join(
        path.file_name()
            .expect("consensus state path has a filename"),
    );
    fs::rename(&path, &archive_path).map_err(|source| ConsensusStateError::PersistState {
        path: archive_path.clone(),
        source,
    })?;
    // Once moved, preserve the original even if a later durability barrier fails.
    let archive_dir = archive.keep();
    for directory in [archive_dir.as_path(), parent] {
        fs::File::open(directory)
            .and_then(|file| file.sync_all())
            .map_err(|source| ConsensusStateError::PersistState {
                path: directory.to_path_buf(),
                source,
            })?;
    }
    Ok(Some(archive_path))
}

#[derive(Debug, Clone, Copy)]
struct RecentConsensusStateStaleness {
    trusted_slot: u64,
    trusted_epoch: u64,
    current_epoch: u64,
    max_epochs: u64,
}

fn recent_consensus_state_staleness(
    consensus: &ConsensusStore,
) -> Option<RecentConsensusStateStaleness> {
    let trusted_slot = consensus.trusted_beacon_slot()?;
    let trusted_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(trusted_slot);
    let current_epoch = MAINNET_CONSENSUS_CHAIN_SPEC.wall_clock_epoch();
    (current_epoch > trusted_epoch.saturating_add(RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG))
        .then_some(RecentConsensusStateStaleness {
            trusted_slot,
            trusted_epoch,
            current_epoch,
            max_epochs: RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG,
        })
}

#[derive(Debug, Clone, Copy)]
struct LocalExecutionProgressStaleness {
    block_number: u64,
    timestamp: u64,
    age_secs: u64,
    max_age_secs: u64,
}

fn local_execution_progress_staleness(
    sync_head: Option<SyncHead>,
    consensus: &ConsensusStore,
) -> Option<LocalExecutionProgressStaleness> {
    let mut latest = sync_head
        .filter(|head| head.timestamp > 0)
        .map(|head| (head.block_number, head.timestamp));
    let anchor_coverage = consensus.anchor_coverage();
    if anchor_coverage.gap_count == 0 {
        if let Some(anchor) = anchor_coverage.ceiling {
            let anchor_timestamp = consensus_slot_timestamp(anchor.beacon_slot);
            if latest
                .map(|(_, timestamp)| anchor_timestamp > timestamp)
                .unwrap_or(true)
            {
                latest = Some((anchor.block_number, anchor_timestamp));
            }
        }
    } else if latest.is_none()
        && let Some(anchor) = anchor_coverage.floor
    {
        let anchor_timestamp = consensus_slot_timestamp(anchor.beacon_slot);
        latest = Some((anchor.block_number, anchor_timestamp));
    }

    let (block_number, timestamp) = latest?;
    let now = current_unix_timestamp();
    let age_secs = now.saturating_sub(timestamp);
    let max_age_secs = recent_checkpoint_max_age_secs();
    (age_secs > max_age_secs).then_some(LocalExecutionProgressStaleness {
        block_number,
        timestamp,
        age_secs,
        max_age_secs,
    })
}

fn recent_checkpoint_max_age_secs() -> u64 {
    RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG
        .saturating_mul(MAINNET_SLOTS_PER_EPOCH)
        .saturating_mul(MAINNET_SECONDS_PER_SLOT)
}

async fn wait_for_shutdown_signal() -> &'static str {
    #[cfg(unix)]
    {
        let mut interrupt =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
                .expect("failed to install SIGINT handler");
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = interrupt.recv() => "SIGINT",
            _ = terminate.recv() => "SIGTERM",
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        "SIGINT"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

    #[test]
    fn runtime_failure_watchdog_expires_without_application_runtime() {
        for initially_failed in [true, false] {
            let message: Arc<str> = Arc::from("state write failed");
            let (failure, receiver) =
                tokio::sync::watch::channel(initially_failed.then(|| Arc::clone(&message)));
            let (expired, observed) = std::sync::mpsc::channel();
            let _watchdog =
                start_runtime_failure_watchdog(receiver, Duration::from_millis(20), move || {
                    expired.send(()).unwrap()
                })
                .unwrap();
            if !initially_failed {
                failure.send(Some(message)).unwrap();
            }
            // No Tokio application runtime exists in this test. The expiry
            // callback reports through a channel instead of exiting a process.
            observed.recv_timeout(Duration::from_secs(5)).unwrap();
        }
    }

    #[test]
    fn runtime_failure_watchdog_drop_releases_waiting_thread() {
        let (_failure, receiver) = tokio::sync::watch::channel(None);
        let (expired, observed) = std::sync::mpsc::channel();
        let watchdog =
            start_runtime_failure_watchdog(receiver, Duration::from_secs(30), move || {
                expired.send(()).unwrap()
            })
            .unwrap();
        drop(watchdog);
        // Disconnection proves the worker released its callback without firing
        // it or remaining blocked on the fault/grace period.
        assert_eq!(
            observed.recv_timeout(Duration::from_secs(5)),
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
        );
    }

    #[test]
    fn runtime_failure_watchdog_deadline_is_disarmed_by_cleanup() {
        for send_completion in [true, false] {
            let (complete, completed) = std::sync::mpsc::channel();
            if send_completion {
                complete.send(()).unwrap();
            }
            drop(complete);
            let expired = std::cell::Cell::new(false);
            finish_runtime_failure_watchdog(completed, Duration::ZERO, || expired.set(true));
            assert!(!expired.get());
        }
    }

    #[tokio::test]
    async fn consensus_storage_failure_waiter_observes_existing_latch() {
        let (sender, _) = tokio::sync::watch::channel(None);
        let message: Arc<str> = Arc::from("consensus state write failed");
        sender.send_replace(Some(Arc::clone(&message)));
        let mut receiver = Some(sender.subscribe());
        assert_eq!(wait_for_runtime_failure(&mut receiver).await, message);
    }

    #[tokio::test]
    async fn consensus_storage_failure_waiter_observes_new_latch() {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        let mut receiver = Some(receiver);
        let mut waiting = pin!(wait_for_runtime_failure(&mut receiver));
        assert!(futures_util::poll!(&mut waiting).is_pending());
        let message: Arc<str> = Arc::from("consensus state rename failed");
        sender.send(Some(Arc::clone(&message))).unwrap();
        assert_eq!(waiting.await, message);
    }

    #[tokio::test]
    async fn consensus_storage_failure_waiter_without_store_remains_pending() {
        let mut receiver = None;
        let mut waiting = pin!(wait_for_runtime_failure(&mut receiver));
        assert!(futures_util::poll!(&mut waiting).is_pending());
    }

    #[tokio::test]
    async fn consensus_storage_failure_waiter_closed_channel_is_not_a_failure() {
        let (sender, receiver) = tokio::sync::watch::channel(None);
        drop(sender);
        let mut receiver = Some(receiver);
        let mut waiting = pin!(wait_for_runtime_failure(&mut receiver));
        assert!(futures_util::poll!(&mut waiting).is_pending());
    }

    #[test]
    fn consensus_storage_failure_marks_sync_unavailable() {
        let status = std::sync::Mutex::new(SyncStatus {
            syncing: true,
            consensus_head_fresh: Some(true),
            eta_seconds: Some(10.0),
            historical_eta_seconds: Some(20.0),
            ..Default::default()
        });
        mark_sync_stopped_for_runtime_failure(&status, true);
        let status = status.lock().unwrap();
        assert!(!status.syncing);
        assert_eq!(status.consensus_head_fresh, Some(false));
        assert_eq!(status.node_state, logex_types::NodeState::Disconnected);
        assert!(status.eta_seconds.is_none());
        assert!(status.historical_eta_seconds.is_none());
    }

    #[test]
    fn execution_failure_marks_sync_unavailable_without_changing_consensus_freshness() {
        for freshness in [None, Some(false), Some(true)] {
            let status = std::sync::Mutex::new(SyncStatus {
                syncing: true,
                consensus_head_fresh: freshness,
                eta_seconds: Some(10.0),
                historical_eta_seconds: Some(20.0),
                ..Default::default()
            });
            mark_sync_stopped_for_runtime_failure(&status, false);
            let status = status.lock().unwrap();
            assert!(!status.syncing);
            assert_eq!(status.consensus_head_fresh, freshness);
            assert_eq!(status.node_state, logex_types::NodeState::Disconnected);
            assert!(status.eta_seconds.is_none());
            assert!(status.historical_eta_seconds.is_none());
        }
    }

    fn checkpoint_at_slot(slot: u64) -> String {
        format!("{slot}@{:#x}", B256::repeat_byte(0x42))
    }

    fn open_storage_at(path: &Path) -> PartitionManager {
        PartitionManager::open(PartitionManagerConfig {
            data_dir: path.to_path_buf(),
            ..PartitionManagerConfig::default()
        })
        .unwrap()
    }

    fn consensus_store_at_slot(slot: u64) -> (tempfile::TempDir, ConsensusStore) {
        let temp = tempfile::tempdir().unwrap();
        let checkpoint = checkpoint_at_slot(slot);
        let store = ConsensusStore::open(temp.path(), Some(&checkpoint)).unwrap();
        (temp, store)
    }

    fn recent_checkpoint_slot(epoch_lag: u64) -> u64 {
        MAINNET_CONSENSUS_CHAIN_SPEC
            .wall_clock_epoch()
            .saturating_sub(epoch_lag)
            .saturating_mul(MAINNET_SLOTS_PER_EPOCH)
    }

    fn anchor_record(block_number: u64, beacon_slot: u64) -> logex_cl::AnchorRecord {
        logex_cl::AnchorRecord {
            anchor: logex_types::ExecutionAnchor {
                beacon_root: B256::repeat_byte(0x51),
                beacon_slot,
                block_number,
                block_hash: B256::repeat_byte(0x52),
                receipts_root: B256::repeat_byte(0x53),
            },
            finalized: true,
            parent_beacon_root: None,
        }
    }

    fn execution_anchor(block_number: u64, beacon_slot: u64) -> logex_types::ExecutionAnchor {
        logex_types::ExecutionAnchor {
            beacon_root: B256::repeat_byte((block_number % 251) as u8),
            beacon_slot,
            block_number,
            block_hash: B256::repeat_byte((block_number % 253) as u8),
            receipts_root: B256::repeat_byte((block_number % 241) as u8),
        }
    }

    #[test]
    fn auto_p2p_selection_prefers_public_ipv4() {
        let selection = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: Some(Ipv4Addr::new(203, 0, 114, 10)),
                ipv6: Some("2604:a880:400:d0::1".parse().unwrap()),
            },
            None,
        );

        assert_eq!(selection.mode, P2pAddressSelectionMode::AutoPublicIpv4);
        assert_eq!(
            selection.nat,
            NatResolver::ExternalIp(IpAddr::V4(Ipv4Addr::new(203, 0, 114, 10)))
        );
        assert_eq!(selection.bind_ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(selection.dial_families, DialAddressFamilies::BOTH);
        assert_eq!(selection.advertised_families, DialAddressFamilies::IPV4);
        assert_eq!(
            selection.external_ip,
            Some(IpAddr::V4(Ipv4Addr::new(203, 0, 114, 10)))
        );
        assert_eq!(selection.warnings.len(), 1);
        assert!(selection.warnings[0].contains("both detected"));
    }

    #[test]
    fn auto_p2p_selection_falls_back_to_public_ipv6() {
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let selection = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: None,
                ipv6: Some(public_ipv6),
            },
            None,
        );

        assert_eq!(selection.mode, P2pAddressSelectionMode::AutoPublicIpv6);
        assert_eq!(
            selection.nat,
            NatResolver::ExternalIp(IpAddr::V6(public_ipv6))
        );
        assert_eq!(selection.bind_ip, IpAddr::V6(public_ipv6));
        assert_eq!(selection.dial_families, DialAddressFamilies::IPV6);
        assert_eq!(selection.advertised_families, DialAddressFamilies::IPV6);
        assert_eq!(selection.external_ip, Some(IpAddr::V6(public_ipv6)));
    }

    #[test]
    fn auto_p2p_selection_advertises_public_ipv6_and_dials_outbound_ipv4() {
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let selection = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: Some(Ipv4Addr::new(10, 0, 0, 24)),
                ipv6: Some(public_ipv6),
            },
            None,
        );

        assert_eq!(selection.mode, P2pAddressSelectionMode::AutoPublicIpv6);
        assert_eq!(
            selection.nat,
            NatResolver::ExternalIp(IpAddr::V6(public_ipv6))
        );
        assert_eq!(selection.bind_ip, IpAddr::V6(public_ipv6));
        assert_eq!(selection.dial_families, DialAddressFamilies::BOTH);
        assert_eq!(selection.advertised_families, DialAddressFamilies::IPV6);
        assert_eq!(selection.external_ip, Some(IpAddr::V6(public_ipv6)));
        assert_eq!(selection.warnings.len(), 1);
        assert!(selection.warnings[0].contains("outbound IPv4"));
    }

    #[test]
    fn ipv6_only_selection_warns_without_execution_bootnodes() {
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let mut selection = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: None,
                ipv6: Some(public_ipv6),
            },
            None,
        );

        add_runtime_p2p_warnings(&mut selection, &[]);

        assert_eq!(selection.warnings.len(), 1);
        assert!(selection.warnings[0].contains("IPv6-only execution sync"));
        assert!(selection.warnings[0].contains("--execution-bootnode"));
        assert!(selection.warnings[0].contains("cached for restart"));
    }

    #[test]
    fn ipv6_only_selection_does_not_warn_with_execution_bootnodes() {
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let mut selection = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: None,
                ipv6: Some(public_ipv6),
            },
            None,
        );

        add_runtime_p2p_warnings(
            &mut selection,
            &["enode://abc@[2604:a880:400:d0::2]:30303".to_owned()],
        );

        assert!(selection.warnings.is_empty());
    }

    #[test]
    fn consensus_selection_uses_public_ipv6_when_execution_defaults_to_ipv4_on_dual_stack() {
        let public_ipv4 = Ipv4Addr::new(203, 0, 114, 10);
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let execution = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: Some(public_ipv4),
                ipv6: Some(public_ipv6),
            },
            None,
        );

        let consensus = select_consensus_p2p_address(
            &execution,
            LocalP2pAddressCandidates {
                ipv4: Some(public_ipv4),
                ipv6: Some(public_ipv6),
            },
        );

        assert_eq!(execution.mode, P2pAddressSelectionMode::AutoPublicIpv4);
        assert_eq!(execution.bind_ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(execution.external_ip, Some(IpAddr::V4(public_ipv4)));
        assert_eq!(consensus.bind_ip, IpAddr::V6(public_ipv6));
        assert_eq!(consensus.external_ip, Some(IpAddr::V6(public_ipv6)));
        assert_eq!(consensus.dial_families, ConsensusDialAddressFamilies::IPV6);
        assert_eq!(consensus.warnings.len(), 1);
        assert!(consensus.warnings[0].contains("consensus advertises IPv6"));
    }

    #[test]
    fn consensus_selection_keeps_explicit_ipv6_strict() {
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let mut warnings = Vec::new();
        let execution = choose_explicit_p2p_address(
            NatResolver::ExternalIp(IpAddr::V6(public_ipv6)),
            Some(IpAddr::V6(public_ipv6)),
            30303,
            LocalP2pAddressCandidates {
                ipv4: Some(Ipv4Addr::new(10, 0, 0, 24)),
                ipv6: Some(public_ipv6),
            },
            &mut warnings,
        );

        let consensus = select_consensus_p2p_address(
            &execution,
            LocalP2pAddressCandidates {
                ipv4: Some(Ipv4Addr::new(10, 0, 0, 24)),
                ipv6: Some(public_ipv6),
            },
        );

        assert_eq!(execution.mode, P2pAddressSelectionMode::Explicit);
        assert_eq!(consensus.bind_ip, IpAddr::V6(public_ipv6));
        assert_eq!(consensus.external_ip, Some(IpAddr::V6(public_ipv6)));
        assert_eq!(consensus.dial_families, ConsensusDialAddressFamilies::IPV6);
        assert!(consensus.warnings.is_empty());
    }

    #[test]
    fn consensus_selection_keeps_auto_ipv4_when_no_public_ipv6_exists() {
        let public_ipv4 = Ipv4Addr::new(203, 0, 114, 10);
        let execution = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: Some(public_ipv4),
                ipv6: None,
            },
            None,
        );

        let consensus = select_consensus_p2p_address(
            &execution,
            LocalP2pAddressCandidates {
                ipv4: Some(public_ipv4),
                ipv6: None,
            },
        );

        assert_eq!(execution.mode, P2pAddressSelectionMode::AutoPublicIpv4);
        assert_eq!(consensus.bind_ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(consensus.external_ip, Some(IpAddr::V4(public_ipv4)));
        assert_eq!(consensus.dial_families, ConsensusDialAddressFamilies::IPV4);
        assert!(consensus.warnings.is_empty());
    }

    #[test]
    fn auto_p2p_selection_narrows_unspecified_ipv6_bind_to_public_ipv6() {
        let public_ipv4 = Ipv4Addr::new(203, 0, 114, 10);
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();

        let selection = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: Some(public_ipv4),
                ipv6: Some(public_ipv6),
            },
            Some(IpAddr::V6(Ipv6Addr::UNSPECIFIED)),
        );

        assert_eq!(selection.mode, P2pAddressSelectionMode::AutoPublicIpv6);
        assert_eq!(selection.bind_ip, IpAddr::V6(public_ipv6));
        assert_eq!(selection.dial_families, DialAddressFamilies::IPV6);
        assert_eq!(selection.advertised_families, DialAddressFamilies::IPV6);
        assert_eq!(selection.external_ip, Some(IpAddr::V6(public_ipv6)));
    }

    #[test]
    fn ipv6_wildcard_bind_warns_when_external_ipv6_is_not_local() {
        let mut warnings = Vec::new();
        let bind_ip = narrow_unspecified_ipv6_bind(
            IpAddr::V6(Ipv6Addr::UNSPECIFIED),
            Some(IpAddr::V6("2604:a880:400:d0::1".parse().unwrap())),
            Some("2604:a880:400:d0::2".parse().unwrap()),
            &mut warnings,
        );

        assert_eq!(bind_ip, IpAddr::V6(Ipv6Addr::UNSPECIFIED));
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("dual-stack"));
    }

    #[test]
    fn auto_p2p_selection_respects_explicit_ipv6_bind_family() {
        let public_ipv4 = Ipv4Addr::new(203, 0, 114, 10);
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let bind_ip = "2001:db8::1234".parse::<IpAddr>().unwrap();

        let selection = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: Some(public_ipv4),
                ipv6: Some(public_ipv6),
            },
            Some(bind_ip),
        );

        assert_eq!(selection.mode, P2pAddressSelectionMode::AutoPublicIpv6);
        assert_eq!(selection.bind_ip, bind_ip);
        assert_eq!(selection.dial_families, DialAddressFamilies::IPV6);
        assert_eq!(
            selection.nat,
            NatResolver::ExternalIp(IpAddr::V6(public_ipv6))
        );
    }

    #[test]
    fn explicit_ipv6_nat_without_bind_keeps_outbound_ipv4_route() {
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let mut warnings = Vec::new();
        let selection = choose_explicit_p2p_address(
            NatResolver::ExternalIp(IpAddr::V6(public_ipv6)),
            None,
            30303,
            LocalP2pAddressCandidates {
                ipv4: Some(Ipv4Addr::new(10, 0, 0, 24)),
                ipv6: Some(public_ipv6),
            },
            &mut warnings,
        );

        assert_eq!(selection.mode, P2pAddressSelectionMode::Explicit);
        assert_eq!(selection.bind_ip, IpAddr::V6(public_ipv6));
        assert_eq!(selection.advertised_families, DialAddressFamilies::IPV6);
        assert_eq!(selection.dial_families, DialAddressFamilies::BOTH);
        assert_eq!(selection.external_ip, Some(IpAddr::V6(public_ipv6)));
        assert!(selection.warnings.is_empty());
    }

    #[test]
    fn explicit_ipv6_bind_remains_strict_ipv6() {
        let public_ipv6 = "2604:a880:400:d0::1".parse::<Ipv6Addr>().unwrap();
        let mut warnings = Vec::new();
        let selection = choose_explicit_p2p_address(
            NatResolver::ExternalIp(IpAddr::V6(public_ipv6)),
            Some(IpAddr::V6(public_ipv6)),
            30303,
            LocalP2pAddressCandidates {
                ipv4: Some(Ipv4Addr::new(10, 0, 0, 24)),
                ipv6: Some(public_ipv6),
            },
            &mut warnings,
        );

        assert_eq!(selection.bind_ip, IpAddr::V6(public_ipv6));
        assert_eq!(selection.advertised_families, DialAddressFamilies::IPV6);
        assert_eq!(selection.dial_families, DialAddressFamilies::IPV6);
        assert!(selection.warnings.is_empty());
    }

    #[test]
    fn explicit_nat_none_keeps_all_routed_outbound_families() {
        let mut warnings = Vec::new();
        let selection = choose_explicit_p2p_address(
            NatResolver::None,
            None,
            30303,
            LocalP2pAddressCandidates {
                ipv4: Some(Ipv4Addr::new(10, 0, 0, 24)),
                ipv6: Some("fd00::24".parse().unwrap()),
            },
            &mut warnings,
        );

        assert_eq!(selection.mode, P2pAddressSelectionMode::Explicit);
        assert_eq!(selection.nat, NatResolver::None);
        assert_eq!(selection.bind_ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(selection.advertised_families, DialAddressFamilies::IPV4);
        assert_eq!(selection.dial_families, DialAddressFamilies::BOTH);
        assert_eq!(selection.external_ip, None);
    }

    #[test]
    fn auto_p2p_selection_uses_outbound_only_without_public_address() {
        let selection = choose_auto_p2p_address(LocalP2pAddressCandidates::default(), None);

        assert_eq!(selection.mode, P2pAddressSelectionMode::AutoOutboundOnly);
        assert_eq!(selection.nat, NatResolver::None);
        assert_eq!(selection.bind_ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(selection.dial_families, DialAddressFamilies::IPV4);
        assert_eq!(selection.advertised_families, DialAddressFamilies::IPV4);
        assert_eq!(selection.external_ip, None);
        assert_eq!(selection.warnings.len(), 1);
        assert!(selection.warnings[0].contains("outbound-only"));
    }

    #[test]
    fn auto_p2p_selection_outbound_only_keeps_both_routed_families() {
        let selection = choose_auto_p2p_address(
            LocalP2pAddressCandidates {
                ipv4: Some(Ipv4Addr::new(10, 0, 0, 24)),
                ipv6: Some("fd00::24".parse().unwrap()),
            },
            None,
        );

        assert_eq!(selection.mode, P2pAddressSelectionMode::AutoOutboundOnly);
        assert_eq!(selection.nat, NatResolver::None);
        assert_eq!(selection.bind_ip, IpAddr::V4(Ipv4Addr::UNSPECIFIED));
        assert_eq!(selection.dial_families, DialAddressFamilies::BOTH);
        assert_eq!(selection.advertised_families, DialAddressFamilies::IPV4);
        assert_eq!(selection.external_ip, None);
        assert_eq!(selection.warnings.len(), 1);
        assert!(selection.warnings[0].contains("outbound-only"));
    }

    #[test]
    fn outbound_only_selection_warns_without_persisted_known_peers() {
        let mut selection = choose_auto_p2p_address(LocalP2pAddressCandidates::default(), None);

        add_known_peer_fallback_warnings(&mut selection, 0);

        assert_eq!(selection.mode, P2pAddressSelectionMode::AutoOutboundOnly);
        assert!(selection.warnings.iter().any(|warning| {
            warning.contains("outbound-only execution p2p")
                && warning.contains("no persisted known peers")
        }));
    }

    #[test]
    fn outbound_only_selection_does_not_warn_when_known_peers_exist() {
        let mut selection = choose_auto_p2p_address(LocalP2pAddressCandidates::default(), None);
        let warning_count = selection.warnings.len();

        add_known_peer_fallback_warnings(&mut selection, 3);

        assert_eq!(selection.mode, P2pAddressSelectionMode::AutoOutboundOnly);
        assert_eq!(selection.warnings.len(), warning_count);
    }

    #[test]
    fn public_ipv4_filter_rejects_private_shared_and_documentation_ranges() {
        for ip in [
            Ipv4Addr::new(10, 1, 2, 3),
            Ipv4Addr::new(172, 20, 1, 1),
            Ipv4Addr::new(192, 168, 1, 1),
            Ipv4Addr::new(100, 64, 1, 1),
            Ipv4Addr::new(100, 127, 255, 254),
            Ipv4Addr::new(192, 0, 2, 1),
            Ipv4Addr::new(198, 51, 100, 1),
            Ipv4Addr::new(203, 0, 113, 1),
            Ipv4Addr::new(198, 18, 0, 1),
            Ipv4Addr::new(224, 0, 0, 1),
        ] {
            assert!(!is_public_ipv4(ip), "{ip} should not be public");
        }

        assert!(is_public_ipv4(Ipv4Addr::new(8, 8, 8, 8)));
        assert!(is_public_ipv4(Ipv4Addr::new(203, 0, 114, 1)));
    }

    #[test]
    fn public_ipv6_filter_rejects_non_public_ranges() {
        for ip in [
            Ipv6Addr::LOCALHOST,
            "fe80::1".parse().unwrap(),
            "fc00::1".parse().unwrap(),
            "fd00::1".parse().unwrap(),
            "100::1".parse().unwrap(),
            "64:ff9b::1".parse().unwrap(),
            "2001::1".parse().unwrap(),
            "2001:10::1".parse().unwrap(),
            "2001:db8::1".parse().unwrap(),
            "2001:2::1".parse().unwrap(),
            "2001:20::1".parse().unwrap(),
            "2002::1".parse().unwrap(),
            "3fff::1".parse().unwrap(),
            "8000::1".parse().unwrap(),
        ] {
            assert!(!is_public_ipv6(ip), "{ip} should not be public");
        }

        assert!(is_public_ipv6("2604:a880:400:d0::1".parse().unwrap()));
        assert!(is_public_ipv6("2a00:1450:4001:80b::200e".parse().unwrap()));
    }

    #[test]
    fn native_consensus_state_reopens_without_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let storage = open_storage_at(temp.path());
        let checkpoint = checkpoint_at_slot(recent_checkpoint_slot(1));
        let original = ConsensusStore::open(temp.path(), Some(&checkpoint)).unwrap();
        assert!(consensus_state_exists(temp.path()).unwrap());
        let reopened = maybe_open_consensus_store(temp.path(), &storage, None)
            .unwrap()
            .unwrap();
        assert_eq!(original.checkpoint(), reopened.checkpoint());
    }

    #[test]
    fn legacy_consensus_state_is_preserved_and_not_replaced() {
        let temp = tempfile::tempdir().unwrap();
        let storage = open_storage_at(temp.path());
        fs::create_dir_all(temp.path().join("cl")).unwrap();
        let legacy = temp.path().join("cl/consensus_state.json");
        fs::write(&legacy, b"legacy evidence").unwrap();
        assert!(consensus_state_exists(temp.path()).unwrap());
        let checkpoint = checkpoint_at_slot(recent_checkpoint_slot(1));
        for requested in [None, Some(checkpoint.as_str())] {
            assert!(matches!(
                maybe_open_consensus_store(temp.path(), &storage, requested),
                Err(ConsensusStateError::ParseState { .. })
            ));
        }
        assert_eq!(fs::read(&legacy).unwrap(), b"legacy evidence");
        assert!(!consensus_state_path(temp.path()).exists());
    }

    #[test]
    fn consensus_state_metadata_failure_is_not_missing() {
        let temp = tempfile::tempdir().unwrap();
        let storage = open_storage_at(temp.path());
        fs::write(temp.path().join("cl"), b"preserve parent").unwrap();
        assert!(matches!(
            maybe_open_consensus_store(temp.path(), &storage, None),
            Err(ConsensusStateError::ReadState { .. })
        ));
        assert_eq!(
            fs::read(temp.path().join("cl")).unwrap(),
            b"preserve parent"
        );
    }

    #[cfg(unix)]
    #[test]
    fn dangling_consensus_state_is_not_missing() {
        for legacy in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let storage = open_storage_at(temp.path());
            fs::create_dir_all(temp.path().join("cl")).unwrap();
            let native_path = consensus_state_path(temp.path());
            let path = if legacy {
                temp.path().join("cl/consensus_state.json")
            } else {
                native_path.clone()
            };
            std::os::unix::fs::symlink("missing-target", &path).unwrap();
            assert!(consensus_state_exists(temp.path()).unwrap());
            assert!(maybe_open_consensus_store(temp.path(), &storage, None).is_err());
            assert_eq!(
                fs::read_link(&path).unwrap(),
                PathBuf::from("missing-target")
            );
            if legacy {
                assert!(fs::symlink_metadata(native_path).is_err());
            }
        }
    }

    #[test]
    fn fresh_data_directory_requires_checkpoint_before_sync() {
        let temp = tempfile::tempdir().unwrap();
        let storage = open_storage_at(temp.path());

        let error = maybe_open_consensus_store(temp.path(), &storage, None).unwrap_err();

        assert!(matches!(error, ConsensusStateError::MissingCheckpoint));
    }

    #[test]
    fn fresh_data_directory_accepts_recent_checkpoint() {
        let temp = tempfile::tempdir().unwrap();
        let storage = open_storage_at(temp.path());
        let checkpoint = checkpoint_at_slot(recent_checkpoint_slot(1));

        let consensus = maybe_open_consensus_store(temp.path(), &storage, Some(&checkpoint))
            .unwrap()
            .unwrap();

        assert!(recent_consensus_state_staleness(&consensus).is_none());
    }

    #[test]
    fn restart_guard_accepts_recent_consensus_trusted_slot() {
        let recent_slot = recent_checkpoint_slot(1);
        let (_temp, consensus) = consensus_store_at_slot(recent_slot);

        assert!(recent_consensus_state_staleness(&consensus).is_none());
    }

    #[test]
    fn restart_guard_rejects_consensus_trusted_slot_outside_recent_window() {
        let stale_slot =
            recent_checkpoint_slot(RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG.saturating_add(1));
        let (_temp, consensus) = consensus_store_at_slot(stale_slot);

        let staleness = recent_consensus_state_staleness(&consensus).unwrap();

        assert_eq!(staleness.trusted_slot, stale_slot);
        assert_eq!(
            staleness.trusted_epoch,
            MAINNET_CONSENSUS_CHAIN_SPEC.epoch_for_slot(stale_slot)
        );
        assert_eq!(
            staleness.max_epochs,
            RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG
        );
    }

    #[test]
    fn restart_guard_accepts_recent_local_execution_progress() {
        let recent_slot = recent_checkpoint_slot(1);
        let (_temp, consensus) = consensus_store_at_slot(recent_slot);
        let sync_head = SyncHead {
            block_number: 25_000_000,
            block_hash: B256::repeat_byte(0x61),
            timestamp: current_unix_timestamp(),
        };

        assert!(local_execution_progress_staleness(Some(sync_head), &consensus).is_none());
    }

    #[test]
    fn restart_guard_rejects_stale_local_execution_progress() {
        let recent_slot = recent_checkpoint_slot(1);
        let (_temp, consensus) = consensus_store_at_slot(recent_slot);
        let stale_timestamp = current_unix_timestamp()
            .saturating_sub(recent_checkpoint_max_age_secs())
            .saturating_sub(1);
        let sync_head = SyncHead {
            block_number: 24_000_000,
            block_hash: B256::repeat_byte(0x62),
            timestamp: stale_timestamp,
        };

        let staleness = local_execution_progress_staleness(Some(sync_head), &consensus).unwrap();

        assert_eq!(staleness.block_number, sync_head.block_number);
        assert_eq!(staleness.timestamp, stale_timestamp);
        assert!(staleness.age_secs > staleness.max_age_secs);
    }

    #[test]
    fn archived_consensus_state_allows_fresh_checkpoint_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let old_checkpoint = checkpoint_at_slot(recent_checkpoint_slot(1));
        let _old_store = ConsensusStore::open(temp.path(), Some(&old_checkpoint)).unwrap();
        let state_path = consensus_state_path(temp.path());

        let original_bytes = fs::read(&state_path).unwrap();
        let archive_path = archive_consensus_state(temp.path(), "test-refresh")
            .unwrap()
            .unwrap();

        assert!(!state_path.exists());
        assert_eq!(fs::read(&archive_path).unwrap(), original_bytes);
        assert_eq!(archive_path.file_name(), state_path.file_name());

        let new_slot = recent_checkpoint_slot(0);
        let new_root = B256::repeat_byte(0x43);
        let new_checkpoint = format!("{new_slot}@{new_root:#x}");
        let refreshed = ConsensusStore::open(temp.path(), Some(&new_checkpoint)).unwrap();

        let checkpoint = refreshed.checkpoint();
        assert_eq!(checkpoint.beacon_slot, Some(new_slot));
        assert_eq!(checkpoint.beacon_root, new_root);
    }

    #[test]
    fn restart_guard_uses_recent_contiguous_consensus_anchor_for_progress() {
        let recent_slot = recent_checkpoint_slot(1);
        let (_temp, consensus) = consensus_store_at_slot(recent_slot);
        consensus
            .append_anchors(vec![anchor_record(25_000_000, recent_slot)])
            .unwrap();
        let stale_timestamp = current_unix_timestamp()
            .saturating_sub(recent_checkpoint_max_age_secs())
            .saturating_sub(1);
        let sync_head = SyncHead {
            block_number: 24_000_000,
            block_hash: B256::repeat_byte(0x63),
            timestamp: stale_timestamp,
        };

        assert!(local_execution_progress_staleness(Some(sync_head), &consensus).is_none());
    }

    #[test]
    fn startup_network_head_uses_light_client_head_when_coverage_is_empty() {
        let finalized = execution_anchor(25_424_100, 14_660_000);
        let optimistic = execution_anchor(25_424_288, 14_660_188);
        let anchors = ChainAnchors {
            indexed_head: None,
            finalized_head: Some(finalized),
            optimistic_head: Some(optimistic),
        };
        let coverage = AnchorCoverage {
            floor: None,
            ceiling: None,
            count: 0,
            gap_count: 0,
        };

        let head = consensus_execution_head_anchor(anchors, coverage).unwrap();

        assert_eq!(head, optimistic);
        let network_head = consensus_anchor_network_head(head);
        assert_eq!(network_head.number, optimistic.block_number);
        assert_eq!(network_head.hash, optimistic.block_hash);
    }

    #[test]
    fn initial_sync_status_does_not_treat_resume_block_as_network_target() {
        let status = initial_sync_status(
            83_714,
            &logex_types::ChainAnchors::default(),
            None,
            None,
            false,
            None,
        );

        assert_eq!(status.current_block, 83_714);
        assert_eq!(status.target_block, 0);
    }

    #[test]
    fn disable_historical_sync_initializes_fresh_data_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            ..PartitionManagerConfig::default()
        })
        .unwrap();

        let mode = resolve_historical_sync_mode(tmp.path(), &storage, false, true).unwrap();

        assert_eq!(mode, HistoricalSyncMode::Disabled);
        let state = read_sync_mode_state(tmp.path()).unwrap().unwrap();
        assert!(state.historical_sync_disabled);
    }

    #[test]
    fn disable_historical_sync_rejects_existing_default_data_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let mut storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            ..PartitionManagerConfig::default()
        })
        .unwrap();
        storage
            .record_sync_head(
                12_345,
                alloy_primitives::B256::repeat_byte(0x12),
                1_700_000_000,
            )
            .unwrap();

        let error = resolve_historical_sync_mode(tmp.path(), &storage, false, true).unwrap_err();

        assert!(error.contains("--disable-historical-sync can only be used with a fresh"));
    }

    #[test]
    fn disabling_historical_sync_can_resume_when_marker_exists() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            ..PartitionManagerConfig::default()
        })
        .unwrap();
        write_sync_mode_state(
            tmp.path(),
            &SyncModeState {
                historical_sync_disabled: true,
            },
        )
        .unwrap();

        let mode = resolve_historical_sync_mode(tmp.path(), &storage, false, true).unwrap();

        assert_eq!(mode, HistoricalSyncMode::Disabled);
    }

    #[test]
    fn omitted_disable_flag_converts_disabled_directory_to_historical_sync() {
        let tmp = tempfile::tempdir().unwrap();
        let storage = PartitionManager::open(PartitionManagerConfig {
            data_dir: tmp.path().to_path_buf(),
            ..PartitionManagerConfig::default()
        })
        .unwrap();
        write_sync_mode_state(
            tmp.path(),
            &SyncModeState {
                historical_sync_disabled: true,
            },
        )
        .unwrap();

        let mode = resolve_historical_sync_mode(tmp.path(), &storage, false, false).unwrap();

        assert_eq!(mode, HistoricalSyncMode::Enabled);
        assert!(read_sync_mode_state(tmp.path()).unwrap().is_none());
    }
}
