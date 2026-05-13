use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy_primitives::U256;
use logex_cl::{
    ConsensusNetworkConfig, ConsensusStateError, ConsensusStore, MAINNET_CONSENSUS_CHAIN_SPEC,
    spawn_consensus_network,
};
use logex_server::{AppState, SubscriptionManager};
use logex_storage::{PartitionManager, PartitionManagerConfig, SyncHead};
use logex_sync::SyncConfig;
use logex_sync::engine::SyncEngine;
use logex_sync::p2p::{
    peer_manager::{PeerManager, PeerManagerConfig},
    persistence::{
        discovery_secret_path, known_peers_path, load_known_peers, load_or_create_secret_key,
        persist_known_peers,
    },
};
use logex_types::SyncStatus;
use reth_chainspec::{EthChainSpec, MAINNET};
use reth_discv4::NatResolver;
use reth_ethereum_forks::Head;

use crate::background::{log_task_exit, run_background_indexer};
use crate::checkpoint::resolve_checkpoint;

pub struct RunSyncOptions {
    pub pm_config: PartitionManagerConfig,
    pub checkpoint: Option<String>,
    pub checkpoint_sync_url: Option<String>,
    pub http_port: u16,
    pub grpc_port: u16,
    pub discovery_port: u16,
    pub p2p_port: u16,
    pub max_peers: usize,
    pub nat: String,
    pub cl_discovery_port: u16,
    pub cl_p2p_port: u16,
    pub cl_max_peers: usize,
    pub dashboard_enabled: bool,
    pub dashboard_password: Option<String>,
}

pub async fn run_sync(options: RunSyncOptions) {
    let RunSyncOptions {
        pm_config,
        checkpoint,
        checkpoint_sync_url,
        http_port,
        grpc_port,
        discovery_port,
        p2p_port,
        max_peers,
        nat,
        cl_discovery_port,
        cl_p2p_port,
        cl_max_peers,
        dashboard_enabled,
        dashboard_password,
    } = options;
    let nat = match nat.parse::<NatResolver>() {
        Ok(nat) => nat,
        Err(error) => {
            tracing::error!(%error, "invalid EL NAT resolver");
            std::process::exit(1);
        }
    };

    let data_dir = pm_config.data_dir.clone();
    let discovery_secret_file = discovery_secret_path(&data_dir);
    let known_peers_file = known_peers_path(&data_dir);
    let consensus_state_exists = data_dir.join("cl").join("consensus_state.json").exists();
    let checkpoint = if consensus_state_exists && checkpoint.is_none() {
        checkpoint
    } else {
        match resolve_checkpoint(checkpoint, checkpoint_sync_url.as_deref()).await {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                tracing::error!(%error, "failed to resolve weak-subjectivity checkpoint");
                std::process::exit(1);
            }
        }
    };

    let mut storage = match PartitionManager::open(pm_config) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "failed to open storage");
            std::process::exit(1);
        }
    };

    let sync_head = storage.sync_head();
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

    let consensus = match maybe_open_consensus_store(&data_dir, &storage, checkpoint.as_deref()) {
        Ok(store) => store.map(Arc::new),
        Err(ConsensusStateError::MissingCheckpoint) => {
            tracing::error!(
                data_dir = %data_dir.display(),
                "fresh data directories now require --checkpoint <root-or-descriptor> to start canonical sync"
            );
            std::process::exit(1);
        }
        Err(error) => {
            tracing::error!(%error, "failed to initialize consensus state");
            std::process::exit(1);
        }
    };

    if let Some(consensus) = consensus.as_ref() {
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

    let storage_anchors = storage.chain_anchors();
    let historical_floor = storage.historical_floor();
    let historical_anchor = storage.historical_anchor();
    let state = Arc::new(AppState::new(
        storage,
        Some(SubscriptionManager::new()),
        initial_sync_status(
            resume_block,
            &storage_anchors,
            historical_floor,
            historical_anchor,
            consensus.as_deref(),
        ),
    ));

    let known_peers = match load_known_peers(&known_peers_file) {
        Ok(peers) => peers,
        Err(e) => {
            tracing::warn!(
                error = %e,
                path = %known_peers_file.display(),
                "failed to load known peers, starting with an empty peer cache"
            );
            Vec::new()
        }
    };
    tracing::info!(
        peers = known_peers.len(),
        path = %known_peers_file.display(),
        "loaded known peers"
    );

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

    let consensus_network_handle = consensus.as_ref().map(|consensus| {
        spawn_consensus_network(
            ConsensusNetworkConfig {
                data_dir: data_dir.clone(),
                checkpoint: consensus.checkpoint(),
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
        Some(Ok(handle)) => Some(handle),
        Some(Err(error)) => {
            tracing::error!(%error, "failed to start consensus network");
            std::process::exit(1);
        }
        None => None,
    };

    let http_addr: SocketAddr = ([0, 0, 0, 0], http_port).into();
    let http_state = Arc::clone(&state);
    let http_shutdown = shutdown_rx.clone();
    let http_handle = tokio::spawn(async move {
        tracing::info!(%http_addr, "HTTP server starting");
        let http_config = logex_server::HttpServerConfig {
            dashboard_enabled,
            dashboard_password,
        };
        if let Err(e) =
            logex_server::serve_with_config(http_state, http_addr, http_shutdown, http_config).await
        {
            tracing::error!(error = %e, "HTTP server error");
        }
    });

    let grpc_addr: SocketAddr = ([0, 0, 0, 0], grpc_port).into();
    let grpc_state = Arc::clone(&state);
    let grpc_shutdown = shutdown_rx.clone();
    let grpc_handle = tokio::spawn(async move {
        tracing::info!(%grpc_addr, "gRPC server starting");
        if let Err(e) = logex_server::grpc::serve_grpc(grpc_state, grpc_addr, grpc_shutdown).await {
            tracing::error!(error = %e, "gRPC server error");
        }
    });

    let index_state = Arc::clone(&state);
    let index_shutdown = shutdown_rx.clone();
    let index_handle = tokio::spawn(run_background_indexer(index_state, index_shutdown));

    tracing::info!(
        http = %format!("http://{http_addr}"),
        grpc = %format!("http://{grpc_addr}"),
        "query endpoints ready"
    );

    let our_head = startup_network_head(sync_head, consensus.as_deref());
    let peers = match PeerManager::new(PeerManagerConfig {
        secret_key,
        listener_port: p2p_port,
        discovery_port,
        max_peers,
        nat_resolver: nat,
        our_head,
        known_peers,
        known_peers_path: known_peers_file.clone(),
    })
    .await
    {
        Ok(peers) => peers,
        Err(e) => {
            tracing::error!(error = %e, "failed to start p2p networking");
            std::process::exit(1);
        }
    };

    let sync_config = SyncConfig {
        max_peers,
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

    let engine_result = {
        let mut engine_run = pin!(engine.run());
        tokio::select! {
            res = &mut engine_run => res,
            signal = wait_for_shutdown_signal() => {
                tracing::info!(signal, "shutdown requested, stopping node gracefully");
                let _ = shutdown_tx.send(true);
                match tokio::time::timeout(Duration::from_secs(15), &mut engine_run).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::warn!(
                            "sync engine did not stop within shutdown timeout, closing network tasks"
                        );
                        Ok(())
                    }
                }
            }
        }
    };

    if let Err(e) = engine_result {
        tracing::error!(error = %e, "sync engine error");
    }

    engine.shutdown().await;

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
    log_task_exit("HTTP server", http_handle).await;
    log_task_exit("gRPC server", grpc_handle).await;
    log_task_exit("background indexer", index_handle).await;
    if let Some(handle) = consensus_network_handle {
        log_task_exit("consensus network", handle).await;
    }
    tracing::info!("shutting down");
}

fn initial_sync_status(
    resume_block: u64,
    storage_anchors: &logex_types::ChainAnchors,
    historical_floor: Option<logex_types::ExecutionBlockMarker>,
    historical_anchor: Option<logex_types::ExecutionBlockMarker>,
    consensus: Option<&ConsensusStore>,
) -> SyncStatus {
    let mut status = SyncStatus {
        current_block: resume_block,
        target_block: 0,
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
                .or_else(|| consensus.map(consensus_checkpoint_network_head))
                .unwrap_or_else(genesis_network_head)
        }
        None => consensus
            .and_then(|consensus| consensus.anchor_coverage().ceiling)
            .map(consensus_anchor_network_head)
            .or_else(|| consensus.map(consensus_checkpoint_network_head))
            .unwrap_or_else(genesis_network_head),
    }
}

fn consensus_anchor_network_head(anchor: logex_types::ExecutionAnchor) -> Head {
    network_head(
        anchor.block_number,
        anchor.block_hash,
        MAINNET_CONSENSUS_CHAIN_SPEC
            .genesis_time
            .saturating_add(anchor.beacon_slot.saturating_mul(12)),
    )
}

fn consensus_checkpoint_network_head(consensus: &ConsensusStore) -> Head {
    let timestamp = consensus
        .checkpoint()
        .beacon_slot
        .map(consensus_slot_timestamp)
        .unwrap_or_else(current_unix_timestamp);
    network_head(0, MAINNET.genesis_hash(), timestamp)
}

fn consensus_slot_timestamp(slot: u64) -> u64 {
    MAINNET_CONSENSUS_CHAIN_SPEC
        .genesis_time
        .saturating_add(slot.saturating_mul(12))
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
    let state_path = data_dir.join("cl").join("consensus_state.json");
    if state_path.exists() || checkpoint.is_some() {
        return ConsensusStore::open(data_dir, checkpoint).map(Some);
    }

    if storage.sync_head().is_none() && storage.total_rows() == 0 {
        return Err(ConsensusStateError::MissingCheckpoint);
    }

    Ok(None)
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

    #[test]
    fn initial_sync_status_does_not_treat_resume_block_as_network_target() {
        let status = initial_sync_status(
            83_714,
            &logex_types::ChainAnchors::default(),
            None,
            None,
            None,
        );

        assert_eq!(status.current_block, 83_714);
        assert_eq!(status.target_block, 0);
    }
}
