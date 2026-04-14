use std::net::SocketAddr;
use std::pin::pin;
use std::sync::Arc;

use logex_server::{AppState, SubscriptionManager};
use logex_storage::{PartitionManager, PartitionManagerConfig, SyncHead};
use logex_sync::SyncConfig;
use logex_sync::engine::SyncEngine;
use logex_sync::p2p::{
    peer_manager::PeerManager,
    persistence::{
        discovery_secret_path, known_peers_path, load_known_peers, load_or_create_secret_key,
        persist_known_peers,
    },
};
use logex_types::SyncStatus;
use reth_chainspec::{EthChainSpec, MAINNET};
use reth_ethereum_forks::Head;

use crate::background::{log_task_exit, run_background_indexer};

pub async fn run_sync(
    pm_config: PartitionManagerConfig,
    http_port: u16,
    grpc_port: u16,
    discovery_port: u16,
    p2p_port: u16,
    max_peers: usize,
) {
    let data_dir = pm_config.data_dir.clone();
    let discovery_secret_file = discovery_secret_path(&data_dir);
    let known_peers_file = known_peers_path(&data_dir);

    let storage = match PartitionManager::open(pm_config) {
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

    let state = Arc::new(AppState {
        storage: Arc::new(tokio::sync::RwLock::new(storage)),
        subscriptions: Some(SubscriptionManager::new()),
        sync_status: Arc::new(std::sync::Mutex::new(initial_sync_status(resume_block))),
    });

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

    let http_addr: SocketAddr = ([0, 0, 0, 0], http_port).into();
    let http_state = Arc::clone(&state);
    let http_shutdown = shutdown_rx.clone();
    let http_handle = tokio::spawn(async move {
        tracing::info!(%http_addr, "HTTP server starting");
        if let Err(e) = logex_server::serve(http_state, http_addr, http_shutdown).await {
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

    let our_head = startup_network_head(sync_head);
    let peers = match PeerManager::new(
        secret_key,
        p2p_port,
        discovery_port,
        max_peers,
        our_head,
        known_peers,
        known_peers_file.clone(),
    )
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
        shutdown_rx.clone(),
    );

    let engine_result = {
        let mut engine_run = pin!(engine.run());
        tokio::select! {
            res = &mut engine_run => res,
            signal = wait_for_shutdown_signal() => {
                tracing::info!(signal, "shutdown requested, stopping node gracefully");
                let _ = shutdown_tx.send(true);
                engine_run.await
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
    tracing::info!("shutting down");
}

fn initial_sync_status(resume_block: u64) -> SyncStatus {
    SyncStatus {
        current_block: resume_block,
        target_block: 0,
        ..Default::default()
    }
}

fn startup_network_head(sync_head: Option<SyncHead>) -> Head {
    match sync_head {
        Some(head) if head.block_number == 0 || head.timestamp > 0 => Head {
            number: head.block_number,
            hash: head.block_hash,
            timestamp: if head.block_number == 0 {
                MAINNET.genesis().timestamp
            } else {
                head.timestamp
            },
            ..Default::default()
        },
        Some(head) => {
            tracing::warn!(
                block_number = head.block_number,
                "sync metadata is missing the block timestamp, starting network status from genesis until a new verified block updates it"
            );
            genesis_network_head()
        }
        None => genesis_network_head(),
    }
}

fn genesis_network_head() -> Head {
    Head {
        number: 0,
        hash: MAINNET.genesis_hash(),
        timestamp: MAINNET.genesis().timestamp,
        ..Default::default()
    }
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
        let status = initial_sync_status(83_714);

        assert_eq!(status.current_block, 83_714);
        assert_eq!(status.target_block, 0);
    }
}
