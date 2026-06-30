use std::collections::BTreeSet;
use std::fs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, UdpSocket};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use alloy_primitives::U256;
use logex_cl::{
    AnchorCoverage, ConsensusDialAddressFamilies, ConsensusNetworkConfig, ConsensusStateError,
    ConsensusStore, MAINNET_CONSENSUS_CHAIN_SPEC, spawn_consensus_network,
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
use logex_types::{ChainAnchors, ExecutionAnchor, SyncStatus};
use reth_chainspec::{EthChainSpec, MAINNET};
use reth_discv4::NatResolver;
use reth_ethereum_forks::Head;
use serde::{Deserialize, Serialize};

use crate::background::{log_task_exit, run_background_indexer};
use crate::checkpoint::{RECENT_CHECKPOINT_MAX_FINALIZED_EPOCH_LAG, resolve_checkpoint};

const SYNC_ENGINE_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(120);
const LOW_DISK_SPACE_POLL_INTERVAL: Duration = Duration::from_secs(10);
const LOW_DISK_SPACE_MIN_FREE_BYTES: u64 = 10 * 1024 * 1024 * 1024;
const SYNC_MODE_FILE_NAME: &str = "sync-mode.json";
const MAINNET_SECONDS_PER_SLOT: u64 = 12;
const MAINNET_SLOTS_PER_EPOCH: u64 = 32;
const IPV4_ROUTE_PROBE: (Ipv4Addr, u16) = (Ipv4Addr::new(1, 1, 1, 1), 80);
const IPV6_ROUTE_PROBE: (Ipv6Addr, u16) = (
    Ipv6Addr::new(0x2606, 0x4700, 0x4700, 0, 0, 0, 0, 0x1111),
    80,
);

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

#[derive(Debug, Deserialize, Serialize)]
struct SyncModeState {
    historical_sync_disabled: bool,
}

pub struct RunSyncOptions {
    pub pm_config: PartitionManagerConfig,
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

pub async fn run_sync(options: RunSyncOptions) {
    let RunSyncOptions {
        pm_config,
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
    let mut p2p_address = match select_p2p_address(&nat, p2p_bind_ip, p2p_port).await {
        Ok(selection) => selection,
        Err(error) => {
            tracing::error!(%error, "invalid EL NAT resolver");
            std::process::exit(1);
        }
    };
    add_runtime_p2p_warnings(&mut p2p_address, &execution_bootnodes);
    let consensus_p2p_address =
        select_consensus_p2p_address(&p2p_address, detect_local_p2p_addresses());
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
        if let Some(staleness) = recent_consensus_state_staleness(consensus) {
            tracing::error!(
                trusted_slot = staleness.trusted_slot,
                trusted_epoch = staleness.trusted_epoch,
                current_epoch = staleness.current_epoch,
                max_epochs = staleness.max_epochs,
                "persisted consensus state is too stale; start from a fresh recent checkpoint"
            );
            std::process::exit(1);
        }
        if let Some(staleness) = local_execution_progress_staleness(sync_head, consensus) {
            tracing::error!(
                block_number = staleness.block_number,
                timestamp = staleness.timestamp,
                age_secs = staleness.age_secs,
                max_age_secs = staleness.max_age_secs,
                "local execution progress is too stale; start from a fresh recent checkpoint in a fresh data directory"
            );
            std::process::exit(1);
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
        Some(Ok(handle)) => Some(handle),
        Some(Err(error)) => {
            tracing::error!(%error, "failed to start consensus network");
            std::process::exit(1);
        }
        None => None,
    };

    let http_addr = SocketAddr::new(http_host, http_port);
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

    let grpc_addr = SocketAddr::new(grpc_host, grpc_port);
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

    let engine_result = {
        let mut engine_run = pin!(engine.run());
        tokio::select! {
            res = &mut engine_run => res,
            signal = wait_for_shutdown_signal() => {
                tracing::info!(signal, "shutdown requested, stopping node gracefully");
                let _ = shutdown_tx.send(true);
                match tokio::time::timeout(SYNC_ENGINE_SHUTDOWN_TIMEOUT, &mut engine_run).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::warn!(
                            ?SYNC_ENGINE_SHUTDOWN_TIMEOUT,
                            "sync engine did not stop within shutdown timeout, closing network tasks"
                        );
                        Ok(())
                    }
                }
            },
            low_disk = wait_for_low_disk_space(data_dir.clone()) => {
                tracing::error!(
                    path = %low_disk.path.display(),
                    free_bytes = low_disk.free_bytes,
                    min_free_bytes = low_disk.min_free_bytes,
                    "disk space below safety threshold, stopping node gracefully"
                );
                mark_sync_stopped_for_low_disk(&state);
                let _ = shutdown_tx.send(true);
                match tokio::time::timeout(SYNC_ENGINE_SHUTDOWN_TIMEOUT, &mut engine_run).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::warn!(
                            ?SYNC_ENGINE_SHUTDOWN_TIMEOUT,
                            "sync engine did not stop within low-disk shutdown timeout, closing network tasks"
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

async fn select_p2p_address(
    nat: &str,
    p2p_bind_ip: Option<IpAddr>,
    p2p_port: u16,
) -> Result<P2pAddressSelection, String> {
    let parsed = nat
        .parse::<NatResolver>()
        .map_err(|error| error.to_string())?;
    if parsed == NatResolver::Any {
        let candidates = detect_local_p2p_addresses();
        return Ok(choose_auto_p2p_address(candidates, p2p_bind_ip));
    }

    let resolved = resolve_startup_nat(parsed).await;
    let mut warnings = Vec::new();
    Ok(choose_explicit_p2p_address(
        resolved,
        p2p_bind_ip,
        p2p_port,
        detect_local_p2p_addresses(),
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
            "public IPv4 and IPv6 were both detected; IPv4 is advertised while IPv6 outbound candidates are also accepted"
                .to_owned(),
        );
    } else if p2p_bind_ip.is_none() && public_ipv6.is_some() && candidates.ipv4.is_some() {
        warnings.push(
            "public IPv6 and outbound IPv4 were detected; IPv6 is advertised while IPv4 outbound candidates are also accepted"
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
            "no locally owned public IPv4 or IPv6 address was detected; using outbound-only P2P without advertising a public address"
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

fn detect_local_p2p_addresses() -> LocalP2pAddressCandidates {
    LocalP2pAddressCandidates {
        ipv4: default_route_ipv4(),
        ipv6: default_route_ipv6(),
    }
}

fn default_route_ipv4() -> Option<Ipv4Addr> {
    let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect(IPV4_ROUTE_PROBE).ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(ip) => Some(ip),
        IpAddr::V6(_) => None,
    }
}

fn default_route_ipv6() -> Option<Ipv6Addr> {
    let socket = UdpSocket::bind((Ipv6Addr::UNSPECIFIED, 0)).ok()?;
    socket.connect(IPV6_ROUTE_PROBE).ok()?;
    match socket.local_addr().ok()?.ip() {
        IpAddr::V4(_) => None,
        IpAddr::V6(ip) => Some(ip),
    }
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
            "strict IPv6-only execution sync depends on public IPv6 EL peers; public discovery can be sparse, so configure --execution-bootnode with IPv6 enode:// or enr: records if EL peers stay at zero"
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

fn sync_mode_state_path(data_dir: &Path) -> PathBuf {
    data_dir.join(SYNC_MODE_FILE_NAME)
}

fn read_sync_mode_state(data_dir: &Path) -> Result<Option<SyncModeState>, String> {
    let path = sync_mode_state_path(data_dir);
    match fs::read_to_string(&path) {
        Ok(contents) => serde_json::from_str(&contents)
            .map(Some)
            .map_err(|error| format!("failed to parse {}: {error}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(format!("failed to read {}: {error}", path.display())),
    }
}

fn write_sync_mode_state(data_dir: &Path, state: &SyncModeState) -> Result<(), String> {
    fs::create_dir_all(data_dir)
        .map_err(|error| format!("failed to create {}: {error}", data_dir.display()))?;
    let path = sync_mode_state_path(data_dir);
    let contents = serde_json::to_vec_pretty(state)
        .map_err(|error| format!("failed to encode {}: {error}", path.display()))?;
    fs::write(&path, contents)
        .map_err(|error| format!("failed to write {}: {error}", path.display()))
}

fn remove_sync_mode_state(data_dir: &Path) -> Result<(), String> {
    let path = sync_mode_state_path(data_dir);
    match fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!("failed to remove {}: {error}", path.display())),
    }
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
    let state_path = data_dir.join("cl").join("consensus_state.json");
    if state_path.exists() || checkpoint.is_some() {
        return ConsensusStore::open(data_dir, checkpoint).map(Some);
    }

    if storage.sync_head().is_none() && storage.total_rows() == 0 {
        return Err(ConsensusStateError::MissingCheckpoint);
    }

    Ok(None)
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

#[derive(Debug)]
struct LowDiskSpace {
    path: PathBuf,
    free_bytes: u64,
    min_free_bytes: u64,
}

async fn wait_for_low_disk_space(path: PathBuf) -> LowDiskSpace {
    let mut interval = tokio::time::interval(LOW_DISK_SPACE_POLL_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;
        for probe_path in disk_space_probe_paths(&path) {
            match free_space_bytes(&probe_path) {
                Ok(free_bytes) if disk_space_is_low(free_bytes, LOW_DISK_SPACE_MIN_FREE_BYTES) => {
                    return LowDiskSpace {
                        path: probe_path,
                        free_bytes,
                        min_free_bytes: LOW_DISK_SPACE_MIN_FREE_BYTES,
                    };
                }
                Ok(_) => {}
                Err(error) => {
                    tracing::warn!(
                        path = %probe_path.display(),
                        %error,
                        "failed to check data directory free space"
                    );
                }
            }
        }
    }
}

fn disk_space_probe_paths(data_dir: &Path) -> Vec<PathBuf> {
    let mut probes = BTreeSet::new();
    insert_disk_space_probe_path(&mut probes, data_dir.to_path_buf());
    insert_disk_space_probe_path(&mut probes, data_dir.join("segments"));

    probes.into_iter().collect()
}

fn insert_disk_space_probe_path(probes: &mut BTreeSet<PathBuf>, path: PathBuf) {
    let path = path.canonicalize().unwrap_or(path);
    probes.insert(path);
}

fn disk_space_is_low(free_bytes: u64, min_free_bytes: u64) -> bool {
    free_bytes < min_free_bytes
}

fn mark_sync_stopped_for_low_disk(state: &AppState) {
    let mut status = state
        .sync_status
        .lock()
        .expect("sync status mutex poisoned");
    status.syncing = false;
    status.eta_seconds = None;
    status.historical_eta_seconds = None;
    status.node_state = logex_types::NodeState::Disconnected;
}

#[cfg(unix)]
fn free_space_bytes(path: &Path) -> std::io::Result<u64> {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let path = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
        std::io::Error::new(std::io::ErrorKind::InvalidInput, "path contains NUL byte")
    })?;

    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    let result = unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) };
    if result != 0 {
        return Err(std::io::Error::last_os_error());
    }

    let stat = unsafe { stat.assume_init() };
    let free = (stat.f_bavail as u128).saturating_mul(stat.f_frsize as u128);
    Ok(free.min(u64::MAX as u128) as u64)
}

#[cfg(not(unix))]
fn free_space_bytes(_path: &Path) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "free-space reporting is not implemented on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;

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
    fn disk_space_guard_trips_below_threshold() {
        assert!(disk_space_is_low(9, 10));
        assert!(!disk_space_is_low(10, 10));
    }

    #[cfg(unix)]
    #[test]
    fn disk_space_probe_paths_track_writable_storage_roots_not_sealed_segment_targets() {
        let base =
            std::env::temp_dir().join(format!("logex-node-disk-probes-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let data_dir = base.join("data");
        let segments_dir = data_dir.join("segments");
        let extra_segments_dir = base.join("extra").join("segments");
        let target_segment = extra_segments_dir.join("s_1");
        std::fs::create_dir_all(&segments_dir).unwrap();
        std::fs::create_dir_all(&target_segment).unwrap();
        std::os::unix::fs::symlink(&target_segment, segments_dir.join("s_1")).unwrap();

        let probes = disk_space_probe_paths(&data_dir);

        assert!(probes.contains(&data_dir.canonicalize().unwrap()));
        assert!(probes.contains(&segments_dir.canonicalize().unwrap()));
        assert!(!probes.contains(&extra_segments_dir.canonicalize().unwrap()));

        std::fs::remove_dir_all(&base).unwrap();
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
