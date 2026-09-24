//! Lazily owned execution transport and read-only retained consensus trust.
use std::{
    net::IpAddr,
    path::PathBuf,
    sync::{Arc, OnceLock},
    time::Duration,
};

use alloy_consensus::Header;
use alloy_primitives::B256;
use eyre::{Result, WrapErr, ensure};
use logex_cl::{ConsensusStateError, ConsensusStore};
use logex_sync::{
    p2p::{
        peer_manager::{PeerManager, PeerManagerConfig, SourcedBlockBody, SourcedReceiptSet},
        persistence::{
            discovery_secret_path, known_peers_path, load_known_peers, load_or_create_secret_key,
        },
    },
    repair::{RepairConsensusProvider, RepairSource},
};
use reth_network_peers::PeerId;
use tokio::{sync::watch, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use super::super::{
    RUNTIME_FAILURE_CLEANUP_GRACE, RuntimeFailureWatchdog, detect_local_p2p_addresses,
    select_p2p_address, start_runtime_failure_watchdog, startup_network_head,
};

#[derive(Clone, Debug)]
pub(crate) struct RepairNetworkOptions {
    pub discovery_port: u16,
    pub p2p_port: u16,
    pub max_peers: usize,
    pub nat: String,
    pub p2p_bind_ip: Option<IpAddr>,
    pub execution_bootnodes: Vec<String>,
    pub execution_discv5_port: u16,
}

pub(super) struct RetainedConsensus {
    root: PathBuf,
    store: Arc<OnceLock<ConsensusStore>>,
}

impl RetainedConsensus {
    pub(super) fn new(root: PathBuf) -> Self {
        Self {
            root,
            store: Arc::new(OnceLock::new()),
        }
    }

    pub(super) fn shared(&self) -> Arc<OnceLock<ConsensusStore>> {
        Arc::clone(&self.store)
    }
}

impl RepairConsensusProvider for RetainedConsensus {
    fn consensus(&mut self) -> Result<&ConsensusStore> {
        if self.store.get().is_none() {
            // No checkpoint argument: existing trust is validated, never
            // reconciled, initialized or silently replaced for maintenance.
            let store = ConsensusStore::open(&self.root, None).map_err(|error| {
                let context = match &error {
                    ConsensusStateError::MissingCheckpoint =>
                        "primary reconstruction requires retained verified consensus state; preserve this directory and restore its compatible consensus state, or resync in a fresh directory",
                    ConsensusStateError::StaleWeakSubjectivityCheckpoint { .. } =>
                        "primary reconstruction cannot use stale retained trust; preserve this directory and establish current trusted state through the normal consensus workflow before retrying",
                    _ => "cannot validate retained consensus state for primary reconstruction; preserve its files and resolve the reported state error before retrying",
                };
                eyre::Report::new(error).wrap_err(context)
            })?;
            // Only this provider initializes the shared cell. Readers (including
            // lazy networking) never create a competing trust store.
            ensure!(
                self.store.set(store).is_ok(),
                "retained consensus was initialized concurrently"
            );
        }
        Ok(self.store.get().expect("retained consensus initialized"))
    }
}

pub(super) struct LazyRepairSource {
    root: PathBuf,
    options: RepairNetworkOptions,
    consensus: Arc<OnceLock<ConsensusStore>>,
    failures: watch::Sender<Option<Arc<str>>>,
    peers: Option<PeerManager>,
    peer_failure: Option<watch::Receiver<Option<Arc<str>>>>,
    watcher: Option<JoinHandle<()>>,
    stop_watcher: CancellationToken,
    // Keep this guard through peer shutdown and watcher acknowledgement.
    watchdog: Option<RuntimeFailureWatchdog>,
    stopped: bool,
}

impl LazyRepairSource {
    pub(super) fn new(
        root: PathBuf,
        options: RepairNetworkOptions,
        consensus: Arc<OnceLock<ConsensusStore>>,
        failures: watch::Sender<Option<Arc<str>>>,
    ) -> Self {
        Self {
            root,
            options,
            consensus,
            failures,
            peers: None,
            peer_failure: None,
            watcher: None,
            stop_watcher: CancellationToken::new(),
            watchdog: None,
            stopped: false,
        }
    }

    async fn peers(&mut self) -> Result<&mut PeerManager> {
        ensure!(
            !self.stopped,
            "repair execution networking has already stopped"
        );
        if self.peers.is_none() {
            let consensus = self.consensus.get().ok_or_else(|| {
                eyre::eyre!("resolve retained consensus trust before requesting repair data")
            })?;
            let our_head = startup_network_head(None, Some(consensus));
            ensure!(
                our_head.number > 0,
                "retained consensus has no non-genesis execution anchor for repair networking; restore verified anchor coverage before retrying"
            );
            let candidates = detect_local_p2p_addresses().await;
            let address = select_p2p_address(
                &self.options.nat,
                self.options.p2p_bind_ip,
                self.options.p2p_port,
                candidates,
            )
            .await
            .map_err(|error| eyre::eyre!("select repair execution network address: {error}"))?;
            for warning in &address.warnings {
                tracing::warn!(%warning, "repair execution address selection");
            }
            let secret_key = load_or_create_secret_key(&discovery_secret_path(&self.root))
                .wrap_err("load repair execution discovery identity")?;
            let known_peers_path = known_peers_path(&self.root);
            let known_peers =
                load_known_peers(&known_peers_path).wrap_err("load repair execution peer cache")?;
            let mut peers = PeerManager::new(PeerManagerConfig {
                secret_key,
                listener_port: self.options.p2p_port,
                discovery_port: self.options.discovery_port,
                bind_ip: address.bind_ip,
                dial_families: address.dial_families,
                max_peers: self.options.max_peers,
                nat_resolver: address.nat,
                our_head,
                known_peers,
                known_peers_path,
                execution_bootnodes: self.options.execution_bootnodes.clone(),
                execution_discv5_port: self.options.execution_discv5_port,
            })
            .await
            .wrap_err("start repair execution networking")?;
            let receiver = peers.task_failure_receiver();
            let watchdog = match start_runtime_failure_watchdog(
                receiver.clone(),
                RUNTIME_FAILURE_CLEANUP_GRACE,
                || std::process::exit(1),
            ) {
                Ok(watchdog) => watchdog,
                Err(error) => {
                    // Creation failure must not leave any execution workers
                    // detached. PeerManager Drop also aborts on cancellation.
                    let cleanup = peers.shutdown().await;
                    return Err(eyre::eyre!(
                        "start repair network failure watchdog: {error}; network cleanup: {cleanup:?}"
                    ));
                }
            };
            let failures = self.failures.clone();
            let stop = self.stop_watcher.clone();
            let mut watched = receiver.clone();
            let watcher = tokio::spawn(async move {
                loop {
                    let failure = watched.borrow_and_update().clone();
                    if let Some(failure) = failure {
                        latch_failure(&failures, failure);
                        return;
                    }
                    tokio::select! {
                        _ = stop.cancelled() => return,
                        changed = watched.changed() => {
                            if changed.is_err() { return; }
                        }
                    }
                }
            });
            // No await separates worker creation from ownership publication.
            self.peers = Some(peers);
            self.peer_failure = Some(receiver);
            self.watcher = Some(watcher);
            self.watchdog = Some(watchdog);
        }
        if let Some(failure) = self
            .peer_failure
            .as_ref()
            .and_then(|rx| rx.borrow().clone())
        {
            latch_failure(&self.failures, Arc::clone(&failure));
            return Err(eyre::eyre!("repair execution worker failed: {failure}"));
        }
        Ok(self.peers.as_mut().expect("repair peers initialized"))
    }

    pub(super) async fn shutdown(&mut self) -> Result<()> {
        self.stopped = true;
        let mut errors = Vec::new();
        if let Some(peers) = &mut self.peers
            && let Err(error) = peers.shutdown().await
        {
            errors.push(format!("repair execution cleanup failed: {error:#}"));
        }
        // Inspect synchronously as well: stopping the watcher must not hide a
        // first failure that was published just before shutdown completed.
        if let Some(failure) = self
            .peer_failure
            .as_ref()
            .and_then(|rx| rx.borrow().clone())
        {
            latch_failure(&self.failures, Arc::clone(&failure));
            errors.insert(0, format!("repair execution worker failed: {failure}"));
        }
        self.stop_watcher.cancel();
        if let Some(watcher) = self.watcher.as_mut()
            && let Err(error) = watcher.await
        {
            errors.push(format!("repair network failure watcher failed: {error}"));
        }
        self.watcher.take();
        self.peers.take();
        self.peer_failure.take();
        self.watchdog.take();
        if errors.is_empty() {
            Ok(())
        } else {
            latch_failure(&self.failures, Arc::from(errors[0].as_str()));
            Err(eyre::eyre!(errors.join("; ")))
        }
    }
}

impl Drop for LazyRepairSource {
    fn drop(&mut self) {
        self.stop_watcher.cancel();
        if let Some(watcher) = &self.watcher {
            watcher.abort();
        }
        // PeerManager owns the remaining abort fallback; normal completion
        // must call shutdown so all workers acknowledge before runtime teardown.
    }
}

fn latch_failure(sender: &watch::Sender<Option<Arc<str>>>, failure: Arc<str>) {
    sender.send_if_modified(|current| {
        if current.is_some() {
            false
        } else {
            *current = Some(failure);
            true
        }
    });
}

impl RepairSource for LazyRepairSource {
    async fn headers(
        &mut self,
        start: B256,
        count: u64,
        timeout: Duration,
        attempts: usize,
    ) -> Result<(PeerId, Vec<Header>)> {
        RepairSource::headers(self.peers().await?, start, count, timeout, attempts).await
    }

    async fn body(
        &mut self,
        hash: B256,
        number: u64,
        timeout: Duration,
        attempts: usize,
    ) -> Result<Vec<SourcedBlockBody>> {
        RepairSource::body(self.peers().await?, hash, number, timeout, attempts).await
    }

    async fn receipts(
        &mut self,
        header: &Header,
        preferred: PeerId,
        timeout: Duration,
        attempts: usize,
    ) -> Result<Vec<SourcedReceiptSet>> {
        RepairSource::receipts(self.peers().await?, header, preferred, timeout, attempts).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> RepairNetworkOptions {
        RepairNetworkOptions {
            discovery_port: 0,
            p2p_port: 0,
            max_peers: 1,
            nat: "none".to_owned(),
            p2p_bind_ip: None,
            execution_bootnodes: Vec::new(),
            execution_discv5_port: 0,
        }
    }

    #[test]
    fn missing_retained_trust_does_not_initialize_state() {
        let root = tempfile::tempdir().unwrap();
        let mut provider = RetainedConsensus::new(root.path().to_owned());
        assert!(provider.shared().get().is_none());
        let error = provider.consensus().unwrap_err();
        assert!(format!("{error:#}").contains("retained verified consensus state"));
        assert!(provider.shared().get().is_none());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn unresolved_trust_and_unused_shutdown_never_initialize_network_artifacts() {
        let root = tempfile::tempdir().unwrap();
        let provider = RetainedConsensus::new(root.path().to_owned());
        let (failures, receiver) = watch::channel(None);
        let mut source = LazyRepairSource::new(
            root.path().to_owned(),
            options(),
            provider.shared(),
            failures,
        );
        let error = source
            .headers(B256::ZERO, 1, Duration::from_millis(1), 1)
            .await
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("resolve retained consensus trust")
        );
        assert!(source.peers.is_none());
        assert!(source.watcher.is_none());
        assert!(source.watchdog.is_none());
        source.shutdown().await.unwrap();
        source.shutdown().await.unwrap();
        assert!(receiver.borrow().is_none());
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn network_failure_latch_preserves_the_first_failure() {
        let (sender, receiver) = watch::channel(None);
        latch_failure(&sender, Arc::from("first failure"));
        latch_failure(&sender, Arc::from("cleanup failure"));
        assert_eq!(receiver.borrow().as_deref(), Some("first failure"));
    }
}
