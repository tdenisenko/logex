//! Owned, finite handoffs to the live execution network. Disabled until attached.
use std::{collections::VecDeque, sync::Arc, time::Duration};

use alloy_consensus::Header;
use alloy_primitives::B256;
use eyre::{Result, bail, eyre};
use reth_network_peers::PeerId;
use serde::Serialize;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::{
    p2p::peer_manager::{
        BodyReceiptRequestAccounting, BodyReceiptRequestOutcome, PeerManager,
        ReceiptRequestContext, ReverseHeaderPagesRequestOutcome, SourcedBlockBody,
        SourcedBodyReceipts, SourcedReceiptSet,
    },
    repair::RepairSource,
};

/// One consumer, one queued request, and at most one running request plan. This
/// limits maintenance handoffs, not allocations in shared transport decoders.
/// The owning worker must apply complete receipt/header validation to results.
pub struct AuditNetworkClient {
    tx: mpsc::Sender<Request>,
    wake: Arc<Notify>,
    batch_blocks: usize,
    headers: VecDeque<Header>,
    payloads: VecDeque<(B256, SourcedBodyReceipts)>,
    receipt: Option<(B256, SourcedReceiptSet)>,
    metrics: AuditNetworkMetrics,
}

/// Normalized returned prefixes, not billed wire traffic. These omit transport,
/// request framing, discarded retry responses and out-of-order residuals. Receipt
/// blooms may have been reconstructed by the protocol decoder.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct AuditNetworkMetrics {
    pub header_handoffs: u64,
    pub payload_handoffs: u64,
    pub delivered_headers: u64,
    pub delivered_payload_blocks: u64,
    pub delivered_header_rlp_bytes: u64,
    pub delivered_body_rlp_bytes: u64,
    pub delivered_receipt_rlp_bytes: u64,
}

fn add_count(counter: &mut u64, amount: usize) -> Result<()> {
    *counter = counter
        .checked_add(u64::try_from(amount)?)
        .ok_or_else(|| eyre!("audit network counter overflow"))?;
    Ok(())
}

/// Owned by SyncEngine. Preparing/polling this service never waits for discovery
/// or an unfinished network request. Only dispatch while normal sync is healthy
/// and has no ready forward/historical work. Shutdown must join owned tasks.
pub struct AuditNetworkService {
    rx: mpsc::Receiver<Request>,
    wake: Arc<Notify>,
    pending: Option<Pending>,
    accounting_tx: mpsc::UnboundedSender<BodyReceiptRequestAccounting>,
    accounting_rx: mpsc::UnboundedReceiver<BodyReceiptRequestAccounting>,
}

struct Request {
    kind: RequestKind,
    attempts: usize,
    reply: oneshot::Sender<Result<Response>>,
}
enum RequestKind {
    Headers {
        start: B256,
        count: u64,
    },
    Payloads {
        blocks: Vec<ReceiptRequestContext>,
        required: u64,
    },
}
enum Response {
    Headers(PeerId, Vec<Header>),
    Payloads(Vec<SourcedBodyReceipts>),
}
enum Outcome {
    Headers(ReverseHeaderPagesRequestOutcome),
    Payloads(BodyReceiptRequestOutcome),
}
struct Pending {
    task: JoinHandle<Outcome>,
    reply: Option<oneshot::Sender<Result<Response>>>,
    owner: Option<u64>,
}
impl Drop for Pending {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct WakeOnDrop(Arc<Notify>);
impl Drop for WakeOnDrop {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

impl AuditNetworkService {
    /// No automatic job is started. The node must explicitly attach the service
    /// and give the single client to its owned maintenance worker.
    pub fn channel(batch_blocks: usize) -> Result<(AuditNetworkClient, Self)> {
        if !(1..=32).contains(&batch_blocks) {
            bail!("audit network batch must be 1..=32 blocks");
        }
        let (tx, rx) = mpsc::channel(1);
        let (accounting_tx, accounting_rx) = mpsc::unbounded_channel();
        let wake = Arc::new(Notify::new());
        Ok((
            AuditNetworkClient {
                tx,
                wake: Arc::clone(&wake),
                batch_blocks,
                headers: VecDeque::new(),
                payloads: VecDeque::new(),
                receipt: None,
                metrics: AuditNetworkMetrics::default(),
            },
            Self {
                rx,
                wake,
                pending: None,
                accounting_tx,
                accounting_rx,
            },
        ))
    }

    /// Wake idle sync waits when a handoff arrives or a request finishes. The
    /// notifier never grants priority over newly ready normal sync work.
    pub(crate) fn notifier(&self) -> Arc<Notify> {
        Arc::clone(&self.wake)
    }

    fn drain_accounting(&mut self, peers: &mut PeerManager) {
        // One bounded plan emits finite request accounting. Preserve owner/session
        // filtering instead of attributing stale audit completions to live work.
        let events = std::iter::from_fn(|| self.accounting_rx.try_recv().ok());
        peers.apply_body_receipt_request_accounting_events(events);
    }

    pub(crate) async fn poll(&mut self, peers: &mut PeerManager, allow_start: bool) {
        self.drain_accounting(peers);
        if let Some(pending) = &mut self.pending
            && pending
                .reply
                .as_ref()
                .is_none_or(oneshot::Sender::is_closed)
        {
            pending.task.abort();
            if let Some(owner) = pending.owner.take() {
                peers.retire_body_receipt_owner(owner);
            }
        }
        // Even cancellation only requests an abort here. A task currently doing
        // finite CPU work may not have observed it; do not join it in the live
        // loop until completion is reported. Shutdown owns the final join.
        if self.pending.as_ref().is_some_and(|p| p.task.is_finished()) {
            let mut pending = self.pending.take().expect("checked pending request");
            let outcome = (&mut pending.task).await;
            self.drain_accounting(peers);
            let result = match outcome {
                Ok(Outcome::Headers(outcome)) => peers
                    .complete_reverse_header_pages_request(outcome)
                    .and_then(|mut pages| {
                        if pages.len() != 1 {
                            bail!("audit header page is unavailable");
                        }
                        let (peer, headers) = pages.pop().expect("one page");
                        Ok(Response::Headers(peer, headers))
                    }),
                Ok(Outcome::Payloads(outcome)) => peers
                    .complete_bodies_and_receipts_request(outcome)
                    .and_then(|value| {
                        let completion =
                            value.ok_or_else(|| eyre!("audit payload batch is unavailable"))?;
                        // The caller validates the ordered prefix and retries any
                        // missing suffix. Out-of-order residuals never become data.
                        Ok(Response::Payloads(completion.blocks))
                    }),
                Err(error) => Err(eyre!("audit network task ended: {error}")),
            };
            if let Some(owner) = pending.owner.take() {
                peers.retire_body_receipt_owner(owner);
            }
            if let Some(reply) = pending.reply.take() {
                let _ = reply.send(result);
            }
        }
        if !allow_start || self.pending.is_some() {
            return;
        }
        let Ok(request) = self.rx.try_recv() else {
            return;
        };
        if request.reply.is_closed() {
            return;
        }
        let prepared = match request.kind {
            RequestKind::Headers { start, count } => peers
                .prepare_audit_headers(start, count, request.attempts)
                .map(|plan| {
                    plan.map(|plan| {
                        let wake = Arc::clone(&self.wake);
                        (
                            tokio::spawn(async move {
                                let _wake = WakeOnDrop(wake);
                                Outcome::Headers(plan.execute().await)
                            }),
                            None,
                        )
                    })
                }),
            RequestKind::Payloads { blocks, required } => peers
                .prepare_audit_payloads(blocks, required, request.attempts)
                .map(|plan| {
                    plan.map(|mut plan| {
                        let owner = peers.register_body_receipt_plan(
                            &mut plan,
                            self.accounting_tx.clone(),
                            true,
                        );
                        let wake = Arc::clone(&self.wake);
                        (
                            tokio::spawn(async move {
                                let _wake = WakeOnDrop(wake);
                                Outcome::Payloads(plan.execute().await)
                            }),
                            Some(owner),
                        )
                    })
                }),
        };
        match prepared {
            Ok(Some((task, owner))) => {
                self.pending = Some(Pending {
                    task,
                    reply: Some(request.reply),
                    owner,
                })
            }
            Ok(None) => {
                let _ = request
                    .reply
                    .send(Err(eyre!("no eligible audit network candidates")));
            }
            Err(error) => {
                let _ = request.reply.send(Err(error));
            }
        }
    }

    pub(crate) async fn shutdown(&mut self, peers: &mut PeerManager) {
        self.rx.close();
        while let Ok(request) = self.rx.try_recv() {
            let _ = request
                .reply
                .send(Err(eyre!("audit network service stopped")));
        }
        if let Some(mut pending) = self.pending.take() {
            pending.task.abort();
            let _ = (&mut pending.task).await;
            self.drain_accounting(peers);
            if let Some(owner) = pending.owner.take() {
                peers.retire_body_receipt_owner(owner);
            }
        }
        self.drain_accounting(peers);
    }
}

impl AuditNetworkClient {
    pub fn metrics(&self) -> AuditNetworkMetrics {
        self.metrics
    }

    async fn request(
        &mut self,
        kind: RequestKind,
        attempts: usize,
        timeout: Duration,
    ) -> Result<Response> {
        if timeout.is_zero() || !(1..=4).contains(&attempts) {
            bail!("invalid audit network request budget");
        }
        let _wake_on_cancel = WakeOnDrop(Arc::clone(&self.wake));
        let header_request = matches!(&kind, RequestKind::Headers { .. });
        let (tx, rx) = oneshot::channel();
        let result = tokio::time::timeout(timeout, async {
            self.tx
                .send(Request {
                    kind,
                    attempts,
                    reply: tx,
                })
                .await
                .map_err(|_| eyre!("audit network service unavailable"))?;
            add_count(
                if header_request {
                    &mut self.metrics.header_handoffs
                } else {
                    &mut self.metrics.payload_handoffs
                },
                1,
            )?;
            self.wake.notify_one();
            rx.await
                .map_err(|_| eyre!("audit network request owner ended"))?
        })
        .await
        .map_err(|_| eyre!("audit network handoff deadline exceeded"))?;
        // Waking also lets the engine notice a closed response after failures.
        self.wake.notify_one();
        result
    }
}

impl Drop for AuditNetworkClient {
    fn drop(&mut self) {
        self.wake.notify_one();
    }
}

impl RepairSource for AuditNetworkClient {
    async fn headers(
        &mut self,
        start: B256,
        count: u64,
        timeout: Duration,
        attempts: usize,
    ) -> Result<(PeerId, Vec<Header>)> {
        if !(1..=1024).contains(&count) {
            bail!("audit header page exceeds its allowance");
        }
        self.headers.clear();
        self.payloads.clear();
        self.receipt = None;
        let Response::Headers(peer, headers) = self
            .request(RequestKind::Headers { start, count }, attempts, timeout)
            .await?
        else {
            bail!("audit network response kind mismatch");
        };
        if headers.len() as u64 > count {
            bail!("audit network header response overflow");
        }
        self.metrics.delivered_headers = self
            .metrics
            .delivered_headers
            .checked_add(headers.len() as u64)
            .ok_or_else(|| eyre!("audit header counter overflow"))?;
        for header in &headers {
            add_count(
                &mut self.metrics.delivered_header_rlp_bytes,
                alloy_rlp::Encodable::length(header),
            )?;
        }
        self.headers.extend(headers.iter().cloned());
        Ok((peer, headers))
    }
    async fn body(
        &mut self,
        hash: B256,
        number: u64,
        timeout: Duration,
        attempts: usize,
    ) -> Result<Vec<SourcedBlockBody>> {
        if self.receipt.is_some() {
            bail!("previous audit receipt has not been consumed");
        }
        while self.headers.front().is_some_and(|h| h.number > number) {
            self.headers.pop_front();
        }
        let header = self
            .headers
            .front()
            .ok_or_else(|| eyre!("audit body has no requested header"))?;
        if header.number != number || header.hash_slow() != hash {
            bail!("audit body/header order mismatch");
        }
        if self.payloads.is_empty() {
            let selected: Vec<_> = self.headers.iter().take(self.batch_blocks).collect();
            let hashes: Vec<_> = selected.iter().map(|h| h.hash_slow()).collect();
            let blocks = selected
                .iter()
                .map(|h| ReceiptRequestContext::from_header(h))
                .collect();
            let Response::Payloads(payloads) = self
                .request(
                    RequestKind::Payloads {
                        blocks,
                        required: number,
                    },
                    attempts,
                    timeout,
                )
                .await?
            else {
                bail!("audit network response kind mismatch");
            };
            if payloads.is_empty() || payloads.len() > hashes.len() {
                bail!("audit payload response is empty or oversized");
            }
            add_count(&mut self.metrics.delivered_payload_blocks, payloads.len())?;
            for (body, receipts) in &payloads {
                add_count(
                    &mut self.metrics.delivered_body_rlp_bytes,
                    alloy_rlp::Encodable::length(&body.1),
                )?;
                add_count(
                    &mut self.metrics.delivered_receipt_rlp_bytes,
                    alloy_rlp::Encodable::length(&receipts.1),
                )?;
            }
            self.payloads.extend(hashes.into_iter().zip(payloads));
        }
        let (received, (body, receipts)) = self
            .payloads
            .pop_front()
            .ok_or_else(|| eyre!("audit body unavailable"))?;
        if received != hash {
            bail!("audit payload/header order mismatch");
        }
        self.receipt = Some((hash, receipts));
        self.headers.pop_front();
        Ok(vec![body])
    }
    async fn receipts(
        &mut self,
        header: &Header,
        _preferred: PeerId,
        _timeout: Duration,
        _attempts: usize,
    ) -> Result<Vec<SourcedReceiptSet>> {
        let (hash, receipts) = self
            .receipt
            .take()
            .ok_or_else(|| eyre!("audit receipts have no paired body"))?;
        if hash != header.hash_slow() {
            bail!("audit receipt/header identity mismatch");
        }
        Ok(vec![receipts])
    }
}

#[cfg(test)]
mod tests;
