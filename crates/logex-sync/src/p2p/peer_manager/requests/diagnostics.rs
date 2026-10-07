//! Bounded local exchange diagnostics, separate from protocol request IDs.
//!
//! A received response has not yet passed the caller's shape/root checks. Dropping
//! the local receiver does not cancel a request already admitted to the session.

use super::*;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_EXCHANGE: AtomicU64 = AtomicU64::new(1);

pub(super) struct Exchange {
    id: u64,
    peer: PeerId,
    scope: Scope,
    started: Instant,
    admitted: bool,
    finished: bool,
}

impl Exchange {
    pub(super) fn new(
        peer: PeerId,
        request: &PeerRequest<LogexNetworkPrimitives>,
        timeout: Duration,
    ) -> Self {
        let exchange = Self {
            id: NEXT_EXCHANGE.fetch_add(1, Ordering::Relaxed),
            peer,
            scope: Scope::from_request(request),
            started: Instant::now(),
            admitted: false,
            finished: false,
        };
        tracing::debug!(target: "logex_requests", exchange_id = exchange.id, %peer,
            scope = %exchange.scope, timeout_ms = timeout.as_millis() as u64,
            "execution exchange queued");
        exchange
    }

    pub(super) fn admitted(&mut self) {
        self.admitted = true;
        self.record("session_admitted");
    }

    pub(super) fn finish(&mut self, error: Option<&RequestAttempt>) {
        self.finished = true;
        if let Some(error) = error {
            tracing::info!(target: "logex_requests", exchange_id = self.id, peer = %self.peer,
                scope = %self.scope, session_admitted = self.admitted,
                elapsed_ms = self.started.elapsed().as_millis() as u64, ?error,
                "execution exchange failed");
        } else {
            self.record("response_received");
        }
    }

    fn record(&self, outcome: &'static str) {
        tracing::debug!(target: "logex_requests", exchange_id = self.id, peer = %self.peer,
            scope = %self.scope, session_admitted = self.admitted,
            elapsed_ms = self.started.elapsed().as_millis() as u64, outcome,
            "execution exchange lifecycle");
    }
}

impl Drop for Exchange {
    fn drop(&mut self) {
        if !self.finished {
            self.record("local_receiver_dropped");
        }
    }
}

// Hash the ordered list instead of logging or retaining an unbounded payload.
// Header requests retain their exact start/count/skip/direction. Payload requests
// carry a digest of all requested hashes and the ETH70 continuation offset.
enum Scope {
    Headers(GetBlockHeaders),
    Hashes {
        kind: &'static str,
        count: usize,
        digest: B256,
        first_receipt: Option<u64>,
    },
}

impl Scope {
    fn hashes(kind: &'static str, hashes: &[B256], first_receipt: Option<u64>) -> Self {
        let mut digest = blake3::Hasher::new();
        for hash in hashes {
            digest.update(hash.as_slice());
        }
        Self::Hashes {
            kind,
            count: hashes.len(),
            digest: B256::from(*digest.finalize().as_bytes()),
            first_receipt,
        }
    }

    fn from_request(request: &PeerRequest<LogexNetworkPrimitives>) -> Self {
        match request {
            PeerRequest::GetBlockHeaders { request, .. } => Self::Headers(*request),
            PeerRequest::GetBlockBodies { request, .. } => Self::hashes("bodies", &request.0, None),
            PeerRequest::GetReceipts { request, .. } => {
                Self::hashes("receipts68", &request.0, None)
            }
            PeerRequest::GetReceipts69 { request, .. } => {
                Self::hashes("receipts69", &request.0, None)
            }
            PeerRequest::GetReceipts70 { request, .. } => Self::hashes(
                "receipts70",
                &request.block_hashes,
                Some(request.first_block_receipt_index),
            ),
            PeerRequest::GetPooledTransactions { request, .. } => {
                Self::hashes("transactions", &request.0, None)
            }
            PeerRequest::GetNodeData { request, .. } => Self::hashes("node_data", &request.0, None),
        }
    }
}

impl fmt::Display for Scope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Headers(request) => write!(
                f,
                "headers start={:?} count={} skip={} direction={:?}",
                request.start_block, request.limit, request.skip, request.direction
            ),
            Self::Hashes {
                kind,
                count,
                digest,
                first_receipt,
            } => write!(
                f,
                "{kind} count={count} ordered_hashes_blake3={digest} first_receipt={first_receipt:?}"
            ),
        }
    }
}

#[cfg(test)]
mod tests;
