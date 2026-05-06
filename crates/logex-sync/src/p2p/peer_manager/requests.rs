use alloy_eips::BlockHashOrNumber;
use eyre::{Result, bail};
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{debug, trace, warn};

use super::*;

impl PeerManager {
    /// Request block headers starting at `start_block` for `count` blocks.
    pub async fn get_headers(
        &mut self,
        start_block: u64,
        count: u64,
    ) -> Result<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        let request = HeadersRequest::rising(start_block.into(), count);
        self.get_headers_from_peers(request, Some(start_block))
            .await
    }

    /// Request block headers ending at `start_block` in descending block order.
    pub async fn get_headers_reverse(
        &mut self,
        start: BlockHashOrNumber,
        count: u64,
    ) -> Result<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        let required_block = match start {
            BlockHashOrNumber::Number(block) => Some(block),
            BlockHashOrNumber::Hash(_) => None,
        };
        let request = HeadersRequest::falling(start, count);
        self.get_headers_from_peers(request, required_block).await
    }

    /// Request a single block header by hash or number.
    pub async fn get_header(
        &mut self,
        id: BlockHashOrNumber,
    ) -> Result<(
        PeerId,
        Option<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        let required_block = match id {
            BlockHashOrNumber::Number(block) => Some(block),
            BlockHashOrNumber::Hash(_) => None,
        };
        let (peer_id, mut headers) = self
            .get_headers_from_peers(HeadersRequest::one(id), required_block)
            .await?;
        Ok((peer_id, headers.pop()))
    }

    /// Request a single block header by hash.
    pub async fn get_header_by_hash(
        &mut self,
        hash: B256,
    ) -> Result<(
        PeerId,
        Option<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        self.get_header(BlockHashOrNumber::Hash(hash)).await
    }

    /// Request block bodies for the given block hashes.
    pub async fn get_bodies(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
    ) -> Result<Vec<SourcedBlockBody>> {
        self.get_bodies_prefer_peers(hashes, required_block, &[])
            .await
    }

    /// Request block bodies while trying the supplied proven peers before the
    /// regular rotating candidate set.
    pub async fn get_bodies_prefer_peers(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        preferred_peers: &[PeerId],
    ) -> Result<Vec<SourcedBlockBody>> {
        self.drain_events_now();
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let mut remaining_hashes = hashes.clone();
        let mut collected = Vec::with_capacity(hashes.len());
        let peer_ids = self
            .peer_ids_for_block_requests(Some(required_block), preferred_peers)
            .await;
        let mut dead_peers = HashSet::new();

        for peer_id in peer_ids {
            while !remaining_hashes.is_empty() {
                let request_hashes = remaining_hashes.clone();
                let bodies = match self.request_bodies(peer_id, request_hashes.clone()).await {
                    Ok(bodies) => bodies,
                    Err(error) => {
                        let should_drop = self.on_request_error(peer_id, &error);
                        debug!(peer = %peer_id, ?error, "block body request failed");
                        if should_drop {
                            dead_peers.insert(peer_id);
                        }
                        break;
                    }
                };

                match classify_response_progress(request_hashes.len(), bodies.len()) {
                    ResponseProgress::Complete => {
                        self.note_peer_success(peer_id);
                        self.advance_request_cursor();
                        collected.extend(bodies.into_iter().map(|body| (peer_id, body)));
                        self.remove_dead_peers(&dead_peers);
                        return Ok(collected);
                    }
                    ResponseProgress::Partial { returned } => {
                        self.on_partial_response(
                            peer_id,
                            "block bodies",
                            request_hashes.len(),
                            returned,
                        );
                        collected.extend(bodies.into_iter().map(|body| (peer_id, body)));
                        remaining_hashes = request_hashes[returned..].to_vec();
                    }
                    ResponseProgress::Empty => {
                        self.on_zero_progress_response(
                            peer_id,
                            "block bodies",
                            request_hashes.len(),
                        );
                        self.drop_unproductive_peer(peer_id, "block bodies", request_hashes.len());
                        break;
                    }
                    ResponseProgress::Overflow { returned } => {
                        self.on_invalid_response_length(
                            peer_id,
                            "block bodies",
                            request_hashes.len(),
                            returned,
                        );
                        dead_peers.insert(peer_id);
                        break;
                    }
                }
            }
        }

        self.remove_dead_peers(&dead_peers);
        if remaining_hashes.is_empty() {
            Ok(collected)
        } else {
            bail!("no peers available to handle block body request")
        }
    }

    async fn get_headers_from_peers(
        &mut self,
        request: HeadersRequest,
        required_block: Option<u64>,
    ) -> Result<(
        PeerId,
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
    )> {
        self.drain_events_now();

        let peer_ids = self.peer_ids_for_block_requests(required_block, &[]).await;
        let mut dead_peers = HashSet::new();
        let mut saw_empty_response = false;

        for peer_id in peer_ids {
            match self.request_headers(peer_id, request.clone()).await {
                Ok(headers) => {
                    if headers.len() > request.limit as usize {
                        self.on_invalid_response_length(
                            peer_id,
                            "headers",
                            request.limit as usize,
                            headers.len(),
                        );
                        dead_peers.insert(peer_id);
                        continue;
                    }
                    if headers.is_empty() && request.limit > 0 {
                        saw_empty_response = true;
                        self.on_zero_progress_response(peer_id, "headers", request.limit as usize);
                        continue;
                    }
                    self.note_peer_success(peer_id);
                    self.advance_request_cursor();
                    self.remove_dead_peers(&dead_peers);
                    return Ok((peer_id, headers));
                }
                Err(error) => {
                    let should_drop = self.on_request_error(peer_id, &error);
                    debug!(peer = %peer_id, ?error, "header request failed");
                    if should_drop {
                        dead_peers.insert(peer_id);
                    }
                }
            }
        }

        self.remove_dead_peers(&dead_peers);
        if saw_empty_response {
            return Ok((PeerId::ZERO, Vec::new()));
        }
        bail!("no peers available to handle header request")
    }

    /// Request receipts for the given block hashes.
    pub async fn get_receipts(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
    ) -> Result<(
        PeerId,
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    )> {
        self.get_receipts_inner(hashes, required_block, None, &[])
            .await
    }

    /// Request receipts and only return a peer response whose per-block receipt
    /// counts match the already fetched bodies. Peers that return receipt sets
    /// inconsistent with the bodies are disconnected and the request is retried
    /// against the next eligible peer.
    pub async fn get_receipts_matching_counts(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        expected_receipt_counts: &[usize],
    ) -> Result<(
        PeerId,
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    )> {
        self.get_receipts_matching_counts_prefer_peers(
            hashes,
            required_block,
            expected_receipt_counts,
            &[],
        )
        .await
    }

    /// Request receipts while trying body-proven peers before rotating through
    /// the rest of the eligible peer set.
    pub async fn get_receipts_matching_counts_prefer_peers(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        expected_receipt_counts: &[usize],
        preferred_peers: &[PeerId],
    ) -> Result<(
        PeerId,
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    )> {
        self.get_receipts_inner(
            hashes,
            required_block,
            Some(expected_receipt_counts),
            preferred_peers,
        )
        .await
    }

    async fn get_receipts_inner(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
        expected_receipt_counts: Option<&[usize]>,
        preferred_peers: &[PeerId],
    ) -> Result<(
        PeerId,
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
    )> {
        self.drain_events_now();
        if hashes.is_empty() {
            return Ok((PeerId::ZERO, Vec::new()));
        }
        if let Some(expected) = expected_receipt_counts
            && expected.len() != hashes.len()
        {
            bail!(
                "receipt count expectation length mismatch: expected {} entries for {} hashes",
                expected.len(),
                hashes.len()
            );
        }

        let peer_ids = self
            .peer_ids_for_receipt_requests(required_block, preferred_peers)
            .await;
        let mut dead_peers = HashSet::new();

        for peer_id in peer_ids {
            let version = match self.peers.get(&peer_id) {
                Some(peer) => {
                    trace!(
                        peer = %peer_id,
                        client_version = %peer.client_version,
                        version = ?peer.version,
                        serving = peer.is_serving,
                        latest_block = ?peer.remote_status.latest_block,
                        earliest_block = ?peer.remote_status.earliest_block,
                        required_block,
                        hashes = hashes.len(),
                        preferred = preferred_peers.contains(&peer_id),
                        "requesting receipts from execution peer"
                    );
                    peer.version
                }
                None => continue,
            };

            if version >= EthVersion::Eth70 {
                match self.request_receipts70(peer_id, hashes.clone()).await {
                    Ok(receipts) => {
                        if let Err(error) = validate_receipt_response_counts(
                            "receipts70",
                            hashes.len(),
                            &receipts,
                            expected_receipt_counts,
                        ) {
                            self.on_receipt_count_mismatch(peer_id, error);
                            dead_peers.insert(peer_id);
                            continue;
                        }
                        self.on_receipt_request_success(peer_id);
                        return Ok((peer_id, receipts));
                    }
                    Err(error) => {
                        let should_drop = self.on_request_error(peer_id, &error);
                        debug!(
                            peer = %peer_id,
                            ?error,
                            "eth/70 receipt request failed"
                        );
                        if should_drop {
                            dead_peers.insert(peer_id);
                        }
                        continue;
                    }
                }
            }

            let mut remaining_hashes = hashes.clone();
            let mut collected = Vec::with_capacity(hashes.len());

            while !remaining_hashes.is_empty() {
                let request_hashes = remaining_hashes.clone();
                let attempt = if version >= EthVersion::Eth69 {
                    self.request_receipts69(peer_id, request_hashes.clone())
                        .await
                } else {
                    self.request_receipts(peer_id, request_hashes.clone()).await
                };

                match attempt {
                    Ok(receipts) => {
                        match classify_response_progress(request_hashes.len(), receipts.len()) {
                            ResponseProgress::Complete => {
                                if let Err(error) = validate_receipt_response_counts(
                                    "receipts",
                                    request_hashes.len(),
                                    &receipts,
                                    expected_receipt_counts.map(|expected| {
                                        &expected[hashes.len() - remaining_hashes.len()..]
                                    }),
                                ) {
                                    self.on_receipt_count_mismatch(peer_id, error);
                                    dead_peers.insert(peer_id);
                                    break;
                                }
                                self.on_receipt_request_success(peer_id);
                                collected.extend(receipts);
                                return Ok((peer_id, collected));
                            }
                            ResponseProgress::Partial { returned } => {
                                if let Err(error) = validate_receipt_response_counts(
                                    "receipts",
                                    returned,
                                    &receipts,
                                    expected_receipt_counts.map(|expected| {
                                        let offset = hashes.len() - remaining_hashes.len();
                                        &expected[offset..offset + returned]
                                    }),
                                ) {
                                    self.on_receipt_count_mismatch(peer_id, error);
                                    dead_peers.insert(peer_id);
                                    break;
                                }
                                self.on_partial_response(
                                    peer_id,
                                    "receipts",
                                    request_hashes.len(),
                                    returned,
                                );
                                collected.extend(receipts);
                                remaining_hashes = request_hashes[returned..].to_vec();
                            }
                            ResponseProgress::Empty => {
                                self.on_zero_progress_response(
                                    peer_id,
                                    "receipts",
                                    request_hashes.len(),
                                );
                                self.drop_unproductive_peer(
                                    peer_id,
                                    "receipts",
                                    request_hashes.len(),
                                );
                                break;
                            }
                            ResponseProgress::Overflow { returned } => {
                                self.on_invalid_response_length(
                                    peer_id,
                                    "receipts",
                                    request_hashes.len(),
                                    returned,
                                );
                                dead_peers.insert(peer_id);
                                break;
                            }
                        }
                    }
                    Err(error) => {
                        let should_drop = self.on_request_error(peer_id, &error);
                        debug!(peer = %peer_id, ?error, "receipt request failed");
                        if should_drop {
                            dead_peers.insert(peer_id);
                        }
                        break;
                    }
                }
            }
        }

        self.remove_dead_peers(&dead_peers);
        bail!("no peers available to handle receipt request")
    }

    async fn peer_ids_for_receipt_requests(
        &mut self,
        required_block: u64,
        preferred_peers: &[PeerId],
    ) -> Vec<PeerId> {
        self.peer_ids_for_block_requests(Some(required_block), preferred_peers)
            .await
    }

    async fn peer_ids_for_block_requests(
        &mut self,
        required_block: Option<u64>,
        preferred_peers: &[PeerId],
    ) -> Vec<PeerId> {
        let mut peer_ids = prioritize_preferred_peer_ids(
            self.peer_ids_for_requests(required_block),
            preferred_peers,
        );
        if !peer_ids.is_empty() {
            return peer_ids;
        }

        for _ in 0..REQUEST_PEER_REFILL_ATTEMPTS {
            self.dial_pending_peers(self.max_peers);
            if !self.wait_for_activity(Duration::from_millis(250)).await {
                self.drain_events_now();
            }
            peer_ids = prioritize_preferred_peer_ids(
                self.peer_ids_for_requests(required_block),
                preferred_peers,
            );
            if !peer_ids.is_empty() {
                return peer_ids;
            }
        }

        peer_ids
    }

    pub(super) async fn request_headers(
        &self,
        peer_id: PeerId,
        request: HeadersRequest,
    ) -> std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockHeader>,
        RequestAttempt,
    > {
        self.request_with_channel(peer_id, &move |response| PeerRequest::GetBlockHeaders {
            request: GetBlockHeaders {
                start_block: request.start,
                limit: request.limit,
                skip: 0,
                direction: request.direction,
            },
            response,
        })
        .await
    }

    pub(super) async fn request_bodies(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<<LogexNetworkPrimitives as NetworkPrimitives>::BlockBody>,
        RequestAttempt,
    > {
        self.request_with_channel(peer_id, &move |response| PeerRequest::GetBlockBodies {
            request: GetBlockBodies(hashes.clone()),
            response,
        })
        .await
    }

    pub(super) async fn request_with_channel<T, W, MakeRequest>(
        &self,
        peer_id: PeerId,
        make_request: &MakeRequest,
    ) -> std::result::Result<T, RequestAttempt>
    where
        W: IntoResponseValue<T>,
        MakeRequest: Fn(
            oneshot::Sender<reth_network::p2p::error::RequestResult<W>>,
        ) -> PeerRequest<LogexNetworkPrimitives>,
    {
        let Some(peer) = self.peers.get(&peer_id) else {
            return Err(RequestAttempt::Disconnected);
        };

        let sender = peer.sender.clone();
        let (response_tx, response_rx) = oneshot::channel();
        sender
            .to_session_tx
            .send(make_request(response_tx))
            .await
            .map_err(|_| RequestAttempt::Disconnected)?;

        match timeout(REQUEST_TIMEOUT, response_rx).await {
            Ok(Ok(Ok(response))) => Ok(response.into_value()),
            Ok(Ok(Err(error))) => Err(RequestAttempt::Request(error)),
            Ok(Err(_)) => Err(RequestAttempt::Disconnected),
            Err(_) => Err(RequestAttempt::Request(
                reth_network::p2p::error::RequestError::Timeout,
            )),
        }
    }

    pub(super) async fn request_receipts(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        self.request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts {
            request: GetReceipts(hashes.clone()),
            response,
        })
        .await
    }

    pub(super) async fn request_receipts69(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        let receipts: Vec<Vec<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt>> = self
            .request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts69 {
                request: GetReceipts(hashes.clone()),
                response,
            })
            .await?;
        Ok(Receipts69(receipts).into_with_bloom().0)
    }

    pub(super) async fn request_receipts70(
        &self,
        peer_id: PeerId,
        hashes: Vec<B256>,
    ) -> std::result::Result<
        Vec<
            Vec<
                alloy_consensus::ReceiptWithBloom<
                    <LogexNetworkPrimitives as NetworkPrimitives>::Receipt,
                >,
            >,
        >,
        RequestAttempt,
    > {
        let mut merged = Vec::with_capacity(hashes.len());
        let mut next_block_index = 0usize;
        let mut first_block_receipt_index = 0u64;

        while next_block_index < hashes.len() {
            let request_hashes = hashes[next_block_index..].to_vec();
            let response: Receipts70<<LogexNetworkPrimitives as NetworkPrimitives>::Receipt> = self
                .request_with_channel(peer_id, &move |response| PeerRequest::GetReceipts70 {
                    request: GetReceipts70 {
                        first_block_receipt_index,
                        block_hashes: request_hashes.clone(),
                    },
                    response,
                })
                .await?;

            let (updated_block_index, updated_receipt_index) = merge_receipts70_response(
                &mut merged,
                next_block_index,
                first_block_receipt_index,
                response,
                hashes.len(),
            )
            .map_err(Receipts70MergeError::into_request_attempt)?;

            next_block_index = updated_block_index;
            first_block_receipt_index = updated_receipt_index;
        }

        Ok(merged)
    }

    fn on_receipt_count_mismatch(&mut self, peer_id: PeerId, error: ReceiptCountMismatch) {
        self.network
            .reputation_change(peer_id, ReputationChangeKind::BadProtocol);
        warn!(
            peer = %peer_id,
            response_kind = error.response_kind,
            requested_blocks = error.requested_blocks,
            returned_blocks = error.returned_blocks,
            block_index = error.block_index,
            expected_receipts = error.expected_receipts,
            returned_receipts = error.returned_receipts,
            "peer returned receipts inconsistent with requested blocks, disconnecting it"
        );
    }
}

#[derive(Debug, Clone)]
pub(super) enum RequestAttempt {
    Disconnected,
    Request(reth_network::p2p::error::RequestError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Receipts70MergeError {
    EmptyResponse,
    ResponseOverflow,
    UnexpectedAppend,
    NoProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResponseProgress {
    Complete,
    Partial { returned: usize },
    Empty,
    Overflow { returned: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ReceiptCountMismatch {
    response_kind: &'static str,
    requested_blocks: usize,
    returned_blocks: usize,
    block_index: Option<usize>,
    expected_receipts: Option<usize>,
    returned_receipts: Option<usize>,
}

impl Receipts70MergeError {
    pub(super) fn into_request_attempt(self) -> RequestAttempt {
        RequestAttempt::Request(reth_network::p2p::error::RequestError::BadResponse)
    }
}

pub(super) fn classify_response_progress(requested: usize, returned: usize) -> ResponseProgress {
    match returned.cmp(&requested) {
        std::cmp::Ordering::Equal => ResponseProgress::Complete,
        std::cmp::Ordering::Less if returned == 0 => ResponseProgress::Empty,
        std::cmp::Ordering::Less => ResponseProgress::Partial { returned },
        std::cmp::Ordering::Greater => ResponseProgress::Overflow { returned },
    }
}

pub(super) fn prioritize_preferred_peer_ids(
    mut peer_ids: Vec<PeerId>,
    preferred_peers: &[PeerId],
) -> Vec<PeerId> {
    if peer_ids.len() <= 1 || preferred_peers.is_empty() {
        return peer_ids;
    }

    let mut prioritized = Vec::with_capacity(peer_ids.len());
    for preferred in preferred_peers {
        if let Some(index) = peer_ids.iter().position(|peer_id| peer_id == preferred) {
            prioritized.push(peer_ids.remove(index));
        }
    }
    prioritized.extend(peer_ids);
    prioritized
}

fn validate_receipt_response_counts<T>(
    response_kind: &'static str,
    requested_blocks: usize,
    receipts: &[Vec<alloy_consensus::ReceiptWithBloom<T>>],
    expected_receipt_counts: Option<&[usize]>,
) -> std::result::Result<(), ReceiptCountMismatch> {
    if receipts.len() != requested_blocks {
        return Err(ReceiptCountMismatch {
            response_kind,
            requested_blocks,
            returned_blocks: receipts.len(),
            block_index: None,
            expected_receipts: None,
            returned_receipts: None,
        });
    }

    let Some(expected_receipt_counts) = expected_receipt_counts else {
        return Ok(());
    };

    for (block_index, (block_receipts, expected_count)) in receipts
        .iter()
        .zip(expected_receipt_counts.iter().copied())
        .enumerate()
    {
        if block_receipts.len() != expected_count {
            return Err(ReceiptCountMismatch {
                response_kind,
                requested_blocks,
                returned_blocks: receipts.len(),
                block_index: Some(block_index),
                expected_receipts: Some(expected_count),
                returned_receipts: Some(block_receipts.len()),
            });
        }
    }

    Ok(())
}

pub(super) fn merge_receipts70_response<T>(
    merged: &mut Vec<Vec<alloy_consensus::ReceiptWithBloom<T>>>,
    next_block_index: usize,
    first_block_receipt_index: u64,
    response: Receipts70<T>,
    expected_blocks: usize,
) -> std::result::Result<(usize, u64), Receipts70MergeError>
where
    T: alloy_consensus::TxReceipt,
{
    let previous_state = (next_block_index, first_block_receipt_index);
    let returned_blocks = response.receipts.len();
    if returned_blocks == 0 {
        return Err(Receipts70MergeError::EmptyResponse);
    }

    if next_block_index + returned_blocks > expected_blocks {
        return Err(Receipts70MergeError::ResponseOverflow);
    }

    let last_block_incomplete = response.last_block_incomplete;
    let receipts = response.into_with_bloom().0;
    for (offset, block_receipts) in receipts.into_iter().enumerate() {
        let target_index = next_block_index + offset;
        if target_index < merged.len() {
            if offset != 0 || first_block_receipt_index == 0 {
                return Err(Receipts70MergeError::UnexpectedAppend);
            }
            if block_receipts.is_empty() {
                return Err(Receipts70MergeError::NoProgress);
            }
            merged[target_index].extend(block_receipts);
        } else if target_index == merged.len() {
            merged.push(block_receipts);
        } else {
            return Err(Receipts70MergeError::ResponseOverflow);
        }
    }

    let (updated_block_index, updated_receipt_index) = if last_block_incomplete {
        let partial_block_index = next_block_index + returned_blocks - 1;
        let received_receipts = merged
            .get(partial_block_index)
            .map(Vec::len)
            .unwrap_or_default();
        if received_receipts == 0 {
            return Err(Receipts70MergeError::NoProgress);
        }
        (
            next_block_index + returned_blocks - 1,
            received_receipts as u64,
        )
    } else {
        (next_block_index + returned_blocks, 0)
    };

    if (updated_block_index, updated_receipt_index) == previous_state {
        return Err(Receipts70MergeError::NoProgress);
    }

    Ok((updated_block_index, updated_receipt_index))
}

pub(super) trait IntoResponseValue<T> {
    fn into_value(self) -> T;
}

impl<T> IntoResponseValue<Vec<T>> for BlockHeaders<T> {
    fn into_value(self) -> Vec<T> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<T>> for BlockBodies<T> {
    fn into_value(self) -> Vec<T> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<Vec<alloy_consensus::ReceiptWithBloom<T>>>> for Receipts<T> {
    fn into_value(self) -> Vec<Vec<alloy_consensus::ReceiptWithBloom<T>>> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<Vec<T>>> for Receipts69<T> {
    fn into_value(self) -> Vec<Vec<T>> {
        self.0
    }
}

impl<T> IntoResponseValue<Vec<Vec<T>>> for Receipts70<T> {
    fn into_value(self) -> Vec<Vec<T>> {
        self.receipts
    }
}

impl<T> IntoResponseValue<Receipts70<T>> for Receipts70<T> {
    fn into_value(self) -> Receipts70<T> {
        self
    }
}

#[cfg(test)]
mod tests {
    use alloy_consensus::{ReceiptWithBloom, TxType};
    use alloy_primitives::Log;
    use reth_ethereum_primitives::Receipt;

    use super::*;

    #[test]
    fn response_progress_distinguishes_complete_partial_and_overflow() {
        assert_eq!(classify_response_progress(8, 8), ResponseProgress::Complete);
        assert_eq!(classify_response_progress(8, 0), ResponseProgress::Empty);
        assert_eq!(
            classify_response_progress(8, 7),
            ResponseProgress::Partial { returned: 7 }
        );
        assert_eq!(
            classify_response_progress(8, 9),
            ResponseProgress::Overflow { returned: 9 }
        );
    }

    #[test]
    fn preferred_peer_ids_are_tried_before_rotation_order() {
        let first = PeerId::repeat_byte(0x11);
        let second = PeerId::repeat_byte(0x22);
        let third = PeerId::repeat_byte(0x33);
        let missing = PeerId::repeat_byte(0x44);

        let ordered =
            prioritize_preferred_peer_ids(vec![first, second, third], &[missing, third, first]);

        assert_eq!(ordered, vec![third, first, second]);
    }

    fn fake_receipt(gas: u64) -> Receipt {
        Receipt {
            tx_type: TxType::Legacy,
            success: true,
            cumulative_gas_used: gas,
            logs: Vec::<Log>::new(),
        }
    }

    #[test]
    fn eth70_partial_receipts_are_merged_across_requests() {
        let mut merged = Vec::<Vec<ReceiptWithBloom<Receipt>>>::new();

        let (next_block_index, first_block_receipt_index) = merge_receipts70_response(
            &mut merged,
            0,
            0,
            Receipts70 {
                last_block_incomplete: true,
                receipts: vec![
                    vec![fake_receipt(1)],
                    vec![fake_receipt(2)],
                    vec![fake_receipt(3)],
                ],
            },
            3,
        )
        .expect("first partial response should merge");

        assert_eq!(next_block_index, 2);
        assert_eq!(first_block_receipt_index, 1);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[2].len(), 1);

        let (next_block_index, first_block_receipt_index) = merge_receipts70_response(
            &mut merged,
            next_block_index,
            first_block_receipt_index,
            Receipts70 {
                last_block_incomplete: false,
                receipts: vec![vec![fake_receipt(4)]],
            },
            3,
        )
        .expect("continuation response should merge");

        assert_eq!(next_block_index, 3);
        assert_eq!(first_block_receipt_index, 0);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[2].len(), 2);
    }

    #[test]
    fn eth70_empty_response_is_rejected() {
        let mut merged = Vec::<Vec<ReceiptWithBloom<Receipt>>>::new();

        let error = merge_receipts70_response(
            &mut merged,
            0,
            0,
            Receipts70 {
                last_block_incomplete: false,
                receipts: Vec::<Vec<Receipt>>::new(),
            },
            1,
        )
        .expect_err("empty response should be rejected");

        assert_eq!(error, Receipts70MergeError::EmptyResponse);
    }
}
