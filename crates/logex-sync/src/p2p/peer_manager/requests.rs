use eyre::bail;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::debug;

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
        self.drain_events_now();

        let request = HeadersRequest::rising(start_block.into(), count);
        let response = timeout(REQUEST_TIMEOUT, self.fetch_client.get_headers(request))
            .await
            .map_err(|_| eyre::eyre!("header request timed out"))?
            .map_err(|error| eyre::eyre!("header request failed: {error}"))?;
        let (peer_id, headers) = response.split();
        self.note_peer_success(peer_id);
        Ok((peer_id, headers))
    }

    /// Request block bodies for the given block hashes.
    pub async fn get_bodies(
        &mut self,
        hashes: Vec<B256>,
        required_block: u64,
    ) -> Result<Vec<SourcedBlockBody>> {
        self.drain_events_now();
        if hashes.is_empty() {
            return Ok(Vec::new());
        }

        let mut remaining_hashes = hashes.clone();
        let mut collected = Vec::with_capacity(hashes.len());

        while !remaining_hashes.is_empty() {
            let request_hashes = remaining_hashes.clone();
            let response = timeout(
                REQUEST_TIMEOUT,
                self.fetch_client
                    .get_block_bodies_with_priority_and_range_hint(
                        request_hashes.clone(),
                        reth_network::p2p::priority::Priority::Normal,
                        Some(body_range_hint(required_block, request_hashes.len())),
                    ),
            )
            .await
            .map_err(|_| eyre::eyre!("block body request timed out"))?
            .map_err(|error| eyre::eyre!("block body request failed: {error}"))?;
            let (peer_id, bodies) = response.split();

            match classify_response_progress(request_hashes.len(), bodies.len()) {
                ResponseProgress::Complete => {
                    self.note_peer_success(peer_id);
                    collected.extend(bodies.into_iter().map(|body| (peer_id, body)));
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
                    self.on_zero_progress_response(peer_id, "block bodies", request_hashes.len());
                    bail!("peer returned zero block bodies for a non-empty request")
                }
                ResponseProgress::Overflow { returned } => {
                    self.on_invalid_response_length(
                        peer_id,
                        "block bodies",
                        request_hashes.len(),
                        returned,
                    );
                    self.network.disconnect_peer(peer_id);
                    self.remove_peer(peer_id);
                    bail!("peer returned more block bodies than requested")
                }
            }
        }

        Ok(collected)
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
        self.drain_events_now();
        if hashes.is_empty() {
            return Ok((PeerId::ZERO, Vec::new()));
        }

        let peer_ids = self.peer_ids_for_requests(Some(required_block));
        let mut dead_peers = HashSet::new();

        for peer_id in peer_ids {
            let version = match self.peers.get(&peer_id) {
                Some(peer) => peer.version,
                None => continue,
            };

            if version >= EthVersion::Eth70 {
                match self.request_receipts70(peer_id, hashes.clone()).await {
                    Ok(receipts) => {
                        self.on_receipt_request_success(peer_id);
                        return Ok((peer_id, receipts));
                    }
                    Err(error) => {
                        let should_drop = self.on_request_error(peer_id, &error);
                        debug!(peer = %peer_id, ?error, "receipt request failed");
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
                                self.on_receipt_request_success(peer_id);
                                collected.extend(receipts);
                                return Ok((peer_id, collected));
                            }
                            ResponseProgress::Partial { returned } => {
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
