use super::*;
use crate::EXECUTION_HISTORY_TARGET_BLOCK;
use crate::validation::validate_header_matches_anchor;
use alloy_eips::BlockHashOrNumber;
use logex_types::{ExecutionAnchor, NodeState};

const CONSENSUS_WAIT_INTERVAL: Duration = Duration::from_secs(2);
const CONSENSUS_ANCHOR_FORWARD_BATCH_LIMIT: u64 = 32;

#[derive(Debug)]
struct ConsensusReorg {
    retained_headers: Vec<Header>,
    indexed_head: Option<ExecutionAnchor>,
    reverted_hashes: Vec<B256>,
}

impl SyncEngine {
    pub(super) async fn run_consensus_sync(&mut self) -> Result<()> {
        self.refresh_consensus_status().await;
        self.set_runtime_state(NodeState::Discovering);

        let mut attempt: u32 = 0;
        loop {
            if self.shutdown_requested() {
                return self.finish_shutdown();
            }
            self.refresh_consensus_status().await;
            if !self.set_peer_head_from_consensus() {
                self.set_runtime_state(NodeState::WaitingForConsensus);
                if cancelable(
                    &mut self.shutdown,
                    tokio::time::sleep(CONSENSUS_WAIT_INTERVAL),
                )
                .await
                .is_none()
                {
                    return self.finish_shutdown();
                }
                continue;
            }
            self.refresh_connectivity_state();
            let min_peers = peer_refill_goal(
                self.peers.peer_count(),
                self.peers.serving_peer_count(),
                self.config.max_peers,
            )
            .unwrap_or_else(|| refill_peer_floor(self.config.max_peers));
            if cancelable(
                &mut self.shutdown,
                self.peers.fill_peers(min_peers, self.config.max_peers),
            )
            .await
            .is_none()
            {
                return self.finish_shutdown();
            }
            self.refresh_connectivity_state();
            let required_block = self
                .consensus
                .as_ref()
                .and_then(|consensus| consensus.anchor_coverage().floor)
                .map_or(1, |anchor| anchor.block_number);
            if self.peers.has_block_request_peer(required_block) {
                self.connected_once = true;
                break;
            }
            attempt += 1;
            let delay = Duration::from_secs((attempt as u64).min(10));
            tracing::warn!(
                attempt,
                ?delay,
                "no execution peer eligible for consensus-anchored sync yet"
            );
            if cancelable(&mut self.shutdown, tokio::time::sleep(delay))
                .await
                .is_none()
            {
                return self.finish_shutdown();
            }
        }

        loop {
            if self.shutdown_requested() {
                return self.finish_shutdown();
            }

            if let Some(min_peers) = peer_refill_goal(
                self.peers.peer_count(),
                self.peers.serving_peer_count(),
                self.config.max_peers,
            ) {
                self.refresh_connectivity_state();
                if cancelable(
                    &mut self.shutdown,
                    self.peers.fill_peers(min_peers, self.config.max_peers),
                )
                .await
                .is_none()
                {
                    return self.finish_shutdown();
                }
                self.refresh_connectivity_state();
            }

            let current = self.current_block();
            self.refresh_consensus_status().await;
            self.set_peer_head_from_consensus();
            self.refresh_historical_status().await;
            if self.reconcile_consensus_reorg().await? {
                continue;
            }

            let Some(consensus) = self.consensus.clone() else {
                return Ok(());
            };

            let anchor_batch_limit = if current == 0 {
                1
            } else {
                self.config
                    .header_batch_size
                    .min(CONSENSUS_ANCHOR_FORWARD_BATCH_LIMIT)
            };
            let anchors = self
                .next_consensus_anchor_batch(current, &consensus, anchor_batch_limit)
                .await;
            if anchors.is_empty() {
                if self.try_mark_synced("caught up to available consensus anchors") {
                    self.sync_status_peers();
                } else {
                    self.set_runtime_state(NodeState::WaitingForConsensus);
                }
                if self.ingest_historical_backfill_batch().await? {
                    continue;
                }
                if cancelable(
                    &mut self.shutdown,
                    tokio::time::sleep(CONSENSUS_WAIT_INTERVAL),
                )
                .await
                .is_none()
                {
                    return self.finish_shutdown();
                }
                continue;
            };

            let forward_progressed = self.ingest_anchored_blocks(anchors).await?;
            let (current, target) = self.sync_cursor();
            let historical_progressed = if should_run_historical_backfill(
                current,
                target,
                LIVE_LAG_HISTORICAL_BACKFILL_THRESHOLD,
            ) {
                self.ingest_historical_backfill_batch().await?
            } else {
                false
            };
            if !(forward_progressed || historical_progressed)
                && cancelable(
                    &mut self.shutdown,
                    tokio::time::sleep(CONSENSUS_WAIT_INTERVAL),
                )
                .await
                .is_none()
            {
                return self.finish_shutdown();
            }
        }
    }

    async fn ingest_anchored_blocks(&mut self, anchors: Vec<ExecutionAnchor>) -> Result<bool> {
        let Some(first_anchor) = anchors.first().copied() else {
            return Ok(false);
        };
        let request_count = anchors.len() as u64;
        let (header_peer, headers) = match cancelable(
            &mut self.shutdown,
            self.peers
                .get_headers(first_anchor.block_number, request_count),
        )
        .await
        {
            Some(Ok((peer_id, headers))) if !headers.is_empty() => (peer_id, headers),
            Some(Ok((_peer_id, _headers))) => {
                tracing::debug!(
                    block_number = first_anchor.block_number,
                    "no peer returned the anchored headers yet"
                );
                return Ok(false);
            }
            Some(Err(error)) => {
                tracing::warn!(
                    error = %error,
                    start_block = first_anchor.block_number,
                    request_count,
                    "anchored header batch request failed"
                );
                return Ok(false);
            }
            None => {
                self.finish_shutdown()?;
                return Ok(false);
            }
        };

        if headers.len() > anchors.len() {
            tracing::warn!(
                requested = anchors.len(),
                returned = headers.len(),
                header_peer = %header_peer,
                "anchored header batch returned more headers than requested"
            );
            self.peers.report_invalid_block_data(header_peer, "headers");
            return Ok(false);
        }

        let anchors = &anchors[..headers.len()];
        if let Err(error) = validate_downloaded_headers(
            first_anchor.block_number,
            self.expected_parent_for_validation(first_anchor.block_number),
            &headers,
        ) {
            tracing::warn!(
                start_block = first_anchor.block_number,
                headers = headers.len(),
                header_peer = %header_peer,
                %error,
                "anchored header batch validation failed"
            );
            self.peers.report_invalid_block_data(header_peer, "headers");
            return Ok(false);
        }

        for (anchor, header) in anchors.iter().zip(&headers) {
            let block_hash = header.hash_slow();
            if let Err(error) = validate_header_matches_anchor(anchor, header, block_hash) {
                tracing::warn!(
                    block_number = anchor.block_number,
                    expected_hash = %anchor.block_hash,
                    got_hash = %block_hash,
                    header_peer = %header_peer,
                    %error,
                    "anchored header did not match the consensus anchor"
                );
                self.peers.report_invalid_block_data(header_peer, "headers");
                return Ok(false);
            }
        }

        let mut newly_serving_peers = HashSet::new();
        let hashes: Vec<B256> = headers.iter().map(|header| header.hash_slow()).collect();
        let mut progressed = false;
        let mut last_validated_header = None;
        let mut last_head = None;

        for ((chunk_headers, chunk_hashes), chunk_anchors) in headers
            .chunks(self.config.fetch_batch_size)
            .zip(hashes.chunks(self.config.fetch_batch_size))
            .zip(anchors.chunks(self.config.fetch_batch_size))
        {
            let chunk_headers = chunk_headers.to_vec();
            let chunk_hashes = chunk_hashes.to_vec();
            let required_block = chunk_headers
                .last()
                .map(|header| header.number())
                .unwrap_or(first_anchor.block_number);

            let bodies = match cancelable(
                &mut self.shutdown,
                self.peers.get_bodies_prefer_peers(
                    chunk_hashes.clone(),
                    required_block,
                    &[header_peer],
                ),
            )
            .await
            {
                Some(Ok(bodies)) if bodies.len() == chunk_headers.len() => bodies,
                Some(Ok(bodies)) => {
                    tracing::warn!(
                        headers = chunk_headers.len(),
                        bodies = bodies.len(),
                        "anchored block body request returned an unexpected response"
                    );
                    return Ok(progressed);
                }
                Some(Err(error)) => {
                    tracing::warn!(%error, "anchored block body request failed");
                    return Ok(progressed);
                }
                None => {
                    self.finish_shutdown()?;
                    return Ok(progressed);
                }
            };

            for (i, header) in chunk_headers.iter().enumerate() {
                let block_number = header.number();
                let block_hash = chunk_hashes[i];
                let (body_peer, body) = &bodies[i];
                if let Err(error) = validate_block_pre_execution(header, body) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        body_peer = %body_peer,
                        %error,
                        "anchored block pre-execution validation failed"
                    );
                    self.peers
                        .report_invalid_block_data(*body_peer, "block bodies");
                    return Ok(progressed);
                }
            }

            let expected_receipt_counts: Vec<usize> = bodies
                .iter()
                .map(|(_peer_id, body)| body.transaction_count())
                .collect();
            let receipt_peer_preference = preferred_body_peers(&bodies, header_peer);
            let (receipt_peer, receipts) = match cancelable(
                &mut self.shutdown,
                self.peers.get_receipts_matching_counts_prefer_peers(
                    chunk_hashes.clone(),
                    required_block,
                    &expected_receipt_counts,
                    &receipt_peer_preference,
                ),
            )
            .await
            {
                Some(Ok((peer_id, receipts))) if receipts.len() == chunk_headers.len() => {
                    (peer_id, receipts)
                }
                Some(Ok((peer_id, receipts))) => {
                    tracing::warn!(
                        headers = chunk_headers.len(),
                        receipt_peer = %peer_id,
                        returned_receipt_sets = receipts.len(),
                        "anchored receipt request returned an unexpected response"
                    );
                    return Ok(progressed);
                }
                Some(Err(error)) => {
                    tracing::warn!(%error, "anchored receipt request failed");
                    return Ok(progressed);
                }
                None => {
                    self.finish_shutdown()?;
                    return Ok(progressed);
                }
            };

            for (i, header) in chunk_headers.iter().enumerate() {
                let anchor = chunk_anchors[i];
                let block_hash = chunk_hashes[i];
                let block_number = header.number();
                let (body_peer, body) = &bodies[i];

                if !receipts_match_transaction_count(body, &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        transactions = body.transaction_count(),
                        receipts = receipts[i].len(),
                        "anchored block body / receipt count mismatch"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    return Ok(progressed);
                }

                if let Err(error) = validate_receipts_for_header(header, &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        %error,
                        "anchored receipt validation failed"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    return Ok(progressed);
                }

                let txs = assemble_txs(body, &receipts[i]);
                if let Some(reorg) = self.head_tracker.track(header.clone()) {
                    self.handle_reorg(reorg).await?;
                }

                let recent_headers = self.head_tracker.snapshot();
                self.peers
                    .cache_canonical_block(header.clone(), body.clone(), &receipts[i]);
                let log_count = self
                    .ingest_block(header, block_hash, &txs, &recent_headers, Some(&anchor))
                    .await?;
                self.progress.record_block(block_number, log_count);
                self.note_serving_peer(header_peer, &mut newly_serving_peers);
                self.note_serving_peer(*body_peer, &mut newly_serving_peers);
                self.note_serving_peer(receipt_peer, &mut newly_serving_peers);
                last_validated_header = Some(header.clone());
                last_head = Some(execution_head(block_number, block_hash, header.timestamp()));
                progressed = true;
            }
        }

        self.last_validated_header = last_validated_header;
        self.refresh_consensus_status().await;
        self.refresh_historical_status().await;

        if let Some(head) = last_head {
            self.peers.set_head(head);
        }

        if self.try_mark_synced("caught up to available consensus anchors") {
            self.sync_status_peers();
        } else {
            self.refresh_connectivity_state();
        }

        Ok(progressed)
    }

    async fn next_consensus_anchor_batch(
        &self,
        current: u64,
        consensus: &ConsensusStore,
        limit: u64,
    ) -> Vec<ExecutionAnchor> {
        let storage_has_head = if current == 0 {
            let storage = self.storage.read().await;
            storage.sync_head().is_some()
        } else {
            true
        };
        contiguous_anchor_batch(
            current,
            !storage_has_head,
            &consensus.ordered_anchors(),
            limit as usize,
        )
    }
}

fn contiguous_anchor_batch(
    current: u64,
    allow_bootstrap_jump: bool,
    records: &[logex_cl::AnchorRecord],
    limit: usize,
) -> Vec<ExecutionAnchor> {
    if limit == 0 {
        return Vec::new();
    }

    let mut anchors = Vec::new();
    let mut next_expected = current.saturating_add(1);
    for record in records
        .iter()
        .map(|record| record.anchor)
        .filter(|anchor| anchor.block_number > current)
    {
        if anchors.is_empty() {
            if record.block_number == next_expected
                || can_bootstrap_from_anchor(current, record.block_number) && allow_bootstrap_jump
            {
                next_expected = record.block_number.saturating_add(1);
                anchors.push(record);
            } else {
                return Vec::new();
            }
        } else if record.block_number == next_expected {
            next_expected = next_expected.saturating_add(1);
            anchors.push(record);
        } else {
            break;
        }

        if anchors.len() >= limit {
            break;
        }
    }

    anchors
}

impl SyncEngine {
    async fn ingest_historical_backfill_batch(&mut self) -> Result<bool> {
        let child_header = {
            let storage = self.storage.read().await;
            storage.historical_floor_header().cloned()
        };
        let Some(child_header) = child_header else {
            return Ok(false);
        };
        if child_header.number() == EXECUTION_HISTORY_TARGET_BLOCK {
            self.refresh_historical_status().await;
            return Ok(false);
        }

        if self.peers.peer_count() == 0 {
            self.refresh_connectivity_state();
            return Ok(false);
        }

        let request_count = self
            .config
            .header_batch_size
            .min(HISTORICAL_BACKFILL_HEADER_BATCH_LIMIT)
            .min(child_header.number() - EXECUTION_HISTORY_TARGET_BLOCK);
        let (header_peer, headers) = match cancelable(
            &mut self.shutdown,
            self.peers.get_headers_reverse(
                BlockHashOrNumber::Hash(child_header.parent_hash()),
                request_count,
            ),
        )
        .await
        {
            Some(Ok(response)) => response,
            Some(Err(error)) => {
                tracing::debug!(
                    error = %error,
                    child_block = child_header.number(),
                    "historical reverse header request failed"
                );
                self.refresh_connectivity_state();
                return Ok(false);
            }
            None => {
                self.finish_shutdown()?;
                return Ok(false);
            }
        };

        if headers.is_empty() {
            self.refresh_connectivity_state();
            return Ok(false);
        }

        if let Err(error) = validate_reverse_downloaded_headers(&child_header, &headers) {
            tracing::warn!(
                child_block = child_header.number(),
                header_peer = %header_peer,
                %error,
                "historical reverse header validation failed"
            );
            self.peers.report_invalid_block_data(header_peer, "headers");
            self.refresh_connectivity_state();
            return Ok(false);
        }

        let mut newly_serving_peers = HashSet::new();

        let hashes: Vec<B256> = headers.iter().map(|header| header.hash_slow()).collect();
        let mut progressed = false;
        for (chunk_headers, chunk_hashes) in headers
            .chunks(self.config.fetch_batch_size)
            .zip(hashes.chunks(self.config.fetch_batch_size))
        {
            let chunk_headers = chunk_headers.to_vec();
            let chunk_hashes = chunk_hashes.to_vec();
            let required_block = chunk_headers
                .first()
                .map(|header| header.number())
                .unwrap_or(child_header.number().saturating_sub(1));

            let bodies = match cancelable(
                &mut self.shutdown,
                self.peers.get_bodies_prefer_peers(
                    chunk_hashes.clone(),
                    required_block,
                    &[header_peer],
                ),
            )
            .await
            {
                Some(Ok(bodies)) if bodies.len() == chunk_headers.len() => bodies,
                Some(Ok(bodies)) => {
                    tracing::debug!(
                        headers = chunk_headers.len(),
                        bodies = bodies.len(),
                        "historical body response count mismatch"
                    );
                    return Ok(progressed);
                }
                Some(Err(error)) => {
                    tracing::debug!(error = %error, "historical body request failed");
                    return Ok(progressed);
                }
                None => {
                    self.finish_shutdown()?;
                    return Ok(progressed);
                }
            };

            for (i, header) in chunk_headers.iter().enumerate() {
                let block_hash = chunk_hashes[i];
                let block_number = header.number();
                let (body_peer, body) = &bodies[i];
                if let Err(error) = validate_block_pre_execution(header, body) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        body_peer = %body_peer,
                        %error,
                        "historical block pre-execution validation failed"
                    );
                    self.peers
                        .report_invalid_block_data(*body_peer, "block bodies");
                    return Ok(progressed);
                }
            }

            let expected_receipt_counts: Vec<usize> = bodies
                .iter()
                .map(|(_peer_id, body)| body.transaction_count())
                .collect();
            let receipt_peer_preference = preferred_body_peers(&bodies, header_peer);

            let (receipt_peer, receipts) = match cancelable(
                &mut self.shutdown,
                self.peers.get_receipts_matching_counts_prefer_peers(
                    chunk_hashes.clone(),
                    required_block,
                    &expected_receipt_counts,
                    &receipt_peer_preference,
                ),
            )
            .await
            {
                Some(Ok((peer_id, receipts))) if receipts.len() == chunk_headers.len() => {
                    (peer_id, receipts)
                }
                Some(Ok((_peer_id, receipts))) => {
                    tracing::debug!(
                        headers = chunk_headers.len(),
                        receipts = receipts.len(),
                        "historical receipt response count mismatch"
                    );
                    return Ok(progressed);
                }
                Some(Err(error)) => {
                    tracing::debug!(error = %error, "historical receipt request failed");
                    return Ok(progressed);
                }
                None => {
                    self.finish_shutdown()?;
                    return Ok(progressed);
                }
            };

            for (i, header) in chunk_headers.iter().enumerate() {
                let block_hash = chunk_hashes[i];
                let block_number = header.number();
                let (body_peer, body) = &bodies[i];

                if !receipts_match_transaction_count(body, &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        transactions = body.transaction_count(),
                        receipts = receipts[i].len(),
                        "historical block body / receipt count mismatch"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    return Ok(progressed);
                }

                if let Err(error) = validate_receipts_for_header(header, &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        %error,
                        "historical receipt validation failed"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    return Ok(progressed);
                }

                let txs = assemble_txs(body, &receipts[i]);
                self.peers
                    .cache_canonical_block(header.clone(), body.clone(), &receipts[i]);
                let log_count = self
                    .ingest_historical_block(header, block_hash, &txs)
                    .await?;
                self.note_serving_peer(header_peer, &mut newly_serving_peers);
                self.note_serving_peer(*body_peer, &mut newly_serving_peers);
                self.note_serving_peer(receipt_peer, &mut newly_serving_peers);
                progressed = true;

                tracing::trace!(
                    block_number,
                    logs = log_count,
                    "historical block verified and ingested"
                );
            }
        }

        self.refresh_historical_status().await;
        Ok(progressed)
    }

    async fn reconcile_consensus_reorg(&mut self) -> Result<bool> {
        let Some(consensus) = self.consensus.as_ref() else {
            return Ok(false);
        };

        let recent_headers = self.head_tracker.snapshot();
        let Some(reorg) = locate_consensus_reorg(consensus, &recent_headers)? else {
            return Ok(false);
        };

        self.peers.remove_cached_blocks(&reorg.reverted_hashes);

        let reverted_rows = {
            let mut storage = self.storage.write().await;
            let mut total_reverted = 0u64;
            for hash in &reorg.reverted_hashes {
                total_reverted += storage
                    .mark_non_canonical(*hash)
                    .map_err(|error| eyre::eyre!("consensus reorg error: {error}"))?;
            }
            storage
                .rewind_canonical_state(&reorg.retained_headers, reorg.indexed_head)
                .map_err(|error| eyre::eyre!("consensus reorg state rewind error: {error}"))?;
            total_reverted
        };

        self.head_tracker.restore(reorg.retained_headers.clone());
        self.last_validated_header = reorg.retained_headers.last().cloned();
        self.progress
            .rewind_to(reorg.indexed_head.map_or(0, |anchor| anchor.block_number));
        self.refresh_consensus_status().await;

        tracing::warn!(
            reverted_blocks = reorg.reverted_hashes.len(),
            reverted_rows,
            rewind_to = reorg.indexed_head.map(|anchor| anchor.block_number),
            "rewound indexed canonical state to match the latest consensus anchors"
        );

        Ok(true)
    }

    pub(super) async fn refresh_consensus_status(&self) {
        let Some(consensus) = self.consensus.as_ref() else {
            return;
        };

        let checkpoint = consensus.checkpoint();
        let mut anchors = consensus.chain_anchors();
        let indexed_head = {
            let storage = self.storage.read().await;
            storage.chain_anchors().indexed_head
        };
        anchors.indexed_head = indexed_head;
        self.progress.update_consensus_state(checkpoint, &anchors);
    }

    async fn refresh_historical_status(&self) {
        let (floor, anchor) = {
            let storage = self.storage.read().await;
            (storage.historical_floor(), storage.historical_anchor())
        };
        self.progress
            .initialize_historical_state(floor, anchor, EXECUTION_HISTORY_TARGET_BLOCK);
    }
}

fn can_bootstrap_from_anchor(current: u64, anchor_block: u64) -> bool {
    current == 0 && anchor_block > 0
}

fn locate_consensus_reorg(
    consensus: &ConsensusStore,
    recent_headers: &[Header],
) -> Result<Option<ConsensusReorg>> {
    let Some(tip) = recent_headers.last() else {
        return Ok(None);
    };

    let tip_hash = tip.hash_slow();
    if consensus
        .anchor_at(tip.number())
        .is_some_and(|anchor| anchor.block_hash == tip_hash)
    {
        return Ok(None);
    }

    for index in (0..recent_headers.len()).rev() {
        let header = &recent_headers[index];
        let header_hash = header.hash_slow();
        if let Some(anchor) = consensus.anchor_at(header.number())
            && anchor.block_hash == header_hash
        {
            return Ok(Some(ConsensusReorg {
                retained_headers: recent_headers[..=index].to_vec(),
                indexed_head: Some(anchor),
                reverted_hashes: recent_headers[index + 1..]
                    .iter()
                    .map(|header| header.hash_slow())
                    .collect(),
            }));
        }
    }

    let first_block = recent_headers
        .first()
        .map(Header::number)
        .unwrap_or_default();
    let last_block = recent_headers
        .last()
        .map(Header::number)
        .unwrap_or_default();
    Err(eyre::eyre!(
        "consensus anchor reorg exceeded the persisted recent-header window ({first_block}..{last_block}); a fresh checkpointed resync is required"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::B256;
    use logex_cl::AnchorRecord;
    use tempfile::TempDir;

    fn header(number: u64, parent_hash: B256, marker: u8) -> Header {
        let mut header = Header {
            number,
            parent_hash,
            gas_limit: 30_000_000,
            timestamp: 1_700_000_000 + number,
            ..Default::default()
        };
        header.extra_data = vec![marker].into();
        header
    }

    fn anchor_for(header: &Header, beacon_slot: u64) -> AnchorRecord {
        AnchorRecord {
            anchor: ExecutionAnchor {
                beacon_root: B256::repeat_byte(beacon_slot as u8),
                beacon_slot,
                block_number: header.number(),
                block_hash: header.hash_slow(),
                receipts_root: header.receipts_root(),
            },
            finalized: false,
            parent_beacon_root: None,
        }
    }

    #[test]
    fn contiguous_anchor_batch_allows_fresh_checkpoint_jump() {
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let third = header(102, second.hash_slow(), 0x03);
        let anchors = vec![
            anchor_for(&first, 1),
            anchor_for(&second, 2),
            anchor_for(&third, 3),
        ];

        let batch = contiguous_anchor_batch(0, true, &anchors, 2);

        assert_eq!(
            batch
                .iter()
                .map(|anchor| anchor.block_number)
                .collect::<Vec<_>>(),
            vec![100, 101]
        );
    }

    #[test]
    fn contiguous_anchor_batch_stops_at_first_gap() {
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let gap = header(103, second.hash_slow(), 0x03);
        let anchors = vec![
            anchor_for(&first, 1),
            anchor_for(&second, 2),
            anchor_for(&gap, 3),
        ];

        let batch = contiguous_anchor_batch(99, false, &anchors, 16);

        assert_eq!(
            batch
                .iter()
                .map(|anchor| anchor.block_number)
                .collect::<Vec<_>>(),
            vec![100, 101]
        );
    }

    #[test]
    fn contiguous_anchor_batch_rejects_noncontiguous_start_without_bootstrap() {
        let first = header(100, B256::ZERO, 0x01);
        let anchors = vec![anchor_for(&first, 1)];

        assert!(contiguous_anchor_batch(98, false, &anchors, 16).is_empty());
    }

    #[test]
    fn locate_consensus_reorg_returns_none_for_matching_tip() {
        let temp = TempDir::new().unwrap();
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
        )
        .unwrap();
        store
            .append_anchors(vec![anchor_for(&first, 1), anchor_for(&second, 2)])
            .unwrap();

        assert!(
            locate_consensus_reorg(&store, &[first, second])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn locate_consensus_reorg_finds_common_ancestor() {
        let temp = TempDir::new().unwrap();
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let old_third = header(102, second.hash_slow(), 0x03);
        let new_third = header(102, second.hash_slow(), 0x13);
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        )
        .unwrap();
        store
            .append_anchors(vec![
                anchor_for(&first, 1),
                anchor_for(&second, 2),
                anchor_for(&new_third, 3),
            ])
            .unwrap();

        let reorg =
            locate_consensus_reorg(&store, &[first.clone(), second.clone(), old_third.clone()])
                .unwrap()
                .expect("expected a consensus reorg");
        assert_eq!(reorg.retained_headers, vec![first, second.clone()]);
        assert_eq!(
            reorg.indexed_head.map(|anchor| anchor.block_hash),
            Some(second.hash_slow())
        );
        assert_eq!(reorg.reverted_hashes, vec![old_third.hash_slow()]);
    }

    #[test]
    fn locate_consensus_reorg_errors_when_window_has_no_common_ancestor() {
        let temp = TempDir::new().unwrap();
        let first = header(100, B256::ZERO, 0x01);
        let second = header(101, first.hash_slow(), 0x02);
        let competing_first = header(100, B256::ZERO, 0x11);
        let competing_second = header(101, competing_first.hash_slow(), 0x12);
        let store = ConsensusStore::open(
            temp.path(),
            Some("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"),
        )
        .unwrap();
        store
            .append_anchors(vec![
                anchor_for(&competing_first, 1),
                anchor_for(&competing_second, 2),
            ])
            .unwrap();

        let error = locate_consensus_reorg(&store, &[first, second]).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("consensus anchor reorg exceeded the persisted recent-header window")
        );
    }
}
