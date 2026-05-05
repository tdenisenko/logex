use super::*;
use crate::validation::validate_header_matches_anchor;
use logex_types::{ExecutionAnchor, NodeState};

const CONSENSUS_WAIT_INTERVAL: Duration = Duration::from_secs(2);

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
            self.refresh_connectivity_state();
            if cancelable(
                &mut self.shutdown,
                self.peers.fill_peers(1, self.config.max_peers),
            )
            .await
            .is_none()
            {
                return self.finish_shutdown();
            }
            self.refresh_connectivity_state();
            if self.peers.peer_count() > 0 {
                self.connected_once = true;
                break;
            }
            attempt += 1;
            let delay = Duration::from_secs((attempt as u64).min(10));
            tracing::warn!(
                attempt,
                ?delay,
                "no peers connected yet while waiting for consensus-anchored sync"
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

            if self.peers.peer_count() < self.config.max_peers / 2 {
                let min_peers =
                    desired_refill_min_peers(self.peers.peer_count(), self.config.max_peers);
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
            if self.reconcile_consensus_reorg().await? {
                continue;
            }

            let Some(consensus) = self.consensus.clone() else {
                return Ok(());
            };

            let Some(anchor) = consensus.next_anchor_after(current) else {
                if self.try_mark_synced("caught up to available consensus anchors") {
                    self.sync_status_peers();
                } else {
                    self.set_runtime_state(NodeState::WaitingForConsensus);
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

            if anchor.block_number != current.saturating_add(1) {
                self.set_runtime_state(NodeState::WaitingForConsensus);
                tracing::debug!(
                    current_block = current,
                    next_anchor_block = anchor.block_number,
                    "consensus anchor gap detected; waiting for a contiguous anchor window"
                );
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

            let progressed = self.ingest_anchored_block(anchor).await?;
            if !progressed
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

    async fn ingest_anchored_block(&mut self, anchor: ExecutionAnchor) -> Result<bool> {
        let (header_peer, header) = match cancelable(
            &mut self.shutdown,
            self.peers.get_header_by_hash(anchor.block_hash),
        )
        .await
        {
            Some(Ok((peer_id, Some(header)))) => (peer_id, header),
            Some(Ok((_peer_id, None))) => {
                tracing::debug!(
                    block_number = anchor.block_number,
                    block_hash = %anchor.block_hash,
                    "no peer returned the anchored header yet"
                );
                return Ok(false);
            }
            Some(Err(error)) => {
                tracing::debug!(
                    error = %error,
                    block_number = anchor.block_number,
                    block_hash = %anchor.block_hash,
                    "anchored header request failed"
                );
                return Ok(false);
            }
            None => {
                self.finish_shutdown()?;
                return Ok(false);
            }
        };

        if let Err(error) = validate_downloaded_headers(
            anchor.block_number,
            self.expected_parent_for_validation(anchor.block_number),
            std::slice::from_ref(&header),
        ) {
            tracing::warn!(
                block_number = anchor.block_number,
                block_hash = %anchor.block_hash,
                header_peer = %header_peer,
                %error,
                "anchored header validation failed"
            );
            self.peers.report_invalid_block_data(header_peer, "headers");
            return Ok(false);
        }

        let block_hash = header.hash_slow();
        if let Err(error) = validate_header_matches_anchor(&anchor, &header, block_hash) {
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

        let mut newly_serving_peers = HashSet::new();
        self.note_serving_peer(header_peer, &mut newly_serving_peers);

        let bodies = match cancelable(
            &mut self.shutdown,
            self.peers.get_bodies(vec![block_hash], anchor.block_number),
        )
        .await
        {
            Some(Ok(bodies)) if bodies.len() == 1 => bodies,
            Some(Ok(_)) | Some(Err(_)) => return Ok(false),
            None => {
                self.finish_shutdown()?;
                return Ok(false);
            }
        };
        let (receipt_peer, receipts) = match cancelable(
            &mut self.shutdown,
            self.peers
                .get_receipts(vec![block_hash], anchor.block_number),
        )
        .await
        {
            Some(Ok((peer_id, receipts))) if receipts.len() == 1 => (peer_id, receipts),
            Some(Ok(_)) | Some(Err(_)) => return Ok(false),
            None => {
                self.finish_shutdown()?;
                return Ok(false);
            }
        };

        let (body_peer, body) = &bodies[0];
        if let Err(error) = validate_block_pre_execution(&header, body) {
            tracing::warn!(
                block_number = anchor.block_number,
                %block_hash,
                body_peer = %body_peer,
                %error,
                "anchored block pre-execution validation failed"
            );
            self.peers
                .report_invalid_block_data(*body_peer, "block bodies");
            return Ok(false);
        }

        if !receipts_match_transaction_count(body, &receipts[0]) {
            tracing::warn!(
                block_number = anchor.block_number,
                %block_hash,
                receipt_peer = %receipt_peer,
                transactions = body.transaction_count(),
                receipts = receipts[0].len(),
                "anchored block body / receipt count mismatch"
            );
            self.peers
                .report_invalid_block_data(receipt_peer, "receipts");
            return Ok(false);
        }

        if let Err(error) = validate_receipts_for_header(&header, &receipts[0]) {
            tracing::warn!(
                block_number = anchor.block_number,
                %block_hash,
                receipt_peer = %receipt_peer,
                %error,
                "anchored receipt validation failed"
            );
            self.peers
                .report_invalid_block_data(receipt_peer, "receipts");
            return Ok(false);
        }

        let txs = assemble_txs(body, &receipts[0]);
        if let Some(reorg) = self.head_tracker.track(header.clone()) {
            self.handle_reorg(reorg).await?;
        }

        let recent_headers = self.head_tracker.snapshot();
        self.peers
            .cache_canonical_block(header.clone(), body.clone(), &receipts[0]);
        let log_count = self
            .ingest_block(&header, block_hash, &txs, &recent_headers, Some(&anchor))
            .await?;
        self.progress.record_block(anchor.block_number, log_count);
        self.note_serving_peer(*body_peer, &mut newly_serving_peers);
        self.note_serving_peer(receipt_peer, &mut newly_serving_peers);
        self.last_validated_header = Some(header.clone());
        self.refresh_consensus_status().await;

        self.peers.set_head(Head {
            number: anchor.block_number,
            hash: block_hash,
            timestamp: header.timestamp(),
            ..Default::default()
        });

        if self.try_mark_synced("caught up to available consensus anchors") {
            self.sync_status_peers();
        } else {
            self.refresh_connectivity_state();
        }

        Ok(true)
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
