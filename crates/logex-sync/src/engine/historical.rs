use super::*;

impl SyncEngine {
    /// Run the sync loop: historical catch-up, then live following.
    pub async fn run(&mut self) -> Result<()> {
        let (start_block, recent_headers) = {
            let storage = self.storage.read().await;
            (
                storage
                    .sync_head()
                    .map(|head| head.block_number + 1)
                    .or_else(|| storage.indexed_head_block().map(|block| block + 1))
                    .unwrap_or(0),
                storage.recent_headers().to_vec(),
            )
        };

        if !recent_headers.is_empty() {
            self.head_tracker.restore(recent_headers.clone());
            self.last_validated_header = recent_headers.last().cloned();
            tracing::info!(
                restored_headers = recent_headers.len(),
                restored_tip = self.head_tracker.tip().map(|(number, _)| number),
                "restored recent canonical header window from storage"
            );
        }

        tracing::info!(start_block, "starting sync");

        if self.consensus.is_some() {
            return self.run_consensus_sync().await;
        }

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
                if let Some(target) = self.peers.highest_peer_block() {
                    self.progress.set_target(target);
                }
                self.refresh_connectivity_state();
                break;
            }
            attempt += 1;
            let delay = Duration::from_secs((attempt as u64).min(10));
            tracing::warn!(
                attempt,
                ?delay,
                "no peers connected yet, waiting for discovery to populate"
            );
            if cancelable(&mut self.shutdown, tokio::time::sleep(delay))
                .await
                .is_none()
            {
                return self.finish_shutdown();
            }
        }
        tracing::info!(peers = self.peers.peer_count(), "connected to peers");

        let mut current = start_block;
        let mut consecutive_empty: u32 = 0;
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
            if self.peers.peer_count() == 0 {
                self.refresh_connectivity_state();
                tracing::warn!("no peers available, waiting for discovery");
                if cancelable(
                    &mut self.shutdown,
                    tokio::time::sleep(Duration::from_secs(2)),
                )
                .await
                .is_none()
                {
                    return self.finish_shutdown();
                }
                continue;
            }
            if let Some(target) = self.peers.highest_peer_block() {
                self.progress.set_target(target);
            }
            self.refresh_connectivity_state();

            let (header_peer, headers) = match cancelable(
                &mut self.shutdown,
                self.peers
                    .get_headers(current, self.config.header_batch_size),
            )
            .await
            {
                Some(Ok(h)) => h,
                Some(Err(e)) => {
                    self.refresh_connectivity_state();
                    tracing::debug!(error = %e, current, "header request failed, retrying");
                    continue;
                }
                None => return self.finish_shutdown(),
            };
            self.refresh_connectivity_state();
            if headers.is_empty() {
                consecutive_empty += 1;
                let target_block = self.known_target_block();
                if let Some(target_block) =
                    should_mark_historical_complete(current, target_block, consecutive_empty)
                {
                    tracing::info!(
                        current_block = current.saturating_sub(1),
                        target_block,
                        consecutive_empty,
                        "historical catch-up reached advertised peer tip"
                    );
                    self.progress.mark_synced();
                    self.sync_status_peers();
                    break;
                }

                if should_switch_to_live_without_target(current, target_block, consecutive_empty) {
                    tracing::info!(
                        current_block = current.saturating_sub(1),
                        consecutive_empty,
                        "no higher historical batches available from current serving peers, switching to head polling"
                    );
                    break;
                }

                if let Some(target_block) = target_block {
                    tracing::debug!(
                        next_block = current,
                        target_block,
                        consecutive_empty,
                        "peer returned empty headers before the advertised historical target was confirmed"
                    );
                } else if consecutive_empty == 1
                    || consecutive_empty.is_multiple_of(HISTORICAL_EMPTY_THRESHOLD)
                {
                    tracing::info!(
                        next_block = current,
                        consecutive_empty,
                        connected_peers = self.peers.peer_count(),
                        serving_peers = self.peers.serving_peer_count(),
                        pending_peers = self.peers.pending_count(),
                        "waiting for a serving peer to provide a credible sync target"
                    );
                } else {
                    tracing::debug!(
                        next_block = current,
                        consecutive_empty,
                        "peer returned empty headers while the sync target is still unknown"
                    );
                }
                continue;
            }
            consecutive_empty = 0;

            if let Err(error) = validate_downloaded_headers(
                current,
                self.expected_parent_for_validation(current),
                &headers,
            ) {
                tracing::warn!(
                    start_block = current,
                    header_peer = %header_peer,
                    %error,
                    "header validation failed — retrying from last ingested block"
                );
                self.peers.report_invalid_block_data(header_peer, "headers");
                self.refresh_connectivity_state();
                continue;
            }

            let hashes: Vec<B256> = headers.iter().map(|h| h.hash_slow()).collect();
            let mut next_block = current;
            let mut chunk_failed = false;
            let mut last_ingested_head = None;
            let mut newly_serving_peers = HashSet::new();

            for (chunk_headers, chunk_hashes) in headers
                .chunks(self.config.fetch_batch_size)
                .zip(hashes.chunks(self.config.fetch_batch_size))
            {
                let chunk_headers = chunk_headers.to_vec();
                let chunk_hashes = chunk_hashes.to_vec();
                let required_block = chunk_headers
                    .last()
                    .map(|header| header.number())
                    .unwrap_or(current);

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
                    Some(Ok(bodies)) => bodies,
                    Some(Err(e)) => {
                        tracing::debug!(error = %e, "body request failed, retrying batch");
                        chunk_failed = true;
                        break;
                    }
                    None => return self.finish_shutdown(),
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
                            "block pre-execution validation failed — retrying from last ingested block"
                        );
                        self.peers
                            .report_invalid_block_data(*body_peer, "block bodies");
                        chunk_failed = true;
                        break;
                    }
                }
                if chunk_failed {
                    break;
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
                    Some(Ok(receipts)) => receipts,
                    Some(Err(e)) => {
                        tracing::debug!(error = %e, "receipt request failed, retrying batch");
                        chunk_failed = true;
                        break;
                    }
                    None => return self.finish_shutdown(),
                };

                if bodies.len() != chunk_headers.len() || receipts.len() != chunk_headers.len() {
                    tracing::warn!(
                        headers = chunk_headers.len(),
                        bodies = bodies.len(),
                        receipts = receipts.len(),
                        "peer returned mismatched counts, retrying from last ingested block"
                    );
                    chunk_failed = true;
                    break;
                }

                for (i, header) in chunk_headers.iter().enumerate() {
                    let block_hash = chunk_hashes[i];
                    let block_number = header.number();
                    let timestamp = header.timestamp();
                    let (body_peer, body) = &bodies[i];

                    if !receipts_match_transaction_count(body, &receipts[i]) {
                        tracing::warn!(
                            block_number,
                            %block_hash,
                            receipt_peer = %receipt_peer,
                            transactions = body.transaction_count(),
                            receipts = receipts[i].len(),
                            "block body / receipt count mismatch — retrying from last ingested block"
                        );
                        self.peers
                            .report_invalid_block_data(receipt_peer, "receipts");
                        chunk_failed = true;
                        break;
                    }

                    if let Err(error) = validate_receipts_for_header(header, &receipts[i]) {
                        tracing::warn!(
                            block_number,
                            %block_hash,
                            receipt_peer = %receipt_peer,
                            %error,
                            "receipt validation failed — retrying from last ingested block"
                        );
                        self.peers
                            .report_invalid_block_data(receipt_peer, "receipts");
                        chunk_failed = true;
                        break;
                    }

                    let txs = assemble_txs(body, &receipts[i]);

                    if let Some(reorg) = self.head_tracker.track(header.clone()) {
                        self.handle_reorg(reorg).await?;
                    }

                    let recent_headers = self.head_tracker.snapshot();
                    self.peers
                        .cache_canonical_block(header.clone(), body.clone(), &receipts[i]);
                    let log_count = self
                        .ingest_block(header, block_hash, &txs, &recent_headers, None)
                        .await?;
                    self.progress.record_block(block_number, log_count);
                    self.note_serving_peer(header_peer, &mut newly_serving_peers);
                    self.note_serving_peer(*body_peer, &mut newly_serving_peers);
                    self.note_serving_peer(receipt_peer, &mut newly_serving_peers);
                    next_block = block_number + 1;
                    last_ingested_head = Some(execution_head(block_number, block_hash, timestamp));
                }

                if chunk_failed {
                    break;
                }
            }

            if let Some(head) = last_ingested_head {
                self.peers.set_head(head);
            }
            if !chunk_failed {
                self.last_validated_header = headers.last().cloned();
            }
            current = next_block;

            if chunk_failed {
                continue;
            }
        }

        tracing::info!(
            current_block = self.current_block(),
            target_block = self.known_target_block(),
            "polling peers for new canonical blocks"
        );
        self.run_live_sync().await
    }
}
