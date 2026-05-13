use super::*;

impl SyncEngine {
    /// Follow the chain head, ingesting new blocks as they arrive.
    pub(super) async fn run_live_sync(&mut self) -> Result<()> {
        loop {
            if cancelable(
                &mut self.shutdown,
                tokio::time::sleep(LIVE_SYNC_POLL_INTERVAL),
            )
            .await
            .is_none()
            {
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
                continue;
            }
            if let Some(target) = self.peers.highest_peer_block() {
                self.progress.set_target(target);
            }

            let current = {
                let status = self.sync_status.lock().unwrap();
                status.current_block
            };

            let (header_peer, headers) = match cancelable(
                &mut self.shutdown,
                self.peers.get_headers(current + 1, 16),
            )
            .await
            {
                Some(Ok((peer_id, headers))) if !headers.is_empty() => {
                    self.refresh_connectivity_state();
                    (peer_id, headers)
                }
                Some(Ok(_)) => {
                    if self.try_mark_synced("caught up to advertised peer tip") {
                        self.sync_status_peers();
                    } else {
                        self.refresh_connectivity_state();
                        tracing::debug!(
                            current_block = current,
                            target_block = self.known_target_block(),
                            "live head poll returned no headers while waiting for a better peer response"
                        );
                    }
                    continue;
                }
                Some(Err(e)) => {
                    self.refresh_connectivity_state();
                    tracing::debug!(error = %e, current, "live header request failed");
                    continue;
                }
                None => return self.finish_shutdown(),
            };

            if let Err(error) = validate_downloaded_headers(
                current + 1,
                self.expected_parent_for_validation(current + 1),
                &headers,
            ) {
                tracing::warn!(
                    start_block = current + 1,
                    header_peer = %header_peer,
                    %error,
                    "live header validation failed — retrying from current head"
                );
                self.peers.report_invalid_block_data(header_peer, "headers");
                self.refresh_connectivity_state();
                continue;
            }

            let hashes: Vec<B256> = headers.iter().map(|h| h.hash_slow()).collect();
            let required_block = headers
                .last()
                .map(|header| header.number())
                .unwrap_or(current + 1);
            let mut newly_serving_peers = HashSet::new();

            let bodies = match cancelable(
                &mut self.shutdown,
                self.peers
                    .get_bodies_prefer_peers(hashes.clone(), required_block, &[header_peer]),
            )
            .await
            {
                Some(Ok(bodies)) if bodies.len() == headers.len() => bodies,
                Some(Ok(_)) | Some(Err(_)) => continue,
                None => return self.finish_shutdown(),
            };
            let expected_receipt_counts: Vec<usize> = bodies
                .iter()
                .map(|(_peer_id, body)| body.transaction_count())
                .collect();
            let receipt_peer_preference = preferred_body_peers(&bodies, header_peer);
            let (receipt_peer, receipts) = match cancelable(
                &mut self.shutdown,
                self.peers.get_receipts_matching_counts_prefer_peers(
                    hashes.clone(),
                    required_block,
                    &expected_receipt_counts,
                    &receipt_peer_preference,
                ),
            )
            .await
            {
                Some(Ok((peer_id, receipts))) if receipts.len() == headers.len() => {
                    (peer_id, receipts)
                }
                Some(Ok(_)) | Some(Err(_)) => continue,
                None => return self.finish_shutdown(),
            };
            let mut batch_failed = false;
            for (i, header) in headers.iter().enumerate() {
                let block_hash = hashes[i];
                let block_number = header.number();
                let timestamp = header.timestamp();
                let (body_peer, body) = &bodies[i];

                if let Err(error) = validate_block_pre_execution(header, block_hash, body) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        body_peer = %body_peer,
                        %error,
                        "block pre-execution validation failed in live sync — retrying from current head"
                    );
                    self.peers
                        .report_invalid_block_data(*body_peer, "block bodies");
                    batch_failed = true;
                    break;
                }

                if !receipts_match_transaction_count(body, &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        transactions = body.transaction_count(),
                        receipts = receipts[i].len(),
                        "block body / receipt count mismatch in live sync — retrying from current head"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    batch_failed = true;
                    break;
                }

                if let Err(error) = validate_receipts_for_header(header, &receipts[i]) {
                    tracing::warn!(
                        block_number,
                        %block_hash,
                        receipt_peer = %receipt_peer,
                        %error,
                        "receipt validation failed in live sync — retrying from current head"
                    );
                    self.peers
                        .report_invalid_block_data(receipt_peer, "receipts");
                    batch_failed = true;
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

                self.peers
                    .set_head(execution_head(block_number, block_hash, timestamp));
            }

            if batch_failed {
                continue;
            }

            self.last_validated_header = headers.last().cloned();

            if self.try_mark_synced("caught up to advertised peer tip") {
                self.sync_status_peers();
            } else {
                self.refresh_connectivity_state();
            }
        }
    }
}
