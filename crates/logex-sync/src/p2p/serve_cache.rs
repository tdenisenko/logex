use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::{Bound, RangeBounds, RangeInclusive};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use alloy_consensus::{Block, BlockBody, Header, ReceiptWithBloom, RlpEncodableReceipt as _};
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::{Address, B256, BlockHash, BlockNumber, Bloom, TxHash, TxNumber};
use alloy_rlp::Encodable as _;
use reth_chainspec::{ChainInfo, ChainSpecProvider, MAINNET};
use reth_db_models::StoredBlockBodyIndices;
use reth_primitives_traits::{RecoveredBlock, SealedHeader};
use reth_storage_api::errors::provider::ProviderResult;
use reth_storage_api::{
    BlockBodyIndicesProvider, BlockHashReader, BlockIdReader, BlockNumReader, BlockReader,
    BlockSource, HeaderProvider, ReceiptProvider, ReceiptProviderIdExt, TransactionVariant,
    TransactionsProvider,
};

use crate::primitives::LogexReceipt;

const SERVE_CACHE_BLOCK_LIMIT: usize = 4_096;
const SERVE_CACHE_HEADER_LIMIT: usize = 8_192;
// Normalized encoded header/body/receipt weight, not resident memory. Separate
// headers, spare capacity, shared backing and outgoing copies are not charged.
const SERVE_CACHE_PAYLOAD_LIMIT: u64 = 128 * 1024 * 1024;
const P2P_UPLOAD_RATE_WINDOW: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct ServeCacheProvider {
    chain_spec: Arc<reth_chainspec::ChainSpec>,
    inner: Arc<RwLock<ServeCacheState>>,
    upload_metrics: Arc<Mutex<PayloadBandwidthWindow>>,
    payload_limit: u64,
}

#[derive(Debug, Default)]
struct ServeCacheState {
    number_to_hash: BTreeMap<u64, B256>,
    hash_to_number: HashMap<B256, u64>,
    header_number_to_hash: BTreeMap<u64, B256>,
    headers: HashMap<B256, Header>,
    blocks: HashMap<B256, CachedBlock>,
    payload_bytes: u64,
}

#[derive(Debug, Clone)]
struct CachedBlock {
    block: Block<reth_ethereum_primitives::TransactionSigned>,
    receipts: Vec<LogexReceipt>,
    payload_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct P2pUploadSnapshot {
    bytes_per_sec: u64,
    total_payload_bytes: u64,
}

#[derive(Debug, Default)]
struct PayloadBandwidthWindow {
    events: VecDeque<(Instant, u64)>,
    window_payload_bytes: u64,
    total_payload_bytes: u64,
}

impl PayloadBandwidthWindow {
    fn record(&mut self, payload_bytes: u64, now: Instant) {
        if payload_bytes == 0 {
            return;
        }
        self.events.push_back((now, payload_bytes));
        self.window_payload_bytes = self.window_payload_bytes.saturating_add(payload_bytes);
        self.total_payload_bytes = self.total_payload_bytes.saturating_add(payload_bytes);
        self.prune(now);
    }

    fn snapshot(&mut self, now: Instant) -> P2pUploadSnapshot {
        self.prune(now);
        let bytes_per_sec = self
            .events
            .front()
            .map(|(first_event_at, _)| {
                let elapsed = now
                    .saturating_duration_since(*first_event_at)
                    .max(Duration::from_secs(1))
                    .min(P2P_UPLOAD_RATE_WINDOW);
                (self.window_payload_bytes as f64 / elapsed.as_secs_f64()).round() as u64
            })
            .unwrap_or_default();

        P2pUploadSnapshot {
            bytes_per_sec,
            total_payload_bytes: self.total_payload_bytes,
        }
    }

    fn prune(&mut self, now: Instant) {
        while let Some((event_at, payload_bytes)) = self.events.front().copied() {
            if now.saturating_duration_since(event_at) <= P2P_UPLOAD_RATE_WINDOW {
                break;
            }
            self.events.pop_front();
            self.window_payload_bytes = self.window_payload_bytes.saturating_sub(payload_bytes);
        }
    }
}

fn usize_to_u64(value: usize) -> u64 {
    value.try_into().unwrap_or(u64::MAX)
}

fn header_payload_bytes(header: &Header) -> u64 {
    usize_to_u64(header.length())
}

fn headers_payload_bytes(headers: &[Header]) -> u64 {
    headers.iter().fold(0u64, |total, header| {
        total.saturating_add(header_payload_bytes(header))
    })
}

fn block_payload_bytes(block: &Block<reth_ethereum_primitives::TransactionSigned>) -> u64 {
    usize_to_u64(block.body.length())
}

fn receipts_payload_bytes(receipts: &[LogexReceipt]) -> u64 {
    receipts.iter().fold(0u64, |total, receipt| {
        total.saturating_add(receipt_payload_bytes(receipt))
    })
}

fn receipt_payload_bytes(receipt: &LogexReceipt) -> u64 {
    // Bloom is always a fixed 256-byte string. Its value does not affect
    // encoded length, so accounting need not hash every log to reconstruct it.
    usize_to_u64(receipt.rlp_encoded_length_with_bloom(&Bloom::ZERO))
}

fn cached_payload_bytes(
    header: &Header,
    body: &BlockBody<reth_ethereum_primitives::TransactionSigned, Header>,
    receipts: &[ReceiptWithBloom<LogexReceipt>],
) -> u64 {
    receipts.iter().fold(
        header_payload_bytes(header).saturating_add(usize_to_u64(body.length())),
        |total, receipt| total.saturating_add(receipt_payload_bytes(&receipt.receipt)),
    )
}

// Returns whether replacing this header removed a cached body and its receipts.
fn insert_header_inner(state: &mut ServeCacheState, hash: B256, header: Header) -> bool {
    let number = header.number;
    let body_changed = state
        .number_to_hash
        .get(&number)
        .is_some_and(|old| *old != hash);
    if body_changed && let Some(old_hash) = state.number_to_hash.get(&number).copied() {
        state.remove_block(&old_hash);
    }
    if let Some(old_hash) = state.header_number_to_hash.insert(number, hash)
        && old_hash != hash
    {
        state.headers.remove(&old_hash);
    }
    state.headers.insert(hash, header);

    while state.headers.len() > SERVE_CACHE_HEADER_LIMIT {
        let Some((&evict_number, &evict_hash)) = state.header_number_to_hash.iter().next() else {
            break;
        };
        state.header_number_to_hash.remove(&evict_number);
        state.headers.remove(&evict_hash);
    }
    body_changed
}

fn number_range(range: impl RangeBounds<BlockNumber>) -> Option<RangeInclusive<BlockNumber>> {
    let start = match range.start_bound() {
        Bound::Included(value) => *value,
        Bound::Excluded(value) => value.checked_add(1)?,
        Bound::Unbounded => 0,
    };
    let end = match range.end_bound() {
        Bound::Included(value) => *value,
        Bound::Excluded(value) => value.checked_sub(1)?,
        Bound::Unbounded => BlockNumber::MAX,
    };
    (start <= end).then_some(start..=end)
}

impl ServeCacheState {
    fn remove_block(&mut self, hash: &B256) {
        if let Some(cached) = self.blocks.remove(hash) {
            self.payload_bytes -= cached.payload_bytes;
        }
        if let Some(number) = self.hash_to_number.remove(hash)
            && self.number_to_hash.get(&number) == Some(hash)
        {
            self.number_to_hash.remove(&number);
        }
    }

    // Only older entries may be evicted for a candidate. Check without mutating
    // so an optional candidate that cannot remain does not evict useful data.
    fn can_retain_block(&self, number: u64, payload_bytes: u64, limit: u64) -> bool {
        if payload_bytes > limit {
            return false;
        }
        let replaced = self
            .number_to_hash
            .get(&number)
            .and_then(|hash| self.blocks.get(hash));
        let mut count = self.blocks.len() + 1 - usize::from(replaced.is_some());
        let Some(mut total) = (self.payload_bytes
            - replaced.map_or(0, |cached| cached.payload_bytes))
        .checked_add(payload_bytes) else {
            return false;
        };
        if count <= SERVE_CACHE_BLOCK_LIMIT && total <= limit {
            return true;
        }
        for (_, hash) in self.number_to_hash.range(..number) {
            let cached = self.blocks.get(hash).expect("cached body mapping exists");
            count -= 1;
            total -= cached.payload_bytes;
            if count <= SERVE_CACHE_BLOCK_LIMIT && total <= limit {
                return true;
            }
        }
        false
    }

    fn header(&self, hash: &B256) -> Option<&Header> {
        self.headers
            .get(hash)
            .or_else(|| self.blocks.get(hash).map(|cached| &cached.block.header))
    }

    fn header_hash(&self, number: u64) -> Option<B256> {
        self.header_number_to_hash
            .get(&number)
            .or_else(|| self.number_to_hash.get(&number))
            .copied()
    }

    // Both maps are ordered. Prefer the header inventory at an equal height and
    // visit only retained entries, including bodies whose header entry was evicted.
    fn header_hashes_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> impl Iterator<Item = (u64, B256)> + '_ {
        let mut headers = self.header_number_to_hash.range(range.clone()).peekable();
        let mut blocks = self.number_to_hash.range(range).peekable();
        std::iter::from_fn(move || {
            let entry = match (headers.peek(), blocks.peek()) {
                (Some((header_number, _)), Some((block_number, _))) => {
                    match header_number.cmp(block_number) {
                        std::cmp::Ordering::Less => headers.next(),
                        std::cmp::Ordering::Equal => {
                            blocks.next();
                            headers.next()
                        }
                        std::cmp::Ordering::Greater => blocks.next(),
                    }
                }
                (Some(_), None) => headers.next(),
                (None, Some(_)) => blocks.next(),
                (None, None) => None,
            };
            entry.map(|(number, hash)| (*number, *hash))
        })
    }
}

impl ServeCacheProvider {
    pub fn new() -> Self {
        Self {
            chain_spec: MAINNET.clone(),
            inner: Arc::new(RwLock::new(ServeCacheState::default())),
            upload_metrics: Arc::new(Mutex::new(PayloadBandwidthWindow::default())),
            payload_limit: SERVE_CACHE_PAYLOAD_LIMIT,
        }
    }

    pub fn p2p_upload_snapshot(&self) -> (u64, u64) {
        let snapshot = self
            .upload_metrics
            .lock()
            .expect("serve cache upload metrics poisoned")
            .snapshot(Instant::now());
        (snapshot.bytes_per_sec, snapshot.total_payload_bytes)
    }

    pub fn record_p2p_upload_payload(&self, payload_bytes: u64) {
        self.upload_metrics
            .lock()
            .expect("serve cache upload metrics poisoned")
            .record(payload_bytes, Instant::now());
    }

    pub fn insert_block(
        &self,
        header: &Header,
        body: &BlockBody<reth_ethereum_primitives::TransactionSigned, Header>,
        receipts: &[ReceiptWithBloom<LogexReceipt>],
    ) {
        let hash = header.hash_slow();
        let number = header.number;
        // Avoid even a length walk for historical entries that the count cap
        // already excludes. Header publication still happens below.
        let too_old = {
            let state = self.inner.read().expect("serve cache poisoned");
            state.blocks.len() >= SERVE_CACHE_BLOCK_LIMIT
                && state
                    .number_to_hash
                    .first_key_value()
                    .is_some_and(|(&oldest, _)| number < oldest)
        };
        let cached = if too_old {
            None
        } else {
            let payload_bytes = cached_payload_bytes(header, body, receipts);
            let admissible = payload_bytes <= self.payload_limit
                && self
                    .inner
                    .read()
                    .expect("serve cache poisoned")
                    .can_retain_block(number, payload_bytes, self.payload_limit);
            // Keep length walks and payload copies outside the write guard.
            // Concurrent changes may invalidate this optional preflight; the
            // write-side recheck below alone determines retained accounting.
            admissible.then(|| CachedBlock {
                block: Block::new(header.clone(), body.clone()),
                receipts: receipts
                    .iter()
                    .map(|receipt| receipt.receipt.clone())
                    .collect(),
                payload_bytes,
            })
        };

        let mut state = self.inner.write().expect("serve cache poisoned");
        insert_header_inner(&mut state, hash, header.clone());
        let Some(cached) = cached else {
            return;
        };
        if !state.can_retain_block(number, cached.payload_bytes, self.payload_limit) {
            return;
        }
        state.remove_block(&hash);
        while state.blocks.len() >= SERVE_CACHE_BLOCK_LIMIT
            || state.payload_bytes > self.payload_limit - cached.payload_bytes
        {
            let (&evict_number, &evict_hash) = state
                .number_to_hash
                .first_key_value()
                .expect("admitted cache entry has an eviction candidate");
            debug_assert!(evict_number < number);
            state.remove_block(&evict_hash);
        }
        state.number_to_hash.insert(number, hash);
        state.hash_to_number.insert(hash, number);
        state.payload_bytes += cached.payload_bytes;
        state.blocks.insert(hash, cached);
    }

    /// Returns whether canonical replacement removed any cached bodies.
    pub fn insert_headers(&self, headers: impl IntoIterator<Item = Header>) -> bool {
        let mut state = self.inner.write().expect("serve cache poisoned");
        let mut bodies_changed = false;
        for header in headers {
            bodies_changed |= insert_header_inner(&mut state, header.hash_slow(), header);
        }
        bodies_changed
    }

    pub fn remove_blocks(&self, reverted_hashes: &[B256]) {
        if reverted_hashes.is_empty() {
            return;
        }

        let mut state = self.inner.write().expect("serve cache poisoned");
        for hash in reverted_hashes {
            state.remove_block(hash);
            if let Some(header) = state.headers.remove(hash)
                && state.header_number_to_hash.get(&header.number) == Some(hash)
            {
                state.header_number_to_hash.remove(&header.number);
            }
        }
    }

    pub fn advertised_history_range(&self) -> Option<(u64, u64, B256)> {
        let state = self.inner.read().expect("serve cache poisoned");
        let (&latest, &latest_hash) = state.number_to_hash.iter().next_back()?;
        let mut earliest = latest;
        while earliest > 0 && state.number_to_hash.contains_key(&(earliest - 1)) {
            earliest -= 1;
        }
        Some((earliest, latest, latest_hash))
    }

    fn chain_info_inner(&self) -> ChainInfo {
        let state = self.inner.read().expect("serve cache poisoned");
        if let Some((&best_number, &best_hash)) = state.number_to_hash.iter().next_back() {
            ChainInfo {
                best_hash,
                best_number,
            }
        } else {
            ChainInfo {
                best_hash: self.chain_spec.genesis_hash(),
                best_number: 0,
            }
        }
    }

    fn header_hash_for_number(&self, number: u64) -> Option<B256> {
        self.inner
            .read()
            .expect("serve cache poisoned")
            .header_hash(number)
            .or_else(|| (number == 0).then(|| self.chain_spec.genesis_hash()))
    }

    fn header_by_hash_inner(&self, hash: B256) -> Option<Header> {
        let state = self.inner.read().expect("serve cache poisoned");
        state.header(&hash).cloned().or_else(|| {
            (hash == self.chain_spec.genesis_hash())
                .then(|| self.chain_spec.genesis_header().clone())
        })
    }

    fn header_by_number_inner(&self, number: u64) -> Option<(B256, Header)> {
        let state = self.inner.read().expect("serve cache poisoned");
        if let Some(hash) = state.header_hash(number) {
            return state.header(&hash).map(|header| (hash, header.clone()));
        }
        (number == 0).then(|| {
            (
                self.chain_spec.genesis_hash(),
                self.chain_spec.genesis_header().clone(),
            )
        })
    }

    // These mapping closures are private field projections. External predicates
    // run only after the owned snapshot has released the cache guard.
    fn map_headers_range<T>(
        &self,
        range: impl RangeBounds<BlockNumber>,
        mut map: impl FnMut(B256, &Header) -> T,
    ) -> Vec<T> {
        let Some(range) = number_range(range) else {
            return Vec::new();
        };
        let state = self.inner.read().expect("serve cache poisoned");
        let mut result = Vec::new();
        if range.contains(&0) && state.header_hash(0).is_none() {
            result.push(map(
                self.chain_spec.genesis_hash(),
                self.chain_spec.genesis_header(),
            ));
        }
        result.extend(
            state
                .header_hashes_range(range)
                .filter_map(|(_, hash)| state.header(&hash).map(|header| map(hash, header))),
        );
        result
    }

    fn genesis_block(&self) -> CachedBlock {
        CachedBlock {
            block: Block::new(
                self.chain_spec.genesis_header().clone(),
                BlockBody::default(),
            ),
            receipts: Vec::new(),
            payload_bytes: 0, // Ephemeral genesis fallback is not retained.
        }
    }

    fn map_block<T>(
        &self,
        id: BlockHashOrNumber,
        map: impl FnOnce(&CachedBlock) -> T,
    ) -> Option<T> {
        let state = self.inner.read().expect("serve cache poisoned");
        let hash = match id {
            BlockHashOrNumber::Hash(hash) => hash,
            BlockHashOrNumber::Number(number) => state
                .number_to_hash
                .get(&number)
                .copied()
                .or_else(|| (number == 0).then(|| self.chain_spec.genesis_hash()))?,
        };
        if let Some(cached) = state.blocks.get(&hash) {
            return Some(map(cached));
        }
        (hash == self.chain_spec.genesis_hash()).then(|| map(&self.genesis_block()))
    }

    fn map_blocks_range<T>(
        &self,
        range: impl RangeBounds<BlockNumber>,
        mut map: impl FnMut(&CachedBlock) -> T,
    ) -> Vec<T> {
        let Some(range) = number_range(range) else {
            return Vec::new();
        };
        let state = self.inner.read().expect("serve cache poisoned");
        let mut result = Vec::new();
        if range.contains(&0) && !state.number_to_hash.contains_key(&0) {
            result.push(map(&self.genesis_block()));
        }
        result.extend(
            state
                .number_to_hash
                .range(range)
                .filter_map(|(_, hash)| state.blocks.get(hash).map(&mut map)),
        );
        result
    }
}

impl Default for ServeCacheProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl ChainSpecProvider for ServeCacheProvider {
    type ChainSpec = reth_chainspec::ChainSpec;

    fn chain_spec(&self) -> Arc<Self::ChainSpec> {
        Arc::clone(&self.chain_spec)
    }
}

impl BlockHashReader for ServeCacheProvider {
    fn block_hash(&self, number: u64) -> ProviderResult<Option<B256>> {
        Ok(self.header_hash_for_number(number))
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        let Some(range) = number_range(start..end) else {
            return Ok(Vec::new());
        };
        let state = self.inner.read().expect("serve cache poisoned");
        Ok(state
            .header_hashes_range(range)
            .map(|(_, hash)| hash)
            .collect())
    }
}

impl BlockNumReader for ServeCacheProvider {
    fn chain_info(&self) -> ProviderResult<ChainInfo> {
        Ok(self.chain_info_inner())
    }

    fn best_block_number(&self) -> ProviderResult<BlockNumber> {
        Ok(self.chain_info_inner().best_number)
    }

    fn last_block_number(&self) -> ProviderResult<BlockNumber> {
        Ok(self.chain_info_inner().best_number)
    }

    fn block_number(&self, hash: B256) -> ProviderResult<Option<BlockNumber>> {
        let state = self.inner.read().expect("serve cache poisoned");
        Ok(state
            .hash_to_number
            .get(&hash)
            .copied()
            .or_else(|| state.headers.get(&hash).map(|header| header.number))
            .or_else(|| (hash == self.chain_spec.genesis_hash()).then_some(0)))
    }
}

impl BlockIdReader for ServeCacheProvider {
    fn pending_block_num_hash(&self) -> ProviderResult<Option<alloy_eips::BlockNumHash>> {
        Ok(None)
    }

    fn safe_block_num_hash(&self) -> ProviderResult<Option<alloy_eips::BlockNumHash>> {
        Ok(None)
    }

    fn finalized_block_num_hash(&self) -> ProviderResult<Option<alloy_eips::BlockNumHash>> {
        Ok(None)
    }
}

impl HeaderProvider for ServeCacheProvider {
    type Header = Header;

    fn header(&self, block_hash: BlockHash) -> ProviderResult<Option<Self::Header>> {
        let header = self.header_by_hash_inner(block_hash);
        if let Some(header) = &header {
            self.record_p2p_upload_payload(header_payload_bytes(header));
        }
        Ok(header)
    }

    fn header_by_number(&self, num: u64) -> ProviderResult<Option<Self::Header>> {
        let header = self.header_by_number_inner(num).map(|(_, header)| header);
        if let Some(header) = &header {
            self.record_p2p_upload_payload(header_payload_bytes(header));
        }
        Ok(header)
    }

    fn headers_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Self::Header>> {
        let headers = self.map_headers_range(range, |_, header| header.clone());
        self.record_p2p_upload_payload(headers_payload_bytes(&headers));
        Ok(headers)
    }

    fn sealed_header(
        &self,
        number: BlockNumber,
    ) -> ProviderResult<Option<SealedHeader<Self::Header>>> {
        let header = self.header_by_number_inner(number);
        if let Some((_, header)) = &header {
            self.record_p2p_upload_payload(header_payload_bytes(header));
        }
        Ok(header.map(|(hash, header)| SealedHeader::new(header, hash)))
    }

    fn sealed_headers_while(
        &self,
        range: impl RangeBounds<BlockNumber>,
        mut predicate: impl FnMut(&SealedHeader<Self::Header>) -> bool,
    ) -> ProviderResult<Vec<SealedHeader<Self::Header>>> {
        let snapshot = self.map_headers_range(range, |hash, header| {
            SealedHeader::new(header.clone(), hash)
        });
        Ok(snapshot
            .into_iter()
            .take_while(|sealed| {
                self.record_p2p_upload_payload(header_payload_bytes(sealed.header()));
                predicate(sealed)
            })
            .collect())
    }
}

impl BlockBodyIndicesProvider for ServeCacheProvider {
    fn block_body_indices(&self, num: u64) -> ProviderResult<Option<StoredBlockBodyIndices>> {
        Ok(self.map_block(num.into(), |cached| StoredBlockBodyIndices {
            first_tx_num: 0,
            tx_count: usize_to_u64(cached.block.body.transactions.len()),
        }))
    }

    fn block_body_indices_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<StoredBlockBodyIndices>> {
        Ok(
            self.map_blocks_range(range, |cached| StoredBlockBodyIndices {
                first_tx_num: 0,
                tx_count: usize_to_u64(cached.block.body.transactions.len()),
            }),
        )
    }
}

impl TransactionsProvider for ServeCacheProvider {
    type Transaction = reth_ethereum_primitives::TransactionSigned;

    fn transaction_id(&self, _tx_hash: TxHash) -> ProviderResult<Option<TxNumber>> {
        Ok(None)
    }

    fn transaction_by_id(&self, _id: TxNumber) -> ProviderResult<Option<Self::Transaction>> {
        Ok(None)
    }

    fn transaction_by_id_unhashed(
        &self,
        _id: TxNumber,
    ) -> ProviderResult<Option<Self::Transaction>> {
        Ok(None)
    }

    fn transaction_by_hash(&self, hash: TxHash) -> ProviderResult<Option<Self::Transaction>> {
        let state = self.inner.read().expect("serve cache poisoned");
        Ok(state
            .blocks
            .values()
            .flat_map(|block| block.block.body.transactions.iter())
            .find(|tx| *tx.tx_hash() == hash)
            .cloned())
    }

    fn transaction_by_hash_with_meta(
        &self,
        _hash: TxHash,
    ) -> ProviderResult<
        Option<(
            Self::Transaction,
            alloy_consensus::transaction::TransactionMeta,
        )>,
    > {
        Ok(None)
    }

    fn transactions_by_block(
        &self,
        block: BlockHashOrNumber,
    ) -> ProviderResult<Option<Vec<Self::Transaction>>> {
        let transactions = self.map_block(block, |cached| cached.block.body.transactions.clone());
        if let Some(transactions) = &transactions {
            let payload_bytes = transactions.iter().fold(0u64, |total, tx| {
                total.saturating_add(usize_to_u64(tx.length()))
            });
            self.record_p2p_upload_payload(payload_bytes);
        }
        Ok(transactions)
    }

    fn transactions_by_block_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Vec<Self::Transaction>>> {
        let transactions =
            self.map_blocks_range(range, |cached| cached.block.body.transactions.clone());
        let payload_bytes = transactions.iter().flatten().fold(0u64, |total, tx| {
            total.saturating_add(usize_to_u64(tx.length()))
        });
        self.record_p2p_upload_payload(payload_bytes);
        Ok(transactions)
    }

    fn transactions_by_tx_range(
        &self,
        _range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Self::Transaction>> {
        Ok(Vec::new())
    }

    fn senders_by_tx_range(
        &self,
        _range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Address>> {
        Ok(Vec::new())
    }

    fn transaction_sender(&self, _id: TxNumber) -> ProviderResult<Option<Address>> {
        Ok(None)
    }
}

impl ReceiptProvider for ServeCacheProvider {
    type Receipt = LogexReceipt;

    fn receipt(&self, _id: TxNumber) -> ProviderResult<Option<Self::Receipt>> {
        Ok(None)
    }

    fn receipt_by_hash(&self, _hash: TxHash) -> ProviderResult<Option<Self::Receipt>> {
        Ok(None)
    }

    fn receipts_by_block(
        &self,
        block: BlockHashOrNumber,
    ) -> ProviderResult<Option<Vec<Self::Receipt>>> {
        let receipts = self.map_block(block, |cached| cached.receipts.clone());
        if let Some(receipts) = &receipts {
            self.record_p2p_upload_payload(receipts_payload_bytes(receipts));
        }
        Ok(receipts)
    }

    fn receipts_by_tx_range(
        &self,
        _range: impl RangeBounds<TxNumber>,
    ) -> ProviderResult<Vec<Self::Receipt>> {
        Ok(Vec::new())
    }

    fn receipts_by_block_range(
        &self,
        block_range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<Vec<Self::Receipt>>> {
        let receipts = self.map_blocks_range(block_range, |cached| cached.receipts.clone());
        let payload_bytes = receipts.iter().fold(0u64, |total, block| {
            total.saturating_add(receipts_payload_bytes(block))
        });
        self.record_p2p_upload_payload(payload_bytes);
        Ok(receipts)
    }
}

impl ReceiptProviderIdExt for ServeCacheProvider {}

impl BlockReader for ServeCacheProvider {
    type Block = reth_ethereum_primitives::Block;

    fn find_block_by_hash(
        &self,
        hash: B256,
        source: BlockSource,
    ) -> ProviderResult<Option<Self::Block>> {
        if !source.is_canonical() {
            return Ok(None);
        }
        Ok(self.map_block(hash.into(), |cached| cached.block.clone()))
    }

    fn block(&self, id: BlockHashOrNumber) -> ProviderResult<Option<Self::Block>> {
        let block = self.map_block(id, |cached| cached.block.clone());
        if let Some(block) = &block {
            self.record_p2p_upload_payload(block_payload_bytes(block));
        }
        Ok(block)
    }

    fn pending_block(&self) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        Ok(None)
    }

    fn pending_block_and_receipts(
        &self,
    ) -> ProviderResult<Option<(RecoveredBlock<Self::Block>, Vec<Self::Receipt>)>> {
        Ok(None)
    }

    fn recovered_block(
        &self,
        _id: BlockHashOrNumber,
        _transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        Ok(None)
    }

    fn sealed_block_with_senders(
        &self,
        _id: BlockHashOrNumber,
        _transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<RecoveredBlock<Self::Block>>> {
        Ok(None)
    }

    fn block_range(&self, range: RangeInclusive<BlockNumber>) -> ProviderResult<Vec<Self::Block>> {
        let blocks = self.map_blocks_range(range, |cached| cached.block.clone());
        let payload_bytes = blocks.iter().fold(0u64, |total, block| {
            total.saturating_add(block_payload_bytes(block))
        });
        self.record_p2p_upload_payload(payload_bytes);
        Ok(blocks)
    }

    fn block_with_senders_range(
        &self,
        _range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<RecoveredBlock<Self::Block>>> {
        Ok(Vec::new())
    }

    fn recovered_block_range(
        &self,
        _range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<RecoveredBlock<Self::Block>>> {
        Ok(Vec::new())
    }

    fn block_by_transaction_id(&self, _id: TxNumber) -> ProviderResult<Option<BlockNumber>> {
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{Eip658Value, TxReceipt as _, TxType};
    use alloy_primitives::{Log, LogData};

    fn test_block(
        number: u64,
    ) -> (
        Header,
        BlockBody<reth_ethereum_primitives::TransactionSigned, Header>,
    ) {
        let header = Header {
            number,
            ..Default::default()
        };
        let body = BlockBody {
            transactions: Vec::new(),
            ommers: Vec::new(),
            withdrawals: None,
        };
        (header, body)
    }

    fn test_receipt() -> ReceiptWithBloom<LogexReceipt> {
        ReceiptWithBloom {
            receipt: LogexReceipt {
                tx_type: TxType::Legacy,
                status: Eip658Value::success(),
                cumulative_gas_used: 21_000,
                logs: vec![Log {
                    address: Address::ZERO,
                    data: LogData::default(),
                }],
            },
            logs_bloom: Default::default(),
        }
    }

    #[test]
    fn served_payload_sizes_use_encoded_fields() {
        use alloy_consensus::{SignableTransaction, TxLegacy};
        use alloy_primitives::{Signature, U256};

        let (header, mut body) = test_block(2);
        body.ommers.push(Header::default());
        body.transactions.push(
            TxLegacy::default()
                .into_signed(Signature::new(U256::from(1), U256::from(2), false))
                .into(),
        );
        let block = Block::new(header, body);
        assert_eq!(
            block_payload_bytes(&block),
            alloy_rlp::encode(&block.body).len() as u64
        );

        let receipts = [TxType::Legacy, TxType::Eip1559].map(|tx_type| {
            let mut receipt = test_receipt().receipt;
            receipt.tx_type = tx_type;
            receipt.status = if tx_type.is_legacy() {
                Eip658Value::PostState(B256::repeat_byte(3))
            } else {
                Eip658Value::success()
            };
            receipt
        });
        let expected = receipts
            .iter()
            .map(|receipt| {
                let mut bytes = Vec::new();
                receipt.rlp_encode_with_bloom(&receipt.bloom(), &mut bytes);
                bytes.len() as u64
            })
            .sum::<u64>();
        assert_eq!(receipts_payload_bytes(&receipts), expected);

        let transaction_bytes = block
            .body
            .transactions
            .iter()
            .map(|tx| alloy_rlp::encode(tx).len() as u64)
            .sum::<u64>();
        let provider = ServeCacheProvider::new();
        provider.insert_block(&block.header, &block.body, &[]);
        assert_eq!(
            provider.transactions_by_block(2.into()).unwrap(),
            Some(block.body.transactions.clone())
        );
        assert_eq!(provider.p2p_upload_snapshot().1, transaction_bytes);
        assert_eq!(
            provider.transactions_by_block_range(2..=2).unwrap(),
            vec![block.body.transactions]
        );
        assert_eq!(provider.p2p_upload_snapshot().1, transaction_bytes * 2);
    }

    #[test]
    fn serves_cached_block_by_hash_and_number() {
        let provider = ServeCacheProvider::new();
        let (header, body) = test_block(7);
        let hash = header.hash_slow();
        provider.insert_block(&header, &body, &[test_receipt()]);

        assert_eq!(provider.block_hash(7).unwrap(), Some(hash));
        assert_eq!(provider.header(hash).unwrap().unwrap().number, 7);
        assert_eq!(
            provider.header_by_number(7).unwrap().unwrap().hash_slow(),
            hash
        );
        assert_eq!(
            provider
                .receipts_by_block(hash.into())
                .unwrap()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            provider.block_by_number(7).unwrap().unwrap().header.number,
            7
        );
    }

    #[test]
    fn p2p_upload_snapshot_reports_recent_payloads() {
        let provider = ServeCacheProvider::new();

        assert_eq!(provider.p2p_upload_snapshot(), (0, 0));

        provider.record_p2p_upload_payload(1_024);
        let (bytes_per_sec, total_payload_bytes) = provider.p2p_upload_snapshot();

        assert_eq!(total_payload_bytes, 1_024);
        assert!(bytes_per_sec >= 1_024);
    }

    #[test]
    fn serves_genesis_fallback_when_cache_is_empty() {
        let provider = ServeCacheProvider::new();
        let genesis_hash = provider.chain_spec.genesis_hash();

        assert_eq!(provider.block_hash(0).unwrap(), Some(genesis_hash));
        assert_eq!(provider.block_number(genesis_hash).unwrap(), Some(0));
        assert_eq!(
            provider.header_by_number(0).unwrap().unwrap().hash_slow(),
            genesis_hash
        );
        assert_eq!(
            provider
                .block_by_hash(genesis_hash)
                .unwrap()
                .unwrap()
                .header
                .number,
            0
        );
        assert!(
            provider
                .receipts_by_block(genesis_hash.into())
                .unwrap()
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn serves_restored_headers_without_advertising_bodies() {
        let provider = ServeCacheProvider::new();
        let (header, _body) = test_block(17);
        let hash = header.hash_slow();

        provider.insert_headers(vec![header.clone()]);

        assert_eq!(provider.block_hash(17).unwrap(), Some(hash));
        assert_eq!(provider.header(hash).unwrap().unwrap().number, 17);
        assert_eq!(
            provider.header_by_number(17).unwrap().unwrap().hash_slow(),
            hash
        );
        assert!(provider.block_by_number(17).unwrap().is_none());
        assert!(provider.receipts_by_block(hash.into()).unwrap().is_none());
        assert_eq!(provider.advertised_history_range(), None);
    }

    #[test]
    fn removes_reorged_hashes_from_cache() {
        let provider = ServeCacheProvider::new();
        let (header, body) = test_block(11);
        let hash = header.hash_slow();
        provider.insert_block(&header, &body, &[test_receipt()]);

        provider.remove_blocks(&[hash]);

        assert!(provider.header(hash).unwrap().is_none());
        assert!(provider.block_hash(11).unwrap().is_none());
        assert!(provider.receipts_by_block(hash.into()).unwrap().is_none());
    }

    #[test]
    fn advertised_history_range_tracks_cached_window() {
        let provider = ServeCacheProvider::new();
        assert_eq!(provider.advertised_history_range(), None);

        let (first_header, first_body) = test_block(11);
        let (second_header, second_body) = test_block(12);
        provider.insert_block(&first_header, &first_body, &[test_receipt()]);
        provider.insert_block(&second_header, &second_body, &[test_receipt()]);

        assert_eq!(
            provider.advertised_history_range(),
            Some((11, 12, second_header.hash_slow()))
        );
    }

    #[test]
    fn advertised_history_range_does_not_claim_gaps() {
        let provider = ServeCacheProvider::new();
        let (low_header, low_body) = test_block(10);
        let (tip_parent_header, tip_parent_body) = test_block(20);
        let (tip_header, tip_body) = test_block(21);
        provider.insert_block(&low_header, &low_body, &[test_receipt()]);
        provider.insert_block(&tip_parent_header, &tip_parent_body, &[test_receipt()]);
        provider.insert_block(&tip_header, &tip_body, &[test_receipt()]);

        assert_eq!(
            provider.advertised_history_range(),
            Some((20, 21, tip_header.hash_slow()))
        );
    }

    #[test]
    fn cache_eviction_preserves_recent_advertised_window() {
        let provider = ServeCacheProvider::new();
        let first_recent_block = 10_000;
        let last_recent_block = first_recent_block + SERVE_CACHE_BLOCK_LIMIT as u64 - 1;
        let mut tip_hash = B256::ZERO;

        for number in first_recent_block..=last_recent_block {
            let (header, body) = test_block(number);
            tip_hash = header.hash_slow();
            provider.insert_block(&header, &body, &[test_receipt()]);
        }

        let (historical_header, historical_body) = test_block(1_000);
        let historical_hash = historical_header.hash_slow();
        provider.insert_block(&historical_header, &historical_body, &[test_receipt()]);

        assert!(provider.block_by_number(1_000).unwrap().is_none());
        assert!(provider.block_hash(first_recent_block).unwrap().is_some());
        assert_eq!(
            provider.advertised_history_range(),
            Some((first_recent_block, last_recent_block, tip_hash))
        );
        assert!(provider.header(historical_hash).unwrap().is_some());
        provider.remove_blocks(&[historical_hash]);
        assert!(provider.header(historical_hash).unwrap().is_none());
        assert!(provider.block_hash(1_000).unwrap().is_none());
        assert_eq!(
            provider.advertised_history_range(),
            Some((first_recent_block, last_recent_block, tip_hash))
        );
    }
    #[test]
    fn restored_header_reorg_removes_all_header_indexes() {
        let provider = ServeCacheProvider::new();
        let (header, _) = test_block(17);
        let (neighbor, _) = test_block(18);
        let hash = header.hash_slow();
        provider.insert_headers([header, neighbor.clone()]);
        provider.remove_blocks(&[hash]);
        assert!(provider.header(hash).unwrap().is_none());
        assert!(provider.header_by_number(17).unwrap().is_none());
        assert!(provider.block_hash(17).unwrap().is_none());
        assert!(provider.block_number(hash).unwrap().is_none());
        assert_eq!(provider.header_by_number(18).unwrap(), Some(neighbor));
        assert_eq!(provider.advertised_history_range(), None);
    }

    #[test]
    fn header_replacement_invalidates_conflicting_body_and_receipts() {
        let provider = ServeCacheProvider::new();
        let (old, body) = test_block(17);
        let old_hash = old.hash_slow();
        provider.insert_block(&old, &body, &[test_receipt()]);
        let mut replacement = old;
        replacement.timestamp = 1;
        let replacement_hash = replacement.hash_slow();
        provider.insert_headers([replacement.clone()]);
        assert_eq!(provider.block_hash(17).unwrap(), Some(replacement_hash));
        assert_eq!(
            provider.header_by_number(17).unwrap(),
            Some(replacement.clone())
        );
        assert!(provider.block_by_number(17).unwrap().is_none());
        assert!(provider.block_by_hash(old_hash).unwrap().is_none());
        assert!(provider.receipts_by_block(17.into()).unwrap().is_none());
        assert!(
            provider
                .receipts_by_block(old_hash.into())
                .unwrap()
                .is_none()
        );
        assert!(provider.header(old_hash).unwrap().is_none());
        assert!(provider.block_number(old_hash).unwrap().is_none());
        assert_eq!(provider.advertised_history_range(), None);
        // A delayed invalidation of the old identity must not erase its successor.
        provider.remove_blocks(&[old_hash]);
        assert_eq!(provider.header_by_number(17).unwrap(), Some(replacement));
    }

    #[test]
    fn same_header_reinsertion_preserves_body_availability() {
        let provider = ServeCacheProvider::new();
        let (header, body) = test_block(17);
        let hash = header.hash_slow();
        provider.insert_block(&header, &body, &[test_receipt()]);
        provider.insert_headers([header.clone()]);
        assert_eq!(
            provider.block_by_number(17).unwrap(),
            Some(Block::new(header, body))
        );
        assert_eq!(
            provider.receipts_by_block(hash.into()).unwrap(),
            Some(vec![test_receipt().receipt])
        );
        assert_eq!(provider.advertised_history_range(), Some((17, 17, hash)));
    }

    #[test]
    fn sealed_header_supports_restored_header_only_entries() {
        let provider = ServeCacheProvider::new();
        let (header, _) = test_block(17);
        provider.insert_headers([header.clone()]);
        let sealed = provider
            .sealed_header(17)
            .unwrap()
            .expect("restored header");
        assert_eq!(sealed.hash(), header.hash_slow());
        assert_eq!(sealed.header(), &header);
    }

    #[test]
    fn sealed_header_hash_matches_replaced_header() {
        let provider = ServeCacheProvider::new();
        let (old, body) = test_block(17);
        provider.insert_block(&old, &body, &[]);
        let mut replacement = old;
        replacement.timestamp = 1;
        provider.insert_headers([replacement.clone()]);
        let sealed = provider
            .sealed_header(17)
            .unwrap()
            .expect("replacement header");
        assert_eq!(sealed.header(), &replacement);
        assert_eq!(sealed.hash(), replacement.hash_slow());
    }

    #[test]
    fn exclusive_zero_header_range_is_empty_but_inclusive_genesis_is_present() {
        let provider = ServeCacheProvider::new();
        assert!(provider.headers_range(..0).unwrap().is_empty());
        assert!(
            provider
                .sealed_headers_while(..0, |_| true)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            provider.headers_range(0..=0).unwrap(),
            vec![provider.chain_spec.genesis_header().clone()]
        );
        assert!(provider.canonical_hashes_range(0, 1).unwrap().is_empty());
    }

    #[test]
    fn unbounded_header_ranges_include_sparse_restored_inventory() {
        let provider = ServeCacheProvider::new();
        provider.insert_headers([test_block(17).0, test_block(4).0]);
        assert_eq!(
            provider
                .headers_range(..)
                .unwrap()
                .iter()
                .map(|h| h.number)
                .collect::<Vec<_>>(),
            vec![0, 4, 17]
        );
        assert_eq!(
            provider
                .headers_range(5..)
                .unwrap()
                .iter()
                .map(|h| h.number)
                .collect::<Vec<_>>(),
            vec![17]
        );
        assert_eq!(
            provider
                .sealed_headers_while(.., |_| true)
                .unwrap()
                .iter()
                .map(|h| h.number)
                .collect::<Vec<_>>(),
            vec![0, 4, 17]
        );
        // Header-only availability must not invent body/transaction availability.
        assert!(
            provider
                .transactions_by_block_range(1..)
                .unwrap()
                .is_empty()
        );
        assert_eq!(provider.advertised_history_range(), None);
    }

    #[test]
    fn header_and_transaction_ranges_match_independent_bound_filter() {
        let provider = ServeCacheProvider::new();
        for number in [2, 6] {
            let (header, body) = test_block(number);
            provider.insert_block(&header, &body, &[]);
        }
        provider.insert_headers([test_block(4).0]);
        let bounds = [
            (Bound::Unbounded, Bound::Unbounded),
            (Bound::Included(2), Bound::Included(4)),
            (Bound::Excluded(2), Bound::Excluded(6)),
            (Bound::Unbounded, Bound::Excluded(0)),
            (Bound::Excluded(u64::MAX), Bound::Included(u64::MAX)),
            (Bound::Included(6), Bound::Included(2)),
            (Bound::Included(0), Bound::Included(0)),
        ];
        for range in bounds {
            let includes = |number: &u64| {
                let lower = match range.0 {
                    Bound::Unbounded => true,
                    Bound::Included(n) => *number >= n,
                    Bound::Excluded(n) => *number > n,
                };
                let upper = match range.1 {
                    Bound::Unbounded => true,
                    Bound::Included(n) => *number <= n,
                    Bound::Excluded(n) => *number < n,
                };
                lower && upper
            };
            let expected_headers = [0, 2, 4, 6]
                .into_iter()
                .filter(includes)
                .collect::<Vec<_>>();
            assert_eq!(
                provider
                    .headers_range(range)
                    .unwrap()
                    .iter()
                    .map(|h| h.number)
                    .collect::<Vec<_>>(),
                expected_headers,
                "header bounds {range:?}"
            );
            let expected_bodies = [0, 2, 6].into_iter().filter(includes).count();
            assert_eq!(
                provider.transactions_by_block_range(range).unwrap().len(),
                expected_bodies,
                "transaction bounds {range:?}"
            );
        }
    }

    #[test]
    fn sealed_header_predicate_runs_without_cache_read_guard() {
        let provider = ServeCacheProvider::new();
        let (header, body) = test_block(2);
        provider.insert_block(&header, &body, &[]);
        let mut calls = 0;
        let result = provider
            .sealed_headers_while(2..=2, |_| {
                // try_write makes a regression fail immediately instead of deadlocking.
                assert!(provider.inner.try_write().is_ok());
                calls += 1;
                true
            })
            .unwrap();
        assert_eq!(calls, 1);
        assert_eq!(result.len(), 1);
    }
    #[test]
    fn sparse_provider_ranges_preserve_body_and_header_availability() {
        let provider = ServeCacheProvider::new();
        for number in [2, 6] {
            let (header, body) = test_block(number);
            provider.insert_block(&header, &body, &[test_receipt()]);
        }
        provider.insert_headers([test_block(4).0]);
        let hashes = provider.canonical_hashes_range(0, 7).unwrap();
        assert_eq!(
            hashes,
            [2, 4, 6].map(|number| test_block(number).0.hash_slow())
        );
        assert_eq!(
            provider
                .block_range(0..=6)
                .unwrap()
                .iter()
                .map(|block| block.header.number)
                .collect::<Vec<_>>(),
            vec![0, 2, 6]
        );
        let indices = provider.block_body_indices_range(0..=6).unwrap();
        assert_eq!(indices.len(), 3);
        assert!(indices.iter().all(|index| index.tx_count == 0));
        let receipts = provider.receipts_by_block_range(0..=6).unwrap();
        assert_eq!(
            receipts,
            vec![
                Vec::new(),
                vec![test_receipt().receipt],
                vec![test_receipt().receipt]
            ]
        );
        assert_eq!(
            provider.transactions_by_block_range(0..=6).unwrap(),
            vec![Vec::new(); 3]
        );
        assert!(provider.canonical_hashes_range(4, 4).unwrap().is_empty());
        assert!(provider.block_range(3..=5).unwrap().is_empty());
        assert!(provider.receipts_by_block_range(3..=5).unwrap().is_empty());
        assert!(provider.block_body_indices_range(3..=5).unwrap().is_empty());
    }
    #[test]
    fn sparse_ranges_span_numeric_domain_without_height_iteration() {
        let provider = ServeCacheProvider::new();
        for number in [2, u64::MAX] {
            let (header, body) = test_block(number);
            provider.insert_block(&header, &body, &[test_receipt()]);
        }
        provider.insert_headers([test_block(u64::MAX - 1).0]);
        let expected_headers = vec![0, 2, u64::MAX - 1, u64::MAX];
        assert_eq!(
            provider
                .headers_range(..)
                .unwrap()
                .iter()
                .map(|h| h.number)
                .collect::<Vec<_>>(),
            expected_headers
        );
        assert_eq!(
            provider
                .sealed_headers_while(.., |_| true)
                .unwrap()
                .iter()
                .map(|h| h.number)
                .collect::<Vec<_>>(),
            expected_headers
        );
        // The canonical hash method has an exclusive end and no implicit genesis.
        assert_eq!(
            provider.canonical_hashes_range(0, u64::MAX).unwrap(),
            [2, u64::MAX - 1].map(|number| test_block(number).0.hash_slow())
        );
        assert_eq!(
            provider
                .block_range(0..=u64::MAX)
                .unwrap()
                .iter()
                .map(|b| b.header.number)
                .collect::<Vec<_>>(),
            vec![0, 2, u64::MAX]
        );
        assert_eq!(provider.transactions_by_block_range(..).unwrap().len(), 3);
        assert_eq!(
            provider
                .receipts_by_block_range(0..=u64::MAX)
                .unwrap()
                .len(),
            3
        );
        assert_eq!(
            provider
                .block_body_indices_range(0..=u64::MAX)
                .unwrap()
                .len(),
            3
        );
        let exhausted = (Bound::Excluded(u64::MAX), Bound::Unbounded);
        assert!(provider.headers_range(exhausted).unwrap().is_empty());
        assert!(
            provider
                .transactions_by_block_range(exhausted)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            provider.headers_range(u64::MAX..).unwrap(),
            vec![test_block(u64::MAX).0]
        );
        assert_eq!(
            provider
                .transactions_by_block_range(u64::MAX..)
                .unwrap()
                .len(),
            1
        );
        assert!(provider.headers_range(..0).unwrap().is_empty());
    }

    #[test]
    fn header_eviction_falls_back_to_body_and_replacement_reports_availability_change() {
        let provider = ServeCacheProvider::new();
        let (header, body) = test_block(1);
        let hash = header.hash_slow();
        provider.insert_block(&header, &body, &[test_receipt()]);
        assert!(!provider.insert_headers(
            (2..=SERVE_CACHE_HEADER_LIMIT as u64 + 1).map(|number| test_block(number).0)
        ));
        assert!(!provider.inner.read().unwrap().headers.contains_key(&hash));
        assert_eq!(provider.header_by_number(1).unwrap(), Some(header.clone()));
        assert_eq!(provider.sealed_header(1).unwrap().unwrap().hash(), hash);
        assert_eq!(provider.headers_range(1..=1).unwrap(), vec![header.clone()]);
        assert_eq!(provider.canonical_hashes_range(1, 2).unwrap(), vec![hash]);
        let mut replacement = header;
        replacement.timestamp = 1;
        // Even with its header inventory entry evicted, the conflicting body is removed.
        assert!(provider.insert_headers([replacement]));
        assert!(provider.block_by_number(1).unwrap().is_none());
        assert!(provider.receipts_by_block(hash.into()).unwrap().is_none());
        assert!(provider.header(hash).unwrap().is_none());
        assert_eq!(provider.advertised_history_range(), None);
    }

    #[test]
    fn header_insert_change_flag_tracks_only_body_availability_changes() {
        let provider = ServeCacheProvider::new();
        let (header, body) = test_block(2);
        assert!(!provider.insert_headers([header.clone()]));
        assert!(!provider.insert_headers([header.clone()]));
        provider.insert_block(&header, &body, &[]);
        assert!(!provider.insert_headers([header.clone()]));
        let mut replacement = header;
        replacement.timestamp = 1;
        // Aggregate flag remains true if a later independent insertion changes no body.
        assert!(provider.insert_headers([replacement.clone(), test_block(4).0]));
        assert!(!provider.insert_headers([replacement]));
        assert!(!provider.insert_headers(std::iter::empty()));
    }

    #[test]
    fn delayed_old_hash_removal_preserves_newer_full_block() {
        let provider = ServeCacheProvider::new();
        let (old, body) = test_block(2);
        let old_hash = old.hash_slow();
        provider.insert_block(&old, &body, &[test_receipt()]);
        let mut replacement = old;
        replacement.timestamp = 1;
        let hash = replacement.hash_slow();
        provider.insert_block(&replacement, &body, &[test_receipt()]);
        provider.remove_blocks(&[old_hash]);
        assert_eq!(provider.block_hash(2).unwrap(), Some(hash));
        assert_eq!(
            provider.block_by_number(2).unwrap(),
            Some(Block::new(replacement.clone(), body))
        );
        assert_eq!(
            provider.receipts_by_block(2.into()).unwrap(),
            Some(vec![test_receipt().receipt])
        );
        assert_eq!(provider.header_by_number(2).unwrap(), Some(replacement));
        assert_eq!(provider.advertised_history_range(), Some((2, 2, hash)));
    }

    #[test]
    fn header_predicate_mutation_observes_pre_mutation_snapshot() {
        let provider = ServeCacheProvider::new();
        let first = test_block(2).0;
        let second = test_block(4).0;
        let second_hash = second.hash_slow();
        provider.insert_headers([first.clone(), second.clone()]);
        let mut replacement = second.clone();
        replacement.timestamp = 1;
        let mut calls = 0;
        let snapshot = provider
            .sealed_headers_while(2..=4, |_| {
                assert!(
                    provider.inner.try_write().is_ok(),
                    "predicate must not run under the cache lock"
                );
                if calls == 0 {
                    provider.insert_headers([replacement.clone()]);
                }
                calls += 1;
                true
            })
            .unwrap();
        assert_eq!(calls, 2);
        assert_eq!(
            snapshot
                .iter()
                .map(|h| h.header().clone())
                .collect::<Vec<_>>(),
            vec![first, second]
        );
        assert_eq!(snapshot[1].hash(), second_hash);
        assert_eq!(provider.header_by_number(4).unwrap(), Some(replacement));
    }
}

#[cfg(test)]
mod payload_budget_tests;
