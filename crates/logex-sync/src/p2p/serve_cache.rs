use std::collections::{BTreeMap, HashMap, VecDeque};
use std::ops::{Bound, RangeBounds, RangeInclusive};
use std::sync::{Arc, RwLock};

use alloy_consensus::{Block, BlockBody, Header, ReceiptWithBloom};
use alloy_eips::BlockHashOrNumber;
use alloy_primitives::{Address, B256, BlockHash, BlockNumber, TxHash, TxNumber};
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

#[derive(Debug, Clone)]
pub struct ServeCacheProvider {
    chain_spec: Arc<reth_chainspec::ChainSpec>,
    inner: Arc<RwLock<ServeCacheState>>,
}

#[derive(Debug, Default)]
struct ServeCacheState {
    order: VecDeque<(u64, B256)>,
    number_to_hash: BTreeMap<u64, B256>,
    hash_to_number: HashMap<B256, u64>,
    blocks: HashMap<B256, CachedBlock>,
}

#[derive(Debug, Clone)]
struct CachedBlock {
    block: Block<reth_ethereum_primitives::TransactionSigned>,
    receipts: Vec<LogexReceipt>,
}

impl ServeCacheProvider {
    pub fn new() -> Self {
        Self {
            chain_spec: MAINNET.clone(),
            inner: Arc::new(RwLock::new(ServeCacheState::default())),
        }
    }

    pub fn insert_block(
        &self,
        header: Header,
        body: BlockBody<reth_ethereum_primitives::TransactionSigned, Header>,
        receipts: &[ReceiptWithBloom<LogexReceipt>],
    ) {
        let hash = header.hash_slow();
        let number = header.number;
        let block = Block::new(header, body);
        let receipts = receipts
            .iter()
            .cloned()
            .map(|receipt| receipt.receipt)
            .collect();

        let mut state = self.inner.write().expect("serve cache poisoned");
        if let Some(old_hash) = state.number_to_hash.insert(number, hash)
            && old_hash != hash
        {
            state.hash_to_number.remove(&old_hash);
            state.blocks.remove(&old_hash);
        }

        state.hash_to_number.insert(hash, number);
        state.blocks.insert(hash, CachedBlock { block, receipts });
        state.order.push_back((number, hash));

        while state.blocks.len() > SERVE_CACHE_BLOCK_LIMIT {
            let Some((evict_number, evict_hash)) = state.order.pop_front() else {
                break;
            };
            let still_canonical =
                state.number_to_hash.get(&evict_number).copied() == Some(evict_hash);
            if still_canonical {
                state.number_to_hash.remove(&evict_number);
                state.hash_to_number.remove(&evict_hash);
                state.blocks.remove(&evict_hash);
            }
        }
    }

    pub fn remove_blocks(&self, reverted_hashes: &[B256]) {
        if reverted_hashes.is_empty() {
            return;
        }

        let mut state = self.inner.write().expect("serve cache poisoned");
        for hash in reverted_hashes {
            let Some(number) = state.hash_to_number.remove(hash) else {
                continue;
            };
            if state.number_to_hash.get(&number).copied() == Some(*hash) {
                state.number_to_hash.remove(&number);
            }
            state.blocks.remove(hash);
        }
    }

    pub fn advertised_history_range(&self) -> Option<(u64, u64, B256)> {
        let state = self.inner.read().expect("serve cache poisoned");
        let (&earliest, _) = state.number_to_hash.iter().next()?;
        let (&latest, &latest_hash) = state.number_to_hash.iter().next_back()?;
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

    fn block_hash_for_number(&self, number: u64) -> Option<B256> {
        let state = self.inner.read().expect("serve cache poisoned");
        state
            .number_to_hash
            .get(&number)
            .copied()
            .or_else(|| (number == 0).then(|| self.chain_spec.genesis_hash()))
    }

    fn block_by_hash_inner(
        &self,
        hash: B256,
    ) -> Option<Block<reth_ethereum_primitives::TransactionSigned>> {
        let state = self.inner.read().expect("serve cache poisoned");
        state
            .blocks
            .get(&hash)
            .map(|cached| cached.block.clone())
            .or_else(|| {
                (hash == self.chain_spec.genesis_hash()).then(|| {
                    Block::new(
                        self.chain_spec.genesis_header().clone(),
                        BlockBody::default(),
                    )
                })
            })
    }

    fn header_by_number_inner(&self, number: u64) -> Option<Header> {
        let hash = self.block_hash_for_number(number)?;
        self.header(hash).ok().flatten()
    }

    fn receipts_by_block_inner(&self, block: BlockHashOrNumber) -> Option<Vec<LogexReceipt>> {
        let hash = match block {
            BlockHashOrNumber::Hash(hash) => hash,
            BlockHashOrNumber::Number(number) => self.block_hash_for_number(number)?,
        };

        let state = self.inner.read().expect("serve cache poisoned");
        state
            .blocks
            .get(&hash)
            .map(|cached| cached.receipts.clone())
            .or_else(|| (hash == self.chain_spec.genesis_hash()).then(Vec::new))
    }

    fn number_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> Option<RangeInclusive<BlockNumber>> {
        let best_number = self.chain_info_inner().best_number;
        let start = match range.start_bound() {
            Bound::Included(value) => *value,
            Bound::Excluded(value) => value.saturating_add(1),
            Bound::Unbounded => 0,
        };
        let end = match range.end_bound() {
            Bound::Included(value) => *value,
            Bound::Excluded(value) => value.saturating_sub(1),
            Bound::Unbounded => best_number,
        };
        (start <= end).then_some(start..=end)
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
        Ok(self.block_hash_for_number(number))
    }

    fn canonical_hashes_range(
        &self,
        start: BlockNumber,
        end: BlockNumber,
    ) -> ProviderResult<Vec<B256>> {
        let state = self.inner.read().expect("serve cache poisoned");
        Ok((start..end)
            .filter_map(|number| state.number_to_hash.get(&number).copied())
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
        Ok(self
            .block_by_hash_inner(block_hash)
            .map(|block| block.header))
    }

    fn header_by_number(&self, num: u64) -> ProviderResult<Option<Self::Header>> {
        Ok(self.header_by_number_inner(num))
    }

    fn headers_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Self::Header>> {
        let Some(range) = self.number_range(range) else {
            return Ok(Vec::new());
        };

        Ok(range
            .filter_map(|number| self.header_by_number_inner(number))
            .collect())
    }

    fn sealed_header(
        &self,
        number: BlockNumber,
    ) -> ProviderResult<Option<SealedHeader<Self::Header>>> {
        let Some(hash) = self.block_hash_for_number(number) else {
            return Ok(None);
        };
        Ok(self
            .header_by_number_inner(number)
            .map(|header| SealedHeader::new(header, hash)))
    }

    fn sealed_headers_while(
        &self,
        range: impl RangeBounds<BlockNumber>,
        mut predicate: impl FnMut(&SealedHeader<Self::Header>) -> bool,
    ) -> ProviderResult<Vec<SealedHeader<Self::Header>>> {
        let Some(range) = self.number_range(range) else {
            return Ok(Vec::new());
        };

        let mut headers = Vec::new();
        for number in range {
            let Some(sealed) = self.sealed_header(number)? else {
                continue;
            };
            if !predicate(&sealed) {
                break;
            }
            headers.push(sealed);
        }

        Ok(headers)
    }
}

impl BlockBodyIndicesProvider for ServeCacheProvider {
    fn block_body_indices(&self, num: u64) -> ProviderResult<Option<StoredBlockBodyIndices>> {
        let tx_count = self
            .block_by_number(num)?
            .map(|block| block.body.transactions.len() as u64);
        Ok(tx_count.map(|tx_count| StoredBlockBodyIndices {
            first_tx_num: 0,
            tx_count,
        }))
    }

    fn block_body_indices_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<StoredBlockBodyIndices>> {
        Ok(range
            .filter_map(|number| self.block_body_indices(number).ok().flatten())
            .collect())
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
        Ok(self.block(block)?.map(|block| block.body.transactions))
    }

    fn transactions_by_block_range(
        &self,
        range: impl RangeBounds<BlockNumber>,
    ) -> ProviderResult<Vec<Vec<Self::Transaction>>> {
        let Some(range) = self.number_range(range) else {
            return Ok(Vec::new());
        };

        Ok(range
            .filter_map(|number| self.transactions_by_block(number.into()).ok().flatten())
            .collect())
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
        Ok(self.receipts_by_block_inner(block))
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
        Ok(block_range
            .filter_map(|number| self.receipts_by_block(number.into()).ok().flatten())
            .collect())
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
        Ok(self.block_by_hash_inner(hash))
    }

    fn block(&self, id: BlockHashOrNumber) -> ProviderResult<Option<Self::Block>> {
        let hash = match id {
            BlockHashOrNumber::Hash(hash) => hash,
            BlockHashOrNumber::Number(number) => match self.block_hash_for_number(number) {
                Some(hash) => hash,
                None => return Ok(None),
            },
        };

        Ok(self.block_by_hash_inner(hash))
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
        Ok(range
            .filter_map(|number| self.block_by_number(number).ok().flatten())
            .collect())
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
    use alloy_consensus::{Eip658Value, TxType};
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
    fn serves_cached_block_by_hash_and_number() {
        let provider = ServeCacheProvider::new();
        let (header, body) = test_block(7);
        let hash = header.hash_slow();
        provider.insert_block(header.clone(), body.clone(), &[test_receipt()]);

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
    fn removes_reorged_hashes_from_cache() {
        let provider = ServeCacheProvider::new();
        let (header, body) = test_block(11);
        let hash = header.hash_slow();
        provider.insert_block(header, body, &[test_receipt()]);

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
        provider.insert_block(first_header.clone(), first_body, &[test_receipt()]);
        provider.insert_block(second_header.clone(), second_body, &[test_receipt()]);

        assert_eq!(
            provider.advertised_history_range(),
            Some((11, 12, second_header.hash_slow()))
        );
    }
}
