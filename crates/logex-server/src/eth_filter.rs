use alloy_primitives::{Address, B256};
use serde::{Deserialize, Serialize};

use logex_storage::native::{NativeLogFilter, TopicConstraint};
use logex_types::LogRow;

/// `eth_getLogs` filter parameter — compatible with the Ethereum JSON-RPC spec.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EthFilter {
    /// Starting block (inclusive). Can be a hex number or "latest"/"earliest".
    pub from_block: Option<BlockId>,
    /// Ending block (inclusive). Can be a hex number or "latest"/"earliest".
    pub to_block: Option<BlockId>,
    /// Contract address or list of addresses to filter.
    #[serde(default)]
    pub address: AddressFilter,
    /// Topic filters — up to 4 positions, each can be null, a single hash, or
    /// an array of hashes (OR within position, AND across positions).
    #[serde(default)]
    pub topics: Vec<Option<TopicFilter>>,
    /// Block hash — mutually exclusive with fromBlock/toBlock.
    pub block_hash: Option<B256>,
}

impl EthFilter {
    /// Convert this RPC filter into the native storage filter shape that the
    /// rewritten storage/query layer should execute directly.
    pub fn to_native_filter(&self, head_block: u64) -> NativeLogFilter {
        let mut filter = NativeLogFilter::new();

        if self.block_hash.is_none() {
            filter.from_block = self
                .from_block
                .as_ref()
                .map(|id| resolve_block_id(id, head_block));
            filter.to_block = self
                .to_block
                .as_ref()
                .map(|id| resolve_block_id(id, head_block));
        }

        filter.block_hash = self.block_hash;
        filter.addresses = match &self.address {
            AddressFilter::Any => Vec::new(),
            AddressFilter::Single(addr) => vec![*addr],
            AddressFilter::Multiple(addrs) => addrs.clone(),
        };

        for (idx, topic_filter) in self.topics.iter().enumerate().take(4) {
            if let Some(topic_filter) = topic_filter {
                filter.topics[idx] = match topic_filter {
                    TopicFilter::Single(hash) => TopicConstraint::One(*hash),
                    TopicFilter::Multiple(hashes) => TopicConstraint::AnyOf(hashes.clone()),
                };
            }
        }

        filter
    }
}

/// A block identifier: hex number or named tag.
#[derive(Debug, Clone)]
pub enum BlockId {
    Number(u64),
    Latest,
    Earliest,
}

fn resolve_block_id(id: &BlockId, head_block: u64) -> u64 {
    match id {
        BlockId::Number(n) => *n,
        BlockId::Latest => head_block,
        BlockId::Earliest => 0,
    }
}

impl<'de> Deserialize<'de> for BlockId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "latest" | "pending" | "safe" | "finalized" => Ok(BlockId::Latest),
            "earliest" => Ok(BlockId::Earliest),
            hex => {
                let hex = hex.strip_prefix("0x").unwrap_or(hex);
                u64::from_str_radix(hex, 16)
                    .map(BlockId::Number)
                    .map_err(serde::de::Error::custom)
            }
        }
    }
}

/// Address filter: single address, array, or none (match all).
#[derive(Debug, Clone, Default)]
pub enum AddressFilter {
    #[default]
    Any,
    Single(Address),
    Multiple(Vec<Address>),
}

impl<'de> Deserialize<'de> for AddressFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::Null => Ok(AddressFilter::Any),
            serde_json::Value::String(s) => {
                let addr = parse_address(&s).map_err(serde::de::Error::custom)?;
                Ok(AddressFilter::Single(addr))
            }
            serde_json::Value::Array(arr) => {
                let addrs: Result<Vec<Address>, _> = arr
                    .iter()
                    .map(|v| {
                        v.as_str()
                            .ok_or_else(|| serde::de::Error::custom("expected hex string"))
                            .and_then(|s| parse_address(s).map_err(serde::de::Error::custom))
                    })
                    .collect();
                Ok(AddressFilter::Multiple(addrs?))
            }
            _ => Err(serde::de::Error::custom("invalid address filter")),
        }
    }
}

/// Topic filter for a single position.
#[derive(Debug, Clone)]
pub enum TopicFilter {
    Single(B256),
    Multiple(Vec<B256>),
}

impl<'de> Deserialize<'de> for TopicFilter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value {
            serde_json::Value::String(s) => {
                let hash = parse_b256(&s).map_err(serde::de::Error::custom)?;
                Ok(TopicFilter::Single(hash))
            }
            serde_json::Value::Array(arr) => {
                let hashes: Result<Vec<B256>, _> = arr
                    .iter()
                    .filter_map(|v| v.as_str().map(parse_b256))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(serde::de::Error::custom);
                Ok(TopicFilter::Multiple(hashes?))
            }
            _ => Err(serde::de::Error::custom("invalid topic filter")),
        }
    }
}

fn parse_address(s: &str) -> Result<Address, String> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(hex).map_err(|e| format!("invalid address hex: {e}"))?;
    if bytes.len() != 20 {
        return Err(format!("address must be 20 bytes, got {}", bytes.len()));
    }
    Ok(Address::from_slice(&bytes))
}

fn parse_b256(s: &str) -> Result<B256, String> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(hex).map_err(|e| format!("invalid hash hex: {e}"))?;
    if bytes.len() != 32 {
        return Err(format!("hash must be 32 bytes, got {}", bytes.len()));
    }
    Ok(B256::from_slice(&bytes))
}

/// JSON-RPC log object returned by `eth_getLogs`.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcLog {
    pub address: String,
    pub topics: Vec<String>,
    pub data: String,
    pub block_number: String,
    pub block_hash: String,
    pub transaction_hash: String,
    pub transaction_index: String,
    pub log_index: String,
    pub removed: bool,
}

impl From<&LogRow> for RpcLog {
    fn from(row: &LogRow) -> Self {
        let mut topics = Vec::with_capacity(4);
        if let Some(t) = &row.topic0 {
            topics.push(format!("0x{}", hex::encode(t)));
        }
        if let Some(t) = &row.topic1 {
            topics.push(format!("0x{}", hex::encode(t)));
        }
        if let Some(t) = &row.topic2 {
            topics.push(format!("0x{}", hex::encode(t)));
        }
        if let Some(t) = &row.topic3 {
            topics.push(format!("0x{}", hex::encode(t)));
        }

        RpcLog {
            address: format!("0x{}", hex::encode(row.address)),
            topics,
            data: format!("0x{}", hex::encode(&row.data)),
            block_number: format!("0x{:x}", row.block_number),
            block_hash: format!("0x{}", hex::encode(row.block_hash)),
            transaction_hash: format!("0x{}", hex::encode(row.tx_hash)),
            transaction_index: format!("0x{:x}", row.tx_index),
            log_index: format!("0x{:x}", row.log_index),
            removed: false,
        }
    }
}

/// Check if a LogRow matches an `eth_getLogs` filter.
pub fn matches_filter(row: &LogRow, filter: &EthFilter) -> bool {
    if let Some(block_hash) = filter.block_hash
        && row.block_hash != block_hash
    {
        return false;
    }

    // Address filter
    match &filter.address {
        AddressFilter::Any => {}
        AddressFilter::Single(addr) => {
            if row.address != *addr {
                return false;
            }
        }
        AddressFilter::Multiple(addrs) => {
            if !addrs.contains(&row.address) {
                return false;
            }
        }
    }

    // Topic filters
    let row_topics: [Option<&B256>; 4] = [
        row.topic0.as_ref(),
        row.topic1.as_ref(),
        row.topic2.as_ref(),
        row.topic3.as_ref(),
    ];

    for (i, topic_filter) in filter.topics.iter().enumerate() {
        if i >= 4 {
            break;
        }
        if let Some(tf) = topic_filter {
            match tf {
                TopicFilter::Single(expected) => {
                    if row_topics[i] != Some(expected) {
                        return false;
                    }
                }
                TopicFilter::Multiple(expected) => match row_topics[i] {
                    Some(actual) if expected.contains(actual) => {}
                    _ => return false,
                },
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::bytes;
    use logex_types::Source;

    fn test_row() -> LogRow {
        LogRow {
            block_number: 100,
            block_hash: B256::repeat_byte(0x01),
            timestamp: 1_700_000_000,
            tx_hash: B256::repeat_byte(0x11),
            tx_index: 0,
            log_index: 0,
            address: Address::repeat_byte(0xAA),
            topic0: Some(B256::repeat_byte(0xDD)),
            topic1: Some(B256::repeat_byte(0xEE)),
            topic2: None,
            topic3: None,
            data: bytes!("cafe"),
            data_len: 2,
            source: Source::Receipt,
        }
    }

    #[test]
    fn test_filter_no_criteria() {
        let filter = EthFilter::default();
        assert!(matches_filter(&test_row(), &filter));
    }

    #[test]
    fn test_filter_address_match() {
        let filter = EthFilter {
            address: AddressFilter::Single(Address::repeat_byte(0xAA)),
            ..Default::default()
        };
        assert!(matches_filter(&test_row(), &filter));
    }

    #[test]
    fn test_filter_address_mismatch() {
        let filter = EthFilter {
            address: AddressFilter::Single(Address::repeat_byte(0xBB)),
            ..Default::default()
        };
        assert!(!matches_filter(&test_row(), &filter));
    }

    #[test]
    fn test_filter_topic0_match() {
        let filter = EthFilter {
            topics: vec![Some(TopicFilter::Single(B256::repeat_byte(0xDD)))],
            ..Default::default()
        };
        assert!(matches_filter(&test_row(), &filter));
    }

    #[test]
    fn test_filter_topic0_mismatch() {
        let filter = EthFilter {
            topics: vec![Some(TopicFilter::Single(B256::repeat_byte(0xFF)))],
            ..Default::default()
        };
        assert!(!matches_filter(&test_row(), &filter));
    }

    #[test]
    fn test_filter_topic_or() {
        let filter = EthFilter {
            topics: vec![Some(TopicFilter::Multiple(vec![
                B256::repeat_byte(0xFF),
                B256::repeat_byte(0xDD),
            ]))],
            ..Default::default()
        };
        assert!(matches_filter(&test_row(), &filter));
    }

    #[test]
    fn test_filter_topic_skip_wildcard() {
        // null in position 0 = wildcard, match on topic1
        let filter = EthFilter {
            topics: vec![None, Some(TopicFilter::Single(B256::repeat_byte(0xEE)))],
            ..Default::default()
        };
        assert!(matches_filter(&test_row(), &filter));
    }

    #[test]
    fn test_filter_block_hash_mismatch() {
        let filter = EthFilter {
            block_hash: Some(B256::repeat_byte(0xFF)),
            ..Default::default()
        };
        assert!(!matches_filter(&test_row(), &filter));
    }

    #[test]
    fn test_rpc_log_serialization() {
        let row = test_row();
        let rpc = RpcLog::from(&row);
        assert!(rpc.address.starts_with("0x"));
        assert_eq!(rpc.topics.len(), 2);
        assert_eq!(rpc.block_number, "0x64");
        assert!(!rpc.removed);
    }

    #[test]
    fn test_deserialize_filter() {
        let json = r#"{
            "fromBlock": "0x100",
            "toBlock": "latest",
            "address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "topics": [
                "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
            ]
        }"#;
        let filter: EthFilter = serde_json::from_str(json).unwrap();
        assert!(matches!(filter.from_block, Some(BlockId::Number(256))));
        assert!(matches!(filter.to_block, Some(BlockId::Latest)));
        assert!(matches!(filter.address, AddressFilter::Single(_)));
    }

    #[test]
    fn test_native_filter_conversion() {
        let filter = EthFilter {
            from_block: Some(BlockId::Number(100)),
            to_block: Some(BlockId::Latest),
            address: AddressFilter::Multiple(vec![
                Address::repeat_byte(0xAA),
                Address::repeat_byte(0xBB),
            ]),
            topics: vec![
                Some(TopicFilter::Single(B256::repeat_byte(0x11))),
                None,
                Some(TopicFilter::Multiple(vec![
                    B256::repeat_byte(0x22),
                    B256::repeat_byte(0x33),
                ])),
            ],
            block_hash: None,
        };

        let native = filter.to_native_filter(500);
        assert_eq!(native.from_block, Some(100));
        assert_eq!(native.to_block, Some(500));
        assert_eq!(native.addresses.len(), 2);
        assert!(matches!(
            native.topics[0],
            TopicConstraint::One(hash) if hash == B256::repeat_byte(0x11)
        ));
        assert!(matches!(
            native.topics[2],
            TopicConstraint::AnyOf(ref hashes) if hashes.len() == 2
        ));
    }
}
