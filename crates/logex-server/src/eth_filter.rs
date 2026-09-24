use alloy_primitives::{Address, B256};
use serde::de::{IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};

use logex_storage::native::{NativeLogFilter, TopicConstraint};
use logex_types::LogRow;

/// `eth_getLogs` filter parameter — compatible with the Ethereum JSON-RPC spec.
#[derive(Debug, Clone, Default)]
pub struct EthFilter {
    /// Starting block (inclusive). Can be a hex number or "latest"/"earliest".
    pub from_block: Option<BlockId>,
    /// Ending block (inclusive). Can be a hex number or "latest"/"earliest".
    pub to_block: Option<BlockId>,
    /// Contract address or list of addresses to filter.
    pub address: AddressFilter,
    /// Topic filters — up to 4 positions, each can be null, a single hash, or
    /// an array of hashes (OR within position, AND across positions).
    pub topics: Vec<Option<TopicFilter>>,
    /// Block hash — mutually exclusive with fromBlock/toBlock.
    pub block_hash: Option<B256>,
    /// Non-standard LogEx pagination limit. Defaults to the server page size.
    pub limit: Option<usize>,
    /// Non-standard LogEx pagination offset.
    pub offset: usize,
}

impl<'de> Deserialize<'de> for EthFilter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FilterVisitor;
        impl<'de> Visitor<'de> for FilterVisitor {
            type Value = EthFilter;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an Ethereum filter object")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut filter = EthFilter::default();
                let mut seen = 0_u8;
                while let Some(key) = map.next_key::<String>()? {
                    let bit = match key.as_str() {
                        "fromBlock" => 1,
                        "toBlock" => 2,
                        "address" => 4,
                        "topics" => 8,
                        "blockHash" => 16,
                        "limit" => 32,
                        "offset" => 64,
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                            continue;
                        }
                    };
                    if seen & bit != 0 {
                        return Err(serde::de::Error::custom(format!(
                            "duplicate filter field {key}"
                        )));
                    }
                    seen |= bit;
                    match key.as_str() {
                        "fromBlock" => filter.from_block = map.next_value()?,
                        "toBlock" => filter.to_block = map.next_value()?,
                        "address" => filter.address = map.next_value()?,
                        "topics" => {
                            filter.topics = map
                                .next_value::<Option<Vec<Option<TopicFilter>>>>()?
                                .unwrap_or_default()
                        }
                        "blockHash" => filter.block_hash = map.next_value()?,
                        "limit" => filter.limit = map.next_value()?,
                        "offset" => filter.offset = map.next_value()?,
                        _ => unreachable!("unknown fields were consumed"),
                    }
                }
                Ok(filter)
            }
        }
        deserializer.deserialize_map(FilterVisitor)
    }
}

impl EthFilter {
    pub fn validate(&self) -> Result<(), String> {
        if self.block_hash.is_some() && (self.from_block.is_some() || self.to_block.is_some()) {
            return Err("blockHash is mutually exclusive with fromBlock/toBlock".into());
        }
        if self.topics.len() > 4 {
            return Err("at most four topic positions are supported".into());
        }
        Ok(())
    }

    /// Resolve only unambiguous live subscription bounds. Pagination is not a stream predicate.
    pub(crate) fn to_stream_filter(&self) -> Result<NativeLogFilter, String> {
        self.validate()?;
        if [&self.from_block, &self.to_block]
            .into_iter()
            .any(|bound| matches!(bound, Some(BlockId::Latest)))
        {
            return Err("named latest bounds are not supported for live subscriptions; use numeric bounds or earliest".into());
        }
        Ok(self.to_native_filter(0))
    }

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

        filter.min_topic_count = self.topics.len();
        for (constraint, topic_filter) in filter.topics.iter_mut().zip(&self.topics) {
            if let Some(topic_filter) = topic_filter {
                *constraint = match topic_filter {
                    TopicFilter::Any => TopicConstraint::Any,
                    TopicFilter::Multiple(hashes) if hashes.is_empty() => TopicConstraint::Any,
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
            "latest" => Ok(BlockId::Latest),
            "pending" | "safe" | "finalized" => Err(serde::de::Error::custom(format!(
                "unsupported block tag {s}"
            ))),
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
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct AddressVisitor;
        impl<'de> Visitor<'de> for AddressVisitor {
            type Value = AddressFilter;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an address string, address array or null")
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
                Ok(AddressFilter::Any)
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                parse_address(value)
                    .map(AddressFilter::Single)
                    .map_err(E::custom)
            }
            fn visit_seq<S: SeqAccess<'de>>(
                self,
                mut sequence: S,
            ) -> Result<Self::Value, S::Error> {
                let mut addresses = Vec::new();
                while let Some(address) = sequence.next_element::<String>()? {
                    addresses.push(parse_address(&address).map_err(serde::de::Error::custom)?);
                }
                Ok(AddressFilter::Multiple(addresses))
            }
        }
        deserializer.deserialize_any(AddressVisitor)
    }
}

/// Topic filter for a single position.
#[derive(Debug, Clone)]
pub enum TopicFilter {
    Any,
    Single(B256),
    Multiple(Vec<B256>),
}

impl<'de> Deserialize<'de> for TopicFilter {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct TopicVisitor;
        impl<'de> Visitor<'de> for TopicVisitor {
            type Value = TopicFilter;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a topic hash or OR array of hashes/null")
            }
            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
                parse_b256(value)
                    .map(TopicFilter::Single)
                    .map_err(E::custom)
            }
            fn visit_seq<S: SeqAccess<'de>>(
                self,
                mut sequence: S,
            ) -> Result<Self::Value, S::Error> {
                let mut hashes = Vec::new();
                let mut wildcard = false;
                while let Some(value) = sequence.next_element::<Option<String>>()? {
                    match value {
                        Some(hash) => {
                            hashes.push(parse_b256(&hash).map_err(serde::de::Error::custom)?)
                        }
                        None => wildcard = true,
                    }
                }
                Ok(if wildcard || hashes.is_empty() {
                    TopicFilter::Any
                } else {
                    TopicFilter::Multiple(hashes)
                })
            }
        }
        deserializer.deserialize_any(TopicVisitor)
    }
}

pub(crate) fn parse_address(s: &str) -> Result<Address, String> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if hex.len() != 40 {
        return Err(format!(
            "address must be 20 bytes (40 hex digits), got {} hex digits",
            hex.len()
        ));
    }
    let mut bytes = [0_u8; 20];
    hex::decode_to_slice(hex, &mut bytes)
        .map_err(|error| format!("invalid address hex: {error}"))?;
    Ok(Address::from(bytes))
}

fn parse_b256(s: &str) -> Result<B256, String> {
    let hex = s.strip_prefix("0x").unwrap_or(s);
    if hex.len() != 64 {
        return Err(format!(
            "hash must be 32 bytes (64 hex digits), got {} hex digits",
            hex.len()
        ));
    }
    let mut bytes = [0_u8; 32];
    hex::decode_to_slice(hex, &mut bytes).map_err(|error| format!("invalid hash hex: {error}"))?;
    Ok(B256::from(bytes))
}

/// Owned Ethereum log object used for subscription delivery.
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

/// Borrow query rows while writing their JSON directly into the charged output.
/// Subscription objects still use `RpcLog`, because those own a different lifetime.
pub(crate) struct RpcLogs<'a>(pub &'a [LogRow]);

impl Serialize for RpcLogs<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut rows = serializer.serialize_seq(Some(self.0.len()))?;
        for row in self.0 {
            rows.serialize_element(&RpcLogRef(row))?;
        }
        rows.end()
    }
}

struct RpcLogRef<'a>(&'a LogRow);

impl Serialize for RpcLogRef<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeStruct;
        let row = self.0;
        let mut object = serializer.serialize_struct("RpcLog", 9)?;
        // Preserve the key order of the previously materialized JSON value.
        object.serialize_field("address", &HexBytes(row.address.as_slice()))?;
        object.serialize_field("blockHash", &HexBytes(row.block_hash.as_slice()))?;
        object.serialize_field("blockNumber", &HexQuantity(row.block_number))?;
        object.serialize_field("data", &HexBytes(&row.data))?;
        object.serialize_field("logIndex", &HexQuantity(u64::from(row.log_index)))?;
        object.serialize_field("removed", &false)?;
        object.serialize_field("topics", &RpcTopics(row))?;
        object.serialize_field("transactionHash", &HexBytes(row.tx_hash.as_slice()))?;
        object.serialize_field("transactionIndex", &HexQuantity(u64::from(row.tx_index)))?;
        object.end()
    }
}

struct RpcTopics<'a>(&'a LogRow);

impl Serialize for RpcTopics<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let topics = [
            &self.0.topic0,
            &self.0.topic1,
            &self.0.topic2,
            &self.0.topic3,
        ];
        let mut sequence = serializer
            .serialize_seq(Some(topics.iter().filter(|topic| topic.is_some()).count()))?;
        for topic in topics.into_iter().flatten() {
            sequence.serialize_element(&HexBytes(topic.as_slice()))?;
        }
        sequence.end()
    }
}

struct HexQuantity(u64);

impl std::fmt::Display for HexQuantity {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "0x{:x}", self.0)
    }
}

impl Serialize for HexQuantity {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

struct HexBytes<'a>(&'a [u8]);

impl std::fmt::Display for HexBytes<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("0x")?;
        if self.0.len() <= 32 {
            write_hex_chunk(self.0, &mut [0; 64], formatter)
        } else {
            write_hex_payload(self.0, formatter)
        }
    }
}

impl Serialize for HexBytes<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[inline(never)]
fn write_hex_payload(bytes: &[u8], formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    let mut scratch = [0; 8192];
    for chunk in bytes.chunks(scratch.len() / 2) {
        write_hex_chunk(chunk, &mut scratch, formatter)?;
    }
    Ok(())
}

fn write_hex_chunk(
    bytes: &[u8],
    scratch: &mut [u8],
    formatter: &mut std::fmt::Formatter<'_>,
) -> std::fmt::Result {
    let encoded = &mut scratch[..bytes.len() * 2];
    hex::encode_to_slice(bytes, encoded).map_err(|_| std::fmt::Error)?;
    formatter.write_str(std::str::from_utf8(encoded).map_err(|_| std::fmt::Error)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::bytes;
    use logex_types::Source;

    fn matches_filter(row: &LogRow, filter: &EthFilter) -> bool {
        logex_query::matches_native_filter(row, &filter.to_stream_filter().unwrap())
    }

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
    fn borrowed_logs_match_the_previous_wire_representation() {
        for length in [0, 20, 32, 33, 4096, 4097] {
            for topic_count in 0..=4 {
                let mut row = test_row();
                row.block_number = u64::MAX;
                row.tx_index = u32::MAX;
                row.log_index = u32::MAX;
                row.data = (0..length)
                    .map(|index| index as u8)
                    .collect::<Vec<_>>()
                    .into();
                row.data_len = length as u32;
                row.topic0 = (topic_count > 0).then_some(B256::repeat_byte(0x01));
                row.topic1 = (topic_count > 1).then_some(B256::repeat_byte(0x02));
                row.topic2 = (topic_count > 2).then_some(B256::repeat_byte(0x03));
                row.topic3 = (topic_count > 3).then_some(B256::repeat_byte(0x04));
                let previous = serde_json::to_value(vec![RpcLog::from(&row)]).unwrap();
                let direct = serde_json::to_vec(&RpcLogs(std::slice::from_ref(&row))).unwrap();
                assert_eq!(direct, serde_json::to_vec(&previous).unwrap());
            }
        }
        assert_eq!(serde_json::to_vec(&RpcLogs(&[])).unwrap(), b"[]");
        let mut sparse = test_row();
        sparse.block_number = 0;
        sparse.tx_index = 0;
        sparse.log_index = 0;
        sparse.topic0 = None;
        sparse.topic3 = Some(B256::repeat_byte(0x04));
        assert_eq!(
            serde_json::to_value(RpcLogs(std::slice::from_ref(&sparse))).unwrap(),
            serde_json::to_value(vec![RpcLog::from(&sparse)]).unwrap(),
        );
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
            limit: None,
            offset: 0,
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
