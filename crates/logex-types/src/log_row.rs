use alloy_primitives::{Address, B256, Bytes};
use serde::{Deserialize, Serialize};

/// How a log row was obtained.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum Source {
    /// Extracted from a transaction receipt whose Merkle root was validated
    /// against the block header's consensus-attested `receiptsRoot`.
    Receipt = 0,
    /// Synthesized from an EVM execution trace obtained from an archive node.
    /// Correct if and only if the archive node executed faithfully.
    Trace = 1,
}

impl Source {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Receipt),
            1 => Some(Self::Trace),
            _ => None,
        }
    }
}

/// The atomic unit of storage in LogEx. Each log entry is decomposed into this
/// flat structure — no nesting, no trie traversal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRow {
    pub block_number: u64,
    pub block_hash: B256,
    pub timestamp: u64,
    pub tx_hash: B256,
    pub tx_index: u32,
    /// Global log index within the block.
    pub log_index: u32,
    /// The emitting contract address (20 bytes).
    pub address: Address,
    /// First log topic; often the event signature for non-anonymous Solidity
    /// events. Anonymous events may also have topics. `None` means no topics.
    pub topic0: Option<B256>,
    /// Second log topic; often the first indexed parameter.
    pub topic1: Option<B256>,
    /// Third log topic; often the second indexed parameter.
    pub topic2: Option<B256>,
    /// Fourth log topic; often the third indexed parameter.
    pub topic3: Option<B256>,
    /// Non-indexed ABI-encoded parameters.
    pub data: Bytes,
    /// Length of `data` for fast filtering without loading the data column.
    pub data_len: u32,
    /// Provenance of this row.
    pub source: Source,
}

/// Block-level context needed to convert an alloy Log into a LogRow.
#[derive(Debug, Clone)]
pub struct BlockContext {
    pub block_number: u64,
    pub block_hash: B256,
    pub timestamp: u64,
}

/// A primitive log cannot be represented by the persisted row schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogRowConversionError {
    TooManyTopics { count: usize },
    DataTooLong { length: usize },
}

impl std::fmt::Display for LogRowConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooManyTopics { count } => {
                write!(f, "log has {count} topics; at most 4 are supported")
            }
            Self::DataTooLong { length } => {
                write!(f, "log data length {length} exceeds the u32 row limit")
            }
        }
    }
}

impl std::error::Error for LogRowConversionError {}

fn checked_log_shape(topic_count: usize, data_length: usize) -> Result<u32, LogRowConversionError> {
    if topic_count > 4 {
        return Err(LogRowConversionError::TooManyTopics { count: topic_count });
    }
    u32::try_from(data_length).map_err(|_| LogRowConversionError::DataTooLong {
        length: data_length,
    })
}

impl LogRow {
    /// Convert an alloy RPC log into a `LogRow`.
    ///
    /// The alloy `Log` type already carries block/tx metadata when returned
    /// from an RPC response, but during P2P sync we have the block
    /// context separately, so this constructor accepts both.
    ///
    /// # Panics
    /// Panics if the log exceeds the row's topic or data-length limits. Use
    /// [`Self::try_from_alloy_log`] for input that has not been checked.
    pub fn from_alloy_log(
        log: &alloy_rpc_types::Log,
        ctx: &BlockContext,
        tx_hash: B256,
        tx_index: u32,
        log_index: u32,
    ) -> Self {
        Self::from_primitives_log(&log.inner, ctx, tx_hash, tx_index, log_index)
    }

    /// Checked RPC conversion using the caller's context and receipt provenance.
    /// This does not authenticate the log or its RPC metadata.
    pub fn try_from_alloy_log(
        log: &alloy_rpc_types::Log,
        ctx: &BlockContext,
        tx_hash: B256,
        tx_index: u32,
        log_index: u32,
    ) -> Result<Self, LogRowConversionError> {
        Self::try_from_primitives_log(&log.inner, ctx, tx_hash, tx_index, log_index)
    }

    /// Convert a primitive log (as found in receipts) into a `LogRow`.
    ///
    /// This is the lower-level constructor used during P2P sync where
    /// we get `alloy_primitives::Log` directly from receipts rather than
    /// the RPC-wrapped `alloy_rpc_types::Log`.
    /// The caller must validate receipt provenance and supply block/transaction
    /// metadata; this conversion assigns `Source::Receipt` without verifying it.
    ///
    /// # Panics
    /// Panics if the log exceeds the row's topic or data-length limits. Use
    /// [`Self::try_from_primitives_log`] for input that has not been checked.
    pub fn from_primitives_log(
        log: &alloy_primitives::Log,
        ctx: &BlockContext,
        tx_hash: B256,
        tx_index: u32,
        log_index: u32,
    ) -> Self {
        Self::try_from_primitives_log(log, ctx, tx_hash, tx_index, log_index)
            .expect("primitive log must fit the LogRow schema")
    }

    /// Convert without truncating topics or data length. The caller must still
    /// authenticate receipts and supply their block/transaction metadata.
    #[inline]
    pub fn try_from_primitives_log(
        log: &alloy_primitives::Log,
        ctx: &BlockContext,
        tx_hash: B256,
        tx_index: u32,
        log_index: u32,
    ) -> Result<Self, LogRowConversionError> {
        let topics = log.data.topics();
        let data_len = checked_log_shape(topics.len(), log.data.data.len())?;
        Ok(Self {
            block_number: ctx.block_number,
            block_hash: ctx.block_hash,
            timestamp: ctx.timestamp,
            tx_hash,
            tx_index,
            log_index,
            address: log.address,
            topic0: topics.first().copied(),
            topic1: topics.get(1).copied(),
            topic2: topics.get(2).copied(),
            topic3: topics.get(3).copied(),
            data_len,
            data: log.data.data.clone(),
            source: Source::Receipt,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, LogData, bytes};

    fn make_test_log_row() -> LogRow {
        LogRow {
            block_number: 18_500_000,
            block_hash: B256::repeat_byte(0xAA),
            timestamp: 1_700_000_000,
            tx_hash: B256::repeat_byte(0xBB),
            tx_index: 42,
            log_index: 7,
            address: Address::repeat_byte(0x01),
            topic0: Some(B256::repeat_byte(0x10)),
            topic1: Some(B256::repeat_byte(0x20)),
            topic2: None,
            topic3: None,
            data: bytes!("deadbeef"),
            data_len: 4,
            source: Source::Receipt,
        }
    }

    #[test]
    fn primitive_conversion_rejects_excess_topics() {
        let log = alloy_primitives::Log {
            address: Address::ZERO,
            data: LogData::new_unchecked(vec![B256::ZERO; 5], Bytes::new()),
        };
        let ctx = BlockContext {
            block_number: 0,
            block_hash: B256::ZERO,
            timestamp: 0,
        };
        assert!(
            std::panic::catch_unwind(|| LogRow::from_primitives_log(&log, &ctx, B256::ZERO, 0, 0))
                .is_err()
        );
    }

    #[test]
    fn checked_conversion_validates_shape_and_preserves_topic_presence() {
        let ctx = BlockContext {
            block_number: u64::MAX,
            block_hash: B256::ZERO,
            timestamp: u64::MAX,
        };
        for count in 0..=5 {
            let log = alloy_primitives::Log {
                address: Address::ZERO,
                data: LogData::new_unchecked(vec![B256::ZERO; count], bytes!("0001")),
            };
            let result =
                LogRow::try_from_primitives_log(&log, &ctx, B256::ZERO, u32::MAX, u32::MAX);
            if count > 4 {
                assert_eq!(result, Err(LogRowConversionError::TooManyTopics { count }));
            } else {
                let row = result.unwrap();
                assert_eq!(
                    [row.topic0, row.topic1, row.topic2, row.topic3],
                    std::array::from_fn(|i| (i < count).then_some(B256::ZERO))
                );
                assert_eq!(row.data_len, 2);
                assert_eq!(row.data, bytes!("0001"));
                assert_eq!(row.tx_index, u32::MAX);
                assert_eq!(row.log_index, u32::MAX);
                assert_eq!(row.block_number, u64::MAX);
                assert_eq!(row.timestamp, u64::MAX);
            }
        }
        assert_eq!(checked_log_shape(0, 0), Ok(0));
        assert_eq!(checked_log_shape(4, u32::MAX as usize), Ok(u32::MAX));
        if let Some(length) = (u32::MAX as usize).checked_add(1) {
            assert_eq!(
                checked_log_shape(4, length),
                Err(LogRowConversionError::DataTooLong { length })
            );
        }
    }

    #[test]
    fn test_log_row_construction() {
        let row = make_test_log_row();
        assert_eq!(row.block_number, 18_500_000);
        assert_eq!(row.data_len, 4);
        assert_eq!(row.source, Source::Receipt);
        assert!(row.topic0.is_some());
        assert!(row.topic2.is_none());
    }

    #[test]
    fn test_source_roundtrip() {
        assert_eq!(Source::from_u8(0), Some(Source::Receipt));
        assert_eq!(Source::from_u8(1), Some(Source::Trace));
        assert_eq!(Source::from_u8(2), None);
    }

    #[test]
    fn test_from_alloy_log() {
        let topic0 = B256::repeat_byte(0xDD);
        let topic1 = B256::repeat_byte(0xEE);
        let log_data = LogData::new(vec![topic0, topic1], bytes!("cafe")).unwrap();
        let alloy_log = alloy_rpc_types::Log {
            inner: alloy_primitives::Log {
                address: Address::repeat_byte(0x55),
                data: log_data,
            },
            block_hash: None,
            block_number: None,
            block_timestamp: None,
            transaction_hash: None,
            transaction_index: None,
            log_index: None,
            removed: false,
        };

        let ctx = BlockContext {
            block_number: 100,
            block_hash: B256::repeat_byte(0x11),
            timestamp: 12345,
        };

        let row = LogRow::from_alloy_log(&alloy_log, &ctx, B256::repeat_byte(0x22), 5, 10);
        assert_eq!(
            LogRow::try_from_alloy_log(&alloy_log, &ctx, B256::repeat_byte(0x22), 5, 10).unwrap(),
            row
        );

        assert_eq!(row.block_number, 100);
        assert_eq!(row.block_hash, B256::repeat_byte(0x11));
        assert_eq!(row.timestamp, 12345);
        assert_eq!(row.tx_hash, B256::repeat_byte(0x22));
        assert_eq!(row.tx_index, 5);
        assert_eq!(row.log_index, 10);
        assert_eq!(row.address, Address::repeat_byte(0x55));
        assert_eq!(row.topic0, Some(topic0));
        assert_eq!(row.topic1, Some(topic1));
        assert_eq!(row.topic2, None);
        assert_eq!(row.topic3, None);
        assert_eq!(row.data, bytes!("cafe"));
        assert_eq!(row.data_len, 2);
        assert_eq!(row.source, Source::Receipt);
    }

    #[test]
    fn test_log_row_serde_roundtrip() {
        let row = make_test_log_row();
        let json = serde_json::to_string(&row).unwrap();
        let deserialized: LogRow = serde_json::from_str(&json).unwrap();
        assert_eq!(row, deserialized);
    }
}
