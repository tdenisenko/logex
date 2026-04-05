use serde::{Deserialize, Serialize};

/// Identifies a column in the columnar storage engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ColumnId {
    Address,
    Topic0,
    Topic1,
    Topic2,
    Topic3,
    BlockNumber,
    BlockHash,
    TxHash,
    TxIndex,
    LogIndex,
    Timestamp,
    Data,
    DataLen,
    Source,
}

impl ColumnId {
    /// File name for this column within a partition directory.
    pub fn file_name(&self) -> &'static str {
        match self {
            Self::Address => "address.col",
            Self::Topic0 => "topic0.col",
            Self::Topic1 => "topic1.col",
            Self::Topic2 => "topic2.col",
            Self::Topic3 => "topic3.col",
            Self::BlockNumber => "block_number.col",
            Self::BlockHash => "block_hash.col",
            Self::TxHash => "tx_hash.col",
            Self::TxIndex => "tx_index.col",
            Self::LogIndex => "log_index.col",
            Self::Timestamp => "timestamp.col",
            Self::Data => "data.col",
            Self::DataLen => "data_len.col",
            Self::Source => "source.col",
        }
    }

    /// All column variants.
    pub fn all() -> &'static [ColumnId] {
        &[
            Self::Address,
            Self::Topic0,
            Self::Topic1,
            Self::Topic2,
            Self::Topic3,
            Self::BlockNumber,
            Self::BlockHash,
            Self::TxHash,
            Self::TxIndex,
            Self::LogIndex,
            Self::Timestamp,
            Self::Data,
            Self::DataLen,
            Self::Source,
        ]
    }
}
