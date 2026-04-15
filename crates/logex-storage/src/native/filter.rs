use alloy_primitives::{Address, B256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogOrder {
    #[default]
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum TopicConstraint {
    #[default]
    Any,
    One(B256),
    AnyOf(Vec<B256>),
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct NativeLogFilter {
    pub from_block: Option<u64>,
    pub to_block: Option<u64>,
    pub block_hash: Option<B256>,
    pub addresses: Vec<Address>,
    pub topics: [TopicConstraint; 4],
    pub canonical_only: bool,
    pub order: LogOrder,
    pub limit: Option<usize>,
}

impl NativeLogFilter {
    pub fn new() -> Self {
        Self {
            canonical_only: true,
            ..Self::default()
        }
    }

    pub fn with_block_range(mut self, from_block: Option<u64>, to_block: Option<u64>) -> Self {
        self.from_block = from_block;
        self.to_block = to_block;
        self
    }

    pub fn with_block_hash(mut self, block_hash: B256) -> Self {
        self.block_hash = Some(block_hash);
        self
    }

    pub fn with_addresses(mut self, addresses: Vec<Address>) -> Self {
        self.addresses = addresses;
        self
    }

    pub fn with_topic(mut self, index: usize, constraint: TopicConstraint) -> Self {
        if index < self.topics.len() {
            self.topics[index] = constraint;
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_canonical_eth_get_logs_style_queries() {
        let filter = NativeLogFilter::new();
        assert!(filter.canonical_only);
        assert_eq!(filter.order, LogOrder::Ascending);
        assert!(matches!(filter.topics[0], TopicConstraint::Any));
        assert!(filter.block_hash.is_none());
    }

    #[test]
    fn builder_helpers_set_expected_constraints() {
        let block_hash = B256::repeat_byte(0x11);
        let address = Address::repeat_byte(0x22);
        let topic = B256::repeat_byte(0x33);

        let filter = NativeLogFilter::new()
            .with_block_range(Some(100), Some(200))
            .with_block_hash(block_hash)
            .with_addresses(vec![address])
            .with_topic(0, TopicConstraint::One(topic));

        assert_eq!(filter.from_block, Some(100));
        assert_eq!(filter.to_block, Some(200));
        assert_eq!(filter.block_hash, Some(block_hash));
        assert_eq!(filter.addresses, vec![address]);
        assert!(matches!(filter.topics[0], TopicConstraint::One(value) if value == topic));
    }
}
