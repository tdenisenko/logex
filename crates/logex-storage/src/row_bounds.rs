use logex_types::LogRow;

/// Exact count and extrema of an in-memory batch, independent of physical order.
/// This summary is ephemeral; catalog/source serialization does not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RowBounds {
    pub(crate) row_count: u64,
    pub(crate) min_block: u64,
    pub(crate) max_block: u64,
    pub(crate) min_timestamp: u64,
    pub(crate) max_timestamp: u64,
}

impl RowBounds {
    pub(crate) fn from_row(row: &LogRow) -> Self {
        Self {
            row_count: 1,
            min_block: row.block_number,
            max_block: row.block_number,
            min_timestamp: row.timestamp,
            max_timestamp: row.timestamp,
        }
    }

    pub(crate) fn include(&mut self, row: &LogRow) {
        self.row_count += 1;
        self.min_block = self.min_block.min(row.block_number);
        self.max_block = self.max_block.max(row.block_number);
        self.min_timestamp = self.min_timestamp.min(row.timestamp);
        self.max_timestamp = self.max_timestamp.max(row.timestamp);
    }

    pub(crate) fn from_rows(rows: &[LogRow]) -> Option<Self> {
        let (first, rest) = rows.split_first()?;
        let mut bounds = Self::from_row(first);
        for row in rest {
            bounds.include(row);
        }
        Some(bounds)
    }
}
