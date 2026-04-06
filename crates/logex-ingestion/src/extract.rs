use alloy_primitives::{B256, Log};

use logex_types::{BlockContext, LogRow};

/// Extract `LogRow`s from a block's receipts.
///
/// Takes the block context (number, hash, timestamp), a list of (tx_hash, receipt_logs)
/// pairs, and produces a flat list of `LogRow`s with correct indexing.
pub fn extract_logs(ctx: &BlockContext, txs: &[(B256, Vec<Log>)]) -> Vec<LogRow> {
    let mut rows = Vec::new();
    let mut global_log_index = 0u32;

    for (tx_index, (tx_hash, logs)) in txs.iter().enumerate() {
        for log in logs {
            rows.push(LogRow::from_primitives_log(
                log,
                ctx,
                *tx_hash,
                tx_index as u32,
                global_log_index,
            ));
            global_log_index += 1;
        }
    }

    rows
}

/// Convenience: extract logs from a pre-flattened receipt list where each
/// receipt is represented as (tx_hash, logs_vec).
///
/// This is the primary extraction path used by the sync engine where we iterate
/// block bodies and their corresponding receipts.
pub fn extract_from_block(
    block_number: u64,
    block_hash: B256,
    timestamp: u64,
    txs: &[(B256, Vec<Log>)],
) -> Vec<LogRow> {
    let ctx = BlockContext {
        block_number,
        block_hash,
        timestamp,
    };
    extract_logs(&ctx, txs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes as ABytes, bytes};

    fn make_log(addr: Address, topics: Vec<B256>, data: ABytes) -> Log {
        Log::new(addr, topics, data).unwrap()
    }

    #[test]
    fn test_extract_logs_empty() {
        let ctx = BlockContext {
            block_number: 100,
            block_hash: B256::repeat_byte(0x01),
            timestamp: 1_700_000_000,
        };
        let rows = extract_logs(&ctx, &[]);
        assert!(rows.is_empty());
    }

    #[test]
    fn test_extract_logs_multiple_txs() {
        let ctx = BlockContext {
            block_number: 100,
            block_hash: B256::repeat_byte(0x01),
            timestamp: 1_700_000_000,
        };

        let topic0 = B256::repeat_byte(0xDD);

        let txs = vec![
            (
                B256::repeat_byte(0x11),
                vec![
                    make_log(Address::repeat_byte(0xAA), vec![topic0], bytes!("cafe")),
                    make_log(Address::repeat_byte(0xBB), vec![topic0], bytes!("cafe")),
                ],
            ),
            (
                B256::repeat_byte(0x22),
                vec![make_log(
                    Address::repeat_byte(0xCC),
                    vec![topic0],
                    bytes!("cafe"),
                )],
            ),
        ];

        let rows = extract_logs(&ctx, &txs);
        assert_eq!(rows.len(), 3);

        // First tx, first log
        assert_eq!(rows[0].tx_index, 0);
        assert_eq!(rows[0].log_index, 0);
        assert_eq!(rows[0].address, Address::repeat_byte(0xAA));
        assert_eq!(rows[0].tx_hash, B256::repeat_byte(0x11));

        // First tx, second log
        assert_eq!(rows[1].tx_index, 0);
        assert_eq!(rows[1].log_index, 1);
        assert_eq!(rows[1].address, Address::repeat_byte(0xBB));

        // Second tx, first log — global log_index continues
        assert_eq!(rows[2].tx_index, 1);
        assert_eq!(rows[2].log_index, 2);
        assert_eq!(rows[2].address, Address::repeat_byte(0xCC));
        assert_eq!(rows[2].tx_hash, B256::repeat_byte(0x22));

        // All rows share block context
        for row in &rows {
            assert_eq!(row.block_number, 100);
            assert_eq!(row.block_hash, B256::repeat_byte(0x01));
            assert_eq!(row.timestamp, 1_700_000_000);
        }
    }

    #[test]
    fn test_extract_from_block() {
        let topic0 = B256::repeat_byte(0xDD);

        let txs = vec![(
            B256::repeat_byte(0x11),
            vec![make_log(
                Address::repeat_byte(0xAA),
                vec![topic0],
                bytes!(""),
            )],
        )];

        let rows = extract_from_block(500, B256::repeat_byte(0x05), 1_700_005_000, &txs);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].block_number, 500);
        assert_eq!(rows[0].topic0, Some(topic0));
    }
}
