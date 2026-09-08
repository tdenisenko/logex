use alloy_consensus::transaction::TxHashRef;
use alloy_primitives::{B256, Log};
use reth_primitives_traits::BlockBody;

use logex_types::{BlockContext, LogRow};

/// Extract `LogRow`s from a block's receipts.
///
/// Takes the block context (number, hash, timestamp), a list of `(tx_hash,
/// receipt_logs)` pairs, and produces a flat list of `LogRow`s with correct
/// per-transaction and global block log indexing.
pub fn extract_logs(ctx: &BlockContext, txs: &[(B256, Vec<Log>)]) -> Vec<LogRow> {
    let total_logs = txs.iter().map(|(_, logs)| logs.len()).sum();
    let mut rows = Vec::with_capacity(total_logs);
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

/// Convenience wrapper for the primary sync path where block context and
/// transaction receipts are handled separately by the engine.
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

/// Append `LogRow`s directly from a block body and receipts into an existing
/// buffer. This is used by historical batch ingestion to avoid per-block
/// temporary vectors while preserving transaction and log ordering.
pub fn append_from_body_receipts<B, R>(
    rows: &mut Vec<LogRow>,
    block_number: u64,
    block_hash: B256,
    timestamp: u64,
    body: &B,
    receipts: &[R],
) where
    B: BlockBody,
    B::Transaction: TxHashRef,
    R: alloy_consensus::TxReceipt<Log = Log>,
{
    let ctx = BlockContext {
        block_number,
        block_hash,
        timestamp,
    };
    let mut global_log_index = 0u32;

    for (tx_index, (tx, receipt)) in body.transactions().iter().zip(receipts.iter()).enumerate() {
        let tx_hash = *tx.tx_hash();
        for log in receipt.logs() {
            rows.push(LogRow::from_primitives_log(
                log,
                &ctx,
                tx_hash,
                tx_index as u32,
                global_log_index,
            ));
            global_log_index += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes as ABytes, bytes};

    fn make_log(addr: Address, topics: Vec<B256>, data: ABytes) -> Log {
        Log::new(addr, topics, data).unwrap()
    }

    #[test]
    fn extract_logs_handles_empty_blocks() {
        let ctx = BlockContext {
            block_number: 100,
            block_hash: B256::repeat_byte(0x01),
            timestamp: 1_700_000_000,
        };
        let rows = extract_logs(&ctx, &[]);
        assert!(rows.is_empty());
    }

    #[test]
    fn extract_logs_assigns_transaction_and_global_indices() {
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
        assert_eq!(rows[0].tx_index, 0);
        assert_eq!(rows[0].log_index, 0);
        assert_eq!(rows[1].tx_index, 0);
        assert_eq!(rows[1].log_index, 1);
        assert_eq!(rows[2].tx_index, 1);
        assert_eq!(rows[2].log_index, 2);
    }

    #[test]
    fn body_receipt_extraction_preserves_empty_transactions_and_topic_presence() {
        use crate::primitives::LogexReceipt;
        use alloy_consensus::{SignableTransaction, TxLegacy};
        use alloy_primitives::{Signature, U256};

        let mut body = reth_ethereum_primitives::BlockBody::default();
        let mut receipts = Vec::new();
        let mut txs = Vec::new();
        for tx_index in 0..11 {
            let tx = TxLegacy {
                nonce: tx_index,
                ..Default::default()
            }
            .into_signed(Signature::new(U256::from(1), U256::from(2), false));
            body.transactions.push(tx.into());
            let logs = if tx_index % 2 == 1 {
                vec![make_log(
                    Address::repeat_byte(tx_index as u8),
                    vec![B256::ZERO; (tx_index / 2) as usize],
                    bytes!("0001"),
                )]
            } else {
                Vec::new()
            };
            txs.push((*body.transactions.last().unwrap().tx_hash(), logs.clone()));
            receipts.push(LogexReceipt {
                logs,
                ..Default::default()
            });
        }
        let ctx = BlockContext {
            block_number: 100,
            block_hash: B256::repeat_byte(0x12),
            timestamp: 1_000,
        };
        let expected = extract_logs(&ctx, &txs);
        let mut rows = expected[..1].to_vec(); // appending must preserve an existing batch
        append_from_body_receipts(
            &mut rows,
            ctx.block_number,
            ctx.block_hash,
            ctx.timestamp,
            &body,
            &receipts,
        );
        assert_eq!(&rows[1..], expected);
        assert_eq!(rows[0], expected[0]);
        assert_eq!(expected.len(), 5);
        for (index, row) in expected.iter().enumerate() {
            assert_eq!(row.tx_index, (index * 2 + 1) as u32);
            assert_eq!(row.log_index, index as u32);
            assert_eq!(row.tx_hash, txs[index * 2 + 1].0);
            assert_eq!(row.address, Address::repeat_byte((index * 2 + 1) as u8));
            assert_eq!(
                [row.topic0, row.topic1, row.topic2, row.topic3],
                std::array::from_fn(|topic| (topic < index).then_some(B256::ZERO))
            );
            assert_eq!(row.data, bytes!("0001"));
            assert_eq!(row.data_len, 2);
            assert_eq!(row.source, logex_types::Source::Receipt);
        }
    }

    #[test]
    fn extract_from_block_builds_block_context() {
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
        assert_eq!(rows[0].block_hash, B256::repeat_byte(0x05));
        assert_eq!(rows[0].topic0, Some(topic0));
    }
}
