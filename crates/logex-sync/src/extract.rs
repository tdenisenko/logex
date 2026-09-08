use alloy_consensus::transaction::TxHashRef;
use alloy_primitives::{B256, Log};
use eyre::{Result, WrapErr, ensure};
use reth_primitives_traits::BlockBody;

use logex_types::{BlockContext, LogRow};

/// Count rows before allocation, including across blocks in a historical batch.
pub(crate) fn checked_row_count(counts: impl IntoIterator<Item = usize>) -> Result<usize> {
    counts.into_iter().try_fold(0usize, |total, count| {
        total
            .checked_add(count)
            .ok_or_else(|| eyre::eyre!("log row count exceeds usize"))
    })
}

fn checked_log_count(counts: impl IntoIterator<Item = usize>) -> Result<usize> {
    let total = checked_row_count(counts)?;
    check_indexed_count(total, "block log")?;
    Ok(total)
}

// Check the last index rather than the count: u32::MAX is a valid row index.
fn check_indexed_count(count: usize, field: &str) -> Result<()> {
    if let Some(last) = count.checked_sub(1) {
        u32::try_from(last).wrap_err_with(|| format!("{field} index exceeds u32"))?;
    }
    Ok(())
}

/// Extract authenticated receipt logs using caller-supplied block/tx metadata.
/// Returns an error if the logs cannot be represented without truncation.
pub fn extract_logs(ctx: &BlockContext, txs: &[(B256, Vec<Log>)]) -> Result<Vec<LogRow>> {
    check_indexed_count(txs.len(), "transaction")?;
    let total = checked_log_count(txs.iter().map(|(_, logs)| logs.len()))?;
    let mut rows = Vec::new();
    rows.try_reserve(total)
        .wrap_err("reserve extracted log rows")?;
    append_logs(
        &mut rows,
        ctx,
        txs.iter().map(|(hash, logs)| (*hash, logs.as_slice())),
    )?;
    Ok(rows)
}

/// Convenience wrapper for the primary sync path.
pub fn extract_from_block(
    block_number: u64,
    block_hash: B256,
    timestamp: u64,
    txs: &[(B256, Vec<Log>)],
) -> Result<Vec<LogRow>> {
    extract_logs(
        &BlockContext {
            block_number,
            block_hash,
            timestamp,
        },
        txs,
    )
}

/// Append a complete block's authenticated receipts to an existing batch.
/// On error, existing rows are unchanged and no rows from this block remain.
/// Allocation capacity may grow. This checks shape, not receipt provenance.
pub fn append_from_body_receipts<B, R>(
    rows: &mut Vec<LogRow>,
    block_number: u64,
    block_hash: B256,
    timestamp: u64,
    body: &B,
    receipts: &[R],
) -> Result<()>
where
    B: BlockBody,
    B::Transaction: TxHashRef,
    R: alloy_consensus::TxReceipt<Log = Log>,
{
    ensure!(
        body.transactions().len() == receipts.len(),
        "transaction/receipt count mismatch: transactions={}, receipts={}",
        body.transactions().len(),
        receipts.len()
    );
    check_indexed_count(receipts.len(), "transaction")?;
    let total = checked_log_count(receipts.iter().map(|receipt| receipt.logs().len()))?;
    rows.try_reserve(total)
        .wrap_err("reserve extracted log rows")?;
    append_logs(
        rows,
        &BlockContext {
            block_number,
            block_hash,
            timestamp,
        },
        body.transactions()
            .iter()
            .zip(receipts)
            .map(|(tx, receipt)| (*tx.tx_hash(), receipt.logs())),
    )
}

fn append_logs<'a>(
    rows: &mut Vec<LogRow>,
    ctx: &BlockContext,
    txs: impl Iterator<Item = (B256, &'a [Log])>,
) -> Result<()> {
    let start = rows.len();
    let result = (|| -> Result<()> {
        for (tx_index, (tx_hash, logs)) in txs.enumerate() {
            let tx_index = u32::try_from(tx_index)
                .map_err(|_| eyre::eyre!("transaction index exceeds u32"))?;
            for log in logs {
                let log_index = u32::try_from(rows.len() - start)
                    .map_err(|_| eyre::eyre!("block log index exceeds u32"))?;
                let row =
                    match LogRow::try_from_primitives_log(log, ctx, tx_hash, tx_index, log_index) {
                        Ok(row) => row,
                        Err(error) => {
                            return Err(row_conversion_error(
                                error,
                                ctx.block_number,
                                tx_index,
                                log_index,
                            ));
                        }
                    };
                rows.push(row);
            }
        }
        Ok(())
    })();
    if result.is_err() {
        rows.truncate(start);
    }
    result
}

#[cold]
fn row_conversion_error(
    error: logex_types::LogRowConversionError,
    block_number: u64,
    tx_index: u32,
    log_index: u32,
) -> eyre::Report {
    eyre::Report::new(error).wrap_err(format!(
        "block {block_number} transaction {tx_index} log {log_index}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, Bytes as ABytes, bytes};

    fn make_log(addr: Address, topics: Vec<B256>, data: ABytes) -> Log {
        Log::new(addr, topics, data).unwrap()
    }

    #[test]
    fn extraction_rejects_receipt_count_mismatch_without_changing_rows() {
        let body = reth_ethereum_primitives::BlockBody::default();
        let receipts = vec![crate::primitives::LogexReceipt::default()];
        let mut rows = Vec::new();
        let result = append_from_body_receipts(&mut rows, 0, B256::ZERO, 0, &body, &receipts);
        assert!(result.is_err());
        assert!(rows.is_empty());
    }

    #[test]
    fn extraction_count_boundaries_do_not_require_large_allocations() {
        assert_eq!(checked_log_count([]).unwrap(), 0);
        assert_eq!(checked_log_count([0, 1, 4]).unwrap(), 5);
        assert!(checked_log_count([usize::MAX, 1]).is_err());
        let max = u32::MAX as usize;
        assert_eq!(checked_log_count([max]).unwrap(), max);
        if let Some(count) = max.checked_add(1) {
            assert_eq!(checked_log_count([max, 1]).unwrap(), count);
            assert!(check_indexed_count(count, "transaction").is_ok());
            assert!(checked_log_count([max, 2]).is_err());
            assert!(check_indexed_count(count + 1, "transaction").is_err());
        }
    }

    #[test]
    fn invalid_later_log_rolls_back_the_entire_appended_block() {
        use crate::primitives::LogexReceipt;
        use alloy_consensus::{SignableTransaction, TxLegacy};
        use alloy_primitives::{LogData, Signature, U256};
        let good = make_log(Address::ZERO, vec![], bytes!("1234"));
        let bad = Log {
            address: Address::ZERO,
            data: LogData::new_unchecked(vec![B256::ZERO; 5], ABytes::new()),
        };
        let ctx = BlockContext {
            block_number: 9,
            block_hash: B256::ZERO,
            timestamp: 0,
        };
        let original = extract_logs(&ctx, &[(B256::ZERO, vec![good.clone()])]).unwrap();
        let mut body = reth_ethereum_primitives::BlockBody::default();
        for nonce in 0..3 {
            body.transactions.push(
                TxLegacy {
                    nonce,
                    ..Default::default()
                }
                .into_signed(Signature::new(U256::from(1), U256::from(2), false))
                .into(),
            );
        }
        let mut receipts = vec![
            LogexReceipt {
                logs: vec![good.clone()],
                ..Default::default()
            },
            LogexReceipt::default(),
            LogexReceipt {
                logs: vec![good, bad],
                ..Default::default()
            },
        ];
        let mut rows = original.clone();
        let error =
            append_from_body_receipts(&mut rows, 10, B256::ZERO, 0, &body, &receipts).unwrap_err();
        assert!(format!("{error:#}").contains("block 10 transaction 2 log 2"));
        assert_eq!(rows, original);
        let txs: Vec<_> = body
            .transactions
            .iter()
            .zip(&receipts)
            .map(|(tx, receipt)| (*tx.tx_hash(), receipt.logs.clone()))
            .collect();
        assert!(extract_logs(&ctx, &txs).is_err());
        // Too few and too many receipts both fail before appending.
        for count in [2, 4] {
            receipts.resize_with(count, Default::default);
            assert!(
                append_from_body_receipts(&mut rows, 10, B256::ZERO, 0, &body, &receipts).is_err()
            );
            assert_eq!(rows, original);
        }
    }

    #[test]
    fn extract_logs_handles_empty_blocks() {
        let ctx = BlockContext {
            block_number: 100,
            block_hash: B256::repeat_byte(0x01),
            timestamp: 1_700_000_000,
        };
        let rows = extract_logs(&ctx, &[]).unwrap();
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

        let rows = extract_logs(&ctx, &txs).unwrap();
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
        let expected = extract_logs(&ctx, &txs).unwrap();
        let mut rows = expected[..1].to_vec(); // appending must preserve an existing batch
        append_from_body_receipts(
            &mut rows,
            ctx.block_number,
            ctx.block_hash,
            ctx.timestamp,
            &body,
            &receipts,
        )
        .unwrap();
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

        let rows = extract_from_block(500, B256::repeat_byte(0x05), 1_700_005_000, &txs).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].block_number, 500);
        assert_eq!(rows[0].block_hash, B256::repeat_byte(0x05));
        assert_eq!(rows[0].topic0, Some(topic0));
    }
    #[test]
    #[ignore = "release comparison; see docs/audit/extraction-boundaries.md"]
    fn extraction_release_baseline() {
        use alloy_consensus::{SignableTransaction, TxLegacy};
        use alloy_primitives::{Signature, U256};
        use std::{hint::black_box, time::Instant};
        let ctx = BlockContext {
            block_number: 20_000_000,
            block_hash: B256::repeat_byte(3),
            timestamp: 1_800_000_000,
        };
        let mut body = reth_ethereum_primitives::BlockBody::default();
        let mut receipts = Vec::new();
        let mut txs = Vec::new();
        for nonce in 0..4096 {
            body.transactions.push(
                TxLegacy {
                    nonce,
                    ..Default::default()
                }
                .into_signed(Signature::new(U256::from(1), U256::from(2), false))
                .into(),
            );
            let logs: Vec<_> = (0..nonce % 5)
                .map(|index| {
                    make_log(
                        Address::repeat_byte(index as u8),
                        vec![B256::repeat_byte(index as u8); index as usize],
                        ABytes::from(vec![7; 256]),
                    )
                })
                .collect();
            txs.push((*body.transactions.last().unwrap().tx_hash(), logs.clone()));
            receipts.push(crate::primitives::LogexReceipt {
                logs,
                ..Default::default()
            });
        }
        let expected: Vec<_> = txs
            .iter()
            .enumerate()
            .flat_map(|(tx, (hash, logs))| logs.iter().map(move |log| (tx, *hash, log)))
            .enumerate()
            .map(|(index, (tx, hash, log))| LogRow {
                block_number: ctx.block_number,
                block_hash: ctx.block_hash,
                timestamp: ctx.timestamp,
                tx_hash: hash,
                tx_index: tx as u32,
                log_index: index as u32,
                address: log.address,
                topic0: log.topics().first().copied(),
                topic1: log.topics().get(1).copied(),
                topic2: log.topics().get(2).copied(),
                topic3: log.topics().get(3).copied(),
                data: log.data.data.clone(),
                data_len: 256,
                source: logex_types::Source::Receipt,
            })
            .collect();
        assert_eq!(extract_logs(&ctx, &txs).unwrap(), expected);
        let mut reused = Vec::new();
        append_from_body_receipts(
            &mut reused,
            ctx.block_number,
            ctx.block_hash,
            ctx.timestamp,
            &body,
            &receipts,
        )
        .unwrap();
        assert_eq!(reused, expected);
        for path in ["owned", "append"] {
            for sample in 0..6 {
                let started = Instant::now();
                for _ in 0..100 {
                    if path == "owned" {
                        black_box(extract_logs(black_box(&ctx), black_box(&txs)).unwrap());
                    } else {
                        reused.clear();
                        append_from_body_receipts(
                            &mut reused,
                            ctx.block_number,
                            ctx.block_hash,
                            ctx.timestamp,
                            black_box(&body),
                            black_box(&receipts),
                        )
                        .unwrap();
                        black_box(&reused);
                    }
                }
                if sample != 0 {
                    eprintln!(
                        "EXTRACTION_BENCH {{\"path\":\"{path}\",\"sample\":{sample},\"iterations\":100,\"rows\":{},\"elapsed_ns\":{}}}",
                        expected.len(),
                        started.elapsed().as_nanos()
                    );
                }
            }
        }
    }
}
