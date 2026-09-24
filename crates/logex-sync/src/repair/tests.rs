use super::*;
use crate::primitives::LogexReceipt;
use alloy_consensus::{ReceiptWithBloom, SignableTransaction, TxLegacy, proofs};
use alloy_primitives::{Address, Log, LogData, Signature, U256};
use reth_ethereum_primitives::BlockBody as EthereumBody;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct Scripted {
    headers: BTreeMap<B256, Header>,
    payloads: BTreeMap<B256, (EthereumBody, Vec<ReceiptWithBloom<LogexReceipt>>)>,
    pub(super) header_calls: Vec<(B256, u64)>,
    pub(super) body_calls: Vec<u64>,
    pub(super) receipt_calls: Vec<u64>,
    partial: bool,
    empty_headers: bool,
    overlong_headers: bool,
    pub(super) pending_headers: bool,
    pending_body: bool,
    pending_receipts: bool,
    wrong_body: bool,
    wrong_receipts: bool,
    missing_body: bool,
    body_error: bool,
}
impl RepairSource for Scripted {
    async fn headers(
        &mut self,
        mut start: B256,
        count: u64,
        _: Duration,
        _: usize,
    ) -> Result<(PeerId, Vec<Header>)> {
        self.header_calls.push((start, count));
        if self.pending_headers {
            std::future::pending::<()>().await;
        }
        if self.empty_headers {
            return Ok((PeerId::ZERO, Vec::new()));
        }
        let mut headers = Vec::new();
        for _ in 0..if self.partial { 1 } else { count } {
            let Some(header) = self.headers.get(&start) else {
                break;
            };
            headers.push(header.clone());
            start = header.parent_hash;
        }
        if self.overlong_headers {
            headers.push(headers[0].clone());
        }
        Ok((PeerId::ZERO, headers))
    }
    async fn body(
        &mut self,
        hash: B256,
        number: u64,
        _: Duration,
        _: usize,
    ) -> Result<Vec<SourcedBlockBody>> {
        self.body_calls.push(number);
        if self.body_error {
            return Err(eyre::eyre!("scripted peers lack ancient payload"));
        }
        if self.pending_body {
            std::future::pending::<()>().await;
        }
        if self.missing_body {
            return Ok(Vec::new());
        }
        let mut body = self.payloads[&hash].0.clone();
        if self.wrong_body {
            body.transactions.clear();
        }
        Ok(vec![(PeerId::ZERO, body)])
    }
    async fn receipts(
        &mut self,
        header: &Header,
        _: PeerId,
        _: Duration,
        _: usize,
    ) -> Result<Vec<SourcedReceiptSet>> {
        self.receipt_calls.push(header.number);
        if self.pending_receipts {
            std::future::pending::<()>().await;
        }
        let mut receipts = self.payloads[&header.hash_slow()].1.clone();
        if self.wrong_receipts {
            receipts[1].receipt.logs.clear();
        }
        Ok(vec![(PeerId::ZERO, receipts)])
    }
}

pub(super) fn fixture() -> (Scripted, Vec<Header>) {
    fixture_with_logs(&[2])
}

pub(super) fn fixture_with_logs(logged_blocks: &[u64]) -> (Scripted, Vec<Header>) {
    let mut source = Scripted::default();
    let mut headers: Vec<Header> = Vec::new();
    for index in 0..4 {
        let mut body = EthereumBody::default();
        let mut receipts = Vec::new();
        if index == 1 || index == 2 || logged_blocks.contains(&index) {
            // A no-log transaction precedes two logging transactions, preserving
            // transaction positions independently of row count.
            for tx_index in 0..3 {
                body.transactions.push(
                    TxLegacy {
                        nonce: tx_index,
                        gas_limit: 21_000,
                        ..Default::default()
                    }
                    .into_signed(Signature::new(U256::from(1), U256::from(2), false))
                    .into(),
                );
                let receipt = LogexReceipt {
                    cumulative_gas_used: (tx_index + 1) * 21_000,
                    logs: if logged_blocks.contains(&index) && tx_index > 0 {
                        vec![Log {
                            address: Address::repeat_byte(7),
                            data: LogData::new_unchecked(
                                vec![B256::repeat_byte(8)],
                                vec![tx_index as u8; 4].into(),
                            ),
                        }]
                    } else {
                        Vec::new()
                    },
                    ..Default::default()
                };
                receipts.push(ReceiptWithBloom {
                    logs_bloom: receipt.bloom(),
                    receipt,
                });
            }
        }
        let header = Header {
            number: 4_370_000 + index,
            gas_limit: 100_000,
            gas_used: receipts
                .last()
                .map_or(0, |receipt| receipt.receipt.cumulative_gas_used),
            timestamp: 1000 + index,
            parent_hash: headers.last().map_or(B256::ZERO, Header::hash_slow),
            transactions_root: body.calculate_tx_root(),
            ommers_hash: body.calculate_ommers_root(),
            receipts_root: proofs::calculate_receipt_root(&receipts),
            logs_bloom: receipts.iter().fold(Default::default(), |bloom, receipt| {
                bloom | receipt.logs_bloom
            }),
            ..Default::default()
        };
        source.headers.insert(header.hash_slow(), header.clone());
        source.payloads.insert(header.hash_slow(), (body, receipts));
        headers.push(header);
    }
    (source, headers)
}
pub(super) fn anchor(header: &Header) -> ExecutionAnchor {
    ExecutionAnchor {
        beacon_root: B256::repeat_byte(1),
        beacon_slot: 10,
        block_number: header.number,
        block_hash: header.hash_slow(),
        receipts_root: header.receipts_root,
    }
}
pub(super) fn limits() -> RepairFetchLimits {
    RepairFetchLimits {
        header_page_size: 2,
        max_headers: 8,
        max_transactions_per_block: 8,
        max_encoded_body_bytes: 4096,
        max_rows_per_block: 8,
        max_log_data_bytes_per_block: 64,
        request_timeout: Duration::from_secs(1),
        max_attempts: 2,
        deadline: Instant::now() + Duration::from_secs(10),
    }
}
fn cursor(headers: &[Header], start: usize, end: usize) -> RepairFetcher {
    RepairFetcher::new(
        RepairRange {
            start: headers[start].number,
            end: headers[end].number,
        },
        anchor(headers.last().unwrap()),
        limits(),
        CancellationToken::new(),
    )
    .unwrap()
}

#[tokio::test]
async fn repair_fetch_bridges_anchor_and_delivers_exact_range_including_empty_blocks() {
    for partial in [false, true] {
        let (mut source, headers) = fixture();
        source.partial = partial;
        let mut fetch = cursor(&headers, 0, 2);
        let mut delivered = Vec::new();
        loop {
            match fetch.next_block(&mut source).await.unwrap() {
                RepairFetchStep::Block(block) => delivered.push(block),
                RepairFetchStep::Complete(done) => {
                    assert_eq!(
                        done.range(),
                        RepairRange {
                            start: headers[0].number,
                            end: headers[2].number
                        }
                    );
                    assert_eq!(done.anchor(), anchor(&headers[3]));
                    assert_eq!(done.delivered_blocks(), 3);
                    break;
                }
            }
        }
        assert_eq!(
            delivered
                .iter()
                .map(|block| block.header().number)
                .collect::<Vec<_>>(),
            vec![headers[2].number, headers[1].number, headers[0].number]
        );
        assert_eq!(
            delivered[0]
                .rows()
                .iter()
                .map(|row| (row.tx_index, row.log_index))
                .collect::<Vec<_>>(),
            vec![(1, 0), (2, 1)]
        );
        assert!(delivered[1].rows().is_empty());
        assert!(delivered[2].rows().is_empty());
        assert_eq!(
            source.body_calls,
            vec![headers[2].number, headers[1].number, headers[0].number]
        );
        assert_eq!(source.receipt_calls, source.body_calls);
        assert_eq!(source.header_calls[0], (headers[3].hash_slow(), 1));
    }
}

#[tokio::test]
async fn repair_fetch_rejects_bad_anchor_and_incomplete_or_oversized_header_responses() {
    for mode in 0..4 {
        let (mut source, headers) = fixture();
        let mut fetch = cursor(&headers, 0, 2);
        match mode {
            0 => fetch.anchor.receipts_root = B256::ZERO,
            1 => source.empty_headers = true,
            2 => source.overlong_headers = true,
            _ => {
                let hash = headers[3].hash_slow();
                source.headers.get_mut(&hash).unwrap().gas_limit = 0;
            }
        }
        assert!(fetch.next_block(&mut source).await.is_err());
        assert_eq!(fetch.delivered, 0);
        assert!(source.body_calls.is_empty());
        assert!(
            fetch
                .next_block(&mut source)
                .await
                .unwrap_err()
                .to_string()
                .contains("terminal")
        );
    }
}

#[tokio::test]
async fn repair_fetch_rejects_payloads_before_delivery_and_never_marks_prefix_complete() {
    for mode in 0..4 {
        let (mut source, headers) = fixture();
        let mut fetch = cursor(&headers, 0, 2);
        match mode {
            0 => source.wrong_body = true,
            1 => source.wrong_receipts = true,
            2 => source.missing_body = true,
            _ => fetch.limits.max_rows_per_block = 1,
        }
        assert!(fetch.next_block(&mut source).await.is_err());
        assert_eq!(fetch.delivered, 0);
        if mode == 0 {
            assert!(source.receipt_calls.is_empty());
        }
        assert!(fetch.next_block(&mut source).await.is_err());
    }
    let (mut source, headers) = fixture();
    let mut fetch = cursor(&headers, 0, 2);
    assert!(matches!(
        fetch.next_block(&mut source).await.unwrap(),
        RepairFetchStep::Block(_)
    ));
    source.missing_body = true;
    assert!(fetch.next_block(&mut source).await.is_err());
    assert_eq!(fetch.delivered, 1);
    assert!(fetch.next_block(&mut source).await.is_err());
}

#[tokio::test(start_paused = true)]
async fn repair_fetch_deadline_and_dropped_wait_poison_cursor() {
    let (mut source, headers) = fixture();
    source.pending_headers = true;
    let mut fetch = cursor(&headers, 0, 2);
    assert!(
        fetch
            .next_block(&mut source)
            .await
            .unwrap_err()
            .to_string()
            .contains("deadline")
    );
    assert!(fetch.poisoned);
    let (mut source, headers) = fixture();
    source.pending_headers = true;
    let mut fetch = cursor(&headers, 0, 2);
    {
        let future = fetch.next_block(&mut source);
        tokio::pin!(future);
        tokio::select! { biased; _ = &mut future => panic!("scripted request must stay pending"), _ = tokio::task::yield_now() => {} }
    }
    source.pending_headers = false;
    assert!(
        fetch
            .next_block(&mut source)
            .await
            .unwrap_err()
            .to_string()
            .contains("terminal")
    );
    assert_eq!(source.header_calls.len(), 1);
}

#[tokio::test]
async fn repair_fetch_cancellation_and_invalid_inputs_do_not_request_work() {
    let (mut source, headers) = fixture();
    let mut fetch = cursor(&headers, 0, 2);
    fetch.cancellation.cancel();
    assert!(fetch.next_block(&mut source).await.is_err());
    assert!(source.header_calls.is_empty());
    let range = RepairRange {
        start: headers[0].number,
        end: headers[2].number,
    };
    for mode in 0..5 {
        let mut budget = limits();
        match mode {
            0 => budget.header_page_size = 0,
            1 => budget.max_headers = 1,
            2 => budget.max_attempts = 0,
            3 => budget.request_timeout = Duration::ZERO,
            _ => budget.max_encoded_body_bytes = 0,
        }
        assert!(
            RepairFetcher::new(range, anchor(&headers[3]), budget, CancellationToken::new())
                .is_err()
        );
    }
    let mut high = anchor(&headers[3]);
    high.block_number = u64::MAX;
    assert!(
        RepairFetcher::new(
            RepairRange {
                start: 0,
                end: u64::MAX
            },
            high,
            limits(),
            CancellationToken::new()
        )
        .is_err()
    );
    assert!(
        RepairFetcher::new(
            RepairRange {
                start: u64::MAX,
                end: u64::MAX
            },
            high,
            limits(),
            CancellationToken::new()
        )
        .is_ok()
    );
}

#[tokio::test]
async fn repair_fetch_cancellation_during_body_and_receipt_waits_is_terminal() {
    for receipts in [false, true] {
        let (mut source, headers) = fixture();
        source.pending_body = !receipts;
        source.pending_receipts = receipts;
        let mut fetch = cursor(&headers, 0, 2);
        let cancellation = fetch.cancellation.clone();
        let cancel = async {
            tokio::task::yield_now().await;
            cancellation.cancel();
        };
        let (result, ()) = tokio::join!(fetch.next_block(&mut source), cancel);
        assert_eq!(
            result
                .unwrap_err()
                .downcast_ref::<RepairFetchError>()
                .unwrap()
                .kind,
            RepairFetchErrorKind::Cancelled
        );
        assert_eq!(fetch.delivered, 0);
        assert_eq!(source.body_calls.len(), 1);
        assert_eq!(source.receipt_calls.len(), usize::from(receipts));
        assert!(fetch.next_block(&mut source).await.is_err());
    }
}

#[tokio::test]
async fn repair_fetch_rejects_broken_parent_at_next_page_after_valid_prefix() {
    let (mut source, headers) = fixture();
    let mut fetch = cursor(&headers, 0, 2);
    fetch.limits.header_page_size = 1;
    assert!(matches!(
        fetch.next_block(&mut source).await.unwrap(),
        RepairFetchStep::Block(_)
    ));
    source
        .headers
        .get_mut(&headers[1].hash_slow())
        .unwrap()
        .parent_hash = B256::repeat_byte(99);
    let error = fetch.next_block(&mut source).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<RepairFetchError>().unwrap().kind,
        RepairFetchErrorKind::InvalidData
    );
    assert_eq!(fetch.delivered, 1);
    assert_eq!(source.body_calls, vec![headers[2].number]);
}

#[tokio::test]
async fn repair_fetch_validates_pre_byzantium_post_state_receipts() {
    let (original, headers) = fixture();
    let (body, mut receipts) = original.payloads[&headers[2].hash_slow()].clone();
    for receipt in &mut receipts {
        receipt.receipt.status = alloy_consensus::Eip658Value::PostState(B256::repeat_byte(11));
    }
    let header = Header {
        number: 4_000_000,
        receipts_root: proofs::calculate_receipt_root(&receipts),
        ..headers[2].clone()
    };
    let mut source = Scripted::default();
    source.headers.insert(header.hash_slow(), header.clone());
    source.payloads.insert(header.hash_slow(), (body, receipts));
    let mut fetch = RepairFetcher::new(
        RepairRange {
            start: header.number,
            end: header.number,
        },
        anchor(&header),
        limits(),
        CancellationToken::new(),
    )
    .unwrap();
    let RepairFetchStep::Block(block) = fetch.next_block(&mut source).await.unwrap() else {
        panic!("expected block")
    };
    assert_eq!(block.rows().len(), 2);
    assert!(matches!(
        fetch.next_block(&mut source).await.unwrap(),
        RepairFetchStep::Complete(_)
    ));
}

#[tokio::test]
async fn repair_fetch_genesis_and_maximum_height_need_no_successor_request() {
    let (_, headers) = fixture();
    for header in [
        reth_chainspec::MAINNET.genesis_header().clone(),
        Header {
            number: u64::MAX,
            base_fee_per_gas: Some(1),
            ..headers[0].clone()
        },
    ] {
        let mut source = Scripted::default();
        source.headers.insert(header.hash_slow(), header.clone());
        source
            .payloads
            .insert(header.hash_slow(), (EthereumBody::default(), Vec::new()));
        let mut fetch = RepairFetcher::new(
            RepairRange {
                start: header.number,
                end: header.number,
            },
            anchor(&header),
            limits(),
            CancellationToken::new(),
        )
        .unwrap();
        let RepairFetchStep::Block(block) = fetch.next_block(&mut source).await.unwrap() else {
            panic!("expected block")
        };
        assert!(block.rows().is_empty());
        assert_eq!(block.header().number, header.number);
        assert!(matches!(
            fetch.next_block(&mut source).await.unwrap(),
            RepairFetchStep::Complete(_)
        ));
        assert_eq!(source.header_calls, vec![(header.hash_slow(), 1)]);
    }
}

#[tokio::test]
async fn repair_fetch_error_categories_distinguish_unavailable_limits_and_invalid_rows() {
    let (mut source, headers) = fixture();
    source.empty_headers = true;
    let mut fetch = cursor(&headers, 0, 2);
    assert_eq!(
        fetch
            .next_block(&mut source)
            .await
            .unwrap_err()
            .downcast_ref::<RepairFetchError>()
            .unwrap()
            .kind,
        RepairFetchErrorKind::Unavailable
    );
    let (mut source, headers) = fixture();
    let mut fetch = cursor(&headers, 0, 2);
    fetch.limits.max_log_data_bytes_per_block = 1;
    assert_eq!(
        fetch
            .next_block(&mut source)
            .await
            .unwrap_err()
            .downcast_ref::<RepairFetchError>()
            .unwrap()
            .kind,
        RepairFetchErrorKind::LimitExceeded
    );
    let (original, headers) = fixture();
    let (body, mut receipts) = original.payloads[&headers[2].hash_slow()].clone();
    receipts[1].receipt.logs[0].data = LogData::new_unchecked(vec![B256::ZERO; 5], vec![1].into());
    receipts[1].logs_bloom = receipts[1].receipt.bloom();
    let header = Header {
        receipts_root: proofs::calculate_receipt_root(&receipts),
        logs_bloom: receipts.iter().fold(Default::default(), |bloom, receipt| {
            bloom | receipt.logs_bloom
        }),
        ..headers[2].clone()
    };
    let mut source = Scripted::default();
    source.headers.insert(header.hash_slow(), header.clone());
    source.payloads.insert(header.hash_slow(), (body, receipts));
    let mut fetch = RepairFetcher::new(
        RepairRange {
            start: header.number,
            end: header.number,
        },
        anchor(&header),
        limits(),
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(
        fetch
            .next_block(&mut source)
            .await
            .unwrap_err()
            .downcast_ref::<RepairFetchError>()
            .unwrap()
            .kind,
        RepairFetchErrorKind::InvalidData
    );
    assert_eq!(fetch.delivered, 0);
}

#[tokio::test]
async fn repair_fetch_unavailable_source_error_preserves_requested_identity_and_cause() {
    let (mut source, headers) = fixture();
    source.body_error = true;
    let mut fetch = cursor(&headers, 0, 2);
    let error = fetch.next_block(&mut source).await.unwrap_err();
    assert_eq!(
        error.downcast_ref::<RepairFetchError>().unwrap().kind,
        RepairFetchErrorKind::Unavailable
    );
    let message = error.to_string();
    assert!(message.contains(&headers[2].number.to_string()));
    assert!(message.contains(&headers[2].hash_slow().to_string()));
    assert!(message.contains("scripted peers lack ancient payload"));
    assert!(source.receipt_calls.is_empty());
    assert_eq!(fetch.delivered, 0);
}
