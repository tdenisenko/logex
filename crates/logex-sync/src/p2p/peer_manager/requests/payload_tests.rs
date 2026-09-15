use super::*;
use alloy_consensus::{Eip658Value, SignableTransaction, TxLegacy, TxReceipt as _, TxType};
use alloy_primitives::{Address, Bytes, Log, LogData, Signature, U256};
use alloy_rlp::Encodable;
use std::cell::Cell;

struct CountedValue<'a> {
    value: u64,
    encode_calls: &'a Cell<usize>,
}

impl Encodable for CountedValue<'_> {
    fn encode(&self, out: &mut dyn alloy_rlp::BufMut) {
        self.encode_calls.set(self.encode_calls.get() + 1);
        self.value.encode(out);
    }

    fn length(&self) -> usize {
        self.value.length()
    }
}

#[test]
fn header_payload_sizing_does_not_encode_values() {
    let encode_calls = Cell::new(0);
    let values = [
        CountedValue {
            value: 1,
            encode_calls: &encode_calls,
        },
        CountedValue {
            value: 128,
            encode_calls: &encode_calls,
        },
    ];
    let actual = headers_payload_bytes(&values);
    assert_eq!(
        encode_calls.get(),
        0,
        "telemetry sizing must not encode a second payload"
    );
    let expected = alloy_rlp::encode(vec![1u64, 128]).len() as u64;
    assert_eq!(actual, expected);
}

#[test]
fn header_payload_metric_matches_normalized_rlp_encoding() {
    let first = alloy_consensus::Header::default();
    let second = alloy_consensus::Header {
        number: 128,
        extra_data: Bytes::from_static(b"ordinary fixture"),
        ..Default::default()
    };
    for headers in [vec![], vec![first], vec![second]] {
        let encoded = alloy_rlp::encode(&headers);
        assert_eq!(headers_payload_bytes(&headers), encoded.len() as u64);
    }
}

#[test]
fn body_payload_metric_matches_normalized_rlp_encoding() {
    let empty = reth_ethereum_primitives::BlockBody::default();
    let mut populated = empty.clone();
    // Structural transaction only; never submitted or executed.
    populated.transactions.push(
        TxLegacy::default()
            .into_signed(Signature::new(U256::from(1), U256::from(2), false))
            .into(),
    );
    for bodies in [vec![], vec![empty], vec![populated]] {
        let encoded = alloy_rlp::encode(&bodies);
        assert_eq!(
            raw_block_bodies_payload_bytes(&bodies),
            encoded.len() as u64
        );
    }
}

#[test]
fn receipt_payload_metric_matches_normalized_rlp_encoding() {
    let make_receipt = |tx_type, status, with_log| {
        let receipt = LogexReceipt {
            tx_type,
            status,
            cumulative_gas_used: 21_000,
            logs: if with_log {
                vec![Log {
                    address: Address::repeat_byte(1),
                    data: LogData::new_unchecked(
                        vec![B256::repeat_byte(2)],
                        Bytes::from_static(b"ordinary log"),
                    ),
                }]
            } else {
                Vec::new()
            },
        };
        alloy_consensus::ReceiptWithBloom {
            logs_bloom: receipt.bloom(),
            receipt,
        }
    };
    let legacy = make_receipt(TxType::Legacy, Eip658Value::success(), false);
    let post_state = make_receipt(
        TxType::Legacy,
        Eip658Value::PostState(B256::repeat_byte(3)),
        true,
    );
    let typed = make_receipt(TxType::Eip1559, Eip658Value::success(), true);
    // These are normalized decoded receipts with blooms, including for ETH69/70.
    // The metric does not claim to equal their original compressed wire encoding.
    let batches: [ReceiptBatch; 5] = [
        vec![],
        vec![vec![]],
        vec![vec![legacy]],
        vec![vec![post_state]],
        vec![vec![typed]],
    ];
    for receipts in batches {
        let encoded = alloy_rlp::encode(&receipts);
        assert_eq!(receipt_batch_payload_bytes(&receipts), encoded.len() as u64);
    }
}

#[tokio::test]
async fn downloading_payload_does_not_add_uploaded_payload() {
    let mut fixture = super::limit_tests::Fixture::new().await;
    fixture.manager.serve_cache.record_p2p_upload_payload(96);
    fixture
        .manager
        .record_p2p_download_payload(1_000, Duration::from_secs(1));
    let status = fixture.manager.execution_network_status();
    assert_eq!(status.p2p_downloaded_payload_bytes, 1_000);
    assert_eq!(status.p2p_uploaded_payload_bytes, 96);
}
