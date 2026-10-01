use super::*;
use alloy_consensus::{
    Eip658Value, Receipt, ReceiptEnvelope, SignableTransaction, TxEnvelope, TxLegacy,
};
use alloy_primitives::{Address, Bytes, Signature};

pub(crate) fn block(
    number: u64,
    parent_hash: B256,
    logs: usize,
) -> (Header, Vec<LogRow>, VerifiedBlockLogs) {
    let tx: TxEnvelope = TxLegacy {
        nonce: number,
        gas_limit: 100_000,
        ..Default::default()
    }
    .into_signed(Signature::test_signature())
    .into();
    let receipt = ReceiptEnvelope::Legacy(
        Receipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 21_000,
            logs: (0..logs)
                .map(|_| {
                    Log::new(
                        Address::repeat_byte(0x51),
                        vec![B256::repeat_byte(0x33)],
                        Bytes::from_static(b"identical event"),
                    )
                    .unwrap()
                })
                .collect(),
        }
        .with_bloom(),
    );
    let header = Header {
        number,
        parent_hash,
        timestamp: number + 1,
        gas_used: 21_000,
        transactions_root: proofs::calculate_transaction_root(std::slice::from_ref(&tx)),
        receipts_root: proofs::calculate_receipt_root(std::slice::from_ref(&receipt)),
        logs_bloom: receipt.bloom(),
        ..Default::default()
    };
    let mut rows = Vec::new();
    let proof =
        VerifiedBlockLogs::verify_and_append(&header, &[tx], &[receipt], &mut rows).unwrap();
    (header, rows, proof)
}

#[test]
fn binds_every_event_field_and_preserves_distinct_identical_emissions() {
    let (_, rows, proof) = block(42, B256::ZERO, 2);
    assert_eq!(rows[0].data, rows[1].data);
    assert_eq!(rows[0].log_index, 0);
    assert_eq!(rows[1].log_index, 1);
    assert!(VerifiedBlockLogs::verify_batch(std::slice::from_ref(&proof), &rows).is_ok());
    for field in 0..14 {
        let mut changed = rows.clone();
        let row = &mut changed[0];
        match field {
            0 => row.block_number += 1,
            1 => row.block_hash = B256::ZERO,
            2 => row.timestamp += 1,
            3 => row.tx_hash = B256::ZERO,
            4 => row.tx_index += 1,
            5 => row.log_index += 1,
            6 => row.address = Address::ZERO,
            7 => row.topic0 = None,
            8 => row.topic1 = Some(B256::ZERO),
            9 => row.topic2 = Some(B256::ZERO),
            10 => row.topic3 = Some(B256::ZERO),
            11 => row.data = Bytes::new(),
            12 => row.data_len += 1,
            _ => row.source = logex_types::Source::Trace,
        }
        assert!(
            VerifiedBlockLogs::verify_batch(std::slice::from_ref(&proof), &changed).is_err(),
            "field {field}"
        );
    }
    for changed in [
        vec![rows[0].clone()],
        vec![rows[0].clone(), rows[0].clone()],
        vec![rows[1].clone(), rows[0].clone()],
    ] {
        assert!(VerifiedBlockLogs::verify_batch(std::slice::from_ref(&proof), &changed).is_err());
    }
}

#[test]
fn checks_empty_blocks_gaps_parents_and_duplicate_block_proofs() {
    let first = Header {
        number: 5,
        ..Default::default()
    };
    let second = Header {
        number: 6,
        parent_hash: first.hash_slow(),
        ..Default::default()
    };
    let third = Header {
        number: 7,
        parent_hash: second.hash_slow(),
        ..Default::default()
    };
    let proofs: Vec<_> = [&first, &second, &third]
        .into_iter()
        .map(|header| VerifiedBlockLogs::from_empty_header(header).unwrap())
        .collect();
    assert!(VerifiedBlockLogs::verify_batch(&proofs, &[]).is_ok());
    assert!(
        VerifiedBlockLogs::verify_batch(
            &[proofs[2].clone(), proofs[1].clone(), proofs[0].clone()],
            &[]
        )
        .is_ok()
    );
    assert!(VerifiedBlockLogs::verify_batch(&[proofs[0].clone(), proofs[2].clone()], &[]).is_err());
    assert!(VerifiedBlockLogs::verify_batch(&[proofs[0].clone(), proofs[0].clone()], &[]).is_err());
    let mut wrong = second;
    wrong.parent_hash = B256::ZERO;
    assert!(
        VerifiedBlockLogs::verify_batch(
            &[
                proofs[0].clone(),
                VerifiedBlockLogs::from_empty_header(&wrong).unwrap()
            ],
            &[]
        )
        .is_err()
    );
    assert!(VerifiedBlockLogs::verify_batch(&[], &[]).is_err());
}

#[test]
fn empty_bloom_does_not_prove_an_empty_receipt_set() {
    let (header, rows, _) = block(5, B256::ZERO, 0);
    assert!(rows.is_empty());
    assert_eq!(header.logs_bloom, Bloom::ZERO);
    assert!(VerifiedBlockLogs::from_empty_header(&header).is_err());
    let mut rows = Vec::new();
    assert!(
        VerifiedBlockLogs::verify_and_append::<TxEnvelope, ReceiptEnvelope>(
            &header,
            &[],
            &[],
            &mut rows
        )
        .is_err()
    );
}

#[test]
fn invalid_payload_does_not_modify_existing_rows() {
    let (mut header, mut rows, _) = block(8, B256::ZERO, 1);
    let original = rows.clone();
    header.receipts_root = B256::ZERO;
    assert!(
        VerifiedBlockLogs::verify_and_append::<TxEnvelope, ReceiptEnvelope>(
            &header,
            &[],
            &[],
            &mut rows
        )
        .is_err()
    );
    assert_eq!(rows, original);
}

#[test]
fn event_metadata_uses_authenticated_bytes_not_a_cached_transaction_hash() {
    let transaction = TxLegacy::default();
    let tx = TxEnvelope::Legacy(alloy_consensus::Signed::new_unchecked(
        transaction,
        Signature::test_signature(),
        B256::ZERO,
    ));
    let receipt = ReceiptEnvelope::Legacy(
        Receipt {
            status: Eip658Value::Eip658(true),
            cumulative_gas_used: 21_000,
            logs: vec![Log::new(Address::ZERO, vec![], Bytes::new()).unwrap()],
        }
        .with_bloom(),
    );
    let header = Header {
        gas_used: 21_000,
        transactions_root: proofs::calculate_transaction_root(std::slice::from_ref(&tx)),
        receipts_root: proofs::calculate_receipt_root(std::slice::from_ref(&receipt)),
        logs_bloom: receipt.bloom(),
        ..Default::default()
    };
    let expected = alloy_primitives::keccak256(tx.encoded_2718());
    assert_ne!(expected, B256::ZERO);
    let mut rows = Vec::new();
    VerifiedBlockLogs::verify_and_append(&header, &[tx], &[receipt], &mut rows).unwrap();
    assert_eq!(rows[0].tx_hash, expected);
}
