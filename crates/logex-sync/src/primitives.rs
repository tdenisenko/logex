use alloy_consensus::{
    Eip658Value, Eip2718DecodableReceipt, Eip2718EncodableReceipt, ReceiptWithBloom,
    RlpDecodableReceipt, RlpEncodableReceipt, TxReceipt, TxType,
};
use alloy_eips::Typed2718;
use alloy_eips::eip2718::{Eip2718Error, Eip2718Result};
use alloy_primitives::{Address, B256, Bloom, Log};
use alloy_rlp::{BufMut, Decodable, Encodable, Header};
use reth_eth_wire::BasicNetworkPrimitives;
use reth_ethereum_primitives::{Block, BlockBody, PooledTransactionVariant, TransactionSigned};
use reth_primitives_traits::{InMemorySize, NodePrimitives};
use rustc_hash::FxHashMap;

const RECEIPT_BLOOM_ADDRESS_CACHE_LIMIT: usize = 4_096;
const RECEIPT_BLOOM_TOPIC_CACHE_LIMIT: usize = 8_192;

/// LogEx's internal primitive set keeps Ethereum block/transaction types but
/// uses a receipt representation that can decode both pre- and post-Byzantium
/// receipts from the wire.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogexPrimitives;

impl NodePrimitives for LogexPrimitives {
    type Block = Block;
    type BlockHeader = alloy_consensus::Header;
    type BlockBody = BlockBody;
    type SignedTx = TransactionSigned;
    type Receipt = LogexReceipt;
}

/// Network primitives used by LogEx's devp2p stack.
pub type LogexNetworkPrimitives = BasicNetworkPrimitives<LogexPrimitives, PooledTransactionVariant>;

/// Receipt type used on the devp2p wire.
///
/// This preserves the transaction type and the historical
/// `status_or_post_state` field so old mainnet receipts can be decoded
/// correctly, while still letting higher layers treat receipts generically via
/// `TxReceipt`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogexReceipt {
    pub tx_type: TxType,
    pub status: Eip658Value,
    pub cumulative_gas_used: u64,
    pub logs: Vec<Log>,
}

#[derive(Debug)]
pub(crate) struct ReceiptBloomCache {
    address_blooms: FxHashMap<Address, Bloom>,
    topic_blooms: FxHashMap<B256, Bloom>,
}

impl Default for ReceiptBloomCache {
    fn default() -> Self {
        Self {
            address_blooms: FxHashMap::with_capacity_and_hasher(256, Default::default()),
            topic_blooms: FxHashMap::with_capacity_and_hasher(1_024, Default::default()),
        }
    }
}

impl ReceiptBloomCache {
    pub(crate) fn receipt_bloom(&mut self, receipt: &LogexReceipt) -> Bloom {
        let mut bloom = Bloom::ZERO;
        for log in &receipt.logs {
            let address_bloom = self.address_bloom(log.address);
            bloom.accrue_bloom(&address_bloom);
            for topic in log.topics() {
                let topic_bloom = self.topic_bloom(*topic);
                bloom.accrue_bloom(&topic_bloom);
            }
        }
        bloom
    }

    fn address_bloom(&mut self, address: Address) -> Bloom {
        if let Some(bloom) = self.address_blooms.get(&address) {
            return *bloom;
        }

        let bloom = bloom_for_bytes(address.as_slice());
        if self.address_blooms.len() < RECEIPT_BLOOM_ADDRESS_CACHE_LIMIT {
            self.address_blooms.insert(address, bloom);
        }
        bloom
    }

    fn topic_bloom(&mut self, topic: B256) -> Bloom {
        if let Some(bloom) = self.topic_blooms.get(&topic) {
            return *bloom;
        }

        let bloom = bloom_for_bytes(topic.as_slice());
        if self.topic_blooms.len() < RECEIPT_BLOOM_TOPIC_CACHE_LIMIT {
            self.topic_blooms.insert(topic, bloom);
        }
        bloom
    }

    #[cfg(test)]
    fn cached_entries(&self) -> (usize, usize) {
        (self.address_blooms.len(), self.topic_blooms.len())
    }
}

pub(crate) fn logex_receipt_batches_with_cached_blooms(
    receipts: Vec<Vec<LogexReceipt>>,
    cache: &mut ReceiptBloomCache,
) -> Vec<Vec<ReceiptWithBloom<LogexReceipt>>> {
    receipts
        .into_iter()
        .map(|block_receipts| {
            block_receipts
                .into_iter()
                .map(|receipt| {
                    let logs_bloom = cache.receipt_bloom(&receipt);
                    ReceiptWithBloom {
                        receipt,
                        logs_bloom,
                    }
                })
                .collect()
        })
        .collect()
}

fn bloom_for_bytes(bytes: &[u8]) -> Bloom {
    let mut bloom = Bloom::ZERO;
    bloom.m3_2048(bytes);
    bloom
}

fn decode_receipt_status(buf: &mut &[u8]) -> alloy_rlp::Result<Eip658Value> {
    // Eip658Value's general-purpose decoder coerces other one-byte values to
    // true. The wire must preserve canonical status/root bytes before hashing.
    match Header::decode_bytes(buf, false)? {
        [] => Ok(Eip658Value::Eip658(false)),
        [1] => Ok(Eip658Value::Eip658(true)),
        bytes if bytes.len() == 32 => Ok(Eip658Value::PostState(B256::from_slice(bytes))),
        _ => Err(alloy_rlp::Error::Custom(
            "invalid receipt status or post-state",
        )),
    }
}

fn decode_receipt_logs(buf: &mut &[u8]) -> alloy_rlp::Result<Vec<Log>> {
    let mut payload = Header::decode_bytes(buf, true)?;
    let mut logs = Vec::new();
    while !payload.is_empty() {
        // Bound every nested structure before decoding fields. The generic
        // Log decoder does not enforce list headers or the four-topic limit.
        let mut fields = Header::decode_bytes(&mut payload, true)?;
        let address = Address::decode(&mut fields)?;
        let mut topic_bytes = Header::decode_bytes(&mut fields, true)?;
        let mut topics = Vec::new();
        while !topic_bytes.is_empty() {
            if topics.len() == 4 {
                return Err(alloy_rlp::Error::Custom(
                    "receipt log has more than four topics",
                ));
            }
            topics.push(B256::decode(&mut topic_bytes)?);
        }
        let data = Decodable::decode(&mut fields)?;
        if !fields.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }
        // Cardinality was checked before growing the topic vector.
        logs.push(Log::new_unchecked(address, topics, data));
    }
    Ok(logs)
}

impl LogexReceipt {
    fn network_encoded_fields_length(&self) -> usize {
        self.tx_type.ty().length()
            + self.status.length()
            + self.cumulative_gas_used.length()
            + self.logs.length()
    }

    fn network_encode_fields(&self, out: &mut dyn BufMut) {
        self.tx_type.ty().encode(out);
        self.status.encode(out);
        self.cumulative_gas_used.encode(out);
        self.logs.encode(out);
    }

    fn network_header(&self) -> Header {
        Header {
            list: true,
            payload_length: self.network_encoded_fields_length(),
        }
    }

    fn network_decode_inner(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        let mut fields = Header::decode_bytes(buf, true)?;
        let tx_type = TxType::try_from(u8::decode(&mut fields)?)
            .map_err(|_| alloy_rlp::Error::Custom("invalid receipt tx type"))?;
        let status = decode_receipt_status(&mut fields)?;
        let cumulative_gas_used = Decodable::decode(&mut fields)?;
        let logs = decode_receipt_logs(&mut fields)?;

        if !fields.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }

        Ok(Self {
            tx_type,
            status,
            cumulative_gas_used,
            logs,
        })
    }

    fn rlp_encoded_fields_length_with_bloom(&self, bloom: &Bloom) -> usize {
        self.status.length()
            + self.cumulative_gas_used.length()
            + bloom.length()
            + self.logs.length()
    }

    fn rlp_encode_fields_with_bloom(&self, bloom: &Bloom, out: &mut dyn BufMut) {
        self.status.encode(out);
        self.cumulative_gas_used.encode(out);
        bloom.encode(out);
        self.logs.encode(out);
    }

    fn rlp_header_with_bloom(&self, bloom: &Bloom) -> Header {
        Header {
            list: true,
            payload_length: self.rlp_encoded_fields_length_with_bloom(bloom),
        }
    }

    fn rlp_decode_inner_with_bloom(
        buf: &mut &[u8],
        tx_type: TxType,
    ) -> alloy_rlp::Result<ReceiptWithBloom<Self>> {
        let mut fields = Header::decode_bytes(buf, true)?;
        let status = decode_receipt_status(&mut fields)?;
        let cumulative_gas_used = Decodable::decode(&mut fields)?;
        let logs_bloom = Decodable::decode(&mut fields)?;
        let logs = decode_receipt_logs(&mut fields)?;

        if !fields.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }

        Ok(ReceiptWithBloom {
            receipt: Self {
                tx_type,
                status,
                cumulative_gas_used,
                logs,
            },
            logs_bloom,
        })
    }
}

impl TxReceipt for LogexReceipt {
    type Log = Log;

    fn status_or_post_state(&self) -> Eip658Value {
        self.status
    }

    fn status(&self) -> bool {
        self.status.coerce_status()
    }

    fn bloom(&self) -> Bloom {
        alloy_primitives::logs_bloom(self.logs.iter())
    }

    fn cumulative_gas_used(&self) -> u64 {
        self.cumulative_gas_used
    }

    fn logs(&self) -> &[Self::Log] {
        &self.logs
    }

    fn into_logs(self) -> Vec<Self::Log> {
        self.logs
    }
}

impl Typed2718 for LogexReceipt {
    fn ty(&self) -> u8 {
        self.tx_type.ty()
    }
}

impl InMemorySize for LogexReceipt {
    fn size(&self) -> usize {
        self.tx_type.size()
            + self.status.length()
            + self.cumulative_gas_used.size()
            + self.logs.size()
    }
}

impl Encodable for LogexReceipt {
    fn encode(&self, out: &mut dyn BufMut) {
        self.network_header().encode(out);
        self.network_encode_fields(out);
    }

    fn length(&self) -> usize {
        self.network_header().length_with_payload()
    }
}

impl Decodable for LogexReceipt {
    fn decode(buf: &mut &[u8]) -> alloy_rlp::Result<Self> {
        Self::network_decode_inner(buf)
    }
}

impl RlpEncodableReceipt for LogexReceipt {
    fn rlp_encoded_length_with_bloom(&self, bloom: &Bloom) -> usize {
        let inner = self.rlp_header_with_bloom(bloom).length_with_payload();
        if self.tx_type.is_legacy() {
            return inner;
        }

        Header {
            list: false,
            payload_length: self.eip2718_encoded_length_with_bloom(bloom),
        }
        .length()
            + self.eip2718_encoded_length_with_bloom(bloom)
    }

    fn rlp_encode_with_bloom(&self, bloom: &Bloom, out: &mut dyn BufMut) {
        if !self.tx_type.is_legacy() {
            Header {
                list: false,
                payload_length: self.eip2718_encoded_length_with_bloom(bloom),
            }
            .encode(out);
        }
        self.eip2718_encode_with_bloom(bloom, out);
    }
}

impl RlpDecodableReceipt for LogexReceipt {
    fn rlp_decode_with_bloom(buf: &mut &[u8]) -> alloy_rlp::Result<ReceiptWithBloom<Self>> {
        let header_buf = &mut &**buf;
        let header = Header::decode(header_buf)?;

        if header.list {
            return Self::rlp_decode_inner_with_bloom(buf, TxType::Legacy);
        }

        let payload = Header::decode_bytes(buf, false)?;
        let (&ty, mut fields) = payload
            .split_first()
            .ok_or(alloy_rlp::Error::InputTooShort)?;
        let this = Self::typed_decode_with_bloom(ty, &mut fields)?;

        if !fields.is_empty() {
            return Err(alloy_rlp::Error::UnexpectedLength);
        }

        Ok(this)
    }
}

impl Eip2718EncodableReceipt for LogexReceipt {
    fn eip2718_encoded_length_with_bloom(&self, bloom: &Bloom) -> usize {
        !self.tx_type.is_legacy() as usize + self.rlp_header_with_bloom(bloom).length_with_payload()
    }

    fn eip2718_encode_with_bloom(&self, bloom: &Bloom, out: &mut dyn BufMut) {
        if !self.tx_type.is_legacy() {
            out.put_u8(self.tx_type.ty());
        }
        self.rlp_header_with_bloom(bloom).encode(out);
        self.rlp_encode_fields_with_bloom(bloom, out);
    }
}

impl Eip2718DecodableReceipt for LogexReceipt {
    fn typed_decode_with_bloom(ty: u8, buf: &mut &[u8]) -> Eip2718Result<ReceiptWithBloom<Self>> {
        let tx_type = TxType::try_from(ty)?;
        if tx_type.is_legacy() {
            return Err(Eip2718Error::UnexpectedType(ty));
        }
        Ok(Self::rlp_decode_inner_with_bloom(buf, tx_type)?)
    }

    fn fallback_decode_with_bloom(buf: &mut &[u8]) -> Eip2718Result<ReceiptWithBloom<Self>> {
        Ok(Self::rlp_decode_inner_with_bloom(buf, TxType::Legacy)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{Address, B256, Bytes, LogData};

    fn test_log() -> Log {
        Log {
            address: Address::repeat_byte(0x11),
            data: LogData::new_unchecked(vec![B256::repeat_byte(0x22)], Bytes::from_static(b"log")),
        }
    }

    #[test]
    fn receipt_decode_rejects_noncanonical_status_values() {
        let mut statuses = vec![
            vec![0x00],
            vec![0x02],
            vec![0x7f],
            vec![0xc0],
            vec![0xc1, 0x01],
        ];
        statuses.push([vec![0xe0], vec![0x11; 32]].concat()); // list masquerading as post-state
        statuses.push(vec![0x81, 0x01]); // noncanonical single byte
        statuses.push(vec![0x82, 0x00, 0x01]); // padded status
        for status in statuses {
            for with_bloom in [false, true] {
                let mut fields = Vec::new();
                if !with_bloom {
                    0u8.encode(&mut fields);
                }
                fields.extend_from_slice(&status);
                1u64.encode(&mut fields);
                if with_bloom {
                    Bloom::ZERO.encode(&mut fields);
                }
                fields.push(0xc0); // logs
                let mut encoded = Vec::new();
                Header {
                    list: true,
                    payload_length: fields.len(),
                }
                .encode(&mut encoded);
                encoded.extend_from_slice(&fields);
                let rejected = if with_bloom {
                    LogexReceipt::rlp_decode_with_bloom(&mut encoded.as_slice()).is_err()
                } else {
                    LogexReceipt::decode(&mut encoded.as_slice()).is_err()
                };
                assert!(rejected, "status={status:02x?}, with_bloom={with_bloom}");
            }
        }
    }

    #[test]
    fn receipt_decode_rejects_string_wrapped_logs() {
        let log = test_log();
        let encoded_log = alloy_rlp::encode(&log);
        let mut fields = encoded_log.as_slice();
        let header = Header::decode(&mut fields).unwrap();
        let mut malformed = Vec::new();
        Header {
            list: false,
            ..header
        }
        .encode(&mut malformed);
        malformed.extend_from_slice(fields);
        let encoded = network_receipt_with_encoded_log(&malformed);
        assert!(LogexReceipt::decode(&mut encoded.as_slice()).is_err());
    }

    #[test]
    fn receipt_decode_rejects_more_than_four_topics() {
        let mut log = test_log();
        log.data.set_topics_unchecked(vec![B256::ZERO; 5]);
        let encoded = network_receipt_with_encoded_log(&alloy_rlp::encode(&log));
        assert!(LogexReceipt::decode(&mut encoded.as_slice()).is_err());
    }

    #[test]
    fn receipt_decode_does_not_consume_fields_outside_declared_list() {
        let receipt = LogexReceipt {
            logs: vec![test_log()],
            ..Default::default()
        };
        let encoded = alloy_rlp::encode(&receipt);
        let mut fields = encoded.as_slice();
        Header::decode(&mut fields).unwrap();
        let mut malformed = vec![0xc0]; // empty receipt, followed by unrelated bytes
        malformed.extend_from_slice(fields);
        let mut input = malformed.as_slice();
        assert!(LogexReceipt::decode(&mut input).is_err());
        assert_eq!(input, fields);
    }

    #[test]
    fn typed_receipt_decode_rejects_legacy_type_envelope() {
        let receipt = LogexReceipt::default();
        let mut encoded = Vec::new();
        receipt.eip2718_encode_with_bloom(&Bloom::ZERO, &mut encoded);
        assert!(LogexReceipt::typed_decode_with_bloom(0, &mut encoded.as_slice()).is_err());
    }

    fn network_receipt_with_encoded_log(log: &[u8]) -> Vec<u8> {
        let mut fields = vec![0x80, 0x01, 0x01]; // legacy type, success, gas
        Header {
            list: true,
            payload_length: log.len(),
        }
        .encode(&mut fields);
        fields.extend_from_slice(log);
        let mut encoded = Vec::new();
        Header {
            list: true,
            payload_length: fields.len(),
        }
        .encode(&mut encoded);
        encoded.extend_from_slice(&fields);
        encoded
    }

    #[test]
    fn receipt_codecs_match_alloy_for_valid_statuses_types_and_logs() {
        use alloy_consensus::{Receipt, ReceiptEnvelope};
        use alloy_eips::eip2718::{Decodable2718, Encodable2718};

        for tx_type in [
            TxType::Legacy,
            TxType::Eip2930,
            TxType::Eip1559,
            TxType::Eip4844,
            TxType::Eip7702,
        ] {
            let mut statuses = vec![Eip658Value::Eip658(false), Eip658Value::Eip658(true)];
            if tx_type.is_legacy() {
                statuses.push(Eip658Value::PostState(B256::ZERO));
            }
            for status in statuses {
                for topic_count in 0..=4 {
                    for data_len in [0, 1, 55, 56, 256] {
                        let log = Log::new(
                            Address::repeat_byte(0x11),
                            (0..topic_count)
                                .map(|topic| B256::repeat_byte(topic + 1))
                                .collect(),
                            Bytes::from(vec![0xa5; data_len]),
                        )
                        .unwrap();
                        let receipt = LogexReceipt {
                            tx_type,
                            status,
                            cumulative_gas_used: u64::MAX,
                            logs: vec![log],
                        };
                        let logs_bloom = receipt.bloom();
                        let alloy_receipt = ReceiptWithBloom {
                            receipt: Receipt {
                                status,
                                cumulative_gas_used: receipt.cumulative_gas_used,
                                logs: receipt.logs.clone(),
                            },
                            logs_bloom,
                        };
                        let envelope = match tx_type {
                            TxType::Legacy => ReceiptEnvelope::Legacy(alloy_receipt),
                            TxType::Eip2930 => ReceiptEnvelope::Eip2930(alloy_receipt),
                            TxType::Eip1559 => ReceiptEnvelope::Eip1559(alloy_receipt),
                            TxType::Eip4844 => ReceiptEnvelope::Eip4844(alloy_receipt),
                            TxType::Eip7702 => ReceiptEnvelope::Eip7702(alloy_receipt),
                        };
                        let expected = ReceiptWithBloom {
                            receipt: receipt.clone(),
                            logs_bloom,
                        };
                        let canonical = alloy_rlp::encode(&envelope);
                        assert_eq!(canonical, alloy_rlp::encode(&expected));
                        let mut input = canonical.as_slice();
                        assert_eq!(
                            ReceiptWithBloom::<LogexReceipt>::decode(&mut input).unwrap(),
                            expected
                        );
                        assert!(input.is_empty());
                        let encoded_2718 = envelope.encoded_2718();
                        let mut input = encoded_2718.as_slice();
                        assert_eq!(
                            ReceiptWithBloom::<LogexReceipt>::decode_2718(&mut input).unwrap(),
                            expected
                        );
                        assert!(input.is_empty());

                        let encoded = alloy_rlp::encode(&receipt);
                        let mut input = encoded.as_slice();
                        assert_eq!(LogexReceipt::decode(&mut input).unwrap(), receipt);
                        assert!(input.is_empty());
                        for end in 0..encoded.len() {
                            assert!(LogexReceipt::decode(&mut &encoded[..end]).is_err());
                        }
                        for end in 0..canonical.len() {
                            assert!(
                                ReceiptWithBloom::<LogexReceipt>::decode(&mut &canonical[..end])
                                    .is_err()
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn receipt_decode_mutations_never_normalize_accepted_bytes() {
        for tx_type in [TxType::Legacy, TxType::Eip1559] {
            let receipt = LogexReceipt {
                tx_type,
                logs: vec![test_log()],
                ..Default::default()
            };
            for with_bloom in [false, true] {
                let encoded = if with_bloom {
                    alloy_rlp::encode(ReceiptWithBloom {
                        receipt: receipt.clone(),
                        logs_bloom: receipt.bloom(),
                    })
                } else {
                    alloy_rlp::encode(&receipt)
                };
                for index in 0..encoded.len() {
                    for byte in [
                        0x00, 0x01, 0x02, 0x7f, 0x80, 0x81, 0xb8, 0xc0, 0xc1, 0xf8, 0xff,
                    ] {
                        let mut mutated = encoded.clone();
                        mutated[index] = byte;
                        let mut input = mutated.as_slice();
                        let decoded = if with_bloom {
                            LogexReceipt::rlp_decode_with_bloom(&mut input).map(alloy_rlp::encode)
                        } else {
                            LogexReceipt::decode(&mut input).map(alloy_rlp::encode)
                        };
                        if let Ok(canonical) = decoded {
                            assert_eq!(
                                canonical,
                                mutated[..mutated.len() - input.len()],
                                "index={index}, byte={byte}, with_bloom={with_bloom}"
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn receipt_codecs_preserve_siblings_and_reject_extra_fields() {
        let receipt = LogexReceipt {
            tx_type: TxType::Eip1559,
            logs: vec![test_log()],
            ..Default::default()
        };
        for with_bloom in [false, true] {
            let encoded = if with_bloom {
                alloy_rlp::encode(ReceiptWithBloom {
                    receipt: receipt.clone(),
                    logs_bloom: receipt.bloom(),
                })
            } else {
                alloy_rlp::encode(&receipt)
            };
            let mut siblings = encoded.clone();
            siblings.extend_from_slice(&encoded);
            let mut input = siblings.as_slice();
            if with_bloom {
                LogexReceipt::rlp_decode_with_bloom(&mut input).unwrap();
            } else {
                LogexReceipt::decode(&mut input).unwrap();
            }
            assert_eq!(input, encoded);

            let mut fields = encoded.as_slice();
            let mut header = Header::decode(&mut fields).unwrap();
            header.payload_length += 1;
            let mut trailing = Vec::new();
            header.encode(&mut trailing);
            trailing.extend_from_slice(fields);
            trailing.push(0x80);
            if with_bloom {
                assert!(LogexReceipt::rlp_decode_with_bloom(&mut trailing.as_slice()).is_err());
            } else {
                assert!(LogexReceipt::decode(&mut trailing.as_slice()).is_err());
            }
        }
    }

    #[test]
    fn cached_receipt_bloom_matches_alloy_bloom() {
        let receipt = LogexReceipt {
            tx_type: TxType::Eip1559,
            status: Eip658Value::success(),
            cumulative_gas_used: 42_000,
            logs: vec![test_log(), test_log()],
        };
        let mut cache = ReceiptBloomCache::default();

        let cached = cache.receipt_bloom(&receipt);
        let expected = alloy_primitives::logs_bloom(receipt.logs.iter());

        assert_eq!(cached, expected);
        assert_eq!(cache.cached_entries(), (1, 1));
    }

    #[test]
    fn no_bloom_roundtrip_preserves_post_state() {
        let receipt = LogexReceipt {
            tx_type: TxType::Legacy,
            status: Eip658Value::PostState(B256::repeat_byte(0x33)),
            cumulative_gas_used: 21_000,
            logs: vec![test_log()],
        };

        let encoded = alloy_rlp::encode(&receipt);
        let decoded = LogexReceipt::decode(&mut encoded.as_slice()).expect("receipt decodes");

        assert_eq!(decoded, receipt);
        assert!(decoded.status.is_post_state());
    }

    #[test]
    fn no_bloom_decode_accepts_eth69_network_shape() {
        let mut encoded = Vec::new();
        Header {
            list: true,
            payload_length: 0u8.length()
                + Eip658Value::success().length()
                + 21_000u64.length()
                + Vec::<Log>::new().length(),
        }
        .encode(&mut encoded);
        0u8.encode(&mut encoded);
        Eip658Value::success().encode(&mut encoded);
        21_000u64.encode(&mut encoded);
        Vec::<Log>::new().encode(&mut encoded);

        let decoded = LogexReceipt::decode(&mut encoded.as_slice()).expect("receipt decodes");

        assert_eq!(decoded.tx_type, TxType::Legacy);
        assert!(decoded.status());
        assert_eq!(decoded.cumulative_gas_used, 21_000);
        assert!(decoded.logs.is_empty());
    }

    #[test]
    fn typed_no_bloom_roundtrip_preserves_type() {
        let receipt = LogexReceipt {
            tx_type: TxType::Eip1559,
            status: Eip658Value::success(),
            cumulative_gas_used: 42_000,
            logs: vec![test_log()],
        };

        let encoded = alloy_rlp::encode(vec![vec![receipt.clone()]]);
        let decoded = Vec::<Vec<LogexReceipt>>::decode(&mut encoded.as_slice())
            .expect("typed receipts decode");

        assert_eq!(decoded, vec![vec![receipt]]);
    }

    #[test]
    fn with_bloom_roundtrip_preserves_post_state() {
        let receipt = ReceiptWithBloom {
            receipt: LogexReceipt {
                tx_type: TxType::Legacy,
                status: Eip658Value::PostState(B256::repeat_byte(0x44)),
                cumulative_gas_used: 55_000,
                logs: vec![test_log()],
            },
            logs_bloom: Bloom::repeat_byte(0x55),
        };

        let encoded = alloy_rlp::encode(&receipt);
        let decoded =
            ReceiptWithBloom::<LogexReceipt>::decode(&mut encoded.as_slice()).expect("decodes");

        assert_eq!(decoded, receipt);
        assert!(decoded.receipt.status.is_post_state());
    }
}
