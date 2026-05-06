use alloy_consensus::{
    Eip658Value, Eip2718DecodableReceipt, Eip2718EncodableReceipt, ReceiptWithBloom,
    RlpDecodableReceipt, RlpEncodableReceipt, TxReceipt, TxType,
};
use alloy_eips::Typed2718;
use alloy_eips::eip2718::Eip2718Result;
use alloy_primitives::{Bloom, Log};
use alloy_rlp::{BufMut, Decodable, Encodable, Header};
use reth_eth_wire::BasicNetworkPrimitives;
use reth_ethereum_primitives::{Block, BlockBody, PooledTransactionVariant, TransactionSigned};
use reth_primitives_traits::{InMemorySize, NodePrimitives};

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
        let header = Header::decode(buf)?;
        if !header.list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }

        let remaining = buf.len();
        let tx_type = TxType::try_from(u8::decode(buf)?)
            .map_err(|_| alloy_rlp::Error::Custom("invalid receipt tx type"))?;
        let status = Decodable::decode(buf)?;
        let cumulative_gas_used = Decodable::decode(buf)?;
        let logs = Decodable::decode(buf)?;

        if buf.len() + header.payload_length != remaining {
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
        let header = Header::decode(buf)?;
        if !header.list {
            return Err(alloy_rlp::Error::UnexpectedString);
        }

        let remaining = buf.len();
        let status = Decodable::decode(buf)?;
        let cumulative_gas_used = Decodable::decode(buf)?;
        let logs_bloom = Decodable::decode(buf)?;
        let logs = Decodable::decode(buf)?;

        if buf.len() + header.payload_length != remaining {
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

        *buf = *header_buf;
        let remaining = buf.len();
        let tx_type = TxType::decode(buf)?;
        let this = Self::rlp_decode_inner_with_bloom(buf, tx_type)?;

        if buf.len() + header.payload_length != remaining {
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
        Ok(Self::rlp_decode_inner_with_bloom(
            buf,
            TxType::try_from(ty)?,
        )?)
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
