use std::io;

use alloy_primitives::B256;
use async_trait::async_trait;
use futures::prelude::*;
use libp2p::request_response::{self, Codec, ProtocolSupport};
use snap::read::FrameDecoder;
use snap::write::FrameEncoder;

pub(crate) const STATUS_V1_PROTOCOL_ID: &str = "/eth2/beacon_chain/req/status/1/ssz_snappy";
pub(crate) const STATUS_V2_PROTOCOL_ID: &str = "/eth2/beacon_chain/req/status/2/ssz_snappy";
pub(crate) const GOODBYE_V1_PROTOCOL_ID: &str = "/eth2/beacon_chain/req/goodbye/1/ssz_snappy";
pub(crate) const METADATA_V1_PROTOCOL_ID: &str = "/eth2/beacon_chain/req/metadata/1/ssz_snappy";
pub(crate) const METADATA_V2_PROTOCOL_ID: &str = "/eth2/beacon_chain/req/metadata/2/ssz_snappy";
pub(crate) const METADATA_V3_PROTOCOL_ID: &str = "/eth2/beacon_chain/req/metadata/3/ssz_snappy";
pub(crate) const PING_PROTOCOL_ID: &str = "/eth2/beacon_chain/req/ping/1/ssz_snappy";
pub(crate) const LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID: &str =
    "/eth2/beacon_chain/req/light_client_bootstrap/1/ssz_snappy";
pub(crate) const LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID: &str =
    "/eth2/beacon_chain/req/light_client_finality_update/1/ssz_snappy";
pub(crate) const LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID: &str =
    "/eth2/beacon_chain/req/light_client_optimistic_update/1/ssz_snappy";

const SUCCESS_CODE: u8 = 0;
const RESOURCE_UNAVAILABLE_CODE: u8 = 3;
const ERROR_MESSAGE_LIMIT: usize = 256;

pub type Eth2RpcBehaviour = request_response::Behaviour<Eth2RpcCodec>;
pub type Eth2RpcEvent = request_response::Event<Eth2RpcRequest, Eth2RpcResponse>;
pub type Eth2OutboundRequestId = request_response::OutboundRequestId;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Eth2RpcProtocol {
    StatusV2,
    StatusV1,
    GoodbyeV1,
    MetadataV3,
    MetadataV2,
    MetadataV1,
    PingV1,
    LightClientBootstrapV1,
    LightClientFinalityUpdateV1,
    LightClientOptimisticUpdateV1,
}

impl AsRef<str> for Eth2RpcProtocol {
    fn as_ref(&self) -> &str {
        match self {
            Self::StatusV2 => STATUS_V2_PROTOCOL_ID,
            Self::StatusV1 => STATUS_V1_PROTOCOL_ID,
            Self::GoodbyeV1 => GOODBYE_V1_PROTOCOL_ID,
            Self::MetadataV3 => METADATA_V3_PROTOCOL_ID,
            Self::MetadataV2 => METADATA_V2_PROTOCOL_ID,
            Self::MetadataV1 => METADATA_V1_PROTOCOL_ID,
            Self::PingV1 => PING_PROTOCOL_ID,
            Self::LightClientBootstrapV1 => LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID,
            Self::LightClientFinalityUpdateV1 => LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID,
            Self::LightClientOptimisticUpdateV1 => LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Eth2RpcRequest {
    Status(StatusMessage),
    Goodbye(u64),
    MetaData,
    Ping(u64),
    LightClientBootstrap(B256),
    LightClientFinalityUpdate,
    LightClientOptimisticUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Eth2RpcResponse {
    Status(StatusMessage),
    Goodbye(u64),
    MetaData(MetaData),
    Ping(u64),
    LightClientBootstrap(RawRpcResponse),
    LightClientFinalityUpdate(RawRpcResponse),
    LightClientOptimisticUpdate(RawRpcResponse),
    Error(Eth2RpcErrorResponse),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatusMessage {
    pub fork_digest: [u8; 4],
    pub finalized_root: B256,
    pub finalized_epoch: u64,
    pub head_root: B256,
    pub head_slot: u64,
    pub earliest_available_slot: u64,
}

impl StatusMessage {
    pub fn genesis(fork_digest: [u8; 4], genesis_block_root: B256) -> Self {
        Self {
            fork_digest,
            finalized_root: B256::ZERO,
            finalized_epoch: 0,
            head_root: genesis_block_root,
            head_slot: 0,
            earliest_available_slot: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetaData {
    pub seq_number: u64,
    pub attnets: [u8; 8],
    pub syncnets: [u8; 1],
    pub custody_group_count: u64,
}

impl MetaData {
    pub const fn empty() -> Self {
        Self {
            seq_number: 0,
            attnets: [0u8; 8],
            syncnets: [0u8; 1],
            custody_group_count: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRpcResponse {
    pub context_bytes: Option<[u8; 4]>,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Eth2RpcErrorResponse {
    pub code: u8,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct Eth2RpcCodec;

fn build_rpc_behaviour(
    protocols: impl IntoIterator<Item = (Eth2RpcProtocol, ProtocolSupport)>,
) -> Eth2RpcBehaviour {
    let config = request_response::Config::default()
        .with_request_timeout(std::time::Duration::from_secs(15))
        .with_max_concurrent_streams(64);

    Eth2RpcBehaviour::with_codec(Eth2RpcCodec, protocols, config)
}

pub fn build_status_behaviour() -> Eth2RpcBehaviour {
    build_rpc_behaviour([
        (Eth2RpcProtocol::StatusV2, ProtocolSupport::Full),
        (Eth2RpcProtocol::StatusV1, ProtocolSupport::Full),
    ])
}

pub fn build_goodbye_behaviour() -> Eth2RpcBehaviour {
    build_rpc_behaviour([(Eth2RpcProtocol::GoodbyeV1, ProtocolSupport::Full)])
}

pub fn build_metadata_behaviour() -> Eth2RpcBehaviour {
    build_rpc_behaviour([
        (Eth2RpcProtocol::MetadataV3, ProtocolSupport::Full),
        (Eth2RpcProtocol::MetadataV2, ProtocolSupport::Full),
        (Eth2RpcProtocol::MetadataV1, ProtocolSupport::Full),
    ])
}

pub fn build_ping_behaviour() -> Eth2RpcBehaviour {
    build_rpc_behaviour([(Eth2RpcProtocol::PingV1, ProtocolSupport::Full)])
}

pub fn build_light_client_bootstrap_behaviour() -> Eth2RpcBehaviour {
    build_rpc_behaviour([(
        Eth2RpcProtocol::LightClientBootstrapV1,
        ProtocolSupport::Full,
    )])
}

pub fn build_light_client_finality_update_behaviour() -> Eth2RpcBehaviour {
    build_rpc_behaviour([(
        Eth2RpcProtocol::LightClientFinalityUpdateV1,
        ProtocolSupport::Full,
    )])
}

pub fn build_light_client_optimistic_update_behaviour() -> Eth2RpcBehaviour {
    build_rpc_behaviour([(
        Eth2RpcProtocol::LightClientOptimisticUpdateV1,
        ProtocolSupport::Full,
    )])
}

#[async_trait]
impl Codec for Eth2RpcCodec {
    type Protocol = Eth2RpcProtocol;
    type Request = Eth2RpcRequest;
    type Response = Eth2RpcResponse;

    async fn read_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let payload = read_request_payload(io).await?;
        decode_request(protocol, &payload)
    }

    async fn read_response<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut bytes = Vec::new();
        io.read_to_end(&mut bytes).await?;
        decode_response(protocol, &bytes)
    }

    async fn write_request<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let payload = encode_request(protocol, req)?;
        io.write_all(&payload).await?;
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let payload = encode_response(protocol, res)?;
        io.write_all(&payload).await?;
        io.close().await
    }
}

fn encode_request(protocol: &Eth2RpcProtocol, request: Eth2RpcRequest) -> io::Result<Vec<u8>> {
    let request_label = format!("{request:?}");
    match (protocol, request) {
        (Eth2RpcProtocol::StatusV1 | Eth2RpcProtocol::StatusV2, Eth2RpcRequest::Status(status)) => {
            encode_ssz_snappy_payload(&encode_status(protocol, status))
        }
        (Eth2RpcProtocol::GoodbyeV1, Eth2RpcRequest::Goodbye(reason)) => {
            encode_ssz_snappy_payload(&encode_u64(reason))
        }
        (
            Eth2RpcProtocol::MetadataV1 | Eth2RpcProtocol::MetadataV2 | Eth2RpcProtocol::MetadataV3,
            Eth2RpcRequest::MetaData,
        ) => Ok(Vec::new()),
        (Eth2RpcProtocol::PingV1, Eth2RpcRequest::Ping(seq_number)) => {
            encode_ssz_snappy_payload(&encode_u64(seq_number))
        }
        (Eth2RpcProtocol::LightClientBootstrapV1, Eth2RpcRequest::LightClientBootstrap(root)) => {
            encode_ssz_snappy_payload(root.as_slice())
        }
        (
            Eth2RpcProtocol::LightClientFinalityUpdateV1,
            Eth2RpcRequest::LightClientFinalityUpdate,
        )
        | (
            Eth2RpcProtocol::LightClientOptimisticUpdateV1,
            Eth2RpcRequest::LightClientOptimisticUpdate,
        ) => Ok(Vec::new()),
        _ => Err(invalid_data(format!(
            "request {request_label} is not valid for protocol {}",
            protocol.as_ref()
        ))),
    }
}

fn encode_response(protocol: &Eth2RpcProtocol, response: Eth2RpcResponse) -> io::Result<Vec<u8>> {
    match response {
        Eth2RpcResponse::Status(status) => {
            encode_single_success_response(&encode_status(protocol, status))
        }
        Eth2RpcResponse::Goodbye(reason) => encode_single_success_response(&encode_u64(reason)),
        Eth2RpcResponse::MetaData(metadata) => {
            encode_single_success_response(&encode_metadata(protocol, metadata))
        }
        Eth2RpcResponse::Ping(seq_number) => {
            encode_single_success_response(&encode_u64(seq_number))
        }
        Eth2RpcResponse::LightClientBootstrap(raw)
        | Eth2RpcResponse::LightClientFinalityUpdate(raw)
        | Eth2RpcResponse::LightClientOptimisticUpdate(raw) => {
            encode_single_success_response_with_context(raw.context_bytes, &raw.bytes)
        }
        Eth2RpcResponse::Error(error) => encode_single_error_response(error),
    }
}

fn decode_request(protocol: &Eth2RpcProtocol, payload: &[u8]) -> io::Result<Eth2RpcRequest> {
    match protocol {
        Eth2RpcProtocol::StatusV1 | Eth2RpcProtocol::StatusV2 => {
            decode_status(protocol, payload).map(Eth2RpcRequest::Status)
        }
        Eth2RpcProtocol::GoodbyeV1 => decode_u64(payload).map(Eth2RpcRequest::Goodbye),
        Eth2RpcProtocol::MetadataV1 | Eth2RpcProtocol::MetadataV2 | Eth2RpcProtocol::MetadataV3 => {
            if payload.is_empty() {
                Ok(Eth2RpcRequest::MetaData)
            } else {
                Err(invalid_data("metadata request must be empty"))
            }
        }
        Eth2RpcProtocol::PingV1 => decode_u64(payload).map(Eth2RpcRequest::Ping),
        Eth2RpcProtocol::LightClientBootstrapV1 => {
            decode_root(payload).map(Eth2RpcRequest::LightClientBootstrap)
        }
        Eth2RpcProtocol::LightClientFinalityUpdateV1 => {
            if payload.is_empty() {
                Ok(Eth2RpcRequest::LightClientFinalityUpdate)
            } else {
                Err(invalid_data(
                    "light client finality update request must be empty",
                ))
            }
        }
        Eth2RpcProtocol::LightClientOptimisticUpdateV1 => {
            if payload.is_empty() {
                Ok(Eth2RpcRequest::LightClientOptimisticUpdate)
            } else {
                Err(invalid_data(
                    "light client optimistic update request must be empty",
                ))
            }
        }
    }
}

fn decode_response(protocol: &Eth2RpcProtocol, bytes: &[u8]) -> io::Result<Eth2RpcResponse> {
    if bytes.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "empty RPC response",
        ));
    }

    let result_code = bytes[0];
    if result_code != SUCCESS_CODE {
        let payload = decode_ssz_snappy_payload(&bytes[1..])?;
        if payload.len() > ERROR_MESSAGE_LIMIT {
            return Err(invalid_data("error payload exceeds ErrorMessage limit"));
        }
        return Ok(Eth2RpcResponse::Error(Eth2RpcErrorResponse {
            code: result_code,
            message: payload,
        }));
    }

    let context_len = success_response_context_len(protocol);
    if bytes.len() < 1 + context_len {
        return Err(invalid_data(format!(
            "response for {} missing {} context bytes",
            protocol.as_ref(),
            context_len
        )));
    }
    let context_bytes = if context_len == 4 {
        let mut context = [0u8; 4];
        context.copy_from_slice(&bytes[1..5]);
        Some(context)
    } else {
        None
    };
    let payload = decode_ssz_snappy_payload(&bytes[(1 + context_len)..])?;

    match protocol {
        Eth2RpcProtocol::StatusV1 | Eth2RpcProtocol::StatusV2 => {
            decode_status(protocol, &payload).map(Eth2RpcResponse::Status)
        }
        Eth2RpcProtocol::GoodbyeV1 => decode_u64(&payload).map(Eth2RpcResponse::Goodbye),
        Eth2RpcProtocol::MetadataV1 | Eth2RpcProtocol::MetadataV2 | Eth2RpcProtocol::MetadataV3 => {
            decode_metadata(protocol, &payload).map(Eth2RpcResponse::MetaData)
        }
        Eth2RpcProtocol::PingV1 => decode_u64(&payload).map(Eth2RpcResponse::Ping),
        Eth2RpcProtocol::LightClientBootstrapV1 => {
            Ok(Eth2RpcResponse::LightClientBootstrap(RawRpcResponse {
                context_bytes,
                bytes: payload,
            }))
        }
        Eth2RpcProtocol::LightClientFinalityUpdateV1 => {
            Ok(Eth2RpcResponse::LightClientFinalityUpdate(RawRpcResponse {
                context_bytes,
                bytes: payload,
            }))
        }
        Eth2RpcProtocol::LightClientOptimisticUpdateV1 => Ok(
            Eth2RpcResponse::LightClientOptimisticUpdate(RawRpcResponse {
                context_bytes,
                bytes: payload,
            }),
        ),
    }
}

async fn read_request_payload<T>(io: &mut T) -> io::Result<Vec<u8>>
where
    T: AsyncRead + Unpin + Send,
{
    let mut bytes = Vec::new();
    io.read_to_end(&mut bytes).await?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    decode_ssz_snappy_payload(&bytes)
}

fn encode_single_success_response(raw_ssz: &[u8]) -> io::Result<Vec<u8>> {
    encode_single_success_response_with_context(None, raw_ssz)
}

fn encode_single_success_response_with_context(
    context_bytes: Option<[u8; 4]>,
    raw_ssz: &[u8],
) -> io::Result<Vec<u8>> {
    let mut response = Vec::with_capacity(1 + raw_ssz.len());
    response.push(SUCCESS_CODE);
    if let Some(context_bytes) = context_bytes {
        response.extend_from_slice(&context_bytes);
    }
    response.extend(encode_ssz_snappy_payload(raw_ssz)?);
    Ok(response)
}

fn encode_single_error_response(error: Eth2RpcErrorResponse) -> io::Result<Vec<u8>> {
    if error.message.len() > ERROR_MESSAGE_LIMIT {
        return Err(invalid_data("error message exceeds 256 bytes"));
    }
    let mut response = Vec::with_capacity(1 + error.message.len());
    response.push(error.code);
    response.extend(encode_ssz_snappy_payload(&error.message)?);
    Ok(response)
}

fn encode_ssz_snappy_payload(raw_ssz: &[u8]) -> io::Result<Vec<u8>> {
    let mut payload = Vec::new();
    let mut header = unsigned_varint::encode::u64_buffer();
    payload.extend_from_slice(unsigned_varint::encode::u64(
        raw_ssz.len() as u64,
        &mut header,
    ));

    let mut encoder = FrameEncoder::new(Vec::new());
    io::Write::write_all(&mut encoder, raw_ssz)?;
    let compressed = encoder
        .into_inner()
        .map_err(|error| io::Error::other(error.error().to_string()))?;
    payload.extend_from_slice(&compressed);
    Ok(payload)
}

fn decode_ssz_snappy_payload(bytes: &[u8]) -> io::Result<Vec<u8>> {
    let (declared_len, compressed) =
        unsigned_varint::decode::u64(bytes).map_err(|error| invalid_data(error.to_string()))?;
    let mut decoder = FrameDecoder::new(compressed);
    let mut raw = Vec::new();
    io::Read::read_to_end(&mut decoder, &mut raw)?;
    if raw.len() != declared_len as usize {
        return Err(invalid_data(format!(
            "decoded payload length {} did not match declared {}",
            raw.len(),
            declared_len
        )));
    }
    Ok(raw)
}

fn encode_status(protocol: &Eth2RpcProtocol, status: StatusMessage) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(match protocol {
        Eth2RpcProtocol::StatusV2 => 92,
        _ => 84,
    });
    bytes.extend_from_slice(&status.fork_digest);
    bytes.extend_from_slice(status.finalized_root.as_slice());
    bytes.extend_from_slice(&encode_u64(status.finalized_epoch));
    bytes.extend_from_slice(status.head_root.as_slice());
    bytes.extend_from_slice(&encode_u64(status.head_slot));
    if matches!(protocol, Eth2RpcProtocol::StatusV2) {
        bytes.extend_from_slice(&encode_u64(status.earliest_available_slot));
    }
    bytes
}

fn encode_metadata(protocol: &Eth2RpcProtocol, metadata: MetaData) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(match protocol {
        Eth2RpcProtocol::MetadataV3 => 25,
        _ => 17,
    });
    bytes.extend_from_slice(&encode_u64(metadata.seq_number));
    bytes.extend_from_slice(&metadata.attnets);
    bytes.extend_from_slice(&metadata.syncnets);
    if matches!(protocol, Eth2RpcProtocol::MetadataV3) {
        bytes.extend_from_slice(&encode_u64(metadata.custody_group_count));
    }
    bytes
}

fn decode_status(protocol: &Eth2RpcProtocol, bytes: &[u8]) -> io::Result<StatusMessage> {
    let expected_len = match protocol {
        Eth2RpcProtocol::StatusV2 => 92,
        _ => 84,
    };
    if bytes.len() != expected_len {
        return Err(invalid_data(format!(
            "status message for {} must be {} bytes, got {}",
            protocol.as_ref(),
            expected_len,
            bytes.len()
        )));
    }

    let mut fork_digest = [0u8; 4];
    fork_digest.copy_from_slice(&bytes[0..4]);
    let finalized_root = B256::from_slice(&bytes[4..36]);
    let finalized_epoch = decode_u64(&bytes[36..44])?;
    let head_root = B256::from_slice(&bytes[44..76]);
    let head_slot = decode_u64(&bytes[76..84])?;
    let earliest_available_slot = match protocol {
        Eth2RpcProtocol::StatusV2 => decode_u64(&bytes[84..92])?,
        _ => 0,
    };

    Ok(StatusMessage {
        fork_digest,
        finalized_root,
        finalized_epoch,
        head_root,
        head_slot,
        earliest_available_slot,
    })
}

fn decode_metadata(protocol: &Eth2RpcProtocol, bytes: &[u8]) -> io::Result<MetaData> {
    let expected_len = match protocol {
        Eth2RpcProtocol::MetadataV3 => 25,
        _ => 17,
    };
    if bytes.len() != expected_len {
        return Err(invalid_data(format!(
            "metadata for {} must be {} bytes, got {}",
            protocol.as_ref(),
            expected_len,
            bytes.len()
        )));
    }

    let seq_number = decode_u64(&bytes[0..8])?;
    let mut attnets = [0u8; 8];
    attnets.copy_from_slice(&bytes[8..16]);
    let mut syncnets = [0u8; 1];
    syncnets.copy_from_slice(&bytes[16..17]);
    let custody_group_count = match protocol {
        Eth2RpcProtocol::MetadataV3 => decode_u64(&bytes[17..25])?,
        _ => 0,
    };

    Ok(MetaData {
        seq_number,
        attnets,
        syncnets,
        custody_group_count,
    })
}

fn encode_u64(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

fn decode_u64(bytes: &[u8]) -> io::Result<u64> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| invalid_data(format!("expected 8 bytes, got {}", bytes.len())))?;
    Ok(u64::from_le_bytes(array))
}

fn decode_root(bytes: &[u8]) -> io::Result<B256> {
    if bytes.len() != 32 {
        return Err(invalid_data(format!(
            "expected 32-byte root, got {}",
            bytes.len()
        )));
    }
    Ok(B256::from_slice(bytes))
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn success_response_context_len(protocol: &Eth2RpcProtocol) -> usize {
    match protocol {
        Eth2RpcProtocol::LightClientBootstrapV1
        | Eth2RpcProtocol::LightClientFinalityUpdateV1
        | Eth2RpcProtocol::LightClientOptimisticUpdateV1 => 4,
        Eth2RpcProtocol::StatusV1
        | Eth2RpcProtocol::StatusV2
        | Eth2RpcProtocol::GoodbyeV1
        | Eth2RpcProtocol::MetadataV1
        | Eth2RpcProtocol::MetadataV2
        | Eth2RpcProtocol::MetadataV3
        | Eth2RpcProtocol::PingV1 => 0,
    }
}

pub fn resource_unavailable(message: impl Into<Vec<u8>>) -> Eth2RpcResponse {
    Eth2RpcResponse::Error(Eth2RpcErrorResponse {
        code: RESOURCE_UNAVAILABLE_CODE,
        message: message.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trip() {
        let status = StatusMessage {
            fork_digest: [1, 2, 3, 4],
            finalized_root: B256::repeat_byte(0x11),
            finalized_epoch: 42,
            head_root: B256::repeat_byte(0x22),
            head_slot: 96,
            earliest_available_slot: 48,
        };

        let encoded = encode_status(&Eth2RpcProtocol::StatusV2, status);
        let decoded = decode_status(&Eth2RpcProtocol::StatusV2, &encoded).unwrap();

        assert_eq!(decoded, status);
    }

    #[test]
    fn ssz_snappy_payload_round_trip() {
        let raw = vec![0u8, 1, 2, 3, 4, 5, 6, 7, 8, 9];
        let encoded = encode_ssz_snappy_payload(&raw).unwrap();
        let decoded = decode_ssz_snappy_payload(&encoded).unwrap();

        assert_eq!(decoded, raw);
    }

    #[test]
    fn response_error_round_trip() {
        let response = Eth2RpcResponse::Error(Eth2RpcErrorResponse {
            code: 3,
            message: b"resource unavailable".to_vec(),
        });

        let encoded = encode_response(&Eth2RpcProtocol::LightClientBootstrapV1, response).unwrap();
        let decoded = decode_response(&Eth2RpcProtocol::LightClientBootstrapV1, &encoded).unwrap();

        assert_eq!(
            decoded,
            Eth2RpcResponse::Error(Eth2RpcErrorResponse {
                code: 3,
                message: b"resource unavailable".to_vec(),
            })
        );
    }

    #[test]
    fn goodbye_round_trip() {
        let encoded =
            encode_response(&Eth2RpcProtocol::GoodbyeV1, Eth2RpcResponse::Goodbye(3)).unwrap();
        let decoded = decode_response(&Eth2RpcProtocol::GoodbyeV1, &encoded).unwrap();

        assert_eq!(decoded, Eth2RpcResponse::Goodbye(3));
    }

    #[test]
    fn metadata_v2_round_trip() {
        let metadata = MetaData {
            seq_number: 7,
            attnets: [0xaa; 8],
            syncnets: [0x0f],
            custody_group_count: 0,
        };

        let encoded = encode_metadata(&Eth2RpcProtocol::MetadataV2, metadata);
        let decoded = decode_metadata(&Eth2RpcProtocol::MetadataV2, &encoded).unwrap();

        assert_eq!(decoded, metadata);
    }

    #[test]
    fn metadata_v1_round_trip() {
        let metadata = MetaData {
            seq_number: 5,
            attnets: [0xbb; 8],
            syncnets: [0x03],
            custody_group_count: 99,
        };

        let encoded = encode_metadata(&Eth2RpcProtocol::MetadataV1, metadata);
        let decoded = decode_metadata(&Eth2RpcProtocol::MetadataV1, &encoded).unwrap();

        assert_eq!(
            decoded,
            MetaData {
                custody_group_count: 0,
                ..metadata
            }
        );
    }

    #[test]
    fn status_v1_round_trip_zeroes_earliest_available_slot() {
        let status = StatusMessage {
            fork_digest: [9, 8, 7, 6],
            finalized_root: B256::repeat_byte(0x33),
            finalized_epoch: 1,
            head_root: B256::repeat_byte(0x44),
            head_slot: 64,
            earliest_available_slot: 12,
        };

        let encoded = encode_status(&Eth2RpcProtocol::StatusV1, status);
        let decoded = decode_status(&Eth2RpcProtocol::StatusV1, &encoded).unwrap();

        assert_eq!(
            decoded,
            StatusMessage {
                earliest_available_slot: 0,
                ..status
            }
        );
    }

    #[test]
    fn metadata_v3_round_trip() {
        let metadata = MetaData {
            seq_number: 7,
            attnets: [0xaa; 8],
            syncnets: [0x0f],
            custody_group_count: 3,
        };

        let encoded = encode_metadata(&Eth2RpcProtocol::MetadataV3, metadata);
        let decoded = decode_metadata(&Eth2RpcProtocol::MetadataV3, &encoded).unwrap();

        assert_eq!(decoded, metadata);
    }

    #[test]
    fn genesis_status_uses_genesis_block_root() {
        let genesis_root = B256::repeat_byte(0x55);
        let status = StatusMessage::genesis([1, 2, 3, 4], genesis_root);

        assert_eq!(status.finalized_root, B256::ZERO);
        assert_eq!(status.head_root, genesis_root);
        assert_eq!(status.finalized_epoch, 0);
        assert_eq!(status.head_slot, 0);
        assert_eq!(status.earliest_available_slot, 0);
    }

    #[test]
    fn light_client_response_round_trip_with_fork_context() {
        let response = Eth2RpcResponse::LightClientFinalityUpdate(RawRpcResponse {
            context_bytes: Some([0xaa, 0xbb, 0xcc, 0xdd]),
            bytes: vec![1, 2, 3, 4, 5],
        });

        let encoded =
            encode_response(&Eth2RpcProtocol::LightClientFinalityUpdateV1, response).unwrap();
        let decoded =
            decode_response(&Eth2RpcProtocol::LightClientFinalityUpdateV1, &encoded).unwrap();

        assert_eq!(
            decoded,
            Eth2RpcResponse::LightClientFinalityUpdate(RawRpcResponse {
                context_bytes: Some([0xaa, 0xbb, 0xcc, 0xdd]),
                bytes: vec![1, 2, 3, 4, 5],
            })
        );
    }
}
