use std::io;

use async_trait::async_trait;
use alloy_primitives::B256;
use futures::prelude::*;
use libp2p::request_response::{self, Codec, ProtocolSupport};
use snap::read::FrameDecoder;
use snap::write::FrameEncoder;

const STATUS_PROTOCOL_ID: &str = "/eth2/beacon_chain/req/status/1/ssz_snappy";
const PING_PROTOCOL_ID: &str = "/eth2/beacon_chain/req/ping/1/ssz_snappy";
const LIGHT_CLIENT_BOOTSTRAP_PROTOCOL_ID: &str =
    "/eth2/beacon_chain/req/light_client_bootstrap/1/ssz_snappy";
const LIGHT_CLIENT_FINALITY_UPDATE_PROTOCOL_ID: &str =
    "/eth2/beacon_chain/req/light_client_finality_update/1/ssz_snappy";
const LIGHT_CLIENT_OPTIMISTIC_UPDATE_PROTOCOL_ID: &str =
    "/eth2/beacon_chain/req/light_client_optimistic_update/1/ssz_snappy";

const SUCCESS_CODE: u8 = 0;
const ERROR_MESSAGE_LIMIT: usize = 256;

pub type Eth2RpcBehaviour = request_response::Behaviour<Eth2RpcCodec>;
pub type Eth2RpcEvent = request_response::Event<Eth2RpcRequest, Eth2RpcResponse>;
pub type Eth2OutboundRequestId = request_response::OutboundRequestId;
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Eth2RpcProtocol {
    StatusV1,
    PingV1,
    LightClientBootstrapV1,
    LightClientFinalityUpdateV1,
    LightClientOptimisticUpdateV1,
}

impl AsRef<str> for Eth2RpcProtocol {
    fn as_ref(&self) -> &str {
        match self {
            Self::StatusV1 => STATUS_PROTOCOL_ID,
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
    Ping(u64),
    LightClientBootstrap(B256),
    LightClientFinalityUpdate,
    LightClientOptimisticUpdate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Eth2RpcResponse {
    Status(StatusMessage),
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
}

impl StatusMessage {
    pub fn genesis(fork_digest: [u8; 4]) -> Self {
        Self {
            fork_digest,
            finalized_root: B256::ZERO,
            finalized_epoch: 0,
            head_root: B256::ZERO,
            head_slot: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRpcResponse {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Eth2RpcErrorResponse {
    pub code: u8,
    pub message: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct Eth2RpcCodec;

pub fn build_rpc_behaviour() -> Eth2RpcBehaviour {
    let config = request_response::Config::default()
        .with_request_timeout(std::time::Duration::from_secs(15))
        .with_max_concurrent_streams(64);

    Eth2RpcBehaviour::with_codec(
        Eth2RpcCodec,
        [
            (Eth2RpcProtocol::StatusV1, ProtocolSupport::Outbound),
            (Eth2RpcProtocol::PingV1, ProtocolSupport::Outbound),
            (
                Eth2RpcProtocol::LightClientBootstrapV1,
                ProtocolSupport::Outbound,
            ),
            (
                Eth2RpcProtocol::LightClientFinalityUpdateV1,
                ProtocolSupport::Outbound,
            ),
            (
                Eth2RpcProtocol::LightClientOptimisticUpdateV1,
                ProtocolSupport::Outbound,
            ),
        ],
        config,
    )
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
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let payload = encode_request(req)?;
        io.write_all(&payload).await?;
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        res: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let payload = encode_response(res)?;
        io.write_all(&payload).await?;
        io.close().await
    }
}

fn encode_request(request: Eth2RpcRequest) -> io::Result<Vec<u8>> {
    match request {
        Eth2RpcRequest::Status(status) => encode_ssz_snappy_payload(&encode_status(status)),
        Eth2RpcRequest::Ping(seq_number) => {
            encode_ssz_snappy_payload(&encode_u64(seq_number))
        }
        Eth2RpcRequest::LightClientBootstrap(root) => {
            encode_ssz_snappy_payload(root.as_slice())
        }
        Eth2RpcRequest::LightClientFinalityUpdate => Ok(Vec::new()),
        Eth2RpcRequest::LightClientOptimisticUpdate => Ok(Vec::new()),
    }
}

fn encode_response(response: Eth2RpcResponse) -> io::Result<Vec<u8>> {
    match response {
        Eth2RpcResponse::Status(status) => {
            encode_single_success_response(&encode_status(status))
        }
        Eth2RpcResponse::Ping(seq_number) => {
            encode_single_success_response(&encode_u64(seq_number))
        }
        Eth2RpcResponse::LightClientBootstrap(raw)
        | Eth2RpcResponse::LightClientFinalityUpdate(raw)
        | Eth2RpcResponse::LightClientOptimisticUpdate(raw) => {
            encode_single_success_response(&raw.bytes)
        }
        Eth2RpcResponse::Error(error) => encode_single_error_response(error),
    }
}

fn decode_request(protocol: &Eth2RpcProtocol, payload: &[u8]) -> io::Result<Eth2RpcRequest> {
    match protocol {
        Eth2RpcProtocol::StatusV1 => decode_status(payload).map(Eth2RpcRequest::Status),
        Eth2RpcProtocol::PingV1 => decode_u64(payload).map(Eth2RpcRequest::Ping),
        Eth2RpcProtocol::LightClientBootstrapV1 => decode_root(payload)
            .map(Eth2RpcRequest::LightClientBootstrap),
        Eth2RpcProtocol::LightClientFinalityUpdateV1 => {
            if payload.is_empty() {
                Ok(Eth2RpcRequest::LightClientFinalityUpdate)
            } else {
                Err(invalid_data("light client finality update request must be empty"))
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
    let payload = decode_ssz_snappy_payload(&bytes[1..])?;
    if result_code != SUCCESS_CODE {
        if payload.len() > ERROR_MESSAGE_LIMIT {
            return Err(invalid_data("error payload exceeds ErrorMessage limit"));
        }
        return Ok(Eth2RpcResponse::Error(Eth2RpcErrorResponse {
            code: result_code,
            message: payload,
        }));
    }

    match protocol {
        Eth2RpcProtocol::StatusV1 => {
            decode_status(&payload).map(Eth2RpcResponse::Status)
        }
        Eth2RpcProtocol::PingV1 => decode_u64(&payload).map(Eth2RpcResponse::Ping),
        Eth2RpcProtocol::LightClientBootstrapV1 => Ok(
            Eth2RpcResponse::LightClientBootstrap(RawRpcResponse { bytes: payload }),
        ),
        Eth2RpcProtocol::LightClientFinalityUpdateV1 => Ok(
            Eth2RpcResponse::LightClientFinalityUpdate(RawRpcResponse { bytes: payload }),
        ),
        Eth2RpcProtocol::LightClientOptimisticUpdateV1 => Ok(
            Eth2RpcResponse::LightClientOptimisticUpdate(RawRpcResponse { bytes: payload }),
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
    let mut response = Vec::with_capacity(1 + raw_ssz.len());
    response.push(SUCCESS_CODE);
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

fn encode_status(status: StatusMessage) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(84);
    bytes.extend_from_slice(&status.fork_digest);
    bytes.extend_from_slice(status.finalized_root.as_slice());
    bytes.extend_from_slice(&encode_u64(status.finalized_epoch));
    bytes.extend_from_slice(status.head_root.as_slice());
    bytes.extend_from_slice(&encode_u64(status.head_slot));
    bytes
}

fn decode_status(bytes: &[u8]) -> io::Result<StatusMessage> {
    if bytes.len() != 84 {
        return Err(invalid_data(format!(
            "status message must be 84 bytes, got {}",
            bytes.len()
        )));
    }

    let mut fork_digest = [0u8; 4];
    fork_digest.copy_from_slice(&bytes[0..4]);
    let finalized_root = B256::from_slice(&bytes[4..36]);
    let finalized_epoch = decode_u64(&bytes[36..44])?;
    let head_root = B256::from_slice(&bytes[44..76]);
    let head_slot = decode_u64(&bytes[76..84])?;

    Ok(StatusMessage {
        fork_digest,
        finalized_root,
        finalized_epoch,
        head_root,
        head_slot,
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
        };

        let encoded = encode_status(status);
        let decoded = decode_status(&encoded).unwrap();

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

        let encoded = encode_response(response).unwrap();
        let decoded = decode_response(&Eth2RpcProtocol::LightClientBootstrapV1, &encoded).unwrap();

        assert_eq!(
            decoded,
            Eth2RpcResponse::Error(Eth2RpcErrorResponse {
                code: 3,
                message: b"resource unavailable".to_vec(),
            })
        );
    }
}
