use super::compression::{
    compress, CompressionEncoding, CompressionSettings, SingleMessageCompressionOverride,
};
use super::{
    BufferSettings, EncodeBuf, Encoder, OwnedEncodeBuffer, PreparedEncode,
    DEFAULT_MAX_SEND_MESSAGE_SIZE, HEADER_SIZE,
};
use crate::Status;
use bytes::{BufMut, Bytes, BytesMut};
use http::HeaderMap;
use http_body::{Body, Frame};
use pin_project::pin_project;
use std::{
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio_stream::{adapters::Fuse, Stream, StreamExt};

/// Combinator for efficient encoding of messages into reasonably sized buffers.
/// EncodedBytes encodes ready messages from its delegate stream into a BytesMut,
/// splitting off and yielding a buffer when either:
///  * The delegate stream polls as not ready, or
///  * The encoded buffer surpasses YIELD_THRESHOLD.
#[pin_project(project = EncodedBytesProj)]
#[derive(Debug)]
struct EncodedBytes<T, U> {
    #[pin]
    source: Option<Fuse<U>>,
    encoder: T,
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    buf: BytesMut,
    uncompression_buf: BytesMut,
    error: Option<Status>,
    prepared: bool,
    owned: Option<OwnedEncodeBuffer>,
    identity: Option<PreparedEncode>,
    done: bool,
}

impl<T: Encoder, U: Stream> EncodedBytes<T, U> {
    fn new(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        compression_override: SingleMessageCompressionOverride,
        max_message_size: Option<usize>,
    ) -> Self {
        let buffer_settings = encoder.buffer_settings();
        let prepared = encoder.uses_prepared_buffers();
        let buf = if prepared {
            BytesMut::new()
        } else {
            BytesMut::with_capacity(buffer_settings.buffer_size)
        };

        let compression_encoding =
            if compression_override == SingleMessageCompressionOverride::Disable {
                None
            } else {
                compression_encoding
            };

        let uncompression_buf = if compression_encoding.is_some() && !prepared {
            BytesMut::with_capacity(buffer_settings.buffer_size)
        } else {
            BytesMut::new()
        };

        Self {
            source: Some(source.fuse()),
            encoder,
            compression_encoding,
            max_message_size,
            buf,
            uncompression_buf,
            error: None,
            prepared,
            owned: None,
            identity: None,
            done: false,
        }
    }
}

impl<T, U> Stream for EncodedBytes<T, U>
where
    T: Encoder<Error = Status>,
    U: Stream<Item = Result<T::Item, Status>>,
{
    type Item = Result<Bytes, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let EncodedBytesProj {
            mut source,
            encoder,
            compression_encoding,
            max_message_size,
            buf,
            uncompression_buf,
            error,
            prepared,
            owned,
            identity,
            done,
        } = self.project();
        if *done {
            return Poll::Ready(None);
        }
        if let Some(status) = error.take() {
            *done = true;
            source.set(None);
            return Poll::Ready(Some(Err(status)));
        }
        let settings = encoder.buffer_settings();
        loop {
            let polled = match source.as_mut().as_pin_mut() {
                Some(source) => source.poll_next(cx),
                None => Poll::Ready(None),
            };
            match polled {
                Poll::Pending => {
                    if output_len(buf, owned) == 0 {
                        return Poll::Pending;
                    }
                    return Poll::Ready(Some(Ok(flush_output(buf, owned))));
                }
                Poll::Ready(None) => {
                    source.set(None);
                    *done = true;
                    return Poll::Ready(if output_len(buf, owned) == 0 {
                        None
                    } else {
                        Some(Ok(flush_output(buf, owned)))
                    });
                }
                Poll::Ready(Some(Ok(item))) => {
                    let result = if *prepared {
                        encode_prepared(
                            encoder,
                            owned,
                            identity,
                            *compression_encoding,
                            *max_message_size,
                            settings,
                            item,
                        )
                    } else {
                        encode_item(
                            encoder,
                            buf,
                            uncompression_buf,
                            *compression_encoding,
                            *max_message_size,
                            settings,
                            item,
                        )
                    };
                    if let Err(status) = result {
                        *done = true;
                        source.set(None);
                        *owned = None;
                        *buf = BytesMut::new();
                        *uncompression_buf = BytesMut::new();
                        return Poll::Ready(Some(Err(status)));
                    }
                    if output_len(buf, owned) >= settings.yield_threshold {
                        return Poll::Ready(Some(Ok(flush_output(buf, owned))));
                    }
                }
                Poll::Ready(Some(Err(status))) => {
                    source.set(None);
                    if output_len(buf, owned) == 0 {
                        *done = true;
                        return Poll::Ready(Some(Err(status)));
                    }
                    *error = Some(status);
                    return Poll::Ready(Some(Ok(flush_output(buf, owned))));
                }
            }
        }
    }
}

fn output_len(buf: &BytesMut, owned: &Option<OwnedEncodeBuffer>) -> usize {
    owned.as_ref().map_or(buf.len(), OwnedEncodeBuffer::len)
}
fn flush_output(buf: &mut BytesMut, owned: &mut Option<OwnedEncodeBuffer>) -> Bytes {
    match owned.take() {
        Some(buffer) => buffer.into_bytes(),
        None => buf.split_to(buf.len()).freeze(),
    }
}

fn encode_prepared<T: Encoder<Error = Status>>(
    encoder: &mut T,
    owned: &mut Option<OwnedEncodeBuffer>,
    identity: &mut Option<PreparedEncode>,
    compression: Option<CompressionEncoding>,
    maximum: Option<usize>,
    settings: BufferSettings,
    item: T::Item,
) -> Result<(), Status> {
    if compression.is_some() {
        return Err(Status::unimplemented(
            "prepared encoding does not support compression",
        ));
    }
    let preparation = encoder.prepare(&item)?;
    if let Some(previous) = identity {
        if !std::sync::Arc::ptr_eq(&previous.allocator, &preparation.allocator) {
            return Err(Status::internal("prepared encoder changed allocator"));
        }
    } else {
        *identity = Some(preparation.clone());
    }
    let length = preparation.payload_len;
    if length > maximum.unwrap_or(DEFAULT_MAX_SEND_MESSAGE_SIZE) {
        return Err(Status::out_of_range(
            "encoded message exceeds configured maximum",
        ));
    }
    if length > u32::MAX as usize {
        return Err(Status::resource_exhausted("encoded message exceeds 4GB"));
    }
    let offset = owned.as_ref().map_or(0, OwnedEncodeBuffer::len);
    let needed = offset
        .checked_add(HEADER_SIZE)
        .and_then(|n| n.checked_add(length))
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or_else(|| Status::resource_exhausted("encoded buffer size overflow"))?;
    if owned
        .as_ref()
        .map_or(true, |buffer| buffer.capacity() < needed)
    {
        let interval = settings.buffer_size.max(1);
        let capacity = needed
            .checked_add(interval - 1)
            .map(|n| n / interval * interval)
            .filter(|n| *n <= isize::MAX as usize)
            .ok_or_else(|| Status::resource_exhausted("encoded buffer capacity overflow"))?;
        let mut replacement = preparation.allocator.allocate(capacity)?;
        if !replacement.is_empty() || replacement.capacity() < capacity {
            return Err(Status::internal(
                "allocator returned invalid encoding buffer",
            ));
        }
        if let Some(previous) = owned.as_ref() {
            replacement.buffer.extend_from_slice(&previous.buffer);
        }
        *owned = Some(replacement);
    }
    let buffer = &mut owned.as_mut().expect("allocated encoding buffer").buffer;
    buffer.extend_from_slice(&[0; HEADER_SIZE]);
    encoder.encode(item, &mut EncodeBuf::bounded(buffer, length))?;
    if buffer.len() != needed {
        return Err(Status::internal(
            "encoder did not write its prepared length",
        ));
    }
    finish_encoding(None, maximum, &mut buffer[offset..])
}

fn encode_item<T>(
    encoder: &mut T,
    buf: &mut BytesMut,
    uncompression_buf: &mut BytesMut,
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    buffer_settings: BufferSettings,
    item: T::Item,
) -> Result<(), Status>
where
    T: Encoder<Error = Status>,
{
    let offset = buf.len();

    buf.put_slice(&[0; HEADER_SIZE]);

    if let Some(encoding) = compression_encoding {
        uncompression_buf.clear();

        encoder
            .encode(item, &mut EncodeBuf::new(uncompression_buf))
            .map_err(|err| Status::internal(format!("Error encoding: {err}")))?;

        let uncompressed_len = uncompression_buf.len();

        compress(
            CompressionSettings {
                encoding,
                buffer_growth_interval: buffer_settings.buffer_size,
            },
            uncompression_buf,
            buf,
            uncompressed_len,
        )
        .map_err(|err| Status::internal(format!("Error compressing: {err}")))?;
    } else {
        encoder
            .encode(item, &mut EncodeBuf::new(buf))
            .map_err(|err| Status::internal(format!("Error encoding: {err}")))?;
    }

    // now that we know length, we can write the header
    finish_encoding(compression_encoding, max_message_size, &mut buf[offset..])
}

fn finish_encoding(
    compression_encoding: Option<CompressionEncoding>,
    max_message_size: Option<usize>,
    buf: &mut [u8],
) -> Result<(), Status> {
    let len = buf.len() - HEADER_SIZE;
    let limit = max_message_size.unwrap_or(DEFAULT_MAX_SEND_MESSAGE_SIZE);
    if len > limit {
        return Err(Status::out_of_range(format!(
            "Error, encoded message length too large: found {len} bytes, the limit is: {limit} bytes"
        )));
    }

    if len > u32::MAX as usize {
        return Err(Status::resource_exhausted(format!(
            "Cannot return body with more than 4GB of data but got {len} bytes"
        )));
    }
    {
        let mut buf = &mut buf[..HEADER_SIZE];
        buf.put_u8(compression_encoding.is_some() as u8);
        buf.put_u32(len as u32);
    }

    Ok(())
}

#[derive(Debug)]
enum Role {
    Client,
    Server,
}

/// A specialized implementation of [Body] for encoding [Result<Bytes, Status>].
#[pin_project]
#[derive(Debug)]
pub struct EncodeBody<T, U> {
    #[pin]
    inner: EncodedBytes<T, U>,
    state: EncodeState,
}

#[derive(Debug)]
struct EncodeState {
    error: Option<Status>,
    role: Role,
    is_end_stream: bool,
}

impl<T: Encoder, U: Stream> EncodeBody<T, U> {
    /// Turns a stream of grpc messages into [EncodeBody] which is used by grpc clients for
    /// turning the messages into http frames for sending over the network.
    pub fn new_client(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        max_message_size: Option<usize>,
    ) -> Self {
        Self {
            inner: EncodedBytes::new(
                encoder,
                source,
                compression_encoding,
                SingleMessageCompressionOverride::default(),
                max_message_size,
            ),
            state: EncodeState {
                error: None,
                role: Role::Client,
                is_end_stream: false,
            },
        }
    }

    /// Turns a stream of grpc results (message or error status) into [EncodeBody] which is used by grpc
    /// servers for turning the messages into http frames for sending over the network.
    pub fn new_server(
        encoder: T,
        source: U,
        compression_encoding: Option<CompressionEncoding>,
        compression_override: SingleMessageCompressionOverride,
        max_message_size: Option<usize>,
    ) -> Self {
        Self {
            inner: EncodedBytes::new(
                encoder,
                source,
                compression_encoding,
                compression_override,
                max_message_size,
            ),
            state: EncodeState {
                error: None,
                role: Role::Server,
                is_end_stream: false,
            },
        }
    }
}

impl EncodeState {
    fn trailers(&mut self) -> Option<Result<HeaderMap, Status>> {
        match self.role {
            Role::Client => {
                self.is_end_stream = true;
                None
            }
            Role::Server => {
                if self.is_end_stream {
                    return None;
                }

                self.is_end_stream = true;
                let status = if let Some(status) = self.error.take() {
                    status
                } else {
                    Status::ok("")
                };
                Some(status.to_header_map())
            }
        }
    }
}

impl<T, U> Body for EncodeBody<T, U>
where
    T: Encoder<Error = Status>,
    U: Stream<Item = Result<T::Item, Status>>,
{
    type Data = Bytes;
    type Error = Status;

    fn is_end_stream(&self) -> bool {
        self.state.is_end_stream
    }

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let self_proj = self.project();
        if self_proj.state.is_end_stream {
            return Poll::Ready(None);
        }
        match ready!(self_proj.inner.poll_next(cx)) {
            Some(Ok(d)) => Some(Ok(Frame::data(d))).into(),
            Some(Err(status)) => match self_proj.state.role {
                Role::Client => {
                    self_proj.state.is_end_stream = true;
                    Some(Err(status)).into()
                }
                Role::Server => {
                    self_proj.state.is_end_stream = true;
                    Some(Ok(Frame::trailers(status.to_header_map()?))).into()
                }
            },
            None => self_proj
                .state
                .trailers()
                .map(|t| t.map(Frame::trailers))
                .into(),
        }
    }
}

#[cfg(test)]
#[path = "logex_tests.rs"]
mod logex_tests;
