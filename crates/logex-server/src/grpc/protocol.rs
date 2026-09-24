//! Inseparable owners for protobuf allocations and the buffers that encode them.
// Tonic's Codec/Encoder signatures require an unboxed Status.
#![allow(clippy::result_large_err)]
use std::{
    fmt, io,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::{Bytes, BytesMut};
use logex_query::QueryCancelCheck;
use logex_types::{
    LogRow, QueryBuffer, QueryMemoryBudget, QueryMemoryError, QueryMemoryReservation,
};
use prost::Message;
use serde::Serialize;
use tokio_stream::Stream;
use tonic::{
    Status,
    codec::{
        Codec, EncodeBuf, EncodeBufferAllocator, Encoder, OwnedEncodeBuffer, PreparedEncode,
        ProstCodec,
    },
};

use super::pb::{GetLogsResponse, LogEntry};

mod query;
pub(super) use query::query_response;

const PROTOCOL_STAGE: &str = "gRPC query protocol";
const ENCODING_STAGE: &str = "gRPC query encoding";
type EncodingCheck = Arc<dyn Fn() -> Result<(), Status> + Send + Sync>;

/// A generated response whose allocation accounting cannot be detached from it.
/// Read-only access preserves normal inspection; encoding consumes this owner.
pub struct OwnedResponse<T> {
    message: T,
    encoded_len: usize,
    allocator: Arc<dyn EncodeBufferAllocator>,
    check: Option<EncodingCheck>,
    // The message (and every nested allocation) must be dropped first.
    charge: Arc<QueryMemoryReservation>,
}

impl<T: fmt::Debug> fmt::Debug for OwnedResponse<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedResponse")
            .field("message", &self.message)
            .field("encoded_len", &self.encoded_len)
            .finish_non_exhaustive()
    }
}

impl<T> Deref for OwnedResponse<T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.message
    }
}

impl<T> OwnedResponse<T> {
    pub(super) fn with_encoding_check(
        mut self,
        check: impl Fn() -> Result<(), Status> + Send + Sync + 'static,
    ) -> Self {
        let check: EncodingCheck = Arc::new(check);
        self.allocator = Arc::new(CheckedAllocator {
            allocator: Arc::clone(&self.allocator),
            check: Arc::clone(&check),
        });
        self.check = Some(check);
        self
    }

    fn check_encoding(&self) -> Result<(), Status> {
        self.check.as_ref().map_or(Ok(()), |check| check())
    }
}

struct CheckedAllocator {
    allocator: Arc<dyn EncodeBufferAllocator>,
    check: EncodingCheck,
}

impl EncodeBufferAllocator for CheckedAllocator {
    fn allocate(&self, capacity: usize) -> Result<OwnedEncodeBuffer, Status> {
        (self.check)()?;
        self.allocator.allocate(capacity)
    }
}

/// Uses standard Prost decoding and Tonic framing, with an owned encoding item.
pub struct OwnedProstCodec<T, U>(PhantomData<fn() -> (T, U)>);

impl<T, U> Default for OwnedProstCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T, U> Codec for OwnedProstCodec<T, U>
where
    T: Message + Send + 'static,
    U: Message + Default + Send + 'static,
{
    type Encode = OwnedResponse<T>;
    type Decode = U;
    type Encoder = OwnedProstEncoder<T>;
    type Decoder = <ProstCodec<T, U> as Codec>::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        OwnedProstEncoder(PhantomData)
    }
    fn decoder(&mut self) -> Self::Decoder {
        ProstCodec::<T, U>::default().decoder()
    }
}

/// The item stays intact while Prost borrows its message for encoding.
pub struct OwnedProstEncoder<T>(PhantomData<fn() -> T>);

impl<T: Message> Encoder for OwnedProstEncoder<T> {
    type Item = OwnedResponse<T>;
    type Error = Status;

    fn uses_prepared_buffers(&self) -> bool {
        true
    }

    fn prepare(&mut self, item: &Self::Item) -> Result<PreparedEncode, Status> {
        item.check_encoding()?;
        Ok(PreparedEncode::new(
            item.encoded_len,
            Arc::clone(&item.allocator),
        ))
    }

    fn encode(&mut self, item: Self::Item, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        item.check_encoding()?;
        item.message
            .encode(dst)
            .map_err(|error| Status::internal(format!("protobuf size mismatch: {error}")))?;
        // Prost encodes one admitted message synchronously. Recheck before Tonic
        // can publish it; cancellation does not interrupt an individual copy.
        item.check_encoding()
    }
}

struct OutputAllocator(QueryMemoryBudget);

struct Allocation<T> {
    value: T,
    charge: QueryMemoryReservation,
}

impl EncodeBufferAllocator for OutputAllocator {
    fn allocate(&self, capacity: usize) -> Result<OwnedEncodeBuffer, Status> {
        self.allocate_buffer(capacity).map_err(into_status)
    }
}

impl OutputAllocator {
    fn allocate_buffer(&self, capacity: usize) -> io::Result<OwnedEncodeBuffer> {
        if capacity == 0 {
            return Ok(OwnedEncodeBuffer::new(
                BytesMut::new(),
                self.0
                    .reserve(0, ENCODING_STAGE)
                    .map_err(io::Error::other)?,
            ));
        }
        let mut allocation = Allocation {
            value: Vec::<u8>::new(),
            charge: self
                .0
                .reserve(capacity, ENCODING_STAGE)
                .map_err(io::Error::other)?,
        };
        allocation
            .value
            .try_reserve_exact(capacity)
            .map_err(io::Error::other)?;
        reconcile(
            &self.0,
            &mut allocation.charge,
            capacity,
            allocation.value.capacity(),
            ENCODING_STAGE,
        )?;
        let requested = allocation.value.capacity();
        // Pinned bytes 1.11.1 transfers this uniquely owned Vec without copying.
        // Its fallible conversion retains the original allocation on failure.
        let mut allocation = Allocation {
            value: Bytes::from(allocation.value)
                .try_into_mut()
                .map_err(|_| io::Error::other("cannot transfer uniquely owned gRPC buffer"))?,
            charge: allocation.charge,
        };
        reconcile(
            &self.0,
            &mut allocation.charge,
            requested,
            allocation.value.capacity(),
            ENCODING_STAGE,
        )?;
        Ok(OwnedEncodeBuffer::new(allocation.value, allocation.charge))
    }
}

fn reconcile(
    memory: &QueryMemoryBudget,
    charge: &mut QueryMemoryReservation,
    requested: usize,
    actual: usize,
    stage: &'static str,
) -> io::Result<()> {
    if actual > requested {
        let extra = actual - requested;
        let used = memory.used();
        // Keep observed allocator excess charged even on rejection, until the
        // enclosing owner drops the backing allocation before this reservation.
        charge.record_existing(extra);
        if memory.used() > memory.limit() as u128 {
            return Err(io::Error::other(QueryMemoryError::CapacityExceeded {
                requested: extra,
                used,
                limit: memory.limit(),
                stage,
            }));
        }
    } else if actual < requested {
        charge
            .shrink(requested - actual)
            .map_err(io::Error::other)?;
    }
    Ok(())
}

struct ResponseBuilder<T> {
    message: T,
    memory: QueryMemoryBudget,
    charge: QueryMemoryReservation,
}

impl<T> ResponseBuilder<T> {
    // The initial message must have no owned heap allocations. The allowance
    // covers every requested vector capacity before construction starts.
    fn new(message: T, memory: &QueryMemoryBudget, initial_bytes: usize) -> io::Result<Self> {
        Ok(Self {
            message,
            memory: memory.clone(),
            charge: memory
                .reserve(initial_bytes, PROTOCOL_STAGE)
                .map_err(io::Error::other)?,
        })
    }

    fn allocate_vec<U>(&mut self, len: usize) -> io::Result<Vec<U>> {
        let requested =
            QueryMemoryBudget::array_bytes::<U>(len, PROTOCOL_STAGE).map_err(io::Error::other)?;
        let mut values = Vec::new();
        values.try_reserve_exact(len).map_err(io::Error::other)?;
        let actual = QueryMemoryBudget::array_bytes::<U>(values.capacity(), PROTOCOL_STAGE)
            .map_err(io::Error::other)?;
        reconcile(
            &self.memory,
            &mut self.charge,
            requested,
            actual,
            PROTOCOL_STAGE,
        )?;
        Ok(values)
    }

    fn copy_bytes(
        &mut self,
        bytes: &[u8],
        cancel: Option<&QueryCancelCheck>,
    ) -> io::Result<Vec<u8>> {
        let mut output = self.allocate_vec(bytes.len())?;
        for chunk in bytes.chunks(64 * 1024) {
            check_cancel(cancel)?;
            output.extend_from_slice(chunk);
        }
        Ok(output)
    }

    fn json<S: Serialize + ?Sized>(
        &mut self,
        value: &S,
        cancel: Option<&QueryCancelCheck>,
    ) -> io::Result<String> {
        struct Pending<T> {
            value: T,
            charge: Option<QueryMemoryReservation>,
        }
        impl<T> From<QueryBuffer<T>> for Pending<Vec<T>> {
            fn from(buffer: QueryBuffer<T>) -> Self {
                let (value, charge) = buffer.into_parts();
                Self { value, charge }
            }
        }
        let pending = Pending::from(crate::query_encoding::serialize_json_buffer(
            value,
            &self.memory,
            cancel,
            PROTOCOL_STAGE,
        )?);
        let mut pending = Pending {
            value: String::from_utf8(pending.value)
                .map_err(|_| io::Error::other("JSON encoding produced invalid UTF-8"))?,
            charge: pending.charge,
        };
        let charge = pending
            .charge
            .as_mut()
            .ok_or_else(|| io::Error::other("missing gRPC JSON allocation owner"))?;
        self.charge.absorb(charge).map_err(io::Error::other)?;
        Ok(pending.value)
    }
}

impl<T: CheckedEncodedLen> ResponseBuilder<T> {
    fn finish(self) -> io::Result<OwnedResponse<T>> {
        let encoded_len = self.message.checked_encoded_len()?;
        Ok(OwnedResponse {
            message: self.message,
            encoded_len,
            allocator: Arc::new(OutputAllocator(self.memory)),
            check: None,
            charge: Arc::new(self.charge),
        })
    }
}

trait CheckedEncodedLen {
    fn checked_encoded_len(&self) -> io::Result<usize>;
}

fn checked_add(total: &mut usize, value: usize) -> io::Result<()> {
    *total = total
        .checked_add(value)
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or_else(|| {
            io::Error::other(QueryMemoryError::SizeOverflow {
                stage: PROTOCOL_STAGE,
            })
        })?;
    Ok(())
}

fn uint_field_len(value: u64, present: bool) -> usize {
    if present {
        1 + prost::encoding::encoded_len_varint(value)
    } else {
        0
    }
}

fn message_field_len(len: usize) -> io::Result<usize> {
    let value = u64::try_from(len).map_err(io::Error::other)?;
    let mut size = uint_field_len(value, true);
    checked_add(&mut size, len)?;
    Ok(size)
}

fn bytes_field_len(len: usize, present: bool) -> io::Result<usize> {
    if present {
        message_field_len(len)
    } else {
        Ok(0)
    }
}

fn check_cancel(cancel: Option<&QueryCancelCheck>) -> io::Result<()> {
    if cancel.is_some_and(|check| check()) {
        Err(io::Error::new(io::ErrorKind::Interrupted, "query canceled"))
    } else {
        Ok(())
    }
}

pub(super) fn into_status(error: io::Error) -> Status {
    if crate::query_encoding::is_capacity_error(&error) {
        Status::resource_exhausted(error.to_string())
    } else {
        match error.kind() {
            io::ErrorKind::WouldBlock => Status::aborted(error.to_string()),
            io::ErrorKind::Interrupted => Status::cancelled(error.to_string()),
            _ => Status::internal(format!("query response error: {error}")),
        }
    }
}

pub(super) fn log_response(
    rows: &[LogRow],
    memory: &QueryMemoryBudget,
    cancel: Option<&QueryCancelCheck>,
) -> io::Result<OwnedResponse<GetLogsResponse>> {
    check_cancel(cancel)?;
    let mut requested = QueryMemoryBudget::array_bytes::<LogEntry>(rows.len(), PROTOCOL_STAGE)
        .map_err(io::Error::other)?;
    for (index, row) in rows.iter().enumerate() {
        if index % 256 == 0 {
            check_cancel(cancel)?;
        }
        let topics = [row.topic0, row.topic1, row.topic2, row.topic3]
            .into_iter()
            .flatten()
            .count();
        checked_add(&mut requested, 32 + 32 + 20)?;
        checked_add(&mut requested, row.data.len())?;
        checked_add(
            &mut requested,
            QueryMemoryBudget::array_bytes::<Vec<u8>>(topics, PROTOCOL_STAGE)
                .map_err(io::Error::other)?,
        )?;
        checked_add(&mut requested, topics * 32)?;
    }
    let mut builder = ResponseBuilder::new(GetLogsResponse::default(), memory, requested)?;
    builder.message.logs = builder.allocate_vec(rows.len())?;
    for row in rows {
        check_cancel(cancel)?;
        let mut entry = LogEntry {
            block_number: row.block_number,
            timestamp: row.timestamp,
            tx_index: row.tx_index,
            log_index: row.log_index,
            data_len: row.data_len,
            source: row.source as u32,
            ..Default::default()
        };
        entry.block_hash = builder.copy_bytes(row.block_hash.as_slice(), cancel)?;
        entry.tx_hash = builder.copy_bytes(row.tx_hash.as_slice(), cancel)?;
        entry.address = builder.copy_bytes(row.address.as_slice(), cancel)?;
        let topics = [row.topic0, row.topic1, row.topic2, row.topic3];
        entry.topics = builder.allocate_vec(topics.iter().flatten().count())?;
        for topic in topics.into_iter().flatten() {
            entry
                .topics
                .push(builder.copy_bytes(topic.as_slice(), cancel)?);
        }
        entry.data = builder.copy_bytes(&row.data, cancel)?;
        builder.message.logs.push(entry);
    }
    builder.message.row_count = u64::try_from(rows.len()).map_err(io::Error::other)?;
    check_cancel(cancel)?;
    builder.finish()
}

impl CheckedEncodedLen for LogEntry {
    fn checked_encoded_len(&self) -> io::Result<usize> {
        let mut len = 0;
        for value in [
            self.block_number,
            self.timestamp,
            u64::from(self.tx_index),
            u64::from(self.log_index),
            u64::from(self.data_len),
            u64::from(self.source),
        ] {
            checked_add(&mut len, uint_field_len(value, value != 0))?;
        }
        for bytes in [&self.block_hash, &self.tx_hash, &self.address, &self.data] {
            checked_add(&mut len, bytes_field_len(bytes.len(), !bytes.is_empty())?)?;
        }
        for topic in &self.topics {
            checked_add(&mut len, bytes_field_len(topic.len(), true)?)?;
        }
        Ok(len)
    }
}

impl CheckedEncodedLen for GetLogsResponse {
    fn checked_encoded_len(&self) -> io::Result<usize> {
        let mut len = uint_field_len(self.row_count, self.row_count != 0);
        for log in &self.logs {
            checked_add(&mut len, message_field_len(log.checked_encoded_len()?)?)?;
        }
        Ok(len)
    }
}

pub(super) struct LogResponseStream {
    entries: std::vec::IntoIter<LogEntry>,
    done: bool,
    allocator: Arc<dyn EncodeBufferAllocator>,
    check: Option<EncodingCheck>,
    charge: Arc<QueryMemoryReservation>,
}

impl OwnedResponse<GetLogsResponse> {
    pub(super) fn into_stream(self) -> LogResponseStream {
        LogResponseStream {
            entries: self.message.logs.into_iter(),
            done: false,
            allocator: self.allocator,
            check: self.check,
            charge: self.charge,
        }
    }
}

impl Stream for LogResponseStream {
    type Item = Result<OwnedResponse<LogEntry>, Status>;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.done {
            return Poll::Ready(None);
        }
        // Empty streams and the final trailers have no message to prepare.
        // Check their state too, before allowing Tonic to publish success.
        if let Some(check) = &this.check
            && let Err(status) = check()
        {
            this.done = true;
            this.entries = Vec::new().into_iter();
            return Poll::Ready(Some(Err(status)));
        }
        let item = this.entries.next().map(|entry| {
            let encoded_len = entry.checked_encoded_len().map_err(into_status)?;
            Ok(OwnedResponse {
                message: entry,
                encoded_len,
                allocator: Arc::clone(&this.allocator),
                check: this.check.clone(),
                charge: Arc::clone(&this.charge),
            })
        });
        if !matches!(item, Some(Ok(_))) {
            this.done = true;
        }
        Poll::Ready(item)
    }
}

#[cfg(test)]
mod tests;
