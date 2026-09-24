use crate::Status;
use bytes::{Bytes, BytesMut};
use std::{fmt, sync::Arc};

/// Allocates an admitted encoding buffer and its inseparable lifetime guard.
/// Implementations must admit capacity before allocation and reconcile actual
/// capacity before returning. Existing buffers remain live during replacement.
pub trait EncodeBufferAllocator: Send + Sync {
    /// Allocate an empty buffer with at least the requested capacity.
    fn allocate(&self, capacity: usize) -> Result<OwnedEncodeBuffer, Status>;
}

/// A buffer whose guard is released only after its backing allocation.
pub struct OwnedEncodeBuffer {
    pub(super) buffer: BytesMut,
    _guard: Box<dyn Send + Sync>,
}
impl OwnedEncodeBuffer {
    /// Couple backing to an allocation guard. The allocator must account its
    /// actual capacity, not merely the initialized length. Supply a unique,
    /// unsplit backing allocation: an external alias or a shared spare tail
    /// cannot outlive this owner unless it also retains the same charge.
    pub fn new(buffer: BytesMut, guard: impl Send + Sync + 'static) -> Self {
        Self {
            buffer,
            _guard: Box::new(guard),
        }
    }
    /// Allocated capacity.
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }
    /// Initialized length.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }
    /// Whether no bytes have been initialized.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
    pub(super) fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
    }
}
impl AsRef<[u8]> for OwnedEncodeBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.buffer
    }
}
impl fmt::Debug for OwnedEncodeBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedEncodeBuffer")
            .field("len", &self.len())
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

/// Exact payload size and allocator for one prepared message.
#[derive(Clone)]
pub struct PreparedEncode {
    pub(super) payload_len: usize,
    pub(super) allocator: Arc<dyn EncodeBufferAllocator>,
}
impl PreparedEncode {
    /// Prepare an exact unframed payload length with its allocator.
    pub fn new(payload_len: usize, allocator: Arc<dyn EncodeBufferAllocator>) -> Self {
        Self {
            payload_len,
            allocator,
        }
    }
}
impl fmt::Debug for PreparedEncode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedEncode")
            .field("payload_len", &self.payload_len)
            .finish_non_exhaustive()
    }
}
