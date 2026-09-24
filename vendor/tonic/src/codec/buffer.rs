use bytes::buf::UninitSlice;
use bytes::{Buf, BufMut, Bytes, BytesMut};

/// A specialized buffer to decode gRPC messages from.
#[derive(Debug)]
pub struct DecodeBuf<'a> {
    buf: &'a mut BytesMut,
    len: usize,
}

/// A specialized buffer to encode gRPC messages into.
#[derive(Debug)]
pub struct EncodeBuf<'a> {
    buf: &'a mut BytesMut,
    end: Option<usize>,
}

impl<'a> DecodeBuf<'a> {
    pub(crate) fn new(buf: &'a mut BytesMut, len: usize) -> Self {
        DecodeBuf { buf, len }
    }
}

impl Buf for DecodeBuf<'_> {
    #[inline]
    fn remaining(&self) -> usize {
        self.len
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        let ret = self.buf.chunk();

        if ret.len() > self.len {
            &ret[..self.len]
        } else {
            ret
        }
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        assert!(cnt <= self.len);
        self.buf.advance(cnt);
        self.len -= cnt;
    }

    #[inline]
    fn copy_to_bytes(&mut self, len: usize) -> Bytes {
        assert!(len <= self.len);
        self.len -= len;
        self.buf.copy_to_bytes(len)
    }
}

impl<'a> EncodeBuf<'a> {
    pub(crate) fn new(buf: &'a mut BytesMut) -> Self {
        EncodeBuf { buf, end: None }
    }
}

impl<'a> EncodeBuf<'a> {
    pub(crate) fn bounded(buf: &'a mut BytesMut, length: usize) -> Self {
        let end = buf
            .len()
            .checked_add(length)
            .expect("prepared length checked");
        assert!(end <= buf.capacity());
        Self {
            buf,
            end: Some(end),
        }
    }
    fn check_write(&self, len: usize) {
        if let Some(end) = self.end {
            assert!(
                len <= end - self.buf.len(),
                "encoder exceeded prepared length"
            );
        }
    }

    /// Reserves capacity for at least `additional` more bytes to be inserted
    /// into the buffer.
    ///
    /// More than `additional` bytes may be reserved in order to avoid frequent
    /// reallocations. A call to `reserve` may result in an allocation.
    #[inline]
    pub fn reserve(&mut self, additional: usize) {
        self.check_write(additional);
        if self.end.is_none() {
            self.buf.reserve(additional);
        }
    }
}

// SAFETY: writable chunks come directly from BytesMut and are bounded to the
// admitted span. Every cursor advance delegates the caller's initialization
// obligation to BytesMut, after checking the same bound. No bounded operation
// can grow the backing allocation.
unsafe impl BufMut for EncodeBuf<'_> {
    #[inline]
    fn remaining_mut(&self) -> usize {
        self.end
            .map_or_else(|| self.buf.remaining_mut(), |end| end - self.buf.len())
    }

    #[inline]
    unsafe fn advance_mut(&mut self, cnt: usize) {
        self.check_write(cnt);
        // SAFETY: the caller initialized these bytes under BufMut::advance_mut
        // and check_write above proves they lie inside the prepared span.
        unsafe { self.buf.advance_mut(cnt) }
    }

    #[inline]
    fn chunk_mut(&mut self) -> &mut UninitSlice {
        let remaining = self.remaining_mut();
        if remaining == 0 {
            return UninitSlice::new(&mut []);
        }
        let chunk = self.buf.chunk_mut();
        let take = remaining.min(chunk.len());
        &mut chunk[..take]
    }

    #[inline]
    fn put<T: Buf>(&mut self, src: T)
    where
        Self: Sized,
    {
        self.check_write(src.remaining());
        self.buf.put(src)
    }

    #[inline]
    fn put_slice(&mut self, src: &[u8]) {
        self.check_write(src.len());
        self.buf.put_slice(src)
    }

    #[inline]
    fn put_bytes(&mut self, val: u8, cnt: usize) {
        self.check_write(cnt);
        self.buf.put_bytes(val, cnt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_buf() {
        let mut payload = BytesMut::with_capacity(100);
        payload.put(&vec![0u8; 50][..]);
        let mut buf = DecodeBuf::new(&mut payload, 20);

        assert_eq!(buf.len, 20);
        assert_eq!(buf.remaining(), 20);
        assert_eq!(buf.chunk().len(), 20);

        buf.advance(10);
        assert_eq!(buf.remaining(), 10);

        let mut out = [0; 5];
        buf.copy_to_slice(&mut out);
        assert_eq!(buf.remaining(), 5);
        assert_eq!(buf.chunk().len(), 5);

        assert_eq!(buf.copy_to_bytes(5).len(), 5);
        assert!(!buf.has_remaining());
    }

    #[test]
    fn encode_buf() {
        let mut bytes = BytesMut::with_capacity(100);
        let mut buf = EncodeBuf::new(&mut bytes);

        let initial = buf.remaining_mut();
        buf.put_bytes(0, 20);
        assert_eq!(buf.remaining_mut(), initial - 20);

        buf.put_u8(b'a');
        assert_eq!(buf.remaining_mut(), initial - 20 - 1);
    }
}
