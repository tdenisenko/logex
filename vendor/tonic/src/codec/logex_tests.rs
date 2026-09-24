// LogEx patch controls run on the repository-pinned compiler. Waker::noop is
// test-only; production additions retain the vendor's Rust 1.75 API baseline.
use super::*;
use crate::codec::EncodeBufferAllocator;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::task::Waker;

#[derive(Default)]
struct Counts {
    used: AtomicUsize,
    peak: AtomicUsize,
    limit: usize,
}
struct Guard {
    counts: Arc<Counts>,
    capacity: usize,
}
impl Drop for Guard {
    fn drop(&mut self) {
        self.counts.used.fetch_sub(self.capacity, Ordering::SeqCst);
    }
}
struct Allocator(Arc<Counts>);
impl EncodeBufferAllocator for Allocator {
    fn allocate(&self, capacity: usize) -> Result<OwnedEncodeBuffer, Status> {
        let used = self.0.used.load(Ordering::SeqCst);
        if used + capacity > self.0.limit {
            return Err(Status::resource_exhausted("test capacity"));
        }
        let buffer = BytesMut::with_capacity(capacity);
        let capacity = buffer.capacity();
        let now = self.0.used.fetch_add(capacity, Ordering::SeqCst) + capacity;
        self.0.peak.fetch_max(now, Ordering::SeqCst);
        Ok(OwnedEncodeBuffer::new(
            buffer,
            Guard {
                counts: self.0.clone(),
                capacity,
            },
        ))
    }
}
struct TestEncoder {
    allocator: Arc<dyn EncodeBufferAllocator>,
    prepared: bool,
    error: bool,
    length: usize,
    threshold: usize,
}
impl Encoder for TestEncoder {
    type Item = u8;
    type Error = Status;
    fn uses_prepared_buffers(&self) -> bool {
        self.prepared
    }
    fn prepare(&mut self, _: &u8) -> Result<PreparedEncode, Status> {
        Ok(PreparedEncode::new(self.length, self.allocator.clone()))
    }
    fn encode(&mut self, item: u8, dst: &mut EncodeBuf<'_>) -> Result<(), Status> {
        if self.error || item == 255 {
            return Err(Status::resource_exhausted("encoder error"));
        }
        dst.put_u8(item);
        Ok(())
    }
    fn buffer_settings(&self) -> BufferSettings {
        BufferSettings::new(8, self.threshold)
    }
}
fn make_encoder(limit: usize) -> (TestEncoder, Arc<Counts>) {
    let counts = Arc::new(Counts {
        limit,
        ..Counts::default()
    });
    (
        TestEncoder {
            allocator: Arc::new(Allocator(counts.clone())),
            prepared: true,
            error: false,
            length: 1,
            threshold: 32,
        },
        counts,
    )
}
fn poll<T: Stream + Unpin>(stream: &mut T) -> Poll<Option<T::Item>> {
    Pin::new(stream).poll_next(&mut Context::from_waker(Waker::noop()))
}
fn stream(
    encoder: TestEncoder,
    values: Vec<Result<u8, Status>>,
    maximum: Option<usize>,
) -> EncodedBytes<TestEncoder, tokio_stream::Iter<std::vec::IntoIter<Result<u8, Status>>>> {
    EncodedBytes::new(
        encoder,
        tokio_stream::iter(values),
        None,
        SingleMessageCompressionOverride::default(),
        maximum,
    )
}

#[test]
fn prepared_lazy_batched_aliases_and_replacement_overlap() {
    let (encoder, counts) = make_encoder(64);
    let mut stream = stream(encoder, vec![Ok(7), Ok(8)], None);
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
    let Poll::Ready(Some(Ok(bytes))) = poll(&mut stream) else {
        panic!("data")
    };
    assert_eq!(&bytes[..], &[0, 0, 0, 0, 1, 7, 0, 0, 0, 0, 1, 8]);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 24); // old8 + replacement16
    assert_eq!(counts.used.load(Ordering::SeqCst), 16);
    let alias = bytes.slice(0..1);
    drop(bytes);
    drop(stream);
    assert_eq!(counts.used.load(Ordering::SeqCst), 16);
    drop(alias);
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
}

#[test]
fn allocation_and_encoder_failures_are_terminal_and_release_backing() {
    for (limit, encoder_error, maximum) in [
        (0, false, None),
        (20, false, None),
        (64, true, None),
        (64, false, Some(0)),
    ] {
        let (mut encoder, counts) = make_encoder(limit);
        encoder.error = encoder_error;
        let mut stream = stream(encoder, vec![Ok(1), Ok(2)], maximum);
        let Poll::Ready(Some(Err(status))) = poll(&mut stream) else {
            panic!("error")
        };
        assert_eq!(
            status.code(),
            if maximum.is_some() {
                crate::Code::OutOfRange
            } else {
                crate::Code::ResourceExhausted
            }
        );
        assert_eq!(counts.used.load(Ordering::SeqCst), 0);
        assert!(matches!(poll(&mut stream), Poll::Ready(None)));
        assert!(matches!(poll(&mut stream), Poll::Ready(None)));
    }
}

#[test]
fn source_error_flushes_valid_batch_once() {
    let (encoder, counts) = make_encoder(64);
    let mut stream = stream(
        encoder,
        vec![Ok(1), Err(Status::aborted("stop")), Ok(2)],
        None,
    );
    let Poll::Ready(Some(Ok(bytes))) = poll(&mut stream) else {
        panic!("valid data")
    };
    assert_eq!(&bytes[..], &[0, 0, 0, 0, 1, 1]);
    assert!(matches!(poll(&mut stream), Poll::Ready(Some(Err(_)))));
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
    drop(bytes);
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
}

#[test]
fn body_terminal_after_server_trailers_and_client_error() {
    for client in [false, true] {
        let (mut encoder, _) = make_encoder(64);
        encoder.prepared = false;
        let values = tokio_stream::iter(vec![Err(Status::aborted("stop")), Ok(2)]);
        let mut body = if client {
            EncodeBody::new_client(encoder, values, None, None)
        } else {
            EncodeBody::new_server(
                encoder,
                values,
                None,
                SingleMessageCompressionOverride::default(),
                None,
            )
        };
        let mut cx = Context::from_waker(Waker::noop());
        let Poll::Ready(Some(frame)) = Pin::new(&mut body).poll_frame(&mut cx) else {
            panic!("terminal")
        };
        if client {
            assert!(frame.is_err());
        } else {
            assert!(frame.unwrap().is_trailers());
        }
        assert!(body.is_end_stream());
        assert!(matches!(
            Pin::new(&mut body).poll_frame(&mut cx),
            Poll::Ready(None)
        ));
    }
}

#[test]
fn default_encoding_and_length_mismatch() {
    let (mut encoder, _) = make_encoder(64);
    encoder.prepared = false;
    let mut legacy = stream(encoder, vec![Ok(3)], None);
    let Poll::Ready(Some(Ok(bytes))) = poll(&mut legacy) else {
        panic!("data")
    };
    assert_eq!(&bytes[..], &[0, 0, 0, 0, 1, 3]);
    let (mut encoder, counts) = make_encoder(64);
    encoder.length = 2;
    let mut mismatch = stream(encoder, vec![Ok(3)], None);
    let Poll::Ready(Some(Err(status))) = poll(&mut mismatch) else {
        panic!("length error")
    };
    assert_eq!(status.code(), crate::Code::Internal);
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
}

#[test]
fn prior_frame_alias_coexists_with_next_frame_and_unpolled_drop_is_lazy() {
    let (mut encoder, counts) = make_encoder(16);
    encoder.threshold = 1;
    let mut stream = stream(encoder, vec![Ok(1), Ok(2)], None);
    let Poll::Ready(Some(Ok(first))) = poll(&mut stream) else {
        panic!("first")
    };
    let Poll::Ready(Some(Ok(second))) = poll(&mut stream) else {
        panic!("second")
    };
    assert_eq!(counts.used.load(Ordering::SeqCst), 16);
    drop(stream);
    drop(first);
    assert_eq!(counts.used.load(Ordering::SeqCst), 8);
    drop(second);
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
    let (encoder, counts) = make_encoder(0);
    drop(self::stream(encoder, vec![Ok(1)], None));
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
}

#[test]
fn allocator_identity_and_oversize_are_rejected_before_new_allocation() {
    let (mut encoder, counts) = make_encoder(64);
    let mut owned = None;
    let mut identity = None;
    encode_prepared(
        &mut encoder,
        &mut owned,
        &mut identity,
        None,
        None,
        BufferSettings::new(8, 32),
        1,
    )
    .unwrap();
    let (other, other_counts) = make_encoder(64);
    encoder.allocator = other.allocator;
    let error = encode_prepared(
        &mut encoder,
        &mut owned,
        &mut identity,
        None,
        None,
        BufferSettings::new(8, 32),
        2,
    )
    .unwrap_err();
    assert_eq!(error.code(), crate::Code::Internal);
    assert_eq!(other_counts.used.load(Ordering::SeqCst), 0);
    drop(owned);
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
    let (mut encoder, counts) = make_encoder(64);
    encoder.length = usize::MAX;
    let mut stream = stream(encoder, vec![Ok(1)], None);
    assert!(matches!(poll(&mut stream), Poll::Ready(Some(Err(_)))));
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
}

#[test]
fn bounded_writes_cannot_grow_and_zero_remaining_chunk_is_empty() {
    let mut bytes = BytesMut::with_capacity(1);
    {
        let mut bounded = EncodeBuf::bounded(&mut bytes, 1);
        bounded.reserve(1);
        bounded.put_u8(7);
        assert_eq!(bounded.chunk_mut().len(), 0);
        assert_eq!(bounded.remaining_mut(), 0);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| bounded.put_u8(8)));
        assert!(result.is_err());
    }
    assert_eq!(bytes.len(), 1);
    assert_eq!(bytes.capacity(), 1);
}

#[test]
fn terminal_drops_source_without_waiting_for_body_drop() {
    struct Source(Arc<AtomicUsize>);
    impl Stream for Source {
        type Item = Result<u8, Status>;
        fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(Some(Err(Status::aborted("stop"))))
        }
    }
    impl Drop for Source {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let (encoder, _) = make_encoder(64);
    let mut stream = EncodedBytes::new(
        encoder,
        Source(dropped.clone()),
        None,
        SingleMessageCompressionOverride::default(),
        None,
    );
    assert!(matches!(poll(&mut stream), Poll::Ready(Some(Err(_)))));
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
}

#[cfg(feature = "gzip")]
#[test]
fn prepared_compression_rejected_without_allocation() {
    let (encoder, counts) = make_encoder(64);
    let mut stream = EncodedBytes::new(
        encoder,
        tokio_stream::iter(vec![Ok(1)]),
        Some(CompressionEncoding::Gzip),
        SingleMessageCompressionOverride::default(),
        None,
    );
    let Poll::Ready(Some(Err(error))) = poll(&mut stream) else {
        panic!("compression error")
    };
    assert_eq!(error.code(), crate::Code::Unimplemented);
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
}

#[test]
fn default_failures_discard_initialized_partial_batch() {
    for (values, maximum) in [
        (vec![Ok(255)], None),
        (vec![Ok(1), Ok(255)], None),
        (vec![Ok(1)], Some(0)),
    ] {
        let (mut encoder, _) = make_encoder(64);
        encoder.prepared = false;
        let mut stream = stream(encoder, values, maximum);
        let Poll::Ready(Some(Err(error))) = poll(&mut stream) else {
            panic!("failure")
        };
        assert_eq!(
            error.code(),
            if maximum.is_some() {
                crate::Code::OutOfRange
            } else {
                crate::Code::Internal
            }
        );
        assert!(stream.buf.is_empty());
        assert_eq!(stream.buf.capacity(), 0);
        assert!(matches!(poll(&mut stream), Poll::Ready(None)));
    }
    let (encoder, counts) = make_encoder(64);
    let mut stream = stream(encoder, vec![Ok(1), Ok(255)], None);
    assert!(matches!(poll(&mut stream), Poll::Ready(Some(Err(_)))));
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
}

#[test]
fn pending_flushes_ready_message_then_waits_without_allocating() {
    struct Pausing(bool);
    impl Stream for Pausing {
        type Item = Result<u8, Status>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if std::mem::replace(&mut self.0, false) {
                Poll::Ready(Some(Ok(1)))
            } else {
                Poll::Pending
            }
        }
    }
    let (encoder, counts) = make_encoder(64);
    let mut stream = EncodedBytes::new(
        encoder,
        Pausing(true),
        None,
        SingleMessageCompressionOverride::default(),
        None,
    );
    let Poll::Ready(Some(Ok(bytes))) = poll(&mut stream) else {
        panic!("pending flush")
    };
    assert_eq!(&bytes[..], &[0, 0, 0, 0, 1, 1]);
    assert!(matches!(poll(&mut stream), Poll::Pending));
    assert_eq!(counts.used.load(Ordering::SeqCst), 8);
    drop(stream);
    drop(bytes);
    assert_eq!(counts.used.load(Ordering::SeqCst), 0);
}

#[test]
fn prepare_failure_drops_current_and_pending_messages() {
    struct Message(Arc<AtomicUsize>);
    impl Drop for Message {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Failing;
    impl Encoder for Failing {
        type Item = Message;
        type Error = Status;
        fn uses_prepared_buffers(&self) -> bool {
            true
        }
        fn prepare(&mut self, _: &Message) -> Result<PreparedEncode, Status> {
            Err(Status::permission_denied("prepare refused"))
        }
        fn encode(&mut self, _: Message, _: &mut EncodeBuf<'_>) -> Result<(), Status> {
            panic!("must not encode")
        }
    }
    let dropped = Arc::new(AtomicUsize::new(0));
    let source = tokio_stream::iter(vec![
        Ok(Message(dropped.clone())),
        Ok(Message(dropped.clone())),
    ]);
    let mut stream = EncodedBytes::new(
        Failing,
        source,
        None,
        SingleMessageCompressionOverride::default(),
        None,
    );
    let Poll::Ready(Some(Err(error))) = poll(&mut stream) else {
        panic!("prepare failure")
    };
    assert_eq!(error.code(), crate::Code::PermissionDenied);
    assert_eq!(dropped.load(Ordering::SeqCst), 2);
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
}

#[test]
fn invalid_allocator_is_rejected_and_excess_capacity_follows_alias() {
    struct Fake {
        counts: Arc<Counts>,
        capacity: usize,
        nonempty: bool,
    }
    impl EncodeBufferAllocator for Fake {
        fn allocate(&self, _: usize) -> Result<OwnedEncodeBuffer, Status> {
            let mut buffer = BytesMut::with_capacity(self.capacity);
            if self.nonempty {
                buffer.put_u8(9);
            }
            let capacity = buffer.capacity();
            self.counts.used.fetch_add(capacity, Ordering::SeqCst);
            Ok(OwnedEncodeBuffer::new(
                buffer,
                Guard {
                    counts: self.counts.clone(),
                    capacity,
                },
            ))
        }
    }
    for (capacity, nonempty, succeeds) in [(1, false, false), (8, true, false), (24, false, true)] {
        let (mut encoder, counts) = make_encoder(64);
        encoder.allocator = Arc::new(Fake {
            counts: counts.clone(),
            capacity,
            nonempty,
        });
        let mut stream = stream(encoder, vec![Ok(1)], None);
        match poll(&mut stream) {
            Poll::Ready(Some(Ok(bytes))) if succeeds => {
                assert_eq!(counts.used.load(Ordering::SeqCst), 24);
                let alias = bytes.slice(1..2);
                drop(bytes);
                drop(stream);
                assert_eq!(counts.used.load(Ordering::SeqCst), 24);
                drop(alias);
            }
            Poll::Ready(Some(Err(error))) if !succeeds => {
                assert_eq!(error.code(), crate::Code::Internal);
            }
            _ => panic!("unexpected allocator result"),
        }
        assert_eq!(counts.used.load(Ordering::SeqCst), 0);
    }
}
