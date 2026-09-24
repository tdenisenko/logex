//! Small, offline controls for the pinned transport's response lifecycle.
use std::{future::poll_fn, pin::Pin};

use http_body::Body;
use tonic::{
    Status,
    codec::{Codec, EncodeBody, ProstCodec},
};

#[derive(Clone, PartialEq, prost::Message)]
struct TinyMessage {
    #[prost(uint64, tag = "1")]
    value: u64,
}

#[tokio::test]
async fn terminal_grpc_error_permanently_ends_the_body() {
    let encoder = ProstCodec::<TinyMessage, TinyMessage>::default().encoder();
    let source = tokio_stream::iter([
        Err(Status::resource_exhausted("fixture capacity")),
        Ok(TinyMessage { value: 1 }),
    ]);
    let mut body = EncodeBody::new_server(encoder, source, None, Default::default(), None);
    let frame = poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.trailers_ref().unwrap()["grpc-status"], "8");
    assert!(body.is_end_stream());
    for _ in 0..2 {
        assert!(
            poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
                .await
                .is_none()
        );
    }
}
