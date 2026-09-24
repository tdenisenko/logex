//! Keep admission occupied while response bodies or emitted frames remain owned.
//!
//! This tracks query lifetime, not the number of allocated bytes.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::http::{Request, Response};
use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use tower::Service;

use crate::handler::QueryLease;

struct LeasedBytes {
    bytes: Bytes,
    _lease: QueryLease,
}

impl AsRef<[u8]> for LeasedBytes {
    fn as_ref(&self) -> &[u8] {
        self.bytes.as_ref()
    }
}

struct QueryResponseBody<B> {
    inner: Pin<Box<B>>,
    lease: QueryLease,
}

impl<B> QueryResponseBody<B> {
    fn new(inner: B, lease: QueryLease) -> Self {
        Self {
            inner: Box::pin(inner),
            lease,
        }
    }
}

impl<B: Body<Data = Bytes>> Body for QueryResponseBody<B> {
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        this.inner.as_mut().poll_frame(cx).map(|frame| {
            frame.map(|result| {
                result.map(|frame| {
                    frame.map_data(|bytes| {
                        Bytes::from_owner(LeasedBytes {
                            bytes,
                            _lease: this.lease.clone(),
                        })
                    })
                })
            })
        })
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

pub(crate) fn retain_query_lease(
    response: axum::response::Response,
    lease: QueryLease,
) -> axum::response::Response {
    response.map(|body| axum::body::Body::new(QueryResponseBody::new(body, lease)))
}

/// Wrap a generated gRPC service so its query leases follow encoded response data.
/// Construct through [`crate::grpc::grpc_service`] for the LogEx service.
#[derive(Clone)]
pub struct QueryLeaseService<S> {
    inner: S,
}

impl<S> QueryLeaseService<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for QueryLeaseService<S> {
    const NAME: &'static str = S::NAME;
}

impl<S, B> Service<Request<B>> for QueryLeaseService<S>
where
    S: Service<Request<B>, Response = Response<tonic::body::Body>>,
    S::Future: Send + 'static,
    S::Error: 'static,
{
    type Response = Response<tonic::body::Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let future = self.inner.call(request);
        Box::pin(async move {
            let mut response = future.await?;
            // Tonic preserves extensions while turning messages into its lazy
            // encoding body. Move ownership before that body can be polled.
            let Some(lease) = response.extensions_mut().remove::<QueryLease>() else {
                return Ok(response);
            };
            Ok(response.map(|body| tonic::body::Body::new(QueryResponseBody::new(body, lease))))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handler::{QueryConcurrencyLimit, QueryControl};
    use std::sync::Arc;

    #[tokio::test]
    async fn http_body_and_frame_clones_retain_admission() {
        let control = Arc::new(QueryControl::new(QueryConcurrencyLimit::new(1).unwrap()));
        let query = control.start_concurrent().unwrap();
        let response = axum::response::Response::new(axum::body::Body::from("response"));
        let response = retain_query_lease(response, query.lease());
        drop(query);
        assert!(control.start_concurrent().is_err());
        let mut body = response.into_body();
        let frame = std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx))
            .await
            .unwrap()
            .unwrap();
        let bytes = frame.into_data().unwrap();
        let clone = bytes.clone();
        let slice = bytes.slice(1..3);
        drop(body);
        drop(bytes);
        assert!(control.start_concurrent().is_err());
        drop(clone);
        assert!(control.start_concurrent().is_err());
        drop(slice);
        assert!(control.start_concurrent().is_ok());
    }

    #[test]
    fn unpolled_http_body_releases_admission_on_drop() {
        let control = Arc::new(QueryControl::new(QueryConcurrencyLimit::new(1).unwrap()));
        let query = control.start_concurrent().unwrap();
        let response = retain_query_lease(
            axum::response::Response::new(axum::body::Body::from("response")),
            query.lease(),
        );
        drop(query);
        assert!(control.start_concurrent().is_err());
        drop(response);
        assert!(control.start_concurrent().is_ok());
    }
}
