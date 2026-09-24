//! Ordinary interoperability coverage for the pinned TLS/QUIC dependency stack.

use std::{future::Future, time::Duration};

use futures::{AsyncReadExt, AsyncWriteExt, StreamExt, future::poll_fn};
use libp2p::{
    PeerId, Transport,
    core::{
        Endpoint,
        muxing::StreamMuxerExt,
        transport::{DialOpts, ListenerId, PortUse, TransportEvent},
    },
    identity, quic,
};

#[tokio::test]
async fn quic_authenticates_local_peers_and_exchanges_data() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let listener_key = identity::Keypair::generate_ed25519();
        let dialer_key = identity::Keypair::generate_ed25519();
        let listener_id = listener_key.public().to_peer_id();
        let dialer_id = dialer_key.public().to_peer_id();
        let mut listener = quic::tokio::Transport::new(quic::Config::new(&listener_key)).boxed();
        let mut dialer = quic::tokio::Transport::new(quic::Config::new(&dialer_key)).boxed();
        listener
            .listen_on(
                ListenerId::next(),
                "/ip4/127.0.0.1/udp/0/quic-v1".parse().unwrap(),
            )
            .unwrap();
        let address = listener
            .next()
            .await
            .expect("listener must remain open")
            .into_new_address()
            .expect("listener must report its assigned loopback port")
            .with(libp2p::multiaddr::Protocol::P2p(listener_id));
        let dial = dialer
            .dial(
                address,
                DialOpts {
                    role: Endpoint::Dialer,
                    port_use: PortUse::Reuse,
                },
            )
            .unwrap();

        let accept = async {
            let (upgrade, _) = listener
                .next()
                .await
                .expect("listener must accept the local dial")
                .into_incoming()
                .expect("expected incoming QUIC connection");
            drive_transport(&mut listener, async {
                let (peer, mut connection) = upgrade.await.unwrap();
                assert_eq!(peer, dialer_id);
                let mut stream = poll_fn(|cx| {
                    let _ = connection.poll_unpin(cx)?;
                    connection.poll_inbound_unpin(cx)
                })
                .await
                .unwrap();
                let mut request = [0; 4];
                stream.read_exact(&mut request).await.unwrap();
                assert_eq!(&request, b"ping");
                stream.write_all(b"pong").await.unwrap();
                stream.close().await.unwrap();
                // Keep the authenticated connection alive until the peer consumes
                // the reply and acknowledges it on the same bidirectional stream.
                let mut ack = [0; 2];
                stream.read_exact(&mut ack).await.unwrap();
                assert_eq!(&ack, b"ok");
            })
            .await;
        };
        let connect = drive_transport(&mut dialer, async {
            let (peer, mut connection) = dial.await.unwrap();
            assert_eq!(peer, listener_id);
            let mut stream = poll_fn(|cx| {
                let _ = connection.poll_unpin(cx)?;
                connection.poll_outbound_unpin(cx)
            })
            .await
            .unwrap();
            // Writing first makes the new QUIC stream visible to the listener.
            stream.write_all(b"ping").await.unwrap();
            stream.flush().await.unwrap();
            let mut reply = [0; 4];
            stream.read_exact(&mut reply).await.unwrap();
            assert_eq!(&reply, b"pong");
            stream.write_all(b"ok").await.unwrap();
            stream.close().await.unwrap();
            // join! retains this endpoint until the listener receives the ack.
            connection
        });
        let ((), _dialer_connection) = futures::join!(accept, connect);
    })
    .await
    .expect("local QUIC handshake and exchange must finish within ten seconds");
}

// Poll both transport ownership and connection work without detached tasks.
// Dropping the outer timeout drops both endpoints and all in-flight work.
async fn drive_transport<F: Future>(
    transport: &mut libp2p::core::transport::Boxed<(PeerId, quic::Connection)>,
    work: F,
) -> F::Output {
    futures::pin_mut!(work);
    poll_fn(|cx| {
        if let std::task::Poll::Ready(event) = transport.poll_next_unpin(cx) {
            match event {
                Some(TransportEvent::NewAddress { .. }) => {}
                other => panic!("unexpected transport event during exchange: {other:?}"),
            }
        }
        work.as_mut().poll(cx)
    })
    .await
}
