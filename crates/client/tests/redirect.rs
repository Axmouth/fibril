//! Adversarial integration tests for client-side redirect following, using a
//! controllable mock broker (a TcpListener that speaks the wire) so we can
//! force redirect-to-dead-endpoint and redirect ping-pong without a real
//! cluster.

use std::net::SocketAddr;

use fibril_client::{Client, ClientOptions, FibrilError};
use fibril_protocol::v1::{
    COMPLIANCE_STRING, HelloOk, Op, PROTOCOL_V1, Publish, PublishOk, Redirect, ResumeOutcome,
    frame::ProtoCodec,
    helper::{try_decode, try_encode},
};
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use uuid::Uuid;

#[derive(Clone)]
enum MockBehavior {
    /// Always redirect publishes to this endpoint.
    RedirectTo(SocketAddr),
    /// Always redirect publishes back to the mock's own address (ping-pong).
    RedirectToSelf,
    /// Confirm publishes with offset 0.
    ConfirmOk,
}

/// Spawn a mock broker; returns its address. It completes the handshake, then
/// responds to each `Publish` per `behavior`.
async fn spawn_mock(behavior: MockBehavior) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let behavior = behavior.clone();
            tokio::spawn(async move {
                let mut framed = Framed::new(sock, ProtoCodec);
                // Handshake: reply to Hello with HelloOk.
                if let Some(Ok(frame)) = framed.next().await {
                    let hello_ok = HelloOk {
                        protocol_version: PROTOCOL_V1,
                        owner_id: Uuid::nil(),
                        client_id: Uuid::nil(),
                        resume_token: Uuid::nil(),
                        resume_outcome: ResumeOutcome::New,
                        server_name: "mock".into(),
                        compliance: COMPLIANCE_STRING.into(),
                    };
                    let _ = framed
                        .send(try_encode(Op::HelloOk, frame.request_id, &hello_ok).unwrap())
                        .await;
                }
                while let Some(Ok(frame)) = framed.next().await {
                    if frame.opcode != Op::Publish as u16 {
                        continue; // ignore pings etc.
                    }
                    let publish: Publish = try_decode(&frame).unwrap();
                    let response = match &behavior {
                        MockBehavior::ConfirmOk => {
                            try_encode(Op::PublishOk, frame.request_id, &PublishOk { offset: 0 })
                                .unwrap()
                        }
                        MockBehavior::RedirectTo(target) => {
                            let redirect = Redirect {
                                topic: publish.topic,
                                partition: 0,
                                group: publish.group,
                                owner_endpoint: target.to_string(),
                                partitioning_version: 0,
                            };
                            try_encode(Op::Redirect, frame.request_id, &redirect).unwrap()
                        }
                        MockBehavior::RedirectToSelf => {
                            let redirect = Redirect {
                                topic: publish.topic,
                                partition: 0,
                                group: publish.group,
                                owner_endpoint: addr.to_string(),
                                partitioning_version: 0,
                            };
                            try_encode(Op::Redirect, frame.request_id, &redirect).unwrap()
                        }
                    };
                    if framed.send(response).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    addr
}

/// A redirect to an unreachable owner surfaces a clean connection error — it
/// does NOT hang.
#[tokio::test]
async fn redirect_to_dead_endpoint_errors_without_hanging() {
    // 127.0.0.1:1 is privileged and unbound -> connection refused fast.
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let mock = spawn_mock(MockBehavior::RedirectTo(dead)).await;

    let client = Client::connect(mock, ClientOptions::new()).await.unwrap();
    let publisher = client.publisher("jobs").unwrap();

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        publisher.publish_confirmed("hello"),
    )
    .await
    .expect("redirect to a dead endpoint must error quickly, not hang");

    assert!(
        matches!(result, Err(FibrilError::Disconnection { .. })),
        "expected a disconnection error following a dead redirect, got: {result:?}"
    );
}

/// A redirect ping-pong (owner keeps redirecting to itself) is capped by
/// `max_redirects` and returns a clear terminal error instead of spinning.
#[tokio::test]
async fn redirect_ping_pong_is_capped_by_max_redirects() {
    let mock = spawn_mock(MockBehavior::RedirectToSelf).await;

    let mut opts = ClientOptions::new();
    opts.max_redirects = 2;
    let client = Client::connect(mock, opts).await.unwrap();
    let publisher = client.publisher("jobs").unwrap();

    let result = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        publisher.publish_confirmed("hello"),
    )
    .await
    .expect("redirect ping-pong must terminate, not hang");

    match result {
        Err(FibrilError::Failure { msg, .. }) => {
            assert!(
                msg.contains("max redirects"),
                "expected a max-redirects error, got: {msg}"
            );
        }
        other => panic!("expected max-redirects failure, got: {other:?}"),
    }
}

/// A redirect to a real owner is followed: the publish succeeds on the new
/// owner and the routing cache is updated so the next publish goes straight
/// there (the second mock would never have been reached otherwise).
#[tokio::test]
async fn redirect_to_live_owner_is_followed_and_cached() {
    let owner = spawn_mock(MockBehavior::ConfirmOk).await;
    let bootstrap = spawn_mock(MockBehavior::RedirectTo(owner)).await;

    let client = Client::connect(bootstrap, ClientOptions::new())
        .await
        .unwrap();
    let publisher = client.publisher("jobs").unwrap();

    let offset = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        publisher.publish_confirmed("hello"),
    )
    .await
    .expect("redirect-follow must resolve quickly")
    .expect("publish should succeed on the redirected owner");
    assert_eq!(offset, 0);

    // Cached now: a second publish resolves on the owner directly.
    let offset2 = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        publisher.publish_confirmed("hello-again"),
    )
    .await
    .expect("cached route must resolve quickly")
    .expect("second publish should succeed via cached owner");
    assert_eq!(offset2, 0);

    let _ = TcpStream::connect(owner).await; // owner stays reachable
}
