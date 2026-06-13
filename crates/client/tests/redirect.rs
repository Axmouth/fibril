//! Adversarial integration tests for client-side redirect following, using a
//! controllable mock broker (a TcpListener that speaks the wire) so we can
//! force redirect-to-dead-endpoint and redirect ping-pong without a real
//! cluster.

use std::net::SocketAddr;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fibril_client::{Client, ClientOptions, FibrilError, ReconnectOutcome};
use fibril_protocol::v1::{
    COMPLIANCE_STRING, Hello, HelloOk, Op, PROTOCOL_V1, Publish, PublishOk, QueueTopologyEntry,
    ReconcileResult, Redirect, ResumeOutcome, TopologyOk,
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

#[derive(Clone, Default)]
struct MockConfig {
    publish: Option<MockBehavior>,
    /// If set, answer `Op::Topology` with this.
    topology: Option<TopologyOk>,
    /// Counts handshakes that carried a resume identity.
    resumes: Option<Arc<AtomicUsize>>,
}

async fn spawn_mock(behavior: MockBehavior) -> SocketAddr {
    spawn_configurable_mock(MockConfig {
        publish: Some(behavior),
        ..Default::default()
    })
    .await
}

/// Spawn a mock broker; returns its address. Completes the handshake (reporting
/// `Resumed` when the client presents a resume identity), answers `Op::Topology`
/// when configured, and responds to each `Publish` per the publish behavior.
async fn spawn_configurable_mock(config: MockConfig) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            let config = config.clone();
            tokio::spawn(async move {
                let mut framed = Framed::new(sock, ProtoCodec);
                // Handshake: report Resumed iff the client presented an identity.
                if let Some(Ok(frame)) = framed.next().await {
                    let resumed = try_decode::<Hello>(&frame)
                        .map(|hello| hello.resume.is_some())
                        .unwrap_or(false);
                    if resumed {
                        if let Some(counter) = &config.resumes {
                            counter.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                    let hello_ok = HelloOk {
                        protocol_version: PROTOCOL_V1,
                        owner_id: Uuid::nil(),
                        client_id: Uuid::nil(),
                        resume_token: Uuid::nil(),
                        resume_outcome: if resumed {
                            ResumeOutcome::Resumed
                        } else {
                            ResumeOutcome::New
                        },
                        server_name: "mock".into(),
                        compliance: COMPLIANCE_STRING.into(),
                    };
                    let _ = framed
                        .send(try_encode(Op::HelloOk, frame.request_id, &hello_ok).unwrap())
                        .await;
                }
                while let Some(Ok(frame)) = framed.next().await {
                    let response = if frame.opcode == Op::ReconcileClient as u16 {
                        // Resumed handshakes reconcile subscriptions; answer with
                        // an empty result so the client finishes connecting.
                        try_encode(
                            Op::ReconcileResult,
                            frame.request_id,
                            &ReconcileResult {
                                subscriptions: vec![],
                            },
                        )
                        .unwrap()
                    } else if frame.opcode == Op::Topology as u16 {
                        match &config.topology {
                            Some(topology) => {
                                try_encode(Op::TopologyOk, frame.request_id, topology).unwrap()
                            }
                            None => continue,
                        }
                    } else if frame.opcode == Op::Publish as u16 {
                        let publish: Publish = try_decode(&frame).unwrap();
                        match &config.publish {
                            None => continue,
                            Some(MockBehavior::ConfirmOk) => try_encode(
                                Op::PublishOk,
                                frame.request_id,
                                &PublishOk { offset: 0 },
                            )
                            .unwrap(),
                            Some(MockBehavior::RedirectTo(target)) => {
                                let redirect = Redirect {
                                    topic: publish.topic,
                                    partition: 0,
                                    group: publish.group,
                                    owner_endpoint: target.to_string(),
                                    partitioning_version: 0,
                                };
                                try_encode(Op::Redirect, frame.request_id, &redirect).unwrap()
                            }
                            Some(MockBehavior::RedirectToSelf) => {
                                let redirect = Redirect {
                                    topic: publish.topic,
                                    partition: 0,
                                    group: publish.group,
                                    owner_endpoint: addr.to_string(),
                                    partitioning_version: 0,
                                };
                                try_encode(Op::Redirect, frame.request_id, &redirect).unwrap()
                            }
                        }
                    } else {
                        continue; // ignore pings etc.
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

/// `fetch_topology` populates the routing cache from the broker's answer, and
/// subsequent publishes route to the fetched owner. The bootstrap mock ignores
/// publishes, so a successful publish proves routing used the fetched owner.
#[tokio::test]
async fn fetch_topology_populates_cache_and_routes() {
    let owner = spawn_mock(MockBehavior::ConfirmOk).await;
    let bootstrap = spawn_configurable_mock(MockConfig {
        publish: None, // bootstrap never serves publishes
        topology: Some(TopologyOk {
            generation: 1,
            queues: vec![QueueTopologyEntry {
                topic: "jobs".into(),
                partition: 0,
                group: None,
                owner_endpoint: Some(owner.to_string()),
                partitioning_version: 0,
            }],
        }),
        ..Default::default()
    })
    .await;

    let client = Client::connect(bootstrap, ClientOptions::new())
        .await
        .unwrap();

    let topology = client.fetch_topology().await.unwrap();
    assert_eq!(topology.generation, 1);
    assert_eq!(topology.queues.len(), 1);
    assert_eq!(topology.queues[0].topic, "jobs");

    // Routes to the fetched owner; the bootstrap mock would hang on a publish.
    let publisher = client.publisher("jobs").unwrap();
    let offset = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        publisher.publish_confirmed("hello"),
    )
    .await
    .expect("publish must route to the fetched owner, not the bootstrap")
    .expect("publish should succeed on the fetched owner");
    assert_eq!(offset, 0);
}

/// Reconnect presents the prior resume identity, and the broker reports it as a
/// resumed connection.
#[tokio::test]
async fn reconnect_presents_resume_identity() {
    let resumes = Arc::new(AtomicUsize::new(0));
    let mock = spawn_configurable_mock(MockConfig {
        resumes: Some(resumes.clone()),
        ..Default::default()
    })
    .await;

    let mut client = Client::connect(mock, ClientOptions::new()).await.unwrap();
    let outcome: ReconnectOutcome =
        tokio::time::timeout(std::time::Duration::from_secs(5), client.reconnect())
            .await
            .expect("reconnect must not hang")
            .expect("reconnect should succeed");

    assert!(
        matches!(outcome.resume_outcome, ResumeOutcome::Resumed),
        "broker should report a resumed connection, got {:?}",
        outcome.resume_outcome
    );
    assert!(
        resumes.load(Ordering::SeqCst) >= 1,
        "broker should have seen a resume identity on reconnect"
    );
}
