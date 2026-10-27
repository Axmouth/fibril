//! Adversarial integration tests for client-side redirect following, using a
//! controllable mock broker (a TcpListener that speaks the wire) so we can
//! force redirect-to-dead-endpoint and redirect ping-pong without a real
//! cluster.

use std::collections::HashMap;
use std::net::SocketAddr;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fibril_client::{Client, ClientOptions, FibrilError, NewMessage, ReconnectOutcome};
use fibril_wire::{
    AdvertisedAddress, COMPLIANCE_STRING, Deliver, DeliveryTag, Hello, HelloOk, Op, PROTOCOL_V1,
    Partition, Publish, PublishOk, QueueTopologyEntry, ReconcileResult, Redirect, ResumeOutcome,
    Subscribe, SubscribeOk, TopologyOk,
    frame::ProtoCodec,
    helper::{try_decode, try_encode},
    wire,
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
    /// Answer `ERR_UNAVAILABLE` (owner recovering) while the counter is above
    /// zero, counting it down, then confirm.
    UnavailableThen(Arc<AtomicUsize>),
    /// Answer every publish with `ERR_GATEWAY_TIMEOUT`, counting them.
    GatewayTimeout(Arc<AtomicUsize>),
}

/// A self-owned multi-partition spread for one queue identity `(topic, group)`.
#[derive(Clone)]
struct SelfPartitions {
    topic: String,
    group: Option<String>,
    partition_count: u32,
    partitioning_version: u64,
    /// If set, the topology answer reports this live count instead of the fixed
    /// `partition_count`, so a test can simulate a live grow.
    live_partition_count: Option<Arc<std::sync::atomic::AtomicU32>>,
}

#[derive(Clone, Default)]
struct MockConfig {
    stop: Option<Arc<tokio::sync::Notify>>,
    reject_subscribes: Option<Arc<AtomicUsize>>,
    /// Code and opcode for rejected subscribes. Defaults to a 503 `Op::Error`.
    reject_with: Option<(u16, Op)>,
    publish: Option<MockBehavior>,
    /// If set, answer `Op::Topology` with this.
    topology: Option<TopologyOk>,
    /// If set, answer `Op::Topology` with a self-owned spread for the queue
    /// `(topic, group)`: one entry per partition (`0..partition_count`), all
    /// owned by this mock's own address. Group is part of the queue identity, so
    /// it is carried through. Takes precedence over `topology`.
    self_partitions: Option<SelfPartitions>,
    /// Records the `partition` field of every `Publish` frame received.
    recorded_partitions: Option<Arc<std::sync::Mutex<Vec<u32>>>>,
    /// Records the `partitioning_version` of every `Publish` frame received.
    recorded_versions: Option<Arc<std::sync::Mutex<Vec<u64>>>>,
    /// Records the `partition` of every `Subscribe` frame received.
    subscribe_partitions: Option<Arc<std::sync::Mutex<Vec<u32>>>>,
    /// Counts handshakes that carried a resume identity.
    resumes: Option<Arc<AtomicUsize>>,
    /// Once the flag is set, answer `Op::Topology` with this view instead,
    /// like a broker that restarted and still serves an older snapshot.
    stale_topology: Option<(Arc<std::sync::atomic::AtomicBool>, TopologyOk)>,
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
            let accepted = tokio::select! {
                accepted = listener.accept() => accepted,
                _ = async { match &config.stop { Some(stop) => stop.notified().await, None => std::future::pending().await } } => return,
            };
            let Ok((sock, _)) = accepted else {
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
                loop {
                    let next = tokio::select! {
                        next = framed.next() => next,
                        _ = async { match &config.stop { Some(stop) => stop.notified().await, None => std::future::pending().await } } => return,
                    };
                    let Some(Ok(frame)) = next else { return; };
                    // Subscribe is answered with SubscribeOk plus one tagged
                    // Deliver per partition (payload = partition), so a fan-in
                    // subscription receives a message from each partition.
                    if frame.opcode == Op::Subscribe as u16 {
                        let sub: Subscribe = try_decode(&frame).unwrap();
                        if config.reject_subscribes.as_ref().is_some_and(|remaining| {
                            remaining.try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1)).is_ok()
                        }) {
                            let (code, op) = config.reject_with.unwrap_or((503, Op::Error));
                            let reply = fibril_wire::ErrorMsg { code, message: "rejected".into() };
                            framed.send(try_encode(op, frame.request_id, &reply).unwrap()).await.unwrap();
                            continue;
                        }
                        if let Some(recorder) = &config.subscribe_partitions {
                            if let Ok(mut parts) = recorder.lock() {
                                parts.push(sub.partition.id());
                            }
                        }
                        let sub_id = sub.partition.id() as u64;
                        let ok = SubscribeOk {
                            sub_id,
                            topic: sub.topic.clone(),
                            group: sub.group.clone(),
                            partition: sub.partition,
                            prefetch: sub.prefetch,
                            consumer_group: sub.consumer_group.clone(),
                            consumer_target: sub.consumer_target,
                            member_id: None,
                        };
                        if framed
                            .send(try_encode(Op::SubscribeOk, frame.request_id, &ok).unwrap())
                            .await
                            .is_err()
                        {
                            return;
                        }
                        let deliver = Deliver {
                            sub_id,
                            topic: sub.topic,
                            group: sub.group,
                            partition: sub.partition,
                            offset: 0,
                            // Arbitrary placeholder: deliveries correlate by
                            // sub_id, and this test never acks (the mock ignores
                            // acks), so the tag value is irrelevant here.
                            delivery_tag: DeliveryTag { epoch: 0 },
                            published: 0,
                            publish_received: 0,
                            content_type: None,
                            headers: HashMap::new(),
                            payload: vec![sub.partition.id() as u8],
                        };
                        if framed
                            .send(wire::encode_deliver(0, &deliver).unwrap())
                            .await
                            .is_err()
                        {
                            return;
                        }
                        continue;
                    }
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
                        if let Some((stale, topology)) = &config.stale_topology
                            && stale.load(Ordering::SeqCst)
                        {
                            try_encode(Op::TopologyOk, frame.request_id, topology).unwrap()
                        } else if let Some(spread) = &config.self_partitions {
                            let count = spread
                                .live_partition_count
                                .as_ref()
                                .map(|c| c.load(Ordering::SeqCst))
                                .unwrap_or(spread.partition_count);
                            let queues = (0..count)
                                .map(|partition| QueueTopologyEntry {
                                    topic: spread.topic.clone(),
                                    partition: Partition::new(partition),
                                    group: spread.group.clone(),
                                    owner_endpoints: vec![
                                        AdvertisedAddress::parse(&addr.to_string())
                                            .expect("valid test owner endpoint"),
                                    ],
                                    partitioning_version: spread.partitioning_version,
                                    partition_count: count,
                                })
                                .collect();
                            let topology = TopologyOk {
                                generation: 1,
                                queues,
                                streams: vec![],
                            };
                            try_encode(Op::TopologyOk, frame.request_id, &topology).unwrap()
                        } else {
                            // Answer an empty topology by default so the client's
                            // connect-time warm completes promptly instead of
                            // waiting out its timeout.
                            let mut topology = config.topology.clone().unwrap_or(TopologyOk {
                                generation: 0,
                                queues: vec![],
                                streams: vec![],
                            });
                            // Fill in unset owners with this mock so the client
                            // routes back here, while leaving any explicitly set
                            // owner endpoints (some tests point elsewhere) intact.
                            let self_addr = vec![
                                AdvertisedAddress::parse(&addr.to_string())
                                    .expect("valid test owner endpoint"),
                            ];
                            for entry in &mut topology.queues {
                                if entry.owner_endpoints.is_empty() {
                                    entry.owner_endpoints = self_addr.clone();
                                }
                            }
                            try_encode(Op::TopologyOk, frame.request_id, &topology).unwrap()
                        }
                    } else if frame.opcode == Op::Publish as u16 {
                        let publish: Publish = wire::decode_publish(&frame).unwrap();
                        if let Some(recorder) = &config.recorded_partitions {
                            if let Ok(mut partitions) = recorder.lock() {
                                partitions.push(publish.partition.id());
                            }
                        }
                        if let Some(recorder) = &config.recorded_versions {
                            if let Ok(mut versions) = recorder.lock() {
                                versions.push(publish.partitioning_version);
                            }
                        }
                        match &config.publish {
                            None => continue,
                            Some(MockBehavior::ConfirmOk) => {
                                wire::encode_publish_ok(frame.request_id, &PublishOk { offset: 0 })
                                    .unwrap()
                            }
                            Some(MockBehavior::RedirectTo(target)) => {
                                let redirect = Redirect {
                                    topic: publish.topic,
                                    partition: Partition::new(0),
                                    group: publish.group,
                                    owner_endpoints: vec![
                                        AdvertisedAddress::parse(&target.to_string())
                                            .expect("valid test owner endpoint"),
                                    ],
                                    partitioning_version: 0,
                                };
                                try_encode(Op::Redirect, frame.request_id, &redirect).unwrap()
                            }
                            Some(MockBehavior::UnavailableThen(left)) => {
                                if left
                                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                                        n.checked_sub(1)
                                    })
                                    .is_ok()
                                {
                                    fibril_wire::helper::error_frame(
                                        frame.request_id,
                                        fibril_wire::ERR_UNAVAILABLE,
                                        "recovering",
                                    )
                                    .unwrap()
                                } else {
                                    wire::encode_publish_ok(frame.request_id, &PublishOk { offset: 0 })
                                        .unwrap()
                                }
                            }
                            Some(MockBehavior::GatewayTimeout(seen)) => {
                                seen.fetch_add(1, Ordering::SeqCst);
                                fibril_wire::helper::error_frame(
                                    frame.request_id,
                                    fibril_wire::ERR_GATEWAY_TIMEOUT,
                                    "the partition owner did not answer, the outcome is unknown",
                                )
                                .unwrap()
                            }
                            Some(MockBehavior::RedirectToSelf) => {
                                let redirect = Redirect {
                                    topic: publish.topic,
                                    partition: Partition::new(0),
                                    group: publish.group,
                                    owner_endpoints: vec![
                                        AdvertisedAddress::parse(&addr.to_string())
                                            .expect("valid test owner endpoint"),
                                    ],
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

/// With failover retry disabled (`publish_timeout_ms = 0`), a redirect to an
/// unreachable owner surfaces a clean connection error fast — it does NOT hang.
/// (The default config retries such transient transport failures across an owner
/// failover until the budget; that bounded-retry path is exercised by the
/// failover-under-load harness rather than this fail-fast unit.)
#[tokio::test]
async fn redirect_to_dead_endpoint_errors_without_hanging() {
    // 127.0.0.1:1 is privileged and unbound -> connection refused fast.
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let mock = spawn_mock(MockBehavior::RedirectTo(dead)).await;

    // Disable the failover retry so a dead redirect fails fast (the opt-out path).
    let client = Client::connect(mock, ClientOptions::new().publish_timeout_ms(0))
        .await
        .unwrap();
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

/// A redirect ping-pong (owner keeps redirecting to itself) backs off past
/// `max_redirects` until the publish budget, then returns a clear terminal
/// error instead of spinning.
#[tokio::test]
async fn redirect_ping_pong_is_capped_by_max_redirects() {
    let mock = spawn_mock(MockBehavior::RedirectToSelf).await;

    let mut opts = ClientOptions::new().publish_timeout_ms(300);
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
                partition: Partition::new(0),
                group: None,
                owner_endpoints: vec![
                    AdvertisedAddress::parse(&owner.to_string())
                        .expect("valid test owner endpoint"),
                ],
                partitioning_version: 0,
                partition_count: 1,
            }],
            streams: vec![],
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

/// With a multi-partition topology, keyless publishes spread across partitions
/// (round-robin) while publishes carrying the same partition key all land on a
/// single partition (stable hash routing).
#[tokio::test]
async fn keyless_publishes_spread_keyed_publishes_stick() {
    let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mock = spawn_configurable_mock(MockConfig {
        publish: Some(MockBehavior::ConfirmOk),
        self_partitions: Some(SelfPartitions {
            topic: "jobs".into(),
            group: None,
            partition_count: 4,
            partitioning_version: 0,
            live_partition_count: None,
        }),
        recorded_partitions: Some(recorded.clone()),
        ..Default::default()
    })
    .await;

    let client = Client::connect(mock, ClientOptions::new()).await.unwrap();
    // Populate the routing cache so the client sees partition_count = 4.
    let topology = client.fetch_topology().await.unwrap();
    assert_eq!(topology.queues.len(), 4);

    let publisher = client.publisher("jobs").unwrap();

    // Keyless publishes should fan out across the partitions.
    let keyless = 16usize;
    for _ in 0..keyless {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            publisher.publish_confirmed(NewMessage::text("spread")),
        )
        .await
        .expect("keyless publish must not hang")
        .expect("keyless publish should succeed");
    }

    let keyless_partitions: Vec<u32> = recorded.lock().unwrap().clone();
    let distinct: std::collections::HashSet<u32> = keyless_partitions.iter().copied().collect();
    assert!(
        distinct.len() > 1,
        "keyless publishes should spread across partitions, saw only {distinct:?}"
    );

    // Keyed publishes (same key) should all land on one partition.
    recorded.lock().unwrap().clear();
    let keyed = 16usize;
    for _ in 0..keyed {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            publisher.publish_confirmed(NewMessage::text("sticky").partition_key("order-42")),
        )
        .await
        .expect("keyed publish must not hang")
        .expect("keyed publish should succeed");
    }

    let keyed_partitions: Vec<u32> = recorded.lock().unwrap().clone();
    let keyed_distinct: std::collections::HashSet<u32> = keyed_partitions.iter().copied().collect();
    assert_eq!(
        keyed_distinct.len(),
        1,
        "same partition key must route to a single partition, saw {keyed_distinct:?}"
    );
    assert!(
        keyed_partitions[0] < 4,
        "routed partition must be within partition_count"
    );
}

/// A topology fetch that answers with an older generation than the cache
/// already holds, as a broker that just restarted can, must not regress the
/// cache. Here the stale view lacks the queue, and applying it would route
/// every keyless publish to partition 0 and make the queue look deleted.
#[tokio::test]
async fn stale_topology_fetch_does_not_regress_the_cache() {
    let recorded = Arc::new(std::sync::Mutex::new(Vec::new()));
    let stale = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mock = spawn_configurable_mock(MockConfig {
        publish: Some(MockBehavior::ConfirmOk),
        self_partitions: Some(SelfPartitions {
            topic: "jobs".into(),
            group: None,
            partition_count: 4,
            partitioning_version: 0,
            live_partition_count: None,
        }),
        recorded_partitions: Some(recorded.clone()),
        stale_topology: Some((
            stale.clone(),
            TopologyOk {
                generation: 0,
                queues: vec![QueueTopologyEntry {
                    topic: "other".into(),
                    partition: Partition::new(0),
                    group: None,
                    owner_endpoints: vec![],
                    partitioning_version: 0,
                    partition_count: 1,
                }],
                streams: vec![],
            },
        )),
        ..Default::default()
    })
    .await;

    let client = Client::connect(mock, ClientOptions::new()).await.unwrap();
    assert_eq!(client.fetch_topology().await.unwrap().generation, 1);
    stale.store(true, Ordering::SeqCst);
    assert_eq!(client.fetch_topology().await.unwrap().generation, 0);

    let publisher = client.publisher("jobs").unwrap();
    for _ in 0..8 {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            publisher.publish_confirmed(NewMessage::text("spread")),
        )
        .await
        .expect("publish must not hang")
        .expect("publish should succeed");
    }
    let distinct: std::collections::HashSet<u32> =
        recorded.lock().unwrap().iter().copied().collect();
    assert!(
        distinct.len() > 1,
        "the cache should keep the newer four-partition view, saw only {distinct:?}"
    );
}

/// The client stamps each publish with the partitioning version it routed
/// under (learned from the topology cache), so the owner can fence a stale
/// view. With a topology at version 5, every publish carries version 5.
#[tokio::test]
async fn publishes_carry_routed_partitioning_version() {
    let versions = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mock = spawn_configurable_mock(MockConfig {
        publish: Some(MockBehavior::ConfirmOk),
        self_partitions: Some(SelfPartitions {
            topic: "jobs".into(),
            group: None,
            partition_count: 3,
            partitioning_version: 5,
            live_partition_count: None,
        }),
        recorded_versions: Some(versions.clone()),
        ..Default::default()
    })
    .await;

    let client = Client::connect(mock, ClientOptions::new()).await.unwrap();
    client.fetch_topology().await.unwrap();

    let publisher = client.publisher("jobs").unwrap();
    for _ in 0..5 {
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            publisher.publish_confirmed(NewMessage::text("v")),
        )
        .await
        .expect("publish must not hang")
        .expect("publish should succeed");
    }

    let stamped: Vec<u64> = versions.lock().unwrap().clone();
    assert_eq!(stamped.len(), 5);
    assert!(
        stamped.iter().all(|&v| v == 5),
        "every publish must carry the routed version 5, saw {stamped:?}"
    );
}

/// A subscription transparently fans in across all partitions the topology
/// cache knows about: the client opens one `Subscribe` per partition and the
/// merged stream yields messages from every partition.
#[tokio::test]
async fn subscription_fans_in_all_partitions() {
    let subscribed = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mock = spawn_configurable_mock(MockConfig {
        self_partitions: Some(SelfPartitions {
            topic: "jobs".into(),
            group: None,
            partition_count: 3,
            partitioning_version: 0,
            live_partition_count: None,
        }),
        subscribe_partitions: Some(subscribed.clone()),
        ..Default::default()
    })
    .await;

    // No explicit fetch_topology: connect warms the cache, so a pure consumer
    // transparently fans in over all partitions.
    let client = Client::connect(mock, ClientOptions::new()).await.unwrap();

    let mut sub = client.subscribe("jobs").unwrap().sub().await.unwrap();

    // The mock delivers one message per partition (payload = partition).
    let mut payloads = std::collections::HashSet::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
            .await
            .expect("fan-in delivery must not hang")
            .delivery()
            .expect("subscription should yield a message");
        payloads.insert(msg.payload[0]);
    }
    assert_eq!(
        payloads,
        std::collections::HashSet::from([0u8, 1, 2]),
        "fan-in should deliver from every partition"
    );

    let mut subbed = subscribed.lock().unwrap().clone();
    subbed.sort();
    assert_eq!(
        subbed,
        vec![0u32, 1, 2],
        "client should open one subscribe per partition"
    );
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

/// A live subscription picks up a partition added by a grow: the mock reports
/// partition_count 1, the consumer subscribes, then the count grows to 2 and the
/// auto-resubscribe manager subscribes to the new partition and delivers from it.
#[tokio::test]
async fn subscription_auto_resubscribes_to_grown_partition() {
    let count = Arc::new(std::sync::atomic::AtomicU32::new(1));
    let mock = spawn_configurable_mock(MockConfig {
        self_partitions: Some(SelfPartitions {
            topic: "orders".into(),
            group: None,
            partition_count: 1,
            partitioning_version: 0,
            live_partition_count: Some(count.clone()),
        }),
        ..Default::default()
    })
    .await;

    let opts = ClientOptions::new()
        .topology_warm_timeout_ms(2_000)
        .partition_resubscribe_interval_ms(150);
    let client = Client::connect(mock, opts).await.unwrap();

    let mut sub = client
        .subscribe("orders")
        .unwrap()
        .sub_auto_ack()
        .await
        .unwrap();

    // Initial fan-in covers partition 0 (mock delivers payload = [partition]).
    let first = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
        .await
        .expect("first delivery")
        .delivery()
        .expect("message");
    assert_eq!(first.payload, vec![0]);

    // Grow to 2 partitions; the manager should subscribe to partition 1.
    count.store(2, std::sync::atomic::Ordering::SeqCst);

    // Collect deliveries until partition 1's message arrives.
    let saw_partition_1 = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match sub.recv().await {
                fibril_client::SubEvent::Delivery(msg) if msg.payload == vec![1] => break true,
                fibril_client::SubEvent::Delivery(_) => continue,
                fibril_client::SubEvent::Closed(_) => break false,
            }
        }
    })
    .await
    .expect("auto-resubscribe should deliver from the new partition");
    assert!(
        saw_partition_1,
        "consumer received a delivery from the grown partition"
    );
}

/// A routing pattern subscription fans in across every queue whose topic matches
/// the glob (and no others), tagging each delivery with its source channel.
#[tokio::test]
async fn pattern_subscription_fans_in_matching_queues() {
    let entry = |topic: &str| QueueTopologyEntry {
        topic: topic.into(),
        partition: Partition::new(0),
        group: None,
        owner_endpoints: vec![],
        partitioning_version: 0,
        partition_count: 1,
    };
    let mock = spawn_configurable_mock(MockConfig {
        topology: Some(TopologyOk {
            generation: 1,
            queues: vec![
                entry("events.click"),
                entry("events.view"),
                entry("orders.new"),
            ],
            streams: vec![],
        }),
        ..Default::default()
    })
    .await;

    // Connect warms the catalogue, so the pattern can resolve its matches without
    // an explicit topology fetch.
    let client = Client::connect(mock, ClientOptions::new()).await.unwrap();

    let mut sub = client
        .routing()
        .subscribe_pattern("events.*")
        .sub()
        .await
        .unwrap();

    // Both events.* queues deliver; orders.new must not.
    let mut sources = std::collections::HashSet::new();
    for _ in 0..2 {
        let (source, _msg) = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
            .await
            .expect("pattern fan-in must not hang")
            .delivery()
            .expect("subscription should yield a message");
        sources.insert(source.topic);
    }
    assert_eq!(
        sources,
        std::collections::HashSet::from(["events.click".to_string(), "events.view".to_string()]),
        "pattern should fan in only the matching queues"
    );
}

/// The auto-ack pattern variant fans in the same matching queues, yielding
/// settled messages tagged with their source.
#[tokio::test]
async fn pattern_subscription_auto_ack_fans_in_matching_queues() {
    let entry = |topic: &str| QueueTopologyEntry {
        topic: topic.into(),
        partition: Partition::new(0),
        group: None,
        owner_endpoints: vec![],
        partitioning_version: 0,
        partition_count: 1,
    };
    let mock = spawn_configurable_mock(MockConfig {
        topology: Some(TopologyOk {
            generation: 1,
            queues: vec![
                entry("events.click"),
                entry("events.view"),
                entry("orders.new"),
            ],
            streams: vec![],
        }),
        ..Default::default()
    })
    .await;

    let client = Client::connect(mock, ClientOptions::new()).await.unwrap();

    let mut sub = client
        .routing()
        .subscribe_pattern("events.*")
        .sub_auto_ack()
        .await
        .unwrap();

    let mut sources = std::collections::HashSet::new();
    for _ in 0..2 {
        let (source, _msg) = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
            .await
            .expect("pattern fan-in must not hang")
            .delivery()
            .expect("subscription should yield a message");
        sources.insert(source.topic);
    }
    assert_eq!(
        sources,
        std::collections::HashSet::from(["events.click".to_string(), "events.view".to_string()]),
        "auto-ack pattern should fan in only the matching queues"
    );
}

#[tokio::test]
async fn subscriber_discovers_survivor_and_waits_through_recovery() {
    let layout = || SelfPartitions {
        topic: "jobs".into(), group: None, partition_count: 1,
        partitioning_version: 1, live_partition_count: None,
    };
    let retries = Arc::new(AtomicUsize::new(2));
    let survivor = spawn_configurable_mock(MockConfig {
        self_partitions: Some(layout()), reject_subscribes: Some(retries.clone()),
        ..Default::default()
    }).await;
    let stop = Arc::new(tokio::sync::Notify::new());
    let initial = spawn_configurable_mock(MockConfig {
        self_partitions: Some(layout()), stop: Some(stop.clone()),
        ..Default::default()
    }).await;
    let client = ClientOptions::new().discovery_endpoints([survivor.to_string()])
        .connect(initial).await.unwrap();
    let mut sub = client.subscribe("jobs").unwrap().sub().await.unwrap().into_stream().boxed();
    let _first = tokio::time::timeout(std::time::Duration::from_secs(5), sub.next()).await.unwrap().unwrap();
    stop.notify_waiters();
    let delivered = tokio::time::timeout(std::time::Duration::from_secs(8), sub.next())
        .await.expect("same subscription must migrate without a fresh client");
    assert!(delivered.is_some());
    assert_eq!(retries.load(Ordering::SeqCst), 0);
    client.shutdown().await;
}

/// An owner that is still recovering answers `ERR_UNAVAILABLE`. A confirmed
/// publish waits it out with backoff instead of failing.
#[tokio::test]
async fn publish_waits_out_a_recovering_owner() {
    let left = Arc::new(AtomicUsize::new(3));
    let mock = spawn_mock(MockBehavior::UnavailableThen(left.clone())).await;
    let client = Client::connect(mock, ClientOptions::new().publish_timeout_ms(5_000))
        .await
        .unwrap();
    let publisher = client.publisher("jobs").unwrap();

    let offset = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        publisher.publish_confirmed("hello"),
    )
    .await
    .expect("must not hang")
    .expect("publish succeeds once the owner is ready");
    assert_eq!(offset, 0);
    assert_eq!(left.load(Ordering::SeqCst), 0, "every unavailable reply was retried");
}

/// With the retry budget disabled, a recovering owner surfaces its retryable
/// `ERR_UNAVAILABLE` at once.
#[tokio::test]
async fn recovering_owner_fails_fast_without_a_budget() {
    let mock = spawn_mock(MockBehavior::UnavailableThen(Arc::new(AtomicUsize::new(100)))).await;
    let client = Client::connect(mock, ClientOptions::new().publish_timeout_ms(0))
        .await
        .unwrap();
    let publisher = client.publisher("jobs").unwrap();
    let err = publisher.publish_confirmed("hello").await.unwrap_err();
    assert!(
        matches!(err, FibrilError::Failure { code: fibril_wire::ERR_UNAVAILABLE, .. }),
        "got {err:?}"
    );
    assert!(err.is_retryable());
}

/// A gateway that lost the owner after forwarding a publish answers `504`:
/// the publish may have been applied. The client surfaces it at once instead
/// of replaying it, even with a retry budget, and advises against a blind
/// retry. The reliable publisher stops on it too.
#[tokio::test]
async fn gateway_timeout_is_not_replayed() {
    let seen = Arc::new(AtomicUsize::new(0));
    let mock = spawn_mock(MockBehavior::GatewayTimeout(seen.clone())).await;
    let client = Client::connect(mock, ClientOptions::new().publish_timeout_ms(5_000))
        .await
        .unwrap();
    let publisher = client.publisher("jobs").unwrap();

    let err = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        publisher.publish_confirmed("hello"),
    )
    .await
    .expect("must not wait out the retry budget")
    .unwrap_err();
    assert!(
        matches!(err, FibrilError::Failure { code: fibril_wire::ERR_GATEWAY_TIMEOUT, .. }),
        "got {err:?}"
    );
    assert_eq!(err.retry_advice(), fibril_client::RetryAdvice::DoNotRetry);
    assert!(!err.is_retryable());
    assert_eq!(seen.load(Ordering::SeqCst), 1, "a 504 publish was replayed");

    let reliable = client.publisher("jobs").unwrap().reliable();
    let err = tokio::time::timeout(std::time::Duration::from_secs(5), reliable.publish("again"))
        .await
        .expect("the reliable publisher must not loop on 504")
        .unwrap_err();
    assert!(
        matches!(err, FibrilError::Failure { code: fibril_wire::ERR_GATEWAY_TIMEOUT, .. }),
        "got {err:?}"
    );
    assert_eq!(seen.load(Ordering::SeqCst), 2);
    client.shutdown().await;
}

/// A subscribe the broker answers with a non-retryable error must fail
/// promptly, never leave `sub()` waiting.
#[tokio::test]
async fn subscribe_error_reply_does_not_hang() {
    for (code, op) in [(500, Op::SubscribeErr), (500, Op::Error), (503, Op::SubscribeErr)] {
        let mock = spawn_configurable_mock(MockConfig {
            reject_subscribes: Some(Arc::new(AtomicUsize::new(usize::MAX))),
            reject_with: Some((code, op)),
            ..Default::default()
        })
        .await;
        let client = Client::connect(mock, ClientOptions::new().publish_timeout_ms(500))
            .await
            .unwrap();
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.subscribe("jobs").unwrap().sub(),
        )
        .await;
        assert!(
            matches!(result, Ok(Err(_))),
            "code {code} via {op:?} must fail, got {:?}",
            result.map(|r| r.map(|_| ()))
        );
    }
}

/// Who the late-owner mock's topology names as the owner of `jobs` partition 0.
#[derive(Clone, Copy)]
enum MockOwner {
    /// Not in the topology yet, the way a just-declared queue can be missing
    /// from the client's cache.
    Unknown,
    /// The mock itself.
    Itself,
    /// Another endpoint, as after a failover.
    At(SocketAddr),
}

/// A mock owner for `jobs` partition 0 whose topology answer follows `owner`.
/// Like the broker, it refuses a second subscribe to the same partition on one
/// connection while the first is live. Deliveries on the first subscription
/// follow `deliver`: each bump sends payloads up to the new count.
async fn spawn_late_owner_mock(
    owner: Arc<std::sync::Mutex<MockOwner>>,
    subscribes: Arc<AtomicUsize>,
    deliver: tokio::sync::watch::Receiver<u8>,
) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            let owner = owner.clone();
            let subscribes = subscribes.clone();
            let mut deliver = deliver.clone();
            tokio::spawn(async move {
                let mut framed = Framed::new(sock, ProtoCodec);
                let Some(Ok(hello)) = framed.next().await else {
                    return;
                };
                let hello_ok = HelloOk {
                    protocol_version: PROTOCOL_V1,
                    owner_id: Uuid::nil(),
                    client_id: Uuid::nil(),
                    resume_token: Uuid::nil(),
                    resume_outcome: ResumeOutcome::New,
                    server_name: "mock".into(),
                    compliance: COMPLIANCE_STRING.into(),
                };
                if framed
                    .send(try_encode(Op::HelloOk, hello.request_id, &hello_ok).unwrap())
                    .await
                    .is_err()
                {
                    return;
                }
                let mut live: Option<Subscribe> = None;
                let mut sent = 0u8;
                loop {
                    tokio::select! {
                        next = framed.next() => {
                            let Some(Ok(frame)) = next else { return; };
                            let reply = if frame.opcode == Op::Subscribe as u16 {
                                let sub: Subscribe = try_decode(&frame).unwrap();
                                subscribes.fetch_add(1, Ordering::SeqCst);
                                if live.is_some() {
                                    fibril_wire::helper::error_frame(
                                        frame.request_id,
                                        fibril_wire::ERR_CONFLICT,
                                        "already subscribed",
                                    )
                                    .unwrap()
                                } else {
                                    let ok = SubscribeOk {
                                        sub_id: 1,
                                        topic: sub.topic.clone(),
                                        group: sub.group.clone(),
                                        partition: sub.partition,
                                        prefetch: sub.prefetch,
                                        consumer_group: None,
                                        consumer_target: None,
                                        member_id: None,
                                    };
                                    live = Some(sub);
                                    try_encode(Op::SubscribeOk, frame.request_id, &ok).unwrap()
                                }
                            } else if frame.opcode == Op::Topology as u16 {
                                let current = *owner.lock().unwrap();
                                let owner_addr = match current {
                                    MockOwner::Unknown => None,
                                    MockOwner::Itself => Some(addr),
                                    MockOwner::At(other) => Some(other),
                                };
                                let queues = owner_addr
                                    .map(|owner_addr| QueueTopologyEntry {
                                        topic: "jobs".into(),
                                        partition: Partition::new(0),
                                        group: None,
                                        owner_endpoints: vec![
                                            AdvertisedAddress::parse(&owner_addr.to_string())
                                                .expect("valid test owner endpoint"),
                                        ],
                                        partitioning_version: 0,
                                        partition_count: 1,
                                    })
                                    .into_iter()
                                    .collect();
                                let topology = TopologyOk {
                                    generation: 1,
                                    queues,
                                    streams: vec![],
                                };
                                try_encode(Op::TopologyOk, frame.request_id, &topology).unwrap()
                            } else {
                                continue; // acks, pings
                            };
                            if framed.send(reply).await.is_err() {
                                return;
                            }
                        }
                        changed = deliver.changed() => {
                            if changed.is_err() {
                                return;
                            }
                            let target = *deliver.borrow_and_update();
                            let Some(sub) = live.as_ref() else { continue; };
                            while sent < target {
                                sent += 1;
                                let msg = Deliver {
                                    sub_id: 1,
                                    topic: sub.topic.clone(),
                                    group: sub.group.clone(),
                                    partition: sub.partition,
                                    offset: sent as u64,
                                    delivery_tag: DeliveryTag { epoch: sent as u64 },
                                    published: 0,
                                    publish_received: 0,
                                    content_type: None,
                                    headers: HashMap::new(),
                                    payload: vec![sent],
                                };
                                if framed.send(wire::encode_deliver(0, &msg).unwrap()).await.is_err() {
                                    return;
                                }
                            }
                        }
                    }
                }
            });
        }
    });
    addr
}

async fn next_payload(sub: &mut fibril_client::Subscription) -> Vec<u8> {
    tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
        .await
        .expect("delivery must not stall")
        .delivery()
        .expect("subscription should yield a message")
        .payload
}

/// A subscription made before the client's topology cache knows the queue goes
/// through the bootstrap connection. When the cache later learns that the owner
/// is that same endpoint, the leg has not moved: it must keep delivering and
/// must not be re-subscribed, which the broker would refuse while the original
/// subscription is still live.
#[tokio::test]
async fn subscription_keeps_delivering_when_cache_learns_its_owner() {
    let owner = Arc::new(std::sync::Mutex::new(MockOwner::Unknown));
    let subscribes = Arc::new(AtomicUsize::new(0));
    let (deliver, deliver_rx) = tokio::sync::watch::channel(0u8);
    let mock = spawn_late_owner_mock(owner.clone(), subscribes.clone(), deliver_rx).await;

    let client = Client::connect(mock, ClientOptions::new()).await.unwrap();
    let mut sub = client
        .subscribe("jobs")
        .unwrap()
        .prefetch(4)
        .sub()
        .await
        .unwrap();

    deliver.send(1).unwrap();
    assert_eq!(next_payload(&mut sub).await, vec![1]);

    // The owner appears in the topology, at the endpoint the leg already uses.
    // Wait out a few of the supervisor's owner checks.
    *owner.lock().unwrap() = MockOwner::Itself;
    tokio::time::sleep(std::time::Duration::from_millis(3_500)).await;

    // More than the prefetch, so a leg nobody reads would also block the
    // connection reader.
    deliver.send(12).unwrap();
    for expected in 2..=12u8 {
        assert_eq!(next_payload(&mut sub).await, vec![expected]);
    }
    assert_eq!(
        subscribes.load(Ordering::SeqCst),
        1,
        "a leg already bound to the owner must not be re-subscribed"
    );
}

/// When the topology names a new owner that is not taking subscribes yet, the
/// old leg can still be delivering. The supervisor keeps forwarding it while it
/// retries the new owner, so those deliveries are not stranded.
#[tokio::test]
async fn moved_subscription_keeps_old_leg_flowing_while_resubscribing() {
    let new_owner = spawn_configurable_mock(MockConfig {
        reject_subscribes: Some(Arc::new(AtomicUsize::new(usize::MAX))),
        reject_with: Some((fibril_wire::ERR_UNAVAILABLE, Op::Error)),
        ..Default::default()
    })
    .await;
    let owner = Arc::new(std::sync::Mutex::new(MockOwner::Itself));
    let subscribes = Arc::new(AtomicUsize::new(0));
    let (deliver, deliver_rx) = tokio::sync::watch::channel(0u8);
    let old_owner = spawn_late_owner_mock(owner.clone(), subscribes, deliver_rx).await;

    let client = Client::connect(old_owner, ClientOptions::new())
        .await
        .unwrap();
    let mut sub = client
        .subscribe("jobs")
        .unwrap()
        .prefetch(4)
        .sub()
        .await
        .unwrap();
    deliver.send(1).unwrap();
    assert_eq!(next_payload(&mut sub).await, vec![1]);

    // The owner moves, and the new one keeps answering "unavailable".
    *owner.lock().unwrap() = MockOwner::At(new_owner);
    tokio::time::sleep(std::time::Duration::from_millis(2_500)).await;

    deliver.send(12).unwrap();
    for expected in 2..=12u8 {
        assert_eq!(next_payload(&mut sub).await, vec![expected]);
    }
}
