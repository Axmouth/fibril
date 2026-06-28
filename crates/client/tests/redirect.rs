//! Adversarial integration tests for client-side redirect following, using a
//! controllable mock broker (a TcpListener that speaks the wire) so we can
//! force redirect-to-dead-endpoint and redirect ping-pong without a real
//! cluster.

use std::collections::HashMap;
use std::net::SocketAddr;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fibril_client::{Client, ClientOptions, FibrilError, NewMessage, ReconnectOutcome};
use fibril_protocol::v1::{
    AdvertisedAddress, COMPLIANCE_STRING, Deliver, Hello, HelloOk, Op, PROTOCOL_V1, Publish,
    PublishOk,
    QueueTopologyEntry, ReconcileResult, Redirect, ResumeOutcome, Subscribe, SubscribeOk,
    TopologyOk,
    frame::ProtoCodec,
    helper::{try_decode, try_encode},
    wire,
};
use fibril_storage::{DeliveryTag, Partition};
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
                    // Subscribe is answered with SubscribeOk plus one tagged
                    // Deliver per partition (payload = partition), so a fan-in
                    // subscription receives a message from each partition.
                    if frame.opcode == Op::Subscribe as u16 {
                        let sub: Subscribe = try_decode(&frame).unwrap();
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
                        if let Some(spread) = &config.self_partitions {
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
                                    owner_endpoints: vec![AdvertisedAddress::parse(&addr.to_string()).expect("valid test owner endpoint")],
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
                                    owner_endpoints: vec![AdvertisedAddress::parse(&target.to_string()).expect("valid test owner endpoint")],
                                    partitioning_version: 0,
                                };
                                try_encode(Op::Redirect, frame.request_id, &redirect).unwrap()
                            }
                            Some(MockBehavior::RedirectToSelf) => {
                                let redirect = Redirect {
                                    topic: publish.topic,
                                    partition: Partition::new(0),
                                    group: publish.group,
                                    owner_endpoints: vec![AdvertisedAddress::parse(&addr.to_string()).expect("valid test owner endpoint")],
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
                partition: Partition::new(0),
                group: None,
                owner_endpoints: vec![AdvertisedAddress::parse(&owner.to_string()).expect("valid test owner endpoint")],
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
            publisher.publish_confirmed(NewMessage::content("spread")),
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
            publisher.publish_confirmed(NewMessage::content("sticky").partition_key("order-42")),
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
            publisher.publish_confirmed(NewMessage::content("v")),
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

    let mut sub = client
        .subscribe("jobs")
        .unwrap()
        .sub()
        .await
        .unwrap();

    // The mock delivers one message per partition (payload = partition).
    let mut payloads = std::collections::HashSet::new();
    for _ in 0..3 {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(5), sub.recv())
            .await
            .expect("fan-in delivery must not hang")
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
        .expect("message");
    assert_eq!(first.payload, vec![0]);

    // Grow to 2 partitions; the manager should subscribe to partition 1.
    count.store(2, std::sync::atomic::Ordering::SeqCst);

    // Collect deliveries until partition 1's message arrives.
    let saw_partition_1 = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match sub.recv().await {
                Some(msg) if msg.payload == vec![1] => break true,
                Some(_) => continue,
                None => break false,
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
            .expect("subscription should yield a message");
        sources.insert(source.topic);
    }
    assert_eq!(
        sources,
        std::collections::HashSet::from(["events.click".to_string(), "events.view".to_string()]),
        "pattern should fan in only the matching queues"
    );
}
