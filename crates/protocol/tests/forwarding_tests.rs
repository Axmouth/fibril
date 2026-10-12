//! Forwarding to private partition owners, end to end through real handler
//! connections: a client talks to a gateway broker that owns nothing, and the
//! gateway relays to an owner broker (real or scripted) on an upstream session.
#![cfg(not(feature = "simulation"))]

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use fibril_broker::{
    broker::{Broker, BrokerConfig, StaticQueueOwnership},
    queue_engine::StromaEngine,
};
use fibril_metrics::{ConnectionStats, ForwardStats, TcpStats};
use fibril_protocol::v1::{
    Ack, Auth, DeliveryTag, ERR_CONFLICT, ERR_GATEWAY_TIMEOUT, ERR_INVALID, ERR_UNAVAILABLE,
    ErrorMsg, ForwardedSession, ForwardedSessionOk, Hello, HelloOk, MAX_FORWARD_HOPS, Op,
    PROTOCOL_V1, Partition, Publish, PublishOk, Subscribe, SubscribeOk, TopologyOk,
    forwarding::{
        FORWARDED_SUB_ID_BASE, Forwarder, ForwardingConfig, PartitionRoute, RoutingLimits,
    },
    frame::Frame,
    handler::{ClientTopologySource, ConnectionSettings, handle_connection},
    helper::{Conn, plain_conn, try_decode, try_encode},
    replication::ProtocolOwnerPeerAuth,
    wire,
};
use fibril_util::{StaticAuthHandler, unix_millis};
use futures::{SinkExt, StreamExt};
use stroma_core::{KeratinConfig, SnapshotConfig, StromaKeratinConfig, TempDir};
use tokio::net::{TcpListener, TcpStream};
use uuid::Uuid;

const GATEWAY_ADDR: &str = "gateway.test:9876";
const OWNER: &str = "owner-p";

async fn open_broker(owns_everything: bool) -> (Arc<Broker<StromaEngine>>, TempDir) {
    let dir = TempDir {
        root: std::env::current_dir()
            .unwrap()
            .join("test_data")
            .join(format!("forwarding_test-{}", Uuid::now_v7())),
    };
    std::fs::create_dir_all(&dir.root).unwrap();
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let config = BrokerConfig {
        inflight_ttl_ms: 30_000,
        delivery_poll_max_ms: 50,
        queue_idle_evict_after_ms: None,
        ..Default::default()
    };
    let broker = if owns_everything {
        Broker::new(engine, config, None)
    } else {
        Broker::new_with_ownership(
            engine,
            config,
            None,
            Arc::new(StaticQueueOwnership::new(HashSet::new())),
        )
    };
    (broker, dir)
}

/// Routing view the gateway sees: every queue partition belongs to `OWNER`,
/// a private node, and clients are told to use the gateway itself.
struct GatewayTopology {
    peers: Mutex<HashMap<String, String>>,
    route: Mutex<Option<PartitionRoute>>,
}

impl GatewayTopology {
    fn new(owner_peer: String) -> Arc<Self> {
        Arc::new(Self {
            peers: Mutex::new(HashMap::from([(OWNER.to_string(), owner_peer)])),
            route: Mutex::new(Some(PartitionRoute {
                owner_node_id: Some(OWNER.into()),
                owner_is_local: false,
                owner_endpoints: Vec::new(),
                client_endpoints: vec![GATEWAY_ADDR.into()],
                partitioning_version: 0,
            })),
        })
    }

    fn set_route(&self, route: Option<PartitionRoute>) {
        *self.route.lock().unwrap() = route;
    }

    fn add_peer(&self, node_id: &str, addr: String) {
        self.peers.lock().unwrap().insert(node_id.into(), addr);
    }
}

impl ClientTopologySource for GatewayTopology {
    fn topology(&self) -> TopologyOk {
        TopologyOk {
            generation: 1,
            queues: Vec::new(),
            streams: Vec::new(),
        }
    }

    fn owner_endpoint(&self, _: &str, _: Partition, _: Option<&str>) -> Option<(String, u64)> {
        None
    }

    fn queue_route(&self, _: &str, _: Partition, _: Option<&str>) -> Option<PartitionRoute> {
        self.route.lock().unwrap().clone()
    }

    fn stream_route(&self, _: &str, _: Partition) -> Option<PartitionRoute> {
        self.route.lock().unwrap().clone()
    }

    fn local_client_endpoints(&self) -> Vec<String> {
        vec![GATEWAY_ADDR.into()]
    }

    fn node_peer_address(&self, node_id: &str) -> Option<String> {
        self.peers.lock().unwrap().get(node_id).cloned()
    }
}

fn node_auth() -> StaticAuthHandler {
    StaticAuthHandler::new("@node".into(), "secret".into())
}

/// Serve `broker` on a loopback listener, one handler per connection,
/// authenticating the node principal like a cluster owner.
async fn serve_owner(broker: Arc<Broker<StromaEngine>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((socket, peer)) = listener.accept().await else {
                return;
            };
            let broker = broker.clone();
            tokio::spawn(async move {
                let stats = ConnectionStats::new();
                let conn_id = stats.add_connection(peer, Instant::now(), false);
                let _ = handle_connection(
                    socket,
                    Some(peer),
                    broker,
                    TcpStats::new(10),
                    stats,
                    conn_id,
                    Some(node_auth()),
                    None,
                    ConnectionSettings::new(Some(60)),
                    None,
                    None,
                    None,
                )
                .await;
            });
        }
    });
    addr
}

struct Gateway {
    broker: Arc<Broker<StromaEngine>>,
    topology: Arc<GatewayTopology>,
    settings: ConnectionSettings,
    stats: Arc<ForwardStats>,
    _dir: TempDir,
}

async fn gateway(owner_peer: String, limits: RoutingLimits) -> Gateway {
    let (broker, dir) = open_broker(false).await;
    let stats = ForwardStats::new();
    let forwarder = Forwarder::new(
        ForwardingConfig {
            node_id: "gateway-g".into(),
            auth: ProtocolOwnerPeerAuth {
                username: "@node".into(),
                password: "secret".into(),
            },
            tls: None,
        },
        limits,
        stats.clone(),
    );
    let settings = ConnectionSettings::new(Some(60))
        .with_forward_stats(stats.clone())
        .with_forwarder(Arc::new(forwarder));
    Gateway {
        broker,
        topology: GatewayTopology::new(owner_peer),
        settings,
        stats,
        _dir: dir,
    }
}

impl Gateway {
    /// A client connection to the gateway, after HELLO.
    async fn client(&self) -> Conn {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();
        let stats = ConnectionStats::new();
        let conn_id = stats.add_connection(peer, Instant::now(), false);
        let topology: Arc<dyn ClientTopologySource> = self.topology.clone();
        tokio::spawn(handle_connection(
            server,
            Some(peer),
            self.broker.clone(),
            TcpStats::new(10),
            stats,
            conn_id,
            None::<StaticAuthHandler>,
            None,
            self.settings.clone(),
            Some(topology),
            None,
            None,
        ));
        let mut conn = plain_conn(client);
        hello(&mut conn).await;
        conn
    }
}

async fn hello(conn: &mut Conn) {
    conn.send(
        try_encode(
            Op::Hello,
            1,
            &Hello {
                client_name: "forwarding-test".into(),
                client_version: "0.1.0".into(),
                protocol_version: PROTOCOL_V1,
                resume: None,
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let frame = recv(conn).await;
    assert_eq!(frame.opcode, Op::HelloOk as u16);
    let _: HelloOk = try_decode(&frame).unwrap();
}

/// Next frame that is not a heartbeat.
async fn recv(conn: &mut Conn) -> Frame {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(15), conn.next())
            .await
            .expect("frame did not arrive in time")
            .expect("connection ended")
            .expect("frame decode failed");
        if frame.opcode == Op::Ping as u16 {
            let _ = conn
                .send(try_encode(Op::Pong, frame.request_id, &()).unwrap())
                .await;
            continue;
        }
        return frame;
    }
}

fn publish_frame(request_id: u64, topic: &str, payload: &[u8], confirm: bool) -> Frame {
    try_encode(
        Op::Publish,
        request_id,
        &Publish {
            topic: topic.into(),
            partition: Partition::new(0),
            group: None,
            require_confirm: confirm,
            content_type: None,
            headers: HashMap::new(),
            payload: payload.to_vec(),
            published: unix_millis(),
            partition_key: None,
            partitioning_version: 0,
            ttl_ms: None,
        },
    )
    .unwrap()
}

fn subscribe_frame(request_id: u64, topic: &str, prefetch: u32) -> Frame {
    try_encode(
        Op::Subscribe,
        request_id,
        &Subscribe {
            topic: topic.into(),
            partition: Partition::new(0),
            group: None,
            prefetch,
            auto_ack: false,
            consumer_group: None,
            consumer_target: None,
            member_id: None,
        },
    )
    .unwrap()
}

fn error_of(frame: &Frame) -> ErrorMsg {
    try_decode(frame)
        .unwrap_or_else(|_| panic!("expected an error frame, got opcode {}", frame.opcode))
}

/// A scripted owner: accepts the gateway's setup (HELLO, AUTH as the node
/// principal, ForwardedSession) and hands the test the connection.
async fn scripted_owner() -> (
    String,
    tokio::sync::mpsc::Receiver<(Conn, ForwardedSession)>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let mut conn = plain_conn(socket);
            let hello = conn.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            conn.send(
                try_encode(
                    Op::HelloOk,
                    hello.request_id,
                    &HelloOk {
                        protocol_version: PROTOCOL_V1,
                        owner_id: Uuid::new_v4(),
                        client_id: Uuid::new_v4(),
                        resume_token: Uuid::new_v4(),
                        resume_outcome: fibril_protocol::v1::ResumeOutcome::New,
                        server_name: "scripted".into(),
                        compliance: fibril_protocol::v1::COMPLIANCE_STRING.into(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
            let auth = conn.next().await.unwrap().unwrap();
            let auth_body: Auth = try_decode(&auth).unwrap();
            assert_eq!(auth_body.username, "@node");
            conn.send(try_encode(Op::AuthOk, auth.request_id, &()).unwrap())
                .await
                .unwrap();
            let marker = conn.next().await.unwrap().unwrap();
            let session = wire::decode_forwarded_session(&marker).unwrap();
            conn.send(
                wire::encode_forwarded_session_ok(
                    marker.request_id,
                    &ForwardedSessionOk {
                        forwarding_version: 1,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
            if tx.send((conn, session)).await.is_err() {
                return;
            }
        }
    });
    (addr, rx)
}

async fn next_upstream_request(owner: &mut Conn) -> Frame {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(15), owner.next())
            .await
            .expect("upstream request did not arrive")
            .expect("upstream closed")
            .unwrap();
        if frame.opcode == Op::Pong as u16 || frame.opcode == Op::Ping as u16 {
            continue;
        }
        return frame;
    }
}

#[tokio::test]
async fn forwarded_publish_confirms_only_with_the_owners_confirm_and_offset() {
    let (owner_addr, mut owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;

    client
        .send(publish_frame(41, "orders", b"one", true))
        .await
        .unwrap();
    let (mut owner, session) = owners.recv().await.unwrap();
    assert_eq!(session.origin_node, "gateway-g");
    assert_eq!(session.hop_count, MAX_FORWARD_HOPS);

    let upstream = next_upstream_request(&mut owner).await;
    assert_eq!(upstream.opcode, Op::Publish as u16);
    assert_ne!(
        upstream.request_id, 41,
        "upstream request ids are the gateway's own"
    );
    let body = wire::decode_publish(&upstream).unwrap();
    assert_eq!(body.payload, b"one");
    assert!(body.require_confirm);

    // No confirm reaches the client while the owner has not confirmed.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), client.next())
            .await
            .is_err(),
        "the gateway confirmed before the owner did"
    );

    owner
        .send(wire::encode_publish_ok(upstream.request_id, &PublishOk { offset: 4242 }).unwrap())
        .await
        .unwrap();
    let confirm = recv(&mut client).await;
    assert_eq!(confirm.opcode, Op::PublishOk as u16);
    assert_eq!(confirm.request_id, 41);
    assert_eq!(wire::decode_publish_ok(&confirm).unwrap().offset, 4242);
    assert_eq!(
        gateway.stats.requests(
            fibril_metrics::ForwardOp::Publish,
            fibril_metrics::ForwardOutcome::Ok
        ),
        1
    );
    // The gateway never counts a forwarded publish as its own.
    assert!(
        gateway
            .broker
            .queue_activity_snapshot("orders", None)
            .is_none()
    );
}

#[tokio::test]
async fn dropped_upstream_with_a_publish_in_flight_answers_504() {
    let (owner_addr, mut owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;

    client
        .send(publish_frame(7, "orders", b"x", true))
        .await
        .unwrap();
    let (mut owner, _) = owners.recv().await.unwrap();
    let upstream = next_upstream_request(&mut owner).await;
    assert_eq!(upstream.opcode, Op::Publish as u16);
    drop(owner);

    let reply = recv(&mut client).await;
    assert_eq!(reply.request_id, 7);
    assert_eq!(error_of(&reply).code, ERR_GATEWAY_TIMEOUT);
}

#[tokio::test]
async fn unreachable_owner_answers_503_because_nothing_was_sent() {
    // A port nothing listens on.
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = closed.local_addr().unwrap().to_string();
    drop(closed);
    let gateway = gateway(
        addr,
        RoutingLimits {
            upstream_connect_timeout_ms: 500,
            ..RoutingLimits::default()
        },
    )
    .await;
    let mut client = gateway.client().await;
    client
        .send(publish_frame(3, "orders", b"x", true))
        .await
        .unwrap();
    let reply = recv(&mut client).await;
    assert_eq!(reply.request_id, 3);
    assert_eq!(error_of(&reply).code, ERR_UNAVAILABLE);
}

#[tokio::test]
async fn upstream_redirect_is_rebuilt_from_the_gateway_view() {
    let (owner_addr, mut owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;

    client
        .send(publish_frame(9, "orders", b"x", true))
        .await
        .unwrap();
    let (mut owner, _) = owners.recv().await.unwrap();
    let upstream = next_upstream_request(&mut owner).await;

    // Meanwhile the gateway's view moved the partition to a public owner
    // with its own address.
    gateway.topology.set_route(Some(PartitionRoute {
        owner_node_id: Some("owner-q".into()),
        owner_is_local: false,
        owner_endpoints: vec!["public-q.test:9876".into()],
        client_endpoints: vec!["public-q.test:9876".into()],
        partitioning_version: 3,
    }));
    // The old owner refused with a redirect to an internal address.
    owner
        .send(
            try_encode(
                Op::Redirect,
                upstream.request_id,
                &fibril_protocol::v1::Redirect {
                    topic: "orders".into(),
                    partition: Partition::new(0),
                    group: None,
                    owner_endpoints: vec![
                        fibril_protocol::v1::AdvertisedAddress::parse("10.9.9.9:9876").unwrap(),
                    ],
                    partitioning_version: 5,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let reply = recv(&mut client).await;
    assert_eq!(reply.opcode, Op::Redirect as u16);
    assert_eq!(reply.request_id, 9);
    let redirect = wire::decode_redirect(&reply).unwrap();
    let targets: Vec<_> = redirect
        .owner_endpoints
        .iter()
        .map(|a| a.target())
        .collect();
    assert_eq!(targets, vec!["public-q.test:9876".to_string()]);
    // The higher of the two partitioning versions.
    assert_eq!(redirect.partitioning_version, 5);
}

#[tokio::test]
async fn owner_moved_to_another_private_node_gets_one_more_upstream_attempt() {
    let (first_addr, mut first_owners) = scripted_owner().await;
    let (second_addr, mut second_owners) = scripted_owner().await;
    let gateway = gateway(first_addr, RoutingLimits::default()).await;
    gateway.topology.add_peer("owner-r", second_addr);
    let mut client = gateway.client().await;

    client
        .send(publish_frame(12, "orders", b"x", true))
        .await
        .unwrap();
    let (mut first, _) = first_owners.recv().await.unwrap();
    let upstream = next_upstream_request(&mut first).await;
    gateway.topology.set_route(Some(PartitionRoute {
        owner_node_id: Some("owner-r".into()),
        owner_is_local: false,
        owner_endpoints: Vec::new(),
        client_endpoints: vec![GATEWAY_ADDR.into()],
        partitioning_version: 0,
    }));
    first
        .send(
            try_encode(
                Op::Error,
                upstream.request_id,
                &ErrorMsg {
                    code: ERR_CONFLICT,
                    message: "not owner".into(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let (mut second, _) = second_owners.recv().await.unwrap();
    let retried = next_upstream_request(&mut second).await;
    assert_eq!(retried.opcode, Op::Publish as u16);
    assert_eq!(wire::decode_publish(&retried).unwrap().payload, b"x");
    // A second refusal is final: no third attempt.
    second
        .send(
            try_encode(
                Op::Error,
                retried.request_id,
                &ErrorMsg {
                    code: ERR_CONFLICT,
                    message: "not owner".into(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let reply = recv(&mut client).await;
    assert_eq!(reply.request_id, 12);
    assert_eq!(error_of(&reply).code, ERR_UNAVAILABLE);

    // The next publish goes straight to the new owner and confirms.
    client
        .send(publish_frame(13, "orders", b"y", true))
        .await
        .unwrap();
    let request = next_upstream_request(&mut second).await;
    second
        .send(wire::encode_publish_ok(request.request_id, &PublishOk { offset: 1 }).unwrap())
        .await
        .unwrap();
    let confirm = recv(&mut client).await;
    assert_eq!(confirm.opcode, Op::PublishOk as u16);
    assert_eq!(confirm.request_id, 13);
}

#[tokio::test]
async fn not_owner_with_an_unchanged_view_answers_503() {
    let (owner_addr, mut owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;

    client
        .send(publish_frame(5, "orders", b"x", true))
        .await
        .unwrap();
    let (mut owner, _) = owners.recv().await.unwrap();
    let upstream = next_upstream_request(&mut owner).await;
    owner
        .send(
            try_encode(
                Op::Error,
                upstream.request_id,
                &ErrorMsg {
                    code: ERR_CONFLICT,
                    message: "not owner".into(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let reply = recv(&mut client).await;
    assert_eq!(reply.request_id, 5);
    assert_eq!(error_of(&reply).code, ERR_UNAVAILABLE);
}

#[tokio::test]
async fn forwarded_subscribe_delivers_and_settles_on_the_owner() {
    let (owner_broker, _owner_dir) = open_broker(true).await;
    let owner_addr = serve_owner(owner_broker.clone()).await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;

    client.send(subscribe_frame(20, "jobs", 1)).await.unwrap();
    let reply = recv(&mut client).await;
    assert_eq!(
        reply.opcode,
        Op::SubscribeOk as u16,
        "{:?}",
        try_decode::<ErrorMsg>(&reply)
    );
    assert_eq!(reply.request_id, 20);
    let ok: SubscribeOk = try_decode(&reply).unwrap();
    assert!(ok.sub_id >= FORWARDED_SUB_ID_BASE);

    for (id, payload) in [(21, b"first"), (22, b"other")] {
        client
            .send(publish_frame(id, "jobs", payload, true))
            .await
            .unwrap();
    }
    // Confirms and the first delivery interleave. Prefetch 1 holds the
    // second delivery until the first is acked through the gateway.
    let mut confirms = 0;
    let mut first: Option<fibril_protocol::v1::Deliver> = None;
    while confirms < 2 || first.is_none() {
        let frame = recv(&mut client).await;
        match frame.opcode {
            x if x == Op::PublishOk as u16 => confirms += 1,
            x if x == Op::Deliver as u16 => {
                assert!(
                    first.is_none(),
                    "prefetch 1 allows one delivery before an ack"
                );
                first = Some(wire::decode_deliver(&frame).unwrap());
            }
            other => panic!("unexpected opcode {other}"),
        }
    }
    let first = first.unwrap();
    assert_eq!(first.sub_id, ok.sub_id);
    assert_eq!(first.payload, b"first");

    client
        .send(
            try_encode(
                Op::Ack,
                23,
                &Ack {
                    topic: "jobs".into(),
                    group: None,
                    partition: Partition::new(0),
                    tags: vec![first.delivery_tag],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let second = recv(&mut client).await;
    assert_eq!(second.opcode, Op::Deliver as u16);
    let second = wire::decode_deliver(&second).unwrap();
    assert_eq!(second.payload, b"other");
    assert_eq!(second.sub_id, ok.sub_id);
    // Tags pass through unchanged: the owner minted distinct ones.
    assert_ne!(second.delivery_tag, first.delivery_tag);

    // A settle with a tag the owner never issued is ignored there, like a
    // direct client's, and the connection stays healthy.
    client
        .send(
            try_encode(
                Op::Ack,
                24,
                &Ack {
                    topic: "jobs".into(),
                    group: None,
                    partition: Partition::new(0),
                    tags: vec![DeliveryTag { epoch: u64::MAX }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .send(try_encode(Op::Ping, 25, &()).unwrap())
        .await
        .unwrap();
    assert_eq!(recv(&mut client).await.opcode, Op::Pong as u16);
}

#[tokio::test]
async fn ending_the_client_session_releases_the_owners_subscriptions() {
    let (owner_addr, mut owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;

    client
        .send(publish_frame(1, "orders", b"x", true))
        .await
        .unwrap();
    let (mut owner, _) = owners.recv().await.unwrap();
    let upstream = next_upstream_request(&mut owner).await;
    owner
        .send(wire::encode_publish_ok(upstream.request_id, &PublishOk { offset: 0 }).unwrap())
        .await
        .unwrap();
    assert_eq!(recv(&mut client).await.opcode, Op::PublishOk as u16);

    // No reconnect grace: the client session ends with the connection.
    drop(client);
    let reconcile = next_upstream_request(&mut owner).await;
    assert_eq!(reconcile.opcode, Op::ReconcileClient as u16);
    let body: fibril_protocol::v1::ReconcileClient = try_decode(&reconcile).unwrap();
    assert!(
        body.subscriptions.is_empty(),
        "the owner keeps nothing for an ended session"
    );
    // Then the upstream session closes.
    let end = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match owner.next().await {
                None | Some(Err(_)) => return,
                Some(Ok(frame)) if frame.opcode == Op::Pong as u16 => continue,
                Some(Ok(frame)) => panic!("unexpected frame {}", frame.opcode),
            }
        }
    })
    .await;
    assert!(end.is_ok(), "the upstream session stayed open");
    tokio::time::timeout(Duration::from_secs(5), async {
        while gateway.stats.upstream_sessions() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the session slot is released");
}

#[tokio::test]
async fn cohort_subscribe_through_a_gateway_is_refused_clearly() {
    let (owner_addr, _owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;

    let cohort = try_encode(
        Op::Subscribe,
        30,
        &Subscribe {
            topic: "jobs".into(),
            partition: Partition::new(0),
            group: None,
            prefetch: 1,
            auto_ack: false,
            consumer_group: Some("workers".into()),
            consumer_target: None,
            member_id: None,
        },
    )
    .unwrap();
    client.send(cohort).await.unwrap();
    let reply = recv(&mut client).await;
    assert_eq!(reply.opcode, Op::SubscribeErr as u16);
    let err = error_of(&reply);
    assert_eq!(err.code, ERR_INVALID);
    assert!(err.message.contains("consumer groups"), "{}", err.message);
}

#[tokio::test]
async fn per_owner_session_cap_answers_503_naming_the_limit() {
    let (owner_addr, mut owners) = scripted_owner().await;
    let gateway = gateway(
        owner_addr,
        RoutingLimits {
            max_upstream_sessions_per_owner: 1,
            ..RoutingLimits::default()
        },
    )
    .await;
    let mut first = gateway.client().await;
    first
        .send(publish_frame(1, "orders", b"x", true))
        .await
        .unwrap();
    let (_owner, _) = owners.recv().await.unwrap();

    // A second client session needs its own upstream session to the owner.
    let mut second = gateway.client().await;
    second
        .send(publish_frame(2, "orders", b"y", true))
        .await
        .unwrap();
    let reply = recv(&mut second).await;
    let err = error_of(&reply);
    assert_eq!(err.code, ERR_UNAVAILABLE);
    assert!(
        err.message.contains("max_upstream_sessions_per_owner"),
        "{}",
        err.message
    );
    assert_eq!(
        gateway
            .stats
            .rejected_total(fibril_metrics::ForwardLimit::UpstreamSessionsPerOwner),
        1
    );
}

// ---------------------------------------------------------------------------
// Owner side: the ForwardedSession marker
// ---------------------------------------------------------------------------

async fn owner_connection(auth: StaticAuthHandler, settings: ConnectionSettings) -> Conn {
    let (broker, dir) = open_broker(true).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, peer) = listener.accept().await.unwrap();
    let stats = ConnectionStats::new();
    let conn_id = stats.add_connection(peer, Instant::now(), false);
    tokio::spawn(async move {
        let _dir = dir;
        let _ = handle_connection(
            server,
            Some(peer),
            broker,
            TcpStats::new(10),
            stats,
            conn_id,
            Some(auth),
            None,
            settings,
            None,
            None,
            None,
        )
        .await;
    });
    let mut conn = plain_conn(client);
    hello(&mut conn).await;
    conn
}

async fn authenticate(conn: &mut Conn, username: &str, password: &str) {
    conn.send(
        try_encode(
            Op::Auth,
            2,
            &Auth {
                username: username.into(),
                password: password.into(),
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(recv(conn).await.opcode, Op::AuthOk as u16);
}

fn marker(request_id: u64, hop_count: u8) -> Frame {
    wire::encode_forwarded_session(
        request_id,
        &ForwardedSession {
            origin_node: "gateway-g".into(),
            hop_count,
            acting_user: "alice".into(),
            client_id: Uuid::new_v4(),
            client_conn_id: Uuid::new_v4(),
            client_addr: Some("203.0.113.5:4000".into()),
            release_on_close: false,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn forwarded_session_is_refused_to_anyone_but_the_node_principal() {
    let mut conn = owner_connection(
        StaticAuthHandler::new("alice".into(), "pw".into()),
        ConnectionSettings::new(Some(60)),
    )
    .await;
    authenticate(&mut conn, "alice", "pw").await;
    conn.send(marker(5, 1)).await.unwrap();
    let reply = recv(&mut conn).await;
    assert_eq!(reply.request_id, 5);
    assert_eq!(error_of(&reply).code, 403);
}

#[tokio::test]
async fn forwarded_session_enforces_one_hop_and_counts_sessions() {
    let stats = ForwardStats::new();
    let mut conn = owner_connection(
        node_auth(),
        ConnectionSettings::new(Some(60)).with_forward_stats(stats.clone()),
    )
    .await;
    authenticate(&mut conn, "@node", "secret").await;

    for hops in [0, MAX_FORWARD_HOPS + 1] {
        conn.send(marker(6, hops)).await.unwrap();
        let reply = recv(&mut conn).await;
        assert_eq!(error_of(&reply).code, ERR_INVALID, "hop count {hops}");
    }
    assert_eq!(stats.forwarded_sessions(), 0);

    conn.send(marker(7, MAX_FORWARD_HOPS)).await.unwrap();
    let reply = recv(&mut conn).await;
    assert_eq!(reply.opcode, Op::ForwardedSessionOk as u16);
    assert_eq!(reply.request_id, 7);
    assert_eq!(stats.forwarded_sessions(), 1);

    // Marked once per session.
    conn.send(marker(8, MAX_FORWARD_HOPS)).await.unwrap();
    assert_eq!(error_of(&recv(&mut conn).await).code, ERR_INVALID);

    drop(conn);
    tokio::time::timeout(Duration::from_secs(5), async {
        while stats.forwarded_sessions() != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("closing the session releases the gauge");
}

#[tokio::test]
async fn a_forwarded_session_never_forwards_again() {
    // A broker with forwarding enabled, reached through a forwarded session,
    // answers for a partition it does not own instead of forwarding again.
    let (owner_addr, mut owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = TcpStream::connect(addr).await.unwrap();
    let (server, peer) = listener.accept().await.unwrap();
    let stats = ConnectionStats::new();
    let conn_id = stats.add_connection(peer, Instant::now(), false);
    let topology: Arc<dyn ClientTopologySource> = gateway.topology.clone();
    tokio::spawn(handle_connection(
        server,
        Some(peer),
        gateway.broker.clone(),
        TcpStats::new(10),
        stats,
        conn_id,
        Some(node_auth()),
        None,
        gateway.settings.clone(),
        Some(topology),
        None,
        None,
    ));
    let mut conn = plain_conn(client);
    hello(&mut conn).await;
    authenticate(&mut conn, "@node", "secret").await;
    conn.send(marker(3, 1)).await.unwrap();
    assert_eq!(recv(&mut conn).await.opcode, Op::ForwardedSessionOk as u16);

    conn.send(publish_frame(4, "orders", b"x", true))
        .await
        .unwrap();
    let reply = recv(&mut conn).await;
    assert_eq!(reply.request_id, 4);
    // The only client route is the gateway itself, so there is no redirect
    // either: a plain not-owner answer goes back to the sending gateway.
    assert_ne!(reply.opcode, Op::PublishOk as u16);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), owners.recv())
            .await
            .is_err(),
        "a forwarded session opened an upstream session"
    );
}
