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
    broker::{Broker, BrokerConfig, StaticQueueOwnership, StreamOpenConfig, StreamOwnership},
    queue_engine::StromaEngine,
    stream::StreamDurability,
};
use fibril_metrics::{ConnectionStats, ForwardStats, TcpStats};
use fibril_protocol::v1::{
    Ack, AssignmentChanged, Auth, Deliver, DeliveryTag, ERR_CONFLICT, ERR_GATEWAY_TIMEOUT,
    ERR_INVALID, ERR_UNAVAILABLE, ErrorMsg, ForwardedSession, ForwardedSessionOk, Hello, HelloOk,
    MAX_FORWARD_HOPS, Op, PROTOCOL_V1, Partition, Publish, PublishOk, ReasonCode, ReconcileAction,
    ReconcileClient, ReconcilePolicy, ReconcileResult, ReconcileSubscription,
    ReconcileSubscriptionResult, ResumeIdentity, ResumeOutcome, StreamStart, Subscribe,
    SubscribeOk, SubscribeStream, TopologyOk,
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
        Broker::new_with_ownerships(
            engine,
            config,
            None,
            Arc::new(StaticQueueOwnership::new(HashSet::new())),
            Arc::new(GatewayStreams),
        )
    };
    (broker, dir)
}

/// Stream view of a gateway that owns no stream partition: topics named
/// `stream.*` are streams declared in coordination, as a cluster gateway
/// would see them.
#[derive(Debug)]
struct GatewayStreams;

impl StreamOwnership for GatewayStreams {
    fn owns_stream(&self, _: &str, _: Partition) -> bool {
        false
    }

    fn stream_open_config(&self, topic: &str) -> Option<StreamOpenConfig> {
        topic.starts_with("stream.").then(|| StreamOpenConfig {
            durability: StreamDurability::default(),
            retention: None,
        })
    }
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
    serve_owner_with(broker, ConnectionSettings::new(Some(60))).await
}

/// Like [`serve_owner`], with one settings object (and so one resume
/// registry) shared by every connection, as on a real broker.
async fn serve_owner_with(
    broker: Arc<Broker<StromaEngine>>,
    settings: ConnectionSettings,
) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        loop {
            let Ok((socket, peer)) = listener.accept().await else {
                return;
            };
            let broker = broker.clone();
            let settings = settings.clone();
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
                    settings,
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
    gateway_with_grace(owner_peer, limits, None).await
}

/// A gateway whose client sessions keep a reconnect grace of `grace_ms`,
/// which is also how long it retries a lost upstream session.
async fn gateway_with_grace(
    owner_peer: String,
    limits: RoutingLimits,
    grace_ms: Option<u64>,
) -> Gateway {
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
        .with_reconnect_grace_ms(grace_ms)
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
        self.client_with(None, None).await.0
    }

    /// A client connection with optional password auth on the gateway and an
    /// optional resume identity, after HELLO (and AUTH when `auth` is set).
    async fn client_with(
        &self,
        auth: Option<(&str, &str)>,
        resume: Option<ResumeIdentity>,
    ) -> (Conn, HelloOk) {
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
            auth.map(|(user, password)| StaticAuthHandler::new(user.into(), password.into())),
            None,
            self.settings.clone(),
            Some(topology),
            None,
            None,
        ));
        let mut conn = plain_conn(client);
        let hello_ok = hello_with(&mut conn, resume).await;
        if let Some((user, password)) = auth {
            authenticate(&mut conn, user, password).await;
        }
        (conn, hello_ok)
    }
}

async fn hello(conn: &mut Conn) {
    hello_with(conn, None).await;
}

async fn hello_with(conn: &mut Conn, resume: Option<ResumeIdentity>) -> HelloOk {
    conn.send(
        try_encode(
            Op::Hello,
            1,
            &Hello {
                client_name: "forwarding-test".into(),
                client_version: "0.1.0".into(),
                protocol_version: PROTOCOL_V1,
                resume,
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let frame = recv(conn).await;
    assert_eq!(frame.opcode, Op::HelloOk as u16);
    try_decode(&frame).unwrap()
}

fn identity_of(hello_ok: &HelloOk) -> ResumeIdentity {
    ResumeIdentity {
        owner_id: hello_ok.owner_id,
        client_id: hello_ok.client_id,
        resume_token: hello_ok.resume_token,
    }
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
    let (addr, mut rx) = scripted_owner_resuming(ResumeOutcome::Resumed).await;
    let (tx, plain) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        while let Some((conn, session, _)) = rx.recv().await {
            if tx.send((conn, session)).await.is_err() {
                return;
            }
        }
    });
    (addr, plain)
}

/// A scripted owner with one resume identity. A HELLO that presents it gets
/// `on_resume` as the outcome, any other gets `New`. The test also gets the
/// identity the HELLO presented.
async fn scripted_owner_resuming(
    on_resume: ResumeOutcome,
) -> (
    String,
    tokio::sync::mpsc::Receiver<(Conn, ForwardedSession, Option<ResumeIdentity>)>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let owner_id = Uuid::new_v4();
    let client_id = Uuid::new_v4();
    let resume_token = Uuid::new_v4();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let mut conn = plain_conn(socket);
            let hello = conn.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            let presented = try_decode::<Hello>(&hello).unwrap().resume;
            let outcome = if presented.is_some() {
                on_resume
            } else {
                ResumeOutcome::New
            };
            conn.send(
                try_encode(
                    Op::HelloOk,
                    hello.request_id,
                    &HelloOk {
                        protocol_version: PROTOCOL_V1,
                        owner_id,
                        client_id,
                        resume_token,
                        resume_outcome: outcome,
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
            if tx.send((conn, session, presented)).await.is_err() {
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

fn cohort_subscribe_frame(request_id: u64, partition: u32, member_id: Option<Uuid>) -> Frame {
    try_encode(
        Op::Subscribe,
        request_id,
        &Subscribe {
            topic: "jobs".into(),
            partition: Partition::new(partition),
            group: None,
            prefetch: 1,
            auto_ack: false,
            consumer_group: Some("workers".into()),
            consumer_target: Some(2),
            member_id,
        },
    )
    .unwrap()
}

/// Answer a forwarded subscribe as an owner would, echoing its fields.
async fn answer_subscribe(owner: &mut Conn, request: &Frame, owner_sub_id: u64) -> Subscribe {
    let body: Subscribe = try_decode(request).unwrap();
    owner
        .send(
            try_encode(
                Op::SubscribeOk,
                request.request_id,
                &SubscribeOk {
                    sub_id: owner_sub_id,
                    topic: body.topic.clone(),
                    group: body.group.clone(),
                    partition: body.partition,
                    prefetch: body.prefetch,
                    consumer_group: body.consumer_group.clone(),
                    consumer_target: body.consumer_target,
                    member_id: body.member_id,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    body
}

#[tokio::test]
async fn cohort_subscribes_carry_one_member_id_and_assignments_reach_the_client() {
    let (owner_addr, mut owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;

    // No member id from the client: the gateway settles one and sends it.
    client
        .send(cohort_subscribe_frame(30, 0, None))
        .await
        .unwrap();
    let (mut owner, _) = owners.recv().await.unwrap();
    let first = next_upstream_request(&mut owner).await;
    assert_eq!(first.opcode, Op::Subscribe as u16);
    let body = answer_subscribe(&mut owner, &first, 100).await;
    let member = body.member_id.expect("the owner never mints a member id");
    assert_eq!(body.consumer_group.as_deref(), Some("workers"));
    assert_eq!(body.consumer_target, Some(2));
    let ok: SubscribeOk = try_decode(&recv(&mut client).await).unwrap();
    assert_eq!(ok.member_id, Some(member));
    assert!(ok.sub_id >= FORWARDED_SUB_ID_BASE);

    // The next partition of the same session uses the same member id.
    client
        .send(cohort_subscribe_frame(31, 1, None))
        .await
        .unwrap();
    let second = next_upstream_request(&mut owner).await;
    let body = answer_subscribe(&mut owner, &second, 101).await;
    assert_eq!(body.member_id, Some(member));
    let _: SubscribeOk = try_decode(&recv(&mut client).await).unwrap();

    // A different id on the same session conflicts, as on a direct broker.
    client
        .send(cohort_subscribe_frame(32, 2, Some(Uuid::new_v4())))
        .await
        .unwrap();
    let reply = recv(&mut client).await;
    assert_eq!(reply.opcode, Op::SubscribeErr as u16);
    assert_eq!(error_of(&reply).code, ERR_CONFLICT);

    // The owner's rebalance notice reaches the client as it is.
    let notice = AssignmentChanged {
        topic: "jobs".into(),
        group: None,
        consumer_group: "workers".into(),
        generation: 3,
        assigned: vec![Partition::new(1)],
        added: Vec::new(),
        revoked: vec![Partition::new(0)],
    };
    owner
        .send(try_encode(Op::AssignmentChanged, 900, &notice).unwrap())
        .await
        .unwrap();
    let pushed = recv(&mut client).await;
    assert_eq!(pushed.opcode, Op::AssignmentChanged as u16);
    let pushed: AssignmentChanged = try_decode(&pushed).unwrap();
    assert_eq!(pushed.generation, 3);
    assert_eq!(pushed.revoked, vec![Partition::new(0)]);
}

#[tokio::test]
async fn cohort_member_id_carried_by_the_client_is_kept() {
    let (owner_addr, mut owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;
    let member = Uuid::new_v4();
    client
        .send(cohort_subscribe_frame(40, 0, Some(member)))
        .await
        .unwrap();
    let (mut owner, _) = owners.recv().await.unwrap();
    let request = next_upstream_request(&mut owner).await;
    let body = answer_subscribe(&mut owner, &request, 7).await;
    assert_eq!(body.member_id, Some(member));
    let ok: SubscribeOk = try_decode(&recv(&mut client).await).unwrap();
    assert_eq!(ok.member_id, Some(member));
}

#[tokio::test]
async fn exclusive_group_through_a_gateway_delivers_from_a_real_owner() {
    let (owner_broker, _owner_dir) = open_broker(true).await;
    let owner_addr = serve_owner(owner_broker.clone()).await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;

    client
        .send(cohort_subscribe_frame(50, 0, None))
        .await
        .unwrap();
    let reply = loop {
        let frame = recv(&mut client).await;
        // The initial assignment push may come first.
        if frame.opcode != Op::AssignmentChanged as u16 {
            break frame;
        }
    };
    assert_eq!(
        reply.opcode,
        Op::SubscribeOk as u16,
        "{:?}",
        try_decode::<ErrorMsg>(&reply)
    );
    let ok: SubscribeOk = try_decode(&reply).unwrap();
    let member = ok.member_id.expect("member id");

    // The owner registered the member the gateway chose.
    let coverage = owner_broker.local_cohort_coverage();
    assert!(
        coverage.iter().any(|cohort| cohort
            .members
            .iter()
            .any(|m| m.member == member.to_string())),
        "{coverage:?}"
    );

    client
        .send(publish_frame(51, "jobs", b"one", true))
        .await
        .unwrap();
    let mut confirmed = false;
    let mut delivered = None;
    while !confirmed || delivered.is_none() {
        let frame = recv(&mut client).await;
        match frame.opcode {
            x if x == Op::PublishOk as u16 => confirmed = true,
            x if x == Op::Deliver as u16 => delivered = Some(wire::decode_deliver(&frame).unwrap()),
            x if x == Op::AssignmentChanged as u16 => {}
            other => panic!("unexpected opcode {other}"),
        }
    }
    let delivered = delivered.unwrap();
    assert_eq!(delivered.sub_id, ok.sub_id);
    assert_eq!(delivered.payload, b"one");
}

fn stream_subscribe_frame(
    request_id: u64,
    durable_name: Option<&str>,
    filter: Vec<(String, String)>,
) -> Frame {
    try_encode(
        Op::SubscribeStream,
        request_id,
        &SubscribeStream {
            topic: "stream.events".into(),
            partition: Partition::new(0),
            durable_name: durable_name.map(str::to_string),
            start: StreamStart::Earliest,
            filter,
            prefetch: 16,
            auto_ack: false,
        },
    )
    .unwrap()
}

fn stream_publish_frame(request_id: u64, payload: &[u8], kind: &str) -> Frame {
    try_encode(
        Op::Publish,
        request_id,
        &Publish {
            topic: "stream.events".into(),
            partition: Partition::new(0),
            group: None,
            require_confirm: true,
            content_type: None,
            headers: HashMap::from([("kind".to_string(), kind.to_string())]),
            payload: payload.to_vec(),
            published: unix_millis(),
            partition_key: None,
            partitioning_version: 0,
            ttl_ms: None,
        },
    )
    .unwrap()
}

/// Collect `count` stream deliveries.
async fn stream_deliveries(conn: &mut Conn, count: usize) -> Vec<Deliver> {
    let mut out = Vec::new();
    while out.len() < count {
        let frame = recv(conn).await;
        assert_eq!(frame.opcode, Op::Deliver as u16, "opcode {}", frame.opcode);
        out.push(wire::decode_deliver(&frame).unwrap());
    }
    out
}

async fn stream_owner() -> (Arc<Broker<StromaEngine>>, TempDir, String) {
    let (owner_broker, dir) = open_broker(true).await;
    owner_broker
        .get_or_open_stream("stream.events", 0, StreamDurability::default(), None)
        .await
        .unwrap();
    let owner_addr = serve_owner(owner_broker.clone()).await;
    (owner_broker, dir, owner_addr)
}

#[tokio::test]
async fn stream_publish_subscribe_filter_and_cursor_commit_through_a_gateway() {
    let (_owner_broker, _owner_dir, owner_addr) = stream_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;

    // Confirmed stream publishes answer with the owner's offsets.
    let mut producer = gateway.client().await;
    let mut offsets = Vec::new();
    for (id, (payload, kind)) in [(b"a1", "a"), (b"b1", "b"), (b"a2", "a")]
        .into_iter()
        .enumerate()
    {
        producer
            .send(stream_publish_frame(60 + id as u64, payload, kind))
            .await
            .unwrap();
        let reply = recv(&mut producer).await;
        assert_eq!(
            reply.opcode,
            Op::PublishOk as u16,
            "{:?}",
            try_decode::<ErrorMsg>(&reply)
        );
        offsets.push(try_decode::<PublishOk>(&reply).unwrap().offset);
    }
    assert!(offsets.windows(2).all(|pair| pair[0] < pair[1]));

    // A durable subscription from the earliest record.
    let mut reader = gateway.client().await;
    reader
        .send(stream_subscribe_frame(70, Some("reader"), Vec::new()))
        .await
        .unwrap();
    let reply = recv(&mut reader).await;
    assert_eq!(
        reply.opcode,
        Op::SubscribeOk as u16,
        "{:?}",
        try_decode::<ErrorMsg>(&reply)
    );
    let ok: SubscribeOk = try_decode(&reply).unwrap();
    assert!(ok.sub_id >= FORWARDED_SUB_ID_BASE);
    let got = stream_deliveries(&mut reader, 3).await;
    assert!(got.iter().all(|deliver| deliver.sub_id == ok.sub_id));
    assert_eq!(
        got.iter().map(|d| d.offset).collect::<Vec<_>>(),
        offsets,
        "per-partition order is kept"
    );

    // Commit the cursor at the second record, through the gateway.
    reader
        .send(
            try_encode(
                Op::Ack,
                71,
                &Ack {
                    topic: "stream.events".into(),
                    group: None,
                    partition: Partition::new(0),
                    tags: vec![got[1].delivery_tag],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(reader);
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Resuming the durable name starts after the committed record.
    let mut resumed = gateway.client().await;
    resumed
        .send(stream_subscribe_frame(72, Some("reader"), Vec::new()))
        .await
        .unwrap();
    assert_eq!(recv(&mut resumed).await.opcode, Op::SubscribeOk as u16);
    let after = stream_deliveries(&mut resumed, 1).await;
    assert_eq!(after[0].offset, offsets[2]);

    // A filtered ephemeral subscription sees only matching records.
    let mut filtered = gateway.client().await;
    filtered
        .send(stream_subscribe_frame(
            73,
            None,
            vec![("kind".into(), "b".into())],
        ))
        .await
        .unwrap();
    assert_eq!(recv(&mut filtered).await.opcode, Op::SubscribeOk as u16);
    let only = stream_deliveries(&mut filtered, 1).await;
    assert_eq!(only[0].payload, b"b1");
    tokio::time::sleep(Duration::from_millis(300)).await;
    filtered
        .send(try_encode(Op::Ping, 74, &()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        recv(&mut filtered).await.opcode,
        Op::Pong as u16,
        "no other record passed the filter"
    );
}

#[tokio::test]
async fn a_second_stream_subscribe_replaces_the_first() {
    let (_owner_broker, _owner_dir, owner_addr) = stream_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;
    client
        .send(stream_subscribe_frame(80, None, Vec::new()))
        .await
        .unwrap();
    let first: SubscribeOk = try_decode(&recv(&mut client).await).unwrap();
    client
        .send(stream_subscribe_frame(81, None, Vec::new()))
        .await
        .unwrap();
    let second: SubscribeOk = try_decode(&recv(&mut client).await).unwrap();
    assert_ne!(first.sub_id, second.sub_id);

    client
        .send(stream_publish_frame(82, b"x", "a"))
        .await
        .unwrap();
    let mut confirmed = false;
    let mut delivered = Vec::new();
    let deadline = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < deadline {
        let Ok(Some(Ok(frame))) =
            tokio::time::timeout(Duration::from_millis(200), client.next()).await
        else {
            continue;
        };
        match frame.opcode {
            x if x == Op::PublishOk as u16 => confirmed = true,
            x if x == Op::Deliver as u16 => delivered.push(wire::decode_deliver(&frame).unwrap()),
            _ => {}
        }
    }
    assert!(confirmed);
    assert_eq!(
        delivered.len(),
        1,
        "only the replacing subscription delivers"
    );
    assert_eq!(delivered[0].sub_id, second.sub_id);
}

fn deliver_frame(owner_sub_id: u64, epoch: u64, payload: &[u8]) -> Frame {
    wire::encode_deliver(
        500 + epoch,
        &Deliver {
            sub_id: owner_sub_id,
            topic: "jobs".into(),
            group: None,
            partition: Partition::new(0),
            offset: epoch,
            delivery_tag: DeliveryTag { epoch },
            published: 0,
            publish_received: 0,
            content_type: None,
            headers: HashMap::new(),
            payload: payload.to_vec(),
        },
    )
    .unwrap()
}

fn reconcile_reply(request: &Frame, action: ReconcileAction, kept: bool) -> Frame {
    let body: ReconcileClient = try_decode(request).unwrap();
    try_encode(
        Op::ReconcileResult,
        request.request_id,
        &ReconcileResult {
            subscriptions: body
                .subscriptions
                .into_iter()
                .map(|sub| ReconcileSubscriptionResult {
                    client: Some(sub.clone()),
                    server: kept.then_some(sub),
                    action,
                    code: if kept {
                        ReasonCode::Matched
                    } else {
                        ReasonCode::Recreate
                    },
                    reason: String::new(),
                })
                .collect(),
        },
    )
    .unwrap()
}

/// A forwarded subscription on a scripted owner. Returns the owner
/// connection and the client-facing subscription id.
async fn subscribed_through(
    client: &mut Conn,
    owners: &mut tokio::sync::mpsc::Receiver<(Conn, ForwardedSession, Option<ResumeIdentity>)>,
    owner_sub_id: u64,
) -> (Conn, u64) {
    client.send(subscribe_frame(90, "jobs", 4)).await.unwrap();
    let (mut owner, _, presented) = owners.recv().await.unwrap();
    assert!(
        presented.is_none(),
        "a new upstream session resumes nothing"
    );
    let request = next_upstream_request(&mut owner).await;
    answer_subscribe(&mut owner, &request, owner_sub_id).await;
    let ok: SubscribeOk = try_decode(&recv(client).await).unwrap();
    (owner, ok.sub_id)
}

#[tokio::test]
async fn a_lost_upstream_resumes_and_keeps_its_subscriptions() {
    let (owner_addr, mut owners) = scripted_owner_resuming(ResumeOutcome::Resumed).await;
    let gateway = gateway_with_grace(owner_addr, RoutingLimits::default(), Some(5_000)).await;
    let mut client = gateway.client().await;
    let (mut owner, client_sub_id) = subscribed_through(&mut client, &mut owners, 7).await;

    // A publish reaches the owner, then the link breaks before its answer.
    client
        .send(publish_frame(2, "jobs", b"lost", true))
        .await
        .unwrap();
    let _ = next_upstream_request(&mut owner).await;
    drop(owner);
    let reply = recv(&mut client).await;
    assert_eq!(reply.request_id, 2);
    assert_eq!(
        error_of(&reply).code,
        ERR_GATEWAY_TIMEOUT,
        "a sent request has an unknown outcome"
    );

    // The gateway comes back with the owner's resume identity and asks the
    // owner to keep the subscription it holds.
    let (mut owner, _, presented) = owners.recv().await.unwrap();
    assert!(
        presented.is_some(),
        "the gateway presents its resume identity"
    );
    let reconcile = next_upstream_request(&mut owner).await;
    assert_eq!(reconcile.opcode, Op::ReconcileClient as u16);
    let body: ReconcileClient = try_decode(&reconcile).unwrap();
    assert_eq!(body.subscriptions.len(), 1);
    assert_eq!(body.subscriptions[0].sub_id, 7);
    owner
        .send(reconcile_reply(&reconcile, ReconcileAction::Keep, true))
        .await
        .unwrap();

    // Deliveries keep flowing under the same client-facing id, and a settle
    // reaches the resumed session.
    owner.send(deliver_frame(7, 42, b"after")).await.unwrap();
    let deliver = recv(&mut client).await;
    assert_eq!(deliver.opcode, Op::Deliver as u16);
    let deliver = wire::decode_deliver(&deliver).unwrap();
    assert_eq!(deliver.sub_id, client_sub_id);
    assert_eq!(deliver.delivery_tag, DeliveryTag { epoch: 42 });
    client
        .send(
            try_encode(
                Op::Ack,
                3,
                &Ack {
                    topic: "jobs".into(),
                    group: None,
                    partition: Partition::new(0),
                    tags: vec![deliver.delivery_tag],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let ack = next_upstream_request(&mut owner).await;
    assert_eq!(ack.opcode, Op::Ack as u16);
    assert_eq!(gateway.stats.upstream_resumes(true), 1);
}

#[tokio::test]
async fn subscriptions_the_resumed_owner_no_longer_has_close_for_the_client() {
    let (owner_addr, mut owners) = scripted_owner_resuming(ResumeOutcome::Resumed).await;
    let gateway = gateway_with_grace(owner_addr, RoutingLimits::default(), Some(5_000)).await;
    let mut client = gateway.client().await;
    let (owner, client_sub_id) = subscribed_through(&mut client, &mut owners, 7).await;
    drop(owner);

    let (mut owner, _, _) = owners.recv().await.unwrap();
    let reconcile = next_upstream_request(&mut owner).await;
    owner
        .send(reconcile_reply(
            &reconcile,
            ReconcileAction::RecreateClientSide,
            false,
        ))
        .await
        .unwrap();
    let closed = recv(&mut client).await;
    assert_eq!(closed.opcode, Op::SubscriptionClosed as u16);
    let closed = wire::decode_subscription_closed(&closed).unwrap();
    assert_eq!(closed.sub_id, client_sub_id);
    assert_eq!(closed.code, ReasonCode::OwnerMoved);
}

#[tokio::test]
async fn an_owner_that_lost_the_session_closes_subscriptions_for_resubscribe() {
    let (owner_addr, mut owners) = scripted_owner_resuming(ResumeOutcome::ResumeNotFound).await;
    let gateway = gateway_with_grace(owner_addr, RoutingLimits::default(), Some(5_000)).await;
    let mut client = gateway.client().await;
    let (owner, client_sub_id) = subscribed_through(&mut client, &mut owners, 7).await;
    drop(owner);

    let (_owner, _, presented) = owners.recv().await.unwrap();
    assert!(presented.is_some());
    let closed = recv(&mut client).await;
    assert_eq!(closed.opcode, Op::SubscriptionClosed as u16);
    let closed = wire::decode_subscription_closed(&closed).unwrap();
    assert_eq!(closed.sub_id, client_sub_id);
    assert_eq!(closed.code, ReasonCode::OwnerMoved);
    assert_eq!(gateway.stats.upstream_resumes(false), 1);
}

#[tokio::test]
async fn without_a_resume_window_a_lost_upstream_closes_its_subscriptions() {
    let (owner_addr, mut owners) = scripted_owner_resuming(ResumeOutcome::Resumed).await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;
    let (owner, client_sub_id) = subscribed_through(&mut client, &mut owners, 7).await;
    drop(owner);
    let closed = recv(&mut client).await;
    assert_eq!(closed.opcode, Op::SubscriptionClosed as u16);
    assert_eq!(
        wire::decode_subscription_closed(&closed).unwrap().sub_id,
        client_sub_id
    );
}

#[tokio::test]
async fn a_client_resuming_on_the_gateway_keeps_forwarded_subscriptions() {
    let (owner_broker, _owner_dir) = open_broker(true).await;
    let owner_addr = serve_owner(owner_broker.clone()).await;
    let gateway = gateway_with_grace(owner_addr, RoutingLimits::default(), Some(10_000)).await;

    // The gateway requires a password, so the resumed connection
    // authenticates again, as every SDK does.
    let (mut client, hello_ok) = gateway.client_with(Some(("alice", "pw")), None).await;
    client.send(subscribe_frame(1, "jobs", 2)).await.unwrap();
    let ok: SubscribeOk = try_decode(&recv(&mut client).await).unwrap();
    client
        .send(publish_frame(2, "jobs", b"held", true))
        .await
        .unwrap();
    let mut held = None;
    let mut confirmed = false;
    while held.is_none() || !confirmed {
        let frame = recv(&mut client).await;
        match frame.opcode {
            x if x == Op::PublishOk as u16 => confirmed = true,
            x if x == Op::Deliver as u16 => held = Some(wire::decode_deliver(&frame).unwrap()),
            other => panic!("unexpected opcode {other}"),
        }
    }
    let held = held.unwrap();
    drop(client);

    let (mut client, resumed) = gateway
        .client_with(Some(("alice", "pw")), Some(identity_of(&hello_ok)))
        .await;
    assert_eq!(resumed.resume_outcome, ResumeOutcome::Resumed);
    let subscription = ReconcileSubscription {
        sub_id: ok.sub_id,
        topic: "jobs".into(),
        group: None,
        partition: Partition::new(0),
        auto_ack: false,
        prefetch: 2,
        consumer_group: None,
        consumer_target: None,
        member_id: None,
    };
    client
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![subscription],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let result: ReconcileResult = try_decode(&recv(&mut client).await).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    assert_eq!(result.subscriptions[0].action, ReconcileAction::Keep);
    assert_eq!(result.subscriptions[0].code, ReasonCode::Matched);

    // The delivery held across the reconnect still settles, and the next
    // one arrives on the same subscription.
    client
        .send(
            try_encode(
                Op::Ack,
                4,
                &Ack {
                    topic: "jobs".into(),
                    group: None,
                    partition: Partition::new(0),
                    tags: vec![held.delivery_tag],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    client
        .send(publish_frame(5, "jobs", b"next", true))
        .await
        .unwrap();
    let mut next = None;
    while next.is_none() {
        let frame = recv(&mut client).await;
        if frame.opcode == Op::Deliver as u16 {
            next = Some(wire::decode_deliver(&frame).unwrap());
        }
    }
    let next = next.unwrap();
    assert_eq!(next.sub_id, ok.sub_id);
    assert_eq!(next.payload, b"next");
}

#[tokio::test]
async fn restore_policy_resubscribes_a_forwarded_subscription_on_the_owner() {
    let (owner_broker, _owner_dir) = open_broker(true).await;
    let owner_addr = serve_owner(owner_broker.clone()).await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    // A fresh session, as after a public broker restart, that still holds a
    // subscription client-side.
    let mut client = gateway.client().await;
    let subscription = ReconcileSubscription {
        sub_id: 12345,
        topic: "jobs".into(),
        group: None,
        partition: Partition::new(0),
        auto_ack: false,
        prefetch: 2,
        consumer_group: Some("workers".into()),
        consumer_target: None,
        member_id: Some(Uuid::new_v4()),
    };
    client
        .send(
            try_encode(
                Op::ReconcileClient,
                1,
                &ReconcileClient {
                    policy: ReconcilePolicy::Restore,
                    subscriptions: vec![subscription.clone()],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let result = loop {
        let frame = recv(&mut client).await;
        if frame.opcode == Op::ReconcileResult as u16 {
            break try_decode::<ReconcileResult>(&frame).unwrap();
        }
    };
    let item = &result.subscriptions[0];
    assert_eq!(item.action, ReconcileAction::Keep);
    assert_eq!(item.code, ReasonCode::ServerRestored);
    let server = item.server.clone().unwrap();
    assert!(server.sub_id >= FORWARDED_SUB_ID_BASE);
    assert_eq!(server.member_id, subscription.member_id);

    client
        .send(publish_frame(2, "jobs", b"restored", true))
        .await
        .unwrap();
    let mut delivered = None;
    while delivered.is_none() {
        let frame = recv(&mut client).await;
        if frame.opcode == Op::Deliver as u16 {
            delivered = Some(wire::decode_deliver(&frame).unwrap());
        }
    }
    assert_eq!(delivered.unwrap().sub_id, server.sub_id);
}

#[tokio::test]
async fn conservative_reconcile_recreates_a_missing_forwarded_cohort_subscription() {
    let (owner_addr, _owners) = scripted_owner().await;
    let gateway = gateway(owner_addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;
    let subscription = ReconcileSubscription {
        sub_id: 9,
        topic: "jobs".into(),
        group: None,
        partition: Partition::new(0),
        auto_ack: false,
        prefetch: 2,
        consumer_group: Some("workers".into()),
        consumer_target: None,
        member_id: Some(Uuid::new_v4()),
    };
    client
        .send(
            try_encode(
                Op::ReconcileClient,
                1,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![subscription],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let result: ReconcileResult = try_decode(&recv(&mut client).await).unwrap();
    assert_eq!(
        result.subscriptions[0].action,
        ReconcileAction::RecreateClientSide
    );
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

/// Open a node session on `addr` with an optional resume identity and mark
/// it forwarded. Returns the connection and the owner's HELLO answer.
async fn node_session(
    addr: &str,
    resume: Option<ResumeIdentity>,
    release_on_close: bool,
) -> (Conn, HelloOk) {
    let mut conn = plain_conn(TcpStream::connect(addr).await.unwrap());
    let hello_ok = hello_with(&mut conn, resume).await;
    authenticate(&mut conn, "@node", "secret").await;
    let marker = wire::encode_forwarded_session(
        5,
        &ForwardedSession {
            origin_node: "gateway-g".into(),
            hop_count: 1,
            acting_user: String::new(),
            client_id: Uuid::new_v4(),
            client_conn_id: Uuid::new_v4(),
            client_addr: None,
            release_on_close,
        },
    )
    .unwrap();
    conn.send(marker).await.unwrap();
    let ok = recv(&mut conn).await;
    assert_eq!(ok.opcode, Op::ForwardedSessionOk as u16);
    let ok = wire::decode_forwarded_session_ok(&ok).unwrap();
    assert_eq!(
        ok.forwarding_version,
        fibril_protocol::v1::FORWARDING_PROTOCOL_VERSION
    );
    (conn, hello_ok)
}

#[tokio::test]
async fn release_on_close_ends_the_owner_session_on_an_orderly_close() {
    let (broker, _dir) = open_broker(true).await;
    let addr = serve_owner_with(
        broker,
        ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(30_000)),
    )
    .await;

    // Without the option an orderly close keeps the session for its grace,
    // and the same node resumes it, authenticating again on the new
    // connection.
    let (mut conn, hello_ok) = node_session(&addr, None, false).await;
    conn.close().await.unwrap();
    drop(conn);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (_resumed, again) = node_session(&addr, Some(identity_of(&hello_ok)), false).await;
    assert_eq!(again.resume_outcome, ResumeOutcome::Resumed);

    // With it the owner releases the session at once.
    let (mut conn, hello_ok) = node_session(&addr, None, true).await;
    conn.close().await.unwrap();
    drop(conn);
    tokio::time::sleep(Duration::from_millis(200)).await;
    let (_fresh, again) = node_session(&addr, Some(identity_of(&hello_ok)), false).await;
    assert_eq!(again.resume_outcome, ResumeOutcome::ResumeNotFound);
}

#[tokio::test]
async fn gateway_falls_back_when_an_owner_refuses_release_on_close() {
    // An owner that only reads the FFS1 marker body, as before forwarding
    // version 2.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let (tx, mut markers) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut conn = plain_conn(socket);
        let hello = conn.next().await.unwrap().unwrap();
        conn.send(
            try_encode(
                Op::HelloOk,
                hello.request_id,
                &HelloOk {
                    protocol_version: PROTOCOL_V1,
                    owner_id: Uuid::new_v4(),
                    client_id: Uuid::new_v4(),
                    resume_token: Uuid::new_v4(),
                    resume_outcome: ResumeOutcome::New,
                    server_name: "old".into(),
                    compliance: fibril_protocol::v1::COMPLIANCE_STRING.into(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
        let auth = conn.next().await.unwrap().unwrap();
        conn.send(try_encode(Op::AuthOk, auth.request_id, &()).unwrap())
            .await
            .unwrap();
        loop {
            let marker = conn.next().await.unwrap().unwrap();
            let old_format = &marker.payload[..4] == b"FFS1";
            tx.send(old_format).await.unwrap();
            if !old_format {
                conn.send(
                    try_encode(
                        Op::Error,
                        marker.request_id,
                        &ErrorMsg {
                            code: 400,
                            message: "malformed frame".into(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
                continue;
            }
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
            let publish = next_upstream_request(&mut conn).await;
            conn.send(
                wire::encode_publish_ok(publish.request_id, &PublishOk { offset: 3 }).unwrap(),
            )
            .await
            .unwrap();
            let _ = conn.next().await;
            return;
        }
    });
    let gateway = gateway(addr, RoutingLimits::default()).await;
    let mut client = gateway.client().await;
    client
        .send(publish_frame(1, "orders", b"x", true))
        .await
        .unwrap();
    let reply = recv(&mut client).await;
    assert_eq!(reply.opcode, Op::PublishOk as u16);
    assert_eq!(
        markers.recv().await,
        Some(false),
        "the option is tried first"
    );
    assert_eq!(markers.recv().await, Some(true), "then the old body");
}
