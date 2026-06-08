use std::{collections::HashMap, sync::Arc, time::Duration, time::Instant};

use bytes::Bytes;
use fibril_broker::{
    broker::{Broker, BrokerConfig, QueueEvictionAttempt},
    queue_engine::{EvictOutcome, GlobalDLQ, QueueEngine, StromaEngine},
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_protocol::v1::{
    Ack, ContentType, DeclareQueue, Deliver, ErrorMsg, Hello, HelloOk, Nack, Op, PROTOCOL_V1,
    Publish, PublishDelayed, QueueDlqPolicy, ResumeIdentity, ResumeOutcome, Subscribe,
    frame::{Frame, ProtoCodec},
    handler::{ConnectionSettings, handle_connection},
    helper::{try_decode, try_encode},
};
use fibril_util::{StaticAuthHandler, unix_millis};
use futures::{SinkExt, StreamExt};
use stroma_core::{KeratinConfig, SnapshotConfig, StromaKeratinConfig, TempDir};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use uuid::Uuid;

async fn open_test_engine() -> (StromaEngine, TempDir) {
    let dir = TempDir {
        root: std::env::current_dir()
            .unwrap()
            .join("test_data")
            .join(format!("protocol_handler_test-{}", Uuid::now_v7())),
    };
    std::fs::create_dir_all(&dir.root).unwrap();
    let engine = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    (engine, dir)
}

async fn open_test_broker() -> (Arc<Broker<StromaEngine>>, TempDir) {
    let (engine, dir) = open_test_engine().await;
    let broker = Broker::new(
        engine,
        BrokerConfig {
            inflight_ttl_ms: 2_000,
            expiry_poll_min_ms: 50,
            expiry_batch_max: 100,
            delivery_poll_max_ms: 50,
            queue_idle_evict_after_ms: None,
            queue_idle_sweep_interval_ms: 60_000,
        },
        None,
    );

    (broker, dir)
}

async fn open_protocol_connection() -> (
    Framed<TcpStream, ProtoCodec>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    TempDir,
) {
    let (framed, server_task, dir, _broker) =
        open_protocol_connection_with_settings(ConnectionSettings::new(Some(60))).await;
    (framed, server_task, dir)
}

async fn open_protocol_connection_with_settings(
    settings: ConnectionSettings,
) -> (
    Framed<TcpStream, ProtoCodec>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    TempDir,
    Arc<Broker<StromaEngine>>,
) {
    let (broker, dir) = open_test_broker().await;
    open_protocol_connection_for_broker(settings, broker, dir).await
}

async fn open_protocol_connection_for_broker(
    settings: ConnectionSettings,
    broker: Arc<Broker<StromaEngine>>,
    dir: TempDir,
) -> (
    Framed<TcpStream, ProtoCodec>,
    tokio::task::JoinHandle<anyhow::Result<()>>,
    TempDir,
    Arc<Broker<StromaEngine>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client = TcpStream::connect(addr).await.unwrap();
    let (server, peer) = listener.accept().await.unwrap();
    let tcp_stats = TcpStats::new(10);
    let connection_stats = ConnectionStats::new();
    let conn_id = connection_stats.add_connection(peer, Instant::now(), false);

    let server_task = tokio::spawn(handle_connection(
        server,
        broker.clone(),
        tcp_stats,
        connection_stats,
        conn_id,
        None::<StaticAuthHandler>,
        settings,
    ));

    (Framed::new(client, ProtoCodec), server_task, dir, broker)
}

async fn recv_frame(framed: &mut Framed<TcpStream, ProtoCodec>) -> Frame {
    tokio::time::timeout(Duration::from_secs(2), framed.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn handshake(framed: &mut Framed<TcpStream, ProtoCodec>) {
    framed
        .send(
            try_encode(
                Op::Hello,
                1,
                &Hello {
                    client_name: "protocol-test".into(),
                    client_version: "0.1.0".into(),
                    protocol_version: PROTOCOL_V1,
                    resume: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::HelloOk as u16);
    let hello_ok: HelloOk = try_decode(&frame).unwrap();
    assert_eq!(hello_ok.protocol_version, PROTOCOL_V1);
    assert_eq!(hello_ok.resume_outcome, ResumeOutcome::New);
}

async fn handshake_with_resume(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    resume: Option<ResumeIdentity>,
) -> HelloOk {
    framed
        .send(
            try_encode(
                Op::Hello,
                1,
                &Hello {
                    client_name: "protocol-test".into(),
                    client_version: "0.1.0".into(),
                    protocol_version: PROTOCOL_V1,
                    resume,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::HelloOk as u16);
    try_decode(&frame).unwrap()
}

async fn assert_error_frame(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    request_id: u64,
    code: u16,
) {
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::Error as u16);
    assert_eq!(frame.request_id, request_id);
    let err: ErrorMsg = try_decode(&frame).unwrap();
    assert_eq!(err.code, code);
    assert!(!err.message.is_empty());
}

async fn assert_connection_still_responds(framed: &mut Framed<TcpStream, ProtoCodec>) {
    framed
        .send(try_encode(Op::Ping, 99, &()).unwrap())
        .await
        .unwrap();
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::Pong as u16);
    assert_eq!(frame.request_id, 99);
}

async fn framed_subscribe(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    request_id: u64,
    topic: &str,
    group: Option<&str>,
    auto_ack: bool,
) {
    framed
        .send(
            try_encode(
                Op::Subscribe,
                request_id,
                &Subscribe {
                    topic: topic.into(),
                    group: group.map(str::to_string),
                    prefetch: 1,
                    auto_ack,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(framed).await.opcode, Op::SubscribeOk as u16);
}

async fn framed_publish(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    request_id: u64,
    topic: &str,
    group: Option<&str>,
    payload: &[u8],
) {
    framed
        .send(
            try_encode(
                Op::Publish,
                request_id,
                &Publish {
                    topic: topic.into(),
                    partition: 0,
                    group: group.map(str::to_string),
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: payload.to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::PublishOk as u16);
    assert_eq!(frame.request_id, request_id);
}

#[tokio::test]
async fn hello_can_resume_with_owner_scoped_identity() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    assert_eq!(first_ok.resume_outcome, ResumeOutcome::New);
    let resume = ResumeIdentity {
        owner_id: first_ok.owner_id,
        client_id: first_ok.client_id,
        resume_token: first_ok.resume_token,
    };
    drop(first);
    first_task.await.unwrap().unwrap();

    let (mut second, second_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;

    let second_ok = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(second_ok.resume_outcome, ResumeOutcome::Resumed);
    assert_eq!(second_ok.client_id, first_ok.client_id);
    assert_eq!(second_ok.owner_id, first_ok.owner_id);
    assert_eq!(second_ok.resume_token, first_ok.resume_token);

    drop(second);
    second_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconnect_grace_accepts_late_ack_after_resume() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(100));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    framed_subscribe(&mut first, 2, "grace.ack", None, false).await;
    framed_publish(&mut first, 3, "grace.ack", None, b"ack-after-resume").await;
    let delivered = recv_delivery_for_topic(&mut first, "grace.ack").await;

    let resume = ResumeIdentity {
        owner_id: first_ok.owner_id,
        client_id: first_ok.client_id,
        resume_token: first_ok.resume_token,
    };
    drop(first);
    first_task.await.unwrap().unwrap();

    let (mut second, second_task, dir, broker) =
        open_protocol_connection_for_broker(settings.clone(), broker, dir).await;
    let second_ok = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(second_ok.resume_outcome, ResumeOutcome::Resumed);

    second
        .send(
            try_encode(
                Op::Ack,
                2,
                &Ack {
                    topic: "grace.ack".into(),
                    group: None,
                    partition: 0,
                    tags: vec![delivered.delivery_tag],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    broker.wait_for_pending_settles().await;

    drop(second);
    second_task.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (mut third, third_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;
    handshake(&mut third).await;
    framed_subscribe(&mut third, 2, "grace.ack", None, false).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), third.next())
            .await
            .is_err()
    );

    drop(third);
    third_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconnect_grace_expiry_requeues_unsettled_inflight() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(100));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    handshake(&mut first).await;
    framed_subscribe(&mut first, 2, "grace.requeue", None, false).await;
    framed_publish(
        &mut first,
        3,
        "grace.requeue",
        None,
        b"requeued-after-grace",
    )
    .await;
    let delivered = recv_delivery_for_topic(&mut first, "grace.requeue").await;
    assert_eq!(delivered.payload, b"requeued-after-grace".to_vec());

    drop(first);
    first_task.await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;

    let (mut second, second_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;
    handshake(&mut second).await;
    framed_subscribe(&mut second, 2, "grace.requeue", None, false).await;
    let redelivered = recv_delivery_for_topic(&mut second, "grace.requeue").await;
    assert_eq!(redelivered.payload, b"requeued-after-grace".to_vec());

    drop(second);
    second_task.await.unwrap().unwrap();
}

async fn recv_delivery_for_topic(
    framed: &mut Framed<TcpStream, ProtoCodec>,
    topic: &str,
) -> Deliver {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = recv_frame(framed).await;
            if frame.opcode == Op::Deliver as u16 {
                let delivered: Deliver = try_decode(&frame).unwrap();
                if delivered.topic == topic {
                    break delivered;
                }
            }
        }
    })
    .await
    .unwrap()
}

async fn wait_for_queue_idle(broker: &Broker<StromaEngine>, topic: &str, group: Option<&str>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if broker
                .queue_activity_snapshot(topic, group)
                .is_some_and(|snapshot| {
                    snapshot.active_publishers == 0
                        && snapshot.active_subscribers == 0
                        && snapshot.idle_since_ms.is_some()
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn malformed_hello_returns_error_without_panicking() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;

    framed
        .send(Frame {
            version: PROTOCOL_V1,
            opcode: Op::Hello as u16,
            flags: 0,
            request_id: 9,
            payload: Bytes::from_static(b"not msgpack"),
        })
        .await
        .unwrap();

    assert_error_frame(&mut framed, 9, 400).await;

    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn malformed_publish_returns_error_and_keeps_connection_open() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(Frame {
            version: PROTOCOL_V1,
            opcode: Op::Publish as u16,
            flags: 0,
            request_id: 2,
            payload: Bytes::from_static(b"bad publish"),
        })
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 400).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publish_content_type_header_is_delivered_as_metadata() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "content.type".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: true,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);

    framed
        .send(
            try_encode(
                Op::Publish,
                3,
                &Publish {
                    topic: "content.type".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::from([("Content-Type".into(), "application/json".into())]),
                    payload: br#"{"ok":true}"#.to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::PublishOk as u16);
    assert_eq!(frame.request_id, 3);

    let delivered = recv_delivery_for_topic(&mut framed, "content.type").await;
    assert!(matches!(delivered.content_type, Some(ContentType::Json)));
    assert!(!delivered.headers.contains_key("content-type"));

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publish_with_reserved_header_returns_error_and_keeps_connection_open() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "reserved.headers".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::from([("fibril.retries".into(), "1".into())]),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 400).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn delayed_publish_with_reserved_header_returns_error_and_keeps_connection_open() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::PublishDelayed,
                2,
                &PublishDelayed {
                    topic: "reserved.delayed.headers".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    not_before: unix_millis() + 150,
                    headers: HashMap::from([("stroma.source_offset".into(), "1".into())]),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 400).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn delayed_publish_over_tcp_waits_until_not_before() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "delayed.tcp".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: true,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);

    let not_before = unix_millis() + 150;
    framed
        .send(
            try_encode(
                Op::PublishDelayed,
                3,
                &PublishDelayed {
                    topic: "delayed.tcp".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    not_before,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"delayed".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::PublishOk as u16);
    assert_eq!(frame.request_id, 3);

    assert!(
        tokio::time::timeout(Duration::from_millis(50), framed.next())
            .await
            .is_err()
    );

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Deliver as u16);
    let delivered: Deliver = try_decode(&frame).unwrap();
    assert_eq!(delivered.payload, b"delayed".to_vec());
    assert!(unix_millis() >= not_before);

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn delayed_retry_over_tcp_waits_until_not_before() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "delayed.retry.tcp".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

    framed
        .send(
            try_encode(
                Op::Publish,
                3,
                &Publish {
                    topic: "delayed.retry.tcp".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"retry-later".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::PublishOk as u16);

    let delivered = recv_delivery_for_topic(&mut framed, "delayed.retry.tcp").await;
    let not_before = unix_millis() + 150;
    framed
        .send(
            try_encode(
                Op::Nack,
                4,
                &Nack {
                    topic: "delayed.retry.tcp".into(),
                    group: None,
                    partition: 0,
                    tags: vec![delivered.delivery_tag],
                    requeue: true,
                    not_before: Some(not_before),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert!(
        tokio::time::timeout(Duration::from_millis(50), framed.next())
            .await
            .is_err()
    );

    let redelivered = recv_delivery_for_topic(&mut framed, "delayed.retry.tcp").await;
    assert_eq!(redelivered.payload, b"retry-later".to_vec());
    assert!(unix_millis() >= not_before);

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn exhausted_message_routes_to_global_dlq_over_tcp() {
    let (engine, dir) = open_test_engine().await;
    engine
        .set_global_dlq(
            Some(GlobalDLQ::new("_dlq.source", 0, None).await.unwrap()),
            0,
        )
        .await
        .unwrap();
    let broker = Broker::new(engine, BrokerConfig::default(), None);
    let (mut framed, server_task, _dir, _broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::DeclareQueue,
                2,
                &DeclareQueue {
                    topic: "source".into(),
                    group: None,
                    dlq_policy: Some(QueueDlqPolicy::Global),
                    dlq_max_retries: Some(0),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        recv_frame(&mut framed).await.opcode,
        Op::DeclareQueueOk as u16
    );

    framed
        .send(
            try_encode(
                Op::Subscribe,
                3,
                &Subscribe {
                    topic: "source".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

    framed
        .send(
            try_encode(
                Op::Subscribe,
                4,
                &Subscribe {
                    topic: "_dlq.source".into(),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

    framed
        .send(
            try_encode(
                Op::Publish,
                5,
                &Publish {
                    topic: "source".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::from([("x-trace-id".into(), "dlq-flow".into())]),
                    payload: b"poison".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let source = recv_delivery_for_topic(&mut framed, "source").await;
    assert_eq!(source.payload, b"poison".to_vec());

    framed
        .send(
            try_encode(
                Op::Nack,
                6,
                &Nack {
                    topic: "source".into(),
                    group: None,
                    partition: 0,
                    tags: vec![source.delivery_tag],
                    requeue: true,
                    not_before: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let dlq = recv_delivery_for_topic(&mut framed, "_dlq.source").await;
    assert_eq!(dlq.payload, b"poison".to_vec());
    assert_eq!(
        dlq.headers.get("x-trace-id").map(String::as_str),
        Some("dlq-flow")
    );
    assert_eq!(
        dlq.headers
            .get("stroma.dlq.source_topic")
            .map(String::as_str),
        Some("source")
    );
    assert_eq!(
        dlq.headers
            .get("stroma.dlq.source_offset")
            .map(String::as_str),
        Some("0")
    );
    assert_eq!(
        dlq.headers
            .get("stroma.dlq.retry_count")
            .map(String::as_str),
        Some("0")
    );
    assert_eq!(
        dlq.headers.get("stroma.dlq.reason").map(String::as_str),
        Some("retries_exhausted")
    );
    assert!(dlq.headers.contains_key("stroma.dlq.dead_lettered_at_ms"));

    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publisher_cache_idle_timeout_allows_queue_eviction_while_connection_stays_open() {
    let (mut framed, server_task, _dir, broker) = open_protocol_connection_with_settings(
        ConnectionSettings::new(Some(1)).with_publisher_cache_idle_timeout_ms(Some(0)),
    )
    .await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "publisher.cache.eviction".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::PublishOk as u16);
    assert_eq!(frame.request_id, 2);
    assert!(broker.is_queue_materialized("publisher.cache.eviction", None));
    assert_eq!(
        broker
            .queue_activity_snapshot("publisher.cache.eviction", None)
            .unwrap()
            .active_publishers,
        1
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Ping as u16);

    wait_for_queue_idle(&broker, "publisher.cache.eviction", None).await;

    let attempt = broker
        .try_evict_inactive_queue("publisher.cache.eviction", None, 0)
        .await
        .unwrap();
    assert_eq!(
        attempt,
        QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
    );

    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publisher_cache_idle_timeout_expires_on_next_frame_without_waiting_for_heartbeat() {
    let (mut framed, server_task, _dir, broker) = open_protocol_connection_with_settings(
        ConnectionSettings::new(Some(60)).with_publisher_cache_idle_timeout_ms(Some(0)),
    )
    .await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "publisher.cache.frame.expiry".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::PublishOk as u16);
    assert_eq!(frame.request_id, 2);
    assert_eq!(
        broker
            .queue_activity_snapshot("publisher.cache.frame.expiry", None)
            .unwrap()
            .active_publishers,
        1
    );

    framed
        .send(try_encode(Op::Ping, 3, &()).unwrap())
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Pong as u16);
    assert_eq!(frame.request_id, 3);

    wait_for_queue_idle(&broker, "publisher.cache.frame.expiry", None).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn demo_like_grouped_auto_ack_publish_survives_idle_cleanup() {
    let (engine, dir) = open_test_engine().await;
    let broker = Broker::new(
        engine,
        BrokerConfig {
            inflight_ttl_ms: 2_000,
            expiry_poll_min_ms: 10,
            expiry_batch_max: 100,
            delivery_poll_max_ms: 10,
            queue_idle_evict_after_ms: Some(5),
            queue_idle_sweep_interval_ms: 5,
        },
        None,
    );
    let (mut framed, server_task, _dir, broker) = open_protocol_connection_for_broker(
        ConnectionSettings::new(Some(60)).with_publisher_cache_idle_timeout_ms(Some(1)),
        broker,
        dir,
    )
    .await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "notices".into(),
                    group: Some("workers".into()),
                    prefetch: 20,
                    auto_ack: true,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

    for i in 0..40_u64 {
        let request_id = 10 + i;
        let payload = format!("notice-{i}").into_bytes();
        framed
            .send(
                try_encode(
                    Op::Publish,
                    request_id,
                    &Publish {
                        topic: "notices".into(),
                        partition: 0,
                        group: Some("workers".into()),
                        require_confirm: true,
                        content_type: None,
                        headers: HashMap::new(),
                        payload: payload.clone(),
                        published: unix_millis(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let mut saw_publish_ok = false;
        let mut saw_delivery = false;
        while !saw_publish_ok || !saw_delivery {
            let frame = recv_frame(&mut framed).await;
            match frame.opcode {
                x if x == Op::PublishOk as u16 && frame.request_id == request_id => {
                    saw_publish_ok = true;
                }
                x if x == Op::Deliver as u16 => {
                    let delivered: Deliver = try_decode(&frame).unwrap();
                    if delivered.topic == "notices"
                        && delivered.group.as_deref() == Some("workers")
                        && delivered.payload == payload
                    {
                        saw_delivery = true;
                    }
                }
                x if x == Op::Error as u16 => {
                    let err: ErrorMsg = try_decode(&frame).unwrap();
                    panic!(
                        "unexpected publish flow error for request {}: {} {}",
                        frame.request_id, err.code, err.message
                    );
                }
                _ => {}
            }
        }

        tokio::time::sleep(Duration::from_millis(2)).await;
    }

    tokio::time::sleep(Duration::from_millis(10)).await;
    framed
        .send(try_encode(Op::Ping, 1000, &()).unwrap())
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Pong as u16);
    assert_eq!(frame.request_id, 1000);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if broker
                .queue_activity_snapshot("notices", Some("workers"))
                .is_some_and(|snapshot| {
                    snapshot.active_publishers == 0 && snapshot.active_subscribers == 1
                })
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(broker.is_queue_materialized("notices", Some("workers")));

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn publisher_cache_idle_timeout_updates_existing_connection() {
    let settings = ConnectionSettings::new(Some(1));
    let (mut framed, server_task, _dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "publisher.cache.live".into(),
                    partition: 0,
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::PublishOk as u16);
    assert_eq!(frame.request_id, 2);
    assert_eq!(
        broker
            .queue_activity_snapshot("publisher.cache.live", None)
            .unwrap()
            .active_publishers,
        1
    );

    settings.update_runtime(fibril_protocol::v1::handler::ConnectionRuntimeSettings {
        publisher_cache_idle_timeout_ms: Some(0),
    });

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Ping as u16);

    wait_for_queue_idle(&broker, "publisher.cache.live", None).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}
