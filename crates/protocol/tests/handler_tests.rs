use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
    time::Instant,
};

use bytes::Bytes;
use fibril_broker::storage::Partition;
use fibril_broker::{
    broker::{
        Broker, BrokerConfig, BrokerOwnerReplicationPeer, BrokerOwnerReplicationPeerResolver,
        ConsumerCloseCause, FollowerReplicationWorkerConfig, FollowerReplicationWorkerLoopExit,
        FollowerReplicationWorkerStatus, OwnedQueue, QueueEvictionAttempt, ReplicationResourceKind,
        StaticQueueOwnership,
    },
    coordination::{
        CoordinationSnapshot, LocalAssignmentIntent, LocalAssignmentRole,
        LocalAssignmentTransition, NodeInfo, PartitionAssignment, QueueIdentity,
        ReplicationDurabilityPolicy, StaticCoordination,
    },
    queue_engine::{
        EvictOutcome, GlobalDLQ, OwnerReplicationRead, QueueEngine, QueuePromotionOutcome,
        StromaEngine,
    },
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_protocol::v1::{
    Ack, AdvertisedAddress, ContentType, DeclarePlexus, DeclarePlexusOk, DeclareQueue,
    DeclareQueueOk, Deliver, ERR_CONFLICT, ERR_INVALID, ErrorMsg, HEADER_SPECULATIVE, Hello,
    HelloOk, Nack, Op, PROTOCOL_V1, Publish, PublishDelayed, QueueDlqPolicy, QueueTopologyEntry,
    ReasonCode, ReconcileAction, ReconcileClient, ReconcilePolicy, ReconcileResult,
    ReconcileSubscription, ReplicationApply, ReplicationApplyOk, ReplicationCheckpointExport,
    ReplicationCheckpointExportOk, ReplicationCheckpointInstall, ReplicationCheckpointInstallOk,
    ReplicationCheckpointRequired, ReplicationEventApplyBatch, ReplicationEventRead,
    ReplicationEventRecord, ReplicationMessageApplyBatch, ReplicationMessageRead,
    ReplicationMessageRecord, ReplicationRead, ReplicationReadOk, ReplicationStateCheckpoint,
    ResumeIdentity, ResumeOutcome, StreamDurability, StreamRetention, StreamStart, Subscribe,
    SubscribeOk, SubscribeStream, SubscriptionClosed, TopologyOk, TopologyRequest,
    TopologyUpdateAck,
    frame::{Frame, ProtoCodec},
    handler::{
        ClientTopologySource, ConnectionSettings, DeclareCoordinator, ProtocolConnectionError,
        TopologyAdoptionTracker, handle_connection,
    },
    helper::{Conn, plain_conn, try_decode, try_encode},
    replication::{
        CoordinationProtocolOwnerPeerResolver, ProtocolOwnerPeerResolverConfig,
        ProtocolOwnerReplicationPeer, ProtocolReplicationCatchUp,
        ProtocolReplicationCatchUpOptions, StaticProtocolOwnerPeerResolver,
        catch_up_replication_over_protocol,
    },
    wire,
};
use fibril_util::{StaticAuthHandler, unix_millis};
use futures::{SinkExt, StreamExt};
use stroma_core::{KeratinConfig, SnapshotConfig, StromaEvent, StromaKeratinConfig, TempDir};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tokio_util::sync::CancellationToken;
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
            ..Default::default()
        },
        None,
    );

    (broker, dir)
}

async fn open_test_broker_with_ownership(
    ownership: Arc<StaticQueueOwnership>,
) -> (Arc<Broker<StromaEngine>>, TempDir) {
    let (engine, dir) = open_test_engine().await;
    let broker = Broker::new_with_ownership(engine, BrokerConfig::default(), None, ownership);

    (broker, dir)
}

async fn open_protocol_connection() -> (
    Conn,
    tokio::task::JoinHandle<Result<(), ProtocolConnectionError>>,
    TempDir,
) {
    let (framed, server_task, dir, _broker) =
        open_protocol_connection_with_settings(ConnectionSettings::new(Some(60))).await;
    (framed, server_task, dir)
}

async fn open_protocol_connection_with_settings(
    settings: ConnectionSettings,
) -> (
    Conn,
    tokio::task::JoinHandle<Result<(), ProtocolConnectionError>>,
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
    Conn,
    tokio::task::JoinHandle<Result<(), ProtocolConnectionError>>,
    TempDir,
    Arc<Broker<StromaEngine>>,
) {
    open_protocol_connection_for_broker_with_auth(settings, broker, dir, None).await
}

async fn open_node_connection_for_broker(
    settings: ConnectionSettings,
    broker: Arc<Broker<StromaEngine>>,
    dir: TempDir,
) -> (
    Conn,
    tokio::task::JoinHandle<Result<(), ProtocolConnectionError>>,
    TempDir,
    Arc<Broker<StromaEngine>>,
) {
    open_protocol_connection_for_broker_with_auth(settings, broker, dir, Some(node_auth())).await
}

async fn open_node_connection() -> (
    Conn,
    tokio::task::JoinHandle<Result<(), ProtocolConnectionError>>,
    TempDir,
) {
    let (broker, dir) = open_test_broker().await;
    let (conn, task, dir, _) =
        open_node_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    (conn, task, dir)
}

fn node_auth() -> StaticAuthHandler {
    StaticAuthHandler::new("@node".into(), "secret".into())
}

async fn node_handshake(conn: &mut Conn) {
    handshake(conn).await;
    conn.send(
        try_encode(
            Op::Auth,
            900,
            &fibril_protocol::v1::Auth {
                username: "@node".into(),
                password: "secret".into(),
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(recv_frame(conn).await.opcode, Op::AuthOk as u16);
}

async fn open_protocol_connection_for_broker_with_auth(
    settings: ConnectionSettings,
    broker: Arc<Broker<StromaEngine>>,
    dir: TempDir,
    auth: Option<StaticAuthHandler>,
) -> (
    Conn,
    tokio::task::JoinHandle<Result<(), ProtocolConnectionError>>,
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
        Some(peer),
        broker.clone(),
        tcp_stats,
        connection_stats,
        conn_id,
        auth,
        None,
        settings,
        None,
        None,
        None,
    ));

    (plain_conn(client), server_task, dir, broker)
}

async fn start_protocol_listener_for_broker(
    settings: ConnectionSettings,
    broker: Arc<Broker<StromaEngine>>,
    dir: TempDir,
    auth: Option<StaticAuthHandler>,
) -> (
    SocketAddr,
    tokio::task::JoinHandle<Result<(), ProtocolConnectionError>>,
    TempDir,
    Arc<Broker<StromaEngine>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_broker = broker.clone();

    let server_task = tokio::spawn(async move {
        let (server, peer) = listener.accept().await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            server,
            Some(peer),
            server_broker,
            tcp_stats,
            connection_stats,
            conn_id,
            auth,
            None,
            settings,
            None,
            None,
            None,
        )
        .await
    });

    (addr, server_task, dir, broker)
}

fn follower_assignment_transition(topic: &str, group: Option<&str>) -> LocalAssignmentTransition {
    LocalAssignmentTransition {
        queue: QueueIdentity::new(topic, Partition::new(0), group),
        previous_role: None,
        next_role: Some(LocalAssignmentRole::Follower),
        previous: None,
        next: None,
        intent: LocalAssignmentIntent::BecomeFollower,
    }
}

/// A real owner treats a follower dropping its connection as end-of-connection,
/// not a fault. At teardown the follower and resolver close their sockets
/// abruptly, so depending on scheduling a fake owner server's blocked read (or a
/// mid-flight send) surfaces a connection reset or broken pipe rather than a
/// graceful EOF. Fold those two IO kinds into a clean exit so the teardown
/// assertion is deterministic under load - any other error is a genuine protocol
/// fault and still fails the test.
fn tolerate_peer_disconnect(result: anyhow::Result<()>) -> anyhow::Result<()> {
    match result {
        Err(err)
            if matches!(
                err.downcast_ref::<std::io::Error>()
                    .map(std::io::Error::kind),
                Some(std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe)
            ) =>
        {
            Ok(())
        }
        other => other,
    }
}

async fn start_checkpoint_required_owner_server(
    checkpoint: ReplicationStateCheckpoint,
    message_records: Vec<ReplicationMessageRecord>,
    event_records: Vec<ReplicationEventRecord>,
) -> (SocketAddr, tokio::task::JoinHandle<anyhow::Result<()>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let served: anyhow::Result<()> = async move {
            let (stream, _) = listener.accept().await?;
            let mut conn = plain_conn(stream);

            let frame = conn
                .next()
                .await
                .ok_or_else(|| anyhow::anyhow!("client closed before hello"))??;
            assert_eq!(frame.opcode, Op::Hello as u16);
            conn.send(try_encode(
                Op::HelloOk,
                frame.request_id,
                &HelloOk {
                    protocol_version: PROTOCOL_V1,
                    owner_id: Uuid::now_v7(),
                    client_id: Uuid::now_v7(),
                    resume_token: Uuid::now_v7(),
                    resume_outcome: ResumeOutcome::New,
                    server_name: "fake-owner".into(),
                    compliance: "test".into(),
                },
            )?)
            .await?;

            while let Some(frame) = conn.next().await {
                let frame = frame?;
                match frame.opcode {
                    x if x == Op::ReplicationRead as u16 => {
                        let read: ReplicationRead = try_decode(&frame)?;
                        // This fake models an owner whose event log has been
                        // compacted but whose messages are still available from
                        // the checkpoint message offset.
                        let response = if read.event_from < checkpoint.event_next_offset {
                            ReplicationReadOk {
                                messages: read_fake_message_batch(
                                    &checkpoint,
                                    &message_records,
                                    read.message_from,
                                    read.max_messages,
                                ),
                                events: ReplicationEventRead::CheckpointRequired(
                                    ReplicationCheckpointRequired {
                                        epoch: checkpoint.event_epoch,
                                        requested_offset: read.event_from,
                                        head_offset: checkpoint.event_next_offset,
                                        next_offset: checkpoint.event_next_offset,
                                    },
                                ),
                            }
                        } else {
                            ReplicationReadOk {
                                messages: read_fake_message_batch(
                                    &checkpoint,
                                    &message_records,
                                    read.message_from,
                                    read.max_messages,
                                ),
                                events: read_fake_event_batch(
                                    &checkpoint,
                                    &event_records,
                                    read.event_from,
                                    read.max_events,
                                ),
                            }
                        };
                        conn.send(wire::encode_replication_read_ok(
                            frame.request_id,
                            &response,
                        )?)
                        .await?;
                    }
                    x if x == Op::ReplicationCheckpointExport as u16 => {
                        let _: ReplicationCheckpointExport = try_decode(&frame)?;
                        conn.send(try_encode(
                            Op::ReplicationCheckpointExportOk,
                            frame.request_id,
                            &ReplicationCheckpointExportOk {
                                checkpoint: checkpoint.clone(),
                            },
                        )?)
                        .await?;
                    }
                    other => anyhow::bail!("unexpected fake owner opcode {other}"),
                }
            }
            Ok(())
        }
        .await;
        tolerate_peer_disconnect(served)
    });
    (addr, server_task)
}

fn read_fake_message_batch(
    checkpoint: &ReplicationStateCheckpoint,
    message_records: &[ReplicationMessageRecord],
    from: u64,
    max: u32,
) -> ReplicationMessageRead {
    let records = message_records
        .iter()
        .filter(|record| record.offset >= from)
        .take(max as usize)
        .cloned()
        .collect::<Vec<_>>();
    let next_offset = records.last().map_or(from, |record| record.offset + 1);
    ReplicationMessageRead::Batch {
        epoch: checkpoint.message_epoch,
        requested_offset: from,
        next_offset,
        records,
    }
}

fn read_fake_event_batch(
    checkpoint: &ReplicationStateCheckpoint,
    event_records: &[ReplicationEventRecord],
    from: u64,
    max: u32,
) -> ReplicationEventRead {
    let records = event_records
        .iter()
        .filter(|record| record.offset >= from)
        .take(max as usize)
        .cloned()
        .collect::<Vec<_>>();
    let next_offset = records.last().map_or(from, |record| record.offset + 1);
    ReplicationEventRead::Batch {
        epoch: checkpoint.event_epoch,
        requested_offset: from,
        next_offset,
        records,
    }
}

async fn recv_frame(framed: &mut Conn) -> Frame {
    // A failure-detection bound, not a performance assertion. The whole suite
    // runs many timing-sensitive cohort/failover tests in parallel, so a loaded
    // machine is slow, not stuck. Keep this generous so contention does not flake
    // a correct test (a real hang still fails, just later).
    tokio::time::timeout(Duration::from_secs(15), framed.next())
        .await
        .expect("frame did not arrive within the receive timeout")
        .expect("framed stream ended unexpectedly")
        .expect("frame decode failed")
}

/// Receive the next frame matching `expected`, transparently skipping the
/// server heartbeat Pings that can interleave ahead of a response on a
/// short-heartbeat connection under load. A Ping is skipped only when it is not
/// itself the expected opcode, so a test that awaits a heartbeat still gets one.
/// Any other unexpected opcode fails loudly rather than looping.
async fn recv_frame_expect(framed: &mut Conn, expected: Op) -> Frame {
    loop {
        let frame = recv_frame(framed).await;
        if frame.opcode == expected as u16 {
            return frame;
        }
        assert_eq!(
            frame.opcode,
            Op::Ping as u16,
            "expected {expected:?} while skipping heartbeats, got a different opcode"
        );
    }
}

async fn handshake(framed: &mut Conn) {
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

async fn handshake_with_resume(framed: &mut Conn, resume: Option<ResumeIdentity>) -> HelloOk {
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

async fn assert_error_frame(framed: &mut Conn, request_id: u64, code: u16) {
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::Error as u16);
    assert_eq!(frame.request_id, request_id);
    let err: ErrorMsg = try_decode(&frame).unwrap();
    assert_eq!(err.code, code);
    assert!(!err.message.is_empty());
}

async fn assert_subscribe_error_frame(framed: &mut Conn, request_id: u64, code: u16) {
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::SubscribeErr as u16);
    assert_eq!(frame.request_id, request_id);
    let err: ErrorMsg = try_decode(&frame).unwrap();
    assert_eq!(err.code, code);
    assert!(!err.message.is_empty());
}

async fn assert_connection_still_responds(framed: &mut Conn) {
    framed
        .send(try_encode(Op::Ping, 99, &()).unwrap())
        .await
        .unwrap();
    // Skip any server heartbeat Ping that raced ahead of our Pong.
    let frame = recv_frame_expect(framed, Op::Pong).await;
    assert_eq!(frame.request_id, 99);
}

async fn framed_subscribe(
    framed: &mut Conn,
    request_id: u64,
    topic: &str,
    group: Option<&str>,
    auto_ack: bool,
) -> fibril_protocol::v1::SubscribeOk {
    framed
        .send(
            try_encode(
                Op::Subscribe,
                request_id,
                &Subscribe {
                    topic: topic.into(),
                    partition: Partition::new(0),
                    group: group.map(str::to_string),
                    prefetch: 1,
                    auto_ack,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);
    try_decode(&frame).unwrap()
}

async fn framed_publish(
    framed: &mut Conn,
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
                    partition: Partition::new(0),
                    group: group.map(str::to_string),
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: payload.to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
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
async fn reconcile_after_resume_keeps_matching_subscription() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    first
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "reconcile.keep".into(),
                    partition: Partition::new(0),
                    group: Some("workers".into()),
                    prefetch: 1,
                    auto_ack: false,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut first).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);
    let sub_ok: fibril_protocol::v1::SubscribeOk = try_decode(&frame).unwrap();

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

    second
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![ReconcileSubscription {
                        sub_id: sub_ok.sub_id,
                        topic: sub_ok.topic,
                        group: sub_ok.group,
                        partition: sub_ok.partition,
                        auto_ack: false,
                        prefetch: sub_ok.prefetch,
                        consumer_group: None,
                        consumer_target: None,
                        member_id: None,
                    }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut second).await;
    assert_eq!(frame.opcode, Op::ReconcileResult as u16);
    assert_eq!(frame.request_id, 3);
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    assert_eq!(result.subscriptions[0].action, ReconcileAction::Keep);
    assert_eq!(result.subscriptions[0].reason, "matched");

    drop(second);
    second_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn reconcile_after_resume_closes_mismatched_subscription() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    first
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "reconcile.recreate".into(),
                    partition: Partition::new(0),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut first).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);
    let sub_ok: fibril_protocol::v1::SubscribeOk = try_decode(&frame).unwrap();

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

    second
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![ReconcileSubscription {
                        sub_id: sub_ok.sub_id,
                        topic: sub_ok.topic,
                        group: sub_ok.group,
                        partition: sub_ok.partition,
                        auto_ack: false,
                        prefetch: sub_ok.prefetch + 1,
                        consumer_group: None,
                        consumer_target: None,
                        member_id: None,
                    }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut second).await;
    assert_eq!(frame.opcode, Op::ReconcileResult as u16);
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    assert_eq!(
        result.subscriptions[0].action,
        ReconcileAction::CloseClientSide
    );
    assert_eq!(result.subscriptions[0].reason, "server_mismatch");
    assert_eq!(result.subscriptions[0].code, ReasonCode::ServerMismatch);

    drop(second);
    second_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn conservative_reconcile_advises_recreate_for_missing_manual_ack_sub() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut framed, task, _dir, _broker) = open_protocol_connection_with_settings(settings).await;
    let _hello = handshake_with_resume(&mut framed, None).await;

    // The client claims two subscriptions the server has no record of: a
    // manual-ack one (safely recreatable, unsettled work redelivers on the
    // recreated sub) and an auto-ack one (deliveries in flight at the
    // disconnect were already settled at send, so a silent recreate could
    // hide loss and it keeps the close verdict).
    framed
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![
                        ReconcileSubscription {
                            sub_id: 11,
                            topic: "reconcile.manual".into(),
                            group: None,
                            partition: Partition::new(0),
                            auto_ack: false,
                            prefetch: 1,
                            consumer_group: None,
                            consumer_target: None,
                            member_id: None,
                        },
                        ReconcileSubscription {
                            sub_id: 12,
                            topic: "reconcile.auto".into(),
                            group: None,
                            partition: Partition::new(0),
                            auto_ack: true,
                            prefetch: 1,
                            consumer_group: None,
                            consumer_target: None,
                            member_id: None,
                        },
                    ],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame_expect(&mut framed, Op::ReconcileResult).await;
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 2);
    for sub in &result.subscriptions {
        let client = sub.client.as_ref().expect("client side present");
        if client.auto_ack {
            assert_eq!(sub.action, ReconcileAction::CloseClientSide);
            assert_eq!(sub.code, ReasonCode::ServerMissing);
        } else {
            assert_eq!(sub.action, ReconcileAction::RecreateClientSide);
            assert_eq!(sub.code, ReasonCode::Recreate);
            assert_eq!(sub.reason, "recreate");
        }
    }

    drop(framed);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn queue_delete_closes_live_subscription_with_typed_reason() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut framed, task, _dir, broker) = open_protocol_connection_with_settings(settings).await;
    handshake(&mut framed).await;
    let sub_ok = framed_subscribe(&mut framed, 2, "delete.notice", None, false).await;

    // The admin delete path closes the queue's live consumers after tearing
    // down storage. The subscriber gets a typed close instead of a delivery
    // stream that silently never speaks again.
    broker.close_queue_consumers("delete.notice", None, ConsumerCloseCause::TopicDeleted);

    let frame = recv_frame_expect(&mut framed, Op::SubscriptionClosed).await;
    let closed: SubscriptionClosed = try_decode(&frame).unwrap();
    assert_eq!(closed.sub_id, sub_ok.sub_id);
    assert_eq!(closed.code, ReasonCode::TopicDeleted);
    assert!(!closed.message.is_empty());

    drop(framed);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn resume_across_restart_honors_a_durable_skeleton() {
    use fibril_protocol::v1::session_store::SessionSkeletonStore;

    // One durable engine underneath (keratin's global store is fsync-durable
    // by construction). A restart is modeled at the session layer: boot 2
    // loads a FRESH session store + fresh resume registry from the same durable
    // data, which is exactly the state a new process sees - the persisted
    // owner id and skeletons, but no live in-memory sessions.
    let (engine, dir) = open_test_engine().await;

    // Boot 1.
    let store1 = Arc::new(
        SessionSkeletonStore::load_from_stroma_engine(&engine, Uuid::new_v4())
            .await
            .expect("load session store"),
    );
    let broker1 = Broker::new(engine.clone(), BrokerConfig::default(), None);
    // A grace window so the clean disconnect below enters grace (session stays
    // resumable) rather than immediately forgetting it. A real broker crash
    // never runs the clean-disconnect cleanup at all, so the on-disk skeleton
    // survives regardless; grace models the session still being alive at the
    // moment the broker restarts. The window is long enough that its expiry
    // timer never fires during the test.
    let settings1 = ConnectionSettings::new(Some(60))
        .with_reconnect_grace_ms(Some(30_000))
        .with_resume_session_restart_ttl_ms(Some(60_000))
        .with_session_store(store1);

    let (mut first, first_task, dir, _broker) =
        open_protocol_connection_for_broker(settings1, broker1, dir).await;
    let hello_ok = handshake_with_resume(&mut first, None).await;
    assert_eq!(hello_ok.resume_outcome, ResumeOutcome::New);
    let sub_ok = framed_subscribe(&mut first, 2, "restart.jobs", None, false).await;
    // Ping/pong fences the skeleton persist: the frame loop awaits the write
    // before the next frame, so a Pong proves it landed durably.
    assert_connection_still_responds(&mut first).await;

    let resume = ResumeIdentity {
        owner_id: hello_ok.owner_id,
        client_id: hello_ok.client_id,
        resume_token: hello_ok.resume_token,
    };
    drop(first);
    first_task.await.unwrap().unwrap();

    // Boot 2 ("restart"): fresh store + registry from the same durable data.
    let store2 = Arc::new(
        SessionSkeletonStore::load_from_stroma_engine(&engine, Uuid::new_v4())
            .await
            .expect("reload session store"),
    );
    // The persisted skeleton survived the restart.
    assert!(
        store2
            .lookup(&hello_ok.client_id, &hello_ok.resume_token, 60_000)
            .await
            .is_some(),
        "skeleton for client {} did not survive restart",
        hello_ok.client_id,
    );
    let broker2 = Broker::new(engine.clone(), BrokerConfig::default(), None);
    let settings2 = ConnectionSettings::new(Some(60))
        .with_resume_session_restart_ttl_ms(Some(60_000))
        .with_session_store(store2);
    let (mut second, second_task, dir, _broker) =
        open_protocol_connection_for_broker(settings2, broker2, dir).await;

    // The client resumes with its cached identity. The restarted broker keeps
    // its owner id (persisted), so the owner check passes, the live session is
    // gone, and the durable skeleton upgrades not-found to resumed-after-restart.
    let restart_ok = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(
        restart_ok.resume_outcome,
        ResumeOutcome::ResumedAfterRestart
    );
    assert_eq!(restart_ok.client_id, hello_ok.client_id);

    // And the client can reconcile its subscription set on the restored session
    // (the fresh session is empty, so a manual-ack sub on an owned queue is
    // advised to recreate).
    second
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![ReconcileSubscription {
                        sub_id: sub_ok.sub_id,
                        topic: "restart.jobs".into(),
                        group: None,
                        partition: Partition::new(0),
                        auto_ack: false,
                        prefetch: 1,
                        consumer_group: None,
                        consumer_target: None,
                        member_id: None,
                    }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame_expect(&mut second, Op::ReconcileResult).await;
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    assert_eq!(
        result.subscriptions[0].action,
        ReconcileAction::RecreateClientSide
    );

    drop(second);
    second_task.await.unwrap().unwrap();
    drop(dir);
}

#[tokio::test]
async fn resume_with_a_missing_skeleton_stays_not_found() {
    use fibril_protocol::v1::session_store::SessionSkeletonStore;

    // A resume presenting the correct owner id but a client id the store never
    // saw reports not-found (not resumed-after-restart), and a genuinely wrong
    // owner stays rejected - the two outcomes the skeleton path must preserve.
    let (broker, dir) = open_test_broker().await;
    let store = Arc::new(
        SessionSkeletonStore::load_from_stroma_engine(&broker.engine(), Uuid::new_v4())
            .await
            .expect("load session store"),
    );
    let owner_id = store.owner_id();
    let settings = ConnectionSettings::new(Some(60))
        .with_resume_session_restart_ttl_ms(Some(60_000))
        .with_session_store(store);
    let (mut framed, task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;

    // Right owner, unknown client id: no skeleton, so not-found.
    let outcome = handshake_with_resume(
        &mut framed,
        Some(ResumeIdentity {
            owner_id,
            client_id: Uuid::from_u128(0xdead),
            resume_token: Uuid::from_u128(0xbeef),
        }),
    )
    .await;
    assert_eq!(outcome.resume_outcome, ResumeOutcome::ResumeNotFound);

    drop(framed);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn owner_identity_persists_across_a_real_engine_restart() {
    use fibril_protocol::v1::session_store::SessionSkeletonStore;

    // A genuine on-disk restart: open the engine, shut it down, drop it (the
    // keratin flock releases), and reopen the same data dir. The persisted
    // owner id survives, which is what lets a client's cached resume identity
    // still match a restarted broker.
    let (engine1, dir) = open_test_engine().await;
    let store1 = SessionSkeletonStore::load_from_stroma_engine(&engine1, Uuid::from_u128(1))
        .await
        .expect("load session store");
    let owner = store1.owner_id();
    drop(store1);
    engine1.shutdown().await.expect("engine shutdown");
    drop(engine1);

    let engine2 = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .expect("reopen engine");
    let store2 = SessionSkeletonStore::load_from_stroma_engine(&engine2, Uuid::from_u128(2))
        .await
        .expect("reload session store");
    assert_eq!(
        store2.owner_id(),
        owner,
        "owner id must persist across a restart"
    );
    engine2.shutdown().await.expect("engine shutdown");
    drop(dir);
}

#[tokio::test]
async fn restart_resume_disabled_by_zero_ttl() {
    use fibril_protocol::v1::session_store::SessionSkeletonStore;

    // With the restart TTL set to 0, a persisted skeleton is never honored, so
    // a resume after a restart falls through to the ordinary not-found outcome.
    let (broker, dir) = open_test_broker().await;
    let store1 = Arc::new(
        SessionSkeletonStore::load_from_stroma_engine(&broker.engine(), Uuid::from_u128(1))
            .await
            .expect("load session store"),
    );
    let settings1 = ConnectionSettings::new(Some(60))
        .with_reconnect_grace_ms(Some(30_000))
        .with_resume_session_restart_ttl_ms(Some(60_000))
        .with_session_store(store1);
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_for_broker(settings1, broker, dir).await;
    let hello_ok = handshake_with_resume(&mut first, None).await;
    let _ = framed_subscribe(&mut first, 2, "jobs", None, false).await;
    assert_connection_still_responds(&mut first).await;
    let resume = ResumeIdentity {
        owner_id: hello_ok.owner_id,
        client_id: hello_ok.client_id,
        resume_token: hello_ok.resume_token,
    };
    drop(first);
    first_task.await.unwrap().unwrap();

    // Fresh store + registry (a restart), but restart resume is disabled.
    let store2 = Arc::new(
        SessionSkeletonStore::load_from_stroma_engine(&broker.engine(), Uuid::from_u128(2))
            .await
            .expect("reload session store"),
    );
    let settings2 = ConnectionSettings::new(Some(60))
        .with_reconnect_grace_ms(Some(30_000))
        .with_resume_session_restart_ttl_ms(Some(0))
        .with_session_store(store2);
    let (mut second, second_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings2, broker, dir).await;
    let outcome = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(outcome.resume_outcome, ResumeOutcome::ResumeNotFound);

    drop(second);
    second_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn clean_disconnect_forgets_the_skeleton() {
    use fibril_protocol::v1::session_store::SessionSkeletonStore;

    // A clean disconnect with no grace forgets the session AND its durable
    // skeleton (the client is gone, no resume is owed). A later resume against
    // a fresh store therefore reports not-found, not resumed-after-restart -
    // only a real crash, which never runs cleanup, leaves the skeleton behind.
    let (broker, dir) = open_test_broker().await;
    let store1 = Arc::new(
        SessionSkeletonStore::load_from_stroma_engine(&broker.engine(), Uuid::from_u128(1))
            .await
            .expect("load session store"),
    );
    // Grace off: disconnect forgets immediately.
    let settings1 = ConnectionSettings::new(Some(60))
        .with_reconnect_grace_ms(Some(0))
        .with_resume_session_restart_ttl_ms(Some(60_000))
        .with_session_store(store1.clone());
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_for_broker(settings1, broker, dir).await;
    let hello_ok = handshake_with_resume(&mut first, None).await;
    let _ = framed_subscribe(&mut first, 2, "jobs", None, false).await;
    assert_connection_still_responds(&mut first).await;
    let resume = ResumeIdentity {
        owner_id: hello_ok.owner_id,
        client_id: hello_ok.client_id,
        resume_token: hello_ok.resume_token,
    };
    // Clean disconnect: cleanup runs and removes the skeleton.
    drop(first);
    first_task.await.unwrap().unwrap();

    // The skeleton is gone even to a fresh store load.
    let store2 = Arc::new(
        SessionSkeletonStore::load_from_stroma_engine(&broker.engine(), Uuid::from_u128(2))
            .await
            .expect("reload session store"),
    );
    assert!(
        store2
            .lookup(&hello_ok.client_id, &hello_ok.resume_token, 60_000)
            .await
            .is_none(),
        "a clean disconnect must forget the skeleton",
    );
    let settings2 = ConnectionSettings::new(Some(60))
        .with_reconnect_grace_ms(Some(30_000))
        .with_resume_session_restart_ttl_ms(Some(60_000))
        .with_session_store(store2);
    let (mut second, second_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings2, broker, dir).await;
    let outcome = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(outcome.resume_outcome, ResumeOutcome::ResumeNotFound);

    drop(second);
    second_task.await.unwrap().unwrap();
}

/// Broker config with a short inflight lease so a restart test can watch
/// unacked work redeliver quickly.
fn short_lease_broker_config() -> BrokerConfig {
    BrokerConfig {
        inflight_ttl_ms: 400,
        expiry_poll_min_ms: 25,
        expiry_batch_max: 100,
        delivery_poll_max_ms: 25,
        queue_idle_evict_after_ms: None,
        queue_idle_sweep_interval_ms: 60_000,
        ..Default::default()
    }
}

fn restart_settings(
    store: Arc<fibril_protocol::v1::session_store::SessionSkeletonStore>,
) -> ConnectionSettings {
    ConnectionSettings::new(Some(60))
        .with_reconnect_grace_ms(Some(30_000))
        .with_resume_session_restart_ttl_ms(Some(60_000))
        .with_session_store(store)
}

#[tokio::test]
async fn resume_across_a_real_restart_redelivers_unacked_work() {
    use fibril_protocol::v1::session_store::SessionSkeletonStore;

    // The full chain, end to end over real wire frames and real durable
    // storage: a client subscribes and receives a message it does not ack, the
    // broker restarts for real (engine shutdown + reopen on the same data dir),
    // the client resumes with ResumedAfterRestart, reconciles its subscription
    // into a recreate, re-subscribes, and the unacked message redelivers per
    // at-least-once.
    let (engine1, dir) = open_test_engine().await;
    let store1 = Arc::new(
        SessionSkeletonStore::load_from_stroma_engine(&engine1, Uuid::from_u128(1))
            .await
            .expect("load session store"),
    );
    let broker1 = Broker::new(engine1.clone(), short_lease_broker_config(), None);

    let (mut first, first_task, dir, _broker) =
        open_protocol_connection_for_broker(restart_settings(store1), broker1.clone(), dir).await;
    let hello_ok = handshake_with_resume(&mut first, None).await;
    assert_eq!(hello_ok.resume_outcome, ResumeOutcome::New);
    let sub_ok = framed_subscribe(&mut first, 2, "e2e.restart", None, false).await;
    framed_publish(&mut first, 3, "e2e.restart", None, b"pending").await;

    // Receive the delivery but deliberately leave it unacked (inflight).
    let delivered = recv_delivery_for_topic(&mut first, "e2e.restart").await;
    assert_eq!(delivered.payload, b"pending");
    // Fence the skeleton persist.
    assert_connection_still_responds(&mut first).await;

    let resume = ResumeIdentity {
        owner_id: hello_ok.owner_id,
        client_id: hello_ok.client_id,
        resume_token: hello_ok.resume_token,
    };

    // Restart the broker for real: drop the connection and broker, shut down
    // the engine, drop it (the flock releases), then reopen the same data dir.
    drop(first);
    first_task.await.unwrap().unwrap();
    drop(broker1);
    engine1.shutdown().await.expect("engine shutdown");
    drop(engine1);

    let engine2 = StromaEngine::open(
        &dir.root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .expect("reopen engine");
    let store2 = Arc::new(
        SessionSkeletonStore::load_from_stroma_engine(&engine2, Uuid::from_u128(2))
            .await
            .expect("reload session store"),
    );
    let broker2 = Broker::new(engine2.clone(), short_lease_broker_config(), None);

    let (mut second, second_task, dir, _broker) =
        open_protocol_connection_for_broker(restart_settings(store2), broker2, dir).await;
    let restart_ok = handshake_with_resume(&mut second, Some(resume)).await;
    assert_eq!(
        restart_ok.resume_outcome,
        ResumeOutcome::ResumedAfterRestart
    );

    // Reconcile the subscription: the fresh session has none, so the manual-ack
    // sub on the still-owned queue is advised to recreate.
    second
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![ReconcileSubscription {
                        sub_id: sub_ok.sub_id,
                        topic: "e2e.restart".into(),
                        group: None,
                        partition: Partition::new(0),
                        auto_ack: false,
                        prefetch: 1,
                        consumer_group: None,
                        consumer_target: None,
                        member_id: None,
                    }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame_expect(&mut second, Op::ReconcileResult).await;
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(
        result.subscriptions[0].action,
        ReconcileAction::RecreateClientSide
    );

    // The client re-subscribes (what a real client does on a recreate), and the
    // unacked message redelivers once its pre-restart lease expires.
    let resub_ok = framed_subscribe(&mut second, 4, "e2e.restart", None, false).await;
    assert_eq!(resub_ok.topic, "e2e.restart");
    let redelivered = recv_delivery_for_topic(&mut second, "e2e.restart").await;
    assert_eq!(
        redelivered.payload, b"pending",
        "unacked work redelivers after restart"
    );

    drop(second);
    second_task.await.unwrap().unwrap();
    engine2.shutdown().await.ok();
    drop(dir);
}

#[tokio::test]
async fn conservative_reconcile_drops_server_only_subscription() {
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(1_000));
    let (mut framed, task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let _hello = handshake_with_resume(&mut framed, None).await;
    let sub_ok = framed_subscribe(&mut framed, 2, "reconcile.server.only", None, false).await;
    assert!(
        broker
            .queue_activity_snapshot("reconcile.server.only", None)
            .is_some_and(|snapshot| snapshot.active_subscribers == 1)
    );

    framed
        .send(
            try_encode(
                Op::ReconcileClient,
                3,
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: vec![],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::ReconcileResult as u16);
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    assert_eq!(result.subscriptions[0].client, None);
    assert_eq!(
        result.subscriptions[0].action,
        ReconcileAction::CloseServerSide
    );
    assert_eq!(result.subscriptions[0].reason, "client_missing");
    assert_eq!(
        result.subscriptions[0]
            .server
            .as_ref()
            .map(|sub| sub.sub_id),
        Some(sub_ok.sub_id)
    );
    wait_for_queue_idle(&broker, "reconcile.server.only", None).await;

    drop(framed);
    task.await.unwrap().unwrap();
    drop(dir);
}

#[tokio::test]
async fn restore_policy_recreates_client_only_subscription() {
    let (mut framed, task, dir, _broker) =
        open_protocol_connection_with_settings(ConnectionSettings::new(Some(60))).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::ReconcileClient,
                2,
                &ReconcileClient {
                    policy: ReconcilePolicy::Restore,
                    subscriptions: vec![ReconcileSubscription {
                        sub_id: 99,
                        topic: "reconcile.restore".into(),
                        group: Some("workers".into()),
                        partition: Partition::new(0),
                        auto_ack: false,
                        prefetch: 2,
                        consumer_group: None,
                        consumer_target: None,
                        member_id: None,
                    }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::ReconcileResult as u16);
    let result: ReconcileResult = try_decode(&frame).unwrap();
    assert_eq!(result.subscriptions.len(), 1);
    let item = &result.subscriptions[0];
    assert_eq!(item.action, ReconcileAction::Keep);
    assert_eq!(item.reason, "server_restored");
    assert_eq!(item.client.as_ref().map(|sub| sub.sub_id), Some(99));
    let restored = item.server.as_ref().unwrap();
    assert_ne!(restored.sub_id, 99);
    assert_eq!(restored.topic, "reconcile.restore");
    assert_eq!(restored.group.as_deref(), Some("workers"));
    assert_eq!(restored.prefetch, 2);

    framed_publish(
        &mut framed,
        3,
        "reconcile.restore",
        Some("workers"),
        b"restored",
    )
    .await;
    let delivered = recv_delivery_for_topic(&mut framed, "reconcile.restore").await;
    assert_eq!(delivered.sub_id, restored.sub_id);
    assert_eq!(delivered.payload, Bytes::from_static(b"restored"));

    drop(framed);
    task.await.unwrap().unwrap();
    drop(dir);
}

/// Reconnect-reconcile restores EXCLUSIVE membership (limitation b): a restored
/// subscription carrying its cohort id rejoins the cohort instead of silently
/// becoming a plain competing consumer. Proof: only an exclusive member receives
/// an AssignmentChanged push, so its arrival means exclusivity was restored.
#[tokio::test]
async fn reconcile_restores_exclusive_cohort_membership() {
    let (mut framed, task, dir, _broker) =
        open_protocol_connection_with_settings(ConnectionSettings::new(Some(60))).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::ReconcileClient,
                2,
                &ReconcileClient {
                    policy: ReconcilePolicy::Restore,
                    subscriptions: vec![ReconcileSubscription {
                        sub_id: 99,
                        topic: "reconcile.exclusive".into(),
                        group: None,
                        partition: Partition::new(0),
                        auto_ack: true,
                        prefetch: 4,
                        consumer_group: Some("default".into()),
                        consumer_target: None,
                        member_id: None,
                    }],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let assignment =
        recv_assignment_until(&mut framed, |asg| asg.consumer_group == "default").await;
    assert_eq!(assignment.topic, "reconcile.exclusive");
    assert_eq!(assignment.assigned, vec![Partition::new(0)]);

    drop(framed);
    task.await.unwrap().unwrap();
    drop(dir);
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
                    partition: Partition::new(0),
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
async fn reconnect_grace_runtime_update_affects_future_disconnects() {
    let settings = ConnectionSettings::new(Some(60));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;

    let first_ok = handshake_with_resume(&mut first, None).await;
    framed_subscribe(&mut first, 2, "grace.live", None, false).await;
    framed_publish(&mut first, 3, "grace.live", None, b"ack-after-live-update").await;
    let delivered = recv_delivery_for_topic(&mut first, "grace.live").await;

    settings.update_runtime(fibril_protocol::v1::handler::ConnectionRuntimeSettings {
        reconnect_grace_ms: Some(100),
        ..Default::default()
    });

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
                    topic: "grace.live".into(),
                    group: None,
                    partition: Partition::new(0),
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
    framed_subscribe(&mut third, 2, "grace.live", None, false).await;
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

async fn recv_delivery_for_topic(framed: &mut Conn, topic: &str) -> Deliver {
    // Generous failure-detection bound, see recv_frame. Failover delivery in
    // particular waits on an async cleanup + gate recompute, which a loaded
    // parallel run can slow well past a couple seconds.
    tokio::time::timeout(Duration::from_secs(15), async {
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
    .expect("delivery did not arrive within the receive timeout")
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
async fn replication_read_returns_owner_log_records() {
    let (mut framed, server_task, _dir) = open_node_connection().await;
    node_handshake(&mut framed).await;
    framed_publish(
        &mut framed,
        2,
        "replication.read.tcp",
        Some("workers"),
        b"replicated-payload",
    )
    .await;

    framed
        .send(
            try_encode(
                Op::ReplicationRead,
                3,
                &ReplicationRead {
                    reporter_epoch: None,
                    topic: "replication.read.tcp".into(),
                    group: Some("workers".into()),
                    partition: Partition::new(0),
                    message_from: 0,
                    event_from: 0,
                    max_messages: 10,
                    max_events: 10,
                    max_bytes: 1024 * 1024,
                    max_wait_ms: 0,
                    reporter_node_id: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::ReplicationReadOk as u16);
    assert_eq!(frame.request_id, 3);
    let response: ReplicationReadOk = wire::decode_replication_read_ok(&frame).unwrap();

    match response.messages {
        ReplicationMessageRead::Batch { records, .. } => {
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].offset, 0);
            assert_eq!(records[0].flags, 0);
            assert_eq!(records[0].payload, b"replicated-payload".to_vec());
        }
        ReplicationMessageRead::CheckpointRequired(required) => {
            panic!("unexpected message checkpoint requirement: {required:?}");
        }
    }

    match response.events {
        ReplicationEventRead::Batch { records, .. } => {
            assert!(!records.is_empty());
            assert_eq!(records[0].offset, 0);
            assert!(!records[0].payload.is_empty());
        }
        ReplicationEventRead::CheckpointRequired(required) => {
            panic!("unexpected event checkpoint requirement: {required:?}");
        }
    }

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unowned_replication_read_returns_not_owner_error_and_keeps_connection_open() {
    let (broker, dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::new()))).await;
    let (mut framed, server_task, _dir, _broker) =
        open_node_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    node_handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::ReplicationRead,
                2,
                &ReplicationRead {
                    reporter_epoch: None,
                    topic: "unowned".into(),
                    group: None,
                    partition: Partition::new(0),
                    message_from: 0,
                    event_from: 0,
                    max_messages: 10,
                    max_events: 10,
                    max_bytes: 1024 * 1024,
                    max_wait_ms: 0,
                    reporter_node_id: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 409).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn replication_apply_writes_follower_log_records() {
    let (broker, dir) = open_test_broker().await;
    broker
        .become_replication_follower("replication.apply.tcp", Partition::new(0), None)
        .await
        .unwrap();
    let (mut framed, server_task, _dir, broker) =
        open_node_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    node_handshake(&mut framed).await;

    let event_payload = StromaEvent::Enqueue {
        off: 0,
        retries: 0,
        expire_at: None,
    }
    .encode()
    .unwrap();
    framed
        .send(
            try_encode(
                Op::ReplicationApply,
                2,
                &ReplicationApply {
                    topic: "replication.apply.tcp".into(),
                    group: None,
                    partition: Partition::new(0),
                    messages: Some(ReplicationMessageApplyBatch {
                        epoch: 0,
                        records: vec![ReplicationMessageRecord {
                            offset: 0,
                            flags: 0,
                            headers: Vec::new(),
                            payload: b"replicated-follower-payload".to_vec(),
                        }],
                    }),
                    events: Some(ReplicationEventApplyBatch {
                        epoch: 0,
                        records: vec![ReplicationEventRecord {
                            offset: 0,
                            payload: event_payload,
                        }],
                    }),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::ReplicationApplyOk as u16);
    assert_eq!(frame.request_id, 2);
    let response: ReplicationApplyOk = try_decode(&frame).unwrap();
    assert!(response.messages_applied);
    assert!(response.events_applied);

    let promoted = broker
        .promote_replication_follower_if_caught_up(
            "replication.apply.tcp",
            Partition::new(0),
            None,
            1,
            1,
        )
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: 1,
            event_next_offset: 1,
            applied_event_offset: Some(0),
        }
    );

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn replication_apply_rejects_non_contiguous_records_and_keeps_connection_open() {
    let (broker, dir) = open_test_broker().await;
    broker
        .become_replication_follower("replication.apply.bad", Partition::new(0), None)
        .await
        .unwrap();
    let (mut framed, server_task, _dir, _broker) =
        open_node_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    node_handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::ReplicationApply,
                2,
                &ReplicationApply {
                    topic: "replication.apply.bad".into(),
                    group: None,
                    partition: Partition::new(0),
                    messages: Some(ReplicationMessageApplyBatch {
                        epoch: 0,
                        records: vec![
                            ReplicationMessageRecord {
                                offset: 0,
                                flags: 0,
                                headers: Vec::new(),
                                payload: b"first".to_vec(),
                            },
                            ReplicationMessageRecord {
                                offset: 2,
                                flags: 0,
                                headers: Vec::new(),
                                payload: b"gap".to_vec(),
                            },
                        ],
                    }),
                    events: None,
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
async fn replication_read_and_apply_compose_for_manual_catch_up() {
    let topic = "replication.catchup.tcp";
    let group = Some("workers".to_string());
    let (mut owner_framed, owner_task, _owner_dir) = open_node_connection().await;
    node_handshake(&mut owner_framed).await;
    framed_publish(
        &mut owner_framed,
        2,
        topic,
        group.as_deref(),
        b"first-replicated-payload",
    )
    .await;
    framed_publish(
        &mut owner_framed,
        3,
        topic,
        group.as_deref(),
        b"second-replicated-payload",
    )
    .await;

    let (follower_broker, follower_dir) = open_test_broker().await;
    follower_broker
        .become_replication_follower(topic, Partition::new(0), group.as_deref())
        .await
        .unwrap();
    let (mut follower_framed, follower_task, _follower_dir, follower_broker) =
        open_node_connection_for_broker(
            ConnectionSettings::new(Some(60)),
            follower_broker,
            follower_dir,
        )
        .await;
    node_handshake(&mut follower_framed).await;

    let outcome = catch_up_replication_over_protocol(
        &mut owner_framed,
        &mut follower_framed,
        topic,
        Partition::new(0),
        group.as_deref(),
        ProtocolReplicationCatchUpOptions {
            max_messages_per_read: 1,
            max_events_per_read: 1,
            max_iterations: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        outcome,
        ProtocolReplicationCatchUp::CaughtUp(
            fibril_protocol::v1::replication::ProtocolReplicationCatchUpProgress {
                iterations: 2,
                applied_message_records: 2,
                applied_event_records: 2,
                message_next_offset: 2,
                event_next_offset: 2,
            }
        )
    );

    let promoted = follower_broker
        .promote_replication_follower_if_caught_up(topic, Partition::new(0), group.as_deref(), 2, 2)
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: 2,
            event_next_offset: 2,
            applied_event_offset: Some(1),
        }
    );

    drop(owner_framed);
    drop(follower_framed);
    owner_task.await.unwrap().unwrap();
    follower_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_owner_replication_peer_reads_owner_records() {
    let topic = "replication.peer.read";
    let group = Some("workers".to_string());
    let (mut owner_framed, owner_task, _owner_dir) = open_node_connection().await;
    node_handshake(&mut owner_framed).await;
    framed_publish(
        &mut owner_framed,
        2,
        topic,
        group.as_deref(),
        b"peer-replicated-payload",
    )
    .await;

    let peer = ProtocolOwnerReplicationPeer::new(owner_framed);
    let records = peer
        .read_owner_replication_records(
            topic,
            Partition::new(0),
            group.as_deref(),
            0,
            0,
            8,
            8,
            usize::MAX,
            0,
        )
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message batch");
    };
    assert_eq!(messages.requested_offset, 0);
    assert_eq!(messages.next_offset, 1);
    assert_eq!(messages.records.len(), 1);
    assert_eq!(messages.records[0].0, 0);
    assert_eq!(messages.records[0].1.payload, b"peer-replicated-payload");

    let OwnerReplicationRead::Batch(events) = records.events else {
        panic!("expected event batch");
    };
    assert_eq!(events.requested_offset, 0);
    assert_eq!(events.next_offset, 1);
    assert_eq!(events.records.len(), 1);

    drop(peer);
    owner_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_owner_replication_peer_exports_checkpoint() {
    let topic = "replication.peer.checkpoint";
    let group = Some("workers".to_string());
    let (mut owner_framed, owner_task, _owner_dir) = open_node_connection().await;
    node_handshake(&mut owner_framed).await;
    framed_publish(
        &mut owner_framed,
        2,
        topic,
        group.as_deref(),
        b"checkpointed-payload",
    )
    .await;

    let peer = ProtocolOwnerReplicationPeer::new(owner_framed);
    let checkpoint = peer
        .export_owner_state_checkpoint(topic, Partition::new(0), group.as_deref())
        .await
        .unwrap();

    assert_eq!(checkpoint.message_checkpoint_offset, 0);
    assert_eq!(checkpoint.message_next_offset, 1);
    assert_eq!(checkpoint.event_next_offset, 1);
    assert_eq!(checkpoint.applied_event_offset, 0);
    assert!(!checkpoint.state_snapshot.is_empty());

    drop(peer);
    owner_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn protocol_owner_replication_peer_maps_not_owner_error() {
    let (broker, dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::new()))).await;
    let (mut framed, server_task, _dir, _broker) =
        open_node_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    node_handshake(&mut framed).await;

    let peer = ProtocolOwnerReplicationPeer::new(framed);
    let err = peer
        .read_owner_replication_records(
            "unowned",
            Partition::new(0),
            None,
            0,
            0,
            8,
            8,
            usize::MAX,
            0,
        )
        .await
        .unwrap_err();

    assert!(matches!(
        err,
        fibril_broker::broker::BrokerError::NotOwner { .. }
    ));

    drop(peer);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn static_protocol_owner_peer_resolver_reads_from_owner_node() {
    let topic = "replication.resolver.read";
    let group = Some("workers".to_string());
    let (owner_broker, owner_dir) = open_test_broker().await;
    let publisher = owner_broker
        .get_publisher(topic, Partition::new(0), &group)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"resolver-payload".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let (addr, server_task, _dir, _broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        Some(node_auth()),
    )
    .await;
    let resolver = StaticProtocolOwnerPeerResolver::with_config(
        ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
            "owner-a".to_string(),
            addr.to_string(),
        )]))
        .with_auth("@node", "secret"),
    );
    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, Partition::new(0), group.as_deref()),
        "owner-a",
        vec![],
        1,
    );

    let peer = resolver
        .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("owner peer");
    let records = peer
        .read_owner_replication_records(
            topic,
            Partition::new(0),
            group.as_deref(),
            0,
            0,
            8,
            8,
            usize::MAX,
            0,
        )
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message batch");
    };
    assert_eq!(messages.records.len(), 1);
    assert_eq!(messages.records[0].1.payload, b"resolver-payload");

    drop(peer);
    resolver.close_all().await;
    drop(resolver);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn static_protocol_owner_peer_resolver_returns_none_for_unknown_owner() {
    let resolver = StaticProtocolOwnerPeerResolver::new(HashMap::new());
    let assignment = PartitionAssignment::new(
        QueueIdentity::new("replication.resolver.missing", Partition::new(0), None),
        "missing-owner",
        vec![],
        1,
    );

    assert!(
        resolver
            .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn static_protocol_owner_peer_resolver_reuses_peer_for_owner() {
    let addr: std::net::SocketAddr = "127.0.0.1:9".parse().unwrap();
    let resolver = StaticProtocolOwnerPeerResolver::new(HashMap::from([(
        "owner-a".to_string(),
        addr.to_string(),
    )]));
    let assignment = PartitionAssignment::new(
        QueueIdentity::new("replication.resolver.cached", Partition::new(0), None),
        "owner-a",
        vec![],
        1,
    );

    let first = resolver
        .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("first owner peer");
    let second = resolver
        .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("second owner peer");

    assert!(Arc::ptr_eq(&first, &second));
}

fn protocol_coordination_snapshot(
    owner_addr: Option<SocketAddr>,
    assignment: PartitionAssignment,
    generation: u64,
) -> CoordinationSnapshot {
    let nodes = owner_addr
        .map(|broker_addr| {
            HashMap::from([(
                assignment.owner.clone(),
                NodeInfo {
                    node_id: assignment.owner.clone(),
                    broker_addr: broker_addr.to_string(),
                    admin_addr: None,
                },
            )])
        })
        .unwrap_or_default();
    CoordinationSnapshot {
        nodes,
        assignments: HashMap::from([(assignment.queue.clone(), assignment)]),
        stream_assignments: HashMap::new(),
        generation,
    }
}

#[tokio::test]
async fn coordination_protocol_owner_peer_resolver_returns_none_for_missing_owner_node() {
    let assignment = PartitionAssignment::new(
        QueueIdentity::new(
            "replication.resolver.coord.missing",
            Partition::new(0),
            None,
        ),
        "owner-a",
        vec![],
        1,
    );
    let coordination = Arc::new(StaticCoordination::new(
        "node-b",
        protocol_coordination_snapshot(None, assignment.clone(), 1),
    ));
    let resolver = CoordinationProtocolOwnerPeerResolver::new(coordination);

    assert!(
        resolver
            .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn coordination_protocol_owner_peer_resolver_reuses_stable_owner_address() {
    let assignment = PartitionAssignment::new(
        QueueIdentity::new("replication.resolver.coord.cached", Partition::new(0), None),
        "owner-a",
        vec![],
        1,
    );
    let coordination = Arc::new(StaticCoordination::new(
        "node-b",
        protocol_coordination_snapshot(
            Some("127.0.0.1:10001".parse().unwrap()),
            assignment.clone(),
            1,
        ),
    ));
    let resolver = CoordinationProtocolOwnerPeerResolver::new(coordination);

    let first = resolver
        .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("first owner peer");
    let second = resolver
        .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("second owner peer");

    assert!(Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn coordination_protocol_owner_peer_resolver_replaces_peer_when_owner_address_changes() {
    let assignment = PartitionAssignment::new(
        QueueIdentity::new(
            "replication.resolver.coord.changed",
            Partition::new(0),
            None,
        ),
        "owner-a",
        vec![],
        1,
    );
    let coordination = Arc::new(StaticCoordination::new(
        "node-b",
        protocol_coordination_snapshot(
            Some("127.0.0.1:10001".parse().unwrap()),
            assignment.clone(),
            1,
        ),
    ));
    let resolver = CoordinationProtocolOwnerPeerResolver::new(coordination.clone());

    let first = resolver
        .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("first owner peer");

    coordination.update_snapshot(protocol_coordination_snapshot(
        Some("127.0.0.1:10002".parse().unwrap()),
        assignment.clone(),
        2,
    ));

    let second = resolver
        .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("second owner peer");

    assert!(!Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn coordination_protocol_owner_peer_resolver_uses_assignment_owner_after_move() {
    let queue = QueueIdentity::new("replication.resolver.coord.moved", Partition::new(0), None);
    let first_assignment = PartitionAssignment::new(queue.clone(), "owner-a", vec![], 1);
    let second_assignment = PartitionAssignment::new(queue.clone(), "owner-b", vec![], 2);
    let mut nodes = HashMap::new();
    nodes.insert(
        "owner-a".to_string(),
        NodeInfo {
            node_id: "owner-a".to_string(),
            broker_addr: "127.0.0.1:10001".to_string(),
            admin_addr: None,
        },
    );
    nodes.insert(
        "owner-b".to_string(),
        NodeInfo {
            node_id: "owner-b".to_string(),
            broker_addr: "127.0.0.1:10002".to_string(),
            admin_addr: None,
        },
    );
    let coordination = Arc::new(StaticCoordination::new(
        "node-c",
        CoordinationSnapshot {
            nodes: nodes.clone(),
            assignments: HashMap::from([(queue.clone(), first_assignment.clone())]),
            stream_assignments: HashMap::new(),
            generation: 1,
        },
    ));
    let resolver = CoordinationProtocolOwnerPeerResolver::new(coordination.clone());

    let first = resolver
        .resolve_owner_peer(&first_assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("first owner peer");

    coordination.update_snapshot(CoordinationSnapshot {
        nodes,
        assignments: HashMap::from([(queue, second_assignment.clone())]),
        stream_assignments: HashMap::new(),
        generation: 2,
    });

    let second = resolver
        .resolve_owner_peer(&second_assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("second owner peer");

    assert!(!Arc::ptr_eq(&first, &second));
}

#[tokio::test]
async fn static_protocol_owner_peer_resolver_can_authenticate() {
    let topic = "replication.resolver.auth";
    let (owner_broker, owner_dir) = open_test_broker().await;
    let publisher = owner_broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"auth-payload".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let (addr, server_task, _dir, _broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        Some(node_auth()),
    )
    .await;
    let resolver = StaticProtocolOwnerPeerResolver::with_config(
        ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
            "owner-a".to_string(),
            addr.to_string(),
        )]))
        .with_auth("@node", "secret"),
    );
    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, Partition::new(0), None),
        "owner-a",
        vec![],
        1,
    );

    let peer = resolver
        .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("owner peer");
    let records = peer
        .read_owner_replication_records(topic, Partition::new(0), None, 0, 0, 8, 8, usize::MAX, 0)
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message batch");
    };
    assert_eq!(messages.records[0].1.payload, b"auth-payload");

    drop(peer);
    drop(resolver);
    server_task.await.unwrap().unwrap();
}

/// Replication over TLS end to end: the owner serves its protocol port
/// behind a TLS acceptor built from generated material, the follower
/// resolver dials it with a connector trusting the deployment CA, and a
/// replication read returns the owner's records over the encrypted link.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owner_peer_replication_works_over_tls() {
    let topic = "replication.resolver.tls";
    let (owner_broker, _owner_dir) = open_test_broker().await;
    let publisher = owner_broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"tls-payload".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let tls_dir = TempDir {
        root: std::env::current_dir()
            .unwrap()
            .join("test_data")
            .join(format!("protocol_tls_material-{}", Uuid::now_v7())),
    };
    std::fs::create_dir_all(&tls_dir.root).unwrap();
    let server_tls =
        fibril_tls::build_server_tls(&fibril_tls::TlsMode::AutoSelfSigned, &tls_dir.root, &[])
            .unwrap()
            .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let acceptor = server_tls.acceptor.clone();
    let server_broker = owner_broker.clone();
    let server_task = tokio::spawn(async move {
        let (tcp, peer) = listener.accept().await.unwrap();
        let stream = acceptor.accept(tcp).await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            stream,
            Some(peer),
            server_broker,
            tcp_stats,
            connection_stats,
            conn_id,
            Some(node_auth()),
            None,
            ConnectionSettings::new(Some(60)),
            None,
            None,
            None,
        )
        .await
    });

    let connector = fibril_tls::build_peer_connector(None, &tls_dir.root).unwrap();
    let resolver = StaticProtocolOwnerPeerResolver::with_config(
        ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
            "owner-a".to_string(),
            addr.to_string(),
        )]))
        .with_auth("@node", "secret")
        .with_tls(connector),
    );
    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, Partition::new(0), None),
        "owner-a",
        vec![],
        1,
    );

    let peer = resolver
        .resolve_owner_peer(&assignment, ReplicationResourceKind::Queue)
        .await
        .unwrap()
        .expect("owner peer");
    let records = peer
        .read_owner_replication_records(topic, Partition::new(0), None, 0, 0, 8, 8, usize::MAX, 0)
        .await
        .unwrap();

    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected message batch");
    };
    assert_eq!(messages.records[0].1.payload, b"tls-payload");

    drop(peer);
    drop(resolver);
    server_task.await.unwrap().unwrap();
}

/// Supervised assignment watcher reacts to a CONTROLLER-written
/// coordination assignment, starts the follower loop, resolves the owner from
/// the snapshot's node table, and replicates over real protocol TCP.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ganglion_coordination_drives_supervised_follower_replication() {
    use fibril_coordination_ganglion::GanglionCoordination;
    use ganglion_openraft::{InProcessRouter, RaftMetadataNode, default_raft_config};

    let topic = "replication.coordination.supervised";

    // Embedded coordinator; this provider belongs to the FOLLOWER broker.
    let router = InProcessRouter::new();
    let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
        .await
        .unwrap();
    let mut members = std::collections::BTreeMap::new();
    members.insert(
        1u64,
        ganglion_openraft::openraft::BasicNode::new("coordinator"),
    );
    raft_node.initialize(members).await.unwrap();
    raft_node
        .wait_for_leader(1, Duration::from_secs(10))
        .await
        .unwrap();
    let coordination = Arc::new(GanglionCoordination::new("b-follower", raft_node));

    // Owner broker with data, serving the replication protocol on a real port.
    let (owner_broker, owner_dir) = open_test_broker().await;
    let publisher = owner_broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    for payload in [b"coord-first".as_slice(), b"coord-second".as_slice()] {
        let reply = publisher
            .publish(
                payload.to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        reply.await.unwrap().unwrap();
    }
    let owner_checkpoint = owner_broker
        .export_owner_state_checkpoint(topic, Partition::new(0), None)
        .await
        .unwrap();
    let (owner_addr, server_task, _owner_dir, owner_broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        Some(node_auth()),
    )
    .await;

    // Cluster facts: both brokers registered (owner's broker_addr is the real
    // listener — the resolver dials it from the snapshot), queue in catalogue.
    let node = |id: &str, addr: std::net::SocketAddr| fibril_broker::coordination::NodeInfo {
        node_id: id.to_string(),
        broker_addr: addr.to_string(),
        admin_addr: None,
    };
    coordination
        .register_self(&node("a-owner", owner_addr))
        .await
        .unwrap();
    coordination
        .register_self(&node("b-follower", "127.0.0.1:1".parse().unwrap()))
        .await
        .unwrap();
    register_legacy_test_queue(&coordination, &QueueIdentity::new(topic, Partition::new(0), None)).await;

    // Follower broker: ONLY the supervised watcher — no manual transitions.
    let (follower_broker, _follower_dir) = open_test_broker().await;
    let resolver = Arc::new(
        fibril_protocol::v1::replication::CoordinationProtocolOwnerPeerResolver::with_config(
            coordination.clone(),
            ProtocolOwnerPeerResolverConfig::new(HashMap::new())
                .with_auth("@node", "secret")
                .with_reporter("b-follower"),
        ),
    );
    follower_broker.spawn_assignment_watcher_with_follower_replication(
        coordination.clone(),
        resolver.clone(),
        FollowerReplicationWorkerConfig {
            caught_up_poll_ms: 60_000,
            ..Default::default()
        },
    );

    // The controller writes the assignment (deterministic placement: sorted
    // node order makes a-owner the owner, b-follower the follower).
    let live = coordination.live_nodes(Duration::from_secs(30));
    let committed = coordination
        .control_iteration(
            &fibril_broker::coordination::DeterministicPartitionPlacement,
            &coordination.registered_queues(),
            &fibril_broker::coordination::DeterministicStreamPlacement,
            &coordination.registered_streams(),
            1,
            1,
            ReplicationDurabilityPolicy::LocalDurable,
            &live,
            8,
        )
        .await
        .unwrap()
        .expect("leader iteration");
    let assignment = committed
        .assignment_for(topic, Partition::new(0), None)
        .expect("assigned")
        .clone();
    assert_eq!(assignment.owner, "a-owner");
    assert_eq!(assignment.followers, vec!["b-follower".to_string()]);
    // The owner broker has no watcher in this harness; apply what its
    // watcher's BecomeOwner would: fence its logs at the assignment epoch so
    // its replication reads carry the fenced epoch.
    owner_broker.cache_queue_assignment(&assignment);
    owner_broker
        .advance_replication_epoch(topic, Partition::new(0), None, assignment.epoch)
        .await
        .unwrap();

    // The watcher must start the worker and replicate to caught-up.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = follower_broker
                .follower_replication_worker_snapshot(topic, Partition::new(0), None)
                .await;
            if state
                .as_ref()
                .is_some_and(|state| state.status == FollowerReplicationWorkerStatus::CaughtUp)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("supervised follower should catch up from the coordination assignment");

    // Wire path: the follower's stamped reads reported its durable
    // progress to the owner.
    let progress = owner_broker.follower_replication_progress(topic, Partition::new(0), None);
    assert!(
        progress
            .iter()
            .any(|(node, (message_next, _))| node == "b-follower"
                && *message_next >= owner_checkpoint.message_next_offset),
        "owner must have follower progress from stamped reads: {progress:?}"
    );

    // Replicated tails match the owner checkpoint exactly.
    let promoted = follower_broker
        .promote_replication_follower_if_caught_up(
            topic,
            Partition::new(0),
            None,
            owner_checkpoint.message_next_offset,
            owner_checkpoint.event_next_offset,
        )
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: owner_checkpoint.message_next_offset,
            event_next_offset: owner_checkpoint.event_next_offset,
            applied_event_offset: Some(owner_checkpoint.applied_event_offset),
        }
    );

    coordination.consensus_node().shutdown().await.unwrap();
    resolver.close_all().await;
    follower_broker.shutdown().await;
    owner_broker.shutdown().await;
    server_task.await.unwrap().unwrap();
}

/// Owner death drives the full failover choreography with no manual
/// steps — TTL drops the owner from the live set, the controller reassigns
/// with an epoch bump, and the follower's supervised watcher drains its
/// worker, promotes at local tails, and starts serving as owner.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ganglion_owner_death_fails_over_to_caught_up_follower() {
    use fibril_coordination_ganglion::GanglionCoordination;
    use ganglion_openraft::{InProcessRouter, RaftMetadataNode, default_raft_config};

    let topic = "replication.coordination.failover";

    let router = InProcessRouter::new();
    let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
        .await
        .unwrap();
    let mut members = std::collections::BTreeMap::new();
    members.insert(
        1u64,
        ganglion_openraft::openraft::BasicNode::new("coordinator"),
    );
    raft_node.initialize(members).await.unwrap();
    raft_node
        .wait_for_leader(1, Duration::from_secs(10))
        .await
        .unwrap();
    let coordination = Arc::new(GanglionCoordination::new("b-follower", raft_node));

    // Owner broker with two committed messages, serving replication over TCP.
    let (owner_broker, owner_dir) = open_test_broker().await;
    let publisher = owner_broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    for payload in [b"failover-first".as_slice(), b"failover-second".as_slice()] {
        let reply = publisher
            .publish(
                payload.to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        reply.await.unwrap().unwrap();
    }
    let owner_checkpoint = owner_broker
        .export_owner_state_checkpoint(topic, Partition::new(0), None)
        .await
        .unwrap();
    let (owner_addr, server_task, _owner_dir, owner_broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        Some(node_auth()),
    )
    .await;

    let node = |id: &str, addr: std::net::SocketAddr| fibril_broker::coordination::NodeInfo {
        node_id: id.to_string(),
        broker_addr: addr.to_string(),
        admin_addr: None,
    };
    coordination
        .register_self(&node("a-owner", owner_addr))
        .await
        .unwrap();
    coordination
        .register_self(&node("b-follower", "127.0.0.1:1".parse().unwrap()))
        .await
        .unwrap();
    register_legacy_test_queue(&coordination, &QueueIdentity::new(topic, Partition::new(0), None)).await;

    let (follower_broker, _follower_dir) = open_test_broker().await;
    let resolver = Arc::new(
        fibril_protocol::v1::replication::CoordinationProtocolOwnerPeerResolver::with_config(
            coordination.clone(),
            ProtocolOwnerPeerResolverConfig::new(HashMap::new())
                .with_auth("@node", "secret")
                .with_reporter("b-follower"),
        ),
    );
    follower_broker.spawn_assignment_watcher_with_follower_replication(
        coordination.clone(),
        resolver.clone(),
        FollowerReplicationWorkerConfig {
            caught_up_poll_ms: 60_000,
            ..Default::default()
        },
    );

    // Phase 1: normal assignment; the follower replicates to caught-up.
    let live = coordination.live_nodes(Duration::from_secs(30));
    let committed = coordination
        .control_iteration(
            &fibril_broker::coordination::DeterministicPartitionPlacement,
            &coordination.registered_queues(),
            &fibril_broker::coordination::DeterministicStreamPlacement,
            &coordination.registered_streams(),
            1,
            1,
            ReplicationDurabilityPolicy::LocalDurable,
            &live,
            8,
        )
        .await
        .unwrap()
        .expect("leader iteration");
    let first = committed
        .assignment_for(topic, Partition::new(0), None)
        .expect("assigned")
        .clone();
    assert_eq!(first.owner, "a-owner");
    assert_eq!(first.epoch, 1);
    // Watcher-less harness owner: fence at the assignment epoch, as its own
    // watcher's BecomeOwner would in production.
    owner_broker
        .advance_replication_epoch(topic, Partition::new(0), None, first.epoch)
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let state = follower_broker
                .follower_replication_worker_snapshot(topic, Partition::new(0), None)
                .await;
            if state
                .as_ref()
                .is_some_and(|state| state.status == FollowerReplicationWorkerStatus::CaughtUp)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("follower catches up before the failover");

    // Phase 2: the owner dies — only the follower stays in the live set.
    let mut live_after_death = std::collections::HashMap::new();
    live_after_death.insert(
        "b-follower".to_string(),
        node("b-follower", "127.0.0.1:1".parse().unwrap()),
    );
    let committed = coordination
        .control_iteration(
            &fibril_broker::coordination::DeterministicPartitionPlacement,
            &coordination.registered_queues(),
            &fibril_broker::coordination::DeterministicStreamPlacement,
            &coordination.registered_streams(),
            1,
            1,
            ReplicationDurabilityPolicy::LocalDurable,
            &live_after_death,
            8,
        )
        .await
        .unwrap()
        .expect("failover iteration");
    let moved = committed
        .assignment_for(topic, Partition::new(0), None)
        .expect("still assigned")
        .clone();
    assert_eq!(moved.owner, "b-follower", "ownership must move");
    assert_eq!(moved.epoch, first.epoch + 1, "the move must fence");

    // Phase 3: the watcher promotes the follower at its local tails and the
    // broker serves as owner — verified by a successful new publish.
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if fibril_broker::broker::QueueOwnership::owns_queue(
                coordination.as_ref(),
                topic,
                Partition::new(0),
                None,
            ) {
                if let Ok(publisher) = follower_broker
                    .get_publisher(topic, Partition::new(0), &None)
                    .await
                {
                    let reply = publisher
                        .publish(
                            b"post-failover".to_vec(),
                            unix_millis(),
                            unix_millis(),
                            None,
                            Default::default(),
                            None,
                        )
                        .await;
                    if let Ok(reply) = reply {
                        if reply.await.unwrap().is_ok() {
                            break;
                        }
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("promoted follower must accept owner traffic after failover");

    // The promoted log continued from exactly the replicated tails.
    let promoted_checkpoint = follower_broker
        .export_owner_state_checkpoint(topic, Partition::new(0), None)
        .await
        .unwrap();
    assert_eq!(
        promoted_checkpoint.message_next_offset,
        owner_checkpoint.message_next_offset + 1,
        "exactly the replicated history plus the post-failover publish"
    );

    coordination.consensus_node().shutdown().await.unwrap();
    resolver.close_all().await;
    follower_broker.shutdown().await;
    owner_broker.shutdown().await;
    server_task.await.unwrap().unwrap();
}

/// Adversarial: the OLD owner observes its demotion through its own
/// watcher when it comes back — owner runtime torn down, queue demoted to
/// follower, new owner publishes rejected locally.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ganglion_returning_old_owner_is_demoted_and_refuses_publishes() {
    use fibril_coordination_ganglion::GanglionCoordination;
    use ganglion_openraft::{InProcessRouter, RaftMetadataNode, default_raft_config};

    let topic = "replication.coordination.old-owner";

    let router = InProcessRouter::new();
    let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
        .await
        .unwrap();
    let mut members = std::collections::BTreeMap::new();
    members.insert(
        1u64,
        ganglion_openraft::openraft::BasicNode::new("coordinator"),
    );
    raft_node.initialize(members).await.unwrap();
    raft_node
        .wait_for_leader(1, Duration::from_secs(10))
        .await
        .unwrap();
    // This provider belongs to the OWNER broker.
    let coordination = Arc::new(GanglionCoordination::new("a-owner", raft_node));

    let (owner_broker, _owner_dir) = open_test_broker().await;
    let publisher = owner_broker
        .get_publisher(topic, Partition::new(0), &None)
        .await
        .unwrap();
    let reply = publisher
        .publish(
            b"pre-fence".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    reply.await.unwrap().unwrap();

    let node = |id: &str, port: u16| fibril_broker::coordination::NodeInfo {
        node_id: id.to_string(),
        broker_addr: format!("127.0.0.1:{port}"),
        admin_addr: None,
    };
    coordination
        .register_self(&node("a-owner", 9100))
        .await
        .unwrap();
    coordination
        .register_self(&node("b-follower", 9101))
        .await
        .unwrap();
    register_legacy_test_queue(&coordination, &QueueIdentity::new(topic, Partition::new(0), None)).await;

    // The owner broker runs the supervised watcher (it will see both the
    // initial ownership and, later, its own demotion).
    let resolver = Arc::new(
        fibril_protocol::v1::replication::CoordinationProtocolOwnerPeerResolver::new(
            coordination.clone(),
        ),
    );
    owner_broker.spawn_assignment_watcher_with_follower_replication(
        coordination.clone(),
        resolver,
        FollowerReplicationWorkerConfig {
            caught_up_poll_ms: 60_000,
            ..Default::default()
        },
    );

    let live = coordination.live_nodes(Duration::from_secs(30));
    let committed = coordination
        .control_iteration(
            &fibril_broker::coordination::DeterministicPartitionPlacement,
            &coordination.registered_queues(),
            &fibril_broker::coordination::DeterministicStreamPlacement,
            &coordination.registered_streams(),
            1,
            1,
            ReplicationDurabilityPolicy::LocalDurable,
            &live,
            8,
        )
        .await
        .unwrap()
        .expect("leader iteration");
    let first = committed
        .assignment_for(topic, Partition::new(0), None)
        .expect("assigned")
        .clone();
    assert_eq!(first.owner, "a-owner");
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(checkpoint) = owner_broker
                .export_owner_state_checkpoint(topic, Partition::new(0), None)
                .await
            {
                if checkpoint.message_epoch == first.epoch && checkpoint.event_epoch == first.epoch
                {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("old owner watcher should observe initial ownership before failover");

    // Failover away from a-owner (simulates: it was partitioned, the cluster
    // moved on, now its watcher sees the fenced assignment).
    let mut live_without_owner = std::collections::HashMap::new();
    live_without_owner.insert("b-follower".to_string(), node("b-follower", 9101));
    let committed = coordination
        .control_iteration(
            &fibril_broker::coordination::DeterministicPartitionPlacement,
            &coordination.registered_queues(),
            &fibril_broker::coordination::DeterministicStreamPlacement,
            &coordination.registered_streams(),
            1,
            1,
            ReplicationDurabilityPolicy::LocalDurable,
            &live_without_owner,
            8,
        )
        .await
        .unwrap()
        .expect("failover iteration");
    let moved = committed
        .assignment_for(topic, Partition::new(0), None)
        .expect("assigned")
        .clone();
    assert_eq!(moved.owner, "b-follower");
    assert_eq!(moved.epoch, first.epoch + 1);

    // The old owner demotes itself: its owner runtime is cancelled and the
    // engine refuses owner traffic, so writes on the EXISTING publisher fail (no
    // silent stale writes). We assert through the existing `publisher` rather than
    // a fresh `get_publisher`: this test broker uses OwnAllQueues (no ownership
    // gate), so a fresh get_publisher would re-materialize the queue as owner past
    // the freeze - a path the real coordination gate forbids in a cluster, and a
    // Stroma role-durability gap tracked in FOLLOWUPS.md (durable queue role).
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let refused = match publisher
                .publish(
                    b"stale-after-fence".to_vec(),
                    unix_millis(),
                    unix_millis(),
                    None,
                    Default::default(),
                    None,
                )
                .await
            {
                Ok(reply) => reply.await.map(|inner| inner.is_err()).unwrap_or(true),
                Err(_) => true,
            };
            if refused {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("demoted old owner must refuse writes on the existing publisher");

    coordination.consensus_node().shutdown().await.unwrap();
    owner_broker.shutdown().await;
}

#[tokio::test]
async fn follower_worker_loop_catches_up_over_static_protocol_resolver() {
    let topic = "replication.resolver.loop";
    let group = Some("workers".to_string());
    let (owner_broker, owner_dir) = open_test_broker().await;
    let publisher = owner_broker
        .get_publisher(topic, Partition::new(0), &group)
        .await
        .unwrap();
    for payload in [b"loop-first".as_slice(), b"loop-second".as_slice()] {
        let reply = publisher
            .publish(
                payload.to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        reply.await.unwrap().unwrap();
    }
    let owner_checkpoint = owner_broker
        .export_owner_state_checkpoint(topic, Partition::new(0), group.as_deref())
        .await
        .unwrap();

    let (addr, server_task, _owner_dir, owner_broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        Some(node_auth()),
    )
    .await;
    let resolver = StaticProtocolOwnerPeerResolver::with_config(
        ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
            "owner-a".to_string(),
            addr.to_string(),
        )]))
        .with_auth("@node", "secret"),
    );
    let resolver = Arc::new(resolver);

    let (follower_broker, _follower_dir) = open_test_broker().await;
    follower_broker
        .apply_assignment_transition(&follower_assignment_transition(topic, group.as_deref()))
        .await
        .unwrap();

    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, Partition::new(0), group.as_deref()),
        "owner-a",
        vec!["follower-a".to_string()],
        1,
    );
    let shutdown = CancellationToken::new();
    let cfg = FollowerReplicationWorkerConfig {
        caught_up_poll_ms: 60_000,
        ..Default::default()
    };

    let observer = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = follower_broker
                    .follower_replication_worker_snapshot(
                        topic,
                        Partition::new(0),
                        group.as_deref(),
                    )
                    .await;
                if state
                    .as_ref()
                    .is_some_and(|state| state.status == FollowerReplicationWorkerStatus::CaughtUp)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("follower worker should catch up over protocol");
        shutdown.cancel();
    };
    let loop_task = follower_broker.run_follower_replication_worker_loop(
        assignment,
        resolver.clone(),
        ReplicationResourceKind::Queue,
        cfg,
        shutdown.clone(),
    );
    let (_, loop_outcome) = tokio::join!(observer, loop_task);

    let FollowerReplicationWorkerLoopExit::Cancelled { ticks } = loop_outcome.unwrap() else {
        panic!("follower worker loop should exit by cancellation");
    };
    assert!(ticks >= 1, "worker must run at least one catch-up tick");
    let promoted = follower_broker
        .promote_replication_follower_if_caught_up(
            topic,
            Partition::new(0),
            group.as_deref(),
            owner_checkpoint.message_next_offset,
            owner_checkpoint.event_next_offset,
        )
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: owner_checkpoint.message_next_offset,
            event_next_offset: owner_checkpoint.event_next_offset,
            applied_event_offset: Some(owner_checkpoint.applied_event_offset),
        }
    );

    resolver.close_all().await;
    follower_broker.shutdown().await;
    owner_broker.shutdown().await;
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn follower_worker_loop_installs_checkpoint_over_static_protocol_resolver() {
    let topic = "replication.resolver.checkpoint";
    let group = Some("workers".to_string());
    let (owner_broker, _owner_dir) = open_test_broker().await;
    let publisher = owner_broker
        .get_publisher(topic, Partition::new(0), &group)
        .await
        .unwrap();
    for payload in [
        b"checkpoint-loop-first".as_slice(),
        b"checkpoint-loop-second".as_slice(),
    ] {
        let reply = publisher
            .publish(
                payload.to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        reply.await.unwrap().unwrap();
    }
    let owner_checkpoint = owner_broker
        .export_owner_state_checkpoint(topic, Partition::new(0), group.as_deref())
        .await
        .unwrap();
    let owner_records = owner_broker
        .read_owner_replication_records(
            topic,
            Partition::new(0),
            group.as_deref(),
            0,
            0,
            8,
            8,
            usize::MAX,
            0,
        )
        .await
        .unwrap();
    let OwnerReplicationRead::Batch(messages) = owner_records.messages else {
        panic!("expected owner message batch");
    };
    let OwnerReplicationRead::Batch(events) = owner_records.events else {
        panic!("expected owner event batch");
    };
    let message_records = messages
        .records
        .into_iter()
        .map(|(offset, message)| ReplicationMessageRecord {
            offset,
            flags: message.flags,
            headers: message.headers,
            payload: message.payload,
        })
        .collect::<Vec<_>>();
    let event_records = events
        .records
        .into_iter()
        .map(|(offset, event)| ReplicationEventRecord {
            offset,
            payload: event.encode().expect("owner event should encode"),
        })
        .collect::<Vec<_>>();

    let checkpoint = ReplicationStateCheckpoint {
        message_epoch: owner_checkpoint.message_epoch,
        event_epoch: owner_checkpoint.event_epoch,
        message_checkpoint_offset: owner_checkpoint.message_checkpoint_offset,
        message_next_offset: owner_checkpoint.message_next_offset,
        event_next_offset: owner_checkpoint.event_next_offset,
        applied_event_offset: owner_checkpoint.applied_event_offset,
        state_snapshot: owner_checkpoint.state_snapshot.clone(),
    };
    let (addr, server_task) =
        start_checkpoint_required_owner_server(checkpoint, message_records, event_records).await;
    let resolver = StaticProtocolOwnerPeerResolver::new(HashMap::from([(
        "owner-a".to_string(),
        addr.to_string(),
    )]));
    let resolver = Arc::new(resolver);

    let (follower_broker, _follower_dir) = open_test_broker().await;
    follower_broker
        .apply_assignment_transition(&follower_assignment_transition(topic, group.as_deref()))
        .await
        .unwrap();

    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, Partition::new(0), group.as_deref()),
        "owner-a",
        vec!["follower-a".to_string()],
        1,
    );
    let shutdown = CancellationToken::new();
    let cfg = FollowerReplicationWorkerConfig {
        allow_checkpoint_install: true,
        checkpoint_retry_poll_ms: 1,
        caught_up_poll_ms: 60_000,
        ..Default::default()
    };

    let observer = async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = follower_broker
                    .follower_replication_worker_snapshot(
                        topic,
                        Partition::new(0),
                        group.as_deref(),
                    )
                    .await;
                if state
                    .as_ref()
                    .is_some_and(|state| state.status == FollowerReplicationWorkerStatus::CaughtUp)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("follower worker should install checkpoint and catch up");
        shutdown.cancel();
    };
    let loop_task = follower_broker.run_follower_replication_worker_loop(
        assignment,
        resolver.clone(),
        ReplicationResourceKind::Queue,
        cfg,
        shutdown.clone(),
    );
    let (_, loop_outcome) = tokio::join!(observer, loop_task);

    let worker_state = follower_broker
        .follower_replication_worker_snapshot(topic, Partition::new(0), group.as_deref())
        .await
        .expect("follower worker state should exist after checkpoint loop");
    assert_eq!(
        worker_state.message_next_offset,
        owner_checkpoint.message_next_offset
    );
    assert_eq!(
        worker_state.event_next_offset,
        owner_checkpoint.event_next_offset
    );

    let FollowerReplicationWorkerLoopExit::Cancelled { ticks } = loop_outcome.unwrap() else {
        panic!("follower worker loop should exit by cancellation");
    };
    assert!(
        ticks >= 1,
        "worker should perform at least one checkpoint-aware catch-up tick"
    );
    let promoted = follower_broker
        .promote_replication_follower_if_caught_up(
            topic,
            Partition::new(0),
            group.as_deref(),
            owner_checkpoint.message_next_offset,
            owner_checkpoint.event_next_offset,
        )
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: owner_checkpoint.message_next_offset,
            event_next_offset: owner_checkpoint.event_next_offset,
            applied_event_offset: Some(owner_checkpoint.applied_event_offset),
        }
    );

    resolver.close_all().await;
    follower_broker.shutdown().await;
    owner_broker.shutdown().await;
    server_task.await.unwrap().unwrap();
}

/// End-to-end durability contract between two real brokers over TCP: a publish
/// under a replica-durable policy resolves its confirm ONLY because the
/// follower replicates the record over the wire and its stamped reads advance
/// its durable progress past the offset on the owner's confirm gate.
#[tokio::test]
async fn replica_durable_confirm_resolves_over_wire_from_follower_progress() {
    replica_durable_confirm_over_wire(false).await;
}

#[tokio::test]
async fn replica_durable_confirm_resolves_over_authenticated_stream() {
    replica_durable_confirm_over_wire(true).await;
}

async fn replica_durable_confirm_over_wire(stream_enabled: bool) {
    let topic = "confirm.over.wire";
    let group: Option<String> = None;

    let (owner_broker, owner_dir) = open_test_broker().await;
    let (addr, server_task, _owner_dir, owner_broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        Some(node_auth()),
    )
    .await;

    // The owner owns the queue; its assignment demands two durable nodes (owner
    // plus one follower).
    owner_broker.cache_queue_assignment(
        &PartitionAssignment::new(
            QueueIdentity::new(topic, Partition::new(0), group.as_deref()),
            "owner-a",
            vec!["follower-a".to_string()],
            1,
        )
        .with_durability(ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 }),
    );

    // The follower materializes the queue and replicates from the owner over
    // TCP, stamping its reports so the owner's gate can observe its progress.
    let (follower_broker, _follower_dir) = open_test_broker().await;
    follower_broker
        .apply_assignment_transition(&follower_assignment_transition(topic, group.as_deref()))
        .await
        .unwrap();
    let resolver = Arc::new(StaticProtocolOwnerPeerResolver::with_config(
        ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
            "owner-a".to_string(),
            addr.to_string(),
        )]))
        .with_auth("@node", "secret")
        .with_reporter("follower-a"),
    ));
    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, Partition::new(0), group.as_deref()),
        "owner-a",
        vec!["follower-a".to_string()],
        1,
    );
    let shutdown = CancellationToken::new();
    // Keep polling briskly so the follower picks up the new record promptly.
    let worker_cfg = FollowerReplicationWorkerConfig {
        stream_enabled,
        caught_up_poll_ms: 50,
        retry_poll_ms: 50,
        ..Default::default()
    };
    let loop_task = follower_broker.run_follower_replication_worker_loop(
        assignment,
        resolver.clone(),
        ReplicationResourceKind::Queue,
        worker_cfg,
        shutdown.clone(),
    );

    let publish_and_check = async {
        let publisher = owner_broker
            .get_publisher(topic, Partition::new(0), &group)
            .await
            .unwrap();
        let reply = publisher
            .publish(
                b"over-the-wire".to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        // The confirm can only resolve via the follower's wire replication.
        let offset = tokio::time::timeout(Duration::from_secs(8), reply)
            .await
            .expect("confirm should resolve well within bound")
            .unwrap()
            .expect("replica-durable confirm resolves from follower wire progress");
        assert_eq!(offset, 0);

        // And the owner recorded that progress from the stamped reads.
        let progress =
            owner_broker.follower_replication_progress(topic, Partition::new(0), group.as_deref());
        let follower = progress
            .iter()
            .find(|(node, _)| node == "follower-a")
            .expect("owner recorded follower progress over the wire");
        assert!(
            follower.1.0 > 0,
            "follower durable message_next must pass the published offset"
        );

        shutdown.cancel();
    };

    let (_, loop_outcome) = tokio::join!(publish_and_check, loop_task);
    loop_outcome.unwrap();

    resolver.close_all().await;
    follower_broker.shutdown().await;
    owner_broker.shutdown().await;
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn replica_durable_confirm_stays_pending_until_follower_connects() {
    let topic = "confirm.waits.for.follower";
    let group: Option<String> = None;

    let (owner_broker, owner_dir) = open_test_broker().await;
    let (addr, server_task, _owner_dir, owner_broker) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        owner_broker,
        owner_dir,
        Some(node_auth()),
    )
    .await;

    // The owner owns the queue; its assignment demands two durable nodes (owner
    // plus one follower).
    owner_broker.cache_queue_assignment(
        &PartitionAssignment::new(
            QueueIdentity::new(topic, Partition::new(0), group.as_deref()),
            "owner-a",
            vec!["follower-a".to_string()],
            1,
        )
        .with_durability(ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 }),
    );

    // The follower materializes the queue and replicates from the owner over
    // TCP, stamping its reports so the owner's gate can observe its progress.
    let (follower_broker, _follower_dir) = open_test_broker().await;
    follower_broker
        .apply_assignment_transition(&follower_assignment_transition(topic, group.as_deref()))
        .await
        .unwrap();
    let resolver = Arc::new(StaticProtocolOwnerPeerResolver::with_config(
        ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
            "owner-a".to_string(),
            addr.to_string(),
        )]))
        .with_auth("@node", "secret")
        .with_reporter("follower-a"),
    ));
    let assignment = PartitionAssignment::new(
        QueueIdentity::new(topic, Partition::new(0), group.as_deref()),
        "owner-a",
        vec!["follower-a".to_string()],
        1,
    );
    let shutdown = CancellationToken::new();
    // Keep polling briskly so the follower picks up the new record promptly.
    let worker_cfg = FollowerReplicationWorkerConfig {
        caught_up_poll_ms: 50,
        retry_poll_ms: 50,
        ..Default::default()
    };
    let publisher = owner_broker
        .get_publisher(topic, Partition::new(0), &group)
        .await
        .unwrap();
    let mut reply = publisher
        .publish(
            b"over-the-wire".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    // No follower transport is being polled yet. Even a promptly flushed owner
    // socket must not release a replica-durable publisher confirmation here.
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut reply)
            .await
            .is_err(),
        "confirmation must remain pending until a required follower is durable"
    );
    assert!(
        owner_broker
            .follower_replication_progress(topic, Partition::new(0), group.as_deref())
            .is_empty()
    );

    let loop_task = follower_broker.run_follower_replication_worker_loop(
        assignment,
        resolver.clone(),
        ReplicationResourceKind::Queue,
        worker_cfg,
        shutdown.clone(),
    );

    let publish_and_check = async {
        // The confirm can only resolve via the follower's wire replication.
        let offset = tokio::time::timeout(Duration::from_secs(8), reply)
            .await
            .expect("confirm should resolve well within bound")
            .unwrap()
            .expect("replica-durable confirm resolves from follower wire progress");
        assert_eq!(offset, 0);

        // And the owner recorded that progress from the stamped reads.
        let progress =
            owner_broker.follower_replication_progress(topic, Partition::new(0), group.as_deref());
        let follower = progress
            .iter()
            .find(|(node, _)| node == "follower-a")
            .expect("owner recorded follower progress over the wire");
        assert!(
            follower.1.0 > 0,
            "follower durable message_next must pass the published offset"
        );

        shutdown.cancel();
    };

    let (_, loop_outcome) = tokio::join!(publish_and_check, loop_task);
    loop_outcome.unwrap();

    resolver.close_all().await;
    follower_broker.shutdown().await;
    owner_broker.shutdown().await;
    server_task.await.unwrap().unwrap();
}

/// Test topology source returning a fixed snapshot.
#[derive(Clone)]
struct FixedTopology(TopologyOk);
impl ClientTopologySource for FixedTopology {
    fn topology(&self) -> TopologyOk {
        self.0.clone()
    }
    fn owner_endpoint(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Option<(String, u64)> {
        self.0
            .queues
            .iter()
            .find(|q| q.topic == topic && q.partition == partition && q.group.as_deref() == group)
            .and_then(|q| {
                q.owner_endpoints
                    .first()
                    .map(|e| (e.target(), q.partitioning_version))
            })
    }
}

/// A topology source whose generation can be bumped at runtime, to drive the
/// broker's change-detected push.
struct BumpingTopology {
    generation: Arc<std::sync::atomic::AtomicU64>,
}
impl ClientTopologySource for BumpingTopology {
    fn topology(&self) -> TopologyOk {
        let generation = self.generation.load(std::sync::atomic::Ordering::SeqCst);
        TopologyOk {
            generation,
            // The routing content tracks the generation (partitioning_version), so
            // bumping the generation is a real content change the broker pushes on.
            queues: vec![QueueTopologyEntry {
                topic: "jobs".into(),
                partition: Partition::new(0),
                group: None,
                owner_endpoints: vec![
                    AdvertisedAddress::parse("127.0.0.1:7000").expect("valid test owner endpoint"),
                ],
                partitioning_version: generation,
                partition_count: 1,
            }],
            streams: Vec::new(),
        }
    }
    fn generation(&self) -> u64 {
        self.generation.load(std::sync::atomic::Ordering::SeqCst)
    }
    fn owner_endpoint(
        &self,
        _topic: &str,
        _partition: Partition,
        _group: Option<&str>,
    ) -> Option<(String, u64)> {
        None
    }
}

/// The broker pushes a `TopologyUpdate` when the coordination generation changes
/// and accepts the client's ack without disturbing the connection.
#[tokio::test]
async fn broker_pushes_topology_update_on_generation_change() {
    let generation = Arc::new(std::sync::atomic::AtomicU64::new(1));
    let source = Arc::new(BumpingTopology {
        generation: generation.clone(),
    });

    let (broker, dir) = open_test_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (server, peer) = listener.accept().await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            server,
            Some(peer),
            broker,
            tcp_stats,
            connection_stats,
            conn_id,
            None::<StaticAuthHandler>,
            None,
            ConnectionSettings::new(Some(60)),
            Some(source as Arc<dyn ClientTopologySource>),
            None,
            None,
        )
        .await
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let mut framed = plain_conn(client);
    handshake(&mut framed).await;

    // Bump the coordination generation; the broker pushes on its next tick.
    generation.store(2, std::sync::atomic::Ordering::SeqCst);
    let pushed = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let frame = recv_frame(&mut framed).await;
            if frame.opcode == Op::TopologyUpdate as u16 {
                return frame;
            }
        }
    })
    .await
    .expect("broker pushed a topology update");
    let topo: TopologyOk = try_decode(&pushed).unwrap();
    assert_eq!(topo.generation, 2);

    // The client acks; the connection stays healthy and still serves requests.
    framed
        .send(
            try_encode(
                Op::TopologyUpdateAck,
                7,
                &TopologyUpdateAck { generation: 2 },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    framed
        .send(try_encode(Op::Topology, 8, &TopologyRequest::default()).unwrap())
        .await
        .unwrap();
    let resp = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let frame = recv_frame(&mut framed).await;
            if frame.opcode == Op::TopologyOk as u16 {
                return frame;
            }
        }
    })
    .await
    .expect("server still answers after the ack");
    let ok: TopologyOk = try_decode(&resp).unwrap();
    assert_eq!(ok.generation, 2);

    drop(framed);
    let _ = server_task.await;
    drop(dir);
}

/// A topology source whose generation bumps every read but whose routing content
/// never changes - models the coordination generation churning on heartbeats.
struct QuietTopology {
    generation: Arc<std::sync::atomic::AtomicU64>,
}
impl ClientTopologySource for QuietTopology {
    fn topology(&self) -> TopologyOk {
        TopologyOk {
            generation: self
                .generation
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
            queues: vec![QueueTopologyEntry {
                topic: "jobs".into(),
                partition: Partition::new(0),
                group: None,
                owner_endpoints: vec![
                    AdvertisedAddress::parse("127.0.0.1:7000").expect("valid test owner endpoint"),
                ],
                partitioning_version: 1,
                partition_count: 1,
            }],
            streams: Vec::new(),
        }
    }
    fn owner_endpoint(
        &self,
        _topic: &str,
        _partition: Partition,
        _group: Option<&str>,
    ) -> Option<(String, u64)> {
        None
    }
}

/// The broker must NOT push a `TopologyUpdate` when the coordination generation
/// churns (e.g. heartbeat liveness timestamps) but the routing content is
/// unchanged. Otherwise every client gets an identical topology each heartbeat.
#[tokio::test]
async fn broker_does_not_push_topology_when_content_unchanged() {
    let source = Arc::new(QuietTopology {
        generation: Arc::new(std::sync::atomic::AtomicU64::new(1)),
    });

    let (broker, dir) = open_test_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (server, peer) = listener.accept().await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            server,
            Some(peer),
            broker,
            tcp_stats,
            connection_stats,
            conn_id,
            None::<StaticAuthHandler>,
            None,
            ConnectionSettings::new(Some(60)),
            Some(source as Arc<dyn ClientTopologySource>),
            None,
            None,
        )
        .await
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let mut framed = plain_conn(client);
    handshake(&mut framed).await;

    // Over several topology ticks (1s each) the generation churns but content does
    // not, so no TopologyUpdate should arrive.
    let pushed = tokio::time::timeout(std::time::Duration::from_millis(2500), async {
        loop {
            let frame = recv_frame(&mut framed).await;
            if frame.opcode == Op::TopologyUpdate as u16 {
                return true;
            }
        }
    })
    .await;
    assert!(
        pushed.is_err(),
        "no topology push expected on content-stable churn"
    );

    drop(framed);
    let _ = server_task.await;
    drop(dir);
}

/// The handler answers `Op::Topology` from its injected topology source,
/// honoring a topic filter.
#[tokio::test]
async fn handler_answers_topology_query_from_source() {
    let source = Arc::new(FixedTopology(TopologyOk {
        generation: 4,
        queues: vec![
            QueueTopologyEntry {
                topic: "orders".into(),
                partition: Partition::new(0),
                group: Some("workers".into()),
                owner_endpoints: vec![
                    AdvertisedAddress::parse("127.0.0.1:9000").expect("valid test owner endpoint"),
                ],
                partitioning_version: 0,
                partition_count: 1,
            },
            QueueTopologyEntry {
                topic: "emails".into(),
                partition: Partition::new(0),
                group: None,
                owner_endpoints: vec![
                    AdvertisedAddress::parse("127.0.0.1:9001").expect("valid test owner endpoint"),
                ],
                partitioning_version: 0,
                partition_count: 1,
            },
        ],
        streams: Vec::new(),
    }));

    let (broker, dir) = open_test_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (server, peer) = listener.accept().await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            server,
            Some(peer),
            broker,
            tcp_stats,
            connection_stats,
            conn_id,
            None::<StaticAuthHandler>,
            None,
            ConnectionSettings::new(Some(60)),
            Some(source as Arc<dyn ClientTopologySource>),
            None,
            None,
        )
        .await
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let mut framed = plain_conn(client);
    handshake(&mut framed).await;

    // Full topology.
    framed
        .send(try_encode(Op::Topology, 2, &TopologyRequest::default()).unwrap())
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::TopologyOk as u16);
    let all: TopologyOk = try_decode(&frame).unwrap();
    assert_eq!(all.generation, 4);
    assert_eq!(all.queues.len(), 2);

    // Filtered by topic.
    framed
        .send(
            try_encode(
                Op::Topology,
                3,
                &TopologyRequest {
                    topic: Some("orders".into()),
                    group: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    let filtered: TopologyOk = try_decode(&frame).unwrap();
    assert_eq!(filtered.queues.len(), 1);
    assert_eq!(filtered.queues[0].topic, "orders");
    assert_eq!(
        filtered.queues[0]
            .owner_endpoints
            .first()
            .map(|a| a.target()),
        Some("127.0.0.1:9000".to_string())
    );

    drop(framed);
    server_task.await.unwrap().unwrap();
    drop(dir);
}

/// A publish to a queue this broker does not own returns an `Op::Redirect` to
/// the current owner (resolved from the topology source), not a plain error.
#[tokio::test]
async fn unowned_publish_redirects_to_current_owner() {
    let (broker, dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::new()))).await;
    let source = Arc::new(FixedTopology(TopologyOk {
        generation: 2,
        queues: vec![QueueTopologyEntry {
            topic: "elsewhere".into(),
            partition: Partition::new(0),
            group: None,
            owner_endpoints: vec![
                AdvertisedAddress::parse("127.0.0.1:9999").expect("valid test owner endpoint"),
            ],
            partitioning_version: 0,
            partition_count: 1,
        }],
        streams: Vec::new(),
    }));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (server, peer) = listener.accept().await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            server,
            Some(peer),
            broker,
            tcp_stats,
            connection_stats,
            conn_id,
            None::<StaticAuthHandler>,
            None,
            ConnectionSettings::new(Some(60)),
            Some(source as Arc<dyn ClientTopologySource>),
            None,
            None,
        )
        .await
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let mut framed = plain_conn(client);
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "elsewhere".into(),
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Redirect as u16);
    let redirect: fibril_protocol::v1::Redirect = try_decode(&frame).unwrap();
    assert_eq!(redirect.topic, "elsewhere");
    assert_eq!(redirect.owner_endpoints[0].target(), "127.0.0.1:9999");

    drop(framed);
    server_task.await.unwrap().unwrap();
    drop(dir);
}

/// A publish stamped with a partitioning version older than the queue's
/// authoritative version is fenced: the owner redirects the client (with the
/// current version) so it re-fetches topology and re-routes, instead of writing
/// into a partition chosen under a stale view.
#[tokio::test]
async fn stale_partitioning_version_publish_is_fenced() {
    let (broker, dir) = open_test_broker().await;
    let source = Arc::new(FixedTopology(TopologyOk {
        generation: 7,
        queues: vec![QueueTopologyEntry {
            topic: "jobs".into(),
            partition: Partition::new(0),
            group: None,
            owner_endpoints: vec![
                AdvertisedAddress::parse("127.0.0.1:9100").expect("valid test owner endpoint"),
            ],
            partitioning_version: 5,
            partition_count: 4,
        }],
        streams: Vec::new(),
    }));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_task = tokio::spawn(async move {
        let (server, peer) = listener.accept().await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            server,
            Some(peer),
            broker,
            tcp_stats,
            connection_stats,
            conn_id,
            None::<StaticAuthHandler>,
            None,
            ConnectionSettings::new(Some(60)),
            Some(source as Arc<dyn ClientTopologySource>),
            None,
            None,
        )
        .await
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let mut framed = plain_conn(client);
    handshake(&mut framed).await;

    // Client routed under version 2; the queue is now at version 5.
    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "jobs".into(),
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 2,
                    ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Redirect as u16);
    let redirect: fibril_protocol::v1::Redirect = try_decode(&frame).unwrap();
    assert_eq!(redirect.topic, "jobs");
    assert_eq!(redirect.owner_endpoints[0].target(), "127.0.0.1:9100");
    // The redirect carries the current version so the client can re-route.
    assert_eq!(redirect.partitioning_version, 5);

    drop(framed);
    server_task.await.unwrap().unwrap();
    drop(dir);
}

/// End-to-end through a real broker and the real Stroma logs: publishing to two
/// partitions of one queue and subscribing to each delivers only that
/// partition's own message — multi-partition publish and subscribe are isolated
/// across the full server stack (handler -> broker -> per-partition log ->
/// delivery), not just in unit tests.
#[tokio::test]
async fn multi_partition_publish_subscribe_is_isolated_e2e() {
    let (broker, dir) = open_test_broker().await;
    let (addr, server_task, dir, _broker) =
        start_protocol_listener_for_broker(ConnectionSettings::new(Some(60)), broker, dir, None)
            .await;

    let client = TcpStream::connect(addr).await.unwrap();
    let mut framed = plain_conn(client);
    handshake(&mut framed).await;

    let topic = "jobs";

    // One message into each of partitions 0 and 1 (same logical queue).
    for (req, partition, payload) in [(2u64, 0u32, b"to-p0".to_vec()), (3, 1, b"to-p1".to_vec())] {
        framed
            .send(
                try_encode(
                    Op::Publish,
                    req,
                    &Publish {
                        topic: topic.into(),
                        partition: Partition::new(partition),
                        group: None,
                        require_confirm: true,
                        content_type: None,
                        headers: HashMap::new(),
                        payload,
                        published: unix_millis(),
                        partition_key: None,
                        partitioning_version: 0,
                        ttl_ms: None,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let resp = recv_frame(&mut framed).await;
        assert_eq!(resp.opcode, Op::PublishOk as u16);
    }

    // Subscribe to each partition; each must receive only its own message.
    for (req, partition, expected) in [(4u64, 0u32, b"to-p0".to_vec()), (5, 1, b"to-p1".to_vec())] {
        framed
            .send(
                try_encode(
                    Op::Subscribe,
                    req,
                    &Subscribe {
                        topic: topic.into(),
                        partition: Partition::new(partition),
                        group: None,
                        prefetch: 8,
                        auto_ack: true,
                        consumer_group: None,
                        consumer_target: None,
                        member_id: None,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let resp = recv_frame(&mut framed).await;
        assert_eq!(resp.opcode, Op::SubscribeOk as u16);

        let delivered = recv_delivery_for_topic(&mut framed, topic).await;
        assert_eq!(delivered.partition, Partition::new(partition));
        assert_eq!(delivered.payload, expected);
    }

    drop(framed);
    server_task.await.unwrap().unwrap();
    drop(dir);
}

/// Accept many client connections against one broker, each on its own handler
/// task, so two connections become two distinct logical members of a cohort.
async fn start_multi_connection_listener(
    settings: ConnectionSettings,
    broker: Arc<Broker<StromaEngine>>,
) -> (SocketAddr, CancellationToken) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let accept_shutdown = shutdown.clone();
    tokio::spawn(async move {
        loop {
            let (server, peer) = tokio::select! {
                _ = accept_shutdown.cancelled() => break,
                accepted = listener.accept() => match accepted {
                    Ok(value) => value,
                    Err(_) => break,
                },
            };
            let broker = broker.clone();
            let settings = settings.clone();
            tokio::spawn(async move {
                let tcp_stats = TcpStats::new(10);
                let connection_stats = ConnectionStats::new();
                let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
                let _ = handle_connection(
                    server,
                    Some(peer),
                    broker,
                    tcp_stats,
                    connection_stats,
                    conn_id,
                    None::<StaticAuthHandler>,
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
    (addr, shutdown)
}

async fn subscribe_exclusive(
    framed: &mut Conn,
    request_id: u64,
    topic: &str,
    partition: u32,
    consumer_group: &str,
    consumer_target: Option<u32>,
    member_id: uuid::Uuid,
) {
    framed
        .send(
            try_encode(
                Op::Subscribe,
                request_id,
                &Subscribe {
                    topic: topic.into(),
                    partition: Partition::new(partition),
                    group: None,
                    prefetch: 8,
                    auto_ack: true,
                    consumer_group: Some(consumer_group.into()),
                    consumer_target,
                    member_id: Some(member_id),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    // AssignmentChanged pushes may interleave with the SubscribeOk; skip them.
    loop {
        let frame = recv_frame(framed).await;
        if frame.opcode == Op::AssignmentChanged as u16 {
            continue;
        }
        assert_eq!(frame.opcode, Op::SubscribeOk as u16);
        break;
    }
}

/// Read frames until an `AssignmentChanged` satisfying `pred`, returning it
/// (skipping any other frames, e.g. SubscribeOk / Deliver / earlier assignments).
async fn recv_assignment_until(
    framed: &mut Conn,
    pred: impl Fn(&fibril_protocol::v1::AssignmentChanged) -> bool,
) -> fibril_protocol::v1::AssignmentChanged {
    // Generous failure-detection bound, see recv_frame.
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let frame = recv_frame(framed).await;
            if frame.opcode == Op::AssignmentChanged as u16 {
                let assignment: fibril_protocol::v1::AssignmentChanged =
                    try_decode(&frame).unwrap();
                if pred(&assignment) {
                    break assignment;
                }
            }
        }
    })
    .await
    .expect("assignment did not arrive within the receive timeout")
}

async fn publish_to_partition(
    framed: &mut Conn,
    request_id: u64,
    topic: &str,
    partition: u32,
    payload: Vec<u8>,
) {
    framed
        .send(
            try_encode(
                Op::Publish,
                request_id,
                &Publish {
                    topic: topic.into(),
                    partition: Partition::new(partition),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload,
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let resp = recv_frame(framed).await;
    assert_eq!(resp.opcode, Op::PublishOk as u16);
}

/// Read `count` deliveries for `topic` and return the distinct partitions seen.
async fn collected_partitions(framed: &mut Conn, topic: &str, count: usize) -> HashSet<u32> {
    let mut partitions = HashSet::new();
    for _ in 0..count {
        partitions.insert(recv_delivery_for_topic(framed, topic).await.partition.id());
    }
    partitions
}

/// An opt-in exclusive consumer group exclusively divides a 2-partition queue
/// between two members (each fanned in to both partitions, Model A), and on a
/// member's disconnect the survivor takes over the revoked partition.
#[tokio::test]
async fn exclusive_consumer_group_splits_partitions_and_fails_over_e2e() {
    let (broker, dir) = open_test_broker().await;
    let (addr, shutdown) =
        start_multi_connection_listener(ConnectionSettings::new(None), broker.clone()).await;

    let topic = "exgroup";
    let cohort = "g";

    let mut a = plain_conn(TcpStream::connect(addr).await.unwrap());
    let mut b = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut a).await;
    handshake(&mut b).await;

    // Both members fan in to BOTH partitions under the same cohort id. After all
    // four SubscribeOks the per-partition gates have settled to the split.
    // Each connection is one cohort member, carrying a stable member id across
    // its per-partition subscribes (as the real client does).
    let member_a = uuid::Uuid::new_v4();
    let member_b = uuid::Uuid::new_v4();
    subscribe_exclusive(&mut a, 10, topic, 0, cohort, None, member_a).await;
    subscribe_exclusive(&mut a, 11, topic, 1, cohort, None, member_a).await;
    subscribe_exclusive(&mut b, 20, topic, 0, cohort, None, member_b).await;
    subscribe_exclusive(&mut b, 21, topic, 1, cohort, None, member_b).await;

    // Publisher connection: two messages into each partition.
    let mut publisher = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut publisher).await;
    for round in 0..2u64 {
        for partition in 0..2u32 {
            publish_to_partition(
                &mut publisher,
                100 + round * 10 + partition as u64,
                topic,
                partition,
                format!("r{round}-p{partition}").into_bytes(),
            )
            .await;
        }
    }

    // Each member receives ONLY its assigned partition's messages, and together
    // they cover both partitions (exclusive split).
    let a_parts = collected_partitions(&mut a, topic, 2).await;
    let b_parts = collected_partitions(&mut b, topic, 2).await;
    assert_eq!(a_parts.len(), 1, "member A is exclusive to one partition");
    assert_eq!(b_parts.len(), 1, "member B is exclusive to one partition");
    assert_ne!(a_parts, b_parts, "the cohort splits the partitions");
    let mut covered: Vec<u32> = a_parts.union(&b_parts).copied().collect();
    covered.sort();
    assert_eq!(covered, vec![0, 1], "both partitions are covered");

    // Failover: drop member A; the survivor must take over A's partition too.
    drop(a);
    for partition in 0..2u32 {
        publish_to_partition(
            &mut publisher,
            200 + partition as u64,
            topic,
            partition,
            format!("after-p{partition}").into_bytes(),
        )
        .await;
    }
    let mut after: Vec<u32> = collected_partitions(&mut b, topic, 2)
        .await
        .into_iter()
        .collect();
    after.sort();
    assert_eq!(
        after,
        vec![0, 1],
        "survivor takes over the revoked partition"
    );

    shutdown.cancel();
    drop(b);
    drop(publisher);
    drop(dir);
}

/// A member's soft target shapes the split: a member that caps itself at one
/// partition gets exactly one of three, and the uncapped peer absorbs the other
/// two — deterministic regardless of which member sorts first.
#[tokio::test]
async fn exclusive_consumer_group_member_target_shapes_split_e2e() {
    let (broker, dir) = open_test_broker().await;
    let (addr, shutdown) =
        start_multi_connection_listener(ConnectionSettings::new(None), broker.clone()).await;

    let topic = "exgroup-target";
    let cohort = "g";

    // `capped` self-limits to 1 partition; `flex` is uncapped. Both fan in to all
    // three partitions of the queue.
    let mut capped = plain_conn(TcpStream::connect(addr).await.unwrap());
    let mut flex = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut capped).await;
    handshake(&mut flex).await;

    let member_capped = uuid::Uuid::new_v4();
    let member_flex = uuid::Uuid::new_v4();
    for partition in 0..3u32 {
        subscribe_exclusive(
            &mut capped,
            10 + partition as u64,
            topic,
            partition,
            cohort,
            Some(1),
            member_capped,
        )
        .await;
    }
    for partition in 0..3u32 {
        subscribe_exclusive(
            &mut flex,
            20 + partition as u64,
            topic,
            partition,
            cohort,
            None,
            member_flex,
        )
        .await;
    }

    let mut publisher = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut publisher).await;
    for partition in 0..3u32 {
        publish_to_partition(
            &mut publisher,
            100 + partition as u64,
            topic,
            partition,
            format!("p{partition}").into_bytes(),
        )
        .await;
    }

    let capped_parts = collected_partitions(&mut capped, topic, 1).await;
    let flex_parts = collected_partitions(&mut flex, topic, 2).await;
    assert_eq!(
        capped_parts.len(),
        1,
        "the capped member honors its target of 1"
    );
    assert_eq!(flex_parts.len(), 2, "the uncapped member absorbs the rest");
    assert!(
        capped_parts.is_disjoint(&flex_parts),
        "no partition is shared across the cohort"
    );
    let mut covered: Vec<u32> = capped_parts.union(&flex_parts).copied().collect();
    covered.sort();
    assert_eq!(covered, vec![0, 1, 2], "all partitions covered");

    shutdown.cancel();
    drop(capped);
    drop(flex);
    drop(publisher);
    drop(dir);
}

/// The server pushes AssignmentChanged to a cohort member when its partition set
/// changes: the sole member is assigned both partitions, then a second member
/// joining revokes one from the first and assigns it to the newcomer.
#[tokio::test]
async fn exclusive_consumer_group_pushes_assignment_changes_e2e() {
    let (broker, dir) = open_test_broker().await;
    let (addr, shutdown) =
        start_multi_connection_listener(ConnectionSettings::new(None), broker.clone()).await;

    let topic = "exgroup-push";
    let cohort = "g";

    // Raw subscribe sends (no eager read) so the AssignmentChanged pushes are not
    // consumed while waiting for SubscribeOk.
    async fn send_sub(
        framed: &mut Conn,
        req: u64,
        topic: &str,
        partition: u32,
        cohort: &str,
        member_id: uuid::Uuid,
    ) {
        framed
            .send(
                try_encode(
                    Op::Subscribe,
                    req,
                    &Subscribe {
                        topic: topic.into(),
                        partition: Partition::new(partition),
                        group: None,
                        prefetch: 8,
                        auto_ack: true,
                        consumer_group: Some(cohort.into()),
                        consumer_target: None,
                        member_id: Some(member_id),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }

    let mut a = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut a).await;
    let member_a = uuid::Uuid::new_v4();
    send_sub(&mut a, 10, topic, 0, cohort, member_a).await;
    send_sub(&mut a, 11, topic, 1, cohort, member_a).await;

    // Sole member: eventually assigned both partitions.
    let sole = recv_assignment_until(&mut a, |asg| asg.assigned.len() == 2).await;
    assert_eq!(sole.topic, topic);
    assert_eq!(sole.consumer_group, cohort);
    let mut both: Vec<u32> = sole.assigned.iter().map(|p| p.id()).collect();
    both.sort();
    assert_eq!(both, vec![0, 1]);

    // Second member joins -> the cohort rebalances to one partition each.
    let mut b = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut b).await;
    let member_b = uuid::Uuid::new_v4();
    send_sub(&mut b, 20, topic, 0, cohort, member_b).await;
    send_sub(&mut b, 21, topic, 1, cohort, member_b).await;

    // b is told it owns exactly one partition; a is told one was revoked.
    let b_asg = recv_assignment_until(&mut b, |asg| asg.assigned.len() == 1).await;
    let a_asg = recv_assignment_until(&mut a, |asg| {
        asg.assigned.len() == 1 && !asg.revoked.is_empty()
    })
    .await;
    let a_part = a_asg.assigned[0].id();
    let b_part = b_asg.assigned[0].id();
    assert_ne!(a_part, b_part, "the two members own different partitions");
    let mut covered = vec![a_part, b_part];
    covered.sort();
    assert_eq!(covered, vec![0, 1], "between them they cover the queue");

    shutdown.cancel();
    drop(a);
    drop(b);
    drop(dir);
}

/// An exclusive cohort still gates and delivers correctly when the queue's
/// partitions are owned by DIFFERENT brokers. Each owner runs its own router over
/// the partitions it owns and gates them independently; together they serve the
/// cohort. Per-partition correctness holds across owners; cluster-wide balance
/// across owners is handled by the cross-broker coordinator.
#[tokio::test]
async fn exclusive_cohort_works_across_partition_owners_e2e() {
    let topic = "exgroup-cluster";

    // Broker A owns partition 0; broker B owns partition 1.
    let (broker_a, dir_a) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::from([
            OwnedQueue::new(topic, Partition::new(0), None),
        ]))))
        .await;
    let (broker_b, dir_b) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::from([
            OwnedQueue::new(topic, Partition::new(1), None),
        ]))))
        .await;
    let (addr_a, shutdown_a) =
        start_multi_connection_listener(ConnectionSettings::new(None), broker_a.clone()).await;
    let (addr_b, shutdown_b) =
        start_multi_connection_listener(ConnectionSettings::new(None), broker_b.clone()).await;

    // The cohort member subscribes to each partition on its owner (the fan-in the
    // client performs after topology routing).
    let mut a = plain_conn(TcpStream::connect(addr_a).await.unwrap());
    let mut b = plain_conn(TcpStream::connect(addr_b).await.unwrap());
    handshake(&mut a).await;
    handshake(&mut b).await;
    // One logical consumer spanning both owners carries the SAME member id, so
    // each broker recognizes it as the same cohort member.
    let member = uuid::Uuid::new_v4();
    subscribe_exclusive(&mut a, 10, topic, 0, "default", None, member).await;
    subscribe_exclusive(&mut b, 20, topic, 1, "default", None, member).await;

    // Publish to each partition on its owner.
    let mut pa = plain_conn(TcpStream::connect(addr_a).await.unwrap());
    let mut pb = plain_conn(TcpStream::connect(addr_b).await.unwrap());
    handshake(&mut pa).await;
    handshake(&mut pb).await;
    publish_to_partition(&mut pa, 100, topic, 0, b"p0".to_vec()).await;
    publish_to_partition(&mut pb, 101, topic, 1, b"p1".to_vec()).await;

    // Each owner gated its partition to the cohort member and delivered it.
    let da = recv_delivery_for_topic(&mut a, topic).await;
    assert_eq!(da.partition, Partition::new(0));
    assert_eq!(da.payload, b"p0");
    let db = recv_delivery_for_topic(&mut b, topic).await;
    assert_eq!(db.partition, Partition::new(1));
    assert_eq!(db.payload, b"p1");

    shutdown_a.cancel();
    shutdown_b.cancel();
    drop(a);
    drop(b);
    drop(pa);
    drop(pb);
    drop(dir_a);
    drop(dir_b);
}

/// A queue has a single exclusive cohort: subscribing with a second, different
/// cohort id on the same queue is rejected (one cohort per queue).
#[tokio::test]
async fn exclusive_consumer_group_rejects_second_cohort_e2e() {
    let (broker, dir) = open_test_broker().await;
    let (addr, shutdown) =
        start_multi_connection_listener(ConnectionSettings::new(None), broker.clone()).await;

    let topic = "exgroup-conflict";

    // First cohort claims the queue.
    let mut a = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut a).await;
    subscribe_exclusive(
        &mut a,
        10,
        topic,
        0,
        "cohort-one",
        None,
        uuid::Uuid::new_v4(),
    )
    .await;

    // A different cohort id on the same queue is refused.
    let mut b = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut b).await;
    b.send(
        try_encode(
            Op::Subscribe,
            20,
            &Subscribe {
                topic: topic.into(),
                partition: Partition::new(0),
                group: None,
                prefetch: 8,
                auto_ack: true,
                consumer_group: Some("cohort-two".into()),
                consumer_target: None,
                member_id: None,
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let frame = recv_frame(&mut b).await;
    assert_eq!(
        frame.opcode,
        Op::SubscribeErr as u16,
        "second cohort rejected"
    );

    shutdown.cancel();
    drop(a);
    drop(b);
    drop(dir);
}

/// A nil cohort member id is malformed and rejected (ERR_INVALID), not silently
/// accepted or remapped.
#[tokio::test]
async fn exclusive_subscribe_rejects_nil_member_id() {
    let (broker, dir) = open_test_broker().await;
    let (addr, shutdown) =
        start_multi_connection_listener(ConnectionSettings::new(None), broker.clone()).await;

    let mut a = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut a).await;
    a.send(
        try_encode(
            Op::Subscribe,
            10,
            &Subscribe {
                topic: "exgroup-nil".into(),
                partition: Partition::new(0),
                group: None,
                prefetch: 8,
                auto_ack: true,
                consumer_group: Some("default".into()),
                consumer_target: None,
                member_id: Some(uuid::Uuid::nil()),
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let frame = recv_frame(&mut a).await;
    assert_eq!(frame.opcode, Op::SubscribeErr as u16);
    let err: ErrorMsg = try_decode(&frame).unwrap();
    assert_eq!(err.code, ERR_INVALID, "nil member id is a bad request");

    shutdown.cancel();
    drop(a);
    drop(dir);
}

/// One connection cannot present two different cohort member ids: the first
/// exclusive subscribe establishes the connection's identity, a later subscribe
/// with a different id is a conflict.
#[tokio::test]
async fn exclusive_subscribe_rejects_mismatched_member_id_on_same_connection() {
    let (broker, dir) = open_test_broker().await;
    let (addr, shutdown) =
        start_multi_connection_listener(ConnectionSettings::new(None), broker.clone()).await;
    let topic = "exgroup-identity";

    let mut a = plain_conn(TcpStream::connect(addr).await.unwrap());
    handshake(&mut a).await;
    // First exclusive subscribe establishes member id `first` on this connection.
    let first = uuid::Uuid::new_v4();
    subscribe_exclusive(&mut a, 10, topic, 0, "default", None, first).await;

    // A second exclusive subscribe on the same connection with a DIFFERENT id is
    // rejected as a conflict.
    a.send(
        try_encode(
            Op::Subscribe,
            20,
            &Subscribe {
                topic: topic.into(),
                partition: Partition::new(1),
                group: None,
                prefetch: 8,
                auto_ack: true,
                consumer_group: Some("default".into()),
                consumer_target: None,
                member_id: Some(uuid::Uuid::new_v4()),
            },
        )
        .unwrap(),
    )
    .await
    .unwrap();
    // An assignment push for the first subscribe may interleave; skip to the err.
    let frame = loop {
        let frame = recv_frame(&mut a).await;
        if frame.opcode != Op::AssignmentChanged as u16 {
            break frame;
        }
    };
    assert_eq!(frame.opcode, Op::SubscribeErr as u16);
    let err: ErrorMsg = try_decode(&frame).unwrap();
    assert_eq!(err.code, ERR_CONFLICT, "mismatched member id is a conflict");

    // Re-using the established id (or sending none) on another partition is fine.
    subscribe_exclusive(&mut a, 30, topic, 1, "default", None, first).await;

    shutdown.cancel();
    drop(a);
    drop(dir);
}

/// Declare resolves the partition count (explicit, else the cluster default)
/// and reports it. Standalone (no coordinator) materializes locally.
#[tokio::test]
async fn declare_fans_out_partitions_standalone() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    // Explicit partition count is honored.
    framed
        .send(
            try_encode(
                Op::DeclareQueue,
                2,
                &DeclareQueue {
                    topic: "orders".into(),
                    group: None,
                    dlq_policy: None,
                    dlq_max_retries: None,
                    partition_count: Some(3),
                    default_message_ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::DeclareQueueOk as u16);
    let ok: DeclareQueueOk = try_decode(&frame).unwrap();
    assert_eq!(ok.partition_count, 3);

    // Omitted count falls back to the cluster default (1 in the test broker).
    framed
        .send(
            try_encode(
                Op::DeclareQueue,
                3,
                &DeclareQueue {
                    topic: "emails".into(),
                    group: None,
                    dlq_policy: None,
                    dlq_max_retries: None,
                    partition_count: None,
                    default_message_ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    let ok: DeclareQueueOk = try_decode(&frame).unwrap();
    assert_eq!(ok.partition_count, 1);

    drop(framed);
    server_task.await.unwrap().unwrap();
}

/// In cluster mode the coordinator's effective count is authoritative — it
/// overrides the requested count (e.g. the queue was already declared).
#[tokio::test]
async fn declare_uses_coordinator_effective_count() {
    struct FixedCoordinator(u32);
    impl DeclareCoordinator for FixedCoordinator {
        fn declare_partitioning<'a>(
            &'a self,
            _topic: &'a str,
            _group: Option<&'a str>,
            _partition_count: u32,
            _meta: fibril_broker::queue_engine::DeclareMeta,
        ) -> futures::future::BoxFuture<'a, Result<u32, String>> {
            let effective = self.0;
            Box::pin(async move { Ok(effective) })
        }

        fn declare_stream<'a>(
            &'a self,
            _topic: &'a str,
            _partition_count: u32,
            _durability: u8,
            _retention: fibril_protocol::v1::StreamRetention,
            _replication_factor: Option<u32>,
        ) -> futures::future::BoxFuture<'a, Result<u32, String>> {
            let effective = self.0;
            Box::pin(async move { Ok(effective) })
        }
    }

    let (broker, dir) = open_test_broker().await;
    let observer = broker.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let coordinator = Arc::new(FixedCoordinator(5));
    let server_task = tokio::spawn(async move {
        let (server, peer) = listener.accept().await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            server,
            Some(peer),
            broker,
            tcp_stats,
            connection_stats,
            conn_id,
            None::<StaticAuthHandler>,
            None,
            ConnectionSettings::new(Some(60)),
            None,
            Some(coordinator as Arc<dyn DeclareCoordinator>),
            None,
        )
        .await
    });

    let client = TcpStream::connect(addr).await.unwrap();
    let mut framed = plain_conn(client);
    handshake(&mut framed).await;
    framed
        .send(
            try_encode(
                Op::DeclareQueue,
                2,
                &DeclareQueue {
                    topic: "orders".into(),
                    group: None,
                    dlq_policy: None,
                    dlq_max_retries: None,
                    partition_count: Some(3),
                    default_message_ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    let ok: DeclareQueueOk = try_decode(&frame).unwrap();
    assert_eq!(
        ok.partition_count, 5,
        "coordinator count overrides the request"
    );

    assert!(
        !observer.engine().is_materialized("orders", 0, None),
        "a coordinated declaration must not create an owner log on the receiving broker"
    );
    drop(framed);
    server_task.await.unwrap().unwrap();
    drop(dir);
}

#[tokio::test]
async fn replication_checkpoint_export_install_composes_with_catch_up() {
    let topic = "replication.checkpoint.tcp";
    let group = Some("workers".to_string());
    let (mut owner_framed, owner_task, _owner_dir) = open_node_connection().await;
    node_handshake(&mut owner_framed).await;
    framed_publish(
        &mut owner_framed,
        2,
        topic,
        group.as_deref(),
        b"checkpoint-first",
    )
    .await;
    framed_publish(
        &mut owner_framed,
        3,
        topic,
        group.as_deref(),
        b"checkpoint-second",
    )
    .await;

    owner_framed
        .send(
            try_encode(
                Op::ReplicationCheckpointExport,
                4,
                &ReplicationCheckpointExport {
                    topic: topic.into(),
                    group: group.clone(),
                    partition: Partition::new(0),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut owner_framed).await;
    assert_eq!(frame.opcode, Op::ReplicationCheckpointExportOk as u16);
    let export: ReplicationCheckpointExportOk = try_decode(&frame).unwrap();

    let (follower_broker, follower_dir) = open_test_broker().await;
    follower_broker
        .become_replication_follower(topic, Partition::new(0), group.as_deref())
        .await
        .unwrap();
    let (mut follower_framed, follower_task, _follower_dir, follower_broker) =
        open_node_connection_for_broker(
            ConnectionSettings::new(Some(60)),
            follower_broker,
            follower_dir,
        )
        .await;
    node_handshake(&mut follower_framed).await;

    follower_framed
        .send(
            try_encode(
                Op::ReplicationCheckpointInstall,
                5,
                &ReplicationCheckpointInstall {
                    topic: topic.into(),
                    group: group.clone(),
                    partition: Partition::new(0),
                    checkpoint: export.checkpoint.clone(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut follower_framed).await;
    assert_eq!(frame.opcode, Op::ReplicationCheckpointInstallOk as u16);
    let install: ReplicationCheckpointInstallOk = try_decode(&frame).unwrap();
    assert_eq!(
        install.event_next_offset,
        export.checkpoint.event_next_offset
    );
    assert_eq!(
        install.applied_event_offset,
        export.checkpoint.applied_event_offset
    );

    let outcome = catch_up_replication_over_protocol(
        &mut owner_framed,
        &mut follower_framed,
        topic,
        Partition::new(0),
        group.as_deref(),
        ProtocolReplicationCatchUpOptions {
            message_from: export.checkpoint.message_checkpoint_offset,
            event_from: export.checkpoint.event_next_offset,
            max_messages_per_read: 10,
            max_events_per_read: 10,
            max_iterations: 2,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert!(matches!(outcome, ProtocolReplicationCatchUp::CaughtUp(_)));

    let promoted = follower_broker
        .promote_replication_follower_if_caught_up(
            topic,
            Partition::new(0),
            group.as_deref(),
            export.checkpoint.message_next_offset,
            export.checkpoint.event_next_offset,
        )
        .await
        .unwrap();
    assert_eq!(
        promoted,
        QueuePromotionOutcome::Promoted {
            message_next_offset: export.checkpoint.message_next_offset,
            event_next_offset: export.checkpoint.event_next_offset,
            applied_event_offset: Some(export.checkpoint.applied_event_offset),
        }
    );

    drop(owner_framed);
    drop(follower_framed);
    owner_task.await.unwrap().unwrap();
    follower_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn replication_checkpoint_install_rejects_delayed_old_epoch_without_erasing_data() {
    let topic = "checkpoint.delayed.epoch";
    let (broker, dir) = open_test_broker().await;
    let (mut framed, task, _dir, broker) =
        open_node_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    node_handshake(&mut framed).await;
    framed_publish(&mut framed, 2, topic, None, b"first").await;
    framed
        .send(
            try_encode(
                Op::ReplicationCheckpointExport,
                3,
                &ReplicationCheckpointExport {
                    topic: topic.into(),
                    group: None,
                    partition: Partition::new(0),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    let old: ReplicationCheckpointExportOk = try_decode(&frame).unwrap();
    assert_eq!(old.checkpoint.message_epoch, 0);

    // A checkpoint response from epoch 0 arrives after new data and the epoch-1
    // assignment fence. It must not erase either log or replace queue state.
    framed_publish(&mut framed, 4, topic, None, b"newer").await;
    broker
        .become_replication_follower_with_epoch(topic, Partition::new(0), None, 1)
        .await
        .unwrap();
    framed
        .send(
            try_encode(
                Op::ReplicationCheckpointInstall,
                5,
                &ReplicationCheckpointInstall {
                    topic: topic.into(),
                    group: None,
                    partition: Partition::new(0),
                    checkpoint: old.checkpoint,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(
        frame.opcode,
        Op::Error as u16,
        "stale checkpoint must be rejected"
    );
    let error: ErrorMsg = try_decode(&frame).unwrap();
    assert!(error.message.contains("epoch"), "{error:?}");

    let promoted = broker
        .promote_replication_follower_if_caught_up(topic, Partition::new(0), None, 2, 2)
        .await
        .unwrap();
    assert!(
        matches!(promoted, QueuePromotionOutcome::Promoted { .. }),
        "{promoted:?}"
    );
    let records = broker
        .read_owner_replication_records(topic, Partition::new(0), None, 0, 0, 10, 10, usize::MAX, 0)
        .await
        .unwrap();
    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("messages lost")
    };
    assert_eq!(messages.records.len(), 2);
    assert_eq!(messages.records[0].1.payload, b"first");
    assert_eq!(messages.records[1].1.payload, b"newer");
    drop(framed);
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unowned_publish_returns_not_owner_error_and_keeps_connection_open() {
    let (broker, dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::new()))).await;
    let (mut framed, server_task, _dir, _broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Publish,
                2,
                &Publish {
                    topic: "unowned".into(),
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_error_frame(&mut framed, 2, 409).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn unowned_subscribe_returns_not_owner_error_and_keeps_connection_open() {
    let (broker, dir) =
        open_test_broker_with_ownership(Arc::new(StaticQueueOwnership::new(HashSet::new()))).await;
    let (mut framed, server_task, _dir, _broker) =
        open_protocol_connection_for_broker(ConnectionSettings::new(Some(60)), broker, dir).await;
    handshake(&mut framed).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                2,
                &Subscribe {
                    topic: "unowned".into(),
                    partition: Partition::new(0),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_subscribe_error_frame(&mut framed, 2, 409).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn duplicate_subscribe_returns_conflict_and_keeps_connection_open() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;
    framed_subscribe(&mut framed, 2, "duplicate.subscribe", None, false).await;

    framed
        .send(
            try_encode(
                Op::Subscribe,
                3,
                &Subscribe {
                    topic: "duplicate.subscribe".into(),
                    partition: Partition::new(0),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    assert_subscribe_error_frame(&mut framed, 3, 409).await;
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
                    partition: Partition::new(0),
                    group: None,
                    prefetch: 1,
                    auto_ack: true,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
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
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::from([("Content-Type".into(), "application/json".into())]),
                    payload: br#"{"ok":true}"#.to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
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
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::from([("fibril.retries".into(), "1".into())]),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
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
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    not_before: unix_millis() + 150,
                    headers: HashMap::from([("stroma.source_offset".into(), "1".into())]),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
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
                    partition: Partition::new(0),
                    group: None,
                    prefetch: 1,
                    auto_ack: true,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
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
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    not_before,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"delayed".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
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
                    partition: Partition::new(0),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
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
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"retry-later".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
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
                    partition: Partition::new(0),
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
                    partition_count: None,
                    default_message_ttl_ms: None,
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
                    partition: Partition::new(0),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
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
                    partition: Partition::new(0),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
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
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::from([("x-trace-id".into(), "dlq-flow".into())]),
                    payload: b"poison".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
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
                    partition: Partition::new(0),
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
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    // Under load the 1s heartbeat can beat the PublishOk to the socket, so skip
    // an interleaved Ping rather than asserting the wrong frame.
    let frame = recv_frame_expect(&mut framed, Op::PublishOk).await;
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
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
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
            ..Default::default()
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
                    partition: Partition::new(0),
                    group: Some("workers".into()),
                    prefetch: 20,
                    auto_ack: true,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
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
                        partition: Partition::new(0),
                        group: Some("workers".into()),
                        require_confirm: true,
                        content_type: None,
                        headers: HashMap::new(),
                        payload: payload.clone(),
                        published: unix_millis(),
                        partition_key: None,
                        partitioning_version: 0,
                        ttl_ms: None,
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
                    partition: Partition::new(0),
                    group: None,
                    require_confirm: true,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: b"payload".to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    // Under load the 1s heartbeat can beat the PublishOk to the socket, so skip
    // an interleaved Ping rather than asserting the wrong frame.
    let frame = recv_frame_expect(&mut framed, Op::PublishOk).await;
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
        ..Default::default()
    });

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Ping as u16);

    wait_for_queue_idle(&broker, "publisher.cache.live", None).await;
    assert_connection_still_responds(&mut framed).await;

    drop(framed);
    server_task.await.unwrap().unwrap();
}

// ---------------------------------------------------------------------------
// Plexus stream ops (declare / subscribe / publish / ack-cursor)
// ---------------------------------------------------------------------------

async fn framed_declare_plexus(
    framed: &mut Conn,
    request_id: u64,
    topic: &str,
    partition_count: Option<u32>,
) -> DeclarePlexusOk {
    framed
        .send(
            try_encode(
                Op::DeclarePlexus,
                request_id,
                &DeclarePlexus {
                    topic: topic.into(),
                    partition_count,
                    durability: Default::default(),
                    retention: StreamRetention::default(),
                    replication_factor: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::DeclarePlexusOk as u16);
    try_decode(&frame).unwrap()
}

async fn framed_subscribe_stream(
    framed: &mut Conn,
    request_id: u64,
    topic: &str,
    partition: u32,
    durable_name: Option<&str>,
    start: StreamStart,
    auto_ack: bool,
) -> SubscribeOk {
    framed
        .send(
            try_encode(
                Op::SubscribeStream,
                request_id,
                &SubscribeStream {
                    topic: topic.into(),
                    partition: Partition::new(partition),
                    durable_name: durable_name.map(str::to_string),
                    start,
                    filter: Vec::new(),
                    prefetch: 16,
                    auto_ack,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(framed).await;
    assert_eq!(frame.opcode, Op::SubscribeOk as u16);
    try_decode(&frame).unwrap()
}

async fn send_stream_publish(
    framed: &mut Conn,
    request_id: u64,
    topic: &str,
    partition: u32,
    payload: &[u8],
    require_confirm: bool,
) {
    framed
        .send(
            try_encode(
                Op::Publish,
                request_id,
                &Publish {
                    topic: topic.into(),
                    partition: Partition::new(partition),
                    group: None,
                    require_confirm,
                    content_type: None,
                    headers: HashMap::new(),
                    payload: payload.to_vec(),
                    published: unix_millis(),
                    partition_key: None,
                    partitioning_version: 0,
                    ttl_ms: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
}

/// Read frames until the next `Deliver`, skipping confirms (`PublishOk`).
async fn recv_stream_deliver(framed: &mut Conn) -> Deliver {
    loop {
        let frame = recv_frame(framed).await;
        if frame.opcode == Op::Deliver as u16 {
            return try_decode(&frame).unwrap();
        }
        assert_eq!(frame.opcode, Op::PublishOk as u16);
    }
}

#[tokio::test]
async fn plexus_declare_subscribe_publish_fans_out() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    let ok = framed_declare_plexus(&mut framed, 2, "plexus.fanout", Some(1)).await;
    assert_eq!(ok.partition_count, 1);

    // Live subscriber sees records published from now on.
    framed_subscribe_stream(
        &mut framed,
        3,
        "plexus.fanout",
        0,
        Some("consumer-1"),
        StreamStart::Latest,
        false,
    )
    .await;

    send_stream_publish(&mut framed, 4, "plexus.fanout", 0, b"a", true).await;
    let first = recv_stream_deliver(&mut framed).await;
    assert_eq!(first.payload, b"a".to_vec());
    assert_eq!(first.offset, 0);
    assert_eq!(first.delivery_tag.epoch, 0);

    // Ack settles the durable cursor (delivery tag == offset).
    framed
        .send(
            try_encode(
                Op::Ack,
                5,
                &Ack {
                    topic: "plexus.fanout".into(),
                    group: None,
                    partition: Partition::new(0),
                    tags: vec![first.delivery_tag],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();

    send_stream_publish(&mut framed, 6, "plexus.fanout", 0, b"b", true).await;
    let second = recv_stream_deliver(&mut framed).await;
    assert_eq!(second.payload, b"b".to_vec());
    assert_eq!(second.offset, 1);

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn plexus_durable_cursor_resumes_after_ack() {
    let settings = ConnectionSettings::new(Some(60));
    let (mut first, first_task, dir, broker) =
        open_protocol_connection_with_settings(settings.clone()).await;
    handshake(&mut first).await;

    framed_declare_plexus(&mut first, 2, "plexus.resume", Some(1)).await;

    // Durable consumer reads from earliest, manual ack.
    framed_subscribe_stream(
        &mut first,
        3,
        "plexus.resume",
        0,
        Some("c1"),
        StreamStart::Earliest,
        false,
    )
    .await;

    send_stream_publish(&mut first, 4, "plexus.resume", 0, b"a", false).await;
    send_stream_publish(&mut first, 5, "plexus.resume", 0, b"b", false).await;

    let d0 = recv_stream_deliver(&mut first).await;
    assert_eq!(d0.payload, b"a".to_vec());
    let d1 = recv_stream_deliver(&mut first).await;
    assert_eq!(d1.payload, b"b".to_vec());

    // Settle the cursor through both records.
    first
        .send(
            try_encode(
                Op::Ack,
                6,
                &Ack {
                    topic: "plexus.resume".into(),
                    group: None,
                    partition: Partition::new(0),
                    tags: vec![d0.delivery_tag, d1.delivery_tag],
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    // Let the durable cursor commit land before reconnecting.
    tokio::time::sleep(Duration::from_millis(200)).await;

    drop(first);
    first_task.await.unwrap().unwrap();

    // A fresh connection resuming the same durable name skips the acked records.
    let (mut second, second_task, _dir, _broker) =
        open_protocol_connection_for_broker(settings, broker, dir).await;
    handshake(&mut second).await;

    framed_subscribe_stream(
        &mut second,
        2,
        "plexus.resume",
        0,
        Some("c1"),
        StreamStart::Earliest,
        false,
    )
    .await;

    send_stream_publish(&mut second, 3, "plexus.resume", 0, b"c", false).await;
    let resumed = recv_stream_deliver(&mut second).await;
    assert_eq!(resumed.payload, b"c".to_vec());
    assert_eq!(resumed.offset, 2);

    drop(second);
    second_task.await.unwrap().unwrap();
}

/// Spawn a broker behind an accept-loop listener so a test can open many client
/// connections to it (real traffic across the wire).
async fn start_fanout_broker() -> (std::net::SocketAddr, Arc<Broker<StromaEngine>>, TempDir) {
    let (broker, dir) = open_test_broker().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server_broker = broker.clone();
    tokio::spawn(async move {
        loop {
            let Ok((server, peer)) = listener.accept().await else {
                return;
            };
            let broker = server_broker.clone();
            tokio::spawn(async move {
                let tcp_stats = TcpStats::new(10);
                let connection_stats = ConnectionStats::new();
                let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
                let _ = handle_connection(
                    server,
                    Some(peer),
                    broker,
                    tcp_stats,
                    connection_stats,
                    conn_id,
                    None::<StaticAuthHandler>,
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
    (addr, broker, dir)
}

async fn connect_and_handshake(addr: std::net::SocketAddr) -> Conn {
    let client = TcpStream::connect(addr).await.unwrap();
    let mut framed = plain_conn(client);
    handshake(&mut framed).await;
    framed
}

#[tokio::test]
async fn plexus_fans_out_to_many_subscribers() {
    let (addr, _broker, _dir) = start_fanout_broker().await;

    let mut admin = connect_and_handshake(addr).await;
    framed_declare_plexus(&mut admin, 1, "plexus.scale", Some(1)).await;

    // Several independent fan-out consumers, each a distinct durable name, all
    // reading from the live tail.
    const SUBS: usize = 5;
    let mut subs = Vec::with_capacity(SUBS);
    for i in 0..SUBS {
        let mut s = connect_and_handshake(addr).await;
        framed_subscribe_stream(
            &mut s,
            2,
            "plexus.scale",
            0,
            Some(&format!("c{i}")),
            StreamStart::Latest,
            true,
        )
        .await;
        subs.push(s);
    }

    // Burst of records on a dedicated publisher connection.
    let mut publisher = connect_and_handshake(addr).await;
    const N: u64 = 50;
    for n in 0..N {
        send_stream_publish(
            &mut publisher,
            100 + n,
            "plexus.scale",
            0,
            format!("m{n}").as_bytes(),
            false,
        )
        .await;
    }

    // Every consumer sees every record, in offset order (fan-out, not work-share).
    for s in &mut subs {
        for n in 0..N {
            let d = recv_stream_deliver(s).await;
            assert_eq!(d.offset, n);
            assert_eq!(d.payload, format!("m{n}").into_bytes());
        }
    }
}

#[tokio::test]
async fn declaring_a_queue_then_plexus_same_topic_is_rejected() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    // Declare a queue, then try to declare the same topic as a plexus stream.
    framed
        .send(
            try_encode(
                Op::DeclareQueue,
                2,
                &DeclareQueue {
                    topic: "shared.kind".into(),
                    group: None,
                    dlq_policy: None,
                    dlq_max_retries: None,
                    partition_count: Some(1),
                    default_message_ttl_ms: None,
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
                Op::DeclarePlexus,
                3,
                &DeclarePlexus {
                    topic: "shared.kind".into(),
                    partition_count: Some(1),
                    durability: Default::default(),
                    retention: StreamRetention::default(),
                    replication_factor: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::Error as u16);
    let err: ErrorMsg = try_decode(&frame).unwrap();
    assert_eq!(err.code, 400);

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn queue_subscribe_to_a_plexus_topic_is_rejected() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    framed_declare_plexus(&mut framed, 2, "stream.only", Some(1)).await;

    // A queue-style Subscribe to a stream partition must be rejected up front.
    framed
        .send(
            try_encode(
                Op::Subscribe,
                3,
                &Subscribe {
                    topic: "stream.only".into(),
                    partition: Partition::new(0),
                    group: None,
                    prefetch: 1,
                    auto_ack: false,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let frame = recv_frame(&mut framed).await;
    assert_eq!(frame.opcode, Op::SubscribeErr as u16);

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn multi_partition_queue_blocks_plexus_no_mixed_partitions() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;

    // A 3-partition queue.
    framed
        .send(
            try_encode(
                Op::DeclareQueue,
                2,
                &DeclareQueue {
                    topic: "mix.guard".into(),
                    group: None,
                    dlq_policy: None,
                    dlq_max_retries: None,
                    partition_count: Some(3),
                    default_message_ttl_ms: None,
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

    // Declaring it as a plexus must fail (partition 0 collides before any stream
    // partition is materialized).
    framed
        .send(
            try_encode(
                Op::DeclarePlexus,
                3,
                &DeclarePlexus {
                    topic: "mix.guard".into(),
                    partition_count: Some(3),
                    durability: Default::default(),
                    retention: StreamRetention::default(),
                    replication_factor: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut framed).await.opcode, Op::Error as u16);

    // No partition became a stream: a stream subscribe to any partition is
    // rejected (the topic is a queue end to end).
    for partition in 0..3u32 {
        framed
            .send(
                try_encode(
                    Op::SubscribeStream,
                    10 + partition as u64,
                    &SubscribeStream {
                        topic: "mix.guard".into(),
                        partition: Partition::new(partition),
                        durable_name: None,
                        start: StreamStart::Latest,
                        filter: Vec::new(),
                        prefetch: 1,
                        auto_ack: true,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            recv_frame(&mut framed).await.opcode,
            Op::SubscribeErr as u16
        );
    }

    drop(framed);
    server_task.await.unwrap().unwrap();
}

async fn framed_declare_plexus_durability(
    framed: &mut Conn,
    request_id: u64,
    topic: &str,
    durability: StreamDurability,
) {
    framed
        .send(
            try_encode(
                Op::DeclarePlexus,
                request_id,
                &DeclarePlexus {
                    topic: topic.into(),
                    partition_count: Some(1),
                    durability,
                    retention: StreamRetention::default(),
                    replication_factor: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(framed).await.opcode, Op::DeclarePlexusOk as u16);
}

#[tokio::test]
async fn plexus_ephemeral_delivers_and_confirms() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;
    framed_declare_plexus_durability(&mut framed, 2, "plexus.eph", StreamDurability::Ephemeral)
        .await;
    framed_subscribe_stream(
        &mut framed,
        3,
        "plexus.eph",
        0,
        None,
        StreamStart::Latest,
        false,
    )
    .await;

    send_stream_publish(&mut framed, 4, "plexus.eph", 0, b"x", true).await;
    // Both the confirm and the delivery arrive; the record is not marked speculative.
    let mut got_ok = false;
    let mut delivered: Option<Deliver> = None;
    for _ in 0..2 {
        let frame = recv_frame(&mut framed).await;
        if frame.opcode == Op::PublishOk as u16 {
            got_ok = true;
        } else if frame.opcode == Op::Deliver as u16 {
            delivered = Some(try_decode(&frame).unwrap());
        }
    }
    assert!(got_ok, "ephemeral publish should confirm");
    let d = delivered.expect("ephemeral record should be delivered");
    assert_eq!(d.payload, b"x".to_vec());
    assert!(!d.headers.contains_key(HEADER_SPECULATIVE));

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[tokio::test]
async fn plexus_speculative_marks_delivery_and_confirms() {
    let (mut framed, server_task, _dir) = open_protocol_connection().await;
    handshake(&mut framed).await;
    framed_declare_plexus_durability(&mut framed, 2, "plexus.spec", StreamDurability::Speculative)
        .await;
    framed_subscribe_stream(
        &mut framed,
        3,
        "plexus.spec",
        0,
        None,
        StreamStart::Latest,
        false,
    )
    .await;

    send_stream_publish(&mut framed, 4, "plexus.spec", 0, b"y", true).await;
    let mut got_ok = false;
    let mut delivered: Option<Deliver> = None;
    for _ in 0..2 {
        let frame = recv_frame(&mut framed).await;
        if frame.opcode == Op::PublishOk as u16 {
            got_ok = true;
        } else if frame.opcode == Op::Deliver as u16 {
            delivered = Some(try_decode(&frame).unwrap());
        }
    }
    assert!(got_ok, "speculative publish should confirm once durable");
    let d = delivered.expect("speculative record should be delivered");
    assert_eq!(d.payload, b"y".to_vec());
    // The broker marks a speculative delivery with the server-owned header.
    assert_eq!(
        d.headers.get(HEADER_SPECULATIVE).map(String::as_str),
        Some("1")
    );

    drop(framed);
    server_task.await.unwrap().unwrap();
}

#[test]
fn topology_adoption_tracker_reports_minimum_of_acked_connections() {
    let tracker = TopologyAdoptionTracker::new();
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();

    // No acks yet -> no adoption signal.
    assert_eq!(tracker.min_acked_generation(), None);

    // The minimum is taken across connections that have acked.
    tracker.record(a, 7);
    assert_eq!(tracker.min_acked_generation(), Some(7));
    tracker.record(b, 5);
    assert_eq!(tracker.min_acked_generation(), Some(5));

    // Acks are monotonic per connection: a stale ack never lowers a connection.
    tracker.record(b, 3);
    assert_eq!(tracker.min_acked_generation(), Some(5));

    // The laggard catching up raises the cluster minimum.
    tracker.record(b, 9);
    assert_eq!(tracker.min_acked_generation(), Some(7));

    // A gone connection no longer holds the minimum down.
    tracker.remove(&a);
    assert_eq!(tracker.min_acked_generation(), Some(9));
    tracker.remove(&b);
    assert_eq!(tracker.min_acked_generation(), None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_seal_requires_node_auth_and_exact_committed_authority_over_tcp() {
    use fibril_broker::coordination::{
        DeterministicPartitionPlacement, DeterministicStreamPlacement,
    };
    use fibril_coordination_ganglion::GanglionCoordination;
    use fibril_protocol::v1::{Auth, RecoverySeal, RecoverySealOk};
    use ganglion_openraft::{InProcessRouter, RaftMetadataNode, default_raft_config};
    use std::collections::BTreeMap;

    let router = InProcessRouter::new();
    let raft = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
        .await
        .unwrap();
    raft.initialize(BTreeMap::from([(
        1,
        ganglion_openraft::openraft::BasicNode::new("coordinator"),
    )]))
    .await
    .unwrap();
    raft.wait_for_leader(1, Duration::from_secs(10))
        .await
        .unwrap();
    let provider = Arc::new(GanglionCoordination::new("b", raft));
    let node = |id: &str| NodeInfo {
        node_id: id.into(),
        broker_addr: "127.0.0.1:1".into(),
        admin_addr: None,
    };
    for id in ["a", "b", "c"] {
        provider.register_self(&node(id)).await.unwrap();
    }
    let queue = QueueIdentity::new("seal-wire", Partition::new(0), None);
    register_legacy_test_queue(&provider, &queue).await;
    let mut live = HashMap::from([
        ("a".into(), node("a")),
        ("b".into(), node("b")),
        ("c".into(), node("c")),
    ]);
    provider
        .control_iteration(
            &DeterministicPartitionPlacement,
            &provider.registered_queues(),
            &DeterministicStreamPlacement,
            &provider.registered_streams(),
            2,
            2,
            ReplicationDurabilityPolicy::MajorityDurable,
            &live,
            8,
        )
        .await
        .unwrap()
        .unwrap();
    live.remove("a");
    provider
        .control_iteration(
            &DeterministicPartitionPlacement,
            &provider.registered_queues(),
            &DeterministicStreamPlacement,
            &provider.registered_streams(),
            2,
            2,
            ReplicationDurabilityPolicy::MajorityDurable,
            &live,
            8,
        )
        .await
        .unwrap()
        .unwrap();
    let pending = provider.pending_recoveries().unwrap().remove(0);
    let command = pending.seal_command().unwrap();
    let request = RecoverySeal {
        topic: command.topic,
        partition: command.partition,
        group: command.group,
        stream: command.stream,
        transition: command.transition,
        fence_epoch: command.fence_epoch,
    };
    let (engine, mut dir) = open_test_engine().await;
    let broker = Broker::new_with_ownership(
        engine.clone(),
        BrokerConfig::default(),
        None,
        provider.clone(),
    );
    broker
        .become_replication_follower_with_epoch(
            &request.topic,
            request.partition,
            None,
            pending.previous.epoch,
        )
        .await
        .unwrap();
    engine
        .apply_replicated_queue_batch(
            &request.topic,
            0,
            None,
            Some(stroma_core::ReplicatedMessageBatch {
                epoch: pending.previous.epoch,
                first_offset: 0,
                records: vec![stroma_core::Message {
                    flags: 0,
                    headers: vec![],
                    payload: b"sealed-wire-body".to_vec(),
                }],
                durability: None,
            }),
            None,
        )
        .await
        .unwrap();
    for identity in [None, Some("ordinary-user"), Some("@node")] {
        let auth = identity.map(|name| StaticAuthHandler::new(name.into(), "secret".into()));
        let (addr, task, returned, _) = start_protocol_listener_for_broker(
            ConnectionSettings::new(Some(60)),
            broker.clone(),
            dir,
            auth,
        )
        .await;
        dir = returned;
        let mut conn = plain_conn(TcpStream::connect(addr).await.unwrap());
        handshake(&mut conn).await;
        if let Some(identity) = identity {
            conn.send(
                try_encode(
                    Op::Auth,
                    2,
                    &Auth {
                        username: identity.into(),
                        password: "secret".into(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(recv_frame(&mut conn).await.opcode, Op::AuthOk as u16);
        }
        if identity != Some("@node") {
            let read = fibril_protocol::v1::RecoveryRead {
                seal: request.clone(),
                history_id: [0; 32],
                source: 0,
                from: 0,
                max_records: 1,
                max_bytes: 1024,
            };
            conn.send(try_encode(Op::RecoveryRead, 30, &read).unwrap())
                .await
                .unwrap();
            let error: ErrorMsg = try_decode(&recv_frame(&mut conn).await).unwrap();
            assert_eq!(error.code, 403);
        }
        if identity == Some("@node") {
            let mut stale = request.clone();
            stale.transition[0] ^= 1;
            conn.send(try_encode(Op::RecoverySeal, 3, &stale).unwrap())
                .await
                .unwrap();
            assert_eq!(recv_frame(&mut conn).await.opcode, Op::Error as u16);
            broker
                .become_replication_follower_with_epoch(
                    &request.topic,
                    request.partition,
                    None,
                    pending.previous.epoch,
                )
                .await
                .unwrap();
        }
        conn.send(try_encode(Op::RecoverySeal, 4, &request).unwrap())
            .await
            .unwrap();
        let reply = recv_frame(&mut conn).await;
        if identity != Some("@node") {
            let error: ErrorMsg = try_decode(&reply).unwrap();
            assert_eq!(error.code, 403);
            broker
                .become_replication_follower_with_epoch(
                    &request.topic,
                    request.partition,
                    None,
                    pending.previous.epoch,
                )
                .await
                .unwrap();
        } else {
            let sealed: RecoverySealOk = try_decode(&reply).unwrap();
            assert_eq!(sealed.replica_id, "b");
            assert_eq!(sealed.transition, request.transition);
            assert_eq!(sealed.fence_epoch, request.fence_epoch);
            assert_eq!(sealed.history_version, 1);
            let valid = fibril_protocol::v1::RecoveryRead {
                seal: request.clone(),
                history_id: sealed.history_id,
                source: 0,
                from: 0,
                max_records: 1,
                max_bytes: 1024,
            };
            for mutation in 0..4 {
                let mut bad = valid.clone();
                match mutation {
                    0 => bad.seal.transition[0] ^= 1,
                    1 => bad.history_id[0] ^= 1,
                    2 => bad.from = 2,
                    _ => bad.max_bytes = 0,
                }
                conn.send(try_encode(Op::RecoveryRead, 31, &bad).unwrap())
                    .await
                    .unwrap();
                assert_eq!(recv_frame(&mut conn).await.opcode, Op::Error as u16);
            }
            conn.send(try_encode(Op::RecoveryRead, 32, &valid).unwrap())
                .await
                .unwrap();
            let page: fibril_protocol::v1::RecoveryReadOk =
                try_decode(&recv_frame(&mut conn).await).unwrap();
            assert_eq!(page.records[0].payload, b"sealed-wire-body");
            conn.send(try_encode(Op::RecoverySeal, 5, &request).unwrap())
                .await
                .unwrap();
            assert_eq!(
                try_decode::<RecoverySealOk>(&recv_frame(&mut conn).await).unwrap(),
                sealed
            );
            assert!(
                broker
                    .become_replication_follower_with_epoch(
                        &request.topic,
                        request.partition,
                        None,
                        pending.previous.epoch
                    )
                    .await
                    .is_err()
            );
        }
        drop(conn);
        task.await.unwrap().unwrap();
    }
    // The explicit collector transport reauthenticates on each retry and
    // attributes evidence to the contacted old replica. Retries count once.
    let mut witnesses = fibril_coordination_ganglion::recovery_witnesses::RecoveryWitnessSet::new(
        &provider.consensus_node().committed_snapshot(),
        &pending,
    )
    .unwrap();
    for _ in 0..2 {
        let (addr, task, returned, _) = start_protocol_listener_for_broker(
            ConnectionSettings::new(Some(60)),
            broker.clone(),
            dir,
            Some(node_auth()),
        )
        .await;
        dir = returned;
        let config =
            ProtocolOwnerPeerResolverConfig::new(HashMap::from([("b".into(), addr.to_string())]))
                .with_auth("@node", "secret");
        let evidence = fibril_protocol::v1::replication::request_recovery_seal(
            &config,
            "b",
            witnesses.command(),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
        let retained = evidence.clone();
        witnesses
            .record(
                &provider.consensus_node().committed_snapshot(),
                "b",
                evidence,
            )
            .unwrap();
        task.await.unwrap().unwrap();
        for source in [
            fibril_broker::recovery::RecoveryReadSource::Messages,
            fibril_broker::recovery::RecoveryReadSource::Events,
            fibril_broker::recovery::RecoveryReadSource::Snapshot,
        ] {
            let (addr, task, returned, _) = start_protocol_listener_for_broker(
                ConnectionSettings::new(Some(60)),
                broker.clone(),
                dir,
                Some(node_auth()),
            )
            .await;
            dir = returned;
            let cfg = ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
                "b".into(),
                addr.to_string(),
            )]))
            .with_auth("@node", "secret");
            let read = fibril_broker::recovery::RecoveryReadRequest {
                seal: retained.seal.request.clone(),
                history_id: retained.seal.history.id,
                source,
                from: 0,
                max_records: 10,
                max_bytes: 1024,
            };
            let page = fibril_protocol::v1::replication::request_recovery_read(
                &cfg,
                witnesses.command(),
                &retained,
                &read,
                Duration::from_secs(10),
            )
            .await
            .unwrap();
            if source == fibril_broker::recovery::RecoveryReadSource::Messages {
                assert_eq!(page.records[0].payload, b"sealed-wire-body");
                assert_eq!(page.next, 1);
            } else {
                assert!(page.records.is_empty());
            }
            task.await.unwrap().unwrap();
        }
    }
    assert_eq!(
        witnesses
            .progress(&provider.consensus_node().committed_snapshot())
            .unwrap(),
        fibril_coordination_ganglion::recovery_witnesses::SealCollectionProgress::AwaitingSeals {
            received: 1,
            required: 2,
            missing: vec!["a".into(), "c".into()],
        }
    );
    assert_eq!(provider.pending_recoveries().unwrap(), vec![pending]);
    broker.shutdown().await;
    provider.consensus_node().shutdown().await.unwrap();
}

#[tokio::test]
async fn replication_controls_do_not_inherit_node_privileges_from_a_resumed_session() {
    use fibril_protocol::v1::{Auth, RecoverySeal};
    let settings = ConnectionSettings::new(Some(60)).with_reconnect_grace_ms(Some(5_000));
    let (broker, dir) = open_test_broker().await;
    let auth = StaticAuthHandler::new("@node".into(), "secret".into());
    let (addr, task, dir, _) = start_protocol_listener_for_broker(
        settings.clone(),
        broker.clone(),
        dir,
        Some(auth.clone()),
    )
    .await;
    let mut first = plain_conn(TcpStream::connect(addr).await.unwrap());
    let hello = handshake_with_resume(&mut first, None).await;
    first
        .send(
            try_encode(
                Op::Auth,
                2,
                &Auth {
                    username: "@node".into(),
                    password: "secret".into(),
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(recv_frame(&mut first).await.opcode, Op::AuthOk as u16);
    drop(first);
    task.await.unwrap().unwrap();
    let (addr, task, _dir, _) =
        start_protocol_listener_for_broker(settings, broker.clone(), dir, Some(auth)).await;
    let mut second = plain_conn(TcpStream::connect(addr).await.unwrap());
    let resumed = handshake_with_resume(
        &mut second,
        Some(ResumeIdentity {
            owner_id: hello.owner_id,
            client_id: hello.client_id,
            resume_token: hello.resume_token,
        }),
    )
    .await;
    assert_eq!(resumed.resume_outcome, ResumeOutcome::Resumed);
    second
        .send(
            try_encode(
                Op::RecoverySeal,
                3,
                &RecoverySeal {
                    topic: "q".into(),
                    partition: Partition::new(0),
                    group: None,
                    stream: false,
                    transition: [0; 32],
                    fence_epoch: 1,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_error_frame(&mut second, 3, 403).await;
    assert_replication_controls_forbidden(&mut second).await;
    drop(second);
    task.await.unwrap().unwrap();
    broker.shutdown().await;
}


// Exercise every internal request before payload decoding, including stream
// controls that otherwise return no response when no stream has been opened.
async fn assert_replication_controls_forbidden(conn: &mut Conn) {
    for opcode in [
        Op::ReplicationRead,
        Op::StreamReplicationRead,
        Op::ReplicationApply,
        Op::ReplicationCheckpointExport,
        Op::ReplicationCheckpointInstall,
        Op::ReplicationStreamStart,
        Op::ReplicationStreamProgress,
        Op::ReplicationStreamReset,
        Op::ReplicationStreamStop,
        Op::RecoverySeal,
        Op::RecoveryRead,
        Op::InitialHistoryPrepare,
        Op::RecoveryTransfer,
        Op::HistoryReplication,
    ] {
        conn.send(Frame {
            version: PROTOCOL_V1,
            flags: 0,
            opcode: opcode as u16,
            request_id: 910,
            payload: Bytes::new(),
        })
        .await
        .unwrap();
        let frame = recv_frame(conn).await;
        assert_eq!(frame.opcode, Op::Error as u16, "opcode {opcode:?}");
        let error: ErrorMsg = try_decode(&frame).unwrap();
        assert_eq!(error.code, 403, "opcode {opcode:?}: {error:?}");
    }
    assert_connection_still_responds(conn).await;
}

#[tokio::test]
async fn replication_controls_reject_anonymous_and_ordinary_users() {
    use fibril_protocol::v1::Auth;
    for identity in [None, Some("ordinary-user")] {
        let (broker, dir) = open_test_broker().await;
        let (addr, task, _dir, _) = start_protocol_listener_for_broker(
            ConnectionSettings::new(Some(60)),
            broker.clone(),
            dir,
            identity.map(|name| StaticAuthHandler::new(name.into(), "secret".into())),
        )
        .await;
        let mut conn = plain_conn(TcpStream::connect(addr).await.unwrap());
        handshake(&mut conn).await;
        if let Some(identity) = identity {
            conn.send(
                try_encode(
                    Op::Auth,
                    2,
                    &Auth {
                        username: identity.into(),
                        password: "secret".into(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
            assert_eq!(recv_frame(&mut conn).await.opcode, Op::AuthOk as u16);
        }
        // A valid read must not leak retained records to an ordinary client.
        framed_publish(&mut conn, 3, "private-replica", None, b"private-body").await;
        conn.send(
            try_encode(
                Op::ReplicationRead,
                4,
                &ReplicationRead {
                    topic: "private-replica".into(),
                    group: None,
                    partition: Partition::new(0),
                    message_from: 0,
                    event_from: 0,
                    max_messages: 10,
                    max_events: 10,
                    max_bytes: 1024 * 1024,
                    max_wait_ms: 0,
                    reporter_node_id: None,
                    reporter_epoch: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert_error_frame(&mut conn, 4, 403).await;
        assert_replication_controls_forbidden(&mut conn).await;
        broker
            .become_replication_follower("private-follower", Partition::new(0), None)
            .await
            .unwrap();
        conn.send(
            try_encode(
                Op::ReplicationApply,
                5,
                &ReplicationApply {
                    topic: "private-follower".into(),
                    group: None,
                    partition: Partition::new(0),
                    messages: Some(ReplicationMessageApplyBatch {
                        epoch: 0,
                        records: vec![ReplicationMessageRecord {
                            offset: 0,
                            flags: 0,
                            headers: vec![],
                            payload: b"forged".to_vec(),
                        }],
                    }),
                    events: None,
                },
            )
            .unwrap(),
        )
        .await
        .unwrap();
        assert_error_frame(&mut conn, 5, 403).await;
        assert_eq!(
            broker
                .promote_replication_follower_if_caught_up(
                    "private-follower",
                    Partition::new(0),
                    None,
                    0,
                    0,
                )
                .await
                .unwrap(),
            QueuePromotionOutcome::Promoted {
                message_next_offset: 0,
                event_next_offset: 0,
                applied_event_offset: None,
            }
        );
        drop(conn);
        task.await.unwrap().unwrap();
        broker.shutdown().await;
    }
}


#[tokio::test]
async fn owner_peer_reauthenticates_after_transport_loss() {
    use fibril_protocol::v1::{Auth, replication::ProtocolOwnerPeerAuth};
    let (broker, _dir) = open_test_broker().await;
    let publisher = broker
        .get_publisher("reconnect-auth", Partition::new(0), &None)
        .await
        .unwrap();
    publisher
        .publish(
            b"after-reconnect".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap()
        .await
        .unwrap()
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving_broker = broker.clone();
    let server = tokio::spawn(async move {
        // Lose the first transport after successful authentication. The next
        // attempt reaches the real handler and must authenticate again.
        let (socket, _) = listener.accept().await.unwrap();
        let mut first = plain_conn(socket);
        let hello = recv_frame(&mut first).await;
        assert_eq!(hello.opcode, Op::Hello as u16);
        first
            .send(
                try_encode(
                    Op::HelloOk,
                    hello.request_id,
                    &HelloOk {
                        protocol_version: PROTOCOL_V1,
                        owner_id: Uuid::now_v7(),
                        client_id: Uuid::now_v7(),
                        resume_token: Uuid::now_v7(),
                        resume_outcome: ResumeOutcome::New,
                        server_name: "dropped-owner".into(),
                        compliance: "test".into(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        let frame = recv_frame(&mut first).await;
        let auth: Auth = try_decode(&frame).unwrap();
        assert_eq!(auth.username, "@node");
        assert_eq!(auth.password, "secret");
        first
            .send(try_encode(Op::AuthOk, frame.request_id, &()).unwrap())
            .await
            .unwrap();
        drop(first);
        let (socket, peer) = listener.accept().await.unwrap();
        let stats = ConnectionStats::new();
        let id = stats.add_connection(peer, Instant::now(), false);
        handle_connection(
            socket,
            Some(peer),
            serving_broker,
            TcpStats::new(10),
            stats,
            id,
            Some(node_auth()),
            None,
            ConnectionSettings::new(Some(60)),
            None,
            None,
            None,
        )
        .await
    });
    let peer = ProtocolOwnerReplicationPeer::new_reconnecting(
        addr.to_string(),
        Some(ProtocolOwnerPeerAuth {
            username: "@node".into(),
            password: "secret".into(),
        }),
        "reconnect-test".into(),
        "test".into(),
    );
    assert!(
        peer.read_owner_replication_records(
            "reconnect-auth",
            Partition::new(0),
            None,
            0,
            0,
            8,
            8,
            1024 * 1024,
            0
        )
        .await
        .is_err()
    );
    let records = peer
        .read_owner_replication_records(
            "reconnect-auth",
            Partition::new(0),
            None,
            0,
            0,
            8,
            8,
            1024 * 1024,
            0,
        )
        .await
        .unwrap();
    let OwnerReplicationRead::Batch(messages) = records.messages else {
        panic!("expected records")
    };
    assert_eq!(messages.records[0].1.payload, b"after-reconnect");
    peer.close().await;
    server.await.unwrap().unwrap();
    broker.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_pair_inspection_compares_real_peers_and_discards_incomplete_reads() {
    sealed_pair_inspection_scenario(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_pair_checkpoint_replay_compares_different_boundaries_over_tcp() {
    sealed_pair_inspection_scenario(true).await;
}

async fn sealed_pair_inspection_scenario(checkpoints: bool) {
    use fibril_broker::coordination::{
        DeterministicPartitionPlacement, DeterministicStreamPlacement,
    };
    use fibril_broker::recovery::inspection::{
        RecoveryInspectionLimits, RecoveryOverlap, RecoveryProofRequirement,
    };
    use fibril_coordination_ganglion::GanglionCoordination;
    use fibril_protocol::v1::recovery_inspection::inspect_recovery_pair;
    use ganglion_openraft::{InProcessRouter, RaftMetadataNode, default_raft_config};
    use std::collections::BTreeMap;

    let router = InProcessRouter::new();
    let raft = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
        .await
        .unwrap();
    let other = RaftMetadataNode::start(2, default_raft_config().unwrap(), &router)
        .await
        .unwrap();
    // Raft replication uses the in-process router; follower authorization writes
    // forward over real metadata TCP to the elected leader.
    let metadata_stop = tokio_util::sync::CancellationToken::new();
    let mut metadata_servers = vec![];
    let mut members = BTreeMap::new();
    for node in [&raft, &other] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            node.node_id(),
            ganglion_openraft::openraft::BasicNode::new(listener.local_addr().unwrap().to_string()),
        );
        let handle = node.raft().clone();
        let stopped = metadata_stop.clone();
        metadata_servers.push(tokio::spawn(async move {
            let mut connections=tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _=stopped.cancelled()=>break,
                    accepted=listener.accept()=>{
                        let (socket,_)=accepted.unwrap();let handle=handle.clone();
                        connections.spawn(async move { ganglion_openraft::serve_connection(socket,handle,ganglion_openraft::WireFormat::default()).await });
                    }
                    _=connections.join_next(), if !connections.is_empty()=>{}
                }
            }
            connections.shutdown().await;
        }));
    }
    raft.initialize(members).await.unwrap();
    raft.wait_for_leader(1, Duration::from_secs(10))
        .await
        .unwrap();
    let providers = [
        Arc::new(GanglionCoordination::new("b", raft)),
        Arc::new(GanglionCoordination::new("c", other)),
    ];
    let provider = &providers[0];
    let node = |id: &str| NodeInfo {
        node_id: id.into(),
        broker_addr: "127.0.0.1:1".into(),
        admin_addr: None,
    };
    for id in ["a", "b", "c"] {
        provider.register_self(&node(id)).await.unwrap();
    }
    let queue = QueueIdentity::new("inspect-wire", Partition::new(0), None);
    register_legacy_test_queue(&provider, &queue).await;
    let mut live = HashMap::from([
        ("a".into(), node("a")),
        ("b".into(), node("b")),
        ("c".into(), node("c")),
    ]);
    for remove in [false, true] {
        if remove {
            live.remove("a");
        }
        provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                2,
                2,
                ReplicationDurabilityPolicy::MajorityDurable,
                &live,
                8,
            )
            .await
            .unwrap()
            .unwrap();
    }
    let pending = provider.pending_recoveries().unwrap().remove(0);
    let command = pending.seal_command().unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while providers[1].pending_recoveries().unwrap() != vec![pending.clone()] {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let mut dirs = vec![];
    let mut brokers = vec![];
    let mut seals = vec![];
    let mut servers = vec![];
    let mut nodes = HashMap::new();
    for (index, id) in ["b", "c"].into_iter().enumerate() {
        let (engine, dir) = open_test_engine().await;
        let broker = Broker::new_with_ownership(
            engine.clone(),
            BrokerConfig::default(),
            None,
            providers[index].clone(),
        );
        broker
            .become_replication_follower_with_epoch(
                &command.topic,
                command.partition,
                None,
                pending.previous.epoch,
            )
            .await
            .unwrap();
        engine
            .apply_replicated_queue_batch(
                &command.topic,
                0,
                None,
                Some(stroma_core::ReplicatedMessageBatch {
                    epoch: pending.previous.epoch,
                    first_offset: 0,
                    durability: None,
                    records: (0..index + 2)
                        .map(|i| stroma_core::Message {
                            flags: 0,
                            headers: vec![],
                            payload: vec![i as u8],
                        })
                        .collect(),
                }),
                Some(stroma_core::ReplicatedEventBatch {
                    epoch: pending.previous.epoch,
                    first_offset: 0,
                    durability: None,
                    events: vec![
                        stroma_core::StromaEvent::EnqueueMany { reqs: vec![
                            stroma_core::EnqueueEventMeta { off: 0, retries: 0, expire_at: None },
                            stroma_core::EnqueueEventMeta { off: 1, retries: 2, expire_at: Some(500) },
                        ] },
                        stroma_core::StromaEvent::MarkInflight { off: 0, deadline: 300 },
                        stroma_core::StromaEvent::Ack { off: 0 },
                    ].into_iter().take(if checkpoints { index + 2 } else { 3 }).collect(),
                }),
            )
            .await
            .unwrap();
        if checkpoints {
            engine.snapshot_partition(&command.topic, 0, None).await.unwrap();
            if index == 0 {
                engine.apply_replicated_queue_batch(&command.topic, 0, None, None,
                    Some(stroma_core::ReplicatedEventBatch {
                        epoch: pending.previous.epoch, first_offset: 2, durability: None,
                        events: vec![stroma_core::StromaEvent::Ack { off: 0 }],
                    })).await.unwrap();
            }
        }
        seals.push(
            broker
                .seal_replica_for_recovery(command.clone())
                .await
                .unwrap(),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        nodes.insert(id.into(), listener.local_addr().unwrap().to_string());
        let serving = broker.clone();
        let stop = tokio_util::sync::CancellationToken::new();
        let cancelled = stop.clone();
        let task = tokio::spawn(async move {
            loop {
                let (socket, peer) = tokio::select! { _=cancelled.cancelled()=>break, accepted=listener.accept()=>accepted.unwrap() };
                let stats = ConnectionStats::new();
                let conn_id = stats.add_connection(peer, Instant::now(), false);
                handle_connection(
                    socket,
                    Some(peer),
                    serving.clone(),
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
                .await
                .unwrap();
            }
        });
        servers.push((stop, task));
        brokers.push(broker);
        dirs.push(dir);
    }
    let config = ProtocolOwnerPeerResolverConfig::new(nodes).with_auth("@node", "secret");
    let limits = RecoveryInspectionLimits {
        page_records: 1,
        ..Default::default()
    };
    let report = inspect_recovery_pair(
        &config,
        &command,
        &seals[0],
        &seals[1],
        limits,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    assert_eq!(
        report.evidence.messages.overlap,
        RecoveryOverlap::Matching { from: 0, next: 2 }
    );
    assert_eq!(
        report.evidence.events.overlap,
        RecoveryOverlap::Matching { from: 0, next: 3 }
    );
    assert_eq!(report.evidence.records, 11);
    assert!(
        report
            .evidence
            .remaining_proofs
            .contains(&RecoveryProofRequirement::CommonOriginAndInstalledLineage)
    );
    for target in [0, 3] {
        let replay = fibril_protocol::v1::recovery_inspection::inspect_recovery_pair_with_queue_replay(
            &config, &command, &seals[0], &seals[1], limits, target, Default::default(), Duration::from_secs(10),
        ).await.unwrap();
        let [left, right] = replay.evidence.queue_replay.unwrap();
        assert_eq!(left.event_next, target);
        assert_eq!(left.state_digest, right.state_digest);
        assert_eq!(left.required_message_next, if target == 0 { 0 } else { 2 });
        assert_ne!(left.message_digest, right.message_digest);
        assert_ne!(left.history_id, right.history_id);
    }
    if checkpoints {
        let result = fibril_protocol::v1::recovery_inspection::inspect_recovery_pair_with_checkpoints(
            &config, &command, &seals[0], &seals[1], limits, 3, Default::default(), 1024 * 1024,
            Duration::from_secs(10),
        ).await.unwrap();
        let [left, right] = result.evidence.queue_replay.unwrap();
        assert_eq!(left.checkpoint_event_next, Some(2));
        assert_eq!(right.checkpoint_event_next, Some(3));
        assert_eq!(left.lease_normalized_state_digest, right.lease_normalized_state_digest);
        assert_eq!(left.live_payload_digest, right.live_payload_digest);
        assert!(left.live_payload_digest.is_some());
        assert_ne!(left.message_digest, right.message_digest);
        let artifact = fibril_protocol::v1::recovery_inspection::inspect_recovery_source_artifact(
            &config, &command, &seals[0], limits, Default::default(), 1024 * 1024,
            1024 * 1024, Duration::from_secs(10),
        ).await.unwrap();
        assert_eq!(artifact.evidence().history_id, seals[0].seal.history.id);
        assert_eq!(artifact.evidence().event_next, seals[0].seal.event_next);
        assert_eq!(artifact.evidence().live_payload_digest, left.live_payload_digest);
        assert!(!artifact.state_snapshot().is_empty());
    }
    let error = inspect_recovery_pair(
        &config,
        &command,
        &seals[0],
        &seals[1],
        RecoveryInspectionLimits {
            total_pages: 1,
            ..limits
        },
        Duration::from_secs(10),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("page budget"));
    assert!(
        inspect_recovery_pair(
            &config,
            &command,
            &seals[0],
            &seals[0],
            limits,
            Duration::from_secs(10)
        )
        .await
        .is_err()
    );

    // Bound the whole operation when a peer accepts but never handshakes.
    let hanging = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = hanging.local_addr().unwrap();
    let (release, hold) = tokio::sync::oneshot::channel::<()>();
    let hanging_task = tokio::spawn(async move {
        let (_socket, _) = hanging.accept().await.unwrap();
        let _ = hold.await;
    });
    let hang_config =
        ProtocolOwnerPeerResolverConfig::new(HashMap::from([("b".into(), addr.to_string())]))
            .with_auth("@node", "secret");
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        inspect_recovery_pair(
            &hang_config,
            &command,
            &seals[0],
            &seals[1],
            limits,
            Duration::from_millis(100),
        ),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(error.to_string().contains("deadline"));
    release.send(()).unwrap();
    hanging_task.await.unwrap();

    // Losing the second source cannot return the first source's partial evidence.
    let (stop, task) = servers.pop().unwrap();
    stop.cancel();
    task.await.unwrap();
    assert!(
        inspect_recovery_pair(
            &config,
            &command,
            &seals[0],
            &seals[1],
            limits,
            Duration::from_secs(10)
        )
        .await
        .is_err()
    );
    let (stop, task) = servers.pop().unwrap();
    stop.cancel();
    task.await.unwrap();
    assert_eq!(provider.pending_recoveries().unwrap(), vec![pending]);
    for broker in brokers {
        broker.shutdown().await;
    }
    metadata_stop.cancel();
    for server in metadata_servers {
        server.await.unwrap();
    }
    for provider in providers {
        provider.consensus_node().shutdown().await.unwrap();
    }
    drop(dirs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initial_history_preparation_uses_authenticated_replicas_and_fresh_consensus() {
    authenticated_recovery_scenario(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_candidate_handoff_preserves_completed_stage_and_fences_old_candidate() {
    authenticated_recovery_scenario(true, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn excluded_learner_catches_up_without_interrupting_the_serving_quorum() {
    authenticated_recovery_scenario(false, true).await;
}

async fn authenticated_recovery_scenario(handoff: bool, learner: bool) {
    use fibril_broker::initial_history::{
        InitialHistoryLocalReceipt, InitialHistoryPrepareCommand,
    };
    use fibril_coordination_ganglion::{
        GanglionCoordination, initial_history::InitialHistoryReceiptSet,
    };
    use fibril_protocol::v1::initial_history::request_preparation;
    use ganglion_openraft::{RaftMetadataNode, default_raft_config};
    use std::collections::BTreeMap;

    async fn retry_metadata<T, F, Fut>(mut operation: F) -> T
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, ganglion_openraft::OpenraftAdapterError>>,
    {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                match operation().await {
                    Ok(value) => break value,
                    Err(
                        ganglion_openraft::OpenraftAdapterError::NotLeader
                        | ganglion_openraft::OpenraftAdapterError::GenerationMismatch { .. }
                        | ganglion_openraft::OpenraftAdapterError::AttributeMismatch { .. },
                    ) => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(error) => panic!("metadata operation failed: {error}"),
                }
            }
        })
        .await
        .unwrap()
    }

    async fn call(
        broker: Arc<Broker<StromaEngine>>,
        dir: TempDir,
        command: &InitialHistoryPrepareCommand,
    ) -> (
        Result<InitialHistoryLocalReceipt, fibril_broker::broker::BrokerError>,
        TempDir,
    ) {
        let (addr, task, dir, _) = start_protocol_listener_for_broker(
            ConnectionSettings::new(Some(60)),
            broker,
            dir,
            Some(node_auth()),
        )
        .await;
        let config = ProtocolOwnerPeerResolverConfig::new(HashMap::from([(
            command.replica_id.clone(),
            addr.to_string(),
        )]))
        .with_auth("@node", "secret");
        let reply = request_preparation(&config, command, Duration::from_secs(10)).await;
        task.await.unwrap().unwrap();
        (reply, dir)
    }
    async fn transfer(broker: Arc<Broker<StromaEngine>>,dir: TempDir,request: &fibril_broker::recovery_transfer::QueueRecoveryRequest) -> (Result<fibril_broker::recovery_transfer::QueueRecoveryReply,fibril_broker::broker::BrokerError>,TempDir) {
        let (addr,task,dir,_) = start_protocol_listener_for_broker(ConnectionSettings::new(Some(60)),broker,dir,Some(node_auth())).await;
        let config=ProtocolOwnerPeerResolverConfig::new(HashMap::from([(request.command.replica_id.clone(),addr.to_string())])).with_auth("@node","secret");
        let result=fibril_protocol::v1::recovery_transfer::request_transfer(&config,request,Duration::from_secs(10)).await;
        task.await.unwrap().unwrap(); (result,dir)
    }
    async fn synced(providers: &[Arc<GanglionCoordination>], generation: u64) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while providers
                .iter()
                .any(|p| p.consensus_node().committed_snapshot().generation < generation)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
    }
    async fn set_assignment(
        providers: &[Arc<GanglionCoordination>],
        resource: &ganglion_core::ResourceIdentity,
        epoch: u64,
    ) -> u64 {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for provider in providers {
                    if !provider.consensus_node().is_leader().await {
                        continue;
                    }
                    let mut snapshot = provider.consensus_node().committed_snapshot();
                    if !snapshot.resources.contains(resource) {
                        continue;
                    }
                    let generation = snapshot.generation;
                    let mut assignment = ganglion_core::PartitionAssignment::new(
                        resource.clone(),
                        "a",
                        vec!["b".into(), "c".into()],
                        epoch,
                    );
                    assignment.durability =
                        ganglion_core::ReplicationDurabilityPolicy::MajorityDurable;
                    snapshot.assignments.insert(resource.clone(), assignment);
                    snapshot.generation += 1;
                    match provider
                        .consensus_node()
                        .write_snapshot_guarded(generation, snapshot)
                        .await
                    {
                        Ok(_) => return generation + 1,
                        Err(
                            ganglion_openraft::OpenraftAdapterError::NotLeader
                            | ganglion_openraft::OpenraftAdapterError::GenerationMismatch { .. },
                        ) => {}
                        Err(error) => panic!("assignment write failed: {error}"),
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }
    async fn persist_pending(
        providers: &[Arc<GanglionCoordination>],
        resource: &ganglion_core::ResourceIdentity,
        activation: [u8; 32],
        candidate: &str,
    ) -> fibril_coordination_ganglion::promotion::PendingRecovery {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for provider in providers {
                    if !provider.consensus_node().is_leader().await {
                        continue;
                    }
                    let mut snapshot = provider.consensus_node().committed_snapshot();
                    let generation = snapshot.generation;
                    let previous = snapshot.assignments[resource].clone();
                    let mut proposed = previous.clone();
                    proposed.owner = candidate.into();
                    proposed.followers = ["a", "b", "c"].into_iter().filter(|n| *n != candidate).map(str::to_owned).collect();
                    proposed.epoch += 1;
                    let pending = fibril_coordination_ganglion::promotion::PendingRecovery {
                        version: 1,
                        resource_incarnation:
                            fibril_coordination_ganglion::history_identity::resource_incarnation(
                                &snapshot, resource,
                            )
                            .unwrap(),
                        previous_activation: Some(activation),
                        requested_generation: generation + 1,
                        previous,
                        proposed,
                        previous_write_nodes: 2,
                        proposed_write_nodes: 2,
                        required_old_witnesses: provider
                            .snapshot()
                            .assignment_for(
                                &resource.name,
                                Partition::new(resource.partition as u32),
                                resource.group.as_deref(),
                            )
                            .unwrap()
                            .history
                            .as_ref()
                            .unwrap()
                            .replicas
                            .len()
                            - 2
                            + 1,
                    };
                    snapshot.attributes.insert(
                        fibril_coordination_ganglion::promotion::pending_recovery_key(resource),
                        serde_json::to_string(&pending).unwrap(),
                    );
                    snapshot.generation += 1;
                    match provider
                        .consensus_node()
                        .write_snapshot_guarded(generation, snapshot)
                        .await
                    {
                        Ok(_) => return pending,
                        Err(
                            ganglion_openraft::OpenraftAdapterError::NotLeader
                            | ganglion_openraft::OpenraftAdapterError::GenerationMismatch { .. },
                        ) => {}
                        Err(error) => panic!("pending recovery write failed: {error}"),
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap()
    }
    let metadata_dir = TempDir {
        root: std::env::current_dir()
            .unwrap()
            .join("test_data")
            .join(format!("initial-history-metadata-{}", Uuid::now_v7())),
    };
    let mut nodes = vec![];
    let mut servers = vec![];
    for id in 1..=3 {
        let (node, server) = RaftMetadataNode::start_durable_tcp(
            id,
            default_raft_config().unwrap(),
            "127.0.0.1:0",
            metadata_dir.root.join(id.to_string()),
        )
        .await
        .unwrap();
        nodes.push(node);
        servers.push(server);
    }
    nodes[0]
        .initialize(
            servers
                .iter()
                .enumerate()
                .map(|(i, s)| {
                    (
                        i as u64 + 1,
                        ganglion_openraft::openraft::BasicNode::new(s.local_addr().to_string()),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .await
        .unwrap();
    let leader = nodes[0]
        .wait_for_any_leader(Duration::from_secs(10))
        .await
        .unwrap();
    let providers: Vec<_> = nodes
        .into_iter()
        .zip(["a", "b", "c"])
        .map(|(node, id)| Arc::new(GanglionCoordination::new(id, node)))
        .collect();
    let coordinator = &providers[leader as usize - 1];
    let resource =
        ganglion_core::ResourceIdentity::new("fibril/queue", "initial-wire", 0, None::<String>);
    coordinator
        .register_initial_history_resource(&resource)
        .await
        .unwrap();
    let generation = set_assignment(&providers, &resource, 1).await;
    synced(&providers, generation).await;
    let decision = providers[0]
        .prepare_initial_history(&resource)
        .await
        .unwrap();
    // A forwarded response can precede the caller's local watch update.
    let decision_key_seen = || {
        providers.iter().all(|p| {
            InitialHistoryReceiptSet::new(
                &p.consensus_node().committed_snapshot(),
                decision.clone(),
            )
            .is_ok()
        })
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while !decision_key_seen() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let mut receipts = InitialHistoryReceiptSet::new(
        &providers[0].consensus_node().committed_snapshot(),
        decision.clone(),
    )
    .unwrap();
    let mut engines = vec![];
    let mut brokers = vec![];
    let mut dirs = vec![];
    for provider in &providers {
        let (engine, dir) = open_test_engine().await;
        brokers.push(Broker::new_with_ownership(
            engine.clone(),
            BrokerConfig::default(),
            None,
            provider.clone(),
        ));
        engines.push(engine);
        dirs.push(Some(dir));
    }
    for (index, id) in ["a", "b"].into_iter().enumerate() {
        let command = decision.prepare_command(id).unwrap();
        let mut stale = command.clone();
        stale.decision[0] ^= 1;
        let (failed, dir) = call(brokers[index].clone(), dirs[index].take().unwrap(), &stale).await;
        dirs[index] = Some(dir);
        assert!(failed.is_err());
        assert!(
            engines[index]
                .storage_history_binding("initial-wire", 0, None)
                .unwrap()
                .is_none(),
            "stale authorization must fail before storage is prepared"
        );
        let mut first = None;
        for _ in 0..2 {
            let (reply, dir) = call(
                brokers[index].clone(),
                dirs[index].take().unwrap(),
                &command,
            )
            .await;
            dirs[index] = Some(dir);
            let receipt = reply.unwrap();
            if let Some(previous) = &first {
                assert_eq!(previous, &receipt);
            }
            first = Some(receipt.clone());
            receipts
                .record(
                    &providers[0].consensus_node().committed_snapshot(),
                    id,
                    receipt,
                )
                .unwrap();
        }
        assert!(matches!(
            engines[index]
                .ensure_queue_owner_epoch("initial-wire", 0, None, Some(1))
                .await,
            Err(stroma_core::StromaError::HistoryAdmissionRequired { .. })
        ));
    }
    let quorum = retry_metadata(|| providers[0].persist_initial_history_quorum(&receipts)).await;
    assert_eq!(quorum.reports.len(), 2);
    assert!(quorum.reports.contains_key("a") && quorum.reports.contains_key("b"));
    // Misrouting to another member must reject even with correct node credentials.
    let (wrong, dir) = call(
        brokers[2].clone(),
        dirs[2].take().unwrap(),
        &decision.prepare_command("b").unwrap(),
    )
    .await;
    dirs[2] = Some(dir);
    assert!(wrong.is_err());
    assert!(
        engines[2]
            .storage_history_binding("initial-wire", 0, None)
            .unwrap()
            .is_none()
    );
    // Activate the fixed majority without weakening its configured threshold.
    use fibril_broker::coordination::Coordination;
    tokio::time::timeout(Duration::from_secs(10), async {
        while providers[0]
            .prepared_initial_history_quorum(&decision)
            .unwrap()
            .is_none()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    retry_metadata(|| providers[0].commit_initial_history_activation(&decision, &engines[0])).await;
    tokio::time::timeout(Duration::from_secs(10), async {
        while providers.iter().any(|provider| {
            provider
                .initial_history_activation(&decision)
                .unwrap()
                .is_none()
        }) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let generation = providers[0]
        .consensus_node()
        .committed_snapshot()
        .generation;
    synced(&providers, generation).await;
    for index in 0..2 {
        retry_metadata(|| providers[index].admit_local_initial_history(&decision, &engines[index]))
            .await;
    }
    assert!(
        providers[2]
            .admit_local_initial_history(&decision, &engines[2])
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while providers[0]
            .snapshot()
            .assignment_for("initial-wire", Partition::new(0), None)
            .is_none()
            || providers[1]
                .snapshot()
                .assignment_for("initial-wire", Partition::new(0), None)
                .is_none()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let assignment = providers[0]
        .snapshot()
        .assignment_for("initial-wire", Partition::new(0), None)
        .unwrap()
        .clone();
    assert_eq!(assignment.replica_set_size(), 3);
    assert_eq!(assignment.durability_requirement().unwrap().nodes, 2);
    assert!(assignment.is_followed_by("b"));
    assert!(!assignment.is_followed_by("c"));
    let unprepared_view = providers[2].snapshot();
    let remote_route = unprepared_view
        .assignment_for("initial-wire", Partition::new(0), None)
        .unwrap();
    assert_eq!(remote_route.owner, "a");
    assert!(!remote_route.is_followed_by("c"));
    let history = assignment.history.as_ref().unwrap();
    let session = history
        .session("initial-wire", Partition::new(0), None, false, "b", "a")
        .unwrap();
    for (index, id) in ["a", "b"].into_iter().enumerate() {
        for result in brokers[index]
            .apply_assignment_snapshot_transitions(
                id,
                &CoordinationSnapshot::default(),
                &providers[index].snapshot(),
            )
            .await
        {
            result.unwrap();
        }
    }
    let (mut connection, owner_task, owner_dir, _) = open_node_connection_for_broker(
        ConnectionSettings::new(Some(60)),
        brokers[0].clone(),
        dirs[0].take().unwrap(),
    )
    .await;
    dirs[0] = Some(owner_dir);
    node_handshake(&mut connection).await;
    let read = ReplicationRead {
        topic: "initial-wire".into(),
        group: None,
        partition: Partition::new(0),
        message_from: 0,
        event_from: 0,
        max_messages: 8,
        max_events: 8,
        max_bytes: 65536,
        max_wait_ms: 0,
        reporter_node_id: Some("b".into()),
        reporter_epoch: Some(1),
    };
    // An authenticated node cannot fall back to an unstamped request, including
    // aliases for the physical default group.
    for group in [None, Some("default".into()), Some("".into())] {
        let mut legacy = read.clone();
        legacy.group = group;
        connection
            .send(try_encode(Op::ReplicationRead, 70, &legacy).unwrap())
            .await
            .unwrap();
        assert_eq!(recv_frame(&mut connection).await.opcode, Op::Error as u16);
    }
    for mutation in 0..10 {
        let mut bad = session.clone();
        match mutation {
            0 => bad.activation[0] ^= 1,
            1 => bad.binding.resource_incarnation[0] ^= 1,
            2 => bad.binding.accepted_history[0] ^= 1,
            3 => bad.binding.writer_session[0] ^= 1,
            4 => bad.sender_instance.process[0] ^= 1,
            5 => bad.sender_instance.storage[0] ^= 1,
            6 => bad.receiver_instance.process[0] ^= 1,
            7 => bad.receiver_instance.storage[0] ^= 1,
            8 => bad.sender = "c".into(),
            _ => bad.stream = true,
        }
        let frame = fibril_protocol::v1::history_replication::encode_frame(
            &bad,
            try_encode(Op::ReplicationRead, 71, &read).unwrap(),
        )
        .unwrap();
        connection.send(frame).await.unwrap();
        assert_eq!(
            recv_frame(&mut connection).await.opcode,
            Op::Error as u16,
            "mutation {mutation}"
        );
    }
    for mutation in 0..3 {
        let mut bad = read.clone();
        match mutation {
            0 => bad.topic = "other".into(),
            1 => bad.partition = Partition::new(1),
            _ => bad.reporter_node_id = Some("c".into()),
        }
        let frame = fibril_protocol::v1::history_replication::encode_frame(
            &session,
            try_encode(Op::ReplicationRead, 72, &bad).unwrap(),
        )
        .unwrap();
        connection.send(frame).await.unwrap();
        assert_eq!(recv_frame(&mut connection).await.opcode, Op::Error as u16);
    }
    let publisher = brokers[0]
        .get_publisher("initial-wire", Partition::new(0), &None)
        .await
        .unwrap();
    let mut confirmed = publisher
        .publish(
            b"activated-history".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    let peer = ProtocolOwnerReplicationPeer::new(connection)
        .with_reporter("b")
        .with_history_session(session.clone());
    // A durable owner write is insufficient for this majority confirmation.
    let records = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let records = peer.read_owner_replication_records_fenced("initial-wire", Partition::new(0), None, 0, 0, 8, 8, 65536, 20, Some(1)).await.unwrap();
            if matches!(&records.messages, OwnerReplicationRead::Batch(batch) if !batch.records.is_empty())
                && matches!(&records.events, OwnerReplicationRead::Batch(batch) if !batch.records.is_empty()) { break records; }
        }
    }).await.unwrap();
    assert!(matches!(
        confirmed.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    brokers[1]
        .apply_follower_replication_records(
            "initial-wire",
            Partition::new(0),
            None,
            ReplicationResourceKind::Queue,
            records,
        )
        .await
        .unwrap();
    peer.read_owner_replication_records_fenced(
        "initial-wire",
        Partition::new(0),
        None,
        1,
        1,
        8,
        8,
        65536,
        0,
        Some(1),
    )
    .await
    .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), confirmed)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        0
    );
    // Exercise the stamped streaming sender and progress path too.
    struct ApplyHistory(
        Arc<Broker<StromaEngine>>,
        fibril_broker::history_replication::HistoryReplicationSession,
    );
    impl fibril_broker::broker::BrokerReplicationStreamApply for ApplyHistory {
        fn apply_stream_batch<'a>(
            &'a self,
            records: fibril_broker::broker::BrokerOwnerReplicationRecords,
        ) -> futures::future::BoxFuture<
            'a,
            Result<
                fibril_broker::broker::ReplicatedStreamApply,
                fibril_broker::broker::BrokerError,
            >,
        > {
            Box::pin(async move {
                self.0.authorize_history_replication(&self.1, false)?;
                self.0
                    .apply_replicated_stream_batch("initial-wire", Partition::new(0), None, records)
                    .await
            })
        }
    }
    let (mut stream_connection, stream_task, dir, _) = open_node_connection_for_broker(
        ConnectionSettings::new(Some(60)),
        brokers[0].clone(),
        dirs[0].take().unwrap(),
    )
    .await;
    dirs[0] = Some(dir);
    node_handshake(&mut stream_connection).await;
    let streaming_peer = ProtocolOwnerReplicationPeer::new(stream_connection)
        .with_reporter("b")
        .with_history_session(session.clone());
    let apply = Arc::new(ApplyHistory(
        brokers[1].clone(),
        history
            .session("initial-wire", Partition::new(0), None, false, "a", "b")
            .unwrap(),
    ));
    let stop = CancellationToken::new();
    let streaming = tokio::spawn({
        let stop = stop.clone();
        async move {
            streaming_peer
                .stream_replication_fenced(
                    "initial-wire",
                    Partition::new(0),
                    None,
                    1,
                    1,
                    65536,
                    Arc::new(|| fibril_broker::replication::StreamApplyTunables {
                        keepalive_ms: 20,
                        apply_linger_us: 0,
                        max_merge_bytes: 65536,
                    }),
                    4,
                    apply,
                    stop,
                    Some(1),
                )
                .await
        }
    });
    let second = publisher
        .publish(
            b"streamed-history".to_vec(),
            unix_millis(),
            unix_millis(),
            None,
            Default::default(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        1
    );
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), streaming)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    stream_task.await.unwrap().unwrap();
    if learner {
        use fibril_broker::broker::{BrokerReplicationCatchUp, BrokerReplicationCatchUpOptions};
        use fibril_protocol::v1::replication::connect_protocol_owner_peer;
        for (provider, id) in providers.iter().zip(["a", "b", "c"]) {
            let node = NodeInfo {
                node_id: id.into(),
                broker_addr: "127.0.0.1:1".into(),
                admin_addr: None,
            };
            retry_metadata(|| provider.register_self(&node)).await;
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while providers
                .iter()
                .any(|p| p.consensus_node().committed_snapshot().nodes.len() != 3)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let before = providers[0].snapshot();
        let old_assignment = before
            .assignment_for("initial-wire", Partition::new(0), None)
            .unwrap()
            .clone();
        let intent = retry_metadata(|| providers[2].begin_queue_learner(&resource)).await;
        let prepared =
            retry_metadata(|| providers[2].prepare_queue_learner(&intent, &engines[2])).await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while providers.iter().any(|p| {
                intent
                    .session(&p.consensus_node().committed_snapshot())
                    .is_err()
            }) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(providers[0].begin_queue_learner(&resource).await.is_err());
        assert!(
            providers[0]
                .prepare_queue_learner(&intent, &engines[0])
                .await
                .is_err()
        );
        assert!(
            !providers[2]
                .snapshot()
                .assignment_for("initial-wire", Partition::new(0), None)
                .unwrap()
                .is_followed_by("c")
        );
        assert!(
            brokers[0]
                .begin_replication_progress_session("initial-wire", Partition::new(0), None, "c", 1)
                .is_none()
        );
        engines[2]
            .become_queue_follower_with_epoch("initial-wire", 0, None, 1)
            .await
            .unwrap();
        assert!(
            providers[2]
                .admit_queue_learner(&intent, &engines[2], 2, 2)
                .await
                .is_err()
        );
        let (addr, learner_task, dir, _) = start_protocol_listener_for_broker(
            ConnectionSettings::new(Some(60)),
            brokers[0].clone(),
            dirs[0].take().unwrap(),
            Some(node_auth()),
        )
        .await;
        dirs[0] = Some(dir);
        let auth =
            ProtocolOwnerPeerResolverConfig::new(HashMap::new()).with_auth("@node", "secret");
        let learner_peer = connect_protocol_owner_peer(
            addr.to_string(),
            auth.auth.as_ref(),
            None,
            "learner-test",
            "1",
        )
        .await
        .unwrap()
        .with_reporter("c")
        .with_history_session(
            intent
                .session(&providers[2].consensus_node().committed_snapshot())
                .unwrap(),
        );
        let cut = learner_peer
            .export_owner_state_checkpoint("initial-wire", Partition::new(0), None)
            .await
            .unwrap();
        let caught = brokers[2]
            .catch_up_replication_follower_from_owner_with_checkpoint(
                &learner_peer,
                "initial-wire",
                Partition::new(0),
                None,
                ReplicationResourceKind::Queue,
                BrokerReplicationCatchUpOptions::default(),
            )
            .await
            .unwrap();
        assert!(matches!(caught, BrokerReplicationCatchUp::CaughtUp(_)));
        let mut waiting = publisher
            .publish(
                b"during-learner-admission".to_vec(),
                unix_millis(),
                unix_millis(),
                None,
                Default::default(),
                None,
            )
            .await
            .unwrap();
        // Even forged optimistic learner progress must never satisfy a confirm.
        let _ = learner_peer
            .read_owner_replication_records_fenced(
                "initial-wire",
                Partition::new(0),
                None,
                3,
                3,
                8,
                8,
                65536,
                0,
                Some(1),
            )
            .await
            .unwrap();
        assert!(matches!(
            waiting.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(
            providers[0]
                .snapshot()
                .assignment_for("initial-wire", Partition::new(0), None)
                .unwrap(),
            &old_assignment
        );
        assert!(providers[0].pending_recoveries().unwrap().is_empty());
        retry_metadata(|| {
            providers[2].admit_queue_learner(
                &intent,
                &engines[2],
                cut.message_next_offset,
                cut.event_next_offset,
            )
        })
        .await;
        tokio::time::timeout(Duration::from_secs(10), async {
            while providers.iter().any(|p| {
                !p.snapshot()
                    .assignment_for("initial-wire", Partition::new(0), None)
                    .unwrap()
                    .is_followed_by("c")
            }) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let after = providers[0].snapshot();
        let joined = after
            .assignment_for("initial-wire", Partition::new(0), None)
            .unwrap();
        assert!(joined.preserves_replication_contract(&old_assignment));
        assert_eq!(joined.epoch, 1);
        assert_eq!(joined.owner, "a");
        assert_eq!(
            joined.history.as_ref().unwrap().activation,
            history.activation
        );
        assert_eq!(
            joined.history.as_ref().unwrap().replicas["c"].storage,
            prepared.storage_instance
        );
        for i in 0..3 {
            for result in brokers[i]
                .apply_assignment_snapshot_transitions(
                    ["a", "b", "c"][i],
                    &before,
                    &providers[i].snapshot(),
                )
                .await
            {
                result.unwrap();
            }
        }
        assert!(
            learner_peer
                .read_owner_replication_records_fenced(
                    "initial-wire",
                    Partition::new(0),
                    None,
                    3,
                    3,
                    8,
                    8,
                    65536,
                    0,
                    Some(1)
                )
                .await
                .is_err()
        );
        drop(learner_peer);
        learner_task.await.unwrap().unwrap();
        let (addr, new_task, dir, _) = start_protocol_listener_for_broker(
            ConnectionSettings::new(Some(60)),
            brokers[0].clone(),
            dirs[0].take().unwrap(),
            Some(node_auth()),
        )
        .await;
        dirs[0] = Some(dir);
        let new_peer = connect_protocol_owner_peer(
            addr.to_string(),
            auth.auth.as_ref(),
            None,
            "joined-test",
            "1",
        )
        .await
        .unwrap()
        .with_reporter("c")
        .with_history_session(
            joined
                .history
                .as_ref()
                .unwrap()
                .session("initial-wire", Partition::new(0), None, false, "c", "a")
                .unwrap(),
        );
        let from = engines[2]
            .verify_queue_learner_caught_up("initial-wire", 0, None, 1, 0, 0)
            .await
            .unwrap();
        let caught = brokers[2]
            .catch_up_replication_follower_from_owner_with_checkpoint(
                &new_peer,
                "initial-wire",
                Partition::new(0),
                None,
                ReplicationResourceKind::Queue,
                BrokerReplicationCatchUpOptions {
                    message_from: from.message_next,
                    event_from: from.event_next,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(caught, BrokerReplicationCatchUp::CaughtUp(_)));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), waiting)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            2
        );
        drop(new_peer);
        new_task.await.unwrap().unwrap();
        // Joined membership participates in the next recovery intersection proof.
        let pending = persist_pending(&providers, &resource, history.activation, "b").await;
        synced(&providers, pending.requested_generation).await;
        assert_eq!(pending.required_old_witnesses, 2);
        let seal = brokers[2]
            .seal_replica_for_recovery(pending.seal_command().unwrap())
            .await
            .unwrap();
        let mut witnesses =
            fibril_coordination_ganglion::recovery_witnesses::RecoveryWitnessSet::new(
                &providers[0].consensus_node().committed_snapshot(),
                &pending,
            )
            .unwrap();
        witnesses
            .record(
                &providers[0].consensus_node().committed_snapshot(),
                "c",
                seal,
            )
            .unwrap();
        assert!(
            providers[2]
                .admit_queue_learner(&intent, &engines[2], 0, 0)
                .await
                .is_err()
        );
        drop(peer);
        owner_task.await.unwrap().unwrap();
        for broker in &brokers {
            broker.shutdown().await;
        }
        for provider in &providers {
            provider.consensus_node().shutdown().await.unwrap();
        }
        for server in servers {
            server.shutdown();
        }
        return;
    }
    // Fencing closes the live session but retains the accepted certificate.
    // The one surviving prepared follower is enough: every previous majority
    // confirm needed both prepared replicas, and c was never eligible to vote.
    let pending = persist_pending(&providers, &resource, history.activation, if handoff { "c" } else { "b" }).await;
    synced(&providers, pending.requested_generation).await;
    let command = pending.seal_command().unwrap();
    let (addr, task, dir, _) = start_protocol_listener_for_broker(
        ConnectionSettings::new(Some(60)),
        brokers[1].clone(),
        dirs[1].take().unwrap(),
        Some(node_auth()),
    )
    .await;
    dirs[1] = Some(dir);
    let config =
        ProtocolOwnerPeerResolverConfig::new(HashMap::from([("b".into(), addr.to_string())]))
            .with_auth("@node", "secret");
    let sealed = fibril_protocol::v1::replication::request_recovery_seal(
        &config,
        "b",
        &command,
        Duration::from_secs(10),
    )
    .await
    .unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(sealed.seal.history.version, 2);
    assert_eq!(
        sealed.seal.history.storage_history.as_ref().unwrap(),
        &quorum.reports["b"].storage
    );
    assert_eq!((sealed.seal.message_next, sealed.seal.event_next), (2, 2));
    let mut witnesses = fibril_coordination_ganglion::recovery_witnesses::RecoveryWitnessSet::new(
        &providers[0].consensus_node().committed_snapshot(),
        &pending,
    )
    .unwrap();
    witnesses
        .record(
            &providers[0].consensus_node().committed_snapshot(),
            "b",
            sealed.clone(),
        )
        .unwrap();
    assert!(
        witnesses
            .accepted_history(&providers[0].consensus_node().committed_snapshot())
            .unwrap()
            .is_some()
    );
    assert_eq!(witnesses.progress(&providers[0].consensus_node().committed_snapshot()).unwrap(),
        fibril_coordination_ganglion::recovery_witnesses::SealCollectionProgress::AwaitingHistoryValidation {received:1,required:1});
    // The same authenticated sealed-read path reconstructs an installable state
    // artifact. One source contributes one witness even though the bounded pair
    // inspector verifies it against itself internally.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = ProtocolOwnerPeerResolverConfig::new(HashMap::from([
        ("b".into(), listener.local_addr().unwrap().to_string()),
    ])).with_auth("@node", "secret");
    let serving = brokers[1].clone();
    let stop_reads = tokio_util::sync::CancellationToken::new();
    let cancelled = stop_reads.clone();
    let reads = tokio::spawn(async move {
        loop {
            let (socket, peer) = tokio::select! {
                _ = cancelled.cancelled() => break,
                accepted = listener.accept() => accepted.unwrap(),
            };
            let stats = ConnectionStats::new();
            let conn_id = stats.add_connection(peer, Instant::now(), false);
            handle_connection(socket, Some(peer), serving.clone(), TcpStats::new(10),
                stats, conn_id, Some(node_auth()), None, ConnectionSettings::new(Some(60)),
                None, None, None).await.unwrap();
        }
    });
    let artifact = fibril_protocol::v1::recovery_inspection::inspect_recovery_source_artifact(
        &config, &command, &sealed, Default::default(), Default::default(),
        1024 * 1024, 1024 * 1024, Duration::from_secs(10),
    ).await.unwrap();
    let chosen = witnesses.select_queue_source(
        &providers[0].consensus_node().committed_snapshot(),
        &BTreeMap::from([("b".into(), artifact.clone())]), &[],
    ).unwrap();
    assert_eq!(chosen.source_node(), "b");
    assert_eq!((chosen.event_next(), chosen.message_next()), (2, 2));
    assert!(providers[0].persist_queue_recovery_plan(&pending, &witnesses, &chosen).await.is_err());
    let original_candidate = if handoff { 2 } else { 1 };
    let plan = retry_metadata(|| providers[original_candidate].persist_queue_recovery_plan(&pending, &witnesses, &chosen)).await;
    // Forwarded consensus completion can precede this provider's local watch.
    // Wait for the actual intent rather than sampling a possibly older generation.
    tokio::time::timeout(Duration::from_secs(10), async {
        while providers.iter().any(|provider| provider.queue_recovery_plan(&pending).unwrap() != Some(plan.clone())) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    assert_eq!(plan.source_node(), "b");
    assert_eq!((plan.event_next(), plan.message_next()), (2, 2));
    assert_ne!(plan.binding(), &decision.binding);
    assert_eq!(providers[2].queue_recovery_plan(&pending).unwrap(), Some(plan.clone()));
    // A lost response retries the exact intent, including its generated IDs.
    assert_eq!(retry_metadata(|| providers[original_candidate].persist_queue_recovery_plan(&pending, &witnesses, &chosen)).await, plan);
    assert_eq!(providers[0].pending_recoveries().unwrap(), vec![pending.clone()]);
    // A proposed replica receives the selected state and native-log payloads in
    // non-serving staging. The old source stays sealed and readable throughout.
    let stage = retry_metadata(|| providers[2].open_local_queue_recovery_stage(
        &plan, &engines[2], &artifact, Default::default())).await;
    while stage.next_offset().await < plan.message_next() {
        let reply = fibril_protocol::v1::replication::request_recovery_read(
            &config, &command, &sealed,
            &fibril_broker::recovery::RecoveryReadRequest {
                seal: sealed.seal.request.clone(), history_id: sealed.seal.history.id,
                source: fibril_broker::recovery::RecoveryReadSource::Messages,
                from: stage.next_offset().await, max_records:1, max_bytes:65536,
            }, Duration::from_secs(10),
        ).await.unwrap();
        stage.append(fibril_broker::recovery::RecoveryReadPage {
            history_id: reply.history_id,
            source: fibril_broker::recovery::RecoveryReadSource::Messages,
            from: reply.from, next: reply.next, end: reply.end,
            snapshot_bytes: reply.snapshot_bytes,
            records: reply.records.into_iter().map(|r| fibril_broker::recovery::RecoveryRecord {
                offset:r.offset, flags:r.flags, headers:r.headers, payload:r.payload,
            }).collect(),
        }).await.unwrap();
    }
    let staged = stage.finish().await.unwrap();
    assert_eq!(staged.plan, plan.digest().unwrap());
    assert_eq!((staged.event_next, staged.message_next), (2,2));
    assert!(providers[2].admit_local_initial_history(&decision, &engines[2]).await.is_err());
    drop(stage);
    stop_reads.cancel();
    reads.await.unwrap();
    let resumed = retry_metadata(|| providers[2].resume_local_queue_recovery_stage(
        &plan, &engines[2], Default::default())).await;
    assert_eq!(resumed.finish().await.unwrap(), staged);
    drop(resumed);
    use fibril_broker::recovery_transfer::{QueueRecoveryRequest,QueueRecoveryCommand,QueueRecoveryOperation as RecoveryOp,QueueRecoveryReply as RecoveryReply};
    let transfer_command=|id:&str|QueueRecoveryCommand {replica_id:id.into(),topic:"initial-wire".into(),partition:0,group:None,plan:plan.digest().unwrap()};
    let request=|id:&str,operation|QueueRecoveryRequest {command:transfer_command(id),operation};
    let (result,dir)=transfer(brokers[2].clone(),dirs[2].take().unwrap(),&request("c",RecoveryOp::Install)).await;
    dirs[2]=Some(dir);
    let RecoveryReply::Installed(installed_c)=result.unwrap() else {panic!("expected installed receipt")};
    assert!(engines[2].ensure_queue_owner_epoch("initial-wire",0,None,Some(2)).await.is_err());
    assert!(providers[1].activate_queue_recovery(&plan,&engines[1]).await.is_err());
    let (result,dir)=transfer(brokers[2].clone(),dirs[2].take().unwrap(),&request("c",RecoveryOp::Snapshot)).await;
    dirs[2]=Some(dir);
    let RecoveryReply::Snapshot(source_snapshot)=result.unwrap() else {panic!("expected completed snapshot")};
    let (result,dir)=transfer(brokers[1].clone(),dirs[1].take().unwrap(),&request("b",RecoveryOp::Begin {snapshot:source_snapshot})).await;
    dirs[1]=Some(dir); result.unwrap();
    for from in 0..2 {
        let (result,dir)=transfer(brokers[2].clone(),dirs[2].take().unwrap(),&request("c",RecoveryOp::Read {from,max_records:1,max_bytes:65536})).await;
        dirs[2]=Some(dir);
        let RecoveryReply::Page(page)=result.unwrap() else {panic!("expected completed page")};
        if from==0 {
            let mut invalid=page.clone(); invalid.records[0].offset+=1;
            let (result,dir)=transfer(brokers[1].clone(),dirs[1].take().unwrap(),&request("b",RecoveryOp::Append {page:invalid})).await;
            dirs[1]=Some(dir); assert!(result.is_err());
        }
        let (result,dir)=transfer(brokers[1].clone(),dirs[1].take().unwrap(),&request("b",RecoveryOp::Append {page})).await;
        dirs[1]=Some(dir); result.unwrap();
    }
    let (result,dir)=transfer(brokers[1].clone(),dirs[1].take().unwrap(),&request("b",RecoveryOp::Finish)).await;
    dirs[1]=Some(dir); result.unwrap();
    let (result,dir)=transfer(brokers[1].clone(),dirs[1].take().unwrap(),&request("b",RecoveryOp::Install)).await;
    dirs[1]=Some(dir);
    let RecoveryReply::Installed(installed_b)=result.unwrap() else {panic!("expected installed receipt")};
    tokio::time::timeout(Duration::from_secs(10),async {
        let expected = [serde_json::to_string(&installed_b).unwrap(),serde_json::to_string(&installed_c).unwrap()];
        while providers.iter().any(|p| expected.iter().any(|r| !p.consensus_node().committed_snapshot().attributes.values().any(|v|v==r))) {tokio::time::sleep(Duration::from_millis(5)).await;}
    }).await.unwrap();
    if handoff {
        // Both targets are fully installed. The old candidate could activate at
        // this point; removing it from liveness must fence that exact attempt.
        let live = HashMap::from([(
            "b".into(),
            NodeInfo {
                node_id: "b".into(),
                broker_addr: "127.0.0.1:1".into(),
                admin_addr: None,
            },
        )]);
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                for provider in &providers {
                    let _ = provider
                        .control_iteration(
                            &fibril_broker::coordination::DeterministicPartitionPlacement,
                            &[QueueIdentity::new("initial-wire", Partition::new(0), None)],
                            &fibril_broker::coordination::DeterministicStreamPlacement,
                            &[],
                            2,
                            2,
                            ReplicationDurabilityPolicy::MajorityDurable,
                            &live,
                            8,
                        )
                        .await;
                }
                if providers.iter().all(|p| {
                    p.queue_recovery_candidate(&pending)
                        .is_ok_and(|a| a.owner == "b")
                }) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(
            providers[1].queue_recovery_plan(&pending).unwrap(),
            Some(plan.clone())
        );
        assert_eq!(
            providers[1].pending_recoveries().unwrap(),
            vec![pending.clone()]
        );
        assert!(
            providers[2]
                .persist_queue_recovery_plan(&pending, &witnesses, &chosen)
                .await
                .is_err()
        );
        assert!(
            providers[2]
                .activate_queue_recovery(&plan, &engines[2])
                .await
                .is_err()
        );
        let (result, dir) = transfer(
            brokers[2].clone(),
            dirs[2].take().unwrap(),
            &request("c", RecoveryOp::Finish),
        )
        .await;
        dirs[2] = Some(dir);
        let RecoveryReply::Complete(completed) = result.unwrap() else {
            panic!("expected completed stage")
        };
        assert_eq!(completed, staged);
    }
    let recovered = retry_metadata(|| providers[1].activate_queue_recovery(&plan,&engines[1])).await;
    tokio::time::timeout(Duration::from_secs(10),async {
        while providers.iter().any(|p|p.queue_recovery_activation(&plan).unwrap()!=Some(recovered.clone())) {tokio::time::sleep(Duration::from_millis(5)).await;}
    }).await.unwrap();
    // A lost activation reply is resolved by the exact certificate, with no
    // reinstall or reset. Ordinary roles remain closed until local admission.
    assert_eq!(retry_metadata(||providers[1].activate_queue_recovery(&plan,&engines[1])).await,recovered);
    for index in [1,2] {
        let id=if index==1 {"b"} else {"c"};
        let (result,dir)=transfer(brokers[index].clone(),dirs[index].take().unwrap(),&request(id,RecoveryOp::Admit)).await;
        dirs[index]=Some(dir); assert!(matches!(result.unwrap(),RecoveryReply::Admitted(_)));
    }
    assert!(providers[0].admit_local_queue_recovery(&recovered,&engines[0]).await.is_err());
    let (result,dir)=transfer(brokers[2].clone(),dirs[2].take().unwrap(),&request("c",RecoveryOp::Install)).await;
    dirs[2]=Some(dir); assert!(result.is_err());
    for index in [1,2] {
        let id = if index==1 {"b"} else {"c"};
        for result in brokers[index].apply_assignment_snapshot_transitions(id,&CoordinationSnapshot::default(),&providers[index].snapshot()).await {result.unwrap();}
    }
    let recovered_assignment = providers[1].snapshot().assignment_for("initial-wire",Partition::new(0),None).unwrap().clone();
    let recovered_history = recovered_assignment.history.as_ref().unwrap();
    assert_eq!(recovered_history.activation,recovered.digest().unwrap());
    assert_eq!(recovered_assignment.owner,"b");
    assert!(recovered_assignment.is_followed_by("c"));
    assert!(!recovered_assignment.is_followed_by("a"));
    let recovered_session = recovered_history.session("initial-wire",Partition::new(0),None,false,"c","b").unwrap();
    brokers[1].authorize_history_replication(&recovered_session,true).unwrap();
    assert!(brokers[1].authorize_history_replication(&session,true).is_err());
    // A new majority confirmation must use the recovered process/storage
    // identities and retain the selected offset continuation.
    let (mut recovered_connection,recovered_task,dir,_) = open_node_connection_for_broker(ConnectionSettings::new(Some(60)),brokers[1].clone(),dirs[1].take().unwrap()).await;
    dirs[1]=Some(dir);node_handshake(&mut recovered_connection).await;
    let recovered_peer=ProtocolOwnerReplicationPeer::new(recovered_connection).with_reporter("c").with_history_session(recovered_session.clone());
    let publisher_b=brokers[1].get_publisher("initial-wire",Partition::new(0),&None).await.unwrap();
    let mut new_confirm=publisher_b.publish(b"after recovered activation".to_vec(),unix_millis(),unix_millis(),None,Default::default(),None).await.unwrap();
    let records=tokio::time::timeout(Duration::from_secs(10),async {
        loop {
            let records=recovered_peer.read_owner_replication_records_fenced("initial-wire",Partition::new(0),None,plan.message_next(),plan.event_next(),8,8,65536,20,Some(2)).await.unwrap();
            if matches!(&records.messages,OwnerReplicationRead::Batch(batch) if !batch.records.is_empty()) && matches!(&records.events,OwnerReplicationRead::Batch(batch) if !batch.records.is_empty()) {break records}
        }
    }).await.unwrap();
    assert!(matches!(new_confirm.try_recv(),Err(tokio::sync::oneshot::error::TryRecvError::Empty)));
    let message_next=match &records.messages {OwnerReplicationRead::Batch(batch)=>batch.records.last().unwrap().0+1,_=>unreachable!()};
    let event_next=match &records.events {OwnerReplicationRead::Batch(batch)=>batch.records.last().unwrap().0+1,_=>unreachable!()};
    brokers[2].apply_follower_replication_records("initial-wire",Partition::new(0),None,ReplicationResourceKind::Queue,records).await.unwrap();
    recovered_peer.read_owner_replication_records_fenced("initial-wire",Partition::new(0),None,message_next,event_next,8,8,65536,0,Some(2)).await.unwrap();
    assert_eq!(tokio::time::timeout(Duration::from_secs(10),new_confirm).await.unwrap().unwrap().unwrap(),2);
    drop(recovered_peer);recovered_task.await.unwrap().unwrap();
    assert_eq!(retry_metadata(||providers[1].activate_queue_recovery(&plan,&engines[1])).await,recovered);
    retry_metadata(||providers[1].admit_local_queue_recovery(&recovered,&engines[1])).await;
    assert_eq!(engines[1].queue_durable_frontiers("initial-wire",0,None).await.unwrap().message_next,3);
    // Seal the recovered generation under a second transition. Its authority
    // comes from the recovered quorum, rather than the old initial assignment.
    let again = persist_pending(&providers,&resource,recovered.digest().unwrap(),"b").await;
    synced(&providers,again.requested_generation).await;
    let again_sealed = brokers[1].seal_replica_for_recovery(again.seal_command().unwrap()).await.unwrap();
    assert_eq!(again_sealed.seal.history.storage_history.as_ref().unwrap().binding,*plan.binding());
    let mut again_witnesses = fibril_coordination_ganglion::recovery_witnesses::RecoveryWitnessSet::new(&providers[1].consensus_node().committed_snapshot(),&again).unwrap();
    again_witnesses.record(&providers[1].consensus_node().committed_snapshot(),"b",again_sealed).unwrap();
    assert_eq!(again_witnesses.accepted_history(&providers[1].consensus_node().committed_snapshot()).unwrap().unwrap().activation,recovered.digest().unwrap());
    assert!(providers[1].admit_local_queue_recovery(&recovered,&engines[1]).await.is_err());

    // Replacing storage under the same metadata/provider instance cannot reuse
    // the durable preparation receipt as permission for a fresh storage process.
    let follower_root = dirs[1].as_ref().unwrap().root.clone();
    brokers[1].shutdown().await;
    let reopened = StromaEngine::open(
        &follower_root,
        StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
        SnapshotConfig::default(),
    )
    .await
    .unwrap();
    let replacement = Broker::new_with_ownership(
        reopened.clone(),
        BrokerConfig::default(),
        None,
        providers[1].clone(),
    );
    let (failed, dir) = call(
        replacement.clone(),
        dirs[1].take().unwrap(),
        &decision.prepare_command("b").unwrap(),
    )
    .await;
    dirs[1] = Some(dir);
    assert!(failed.is_err());
    assert!(
        providers[1]
            .admit_local_initial_history(&decision, &reopened)
            .await
            .is_err()
    );
    let reverse = history
        .session("initial-wire", Partition::new(0), None, false, "a", "b")
        .unwrap();
    assert!(
        replacement
            .authorize_history_replication(&reverse, false)
            .is_err()
    );
    replacement.shutdown().await;
    // Assignment changes invalidate the old command through a fresh consensus
    // check, including on a replica that had not prepared any storage yet.
    let generation = set_assignment(&providers, &resource, 2).await;
    synced(&providers, generation).await;
    assert!(providers[2].open_local_queue_recovery_stage(
        &plan, &engines[2], &artifact, Default::default()).await.is_err());
    assert!(providers[2].resume_local_queue_recovery_stage(
        &plan, &engines[2], Default::default()).await.is_err());
    assert!(
        peer.read_owner_replication_records_fenced(
            "initial-wire",
            Partition::new(0),
            None,
            1,
            1,
            8,
            8,
            65536,
            0,
            Some(1)
        )
        .await
        .is_err()
    );
    drop(peer);
    owner_task.await.unwrap().unwrap();
    let (stale, dir) = call(
        brokers[2].clone(),
        dirs[2].take().unwrap(),
        &decision.prepare_command("c").unwrap(),
    )
    .await;
    dirs[2] = Some(dir);
    assert!(stale.is_err());
    assert_eq!(engines[2].storage_history_binding("initial-wire",0,None).unwrap().as_ref(),Some(plan.binding()));
    for broker in &brokers {
        broker.shutdown().await;
    }
    for provider in &providers {
        provider.consensus_node().shutdown().await.unwrap();
    }
    for server in servers {
        server.shutdown();
    }
}

// These protocol fixtures intentionally exercise legacy unbound replication and
// sealing. Fresh-queue enrollment is covered by the server worker lifecycle test.
async fn register_legacy_test_queue(
    provider: &fibril_coordination_ganglion::GanglionCoordination,
    queue: &fibril_broker::coordination::QueueIdentity,
) {
    let mut snapshot = provider.consensus_node().committed_snapshot();
    let generation = snapshot.generation;
    snapshot.resources.insert(ganglion_core::ResourceIdentity::new(
        "fibril/queue", queue.topic.clone(), u64::from(queue.partition.id()), queue.group.clone(),
    ));
    snapshot.generation += 1;
    provider.consensus_node().write_snapshot_guarded(generation, snapshot).await.unwrap();
}
