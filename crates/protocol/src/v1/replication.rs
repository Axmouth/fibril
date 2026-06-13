use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use anyhow::{Context, bail};
use fibril_broker::{
    Offset, Partition,
    broker::{
        BrokerError, BrokerOwnerReplicationPeer, BrokerOwnerReplicationPeerResolver,
        BrokerOwnerReplicationRecords,
    },
    coordination::{Coordination, NodeInfo, PartitionAssignment},
    queue_engine::{
        Message, OwnerReplicationBatch, OwnerReplicationRead, OwnerStateCheckpoint, StromaEvent,
    },
};
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::Mutex};

use crate::v1::{
    Auth, ERR_NOT_OWNER, ErrorMsg, Hello, HelloOk, Op, PROTOCOL_V1, ReplicationApply,
    ReplicationApplyOk, ReplicationCheckpointExport, ReplicationCheckpointExportOk,
    ReplicationCheckpointRequired, ReplicationEventApplyBatch, ReplicationEventRead,
    ReplicationMessageApplyBatch, ReplicationMessageRead, ReplicationRead, ReplicationReadOk,
    frame::ProtoCodec,
    helper::{Conn, try_decode, try_encode},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolReplicationCatchUpOptions {
    pub message_from: u64,
    pub event_from: u64,
    pub max_messages_per_read: u32,
    pub max_events_per_read: u32,
    pub max_iterations: usize,
    pub request_id_start: u64,
}

impl Default for ProtocolReplicationCatchUpOptions {
    fn default() -> Self {
        Self {
            message_from: 0,
            event_from: 0,
            max_messages_per_read: 256,
            max_events_per_read: 256,
            max_iterations: 1024,
            request_id_start: 10_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProtocolReplicationCatchUpProgress {
    pub iterations: usize,
    pub applied_message_records: usize,
    pub applied_event_records: usize,
    pub message_next_offset: u64,
    pub event_next_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolReplicationCatchUp {
    CaughtUp(ProtocolReplicationCatchUpProgress),
    CheckpointRequired {
        progress: ProtocolReplicationCatchUpProgress,
        messages: Option<ReplicationCheckpointRequired>,
        events: Option<ReplicationCheckpointRequired>,
    },
    IterationLimit {
        progress: ProtocolReplicationCatchUpProgress,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolOwnerPeerAuth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub struct ProtocolOwnerPeerResolverConfig {
    pub nodes: HashMap<String, SocketAddr>,
    pub auth: Option<ProtocolOwnerPeerAuth>,
    pub client_name: String,
    pub client_version: String,
    /// This follower's node id, stamped into replication reads so the owner
    /// can track durable progress per follower (publish-confirm policies).
    pub reporter_node_id: Option<String>,
}

impl ProtocolOwnerPeerResolverConfig {
    pub fn new(nodes: HashMap<String, SocketAddr>) -> Self {
        Self {
            nodes,
            auth: None,
            client_name: "fibril-replication".into(),
            client_version: env!("CARGO_PKG_VERSION").into(),
            reporter_node_id: None,
        }
    }

    /// Stamp replication reads with this follower's identity so the owner
    /// tracks its durable progress (publish-confirm durability policies).
    pub fn with_reporter(mut self, node_id: impl Into<String>) -> Self {
        self.reporter_node_id = Some(node_id.into());
        self
    }

    pub fn from_nodes(nodes: impl IntoIterator<Item = NodeInfo>) -> Self {
        Self::new(
            nodes
                .into_iter()
                .map(|node| (node.node_id, node.broker_addr))
                .collect(),
        )
    }

    pub fn with_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.auth = Some(ProtocolOwnerPeerAuth {
            username: username.into(),
            password: password.into(),
        });
        self
    }
}

/// Static owner-peer resolver for early replication wiring and tests.
///
/// It keeps one lazy protocol peer per owner id. Transport failures clear the
/// peer's current connection, then the next request reconnects through the same
/// cached peer object. Topology refresh/invalidation belongs above this static
/// resolver.
pub struct StaticProtocolOwnerPeerResolver {
    cfg: ProtocolOwnerPeerResolverConfig,
    peers: Mutex<HashMap<String, Arc<dyn BrokerOwnerReplicationPeer>>>,
}

impl StaticProtocolOwnerPeerResolver {
    pub fn new(nodes: HashMap<String, SocketAddr>) -> Self {
        Self {
            cfg: ProtocolOwnerPeerResolverConfig::new(nodes),
            peers: Mutex::new(HashMap::new()),
        }
    }

    pub fn with_config(cfg: ProtocolOwnerPeerResolverConfig) -> Self {
        Self {
            cfg,
            peers: Mutex::new(HashMap::new()),
        }
    }
}

impl BrokerOwnerReplicationPeerResolver for StaticProtocolOwnerPeerResolver {
    fn resolve_owner_peer<'a>(
        &'a self,
        assignment: &'a PartitionAssignment,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<Arc<dyn BrokerOwnerReplicationPeer>>, BrokerError>,
    > {
        Box::pin(async move {
            let Some(addr) = self.cfg.nodes.get(&assignment.owner).copied() else {
                return Ok(None);
            };

            let mut peers = self.peers.lock().await;
            if let Some(peer) = peers.get(&assignment.owner) {
                return Ok(Some(peer.clone()));
            }

            let mut built = ProtocolOwnerReplicationPeer::new_reconnecting(
                addr,
                self.cfg.auth.clone(),
                self.cfg.client_name.clone(),
                self.cfg.client_version.clone(),
            );
            if let Some(reporter) = &self.cfg.reporter_node_id {
                built = built.with_reporter(reporter.clone());
            }
            let peer: Arc<dyn BrokerOwnerReplicationPeer> = Arc::new(built);
            peers.insert(assignment.owner.clone(), peer.clone());
            Ok(Some(peer))
        })
    }
}

struct CachedProtocolOwnerPeer {
    addr: SocketAddr,
    peer: Arc<dyn BrokerOwnerReplicationPeer>,
}

/// Coordination-backed owner-peer resolver for replication workers.
///
/// Each resolve uses the current coordination snapshot, so owner address changes
/// are picked up without restarting the worker. The peer cache is still small
/// and conservative: one cached peer per owner id, invalidated when that owner
/// disappears or its broker address changes.
pub struct CoordinationProtocolOwnerPeerResolver {
    coordination: Arc<dyn Coordination>,
    cfg: ProtocolOwnerPeerResolverConfig,
    peers: Mutex<HashMap<String, CachedProtocolOwnerPeer>>,
}

impl CoordinationProtocolOwnerPeerResolver {
    pub fn new(coordination: Arc<dyn Coordination>) -> Self {
        // Report as the local node by default: the owner needs the follower's
        // identity to track durable progress for publish-confirm policies.
        let reporter = coordination.node_id().to_string();
        Self::with_config(
            coordination,
            ProtocolOwnerPeerResolverConfig::new(HashMap::new()).with_reporter(reporter),
        )
    }

    pub fn with_config(
        coordination: Arc<dyn Coordination>,
        cfg: ProtocolOwnerPeerResolverConfig,
    ) -> Self {
        Self {
            coordination,
            cfg,
            peers: Mutex::new(HashMap::new()),
        }
    }
}

impl BrokerOwnerReplicationPeerResolver for CoordinationProtocolOwnerPeerResolver {
    fn resolve_owner_peer<'a>(
        &'a self,
        assignment: &'a PartitionAssignment,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<Arc<dyn BrokerOwnerReplicationPeer>>, BrokerError>,
    > {
        Box::pin(async move {
            let snapshot = self.coordination.snapshot();
            let Some(node) = snapshot.nodes.get(&assignment.owner) else {
                self.peers.lock().await.remove(&assignment.owner);
                return Ok(None);
            };
            let addr = node.broker_addr;

            let mut peers = self.peers.lock().await;
            if let Some(cached) = peers.get(&assignment.owner) {
                if cached.addr == addr {
                    return Ok(Some(cached.peer.clone()));
                }
            }

            let mut built = ProtocolOwnerReplicationPeer::new_reconnecting(
                addr,
                self.cfg.auth.clone(),
                self.cfg.client_name.clone(),
                self.cfg.client_version.clone(),
            );
            if let Some(reporter) = &self.cfg.reporter_node_id {
                built = built.with_reporter(reporter.clone());
            }
            let peer: Arc<dyn BrokerOwnerReplicationPeer> = Arc::new(built);
            peers.insert(
                assignment.owner.clone(),
                CachedProtocolOwnerPeer {
                    addr,
                    peer: peer.clone(),
                },
            );
            Ok(Some(peer))
        })
    }
}

async fn open_protocol_owner_conn(
    addr: SocketAddr,
    auth: Option<&ProtocolOwnerPeerAuth>,
    client_name: &str,
    client_version: &str,
) -> Result<Conn, BrokerError> {
    let stream = TcpStream::connect(addr)
        .await
        .map_err(|err| BrokerError::Unknown(format!("owner peer connect failed: {err}")))?;
    let mut conn = tokio_util::codec::Framed::new(stream, ProtoCodec);

    let request_id = 1;
    conn.send(
        try_encode(
            Op::Hello,
            request_id,
            &Hello {
                client_name: client_name.to_string(),
                client_version: client_version.to_string(),
                protocol_version: PROTOCOL_V1,
                resume: None,
            },
        )
        .map_err(protocol_error)?,
    )
    .await
    .map_err(|err| BrokerError::Unknown(format!("owner peer hello send failed: {err}")))?;

    let _: HelloOk = recv_response(&mut conn, request_id, Op::HelloOk)
        .await
        .map_err(|err| BrokerError::Unknown(err.to_string()))?;

    if let Some(auth) = auth {
        let request_id = 2;
        conn.send(
            try_encode(
                Op::Auth,
                request_id,
                &Auth {
                    username: auth.username.clone(),
                    password: auth.password.clone(),
                },
            )
            .map_err(protocol_error)?,
        )
        .await
        .map_err(|err| BrokerError::Unknown(format!("owner peer auth send failed: {err}")))?;

        let _: () = recv_response(&mut conn, request_id, Op::AuthOk)
            .await
            .map_err(|err| BrokerError::Unknown(err.to_string()))?;
    }

    Ok(conn)
}

pub async fn connect_protocol_owner_peer(
    addr: SocketAddr,
    auth: Option<&ProtocolOwnerPeerAuth>,
    client_name: &str,
    client_version: &str,
) -> Result<ProtocolOwnerReplicationPeer, BrokerError> {
    Ok(ProtocolOwnerReplicationPeer::new(
        open_protocol_owner_conn(addr, auth, client_name, client_version).await?,
    ))
}

#[derive(Debug, Clone)]
struct ProtocolOwnerPeerConnectConfig {
    addr: SocketAddr,
    auth: Option<ProtocolOwnerPeerAuth>,
    client_name: String,
    client_version: String,
}

impl ProtocolOwnerPeerConnectConfig {
    async fn open(&self) -> Result<Conn, BrokerError> {
        open_protocol_owner_conn(
            self.addr,
            self.auth.as_ref(),
            &self.client_name,
            &self.client_version,
        )
        .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolReplicationRequestError {
    #[error("connection closed while waiting for response")]
    ConnectionClosed,
    #[error("failed to read response frame: {0}")]
    Read(String),
    #[error("failed to encode heartbeat response: {0}")]
    HeartbeatEncode(String),
    #[error("failed to send heartbeat response: {0}")]
    HeartbeatSend(String),
    #[error("unexpected response request id {actual}; expected {expected}")]
    UnexpectedRequestId { actual: u64, expected: u64 },
    #[error("unexpected response opcode {actual}; expected {expected}")]
    UnexpectedOpcode { actual: u16, expected: u16 },
    #[error("replication request failed: {code} {message}")]
    Protocol { code: u16, message: String },
    #[error("failed to decode response frame: {0}")]
    Decode(String),
}

impl ProtocolReplicationRequestError {
    fn invalidates_connection(&self) -> bool {
        !matches!(self, Self::Protocol { .. } | Self::HeartbeatEncode(_))
    }
}

/// Protocol-backed owner replication peer.
///
/// This is the transport adapter for the broker's follower worker boundary. It
/// owns one already-handshaken protocol connection to an owner and serializes
/// requests on that connection.
pub struct ProtocolOwnerReplicationPeer {
    conn: Mutex<Option<Conn>>,
    next_request_id: AtomicU64,
    reconnect: Option<ProtocolOwnerPeerConnectConfig>,
    /// Stamped into replication reads for owner-side progress tracking.
    reporter_node_id: Option<String>,
}

impl ProtocolOwnerReplicationPeer {
    pub fn new(conn: Conn) -> Self {
        Self {
            conn: Mutex::new(Some(conn)),
            next_request_id: AtomicU64::new(20_000),
            reconnect: None,
            reporter_node_id: None,
        }
    }

    /// Stamp replication reads with this follower's identity.
    pub fn with_reporter(mut self, node_id: impl Into<String>) -> Self {
        self.reporter_node_id = Some(node_id.into());
        self
    }

    pub fn new_reconnecting(
        addr: SocketAddr,
        auth: Option<ProtocolOwnerPeerAuth>,
        client_name: String,
        client_version: String,
    ) -> Self {
        Self {
            conn: Mutex::new(None),
            next_request_id: AtomicU64::new(20_000),
            reporter_node_id: None,
            reconnect: Some(ProtocolOwnerPeerConnectConfig {
                addr,
                auth,
                client_name,
                client_version,
            }),
        }
    }

    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn take_conn(&self, guard: &mut Option<Conn>) -> Result<Conn, BrokerError> {
        if guard.is_none() {
            let reconnect = self.reconnect.as_ref().ok_or_else(|| {
                BrokerError::Unknown("protocol owner peer connection is closed".into())
            })?;
            *guard = Some(reconnect.open().await?);
        }

        guard
            .take()
            .ok_or_else(|| BrokerError::Unknown("protocol owner peer connection is closed".into()))
    }
}

impl BrokerOwnerReplicationPeer for ProtocolOwnerReplicationPeer {
    fn read_owner_replication_records<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
    ) -> futures::future::BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        Box::pin(async move {
            let max_messages = u32::try_from(max_messages).map_err(|_| {
                BrokerError::InvalidArgument("max_messages exceeds protocol limit".into())
            })?;
            let max_events = u32::try_from(max_events).map_err(|_| {
                BrokerError::InvalidArgument("max_events exceeds protocol limit".into())
            })?;
            let request_id = self.next_request_id();
            let mut conn_guard = self.conn.lock().await;
            let mut conn = self.take_conn(&mut conn_guard).await?;
            if let Err(err) = conn
                .send(
                    try_encode(
                        Op::ReplicationRead,
                        request_id,
                        &ReplicationRead {
                            topic: topic.to_string(),
                            group: group.map(str::to_string),
                            partition,
                            message_from,
                            event_from,
                            max_messages,
                            max_events,
                            reporter_node_id: self.reporter_node_id.clone(),
                        },
                    )
                    .map_err(protocol_error)?,
                )
                .await
            {
                return Err(BrokerError::Unknown(format!(
                    "replication read send failed: {err}"
                )));
            }

            let read: ReplicationReadOk =
                match recv_response(&mut conn, request_id, Op::ReplicationReadOk).await {
                    Ok(read) => read,
                    Err(err) => {
                        if !err.invalidates_connection() {
                            *conn_guard = Some(conn);
                        }
                        return Err(protocol_request_error_to_broker(
                            err, topic, partition, group,
                        ));
                    }
                };
            *conn_guard = Some(conn);
            to_broker_owner_replication_records(read)
        })
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>> {
        Box::pin(async move {
            let request_id = self.next_request_id();
            let mut conn_guard = self.conn.lock().await;
            let mut conn = self.take_conn(&mut conn_guard).await?;
            if let Err(err) = conn
                .send(
                    try_encode(
                        Op::ReplicationCheckpointExport,
                        request_id,
                        &ReplicationCheckpointExport {
                            topic: topic.to_string(),
                            group: group.map(str::to_string),
                            partition,
                        },
                    )
                    .map_err(protocol_error)?,
                )
                .await
            {
                return Err(BrokerError::Unknown(format!(
                    "replication checkpoint export send failed: {err}"
                )));
            }

            let export: ReplicationCheckpointExportOk =
                match recv_response(&mut conn, request_id, Op::ReplicationCheckpointExportOk).await
                {
                    Ok(export) => export,
                    Err(err) => {
                        if !err.invalidates_connection() {
                            *conn_guard = Some(conn);
                        }
                        return Err(protocol_request_error_to_broker(
                            err, topic, partition, group,
                        ));
                    }
                };
            *conn_guard = Some(conn);
            Ok(OwnerStateCheckpoint {
                message_epoch: export.checkpoint.message_epoch,
                event_epoch: export.checkpoint.event_epoch,
                message_checkpoint_offset: export.checkpoint.message_checkpoint_offset,
                message_next_offset: export.checkpoint.message_next_offset,
                event_next_offset: export.checkpoint.event_next_offset,
                applied_event_offset: export.checkpoint.applied_event_offset,
                state_snapshot: export.checkpoint.state_snapshot,
            })
        })
    }
}
/// Pull replicated log records from an owner connection and apply them to a
/// follower connection until both streams return empty or a limit is reached.
///
/// This is intentionally only the manual catch-up core. Background scheduling,
/// ownership discovery, retries, and checkpoint installation belong above it.
pub async fn catch_up_replication_over_protocol(
    owner: &mut Conn,
    follower: &mut Conn,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
    options: ProtocolReplicationCatchUpOptions,
) -> anyhow::Result<ProtocolReplicationCatchUp> {
    if options.max_messages_per_read == 0
        || options.max_events_per_read == 0
        || options.max_iterations == 0
    {
        bail!("replication catch-up limits must be greater than zero");
    }

    let mut request_id = options.request_id_start;
    let mut progress = ProtocolReplicationCatchUpProgress {
        message_next_offset: options.message_from,
        event_next_offset: options.event_from,
        ..Default::default()
    };

    for _ in 0..options.max_iterations {
        let read_request_id = request_id;
        request_id += 1;
        owner
            .send(try_encode(
                Op::ReplicationRead,
                read_request_id,
                &ReplicationRead {
                    topic: topic.to_string(),
                    group: group.map(str::to_string),
                    partition,
                    message_from: progress.message_next_offset,
                    event_from: progress.event_next_offset,
                    max_messages: options.max_messages_per_read,
                    max_events: options.max_events_per_read,
                    reporter_node_id: None,
                },
            )?)
            .await
            .context("failed to send replication read request")?;

        let read: ReplicationReadOk =
            recv_response(owner, read_request_id, Op::ReplicationReadOk).await?;

        if let Some(required) = message_checkpoint_required(&read.messages) {
            return Ok(ProtocolReplicationCatchUp::CheckpointRequired {
                progress,
                messages: Some(required),
                events: event_checkpoint_required(&read.events),
            });
        }
        if let Some(required) = event_checkpoint_required(&read.events) {
            return Ok(ProtocolReplicationCatchUp::CheckpointRequired {
                progress,
                messages: None,
                events: Some(required),
            });
        }

        let (messages, message_record_count, message_next_offset) =
            message_apply_batch(read.messages)?;
        let (events, event_record_count, event_next_offset) = event_apply_batch(read.events)?;

        if message_record_count == 0 && event_record_count == 0 {
            return Ok(ProtocolReplicationCatchUp::CaughtUp(progress));
        }

        let apply_request_id = request_id;
        request_id += 1;
        follower
            .send(try_encode(
                Op::ReplicationApply,
                apply_request_id,
                &ReplicationApply {
                    topic: topic.to_string(),
                    group: group.map(str::to_string),
                    partition,
                    messages,
                    events,
                },
            )?)
            .await
            .context("failed to send replication apply request")?;

        let apply: ReplicationApplyOk =
            recv_response(follower, apply_request_id, Op::ReplicationApplyOk).await?;
        if message_record_count > 0 && !apply.messages_applied {
            bail!("replication apply response reported unapplied message records");
        }
        if event_record_count > 0 && !apply.events_applied {
            bail!("replication apply response reported unapplied event records");
        }

        progress.iterations += 1;
        progress.applied_message_records += message_record_count;
        progress.applied_event_records += event_record_count;
        progress.message_next_offset = message_next_offset;
        progress.event_next_offset = event_next_offset;
    }

    Ok(ProtocolReplicationCatchUp::IterationLimit { progress })
}

async fn recv_response<T>(
    conn: &mut Conn,
    request_id: u64,
    expected: Op,
) -> Result<T, ProtocolReplicationRequestError>
where
    T: for<'de> serde::Deserialize<'de>,
{
    loop {
        let frame = conn
            .next()
            .await
            .ok_or(ProtocolReplicationRequestError::ConnectionClosed)?
            .map_err(|err| ProtocolReplicationRequestError::Read(err.to_string()))?;

        if frame.opcode == Op::Ping as u16 {
            let pong = try_encode(Op::Pong, frame.request_id, &())
                .map_err(|err| ProtocolReplicationRequestError::HeartbeatEncode(err.to_string()))?;
            conn.send(pong)
                .await
                .map_err(|err| ProtocolReplicationRequestError::HeartbeatSend(err.to_string()))?;
            continue;
        }

        if frame.request_id != request_id {
            return Err(ProtocolReplicationRequestError::UnexpectedRequestId {
                actual: frame.request_id,
                expected: request_id,
            });
        }

        if frame.opcode == Op::Error as u16
            || frame.opcode == Op::HelloErr as u16
            || frame.opcode == Op::AuthErr as u16
        {
            let error: ErrorMsg = try_decode(&frame)
                .map_err(|err| ProtocolReplicationRequestError::Decode(err.to_string()))?;
            return Err(ProtocolReplicationRequestError::Protocol {
                code: error.code,
                message: error.message,
            });
        }

        if frame.opcode != expected as u16 {
            return Err(ProtocolReplicationRequestError::UnexpectedOpcode {
                actual: frame.opcode,
                expected: expected as u16,
            });
        }

        return try_decode(&frame)
            .map_err(|err| ProtocolReplicationRequestError::Decode(err.to_string()));
    }
}

fn message_checkpoint_required(
    read: &ReplicationMessageRead,
) -> Option<ReplicationCheckpointRequired> {
    match read {
        ReplicationMessageRead::CheckpointRequired(required) => Some(required.clone()),
        ReplicationMessageRead::Batch { .. } => None,
    }
}

fn event_checkpoint_required(read: &ReplicationEventRead) -> Option<ReplicationCheckpointRequired> {
    match read {
        ReplicationEventRead::CheckpointRequired(required) => Some(required.clone()),
        ReplicationEventRead::Batch { .. } => None,
    }
}

fn message_apply_batch(
    read: ReplicationMessageRead,
) -> anyhow::Result<(Option<ReplicationMessageApplyBatch>, usize, u64)> {
    match read {
        ReplicationMessageRead::Batch {
            epoch,
            next_offset,
            records,
            ..
        } => {
            let count = records.len();
            Ok((
                (count > 0).then_some(ReplicationMessageApplyBatch { epoch, records }),
                count,
                next_offset,
            ))
        }
        ReplicationMessageRead::CheckpointRequired(_) => {
            bail!("message checkpoint requirement was not handled before apply conversion")
        }
    }
}

fn event_apply_batch(
    read: ReplicationEventRead,
) -> anyhow::Result<(Option<ReplicationEventApplyBatch>, usize, u64)> {
    match read {
        ReplicationEventRead::Batch {
            epoch,
            next_offset,
            records,
            ..
        } => {
            let count = records.len();
            Ok((
                (count > 0).then_some(ReplicationEventApplyBatch { epoch, records }),
                count,
                next_offset,
            ))
        }
        ReplicationEventRead::CheckpointRequired(_) => {
            bail!("event checkpoint requirement was not handled before apply conversion")
        }
    }
}

fn protocol_error(err: impl std::fmt::Display) -> BrokerError {
    BrokerError::Unknown(format!("protocol replication error: {err}"))
}

fn protocol_request_error_to_broker(
    err: ProtocolReplicationRequestError,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
) -> BrokerError {
    match err {
        ProtocolReplicationRequestError::Protocol { code, .. } if code == ERR_NOT_OWNER => {
            BrokerError::NotOwner {
                topic: topic.into(),
                partition,
                group: group.map(str::to_string),
            }
        }
        _ => BrokerError::Unknown(err.to_string()),
    }
}

fn to_broker_owner_replication_records(
    read: ReplicationReadOk,
) -> Result<BrokerOwnerReplicationRecords, BrokerError> {
    Ok(BrokerOwnerReplicationRecords {
        messages: to_broker_message_read(read.messages)?,
        events: to_broker_event_read(read.events)?,
    })
}

fn to_broker_message_read(
    read: ReplicationMessageRead,
) -> Result<OwnerReplicationRead<Message>, BrokerError> {
    Ok(match read {
        ReplicationMessageRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => OwnerReplicationRead::Batch(OwnerReplicationBatch {
            epoch,
            requested_offset,
            next_offset,
            records: records
                .into_iter()
                .map(|record| {
                    (
                        record.offset,
                        Message {
                            flags: record.flags,
                            headers: record.headers,
                            payload: record.payload,
                        },
                    )
                })
                .collect(),
        }),
        ReplicationMessageRead::CheckpointRequired(required) => {
            OwnerReplicationRead::CheckpointRequired {
                epoch: required.epoch,
                requested_offset: required.requested_offset,
                head_offset: required.head_offset,
                next_offset: required.next_offset,
            }
        }
    })
}

fn to_broker_event_read(
    read: ReplicationEventRead,
) -> Result<OwnerReplicationRead<StromaEvent>, BrokerError> {
    Ok(match read {
        ReplicationEventRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => {
            let records = records
                .into_iter()
                .map(|record| {
                    let event = StromaEvent::decode(&record.payload).map_err(|err| {
                        BrokerError::Unknown(format!(
                            "failed to decode replicated event record: {err}"
                        ))
                    })?;
                    Ok((record.offset, event))
                })
                .collect::<Result<Vec<_>, BrokerError>>()?;
            OwnerReplicationRead::Batch(OwnerReplicationBatch {
                epoch,
                requested_offset,
                next_offset,
                records,
            })
        }
        ReplicationEventRead::CheckpointRequired(required) => {
            OwnerReplicationRead::CheckpointRequired {
                epoch: required.epoch,
                requested_offset: required.requested_offset,
                head_offset: required.head_offset,
                next_offset: required.next_offset,
            }
        }
    })
}
