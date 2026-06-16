use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

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
use serde::de::DeserializeOwned;
use tokio::{net::TcpStream, sync::Mutex};
use tokio_util::sync::CancellationToken;

use crate::v1::{
    Auth, ERR_NOT_OWNER, ErrorMsg, Hello, HelloOk, Op, PROTOCOL_V1, ReplicationApply,
    ReplicationApplyOk, ReplicationCheckpointExport, ReplicationCheckpointExportOk,
    ReplicationCheckpointRequired, ReplicationEventApplyBatch, ReplicationEventRead,
    ReplicationMessageApplyBatch, ReplicationMessageRead, ReplicationRead, ReplicationReadOk,
    frame::{Frame, ProtoCodec},
    helper::{Conn, ProtocolError, try_decode, try_encode},
    replication_payload::decode_replication_read_ok,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolReplicationCatchUpOptions {
    pub message_from: u64,
    pub event_from: u64,
    pub max_messages_per_read: u32,
    pub max_events_per_read: u32,
    pub max_bytes_per_read: u64,
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
            max_bytes_per_read: 8 * 1024 * 1024,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolReplicationStream {
    Messages,
    Events,
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolReplicationCatchUpError {
    #[error("replication catch-up limits must be greater than zero")]
    InvalidLimits,
    #[error("failed to encode {operation} request: {source}")]
    Encode {
        operation: &'static str,
        #[source]
        source: ProtocolError,
    },
    #[error("failed to send {operation} request: {message}")]
    Send {
        operation: &'static str,
        message: String,
    },
    #[error(transparent)]
    Request(#[from] ProtocolReplicationRequestError),
    #[error("replication apply response reported unapplied {stream:?} records")]
    Unapplied { stream: ProtocolReplicationStream },
    #[error("{stream:?} checkpoint requirement was not handled before apply conversion")]
    UnhandledCheckpointRequired { stream: ProtocolReplicationStream },
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
    peers: Mutex<HashMap<String, Arc<ProtocolOwnerReplicationPeer>>>,
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

    pub async fn close_all(&self) {
        let peers = {
            let mut peers = self.peers.lock().await;
            peers.drain().map(|(_, peer)| peer).collect::<Vec<_>>()
        };
        for peer in peers {
            peer.close().await;
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
                let peer: Arc<dyn BrokerOwnerReplicationPeer> = peer.clone();
                return Ok(Some(peer));
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
            let peer = Arc::new(built);
            peers.insert(assignment.owner.clone(), peer.clone());
            let peer: Arc<dyn BrokerOwnerReplicationPeer> = peer;
            Ok(Some(peer))
        })
    }
}

struct CachedProtocolOwnerPeer {
    addr: SocketAddr,
    peer: Arc<ProtocolOwnerReplicationPeer>,
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

    pub async fn close_all(&self) {
        let peers = {
            let mut peers = self.peers.lock().await;
            peers
                .drain()
                .map(|(_, cached)| cached.peer)
                .collect::<Vec<_>>()
        };
        for peer in peers {
            peer.close().await;
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
                    let peer: Arc<dyn BrokerOwnerReplicationPeer> = cached.peer.clone();
                    return Ok(Some(peer));
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
            let peer = Arc::new(built);
            peers.insert(
                assignment.owner.clone(),
                CachedProtocolOwnerPeer {
                    addr,
                    peer: peer.clone(),
                },
            );
            let peer: Arc<dyn BrokerOwnerReplicationPeer> = peer;
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
    request_lock: Mutex<()>,
    next_request_id: AtomicU64,
    reconnect: Option<ProtocolOwnerPeerConnectConfig>,
    close_token: CancellationToken,
    /// Stamped into replication reads for owner-side progress tracking.
    reporter_node_id: Option<String>,
}

impl ProtocolOwnerReplicationPeer {
    pub fn new(conn: Conn) -> Self {
        Self {
            conn: Mutex::new(Some(conn)),
            request_lock: Mutex::new(()),
            next_request_id: AtomicU64::new(20_000),
            reconnect: None,
            close_token: CancellationToken::new(),
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
            request_lock: Mutex::new(()),
            next_request_id: AtomicU64::new(20_000),
            reporter_node_id: None,
            reconnect: Some(ProtocolOwnerPeerConnectConfig {
                addr,
                auth,
                client_name,
                client_version,
            }),
            close_token: CancellationToken::new(),
        }
    }

    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn take_conn(&self) -> Result<Conn, BrokerError> {
        if self.close_token.is_cancelled() {
            return Err(BrokerError::Unknown("protocol owner peer closed".into()));
        }

        if let Some(conn) = self.conn.lock().await.take() {
            return Ok(conn);
        }

        let reconnect = self.reconnect.as_ref().ok_or_else(|| {
            BrokerError::Unknown("protocol owner peer connection is closed".into())
        })?;
        reconnect.open().await
    }

    async fn restore_conn(&self, conn: Conn) {
        if self.close_token.is_cancelled() {
            return;
        }
        *self.conn.lock().await = Some(conn);
    }

    pub async fn close(&self) {
        self.close_token.cancel();
        self.conn.lock().await.take();
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
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> futures::future::BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        Box::pin(async move {
            let max_messages = u32::try_from(max_messages).map_err(|_| {
                BrokerError::InvalidArgument("max_messages exceeds protocol limit".into())
            })?;
            let max_events = u32::try_from(max_events).map_err(|_| {
                BrokerError::InvalidArgument("max_events exceeds protocol limit".into())
            })?;
            let max_bytes = u64::try_from(max_bytes).map_err(|_| {
                BrokerError::InvalidArgument("max_bytes exceeds protocol limit".into())
            })?;
            let max_wait_ms = u32::try_from(max_wait_ms).map_err(|_| {
                BrokerError::InvalidArgument("max_wait_ms exceeds protocol limit".into())
            })?;
            let _request_guard = tokio::select! {
                biased;
                _ = self.close_token.cancelled() => {
                    return Err(BrokerError::Unknown("protocol owner peer closed".into()));
                }
                guard = self.request_lock.lock() => guard,
            };
            let request_id = self.next_request_id();
            let mut conn = self.take_conn().await?;
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
                            max_bytes,
                            max_wait_ms,
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

            let read: ReplicationReadOk = match tokio::select! {
                biased;
                _ = self.close_token.cancelled() => {
                    Err(ProtocolReplicationRequestError::ConnectionClosed)
                }
                response = recv_replication_read_ok_response(&mut conn, request_id) => {
                    response
                }
            } {
                Ok(read) => read,
                Err(err) => {
                    if !err.invalidates_connection() {
                        self.restore_conn(conn).await;
                    }
                    return Err(protocol_request_error_to_broker(
                        err, topic, partition, group,
                    ));
                }
            };
            self.restore_conn(conn).await;
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
            let _request_guard = tokio::select! {
                biased;
                _ = self.close_token.cancelled() => {
                    return Err(BrokerError::Unknown("protocol owner peer closed".into()));
                }
                guard = self.request_lock.lock() => guard,
            };
            let request_id = self.next_request_id();
            let mut conn = self.take_conn().await?;
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

            let export: ReplicationCheckpointExportOk = match tokio::select! {
                biased;
                _ = self.close_token.cancelled() => {
                    Err(ProtocolReplicationRequestError::ConnectionClosed)
                }
                response = recv_response(
                    &mut conn,
                    request_id,
                    Op::ReplicationCheckpointExportOk,
                ) => {
                    response
                }
            } {
                Ok(export) => export,
                Err(err) => {
                    if !err.invalidates_connection() {
                        self.restore_conn(conn).await;
                    }
                    return Err(protocol_request_error_to_broker(
                        err, topic, partition, group,
                    ));
                }
            };
            self.restore_conn(conn).await;
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
) -> Result<ProtocolReplicationCatchUp, ProtocolReplicationCatchUpError> {
    if options.max_messages_per_read == 0
        || options.max_events_per_read == 0
        || options.max_bytes_per_read == 0
        || options.max_iterations == 0
    {
        return Err(ProtocolReplicationCatchUpError::InvalidLimits);
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
        let read_request = try_encode(
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
                max_bytes: options.max_bytes_per_read,
                max_wait_ms: 0,
                reporter_node_id: None,
            },
        )
        .map_err(|source| ProtocolReplicationCatchUpError::Encode {
            operation: "replication read",
            source,
        })?;
        owner
            .send(read_request)
            .await
            .map_err(|err| ProtocolReplicationCatchUpError::Send {
                operation: "replication read",
                message: err.to_string(),
            })?;

        let read: ReplicationReadOk =
            recv_replication_read_ok_response(owner, read_request_id).await?;

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
        let apply_request = try_encode(
            Op::ReplicationApply,
            apply_request_id,
            &ReplicationApply {
                topic: topic.to_string(),
                group: group.map(str::to_string),
                partition,
                messages,
                events,
            },
        )
        .map_err(|source| ProtocolReplicationCatchUpError::Encode {
            operation: "replication apply",
            source,
        })?;
        follower.send(apply_request).await.map_err(|err| {
            ProtocolReplicationCatchUpError::Send {
                operation: "replication apply",
                message: err.to_string(),
            }
        })?;

        let apply: ReplicationApplyOk =
            recv_response(follower, apply_request_id, Op::ReplicationApplyOk).await?;
        if message_record_count > 0 && !apply.messages_applied {
            return Err(ProtocolReplicationCatchUpError::Unapplied {
                stream: ProtocolReplicationStream::Messages,
            });
        }
        if event_record_count > 0 && !apply.events_applied {
            return Err(ProtocolReplicationCatchUpError::Unapplied {
                stream: ProtocolReplicationStream::Events,
            });
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
    T: DeserializeOwned + Send + 'static,
{
    let frame = recv_response_frame(conn, request_id, expected).await?;
    decode_response_frame(frame).await
}

async fn recv_replication_read_ok_response(
    conn: &mut Conn,
    request_id: u64,
) -> Result<ReplicationReadOk, ProtocolReplicationRequestError> {
    let frame = recv_response_frame(conn, request_id, Op::ReplicationReadOk).await?;
    decode_replication_read_ok(&frame)
        .map_err(|err| ProtocolReplicationRequestError::Decode(err.to_string()))
}

async fn recv_response_frame(
    conn: &mut Conn,
    request_id: u64,
    expected: Op,
) -> Result<Frame, ProtocolReplicationRequestError> {
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

        return Ok(frame);
    }
}

async fn decode_response_frame<T>(frame: Frame) -> Result<T, ProtocolReplicationRequestError>
where
    T: DeserializeOwned + Send + 'static,
{
    const BLOCKING_DECODE_BYTES: usize = 1 << 20;

    if frame.payload.len() < BLOCKING_DECODE_BYTES {
        return try_decode(&frame)
            .map_err(|err| ProtocolReplicationRequestError::Decode(err.to_string()));
    }

    tokio::task::spawn_blocking(move || try_decode::<T>(&frame))
        .await
        .map_err(|err| {
            ProtocolReplicationRequestError::Decode(format!("decode task failed: {err}"))
        })?
        .map_err(|err| ProtocolReplicationRequestError::Decode(err.to_string()))
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
) -> Result<(Option<ReplicationMessageApplyBatch>, usize, u64), ProtocolReplicationCatchUpError> {
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
        ReplicationMessageRead::CheckpointRequired(_) => Err(
            ProtocolReplicationCatchUpError::UnhandledCheckpointRequired {
                stream: ProtocolReplicationStream::Messages,
            },
        ),
    }
}

fn event_apply_batch(
    read: ReplicationEventRead,
) -> Result<(Option<ReplicationEventApplyBatch>, usize, u64), ProtocolReplicationCatchUpError> {
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
        ReplicationEventRead::CheckpointRequired(_) => Err(
            ProtocolReplicationCatchUpError::UnhandledCheckpointRequired {
                stream: ProtocolReplicationStream::Events,
            },
        ),
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
