use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use fibril_broker::{
    Offset, Partition,
    broker::{
        BrokerError, BrokerOwnerReplicationPeer, BrokerOwnerReplicationPeerResolver,
        BrokerOwnerReplicationRecords, BrokerReplicationStreamApply, FollowerStreamExit,
        ReplicatedStreamApply, ReplicationResourceKind,
    },
    coordination::{Coordination, NodeInfo, PartitionAssignment},
    queue_engine::{
        Message, OwnerReplicationBatch, OwnerReplicationRead, OwnerStateCheckpoint, StromaEvent,
    },
    replication::StreamApplyTunablesFn,
};
use fibril_util::net::TcpStream;
use futures::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use tokio::{sync::Mutex, sync::mpsc};
use tokio_rustls::rustls;
use tokio_util::sync::CancellationToken;

use crate::v1::{
    Auth, ERR_NOT_OWNER, ErrorMsg, Hello, HelloOk, Op, PROTOCOL_V1, ReplicationApply,
    ReplicationApplyOk, ReplicationCheckpointExport, ReplicationCheckpointExportOk,
    ReplicationCheckpointRequired, ReplicationEventApplyBatch, ReplicationEventRead,
    ReplicationMessageApplyBatch, ReplicationMessageRead, ReplicationRead, ReplicationReadOk,
    ReplicationStreamProgress, ReplicationStreamReset, ReplicationStreamStart,
    frame::Frame,
    helper::{self, Conn, ProtocolError, try_decode, try_encode},
    replication_stream::{
        self, ApplierExit, FollowerApplyOutcome, FollowerStreamControl, FollowerStreamSink,
        run_follower_stream_applier,
    },
    wire,
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
            max_messages_per_read: 2048,
            max_events_per_read: 2048,
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

/// TLS connector for follower-to-owner connections. A newtype so the
/// resolver config keeps deriving `Debug` (the rustls connector does not).
#[derive(Clone)]
pub struct PeerTlsConnector(pub tokio_rustls::TlsConnector);

impl std::fmt::Debug for PeerTlsConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PeerTlsConnector")
    }
}

#[derive(Debug, Clone)]
pub struct ProtocolOwnerPeerResolverConfig {
    pub nodes: HashMap<String, String>,
    pub auth: Option<ProtocolOwnerPeerAuth>,
    /// When set, owner connections are wrapped in TLS with this connector.
    pub tls: Option<PeerTlsConnector>,
    pub client_name: String,
    pub client_version: String,
    /// This follower's node id, stamped into replication reads so the owner
    /// can track durable progress per follower (publish-confirm policies).
    pub reporter_node_id: Option<String>,
    /// Slack added to a read's long-poll window before the follower abandons it
    /// and drops the connection. See [`DEFAULT_READ_TIMEOUT_SLACK_MS`].
    pub read_timeout_slack_ms: u64,
    /// Upper bound on establishing a connection to an owner (connect plus
    /// handshake). See [`DEFAULT_OWNER_CONNECT_TIMEOUT_MS`].
    pub owner_connect_timeout_ms: u64,
}

impl ProtocolOwnerPeerResolverConfig {
    pub fn new(nodes: HashMap<String, String>) -> Self {
        Self {
            nodes,
            auth: None,
            tls: None,
            client_name: "fibril-replication".into(),
            client_version: env!("CARGO_PKG_VERSION").into(),
            reporter_node_id: None,
            read_timeout_slack_ms: DEFAULT_READ_TIMEOUT_SLACK_MS,
            owner_connect_timeout_ms: DEFAULT_OWNER_CONNECT_TIMEOUT_MS,
        }
    }

    /// Set the follower read-timeout slack and owner connection-setup timeout
    /// (both milliseconds), threaded from the broker's replication settings.
    pub fn with_timeouts(
        mut self,
        read_timeout_slack_ms: u64,
        owner_connect_timeout_ms: u64,
    ) -> Self {
        self.read_timeout_slack_ms = read_timeout_slack_ms;
        self.owner_connect_timeout_ms = owner_connect_timeout_ms;
        self
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

    /// Wrap owner connections in TLS with this connector.
    pub fn with_tls(mut self, connector: tokio_rustls::TlsConnector) -> Self {
        self.tls = Some(PeerTlsConnector(connector));
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
    // Keyed by (owner, kind): a stream peer reads via the stream-mode pull op, so
    // it is cached separately from a queue peer to the same owner.
    peers: Mutex<HashMap<(String, ReplicationResourceKind), Arc<ProtocolOwnerReplicationPeer>>>,
}

impl StaticProtocolOwnerPeerResolver {
    pub fn new(nodes: HashMap<String, String>) -> Self {
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
        kind: ReplicationResourceKind,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<Arc<dyn BrokerOwnerReplicationPeer>>, BrokerError>,
    > {
        Box::pin(async move {
            let Some(addr) = self.cfg.nodes.get(&assignment.owner).cloned() else {
                return Ok(None);
            };

            let cache_key = (assignment.owner.clone(), kind);
            let mut peers = self.peers.lock().await;
            if let Some(peer) = peers.get(&cache_key) {
                let peer: Arc<dyn BrokerOwnerReplicationPeer> = peer.clone();
                return Ok(Some(peer));
            }

            let mut built = ProtocolOwnerReplicationPeer::new_reconnecting(
                addr,
                self.cfg.auth.clone(),
                self.cfg.client_name.clone(),
                self.cfg.client_version.clone(),
            )
            .with_read_timeout_slack_ms(self.cfg.read_timeout_slack_ms)
            .with_owner_connect_timeout_ms(self.cfg.owner_connect_timeout_ms);
            if let Some(reporter) = &self.cfg.reporter_node_id {
                built = built.with_reporter(reporter.clone());
            }
            if let Some(tls) = &self.cfg.tls {
                built = built.with_peer_tls(tls.clone());
            }
            if kind == ReplicationResourceKind::Stream {
                built = built.with_stream_mode();
            }
            let peer = Arc::new(built);
            peers.insert(cache_key, peer.clone());
            let peer: Arc<dyn BrokerOwnerReplicationPeer> = peer;
            Ok(Some(peer))
        })
    }
}

struct CachedProtocolOwnerPeer {
    addr: String,
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
    // Keyed by (owner, kind): a stream peer reads via the stream-mode pull op, so
    // it is cached separately from a queue peer to the same owner.
    peers: Mutex<HashMap<(String, ReplicationResourceKind), CachedProtocolOwnerPeer>>,
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
        kind: ReplicationResourceKind,
    ) -> futures::future::BoxFuture<
        'a,
        Result<Option<Arc<dyn BrokerOwnerReplicationPeer>>, BrokerError>,
    > {
        Box::pin(async move {
            let cache_key = (assignment.owner.clone(), kind);
            let snapshot = self.coordination.snapshot();
            let Some(node) = snapshot.nodes.get(&assignment.owner) else {
                self.peers.lock().await.remove(&cache_key);
                return Ok(None);
            };
            let addr = node.broker_addr.clone();

            let mut peers = self.peers.lock().await;
            if let Some(cached) = peers.get(&cache_key) {
                if cached.addr == addr {
                    let peer: Arc<dyn BrokerOwnerReplicationPeer> = cached.peer.clone();
                    return Ok(Some(peer));
                }
            }

            let mut built = ProtocolOwnerReplicationPeer::new_reconnecting(
                addr.clone(),
                self.cfg.auth.clone(),
                self.cfg.client_name.clone(),
                self.cfg.client_version.clone(),
            )
            .with_read_timeout_slack_ms(self.cfg.read_timeout_slack_ms)
            .with_owner_connect_timeout_ms(self.cfg.owner_connect_timeout_ms);
            if let Some(reporter) = &self.cfg.reporter_node_id {
                built = built.with_reporter(reporter.clone());
            }
            if let Some(tls) = &self.cfg.tls {
                built = built.with_peer_tls(tls.clone());
            }
            if kind == ReplicationResourceKind::Stream {
                built = built.with_stream_mode();
            }
            let peer = Arc::new(built);
            peers.insert(
                cache_key,
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
    addr: String,
    auth: Option<&ProtocolOwnerPeerAuth>,
    tls: Option<&PeerTlsConnector>,
    client_name: &str,
    client_version: &str,
    connect_timeout_ms: u64,
) -> Result<Conn, BrokerError> {
    // Bound connect plus handshake: a partition that drops the SYN or a handshake
    // response must not hang the follower worker on a half-open connection. On
    // timeout the worker errors out and retries, redialing once the link is back.
    let setup = async {
        let stream = TcpStream::connect(addr.as_str())
            .await
            .map_err(|err| BrokerError::Unknown(format!("owner peer connect failed: {err}")))?;
        let mut conn = match tls {
            None => helper::plain_conn(stream),
            Some(connector) => {
                let name = peer_server_name(&addr)?;
                match connector.0.connect(name, stream).await {
                    Ok(stream) => helper::tls_conn(stream),
                    Err(err) => return Err(classify_peer_tls_error(err, &addr)),
                }
            }
        };

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

        Ok::<Conn, BrokerError>(conn)
    };

    match tokio::time::timeout(std::time::Duration::from_millis(connect_timeout_ms), setup).await {
        Ok(result) => result,
        Err(_) => Err(BrokerError::Unknown(format!(
            "owner peer connection setup timed out after {connect_timeout_ms}ms"
        ))),
    }
}

/// Derive the TLS server name from an owner `host:port` address. Brackets
/// are stripped so an IPv6 literal verifies as a plain address.
fn peer_server_name(addr: &str) -> Result<rustls::pki_types::ServerName<'static>, BrokerError> {
    let host = addr
        .rsplit_once(':')
        .map(|(host, _)| host)
        .unwrap_or(addr)
        .trim_matches(['[', ']'])
        .to_string();
    rustls::pki_types::ServerName::try_from(host).map_err(|err| {
        BrokerError::Unknown(format!(
            "invalid TLS server name derived from owner address `{addr}`: {err}"
        ))
    })
}

/// Name a failed peer TLS handshake so the follower log guides the fix: an
/// early close is almost always a plaintext owner, a certificate failure is
/// peer-CA trust configuration, everything else stays a handshake error.
fn classify_peer_tls_error(err: std::io::Error, addr: &str) -> BrokerError {
    if matches!(
        err.kind(),
        std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
    ) {
        return BrokerError::Unknown(format!(
            "owner peer at {addr} closed the connection during the TLS handshake: it is \
             likely serving plaintext. Enable tls on that broker or set \
             tls.inter_broker = false on this one"
        ));
    }
    if let Some(inner) = err.get_ref()
        && let Some(tls_err) = inner.downcast_ref::<rustls::Error>()
        && matches!(tls_err, rustls::Error::InvalidCertificate(_))
    {
        return BrokerError::Unknown(format!(
            "owner peer at {addr} presented a certificate this node does not trust: {tls_err}. \
             Point tls.peer_ca_path at the CA that issued the peer certificates"
        ));
    }
    BrokerError::Unknown(format!(
        "owner peer TLS handshake with {addr} failed: {err}"
    ))
}

pub async fn connect_protocol_owner_peer(
    addr: String,
    auth: Option<&ProtocolOwnerPeerAuth>,
    tls: Option<&PeerTlsConnector>,
    client_name: &str,
    client_version: &str,
) -> Result<ProtocolOwnerReplicationPeer, BrokerError> {
    Ok(ProtocolOwnerReplicationPeer::new(
        open_protocol_owner_conn(
            addr,
            auth,
            tls,
            client_name,
            client_version,
            DEFAULT_OWNER_CONNECT_TIMEOUT_MS,
        )
        .await?,
    ))
}

#[derive(Debug, Clone)]
struct ProtocolOwnerPeerConnectConfig {
    addr: String,
    auth: Option<ProtocolOwnerPeerAuth>,
    tls: Option<PeerTlsConnector>,
    client_name: String,
    client_version: String,
    connect_timeout_ms: u64,
}

impl ProtocolOwnerPeerConnectConfig {
    async fn open(&self) -> Result<Conn, BrokerError> {
        open_protocol_owner_conn(
            self.addr.clone(),
            self.auth.as_ref(),
            self.tls.as_ref(),
            &self.client_name,
            &self.client_version,
            self.connect_timeout_ms,
        )
        .await
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolReplicationRequestError {
    #[error("connection closed while waiting for response")]
    ConnectionClosed,
    #[error("timed out after {0}ms waiting for the owner's response")]
    ReadTimeout(u64),
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
/// Slack added on top of a read's long-poll window before the follower gives up
/// on the owner's response and drops the connection. A read long-polls for
/// `max_wait_ms`, so the deadline is `max_wait_ms + slack`; the slack covers round
/// trip and owner processing. Without a deadline a partition that drops the
/// in-flight response would leave the worker waiting on a dead connection until
/// the transport itself broke, which can take a very long time.
pub const DEFAULT_READ_TIMEOUT_SLACK_MS: u64 = 10_000;

/// Upper bound on establishing a replication connection (TCP connect plus the
/// HELLO/AUTH handshake). A partition that drops the SYN or a handshake response
/// must not hang the follower worker on a half-open connection.
pub const DEFAULT_OWNER_CONNECT_TIMEOUT_MS: u64 = 5_000;

pub struct ProtocolOwnerReplicationPeer {
    conn: Mutex<Option<Conn>>,
    request_lock: Mutex<()>,
    next_request_id: AtomicU64,
    reconnect: Option<ProtocolOwnerPeerConnectConfig>,
    close_token: CancellationToken,
    /// Stamped into replication reads for owner-side progress tracking.
    reporter_node_id: Option<String>,
    /// When set, the peer reads a STREAM partition: it sends
    /// `StreamReplicationRead` and expects `StreamReplicationReadOk`. The body is
    /// identical to the queue read; only the op differs.
    stream_mode: bool,
    /// Added to a read's `max_wait_ms` to bound how long the follower waits for
    /// the owner's response before treating the connection as dead.
    read_timeout_slack_ms: u64,
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
            stream_mode: false,
            read_timeout_slack_ms: DEFAULT_READ_TIMEOUT_SLACK_MS,
        }
    }

    /// Stamp replication reads with this follower's identity.
    pub fn with_reporter(mut self, node_id: impl Into<String>) -> Self {
        self.reporter_node_id = Some(node_id.into());
        self
    }

    /// Override the slack added to a read's long-poll window before the follower
    /// gives up on the owner's response. See [`DEFAULT_READ_TIMEOUT_SLACK_MS`].
    pub fn with_read_timeout_slack_ms(mut self, slack_ms: u64) -> Self {
        self.read_timeout_slack_ms = slack_ms;
        self
    }

    /// Override the upper bound on establishing the connection (connect plus
    /// handshake). See [`DEFAULT_OWNER_CONNECT_TIMEOUT_MS`]. No-op on a peer that
    /// wraps an already-open connection (it never reconnects).
    pub fn with_owner_connect_timeout_ms(mut self, timeout_ms: u64) -> Self {
        if let Some(reconnect) = self.reconnect.as_mut() {
            reconnect.connect_timeout_ms = timeout_ms;
        }
        self
    }

    /// Read stream partitions (send `StreamReplicationRead`) instead of queues.
    pub fn with_stream_mode(mut self) -> Self {
        self.stream_mode = true;
        self
    }

    pub fn new_reconnecting(
        addr: String,
        auth: Option<ProtocolOwnerPeerAuth>,
        client_name: String,
        client_version: String,
    ) -> Self {
        Self {
            conn: Mutex::new(None),
            request_lock: Mutex::new(()),
            next_request_id: AtomicU64::new(20_000),
            reporter_node_id: None,
            stream_mode: false,
            read_timeout_slack_ms: DEFAULT_READ_TIMEOUT_SLACK_MS,
            reconnect: Some(ProtocolOwnerPeerConnectConfig {
                addr,
                auth,
                tls: None,
                client_name,
                client_version,
                connect_timeout_ms: DEFAULT_OWNER_CONNECT_TIMEOUT_MS,
            }),
            close_token: CancellationToken::new(),
        }
    }

    /// Wrap owner connections in TLS. No-op on a peer that wraps an
    /// already-open connection (it never reconnects).
    pub fn with_peer_tls(mut self, tls: PeerTlsConnector) -> Self {
        if let Some(reconnect) = self.reconnect.as_mut() {
            reconnect.tls = Some(tls);
        }
        self
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
            let read_op = if self.stream_mode {
                Op::StreamReplicationRead
            } else {
                Op::ReplicationRead
            };
            let mut conn = self.take_conn().await?;
            if let Err(err) = conn
                .send(
                    try_encode(
                        read_op,
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

            let recv = async {
                if self.stream_mode {
                    recv_stream_replication_read_ok_response(&mut conn, request_id).await
                } else {
                    recv_replication_read_ok_response(&mut conn, request_id).await
                }
            };
            // Bound the wait: the owner long-polls up to `max_wait_ms`, so anything
            // past that plus slack means the response was lost (for example a
            // partition that dropped it). Give up and drop the connection so the
            // worker reconnects instead of hanging on a dead socket.
            let read_deadline_ms = max_wait_ms as u64 + self.read_timeout_slack_ms;
            let read: ReplicationReadOk = match tokio::select! {
                biased;
                _ = self.close_token.cancelled() => {
                    Err(ProtocolReplicationRequestError::ConnectionClosed)
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(read_deadline_ms)) => {
                    Err(ProtocolReplicationRequestError::ReadTimeout(read_deadline_ms))
                }
                response = recv => {
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

    fn stream_replication<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        credit_bytes: u64,
        tunables: StreamApplyTunablesFn,
        buffer_batches: usize,
        apply: Arc<dyn BrokerReplicationStreamApply>,
        shutdown: CancellationToken,
    ) -> futures::future::BoxFuture<'a, Result<FollowerStreamExit, BrokerError>> {
        Box::pin(async move {
            // The stream owns a connection for its lifetime (no concurrent
            // request/response on it). take_conn reconnects if needed.
            let conn = self.take_conn().await?;
            let stream_id = self.next_request_id();
            let sink = Arc::new(StreamApplyAdapterSink { apply });
            let exit = run_follower_replication_stream(
                conn,
                sink,
                topic.to_string(),
                partition,
                group.map(str::to_string),
                message_from,
                event_from,
                credit_bytes,
                tunables,
                self.reporter_node_id.clone(),
                stream_id,
                buffer_batches,
                shutdown,
            )
            .await;
            // The connection is consumed/closed by the transport on exit; the
            // next take_conn reconnects.
            Ok(exit)
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
    wire::decode_replication_read_ok(&frame)
        .map_err(|err| ProtocolReplicationRequestError::Decode(err.to_string()))
}

async fn recv_stream_replication_read_ok_response(
    conn: &mut Conn,
    request_id: u64,
) -> Result<ReplicationReadOk, ProtocolReplicationRequestError> {
    let frame = recv_response_frame(conn, request_id, Op::StreamReplicationReadOk).await?;
    wire::decode_stream_replication_read_ok(&frame)
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
    // Decode inline below this size, else offload to spawn_blocking. This is a
    // SCHEDULER-FAIRNESS threshold, not a throughput one: inline decode is faster
    // (no threadpool handoff), but a long CPU-bound decode hogs the async worker
    // and starves other tasks. So the threshold is "the payload whose decode
    // exceeds the time we're willing to block a worker inline".
    //
    // Derived from `cargo bench --bench encode -- decode_publish/...`: decode runs
    // ~20 GB/s (cache-cold large) to ~50 GB/s (in-cache), so 4 MiB is ~80-200us
    // inline. That fits a ~150us cooperative budget with headroom for replication
    // batches (which do more per-record work than this payload-memcpy-bound bench).
    // Re-bench the actual ReplicationReadOk many-records case before raising further.
    //
    // FUTURE (adaptive): instead of a static value tuned on one CPU, FIT A
    // FUNCTION of decode-time vs bytes from a few real samples and solve
    // f(bytes) = block-budget for the threshold (the "breaking point"). Time
    // scales NON-linearly: ~linear within a cache tier but with a knee when the
    // payload spills L2/L3 (measured ~0.018 ns/B in-cache -> ~0.048 ns/B at
    // 16 MiB), so a single ns/B is wrong for large sizes - a 2-regime / piecewise
    // model (in-cache slope + out-of-cache slope, knee near cache size) fit from a
    // small and a large sample captures it. Sample on the inline path (EWMA per
    // regime, clamped + slow-moving); a startup calibration is a simpler first step.
    // Also account for the LOWER bound: spawn_blocking is not free (handoff to the
    // blocking pool + a oneshot await + a finite pool slot), so there is a
    // break-even floor - never offload a decode cheaper than the handoff. (Measure
    // the handoff cost to set that floor; 4 MiB is well above it.)
    const BLOCKING_DECODE_BYTES: usize = 4 << 20;

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

// ===========================================================================
// Follower side: credit-based streaming transport (increment 3b).
// ===========================================================================

fn stream_batch_bytes(batch: &ReplicationReadOk) -> u64 {
    let messages = match &batch.messages {
        ReplicationMessageRead::Batch { records, .. } => records
            .iter()
            .map(|record| (record.headers.len() + record.payload.len()) as u64)
            .sum(),
        ReplicationMessageRead::CheckpointRequired(_) => 0,
    };
    let events = match &batch.events {
        ReplicationEventRead::Batch { records, .. } => records
            .iter()
            .map(|record| record.payload.len() as u64)
            .sum(),
        ReplicationEventRead::CheckpointRequired(_) => 0,
    };
    messages + events
}

/// Run one follower replication stream over an established connection: send
/// `ReplicationStreamStart`, then a reader demuxes pushed `ReplicationStreamBatch`
/// frames into a bounded in-order buffer while an applier drains it (durably,
/// in order) and sends combined progress+credit back. Returns why the stream
/// ended. `buffer_batches` bounds the in-flight buffer; the granted `credit_bytes`
/// bounds in-flight bytes.
pub async fn run_follower_replication_stream<S: FollowerStreamSink>(
    conn: Conn,
    sink: Arc<S>,
    topic: String,
    partition: Partition,
    group: Option<String>,
    message_from: Offset,
    event_from: Offset,
    credit_bytes: u64,
    tunables: StreamApplyTunablesFn,
    reporter_node_id: Option<String>,
    stream_id: u64,
    buffer_batches: usize,
    shutdown: CancellationToken,
) -> FollowerStreamExit {
    let (mut conn_sink, mut conn_stream) = conn.split();

    let start = ReplicationStreamStart {
        topic,
        group,
        partition,
        message_from,
        event_from,
        credit_bytes,
        reporter_node_id,
    };
    match wire::encode_replication_stream_start(stream_id, &start) {
        Ok(frame) => {
            if conn_sink.send(frame).await.is_err() {
                return FollowerStreamExit::Error("failed to send stream start".into());
            }
        }
        Err(err) => return FollowerStreamExit::Error(err.to_string()),
    }

    let (batch_tx, batch_rx) = mpsc::channel::<ReplicationReadOk>(buffer_batches.max(1));
    let (control_tx, mut control_rx) = mpsc::channel::<FollowerStreamControl>(64);

    // Reader: demux pushed frames into the buffer; return the stream-end reason.
    let reader = tokio::spawn(async move {
        loop {
            match conn_stream.next().await {
                Some(Ok(frame)) => {
                    if frame.opcode == Op::ReplicationStreamBatch as u16 {
                        match wire::decode_replication_stream_batch(&frame) {
                            Ok(batch) => {
                                if batch_tx.send(batch).await.is_err() {
                                    return FollowerStreamExit::Closed;
                                }
                            }
                            Err(err) => {
                                return FollowerStreamExit::Error(format!(
                                    "decode stream batch: {err}"
                                ));
                            }
                        }
                    } else if frame.opcode == Op::ReplicationStreamEnd as u16 {
                        let code = wire::decode_replication_stream_end(&frame)
                            .map(|end| end.code)
                            .unwrap_or(replication_stream::STREAM_END_ERROR);
                        return if code == replication_stream::STREAM_END_CHECKPOINT_REQUIRED {
                            FollowerStreamExit::CheckpointRequired
                        } else if code == replication_stream::STREAM_END_CLOSED {
                            FollowerStreamExit::Closed
                        } else {
                            FollowerStreamExit::Error("owner ended stream".into())
                        };
                    }
                    // ignore unrelated opcodes on this connection
                }
                Some(Err(err)) => return FollowerStreamExit::Error(format!("stream recv: {err}")),
                None => return FollowerStreamExit::Error("connection closed".into()),
            }
        }
    });

    // Control writer: encode progress/reset back to the owner.
    let writer = tokio::spawn(async move {
        while let Some(control) = control_rx.recv().await {
            let frame = match control {
                FollowerStreamControl::Progress {
                    durable_message_next,
                    durable_event_next,
                    credit_add_bytes,
                } => wire::encode_replication_stream_progress(
                    stream_id,
                    &ReplicationStreamProgress {
                        durable_message_next,
                        durable_event_next,
                        credit_add_bytes,
                    },
                ),
                FollowerStreamControl::Reset {
                    message_from,
                    event_from,
                } => wire::encode_replication_stream_reset(
                    stream_id,
                    &ReplicationStreamReset {
                        message_from,
                        event_from,
                    },
                ),
            };
            match frame {
                Ok(frame) => {
                    if conn_sink.send(frame).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });

    let applier_exit = tokio::select! {
        _ = shutdown.cancelled() => None,
        exit = run_follower_stream_applier(
            sink,
            batch_rx,
            control_tx,
            message_from,
            event_from,
            tunables,
        ) => Some(exit),
    };

    let exit = match applier_exit {
        // The applier stopped because the reader closed the buffer; the reader
        // knows why (StreamEnd code or connection error).
        Some(ApplierExit::ReaderClosed) => match reader.await {
            Ok(reason) => reason,
            Err(_) => FollowerStreamExit::Error("reader task aborted".into()),
        },
        Some(ApplierExit::CheckpointRequired) => {
            reader.abort();
            FollowerStreamExit::CheckpointRequired
        }
        Some(ApplierExit::Error(err)) => {
            reader.abort();
            FollowerStreamExit::Error(err)
        }
        // Shutdown cancelled the applier.
        None => {
            reader.abort();
            FollowerStreamExit::Closed
        }
    };
    writer.abort();
    exit
}

/// Adapts a broker-side [`BrokerReplicationStreamApply`] (which speaks broker
/// records) to the protocol [`FollowerStreamSink`] the transport drives. Decodes
/// the batch to broker records, computes the credit (bytes) to return, and maps
/// the broker apply outcome.
struct StreamApplyAdapterSink {
    apply: Arc<dyn BrokerReplicationStreamApply>,
}

#[async_trait::async_trait]
impl FollowerStreamSink for StreamApplyAdapterSink {
    async fn apply(&self, batch: ReplicationReadOk) -> FollowerApplyOutcome {
        let bytes = stream_batch_bytes(&batch);
        let records = match to_broker_owner_replication_records(batch) {
            Ok(records) => records,
            Err(err) => return FollowerApplyOutcome::Error(err.to_string()),
        };
        match self.apply.apply_stream_batch(records).await {
            Ok(ReplicatedStreamApply::Applied {
                message_next,
                event_next,
            }) => FollowerApplyOutcome::Applied {
                durable_message_next: message_next,
                durable_event_next: event_next,
                bytes,
            },
            Ok(ReplicatedStreamApply::CheckpointRequired) => {
                FollowerApplyOutcome::CheckpointRequired
            }
            Err(err) => FollowerApplyOutcome::Error(err.to_string()),
        }
    }
}

#[cfg(test)]
mod stream_transport_tests {
    use super::*;
    use crate::v1::{ReplicationMessageRecord, ReplicationStreamEnd};
    use std::sync::Mutex as StdMutex;
    use tokio::net::TcpListener;
    use tokio_util::codec::Framed;

    struct RecordingSink {
        applied: StdMutex<Vec<(u64, u64)>>, // (message_next, bytes) per batch
    }

    #[async_trait::async_trait]
    impl FollowerStreamSink for RecordingSink {
        async fn apply(&self, batch: ReplicationReadOk) -> FollowerApplyOutcome {
            let bytes = stream_batch_bytes(&batch);
            let (message_next, event_next) = match (&batch.messages, &batch.events) {
                (
                    ReplicationMessageRead::Batch { next_offset: m, .. },
                    ReplicationEventRead::Batch { next_offset: e, .. },
                ) => (*m, *e),
                _ => return FollowerApplyOutcome::CheckpointRequired,
            };
            self.applied.lock().unwrap().push((message_next, bytes));
            FollowerApplyOutcome::Applied {
                durable_message_next: message_next,
                durable_event_next: event_next,
                bytes,
            }
        }
    }

    fn sample_batch(first_offset: u64, count: u64) -> ReplicationReadOk {
        let records = (0..count)
            .map(|i| ReplicationMessageRecord {
                offset: first_offset + i,
                flags: 0,
                headers: Vec::new(),
                payload: vec![7u8; 100],
            })
            .collect();
        ReplicationReadOk {
            messages: ReplicationMessageRead::Batch {
                epoch: 1,
                requested_offset: first_offset,
                next_offset: first_offset + count,
                records,
            },
            events: ReplicationEventRead::Batch {
                epoch: 1,
                requested_offset: 0,
                next_offset: 0,
                records: Vec::new(),
            },
        }
    }

    // Mock owner ↔ real follower transport over a real TCP connection: proves the
    // wire round-trip (Start, batch demux, progress+credit, StreamEnd handling).
    #[tokio::test]
    async fn follower_stream_round_trips_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Mock owner: recv Start, push 3 batches, recv 3 progress, send StreamEnd.
        let owner = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut conn = helper::plain_conn(socket);

            let start_frame = conn.next().await.unwrap().unwrap();
            let start = wire::decode_replication_stream_start(&start_frame).unwrap();
            assert_eq!(start.topic, "orders");
            assert_eq!(start.message_from, 0);
            assert!(start.credit_bytes > 0);
            let stream_id = start_frame.request_id;

            for (first, count) in [(0u64, 2u64), (2, 3), (5, 1)] {
                let frame =
                    wire::encode_replication_stream_batch(stream_id, &sample_batch(first, count))
                        .unwrap();
                conn.send(frame).await.unwrap();
            }

            let mut progress = Vec::new();
            while progress.len() < 3 {
                let frame = conn.next().await.unwrap().unwrap();
                if frame.opcode == Op::ReplicationStreamProgress as u16 {
                    progress.push(wire::decode_replication_stream_progress(&frame).unwrap());
                }
            }

            let end = wire::encode_replication_stream_end(
                stream_id,
                &ReplicationStreamEnd {
                    code: replication_stream::STREAM_END_CLOSED,
                    message: "done".into(),
                },
            )
            .unwrap();
            conn.send(end).await.unwrap();
            progress
        });

        let socket = TcpStream::connect(addr).await.unwrap();
        let conn = helper::plain_conn(socket);
        let sink = Arc::new(RecordingSink {
            applied: StdMutex::new(Vec::new()),
        });
        let exit = run_follower_replication_stream(
            conn,
            sink.clone(),
            "orders".into(),
            Partition::new(0),
            None,
            0,
            0,
            8 * 1024 * 1024,
            // keepalive/linger disabled; max_merge_bytes = 1 is below any batch, so
            // the applier never folds and each batch applies separately. This test
            // verifies the per-batch round-trip over TCP (transport + apply
            // offset/bytes + progress/credit + stream close); its assertions always
            // expected 3 distinct applies. Folding is non-deterministic over a fast
            // loopback (queued frames coalesce) and is covered on its own by the
            // applier unit tests, so we pin it off here rather than racing it.
            Arc::new(|| fibril_broker::replication::StreamApplyTunables {
                keepalive_ms: 0,
                apply_linger_us: 0,
                max_merge_bytes: 1,
            }),
            Some("broker-2".into()),
            42,
            4,
            CancellationToken::new(),
        )
        .await;

        assert_eq!(exit, FollowerStreamExit::Closed);
        let applied = sink.applied.lock().unwrap().clone();
        assert_eq!(
            applied,
            vec![(2, 200), (5, 300), (6, 100)],
            "applied 3 batches with correct next-offset + bytes"
        );

        let progress = owner.await.unwrap();
        // Combined progress+credit: durable advances and credit = bytes applied.
        assert_eq!(progress[0].durable_message_next, 2);
        assert_eq!(progress[0].credit_add_bytes, 200);
        assert_eq!(progress[2].durable_message_next, 6);
        assert_eq!(progress[2].credit_add_bytes, 100);
    }
}
