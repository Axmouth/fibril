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
        BrokerOwnerStreamRecords, BrokerQueuePushApply, FollowerStreamExit, QueuePushApply,
        ReplicationResourceKind,
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
    Auth, ERR_NOT_OWNER, ErrorMsg, Hello, HelloOk, Op, PROTOCOL_V1, QueuePushProgress,
    QueuePushReset, QueuePushStart, QueueReplicationRead, QueueReplicationReadOk,
    ReplicationCheckpointExport, ReplicationCheckpointExportOk, ReplicationEventRead,
    ReplicationMessageRead, StreamReplicationRead, StreamReplicationReadOk,
    frame::Frame,
    helper::{self, Conn, try_decode, try_encode},
    queue_push::{
        self, ApplierExit, FollowerApplyOutcome, FollowerPushControl, FollowerPushSink,
        run_follower_push_applier,
    },
    wire,
};

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
    peers: Mutex<
        HashMap<
            (
                String,
                ReplicationResourceKind,
                Option<fibril_broker::coordination::QueueIdentity>,
            ),
            Arc<ProtocolOwnerReplicationPeer>,
        >,
    >,
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

            let history = assignment
                .history
                .as_ref()
                .map(|history| {
                    let sender = self.cfg.reporter_node_id.as_deref().ok_or_else(|| {
                        BrokerError::InvalidArgument(
                            "history replication requires local replica identity".into(),
                        )
                    })?;
                    history
                        .session(
                            &assignment.queue.topic,
                            assignment.queue.partition,
                            assignment.queue.group.as_deref(),
                            kind == ReplicationResourceKind::Stream,
                            sender,
                            &assignment.owner,
                        )
                        .map_err(BrokerError::InvalidArgument)
                })
                .transpose()?;
            let cache_key = (
                assignment.owner.clone(),
                kind,
                history.as_ref().map(|_| assignment.queue.clone()),
            );
            let mut peers = self.peers.lock().await;
            if let Some(peer) = peers.get(&cache_key) {
                if peer.history == history {
                    let peer: Arc<dyn BrokerOwnerReplicationPeer> = peer.clone();
                    return Ok(Some(peer));
                }
            }

            let mut built = ProtocolOwnerReplicationPeer::new_reconnecting(
                addr,
                self.cfg.auth.clone(),
                self.cfg.client_name.clone(),
                self.cfg.client_version.clone(),
            )
            .with_read_timeout_slack_ms(self.cfg.read_timeout_slack_ms)
            .with_owner_connect_timeout_ms(self.cfg.owner_connect_timeout_ms);
            built.history = history;
            if let Some(reporter) = &self.cfg.reporter_node_id {
                built = built.with_reporter(reporter.clone());
            }
            if let Some(tls) = &self.cfg.tls {
                built = built.with_peer_tls(tls.clone());
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
    peers: Mutex<
        HashMap<
            (
                String,
                ReplicationResourceKind,
                Option<fibril_broker::coordination::QueueIdentity>,
            ),
            CachedProtocolOwnerPeer,
        >,
    >,
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
            if assignment.history.is_some()
                && self.cfg.reporter_node_id.as_deref() != Some(self.coordination.node_id())
            {
                return Err(BrokerError::InvalidArgument(
                    "history reporter must identify the local coordination node".into(),
                ));
            }
            let history = assignment
                .history
                .as_ref()
                .map(|history| {
                    let sender = self.cfg.reporter_node_id.as_deref().ok_or_else(|| {
                        BrokerError::InvalidArgument(
                            "history replication requires local replica identity".into(),
                        )
                    })?;
                    history
                        .session(
                            &assignment.queue.topic,
                            assignment.queue.partition,
                            assignment.queue.group.as_deref(),
                            kind == ReplicationResourceKind::Stream,
                            sender,
                            &assignment.owner,
                        )
                        .map_err(BrokerError::InvalidArgument)
                })
                .transpose()?;
            let cache_key = (
                assignment.owner.clone(),
                kind,
                history.as_ref().map(|_| assignment.queue.clone()),
            );
            let snapshot = self.coordination.snapshot();
            let Some(node) = snapshot.nodes.get(&assignment.owner) else {
                self.peers.lock().await.remove(&cache_key);
                return Ok(None);
            };
            let addr = node.broker_addr.clone();

            let mut peers = self.peers.lock().await;
            if let Some(cached) = peers.get(&cache_key) {
                if cached.addr == addr && cached.peer.history == history {
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
            built.history = history;
            if let Some(reporter) = &self.cfg.reporter_node_id {
                built = built.with_reporter(reporter.clone());
            }
            if let Some(tls) = &self.cfg.tls {
                built = built.with_peer_tls(tls.clone());
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

pub(super) async fn open_protocol_owner_conn(
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
    history: Option<fibril_broker::history_replication::HistoryReplicationSession>,
    conn: Mutex<Option<Conn>>,
    request_lock: Mutex<()>,
    next_request_id: AtomicU64,
    reconnect: Option<ProtocolOwnerPeerConnectConfig>,
    close_token: CancellationToken,
    /// Stamped into replication reads for owner-side progress tracking.
    reporter_node_id: Option<String>,
    /// Added to a read's `max_wait_ms` to bound how long the follower waits for
    /// the owner's response before treating the connection as dead.
    read_timeout_slack_ms: u64,
}

impl ProtocolOwnerReplicationPeer {
    pub fn with_history_session(
        mut self,
        history: fibril_broker::history_replication::HistoryReplicationSession,
    ) -> Self {
        self.history = Some(history);
        self
    }

    fn history_frame(&self, frame: Frame) -> Result<Frame, BrokerError> {
        match &self.history {
            Some(history) => {
                crate::v1::history_replication::encode_frame(history, frame).map_err(protocol_error)
            }
            None => Ok(frame),
        }
    }

    pub fn new(conn: Conn) -> Self {
        Self {
            history: None,
            conn: Mutex::new(Some(conn)),
            request_lock: Mutex::new(()),
            next_request_id: AtomicU64::new(20_000),
            reconnect: None,
            close_token: CancellationToken::new(),
            reporter_node_id: None,
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

    pub fn new_reconnecting(
        addr: String,
        auth: Option<ProtocolOwnerPeerAuth>,
        client_name: String,
        client_version: String,
    ) -> Self {
        Self {
            history: None,
            conn: Mutex::new(None),
            request_lock: Mutex::new(()),
            next_request_id: AtomicU64::new(20_000),
            reporter_node_id: None,
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

impl ProtocolOwnerReplicationPeer {
    /// Send one pull read and wait for its reply, bounded by the long-poll
    /// window plus slack.
    async fn request_replication_read<T>(
        &self,
        request: wire::WireResult<Frame>,
        reply_op: Op,
        decode: fn(&Frame) -> wire::WireResult<T>,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        max_wait_ms: u32,
    ) -> Result<T, BrokerError> {
        let request = request.map_err(protocol_error)?;
        let _request_guard = tokio::select! {
            biased;
            _ = self.close_token.cancelled() => {
                return Err(BrokerError::Unknown("protocol owner peer closed".into()));
            }
            guard = self.request_lock.lock() => guard,
        };
        let request_id = self.next_request_id();
        let mut request = request;
        request.request_id = request_id;
        let mut conn = self.take_conn().await?;
        if let Err(err) = conn.send(self.history_frame(request)?).await {
            return Err(BrokerError::Unknown(format!(
                "replication read send failed: {err}"
            )));
        }

        let recv = async {
            let frame = recv_response_frame(&mut conn, request_id, reply_op).await?;
            decode(&frame).map_err(|err| ProtocolReplicationRequestError::Decode(err.to_string()))
        };
        // Bound the wait: the owner long-polls up to `max_wait_ms`, so anything
        // past that plus slack means the response was lost (for example a
        // partition that dropped it). Give up and drop the connection so the
        // worker reconnects instead of hanging on a dead socket.
        let read_deadline_ms = max_wait_ms as u64 + self.read_timeout_slack_ms;
        let read = match tokio::select! {
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
        Ok(read)
    }
}

impl BrokerOwnerReplicationPeer for ProtocolOwnerReplicationPeer {
    fn read_owner_queue_records<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        from: Offset,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> futures::future::BoxFuture<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>> {
        self.read_owner_queue_records_fenced(
            topic,
            partition,
            group,
            from,
            max_records,
            max_bytes,
            max_wait_ms,
            None,
        )
    }

    fn read_owner_queue_records_fenced<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        from: Offset,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
        reporter_epoch: Option<u64>,
    ) -> futures::future::BoxFuture<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>> {
        Box::pin(async move {
            let max_records = u32::try_from(max_records).map_err(|_| {
                BrokerError::InvalidArgument("max_records exceeds protocol limit".into())
            })?;
            let max_bytes = u64::try_from(max_bytes).map_err(|_| {
                BrokerError::InvalidArgument("max_bytes exceeds protocol limit".into())
            })?;
            let max_wait_ms = u32::try_from(max_wait_ms).map_err(|_| {
                BrokerError::InvalidArgument("max_wait_ms exceeds protocol limit".into())
            })?;
            let request = QueueReplicationRead {
                topic: topic.to_string(),
                group: group.map(str::to_string),
                partition,
                from,
                max_records,
                max_bytes,
                max_wait_ms,
                reporter_node_id: self.reporter_node_id.clone(),
                reporter_epoch,
            };
            let read = self
                .request_replication_read(
                    wire::encode_queue_replication_read(0, &request),
                    Op::QueueReplicationReadOk,
                    wire::decode_queue_replication_read_ok,
                    topic,
                    partition,
                    group,
                    max_wait_ms,
                )
                .await?;
            to_broker_event_read(read)
        })
    }

    fn read_owner_stream_records<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> futures::future::BoxFuture<'a, Result<BrokerOwnerStreamRecords, BrokerError>> {
        self.read_owner_stream_records_fenced(
            topic,
            partition,
            message_from,
            event_from,
            max_messages,
            max_events,
            max_bytes,
            max_wait_ms,
            None,
        )
    }

    fn read_owner_stream_records_fenced<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
        max_wait_ms: u64,
        reporter_epoch: Option<u64>,
    ) -> futures::future::BoxFuture<'a, Result<BrokerOwnerStreamRecords, BrokerError>> {
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
            let request = StreamReplicationRead {
                reporter_epoch,
                topic: topic.to_string(),
                group: None,
                partition,
                message_from,
                event_from,
                max_messages,
                max_events,
                max_bytes,
                max_wait_ms,
                reporter_node_id: self.reporter_node_id.clone(),
            };
            let read = self
                .request_replication_read(
                    wire::encode_stream_replication_read(0, &request),
                    Op::StreamReplicationReadOk,
                    wire::decode_stream_replication_read_ok,
                    topic,
                    partition,
                    None,
                    max_wait_ms,
                )
                .await?;
            to_broker_stream_records(read)
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
                    self.history_frame(
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
                    )?,
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
                combined_log: export.checkpoint.combined_log,
                event_start_offset: export.checkpoint.event_start_offset,
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

    fn push_queue_replication<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        from: Offset,
        credit_bytes: u64,
        tunables: StreamApplyTunablesFn,
        buffer_batches: usize,
        apply: Arc<dyn BrokerQueuePushApply>,
        shutdown: CancellationToken,
    ) -> futures::future::BoxFuture<'a, Result<FollowerStreamExit, BrokerError>> {
        self.push_queue_replication_fenced(
            topic,
            partition,
            group,
            from,
            credit_bytes,
            tunables,
            buffer_batches,
            apply,
            shutdown,
            None,
        )
    }

    fn push_queue_replication_fenced<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        from: Offset,
        credit_bytes: u64,
        tunables: StreamApplyTunablesFn,
        buffer_batches: usize,
        apply: Arc<dyn BrokerQueuePushApply>,
        shutdown: CancellationToken,
        reporter_epoch: Option<u64>,
    ) -> futures::future::BoxFuture<'a, Result<FollowerStreamExit, BrokerError>> {
        Box::pin(async move {
            // The push owns a connection for its lifetime (no concurrent
            // request/response on it). take_conn reconnects if needed.
            let conn = self.take_conn().await?;
            let stream_id = self.next_request_id();
            let sink = Arc::new(QueuePushSink { apply });
            let exit = run_follower_queue_push_with_history(
                conn,
                sink,
                topic.to_string(),
                partition,
                group.map(str::to_string),
                from,
                credit_bytes,
                tunables,
                self.reporter_node_id.clone(),
                reporter_epoch,
                stream_id,
                buffer_batches,
                shutdown,
                self.history.clone(),
            )
            .await;
            // The connection is consumed/closed by the transport on exit; the
            // next take_conn reconnects.
            Ok(exit)
        })
    }
}
pub(super) async fn recv_response<T>(
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
    // Re-bench the actual QueueReplicationReadOk many-records case before raising further.
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

pub(super) fn protocol_error(err: impl std::fmt::Display) -> BrokerError {
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

fn to_broker_stream_records(read: StreamReplicationReadOk) -> Result<BrokerOwnerStreamRecords, BrokerError> {
    Ok(BrokerOwnerStreamRecords {
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

/// Decode one log read: a queue log reply, or a stream's cursor-event lane.
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
// Follower side: credit-based queue push transport.
// ===========================================================================

/// Run one follower queue push over an established connection: send
/// `QueuePushStart`, then a reader demuxes pushed `QueuePushBatch` frames into
/// a bounded in-order buffer while an applier drains it (durably, in order)
/// and sends combined progress+credit back. Returns why the push ended.
/// `buffer_batches` bounds the in-flight buffer; the granted `credit_bytes`
/// bounds in-flight bytes.
pub async fn run_follower_queue_push<S: FollowerPushSink>(
    conn: Conn,
    sink: Arc<S>,
    topic: String,
    partition: Partition,
    group: Option<String>,
    from: Offset,
    credit_bytes: u64,
    tunables: StreamApplyTunablesFn,
    reporter_node_id: Option<String>,
    reporter_epoch: Option<u64>,
    push_id: u64,
    buffer_batches: usize,
    shutdown: CancellationToken,
) -> FollowerStreamExit {
    run_follower_queue_push_with_history(
        conn,
        sink,
        topic,
        partition,
        group,
        from,
        credit_bytes,
        tunables,
        reporter_node_id,
        reporter_epoch,
        push_id,
        buffer_batches,
        shutdown,
        None,
    )
    .await
}

async fn run_follower_queue_push_with_history<S: FollowerPushSink>(
    conn: Conn,
    sink: Arc<S>,
    topic: String,
    partition: Partition,
    group: Option<String>,
    from: Offset,
    credit_bytes: u64,
    tunables: StreamApplyTunablesFn,
    reporter_node_id: Option<String>,
    reporter_epoch: Option<u64>,
    push_id: u64,
    buffer_batches: usize,
    shutdown: CancellationToken,
    history: Option<fibril_broker::history_replication::HistoryReplicationSession>,
) -> FollowerStreamExit {
    let (mut conn_sink, mut conn_stream) = conn.split();

    let start = QueuePushStart {
        topic,
        group,
        partition,
        from,
        credit_bytes,
        reporter_node_id,
        reporter_epoch,
    };
    match wire::encode_queue_push_start(push_id, &start).and_then(|frame| match &history {
        Some(history) => crate::v1::history_replication::encode_frame(history, frame),
        None => Ok(frame),
    }) {
        Ok(frame) => {
            if conn_sink.send(frame).await.is_err() {
                return FollowerStreamExit::Error("failed to send push start".into());
            }
        }
        Err(err) => return FollowerStreamExit::Error(err.to_string()),
    }

    let (batch_tx, batch_rx) = mpsc::channel::<QueueReplicationReadOk>(buffer_batches.max(1));
    let (control_tx, mut control_rx) = mpsc::channel::<FollowerPushControl>(64);

    // Reader: demux pushed frames into the buffer; return the push-end reason.
    let reader = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        loop {
            match conn_stream.next().await {
                Some(Ok(frame)) => {
                    if frame.request_id != push_id {
                        continue;
                    }
                    if frame.opcode == Op::Error as u16 {
                        return FollowerStreamExit::Error(
                            try_decode::<ErrorMsg>(&frame)
                                .map(|e| e.message)
                                .unwrap_or_else(|e| format!("invalid push error: {e}")),
                        );
                    }
                    if frame.opcode == Op::QueuePushBatch as u16 {
                        match wire::decode_queue_push_batch(&frame) {
                            Ok(batch) => {
                                if batch_tx.send(batch).await.is_err() {
                                    return FollowerStreamExit::Closed;
                                }
                            }
                            Err(err) => {
                                return FollowerStreamExit::Error(format!(
                                    "decode push batch: {err}"
                                ));
                            }
                        }
                    } else if frame.opcode == Op::QueuePushEnd as u16 {
                        let code = wire::decode_queue_push_end(&frame)
                            .map(|end| end.code)
                            .unwrap_or(queue_push::PUSH_END_ERROR);
                        return if code == queue_push::PUSH_END_CHECKPOINT_REQUIRED {
                            FollowerStreamExit::CheckpointRequired
                        } else if code == queue_push::PUSH_END_CLOSED {
                            FollowerStreamExit::Closed
                        } else {
                            FollowerStreamExit::Error("owner ended push".into())
                        };
                    }
                    // ignore unrelated opcodes on this connection
                }
                Some(Err(err)) => return FollowerStreamExit::Error(format!("push recv: {err}")),
                None => return FollowerStreamExit::Error("connection closed".into()),
            }
        }
    }));

    // Control writer: encode progress/reset back to the owner.
    let writer = tokio_util::task::AbortOnDropHandle::new(tokio::spawn(async move {
        while let Some(control) = control_rx.recv().await {
            let frame = match control {
                FollowerPushControl::Progress {
                    durable_next,
                    credit_add_bytes,
                } => wire::encode_queue_push_progress(
                    push_id,
                    &QueuePushProgress {
                        durable_next,
                        credit_add_bytes,
                    },
                ),
                FollowerPushControl::Reset { from } => {
                    wire::encode_queue_push_reset(push_id, &QueuePushReset { from })
                }
            };
            let frame = frame.and_then(|frame| match &history {
                Some(history) => crate::v1::history_replication::encode_frame(history, frame),
                None => Ok(frame),
            });
            match frame {
                Ok(frame) => {
                    if conn_sink.send(frame).await.is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    }));

    let applier_exit = tokio::select! {
        _ = shutdown.cancelled() => None,
        exit = run_follower_push_applier(
            sink,
            batch_rx,
            control_tx,
            from,
            tunables,
        ) => Some(exit),
    };

    let exit = match applier_exit {
        // The applier stopped because the reader closed the buffer; the reader
        // knows why (push end code or connection error).
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

/// Adapts a broker-side [`BrokerQueuePushApply`] to the protocol
/// [`FollowerPushSink`] the transport drives. Decodes each pushed batch,
/// computes the credit (bytes) to return, and maps the broker outcome back to
/// push progress.
struct QueuePushSink {
    apply: Arc<dyn BrokerQueuePushApply>,
}

fn queue_push_outcome(outcome: Result<QueuePushApply, BrokerError>, bytes: u64) -> FollowerApplyOutcome {
    match outcome {
        Ok(QueuePushApply::Applied { next }) => FollowerApplyOutcome::Applied {
            durable_next: next,
            bytes,
        },
        Ok(QueuePushApply::CheckpointRequired) => FollowerApplyOutcome::CheckpointRequired,
        Err(err) => FollowerApplyOutcome::Error(err.to_string()),
    }
}

#[async_trait::async_trait]
impl FollowerPushSink for QueuePushSink {
    async fn apply_open(
        &self,
        frames: futures::stream::BoxStream<'_, QueueReplicationReadOk>,
    ) -> FollowerApplyOutcome {
        let mut bytes = 0u64;
        let records = frames
            .map(|frame| {
                bytes = bytes.saturating_add(queue_push::batch_bytes(&frame));
                to_broker_event_read(frame)
            })
            .boxed();
        let outcome = self.apply.apply_open_queue_push(records).await;
        queue_push_outcome(outcome, bytes)
    }
    async fn apply(&self, batch: QueueReplicationReadOk) -> FollowerApplyOutcome {
        let bytes = queue_push::batch_bytes(&batch);
        let records = match to_broker_event_read(batch) {
            Ok(records) => records,
            Err(err) => return FollowerApplyOutcome::Error(err.to_string()),
        };
        queue_push_outcome(self.apply.apply_queue_push(records).await, bytes)
    }
}

#[cfg(test)]
mod queue_read_tests {
    use super::*;
    use crate::v1::{ReplicationCheckpointRequired, ReplicationEventRecord};

    #[test]
    fn queue_reply_decodes_log_records_and_checkpoint_requirements() {
        let event = StromaEvent::Ack { off: 4 }.encode().unwrap();
        let reply: QueueReplicationReadOk = ReplicationEventRead::Batch {
            epoch: 3,
            requested_offset: 5,
            next_offset: 6,
            records: vec![ReplicationEventRecord { offset: 5, payload: event }],
        };
        let OwnerReplicationRead::Batch(batch) = to_broker_event_read(reply).unwrap() else {
            panic!("queue batch")
        };
        assert_eq!((batch.epoch, batch.requested_offset, batch.next_offset), (3, 5, 6));
        assert_eq!(batch.records, vec![(5, StromaEvent::Ack { off: 4 })]);

        let required: QueueReplicationReadOk =
            ReplicationEventRead::CheckpointRequired(ReplicationCheckpointRequired {
                epoch: 7,
                requested_offset: 2,
                head_offset: 9,
                next_offset: 12,
            });
        assert_eq!(
            to_broker_event_read(required).unwrap(),
            OwnerReplicationRead::CheckpointRequired {
                epoch: 7,
                requested_offset: 2,
                head_offset: 9,
                next_offset: 12,
            }
        );

        // A record that is not a Stroma event is refused before apply.
        let garbage: QueueReplicationReadOk = ReplicationEventRead::Batch {
            epoch: 3,
            requested_offset: 5,
            next_offset: 6,
            records: vec![ReplicationEventRecord {
                offset: 5,
                payload: vec![0xff; 3],
            }],
        };
        assert!(to_broker_event_read(garbage).is_err());
    }
}

#[cfg(test)]
mod push_transport_tests {
    use super::*;
    use crate::v1::{QueuePushEnd, ReplicationEventRecord};
    use std::sync::Mutex as StdMutex;
    use tokio::net::TcpListener;

    struct RecordingSink {
        applied: StdMutex<Vec<(u64, u64)>>, // (next, bytes) per batch
    }

    #[async_trait::async_trait]
    impl FollowerPushSink for RecordingSink {
        async fn apply(&self, batch: QueueReplicationReadOk) -> FollowerApplyOutcome {
            let bytes = queue_push::batch_bytes(&batch);
            let ReplicationEventRead::Batch { next_offset, .. } = batch else {
                return FollowerApplyOutcome::CheckpointRequired;
            };
            self.applied.lock().unwrap().push((next_offset, bytes));
            FollowerApplyOutcome::Applied {
                durable_next: next_offset,
                bytes,
            }
        }
    }

    fn sample_batch(first_offset: u64, count: u64) -> QueueReplicationReadOk {
        let records = (0..count)
            .map(|i| ReplicationEventRecord {
                offset: first_offset + i,
                payload: vec![7u8; 100],
            })
            .collect();
        ReplicationEventRead::Batch {
            epoch: 1,
            requested_offset: first_offset,
            next_offset: first_offset + count,
            records,
        }
    }

    #[tokio::test]
    async fn push_rejection_exits_and_parent_cancellation_closes_transport_tasks() {
        for cancel in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let socket = TcpStream::connect(listener.local_addr().unwrap())
                .await
                .unwrap();
            let (server, _) = listener.accept().await.unwrap();
            let mut owner = helper::plain_conn(server);
            let sink = Arc::new(RecordingSink {
                applied: StdMutex::new(Vec::new()),
            });
            let follower = tokio::spawn(run_follower_queue_push(
                helper::plain_conn(socket),
                sink.clone(),
                "orders".into(),
                Partition::new(0),
                None,
                0,
                1024,
                Arc::new(|| fibril_broker::replication::StreamApplyTunables {
                    keepalive_ms: 0,
                    apply_linger_us: 0,
                    max_merge_bytes: 1,
                }),
                Some("b".into()),
                Some(1),
                42,
                4,
                CancellationToken::new(),
            ));
            let start = owner.next().await.unwrap().unwrap();
            assert_eq!(start.opcode, Op::QueuePushStart as u16);
            if cancel {
                follower.abort();
                assert!(follower.await.unwrap_err().is_cancelled());
            } else {
                owner
                    .send(
                        try_encode(
                            Op::Error,
                            42,
                            &ErrorMsg {
                                code: 400,
                                message: "history replaced".into(),
                            },
                        )
                        .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    tokio::time::timeout(std::time::Duration::from_secs(5), follower)
                        .await
                        .unwrap()
                        .unwrap(),
                    FollowerStreamExit::Error("history replaced".into())
                );
            }
            assert!(
                tokio::time::timeout(std::time::Duration::from_secs(5), owner.next())
                    .await
                    .unwrap()
                    .is_none(),
                "both transport tasks must release their socket halves"
            );
            assert!(sink.applied.lock().unwrap().is_empty());
        }
    }

    // Mock owner and real follower transport over a real TCP connection: proves
    // the wire round-trip (start, batch demux, progress+credit, push end).
    #[tokio::test]
    async fn follower_push_round_trips_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Mock owner: recv start, push 3 batches, recv 3 progress, send end.
        let owner = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut conn = helper::plain_conn(socket);

            let start_frame = conn.next().await.unwrap().unwrap();
            let start = wire::decode_queue_push_start(&start_frame).unwrap();
            assert_eq!(start.topic, "orders");
            assert_eq!(start.from, 0);
            assert!(start.credit_bytes > 0);
            let push_id = start_frame.request_id;

            for (first, count) in [(0u64, 2u64), (2, 3), (5, 1)] {
                let frame =
                    wire::encode_queue_push_batch(push_id, &sample_batch(first, count)).unwrap();
                conn.send(frame).await.unwrap();
            }

            let mut progress = Vec::new();
            while progress.len() < 3 {
                let frame = conn.next().await.unwrap().unwrap();
                if frame.opcode == Op::QueuePushProgress as u16 {
                    progress.push(wire::decode_queue_push_progress(&frame).unwrap());
                }
            }

            let end = wire::encode_queue_push_end(
                push_id,
                &QueuePushEnd {
                    code: queue_push::PUSH_END_CLOSED,
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
        let exit = run_follower_queue_push(
            conn,
            sink.clone(),
            "orders".into(),
            Partition::new(0),
            None,
            0,
            8 * 1024 * 1024,
            // keepalive/linger disabled; max_merge_bytes = 1 is below any batch, so
            // the applier never folds and each batch applies separately. This test
            // verifies the per-batch round-trip over TCP (transport + apply
            // offset/bytes + progress/credit + push close); its assertions always
            // expected 3 distinct applies. Folding is non-deterministic over a fast
            // loopback (queued frames coalesce) and is covered on its own by the
            // applier unit tests, so we pin it off here rather than racing it.
            Arc::new(|| fibril_broker::replication::StreamApplyTunables {
                keepalive_ms: 0,
                apply_linger_us: 0,
                max_merge_bytes: 1,
            }),
            Some("broker-2".into()),
            Some(1),
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
        assert_eq!(progress[0].durable_next, 2);
        assert_eq!(progress[0].credit_add_bytes, 200);
        assert_eq!(progress[2].durable_next, 6);
        assert_eq!(progress[2].credit_add_bytes, 100);
    }
}

/// Explicit recovery control call on a fresh authenticated connection. The
/// target ID must come from the persisted old replica set; the receiver verifies
/// the transition through consensus. No automatic controller calls this yet.
///
/// The deadline covers connection setup and the reply. Timeout/cancellation can
/// leave an admitted seal running remotely; retry the identical command.
pub async fn request_recovery_seal(
    config: &ProtocolOwnerPeerResolverConfig,
    replica_id: &str,
    command: &fibril_broker::recovery::RecoverySealCommand,
    deadline: std::time::Duration,
) -> Result<fibril_broker::recovery::BrokerSealedReplica, BrokerError> {
    let addr = config.nodes.get(replica_id).ok_or_else(|| {
        BrokerError::InvalidArgument("recovery replica has no configured address".into())
    })?;
    let operation = async {
        let mut conn = open_protocol_owner_conn(
            addr.clone(),
            config.auth.as_ref(),
            config.tls.as_ref(),
            &config.client_name,
            &config.client_version,
            config.owner_connect_timeout_ms,
        )
        .await?;
        let request = crate::v1::RecoverySeal {
            topic: command.topic.clone(),
            partition: command.partition,
            group: command.group.clone(),
            stream: command.stream,
            transition: command.transition,
            fence_epoch: command.fence_epoch,
        };
        conn.send(try_encode(Op::RecoverySeal, 3, &request).map_err(protocol_error)?)
            .await
            .map_err(|err| BrokerError::Unknown(format!("recovery request send failed: {err}")))?;
        let reply: crate::v1::RecoverySealOk = recv_response(&mut conn, 3, Op::RecoverySealOk)
            .await
            .map_err(|err| BrokerError::Unknown(format!("recovery seal failed: {err}")))?;
        validate_recovery_seal_reply(replica_id, command, reply)
    };
    tokio::time::timeout(deadline, operation)
        .await
        .map_err(|_| {
            BrokerError::Unknown(
                "recovery seal deadline elapsed; retry the identical request".into(),
            )
        })?
}

fn validate_recovery_seal_reply(
    replica_id: &str,
    command: &fibril_broker::recovery::RecoverySealCommand,
    reply: crate::v1::RecoverySealOk,
) -> Result<fibril_broker::recovery::BrokerSealedReplica, BrokerError> {
    use fibril_broker::recovery::{
        BrokerSealedReplica, RecoverySealRequest, RetainedHistoryIdentity, SealedReplicaFrontiers,
    };
    if reply.replica_id != replica_id
        || reply.transition != command.transition
        || reply.fence_epoch != command.fence_epoch
        || !matches!(
            (reply.history_version, reply.storage_history.is_some()),
            (1, false) | (2, true)
        )
        || (reply.combined_log && (command.stream || reply.history_version != 2 || reply.message_head != 0 || reply.message_next != 0))
        || reply.message_head > reply.message_next
        || reply.event_head > reply.event_next
    {
        return Err(BrokerError::InvalidArgument(
            "recovery reply identity, transition or bounds mismatch".into(),
        ));
    }
    Ok(BrokerSealedReplica {
        node_id: reply.replica_id,
        seal: SealedReplicaFrontiers {
            request: RecoverySealRequest {
                transition: reply.transition,
                fence_epoch: reply.fence_epoch,
            },
            history: RetainedHistoryIdentity {
                log_layout: if reply.combined_log {
                    fibril_broker::recovery::QueueLogLayout::Combined
                } else {
                    fibril_broker::recovery::QueueLogLayout::Separate
                },
                version: reply.history_version,
                storage_history: reply.storage_history.map(|h| {
                    fibril_broker::queue_engine::PreparedStorageHistory {
                        topic: command.topic.clone(),
                        partition: command.partition.id(),
                        group: command.group.clone(),
                        stream: command.stream,
                        binding: fibril_broker::queue_engine::StorageHistoryBinding {
                            resource_incarnation: h.resource_incarnation,
                            accepted_history: h.accepted_history,
                            writer_session: h.writer_session,
                        },
                        storage_instance: h.storage_instance,
                    }
                }),
                id: reply.history_id,
                message_digest: reply.message_digest,
                event_digest: reply.event_digest,
                snapshot_digest: reply.snapshot_digest,
                message_head: reply.message_head,
                message_next: reply.message_next,
                event_head: reply.event_head,
                event_next: reply.event_next,
            },
            message_head: reply.message_head,
            message_next: reply.message_next,
            event_head: reply.event_head,
            event_next: reply.event_next,
        },
    })
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::v1::RecoverySealOk;
    use fibril_broker::recovery::RecoverySealCommand;

    fn command() -> RecoverySealCommand {
        RecoverySealCommand {
            topic: "q".into(),
            partition: Partition::new(0),
            group: None,
            stream: false,
            transition: [1; 32],
            fence_epoch: 8,
        }
    }

    #[test]
    fn recovery_reply_must_match_target_transition_version_and_bounds() {
        let reply = RecoverySealOk {
            combined_log: false,
            storage_history: None,
            replica_id: "b".into(),
            transition: [1; 32],
            fence_epoch: 8,
            history_version: 1,
            history_id: [2; 32],
            message_digest: [3; 32],
            event_digest: [4; 32],
            snapshot_digest: None,
            message_head: 0,
            message_next: 0,
            event_head: 0,
            event_next: 0,
        };
        assert!(validate_recovery_seal_reply("b", &command(), reply.clone()).is_ok());
        let mut combined = reply.clone();
        combined.combined_log = true;
        assert!(validate_recovery_seal_reply("b", &command(), combined.clone()).is_err());
        combined.history_version = 2;
        combined.storage_history = Some(crate::v1::RecoveryStorageHistory {
            resource_incarnation: [5; 16], accepted_history: [6; 16],
            writer_session: [7; 16], storage_instance: [8; 16],
        });
        let recovered = validate_recovery_seal_reply("b", &command(), combined.clone()).unwrap();
        assert_eq!(recovered.seal.history.log_layout, fibril_broker::recovery::QueueLogLayout::Combined);
        let mut stream = command();
        stream.stream = true;
        assert!(validate_recovery_seal_reply("b", &stream, combined.clone()).is_err());
        combined.message_next = 1;
        assert!(validate_recovery_seal_reply("b", &command(), combined).is_err());
        for mutation in 0..6 {
            let mut bad = reply.clone();
            match mutation {
                0 => bad.replica_id = "c".into(),
                1 => bad.transition[0] ^= 1,
                2 => bad.fence_epoch += 1,
                3 => bad.history_version += 1,
                4 => bad.message_head = 1,
                _ => bad.event_head = 1,
            }
            assert!(validate_recovery_seal_reply("b", &command(), bad).is_err());
        }
    }

    #[tokio::test]
    async fn recovery_deadline_bounds_a_peer_that_accepts_but_never_replies() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let config =
            ProtocolOwnerPeerResolverConfig::new(HashMap::from([("b".into(), addr.to_string())]))
                .with_auth("@node", "secret");
        let (release, hold) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.unwrap();
            let _ = hold.await;
        });
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            request_recovery_seal(
                &config,
                "b",
                &command(),
                std::time::Duration::from_millis(100),
            ),
        )
        .await
        .expect("whole-call deadline must bound handshake too");
        assert!(result.unwrap_err().to_string().contains("deadline elapsed"));
        let _ = release.send(());
        server.await.unwrap();
    }
}

/// Read a bounded page from the exact seal already collected from this replica.
/// Remote work retains its read slot until disk verification finishes, even if
/// this call times out. No automatic recovery dispatcher is enabled.
pub async fn request_recovery_read(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &fibril_broker::recovery::RecoverySealCommand,
    sealed: &fibril_broker::recovery::BrokerSealedReplica,
    request: &fibril_broker::recovery::RecoveryReadRequest,
    deadline: std::time::Duration,
) -> Result<crate::v1::RecoveryReadOk, BrokerError> {
    request_recovery_read_reusing(config, command, sealed, request, deadline, &mut None).await
}

/// The caller scopes this slot to one immutable source/seal and inspection.
/// Take ownership before awaiting: cancellation, timeout or any reply error drops
/// the socket. Only a fully consumed and identity-validated response returns it.
pub(super) async fn request_recovery_read_reusing(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &fibril_broker::recovery::RecoverySealCommand,
    sealed: &fibril_broker::recovery::BrokerSealedReplica,
    request: &fibril_broker::recovery::RecoveryReadRequest,
    deadline: std::time::Duration,
    idle: &mut Option<Conn>,
) -> Result<crate::v1::RecoveryReadOk, BrokerError> {
    request_recovery_read_mode(config, command, sealed, request, deadline, idle, false).await
}

pub(super) async fn request_recovery_read_sequential_reusing(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &fibril_broker::recovery::RecoverySealCommand,
    sealed: &fibril_broker::recovery::BrokerSealedReplica,
    request: &fibril_broker::recovery::RecoveryReadRequest,
    deadline: std::time::Duration,
    idle: &mut Option<Conn>,
) -> Result<crate::v1::RecoveryReadOk, BrokerError> {
    request_recovery_read_mode(config, command, sealed, request, deadline, idle, true).await
}

async fn request_recovery_read_mode(
    config: &ProtocolOwnerPeerResolverConfig,
    command: &fibril_broker::recovery::RecoverySealCommand,
    sealed: &fibril_broker::recovery::BrokerSealedReplica,
    request: &fibril_broker::recovery::RecoveryReadRequest,
    deadline: std::time::Duration,
    idle: &mut Option<Conn>,
    sequential: bool,
) -> Result<crate::v1::RecoveryReadOk, BrokerError> {
    let cached = idle.take();
    use fibril_broker::recovery::RecoveryReadSource;
    if request.seal != sealed.seal.request
        || request.history_id != sealed.seal.history.id
        || command.transition != request.seal.transition
        || command.fence_epoch != request.seal.fence_epoch
        || request.max_bytes == 0
        || request.max_bytes > 16 * 1024 * 1024
        || request.max_records == 0
        || request.max_records > 4096
    {
        return Err(BrokerError::InvalidArgument(
            "recovery read does not match collected seal or budget".into(),
        ));
    }
    let addr = config.nodes.get(&sealed.node_id).ok_or_else(|| {
        BrokerError::InvalidArgument("recovery source has no configured address".into())
    })?;
    let wire_request = crate::v1::RecoveryRead {
        seal: crate::v1::RecoverySeal {
            topic: command.topic.clone(),
            partition: command.partition,
            group: command.group.clone(),
            stream: command.stream,
            transition: command.transition,
            fence_epoch: command.fence_epoch,
        },
        history_id: request.history_id,
        source: match request.source {
            RecoveryReadSource::Messages => 0,
            RecoveryReadSource::Events => 1,
            RecoveryReadSource::Snapshot => 2,
        },
        from: request.from,
        max_records: request.max_records,
        max_bytes: request.max_bytes,
    };
    let operation = async {
        let mut conn = match cached {
            Some(conn) => conn,
            None => {
                open_protocol_owner_conn(
                    addr.clone(),
                    config.auth.as_ref(),
                    config.tls.as_ref(),
                    &config.client_name,
                    &config.client_version,
                    config.owner_connect_timeout_ms,
                )
                .await?
            }
        };
        conn.send(
            try_encode(
                if sequential {
                    Op::RecoveryReadSequential
                } else {
                    Op::RecoveryRead
                },
                3,
                &wire_request,
            )
            .map_err(protocol_error)?,
        )
        .await
        .map_err(|err| BrokerError::Unknown(format!("recovery read send failed: {err}")))?;
        let reply: crate::v1::RecoveryReadOk = recv_response(&mut conn, 3, Op::RecoveryReadOk)
            .await
            .map_err(|err| BrokerError::Unknown(format!("recovery read failed: {err}")))?;
        validate_recovery_read_reply(&sealed.node_id, &wire_request, &sealed.seal.history, &reply)?;
        *idle = Some(conn);
        Ok(reply)
    };
    tokio::time::timeout(deadline, operation)
        .await
        .map_err(|_| BrokerError::Unknown("recovery read deadline elapsed".into()))?
}

fn validate_recovery_read_reply(
    node: &str,
    request: &crate::v1::RecoveryRead,
    history: &fibril_broker::recovery::RetainedHistoryIdentity,
    reply: &crate::v1::RecoveryReadOk,
) -> Result<(), BrokerError> {
    let invalid =
        || BrokerError::InvalidArgument("recovery page identity, range or budget mismatch".into());
    if reply.replica_id != node
        || reply.transition != request.seal.transition
        || reply.fence_epoch != request.seal.fence_epoch
        || reply.history_id != history.id
        || reply.history_id != request.history_id
        || reply.source != request.source
        || reply.from != request.from
        || reply.next < reply.from
        || reply.next > reply.end
        || reply.next == reply.from && reply.next != reply.end
    {
        return Err(invalid());
    }
    if reply.source == 2 {
        if !reply.records.is_empty()
            || reply.snapshot_bytes.len() > request.max_bytes as usize
            || reply.next - reply.from != reply.snapshot_bytes.len() as u64
        {
            return Err(invalid());
        }
    } else {
        let (head, end) = if reply.source == 0 {
            (history.message_head, history.message_next)
        } else {
            (history.event_head, history.event_next)
        };
        if reply.source > 1
            || reply.from < head
            || reply.end != end
            || !reply.snapshot_bytes.is_empty()
            || reply.records.len() > request.max_records as usize
            || reply.next - reply.from != reply.records.len() as u64
        {
            return Err(invalid());
        }
        let mut bytes = 0usize;
        for (i, record) in reply.records.iter().enumerate() {
            if record.offset != reply.from + i as u64 {
                return Err(invalid());
            }
            bytes = bytes
                .checked_add(18)
                .and_then(|n| n.checked_add(record.headers.len()))
                .and_then(|n| n.checked_add(record.payload.len()))
                .ok_or_else(invalid)?;
        }
        if bytes > request.max_bytes as usize {
            return Err(invalid());
        }
    }
    Ok(())
}

#[cfg(test)]
mod recovery_read_tests {
    use super::*;
    use crate::v1::{RecoveryRead, RecoveryReadOk, RecoverySeal, ReplicationMessageRecord};
    use fibril_broker::recovery::RetainedHistoryIdentity;

    #[tokio::test]
    async fn inspection_socket_reuse_discards_timeout_cancelled_and_invalid_replies() {
        use fibril_broker::recovery::{
            RecoveryReadRequest, RecoveryReadSource, RecoverySealCommand,
        };
        use std::time::Duration;
        let listener = fibril_util::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let handshakes = Arc::new(AtomicU64::new(0));
        let auths = Arc::new(AtomicU64::new(0));
        let reads = Arc::new(AtomicU64::new(0));
        let counts = (handshakes.clone(), auths.clone(), reads.clone());
        let server = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let (handshakes, auths, reads) = counts.clone();
                tasks.spawn(async move {
                    let mut conn = helper::plain_conn(socket);
                    while let Some(Ok(frame)) = conn.next().await {
                        let response = match frame.opcode {
                            op if op == Op::Hello as u16 => {
                                handshakes.fetch_add(1, Ordering::SeqCst);
                                try_encode(
                                    Op::HelloOk,
                                    frame.request_id,
                                    &HelloOk {
                                        protocol_version: PROTOCOL_V1,
                                        owner_id: uuid::Uuid::nil(),
                                        client_id: uuid::Uuid::nil(),
                                        resume_token: uuid::Uuid::nil(),
                                        resume_outcome: crate::v1::ResumeOutcome::New,
                                        server_name: "test".into(),
                                        compliance: crate::v1::COMPLIANCE_STRING.into(),
                                    },
                                )
                                .unwrap()
                            }
                            op if op == Op::Auth as u16 => {
                                let auth: Auth = try_decode(&frame).unwrap();
                                assert_eq!(auth.username, "@node");
                                assert_eq!(auth.password, "secret");
                                auths.fetch_add(1, Ordering::SeqCst);
                                try_encode(Op::AuthOk, frame.request_id, &()).unwrap()
                            }
                            op if op == Op::RecoveryRead as u16 => {
                                let n = reads.fetch_add(1, Ordering::SeqCst) + 1;
                                if n == 3 || n == 7 {
                                    // Consume the request, but leave its response incomplete.
                                    assert!(conn.next().await.is_none());
                                    return;
                                }
                                let req: RecoveryRead = try_decode(&frame).unwrap();
                                try_encode(
                                    Op::RecoveryReadOk,
                                    frame.request_id,
                                    &RecoveryReadOk {
                                        replica_id: "b".into(),
                                        transition: req.seal.transition,
                                        fence_epoch: req.seal.fence_epoch,
                                        history_id: if n == 5 { [99; 32] } else { req.history_id },
                                        source: req.source,
                                        from: 0,
                                        next: 0,
                                        end: 0,
                                        records: vec![],
                                        snapshot_bytes: vec![],
                                    },
                                )
                                .unwrap()
                            }
                            _ => panic!("unexpected operation"),
                        };
                        if conn.send(response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        let command = RecoverySealCommand {
            topic: "q".into(),
            partition: Partition::new(0),
            group: None,
            stream: false,
            transition: [1; 32],
            fence_epoch: 8,
        };
        let sealed = validate_recovery_seal_reply(
            "b",
            &command,
            crate::v1::RecoverySealOk {
                combined_log: false,
                storage_history: None,
                replica_id: "b".into(),
                transition: [1; 32],
                fence_epoch: 8,
                history_version: 1,
                history_id: [2; 32],
                message_digest: [3; 32],
                event_digest: [4; 32],
                snapshot_digest: None,
                message_head: 0,
                message_next: 0,
                event_head: 0,
                event_next: 0,
            },
        )
        .unwrap();
        let request = RecoveryReadRequest {
            seal: sealed.seal.request.clone(),
            history_id: sealed.seal.history.id,
            source: RecoveryReadSource::Messages,
            from: 0,
            max_records: 1,
            max_bytes: 32,
        };
        let config = ProtocolOwnerPeerResolverConfig::new(HashMap::from([("b".into(), address)]))
            .with_auth("@node", "secret");
        let mut idle = None;
        for n in 1..=8 {
            let deadline = if n == 3 {
                Duration::from_millis(100)
            } else {
                Duration::from_secs(3)
            };
            let call = request_recovery_read_reusing(
                &config, &command, &sealed, &request, deadline, &mut idle,
            );
            if n == 7 {
                assert!(
                    tokio::time::timeout(Duration::from_millis(100), call)
                        .await
                        .is_err()
                );
            } else {
                assert_eq!(call.await.is_err(), n == 3 || n == 5);
            }
            assert_eq!(idle.is_none(), n == 3 || n == 5 || n == 7);
        }
        assert_eq!(reads.load(Ordering::SeqCst), 8);
        assert_eq!(handshakes.load(Ordering::SeqCst), 4);
        assert_eq!(auths.load(Ordering::SeqCst), 4);
        drop(idle);
        server.abort();
    }

    #[test]
    fn read_reply_rejects_wrong_identity_holes_bounds_and_budget_violations() {
        let history = RetainedHistoryIdentity {
            log_layout: Default::default(),
            storage_history: None,
            version: 1,
            id: [2; 32],
            message_digest: [3; 32],
            event_digest: [4; 32],
            snapshot_digest: Some([5; 32]),
            message_head: 4,
            message_next: 8,
            event_head: 1,
            event_next: 9,
        };
        let request = RecoveryRead {
            seal: RecoverySeal {
                topic: "q".into(),
                partition: Partition::new(0),
                group: None,
                stream: false,
                transition: [1; 32],
                fence_epoch: 8,
            },
            history_id: history.id,
            source: 0,
            from: 4,
            max_records: 1,
            max_bytes: 20,
        };
        let reply = RecoveryReadOk {
            replica_id: "b".into(),
            transition: [1; 32],
            fence_epoch: 8,
            history_id: history.id,
            source: 0,
            from: 4,
            next: 5,
            end: 8,
            records: vec![ReplicationMessageRecord {
                offset: 4,
                flags: 0,
                headers: vec![],
                payload: vec![6, 7],
            }],
            snapshot_bytes: vec![],
        };
        assert!(validate_recovery_read_reply("b", &request, &history, &reply).is_ok());
        for case in 0..13 {
            let mut bad = reply.clone();
            match case {
                0 => bad.replica_id = "c".into(),
                1 => bad.transition[0] ^= 1,
                2 => bad.fence_epoch += 1,
                3 => bad.history_id[0] ^= 1,
                4 => bad.source = 1,
                5 => bad.from = 3,
                6 => bad.next = 9,
                7 => bad.end = 7,
                8 => bad.records[0].offset = 5,
                9 => bad.records[0].payload.push(8),
                10 => bad.snapshot_bytes.push(1),
                11 => {
                    bad.records.clear();
                    bad.next = 4;
                }
                _ => {
                    bad.records.push(bad.records[0].clone());
                    bad.next = 6;
                }
            }
            assert!(
                validate_recovery_read_reply("b", &request, &history, &bad).is_err(),
                "case {case}"
            );
        }
        let mut eof_req = request.clone();
        eof_req.from = 8;
        let mut eof = reply.clone();
        eof.from = 8;
        eof.next = 8;
        eof.records.clear();
        assert!(validate_recovery_read_reply("b", &eof_req, &history, &eof).is_ok());
        let mut events_req = request.clone();
        events_req.source = 1;
        events_req.from = 1;
        let mut events = reply.clone();
        events.source = 1;
        events.from = 1;
        events.next = 2;
        events.end = 9;
        events.records[0].offset = 1;
        assert!(validate_recovery_read_reply("b", &events_req, &history, &events).is_ok());
        events.from = 0;
        events_req.from = 0;
        events.next = 1;
        events.records[0].offset = 0;
        assert!(validate_recovery_read_reply("b", &events_req, &history, &events).is_err());
        let mut snapshot_req = request.clone();
        snapshot_req.source = 2;
        let mut snapshot = reply;
        snapshot.source = 2;
        snapshot.records.clear();
        snapshot.snapshot_bytes = vec![8];
        assert!(validate_recovery_read_reply("b", &snapshot_req, &history, &snapshot).is_ok());
        snapshot.snapshot_bytes.push(9);
        assert!(validate_recovery_read_reply("b", &snapshot_req, &history, &snapshot).is_err());
        snapshot.next = 6;
        snapshot_req.max_bytes = 1;
        assert!(validate_recovery_read_reply("b", &snapshot_req, &history, &snapshot).is_err());
    }
}
