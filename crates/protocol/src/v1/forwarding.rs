//! Forwarding client operations to private partition owners.
//!
//! A public broker serves a partition whose owner is private (or shares its
//! own client address) by relaying the client's frames on an upstream session
//! to the owner. There is one upstream session per (client session, owner),
//! opened lazily and authenticated as the node principal over the peer TLS
//! connector. The owner runs its normal session code, so leases, delivery
//! tags, prefetch and settle validation stay owner-side. The gateway maps
//! request ids and subscription ids and nothing else.
//!
//! A forwarded confirm means what a direct confirm means: the gateway answers
//! `PublishOk` only when the owner's `PublishOk` arrives, with the owner's
//! offset. A request sent upstream and then lost answers `504` (outcome
//! unknown). A request that never left answers `503`.

use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use arc_swap::ArcSwap;
use fibril_metrics::{ForwardLimit, ForwardOp, ForwardOutcome, ForwardStats};
use futures::{SinkExt, StreamExt};
use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::v1::{
    AdvertisedAddress, DEFAULT_HEARTBEAT_INTERVAL, ERR_GATEWAY_TIMEOUT, ERR_NOT_OWNER,
    ERR_UNAVAILABLE, ErrorMsg, ForwardedSession, ForwardedSessionOk, MAX_FORWARD_HOPS, Op,
    Partition, ReconcileClient, ReconcilePolicy, ReconcileSubscription, Redirect, SubscribeOk,
    SubscriptionClosed,
    frame::Frame,
    handler::ClientTopologySource,
    helper::{try_decode, try_encode},
    replication::{PeerTlsConnector, ProtocolOwnerPeerAuth, open_protocol_owner_conn},
    wire,
};

type SubKey = (String, Partition, Option<String>);
type FrameSink = mpsc::Sender<Frame>;

/// Client-facing ids of forwarded subscriptions start here, far above the
/// broker's own subscription counter so the two never collide on one
/// connection, and below 2^53 so every SDK holds them exactly.
pub const FORWARDED_SUB_ID_BASE: u64 = 1 << 48;

/// Frames queued for one upstream session before the client's connection
/// waits. Publish memory is bounded separately by the in-flight byte cap.
const UPSTREAM_QUEUE_FRAMES: usize = 1024;

/// How often an upstream session checks request deadlines and idleness.
const UPSTREAM_TICK: Duration = Duration::from_millis(250);

/// After an owner could not be reached, new operations for it fail fast for
/// this long instead of each waiting out a connect timeout.
const CONNECT_FAILURE_BACKOFF: Duration = Duration::from_secs(1);

/// Rate limit for repeated forwarding warnings.
const WARN_INTERVAL: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Routing rule
// ---------------------------------------------------------------------------

/// Routing view of one partition, as the handler's topology source sees it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartitionRoute {
    /// Owner node id, when the source knows it.
    pub owner_node_id: Option<String>,
    /// This broker owns the partition.
    pub owner_is_local: bool,
    /// The owner's own client endpoints. Empty when the owner is private.
    pub owner_endpoints: Vec<String>,
    /// What clients are told: the owner's endpoints or a gateway's.
    pub client_endpoints: Vec<String>,
    pub partitioning_version: u64,
}

/// What a broker does with an operation for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision {
    /// Serve it here (or answer `503` while recovering).
    Local,
    /// Relay it to the owner on an upstream session.
    Forward { owner: String },
    /// Send the client elsewhere. Never one of this broker's own addresses.
    Redirect {
        endpoints: Vec<String>,
        partitioning_version: u64,
    },
    /// No usable route.
    Unknown,
}

/// The handler's routing rule. A broker owns the partition: serve it. The
/// owner advertises no client address, or only addresses this broker also
/// advertises: forward, when this broker may forward. Otherwise redirect to
/// the client-facing endpoints, but never to this broker's own addresses,
/// which would only bounce the client back here.
pub fn decide_route(
    route: Option<&PartitionRoute>,
    local_endpoints: &[String],
    may_forward: bool,
) -> RouteDecision {
    let Some(route) = route else {
        return RouteDecision::Unknown;
    };
    if route.owner_is_local {
        return RouteDecision::Local;
    }
    let all_local = |endpoints: &[String]| {
        endpoints
            .iter()
            .all(|endpoint| local_endpoints.contains(endpoint))
    };
    let owner_reachable = !route.owner_endpoints.is_empty() && !all_local(&route.owner_endpoints);
    if !owner_reachable
        && may_forward
        && let Some(owner) = route.owner_node_id.as_ref()
    {
        return RouteDecision::Forward {
            owner: owner.clone(),
        };
    }
    if !route.client_endpoints.is_empty() && !all_local(&route.client_endpoints) {
        return RouteDecision::Redirect {
            endpoints: route.client_endpoints.clone(),
            partitioning_version: route.partitioning_version,
        };
    }
    RouteDecision::Unknown
}

/// Encode a client redirect to `endpoints`. `None` when nothing parses.
pub fn client_redirect_frame(
    request_id: u64,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
    endpoints: &[String],
    partitioning_version: u64,
) -> Option<Frame> {
    let owner_endpoints: Vec<AdvertisedAddress> = endpoints
        .iter()
        .filter_map(|endpoint| AdvertisedAddress::parse(endpoint))
        .collect();
    if owner_endpoints.is_empty() {
        return None;
    }
    try_encode(
        Op::Redirect,
        request_id,
        &Redirect {
            topic: topic.to_string(),
            partition,
            group: group.map(str::to_string),
            owner_endpoints,
            partitioning_version,
        },
    )
    .ok()
}

// ---------------------------------------------------------------------------
// Broker-wide forwarder
// ---------------------------------------------------------------------------

/// Live limits from the `routing` runtime settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingLimits {
    pub max_upstream_sessions: u64,
    pub max_upstream_sessions_per_owner: u64,
    pub max_inflight_forward_bytes: u64,
    pub forward_timeout_ms: u64,
    pub upstream_connect_timeout_ms: u64,
    pub upstream_idle_close_ms: u64,
}

impl Default for RoutingLimits {
    fn default() -> Self {
        Self {
            max_upstream_sessions: 4096,
            max_upstream_sessions_per_owner: 1024,
            max_inflight_forward_bytes: 256 * 1024 * 1024,
            forward_timeout_ms: 30_000,
            upstream_connect_timeout_ms: 5_000,
            upstream_idle_close_ms: 30_000,
        }
    }
}

/// How this broker dials owners.
#[derive(Debug, Clone)]
pub struct ForwardingConfig {
    /// This broker's node id, sent as the origin of forwarded sessions.
    pub node_id: String,
    /// Node principal credentials.
    pub auth: ProtocolOwnerPeerAuth,
    /// Peer TLS, the same connector replication uses.
    pub tls: Option<PeerTlsConnector>,
}

/// Broker-wide forwarding state: dial settings, live limits, counters and
/// the limit accounting shared by every client session. Present only on a
/// public broker in cluster mode.
pub struct Forwarder {
    cfg: ForwardingConfig,
    limits: ArcSwap<RoutingLimits>,
    stats: Arc<ForwardStats>,
    next_sub_id: AtomicU64,
    next_upstream_id: AtomicU64,
    bytes_released: Arc<Notify>,
    sessions_lock: std::sync::Mutex<()>,
    connect_failures: std::sync::Mutex<HashMap<String, Instant>>,
    warnings: RateLimitedLog,
}

impl std::fmt::Debug for Forwarder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Forwarder")
            .field("node_id", &self.cfg.node_id)
            .field("limits", &**self.limits.load())
            .finish_non_exhaustive()
    }
}

impl Forwarder {
    pub fn new(cfg: ForwardingConfig, limits: RoutingLimits, stats: Arc<ForwardStats>) -> Self {
        Self {
            cfg,
            limits: ArcSwap::from_pointee(limits),
            stats,
            next_sub_id: AtomicU64::new(FORWARDED_SUB_ID_BASE),
            next_upstream_id: AtomicU64::new(1),
            bytes_released: Arc::new(Notify::new()),
            sessions_lock: std::sync::Mutex::new(()),
            connect_failures: std::sync::Mutex::new(HashMap::new()),
            warnings: RateLimitedLog::default(),
        }
    }

    /// Apply new limits. Open sessions keep running; caps apply to new work.
    pub fn update_limits(&self, limits: RoutingLimits) {
        self.limits.store(Arc::new(limits));
        // Waiters re-check the byte cap against the new limit.
        self.bytes_released.notify_waiters();
    }

    pub fn limits(&self) -> RoutingLimits {
        **self.limits.load()
    }

    pub fn stats(&self) -> &Arc<ForwardStats> {
        &self.stats
    }

    fn next_client_sub_id(&self) -> u64 {
        self.next_sub_id.fetch_add(1, Ordering::Relaxed)
    }

    fn reserve_session(&self, owner: &str) -> Result<SessionSlot, ForwardLimit> {
        let limits = self.limits();
        let _guard = self.sessions_lock.lock().unwrap_or_else(|e| e.into_inner());
        if self.stats.upstream_sessions() >= limits.max_upstream_sessions {
            self.stats.rejected(ForwardLimit::UpstreamSessions);
            return Err(ForwardLimit::UpstreamSessions);
        }
        if self.stats.upstream_sessions_for(owner) >= limits.max_upstream_sessions_per_owner {
            self.stats.rejected(ForwardLimit::UpstreamSessionsPerOwner);
            return Err(ForwardLimit::UpstreamSessionsPerOwner);
        }
        self.stats.upstream_opened(owner);
        Ok(SessionSlot {
            stats: self.stats.clone(),
            owner: owner.to_string(),
        })
    }

    /// Reserve in-flight publish bytes, waiting up to `wait` for room.
    async fn reserve_bytes(&self, bytes: u64, wait: Duration) -> Option<BytesPermit> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            // Register before checking so a release between the check and
            // the wait is not missed.
            let released = self.bytes_released.notified();
            let max = self.limits().max_inflight_forward_bytes;
            if self.stats.try_reserve_inflight(bytes, max) {
                return Some(BytesPermit {
                    stats: self.stats.clone(),
                    released: self.bytes_released.clone(),
                    bytes,
                });
            }
            if tokio::time::timeout_at(deadline, released).await.is_err() {
                self.stats.rejected(ForwardLimit::InflightForwardBytes);
                return None;
            }
        }
    }

    fn recently_unreachable(&self, owner: &str) -> bool {
        let failures = self
            .connect_failures
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        failures
            .get(owner)
            .is_some_and(|at| at.elapsed() < CONNECT_FAILURE_BACKOFF)
    }

    fn record_connect_result(&self, owner: &str, ok: bool) {
        let mut failures = self
            .connect_failures
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if ok {
            failures.remove(owner);
        } else {
            failures.insert(owner.to_string(), Instant::now());
        }
    }
}

/// One counted upstream session. Dropping it frees the slot.
struct SessionSlot {
    stats: Arc<ForwardStats>,
    owner: String,
}

impl Drop for SessionSlot {
    fn drop(&mut self) {
        self.stats.upstream_closed(&self.owner);
    }
}

/// In-flight publish bytes held until the owner answers, or until a
/// fire-and-forget publish is written upstream.
struct BytesPermit {
    stats: Arc<ForwardStats>,
    released: Arc<Notify>,
    bytes: u64,
}

impl Drop for BytesPermit {
    fn drop(&mut self) {
        self.stats.release_inflight(self.bytes);
        self.released.notify_waiters();
    }
}

#[derive(Default)]
struct RateLimitedLog {
    last: std::sync::Mutex<HashMap<String, Instant>>,
}

impl RateLimitedLog {
    fn allow(&self, key: &str) -> bool {
        let mut last = self.last.lock().unwrap_or_else(|e| e.into_inner());
        match last.get(key) {
            Some(at) if at.elapsed() < WARN_INTERVAL => false,
            _ => {
                last.insert(key.to_string(), Instant::now());
                true
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per client session state
// ---------------------------------------------------------------------------

/// Who the upstream sessions act for, sent in `ForwardedSession`.
#[derive(Debug, Clone)]
pub(crate) struct ForwardIdentity {
    pub acting_user: String,
    pub client_id: Uuid,
    pub conn_id: Uuid,
    pub client_addr: Option<String>,
}

/// A forwarded subscription as the gateway's client session sees it.
#[derive(Debug, Clone)]
struct ForwardedSub {
    owner: String,
    upstream_id: u64,
    client_sub_id: u64,
    prefetch: u32,
    auto_ack: bool,
}

/// Why a forwarded operation was refused before anything was sent.
#[derive(Debug, Clone)]
pub(crate) struct ForwardReject {
    pub message: String,
}

impl ForwardReject {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

/// One operation to relay.
pub(crate) struct ForwardRequest {
    pub owner: String,
    pub op: ForwardOp,
    /// The client's frame, with the client's request id.
    pub frame: Frame,
    pub key: SubKey,
    /// Where the answer goes.
    pub reply: FrameSink,
    /// Whether the client waits for an answer.
    pub confirm: bool,
    pub prefetch: u32,
    pub auto_ack: bool,
}

/// Forwarding state of one client session (one logical connection): its
/// upstream sessions by owner and its forwarded subscriptions. Survives a
/// client reconnect within the reconnect grace.
pub(crate) struct ForwardSession {
    forwarder: Arc<Forwarder>,
    topology: Arc<dyn ClientTopologySource>,
    transport: watch::Receiver<Option<FrameSink>>,
    identity: std::sync::Mutex<ForwardIdentity>,
    upstreams: std::sync::Mutex<HashMap<String, Arc<Upstream>>>,
    subs: std::sync::Mutex<HashMap<SubKey, ForwardedSub>>,
    closed: AtomicBool,
    weak_self: Weak<ForwardSession>,
}

impl ForwardSession {
    pub(crate) fn new(
        forwarder: Arc<Forwarder>,
        topology: Arc<dyn ClientTopologySource>,
        transport: watch::Receiver<Option<FrameSink>>,
        identity: ForwardIdentity,
    ) -> Arc<Self> {
        Arc::new_cyclic(|weak_self| Self {
            forwarder,
            topology,
            transport,
            identity: std::sync::Mutex::new(identity),
            upstreams: std::sync::Mutex::new(HashMap::new()),
            subs: std::sync::Mutex::new(HashMap::new()),
            closed: AtomicBool::new(false),
            weak_self: weak_self.clone(),
        })
    }

    /// Refresh the identity after the client re-authenticated on a new
    /// connection. Applies to upstream sessions opened from now on.
    pub(crate) fn set_identity(&self, identity: ForwardIdentity) {
        *self.identity.lock().unwrap_or_else(|e| e.into_inner()) = identity;
    }

    fn identity(&self) -> ForwardIdentity {
        self.identity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Whether the client session has a forwarded subscription for `key`.
    pub(crate) fn has_sub(&self, key: &SubKey) -> bool {
        self.subs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(key)
    }

    /// Forwarded subscriptions in reconcile form, with client-facing ids.
    pub(crate) fn reconcile_view(&self, key: &SubKey) -> Option<ReconcileSubscription> {
        let subs = self.subs.lock().unwrap_or_else(|e| e.into_inner());
        subs.get(key).map(|sub| ReconcileSubscription {
            sub_id: sub.client_sub_id,
            topic: key.0.clone(),
            group: key.2.clone(),
            partition: key.1,
            auto_ack: sub.auto_ack,
            prefetch: sub.prefetch,
            consumer_group: None,
            consumer_target: None,
            member_id: None,
        })
    }

    pub(crate) fn sub_keys(&self) -> Vec<SubKey> {
        self.subs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect()
    }

    /// Relay one request to its owner. On `Err` nothing was sent and the
    /// caller answers `503` with the message.
    pub(crate) async fn forward(&self, request: ForwardRequest) -> Result<(), ForwardReject> {
        let permit = match request.op {
            ForwardOp::Publish | ForwardOp::PublishDelayed => {
                let limits = self.forwarder.limits();
                // Waiting holds up this client's connection loop only. Cap it
                // well below the client heartbeat timeout so the connection
                // keeps answering pings.
                let wait = Duration::from_millis(limits.forward_timeout_ms)
                    .min(Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL));
                match self
                    .forwarder
                    .reserve_bytes(request.frame.payload.len() as u64, wait)
                    .await
                {
                    Some(permit) => Some(permit),
                    None => {
                        self.warn_limit(ForwardLimit::InflightForwardBytes, &request.owner);
                        return Err(ForwardReject::new(format!(
                            "forwarding limit reached: routing.max_inflight_forward_bytes \
                             ({} bytes) is in use, retry shortly",
                            limits.max_inflight_forward_bytes
                        )));
                    }
                }
            }
            ForwardOp::Subscribe => None,
        };
        self.send_request(request, 1, permit).await
    }

    async fn send_request(
        &self,
        request: ForwardRequest,
        attempt: u8,
        permit: Option<BytesPermit>,
    ) -> Result<(), ForwardReject> {
        let upstream = self.upstream_for(&request.owner)?;
        let upstream_req_id = upstream.next_request_id();
        let mut frame = request.frame.clone();
        frame.request_id = upstream_req_id;
        let bytes = frame.payload.len() as u64;
        let write_permit = if request.confirm {
            let deadline =
                Instant::now() + Duration::from_millis(self.forwarder.limits().forward_timeout_ms);
            let pending = Pending {
                op: request.op,
                client_req_id: request.frame.request_id,
                reply: request.reply,
                original: request.frame,
                key: request.key,
                attempt,
                deadline,
                started: Instant::now(),
                prefetch: request.prefetch,
                auto_ack: request.auto_ack,
                permit,
            };
            if !upstream.insert_pending(upstream_req_id, pending) {
                return Err(ForwardReject::new(
                    "the session to the partition owner closed, retry shortly",
                ));
            }
            None
        } else {
            permit
        };
        let sent = upstream
            .tx
            .send(UpstreamWrite::Frame {
                frame,
                permit: write_permit,
            })
            .await
            .is_ok();
        if !sent {
            // When the closing session already took the request, it answers
            // it. Answering here too would reply twice.
            if request.confirm && upstream.take_pending(upstream_req_id).is_none() {
                return Ok(());
            }
            return Err(ForwardReject::new(
                "the session to the partition owner closed, retry shortly",
            ));
        }
        self.forwarder.stats.bytes_out(bytes);
        upstream.touch();
        Ok(())
    }

    /// Relay an `Ack` or `Nack` to the upstream session holding the
    /// subscription. Returns false when no forwarded subscription matches, so
    /// the settle is ignored like one for an unknown subscription.
    pub(crate) async fn relay_settle(&self, key: &SubKey, frame: Frame) -> bool {
        let Some(sub) = self
            .subs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .cloned()
        else {
            return false;
        };
        let Some(upstream) = self.current_upstream(&sub.owner, sub.upstream_id) else {
            return false;
        };
        let bytes = frame.payload.len() as u64;
        if upstream
            .tx
            .send(UpstreamWrite::Frame {
                frame,
                permit: None,
            })
            .await
            .is_ok()
        {
            self.forwarder.stats.bytes_out(bytes);
            upstream.touch();
        }
        true
    }

    /// Close forwarded subscriptions the client no longer wants. The owner
    /// learns it from a reconcile on the upstream session that lists only the
    /// subscriptions to keep, so it releases the closed ones' deliveries at
    /// once. Returns the closed subscriptions with client-facing ids.
    pub(crate) async fn close_subs(&self, keys: &[SubKey]) -> Vec<ReconcileSubscription> {
        let mut closed = Vec::new();
        let mut by_upstream: HashMap<(String, u64), ()> = HashMap::new();
        {
            let mut subs = self.subs.lock().unwrap_or_else(|e| e.into_inner());
            for key in keys {
                if let Some(sub) = subs.remove(key) {
                    by_upstream.insert((sub.owner.clone(), sub.upstream_id), ());
                    closed.push(ReconcileSubscription {
                        sub_id: sub.client_sub_id,
                        topic: key.0.clone(),
                        group: key.2.clone(),
                        partition: key.1,
                        auto_ack: sub.auto_ack,
                        prefetch: sub.prefetch,
                        consumer_group: None,
                        consumer_target: None,
                        member_id: None,
                    });
                }
            }
        }
        for (owner, upstream_id) in by_upstream.into_keys() {
            if let Some(upstream) = self.current_upstream(&owner, upstream_id) {
                upstream.forget_subs(keys);
                let keep = upstream.reconcile_list();
                if let Ok(frame) = try_encode(
                    Op::ReconcileClient,
                    upstream.next_request_id(),
                    &ReconcileClient {
                        policy: ReconcilePolicy::Conservative,
                        subscriptions: keep,
                    },
                ) {
                    let _ = upstream
                        .tx
                        .send(UpstreamWrite::Frame {
                            frame,
                            permit: None,
                        })
                        .await;
                }
            }
        }
        closed
    }

    /// End every upstream session for good: the client session is gone. Each
    /// owner gets an empty reconcile first, so it closes the subscriptions and
    /// redelivers their unsettled messages without waiting out its own
    /// reconnect grace.
    pub(crate) async fn close(&self, reason: &'static str) {
        if self.closed.swap(true, Ordering::AcqRel) {
            return;
        }
        let upstreams: Vec<Arc<Upstream>> = self
            .upstreams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .drain()
            .map(|(_, upstream)| upstream)
            .collect();
        self.subs.lock().unwrap_or_else(|e| e.into_inner()).clear();
        for upstream in upstreams {
            upstream.set_close_reason(reason);
            if let Ok(frame) = try_encode(
                Op::ReconcileClient,
                upstream.next_request_id(),
                &ReconcileClient {
                    policy: ReconcilePolicy::Conservative,
                    subscriptions: Vec::new(),
                },
            ) {
                let _ = upstream.tx.try_send(UpstreamWrite::Frame {
                    frame,
                    permit: None,
                });
            }
            let _ = upstream.tx.try_send(UpstreamWrite::Close);
            // Also wakes a session waiting on a dormant client. It writes what
            // is already queued, the reconcile included, before closing.
            upstream.cancel.cancel();
        }
    }

    fn current_upstream(&self, owner: &str, upstream_id: u64) -> Option<Arc<Upstream>> {
        self.upstreams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(owner)
            .filter(|upstream| upstream.id == upstream_id)
            .cloned()
    }

    fn upstream_for(&self, owner: &str) -> Result<Arc<Upstream>, ForwardReject> {
        if self.closed.load(Ordering::Acquire) {
            return Err(ForwardReject::new("the client session is closing"));
        }
        let mut upstreams = self.upstreams.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(upstream) = upstreams.get(owner)
            && !upstream.is_closed()
        {
            return Ok(upstream.clone());
        }
        if self.forwarder.recently_unreachable(owner) {
            return Err(ForwardReject::new(format!(
                "partition owner {owner} is unreachable from this broker, retry shortly"
            )));
        }
        let Some(addr) = self.topology.node_peer_address(owner) else {
            return Err(ForwardReject::new(format!(
                "partition owner {owner} has no known peer address yet, retry shortly"
            )));
        };
        let slot = match self.forwarder.reserve_session(owner) {
            Ok(slot) => slot,
            Err(limit) => {
                self.warn_limit(limit, owner);
                return Err(ForwardReject::new(format!(
                    "forwarding limit reached: routing.{} is in use, retry shortly",
                    limit.as_str()
                )));
            }
        };
        let (tx, rx) = mpsc::channel(UPSTREAM_QUEUE_FRAMES);
        let upstream = Arc::new(Upstream {
            id: self
                .forwarder
                .next_upstream_id
                .fetch_add(1, Ordering::Relaxed),
            owner: owner.to_string(),
            addr,
            tx,
            cancel: CancellationToken::new(),
            state: std::sync::Mutex::new(UpstreamState::default()),
            next_req: AtomicU64::new(10),
            last_active: std::sync::Mutex::new(Instant::now()),
            close_reason: std::sync::Mutex::new(None),
            _slot: slot,
        });
        upstreams.insert(owner.to_string(), upstream.clone());
        drop(upstreams);
        tokio::spawn(run_upstream(
            self.weak_self.clone(),
            upstream.clone(),
            rx,
            self.identity(),
        ));
        Ok(upstream)
    }

    fn remove_upstream(&self, upstream: &Upstream) {
        let mut upstreams = self.upstreams.lock().unwrap_or_else(|e| e.into_inner());
        if upstreams
            .get(&upstream.owner)
            .is_some_and(|current| current.id == upstream.id)
        {
            upstreams.remove(&upstream.owner);
        }
    }

    fn warn_limit(&self, limit: ForwardLimit, owner: &str) {
        if self.forwarder.warnings.allow(limit.as_str()) {
            let identity = self.identity();
            tracing::warn!(
                limit = limit.as_str(),
                owner_node = owner,
                client_id = %identity.client_id,
                "forwarding limit reached, answering 503. Raise routing.{} if this persists",
                limit.as_str()
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Upstream session
// ---------------------------------------------------------------------------

enum UpstreamWrite {
    Frame {
        frame: Frame,
        permit: Option<BytesPermit>,
    },
    Close,
}

struct Pending {
    op: ForwardOp,
    client_req_id: u64,
    reply: FrameSink,
    original: Frame,
    key: SubKey,
    attempt: u8,
    deadline: Instant,
    started: Instant,
    prefetch: u32,
    auto_ack: bool,
    permit: Option<BytesPermit>,
}

#[derive(Default)]
struct UpstreamState {
    pending: HashMap<u64, Pending>,
    /// Owner subscription id to (client-facing id, key).
    subs: HashMap<u64, (u64, SubKey, u32, bool)>,
    connected: bool,
    closed: bool,
}

struct Upstream {
    id: u64,
    owner: String,
    addr: String,
    tx: mpsc::Sender<UpstreamWrite>,
    cancel: CancellationToken,
    state: std::sync::Mutex<UpstreamState>,
    next_req: AtomicU64,
    last_active: std::sync::Mutex<Instant>,
    close_reason: std::sync::Mutex<Option<&'static str>>,
    _slot: SessionSlot,
}

impl Upstream {
    fn state(&self) -> std::sync::MutexGuard<'_, UpstreamState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn next_request_id(&self) -> u64 {
        self.next_req.fetch_add(1, Ordering::Relaxed)
    }

    fn is_closed(&self) -> bool {
        self.state().closed
    }

    fn touch(&self) {
        *self.last_active.lock().unwrap_or_else(|e| e.into_inner()) = Instant::now();
    }

    fn idle_for(&self) -> Duration {
        self.last_active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .elapsed()
    }

    fn set_close_reason(&self, reason: &'static str) {
        let mut slot = self.close_reason.lock().unwrap_or_else(|e| e.into_inner());
        slot.get_or_insert(reason);
    }

    fn close_reason(&self) -> &'static str {
        self.close_reason
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .unwrap_or("closed")
    }

    fn insert_pending(&self, request_id: u64, pending: Pending) -> bool {
        let mut state = self.state();
        if state.closed {
            return false;
        }
        state.pending.insert(request_id, pending);
        true
    }

    fn take_pending(&self, request_id: u64) -> Option<Pending> {
        self.state().pending.remove(&request_id)
    }

    fn forget_subs(&self, keys: &[SubKey]) {
        self.state()
            .subs
            .retain(|_, (_, key, _, _)| !keys.contains(key));
    }

    /// Subscriptions to keep in a reconcile: the live ones, plus subscribes
    /// still waiting for the owner's answer (the owner matches by key, so
    /// their id does not matter).
    fn reconcile_list(&self) -> Vec<ReconcileSubscription> {
        let state = self.state();
        let live = state
            .subs
            .iter()
            .map(|(upstream_sub_id, (_, key, prefetch, auto_ack))| {
                (*upstream_sub_id, key, *prefetch, *auto_ack)
            });
        let waiting = state
            .pending
            .values()
            .filter(|pending| pending.op == ForwardOp::Subscribe)
            .map(|pending| (0, &pending.key, pending.prefetch, pending.auto_ack));
        live.chain(waiting)
            .map(|(sub_id, key, prefetch, auto_ack)| ReconcileSubscription {
                sub_id,
                topic: key.0.clone(),
                group: key.2.clone(),
                partition: key.1,
                auto_ack,
                prefetch,
                consumer_group: None,
                consumer_target: None,
                member_id: None,
            })
            .collect()
    }
}

/// Why an upstream session ended.
enum UpstreamEnd {
    ConnectFailed(String),
    Lost(String),
    Timeout,
    Idle,
    Closed,
}

async fn run_upstream(
    session: Weak<ForwardSession>,
    upstream: Arc<Upstream>,
    mut rx: mpsc::Receiver<UpstreamWrite>,
    identity: ForwardIdentity,
) {
    let Some(strong) = session.upgrade() else {
        return;
    };
    let forwarder = strong.forwarder.clone();
    let mut transport = strong.transport.clone();
    let local_endpoints = strong.topology.local_client_endpoints();
    drop(strong);
    let limits = forwarder.limits();

    let connected = tokio::select! {
        result = open_upstream(&forwarder, &upstream, &identity, limits.upstream_connect_timeout_ms) => result,
        _ = upstream.cancel.cancelled() => Err("the client session closed".to_string()),
    };
    let conn = match connected {
        Ok(conn) => {
            forwarder.record_connect_result(&upstream.owner, true);
            upstream.state().connected = true;
            tracing::info!(
                owner_node = %upstream.owner,
                owner_addr = %upstream.addr,
                client_id = %identity.client_id,
                conn_id = %identity.conn_id,
                acting_user = %identity.acting_user,
                "upstream session opened"
            );
            conn
        }
        Err(message) => {
            forwarder.record_connect_result(&upstream.owner, false);
            if forwarder
                .warnings
                .allow(&format!("connect:{}", upstream.owner))
            {
                tracing::warn!(
                    owner_node = %upstream.owner,
                    owner_addr = %upstream.addr,
                    client_id = %identity.client_id,
                    "could not open an upstream session to the partition owner: {message}. \
                     Forwarded operations get 503 until it is reachable"
                );
            }
            finish_upstream(
                &session,
                &forwarder,
                &upstream,
                &mut rx,
                &mut transport,
                UpstreamEnd::ConnectFailed(message),
                &identity,
            )
            .await;
            return;
        }
    };

    let (mut sink, mut stream) = conn.split();
    let mut cached_tx = None;
    let mut tick = tokio::time::interval(UPSTREAM_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let read_timeout = Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL * 3);
    let mut last_read = Instant::now();

    let end = loop {
        tokio::select! {
            write = rx.recv() => match write {
                Some(UpstreamWrite::Frame { frame, permit }) => {
                    if let Err(err) = sink.send(frame).await {
                        break UpstreamEnd::Lost(format!("write failed: {err}"));
                    }
                    // A fire-and-forget publish holds its bytes until written.
                    drop(permit);
                }
                Some(UpstreamWrite::Close) | None => break UpstreamEnd::Closed,
            },
            frame = stream.next() => match frame {
                Some(Ok(frame)) => {
                    last_read = Instant::now();
                    forwarder.stats.bytes_in(frame.payload.len() as u64);
                    if frame.opcode == Op::Ping as u16 {
                        if let Ok(pong) = try_encode(Op::Pong, frame.request_id, &()) {
                            if let Err(err) = sink.send(pong).await {
                                break UpstreamEnd::Lost(format!("write failed: {err}"));
                            }
                        }
                        continue;
                    }
                    handle_upstream_frame(
                        &session,
                        &forwarder,
                        &upstream,
                        &local_endpoints,
                        &mut transport,
                        &mut cached_tx,
                        frame,
                    )
                    .await;
                }
                Some(Err(err)) => break UpstreamEnd::Lost(format!("read failed: {err}")),
                None => break UpstreamEnd::Lost("the owner closed the connection".into()),
            },
            _ = tick.tick() => {
                if last_read.elapsed() > read_timeout {
                    break UpstreamEnd::Lost("the owner stopped answering heartbeats".into());
                }
                let limits = forwarder.limits();
                let now = Instant::now();
                let expired = upstream.state().pending.values().any(|pending| pending.deadline <= now);
                if expired {
                    // The owner sat on a request past the forward timeout. Treat the
                    // session as broken: everything pending gets 504 and the next
                    // operation opens a fresh session.
                    break UpstreamEnd::Timeout;
                }
                let idle = {
                    let state = upstream.state();
                    state.pending.is_empty() && state.subs.is_empty()
                };
                if idle && upstream.idle_for() >= Duration::from_millis(limits.upstream_idle_close_ms) {
                    // Leave the table first so no new request lands on a closing session.
                    if let Some(session) = session.upgrade() {
                        session.remove_upstream(&upstream);
                    }
                    upstream.state().closed = true;
                    if upstream.state().pending.is_empty() {
                        break UpstreamEnd::Idle;
                    }
                }
            }
            _ = upstream.cancel.cancelled() => break UpstreamEnd::Closed,
        }
    };
    if matches!(end, UpstreamEnd::Closed) {
        // Flush what the session queued before closing, such as the reconcile
        // that releases the owner's subscriptions.
        while let Ok(UpstreamWrite::Frame { frame, .. }) = rx.try_recv() {
            if sink.send(frame).await.is_err() {
                break;
            }
        }
    }
    let _ = sink.close().await;
    finish_upstream(
        &session,
        &forwarder,
        &upstream,
        &mut rx,
        &mut transport,
        end,
        &identity,
    )
    .await;
}

async fn open_upstream(
    forwarder: &Forwarder,
    upstream: &Upstream,
    identity: &ForwardIdentity,
    connect_timeout_ms: u64,
) -> Result<crate::v1::helper::Conn, String> {
    let setup = async {
        let mut conn = open_protocol_owner_conn(
            upstream.addr.clone(),
            Some(&forwarder.cfg.auth),
            forwarder.cfg.tls.as_ref(),
            "fibril-gateway",
            env!("CARGO_PKG_VERSION"),
            connect_timeout_ms,
        )
        .await
        .map_err(|err| err.to_string())?;
        let request_id = 3;
        let marker = try_encode(
            Op::ForwardedSession,
            request_id,
            &ForwardedSession {
                origin_node: forwarder.cfg.node_id.clone(),
                hop_count: MAX_FORWARD_HOPS,
                acting_user: identity.acting_user.clone(),
                client_id: identity.client_id,
                client_conn_id: identity.conn_id,
                client_addr: identity.client_addr.clone(),
                release_on_close: false,
            },
        )
        .map_err(|err| err.to_string())?;
        conn.send(marker)
            .await
            .map_err(|err| format!("forwarded session send failed: {err}"))?;
        loop {
            let frame = conn
                .next()
                .await
                .ok_or_else(|| "the owner closed the connection during setup".to_string())?
                .map_err(|err| err.to_string())?;
            if frame.opcode == Op::Ping as u16 {
                if let Ok(pong) = try_encode(Op::Pong, frame.request_id, &()) {
                    conn.send(pong).await.map_err(|err| err.to_string())?;
                }
                continue;
            }
            if frame.request_id != request_id {
                continue;
            }
            if frame.opcode == Op::ForwardedSessionOk as u16 {
                let ok: ForwardedSessionOk = try_decode(&frame).map_err(|err| err.to_string())?;
                tracing::debug!(
                    owner_node = %upstream.owner,
                    forwarding_version = ok.forwarding_version,
                    "owner accepted forwarded session"
                );
                return Ok(conn);
            }
            let detail = try_decode::<ErrorMsg>(&frame)
                .map(|err| format!("{} {}", err.code, err.message))
                .unwrap_or_else(|_| format!("opcode {}", frame.opcode));
            return Err(format!(
                "the owner refused the forwarded session ({detail}). It is likely an older \
                 version without forwarding support"
            ));
        }
    };
    match tokio::time::timeout(Duration::from_millis(connect_timeout_ms.max(1)), setup).await {
        Ok(result) => result,
        Err(_) => Err(format!(
            "upstream session setup timed out after {connect_timeout_ms}ms"
        )),
    }
}

async fn handle_upstream_frame(
    session: &Weak<ForwardSession>,
    forwarder: &Arc<Forwarder>,
    upstream: &Arc<Upstream>,
    local_endpoints: &[String],
    transport: &mut watch::Receiver<Option<FrameSink>>,
    cached_tx: &mut Option<FrameSink>,
    frame: Frame,
) {
    let opcode = frame.opcode;
    if opcode == Op::Deliver as u16 {
        let Ok(upstream_sub_id) = wire::deliver_sub_id(&frame) else {
            return;
        };
        let client_sub_id = upstream
            .state()
            .subs
            .get(&upstream_sub_id)
            .map(|(client_sub_id, ..)| *client_sub_id);
        let Some(client_sub_id) = client_sub_id else {
            // A delivery for a subscription this session closed or never
            // confirmed. The owner redelivers it after its lease.
            return;
        };
        if let Ok(rewritten) = wire::rewrite_deliver_sub_id(&frame, client_sub_id) {
            // Waits on the client's own connection, which is the backpressure:
            // a slow client stops reading its upstream and only its upstream.
            tokio::select! {
                _ = crate::v1::handler::send_to_current_transport(transport, cached_tx, rewritten) => {}
                _ = upstream.cancel.cancelled() => {}
            }
        }
        return;
    }
    if opcode == Op::SubscriptionClosed as u16 {
        let Ok(closed) = wire::decode_subscription_closed(&frame) else {
            return;
        };
        let removed = upstream.state().subs.remove(&closed.sub_id);
        let Some((client_sub_id, key, ..)) = removed else {
            return;
        };
        if let Some(session) = session.upgrade() {
            let mut subs = session.subs.lock().unwrap_or_else(|e| e.into_inner());
            if subs
                .get(&key)
                .is_some_and(|sub| sub.upstream_id == upstream.id)
            {
                subs.remove(&key);
            }
        }
        let relayed = SubscriptionClosed {
            sub_id: client_sub_id,
            ..closed
        };
        if let Ok(frame) = wire::encode_subscription_closed(frame.request_id, &relayed) {
            let _ =
                crate::v1::handler::send_to_current_transport(transport, cached_tx, frame).await;
        }
        return;
    }
    let is_reply = opcode == Op::PublishOk as u16
        || opcode == Op::Error as u16
        || opcode == Op::SubscribeOk as u16
        || opcode == Op::SubscribeErr as u16
        || opcode == Op::Redirect as u16;
    if !is_reply {
        // GoingAway, TopologyUpdate, ReconcileResult and the like stay on the
        // upstream session. A draining owner's moves arrive as closes.
        tracing::trace!(opcode, owner_node = %upstream.owner, "ignored upstream frame");
        return;
    }
    let Some(pending) = upstream.take_pending(frame.request_id) else {
        return;
    };
    upstream.touch();
    handle_reply(
        session,
        forwarder,
        upstream,
        local_endpoints,
        pending,
        frame,
    )
    .await;
}

async fn handle_reply(
    session: &Weak<ForwardSession>,
    forwarder: &Arc<Forwarder>,
    upstream: &Arc<Upstream>,
    local_endpoints: &[String],
    mut pending: Pending,
    frame: Frame,
) {
    let stats = &forwarder.stats;
    let opcode = frame.opcode;
    if opcode == Op::PublishOk as u16 {
        stats.confirm_latency.observe(pending.started.elapsed());
        stats.request(pending.op, ForwardOutcome::Ok);
        let mut reply = frame;
        reply.request_id = pending.client_req_id;
        let _ = pending.reply.send(reply).await;
        return;
    }
    if opcode == Op::SubscribeOk as u16 {
        let Ok(ok) = try_decode::<SubscribeOk>(&frame) else {
            answer(
                &pending,
                ERR_UNAVAILABLE,
                "the owner sent a malformed subscribe answer",
            )
            .await;
            stats.request(pending.op, ForwardOutcome::Unavailable);
            return;
        };
        let Some(session) = session.upgrade() else {
            return;
        };
        let client_sub_id = forwarder.next_client_sub_id();
        upstream.state().subs.insert(
            ok.sub_id,
            (
                client_sub_id,
                pending.key.clone(),
                ok.prefetch,
                pending.auto_ack,
            ),
        );
        session
            .subs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                pending.key.clone(),
                ForwardedSub {
                    owner: upstream.owner.clone(),
                    upstream_id: upstream.id,
                    client_sub_id,
                    prefetch: ok.prefetch,
                    auto_ack: pending.auto_ack,
                },
            );
        stats.request(pending.op, ForwardOutcome::Ok);
        let relayed = SubscribeOk {
            sub_id: client_sub_id,
            ..ok
        };
        if let Ok(reply) = try_encode(Op::SubscribeOk, pending.client_req_id, &relayed) {
            let _ = pending.reply.send(reply).await;
        }
        return;
    }
    // An owner redirect or not-owner answer means it applied nothing.
    let moved_version = if opcode == Op::Redirect as u16 {
        Some(
            wire::decode_redirect(&frame)
                .map(|redirect| redirect.partitioning_version)
                .unwrap_or(0),
        )
    } else {
        match try_decode::<ErrorMsg>(&frame) {
            Ok(err) if err.code == ERR_NOT_OWNER => Some(0),
            _ => None,
        }
    };
    let Some(upstream_version) = moved_version else {
        // 400, 403, 404, 413, an owner 503 and the rest mean the same thing
        // they mean to a direct client.
        stats.request(pending.op, ForwardOutcome::RelayedError);
        let mut reply = frame;
        reply.request_id = pending.client_req_id;
        let _ = pending.reply.send(reply).await;
        return;
    };
    let Some(session) = session.upgrade() else {
        return;
    };
    let route = route_for(&session, &pending);
    let decision = decide_route(route.as_ref(), local_endpoints, pending.attempt < 2);
    match decision {
        RouteDecision::Forward { owner } if owner != upstream.owner => {
            // The view already names a new owner: one more upstream attempt.
            tracing::debug!(
                from = %upstream.owner,
                to = %owner,
                topic = %pending.key.0,
                "partition owner moved, retrying the forwarded request once"
            );
            let permit = pending.permit.take();
            let request = ForwardRequest {
                owner,
                op: pending.op,
                frame: pending.original.clone(),
                key: pending.key.clone(),
                reply: pending.reply.clone(),
                confirm: true,
                prefetch: pending.prefetch,
                auto_ack: pending.auto_ack,
            };
            if let Err(reject) = session.send_request(request, 2, permit).await {
                answer(&pending, ERR_UNAVAILABLE, reject.message).await;
                stats.request(pending.op, ForwardOutcome::Unavailable);
            }
        }
        RouteDecision::Redirect {
            endpoints,
            partitioning_version,
        } => {
            // Rebuilt from this broker's view, never relayed raw.
            let version = partitioning_version.max(upstream_version);
            match client_redirect_frame(
                pending.client_req_id,
                &pending.key.0,
                pending.key.1,
                pending.key.2.as_deref(),
                &endpoints,
                version,
            ) {
                Some(redirect) => {
                    stats.request(pending.op, ForwardOutcome::Redirected);
                    let _ = pending.reply.send(redirect).await;
                }
                None => {
                    stats.request(pending.op, ForwardOutcome::Unavailable);
                    answer(&pending, ERR_UNAVAILABLE, OWNER_MOVING_MESSAGE).await;
                }
            }
        }
        _ => {
            // Local, the same owner, or no route: the view has not caught up
            // with the move. Nothing was applied, so a retry is safe.
            stats.request(pending.op, ForwardOutcome::Unavailable);
            answer(&pending, ERR_UNAVAILABLE, OWNER_MOVING_MESSAGE).await;
        }
    }
}

const OWNER_MOVING_MESSAGE: &str = "partition ownership is moving and nothing was applied, \
    retry shortly";

fn route_for(session: &ForwardSession, pending: &Pending) -> Option<PartitionRoute> {
    session
        .topology
        .queue_route(&pending.key.0, pending.key.1, pending.key.2.as_deref())
}

/// Answer a pending request with an error code, as `SubscribeErr` for a
/// subscribe and `Error` otherwise.
async fn answer(pending: &Pending, code: u16, message: impl Into<String>) {
    let op = if pending.op == ForwardOp::Subscribe {
        Op::SubscribeErr
    } else {
        Op::Error
    };
    if let Ok(frame) = try_encode(
        op,
        pending.client_req_id,
        &ErrorMsg {
            code,
            message: message.into(),
        },
    ) {
        let _ = pending.reply.send(frame).await;
    }
}

async fn finish_upstream(
    session: &Weak<ForwardSession>,
    forwarder: &Arc<Forwarder>,
    upstream: &Arc<Upstream>,
    rx: &mut mpsc::Receiver<UpstreamWrite>,
    transport: &mut watch::Receiver<Option<FrameSink>>,
    end: UpstreamEnd,
    identity: &ForwardIdentity,
) {
    let (pending, subs, connected) = {
        let mut state = upstream.state();
        state.closed = true;
        (
            std::mem::take(&mut state.pending),
            std::mem::take(&mut state.subs),
            state.connected,
        )
    };
    rx.close();
    // Frames that never left. Fire-and-forget publishes among them are lost
    // and counted. Their confirmed peers are answered from `pending` below.
    while let Ok(write) = rx.try_recv() {
        if let UpstreamWrite::Frame { frame, .. } = write
            && (frame.opcode == Op::Publish as u16 || frame.opcode == Op::PublishDelayed as u16)
            && !pending.contains_key(&frame.request_id)
        {
            forwarder.stats.dropped();
        }
    }
    let strong = session.upgrade();
    if let Some(session) = strong.as_ref() {
        session.remove_upstream(upstream);
    }

    let reason = match &end {
        UpstreamEnd::ConnectFailed(message) => message.clone(),
        UpstreamEnd::Lost(message) => message.clone(),
        UpstreamEnd::Timeout => format!(
            "a request waited past routing.forward_timeout_ms ({}ms)",
            forwarder.limits().forward_timeout_ms
        ),
        UpstreamEnd::Idle => "idle".to_string(),
        UpstreamEnd::Closed => upstream.close_reason().to_string(),
    };
    if connected {
        tracing::info!(
            owner_node = %upstream.owner,
            client_id = %identity.client_id,
            conn_id = %identity.conn_id,
            acting_user = %identity.acting_user,
            pending = pending.len(),
            subscriptions = subs.len(),
            reason = %reason,
            "upstream session closed"
        );
    }

    for (_, pending) in pending {
        // A request on a session that never connected was never sent. One on
        // a connected session may have been applied, except a subscribe: the
        // owner drops it with the session, so a retry is safe.
        let (code, outcome, message) = if !connected || pending.op == ForwardOp::Subscribe {
            (
                ERR_UNAVAILABLE,
                ForwardOutcome::Unavailable,
                format!(
                    "the partition owner could not be reached through this broker: {reason}. Retry shortly"
                ),
            )
        } else {
            (
                ERR_GATEWAY_TIMEOUT,
                ForwardOutcome::Timeout,
                format!(
                    "the partition owner did not answer ({reason}) after the request was sent. \
                     The outcome is unknown: the publish may or may not have been applied"
                ),
            )
        };
        if code == ERR_GATEWAY_TIMEOUT {
            tracing::info!(
                owner_node = %upstream.owner,
                client_id = %identity.client_id,
                topic = %pending.key.0,
                partition = pending.key.1.id(),
                code,
                "forwarded request failed with unknown outcome"
            );
        }
        forwarder.stats.request(pending.op, outcome);
        answer(&pending, code, message).await;
    }

    if subs.is_empty() {
        return;
    }
    if let Some(session) = strong.as_ref() {
        let mut session_subs = session.subs.lock().unwrap_or_else(|e| e.into_inner());
        session_subs.retain(|_, sub| sub.upstream_id != upstream.id);
    }
    // Tell the client each subscription ended so its supervisor resubscribes,
    // through this broker, to whoever owns the partition now.
    let mut cached_tx = None;
    for (_, (client_sub_id, _key, _, _)) in subs {
        let closed = SubscriptionClosed {
            sub_id: client_sub_id,
            code: crate::v1::ReasonCode::OwnerMoved,
            message: format!(
                "the gateway lost its session to the partition owner ({reason}), resubscribe"
            ),
        };
        if let Ok(frame) = wire::encode_subscription_closed(0, &closed) {
            let sent = tokio::time::timeout(
                Duration::from_secs(1),
                crate::v1::handler::send_to_current_transport(transport, &mut cached_tx, frame),
            )
            .await;
            if !matches!(sent, Ok(Ok(()))) {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn route(owner_endpoints: &[&str], client_endpoints: &[&str]) -> PartitionRoute {
        PartitionRoute {
            owner_node_id: Some("owner".into()),
            owner_is_local: false,
            owner_endpoints: owner_endpoints.iter().map(|s| s.to_string()).collect(),
            client_endpoints: client_endpoints.iter().map(|s| s.to_string()).collect(),
            partitioning_version: 4,
        }
    }

    fn local(endpoints: &[&str]) -> Vec<String> {
        endpoints.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn local_owner_is_served_here() {
        let mut route = route(&[], &[]);
        route.owner_is_local = true;
        assert_eq!(
            decide_route(Some(&route), &local(&["me:1"]), true),
            RouteDecision::Local
        );
        assert_eq!(decide_route(None, &[], true), RouteDecision::Unknown);
    }

    #[test]
    fn private_owner_is_forwarded_by_a_public_broker() {
        let route = route(&[], &["me:1"]);
        assert_eq!(
            decide_route(Some(&route), &local(&["me:1"]), true),
            RouteDecision::Forward {
                owner: "owner".into()
            }
        );
    }

    #[test]
    fn a_broker_that_may_not_forward_redirects_to_the_gateway_but_never_to_itself() {
        // A private broker or a forwarded session sends the client to the
        // gateway the topology names.
        let route = route(&[], &["gw:1"]);
        assert_eq!(
            decide_route(Some(&route), &[], false),
            RouteDecision::Redirect {
                endpoints: local(&["gw:1"]),
                partitioning_version: 4
            }
        );
        // When that gateway is this broker, a redirect would bounce the client
        // straight back here.
        assert_eq!(
            decide_route(Some(&route), &local(&["gw:1"]), false),
            RouteDecision::Unknown
        );
    }

    #[test]
    fn public_owner_with_its_own_address_gets_a_redirect() {
        let route = route(&["owner:1"], &["owner:1"]);
        assert_eq!(
            decide_route(Some(&route), &local(&["me:1"]), true),
            RouteDecision::Redirect {
                endpoints: local(&["owner:1"]),
                partitioning_version: 4
            }
        );
    }

    #[test]
    fn owner_sharing_this_brokers_address_is_forwarded_not_redirected() {
        // Two public brokers behind one load balancer address: a redirect to
        // the shared address would land on any broker, possibly this one.
        let route = route(&["lb:9876"], &["lb:9876"]);
        assert_eq!(
            decide_route(Some(&route), &local(&["lb:9876"]), true),
            RouteDecision::Forward {
                owner: "owner".into()
            }
        );
        assert_eq!(
            decide_route(Some(&route), &local(&["lb:9876"]), false),
            RouteDecision::Unknown
        );
    }

    #[test]
    fn redirect_frame_carries_parsed_endpoints() {
        let frame = client_redirect_frame(
            7,
            "orders",
            Partition::new(1),
            None,
            &local(&["gw:9876", "bad"]),
            9,
        )
        .unwrap();
        let redirect = wire::decode_redirect(&frame).unwrap();
        assert_eq!(frame.request_id, 7);
        assert_eq!(redirect.owner_endpoints.len(), 1);
        assert_eq!(redirect.owner_endpoints[0].target(), "gw:9876");
        assert_eq!(redirect.partitioning_version, 9);
        assert!(client_redirect_frame(7, "orders", Partition::new(1), None, &[], 9).is_none());
    }
}
