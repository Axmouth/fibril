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
//!
//! Queue and stream publishes, queue subscribes (exclusive consumer groups
//! included), stream subscribes and settles are relayed. For exclusive
//! groups the gateway settles one member id per client session and always
//! sends it upstream, so owners never mint one for a forwarded session.
//!
//! A lost upstream link is resumed with the owner's resume identity for the
//! gateway's reconnect grace. A resumed session keeps its subscriptions and
//! held deliveries stay settleable. A session that ends for good closes in
//! order, which tells an owner that knows `release_on_close` to release it
//! at once.

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
    ERR_UNAVAILABLE, ErrorMsg, ForwardedSession, ForwardedSessionOk, HelloOk, MAX_FORWARD_HOPS, Op,
    Partition, ReasonCode, ReconcileAction, ReconcileClient, ReconcilePolicy, ReconcileResult,
    ReconcileSubscription, Redirect, ResumeIdentity, ResumeOutcome, SubscribeOk,
    SubscriptionClosed,
    frame::Frame,
    handler::ClientTopologySource,
    helper::{Conn, try_decode, try_encode},
    replication::{PeerTlsConnector, ProtocolOwnerPeerAuth, open_protocol_owner_session},
    wire,
};

type SubKey = (String, Partition, Option<String>);
type FrameSink = mpsc::Sender<Frame>;
type UpstreamSink = futures::stream::SplitSink<Conn, Frame>;

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

/// Resume attempts back off from this delay, doubling up to
/// [`RESUME_MAX_BACKOFF`], until the resume window ends.
const RESUME_FIRST_BACKOFF: Duration = Duration::from_millis(100);
const RESUME_MAX_BACKOFF: Duration = Duration::from_secs(1);

/// How long the gateway keeps trying to resume a lost upstream session when
/// no reconnect grace was configured.
pub const DEFAULT_RESUME_WINDOW_MS: u64 = 5_000;

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
    resume_window_ms: AtomicU64,
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
            resume_window_ms: AtomicU64::new(DEFAULT_RESUME_WINDOW_MS),
        }
    }

    /// How long a lost upstream session is retried with its resume identity.
    /// The broker passes its own `reconnect_grace_ms`, which matches the
    /// owners' grace when the cluster uses one setting. `0` turns resume off.
    pub fn set_resume_window_ms(&self, window_ms: u64) {
        self.resume_window_ms.store(window_ms, Ordering::Relaxed);
    }

    pub fn resume_window(&self) -> Duration {
        Duration::from_millis(self.resume_window_ms.load(Ordering::Relaxed))
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

/// Queue subscriptions and stream subscriptions share the key shape, a
/// topic is one or the other. Streams take no part in reconcile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubKind {
    Queue,
    Stream,
}

/// Exclusive consumer group fields of a queue subscription. `member_id` is
/// always the id the gateway settled for the client session.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CohortFields {
    pub consumer_group: Option<String>,
    pub consumer_target: Option<u32>,
    pub member_id: Option<Uuid>,
}

/// A subscription as the client asked for it.
#[derive(Debug, Clone)]
struct SubSpec {
    key: SubKey,
    kind: SubKind,
    prefetch: u32,
    auto_ack: bool,
    cohort: CohortFields,
}

impl SubSpec {
    fn reconcile(&self, sub_id: u64) -> ReconcileSubscription {
        ReconcileSubscription {
            sub_id,
            topic: self.key.0.clone(),
            group: self.key.2.clone(),
            partition: self.key.1,
            auto_ack: self.auto_ack,
            prefetch: self.prefetch,
            consumer_group: self.cohort.consumer_group.clone(),
            consumer_target: self.cohort.consumer_target,
            member_id: self.cohort.member_id,
        }
    }
}

/// A forwarded subscription as the gateway's client session sees it.
#[derive(Debug, Clone)]
struct ForwardedSub {
    owner: String,
    upstream_id: u64,
    client_sub_id: u64,
    spec: SubSpec,
}

/// A forwarded subscription on one upstream session, by owner sub id.
#[derive(Debug, Clone)]
struct UpstreamSub {
    client_sub_id: u64,
    spec: SubSpec,
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
    /// Whether `key` names a queue or a stream partition, for re-routing
    /// after the owner refuses.
    pub kind: SubKind,
    /// Where the answer goes.
    pub reply: FrameSink,
    /// Whether the client waits for an answer.
    pub confirm: bool,
    pub prefetch: u32,
    pub auto_ack: bool,
    pub cohort: CohortFields,
}

fn is_subscribe(op: ForwardOp) -> bool {
    matches!(op, ForwardOp::Subscribe | ForwardOp::SubscribeStream)
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

    fn subs(&self) -> std::sync::MutexGuard<'_, HashMap<SubKey, ForwardedSub>> {
        self.subs.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether the client session has a forwarded queue subscription for `key`.
    pub(crate) fn has_sub(&self, key: &SubKey) -> bool {
        self.subs()
            .get(key)
            .is_some_and(|sub| sub.spec.kind == SubKind::Queue)
    }

    /// The forwarded queue subscription for `key` in reconcile form, with its
    /// client-facing id.
    pub(crate) fn reconcile_view(&self, key: &SubKey) -> Option<ReconcileSubscription> {
        self.subs()
            .get(key)
            .filter(|sub| sub.spec.kind == SubKind::Queue)
            .map(|sub| sub.spec.reconcile(sub.client_sub_id))
    }

    /// Keys of the forwarded queue subscriptions. Streams are not reconciled.
    pub(crate) fn sub_keys(&self) -> Vec<SubKey> {
        self.subs()
            .iter()
            .filter(|(_, sub)| sub.spec.kind == SubKind::Queue)
            .map(|(key, _)| key.clone())
            .collect()
    }

    /// The exclusive group member id this client session already uses on
    /// forwarded subscriptions, live or still waiting for the owner. Every
    /// exclusive subscribe of one session must use one member id.
    pub(crate) fn cohort_member(&self) -> Option<Uuid> {
        let live = self
            .subs()
            .values()
            .find_map(|sub| sub.spec.cohort.member_id);
        if live.is_some() {
            return live;
        }
        let upstreams: Vec<Arc<Upstream>> = self
            .upstreams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        upstreams.iter().find_map(|upstream| {
            upstream
                .state()
                .pending
                .values()
                .find_map(|pending| pending.cohort.member_id)
        })
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
            ForwardOp::Subscribe | ForwardOp::SubscribeStream => None,
        };
        self.send_request(request, 1, permit).await
    }

    /// Relay a subscribe and wait for the answer the client would have
    /// received. Used to restore a subscription during a reconcile.
    pub(crate) async fn forward_and_wait(
        &self,
        mut request: ForwardRequest,
    ) -> Result<Frame, ForwardReject> {
        let (reply, mut answers) = mpsc::channel(1);
        request.reply = reply;
        request.confirm = true;
        self.forward(request).await?;
        answers
            .recv()
            .await
            .ok_or_else(|| ForwardReject::new("the session to the partition owner closed"))
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
        let (write_permit, pending_id) = if request.confirm {
            let deadline =
                Instant::now() + Duration::from_millis(self.forwarder.limits().forward_timeout_ms);
            let pending = Pending {
                op: request.op,
                client_req_id: request.frame.request_id,
                reply: request.reply,
                original: request.frame,
                key: request.key,
                kind: request.kind,
                attempt,
                deadline,
                started: Instant::now(),
                prefetch: request.prefetch,
                auto_ack: request.auto_ack,
                cohort: request.cohort,
                permit,
                sent: false,
            };
            if !upstream.insert_pending(upstream_req_id, pending) {
                return Err(ForwardReject::new(
                    "the session to the partition owner closed, retry shortly",
                ));
            }
            (None, Some(upstream_req_id))
        } else {
            (permit, None)
        };
        let sent = upstream
            .tx
            .send(UpstreamWrite::Frame {
                frame,
                permit: write_permit,
                pending: pending_id,
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
    /// subscription. A stream `Ack` is a cursor commit. Returns false when no
    /// forwarded subscription matches, so the settle is ignored like one for
    /// an unknown subscription.
    pub(crate) async fn relay_settle(&self, key: &SubKey, frame: Frame) -> bool {
        let Some(sub) = self.subs().get(key).cloned() else {
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
                pending: None,
            })
            .await
            .is_ok()
        {
            self.forwarder.stats.bytes_out(bytes);
            upstream.touch();
        }
        true
    }

    /// Close forwarded queue subscriptions the client no longer wants. The
    /// owner learns it from a reconcile on the upstream session that lists
    /// only the subscriptions to keep, so it releases the closed ones'
    /// deliveries at once. Returns the closed subscriptions with
    /// client-facing ids.
    pub(crate) async fn close_subs(&self, keys: &[SubKey]) -> Vec<ReconcileSubscription> {
        let mut closed = Vec::new();
        let mut by_upstream: HashMap<(String, u64), ()> = HashMap::new();
        {
            let mut subs = self.subs();
            for key in keys {
                if subs
                    .get(key)
                    .is_some_and(|sub| sub.spec.kind == SubKind::Queue)
                    && let Some(sub) = subs.remove(key)
                {
                    by_upstream.insert((sub.owner.clone(), sub.upstream_id), ());
                    closed.push(sub.spec.reconcile(sub.client_sub_id));
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
                            pending: None,
                        })
                        .await;
                }
            }
        }
        closed
    }

    /// End every upstream session for good: the client session is gone. Each
    /// owner gets an empty reconcile first, so it closes the subscriptions and
    /// redelivers their unsettled messages, then an orderly close, which an
    /// owner that knows `release_on_close` takes as the end of the session.
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
        self.subs().clear();
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
                    pending: None,
                });
            }
            let _ = upstream.tx.try_send(UpstreamWrite::Close);
            // Also wakes a session waiting on a dormant client or retrying a
            // resume. It writes what is already queued, the reconcile
            // included, before closing.
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

    /// Record a subscription the owner confirmed. A stream subscribe for a
    /// partition the session already reads replaces the old one, as it does
    /// on a direct connection, and the old mapping is dropped so its late
    /// deliveries are discarded.
    fn insert_sub(
        &self,
        upstream: &Upstream,
        owner_sub_id: u64,
        client_sub_id: u64,
        spec: SubSpec,
    ) {
        upstream.state().subs.insert(
            owner_sub_id,
            UpstreamSub {
                client_sub_id,
                spec: spec.clone(),
            },
        );
        let replaced = self.subs().insert(
            spec.key.clone(),
            ForwardedSub {
                owner: upstream.owner.clone(),
                upstream_id: upstream.id,
                client_sub_id,
                spec,
            },
        );
        if let Some(old) = replaced
            && let Some(old_upstream) = self.current_upstream(&old.owner, old.upstream_id)
        {
            old_upstream
                .state()
                .subs
                .retain(|_, sub| sub.client_sub_id != old.client_sub_id);
        }
    }

    /// Drop the session's view of subscriptions that ended on `upstream`.
    fn forget_upstream_subs(&self, upstream_id: u64, client_sub_ids: &[u64]) {
        self.subs().retain(|_, sub| {
            !(sub.upstream_id == upstream_id && client_sub_ids.contains(&sub.client_sub_id))
        });
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
        /// The pending request this frame carries, marked as sent when it
        /// is written.
        pending: Option<u64>,
    },
    Close,
}

struct Pending {
    op: ForwardOp,
    client_req_id: u64,
    reply: FrameSink,
    original: Frame,
    key: SubKey,
    kind: SubKind,
    attempt: u8,
    deadline: Instant,
    started: Instant,
    prefetch: u32,
    auto_ack: bool,
    cohort: CohortFields,
    permit: Option<BytesPermit>,
    /// Written to the owner's socket at least once. A request lost before
    /// that answers `503`, after it the outcome is unknown.
    sent: bool,
}

impl Pending {
    fn spec(&self) -> SubSpec {
        SubSpec {
            key: self.key.clone(),
            kind: self.kind,
            prefetch: self.prefetch,
            auto_ack: self.auto_ack,
            cohort: self.cohort.clone(),
        }
    }
}

#[derive(Default)]
struct UpstreamState {
    pending: HashMap<u64, Pending>,
    subs: HashMap<u64, UpstreamSub>,
    connected: bool,
    closed: bool,
    /// Owner resume identity, from the last HELLO answer.
    resume: Option<ResumeIdentity>,
    /// After a resume: the reconcile request id and the owner sub ids it
    /// asked the owner to keep. Any the owner no longer had are closed.
    resume_check: Option<(u64, Vec<u64>)>,
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

    fn mark_sent(&self, request_id: u64) {
        if let Some(pending) = self.state().pending.get_mut(&request_id) {
            pending.sent = true;
        }
    }

    fn forget_subs(&self, keys: &[SubKey]) {
        self.state()
            .subs
            .retain(|_, sub| sub.spec.kind != SubKind::Queue || !keys.contains(&sub.spec.key));
    }

    /// Queue subscriptions to keep in a reconcile: the live ones, plus
    /// subscribes still waiting for the owner's answer (the owner matches by
    /// key, so their id does not matter). Streams are not reconciled.
    fn reconcile_list(&self) -> Vec<ReconcileSubscription> {
        let state = self.state();
        let live = state
            .subs
            .iter()
            .filter(|(_, sub)| sub.spec.kind == SubKind::Queue)
            .map(|(upstream_sub_id, sub)| sub.spec.reconcile(*upstream_sub_id));
        let waiting = state
            .pending
            .values()
            .filter(|pending| pending.op == ForwardOp::Subscribe)
            .map(|pending| pending.spec().reconcile(0));
        live.chain(waiting).collect()
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
        result = open_upstream(&forwarder, &upstream, &identity, limits.upstream_connect_timeout_ms, None) => result,
        _ = upstream.cancel.cancelled() => Err("the client session closed".to_string()),
    };
    let mut conn = match connected {
        Ok((conn, hello_ok)) => {
            forwarder.record_connect_result(&upstream.owner, true);
            {
                let mut state = upstream.state();
                state.connected = true;
                state.resume = Some(resume_identity(&hello_ok));
            }
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

    let end = loop {
        let end = serve_upstream(
            &session,
            &forwarder,
            &upstream,
            &local_endpoints,
            &mut rx,
            &mut transport,
            conn,
        )
        .await;
        let UpstreamEnd::Lost(reason) = end else {
            break end;
        };
        // Requests that reached the socket have an unknown outcome now.
        // Everything still queued is sent on the resumed session.
        fail_sent_requests(&forwarder, &upstream, &identity, &reason).await;
        let resumable = !upstream.state().subs.is_empty()
            && !forwarder.resume_window().is_zero()
            && !upstream.cancel.is_cancelled();
        if !resumable {
            break UpstreamEnd::Lost(reason);
        }
        tracing::info!(
            owner_node = %upstream.owner,
            client_id = %identity.client_id,
            reason = %reason,
            "upstream session lost, resuming"
        );
        match resume_upstream(&session, &forwarder, &upstream, &mut transport, &identity).await {
            Some(resumed) => conn = resumed,
            None => break UpstreamEnd::Lost(reason),
        }
    };
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

fn resume_identity(hello_ok: &HelloOk) -> ResumeIdentity {
    ResumeIdentity {
        owner_id: hello_ok.owner_id,
        client_id: hello_ok.client_id,
        resume_token: hello_ok.resume_token,
    }
}

/// Relay frames both ways on one connection until it ends.
async fn serve_upstream(
    session: &Weak<ForwardSession>,
    forwarder: &Arc<Forwarder>,
    upstream: &Arc<Upstream>,
    local_endpoints: &[String],
    rx: &mut mpsc::Receiver<UpstreamWrite>,
    transport: &mut watch::Receiver<Option<FrameSink>>,
    conn: Conn,
) -> UpstreamEnd {
    let (mut sink, mut stream) = conn.split();
    let mut cached_tx = None;
    let mut tick = tokio::time::interval(UPSTREAM_TICK);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let read_timeout = Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL * 3);
    let mut last_read = Instant::now();

    let end = loop {
        tokio::select! {
            write = rx.recv() => match write {
                Some(UpstreamWrite::Frame { frame, permit, pending }) => {
                    // Marked before the write: a failed write may still have
                    // reached the owner.
                    if let Some(request_id) = pending {
                        upstream.mark_sent(request_id);
                    }
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
                    forwarder.stats.bytes_in(frame.payload.len() as u64);
                    if frame.opcode == Op::Ping as u16 {
                        last_read = Instant::now();
                        if let Ok(pong) = try_encode(Op::Pong, frame.request_id, &()) {
                            if let Err(err) = sink.send(pong).await {
                                break UpstreamEnd::Lost(format!("write failed: {err}"));
                            }
                        }
                        continue;
                    }
                    let handled = handle_upstream_frame(
                        session,
                        forwarder,
                        upstream,
                        local_endpoints,
                        transport,
                        &mut cached_tx,
                        &mut sink,
                        frame,
                    )
                    .await;
                    // A relay can wait on a slow client for a long time. The
                    // owner was heard from before that, and frames it sent
                    // meanwhile wait in the socket.
                    last_read = Instant::now();
                    if let Err(reason) = handled {
                        break UpstreamEnd::Lost(reason);
                    }
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
                        session.remove_upstream(upstream);
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
    if matches!(end, UpstreamEnd::Lost(_)) {
        // Nothing to close in order. Resume or the owner's grace takes over.
        return end;
    }
    if matches!(end, UpstreamEnd::Closed) {
        // Flush what the session queued before closing, such as the reconcile
        // that releases the owner's subscriptions.
        while let Ok(UpstreamWrite::Frame { frame, pending, .. }) = rx.try_recv() {
            if let Some(request_id) = pending {
                upstream.mark_sent(request_id);
            }
            if sink.send(frame).await.is_err() {
                break;
            }
        }
    }
    // An orderly close tells the owner this session is over.
    let _ = sink.close().await;
    end
}

/// Answer requests that reached the owner's socket before the link was
/// lost. A publish may have been applied: `504`. A subscribe is dropped by
/// the owner's reconcile after a resume, or with the session: `503`.
async fn fail_sent_requests(
    forwarder: &Arc<Forwarder>,
    upstream: &Arc<Upstream>,
    identity: &ForwardIdentity,
    reason: &str,
) {
    let sent: Vec<Pending> = {
        let mut state = upstream.state();
        let ids: Vec<u64> = state
            .pending
            .iter()
            .filter(|(_, pending)| pending.sent)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .filter_map(|id| state.pending.remove(&id))
            .collect()
    };
    for pending in sent {
        answer_lost(forwarder, upstream, identity, &pending, reason).await;
    }
}

/// Answer one request whose upstream session was lost or closed.
async fn answer_lost(
    forwarder: &Arc<Forwarder>,
    upstream: &Upstream,
    identity: &ForwardIdentity,
    pending: &Pending,
    reason: &str,
) {
    let (code, outcome, message) = if !pending.sent || is_subscribe(pending.op) {
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
    answer(pending, code, message).await;
}

/// Reconnect a lost upstream session with its resume identity, retrying
/// with backoff for the resume window. A resumed session keeps the owner's
/// subscriptions: a reconcile confirms which ones it still has. Any other
/// outcome is a fresh session, so the subscriptions are closed for the
/// client's supervisor to resubscribe. `None` when the owner could not be
/// reached in time.
async fn resume_upstream(
    session: &Weak<ForwardSession>,
    forwarder: &Arc<Forwarder>,
    upstream: &Arc<Upstream>,
    transport: &mut watch::Receiver<Option<FrameSink>>,
    identity: &ForwardIdentity,
) -> Option<Conn> {
    let deadline = Instant::now() + forwarder.resume_window();
    let mut backoff = RESUME_FIRST_BACKOFF;
    let mut last_error: String;
    loop {
        let resume = upstream.state().resume.clone();
        let connect_timeout = forwarder
            .limits()
            .upstream_connect_timeout_ms
            .min(
                deadline
                    .saturating_duration_since(Instant::now())
                    .as_millis() as u64,
            )
            .max(1);
        let attempt = tokio::select! {
            result = open_upstream(forwarder, upstream, identity, connect_timeout, resume) => result,
            _ = upstream.cancel.cancelled() => return None,
        };
        match attempt {
            Ok((mut conn, hello_ok)) => {
                let resumed = hello_ok.resume_outcome == ResumeOutcome::Resumed;
                upstream.state().resume = Some(resume_identity(&hello_ok));
                if resumed {
                    let keep: Vec<(u64, ReconcileSubscription)> = upstream
                        .state()
                        .subs
                        .iter()
                        .filter(|(_, sub)| sub.spec.kind == SubKind::Queue)
                        .map(|(owner_sub_id, sub)| {
                            (*owner_sub_id, sub.spec.reconcile(*owner_sub_id))
                        })
                        .collect();
                    let request_id = upstream.next_request_id();
                    upstream.state().resume_check =
                        Some((request_id, keep.iter().map(|(id, _)| *id).collect()));
                    let reconcile = try_encode(
                        Op::ReconcileClient,
                        request_id,
                        &ReconcileClient {
                            policy: ReconcilePolicy::Conservative,
                            subscriptions: keep.into_iter().map(|(_, sub)| sub).collect(),
                        },
                    );
                    let sent = match reconcile {
                        Ok(frame) => conn.send(frame).await.is_ok(),
                        Err(_) => true,
                    };
                    if sent {
                        forwarder.stats.upstream_resume(true);
                        tracing::info!(
                            owner_node = %upstream.owner,
                            client_id = %identity.client_id,
                            subscriptions = upstream.state().subs.len(),
                            "upstream session resumed"
                        );
                        return Some(conn);
                    }
                    last_error = "the resumed connection failed at once".into();
                } else {
                    forwarder.stats.upstream_resume(false);
                    tracing::info!(
                        owner_node = %upstream.owner,
                        client_id = %identity.client_id,
                        outcome = ?hello_ok.resume_outcome,
                        "upstream session reconnected without its old session, \
                         closing its subscriptions for the client to resubscribe"
                    );
                    let all: Vec<u64> = upstream.state().subs.keys().copied().collect();
                    close_upstream_subs(
                        session,
                        upstream,
                        transport,
                        &all,
                        "the partition owner no longer had the forwarded session",
                    )
                    .await;
                    return Some(conn);
                }
            }
            Err(message) => last_error = message,
        }
        if Instant::now() + backoff >= deadline {
            break;
        }
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = upstream.cancel.cancelled() => return None,
        }
        backoff = (backoff * 2).min(RESUME_MAX_BACKOFF);
    }
    forwarder.stats.upstream_resume(false);
    tracing::info!(
        owner_node = %upstream.owner,
        client_id = %identity.client_id,
        "could not resume the upstream session in time: {last_error}"
    );
    None
}

/// Close subscriptions of one upstream session for the client: drop them
/// from both maps and send `SubscriptionClosed` with `OwnerMoved`, which
/// every SDK's supervisor answers by resubscribing.
async fn close_upstream_subs(
    session: &Weak<ForwardSession>,
    upstream: &Upstream,
    transport: &mut watch::Receiver<Option<FrameSink>>,
    owner_sub_ids: &[u64],
    reason: &str,
) {
    let removed: Vec<u64> = {
        let mut state = upstream.state();
        owner_sub_ids
            .iter()
            .filter_map(|id| state.subs.remove(id))
            .map(|sub| sub.client_sub_id)
            .collect()
    };
    if removed.is_empty() {
        return;
    }
    if let Some(session) = session.upgrade() {
        session.forget_upstream_subs(upstream.id, &removed);
    }
    send_closes(transport, &removed, reason).await;
}

async fn send_closes(
    transport: &mut watch::Receiver<Option<FrameSink>>,
    client_sub_ids: &[u64],
    reason: &str,
) {
    let mut cached_tx = None;
    for client_sub_id in client_sub_ids {
        let closed = SubscriptionClosed {
            sub_id: *client_sub_id,
            code: ReasonCode::OwnerMoved,
            message: format!("{reason}, resubscribe"),
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

async fn open_upstream(
    forwarder: &Forwarder,
    upstream: &Upstream,
    identity: &ForwardIdentity,
    connect_timeout_ms: u64,
    resume: Option<ResumeIdentity>,
) -> Result<(Conn, HelloOk), String> {
    let setup = async {
        let (mut conn, hello_ok) = open_protocol_owner_session(
            upstream.addr.clone(),
            Some(&forwarder.cfg.auth),
            forwarder.cfg.tls.as_ref(),
            "fibril-gateway",
            env!("CARGO_PKG_VERSION"),
            connect_timeout_ms,
            resume,
        )
        .await
        .map_err(|err| err.to_string())?;
        let mut marker = ForwardedSession {
            origin_node: forwarder.cfg.node_id.clone(),
            hop_count: MAX_FORWARD_HOPS,
            acting_user: identity.acting_user.clone(),
            client_id: identity.client_id,
            client_conn_id: identity.conn_id,
            client_addr: identity.client_addr.clone(),
            release_on_close: true,
        };
        let mut request_id = 3;
        loop {
            let frame = wire::encode_forwarded_session(request_id, &marker)
                .map_err(|err| err.to_string())?;
            conn.send(frame)
                .await
                .map_err(|err| format!("forwarded session send failed: {err}"))?;
            let reply = recv_reply(&mut conn, request_id).await?;
            if reply.opcode == Op::ForwardedSessionOk as u16 {
                let ok: ForwardedSessionOk = try_decode(&reply).map_err(|err| err.to_string())?;
                tracing::debug!(
                    owner_node = %upstream.owner,
                    forwarding_version = ok.forwarding_version,
                    release_on_close = marker.release_on_close,
                    "owner accepted forwarded session"
                );
                return Ok((conn, hello_ok));
            }
            let detail = try_decode::<ErrorMsg>(&reply)
                .map(|err| format!("{} {}", err.code, err.message))
                .unwrap_or_else(|_| format!("opcode {}", reply.opcode));
            if marker.release_on_close {
                // An owner before forwarding version 2 cannot read the
                // option. Without it, the owner keeps an orderly closed
                // session for its grace, holding nothing after the empty
                // reconcile, which is safe.
                tracing::debug!(
                    owner_node = %upstream.owner,
                    "owner refused release_on_close ({detail}), sending the session without it"
                );
                marker.release_on_close = false;
                request_id += 1;
                continue;
            }
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

/// The owner's answer to `request_id` during setup, answering pings.
async fn recv_reply(conn: &mut Conn, request_id: u64) -> Result<Frame, String> {
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
        if frame.request_id == request_id {
            return Ok(frame);
        }
    }
}

/// Send a frame to the client. Waits on the client's own connection, which
/// is the backpressure: a slow client stops reading its upstream and only
/// its upstream. Meanwhile the owner gets a ping each heartbeat interval, so
/// a long wait does not look like a dead link to it. `Err` when the upstream
/// write failed.
async fn relay_to_client(
    transport: &mut watch::Receiver<Option<FrameSink>>,
    cached_tx: &mut Option<FrameSink>,
    upstream: &Upstream,
    sink: &mut UpstreamSink,
    frame: Frame,
) -> Result<(), String> {
    let send = crate::v1::handler::send_to_current_transport(transport, cached_tx, frame);
    tokio::pin!(send);
    let every = Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL);
    let mut keepalive = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    loop {
        tokio::select! {
            _ = &mut send => return Ok(()),
            _ = upstream.cancel.cancelled() => return Ok(()),
            _ = keepalive.tick() => {
                let ping = try_encode(Op::Ping, upstream.next_request_id(), &())
                    .map_err(|err| err.to_string())?;
                sink.send(ping)
                    .await
                    .map_err(|err| format!("write failed: {err}"))?;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_upstream_frame(
    session: &Weak<ForwardSession>,
    forwarder: &Arc<Forwarder>,
    upstream: &Arc<Upstream>,
    local_endpoints: &[String],
    transport: &mut watch::Receiver<Option<FrameSink>>,
    cached_tx: &mut Option<FrameSink>,
    sink: &mut UpstreamSink,
    frame: Frame,
) -> Result<(), String> {
    let opcode = frame.opcode;
    if opcode == Op::Deliver as u16 {
        let Ok(upstream_sub_id) = wire::deliver_sub_id(&frame) else {
            return Ok(());
        };
        let client_sub_id = upstream
            .state()
            .subs
            .get(&upstream_sub_id)
            .map(|sub| sub.client_sub_id);
        let Some(client_sub_id) = client_sub_id else {
            // A delivery for a subscription this session closed, replaced or
            // never confirmed. The owner redelivers it after its lease.
            return Ok(());
        };
        if let Ok(rewritten) = wire::rewrite_deliver_sub_id(&frame, client_sub_id) {
            relay_to_client(transport, cached_tx, upstream, sink, rewritten).await?;
        }
        return Ok(());
    }
    if opcode == Op::AssignmentChanged as u16 {
        // Exclusive group assignment pushes carry no subscription id, every
        // field is already the client's own. Relayed as they are.
        return relay_to_client(transport, cached_tx, upstream, sink, frame).await;
    }
    if opcode == Op::SubscriptionClosed as u16 {
        let Ok(closed) = wire::decode_subscription_closed(&frame) else {
            return Ok(());
        };
        let removed = upstream.state().subs.remove(&closed.sub_id);
        let Some(sub) = removed else {
            return Ok(());
        };
        if let Some(session) = session.upgrade() {
            session.forget_upstream_subs(upstream.id, &[sub.client_sub_id]);
        }
        let relayed = SubscriptionClosed {
            sub_id: sub.client_sub_id,
            ..closed
        };
        if let Ok(frame) = wire::encode_subscription_closed(frame.request_id, &relayed) {
            relay_to_client(transport, cached_tx, upstream, sink, frame).await?;
        }
        return Ok(());
    }
    if opcode == Op::ReconcileResult as u16 {
        let check = {
            let mut state = upstream.state();
            match &state.resume_check {
                Some((request_id, _)) if *request_id == frame.request_id => {
                    state.resume_check.take()
                }
                _ => None,
            }
        };
        if let Some((_, asked)) = check {
            let kept: Vec<u64> = try_decode::<ReconcileResult>(&frame)
                .map(|result| {
                    result
                        .subscriptions
                        .into_iter()
                        .filter(|item| item.action == ReconcileAction::Keep)
                        .filter_map(|item| item.server.or(item.client).map(|sub| sub.sub_id))
                        .collect()
                })
                .unwrap_or_default();
            let gone: Vec<u64> = asked.into_iter().filter(|id| !kept.contains(id)).collect();
            if !gone.is_empty() {
                tracing::info!(
                    owner_node = %upstream.owner,
                    closed = gone.len(),
                    "the resumed upstream session lost some subscriptions"
                );
                close_upstream_subs(
                    session,
                    upstream,
                    transport,
                    &gone,
                    "the subscription ended on the partition owner while the gateway reconnected",
                )
                .await;
            }
        }
        return Ok(());
    }
    let is_reply = opcode == Op::PublishOk as u16
        || opcode == Op::Error as u16
        || opcode == Op::SubscribeOk as u16
        || opcode == Op::SubscribeErr as u16
        || opcode == Op::Redirect as u16;
    if !is_reply {
        // GoingAway, TopologyUpdate and the like stay on the upstream
        // session. A draining owner's moves arrive as closes.
        tracing::trace!(opcode, owner_node = %upstream.owner, "ignored upstream frame");
        return Ok(());
    }
    let Some(pending) = upstream.take_pending(frame.request_id) else {
        return Ok(());
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
    Ok(())
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
        let mut spec = pending.spec();
        spec.prefetch = ok.prefetch;
        session.insert_sub(upstream, ok.sub_id, client_sub_id, spec);
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
                kind: pending.kind,
                reply: pending.reply.clone(),
                confirm: true,
                prefetch: pending.prefetch,
                auto_ack: pending.auto_ack,
                cohort: pending.cohort.clone(),
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
            let group = match pending.kind {
                SubKind::Queue => pending.key.2.as_deref(),
                SubKind::Stream => None,
            };
            match client_redirect_frame(
                pending.client_req_id,
                &pending.key.0,
                pending.key.1,
                group,
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
    match pending.kind {
        SubKind::Queue => {
            session
                .topology
                .queue_route(&pending.key.0, pending.key.1, pending.key.2.as_deref())
        }
        SubKind::Stream => session.topology.stream_route(&pending.key.0, pending.key.1),
    }
}

/// Answer a pending request with an error code, as `SubscribeErr` for a
/// subscribe and `Error` otherwise.
async fn answer(pending: &Pending, code: u16, message: impl Into<String>) {
    let op = if is_subscribe(pending.op) {
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
        if let UpstreamWrite::Frame {
            frame,
            pending: None,
            ..
        } = write
            && (frame.opcode == Op::Publish as u16 || frame.opcode == Op::PublishDelayed as u16)
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
        answer_lost(forwarder, upstream, identity, &pending, &reason).await;
    }

    if subs.is_empty() {
        return;
    }
    let client_sub_ids: Vec<u64> = subs.values().map(|sub| sub.client_sub_id).collect();
    if let Some(session) = strong.as_ref() {
        session.forget_upstream_subs(upstream.id, &client_sub_ids);
    }
    // Tell the client each subscription ended so its supervisor resubscribes,
    // through this broker, to whoever owns the partition now.
    send_closes(
        transport,
        &client_sub_ids,
        &format!("the gateway lost its session to the partition owner ({reason})"),
    )
    .await;
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
