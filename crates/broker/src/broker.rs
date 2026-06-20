use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use fibril_metrics::{BrokerStats, QueuesStateSnapshot};
use futures::FutureExt;
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use fibril_storage::{
    DeliverableMessage, DeliveryTag, Group, Offset, Partition, StorageError, StoredMessage, Topic,
};
use fibril_util::unix_millis;
use uuid::Uuid;

use crate::coordination::{
    CohortMemberInfo, ConsumerGroupKey, Coordination, CoordinationSnapshot,
    ExclusiveConsumerGroups, LocalAssignmentIntent, LocalAssignmentTransition,
    LocalCohortMembership, PartitionAssignment, QueueIdentity, StaticCoordination,
    StickyConsumerGroupAssignor, plan_local_assignment_transitions,
};
use crate::queue_engine::{
    DestroyOutcome, EvictOutcome, QueueEngine, QueuePromotionOutcome, SettleKind,
    SettleRequest as EngineSettleRequest, StromaEngine,
};
use stroma_core::{
    AckEventMeta, AppendCompletion, AppendResult, CompletionPair, IoError, KeratinAppendCompletion,
    MessageContentType, MessageHeaders, NackEventMeta, StromaDebugSnapshot, StromaError,
    StromaMetrics, TaskGroup, UnixMillis,
};

// Replication/clustering types + traits now live in `replication.rs`; re-export so
// existing `fibril_broker::broker::*` import paths keep resolving (clustering-module
// separation, brick 1).
pub use crate::replication::*;

#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("engine error: {0}")]
    Engine(#[from] StromaError),

    #[error("channel closed")]
    ChannelClosed,

    #[error("not owner for queue {topic}/{partition}/{group:?}")]
    NotOwner {
        topic: Topic,
        partition: Partition,
        group: Option<Group>,
    },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("invalid replicated {stream} progress: {reason}")]
    InvalidReplicationProgress {
        stream: &'static str,
        reason: String,
    },

    #[error(
        "not enough in-sync replicas for {topic}/{partition}: {in_sync} in sync, {required} required"
    )]
    NotEnoughInSyncReplicas {
        topic: String,
        partition: Partition,
        in_sync: usize,
        required: usize,
    },

    #[error("unknown: {0}")]
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SettleType {
    Ack,
    Nack {
        requeue: Option<bool>,
        not_before: Option<UnixMillis>,
    },
    Reject {
        requeue: Option<bool>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SettleKindGroupKey {
    Ack,
    Nack { requeue: bool },
    Reject { requeue: bool },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SettleRequest {
    pub settle_type: SettleType,
    pub delivery_tag: DeliveryTag,
}

impl SettleRequest {
    pub fn is_ack(&self) -> bool {
        matches!(self.settle_type, SettleType::Ack)
    }

    pub fn is_nack(&self) -> bool {
        matches!(self.settle_type, SettleType::Nack { .. })
    }

    pub fn is_reject(&self) -> bool {
        matches!(self.settle_type, SettleType::Reject { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerConfig {
    pub prefetch: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueEvictionSkip {
    NotTracked,
    Active,
    NotIdleEnough,
    PendingSettles,
    HasBrokerDeliveries,
    HasInflight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueEvictionAttempt {
    Skipped(QueueEvictionSkip),
    Storage(EvictOutcome),
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self { prefetch: 32 }
    }
}

impl ConsumerConfig {
    pub fn with_prefetch_count(mut self, count: usize) -> Self {
        self.prefetch = count;
        self
    }
}

pub struct ConsumerHandle {
    pub sub_id: u64,
    pub client_id: Uuid,
    pub config: ConsumerConfig,
    pub group: Option<Box<str>>,
    pub topic: Box<str>,
    pub partition: Partition,
    pub messages: mpsc::Receiver<DeliverableMessage>,
    pub settler: mpsc::Sender<SettleRequest>,
    pub pending_settles: Arc<AtomicUsize>,
    pub activity_lease: ConsumerLease,
}

impl ConsumerHandle {
    pub async fn settle(&self, req: SettleRequest) -> Result<(), BrokerError> {
        if self.settler.is_closed() {
            tracing::debug!("Settle channel is closed for consumer {}", self.sub_id);
        }
        let s = self.pending_settles.fetch_add(1, Ordering::AcqRel);
        tracing::debug!("Pending settles incremented to {}", s + 1);
        self.settler.send(req).await.map_err(|_| {
            let s = self.pending_settles.fetch_sub(1, Ordering::AcqRel);
            tracing::debug!(
                "Pending settles decremented to {} due to send failure",
                s - 1
            );
            BrokerError::ChannelClosed
        })
    }

    pub async fn recv(&mut self) -> Option<DeliverableMessage> {
        self.messages.recv().await
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

pub struct PublishRequest {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Result<Offset, BrokerError>>,
    pub not_before: Option<UnixMillis>,
    pub require_confirm: bool,
    pub published: u64,
    pub publish_received: u64,
    pub content_type: Option<MessageContentType>,
    pub extra: HashMap<String, String>,
}

#[derive(Debug)]
pub struct PublisherHandle {
    // Intentionally not Clone. A broker publisher handle owns one sink task and
    // one active-publisher lease. Sharing a cloned sender would make lease
    // accounting easy to misunderstand, while cloning with a new lease would
    // need a matching sink lifetime. Call get_publisher again for another
    // independently tracked publisher.
    pub publisher: mpsc::Sender<PublishRequest>,
}

impl PublisherHandle {
    pub async fn publish(
        &self,
        payload: Vec<u8>,
        published: u64,
        publish_received: u64,
        content_type: Option<MessageContentType>,
        extra: HashMap<String, String>,
    ) -> Result<oneshot::Receiver<Result<u64, BrokerError>>, BrokerError> {
        let (tx, rx) = oneshot::channel();

        self.publisher
            .send(PublishRequest {
                payload,
                reply: tx,
                require_confirm: true,
                not_before: None,
                published,
                publish_received,
                content_type,
                extra,
            })
            .await
            .map_err(|_| BrokerError::ChannelClosed)?;

        // // TODO: move to separare task per publisher
        // tokio::spawn(async move {
        //     if let Err(e) = rx.await {
        //         tracing::error!("Error receiving publish response: {e:?}");
        //     }
        // });

        Ok(rx)
    }

    pub async fn publish_no_confirm(
        &self,
        payload: Vec<u8>,
        published: u64,
        publish_received: u64,
        content_type: Option<MessageContentType>,
        extra: HashMap<String, String>,
    ) -> Result<oneshot::Receiver<Result<u64, BrokerError>>, BrokerError> {
        let (tx, rx) = oneshot::channel();

        self.publisher
            .send(PublishRequest {
                payload,
                reply: tx,
                require_confirm: false,
                not_before: None,
                publish_received,
                published,
                content_type,
                extra,
            })
            .await
            .map_err(|_| BrokerError::ChannelClosed)?;

        // // TODO: move to separare task per publisher
        // tokio::spawn(async move {
        //     if let Err(e) = rx.await {
        //         tracing::error!("Error receiving publish response: {e:?}");
        //     }
        // });

        Ok(rx)
    }

    pub async fn publish_delayed(
        &self,
        payload: Vec<u8>,
        published: u64,
        publish_received: u64,
        content_type: Option<MessageContentType>,
        extra: HashMap<String, String>,
        not_before: u64,
    ) -> Result<oneshot::Receiver<Result<u64, BrokerError>>, BrokerError> {
        let (tx, rx) = oneshot::channel();

        self.publisher
            .send(PublishRequest {
                payload,
                reply: tx,
                require_confirm: true,
                not_before: Some(not_before),
                published,
                publish_received,
                content_type,
                extra,
            })
            .await
            .map_err(|_| BrokerError::ChannelClosed)?;
        Ok(rx)
    }

    pub async fn publish_no_confirm_delayed(
        &self,
        payload: Vec<u8>,
        published: u64,
        publish_received: u64,
        content_type: Option<MessageContentType>,
        extra: HashMap<String, String>,
        not_before: u64,
    ) -> Result<oneshot::Receiver<Result<u64, BrokerError>>, BrokerError> {
        let (tx, rx) = oneshot::channel();

        self.publisher
            .send(PublishRequest {
                payload,
                reply: tx,
                require_confirm: false,
                not_before: Some(not_before),
                published,
                publish_received,
                content_type,
                extra,
            })
            .await
            .map_err(|_| BrokerError::ChannelClosed)?;
        Ok(rx)
    }
}

pub struct ConfirmStream {
    rx: mpsc::Receiver<Offset>,
}

impl ConfirmStream {
    pub async fn recv(&mut self) -> Option<Offset> {
        self.rx.recv().await
    }

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub inflight_ttl_ms: u64,
    pub expiry_poll_min_ms: u64,
    pub expiry_batch_max: usize,
    pub delivery_poll_max_ms: u64,
    pub queue_idle_evict_after_ms: Option<u64>,
    pub queue_idle_sweep_interval_ms: u64,
    pub replication_confirm_timeout_ms: u64,
    pub replication_caught_up_poll_ms: u64,
    pub replication_retry_poll_ms: u64,
    pub replication_checkpoint_retry_poll_ms: u64,
    pub replication_max_messages_per_read: usize,
    pub replication_max_events_per_read: usize,
    pub replication_max_bytes_per_read: usize,
    pub replication_max_iterations_per_tick: usize,
    /// Minimum in-sync replicas (owner + healthy followers) required to accept
    /// a replica-durable publish. 1 (default) disables the floor.
    pub replication_min_in_sync_replicas: usize,
    /// How recently a follower must have reported progress to count as in-sync.
    pub replication_isr_timeout_ms: u64,
    /// Use credit-based streaming replication on the follower instead of the
    /// pull loop. Default true (fold + failover validated); pull stays the
    /// automatic fallback on checkpoint/error.
    pub replication_stream_enabled: bool,
    /// Linger (microseconds) the streaming follower spends gathering more
    /// contiguous frames before applying them as one fsynced batch. 0 = drain-only.
    pub replication_stream_apply_linger_us: u64,
    /// Byte cap on a single coalesced streaming-apply (peak memory vs fsync
    /// amortization). Pairs with `replication_stream_apply_linger_us`.
    pub replication_stream_apply_max_merge_bytes: u64,
    /// In-flight batch buffer depth (credit window) for the streaming follower
    /// (setup-time: applies on the next stream).
    pub replication_stream_buffer_batches: usize,
    /// Partition count for a queue declared without an explicit count.
    pub default_partition_count: u32,
    /// Soft target partitions-per-consumer for exclusive consumer groups. When
    /// the balanced load exceeds it the group is flagged under-provisioned
    /// (coverage is never reduced). `None` disables the signal.
    pub default_consumer_target: Option<usize>,
}
impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            inflight_ttl_ms: 60_000,
            expiry_poll_min_ms: 200,
            expiry_batch_max: 8192,
            delivery_poll_max_ms: 500,
            queue_idle_evict_after_ms: None,
            queue_idle_sweep_interval_ms: 60_000,
            replication_confirm_timeout_ms: 5_000,
            replication_caught_up_poll_ms: 1_000,
            replication_retry_poll_ms: 100,
            replication_checkpoint_retry_poll_ms: 5_000,
            // One fsync per replicated append call, so amortize over more records
            // (see ReplicationSettings::default for the rationale). max_bytes_per_read
            // bounds per-batch memory for large payloads.
            replication_max_messages_per_read: 2048,
            replication_max_events_per_read: 2048,
            replication_max_bytes_per_read: 8 * 1024 * 1024,
            replication_max_iterations_per_tick: 8,
            replication_min_in_sync_replicas: 1,
            replication_isr_timeout_ms: 10_000,
            replication_stream_enabled: true,
            replication_stream_apply_linger_us: 2_000,
            replication_stream_apply_max_merge_bytes: 16 * 1024 * 1024,
            replication_stream_buffer_batches: FOLLOWER_STREAM_BUFFER_BATCHES,
            default_partition_count: 1,
            default_consumer_target: None,
        }
    }
}

pub trait QueueOwnership: std::fmt::Debug + Send + Sync {
    fn owns_queue(&self, topic: &str, partition: Partition, group: Option<&str>) -> bool;
}

#[derive(Debug, Clone, Default)]
pub struct OwnAllQueues;

impl QueueOwnership for OwnAllQueues {
    fn owns_queue(&self, _topic: &str, _partition: Partition, _group: Option<&str>) -> bool {
        true
    }
}

#[derive(Debug, Clone, Default)]
pub struct StaticQueueOwnership {
    owned: HashSet<OwnedQueue>,
}

impl StaticQueueOwnership {
    pub fn new(owned: HashSet<OwnedQueue>) -> Self {
        Self { owned }
    }
}

impl QueueOwnership for StaticQueueOwnership {
    fn owns_queue(&self, topic: &str, partition: Partition, group: Option<&str>) -> bool {
        self.owned
            .contains(&OwnedQueue::new(topic, partition, group))
    }
}

impl QueueOwnership for StaticCoordination {
    fn owns_queue(&self, topic: &str, partition: Partition, group: Option<&str>) -> bool {
        Coordination::owns_queue(self, topic, partition, group)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnedQueue {
    pub topic: Topic,
    pub partition: Partition,
    pub group: Option<Group>,
}

impl OwnedQueue {
    pub fn new(topic: impl Into<Topic>, partition: Partition, group: Option<&str>) -> Self {
        Self {
            topic: topic.into(),
            partition,
            group: group.map(str::to_string),
        }
    }
}


/// In-flight batch buffer depth for a follower stream (bounds memory together
/// with the byte credit).
pub(crate) const FOLLOWER_STREAM_BUFFER_BATCHES: usize = 8;


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerAssignmentTransitionApply {
    Applied(LocalAssignmentIntent),
    Deferred {
        intent: LocalAssignmentIntent,
        reason: &'static str,
    },
    Noop(LocalAssignmentIntent),
}

// ---------------- Internal state ----------------

type ConsumerId = u64;

#[derive(Debug)]
struct ConsumerState {
    sub_id: ConsumerId,
    tx: mpsc::Sender<DeliverableMessage>,
    // flow control
    prefetch: AtomicUsize,
    inflight: AtomicUsize,
}

impl ConsumerState {
    fn can_accept(&self) -> bool {
        self.inflight.load(Ordering::Acquire) < self.prefetch.load(Ordering::Acquire)
    }
    fn inc_inflight(&self) {
        self.inflight.fetch_add(1, Ordering::AcqRel);
    }
    fn dec_inflight(&self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueActivitySnapshot {
    pub active_publishers: usize,
    pub active_subscribers: usize,
    pub idle_since_ms: Option<u64>,
    pub last_active_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct QueueActivityState {
    active_publishers: usize,
    active_subscribers: usize,
    idle_since_ms: Option<u64>,
    last_active_ms: Option<u64>,
}

#[derive(Debug, Default)]
struct QueueActivity {
    state: Mutex<QueueActivityState>,
    last_used_ms: AtomicU64,
}

impl QueueActivity {
    fn snapshot(&self) -> QueueActivitySnapshot {
        let state = self.state.lock().expect("queue activity lock poisoned");
        let last_used_ms = self.last_used_ms.load(Ordering::Acquire);
        QueueActivitySnapshot {
            active_publishers: state.active_publishers,
            active_subscribers: state.active_subscribers,
            idle_since_ms: state.idle_since_ms,
            last_active_ms: (last_used_ms > 0).then_some(last_used_ms),
        }
    }

    fn add_publisher(self: &Arc<Self>) -> PublisherLease {
        let mut state = self.state.lock().expect("queue activity lock poisoned");
        let now = unix_millis();
        state.active_publishers += 1;
        state.idle_since_ms = None;
        state.last_active_ms = Some(now);
        self.last_used_ms.store(now, Ordering::Release);
        drop(state);

        PublisherLease {
            activity: self.clone(),
        }
    }

    fn drop_publisher(&self) {
        let mut state = self.state.lock().expect("queue activity lock poisoned");
        debug_assert!(state.active_publishers > 0);
        state.active_publishers = state.active_publishers.saturating_sub(1);
        Self::mark_idle_if_empty(&mut state, &self.last_used_ms);
    }

    fn add_subscriber(self: &Arc<Self>) -> ConsumerLease {
        let mut state = self.state.lock().expect("queue activity lock poisoned");
        let now = unix_millis();
        state.active_subscribers += 1;
        state.idle_since_ms = None;
        state.last_active_ms = Some(now);
        self.last_used_ms.store(now, Ordering::Release);
        drop(state);

        ConsumerLease {
            activity: self.clone(),
        }
    }

    fn drop_subscriber(&self) {
        let mut state = self.state.lock().expect("queue activity lock poisoned");
        debug_assert!(state.active_subscribers > 0);
        state.active_subscribers = state.active_subscribers.saturating_sub(1);
        Self::mark_idle_if_empty(&mut state, &self.last_used_ms);
    }

    fn touch(&self) {
        self.last_used_ms.store(unix_millis(), Ordering::Release);
    }

    fn mark_idle_if_no_leases(&self) {
        let mut state = self.state.lock().expect("queue activity lock poisoned");
        Self::mark_idle_if_empty(&mut state, &self.last_used_ms);
    }

    fn mark_idle_if_empty(state: &mut QueueActivityState, last_used_ms: &AtomicU64) {
        if state.active_publishers == 0 && state.active_subscribers == 0 {
            let now = unix_millis();
            state.idle_since_ms.get_or_insert(now);
            state.last_active_ms = Some(now);
            last_used_ms.store(now, Ordering::Release);
        }
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueueEvictionObservation {
    pub attempted_at_ms: u64,
    #[serde(skip_serializing)]
    pub outcome: QueueEvictionAttempt,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SparseQueueEvictionObservation {
    pub attempted_at_ms: u64,
    pub kind: &'static str,
    pub outcome: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SparseQueueObservability {
    pub topic: String,
    pub group: Option<String>,
    pub active_publishers: usize,
    pub active_subscribers: usize,
    pub idle_since_ms: Option<u64>,
    pub idle_for_ms: Option<u64>,
    pub last_active_ms: Option<u64>,
    pub last_eviction_attempt: Option<SparseQueueEvictionObservation>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SparseQueueObservabilitySummary {
    pub tracked_queue_count: usize,
    pub active_queue_count: usize,
    pub idle_queue_count: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SparseQueueObservabilitySnapshot {
    pub queues: Vec<SparseQueueObservability>,
    pub summary: SparseQueueObservabilitySummary,
    pub replication_followers: Vec<FollowerReplicationWorkerObservability>,
    pub replication_summary: FollowerReplicationWorkerSummary,
    pub owned_replicas: Vec<OwnedQueueReplicaObservability>,
    pub owned_replica_summary: OwnedQueueReplicaSummary,
    pub replication_timing: ReplicationTimingSnapshot,
}

fn eviction_attempt_summary(
    observation: &QueueEvictionObservation,
) -> SparseQueueEvictionObservation {
    let (kind, outcome) = match observation.outcome {
        QueueEvictionAttempt::Skipped(skip) => ("skipped", eviction_skip_label(skip)),
        QueueEvictionAttempt::Storage(outcome) => ("storage", evict_outcome_label(outcome)),
    };
    SparseQueueEvictionObservation {
        attempted_at_ms: observation.attempted_at_ms,
        kind,
        outcome: outcome.into(),
    }
}

fn eviction_skip_label(skip: QueueEvictionSkip) -> &'static str {
    match skip {
        QueueEvictionSkip::NotTracked => "not_tracked",
        QueueEvictionSkip::Active => "active",
        QueueEvictionSkip::NotIdleEnough => "not_idle_enough",
        QueueEvictionSkip::PendingSettles => "pending_settles",
        QueueEvictionSkip::HasBrokerDeliveries => "has_broker_deliveries",
        QueueEvictionSkip::HasInflight => "has_inflight",
    }
}

fn evict_outcome_label(outcome: EvictOutcome) -> &'static str {
    match outcome {
        EvictOutcome::Evicted => "evicted",
        EvictOutcome::NotPresent => "not_present",
        EvictOutcome::NotMaterialized => "not_materialized",
        EvictOutcome::HasInflight => "has_inflight",
        EvictOutcome::RaceLost => "race_lost",
    }
}

#[derive(Debug)]
pub struct PublisherLease {
    activity: Arc<QueueActivity>,
}

impl Drop for PublisherLease {
    fn drop(&mut self) {
        self.activity.drop_publisher();
    }
}

#[derive(Debug)]
pub struct ConsumerLease {
    activity: Arc<QueueActivity>,
}

impl Drop for ConsumerLease {
    fn drop(&mut self) {
        self.activity.drop_subscriber();
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct QueueKey {
    pub(crate) tp: Topic,
    pub(crate) part: Partition,
    pub(crate) group: Option<Group>,
}

/// Tag -> delivery record (so we can validate settle)
#[derive(Debug, Clone)]
struct TagRecord {
    key: QueueKey,
    offset: Offset,
    consumer_id: ConsumerId,
}

/// One loop per queue (tp,part,group)
#[derive(Debug)]
pub(crate) struct QueueLoopState {
    started: AtomicBool,
    rr: AtomicU64,
    consumers: DashMap<ConsumerId, Arc<ConsumerState>>,
    activity: Arc<QueueActivity>,
    eviction_lock: AsyncMutex<()>,
    owner_runtime_shutdown: CancellationToken,
    // used to wake the delivery loop
    notify: tokio::sync::Notify,
    // used to wake caught-up follower long-poll reads
    pub(crate) replication_notify: tokio::sync::Notify,
    epoch: AtomicU64,
    /// Exclusive consumer-group gate: when set, deliver this partition's messages
    /// ONLY to the assigned consumer (the rest stay subscribed as standbys). The
    /// sentinel `NO_EXCLUSIVE_ASSIGNEE` means "no gate" — deliver to all
    /// consumers (the default competing-consumer behavior).
    exclusive_assignee: AtomicU64,
    /// Repartition drain gate: while true this partition holds ALL delivery. Set
    /// on a newly added partition during a grow transition so it does not deliver
    /// post-cutover (v_new) messages until its single source old partition has
    /// drained its pre-cutover (v_old) backlog, preserving per-key order across
    /// the remap. Cleared (and the partition delivers normally) once the source
    /// has drained. Default false — no transition in progress.
    delivery_held: AtomicBool,
    /// Repartition SHRINK gate: a surviving partition delivers messages with
    /// offset BELOW this boundary (its own pre-cutover backlog, all staying keys)
    /// but holds those at or above it (post-cutover, possibly moved keys whose old
    /// messages are still in a merged-away partition) until every merge source has
    /// drained. Sentinel `NO_HOLD_ABOVE` (u64::MAX) means no shrink hold.
    hold_above_offset: AtomicU64,
    /// Replica-durable VISIBILITY gate: the committed-replicated message
    /// watermark (next offset, exclusive). Delivery leases only offsets BELOW
    /// this, so a consumer never sees a message that is not yet durable on enough
    /// replicas (Kafka high-watermark model). Maintained from follower durable
    /// progress reports. Sentinel `NO_VISIBILITY_CEILING` (u64::MAX) means no gate
    /// (local-durable queues deliver as soon as a message is locally ready).
    committed_message_offset: AtomicU64,
}

/// Sentinel for [`QueueLoopState::exclusive_assignee`] meaning "no exclusive
/// gate" (deliver to all consumers).
const NO_EXCLUSIVE_ASSIGNEE: u64 = u64::MAX;

/// Sentinel for [`QueueLoopState::hold_above_offset`] meaning "no shrink hold"
/// (deliver every offset).
const NO_HOLD_ABOVE: u64 = u64::MAX;

/// Sentinel for [`QueueLoopState::committed_message_offset`] meaning "no
/// replica-durable visibility gate" (deliver every locally-ready offset).
const NO_VISIBILITY_CEILING: u64 = u64::MAX;

impl QueueLoopState {
    fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            rr: AtomicU64::new(0),
            consumers: DashMap::new(),
            activity: Arc::new(QueueActivity::default()),
            eviction_lock: AsyncMutex::new(()),
            owner_runtime_shutdown: CancellationToken::new(),
            notify: tokio::sync::Notify::new(),
            replication_notify: tokio::sync::Notify::new(),
            epoch: AtomicU64::new(1),
            exclusive_assignee: AtomicU64::new(NO_EXCLUSIVE_ASSIGNEE),
            delivery_held: AtomicBool::new(false),
            hold_above_offset: AtomicU64::new(NO_HOLD_ABOVE),
            committed_message_offset: AtomicU64::new(NO_VISIBILITY_CEILING),
        }
    }
    fn wake(&self) {
        self.epoch.fetch_add(1, Ordering::Release);
        self.notify.notify_one();
    }

    fn wake_replication_followers(&self) {
        self.replication_notify.notify_waiters();
    }

    fn wake_with_replication(&self) {
        self.wake();
        self.wake_replication_followers();
    }

    /// The current exclusive assignee, or `None` when delivery is open to all.
    fn exclusive_assignee(&self) -> Option<ConsumerId> {
        match self.exclusive_assignee.load(Ordering::Acquire) {
            NO_EXCLUSIVE_ASSIGNEE => None,
            id => Some(id),
        }
    }

    /// Gate delivery to a single consumer (`Some`) or reopen to all (`None`),
    /// waking the delivery loop so the change takes effect immediately.
    fn set_exclusive_assignee(&self, assignee: Option<ConsumerId>) {
        self.exclusive_assignee
            .store(assignee.unwrap_or(NO_EXCLUSIVE_ASSIGNEE), Ordering::Release);
        self.wake();
    }

    /// Whether this partition is holding delivery for a repartition transition.
    fn is_delivery_held(&self) -> bool {
        self.delivery_held.load(Ordering::Acquire)
    }

    /// Hold (`true`) or release (`false`) this partition's delivery for a
    /// repartition transition, waking the delivery loop so a release delivers any
    /// queued messages immediately.
    fn set_delivery_held(&self, held: bool) {
        self.delivery_held.store(held, Ordering::Release);
        self.wake();
    }

    /// The shrink hold boundary, or `None` when delivery is unbounded.
    fn hold_above_offset(&self) -> Option<Offset> {
        match self.hold_above_offset.load(Ordering::Acquire) {
            NO_HOLD_ABOVE => None,
            boundary => Some(boundary),
        }
    }

    /// Hold delivery at or above `boundary` (`Some`) or remove the shrink hold
    /// (`None`), waking the delivery loop so a release delivers immediately.
    fn set_hold_above_offset(&self, boundary: Option<Offset>) {
        self.hold_above_offset
            .store(boundary.unwrap_or(NO_HOLD_ABOVE), Ordering::Release);
        self.wake();
    }

    /// The replica-durable visibility ceiling (exclusive): deliver only offsets
    /// below this. `u64::MAX` (the sentinel) means no gate. Read on the delivery
    /// hot path, so it is a single atomic load with no lock.
    fn visibility_ceiling(&self) -> Offset {
        self.committed_message_offset.load(Ordering::Acquire)
    }

    /// Set the replica-durable visibility ceiling. `None` removes the gate
    /// (local-durable). Monotonic for a given incarnation: the committed
    /// watermark only advances, so a stale lower report never regresses it (a
    /// real reset, e.g. demotion/eviction, recreates the queue loop state).
    /// Wakes the delivery loop so newly-committed messages flow immediately.
    fn set_visibility_ceiling(&self, ceiling: Option<Offset>) {
        let next = ceiling.unwrap_or(NO_VISIBILITY_CEILING);
        let mut current = self.committed_message_offset.load(Ordering::Acquire);
        loop {
            // Never regress a real ceiling; setting the no-gate sentinel (MAX) or
            // a higher ceiling always wins.
            if next != NO_VISIBILITY_CEILING && next <= current && current != NO_VISIBILITY_CEILING {
                return;
            }
            match self.committed_message_offset.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        self.wake();
    }

    fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    async fn lock_for_eviction(&self) -> AsyncMutexGuard<'_, ()> {
        self.eviction_lock.lock().await
    }

    fn cancel_owner_runtime(&self) {
        self.owner_runtime_shutdown.cancel();
        self.wake();
    }
}

/// One per-partition delivery-gate update produced by recomputing an exclusive
/// cohort: `Some(sub_id)` gates the partition to that consumer, `None` reopens it
/// to all competing consumers.
type GateUpdate = (Partition, Option<ConsumerId>);

/// A member's exclusive-cohort assignment change, pushed to that member's
/// connection (informational; the gate enforces exclusivity regardless). The
/// protocol layer turns this into an `AssignmentChanged` frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExclusiveAssignmentUpdate {
    pub topic: Topic,
    pub group: Option<Group>,
    pub consumer_group: String,
    pub generation: u64,
    pub assigned: Vec<Partition>,
    pub added: Vec<Partition>,
    pub revoked: Vec<Partition>,
}

/// Broker-side routing for opt-in exclusive consumer groups (Model A): the pure
/// assignment registry plus the live `member -> partition -> sub_id` map needed
/// to translate a computed assignment into concrete per-partition delivery-gate
/// targets. The broker holds the authoritative view (which member subscribed
/// which partition under which connection), so on every change it recomputes the
/// union of subscribed partitions and live members and re-derives every gate.
struct ExclusiveGroupRouter {
    registry: ExclusiveConsumerGroups,
    /// cohort -> member (connection client_id) -> partition -> broker sub_id.
    subs: HashMap<ConsumerGroupKey, HashMap<String, HashMap<Partition, ConsumerId>>>,
    /// cohort -> member -> soft per-consumer target override (the member's
    /// desired max partitions). Members absent here fall back to the group
    /// default. Fed to the assignor on every recompute.
    targets: HashMap<ConsumerGroupKey, HashMap<String, usize>>,
    /// cohort -> member -> channel pushing that member's assignment changes to
    /// its connection. Registered by the handler on the member's first exclusive
    /// subscribe; dropped when the member leaves (closing the forwarder).
    notifiers: HashMap<
        ConsumerGroupKey,
        HashMap<String, mpsc::UnboundedSender<ExclusiveAssignmentUpdate>>,
    >,
    /// cohort -> partition -> assigned member id, supplied by the cross-broker
    /// coordinator. When present for a cohort, gates are resolved from this global
    /// plan (each owner gates its partitions to the assigned member's local
    /// sub_id) instead of the broker computing the assignment locally. Empty in
    /// single-node / un-coordinated mode (then local computation applies).
    external: HashMap<ConsumerGroupKey, HashMap<Partition, String>>,
    /// cohort -> generation of the external plan currently held. A plan whose
    /// generation is older than this is fenced (ignored), so a late or
    /// out-of-order slice never overwrites a newer one. Also the value reported
    /// for convergence observability.
    external_gen: HashMap<ConsumerGroupKey, u64>,
}

impl ExclusiveGroupRouter {
    fn new(target_per_consumer: Option<usize>) -> Self {
        Self {
            registry: ExclusiveConsumerGroups::new(
                // Sticky by default: a membership change moves the minimum
                // partitions, avoiding needless drain + cold-start churn.
                Arc::new(StickyConsumerGroupAssignor),
                target_per_consumer,
            ),
            subs: HashMap::new(),
            targets: HashMap::new(),
            notifiers: HashMap::new(),
            external: HashMap::new(),
            external_gen: HashMap::new(),
        }
    }

    /// True if a *different* exclusive cohort already has live members on the
    /// same `(topic, group)`. A queue supports a single cohort (the gate has one
    /// assignee slot, and there is no fan-out), so a second distinct cohort id
    /// would silently fight over the gate.
    fn has_conflicting_cohort(&self, key: &ConsumerGroupKey) -> bool {
        self.subs.iter().any(|(existing, members)| {
            existing.topic == key.topic
                && existing.group == key.group
                && existing.consumer_group != key.consumer_group
                && !members.is_empty()
        })
    }

    /// Register a member's assignment-change channel (idempotent per member —
    /// re-registering replaces the sender, closing any prior forwarder).
    fn register_notifier(
        &mut self,
        key: ConsumerGroupKey,
        member: String,
    ) -> mpsc::UnboundedReceiver<ExclusiveAssignmentUpdate> {
        let (tx, rx) = mpsc::unbounded_channel();
        self.notifiers.entry(key).or_default().insert(member, tx);
        rx
    }

    /// Record an exclusive member's subscription to one partition (with the broker
    /// sub_id it was assigned) and its optional soft target, then return the gate
    /// updates to apply.
    fn join(
        &mut self,
        key: ConsumerGroupKey,
        partition: Partition,
        member: String,
        sub_id: ConsumerId,
        target: Option<usize>,
    ) -> Vec<GateUpdate> {
        self.subs
            .entry(key.clone())
            .or_default()
            .entry(member.clone())
            .or_default()
            .insert(partition, sub_id);
        match target {
            Some(target) => {
                self.targets
                    .entry(key.clone())
                    .or_default()
                    .insert(member, target);
            }
            None => {
                if let Some(group_targets) = self.targets.get_mut(&key) {
                    group_targets.remove(&member);
                }
            }
        }
        // A coordinator-supplied global plan, when present, decides the gates
        // (this owner just resolves the assigned member to a local sub_id);
        // otherwise the broker computes the assignment locally.
        if self.external.contains_key(&key) {
            self.resolve_external_gates(&key)
        } else {
            self.recompute(&key, &[partition])
        }
    }

    /// Drop a member from the cohort and return the gate updates to apply.
    /// `partition` scopes the affected set so a partition the leaver solely served
    /// is reopened even if it falls out of the remaining union.
    ///
    /// DELIBERATE: this removes the member ENTIRELY (all its partitions), not just
    /// `partition`. The dominant caller is full-connection cleanup, which calls
    /// `leave` once per drained sub; whole-member removal makes the FIRST call
    /// reassign all the member's partitions to survivors at once (clean, instant
    /// failover — no transient stall while later per-partition calls trickle in).
    /// The cost is that a *partial* single-partition unsubscribe of a cohort
    /// (raw-protocol only — the high-level client subscribes/unsubscribes a cohort
    /// as a whole) also drops the whole member, briefly losing exclusivity on its
    /// other partitions until it re-subscribes. Acceptable for single-owner scope;
    /// revisit (per-partition leave) if/when client narrowing lands, where partial
    /// unsubscribe becomes a normal operation. See REPLICATION_WORKLOG limitation (c).
    fn leave(
        &mut self,
        key: ConsumerGroupKey,
        partition: Partition,
        member: &str,
    ) -> Vec<GateUpdate> {
        let Some(group_subs) = self.subs.get_mut(&key) else {
            return Vec::new();
        };
        group_subs.remove(member);
        if group_subs.is_empty() {
            self.subs.remove(&key);
        }
        if let Some(group_targets) = self.targets.get_mut(&key) {
            group_targets.remove(member);
            if group_targets.is_empty() {
                self.targets.remove(&key);
            }
        }
        // Drop the departed member's notifier (closes its forwarder); survivors
        // are notified of their new partitions by the recompute below.
        if let Some(group_notifiers) = self.notifiers.get_mut(&key) {
            group_notifiers.remove(member);
            if group_notifiers.is_empty() {
                self.notifiers.remove(&key);
            }
        }
        // Coordinator-driven cohorts keep their gates from the global plan until
        // the coordinator re-applies (a departed member's partitions just pause —
        // the gate points at a now-absent sub_id, never mis-delivers); otherwise
        // recompute locally.
        if self.external.contains_key(&key) {
            self.resolve_external_gates(&key)
        } else {
            self.recompute(&key, &[partition])
        }
    }

    /// Snapshot this broker's local cohort membership (cohort -> members+targets)
    /// for the controller to aggregate. A member appears once per cohort even if
    /// it subscribed several partitions here; the cluster member id makes it
    /// dedup-able across brokers.
    fn local_membership(&self) -> Vec<LocalCohortMembership> {
        self.subs
            .iter()
            .map(|(key, members)| {
                let targets = self.targets.get(key);
                let mut members: Vec<CohortMemberInfo> = members
                    .keys()
                    .map(|member| CohortMemberInfo {
                        member: member.clone(),
                        target: targets.and_then(|targets| targets.get(member).copied()),
                    })
                    .collect();
                members.sort_by(|a, b| a.member.cmp(&b.member));
                LocalCohortMembership {
                    topic: key.topic.clone(),
                    group: key.group.clone(),
                    consumer_group: key.consumer_group.clone(),
                    members,
                }
            })
            .collect()
    }

    /// Install (or replace) the coordinator's global assignment for a cohort at
    /// `generation` and return the gate updates to apply. From now on this
    /// cohort's gates follow the supplied `partition -> member` plan rather than
    /// local computation. A plan older than the one already held is fenced
    /// (ignored, no gate changes). An equal generation is still re-resolved,
    /// since local subs may have changed since the last apply (a member finally
    /// subscribing to its assigned partition).
    fn set_external(
        &mut self,
        key: ConsumerGroupKey,
        generation: u64,
        assignment: HashMap<Partition, String>,
    ) -> Vec<GateUpdate> {
        if let Some(&held) = self.external_gen.get(&key) {
            if generation < held {
                return Vec::new();
            }
        }
        self.external_gen.insert(key.clone(), generation);
        self.external.insert(key.clone(), assignment);
        self.resolve_external_gates(&key)
    }

    /// The generation of the external plan currently held for a cohort, if any.
    fn external_generation(&self, key: &ConsumerGroupKey) -> Option<u64> {
        self.external_gen.get(key).copied()
    }

    /// Resolve the cohort's coordinator-supplied plan into gate updates: gate each
    /// planned partition to the assigned member's LOCAL sub_id. Partitions whose
    /// assignee is not subscribed on this broker (owned elsewhere, or not yet
    /// arrived) are left as-is — never reopened.
    fn resolve_external_gates(&self, key: &ConsumerGroupKey) -> Vec<GateUpdate> {
        let Some(plan) = self.external.get(key) else {
            return Vec::new();
        };
        let group_subs = self.subs.get(key);
        let mut updates = Vec::new();
        for (partition, member) in plan {
            if let Some(sub_id) = group_subs
                .and_then(|subs| subs.get(member))
                .and_then(|parts| parts.get(partition))
                .copied()
            {
                updates.push((*partition, Some(sub_id)));
            }
        }
        updates
    }

    /// Recompute the cohort's assignment from the authoritative `subs` view and
    /// derive a gate update for every affected partition. A partition whose
    /// assigned member has not (yet) subscribed to it keeps its existing gate
    /// (ramp-up / drain); one with no assignee at all (cohort emptied) reopens.
    fn recompute(&mut self, key: &ConsumerGroupKey, affected: &[Partition]) -> Vec<GateUpdate> {
        let group_subs = self.subs.get(key);
        let mut union: BTreeSet<Partition> = BTreeSet::new();
        let mut members: Vec<String> = Vec::new();
        if let Some(group_subs) = group_subs {
            members.reserve(group_subs.len());
            for (member, parts) in group_subs {
                members.push(member.clone());
                union.extend(parts.keys().copied());
            }
        }
        let union: Vec<Partition> = union.into_iter().collect();

        let member_targets = self.targets.get(key).cloned().unwrap_or_default();
        let delta = self
            .registry
            .reconcile(key.clone(), union.clone(), members, member_targets);

        // partition -> assigned member (from the full per-member assignment).
        let mut assignee_of: HashMap<Partition, &String> = HashMap::new();
        for (member, change) in &delta.per_member {
            for partition in &change.assigned {
                assignee_of.insert(*partition, member);
            }
        }

        let mut targets: BTreeSet<Partition> = union.iter().copied().collect();
        targets.extend(affected.iter().copied());

        let mut updates = Vec::new();
        for partition in targets {
            match assignee_of.get(&partition) {
                Some(member) => {
                    if let Some(sub_id) = self
                        .subs
                        .get(key)
                        .and_then(|group_subs| group_subs.get(*member))
                        .and_then(|parts| parts.get(&partition))
                        .copied()
                    {
                        updates.push((partition, Some(sub_id)));
                    }
                    // else: assignee hasn't subscribed here yet — leave the gate
                    // as-is (prior assignee keeps draining; never reopen).
                }
                // No exclusive owner (cohort emptied or partition fell out) —
                // reopen to competing consumers.
                None => updates.push((partition, None)),
            }
        }

        // Push assignment-change notifications to members whose set changed and
        // that have a registered notifier (informational; gate already applied).
        if let Some(group_notifiers) = self.notifiers.get(key) {
            for (member, change) in &delta.per_member {
                if change.added.is_empty() && change.revoked.is_empty() {
                    continue;
                }
                if let Some(tx) = group_notifiers.get(member) {
                    let _ = tx.send(ExclusiveAssignmentUpdate {
                        topic: key.topic.clone(),
                        group: key.group.clone(),
                        consumer_group: key.consumer_group.clone(),
                        generation: delta.generation,
                        assigned: change.assigned.clone(),
                        added: change.added.clone(),
                        revoked: change.revoked.clone(),
                    });
                }
            }
        }

        updates
    }
}

#[derive(Debug, Clone)]
enum WakeReason {
    Notify,
    Timer,
    SettingsChanged,
}

impl std::fmt::Display for WakeReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WakeReason::Notify => write!(f, "notify"),
            WakeReason::Timer => write!(f, "timer"),
            WakeReason::SettingsChanged => write!(f, "settings_changed"),
        }
    }
}

// ---------------- Broker ----------------

// TODO cleanup old?
pub struct Broker<E: QueueEngine + std::fmt::Debug + Send + Sync + 'static> {
    pub(crate) cfg: Arc<ArcSwap<BrokerConfig>>,
    pub(crate) engine: E,
    pub(crate) shutdown_publishers: CancellationToken,
    shutdown_consumers: CancellationToken,
    shutdown_settle: CancellationToken,
    shutdown_expiry: CancellationToken,
    shutdown_queue_eviction: CancellationToken,

    next_sub_id: AtomicU64,
    next_tag_epoch: AtomicU64,

    queues: DashMap<QueueKey, Arc<QueueLoopState>>,
    records_by_tags: DashMap<DeliveryTag, TagRecord>,
    tags_by_key_offset: DashMap<(QueueKey, Offset), DeliveryTag>,
    queue_eviction_observations: DashMap<QueueKey, QueueEvictionObservation>,
    pub(crate) follower_replication_workers:
        DashMap<crate::coordination::QueueIdentity, Arc<FollowerReplicationWorkerRuntime>>,

    pending_settles: Arc<AtomicUsize>,
    settle_drained: Arc<Notify>,
    settings_changed: Arc<Notify>,
    settings_epoch: AtomicU64,

    pub(crate) task_group: Arc<TaskGroup>,

    metrics: Option<Arc<BrokerStats>>,
    pub(crate) ownership: Arc<dyn QueueOwnership>,

    /// Per-queue durable replication progress reported by followers (from
    /// stamped replication reads). Drives publish-confirm durability policies.
    pub(crate) replication_progress: Arc<DashMap<QueueKey, Arc<ReplicationProgressCell>>>,
    /// Broker-local aggregate timings for replicated confirms and follower pull.
    pub(crate) replication_timing: Arc<ReplicationTimingMetrics>,
    /// Latest assignment applied for each local queue (durability policy +
    /// replica set source for confirms). Maintained by the assignment watcher;
    /// empty for standalone brokers (local-durable confirms only).
    pub(crate) assignment_cache: Arc<DashMap<QueueKey, PartitionAssignment>>,
    /// Opt-in exclusive consumer-group routing: maps cohort membership to the
    /// per-partition delivery gate. Absent cohorts leave delivery competing.
    exclusive_groups: Mutex<ExclusiveGroupRouter>,
    /// In-progress live-repartition transitions, keyed by (topic, group). Cold
    /// path (a repartition is rare), so a plain Mutex is fine. Drives the
    /// per-partition `delivery_held` gate: a new partition is held until its
    /// source old partition has drained its pre-cutover backlog.
    repartition_transitions: Mutex<HashMap<RepartitionKey, RepartitionLocal>>,
}

type RepartitionKey = (String, Option<String>);

/// Broker-local state for one in-progress grow of a queue.
#[derive(Debug, Default)]
struct RepartitionLocal {
    version: u64,
    n_old: u32,
    n_new: u32,
    /// old partition -> cutover boundary offset (snapshotted by this owner).
    boundaries: HashMap<u32, Offset>,
    /// old partitions this owner has confirmed drained (reported via heartbeat).
    self_drained: HashSet<u32>,
    /// old partitions known drained cluster-wide (sources that let a new
    /// partition lift its hold).
    drained_sources: HashSet<u32>,
}

/// One owner's repartition drain status for a queue, surfaced for the heartbeat
/// label. Plain data so the broker does not depend on the coordination crate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepartitionDrainStatus {
    pub topic: String,
    pub group: Option<String>,
    pub version: u64,
    pub drained: Vec<u32>,
}

/// The merge sources of surviving partition `r` under a shrink to `n_new`: every
/// old partition `p < n_old` with `p % n_new == r`, i.e. `{r, r+n_new, ...}`. A
/// survivor may lift its hold only once all of these have drained.
fn shrink_sources(r: u32, n_new: u32, n_old: u32) -> impl Iterator<Item = u32> {
    let step = n_new.max(1) as usize;
    (r..n_old).step_by(step)
}

impl<E: QueueEngine + std::fmt::Debug + Clone + Send + Sync + 'static> Broker<E> {
    pub fn new(engine: E, cfg: BrokerConfig, metrics: Option<Arc<BrokerStats>>) -> Arc<Self> {
        Self::new_with_ownership(engine, cfg, metrics, Arc::new(OwnAllQueues))
    }

    pub fn new_with_ownership(
        engine: E,
        cfg: BrokerConfig,
        metrics: Option<Arc<BrokerStats>>,
        ownership: Arc<dyn QueueOwnership>,
    ) -> Arc<Self> {
        let metrics_clone = metrics.clone();
        if let Some(metrics) = metrics_clone {
            metrics.register_queue_state_callback(Some(Arc::new({
                let engine = engine.clone();
                move || {
                    let engine = engine.clone();
                    Box::pin(async move {
                        match engine.queue_stats_snapshot().await {
                            Ok(stats) => stats,
                            Err(e) => {
                                tracing::error!(
                                    "Failed to get queue stats snapshot for metrics: {e:?}"
                                );
                                QueuesStateSnapshot {
                                    queues: std::collections::HashMap::new(),
                                }
                            }
                        }
                    })
                }
            })));
        }

        let default_consumer_target = cfg.default_consumer_target;
        let this = Arc::new(Self {
            cfg: Arc::new(ArcSwap::from_pointee(cfg)),
            engine,
            shutdown_publishers: CancellationToken::new(),
            shutdown_consumers: CancellationToken::new(),
            shutdown_settle: CancellationToken::new(),
            shutdown_expiry: CancellationToken::new(),
            shutdown_queue_eviction: CancellationToken::new(),
            next_sub_id: AtomicU64::new(1),
            next_tag_epoch: AtomicU64::new(1),
            queues: DashMap::new(),
            records_by_tags: DashMap::new(),
            tags_by_key_offset: DashMap::new(),
            queue_eviction_observations: DashMap::new(),
            follower_replication_workers: DashMap::new(),
            replication_progress: Arc::new(DashMap::new()),
            replication_timing: Arc::new(ReplicationTimingMetrics::default()),
            assignment_cache: Arc::new(DashMap::new()),
            exclusive_groups: Mutex::new(ExclusiveGroupRouter::new(default_consumer_target)),
            repartition_transitions: Mutex::new(HashMap::new()),
            pending_settles: Arc::new(AtomicUsize::new(0)),
            settle_drained: Arc::new(Notify::new()),
            settings_changed: Arc::new(Notify::new()),
            settings_epoch: AtomicU64::new(1),
            task_group: Arc::new(TaskGroup::new()),
            metrics,
            ownership,
        });

        // expiry worker: keeps Stroma turning inflight -> ready again
        Self::spawn_expiry_worker(this.clone());
        Self::spawn_queue_eviction_worker(this.clone());

        this
    }

    pub fn engine(&self) -> E {
        self.engine.clone()
    }

    pub fn stroma_metrics(&self) -> Arc<StromaMetrics> {
        self.engine.metrics()
    }

    pub fn config_snapshot(&self) -> Arc<BrokerConfig> {
        self.cfg.load_full()
    }

    /// Recompute the replica-durable visibility ceiling for a queue from its
    /// cached assignment and follower durable progress, and store it on the
    /// queue loop state for the delivery gate. The committed watermark is the
    /// (nodes-1)-th largest follower `message_next` (the owner is always durable
    /// locally), i.e. the highest offset durable on the required number of
    /// replicas. Local-durable queues (nodes <= 1) are left ungated. No-op when
    /// the queue is not yet known locally.
    pub(crate) fn refresh_visibility_ceiling(&self, key: &QueueKey) {
        let Some(assignment) = self.assignment_cache.get(key).map(|entry| entry.clone()) else {
            return;
        };
        let Ok(requirement) = assignment.durability_requirement() else {
            return;
        };
        if requirement.nodes <= 1 {
            // Local-durable: deliver as soon as locally ready (no gate).
            return;
        }
        let required_followers = requirement.nodes - 1;
        let Some(qs) = self.queues.get(key).map(|entry| entry.value().clone()) else {
            return;
        };

        // A follower that has not reported counts as 0. The committed watermark
        // is the required-th largest follower message_next.
        let mut follower_nexts: Vec<Offset> = {
            match self.replication_progress.get(key) {
                Some(cell) => {
                    let followers = cell.lock_followers();
                    assignment
                        .followers
                        .iter()
                        .map(|node| followers.get(node).map(|p| p.message_next).unwrap_or(0))
                        .collect()
                }
                None => vec![0; assignment.followers.len()],
            }
        };
        follower_nexts.sort_unstable_by(|a, b| b.cmp(a));
        let watermark = follower_nexts
            .get(required_followers - 1)
            .copied()
            .unwrap_or(0);
        qs.set_visibility_ceiling(Some(watermark));
    }

    /// Cache the assignment governing a local queue (watcher-maintained).
    pub fn cache_queue_assignment(&self, assignment: &PartitionAssignment) {
        let key = QueueKey {
            tp: assignment.queue.topic.to_string(),
            part: assignment.queue.partition,
            group: assignment.queue.group.clone(),
        };
        self.assignment_cache.insert(key.clone(), assignment.clone());
        // A newly cached replica-durable assignment installs the visibility gate
        // (no-op for local-durable or an unmaterialized queue).
        self.refresh_visibility_ceiling(&key);
    }
}

impl<E: QueueEngine + std::fmt::Debug + Clone + Send + Sync + 'static> Broker<E> {
    pub fn update_config(&self, cfg: BrokerConfig) {
        self.cfg.store(Arc::new(cfg));
        self.settings_epoch.fetch_add(1, Ordering::AcqRel);
        self.settings_changed.notify_waiters();
        for qs in self.queues.iter() {
            qs.value().wake();
        }
    }

    fn settings_epoch(&self) -> u64 {
        self.settings_epoch.load(Ordering::Acquire)
    }

    pub(crate) fn ensure_queue_owner(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Result<(), BrokerError> {
        if self.ownership.owns_queue(topic, partition, group) {
            return Ok(());
        }

        Err(BrokerError::NotOwner {
            topic: topic.to_string(),
            partition,
            group: group.map(str::to_string),
        })
    }

    async fn materialize_owned_queue(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Result<(), BrokerError> {
        self.engine
            .materialize(topic, partition.id(), group)
            .await
            .map_err(BrokerError::Engine)?;

        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        if let Some(assignment) = self.assignment_cache.get(&key).map(|entry| entry.clone()) {
            self.engine
                .become_queue_owner_with_epoch(topic, partition.id(), group, assignment.epoch)
                .await?;
        }

        Ok(())
    }

    pub async fn shutdown(&self) {
        self.shutdown_publishers.cancel();
        self.shutdown_consumers.cancel();
        self.shutdown_settle.cancel();
        self.shutdown_expiry.cancel();
        self.shutdown_queue_eviction.cancel();
        self.stop_all_follower_replication_workers().await;
        self.task_group.shutdown().await;
        self.engine
            .shutdown()
            .await
            .inspect_err(|e| {
                tracing::error!("engine shutdown error: {:?}", e);
            })
            .ok();
    }

    pub async fn shutdown_graceful(&self) {
        tracing::info!("Broker initiating graceful shutdown");

        // Stop accepting new publishers, consumers immediately
        self.shutdown_publishers.cancel();
        self.shutdown_consumers.cancel();
        self.shutdown_expiry.cancel();
        self.shutdown_queue_eviction.cancel();
        self.stop_all_follower_replication_workers().await;
        tracing::debug!("Signaled shutdown to publishers, consumers, and expiry worker");
        self.shutdown_settle.cancel();
        tracing::debug!("Signaled shutdown to settle workers");

        // TODO: find a bette way to reliably wait for settles
        // tokio::time::sleep(Duration::from_millis(150)).await; // give some time for in-flight messages to be processed and settle requests to be sent

        // TODO: should we pending confirms too?
        // Wait until settle channels drained
        while self.pending_settles.load(Ordering::Acquire) != 0 {
            tracing::debug!(
                "Waiting for pending settles to drain: {}",
                self.pending_settles.load(Ordering::Acquire)
            );
            self.settle_drained.notified().await;
        }

        tracing::debug!("All pending settles drained, proceeding with shutdown");

        // Now stop tasks
        self.task_group.shutdown().await;

        // Shutdown engine
        if let Err(e) = self.engine.shutdown().await {
            tracing::error!("engine shutdown error: {:?}", e);
        }
    }

    pub async fn wait_for_pending_settles(&self) {
        loop {
            let notified = self.settle_drained.notified();
            if self.pending_settles.load(Ordering::Acquire) == 0 {
                break;
            }
            notified.await;
        }
    }

    pub async fn get_publisher(
        self: &Arc<Self>,
        topic: &str,
        partition: Partition,
        group: &Option<Group>,
    ) -> Result<(PublisherHandle, ConfirmStream), BrokerError> {
        // TODO: make configurable?
        let (tx, mut rx) = mpsc::channel::<PublishRequest>(16384);
        let (confirm_tx, confirm_rx) = mpsc::channel::<Offset>(16384);

        let engine = self.engine.clone();
        let shutdown = self.shutdown_publishers.clone();
        let tp: Topic = topic.to_string();
        let part: Partition = partition;
        let group = group.clone();

        self.ensure_queue_owner(&tp, part, group.as_deref())?;

        // Confirm gate (built early: later closures move `tp`/`group`).
        let confirm_gate = self.replication_confirm_gate();
        let confirm_key = QueueKey {
            tp: tp.clone(),
            part,
            group: group.clone(),
        };

        let qs = self
            .queue(&QueueKey {
                tp: tp.clone(),
                part,
                group: group.clone(),
            })
            .await;

        // TODO: make async by maybe making two tasks: one to receive publish requests and one to wait for completions and send confirms? Or use a bounded channel and backpressure?
        let (confirm_sink_tx, mut confirm_sink_rx) = mpsc::channel::<(
            oneshot::Receiver<Result<AppendResult, IoError>>,
            oneshot::Sender<Result<u64, BrokerError>>,
        )>(16384);
        let metrics = self.metrics.clone();
        let activity_lease = {
            let eviction_guard = qs.lock_for_eviction().await;
            let lease = qs.activity.add_publisher();
            if let Err(err) = self
                .materialize_owned_queue(&tp, part, group.as_deref())
                .await
            {
                drop(lease);
                drop(eviction_guard);
                return Err(err);
            }
            drop(eviction_guard);
            lease
        };
        let qs_publish = qs.clone();
        let qs_clone = qs.clone();
        let replication_timing = self.replication_timing.clone();
        let owner_runtime_shutdown = qs.owner_runtime_shutdown.clone();
        // TODO: do not keep handle(memory leak effective) if relevant connection dies
        self.task_group.spawn("publisher_sink", async move {
            let _activity_lease = activity_lease;
            const MAX_BATCH: usize = 256;
            const COALESCE_WINDOW: Duration = Duration::from_micros(250);
            const SMALL_BATCH: usize = 32;
            let mut last_flush = None;

            loop {
                let first = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    _ = owner_runtime_shutdown.cancelled() => break,
                    req = rx.recv() => req,
                };
                let Some(first) = first else {
                    break;
                };

                let mut batch = vec![first];

                // Drain immediately available
                while batch.len() < MAX_BATCH {
                    match rx.try_recv() {
                        Ok(req) => batch.push(req),
                        Err(_) => break,
                    }
                }

                // Coalesce only inside the window opened by the previous flush.
                // A first message after a quiet period should not wait for a batch.
                if batch.len() < SMALL_BATCH {
                    if let Some(last_flush) = last_flush {
                        let deadline = last_flush + COALESCE_WINDOW;
                        if deadline > tokio::time::Instant::now() {
                            while batch.len() < MAX_BATCH {
                                tokio::select! {
                                    biased;
                                    _ = shutdown.cancelled() => break,
                                    _ = owner_runtime_shutdown.cancelled() => break,
                                    res = tokio::time::timeout_at(deadline, rx.recv()) => {
                                        match res {
                                            Ok(Some(req)) => batch.push(req),
                                            _ => break,
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                let mut items = Vec::with_capacity(batch.len());
                let mut confirmations = Vec::with_capacity(batch.len());
                for PublishRequest {
                    payload,
                    reply,
                    not_before,
                    require_confirm: _,
                    published,
                    publish_received,
                    content_type,
                    extra,
                } in batch
                {
                    let headers = MessageHeaders {
                        published,
                        publish_received,
                        content_type,
                        extra,
                    };
                    let (completion, rx_completion) = KeratinAppendCompletion::pair();

                    confirmations.push((rx_completion, reply));
                    items.push(stroma_core::PublishItem {
                        headers,
                        payload,
                        not_before,
                        completion,
                    });
                }

                if items.is_empty() {
                    continue;
                }

                let batch_size = items.len();
                if let Err(err) = engine
                    .publish_batch(&tp, part.id(), group.as_deref(), items)
                    .await
                {
                    tracing::error!("publish_batch failed: {err:?}");
                    let err_msg = err.to_string();
                    for (_, reply) in confirmations {
                        let res = Err(BrokerError::Engine(StromaError::Io(err_msg.clone())));
                        if let Err(e) = reply.send(res) {
                            tracing::error!("Failed to send publish error response: {e:?}");
                        }
                    }
                    continue;
                }

                for confirmation in confirmations {
                    if let Err(e) = confirm_sink_tx.send(confirmation).await {
                        let (_, reply) = e.0;
                        if let Err(e) = reply.send(Err(BrokerError::ChannelClosed)) {
                            tracing::error!("Failed to send publish error response: {e:?}");
                        }
                    }
                }

                if let Some(metrics) = metrics.as_ref() {
                    metrics.published_many(batch_size as u64);
                }
                qs_publish.activity.touch();
                qs_publish.wake();
                last_flush = Some(tokio::time::Instant::now());
            }
        });

        // TODO: do not keep handle(memory leak effective) if relevant connection dies

        self.task_group.spawn("confirm_sink_loop", async move {
            while let Some((rx_completion, reply)) = confirm_sink_rx.recv().await {
                // Wait for durability
                match rx_completion.await {
                    Ok(Ok(append)) => {
                        let offset = append.base_offset;
                        if let Err(e) = confirm_tx.send(offset).await {
                            tracing::error!("Failed to send confirm offset: {e:?}");
                        }
                        replication_timing.record_replication_wake();
                        qs_clone.wake_with_replication();
                        // Local durability first, then the assignment's
                        // replication policy (replica/majority acks) before
                        // the producer sees the confirm.
                        let res: Result<u64, BrokerError> = confirm_gate
                            .await_confirm(&confirm_key, offset)
                            .await
                            .map(|()| offset);
                        if let Err(e) = reply.send(res) {
                            tracing::error!("Failed to send publish response: {e:?}");
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Append failed: {e:?}");
                        qs_clone.wake();
                        let res = Err(BrokerError::Engine(StromaError::Io(e.to_string())));
                        if let Err(e) = reply.send(res) {
                            tracing::error!("Failed to send publish response: {e:?}");
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to receive append completion: {e:?}");
                        qs_clone.wake();
                        let res = Err(BrokerError::Engine(StromaError::Io(
                            "append completion channel closed".to_string(),
                        )));
                        if let Err(e) = reply.send(res) {
                            tracing::error!("Failed to send publish response: {e:?}");
                        }
                    }
                }
            }
        });

        Ok((
            PublisherHandle { publisher: tx },
            ConfirmStream { rx: confirm_rx },
        ))
    }

    pub(crate) async fn queue(&self, key: &QueueKey) -> Arc<QueueLoopState> {
        self.queues
            .entry(key.clone())
            .or_insert_with(|| Arc::new(QueueLoopState::new()))
            .value()
            .clone()
    }

    fn stop_owner_queue_runtime(&self, key: &QueueKey) -> Option<Vec<Offset>> {
        let Some((_, qs)) = self.queues.remove(key) else {
            return None;
        };

        qs.cancel_owner_runtime();
        qs.consumers.clear();
        Some(self.take_delivery_offsets_for_queue(key))
    }

    fn take_delivery_offsets_for_queue(&self, key: &QueueKey) -> Vec<Offset> {
        let mut stale_tags = Vec::new();
        for entry in self.records_by_tags.iter() {
            if entry.value().key == *key {
                stale_tags.push((*entry.key(), entry.value().offset));
            }
        }

        let mut offsets = Vec::with_capacity(stale_tags.len());
        for (tag, offset) in stale_tags {
            self.records_by_tags.remove(&tag);
            self.tags_by_key_offset.remove(&(key.clone(), offset));
            offsets.push(offset);
        }

        offsets
    }

    async fn release_offsets_for_role_transition(
        &self,
        key: &QueueKey,
        offsets: Vec<Offset>,
    ) -> Result<(), BrokerError> {
        if offsets.is_empty() {
            return Ok(());
        }

        let count = offsets.len();
        let (done_tx, done_rx) = oneshot::channel::<bool>();
        let mut done_tx = Some(done_tx);
        let done = move |ok: bool| {
            if let Some(done_tx) = done_tx.take() {
                let _ = done_tx.send(ok);
            }
        };

        let reqs = offsets
            .into_iter()
            .map(|off| AckEventMeta { off })
            .collect();
        let completion: Box<dyn AppendCompletion<IoError>> = Box::new(SimpleCompletion::new(done));
        self.engine
            .release_inflight_batch(
                &key.tp,
                key.part.id(),
                key.group.as_deref(),
                reqs,
                completion,
            )
            .await?;

        match done_rx.await {
            Ok(true) => {
                tracing::debug!(
                    topic = %key.tp,
                    partition = key.part.id(),
                    group = ?key.group,
                    count,
                    "released broker-tracked deliveries before role transition"
                );
                Ok(())
            }
            Ok(false) => Err(BrokerError::Engine(StromaError::Io(format!(
                "role transition release failed for {count} broker-tracked deliveries"
            )))),
            Err(_) => Err(BrokerError::ChannelClosed),
        }
    }

    pub fn queue_activity_snapshot(
        &self,
        topic: &str,
        group: Option<&str>,
    ) -> Option<QueueActivitySnapshot> {
        let key = QueueKey {
            tp: topic.to_string(),
            part: Partition::ZERO,
            group: group.map(str::to_string),
        };
        self.queues.get(&key).map(|qs| qs.activity.snapshot())
    }

    pub fn is_queue_materialized(&self, topic: &str, group: Option<&str>) -> bool {
        self.engine.is_materialized(topic, 0, group)
    }

    pub async fn debug_snapshot(&self) -> Result<StromaDebugSnapshot, BrokerError> {
        Ok(self.engine.debug_snapshot().await?)
    }

    fn record_queue_eviction_attempt(
        &self,
        key: &QueueKey,
        outcome: QueueEvictionAttempt,
    ) -> QueueEvictionAttempt {
        if let Some(metrics) = &self.metrics {
            match outcome {
                QueueEvictionAttempt::Skipped(skip) => {
                    metrics.queue_cleanup_skipped(eviction_skip_label(skip));
                }
                QueueEvictionAttempt::Storage(storage) => {
                    metrics.queue_cleanup_storage_outcome(evict_outcome_label(storage));
                }
            }
        }
        self.queue_eviction_observations.insert(
            key.clone(),
            QueueEvictionObservation {
                attempted_at_ms: unix_millis(),
                outcome,
            },
        );
        outcome
    }

    fn queue_already_unloaded_since_last_activity(&self, key: &QueueKey) -> bool {
        // Admin inspection can materialize storage without creating a broker lease.
        // In that case the previous "unloaded" observation is stale.
        if self
            .engine
            .is_materialized(&key.tp, key.part.id(), key.group.as_deref())
        {
            return false;
        }
        let Some(observation) = self.queue_eviction_observations.get(key) else {
            return false;
        };
        let QueueEvictionAttempt::Storage(EvictOutcome::Evicted | EvictOutcome::NotMaterialized) =
            observation.outcome
        else {
            return false;
        };
        let Some(qs) = self.queues.get(key) else {
            return false;
        };
        let activity = qs.activity.snapshot();
        activity
            .idle_since_ms
            .is_some_and(|idle_since| idle_since <= observation.attempted_at_ms)
    }

    pub fn sparse_queue_observability_report(&self) -> SparseQueueObservabilitySnapshot {
        let now = unix_millis();
        let mut active_queue_count = 0;
        let mut idle_queue_count = 0;
        let mut queues: Vec<_> = self
            .queues
            .iter()
            .map(|entry| {
                let key = entry.key();
                let activity = entry.value().activity.snapshot();
                if activity.active_publishers > 0 || activity.active_subscribers > 0 {
                    active_queue_count += 1;
                }
                if activity.idle_since_ms.is_some() {
                    idle_queue_count += 1;
                }
                let last_eviction_attempt = self
                    .queue_eviction_observations
                    .get(key)
                    .map(|entry| eviction_attempt_summary(entry.value()));
                SparseQueueObservability {
                    topic: key.tp.to_string(),
                    group: key.group.as_ref().map(ToString::to_string),
                    active_publishers: activity.active_publishers,
                    active_subscribers: activity.active_subscribers,
                    idle_since_ms: activity.idle_since_ms,
                    idle_for_ms: activity.idle_since_ms.map(|idle| now.saturating_sub(idle)),
                    last_active_ms: activity.last_active_ms,
                    last_eviction_attempt,
                }
            })
            .collect();
        queues.sort_by(|a, b| a.group.cmp(&b.group).then_with(|| a.topic.cmp(&b.topic)));
        let summary = SparseQueueObservabilitySummary {
            tracked_queue_count: queues.len(),
            active_queue_count,
            idle_queue_count,
        };
        let (replication_followers, replication_summary) =
            self.follower_replication_observability_report();
        let (owned_replicas, owned_replica_summary) = self.owned_replica_observability_report();
        SparseQueueObservabilitySnapshot {
            queues,
            summary,
            replication_followers,
            replication_summary,
            owned_replicas,
            owned_replica_summary,
            replication_timing: self.replication_timing.snapshot(),
        }
    }

    pub fn sparse_queue_observability_snapshot(&self) -> Vec<SparseQueueObservability> {
        self.sparse_queue_observability_report().queues
    }

    pub async fn try_evict_inactive_queue(
        &self,
        topic: &str,
        group: Option<&str>,
        idle_for_ms: u64,
    ) -> Result<QueueEvictionAttempt, BrokerError> {
        let key = QueueKey {
            tp: topic.to_string(),
            part: Partition::ZERO,
            group: group.map(str::to_string),
        };

        let Some(qs) = self.queues.get(&key).map(|entry| entry.value().clone()) else {
            return Ok(self.record_queue_eviction_attempt(
                &key,
                QueueEvictionAttempt::Skipped(QueueEvictionSkip::NotTracked),
            ));
        };

        let eviction_guard = qs.lock_for_eviction().await;

        let activity = qs.activity.snapshot();
        if activity.active_publishers > 0 || activity.active_subscribers > 0 {
            drop(eviction_guard);
            return Ok(self.record_queue_eviction_attempt(
                &key,
                QueueEvictionAttempt::Skipped(QueueEvictionSkip::Active),
            ));
        }

        let Some(idle_since_ms) = activity.idle_since_ms else {
            drop(eviction_guard);
            return Ok(self.record_queue_eviction_attempt(
                &key,
                QueueEvictionAttempt::Skipped(QueueEvictionSkip::Active),
            ));
        };
        if unix_millis().saturating_sub(idle_since_ms) < idle_for_ms {
            drop(eviction_guard);
            return Ok(self.record_queue_eviction_attempt(
                &key,
                QueueEvictionAttempt::Skipped(QueueEvictionSkip::NotIdleEnough),
            ));
        }

        if self.pending_settles.load(Ordering::Acquire) > 0 {
            drop(eviction_guard);
            return Ok(self.record_queue_eviction_attempt(
                &key,
                QueueEvictionAttempt::Skipped(QueueEvictionSkip::PendingSettles),
            ));
        }

        if self
            .records_by_tags
            .iter()
            .any(|entry| entry.value().key == key)
        {
            drop(eviction_guard);
            return Ok(self.record_queue_eviction_attempt(
                &key,
                QueueEvictionAttempt::Skipped(QueueEvictionSkip::HasBrokerDeliveries),
            ));
        }

        if self
            .engine
            .has_inflight(&key.tp, key.part.id(), key.group.as_deref())
            .await?
        {
            drop(eviction_guard);
            return Ok(self.record_queue_eviction_attempt(
                &key,
                QueueEvictionAttempt::Skipped(QueueEvictionSkip::HasInflight),
            ));
        }

        let outcome = self
            .engine
            .unmaterialize(&key.tp, key.part.id(), key.group.as_deref())
            .await?;
        drop(eviction_guard);
        Ok(self.record_queue_eviction_attempt(&key, QueueEvictionAttempt::Storage(outcome)))
    }

    pub async fn evict_inactive_queues(
        &self,
        idle_for_ms: u64,
    ) -> Result<Vec<(Topic, Option<Group>, QueueEvictionAttempt)>, BrokerError> {
        let mut seen = HashSet::new();
        let mut keys: Vec<QueueKey> = self
            .queues
            .iter()
            .filter_map(|entry| {
                let key = entry.key().clone();
                seen.insert(key.clone()).then_some(key)
            })
            .collect();

        let storage_snapshot = self.engine.debug_snapshot().await?;
        for queue in storage_snapshot.queues {
            if !queue.materialized || queue.evicting {
                continue;
            }
            let key = QueueKey {
                tp: queue.topic,
                part: Partition::new(queue.partition),
                group: queue.group,
            };
            if seen.insert(key.clone()) {
                self.queue(&key).await.activity.mark_idle_if_no_leases();
                keys.push(key);
            }
        }

        let mut attempts = Vec::with_capacity(keys.len());
        for key in keys {
            if self.queue_already_unloaded_since_last_activity(&key) {
                continue;
            }
            let attempt = self
                .try_evict_inactive_queue(&key.tp, key.group.as_deref(), idle_for_ms)
                .await?;
            attempts.push((key.tp, key.group, attempt));
        }

        Ok(attempts)
    }

    pub async fn subscribe(
        self: &Arc<Self>,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        client_id: Uuid,
        cfg: ConsumerConfig,
    ) -> Result<ConsumerHandle, BrokerError> {
        let tp: Topic = topic.to_string();
        let part: Partition = partition;
        let group: Option<Group> = group.map(|s| s.to_string());

        self.ensure_queue_owner(&tp, part, group.as_deref())?;

        let sub_id = self.next_sub_id.fetch_add(1, Ordering::SeqCst);

        let prefetch = cfg.prefetch.max(1);

        let (msg_tx, msg_rx) = mpsc::channel::<DeliverableMessage>(prefetch * 4);
        let (settle_tx, settle_rx) = mpsc::channel::<SettleRequest>(prefetch * 8);

        let consumer = Arc::new(ConsumerState {
            sub_id,
            tx: msg_tx.clone(),
            prefetch: AtomicUsize::new(prefetch),
            inflight: AtomicUsize::new(0),
        });

        let key = QueueKey {
            tp: tp.clone(),
            part,
            group: group.clone(),
        };

        let qs = self.queue(&key).await;
        let activity_lease = {
            let eviction_guard = qs.lock_for_eviction().await;
            let lease = qs.activity.add_subscriber();
            if let Err(err) = self
                .materialize_owned_queue(&tp, part, group.as_deref())
                .await
            {
                drop(lease);
                drop(eviction_guard);
                return Err(err);
            }
            drop(eviction_guard);
            lease
        };

        qs.consumers.insert(sub_id, consumer.clone());

        // spawn settle loop for this consumer
        self.spawn_settle_loop(consumer.clone(), settle_rx);

        // Initialize the replica-durable visibility gate before any delivery so
        // a replica-durable queue never leases an uncommitted offset (it blocks
        // at 0 until followers report). No-op for local-durable queues.
        self.refresh_visibility_ceiling(&key);

        // spawn delivery loop once per queue
        if !qs.started.swap(true, Ordering::SeqCst) {
            tracing::debug!(
                "Starting delivery loop for tp={} part={} group={:?}",
                tp,
                part,
                group
            );
            self.spawn_delivery_loop(key.clone(), qs.clone());
        }

        // Repartition drain gate: a new partition (added by a grow) holds delivery
        // until its source old partition has drained. Apply here so the hold is in
        // place no matter when this partition's loop first appears.
        if self.repartition_new_partition_should_hold(&key.tp, key.part.id(), key.group.as_deref())
        {
            qs.set_delivery_held(true);
        }
        if self.repartition_shrink_survivor_should_hold(
            &key.tp,
            key.part.id(),
            key.group.as_deref(),
        ) {
            // Hold everything until a drain refresh captures the boundary.
            qs.set_hold_above_offset(Some(0));
        }

        qs.wake();

        Ok(ConsumerHandle {
            sub_id,
            client_id,
            config: cfg,
            group: group.map(|g| g.into()),
            topic: tp.into(),
            partition: part,
            messages: msg_rx,
            settler: settle_tx,
            pending_settles: self.pending_settles.clone(),
            activity_lease,
        })
    }

    /// Gate an exclusive consumer-group partition: deliver only to `assignee`
    /// (`Some`) or reopen to all competing consumers (`None`). No-op if the
    /// partition has no delivery loop yet.
    pub fn set_exclusive_assignee(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        assignee: Option<ConsumerId>,
    ) {
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        if let Some(qs) = self.queues.get(&key).map(|e| e.value().clone()) {
            qs.set_exclusive_assignee(assignee);
        }
    }

    /// Hold or release a partition's delivery for a repartition transition. A
    /// held partition delivers nothing until released. Used during a grow to keep
    /// a newly added partition silent until its source old partition has drained.
    /// No-op if the partition's delivery loop is not present on this broker.
    pub fn set_partition_delivery_held(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        held: bool,
    ) {
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        if let Some(qs) = self.queues.get(&key).map(|e| e.value().clone()) {
            qs.set_delivery_held(held);
        }
    }

    /// Hold a surviving partition's delivery at or above `boundary` for a shrink
    /// transition (`None` removes the hold). Below the boundary it keeps
    /// delivering its own pre-cutover backlog so it can drain. No-op if the
    /// partition's delivery loop is not present on this broker.
    pub fn set_partition_hold_above_offset(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        boundary: Option<Offset>,
    ) {
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        if let Some(qs) = self.queues.get(&key).map(|e| e.value().clone()) {
            qs.set_hold_above_offset(boundary);
        }
    }

    /// The lowest not-yet-settled offset for a partition: every offset below it
    /// is consumed and gone. Live repartitioning compares this to a partition's
    /// cutover boundary to tell when its pre-cutover backlog has drained (the
    /// settled offset has reached the boundary).
    pub async fn partition_lowest_unacked_offset(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Result<Offset, BrokerError> {
        Ok(self
            .engine
            .lowest_unacked_offset(topic, partition.id(), group)
            .await?)
    }

    /// The partition's next write offset (high-water). Live repartitioning
    /// snapshots this at cutover as the partition's boundary: the dividing line
    /// between pre-cutover (v_old) and post-cutover (v_new) messages.
    pub async fn partition_next_offset(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Result<Offset, BrokerError> {
        Ok(self
            .engine
            .current_next_offset(topic, partition.id(), group)
            .await?)
    }

    fn lock_repartition(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<RepartitionKey, RepartitionLocal>> {
        self.repartition_transitions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether a partition should hold delivery for an in-progress repartition:
    /// true only for a NEW partition (index >= n_old) whose source old partition
    /// (index % n_old) has not yet drained. Consulted when a delivery loop is
    /// created so a new partition starts held even if its loop appears mid-grow.
    fn repartition_new_partition_should_hold(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
    ) -> bool {
        let map = self.lock_repartition();
        match map.get(&(topic.to_string(), group.map(str::to_string))) {
            Some(local) if partition >= local.n_old => !local
                .drained_sources
                .contains(&(partition % local.n_old.max(1))),
            _ => false,
        }
    }

    /// Whether a partition is a shrink survivor whose merge sources have not all
    /// drained, so its loop should start held (at offset 0 until a refresh
    /// captures its boundary). Consulted when a delivery loop is created mid-shrink.
    fn repartition_shrink_survivor_should_hold(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
    ) -> bool {
        let map = self.lock_repartition();
        match map.get(&(topic.to_string(), group.map(str::to_string))) {
            Some(local) if local.n_new < local.n_old && partition < local.n_new => {
                !shrink_sources(partition, local.n_new.max(1), local.n_old.max(1))
                    .all(|s| local.drained_sources.contains(&s))
            }
            _ => false,
        }
    }

    /// Record an in-progress repartition and hold any already-active partition
    /// loops that need it. GROW holds each new partition (>= n_old) whole until
    /// its single source drains. SHRINK holds each surviving partition (< n_new)
    /// at offset 0 (everything) until a drain refresh captures its boundary and
    /// relaxes it, and until all its merge sources drain. Idempotent on
    /// `version`; a newer version supersedes (resets the local state).
    pub fn apply_repartition_transition(
        &self,
        topic: &str,
        group: Option<&str>,
        version: u64,
        n_old: u32,
        n_new: u32,
    ) {
        let key = (topic.to_string(), group.map(str::to_string));
        let drained_sources = {
            let mut map = self.lock_repartition();
            let local = map.entry(key).or_insert_with(|| RepartitionLocal {
                version,
                n_old,
                n_new,
                ..Default::default()
            });
            if local.version != version {
                *local = RepartitionLocal {
                    version,
                    n_old,
                    n_new,
                    ..Default::default()
                };
            }
            local.drained_sources.clone()
        };
        let n_old = n_old.max(1);
        let n_new = n_new.max(1);
        let grow = n_new > n_old;
        for entry in self.queues.iter() {
            let qk = entry.key();
            if qk.tp != topic || qk.group.as_deref() != group {
                continue;
            }
            let p = qk.part.id();
            if grow {
                if p >= n_old && !drained_sources.contains(&(p % n_old)) {
                    entry.value().set_delivery_held(true);
                }
            } else if p < n_new
                && !shrink_sources(p, n_new, n_old).all(|s| drained_sources.contains(&s))
            {
                // Survivor: hold everything until the refresh captures the boundary.
                entry.value().set_hold_above_offset(Some(0));
            }
        }
    }

    /// Snapshot each owned old partition's cutover boundary (once) and recompute
    /// which have drained their pre-cutover backlog, for this owner's heartbeat
    /// report. Call periodically while a grow runs.
    pub async fn refresh_repartition_drain(&self, topic: &str, group: Option<&str>) {
        let key = (topic.to_string(), group.map(str::to_string));
        let (n_old, n_new, known_boundaries, drained_sources) = {
            let map = self.lock_repartition();
            match map.get(&key) {
                Some(local) => (
                    local.n_old.max(1),
                    local.n_new.max(1),
                    local.boundaries.clone(),
                    local.drained_sources.clone(),
                ),
                None => return,
            }
        };
        let shrink = n_new < n_old;
        // Every partition below n_old is a drain source (both directions).
        let owned_sources: Vec<u32> = self
            .queues
            .iter()
            .filter(|e| e.key().tp == topic && e.key().group.as_deref() == group)
            .map(|e| e.key().part.id())
            .filter(|p| *p < n_old)
            .collect();

        let mut self_drained = HashSet::new();
        for r in owned_sources {
            let boundary = match known_boundaries.get(&r) {
                Some(b) => *b,
                None => {
                    // Capture the boundary once, the partition's high-water now.
                    let Ok(b) = self
                        .partition_next_offset(topic, Partition::new(r), group)
                        .await
                    else {
                        continue;
                    };
                    if let Some(local) = self.lock_repartition().get_mut(&key) {
                        local.boundaries.entry(r).or_insert(b);
                    }
                    b
                }
            };
            // A surviving partition (shrink) delivers below its boundary and holds
            // above it until all its merge sources have drained, then lifts.
            if shrink && r < n_new {
                let all_drained =
                    shrink_sources(r, n_new, n_old).all(|s| drained_sources.contains(&s));
                self.set_partition_hold_above_offset(
                    topic,
                    Partition::new(r),
                    group,
                    if all_drained { None } else { Some(boundary) },
                );
            }
            if let Ok(settled) = self
                .partition_lowest_unacked_offset(topic, Partition::new(r), group)
                .await
            {
                if settled >= boundary {
                    self_drained.insert(r);
                }
            }
        }
        if let Some(local) = self.lock_repartition().get_mut(&key) {
            local.self_drained = self_drained;
        }
    }

    /// This owner's repartition drain status across all in-progress grows, for
    /// the heartbeat label.
    pub fn repartition_drained_reports(&self) -> Vec<RepartitionDrainStatus> {
        self.lock_repartition()
            .iter()
            .map(|((topic, group), local)| {
                let mut drained: Vec<u32> = local.self_drained.iter().copied().collect();
                drained.sort_unstable();
                RepartitionDrainStatus {
                    topic: topic.clone(),
                    group: group.clone(),
                    version: local.version,
                    drained,
                }
            })
            .collect()
    }

    /// Apply the cluster-wide set of drained source partitions for a queue. On a
    /// GROW this lifts the hold on any owned new partition whose source is now
    /// drained. On a SHRINK it just records the set; the next drain refresh lifts
    /// each surviving partition once all its merge sources are in it.
    pub fn apply_repartition_drained(
        &self,
        topic: &str,
        group: Option<&str>,
        drained_sources: std::collections::HashSet<u32>,
    ) {
        let key = (topic.to_string(), group.map(str::to_string));
        let n_old = {
            let mut map = self.lock_repartition();
            let Some(local) = map.get_mut(&key) else {
                return;
            };
            local.drained_sources = drained_sources.clone();
            local.n_old.max(1)
        };
        for entry in self.queues.iter() {
            let qk = entry.key();
            if qk.tp == topic && qk.group.as_deref() == group {
                let p = qk.part.id();
                if p >= n_old && drained_sources.contains(&(p % n_old)) {
                    entry.value().set_delivery_held(false);
                }
            }
        }
    }

    /// The queues this broker is currently tracking a repartition transition for.
    /// Owners compare this to the cluster's active transitions to drop local
    /// state once a grow's marker is gone.
    pub fn active_repartition_queues(&self) -> Vec<(String, Option<String>)> {
        self.lock_repartition().keys().cloned().collect()
    }

    /// Finish a repartition: drop the local transition and release any remaining
    /// holds for the queue (all sources have drained).
    pub fn clear_repartition_transition(&self, topic: &str, group: Option<&str>) {
        let key = (topic.to_string(), group.map(str::to_string));
        let removed = self.lock_repartition().remove(&key);
        if removed.is_some() {
            // Release any remaining holds for this queue (grow new partitions and
            // shrink survivors); all sources have drained.
            for entry in self.queues.iter() {
                let qk = entry.key();
                if qk.tp == topic && qk.group.as_deref() == group {
                    entry.value().set_delivery_held(false);
                    entry.value().set_hold_above_offset(None);
                }
            }
        }
    }

    /// Fully retire a partition this node holds: drop its local delivery loop
    /// state and free its on-disk storage. Used on shrink completion to reclaim
    /// the storage of merged-away partitions, once they are deregistered and
    /// drained.
    ///
    /// The storage delete is airtight against a concurrent materialize at the
    /// engine layer (a destroying tombstone parks any reopen, the dir is renamed
    /// aside before deletion). Callers must still ensure the partition is no
    /// longer a live routing target (the still-retired fence) so a freshly
    /// recreated incarnation is never destroyed.
    pub async fn retire_partition(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<DestroyOutcome, BrokerError> {
        // Drop the local loop state so the delivery loop stops referencing it.
        self.queues.retain(|qk, _| {
            !(qk.tp == topic && qk.part.id() == part && qk.group.as_deref() == group)
        });
        Ok(self
            .engine
            .destroy_partition(topic, part, group)
            .await?)
    }

    fn lock_exclusive_groups(&self) -> std::sync::MutexGuard<'_, ExclusiveGroupRouter> {
        self.exclusive_groups
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Whether subscribing to `consumer_group` on `(topic, group)` would conflict
    /// with a different exclusive cohort already live on the same queue. Callers
    /// should reject the subscribe (a queue has a single exclusive cohort).
    pub fn exclusive_cohort_conflicts(
        &self,
        topic: &str,
        group: Option<&str>,
        consumer_group: &str,
    ) -> bool {
        let key = ConsumerGroupKey::new(topic, group, consumer_group);
        self.lock_exclusive_groups().has_conflicting_cohort(&key)
    }

    /// Register a channel that receives a member's exclusive-cohort assignment
    /// changes, to be forwarded to its connection as `AssignmentChanged` pushes.
    /// Call once per (connection, cohort) before the first `exclusive_group_join`
    /// so the member observes its initial assignment. The receiver closes when the
    /// member leaves the cohort.
    pub fn register_exclusive_member(
        &self,
        topic: &str,
        partition_group: Option<&str>,
        consumer_group: &str,
        member: impl Into<String>,
    ) -> mpsc::UnboundedReceiver<ExclusiveAssignmentUpdate> {
        let key = ConsumerGroupKey::new(topic, partition_group, consumer_group);
        self.lock_exclusive_groups()
            .register_notifier(key, member.into())
    }

    /// Register an exclusive consumer-group member's subscription to one
    /// partition and re-apply the cohort's per-partition delivery gates. `member`
    /// is the connection's stable client id; `sub_id` is the broker consumer id
    /// just created for this partition subscription; `target` is the member's
    /// optional soft per-consumer capacity. Called after `subscribe`.
    pub fn exclusive_group_join(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        consumer_group: &str,
        member: impl Into<String>,
        sub_id: ConsumerId,
        target: Option<usize>,
    ) {
        let key = ConsumerGroupKey::new(topic, group, consumer_group);
        let updates =
            self.lock_exclusive_groups()
                .join(key, partition, member.into(), sub_id, target);
        for (partition, assignee) in updates {
            self.set_exclusive_assignee(topic, partition, group, assignee);
        }
    }

    /// Remove an exclusive member (connection closed or unsubscribed) and
    /// re-apply the cohort's gates: revoked partitions move to their new assignee,
    /// or reopen to competing consumers once the cohort is empty.
    pub fn exclusive_group_leave(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        consumer_group: &str,
        member: &str,
    ) {
        let key = ConsumerGroupKey::new(topic, group, consumer_group);
        let updates = self.lock_exclusive_groups().leave(key, partition, member);
        for (partition, assignee) in updates {
            self.set_exclusive_assignee(topic, partition, group, assignee);
        }
    }

    /// This broker's local exclusive-cohort membership, for the controller to
    /// aggregate into cluster-wide membership (e.g. published on the heartbeat).
    pub fn local_cohort_membership(&self) -> Vec<LocalCohortMembership> {
        self.lock_exclusive_groups().local_membership()
    }

    /// Install the cross-broker coordinator's global assignment for a cohort: a
    /// `partition -> member id` plan covering the whole queue. This owner gates
    /// each partition it owns to the assigned member's local sub_id and, from now
    /// on, follows the plan instead of computing the assignment locally. The
    /// member ids are the cluster cohort member ids the clients carry.
    pub fn apply_exclusive_assignment(
        &self,
        topic: &str,
        group: Option<&str>,
        consumer_group: &str,
        generation: u64,
        assignment: HashMap<Partition, String>,
    ) {
        let key = ConsumerGroupKey::new(topic, group, consumer_group);
        let updates = self
            .lock_exclusive_groups()
            .set_external(key, generation, assignment);
        for (partition, assignee) in updates {
            self.set_exclusive_assignee(topic, partition, group, assignee);
        }
    }

    /// The generation of the cross-broker plan this owner has applied for a
    /// cohort, if any. Lets callers observe convergence (an owner sitting on an
    /// older generation has not yet picked up the latest plan).
    pub fn exclusive_assignment_generation(
        &self,
        topic: &str,
        group: Option<&str>,
        consumer_group: &str,
    ) -> Option<u64> {
        let key = ConsumerGroupKey::new(topic, group, consumer_group);
        self.lock_exclusive_groups().external_generation(&key)
    }

    pub async fn unsubscribe(
        &self,
        topic: &str,
        group: Option<&str>,
        partition: Partition,
        sub_id: ConsumerId,
    ) -> Result<(), BrokerError> {
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };

        if let Some(qs) = self.queues.get(&key).map(|e| e.value().clone()) {
            qs.consumers.remove(&sub_id);
            qs.wake();
        }

        let mut tagged_tags = Vec::new();
        for entry in self.records_by_tags.iter() {
            let tag = *entry.key();
            let rec = entry.value();
            if rec.consumer_id == sub_id && rec.key == key {
                tagged_tags.push((tag, rec.offset));
            }
        }

        if tagged_tags.is_empty() {
            return Ok(());
        }

        let mut offsets = Vec::with_capacity(tagged_tags.len());
        for (tag, offset) in tagged_tags {
            self.records_by_tags.remove(&tag);
            self.tags_by_key_offset.remove(&(key.clone(), offset));
            offsets.push(offset);
        }

        let count = offsets.len();
        let qs = self.queues.get(&key).map(|e| e.value().clone());
        let replication_timing = self.replication_timing.clone();
        let (done_tx, done_rx) = oneshot::channel::<bool>();
        let mut done_tx = Some(done_tx);
        let done = move |ok: bool| {
            if let Some(qs) = &qs {
                if ok {
                    replication_timing.record_replication_wake();
                    qs.wake_with_replication();
                } else {
                    qs.wake();
                }
            }
            if let Some(done_tx) = done_tx.take() {
                let _ = done_tx.send(ok);
            }
        };

        let reqs = offsets
            .into_iter()
            .map(|off| NackEventMeta {
                off,
                requeue: true,
                not_before: None,
            })
            .collect();
        let completion: Box<dyn AppendCompletion<IoError>> = Box::new(SimpleCompletion::new(done));
        self.engine
            .nack_batch(
                &key.tp,
                key.part.id(),
                key.group.as_deref(),
                reqs,
                completion,
            )
            .await?;

        match done_rx.await {
            Ok(true) => {
                tracing::debug!(
                    "Unsubscribed consumer {sub_id}, requeued {count} inflight messages"
                );
            }
            Ok(false) => {
                tracing::warn!(
                    "Unsubscribed consumer {sub_id}, but requeue completion failed for {count} messages"
                );
            }
            Err(_) => {
                tracing::warn!(
                    "Unsubscribed consumer {sub_id}, but requeue completion channel closed for {count} messages"
                );
            }
        }

        Ok(())
    }

    fn spawn_settle_loop(
        self: &Arc<Self>,
        consumer: Arc<ConsumerState>,
        mut settle_rx: mpsc::Receiver<SettleRequest>,
    ) {
        let broker = self.clone();
        self.task_group.spawn("settle_loop", async move {
            const MAX_BATCH: usize = 64;
            const COALESCE_WINDOW: Duration = Duration::from_micros(500);
            const SMALL_BATCH: usize = 8;

            loop {
                let first = tokio::select! {
                    biased;
                    _ = broker.shutdown_settle.cancelled() => break,
                    req = settle_rx.recv() => req,
                };
                let Some(first) = first else {
                    break;
                };

                let mut batch = vec![first];

                while batch.len() < MAX_BATCH {
                    match settle_rx.try_recv() {
                        Ok(req) => batch.push(req),
                        Err(_) => break,
                    }
                }

                if batch.len() < SMALL_BATCH {
                    let deadline = tokio::time::Instant::now() + COALESCE_WINDOW;
                    while batch.len() < MAX_BATCH {
                        tokio::select! {
                            biased;
                            _ = broker.shutdown_settle.cancelled() => break,
                            res = tokio::time::timeout_at(deadline, settle_rx.recv()) => {
                                match res {
                                    Ok(Some(req)) => batch.push(req),
                                    _ => break,
                                }
                            }
                        }
                    }
                }

                broker.handle_settle_batch(&consumer, batch).await;
            }

            // Drain
            settle_rx.close();
            let mut remaining = Vec::new();
            while let Some(req) = settle_rx.recv().await {
                remaining.push(req);
            }
            if !remaining.is_empty() {
                broker.handle_settle_batch(&consumer, remaining).await;
            }
        });
    }

    async fn handle_settle(&self, consumer: &Arc<ConsumerState>, req: SettleRequest) {
        let Some(tag_rec) = self
            .records_by_tags
            .remove(&req.delivery_tag)
            .map(|kv| kv.1)
        else {
            // unknown tag -> ignore (or warn)
            tracing::warn!(
                "Received settle for unknown delivery tag {:?} from consumer {}",
                req.delivery_tag,
                consumer.sub_id
            );
            return;
        };
        self.tags_by_key_offset
            .remove(&(tag_rec.key.clone(), tag_rec.offset));

        // validate consumer
        if tag_rec.consumer_id != consumer.sub_id {
            // wrong consumer: ignore (or warn)
            tracing::warn!(
                "Received settle for delivery tag {:?} from consumer {}, but tag belongs to consumer {}",
                req.delivery_tag,
                consumer.sub_id,
                tag_rec.consumer_id
            );
            return;
        }

        tracing::debug!(
            "Handling settle for consumer {}: {:?} (tag {:?}) (offset {})",
            consumer.sub_id,
            req.settle_type,
            req.delivery_tag,
            tag_rec.offset
        );

        let settle_kind = match req.settle_type {
            SettleType::Ack => SettleKind::Ack,
            SettleType::Nack {
                requeue,
                not_before,
            } => SettleKind::Nack {
                requeue: requeue.unwrap_or(true),
                not_before,
            },
            SettleType::Reject { .. } => SettleKind::Nack {
                requeue: false,
                not_before: None,
            },
        };

        // IMPORTANT:
        // Only decrement inflight when the settle append is durably accepted (completion).
        // So we keep consumer.inflight until completion callback fires.

        let engine = self.engine.clone();
        let qs = self.queues.get(&tag_rec.key).map(|e| e.value().clone());

        // Make a completion that wakes the queue loop and decrements inflight.
        let consumer2 = consumer.clone();
        let pending_settles = self.pending_settles.clone();
        let settle_drained = self.settle_drained.clone();
        let metrics = self.metrics.clone();
        let replication_timing = self.replication_timing.clone();
        let done = move |ok: bool| {
            if ok {
                consumer2.dec_inflight();
                if let SettleKind::Ack = settle_kind
                    && let Some(metrics) = metrics
                {
                    metrics.acked();
                }

                if let Some(qs) = &qs {
                    replication_timing.record_replication_wake();
                    qs.wake_with_replication();
                }
            } else {
                // If settle append failed, you may want to:
                // - reinsert tag mapping? (probably no; client should retry)
                // - or treat as "still inflight" and rely on expiry
                // For now: keep inflight as-is (conservative).
                if let Some(qs) = &qs {
                    qs.wake();
                }
            }

            let pending = pending_settles.fetch_sub(1, Ordering::AcqRel);
            if pending <= 1 {
                settle_drained.notify_waiters();
            }
            tracing::debug!(
                "Settle completed for consumer {}, pending settles now {}",
                consumer2.sub_id,
                pending - 1
            );
        };

        // You’ll implement a real completion type; here is the intent:
        let completion: Box<dyn AppendCompletion<IoError>> = Box::new(SimpleCompletion::new(done));
        let _ = engine
            .settle(
                &tag_rec.key.tp,
                tag_rec.key.part.id(),
                tag_rec.key.group.as_deref(),
                EngineSettleRequest {
                    offset: tag_rec.offset,
                    kind: settle_kind,
                },
                completion,
            )
            .await;
    }

    async fn handle_settle_batch(&self, consumer: &Arc<ConsumerState>, reqs: Vec<SettleRequest>) {
        // Group by queue. Acks all go to one bucket per queue.
        // Nacks carry per-offset settlement metadata, including optional retry deadlines.

        let mut acks_by_queue: HashMap<QueueKey, Vec<Offset>> = HashMap::new();
        let mut nacks_by_queue: HashMap<QueueKey, Vec<NackEventMeta>> = HashMap::new();

        for req in reqs {
            let Some(tag_rec) = self
                .records_by_tags
                .remove(&req.delivery_tag)
                .map(|kv| kv.1)
            else {
                tracing::warn!("Settle for unknown tag {:?}", req.delivery_tag);
                continue;
            };
            self.tags_by_key_offset
                .remove(&(tag_rec.key.clone(), tag_rec.offset));

            if tag_rec.consumer_id != consumer.sub_id {
                tracing::warn!("Settle from wrong consumer");
                continue;
            }

            match req.settle_type {
                SettleType::Ack => {
                    acks_by_queue
                        .entry(tag_rec.key)
                        .or_default()
                        .push(tag_rec.offset);
                }
                SettleType::Nack {
                    requeue,
                    not_before,
                } => {
                    let r = requeue.unwrap_or(true);
                    nacks_by_queue
                        .entry(tag_rec.key)
                        .or_default()
                        .push(NackEventMeta {
                            off: tag_rec.offset,
                            requeue: r,
                            not_before,
                        });
                }
                SettleType::Reject { .. } => {
                    // Treat as nack with requeue=false (until you remove Reject)
                    nacks_by_queue
                        .entry(tag_rec.key)
                        .or_default()
                        .push(NackEventMeta {
                            off: tag_rec.offset,
                            requeue: false,
                            not_before: None,
                        });
                }
            }
        }

        // Issue one engine call per (queue, kind) group
        for (key, offsets) in acks_by_queue {
            let count = offsets.len();
            let consumer_clone = consumer.clone();
            let qs = self.queues.get(&key).map(|e| e.value().clone());
            let pending_settles = self.pending_settles.clone();
            let settle_drained = self.settle_drained.clone();
            let metrics = self.metrics.clone();
            let replication_timing = self.replication_timing.clone();

            let done = move |ok: bool| {
                if ok {
                    for _ in 0..count {
                        consumer_clone.dec_inflight();
                    }
                    if let Some(m) = metrics {
                        m.acked_many(count as u64);
                    }
                    if let Some(qs) = &qs {
                        replication_timing.record_replication_wake();
                        qs.wake_with_replication();
                    }
                } else if let Some(qs) = &qs {
                    qs.wake();
                }

                let pending = pending_settles.fetch_sub(count, Ordering::AcqRel);
                if pending <= count {
                    settle_drained.notify_waiters();
                }
            };

            let completion: Box<dyn AppendCompletion<IoError>> =
                Box::new(SimpleCompletion::new(done));
            let reqs = offsets
                .into_iter()
                .map(|off| AckEventMeta { off })
                .collect();
            let _ = self
                .engine
                .ack_batch(
                    &key.tp,
                    key.part.id(),
                    key.group.as_deref(),
                    reqs,
                    completion,
                )
                .await;
        }

        for (key, items) in nacks_by_queue {
            let count = items.len();
            let consumer_clone = consumer.clone();
            let qs = self.queues.get(&key).map(|e| e.value().clone());
            let pending_settles = self.pending_settles.clone();
            let settle_drained = self.settle_drained.clone();
            let replication_timing = self.replication_timing.clone();

            let done = move |ok: bool| {
                if ok {
                    for _ in 0..count {
                        consumer_clone.dec_inflight();
                    }
                    if let Some(qs) = &qs {
                        replication_timing.record_replication_wake();
                        qs.wake_with_replication();
                    }
                } else if let Some(qs) = &qs {
                    qs.wake();
                }

                let pending = pending_settles.fetch_sub(count, Ordering::AcqRel);
                if pending <= count {
                    settle_drained.notify_waiters();
                }
            };

            let completion: Box<dyn AppendCompletion<IoError>> =
                Box::new(SimpleCompletion::new(done));
            let _ = self
                .engine
                .nack_batch(
                    &key.tp,
                    key.part.id(),
                    key.group.as_deref(),
                    items,
                    completion,
                )
                .await;
        }
    }

    fn spawn_delivery_loop(self: &Arc<Self>, key: QueueKey, qs: Arc<QueueLoopState>) {
        let broker = self.clone();
        let metrics = self.metrics.clone();
        let owner_runtime_shutdown = qs.owner_runtime_shutdown.clone();
        // TODO: do not keep handle(memory leak effective) if relevant connection dies
        self.task_group.spawn("delivery_loop", async move {
            let mut last_epoch_seen = qs.current_epoch();

            loop {
                let cfg = broker.config_snapshot();
                let poll = Duration::from_millis(cfg.delivery_poll_max_ms.max(1));
                let reason = tokio::select! {
                    biased;

                    _ = broker.shutdown_consumers.cancelled() => break,
                    _ = owner_runtime_shutdown.cancelled() => break,
                    _ = qs.notify.notified() => WakeReason::Notify,
                    _ = broker.settings_changed.notified() => WakeReason::SettingsChanged,
                    _ = tokio::time::sleep(poll) => WakeReason::Timer,
                };

                // if let WakeReason::Notify = reason {
                //     // Stagger to pick up more potential wakes
                //     tokio::time::sleep(Duration::from_millis(10)).await;
                // }

                let epoch_now = qs.current_epoch();
                let epoch_advanced = epoch_now != last_epoch_seen;
                last_epoch_seen = epoch_now;

                tracing::debug!("Delivery loop woke up for tp={} part={} group={:?} due to {:?}, epoch advanced: {}, current epoch: {}", key.tp, key.part, key.group, reason, epoch_advanced, epoch_now);

                let mut progressed = false;

                // try deliver until we stall
                loop {
                    if broker.shutdown_consumers.is_cancelled()
                        || owner_runtime_shutdown.is_cancelled()
                    {
                        break;
                    }

                    // Repartition drain gate: a newly added partition holds all
                    // delivery until its source old partition drains (per-key
                    // order across the remap). Skip delivery while held.
                    if qs.is_delivery_held() {
                        tracing::debug!("Delivery held for repartition on tp={} part={} group={:?}", key.tp, key.part, key.group);
                        break;
                    }

                    // Repartition SHRINK gate: a surviving partition delivers only
                    // below the boundary until its merge sources drain. When we can
                    // confirm nothing is deliverable below it, hold WITHOUT polling
                    // (no over-lease). `next_deliverable` returns the `upper`
                    // sentinel when the range is empty, so a result at or above the
                    // boundary means "nothing below". Anything else proceeds and the
                    // per-deliverable filter below enforces the boundary.
                    let hold_above = qs.hold_above_offset();
                    if let Some(boundary) = hold_above {
                        if let Ok(Some(next)) = broker
                            .engine
                            .next_deliverable(
                                &key.tp,
                                key.part.id(),
                                key.group.as_deref(),
                                0,
                                boundary,
                            )
                            .await
                        {
                            if next >= boundary {
                                break;
                            }
                        }
                    }

                    let mut consumers: Vec<Arc<ConsumerState>> =
                        qs.consumers.iter().map(|e| e.value().clone()).collect();
                    // Exclusive consumer-group gate: when an assignee is set,
                    // deliver only to it (others stay subscribed as standbys).
                    if let Some(assignee) = qs.exclusive_assignee() {
                        consumers.retain(|c| c.sub_id == assignee);
                    }
                    if consumers.is_empty() {
                        tracing::debug!("No consumers for tp={} part={} group={:?}, skipping delivery", key.tp, key.part, key.group);
                        break;
                    }

                    let total_cap: usize = consumers
                        .iter()
                        .map(|c| {
                            let p = c.prefetch.load(Ordering::Acquire);
                            let i = c.inflight.load(Ordering::Acquire);
                            p.saturating_sub(i)
                        })
                        .sum();

                    tracing::debug!("Total delivery capacity for tp={} part={} group={:?}: {}, Total consumers: {}", key.tp, key.part, key.group, total_cap, consumers.len());

                    if total_cap == 0 {
                        break;
                    }

                    let cfg = broker.config_snapshot();
                    let lease_deadline = unix_millis() + cfg.inflight_ttl_ms;

                    // Replica-durable visibility gate: never lease an offset that
                    // is not yet committed on enough replicas (sentinel u64::MAX =
                    // no gate). Combine with the shrink hold boundary; both are
                    // exclusive deliverable ceilings, so the lower one wins. When
                    // nothing is committed yet poll returns empty and the loop
                    // parks until set_visibility_ceiling wakes it.
                    let poll_upper = qs
                        .visibility_ceiling()
                        .min(hold_above.unwrap_or(u64::MAX));

                    // TODO: Also limit each poll batch based on size of aggreated messages
                    let deliverables = match broker
                        .engine
                        .poll_ready(
                            &key.tp,
                            key.part.id(),
                            key.group.as_deref(),
                            total_cap,
                            lease_deadline,
                            poll_upper,
                        )
                        .await
                    {
                        Ok(v) if !v.is_empty() => v,
                        _ => break,
                    };

                    tracing::debug!("Polled {} deliverables for tp={} part={} group={:?}", deliverables.len(), key.tp, key.part, key.group);
                    // `poll_ready` records the in-flight lease before returning,
                    // so followers need a prompt read even if local delivery
                    // later races with subscriber capacity.
                    broker.replication_timing.record_replication_wake();
                    qs.wake_replication_followers();

                    let mut delivered = 0;
                    let mut rr = qs.rr.fetch_add(1, Ordering::Relaxed) as usize;
                    let mut redelivered = 0;
                    for d in deliverables {
                        // Shrink hold: never deliver at or above the boundary.
                        // Deliverables are earliest-first, so the rest are too.
                        if let Some(boundary) = hold_above {
                            if d.offset >= boundary {
                                break;
                            }
                        }
                        let mut picked = None;
                        for _ in 0..consumers.len() {
                            let c = &consumers[rr % consumers.len()];
                            rr += 1;
                            if c.can_accept() {
                                picked = Some(c.clone());
                                break;
                            }
                        }
                        let Some(c) = picked else {
                            qs.wake();
                            break;
                        };

                        let epoch = broker.next_tag_epoch.fetch_add(1, Ordering::SeqCst);
                        let tag = DeliveryTag { epoch };

                        broker.records_by_tags.insert(
                            tag,
                            TagRecord {
                                key: key.clone(),
                                offset: d.offset,
                                consumer_id: c.sub_id,
                            },
                        );
                        broker
                            .tags_by_key_offset
                            .insert((key.clone(), d.offset), tag);

                        c.inc_inflight();

                        let msg = DeliverableMessage {
                            message: StoredMessage {
                                topic: key.tp.clone(),
                                group: key.group.clone(),
                                partition: key.part,
                                offset: d.offset,
                                published: d.published,
                                publish_received: d.publish_received,
                                retried: d.retries,
                                content_type: d.content_type,
                                headers: d.extra_headers,
                                payload: d.payload,
                            },
                            delivery_tag: tag,
                            group: key.group.clone(),
                        };

                        if d.retries > 0 {
                            redelivered += 1;
                        }

                        if c.tx.send(msg).await.is_err() {
                            // TODO: Currently handled by expiry (since we keep inflight until completion),
                            //          but you could also immediately decrement inflight and requeue here.
                            qs.consumers.remove(&c.sub_id);
                        } else {
                            if let Some(metrics) = &broker.metrics {
                                metrics.delivered();
                            }
                            qs.activity.touch();
                            progressed = true;
                        }
                        qs.wake();

                        delivered += 1;

                        // TODO: eval
                        if delivered % 1024 == 0 {
                            // tokio::task::yield_now().await;
                        }
                    }

                    if redelivered > 0
                        && let Some(metrics) = &metrics {
                            metrics.redelivered_many(redelivered);
                        }
                }

                if progressed && matches!(reason, WakeReason::Timer) && epoch_advanced {
                    tracing::warn!(
                        "Delivery progressed only after timer wakeup (possible missed notify) tp={} part={} group={:?}",
                        key.tp,
                        key.part,
                        key.group
                    );
                }
            }
        });
    }

    fn spawn_queue_eviction_worker(broker: Arc<Self>) {
        let broker_clone = broker.clone();
        broker_clone
            .task_group
            .spawn("queue_eviction_worker", async move {
                let mut settings_epoch = broker.settings_epoch();
                loop {
                    let current_epoch = broker.settings_epoch();
                    if current_epoch != settings_epoch {
                        settings_epoch = current_epoch;
                    }
                    let cfg = broker.config_snapshot();
                    let Some(_) = cfg.queue_idle_evict_after_ms else {
                        let settings_changed = broker.settings_changed.notified();
                        tokio::pin!(settings_changed);
                        let current_epoch = broker.settings_epoch();
                        if current_epoch != settings_epoch {
                            settings_epoch = current_epoch;
                            continue;
                        }
                        tokio::select! {
                            biased;
                            _ = broker.shutdown_queue_eviction.cancelled() => break,
                            _ = &mut settings_changed => {
                                settings_epoch = broker.settings_epoch();
                                continue;
                            }
                        }
                    };
                    let interval_ms = cfg.queue_idle_sweep_interval_ms.max(1);

                    let settings_changed = broker.settings_changed.notified();
                    tokio::pin!(settings_changed);
                    tokio::select! {
                        biased;
                        _ = broker.shutdown_queue_eviction.cancelled() => break,
                        _ = &mut settings_changed => {
                            settings_epoch = broker.settings_epoch();
                            continue;
                        }
                        _ = tokio::time::sleep(Duration::from_millis(interval_ms)) => {}
                    }

                    let Some(idle_for_ms) = broker.config_snapshot().queue_idle_evict_after_ms
                    else {
                        continue;
                    };
                    match broker.evict_inactive_queues(idle_for_ms).await {
                        Ok(attempts) => {
                            let evicted = attempts
                                .iter()
                                .filter(|(_, _, attempt)| {
                                    matches!(
                                        attempt,
                                        QueueEvictionAttempt::Storage(EvictOutcome::Evicted)
                                    )
                                })
                                .count();
                            if evicted > 0 {
                                tracing::debug!(
                                    "Queue eviction worker unmaterialized {evicted} idle queues"
                                );
                            }
                        }
                        Err(err) => {
                            tracing::error!("Queue eviction worker error: {err}");
                        }
                    }
                }
            });
    }

    fn spawn_expiry_worker(broker: Arc<Self>) {
        let broker_clone = broker.clone();
        let deadline_awaker = broker.engine.deadline_awaker();
        broker_clone.task_group.spawn("expiry_worker", async move {
            let mut expiry_hint = Some(0);
            loop {
                let deadline_awaker = deadline_awaker.notified();
                tokio::pin!(deadline_awaker);
                tokio::select! {
                    biased;

                    _ = broker.shutdown_expiry.cancelled() => break,
                    _ = broker.settings_changed.notified() => {
                        expiry_hint = broker.engine.next_expiry_hint().await.unwrap_or(None);
                        continue;
                    }

                    // TODO: Add branch to be notified when to recheck for hint(retry or enqueue with delay?)
                    _ = &mut deadline_awaker => {
                        // Wait for potential burst to settle
                        // TODO: Config as broker.cfg.expiry_wake_debounce_ms ?
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        // Drain any accumulated permit so we don't immediately re-fire next iteration
                        let _ = broker.engine.deadline_awaker().notified().now_or_never();
                        // earlier deadline arrived, recompute and loop
                        expiry_hint = broker.engine.next_expiry_hint().await.unwrap_or(None);
                        continue;
                    }

                    _ = async {
                        match expiry_hint {
                            Some(ts) => {
                                let now = unix_millis();
                                if ts > now {
                                    tracing::info!("Expiry worker sleeping for {} ms..", ts - now);
                                    fibril_util::sleep_until(ts).await;
                                }
                            }
                            None => {
                                // TODO: move to timer?
                                let cfg = broker.config_snapshot();
                                tracing::info!("Expiry worker sleeping for {} ms..", cfg.expiry_poll_min_ms);
                                tokio::time::sleep(Duration::from_millis(
                                    cfg.expiry_poll_min_ms
                                )).await;
                            }
                        }
                    } => {}
                }

                let ran_for_deadline = expiry_hint.is_some();
                expiry_hint = broker.engine.next_expiry_hint().await.unwrap_or(None);
                tracing::info!("Expiry worker running..");

                // Requeue expired inside Stroma (durable)
                let expired = match broker
                    .engine
                    .requeue_expired(unix_millis(), broker.config_snapshot().expiry_batch_max)
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        // TODO: log? handle?
                        tracing::error!("Expiry worker error: {err}");
                        continue;
                    }
                };

                tracing::info!(
                    "Expiry worker woke up, requeued {} expired messages",
                    expired.len()
                );

                if ran_for_deadline {
                    for qs in broker.queues.iter().map(|entry| entry.value().clone()) {
                        qs.wake();
                    }
                }

                if expired.is_empty() {
                    continue;
                }

                if let Some(metrics) = &broker.metrics {
                    metrics.expired_many(expired.len() as u64);
                }

                // TODO: windowed iterator and spawn more at a time? perhaps parallelism / 2
                for (tp, part, group, offset) in expired.iter().cloned() {
                    let key = QueueKey { tp, part: Partition::new(part), group };

                    // find the tag for this offset
                    let tag = broker
                        .tags_by_key_offset
                        .remove(&(key.clone(), offset))
                        .map(|kv| kv.1);

                    if let Some(tag) = tag
                        && let Some((_, rec)) = broker.records_by_tags.remove(&tag)
                        && let Some(qs) = broker.queues.get(&rec.key)
                    {
                        if let Some(consumer) = qs.consumers.get(&rec.consumer_id) {
                            consumer.dec_inflight();
                        }
                        qs.wake();
                    }
                }

                // Wake each affected queue exactly once
                let mut touched: HashSet<QueueKey> = HashSet::new();
                for (tp, part, group, _off) in expired {
                    touched.insert(QueueKey { tp, part: Partition::new(part), group });
                }

                for key in touched {
                    if let Some(qs) = broker.queues.get(&key).map(|e| e.value().clone()) {
                        broker.replication_timing.record_replication_wake();
                        qs.wake_with_replication();
                    }
                }

                tracing::info!(
                    "Expiry worker iteration finished"
                );
            }
        });
    }
}

impl Broker<StromaEngine> {
    pub fn spawn_assignment_watcher(self: &Arc<Self>, coordination: Arc<dyn Coordination>) {
        let broker = self.clone();
        self.task_group.spawn("assignment_watcher", async move {
            let node_id = coordination.node_id().to_string();
            let mut previous = CoordinationSnapshot::default();
            let mut watch = coordination.watch();

            let initial = coordination.snapshot();
            broker
                .apply_assignment_snapshot_transitions(&node_id, &previous, &initial)
                .await;
            previous = initial;

            loop {
                if watch.changed().await.is_err() {
                    tracing::debug!("assignment watcher exiting because coordination watch closed");
                    break;
                }

                let next = watch.borrow().clone();
                if next == previous {
                    continue;
                }

                broker
                    .apply_assignment_snapshot_transitions(&node_id, &previous, &next)
                    .await;
                previous = next;
            }
        });
    }

    pub fn spawn_assignment_watcher_with_follower_replication(
        self: &Arc<Self>,
        coordination: Arc<dyn Coordination>,
        resolver: Arc<dyn BrokerOwnerReplicationPeerResolver>,
        cfg: FollowerReplicationWorkerConfig,
    ) {
        let broker = self.clone();
        self.task_group.spawn("assignment_watcher", async move {
            let node_id = coordination.node_id().to_string();
            let mut previous = CoordinationSnapshot::default();
            let mut watch = coordination.watch();

            let initial = coordination.snapshot();
            broker
                .apply_assignment_snapshot_transitions_with_follower_replication(
                    &node_id,
                    &previous,
                    &initial,
                    resolver.clone(),
                    cfg,
                )
                .await;
            previous = initial;

            loop {
                if watch.changed().await.is_err() {
                    tracing::debug!("assignment watcher exiting because coordination watch closed");
                    break;
                }

                let next = watch.borrow().clone();
                if next == previous {
                    continue;
                }

                broker
                    .apply_assignment_snapshot_transitions_with_follower_replication(
                        &node_id,
                        &previous,
                        &next,
                        resolver.clone(),
                        cfg,
                    )
                    .await;
                previous = next;
            }
        });
    }

    pub async fn apply_assignment_snapshot_transitions(
        &self,
        node_id: &str,
        previous: &CoordinationSnapshot,
        next: &CoordinationSnapshot,
    ) -> Vec<Result<BrokerAssignmentTransitionApply, BrokerError>> {
        let previous = self.snapshot_with_cached_local_assignments(node_id, previous);
        let transitions = plan_local_assignment_transitions(node_id, &previous, next);
        let mut outcomes = Vec::with_capacity(transitions.len());
        for transition in transitions {
            let result = self.apply_assignment_transition(&transition).await;
            if let Err(err) = &result {
                tracing::error!(
                    topic = %transition.queue.topic,
                    partition = transition.queue.partition.id(),
                    group = ?transition.queue.group,
                    intent = ?transition.intent,
                    "failed to apply assignment transition: {err:?}"
                );
            }
            outcomes.push(result);
        }
        outcomes
    }

    async fn apply_assignment_snapshot_transitions_with_follower_replication(
        self: &Arc<Self>,
        node_id: &str,
        previous: &CoordinationSnapshot,
        next: &CoordinationSnapshot,
        resolver: Arc<dyn BrokerOwnerReplicationPeerResolver>,
        cfg: FollowerReplicationWorkerConfig,
    ) -> Vec<Result<BrokerAssignmentTransitionApply, BrokerError>> {
        let previous = self.snapshot_with_cached_local_assignments(node_id, previous);
        let transitions = plan_local_assignment_transitions(node_id, &previous, next);
        let mut outcomes = Vec::with_capacity(transitions.len());
        for transition in transitions {
            let result = self.apply_assignment_transition(&transition).await;
            match &result {
                Ok(BrokerAssignmentTransitionApply::Applied(
                    LocalAssignmentIntent::BecomeFollower
                    | LocalAssignmentIntent::DemoteOwnerToFollower,
                )) => {
                    if let Some(assignment) = transition.next.clone() {
                        if let Err(err) = self.spawn_follower_replication_worker_loop(
                            assignment,
                            resolver.clone(),
                            cfg,
                        ) {
                            tracing::error!(
                                topic = %transition.queue.topic,
                                partition = transition.queue.partition.id(),
                                group = ?transition.queue.group,
                                intent = ?transition.intent,
                                "failed to start follower replication worker: {err:?}"
                            );
                        }
                    }
                }
                Err(err) => {
                    tracing::error!(
                        topic = %transition.queue.topic,
                        partition = transition.queue.partition.id(),
                        group = ?transition.queue.group,
                        intent = ?transition.intent,
                        "failed to apply assignment transition: {err:?}"
                    );
                }
                _ => {}
            }
            outcomes.push(result);
        }
        outcomes
    }

    fn snapshot_with_cached_local_assignments(
        &self,
        node_id: &str,
        previous: &CoordinationSnapshot,
    ) -> CoordinationSnapshot {
        let mut previous = previous.clone();
        for entry in self.assignment_cache.iter() {
            let assignment = entry.value();
            if !assignment.is_owned_by(node_id) && !assignment.is_followed_by(node_id) {
                continue;
            }
            let key = entry.key();
            previous.assignments.insert(
                QueueIdentity::new(key.tp.clone(), key.part, key.group.as_deref()),
                assignment.clone(),
            );
        }
        previous
    }

    pub async fn apply_assignment_transition(
        &self,
        transition: &LocalAssignmentTransition,
    ) -> Result<BrokerAssignmentTransitionApply, BrokerError> {
        let topic = transition.queue.topic.to_string();
        let group = transition.queue.group.as_deref();
        // Confirm policies read the assignment governing this queue.
        match &transition.next {
            Some(next) => self.cache_queue_assignment(next),
            None => {
                self.assignment_cache.remove(&QueueKey {
                    tp: topic.clone(),
                    part: transition.queue.partition,
                    group: transition.queue.group.clone(),
                });
            }
        }
        // Fencing epoch from the assignment driving this transition. Role
        // changes persist it into the queue logs BEFORE role-specific work
        // (epoch-before-use), so stale-epoch replication is rejected at the
        // storage layer from that point on.
        let assignment_epoch = transition
            .next
            .as_ref()
            .map(|assignment| assignment.epoch)
            .unwrap_or(0);
        match transition.intent {
            LocalAssignmentIntent::Noop => Ok(BrokerAssignmentTransitionApply::Noop(
                LocalAssignmentIntent::Noop,
            )),
            LocalAssignmentIntent::RefreshOwner | LocalAssignmentIntent::RefreshFollower => {
                Ok(BrokerAssignmentTransitionApply::Noop(transition.intent))
            }
            LocalAssignmentIntent::BecomeOwner => {
                if self
                    .engine
                    .is_materialized(&topic, transition.queue.partition.id(), group)
                {
                    self.engine
                        .become_queue_owner_with_epoch(
                            &topic,
                            transition.queue.partition.id(),
                            group,
                            assignment_epoch,
                        )
                        .await?;
                    Ok(BrokerAssignmentTransitionApply::Applied(transition.intent))
                } else {
                    Ok(BrokerAssignmentTransitionApply::Noop(transition.intent))
                }
            }
            LocalAssignmentIntent::BecomeFollower => {
                self.become_replication_follower_with_epoch(
                    &topic,
                    transition.queue.partition,
                    group,
                    assignment_epoch,
                )
                .await?;
                self.ensure_follower_replication_worker(&transition.queue);
                Ok(BrokerAssignmentTransitionApply::Applied(transition.intent))
            }
            LocalAssignmentIntent::DemoteOwnerToFollower => {
                let key = QueueKey {
                    tp: topic.clone(),
                    part: transition.queue.partition,
                    group: transition.queue.group.clone(),
                };
                let broker_deliveries = self.stop_owner_queue_runtime(&key).unwrap_or_default();
                self.release_offsets_for_role_transition(&key, broker_deliveries)
                    .await?;
                self.engine
                    .demote_queue_owner_to_follower(&topic, transition.queue.partition.id(), group)
                    .await?;
                // The demoted queue follows under the NEW assignment's epoch:
                // stale-epoch traffic (its own leftovers or a stale peer) is
                // fenced at the log layer from here on.
                self.engine
                    .advance_queue_epoch(
                        &topic,
                        transition.queue.partition.id(),
                        group,
                        assignment_epoch,
                    )
                    .await?;
                self.ensure_follower_replication_worker(&transition.queue);
                Ok(BrokerAssignmentTransitionApply::Applied(transition.intent))
            }
            LocalAssignmentIntent::FreezeOwner => {
                let key = QueueKey {
                    tp: topic.clone(),
                    part: transition.queue.partition,
                    group: transition.queue.group.clone(),
                };
                let broker_deliveries = self.stop_owner_queue_runtime(&key).unwrap_or_default();
                self.release_offsets_for_role_transition(&key, broker_deliveries)
                    .await?;
                self.engine
                    .freeze_queue_for_transition(&topic, transition.queue.partition.id(), group)
                    .await?;
                Ok(BrokerAssignmentTransitionApply::Applied(transition.intent))
            }
            LocalAssignmentIntent::PromoteFollowerToOwner => {
                // Never-materialized queues have no local follower state to
                // promote: stay cold, become owner lazily on first traffic
                // (same rule as BecomeOwner).
                if !self
                    .engine
                    .is_materialized(&topic, transition.queue.partition.id(), group)
                {
                    return Ok(BrokerAssignmentTransitionApply::Noop(transition.intent));
                }
                // Failover promotion (promote-to-local-tail under the epoch
                // fence): drain the follower worker first so promotion never
                // races a mid-batch replicated ingest, then promote at this
                // follower's own tails. The dead owner cannot supply expected
                // tails; the bumped assignment epoch fences its unreplicated
                // suffix. Refusals (unapplied local events) leave the queue a
                // follower — explicit refusal over optimistic serving.
                let stopped_worker = self
                    .stop_follower_replication_worker(&transition.queue)
                    .await;
                match self
                    .engine
                    .promote_queue_follower_to_local_tail(
                        &topic,
                        transition.queue.partition.id(),
                        group,
                        assignment_epoch,
                    )
                    .await?
                {
                    QueuePromotionOutcome::Promoted {
                        message_next_offset,
                        event_next_offset,
                        ..
                    } => {
                        tracing::info!(
                            topic,
                            partition = transition.queue.partition.id(),
                            group = ?group,
                            stopped_worker,
                            message_next_offset,
                            event_next_offset,
                            "promoted follower to owner at local tails"
                        );
                        Ok(BrokerAssignmentTransitionApply::Applied(transition.intent))
                    }
                    refused => {
                        tracing::warn!(
                            topic,
                            partition = transition.queue.partition.id(),
                            group = ?group,
                            ?refused,
                            "follower promotion refused; queue stays follower"
                        );
                        Ok(BrokerAssignmentTransitionApply::Deferred {
                            intent: transition.intent,
                            reason: "local promotion checks refused; queue remains follower",
                        })
                    }
                }
            }
            LocalAssignmentIntent::StopFollower => {
                let stopped_worker = self
                    .stop_follower_replication_worker(&transition.queue)
                    .await;
                self.engine
                    .stop_queue_follower_for_transition(
                        &topic,
                        transition.queue.partition.id(),
                        group,
                    )
                    .await?;
                tracing::debug!(
                    topic,
                    partition = transition.queue.partition.id(),
                    group = ?group,
                    stopped_worker,
                    "stopped local follower assignment"
                );
                Ok(BrokerAssignmentTransitionApply::Applied(transition.intent))
            }
        }
    }

}


// ---------------- Completion helper (sketch) ----------------
// TODO:
// We already have completion abstractions, work on replacing this with more *complete* impls.
// This already shows the intent: call closure on complete.

struct SimpleCompletion {
    f: Option<Box<dyn FnOnce(bool) + Send>>,
}

impl SimpleCompletion {
    fn new<F: FnOnce(bool) + Send + 'static>(f: F) -> Self {
        Self {
            f: Some(Box::new(f)),
        }
    }
}

impl AppendCompletion<IoError> for SimpleCompletion {
    fn complete(mut self: Box<Self>, res: Result<AppendResult, IoError>) {
        let ok = res.is_ok();
        if let Some(f) = self.f.take() {
            f(ok);
        }
    }
}

#[cfg(test)]
mod exclusive_router_tests {
    use super::*;
    use crate::coordination::ConsumerGroupKey;

    fn cohort() -> ConsumerGroupKey {
        ConsumerGroupKey::new("jobs", None, "default")
    }

    fn gate_map(updates: Vec<GateUpdate>) -> HashMap<Partition, Option<ConsumerId>> {
        updates.into_iter().collect()
    }

    #[test]
    fn external_assignment_overrides_local_and_resolves_member_sub_ids() {
        let mut router = ExclusiveGroupRouter::new(None);
        let key = cohort();
        // Two members each subscribed (fan-in) to both partitions, with sub_ids.
        router.join(key.clone(), Partition::new(0), "m1".into(), 100, None);
        router.join(key.clone(), Partition::new(1), "m1".into(), 101, None);
        router.join(key.clone(), Partition::new(0), "m2".into(), 200, None);
        router.join(key.clone(), Partition::new(1), "m2".into(), 201, None);

        // The coordinator decides p0->m2, p1->m1; gates resolve to local sub_ids.
        let plan = HashMap::from([
            (Partition::new(0), "m2".to_string()),
            (Partition::new(1), "m1".to_string()),
        ]);
        let gates = gate_map(router.set_external(key, 0, plan));
        assert_eq!(gates[&Partition::new(0)], Some(200));
        assert_eq!(gates[&Partition::new(1)], Some(101));
    }

    #[test]
    fn external_assignment_leaves_unsubscribed_assignee_untouched() {
        let mut router = ExclusiveGroupRouter::new(None);
        let key = cohort();
        router.join(key.clone(), Partition::new(0), "m1".into(), 100, None);
        // Plan assigns p0 to m2, who is not subscribed on this broker -> no gate
        // update (the partition is owned/served elsewhere or m2 hasn't arrived).
        let plan = HashMap::from([(Partition::new(0), "m2".to_string())]);
        assert!(router.set_external(key, 0, plan).is_empty());
    }

    #[test]
    fn local_membership_dedups_members_and_carries_targets() {
        let mut router = ExclusiveGroupRouter::new(None);
        let key = cohort();
        // m1 subscribed both partitions (one member, not two); m2 one, with target.
        router.join(key.clone(), Partition::new(0), "m1".into(), 100, None);
        router.join(key.clone(), Partition::new(1), "m1".into(), 101, None);
        router.join(key.clone(), Partition::new(0), "m2".into(), 200, Some(3));

        let snapshot = router.local_membership();
        assert_eq!(snapshot.len(), 1);
        let cohort = &snapshot[0];
        assert_eq!(cohort.topic, "jobs");
        assert_eq!(cohort.consumer_group, "default");
        assert_eq!(
            cohort.members,
            vec![
                CohortMemberInfo {
                    member: "m1".into(),
                    target: None,
                },
                CohortMemberInfo {
                    member: "m2".into(),
                    target: Some(3),
                },
            ]
        );
    }

    #[test]
    fn external_mode_join_follows_plan_not_local_compute() {
        let mut router = ExclusiveGroupRouter::new(None);
        let key = cohort();
        // Coordinator plan installed first: p0 -> m2.
        let plan = HashMap::from([(Partition::new(0), "m2".to_string())]);
        router.set_external(key.clone(), 0, plan);

        // m1 subscribes p0 first. Local computation would gate m1, but the plan
        // says m2 (not here yet) -> no gate to m1.
        assert!(
            router
                .join(key.clone(), Partition::new(0), "m1".into(), 100, None)
                .is_empty()
        );
        // m2 subscribes p0 -> the gate now resolves to m2 per the plan.
        let gates = gate_map(router.join(key, Partition::new(0), "m2".into(), 200, None));
        assert_eq!(gates[&Partition::new(0)], Some(200));
    }

    #[test]
    fn external_assignment_fences_stale_generation() {
        let mut router = ExclusiveGroupRouter::new(None);
        let key = cohort();
        router.join(key.clone(), Partition::new(0), "m1".into(), 100, None);
        router.join(key.clone(), Partition::new(0), "m2".into(), 200, None);

        // Generation 2 assigns p0 -> m2.
        let plan_v2 = HashMap::from([(Partition::new(0), "m2".to_string())]);
        let gates = gate_map(router.set_external(key.clone(), 2, plan_v2));
        assert_eq!(gates[&Partition::new(0)], Some(200));
        assert_eq!(router.external_generation(&key), Some(2));

        // A late generation-1 slice (p0 -> m1) is fenced: no gate changes, the
        // held generation stays at 2.
        let plan_v1 = HashMap::from([(Partition::new(0), "m1".to_string())]);
        assert!(router.set_external(key.clone(), 1, plan_v1).is_empty());
        assert_eq!(router.external_generation(&key), Some(2));

        // A newer generation-3 slice (p0 -> m1) is accepted.
        let plan_v3 = HashMap::from([(Partition::new(0), "m1".to_string())]);
        let gates = gate_map(router.set_external(key.clone(), 3, plan_v3));
        assert_eq!(gates[&Partition::new(0)], Some(100));
        assert_eq!(router.external_generation(&key), Some(3));
    }
}
