use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use fibril_metrics::{BrokerStats, QueuesStateSnapshot};
use futures::{FutureExt, future::BoxFuture};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard as AsyncMutexGuard, Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use fibril_storage::{
    DeliverableMessage, DeliveryTag, Group, LogId, Offset, StorageError, StoredMessage, Topic,
};
use fibril_util::unix_millis;
use uuid::Uuid;

use crate::coordination::{
    Coordination, CoordinationSnapshot, LocalAssignmentIntent, LocalAssignmentTransition,
    PartitionAssignment, StaticCoordination, plan_local_assignment_transitions,
};
use crate::queue_engine::{
    EvictOutcome, FollowerStateCheckpointInstall, FollowerStateCheckpointInstallOutcome,
    KDurability, OwnerStateCheckpoint, QueueEngine, QueuePromotionOutcome, ReplicatedEventBatch,
    ReplicatedMessageBatch, ReplicatedQueueApplyOutcome, SettleKind,
    SettleRequest as EngineSettleRequest, StromaEngine,
};
use stroma_core::{
    AckEventMeta, AppendCompletion, AppendResult, CompletionPair, IoError, KeratinAppendCompletion,
    Message, MessageContentType, MessageHeaders, NackEventMeta, OwnerReplicationRead,
    StromaDebugSnapshot, StromaError, StromaEvent, StromaMetrics, TaskGroup, UnixMillis,
};

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
        partition: LogId,
        group: Option<Group>,
    },

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error(
        "not enough in-sync replicas for {topic}/{partition}: {in_sync} in sync, {required} required"
    )]
    NotEnoughInSyncReplicas {
        topic: String,
        partition: LogId,
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
    pub partition: LogId,
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
    /// Minimum in-sync replicas (owner + healthy followers) required to accept
    /// a replica-durable publish. 1 (default) disables the floor.
    pub replication_min_in_sync_replicas: usize,
    /// How recently a follower must have reported progress to count as in-sync.
    pub replication_isr_timeout_ms: u64,
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
            replication_min_in_sync_replicas: 1,
            replication_isr_timeout_ms: 10_000,
        }
    }
}

pub trait QueueOwnership: std::fmt::Debug + Send + Sync {
    fn owns_queue(&self, topic: &str, partition: LogId, group: Option<&str>) -> bool;
}

#[derive(Debug, Clone, Default)]
pub struct OwnAllQueues;

impl QueueOwnership for OwnAllQueues {
    fn owns_queue(&self, _topic: &str, _partition: LogId, _group: Option<&str>) -> bool {
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
    fn owns_queue(&self, topic: &str, partition: LogId, group: Option<&str>) -> bool {
        self.owned
            .contains(&OwnedQueue::new(topic, partition, group))
    }
}

impl QueueOwnership for StaticCoordination {
    fn owns_queue(&self, topic: &str, partition: LogId, group: Option<&str>) -> bool {
        Coordination::owns_queue(self, topic, partition, group)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnedQueue {
    pub topic: Topic,
    pub partition: LogId,
    pub group: Option<Group>,
}

impl OwnedQueue {
    pub fn new(topic: impl Into<Topic>, partition: LogId, group: Option<&str>) -> Self {
        Self {
            topic: topic.into(),
            partition,
            group: group.map(str::to_string),
        }
    }
}

#[derive(Debug)]
pub struct BrokerOwnerReplicationRecords {
    pub messages: OwnerReplicationRead<Message>,
    pub events: OwnerReplicationRead<StromaEvent>,
}

pub trait BrokerOwnerReplicationPeer: Send + Sync {
    fn read_owner_replication_records<'a>(
        &'a self,
        topic: &'a str,
        partition: LogId,
        group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
    ) -> BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>>;

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: LogId,
        group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>>;
}

pub trait BrokerOwnerReplicationPeerResolver: Send + Sync {
    fn resolve_owner_peer<'a>(
        &'a self,
        assignment: &'a PartitionAssignment,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn BrokerOwnerReplicationPeer>>, BrokerError>>;
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BrokerReplicationCheckpointRequired {
    pub epoch: u64,
    pub requested_offset: Offset,
    pub head_offset: Offset,
    pub next_offset: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerFollowerReplicationApply {
    Applied(ReplicatedQueueApplyOutcome),
    CheckpointRequired {
        messages: Option<BrokerReplicationCheckpointRequired>,
        events: Option<BrokerReplicationCheckpointRequired>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerReplicationCatchUpOptions {
    pub message_from: Offset,
    pub event_from: Offset,
    pub max_messages_per_read: usize,
    pub max_events_per_read: usize,
    pub max_iterations: usize,
}

impl Default for BrokerReplicationCatchUpOptions {
    fn default() -> Self {
        Self {
            message_from: 0,
            event_from: 0,
            max_messages_per_read: 256,
            max_events_per_read: 256,
            max_iterations: 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct BrokerReplicationCatchUpProgress {
    pub iterations: usize,
    pub applied_message_records: usize,
    pub applied_event_records: usize,
    pub message_next_offset: Offset,
    pub event_next_offset: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerReplicationCatchUp {
    CaughtUp(BrokerReplicationCatchUpProgress),
    CheckpointRequired {
        progress: BrokerReplicationCatchUpProgress,
        messages: Option<BrokerReplicationCheckpointRequired>,
        events: Option<BrokerReplicationCheckpointRequired>,
    },
    IterationLimit {
        progress: BrokerReplicationCatchUpProgress,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowerReplicationWorkerLoopExit {
    Cancelled { ticks: usize },
    WorkerStopped { ticks: usize },
    OwnerChanged { ticks: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FollowerReplicationWorkerConfig {
    pub max_messages_per_read: usize,
    pub max_events_per_read: usize,
    pub max_iterations_per_tick: usize,
    pub allow_checkpoint_install: bool,
    pub caught_up_poll_ms: u64,
    pub retry_poll_ms: u64,
    pub checkpoint_retry_poll_ms: u64,
    /// When true the worker re-reads its poll intervals from the broker's
    /// runtime settings every tick (the production watcher path). Explicit
    /// poll values above are then only the pre-first-snapshot fallback.
    pub follow_runtime_settings: bool,
}

impl Default for FollowerReplicationWorkerConfig {
    fn default() -> Self {
        Self {
            max_messages_per_read: 256,
            max_events_per_read: 256,
            max_iterations_per_tick: 8,
            allow_checkpoint_install: false,
            caught_up_poll_ms: 1000,
            retry_poll_ms: 100,
            checkpoint_retry_poll_ms: 5000,
            follow_runtime_settings: false,
        }
    }
}

impl FollowerReplicationWorkerConfig {
    pub fn catch_up_options(
        self,
        message_from: Offset,
        event_from: Offset,
    ) -> BrokerReplicationCatchUpOptions {
        BrokerReplicationCatchUpOptions {
            message_from,
            event_from,
            max_messages_per_read: self.max_messages_per_read,
            max_events_per_read: self.max_events_per_read,
            max_iterations: self.max_iterations_per_tick,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum FollowerReplicationWorkerStatus {
    Idle,
    CaughtUp,
    PendingRetry,
    CheckpointRequired {
        messages: Option<BrokerReplicationCheckpointRequired>,
        events: Option<BrokerReplicationCheckpointRequired>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FollowerReplicationWorkerState {
    pub message_next_offset: Offset,
    pub event_next_offset: Offset,
    pub status: FollowerReplicationWorkerStatus,
    pub last_progress: Option<BrokerReplicationCatchUpProgress>,
    pub next_delay_ms: u64,
}

impl FollowerReplicationWorkerState {
    pub fn new(message_next_offset: Offset, event_next_offset: Offset) -> Self {
        Self {
            message_next_offset,
            event_next_offset,
            status: FollowerReplicationWorkerStatus::Idle,
            last_progress: None,
            next_delay_ms: 0,
        }
    }

    pub fn catch_up_options(
        &self,
        cfg: FollowerReplicationWorkerConfig,
    ) -> BrokerReplicationCatchUpOptions {
        cfg.catch_up_options(self.message_next_offset, self.event_next_offset)
    }

    pub fn should_install_checkpoint(&self, cfg: FollowerReplicationWorkerConfig) -> bool {
        cfg.allow_checkpoint_install
            && matches!(
                self.status,
                FollowerReplicationWorkerStatus::CheckpointRequired { .. }
            )
    }

    pub fn record_catch_up(
        &mut self,
        cfg: FollowerReplicationWorkerConfig,
        outcome: &BrokerReplicationCatchUp,
    ) {
        match outcome {
            BrokerReplicationCatchUp::CaughtUp(progress) => {
                self.apply_progress(progress);
                self.status = FollowerReplicationWorkerStatus::CaughtUp;
                self.next_delay_ms = cfg.caught_up_poll_ms;
            }
            BrokerReplicationCatchUp::IterationLimit { progress } => {
                self.apply_progress(progress);
                self.status = FollowerReplicationWorkerStatus::PendingRetry;
                self.next_delay_ms = cfg.retry_poll_ms;
            }
            BrokerReplicationCatchUp::CheckpointRequired {
                progress,
                messages,
                events,
            } => {
                self.apply_progress(progress);
                self.status = FollowerReplicationWorkerStatus::CheckpointRequired {
                    messages: messages.clone(),
                    events: events.clone(),
                };
                self.next_delay_ms = cfg.checkpoint_retry_poll_ms;
            }
        }
    }

    fn apply_progress(&mut self, progress: &BrokerReplicationCatchUpProgress) {
        self.message_next_offset = progress.message_next_offset;
        self.event_next_offset = progress.event_next_offset;
        self.last_progress = Some(*progress);
    }
}

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
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FollowerReplicationWorkerObservability {
    pub topic: String,
    pub partition: LogId,
    pub group: Option<String>,
    pub state: Option<FollowerReplicationWorkerState>,
    pub busy: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FollowerReplicationWorkerSummary {
    pub follower_worker_count: usize,
    pub caught_up_count: usize,
    pub pending_retry_count: usize,
    pub checkpoint_required_count: usize,
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
struct QueueKey {
    tp: Topic,
    part: LogId,
    group: Option<Group>,
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
struct QueueLoopState {
    started: AtomicBool,
    rr: AtomicU64,
    consumers: DashMap<ConsumerId, Arc<ConsumerState>>,
    activity: Arc<QueueActivity>,
    eviction_lock: AsyncMutex<()>,
    owner_runtime_shutdown: CancellationToken,
    // used to wake the delivery loop
    notify: tokio::sync::Notify,
    epoch: AtomicU64,
}

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
            epoch: AtomicU64::new(1),
        }
    }
    fn wake(&self) {
        self.epoch.fetch_add(1, Ordering::Release);
        self.notify.notify_one();
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

#[derive(Debug)]
struct FollowerReplicationWorkerRuntime {
    state: Arc<AsyncMutex<FollowerReplicationWorkerState>>,
    shutdown: CancellationToken,
    started: AtomicBool,
    stopping: AtomicBool,
    active_ticks: AtomicUsize,
    idle: Notify,
}

impl FollowerReplicationWorkerRuntime {
    fn new(message_next_offset: Offset, event_next_offset: Offset) -> Self {
        Self {
            state: Arc::new(AsyncMutex::new(FollowerReplicationWorkerState::new(
                message_next_offset,
                event_next_offset,
            ))),
            shutdown: CancellationToken::new(),
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            active_ticks: AtomicUsize::new(0),
            idle: Notify::new(),
        }
    }

    fn begin_tick(self: &Arc<Self>) -> Option<FollowerReplicationTickGuard> {
        if self.stopping.load(Ordering::Acquire) {
            return None;
        }

        self.active_ticks.fetch_add(1, Ordering::AcqRel);
        if self.stopping.load(Ordering::Acquire) {
            self.finish_tick();
            return None;
        }

        Some(FollowerReplicationTickGuard {
            runtime: self.clone(),
        })
    }

    fn finish_tick(&self) {
        if self.active_ticks.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.idle.notify_waiters();
        }
    }

    async fn stop_and_wait(&self) {
        self.stopping.store(true, Ordering::Release);
        self.shutdown.cancel();
        while self.active_ticks.load(Ordering::Acquire) != 0 {
            self.idle.notified().await;
        }
    }
}

#[derive(Debug)]
struct FollowerReplicationTickGuard {
    runtime: Arc<FollowerReplicationWorkerRuntime>,
}

impl Drop for FollowerReplicationTickGuard {
    fn drop(&mut self) {
        self.runtime.finish_tick();
    }
}

// ---------------- Broker ----------------

// TODO cleanup old?
pub struct Broker<E: QueueEngine + std::fmt::Debug + Send + Sync + 'static> {
    cfg: Arc<ArcSwap<BrokerConfig>>,
    engine: E,
    shutdown_publishers: CancellationToken,
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
    follower_replication_workers:
        DashMap<crate::coordination::QueueIdentity, Arc<FollowerReplicationWorkerRuntime>>,

    pending_settles: Arc<AtomicUsize>,
    settle_drained: Arc<Notify>,
    settings_changed: Arc<Notify>,
    settings_epoch: AtomicU64,

    task_group: Arc<TaskGroup>,

    metrics: Option<Arc<BrokerStats>>,
    ownership: Arc<dyn QueueOwnership>,

    /// Per-queue durable replication progress reported by followers (from
    /// stamped replication reads). Drives publish-confirm durability policies.
    replication_progress: Arc<DashMap<QueueKey, Arc<ReplicationProgressCell>>>,
    /// Latest assignment applied for each local queue (durability policy +
    /// replica set source for confirms). Maintained by the assignment watcher;
    /// empty for standalone brokers (local-durable confirms only).
    assignment_cache: Arc<DashMap<QueueKey, PartitionAssignment>>,
}

/// Cheap shared handle for publish-confirm replication waits inside
/// per-queue confirm loops.
#[derive(Clone)]
pub struct ReplicationConfirmGate {
    progress: Arc<DashMap<QueueKey, Arc<ReplicationProgressCell>>>,
    assignments: Arc<DashMap<QueueKey, PartitionAssignment>>,
    cfg: Arc<ArcSwap<BrokerConfig>>,
}

/// One follower's last-reported durable progress, with the report time used to
/// decide in-sync membership (a follower that stopped reporting falls out).
#[derive(Debug, Clone, Copy)]
struct FollowerProgress {
    message_next: Offset,
    event_next: Offset,
    last_report: std::time::Instant,
}

/// Follower durable progress for one queue, plus a waiter wake-up.
#[derive(Debug, Default)]
pub struct ReplicationProgressCell {
    /// follower node id -> last-reported durable progress
    followers: std::sync::Mutex<std::collections::HashMap<String, FollowerProgress>>,
    changed: Notify,
}

impl ReplicationProgressCell {
    /// Lock the progress map, recovering the guard if a previous holder panicked
    /// (a poisoned lock here just means slightly stale progress, never unsafe).
    fn lock_followers(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, FollowerProgress>> {
        self.followers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
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
            assignment_cache: Arc::new(DashMap::new()),
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

    /// Record a follower's durable replication progress (from a stamped
    /// replication read: followers apply durably, so their pull offsets are
    /// durable watermarks). Wakes any publish confirms waiting on policy.
    pub fn record_follower_replication_progress(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        follower_node_id: &str,
        durable_message_next: Offset,
        durable_event_next: Offset,
    ) {
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        let cell = self
            .replication_progress
            .entry(key)
            .or_insert_with(|| Arc::new(ReplicationProgressCell::default()))
            .clone();
        {
            let mut followers = cell.lock_followers();
            let entry = followers
                .entry(follower_node_id.to_string())
                .or_insert(FollowerProgress {
                    message_next: 0,
                    event_next: 0,
                    last_report: std::time::Instant::now(),
                });
            // Monotonic: late/reordered reports never regress progress.
            entry.message_next = entry.message_next.max(durable_message_next);
            entry.event_next = entry.event_next.max(durable_event_next);
            entry.last_report = std::time::Instant::now();
        }
        cell.changed.notify_waiters();
    }

    /// Reported follower durable progress for one queue (observability/tests).
    pub fn follower_replication_progress(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
    ) -> Vec<(String, (Offset, Offset))> {
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        self.replication_progress
            .get(&key)
            .map(|cell| {
                let followers = cell.lock_followers();
                followers
                    .iter()
                    .map(|(node, progress)| {
                        (node.clone(), (progress.message_next, progress.event_next))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Cache the assignment governing a local queue (watcher-maintained).
    pub fn cache_queue_assignment(&self, assignment: &PartitionAssignment) {
        self.assignment_cache.insert(
            QueueKey {
                tp: assignment.queue.topic.to_string(),
                part: assignment.queue.partition,
                group: assignment.queue.group.clone(),
            },
            assignment.clone(),
        );
    }

    /// Shareable confirm gate for spawned per-queue confirm loops.
    pub fn replication_confirm_gate(&self) -> ReplicationConfirmGate {
        ReplicationConfirmGate {
            progress: self.replication_progress.clone(),
            assignments: self.assignment_cache.clone(),
            cfg: self.cfg.clone(),
        }
    }

    /// Wait until the queue's assignment durability policy is satisfied for a
    /// message at `offset` (the owner's own durable write already counts).
    /// No cached assignment (standalone) or `local_durable` returns
    /// immediately. Fails with a descriptive error on timeout.
    pub async fn await_replication_confirm(
        &self,
        key: &QueueKey,
        offset: Offset,
    ) -> Result<(), BrokerError> {
        self.replication_confirm_gate()
            .await_confirm(key, offset)
            .await
    }
}

impl ReplicationConfirmGate {
    /// See `Broker::await_replication_confirm`.
    pub async fn await_confirm(&self, key: &QueueKey, offset: Offset) -> Result<(), BrokerError> {
        let Some(assignment) = self.assignments.get(key).map(|a| a.clone()) else {
            return Ok(());
        };
        let requirement = assignment.durability_requirement().map_err(|error| {
            BrokerError::InvalidArgument(format!(
                "replication durability unsatisfiable for {}/{}: {error:?}",
                key.tp, key.part
            ))
        })?;
        // The owner's local durable write is one of the required nodes.
        let required_followers = requirement.nodes.saturating_sub(1);
        if required_followers == 0 {
            return Ok(());
        }

        let cfg = self.cfg.load();
        let timeout_ms = cfg.replication_confirm_timeout_ms;
        let min_in_sync = cfg.replication_min_in_sync_replicas;
        let isr_timeout = std::time::Duration::from_millis(cfg.replication_isr_timeout_ms);

        let cell = self
            .progress
            .entry(key.clone())
            .or_insert_with(|| Arc::new(ReplicationProgressCell::default()))
            .clone();

        // In-sync replica floor (Kafka min.insync.replicas). A precondition on
        // current cluster health, NOT something to wait on: if too few replicas
        // are healthy we refuse fast so a degraded cluster errors immediately
        // instead of hanging every publish until the confirm timeout. The
        // briefly-unhealthy cold-start window (a follower that has not pulled
        // yet) is self-healing — followers report continuously, so a client
        // retry rides through it. in-sync = owner + followers that reported
        // within the freshness window. A floor of 1 (the default) is satisfied
        // by the owner alone, so skip the work entirely.
        if min_in_sync > 1 {
            let in_sync = {
                let followers = cell.lock_followers();
                let now = std::time::Instant::now();
                let fresh = assignment
                    .followers
                    .iter()
                    .filter(|follower| {
                        followers.get(*follower).is_some_and(|progress| {
                            now.duration_since(progress.last_report) <= isr_timeout
                        })
                    })
                    .count();
                1 + fresh
            };
            if in_sync < min_in_sync {
                return Err(BrokerError::NotEnoughInSyncReplicas {
                    topic: key.tp.clone(),
                    partition: key.part,
                    in_sync,
                    required: min_in_sync,
                });
            }
        }

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);

        // Wait for the durability ack count: enough followers durable past this
        // offset. Unlike the ISR floor, this IS a wait — the acks are in flight
        // and just need replication to catch up to the offset.
        loop {
            let satisfied = {
                let followers = cell.lock_followers();
                assignment
                    .followers
                    .iter()
                    .filter(|follower| {
                        followers
                            .get(*follower)
                            .is_some_and(|progress| progress.message_next > offset)
                    })
                    .count()
                    >= required_followers
            };
            if satisfied {
                return Ok(());
            }
            let notified = cell.changed.notified();
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(BrokerError::Unknown(format!(
                    "publish confirm timed out after {timeout_ms}ms: {:?} requires {} follower acknowledgement(s) past offset {offset} on {}/{}",
                    assignment.durability, required_followers, key.tp, key.part
                )));
            }
        }
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

    fn ensure_queue_owner(
        &self,
        topic: &str,
        partition: LogId,
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

    pub async fn shutdown(&self) {
        self.shutdown_publishers.cancel();
        self.shutdown_consumers.cancel();
        self.shutdown_settle.cancel();
        self.shutdown_expiry.cancel();
        self.shutdown_queue_eviction.cancel();
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
        group: &Option<Group>,
    ) -> Result<(PublisherHandle, ConfirmStream), BrokerError> {
        // TODO: make configurable?
        let (tx, mut rx) = mpsc::channel::<PublishRequest>(16384);
        let (confirm_tx, confirm_rx) = mpsc::channel::<Offset>(16384);

        let engine = self.engine.clone();
        let shutdown = self.shutdown_publishers.clone();
        let tp: Topic = topic.to_string();
        let part: LogId = 0;
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
            if let Err(err) = engine.materialize(&tp, part, group.as_deref()).await {
                drop(lease);
                drop(eviction_guard);
                return Err(BrokerError::Engine(err));
            }
            drop(eviction_guard);
            lease
        };
        let qs_publish = qs.clone();
        let qs_clone = qs.clone();
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
                    .publish_batch(&tp, part, group.as_deref(), items)
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
                        let res = Err(BrokerError::Engine(StromaError::Io(e.to_string())));
                        if let Err(e) = reply.send(res) {
                            tracing::error!("Failed to send publish response: {e:?}");
                        }
                    }
                    Err(e) => {
                        tracing::error!("Failed to receive append completion: {e:?}");
                        let res = Err(BrokerError::Engine(StromaError::Io(
                            "append completion channel closed".to_string(),
                        )));
                        if let Err(e) = reply.send(res) {
                            tracing::error!("Failed to send publish response: {e:?}");
                        }
                    }
                }

                qs_clone.wake();
            }
        });

        Ok((
            PublisherHandle { publisher: tx },
            ConfirmStream { rx: confirm_rx },
        ))
    }

    async fn queue(&self, key: &QueueKey) -> Arc<QueueLoopState> {
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
            .release_inflight_batch(&key.tp, key.part, key.group.as_deref(), reqs, completion)
            .await?;

        match done_rx.await {
            Ok(true) => {
                tracing::debug!(
                    topic = %key.tp,
                    partition = key.part,
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
            part: 0,
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

    pub fn has_follower_replication_worker(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
    ) -> bool {
        self.follower_replication_workers
            .contains_key(&crate::coordination::QueueIdentity::new(
                topic, partition, group,
            ))
    }

    pub async fn follower_replication_worker_snapshot(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
    ) -> Option<FollowerReplicationWorkerState> {
        let worker =
            self.follower_replication_workers
                .get(&crate::coordination::QueueIdentity::new(
                    topic, partition, group,
                ))?;
        Some(worker.value().state.lock().await.clone())
    }

    fn ensure_follower_replication_worker(
        &self,
        queue: &crate::coordination::QueueIdentity,
    ) -> Arc<FollowerReplicationWorkerRuntime> {
        self.follower_replication_workers
            .entry(queue.clone())
            .or_insert_with(|| Arc::new(FollowerReplicationWorkerRuntime::new(0, 0)))
            .value()
            .clone()
    }

    async fn stop_follower_replication_worker(
        &self,
        queue: &crate::coordination::QueueIdentity,
    ) -> bool {
        let Some((_, worker)) = self.follower_replication_workers.remove(queue) else {
            return false;
        };
        worker.stop_and_wait().await;
        true
    }

    fn follower_replication_worker_runtime(
        &self,
        queue: &crate::coordination::QueueIdentity,
    ) -> Result<Arc<FollowerReplicationWorkerRuntime>, BrokerError> {
        self.follower_replication_workers
            .get(queue)
            .map(|entry| entry.value().clone())
            .ok_or_else(|| {
                BrokerError::InvalidArgument(format!(
                    "no follower replication worker registered for {}/{}/{}",
                    queue.topic,
                    queue.partition,
                    queue.group.as_deref().unwrap_or("<default>")
                ))
            })
    }

    fn follower_replication_worker(
        &self,
        queue: &crate::coordination::QueueIdentity,
    ) -> Result<Arc<AsyncMutex<FollowerReplicationWorkerState>>, BrokerError> {
        Ok(self
            .follower_replication_worker_runtime(queue)?
            .state
            .clone())
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
            .is_materialized(&key.tp, key.part, key.group.as_deref())
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
        SparseQueueObservabilitySnapshot {
            queues,
            summary,
            replication_followers,
            replication_summary,
        }
    }

    pub fn sparse_queue_observability_snapshot(&self) -> Vec<SparseQueueObservability> {
        self.sparse_queue_observability_report().queues
    }

    fn follower_replication_observability_report(
        &self,
    ) -> (
        Vec<FollowerReplicationWorkerObservability>,
        FollowerReplicationWorkerSummary,
    ) {
        let mut caught_up_count = 0;
        let mut pending_retry_count = 0;
        let mut checkpoint_required_count = 0;
        let mut workers: Vec<_> = self
            .follower_replication_workers
            .iter()
            .map(|entry| {
                let queue = entry.key();
                let state = entry
                    .value()
                    .state
                    .try_lock()
                    .ok()
                    .map(|state| state.clone());
                if let Some(state) = &state {
                    match state.status {
                        FollowerReplicationWorkerStatus::CaughtUp => caught_up_count += 1,
                        FollowerReplicationWorkerStatus::PendingRetry => pending_retry_count += 1,
                        FollowerReplicationWorkerStatus::CheckpointRequired { .. } => {
                            checkpoint_required_count += 1;
                        }
                        FollowerReplicationWorkerStatus::Idle => {}
                    }
                }
                let busy = state.is_none();
                FollowerReplicationWorkerObservability {
                    topic: queue.topic.to_string(),
                    partition: queue.partition,
                    group: queue.group.as_ref().map(ToString::to_string),
                    state,
                    busy,
                }
            })
            .collect();
        workers.sort_by(|a, b| {
            a.group
                .cmp(&b.group)
                .then_with(|| a.topic.cmp(&b.topic))
                .then_with(|| a.partition.cmp(&b.partition))
        });
        let summary = FollowerReplicationWorkerSummary {
            follower_worker_count: workers.len(),
            caught_up_count,
            pending_retry_count,
            checkpoint_required_count,
        };
        (workers, summary)
    }

    pub async fn try_evict_inactive_queue(
        &self,
        topic: &str,
        group: Option<&str>,
        idle_for_ms: u64,
    ) -> Result<QueueEvictionAttempt, BrokerError> {
        let key = QueueKey {
            tp: topic.to_string(),
            part: 0,
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
            .has_inflight(&key.tp, key.part, key.group.as_deref())
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
            .unmaterialize(&key.tp, key.part, key.group.as_deref())
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
                part: queue.partition,
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
        group: Option<&str>,
        client_id: Uuid,
        cfg: ConsumerConfig,
    ) -> Result<ConsumerHandle, BrokerError> {
        let tp: Topic = topic.to_string();
        let part: LogId = 0;
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
            if let Err(err) = self.engine.materialize(&tp, part, group.as_deref()).await {
                drop(lease);
                drop(eviction_guard);
                return Err(BrokerError::Engine(err));
            }
            drop(eviction_guard);
            lease
        };

        qs.consumers.insert(sub_id, consumer.clone());

        // spawn settle loop for this consumer
        self.spawn_settle_loop(consumer.clone(), settle_rx);

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

    pub async fn unsubscribe(
        &self,
        topic: &str,
        group: Option<&str>,
        partition: LogId,
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
        let (done_tx, done_rx) = oneshot::channel::<bool>();
        let mut done_tx = Some(done_tx);
        let done = move |ok: bool| {
            if let Some(qs) = &qs {
                qs.wake();
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
            .nack_batch(&key.tp, key.part, key.group.as_deref(), reqs, completion)
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
        let done = move |ok: bool| {
            if ok {
                consumer2.dec_inflight();
                if let SettleKind::Ack = settle_kind
                    && let Some(metrics) = metrics
                {
                    metrics.acked();
                }

                if let Some(qs) = &qs {
                    qs.wake();
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
                tag_rec.key.part,
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

            let done = move |ok: bool| {
                if ok {
                    for _ in 0..count {
                        consumer_clone.dec_inflight();
                    }
                    if let Some(m) = metrics {
                        m.acked_many(count as u64);
                    }
                    if let Some(qs) = &qs {
                        qs.wake();
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
                .ack_batch(&key.tp, key.part, key.group.as_deref(), reqs, completion)
                .await;
        }

        for (key, items) in nacks_by_queue {
            let count = items.len();
            let consumer_clone = consumer.clone();
            let qs = self.queues.get(&key).map(|e| e.value().clone());
            let pending_settles = self.pending_settles.clone();
            let settle_drained = self.settle_drained.clone();

            let done = move |ok: bool| {
                if ok {
                    for _ in 0..count {
                        consumer_clone.dec_inflight();
                    }
                    if let Some(qs) = &qs {
                        qs.wake();
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
                .nack_batch(&key.tp, key.part, key.group.as_deref(), items, completion)
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

                    let consumers: Vec<Arc<ConsumerState>> =
                        qs.consumers.iter().map(|e| e.value().clone()).collect();
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

                    // TODO: Also limit each poll batch based on size of aggreated messages
                    let deliverables = match broker
                        .engine
                        .poll_ready(
                            &key.tp,
                            key.part,
                            key.group.as_deref(),
                            total_cap,
                            lease_deadline,
                        )
                        .await
                    {
                        Ok(v) if !v.is_empty() => v,
                        _ => break,
                    };

                    tracing::debug!("Polled {} deliverables for tp={} part={} group={:?}", deliverables.len(), key.tp, key.part, key.group);

                    let mut delivered = 0;
                    let mut rr = qs.rr.fetch_add(1, Ordering::Relaxed) as usize;
                    let mut redelivered = 0;
                    for d in deliverables {
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
                    let key = QueueKey { tp, part, group };

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
                    touched.insert(QueueKey { tp, part, group });
                }

                for key in touched {
                    if let Some(qs) = broker.queues.get(&key).map(|e| e.value().clone()) {
                        qs.wake();
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
        let transitions = plan_local_assignment_transitions(node_id, previous, next);
        let mut outcomes = Vec::with_capacity(transitions.len());
        for transition in transitions {
            let result = self.apply_assignment_transition(&transition).await;
            if let Err(err) = &result {
                tracing::error!(
                    topic = %transition.queue.topic,
                    partition = transition.queue.partition,
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
        let transitions = plan_local_assignment_transitions(node_id, previous, next);
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
                                partition = transition.queue.partition,
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
                        partition = transition.queue.partition,
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
                    .is_materialized(&topic, transition.queue.partition, group)
                {
                    self.engine
                        .become_queue_owner_with_epoch(
                            &topic,
                            transition.queue.partition,
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
                    .demote_queue_owner_to_follower(&topic, transition.queue.partition, group)
                    .await?;
                // The demoted queue follows under the NEW assignment's epoch:
                // stale-epoch traffic (its own leftovers or a stale peer) is
                // fenced at the log layer from here on.
                self.engine
                    .advance_queue_epoch(
                        &topic,
                        transition.queue.partition,
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
                    .freeze_queue_for_transition(&topic, transition.queue.partition, group)
                    .await?;
                Ok(BrokerAssignmentTransitionApply::Applied(transition.intent))
            }
            LocalAssignmentIntent::PromoteFollowerToOwner => {
                // Never-materialized queues have no local follower state to
                // promote: stay cold, become owner lazily on first traffic
                // (same rule as BecomeOwner).
                if !self
                    .engine
                    .is_materialized(&topic, transition.queue.partition, group)
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
                        transition.queue.partition,
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
                            partition = transition.queue.partition,
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
                            partition = transition.queue.partition,
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
                    .stop_queue_follower_for_transition(&topic, transition.queue.partition, group)
                    .await?;
                tracing::debug!(
                    topic,
                    partition = transition.queue.partition,
                    group = ?group,
                    stopped_worker,
                    "stopped local follower assignment"
                );
                Ok(BrokerAssignmentTransitionApply::Applied(transition.intent))
            }
        }
    }

    pub async fn read_owner_replication_records(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
    ) -> Result<BrokerOwnerReplicationRecords, BrokerError> {
        self.ensure_queue_owner(topic, partition, group)?;

        let messages = self
            .engine
            .read_owner_message_records(topic, partition, group, message_from, max_messages)
            .await?;
        let events = self
            .engine
            .read_owner_event_records(topic, partition, group, event_from, max_events)
            .await?;

        Ok(BrokerOwnerReplicationRecords { messages, events })
    }

    pub async fn become_replication_follower(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
    ) -> Result<(), BrokerError> {
        self.engine
            .become_queue_follower(topic, partition, group)
            .await?;
        Ok(())
    }

    /// Fence both queue logs at `epoch` (persisted, monotonic). Also the
    /// substrate for future manual-fence operator tooling.
    pub async fn advance_replication_epoch(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<u64, BrokerError> {
        self.engine
            .advance_queue_epoch(topic, partition, group, epoch)
            .await
            .map_err(BrokerError::from)
    }

    /// `become_replication_follower` fenced at the assignment epoch.
    pub async fn become_replication_follower_with_epoch(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<(), BrokerError> {
        self.engine
            .become_queue_follower_with_epoch(topic, partition, group, epoch)
            .await?;
        Ok(())
    }

    pub async fn export_owner_state_checkpoint(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
    ) -> Result<OwnerStateCheckpoint, BrokerError> {
        self.ensure_queue_owner(topic, partition, group)?;
        Ok(self
            .engine
            .export_owner_state_checkpoint(topic, partition, group)
            .await?)
    }

    pub async fn install_follower_state_checkpoint(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        install: FollowerStateCheckpointInstall,
    ) -> Result<FollowerStateCheckpointInstallOutcome, BrokerError> {
        Ok(self
            .engine
            .install_follower_state_checkpoint(topic, partition, group, install)
            .await?)
    }

    pub async fn apply_follower_replication_records(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        records: BrokerOwnerReplicationRecords,
    ) -> Result<BrokerFollowerReplicationApply, BrokerError> {
        let messages = match records.messages {
            OwnerReplicationRead::Batch(batch) if !batch.records.is_empty() => {
                Some(ReplicatedMessageBatch {
                    epoch: batch.epoch,
                    first_offset: batch.records[0].0,
                    records: batch
                        .records
                        .into_iter()
                        .map(|(_, message)| message)
                        .collect(),
                    // Followers apply durably so their reported pull offsets
                    // are honest DURABLE progress for owner confirm policies
                    // (replica_durable/majority_durable).
                    durability: Some(KDurability::AfterFsync),
                })
            }
            OwnerReplicationRead::Batch(_) => None,
            OwnerReplicationRead::CheckpointRequired {
                epoch,
                requested_offset,
                head_offset,
                next_offset,
            } => {
                tracing::debug!(
                    topic,
                    partition,
                    group,
                    requested_offset,
                    head_offset,
                    next_offset,
                    "replication message read requires checkpoint before follower apply"
                );
                return Ok(BrokerFollowerReplicationApply::CheckpointRequired {
                    messages: Some(BrokerReplicationCheckpointRequired {
                        epoch,
                        requested_offset,
                        head_offset,
                        next_offset,
                    }),
                    events: checkpoint_required(&records.events),
                });
            }
        };
        let events = match records.events {
            OwnerReplicationRead::Batch(batch) if !batch.records.is_empty() => {
                Some(ReplicatedEventBatch {
                    epoch: batch.epoch,
                    first_offset: batch.records[0].0,
                    events: batch.records.into_iter().map(|(_, event)| event).collect(),
                    // Followers apply durably so their reported pull offsets
                    // are honest DURABLE progress for owner confirm policies
                    // (replica_durable/majority_durable).
                    durability: Some(KDurability::AfterFsync),
                })
            }
            OwnerReplicationRead::Batch(_) => None,
            OwnerReplicationRead::CheckpointRequired {
                epoch,
                requested_offset,
                head_offset,
                next_offset,
            } => {
                tracing::debug!(
                    topic,
                    partition,
                    group,
                    requested_offset,
                    head_offset,
                    next_offset,
                    "replication event read requires checkpoint before follower apply"
                );
                return Ok(BrokerFollowerReplicationApply::CheckpointRequired {
                    messages: None,
                    events: Some(BrokerReplicationCheckpointRequired {
                        epoch,
                        requested_offset,
                        head_offset,
                        next_offset,
                    }),
                });
            }
        };

        let outcome = self
            .engine
            .apply_replicated_queue_batch(topic, partition, group, messages, events)
            .await?;
        Ok(BrokerFollowerReplicationApply::Applied(outcome))
    }

    pub async fn promote_replication_follower_if_caught_up(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        expected_message_next_offset: Offset,
        expected_event_next_offset: Offset,
    ) -> Result<QueuePromotionOutcome, BrokerError> {
        self.engine
            .promote_queue_follower_if_caught_up(
                topic,
                partition,
                group,
                expected_message_next_offset,
                expected_event_next_offset,
            )
            .await
            .map_err(BrokerError::from)
    }

    /// Failover promotion at the follower's own tails (see the
    /// `PromoteFollowerToOwner` transition arm for the safety argument).
    pub async fn promote_replication_follower_to_local_tail(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<QueuePromotionOutcome, BrokerError> {
        self.engine
            .promote_queue_follower_to_local_tail(topic, partition, group, epoch)
            .await
            .map_err(BrokerError::from)
    }

    pub async fn catch_up_replication_follower_from_owner(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        options: BrokerReplicationCatchUpOptions,
    ) -> Result<BrokerReplicationCatchUp, BrokerError> {
        if options.max_messages_per_read == 0
            || options.max_events_per_read == 0
            || options.max_iterations == 0
        {
            return Err(BrokerError::InvalidArgument(
                "replication catch-up limits must be greater than zero".into(),
            ));
        }

        let mut progress = BrokerReplicationCatchUpProgress {
            message_next_offset: options.message_from,
            event_next_offset: options.event_from,
            ..Default::default()
        };

        for _ in 0..options.max_iterations {
            let records = owner
                .read_owner_replication_records(
                    topic,
                    partition,
                    group,
                    progress.message_next_offset,
                    progress.event_next_offset,
                    options.max_messages_per_read,
                    options.max_events_per_read,
                )
                .await?;

            let message_progress = owner_read_batch_progress(&records.messages);
            let event_progress = owner_read_batch_progress(&records.events);
            let (message_record_count, message_next_offset, event_record_count, event_next_offset) =
                match (message_progress, event_progress) {
                    (
                        Some((message_record_count, message_next_offset)),
                        Some((event_record_count, event_next_offset)),
                    ) => (
                        message_record_count,
                        message_next_offset,
                        event_record_count,
                        event_next_offset,
                    ),
                    _ => {
                        let apply = self
                            .apply_follower_replication_records(topic, partition, group, records)
                            .await?;
                        return match apply {
                            BrokerFollowerReplicationApply::CheckpointRequired {
                                messages,
                                events,
                            } => Ok(BrokerReplicationCatchUp::CheckpointRequired {
                                progress,
                                messages,
                                events,
                            }),
                            BrokerFollowerReplicationApply::Applied(_) => {
                                tracing::error!(
                                    topic,
                                    partition,
                                    group,
                                    "replication catch-up applied records even though owner read reported checkpoint required"
                                );
                                Err(BrokerError::Unknown(
                                    "checkpoint-required owner read unexpectedly applied".into(),
                                ))
                            }
                        };
                    }
                };

            match self
                .apply_follower_replication_records(topic, partition, group, records)
                .await?
            {
                BrokerFollowerReplicationApply::Applied(_) => {}
                BrokerFollowerReplicationApply::CheckpointRequired { messages, events } => {
                    return Ok(BrokerReplicationCatchUp::CheckpointRequired {
                        progress,
                        messages,
                        events,
                    });
                }
            }

            progress.iterations += 1;
            progress.applied_message_records += message_record_count;
            progress.applied_event_records += event_record_count;
            progress.message_next_offset = message_next_offset;
            progress.event_next_offset = event_next_offset;

            if message_record_count == 0 && event_record_count == 0 {
                return Ok(BrokerReplicationCatchUp::CaughtUp(progress));
            }
        }

        Ok(BrokerReplicationCatchUp::IterationLimit { progress })
    }

    pub async fn catch_up_replication_follower_from_owner_with_checkpoint(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
        options: BrokerReplicationCatchUpOptions,
    ) -> Result<BrokerReplicationCatchUp, BrokerError> {
        if options.max_messages_per_read == 0
            || options.max_events_per_read == 0
            || options.max_iterations == 0
        {
            return Err(BrokerError::InvalidArgument(
                "replication catch-up limits must be greater than zero".into(),
            ));
        }

        let mut options = options;
        for checkpoint_attempts in 0..=1 {
            let outcome = self
                .catch_up_replication_follower_from_owner(owner, topic, partition, group, options)
                .await?;

            let BrokerReplicationCatchUp::CheckpointRequired {
                progress,
                messages,
                events,
            } = outcome
            else {
                return Ok(outcome);
            };

            if checkpoint_attempts == 1 {
                return Ok(BrokerReplicationCatchUp::CheckpointRequired {
                    progress,
                    messages,
                    events,
                });
            }

            let checkpoint = owner
                .export_owner_state_checkpoint(topic, partition, group)
                .await?;
            self.install_follower_state_checkpoint(
                topic,
                partition,
                group,
                FollowerStateCheckpointInstall {
                    message_next_offset: checkpoint.message_checkpoint_offset,
                    event_next_offset: checkpoint.event_next_offset,
                    applied_event_offset: checkpoint.applied_event_offset,
                    state_snapshot: checkpoint.state_snapshot,
                },
            )
            .await?;

            options.message_from = checkpoint.message_checkpoint_offset;
            options.event_from = checkpoint.event_next_offset;
        }

        tracing::error!(
            topic,
            partition,
            group,
            "checkpoint-aware catch-up loop reached an unreachable state"
        );
        unreachable!("checkpoint_attempts loop always returns");
    }

    pub async fn run_follower_replication_worker_once(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        queue: &crate::coordination::QueueIdentity,
        cfg: FollowerReplicationWorkerConfig,
    ) -> Result<BrokerReplicationCatchUp, BrokerError> {
        let worker = self.follower_replication_worker(queue)?;
        let (options, install_checkpoint) = {
            let state = worker.lock().await;
            (
                state.catch_up_options(cfg),
                state.should_install_checkpoint(cfg),
            )
        };

        let outcome = if install_checkpoint {
            self.catch_up_replication_follower_from_owner_with_checkpoint(
                owner,
                queue.topic.as_str(),
                queue.partition,
                queue.group.as_deref(),
                options,
            )
            .await?
        } else {
            self.catch_up_replication_follower_from_owner(
                owner,
                queue.topic.as_str(),
                queue.partition,
                queue.group.as_deref(),
                options,
            )
            .await?
        };

        worker.lock().await.record_catch_up(cfg, &outcome);
        Ok(outcome)
    }

    pub async fn run_follower_replication_worker_loop(
        &self,
        assignment: PartitionAssignment,
        resolver: Arc<dyn BrokerOwnerReplicationPeerResolver>,
        cfg: FollowerReplicationWorkerConfig,
        shutdown: CancellationToken,
    ) -> Result<FollowerReplicationWorkerLoopExit, BrokerError> {
        let mut ticks = 0;
        loop {
            let cfg = if cfg.follow_runtime_settings {
                let snap = self.config_snapshot();
                FollowerReplicationWorkerConfig {
                    caught_up_poll_ms: snap.replication_caught_up_poll_ms,
                    retry_poll_ms: snap.replication_retry_poll_ms,
                    checkpoint_retry_poll_ms: snap.replication_checkpoint_retry_poll_ms,
                    ..cfg
                }
            } else {
                cfg
            };
            let Ok(runtime) = self.follower_replication_worker_runtime(&assignment.queue) else {
                return Ok(FollowerReplicationWorkerLoopExit::WorkerStopped { ticks });
            };

            if shutdown.is_cancelled() {
                return Ok(FollowerReplicationWorkerLoopExit::Cancelled { ticks });
            }

            let Some(owner) = resolver.resolve_owner_peer(&assignment).await? else {
                tracing::warn!(
                    topic = %assignment.queue.topic,
                    partition = assignment.queue.partition,
                    group = ?assignment.queue.group,
                    owner = %assignment.owner,
                    "follower replication worker could not resolve owner peer"
                );
                wait_for_follower_worker_delay(cfg.retry_poll_ms, &shutdown, &runtime.shutdown)
                    .await;
                continue;
            };

            let Some(_tick_guard) = runtime.begin_tick() else {
                return Ok(FollowerReplicationWorkerLoopExit::WorkerStopped { ticks });
            };

            if shutdown.is_cancelled() {
                return Ok(FollowerReplicationWorkerLoopExit::Cancelled { ticks });
            }

            match self
                .run_follower_replication_worker_once(owner.as_ref(), &assignment.queue, cfg)
                .await
            {
                Ok(_) => {
                    ticks += 1;
                }
                Err(BrokerError::InvalidArgument(_))
                    if !self
                        .follower_replication_workers
                        .contains_key(&assignment.queue) =>
                {
                    return Ok(FollowerReplicationWorkerLoopExit::WorkerStopped { ticks });
                }
                Err(BrokerError::NotOwner { .. }) => {
                    tracing::warn!(
                        topic = %assignment.queue.topic,
                        partition = assignment.queue.partition,
                        group = ?assignment.queue.group,
                        owner = %assignment.owner,
                        "follower replication worker owner peer is no longer owner"
                    );
                    return Ok(FollowerReplicationWorkerLoopExit::OwnerChanged { ticks });
                }
                Err(err) => {
                    tracing::warn!(
                        topic = %assignment.queue.topic,
                        partition = assignment.queue.partition,
                        group = ?assignment.queue.group,
                        owner = %assignment.owner,
                        "follower replication worker tick failed: {err:?}"
                    );
                    drop(_tick_guard);
                    wait_for_follower_worker_delay(cfg.retry_poll_ms, &shutdown, &runtime.shutdown)
                        .await;
                    continue;
                }
            }
            drop(_tick_guard);

            let Ok(runtime) = self.follower_replication_worker_runtime(&assignment.queue) else {
                return Ok(FollowerReplicationWorkerLoopExit::WorkerStopped { ticks });
            };

            let delay_ms = runtime.state.lock().await.next_delay_ms;
            wait_for_follower_worker_delay(delay_ms, &shutdown, &runtime.shutdown).await;
        }
    }

    pub fn spawn_follower_replication_worker_loop(
        self: &Arc<Self>,
        assignment: PartitionAssignment,
        resolver: Arc<dyn BrokerOwnerReplicationPeerResolver>,
        cfg: FollowerReplicationWorkerConfig,
    ) -> Result<bool, BrokerError> {
        let runtime = self.follower_replication_worker_runtime(&assignment.queue)?;
        if runtime.started.swap(true, Ordering::AcqRel) {
            return Ok(false);
        }

        let broker = self.clone();
        let shutdown = runtime.shutdown.clone();
        self.task_group
            .spawn("follower_replication_worker", async move {
                let topic = assignment.queue.topic.to_string();
                let partition = assignment.queue.partition;
                let group = assignment.queue.group.clone();
                match broker
                    .run_follower_replication_worker_loop(assignment, resolver, cfg, shutdown)
                    .await
                {
                    Ok(outcome) => {
                        tracing::debug!(
                            topic,
                            partition,
                            group = ?group,
                            outcome = ?outcome,
                            "follower replication worker loop exited"
                        );
                    }
                    Err(err) => {
                        tracing::error!(
                            topic,
                            partition,
                            group = ?group,
                            "follower replication worker loop failed: {err:?}"
                        );
                    }
                }
            });

        Ok(true)
    }
}

impl BrokerOwnerReplicationPeer for Broker<StromaEngine> {
    fn read_owner_replication_records<'a>(
        &'a self,
        topic: &'a str,
        partition: LogId,
        group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
    ) -> BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        Box::pin(async move {
            Broker::<StromaEngine>::read_owner_replication_records(
                self,
                topic,
                partition,
                group,
                message_from,
                event_from,
                max_messages,
                max_events,
            )
            .await
        })
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: LogId,
        group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>> {
        Box::pin(async move {
            Broker::<StromaEngine>::export_owner_state_checkpoint(self, topic, partition, group)
                .await
        })
    }
}

impl<T> BrokerOwnerReplicationPeer for Arc<T>
where
    T: BrokerOwnerReplicationPeer + ?Sized,
{
    fn read_owner_replication_records<'a>(
        &'a self,
        topic: &'a str,
        partition: LogId,
        group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
    ) -> BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        self.as_ref().read_owner_replication_records(
            topic,
            partition,
            group,
            message_from,
            event_from,
            max_messages,
            max_events,
        )
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: LogId,
        group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>> {
        self.as_ref()
            .export_owner_state_checkpoint(topic, partition, group)
    }
}

async fn wait_for_follower_worker_delay(
    delay_ms: u64,
    shutdown: &CancellationToken,
    worker_shutdown: &CancellationToken,
) {
    tokio::select! {
        _ = shutdown.cancelled() => {}
        _ = worker_shutdown.cancelled() => {}
        _ = tokio::time::sleep(Duration::from_millis(delay_ms)) => {}
    }
}

fn checkpoint_required<T>(
    read: &OwnerReplicationRead<T>,
) -> Option<BrokerReplicationCheckpointRequired> {
    match read {
        OwnerReplicationRead::Batch(_) => None,
        OwnerReplicationRead::CheckpointRequired {
            epoch,
            requested_offset,
            head_offset,
            next_offset,
        } => Some(BrokerReplicationCheckpointRequired {
            epoch: *epoch,
            requested_offset: *requested_offset,
            head_offset: *head_offset,
            next_offset: *next_offset,
        }),
    }
}

fn owner_read_batch_progress<T>(read: &OwnerReplicationRead<T>) -> Option<(usize, Offset)> {
    match read {
        OwnerReplicationRead::Batch(batch) => {
            // Stroma reports the owner log tail as batch.next_offset. For a
            // limited read, the next pull must continue after the last returned
            // record rather than jumping straight to the owner tail.
            let next_request_offset = batch
                .records
                .last()
                .map_or(batch.next_offset, |(offset, _)| offset + 1);
            Some((batch.records.len(), next_request_offset))
        }
        OwnerReplicationRead::CheckpointRequired { .. } => None,
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
