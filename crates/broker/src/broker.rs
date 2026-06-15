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
use futures::{FutureExt, future::BoxFuture};
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
    EvictOutcome, FollowerStateCheckpointInstall, FollowerStateCheckpointInstallOutcome,
    KDurability, OwnerStateCheckpoint, QueueEngine, QueuePromotionOutcome, ReplicatedAppendOutcome,
    ReplicatedEventBatch, ReplicatedMessageBatch, ReplicatedQueueApplyOutcome, SettleKind,
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
            replication_max_messages_per_read: 256,
            replication_max_events_per_read: 256,
            replication_max_bytes_per_read: 8 * 1024 * 1024,
            replication_max_iterations_per_tick: 8,
            replication_min_in_sync_replicas: 1,
            replication_isr_timeout_ms: 10_000,
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

#[derive(Debug)]
pub struct BrokerOwnerReplicationRecords {
    pub messages: OwnerReplicationRead<Message>,
    pub events: OwnerReplicationRead<StromaEvent>,
}

pub trait BrokerOwnerReplicationPeer: Send + Sync {
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
    ) -> BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>>;

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
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
    pub max_bytes_per_read: usize,
    pub max_iterations: usize,
    pub max_wait_ms: u64,
}

impl Default for BrokerReplicationCatchUpOptions {
    fn default() -> Self {
        Self {
            message_from: 0,
            event_from: 0,
            max_messages_per_read: 256,
            max_events_per_read: 256,
            max_bytes_per_read: 8 * 1024 * 1024,
            max_iterations: 1024,
            max_wait_ms: 0,
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
    pub max_bytes_per_read: usize,
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
            max_bytes_per_read: 8 * 1024 * 1024,
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
        max_wait_ms: u64,
    ) -> BrokerReplicationCatchUpOptions {
        BrokerReplicationCatchUpOptions {
            message_from,
            event_from,
            max_messages_per_read: self.max_messages_per_read,
            max_events_per_read: self.max_events_per_read,
            max_bytes_per_read: self.max_bytes_per_read,
            max_iterations: self.max_iterations_per_tick,
            max_wait_ms,
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
        let max_wait_ms = match self.status {
            FollowerReplicationWorkerStatus::CaughtUp => cfg.caught_up_poll_ms,
            _ => 0,
        };
        cfg.catch_up_options(
            self.message_next_offset,
            self.event_next_offset,
            max_wait_ms,
        )
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
                self.next_delay_ms = 0;
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
    pub owned_replicas: Vec<OwnedQueueReplicaObservability>,
    pub owned_replica_summary: OwnedQueueReplicaSummary,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FollowerReplicationWorkerObservability {
    pub topic: String,
    pub partition: Partition,
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

/// Owner-side view of one follower's replication health for a queue this broker
/// owns: its last-reported durable offsets, how stale that report is, and
/// whether it currently counts as in-sync.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FollowerReplicaObservability {
    pub node_id: String,
    pub durable_message_next: Offset,
    pub durable_event_next: Offset,
    /// Time since the last progress report; `None` if it has never reported.
    pub last_report_age_ms: Option<u64>,
    pub in_sync: bool,
}

/// Owner-side replication health for one owned queue: the durability policy in
/// force, the in-sync replica count against the configured floor, and the
/// per-follower detail. Surfaces replication lag and ISR risk in topology views.
#[derive(Debug, Clone, serde::Serialize)]
pub struct OwnedQueueReplicaObservability {
    pub topic: String,
    pub partition: Partition,
    pub group: Option<String>,
    pub durability: String,
    pub min_in_sync_replicas: usize,
    pub in_sync_replicas: usize,
    pub below_floor: bool,
    pub followers: Vec<FollowerReplicaObservability>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OwnedQueueReplicaSummary {
    pub owned_queue_count: usize,
    pub below_floor_count: usize,
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
    part: Partition,
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
    // used to wake caught-up follower long-poll reads
    replication_notify: tokio::sync::Notify,
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
}

/// Sentinel for [`QueueLoopState::exclusive_assignee`] meaning "no exclusive
/// gate" (deliver to all consumers).
const NO_EXCLUSIVE_ASSIGNEE: u64 = u64::MAX;

/// Sentinel for [`QueueLoopState::hold_above_offset`] meaning "no shrink hold"
/// (deliver every offset).
const NO_HOLD_ABOVE: u64 = u64::MAX;

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

    /// Record a follower's durable replication progress (from a stamped
    /// replication read: followers apply durably, so their pull offsets are
    /// durable watermarks). Wakes any publish confirms waiting on policy.
    pub fn record_follower_replication_progress(
        &self,
        topic: &str,
        partition: Partition,
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
        partition: Partition,
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

    pub fn has_follower_replication_worker(
        &self,
        topic: &str,
        partition: Partition,
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
        partition: Partition,
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

    async fn stop_all_follower_replication_workers(&self) {
        let queues = self
            .follower_replication_workers
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for queue in queues {
            self.stop_follower_replication_worker(&queue).await;
        }
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
        }
    }

    /// Owner-side replication health: for every queue this broker owns, the
    /// per-follower durable progress, staleness, and in-sync status against the
    /// configured floor. Drives replication-lag / ISR-risk topology views.
    fn owned_replica_observability_report(
        &self,
    ) -> (
        Vec<OwnedQueueReplicaObservability>,
        OwnedQueueReplicaSummary,
    ) {
        let cfg = self.config_snapshot();
        let isr_timeout = std::time::Duration::from_millis(cfg.replication_isr_timeout_ms);
        let min_in_sync = cfg.replication_min_in_sync_replicas;

        let mut below_floor_count = 0;
        let mut owned: Vec<_> =
            self.assignment_cache
                .iter()
                .filter(|entry| {
                    let key = entry.key();
                    self.ownership
                        .owns_queue(&key.tp, key.part, key.group.as_deref())
                })
                .map(|entry| {
                    let key = entry.key();
                    let assignment = entry.value();
                    let cell = self.replication_progress.get(key).map(|c| c.clone());
                    let now = std::time::Instant::now();

                    let followers: Vec<_> = assignment
                        .followers
                        .iter()
                        .map(|node_id| {
                            let progress = cell
                                .as_ref()
                                .and_then(|cell| cell.lock_followers().get(node_id).copied());
                            let (
                                durable_message_next,
                                durable_event_next,
                                last_report_age_ms,
                                in_sync,
                            ) = match progress {
                                Some(progress) => {
                                    let age = now.duration_since(progress.last_report);
                                    (
                                        progress.message_next,
                                        progress.event_next,
                                        Some(age.as_millis().min(u64::MAX as u128) as u64),
                                        age <= isr_timeout,
                                    )
                                }
                                None => (0, 0, None, false),
                            };
                            FollowerReplicaObservability {
                                node_id: node_id.clone(),
                                durable_message_next,
                                durable_event_next,
                                last_report_age_ms,
                                in_sync,
                            }
                        })
                        .collect();

                    let in_sync_replicas = 1 + followers.iter().filter(|f| f.in_sync).count();
                    let below_floor = in_sync_replicas < min_in_sync;
                    if below_floor {
                        below_floor_count += 1;
                    }
                    OwnedQueueReplicaObservability {
                        topic: key.tp.clone(),
                        partition: key.part,
                        group: key.group.clone(),
                        durability: format!("{:?}", assignment.durability),
                        min_in_sync_replicas: min_in_sync,
                        in_sync_replicas,
                        below_floor,
                        followers,
                    }
                })
                .collect();
        owned.sort_by(|a, b| {
            a.group
                .cmp(&b.group)
                .then_with(|| a.topic.cmp(&b.topic))
                .then_with(|| a.partition.cmp(&b.partition))
        });
        let summary = OwnedQueueReplicaSummary {
            owned_queue_count: owned.len(),
            below_floor_count,
        };
        (owned, summary)
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
        let (done_tx, done_rx) = oneshot::channel::<bool>();
        let mut done_tx = Some(done_tx);
        let done = move |ok: bool| {
            if let Some(qs) = &qs {
                if ok {
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
        let done = move |ok: bool| {
            if ok {
                consumer2.dec_inflight();
                if let SettleKind::Ack = settle_kind
                    && let Some(metrics) = metrics
                {
                    metrics.acked();
                }

                if let Some(qs) = &qs {
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

            let done = move |ok: bool| {
                if ok {
                    for _ in 0..count {
                        consumer_clone.dec_inflight();
                    }
                    if let Some(m) = metrics {
                        m.acked_many(count as u64);
                    }
                    if let Some(qs) = &qs {
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

            let done = move |ok: bool| {
                if ok {
                    for _ in 0..count {
                        consumer_clone.dec_inflight();
                    }
                    if let Some(qs) = &qs {
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

                    // TODO: Also limit each poll batch based on size of aggreated messages
                    let deliverables = match broker
                        .engine
                        .poll_ready(
                            &key.tp,
                            key.part.id(),
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
                    // `poll_ready` records the in-flight lease before returning,
                    // so followers need a prompt read even if local delivery
                    // later races with subscriber capacity.
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

    pub async fn read_owner_replication_records(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> Result<BrokerOwnerReplicationRecords, BrokerError> {
        if max_messages == 0 || max_events == 0 || max_bytes == 0 {
            return Err(BrokerError::InvalidArgument(
                "replication read limits must be greater than zero".into(),
            ));
        }

        if max_wait_ms == 0 {
            return self
                .read_owner_replication_records_now(
                    topic,
                    partition,
                    group,
                    message_from,
                    event_from,
                    max_messages,
                    max_events,
                    max_bytes,
                )
                .await;
        }

        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        let qs = self.queue(&key).await;
        let notified = qs.replication_notify.notified();
        tokio::pin!(notified);

        let records = self
            .read_owner_replication_records_now(
                topic,
                partition,
                group,
                message_from,
                event_from,
                max_messages,
                max_events,
                max_bytes,
            )
            .await?;
        if !owner_replication_records_empty(&records) {
            return Ok(records);
        }

        tokio::select! {
            _ = &mut notified => {}
            _ = self.shutdown_publishers.cancelled() => {}
            _ = tokio::time::sleep(Duration::from_millis(max_wait_ms)) => {}
        }

        self.read_owner_replication_records_now(
            topic,
            partition,
            group,
            message_from,
            event_from,
            max_messages,
            max_events,
            max_bytes,
        )
        .await
    }

    async fn read_owner_replication_records_now(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<BrokerOwnerReplicationRecords, BrokerError> {
        self.ensure_queue_owner(topic, partition, group)?;
        let messages = self
            .engine
            .read_owner_message_records(topic, partition.id(), group, message_from, max_messages)
            .await?;
        let events = self
            .engine
            .read_owner_event_records(topic, partition.id(), group, event_from, max_events)
            .await?;

        Ok(cap_owner_replication_records(
            BrokerOwnerReplicationRecords { messages, events },
            max_bytes,
        ))
    }

    pub async fn become_replication_follower(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Result<(), BrokerError> {
        self.engine
            .become_queue_follower(topic, partition.id(), group)
            .await?;
        Ok(())
    }

    /// Fence both queue logs at `epoch` (persisted, monotonic). Also the
    /// substrate for future manual-fence operator tooling.
    pub async fn advance_replication_epoch(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<u64, BrokerError> {
        self.engine
            .advance_queue_epoch(topic, partition.id(), group, epoch)
            .await
            .map_err(BrokerError::from)
    }

    /// `become_replication_follower` fenced at the assignment epoch.
    pub async fn become_replication_follower_with_epoch(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<(), BrokerError> {
        self.engine
            .become_queue_follower_with_epoch(topic, partition.id(), group, epoch)
            .await?;
        Ok(())
    }

    pub async fn export_owner_state_checkpoint(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Result<OwnerStateCheckpoint, BrokerError> {
        self.ensure_queue_owner(topic, partition, group)?;
        Ok(self
            .engine
            .export_owner_state_checkpoint(topic, partition.id(), group)
            .await?)
    }

    pub async fn install_follower_state_checkpoint(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        install: FollowerStateCheckpointInstall,
    ) -> Result<FollowerStateCheckpointInstallOutcome, BrokerError> {
        Ok(self
            .engine
            .install_follower_state_checkpoint(topic, partition.id(), group, install)
            .await?)
    }

    pub async fn apply_follower_replication_records(
        &self,
        topic: &str,
        partition: Partition,
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
                    partition = partition.id(),
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
                    partition = partition.id(),
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
            .apply_replicated_queue_batch(topic, partition.id(), group, messages, events)
            .await?;
        Ok(BrokerFollowerReplicationApply::Applied(outcome))
    }

    pub async fn promote_replication_follower_if_caught_up(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        expected_message_next_offset: Offset,
        expected_event_next_offset: Offset,
    ) -> Result<QueuePromotionOutcome, BrokerError> {
        self.engine
            .promote_queue_follower_if_caught_up(
                topic,
                partition.id(),
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
        partition: Partition,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<QueuePromotionOutcome, BrokerError> {
        self.engine
            .promote_queue_follower_to_local_tail(topic, partition.id(), group, epoch)
            .await
            .map_err(BrokerError::from)
    }

    pub async fn catch_up_replication_follower_from_owner(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        options: BrokerReplicationCatchUpOptions,
    ) -> Result<BrokerReplicationCatchUp, BrokerError> {
        if options.max_messages_per_read == 0
            || options.max_events_per_read == 0
            || options.max_bytes_per_read == 0
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
                    options.max_bytes_per_read,
                    options.max_wait_ms,
                )
                .await?;

            let message_progress = owner_read_batch_progress("message", &records.messages)?;
            let event_progress = owner_read_batch_progress("event", &records.events)?;
            let (message_progress, event_progress) = match (message_progress, event_progress) {
                (Some(message_progress), Some(event_progress)) => {
                    (message_progress, event_progress)
                }
                _ => {
                    let apply = self
                        .apply_follower_replication_records(topic, partition, group, records)
                        .await?;
                    return match apply {
                        BrokerFollowerReplicationApply::CheckpointRequired { messages, events } => {
                            Ok(BrokerReplicationCatchUp::CheckpointRequired {
                                progress,
                                messages,
                                events,
                            })
                        }
                        BrokerFollowerReplicationApply::Applied(_) => {
                            tracing::error!(
                                topic,
                                partition = partition.id(),
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
                BrokerFollowerReplicationApply::Applied(outcome) => {
                    match replicated_append_progress_after_apply(
                        "message",
                        &message_progress,
                        outcome.message_log.as_ref(),
                    )? {
                        ReplicatedAppendProgress::Advanced(next_offset) => {
                            progress.message_next_offset = next_offset;
                        }
                        ReplicatedAppendProgress::CheckpointRequired(checkpoint) => {
                            return Ok(BrokerReplicationCatchUp::CheckpointRequired {
                                progress,
                                messages: Some(checkpoint),
                                events: None,
                            });
                        }
                    }
                    match replicated_append_progress_after_apply(
                        "event",
                        &event_progress,
                        outcome.event_log.as_ref(),
                    )? {
                        ReplicatedAppendProgress::Advanced(next_offset) => {
                            progress.event_next_offset = next_offset;
                        }
                        ReplicatedAppendProgress::CheckpointRequired(checkpoint) => {
                            return Ok(BrokerReplicationCatchUp::CheckpointRequired {
                                progress,
                                messages: None,
                                events: Some(checkpoint),
                            });
                        }
                    }
                }
                BrokerFollowerReplicationApply::CheckpointRequired { messages, events } => {
                    return Ok(BrokerReplicationCatchUp::CheckpointRequired {
                        progress,
                        messages,
                        events,
                    });
                }
            }

            progress.iterations += 1;
            progress.applied_message_records += message_progress.record_count;
            progress.applied_event_records += event_progress.record_count;

            if message_progress.record_count == 0 && event_progress.record_count == 0 {
                return Ok(BrokerReplicationCatchUp::CaughtUp(progress));
            }
        }

        Ok(BrokerReplicationCatchUp::IterationLimit { progress })
    }

    pub async fn catch_up_replication_follower_from_owner_with_checkpoint(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        options: BrokerReplicationCatchUpOptions,
    ) -> Result<BrokerReplicationCatchUp, BrokerError> {
        if options.max_messages_per_read == 0
            || options.max_events_per_read == 0
            || options.max_bytes_per_read == 0
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
            partition = partition.id(),
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
        let options = {
            let state = worker.lock().await;
            state.catch_up_options(cfg)
        };

        let outcome = if cfg.allow_checkpoint_install {
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
                    max_messages_per_read: snap.replication_max_messages_per_read,
                    max_events_per_read: snap.replication_max_events_per_read,
                    max_bytes_per_read: snap.replication_max_bytes_per_read,
                    max_iterations_per_tick: snap.replication_max_iterations_per_tick,
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
                    partition = assignment.queue.partition.id(),
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

            let tick_result = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    return Ok(FollowerReplicationWorkerLoopExit::Cancelled { ticks });
                }
                _ = runtime.shutdown.cancelled() => {
                    return Ok(FollowerReplicationWorkerLoopExit::WorkerStopped { ticks });
                }
                outcome = self.run_follower_replication_worker_once(
                    owner.as_ref(),
                    &assignment.queue,
                    cfg,
                ) => outcome,
            };

            match tick_result {
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
                        partition = assignment.queue.partition.id(),
                        group = ?assignment.queue.group,
                        owner = %assignment.owner,
                        "follower replication worker owner peer is no longer owner"
                    );
                    return Ok(FollowerReplicationWorkerLoopExit::OwnerChanged { ticks });
                }
                Err(err) => {
                    tracing::warn!(
                        topic = %assignment.queue.topic,
                        partition = assignment.queue.partition.id(),
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
                            partition = partition.id(),
                            group = ?group,
                            outcome = ?outcome,
                            "follower replication worker loop exited"
                        );
                    }
                    Err(err) => {
                        tracing::error!(
                            topic,
                            partition = partition.id(),
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
        partition: Partition,
        group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
        max_wait_ms: u64,
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
                max_bytes,
                max_wait_ms,
            )
            .await
        })
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
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
        partition: Partition,
        group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        self.as_ref().read_owner_replication_records(
            topic,
            partition,
            group,
            message_from,
            event_from,
            max_messages,
            max_events,
            max_bytes,
            max_wait_ms,
        )
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
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

struct OwnerMessageReadCap {
    read: OwnerReplicationRead<Message>,
    returned_frontier: Option<Offset>,
    owner_tail: Option<Offset>,
}

fn cap_owner_replication_records(
    records: BrokerOwnerReplicationRecords,
    max_bytes: usize,
) -> BrokerOwnerReplicationRecords {
    let capped_messages = cap_owner_message_read_by_bytes(records.messages, max_bytes);
    let events = match (
        capped_messages.returned_frontier,
        capped_messages.owner_tail,
    ) {
        (Some(message_frontier), Some(message_owner_tail)) => {
            cap_owner_event_read_to_message_frontier(
                records.events,
                message_frontier,
                message_owner_tail,
            )
        }
        _ => records.events,
    };

    BrokerOwnerReplicationRecords {
        messages: capped_messages.read,
        events,
    }
}

fn cap_owner_message_read_by_bytes(
    read: OwnerReplicationRead<Message>,
    max_bytes: usize,
) -> OwnerMessageReadCap {
    let OwnerReplicationRead::Batch(mut batch) = read else {
        return OwnerMessageReadCap {
            read,
            returned_frontier: None,
            owner_tail: None,
        };
    };

    let owner_tail = batch.next_offset;
    let mut used = 0usize;
    let mut kept = Vec::with_capacity(batch.records.len());
    for (offset, message) in batch.records.into_iter() {
        let record_bytes = approximate_replication_message_bytes(&message);
        if !kept.is_empty() && used.saturating_add(record_bytes) > max_bytes {
            break;
        }
        used = used.saturating_add(record_bytes);
        kept.push((offset, message));
    }

    let returned_frontier = kept
        .last()
        .map(|(offset, _)| offset.saturating_add(1))
        .unwrap_or(batch.requested_offset);
    batch.records = kept;

    OwnerMessageReadCap {
        read: OwnerReplicationRead::Batch(batch),
        returned_frontier: Some(returned_frontier),
        owner_tail: Some(owner_tail),
    }
}

fn approximate_replication_message_bytes(message: &Message) -> usize {
    message
        .headers
        .len()
        .saturating_add(message.payload.len())
        .saturating_add(32)
}

fn cap_owner_event_read_to_message_frontier(
    read: OwnerReplicationRead<StromaEvent>,
    message_frontier: Offset,
    message_owner_tail: Offset,
) -> OwnerReplicationRead<StromaEvent> {
    let OwnerReplicationRead::Batch(mut batch) = read else {
        return read;
    };

    let message_tail_fully_returned = message_frontier >= message_owner_tail;
    let mut kept = Vec::with_capacity(batch.records.len());
    for (offset, event) in batch.records.into_iter() {
        if !stroma_event_available_for_replicated_messages(
            &event,
            message_frontier,
            message_tail_fully_returned,
        ) {
            break;
        }
        kept.push((offset, event));
    }
    batch.records = kept;
    OwnerReplicationRead::Batch(batch)
}

fn stroma_event_available_for_replicated_messages(
    event: &StromaEvent,
    message_frontier: Offset,
    message_tail_fully_returned: bool,
) -> bool {
    match event {
        StromaEvent::Enqueue { off, .. }
        | StromaEvent::EnqueueDelayed { off, .. }
        | StromaEvent::MarkInflight { off, .. }
        | StromaEvent::Ack { off }
        | StromaEvent::Nack { off, .. } => *off < message_frontier,
        StromaEvent::EnqueueMany { reqs } => reqs.iter().all(|req| req.off < message_frontier),
        StromaEvent::EnqueueDelayedMany { reqs } => {
            reqs.iter().all(|req| req.off < message_frontier)
        }
        StromaEvent::MarkInflightMany { reqs } => reqs.iter().all(|req| req.off < message_frontier),
        StromaEvent::AckMany { reqs } | StromaEvent::ReleaseInflightMany { reqs } => {
            reqs.iter().all(|req| req.off < message_frontier)
        }
        StromaEvent::NackMany { reqs } => reqs.iter().all(|req| req.off < message_frontier),
        StromaEvent::DeadLetter { reqs } => reqs.iter().all(|req| req.off < message_frontier),
        StromaEvent::DeadLetterCommit { offs } => offs.iter().all(|off| *off < message_frontier),
        StromaEvent::Declare(_) | StromaEvent::ResetQueue { .. } => true,
        StromaEvent::Snapshot { .. } => message_tail_fully_returned,
    }
}

fn owner_replication_records_empty(records: &BrokerOwnerReplicationRecords) -> bool {
    fn read_empty<T>(read: &OwnerReplicationRead<T>) -> bool {
        matches!(read, OwnerReplicationRead::Batch(batch) if batch.records.is_empty())
    }

    read_empty(&records.messages) && read_empty(&records.events)
}

#[derive(Debug, Clone, Copy)]
struct OwnerBatchProgress {
    epoch: u64,
    requested_offset: Offset,
    first_offset: Option<Offset>,
    record_count: usize,
    next_request_offset: Offset,
    owner_next_offset: Offset,
}

fn owner_read_batch_progress<T>(
    stream: &'static str,
    read: &OwnerReplicationRead<T>,
) -> Result<Option<OwnerBatchProgress>, BrokerError> {
    match read {
        OwnerReplicationRead::Batch(batch) => {
            let first_offset = batch.records.first().map(|(offset, _)| *offset);
            let next_request_offset = match batch.records.last() {
                Some((offset, _)) => offset + 1,
                None if batch.next_offset == batch.requested_offset => batch.next_offset,
                None => {
                    return Err(BrokerError::InvalidReplicationProgress {
                        stream,
                        reason: format!(
                            "owner returned empty batch at requested offset {} but reported tail {}",
                            batch.requested_offset, batch.next_offset
                        ),
                    });
                }
            };

            Ok(Some(OwnerBatchProgress {
                epoch: batch.epoch,
                requested_offset: batch.requested_offset,
                first_offset,
                record_count: batch.records.len(),
                next_request_offset,
                owner_next_offset: batch.next_offset,
            }))
        }
        OwnerReplicationRead::CheckpointRequired { .. } => Ok(None),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ReplicatedAppendProgress {
    Advanced(Offset),
    CheckpointRequired(BrokerReplicationCheckpointRequired),
}

fn replicated_append_progress_after_apply(
    stream: &'static str,
    owner: &OwnerBatchProgress,
    outcome: Option<&ReplicatedAppendOutcome>,
) -> Result<ReplicatedAppendProgress, BrokerError> {
    let Some(first_offset) = owner.first_offset else {
        if outcome.is_some() {
            return Err(BrokerError::InvalidReplicationProgress {
                stream,
                reason: "local append outcome present for empty owner batch".into(),
            });
        }
        return Ok(ReplicatedAppendProgress::Advanced(
            owner.next_request_offset,
        ));
    };

    let Some(outcome) = outcome else {
        return Err(BrokerError::InvalidReplicationProgress {
            stream,
            reason: "missing local append outcome for non-empty owner batch".into(),
        });
    };

    match *outcome {
        ReplicatedAppendOutcome::Applied(result) => {
            let end = result.base_offset + u64::from(result.count);
            if result.base_offset == first_offset
                && result.count as usize == owner.record_count
                && end == owner.next_request_offset
            {
                Ok(ReplicatedAppendProgress::Advanced(
                    owner.next_request_offset,
                ))
            } else {
                Err(BrokerError::InvalidReplicationProgress {
                    stream,
                    reason: format!(
                        "applied range {}..{} does not match owner range {}..{} ({} records)",
                        result.base_offset,
                        end,
                        first_offset,
                        owner.next_request_offset,
                        owner.record_count
                    ),
                })
            }
        }
        ReplicatedAppendOutcome::AppliedSuffix {
            requested_first_offset,
            skipped_count,
            result,
        } => {
            let end = result.base_offset + u64::from(result.count);
            if requested_first_offset == first_offset
                && skipped_count as usize <= owner.record_count
                && result.base_offset == first_offset + u64::from(skipped_count)
                && skipped_count as usize + result.count as usize == owner.record_count
                && end == owner.next_request_offset
            {
                Ok(ReplicatedAppendProgress::Advanced(
                    owner.next_request_offset,
                ))
            } else {
                Err(BrokerError::InvalidReplicationProgress {
                    stream,
                    reason: format!(
                        "applied suffix {:?} does not match owner range {}..{} ({} records)",
                        outcome, first_offset, owner.next_request_offset, owner.record_count
                    ),
                })
            }
        }
        ReplicatedAppendOutcome::AlreadyPresent {
            first_offset: present_first,
            count,
            next_offset,
        } => {
            // AlreadyPresent supports idempotent retries. It does not prove the
            // bytes/events are identical; stronger overlap validation belongs
            // in Keratin if we need divergence detection later. Progress only
            // moves over the owner-returned range so catch-up does not skip
            // ahead merely because the local tail is higher.
            if present_first == first_offset
                && count as usize == owner.record_count
                && next_offset >= owner.next_request_offset
            {
                Ok(ReplicatedAppendProgress::Advanced(
                    owner.next_request_offset,
                ))
            } else {
                Err(BrokerError::InvalidReplicationProgress {
                    stream,
                    reason: format!(
                        "already-present range {:?} does not cover owner range {}..{} ({} records)",
                        outcome, first_offset, owner.next_request_offset, owner.record_count
                    ),
                })
            }
        }
        ReplicatedAppendOutcome::Overlap { .. } | ReplicatedAppendOutcome::Gap { .. } => {
            // A new follower can have stale local data for the same queue
            // identity. Without validating the overlapping bytes/events, the
            // only safe repair path is to install an owner checkpoint and then
            // continue from that checkpoint boundary.
            Ok(ReplicatedAppendProgress::CheckpointRequired(
                BrokerReplicationCheckpointRequired {
                    epoch: owner.epoch,
                    requested_offset: owner.requested_offset,
                    head_offset: first_offset,
                    next_offset: owner.owner_next_offset,
                },
            ))
        }
        ReplicatedAppendOutcome::StaleEpoch { .. } => {
            Err(BrokerError::InvalidReplicationProgress {
                stream,
                reason: format!("local append rejected owner batch: {outcome:?}"),
            })
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
mod replication_byte_limit_tests {
    use super::*;
    use stroma_core::OwnerReplicationBatch;

    fn message(payload_len: usize) -> Message {
        Message {
            flags: 0,
            headers: vec![0; 4],
            payload: vec![1; payload_len],
        }
    }

    fn message_batch(
        requested_offset: Offset,
        next_offset: Offset,
        records: Vec<(Offset, Message)>,
    ) -> OwnerReplicationRead<Message> {
        OwnerReplicationRead::Batch(OwnerReplicationBatch {
            epoch: 7,
            requested_offset,
            next_offset,
            records,
        })
    }

    fn event_batch(
        requested_offset: Offset,
        next_offset: Offset,
        records: Vec<(Offset, StromaEvent)>,
    ) -> OwnerReplicationRead<StromaEvent> {
        OwnerReplicationRead::Batch(OwnerReplicationBatch {
            epoch: 7,
            requested_offset,
            next_offset,
            records,
        })
    }

    #[test]
    fn byte_cap_keeps_first_oversized_message_so_replication_can_progress() {
        let records = BrokerOwnerReplicationRecords {
            messages: message_batch(10, 12, vec![(10, message(128)), (11, message(8))]),
            events: event_batch(20, 20, Vec::new()),
        };

        let capped = cap_owner_replication_records(records, 1);
        let OwnerReplicationRead::Batch(messages) = capped.messages else {
            panic!("expected message batch");
        };

        assert_eq!(messages.next_offset, 12);
        assert_eq!(messages.records.len(), 1);
        assert_eq!(messages.records[0].0, 10);
    }

    #[test]
    fn byte_cap_stops_events_before_unreturned_message_payloads() {
        let records = BrokerOwnerReplicationRecords {
            messages: message_batch(
                10,
                13,
                vec![(10, message(16)), (11, message(16)), (12, message(16))],
            ),
            events: event_batch(
                20,
                24,
                vec![
                    (20, StromaEvent::Declare(Default::default())),
                    (
                        21,
                        StromaEvent::Enqueue {
                            off: 10,
                            retries: 0,
                        },
                    ),
                    (
                        22,
                        StromaEvent::Enqueue {
                            off: 11,
                            retries: 0,
                        },
                    ),
                    (23, StromaEvent::Ack { off: 10 }),
                ],
            ),
        };

        let capped = cap_owner_replication_records(records, 60);
        let OwnerReplicationRead::Batch(messages) = capped.messages else {
            panic!("expected message batch");
        };
        let OwnerReplicationRead::Batch(events) = capped.events else {
            panic!("expected event batch");
        };

        assert_eq!(messages.next_offset, 13);
        assert_eq!(messages.records.len(), 1);
        assert_eq!(messages.records[0].0, 10);
        assert_eq!(events.next_offset, 24);
        assert_eq!(
            events
                .records
                .iter()
                .map(|(offset, _)| *offset)
                .collect::<Vec<_>>(),
            vec![20, 21]
        );
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
