//! Replication / clustering types and traits for the broker.
//!
//! First brick of the clustering-module separation: the replication data types,
//! peer/resolver traits, catch-up + stream outcome enums, and the stream-apply
//! trait, lifted out of broker.rs. Re-exported from `broker` so existing
//! `fibril_broker::broker::*` import paths keep resolving.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use futures::future::BoxFuture;
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::storage::{Offset, Partition};
use stroma_core::{Message, OwnerReplicationRead, StromaEvent};

use crate::broker::{Broker, BrokerConfig, BrokerError, FOLLOWER_STREAM_BUFFER_BATCHES, QueueKey};
use crate::coordination::PartitionAssignment;
use crate::queue_engine::{
    KDurability, QueueEngine, QueuePromotionOutcome, QueueStateCheckpoint, ReplicatedAppendOutcome,
    ReplicatedEventBatch, ReplicatedMessageBatch, ReplicatedStreamApplyOutcome, StreamPromotionOutcome,
    StreamStore, StromaEngine,
};

// Protect transient lag, while allowing an offline/stuck replica to fall back
// to non-destructive reseeding rather than retaining an unbounded suffix.
const FOLLOWER_RETENTION_GRACE: Duration = Duration::from_secs(60);
const RESEED_RETENTION_GRACE: Duration = Duration::from_secs(120);

#[derive(Debug)]
struct RetentionObservation {
    assignment: PartitionAssignment,
    since: Instant,
    followers: std::collections::HashMap<String, (u64, Instant)>,
}
impl RetentionObservation {
    /// Exclusive queue log position the protected followers still need.
    fn floor(
        &mut self,
        progress: &std::collections::HashMap<String, FollowerProgress>,
        now: Instant,
    ) -> u64 {
        self.assignment
            .followers
            .iter()
            .fold(u64::MAX, |floor, node| {
                if self
                    .assignment
                    .history
                    .as_ref()
                    .is_some_and(|h| h.suspended_replicas.contains(node))
                {
                    // Give an active repair a bounded interval to copy its fixed
                    // checkpoint without source compaction continually overtaking it.
                    return if now.duration_since(self.since) < RESEED_RETENTION_GRACE {
                        0
                    } else {
                        floor
                    };
                }
                if !self.assignment.is_followed_by(node) {
                    return floor;
                }
                // Only queue progress names a position in the queue log. Any
                // other report protects everything.
                let next = progress.get(node).map_or(0, |p| p.progress.queue_next().unwrap_or(0));
                let observed = self.followers.entry(node.clone()).or_insert((next, now));
                if observed.0 != next {
                    *observed = (next, now);
                }
                // Idle but connected replicas refresh their report without
                // moving offsets. They still need protection when traffic resumes.
                let last_active = progress.get(node).map_or(observed.1, |p| p.last_report.max(observed.1));
                if now.saturating_duration_since(last_active) >= FOLLOWER_RETENTION_GRACE {
                    return floor;
                }
                floor.min(next)
            })
    }
}

struct RetentionBook {
    queues: std::collections::HashMap<QueueKey, RetentionObservation>,
    last_sweep: Instant,
}

pub(crate) struct BrokerRetention<
    E: QueueEngine + crate::queue_engine::StreamStore + std::fmt::Debug + Send + Sync + 'static,
> {
    broker: std::sync::Weak<Broker<E>>,
    observations: std::sync::Mutex<RetentionBook>,
}
impl<E: QueueEngine + crate::queue_engine::StreamStore + std::fmt::Debug + Send + Sync + 'static>
    BrokerRetention<E>
{
    pub(crate) fn new(broker: std::sync::Weak<Broker<E>>) -> Self {
        Self {
            broker,
            observations: std::sync::Mutex::new(RetentionBook {queues:Default::default(),last_sweep:Instant::now()}),
        }
    }
}
impl<E: QueueEngine + crate::queue_engine::StreamStore + std::fmt::Debug + Send + Sync + 'static>
    std::fmt::Debug for BrokerRetention<E>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BrokerRetention")
    }
}
impl<E> stroma_core::QueueReplicationRetention for BrokerRetention<E>
where
    E: QueueEngine
        + crate::queue_engine::StreamStore
        + std::fmt::Debug
        + Clone
        + Send
        + Sync
        + 'static,
{
    fn retained_next(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
        binding: Option<&stroma_core::StorageHistoryBinding>,
    ) -> stroma_core::Result<u64> {
        let Some(broker) = self.broker.upgrade() else {
            return Ok(0);
        };
        let key = QueueKey {
            tp: topic.into(),
            part: partition.into(),
            group: group.map(str::to_owned),
        };
        let Some(assignment) = broker.assignment_cache.get(&key).map(|a| a.clone()) else {
            return Ok(if binding.is_some() { 0 } else { u64::MAX });
        };
        if binding.is_some_and(|binding| {
            assignment
                .history
                .as_ref()
                .is_none_or(|h| &h.binding != binding)
        }) || !broker.owns_queue_partition(topic, Partition::new(partition), group)
        {
            return Ok(0);
        }
        let now = Instant::now();
        let mut observations = self
            .observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if now.duration_since(observations.last_sweep) >= FOLLOWER_RETENTION_GRACE {
            observations.queues.retain(|key, _| broker.assignment_cache.contains_key(key));
            observations.last_sweep=now;
        }
        let observed = observations.queues
            .entry(key.clone())
            .or_insert_with(|| RetentionObservation {
                assignment: assignment.clone(),
                since: now,
                followers: Default::default(),
            });
        if observed.assignment != assignment {
            *observed = RetentionObservation {
                assignment,
                since: now,
                followers: Default::default(),
            };
        }
        let cell = broker.replication_progress.get(&key).map(|c| c.clone());
        Ok(match cell {
            Some(cell) => observed.floor(&cell.lock_followers(), now),
            None => observed.floor(&Default::default(), now),
        })
    }
}

/// Owner reads of a stream partition: the record log and the cursor-commit
/// event log, each with its own cursor.
#[derive(Debug)]
pub struct BrokerOwnerStreamRecords {
    pub messages: OwnerReplicationRead<Message>,
    pub events: OwnerReplicationRead<StromaEvent>,
}

pub trait BrokerOwnerReplicationPeer: Send + Sync {
    /// Read the owner's single queue log from `from`. Payload enqueues and
    /// state events share these offsets.
    fn read_owner_queue_records<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        from: Offset,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>>;

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
    ) -> BoxFuture<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>> {
        let _ = reporter_epoch;
        self.read_owner_queue_records(
            topic,
            partition,
            group,
            from,
            max_records,
            max_bytes,
            max_wait_ms,
        )
    }

    /// Read a stream's record and cursor-event logs. Peers that only serve
    /// queues keep the default, which refuses.
    fn read_owner_stream_records<'a>(
        &'a self,
        _topic: &'a str,
        _partition: Partition,
        _message_from: Offset,
        _event_from: Offset,
        _max_messages: usize,
        _max_events: usize,
        _max_bytes: usize,
        _max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<BrokerOwnerStreamRecords, BrokerError>> {
        Box::pin(async {
            Err(BrokerError::Unknown(
                "stream replication not supported by this peer".into(),
            ))
        })
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
    ) -> BoxFuture<'a, Result<BrokerOwnerStreamRecords, BrokerError>> {
        let _ = reporter_epoch;
        self.read_owner_stream_records(
            topic,
            partition,
            message_from,
            event_from,
            max_messages,
            max_events,
            max_bytes,
            max_wait_ms,
        )
    }

    fn export_queue_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<QueueStateCheckpoint, BrokerError>>;

    /// Run a credit-based queue push from this owner, applying batches
    /// through `apply` until the push ends. The default reports it unsupported
    /// so the follower worker falls back to pull. Pushing peers override it.
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
    ) -> BoxFuture<'a, Result<FollowerStreamExit, BrokerError>> {
        let _ = reporter_epoch;
        self.push_queue_replication(
            topic,
            partition,
            group,
            from,
            credit_bytes,
            tunables,
            buffer_batches,
            apply,
            shutdown,
        )
    }

    fn push_queue_replication<'a>(
        &'a self,
        _topic: &'a str,
        _partition: Partition,
        _group: Option<&'a str>,
        _from: Offset,
        _credit_bytes: u64,
        _tunables: StreamApplyTunablesFn,
        _buffer_batches: usize,
        _apply: Arc<dyn BrokerQueuePushApply>,
        _shutdown: CancellationToken,
    ) -> BoxFuture<'a, Result<FollowerStreamExit, BrokerError>> {
        Box::pin(async {
            Err(BrokerError::Unknown(
                "replication streaming not supported by this peer".into(),
            ))
        })
    }
}

/// Pins reports to the worker's assignment, even while the shared broker cache
/// has already observed a successor assignment and is cancelling the old worker.
struct AssignmentReplicationPeer<'p> {
    peer: &'p dyn BrokerOwnerReplicationPeer,
    epoch: u64,
    broker: Arc<Broker<StromaEngine>>,
    history: Option<crate::history_replication::HistoryReplicationSession>,
}
impl BrokerOwnerReplicationPeer for AssignmentReplicationPeer<'_> {
    fn read_owner_queue_records<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        from: Offset,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>> {
        Box::pin(async move {
            self.check_history()?;
            let records = self
                .peer
                .read_owner_queue_records_fenced(
                    topic,
                    partition,
                    group,
                    from,
                    max_records,
                    max_bytes,
                    max_wait_ms,
                    Some(self.epoch),
                )
                .await?;
            self.check_history()?;
            Ok(records)
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
    ) -> BoxFuture<'a, Result<BrokerOwnerStreamRecords, BrokerError>> {
        Box::pin(async move {
            self.check_history()?;
            let records = self
                .peer
                .read_owner_stream_records_fenced(
                    topic,
                    partition,
                    message_from,
                    event_from,
                    max_messages,
                    max_events,
                    max_bytes,
                    max_wait_ms,
                    Some(self.epoch),
                )
                .await?;
            self.check_history()?;
            Ok(records)
        })
    }
    fn export_queue_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<QueueStateCheckpoint, BrokerError>> {
        Box::pin(async move {
            self.check_history()?;
            let checkpoint = self
                .peer
                .export_queue_state_checkpoint(topic, partition, group)
                .await?;
            self.check_history()?;
            Ok(checkpoint)
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
    ) -> BoxFuture<'a, Result<FollowerStreamExit, BrokerError>> {
        let apply: Arc<dyn BrokerQueuePushApply> = if let Some(history) = &self.history {
            Arc::new(HistoryQueuePushApply {
                broker: self.broker.clone(),
                history: history.clone(),
                inner: apply,
            })
        } else {
            apply
        };
        self.peer.push_queue_replication_fenced(
            topic,
            partition,
            group,
            from,
            credit_bytes,
            tunables,
            buffer_batches,
            apply,
            shutdown,
            Some(self.epoch),
        )
    }
}

impl AssignmentReplicationPeer<'_> {
    fn check_history(&self) -> Result<(), BrokerError> {
        if let Some(history) = &self.history {
            self.broker.authorize_history_replication(history, false)?;
        }
        Ok(())
    }
}

struct HistoryQueuePushApply {
    broker: Arc<Broker<StromaEngine>>,
    history: crate::history_replication::HistoryReplicationSession,
    inner: Arc<dyn BrokerQueuePushApply>,
}
impl BrokerQueuePushApply for HistoryQueuePushApply {
    fn apply_open_queue_push<'a>(
        &'a self,
        frames: BoxStream<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>>,
    ) -> BoxFuture<'a, Result<QueuePushApply, BrokerError>> {
        Box::pin(async move {
            self.broker.authorize_history_replication(&self.history, false)?;
            let checked = frames.map(|frame| {
                self.broker.authorize_history_replication(&self.history, false)?;
                frame
            }).boxed();
            let outcome = self.inner.apply_open_queue_push(checked).await?;
            self.broker.authorize_history_replication(&self.history, false)?;
            Ok(outcome)
        })
    }
    fn apply_queue_push<'a>(
        &'a self,
        records: OwnerReplicationRead<StromaEvent>,
    ) -> BoxFuture<'a, Result<QueuePushApply, BrokerError>> {
        Box::pin(async move {
            self.broker
                .authorize_history_replication(&self.history, false)?;
            let outcome = self.inner.apply_queue_push(records).await?;
            self.broker
                .authorize_history_replication(&self.history, false)?;
            Ok(outcome)
        })
    }
    fn queue_durable_next(&self) -> BoxFuture<'_, Option<Offset>> {
        self.inner.queue_durable_next()
    }
    fn queue_applies_without_sync(&self) -> bool {
        self.inner.queue_applies_without_sync()
    }
}

pub trait BrokerOwnerReplicationPeerResolver: Send + Sync {
    /// Resolve the owner peer for `assignment`. `kind` selects the read transport:
    /// a stream peer reads via the stream-mode pull op, so its peer is built (and
    /// cached) separately from a queue peer to the same owner.
    fn resolve_owner_peer<'a>(
        &'a self,
        assignment: &'a PartitionAssignment,
        kind: ReplicationResourceKind,
    ) -> BoxFuture<'a, Result<Option<Arc<dyn BrokerOwnerReplicationPeer>>, BrokerError>>;
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct BrokerReplicationCheckpointRequired {
    pub epoch: u64,
    pub requested_offset: Offset,
    pub head_offset: Offset,
    pub next_offset: Offset,
}

/// Result of applying one owner read of the single queue log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerQueueFollowerApply {
    /// `None` when the owner returned no records, so nothing was appended.
    Applied(Option<ReplicatedAppendOutcome>),
    CheckpointRequired(BrokerReplicationCheckpointRequired),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerStreamFollowerApply {
    Applied(ReplicatedStreamApplyOutcome),
    CheckpointRequired {
        messages: Option<BrokerReplicationCheckpointRequired>,
        events: Option<BrokerReplicationCheckpointRequired>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueCatchUpOptions {
    pub from: Offset,
    pub max_records_per_read: usize,
    pub max_bytes_per_read: usize,
    pub max_iterations: usize,
    pub max_wait_ms: u64,
}

impl Default for QueueCatchUpOptions {
    fn default() -> Self {
        Self {
            from: 0,
            max_records_per_read: 2048,
            max_bytes_per_read: 8 * 1024 * 1024,
            max_iterations: 1024,
            max_wait_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct QueueCatchUpProgress {
    pub iterations: usize,
    pub applied_records: usize,
    pub next_offset: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerQueueCatchUp {
    CaughtUp(QueueCatchUpProgress),
    CheckpointRequired {
        progress: QueueCatchUpProgress,
        required: BrokerReplicationCheckpointRequired,
    },
    IterationLimit {
        progress: QueueCatchUpProgress,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamCatchUpOptions {
    pub message_from: Offset,
    pub event_from: Offset,
    pub max_messages_per_read: usize,
    pub max_events_per_read: usize,
    pub max_bytes_per_read: usize,
    pub max_iterations: usize,
    pub max_wait_ms: u64,
}

impl Default for StreamCatchUpOptions {
    fn default() -> Self {
        Self {
            message_from: 0,
            event_from: 0,
            max_messages_per_read: 2048,
            max_events_per_read: 2048,
            max_bytes_per_read: 8 * 1024 * 1024,
            max_iterations: 1024,
            max_wait_ms: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct StreamCatchUpProgress {
    pub iterations: usize,
    pub applied_message_records: usize,
    pub applied_event_records: usize,
    pub message_next_offset: Offset,
    pub event_next_offset: Offset,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BrokerStreamCatchUp {
    CaughtUp(StreamCatchUpProgress),
    CheckpointRequired {
        progress: StreamCatchUpProgress,
        messages: Option<BrokerReplicationCheckpointRequired>,
        events: Option<BrokerReplicationCheckpointRequired>,
    },
    IterationLimit {
        progress: StreamCatchUpProgress,
    },
}

/// Outcome of one follower worker tick, typed by the resource it copies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowerCatchUp {
    Queue(BrokerQueueCatchUp),
    Stream(BrokerStreamCatchUp),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowerReplicationWorkerLoopExit {
    Cancelled { ticks: usize },
    WorkerStopped { ticks: usize },
    OwnerChanged { ticks: usize },
}

/// Which durable resource a follower-replication path is catching up: a queue
/// partition or a Plexus stream partition. Queues carry payloads in their event
/// log, while streams keep record and cursor-event logs. The worker loop and
/// runtime map are shared. This kind selects the catch-up path, the engine
/// apply call and the owner peer's read operation. A stream is keyed as a
/// `QueueIdentity` with `group: None` at the worker layer (a topic is either a
/// queue or a stream, so the map never collides).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicationResourceKind {
    Queue,
    Stream,
}

/// Outcome of [`Broker::apply_queue_push_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueuePushApply {
    /// Applied. `next` is the advanced queue log cursor, written to the OS.
    /// `durable_next` is how far the log is synced, which trails `next` only
    /// on a buffered queue.
    Applied { next: Offset, durable_next: Offset },
    /// A checkpoint is required (offset gap / epoch fence). The push caller
    /// falls back to the pull + checkpoint path, then pushes again.
    CheckpointRequired,
}

/// Why a follower queue push ended (returned by
/// [`BrokerOwnerReplicationPeer::push_queue_replication`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowerStreamExit {
    /// The owner closed the stream cleanly (or it was shut down).
    Closed,
    /// A checkpoint is required; the caller falls back to pull+checkpoint, then
    /// re-streams from the post-checkpoint cursor.
    CheckpointRequired,
    /// The connection failed or the owner errored / is no longer owner; the
    /// caller should re-resolve the owner and retry.
    Error(String),
}

/// Durable apply for one pushed queue batch, provided by the follower worker
/// to a pushing peer. Lets the protocol transport apply through the broker
/// without the broker depending on the protocol crate (mirrors the resolver
/// injection).
pub trait BrokerQueuePushApply: Send + Sync {
    fn apply_open_queue_push<'a>(
        &'a self,
        _frames: BoxStream<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>>,
    ) -> BoxFuture<'a, Result<QueuePushApply, BrokerError>> {
        Box::pin(async { Err(BrokerError::InvalidArgument("open follower batches unsupported".into())) })
    }
    fn apply_queue_push<'a>(
        &'a self,
        records: OwnerReplicationRead<StromaEvent>,
    ) -> BoxFuture<'a, Result<QueuePushApply, BrokerError>>;
    /// Current durable end of the follower queue log when it can trail what
    /// was applied, as on a buffered queue. `None` otherwise. No default, so
    /// a wrapping adapter cannot silently drop it.
    fn queue_durable_next(&self) -> BoxFuture<'_, Option<Offset>>;
    /// Whether an apply returns once written, without waiting for a sync.
    fn queue_applies_without_sync(&self) -> bool;
}

// ---------------- Follower replication worker types ----------------

/// Push-apply adapter the follower worker hands to a pushing peer: applies
/// each batch through the broker and advances the worker's reported cursor so
/// the push-to-pull fallback resumes from the right place and the reported
/// cursor stays current during a long-lived push.
pub(crate) struct WorkerQueuePushApply {
    pub(crate) broker: Arc<Broker<StromaEngine>>,
    pub(crate) topic: String,
    pub(crate) partition: Partition,
    pub(crate) group: Option<String>,
    pub(crate) worker: Arc<AsyncMutex<FollowerReplicationWorkerState>>,
}

impl WorkerQueuePushApply {
    async fn advance_worker(&self, next: Offset) {
        let mut state = self.worker.lock().await;
        state.cursor = FollowerCursor::Queue { next_offset: next };
        state.status = FollowerReplicationWorkerStatus::CaughtUp;
    }
}

impl BrokerQueuePushApply for WorkerQueuePushApply {
    fn apply_open_queue_push<'a>(
        &'a self,
        frames: BoxStream<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>>,
    ) -> BoxFuture<'a, Result<QueuePushApply, BrokerError>> {
        Box::pin(async move {
            let written = self.broker.engine.queue_is_buffered(
                &self.topic,
                self.partition.id(),
                self.group.as_deref(),
            );
            // Frames without records advance the cursor without an append, so
            // only frames that carry records produce an apply outcome.
            let mut progress: Vec<(OwnerBatchProgress, bool)> = Vec::new();
            let frames = frames
                .filter_map(|frame| {
                    let convert = || -> Result<Option<ReplicatedEventBatch>, BrokerError> {
                        let read = frame?;
                        let p = owner_read_batch_progress("queue", &read)?.ok_or_else(|| {
                            BrokerError::InvalidArgument("open batch requires queue history".into())
                        })?;
                        let batch = queue_apply_batch(read, written);
                        progress.push((p, batch.is_some()));
                        Ok(batch)
                    };
                    let item = match convert() {
                        Ok(Some(batch)) => Some(Ok(batch)),
                        Ok(None) => None,
                        Err(e) => Some(Err(stroma_core::StromaError::Io(e.to_string()))),
                    };
                    std::future::ready(item)
                })
                .boxed();
            let outcomes = self
                .broker
                .engine
                .apply_open_replicated_queue_batch(
                    &self.topic,
                    self.partition.id(),
                    self.group.as_deref(),
                    frames,
                )
                .await?;
            if outcomes.len() != progress.iter().filter(|(_, applied)| *applied).count() {
                return Err(BrokerError::InvalidArgument("open batch completion count mismatch".into()));
            }
            let mut outcomes = outcomes.into_iter();
            let mut final_next = None;
            for (p, applied) in progress {
                let outcome = if applied { outcomes.next() } else { None };
                match replicated_append_progress_after_apply("queue", &p, outcome.as_ref())? {
                    ReplicatedAppendProgress::Advanced(next) => final_next = Some(next),
                    ReplicatedAppendProgress::CheckpointRequired(_) => {
                        return Ok(QueuePushApply::CheckpointRequired);
                    }
                }
            }
            let next = final_next.ok_or_else(|| BrokerError::InvalidArgument("empty open batch".into()))?;
            self.advance_worker(next).await;
            let durable_next = self
                .broker
                .queue_follower_durable_next(&self.topic, self.partition, self.group.as_deref(), next, written)
                .await?;
            Ok(QueuePushApply::Applied { next, durable_next })
        })
    }
    fn apply_queue_push<'a>(
        &'a self,
        records: OwnerReplicationRead<StromaEvent>,
    ) -> BoxFuture<'a, Result<QueuePushApply, BrokerError>> {
        Box::pin(async move {
            let outcome = self
                .broker
                .apply_queue_push_batch(&self.topic, self.partition, self.group.as_deref(), records)
                .await?;
            if let QueuePushApply::Applied { next, .. } = outcome {
                self.advance_worker(next).await;
            }
            Ok(outcome)
        })
    }
    fn queue_applies_without_sync(&self) -> bool {
        self.broker
            .engine
            .queue_is_buffered(&self.topic, self.partition.id(), self.group.as_deref())
    }
    fn queue_durable_next(&self) -> BoxFuture<'_, Option<Offset>> {
        Box::pin(async move {
            let group = self.group.as_deref();
            if !self
                .broker
                .engine
                .queue_is_buffered(&self.topic, self.partition.id(), group)
            {
                return None;
            }
            self.broker
                .engine
                .queue_log_sync_status(&self.topic, self.partition.id(), group)
                .await
                .ok()
                .map(|status| status.event_log.durable_next)
        })
    }
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
    /// Use credit-based streaming for catch-up instead of the pull tick (pull is
    /// the fallback on checkpoint/error). Read from runtime settings when
    /// `follow_runtime_settings` is set.
    pub stream_enabled: bool,
    /// Linger (microseconds) the streaming applier spends gathering more
    /// contiguous frames before applying them as one fsynced batch. 0 = drain-only.
    pub stream_apply_linger_us: u64,
    /// Byte cap on a single coalesced streaming-apply. Pairs with
    /// `stream_apply_linger_us`.
    pub stream_apply_max_merge_bytes: u64,
    /// In-flight batch buffer depth (credit window) for the streaming follower
    /// (setup-time: applies on the next stream).
    pub stream_buffer_batches: usize,
}

impl Default for FollowerReplicationWorkerConfig {
    fn default() -> Self {
        Self {
            max_messages_per_read: 2048,
            max_events_per_read: 2048,
            max_bytes_per_read: 8 * 1024 * 1024,
            max_iterations_per_tick: 8,
            allow_checkpoint_install: false,
            caught_up_poll_ms: 1000,
            retry_poll_ms: 100,
            checkpoint_retry_poll_ms: 5000,
            follow_runtime_settings: false,
            stream_enabled: false,
            stream_apply_linger_us: 2_000,
            stream_apply_max_merge_bytes: 16 * 1024 * 1024,
            stream_buffer_batches: FOLLOWER_STREAM_BUFFER_BATCHES,
        }
    }
}

/// The streaming-applier knobs that are runtime-tunable. Read LIVE at the top of
/// each apply iteration (via [`StreamApplyTunablesFn`]) so a runtime-settings
/// change takes effect on the next apply with no stream restart - the
/// live-settings-propagation principle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamApplyTunables {
    /// Idle keepalive (ms); 0 disables the keepalive tick.
    pub keepalive_ms: u64,
    /// Linger (us) gathering more contiguous frames before applying; 0 = drain-only.
    pub apply_linger_us: u64,
    /// Byte cap on one coalesced apply.
    pub max_merge_bytes: u64,
    /// Experimental. One collection budget from the first frame that later
    /// arrivals do not end early.
    pub fixed_deadline: bool,
    /// Experimental open batch mode, see `replication.stream_apply_open_batch`.
    pub open_batch: u8,
}

/// A live reader of [`StreamApplyTunables`]: the applier calls it once per apply
/// iteration. The broker backs it with its live runtime-settings snapshot so
/// changes propagate mid-stream; tests back it with fixed/mutable values.
pub type StreamApplyTunablesFn = Arc<dyn Fn() -> StreamApplyTunables + Send + Sync>;

/// Build a live tunables reader backed by the broker's current config snapshot.
/// Each call reads the latest `cfg`, so a runtime-settings update propagates to
/// the streaming applier on its next apply iteration without a stream restart.
pub(crate) fn stream_apply_tunables_from(
    cfg: Arc<arc_swap::ArcSwap<crate::broker::BrokerConfig>>,
) -> StreamApplyTunablesFn {
    Arc::new(move || {
        let c = cfg.load();
        StreamApplyTunables {
            keepalive_ms: c.replication_caught_up_poll_ms,
            apply_linger_us: c.replication_stream_apply_linger_us,
            max_merge_bytes: c.replication_stream_apply_max_merge_bytes,
            fixed_deadline: c.replication_stream_apply_fixed_deadline,
            open_batch: c.replication_stream_apply_open_batch,
        }
    })
}

impl FollowerReplicationWorkerConfig {
    pub fn queue_catch_up_options(self, from: Offset, max_wait_ms: u64) -> QueueCatchUpOptions {
        QueueCatchUpOptions {
            from,
            max_records_per_read: self.max_events_per_read,
            max_bytes_per_read: self.max_bytes_per_read,
            max_iterations: self.max_iterations_per_tick,
            max_wait_ms,
        }
    }

    pub fn stream_catch_up_options(
        self,
        message_from: Offset,
        event_from: Offset,
        max_wait_ms: u64,
    ) -> StreamCatchUpOptions {
        StreamCatchUpOptions {
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
    CheckpointRequired { required: CheckpointRequirement },
}

/// What the owner no longer holds, by resource kind. A queue needs one log
/// position. A stream reports each of its two logs.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckpointRequirement {
    Queue {
        log: BrokerReplicationCheckpointRequired,
    },
    Stream {
        messages: Option<BrokerReplicationCheckpointRequired>,
        events: Option<BrokerReplicationCheckpointRequired>,
    },
}

/// Where a follower worker resumes. A queue follows one position in its
/// single log. A stream follows its record log and cursor-event log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum FollowerCursor {
    Queue {
        next_offset: Offset,
    },
    Stream {
        message_next_offset: Offset,
        event_next_offset: Offset,
    },
}

impl FollowerCursor {
    /// Start of the log or logs for a resource of `kind`.
    pub fn zero(kind: ReplicationResourceKind) -> Self {
        match kind {
            ReplicationResourceKind::Queue => Self::Queue { next_offset: 0 },
            ReplicationResourceKind::Stream => Self::Stream {
                message_next_offset: 0,
                event_next_offset: 0,
            },
        }
    }

    pub fn kind(&self) -> ReplicationResourceKind {
        match self {
            Self::Queue { .. } => ReplicationResourceKind::Queue,
            Self::Stream { .. } => ReplicationResourceKind::Stream,
        }
    }
}

/// Last catch-up pass of a follower worker, typed like its cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CatchUpProgress {
    Queue(QueueCatchUpProgress),
    Stream(StreamCatchUpProgress),
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct FollowerReplicationWorkerState {
    #[serde(flatten)]
    pub cursor: FollowerCursor,
    pub status: FollowerReplicationWorkerStatus,
    pub last_progress: Option<CatchUpProgress>,
    pub next_delay_ms: u64,
}

impl FollowerReplicationWorkerState {
    pub fn new(cursor: FollowerCursor) -> Self {
        Self {
            cursor,
            status: FollowerReplicationWorkerStatus::Idle,
            last_progress: None,
            next_delay_ms: 0,
        }
    }

    fn max_wait_ms(&self, cfg: FollowerReplicationWorkerConfig) -> u64 {
        match self.status {
            FollowerReplicationWorkerStatus::CaughtUp => cfg.caught_up_poll_ms,
            _ => 0,
        }
    }

    /// Options for the next queue pass, or an error when this worker follows
    /// a stream.
    pub fn queue_catch_up_options(
        &self,
        cfg: FollowerReplicationWorkerConfig,
    ) -> Result<QueueCatchUpOptions, BrokerError> {
        match self.cursor {
            FollowerCursor::Queue { next_offset } => {
                Ok(cfg.queue_catch_up_options(next_offset, self.max_wait_ms(cfg)))
            }
            FollowerCursor::Stream { .. } => Err(cursor_kind_mismatch()),
        }
    }

    /// Options for the next stream pass, or an error when this worker follows
    /// a queue.
    pub fn stream_catch_up_options(
        &self,
        cfg: FollowerReplicationWorkerConfig,
    ) -> Result<StreamCatchUpOptions, BrokerError> {
        match self.cursor {
            FollowerCursor::Stream {
                message_next_offset,
                event_next_offset,
            } => Ok(cfg.stream_catch_up_options(
                message_next_offset,
                event_next_offset,
                self.max_wait_ms(cfg),
            )),
            FollowerCursor::Queue { .. } => Err(cursor_kind_mismatch()),
        }
    }

    pub fn should_install_checkpoint(&self, cfg: FollowerReplicationWorkerConfig) -> bool {
        cfg.allow_checkpoint_install
            && matches!(
                self.status,
                FollowerReplicationWorkerStatus::CheckpointRequired { .. }
            )
    }

    pub fn record_catch_up(&mut self, cfg: FollowerReplicationWorkerConfig, outcome: &FollowerCatchUp) {
        match outcome {
            FollowerCatchUp::Queue(outcome) => self.record_queue_catch_up(cfg, outcome),
            FollowerCatchUp::Stream(outcome) => self.record_stream_catch_up(cfg, outcome),
        }
    }

    pub fn record_queue_catch_up(
        &mut self,
        cfg: FollowerReplicationWorkerConfig,
        outcome: &BrokerQueueCatchUp,
    ) {
        let (progress, status, delay) = match outcome {
            BrokerQueueCatchUp::CaughtUp(progress) => {
                (progress, FollowerReplicationWorkerStatus::CaughtUp, 0)
            }
            BrokerQueueCatchUp::IterationLimit { progress } => {
                (progress, FollowerReplicationWorkerStatus::PendingRetry, cfg.retry_poll_ms)
            }
            BrokerQueueCatchUp::CheckpointRequired { progress, required } => (
                progress,
                FollowerReplicationWorkerStatus::CheckpointRequired {
                    required: CheckpointRequirement::Queue { log: required.clone() },
                },
                cfg.checkpoint_retry_poll_ms,
            ),
        };
        self.cursor = FollowerCursor::Queue {
            next_offset: progress.next_offset,
        };
        self.last_progress = Some(CatchUpProgress::Queue(*progress));
        self.status = status;
        self.next_delay_ms = delay;
    }

    pub fn record_stream_catch_up(
        &mut self,
        cfg: FollowerReplicationWorkerConfig,
        outcome: &BrokerStreamCatchUp,
    ) {
        match outcome {
            BrokerStreamCatchUp::CaughtUp(progress) => {
                self.apply_stream_progress(progress);
                self.status = FollowerReplicationWorkerStatus::CaughtUp;
                self.next_delay_ms = 0;
            }
            BrokerStreamCatchUp::IterationLimit { progress } => {
                self.apply_stream_progress(progress);
                self.status = FollowerReplicationWorkerStatus::PendingRetry;
                self.next_delay_ms = cfg.retry_poll_ms;
            }
            BrokerStreamCatchUp::CheckpointRequired {
                progress,
                messages,
                events,
            } => {
                self.apply_stream_progress(progress);
                self.status = FollowerReplicationWorkerStatus::CheckpointRequired {
                    required: CheckpointRequirement::Stream {
                        messages: messages.clone(),
                        events: events.clone(),
                    },
                };
                self.next_delay_ms = cfg.checkpoint_retry_poll_ms;
            }
        }
    }

    fn apply_stream_progress(&mut self, progress: &StreamCatchUpProgress) {
        self.cursor = FollowerCursor::Stream {
            message_next_offset: progress.message_next_offset,
            event_next_offset: progress.event_next_offset,
        };
        self.last_progress = Some(CatchUpProgress::Stream(*progress));
    }
}

fn cursor_kind_mismatch() -> BrokerError {
    BrokerError::InvalidArgument("follower worker cursor belongs to another resource kind".into())
}

// ---------------- Follower replication observability ----------------

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
    /// Last durable position the follower reported, `None` before its first
    /// report.
    pub progress: Option<DurableProgress>,
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

// ---------------- Replication timing metrics ----------------

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplicationTimingSnapshot {
    pub replication_wakes: u64,
    pub follower_progress_reports: u64,
    pub replica_confirm_wait: ReplicationTimingPhaseSnapshot,
    pub owner_read: ReplicationTimingPhaseSnapshot,
    pub follower_owner_read: ReplicationTimingPhaseSnapshot,
    pub follower_apply: ReplicationTimingPhaseSnapshot,
    pub follower_tick: ReplicationTimingPhaseSnapshot,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplicationTimingPhaseSnapshot {
    pub count: u64,
    pub total_ms: f64,
    pub avg_ms: f64,
    pub max_ms: f64,
}

#[derive(Debug, Default)]
pub(crate) struct ReplicationTimingMetrics {
    pub(crate) replication_wakes: AtomicU64,
    pub(crate) follower_progress_reports: AtomicU64,
    pub(crate) replica_confirm_wait: ReplicationTimingPhase,
    pub(crate) owner_read: ReplicationTimingPhase,
    pub(crate) follower_owner_read: ReplicationTimingPhase,
    pub(crate) follower_apply: ReplicationTimingPhase,
    pub(crate) follower_tick: ReplicationTimingPhase,
}

impl ReplicationTimingMetrics {
    pub(crate) fn record_replication_wake(&self) {
        self.replication_wakes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_follower_progress_report(&self) {
        self.follower_progress_reports
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> ReplicationTimingSnapshot {
        ReplicationTimingSnapshot {
            replication_wakes: self.replication_wakes.load(Ordering::Relaxed),
            follower_progress_reports: self.follower_progress_reports.load(Ordering::Relaxed),
            replica_confirm_wait: self.replica_confirm_wait.snapshot(),
            owner_read: self.owner_read.snapshot(),
            follower_owner_read: self.follower_owner_read.snapshot(),
            follower_apply: self.follower_apply.snapshot(),
            follower_tick: self.follower_tick.snapshot(),
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct ReplicationTimingPhase {
    count: AtomicU64,
    total_ns: AtomicU64,
    max_ns: AtomicU64,
}

impl ReplicationTimingPhase {
    fn observe(&self, duration: Duration) {
        let ns = duration.as_nanos().min(u64::MAX as u128) as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.total_ns.fetch_add(ns, Ordering::Relaxed);

        let mut current = self.max_ns.load(Ordering::Relaxed);
        while ns > current {
            match self.max_ns.compare_exchange_weak(
                current,
                ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    pub(crate) fn timer(&self) -> ReplicationTimingGuard<'_> {
        ReplicationTimingGuard {
            phase: self,
            started_at: Instant::now(),
        }
    }

    fn snapshot(&self) -> ReplicationTimingPhaseSnapshot {
        let count = self.count.load(Ordering::Relaxed);
        let total_ns = self.total_ns.load(Ordering::Relaxed);
        let max_ns = self.max_ns.load(Ordering::Relaxed);
        let total_ms = ns_to_ms(total_ns);
        ReplicationTimingPhaseSnapshot {
            count,
            total_ms,
            avg_ms: if count == 0 {
                0.0
            } else {
                total_ms / count as f64
            },
            max_ms: ns_to_ms(max_ns),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ReplicationTimingGuard<'a> {
    phase: &'a ReplicationTimingPhase,
    started_at: Instant,
}

impl Drop for ReplicationTimingGuard<'_> {
    fn drop(&mut self) {
        self.phase.observe(self.started_at.elapsed());
    }
}

fn ns_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

// ---------------- Follower replication worker runtime ----------------

#[derive(Debug)]
pub(crate) struct FollowerReplicationWorkerRuntime {
    pub(crate) state: Arc<AsyncMutex<FollowerReplicationWorkerState>>,
    pub(crate) shutdown: CancellationToken,
    pub(crate) started: AtomicBool,
    stopping: AtomicBool,
    active_ticks: AtomicUsize,
    idle: Notify,
}

impl FollowerReplicationWorkerRuntime {
    pub(crate) fn new(cursor: FollowerCursor) -> Self {
        Self {
            state: Arc::new(AsyncMutex::new(FollowerReplicationWorkerState::new(cursor))),
            shutdown: CancellationToken::new(),
            started: AtomicBool::new(false),
            stopping: AtomicBool::new(false),
            active_ticks: AtomicUsize::new(0),
            idle: Notify::new(),
        }
    }

    pub(crate) fn begin_tick(self: &Arc<Self>) -> Option<FollowerReplicationTickGuard> {
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

    pub(crate) async fn stop_and_wait(&self) {
        self.stopping.store(true, Ordering::Release);
        self.shutdown.cancel();
        loop {
            // Register before checking: the final tick may finish between the
            // predicate and await, with no later tick left to send another wake.
            let idle = self.idle.notified();
            if self.active_ticks.load(Ordering::Acquire) == 0 {
                break;
            }
            idle.await;
        }
    }
}

#[derive(Debug)]
pub(crate) struct FollowerReplicationTickGuard {
    runtime: Arc<FollowerReplicationWorkerRuntime>,
}

impl Drop for FollowerReplicationTickGuard {
    fn drop(&mut self) {
        self.runtime.finish_tick();
    }
}

#[cfg(test)]
mod follower_runtime_tests {
    use super::*;

    #[tokio::test]
    async fn stop_waits_for_last_tick_and_rejects_new_ticks() {
        let runtime = Arc::new(FollowerReplicationWorkerRuntime::new(FollowerCursor::Stream {
            message_next_offset: 3,
            event_next_offset: 7,
        }));
        let tick = runtime.begin_tick().unwrap();
        let stop = runtime.stop_and_wait();
        tokio::pin!(stop);
        assert!(futures::poll!(stop.as_mut()).is_pending());
        assert!(runtime.begin_tick().is_none());
        drop(tick);
        tokio::time::timeout(std::time::Duration::from_secs(1), stop)
            .await
            .expect("the final tick must release the drain without another notification");
    }
}

// ---------------- Replication catch-up helpers ----------------

pub(crate) async fn wait_for_follower_worker_delay(
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

pub(crate) fn checkpoint_required<T>(
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

pub(crate) fn cap_owner_replication_records(
    records: BrokerOwnerStreamRecords,
    max_bytes: usize,
) -> BrokerOwnerStreamRecords {
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

    BrokerOwnerStreamRecords {
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
    batch.next_offset = returned_frontier;

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
            // Head-of-line stop, not a skip: an event referencing a non-durable
            // offset (e.g. a dangling EnqueueMany whose payload never fsynced) halts
            // the stream here. This is safe and self-healing. Durability is a
            // contiguous prefix, so nothing DURABLE can sit behind a non-durable
            // offset, and no confirmed data is ever blocked. A CancelEnqueue that
            // follows a dangling enqueue is not reached, but it is a no-op on a
            // follower that never received the enqueue. If the offset later becomes
            // durable (a transient fsync failure healed by a later flush), the
            // frontier advances and the next read resumes from this cursor. Skipping
            // instead would permanently drop an enqueue that could become durable.
            break;
        }
        kept.push((offset, event));
    }
    batch.next_offset = kept
        .last()
        .map(|(offset, _)| offset.saturating_add(1))
        .unwrap_or(batch.requested_offset);
    batch.records = kept;
    OwnerReplicationRead::Batch(batch)
}

fn stroma_event_available_for_replicated_messages(
    event: &StromaEvent,
    message_frontier: Offset,
    message_tail_fully_returned: bool,
) -> bool {
    match event {
        StromaEvent::PayloadEnqueue { .. } => true,
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
        // A cursor commit carries no message reference (it is a soft consumer
        // bookmark), and a stream truncation is a delete directive, so neither has
        // to wait on the message frontier. A CancelEnqueue is likewise an
        // annihilation directive: it must not be frontier-gated (its offset may
        // never become durable) and is a harmless no-op on a follower that never
        // received the enqueue it cancels.
        StromaEvent::ActivateDelayed { .. }
        | StromaEvent::Declare(_)
        | StromaEvent::ResetQueue { .. }
        | StromaEvent::CursorCommit { .. }
        | StromaEvent::CursorCommitBatch { .. }
        | StromaEvent::CancelEnqueueMany { .. }
        | StromaEvent::StreamTruncate { .. } => true,
        StromaEvent::Snapshot { .. } => message_tail_fully_returned,
    }
}

fn owner_read_empty<T>(read: &OwnerReplicationRead<T>) -> bool {
    matches!(read, OwnerReplicationRead::Batch(batch) if batch.records.is_empty())
}

pub(crate) fn owner_stream_records_empty(records: &BrokerOwnerStreamRecords) -> bool {
    owner_read_empty(&records.messages) && owner_read_empty(&records.events)
}

/// Follower batch for the records of one owner queue read, or `None` when
/// there is nothing to append. `written` appends without waiting for fsync,
/// for a buffered queue on a path that reports written and durable progress
/// separately. Otherwise followers apply durably, so the position they report
/// is honest durable progress for the confirm policies.
fn queue_apply_batch(
    read: OwnerReplicationRead<StromaEvent>,
    written: bool,
) -> Option<ReplicatedEventBatch> {
    match read {
        OwnerReplicationRead::Batch(batch) if !batch.records.is_empty() => {
            Some(ReplicatedEventBatch {
                epoch: batch.epoch,
                first_offset: batch.records[0].0,
                events: batch.records.into_iter().map(|(_, event)| event).collect(),
                durability: Some(if written {
                    KDurability::AfterFlush
                } else {
                    KDurability::AfterFsync
                }),
            })
        }
        _ => None,
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct OwnerBatchProgress {
    epoch: u64,
    requested_offset: Offset,
    first_offset: Option<Offset>,
    pub(crate) record_count: usize,
    next_request_offset: Offset,
    owner_next_offset: Offset,
}

pub(crate) fn owner_read_batch_progress<T>(
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
pub(crate) enum ReplicatedAppendProgress {
    Advanced(Offset),
    CheckpointRequired(BrokerReplicationCheckpointRequired),
}

pub(crate) fn replicated_append_progress_after_apply(
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

// ---------------- Publish-confirm replication gate + progress ----------------

/// Cheap shared handle for publish-confirm replication waits inside
/// per-queue confirm loops.
#[derive(Clone)]
pub struct ReplicationConfirmGate {
    pub(crate) progress: Arc<DashMap<QueueKey, Arc<ReplicationProgressCell>>>,
    pub(crate) assignments: Arc<DashMap<QueueKey, PartitionAssignment>>,
    pub(crate) cfg: Arc<ArcSwap<BrokerConfig>>,
    pub(crate) timing: Arc<ReplicationTimingMetrics>,
}

/// Durable follower position, reported over a progress session whose kind is
/// fixed when the session opens. A queue names one position in its single
/// log. A stream names its record log and its cursor-event log separately.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DurableProgress {
    Queue { next: Offset },
    Stream { message_next: Offset, event_next: Offset },
}

impl DurableProgress {
    /// Nothing proven yet for a session of `kind`.
    pub fn zero(kind: ReplicationResourceKind) -> Self {
        match kind {
            ReplicationResourceKind::Queue => Self::Queue { next: 0 },
            ReplicationResourceKind::Stream => Self::Stream {
                message_next: 0,
                event_next: 0,
            },
        }
    }

    pub fn kind(&self) -> ReplicationResourceKind {
        match self {
            Self::Queue { .. } => ReplicationResourceKind::Queue,
            Self::Stream { .. } => ReplicationResourceKind::Stream,
        }
    }

    /// Exclusive position that releases confirms: the queue log, or stream
    /// records. Cursor-commit events never release a stream record.
    pub fn confirm_next(&self) -> Offset {
        match *self {
            Self::Queue { next } => next,
            Self::Stream { message_next, .. } => message_next,
        }
    }

    /// Queue log position, or `None` for stream progress.
    pub fn queue_next(&self) -> Option<Offset> {
        match *self {
            Self::Queue { next } => Some(next),
            Self::Stream { .. } => None,
        }
    }
}

/// One follower's last-reported durable progress, with the report time used to
/// decide in-sync membership (a follower that stopped reporting falls out).
#[derive(Debug, Clone, Copy)]
pub(crate) struct FollowerProgress {
    pub(crate) progress: DurableProgress,
    /// Exclusive end of the records the follower has written to the OS in
    /// its queue log, synced or not. `None` when it matches the durable
    /// position, as on every durable queue. Only buffered queues confirm and
    /// deliver on it. Retention and checkpoints keep using `progress`.
    pub(crate) written: Option<Offset>,
    pub(crate) last_report: std::time::Instant,
    session: u64,
}

impl FollowerProgress {
    /// Queue log position that confirms and delivers a record: the written
    /// position for a buffered queue, the durable one otherwise.
    fn queue_reached(&self, buffered: bool) -> Option<Offset> {
        let durable = self.progress.queue_next()?;
        Some(match (buffered, self.written) {
            (true, Some(written)) => written.max(durable),
            _ => durable,
        })
    }
}

/// Completed publication boundaries in the single queue log. A boundary is
/// the exclusive end of one publish batch. Visibility only ever advances to a
/// boundary, so a follower that holds part of a batch exposes none of it.
/// Once quorum covers a prefix, its proof folds into one frontier, so offline
/// followers do not retain per-batch history.
#[derive(Debug, Default)]
pub(crate) struct QueueDependencies {
    pub(crate) initialized: bool,
    committed: Offset,
    boundaries: std::collections::VecDeque<Offset>,
}

impl QueueDependencies {
    pub(crate) fn record(&mut self, commit: stroma_core::QueuePublishCommit) {
        let next = commit.next;
        if next > self.committed {
            let at = self.boundaries.partition_point(|boundary| *boundary < next);
            if self.boundaries.get(at) != Some(&next) {
                self.boundaries.insert(at, next);
            }
        }
        // Keep bounded history during an arbitrarily long replica outage.
        // The retained later boundary conservatively covers the discarded prefix.
        if self.boundaries.len() > 4096 {
            self.boundaries.drain(..2048);
        }
    }

    /// Last completed boundary this follower covers. Never the follower
    /// position itself, which may sit inside a publish batch.
    fn covered_next(&self, progress: &FollowerProgress, buffered: bool) -> Offset {
        let Some(next) = progress.queue_reached(buffered) else {
            return self.committed;
        };
        let covered_count = self.boundaries.partition_point(|boundary| *boundary <= next);
        let covered = covered_count
            .checked_sub(1)
            .map(|i| self.boundaries[i])
            .unwrap_or(self.committed);
        covered.min(next).max(self.committed)
    }

    /// `buffered` counts written follower copies instead of durable ones. A
    /// queue keeps one mode for life, so the folded frontier never mixes them.
    pub(crate) fn visibility(
        &mut self,
        assignment: &PartitionAssignment,
        followers: &std::collections::HashMap<String, FollowerProgress>,
        required_followers: usize,
        buffered: bool,
    ) -> Offset {
        let mut nexts: Vec<_> = assignment
            .followers
            .iter()
            .filter(|node| assignment.is_followed_by(node))
            .map(|node| {
                followers
                    .get(node)
                    .map(|p| self.covered_next(p, buffered))
                    .unwrap_or(self.committed)
            })
            .collect();
        nexts.sort_unstable_by(|a, b| b.cmp(a));
        let next = nexts
            .get(required_followers - 1)
            .copied()
            .unwrap_or(self.committed);
        self.committed = self.committed.max(next);
        while self
            .boundaries
            .front()
            .is_some_and(|boundary| *boundary <= self.committed)
        {
            self.boundaries.pop_front();
        }
        self.committed
    }
}

/// Follower durable progress for one queue, plus a waiter wake-up.
#[derive(Debug, Default)]
pub struct ReplicationProgressCell {
    /// follower node id -> last-reported durable progress
    pub(crate) followers: std::sync::Mutex<std::collections::HashMap<String, FollowerProgress>>,
    pub(crate) changed: Notify,
    pub(crate) dependencies: std::sync::Mutex<QueueDependencies>,
    next_session: AtomicU64,
}

/// A progress report belongs to one assignment and one ordered transport
/// session. A replacement stream or reconnect invalidates the previous proof.
#[derive(Debug)]
pub struct ReplicationProgressSession {
    key: QueueKey,
    assignment: PartitionAssignment,
    follower: String,
    /// Fixed at open. Reports of the other kind are dropped, so stream
    /// cursor events can never stand in for queue or record progress.
    kind: ReplicationResourceKind,
    cell: Arc<ReplicationProgressCell>,
    id: u64,
}

impl ReplicationProgressSession {
    pub fn kind(&self) -> ReplicationResourceKind {
        self.kind
    }
}

impl Drop for ReplicationProgressSession {
    fn drop(&mut self) {
        let mut followers = self.cell.lock_followers();
        if followers
            .get(&self.follower)
            .is_some_and(|p| p.session == self.id)
        {
            followers.remove(&self.follower);
            self.cell.changed.notify_waiters();
        }
    }
}

impl ReplicationProgressCell {
    /// Lock the progress map, recovering the guard if a previous holder panicked
    /// (a poisoned lock here just means slightly stale progress, never unsafe).
    pub(crate) fn lock_followers(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, FollowerProgress>> {
        self.followers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

async fn wait_for_replication_progress(
    changed: &Notify,
    deadline: tokio::time::Instant,
    mut satisfied: impl FnMut() -> bool,
) -> Result<(), tokio::time::error::Elapsed> {
    loop {
        // notify_waiters is observed from this future's creation, even before
        // polling. Create it before reading progress so a final report between
        // the check and the await cannot leave satisfied progress asleep.
        let notified = changed.notified();
        if satisfied() {
            return Ok(());
        }
        tokio::time::timeout_at(deadline, notified).await?;
    }
}

impl ReplicationConfirmGate {
    /// Wait until the stream's assignment durability policy is satisfied for
    /// the record at `offset` (the owner's own durable write already counts).
    /// Only stream progress over the record log counts. No cached assignment
    /// (standalone) or `local_durable` returns immediately. Fails with a
    /// descriptive error on timeout.
    pub(crate) async fn await_stream_confirm(
        &self,
        key: &QueueKey,
        offset: Offset,
    ) -> Result<(), BrokerError> {
        let assignment = self.assignments.get(key).map(|a| a.clone());
        self.await_dependency(key, offset, None, assignment).await
    }

    /// Wait for a queue record at `offset` when `dependency` is its
    /// publication, or for a stream record when it is `None`.
    pub(crate) async fn await_dependency(
        &self,
        key: &QueueKey,
        offset: Offset,
        dependency: Option<stroma_core::QueuePublishCommit>,
        expected_assignment: Option<PartitionAssignment>,
    ) -> Result<(), BrokerError> {
        let current = self.assignments.get(key);
        let preserves = match (current.as_deref(), expected_assignment.as_ref()) {
            (Some(now), Some(old)) => now.preserves_replication_contract(old),
            (None, None) => true,
            _ => false,
        };
        drop(current);
        if !preserves {
            return Err(BrokerError::Unknown(
                "assignment changed after publication admission".into(),
            ));
        }
        let Some(assignment) = expected_assignment else {
            return Ok(());
        };
        if dependency.is_some_and(|d| d.epoch != assignment.epoch) {
            return Err(BrokerError::Unknown(
                "publication belongs to a different replication epoch".into(),
            ));
        }
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
        let _replica_confirm_wait_timer = self.timing.replica_confirm_wait.timer();

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
                let current = self.assignments.get(key).ok_or_else(|| BrokerError::Unknown("assignment disappeared before ISR check".into()))?;
                if !current.preserves_replication_contract(&assignment) {return Err(BrokerError::Unknown("assignment changed before ISR check".into()));}
                let followers = cell.lock_followers();
                let now = std::time::Instant::now();
                let fresh = current
                    .followers
                    .iter()
                    .filter(|follower| {
                        if !current.is_followed_by(follower) {
                            return false;
                        }
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
        wait_for_replication_progress(&cell.changed, deadline, || {
            let Some(current) = self.assignments.get(key) else {return false;};
            if !current.preserves_replication_contract(&assignment) {return false;}
            let followers = cell.lock_followers();
            current
                .followers
                .iter()
                .filter(|follower| {
                    if !current.is_followed_by(follower) { return false; }
                    followers.get(*follower).is_some_and(|follower| {
                        match (dependency, follower.progress) {
                            // A queue record needs the whole publication that
                            // carried it in the follower's copy of the log,
                            // written for a buffered publication, synced
                            // otherwise.
                            (Some(d), DurableProgress::Queue { .. }) => {
                                follower
                                    .queue_reached(d.buffered)
                                    .is_some_and(|next| next > offset && next >= d.next)
                            }
                            // A stream record needs the record log only.
                            (None, DurableProgress::Stream { message_next, .. }) => {
                                message_next > offset
                            }
                            // Progress of the other kind proves nothing here.
                            _ => false,
                        }
                    })
                })
                .count()
                >= required_followers
        })
        .await
        .map_err(|_| BrokerError::Unknown(format!(
            "publish confirm timed out after {timeout_ms}ms: {:?} requires {} follower acknowledgement(s) past offset {offset} on {}/{}",
            assignment.durability, required_followers, key.tp, key.part
        )))?;
        if self
            .assignments
            .get(key)
            .is_none_or(|current| !current.preserves_replication_contract(&assignment))
        {
            return Err(BrokerError::Unknown(
                "assignment changed while awaiting replication".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod replication_confirm_wait_tests {
    use super::wait_for_replication_progress;
    use std::{cell::Cell, time::Duration};
    use tokio::{sync::Notify, time::Instant};

    #[tokio::test(start_paused = true)]
    async fn progress_between_check_and_wait_is_not_lost() {
        let changed = Notify::new();
        let covered = Cell::new(false);
        let start = Instant::now();
        let result =
            wait_for_replication_progress(&changed, start + Duration::from_secs(1), || {
                let observed = covered.get();
                if !observed {
                    // A final follower report arrives after the old progress was
                    // read, with no subsequent report to rescue a missed wakeup.
                    covered.set(true);
                    changed.notify_waiters();
                }
                observed
            })
            .await;

        assert!(
            result.is_ok(),
            "durable progress must not wait for another report"
        );
        assert_eq!(Instant::now(), start);
    }

    #[tokio::test(start_paused = true)]
    async fn final_report_wakes_an_existing_waiter() {
        let changed = Notify::new();
        let covered = Cell::new(false);
        let start = Instant::now();
        let wait = wait_for_replication_progress(&changed, start + Duration::from_secs(1), || {
            covered.get()
        });
        tokio::pin!(wait);
        assert!(futures::poll!(wait.as_mut()).is_pending());

        covered.set(true);
        changed.notify_waiters();
        assert!(wait.await.is_ok());
        assert_eq!(Instant::now(), start);
    }

    #[tokio::test(start_paused = true)]
    async fn unrelated_notifications_do_not_extend_the_deadline() {
        let changed = Notify::new();
        let deadline = Instant::now() + Duration::from_millis(100);
        let wait = wait_for_replication_progress(&changed, deadline, || false);
        tokio::pin!(wait);
        assert!(futures::poll!(wait.as_mut()).is_pending());

        for _ in 0..2 {
            tokio::time::advance(Duration::from_millis(40)).await;
            changed.notify_waiters();
            assert!(futures::poll!(wait.as_mut()).is_pending());
        }
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(wait.await.is_err());
        assert_eq!(Instant::now(), deadline);
    }

    #[tokio::test(start_paused = true)]
    async fn covered_progress_does_not_wait_for_a_notification() {
        let changed = Notify::new();
        let start = Instant::now();
        assert!(
            wait_for_replication_progress(&changed, start + Duration::from_secs(1), || true)
                .await
                .is_ok()
        );
        assert_eq!(Instant::now(), start);
    }
}

#[cfg(test)]
mod stream_tunables_tests {
    use super::*;
    use crate::broker::BrokerConfig;
    use arc_swap::ArcSwap;

    /// A runtime-settings change must propagate to the live tunables reader: a
    /// later config snapshot is reflected on the NEXT read (no restart needed) -
    /// the live-settings-propagation principle, at the source.
    #[test]
    fn stream_apply_tunables_reads_live_config() {
        let cfg = Arc::new(ArcSwap::from_pointee(BrokerConfig {
            replication_caught_up_poll_ms: 333,
            replication_stream_apply_linger_us: 222,
            replication_stream_apply_max_merge_bytes: 111,
            ..BrokerConfig::default()
        }));

        let tunables = stream_apply_tunables_from(cfg.clone());
        assert_eq!(
            tunables(),
            StreamApplyTunables {
                keepalive_ms: 333,
                apply_linger_us: 222,
                max_merge_bytes: 111,
                fixed_deadline: false,
                open_batch: 0,
            }
        );

        // Simulate a live runtime-settings update.
        cfg.store(Arc::new(BrokerConfig {
            replication_caught_up_poll_ms: 777,
            replication_stream_apply_linger_us: 888,
            replication_stream_apply_max_merge_bytes: 999,
            replication_stream_apply_fixed_deadline: true,
            replication_stream_apply_open_batch: 3,
            ..BrokerConfig::default()
        }));
        assert_eq!(
            tunables(),
            StreamApplyTunables {
                keepalive_ms: 777,
                apply_linger_us: 888,
                max_merge_bytes: 999,
                fixed_deadline: true,
                open_batch: 3,
            },
            "live config change must be reflected on the next read"
        );
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
        let records = BrokerOwnerStreamRecords {
            messages: message_batch(10, 12, vec![(10, message(128)), (11, message(8))]),
            events: event_batch(20, 20, Vec::new()),
        };

        let capped = cap_owner_replication_records(records, 1);
        let OwnerReplicationRead::Batch(messages) = capped.messages else {
            panic!("expected message batch");
        };

        assert_eq!(messages.next_offset, 11);
        assert_eq!(messages.records.len(), 1);
        assert_eq!(messages.records[0].0, 10);
    }

    #[test]
    fn byte_cap_lookahead_preserves_snapshot_deferral() {
        let records = BrokerOwnerStreamRecords {
            // One accepted record and one lookahead from a bounded storage scan.
            messages: message_batch(10, 12, vec![(10, message(16)), (11, message(16))]),
            events: event_batch(
                20,
                21,
                vec![(
                    20,
                    StromaEvent::Snapshot {
                        tp: "q".into(),
                        part: 0,
                        group: None,
                        blob: vec![],
                    },
                )],
            ),
        };
        let capped = cap_owner_replication_records(records, 48);
        let OwnerReplicationRead::Batch(messages) = capped.messages else {
            panic!("message batch");
        };
        let OwnerReplicationRead::Batch(events) = capped.events else {
            panic!("event batch");
        };
        assert_eq!(messages.next_offset, 11);
        assert!(events.records.is_empty());
        assert_eq!(events.next_offset, 20);
    }

    #[test]
    fn byte_cap_stops_events_before_unreturned_message_payloads() {
        let records = BrokerOwnerStreamRecords {
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
                            expire_at: None,
                        },
                    ),
                    (
                        22,
                        StromaEvent::Enqueue {
                            off: 11,
                            retries: 0,
                            expire_at: None,
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

        assert_eq!(messages.next_offset, 11);
        assert_eq!(messages.records.len(), 1);
        assert_eq!(messages.records[0].0, 10);
        assert_eq!(events.next_offset, 22);
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

// ---------------- impl Broker<StromaEngine>: replication engine methods ----------------

impl Broker<StromaEngine> {
    /// Whether the owner ships written records before its own fsync. On for
    /// every buffered queue, which cannot confirm before followers write, and
    /// for durable queues when `replication.early_replication` is set. Only
    /// history-bound assignments fence the history the way this needs. A
    /// buffered queue without that binding still replicates from the durable
    /// source, one background sync behind.
    fn early_replication_enabled(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> bool {
        (self.config_snapshot().replication_early
            || self.engine.queue_is_buffered(topic, partition.id(), group))
            && self
            .assignment_cache
            .get(&QueueKey {
                tp: topic.into(),
                part: partition,
                group: group.map(str::to_owned),
            })
            .is_some_and(|a| a.history.is_some())
    }

    /// Owner read of the single queue log, optionally long-polling for new
    /// records when the first read comes back empty.
    pub async fn read_owner_queue_records(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        from: Offset,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> Result<OwnerReplicationRead<StromaEvent>, BrokerError> {
        if max_records == 0 || max_bytes == 0 {
            return Err(BrokerError::InvalidArgument(
                "replication read limits must be greater than zero".into(),
            ));
        }

        if max_wait_ms == 0 {
            return self
                .read_owner_queue_records_now(topic, partition, group, from, max_records, max_bytes)
                .await;
        }

        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        let qs = self.queue(&key).await;
        // Watch receivers register before the first read, so writes during that
        // read remain observable even if the select has not been polled yet.
        let mut written = if self.early_replication_enabled(topic, partition, group) {
            Some(
                self.engine
                    .subscribe_owner_queue_written(topic, partition.id(), group)
                    .await?,
            )
        } else {
            None
        };
        let early_wake = async {
            if let Some(written) = &mut written {
                let _ = written.changed().await;
                return;
            }
            std::future::pending::<()>().await;
        };
        let notified = qs.replication_notify.notified();
        tokio::pin!(notified);

        let records = self
            .read_owner_queue_records_now(topic, partition, group, from, max_records, max_bytes)
            .await?;
        if !owner_read_empty(&records) {
            return Ok(records);
        }

        tokio::select! {
            _ = &mut notified => {}
            _ = early_wake => {}
            _ = self.shutdown_publishers.cancelled() => {}
            _ = tokio::time::sleep(Duration::from_millis(max_wait_ms)) => {}
        }

        self.read_owner_queue_records_now(topic, partition, group, from, max_records, max_bytes)
            .await
    }

    async fn read_owner_queue_records_now(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        from: Offset,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<StromaEvent>, BrokerError> {
        self.ensure_replication_source_owner(topic, partition, group)?;
        let _owner_read_timer = self.replication_timing.owner_read.timer();
        if self.early_replication_enabled(topic, partition, group) {
            let records = self
                .engine
                .read_owner_queue_records_early(
                    topic,
                    partition.id(),
                    group,
                    from,
                    max_records,
                    max_bytes,
                )
                .await?;
            self.ensure_replication_source_owner(topic, partition, group)?;
            return Ok(records);
        }
        // The read obeys the byte budget itself. Every payload reference is to
        // an earlier position in this same ordered log.
        Ok(self
            .engine
            .read_owner_queue_records(topic, partition.id(), group, from, max_records, max_bytes)
            .await?)
    }

    /// Owner read of a stream's record and cursor-event logs, optionally
    /// long-polling for new records when the first read comes back empty.
    pub async fn read_owner_stream_records(
        &self,
        topic: &str,
        partition: Partition,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> Result<BrokerOwnerStreamRecords, BrokerError> {
        if max_messages == 0 || max_events == 0 || max_bytes == 0 {
            return Err(BrokerError::InvalidArgument(
                "replication read limits must be greater than zero".into(),
            ));
        }

        // Owners open a cold stream on first traffic, and a follower pull can
        // be the first. Open it as a stream with the declared config before
        // any storage read, which would create it with the default queue kind.
        if !self.engine.is_materialized(topic, partition.id(), None)
            && !self.engine.durable_is_stream(topic, partition.id())
        {
            self.ensure_replication_source_owner(topic, partition, None)?;
            if self.route_stream(topic, partition.id()).await.is_none() {
                return Err(BrokerError::NotOwner {
                    topic: topic.to_string(),
                    partition,
                    group: None,
                });
            }
        }

        if max_wait_ms == 0 {
            return self
                .read_owner_stream_records_now(
                    topic,
                    partition,
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
            group: None,
        };
        let qs = self.queue(&key).await;
        // Watch receivers register before the first read, so writes during that
        // read remain observable even if the select has not been polled yet.
        let mut written = if self.early_replication_enabled(topic, partition, None) {
            Some(
                self.engine
                    .subscribe_owner_written(topic, partition.id(), None)
                    .await?,
            )
        } else {
            None
        };
        let early_wake = async {
            if let Some((messages, events)) = &mut written {
                tokio::select! { _ = messages.changed() => {}, _ = events.changed() => {} }
                return;
            }
            std::future::pending::<()>().await;
        };
        let notified = qs.replication_notify.notified();
        tokio::pin!(notified);
        // Stream publishes and cursor commits run on the stream channel, not
        // the queue loop, so the channel's durability signal wakes followers.
        // Created before the read so a wake that lands during it is kept.
        let stream_progress = self.stream_durable_progress(topic, partition.id());
        let stream_notified = stream_progress.as_ref().map(|progress| progress.notified());
        let stream_wake = async {
            match stream_notified {
                Some(notified) => notified.await,
                None => std::future::pending::<()>().await,
            }
        };

        let records = self
            .read_owner_stream_records_now(
                topic,
                partition,
                message_from,
                event_from,
                max_messages,
                max_events,
                max_bytes,
            )
            .await?;
        if !owner_stream_records_empty(&records) {
            return Ok(records);
        }

        tokio::select! {
            _ = &mut notified => {}
            _ = stream_wake => {}
            _ = early_wake => {}
            _ = self.shutdown_publishers.cancelled() => {}
            _ = tokio::time::sleep(Duration::from_millis(max_wait_ms)) => {}
        }

        self.read_owner_stream_records_now(
            topic,
            partition,
            message_from,
            event_from,
            max_messages,
            max_events,
            max_bytes,
        )
        .await
    }

    async fn read_owner_stream_records_now(
        &self,
        topic: &str,
        partition: Partition,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<BrokerOwnerStreamRecords, BrokerError> {
        self.ensure_replication_source_owner(topic, partition, None)?;
        let _owner_read_timer = self.replication_timing.owner_read.timer();
        if self.early_replication_enabled(topic, partition, None) {
            let messages = self
                .engine
                .read_owner_stream_records_early(
                    topic,
                    partition.id(),
                    message_from,
                    max_messages,
                    max_bytes,
                )
                .await?;
            let events = self
                .engine
                .read_owner_stream_cursor_events_early(
                    topic,
                    partition.id(),
                    event_from,
                    max_events,
                    max_bytes,
                )
                .await?;
            self.ensure_replication_source_owner(topic, partition, None)?;
            return Ok(cap_owner_replication_records(
                BrokerOwnerStreamRecords { messages, events },
                max_bytes,
            ));
        }
        let messages = self
            .engine
            .read_owner_stream_records(topic, partition.id(), message_from, max_messages, max_bytes)
            .await?;
        let events = self
            .engine
            .read_owner_stream_cursor_events(topic, partition.id(), event_from, max_events, max_bytes)
            .await?;
        Ok(cap_owner_replication_records(
            BrokerOwnerStreamRecords { messages, events },
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
        self.apply_followed_queue_mode(&QueueKey::new(topic, partition, group))
            .await;
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
        self.apply_followed_queue_mode(&QueueKey::new(topic, partition, group))
            .await;
        Ok(())
    }

    pub async fn export_queue_state_checkpoint(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Result<QueueStateCheckpoint, BrokerError> {
        self.ensure_replication_source_owner(topic, partition, group)?;
        Ok(self
            .engine
            .export_queue_state_checkpoint(topic, partition.id(), group)
            .await?)
    }

    /// Apply one owner read of the single queue log to this follower.
    pub async fn apply_follower_queue_records(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        read: OwnerReplicationRead<StromaEvent>,
    ) -> Result<BrokerQueueFollowerApply, BrokerError> {
        Self::apply_follower_queue_records_using_engine(
            &self.engine,
            topic,
            partition,
            group,
            read,
            false,
        )
        .await
    }

    /// `written` appends without waiting for fsync. See `queue_apply_batch`.
    async fn apply_follower_queue_records_using_engine(
        engine: &StromaEngine,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        read: OwnerReplicationRead<StromaEvent>,
        written: bool,
    ) -> Result<BrokerQueueFollowerApply, BrokerError> {
        if let Some(required) = checkpoint_required(&read) {
            tracing::debug!(
                topic,
                partition = partition.id(),
                group,
                requested_offset = required.requested_offset,
                head_offset = required.head_offset,
                next_offset = required.next_offset,
                "replication queue read requires checkpoint before follower apply"
            );
            return Ok(BrokerQueueFollowerApply::CheckpointRequired(required));
        }
        let outcome = match queue_apply_batch(read, written) {
            Some(batch) => Some(
                engine
                    .apply_replicated_queue_batch(topic, partition.id(), group, batch)
                    .await?,
            ),
            None => {
                // Nothing to append, but a sealed, dirty or no longer
                // following replica must still refuse, so an empty read never
                // reports progress for a copy that cannot accept records.
                engine
                    .queue_replication_next_offset(topic, partition.id(), group)
                    .await?;
                None
            }
        };
        Ok(BrokerQueueFollowerApply::Applied(outcome))
    }

    /// Apply one owner read of a stream's record and cursor-event logs.
    pub async fn apply_follower_stream_records(
        &self,
        topic: &str,
        partition: Partition,
        records: BrokerOwnerStreamRecords,
    ) -> Result<BrokerStreamFollowerApply, BrokerError> {
        Self::apply_follower_stream_records_using_engine(&self.engine, topic, partition, records)
            .await
    }

    async fn apply_follower_stream_records_using_engine(
        engine: &StromaEngine,
        topic: &str,
        partition: Partition,
        records: BrokerOwnerStreamRecords,
    ) -> Result<BrokerStreamFollowerApply, BrokerError> {
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
                    requested_offset,
                    head_offset,
                    next_offset,
                    "replication message read requires checkpoint before follower apply"
                );
                return Ok(BrokerStreamFollowerApply::CheckpointRequired {
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
                    requested_offset,
                    head_offset,
                    next_offset,
                    "replication event read requires checkpoint before follower apply"
                );
                return Ok(BrokerStreamFollowerApply::CheckpointRequired {
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

        // A stream advances its record tail after the append and carries no
        // consumer group.
        let outcome = engine
            .apply_replicated_stream_batch(topic, partition.id(), messages, events)
            .await?;
        Ok(BrokerStreamFollowerApply::Applied(outcome))
    }

    /// Apply one pushed queue batch and report the advanced durable cursor,
    /// reusing the exact apply and progress path the pull catch-up uses.
    /// `CheckpointRequired` tells the push caller to fall back to the pull +
    /// checkpoint path (offset gap / epoch fence), then push again.
    pub async fn apply_queue_push_batch(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        read: OwnerReplicationRead<StromaEvent>,
    ) -> Result<QueuePushApply, BrokerError> {
        // A checkpoint-required read has nothing to append.
        let Some(progress) = owner_read_batch_progress("queue", &read)? else {
            return Ok(QueuePushApply::CheckpointRequired);
        };

        // The push path reports written and durable progress separately, so
        // a buffered queue's follower can skip the per-batch fsync here.
        let written = self
            .engine
            .queue_is_buffered(topic, partition.id(), group);
        let apply = {
            let _follower_apply_timer = self.replication_timing.follower_apply.timer();
            Self::apply_follower_queue_records_using_engine(
                &self.engine,
                topic,
                partition,
                group,
                read,
                written,
            )
            .await?
        };

        let outcome = match apply {
            BrokerQueueFollowerApply::Applied(outcome) => outcome,
            BrokerQueueFollowerApply::CheckpointRequired(_) => {
                return Ok(QueuePushApply::CheckpointRequired);
            }
        };

        match replicated_append_progress_after_apply("queue", &progress, outcome.as_ref())? {
            ReplicatedAppendProgress::Advanced(next) => {
                let durable_next = self
                    .queue_follower_durable_next(topic, partition, group, next, written)
                    .await?;
                Ok(QueuePushApply::Applied { next, durable_next })
            }
            ReplicatedAppendProgress::CheckpointRequired(_) => Ok(QueuePushApply::CheckpointRequired),
        }
    }

    /// How far a follower's queue log is synced after an apply that reached
    /// `next`. Without `written` the apply synced, so that is `next` itself.
    pub(crate) async fn queue_follower_durable_next(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        next: Offset,
        written: bool,
    ) -> Result<Offset, BrokerError> {
        if !written {
            return Ok(next);
        }
        let status = self
            .engine
            .queue_log_sync_status(topic, partition.id(), group)
            .await?;
        Ok(status.event_log.durable_next.min(next))
    }

    pub async fn promote_replication_follower_if_caught_up(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        expected_next: Offset,
    ) -> Result<QueuePromotionOutcome, BrokerError> {
        self.engine
            .promote_queue_follower_if_caught_up(topic, partition.id(), group, expected_next)
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

    /// Wait until the cached assignment's replication durability policy is
    /// satisfied for `offset` (the owner's local durable write already counts).
    /// Returns immediately when there is no cached assignment (standalone) or the
    /// policy is local-durable. Streams have no per-record enqueue event;
    /// queues use `await_queue_replication_confirm` with an exact dependency.
    pub async fn await_stream_replication_confirm(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        offset: Offset,
    ) -> Result<(), BrokerError> {
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        self.replication_confirm_gate()
            .await_stream_confirm(&key, offset)
            .await
    }

    /// Confirm a queue record only when each counted follower covers both its
    /// payload and the exact enqueue dependency supplied by local completion.
    pub async fn await_queue_replication_confirm(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        offset: Offset,
        dependency: stroma_core::QueuePublishCommit,
    ) -> Result<(), BrokerError> {
        if offset >= dependency.next {
            return Err(BrokerError::InvalidArgument(
                "offset outside queue publication dependency".into(),
            ));
        }
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        let assignment = self.assignment_cache.get(&key).map(|a| a.clone());
        self.replication_confirm_gate()
            .await_dependency(&key, offset, Some(dependency), assignment)
            .await
    }

    /// Stream failover promotion at the follower's own tails (see the queue
    /// `promote_replication_follower_to_local_tail` and the stream
    /// `PromoteFollowerToOwner` transition arm for the safety argument).
    pub async fn promote_stream_follower_to_local_tail(
        &self,
        topic: &str,
        partition: Partition,
        epoch: u64,
    ) -> Result<StreamPromotionOutcome, BrokerError> {
        self.engine
            .promote_stream_follower_to_local_tail(topic, partition.id(), epoch)
            .await
            .map_err(BrokerError::from)
    }

    pub async fn catch_up_queue_follower(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        options: QueueCatchUpOptions,
    ) -> Result<BrokerQueueCatchUp, BrokerError> {
        self.catch_up_queue_follower_using_engine(&self.engine, owner, topic, partition, group, options)
            .await
    }

    /// Copy the owner's single queue log into `engine`, which may be an
    /// explicitly authorized non-voting storage generation.
    pub async fn catch_up_queue_follower_using_engine(
        &self,
        engine: &StromaEngine,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        options: QueueCatchUpOptions,
    ) -> Result<BrokerQueueCatchUp, BrokerError> {
        let mut progress = QueueCatchUpProgress {
            next_offset: options.from,
            ..Default::default()
        };
        self.catch_up_queue_follower_tracked(
            engine,
            owner,
            topic,
            partition,
            group,
            options,
            &mut progress,
        )
        .await
    }

    /// Queue catch-up that keeps `progress` current after every applied
    /// batch, so a caller still knows how far the local log got when a
    /// later read fails.
    #[allow(clippy::too_many_arguments)]
    async fn catch_up_queue_follower_tracked(
        &self,
        engine: &StromaEngine,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        options: QueueCatchUpOptions,
        progress: &mut QueueCatchUpProgress,
    ) -> Result<BrokerQueueCatchUp, BrokerError> {
        if options.max_records_per_read == 0
            || options.max_bytes_per_read == 0
            || options.max_iterations == 0
        {
            return Err(BrokerError::InvalidArgument(
                "replication catch-up limits must be greater than zero".into(),
            ));
        }

        for _ in 0..options.max_iterations {
            let read = {
                let _follower_owner_read_timer =
                    self.replication_timing.follower_owner_read.timer();
                owner
                    .read_owner_queue_records_fenced(
                        topic,
                        partition,
                        group,
                        progress.next_offset,
                        options.max_records_per_read,
                        options.max_bytes_per_read,
                        options.max_wait_ms,
                        self.assignment_cache
                            .get(&QueueKey {
                                tp: topic.to_string(),
                                part: partition,
                                group: group.map(str::to_string),
                            })
                            .map(|a| a.epoch),
                    )
                    .await?
            };

            let Some(read_progress) = owner_read_batch_progress("queue", &read)? else {
                let apply = Self::apply_follower_queue_records_using_engine(
                    engine, topic, partition, group, read, false,
                )
                .await?;
                return match apply {
                    BrokerQueueFollowerApply::CheckpointRequired(required) => {
                        Ok(BrokerQueueCatchUp::CheckpointRequired { progress: *progress, required })
                    }
                    BrokerQueueFollowerApply::Applied(_) => {
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
            };

            let apply = {
                let _follower_apply_timer = self.replication_timing.follower_apply.timer();
                // The pull path reports one position, so it applies durably.
                Self::apply_follower_queue_records_using_engine(
                    engine, topic, partition, group, read, false,
                )
                .await?
            };
            let outcome = match apply {
                BrokerQueueFollowerApply::Applied(outcome) => outcome,
                BrokerQueueFollowerApply::CheckpointRequired(required) => {
                    return Ok(BrokerQueueCatchUp::CheckpointRequired { progress: *progress, required });
                }
            };
            match replicated_append_progress_after_apply("queue", &read_progress, outcome.as_ref())? {
                ReplicatedAppendProgress::Advanced(next) => progress.next_offset = next,
                ReplicatedAppendProgress::CheckpointRequired(required) => {
                    return Ok(BrokerQueueCatchUp::CheckpointRequired { progress: *progress, required });
                }
            }

            progress.iterations += 1;
            progress.applied_records += read_progress.record_count;
            if read_progress.record_count == 0 {
                return Ok(BrokerQueueCatchUp::CaughtUp(*progress));
            }
        }

        Ok(BrokerQueueCatchUp::IterationLimit { progress: *progress })
    }

    /// Queue catch-up that requests a coordinated reseed when the owner has
    /// already compacted past this follower.
    pub async fn catch_up_queue_follower_with_checkpoint(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        options: QueueCatchUpOptions,
    ) -> Result<BrokerQueueCatchUp, BrokerError> {
        if options.max_records_per_read == 0
            || options.max_bytes_per_read == 0
            || options.max_iterations == 0
        {
            return Err(BrokerError::InvalidArgument(
                "replication catch-up limits must be greater than zero".into(),
            ));
        }

        let outcome = self
            .catch_up_queue_follower(owner, topic, partition, group, options)
            .await?;
        if let BrokerQueueCatchUp::CheckpointRequired { .. } = &outcome {
            self.request_reseed_for_exhausted_history(topic, partition, group)
                .await?;
        }
        Ok(outcome)
    }

    pub async fn catch_up_stream_follower(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: Partition,
        options: StreamCatchUpOptions,
    ) -> Result<BrokerStreamCatchUp, BrokerError> {
        let mut progress = StreamCatchUpProgress {
            message_next_offset: options.message_from,
            event_next_offset: options.event_from,
            ..Default::default()
        };
        self.catch_up_stream_follower_tracked(owner, topic, partition, options, &mut progress)
            .await
    }

    /// Stream catch-up that keeps `progress` current after every applied
    /// batch, like [`Self::catch_up_queue_follower_tracked`].
    async fn catch_up_stream_follower_tracked(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: Partition,
        options: StreamCatchUpOptions,
        progress: &mut StreamCatchUpProgress,
    ) -> Result<BrokerStreamCatchUp, BrokerError> {
        if options.max_messages_per_read == 0
            || options.max_events_per_read == 0
            || options.max_bytes_per_read == 0
            || options.max_iterations == 0
        {
            return Err(BrokerError::InvalidArgument(
                "replication catch-up limits must be greater than zero".into(),
            ));
        }

        for _ in 0..options.max_iterations {
            let records = {
                let _follower_owner_read_timer =
                    self.replication_timing.follower_owner_read.timer();
                owner
                    .read_owner_stream_records_fenced(
                        topic,
                        partition,
                        progress.message_next_offset,
                        progress.event_next_offset,
                        options.max_messages_per_read,
                        options.max_events_per_read,
                        options.max_bytes_per_read,
                        options.max_wait_ms,
                        self.assignment_cache
                            .get(&QueueKey {
                                tp: topic.to_string(),
                                part: partition,
                                group: None,
                            })
                            .map(|a| a.epoch),
                    )
                    .await?
            };

            let message_progress = owner_read_batch_progress("message", &records.messages)?;
            let event_progress = owner_read_batch_progress("event", &records.events)?;
            let (message_progress, event_progress) = match (message_progress, event_progress) {
                (Some(message_progress), Some(event_progress)) => {
                    (message_progress, event_progress)
                }
                _ => {
                    let apply = {
                        let _follower_apply_timer = self.replication_timing.follower_apply.timer();
                        self.apply_follower_stream_records(topic, partition, records)
                            .await?
                    };
                    return match apply {
                        BrokerStreamFollowerApply::CheckpointRequired { messages, events } => {
                            Ok(BrokerStreamCatchUp::CheckpointRequired {
                                progress: *progress,
                                messages,
                                events,
                            })
                        }
                        BrokerStreamFollowerApply::Applied(_) => {
                            tracing::error!(
                                topic,
                                partition = partition.id(),
                                "replication catch-up applied records even though owner read reported checkpoint required"
                            );
                            Err(BrokerError::Unknown(
                                "checkpoint-required owner read unexpectedly applied".into(),
                            ))
                        }
                    };
                }
            };

            let apply = {
                let _follower_apply_timer = self.replication_timing.follower_apply.timer();
                self.apply_follower_stream_records(topic, partition, records)
                    .await?
            };

            match apply {
                BrokerStreamFollowerApply::Applied(outcome) => {
                    match replicated_append_progress_after_apply(
                        "message",
                        &message_progress,
                        outcome.message_log.as_ref(),
                    )? {
                        ReplicatedAppendProgress::Advanced(next_offset) => {
                            progress.message_next_offset = next_offset;
                        }
                        ReplicatedAppendProgress::CheckpointRequired(checkpoint) => {
                            return Ok(BrokerStreamCatchUp::CheckpointRequired {
                                progress: *progress,
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
                            return Ok(BrokerStreamCatchUp::CheckpointRequired {
                                progress: *progress,
                                messages: None,
                                events: Some(checkpoint),
                            });
                        }
                    }
                }
                BrokerStreamFollowerApply::CheckpointRequired { messages, events } => {
                    return Ok(BrokerStreamCatchUp::CheckpointRequired {
                        progress: *progress,
                        messages,
                        events,
                    });
                }
            }

            progress.iterations += 1;
            progress.applied_message_records += message_progress.record_count;
            progress.applied_event_records += event_progress.record_count;

            if message_progress.record_count == 0 && event_progress.record_count == 0 {
                return Ok(BrokerStreamCatchUp::CaughtUp(*progress));
            }
        }

        Ok(BrokerStreamCatchUp::IterationLimit { progress: *progress })
    }

    pub async fn catch_up_stream_follower_with_checkpoint(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        topic: &str,
        partition: Partition,
        options: StreamCatchUpOptions,
    ) -> Result<BrokerStreamCatchUp, BrokerError> {
        let outcome = self
            .catch_up_stream_follower(owner, topic, partition, options)
            .await?;
        if let BrokerStreamCatchUp::CheckpointRequired { .. } = &outcome {
            self.request_reseed_for_exhausted_history(topic, partition, None)
                .await?;
        }
        Ok(outcome)
    }

    /// A follower below the owner's retained history can only be rebuilt
    /// through a coordinated reseed, which requires an admitted history
    /// binding. Without one there is no safe local reset, so the worker
    /// reports the requirement and backs off on the checkpoint poll.
    async fn request_reseed_for_exhausted_history(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Result<(), BrokerError> {
        let assignment = self
            .assignment_cache
            .get(&QueueKey {
                tp: topic.into(),
                part: partition,
                group: group.map(str::to_owned),
            })
            .map(|a| a.clone());
        match assignment.filter(|a| a.history.is_some()) {
            Some(assignment) => {
                self.ownership
                    .request_queue_reseed(topic, partition, group, assignment.epoch)
                    .await
                    .map_err(BrokerError::Unknown)?;
                tracing::info!(
                    topic,
                    partition = partition.id(),
                    group,
                    "retained history exhausted, coordinated reseed requested"
                );
            }
            None => tracing::warn!(
                topic,
                partition = partition.id(),
                group,
                "retained history exhausted and no admitted history binding, follower cannot be reseeded"
            ),
        }
        Ok(())
    }

    pub async fn run_follower_replication_worker_once(
        &self,
        owner: &dyn BrokerOwnerReplicationPeer,
        queue: &crate::coordination::QueueIdentity,
        kind: ReplicationResourceKind,
        cfg: FollowerReplicationWorkerConfig,
    ) -> Result<FollowerCatchUp, BrokerError> {
        let _follower_tick_timer = self.replication_timing.follower_tick.timer();
        let worker = self.follower_replication_worker(queue)?;
        let topic = queue.topic.as_str();
        let outcome = match kind {
            ReplicationResourceKind::Queue => {
                let options = worker.lock().await.queue_catch_up_options(cfg)?;
                let group = queue.group.as_deref();
                let mut progress = QueueCatchUpProgress {
                    next_offset: options.from,
                    ..Default::default()
                };
                let result = self
                    .catch_up_queue_follower_tracked(
                        &self.engine,
                        owner,
                        topic,
                        queue.partition,
                        group,
                        options,
                        &mut progress,
                    )
                    .await;
                // Batches applied before a failed read are in the local log.
                // Keep the cursor past them, or the next pass re-reads them,
                // sees an overlap and asks for a checkpoint it cannot get.
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        worker.lock().await.cursor = FollowerCursor::Queue {
                            next_offset: progress.next_offset,
                        };
                        return Err(err);
                    }
                };
                if cfg.allow_checkpoint_install
                    && matches!(outcome, BrokerQueueCatchUp::CheckpointRequired { .. })
                {
                    self.request_reseed_for_exhausted_history(topic, queue.partition, group)
                        .await?;
                }
                FollowerCatchUp::Queue(outcome)
            }
            ReplicationResourceKind::Stream => {
                let options = worker.lock().await.stream_catch_up_options(cfg)?;
                let mut progress = StreamCatchUpProgress {
                    message_next_offset: options.message_from,
                    event_next_offset: options.event_from,
                    ..Default::default()
                };
                let result = self
                    .catch_up_stream_follower_tracked(
                        owner,
                        topic,
                        queue.partition,
                        options,
                        &mut progress,
                    )
                    .await;
                let outcome = match result {
                    Ok(outcome) => outcome,
                    Err(err) => {
                        worker.lock().await.cursor = FollowerCursor::Stream {
                            message_next_offset: progress.message_next_offset,
                            event_next_offset: progress.event_next_offset,
                        };
                        return Err(err);
                    }
                };
                if cfg.allow_checkpoint_install
                    && matches!(outcome, BrokerStreamCatchUp::CheckpointRequired { .. })
                {
                    self.request_reseed_for_exhausted_history(topic, queue.partition, None)
                        .await?;
                }
                FollowerCatchUp::Stream(outcome)
            }
        };

        worker.lock().await.record_catch_up(cfg, &outcome);
        Ok(outcome)
    }

    pub async fn run_follower_replication_worker_loop(
        self: &Arc<Self>,
        assignment: PartitionAssignment,
        resolver: Arc<dyn BrokerOwnerReplicationPeerResolver>,
        kind: ReplicationResourceKind,
        cfg: FollowerReplicationWorkerConfig,
        shutdown: CancellationToken,
    ) -> Result<FollowerReplicationWorkerLoopExit, BrokerError> {
        let mut ticks = 0;
        // Repeats of the same tick error log at debug, so a dead owner does not
        // flood the log until failover. Holds (error text, repeats, first seen).
        let mut failing: Option<(String, u64, std::time::Instant)> = None;
        let mut initialized_history_offsets = false;
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
                    stream_enabled: snap.replication_stream_enabled,
                    stream_apply_linger_us: snap.replication_stream_apply_linger_us,
                    stream_apply_max_merge_bytes: snap.replication_stream_apply_max_merge_bytes,
                    stream_buffer_batches: snap.replication_stream_buffer_batches,
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

            let Some(owner) = resolver.resolve_owner_peer(&assignment, kind).await? else {
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

            let history = assignment
                .history
                .as_ref()
                .map(|history| {
                    let receiver = self.ownership.replication_node_id().ok_or_else(|| {
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
                            &assignment.owner,
                            receiver,
                        )
                        .map_err(BrokerError::InvalidArgument)
                })
                .transpose()?;
            let scoped_owner = AssignmentReplicationPeer {
                peer: owner.as_ref(),
                epoch: assignment.epoch,
                broker: self.clone(),
                history,
            };
            scoped_owner.check_history()?;
            let tick_result = tokio::select! {
                biased;
                _ = shutdown.cancelled() => {
                    return Ok(FollowerReplicationWorkerLoopExit::Cancelled { ticks });
                }
                _ = runtime.shutdown.cancelled() => {
                    return Ok(FollowerReplicationWorkerLoopExit::WorkerStopped { ticks });
                }
                outcome = async {
                    if !initialized_history_offsets && scoped_owner.history.is_some()
                        && kind == ReplicationResourceKind::Queue
                    {
                        // Recovery may install a compacted baseline. Only exact
                        // admitted history permits trusting local cursors; legacy
                        // workers must still reconcile their suffix from zero.
                        scoped_owner.check_history()?;
                        let next = self.engine
                            .queue_replication_next_offset(
                                &assignment.queue.topic,
                                assignment.queue.partition.id(),
                                assignment.queue.group.as_deref(),
                            ).await?;
                        scoped_owner.check_history()?;
                        runtime.state.lock().await.cursor = FollowerCursor::Queue { next_offset: next };
                        initialized_history_offsets = true;
                    }
                    // The credit-based push transport carries only queues (only
                    // the StreamReplicationRead pull op is stream-aware), so a
                    // Plexus stream always catches up via the pull path.
                    if cfg.stream_enabled && kind == ReplicationResourceKind::Queue {
                        self.run_follower_queue_push_tick(
                            &scoped_owner,
                            &assignment.queue,
                            cfg,
                            runtime.shutdown.clone(),
                        )
                        .await
                    } else {
                        self.run_follower_replication_worker_once(
                            &scoped_owner,
                            &assignment.queue,
                            kind,
                            cfg,
                        )
                        .await
                        .map(|_| ())
                    }
                } => outcome,
            };

            match tick_result {
                Ok(_) => {
                    ticks += 1;
                    if let Some((_, count, since)) = failing.take() {
                        tracing::info!(
                            topic = %assignment.queue.topic,
                            partition = assignment.queue.partition.id(),
                            group = ?assignment.queue.group,
                            owner = %assignment.owner,
                            failed_ticks = count,
                            failing_ms = since.elapsed().as_millis() as u64,
                            "follower replication resumed"
                        );
                    }
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
                    let cursor = runtime.state.lock().await.cursor;
                    let text = format!("{err:?}");
                    let repeat = match &mut failing {
                        Some((last, count, _)) if *last == text => {
                            *count += 1;
                            true
                        }
                        _ => {
                            let since = failing.take().map_or_else(std::time::Instant::now, |f| f.2);
                            failing = Some((text.clone(), 1, since));
                            false
                        }
                    };
                    if repeat {
                        tracing::debug!(
                            topic = %assignment.queue.topic,
                            partition = assignment.queue.partition.id(),
                            group = ?assignment.queue.group,
                            owner = %assignment.owner,
                            assignment_epoch = assignment.epoch,
                            ?cursor,
                            "follower replication worker tick failed again: {text}"
                        );
                    } else {
                        tracing::warn!(
                            topic = %assignment.queue.topic,
                            partition = assignment.queue.partition.id(),
                            group = ?assignment.queue.group,
                            owner = %assignment.owner,
                            assignment_epoch = assignment.epoch,
                            ?cursor,
                            "follower replication worker tick failed: {text}"
                        );
                    }
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

    /// One push catch-up: open a credit-based queue push from the owner, apply
    /// through the broker, and on a checkpoint requirement fall back to the
    /// proven pull+checkpoint path for one tick (then the loop pushes again).
    /// The cursor starts from and is synced back to the worker state, so pull
    /// and push share one authoritative cursor.
    async fn run_follower_queue_push_tick(
        self: &Arc<Self>,
        owner: &dyn BrokerOwnerReplicationPeer,
        queue: &crate::coordination::QueueIdentity,
        cfg: FollowerReplicationWorkerConfig,
        shutdown: CancellationToken,
    ) -> Result<(), BrokerError> {
        let _follower_tick_timer = self.replication_timing.follower_tick.timer();
        let worker = self.follower_replication_worker(queue)?;
        let from = worker.lock().await.queue_catch_up_options(cfg)?.from;

        // The apply object advances the worker-state cursor live (per batch), so
        // a fallback/next tick resumes from the right place without an exit sync.
        let apply = Arc::new(WorkerQueuePushApply {
            broker: self.clone(),
            topic: queue.topic.to_string(),
            partition: queue.partition,
            group: queue.group.as_ref().map(|g| g.to_string()),
            worker: worker.clone(),
        });

        // Live tunables: read from the broker's current config snapshot on each
        // apply iteration, so a runtime-settings change takes effect mid-stream
        // (no restart). See the live-settings-propagation principle.
        let tunables = stream_apply_tunables_from(self.cfg.clone());

        let initial_credit = match self.config_snapshot().replication_stream_initial_credit_bytes {
            0 => cfg.max_bytes_per_read as u64,
            credit => credit,
        };

        let exit = owner
            .push_queue_replication_fenced(
                queue.topic.as_str(),
                queue.partition,
                queue.group.as_deref(),
                from,
                initial_credit,
                tunables,
                cfg.stream_buffer_batches,
                apply.clone(),
                shutdown,
                self.assignment_cache
                    .get(&QueueKey {
                        tp: queue.topic.to_string(),
                        part: queue.partition,
                        group: queue.group.clone(),
                    })
                    .map(|a| a.epoch),
            )
            .await?;

        // The worker-state cursor was advanced live by `apply`, so a fallback or
        // next tick already resumes from the right place.
        match exit {
            FollowerStreamExit::Closed => Ok(()),
            FollowerStreamExit::CheckpointRequired => {
                let fallback = FollowerReplicationWorkerConfig {
                    allow_checkpoint_install: true,
                    stream_enabled: false,
                    ..cfg
                };
                let outcome = self
                    .run_follower_replication_worker_once(
                        owner,
                        queue,
                        ReplicationResourceKind::Queue,
                        fallback,
                    )
                    .await?;
                worker.lock().await.record_catch_up(fallback, &outcome);
                Ok(())
            }
            FollowerStreamExit::Error(err) => Err(BrokerError::Unknown(err)),
        }
    }

    pub fn spawn_follower_replication_worker_loop(
        self: &Arc<Self>,
        assignment: PartitionAssignment,
        resolver: Arc<dyn BrokerOwnerReplicationPeerResolver>,
        kind: ReplicationResourceKind,
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
                    .run_follower_replication_worker_loop(assignment, resolver, kind, cfg, shutdown)
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
    fn read_owner_queue_records<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        from: Offset,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>> {
        Box::pin(async move {
            Broker::<StromaEngine>::read_owner_queue_records(
                self,
                topic,
                partition,
                group,
                from,
                max_records,
                max_bytes,
                max_wait_ms,
            )
            .await
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
    ) -> BoxFuture<'a, Result<BrokerOwnerStreamRecords, BrokerError>> {
        Box::pin(async move {
            Broker::<StromaEngine>::read_owner_stream_records(
                self,
                topic,
                partition,
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

    fn export_queue_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<QueueStateCheckpoint, BrokerError>> {
        Box::pin(async move {
            Broker::<StromaEngine>::export_queue_state_checkpoint(self, topic, partition, group)
                .await
        })
    }
}

impl<T> BrokerOwnerReplicationPeer for Arc<T>
where
    T: BrokerOwnerReplicationPeer + ?Sized,
{
    fn read_owner_queue_records<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        from: Offset,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> BoxFuture<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>> {
        self.as_ref().read_owner_queue_records(
            topic,
            partition,
            group,
            from,
            max_records,
            max_bytes,
            max_wait_ms,
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
    ) -> BoxFuture<'a, Result<OwnerReplicationRead<StromaEvent>, BrokerError>> {
        self.as_ref().read_owner_queue_records_fenced(
            topic,
            partition,
            group,
            from,
            max_records,
            max_bytes,
            max_wait_ms,
            reporter_epoch,
        )
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
    ) -> BoxFuture<'a, Result<BrokerOwnerStreamRecords, BrokerError>> {
        self.as_ref().read_owner_stream_records(
            topic,
            partition,
            message_from,
            event_from,
            max_messages,
            max_events,
            max_bytes,
            max_wait_ms,
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
    ) -> BoxFuture<'a, Result<BrokerOwnerStreamRecords, BrokerError>> {
        self.as_ref().read_owner_stream_records_fenced(
            topic,
            partition,
            message_from,
            event_from,
            max_messages,
            max_events,
            max_bytes,
            max_wait_ms,
            reporter_epoch,
        )
    }

    fn export_queue_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
    ) -> BoxFuture<'a, Result<QueueStateCheckpoint, BrokerError>> {
        self.as_ref()
            .export_queue_state_checkpoint(topic, partition, group)
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
    ) -> BoxFuture<'a, Result<FollowerStreamExit, BrokerError>> {
        self.as_ref().push_queue_replication_fenced(
            topic,
            partition,
            group,
            from,
            credit_bytes,
            tunables,
            buffer_batches,
            apply,
            shutdown,
            reporter_epoch,
        )
    }
}

impl<
    E: QueueEngine
        + crate::queue_engine::StreamStore
        + std::fmt::Debug
        + Clone
        + Send
        + Sync
        + 'static,
> Broker<E>
{
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

    pub(crate) fn ensure_follower_replication_worker(
        &self,
        queue: &crate::coordination::QueueIdentity,
        kind: ReplicationResourceKind,
    ) -> Arc<FollowerReplicationWorkerRuntime> {
        self.follower_replication_workers
            .entry(queue.clone())
            .or_insert_with(|| {
                Arc::new(FollowerReplicationWorkerRuntime::new(FollowerCursor::zero(kind)))
            })
            .value()
            .clone()
    }

    pub(crate) async fn stop_follower_replication_worker(
        &self,
        queue: &crate::coordination::QueueIdentity,
    ) -> bool {
        let Some((_, worker)) = self.follower_replication_workers.remove(queue) else {
            return false;
        };
        worker.stop_and_wait().await;
        true
    }

    pub(crate) async fn stop_follower_replication_worker_preserving_state(
        &self,
        queue: &crate::coordination::QueueIdentity,
    ) -> Option<FollowerReplicationWorkerState> {
        let (_, worker) = self.follower_replication_workers.remove(queue)?;
        worker.stop_and_wait().await;
        let state = worker.state.lock().await.clone();
        Some(state)
    }

    pub(crate) async fn stop_all_follower_replication_workers(&self) {
        let queues = self
            .follower_replication_workers
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for queue in queues {
            self.stop_follower_replication_worker(&queue).await;
        }
    }

    pub(crate) fn follower_replication_worker_runtime(
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

    pub(crate) fn follower_replication_worker(
        &self,
        queue: &crate::coordination::QueueIdentity,
    ) -> Result<Arc<AsyncMutex<FollowerReplicationWorkerState>>, BrokerError> {
        Ok(self
            .follower_replication_worker_runtime(queue)?
            .state
            .clone())
    }
}

impl<
    E: QueueEngine
        + crate::queue_engine::StreamStore
        + std::fmt::Debug
        + Clone
        + Send
        + Sync
        + 'static,
> Broker<E>
{
    /// Shareable confirm gate for spawned per-queue confirm loops.
    pub fn replication_confirm_gate(&self) -> ReplicationConfirmGate {
        ReplicationConfirmGate {
            progress: self.replication_progress.clone(),
            assignments: self.assignment_cache.clone(),
            cfg: self.cfg.clone(),
            timing: self.replication_timing.clone(),
        }
    }
}

impl<
    E: QueueEngine
        + crate::queue_engine::StreamStore
        + std::fmt::Debug
        + Clone
        + Send
        + Sync
        + 'static,
> Broker<E>
{
    /// Open a progress session for `follower` under assignment `epoch`. The
    /// session reports only `kind` progress for the rest of its life.
    pub fn begin_replication_progress_session(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        kind: ReplicationResourceKind,
        follower: &str,
        epoch: u64,
    ) -> Option<ReplicationProgressSession> {
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        let assignment_guard = self.assignment_cache.get(&key)?;
        let assignment = assignment_guard.clone();
        // The partition's own kind must match the session kind, so stream
        // progress can never release queue confirms or the reverse. An open
        // stream channel, a stream declared in coordination or the durable
        // kind marker identifies a stream, even before it is materialized.
        let is_stream = group.is_none()
            && (self.is_stream(topic, partition.id())
                || self.stream_declared_in_coordination(topic)
                || self.engine.durable_is_stream(topic, partition.id()));
        let owns = match kind {
            ReplicationResourceKind::Queue => {
                !is_stream && self.owns_queue_partition(topic, partition, group)
            }
            ReplicationResourceKind::Stream => is_stream && self.owns_stream(topic, partition.id()),
        };
        if assignment.epoch != epoch || !assignment.is_followed_by(follower) || !owns {
            return None;
        }
        drop(assignment_guard);
        Some(self.open_progress_session(key, assignment, kind, follower))
    }

    /// Register a fresh session for `follower`, replacing any earlier one.
    /// Callers have checked authority and the partition kind.
    fn open_progress_session(
        &self,
        key: QueueKey,
        assignment: PartitionAssignment,
        kind: ReplicationResourceKind,
        follower: &str,
    ) -> ReplicationProgressSession {
        let cell = self
            .replication_progress
            .entry(key.clone())
            .or_insert_with(|| Arc::new(ReplicationProgressCell::default()))
            .clone();
        let id = cell.next_session.fetch_add(1, Ordering::Relaxed) + 1;
        cell.lock_followers().insert(
            follower.to_string(),
            FollowerProgress {
                progress: DurableProgress::zero(kind),
                written: None,
                last_report: Instant::now(),
                session: id,
            },
        );
        cell.changed.notify_waiters();
        ReplicationProgressSession {
            key,
            assignment,
            follower: follower.to_string(),
            kind,
            cell,
            id,
        }
    }

    /// Record a durable report on `session`. A report whose kind differs from
    /// the session's is dropped.
    pub fn record_replication_session_progress(
        &self,
        session: &ReplicationProgressSession,
        progress: DurableProgress,
    ) {
        self.record_replication_session_written_progress(session, progress, None)
    }

    /// Like `record_replication_session_progress`, with the follower's written
    /// queue log position when it is ahead of the durable one.
    pub fn record_replication_session_written_progress(
        &self,
        session: &ReplicationProgressSession,
        progress: DurableProgress,
        written: Option<Offset>,
    ) {
        if progress.kind() != session.kind {
            tracing::warn!(
                topic = %session.key.tp,
                partition = session.key.part.id(),
                follower = %session.follower,
                ?progress,
                "dropping replication progress of the wrong kind for its session"
            );
            return;
        }
        let Some(assignment_guard) = self.assignment_cache.get(&session.key) else {
            return;
        };
        if !assignment_guard.preserves_replication_session(&session.assignment, &session.follower)
            || self
                .replication_progress
                .get(&session.key)
                .is_none_or(|c| !Arc::ptr_eq(&c, &session.cell))
        {
            return;
        }
        {
            let mut followers = session.cell.lock_followers();
            let Some(p) = followers
                .get_mut(&session.follower)
                .filter(|p| p.session == session.id)
            else {
                return;
            };
            // Reports on a session are ordered. Replace the whole position:
            // taking a maximum could combine progress from before and after
            // a reset.
            p.progress = progress;
            p.written = written;
            p.last_report = Instant::now();
        }
        drop(assignment_guard);
        self.replication_timing.record_follower_progress_report();
        session.cell.changed.notify_waiters();
        self.refresh_visibility_ceiling(&session.key);
    }

    /// Record trusted in-process progress. Network transports must use an
    /// epoch-fenced `ReplicationProgressSession` instead.
    ///
    /// Record a follower's durable replication progress (from a stamped
    /// replication read: followers apply durably, so their pull offsets are
    /// durable watermarks). Wakes any publish confirms waiting on policy. A
    /// report whose kind differs from the follower's recorded progress is
    /// dropped.
    pub fn record_follower_replication_progress(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        follower_node_id: &str,
        progress: DurableProgress,
    ) {
        let key = QueueKey {
            tp: topic.to_string(),
            part: partition,
            group: group.map(str::to_string),
        };
        let cell = self
            .replication_progress
            .entry(key.clone())
            .or_insert_with(|| Arc::new(ReplicationProgressCell::default()))
            .clone();
        {
            let mut followers = cell.lock_followers();
            let entry = followers
                .entry(follower_node_id.to_string())
                .or_insert(FollowerProgress {
                    progress: DurableProgress::zero(progress.kind()),
                    written: None,
                    last_report: std::time::Instant::now(),
                    session: 0,
                });
            if entry.progress.kind() != progress.kind() {
                tracing::warn!(
                    topic,
                    partition = partition.id(),
                    follower = follower_node_id,
                    ?progress,
                    "dropping replication progress of the wrong kind for this follower"
                );
                return;
            }
            entry.progress = progress;
            entry.written = None;
            entry.last_report = std::time::Instant::now();
        }
        self.replication_timing.record_follower_progress_report();
        cell.changed.notify_waiters();
        // Advance the replica-durable visibility ceiling so newly-committed
        // messages become deliverable.
        self.refresh_visibility_ceiling(&key);
    }

    /// Reported follower durable progress for one queue (observability/tests).
    pub fn follower_replication_progress(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Vec<(String, DurableProgress)> {
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
                    .map(|(node, progress)| (node.clone(), progress.progress))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Owner-side replication health: for every queue this broker owns, the
    /// per-follower durable progress, staleness, and in-sync status against the
    /// configured floor. Drives replication-lag / ISR-risk topology views.
    pub(crate) fn owned_replica_observability_report(
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
                            let (durable, last_report_age_ms, in_sync) = match progress {
                                Some(progress) => {
                                    let age = now.duration_since(progress.last_report);
                                    (
                                        Some(progress.progress),
                                        Some(age.as_millis().min(u64::MAX as u128) as u64),
                                        age <= isr_timeout,
                                    )
                                }
                                None => (None, None, false),
                            };
                            FollowerReplicaObservability {
                                node_id: node_id.clone(),
                                progress: durable,
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

    pub(crate) fn follower_replication_observability_report(
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
}

#[cfg(test)]
mod queue_dependency_tests {
    use super::*;
    fn progress(next: u64) -> FollowerProgress {
        FollowerProgress {
            progress: DurableProgress::Queue { next },
            written: None,
            last_report: Instant::now(),
            session: 0,
        }
    }
    fn stream_progress(message_next: u64, event_next: u64) -> FollowerProgress {
        FollowerProgress {
            progress: DurableProgress::Stream {
                message_next,
                event_next,
            },
            written: None,
            last_report: Instant::now(),
            session: 0,
        }
    }
    fn commit(next: u64) -> stroma_core::QueuePublishCommit {
        stroma_core::QueuePublishCommit { next, epoch: 1, buffered: false }
    }
    fn queue(next: u64) -> DurableProgress {
        DurableProgress::Queue { next }
    }
    fn accepted_assignment() -> PartitionAssignment {
        use crate::history_replication::{AcceptedHistory, ReplicaHistoryInstance};
        let mut a = PartitionAssignment::new(
            crate::coordination::QueueIdentity::new("q", 0.into(), None),
            "a",
            vec!["b".into(), "c".into()],
            1,
        );
        a.history = Some(Arc::new(AcceptedHistory {
            activation: [1; 32],
            binding: stroma_core::StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [2; 16],
                writer_session: [3; 16],
            },
            owner: "a".into(),
            replicas: ["a", "b", "c"]
                .into_iter()
                .map(|n| {
                    (
                        n.into(),
                        ReplicaHistoryInstance {
                            process: [1; 16],
                            storage: [2; 16],
                        },
                    )
                })
                .collect(),
            blocked_local_replica: None,
            suspended_replicas: Default::default(),
            replica_generations: Default::default(),
        }));
        a
    }

    #[test]
    fn retention_protects_unknown_and_advancing_followers_but_expires_stalled_pins() {
        let now = Instant::now();
        let mut observation = RetentionObservation {
            assignment: accepted_assignment(),
            since: now,
            followers: Default::default(),
        };
        let mut followers = std::collections::HashMap::new();
        assert_eq!(observation.floor(&followers, now), 0);
        followers.insert("b".into(), progress(20));
        followers.insert("c".into(), progress(16));
        assert_eq!(observation.floor(&followers, now + Duration::from_secs(1)), 16);
        assert_eq!(observation.floor(&followers, now + Duration::from_secs(60)), 16);
        assert_eq!(
            observation.floor(&followers, now + Duration::from_secs(61)),
            u64::MAX
        );
        followers.insert("b".into(), progress(21));
        assert_eq!(observation.floor(&followers, now + Duration::from_secs(62)), 21);
        Arc::make_mut(observation.assignment.history.as_mut().unwrap())
            .suspended_replicas
            .insert("c".into());
        assert_eq!(observation.floor(&followers, now + Duration::from_secs(63)), 0);
        assert_eq!(observation.floor(&followers, now + Duration::from_secs(121)), 21);
    }

    #[test]
    fn retention_never_reads_a_queue_position_from_stream_progress() {
        let now = Instant::now();
        let mut observation = RetentionObservation {
            assignment: accepted_assignment(),
            since: now,
            followers: Default::default(),
        };
        let followers = std::collections::HashMap::from([
            ("b".into(), progress(20)),
            ("c".into(), stream_progress(50, 90)),
        ]);
        assert_eq!(observation.floor(&followers, now), 0);
    }

    #[test]
    fn idle_connected_follower_keeps_retention_when_traffic_resumes() {
        let now=Instant::now();
        let mut observation=RetentionObservation {assignment:accepted_assignment(),since:now,followers:Default::default()};
        let mut followers=std::collections::HashMap::from([("b".into(),progress(9)),("c".into(),progress(9))]);
        assert_eq!(observation.floor(&followers,now),9);
        for progress in followers.values_mut() {progress.last_report=now+Duration::from_secs(300);}
        assert_eq!(observation.floor(&followers,now+Duration::from_secs(301)),9);
    }

    #[tokio::test]
    async fn reseed_suspension_keeps_all_copy_requirement_and_rejects_stale_progress() {
        let dir = stroma_core::test_dir!("reseed_confirmation_requirement");
        let engine = StromaEngine::open(
            &dir.root,
            stroma_core::StromaKeratinConfig::from_message_log(
                stroma_core::KeratinConfig::test_default(),
            ),
            stroma_core::SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let broker = Broker::new(engine, BrokerConfig::default(), None);
        let mut assignment = accepted_assignment();
        assignment.durability =
            crate::coordination::ReplicationDurabilityPolicy::ReplicaDurable { nodes: 3 };
        broker.cache_queue_assignment(&assignment);
        let before=assignment.clone();
        let b = broker.begin_replication_progress_session("q", 0.into(), None, ReplicationResourceKind::Queue, "b", 1).unwrap();
        let old = broker
            .begin_replication_progress_session("q", 0.into(), None, ReplicationResourceKind::Queue, "c", 1)
            .unwrap();
        Arc::make_mut(assignment.history.as_mut().unwrap())
            .suspended_replicas
            .insert("c".into());
        Arc::make_mut(assignment.history.as_mut().unwrap()).replica_generations.insert("c".into(),[8;32]);
        broker.cache_queue_assignment(&assignment);
        broker.record_replication_session_progress(&old, DurableProgress::Queue { next: 10 });
        assert_eq!(broker.follower_replication_progress("q",0.into(),None),vec![("b".into(), queue(0))]);
        broker.record_replication_session_progress(&b, DurableProgress::Queue { next: 10 });
        let key = QueueKey {
            tp: "q".into(),
            part: 0.into(),
            group: None,
        };
        let gate = broker.replication_confirm_gate();
        let wait = gate.await_dependency(&key, 0, Some(commit(1)), Some(before.clone()));
        tokio::pin!(wait);
        assert!(
            futures::poll!(&mut wait).is_pending(),
            "one follower cannot meet the two-follower requirement"
        );
        let mut deps = QueueDependencies::default();
        deps.record(commit(1));
        let followers = std::collections::HashMap::from([
            ("b".into(), progress(10)),
            ("c".into(), progress(10)),
        ]);
        assert_eq!(deps.visibility(&assignment, &followers, 2, false), 0);
        assert_eq!(deps.visibility(&assignment, &followers, 1, false), 1);
        Arc::make_mut(assignment.history.as_mut().unwrap()).suspended_replicas.remove("c");
        broker.cache_queue_assignment(&assignment);
        broker.record_replication_session_progress(&old, DurableProgress::Queue { next: 10 });
        assert!(futures::poll!(&mut wait).is_pending(),"old c session cannot satisfy a waiting confirm after re-admission");
        let c=broker.begin_replication_progress_session("q", 0.into(), None, ReplicationResourceKind::Queue, "c", 1).unwrap();
        broker.record_replication_session_progress(&c, DurableProgress::Queue { next: 10 });
        tokio::time::timeout(Duration::from_secs(1),wait).await.unwrap().unwrap();
        assert_eq!(broker.follower_replication_progress("q",0.into(),None).len(),2);
        broker.shutdown().await;
    }

    #[test]
    fn visibility_uses_same_replica_and_reclaims_committed_boundaries() {
        let a = PartitionAssignment::new(
            crate::coordination::QueueIdentity::new("t", Partition::new(0), None),
            "a",
            vec!["b".into(), "c".into()],
            1,
        );
        let mut deps = QueueDependencies::default();
        deps.record(commit(2));
        deps.record(commit(4));
        let mut followers = std::collections::HashMap::from([
            ("b".into(), progress(3)),
            ("c".into(), progress(1)),
        ]);
        // b holds the first batch and part of the second, so visibility stops
        // at the first boundary instead of the follower position.
        assert_eq!(deps.visibility(&a, &followers, 1, false), 2);
        assert_eq!(deps.boundaries.len(), 1);
        followers.insert("b".into(), progress(4));
        assert_eq!(deps.visibility(&a, &followers, 1, false), 4);
        assert!(deps.boundaries.is_empty());
        deps.record(commit(6));
        followers.insert("b".into(), progress(0));
        assert_eq!(
            deps.visibility(&a, &followers, 1, false),
            4,
            "previously proved prefix survives a reconnect"
        );
        assert_eq!(deps.covered_next(&progress(5), false), 4);
    }

    #[test]
    fn visibility_only_lands_on_publication_boundaries() {
        let a = PartitionAssignment::new(
            crate::coordination::QueueIdentity::new("t", Partition::new(0), None),
            "a",
            vec!["b".into()],
            1,
        );
        let mut deps = QueueDependencies::default();
        for boundary in [3, 7, 12] {
            deps.record(commit(boundary));
        }
        // Recording the same boundary twice keeps one entry.
        deps.record(commit(7));
        assert_eq!(deps.boundaries, [3, 7, 12]);
        let mut seen = Vec::new();
        for next in 0..=13 {
            let followers = std::collections::HashMap::from([("b".into(), progress(next))]);
            let visible = deps.visibility(&a, &followers, 1, false);
            assert!([0, 3, 7, 12].contains(&visible), "{next} exposed {visible}");
            assert!(visible <= next.max(seen.last().copied().unwrap_or(0)));
            seen.push(visible);
        }
        assert_eq!(seen.last(), Some(&12));
    }

    #[test]
    fn stream_progress_covers_no_queue_boundary() {
        let a = PartitionAssignment::new(
            crate::coordination::QueueIdentity::new("t", Partition::new(0), None),
            "a",
            vec!["b".into()],
            1,
        );
        let mut deps = QueueDependencies::default();
        deps.record(commit(2));
        let followers = std::collections::HashMap::from([("b".into(), stream_progress(9, 9))]);
        assert_eq!(deps.visibility(&a, &followers, 1, false), 0);
    }

    #[test]
    fn unavailable_replicas_keep_bounded_conservative_history() {
        let mut deps = QueueDependencies::default();
        for n in 1..=100_000 {
            deps.record(commit(2 * n));
        }
        assert!(deps.boundaries.len() <= 4096);
        // The dropped early boundaries fold into the retained later ones, so a
        // follower at an early boundary proves nothing, conservatively.
        assert_eq!(deps.covered_next(&progress(1), false), 0);
        assert_eq!(deps.covered_next(&progress(200_000), false), 200_000);
        assert_eq!(deps.covered_next(&progress(199_999), false), 199_998);
    }

    #[tokio::test]
    async fn additive_admission_preserves_waiting_confirmation_and_original_progress_session() {
        use crate::history_replication::{AcceptedHistory, ReplicaHistoryInstance};
        let dir = stroma_core::test_dir!("additive_confirmation");
        let engine = StromaEngine::open(
            &dir.root,
            stroma_core::StromaKeratinConfig::from_message_log(
                stroma_core::KeratinConfig::test_default(),
            ),
            stroma_core::SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let broker = Broker::new(engine, BrokerConfig::default(), None);
        let key = QueueKey {
            tp: "q".into(),
            part: Partition::new(0),
            group: None,
        };
        let mut before = PartitionAssignment::new(
            crate::coordination::QueueIdentity::new("q", key.part, None),
            "a",
            vec!["b".into(), "c".into()],
            1,
        );
        before.durability = crate::coordination::ReplicationDurabilityPolicy::MajorityDurable;
        let instance = ReplicaHistoryInstance {
            process: [1; 16],
            storage: [2; 16],
        };
        before.history = Some(Arc::new(AcceptedHistory {
            activation: [1; 32],
            binding: stroma_core::StorageHistoryBinding {
                resource_incarnation: [1; 16],
                accepted_history: [2; 16],
                writer_session: [3; 16],
            },
            owner: "a".into(),
            replicas: std::collections::BTreeMap::from([
                ("a".into(), instance.clone()),
                ("b".into(), instance.clone()),
            ]),
            blocked_local_replica: None,
            suspended_replicas: Default::default(),
            replica_generations: Default::default(),
        }));
        broker.cache_queue_assignment(&before);
        assert!(
            broker
                .begin_replication_progress_session("q", key.part, None, ReplicationResourceKind::Queue, "c", 1)
                .is_none()
        );
        let original = broker
            .begin_replication_progress_session("q", key.part, None, ReplicationResourceKind::Queue, "b", 1)
            .unwrap();
        let gate = broker.replication_confirm_gate();
        let wait = gate.await_dependency(&key, 0, Some(commit(1)), Some(before.clone()));
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        let mut after = before.clone();
        Arc::make_mut(after.history.as_mut().unwrap())
            .replicas
            .insert("c".into(), instance);
        assert!(after.preserves_replication_contract(&before));
        assert!(!before.preserves_replication_contract(&after));
        broker.cache_queue_assignment(&after);
        broker.record_replication_session_progress(&original, DurableProgress::Queue { next: 1 });
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap()
            .unwrap();
        assert!(
            broker
                .begin_replication_progress_session("q", key.part, None, ReplicationResourceKind::Queue, "c", 1)
                .is_some()
        );
        for mutation in 0..5 {
            let mut bad = after.clone();
            match mutation {
                0 => bad.epoch += 1,
                1 => bad.owner = "c".into(),
                2 => {
                    bad.durability = crate::coordination::ReplicationDurabilityPolicy::LocalDurable
                }
                3 => {
                    Arc::make_mut(bad.history.as_mut().unwrap())
                        .replicas
                        .get_mut("a")
                        .unwrap()
                        .storage = [9; 16]
                }
                _ => Arc::make_mut(bad.history.as_mut().unwrap()).activation = [9; 32],
            }
            assert!(!bad.preserves_replication_contract(&before));
        }
        // A follower re-admitted under a new instance keeps the contract, but
        // proof from its earlier sessions is gone. Other sessions keep theirs.
        let mut rebound = after.clone();
        Arc::make_mut(rebound.history.as_mut().unwrap())
            .replicas
            .get_mut("b")
            .unwrap()
            .storage = [9; 16];
        assert!(rebound.preserves_replication_contract(&after));
        assert!(!rebound.preserves_replication_session(&after, "b"));
        assert!(rebound.preserves_replication_session(&after, "c"));
        let mut dropped = after.clone();
        Arc::make_mut(dropped.history.as_mut().unwrap()).replicas.remove("b");
        assert!(!dropped.preserves_replication_contract(&after));
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn restarted_follower_counts_only_after_readmission_under_its_new_process() {
        let dir = stroma_core::test_dir!("restarted_follower_readmission");
        let engine = StromaEngine::open(
            &dir.root,
            stroma_core::StromaKeratinConfig::from_message_log(
                stroma_core::KeratinConfig::test_default(),
            ),
            stroma_core::SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let broker = Broker::new(engine, BrokerConfig::default(), None);
        let key = QueueKey { tp: "q".into(), part: 0.into(), group: None };
        // Three copies, majority: the owner plus one follower confirm.
        let mut assignment = accepted_assignment();
        assignment.durability = crate::coordination::ReplicationDurabilityPolicy::MajorityDurable;
        broker.cache_queue_assignment(&assignment);
        let before = assignment.clone();
        let old_c = broker
            .begin_replication_progress_session("q", 0.into(), None, ReplicationResourceKind::Queue, "c", 1)
            .unwrap();
        let b = broker
            .begin_replication_progress_session("q", 0.into(), None, ReplicationResourceKind::Queue, "b", 1)
            .unwrap();
        let gate = broker.replication_confirm_gate();
        let wait = gate.await_dependency(&key, 0, Some(commit(1)), Some(before.clone()));
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());

        // c restarted: suspended until re-admitted. Its old session is dead.
        let history = Arc::make_mut(assignment.history.as_mut().unwrap());
        history.suspended_replicas.insert("c".into());
        broker.cache_queue_assignment(&assignment);
        broker.record_replication_session_progress(&old_c, DurableProgress::Queue { next: 10 });
        assert!(futures::poll!(&mut wait).is_pending(), "suspended c cannot confirm");
        assert!(
            broker
                .begin_replication_progress_session("q", 0.into(), None, ReplicationResourceKind::Queue, "c", 1)
                .is_none()
        );

        // Re-admission names the new process and storage. The waiting publish
        // keeps its contract, and stale progress from c's old process still
        // does not count.
        let history = Arc::make_mut(assignment.history.as_mut().unwrap());
        history.suspended_replicas.remove("c");
        history.replicas.insert(
            "c".into(),
            crate::history_replication::ReplicaHistoryInstance { process: [7; 16], storage: [8; 16] },
        );
        broker.cache_queue_assignment(&assignment);
        broker.record_replication_session_progress(&old_c, DurableProgress::Queue { next: 10 });
        assert!(futures::poll!(&mut wait).is_pending(), "old c session stays fenced");
        let new_c = broker
            .begin_replication_progress_session("q", 0.into(), None, ReplicationResourceKind::Queue, "c", 1)
            .unwrap();
        broker.record_replication_session_progress(&new_c, DurableProgress::Queue { next: 10 });
        tokio::time::timeout(Duration::from_secs(1), wait).await.unwrap().unwrap();
        // b's session opened before the re-admission still reports.
        broker.record_replication_session_progress(&b, DurableProgress::Queue { next: 3 });
        let reported = broker.follower_replication_progress("q", 0.into(), None);
        assert!(reported.contains(&("b".into(), queue(3))), "{reported:?}");
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn progress_session_kind_is_fixed_and_never_crosses_confirm_kinds() {
        let dir = stroma_core::test_dir!("progress_session_kind");
        let engine = StromaEngine::open(
            &dir.root,
            stroma_core::StromaKeratinConfig::from_message_log(
                stroma_core::KeratinConfig::test_default(),
            ),
            stroma_core::SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let broker = Broker::new(engine, BrokerConfig::default(), None);
        let assignment = PartitionAssignment::new(
            crate::coordination::QueueIdentity::new("q", Partition::new(0), None),
            "a",
            vec!["b".into()],
            1,
        )
        .with_durability(crate::coordination::ReplicationDurabilityPolicy::MajorityDurable);
        broker.cache_queue_assignment(&assignment);
        let key = QueueKey {
            tp: "q".into(),
            part: Partition::new(0),
            group: None,
        };
        let gate = broker.replication_confirm_gate();
        // A queue partition never opens a stream session.
        assert!(
            broker
                .begin_replication_progress_session(
                    "q",
                    Partition::new(0),
                    None,
                    ReplicationResourceKind::Stream,
                    "b",
                    1,
                )
                .is_none()
        );
        // A stream partition never opens a queue session.
        crate::queue_engine::StreamStore::create_stream(&broker.engine(), "s", 0, None)
            .await
            .unwrap();
        broker.cache_queue_assignment(&PartitionAssignment::new(
            crate::coordination::QueueIdentity::new("s", Partition::new(0), None),
            "a",
            vec!["b".into()],
            1,
        ));
        for (kind, opens) in [
            (ReplicationResourceKind::Queue, false),
            (ReplicationResourceKind::Stream, true),
        ] {
            assert_eq!(
                broker
                    .begin_replication_progress_session("s", Partition::new(0), None, kind, "b", 1)
                    .is_some(),
                opens,
                "{kind:?}"
            );
        }
        // Force a stream session on the queue anyway to check that the confirm
        // gate still keeps the kinds apart if a session were ever misrouted.
        let stream_session = broker.open_progress_session(
            key.clone(),
            assignment.clone(),
            ReplicationResourceKind::Stream,
            "b",
        );
        assert_eq!(stream_session.kind(), ReplicationResourceKind::Stream);
        broker.record_replication_session_progress(
            &stream_session,
            DurableProgress::Stream {
                message_next: 100,
                event_next: 100,
            },
        );
        let queue_wait = gate.await_dependency(&key, 0, Some(commit(1)), Some(assignment.clone()));
        tokio::pin!(queue_wait);
        assert!(
            futures::poll!(&mut queue_wait).is_pending(),
            "stream progress cannot release a queue record"
        );
        // A queue report on a stream session is dropped and changes nothing.
        broker.record_replication_session_progress(&stream_session, queue(100));
        assert!(futures::poll!(&mut queue_wait).is_pending());
        assert_eq!(
            broker.follower_replication_progress("q", Partition::new(0), None),
            vec![(
                "b".to_string(),
                DurableProgress::Stream {
                    message_next: 100,
                    event_next: 100
                }
            )]
        );
        // Stream confirms count records only, never cursor events.
        gate.await_stream_confirm(&key, 0).await.unwrap();
        broker.record_replication_session_progress(
            &stream_session,
            DurableProgress::Stream {
                message_next: 0,
                event_next: 50,
            },
        );
        let stream_wait = gate.await_stream_confirm(&key, 0);
        tokio::pin!(stream_wait);
        assert!(
            futures::poll!(&mut stream_wait).is_pending(),
            "cursor events past the records cannot release a stream record"
        );
        // A queue session replaces the stream one for this follower. The old
        // session can no longer report, and queue progress releases the queue
        // record but not the stream one.
        let queue_session = broker
            .begin_replication_progress_session(
                "q",
                Partition::new(0),
                None,
                ReplicationResourceKind::Queue,
                "b",
                1,
            )
            .unwrap();
        broker.record_replication_session_progress(
            &stream_session,
            DurableProgress::Stream {
                message_next: 100,
                event_next: 100,
            },
        );
        assert_eq!(
            broker.follower_replication_progress("q", Partition::new(0), None),
            vec![("b".to_string(), queue(0))]
        );
        broker.record_replication_session_progress(&queue_session, queue(1));
        tokio::time::timeout(Duration::from_secs(1), queue_wait)
            .await
            .unwrap()
            .unwrap();
        assert!(futures::poll!(&mut stream_wait).is_pending());
        // A follower one record short of the publication end never confirms.
        let short_wait = gate.await_dependency(&key, 0, Some(commit(2)), Some(assignment.clone()));
        tokio::pin!(short_wait);
        assert!(futures::poll!(&mut short_wait).is_pending());
        broker.record_replication_session_progress(&queue_session, queue(2));
        tokio::time::timeout(Duration::from_secs(1), short_wait)
            .await
            .unwrap()
            .unwrap();
        // A commit written under another epoch is refused outright.
        assert!(
            gate.await_dependency(
                &key,
                0,
                Some(stroma_core::QueuePublishCommit {
                    epoch: 2,
                    ..commit(2)
                }),
                Some(assignment.clone()),
            )
            .await
            .is_err()
        );
        drop(stream_wait);
        broker.shutdown().await;
    }

    #[tokio::test]
    async fn removed_assignment_cannot_become_a_local_confirmation() {
        let assignments = Arc::new(DashMap::new());
        let key = QueueKey {
            tp: "t".into(),
            part: Partition::new(0),
            group: None,
        };
        let a = PartitionAssignment::new(
            crate::coordination::QueueIdentity::new("t", key.part, None),
            "a",
            vec!["b".into()],
            1,
        );
        let gate = ReplicationConfirmGate {
            assignments,
            progress: Arc::new(DashMap::new()),
            cfg: Arc::new(ArcSwap::from_pointee(BrokerConfig::default())),
            timing: Arc::new(ReplicationTimingMetrics::default()),
        };
        assert!(
            gate.await_dependency(&key, 0, Some(commit(1)), Some(a))
                .await
                .is_err()
        );
    }
}

impl Broker<StromaEngine> {
    pub fn queue_reseed_revocation_applied(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
        node: &str,
        generation: [u8; 32],
    ) -> bool {
        let key = QueueKey {
            tp: topic.into(),
            part: partition.into(),
            group: group.map(str::to_owned),
        };
        let Some(a) = self.assignment_cache.get(&key) else {
            return false;
        };
        if !a.history.as_ref().is_some_and(|h| {
            h.suspended_replicas.contains(node)
                && h.replica_generations.get(node) == Some(&generation)
        }) {
            return false;
        }
        self.replication_progress
            .get(&key)
            .is_none_or(|p| !p.lock_followers().contains_key(node))
    }
    pub async fn stop_queue_replication_for_reseed(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
    ) {
        self.stop_follower_replication_worker(&crate::coordination::QueueIdentity::new(
            topic,
            partition.into(),
            group,
        ))
        .await;
    }
}

