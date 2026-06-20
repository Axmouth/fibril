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
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

use fibril_storage::{Offset, Partition};
use stroma_core::{Message, OwnerReplicationRead, StromaEvent};

use crate::broker::{Broker, BrokerConfig, BrokerError, FOLLOWER_STREAM_BUFFER_BATCHES, QueueKey};
use crate::coordination::PartitionAssignment;
use crate::queue_engine::{
    FollowerStateCheckpointInstall, FollowerStateCheckpointInstallOutcome, KDurability,
    OwnerStateCheckpoint, QueueEngine, QueuePromotionOutcome, ReplicatedAppendOutcome,
    ReplicatedEventBatch, ReplicatedMessageBatch, ReplicatedQueueApplyOutcome, StromaEngine,
};

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

    /// Run a credit-based replication stream from this owner, applying batches
    /// through `apply` until the stream ends. The default reports it unsupported
    /// so the follower worker falls back to pull. Streaming peers override it.
    fn stream_replication<'a>(
        &'a self,
        _topic: &'a str,
        _partition: Partition,
        _group: Option<&'a str>,
        _message_from: Offset,
        _event_from: Offset,
        _credit_bytes: u64,
        _keepalive_ms: u64,
        _apply_linger_us: u64,
        _buffer_batches: usize,
        _apply: Arc<dyn BrokerReplicationStreamApply>,
        _shutdown: CancellationToken,
    ) -> BoxFuture<'a, Result<FollowerStreamExit, BrokerError>> {
        Box::pin(async {
            Err(BrokerError::Unknown(
                "replication streaming not supported by this peer".into(),
            ))
        })
    }
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
            max_messages_per_read: 2048,
            max_events_per_read: 2048,
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

/// Outcome of [`Broker::apply_replicated_stream_batch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicatedStreamApply {
    /// Applied durably; the advanced durable cursor.
    Applied { message_next: Offset, event_next: Offset },
    /// A checkpoint is required (offset gap / epoch fence); the stream caller
    /// should fall back to the pull + checkpoint path, then re-stream.
    CheckpointRequired,
}

/// Why a follower replication stream ended (returned by
/// [`BrokerOwnerReplicationPeer::stream_replication`]).
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

/// Durable apply for one streamed batch, provided by the follower worker to a
/// streaming peer. Lets the protocol transport apply through the broker without
/// the broker depending on the protocol crate (mirrors the resolver injection).
pub trait BrokerReplicationStreamApply: Send + Sync {
    fn apply_stream_batch<'a>(
        &'a self,
        records: BrokerOwnerReplicationRecords,
    ) -> BoxFuture<'a, Result<ReplicatedStreamApply, BrokerError>>;
}

// ---------------- Follower replication worker types ----------------

/// Stream-apply adapter the follower worker hands to a streaming peer: applies
/// each batch through the broker and advances the worker's reported cursor so the
/// stream-to-pull fallback resumes from the right place and the worker's reported
/// cursor stays current during a long-lived stream.
pub(crate) struct WorkerStreamApply {
    pub(crate) broker: Arc<Broker<StromaEngine>>,
    pub(crate) topic: String,
    pub(crate) partition: Partition,
    pub(crate) group: Option<String>,
    pub(crate) worker: Arc<AsyncMutex<FollowerReplicationWorkerState>>,
}

impl BrokerReplicationStreamApply for WorkerStreamApply {
    fn apply_stream_batch<'a>(
        &'a self,
        records: BrokerOwnerReplicationRecords,
    ) -> BoxFuture<'a, Result<ReplicatedStreamApply, BrokerError>> {
        Box::pin(async move {
            let outcome = self
                .broker
                .apply_replicated_stream_batch(
                    &self.topic,
                    self.partition,
                    self.group.as_deref(),
                    records,
                )
                .await?;
            if let ReplicatedStreamApply::Applied {
                message_next,
                event_next,
            } = outcome
            {
                let mut state = self.worker.lock().await;
                state.message_next_offset = message_next;
                state.event_next_offset = event_next;
                state.status = FollowerReplicationWorkerStatus::CaughtUp;
            }
            Ok(outcome)
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
    pub(crate) fn new(message_next_offset: Offset, event_next_offset: Offset) -> Self {
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
        while self.active_ticks.load(Ordering::Acquire) != 0 {
            self.idle.notified().await;
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

pub(crate) fn owner_replication_records_empty(records: &BrokerOwnerReplicationRecords) -> bool {
    fn read_empty<T>(read: &OwnerReplicationRead<T>) -> bool {
        matches!(read, OwnerReplicationRead::Batch(batch) if batch.records.is_empty())
    }

    read_empty(&records.messages) && read_empty(&records.events)
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

/// One follower's last-reported durable progress, with the report time used to
/// decide in-sync membership (a follower that stopped reporting falls out).
#[derive(Debug, Clone, Copy)]
pub(crate) struct FollowerProgress {
    pub(crate) message_next: Offset,
    pub(crate) event_next: Offset,
    pub(crate) last_report: std::time::Instant,
}

/// Follower durable progress for one queue, plus a waiter wake-up.
#[derive(Debug, Default)]
pub struct ReplicationProgressCell {
    /// follower node id -> last-reported durable progress
    pub(crate) followers: std::sync::Mutex<std::collections::HashMap<String, FollowerProgress>>,
    pub(crate) changed: Notify,
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

        assert_eq!(messages.next_offset, 11);
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
        let _owner_read_timer = self.replication_timing.owner_read.timer();
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

    /// Apply one streamed replication batch and report the advanced durable
    /// cursor, reusing the exact apply + progress path the pull catch-up uses.
    /// `CheckpointRequired` tells the stream caller to fall back to the pull +
    /// checkpoint path (offset gap / epoch fence), then re-stream.
    pub async fn apply_replicated_stream_batch(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        records: BrokerOwnerReplicationRecords,
    ) -> Result<ReplicatedStreamApply, BrokerError> {
        let message_progress = owner_read_batch_progress("message", &records.messages)?;
        let event_progress = owner_read_batch_progress("event", &records.events)?;
        let (message_progress, event_progress) = match (message_progress, event_progress) {
            (Some(message_progress), Some(event_progress)) => (message_progress, event_progress),
            // The owner read itself signalled a checkpoint is required.
            _ => {
                let _ = self
                    .apply_follower_replication_records(topic, partition, group, records)
                    .await?;
                return Ok(ReplicatedStreamApply::CheckpointRequired);
            }
        };

        let apply = {
            let _follower_apply_timer = self.replication_timing.follower_apply.timer();
            self.apply_follower_replication_records(topic, partition, group, records)
                .await?
        };

        let outcome = match apply {
            BrokerFollowerReplicationApply::Applied(outcome) => outcome,
            BrokerFollowerReplicationApply::CheckpointRequired { .. } => {
                return Ok(ReplicatedStreamApply::CheckpointRequired);
            }
        };

        let message_next = match replicated_append_progress_after_apply(
            "message",
            &message_progress,
            outcome.message_log.as_ref(),
        )? {
            ReplicatedAppendProgress::Advanced(next_offset) => next_offset,
            ReplicatedAppendProgress::CheckpointRequired(_) => {
                return Ok(ReplicatedStreamApply::CheckpointRequired);
            }
        };
        let event_next = match replicated_append_progress_after_apply(
            "event",
            &event_progress,
            outcome.event_log.as_ref(),
        )? {
            ReplicatedAppendProgress::Advanced(next_offset) => next_offset,
            ReplicatedAppendProgress::CheckpointRequired(_) => {
                return Ok(ReplicatedStreamApply::CheckpointRequired);
            }
        };

        Ok(ReplicatedStreamApply::Applied {
            message_next,
            event_next,
        })
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
            let records = {
                let _follower_owner_read_timer =
                    self.replication_timing.follower_owner_read.timer();
                owner
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
                        self.apply_follower_replication_records(topic, partition, group, records)
                            .await?
                    };
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

            let apply = {
                let _follower_apply_timer = self.replication_timing.follower_apply.timer();
                self.apply_follower_replication_records(topic, partition, group, records)
                    .await?
            };

            match apply {
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
        let _follower_tick_timer = self.replication_timing.follower_tick.timer();
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
        self: &Arc<Self>,
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
                    stream_enabled: snap.replication_stream_enabled,
                    stream_apply_linger_us: snap.replication_stream_apply_linger_us,
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
                outcome = async {
                    if cfg.stream_enabled {
                        self.run_follower_replication_stream_tick(
                            owner.as_ref(),
                            &assignment.queue,
                            cfg,
                            runtime.shutdown.clone(),
                        )
                        .await
                    } else {
                        self.run_follower_replication_worker_once(
                            owner.as_ref(),
                            &assignment.queue,
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

    /// One streaming catch-up: open a credit-based stream from the owner, apply
    /// through the broker, and on a checkpoint requirement fall back to the proven
    /// pull+checkpoint path for one tick (then the loop re-streams). The cursor
    /// starts from and is synced back to the worker state, so pull and stream
    /// share one authoritative cursor.
    async fn run_follower_replication_stream_tick(
        self: &Arc<Self>,
        owner: &dyn BrokerOwnerReplicationPeer,
        queue: &crate::coordination::QueueIdentity,
        cfg: FollowerReplicationWorkerConfig,
        shutdown: CancellationToken,
    ) -> Result<(), BrokerError> {
        let _follower_tick_timer = self.replication_timing.follower_tick.timer();
        let worker = self.follower_replication_worker(queue)?;
        let (message_from, event_from) = {
            let state = worker.lock().await;
            (state.message_next_offset, state.event_next_offset)
        };

        // The apply object advances the worker-state cursor live (per batch), so
        // a fallback/next tick resumes from the right place without an exit sync.
        let apply = Arc::new(WorkerStreamApply {
            broker: self.clone(),
            topic: queue.topic.to_string(),
            partition: queue.partition,
            group: queue.group.as_ref().map(|g| g.to_string()),
            worker: worker.clone(),
        });

        let exit = owner
            .stream_replication(
                queue.topic.as_str(),
                queue.partition,
                queue.group.as_deref(),
                message_from,
                event_from,
                cfg.max_bytes_per_read as u64,
                cfg.caught_up_poll_ms,
                cfg.stream_apply_linger_us,
                FOLLOWER_STREAM_BUFFER_BATCHES,
                apply.clone(),
                shutdown,
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
                    .run_follower_replication_worker_once(owner, queue, fallback)
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

impl<E: QueueEngine + std::fmt::Debug + Clone + Send + Sync + 'static> Broker<E> {
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
    ) -> Arc<FollowerReplicationWorkerRuntime> {
        self.follower_replication_workers
            .entry(queue.clone())
            .or_insert_with(|| Arc::new(FollowerReplicationWorkerRuntime::new(0, 0)))
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
