//! Replication / clustering types and traits for the broker.
//!
//! First brick of the clustering-module separation: the replication data types,
//! peer/resolver traits, catch-up + stream outcome enums, and the stream-apply
//! trait, lifted out of broker.rs. Re-exported from `broker` so existing
//! `fibril_broker::broker::*` import paths keep resolving.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;

use fibril_storage::{Offset, Partition};
use stroma_core::{Message, OwnerReplicationRead, StromaEvent};

use crate::broker::{Broker, BrokerError};
use crate::coordination::PartitionAssignment;
use crate::queue_engine::{
    OwnerStateCheckpoint, ReplicatedAppendOutcome, ReplicatedQueueApplyOutcome, StromaEngine,
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
