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
use crate::queue_engine::{OwnerStateCheckpoint, ReplicatedQueueApplyOutcome, StromaEngine};

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
