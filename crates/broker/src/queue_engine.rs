use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::storage::Offset;
use async_trait::async_trait;
use fibril_metrics::QueuesStateSnapshot;
use fibril_util::{UnixMillis, unix_millis};
use std::collections::HashSet;
use stroma_core::{
    AckEventMeta, CompletionPair, NackEventMeta, PublishItem, StromaDebugSnapshot, StromaMetrics,
};
pub use stroma_core::{
    AdaptiveStagingConfig, AppendCompletion, DLQDiscardPolicyWire, DeclareMeta, DestroyOutcome,
    DiskUsedBreakdownEntry, RecoveryDiskUsage, EmptyBufferResize, EmptyQueueRemoval, EncodedEvent, EnqueuedStreamAppend, EvictOutcome,
    GlobalDLQ,
    GlobalDlqSnapshot, GlobalDlqUpdateOutcome, GlobalKey, GlobalStore, GlobalValue, InspectMode,
    IoError, KDurability, KeratinAppendCompletion, KeratinConfig, LogRuntimeConfig,
    LogRuntimeSettings, LogRuntimeSnapshot, LogRuntimeStatus, Message, MessageContentType,
    MessageHeaders, MessageInspectionPage, MessageInspectionStatus, OwnerReplicationBatch,
    OwnerReplicationRead, PartitionKind, PreparedQueueRecovery,
    QueueCheckpointBase, QueueCheckpointPin, QueueCheckpointTarget, QueueCheckpointContents,
    QueueCheckpointCapsule, QueueCheckpointBuildLimits, QueueLogSyncStatus,
    PreparedStorageHistory, PutOutcome, QuarantineInfo, QueueInspectionState,
    QueueDemotionOutcome, QueuePromotionOutcome, QueuePublishCommit, QueueRecoveryStage,
    QueueRecoveryStageReceipt, QueueRecoveryStageSpec, QueueStateCheckpoint,
    QueueStateCheckpointInstallOutcome, StreamPromotionOutcome,
    RecoveryMismatchPolicy, RecoveryStageLimits, ReplicatedAppendOutcome, ReplicatedEventBatch,
    ReplicatedMessageBatch, ReplicatedStreamApplyOutcome, RetentionConfig, SnapshotConfig,
    StorageHistoryBinding, Stroma, StromaError, StromaEvent, StromaKeratinConfig,
};
use tokio::sync::Notify;

/// When a queue confirms and delivers a record. Fixed when the queue is first
/// declared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueDurability {
    /// After the record is synced to disk on every required copy.
    #[default]
    Durable,
    /// After the record is written to the OS on every required copy, before
    /// it is synced. Power loss or a failed sync on every required copy at
    /// once can lose recent confirmed records.
    Buffered,
}

impl QueueDurability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Durable => "durable",
            Self::Buffered => "buffered",
        }
    }
}

/// Queue settings as the cluster stores them: the Stroma declaration plus the
/// queue's durability mode. The mode sits beside the flattened declaration,
/// so documents written before it existed still read, as durable queues.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct QueueDeclaration {
    #[serde(flatten)]
    pub meta: DeclareMeta,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub durability: Option<QueueDurability>,
}

impl From<DeclareMeta> for QueueDeclaration {
    fn from(meta: DeclareMeta) -> Self {
        Self::new(meta, None)
    }
}

impl QueueDeclaration {
    pub fn new(meta: DeclareMeta, durability: Option<QueueDurability>) -> Self {
        Self { meta, durability }
    }

    /// The stored mode. A declaration without one is durable.
    pub fn mode(&self) -> QueueDurability {
        self.durability.unwrap_or_default()
    }
}

/// Why a local queue declare failed.
#[derive(Debug, thiserror::Error)]
pub enum LocalDeclareError {
    /// The request names a durability mode other than the queue's own.
    #[error("{0}")]
    DurabilityConflict(String),
    #[error(transparent)]
    Storage(#[from] StromaError),
}

/// Declare one queue partition on a broker without coordination. The first
/// declare, while the partition log is still empty, sets the durability mode
/// from `durability`, or durable when it is absent. A later declare naming
/// another mode fails before anything is written. Returns the mode in effect.
pub async fn declare_local_queue_partition<E: QueueEngine + Send + Sync + ?Sized>(
    engine: &E,
    tp: &str,
    part: u32,
    group: Option<&str>,
    meta: DeclareMeta,
    durability: Option<QueueDurability>,
) -> Result<QueueDurability, LocalDeclareError> {
    let fresh = engine.current_next_offset(tp, part, group).await? == 0;
    let current = if engine.queue_is_buffered(tp, part, group) {
        QueueDurability::Buffered
    } else {
        QueueDurability::Durable
    };
    if let Some(requested) = durability
        && !fresh
        && requested != current
    {
        return Err(LocalDeclareError::DurabilityConflict(format!(
            "queue `{tp}` partition {part} is already declared {} (requested {}); the durability mode is fixed when a queue is first declared",
            current.as_str(),
            requested.as_str()
        )));
    }
    engine.declare_queue(tp, part, group, meta).await?;
    if !fresh {
        return Ok(current);
    }
    let mode = durability.unwrap_or_default();
    if mode != current {
        engine
            .set_queue_buffered(tp, part, group, mode == QueueDurability::Buffered)
            .await?;
    }
    Ok(mode)
}

pub struct Deliverable {
    pub published: u64,
    pub publish_received: u64,
    pub content_type: Option<MessageContentType>,
    pub extra_headers: HashMap<String, String>,
    pub retries: u32,
    pub offset: Offset,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub enum SettleKind {
    Ack,
    Nack {
        requeue: bool,
        not_before: Option<UnixMillis>,
    },
}

pub struct SettleRequest {
    pub offset: Offset,
    pub kind: SettleKind,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayDeadLettersReport {
    pub requested: usize,
    pub replayed: usize,
    pub items: Vec<ReplayDeadLetterItem>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayDeadLetterItem {
    pub offset: Offset,
    pub outcome: ReplayDeadLetterOutcome,
    pub target_topic: Option<String>,
    pub target_group: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayDeadLetterOutcome {
    Replayed,
    Skipped,
}

#[async_trait]
pub trait QueueEngine {
    fn set_queue_replication_retention(
        &self,
        _source: Arc<dyn stroma_core::QueueReplicationRetention>,
    ) {
    }
    /// Lease up to `max` ready messages with offset strictly below `upper`. For a
    /// replica-durable queue `upper` is the committed-replicated watermark, so a
    /// consumer never sees an offset that is not yet durable on enough replicas.
    /// Pass `u64::MAX` to disable the ceiling (local-durable queues).
    async fn poll_ready(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        max: usize,
        lease_deadline: UnixMillis,
        upper: Offset,
    ) -> Result<Vec<Deliverable>, StromaError>;

    async fn ack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn ack_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn nack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        requeue: bool,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn nack_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<NackEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn release_inflight_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn settle(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        req: SettleRequest,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn publish(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        headers: &MessageHeaders,
        payload: Vec<u8>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn publish_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<PublishItem>,
    ) -> Result<(), StromaError>;

    /// Queue publications must expose their enqueue dependency to the broker.
    async fn publish_batch_observed(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<PublishItem>,
        observer: stroma_core::QueuePublishObserver,
    ) -> Result<(), StromaError>;

    /// End of the single queue log and its epoch. `next` is the staged next
    /// offset, which can be ahead of the durable frontier, so a dependency on it
    /// can only wait longer, never confirm early.
    async fn queue_durable_commit(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<stroma_core::QueuePublishCommit, StromaError>;

    async fn next_expiry_hint(&self) -> Result<Option<UnixMillis>, StromaError>;

    async fn requeue_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>, StromaError>;

    /// Drop ready messages past their TTL (message age-drop), routing them through
    /// the DLQ/discard pipeline. Distinct from `requeue_expired`, which re-readies
    /// lease-timed-out work.
    async fn drop_ttl_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>, StromaError>;

    async fn shutdown(&self) -> Result<(), StromaError>;

    async fn estimate_disk_used(&self) -> Result<u64, StromaError>;

    /// Disk use per queue (topic + group), split into message-log and
    /// event-log bytes. The same walk as `estimate_disk_used`, so callers
    /// wanting both should take the breakdown and sum it.
    async fn estimate_disk_used_breakdown(
        &self,
    ) -> Result<Vec<DiskUsedBreakdownEntry>, StromaError>;

    /// Optional read-only node storage sample. Implementations keep filesystem
    /// traversal off async workers. The admin surface caches these samples.
    async fn recovery_disk_usage(&self) -> Result<Option<RecoveryDiskUsage>, StromaError> {
        Ok(None)
    }

    /// Every partition known on disk (topic, partition, group), materialized or
    /// not. Includes partitions indexed at open that no command has materialized,
    /// so a caller can reconcile on-disk state against coordination.
    async fn list_partitions(&self) -> Result<Vec<(String, u32, Option<String>)>, StromaError>;

    async fn queue_stats_snapshot(&self) -> Result<QueuesStateSnapshot, StromaError>;

    async fn debug_snapshot(&self) -> Result<StromaDebugSnapshot, StromaError>;

    async fn inspect_messages(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        limit: usize,
        mode: InspectMode,
        include_payload: bool,
        payload_limit_bytes: usize,
    ) -> Result<MessageInspectionPage, StromaError>;

    async fn replay_dead_letters(
        &self,
        dlq_tp: &str,
        dlq_group: Option<&str>,
        offsets: &[Offset],
    ) -> Result<ReplayDeadLettersReport, StromaError>;

    async fn global_dlq(&self) -> Result<GlobalDlqSnapshot, StromaError>;

    /// The node's versioned global key-value store.
    async fn global_store(&self) -> Result<Arc<GlobalStore>, StromaError>;

    async fn set_global_dlq(
        &self,
        target: Option<GlobalDLQ>,
        expected_version: u64,
    ) -> Result<GlobalDlqUpdateOutcome, StromaError>;

    async fn declare_queue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        meta: DeclareMeta,
    ) -> Result<(), StromaError>;

    /// Store whether a queue partition is buffered. Engines without buffered
    /// queues refuse to turn it on.
    async fn set_queue_buffered(
        &self,
        tp: &str,
        part: u32,
        _group: Option<&str>,
        buffered: bool,
    ) -> Result<(), StromaError> {
        if buffered {
            return Err(StromaError::InvalidArgument(format!(
                "{tp}/{part}: this engine has no buffered queues"
            )));
        }
        Ok(())
    }

    /// Whether a queue partition is buffered. Known once its log has opened.
    fn queue_is_buffered(&self, _tp: &str, _part: u32, _group: Option<&str>) -> bool {
        false
    }

    async fn materialize(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError>;

    async fn ensure_queue_owner_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: Option<u64>,
    ) -> Result<(), StromaError>;

    async fn become_queue_owner_with_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<(), StromaError>;

    async fn unmaterialize(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<EvictOutcome, StromaError>;

    /// Fully remove a partition: drop it from the registry AND delete its
    /// on-disk storage. Stronger than `unmaterialize` (which keeps the data).
    /// Used to free storage for partitions a repartition has retired.
    async fn destroy_partition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<DestroyOutcome, StromaError>;

    fn is_materialized(&self, tp: &str, part: u32, group: Option<&str>) -> bool;

    async fn has_inflight(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<bool, StromaError>;

    /// The lowest offset not yet settled (acked): every offset below it is
    /// consumed and gone. Used by live repartitioning to tell when a partition
    /// has drained its pre-cutover backlog (settled offset has reached the
    /// cutover boundary).
    async fn lowest_unsettled_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset, StromaError>;

    /// The partition's next write offset (high-water). Live repartitioning
    /// snapshots this at cutover as a partition's boundary: messages below it are
    /// pre-cutover (v_old), at or above it are post-cutover (v_new).
    async fn current_next_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset, StromaError>;

    /// The next deliverable offset in `[from, upper)`, or `None` if the range has
    /// nothing ready. A shrink uses this to hold a surviving partition's
    /// post-cutover delivery (offset >= boundary) WITHOUT polling/leasing it:
    /// probe `[0, boundary)` and hold when it is empty.
    async fn next_deliverable(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        upper: Offset,
    ) -> Result<Option<Offset>, StromaError>;

    fn metrics(&self) -> Arc<StromaMetrics>;

    fn deadline_awaker(&self) -> Arc<Notify>;

    /// Partitions parked because recovery found a dangling event->message
    /// reference (for health/admin surfacing).
    fn quarantined_partitions(&self) -> Vec<QuarantineInfo>;

    /// The configured recovery dangling-reference policy (decides whether a
    /// quarantine should fail readiness: Refuse hard-fails, Quarantine stays
    /// serving the healthy partitions).
    fn recovery_mismatch_policy(&self) -> RecoveryMismatchPolicy;

    /// Repair a quarantined partition (truncate-to-valid) and clear it.
    async fn repair_partition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError>;
}

/// The stream (Plexus) storage surface. Separate from [`QueueEngine`] because
/// streams are fan-out (push/tail) rather than lease/poll, so they share only the
/// substrate, not the delivery model. The broker-side stream actor drives this.
#[async_trait]
pub trait StreamStore: Send + Sync {
    /// Mark a stream whose every write waits for a sync, so storage can warn
    /// if a background sync ever has to cover its writes. Engines without
    /// that check ignore it.
    async fn set_stream_writes_synced(
        &self,
        _tp: &str,
        _part: u32,
        _synced: bool,
    ) -> Result<(), StromaError> {
        Ok(())
    }

    async fn create_stream(
        &self,
        tp: &str,
        part: u32,
        retention: Option<RetentionConfig>,
    ) -> Result<(), StromaError>;

    async fn append_stream_record(
        &self,
        tp: &str,
        part: u32,
        headers: &MessageHeaders,
        payload: Vec<u8>,
    ) -> Result<Offset, StromaError>;

    /// Pipelined durable append: enqueue the batch as one keratin append and return
    /// immediately, without waiting for stage or durability. `offset` resolves at
    /// stage (the base offset; records occupy `base..base+records.len()`), `durable`
    /// when the batch is fsynced. Lets the caller enqueue the next batch right away
    /// so keratin coalesces fsyncs across appends. For the durable-tier pipeline.
    async fn append_stream_records_enqueue(
        &self,
        tp: &str,
        part: u32,
        records: Vec<(MessageHeaders, Vec<u8>)>,
        durability: Option<KDurability>,
    ) -> Result<EnqueuedStreamAppend, StromaError>;

    /// Flush and fsync a stream partition's message log (advances the durable
    /// watermark; the fsync runs on keratin's worker stage, off the writer thread).
    /// Called on an interval for the ephemeral tier to drain dirty pages steadily so
    /// the writer never hits the kernel writeback cliff.
    async fn sync_stream(&self, tp: &str, part: u32) -> Result<(), StromaError>;

    async fn read_stream_records(
        &self,
        tp: &str,
        part: u32,
        from: Offset,
        max: usize,
    ) -> Result<Vec<(Offset, Vec<u8>, MessageHeaders)>, StromaError>;

    /// Commit a coalesced batch of cursors in one durable record and one actor
    /// apply (see `Stroma::commit_stream_cursors`). Backs the cursor-commit
    /// microbatcher.
    async fn commit_stream_cursors(
        &self,
        tp: &str,
        part: u32,
        commits: Vec<(String, Offset)>,
    ) -> Result<(), StromaError>;

    async fn stream_cursor(
        &self,
        tp: &str,
        part: u32,
        name: &str,
    ) -> Result<Option<Offset>, StromaError>;

    /// `(head, tail)` of a stream. The tail is one past the last staged record,
    /// which may not be durable or readable yet.
    async fn stream_head_tail(&self, tp: &str, part: u32) -> Result<(Offset, Offset), StromaError>;

    /// Exclusive durable frontier of a stream's record log.
    /// `read_stream_records` never returns a record at or above it.
    async fn stream_durable_next(&self, tp: &str, part: u32) -> Result<Offset, StromaError>;

    /// A stream's durable cursors as (name, offset), name-sorted.
    async fn stream_cursors(
        &self,
        tp: &str,
        part: u32,
    ) -> Result<Vec<(String, Offset)>, StromaError>;

    async fn stream_offset_at_or_after_time(
        &self,
        tp: &str,
        part: u32,
        ts_ms: u64,
    ) -> Result<Offset, StromaError>;

    /// Whether `(tp, part)` is durably marked as a stream (Plexus) partition.
    /// Read from the on-disk kind marker so the broker can route a publish to the
    /// stream path before the partition's fan-out channel is materialized.
    fn durable_is_stream(&self, tp: &str, part: u32) -> bool;

    /// Remove the ungrouped queue partition `(tp, part)` only if it never held
    /// a record. See `Stroma::remove_empty_queue_partition` for the exact rule.
    /// Engines without that check keep everything.
    async fn remove_empty_queue_partition(
        &self,
        _tp: &str,
        _part: u32,
    ) -> Result<EmptyQueueRemoval, StromaError> {
        Err(StromaError::Unsupported(
            "this engine cannot remove empty queue partitions".into(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct StromaEngine {
    inner: Arc<Stroma>,
    // Disposable recovery inspection downloads. Kept outside the Stroma root,
    // which refuses entries it does not own.
    recovery_inspection_root: Arc<PathBuf>,
}

/// Default recovery inspection cache for a Stroma root at `<data_dir>/storage`:
/// `<data_dir>/cache/recovery-inspection`.
fn default_recovery_inspection_root(stroma_root: &Path) -> PathBuf {
    stroma_root
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join("cache")
        .join("recovery-inspection")
}

#[async_trait]
impl StreamStore for StromaEngine {
    async fn create_stream(
        &self,
        tp: &str,
        part: u32,
        retention: Option<RetentionConfig>,
    ) -> Result<(), StromaError> {
        self.inner.create_stream(tp, part, retention).await
    }

    async fn set_stream_writes_synced(
        &self,
        tp: &str,
        part: u32,
        synced: bool,
    ) -> Result<(), StromaError> {
        self.inner.set_stream_writes_synced(tp, part, synced).await
    }

    async fn append_stream_record(
        &self,
        tp: &str,
        part: u32,
        headers: &MessageHeaders,
        payload: Vec<u8>,
    ) -> Result<Offset, StromaError> {
        self.inner
            .append_stream_record(tp, part, headers, payload)
            .await
    }

    async fn append_stream_records_enqueue(
        &self,
        tp: &str,
        part: u32,
        records: Vec<(MessageHeaders, Vec<u8>)>,
        durability: Option<KDurability>,
    ) -> Result<EnqueuedStreamAppend, StromaError> {
        self.inner
            .append_stream_records_enqueue(tp, part, records, durability)
            .await
    }

    async fn sync_stream(&self, tp: &str, part: u32) -> Result<(), StromaError> {
        self.inner.sync_stream(tp, part).await
    }

    async fn read_stream_records(
        &self,
        tp: &str,
        part: u32,
        from: Offset,
        max: usize,
    ) -> Result<Vec<(Offset, Vec<u8>, MessageHeaders)>, StromaError> {
        self.inner.read_stream_records(tp, part, from, max).await
    }

    async fn commit_stream_cursors(
        &self,
        tp: &str,
        part: u32,
        commits: Vec<(String, Offset)>,
    ) -> Result<(), StromaError> {
        self.inner.commit_stream_cursors(tp, part, commits).await
    }

    async fn stream_cursor(
        &self,
        tp: &str,
        part: u32,
        name: &str,
    ) -> Result<Option<Offset>, StromaError> {
        self.inner.stream_cursor(tp, part, name).await
    }

    async fn stream_head_tail(&self, tp: &str, part: u32) -> Result<(Offset, Offset), StromaError> {
        self.inner.stream_head_tail(tp, part).await
    }

    async fn stream_durable_next(&self, tp: &str, part: u32) -> Result<Offset, StromaError> {
        self.inner.stream_durable_next(tp, part).await
    }

    async fn stream_cursors(
        &self,
        tp: &str,
        part: u32,
    ) -> Result<Vec<(String, Offset)>, StromaError> {
        self.inner.stream_cursors(tp, part).await
    }

    async fn stream_offset_at_or_after_time(
        &self,
        tp: &str,
        part: u32,
        ts_ms: u64,
    ) -> Result<Offset, StromaError> {
        self.inner
            .stream_offset_at_or_after_time(tp, part, ts_ms)
            .await
    }

    fn durable_is_stream(&self, tp: &str, part: u32) -> bool {
        self.inner.partition_kind(tp, part, None) == PartitionKind::Stream
    }

    async fn remove_empty_queue_partition(
        &self,
        tp: &str,
        part: u32,
    ) -> Result<EmptyQueueRemoval, StromaError> {
        self.inner
            .remove_empty_queue_partition(tp, part, None)
            .await
    }
}

impl StromaEngine {
    /// Local tentative recovery downloads. These files carry no serving authority.
    pub fn recovery_inspection_journal_root(&self) -> PathBuf {
        self.recovery_inspection_root.as_ref().clone()
    }

    /// Use `root` for recovery inspection downloads instead of the default
    /// `cache/recovery-inspection` next to the Stroma root.
    pub fn with_recovery_inspection_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.recovery_inspection_root = Arc::new(root.into());
        self
    }

    fn from_stroma(stroma: Stroma) -> Self {
        let recovery_inspection_root = Arc::new(default_recovery_inspection_root(&stroma.root()));
        Self {
            inner: Arc::new(stroma),
            recovery_inspection_root,
        }
    }

    pub async fn open(
        root: impl AsRef<Path>,
        keratin_cfg: StromaKeratinConfig,
        snap_cfg: SnapshotConfig,
    ) -> Result<Self, StromaError> {
        let stroma = Stroma::open(root, keratin_cfg, snap_cfg).await?;
        Ok(Self::from_stroma(stroma))
    }

    pub async fn open_with_runtime(
        root: impl AsRef<Path>,
        keratin_cfg: StromaKeratinConfig,
        snap_cfg: SnapshotConfig,
        runtime: Arc<LogRuntimeSettings>,
    ) -> Result<Self, StromaError> {
        let stroma = Stroma::open_with_runtime(root, keratin_cfg, snap_cfg, runtime).await?;
        Ok(Self::from_stroma(stroma))
    }

    /// Explicit initial-history primitive. The caller must authorize all three
    /// IDs through coordination before binding previously nonexistent storage.
    /// Ordinary broker creation does not enable this until recovery can readmit
    /// a restarted writer and install accepted history on the required quorum.
    pub async fn initialize_empty_storage_history(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        kind: PartitionKind,
        binding: StorageHistoryBinding,
    ) -> Result<(), StromaError> {
        self.inner
            .initialize_empty_storage_history(tp, part, group, kind, binding)
            .await
    }

    /// Persist an empty baseline while ordinary admission remains closed.
    pub async fn prepare_empty_storage_history(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        kind: PartitionKind,
        binding: StorageHistoryBinding,
    ) -> Result<PreparedStorageHistory, StromaError> {
        self.inner
            .prepare_empty_storage_history(tp, part, group, kind, binding)
            .await
    }

    /// Requires fresh authority proving this origin has never activated.
    pub async fn resume_empty_storage_history(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        kind: PartitionKind,
        binding: StorageHistoryBinding,
    ) -> Result<PreparedStorageHistory, StromaError> {
        self.inner
            .resume_empty_storage_history(tp, part, group, kind, binding)
            .await
    }

    /// Requires fresh authority excluding this replica from the accepted write set.
    pub async fn install_queue_learner_checkpoint(
        &self,
        receipt: PreparedStorageHistory,
        install: QueueStateCheckpoint,
    ) -> Result<QueueStateCheckpointInstallOutcome, StromaError> {
        self.inner
            .install_queue_learner_checkpoint(receipt, install)
            .await
    }

    pub fn installed_queue_reseed_cut(
        &self,
        receipt: &PreparedStorageHistory,
        intent: [u8; 32],
    ) -> Result<Option<(u64, u64)>, StromaError> {
        self.inner.installed_queue_reseed_cut(receipt, intent)
    }

    pub async fn prepare_queue_reseed_storage(
        &self,
        receipt: PreparedStorageHistory,
        intent: [u8; 32],
    ) -> Result<Self, StromaError> {
        Ok(Self {
            inner: Arc::new(
                self.inner
                    .prepare_queue_reseed_storage(receipt, intent)
                    .await?,
            ),
            recovery_inspection_root: self.recovery_inspection_root.clone(),
        })
    }

    pub async fn install_queue_reseed_storage(
        &self,
        receipt: PreparedStorageHistory,
        intent: [u8; 32],
        epoch: u64,
        target: u64,
    ) -> Result<(), StromaError> {
        self.inner
            .install_queue_reseed_storage(receipt, intent, epoch, target)
            .await
    }

    pub async fn prepare_queue_learner_storage(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
        binding: StorageHistoryBinding,
        intent: [u8; 32],
    ) -> Result<PreparedStorageHistory, StromaError> {
        self.inner
            .prepare_queue_learner_storage(topic, part, group, binding, intent)
            .await
    }

    pub async fn queue_replication_next_offset(
        &self,
        topic: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset, StromaError> {
        self.inner
            .queue_replication_next_offset(topic, part, group)
            .await
    }

    pub async fn verify_queue_learner_caught_up(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
        target: u64,
    ) -> Result<QueuePublishCommit, StromaError> {
        self.inner
            .verify_queue_learner_caught_up(tp, part, group, epoch, target)
            .await
    }

    pub fn queue_checkpoint_activity(&self, topic: &str, part: u32, group: Option<&str>) -> Option<stroma_core::QueueCheckpointActivity> {
        self.inner.queue_checkpoint_activity(topic, part, group)
    }

    pub async fn begin_queue_checkpoint_pin(&self, storage: PreparedStorageHistory, attempt: [u8; 32]) -> Result<QueueCheckpointBase, StromaError> {
        self.inner.begin_queue_checkpoint_pin(storage, attempt).await
    }
    pub async fn queue_checkpoint_target(&self, storage: PreparedStorageHistory, minimum_next: u64, minimum_payload_head: u64) -> Result<QueueCheckpointTarget, StromaError> {
        self.inner.queue_checkpoint_target(storage, minimum_next, minimum_payload_head).await
    }
    pub async fn build_queue_checkpoint_capsule(&self, pin: QueueCheckpointPin, target: QueueCheckpointTarget, limits: QueueCheckpointBuildLimits) -> Result<QueueCheckpointCapsule, StromaError> {
        self.inner.build_queue_checkpoint_capsule(pin, target, limits).await
    }
    pub async fn accept_queue_checkpoint(&self, pin: QueueCheckpointPin, certificate: [u8; 32], capsule: [u8; 32], previous: Option<[u8; 32]>) -> Result<(), StromaError> {
        self.inner.accept_queue_checkpoint(pin, certificate, capsule, previous).await
    }
    pub async fn reconcile_queue_checkpoint_pins(&self, storage: PreparedStorageHistory, pending: Option<[u8; 32]>, accepted: Option<[u8; 32]>) -> Result<(), StromaError> {
        self.inner.reconcile_queue_checkpoint_pins(storage, pending, accepted).await
    }

    pub fn verify_admitted_storage_history(
        &self,
        prepared: &PreparedStorageHistory,
    ) -> Result<(), StromaError> {
        self.inner.verify_admitted_storage_history(prepared)
    }

    pub fn verify_prepared_storage_history(
        &self,
        prepared: &PreparedStorageHistory,
    ) -> Result<(), StromaError> {
        self.inner.verify_prepared_storage_history(prepared)
    }

    /// The caller must first obtain fresh activation authority for this exact receipt.
    pub async fn admit_prepared_storage_history(
        &self,
        prepared: PreparedStorageHistory,
    ) -> Result<(), StromaError> {
        self.inner.admit_prepared_storage_history(prepared).await
    }

    /// Stage a recovery baseline separately after fresh plan authorization.
    pub async fn open_queue_recovery_stage(
        &self,
        spec: QueueRecoveryStageSpec,
        snapshot: Vec<u8>,
        limits: RecoveryStageLimits,
    ) -> Result<QueueRecoveryStage, StromaError> {
        self.inner
            .open_queue_recovery_stage(spec, snapshot, limits)
            .await
    }

    pub async fn transfer_queue_recovery_snapshot(
        &self, spec: QueueRecoveryStageSpec, chunk: Option<(u64, Vec<u8>)>,
    ) -> Result<u64, StromaError> {
        self.inner.transfer_queue_recovery_snapshot(spec, chunk).await
    }

    pub async fn open_queue_recovery_stage_from_snapshot(
        &self, spec: QueueRecoveryStageSpec, limits: RecoveryStageLimits,
        seal: crate::recovery::RecoverySealRequest,
    ) -> Result<QueueRecoveryStage, StromaError> {
        self.inner.open_queue_recovery_stage_from_snapshot(spec, limits, seal).await
    }

    pub async fn resume_queue_recovery_stage(
        &self,
        spec: QueueRecoveryStageSpec,
        limits: RecoveryStageLimits,
    ) -> Result<QueueRecoveryStage, StromaError> {
        self.inner.resume_queue_recovery_stage(spec, limits).await
    }

    pub async fn open_queue_recovery_stage_reusing(
        &self,
        spec: QueueRecoveryStageSpec,
        snapshot: Vec<u8>,
        limits: RecoveryStageLimits,
        seal: crate::recovery::RecoverySealRequest,
    ) -> Result<QueueRecoveryStage, StromaError> {
        self.inner
            .open_queue_recovery_stage_reusing(spec, snapshot, limits, seal)
            .await
    }

    pub async fn install_queue_recovery_stage(
        &self,
        spec: QueueRecoveryStageSpec,
        seal: crate::recovery::RecoverySealRequest,
        stage: &QueueRecoveryStage,
    ) -> Result<PreparedQueueRecovery, StromaError> {
        self.inner
            .install_queue_recovery_stage(spec, seal, stage)
            .await
    }
    pub fn verify_prepared_queue_recovery(
        &self,
        prepared: &PreparedQueueRecovery,
    ) -> Result<(), StromaError> {
        self.inner.verify_prepared_queue_recovery(prepared)
    }
    pub async fn admit_prepared_queue_recovery(
        &self,
        prepared: PreparedQueueRecovery,
    ) -> Result<(), StromaError> {
        self.inner.admit_prepared_queue_recovery(prepared).await
    }

    /// Inspect local identity without granting access or opening either log.
    pub fn storage_history_binding(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Option<StorageHistoryBinding>, StromaError> {
        self.inner.storage_history_binding(tp, part, group)
    }

    /// Set the recovery dangling-reference policy (startup config).
    pub fn set_recovery_mismatch_policy(&self, policy: RecoveryMismatchPolicy) {
        self.inner.set_recovery_mismatch_policy(policy);
    }

    /// Persist a queue checkpoint without triggering log compaction.
    pub async fn snapshot_partition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner.snapshot_partition(tp, part, group).await
    }

    /// Truncate the queue log so offsets below `before` are no longer retained,
    /// advancing the log head. Stroma never truncates past live payloads,
    /// protected followers or checkpoint pins. A follower that requests an
    /// offset below the new head must install a checkpoint rather than
    /// tail-replay.
    pub async fn truncate_queue_before(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        before: Offset,
    ) -> Result<u64, StromaError> {
        self.inner
            .truncate_queue_before(tp, part, group, before)
            .await
    }

    pub async fn global_store(&self) -> Result<Arc<GlobalStore>, StromaError> {
        self.inner.global_store().await
    }

    pub async fn subscribe_owner_written(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<
        (
            tokio::sync::watch::Receiver<()>,
            tokio::sync::watch::Receiver<()>,
        ),
        StromaError,
    > {
        self.inner.subscribe_owner_written(tp, part, group).await
    }

    pub async fn subscribe_owner_queue_written(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<tokio::sync::watch::Receiver<()>, StromaError> {
        self.inner.subscribe_owner_queue_written(tp, part, group).await
    }

    pub async fn read_owner_queue_records(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<EncodedEvent>, StromaError> {
        self.inner
            .read_owner_queue_encoded(tp, part, group, from, max, max_bytes)
            .await
    }

    pub async fn read_owner_queue_records_early(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<EncodedEvent>, StromaError> {
        self.inner
            .read_owner_queue_encoded_early(tp, part, group, from, max, max_bytes)
            .await
    }

    pub async fn read_owner_stream_records(
        &self,
        tp: &str,
        part: u32,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<Message>, StromaError> {
        self.inner
            .read_owner_stream_records(tp, part, from, max, max_bytes)
            .await
    }

    pub async fn read_owner_stream_records_early(
        &self,
        tp: &str,
        part: u32,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<Message>, StromaError> {
        self.inner
            .read_owner_stream_records_early(tp, part, from, max, max_bytes)
            .await
    }

    pub async fn read_owner_stream_cursor_events(
        &self,
        tp: &str,
        part: u32,
        from: Offset,
        max: usize,
        max_bytes: usize,
    ) -> Result<OwnerReplicationRead<StromaEvent>, StromaError> {
        self.inner
            .read_owner_stream_cursor_events(tp, part, from, max, max_bytes)
            .await
    }

    /// Durable frontier of a stream's cursor-event log, as a watch.
    pub async fn watch_stream_cursor_events_durable(
        &self,
        tp: &str,
        part: u32,
    ) -> Result<tokio::sync::watch::Receiver<u64>, StromaError> {
        self.inner
            .watch_stream_cursor_events_durable(tp, part)
            .await
    }

    pub async fn export_queue_state_checkpoint(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<QueueStateCheckpoint, StromaError> {
        self.inner
            .export_queue_state_checkpoint(tp, part, group)
            .await
    }

    pub async fn read_sealed_replica_sequential(
        &self,
        command: &crate::recovery::RecoverySealCommand,
        request: stroma_core::RecoveryReadRequest,
        cursor: Option<stroma_core::RecoverySequentialRead>,
    ) -> Result<
        (
            stroma_core::RecoveryReadPage,
            Option<stroma_core::RecoverySequentialRead>,
        ),
        StromaError,
    > {
        self.inner
            .read_sealed_replica_sequential(
                &command.topic,
                command.partition.id(),
                command.group.as_deref(),
                if command.stream {
                    PartitionKind::Stream
                } else {
                    PartitionKind::Queue
                },
                request,
                cursor,
            )
            .await
    }

    pub async fn read_sealed_replica(
        &self,
        command: &crate::recovery::RecoverySealCommand,
        request: stroma_core::RecoveryReadRequest,
    ) -> Result<stroma_core::RecoveryReadPage, StromaError> {
        self.inner
            .read_sealed_replica(
                &command.topic,
                command.partition.id(),
                command.group.as_deref(),
                if command.stream {
                    PartitionKind::Stream
                } else {
                    PartitionKind::Queue
                },
                request,
            )
            .await
    }

    pub fn completed_queue_recovery_seal(
        &self, topic: &str, part: u32, group: Option<&str>, request: &crate::recovery::RecoverySealRequest,
    ) -> Result<Option<crate::recovery::SealedReplicaFrontiers>, StromaError> {
        self.inner.completed_queue_recovery_seal(topic, part, group, request)
    }

    pub async fn seal_replica_for_recovery(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
        stream: bool,
        request: stroma_core::RecoverySealRequest,
    ) -> Result<stroma_core::SealedReplicaFrontiers, StromaError> {
        self.inner
            .seal_replica_for_recovery_checked(
                topic,
                partition,
                group,
                if stream {
                    PartitionKind::Stream
                } else {
                    PartitionKind::Queue
                },
                request,
            )
            .await
    }

    /// Written and durable frontiers and background sync state of the logs
    /// behind a queue partition.
    pub async fn queue_log_sync_status(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<QueueLogSyncStatus, StromaError> {
        self.inner.queue_log_sync_status(tp, part, group).await
    }

    /// Background sync interval for every queue log, open now or later. Zero
    /// turns it off. Logs that fail to change are logged and keep theirs.
    pub async fn set_queue_background_sync_default(&self, interval: std::time::Duration) {
        self.inner.set_queue_background_sync_default(interval).await
    }

    /// Change the background sync interval of the logs behind a queue
    /// partition. Zero turns it off.
    pub async fn set_queue_background_sync_interval(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        interval: std::time::Duration,
    ) -> Result<(), StromaError> {
        self.inner
            .set_queue_background_sync_interval(tp, part, group, interval)
            .await
    }

    pub async fn become_queue_follower(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner.become_queue_follower(tp, part, group).await
    }

    pub async fn stop_queue_follower_for_transition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner
            .stop_queue_follower_for_transition(tp, part, group)
            .await
    }

    pub async fn freeze_queue_for_transition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner
            .freeze_queue_for_transition(tp, part, group)
            .await
    }

    pub async fn demote_queue_owner_to_follower(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<QueueDemotionOutcome, StromaError> {
        self.inner
            .demote_queue_owner_to_follower(tp, part, group)
            .await
    }

    pub async fn apply_open_replicated_queue_batch(
        &self, tp: &str, part: u32, group: Option<&str>,
        frames: futures::stream::BoxStream<'_, Result<ReplicatedEventBatch, StromaError>>,
    ) -> Result<Vec<ReplicatedAppendOutcome>, StromaError> {
        self.inner.apply_open_replicated_queue_batch(tp, part, group, frames).await
    }

    pub async fn apply_replicated_queue_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        batch: ReplicatedEventBatch,
    ) -> Result<ReplicatedAppendOutcome, StromaError> {
        self.inner
            .apply_replicated_queue_batch(tp, part, group, batch)
            .await
    }

    pub async fn promote_queue_follower_if_caught_up(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        expected_next: Offset,
    ) -> Result<QueuePromotionOutcome, StromaError> {
        self.inner
            .promote_queue_follower_if_caught_up(tp, part, group, expected_next)
            .await
    }

    /// Failover promotion: accept the follower's own tails, fenced at the
    /// assignment epoch (persisted before serving).
    pub async fn promote_queue_follower_to_local_tail(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<QueuePromotionOutcome, StromaError> {
        self.inner
            .promote_queue_follower_to_local_tail(tp, part, group, epoch)
            .await
    }

    /// Fence the queue log at the assignment epoch (persisted, monotonic).
    pub async fn advance_queue_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<u64, StromaError> {
        self.inner.advance_queue_epoch(tp, part, group, epoch).await
    }

    /// `become_queue_owner` fenced at the assignment epoch.
    pub async fn become_queue_owner_with_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<(), StromaError> {
        self.inner
            .become_queue_owner_with_epoch(tp, part, group, epoch)
            .await
    }

    /// `become_queue_follower` fenced at the assignment epoch: stale-epoch
    /// owners' replicated batches are rejected from here on.
    pub async fn become_queue_follower_with_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<(), StromaError> {
        self.inner
            .become_queue_follower_with_epoch(tp, part, group, epoch)
            .await
    }

    // ----- Stream (Plexus) replication seam -----------------------------------
    // A stream partition reuses the queue handle (record log + cursor-commit
    // event log, no group), so these delegate to the stream-named stroma
    // primitives. The concrete follower worker calls them like the queue ones.

    pub async fn apply_replicated_stream_batch(
        &self,
        tp: &str,
        part: u32,
        messages: Option<ReplicatedMessageBatch>,
        events: Option<ReplicatedEventBatch>,
    ) -> Result<ReplicatedStreamApplyOutcome, StromaError> {
        self.inner
            .apply_replicated_stream_batch(tp, part, messages, events)
            .await
    }

    pub async fn advance_stream_epoch(
        &self,
        tp: &str,
        part: u32,
        epoch: u64,
    ) -> Result<u64, StromaError> {
        self.inner.advance_stream_epoch(tp, part, epoch).await
    }

    pub async fn become_stream_follower_with_epoch(
        &self,
        tp: &str,
        part: u32,
        epoch: u64,
    ) -> Result<(), StromaError> {
        self.inner
            .become_stream_follower_with_epoch(tp, part, epoch)
            .await
    }

    pub async fn become_stream_owner_with_epoch(
        &self,
        tp: &str,
        part: u32,
        epoch: u64,
    ) -> Result<(), StromaError> {
        self.inner
            .become_stream_owner_with_epoch(tp, part, epoch)
            .await
    }

    /// Failover promotion at the stream follower's own tails, fenced at the
    /// assignment epoch (mirrors `promote_queue_follower_to_local_tail`).
    pub async fn promote_stream_follower_to_local_tail(
        &self,
        tp: &str,
        part: u32,
        epoch: u64,
    ) -> Result<StreamPromotionOutcome, StromaError> {
        self.inner
            .promote_stream_follower_to_local_tail(tp, part, epoch)
            .await
    }

    async fn replay_dead_letter(
        &self,
        dlq_tp: &str,
        dlq_group: Option<&str>,
        offset: Offset,
    ) -> Result<ReplayDeadLetterItem, StromaError> {
        let page = self
            .inner
            .inspect_messages(
                dlq_tp,
                0,
                dlq_group,
                offset,
                1,
                InspectMode::ActiveOnly,
                true,
                usize::MAX,
            )
            .await?;
        let Some(item) = page
            .items
            .into_iter()
            .find(|item| item.state.offset == offset)
        else {
            return Ok(skipped_replay(
                offset,
                "offset is not active in the DLQ queue",
            ));
        };
        let Some(mut headers) = item.headers else {
            return Ok(skipped_replay(
                offset,
                "message headers are missing from the log",
            ));
        };
        let Some(payload) = item.payload else {
            return Ok(skipped_replay(
                offset,
                "message payload is missing from the log",
            ));
        };
        if item.missing_payload {
            return Ok(skipped_replay(
                offset,
                "message payload is missing from the log",
            ));
        }
        if item.payload_truncated {
            return Ok(skipped_replay(
                offset,
                "message payload was truncated during inspection",
            ));
        }

        let Some(target_topic) = headers.extra.get("stroma.dlq.source_topic").cloned() else {
            return Ok(skipped_replay(
                offset,
                "DLQ metadata is missing stroma.dlq.source_topic",
            ));
        };
        let target_group = headers.extra.get("stroma.dlq.source_group").cloned();

        headers.published = unix_millis();
        headers.publish_received = headers.published;
        headers
            .extra
            .retain(|key, _| !key.starts_with("stroma.") && !key.starts_with("fibril."));

        let (completion, receiver) = KeratinAppendCompletion::pair();
        self.inner
            .append_message(
                &target_topic,
                0,
                target_group.as_deref(),
                &headers,
                payload,
                completion,
            )
            .await?;
        match receiver.await {
            Ok(Ok(_)) => Ok(ReplayDeadLetterItem {
                offset,
                outcome: ReplayDeadLetterOutcome::Replayed,
                target_topic: Some(target_topic),
                target_group,
                reason: None,
            }),
            Ok(Err(err)) => Err(StromaError::Io(err.to_string())),
            Err(_) => Err(StromaError::Io("replay append completion dropped".into())),
        }
    }
}

fn skipped_replay(offset: Offset, reason: impl Into<String>) -> ReplayDeadLetterItem {
    ReplayDeadLetterItem {
        offset,
        outcome: ReplayDeadLetterOutcome::Skipped,
        target_topic: None,
        target_group: None,
        reason: Some(reason.into()),
    }
}

// pub fn make_stroma_engine(root: impl AsRef<Path>) -> Result<StromaEngine, StromaError> {
//     let keratin_cfg = KeratinConfig::test_default();
//     let snap_cfg = SnapshotConfig::default();
//     let stroma = futures::executor::block_on(Stroma::open(root, keratin_cfg, snap_cfg))?;
//     Ok(StromaEngine {
//         inner: stroma,
//     })
// }

#[async_trait]
impl QueueEngine for StromaEngine {
    fn set_queue_replication_retention(
        &self,
        source: Arc<dyn stroma_core::QueueReplicationRetention>,
    ) {
        self.inner.set_queue_replication_retention(source);
    }
    async fn poll_ready(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        max: usize,
        lease_deadline: UnixMillis,
        upper: Offset,
    ) -> Result<Vec<Deliverable>, StromaError> {
        let v = self
            .inner
            .poll_ready(tp, part, group, max, lease_deadline, upper)
            .await?;

        Ok(v.into_iter()
            .map(|(offset, headers, payload, retries)| Deliverable {
                offset,
                payload,
                retries,
                content_type: headers.content_type,
                extra_headers: headers.extra,
                publish_received: headers.publish_received,
                published: headers.published,
            })
            .collect())
    }

    async fn ack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .ack_enqueue(tp, part, group, offset, completion)
            .await
    }

    async fn ack_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .ack_enqueue_many(tp, part, group, items, completion)
            .await
    }

    async fn nack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        requeue: bool,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .nack_enqueue(tp, part, group, offset, requeue, completion)
            .await?;
        Ok(())
    }

    async fn nack_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<NackEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .nack_enqueue_many(tp, part, group, items, completion)
            .await
    }

    async fn release_inflight_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .release_inflight_many(tp, part, group, items, completion)
            .await
    }

    async fn settle(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        req: SettleRequest,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        match req.kind {
            SettleKind::Ack => {
                self.inner
                    .ack_enqueue(tp, part, group, req.offset, completion)
                    .await?;
            }
            SettleKind::Nack {
                requeue,
                not_before,
            } => {
                self.inner
                    .nack_enqueue_many(
                        tp,
                        part,
                        group,
                        vec![NackEventMeta {
                            off: req.offset,
                            requeue,
                            not_before,
                        }],
                        completion,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn publish(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        headers: &MessageHeaders,
        payload: Vec<u8>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .append_message(tp, part, group, headers, payload, completion)
            .await?;

        Ok(())
    }

    async fn publish_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<PublishItem>,
    ) -> Result<(), StromaError> {
        self.inner
            .append_message_batch(tp, part, group, items)
            .await
    }

    async fn publish_batch_observed(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<PublishItem>,
        observer: stroma_core::QueuePublishObserver,
    ) -> Result<(), StromaError> {
        self.inner
            .append_message_batch_observed(tp, part, group, items, Some(observer))
            .await
    }

    async fn queue_durable_commit(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<stroma_core::QueuePublishCommit, StromaError> {
        self.inner.queue_durable_commit(tp, part, group).await
    }

    async fn next_expiry_hint(&self) -> Result<Option<UnixMillis>, StromaError> {
        self.inner.next_expiry_hint().await
    }

    async fn requeue_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>, StromaError> {
        self.inner.requeue_expired(now, max).await
    }

    async fn drop_ttl_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>, StromaError> {
        self.inner.drop_ttl_expired(now, max).await
    }

    async fn shutdown(&self) -> Result<(), StromaError> {
        self.inner.shutdown().await?;

        Ok(())
    }

    async fn recovery_disk_usage(&self) -> Result<Option<RecoveryDiskUsage>, StromaError> {
        self.inner.recovery_disk_usage().await.map(Some)
    }

    async fn estimate_disk_used(&self) -> Result<u64, StromaError> {
        self.inner.estimate_disk_used().await
    }

    async fn estimate_disk_used_breakdown(
        &self,
    ) -> Result<Vec<DiskUsedBreakdownEntry>, StromaError> {
        self.inner.estimate_disk_used_breakdown().await
    }

    async fn list_partitions(&self) -> Result<Vec<(String, u32, Option<String>)>, StromaError> {
        Ok(self
            .inner
            .list_queues()
            .into_iter()
            .map(|(tp, part, group)| (tp.to_string(), part, group.map(|s| s.to_string())))
            .collect::<Vec<_>>())
    }

    async fn queue_stats_snapshot(&self) -> Result<QueuesStateSnapshot, StromaError> {
        let stats = self.inner.get_queues_stats().await?;

        let mut snapshot = QueuesStateSnapshot {
            queues: Default::default(),
        };

        for ((tp, group), report) in stats {
            let entry = match report {
                Ok(stat) => fibril_metrics::QueueStateSnapshot::Ok {
                    ready_count: stat.ready_count,
                    inflight_count: stat.inflight_count,
                },
                Err(err) => fibril_metrics::QueueStateSnapshot::Error { message: err },
            };
            snapshot.queues.insert(
                fibril_metrics::QueueKey {
                    topic: tp.to_string(),
                    group: group.map(|s| s.to_string()),
                },
                entry,
            );
        }

        Ok(snapshot)
    }

    async fn debug_snapshot(&self) -> Result<StromaDebugSnapshot, StromaError> {
        self.inner.debug_snapshot().await
    }

    async fn inspect_messages(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        limit: usize,
        mode: InspectMode,
        include_payload: bool,
        payload_limit_bytes: usize,
    ) -> Result<MessageInspectionPage, StromaError> {
        self.inner
            .inspect_messages(
                tp,
                part,
                group,
                from,
                limit,
                mode,
                include_payload,
                payload_limit_bytes,
            )
            .await
    }

    async fn replay_dead_letters(
        &self,
        dlq_tp: &str,
        dlq_group: Option<&str>,
        offsets: &[Offset],
    ) -> Result<ReplayDeadLettersReport, StromaError> {
        let mut items = Vec::with_capacity(offsets.len());
        let mut replayed = 0;

        for &offset in offsets {
            let result = self.replay_dead_letter(dlq_tp, dlq_group, offset).await?;
            if matches!(result.outcome, ReplayDeadLetterOutcome::Replayed) {
                replayed += 1;
            }
            items.push(result);
        }

        Ok(ReplayDeadLettersReport {
            requested: offsets.len(),
            replayed,
            items,
        })
    }

    async fn global_dlq(&self) -> Result<GlobalDlqSnapshot, StromaError> {
        self.inner.global_dlq().await
    }

    async fn global_store(&self) -> Result<Arc<GlobalStore>, StromaError> {
        self.inner.global_store().await
    }

    async fn set_global_dlq(
        &self,
        target: Option<GlobalDLQ>,
        expected_version: u64,
    ) -> Result<GlobalDlqUpdateOutcome, StromaError> {
        self.inner.set_global_dlq(target, expected_version).await
    }

    async fn declare_queue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        meta: DeclareMeta,
    ) -> Result<(), StromaError> {
        self.inner.declare(tp, part, group, meta).await
    }

    async fn set_queue_buffered(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        buffered: bool,
    ) -> Result<(), StromaError> {
        self.inner.set_queue_buffered(tp, part, group, buffered).await
    }

    fn queue_is_buffered(&self, tp: &str, part: u32, group: Option<&str>) -> bool {
        self.inner.queue_is_buffered(tp, part, group)
    }

    async fn materialize(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner.materialize(tp, part, group).await
    }

    async fn ensure_queue_owner_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: Option<u64>,
    ) -> Result<(), StromaError> {
        self.inner
            .ensure_queue_owner_epoch(tp, part, group, epoch)
            .await
    }

    async fn become_queue_owner_with_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<(), StromaError> {
        self.inner
            .become_queue_owner_with_epoch(tp, part, group, epoch)
            .await
    }

    async fn unmaterialize(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<EvictOutcome, StromaError> {
        self.inner.unmaterialize(tp, part, group).await
    }

    async fn destroy_partition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<DestroyOutcome, StromaError> {
        self.inner.destroy_partition(tp, part, group).await
    }

    fn is_materialized(&self, tp: &str, part: u32, group: Option<&str>) -> bool {
        self.inner.is_materialized(tp, part, group)
    }

    async fn has_inflight(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<bool, StromaError> {
        self.inner.has_inflight(tp, part, group).await
    }

    async fn lowest_unsettled_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset, StromaError> {
        self.inner.lowest_unsettled_offset(tp, part, group).await
    }

    async fn current_next_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset, StromaError> {
        self.inner.current_next_offset(tp, part, group).await
    }

    async fn next_deliverable(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        upper: Offset,
    ) -> Result<Option<Offset>, StromaError> {
        self.inner
            .next_deliverable(tp, part, group, from, upper)
            .await
    }

    fn metrics(&self) -> Arc<StromaMetrics> {
        self.inner.metrics()
    }

    fn deadline_awaker(&self) -> Arc<Notify> {
        self.inner.deadline_waker()
    }

    fn quarantined_partitions(&self) -> Vec<QuarantineInfo> {
        self.inner.quarantined_partitions()
    }

    fn recovery_mismatch_policy(&self) -> RecoveryMismatchPolicy {
        self.inner.recovery_mismatch_policy()
    }

    async fn repair_partition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner.repair_partition(tp, part, group).await
    }
}
