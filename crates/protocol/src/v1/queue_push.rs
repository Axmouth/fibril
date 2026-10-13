//! Credit-based queue push replication.
//!
//! On `QueuePushStart` the connection spawns one [`run_owner_queue_push`] task
//! per push. The task holds a cursor in the single queue log and a
//! byte-denominated send budget (credit), reads offset-ordered batches
//! (long-polling so it wakes on new publishes), and pushes them to the
//! follower as `QueuePushBatch` frames until the credit is exhausted. The
//! follower refills credit and reports durable progress through
//! [`OwnerPushControl::Progress`], rewinds the cursor through
//! [`OwnerPushControl::Reset`], and closes with [`OwnerPushControl::Stop`].
//!
//! The batch payload is the pull `QueueReplicationReadOk` body, so this is a
//! transport choice, not a storage or apply change.

use std::sync::Arc;

use async_trait::async_trait;
use fibril_broker::replication::StreamApplyTunablesFn;
use futures::{StreamExt, stream::BoxStream};
use tokio::sync::mpsc;

use crate::v1::{
    Partition, QueuePushEnd, QueuePushStart, QueueReplicationReadOk, ReplicationEventRead,
    frame::Frame, wire,
};

/// `QueuePushEnd.code` values.
pub const PUSH_END_CLOSED: u16 = 0;
pub const PUSH_END_ERROR: u16 = 1;
pub const PUSH_END_CHECKPOINT_REQUIRED: u16 = 2;

/// Control messages from the connection's frame dispatch to a running owner
/// push sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerPushControl {
    /// Follower reported progress and refilled the send budget.
    /// `written_next` is never below `durable_next`, and equal to it on a
    /// durable queue.
    Progress {
        durable_next: u64,
        written_next: u64,
        credit_add_bytes: u64,
    },
    /// Follower asked to rewind the push cursor (gap or post-checkpoint).
    Reset { from: u64 },
    /// Follower closed the push.
    Stop,
}

/// What the owner sender needs from the broker. A trait so the send loop is
/// unit-testable without a live engine.
#[async_trait]
pub trait OwnerPushSource: Send + Sync + 'static {
    /// Snapshot live read limits once per batch. Test sources may use the
    /// configuration supplied when the push starts.
    fn push_config(&self) -> Option<OwnerPushConfig> {
        None
    }

    /// Read the next offset-ordered batch from the queue log. `max_wait_ms`
    /// long-polls so a caught-up push wakes on the next publish.
    async fn read(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        from: u64,
        max_records: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> Result<QueueReplicationReadOk, String>;

    /// Record the follower's durable progress (drives the publish-confirm gate
    /// and the replica-durable visibility watermark).
    fn record_progress(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        reporter: &str,
        durable_next: u64,
    );

    /// Record progress of a follower whose queue log is written past what it
    /// has synced, as on a buffered queue. Sources that only track durable
    /// progress record that.
    fn record_written_progress(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        reporter: &str,
        durable_next: u64,
        _written_next: u64,
    ) {
        self.record_progress(topic, partition, group, reporter, durable_next);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct OwnerPushConfig {
    pub max_records: usize,
    /// Approximate per-read byte budget, independent of credit. An oversized
    /// record can exceed it to preserve progress.
    pub max_batch_bytes: usize,
    /// Long-poll budget for a caught-up read.
    pub long_poll_ms: u64,
}

impl Default for OwnerPushConfig {
    fn default() -> Self {
        Self {
            max_records: 2048,
            max_batch_bytes: 8 * 1024 * 1024,
            long_poll_ms: 1_000,
        }
    }
}

/// A read may exceed its budget to make progress on an oversized record.
/// Keep that excess as debt so returning the batch cannot grow the window.
struct PushCredit {
    available: u64,
    debt: u64,
}

impl PushCredit {
    fn new(available: u64) -> Self {
        Self { available, debt: 0 }
    }

    fn consume(&mut self, bytes: u64) {
        let paid = bytes.min(self.available);
        self.available -= paid;
        self.debt = self.debt.saturating_add(bytes - paid);
    }

    fn replenish(&mut self, bytes: u64) {
        let paid = bytes.min(self.debt);
        self.debt -= paid;
        self.available = self.available.saturating_add(bytes - paid);
    }
}

enum BatchOutcome {
    /// Deliverable records: their total bytes and the advanced cursor.
    Records { bytes: u64, next: u64 },
    /// Nothing available right now (caught up / long-poll timeout).
    Empty,
    /// The owner cannot serve from here without a checkpoint (offset gap).
    CheckpointRequired,
}

/// Payload bytes of a batch. A checkpoint requirement carries none.
pub(crate) fn batch_bytes(batch: &QueueReplicationReadOk) -> u64 {
    match batch {
        ReplicationEventRead::Batch { records, .. } => {
            records.iter().map(|r| r.payload.len() as u64).sum()
        }
        ReplicationEventRead::CheckpointRequired(_) => 0,
    }
}

fn classify(batch: &QueueReplicationReadOk) -> BatchOutcome {
    match batch {
        ReplicationEventRead::CheckpointRequired(_) => BatchOutcome::CheckpointRequired,
        ReplicationEventRead::Batch { next_offset, .. } => match batch_bytes(batch) {
            0 => BatchOutcome::Empty,
            bytes => BatchOutcome::Records {
                bytes,
                next: *next_offset,
            },
        },
    }
}

/// Epoch and exclusive end of a batch, or None for a checkpoint requirement.
fn frame_tail(frame: &QueueReplicationReadOk) -> Option<(u64, u64)> {
    match frame {
        ReplicationEventRead::Batch {
            epoch, next_offset, ..
        } => Some((*epoch, *next_offset)),
        ReplicationEventRead::CheckpointRequired(_) => None,
    }
}

/// Epoch and first offset of a batch, or None for a checkpoint requirement.
fn frame_start(frame: &QueueReplicationReadOk) -> Option<(u64, u64)> {
    match frame {
        ReplicationEventRead::Batch {
            epoch,
            requested_offset,
            ..
        } => Some((*epoch, *requested_offset)),
        ReplicationEventRead::CheckpointRequired(_) => None,
    }
}

/// Fold `next` into `acc` so the follower can apply (and fsync) several pushed
/// frames as one batch. Succeeds only when `next` continues `acc` contiguously
/// at the same epoch and neither is a checkpoint frame. On failure `next` is
/// handed back untouched so the caller applies `acc` and carries `next` to the
/// next apply (this is how an epoch change, an offset gap, or a checkpoint
/// frame stops a merge run without losing data).
fn try_fold_pushed(
    acc: &mut QueueReplicationReadOk,
    next: QueueReplicationReadOk,
) -> Result<(), QueueReplicationReadOk> {
    if frame_tail(acc).is_none() || frame_tail(acc) != frame_start(&next) {
        return Err(next);
    }
    if let (
        ReplicationEventRead::Batch {
            next_offset: an,
            records: ar,
            ..
        },
        ReplicationEventRead::Batch {
            next_offset: bn,
            records: br,
            ..
        },
    ) = (acc, next)
    {
        ar.extend(br);
        *an = bn;
    }
    Ok(())
}

/// Apply a control message. Returns `false` when the push should stop.
fn apply_control<S: OwnerPushSource>(
    control: OwnerPushControl,
    source: &S,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
    reporter: Option<&str>,
    credit: &mut PushCredit,
    from: &mut u64,
) -> bool {
    match control {
        OwnerPushControl::Progress {
            durable_next,
            written_next,
            credit_add_bytes,
        } => {
            credit.replenish(credit_add_bytes);
            if let Some(reporter) = reporter {
                if written_next > durable_next {
                    source.record_written_progress(
                        topic,
                        partition,
                        group,
                        reporter,
                        durable_next,
                        written_next,
                    );
                } else {
                    source.record_progress(topic, partition, group, reporter, durable_next);
                }
            }
            true
        }
        OwnerPushControl::Reset { from: reset } => {
            *from = reset;
            if let Some(reporter) = reporter {
                source.record_progress(topic, partition, group, reporter, reset);
            }
            true
        }
        OwnerPushControl::Stop => false,
    }
}

async fn send_push_end(frame_tx: &mpsc::Sender<Frame>, push_id: u64, code: u16, message: &str) {
    if let Ok(frame) = wire::encode_queue_push_end(
        push_id,
        &QueuePushEnd {
            code,
            message: message.to_string(),
        },
    ) {
        let _ = frame_tx.send(frame).await;
    }
}

/// Drive one owner queue push until the follower stops it, the connection
/// drops, or a fatal read error. The frame `request_id` of every pushed batch
/// is `push_id`.
pub async fn run_owner_queue_push<S: OwnerPushSource>(
    source: Arc<S>,
    frame_tx: mpsc::Sender<Frame>,
    push_id: u64,
    start: QueuePushStart,
    mut control_rx: mpsc::Receiver<OwnerPushControl>,
    cfg: OwnerPushConfig,
) {
    let topic = start.topic;
    let group = start.group;
    let partition = start.partition;
    let reporter = start.reporter_node_id;
    let mut from = start.from;
    let mut credit = PushCredit::new(start.credit_bytes);

    'push: loop {
        // Out of budget: park until the follower refills (or stops / drops).
        if credit.available == 0 {
            match control_rx.recv().await {
                Some(control) => {
                    if !apply_control(
                        control,
                        source.as_ref(),
                        &topic,
                        partition,
                        group.as_deref(),
                        reporter.as_deref(),
                        &mut credit,
                        &mut from,
                    ) {
                        break 'push;
                    }
                    continue 'push;
                }
                None => break 'push,
            }
        }

        let cfg = source.push_config().unwrap_or(cfg);
        let max_bytes = credit.available.min(cfg.max_batch_bytes as u64) as usize;
        let read = source.read(
            &topic,
            partition,
            group.as_deref(),
            from,
            cfg.max_records,
            max_bytes,
            cfg.long_poll_ms,
        );

        tokio::pin!(read);
        loop {
            tokio::select! {
                // Progress only updates credit and durable acknowledgements. Keep
                // the scan alive across progress so its blocking storage work is not
                // discarded and restarted. Reset and stop still cancel promptly.
                control = control_rx.recv() => {
                    match control {
                        Some(control) => {
                            let reset = matches!(control, OwnerPushControl::Reset { .. });
                            if !apply_control(
                                control,
                                source.as_ref(),
                                &topic,
                                partition,
                                group.as_deref(),
                                reporter.as_deref(),
                                &mut credit,
                                &mut from,
                            ) {
                                break 'push;
                            }
                            if reset {
                                continue 'push;
                            }
                        }
                        None => break 'push,
                    }
                }
                result = &mut read => {
                    match result {
                        Ok(batch) => match classify(&batch) {
                            BatchOutcome::Empty => continue 'push,
                            BatchOutcome::CheckpointRequired => {
                                send_push_end(
                                    &frame_tx,
                                    push_id,
                                    PUSH_END_CHECKPOINT_REQUIRED,
                                    "checkpoint required",
                                )
                                .await;
                                break 'push;
                            }
                            BatchOutcome::Records { bytes, next } => {
                                let frame = match wire::encode_queue_push_batch(push_id, &batch) {
                                    Ok(frame) => frame,
                                    Err(err) => {
                                        send_push_end(&frame_tx, push_id, PUSH_END_ERROR, &err.to_string()).await;
                                        break 'push;
                                    }
                                };
                                if frame_tx.send(frame).await.is_err() {
                                    break 'push;
                                }
                                from = next;
                                credit.consume(bytes);
                                continue 'push;
                            }
                        },
                        Err(err) => {
                            send_push_end(&frame_tx, push_id, PUSH_END_ERROR, &err).await;
                            break 'push;
                        }
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Follower side: the applier loop.
// ---------------------------------------------------------------------------

/// Control the follower sends back to the owner as it applies. The transport
/// encodes these to `QueuePushProgress` / `QueuePushReset` frames.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowerPushControl {
    /// Progress plus a credit refill (combined). Sent per applied batch so
    /// progress is prompt (confirm latency) and the budget stays topped up.
    /// `written_next` trails nothing and `durable_next` trails it only on a
    /// buffered queue, whose follower skips the per-batch fsync.
    Progress {
        durable_next: u64,
        written_next: u64,
        credit_add_bytes: u64,
    },
    /// Rewind the owner push cursor (the follower handled a checkpoint).
    Reset { from: u64 },
}

/// Outcome of durably applying one pushed batch.
pub enum FollowerApplyOutcome {
    /// Applied; here is the new log end (`written_next`), how far the log is
    /// synced (`durable_next`, equal unless the queue is buffered) and the
    /// bytes applied (the credit to return).
    Applied { durable_next: u64, written_next: u64, bytes: u64 },
    /// The batch needs a checkpoint (offset gap / epoch fence). The push stops
    /// so the worker can fall back to the proven pull+checkpoint path, then
    /// push again from the post-checkpoint cursor.
    CheckpointRequired,
    /// Fatal apply error; the applier stops.
    Error(String),
}

/// Why a follower push applier loop ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplierExit {
    /// The reader closed the batch channel (push torn down upstream).
    ReaderClosed,
    /// An apply needs a checkpoint; the caller should fall back to pull.
    CheckpointRequired,
    /// A fatal apply error.
    Error(String),
}

/// What the follower applier does with a received batch. A trait so the loop is
/// unit-testable without a live engine.
#[async_trait]
pub trait FollowerPushSink: Send + Sync + 'static {
    async fn apply_open(
        &self,
        _frames: BoxStream<'_, QueueReplicationReadOk>,
    ) -> FollowerApplyOutcome {
        FollowerApplyOutcome::Error("open follower batches unsupported".into())
    }
    async fn apply(&self, batch: QueueReplicationReadOk) -> FollowerApplyOutcome;

    /// Current durable end of the follower log, for keepalives on a buffered
    /// queue whose background sync moves it without any apply. `None` keeps
    /// the last reported value.
    async fn durable_next(&self) -> Option<u64> {
        None
    }
}

/// Input is polled by storage after each frame is staged, keeping one recovery
/// guard across the open group. No record clones and no detached apply task.
struct OpenBatchInput<'a> {
    first: Option<QueueReplicationReadOk>,
    rx: &'a mut mpsc::Receiver<QueueReplicationReadOk>,
    carry: &'a mut Option<QueueReplicationReadOk>,
    deadline: tokio::time::Instant,
    linger: std::time::Duration,
    deadline_policy: bool,
    lingered: bool,
    drain_only: bool,
    bytes: u64,
    limit: u64,
    tail: Option<(u64, u64)>,
}

impl OpenBatchInput<'_> {
    async fn next(&mut self) -> Option<QueueReplicationReadOk> {
        let frame = if let Some(first) = self.first.take() {
            first
        } else {
            if self.bytes >= self.limit || ((self.deadline_policy || self.drain_only) && !self.linger.is_zero()
                && tokio::time::Instant::now() >= self.deadline) { return None; }
            let next = match self.rx.try_recv() {
                Ok(frame) => Some(frame),
                Err(mpsc::error::TryRecvError::Disconnected) => None,
                Err(mpsc::error::TryRecvError::Empty) if self.drain_only || self.linger.is_zero() => None,
                Err(mpsc::error::TryRecvError::Empty) if self.deadline_policy => {
                    tokio::time::timeout_at(self.deadline, self.rx.recv()).await.ok().flatten()
                }
                Err(mpsc::error::TryRecvError::Empty) if !self.lingered => {
                    self.lingered = true;
                    tokio::time::timeout(self.linger, self.rx.recv()).await.ok().flatten()
                }
                Err(mpsc::error::TryRecvError::Empty) => None,
            }?;
            let continues = self.tail.is_some() && frame_start(&next) == self.tail;
            if !continues { *self.carry = Some(next); return None; }
            next
        };
        self.bytes = self.bytes.saturating_add(batch_bytes(&frame));
        self.tail = frame_tail(&frame);
        Some(frame)
    }
}

/// Drain in-order batches from the reader and apply them durably, sending a
/// combined progress+credit refill after each one. Ends when the reader channel
/// closes (push torn down) or an apply error occurs. The reader feeds
/// `batch_rx`; the transport encodes `control_tx` items to frames.
///
/// `from` is the starting durable cursor. When no batch arrives within
/// `keepalive` (the follower is caught up), a zero-credit progress frame is
/// sent so the owner's in-sync freshness check stays satisfied for an
/// idle-but-current follower (the pull path gets this for free via caught-up
/// polling). `keepalive` of zero disables the keepalive.
pub async fn run_follower_push_applier<S: FollowerPushSink>(
    sink: Arc<S>,
    mut batch_rx: mpsc::Receiver<QueueReplicationReadOk>,
    control_tx: mpsc::Sender<FollowerPushControl>,
    from: u64,
    tunables: StreamApplyTunablesFn,
) -> ApplierExit {
    // Microbatch the apply: pushed frames arrive small (the owner reads whatever
    // has trickled in), and applying each one separately means one follower fsync
    // per frame, which collapses durable-replication throughput on real disk. So
    // fold contiguous same-epoch frames into one apply. Under backlog the queued
    // frames fold immediately (zero added latency); when data only trickles, the
    // one-shot `apply_linger` gathers a few first. Bounds: at most one linger wait
    // per apply (so added latency is `apply_linger`, not unbounded), and a byte cap
    // so a deep backlog still applies in reasonable chunks. `apply_linger` is a
    // replication runtime setting (stream_apply_linger_us); 0 = drain-only.
    // `max_merge_bytes` is the other microbatch lever (replication runtime
    // setting stream_apply_max_merge_bytes): caps how large a coalesced apply can
    // grow, i.e. peak memory vs fsync-amortization. Pairs with apply_linger.

    let mut next = from;
    let mut durable = from;
    let mut carry: Option<QueueReplicationReadOk> = None;
    let mut drain_continuation = false;
    loop {
        // Read the tunables LIVE each iteration (not captured once at start), so a
        // runtime-settings change takes effect on the next apply with no push
        // restart.
        let knobs = tunables();
        let keepalive = std::time::Duration::from_millis(knobs.keepalive_ms);
        let apply_linger = std::time::Duration::from_micros(knobs.apply_linger_us);
        let max_merge_bytes = knobs.max_merge_bytes;
        let deadline_policy = knobs.fixed_deadline;
        let open_policy = knobs.open_batch;

        // First frame for this apply: a carried-over unmergeable frame, otherwise
        // a fresh receive. Ok(Some) = batch, Ok(None) = reader closed, Err(()) =
        // idle keepalive tick.
        let first = match carry.take() {
            Some(batch) => batch,
            None => {
                let received: Result<Option<QueueReplicationReadOk>, ()> = if keepalive.is_zero() {
                    Ok(batch_rx.recv().await)
                } else {
                    tokio::time::timeout(keepalive, batch_rx.recv())
                        .await
                        .map_err(|_| ())
                };
                match received {
                    Ok(Some(batch)) => batch,
                    Ok(None) => return ApplierExit::ReaderClosed,
                    Err(()) => {
                        // Caught-up keepalive: refresh progress, no credit.
                        if let Some(synced) = sink.durable_next().await {
                            durable = synced.max(durable).min(next);
                        }
                        if control_tx
                            .send(FollowerPushControl::Progress {
                                durable_next: durable,
                                written_next: next,
                                credit_add_bytes: 0,
                            })
                            .await
                            .is_err()
                        {
                            return ApplierExit::ReaderClosed;
                        }
                        continue;
                    }
                }
            }
        };

        // Retain the epoch for matching a queued continuation to the completed
        // history. A carried epoch/gap boundary starts ordinary collection.
        let applied_history = frame_tail(&first);
        let outcome = if matches!(open_policy, 1 | 3) && applied_history.is_some() {
            let input = OpenBatchInput {
                first: Some(first), rx: &mut batch_rx, carry: &mut carry,
                deadline: tokio::time::Instant::now() + apply_linger,
                linger: apply_linger, deadline_policy, lingered: false,
                drain_only: drain_continuation,
                bytes: 0, limit: max_merge_bytes, tail: None,
            };
            let frames = futures::stream::unfold(input, |mut input| async move {
                input.next().await.map(|frame| (frame, input))
            }).boxed();
            sink.apply_open(frames).await
        } else {
        // Fold as many contiguous frames as are ready (plus a one-shot linger).
        let mut merged = first;
        let mut lingered = false;
        // One budget starts when this apply's first frame is available. Further
        // arrivals do not renew it. Zero preserves drain-only behavior.
        let collection_deadline = tokio::time::Instant::now() + apply_linger;
        while batch_bytes(&merged) < max_merge_bytes {
            if deadline_policy
                && !apply_linger.is_zero()
                && tokio::time::Instant::now() >= collection_deadline
            {
                break;
            }
            let received = match batch_rx.try_recv() {
                Ok(batch) => Some(batch),
                Err(mpsc::error::TryRecvError::Empty) => {
                    if apply_linger.is_zero() {
                        None
                    } else if deadline_policy {
                        tokio::time::timeout_at(collection_deadline, batch_rx.recv())
                            .await
                            .ok()
                            .flatten()
                    } else if lingered {
                        None
                    } else {
                        lingered = true;
                        // Ok(None) = reader closed (apply what we have; the next
                        // loop's receive returns the close). Err = linger elapsed.
                        tokio::time::timeout(apply_linger, batch_rx.recv())
                            .await
                            .ok()
                            .flatten()
                    }
                }
                Err(mpsc::error::TryRecvError::Disconnected) => None,
            };
            match received {
                Some(batch) => match try_fold_pushed(&mut merged, batch) {
                    Ok(()) => continue,
                    Err(unmerged) => {
                        carry = Some(unmerged);
                        break;
                    }
                },
                None => break,
            }
        }

            if open_policy == 2 && frame_tail(&merged).is_some() {
                sink.apply_open(futures::stream::iter([merged]).boxed()).await
            } else {
                sink.apply(merged).await
            }
        };
        match outcome {
            FollowerApplyOutcome::Applied {
                durable_next,
                written_next,
                bytes,
            } => {
                next = written_next;
                durable = durable_next.min(written_next);
                // Snapshot readiness immediately after successful application.
                // Waiting for progress-channel capacity must not turn a later
                // arrival into proof of backlog at the completion boundary.
                drain_continuation = false;
                if open_policy == 3 {
                    if carry.is_none() {
                        carry = batch_rx.try_recv().ok();
                    }
                    drain_continuation = matches!(
                        (carry.as_ref().and_then(frame_start), applied_history),
                        (Some((epoch, start)), Some((applied_epoch, _)))
                            if (epoch, start) == (applied_epoch, next)
                    );
                }
                if control_tx
                    .send(FollowerPushControl::Progress {
                        durable_next: durable,
                        written_next,
                        credit_add_bytes: bytes,
                    })
                    .await
                    .is_err()
                {
                    return ApplierExit::ReaderClosed;
                }
            }
            FollowerApplyOutcome::CheckpointRequired => {
                return ApplierExit::CheckpointRequired;
            }
            FollowerApplyOutcome::Error(err) => return ApplierExit::Error(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::ReplicationEventRecord;
    use fibril_broker::replication::StreamApplyTunables;
    use std::sync::Mutex;
    use std::time::Duration;

    const REC_BYTES: u64 = 100;

    /// Fixed tunables for a test (no live changes).
    fn fixed_tunables(
        keepalive_ms: u64,
        apply_linger_us: u64,
        max_merge_bytes: u64,
    ) -> StreamApplyTunablesFn {
        Arc::new(move || StreamApplyTunables {
            keepalive_ms,
            apply_linger_us,
            max_merge_bytes,
            fixed_deadline: false,
            open_batch: 0,
        })
    }

    /// Same applier with the experimental batching policies forced on.
    fn run_follower_push_applier_with_policy<S: FollowerPushSink>(
        sink: Arc<S>,
        batch_rx: mpsc::Receiver<QueueReplicationReadOk>,
        control_tx: mpsc::Sender<FollowerPushControl>,
        from: u64,
        tunables: StreamApplyTunablesFn,
        fixed_deadline: bool,
        open_batch: u8,
    ) -> impl std::future::Future<Output = ApplierExit> {
        let tunables: StreamApplyTunablesFn = Arc::new(move || StreamApplyTunables {
            fixed_deadline,
            open_batch,
            ..tunables()
        });
        run_follower_push_applier(sink, batch_rx, control_tx, from, tunables)
    }

    /// A mock queue log of `total` records (REC_BYTES each, offsets 0..total).
    /// Each read returns the records from `from` that fit
    /// in `max_bytes`; a caught-up read sleeps for the long-poll then returns
    /// empty (so the loop parks but the select still reacts to control).
    struct MockSource {
        total: u64,
        progress: Mutex<Vec<u64>>,
        config: Mutex<Option<OwnerPushConfig>>,
        reads: Mutex<Vec<(usize, usize, u64)>>,
        read_gate: Option<Arc<tokio::sync::Semaphore>>,
    }

    #[async_trait]
    impl OwnerPushSource for MockSource {
        fn push_config(&self) -> Option<OwnerPushConfig> {
            *self.config.lock().unwrap()
        }

        async fn read(
            &self,
            _topic: &str,
            _partition: Partition,
            _group: Option<&str>,
            from: u64,
            max_records: usize,
            max_bytes: usize,
            max_wait_ms: u64,
        ) -> Result<QueueReplicationReadOk, String> {
            self.reads
                .lock()
                .unwrap()
                .push((max_records, max_bytes, max_wait_ms));
            if let Some(gate) = &self.read_gate {
                gate.acquire().await.unwrap().forget();
            }
            let mut records = Vec::new();
            let mut off = from;
            let mut bytes = 0u64;
            while off < self.total
                && records.len() < max_records
                && (records.is_empty() || bytes + REC_BYTES <= max_bytes as u64)
            {
                records.push(ReplicationEventRecord {
                    offset: off,
                    payload: vec![0u8; REC_BYTES as usize],
                });
                bytes += REC_BYTES;
                off += 1;
            }
            if records.is_empty() {
                tokio::time::sleep(Duration::from_millis(max_wait_ms.min(50))).await;
            }
            Ok(ReplicationEventRead::Batch {
                epoch: 1,
                requested_offset: from,
                next_offset: off,
                records,
            })
        }

        fn record_progress(
            &self,
            _topic: &str,
            _partition: Partition,
            _group: Option<&str>,
            _reporter: &str,
            durable_next: u64,
        ) {
            self.progress.lock().unwrap().push(durable_next);
        }
    }

    fn start(credit_bytes: u64) -> QueuePushStart {
        QueuePushStart {
            reporter_epoch: None,
            topic: "orders".into(),
            group: None,
            partition: Partition::new(0),
            from: 0,
            credit_bytes,
            reporter_node_id: Some("broker-2".into()),
        }
    }

    fn cfg() -> OwnerPushConfig {
        OwnerPushConfig {
            max_records: 1,
            max_batch_bytes: REC_BYTES as usize,
            long_poll_ms: 50,
        }
    }

    async fn recv_offsets(rx: &mut mpsc::Receiver<Frame>, n: usize) -> Vec<u64> {
        let mut offs = Vec::new();
        for _ in 0..n {
            let frame = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("batch frame timed out")
                .expect("push sender closed early");
            let batch = wire::decode_queue_push_batch(&frame).unwrap();
            if let ReplicationEventRead::Batch { records, .. } = batch {
                for r in records {
                    offs.push(r.offset);
                }
            }
        }
        offs
    }

    #[tokio::test]
    async fn credit_gates_then_refill_resumes_and_records_progress() {
        let source = Arc::new(MockSource {
            total: 5,
            progress: Mutex::new(Vec::new()),
            config: Mutex::new(None),
            reads: Mutex::new(Vec::new()),
            read_gate: None,
        });
        let (frame_tx, mut frame_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(8);

        // Credit for exactly 2 records (max_batch_bytes = 1 record, so 2 batches).
        let task = tokio::spawn(run_owner_queue_push(
            source.clone(),
            frame_tx,
            7,
            start(2 * REC_BYTES),
            control_rx,
            cfg(),
        ));

        // Two batches flow, then the push parks (credit exhausted).
        assert_eq!(recv_offsets(&mut frame_rx, 2).await, vec![0, 1]);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), frame_rx.recv())
                .await
                .is_err(),
            "credit gate must hold the 3rd batch"
        );

        // Refill + progress: 3 more batches flow.
        control_tx
            .send(OwnerPushControl::Progress {
                durable_next: 2, written_next: 2,
                credit_add_bytes: 3 * REC_BYTES,
            })
            .await
            .unwrap();
        assert_eq!(recv_offsets(&mut frame_rx, 3).await, vec![2, 3, 4]);
        assert_eq!(source.progress.lock().unwrap().as_slice(), &[2]);

        control_tx.send(OwnerPushControl::Stop).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("sender did not stop")
            .unwrap();
    }

    #[tokio::test]
    async fn reset_rewinds_the_cursor() {
        let source = Arc::new(MockSource {
            total: 10,
            progress: Mutex::new(Vec::new()),
            config: Mutex::new(None),
            reads: Mutex::new(Vec::new()),
            read_gate: None,
        });
        let (frame_tx, mut frame_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(8);

        // Credit for exactly 3 records, so the sender sends 0,1,2 then parks.
        // That makes the reset deterministic (no batches race ahead of it).
        let task = tokio::spawn(run_owner_queue_push(
            source,
            frame_tx,
            1,
            start(3 * REC_BYTES),
            control_rx,
            cfg(),
        ));

        assert_eq!(recv_offsets(&mut frame_rx, 3).await, vec![0, 1, 2]);
        // Rewind the cursor, then refill so it reads again from 0.
        control_tx
            .send(OwnerPushControl::Reset {
                from: 0,
            })
            .await
            .unwrap();
        control_tx
            .send(OwnerPushControl::Progress {
                durable_next: 0, written_next: 0,
                credit_add_bytes: REC_BYTES,
            })
            .await
            .unwrap();
        let after = recv_offsets(&mut frame_rx, 1).await;
        assert_eq!(after, vec![0], "reset must rewind the cursor to 0");

        control_tx.send(OwnerPushControl::Stop).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }

    #[tokio::test]
    async fn oversized_records_repay_debt_without_inflating_the_window() {
        let source = Arc::new(MockSource {
            total: 10,
            progress: Mutex::new(Vec::new()),
            config: Mutex::new(None),
            reads: Mutex::new(Vec::new()),
            read_gate: None,
        });
        let (frame_tx, mut frame_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(8);
        // One-and-a-half records of credit permits two records, then owes half.
        let task = tokio::spawn(run_owner_queue_push(
            source.clone(),
            frame_tx,
            1,
            start(150),
            control_rx,
            cfg(),
        ));
        assert_eq!(recv_offsets(&mut frame_rx, 2).await, vec![0, 1]);
        for offset in 2..6 {
            control_tx
                .send(OwnerPushControl::Progress {
                    durable_next: offset, written_next: offset,
                    credit_add_bytes: REC_BYTES,
                })
                .await
                .unwrap();
            assert_eq!(recv_offsets(&mut frame_rx, 1).await, vec![offset]);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), frame_rx.recv())
                    .await
                    .is_err()
            );
        }
        // Every returned record restores only half a record of available credit.
        let budgets: Vec<_> = source.reads.lock().unwrap().iter().map(|r| r.1).collect();
        assert_eq!(budgets, vec![100, 50, 50, 50, 50, 50]);
        control_tx.send(OwnerPushControl::Stop).await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn live_read_limits_apply_to_the_next_batch_on_an_existing_push() {
        let source = Arc::new(MockSource {
            total: 10,
            progress: Mutex::new(Vec::new()),
            config: Mutex::new(Some(OwnerPushConfig {
                max_records: 2,
                max_batch_bytes: 200,
                long_poll_ms: 17,
            })),
            reads: Mutex::new(Vec::new()),
            read_gate: None,
        });
        let (frame_tx, mut frame_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_owner_queue_push(
            source.clone(),
            frame_tx,
            1,
            start(200),
            control_rx,
            cfg(),
        ));
        assert_eq!(recv_offsets(&mut frame_rx, 1).await, vec![0, 1]);
        *source.config.lock().unwrap() = Some(OwnerPushConfig {
            max_records: 1,
            max_batch_bytes: 100,
            long_poll_ms: 23,
        });
        control_tx
            .send(OwnerPushControl::Progress {
                durable_next: 2, written_next: 2,
                credit_add_bytes: 200,
            })
            .await
            .unwrap();
        assert_eq!(recv_offsets(&mut frame_rx, 2).await, vec![2, 3]);
        assert_eq!(
            *source.reads.lock().unwrap(),
            vec![(2, 200, 17), (1, 100, 23), (1, 100, 23)]
        );
        control_tx.send(OwnerPushControl::Stop).await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn progress_preserves_a_pending_read_but_reset_and_stop_cancel_it() {
        async fn until(check: impl Fn() -> bool) {
            tokio::time::timeout(Duration::from_secs(2), async {
                while !check() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("control was not processed");
        }
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let source = Arc::new(MockSource {
            total: 10,
            progress: Mutex::new(Vec::new()),
            config: Mutex::new(None),
            reads: Mutex::new(Vec::new()),
            read_gate: Some(gate.clone()),
        });
        let (frame_tx, mut frame_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_owner_queue_push(
            source.clone(),
            frame_tx,
            1,
            start(REC_BYTES),
            control_rx,
            cfg(),
        ));
        until(|| source.reads.lock().unwrap().len() == 1).await;
        for count in 1..=3 {
            control_tx
                .send(OwnerPushControl::Progress {
                    durable_next: 0, written_next: 0,
                    credit_add_bytes: 0,
                })
                .await
                .unwrap();
            until(|| source.progress.lock().unwrap().len() == count).await;
        }
        gate.add_permits(1);
        assert_eq!(recv_offsets(&mut frame_rx, 1).await, vec![0]);
        assert_eq!(
            source.reads.lock().unwrap().len(),
            1,
            "progress must not restart a scan"
        );
        control_tx
            .send(OwnerPushControl::Progress {
                durable_next: 1, written_next: 1,
                credit_add_bytes: REC_BYTES,
            })
            .await
            .unwrap();
        until(|| source.reads.lock().unwrap().len() == 2).await;
        control_tx
            .send(OwnerPushControl::Reset {
                from: 4,
            })
            .await
            .unwrap();
        until(|| source.reads.lock().unwrap().len() == 3).await;
        gate.add_permits(1);
        assert_eq!(recv_offsets(&mut frame_rx, 1).await, vec![4]);
        control_tx
            .send(OwnerPushControl::Progress {
                durable_next: 5, written_next: 5,
                credit_add_bytes: REC_BYTES,
            })
            .await
            .unwrap();
        until(|| source.reads.lock().unwrap().len() == 4).await;
        control_tx.send(OwnerPushControl::Stop).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert!(frame_rx.recv().await.is_none());
    }

    // ---- follower applier ----

    struct MockSink {
        // Scripted outcomes, one per applied batch (front = next).
        outcomes: Mutex<std::collections::VecDeque<FollowerApplyOutcome>>,
        applied: Mutex<Vec<u64>>, // first offset of each applied batch
    }

    #[async_trait]
    impl FollowerPushSink for MockSink {
        async fn apply(&self, batch: QueueReplicationReadOk) -> FollowerApplyOutcome {
            if let ReplicationEventRead::Batch { records, .. } = &batch
                && let Some(first) = records.first()
            {
                self.applied.lock().unwrap().push(first.offset);
            }
            self.outcomes
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(FollowerApplyOutcome::Error("no scripted outcome".into()))
        }
    }

    fn batch_from(first_offset: u64, count: u64) -> QueueReplicationReadOk {
        let records = (0..count)
            .map(|i| ReplicationEventRecord {
                offset: first_offset + i,
                payload: vec![0u8; REC_BYTES as usize],
            })
            .collect();
        ReplicationEventRead::Batch {
            epoch: 1,
            requested_offset: first_offset,
            next_offset: first_offset + count,
            records,
        }
    }

    struct OpenSink {
        staged: mpsc::UnboundedSender<u64>,
        sync: Arc<tokio::sync::Notify>,
    }
    #[async_trait]
    impl FollowerPushSink for OpenSink {
        async fn apply(&self, _: QueueReplicationReadOk) -> FollowerApplyOutcome { panic!("unexpected closed apply") }
        async fn apply_open(&self, mut frames: BoxStream<'_, QueueReplicationReadOk>) -> FollowerApplyOutcome {
            let mut next = 0;
            let mut bytes = 0;
            while let Some(frame) = frames.next().await {
                next = frame_tail(&frame).unwrap().1;
                bytes += batch_bytes(&frame);
                self.staged.send(next).unwrap();
            }
            self.sync.notified().await;
            FollowerApplyOutcome::Applied { durable_next: next, written_next: next, bytes }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn open_batch_stages_arrivals_before_cutoff_and_only_reports_after_sync() {
        let (tx, rx) = mpsc::channel(8);
        let (control, mut controls) = mpsc::channel(8);
        let (staged, mut stages) = mpsc::unbounded_channel();
        let sync = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(run_follower_push_applier_with_policy(
            Arc::new(OpenSink { staged, sync: sync.clone() }), rx, control, 0,
            fixed_tunables(0, 20_000, 10_000), true, 1,
        ));
        tx.send(batch_from(0, 1)).await.unwrap();
        assert_eq!(stages.recv().await, Some(1));
        tokio::time::advance(Duration::from_millis(5)).await;
        tx.send(batch_from(1, 1)).await.unwrap();
        assert_eq!(stages.recv().await, Some(2));
        assert!(controls.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(17)).await;
        tokio::task::yield_now().await;
        tx.send(batch_from(2, 1)).await.unwrap();
        tokio::task::yield_now().await;
        assert!(stages.try_recv().is_err(), "arrival after cutoff belongs to next batch");
        assert!(controls.try_recv().is_err(), "cutoff alone must not report durability");
        sync.notify_one();
        assert_eq!(controls.recv().await, Some(FollowerPushControl::Progress {
            durable_next: 2, written_next: 2, credit_add_bytes: 2 * REC_BYTES,
        }));
        assert_eq!(stages.recv().await, Some(3));
        drop(tx);
        sync.notify_one();
        assert_eq!(controls.recv().await, Some(FollowerPushControl::Progress {
            durable_next: 3, written_next: 3, credit_add_bytes: REC_BYTES,
        }));
        assert_eq!(task.await.unwrap(), ApplierExit::ReaderClosed);
    }

    #[tokio::test(start_paused = true)]
    async fn deferred_open_control_collects_first_then_uses_the_same_sync_boundary() {
        let (tx, rx) = mpsc::channel(8);
        let (control, mut controls) = mpsc::channel(8);
        let (staged, mut stages) = mpsc::unbounded_channel();
        let sync = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(run_follower_push_applier_with_policy(
            Arc::new(OpenSink { staged, sync: sync.clone() }), rx, control, 0,
            fixed_tunables(0, 20_000, 10_000), true, 2,
        ));
        tx.send(batch_from(0, 1)).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(5)).await;
        tx.send(batch_from(1, 1)).await.unwrap();
        tokio::task::yield_now().await;
        assert!(stages.try_recv().is_err());
        assert!(controls.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(17)).await;
        assert_eq!(stages.recv().await, Some(2));
        assert!(controls.try_recv().is_err());
        sync.notify_one();
        assert_eq!(controls.recv().await, Some(FollowerPushControl::Progress {
            durable_next: 2, written_next: 2, credit_add_bytes: 2 * REC_BYTES,
        }));
        drop(tx);
        assert_eq!(task.await.unwrap(), ApplierExit::ReaderClosed);
    }

    #[tokio::test(start_paused = true)]
    async fn open_batch_cuts_on_staging_elapsed_deadline_size_and_epoch() {
        for mode in 0..3 {
            let (tx, mut rx) = mpsc::channel(8);
            let mut carry = None;
            let mut next = batch_from(1, 1);
            if mode == 2 {
                if let ReplicationEventRead::Batch { epoch, .. } = &mut next { *epoch = 2; }
            }
            tx.send(next).await.unwrap();
            let mut input = OpenBatchInput {
                first: Some(batch_from(0, 1)), rx: &mut rx, carry: &mut carry,
                deadline: tokio::time::Instant::now() + Duration::from_millis(20),
                linger: Duration::from_millis(20), deadline_policy: true, lingered: false, drain_only: false,
                bytes: 0, limit: if mode == 1 { REC_BYTES } else { 10_000 }, tail: None,
            };
            assert_eq!(frame_tail(&input.next().await.unwrap()).unwrap().1, 1);
            if mode == 0 { tokio::time::advance(Duration::from_millis(21)).await; }
            assert!(input.next().await.is_none());
            drop(input);
            if mode == 2 { assert!(carry.is_some()); } else { assert!(rx.try_recv().is_ok()); }
        }
    }

    struct ContinuationSink {
        closed: mpsc::UnboundedSender<(u64, tokio::time::Instant)>,
        sync: Arc<tokio::sync::Notify>,
        fail: bool,
    }
    #[async_trait]
    impl FollowerPushSink for ContinuationSink {
        async fn apply(&self, _: QueueReplicationReadOk) -> FollowerApplyOutcome {
            panic!("unexpected closed apply")
        }
        async fn apply_open(&self, mut frames: BoxStream<'_, QueueReplicationReadOk>) -> FollowerApplyOutcome {
            let mut next = 0;
            let mut bytes = 0;
            while let Some(frame) = frames.next().await {
                next = frame_tail(&frame).unwrap().1;
                bytes += batch_bytes(&frame);
            }
            self.closed.send((next, tokio::time::Instant::now())).unwrap();
            self.sync.notified().await;
            if self.fail { return FollowerApplyOutcome::Error("sync failed".into()); }
            FollowerApplyOutcome::Applied { durable_next: next, written_next: next, bytes }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn continuation_drains_backlog_then_restores_idle_collection() {
        let (tx, rx) = mpsc::channel(8);
        let (control, mut controls) = mpsc::channel(8);
        let (closed, mut closures) = mpsc::unbounded_channel();
        let sync = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(run_follower_push_applier_with_policy(
            Arc::new(ContinuationSink { closed, sync: sync.clone(), fail: false }),
            rx, control, 0, fixed_tunables(0, 20_000, 10_000), true, 3,
        ));
        let start = tokio::time::Instant::now();
        tx.send(batch_from(0, 1)).await.unwrap();
        let first = closures.recv().await.unwrap();
        assert_eq!(first.0, 1);
        assert!(first.1.duration_since(start) >= Duration::from_millis(20));
        tx.send(batch_from(1, 1)).await.unwrap();
        tx.send(batch_from(2, 1)).await.unwrap();
        let completed_at = tokio::time::Instant::now();
        sync.notify_one();
        assert_eq!(controls.recv().await, Some(FollowerPushControl::Progress {
            durable_next: 1, written_next: 1, credit_add_bytes: REC_BYTES,
        }));
        assert_eq!(closures.recv().await, Some((3, completed_at)), "queued continuation must close without waiting");
        assert!(controls.try_recv().is_err(), "staging and closure are not durable progress");
        sync.notify_one();
        assert_eq!(controls.recv().await, Some(FollowerPushControl::Progress {
            durable_next: 3, written_next: 3, credit_add_bytes: 2 * REC_BYTES,
        }));
        let idle_start = tokio::time::Instant::now();
        tx.send(batch_from(3, 1)).await.unwrap();
        let third = closures.recv().await.unwrap();
        assert_eq!(third.0, 4);
        assert!(third.1.duration_since(idle_start) >= Duration::from_millis(20));
        drop(tx);
        sync.notify_one();
        assert!(controls.recv().await.is_some());
        assert_eq!(task.await.unwrap(), ApplierExit::ReaderClosed);
    }

    #[tokio::test(start_paused = true)]
    async fn continuation_epoch_boundary_restores_collection() {
        let (tx, rx) = mpsc::channel(8);
        let (control, mut controls) = mpsc::channel(8);
        let (closed, mut closures) = mpsc::unbounded_channel();
        let sync = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(run_follower_push_applier_with_policy(
            Arc::new(ContinuationSink { closed, sync: sync.clone(), fail: false }),
            rx, control, 0, fixed_tunables(0, 20_000, 10_000), true, 3,
        ));
        tx.send(batch_from(0, 1)).await.unwrap();
        closures.recv().await.unwrap();
        let mut next = batch_from(1, 1);
        if let ReplicationEventRead::Batch { epoch, .. } = &mut next { *epoch = 2; }
        tx.send(next).await.unwrap();
        let now = tokio::time::Instant::now();
        sync.notify_one();
        controls.recv().await.unwrap();
        let second = closures.recv().await.unwrap();
        assert!(second.1.duration_since(now) >= Duration::from_millis(20));
        drop(tx);
        sync.notify_one();
        controls.recv().await.unwrap();
        assert_eq!(task.await.unwrap(), ApplierExit::ReaderClosed);
    }

    #[tokio::test(start_paused = true)]
    async fn continuation_does_not_advance_after_failed_apply() {
        let (tx, rx) = mpsc::channel(8);
        let (control, mut controls) = mpsc::channel(8);
        let (closed, mut closures) = mpsc::unbounded_channel();
        let sync = Arc::new(tokio::sync::Notify::new());
        let task = tokio::spawn(run_follower_push_applier_with_policy(
            Arc::new(ContinuationSink { closed, sync: sync.clone(), fail: true }),
            rx, control, 0, fixed_tunables(0, 20_000, 10_000), true, 3,
        ));
        tx.send(batch_from(0, 1)).await.unwrap();
        closures.recv().await.unwrap();
        tx.send(batch_from(1, 1)).await.unwrap();
        sync.notify_one();
        assert!(matches!(task.await.unwrap(), ApplierExit::Error(_)));
        assert!(closures.recv().await.is_none());
        assert!(controls.recv().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn continuation_drain_retains_size_and_staging_time_bounds() {
        for time_bound in [false, true] {
            let (tx, mut rx) = mpsc::channel(8);
            let mut carry = None;
            tx.send(batch_from(1, 1)).await.unwrap();
            let mut input = OpenBatchInput {
                first: Some(batch_from(0, 1)), rx: &mut rx, carry: &mut carry,
                deadline: tokio::time::Instant::now() + Duration::from_millis(20),
                linger: Duration::from_millis(20), deadline_policy: true, lingered: false,
                drain_only: true, bytes: 0, limit: if time_bound { 10_000 } else { REC_BYTES }, tail: None,
            };
            assert!(input.next().await.is_some());
            if time_bound { tokio::time::advance(Duration::from_millis(21)).await; }
            assert!(input.next().await.is_none());
            drop(input);
            assert!(rx.try_recv().is_ok(), "bounded continuation preserves remaining input");
        }
    }

    struct DeadlineSink {
        applied: mpsc::UnboundedSender<(u64, u64, tokio::time::Instant)>,
    }

    #[async_trait]
    impl FollowerPushSink for DeadlineSink {
        async fn apply(&self, batch: QueueReplicationReadOk) -> FollowerApplyOutcome {
            let bytes = batch_bytes(&batch);
            let ReplicationEventRead::Batch {
                requested_offset,
                next_offset,
                ..
            } = batch
            else {
                return FollowerApplyOutcome::CheckpointRequired;
            };
            self.applied
                .send((requested_offset, next_offset, tokio::time::Instant::now()))
                .unwrap();
            FollowerApplyOutcome::Applied {
                durable_next: next_offset, written_next: next_offset,
                bytes,
            }
        }
    }

    fn deadline_harness(
        policy: bool,
        linger_us: u64,
        max_bytes: u64,
    ) -> (
        mpsc::Sender<QueueReplicationReadOk>,
        mpsc::UnboundedReceiver<(u64, u64, tokio::time::Instant)>,
        mpsc::Receiver<FollowerPushControl>,
        tokio::task::JoinHandle<ApplierExit>,
    ) {
        let (tx, rx) = mpsc::channel(8);
        let (applied, applied_rx) = mpsc::unbounded_channel();
        let (control, control_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_follower_push_applier_with_policy(
            Arc::new(DeadlineSink { applied }),
            rx,
            control,
            0,
            fixed_tunables(0, linger_us, max_bytes),
            policy,
            0,
        ));
        (tx, applied_rx, control_rx, task)
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_collects_later_frames_without_renewing_budget() {
        let (tx, mut applied, mut controls, task) = deadline_harness(true, 20_000, 10_000);
        let start = tokio::time::Instant::now();
        tx.send(batch_from(0, 1)).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(5)).await;
        tx.send(batch_from(1, 1)).await.unwrap();
        tokio::task::yield_now().await;
        assert!(
            applied.try_recv().is_err(),
            "second arrival must not end collection"
        );
        tokio::time::advance(Duration::from_millis(10)).await;
        tx.send(batch_from(2, 1)).await.unwrap();
        tokio::task::yield_now().await;
        assert!(applied.try_recv().is_err());
        tokio::time::advance(Duration::from_millis(7)).await;
        let (first, next, at) = applied.recv().await.unwrap();
        assert_eq!((first, next), (0, 3));
        assert!(
            at - start <= Duration::from_millis(22),
            "arrivals must not reset deadline"
        );
        assert_eq!(
            controls.recv().await.unwrap(),
            FollowerPushControl::Progress {
                durable_next: 3, written_next: 3,
                credit_add_bytes: 3 * REC_BYTES,
            }
        );
        drop(tx);
        assert_eq!(task.await.unwrap(), ApplierExit::ReaderClosed);
    }

    #[tokio::test(start_paused = true)]
    async fn original_policy_still_closes_after_one_extra_arrival() {
        let (tx, mut applied, _controls, task) = deadline_harness(false, 20_000, 10_000);
        let start = tokio::time::Instant::now();
        tx.send(batch_from(0, 1)).await.unwrap();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(5)).await;
        tx.send(batch_from(1, 1)).await.unwrap();
        let (first, next, at) = applied.recv().await.unwrap();
        assert_eq!((first, next), (0, 2));
        assert!(at - start < Duration::from_millis(20));
        drop(tx);
        assert_eq!(task.await.unwrap(), ApplierExit::ReaderClosed);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_byte_bound_flushes_without_waiting() {
        let (tx, mut applied, _controls, task) = deadline_harness(true, 20_000, 2 * REC_BYTES);
        let start = tokio::time::Instant::now();
        tx.send(batch_from(0, 1)).await.unwrap();
        tx.send(batch_from(1, 1)).await.unwrap();
        let (first, next, at) = applied.recv().await.unwrap();
        assert_eq!((first, next), (0, 2));
        assert!(at - start < Duration::from_millis(20));
        drop(tx);
        assert_eq!(task.await.unwrap(), ApplierExit::ReaderClosed);
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_preserves_gap_and_epoch_boundaries() {
        for change_epoch in [false, true] {
            let (tx, mut applied, _controls, task) = deadline_harness(true, 20_000, 10_000);
            let start = tokio::time::Instant::now();
            tx.send(batch_from(0, 1)).await.unwrap();
            let mut second = batch_from(if change_epoch { 1 } else { 10 }, 1);
            if change_epoch {
                let ReplicationEventRead::Batch { epoch, .. } = &mut second else {
                    unreachable!()
                };
                *epoch = 2;
            }
            tx.send(second).await.unwrap();
            let (first, next, at) = applied.recv().await.unwrap();
            assert_eq!((first, next), (0, 1));
            assert!(at - start < Duration::from_millis(20));
            drop(tx);
            let (first, next, _) = applied.recv().await.unwrap();
            assert_eq!((first, next), if change_epoch { (1, 2) } else { (10, 11) });
            assert_eq!(task.await.unwrap(), ApplierExit::ReaderClosed);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_zero_and_reader_close_flush_promptly() {
        for zero in [false, true] {
            let (tx, mut applied, _controls, task) =
                deadline_harness(true, if zero { 0 } else { 20_000 }, 10_000);
            let start = tokio::time::Instant::now();
            tx.send(batch_from(0, 1)).await.unwrap();
            if zero {
                tokio::task::yield_now().await;
            }
            drop(tx);
            let (first, next, at) = applied.recv().await.unwrap();
            assert_eq!((first, next), (0, 1));
            assert!(at - start < Duration::from_millis(20));
            assert_eq!(task.await.unwrap(), ApplierExit::ReaderClosed);
        }
    }

    // Contiguous same-epoch frames that are already queued fold into ONE apply
    // (one follower fsync) and report a single combined progress + credit refill.
    #[tokio::test]
    async fn applier_coalesces_contiguous_frames_into_one_apply() {
        let sink = Arc::new(MockSink {
            outcomes: Mutex::new(
                [FollowerApplyOutcome::Applied {
                    durable_next: 5, written_next: 5,
                    bytes: 5 * REC_BYTES,
                }]
                .into(),
            ),
            applied: Mutex::new(Vec::new()),
        });
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_follower_push_applier(
            sink.clone(),
            batch_rx,
            control_tx,
            0,
            fixed_tunables(0, 0, 16 * 1024 * 1024),
        ));

        // Both queued before the applier drains -> they fold (0..2 then 2..5).
        batch_tx.send(batch_from(0, 2)).await.unwrap();
        batch_tx.send(batch_from(2, 3)).await.unwrap();

        assert_eq!(
            control_rx.recv().await.unwrap(),
            FollowerPushControl::Progress {
                durable_next: 5, written_next: 5,
                credit_add_bytes: 5 * REC_BYTES,
            },
            "two contiguous frames coalesce into one progress + combined credit"
        );
        assert_eq!(
            sink.applied.lock().unwrap().as_slice(),
            &[0],
            "only one apply (started at the first frame's offset)"
        );

        drop(batch_tx); // reader closed -> applier ends
        let exit = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("applier did not stop")
            .unwrap();
        assert_eq!(exit, ApplierExit::ReaderClosed);
    }

    // The applier reads the tunables LIVE once per apply iteration (not captured
    // once at start), which is what lets a runtime-settings change propagate
    // mid-push. (The source side - a config change reaching the reader - is
    // covered by broker stream_apply_tunables_reads_live_config.)
    #[tokio::test]
    async fn applier_reads_tunables_live_each_iteration() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let sink = Arc::new(MockSink {
            outcomes: Mutex::new(
                [
                    FollowerApplyOutcome::Applied {
                        durable_next: 2, written_next: 2,
                        bytes: 2 * REC_BYTES,
                    },
                    FollowerApplyOutcome::Applied {
                        durable_next: 5, written_next: 5,
                        bytes: 3 * REC_BYTES,
                    },
                ]
                .into(),
            ),
            applied: Mutex::new(Vec::new()),
        });
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let (control_tx, mut control_rx) = mpsc::channel(8);

        // max_merge_bytes = 1 so frames never fold (each batch is its own apply);
        // the closure tallies how many times the applier reads it.
        let reads = Arc::new(AtomicUsize::new(0));
        let reads_in = reads.clone();
        let tunables: StreamApplyTunablesFn = Arc::new(move || {
            reads_in.fetch_add(1, Ordering::Relaxed);
            StreamApplyTunables {
                keepalive_ms: 0,
                apply_linger_us: 0,
                max_merge_bytes: 1,
                fixed_deadline: false,
                open_batch: 0,
            }
        });

        let task = tokio::spawn(run_follower_push_applier(
            sink.clone(),
            batch_rx,
            control_tx,
            0,
            tunables,
        ));

        batch_tx.send(batch_from(0, 2)).await.unwrap();
        control_rx.recv().await.unwrap();
        batch_tx.send(batch_from(2, 3)).await.unwrap();
        control_rx.recv().await.unwrap();

        drop(batch_tx);
        let exit = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("applier did not stop")
            .unwrap();
        assert_eq!(exit, ApplierExit::ReaderClosed);

        assert!(
            reads.load(Ordering::Relaxed) >= 2,
            "tunables read live once per apply iteration; reads={}",
            reads.load(Ordering::Relaxed)
        );
        assert_eq!(
            sink.applied.lock().unwrap().as_slice(),
            &[0, 2],
            "two separate applies (max_merge_bytes=1 disables folding)"
        );
    }

    // A non-contiguous frame (offset gap) must NOT fold: the first frame applies,
    // the gapped frame is carried to its own apply. Same shape guards an epoch
    // change or a checkpoint frame from being merged into the wrong batch.
    #[tokio::test]
    async fn applier_does_not_fold_across_offset_gap() {
        let sink = Arc::new(MockSink {
            outcomes: Mutex::new(
                [
                    FollowerApplyOutcome::Applied {
                        durable_next: 2, written_next: 2,
                        bytes: 2 * REC_BYTES,
                    },
                    FollowerApplyOutcome::Applied {
                        durable_next: 11, written_next: 11,
                        bytes: REC_BYTES,
                    },
                ]
                .into(),
            ),
            applied: Mutex::new(Vec::new()),
        });
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_follower_push_applier(
            sink.clone(),
            batch_rx,
            control_tx,
            0,
            fixed_tunables(0, 0, 16 * 1024 * 1024),
        ));

        batch_tx.send(batch_from(0, 2)).await.unwrap(); // 0..2
        batch_tx.send(batch_from(10, 1)).await.unwrap(); // gap: 10..11

        assert_eq!(
            control_rx.recv().await.unwrap(),
            FollowerPushControl::Progress {
                durable_next: 2, written_next: 2,
                credit_add_bytes: 2 * REC_BYTES,
            }
        );
        assert_eq!(
            control_rx.recv().await.unwrap(),
            FollowerPushControl::Progress {
                durable_next: 11, written_next: 11,
                credit_add_bytes: REC_BYTES,
            }
        );
        assert_eq!(sink.applied.lock().unwrap().as_slice(), &[0, 10]);

        drop(batch_tx);
        let exit = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("applier did not stop")
            .unwrap();
        assert_eq!(exit, ApplierExit::ReaderClosed);
    }

    #[tokio::test]
    async fn applier_exits_checkpoint_required_for_fallback() {
        let sink = Arc::new(MockSink {
            outcomes: Mutex::new([FollowerApplyOutcome::CheckpointRequired].into()),
            applied: Mutex::new(Vec::new()),
        });
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let (control_tx, _control_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_follower_push_applier(
            sink,
            batch_rx,
            control_tx,
            0,
            fixed_tunables(0, 0, 16 * 1024 * 1024),
        ));

        batch_tx.send(batch_from(40, 1)).await.unwrap();
        let exit = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .expect("applier did not stop")
            .unwrap();
        assert_eq!(exit, ApplierExit::CheckpointRequired);
    }

    // Regression: an idle/caught-up follower must keep reporting durable progress
    // (zero-credit) so the owner's in-sync freshness does not go stale.
    #[tokio::test]
    async fn applier_sends_keepalive_progress_when_idle() {
        let sink = Arc::new(MockSink {
            outcomes: Mutex::new(std::collections::VecDeque::new()),
            applied: Mutex::new(Vec::new()),
        });
        let (_batch_tx, batch_rx) = mpsc::channel(8);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        // Start cursor 5; no batches arrive -> keepalive should fire.
        let _task = tokio::spawn(run_follower_push_applier(
            sink,
            batch_rx,
            control_tx,
            5,
            fixed_tunables(20, 0, 16 * 1024 * 1024),
        ));

        let ctrl = tokio::time::timeout(Duration::from_secs(2), control_rx.recv())
            .await
            .expect("keepalive timed out")
            .unwrap();
        assert_eq!(
            ctrl,
            FollowerPushControl::Progress {
                durable_next: 5, written_next: 5,
                credit_add_bytes: 0,
            },
            "idle keepalive reports the current cursor with zero credit"
        );
    }

    // Regression: the keepalive must report the LATEST applied cursor, not the
    // start cursor (so a push that applied then went idle stays honest).
    #[tokio::test]
    async fn applier_keepalive_uses_latest_applied_cursor() {
        let sink = Arc::new(MockSink {
            outcomes: Mutex::new(
                [FollowerApplyOutcome::Applied {
                    durable_next: 100, written_next: 100,
                    bytes: 2 * REC_BYTES,
                }]
                .into(),
            ),
            applied: Mutex::new(Vec::new()),
        });
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let _task = tokio::spawn(run_follower_push_applier(
            sink,
            batch_rx,
            control_tx,
            0,
            fixed_tunables(20, 0, 16 * 1024 * 1024),
        ));

        batch_tx.send(batch_from(0, 2)).await.unwrap();
        // First: the per-batch progress (credit = bytes).
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), control_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            FollowerPushControl::Progress {
                durable_next: 100, written_next: 100,
                credit_add_bytes: 2 * REC_BYTES,
            }
        );
        // Then: idle keepalive reports the advanced cursor with zero credit.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), control_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            FollowerPushControl::Progress {
                durable_next: 100, written_next: 100,
                credit_add_bytes: 0,
            }
        );
    }
}

