//! Owner side of credit-based streaming replication.
//!
//! On `ReplicationStreamStart` the connection spawns one [`run_owner_replication_stream`]
//! task per stream. The task holds a cursor and a byte-denominated send budget
//! (credit), reads offset-ordered batches from the owner log (long-polling so it
//! wakes on new publishes), and pushes them to the follower as `ReplicationStreamBatch`
//! frames until the credit is exhausted. The follower refills credit and reports
//! durable progress through [`OwnerStreamControl::Progress`], rewinds the cursor
//! through [`OwnerStreamControl::Reset`], and closes with [`OwnerStreamControl::Stop`].
//!
//! The batch payload reuses the pull `ReplicationReadOk` body, so this is a
//! transport change, not a storage or apply change.

use std::sync::Arc;

use async_trait::async_trait;
use fibril_broker::replication::StreamApplyTunablesFn;
use tokio::sync::mpsc;

use crate::v1::{
    Partition, ReplicationEventRead, ReplicationMessageRead, ReplicationReadOk,
    ReplicationStreamEnd, ReplicationStreamStart, frame::Frame, wire,
};

/// `ReplicationStreamEnd.code` values.
pub const STREAM_END_CLOSED: u16 = 0;
pub const STREAM_END_ERROR: u16 = 1;
pub const STREAM_END_CHECKPOINT_REQUIRED: u16 = 2;

/// Control messages from the connection's frame dispatch to a running owner
/// stream sender.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerStreamControl {
    /// Follower reported durable progress and refilled the send budget.
    Progress {
        durable_message_next: u64,
        durable_event_next: u64,
        credit_add_bytes: u64,
    },
    /// Follower asked to rewind the stream cursor (gap or post-checkpoint).
    Reset { message_from: u64, event_from: u64 },
    /// Follower closed the stream.
    Stop,
}

/// What the owner sender needs from the broker. A trait so the send loop is
/// unit-testable without a live engine.
#[async_trait]
pub trait OwnerStreamSource: Send + Sync + 'static {
    /// Read the next offset-ordered batch from the owner log. `max_wait_ms`
    /// long-polls so a caught-up stream wakes on the next publish.
    async fn read(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        message_from: u64,
        event_from: u64,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> Result<ReplicationReadOk, String>;

    /// Record the follower's durable progress (drives the publish-confirm gate
    /// and the replica-durable visibility watermark).
    fn record_progress(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        reporter: &str,
        durable_message_next: u64,
        durable_event_next: u64,
    );
}

#[derive(Debug, Clone, Copy)]
pub struct OwnerStreamConfig {
    pub max_messages: usize,
    pub max_events: usize,
    /// Per-batch byte cap, independent of credit (bounds one read's size).
    pub max_batch_bytes: usize,
    /// Long-poll budget for a caught-up read.
    pub long_poll_ms: u64,
}

impl Default for OwnerStreamConfig {
    fn default() -> Self {
        Self {
            max_messages: 2048,
            max_events: 2048,
            max_batch_bytes: 8 * 1024 * 1024,
            long_poll_ms: 1_000,
        }
    }
}

enum BatchOutcome {
    /// Deliverable records: their total bytes and the advanced cursor.
    Records {
        bytes: u64,
        message_next: u64,
        event_next: u64,
    },
    /// Nothing available right now (caught up / long-poll timeout).
    Empty,
    /// The owner cannot serve from here without a checkpoint (offset gap).
    CheckpointRequired,
}

fn message_side(read: &ReplicationMessageRead) -> Option<(u64, u64)> {
    match read {
        ReplicationMessageRead::Batch {
            next_offset,
            records,
            ..
        } => {
            let bytes = records
                .iter()
                .map(|r| (r.headers.len() + r.payload.len()) as u64)
                .sum();
            Some((*next_offset, bytes))
        }
        ReplicationMessageRead::CheckpointRequired(_) => None,
    }
}

fn event_side(read: &ReplicationEventRead) -> Option<(u64, u64)> {
    match read {
        ReplicationEventRead::Batch {
            next_offset,
            records,
            ..
        } => {
            let bytes = records.iter().map(|r| r.payload.len() as u64).sum();
            Some((*next_offset, bytes))
        }
        ReplicationEventRead::CheckpointRequired(_) => None,
    }
}

fn classify(batch: &ReplicationReadOk) -> BatchOutcome {
    let (Some((message_next, msg_bytes)), Some((event_next, evt_bytes))) =
        (message_side(&batch.messages), event_side(&batch.events))
    else {
        return BatchOutcome::CheckpointRequired;
    };
    let bytes = msg_bytes + evt_bytes;
    if bytes == 0 {
        BatchOutcome::Empty
    } else {
        BatchOutcome::Records {
            bytes,
            message_next,
            event_next,
        }
    }
}

fn batch_bytes(batch: &ReplicationReadOk) -> u64 {
    let msg = message_side(&batch.messages).map(|(_, b)| b).unwrap_or(0);
    let evt = event_side(&batch.events).map(|(_, b)| b).unwrap_or(0);
    msg + evt
}

/// Fold `next` into `acc` so the follower can apply (and fsync) several streamed
/// frames as one batch. Succeeds only when both the message and event sides
/// continue `acc` contiguously at the same epoch and neither side is a checkpoint
/// frame. On success `acc` covers both frames; on failure `next` is handed back
/// untouched so the caller applies `acc` and carries `next` to the next apply
/// (this is how an epoch change, an offset gap, or a checkpoint frame stops a
/// merge run without losing data).
fn try_fold_streamed(
    acc: &mut ReplicationReadOk,
    next: ReplicationReadOk,
) -> Result<(), ReplicationReadOk> {
    let messages_continue = matches!(
        (&acc.messages, &next.messages),
        (
            ReplicationMessageRead::Batch { epoch: ae, next_offset: an, .. },
            ReplicationMessageRead::Batch { epoch: be, requested_offset: br, .. },
        ) if ae == be && an == br
    );
    let events_continue = matches!(
        (&acc.events, &next.events),
        (
            ReplicationEventRead::Batch { epoch: ae, next_offset: an, .. },
            ReplicationEventRead::Batch { epoch: be, requested_offset: br, .. },
        ) if ae == be && an == br
    );
    if !(messages_continue && events_continue) {
        return Err(next);
    }

    if let (
        ReplicationMessageRead::Batch {
            next_offset: an,
            records: ar,
            ..
        },
        ReplicationMessageRead::Batch {
            next_offset: bn,
            records: br,
            ..
        },
    ) = (&mut acc.messages, next.messages)
    {
        ar.extend(br);
        *an = bn;
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
    ) = (&mut acc.events, next.events)
    {
        ar.extend(br);
        *an = bn;
    }
    Ok(())
}

/// Apply a control message. Returns `false` when the stream should stop.
fn apply_control<S: OwnerStreamSource>(
    control: OwnerStreamControl,
    source: &S,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
    reporter: Option<&str>,
    credit: &mut u64,
    message_from: &mut u64,
    event_from: &mut u64,
) -> bool {
    match control {
        OwnerStreamControl::Progress {
            durable_message_next,
            durable_event_next,
            credit_add_bytes,
        } => {
            *credit = credit.saturating_add(credit_add_bytes);
            if let Some(reporter) = reporter {
                source.record_progress(
                    topic,
                    partition,
                    group,
                    reporter,
                    durable_message_next,
                    durable_event_next,
                );
            }
            true
        }
        OwnerStreamControl::Reset {
            message_from: m,
            event_from: e,
        } => {
            *message_from = m;
            *event_from = e;
            true
        }
        OwnerStreamControl::Stop => false,
    }
}

async fn send_stream_end(frame_tx: &mpsc::Sender<Frame>, stream_id: u64, code: u16, message: &str) {
    if let Ok(frame) = wire::encode_replication_stream_end(
        stream_id,
        &ReplicationStreamEnd {
            code,
            message: message.to_string(),
        },
    ) {
        let _ = frame_tx.send(frame).await;
    }
}

// ---------------------------------------------------------------------------
// Follower side: the applier loop.
// ---------------------------------------------------------------------------

/// Control the follower needs to send back to the owner as it applies. The
/// transport encodes these to `ReplicationStreamProgress` / `ReplicationStreamReset`
/// frames on the stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FollowerStreamControl {
    /// Durable progress plus a credit refill (combined). Sent per applied batch
    /// so progress is prompt (confirm latency) and the budget stays topped up.
    Progress {
        durable_message_next: u64,
        durable_event_next: u64,
        credit_add_bytes: u64,
    },
    /// Rewind the owner stream cursor (the follower handled a checkpoint).
    Reset { message_from: u64, event_from: u64 },
}

/// Outcome of durably applying one streamed batch.
pub enum FollowerApplyOutcome {
    /// Applied durably; here is the new durable cursor and the bytes applied
    /// (the credit to return).
    Applied {
        durable_message_next: u64,
        durable_event_next: u64,
        bytes: u64,
    },
    /// The batch needs a checkpoint (offset gap / epoch fence). The stream stops
    /// so the worker can fall back to the proven pull+checkpoint path, then
    /// re-stream from the post-checkpoint cursor.
    CheckpointRequired,
    /// Fatal apply error; the applier stops.
    Error(String),
}

/// Why a follower stream applier loop ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplierExit {
    /// The reader closed the batch channel (stream torn down upstream).
    ReaderClosed,
    /// An apply needs a checkpoint; the caller should fall back to pull.
    CheckpointRequired,
    /// A fatal apply error.
    Error(String),
}

/// What the follower applier does with a received batch. A trait so the loop is
/// unit-testable without a live engine.
#[async_trait]
pub trait FollowerStreamSink: Send + Sync + 'static {
    async fn apply(&self, batch: ReplicationReadOk) -> FollowerApplyOutcome;
}

/// Drain in-order batches from the reader and apply them durably, sending a
/// combined progress+credit refill after each one (and a reset when the sink
/// handled a checkpoint). Ends when the reader channel closes (stream torn down)
/// or an apply error occurs. The reader feeds `batch_rx`; the transport encodes
/// `control_tx` items to frames.
///
/// `message_from`/`event_from` is the starting durable cursor. When no batch
/// arrives within `keepalive` (the follower is caught up), a zero-credit progress
/// frame is sent so the owner's in-sync freshness check stays satisfied for an
/// idle-but-current follower (the pull path gets this for free via caught-up
/// polling). `keepalive` of zero disables the keepalive.
pub async fn run_follower_stream_applier<S: FollowerStreamSink>(
    sink: Arc<S>,
    mut batch_rx: mpsc::Receiver<ReplicationReadOk>,
    control_tx: mpsc::Sender<FollowerStreamControl>,
    message_from: u64,
    event_from: u64,
    tunables: StreamApplyTunablesFn,
) -> ApplierExit {
    // Microbatch the apply: streamed frames arrive small (the owner reads whatever
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

    let mut message_next = message_from;
    let mut event_next = event_from;
    let mut carry: Option<ReplicationReadOk> = None;
    loop {
        // Read the tunables LIVE each iteration (not captured once at start), so a
        // runtime-settings change takes effect on the next apply with no stream
        // restart.
        let knobs = tunables();
        let keepalive = std::time::Duration::from_millis(knobs.keepalive_ms);
        let apply_linger = std::time::Duration::from_micros(knobs.apply_linger_us);
        let max_merge_bytes = knobs.max_merge_bytes;

        // First frame for this apply: a carried-over unmergeable frame, otherwise
        // a fresh receive. Ok(Some) = batch, Ok(None) = reader closed, Err(()) =
        // idle keepalive tick.
        let first = match carry.take() {
            Some(batch) => batch,
            None => {
                let received: Result<Option<ReplicationReadOk>, ()> = if keepalive.is_zero() {
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
                        // Caught-up keepalive: refresh durable progress, no credit.
                        if control_tx
                            .send(FollowerStreamControl::Progress {
                                durable_message_next: message_next,
                                durable_event_next: event_next,
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

        // Fold as many contiguous frames as are ready (plus a one-shot linger).
        let mut merged = first;
        let mut lingered = false;
        while batch_bytes(&merged) < max_merge_bytes {
            let next = match batch_rx.try_recv() {
                Ok(batch) => Some(batch),
                Err(mpsc::error::TryRecvError::Empty) => {
                    if lingered || apply_linger.is_zero() {
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
            match next {
                Some(batch) => match try_fold_streamed(&mut merged, batch) {
                    Ok(()) => continue,
                    Err(unmerged) => {
                        carry = Some(unmerged);
                        break;
                    }
                },
                None => break,
            }
        }

        match sink.apply(merged).await {
            FollowerApplyOutcome::Applied {
                durable_message_next,
                durable_event_next,
                bytes,
            } => {
                message_next = durable_message_next;
                event_next = durable_event_next;
                if control_tx
                    .send(FollowerStreamControl::Progress {
                        durable_message_next,
                        durable_event_next,
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
    use crate::v1::ReplicationMessageRecord;
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
        })
    }

    /// A mock log of `total` message records (REC_BYTES each, offsets 0..total)
    /// and no events. Each read returns the records from `message_from` that fit
    /// in `max_bytes`; a caught-up read sleeps for the long-poll then returns
    /// empty (so the loop parks but the select still reacts to control).
    struct MockSource {
        total: u64,
        progress: Mutex<Vec<(u64, u64)>>,
    }

    #[async_trait]
    impl OwnerStreamSource for MockSource {
        async fn read(
            &self,
            _topic: &str,
            _partition: Partition,
            _group: Option<&str>,
            message_from: u64,
            event_from: u64,
            _max_messages: usize,
            _max_events: usize,
            max_bytes: usize,
            max_wait_ms: u64,
        ) -> Result<ReplicationReadOk, String> {
            let mut records = Vec::new();
            let mut off = message_from;
            let mut bytes = 0u64;
            while off < self.total && bytes + REC_BYTES <= max_bytes as u64 {
                records.push(ReplicationMessageRecord {
                    offset: off,
                    flags: 0,
                    headers: Vec::new(),
                    payload: vec![0u8; REC_BYTES as usize],
                });
                bytes += REC_BYTES;
                off += 1;
            }
            if records.is_empty() {
                tokio::time::sleep(Duration::from_millis(max_wait_ms.min(50))).await;
            }
            Ok(ReplicationReadOk {
                messages: ReplicationMessageRead::Batch {
                    epoch: 1,
                    requested_offset: message_from,
                    next_offset: off,
                    records,
                },
                events: ReplicationEventRead::Batch {
                    epoch: 1,
                    requested_offset: event_from,
                    next_offset: event_from,
                    records: Vec::new(),
                },
            })
        }

        fn record_progress(
            &self,
            _topic: &str,
            _partition: Partition,
            _group: Option<&str>,
            _reporter: &str,
            durable_message_next: u64,
            durable_event_next: u64,
        ) {
            self.progress
                .lock()
                .unwrap()
                .push((durable_message_next, durable_event_next));
        }
    }

    fn start(credit_bytes: u64) -> ReplicationStreamStart {
        ReplicationStreamStart {
            topic: "orders".into(),
            group: None,
            partition: Partition::new(0),
            message_from: 0,
            event_from: 0,
            credit_bytes,
            reporter_node_id: Some("broker-2".into()),
        }
    }

    fn cfg() -> OwnerStreamConfig {
        OwnerStreamConfig {
            max_messages: 1,
            max_events: 1,
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
                .expect("stream sender closed early");
            let batch = wire::decode_replication_stream_batch(&frame).unwrap();
            if let ReplicationMessageRead::Batch { records, .. } = batch.messages {
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
        });
        let (frame_tx, mut frame_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(8);

        // Credit for exactly 2 records (max_batch_bytes = 1 record, so 2 batches).
        let task = tokio::spawn(run_owner_replication_stream(
            source.clone(),
            frame_tx,
            7,
            start(2 * REC_BYTES),
            control_rx,
            cfg(),
        ));

        // Two batches flow, then the stream parks (credit exhausted).
        assert_eq!(recv_offsets(&mut frame_rx, 2).await, vec![0, 1]);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), frame_rx.recv())
                .await
                .is_err(),
            "credit gate must hold the 3rd batch"
        );

        // Refill + progress: 3 more batches flow.
        control_tx
            .send(OwnerStreamControl::Progress {
                durable_message_next: 2,
                durable_event_next: 0,
                credit_add_bytes: 3 * REC_BYTES,
            })
            .await
            .unwrap();
        assert_eq!(recv_offsets(&mut frame_rx, 3).await, vec![2, 3, 4]);
        assert_eq!(source.progress.lock().unwrap().as_slice(), &[(2, 0)]);

        control_tx.send(OwnerStreamControl::Stop).await.unwrap();
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
        });
        let (frame_tx, mut frame_rx) = mpsc::channel(64);
        let (control_tx, control_rx) = mpsc::channel(8);

        // Credit for exactly 3 records, so the sender sends 0,1,2 then parks.
        // That makes the reset deterministic (no batches race ahead of it).
        let task = tokio::spawn(run_owner_replication_stream(
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
            .send(OwnerStreamControl::Reset {
                message_from: 0,
                event_from: 0,
            })
            .await
            .unwrap();
        control_tx
            .send(OwnerStreamControl::Progress {
                durable_message_next: 0,
                durable_event_next: 0,
                credit_add_bytes: REC_BYTES,
            })
            .await
            .unwrap();
        let after = recv_offsets(&mut frame_rx, 1).await;
        assert_eq!(after, vec![0], "reset must rewind the cursor to 0");

        control_tx.send(OwnerStreamControl::Stop).await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
    }

    // ---- follower applier ----

    struct MockSink {
        // Scripted outcomes, one per applied batch (front = next).
        outcomes: Mutex<std::collections::VecDeque<FollowerApplyOutcome>>,
        applied: Mutex<Vec<u64>>, // first message offset of each applied batch
    }

    #[async_trait]
    impl FollowerStreamSink for MockSink {
        async fn apply(&self, batch: ReplicationReadOk) -> FollowerApplyOutcome {
            if let ReplicationMessageRead::Batch { records, .. } = &batch.messages
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

    fn batch_from(first_offset: u64, count: u64) -> ReplicationReadOk {
        let records = (0..count)
            .map(|i| ReplicationMessageRecord {
                offset: first_offset + i,
                flags: 0,
                headers: Vec::new(),
                payload: vec![0u8; REC_BYTES as usize],
            })
            .collect();
        ReplicationReadOk {
            messages: ReplicationMessageRead::Batch {
                epoch: 1,
                requested_offset: first_offset,
                next_offset: first_offset + count,
                records,
            },
            events: ReplicationEventRead::Batch {
                epoch: 1,
                requested_offset: 0,
                next_offset: 0,
                records: Vec::new(),
            },
        }
    }

    // Contiguous same-epoch frames that are already queued fold into ONE apply
    // (one follower fsync) and report a single combined progress + credit refill.
    #[tokio::test]
    async fn applier_coalesces_contiguous_frames_into_one_apply() {
        let sink = Arc::new(MockSink {
            outcomes: Mutex::new(
                [FollowerApplyOutcome::Applied {
                    durable_message_next: 5,
                    durable_event_next: 0,
                    bytes: 5 * REC_BYTES,
                }]
                .into(),
            ),
            applied: Mutex::new(Vec::new()),
        });
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_follower_stream_applier(
            sink.clone(),
            batch_rx,
            control_tx,
            0,
            0,
            fixed_tunables(0, 0, 16 * 1024 * 1024),
        ));

        // Both queued before the applier drains -> they fold (0..2 then 2..5).
        batch_tx.send(batch_from(0, 2)).await.unwrap();
        batch_tx.send(batch_from(2, 3)).await.unwrap();

        assert_eq!(
            control_rx.recv().await.unwrap(),
            FollowerStreamControl::Progress {
                durable_message_next: 5,
                durable_event_next: 0,
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
    // mid-stream. (The source side - a config change reaching the reader - is
    // covered by broker stream_apply_tunables_reads_live_config.)
    #[tokio::test]
    async fn applier_reads_tunables_live_each_iteration() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let sink = Arc::new(MockSink {
            outcomes: Mutex::new(
                [
                    FollowerApplyOutcome::Applied {
                        durable_message_next: 2,
                        durable_event_next: 0,
                        bytes: 2 * REC_BYTES,
                    },
                    FollowerApplyOutcome::Applied {
                        durable_message_next: 5,
                        durable_event_next: 0,
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
            }
        });

        let task = tokio::spawn(run_follower_stream_applier(
            sink.clone(),
            batch_rx,
            control_tx,
            0,
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
                        durable_message_next: 2,
                        durable_event_next: 0,
                        bytes: 2 * REC_BYTES,
                    },
                    FollowerApplyOutcome::Applied {
                        durable_message_next: 11,
                        durable_event_next: 0,
                        bytes: REC_BYTES,
                    },
                ]
                .into(),
            ),
            applied: Mutex::new(Vec::new()),
        });
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let task = tokio::spawn(run_follower_stream_applier(
            sink.clone(),
            batch_rx,
            control_tx,
            0,
            0,
            fixed_tunables(0, 0, 16 * 1024 * 1024),
        ));

        batch_tx.send(batch_from(0, 2)).await.unwrap(); // 0..2
        batch_tx.send(batch_from(10, 1)).await.unwrap(); // gap: 10..11

        assert_eq!(
            control_rx.recv().await.unwrap(),
            FollowerStreamControl::Progress {
                durable_message_next: 2,
                durable_event_next: 0,
                credit_add_bytes: 2 * REC_BYTES,
            }
        );
        assert_eq!(
            control_rx.recv().await.unwrap(),
            FollowerStreamControl::Progress {
                durable_message_next: 11,
                durable_event_next: 0,
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
        let task = tokio::spawn(run_follower_stream_applier(
            sink,
            batch_rx,
            control_tx,
            0,
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
        // Start cursor (5, 2); no batches arrive -> keepalive should fire.
        let _task = tokio::spawn(run_follower_stream_applier(
            sink,
            batch_rx,
            control_tx,
            5,
            2,
            fixed_tunables(20, 0, 16 * 1024 * 1024),
        ));

        let ctrl = tokio::time::timeout(Duration::from_secs(2), control_rx.recv())
            .await
            .expect("keepalive timed out")
            .unwrap();
        assert_eq!(
            ctrl,
            FollowerStreamControl::Progress {
                durable_message_next: 5,
                durable_event_next: 2,
                credit_add_bytes: 0,
            },
            "idle keepalive reports the current cursor with zero credit"
        );
    }

    // Regression: the keepalive must report the LATEST applied cursor, not the
    // start cursor (so a stream that applied then went idle stays honest).
    #[tokio::test]
    async fn applier_keepalive_uses_latest_applied_cursor() {
        let sink = Arc::new(MockSink {
            outcomes: Mutex::new(
                [FollowerApplyOutcome::Applied {
                    durable_message_next: 100,
                    durable_event_next: 40,
                    bytes: 2 * REC_BYTES,
                }]
                .into(),
            ),
            applied: Mutex::new(Vec::new()),
        });
        let (batch_tx, batch_rx) = mpsc::channel(8);
        let (control_tx, mut control_rx) = mpsc::channel(8);
        let _task = tokio::spawn(run_follower_stream_applier(
            sink,
            batch_rx,
            control_tx,
            0,
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
            FollowerStreamControl::Progress {
                durable_message_next: 100,
                durable_event_next: 40,
                credit_add_bytes: 2 * REC_BYTES,
            }
        );
        // Then: idle keepalive reports the advanced cursor with zero credit.
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), control_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            FollowerStreamControl::Progress {
                durable_message_next: 100,
                durable_event_next: 40,
                credit_add_bytes: 0,
            }
        );
    }
}

/// Drive one owner replication stream until the follower stops it, the
/// connection drops, or a fatal read error. The frame `request_id` of every
/// pushed batch is `stream_id`.
pub async fn run_owner_replication_stream<S: OwnerStreamSource>(
    source: Arc<S>,
    frame_tx: mpsc::Sender<Frame>,
    stream_id: u64,
    start: ReplicationStreamStart,
    mut control_rx: mpsc::Receiver<OwnerStreamControl>,
    cfg: OwnerStreamConfig,
) {
    let topic = start.topic;
    let group = start.group;
    let partition = start.partition;
    let reporter = start.reporter_node_id;
    let mut message_from = start.message_from;
    let mut event_from = start.event_from;
    let mut credit = start.credit_bytes;

    loop {
        // Out of budget: park until the follower refills (or stops / drops).
        if credit == 0 {
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
                        &mut message_from,
                        &mut event_from,
                    ) {
                        break;
                    }
                    continue;
                }
                None => break,
            }
        }

        let max_bytes = credit.min(cfg.max_batch_bytes as u64) as usize;
        let read = source.read(
            &topic,
            partition,
            group.as_deref(),
            message_from,
            event_from,
            cfg.max_messages,
            cfg.max_events,
            max_bytes,
            cfg.long_poll_ms,
        );

        tokio::select! {
            // React to control even while the long-poll read is parked. The read
            // future is a pure scan (no lease), so dropping it is safe.
            control = control_rx.recv() => {
                match control {
                    Some(control) => {
                        if !apply_control(
                            control,
                            source.as_ref(),
                            &topic,
                            partition,
                            group.as_deref(),
                            reporter.as_deref(),
                            &mut credit,
                            &mut message_from,
                            &mut event_from,
                        ) {
                            break;
                        }
                    }
                    None => break,
                }
            }
            result = read => {
                match result {
                    Ok(batch) => match classify(&batch) {
                        BatchOutcome::Empty => continue,
                        BatchOutcome::CheckpointRequired => {
                            send_stream_end(
                                &frame_tx,
                                stream_id,
                                STREAM_END_CHECKPOINT_REQUIRED,
                                "checkpoint required",
                            )
                            .await;
                            break;
                        }
                        BatchOutcome::Records { bytes, message_next, event_next } => {
                            let frame = match wire::encode_replication_stream_batch(stream_id, &batch) {
                                Ok(frame) => frame,
                                Err(err) => {
                                    send_stream_end(&frame_tx, stream_id, STREAM_END_ERROR, &err.to_string()).await;
                                    break;
                                }
                            };
                            if frame_tx.send(frame).await.is_err() {
                                break;
                            }
                            message_from = message_next;
                            event_from = event_next;
                            credit = credit.saturating_sub(bytes);
                        }
                    },
                    Err(err) => {
                        send_stream_end(&frame_tx, stream_id, STREAM_END_ERROR, &err).await;
                        break;
                    }
                }
            }
        }
    }
}
