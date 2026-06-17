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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v1::{ReplicationMessageRecord, ReplicationStreamProgress};
    use std::sync::Mutex;
    use std::time::Duration;

    const REC_BYTES: u64 = 100;

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
