use anyhow::{Context, bail};
use futures::{SinkExt, StreamExt};

use crate::v1::{
    ErrorMsg, Op, ReplicationApply, ReplicationApplyOk, ReplicationCheckpointRequired,
    ReplicationEventApplyBatch, ReplicationEventRead, ReplicationMessageApplyBatch,
    ReplicationMessageRead, ReplicationRead, ReplicationReadOk,
    helper::{Conn, try_decode, try_encode},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolReplicationCatchUpOptions {
    pub message_from: u64,
    pub event_from: u64,
    pub max_messages_per_read: u32,
    pub max_events_per_read: u32,
    pub max_iterations: usize,
    pub request_id_start: u64,
}

impl Default for ProtocolReplicationCatchUpOptions {
    fn default() -> Self {
        Self {
            message_from: 0,
            event_from: 0,
            max_messages_per_read: 256,
            max_events_per_read: 256,
            max_iterations: 1024,
            request_id_start: 10_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProtocolReplicationCatchUpProgress {
    pub iterations: usize,
    pub applied_message_records: usize,
    pub applied_event_records: usize,
    pub message_next_offset: u64,
    pub event_next_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolReplicationCatchUp {
    CaughtUp(ProtocolReplicationCatchUpProgress),
    CheckpointRequired {
        progress: ProtocolReplicationCatchUpProgress,
        messages: Option<ReplicationCheckpointRequired>,
        events: Option<ReplicationCheckpointRequired>,
    },
    IterationLimit {
        progress: ProtocolReplicationCatchUpProgress,
    },
}

/// Pull replicated log records from an owner connection and apply them to a
/// follower connection until both streams return empty or a limit is reached.
///
/// This is intentionally only the manual catch-up core. Background scheduling,
/// ownership discovery, retries, and checkpoint installation belong above it.
pub async fn catch_up_replication_over_protocol(
    owner: &mut Conn,
    follower: &mut Conn,
    topic: &str,
    partition: u32,
    group: Option<&str>,
    options: ProtocolReplicationCatchUpOptions,
) -> anyhow::Result<ProtocolReplicationCatchUp> {
    if options.max_messages_per_read == 0
        || options.max_events_per_read == 0
        || options.max_iterations == 0
    {
        bail!("replication catch-up limits must be greater than zero");
    }

    let mut request_id = options.request_id_start;
    let mut progress = ProtocolReplicationCatchUpProgress {
        message_next_offset: options.message_from,
        event_next_offset: options.event_from,
        ..Default::default()
    };

    for _ in 0..options.max_iterations {
        let read_request_id = request_id;
        request_id += 1;
        owner
            .send(try_encode(
                Op::ReplicationRead,
                read_request_id,
                &ReplicationRead {
                    topic: topic.to_string(),
                    group: group.map(str::to_string),
                    partition,
                    message_from: progress.message_next_offset,
                    event_from: progress.event_next_offset,
                    max_messages: options.max_messages_per_read,
                    max_events: options.max_events_per_read,
                },
            )?)
            .await
            .context("failed to send replication read request")?;

        let read: ReplicationReadOk =
            recv_response(owner, read_request_id, Op::ReplicationReadOk).await?;

        if let Some(required) = message_checkpoint_required(&read.messages) {
            return Ok(ProtocolReplicationCatchUp::CheckpointRequired {
                progress,
                messages: Some(required),
                events: event_checkpoint_required(&read.events),
            });
        }
        if let Some(required) = event_checkpoint_required(&read.events) {
            return Ok(ProtocolReplicationCatchUp::CheckpointRequired {
                progress,
                messages: None,
                events: Some(required),
            });
        }

        let (messages, message_record_count, message_next_offset) =
            message_apply_batch(read.messages)?;
        let (events, event_record_count, event_next_offset) = event_apply_batch(read.events)?;

        if message_record_count == 0 && event_record_count == 0 {
            return Ok(ProtocolReplicationCatchUp::CaughtUp(progress));
        }

        let apply_request_id = request_id;
        request_id += 1;
        follower
            .send(try_encode(
                Op::ReplicationApply,
                apply_request_id,
                &ReplicationApply {
                    topic: topic.to_string(),
                    group: group.map(str::to_string),
                    partition,
                    messages,
                    events,
                },
            )?)
            .await
            .context("failed to send replication apply request")?;

        let apply: ReplicationApplyOk =
            recv_response(follower, apply_request_id, Op::ReplicationApplyOk).await?;
        if message_record_count > 0 && !apply.messages_applied {
            bail!("replication apply response reported unapplied message records");
        }
        if event_record_count > 0 && !apply.events_applied {
            bail!("replication apply response reported unapplied event records");
        }

        progress.iterations += 1;
        progress.applied_message_records += message_record_count;
        progress.applied_event_records += event_record_count;
        progress.message_next_offset = message_next_offset;
        progress.event_next_offset = event_next_offset;
    }

    Ok(ProtocolReplicationCatchUp::IterationLimit { progress })
}

async fn recv_response<T>(conn: &mut Conn, request_id: u64, expected: Op) -> anyhow::Result<T>
where
    T: for<'de> serde::Deserialize<'de>,
{
    loop {
        let frame = conn
            .next()
            .await
            .ok_or_else(|| anyhow::anyhow!("connection closed while waiting for response"))?
            .context("failed to read response frame")?;

        if frame.opcode == Op::Ping as u16 {
            conn.send(try_encode(Op::Pong, frame.request_id, &())?)
                .await
                .context("failed to respond to heartbeat ping")?;
            continue;
        }

        if frame.request_id != request_id {
            bail!(
                "unexpected response request id {}; expected {}",
                frame.request_id,
                request_id
            );
        }

        if frame.opcode == Op::Error as u16 {
            let error: ErrorMsg = try_decode(&frame)?;
            bail!(
                "replication request failed: {} {}",
                error.code,
                error.message
            );
        }

        if frame.opcode != expected as u16 {
            bail!(
                "unexpected response opcode {}; expected {}",
                frame.opcode,
                expected as u16
            );
        }

        return try_decode(&frame).map_err(anyhow::Error::from);
    }
}

fn message_checkpoint_required(
    read: &ReplicationMessageRead,
) -> Option<ReplicationCheckpointRequired> {
    match read {
        ReplicationMessageRead::CheckpointRequired(required) => Some(required.clone()),
        ReplicationMessageRead::Batch { .. } => None,
    }
}

fn event_checkpoint_required(read: &ReplicationEventRead) -> Option<ReplicationCheckpointRequired> {
    match read {
        ReplicationEventRead::CheckpointRequired(required) => Some(required.clone()),
        ReplicationEventRead::Batch { .. } => None,
    }
}

fn message_apply_batch(
    read: ReplicationMessageRead,
) -> anyhow::Result<(Option<ReplicationMessageApplyBatch>, usize, u64)> {
    match read {
        ReplicationMessageRead::Batch {
            epoch,
            next_offset,
            records,
            ..
        } => {
            let count = records.len();
            Ok((
                (count > 0).then_some(ReplicationMessageApplyBatch { epoch, records }),
                count,
                next_offset,
            ))
        }
        ReplicationMessageRead::CheckpointRequired(_) => {
            bail!("message checkpoint requirement was not handled before apply conversion")
        }
    }
}

fn event_apply_batch(
    read: ReplicationEventRead,
) -> anyhow::Result<(Option<ReplicationEventApplyBatch>, usize, u64)> {
    match read {
        ReplicationEventRead::Batch {
            epoch,
            next_offset,
            records,
            ..
        } => {
            let count = records.len();
            Ok((
                (count > 0).then_some(ReplicationEventApplyBatch { epoch, records }),
                count,
                next_offset,
            ))
        }
        ReplicationEventRead::CheckpointRequired(_) => {
            bail!("event checkpoint requirement was not handled before apply conversion")
        }
    }
}
