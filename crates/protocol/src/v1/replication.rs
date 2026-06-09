use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, bail};
use fibril_broker::{
    LogId, Offset,
    broker::{BrokerError, BrokerOwnerReplicationPeer, BrokerOwnerReplicationRecords},
    queue_engine::{
        Message, OwnerReplicationBatch, OwnerReplicationRead, OwnerStateCheckpoint, StromaEvent,
    },
};
use futures::{SinkExt, StreamExt};
use tokio::sync::Mutex;

use crate::v1::{
    ErrorMsg, Op, ReplicationApply, ReplicationApplyOk, ReplicationCheckpointExport,
    ReplicationCheckpointExportOk, ReplicationCheckpointRequired, ReplicationEventApplyBatch,
    ReplicationEventRead, ReplicationMessageApplyBatch, ReplicationMessageRead, ReplicationRead,
    ReplicationReadOk,
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

/// Protocol-backed owner replication peer.
///
/// This is the transport adapter for the broker's follower worker boundary. It
/// owns one already-handshaken protocol connection to an owner and serializes
/// requests on that connection.
pub struct ProtocolOwnerReplicationPeer {
    conn: Mutex<Conn>,
    next_request_id: AtomicU64,
}

impl ProtocolOwnerReplicationPeer {
    pub fn new(conn: Conn) -> Self {
        Self {
            conn: Mutex::new(conn),
            next_request_id: AtomicU64::new(20_000),
        }
    }

    fn next_request_id(&self) -> u64 {
        self.next_request_id.fetch_add(1, Ordering::Relaxed)
    }
}

impl BrokerOwnerReplicationPeer for ProtocolOwnerReplicationPeer {
    fn read_owner_replication_records<'a>(
        &'a self,
        topic: &'a str,
        partition: LogId,
        group: Option<&'a str>,
        message_from: Offset,
        event_from: Offset,
        max_messages: usize,
        max_events: usize,
    ) -> futures::future::BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        Box::pin(async move {
            let max_messages = u32::try_from(max_messages).map_err(|_| {
                BrokerError::InvalidArgument("max_messages exceeds protocol limit".into())
            })?;
            let max_events = u32::try_from(max_events).map_err(|_| {
                BrokerError::InvalidArgument("max_events exceeds protocol limit".into())
            })?;
            let request_id = self.next_request_id();
            let mut conn = self.conn.lock().await;
            conn.send(
                try_encode(
                    Op::ReplicationRead,
                    request_id,
                    &ReplicationRead {
                        topic: topic.to_string(),
                        group: group.map(str::to_string),
                        partition,
                        message_from,
                        event_from,
                        max_messages,
                        max_events,
                    },
                )
                .map_err(protocol_error)?,
            )
            .await
            .map_err(|err| BrokerError::Unknown(format!("replication read send failed: {err}")))?;

            let read: ReplicationReadOk =
                recv_response(&mut conn, request_id, Op::ReplicationReadOk)
                    .await
                    .map_err(anyhow_to_broker_error)?;
            to_broker_owner_replication_records(read)
        })
    }

    fn export_owner_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: LogId,
        group: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>> {
        Box::pin(async move {
            let request_id = self.next_request_id();
            let mut conn = self.conn.lock().await;
            conn.send(
                try_encode(
                    Op::ReplicationCheckpointExport,
                    request_id,
                    &ReplicationCheckpointExport {
                        topic: topic.to_string(),
                        group: group.map(str::to_string),
                        partition,
                    },
                )
                .map_err(protocol_error)?,
            )
            .await
            .map_err(|err| {
                BrokerError::Unknown(format!("replication checkpoint export send failed: {err}"))
            })?;

            let export: ReplicationCheckpointExportOk =
                recv_response(&mut conn, request_id, Op::ReplicationCheckpointExportOk)
                    .await
                    .map_err(anyhow_to_broker_error)?;
            Ok(OwnerStateCheckpoint {
                message_epoch: export.checkpoint.message_epoch,
                event_epoch: export.checkpoint.event_epoch,
                message_checkpoint_offset: export.checkpoint.message_checkpoint_offset,
                message_next_offset: export.checkpoint.message_next_offset,
                event_next_offset: export.checkpoint.event_next_offset,
                applied_event_offset: export.checkpoint.applied_event_offset,
                state_snapshot: export.checkpoint.state_snapshot,
            })
        })
    }
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

fn protocol_error(err: impl std::fmt::Display) -> BrokerError {
    BrokerError::Unknown(format!("protocol replication error: {err}"))
}

fn anyhow_to_broker_error(err: anyhow::Error) -> BrokerError {
    BrokerError::Unknown(err.to_string())
}

fn to_broker_owner_replication_records(
    read: ReplicationReadOk,
) -> Result<BrokerOwnerReplicationRecords, BrokerError> {
    Ok(BrokerOwnerReplicationRecords {
        messages: to_broker_message_read(read.messages)?,
        events: to_broker_event_read(read.events)?,
    })
}

fn to_broker_message_read(
    read: ReplicationMessageRead,
) -> Result<OwnerReplicationRead<Message>, BrokerError> {
    Ok(match read {
        ReplicationMessageRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => OwnerReplicationRead::Batch(OwnerReplicationBatch {
            epoch,
            requested_offset,
            next_offset,
            records: records
                .into_iter()
                .map(|record| {
                    (
                        record.offset,
                        Message {
                            flags: record.flags,
                            headers: record.headers,
                            payload: record.payload,
                        },
                    )
                })
                .collect(),
        }),
        ReplicationMessageRead::CheckpointRequired(required) => {
            OwnerReplicationRead::CheckpointRequired {
                epoch: required.epoch,
                requested_offset: required.requested_offset,
                head_offset: required.head_offset,
                next_offset: required.next_offset,
            }
        }
    })
}

fn to_broker_event_read(
    read: ReplicationEventRead,
) -> Result<OwnerReplicationRead<StromaEvent>, BrokerError> {
    Ok(match read {
        ReplicationEventRead::Batch {
            epoch,
            requested_offset,
            next_offset,
            records,
        } => {
            let records = records
                .into_iter()
                .map(|record| {
                    let event = StromaEvent::decode(&record.payload).map_err(|err| {
                        BrokerError::Unknown(format!(
                            "failed to decode replicated event record: {err}"
                        ))
                    })?;
                    Ok((record.offset, event))
                })
                .collect::<Result<Vec<_>, BrokerError>>()?;
            OwnerReplicationRead::Batch(OwnerReplicationBatch {
                epoch,
                requested_offset,
                next_offset,
                records,
            })
        }
        ReplicationEventRead::CheckpointRequired(required) => {
            OwnerReplicationRead::CheckpointRequired {
                epoch: required.epoch,
                requested_offset: required.requested_offset,
                head_offset: required.head_offset,
                next_offset: required.next_offset,
            }
        }
    })
}
