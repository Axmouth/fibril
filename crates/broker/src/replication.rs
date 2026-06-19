//! Replication / clustering types and traits for the broker.
//!
//! First brick of the clustering-module separation: the replication data types,
//! peer/resolver traits, catch-up + stream outcome enums, and the stream-apply
//! trait, lifted out of broker.rs. Re-exported from `broker` so existing
//! `fibril_broker::broker::*` import paths keep resolving.

use std::sync::Arc;

use futures::future::BoxFuture;
use tokio_util::sync::CancellationToken;

use fibril_storage::{Offset, Partition};
use stroma_core::{Message, OwnerReplicationRead, StromaEvent};

use crate::broker::BrokerError;
use crate::coordination::PartitionAssignment;
use crate::queue_engine::{OwnerStateCheckpoint, ReplicatedQueueApplyOutcome};

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
