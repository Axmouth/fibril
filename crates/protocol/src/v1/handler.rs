use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Instant,
};

use crate::v1::{
    AdvertisedAddress,
    frame::{Frame, ProtoCodec},
    helper::{ProtocolError, error_frame, try_decode, try_encode},
    wire::{self, WireError},
    *,
};
use arc_swap::ArcSwap;
use fibril_broker::{Group, Partition, Topic};
use fibril_broker::{
    broker::{
        Broker, BrokerError, BrokerFollowerReplicationApply, BrokerOwnerReplicationRecords,
        ConsumerConfig, ConsumerHandle, ConsumerLease, ExclusiveAssignmentUpdate, PublisherHandle,
        ReplicationResourceKind, SettleRequest, SettleType,
    },
    queue_engine::{
        DLQDiscardPolicyWire, DeclareMeta, FollowerStateCheckpointInstall, GlobalDLQ, Message,
        MessageContentType, MessageHeaders, OwnerReplicationBatch, OwnerReplicationRead,
        OwnerStateCheckpoint, QueueEngine, RetentionConfig, StreamStore, StromaEngine, StromaError,
        StromaEvent,
    },
    stream::{
        StreamChannel, StreamDurability as BrokerStreamDurability, StreamFilter, StreamRecord,
        SubscribeStart,
    },
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_util::net::{TcpListener, TcpStream};
use fibril_util::sniff::{
    PrefixedStream, looks_like_plaintext_frame, looks_like_tls_client_hello, sniff_first_bytes,
};
use fibril_util::{AuthHandler, unix_millis};
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{Mutex, mpsc, oneshot, watch};
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::Framed;
use uuid::Uuid;

type SubKey = (Topic, Partition, Option<Group>); // (topic, partition, group)
type StreamSubKey = (Topic, Partition); // streams have no group
type FrameSink = mpsc::Sender<Frame>;

/// Buffer between a stream subscription's backfill+live driver and the per-client
/// delivery task that turns records into `Deliver` frames.
const STREAM_DELIVERY_CHANNEL_CAPACITY: usize = 1024;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolServerError {
    #[error("failed to bind protocol listener at {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to accept protocol connection: {0}")]
    Accept(#[source] std::io::Error),
    #[error("failed to configure TCP_NODELAY for {peer}: {source}")]
    ConfigureSocket {
        peer: SocketAddr,
        #[source]
        source: std::io::Error,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ProtocolConnectionError {
    #[error("connection closed before HELLO")]
    ClosedBeforeHello,
    #[error("connection IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol frame error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("protocol wire format error: {0}")]
    Wire(#[from] WireError),
    #[error("connection frame channel closed")]
    FrameChannelClosed,
    #[error("protocol compliance marker mismatch")]
    ComplianceMarkerMismatch,
    #[error("connection task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error("TLS handshake failed: {0}")]
    TlsHandshake(#[source] std::io::Error),
}

impl From<mpsc::error::SendError<Frame>> for ProtocolConnectionError {
    fn from(_: mpsc::error::SendError<Frame>) -> Self {
        Self::FrameChannelClosed
    }
}

/// Server-side source of client-facing topology, injected into the protocol
/// handler so it can answer `Op::Topology` and emit `Op::Redirect` on not-owner.
/// Implemented by a coordination adapter in the binary; `None` (standalone)
/// means there is no routing info and clients use their direct connection.
pub trait ClientTopologySource: Send + Sync {
    /// Full client-facing topology snapshot for `Op::Topology`.
    fn topology(&self) -> TopologyOk;

    /// Current coordination generation, for cheap change detection on the push
    /// path. The default builds the full snapshot; impls with a cheaper handle to
    /// the generation should override.
    fn generation(&self) -> u64 {
        self.topology().generation
    }
    /// Current owner endpoint and partitioning version for one queue partition,
    /// if known. Used to build an `Op::Redirect`.
    fn owner_endpoint(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Option<(String, u64)>;

    /// Current owner endpoint and partitioning version for one stream partition,
    /// if known. Used to redirect a stream publish/subscribe to its owner. The
    /// default returns `None` (no stream placement view).
    fn stream_owner_endpoint(&self, _topic: &str, _partition: Partition) -> Option<(String, u64)> {
        None
    }
}

/// Tracks, per live client connection, the highest topology generation that
/// client has acked (via `Op::TopologyUpdateAck`). The broker reports the
/// cluster-wide minimum as a heartbeat label so the repartition controller can
/// fence a cutover until clients have adopted the new routing.
///
/// Only connections that have acked at least once count: a silent or pre-push
/// client never appears here, so it never drags the minimum down (the publish
/// version-fence plus an adoption timeout cover those). A connection's entry is
/// removed when it closes. Acks are monotonic per connection, and arrive at most
/// once per generation change per connection, so a `Mutex` on this cold path is
/// fine (it is never touched on the per-message hot path).
#[derive(Debug, Default)]
pub struct TopologyAdoptionTracker {
    acked: std::sync::Mutex<std::collections::HashMap<Uuid, u64>>,
}

impl TopologyAdoptionTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that `conn` has applied topology up to `generation`. Monotonic per
    /// connection: a stale ack never lowers a connection's recorded generation.
    pub fn record(&self, conn: Uuid, generation: u64) {
        let mut acked = self.acked.lock().unwrap_or_else(|e| e.into_inner());
        let entry = acked.entry(conn).or_insert(0);
        if generation > *entry {
            *entry = generation;
        }
    }

    /// Drop a connection's adoption state when it closes, so a gone client never
    /// holds the cluster-wide minimum down.
    pub fn remove(&self, conn: &Uuid) {
        self.acked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(conn);
    }

    /// The lowest generation acked across connections that have acked at least
    /// once, or `None` when no connection has acked yet. `None` means "no
    /// adoption signal" - the controller treats it as not-yet-adopted and leans
    /// on the timeout.
    pub fn min_acked_generation(&self) -> Option<u64> {
        self.acked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .copied()
            .min()
    }
}

/// Server-side writer for declaration coordination: records a resource's
/// partitioning (count + version) in the replicated store and catalogues its
/// partitions for placement, returning the EFFECTIVE partition count (which may
/// differ from the request if the resource was already declared). `None`
/// (standalone) means declare is local-only. Queues and streams live in
/// separate partitioning namespaces, so each has its own method.
pub trait DeclareCoordinator: Send + Sync {
    fn declare_partitioning<'a>(
        &'a self,
        topic: &'a str,
        group: Option<&'a str>,
        partition_count: u32,
    ) -> futures::future::BoxFuture<'a, Result<u32, String>>;

    /// Record a stream's full config (partitioning + durability tier + retention)
    /// and catalogue its partitions (streams have no group). The config rides
    /// coordination so an owner that did not declare the stream can still open its
    /// partitions correctly. Separate from `declare_partitioning` so a coordinator
    /// must handle stream placement explicitly rather than fall back to queue rules.
    fn declare_stream<'a>(
        &'a self,
        topic: &'a str,
        partition_count: u32,
        durability: u8,
        retention: StreamRetention,
        replication_factor: Option<u32>,
    ) -> futures::future::BoxFuture<'a, Result<u32, String>>;
}

/// Identifies an exclusive cohort this connection participates in:
/// `(topic, group, consumer_group)`.
type CohortKey = (String, Option<String>, String);

struct ConnState {
    authenticated: bool,
    subs: HashMap<SubKey, SubState>,
    /// Plexus stream subscriptions on this connection, keyed by (topic, partition).
    stream_subs: HashMap<StreamSubKey, StreamSubState>,
    /// One assignment-change forwarder task per exclusive cohort this connection
    /// is a member of (pushes `AssignmentChanged` frames). Aborted on cleanup.
    exclusive_forwarders: HashMap<CohortKey, tokio::task::JoinHandle<()>>,
}

/// State for one Plexus stream subscription: the fan-out registration plus the
/// tasks that backfill and deliver records. Acks settle the durable cursor.
struct StreamSubState {
    /// The durable cursor name, when the subscription is durable (acks advance it).
    /// `None` for an ephemeral subscription (acks are no-ops).
    durable_name: Option<String>,
    channel: Arc<StreamChannel>,
    auto_ack: bool,
    /// The recovering backfill+live driver task and the delivery task; both
    /// aborted on cleanup (the driver detaches itself from the fan-out).
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

fn has_reserved_headers(headers: &HashMap<String, String>) -> bool {
    // Server-owned = reserved minus the `fibril.client.*` client carve-out.
    headers.keys().any(|key| is_server_owned_header_key(key))
}

fn to_storage_content_type(content_type: Option<ContentType>) -> Option<MessageContentType> {
    content_type.map(|content_type| match content_type {
        ContentType::MsgPack => MessageContentType::MsgPack,
        ContentType::Json => MessageContentType::Json,
        ContentType::Text => MessageContentType::Text,
        ContentType::Custom(value) => MessageContentType::Custom(value.into_boxed_str()),
    })
}

fn to_protocol_content_type(content_type: Option<MessageContentType>) -> Option<ContentType> {
    content_type.map(|content_type| match content_type {
        MessageContentType::MsgPack => ContentType::MsgPack,
        MessageContentType::Json => ContentType::Json,
        MessageContentType::Text => ContentType::Text,
        MessageContentType::Custom(value) => ContentType::Custom(value.into_string()),
    })
}

fn normalize_content_type(
    explicit: Option<ContentType>,
    headers: &mut HashMap<String, String>,
) -> Option<ContentType> {
    let header_key = headers
        .keys()
        .find(|key| key.eq_ignore_ascii_case("content-type"))
        .cloned();
    let header = header_key.and_then(|key| headers.remove(&key));
    explicit.or_else(|| header.map(ContentType::from_header))
}

async fn to_declare_meta(declare: &DeclareQueue) -> Result<DeclareMeta, String> {
    let dlq_policy = match &declare.dlq_policy {
        None => None,
        Some(QueueDlqPolicy::Discard) => Some(DLQDiscardPolicyWire::Discard),
        Some(QueueDlqPolicy::Global) => Some(DLQDiscardPolicyWire::GlobalDQL),
        Some(QueueDlqPolicy::Custom { topic, group }) => {
            let target = GlobalDLQ::new(topic, 0, group.as_deref())
                .await
                .map_err(|err| err.to_string())?;
            Some(DLQDiscardPolicyWire::CustomDQL {
                tp: target.tp.into_boxed_str(),
                part: target.part,
                group: target.group.map(String::into_boxed_str),
            })
        }
    };

    Ok(DeclareMeta {
        dlq_policy,
        dlq_max_retries: declare.dlq_max_retries,
        default_message_ttl_ms: declare.default_message_ttl_ms,
    })
}

async fn send_error_response(
    tx: &mpsc::Sender<Frame>,
    request_id: u64,
    code: u16,
    message: impl Into<String>,
) -> Result<(), ProtocolConnectionError> {
    tx.send(error_frame(request_id, code, message)?).await?;
    Ok(())
}

/// Queue a batch of settle requests with one permit reservation instead of one
/// awaited send per request. Falls back to per-request sends when the batch is
/// larger than the channel capacity (a frame carrying more tags than the
/// subscription can have inflight).
async fn send_settle_batch(
    state_settler: &mpsc::Sender<SettleRequest>,
    pending_settles: &Arc<AtomicUsize>,
    reqs: Vec<SettleRequest>,
) {
    let n = reqs.len();
    if n == 0 {
        return;
    }
    pending_settles.fetch_add(n, Ordering::AcqRel);
    match state_settler.reserve_many(n).await {
        Ok(permits) => {
            for (permit, req) in permits.zip(reqs) {
                permit.send(req);
            }
        }
        Err(_) => {
            let mut sent = 0;
            for req in reqs {
                if state_settler.send(req).await.is_err() {
                    break;
                }
                sent += 1;
            }
            if sent < n {
                pending_settles.fetch_sub(n - sent, Ordering::AcqRel);
            }
        }
    }
}

async fn send_error_response_and_count(
    tx: &mpsc::Sender<Frame>,
    metrics: &TcpStats,
    request_id: u64,
    code: u16,
    message: impl Into<String>,
) {
    if let Err(err) = send_error_response(tx, request_id, code, message).await {
        tracing::error!("failed to send error response: {err}");
    }
    metrics.error();
}

/// Map a broker operation error to a client response code and a message that
/// names the likely fix or check where one exists. Exhaustive so a new
/// `BrokerError` variant has to choose its code and guide here rather than
/// silently defaulting to an unguided 500.
fn broker_error_response(err: &BrokerError) -> (u16, String) {
    match err {
        // Control flow, not a failure: the caller reached the wrong broker. The
        // caller resolves the owner and retries, so the message stays as is.
        BrokerError::NotOwner { .. } => (ERR_NOT_OWNER, err.to_string()),

        // The request itself is malformed or out of range. A client fix, not a
        // server fault, so 400 (retry_advice reads this as DoNotRetry) rather
        // than a retryable 500. The call site's message already says what.
        BrokerError::InvalidArgument(msg) => (ERR_INVALID, msg.clone()),

        // Too few replicas are caught up to meet the queue's durability policy.
        // Usually transient (a replica catches up or rejoins), so it stays a
        // retryable 500, but the guide names the two levers so a persistent case
        // is actionable rather than a bare number.
        BrokerError::NotEnoughInSyncReplicas {
            topic,
            partition,
            in_sync,
            required,
        } => (
            500,
            format!(
                "queue {topic}/{partition} has {in_sync} in-sync replica(s), below the \
                 {required} its durability policy requires. This usually clears as replicas \
                 catch up or rejoin; if it persists, bring replicas online or lower the \
                 queue replication factor"
            ),
        ),

        // A genuine broker-side fault. Keep the retryable 500 but say plainly it
        // is not the caller's request, so a user does not chase a fix on their
        // end. A short trace id is stamped on the client message AND logged
        // here with the full error, so "the broker logs carry the cause"
        // comes with something to actually search for.
        BrokerError::Storage(_)
        | BrokerError::Engine(_)
        | BrokerError::ChannelClosed
        | BrokerError::InvalidReplicationProgress { .. }
        | BrokerError::Unknown(_) => {
            let trace = format!("{:08x}", uuid::Uuid::now_v7().as_u128() as u32);
            tracing::error!("internal broker error [trace {trace}]: {err}");
            (
                500,
                format!(
                    "internal broker error: {err}. This is a broker-side fault, not a \
                     problem with your request - search the broker logs for trace id {trace}"
                ),
            )
        }
    }
}

/// Build an `Op::Redirect` frame for a queue partition if the topology source
/// can resolve its current owner. Redirect is control flow (not an error): it
/// tells the client to retry against the right broker. `None` => no source or
/// owner unknown, so the caller should fall back to its terminal error.
fn owner_redirect_frame(
    topology_source: &Option<Arc<dyn ClientTopologySource>>,
    request_id: u64,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
) -> Option<Frame> {
    let (owner_endpoint, partitioning_version) = topology_source
        .as_ref()?
        .owner_endpoint(topic, partition, group)?;
    let redirect = Redirect {
        topic: topic.to_string(),
        partition,
        group: group.map(str::to_string),
        owner_endpoints: AdvertisedAddress::parse(&owner_endpoint)
            .into_iter()
            .collect(),
        partitioning_version,
    };
    try_encode(Op::Redirect, request_id, &redirect).ok()
}

/// Build an `Op::Redirect` frame for a stream partition if the topology source
/// can resolve its owner. Streams have no group. `None` => fall back to a plain
/// not-owner error.
fn stream_owner_redirect_frame(
    topology_source: &Option<Arc<dyn ClientTopologySource>>,
    request_id: u64,
    topic: &str,
    partition: Partition,
) -> Option<Frame> {
    let (owner_endpoint, partitioning_version) = topology_source
        .as_ref()?
        .stream_owner_endpoint(topic, partition)?;
    let redirect = Redirect {
        topic: topic.to_string(),
        partition,
        group: None,
        owner_endpoints: AdvertisedAddress::parse(&owner_endpoint)
            .into_iter()
            .collect(),
        partitioning_version,
    };
    try_encode(Op::Redirect, request_id, &redirect).ok()
}

/// Fence a publish against a stale partitioning view. The client stamps the
/// partitioning version it routed under; if that lags the queue's authoritative
/// version, the partition it chose may no longer be correct, so we redirect it
/// to re-fetch topology and re-route. Returns `true` if a redirect was sent (the
/// caller must stop processing the publish). A missing topology source, an
/// unknown owner, or an up-to-date (>=) client version all return `false` so the
/// publish proceeds — version `0` against a v0 queue is the standalone path.
async fn fence_stale_partitioning(
    tx: &FrameSink,
    request_id: u64,
    topology_source: &Option<Arc<dyn ClientTopologySource>>,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
    client_version: u64,
) -> bool {
    let Some(source) = topology_source.as_ref() else {
        return false;
    };
    let Some((owner_endpoint, current_version)) = source.owner_endpoint(topic, partition, group)
    else {
        return false;
    };
    if client_version >= current_version {
        return false;
    }
    let redirect = Redirect {
        topic: topic.to_string(),
        partition,
        group: group.map(str::to_string),
        owner_endpoints: AdvertisedAddress::parse(&owner_endpoint)
            .into_iter()
            .collect(),
        partitioning_version: current_version,
    };
    match try_encode(Op::Redirect, request_id, &redirect) {
        Ok(frame) => tx.send(frame).await.is_ok(),
        Err(_) => false,
    }
}

/// Respond to a client-facing operation error. On `NotOwner`, emit a redirect
/// when the owner is resolvable; otherwise fall back to the plain error
/// (terminal `ERR_NOT_OWNER` when the owner is unknown).
async fn send_owner_redirect_or_error(
    tx: &FrameSink,
    metrics: &TcpStats,
    request_id: u64,
    topology_source: &Option<Arc<dyn ClientTopologySource>>,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
    err: &BrokerError,
) {
    if matches!(err, BrokerError::NotOwner { .. }) {
        if let Some(frame) =
            owner_redirect_frame(topology_source, request_id, topic, partition, group)
        {
            if tx.send(frame).await.is_ok() {
                return;
            }
        }
    }
    let (code, message) = broker_error_response(err);
    send_error_response_and_count(tx, metrics, request_id, code, message).await;
}

fn replication_checkpoint_required(
    epoch: u64,
    requested_offset: u64,
    head_offset: u64,
    next_offset: u64,
) -> ReplicationCheckpointRequired {
    ReplicationCheckpointRequired {
        epoch,
        requested_offset,
        head_offset,
        next_offset,
    }
}

fn to_replication_message_read(read: OwnerReplicationRead<Message>) -> ReplicationMessageRead {
    match read {
        OwnerReplicationRead::Batch(batch) => ReplicationMessageRead::Batch {
            epoch: batch.epoch,
            requested_offset: batch.requested_offset,
            next_offset: batch.next_offset,
            records: batch
                .records
                .into_iter()
                .map(|(offset, message)| ReplicationMessageRecord {
                    offset,
                    flags: message.flags,
                    headers: message.headers,
                    payload: message.payload,
                })
                .collect(),
        },
        OwnerReplicationRead::CheckpointRequired {
            epoch,
            requested_offset,
            head_offset,
            next_offset,
        } => ReplicationMessageRead::CheckpointRequired(replication_checkpoint_required(
            epoch,
            requested_offset,
            head_offset,
            next_offset,
        )),
    }
}

fn to_replication_event_read(
    read: OwnerReplicationRead<StromaEvent>,
) -> std::io::Result<ReplicationEventRead> {
    match read {
        OwnerReplicationRead::Batch(batch) => {
            let records = batch
                .records
                .into_iter()
                .map(|(offset, event)| {
                    let payload = event.encode()?;
                    Ok(ReplicationEventRecord { offset, payload })
                })
                .collect::<std::io::Result<Vec<_>>>()?;
            Ok(ReplicationEventRead::Batch {
                epoch: batch.epoch,
                requested_offset: batch.requested_offset,
                next_offset: batch.next_offset,
                records,
            })
        }
        OwnerReplicationRead::CheckpointRequired {
            epoch,
            requested_offset,
            head_offset,
            next_offset,
        } => Ok(ReplicationEventRead::CheckpointRequired(
            replication_checkpoint_required(epoch, requested_offset, head_offset, next_offset),
        )),
    }
}

fn to_replication_read_ok(
    records: BrokerOwnerReplicationRecords,
) -> std::io::Result<ReplicationReadOk> {
    Ok(ReplicationReadOk {
        messages: to_replication_message_read(records.messages),
        events: to_replication_event_read(records.events)?,
    })
}

/// Broker-backed source for an owner replication stream sender. Bridges the
/// generic [`replication_stream::OwnerStreamSource`] to the broker's read +
/// follower-progress calls (the same ones the pull `ReplicationRead` path uses).
struct BrokerOwnerStreamSource {
    broker: Arc<Broker<StromaEngine>>,
}

#[async_trait::async_trait]
impl replication_stream::OwnerStreamSource for BrokerOwnerStreamSource {
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
    ) -> Result<ReplicationReadOk, String> {
        let records = self
            .broker
            .read_owner_replication_records(
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
            .map_err(|err| broker_error_response(&err).1.to_string())?;
        to_replication_read_ok(records).map_err(|err| err.to_string())
    }

    fn record_progress(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
        reporter: &str,
        durable_message_next: u64,
        durable_event_next: u64,
    ) {
        self.broker.record_follower_replication_progress(
            topic,
            partition,
            group,
            reporter,
            durable_message_next,
            durable_event_next,
        );
    }
}

fn ensure_contiguous_offsets<I>(offsets: I) -> Result<(), String>
where
    I: IntoIterator<Item = u64>,
{
    let mut iter = offsets.into_iter();
    let Some(first) = iter.next() else {
        return Ok(());
    };
    let mut expected = first + 1;
    for offset in iter {
        if offset != expected {
            return Err(format!(
                "replication apply records must be contiguous; expected offset {expected}, got {offset}"
            ));
        }
        expected += 1;
    }
    Ok(())
}

fn to_owner_message_read(
    batch: Option<ReplicationMessageApplyBatch>,
) -> Result<OwnerReplicationRead<Message>, String> {
    let Some(batch) = batch else {
        return Ok(OwnerReplicationRead::Batch(OwnerReplicationBatch {
            epoch: 0,
            requested_offset: 0,
            next_offset: 0,
            records: Vec::new(),
        }));
    };

    ensure_contiguous_offsets(batch.records.iter().map(|record| record.offset))?;
    let requested_offset = batch.records.first().map_or(0, |record| record.offset);
    let next_offset = batch.records.last().map_or(0, |record| record.offset + 1);
    let records = batch
        .records
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
        .collect();

    Ok(OwnerReplicationRead::Batch(OwnerReplicationBatch {
        epoch: batch.epoch,
        requested_offset,
        next_offset,
        records,
    }))
}

fn to_owner_event_read(
    batch: Option<ReplicationEventApplyBatch>,
) -> Result<OwnerReplicationRead<StromaEvent>, String> {
    let Some(batch) = batch else {
        return Ok(OwnerReplicationRead::Batch(OwnerReplicationBatch {
            epoch: 0,
            requested_offset: 0,
            next_offset: 0,
            records: Vec::new(),
        }));
    };

    ensure_contiguous_offsets(batch.records.iter().map(|record| record.offset))?;
    let requested_offset = batch.records.first().map_or(0, |record| record.offset);
    let next_offset = batch.records.last().map_or(0, |record| record.offset + 1);
    let records = batch
        .records
        .into_iter()
        .map(|record| {
            let event = StromaEvent::decode(&record.payload).map_err(|err| err.to_string())?;
            Ok((record.offset, event))
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(OwnerReplicationRead::Batch(OwnerReplicationBatch {
        epoch: batch.epoch,
        requested_offset,
        next_offset,
        records,
    }))
}

fn to_owner_replication_records(
    apply: ReplicationApply,
) -> Result<BrokerOwnerReplicationRecords, String> {
    Ok(BrokerOwnerReplicationRecords {
        messages: to_owner_message_read(apply.messages)?,
        events: to_owner_event_read(apply.events)?,
    })
}

fn to_replication_apply_ok(messages_applied: bool, events_applied: bool) -> ReplicationApplyOk {
    ReplicationApplyOk {
        messages_applied,
        events_applied,
    }
}

fn to_replication_state_checkpoint(checkpoint: OwnerStateCheckpoint) -> ReplicationStateCheckpoint {
    ReplicationStateCheckpoint {
        message_epoch: checkpoint.message_epoch,
        event_epoch: checkpoint.event_epoch,
        message_checkpoint_offset: checkpoint.message_checkpoint_offset,
        message_next_offset: checkpoint.message_next_offset,
        event_next_offset: checkpoint.event_next_offset,
        applied_event_offset: checkpoint.applied_event_offset,
        state_snapshot: checkpoint.state_snapshot,
    }
}

fn to_follower_state_checkpoint_install(
    checkpoint: ReplicationStateCheckpoint,
) -> FollowerStateCheckpointInstall {
    FollowerStateCheckpointInstall {
        message_next_offset: checkpoint.message_checkpoint_offset,
        event_next_offset: checkpoint.event_next_offset,
        applied_event_offset: checkpoint.applied_event_offset,
        state_snapshot: checkpoint.state_snapshot,
    }
}

#[derive(Debug, thiserror::Error)]
enum InstallSubscriptionError {
    #[error(transparent)]
    Broker(#[from] BrokerError),

    #[error("already subscribed")]
    AlreadySubscribed,

    #[error(
        "queue already has a different exclusive cohort (one cohort per queue; \
         use a separate group for an unrelated consumer set)"
    )]
    CohortConflict,

    #[error("cohort member id must not be nil")]
    InvalidMemberId,

    #[error(
        "connection already joined this cohort under a different member id (one \
         member identity per connection)"
    )]
    MemberIdConflict,
}

fn install_subscription_error_response(err: &InstallSubscriptionError) -> (u16, String) {
    match err {
        InstallSubscriptionError::Broker(err) => broker_error_response(err),
        InstallSubscriptionError::AlreadySubscribed => (ERR_CONFLICT, err.to_string()),
        InstallSubscriptionError::CohortConflict => (ERR_CONFLICT, err.to_string()),
        InstallSubscriptionError::InvalidMemberId => (ERR_INVALID, err.to_string()),
        InstallSubscriptionError::MemberIdConflict => (ERR_CONFLICT, err.to_string()),
    }
}

struct SubState {
    sub_id: u64,
    partition: Partition,
    /// Exclusive cohort id this subscription joined, if any (drives the
    /// broker-side leave on unsubscribe/disconnect, and reconnect restore).
    consumer_group: Option<String>,
    /// Soft per-consumer target for the cohort (echoed for reconnect restore).
    consumer_target: Option<u32>,
    /// Resolved cluster cohort member id for this subscription (the cohort key);
    /// `Some` iff exclusive. Used for leave and echoed for reconnect.
    cohort_member: Option<Uuid>,
    auto_ack: bool,
    prefetch: u32,
    stats_sub_id: Option<Uuid>,
    state_settler: tokio::sync::mpsc::Sender<SettleRequest>,
    pending_settles: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
    _activity_lease: ConsumerLease,
}

fn sort_reconcile_results(results: &mut [ReconcileSubscriptionResult]) {
    results.sort_by(|a, b| {
        let a_sub = a.client.as_ref().or(a.server.as_ref());
        let b_sub = b.client.as_ref().or(b.server.as_ref());
        a_sub
            .map(|sub| (&sub.group, &sub.topic, sub.sub_id))
            .cmp(&b_sub.map(|sub| (&sub.group, &sub.topic, sub.sub_id)))
    });
}

fn reconcile_subscription_from_state(
    topic: &str,
    group: Option<&str>,
    sub: &SubState,
) -> ReconcileSubscription {
    ReconcileSubscription {
        sub_id: sub.sub_id,
        topic: topic.to_string(),
        group: group.map(str::to_string),
        partition: sub.partition,
        auto_ack: sub.auto_ack,
        prefetch: sub.prefetch,
        consumer_group: sub.consumer_group.clone(),
        consumer_target: sub.consumer_target,
        member_id: sub.cohort_member,
    }
}

struct CachedPublisher {
    handle: PublisherHandle,
    last_used_ms: u64,
}

struct LogicalConnection {
    client_id: Uuid,
    state: Mutex<ConnState>,
    transport: watch::Sender<Option<FrameSink>>,
    generation: AtomicU64,
}

impl std::fmt::Debug for LogicalConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogicalConnection")
            .field("client_id", &self.client_id)
            .field("generation", &self.generation.load(Ordering::Acquire))
            .finish_non_exhaustive()
    }
}

impl LogicalConnection {
    fn new(client_id: Uuid) -> Arc<Self> {
        let (transport, _) = watch::channel(None);
        Arc::new(Self {
            client_id,
            state: Mutex::new(ConnState {
                authenticated: false,
                subs: HashMap::new(),
                stream_subs: HashMap::new(),
                exclusive_forwarders: HashMap::new(),
            }),
            transport,
            generation: AtomicU64::new(0),
        })
    }

    fn attach_transport(&self, tx: FrameSink) -> u64 {
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.transport.send_replace(Some(tx));
        generation
    }

    fn mark_dormant(&self, generation: u64) -> bool {
        if self.generation.load(Ordering::Acquire) != generation {
            return false;
        }
        self.transport.send_replace(None);
        true
    }

    fn is_dormant_generation(&self, generation: u64) -> bool {
        self.generation.load(Ordering::Acquire) == generation && self.transport.borrow().is_none()
    }
}

/// Send a frame to the connection's current transport. `cached_tx` avoids a
/// watch borrow and sink clone per frame and is refreshed only when the cached
/// sink dies or the transport is swapped on reconnect.
async fn send_to_current_transport(
    transport_rx: &mut watch::Receiver<Option<FrameSink>>,
    cached_tx: &mut Option<FrameSink>,
    mut frame: Frame,
) -> Result<(), ()> {
    loop {
        if cached_tx.is_none() {
            *cached_tx = transport_rx.borrow_and_update().clone();
        }
        match cached_tx.as_ref() {
            Some(tx) => match tx.send(frame).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    frame = err.0;
                    *cached_tx = None;
                    if !transport_rx.has_changed().map_err(|_| ())? {
                        transport_rx.changed().await.map_err(|_| ())?;
                    }
                }
            },
            None => transport_rx.changed().await.map_err(|_| ())?,
        }
    }
}

async fn cleanup_connection_state(
    broker: Arc<Broker<StromaEngine>>,
    logical: Arc<LogicalConnection>,
) {
    broker.wait_for_pending_settles().await;

    let (drained_subs, drained_stream_subs, drained_forwarders) = {
        let mut state = logical.state.lock().await;
        let subs = state.subs.drain().collect::<Vec<_>>();
        let stream_subs = state.stream_subs.drain().collect::<Vec<_>>();
        let forwarders = state.exclusive_forwarders.drain().collect::<Vec<_>>();
        (subs, stream_subs, forwarders)
    };

    // Tear down Plexus stream subscriptions (abort tasks, detach from fan-out).
    for (_key, stream_sub) in drained_stream_subs {
        teardown_stream_sub(stream_sub).await;
    }

    // Stop assignment forwarders (the broker also drops their senders on leave).
    for (_cohort, handle) in drained_forwarders {
        handle.abort();
    }

    for ((topic, partition, group), sub) in drained_subs {
        sub.task.abort();
        if let Err(err) = broker
            .unsubscribe(&topic, group.as_deref(), sub.partition, sub.sub_id)
            .await
        {
            tracing::warn!("Failed to unsubscribe consumer {}: {err}", sub.sub_id);
        }
        if let (Some(consumer_group), Some(member)) =
            (sub.consumer_group.as_deref(), sub.cohort_member)
        {
            broker.exclusive_group_leave(
                &topic,
                partition,
                group.as_deref(),
                consumer_group,
                &member.to_string(),
            );
        }
    }
}

async fn remove_subscription(
    broker: &Arc<Broker<StromaEngine>>,
    logical: &Arc<LogicalConnection>,
    connection_stats: &Arc<ConnectionStats>,
    conn_id: &Uuid,
    topic: &str,
    partition: Partition,
    group: Option<&str>,
) -> Option<ReconcileSubscription> {
    let key: SubKey = (topic.to_string(), partition, group.map(str::to_string));
    let sub = {
        let mut state = logical.state.lock().await;
        state.subs.remove(&key)
    }?;

    sub.task.abort();
    broker.wait_for_pending_settles().await;
    if let Some(stats_sub_id) = sub.stats_sub_id {
        connection_stats.remove_sub(conn_id, &stats_sub_id);
    }
    if let Err(err) = broker
        .unsubscribe(topic, group, sub.partition, sub.sub_id)
        .await
    {
        tracing::warn!("Failed to unsubscribe consumer {}: {err}", sub.sub_id);
    }
    if let (Some(consumer_group), Some(member)) = (sub.consumer_group.as_deref(), sub.cohort_member)
    {
        broker.exclusive_group_leave(
            topic,
            sub.partition,
            group,
            consumer_group,
            &member.to_string(),
        );
    }

    Some(ReconcileSubscription {
        sub_id: sub.sub_id,
        topic: topic.to_string(),
        group: group.map(str::to_string),
        partition: sub.partition,
        auto_ack: sub.auto_ack,
        prefetch: sub.prefetch,
        consumer_group: sub.consumer_group.clone(),
        consumer_target: sub.consumer_target,
        member_id: sub.cohort_member,
    })
}

/// Spawn the per-(connection, cohort) task that forwards exclusive-group
/// assignment changes to the client as `AssignmentChanged` frames. Ends when the
/// member leaves the cohort (broker drops the sender) or the connection closes.
fn spawn_assignment_forwarder(
    logical: Arc<LogicalConnection>,
    req_id_gen: Arc<ReqIdGenerator>,
    metrics: Arc<TcpStats>,
    mut updates: mpsc::UnboundedReceiver<ExclusiveAssignmentUpdate>,
) -> tokio::task::JoinHandle<()> {
    let mut transport_rx = logical.transport.subscribe();
    tokio::spawn(async move {
        let mut cached_tx = None;
        while let Some(update) = updates.recv().await {
            let msg = AssignmentChanged {
                topic: update.topic,
                group: update.group,
                consumer_group: update.consumer_group,
                generation: update.generation,
                assigned: update.assigned,
                added: update.added,
                revoked: update.revoked,
            };
            let frame = match try_encode(Op::AssignmentChanged, req_id_gen.next_id(), &msg) {
                Ok(frame) => frame,
                Err(err) => {
                    tracing::error!("Failed to encode AssignmentChanged frame: {err}");
                    metrics.error();
                    break;
                }
            };
            if send_to_current_transport(&mut transport_rx, &mut cached_tx, frame)
                .await
                .is_err()
            {
                break;
            }
        }
    })
}

struct InstallSubscriptionArgs {
    broker: Arc<Broker<StromaEngine>>,
    logical: Arc<LogicalConnection>,
    connection_stats: Arc<ConnectionStats>,
    metrics: Arc<TcpStats>,
    conn_id: Uuid,
    client_id: Uuid,
    req_id_gen: Arc<ReqIdGenerator>,
    topic: String,
    partition: Partition,
    group: Option<String>,
    prefetch: u32,
    auto_ack: bool,
    /// Opt-in exclusive consumer-group id. `None` keeps the default competing
    /// behavior; `Some(id)` joins the cohort that exclusively divides the
    /// queue's partitions.
    consumer_group: Option<String>,
    /// Soft per-consumer target for the exclusive cohort (member's desired max
    /// partitions). Ignored without `consumer_group`.
    consumer_target: Option<usize>,
    /// Cluster cohort member id the client carries; `None` mints a fresh one.
    /// Ignored without `consumer_group`.
    member_id: Option<Uuid>,
}

async fn install_subscription(
    args: InstallSubscriptionArgs,
) -> Result<SubscribeOk, InstallSubscriptionError> {
    let sub_key: SubKey = (args.topic.clone(), args.partition, args.group.clone());

    if args.logical.state.lock().await.subs.contains_key(&sub_key) {
        return Err(InstallSubscriptionError::AlreadySubscribed);
    }

    // A queue has a single exclusive cohort; reject a conflicting second cohort
    // id before creating any subscription state.
    if let Some(consumer_group) = args.consumer_group.as_deref() {
        if args.broker.exclusive_cohort_conflicts(
            &args.topic,
            args.group.as_deref(),
            consumer_group,
        ) {
            return Err(InstallSubscriptionError::CohortConflict);
        }
    }

    // Resolve the cluster cohort member id for an exclusive sub. The broker is
    // the source of truth for one member identity per connection: the connection
    // establishes its id on its first exclusive subscribe (the id the client
    // carried, else a freshly minted one returned in SubscribeOk to echo on its
    // other brokers / reconnects), and every later exclusive subscribe on the
    // same connection must reuse it. A nil id is rejected, a mismatching id is a
    // conflict. `None` for non-exclusive subs.
    let cohort_member: Option<Uuid> = if args.consumer_group.is_some() {
        if matches!(args.member_id, Some(id) if id.is_nil()) {
            return Err(InstallSubscriptionError::InvalidMemberId);
        }
        let established = {
            let state = args.logical.state.lock().await;
            state.subs.values().find_map(|sub| sub.cohort_member)
        };
        let resolved = match (args.member_id, established) {
            (Some(id), Some(prev)) if id != prev => {
                return Err(InstallSubscriptionError::MemberIdConflict);
            }
            (Some(id), _) => id,
            (None, Some(prev)) => prev,
            (None, None) => Uuid::new_v4(),
        };
        Some(resolved)
    } else {
        None
    };

    let consumer = args
        .broker
        .subscribe(
            &args.topic,
            args.partition,
            args.group.as_deref(),
            args.client_id,
            ConsumerConfig {
                prefetch: args.prefetch as usize,
            },
        )
        .await?;

    let ConsumerHandle {
        messages,
        settler,
        sub_id,
        pending_settles,
        activity_lease,
        ..
    } = consumer;

    // Opt-in exclusive cohort: register this connection (member) on this
    // partition so the broker can gate delivery to the assigned member. The
    // queue's delivery loop already exists (subscribe created it).
    if let (Some(consumer_group), Some(member)) = (args.consumer_group.as_deref(), cohort_member) {
        let member = member.to_string();
        // Ensure exactly one assignment-change forwarder per (connection, cohort),
        // registered BEFORE the join so this member sees its initial assignment.
        let cohort_key: CohortKey = (
            args.topic.clone(),
            args.group.clone(),
            consumer_group.to_string(),
        );
        let needs_forwarder = !args
            .logical
            .state
            .lock()
            .await
            .exclusive_forwarders
            .contains_key(&cohort_key);
        if needs_forwarder {
            let updates = args.broker.register_exclusive_member(
                &args.topic,
                args.group.as_deref(),
                consumer_group,
                member.clone(),
            );
            let handle = spawn_assignment_forwarder(
                args.logical.clone(),
                args.req_id_gen.clone(),
                args.metrics.clone(),
                updates,
            );
            args.logical
                .state
                .lock()
                .await
                .exclusive_forwarders
                .insert(cohort_key, handle);
        }
        args.broker.exclusive_group_join(
            &args.topic,
            args.partition,
            args.group.as_deref(),
            consumer_group,
            member,
            sub_id,
            args.consumer_target,
        );
    }

    let mut transport_rx = args.logical.transport.subscribe();

    let stats_sub_id = args.connection_stats.add_sub(
        &args.conn_id,
        args.topic.clone(),
        args.group.clone(),
        Instant::now(),
        args.auto_ack,
    );

    let settler_clone = settler.clone();
    let pending_settles_clone = pending_settles.clone();
    let req_id_gen_clone = args.req_id_gen.clone();
    let metrics = args.metrics.clone();
    let auto_ack = args.auto_ack;
    let handle = tokio::spawn(async move {
        let mut rx = messages;
        let mut cached_tx = None;

        'pump: while let Some(batch) = rx.recv().await {
            let mut auto_ack_tags = Vec::new();
            for msg in batch {
                let delivery_tag = msg.delivery_tag;
                let message = msg.message;
                let mut headers = message.headers;
                if message.retried > 0 {
                    headers.insert("fibril.retries".into(), message.retried.to_string());
                }

                let deliver = Deliver {
                    sub_id,
                    topic: message.topic,
                    group: msg.group,
                    partition: message.partition,
                    offset: message.offset,
                    delivery_tag,
                    published: message.published,
                    publish_received: message.publish_received,
                    content_type: to_protocol_content_type(message.content_type),
                    headers,
                    payload: message.payload,
                };

                tracing::debug!("Sending Deliver");

                let frame = match wire::encode_deliver(req_id_gen_clone.next_id(), &deliver) {
                    Ok(frame) => frame,
                    Err(err) => {
                        tracing::error!("Failed to encode Deliver frame: {err}");
                        metrics.error();
                        break 'pump;
                    }
                };
                if send_to_current_transport(&mut transport_rx, &mut cached_tx, frame)
                    .await
                    .is_err()
                {
                    tracing::warn!("Failed to send deliver frame to active transport");
                    metrics.error();
                    break 'pump;
                }

                if auto_ack {
                    auto_ack_tags.push(delivery_tag);
                }
            }

            if !auto_ack_tags.is_empty() {
                let reqs = auto_ack_tags
                    .into_iter()
                    .map(|delivery_tag| SettleRequest {
                        settle_type: SettleType::Ack,
                        delivery_tag,
                    })
                    .collect();
                send_settle_batch(&settler, &pending_settles_clone, reqs).await;
            }
        }
    });

    args.logical.state.lock().await.subs.insert(
        sub_key,
        SubState {
            sub_id,
            partition: consumer.partition,
            consumer_group: args.consumer_group.clone(),
            consumer_target: args.consumer_target.map(|target| target as u32),
            cohort_member,
            task: handle,
            auto_ack: args.auto_ack,
            prefetch: args.prefetch,
            stats_sub_id,
            state_settler: settler_clone,
            pending_settles,
            _activity_lease: activity_lease,
        },
    );

    Ok(SubscribeOk {
        sub_id,
        topic: args.topic,
        group: args.group,
        partition: consumer.partition,
        prefetch: args.prefetch,
        consumer_group: args.consumer_group,
        consumer_target: args.consumer_target.map(|target| target as u32),
        member_id: cohort_member,
    })
}

/// Resolve a wire `SubscribeStream` start into the broker-side start position.
/// A durable name resumes its cursor (earliest on a fresh name); an ephemeral
/// subscription uses the requested start.
fn resolve_stream_start(durable_name: Option<&str>, start: StreamStart) -> SubscribeStart {
    match durable_name {
        Some(name) => SubscribeStart::DurableResume {
            name: name.to_string(),
        },
        None => match start {
            StreamStart::Latest => SubscribeStart::Latest,
            StreamStart::Earliest => SubscribeStart::Earliest,
            StreamStart::Offset { offset } => SubscribeStart::Offset(offset),
            StreamStart::NBack { count } => SubscribeStart::NBack(count),
            StreamStart::ByTime { time_ms } => SubscribeStart::ByTime(time_ms),
        },
    }
}

/// Register a stream subscription on this connection: resolve its start, attach to
/// the fan-out channel, and spawn the backfill+live driver plus the delivery task
/// that turns records into `Deliver` frames (auto-acking the durable cursor when
/// requested). Returns the `SubscribeOk` to send back.
async fn install_stream_subscription(
    broker: &Arc<Broker<StromaEngine>>,
    logical: &Arc<LogicalConnection>,
    req_id_gen: &Arc<ReqIdGenerator>,
    metrics: &Arc<TcpStats>,
    sub: SubscribeStream,
) -> Result<SubscribeOk, (u16, String)> {
    let channel = broker
        .route_stream(&sub.topic, sub.partition.id())
        .await
        .ok_or_else(|| {
            (
                ERR_NOT_FOUND,
                format!(
                    "stream {} partition {} not found - declare the stream first (declare \
                     plexus) or check the topic name",
                    sub.topic,
                    sub.partition.id()
                ),
            )
        })?;

    let start = resolve_stream_start(sub.durable_name.as_deref(), sub.start);
    let filter = (!sub.filter.is_empty())
        .then(|| StreamFilter::from_pairs(sub.filter.iter().map(|(k, v)| (k.clone(), v.clone()))));

    let sub_id = req_id_gen.next_id();
    let (sink_tx, mut sink_rx) =
        mpsc::channel::<Arc<StreamRecord>>(STREAM_DELIVERY_CHANNEL_CAPACITY);

    // Driver: backfill (durable log then ring) then live records into the sink,
    // re-attaching from its watermark after a lag eviction so delivery stays
    // contiguous per subscriber.
    let driver = tokio::spawn({
        let channel = channel.clone();
        async move { channel.run_subscription(start, filter, sink_tx).await }
    });

    // Delivery: records -> Deliver frames on the current transport. The stream
    // delivery tag is the record offset, so an ack settles the cursor by offset.
    let mut transport_rx = logical.transport.subscribe();
    let req_id_gen_clone = req_id_gen.clone();
    let metrics = metrics.clone();
    let broker_stats = broker.broker_metrics().cloned();
    let topic = sub.topic.clone();
    let partition = sub.partition;
    let auto_ack = sub.auto_ack;
    let durable_name = sub.durable_name.clone();
    let settle_channel = channel.clone();
    let delivery = tokio::spawn(async move {
        let mut cached_tx = None;
        while let Some(record) = sink_rx.recv().await {
            let deliver = Deliver {
                sub_id,
                topic: topic.clone(),
                group: None,
                partition,
                offset: record.offset,
                delivery_tag: DeliveryTag {
                    epoch: record.offset,
                },
                published: record.published,
                publish_received: record.publish_received,
                content_type: record.content_type.clone().map(ContentType::from_header),
                headers: record.headers.clone(),
                payload: record.payload.to_vec(),
            };
            let frame = match wire::encode_deliver(req_id_gen_clone.next_id(), &deliver) {
                Ok(frame) => frame,
                Err(err) => {
                    tracing::error!("Failed to encode stream Deliver frame: {err}");
                    metrics.error();
                    break;
                }
            };
            if send_to_current_transport(&mut transport_rx, &mut cached_tx, frame)
                .await
                .is_err()
            {
                tracing::warn!("Failed to send stream deliver frame to active transport");
                metrics.error();
                break;
            }
            if let Some(stats) = &broker_stats {
                stats.delivered();
            }
            if auto_ack {
                if let Some(name) = &durable_name {
                    if let Err(err) = settle_channel.settle(name, record.offset).await {
                        tracing::warn!("stream auto-ack settle failed: {err}");
                    }
                }
            }
        }
    });

    logical.state.lock().await.stream_subs.insert(
        (sub.topic.clone(), sub.partition),
        StreamSubState {
            durable_name: sub.durable_name,
            channel,
            auto_ack: sub.auto_ack,
            tasks: vec![driver, delivery],
        },
    );

    Ok(SubscribeOk {
        sub_id,
        topic: sub.topic,
        group: None,
        partition: sub.partition,
        prefetch: sub.prefetch,
        consumer_group: None,
        consumer_target: None,
        member_id: None,
    })
}

/// Tear down a stream subscription's tasks. Aborting the driver runs its
/// unsubscribe guard, which detaches the current fan-out registration.
async fn teardown_stream_sub(sub: StreamSubState) {
    for task in &sub.tasks {
        task.abort();
    }
}

/// Persist a record to a stream and fan it out, then confirm the producer (when
/// requested). Used by the publish handler once a frame is routed to a stream.
#[allow(clippy::too_many_arguments)]
async fn handle_stream_publish(
    broker: &Arc<Broker<StromaEngine>>,
    channel: &Arc<StreamChannel>,
    tx: &mpsc::Sender<Frame>,
    request_id: u64,
    payload: Vec<u8>,
    published: u64,
    require_confirm: bool,
    content_type: Option<ContentType>,
    mut headers: HashMap<String, String>,
    publish_received: u64,
) -> Result<(), ProtocolConnectionError> {
    // A speculative stream may deliver ahead of durability, so the broker marks
    // its records with a server-owned header.
    if channel.durability() == BrokerStreamDurability::Speculative {
        headers.insert(HEADER_SPECULATIVE.to_string(), "1".to_string());
    }
    let msg_headers = MessageHeaders {
        published,
        publish_received,
        content_type: to_storage_content_type(content_type),
        extra: headers,
    };

    // The frame loop only enqueues; the channel drain handles fan-out and confirm
    // timing per tier (durable and speculative confirm on durability, ephemeral at
    // stage). The confirm handle resolves accordingly, so the publish path here is
    // the same regardless of tier.
    match channel.publish_pipelined(msg_headers, payload).await {
        Ok(confirm_rx) => {
            if let Some(stats) = broker.broker_metrics() {
                stats.published();
            }
            if require_confirm {
                let tx = tx.clone();
                let broker = broker.clone();
                let topic = channel.topic().to_string();
                let partition = Partition::new(channel.partition());
                tokio::spawn(async move {
                    match confirm_rx.await {
                        Ok(Ok(offset)) => {
                            // Local durability first, then the assignment's
                            // replication policy (a replicated durable stream
                            // waits for replica acks) before the producer sees
                            // the confirm. A no-op for express tiers / owner-only
                            // streams (the gate short-circuits on local-durable).
                            match broker
                                .await_replication_confirm(&topic, partition, None, offset)
                                .await
                            {
                                Ok(()) => {
                                    if let Ok(frame) =
                                        try_encode(Op::PublishOk, request_id, &PublishOk { offset })
                                    {
                                        let _ = tx.send(frame).await;
                                    }
                                }
                                Err(err) => {
                                    let _ = send_error_response(
                                        &tx,
                                        request_id,
                                        500,
                                        format!("stream replication confirm failed: {err}"),
                                    )
                                    .await;
                                }
                            }
                        }
                        _ => {
                            let _ = send_error_response(
                                &tx,
                                request_id,
                                500,
                                "stream record failed to persist",
                            )
                            .await;
                        }
                    }
                });
            }
        }
        Err(err) => {
            send_error_response(tx, request_id, 500, format!("stream publish failed: {err}"))
                .await?;
        }
    }
    Ok(())
}

async fn reconcile_subscriptions(
    broker: Arc<Broker<StromaEngine>>,
    logical: Arc<LogicalConnection>,
    connection_stats: Arc<ConnectionStats>,
    metrics: Arc<TcpStats>,
    req_id_gen: Arc<ReqIdGenerator>,
    conn_id: Uuid,
    client_id: Uuid,
    reconcile: ReconcileClient,
) -> ReconcileResult {
    let policy = reconcile.policy;
    let mut results = Vec::new();
    let mut seen = HashMap::<SubKey, ()>::new();
    metrics.reconcile_request();

    for client in reconcile.subscriptions {
        let key: SubKey = (client.topic.clone(), client.partition, client.group.clone());
        seen.insert(key.clone(), ());

        let server = {
            let state = logical.state.lock().await;
            state.subs.get(&key).map(|sub| {
                reconcile_subscription_from_state(&client.topic, client.group.as_deref(), sub)
            })
        };

        match server {
            Some(server)
                if server.partition == client.partition
                    && server.auto_ack == client.auto_ack
                    && server.prefetch == client.prefetch =>
            {
                let reason = if server.sub_id == client.sub_id {
                    "matched"
                } else {
                    "server_id_changed"
                };
                results.push(ReconcileSubscriptionResult {
                    client: Some(client),
                    server: Some(server),
                    action: ReconcileAction::Keep,
                    reason: reason.into(),
                });
            }
            Some(server) => {
                results.push(ReconcileSubscriptionResult {
                    client: Some(client),
                    server: Some(server),
                    action: ReconcileAction::CloseClientSide,
                    reason: "server_mismatch".into(),
                });
            }
            None if reconcile.policy == ReconcilePolicy::Restore => {
                let restored = install_subscription(InstallSubscriptionArgs {
                    broker: broker.clone(),
                    logical: logical.clone(),
                    connection_stats: connection_stats.clone(),
                    metrics: metrics.clone(),
                    conn_id,
                    client_id,
                    req_id_gen: req_id_gen.clone(),
                    topic: client.topic.clone(),
                    partition: client.partition,
                    group: client.group.clone(),
                    prefetch: client.prefetch,
                    auto_ack: client.auto_ack,
                    // Restore exclusive membership too: the client carries its
                    // cohort id + target in the reconcile request, so a reconnect
                    // rejoins the cohort instead of falling back to competing.
                    consumer_group: client.consumer_group.clone(),
                    consumer_target: client.consumer_target.map(|target| target as usize),
                    member_id: client.member_id,
                })
                .await;

                match restored {
                    Ok(ok) => {
                        let auto_ack = client.auto_ack;
                        results.push(ReconcileSubscriptionResult {
                            client: Some(client),
                            server: Some(ReconcileSubscription {
                                sub_id: ok.sub_id,
                                topic: ok.topic,
                                group: ok.group,
                                partition: ok.partition,
                                auto_ack,
                                prefetch: ok.prefetch,
                                consumer_group: ok.consumer_group,
                                consumer_target: ok.consumer_target,
                                member_id: ok.member_id,
                            }),
                            action: ReconcileAction::Keep,
                            reason: "server_restored".into(),
                        });
                    }
                    Err(err) => {
                        tracing::warn!("failed to restore subscription during reconcile: {err}");
                        results.push(ReconcileSubscriptionResult {
                            client: Some(client),
                            server: None,
                            action: ReconcileAction::CloseClientSide,
                            reason: "server_restore_failed".into(),
                        });
                    }
                }
            }
            None => {
                results.push(ReconcileSubscriptionResult {
                    client: Some(client),
                    server: None,
                    action: ReconcileAction::CloseClientSide,
                    reason: "server_missing".into(),
                });
            }
        }
    }

    let server_only = {
        let state = logical.state.lock().await;
        state
            .subs
            .keys()
            .filter(|key| !seen.contains_key(*key))
            .cloned()
            .collect::<Vec<_>>()
    };

    for (topic, partition, group) in server_only {
        if let Some(server) = remove_subscription(
            &broker,
            &logical,
            &connection_stats,
            &conn_id,
            &topic,
            partition,
            group.as_deref(),
        )
        .await
        {
            results.push(ReconcileSubscriptionResult {
                client: None,
                server: Some(server),
                action: ReconcileAction::CloseServerSide,
                reason: "client_missing".into(),
            });
        }
    }

    sort_reconcile_results(&mut results);
    let mut kept = 0_u64;
    let mut restored = 0_u64;
    let mut client_closed = 0_u64;
    let mut server_dropped = 0_u64;
    let mut mismatched = 0_u64;
    let mut restore_failed = 0_u64;

    for result in &results {
        match result.action {
            ReconcileAction::Keep => {
                kept += 1;
                metrics.reconcile_kept();
                if result.reason == "server_restored" {
                    restored += 1;
                    metrics.reconcile_restored();
                }
            }
            ReconcileAction::CloseClientSide => {
                client_closed += 1;
                metrics.reconcile_client_closed();
                if result.reason == "server_mismatch" {
                    mismatched += 1;
                    metrics.reconcile_mismatched();
                }
                if result.reason == "server_restore_failed" {
                    restore_failed += 1;
                    metrics.reconcile_restore_failed();
                }
            }
            ReconcileAction::CloseServerSide => {
                server_dropped += 1;
                metrics.reconcile_server_dropped();
            }
            ReconcileAction::RecreateClientSide => {}
        }
    }

    tracing::info!(
        client_id = %client_id,
        conn_id = %conn_id,
        policy = ?policy,
        kept,
        restored,
        client_closed,
        server_dropped,
        mismatched,
        restore_failed,
        "subscription reconciliation completed"
    );

    ReconcileResult {
        subscriptions: results,
    }
}

#[derive(Debug, Clone)]
struct ResumeSession {
    resume_token: Uuid,
    logical: Arc<LogicalConnection>,
}

#[derive(Debug)]
struct ResumeSessionRegistry {
    owner_id: Uuid,
    sessions: dashmap::DashMap<Uuid, ResumeSession>,
}

impl ResumeSessionRegistry {
    fn new() -> Self {
        Self {
            owner_id: Uuid::new_v4(),
            sessions: dashmap::DashMap::new(),
        }
    }

    fn resolve(&self, resume: Option<ResumeIdentity>) -> ResumeResolution {
        if let Some(resume) = resume {
            if resume.owner_id != self.owner_id {
                tracing::warn!(
                    client_id = %resume.client_id,
                    owner_id = %self.owner_id,
                    requested_owner_id = %resume.owner_id,
                    "client resume rejected for another owner"
                );
                return self.issue(ResumeOutcome::ResumeRejected);
            }

            if let Some(session) = self.sessions.get(&resume.client_id) {
                if session.resume_token == resume.resume_token {
                    tracing::info!(
                        client_id = %resume.client_id,
                        owner_id = %self.owner_id,
                        "client resume accepted"
                    );
                    return ResumeResolution {
                        client_id: resume.client_id,
                        resume_token: resume.resume_token,
                        outcome: ResumeOutcome::Resumed,
                        logical: session.logical.clone(),
                    };
                }

                tracing::warn!(
                    client_id = %resume.client_id,
                    owner_id = %self.owner_id,
                    "client resume rejected"
                );
                return self.issue(ResumeOutcome::ResumeRejected);
            }

            tracing::warn!(
                client_id = %resume.client_id,
                owner_id = %self.owner_id,
                "client resume identity not found"
            );
            return self.issue(ResumeOutcome::ResumeNotFound);
        }

        self.issue(ResumeOutcome::New)
    }

    fn issue(&self, outcome: ResumeOutcome) -> ResumeResolution {
        let client_id = Uuid::now_v7();
        let resume_token = Uuid::new_v4();
        let logical = LogicalConnection::new(client_id);
        self.sessions.insert(
            client_id,
            ResumeSession {
                resume_token,
                logical: logical.clone(),
            },
        );
        tracing::info!(
            client_id = %client_id,
            owner_id = %self.owner_id,
            resume_outcome = ?outcome,
            "client identity issued"
        );
        ResumeResolution {
            client_id,
            resume_token,
            outcome,
            logical,
        }
    }

    /// Push a [`GoingAway`] drain notice to every session that currently has a
    /// live transport attached. Best-effort: dormant sessions (no socket) are
    /// skipped, and a frame that does not fit the sink buffer is dropped rather
    /// than blocking the drain. Returns how many notices were sent.
    async fn broadcast_going_away(&self, grace_ms: u64, message: &str) -> usize {
        let notice = GoingAway {
            grace_ms,
            message: message.to_string(),
        };
        let frame = match try_encode(Op::GoingAway, 0, &notice) {
            Ok(frame) => frame,
            Err(err) => {
                tracing::error!("failed to encode going-away notice: {err:?}");
                return 0;
            }
        };
        // Snapshot the live sinks first so no DashMap guard is held across await.
        let sinks: Vec<FrameSink> = self
            .sessions
            .iter()
            .filter_map(|entry| entry.value().logical.transport.borrow().clone())
            .collect();
        let mut sent = 0;
        for sink in sinks {
            if sink.send(frame.clone()).await.is_ok() {
                sent += 1;
            }
        }
        sent
    }

    fn forget_if_dormant(
        &self,
        client_id: Uuid,
        logical: &Arc<LogicalConnection>,
        generation: u64,
    ) {
        if !logical.is_dormant_generation(generation) {
            return;
        }
        self.sessions.remove(&client_id);
    }

    fn forget_if_generation(
        &self,
        client_id: Uuid,
        logical: &Arc<LogicalConnection>,
        generation: u64,
    ) {
        if logical.generation.load(Ordering::Acquire) != generation {
            return;
        }
        self.sessions.remove(&client_id);
    }
}

struct ResumeResolution {
    client_id: Uuid,
    resume_token: Uuid,
    outcome: ResumeOutcome,
    logical: Arc<LogicalConnection>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ConnectionRuntimeSettings {
    pub publisher_cache_idle_timeout_ms: Option<u64>,
    pub reconnect_grace_ms: Option<u64>,
}

#[derive(Clone)]
pub struct ConnectionSettings {
    pub heartbeat_interval: Option<u64>,
    runtime: Arc<ArcSwap<ConnectionRuntimeSettings>>,
    resume_sessions: Arc<ResumeSessionRegistry>,
}

impl ConnectionSettings {
    pub fn new(heartbeat_interval: Option<u64>) -> Self {
        Self {
            heartbeat_interval,
            runtime: Arc::new(ArcSwap::from_pointee(ConnectionRuntimeSettings::default())),
            resume_sessions: Arc::new(ResumeSessionRegistry::new()),
        }
    }

    pub fn with_reconnect_grace_ms(self, reconnect_grace_ms: Option<u64>) -> Self {
        let mut runtime = *self.runtime_snapshot();
        runtime.reconnect_grace_ms = reconnect_grace_ms;
        self.update_runtime(runtime);
        self
    }

    pub fn with_publisher_cache_idle_timeout_ms(self, timeout_ms: Option<u64>) -> Self {
        let mut runtime = *self.runtime_snapshot();
        runtime.publisher_cache_idle_timeout_ms = timeout_ms;
        self.update_runtime(runtime);
        self
    }

    pub fn update_runtime(&self, settings: ConnectionRuntimeSettings) {
        self.runtime.store(Arc::new(settings));
    }

    pub fn runtime_snapshot(&self) -> Arc<ConnectionRuntimeSettings> {
        self.runtime.load_full()
    }

    /// Announce a planned drain to every connected client (see [`GoingAway`]).
    /// The server calls this at the start of a graceful shutdown so clients
    /// settle in-flight work and reconnect (redirecting to the current owner)
    /// rather than discovering the broker gone. Returns how many were notified.
    pub async fn announce_going_away(&self, grace_ms: u64, message: &str) -> usize {
        self.resume_sessions
            .broadcast_going_away(grace_ms, message)
            .await
    }
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self::new(None)
    }
}

impl std::fmt::Debug for ConnectionSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionSettings")
            .field("heartbeat_interval", &self.heartbeat_interval)
            .field("runtime", &self.runtime_snapshot())
            .field("owner_id", &self.resume_sessions.owner_id)
            .finish()
    }
}

#[derive(Debug)]
struct ReqIdGenerator {
    current_id: AtomicU64,
}

fn expire_idle_publishers(
    publishers: &mut HashMap<(Topic, Partition, Option<Group>), CachedPublisher>,
    idle_timeout_ms: Option<u64>,
) {
    let Some(idle_timeout_ms) = idle_timeout_ms else {
        return;
    };
    let now = unix_millis();
    publishers.retain(|_, publisher| now.saturating_sub(publisher.last_used_ms) < idle_timeout_ms);
}

impl ReqIdGenerator {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            current_id: AtomicU64::from(0),
        })
    }

    pub fn next_id(&self) -> u64 {
        self.current_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

pub async fn run_server(
    addr: SocketAddr,
    tls: Option<TlsAcceptor>,
    broker: Arc<Broker<StromaEngine>>,
    tcp_stats: Arc<TcpStats>,
    connection_stats: Arc<ConnectionStats>,
    auth: Option<impl AuthHandler + Send + Sync + Clone + 'static>,
    connection_settings: ConnectionSettings,
    topology_source: Option<Arc<dyn ClientTopologySource>>,
    declare_coordinator: Option<Arc<dyn DeclareCoordinator>>,
    topology_adoption: Option<Arc<TopologyAdoptionTracker>>,
) -> Result<(), ProtocolServerError> {
    let listener = TcpListener::bind(addr)
        .await
        .map_err(|source| ProtocolServerError::Bind { addr, source })?;
    print_banner(&addr, tls.is_some());

    loop {
        let (socket, peer) = listener
            .accept()
            .await
            .map_err(ProtocolServerError::Accept)?;
        socket
            .set_nodelay(true)
            .map_err(|source| ProtocolServerError::ConfigureSocket { peer, source })?;
        tcp_stats.connection_opened();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        let broker = broker.clone();

        let auth = auth.clone();
        let tls = tls.clone();
        let tcp_stats = tcp_stats.clone();
        let connection_stats = connection_stats.clone();
        let connection_settings = connection_settings.clone();
        let topology_source = topology_source.clone();
        let declare_coordinator = declare_coordinator.clone();
        let topology_adoption = topology_adoption.clone();
        tokio::spawn(async move {
            tracing::info!("Connection {conn_id} opening..");
            if let Err(e) = serve_connection(
                socket,
                tls,
                peer,
                broker,
                tcp_stats.clone(),
                connection_stats.clone(),
                conn_id,
                auth,
                connection_settings,
                topology_source,
                declare_coordinator,
                topology_adoption.clone(),
            )
            .await
            {
                tracing::error!("conn {} error: {:?}", peer, e);
            }

            connection_stats.remove_connection(&conn_id);
            // Drop this connection's topology-adoption state so a gone client
            // never holds the cutover-fencing minimum down.
            if let Some(adoption) = topology_adoption.as_ref() {
                adoption.remove(&conn_id);
            }

            tracing::info!("Connection {conn_id} closed..");
        });
    }
}

/// Bounds how long an accepted socket may sit silent before its first bytes.
/// A connection that sends nothing within this window is dropped.
const TRANSPORT_SNIFF_TIMEOUT: tokio::time::Duration = tokio::time::Duration::from_secs(10);

/// Sniff a fresh connection's first two bytes to pick the transport and name
/// a TLS/plaintext mismatch in either direction, then run the protocol over
/// the matching stream. Without the sniff, both mismatch directions fail
/// unusably: the plaintext codec reads a ClientHello's leading bytes as a
/// huge frame length and waits forever, and a TLS acceptor turns a plaintext
/// frame into an opaque handshake error.
#[allow(clippy::too_many_arguments)]
async fn serve_connection(
    mut socket: TcpStream,
    tls: Option<TlsAcceptor>,
    peer: SocketAddr,
    broker: Arc<Broker<StromaEngine>>,
    tcp_stats: Arc<TcpStats>,
    connection_stats: Arc<ConnectionStats>,
    conn_id: Uuid,
    auth: Option<impl AuthHandler + Send + Sync + 'static>,
    connection_settings: ConnectionSettings,
    topology_source: Option<Arc<dyn ClientTopologySource>>,
    declare_coordinator: Option<Arc<dyn DeclareCoordinator>>,
    topology_adoption: Option<Arc<TopologyAdoptionTracker>>,
) -> Result<(), ProtocolConnectionError> {
    // Both transports send well over a frame header's worth of bytes in
    // their first flight (a plaintext frame header is exactly 20 bytes and a
    // ClientHello is far larger), so sniffing the full header is safe and
    // lets the TLS-required reply echo the HELLO's request id.
    let sniffed = tokio::time::timeout(
        TRANSPORT_SNIFF_TIMEOUT,
        sniff_first_bytes::<_, 20>(&mut socket),
    )
    .await;
    let (len, prefix) = match sniffed {
        Ok(Ok(pair)) => pair,
        Ok(Err(source)) => return Err(ProtocolConnectionError::Io(source)),
        Err(_) => {
            tracing::debug!("conn {peer} sent no bytes within the sniff window, closing");
            return Ok(());
        }
    };
    if len < 2 {
        tracing::debug!("conn {peer} closed before sending a transport prefix");
        return Ok(());
    }
    let prefix = &prefix[..len];
    let mut stream = PrefixedStream::new(prefix.to_vec(), socket);

    match tls {
        Some(acceptor) => {
            if !looks_like_tls_client_hello(prefix) {
                if looks_like_plaintext_frame(prefix) {
                    tracing::warn!(
                        "rejected plaintext connection from {peer}: this broker listener \
                         requires TLS. Fix on the client: enable TLS and trust the broker \
                         certificate (a CA file or fingerprint pin for self-signed \
                         material). If TLS was enabled on the broker by mistake, set \
                         tls.enabled = false and restart. See \
                         https://fibril.sh/configuration/"
                    );
                    send_tls_required_error(&mut stream, prefix).await;
                } else {
                    tracing::warn!(
                        "conn {peer} sent neither a TLS handshake nor a protocol frame, closing"
                    );
                }
                return Ok(());
            }
            let tls_stream = acceptor
                .accept(stream)
                .await
                .map_err(ProtocolConnectionError::TlsHandshake)?;
            let verified_identity = tls_stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certs| certs.first())
                .and_then(|cert| fibril_util::client_identity_from_der(cert.as_ref()));
            handle_connection(
                tls_stream,
                Some(peer),
                broker,
                tcp_stats,
                connection_stats,
                conn_id,
                auth,
                verified_identity,
                connection_settings,
                topology_source,
                declare_coordinator,
                topology_adoption,
            )
            .await
        }
        None => {
            if looks_like_tls_client_hello(prefix) {
                tracing::warn!(
                    "rejected TLS connection attempt from {peer}: this broker listener is \
                     plaintext. Fix on the broker: set tls.enabled = true with \
                     tls.cert_path and tls.key_path, or tls.auto_self_signed = true. Or \
                     disable TLS in the client if plaintext is intended. See \
                     https://fibril.sh/configuration/"
                );
                return Ok(());
            }
            handle_connection(
                stream,
                Some(peer),
                broker,
                tcp_stats,
                connection_stats,
                conn_id,
                auth,
                None,
                connection_settings,
                topology_source,
                declare_coordinator,
                topology_adoption,
            )
            .await
        }
    }
}

/// Best-effort plaintext `ERR_TLS_REQUIRED` reply on a TLS listener that
/// sniffed a plaintext frame. Echoing the sniffed header's request id routes
/// the error through the client's pending HELLO, so even clients without
/// TLS-aware error handling report a readable cause instead of a bare
/// disconnect.
async fn send_tls_required_error<S>(stream: &mut S, sniffed_header: &[u8])
where
    S: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;
    use tokio_util::codec::Encoder;

    let request_id = match sniffed_header.get(12..20) {
        Some(bytes) => u64::from_be_bytes(bytes.try_into().unwrap_or_default()),
        None => 0,
    };
    let Ok(frame) = error_frame(
        request_id,
        crate::v1::ERR_TLS_REQUIRED,
        "this broker requires TLS: enable TLS in the client and trust the broker \
         certificate (a CA file or fingerprint pin for self-signed material)",
    ) else {
        return;
    };
    let mut buf = bytes::BytesMut::new();
    if ProtoCodec.encode(frame, &mut buf).is_ok() {
        let _ = stream.write_all(&buf).await;
        let _ = stream.shutdown().await;
    }
}

pub enum LoopEvent {
    Heartbeat,
    Frame(Frame),
    Disconnect,
    Timeout,
    /// Time to check whether the coordination generation changed and, if so, push
    /// a `TopologyUpdate` to this connection.
    TopologyTick,
}

#[allow(clippy::too_many_arguments)]
pub async fn handle_connection<S>(
    socket: S,
    peer_addr: Option<SocketAddr>,
    broker: Arc<Broker<StromaEngine>>,
    tcp_stats: Arc<TcpStats>,
    connection_stats: Arc<ConnectionStats>,
    conn_id: Uuid,
    auth_handler: Option<impl AuthHandler + Send + Sync>,
    verified_identity: Option<String>,
    connection_settings: ConnectionSettings,
    topology_source: Option<Arc<dyn ClientTopologySource>>,
    declare_coordinator: Option<Arc<dyn DeclareCoordinator>>,
    topology_adoption: Option<Arc<TopologyAdoptionTracker>>,
) -> Result<(), ProtocolConnectionError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let heartbeat_interval = connection_settings
        .heartbeat_interval
        .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);
    let timeout = tokio::time::Duration::from_secs(heartbeat_interval * 3);
    // Cloned once so the per-publish bump below is a bare atomic increment,
    // never a map lookup on the hot path. Feeds the connections admin view.
    let conn_published = connection_stats.publish_counter(&conn_id);
    // TODO: Consider sharing it between connections?
    let req_id_gen = ReqIdGenerator::new();

    let mut last_seen = Instant::now();
    let mut heartbeat = tokio::time::interval(tokio::time::Duration::from_secs(heartbeat_interval));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Live topology push: poll the coordination generation on a short tick and
    // push a TopologyUpdate only when it changes (so the wire only carries
    // deltas). Seed from the current generation so the initial snapshot is left
    // to the client's TopologyRequest, not duplicated here.
    let mut topology_tick = tokio::time::interval(tokio::time::Duration::from_secs(1));
    topology_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // The last routing topology pushed to this connection, used to push only on a
    // real content change. The coordination generation churns every heartbeat (the
    // liveness timestamp is committed state), so triggering on it would push an
    // identical topology to every client each heartbeat. Seeded with the current
    // topology so the initial snapshot - already delivered via TopologyRequest - is
    // not re-pushed.
    let mut last_pushed_topology = topology_source.as_ref().map(|s| s.topology());
    heartbeat.tick().await; // consume first tick
    // ---- Framed socket -----------------------------------------------------
    let framed = Framed::with_capacity(socket, ProtoCodec, 128 * 1024);
    let (mut writer, mut reader) = framed.split();

    // ---- Write fan-in channel ---------------------------------------------
    let (frame_tx_high_prio, mut frame_rx_high_prio) = mpsc::channel::<Frame>(2048);
    let (frame_tx_low_prio, mut frame_rx_low_prio) = mpsc::channel::<Frame>(16);

    let metrics_clone = tcp_stats.clone();
    // ---- Writer task -------------------------------------------------------
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let req_id_gen_clone = req_id_gen.clone();
    let client_id: Uuid;

    // ---- HELLO handshake ---------------------------------------------------
    let frame = reader
        .next()
        .await
        .ok_or(ProtocolConnectionError::ClosedBeforeHello)??;

    if frame.opcode != Op::Hello as u16 {
        writer
            .send(error_frame(
                frame.request_id,
                400,
                "expected HELLO as the first frame. This is usually a non-Fibril client or a \
                 connection to the wrong port - the broker speaks the Fibril wire protocol \
                 (default port 9876), while the admin API and dashboard use a separate HTTP \
                 port (default 8081)",
            )?)
            .await
            .ok();
        tcp_stats.error();
        return Ok(());
    }

    let hello: Hello = match try_decode(&frame) {
        Ok(hello) => hello,
        Err(err) => {
            writer
                .send(error_frame(
                    frame.request_id,
                    400,
                    format!("malformed HELLO: {err}"),
                )?)
                .await
                .ok();
            tcp_stats.error();
            return Ok(());
        }
    };

    if hello.protocol_version != PROTOCOL_V1 {
        writer
            .send(try_encode(
                Op::HelloErr,
                frame.request_id,
                &ErrorMsg {
                    code: 1,
                    message: "unsupported protocol version".into(),
                },
            )?)
            .await
            .ok();
        tcp_stats.error();
        return Ok(());
    }

    let resume = connection_settings
        .resume_sessions
        .resolve(hello.resume.clone());
    match resume.outcome {
        ResumeOutcome::New => tcp_stats.resume_new(),
        ResumeOutcome::Resumed => tcp_stats.resume_accepted(),
        ResumeOutcome::ResumeNotFound | ResumeOutcome::ResumeRejected => {
            tcp_stats.resume_rejected()
        }
    }
    let logical = resume.logical.clone();
    let transport_generation = logical.attach_transport(frame_tx_high_prio.clone());
    client_id = resume.client_id;
    // A TLS-verified certificate identity that the auth handler maps to a
    // user authenticates the connection with no AUTH frame. A denial is not
    // an error: the connection stays unauthenticated and may still
    // password-auth (the certificate was only a transport pass).
    if let (Some(identity), Some(handler)) = (&verified_identity, &auth_handler)
        && matches!(
            handler.decide_certificate(identity).await,
            fibril_util::AuthDecision::Allow
        )
    {
        logical.state.lock().await.authenticated = true;
        connection_stats.set_connection_auth(&conn_id, true);
        tracing::info!("conn {conn_id} authenticated as `{identity}` by client certificate");
    }
    let hello_ok = HelloOk {
        protocol_version: PROTOCOL_V1,
        owner_id: connection_settings.resume_sessions.owner_id,
        client_id,
        resume_token: resume.resume_token,
        resume_outcome: resume.outcome,
        server_name: "rust-broker".into(),
        compliance: "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md".to_owned(),
    };

    // TODO: Two ways handshake?
    if hello_ok.compliance != COMPLIANCE_STRING {
        tracing::warn!(
            id = "NF-SOVEREIGN-2025-GN-OPT-OUT-TDM",
            expected = COMPLIANCE_STRING,
            got = %hello_ok.compliance,
            "Invariant violated: compliance marker altered or missing"
        );
        return Err(ProtocolConnectionError::ComplianceMarkerMismatch);
    }

    let mut writer_task = tokio::spawn(async move {
        tracing::debug!("[writer] START");

        let metrics = metrics_clone.clone();
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(2));
        ticker.tick().await;

        let mut non_flushed_messages: usize = 0;
        let mut last_flush = Instant::now();
        let mut bytes_queued: usize = 0;
        loop {
            tokio::select! {
                biased;

                // ---- Shutdown signal -----------------------------------------
                _ = &mut shutdown_rx => {
                    tracing::debug!("[writer] Received shutdown signal");
                    // Drain frames already queued and flush before closing:
                    // the final frame is often an error reply (auth denial,
                    // rejected HELLO) and losing it to this race would turn
                    // a guided failure into a bare disconnect.
                    while let Ok(frame) = frame_rx_high_prio.try_recv() {
                        if writer.feed(frame).await.is_err() {
                            break;
                        }
                    }
                    let _ = writer.flush().await;
                    break;
                }

                // ---- Normal write path ---------------------------------------
                Some(frame) = frame_rx_high_prio.recv() => {
                    tracing::debug!(
                        "[writer] Writing Frame to tcp socket.. code={}",
                        frame.opcode
                    );

                    let size = size_of_val(&frame) + frame.payload.len();

                    if let Err(err) = writer.feed(frame).await {
                        metrics.error();
                        tracing::warn!("[writer] Error writing to tcp socket : {err}");
                        break;
                    } else {
                        metrics.bytes_out(size as u64);
                        non_flushed_messages += 1;
                        bytes_queued += size;

                        if non_flushed_messages >= 32 {
                            let _ = writer.flush().await;
                            non_flushed_messages = 0;
                            last_flush = Instant::now();
                            bytes_queued = 0;
                        }
                    }
                }

                Some(frame) = frame_rx_low_prio.recv() => {
                    tracing::debug!(
                        "[writer] Writing Frame to tcp socket.. code={}",
                        frame.opcode
                    );

                    let size = size_of_val(&frame) + frame.payload.len();

                    if let Err(err) = writer.feed(frame).await {
                        metrics.error();
                        tracing::error!("[writer] Error writing to tcp socket : {err}");
                        break;
                    } else {
                        metrics.bytes_out(size as u64);
                        non_flushed_messages += 1;
                        bytes_queued += size;
                    }
                }

                // The tick only exists to flush a sub-window tail of buffered
                // frames, so it stays disabled while nothing is buffered and an
                // idle connection costs no periodic wakeups. When the arm
                // re-enables after a quiet stretch the first tick fires
                // immediately, which just runs the flush check below.
                _ = ticker.tick(), if non_flushed_messages > 0 => {
                    // pass
                }

                // ---- Channel closed ------------------------------------------
                else => break,
            }

            // Basic batching logic, limited by message number, time or total bytes to send
            if (non_flushed_messages > 0)
                && (non_flushed_messages >= 128
                    || bytes_queued >= 1024 * 1024
                    || last_flush.elapsed().as_millis() >= 5)
            {
                if let Err(err) = writer.flush().await {
                    metrics.error();
                    tracing::warn!("[writer] Error writing to tcp socket : {err}");
                    break;
                } else {
                    non_flushed_messages = 0;
                    last_flush = Instant::now();
                    bytes_queued = 0;
                }
            }
        }

        tracing::debug!("[writer] EXIT");
    });

    frame_tx_high_prio
        .send(try_encode(Op::HelloOk, frame.request_id, &hello_ok)?)
        .await?;

    let (pub_tx, mut pub_rx) = tokio::sync::mpsc::channel::<(
        Result<oneshot::Receiver<Result<u64, BrokerError>>, fibril_broker::broker::BrokerError>,
        bool,
        u64,
    )>(8192);

    let metrics_pub = tcp_stats.clone();
    let frame_tx_pub = frame_tx_low_prio.clone();

    let pub_queue_handle = tokio::spawn(async move {
        while let Some((published, require_confirm, request_id)) = pub_rx.recv().await {
            match published {
                Ok(offset_rx) => {
                    let offset = match offset_rx.await {
                        Ok(Ok(offset)) => offset,
                        Ok(Err(err)) => {
                            if require_confirm {
                                let (code, message) = broker_error_response(&err);
                                let send_res =
                                    send_error_response(&frame_tx_pub, request_id, code, message)
                                        .await;

                                if let Err(err) = send_res {
                                    tracing::error!("Error sending Publish Confirm: {err}");
                                }
                                metrics_pub.error();
                            }
                            continue;
                        }
                        Err(_err) => {
                            if require_confirm {
                                let send_res = send_error_response(
                                    &frame_tx_pub,
                                    request_id,
                                    500,
                                    "Channel closed",
                                )
                                .await;

                                if let Err(err) = send_res {
                                    tracing::error!("Error sending Publish Confirm: {err}");
                                }
                                metrics_pub.error();
                            }
                            continue;
                        }
                    };
                    if require_confirm {
                        let send_res =
                            match wire::encode_publish_ok(request_id, &PublishOk { offset }) {
                                Ok(frame) => frame_tx_pub
                                    .send(frame)
                                    .await
                                    .map_err(ProtocolConnectionError::from),
                                Err(err) => Err(ProtocolConnectionError::from(err)),
                            };

                        if let Err(err) = send_res {
                            tracing::error!("Error sending Publish Confirm: {err}");
                        }
                    }
                }
                Err(err) => {
                    let (code, message) = broker_error_response(&err);
                    let send_res =
                        send_error_response(&frame_tx_pub, request_id, code, message).await;

                    if let Err(err) = send_res {
                        tracing::error!("Error sending Publish Confirm: {err}");
                    }
                    metrics_pub.error();
                }
            }
        }
    });

    let mut publishers = HashMap::<(Topic, Partition, Option<Group>), CachedPublisher>::new();

    // ---- Main reader loop --------------------------------------------------
    // TODO: Make handling more async? Spawn task per frame, or have a task pool
    macro_rules! decode_or_400 {
        ($frame:expr, $tx:expr, $metrics:expr, $ty:ty) => {
            match try_decode::<$ty>(&$frame) {
                Ok(value) => value,
                Err(err) => {
                    send_error_response(
                        &$tx,
                        $frame.request_id,
                        400,
                        format!("malformed frame: {err}"),
                    )
                    .await?;
                    $metrics.error();
                    continue;
                }
            }
        };
    }

    macro_rules! decode_wire_or_400 {
        ($frame:expr, $tx:expr, $metrics:expr, $decoder:path) => {
            match $decoder(&$frame) {
                Ok(value) => value,
                Err(err) => {
                    send_error_response(
                        &$tx,
                        $frame.request_id,
                        400,
                        format!("malformed frame: {err}"),
                    )
                    .await?;
                    $metrics.error();
                    continue;
                }
            }
        };
    }

    // Owner-side replication streams opened by followers on this connection,
    // keyed by stream id (the frame request_id). Dropping a control sender (on
    // Stop or when this map drops at connection end) makes its sender task exit.
    let mut owner_streams: HashMap<u64, mpsc::Sender<replication_stream::OwnerStreamControl>> =
        HashMap::new();

    loop {
        let loop_event = tokio::select! {
            // ---- Heartbeat tick ----
            _ = heartbeat.tick() => {
                if last_seen.elapsed() > timeout {
                    tracing::warn!("Heartbeat timeout, closing connection");
                    LoopEvent::Timeout
                }
                else if frame_tx_high_prio.send(try_encode(Op::Ping, req_id_gen_clone.next_id(), &())?).await.is_err() {
                    // channel full or closed -> treat as disconnect
                    tracing::error!("Failed to send heartbeat ping, closing connection");
                    LoopEvent::Timeout
                }
                else {
                    LoopEvent::Heartbeat
                }
            }

            // ---- Topology push tick ----
            _ = topology_tick.tick() => LoopEvent::TopologyTick,

            // ---- Incoming frame ----
            frame = reader.next() => {
                match frame {
                    Some(Ok(f)) => {
                        last_seen = Instant::now();

                        LoopEvent::Frame(f)
                    },
                    Some(Err(err)) => {
                        tracing::warn!("[reader] Error reading from tcp socket: {err}");
                        LoopEvent::Disconnect
                    },
                    None => {
                        tracing::info!("[reader] Disconnected from tcp socket");
                        LoopEvent::Disconnect
                    },
                }
            }
        };

        let frame = match loop_event {
            LoopEvent::Frame(f) => f,
            LoopEvent::Timeout => break,
            LoopEvent::Disconnect => break,
            LoopEvent::Heartbeat => {
                let settings = connection_settings.runtime_snapshot();
                expire_idle_publishers(&mut publishers, settings.publisher_cache_idle_timeout_ms);
                continue;
            }
            LoopEvent::TopologyTick => {
                // Push the topology only when the routing content changed (owners,
                // partition counts, or partitioning versions) and the connection is
                // allowed to receive (authenticated when auth is required). The
                // pushed frame still carries the live coordination generation, which
                // the client acks - so a connection's acked generation advances to
                // the cutover generation on the post-cutover push and the adoption
                // gate sees it. The client refreshes its cache and acks.
                let authed = auth_handler.is_none() || logical.state.lock().await.authenticated;
                if authed {
                    if let Some(source) = topology_source.as_ref() {
                        let topo = source.topology();
                        let changed = match &last_pushed_topology {
                            Some(prev) => {
                                prev.queues != topo.queues || prev.streams != topo.streams
                            }
                            None => true,
                        };
                        if changed {
                            match wire::encode_topology_update(req_id_gen_clone.next_id(), &topo) {
                                Ok(f) => {
                                    if frame_tx_high_prio.send(f).await.is_ok() {
                                        last_pushed_topology = Some(topo);
                                    } else {
                                        break; // send failed -> connection gone
                                    }
                                }
                                Err(err) => {
                                    tracing::warn!("encode topology update failed: {err}");
                                }
                            }
                        }
                    }
                }
                continue;
            }
        };

        let settings = connection_settings.runtime_snapshot();
        expire_idle_publishers(&mut publishers, settings.publisher_cache_idle_timeout_ms);

        let metrics = tcp_stats.clone();
        let size = size_of_val(&frame) + frame.payload.len();
        metrics.bytes_in(size as u64);

        let auth_required = auth_handler.is_some();
        let is_auth = frame.opcode == Op::Auth as u16;
        let is_ping = frame.opcode == Op::Ping as u16;
        let is_pong = frame.opcode == Op::Pong as u16;

        let authenticated = logical.state.lock().await.authenticated;
        if auth_required && !authenticated && !(is_auth || is_ping || is_pong) {
            send_error_response(
                &frame_tx_high_prio,
                frame.request_id,
                401,
                "authentication required",
            )
            .await?;
            metrics.error();

            break; // close connection
        }

        match frame.opcode {
            // -------- AUTH ---------------------------------------------------
            x if x == Op::Auth as u16 => {
                if logical.state.lock().await.authenticated {
                    frame_tx_high_prio
                        .send(try_encode(
                            Op::AuthErr,
                            frame.request_id,
                            &ErrorMsg {
                                code: 401,
                                message: "already authenticated".into(),
                            },
                        )?)
                        .await?;
                    metrics.error();
                } else if auth_handler.is_none() {
                    frame_tx_high_prio
                        .send(try_encode(
                            Op::AuthErr,
                            frame.request_id,
                            &ErrorMsg {
                                code: 400,
                                message: "authentication not applicable".into(),
                            },
                        )?)
                        .await?;
                    metrics.error();
                } else {
                    // ---- AUTH handshake -------------------------------------
                    if let Some(auth_handler) = &auth_handler {
                        let auth_frame: Auth =
                            decode_or_400!(frame, frame_tx_high_prio, metrics, Auth);

                        let decision = auth_handler
                            .decide(
                                &auth_frame.username,
                                &auth_frame.password,
                                peer_addr.map(|peer| peer.ip()),
                            )
                            .await;

                        match decision {
                            fibril_util::AuthDecision::Allow => {
                                logical.state.lock().await.authenticated = true;
                                frame_tx_high_prio
                                    .send(try_encode(Op::AuthOk, frame.request_id, &())?)
                                    .await?;
                                connection_stats.set_connection_auth(&conn_id, true);
                            }
                            fibril_util::AuthDecision::Deny { message } => {
                                frame_tx_high_prio
                                    .send(try_encode(
                                        Op::AuthErr,
                                        frame.request_id,
                                        &ErrorMsg { code: 401, message },
                                    )?)
                                    .await?;
                                metrics.error();

                                break; // close connection
                            }
                        }
                    }
                }
            }

            // -------- RECONCILE ---------------------------------------------
            x if x == Op::ReconcileClient as u16 => {
                let reconcile: ReconcileClient =
                    decode_or_400!(frame, frame_tx_high_prio, metrics, ReconcileClient);
                let result = reconcile_subscriptions(
                    broker.clone(),
                    logical.clone(),
                    connection_stats.clone(),
                    metrics.clone(),
                    req_id_gen.clone(),
                    conn_id,
                    client_id,
                    reconcile,
                )
                .await;
                frame_tx_high_prio
                    .send(try_encode(Op::ReconcileResult, frame.request_id, &result)?)
                    .await?;
            }

            // -------- REPLICATION READ --------------------------------------
            x if x == Op::ReplicationRead as u16 => {
                let read: ReplicationRead =
                    decode_or_400!(frame, frame_tx_high_prio, metrics, ReplicationRead);

                // A stamped read doubles as the follower's durable-progress
                // report (followers apply durably; pull offsets = watermarks).
                if let Some(reporter) = &read.reporter_node_id {
                    broker.record_follower_replication_progress(
                        &read.topic,
                        read.partition,
                        read.group.as_deref(),
                        reporter,
                        read.message_from,
                        read.event_from,
                    );
                }

                match broker
                    .read_owner_replication_records(
                        &read.topic,
                        read.partition,
                        read.group.as_deref(),
                        read.message_from,
                        read.event_from,
                        read.max_messages as usize,
                        read.max_events as usize,
                        usize::try_from(read.max_bytes).unwrap_or(usize::MAX),
                        read.max_wait_ms as u64,
                    )
                    .await
                {
                    Ok(records) => {
                        let response = match to_replication_read_ok(records) {
                            Ok(response) => response,
                            Err(err) => {
                                tracing::error!(
                                    error = %err,
                                    "failed to encode replication read response"
                                );
                                send_error_response_and_count(
                                    &frame_tx_high_prio,
                                    &metrics,
                                    frame.request_id,
                                    500,
                                    "replication read encoding failed",
                                )
                                .await;
                                continue;
                            }
                        };
                        frame_tx_high_prio
                            .send(wire::encode_replication_read_ok(
                                frame.request_id,
                                &response,
                            )?)
                            .await?;
                    }
                    Err(err) => {
                        let (code, message) = broker_error_response(&err);
                        send_error_response_and_count(
                            &frame_tx_high_prio,
                            &metrics,
                            frame.request_id,
                            code,
                            message,
                        )
                        .await;
                    }
                }
            }

            // -------- STREAM REPLICATION READ (Plexus follower pull) -------
            x if x == Op::StreamReplicationRead as u16 => {
                let read: ReplicationRead = decode_wire_or_400!(
                    frame,
                    frame_tx_high_prio,
                    metrics,
                    wire::decode_stream_replication_read
                );

                // A stamped read doubles as the stream follower's durable-progress
                // report (group None), feeding the owner's replica-durable confirm
                // gate exactly as the queue ReplicationRead path does.
                if let Some(reporter) = &read.reporter_node_id {
                    broker.record_follower_replication_progress(
                        &read.topic,
                        read.partition,
                        None,
                        reporter,
                        read.message_from,
                        read.event_from,
                    );
                }

                // The owner serves a stream's record + cursor-commit logs (group
                // None) via the same role-gated owner read as queues; the handle's
                // Owner role is the authority, so a non-owner read returns
                // NotOwner. read.event_from is the cursor-commit-event offset.
                match broker
                    .read_owner_replication_records(
                        &read.topic,
                        read.partition,
                        None,
                        read.message_from,
                        read.event_from,
                        read.max_messages as usize,
                        read.max_events as usize,
                        usize::try_from(read.max_bytes).unwrap_or(usize::MAX),
                        read.max_wait_ms as u64,
                    )
                    .await
                {
                    Ok(records) => match to_replication_read_ok(records) {
                        Ok(response) => {
                            frame_tx_high_prio
                                .send(wire::encode_stream_replication_read_ok(
                                    frame.request_id,
                                    &response,
                                )?)
                                .await?;
                        }
                        Err(err) => {
                            tracing::error!(
                                error = %err,
                                "failed to encode stream replication read response"
                            );
                            send_error_response_and_count(
                                &frame_tx_high_prio,
                                &metrics,
                                frame.request_id,
                                500,
                                "stream replication read encoding failed",
                            )
                            .await;
                        }
                    },
                    Err(err) => {
                        let (code, message) = broker_error_response(&err);
                        send_error_response_and_count(
                            &frame_tx_high_prio,
                            &metrics,
                            frame.request_id,
                            code,
                            message,
                        )
                        .await;
                    }
                }
            }

            // -------- REPLICATION STREAM (credit-based) --------------------
            x if x == Op::ReplicationStreamStart as u16 => {
                let start: ReplicationStreamStart = decode_wire_or_400!(
                    frame,
                    frame_tx_high_prio,
                    metrics,
                    wire::decode_replication_stream_start
                );
                let stream_id = frame.request_id;
                let (control_tx, control_rx) = mpsc::channel(64);
                // Replace any existing stream on this id (drops the old sender,
                // which makes the old task exit).
                owner_streams.insert(stream_id, control_tx);
                let source = Arc::new(BrokerOwnerStreamSource {
                    broker: broker.clone(),
                });
                tokio::spawn(replication_stream::run_owner_replication_stream(
                    source,
                    frame_tx_high_prio.clone(),
                    stream_id,
                    start,
                    control_rx,
                    replication_stream::OwnerStreamConfig::default(),
                ));
            }
            x if x == Op::ReplicationStreamProgress as u16 => {
                let progress: ReplicationStreamProgress = decode_wire_or_400!(
                    frame,
                    frame_tx_high_prio,
                    metrics,
                    wire::decode_replication_stream_progress
                );
                if let Some(control) = owner_streams.get(&frame.request_id) {
                    let _ = control
                        .send(replication_stream::OwnerStreamControl::Progress {
                            durable_message_next: progress.durable_message_next,
                            durable_event_next: progress.durable_event_next,
                            credit_add_bytes: progress.credit_add_bytes,
                        })
                        .await;
                }
            }
            x if x == Op::ReplicationStreamReset as u16 => {
                let reset: ReplicationStreamReset = decode_wire_or_400!(
                    frame,
                    frame_tx_high_prio,
                    metrics,
                    wire::decode_replication_stream_reset
                );
                if let Some(control) = owner_streams.get(&frame.request_id) {
                    let _ = control
                        .send(replication_stream::OwnerStreamControl::Reset {
                            message_from: reset.message_from,
                            event_from: reset.event_from,
                        })
                        .await;
                }
            }
            x if x == Op::ReplicationStreamStop as u16 => {
                // Dropping the control sender makes the sender task exit.
                owner_streams.remove(&frame.request_id);
            }

            // -------- REPLICATION CHECKPOINT EXPORT ------------------------
            x if x == Op::ReplicationCheckpointExport as u16 => {
                let export: ReplicationCheckpointExport = decode_or_400!(
                    frame,
                    frame_tx_high_prio,
                    metrics,
                    ReplicationCheckpointExport
                );

                match broker
                    .export_owner_state_checkpoint(
                        &export.topic,
                        export.partition,
                        export.group.as_deref(),
                    )
                    .await
                {
                    Ok(checkpoint) => {
                        let response = ReplicationCheckpointExportOk {
                            checkpoint: to_replication_state_checkpoint(checkpoint),
                        };
                        frame_tx_high_prio
                            .send(try_encode(
                                Op::ReplicationCheckpointExportOk,
                                frame.request_id,
                                &response,
                            )?)
                            .await?;
                    }
                    Err(err) => {
                        let (code, message) = broker_error_response(&err);
                        send_error_response_and_count(
                            &frame_tx_high_prio,
                            &metrics,
                            frame.request_id,
                            code,
                            message,
                        )
                        .await;
                    }
                }
            }

            // -------- REPLICATION CHECKPOINT INSTALL -----------------------
            x if x == Op::ReplicationCheckpointInstall as u16 => {
                let install: ReplicationCheckpointInstall = decode_or_400!(
                    frame,
                    frame_tx_high_prio,
                    metrics,
                    ReplicationCheckpointInstall
                );
                let topic = install.topic.clone();
                let group = install.group.clone();
                let partition = install.partition;
                let checkpoint = to_follower_state_checkpoint_install(install.checkpoint);

                match broker
                    .install_follower_state_checkpoint(
                        &topic,
                        partition,
                        group.as_deref(),
                        checkpoint,
                    )
                    .await
                {
                    Ok(outcome) => {
                        let response = ReplicationCheckpointInstallOk {
                            message_next_offset: outcome.message_next_offset,
                            event_next_offset: outcome.event_next_offset,
                            applied_event_offset: outcome.applied_event_offset,
                        };
                        frame_tx_high_prio
                            .send(try_encode(
                                Op::ReplicationCheckpointInstallOk,
                                frame.request_id,
                                &response,
                            )?)
                            .await?;
                    }
                    Err(err) => {
                        let (code, message) = broker_error_response(&err);
                        send_error_response_and_count(
                            &frame_tx_high_prio,
                            &metrics,
                            frame.request_id,
                            code,
                            message,
                        )
                        .await;
                    }
                }
            }

            // -------- REPLICATION APPLY -------------------------------------
            x if x == Op::ReplicationApply as u16 => {
                let apply: ReplicationApply =
                    decode_or_400!(frame, frame_tx_high_prio, metrics, ReplicationApply);
                let topic = apply.topic.clone();
                let group = apply.group.clone();
                let partition = apply.partition;
                let records = match to_owner_replication_records(apply) {
                    Ok(records) => records,
                    Err(message) => {
                        send_error_response_and_count(
                            &frame_tx_high_prio,
                            &metrics,
                            frame.request_id,
                            400,
                            message,
                        )
                        .await;
                        continue;
                    }
                };

                match broker
                    .apply_follower_replication_records(
                        &topic,
                        partition,
                        group.as_deref(),
                        ReplicationResourceKind::Queue,
                        records,
                    )
                    .await
                {
                    Ok(BrokerFollowerReplicationApply::Applied(outcome)) => {
                        let response = to_replication_apply_ok(
                            outcome.message_log.is_some(),
                            outcome.event_log.is_some(),
                        );
                        frame_tx_high_prio
                            .send(try_encode(
                                Op::ReplicationApplyOk,
                                frame.request_id,
                                &response,
                            )?)
                            .await?;
                    }
                    Ok(BrokerFollowerReplicationApply::CheckpointRequired { .. }) => {
                        tracing::error!(
                            topic,
                            partition = partition.id(),
                            group,
                            "plain replication apply unexpectedly required a checkpoint"
                        );
                        send_error_response_and_count(
                            &frame_tx_high_prio,
                            &metrics,
                            frame.request_id,
                            500,
                            "replication apply unexpectedly required checkpoint",
                        )
                        .await;
                    }
                    Err(err) => {
                        let (code, message) = broker_error_response(&err);
                        send_error_response_and_count(
                            &frame_tx_high_prio,
                            &metrics,
                            frame.request_id,
                            code,
                            message,
                        )
                        .await;
                    }
                }
            }

            // -------- SUBSCRIBE ---------------------------------------------
            x if x == Op::Subscribe as u16 => {
                let sub: Subscribe = decode_or_400!(frame, frame_tx_high_prio, metrics, Subscribe);

                // A queue subscribe to a stream partition has no queue consumer to
                // attach. Reject it at the routing boundary (use SubscribeStream)
                // rather than installing a consumer that would never be fed.
                if broker.is_stream(&sub.topic, sub.partition.id())
                    || broker
                        .engine()
                        .durable_is_stream(&sub.topic, sub.partition.id())
                {
                    frame_tx_high_prio
                        .send(try_encode(
                            Op::SubscribeErr,
                            frame.request_id,
                            &ErrorMsg {
                                code: ERR_INVALID,
                                message: format!(
                                    "{} is a plexus stream, use a stream subscribe",
                                    sub.topic
                                ),
                            },
                        )?)
                        .await?;
                    metrics.error();
                    continue;
                }

                // Captured for a possible redirect (the identity is moved into
                // the install args below).
                let sub_topic = sub.topic.clone();
                let sub_group = sub.group.clone();
                let sub_partition = sub.partition;

                let sub_ok = install_subscription(InstallSubscriptionArgs {
                    broker: broker.clone(),
                    logical: logical.clone(),
                    connection_stats: connection_stats.clone(),
                    metrics: metrics.clone(),
                    conn_id,
                    client_id,
                    req_id_gen: req_id_gen.clone(),
                    topic: sub.topic,
                    partition: sub.partition,
                    group: sub.group,
                    prefetch: sub.prefetch,
                    auto_ack: sub.auto_ack,
                    consumer_group: sub.consumer_group,
                    consumer_target: sub.consumer_target.map(|target| target as usize),
                    member_id: sub.member_id,
                })
                .await;

                let sub_ok = match sub_ok {
                    Ok(sub_ok) => sub_ok,
                    Err(err) => {
                        // Not-owner on subscribe redirects to the current owner
                        // (when resolvable), just like publish.
                        if matches!(
                            &err,
                            InstallSubscriptionError::Broker(BrokerError::NotOwner { .. })
                        ) {
                            if let Some(redirect) = owner_redirect_frame(
                                &topology_source,
                                frame.request_id,
                                &sub_topic,
                                sub_partition,
                                sub_group.as_deref(),
                            ) {
                                frame_tx_high_prio.send(redirect).await?;
                                continue;
                            }
                        }
                        let (code, message) = install_subscription_error_response(&err);
                        frame_tx_high_prio
                            .send(try_encode(
                                Op::SubscribeErr,
                                frame.request_id,
                                &ErrorMsg { code, message },
                            )?)
                            .await?;
                        metrics.error();
                        continue;
                    }
                };

                frame_tx_high_prio
                    .send(try_encode(Op::SubscribeOk, frame.request_id, &sub_ok)?)
                    .await?;
            }

            // -------- TOPOLOGY ----------------------------------------------
            x if x == Op::Topology as u16 => {
                let req: TopologyRequest =
                    decode_or_400!(frame, frame_tx_high_prio, metrics, TopologyRequest);
                // No source (standalone) => empty topology; the client falls
                // back to its direct connection.
                let mut topology = topology_source
                    .as_ref()
                    .map(|source| source.topology())
                    .unwrap_or(TopologyOk {
                        generation: 0,
                        queues: Vec::new(),
                        streams: Vec::new(),
                    });
                if let Some(topic) = &req.topic {
                    topology.queues.retain(|queue| {
                        &queue.topic == topic
                            && req
                                .group
                                .as_deref()
                                .is_none_or(|group| queue.group.as_deref() == Some(group))
                    });
                    // A stream has no group; only filter it out when the request
                    // names a different topic, never on a group mismatch.
                    topology.streams.retain(|stream| &stream.topic == topic);
                }
                frame_tx_high_prio
                    .send(try_encode(Op::TopologyOk, frame.request_id, &topology)?)
                    .await?;
            }

            // -------- TOPOLOGY UPDATE ACK -----------------------------------
            x if x == Op::TopologyUpdateAck as u16 => {
                // The client applied a pushed topology at this generation. Record
                // it per connection so the broker can report the cluster-wide
                // minimum and the repartition controller can fence a cutover until
                // clients have adopted the new routing.
                match wire::decode_topology_update_ack(&frame) {
                    Ok(ack) => {
                        if let Some(adoption) = topology_adoption.as_ref() {
                            adoption.record(conn_id, ack.generation);
                        }
                        tracing::trace!(
                            generation = ack.generation,
                            "client acked topology update"
                        );
                    }
                    Err(err) => {
                        tracing::warn!("bad topology update ack: {err}");
                    }
                }
            }

            // -------- DECLARE QUEUE -----------------------------------------
            x if x == Op::DeclareQueue as u16 => {
                let declare: DeclareQueue =
                    decode_or_400!(frame, frame_tx_high_prio, metrics, DeclareQueue);
                let meta = match to_declare_meta(&declare).await {
                    Ok(meta) => meta,
                    Err(message) => {
                        let _ = send_error_response(
                            &frame_tx_high_prio,
                            frame.request_id,
                            400,
                            &message,
                        )
                        .await;
                        continue;
                    }
                };

                let requested = declare
                    .partition_count
                    .unwrap_or_else(|| broker.config_snapshot().default_partition_count)
                    .max(1);

                // Record partitioning metadata first (cluster mode): the
                // returned count is authoritative — an already-declared queue's
                // count wins, and a conflicting request errors. Standalone uses
                // the requested count directly.
                let partition_count = if let Some(coordinator) = &declare_coordinator {
                    match coordinator
                        .declare_partitioning(&declare.topic, declare.group.as_deref(), requested)
                        .await
                    {
                        Ok(count) => count,
                        Err(message) => {
                            let _ = send_error_response(
                                &frame_tx_high_prio,
                                frame.request_id,
                                ERR_CONFLICT,
                                &message,
                            )
                            .await;
                            continue;
                        }
                    }
                } else {
                    requested
                };

                // Materialize the partitions locally; in cluster mode the
                // controller assigns ownership and catalogue sync registers them.
                let mut failure: Option<(u16, String)> = None;
                for partition in 0..partition_count {
                    match broker
                        .engine()
                        .declare_queue(
                            &declare.topic,
                            partition,
                            declare.group.as_deref(),
                            meta.clone(),
                        )
                        .await
                    {
                        Ok(()) => {}
                        Err(err @ StromaError::InvalidArgument(_)) => {
                            failure = Some((400, err.to_string()));
                            break;
                        }
                        Err(err) => {
                            tracing::error!("Declare queue failed: {err}");
                            failure = Some((500, "declare queue failed".into()));
                            break;
                        }
                    }
                }
                if let Some((code, message)) = failure {
                    let _ =
                        send_error_response(&frame_tx_high_prio, frame.request_id, code, &message)
                            .await;
                    continue;
                }
                frame_tx_high_prio
                    .send(try_encode(
                        Op::DeclareQueueOk,
                        frame.request_id,
                        &DeclareQueueOk {
                            status: "stored".into(),
                            partition_count,
                        },
                    )?)
                    .await?;
            }

            // -------- DECLARE PLEXUS (stream) -------------------------------
            x if x == Op::DeclarePlexus as u16 => {
                let declare: DeclarePlexus =
                    decode_or_400!(frame, frame_tx_high_prio, metrics, DeclarePlexus);

                let requested = declare
                    .partition_count
                    .unwrap_or_else(|| broker.config_snapshot().default_partition_count)
                    .max(1);

                // Record the stream's partitioning in its own namespace and
                // catalogue its partitions for placement (separate from queues).
                let partition_count = if let Some(coordinator) = &declare_coordinator {
                    match coordinator
                        .declare_stream(
                            &declare.topic,
                            requested,
                            declare.durability.as_u8(),
                            declare.retention.clone(),
                            declare.replication_factor,
                        )
                        .await
                    {
                        Ok(count) => count,
                        Err(message) => {
                            let _ = send_error_response(
                                &frame_tx_high_prio,
                                frame.request_id,
                                ERR_CONFLICT,
                                &message,
                            )
                            .await;
                            continue;
                        }
                    }
                } else {
                    requested
                };

                let retention =
                    (declare.retention != StreamRetention::default()).then(|| RetentionConfig {
                        max_age_ms: declare.retention.max_age_ms,
                        max_bytes: declare.retention.max_bytes,
                        max_records: declare.retention.max_records,
                    });
                // Map the wire durability tier to the broker enum (same ordinals).
                let durability =
                    BrokerStreamDurability::from_u8(declare.durability.as_u8()).unwrap_or_default();

                // Materialize each partition's stream channel (idempotent). The
                // durability tier is recorded for observability; the broker
                // persists durable-first for all tiers today (the express lane for
                // ephemeral/speculative is a later refinement).
                let mut failure: Option<(u16, String)> = None;
                for partition in 0..partition_count {
                    match broker
                        .get_or_open_stream(
                            &declare.topic,
                            partition,
                            durability,
                            retention.clone(),
                        )
                        .await
                    {
                        Ok(_) => {}
                        // A kind conflict (the topic is already a queue) is a client
                        // error to fix, not a transient server fault.
                        Err(err @ StromaError::InvalidArgument(_)) => {
                            failure = Some((400, err.to_string()));
                            break;
                        }
                        Err(err) => {
                            let trace =
                                format!("{:08x}", uuid::Uuid::now_v7().as_u128() as u32);
                            tracing::error!("Declare plexus failed [trace {trace}]: {err}");
                            failure = Some((
                                500,
                                format!(
                                    "declare plexus failed: {err}. This is a broker-side \
                                     fault - search the broker logs for trace id {trace}"
                                ),
                            ));
                            break;
                        }
                    }
                }
                if let Some((code, message)) = failure {
                    let _ =
                        send_error_response(&frame_tx_high_prio, frame.request_id, code, &message)
                            .await;
                    continue;
                }
                frame_tx_high_prio
                    .send(try_encode(
                        Op::DeclarePlexusOk,
                        frame.request_id,
                        &DeclarePlexusOk {
                            status: "stored".into(),
                            partition_count,
                        },
                    )?)
                    .await?;
            }

            // -------- SUBSCRIBE STREAM (Plexus) -----------------------------
            x if x == Op::SubscribeStream as u16 => {
                let sub: SubscribeStream =
                    decode_or_400!(frame, frame_tx_high_prio, metrics, SubscribeStream);

                // Redirect a subscribe to a stream partition this node does not
                // own (cluster mode), so the subscriber attaches to the owner's
                // fan-out. Streams have no group.
                if (broker.is_stream(&sub.topic, sub.partition.id())
                    || broker.stream_declared_in_coordination(&sub.topic))
                    && broker
                        .ensure_stream_owner(&sub.topic, sub.partition.id())
                        .is_err()
                {
                    if let Some(frame) = stream_owner_redirect_frame(
                        &topology_source,
                        frame.request_id,
                        &sub.topic,
                        sub.partition,
                    ) {
                        let _ = frame_tx_high_prio.send(frame).await;
                    } else {
                        frame_tx_high_prio
                            .send(try_encode(
                                Op::SubscribeErr,
                                frame.request_id,
                                &ErrorMsg {
                                    code: ERR_NOT_OWNER,
                                    message: "not the owner of this stream partition, and its current owner could not be resolved - the cluster may still be converging (retry shortly), or coordination is unavailable to route you to the owner".into(),
                                },
                            )?)
                            .await?;
                        metrics.error();
                    }
                    continue;
                }

                match install_stream_subscription(&broker, &logical, &req_id_gen, &metrics, sub)
                    .await
                {
                    Ok(sub_ok) => {
                        frame_tx_high_prio
                            .send(try_encode(Op::SubscribeOk, frame.request_id, &sub_ok)?)
                            .await?;
                    }
                    Err((code, message)) => {
                        frame_tx_high_prio
                            .send(try_encode(
                                Op::SubscribeErr,
                                frame.request_id,
                                &ErrorMsg { code, message },
                            )?)
                            .await?;
                        metrics.error();
                    }
                }
            }

            // -------- ACK ----------------------------------------------------
            x if x == Op::Ack as u16 => {
                // TODO: Decline ack when auto ack? Log?
                let ack: Ack =
                    decode_wire_or_400!(frame, frame_tx_high_prio, metrics, wire::decode_ack);

                // One lock scope resolves both routes. Stream ack: the delivery
                // tag is the record offset, so settling commits the durable
                // cursor to the highest acked offset (+1). An ephemeral or
                // auto-ack stream sub treats the ack as a no-op.
                let (stream_settle, queue_settle) = {
                    let state = logical.state.lock().await;
                    if let Some(sub) = state.stream_subs.get(&(ack.topic.clone(), ack.partition)) {
                        (
                            Some((sub.channel.clone(), sub.durable_name.clone(), sub.auto_ack)),
                            None,
                        )
                    } else {
                        let key: SubKey = (ack.topic.clone(), ack.partition, ack.group.clone());
                        (
                            None,
                            state.subs.get(&key).and_then(|sub| {
                                (!sub.auto_ack).then(|| {
                                    (sub.state_settler.clone(), sub.pending_settles.clone())
                                })
                            }),
                        )
                    }
                };
                if let Some((channel, durable_name, auto_ack)) = stream_settle {
                    if !auto_ack {
                        if let (Some(name), Some(max)) =
                            (durable_name, ack.tags.iter().map(|tag| tag.epoch).max())
                        {
                            if let Err(err) = channel.settle(&name, max).await {
                                tracing::warn!("stream ack settle failed: {err}");
                            }
                        }
                    }
                    continue;
                }

                if let Some((state_settler, pending_settles)) = queue_settle {
                    let reqs = ack
                        .tags
                        .into_iter()
                        .map(|tag| SettleRequest {
                            delivery_tag: tag,
                            settle_type: SettleType::Ack,
                        })
                        .collect();
                    send_settle_batch(&state_settler, &pending_settles, reqs).await;
                }
                // Unknown subscription: ignore (idempotent)
            }

            // -------- NACK ----------------------------------------------------
            x if x == Op::Nack as u16 => {
                // TODO: Decline ack when auto ack? Log?
                let nack: Nack =
                    decode_wire_or_400!(frame, frame_tx_high_prio, metrics, wire::decode_nack);
                if nack.not_before.is_some() && !nack.requeue {
                    send_error_response_and_count(
                        &frame_tx_high_prio,
                        &metrics,
                        frame.request_id,
                        400,
                        "nack not_before requires requeue=true",
                    )
                    .await;
                    continue;
                }

                let key: SubKey = (nack.topic.clone(), nack.partition, nack.group.clone());

                let settle_target = {
                    let state = logical.state.lock().await;
                    state.subs.get(&key).and_then(|sub| {
                        (!sub.auto_ack)
                            .then(|| (sub.state_settler.clone(), sub.pending_settles.clone()))
                    })
                };

                if let Some((state_settler, pending_settles)) = settle_target {
                    let reqs = nack
                        .tags
                        .into_iter()
                        .map(|tag| SettleRequest {
                            delivery_tag: tag,
                            settle_type: SettleType::Nack {
                                requeue: Some(nack.requeue),
                                not_before: nack.not_before,
                            },
                        })
                        .collect();
                    send_settle_batch(&state_settler, &pending_settles, reqs).await;
                }
                // Unknown subscription: ignore (idempotent)
            }

            // -------- PUBLISH ------------------------------------------------
            x if x == Op::Publish as u16 => {
                let publish_received = unix_millis();
                let pubreq: Publish =
                    decode_wire_or_400!(frame, frame_tx_low_prio, metrics, wire::decode_publish);
                if let Some(counter) = &conn_published {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                if fence_stale_partitioning(
                    &frame_tx_low_prio,
                    frame.request_id,
                    &topology_source,
                    &pubreq.topic,
                    pubreq.partition,
                    pubreq.group.as_deref(),
                    pubreq.partitioning_version,
                )
                .await
                {
                    continue;
                }
                let mut headers = pubreq.headers;
                let content_type = normalize_content_type(pubreq.content_type, &mut headers);
                if has_reserved_headers(&headers) {
                    let _ = send_error_response(
                        &frame_tx_low_prio,
                        frame.request_id,
                        400,
                        "headers with reserved prefixes `fibril.` or `stroma.` are reserved",
                    )
                    .await;
                    continue;
                }

                let key = (pubreq.topic.clone(), pubreq.partition, pubreq.group.clone());
                let now_ms = unix_millis();
                // A cached publisher means the topic is a queue, so the steady
                // state skips the stream lookups entirely. The stream checks run
                // only on a cache miss, before a queue publisher is created.
                if !publishers.contains_key(&key) {
                    // Stream fast-path: a stream takes the fan-out path instead
                    // of the queue lease/poll publisher. A stream is recognized
                    // either by an already-open local channel (standalone / hot
                    // owner) or by a coordination declaration (cluster,
                    // including an owner that has not opened it yet). A
                    // non-owner is redirected to the owner.
                    if broker.is_stream(&pubreq.topic, pubreq.partition.id())
                        || broker.stream_declared_in_coordination(&pubreq.topic)
                    {
                        if broker
                            .ensure_stream_owner(&pubreq.topic, pubreq.partition.id())
                            .is_err()
                        {
                            if let Some(frame) = stream_owner_redirect_frame(
                                &topology_source,
                                frame.request_id,
                                &pubreq.topic,
                                pubreq.partition,
                            ) {
                                let _ = frame_tx_low_prio.send(frame).await;
                            } else {
                                send_error_response_and_count(
                                    &frame_tx_low_prio,
                                    &metrics,
                                    frame.request_id,
                                    ERR_NOT_OWNER,
                                    "not the owner of this stream partition, and its current owner could not be resolved - the cluster may still be converging (retry shortly), or coordination is unavailable to route you to the owner",
                                )
                                .await;
                            }
                            continue;
                        }
                        if let Some(channel) = broker
                            .route_stream(&pubreq.topic, pubreq.partition.id())
                            .await
                        {
                            handle_stream_publish(
                                &broker,
                                &channel,
                                &frame_tx_low_prio,
                                frame.request_id,
                                pubreq.payload,
                                pubreq.published,
                                pubreq.require_confirm,
                                content_type,
                                headers,
                                publish_received,
                            )
                            .await?;
                            continue;
                        }
                        // Owner but the channel could not be opened (e.g. storage
                        // error): surface a server error rather than misrouting
                        // to the queue path.
                        send_error_response_and_count(
                            &frame_tx_low_prio,
                            &metrics,
                            frame.request_id,
                            500,
                            "stream partition could not be opened",
                        )
                        .await;
                        continue;
                    }
                    let pubh = match broker
                        .get_publisher(&pubreq.topic, pubreq.partition, &pubreq.group)
                        .await
                    {
                        Ok(publisher) => publisher,
                        Err(err) => {
                            send_owner_redirect_or_error(
                                &frame_tx_low_prio,
                                &metrics,
                                frame.request_id,
                                &topology_source,
                                &pubreq.topic,
                                pubreq.partition,
                                pubreq.group.as_deref(),
                                &err,
                            )
                            .await;
                            continue;
                        }
                    };
                    publishers.insert(
                        key.clone(),
                        CachedPublisher {
                            handle: pubh,
                            last_used_ms: now_ms,
                        },
                    );
                }
                let Some(cached_publisher) = publishers.get_mut(&key) else {
                    send_error_response_and_count(
                        &frame_tx_low_prio,
                        &metrics,
                        frame.request_id,
                        500,
                        "publisher cache unavailable",
                    )
                    .await;
                    continue;
                };
                cached_publisher.last_used_ms = now_ms;
                // Resolve the per-message TTL to an absolute drop deadline using
                // the broker clock (the owner is the clock authority). A per-queue
                // default TTL fallback is applied lower down in Stroma.
                let expire_at = pubreq
                    .ttl_ms
                    .map(|ttl| publish_received.saturating_add(ttl));
                let pub_tx = pub_tx.clone();
                let frame_tx_pub = frame_tx_low_prio.clone();
                // tokio::spawn(async move {
                let published: Result<
                    tokio::sync::oneshot::Receiver<Result<u64, fibril_broker::broker::BrokerError>>,
                    fibril_broker::broker::BrokerError,
                > = if pubreq.require_confirm {
                    cached_publisher
                        .handle
                        .publish(
                            pubreq.payload,
                            pubreq.published,
                            publish_received,
                            to_storage_content_type(content_type),
                            headers,
                            expire_at,
                        )
                        .await
                } else {
                    cached_publisher
                        .handle
                        .publish_no_confirm(
                            pubreq.payload,
                            pubreq.published,
                            publish_received,
                            to_storage_content_type(content_type),
                            headers,
                            expire_at,
                        )
                        .await
                };
                let res = pub_tx
                    .send((published, pubreq.require_confirm, frame.request_id))
                    .await;
                if let Err(_err) = res {
                    let _ =
                        send_error_response(&frame_tx_pub, frame.request_id, 500, "Broken pipe")
                            .await;
                    tracing::error!("Error sending published to queue");
                }
                // });
            }
            x if x == Op::PublishDelayed as u16 => {
                let publish_received = unix_millis();
                let pubreq: PublishDelayed = decode_wire_or_400!(
                    frame,
                    frame_tx_low_prio,
                    metrics,
                    wire::decode_publish_delayed
                );
                if let Some(counter) = &conn_published {
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                if fence_stale_partitioning(
                    &frame_tx_low_prio,
                    frame.request_id,
                    &topology_source,
                    &pubreq.topic,
                    pubreq.partition,
                    pubreq.group.as_deref(),
                    pubreq.partitioning_version,
                )
                .await
                {
                    continue;
                }
                let mut headers = pubreq.headers;
                let content_type = normalize_content_type(pubreq.content_type, &mut headers);
                if has_reserved_headers(&headers) {
                    let _ = send_error_response(
                        &frame_tx_low_prio,
                        frame.request_id,
                        400,
                        "headers with reserved prefixes `fibril.` or `stroma.` are reserved",
                    )
                    .await;
                    continue;
                }

                let key = (pubreq.topic.clone(), pubreq.partition, pubreq.group.clone());
                let now_ms = unix_millis();
                if !publishers.contains_key(&key) {
                    let pubh = match broker
                        .get_publisher(&pubreq.topic, pubreq.partition, &pubreq.group)
                        .await
                    {
                        Ok(publisher) => publisher,
                        Err(err) => {
                            send_owner_redirect_or_error(
                                &frame_tx_low_prio,
                                &metrics,
                                frame.request_id,
                                &topology_source,
                                &pubreq.topic,
                                pubreq.partition,
                                pubreq.group.as_deref(),
                                &err,
                            )
                            .await;
                            continue;
                        }
                    };
                    publishers.insert(
                        key.clone(),
                        CachedPublisher {
                            handle: pubh,
                            last_used_ms: now_ms,
                        },
                    );
                }
                let Some(cached_publisher) = publishers.get_mut(&key) else {
                    send_error_response_and_count(
                        &frame_tx_low_prio,
                        &metrics,
                        frame.request_id,
                        500,
                        "publisher cache unavailable",
                    )
                    .await;
                    continue;
                };
                cached_publisher.last_used_ms = now_ms;
                let pub_tx = pub_tx.clone();
                let frame_tx_pub = frame_tx_low_prio.clone();
                // tokio::spawn(async move {
                let published: Result<
                    tokio::sync::oneshot::Receiver<Result<u64, fibril_broker::broker::BrokerError>>,
                    fibril_broker::broker::BrokerError,
                > = if pubreq.require_confirm {
                    cached_publisher
                        .handle
                        .publish_delayed(
                            pubreq.payload,
                            pubreq.published,
                            publish_received,
                            to_storage_content_type(content_type),
                            headers,
                            pubreq.not_before,
                        )
                        .await
                } else {
                    cached_publisher
                        .handle
                        .publish_no_confirm_delayed(
                            pubreq.payload,
                            pubreq.published,
                            publish_received,
                            to_storage_content_type(content_type),
                            headers,
                            pubreq.not_before,
                        )
                        .await
                };
                let res = pub_tx
                    .send((published, pubreq.require_confirm, frame.request_id))
                    .await;
                if let Err(_err) = res {
                    let _ =
                        send_error_response(&frame_tx_pub, frame.request_id, 500, "Broken pipe")
                            .await;
                    tracing::error!("Error sending published to queue");
                }
                // });
            }

            // -------- PING ---------------------------------------------------
            x if x == Op::Ping as u16 => {
                frame_tx_high_prio
                    .send(try_encode(Op::Pong, frame.request_id, &())?)
                    .await?;
            }

            // -------- PONG ---------------------------------------------------
            x if x == Op::Pong as u16 => {
                // pass
            }

            // -------- UNKNOWN -----------------------------------------------
            _ => {
                frame_tx_high_prio
                    .send(error_frame(
                        frame.request_id,
                        400,
                        format!(
                            "unknown opcode {}. This usually means the client speaks a newer \
                             protocol than this broker - upgrade the broker or use a client \
                             matching its version",
                            frame.opcode
                        ),
                    )?)
                    .await?;
                metrics.error();
            }
        }
    }

    // ---- Connection closing ------------------------------------------------
    let reconnect_grace_ms = connection_settings
        .runtime_snapshot()
        .reconnect_grace_ms
        .unwrap_or_default();
    let entered_grace = reconnect_grace_ms > 0 && logical.mark_dormant(transport_generation);

    if entered_grace {
        tcp_stats.reconnect_grace_entered();
        let grace_ms = reconnect_grace_ms;
        let broker_for_cleanup = broker.clone();
        let logical_for_cleanup = logical.clone();
        let resume_sessions = connection_settings.resume_sessions.clone();
        let metrics_for_cleanup = tcp_stats.clone();
        tracing::info!(
            client_id = %logical.client_id,
            grace_ms,
            "client entered reconnect grace"
        );
        tokio::spawn(async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(grace_ms)).await;
            if logical_for_cleanup.is_dormant_generation(transport_generation) {
                metrics_for_cleanup.reconnect_grace_expired();
                tracing::info!(
                    client_id = %logical_for_cleanup.client_id,
                    grace_ms,
                    "reconnect grace expired; cleaning up dormant connection"
                );
                cleanup_connection_state(broker_for_cleanup, logical_for_cleanup.clone()).await;
                resume_sessions.forget_if_dormant(
                    logical_for_cleanup.client_id,
                    &logical_for_cleanup,
                    transport_generation,
                );
            }
        });
    }

    // Closes this socket session's writer channels.
    drop(pub_tx);
    drop(frame_tx_high_prio);
    drop(frame_tx_low_prio);

    let _ = shutdown_tx.send(());
    // Bounded window for the writer to flush final frames, abort as the
    // backstop for a peer that stops reading.
    let _ = tokio::time::timeout(tokio::time::Duration::from_secs(2), &mut writer_task).await;
    writer_task.abort();
    pub_queue_handle.await?;

    if !entered_grace {
        cleanup_connection_state(broker.clone(), logical.clone()).await;
        connection_settings.resume_sessions.forget_if_generation(
            logical.client_id,
            &logical,
            transport_generation,
        );
    }

    tracing::debug!("[conn] EXIT handle_connection peer={:?}", peer_addr);

    tcp_stats.connection_closed();
    Ok(())
}

pub fn print_banner(bind: &SocketAddr, tls: bool) {
    let ts = Instant::now().elapsed().as_nanos();
    let idx = (ts % (ASCII_ARTS.len() as u128)) as usize;

    let art = ASCII_ARTS[idx];
    let transport = if tls { "TLS" } else { "plaintext" };

    tracing::info!("\n{art}\n🚀 Listening on {bind} ({transport}) 🧵\n");
}

const ASCII_ARTS: &[&str] = &[
    r#"
███████╗██╗██████╗ ██████╗ ██╗██╗     
██╔════╝██║██╔══██╗██╔══██╗██║██║     
█████╗  ██║██████╔╝██████╔╝██║██║     
██╔══╝  ██║██╔══██╗██╔══██╗██║██║     
██║     ██║██████╔╝██║  ██║██║███████╗
╚═╝     ╚═╝╚═════╝ ╚═╝  ╚═╝╚═╝╚══════╝
                                         
                                         
"#,
    r#"
                                                  
88888888888  88  88                       88  88  
88           ""  88                       ""  88  
88               88                           88  
88aaaaa      88  88,dPPYba,   8b,dPPYba,  88  88  
88"""""      88  88P'    "8a  88P'   "Y8  88  88  
88           88  88       d8  88          88  88  
88           88  88b,   ,a8"  88          88  88  
88           88  8Y"Ybbd8"'   88          88  88  
                                                  
                                                  
"#,
    r#"
'||''''|  ||  '||               ||  '||  
 ||  .   ...   || ...  ... ..  ...   ||  
 ||''|    ||   ||'  ||  ||' ''  ||   ||  
 ||       ||   ||    |  ||      ||   ||  
.||.     .||.  '|...'  .||.    .||. .||. 
                                         
                                         
"#,
    r#"
'||''''|      '||                 '||` 
 ||  .    ''   ||             ''   ||  
 ||''|    ||   ||''|, '||''|  ||   ||  
 ||       ||   ||  ||  ||     ||   ||  
.||.     .||. .||..|' .||.   .||. .||. 
                                       
                                       
"#,
    r#"
 ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄   ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄           
▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░▌ ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░▌          
▐░█▀▀▀▀▀▀▀▀▀  ▀▀▀▀█░█▀▀▀▀ ▐░█▀▀▀▀▀▀▀█░▌▐░█▀▀▀▀▀▀▀█░▌ ▀▀▀▀█░█▀▀▀▀ ▐░▌          
▐░▌               ▐░▌     ▐░▌       ▐░▌▐░▌       ▐░▌     ▐░▌     ▐░▌          
▐░█▄▄▄▄▄▄▄▄▄      ▐░▌     ▐░█▄▄▄▄▄▄▄█░▌▐░█▄▄▄▄▄▄▄█░▌     ▐░▌     ▐░▌          
▐░░░░░░░░░░░▌     ▐░▌     ▐░░░░░░░░░░▌ ▐░░░░░░░░░░░▌     ▐░▌     ▐░▌          
▐░█▀▀▀▀▀▀▀▀▀      ▐░▌     ▐░█▀▀▀▀▀▀▀█░▌▐░█▀▀▀▀█░█▀▀      ▐░▌     ▐░▌          
▐░▌               ▐░▌     ▐░▌       ▐░▌▐░▌     ▐░▌       ▐░▌     ▐░▌          
▐░▌           ▄▄▄▄█░█▄▄▄▄ ▐░█▄▄▄▄▄▄▄█░▌▐░▌      ▐░▌  ▄▄▄▄█░█▄▄▄▄ ▐░█▄▄▄▄▄▄▄▄▄ 
▐░▌          ▐░░░░░░░░░░░▌▐░░░░░░░░░░▌ ▐░▌       ▐░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌
 ▀            ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀   ▀         ▀  ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀▀ 
                                                                              
"#,
    r#"
                                                                                    
                             bbbbbbbb                                               
FFFFFFFFFFFFFFFFFFFFFF  iiii b::::::b                                 iiii  lllllll 
F::::::::::::::::::::F i::::ib::::::b                                i::::i l:::::l 
F::::::::::::::::::::F  iiii b::::::b                                 iiii  l:::::l 
FF::::::FFFFFFFFF::::F        b:::::b                                       l:::::l 
  F:::::F       FFFFFFiiiiiii b:::::bbbbbbbbb    rrrrr   rrrrrrrrr  iiiiiii  l::::l 
  F:::::F             i:::::i b::::::::::::::bb  r::::rrr:::::::::r i:::::i  l::::l 
  F::::::FFFFFFFFFF    i::::i b::::::::::::::::b r:::::::::::::::::r i::::i  l::::l 
  F:::::::::::::::F    i::::i b:::::bbbbb:::::::brr::::::rrrrr::::::ri::::i  l::::l 
  F:::::::::::::::F    i::::i b:::::b    b::::::b r:::::r     r:::::ri::::i  l::::l 
  F::::::FFFFFFFFFF    i::::i b:::::b     b:::::b r:::::r     rrrrrrri::::i  l::::l 
  F:::::F              i::::i b:::::b     b:::::b r:::::r            i::::i  l::::l 
  F:::::F              i::::i b:::::b     b:::::b r:::::r            i::::i  l::::l 
FF:::::::FF           i::::::ib:::::bbbbbb::::::b r:::::r           i::::::il::::::l
F::::::::FF           i::::::ib::::::::::::::::b  r:::::r           i::::::il::::::l
F::::::::FF           i::::::ib:::::::::::::::b   r:::::r           i::::::il::::::l
FFFFFFFFFFF           iiiiiiiibbbbbbbbbbbbbbbb    rrrrrrr           iiiiiiiillllllll
                                                                                    
                                                                                    
                                                                                    
                                                                                    
                                                                                    
                                                                                    
                                                                                    
"#,
];

#[cfg(test)]
mod going_away_tests {
    use super::*;

    #[tokio::test]
    async fn going_away_broadcasts_only_to_attached_transports() {
        let registry = ResumeSessionRegistry::new();

        // One session with a live transport, one dormant (no socket attached).
        let live = registry.issue(ResumeOutcome::New);
        let (tx, mut rx) = mpsc::channel::<Frame>(4);
        live.logical.attach_transport(tx);
        let _dormant = registry.issue(ResumeOutcome::New);

        let sent = registry.broadcast_going_away(30_000, "upgrade").await;
        assert_eq!(
            sent, 1,
            "only the session with a live transport is notified"
        );

        let frame = rx.recv().await.expect("a going-away frame");
        assert_eq!(frame.opcode, Op::GoingAway as u16);
        let notice = wire::decode_going_away(&frame).expect("decode going away");
        assert_eq!(notice.grace_ms, 30_000);
        assert_eq!(notice.message, "upgrade");
    }
}

#[cfg(test)]
mod error_guide_tests {
    use super::*;

    #[test]
    fn invalid_argument_is_a_client_error_not_a_server_fault() {
        let (code, msg) = broker_error_response(&BrokerError::InvalidArgument(
            "partition 9 out of range".into(),
        ));
        assert_eq!(
            code, ERR_INVALID,
            "a malformed request is a 400 (DoNotRetry), not a retryable 500"
        );
        assert!(
            msg.contains("partition 9 out of range"),
            "the call site's detail is preserved: {msg}"
        );
    }

    #[test]
    fn not_enough_replicas_stays_retryable_and_names_the_levers() {
        let (code, msg) = broker_error_response(&BrokerError::NotEnoughInSyncReplicas {
            topic: "orders".into(),
            partition: Partition::new(0),
            in_sync: 1,
            required: 2,
        });
        assert_eq!(
            code, 500,
            "a transient durability shortfall stays retryable"
        );
        assert!(
            msg.contains("replication factor") && msg.contains("replicas"),
            "the guide names the two levers rather than a bare number: {msg}"
        );
    }

    #[test]
    fn internal_faults_disclaim_the_callers_request() {
        let (code, msg) = broker_error_response(&BrokerError::Unknown("disk on fire".into()));
        assert_eq!(code, 500);
        assert!(
            msg.contains("broker logs") && msg.contains("not a problem with your request"),
            "the guide tells the user it is not their fault and where to look: {msg}"
        );
    }
}
