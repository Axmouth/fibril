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
    frame::{Frame, ProtoCodec},
    helper::{error_frame, try_decode, try_encode},
    *,
};
use anyhow::Context;
use arc_swap::ArcSwap;
use fibril_broker::{
    broker::{
        Broker, BrokerError, BrokerFollowerReplicationApply, BrokerOwnerReplicationRecords,
        ConsumerConfig, ConsumerHandle, ConsumerLease, PublisherHandle, SettleRequest, SettleType,
    },
    queue_engine::{
        DLQDiscardPolicyWire, DeclareMeta, FollowerStateCheckpointInstall, GlobalDLQ, Message,
        MessageContentType, OwnerReplicationBatch, OwnerReplicationRead, OwnerStateCheckpoint,
        QueueEngine, StromaEngine, StromaError, StromaEvent,
    },
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_storage::{Group, Topic};
use fibril_util::{AuthHandler, unix_millis};
use futures::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::{Mutex, mpsc, oneshot, watch},
};
use tokio_util::codec::Framed;
use uuid::Uuid;

type SubKey = (Topic, Option<Group>); // (topic, group)
type FrameSink = mpsc::Sender<Frame>;
const RESERVED_HEADER_PREFIXES: &[&str] = &["fibril.", "stroma."];

/// Server-side source of client-facing topology, injected into the protocol
/// handler so it can answer `Op::Topology` and emit `Op::Redirect` on not-owner.
/// Implemented by a coordination adapter in the binary; `None` (standalone)
/// means there is no routing info and clients use their direct connection.
pub trait ClientTopologySource: Send + Sync {
    /// Full client-facing topology snapshot for `Op::Topology`.
    fn topology(&self) -> TopologyOk;
    /// Current owner endpoint and partitioning version for one queue partition,
    /// if known. Used to build an `Op::Redirect`.
    fn owner_endpoint(
        &self,
        topic: &str,
        partition: u32,
        group: Option<&str>,
    ) -> Option<(String, u64)>;
}

/// Server-side writer for queue-declaration coordination: records the queue's
/// partitioning (count + version) in the replicated store, returning the
/// EFFECTIVE partition count (which may differ from the request if the queue
/// was already declared). `None` (standalone) means declare is local-only.
pub trait QueueDeclareCoordinator: Send + Sync {
    fn declare_partitioning<'a>(
        &'a self,
        topic: &'a str,
        group: Option<&'a str>,
        partition_count: u32,
    ) -> futures::future::BoxFuture<'a, Result<u32, String>>;
}

struct ConnState {
    authenticated: bool,
    subs: HashMap<SubKey, SubState>,
}

fn has_reserved_headers(headers: &HashMap<String, String>) -> bool {
    headers.keys().any(|key| {
        RESERVED_HEADER_PREFIXES
            .iter()
            .any(|prefix| key.starts_with(prefix))
    })
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
    })
}

async fn send_error_response(
    tx: &mpsc::Sender<Frame>,
    request_id: u64,
    code: u16,
    message: impl Into<String>,
) -> anyhow::Result<()> {
    tx.send(error_frame(request_id, code, message)?).await?;
    Ok(())
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

fn broker_error_response(err: &BrokerError) -> (u16, String) {
    match err {
        BrokerError::NotOwner { .. } => (ERR_NOT_OWNER, err.to_string()),
        _ => (500, err.to_string()),
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
    partition: u32,
    group: Option<&str>,
) -> Option<Frame> {
    let (owner_endpoint, partitioning_version) = topology_source
        .as_ref()?
        .owner_endpoint(topic, partition, group)?;
    let redirect = Redirect {
        topic: topic.to_string(),
        partition,
        group: group.map(str::to_string),
        owner_endpoint,
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
    partition: u32,
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
        owner_endpoint,
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
    partition: u32,
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
}

fn install_subscription_error_response(err: &InstallSubscriptionError) -> (u16, String) {
    match err {
        InstallSubscriptionError::Broker(err) => broker_error_response(err),
        InstallSubscriptionError::AlreadySubscribed => (ERR_CONFLICT, err.to_string()),
    }
}

struct SubState {
    sub_id: u64,
    partition: u32,
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

async fn send_to_current_transport(
    transport_rx: &mut watch::Receiver<Option<FrameSink>>,
    mut frame: Frame,
) -> Result<(), ()> {
    loop {
        let current_tx = transport_rx.borrow().clone();
        if let Some(tx) = current_tx {
            match tx.send(frame).await {
                Ok(()) => return Ok(()),
                Err(err) => {
                    frame = err.0;
                }
            }
        }

        transport_rx.changed().await.map_err(|_| ())?;
    }
}

async fn cleanup_connection_state(
    broker: Arc<Broker<StromaEngine>>,
    logical: Arc<LogicalConnection>,
) {
    broker.wait_for_pending_settles().await;

    let drained_subs = {
        let mut state = logical.state.lock().await;
        state.subs.drain().collect::<Vec<_>>()
    };

    for ((topic, group), sub) in drained_subs {
        sub.task.abort();
        if let Err(err) = broker
            .unsubscribe(&topic, group.as_deref(), sub.partition, sub.sub_id)
            .await
        {
            tracing::warn!("Failed to unsubscribe consumer {}: {err}", sub.sub_id);
        }
    }
}

async fn remove_subscription(
    broker: &Arc<Broker<StromaEngine>>,
    logical: &Arc<LogicalConnection>,
    connection_stats: &Arc<ConnectionStats>,
    conn_id: &Uuid,
    topic: &str,
    group: Option<&str>,
) -> Option<ReconcileSubscription> {
    let key: SubKey = (topic.to_string(), group.map(str::to_string));
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

    Some(ReconcileSubscription {
        sub_id: sub.sub_id,
        topic: topic.to_string(),
        group: group.map(str::to_string),
        partition: sub.partition,
        auto_ack: sub.auto_ack,
        prefetch: sub.prefetch,
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
    group: Option<String>,
    prefetch: u32,
    auto_ack: bool,
}

async fn install_subscription(
    args: InstallSubscriptionArgs,
) -> Result<SubscribeOk, InstallSubscriptionError> {
    let sub_key: SubKey = (args.topic.clone(), args.group.clone());

    if args.logical.state.lock().await.subs.contains_key(&sub_key) {
        return Err(InstallSubscriptionError::AlreadySubscribed);
    }

    let consumer = args
        .broker
        .subscribe(
            &args.topic,
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

        while let Some(msg) = rx.recv().await {
            let mut headers = msg.message.headers.clone();
            if msg.message.retried > 0 {
                headers.insert("fibril.retries".into(), msg.message.retried.to_string());
            }

            let deliver = Deliver {
                sub_id,
                topic: msg.message.topic.clone(),
                group: msg.group.clone(),
                partition: msg.message.partition,
                offset: msg.message.offset,
                delivery_tag: msg.delivery_tag,
                published: msg.message.published,
                publish_received: msg.message.publish_received,
                content_type: to_protocol_content_type(msg.message.content_type),
                headers,
                payload: msg.message.payload.clone(),
            };

            tracing::debug!("Sending Deliver");

            let frame = match try_encode(Op::Deliver, req_id_gen_clone.next_id(), &deliver) {
                Ok(frame) => frame,
                Err(err) => {
                    tracing::error!("Failed to encode Deliver frame: {err}");
                    metrics.error();
                    break;
                }
            };
            if send_to_current_transport(&mut transport_rx, frame)
                .await
                .is_err()
            {
                tracing::warn!("Failed to send deliver frame to active transport");
                metrics.error();
                break;
            }

            if auto_ack {
                pending_settles_clone.fetch_add(1, Ordering::AcqRel);
                let _ = settler
                    .send(SettleRequest {
                        settle_type: SettleType::Ack,
                        delivery_tag: msg.delivery_tag,
                    })
                    .await
                    .inspect_err(|_| {
                        pending_settles_clone.fetch_sub(1, Ordering::AcqRel);
                    });
            }
        }
    });

    args.logical.state.lock().await.subs.insert(
        sub_key,
        SubState {
            sub_id,
            partition: consumer.partition,
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
    })
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
        let key: SubKey = (client.topic.clone(), client.group.clone());
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
            None if reconcile.policy == ReconcilePolicy::RestoreClientSubscriptions => {
                let restored = install_subscription(InstallSubscriptionArgs {
                    broker: broker.clone(),
                    logical: logical.clone(),
                    connection_stats: connection_stats.clone(),
                    metrics: metrics.clone(),
                    conn_id,
                    client_id,
                    req_id_gen: req_id_gen.clone(),
                    topic: client.topic.clone(),
                    group: client.group.clone(),
                    prefetch: client.prefetch,
                    auto_ack: client.auto_ack,
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

    for (topic, group) in server_only {
        if let Some(server) = remove_subscription(
            &broker,
            &logical,
            &connection_stats,
            &conn_id,
            &topic,
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
    publishers: &mut HashMap<(Topic, Option<Group>), CachedPublisher>,
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
    broker: Arc<Broker<StromaEngine>>,
    tcp_stats: Arc<TcpStats>,
    connection_stats: Arc<ConnectionStats>,
    auth: Option<impl AuthHandler + Send + Sync + Clone + 'static>,
    connection_settings: ConnectionSettings,
    topology_source: Option<Arc<dyn ClientTopologySource>>,
    declare_coordinator: Option<Arc<dyn QueueDeclareCoordinator>>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    print_banner(&addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        socket.set_nodelay(true)?;
        tcp_stats.connection_opened();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        let broker = broker.clone();

        let auth = auth.clone();
        let tcp_stats = tcp_stats.clone();
        let connection_stats = connection_stats.clone();
        let connection_settings = connection_settings.clone();
        let topology_source = topology_source.clone();
        let declare_coordinator = declare_coordinator.clone();
        tokio::spawn(async move {
            tracing::info!("Connection {conn_id} opening..");
            if let Err(e) = handle_connection(
                socket,
                broker,
                tcp_stats.clone(),
                connection_stats.clone(),
                conn_id,
                auth,
                connection_settings,
                topology_source,
                declare_coordinator,
            )
            .await
            {
                tracing::error!("conn {} error: {:?}", peer, e);
            }

            connection_stats.remove_connection(&conn_id);

            tracing::info!("Connection {conn_id} closed..");
        });
    }
}

pub const DEFAULT_HEARTBEAT_INTERVAL: u64 = 5; // seconds

pub enum LoopEvent {
    Heartbeat,
    Frame(Frame),
    Disconnect,
    Timeout,
}

// TODO: Resolve publish drowning out delivery

pub async fn handle_connection(
    socket: tokio::net::TcpStream,
    broker: Arc<Broker<StromaEngine>>,
    tcp_stats: Arc<TcpStats>,
    connection_stats: Arc<ConnectionStats>,
    conn_id: Uuid,
    auth_handler: Option<impl AuthHandler + Send + Sync>,
    connection_settings: ConnectionSettings,
    topology_source: Option<Arc<dyn ClientTopologySource>>,
    declare_coordinator: Option<Arc<dyn QueueDeclareCoordinator>>,
) -> anyhow::Result<()> {
    let heartbeat_interval = connection_settings
        .heartbeat_interval
        .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);
    let timeout = tokio::time::Duration::from_secs(heartbeat_interval * 3);
    // TODO: Consider sharing it between connections?
    let req_id_gen = ReqIdGenerator::new();

    let mut last_seen = Instant::now();
    let mut heartbeat = tokio::time::interval(tokio::time::Duration::from_secs(heartbeat_interval));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // consume first tick
    // ---- Framed socket -----------------------------------------------------
    let peer_addr = socket.peer_addr().ok();
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
        .context("connection closed before HELLO")??;

    if frame.opcode != Op::Hello as u16 {
        writer
            .send(error_frame(frame.request_id, 400, "expected HELLO")?)
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
    let hello_ok = &HelloOk {
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
        anyhow::bail!("Protocol compliance marker mismatch");
    }

    let writer_task = tokio::spawn(async move {
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

                _ = ticker.tick() => {
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
                            match try_encode(Op::PublishOk, request_id, &PublishOk { offset }) {
                                Ok(frame) => {
                                    frame_tx_pub.send(frame).await.map_err(anyhow::Error::from)
                                }
                                Err(err) => Err(anyhow::Error::from(err)),
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

    let mut publishers = HashMap::<(Topic, Option<Group>), CachedPublisher>::new();

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

                        let verified = auth_handler
                            .verify(&auth_frame.username, &auth_frame.password)
                            .await;

                        if verified {
                            logical.state.lock().await.authenticated = true;
                            frame_tx_high_prio
                                .send(try_encode(Op::AuthOk, frame.request_id, &())?)
                                .await?;
                            connection_stats.set_connection_auth(&conn_id, true);
                        } else {
                            frame_tx_high_prio
                                .send(try_encode(
                                    Op::AuthErr,
                                    frame.request_id,
                                    &ErrorMsg {
                                        code: 401,
                                        message: "invalid credentials".into(),
                                    },
                                )?)
                                .await?;
                            metrics.error();

                            break; // close connection
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
                            .send(try_encode(
                                Op::ReplicationReadOk,
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
                            partition,
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

                // Captured for a possible redirect (the identity is moved into
                // the install args below). Subscribe is implicitly partition 0
                // today; an explicit partition on the wire comes with
                // multi-partition subscriptions.
                let sub_topic = sub.topic.clone();
                let sub_group = sub.group.clone();
                let sub_partition = 0u32;

                let sub_ok = install_subscription(InstallSubscriptionArgs {
                    broker: broker.clone(),
                    logical: logical.clone(),
                    connection_stats: connection_stats.clone(),
                    metrics: metrics.clone(),
                    conn_id,
                    client_id,
                    req_id_gen: req_id_gen.clone(),
                    topic: sub.topic,
                    group: sub.group,
                    prefetch: sub.prefetch,
                    auto_ack: sub.auto_ack,
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
                    });
                if let Some(topic) = &req.topic {
                    topology.queues.retain(|queue| {
                        &queue.topic == topic
                            && req
                                .group
                                .as_deref()
                                .is_none_or(|group| queue.group.as_deref() == Some(group))
                    });
                }
                frame_tx_high_prio
                    .send(try_encode(Op::TopologyOk, frame.request_id, &topology)?)
                    .await?;
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

            // -------- ACK ----------------------------------------------------
            x if x == Op::Ack as u16 => {
                // TODO: Decline ack when auto ack? Log?
                let ack: Ack = decode_or_400!(frame, frame_tx_high_prio, metrics, Ack);

                let key: SubKey = (ack.topic.clone(), ack.group.clone());

                let settle_target = {
                    let state = logical.state.lock().await;
                    state.subs.get(&key).and_then(|sub| {
                        (!sub.auto_ack)
                            .then(|| (sub.state_settler.clone(), sub.pending_settles.clone()))
                    })
                };

                if let Some((state_settler, pending_settles)) = settle_target {
                    // tokio::spawn(async move {
                    for tag in ack.tags {
                        let req = SettleRequest {
                            delivery_tag: tag,
                            settle_type: SettleType::Ack,
                        };
                        pending_settles.fetch_add(1, Ordering::AcqRel);
                        let _ = state_settler.send(req).await.inspect_err(|_| {
                            pending_settles.fetch_sub(1, Ordering::AcqRel);
                        });
                    }
                    // });
                }
                // Unknown subscription: ignore (idempotent)
            }

            // -------- NACK ----------------------------------------------------
            x if x == Op::Nack as u16 => {
                // TODO: Decline ack when auto ack? Log?
                let nack: Nack = decode_or_400!(frame, frame_tx_high_prio, metrics, Nack);
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

                let key: SubKey = (nack.topic.clone(), nack.group.clone());

                let settle_target = {
                    let state = logical.state.lock().await;
                    state.subs.get(&key).and_then(|sub| {
                        (!sub.auto_ack)
                            .then(|| (sub.state_settler.clone(), sub.pending_settles.clone()))
                    })
                };

                if let Some((state_settler, pending_settles)) = settle_target {
                    // tokio::spawn(async move {
                    for tag in nack.tags {
                        let req = SettleRequest {
                            delivery_tag: tag,
                            settle_type: SettleType::Nack {
                                requeue: Some(nack.requeue),
                                not_before: nack.not_before,
                            },
                        };
                        pending_settles.fetch_add(1, Ordering::AcqRel);
                        let _ = state_settler.send(req).await.inspect_err(|_| {
                            pending_settles.fetch_sub(1, Ordering::AcqRel);
                        });
                    }
                    // });
                }
                // Unknown subscription: ignore (idempotent)
            }

            // -------- PUBLISH ------------------------------------------------
            x if x == Op::Publish as u16 => {
                let publish_received = unix_millis();
                let pubreq: Publish = decode_or_400!(frame, frame_tx_low_prio, metrics, Publish);
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

                let key = (pubreq.topic.clone(), pubreq.group.clone());
                let now_ms = unix_millis();
                if !publishers.contains_key(&key) {
                    let (pubh, mut conf_stream) =
                        match broker.get_publisher(&pubreq.topic, &pubreq.group).await {
                            Ok(publisher) => publisher,
                            Err(err) => {
                                send_owner_redirect_or_error(
                                    &frame_tx_low_prio,
                                    &metrics,
                                    frame.request_id,
                                    &topology_source,
                                    &pubreq.topic,
                                    0,
                                    pubreq.group.as_deref(),
                                    &err,
                                )
                                .await;
                                continue;
                            }
                        };

                    // let conf_sink = frame_tx_low_prio.clone();
                    // let req_id_gen_clone = req_id_gen.clone();
                    tokio::spawn(async move {
                        while let Some(offset) = conf_stream.recv().await {
                            // let res = conf_sink.send(encode(Op::PublishOk, req_id_gen_clone.next_id(), &PublishOk {offset})).await;

                            // if let Err(_) = res {
                            //     tracing::warn!("Error sending confirm for offset {offset}");
                            // }

                            // TODO: confirms are handled elsewhere, see if there's a cleaner way than this
                            let _ = offset;
                        }
                    });
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
                        .publish(
                            pubreq.payload,
                            pubreq.published,
                            publish_received,
                            to_storage_content_type(content_type),
                            headers,
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
                let pubreq: PublishDelayed =
                    decode_or_400!(frame, frame_tx_low_prio, metrics, PublishDelayed);
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

                let key = (pubreq.topic.clone(), pubreq.group.clone());
                let now_ms = unix_millis();
                if !publishers.contains_key(&key) {
                    let (pubh, mut conf_stream) =
                        match broker.get_publisher(&pubreq.topic, &pubreq.group).await {
                            Ok(publisher) => publisher,
                            Err(err) => {
                                send_owner_redirect_or_error(
                                    &frame_tx_low_prio,
                                    &metrics,
                                    frame.request_id,
                                    &topology_source,
                                    &pubreq.topic,
                                    0,
                                    pubreq.group.as_deref(),
                                    &err,
                                )
                                .await;
                                continue;
                            }
                        };

                    // let conf_sink = frame_tx_low_prio.clone();
                    // let req_id_gen_clone = req_id_gen.clone();
                    tokio::spawn(async move {
                        while let Some(offset) = conf_stream.recv().await {
                            // let res = conf_sink.send(encode(Op::PublishOk, req_id_gen_clone.next_id(), &PublishOk {offset})).await;

                            // if let Err(_) = res {
                            //     tracing::warn!("Error sending confirm for offset {offset}");
                            // }

                            // TODO: confirms are handled elsewhere, see if there's a cleaner way than this
                            let _ = offset;
                        }
                    });
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
                    .send(error_frame(frame.request_id, 400, "unknown opcode")?)
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

pub fn print_banner(bind: &SocketAddr) {
    let ts = Instant::now().elapsed().as_nanos();
    let idx = (ts % (ASCII_ARTS.len() as u128)) as usize;

    let art = ASCII_ARTS[idx];

    tracing::info!("\n{art}\nListening on {bind}\n");
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
