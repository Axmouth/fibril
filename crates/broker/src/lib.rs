pub mod broker;
pub mod coordination;
pub mod queue_engine;
pub mod test_util;

use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use fibril_metrics::BrokerStats;
use fibril_storage::{BrokerCompletionPair, CompletionPair};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::coordination::Coordination;
use fibril_storage::{
    DeliverableMessage, DeliveryTag, Group, LogId, Offset, Storage, StorageError, Topic,
};
use fibril_util::{UnixMillis, unix_millis};

macro_rules! invariant {
    ($cond:expr, $($arg:tt)*) => {
        if cfg!(debug_assertions) && !$cond {
            panic!($($arg)*);
        }
    };
}

#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("not the leader for topic {0} partition {1}")]
    NotLeader(String, u32),

    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("coordination error: {0}")]
    Coordination(String),

    #[error("anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),

    #[error("channel closed")]
    ChannelClosed,

    #[error("batch append failed: {0}")]
    BatchAppendFailed(String),

    #[error("unknown error: {0}")]
    Unknown(String),
}

type ConsumerId = u64;

#[derive(Debug)]
struct GroupCursor {
    pub next_offset: u64,
}

#[derive(Debug)]
struct TopicState {
    consumers: DashMap<ConsumerId, mpsc::Sender<DeliverableMessage>>,
    next_offset: AtomicU64,
    delivery_task_started: AtomicBool,
    rr_counter: AtomicU64,
    tag_counter: AtomicU64,
    redelivery: Arc<SegQueue<Offset>>, // lock-free FIFO queue
    inflight_sem: Arc<Semaphore>,
    delivery_tag_by_offset: DashMap<Offset, DeliveryTag>,
    inflight_permits_by_tag: DashMap<DeliveryTag, (Offset, OwnedSemaphorePermit)>,
    events: Arc<Semaphore>,
}

impl TopicState {
    fn new() -> Self {
        Self {
            consumers: DashMap::new(),
            next_offset: AtomicU64::new(0),
            delivery_task_started: AtomicBool::new(false),
            rr_counter: AtomicU64::new(0),
            tag_counter: AtomicU64::new(1),
            redelivery: Arc::new(SegQueue::new()),
            inflight_sem: Arc::new(Semaphore::new(0)),
            delivery_tag_by_offset: DashMap::new(),
            inflight_permits_by_tag: DashMap::new(),
            events: Arc::new(Semaphore::new(0)),
        }
    }

    #[inline]
    fn signal(&self) {
        // "Something changed, delivery may make progress"
        self.events.add_permits(1);
    }
}

pub enum SettleType {
    Ack,
    Nack { requeue: Option<bool> },
    Reject { requeue: Option<bool> },
}

pub struct SettleRequest {
    // pub group: Group,
    // pub topic: Topic,
    // pub partition: Partition,
    pub settle_type: SettleType,
    pub delivery_tag: DeliveryTag,
}

pub struct PublishRequest {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Result<Offset, BrokerError>>,
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub cleanup_interval_secs: u64,
    // TODO: Rename to better decribe? Ack deadline?
    pub inflight_ttl_secs: u64, // seconds
    pub publish_batch_size: usize,
    pub publish_batch_timeout_ms: u64,
    pub ack_batch_size: usize,
    pub ack_batch_timeout_ms: u64,
    pub inflight_batch_size: usize,
    pub inflight_batch_timeout_ms: u64,
    /// Unsafe startup option to reset all inflight state
    pub reset_inflight: bool,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            cleanup_interval_secs: 60,
            inflight_ttl_secs: 60,
            publish_batch_size: 256,
            publish_batch_timeout_ms: 10,
            ack_batch_size: 512,
            ack_batch_timeout_ms: 10,
            inflight_batch_size: 512,
            inflight_batch_timeout_ms: 1,

            reset_inflight: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub prefetch_count: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self { prefetch_count: 32 }
    }
}

impl ConsumerConfig {
    pub fn with_prefetch_count(self, prefetch_count: usize) -> Self {
        Self { prefetch_count }
    }
}

// TODO: Use request IDs?
// TODO: empty and close channels on drop/shutdown
pub struct ConsumerHandle {
    pub sub_id: u64,
    pub config: ConsumerConfig,
    pub group: Option<Box<str>>,
    pub topic: Box<str>,
    pub partition: LogId,
    // TODO: find way to make this complete not on channel deliver, but response sent
    pub messages: tokio::sync::mpsc::Receiver<DeliverableMessage>,
    // TODO: Should it be ack only or generally respond?
    pub settler: tokio::sync::mpsc::Sender<SettleRequest>,
}

impl ConsumerHandle {
    pub async fn ack(&self, ack_request: SettleRequest) -> Result<(), BrokerError> {
        self.settler
            .send(ack_request)
            .await
            .map_err(|_| BrokerError::ChannelClosed)
    }

    pub async fn recv(&mut self) -> Option<DeliverableMessage> {
        self.messages.recv().await
    }
}

// TODO: Use request IDs?
// TODO: empty and close channels on drop/shutdown
pub struct PublisherHandle {
    pub publisher: tokio::sync::mpsc::Sender<PublishRequest>,
    // TODO: separate?
    confirm_tx: tokio::sync::mpsc::Sender<Result<Offset, BrokerError>>,
    task_group: Arc<TaskGroup>,
    topic: Topic,
    partition: LogId,

    pub(crate) msg_count: Arc<AtomicU64>,
}

// TODO: empty and close channels on drop/shutdown
pub struct ConfirmStream {
    rx: mpsc::Receiver<Result<Offset, BrokerError>>,
}

impl PublisherHandle {
    pub async fn publish(&self, payload: Vec<u8>) -> Result<(), BrokerError> {
        let (tx, rx) = oneshot::channel();

        self.publisher
            .send(PublishRequest { payload, reply: tx })
            .await
            .map_err(|err| {
                tracing::error!("Error sending publish request: {err}");
                BrokerError::ChannelClosed
            })?;

        let confirm_rx = self.confirm_tx.clone();

        // TODO: move deeper in? do, perhaps, inside a completion impl? At least test
        self.task_group.spawn(async move {
            match rx.await {
                // TODO: Better error handling, at least log
                Ok(Ok(offset)) => {
                    let _ = confirm_rx.send(Ok(offset)).await;
                }
                Err(err) => {
                    tracing::error!("Error sending confirm: {err:?}");
                    let _ = confirm_rx.send(Err(BrokerError::ChannelClosed)).await;
                }
                Ok(Err(err)) => {
                    tracing::error!("Error during confirm: {err}");
                    let _ = confirm_rx.send(Err(err)).await;
                }
            }
        });

        Ok(())
    }

    pub async fn shutdown(&self) {
        self.task_group.shutdown().await;
    }
}

impl ConfirmStream {
    pub async fn recv_confirm(&mut self) -> Option<Result<Offset, BrokerError>> {
        self.rx.recv().await
    }
}

struct DeliveryCtx {
    storage: Arc<dyn Storage>,
    group_state: Arc<TopicState>,
    topic: Topic,
    group: Option<Group>,
    partition: LogId,
    consumers: Vec<(ConsumerId, mpsc::Sender<DeliverableMessage>)>,
    ttl_deadline_delta_ms: u64,
    metrics: Arc<BrokerStats>,
    broker_config: BrokerConfig,
}

#[derive(Debug)]
struct TaskGroup {
    handles: SegQueue<tokio::task::JoinHandle<()>>,
    shutdown: AtomicBool,
}

impl TaskGroup {
    fn new() -> Self {
        Self {
            handles: SegQueue::new(),
            shutdown: AtomicBool::new(false),
        }
    }

    fn spawn<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Hard gate: no tasks after shutdown
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }

        let handle = tokio::spawn(fut);

        // Push only if still open
        if self.shutdown.load(Ordering::Acquire) {
            handle.abort(); // defensive, extremely rare
        } else {
            self.handles.push(handle);
        }
    }

    async fn shutdown(&self) {
        // Close the gate
        self.shutdown.store(true, Ordering::Release);

        // Drain deterministically
        while let Some(h) = self.handles.pop() {
            h.abort();
        }
    }
}

// TODO: Injectable clock for testing
#[derive(Debug)]
pub struct Broker<C: Coordination + Send + Sync + 'static> {
    pub config: BrokerConfig,
    storage: Arc<dyn Storage>,
    coord: Arc<C>,
    metrics: Arc<BrokerStats>,
    // TODO: Add partition to groups key for corrected and independent cursors
    groups: Arc<DashMap<(Topic, Option<Group>), Arc<TopicState>>>,
    topics: Arc<DashMap<(Topic, Option<Group>), ()>>,
    batchers: Arc<DashMap<(Topic, LogId, Option<Group>), mpsc::Sender<PublishRequest>>>,
    shutdown: CancellationToken,
    task_group: Arc<TaskGroup>,
    recovered_cursors: Arc<DashMap<(Topic, LogId, Option<Group>), GroupCursor>>,
    next_sub_id: AtomicU64,
}

impl<C: Coordination + Send + Sync + 'static> Broker<C> {
    pub async fn try_new(
        storage: Arc<impl Storage + 'static>,
        coord: C,
        metrics: Arc<BrokerStats>,
        config: BrokerConfig,
    ) -> Result<Self, BrokerError> {
        let shutdown = CancellationToken::new();

        if config.reset_inflight {
            storage.clear_all_inflight().await?;
        }

        let now_unix_ts = unix_millis();
        let expired = storage.list_expired(now_unix_ts).await?;

        let ttl_ms = config.inflight_ttl_secs * 1000;
        // Clear expired inflight entries on startup, to avoid stuck messages
        // Rest can be redelivered after TTL by redelivery worker
        for msg in expired {
            // storage
            //     .clear_inflight(&msg.topic, msg.partition, &msg.group, msg.offset)
            //     .await?;
            let deadline = now_unix_ts + ttl_ms;
            storage
                .add_to_redelivery(&msg.topic, msg.partition, &msg.group, msg.offset, deadline)
                .await?;
        }

        // Reconstructing cursor
        let groups = storage.list_groups().await?;
        let recovered_cursors = Arc::new(DashMap::new());

        for (topic, partition, group) in groups {
            let lowest = storage
                .lowest_unacked_offset(&topic, partition, &group)
                .await?;
            let cur = compute_start_offset(&storage, &topic, partition, &group, lowest).await?;

            debug_assert!(
                !storage
                    .is_inflight_or_acked(&topic, partition, &group, cur)
                    .await?,
                "recovered cursor points at inflight/acked offset"
            );

            recovered_cursors.insert((topic, partition, group), GroupCursor { next_offset: cur });
        }

        let broker = Broker {
            config,
            storage,
            metrics,
            coord: Arc::new(coord),
            groups: Arc::new(DashMap::new()),
            topics: Arc::new(DashMap::new()),
            batchers: Arc::new(DashMap::new()),
            shutdown,
            task_group: Arc::new(TaskGroup::new()),
            recovered_cursors,
            next_sub_id: AtomicU64::new(0),
        };

        broker.start_redelivery_worker();
        broker.start_cleanup_worker();

        Ok(broker)
    }

    pub async fn debug_upper(&self, topic: &str, partition: u32, group: &Option<Group>) -> u64 {
        self.storage
            .current_next_offset(&topic.to_string(), partition, group)
            .await
            .unwrap_or(0)
    }

    pub async fn dump_meta_keys(&self) {
        self.storage.dump_meta_keys().await;
    }

    pub async fn flush_storage(&self) -> Result<(), BrokerError> {
        self.storage.flush().await?;
        Ok(())
    }

    pub async fn forced_cleanup(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
    ) -> Result<(), BrokerError> {
        self.storage.cleanup_topic(topic, partition, group).await?;
        Ok(())
    }

    pub async fn get_publisher(
        &self,
        topic: &str,
        group: &Option<Group>,
    ) -> Result<(PublisherHandle, ConfirmStream), BrokerError> {
        let partition = 0;
        let (confirm_tx, confirm_rx) = mpsc::channel::<Result<Offset, BrokerError>>(1024 * 4);

        let batcher = self
            .get_or_create_batcher(&topic.to_string(), partition, group)
            .await;

        Ok((
            PublisherHandle {
                publisher: batcher,
                confirm_tx,
                task_group: self.task_group.clone(),
                topic: topic.to_string(),
                partition,
                msg_count: Arc::new(AtomicU64::new(0)),
            },
            ConfirmStream { rx: confirm_rx },
        ))
    }

    pub async fn publish(
        &self,
        topic: &str,
        group: &Option<Group>,
        payload: &[u8],
    ) -> Result<Offset, BrokerError> {
        let partition = 0;
        let (tx, rx) = oneshot::channel();

        let batcher = self
            .get_or_create_batcher(&topic.to_string(), partition, group)
            .await;

        batcher
            .send(PublishRequest {
                payload: payload.to_vec(),
                reply: tx,
            })
            .await
            .map_err(|_| BrokerError::Unknown("batcher closed".into()))?;

        let offset = rx
            .await
            .map_err(|_| BrokerError::Unknown("batcher died".into()))??;

        Ok(offset)
    }

    pub async fn publish_async(
        self: &Arc<Self>,
        topic: &str,
        partition: LogId,
        group: &Option<Group>,
        payload: &[u8],
    ) -> Result<oneshot::Receiver<Result<Offset, BrokerError>>, BrokerError> {
        let partition = 0;
        let (tx, rx) = oneshot::channel();
        let (response_tx, response_rx) = oneshot::channel();
        let broker = self.clone();

        let topic: Box<str> = topic.into();
        let payload = payload.to_vec();
        let group = group.clone();

        self.task_group.spawn(async move {
            let res: Result<u64, BrokerError> = async move {
                let batcher = broker
                    .get_or_create_batcher(&topic.to_string(), partition, &group)
                    .await;

                batcher
                    .send(PublishRequest {
                        payload: payload.to_vec(),
                        reply: tx,
                    })
                    .await
                    .map_err(|_| BrokerError::Unknown("batcher closed".into()))?;

                let offset = rx
                    .await
                    .map_err(|_| BrokerError::Unknown("batcher died".into()))??;

                Ok(offset)
            }
            .await;

            let _ = response_tx.send(res);
        });

        Ok(response_rx)
    }

    pub async fn subscribe(
        &self,
        topic: &str,
        group: Option<&str>,
        cfg: ConsumerConfig,
    ) -> Result<ConsumerHandle, BrokerError> {
        let prefetch_count = if cfg.prefetch_count == 0 {
            1024
        } else {
            cfg.prefetch_count
        };
        let sub_id = self.next_sub_id.fetch_add(1, Ordering::SeqCst);
        self.topics
            .entry((topic.to_string(), group.map(|s| s.into())))
            .or_insert(());

        self.storage
            .register_group(&topic.to_string(), 0, &group.map(|g| g.into()))
            .await?;

        let topic_clone = topic.to_string();
        let group_clone = group.map(|g| g.to_string());
        let key = (topic_clone.clone(), group_clone.clone());

        let (msg_tx, msg_rx) = mpsc::channel(prefetch_count);
        let (ack_tx, mut ack_rx) = mpsc::channel::<SettleRequest>(prefetch_count);
        let ack_tx_clone = ack_tx.clone();

        // Register consumer in group state
        let entry = self.groups.entry(key.clone());

        let partition = 0;
        let group_state_arc = match entry {
            dashmap::mapref::entry::Entry::Occupied(e) => e.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let gs = Arc::new(TopicState {
                    inflight_sem: Arc::new(Semaphore::new(prefetch_count)),
                    ..TopicState::new()
                });

                if let Some((_key, cursor)) = self.recovered_cursors.remove(&(
                    topic_clone.clone(),
                    partition,
                    group_clone.clone(),
                )) {
                    gs.next_offset.store(cursor.next_offset, Ordering::SeqCst);
                } else {
                    // brand-new group => start from 0 (or retention floor)
                    let cur = compute_start_offset(
                        &self.storage,
                        &topic_clone,
                        partition,
                        &group_clone,
                        0,
                    )
                    .await?;
                    gs.next_offset.store(cur, Ordering::SeqCst);
                }
                v.insert(gs).clone()
            }
        };

        let consumer_id = group_state_arc.rr_counter.fetch_add(1, Ordering::SeqCst);
        group_state_arc
            .consumers
            .insert(consumer_id, msg_tx.clone());
        group_state_arc.signal();

        // Start delivery task once per (topic, group)
        if !group_state_arc
            .delivery_task_started
            .swap(true, Ordering::SeqCst)
        {
            let storage = Arc::clone(&self.storage);
            let _coord = self.coord.clone();
            let groups = self.groups.clone();

            let ttl = self.config.inflight_ttl_secs;
            let shutdown = self.shutdown.clone();

            let metrics = self.metrics.clone();
            let broker_config = self.config.clone();
            self.task_group.spawn(async move {
                // TODO: Keep track of still unacked messages, redeliver on consumer disconnect
                // Dedicated delivery loop for this (topic, group)
                loop {
                    if shutdown.is_cancelled() {
                        break;
                    }

                    // ⚠️ DO NOT add storage scans here. This loop must stay O(1).

                    // Lookup group_state each iteration so we see updated consumers
                    let group_state_opt = groups.get(&key);
                    let group_state = match group_state_opt {
                        Some(g) => g.value().clone(),
                        None => {
                            // Group removed; nothing to do
                            break;
                        }
                    };

                    let partition = 0;

                    // TODO: replace with leadership watch / notification
                    // if !coord.is_leader(topic_clone.clone(), partition).await {
                    //     tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    //     continue;
                    // }

                    // Snapshot consumer list
                    let consumers: Vec<(ConsumerId, mpsc::Sender<DeliverableMessage>)> =
                        group_state
                            .consumers
                            .iter()
                            .map(|entry| (*entry.key(), entry.value().clone()))
                            .collect();

                    if consumers.is_empty() {
                        wait_for_event(&group_state).await;
                        continue;
                    }

                    let ctx = DeliveryCtx {
                        storage: storage.clone(),
                        group_state: group_state.clone(),
                        topic: topic_clone.to_owned(),
                        group: group_clone.to_owned(),
                        partition,
                        consumers: consumers.clone(),
                        ttl_deadline_delta_ms: ttl * 1000,
                        metrics: metrics.clone(),
                        broker_config: broker_config.clone(),
                    };

                    if process_redeliveries(&ctx).await {
                        continue;
                    }

                    if process_fresh_deliveries(&ctx).await {
                        continue;
                    }

                    wait_for_event(&ctx.group_state).await;
                }

                tracing::debug!("Disconnect, Ending Sub");
            });
        }

        // Ack handler for this consumer
        let storage_clone = Arc::clone(&self.storage);

        let shutdown = self.shutdown.clone();

        let group_state = group_state_arc.clone();

        let metrics = self.metrics.clone();
        let task_group_clone = self.task_group.clone();
        let topic_clone: Box<str> = topic.into();
        let group_clone: Option<Box<str>> = group.map(|g| g.into());
        self.task_group.spawn(async move {
            loop {
                tokio::select! {
                        biased;

                        _ = shutdown.cancelled() => {
                            break;
                        }

                        Some(off) = ack_rx.recv() => {
                            let tag = off.delivery_tag;

                            debug_assert!(
                                !group_state_arc.inflight_permits_by_tag.is_empty(),
                                "ACK received but no inflight messages exist {tag:?}"
                            );

                            if group_state_arc.inflight_permits_by_tag.is_empty() {
                                tracing::warn!("ACK received but no inflight messages exist {tag:?}");
                            }

                            // ONLY accept ACKs for messages that are actually inflight
                            if let Some((_, (offset, permit))) =
                                group_state_arc.inflight_permits_by_tag.remove(&tag)
                            {
                                // free capacity
                                drop(permit);
                                group_state_arc.delivery_tag_by_offset.remove(&offset);
                                let (completion, receiver) = BrokerCompletionPair::pair();
                                match storage_clone.ack_enqueue(
                                    &topic_clone,
                                    partition,
                                    &group_clone.clone().map(|s| s.into()),
                                    offset,
                                    completion,
                                ).await {
                                    Ok(()) => {},
                                    Err(err) => {
                                        tracing::error!("Error acknowledging message: {err}");
                                        // TODO: Send error back? Retry?
                                    },
                                };
                                let group_state_arc = group_state_arc.clone();
                                let metrics = metrics.clone();
                                task_group_clone.spawn(async move {
                                    let _ = receiver.await;
                                    // wake delivery loop (capacity changed)
                                    group_state_arc.signal();
                                    metrics.acked();
                                    // TODO: handle error?
                                });
                            } else {
                                // ACK before delivery -> ignore
                                // (optional todo: metrics / trace)
                                // metrics.ack_before_delivery();
                            }
                        }
                }
            }
        });

        group_state.signal();

        Ok(ConsumerHandle {
            sub_id,
            group: group.map(|g| g.into()),
            topic: topic.into(),
            partition,
            config: cfg,
            messages: msg_rx,
            settler: ack_tx_clone,
        })
    }

    fn start_cleanup_worker(&self) {
        let topics = self.topics.clone();
        let keys = self
            .groups
            .iter()
            .map(|kv| {
                let (tp, g) = kv.key();
                (tp.clone(), g.clone())
            })
            .collect::<Vec<_>>();
        // TODO: List partitions too
        let storage = self.storage.clone();
        let cleanup_interval_secs = self.config.cleanup_interval_secs;
        let shutdown = self.shutdown.clone();
        self.task_group.spawn(async move {
            loop {
                if shutdown.is_cancelled() {
                    break;
                }

                tokio::time::sleep(std::time::Duration::from_secs(cleanup_interval_secs)).await;

                for (topic, group) in keys.iter() {
                    // TODO: Handle partition better
                    let partition = 0;
                    if let Err(err) = storage
                        .cleanup_topic(&topic.to_string(), partition, group)
                        .await
                    {
                        tracing::error!("Error in cleanup worker: {}", err);
                    } else {
                        tracing::info!(
                            "Successfully cleaned up: {} {}",
                            &topic,
                            group.clone().unwrap_or_default()
                        )
                    }
                }
            }
        });
    }

    fn start_redelivery_worker(&self) {
        let storage = Arc::clone(&self.storage);
        let coord = self.coord.clone();

        let groups = self.groups.clone();
        let shutdown = self.shutdown.clone();
        let metrics = self.metrics.clone();
        let task_group = self.task_group.clone();
        let ttl_ms = self.config.inflight_ttl_secs * 1000;
        self.task_group.spawn(async move {
            loop {
                if shutdown.is_cancelled() {
                    break;
                }

                //read hint; if none, just sleep a bit or await notify
                let hint = match storage.next_expiry_hint().await {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::error!("Error: {}", err);
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };

                if let Some(ts) = hint {
                    let now = unix_millis();
                    if ts > now {
                        tokio::time::sleep(std::time::Duration::from_millis(ts - now)).await;
                        continue;
                    }
                } else {
                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    continue;
                }

                // (we believe something may be expired now -> scan expired + process
                let now = unix_millis();
                let expired = match storage.list_expired(now).await {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::error!("Error: {}", err);
                        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
                        continue;
                    }
                };

                let mut handles = vec![];
                for msg in expired {
                    if !coord.is_leader(msg.topic.clone(), msg.partition).await {
                        continue;
                    }

                    // TODO adjust to handle more than one expired message per group
                    if let Some(gs) = groups.get(&(msg.topic.clone(), msg.group.clone())) {
                        let current = gs.next_offset.load(Ordering::SeqCst);

                        debug_assert!(
                            msg.offset < current,
                            "expired offset >= next_offset (would duplicate): off={} next={}",
                            msg.offset,
                            current
                        );

                        let expired_offset = msg.offset;

                        if expired_offset >= current {
                            continue;
                        }

                        // TODO: Maybe check if settled/enqueued
                        let is_acked = storage
                            .is_acked(&msg.topic, msg.partition, &msg.group, expired_offset)
                            .await
                            .unwrap_or(true); // conservative

                        if is_acked {
                            continue; // nothing to do
                        }
                        // now we know it still matters
                        let storage = storage.clone();
                        let metrics = metrics.clone();
                        let gs = gs.value().clone();
                        let (txn, rxn) = tokio::sync::oneshot::channel();
                        task_group.spawn(async move {
                            let deadline = unix_millis() + ttl_ms;
                            // let _ = storage
                            //     .clear_inflight(&msg.topic, msg.partition, &msg.group, msg.offset)
                            //     .await;
                            let _ = storage
                                .add_to_redelivery(
                                    &msg.topic,
                                    msg.partition,
                                    &msg.group,
                                    expired_offset,
                                    deadline,
                                )
                                .await;

                            if let Some((_, tag)) =
                                gs.delivery_tag_by_offset.remove(&expired_offset)
                            {
                                // Only remove if above is true
                                if let Some((_tag, (_off, permit))) =
                                    gs.inflight_permits_by_tag.remove(&tag)
                                {
                                    drop(permit);
                                }
                            }
                            gs.redelivery.push(msg.offset);
                            metrics.expired();
                            gs.signal();
                            let _ = txn.send(());
                        });
                        handles.push(rxn);
                    }
                }

                futures::future::join_all(handles).await;

                // recompute hint after processing so next sleep is accurate
                match storage.recompute_and_store_next_expiry_hint().await {
                    Ok(v) => v,
                    Err(err) => {
                        tracing::error!("Error: {}", err);
                        tokio::time::sleep(std::time::Duration::from_millis(15)).await;
                        continue;
                    }
                };

                // tokio::time::sleep(std::time::Duration::from_millis(3)).await;
                tokio::task::yield_now().await;
            }
        });
    }

    async fn get_or_create_batcher(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
    ) -> mpsc::Sender<PublishRequest> {
        let key = (topic.clone(), partition, group.clone());

        self.topics
            .entry((topic.clone(), group.clone()))
            .or_insert(());

        match self.batchers.entry(key) {
            dashmap::Entry::Occupied(e) => e.get().clone(),
            dashmap::Entry::Vacant(v) => {
                // TODO: Use config
                let (tx, rx) = mpsc::channel::<PublishRequest>(1024 * 16);
                v.insert(tx.clone());
                self.spawn_batcher(topic.clone(), partition, group.clone(), rx);
                tx
            }
        }
    }

    // NOTE: Fast processing partly depends on confirms being consumed quickly
    fn spawn_batcher(
        &self,
        topic: Topic,
        partition: LogId,
        group: Option<Group>,
        mut rx: mpsc::Receiver<PublishRequest>,
    ) {
        let storage = Arc::clone(&self.storage);
        let cfg = self.config.clone();
        let shutdown = self.shutdown.clone();
        let groups = self.groups.clone();

        let metrics = self.metrics.clone();
        self.task_group.spawn(async move {
            loop {
                let metrics = metrics.clone();
                tokio::select! {
                    _ = shutdown.cancelled() => break,

                    maybe = rx.recv() => {
                        match maybe {
                            Some(PublishRequest { payload, reply }) => {
                                let (completion, rx) = BrokerCompletionPair::pair();
                                match storage.append_enqueue(&topic, partition, &group, &payload, completion).await {
                                    Ok(()) => {
                                        // TODO: move all this logic to the publisher?
                                        // TODO: Single task to await completions with queue?
                                        let topic = topic.clone();
                                        let groups = groups.clone();
                                        tokio::spawn(async move {
                                            match rx.await {
                                                Ok(res) => {
                                                    // success
                                                    if res.is_ok() {
                                                        metrics.published();
                                                        // metrics.publish_payload(payload.len() as u64);
                                                    } else {
                                                        tracing::error!("Error waiting append enqueue: {res:?}");
                                                    }
                                                    let _ = reply.send(res.map(|r| r.base_offset).map_err(|_e| BrokerError::ChannelClosed));
                                                },
                                                Err(err) => {
                                                    tracing::error!("Error waiting append enqueue level 1: {err}");
                                                    // append failed
                                                    let _ = reply.send(Err(BrokerError::ChannelClosed));
                                                    return;
                                                }
                                            }
                                            for entry in groups.iter() {
                                                let ((t, _g), gs) = entry.pair();
                                                if t == &topic {
                                                    gs.signal();
                                                }
                                            }
                                        });
                                    },
                                    Err(err) => {
                                        let _ = reply.send(Err(err.into()));
                                    },
                                }
                            }

                            None => {
                                // channel closed
                                break;
                            }
                        }
                    }
                }
            }
        });
    }

    pub async fn shutdown(&self) {
        // Close batchers: this causes their tasks to exit naturally
        self.batchers.clear();

        // Clear groups so delivery loops exit
        self.groups.clear();

        // Signal shutdown to background tasks
        self.shutdown.cancel();
        self.task_group.shutdown().await;

        // Give tasks time to see the closed channels & exit
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
}

async fn flush_publish_batch(
    storage: &Arc<dyn Storage>,
    groups: Arc<DashMap<(String, String), Arc<TopicState>>>,
    topic: &Topic,
    partition: u32,
    group: &Option<Group>,
    pending: &mut Vec<PublishRequest>,
    metrics: Arc<BrokerStats>,
) {
    if pending.is_empty() {
        return;
    }

    let payloads: Vec<Vec<u8>> = pending
        .iter_mut()
        .map(|r| std::mem::take(&mut r.payload))
        .collect();

    let batch_size = payloads.len();
    let bytes = payloads.iter().map(|p| p.len()).sum::<usize>();

    let result = storage
        .append_batch(topic, partition, group, &payloads)
        .await;

    match result {
        Ok(offsets) => {
            metrics.published_many(offsets.len() as u64);
            metrics.publish_batch(batch_size, bytes);
            for (req, off) in pending.drain(..).zip(offsets) {
                let _ = req.reply.send(Ok(off));
            }

            for entry in groups.iter() {
                let ((t, _g), gs) = entry.pair();
                if t == topic {
                    gs.signal();
                }
            }
        }
        Err(err) => {
            // On failure, return error to all callers
            let msg = format!("batch append failed: {err}");
            // We can't cheaply clone error; create per-recipient error
            for req in pending.drain(..) {
                let _ = req
                    .reply
                    .send(Err(BrokerError::BatchAppendFailed(msg.clone())));
            }
        }
    }
}

async fn flush_ack_batch(
    storage: &Arc<dyn Storage>,
    topic: &str,
    group: &Option<&str>,
    buf: &mut Vec<Offset>,
    metrics: Arc<BrokerStats>,
) {
    if buf.is_empty() {
        return;
    }
    // best-effort; you can log errors
    let partition = 0;
    let batch_size = buf.len();
    if storage
        .ack_batch(topic, partition, &group.map(|s| s.into()), buf)
        .await
        .is_ok()
    {
        metrics.acked_many(buf.len() as u64);
        metrics.ack_batch(batch_size, batch_size);
    }
    buf.clear();
}

async fn advance_cursor(
    storage: &Arc<dyn Storage>,
    topic: &Topic,
    partition: LogId,
    group: &Option<Group>,
    mut cur: Offset,
) -> Result<Offset, StorageError> {
    let upper = storage.current_next_offset(topic, partition, group).await?;

    while cur < upper {
        // If it is inflight or acked, it can never be delivered again
        if storage
            .is_inflight_or_acked(topic, partition, group, cur)
            .await?
        {
            cur += 1;
            continue;
        }

        // If it is NOT enqueued yet, we must stop here
        if !storage.is_enqueued(topic, partition, group, cur).await? {
            break;
        }

        // Otherwise: enqueued + not inflight + not acked → deliverable
        break;
    }

    Ok(cur)
}

async fn compute_start_offset(
    storage: &Arc<impl Storage + ?Sized>,
    topic: &str,
    partition: u32,
    group: &Option<Group>,
    mut cur: u64,
) -> Result<u64, StorageError> {
    let upper = storage
        .current_next_offset(&topic.to_string(), partition, group)
        .await?;

    while cur < upper {
        if storage
            .is_inflight_or_acked(&topic.to_string(), partition, group, cur)
            .await?
        {
            cur += 1;
        } else {
            break;
        }
    }
    Ok(cur)
}

async fn process_redeliveries(ctx: &DeliveryCtx) -> bool {
    let DeliveryCtx {
        storage,
        group_state,
        topic,
        group,
        consumers,
        ttl_deadline_delta_ms: ttl_ms,
        metrics,
        ..
    } = ctx;

    let mut delivered_any = false;
    let mut inflight_batch: Vec<(DeliveryTag, Offset, UnixMillis)> = Vec::new();
    let partition = 0;

    // println!("Processing Redelivery");
    // println!(
    //     "Processing Redelivery: len {}",
    //     group_state.redelivery.len()
    // );

    let mut to_redeliver = Vec::new();

    let new_deadline = unix_millis() + ttl_ms;
    while let Some(expired_offset) = group_state.redelivery.pop() {
        // println!("Processing Redelivery pop");
        if let Ok(msg) = storage
            .fetch_by_offset(topic, partition, group, expired_offset)
            .await
        {
            if let Ok(false) = storage.is_enqueued(topic, partition, group, msg.offset).await {
                // this is a serious invariant violation; log loudly
                // println!("Enqueued?");
                tracing::error!("redelivery offset not enqueued: off={}", msg.offset);
                continue;
            }


            // let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;
            // if consumers.is_empty() {
            //     break;
            // }
            // let (cid, tx) = &consumers[rr % consumers.len()];

            let permit = match group_state.inflight_sem.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => {
                    // println!("No capacity while sending redelivered");
                    // no capacity; put it back and stop
                    group_state.redelivery.push(expired_offset);
                    break;
                }
            };

            let epoch = group_state.tag_counter.fetch_add(1, Ordering::SeqCst);
            let tag = DeliveryTag { epoch };
            group_state.delivery_tag_by_offset.insert(msg.offset, tag);
            // println!("Sending redelivered: {expired_offset}");
            to_redeliver.push((
                DeliverableMessage {
                    message: msg,
                    delivery_tag: tag,
                    group: group.clone(),
                },
                permit,
            ));
            inflight_batch.push((tag, expired_offset, new_deadline));
            // if tx
            //     .send(DeliverableMessage {
            //         message: msg,
            //         delivery_tag: tag,
            //         group: group.clone(),
            //     })
            //     .await
            //     .is_err()
            // {
            //     // println!("Failed sending redelivered: {expired_offset}");
            //     drop(permit);
            //     group_state.consumers.remove(cid);
            //     group_state.signal(); // consumer set changed
            //     // requeue the message for someone else later
            //     group_state.redelivery.push(expired_offset);
            // } else {
            //     // println!("Succeeded sending redelivered: {expired_offset}");
            //     // group_state.inflight_permits.remove(&tag);
            //     inflight_batch.push((tag, expired_offset, new_deadline, permit));
            //     delivered_any = true;
            //     metrics.redelivered();
            //     metrics.delivered();
            // }
        } else {
            tracing::error!("Offset not found: {expired_offset}");
        }
    }

    if inflight_batch.is_empty() {
        return delivered_any;
    }

    let res = storage
        .mark_inflight_batch(
            topic,
            0,
            group,
            &inflight_batch
                .iter()
                .map(|(_t, o, d)| (*o, *d))
                .collect::<Vec<_>>(),
        )
        .await;

    if let Err(err) = res {
        tracing::error!("Failed to mark inflight during redelivery : {err}");
        // Not durable: release capacity and requeue offsets
        for (tag, offset, _deadline) in inflight_batch.drain(..) {
            group_state.inflight_permits_by_tag.remove(&tag);
            // drop(permit);
            group_state.redelivery.push(offset);
        }
        // make sure we run again promptly
        group_state.signal();
        // optional tiny backoff if storage is dying
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        return true;
    }

    for (msg, permit) in to_redeliver {
        let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;
        if consumers.is_empty() {
            break;
        }
        let (cid, tx) = &consumers[rr % consumers.len()];

        let offset = msg.message.offset;
        let tag = msg.delivery_tag;
        group_state
            .inflight_permits_by_tag
            .insert(msg.delivery_tag, (msg.message.offset, permit));
        group_state
            .delivery_tag_by_offset
            .insert(msg.message.offset, msg.delivery_tag);
        if tx.send(msg).await.is_err() {
            // println!("Failed sending redelivered: {expired_offset}");
            group_state.inflight_permits_by_tag.remove(&tag);
            group_state.delivery_tag_by_offset.remove(&offset);
            group_state.consumers.remove(cid);
            group_state.signal(); // consumer set changed
            // requeue the message for someone else later
            group_state.redelivery.push(offset);
        } else {
            // println!("Succeeded sending redelivered: {expired_offset}");
            // group_state.inflight_permits.remove(&tag);
            delivered_any = true;
            metrics.redelivered();
            metrics.delivered();
        }
    }

    // Durable: keep permits held by inflight_permits
    // for (tag, offset, _deadline) in inflight_batch.drain(..) {
    //     // TODO: make sure it happened earlier as much as possible, eval whether we need to replace tags
    //     group_state
    //         .inflight_permits_by_tag
    //         .insert(tag, (offset, permit));
    //     group_state.delivery_tag_by_offset.insert(offset, tag);
    // }

    delivered_any
}

async fn process_fresh_deliveries(ctx: &DeliveryCtx) -> bool {
    let DeliveryCtx {
        storage,
        group_state,
        topic,
        group,
        consumers,
        ttl_deadline_delta_ms,
        metrics,
        partition,
        ..
    } = ctx;

    if consumers.is_empty() {
        return false;
    }

    // capacity gate
    let available = group_state
        .inflight_sem
        .available_permits()
        .min(ctx.broker_config.inflight_batch_size);
    if available == 0 || !group_state.redelivery.is_empty() {
        return false;
    }

    let start = group_state.next_offset.load(Ordering::SeqCst);
    let upper = match storage.current_next_offset(topic, *partition, group).await {
        Ok(v) => v,
        Err(_) => return false,
    };

    let partition = 0;
    let mut msgs = match storage
        .fetch_available_clamped(topic, partition, group, start, upper, available)
        .await
    {
        Ok(v) if !v.is_empty() => v,
        _ => return false,
    };

    // ---- phase 1: acquire permits + build inflight batch
    let mut entries = Vec::with_capacity(msgs.len());
    let mut permits = Vec::with_capacity(msgs.len());

    for msg in &mut msgs {
        match group_state.inflight_sem.clone().acquire_owned().await {
            Ok(p) => {
                let epoch = group_state.tag_counter.fetch_add(1, Ordering::SeqCst);
                let delivery_tag = DeliveryTag { epoch };
                group_state
                    .delivery_tag_by_offset
                    .insert(msg.offset, delivery_tag);
                permits.push((delivery_tag, p));
                entries.push((msg.offset, unix_millis() + ttl_deadline_delta_ms));
            }
            Err(_) => break,
        }
    }

    if entries.is_empty() {
        return false;
    }

    // ---- phase 2: durable inflight
    let mark_res = storage
        .mark_inflight_batch(topic, partition, group, &entries)
        .await;
    if let Err(err) = mark_res
    {
        tracing::error!("Error marking deliveries as inflight : {err}");
        // release permits
        for (_, p) in permits {
            drop(p);
        }
        return false;
    }

    // ---- phase 3: deliver + register permits
    let mut max_off = None;

    for ((tag, permit), msg) in permits.into_iter().zip(msgs.into_iter()) {
        let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;
        let idx = rr % consumers.len();
        let (_cid, tx) = &consumers[idx];

        let offset = msg.offset;
        let deliverable_msg = DeliverableMessage {
            message: msg,
            delivery_tag: tag,
            group: group.clone(),
        };
        let send_res = tx.send(deliverable_msg).await;
        if let Err(err) = send_res {
            tracing::error!("Failure to deliver message {} : {}", offset, err);
            drop(permit);
            group_state.consumers.remove(&consumers[idx].0);
            group_state.signal();
            // best effort: clear inflight later via expiry
            continue;
        }

        group_state
            .inflight_permits_by_tag
            .insert(tag, (offset, permit));
        max_off = Some(offset);
        metrics.delivered();
    }

    if let Some(max_off) = max_off {
        let base = group_state.next_offset.load(Ordering::SeqCst);
        let seed = base.max(max_off + 1);

        match advance_cursor(storage, topic, partition, group, seed).await {
            Ok(new_cur) => {
                group_state.next_offset.store(new_cur, Ordering::SeqCst);
            }
            Err(err) => {
                tracing::error!("failed to advance cursor: {err}");
                // conservative: do not advance
            }
        };
    }

    true
}

#[inline]
async fn wait_for_event(group_state: &TopicState) {
    // wait until at least one event happens
    let permit = group_state.events.acquire().await;
    if let Ok(p) = permit {
        p.forget(); // consume exactly 1 event
    }
}
