use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use fibril_metrics::{BrokerStats, QueuesStateSnapshot};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use fibril_storage::{DeliverableMessage, DeliveryTag, Group, LogId, Offset, StorageError, Topic};
use fibril_util::unix_millis;

use crate::queue_engine::{QueueEngine, SettleKind, SettleRequest as EngineSettleRequest};
use stroma_core::{
    AppendCompletion, AppendResult, CompletionPair, IoError, KeratinAppendCompletion, StromaError,
};

#[derive(thiserror::Error, Debug)]
pub enum BrokerError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("engine error: {0}")]
    Engine(#[from] StromaError),

    #[error("channel closed")]
    ChannelClosed,

    #[error("unknown: {0}")]
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum SettleType {
    Ack,
    Nack { requeue: Option<bool> },
    Reject { requeue: Option<bool> },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SettleRequest {
    pub settle_type: SettleType,
    pub delivery_tag: DeliveryTag,
}

impl SettleRequest {
    pub fn is_ack(&self) -> bool {
        matches!(self.settle_type, SettleType::Ack)
    }

    pub fn is_nack(&self) -> bool {
        matches!(self.settle_type, SettleType::Nack { .. })
    }

    pub fn is_reject(&self) -> bool {
        matches!(self.settle_type, SettleType::Reject { .. })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerConfig {
    pub prefetch: usize,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self { prefetch: 32 }
    }
}

impl ConsumerConfig {
    pub fn with_prefetch_count(mut self, count: usize) -> Self {
        self.prefetch = count;
        self
    }
}

pub struct ConsumerHandle {
    pub sub_id: u64,
    pub config: ConsumerConfig,
    pub group: Option<Box<str>>,
    pub topic: Box<str>,
    pub partition: LogId,
    pub messages: mpsc::Receiver<DeliverableMessage>,
    pub settler: mpsc::Sender<SettleRequest>,
    pub pending_settles: Arc<AtomicUsize>,
}

impl ConsumerHandle {
    pub async fn settle(&self, req: SettleRequest) -> Result<(), BrokerError> {
        if self.settler.is_closed() {
            tracing::debug!("Settle channel is closed for consumer {}", self.sub_id);
        }
        let s = self.pending_settles.fetch_add(1, Ordering::AcqRel);
        tracing::debug!("Pending settles incremented to {}", s + 1);
        self.settler.send(req).await.map_err(|_| {
            let s = self.pending_settles.fetch_sub(1, Ordering::AcqRel);
            tracing::debug!(
                "Pending settles decremented to {} due to send failure",
                s - 1
            );
            BrokerError::ChannelClosed
        })
    }

    pub async fn recv(&mut self) -> Option<DeliverableMessage> {
        self.messages.recv().await
    }

    pub fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

pub struct PublishRequest {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Result<Offset, BrokerError>>,
    pub require_confirm: bool,
}

#[derive(Debug, Clone)]
pub struct PublisherHandle {
    pub publisher: mpsc::Sender<PublishRequest>,
}

impl PublisherHandle {
    pub async fn publish(
        &self,
        payload: &[u8],
    ) -> Result<oneshot::Receiver<Result<u64, BrokerError>>, BrokerError> {
        let (tx, rx) = oneshot::channel();

        self.publisher
            .send(PublishRequest {
                payload: payload.to_vec(),
                reply: tx,
                require_confirm: true,
            })
            .await
            .map_err(|_| BrokerError::ChannelClosed)?;

        // // TODO: move to separare task per publisher
        // tokio::spawn(async move {
        //     if let Err(e) = rx.await {
        //         tracing::error!("Error receiving publish response: {e:?}");
        //     }
        // });

        Ok(rx)
    }

    pub async fn publish_no_confirm(
        &self,
        payload: &[u8],
    ) -> Result<oneshot::Receiver<Result<u64, BrokerError>>, BrokerError> {
        let (tx, rx) = oneshot::channel();

        self.publisher
            .send(PublishRequest {
                payload: payload.to_vec(),
                reply: tx,
                require_confirm: false,
            })
            .await
            .map_err(|_| BrokerError::ChannelClosed)?;

        // // TODO: move to separare task per publisher
        // tokio::spawn(async move {
        //     if let Err(e) = rx.await {
        //         tracing::error!("Error receiving publish response: {e:?}");
        //     }
        // });

        Ok(rx)
    }
}

pub struct ConfirmStream {
    rx: mpsc::Receiver<Offset>,
}

impl ConfirmStream {
    pub async fn recv(&mut self) -> Option<Offset> {
        self.rx.recv().await
    }

    pub fn is_empty(&self) -> bool {
        self.rx.is_empty()
    }
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

    fn spawn<F>(&self, name: &'static str, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Hard gate: no tasks after shutdown
        if self.shutdown.load(Ordering::Acquire) {
            return;
        }

        #[cfg(tokio_unstable)]
        let handle = tokio::task::Builder::new().name(name).spawn(fut).unwrap();

        #[cfg(not(tokio_unstable))]
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

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub inflight_ttl_ms: u64,
    pub expiry_poll_min_ms: u64,
    pub expiry_batch_max: usize,
    pub delivery_poll_max_ms: u64,
}
impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            inflight_ttl_ms: 60_000,
            expiry_poll_min_ms: 200,
            expiry_batch_max: 8192,
            delivery_poll_max_ms: 500,
        }
    }
}

// ---------------- Internal state ----------------

type ConsumerId = u64;

#[derive(Debug)]
struct ConsumerState {
    id: ConsumerId,
    tx: mpsc::Sender<DeliverableMessage>,
    // flow control
    prefetch: AtomicUsize,
    inflight: AtomicUsize,
}

impl ConsumerState {
    fn can_accept(&self) -> bool {
        self.inflight.load(Ordering::Acquire) < self.prefetch.load(Ordering::Acquire)
    }
    fn inc_inflight(&self) {
        self.inflight.fetch_add(1, Ordering::AcqRel);
    }
    fn dec_inflight(&self) {
        self.inflight.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct QueueKey {
    tp: Topic,
    part: LogId,
    group: Option<Group>,
}

/// Tag -> delivery record (so we can validate settle)
#[derive(Debug, Clone)]
struct TagRecord {
    key: QueueKey,
    offset: Offset,
    consumer_id: ConsumerId,
}

/// One loop per queue (tp,part,group)
#[derive(Debug)]
struct QueueLoopState {
    started: AtomicBool,
    rr: AtomicU64,
    consumers: DashMap<ConsumerId, Arc<ConsumerState>>,
    // used to wake the delivery loop
    notify: tokio::sync::Notify,
    epoch: AtomicU64,
}

impl QueueLoopState {
    fn new() -> Self {
        Self {
            started: AtomicBool::new(false),
            rr: AtomicU64::new(0),
            consumers: DashMap::new(),
            notify: tokio::sync::Notify::new(),
            epoch: AtomicU64::new(1),
        }
    }
    fn wake(&self) {
        self.epoch.fetch_add(1, Ordering::Release);
        self.notify.notify_one();
    }

    fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }
}

#[derive(Debug, Clone)]
enum WakeReason {
    Notify,
    Timer,
}

impl std::fmt::Display for WakeReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WakeReason::Notify => write!(f, "notify"),
            WakeReason::Timer => write!(f, "timer"),
        }
    }
}

// ---------------- Broker ----------------

// TODO cleanup old?
pub struct Broker<E: QueueEngine + std::fmt::Debug + Send + Sync + 'static> {
    cfg: BrokerConfig,
    engine: E,
    shutdown_publishers: CancellationToken,
    shutdown_consumers: CancellationToken,
    shutdown_settle: CancellationToken,
    shutdown_expiry: CancellationToken,

    next_sub_id: AtomicU64,
    next_consumer_id: AtomicU64,
    next_tag_epoch: AtomicU64,

    queues: DashMap<QueueKey, Arc<QueueLoopState>>,
    records_by_tags: DashMap<DeliveryTag, TagRecord>,
    tags_by_key_offset: DashMap<(QueueKey, Offset), DeliveryTag>,

    pending_settles: Arc<AtomicUsize>,
    settle_drained: Arc<Notify>,

    task_group: Arc<TaskGroup>,

    metrics: Option<Arc<BrokerStats>>,
}

impl<E: QueueEngine + std::fmt::Debug + Clone + Send + Sync + 'static> Broker<E> {
    pub fn new(engine: E, cfg: BrokerConfig, metrics: Option<Arc<BrokerStats>>) -> Arc<Self> {
        let metrics_clone = metrics.clone();
        if let Some(metrics) = metrics_clone {
            metrics.register_queue_state_callback(Some(Arc::new({
                let engine = engine.clone();
                move || {
                    let engine = engine.clone();
                    Box::pin(async move {
                        match engine.queue_stats_snapshot().await {
                            Ok(stats) => stats,
                            Err(e) => {
                                tracing::error!(
                                    "Failed to get queue stats snapshot for metrics: {e:?}"
                                );
                                QueuesStateSnapshot {
                                    queues: std::collections::HashMap::new(),
                                }
                            }
                        }
                    })
                }
            })));
        }
        let this = Arc::new(Self {
            cfg,
            engine,
            shutdown_publishers: CancellationToken::new(),
            shutdown_consumers: CancellationToken::new(),
            shutdown_settle: CancellationToken::new(),
            shutdown_expiry: CancellationToken::new(),
            next_sub_id: AtomicU64::new(1),
            next_consumer_id: AtomicU64::new(1),
            next_tag_epoch: AtomicU64::new(1),
            queues: DashMap::new(),
            records_by_tags: DashMap::new(),
            tags_by_key_offset: DashMap::new(),
            pending_settles: Arc::new(AtomicUsize::new(0)),
            settle_drained: Arc::new(Notify::new()),
            task_group: Arc::new(TaskGroup::new()),
            metrics,
        });

        // expiry worker: keeps Stroma turning inflight -> ready again
        Self::spawn_expiry_worker(this.clone());

        this
    }

    pub async fn shutdown(&self) {
        self.shutdown_publishers.cancel();
        self.shutdown_consumers.cancel();
        self.shutdown_settle.cancel();
        self.shutdown_expiry.cancel();
        self.task_group.shutdown().await;
        self.engine
            .shutdown()
            .await
            .inspect_err(|e| {
                tracing::error!("engine shutdown error: {:?}", e);
            })
            .ok();
    }

    pub async fn shutdown_graceful(&self) {
        tracing::info!("Broker initiating graceful shutdown");

        // Stop accepting new publishers, consumers immediately
        self.shutdown_publishers.cancel();
        self.shutdown_consumers.cancel();
        self.shutdown_expiry.cancel();
        tracing::debug!("Signaled shutdown to publishers, consumers, and expiry worker");
        self.shutdown_settle.cancel();
        tracing::debug!("Signaled shutdown to settle workers");

        // TODO: find a bette way to reliably wait for settles
        // tokio::time::sleep(Duration::from_millis(150)).await; // give some time for in-flight messages to be processed and settle requests to be sent

        // TODO: should we pending confirms too?
        // Wait until settle channels drained
        while self.pending_settles.load(Ordering::Acquire) != 0 {
            tracing::debug!(
                "Waiting for pending settles to drain: {}",
                self.pending_settles.load(Ordering::Acquire)
            );
            self.settle_drained.notified().await;
        }

        tracing::debug!("All pending settles drained, proceeding with shutdown");

        // Now stop tasks
        self.task_group.shutdown().await;

        // Shutdown engine
        if let Err(e) = self.engine.shutdown().await {
            tracing::error!("engine shutdown error: {:?}", e);
        }
    }

    pub async fn get_publisher(
        self: &Arc<Self>,
        topic: &str,
        group: &Option<Group>,
    ) -> Result<(PublisherHandle, ConfirmStream), BrokerError> {
        // TODO: make configurable?
        let (tx, mut rx) = mpsc::channel::<PublishRequest>(4_096);
        let (confirm_tx, confirm_rx) = mpsc::channel::<Offset>(4_096);

        let engine = self.engine.clone();
        let shutdown = self.shutdown_publishers.clone();
        let tp: Topic = topic.to_string();
        let part: LogId = 0;
        let group = group.clone();

        let qs = self
            .queue(&QueueKey {
                tp: tp.clone(),
                part,
                group: group.clone(),
            })
            .await;

        // TODO: make async by maybe making two tasks: one to receive publish requests and one to wait for completions and send confirms? Or use a bounded channel and backpressure?
        let (confirm_sink_tx, mut confirm_sink_rx) = mpsc::channel::<(
            oneshot::Receiver<Result<AppendResult, IoError>>,
            oneshot::Sender<Result<u64, BrokerError>>,
        )>(4_096);
        let metrics = self.metrics.clone();
        self.task_group.spawn("publisher_confirm_sink", async move {
            loop {
                tokio::select! {
                    biased;

                    _ = shutdown.cancelled() => break,

                    maybe = rx.recv() => {
                        let Some(PublishRequest { payload, reply, require_confirm }) = maybe else { break; };

                        let (completion, rx_completion) = KeratinAppendCompletion::pair();

                        let publish_res = engine.publish(
                            &tp,
                            part,
                            group.as_deref(),
                            &payload,
                            completion,
                        ).await;

                        if let Err(err) = publish_res {
                            let _ = reply.send(Err(err.into()));
                            continue;
                        }

                        if let Some(metrics) = metrics.as_ref() {
                            metrics.published();
                        }

                        if require_confirm
                            && let Err(e) = confirm_sink_tx.send((rx_completion, reply)).await {
                                tracing::error!("Failed to send completion receiver to confirm sink: {e:?}");
                                // let _ = reply.send(Err(BrokerError::ChannelClosed));
                                continue;
                            }

                        // // Wait for durability
                        // match rx_completion.await {
                        //     Ok(Ok(append)) => {
                        //         let offset = append.base_offset;
                        //         let _ = reply.send(Ok(offset));
                        //         let _ = confirm_tx.send(offset).await;
                        //     }
                        //     Ok(Err(_)) | Err(_) => {
                        //         let _ = reply.send(Err(BrokerError::ChannelClosed));
                        //     }
                        // }
                    }
                }
            }
        });

        self.task_group.spawn("confirm_sink_loop", async move {
            while let Some((rx_completion, reply)) = confirm_sink_rx.recv().await {
                // Wait for durability
                match rx_completion.await {
                    Ok(Ok(append)) => {
                        let offset = append.base_offset;
                        if let Err(e) = confirm_tx.send(offset).await {
                            tracing::error!("Failed to send confirm offset: {e:?}");
                        }
                        let res: Result<u64, BrokerError> = Ok(offset);
                        if let Err(e) = reply.send(res) {
                            tracing::error!("Failed to send publish response: {e:?}");
                        }
                    }
                    Ok(Err(e)) => {
                        tracing::error!("Append failed: {e:?}");
                    }
                    Err(e) => {
                        tracing::error!("Failed to receive append completion: {e:?}");
                    }
                }

                qs.wake();
            }
        });

        Ok((
            PublisherHandle { publisher: tx },
            ConfirmStream { rx: confirm_rx },
        ))
    }

    async fn queue(&self, key: &QueueKey) -> Arc<QueueLoopState> {
        self.queues
            .entry(key.clone())
            .or_insert_with(|| Arc::new(QueueLoopState::new()))
            .value()
            .clone()
    }

    pub async fn subscribe(
        self: &Arc<Self>,
        topic: &str,
        group: Option<&str>,
        cfg: ConsumerConfig,
    ) -> Result<ConsumerHandle, BrokerError> {
        let tp: Topic = topic.to_string();
        let part: LogId = 0;
        let group: Option<Group> = group.map(|s| s.to_string());

        let sub_id = self.next_sub_id.fetch_add(1, Ordering::SeqCst);
        let consumer_id = self.next_consumer_id.fetch_add(1, Ordering::SeqCst);

        let prefetch = cfg.prefetch.max(1);

        let (msg_tx, msg_rx) = mpsc::channel::<DeliverableMessage>(prefetch * 256);
        let (settle_tx, settle_rx) = mpsc::channel::<SettleRequest>(prefetch * 512);

        let consumer = Arc::new(ConsumerState {
            id: consumer_id,
            tx: msg_tx.clone(),
            prefetch: AtomicUsize::new(prefetch),
            inflight: AtomicUsize::new(0),
        });

        let key = QueueKey {
            tp: tp.clone(),
            part,
            group: group.clone(),
        };

        let qs = self.queue(&key).await;

        qs.consumers.insert(consumer_id, consumer.clone());

        // spawn settle loop for this consumer
        self.spawn_settle_loop(consumer.clone(), settle_rx);

        // spawn delivery loop once per queue
        if !qs.started.swap(true, Ordering::SeqCst) {
            tracing::debug!(
                "Starting delivery loop for tp={} part={} group={:?}",
                tp,
                part,
                group
            );
            self.spawn_delivery_loop(key.clone(), qs.clone());
        }

        qs.wake();

        Ok(ConsumerHandle {
            sub_id,
            config: cfg,
            group: group.map(|g| g.into()),
            topic: tp.into(),
            partition: part,
            messages: msg_rx,
            settler: settle_tx,
            pending_settles: self.pending_settles.clone(),
        })
    }

    fn spawn_settle_loop(
        self: &Arc<Self>,
        consumer: Arc<ConsumerState>,
        mut settle_rx: mpsc::Receiver<SettleRequest>,
    ) {
        let broker = self.clone();
        self.task_group.spawn("settle_loop", async move {
            loop {
                tokio::select! {
                    biased;

                    _ = broker.shutdown_settle.cancelled() => break,
                    req = settle_rx.recv() => {
                        let Some(req) = req else {
                            tracing::debug!("Settle channel closed for consumer {}", consumer.id);
                            break;
                        };
                        tracing::debug!("Received settle request from consumer {}: {:?}", consumer.id, req);
                        broker.handle_settle(&consumer, req).await;
                    }
                }
            }
            settle_rx.close();
            tracing::debug!("Settle loop exiting for consumer {}", consumer.id);
            while let Some(req) = settle_rx.recv().await {
                tracing::debug!("Draining settle request from consumer {}: {:?}", consumer.id, req);
                broker.handle_settle(&consumer, req).await;
            }
            tracing::debug!("Settle loop fully exited for consumer {}", consumer.id);
        });
    }

    async fn handle_settle(&self, consumer: &Arc<ConsumerState>, req: SettleRequest) {
        let Some(tag_rec) = self
            .records_by_tags
            .remove(&req.delivery_tag)
            .map(|kv| kv.1)
        else {
            // unknown tag -> ignore (or warn)
            tracing::warn!(
                "Received settle for unknown delivery tag {:?} from consumer {}",
                req.delivery_tag,
                consumer.id
            );
            return;
        };

        // validate consumer
        if tag_rec.consumer_id != consumer.id {
            // wrong consumer: ignore (or warn)
            tracing::warn!(
                "Received settle for delivery tag {:?} from consumer {}, but tag belongs to consumer {}",
                req.delivery_tag,
                consumer.id,
                tag_rec.consumer_id
            );
            return;
        }

        tracing::debug!(
            "Handling settle for consumer {}: {:?} (tag {:?}) (offset {})",
            consumer.id,
            req.settle_type,
            req.delivery_tag,
            tag_rec.offset
        );

        let settle_kind = match req.settle_type {
            SettleType::Ack => SettleKind::Ack,
            SettleType::Nack { requeue } => SettleKind::Nack {
                requeue: requeue.unwrap_or(true),
            },
            SettleType::Reject { .. } => SettleKind::Nack { requeue: false },
        };

        // IMPORTANT:
        // Only decrement inflight when the settle append is durably accepted (completion).
        // So we keep consumer.inflight until completion callback fires.

        let engine = self.engine.clone();
        let qs = self.queues.get(&tag_rec.key).map(|e| e.value().clone());

        // Make a completion that wakes the queue loop and decrements inflight.
        let consumer2 = consumer.clone();
        let pending_settles = self.pending_settles.clone();
        let settle_drained = self.settle_drained.clone();
        let metrics = self.metrics.clone();
        let done = move |ok: bool| {
            if ok {
                consumer2.dec_inflight();
                if let SettleKind::Ack = settle_kind
                    && let Some(metrics) = metrics
                {
                    metrics.acked();
                }

                if let Some(qs) = &qs {
                    qs.wake();
                }
            } else {
                // If settle append failed, you may want to:
                // - reinsert tag mapping? (probably no; client should retry)
                // - or treat as "still inflight" and rely on expiry
                // For now: keep inflight as-is (conservative).
                if let Some(qs) = &qs {
                    qs.wake();
                }
            }

            let pending = pending_settles.fetch_sub(1, Ordering::AcqRel);
            if pending <= 1 {
                settle_drained.notify_waiters();
            }
            tracing::debug!(
                "Settle completed for consumer {}, pending settles now {}",
                consumer2.id,
                pending - 1
            );
        };

        // You’ll implement a real completion type; here is the intent:
        let completion: Box<dyn AppendCompletion<IoError>> = Box::new(SimpleCompletion::new(done));
        let _ = engine
            .settle(
                &tag_rec.key.tp,
                tag_rec.key.part,
                tag_rec.key.group.as_deref(),
                EngineSettleRequest {
                    offset: tag_rec.offset,
                    kind: settle_kind,
                },
                completion,
            )
            .await;
    }

    fn spawn_delivery_loop(self: &Arc<Self>, key: QueueKey, qs: Arc<QueueLoopState>) {
        let broker = self.clone();
        let metrics = self.metrics.clone();
        self.task_group.spawn("delivery_loop", async move {
            let mut last_epoch_seen = qs.current_epoch();
            let ttl_ms = broker.cfg.inflight_ttl_ms;
            let poll = Duration::from_millis(broker.cfg.delivery_poll_max_ms.max(1));
            let mut tick = tokio::time::interval(poll);

            loop {
                let reason = tokio::select! {
                    biased;

                    _ = broker.shutdown_consumers.cancelled() => break,
                    _ = qs.notify.notified() => WakeReason::Notify,
                    _ = tick.tick() => WakeReason::Timer,
                };

                // if let WakeReason::Notify = reason {
                //     // Stagger to pick up more potential wakes
                //     tokio::time::sleep(Duration::from_millis(10)).await;
                // }

                let epoch_now = qs.current_epoch();
                let epoch_advanced = epoch_now != last_epoch_seen;
                last_epoch_seen = epoch_now;

                tracing::debug!("Delivery loop woke up for tp={} part={} group={:?} due to {:?}, epoch advanced: {}, current epoch: {}", key.tp, key.part, key.group, reason, epoch_advanced, epoch_now);

                let mut progressed = false;

                // try deliver until we stall
                loop {
                    if broker.shutdown_consumers.is_cancelled() {
                        break;
                    }

                    let consumers: Vec<Arc<ConsumerState>> =
                        qs.consumers.iter().map(|e| e.value().clone()).collect();
                    if consumers.is_empty() {
                        tracing::debug!("No consumers for tp={} part={} group={:?}, skipping delivery", key.tp, key.part, key.group);
                        break;
                    }

                    let total_cap: usize = consumers
                        .iter()
                        .map(|c| {
                            let p = c.prefetch.load(Ordering::Acquire);
                            let i = c.inflight.load(Ordering::Acquire);
                            p.saturating_sub(i)
                        })
                        .sum();

                    tracing::debug!("Total delivery capacity for tp={} part={} group={:?}: {}, Total consumers: {}", key.tp, key.part, key.group, total_cap, consumers.len());

                    if total_cap == 0 {
                        break;
                    }

                    let lease_deadline = unix_millis() + ttl_ms;

                    let deliverables = match broker
                        .engine
                        .poll_ready(
                            &key.tp,
                            key.part,
                            key.group.as_deref(),
                            total_cap,
                            lease_deadline,
                        )
                        .await
                    {
                        Ok(v) if !v.is_empty() => v,
                        _ => break,
                    };

                    tracing::debug!("Polled {} deliverables for tp={} part={} group={:?}", deliverables.len(), key.tp, key.part, key.group);

                    let mut delivered = 0;
                    let mut rr = qs.rr.fetch_add(1, Ordering::Relaxed) as usize;
                    let mut redelivered = 0;
                    for d in deliverables {
                        let mut picked = None;
                        for _ in 0..consumers.len() {
                            let c = &consumers[rr % consumers.len()];
                            rr += 1;
                            if c.can_accept() {
                                picked = Some(c.clone());
                                break;
                            }
                        }
                        let Some(c) = picked else {
                            qs.wake();
                            break;
                        };

                        let epoch = broker.next_tag_epoch.fetch_add(1, Ordering::SeqCst);
                        let tag = DeliveryTag { epoch };

                        broker.records_by_tags.insert(
                            tag,
                            TagRecord {
                                key: key.clone(),
                                offset: d.offset,
                                consumer_id: c.id,
                            },
                        );
                        broker
                            .tags_by_key_offset
                            .insert((key.clone(), d.offset), tag);

                        c.inc_inflight();

                        let msg = DeliverableMessage {
                            message: fibril_storage::StoredMessage {
                                topic: key.tp.clone(),
                                group: key.group.clone(),
                                partition: key.part,
                                offset: d.offset,
                                timestamp: unix_millis(),
                                retried: d.retries,
                                payload: d.payload,
                            },
                            delivery_tag: tag,
                            group: key.group.clone(),
                        };

                        if d.retries > 0 {
                            redelivered += 1;
                        }

                        if c.tx.send(msg).await.is_err() {
                            // TODO: Currently handled by expiry (since we keep inflight until completion),
                            //          but you could also immediately decrement inflight and requeue here.
                            qs.consumers.remove(&c.id);
                            qs.wake();
                        } else {
                            if let Some(metrics) = &broker.metrics {
                                metrics.delivered();
                            }
                            progressed = true;
                        }

                        delivered += 1;

                        if delivered % 1024 == 0 {
                            tokio::task::yield_now().await;
                        }
                    }

                    if redelivered > 0
                        && let Some(metrics) = &metrics {
                            metrics.redelivered_many(redelivered);
                        }
                }

                if progressed && matches!(reason, WakeReason::Timer) && epoch_advanced {
                    tracing::warn!(
                        "Delivery progressed only after timer wakeup (possible missed notify) tp={} part={} group={:?}",
                        key.tp,
                        key.part,
                        key.group
                    );
                }
            }
        });
    }

    fn spawn_expiry_worker(broker: Arc<Self>) {
        let broker_clone = broker.clone();
        broker_clone.task_group.spawn("expiry_worker", async move {
            loop {
                tokio::select! {
                    biased;

                    _ = broker.shutdown_expiry.cancelled() => break,

                    _ = async {
                        let hint = broker.engine.next_expiry_hint().await.unwrap_or(None);
                        match hint {
                            Some(ts) => {
                                let now = unix_millis();
                                if ts > now {
                                    tracing::info!("Expiry worker sleeping for {} ms..", ts - now);
                                    fibril_util::sleep_until(ts).await;
                                }
                            }
                            None => {
                                // TODO: move to timer?
                                tracing::info!("Expiry worker sleeping for {} ms..", broker.cfg.expiry_poll_min_ms);
                                tokio::time::sleep(Duration::from_millis(
                                    broker.cfg.expiry_poll_min_ms
                                )).await;
                            }
                        }
                    } => {}
                }

                tracing::info!("Expiry worker running..");

                // Requeue expired inside Stroma (durable)
                let expired = match broker
                    .engine
                    .requeue_expired(unix_millis(), broker.cfg.expiry_batch_max)
                    .await
                {
                    Ok(v) => v,
                    Err(err) => {
                        // TODO: log? handle?
                        tracing::error!("Expiry worker error: {err}");
                        continue;
                    }
                };

                tracing::info!(
                    "Expiry worker woke up, requeued {} expired messages",
                    expired.len()
                );

                if expired.is_empty() {
                    continue;
                }

                if let Some(metrics) = &broker.metrics {
                    metrics.expired_many(expired.len() as u64);
                }

                for (tp, part, group, offset) in expired.iter().cloned() {
                    let key = QueueKey { tp, part, group };

                    // find the tag for this offset
                    let tag = broker
                        .tags_by_key_offset
                        .remove(&(key.clone(), offset))
                        .map(|kv| kv.1);

                    if let Some(tag) = tag
                        && let Some((_, rec)) = broker.records_by_tags.remove(&tag)
                        && let Some(qs) = broker.queues.get(&rec.key)
                    {
                        if let Some(consumer) = qs.consumers.get(&rec.consumer_id) {
                            consumer.dec_inflight();
                        }
                        qs.wake();
                    }
                }

                // Wake each affected queue exactly once
                let mut touched: HashSet<QueueKey> = HashSet::new();
                for (tp, part, group, _off) in expired {
                    touched.insert(QueueKey { tp, part, group });
                }

                for key in touched {
                    if let Some(qs) = broker.queues.get(&key).map(|e| e.value().clone()) {
                        qs.wake();
                    }
                }

                tracing::info!(
                    "Expiry worker iteration finished"
                );
            }
        });
    }
}

// ---------------- Completion helper (sketch) ----------------
// TODO:
// We already have completion abstractions, work on replacing this with more *complete* impls.
// This already shows the intent: call closure on complete.

struct SimpleCompletion {
    f: Option<Box<dyn FnOnce(bool) + Send>>,
}

impl SimpleCompletion {
    fn new<F: FnOnce(bool) + Send + 'static>(f: F) -> Self {
        Self {
            f: Some(Box::new(f)),
        }
    }
}

impl AppendCompletion<IoError> for SimpleCompletion {
    fn complete(mut self: Box<Self>, res: Result<AppendResult, IoError>) {
        let ok = res.is_ok();
        if let Some(f) = self.f.take() {
            f(ok);
        }
    }
}
