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
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use fibril_storage::{DeliverableMessage, DeliveryTag, Group, LogId, Offset, StorageError, Topic};
use fibril_util::unix_millis;

// ---- QueueEngine you just defined ----
use crate::queue_engine::{QueueEngine, SettleKind, SettleRequest as EngineSettleRequest};
use stroma_core::{
    AppendCompletion, AppendResult, CompletionPair, IoError, KeratinAppendCompletion, StromaError,
    unix_millis,
};

// ---------------- Public API types (keep same-ish) ----------------

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

pub enum SettleType {
    Ack,
    Nack { requeue: Option<bool> },
    Reject { requeue: Option<bool> }, // you can map Reject to Nack{requeue:false} or a dedicated event later
}

pub struct SettleRequest {
    pub settle_type: SettleType,
    pub delivery_tag: DeliveryTag,
}

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
}

impl ConsumerHandle {
    pub async fn settle(&self, req: SettleRequest) -> Result<(), BrokerError> {
        self.settler
            .send(req)
            .await
            .map_err(|_| BrokerError::ChannelClosed)
    }
    pub async fn recv(&mut self) -> Option<DeliverableMessage> {
        self.messages.recv().await
    }
}

pub struct PublishRequest {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Result<Offset, BrokerError>>,
}

pub struct PublisherHandle {
    pub publisher: mpsc::Sender<PublishRequest>,
}

impl PublisherHandle {
    pub async fn publish(&self, payload: &[u8]) -> Result<Offset, BrokerError> {
        let (tx, rx) = oneshot::channel();

        self.publisher
            .send(PublishRequest {
                payload: payload.to_vec(),
                reply: tx,
            })
            .await
            .map_err(|_| BrokerError::ChannelClosed)?;

        rx.await.map_err(|_| BrokerError::ChannelClosed)?
    }
}

pub struct ConfirmStream {
    rx: mpsc::Receiver<Offset>,
}

impl ConfirmStream {
    pub async fn recv_confirm(&mut self) -> Option<Offset> {
        self.rx.recv().await
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
    // settle input (per consumer)
    settle_rx: tokio::sync::Mutex<mpsc::Receiver<SettleRequest>>,
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

enum WakeReason {
    Notify,
    Timer,
}

// ---------------- Broker ----------------

#[derive(Debug)]
pub struct Broker<E: QueueEngine + std::fmt::Debug + Send + Sync + 'static> {
    cfg: BrokerConfig,
    engine: E,
    shutdown: CancellationToken,

    next_sub_id: AtomicU64,
    next_consumer_id: AtomicU64,
    next_tag_epoch: AtomicU64,

    queues: DashMap<QueueKey, Arc<QueueLoopState>>,
    tags: DashMap<DeliveryTag, TagRecord>,

    task_group: Arc<TaskGroup>,
}

impl<E: QueueEngine + std::fmt::Debug + Clone + Send + Sync + 'static> Broker<E> {
    pub fn new(engine: E, cfg: BrokerConfig) -> Arc<Self> {
        let this = Arc::new(Self {
            cfg,
            engine,
            shutdown: CancellationToken::new(),
            next_sub_id: AtomicU64::new(1),
            next_consumer_id: AtomicU64::new(1),
            next_tag_epoch: AtomicU64::new(1),
            queues: DashMap::new(),
            tags: DashMap::new(),
            task_group: Arc::new(TaskGroup::new()),
        });

        // expiry worker: keeps Stroma turning inflight -> ready again
        Self::spawn_expiry_worker(this.clone());

        this
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        self.task_group.shutdown().await;
        self.engine.shutdown().await.inspect_err(|e| {
            tracing::error!("engine shutdown error: {:?}", e);
        }).ok();
    }

    pub async fn get_publisher(
        self: &Arc<Self>,
        topic: &str,
        group: &Option<Group>,
    ) -> Result<(PublisherHandle, ConfirmStream), BrokerError> {
        let (tx, mut rx) = mpsc::channel::<PublishRequest>(16_384);
        let (confirm_tx, confirm_rx) = mpsc::channel::<Offset>(16_384);

        let engine = self.engine.clone();
        let shutdown = self.shutdown.clone();
        let tp: Topic = topic.to_string();
        let part: LogId = 0;
        let group = group.clone();

        self.task_group.spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,

                    maybe = rx.recv() => {
                        let Some(PublishRequest { payload, reply }) = maybe else { break; };

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

                        // Wait for durability
                        match rx_completion.await {
                            Ok(Ok(append)) => {
                                let offset = append.base_offset;
                                let _ = reply.send(Ok(offset));
                                let _ = confirm_tx.send(offset).await;
                            }
                            Ok(Err(_)) | Err(_) => {
                                let _ = reply.send(Err(BrokerError::ChannelClosed));
                            }
                        }
                    }
                }
            }
        });

        Ok((
            PublisherHandle { publisher: tx },
            ConfirmStream { rx: confirm_rx },
        ))
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

        let (msg_tx, msg_rx) = mpsc::channel::<DeliverableMessage>(prefetch);
        let (settle_tx, settle_rx) = mpsc::channel::<SettleRequest>(prefetch * 2);

        let consumer = Arc::new(ConsumerState {
            id: consumer_id,
            tx: msg_tx.clone(),
            prefetch: AtomicUsize::new(prefetch),
            inflight: AtomicUsize::new(0),
            settle_rx: tokio::sync::Mutex::new(settle_rx),
        });

        let key = QueueKey {
            tp: tp.clone(),
            part,
            group: group.clone(),
        };
        let qs = self
            .queues
            .entry(key.clone())
            .or_insert_with(|| Arc::new(QueueLoopState::new()))
            .clone();

        qs.consumers.insert(consumer_id, consumer.clone());

        // spawn settle loop for this consumer
        self.spawn_settle_loop(consumer.clone());

        // spawn delivery loop once per queue
        if !qs.started.swap(true, Ordering::SeqCst) {
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
        })
    }

    fn spawn_settle_loop(self: &Arc<Self>, consumer: Arc<ConsumerState>) {
        let broker = self.clone();
        self.task_group.spawn(async move {
            loop {
                tokio::select! {
                    _ = broker.shutdown.cancelled() => break,
                    req = async {
                        let mut rx = consumer.settle_rx.lock().await;
                        rx.recv().await
                    } => {
                        let Some(req) = req else { break; };
                        broker.handle_settle(&consumer, req).await;
                    }
                }
            }
        });
    }

    async fn handle_settle(&self, consumer: &Arc<ConsumerState>, req: SettleRequest) {
        let Some(tag_rec) = self.tags.remove(&req.delivery_tag).map(|kv| kv.1) else {
            // unknown tag -> ignore (or warn)
            return;
        };

        // validate consumer
        if tag_rec.consumer_id != consumer.id {
            // wrong consumer: ignore (or warn)
            return;
        }

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
        let done = move |ok: bool| {
            if ok {
                consumer2.dec_inflight();
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
        self.task_group.spawn(async move {
            let mut last_epoch_seen = qs.current_epoch();
            let ttl_ms = broker.cfg.inflight_ttl_ms;
            let poll = Duration::from_millis(broker.cfg.delivery_poll_max_ms.max(1));

            loop {
                let reason = tokio::select! {
                    _ = broker.shutdown.cancelled() => break,
                    _ = qs.notify.notified() => WakeReason::Notify,
                    _ = tokio::time::sleep(poll) => WakeReason::Timer,
                };

                let epoch_now = qs.current_epoch();
                let epoch_advanced = epoch_now != last_epoch_seen;
                last_epoch_seen = epoch_now;

                let mut progressed = false;

                // try deliver until we stall
                loop {
                    if broker.shutdown.is_cancelled() {
                        break;
                    }

                    let consumers: Vec<Arc<ConsumerState>> =
                        qs.consumers.iter().map(|e| e.value().clone()).collect();
                    if consumers.is_empty() {
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

                    if total_cap == 0 {
                        dbg!(total_cap);
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
                    dbg!(deliverables.len());

                    let mut rr = qs.rr.fetch_add(1, Ordering::Relaxed) as usize;
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

                        broker.tags.insert(
                            tag,
                            TagRecord {
                                key: key.clone(),
                                offset: d.offset,
                                consumer_id: c.id,
                            },
                        );

                        c.inc_inflight();

                        let msg = DeliverableMessage {
                            message: fibril_storage::StoredMessage {
                                topic: key.tp.clone(),
                                group: key.group.clone(),
                                partition: key.part,
                                offset: d.offset,
                                timestamp: unix_millis(),
                                payload: d.payload,
                            },
                            delivery_tag: tag,
                            group: key.group.clone(),
                        };

                        if c.tx.send(msg).await.is_err() {
                            qs.consumers.remove(&c.id);
                            qs.wake();
                        } else {
                            progressed = true;
                        }
                    }
                }

                if progressed && matches!(reason, WakeReason::Timer) && epoch_advanced {
                    tracing::warn!(
                        "delivery progressed only after timer wakeup (possible missed notify) tp={} part={} group={:?}",
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
        broker_clone.task_group.spawn(async move {
            loop {
                tokio::select! {
                    _ = broker.shutdown.cancelled() => break,

                    _ = async {
                        let hint = broker.engine.next_expiry_hint().unwrap_or(None);
                        match hint {
                            Some(ts) => {
                                let now = unix_millis();
                                if ts > now {
                                    tokio::time::sleep(Duration::from_millis(ts - now)).await;
                                }
                            }
                            None => {
                                tokio::time::sleep(Duration::from_millis(
                                    broker.cfg.expiry_poll_min_ms
                                )).await;
                            }
                        }
                    } => {}
                }

                // Requeue expired inside Stroma (durable)
                let expired = match broker
                    .engine
                    .requeue_expired(unix_millis(), broker.cfg.expiry_batch_max)
                    .await
                {
                    Ok(v) => v,
                    Err(_e) => {
                        // TODO: log? handle?
                        continue;
                    }
                };

                if expired.is_empty() {
                    continue;
                }

                for (tp, part, group, offset) in expired.iter().cloned() {
                    let key = QueueKey { tp, part, group };

                    // find the tag for this offset
                    let tag = broker
                        .tags
                        .iter()
                        .find(|e| e.value().key == key && e.value().offset == offset)
                        .map(|e| *e.key());

                    if let Some(tag) = tag {
                        if let Some((_, rec)) = broker.tags.remove(&tag) {
                            if let Some(qs) = broker.queues.get(&rec.key) {
                                if let Some(consumer) = qs.consumers.get(&rec.consumer_id) {
                                    consumer.dec_inflight();
                                }
                                qs.wake();
                            }
                        }
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
                        dbg!("expiry awoke");
                    }
                }
            }
        });
    }
}

// ---------------- Completion helper (sketch) ----------------
// TODO:
// We already have completion abstractions, work on replacing this with more *complete* impls.
// This already shows the intent: call closure on complete.

struct SimpleCompletion {
    f: std::sync::Mutex<Option<Box<dyn FnOnce(bool) + Send>>>,
}

impl SimpleCompletion {
    fn new<F: FnOnce(bool) + Send + 'static>(f: F) -> Self {
        Self {
            f: std::sync::Mutex::new(Some(Box::new(f))),
        }
    }
}

impl AppendCompletion<IoError> for SimpleCompletion {
    fn complete(self: Box<Self>, res: Result<AppendResult, IoError>) {
        let ok = res.is_ok();
        if let Some(f) = self.f.lock().unwrap().take() {
            f(ok);
        }
    }
}
