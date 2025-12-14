pub mod coordination;

use crossbeam::queue::SegQueue;
use dashmap::DashMap;
use std::pin::pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::{
    broker::coordination::Coordination,
    storage::{DeliverableMessage, Group, Offset, Partition, Storage, StorageError, Topic},
};

macro_rules! invariant {
    ($cond:expr, $($arg:tt)*) => {
        if cfg!(debug_assertions) && !$cond {
            panic!($($arg)*);
        }
    };
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
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
struct GroupState {
    consumers: DashMap<ConsumerId, mpsc::Sender<DeliverableMessage>>,
    next_offset: AtomicU64,
    delivery_task_started: AtomicBool,
    rr_counter: AtomicU64,
    redelivery: Arc<SegQueue<Offset>>, // lock-free FIFO queue
}

impl GroupState {
    fn new() -> Self {
        Self {
            consumers: DashMap::new(),
            next_offset: AtomicU64::new(0),
            delivery_task_started: AtomicBool::new(false),
            rr_counter: AtomicU64::new(0),
            redelivery: Arc::new(SegQueue::new()),
        }
    }
}

pub struct PublishRequest {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Result<Offset, BrokerError>>,
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub inflight_ttl_secs: u64, // seconds
    pub publish_batch_size: usize,
    pub publish_batch_timeout_ms: u64,
    pub ack_batch_size: usize,
    pub ack_batch_timeout_ms: u64,
    /// Unsafe startup option to reset all inflight state
    pub reset_inflight: bool,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            inflight_ttl_secs: 60,
            publish_batch_size: 128,
            publish_batch_timeout_ms: 50,
            ack_batch_size: 512,
            ack_batch_timeout_ms: 2,
            reset_inflight: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    pub prefetch_count: usize,
}

// TODO: Use request IDs?
// TODO: empty and close channels on drop/shutdown
pub struct ConsumerHandle {
    pub config: ConsumerConfig,
    // TODO: find way to make this complete not on channel deliver, but response sent
    pub messages: tokio::sync::mpsc::Receiver<DeliverableMessage>,
    // TODO: Should it be ack only or generally respond?
    pub acker: tokio::sync::mpsc::Sender<Offset>,
}

impl ConsumerHandle {
    pub async fn ack(&self, offset: Offset) -> Result<(), BrokerError> {
        self.acker
            .send(offset)
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
    confirm_tx: tokio::sync::mpsc::Sender<Result<Offset, BrokerError>>,
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
            .map_err(|_| BrokerError::ChannelClosed)?;

        let confirm_rx = self.confirm_tx.clone();
        tokio::spawn(async move {
            if let Ok(Ok(offset)) = rx.await {
                let _ = confirm_rx.send(Ok(offset)).await;
            } else {
                let _ = confirm_rx.send(Err(BrokerError::ChannelClosed)).await;
            }
        });

        Ok(())
    }
}

impl ConfirmStream {
    pub async fn recv_confirm(&mut self) -> Option<Result<Offset, BrokerError>> {
        self.rx.recv().await
    }
}

// TODO: Injectable clock for testing
#[derive(Debug, Clone)]
pub struct Broker<C: Coordination + Send + Sync + 'static> {
    pub config: BrokerConfig,
    storage: Arc<dyn Storage>,
    coord: Arc<C>,
    groups: Arc<DashMap<(Topic, Group), Arc<GroupState>>>,
    batchers: Arc<DashMap<(Topic, Partition), mpsc::Sender<PublishRequest>>>,
    shutdown: CancellationToken,
}

impl<C: Coordination + Send + Sync + 'static> Broker<C> {
    pub async fn try_new(
        storage: impl Storage + 'static,
        coord: C,
        config: BrokerConfig,
    ) -> Result<Self, BrokerError> {
        let storage = Arc::new(storage);
        let shutdown = CancellationToken::new();

        if config.reset_inflight {
            storage.clear_all_inflight().await?;
        }

        let now = unix_ts();
        let expired = storage.list_expired(now).await?;

        // Clear expired inflight entries on startup, to avoid stuck messages
        // Rest can be redelivered after TTL by redelivery worker
        for msg in expired {
            storage
                .clear_inflight(
                    &msg.message.topic,
                    msg.message.partition,
                    &msg.group,
                    msg.message.offset,
                )
                .await?;
        }

        Ok(Broker {
            config,
            storage,
            coord: Arc::new(coord),
            groups: Arc::new(DashMap::new()),
            batchers: Arc::new(DashMap::new()),
            shutdown,
        })
    }

    pub async fn debug_upper(&self, topic: &str, partition: u32) -> u64 {
        self.storage
            .current_next_offset(&topic.to_string(), partition)
            .await
            .unwrap_or(0)
    }

    pub async fn dump_meta_keys(&self) {
        self.storage.dump_meta_keys().await;
    }

    pub async fn get_publisher(
        &self,
        topic: &str,
    ) -> Result<(PublisherHandle, ConfirmStream), BrokerError> {
        let (confirm_tx, confirm_rx) =
            mpsc::channel::<Result<Offset, BrokerError>>(self.config.publish_batch_size * 4);

        let batcher = self.get_or_create_batcher(&topic.to_string(), 0).await;

        Ok((
            PublisherHandle {
                publisher: batcher,
                confirm_tx,
            },
            ConfirmStream { rx: confirm_rx },
        ))
    }

    pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<Offset, BrokerError> {
        let (tx, rx) = oneshot::channel();

        let batcher = self.get_or_create_batcher(&topic.to_string(), 0).await;

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

    pub async fn subscribe(&self, topic: &str, group: &str) -> Result<ConsumerHandle, BrokerError> {
        self.storage
            .register_group(&topic.to_string(), 0, &group.to_string())
            .await?;

        let topic_clone = topic.to_string();
        let group_clone = group.to_string();
        let key = (topic_clone.clone(), group_clone.clone());

        let (msg_tx, msg_rx) = mpsc::channel(100);
        let (ack_tx, mut ack_rx) = mpsc::channel::<Offset>(100);

        // Register consumer in group state
        let entry = self.groups.entry(key.clone());

        let group_state_arc = match entry {
            dashmap::mapref::entry::Entry::Occupied(e) => e.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                let gs = Arc::new(GroupState::new());

                // Initialize next_offset from durable state (important on restart)
                // restore delivery cursor after restart
                let lowest = self
                    .storage
                    .lowest_unacked_offset(&topic_clone, 0, &group_clone)
                    .await?;

                let upper = self.storage.current_next_offset(&topic_clone, 0).await?;

                let safe = lowest.min(upper);

                // Simple consumer id: increment rr_counter
                gs.next_offset.store(safe, Ordering::SeqCst);

                v.insert(gs).clone()
            }
        };

        let consumer_id = group_state_arc.rr_counter.fetch_add(1, Ordering::SeqCst);
        group_state_arc
            .consumers
            .insert(consumer_id, msg_tx.clone());

        // Start delivery task once per (topic, group)
        if !group_state_arc
            .delivery_task_started
            .swap(true, Ordering::SeqCst)
        {
            let storage = Arc::clone(&self.storage);
            let coord = self.coord.clone();
            let groups = self.groups.clone();

            let ttl = self.config.inflight_ttl_secs;
            let shutdown = self.shutdown.clone();

            tokio::spawn(async move {
                // Dedicated delivery loop for this (topic, group)
                loop {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    // Lookup group_state each iteration so we see updated consumers
                    let group_state_opt = groups.get(&key);
                    let group_state = match group_state_opt {
                        Some(g) => g.value().clone(),
                        None => {
                            // Group removed; nothing to do
                            break;
                        }
                    };

                    if !coord.is_leader(&topic_clone, 0).await {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }

                    // Snapshot consumer list
                    let consumers: Vec<(ConsumerId, mpsc::Sender<DeliverableMessage>)> =
                        group_state
                            .consumers
                            .iter()
                            .map(|entry| (*entry.key(), entry.value().clone()))
                            .collect();

                    if consumers.is_empty() {
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                        continue;
                    }

                    // 1. Deliver all expired messages first
                    while let Some(expired_off) = group_state.redelivery.pop() {
                        // fetch the message directly by offset
                        if let Ok(msg) = storage.fetch_by_offset(&topic_clone, 0, expired_off).await
                        {
                            // reinsert inflight entry with new deadline
                            let new_deadline = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_secs()
                                + ttl;

                            // deliver to consumer via round-robin
                            let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;

                            // TODO: handle empty consumers case
                            let (cid, tx) = &consumers[rr % consumers.len()];

                            if tx
                                .send(DeliverableMessage {
                                    message: msg.clone(),
                                    delivery_tag: expired_off,
                                    group: group_clone.clone(),
                                })
                                .await
                                .is_err()
                            {
                                group_state.consumers.remove(cid);
                            } else {
                                let _ = storage
                                    .mark_inflight(
                                        &topic_clone,
                                        0,
                                        &group_clone,
                                        expired_off,
                                        new_deadline,
                                    )
                                    .await;
                            }

                            // IMPORTANT: DO NOT TOUCH next_offset HERE
                        }
                    }

                    let start_offset = group_state.next_offset.load(Ordering::SeqCst);

                    let upper = match storage.current_next_offset(&topic_clone, 0).await {
                        Ok(v) => v,
                        Err(_) => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            continue;
                        }
                    };

                    let msgs = match storage
                        .fetch_available_clamped(
                            &topic_clone,
                            0,
                            &group_clone,
                            start_offset,
                            upper,
                            32,
                        )
                        .await
                    {
                        Ok(v) => v,
                        Err(_) => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            continue;
                        }
                    };

                    if msgs.is_empty() {
                        // debug / liveness: detect stuck cursor
                        let mut cur = group_state.next_offset.load(Ordering::SeqCst);

                        // Fast-forward over offsets that are already ACKed or currently inflight.
                        // This prevents deadlock when cur points at an inflight gap.
                        while cur < upper {
                            let done = storage
                                .is_inflight_or_acked(&topic_clone, 0, &group_clone, cur)
                                .await
                                .unwrap_or(false);

                            if done {
                                cur += 1;
                                continue;
                            }

                            // cur is neither acked nor inflight, so it's a real deliverable candidate
                            // but fetch_available returned empty => likely no message exists at cur
                            // (or there's a bug). Stop here.
                            break;
                        }

                        // Only store if we actually moved
                        let prev = group_state.next_offset.load(Ordering::SeqCst);
                        if cur > prev {
                            group_state.next_offset.store(cur, Ordering::SeqCst);
                        }

                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        continue;
                    }

                    for msg in msgs {
                        let off = msg.delivery_tag;
                        let prev = group_state.next_offset.load(Ordering::SeqCst);

                        // offsets must be monotonic
                        debug_assert!(
                            off >= prev,
                            "delivery went backwards: off={} prev={}",
                            off,
                            prev
                        );

                        // never deliver something already ACKed
                        debug_assert!(
                            storage
                                .fetch_available_clamped(
                                    &topic_clone,
                                    0,
                                    &group_clone,
                                    off,
                                    off + 1,
                                    1
                                )
                                .await
                                .unwrap()
                                .len()
                                <= 1,
                            "delivered an ACKed or inflight message: off={}",
                            off
                        );

                        // Mark inflight with deadline = now + ttl
                        let deadline = (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs())
                            + ttl;

                        // Choose consumer round-robin
                        let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;
                        let idx = rr % consumers.len();
                        let (_cid, tx) = &consumers[idx];

                        invariant!(
                            msg.delivery_tag
                                >= group_state
                                    .next_offset
                                    .load(Ordering::SeqCst)
                                    .saturating_sub(1),
                            "delivering message behind cursor"
                        );

                        // Try deliver
                        if tx.send(msg).await.is_err() {
                            // consumer dropped; remove and DO NOT advance cursor
                            group_state.consumers.remove(&consumers[idx].0);
                            break; // break out so we re-snapshot consumers next loop
                        }

                        // Only mark inflight AFTER we know message is in consumer channel
                        let mark_res = storage
                            .mark_inflight(&topic_clone, 0, &group_clone, off, deadline)
                            .await;
                        if let Err(err) = mark_res {
                            // Could not mark inflight -> DO NOT advance cursor.
                            // (Optional: may want to log this)
                            eprintln!(
                                "mark_inflight failed for topic={} group={} offset={} error={:?}",
                                &topic_clone, &group_clone, off, err
                            );
                            break;
                        }

                        // Now safe to advance
                        group_state.next_offset.store(off + 1, Ordering::SeqCst);
                    }
                }
            });
        }

        // Ack handler for this consumer
        let storage_clone = Arc::clone(&self.storage);
        let topic_clone = topic.to_string();
        let group_clone = group.to_string();

        let shutdown = self.shutdown.clone();
        let ack_batch_size = self.config.ack_batch_size;
        let ack_batch_timeout_ms = self.config.ack_batch_timeout_ms;

        tokio::spawn(async move {
            let mut buf: Vec<Offset> = Vec::with_capacity(ack_batch_size);
            let mut tick =
                tokio::time::interval(std::time::Duration::from_millis(ack_batch_timeout_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown.cancelled() => {
                        // Drain channel, flush once.
                        while let Ok(off) = ack_rx.try_recv() {
                            buf.push(off);
                            if buf.len() >= ack_batch_size {
                                flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf).await;
                            }
                        }
                        flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf).await;
                        break;
                    }

                    Some(off) = ack_rx.recv() => {
                        buf.push(off);
                        if buf.len() >= ack_batch_size {
                            flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf).await;
                        }
                    }

                    _ = tick.tick() => {
                        flush_ack_batch(&storage_clone, &topic_clone, &group_clone, &mut buf).await;
                    }
                }
            }
        });

        Ok(ConsumerHandle {
            config: ConsumerConfig {
                prefetch_count: 100,
            },
            messages: msg_rx,
            acker: ack_tx,
        })
    }

    pub fn start_redelivery_worker(&self) {
        let storage = Arc::clone(&self.storage);
        let coord = self.coord.clone();

        let groups = self.groups.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            loop {
                if shutdown.is_cancelled() {
                    break;
                }
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                let expired = match storage.list_expired(now_ts).await {
                    Ok(v) => v,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        continue;
                    }
                };

                for msg in expired {
                    if !coord
                        .is_leader(&msg.message.topic, msg.message.partition)
                        .await
                    {
                        continue;
                    }

                    // REMOVE expired inflight entry
                    let _ = storage
                        .clear_inflight(
                            &msg.message.topic,
                            msg.message.partition,
                            &msg.group,
                            msg.message.offset,
                        )
                        .await;

                    // TODO adjust to handle more than one expired message per group
                    if let Some(gs) = groups.get(&(msg.message.topic.clone(), msg.group.clone())) {
                        let current = gs.next_offset.load(Ordering::SeqCst);

                        debug_assert!(
                            msg.message.offset < current,
                            "expired offset >= next_offset (would duplicate): off={} next={}",
                            msg.message.offset,
                            current
                        );

                        let expired_offset = msg.message.offset;

                        if expired_offset >= current {
                            continue;
                        }

                        gs.redelivery.push(expired_offset);
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });
    }

    async fn get_or_create_batcher(
        &self,
        topic: &Topic,
        partition: Partition,
    ) -> mpsc::Sender<PublishRequest> {
        let key = (topic.clone(), partition);

        match self.batchers.entry(key) {
            dashmap::Entry::Occupied(e) => e.get().clone(),
            dashmap::Entry::Vacant(v) => {
                let (tx, rx) = mpsc::channel::<PublishRequest>(self.config.publish_batch_size * 4);
                v.insert(tx.clone());
                self.spawn_batcher(topic.clone(), partition, rx);
                tx
            }
        }
    }

    fn spawn_batcher(
        &self,
        topic: Topic,
        partition: Partition,
        mut rx: mpsc::Receiver<PublishRequest>,
    ) {
        let storage = Arc::clone(&self.storage);
        let cfg = self.config.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut pending = Vec::<PublishRequest>::new();
            let mut last_flush = tokio::time::Instant::now();

            loop {
                if shutdown.is_cancelled() {
                    break;
                }
                // Try to get the next message WITHOUT waiting
                match rx.try_recv() {
                    Ok(msg) => {
                        pending.push(msg);

                        // If we reached batch_size -> flush immediately
                        if pending.len() >= cfg.publish_batch_size {
                            flush_publish_batch(&storage, &topic, partition, &mut pending).await;
                            last_flush = tokio::time::Instant::now();
                        }

                        continue; // go back to top, keep draining buffer
                    }

                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                        // Queue is empty
                        if !pending.is_empty() {
                            // SMART FLUSH: Don't wait for timeout
                            flush_publish_batch(&storage, &topic, partition, &mut pending).await;
                            last_flush = tokio::time::Instant::now();
                        }
                    }

                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        // Should almost never happen
                        if !pending.is_empty() {
                            flush_publish_batch(&storage, &topic, partition, &mut pending).await;
                        }
                        return;
                    }
                }

                // Timeout flush
                if last_flush.elapsed().as_millis() > cfg.publish_batch_timeout_ms as u128 {
                    if !pending.is_empty() {
                        flush_publish_batch(&storage, &topic, partition, &mut pending).await;
                    }
                    last_flush = tokio::time::Instant::now();
                }

                // Sleep a *tiny* amount so we don't busy-loop
                tokio::task::yield_now().await;
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

        // Give tasks time to see the closed channels & exit
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    }
}

async fn flush_publish_batch(
    storage: &Arc<dyn Storage>,
    topic: &Topic,
    partition: u32,
    pending: &mut Vec<PublishRequest>,
) {
    if pending.is_empty() {
        return;
    }

    let payloads: Vec<Vec<u8>> = pending.iter().map(|r| r.payload.clone()).collect();

    let result = storage.append_batch(topic, partition, &payloads).await;

    match result {
        Ok(offsets) => {
            for (req, off) in pending.drain(..).zip(offsets) {
                let _ = req.reply.send(Ok(off));
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
    topic: &String,
    group: &String,
    buf: &mut Vec<Offset>,
) {
    if buf.is_empty() {
        return;
    }
    // best-effort; you can log errors
    let _ = storage.ack_batch(topic, 0, group, buf).await;
    buf.clear();
}
