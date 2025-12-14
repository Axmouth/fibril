pub mod coordination;

use dashmap::DashMap;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use tokio::sync::{mpsc, oneshot};

use crate::{
    broker::coordination::Coordination,
    storage::{DeliverableMessage, Group, Offset, Partition, Storage, StorageError, Topic},
};

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

    #[error("unknown error: {0}")]
    Unknown(String),
}

type ConsumerId = u64;

struct GroupState {
    consumers: DashMap<ConsumerId, mpsc::Sender<DeliverableMessage>>,
    next_offset: AtomicU64,
    delivery_task_started: AtomicBool,
    rr_counter: AtomicU64,
}

impl GroupState {
    fn new() -> Self {
        Self {
            consumers: DashMap::new(),
            next_offset: AtomicU64::new(0),
            delivery_task_started: AtomicBool::new(false),
            rr_counter: AtomicU64::new(0),
        }
    }
}

pub struct PublishRequest {
    pub payload: Vec<u8>,
    pub reply: oneshot::Sender<Offset>,
}

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub ttl: u64, // seconds
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
}

pub struct Broker<C: Coordination + Send + Sync + 'static> {
    config: BrokerConfig,
    storage: Arc<Box<dyn Storage>>,
    coord: Arc<C>,
    groups: Arc<DashMap<(Topic, Group), Arc<GroupState>>>,
    publish_tx: mpsc::Sender<PublishRequest>,
}

impl<C: Coordination + Send + Sync + 'static> Broker<C> {
    pub fn new(storage: Box<dyn Storage>, coord: C, config: BrokerConfig) -> Self {
        let storage = Arc::new(storage);
        let (publish_tx, publish_rx) = mpsc::channel(config.batch_size * 4);

        Broker {
            config,
            storage,
            coord: Arc::new(coord),
            groups: Arc::new(DashMap::new()),
            publish_tx,
        }
    }

    pub async fn publish(&self, topic: &str, payload: &[u8]) -> Result<Offset, BrokerError> {        let offset = self.storage.append(&topic.to_string(), 0, payload).await?;
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
        let group_state_arc = self
            .groups
            .entry(key.clone())
            .or_insert_with(|| Arc::new(GroupState::new()))
            .clone();

        // Simple consumer id: increment rr_counter
        let consumer_id = group_state_arc.rr_counter.fetch_add(1, Ordering::SeqCst);
        group_state_arc
            .consumers
            .insert(consumer_id, msg_tx.clone());

        // Start delivery task once per (topic, group)
        if !group_state_arc
            .delivery_task_started
            .swap(true, Ordering::SeqCst)
        {
            let storage = self.storage.clone();
            let coord = self.coord.clone();
            let groups = self.groups.clone();

            let ttl = self.config.ttl;
            tokio::spawn(async move {
                // Dedicated delivery loop for this (topic, group)
                loop {
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

                    let start_offset = group_state.next_offset.load(Ordering::SeqCst);

                    let msgs = match storage
                        .fetch_available(&topic_clone, 0, &group_clone, start_offset, 32)
                        .await
                    {
                        Ok(v) => v,
                        Err(_) => {
                            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                            continue;
                        }
                    };

                    if msgs.is_empty() {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        continue;
                    }

                    for msg in msgs {
                        let off = msg.delivery_tag;

                        // Mark inflight with deadline = now + ttl
                        let deadline = (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_secs())
                            + ttl;

                        let _ = storage
                            .mark_inflight(&topic_clone, 0, &group_clone, off, deadline)
                            .await;

                        // Choose consumer round-robin
                        let rr = group_state.rr_counter.fetch_add(1, Ordering::SeqCst) as usize;
                        let idx = rr % consumers.len();
                        let (_cid, tx) = &consumers[idx];

                        if tx.send(msg).await.is_err() {
                            // Consumer dropped; remove it
                            group_state.consumers.remove(&consumers[idx].0);
                        }

                        group_state.next_offset.store(off + 1, Ordering::SeqCst);
                    }
                }
            });
        }

        // Ack handler for this consumer
        let storage_clone = self.storage.clone();
        let topic_clone = topic.to_string();
        let group_clone = group.to_string();

        tokio::spawn(async move {
            while let Some(off) = ack_rx.recv().await {
                let _ = storage_clone.ack(&topic_clone, 0, &group_clone, off).await;
            }
        });

        Ok(ConsumerHandle {
            messages: msg_rx,
            acker: ack_tx,
        })
    }

    pub fn start_redelivery_worker(&self) {
        let storage = self.storage.clone();
        let coord = self.coord.clone();

        let groups = self.groups.clone();
        tokio::spawn(async move {
            loop {
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
                        let expired_offset = msg.message.offset;

                        if expired_offset < current {
                            gs.next_offset.store(expired_offset, Ordering::SeqCst);
                        }
                    }
                }

                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });
    }
}

pub struct ConsumerHandle {
    pub messages: tokio::sync::mpsc::Receiver<DeliverableMessage>,
    pub acker: tokio::sync::mpsc::Sender<Offset>,
}
