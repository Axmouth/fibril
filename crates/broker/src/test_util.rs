use std::{sync::Arc, time::Duration};

use fibril_storage::{DeliveryTag, Offset};
use fibril_util::init_tracing;
use hashbrown::HashMap;
use stroma_core::{KeratinConfig, SnapshotConfig, TempDir, test_dir};

use crate::{broker::*, queue_engine::StromaEngine};

pub struct TestState {
    broker_cfg: BrokerConfig,
    brokers: HashMap<String, Arc<Broker<StromaEngine>>>,
    broker_dirs: HashMap<String, TempDir>,

    consumers: HashMap<String, (String, ConsumerHandle)>,
    // publishers: HashMap<String, (String, PublisherHandle)>,

    next_consumer_id: usize,
}

impl Default for TestState {
    fn default() -> Self {
        Self::new()
    }
}

impl TestState {
    pub fn new() -> Self {
        // init_tracing();
        Self {
            broker_cfg: BrokerConfig {
                inflight_ttl_ms: 5000,
                ..BrokerConfig::default()
            },
            brokers: HashMap::new(),
            broker_dirs: HashMap::new(),
            consumers: HashMap::new(),
            // publishers: HashMap::new(),
            next_consumer_id: 1,
        }
    }

    pub fn new_with_cfg(cfg: BrokerConfig) -> Self {
        // init_tracing();
        Self {
            broker_cfg: cfg,
            brokers: HashMap::new(),
            broker_dirs: HashMap::new(),
            consumers: HashMap::new(),
            // publishers: HashMap::new(),
            next_consumer_id: 1,
        }
    }

    pub fn remove_consumer(&mut self, id: &str) {
        self.consumers.remove(id);
    }

    pub async fn start_broker(&mut self, id: &str) -> Result<(), BrokerError> {
        let dir = self
            .broker_dirs
            .entry(id.to_string())
            .or_insert_with(|| {
                println!("Creating test dir for broker {id}");
                test_dir(id)
            });

        if let Some(_b) = self.brokers.get(id) {
            panic!("Broker {id} already started at {}", dir.root.display());
        }

        println!("Did not find existing, starting broker {id} with data dir {}", dir.root.display());

        let engine = StromaEngine::open(
            &dir.root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await?;

        let broker = Broker::new(engine, self.broker_cfg.clone());
        self.brokers.insert(id.to_string(), broker);
        Ok(())
    }

    pub async fn stop_broker(&mut self, id: &str) {
        if let Some(b) = self.brokers.remove(id) {
            b.shutdown().await;
        }

        self.consumers.retain(|_, (broker_id, _)| broker_id != id);
    }

    pub async fn restart_broker(&mut self, id: &str) -> Result<(), BrokerError> {
        self.stop_broker(id).await;
        self.start_broker(id).await
    }
}

impl TestState {
    pub async fn publish(
        &mut self,
        broker: &str,
        topic: &str,
        group: Option<&str>,
        payload: &[u8],
        n: usize,
    ) -> Result<Vec<Offset>, BrokerError> {
        let broker = self
            .brokers
            .get(broker)
            .ok_or_else(|| BrokerError::Unknown(format!("broker {broker} not started")))?;

        let (pubh, _) = broker
            .get_publisher(topic, &group.map(|s| s.into()))
            .await?;

        let mut offs = Vec::new();
        for _ in 0..n {
            offs.push(pubh.publish(payload).await?);
        }
        Ok(offs)
    }

    pub async fn publish_many(
        &mut self,
        broker: &str,
        topic: &str,
        group: Option<&str>,
        payload: &[u8],
        n: usize,
    ) -> Result<Vec<Offset>, BrokerError> {
        let broker = self
            .brokers
            .get(broker)
            .ok_or_else(|| BrokerError::Unknown(format!("broker {broker} not started")))?;

        let (pubh, _) = broker
            .get_publisher(topic, &group.map(|s| s.into()))
            .await?;

        let mut offsets = Vec::with_capacity(n);
        for _ in 0..n {
            offsets.push(pubh.publish(payload).await?);
        }
        Ok(offsets)
    }
}

pub struct TestConsumer {
    pub id: String,
    pub broker: String,
}

pub struct SubBuilder<'a> {
    state: &'a mut TestState,
    broker: String,
    topic: String,
    group: Option<String>,
    prefetch: usize,
}

impl TestState {
    pub fn sub(&mut self, broker: &str, topic: &str, group: Option<&str>) -> SubBuilder<'_> {
        SubBuilder {
            state: self,
            broker: broker.to_string(),
            topic: topic.to_string(),
            group: group.map(|s| s.to_string()),
            prefetch: 1,
        }
    }
}

impl SubBuilder<'_> {
    pub async fn wait(self) -> Result<TestConsumer, BrokerError> {
        let id = format!("c{}", self.state.next_consumer_id);
        self.state.next_consumer_id += 1;

        let handle = self.state.brokers[&self.broker]
            .subscribe(
                &self.topic,
                self.group.as_deref(),
                ConsumerConfig {
                    prefetch: self.prefetch,
                },
            )
            .await?;

        self.state
            .consumers
            .insert(id.clone(), (self.broker.clone(), handle));

        Ok(TestConsumer {
            id,
            broker: self.broker,
        })
    }
}

impl<'a> SubBuilder<'a> {
    pub async fn create(self) -> Result<TestConsumer, BrokerError> {
        let broker = self
            .state
            .brokers
            .get(&self.broker)
            .expect("broker not started");

        let handle = broker
            .subscribe(
                &self.topic,
                self.group.as_deref(),
                ConsumerConfig {
                    prefetch: self.prefetch,
                },
            )
            .await?;

        let id = format!("c{}", self.state.next_consumer_id);
        self.state.next_consumer_id += 1;

        self.state
            .consumers
            .insert(id.clone(), (self.broker.clone(), handle));

        Ok(TestConsumer {
            id,
            broker: self.broker,
        })
    }
}

impl<'a> SubBuilder<'a> {
    pub fn group(mut self, group: &str) -> Self {
        self.group = Some(group.to_string());
        self
    }

    pub fn prefetch(mut self, n: usize) -> Self {
        self.prefetch = n.max(1);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecvMsg {
    pub offset: Offset,
    pub tag: DeliveryTag,
}

impl TestState {
    pub async fn recv(&mut self, c: &TestConsumer) -> Result<RecvMsg, BrokerError> {
        let (_, h) = self.consumers.get_mut(&c.id).unwrap();
        let m = h
            .recv()
            .await
            .ok_or(BrokerError::Unknown("No message".into()))?;

        Ok(RecvMsg {
            offset: m.message.offset,
            tag: m.delivery_tag,
        })
    }

    pub async fn recv_n(
        &mut self,
        c: &TestConsumer,
        n: usize,
    ) -> Result<Vec<RecvMsg>, BrokerError> {
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            v.push(self.recv(c).await?);
        }
        Ok(v)
    }

    pub async fn expect_no_message(&mut self, c: &TestConsumer, timeout_ms: u64) {
        let res = tokio::time::timeout(Duration::from_millis(timeout_ms), self.recv(c)).await;

        match res {
            Err(_) => {} // timeout: good
            Ok(Ok(_)) => panic!("unexpected message received"),
            Ok(Err(e)) => panic!("recv failed unexpectedly: {e:?}"),
        }
    }

    pub async fn ack(&mut self, c: &TestConsumer, m: RecvMsg) -> Result<(), BrokerError> {
        let (_, h) = self.consumers.get_mut(&c.id).unwrap();
        h.settle(SettleRequest {
            settle_type: SettleType::Ack,
            delivery_tag: m.tag,
        })
        .await
    }

    pub async fn nack(
        &mut self,
        c: &TestConsumer,
        m: RecvMsg,
        requeue: bool,
    ) -> Result<(), BrokerError> {
        let (_, h) = self.consumers.get_mut(&c.id).unwrap();
        h.settle(SettleRequest {
            settle_type: SettleType::Nack {
                requeue: Some(requeue),
            },
            delivery_tag: m.tag,
        })
        .await
    }

    pub async fn reject(&mut self, c: &TestConsumer, m: RecvMsg) -> Result<(), BrokerError> {
        let (_, h) = self.consumers.get_mut(&c.id).unwrap();
        h.settle(SettleRequest {
            settle_type: SettleType::Reject {
                requeue: Some(false),
            },
            delivery_tag: m.tag,
        })
        .await
    }

    pub async fn ack_all(
        &mut self,
        c: &TestConsumer,
        msgs: Vec<RecvMsg>,
    ) -> Result<(), BrokerError> {
        for m in msgs {
            self.ack(c, m).await?;
        }
        Ok(())
    }

    pub async fn sleep_ms(&self, ms: u64) {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}
