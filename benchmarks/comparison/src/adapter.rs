use crate::{Args, Broker};
use anyhow::{Context, Result, anyhow, ensure};
use async_nats::jetstream::{self, consumer, stream};
use futures::{StreamExt, future::BoxFuture, stream::BoxStream};
use lapin::{BasicProperties, Connection, ConnectionProperties, options::*, types::*};
use serde_json::{Value, json};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

pub type Confirmation = BoxFuture<'static, Result<()>>;
pub enum Publisher {
    Pool(Vec<Publisher>, AtomicUsize),
    Fibril(fibril_client::Publisher),
    Nats(jetstream::Context, String),
    Rabbit(lapin::Channel, String),
}
impl Publisher {
    pub async fn send(&self, payload: Vec<u8>, id: u64) -> Result<Confirmation> {
        self.send_checked(payload, Some(id)).await
    }
    pub async fn send_response(&self, payload: Vec<u8>) -> Result<Confirmation> {
        self.send_checked(payload, None).await
    }
    async fn send_checked(&self, payload: Vec<u8>, id: Option<u64>) -> Result<Confirmation> {
        let (leaf, id) = match self {
            Self::Pool(items, next) => (
                &items[next.fetch_add(1, Ordering::Relaxed) % items.len()],
                None,
            ),
            _ => (self, id),
        };
        Ok(match leaf {
            Self::Pool(..) => unreachable!("nested publisher pool"),
            Self::Fibril(p) => {
                let confirmation = p
                    .publish_with_confirmation(fibril_client::NewMessage::raw(payload))
                    .await?;
                Box::pin(async move {
                    let offset = confirmation.confirmed().await?;
                    ensure!(
                        id.is_none_or(|id| offset == id),
                        "Fibril confirmation offset mismatch: {offset} vs {id:?}"
                    );
                    Ok(())
                })
            }
            Self::Nats(p, subject) => {
                let confirmation = p.publish(subject.clone(), payload.into()).await?;
                Box::pin(async move {
                    let ack = confirmation.await?;
                    ensure!(
                        id.is_none_or(|id| ack.sequence == id + 1) && !ack.duplicate,
                        "NATS confirmation identity mismatch"
                    );
                    Ok(())
                })
            }
            Self::Rabbit(channel, queue) => {
                let confirmation = channel
                    .basic_publish(
                        "".into(),
                        queue.clone().into(),
                        BasicPublishOptions {
                            mandatory: true,
                            ..Default::default()
                        },
                        &payload,
                        BasicProperties::default().with_delivery_mode(2),
                    )
                    .await?;
                Box::pin(async move {
                    ensure!(
                        matches!(confirmation.await?, lapin::Confirmation::Ack(None)),
                        "Rabbit publish was nacked, returned, or not confirmed"
                    );
                    Ok(())
                })
            }
        })
    }
}

pub enum Delivery {
    Fibril(fibril_client::InflightMessage),
    Nats(jetstream::Message),
    Rabbit(lapin::message::Delivery),
}
impl Delivery {
    pub fn speculative(&self) -> bool {
        matches!(self, Self::Fibril(m) if m.headers.get("fibril.speculative").is_some_and(|v| v == "1"))
    }
    pub fn payload(&self) -> &[u8] {
        match self {
            Self::Fibril(m) => &m.payload,
            Self::Nats(m) => &m.payload,
            Self::Rabbit(m) => &m.data,
        }
    }
    pub fn check_offset(&self, id: Option<u64>) -> Result<()> {
        match self {
            // Rust SDK exposes a settlement tag, not the stored delivery offset.
            // Publish offsets and payload identities are checked separately.
            Self::Fibril(_) => {}
            Self::Nats(m) => {
                let info = m.info().map_err(|e| anyhow!(e.to_string()))?;
                ensure!(
                    id.is_none_or(|id| info.stream_sequence == id + 1) && info.delivered == 1,
                    "NATS sequence/redelivery mismatch"
                );
            }
            Self::Rabbit(m) => ensure!(!m.redelivered, "Rabbit redelivery in fault-free benchmark"),
        }
        Ok(())
    }
    pub async fn ack(self) -> Result<()> {
        match self {
            Self::Fibril(m) => {
                m.complete().await?;
            }
            Self::Nats(m) => m.ack().await.map_err(|e| anyhow!(e.to_string()))?,
            Self::Rabbit(m) => {
                ensure!(
                    m.ack(BasicAckOptions::default()).await?,
                    "Rabbit acknowledgement not sent"
                );
            }
        }
        Ok(())
    }
}

// Keeps both independent TCP connections alive through drain. No connection setup
// or queue declaration is part of the measured workload.
// One setup-only owner per run; boxing this enum would not bound workload memory.
#[allow(clippy::large_enum_variant)]
pub enum Connections {
    Pool(Vec<Connections>),
    Fibril(fibril_client::Client, fibril_client::Client),
    Nats(
        async_nats::Client,
        async_nats::Client,
        consumer::PullConsumer,
    ),
    Rabbit(Connection, Connection, lapin::Channel, String, bool),
}
impl Connections {
    pub async fn close_only(&mut self) -> Result<()> {
        match self {
            Self::Pool(_) => {
                anyhow::bail!("close_only is reserved for single-connection fault probes")
            }
            Self::Fibril(p, c) => {
                c.fetch_topology().await?;
                p.shutdown().await;
                c.shutdown().await;
            }
            Self::Nats(p, c, _) => {
                c.flush().await.map_err(|e| anyhow!(e.to_string()))?;
                p.flush().await.map_err(|e| anyhow!(e.to_string()))?;
            }
            Self::Rabbit(p, c, channel, queue, subscribed) => {
                // Finish the consumer's channel commands before connection
                // closure can requeue deliveries whose ACK is still in flight.
                if *subscribed {
                    channel
                        .basic_cancel("bench".into(), BasicCancelOptions::default())
                        .await?;
                }
                channel
                    .queue_declare(
                        queue.clone().into(),
                        QueueDeclareOptions {
                            passive: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await?;
                p.close(200, "probe complete".into()).await?;
                c.close(200, "probe complete".into()).await?;
            }
        }
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<Value> {
        if let Self::Pool(items) = self {
            let mut results = Vec::new();
            for item in items {
                results.push(item.flush_leaf().await?);
            }
            return Ok(json!(results));
        }
        self.flush_leaf().await
    }
    async fn flush_leaf(&mut self) -> Result<Value> {
        match self {
            Self::Pool(_) => unreachable!("nested connection pool"),
            Self::Fibril(p, c) => {
                // This profile uses one direct connection per client. A topology
                // round trip follows the queued ACKs on that same FIFO engine,
                // forcing their socket flush before shutdown can stop the engine.
                // The runner still checks broker settlement separately.
                c.fetch_topology().await?;
                p.shutdown().await;
                c.shutdown().await;
                Ok(
                    json!({"consumer_transport_barrier": "same_connection_topology_roundtrip", "server_settlement": "runner_admin_check_required"}),
                )
            }
            Self::Nats(p, c, consumer) => {
                c.flush().await.map_err(|e| anyhow!(e.to_string()))?;
                let info = tokio::time::timeout(Duration::from_secs(30), async {
                    loop {
                        let info = consumer.info().await?;
                        if info.num_pending == 0 && info.num_ack_pending == 0 {
                            return Ok::<_, anyhow::Error>(json!({"num_pending":info.num_pending,"num_ack_pending":info.num_ack_pending,"ack_floor_stream_sequence":info.ack_floor.stream_sequence}));
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }).await??;
                p.flush().await.map_err(|e| anyhow!(e.to_string()))?;
                Ok(info)
            }
            Self::Rabbit(p, c, channel, queue, subscribed) => {
                if *subscribed {
                    channel
                        .basic_cancel("bench".into(), BasicCancelOptions::default())
                        .await?;
                }
                let info = channel
                    .queue_declare(
                        queue.clone().into(),
                        QueueDeclareOptions {
                            passive: true,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await?;
                ensure!(
                    info.message_count() == 0,
                    "Rabbit queue still has ready messages"
                );
                p.close(200, "benchmark complete".into()).await?;
                c.close(200, "benchmark complete".into()).await?;
                Ok(json!({"ready":0, "server_settlement":"runner_admin_check_required"}))
            }
        }
    }
}

pub struct Prepared {
    pub publisher: Publisher,
    pub deliveries: BoxStream<'static, Result<Delivery>>,
    pub connections: Connections,
    pub settings: Value,
}

pub async fn prepare(args: &Args) -> Result<Prepared> {
    if args.connections == 1 || args.setup_only {
        return prepare_single(args).await;
    }
    let mut publishers = Vec::new();
    let mut streams = Vec::new();
    let mut connections = Vec::new();
    let mut settings = Vec::new();
    for index in 0..args.connections {
        let mut part = args.clone();
        part.allow_existing |= index > 0;
        // JetStream's durable consumer shares a single ACK budget across pulls.
        // Fibril and Rabbit credit is per subscription/channel.
        if !matches!(args.broker, Broker::Nats) {
            part.prefetch /= args.connections as u32;
        }
        part.pull_batch = (args.pull_batch / args.connections)
            .max(1)
            .min(part.prefetch as usize);
        let prepared = prepare_single(&part).await?;
        publishers.push(prepared.publisher);
        streams.push(prepared.deliveries);
        connections.push(prepared.connections);
        settings.push(prepared.settings);
    }
    Ok(Prepared {
        publisher: Publisher::Pool(publishers, AtomicUsize::new(0)),
        deliveries: futures::stream::select_all(streams).boxed(),
        connections: Connections::Pool(connections),
        settings: json!({"connection_pairs":args.connections,"total_prefetch":args.prefetch,
            "identity_namespace":"payload id; publisher chosen round-robin; merged consumers have no global order assertion",
            "connections":settings}),
    })
}
async fn prepare_single(args: &Args) -> Result<Prepared> {
    match args.broker {
        Broker::Fibril => {
            let connect = || {
                fibril_client::ClientOptions::new()
                    .auth("bench", "bench")
                    .publish_timeout_ms(args.drain_secs * 1000)
                    .connect(args.endpoint.as_str())
            };
            let p = connect().await?;
            p.declare_queue(fibril_client::QueueConfig::new(&args.queue)?.partitions(1))
                .await?;
            let c = connect().await?;
            let deliveries: BoxStream<'static, Result<Delivery>> = if args.setup_only {
                futures::stream::empty().boxed()
            } else {
                c.subscribe(&args.queue)?
                    .prefetch(args.prefetch)
                    .sub()
                    .await?
                    .into_stream()
                    .map(|m| Ok(Delivery::Fibril(m)))
                    .boxed()
            };
            Ok(Prepared {
                publisher: Publisher::Fibril(p.publisher(&args.queue)?),
                deliveries,
                connections: Connections::Fibril(p, c),
                settings: json!({"queue":"single partition", "confirm":if args.copies==3 {
                    "majority_durable requested; verify assignment externally"
                } else {
                    "local_durable requested; verify deployment policy externally"
                },
                    "consumer_ack":"individual", "copies":args.copies}),
            })
        }
        Broker::Nats => {
            let p = async_nats::connect(&args.endpoint)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
            let c = async_nats::connect(&args.endpoint)
                .await
                .map_err(|e| anyhow!(e.to_string()))?;
            let mut js = jetstream::context::ContextBuilder::new()
                .max_ack_inflight(args.confirm_window)
                .build(p.clone());
            js.set_timeout(Duration::from_secs(args.drain_secs));
            let stream_config = stream::Config {
                name: args.queue.clone(),
                subjects: vec![args.queue.clone()],
                retention: stream::RetentionPolicy::WorkQueue,
                storage: stream::StorageType::File,
                num_replicas: args.copies,
                ..Default::default()
            };
            // create_stream fails on incompatible existing config. The runner uses
            // fresh storage; standalone callers must provide an unused queue name.
            let stream = js
                .create_stream(stream_config)
                .await
                .context("NATS stream declaration")?;
            ensure!(
                args.allow_existing
                    || (stream.cached_info().state.messages == 0
                        && stream.cached_info().state.last_sequence == 0),
                "NATS stream is not new"
            );
            let reader_js = jetstream::new(c.clone());
            let reader_stream = reader_js
                .get_stream(&args.queue)
                .await
                .context("NATS consumer stream lookup")?;
            let consumer: consumer::PullConsumer = reader_stream
                .create_consumer(consumer::pull::Config {
                    durable_name: Some("bench".into()),
                    ack_policy: consumer::AckPolicy::Explicit,
                    ack_wait: Duration::from_secs(
                        if args.fault_role.is_some() || args.fault_setup {
                            2
                        } else {
                            args.warmup_secs + args.duration_secs + args.drain_secs + 60
                        },
                    ),
                    max_ack_pending: args.prefetch as i64,
                    ..Default::default()
                })
                .await
                .context("NATS durable consumer declaration")?;
            let settings = json!({"stream":stream.cached_info().config,
                "consumer":consumer.cached_info().config, "sync_interval":args.nats_sync});
            let deliveries: BoxStream<'static, Result<Delivery>> = if args.setup_only {
                futures::stream::empty().boxed()
            } else {
                consumer
                    .stream()
                    .max_messages_per_batch(args.pull_batch)
                    .messages()
                    .await?
                    .map(|m| m.map(Delivery::Nats).map_err(Into::into))
                    .boxed()
            };
            Ok(Prepared {
                publisher: Publisher::Nats(js, args.queue.clone()),
                deliveries,
                connections: Connections::Nats(p, c, consumer),
                settings,
            })
        }
        Broker::Rabbitmq => {
            let connect = || {
                Connection::connect_with_runtime(
                    &args.endpoint,
                    ConnectionProperties::default(),
                    async_rs::Runtime::tokio_current(),
                )
            };
            let p = connect().await?;
            let c = connect().await?;
            let pub_channel = p.create_channel().await?;
            let sub_channel = c.create_channel().await?;
            let mut fields = FieldTable::default();
            fields.insert(
                "x-queue-type".into(),
                AMQPValue::LongString("quorum".into()),
            );
            fields.insert(
                "x-quorum-initial-group-size".into(),
                AMQPValue::LongInt(args.copies as i32),
            );
            let info = pub_channel
                .queue_declare(
                    args.queue.clone().into(),
                    QueueDeclareOptions {
                        durable: true,
                        ..Default::default()
                    },
                    fields,
                )
                .await?;
            ensure!(
                args.allow_existing || (info.message_count() == 0 && info.consumer_count() == 0),
                "Rabbit queue not empty"
            );
            pub_channel
                .confirm_select(ConfirmSelectOptions::default())
                .await?;
            sub_channel
                .basic_qos(args.prefetch as u16, BasicQosOptions::default())
                .await?;
            let deliveries: BoxStream<'static, Result<Delivery>> = if args.setup_only {
                futures::stream::empty().boxed()
            } else {
                sub_channel
                    .basic_consume(
                        args.queue.clone().into(),
                        "bench".into(),
                        BasicConsumeOptions::default(),
                        FieldTable::default(),
                    )
                    .await?
                    .map(|m| m.map(Delivery::Rabbit).map_err(Into::into))
                    .boxed()
            };
            Ok(Prepared {
                publisher: Publisher::Rabbit(pub_channel, args.queue.clone()),
                deliveries,
                connections: Connections::Rabbit(
                    p,
                    c,
                    sub_channel,
                    args.queue.clone(),
                    !args.setup_only,
                ),
                settings: json!({"queue_type":"quorum", "copies":args.copies,"persistent":true,
                    "publisher_confirms":true,"mandatory":true,"consumer_ack":"individual"}),
            })
        }
    }
}
