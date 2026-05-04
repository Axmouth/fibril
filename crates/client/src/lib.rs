use fibril_storage::DeliveryTag;
use fibril_util::unix_millis;
use futures::{SinkExt, StreamExt};
use serde::de::DeserializeOwned;
use uuid::Uuid;
use std::{
    collections::HashMap,
    fmt,
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    sync::{Notify, mpsc, oneshot},
};
use tokio_util::codec::Framed;

use fibril_protocol::v1::{frame::ProtoCodec, helper::*, *};

// ===== Public API ============================================================

#[derive(Debug, Clone, Error)]
pub enum FibrilError {
    #[error("Client was disconnected: {msg}")]
    Disconnection { msg: String },
    #[error("Failed to deserialize data: {msg}")]
    DeserializationFailure { msg: String },
    #[error("Failed to serialize data: {msg}")]
    SerializationFailure { msg: String },
    #[error("Connection to the Client was severed, reconnection is advised")]
    BrokenPipe,
    #[error("Server returned error code {code}: {msg}")]
    Failure { code: u16, msg: String },
    #[error("EOF")]
    Eof,
    #[error("Unexpected error: {msg}")]
    Unexpected { msg: String },
}

pub type FibrilResult<T> = Result<T, FibrilError>;

// TODO: Explore From<..> impls for relevant error types
// TODO: Add opt in event per message sent/received

#[derive(Debug, Clone)]
pub struct Client {
    address: SocketAddr,
    opts: ClientOptions,
    engine: Arc<EngineHandle>,
}

#[derive(Debug, Clone)]
pub struct Publisher {
    engine: Arc<EngineHandle>,
    topic: String,
    group: Option<String>,
}

pub struct Subscription {
    rx: mpsc::Receiver<InflightMessage>,
}

pub struct AutoAckedSubscription {
    rx: mpsc::Receiver<Message>,
}

pub struct Message {
    pub delivery_tag: DeliveryTag,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn deserialize<T: DeserializeOwned>(&self) -> FibrilResult<T> {
        rmp_serde::from_slice(&self.payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() })
    }
}

pub enum SettleRequest {
    Ack { tag: DeliveryTag, request_id: u64, response: oneshot::Sender<Result<(), FibrilError>> },
    Nack { tag: DeliveryTag, requeue: bool, request_id: u64, response: oneshot::Sender<Result<(), FibrilError>> },
}

#[must_use]
pub struct InflightMessage {
    pub delivery_tag: DeliveryTag,
    pub payload: Vec<u8>,
    pub request_id: u64,
    settle: oneshot::Sender<SettleRequest>,
}

impl InflightMessage {
    pub async fn complete(self) -> FibrilResult<Message> {
        let (tx, rx) = oneshot::channel();
        self.settle
            .send(SettleRequest::Ack {
                tag: self.delivery_tag,
                request_id: self.request_id,
                response: tx,
            })
            .map_err(|_e| FibrilError::BrokenPipe)?;
        rx.await.map_err(|_| FibrilError::BrokenPipe)??;
        Ok(Message {
            delivery_tag: self.delivery_tag,
            payload: self.payload,
        })
    }

    pub async fn fail(self) -> FibrilResult<Message> {
        let (tx, rx) = oneshot::channel();
        self.settle
            .send(SettleRequest::Nack {
                tag: self.delivery_tag,
                requeue: false,
                request_id: self.request_id,
                response: tx,
            })
            .map_err(|_e| FibrilError::BrokenPipe)?;
        rx.await.map_err(|_| FibrilError::BrokenPipe)??;
        Ok(Message {
            delivery_tag: self.delivery_tag,
            payload: self.payload,
        })
    }

    // TODO: Implement timeout. Could just markinflight with now + timeout?
    pub async fn retry(self) -> FibrilResult<Message> {
        let (tx, rx) = oneshot::channel();
        self.settle
            .send(SettleRequest::Nack {
                tag: self.delivery_tag,
                requeue: true,
                request_id: self.request_id,
                response: tx,
            })
            .map_err(|_e| FibrilError::BrokenPipe)?;
        rx.await.map_err(|_| FibrilError::BrokenPipe)??;
        Ok(Message {
            delivery_tag: self.delivery_tag,
            payload: self.payload,
        })
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> FibrilResult<T> {
        rmp_serde::from_slice(&self.payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() })
    }
}

enum Waiter {
    Publish(oneshot::Sender<FibrilResult<u64>>),
    SubscribeManual(oneshot::Sender<FibrilResult<AckableSubChannel>>),
    SubscribeAuto(oneshot::Sender<FibrilResult<AutoAckedSubChannel>>),
}

#[derive(Debug, Clone)]
pub struct SubscriptionBuilder<'a> {
    client: &'a Client,
    topic: String,
    group: Option<String>,
    prefetch: u32,
}

impl<'a> SubscriptionBuilder<'a> {
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.group = Some(group.into());
        self
    }

    pub fn prefetch(mut self, prefetch: u32) -> Self {
        self.prefetch = prefetch;
        self
    }

    /// Messages must be acked explicitly; otherwise they may be redelivered.
    #[tracing::instrument(fields(topic = %self.topic, group = ?self.group, prefetch = %self.prefetch))]
    pub async fn sub_manual_ack(self) -> FibrilResult<Subscription> {
        let req = Subscribe {
            topic: self.topic,
            group: self.group,
            prefetch: self.prefetch,
            auto_ack: false,
        };

        let rx = self.client.engine.subscribe(req).await?;
        Ok(Subscription { rx })
    }

    /// Messages that have been received by the client will not be redelivered..
    #[tracing::instrument(fields(topic = %self.topic, group = ?self.group, prefetch = %self.prefetch))]
    pub async fn sub_auto_ack(self) -> FibrilResult<AutoAckedSubscription> {
        let req = Subscribe {
            topic: self.topic,
            group: self.group,
            prefetch: self.prefetch,
            auto_ack: false,
        };

        let rx = self.client.engine.subscribe_auto_ack(req).await?;
        Ok(AutoAckedSubscription { rx })
    }
}

// ===== Client API =============================================================

impl Client {
    /// Connect to a server socket.
    #[tracing::instrument(fields(address = ?address, opts = ?opts))]
    pub async fn connect(
        address: impl ToSocketAddrs + fmt::Debug,
        opts: ClientOptions,
    ) -> FibrilResult<Self> {
        let address = Self::convert_address(address)?;
        let stream = TcpStream::connect(address)
            .await
            .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
        let framed = Framed::new(stream, ProtoCodec);

        let engine = start_engine(framed, opts.clone()).await?;
        Ok(Client {
            engine,
            address,
            opts,
        })
    }

    /// Replaces the internal engine with a new connection.
    /// Existing Publishers/Subscriptions created from the old connection
    /// will remain "broken" (returning BrokenPipe/None).
    #[tracing::instrument(fields(address = ?self.address, opts = ?self.opts))]
    pub async fn reconnect(&mut self) -> FibrilResult<()> {
        let address = self.address;
        let opts = self.opts.clone();
        let stream = TcpStream::connect(address)
            .await
            .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;

        let framed = Framed::new(stream, ProtoCodec);

        // Start a fresh engine
        let new_engine = start_engine(framed, opts).await?;

        // Swap the handle
        self.engine = new_engine;

        Ok(())
    }

    /// Replaces the internal engine with a new connection.
    /// Will attempt to restore existing Publishers/Subscriptions created from the old connection
    /// returning an error if it does not fully succeed. Could lead to duplicated messages.
    // TODO: try to handle inflight acks etc (resend?)
    #[tracing::instrument(fields(address = ?self.address, opts = ?self.opts))]
    pub async fn reconnect_restore(&mut self) -> FibrilResult<()> {
        todo!()
    }

    fn convert_address(address: impl ToSocketAddrs + fmt::Debug) -> FibrilResult<SocketAddr> {
        let mut address_iter = address
            .to_socket_addrs()
            .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
        let first_address = if let Some(address) = address_iter.next() {
            address
        } else {
            return Err(FibrilError::Disconnection {
                msg: "No address provided".into(),
            });
        };
        if address_iter.next().is_some() {
            return Err(FibrilError::Disconnection {
                msg: "More than one addresses provided".into(),
            });
        }
        Ok(first_address)
    }

    /// Get a handle that you can use to publish messages to a specific topic.
    #[tracing::instrument(fields(topic = %topic))]
    pub fn publisher(&self, topic: impl Into<String> + fmt::Display) -> Publisher {
        Publisher {
            engine: self.engine.clone(),
            topic: topic.into(),
            group: None,
        }
    }

    /// Get a handle that you can use to publish messages to a specific grouped topic.
    #[tracing::instrument(fields(topic = %topic, group = %group))]
    pub fn publisher_grouped(
        &self,
        topic: impl Into<String> + fmt::Display,
        group: impl Into<String> + fmt::Display,
    ) -> Publisher {
        Publisher {
            engine: self.engine.clone(),
            topic: topic.into(),
            group: Some(group.into()),
        }
    }

    /// Subscribe to receive messages from a topic, with manual acknowledgements.
    pub fn subscribe(&'_ self, topic: impl Into<String> + fmt::Display) -> SubscriptionBuilder<'_> {
        SubscriptionBuilder {
            client: self,
            topic: topic.into(),
            group: None,
            prefetch: 1, // sensible default
        }
    }

    /// Gracefully shut down the client, closing the connection and all subscription channels.
    pub async fn shutdown(&self) {
        self.engine.shutdown.notify_waiters();
    }
}

impl Publisher {
    // TODO: return a confirmer handle?
    /// Publish a message with manual confirmation
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_unconfirmed<T: serde::Serialize>(&self, payload: &T) -> FibrilResult<()> {
        let bytes = rmp_serde::to_vec(payload)
            .map_err(|e| FibrilError::SerializationFailure { msg: e.to_string() })?;
        self.engine
            .publish_unconfirmed(self.topic.clone(), self.group.clone(), bytes)
            .await
        // TODO: use oneshot channel to wait for when the packet has left(better errors timing)?
    }

    /// Publish a message with automatic confirmation (only returns once the server's publish confirm is received)
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish<T: serde::Serialize>(&self, payload: &T) -> FibrilResult<u64> {
        let bytes = rmp_serde::to_vec(payload)
            .map_err(|e| FibrilError::SerializationFailure { msg: e.to_string() })?;
        self.engine
            .publish(self.topic.clone(), self.group.clone(), bytes)
            .await
        // TODO: use oneshot channel to wait for when the packet has left(better errors timing)?
        // TODO: Or return a notifier you can await? As like async fn PublishResult::confirmed()
    }
}

impl Subscription {
    pub async fn recv(&mut self) -> Option<InflightMessage> {
        self.rx.recv().await
    }

    // TODO: use tokio_stream::wrappers::ReceiverStream?
    pub fn into_stream(self) -> impl futures::Stream<Item = InflightMessage> {
        futures::stream::unfold(self, |mut s| async move {
            s.rx.recv().await.map(|msg| (msg, s))
        })
    }
}

impl AutoAckedSubscription {
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    // TODO: use tokio_stream::wrappers::ReceiverStream?
    pub fn into_stream(self) -> impl futures::Stream<Item = Message> {
        futures::stream::unfold(self, |mut s| async move {
            s.rx.recv().await.map(|msg| (msg, s))
        })
    }
}

// ===== Engine =================================================================

#[derive(Debug, Clone)]
struct EngineHandle {
    tx: mpsc::Sender<Command>,
    shutdown: Arc<Notify>,
}

#[derive(Debug)]
enum Command {
    PublishUnconfirmed {
        topic: String,
        group: Option<String>,
        payload: Vec<u8>,
        published: u64,
    },
    PublishConfirmed {
        topic: String,
        group: Option<String>,
        payload: Vec<u8>,
        published: u64,
        reply: oneshot::Sender<FibrilResult<u64>>,
    },
    Subscribe {
        req: Subscribe,
        reply: oneshot::Sender<FibrilResult<AckableSubChannel>>,
    },
    SubscribeAutoAcked {
        req: Subscribe,
        reply: oneshot::Sender<FibrilResult<AutoAckedSubChannel>>,
    },
    Ack {
        sub_id: u64,
        delivery_tag: DeliveryTag,
        request_id: u64,
    },
    Nack {
        sub_id: u64,
        delivery_tag: DeliveryTag,
        requeue: bool,
        request_id: u64,
    },
}

#[derive(Debug)]
struct AutoAckedSubChannel {
    auto: mpsc::Receiver<Message>,
}

#[derive(Debug)]
struct AckableSubChannel {
    manual: mpsc::Receiver<InflightMessage>,
}

#[derive(Debug, Clone)]
enum SubDelivery {
    Manual(mpsc::Sender<InflightMessage>),
    Auto(mpsc::Sender<Message>),
}

#[derive(Debug, Clone)]
struct SubState {
    topic: String,
    group: Option<String>,
    partition: u32,
    delivery: SubDelivery,
}

const DEFAULT_HEARTBEAT_INTERVAL: u64 = 5; // seconds

// TODO: Further reconnection attempts logic
// TODO: Better handle `t _ = framed.send(...)` errors, which currently just get swallowed. These errors indicate a broken connection and should trigger cleanup and reconnection logic.
async fn start_engine<S>(
    mut framed: Framed<S, ProtoCodec>,
    opts: ClientOptions,
) -> FibrilResult<Arc<EngineHandle>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let shutdown = Arc::new(Notify::new());
    let client_id;
    // handshake
    framed
        .send(encode(
            Op::Hello,
            1,
            &Hello {
                client_name: opts.client_name.clone(),
                client_version: opts.client_version.clone(),
                protocol_version: PROTOCOL_V1,
            },
        ))
        .await
        .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;

    let frame = framed
        .next()
        .await
        .ok_or(FibrilError::Eof)?
        .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
    match frame.opcode {
        x if x == Op::HelloOk as u16 => {
            let ho: HelloOk = decode(&frame);
            client_id = ho.client_id;
            if ho.compliance != COMPLIANCE_STRING {
                tracing::warn!(
                    id = "NF-SOVEREIGN-2025-GN-OPT-OUT-TDM",
                    expected = COMPLIANCE_STRING,
                    got = %ho.compliance,
                    "Invariant violated: compliance marker altered or missing"
                );
                return Err(FibrilError::Disconnection {
                    msg: "Protocol compliance marker mismatch".into(),
                });
            }
            if ho.protocol_version != PROTOCOL_V1 {
                return Err(FibrilError::Disconnection {
                    msg: "Protocol version mismatch".into(),
                });
            }
        }
        x if x == Op::HelloErr as u16 => {
            let e: ErrorMsg = decode(&frame);
            return Err(FibrilError::Failure {
                code: e.code,
                msg: e.message,
            });
        }
        _ => {
            return Err(FibrilError::Unexpected {
                msg: format!("Unexpected frame: opcode {}", frame.opcode),
            });
        }
    }

    if let Some(auth) = opts.auth {
        framed
            .send(encode(Op::Auth, 2, &auth))
            .await
            .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
        let frame = framed
            .next()
            .await
            .ok_or(FibrilError::Eof)?
            .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
        // TODO: prevent progress thiss AuthOk or AuthErr is received, IF auth is used
        match frame.opcode {
            x if x == Op::AuthOk as u16 => {}
            x if x == Op::AuthErr as u16 => {
                let e: ErrorMsg = decode(&frame);
                return Err(FibrilError::Failure {
                    code: e.code,
                    msg: e.message,
                });
            }
            _ => {
                return Err(FibrilError::Unexpected {
                    msg: format!("Unexpected auth frame: opcode {}", frame.opcode),
                });
            }
        }
    }

    let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(8192);
    let handle = Arc::new(EngineHandle {
        tx: cmd_tx.clone(),
        shutdown: shutdown.clone(),
    });

    let mut subs = HashMap::<u64, SubState>::new();

    let shutdown_engine = shutdown.clone();
    let shutdown_acks = shutdown.clone();

    // heartbeat task
    let heartbeat_secs = opts
        .heartbeat_interval
        .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);

    // writer + reader loop
    tokio::spawn(async move {
        let mut next_req = 1u64;
        let mut waiters: HashMap<u64, Waiter> = HashMap::new();
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await; // consume immediate tick

        let timeout = std::time::Duration::from_secs(heartbeat_secs * 3);
        let mut last_seen = tokio::time::Instant::now();

        // In the engine task, before the select! loop:
        let mut fatal_error: Option<FibrilError> = None;

        // Helper closure (or just an inline pattern):
        macro_rules! send_or_die {
            ($framed:expr, $frame:expr, $err_slot:expr) => {
                if let Err(e) = $framed.send($frame).await {
                    $err_slot = Some(FibrilError::Disconnection { msg: e.to_string() });
                    break;
                }
            };
        }

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    if last_seen.elapsed() > timeout {
                        tracing::warn!("Heartbeat timout, exiting event loop.");
                        fatal_error = Some(FibrilError::Disconnection { msg: "heartbeat timeout".into() });
                        break;
                    }
                    let req_id = next_req; next_req = next_req.wrapping_add(1);
                    send_or_die!(framed, encode(Op::Ping, req_id, &()), fatal_error)
                }

                _ = shutdown.notified() => {
                    tracing::info!("Shutting down, exiting event loop.");
                    break;
                }

                Some(cmd) = cmd_rx.recv() => match cmd {
                    Command::PublishUnconfirmed { topic, group, payload, published } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        let p = Publish {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: false,
                            payload,
                            published,
                        };
                        send_or_die!(framed, encode(Op::Publish, req_id, &p) , fatal_error)
                    }
                    Command::PublishConfirmed { topic, group, payload, published, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::Publish(reply));
                        let p = Publish {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: true,
                            payload,
                            published,
                        };
                        send_or_die!(framed, encode(Op::Publish, req_id, &p), fatal_error)
                    }
                    Command::Subscribe { req, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::SubscribeManual(reply));
                        send_or_die!(framed, encode(Op::Subscribe, req_id, &req), fatal_error)
                    }
                    Command::SubscribeAutoAcked { req, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::SubscribeAuto(reply));
                        send_or_die!(framed, encode(Op::Subscribe, req_id, &req),fatal_error)
                    }
                    Command::Ack { sub_id, delivery_tag, request_id } => {
                        if let Some(sub) = subs.get(&sub_id) {
                            let ack = Ack {
                                topic: sub.topic.clone(),
                                group: sub.group.clone(),
                                partition: sub.partition,
                                tags: vec![delivery_tag],
                            };
                            send_or_die!(framed, encode(Op::Ack, request_id, &ack), fatal_error)
                        }
                    }
                    Command::Nack { sub_id, delivery_tag, requeue, request_id } => {
                        if let Some(sub) = subs.get(&sub_id) {
                            let nack = Nack {
                                topic: sub.topic.clone(),
                                group: sub.group.clone(),
                                partition: sub.partition,
                                tags: vec![delivery_tag],
                                requeue,
                            };
                           send_or_die!(framed, encode(Op::Nack, request_id, &nack), fatal_error)
                        }
                    }
                },
                Some(frame) = framed.next() => {
                    let frame = match frame {
                        Ok(f) => f,
                        Err(err) => {
                            tracing::error!("Error receiving frame: {}", err);
                            fatal_error = Some(FibrilError::DeserializationFailure { msg: err.to_string() });
                            break;
                        }
                    };
                    last_seen = tokio::time::Instant::now();

                    match frame.opcode {
                        x if x == Op::PublishOk as u16 => {
                            let ok: PublishOk = decode(&frame);

                            match waiters.remove(&frame.request_id) {
                                Some(Waiter::Publish(tx)) => {
                                    // TODO: use delivery tag?
                                    let _ = tx.send(Ok(ok.offset));
                                }
                                Some(_other) => {
                                    // protocol violation: PublishOk for non-publish request
                                    // log + drop
                                    // TODO
                                    tracing::error!("Internal error: Wrong request/response match")
                                }
                                None => {
                                    // unexpected PublishOk (fire-and-forget or stale)
                                    // log + drop
                                    // Server must not send PublishOk unless require_confirm = true.
                                    // TODO
                                    tracing::error!("Internal error: unexpected PublishOk")
                                }
                            }
                        }
                        x if x == Op::Deliver as u16 => {
                            let d: Deliver = decode(&frame);
                            if let Some(sub) = subs.get(&d.sub_id) {
                                match &sub.delivery {
                                    SubDelivery::Manual(tx) => {
                                        let (ack_tx, ack_rx) = oneshot::channel();
                                        let msg = InflightMessage {
                                            delivery_tag: d.delivery_tag,
                                            payload: d.payload,
                                            settle: ack_tx,
                                            request_id: frame.request_id,
                                        };

                                        if tx.send(msg).await.is_ok() {
                                            let cmd_tx = cmd_tx.clone();
                                            let shutdown_acks = shutdown_acks.clone();
                                            let sub_id = d.sub_id;

                                            tokio::spawn(async move {
                                                // TODO: add timeout or use a shared queue, or have server handle timeout and add proper handling of relevant error

                                                tokio::select! {
                                                    Ok(settle_request) = ack_rx => {
                                                        match settle_request {
                                                            // TODO: Find way to notify of engine disconnection if this happens.
                                                            SettleRequest::Ack { tag, request_id, response } => {
                                                                let res = cmd_tx.send(Command::Ack {
                                                                    sub_id,
                                                                    delivery_tag: tag,
                                                                    request_id,
                                                                }).await;
                                                                if let Err(_err) = res {
                                                                    let _ = response.send(Err(FibrilError::BrokenPipe));
                                                                } else {
                                                                    let _ = response.send(Ok(()));
                                                                }
                                                            }
                                                            SettleRequest::Nack { tag, requeue, request_id, response } => {
                                                                let res = cmd_tx.send(Command::Nack {
                                                                    sub_id,
                                                                    delivery_tag: tag,
                                                                    requeue,
                                                                    request_id,
                                                                }).await;
                                                                if let Err(_err) = res {
                                                                    let _ = response.send(Err(FibrilError::BrokenPipe));
                                                                } else {
                                                                    let _ = response.send(Ok(()));
                                                                }
                                                            }
                                                        }
                                                    }
                                                    _ = shutdown_acks.notified() => {
                                                        // engine is shutting down
                                                    }
                                                }
                                            });
                                        }
                                    }

                                    SubDelivery::Auto(tx) => {
                                        let res = tx.send(Message {
                                            delivery_tag: d.delivery_tag,
                                            payload: d.payload,
                                        }).await;

                                        if res.is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }
                                }
                            }
                        }
                        x if x == Op::SubscribeOk as u16 => {
                            let ok: SubscribeOk = decode(&frame);

                            if let Some(waiter) = waiters.remove(&frame.request_id) {
                                match waiter {
                                    Waiter::SubscribeManual(tx) => {
                                        let (txm, rxm) = mpsc::channel(ok.prefetch as usize);

                                        subs.insert(ok.sub_id, SubState {
                                            topic: ok.topic.clone(),
                                            group: ok.group.clone(),
                                            partition: ok.partition,
                                            delivery: SubDelivery::Manual(txm),
                                        });

                                        let res = tx.send(Ok(AckableSubChannel { manual: rxm }));

                                        if res.is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }

                                    Waiter::SubscribeAuto(tx) => {
                                        let (txa, rxa) = mpsc::channel(ok.prefetch as usize);

                                        subs.insert(ok.sub_id, SubState {
                                            topic: ok.topic.clone(),
                                            group: ok.group.clone(),
                                            partition: ok.partition,
                                            delivery: SubDelivery::Auto(txa),
                                        });

                                        let res = tx.send(Ok(AutoAckedSubChannel { auto: rxa }));

                                        if res.is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }

                                    _ => {
                                        // protocol violation: SubscribeOk for non-subscribe request_id
                                        // TODO
                                        tracing::error!("Internal error: protocol violation: SubscribeOk for non-subscribe request_id")
                                    }
                                }
                            }
                        }
                        x if x == Op::Ping as u16 => {
                            let res = framed.send(encode(Op::Pong, frame.request_id, &())).await.map_err(|e| FibrilError::Disconnection { msg: e.to_string() });

                            if let Err(err) = res {
                                tracing::warn!("Broken pipe");
                                fatal_error = Some(err);
                                break;
                            }
                        }
                        x if x == Op::Pong as u16 => {
                            // pass
                        }
                        x if x == Op::Error as u16 => {
                            let err: ErrorMsg = decode(&frame);

                            if let Some(waiter) = waiters.remove(&frame.request_id) {
                                match waiter {
                                    Waiter::Publish(tx) => {
                                        let res = tx.send(Err(FibrilError::Failure {code: err.code, msg: err.message }));

                                        if res.is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }
                                    Waiter::SubscribeManual(tx) => {
                                        let res = tx.send(Err(FibrilError::Failure { code: err.code, msg: err.message }));

                                        if res.is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }
                                    Waiter::SubscribeAuto(tx) => {
                                        let res = tx.send(Err(FibrilError::Failure { code: err.code, msg: err.message }));

                                        if res.is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }
                                }
                            } else {
                                // connection-level error
                                // fail all waiters
                                let msg = format!("connection error {}: {}", err.code, err.message);
                                fatal_error = Some(FibrilError::Disconnection { msg: msg.clone() });
                                for (_, w) in waiters.drain() {
                                    fail_waiter(w, FibrilError::Disconnection { msg: msg.clone() });
                                }

                                // subs cleared
                                subs.clear();

                                // TODO: notify subscriptions
                                // TODO: possibly resubscribe
                                // TODO: possibly redeliver in-flight messages
                                // close all subscription channels
                                shutdown_engine.notify_waiters();

                                break; // or trigger reconnect
                            }
                        }
                        _ => {}
                    }
                }
                else => {
                    // EOF or channel closed
                    break;
                }
            }
        }

        // ================================
        // FAIL ALL PENDING WAITERS
        // ================================

        for (_, waiter) in waiters.drain() {
            fail_waiter(
                waiter,
                fatal_error.clone().unwrap_or_else(|| FibrilError::Disconnection { msg: "engine shutdown".into() }),
            );
        }

        // subs cleared
        subs.clear();

        // notify shutdown listeners
        shutdown.notify_waiters();
    });

    Ok(handle)
}

fn fail_waiter(waiter: Waiter, err: FibrilError) {
    match waiter {
        Waiter::Publish(tx) => {
            let _ = tx.send(Err(err));
        }
        Waiter::SubscribeManual(tx) => {
            let _ = tx.send(Err(err));
        }
        Waiter::SubscribeAuto(tx) => {
            let _ = tx.send(Err(err));
        }
    }
}

impl EngineHandle {
    async fn publish_unconfirmed(
        &self,
        topic: String,
        group: Option<String>,
        payload: Vec<u8>,
    ) -> FibrilResult<()> {
        let published = unix_millis();
        self.tx
            .send(Command::PublishUnconfirmed {
                topic,
                group,
                payload,
                published,
            })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        Ok(())
    }

    async fn publish(
        &self,
        topic: String,
        group: Option<String>,
        payload: Vec<u8>,
    ) -> FibrilResult<u64> {
        let (tx, rx) = oneshot::channel();
        let published = unix_millis();
        self.tx
            .send(Command::PublishConfirmed {
                topic,
                group,
                payload,
                published,
                reply: tx,
            })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        rx.await.map_err(|_e| FibrilError::BrokenPipe)?
    }

    async fn subscribe(&self, req: Subscribe) -> FibrilResult<mpsc::Receiver<InflightMessage>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Subscribe { req, reply: tx })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        let chans = rx.await.map_err(|_e| FibrilError::BrokenPipe)??;
        Ok(chans.manual)
        // TODO: use oneshot channel to wait for when the packet has left(better errors timing)?
    }

    async fn subscribe_auto_ack(&self, req: Subscribe) -> FibrilResult<mpsc::Receiver<Message>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SubscribeAutoAcked { req, reply: tx })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        let chans = rx.await.map_err(|_e| FibrilError::BrokenPipe)??;
        Ok(chans.auto)
        // TODO: use oneshot channel to wait for when the packet has left(better errors timing)?
    }
}

// ===== Options ===============================================================

#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub client_name: String,
    pub client_version: String,
    pub auth: Option<Auth>,
    pub heartbeat_interval: Option<u64>,
}

impl ClientOptions {
    pub fn new() -> Self {
        let client_version = env!("CARGO_PKG_VERSION");
        let client_name = "Fibril Rust Client";
        Self {
            client_name: client_name.into(),
            client_version: client_version.into(),
            auth: None,
            heartbeat_interval: None,
        }
    }

    pub fn auth(self, username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            auth: Some(Auth {
                username: username.into(),
                password: password.into(),
            }),
            ..self
        }
    }

    pub fn heartbeat_interval(self, interval: u64) -> Self {
        Self {
            heartbeat_interval: Some(interval),
            ..self
        }
    }

    pub async fn connect(self, address: impl ToSocketAddrs + fmt::Debug) -> FibrilResult<Client> {
        Client::connect(address, self).await
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self::new()
    }
}
