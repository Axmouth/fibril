use fibril_storage::DeliveryTag;
use fibril_util::{UnixMillis, unix_millis};
use futures::{SinkExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
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

use fibril_protocol::v1::{
    frame::{Frame, ProtoCodec},
    helper::*,
    *,
};

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
    pub published: UnixMillis,
    pub publish_received: UnixMillis,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn content_type(&self) -> Option<&str> {
        self.headers.get("content-type").map(String::as_str)
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> FibrilResult<T> {
        deserialize_by_content_type(self.content_type(), &self.payload)
    }

    pub fn msg_pack<T: DeserializeOwned>(&self) -> FibrilResult<T> {
        rmp_serde::from_slice(&self.payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() })
    }

    pub fn json<T: DeserializeOwned>(&self) -> FibrilResult<T> {
        serde_json::from_slice(&self.payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() })
    }

    pub fn raw(&self) -> &[u8] {
        &self.payload
    }

    pub fn content(&self) -> Result<&str, FibrilError> {
        std::str::from_utf8(&self.payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() })
    }
}

pub struct NewMessage {
    pub payload: Vec<u8>,
    headers: HashMap<String, String>,
}

impl NewMessage {
    pub fn msg_pack<T: serde::Serialize>(payload: &T) -> FibrilResult<Self> {
        rmp_serde::to_vec(payload)
            .map(|payload| NewMessage::with_content_type(payload, "application/msgpack"))
            .map_err(|e| FibrilError::SerializationFailure { msg: e.to_string() })
    }

    pub fn json<T: serde::Serialize>(payload: &T) -> FibrilResult<Self> {
        serde_json::to_vec(payload)
            .map(|payload| NewMessage::with_content_type(payload, "application/json"))
            .map_err(|e| FibrilError::SerializationFailure { msg: e.to_string() })
    }

    pub fn raw(payload: Vec<u8>) -> Self {
        NewMessage {
            payload,
            headers: HashMap::new(),
        }
    }

    pub fn content(payload: impl Into<Vec<u8>>) -> Self {
        NewMessage::with_content_type(payload.into(), "text/plain; charset=utf-8")
    }

    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    pub fn content_type(self, content_type: impl Into<String>) -> Self {
        self.header("content-type", content_type)
    }

    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    fn with_content_type(payload: Vec<u8>, content_type: &str) -> Self {
        let mut headers = HashMap::new();
        headers.insert("content-type".into(), content_type.into());
        NewMessage { payload, headers }
    }
}

pub trait Publishable {
    fn into_message(self) -> FibrilResult<NewMessage>;
}

impl Publishable for NewMessage {
    fn into_message(self) -> FibrilResult<NewMessage> {
        Ok(self)
    }
}

impl<T: Serialize> Publishable for T {
    fn into_message(self) -> FibrilResult<NewMessage> {
        NewMessage::msg_pack(&self)
    }
}

pub enum SettleRequest {
    Ack {
        tag: DeliveryTag,
        request_id: u64,
        response: oneshot::Sender<Result<(), FibrilError>>,
    },
    Nack {
        tag: DeliveryTag,
        requeue: bool,
        request_id: u64,
        response: oneshot::Sender<Result<(), FibrilError>>,
    },
}

pub trait Delayable {
    fn with_delay(&self) -> std::time::Duration;

    fn deadline(&self) -> UnixMillis {
        unix_millis() + self.with_delay().as_millis() as u64
    }
}

impl Delayable for u64 {
    fn with_delay(&self) -> std::time::Duration {
        std::time::Duration::from_secs(*self)
    }
}

impl Delayable for u32 {
    fn with_delay(&self) -> std::time::Duration {
        std::time::Duration::from_secs((*self).into())
    }
}

impl Delayable for u16 {
    fn with_delay(&self) -> std::time::Duration {
        std::time::Duration::from_secs((*self).into())
    }
}

impl Delayable for u8 {
    fn with_delay(&self) -> std::time::Duration {
        std::time::Duration::from_secs((*self).into())
    }
}

impl Delayable for std::time::Duration {
    fn with_delay(&self) -> std::time::Duration {
        *self
    }
}

#[must_use]
pub struct InflightMessage {
    pub delivery_tag: DeliveryTag,
    pub published: UnixMillis,
    pub publish_received: UnixMillis,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
    pub request_id: u64,
    settle: oneshot::Sender<SettleRequest>,
}

impl InflightMessage {
    pub async fn complete(self) -> FibrilResult<Message> {
        let InflightMessage {
            delivery_tag,
            published,
            publish_received,
            headers,
            payload,
            request_id,
            settle,
        } = self;
        let (tx, rx) = oneshot::channel();
        settle
            .send(SettleRequest::Ack {
                tag: delivery_tag,
                request_id,
                response: tx,
            })
            .map_err(|_e| FibrilError::BrokenPipe)?;
        rx.await.map_err(|_| FibrilError::BrokenPipe)??;
        Ok(Message {
            delivery_tag,
            published,
            publish_received,
            headers,
            payload,
        })
    }

    pub async fn fail(self) -> FibrilResult<Message> {
        let InflightMessage {
            delivery_tag,
            published,
            publish_received,
            headers,
            payload,
            request_id,
            settle,
        } = self;
        let (tx, rx) = oneshot::channel();
        settle
            .send(SettleRequest::Nack {
                tag: delivery_tag,
                requeue: false,
                request_id,
                response: tx,
            })
            .map_err(|_e| FibrilError::BrokenPipe)?;
        rx.await.map_err(|_| FibrilError::BrokenPipe)??;
        Ok(Message {
            delivery_tag,
            published,
            publish_received,
            headers,
            payload,
        })
    }

    pub async fn retry(self) -> FibrilResult<Message> {
        let InflightMessage {
            delivery_tag,
            published,
            publish_received,
            headers,
            payload,
            request_id,
            settle,
        } = self;
        let (tx, rx) = oneshot::channel();
        settle
            .send(SettleRequest::Nack {
                tag: delivery_tag,
                requeue: true,
                request_id,
                response: tx,
            })
            .map_err(|_e| FibrilError::BrokenPipe)?;
        rx.await.map_err(|_| FibrilError::BrokenPipe)??;
        Ok(Message {
            delivery_tag,
            published,
            publish_received,
            headers,
            payload,
        })
    }

    pub async fn retry_after(self, delay: impl Delayable) -> FibrilResult<Message> {
        let _deadline = delay.deadline();
        todo!()
    }

    pub fn content_type(&self) -> Option<&str> {
        self.headers.get("content-type").map(String::as_str)
    }

    pub fn deserialize<T: DeserializeOwned>(&self) -> FibrilResult<T> {
        deserialize_by_content_type(self.content_type(), &self.payload)
    }
}

fn deserialize_by_content_type<T: DeserializeOwned>(
    content_type: Option<&str>,
    payload: &[u8],
) -> FibrilResult<T> {
    match content_type
        .and_then(|value| value.split(';').next())
        .map(str::trim)
    {
        Some("application/json") => serde_json::from_slice(payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() }),
        Some("application/msgpack") | None | Some("") => rmp_serde::from_slice(payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() }),
        Some(other) => Err(FibrilError::DeserializationFailure {
            msg: format!("unsupported content-type `{other}`"),
        }),
    }
}

fn decode_protocol<T: for<'de> serde::Deserialize<'de>>(frame: &Frame) -> FibrilResult<T> {
    try_decode(frame).map_err(|err| FibrilError::DeserializationFailure {
        msg: err.to_string(),
    })
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

// TODO: Replace serializeable with NewMessage struct, so the user can easily choose form of serialization
// TODO: perhaps use generics so that it defaults to message pack and can be used transparently
impl Publisher {
    // TODO: return a confirmer handle?
    /// Publish a message with manual confirmation
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_unconfirmed<T: Publishable>(&self, payload: T) -> FibrilResult<()> {
        let message = payload.into_message()?;
        self.engine
            .publish_unconfirmed(
                self.topic.clone(),
                self.group.clone(),
                message.headers,
                message.payload,
            )
            .await
        // TODO: use oneshot channel to wait for when the packet has left(better errors timing)?
    }

    /// Publish a message with automatic confirmation (only returns once the server's publish confirm is received)
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish<T: Publishable>(&self, payload: T) -> FibrilResult<u64> {
        let message = payload.into_message()?;
        self.engine
            .publish(
                self.topic.clone(),
                self.group.clone(),
                message.headers,
                message.payload,
            )
            .await
        // TODO: use oneshot channel to wait for when the packet has left(better errors timing)?
        // TODO: Or return a notifier you can await? As like async fn PublishResult::confirmed()
    }

    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_unconfirmed_delayed<T: Publishable, D: Delayable + Debug>(
        &self,
        payload: T,
        delay: D,
    ) -> FibrilResult<()> {
        let deadline = delay.deadline();
        let message = payload.into_message()?;
        self.engine
            .publish_unconfirmed_delayed(
                self.topic.clone(),
                self.group.clone(),
                message.headers,
                message.payload,
                deadline,
            )
            .await
    }

    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_delayed<T: Publishable, D: Delayable + Debug>(
        &self,
        payload: T,
        delay: D,
    ) -> FibrilResult<u64> {
        let deadline = delay.deadline();
        let message = payload.into_message()?;
        self.engine
            .publish_delayed(
                self.topic.clone(),
                self.group.clone(),
                message.headers,
                message.payload,
                deadline,
            )
            .await
    }

    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_with_delayed<T: Publishable, D: Delayable + Debug>(
        &self,
        payload: T,
        delay: D,
    ) -> FibrilResult<u64> {
        self.publish_delayed(payload, delay).await
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
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
    },
    PublishConfirmed {
        topic: String,
        group: Option<String>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
        reply: oneshot::Sender<FibrilResult<u64>>,
    },
    PublishDelayedUnconfirmed {
        topic: String,
        group: Option<String>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
        not_before: u64,
    },
    PublishDelayedConfirmed {
        topic: String,
        group: Option<String>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
        not_before: u64,
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

async fn send_protocol_frame<S, T>(
    framed: &mut Framed<S, ProtoCodec>,
    op: Op,
    request_id: u64,
    msg: &T,
) -> FibrilResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: Serialize,
{
    let frame = try_encode(op, request_id, msg).map_err(|err| FibrilError::Unexpected {
        msg: err.to_string(),
    })?;

    framed
        .send(frame)
        .await
        .map_err(|err| FibrilError::Disconnection {
            msg: err.to_string(),
        })
}

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
    // handshake
    send_protocol_frame(
        &mut framed,
        Op::Hello,
        1,
        &Hello {
            client_name: opts.client_name.clone(),
            client_version: opts.client_version.clone(),
            protocol_version: PROTOCOL_V1,
        },
    )
    .await?;

    let frame = framed
        .next()
        .await
        .ok_or(FibrilError::Eof)?
        .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
    match frame.opcode {
        x if x == Op::HelloOk as u16 => {
            let ho: HelloOk = decode_protocol(&frame)?;
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
            let e: ErrorMsg = decode_protocol(&frame)?;
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
        send_protocol_frame(&mut framed, Op::Auth, 2, &auth).await?;
        let frame = framed
            .next()
            .await
            .ok_or(FibrilError::Eof)?
            .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
        // TODO: prevent progress thiss AuthOk or AuthErr is received, IF auth is used
        match frame.opcode {
            x if x == Op::AuthOk as u16 => {}
            x if x == Op::AuthErr as u16 => {
                let e: ErrorMsg = decode_protocol(&frame)?;
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
            ($framed:expr, $op:expr, $request_id:expr, $msg:expr, $err_slot:expr) => {
                if let Err(e) = send_protocol_frame(&mut $framed, $op, $request_id, $msg).await {
                    $err_slot = Some(e);
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
                    send_or_die!(framed, Op::Ping, req_id, &(), fatal_error)
                }

                _ = shutdown.notified() => {
                    tracing::info!("Shutting down, exiting event loop.");
                    break;
                }

                Some(cmd) = cmd_rx.recv() => match cmd {
                    Command::PublishUnconfirmed { topic, group, headers, payload, published } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        let p = Publish {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: false,
                            headers,
                            payload,
                            published,
                        };
                        send_or_die!(framed, Op::Publish, req_id, &p, fatal_error)
                    }
                    Command::PublishConfirmed { topic, group, headers, payload, published, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::Publish(reply));
                        let p = Publish {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: true,
                            headers,
                            payload,
                            published,
                        };
                        send_or_die!(framed, Op::Publish, req_id, &p, fatal_error)
                    }
                    Command::PublishDelayedUnconfirmed { topic, group, headers, payload, published, not_before } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        let p = PublishDelayed {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: false,
                            not_before,
                            headers,
                            payload,
                            published,
                        };
                        send_or_die!(framed, Op::PublishDelayed, req_id, &p, fatal_error)
                    }
                    Command::PublishDelayedConfirmed { topic, group, headers, payload, published, not_before, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::Publish(reply));
                        let p = PublishDelayed {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: true,
                            not_before,
                            headers,
                            payload,
                            published,
                        };
                        send_or_die!(framed, Op::PublishDelayed, req_id, &p, fatal_error)
                    }
                    Command::Subscribe { req, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::SubscribeManual(reply));
                        send_or_die!(framed, Op::Subscribe, req_id, &req, fatal_error)
                    }
                    Command::SubscribeAutoAcked { req, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::SubscribeAuto(reply));
                        send_or_die!(framed, Op::Subscribe, req_id, &req, fatal_error)
                    }
                    Command::Ack { sub_id, delivery_tag, request_id } => {
                        if let Some(sub) = subs.get(&sub_id) {
                            let ack = Ack {
                                topic: sub.topic.clone(),
                                group: sub.group.clone(),
                                partition: sub.partition,
                                tags: vec![delivery_tag],
                            };
                            send_or_die!(framed, Op::Ack, request_id, &ack, fatal_error)
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
                           send_or_die!(framed, Op::Nack, request_id, &nack, fatal_error)
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
                            let ok: PublishOk = match decode_protocol(&frame) {
                                Ok(ok) => ok,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };

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
                            let d: Deliver = match decode_protocol(&frame) {
                                Ok(deliver) => deliver,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };
                            if let Some(sub) = subs.get(&d.sub_id) {
                                match &sub.delivery {
                                    SubDelivery::Manual(tx) => {
                                        let (ack_tx, ack_rx) = oneshot::channel();
                                        let msg = InflightMessage {
                                            delivery_tag: d.delivery_tag,
                                            published: d.published,
                                            publish_received: d.publish_received,
                                            headers: d.headers,
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
                                            published: d.published,
                                            publish_received: d.publish_received,
                                            headers: d.headers,
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
                            let ok: SubscribeOk = match decode_protocol(&frame) {
                                Ok(ok) => ok,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };

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
                            let res = send_protocol_frame(&mut framed, Op::Pong, frame.request_id, &()).await;

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
                            let err: ErrorMsg = match decode_protocol(&frame) {
                                Ok(err) => err,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };

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
                fatal_error
                    .clone()
                    .unwrap_or_else(|| FibrilError::Disconnection {
                        msg: "engine shutdown".into(),
                    }),
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
        headers: HashMap<String, String>,
        payload: Vec<u8>,
    ) -> FibrilResult<()> {
        let published = unix_millis();
        self.tx
            .send(Command::PublishUnconfirmed {
                topic,
                group,
                headers,
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
        headers: HashMap<String, String>,
        payload: Vec<u8>,
    ) -> FibrilResult<u64> {
        let (tx, rx) = oneshot::channel();
        let published = unix_millis();
        self.tx
            .send(Command::PublishConfirmed {
                topic,
                group,
                headers,
                payload,
                published,
                reply: tx,
            })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        rx.await.map_err(|_e| FibrilError::BrokenPipe)?
    }

    async fn publish_unconfirmed_delayed(
        &self,
        topic: String,
        group: Option<String>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        not_before: u64,
    ) -> FibrilResult<()> {
        let published = unix_millis();
        self.tx
            .send(Command::PublishDelayedUnconfirmed {
                topic,
                group,
                headers,
                payload,
                published,
                not_before,
            })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        Ok(())
    }

    async fn publish_delayed(
        &self,
        topic: String,
        group: Option<String>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        not_before: u64,
    ) -> FibrilResult<u64> {
        let (tx, rx) = oneshot::channel();
        let published = unix_millis();
        self.tx
            .send(Command::PublishDelayedConfirmed {
                topic,
                group,
                headers,
                payload,
                published,
                not_before,
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct TestPayload {
        value: u32,
    }

    #[test]
    fn new_message_exposes_headers_and_content_type() {
        let message = NewMessage::content("hello")
            .header("x-trace", "abc")
            .content_type("text/plain");

        assert_eq!(
            message.headers().get("x-trace").map(String::as_str),
            Some("abc")
        );
        assert_eq!(
            message.headers().get("content-type").map(String::as_str),
            Some("text/plain")
        );
    }

    #[test]
    fn deserialize_uses_json_content_type() {
        let message = NewMessage::json(&TestPayload { value: 42 }).unwrap();
        let message = Message {
            delivery_tag: DeliveryTag { epoch: 1 },
            published: 0,
            publish_received: 0,
            headers: message.headers,
            payload: message.payload,
        };

        assert_eq!(
            message.deserialize::<TestPayload>().unwrap(),
            TestPayload { value: 42 }
        );
    }

    #[test]
    fn deserialize_defaults_to_msgpack() {
        let message = NewMessage::msg_pack(&TestPayload { value: 7 }).unwrap();
        let message = Message {
            delivery_tag: DeliveryTag { epoch: 1 },
            published: 0,
            publish_received: 0,
            headers: HashMap::new(),
            payload: message.payload,
        };

        assert_eq!(
            message.deserialize::<TestPayload>().unwrap(),
            TestPayload { value: 7 }
        );
    }

    #[test]
    fn raw_message_has_no_implicit_headers() {
        let message = NewMessage::raw(b"raw".to_vec());

        assert!(message.headers().is_empty());
        assert_eq!(message.payload, b"raw".to_vec());
    }

    #[test]
    fn content_message_sets_text_content_type() {
        let message = NewMessage::content("hello");
        let message = Message {
            delivery_tag: DeliveryTag { epoch: 1 },
            published: 0,
            publish_received: 0,
            headers: message.headers,
            payload: message.payload,
        };

        assert_eq!(message.content_type(), Some("text/plain; charset=utf-8"));
        assert_eq!(message.content().unwrap(), "hello");
    }

    #[test]
    fn deserialize_rejects_unsupported_content_type() {
        let message = NewMessage::raw(b"{}".to_vec()).content_type("application/custom");
        let message = Message {
            delivery_tag: DeliveryTag { epoch: 1 },
            published: 0,
            publish_received: 0,
            headers: message.headers,
            payload: message.payload,
        };

        assert!(matches!(
            message.deserialize::<TestPayload>(),
            Err(FibrilError::DeserializationFailure { .. })
        ));
    }
}
