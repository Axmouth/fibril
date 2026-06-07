//! Async Rust client for a Fibril broker.
//!
//! The client is built around a single connection, topic-scoped publishers, and
//! subscriptions that either expose manual acknowledgements or client-side
//! auto-ack convenience.
//!
//! # Quick start
//!
//! ```no_run
//! use fibril_client::{ClientOptions, NewMessage};
//!
//! # async fn example() -> fibril_client::FibrilResult<()> {
//! let client = ClientOptions::new()
//!     .auth("fibril", "fibril")
//!     .connect("127.0.0.1:9876")
//!     .await?;
//!
//! let publisher = client.publisher("email.send")?;
//! let offset = publisher
//!     .publish_confirmed(NewMessage::json(&serde_json::json!({
//!         "to": "user@example.com",
//!         "template": "welcome",
//!     }))?)
//!     .await?;
//!
//! println!("published at offset {offset}");
//! client.shutdown().await;
//! # Ok(())
//! # }
//! ```

use fibril_storage::DeliveryTag;
use fibril_util::{UnixMillis, unix_millis};
use futures::{SinkExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    net::{SocketAddr, ToSocketAddrs},
    str::FromStr,
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

/// Error type returned by the Fibril Rust client.
///
/// Most operations return [`FibrilResult`]. Connection and shutdown related
/// failures are reported as [`FibrilError::Disconnection`] or
/// [`FibrilError::BrokenPipe`]; server-side request failures use
/// [`FibrilError::Failure`].
#[derive(Debug, Clone, Error)]
pub enum FibrilError {
    /// The TCP connection could not be established or was lost.
    #[error("Client was disconnected: {msg}")]
    Disconnection { msg: String },
    /// A payload or protocol message could not be decoded.
    #[error("Failed to deserialize data: {msg}")]
    DeserializationFailure { msg: String },
    /// A user payload could not be encoded for publishing.
    #[error("Failed to serialize data: {msg}")]
    SerializationFailure { msg: String },
    /// A topic or group name failed client-side validation.
    #[error("Invalid {kind} name `{name}`: {msg}")]
    InvalidName {
        kind: &'static str,
        name: String,
        msg: String,
    },
    /// The user-facing handle can no longer reach the connection engine.
    #[error("Connection to the Client was severed, reconnection is advised")]
    BrokenPipe,
    /// The broker rejected a request with a structured error response.
    #[error("Server returned error code {code}: {msg}")]
    Failure { code: u16, msg: String },
    /// The connection ended before the expected protocol exchange completed.
    #[error("EOF")]
    Eof,
    /// A protocol invariant or internal state expectation was violated.
    #[error("Unexpected error: {msg}")]
    Unexpected { msg: String },
}

/// Result alias used by the Fibril Rust client.
pub type FibrilResult<T> = Result<T, FibrilError>;

// TODO: Explore From<..> impls for relevant error types
// TODO: Add opt in event per message sent/received

/// Validated Fibril topic name.
///
/// Topic names must be 1-128 bytes, lowercase ASCII, and may contain digits,
/// `.`, `_`, and `-`. They cannot start or end with `.`, or contain `..`.
#[derive(Clone, Hash, Eq, PartialEq)]
pub struct TopicName {
    inner: Box<str>,
}

impl TopicName {
    /// Parse and validate a topic name.
    pub fn parse(value: impl AsRef<str>) -> FibrilResult<Self> {
        validate_name("topic", value.as_ref()).map(|inner| Self { inner })
    }

    /// Borrow the topic as a string slice.
    pub fn as_str(&self) -> &str {
        &self.inner
    }

    fn into_string(self) -> String {
        self.inner.into()
    }
}

impl Debug for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("TopicName").field(&self.as_str()).finish()
    }
}

impl fmt::Display for TopicName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TopicName {
    type Err = FibrilError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

/// Validated Fibril queue group name.
///
/// Group names use the same syntax as [`TopicName`].
#[derive(Clone, Hash, Eq, PartialEq)]
pub struct GroupName {
    inner: Box<str>,
}

impl GroupName {
    /// Parse and validate a group name.
    pub fn parse(value: impl AsRef<str>) -> FibrilResult<Self> {
        validate_name("group", value.as_ref()).map(|inner| Self { inner })
    }

    /// Borrow the group as a string slice.
    pub fn as_str(&self) -> &str {
        &self.inner
    }

    fn into_string(self) -> String {
        self.inner.into()
    }
}

impl Debug for GroupName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("GroupName").field(&self.as_str()).finish()
    }
}

impl fmt::Display for GroupName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GroupName {
    type Err = FibrilError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

fn validate_name(kind: &'static str, value: &str) -> FibrilResult<Box<str>> {
    let bytes = value.as_bytes();
    let len = bytes.len();
    if len == 0 || len > 128 {
        return invalid_name(kind, value, "must be 1-128 bytes");
    }
    if bytes[0] == b'.' || bytes[len - 1] == b'.' {
        return invalid_name(kind, value, "cannot start or end with '.'");
    }

    let mut prev_dot = false;
    for &byte in bytes {
        let ok = byte.is_ascii_lowercase()
            || byte.is_ascii_digit()
            || byte == b'.'
            || byte == b'_'
            || byte == b'-';
        if !ok {
            return invalid_name(
                kind,
                value,
                "must contain only lowercase ASCII letters, digits, '.', '_', or '-'",
            );
        }
        if byte == b'.' {
            if prev_dot {
                return invalid_name(kind, value, "cannot contain consecutive dots");
            }
            prev_dot = true;
        } else {
            prev_dot = false;
        }
    }

    Ok(value.into())
}

fn invalid_name<T>(kind: &'static str, name: &str, msg: &str) -> FibrilResult<T> {
    Err(FibrilError::InvalidName {
        kind,
        name: name.into(),
        msg: msg.into(),
    })
}

/// A connected Fibril broker client.
///
/// Clone values share the same underlying connection engine. Use
/// [`Client::publisher`] to create topic-scoped publishers and
/// [`Client::subscribe`] to create subscriptions.
///
/// ```no_run
/// use fibril_client::ClientOptions;
///
/// # async fn example() -> fibril_client::FibrilResult<()> {
/// let client = ClientOptions::new().connect("127.0.0.1:9876").await?;
/// let publisher = client.publisher("jobs")?;
/// publisher.publish("hello").await?;
/// client.shutdown().await;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Client {
    address: SocketAddr,
    opts: ClientOptions,
    engine: Arc<EngineHandle>,
}

/// Topic-scoped handle for publishing messages.
///
/// Create with [`Client::publisher`] or [`Client::publisher_grouped`]. Plain
/// serializable values are encoded as msgpack by default; use [`NewMessage`]
/// for JSON, text, raw bytes, or custom headers.
#[derive(Debug, Clone)]
pub struct Publisher {
    engine: Arc<EngineHandle>,
    topic: TopicName,
    group: Option<GroupName>,
}

/// Broker confirmation for a publish request.
///
/// Returned by [`Publisher::publish_with_confirmation`]. Await it when you need
/// the broker-assigned offset without serializing every publish on the
/// confirmation round trip.
#[derive(Debug)]
pub struct PublishConfirmation {
    rx: oneshot::Receiver<FibrilResult<u64>>,
}

impl PublishConfirmation {
    /// Wait for the broker-assigned topic offset.
    pub async fn confirmed(self) -> FibrilResult<u64> {
        self.rx.await.map_err(|_e| FibrilError::BrokenPipe)?
    }
}

/// Manual-acknowledgement subscription.
///
/// Messages received from this subscription are [`InflightMessage`] values and
/// must be settled with [`InflightMessage::complete`],
/// [`InflightMessage::fail`], or [`InflightMessage::retry`].
pub struct Subscription {
    rx: mpsc::Receiver<InflightMessage>,
}

/// Client-side auto-ack subscription.
///
/// This is a convenience mode that yields [`Message`] directly. Use manual ack
/// when processing correctness depends on explicit success/failure handling.
pub struct AutoAckedSubscription {
    rx: mpsc::Receiver<Message>,
}

/// Delivered message payload and metadata.
///
/// [`Message::deserialize`] chooses a decoder from content type metadata.
/// Missing or empty content type defaults to msgpack.
pub struct Message {
    /// Broker delivery tag used internally for acknowledgement.
    pub delivery_tag: DeliveryTag,
    /// Publisher-provided Unix timestamp in milliseconds.
    pub published: UnixMillis,
    /// Broker receive timestamp in Unix milliseconds.
    pub publish_received: UnixMillis,
    /// User headers plus any broker-provided headers. Content type is exposed
    /// separately through [`Message::content_type`].
    pub headers: HashMap<String, String>,
    /// Payload content type metadata, stored separately from user headers.
    pub content_type: Option<ContentType>,
    /// Raw message body bytes.
    pub payload: Vec<u8>,
}

impl Message {
    /// Return the message content type, if present.
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_ref().map(ContentType::as_header)
    }

    /// Deserialize using content type metadata.
    ///
    /// Supports `application/msgpack` (also the default when absent) and
    /// `application/json`.
    pub fn deserialize<T: DeserializeOwned>(&self) -> FibrilResult<T> {
        deserialize_by_content_type(self.content_type(), &self.payload)
    }

    /// Deserialize the payload as msgpack, ignoring content type metadata.
    pub fn msg_pack<T: DeserializeOwned>(&self) -> FibrilResult<T> {
        rmp_serde::from_slice(&self.payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() })
    }

    /// Deserialize the payload as JSON, ignoring content type metadata.
    pub fn json<T: DeserializeOwned>(&self) -> FibrilResult<T> {
        serde_json::from_slice(&self.payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() })
    }

    /// Borrow the raw payload bytes.
    pub fn raw(&self) -> &[u8] {
        &self.payload
    }

    /// Decode the payload as UTF-8 text.
    pub fn content(&self) -> Result<&str, FibrilError> {
        std::str::from_utf8(&self.payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() })
    }
}

/// Message builder for explicit publish payload encoding and headers.
///
/// Plain values passed to [`Publisher::publish`] are msgpack encoded
/// automatically. Use `NewMessage` when you want explicit encoding, raw bytes,
/// text, or custom headers.
///
/// ```no_run
/// use fibril_client::NewMessage;
///
/// # async fn example(publisher: fibril_client::Publisher) -> fibril_client::FibrilResult<()> {
/// publisher
///     .publish(
///         NewMessage::content("hello")
///             .header("x-trace-id", "abc123"),
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct NewMessage {
    /// Encoded payload bytes sent to the broker.
    pub payload: Vec<u8>,
    content_type: Option<ContentType>,
    headers: HashMap<String, String>,
}

impl NewMessage {
    /// Encode a serializable value as msgpack and set `application/msgpack`.
    pub fn msg_pack<T: serde::Serialize>(payload: &T) -> FibrilResult<Self> {
        rmp_serde::to_vec(payload)
            .map(|payload| NewMessage::with_content_type(payload, "application/msgpack"))
            .map_err(|e| FibrilError::SerializationFailure { msg: e.to_string() })
    }

    /// Encode a serializable value as JSON and set `application/json`.
    pub fn json<T: serde::Serialize>(payload: &T) -> FibrilResult<Self> {
        serde_json::to_vec(payload)
            .map(|payload| NewMessage::with_content_type(payload, "application/json"))
            .map_err(|e| FibrilError::SerializationFailure { msg: e.to_string() })
    }

    /// Publish raw bytes without setting a content type.
    pub fn raw(payload: Vec<u8>) -> Self {
        NewMessage {
            payload,
            content_type: None,
            headers: HashMap::new(),
        }
    }

    /// Publish UTF-8 text and set `text/plain; charset=utf-8`.
    pub fn content(payload: impl Into<Vec<u8>>) -> Self {
        NewMessage::with_content_type(payload.into(), "text/plain; charset=utf-8")
    }

    /// Add or replace a header.
    ///
    /// Fibril reserves `fibril.*` and `stroma.*` headers for system metadata;
    /// user code should avoid those prefixes.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        if key.eq_ignore_ascii_case("content-type") {
            self.content_type = Some(ContentType::from_header(value));
        } else {
            self.headers.insert(key, value);
        }
        self
    }

    /// Set content type metadata.
    pub fn content_type(self, content_type: impl Into<String>) -> Self {
        self.header("content-type", content_type)
    }

    /// Borrow the extra headers that will be sent with this message.
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// Return the content type metadata that will be sent with this message.
    pub fn content_type_value(&self) -> Option<&str> {
        self.content_type.as_ref().map(ContentType::as_header)
    }

    fn with_content_type(payload: Vec<u8>, content_type: &str) -> Self {
        NewMessage {
            payload,
            content_type: Some(ContentType::from_header(content_type)),
            headers: HashMap::new(),
        }
    }
}

/// Value that can be published by a [`Publisher`].
///
/// [`NewMessage`] preserves its explicit payload and headers. Other
/// serializable values are encoded with [`NewMessage::msg_pack`].
pub trait Publishable {
    /// Convert into a publishable message.
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

#[doc(hidden)]
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

/// Type accepted as a delayed publish interval.
///
/// Numeric implementations are interpreted as seconds in the Rust client.
/// Use [`std::time::Duration`] when the unit should be explicit.
pub trait Delayable {
    /// Convert the value into a relative delay.
    fn with_delay(&self) -> std::time::Duration;

    /// Convert the relative delay into a Unix-millisecond deadline.
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
/// Delivered message that is leased to a manual-ack subscription.
///
/// Dropping an `InflightMessage` without settling it does not acknowledge the
/// message. Settle it with [`complete`](Self::complete),
/// [`fail`](Self::fail), or [`retry`](Self::retry).
pub struct InflightMessage {
    /// Broker delivery tag used for acknowledgement.
    pub delivery_tag: DeliveryTag,
    /// Publisher-provided Unix timestamp in milliseconds.
    pub published: UnixMillis,
    /// Broker receive timestamp in Unix milliseconds.
    pub publish_received: UnixMillis,
    /// User headers plus any broker-provided headers. Content type is exposed
    /// separately through [`InflightMessage::content_type`].
    pub headers: HashMap<String, String>,
    /// Payload content type metadata, stored separately from user headers.
    pub content_type: Option<ContentType>,
    /// Raw message body bytes.
    pub payload: Vec<u8>,
    #[doc(hidden)]
    pub request_id: u64,
    settle: oneshot::Sender<SettleRequest>,
}

impl InflightMessage {
    /// Acknowledge successful processing and return the settled message.
    ///
    /// After this succeeds, the broker can advance the queue's settled
    /// frontier when earlier messages are also settled.
    pub async fn complete(self) -> FibrilResult<Message> {
        let InflightMessage {
            delivery_tag,
            published,
            publish_received,
            headers,
            content_type,
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
            content_type,
            payload,
        })
    }

    /// Negatively acknowledge without requeueing.
    ///
    /// Depending on queue configuration, the broker may drop or dead-letter the
    /// message.
    pub async fn fail(self) -> FibrilResult<Message> {
        let InflightMessage {
            delivery_tag,
            published,
            publish_received,
            headers,
            content_type,
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
            content_type,
            payload,
        })
    }

    /// Negatively acknowledge and make the message eligible for redelivery.
    pub async fn retry(self) -> FibrilResult<Message> {
        let InflightMessage {
            delivery_tag,
            published,
            publish_received,
            headers,
            content_type,
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
            content_type,
            payload,
        })
    }

    /// Negatively acknowledge and retry after a delay.
    ///
    /// This method is not wired yet and will panic. Use [`retry`](Self::retry)
    /// for supported immediate retry behavior.
    pub async fn retry_after(self, delay: impl Delayable) -> FibrilResult<Message> {
        let _deadline = delay.deadline();
        todo!()
    }

    /// Return the message content type, if present.
    pub fn content_type(&self) -> Option<&str> {
        self.content_type.as_ref().map(ContentType::as_header)
    }

    /// Deserialize using content type metadata.
    ///
    /// Supports `application/msgpack` (also the default when absent) and
    /// `application/json`.
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
    DeclareQueue(oneshot::Sender<FibrilResult<()>>),
    SubscribeManual(oneshot::Sender<FibrilResult<AckableSubChannel>>),
    SubscribeAuto(oneshot::Sender<FibrilResult<AutoAckedSubChannel>>),
}

#[derive(Debug, Clone)]
pub enum DeadLetterPolicy {
    Discard,
    Global,
    Custom {
        topic: TopicName,
        group: Option<GroupName>,
    },
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    topic: TopicName,
    group: Option<GroupName>,
    dlq_policy: Option<DeadLetterPolicy>,
    dlq_max_retries: Option<u32>,
}

impl QueueConfig {
    /// Start a queue declaration for a topic.
    pub fn new(topic: impl AsRef<str>) -> FibrilResult<Self> {
        Ok(Self {
            topic: TopicName::parse(topic)?,
            group: None,
            dlq_policy: None,
            dlq_max_retries: None,
        })
    }

    /// Declare behavior for a grouped queue namespace.
    pub fn group(mut self, group: impl AsRef<str>) -> FibrilResult<Self> {
        self.group = Some(GroupName::parse(group)?);
        Ok(self)
    }

    /// Set the number of retries before the queue's dead-letter policy applies.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.dlq_max_retries = Some(max_retries);
        self
    }

    /// Drop messages after retries are exhausted.
    pub fn discard_dead_letters(mut self) -> Self {
        self.dlq_policy = Some(DeadLetterPolicy::Discard);
        self
    }

    /// Send messages to the configured global dead-letter queue after retries are exhausted.
    pub fn use_global_dead_letter_queue(mut self) -> Self {
        self.dlq_policy = Some(DeadLetterPolicy::Global);
        self
    }

    /// Send messages to a custom ungrouped dead-letter queue after retries are exhausted.
    pub fn custom_dead_letter_queue(mut self, topic: impl AsRef<str>) -> FibrilResult<Self> {
        self.dlq_policy = Some(DeadLetterPolicy::Custom {
            topic: TopicName::parse(topic)?,
            group: None,
        });
        Ok(self)
    }

    /// Send messages to a custom grouped dead-letter queue after retries are exhausted.
    pub fn custom_dead_letter_queue_grouped(
        mut self,
        topic: impl AsRef<str>,
        group: impl AsRef<str>,
    ) -> FibrilResult<Self> {
        self.dlq_policy = Some(DeadLetterPolicy::Custom {
            topic: TopicName::parse(topic)?,
            group: Some(GroupName::parse(group)?),
        });
        Ok(self)
    }

    fn into_wire(self) -> DeclareQueue {
        let dlq_policy = self.dlq_policy.map(|policy| match policy {
            DeadLetterPolicy::Discard => QueueDlqPolicy::Discard,
            DeadLetterPolicy::Global => QueueDlqPolicy::Global,
            DeadLetterPolicy::Custom { topic, group } => QueueDlqPolicy::Custom {
                topic: topic.into_string(),
                group: group.map(GroupName::into_string),
            },
        });

        DeclareQueue {
            topic: self.topic.into_string(),
            group: self.group.map(GroupName::into_string),
            dlq_policy,
            dlq_max_retries: self.dlq_max_retries,
        }
    }
}

#[derive(Debug, Clone)]
/// Builder for subscription options.
///
/// Construct with [`Client::subscribe`], optionally set a group and prefetch,
/// then choose manual or auto acknowledgement.
///
/// ```no_run
/// # async fn example(client: fibril_client::Client) -> fibril_client::FibrilResult<()> {
/// let mut sub = client
///     .subscribe("email.send")?
///     .group("workers")?
///     .prefetch(32)
///     .sub_manual_ack()
///     .await?;
///
/// while let Some(msg) = sub.recv().await {
///     msg.complete().await?;
/// }
/// # Ok(())
/// # }
/// ```
pub struct SubscriptionBuilder<'a> {
    client: &'a Client,
    topic: TopicName,
    group: Option<GroupName>,
    prefetch: u32,
}

impl<'a> SubscriptionBuilder<'a> {
    /// Set the queue group.
    ///
    /// A group is an optional queue namespace under the topic.
    pub fn group(mut self, group: impl AsRef<str>) -> FibrilResult<Self> {
        self.group = Some(GroupName::parse(group)?);
        Ok(self)
    }

    /// Set the maximum number of messages the broker may lease ahead.
    ///
    /// Higher values improve throughput but increase the number of messages
    /// that may need redelivery if the client disconnects before settling them.
    pub fn prefetch(mut self, prefetch: u32) -> Self {
        self.prefetch = prefetch;
        self
    }

    /// Subscribe with manual acknowledgements.
    ///
    /// Each received [`InflightMessage`] must be settled explicitly.
    #[tracing::instrument(fields(topic = %self.topic, group = ?self.group, prefetch = %self.prefetch))]
    pub async fn sub_manual_ack(self) -> FibrilResult<Subscription> {
        let req = Subscribe {
            topic: self.topic.into_string(),
            group: self.group.map(GroupName::into_string),
            prefetch: self.prefetch,
            auto_ack: false,
        };

        let rx = self.client.engine.subscribe(req).await?;
        Ok(Subscription { rx })
    }

    /// Subscribe with client-side automatic acknowledgement.
    ///
    /// This yields [`Message`] directly. Prefer manual acknowledgement when
    /// processing correctness matters.
    #[tracing::instrument(fields(topic = %self.topic, group = ?self.group, prefetch = %self.prefetch))]
    pub async fn sub_auto_ack(self) -> FibrilResult<AutoAckedSubscription> {
        let req = Subscribe {
            topic: self.topic.into_string(),
            group: self.group.map(GroupName::into_string),
            prefetch: self.prefetch,
            auto_ack: true,
        };

        let rx = self.client.engine.subscribe_auto_ack(req).await?;
        Ok(AutoAckedSubscription { rx })
    }
}

// ===== Client API =============================================================

impl Client {
    /// Connect to a broker TCP socket.
    ///
    /// The address must resolve to exactly one socket address. Use
    /// [`ClientOptions::connect`] for the builder-style equivalent.
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

    /// Replace the internal engine with a new connection.
    ///
    /// Existing [`Publisher`] and [`Subscription`] handles created from the old
    /// connection remain attached to the old engine and will fail with
    /// [`FibrilError::BrokenPipe`] or end their receive streams.
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

    /// Reconnect and restore existing handles.
    ///
    /// This is not implemented yet. Use [`reconnect`](Self::reconnect) and
    /// recreate publishers/subscriptions explicitly.
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

    /// Create a publisher for a topic without a group.
    #[tracing::instrument(fields(topic = ?topic))]
    pub fn publisher(&self, topic: impl AsRef<str> + fmt::Debug) -> FibrilResult<Publisher> {
        Ok(Publisher {
            engine: self.engine.clone(),
            topic: TopicName::parse(topic)?,
            group: None,
        })
    }

    /// Create a publisher for a grouped queue.
    ///
    /// Grouping writes to an optional queue namespace under the topic.
    #[tracing::instrument(fields(topic = ?topic, group = ?group))]
    pub fn publisher_grouped(
        &self,
        topic: impl AsRef<str> + fmt::Debug,
        group: impl AsRef<str> + fmt::Debug,
    ) -> FibrilResult<Publisher> {
        Ok(Publisher {
            engine: self.engine.clone(),
            topic: TopicName::parse(topic)?,
            group: Some(GroupName::parse(group)?),
        })
    }

    /// Start building a subscription to a topic.
    pub fn subscribe(
        &'_ self,
        topic: impl AsRef<str> + fmt::Debug,
    ) -> FibrilResult<SubscriptionBuilder<'_>> {
        Ok(SubscriptionBuilder {
            client: self,
            topic: TopicName::parse(topic)?,
            group: None,
            prefetch: 1, // sensible default
        })
    }

    /// Declare queue behavior such as retry and dead-letter policy.
    ///
    /// Queue declarations apply to the topic plus optional group. Partition
    /// selection is internal.
    #[tracing::instrument(skip(self), fields(topic = %config.topic, group = ?config.group))]
    pub async fn declare_queue(&self, config: QueueConfig) -> FibrilResult<()> {
        self.engine.declare_queue(config.into_wire()).await
    }

    /// Gracefully shut down the client.
    ///
    /// This closes the connection engine and wakes subscription receivers.
    pub async fn shutdown(&self) {
        self.engine.shutdown.notify_waiters();
    }
}

// TODO: Replace serializeable with NewMessage struct, so the user can easily choose form of serialization
// TODO: perhaps use generics so that it defaults to message pack and can be used transparently
impl Publisher {
    /// Publish without waiting for broker confirmation.
    ///
    /// This is the common path. It only waits for the command to be accepted by
    /// the local engine. Use [`publish_confirmed`](Self::publish_confirmed)
    /// when you need the broker-assigned offset.
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish<T: Publishable>(&self, payload: T) -> FibrilResult<()> {
        let message = payload.into_message()?;
        self.engine
            .publish_unconfirmed(
                self.topic.as_str().to_string(),
                self.group.as_ref().map(|group| group.as_str().to_string()),
                message.content_type,
                message.headers,
                message.payload,
            )
            .await
        // TODO: use oneshot channel to wait for when the packet has left(better errors timing)?
    }

    /// Publish and wait for broker confirmation.
    ///
    /// Resolves with the broker-assigned topic offset.
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_confirmed<T: Publishable>(&self, payload: T) -> FibrilResult<u64> {
        self.publish_with_confirmation(payload)
            .await?
            .confirmed()
            .await
    }

    /// Publish and return a handle that can be awaited for broker confirmation.
    ///
    /// This sends a confirmed publish request but does not wait for the
    /// confirmation before returning. Keep the returned handle and await
    /// [`PublishConfirmation::confirmed`] later to pipeline multiple publishes.
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_with_confirmation<T: Publishable>(
        &self,
        payload: T,
    ) -> FibrilResult<PublishConfirmation> {
        let message = payload.into_message()?;
        self.engine
            .publish_with_confirmation(
                self.topic.as_str().to_string(),
                self.group.as_ref().map(|group| group.as_str().to_string()),
                message.content_type,
                message.headers,
                message.payload,
            )
            .await
    }

    /// Publish after a relative delay without waiting for broker confirmation.
    ///
    /// Numeric Rust delays are seconds; use [`std::time::Duration`] for
    /// explicit units.
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_delayed<T: Publishable, D: Delayable + Debug>(
        &self,
        payload: T,
        delay: D,
    ) -> FibrilResult<()> {
        let deadline = delay.deadline();
        let message = payload.into_message()?;
        self.engine
            .publish_unconfirmed_delayed(
                self.topic.as_str().to_string(),
                self.group.as_ref().map(|group| group.as_str().to_string()),
                message.content_type,
                message.headers,
                message.payload,
                deadline,
            )
            .await
    }

    /// Publish after a relative delay and wait for broker confirmation.
    ///
    /// Resolves with the broker-assigned topic offset. Numeric Rust delays are
    /// seconds; use [`std::time::Duration`] for explicit units.
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_delayed_confirmed<T: Publishable, D: Delayable + Debug>(
        &self,
        payload: T,
        delay: D,
    ) -> FibrilResult<u64> {
        self.publish_delayed_with_confirmation(payload, delay)
            .await?
            .confirmed()
            .await
    }

    /// Publish after a relative delay and return a broker-confirmation handle.
    ///
    /// Numeric Rust delays are seconds; use [`std::time::Duration`] for
    /// explicit units.
    #[tracing::instrument(skip(payload), fields(topic = %self.topic))]
    pub async fn publish_delayed_with_confirmation<T: Publishable, D: Delayable + Debug>(
        &self,
        payload: T,
        delay: D,
    ) -> FibrilResult<PublishConfirmation> {
        let deadline = delay.deadline();
        let message = payload.into_message()?;
        self.engine
            .publish_delayed_with_confirmation(
                self.topic.as_str().to_string(),
                self.group.as_ref().map(|group| group.as_str().to_string()),
                message.content_type,
                message.headers,
                message.payload,
                deadline,
            )
            .await
    }
}

impl Subscription {
    /// Receive the next manual-ack message.
    ///
    /// Returns `None` when the subscription channel closes.
    pub async fn recv(&mut self) -> Option<InflightMessage> {
        self.rx.recv().await
    }

    /// Convert this subscription into a stream of manual-ack messages.
    pub fn into_stream(self) -> impl futures::Stream<Item = InflightMessage> {
        futures::stream::unfold(self, |mut s| async move {
            s.rx.recv().await.map(|msg| (msg, s))
        })
    }
}

impl AutoAckedSubscription {
    /// Receive the next auto-ack message.
    ///
    /// Returns `None` when the subscription channel closes.
    pub async fn recv(&mut self) -> Option<Message> {
        self.rx.recv().await
    }

    /// Convert this subscription into a stream of messages.
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
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
    },
    PublishConfirmed {
        topic: String,
        group: Option<String>,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
        reply: oneshot::Sender<FibrilResult<u64>>,
    },
    PublishDelayedUnconfirmed {
        topic: String,
        group: Option<String>,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
        not_before: u64,
    },
    PublishDelayedConfirmed {
        topic: String,
        group: Option<String>,
        content_type: Option<ContentType>,
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
    DeclareQueue {
        req: DeclareQueue,
        reply: oneshot::Sender<FibrilResult<()>>,
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
                    Command::PublishUnconfirmed { topic, group, content_type, headers, payload, published } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        let p = Publish {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: false,
                            content_type,
                            headers,
                            payload,
                            published,
                        };
                        send_or_die!(framed, Op::Publish, req_id, &p, fatal_error)
                    }
                    Command::PublishConfirmed { topic, group, content_type, headers, payload, published, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::Publish(reply));
                        let p = Publish {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: true,
                            content_type,
                            headers,
                            payload,
                            published,
                        };
                        send_or_die!(framed, Op::Publish, req_id, &p, fatal_error)
                    }
                    Command::PublishDelayedUnconfirmed { topic, group, content_type, headers, payload, published, not_before } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        let p = PublishDelayed {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: false,
                            not_before,
                            content_type,
                            headers,
                            payload,
                            published,
                        };
                        send_or_die!(framed, Op::PublishDelayed, req_id, &p, fatal_error)
                    }
                    Command::PublishDelayedConfirmed { topic, group, content_type, headers, payload, published, not_before, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::Publish(reply));
                        let p = PublishDelayed {
                            topic,
                            group,
                            partition: 0,
                            require_confirm: true,
                            not_before,
                            content_type,
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
                    Command::DeclareQueue { req, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::DeclareQueue(reply));
                        send_or_die!(framed, Op::DeclareQueue, req_id, &req, fatal_error)
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
                                            content_type: d.content_type,
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
                                            content_type: d.content_type,
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
                        x if x == Op::DeclareQueueOk as u16 => {
                            let _ok: DeclareQueueOk = match decode_protocol(&frame) {
                                Ok(ok) => ok,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };

                            match waiters.remove(&frame.request_id) {
                                Some(Waiter::DeclareQueue(tx)) => {
                                    let _ = tx.send(Ok(()));
                                }
                                Some(_other) => {
                                    tracing::error!("Internal error: protocol violation: DeclareQueueOk for non-declare request_id")
                                }
                                None => {
                                    tracing::error!("Internal error: unexpected DeclareQueueOk")
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
                                    Waiter::DeclareQueue(tx) => {
                                        let res = tx.send(Err(FibrilError::Failure { code: err.code, msg: err.message }));

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
        Waiter::DeclareQueue(tx) => {
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
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
    ) -> FibrilResult<()> {
        let published = unix_millis();
        self.tx
            .send(Command::PublishUnconfirmed {
                topic,
                group,
                content_type,
                headers,
                payload,
                published,
            })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        Ok(())
    }

    async fn publish_with_confirmation(
        &self,
        topic: String,
        group: Option<String>,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
    ) -> FibrilResult<PublishConfirmation> {
        let (tx, rx) = oneshot::channel();
        let published = unix_millis();
        self.tx
            .send(Command::PublishConfirmed {
                topic,
                group,
                content_type,
                headers,
                payload,
                published,
                reply: tx,
            })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        Ok(PublishConfirmation { rx })
    }

    async fn publish_unconfirmed_delayed(
        &self,
        topic: String,
        group: Option<String>,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        not_before: u64,
    ) -> FibrilResult<()> {
        let published = unix_millis();
        self.tx
            .send(Command::PublishDelayedUnconfirmed {
                topic,
                group,
                content_type,
                headers,
                payload,
                published,
                not_before,
            })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        Ok(())
    }

    async fn publish_delayed_with_confirmation(
        &self,
        topic: String,
        group: Option<String>,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        not_before: u64,
    ) -> FibrilResult<PublishConfirmation> {
        let (tx, rx) = oneshot::channel();
        let published = unix_millis();
        self.tx
            .send(Command::PublishDelayedConfirmed {
                topic,
                group,
                content_type,
                headers,
                payload,
                published,
                not_before,
                reply: tx,
            })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        Ok(PublishConfirmation { rx })
    }

    async fn declare_queue(&self, req: DeclareQueue) -> FibrilResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::DeclareQueue { req, reply: tx })
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
/// Connection options for [`Client`].
///
/// Use [`ClientOptions::new`] for defaults, then chain authentication and
/// heartbeat settings before connecting.
///
/// ```no_run
/// use fibril_client::ClientOptions;
///
/// # async fn example() -> fibril_client::FibrilResult<()> {
/// let client = ClientOptions::new()
///     .auth("fibril", "fibril")
///     .heartbeat_interval(30)
///     .connect("127.0.0.1:9876")
///     .await?;
/// client.shutdown().await;
/// # Ok(())
/// # }
/// ```
pub struct ClientOptions {
    /// Name sent during the protocol handshake.
    pub client_name: String,
    /// Version sent during the protocol handshake.
    pub client_version: String,
    /// Optional username/password auth sent after handshake.
    pub auth: Option<Auth>,
    /// Optional heartbeat interval in seconds. Server timeout is 3x this value.
    pub heartbeat_interval: Option<u64>,
}

impl ClientOptions {
    /// Create default options for the Rust client.
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

    /// Return a copy with username/password authentication configured.
    pub fn auth(self, username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            auth: Some(Auth {
                username: username.into(),
                password: password.into(),
            }),
            ..self
        }
    }

    /// Return a copy with a heartbeat interval in seconds.
    pub fn heartbeat_interval(self, interval: u64) -> Self {
        Self {
            heartbeat_interval: Some(interval),
            ..self
        }
    }

    /// Connect using these options.
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
    fn topic_name_accepts_valid_names() {
        for name in [
            "orders",
            "orders.created",
            "orders-v2",
            "_dlq.orders",
            "a1_b-2.c",
        ] {
            assert_eq!(TopicName::parse(name).unwrap().as_str(), name);
        }
    }

    #[test]
    fn topic_name_rejects_invalid_names() {
        for name in [
            "",
            "Orders",
            ".orders",
            "orders.",
            "orders..created",
            "orders/created",
        ] {
            assert!(matches!(
                TopicName::parse(name),
                Err(FibrilError::InvalidName { kind: "topic", .. })
            ));
        }
    }

    #[test]
    fn group_name_uses_same_rules() {
        assert_eq!(GroupName::parse("workers-a").unwrap().as_str(), "workers-a");
        assert!(matches!(
            GroupName::parse("Workers"),
            Err(FibrilError::InvalidName { kind: "group", .. })
        ));
    }

    fn client_with_command_rx() -> (Client, mpsc::Receiver<Command>) {
        let (tx, rx) = mpsc::channel(8);
        (
            Client {
                address: "127.0.0.1:0".parse().unwrap(),
                opts: ClientOptions::new(),
                engine: Arc::new(EngineHandle {
                    tx,
                    shutdown: Arc::new(Notify::new()),
                }),
            },
            rx,
        )
    }

    #[tokio::test]
    async fn publish_uses_unconfirmed_command() {
        let (client, mut rx) = client_with_command_rx();
        let publisher = client.publisher("jobs").unwrap();

        publisher.publish("hello").await.unwrap();

        match rx.recv().await.unwrap() {
            Command::PublishUnconfirmed { topic, group, .. } => {
                assert_eq!(topic, "jobs");
                assert_eq!(group, None);
            }
            other => panic!("expected unconfirmed publish, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn publish_confirmed_waits_for_offset() {
        let (client, mut rx) = client_with_command_rx();
        let publisher = client.publisher("jobs").unwrap();
        let task = tokio::spawn(async move { publisher.publish_confirmed("hello").await });

        match rx.recv().await.unwrap() {
            Command::PublishConfirmed {
                topic,
                group,
                reply,
                ..
            } => {
                assert_eq!(topic, "jobs");
                assert_eq!(group, None);
                reply.send(Ok(42)).unwrap();
            }
            other => panic!("expected confirmed publish, got {other:?}"),
        }

        assert_eq!(task.await.unwrap().unwrap(), 42);
    }

    #[tokio::test]
    async fn publish_with_confirmation_returns_handle_before_offset() {
        let (client, mut rx) = client_with_command_rx();
        let publisher = client.publisher("jobs").unwrap();

        let confirmation = publisher.publish_with_confirmation("hello").await.unwrap();

        match rx.recv().await.unwrap() {
            Command::PublishConfirmed {
                topic,
                group,
                reply,
                ..
            } => {
                assert_eq!(topic, "jobs");
                assert_eq!(group, None);
                reply.send(Ok(43)).unwrap();
            }
            other => panic!("expected confirmed publish, got {other:?}"),
        }

        assert_eq!(confirmation.confirmed().await.unwrap(), 43);
    }

    #[tokio::test]
    async fn declare_queue_sends_retry_and_dlq_policy() {
        let (client, mut rx) = client_with_command_rx();
        let task = tokio::spawn(async move {
            client
                .declare_queue(
                    QueueConfig::new("jobs")
                        .unwrap()
                        .group("workers")
                        .unwrap()
                        .custom_dead_letter_queue("_dlq.jobs")
                        .unwrap()
                        .max_retries(3),
                )
                .await
        });

        match rx.recv().await.unwrap() {
            Command::DeclareQueue { req, reply } => {
                assert_eq!(req.topic, "jobs");
                assert_eq!(req.group.as_deref(), Some("workers"));
                assert_eq!(req.dlq_max_retries, Some(3));
                match req.dlq_policy {
                    Some(QueueDlqPolicy::Custom { topic, group }) => {
                        assert_eq!(topic, "_dlq.jobs");
                        assert_eq!(group, None);
                    }
                    other => panic!("expected custom dlq policy, got {other:?}"),
                }
                reply.send(Ok(())).unwrap();
            }
            other => panic!("expected declare queue, got {other:?}"),
        }

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn subscribe_flags_match_ack_mode() {
        let (client, mut rx) = client_with_command_rx();
        let manual_client = client.clone();
        let manual = tokio::spawn(async move {
            manual_client
                .subscribe("jobs")
                .unwrap()
                .sub_manual_ack()
                .await
        });

        match rx.recv().await.unwrap() {
            Command::Subscribe { req, reply } => {
                assert!(!req.auto_ack);
                let (_tx, manual_rx) = mpsc::channel(1);
                reply
                    .send(Ok(AckableSubChannel { manual: manual_rx }))
                    .unwrap();
            }
            other => panic!("expected manual subscribe, got {other:?}"),
        }
        manual.await.unwrap().unwrap();

        let auto_client = client.clone();
        let auto =
            tokio::spawn(
                async move { auto_client.subscribe("jobs").unwrap().sub_auto_ack().await },
            );

        match rx.recv().await.unwrap() {
            Command::SubscribeAutoAcked { req, reply } => {
                assert!(req.auto_ack);
                let (_tx, auto_rx) = mpsc::channel(1);
                reply
                    .send(Ok(AutoAckedSubChannel { auto: auto_rx }))
                    .unwrap();
            }
            other => panic!("expected auto subscribe, got {other:?}"),
        }
        auto.await.unwrap().unwrap();
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
        assert!(!message.headers().contains_key("content-type"));
        assert_eq!(message.content_type_value(), Some("text/plain"));
    }

    #[test]
    fn deserialize_uses_json_content_type() {
        let message = NewMessage::json(&TestPayload { value: 42 }).unwrap();
        let message = Message {
            delivery_tag: DeliveryTag { epoch: 1 },
            published: 0,
            publish_received: 0,
            content_type: message.content_type,
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
            content_type: message.content_type,
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
            content_type: message.content_type,
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
            content_type: message.content_type,
            headers: message.headers,
            payload: message.payload,
        };

        assert!(matches!(
            message.deserialize::<TestPayload>(),
            Err(FibrilError::DeserializationFailure { .. })
        ));
    }
}
