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

use arc_swap::ArcSwap;
use fibril_util::{UnixMillis, unix_millis};
use fibril_wire::{DeliveryTag, Partition};
use futures::{SinkExt, StreamExt};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    collections::HashMap,
    fmt::{self, Debug},
    mem::size_of,
    net::{SocketAddr, ToSocketAddrs},
    str::FromStr,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    sync::{Mutex, Notify, broadcast, mpsc, oneshot},
};
use tokio_util::codec::Framed;
use uuid::Uuid;

use fibril_wire::{
    AdvertisedAddress, DEFAULT_HEARTBEAT_INTERVAL,
    frame::{Frame, ProtoCodec},
    helper::*,
    wire, *,
};

// ===== Public API ============================================================

// Client-side clustering / failover behavior (retry classification, publish
// failover retry state, subscription failover supervisor). Re-exported so
// `fibril_client::RetryAdvice` etc. keep resolving.
mod failover;
pub use failover::*;

mod routing;
pub use routing::*;

mod tls;
pub use tls::TlsClientOptions;
use tls::establish_stream;

pub use fibril_wire::ReconcilePolicy;
// Shared header namespace constants (single source of truth in the protocol crate)
// so the client guard and broker rejection cannot drift.
pub use fibril_wire::{
    CLIENT_HEADER_PREFIX, HEADER_PRODUCER_ID, HEADER_PRODUCER_SEQ, RESERVED_HEADER_PREFIXES,
};

/// A push from the broker telling this client that its assignment within an
/// exclusive consumer group changed (see [`SubscriptionBuilder::consumer_group`]).
///
/// Purely informational — the broker enforces exclusivity server-side regardless
/// of whether the app reacts. Use it for partition-affinity processing, metrics,
/// or to drive an app-level drain on `revoked`. Subscribe via
/// [`Client::assignment_events`]. `generation` increases per cohort rebalance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignmentEvent {
    pub topic: String,
    pub group: Option<String>,
    pub consumer_group: String,
    pub generation: u64,
    /// The member's full current partition set.
    pub assigned: Vec<Partition>,
    /// Partitions newly assigned since the previous event.
    pub added: Vec<Partition>,
    /// Partitions taken away since the previous event (drain these).
    pub revoked: Vec<Partition>,
}

/// A push from the broker telling this client it is draining for a planned
/// shutdown or upgrade (see [`Client::going_away_events`]). Purely informational:
/// the app can stop producing, finish in-flight work, or alert. When the socket
/// then closes, the client reconnects (redirecting to the post-drain owner)
/// through its normal path regardless of whether the app reacts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoingAwayNotice {
    /// How long the broker will hold sessions before it stops serving, so the app
    /// knows its window to wind down.
    pub grace_ms: u64,
    /// Human-readable reason (shutdown, upgrade).
    pub message: String,
}

/// A declared queue as seen in the cluster [`Catalogue`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueInfo {
    pub topic: String,
    /// The queue's group namespace, or `None` for the ungrouped default.
    pub group: Option<String>,
    /// Authoritative partition count.
    pub partition_count: u32,
}

/// A declared Plexus stream as seen in the cluster [`Catalogue`]. Streams have no
/// group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamInfo {
    pub topic: String,
    /// Authoritative partition count.
    pub partition_count: u32,
}

/// A snapshot of the channels declared in the cluster: every queue and Plexus
/// stream the client currently knows about, with partition counts. Built from the
/// topology the broker hands out and kept live by topology pushes, so it needs no
/// extra round-trips. `queues` and `streams` are sorted (by topic, then group) for
/// a stable iteration order. Read the current snapshot with [`Client::catalogue`]
/// or subscribe to changes with [`Client::catalogue_events`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Catalogue {
    pub queues: Vec<QueueInfo>,
    pub streams: Vec<StreamInfo>,
    /// Coordination generation this snapshot reflects.
    pub generation: u64,
}

impl Catalogue {
    /// Derive the catalogue from a topology snapshot. The topology lists one entry
    /// per partition, so queues dedupe by `(topic, group)` and streams by topic. A
    /// `BTreeMap` gives a deterministic sorted order.
    fn from_topology(topology: &TopologyOk) -> Self {
        let mut queues = std::collections::BTreeMap::new();
        for entry in &topology.queues {
            queues.insert(
                (entry.topic.clone(), entry.group.clone()),
                entry.partition_count.max(1),
            );
        }
        let mut streams = std::collections::BTreeMap::new();
        for entry in &topology.streams {
            streams.insert(entry.topic.clone(), entry.partition_count.max(1));
        }
        Self {
            queues: queues
                .into_iter()
                .map(|((topic, group), partition_count)| QueueInfo {
                    topic,
                    group,
                    partition_count,
                })
                .collect(),
            streams: streams
                .into_iter()
                .map(|(topic, partition_count)| StreamInfo {
                    topic,
                    partition_count,
                })
                .collect(),
            generation: topology.generation,
        }
    }
}

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
    /// The broker redirected the operation to the current owner. The client
    /// auto-follows this for confirmed publishes and subscribes; if it surfaces
    /// to the caller, topology changed mid-operation and the op is retryable.
    #[error("redirected to owner {:?} for {}/{}", .0.owner_endpoints, .0.topic, .0.partition)]
    Redirect(Box<Redirect>),
    /// The connection ended before the expected protocol exchange completed.
    #[error("EOF")]
    Eof,
    /// The broker requires TLS but this client connected plaintext. Reported
    /// by the broker itself, so it is definitive rather than inferred.
    #[error(
        "the broker requires TLS. Enable client TLS: ClientOptions::new().tls() with \
         .tls_ca_path(...) or .tls_ca_fingerprint(...) to trust self-signed broker \
         material, or bare .tls() for a publicly issued certificate"
    )]
    TlsRequiredByBroker,
    /// TLS is enabled but the handshake ended before completing, which
    /// usually means the broker listener speaks plaintext.
    #[error(
        "TLS handshake with {address} ended early, the broker listener is probably \
         plaintext. Disable TLS in the client options, or set tls.enabled = true on \
         the broker"
    )]
    TlsNotSupportedByBroker { address: String },
    /// The broker certificate failed verification. A trust configuration
    /// problem, distinct from a transport mismatch.
    #[error(
        "broker certificate verification failed: {msg}. Trust the broker CA via \
         .tls_ca_path(...) (generated deployments write <data_dir>/tls/ca.pem) or pin \
         .tls_ca_fingerprint(...) from the broker startup log"
    )]
    TlsCertificateUntrusted { msg: String },
    /// Client-side TLS configuration problem: unreadable ca_path, malformed
    /// fingerprint, or an invalid server name.
    #[error("invalid TLS configuration: {msg}")]
    TlsConfig { msg: String },
    /// Any other TLS handshake failure.
    #[error("TLS handshake failed: {msg}")]
    TlsHandshake { msg: String },
    /// The broker requires a client certificate (`tls.client_auth =
    /// require`) and this client presented none.
    #[error(
        "the broker at {address} requires a client certificate: provide one with          .tls_client_cert(cert, key). Deployment-CA certificates are issued with          fibrilctl cert issue"
    )]
    TlsClientCertificateRequired { address: String },
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

    /// Parse an optional group name, treating `default` as the ungrouped queue.
    pub fn parse_optional(value: impl AsRef<str>) -> FibrilResult<Option<Self>> {
        let value = value.as_ref();
        if value == "default" {
            Ok(None)
        } else {
            Self::parse(value).map(Some)
        }
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
    shared: Arc<ClientShared>,
}

/// Result of an explicit reconnect attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectOutcome {
    /// Handshake outcome reported by the broker.
    pub resume_outcome: ResumeOutcome,
}

/// Topic-scoped handle for publishing messages.
///
/// Create with [`Client::publisher`] or [`Client::publisher_grouped`]. Plain
/// serializable values are encoded as msgpack by default; use [`NewMessage`]
/// for JSON, text, raw bytes, or custom headers.
#[derive(Debug, Clone)]
pub struct Publisher {
    shared: Arc<ClientShared>,
    topic: TopicName,
    group: Option<GroupName>,
    /// Default message TTL (ms) stamped on every immediate publish from this
    /// publisher. Set via [`Publisher::expiring`]. `None` = no default.
    default_message_ttl_ms: Option<u64>,
    /// Explicit partition count for spreading publishes, set via
    /// [`Publisher::partitions`]. When `None` the topology cache supplies the count
    /// (just partition 0 in standalone / cold-cache). Set this for a multi-partition
    /// Plexus stream, whose partitioning the topology cache does not carry yet -
    /// mirrors [`StreamSubscriptionBuilder::partitions`] on the read side.
    partition_count: Option<u32>,
    /// Round-robin cursor for keyless publishes when `partition_count` is set.
    /// `Arc` so a cloned publisher shares one cursor.
    round_robin: Arc<std::sync::atomic::AtomicUsize>,
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
    ///
    /// Requires the `msgpack` feature (enabled by default).
    #[cfg(feature = "msgpack")]
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
    pub fn text(&self) -> Result<&str, FibrilError> {
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
///         NewMessage::text("hello")
///             .header("x-trace-id", "abc123"),
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct NewMessage {
    /// Encoded payload bytes sent to the broker.
    pub payload: Vec<u8>,
    content_type: Option<ContentType>,
    headers: HashMap<String, String>,
    partition_key: Option<Vec<u8>>,
}

impl NewMessage {
    /// Encode a serializable value as msgpack and set `application/msgpack`.
    ///
    /// Requires the `msgpack` feature (enabled by default).
    #[cfg(feature = "msgpack")]
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
            partition_key: None,
        }
    }

    /// Publish UTF-8 text and set `text/plain; charset=utf-8`.
    pub fn text(payload: impl Into<Vec<u8>>) -> Self {
        NewMessage::with_content_type(payload.into(), "text/plain; charset=utf-8")
    }

    /// Add or replace a header.
    ///
    /// Fibril reserves `fibril.*` and `stroma.*` headers for system metadata;
    /// user code should avoid those prefixes.
    /// Set a user header. The `fibril.` and `stroma.` prefixes are reserved for
    /// system metadata and are silently ignored here (the broker rejects them, and
    /// the `fibril.client.*` carve-out is library-owned - see [`ReliablePublisher`]),
    /// so user code cannot set or spoof them.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let key = key.into();
        let value = value.into();
        if key.eq_ignore_ascii_case("content-type") {
            self.content_type = Some(ContentType::from_header(value));
        } else if is_reserved_header_key(&key) {
            // System namespace - not user-settable. Drop it.
        } else {
            self.headers.insert(key, value);
        }
        self
    }

    /// Set a library-owned system header (the `fibril.client.*` carve-out).
    /// Bypasses the user guard in [`header`](Self::header); for internal use only.
    pub(crate) fn system_header(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set content type metadata.
    pub fn content_type(self, content_type: impl Into<String>) -> Self {
        self.header("content-type", content_type)
    }

    /// Set the partition key: `hash(key) % partition_count` selects the
    /// partition, co-locating same-key messages for per-key ordering. Without a
    /// key, publishes round-robin across partitions (the default). Accepts a
    /// string or raw bytes.
    pub fn partition_key(mut self, key: impl Into<Vec<u8>>) -> Self {
        self.partition_key = Some(key.into());
        self
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
            partition_key: None,
        }
    }
}

/// Value that can be published by a [`Publisher`].
///
/// [`NewMessage`] preserves its explicit payload and headers. With the `msgpack`
/// feature (enabled by default) other serializable values are encoded with
/// [`NewMessage::msg_pack`]. Without it, encode explicitly via
/// [`NewMessage::json`], [`NewMessage::content`], or [`NewMessage::raw`].
pub trait Publishable {
    /// Convert into a publishable message.
    fn into_message(self) -> FibrilResult<NewMessage>;
}

impl Publishable for NewMessage {
    fn into_message(self) -> FibrilResult<NewMessage> {
        Ok(self)
    }
}

#[cfg(feature = "msgpack")]
impl<T: Serialize> Publishable for T {
    fn into_message(self) -> FibrilResult<NewMessage> {
        NewMessage::msg_pack(&self)
    }
}

/// A "never miss" publisher built on top of [`Publisher`].
///
/// `Publisher::publish_confirmed` already rides through a transient owner failover
/// (it refreshes topology and retries the transport up to `publish_timeout_ms`).
/// But once that budget is spent it returns a retryable error and leaves the
/// decision to you (see [`FibrilError::is_retryable`]). `ReliablePublisher`
/// automates the rest of the loop: it re-publishes until the message is durably
/// confirmed, a permanent ([`RetryAdvice::DoNotRetry`]) error occurs, or
/// `max_attempts` is reached.
///
/// So today it is **at-least-once-no-miss**: every message is eventually confirmed,
/// but a retry after a lost confirm can produce a DUPLICATE. It stamps every
/// message with a stable **producer id** ([`HEADER_PRODUCER_ID`]) and a monotonic
/// per-producer **sequence** ([`HEADER_PRODUCER_SEQ`]) under the broker's
/// library-owned `fibril.client.*` header carve-out (user code cannot set those).
/// The broker ignores them today; once it dedups on these keys the SAME helper
/// becomes **effectively-once** with no API change. (A later hot-header
/// optimization can promote them to typed fields, like `content_type` already is.)
///
/// Clone it freely: clones share the producer id and the sequence counter, so
/// multiple tasks publishing through clones still emit one coherent id/seq stream.
///
/// ```no_run
/// # async fn example(publisher: fibril_client::Publisher) -> fibril_client::FibrilResult<()> {
/// // Opt in: re-publishes through arbitrarily long failovers until confirmed.
/// let reliable = publisher.reliable();
/// let offset = reliable.publish("order-42").await?; // durably confirmed
/// # let _ = offset;
/// # Ok(())
/// # }
/// ```
///
/// If you would rather handle retries yourself, the classification is explicit:
///
/// ```no_run
/// use fibril_client::RetryAdvice;
/// # async fn example(publisher: fibril_client::Publisher) -> fibril_client::FibrilResult<()> {
/// match publisher.publish_confirmed("order-42").await {
///     Ok(_offset) => {}                       // durable + will be delivered
///     Err(e) if e.is_retryable() => {
///         // Unknown outcome - the write may have landed. Re-publishing is
///         // at-least-once (may duplicate until idempotent dedup ships).
///     }
///     Err(_e) => { /* DoNotRetry: fix the request (bad topic/arg/payload). */ }
/// }
/// # let _ = RetryAdvice::Retry;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ReliablePublisher {
    publisher: Publisher,
    producer_id: Uuid,
    seq: Arc<std::sync::atomic::AtomicU64>,
    /// 0 = retry until confirmed or a permanent error; otherwise cap the attempts.
    max_attempts: u32,
}

impl ReliablePublisher {
    /// Wrap a [`Publisher`]. Use [`Publisher::reliable`] for the idiomatic form.
    pub fn new(publisher: Publisher) -> Self {
        Self {
            publisher,
            producer_id: Uuid::new_v4(),
            seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            max_attempts: 0,
        }
    }

    /// Cap the number of publish attempts. `0` (default) retries until the message
    /// is durably confirmed or hits a permanent error.
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// The stable producer id for this publisher (forward-compat scaffolding for
    /// dedup; not yet transmitted - see the type docs).
    pub fn producer_id(&self) -> Uuid {
        self.producer_id
    }

    /// Publish and keep retrying until durably confirmed (or a permanent error /
    /// `max_attempts`). Returns the broker-assigned offset. A retry after a lost
    /// confirm may duplicate until owner-side dedup ships - see the type docs.
    pub async fn publish<T: Publishable>(&self, payload: T) -> FibrilResult<u64> {
        // Stamp the producer id + monotonic sequence as library-owned system
        // headers (the broker's `fibril.client.*` carve-out). The broker ignores
        // them today, so this stays at-least-once; once it dedups on these keys the
        // SAME helper is effectively-once. (A later hot-header optimization can
        // promote them to typed fields, like content_type/not_before already are.)
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let message = payload
            .into_message()?
            .system_header(HEADER_PRODUCER_ID, self.producer_id.to_string())
            .system_header(HEADER_PRODUCER_SEQ, seq.to_string());
        let mut attempts: u32 = 0;
        loop {
            match self.publisher.publish_confirmed(message.clone()).await {
                Ok(offset) => return Ok(offset),
                // Retryable = unknown outcome (the inner call already exhausted its
                // transport retry budget). Keep going unless the cap is reached.
                Err(err) if err.is_retryable() => {
                    attempts += 1;
                    if self.max_attempts != 0 && attempts >= self.max_attempts {
                        return Err(err);
                    }
                    tokio::time::sleep(publish_retry_nap(PUBLISH_RETRY_INITIAL_BACKOFF_MS)).await;
                }
                Err(err) => return Err(err), // permanent (DoNotRetry)
            }
        }
    }
}

impl Publisher {
    /// Wrap this publisher in a [`ReliablePublisher`] that retries until durably
    /// confirmed and stamps producer-id/sequence headers (opt-in "never miss").
    pub fn reliable(self) -> ReliablePublisher {
        ReliablePublisher::new(self)
    }

    /// Stamp a default message TTL on every immediate publish from this
    /// publisher: the broker drops the message if it is not consumed within the
    /// interval. The interval follows the [`Delayable`] convention - a bare
    /// number is seconds, or pass a [`std::time::Duration`]. Applies to the
    /// immediate publish paths (delayed publishes do not carry a TTL yet).
    pub fn expiring(mut self, ttl: impl Delayable) -> Self {
        self.default_message_ttl_ms = Some(ttl.with_delay().as_millis() as u64);
        self
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
        not_before: Option<UnixMillis>,
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
                not_before: None,
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
                not_before: None,
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
    pub async fn retry_after(self, delay: impl Delayable) -> FibrilResult<Message> {
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
                not_before: Some(delay.deadline()),
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
        #[cfg(feature = "msgpack")]
        Some("application/msgpack") | None | Some("") => rmp_serde::from_slice(payload)
            .map_err(|e| FibrilError::DeserializationFailure { msg: e.to_string() }),
        #[cfg(not(feature = "msgpack"))]
        Some("application/msgpack") | None | Some("") => Err(FibrilError::DeserializationFailure {
            msg: "msgpack payloads need the `msgpack` feature, which is not enabled".to_string(),
        }),
        Some(other) => Err(FibrilError::DeserializationFailure {
            msg: format!("unsupported content-type `{other}`"),
        }),
    }
}

fn decode_protocol<T: for<'de> serde::Deserialize<'de> + 'static>(
    frame: &Frame,
) -> FibrilResult<T> {
    try_decode(frame).map_err(|err| FibrilError::DeserializationFailure {
        msg: err.to_string(),
    })
}

fn client_subscription_registry_poisoned() -> FibrilError {
    FibrilError::Unexpected {
        msg: "client subscription registry poisoned".into(),
    }
}

fn reconcile_subscription_from_subscribe_ok(
    ok: &SubscribeOk,
    auto_ack: bool,
) -> ReconcileSubscription {
    ReconcileSubscription {
        sub_id: ok.sub_id,
        topic: ok.topic.clone(),
        group: ok.group.clone(),
        partition: ok.partition,
        auto_ack,
        prefetch: ok.prefetch,
        // Carry exclusive membership so a reconnect rejoins the cohort.
        consumer_group: ok.consumer_group.clone(),
        consumer_target: ok.consumer_target,
        member_id: ok.member_id,
    }
}

enum Waiter {
    Publish(oneshot::Sender<FibrilResult<u64>>),
    DeclareQueue(oneshot::Sender<FibrilResult<()>>),
    SubscribeManual(oneshot::Sender<FibrilResult<AckableSubChannel>>),
    SubscribeAuto(oneshot::Sender<FibrilResult<AutoAckedSubChannel>>),
    /// Stream (Plexus) subscribe. Same response as the queue variants, but the
    /// subscription is NOT recorded in the reconcile registry: streams manage their
    /// own fan-in / live-grow supervisor and resume via their durable cursor.
    StreamSubscribeManual(oneshot::Sender<FibrilResult<AckableSubChannel>>),
    StreamSubscribeAuto(oneshot::Sender<FibrilResult<AutoAckedSubChannel>>),
    Topology(oneshot::Sender<FibrilResult<TopologyOk>>),
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
    partition_count: Option<u32>,
    default_message_ttl_ms: Option<u64>,
}

impl QueueConfig {
    /// Start a queue declaration for a topic.
    pub fn new(topic: impl AsRef<str>) -> FibrilResult<Self> {
        Ok(Self {
            topic: TopicName::parse(topic)?,
            group: None,
            dlq_policy: None,
            dlq_max_retries: None,
            partition_count: None,
            default_message_ttl_ms: None,
        })
    }

    /// Declare behavior for a grouped queue namespace.
    pub fn group(mut self, group: impl AsRef<str>) -> FibrilResult<Self> {
        self.group = GroupName::parse_optional(group)?;
        Ok(self)
    }

    /// Request a specific partition count for this queue. When unset, the
    /// cluster default applies.
    pub fn partitions(mut self, partition_count: u32) -> Self {
        self.partition_count = Some(partition_count);
        self
    }

    /// Set the number of retries before the queue's dead-letter policy applies.
    pub fn max_retries(mut self, max_retries: u32) -> Self {
        self.dlq_max_retries = Some(max_retries);
        self
    }

    /// Set a default message TTL for this queue: messages published without their
    /// own TTL drop after this interval. Follows the [`Delayable`] convention
    /// (bare number = seconds, or a Duration). This is per-message expiry, not
    /// queue expiration (auto-deleting an idle queue).
    pub fn default_message_ttl(mut self, ttl: impl Delayable) -> Self {
        self.default_message_ttl_ms = Some(ttl.with_delay().as_millis() as u64);
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
            group: GroupName::parse_optional(group)?,
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
            partition_count: self.partition_count,
            default_message_ttl_ms: self.default_message_ttl_ms,
        }
    }
}

/// Declaration for a Plexus (fan-out stream) channel.
///
/// A stream delivers every record to every consumer (unlike a queue, where a
/// message is consumed once). Partitions buy write throughput and per-key
/// ordering — a stream subscription reads ALL partitions and fans them in, so
/// partitioning is not consumer work-sharing.
///
/// Durability tiers trade latency for durability: [`durable`](Self::durable)
/// fsyncs before delivering and confirming, [`speculative`](Self::speculative)
/// delivers early and defers the confirm until durable, and
/// [`ephemeral`](Self::ephemeral) delivers and confirms without an fsync.
///
/// ```no_run
/// # async fn example(client: fibril_client::Client) -> fibril_client::FibrilResult<()> {
/// use fibril_client::StreamConfig;
/// client
///     .declare_plexus(StreamConfig::new("events")?.partitions(4).retain_records(1_000_000))
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StreamConfig {
    topic: TopicName,
    partition_count: Option<u32>,
    durability: StreamDurability,
    retention: StreamRetention,
    replication_factor: Option<u32>,
}

impl StreamConfig {
    /// Start a stream declaration for a topic.
    pub fn new(topic: impl AsRef<str>) -> FibrilResult<Self> {
        Ok(Self {
            topic: TopicName::parse(topic)?,
            partition_count: None,
            durability: StreamDurability::default(),
            retention: StreamRetention::default(),
            replication_factor: None,
        })
    }

    /// Request a specific partition count. When unset, the cluster default
    /// applies.
    pub fn partitions(mut self, partition_count: u32) -> Self {
        self.partition_count = Some(partition_count);
        self
    }

    /// Persist asynchronously without gating delivery or the confirm (lowest
    /// latency, weakest guarantee).
    pub fn ephemeral(mut self) -> Self {
        self.durability = StreamDurability::Ephemeral;
        self
    }

    /// Deliver immediately with a speculative marker, deferring the producer
    /// confirm until the record is durable.
    pub fn speculative(mut self) -> Self {
        self.durability = StreamDurability::Speculative;
        self
    }

    /// Persist before confirming (the default).
    pub fn durable(mut self) -> Self {
        self.durability = StreamDurability::Durable;
        self
    }

    /// Drop records older than this age. Follows the [`Delayable`] convention
    /// (bare number = seconds, or a Duration).
    pub fn retain_for(mut self, age: impl Delayable) -> Self {
        self.retention.max_age_ms = Some(age.with_delay().as_millis() as u64);
        self
    }

    /// Keep at most this many bytes of retained records.
    pub fn retain_bytes(mut self, bytes: u64) -> Self {
        self.retention.max_bytes = Some(bytes);
        self
    }

    /// Keep at most this many records.
    pub fn retain_records(mut self, records: u64) -> Self {
        self.retention.max_records = Some(records);
        self
    }

    /// Per-stream durable-tier replication factor (follower count). When unset,
    /// the cluster default applies. Only the durable tier replicates; the express
    /// tiers stay owner-only regardless. A value of 0 makes a durable stream
    /// owner-only (durable on disk, not highly available).
    pub fn replication_factor(mut self, replicas: u32) -> Self {
        self.replication_factor = Some(replicas);
        self
    }

    fn into_wire(self) -> DeclarePlexus {
        DeclarePlexus {
            topic: self.topic.into_string(),
            partition_count: self.partition_count,
            durability: self.durability,
            retention: self.retention,
            replication_factor: self.replication_factor,
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
///     .sub()
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
    consumer_group: Option<String>,
    consumer_target: Option<u32>,
}

impl<'a> SubscriptionBuilder<'a> {
    /// Set the queue group.
    ///
    /// A group is an optional queue namespace under the topic.
    pub fn group(mut self, group: impl AsRef<str>) -> FibrilResult<Self> {
        self.group = GroupName::parse_optional(group)?;
        Ok(self)
    }

    /// Consume this queue as part of its **exclusive cohort**: each partition is
    /// delivered to exactly one cohort member at a time, preserving per-key
    /// ordering, with partitions balanced (and sticky) across members and
    /// automatic failover when a member disconnects.
    ///
    /// Just run several instances that all call `.exclusive()` on the same queue
    /// — they self-organize into the one cohort; there is nothing to name or
    /// coordinate (a queue has a single exclusive cohort). Without this,
    /// consumers compete for the queue (many per partition, unordered) — the
    /// default, and the right choice when you don't need ordering. Fan-in is
    /// transparent: the client subscribes to every partition and the broker gates
    /// delivery to the assigned member.
    pub fn exclusive(mut self) -> Self {
        self.consumer_group = Some(DEFAULT_COHORT_ID.to_string());
        self
    }

    /// Set this consumer's soft partition target within its exclusive cohort —
    /// the max partitions it would prefer to own. Only meaningful together with
    /// [`Self::exclusive`]; coverage always wins, so a member may still be
    /// assigned more than its target when the cohort is under-provisioned.
    pub fn consumer_target(mut self, max_partitions: u32) -> Self {
        self.consumer_target = Some(max_partitions);
        self
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
    pub async fn sub(self) -> FibrilResult<Subscription> {
        let topic = self.topic.into_string();
        let group = self.group.map(GroupName::into_string);
        let prefetch = self.prefetch;
        let consumer_group = self.consumer_group.clone();
        let consumer_target = self.consumer_target;
        let member_id = self
            .consumer_group
            .as_ref()
            .and_then(|_| self.client.shared.cohort_member_id.get().copied());
        let make_req = |partition| Subscribe {
            topic: topic.clone(),
            partition,
            group: group.clone(),
            prefetch,
            auto_ack: false,
            consumer_group: consumer_group.clone(),
            consumer_target,
            member_id,
        };
        // Fan in across every partition the topology cache knows about
        // (default: just partition 0 — see `partition_set`).
        let partitions = partition_set(self.client, &topic, group.as_deref());
        // Keep each partition's Subscribe request so the supervisor can re-subscribe
        // it to a new owner after a failover.
        let mut subs = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            let req = make_req(*partition);
            let part_rx = subscribe_partition_manual(self.client, req.clone()).await?;
            subs.push((req, part_rx));
        }

        // Static fan-in when auto-resubscribe is disabled.
        let Some(interval_ms) = self.client.shared.opts.partition_resubscribe_interval_ms else {
            return Ok(Subscription::fan_in(
                subs.into_iter().map(|(_, rx)| rx).collect(),
                prefetch,
            ));
        };

        // Dynamic fan-in: supervise each partition (re-subscribe on owner failover)
        // and a manager task picks up partitions added by a live grow.
        let cap = (prefetch as usize).max(1) * subs.len().max(1);
        let (tx, rx) = mpsc::channel(cap);
        let client = self.client.clone();
        for (req, part_rx) in subs {
            supervise_forward_manual(client.clone(), req, part_rx, tx.clone());
        }
        let known: std::collections::HashSet<Partition> = partitions.into_iter().collect();
        tokio::spawn(partition_resubscribe_loop_manual(
            client,
            topic,
            group,
            prefetch,
            consumer_group,
            consumer_target,
            member_id,
            known,
            tx,
            interval_ms,
        ));
        Ok(Subscription { rx })
    }

    /// Subscribe with client-side automatic acknowledgement.
    ///
    /// This yields [`Message`] directly. Prefer manual acknowledgement when
    /// processing correctness matters.
    #[tracing::instrument(fields(topic = %self.topic, group = ?self.group, prefetch = %self.prefetch))]
    pub async fn sub_auto_ack(self) -> FibrilResult<AutoAckedSubscription> {
        let topic = self.topic.into_string();
        let group = self.group.map(GroupName::into_string);
        let prefetch = self.prefetch;
        let consumer_group = self.consumer_group.clone();
        let consumer_target = self.consumer_target;
        let member_id = self
            .consumer_group
            .as_ref()
            .and_then(|_| self.client.shared.cohort_member_id.get().copied());
        let make_req = |partition| Subscribe {
            topic: topic.clone(),
            partition,
            group: group.clone(),
            prefetch,
            auto_ack: true,
            consumer_group: consumer_group.clone(),
            consumer_target,
            member_id,
        };
        let partitions = partition_set(self.client, &topic, group.as_deref());
        let mut subs = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            let req = make_req(*partition);
            let part_rx = subscribe_partition_auto(self.client, req.clone()).await?;
            subs.push((req, part_rx));
        }

        let Some(interval_ms) = self.client.shared.opts.partition_resubscribe_interval_ms else {
            return Ok(AutoAckedSubscription::fan_in(
                subs.into_iter().map(|(_, rx)| rx).collect(),
                prefetch,
            ));
        };

        let cap = (prefetch as usize).max(1) * subs.len().max(1);
        let (tx, rx) = mpsc::channel(cap);
        let client = self.client.clone();
        for (req, part_rx) in subs {
            supervise_forward_auto(client.clone(), req, part_rx, tx.clone());
        }
        let known: std::collections::HashSet<Partition> = partitions.into_iter().collect();
        tokio::spawn(partition_resubscribe_loop_auto(
            client,
            topic,
            group,
            prefetch,
            consumer_group,
            consumer_target,
            member_id,
            known,
            tx,
            interval_ms,
        ));
        Ok(AutoAckedSubscription { rx })
    }
}

/// Builder for a Plexus (fan-out stream) subscription.
///
/// Construct with [`Client::stream`], optionally set a durable name, start
/// position, header filter, and prefetch, then choose manual or auto ack. A
/// stream subscription reads every partition and fans them in; the SAME durable
/// name tracks an independent cursor per partition.
///
/// ```no_run
/// # async fn example(client: fibril_client::Client) -> fibril_client::FibrilResult<()> {
/// let mut sub = client
///     .stream("events")?
///     .durable("analytics")
///     .filter("region", "eu-*")
///     .sub()
///     .await?;
/// while let Some(msg) = sub.recv().await {
///     msg.complete().await?;
/// }
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct StreamSubscriptionBuilder<'a> {
    client: &'a Client,
    topic: TopicName,
    partition_count: Option<u32>,
    durable_name: Option<String>,
    start: StreamStart,
    filter: Vec<(String, String)>,
    prefetch: u32,
}

impl<'a> StreamSubscriptionBuilder<'a> {
    /// Fan in over exactly this many partitions (0..count). When unset, the
    /// topology cache supplies the count (just partition 0 in standalone /
    /// cold-cache). Set this for a multi-partition stream when the cache does not
    /// yet carry its partitioning.
    pub fn partitions(mut self, partition_count: u32) -> Self {
        self.partition_count = Some(partition_count.max(1));
        self
    }

    /// Use a durable broker-side cursor named `name`: the subscription resumes
    /// from the committed position (earliest on a fresh name) and acks advance it.
    /// Without a durable name the subscription is ephemeral and `start` governs the
    /// position.
    pub fn durable(mut self, name: impl Into<String>) -> Self {
        self.durable_name = Some(name.into());
        self
    }

    /// Begin at the live tail (only records published from now on). The default
    /// for an ephemeral subscription.
    pub fn from_latest(mut self) -> Self {
        self.start = StreamStart::Latest;
        self
    }

    /// Begin at the oldest retained record.
    pub fn from_earliest(mut self) -> Self {
        self.start = StreamStart::Earliest;
        self
    }

    /// Begin `count` records back from the tail.
    pub fn from_last(mut self, count: u64) -> Self {
        self.start = StreamStart::NBack { count };
        self
    }

    /// Begin at the first record at or after this wall-clock time (ms).
    pub fn from_time(mut self, time_ms: u64) -> Self {
        self.start = StreamStart::ByTime { time_ms };
        self
    }

    /// Add a header-match clause: deliver only records whose `header` value matches
    /// `pattern` (a literal that may contain `*` wildcards). Repeatable; clauses
    /// are AND-ed.
    pub fn filter(mut self, header: impl Into<String>, pattern: impl Into<String>) -> Self {
        self.filter.push((header.into(), pattern.into()));
        self
    }

    /// Set the maximum number of records the broker may push ahead per partition.
    pub fn prefetch(mut self, prefetch: u32) -> Self {
        self.prefetch = prefetch;
        self
    }

    fn partition_list(&self) -> Vec<Partition> {
        match self.partition_count {
            Some(count) => (0..count).map(Partition::new).collect(),
            None => partition_set(self.client, self.topic.as_str(), None),
        }
    }

    fn make_req(&self, partition: Partition, auto_ack: bool) -> SubscribeStream {
        SubscribeStream {
            topic: self.topic.as_str().to_string(),
            partition,
            durable_name: self.durable_name.clone(),
            start: self.start,
            filter: self.filter.clone(),
            prefetch: self.prefetch,
            auto_ack,
        }
    }

    /// Subscribe with manual acknowledgements. Each [`InflightMessage`] must be
    /// settled; completing one advances the durable cursor past its offset.
    #[tracing::instrument(fields(topic = %self.topic, prefetch = %self.prefetch))]
    pub async fn sub(self) -> FibrilResult<Subscription> {
        let partitions = self.partition_list();
        let mut subs = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            let req = self.make_req(*partition, false);
            let part_rx = subscribe_stream_partition_manual(self.client, req.clone()).await?;
            subs.push((req, part_rx));
        }

        // Static fan-in when partition resubscribe / live-grow tracking is off.
        let Some(interval_ms) = self.client.shared.opts.partition_resubscribe_interval_ms else {
            return Ok(Subscription::fan_in(
                subs.into_iter().map(|(_, rx)| rx).collect(),
                self.prefetch,
            ));
        };

        // Dynamic fan-in: supervise each partition (re-subscribe on owner failover)
        // and a loop picks up partitions added by a live grow. The same durable name
        // tracks an independent cursor per partition.
        let cap = (self.prefetch as usize).max(1) * subs.len().max(1);
        let (tx, rx) = mpsc::channel(cap);
        let client = self.client.clone();
        for (req, part_rx) in subs {
            supervise_forward_stream_manual(client.clone(), req, part_rx, tx.clone());
        }
        let known: std::collections::HashSet<Partition> = partitions.into_iter().collect();
        tokio::spawn(stream_partition_resubscribe_loop_manual(
            client,
            self.topic.as_str().to_string(),
            self.durable_name.clone(),
            self.start,
            self.filter.clone(),
            self.prefetch,
            known,
            tx,
            interval_ms,
        ));
        Ok(Subscription { rx })
    }

    /// Subscribe with client-side automatic acknowledgement, yielding [`Message`]
    /// directly. The broker advances the durable cursor as it delivers.
    #[tracing::instrument(fields(topic = %self.topic, prefetch = %self.prefetch))]
    pub async fn sub_auto_ack(self) -> FibrilResult<AutoAckedSubscription> {
        let partitions = self.partition_list();
        let mut subs = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            let req = self.make_req(*partition, true);
            let part_rx = subscribe_stream_partition_auto(self.client, req.clone()).await?;
            subs.push((req, part_rx));
        }

        let Some(interval_ms) = self.client.shared.opts.partition_resubscribe_interval_ms else {
            return Ok(AutoAckedSubscription::fan_in(
                subs.into_iter().map(|(_, rx)| rx).collect(),
                self.prefetch,
            ));
        };

        let cap = (self.prefetch as usize).max(1) * subs.len().max(1);
        let (tx, rx) = mpsc::channel(cap);
        let client = self.client.clone();
        for (req, part_rx) in subs {
            supervise_forward_stream_auto(client.clone(), req, part_rx, tx.clone());
        }
        let known: std::collections::HashSet<Partition> = partitions.into_iter().collect();
        tokio::spawn(stream_partition_resubscribe_loop_auto(
            client,
            self.topic.as_str().to_string(),
            self.durable_name.clone(),
            self.start,
            self.filter.clone(),
            self.prefetch,
            known,
            tx,
            interval_ms,
        ));
        Ok(AutoAckedSubscription { rx })
    }
}

/// The partitions a subscription should fan in over for `(topic, group)`.
///
/// Cache-only by design: the authoritative count comes from the topology cache
/// (warmed by [`Client::fetch_topology`] or by redirects). An unknown count
/// (cold cache, standalone, in-memory) yields just partition 0 — the
/// single-partition path, unchanged. Subscribe never fetches topology on its
/// own; that would add a round-trip (and could stall harnesses that don't
/// answer `Op::Topology`). A subset selector for consumer-group assignment will
/// narrow this set later.
fn partition_set(client: &Client, topic: &str, group: Option<&str>) -> Vec<Partition> {
    let count = client
        .shared
        .topology
        .load()
        .partitioning(topic, group)
        .map(|p| p.count)
        .unwrap_or(1)
        .max(1);
    (0..count).map(Partition::new).collect()
}

/// Background loop keeping a manual-ack subscription's fan-in current: on each
/// tick it refreshes topology and subscribes to any partition added by a live
/// grow, merging it into the same channel. Removed partitions (shrink) end
/// naturally when the broker retires their stream. Stops when the subscription is
/// dropped (the output sender closes).
#[allow(clippy::too_many_arguments)]
async fn partition_resubscribe_loop_manual(
    client: Client,
    topic: String,
    group: Option<String>,
    prefetch: u32,
    consumer_group: Option<String>,
    consumer_target: Option<u32>,
    member_id: Option<uuid::Uuid>,
    mut known: std::collections::HashSet<Partition>,
    tx: mpsc::Sender<InflightMessage>,
    interval_ms: u64,
) {
    let interval = std::time::Duration::from_millis(interval_ms.max(1));
    loop {
        tokio::select! {
            _ = tx.closed() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let _ = client.fetch_topology().await;
        for partition in partition_set(&client, &topic, group.as_deref()) {
            if known.contains(&partition) {
                continue;
            }
            let req = Subscribe {
                topic: topic.clone(),
                partition,
                group: group.clone(),
                prefetch,
                auto_ack: false,
                consumer_group: consumer_group.clone(),
                consumer_target,
                member_id,
            };
            if let Ok(part_rx) = subscribe_partition_manual(&client, req.clone()).await {
                known.insert(partition);
                supervise_forward_manual(client.clone(), req, part_rx, tx.clone());
            }
        }
    }
}

/// Auto-ack counterpart of [`partition_resubscribe_loop_manual`].
#[allow(clippy::too_many_arguments)]
async fn partition_resubscribe_loop_auto(
    client: Client,
    topic: String,
    group: Option<String>,
    prefetch: u32,
    consumer_group: Option<String>,
    consumer_target: Option<u32>,
    member_id: Option<uuid::Uuid>,
    mut known: std::collections::HashSet<Partition>,
    tx: mpsc::Sender<Message>,
    interval_ms: u64,
) {
    let interval = std::time::Duration::from_millis(interval_ms.max(1));
    loop {
        tokio::select! {
            _ = tx.closed() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let _ = client.fetch_topology().await;
        for partition in partition_set(&client, &topic, group.as_deref()) {
            if known.contains(&partition) {
                continue;
            }
            let req = Subscribe {
                topic: topic.clone(),
                partition,
                group: group.clone(),
                prefetch,
                auto_ack: true,
                consumer_group: consumer_group.clone(),
                consumer_target,
                member_id,
            };
            if let Ok(part_rx) = subscribe_partition_auto(&client, req.clone()).await {
                known.insert(partition);
                supervise_forward_auto(client.clone(), req, part_rx, tx.clone());
            }
        }
    }
}

/// Live-grow pickup for a manual-ack stream subscription: on each tick it
/// refreshes topology and subscribes to any partition added since (merging it into
/// the same fan-in channel under the same durable name, so the new partition gets
/// its own cursor). Stops when the subscription is dropped.
#[allow(clippy::too_many_arguments)]
async fn stream_partition_resubscribe_loop_manual(
    client: Client,
    topic: String,
    durable_name: Option<String>,
    start: StreamStart,
    filter: Vec<(String, String)>,
    prefetch: u32,
    mut known: std::collections::HashSet<Partition>,
    tx: mpsc::Sender<InflightMessage>,
    interval_ms: u64,
) {
    let interval = std::time::Duration::from_millis(interval_ms.max(1));
    loop {
        tokio::select! {
            _ = tx.closed() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let _ = client.fetch_topology().await;
        for partition in partition_set(&client, &topic, None) {
            if known.contains(&partition) {
                continue;
            }
            let req = SubscribeStream {
                topic: topic.clone(),
                partition,
                durable_name: durable_name.clone(),
                start,
                filter: filter.clone(),
                prefetch,
                auto_ack: false,
            };
            if let Ok(part_rx) = subscribe_stream_partition_manual(&client, req.clone()).await {
                known.insert(partition);
                supervise_forward_stream_manual(client.clone(), req, part_rx, tx.clone());
            }
        }
    }
}

/// Auto-ack counterpart of [`stream_partition_resubscribe_loop_manual`].
#[allow(clippy::too_many_arguments)]
async fn stream_partition_resubscribe_loop_auto(
    client: Client,
    topic: String,
    durable_name: Option<String>,
    start: StreamStart,
    filter: Vec<(String, String)>,
    prefetch: u32,
    mut known: std::collections::HashSet<Partition>,
    tx: mpsc::Sender<Message>,
    interval_ms: u64,
) {
    let interval = std::time::Duration::from_millis(interval_ms.max(1));
    loop {
        tokio::select! {
            _ = tx.closed() => return,
            _ = tokio::time::sleep(interval) => {}
        }
        let _ = client.fetch_topology().await;
        for partition in partition_set(&client, &topic, None) {
            if known.contains(&partition) {
                continue;
            }
            let req = SubscribeStream {
                topic: topic.clone(),
                partition,
                durable_name: durable_name.clone(),
                start,
                filter: filter.clone(),
                prefetch,
                auto_ack: true,
            };
            if let Ok(part_rx) = subscribe_stream_partition_auto(&client, req.clone()).await {
                known.insert(partition);
                supervise_forward_stream_auto(client.clone(), req, part_rx, tx.clone());
            }
        }
    }
}

/// Subscribe to a single partition (manual ack), following owner redirects.
async fn subscribe_partition_manual(
    client: &Client,
    req: Subscribe,
) -> FibrilResult<mpsc::Receiver<InflightMessage>> {
    let mut attempts = 0u32;
    loop {
        let engine = client
            .shared
            .engine_for(&req.topic, req.partition, req.group.as_deref())
            .await?;
        match engine.subscribe(req.clone()).await {
            Ok(rx) => return Ok(rx),
            Err(FibrilError::Redirect(redirect)) => {
                if attempts >= client.shared.opts.max_redirects {
                    return Err(FibrilError::Failure {
                        code: ERR_NOT_OWNER,
                        msg: format!(
                            "exceeded max redirects ({}) subscribing {}/{}",
                            client.shared.opts.max_redirects, req.topic, req.partition
                        ),
                    });
                }
                client.shared.apply_redirect(&redirect);
                attempts += 1;
            }
            Err(other) => return Err(other),
        }
    }
}

/// Subscribe to a single partition (auto ack), following owner redirects.
async fn subscribe_partition_auto(
    client: &Client,
    req: Subscribe,
) -> FibrilResult<mpsc::Receiver<Message>> {
    let mut attempts = 0u32;
    loop {
        let engine = client
            .shared
            .engine_for(&req.topic, req.partition, req.group.as_deref())
            .await?;
        match engine.subscribe_auto_ack(req.clone()).await {
            Ok(rx) => return Ok(rx),
            Err(FibrilError::Redirect(redirect)) => {
                if attempts >= client.shared.opts.max_redirects {
                    return Err(FibrilError::Failure {
                        code: ERR_NOT_OWNER,
                        msg: format!(
                            "exceeded max redirects ({}) subscribing {}/{}",
                            client.shared.opts.max_redirects, req.topic, req.partition
                        ),
                    });
                }
                client.shared.apply_redirect(&redirect);
                attempts += 1;
            }
            Err(other) => return Err(other),
        }
    }
}

/// Subscribe to a single stream partition (manual ack), following owner redirects.
async fn subscribe_stream_partition_manual(
    client: &Client,
    req: SubscribeStream,
) -> FibrilResult<mpsc::Receiver<InflightMessage>> {
    let mut attempts = 0u32;
    loop {
        let engine = client
            .shared
            .engine_for(&req.topic, req.partition, None)
            .await?;
        match engine.subscribe_stream(req.clone()).await {
            Ok(rx) => return Ok(rx),
            Err(FibrilError::Redirect(redirect)) => {
                if attempts >= client.shared.opts.max_redirects {
                    return Err(FibrilError::Failure {
                        code: ERR_NOT_OWNER,
                        msg: format!(
                            "exceeded max redirects ({}) subscribing stream {}/{}",
                            client.shared.opts.max_redirects, req.topic, req.partition
                        ),
                    });
                }
                client.shared.apply_redirect(&redirect);
                attempts += 1;
            }
            Err(other) => return Err(other),
        }
    }
}

/// Auto-ack counterpart of [`subscribe_stream_partition_manual`].
async fn subscribe_stream_partition_auto(
    client: &Client,
    req: SubscribeStream,
) -> FibrilResult<mpsc::Receiver<Message>> {
    let mut attempts = 0u32;
    loop {
        let engine = client
            .shared
            .engine_for(&req.topic, req.partition, None)
            .await?;
        match engine.subscribe_stream_auto_ack(req.clone()).await {
            Ok(rx) => return Ok(rx),
            Err(FibrilError::Redirect(redirect)) => {
                if attempts >= client.shared.opts.max_redirects {
                    return Err(FibrilError::Failure {
                        code: ERR_NOT_OWNER,
                        msg: format!(
                            "exceeded max redirects ({}) subscribing stream {}/{}",
                            client.shared.opts.max_redirects, req.topic, req.partition
                        ),
                    });
                }
                client.shared.apply_redirect(&redirect);
                attempts += 1;
            }
            Err(other) => return Err(other),
        }
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
        let user_shutdown = Arc::new(AtomicBool::new(false));
        let (assignment_tx, _) = broadcast::channel(ASSIGNMENT_EVENT_CAPACITY);
        let cohort_member_id = Arc::new(std::sync::OnceLock::new());
        // Build the shared state first (empty pool), then wire its weak
        // self-reference so the bootstrap connection's reader loop can apply
        // pushed topology back into this cache. The slot is connected via
        // engine_slot, which now hands the engine that weak handle.
        let shared = Arc::new(ClientShared {
            bootstrap: vec![address.to_string()],
            opts,
            user_shutdown,
            pool: parking_lot::RwLock::new(HashMap::new()),
            topology: ArcSwap::from_pointee(TopologyCache::default()),
            round_robin: std::sync::atomic::AtomicUsize::new(0),
            assignment_tx,
            cohort_member_id,
            catalogue: ArcSwap::from_pointee(Catalogue::default()),
            catalogue_tx: broadcast::channel(CATALOGUE_EVENT_CAPACITY).0,
            going_away_tx: broadcast::channel(GOING_AWAY_EVENT_CAPACITY).0,
            me: std::sync::OnceLock::new(),
        });
        let _ = shared.me.set(Arc::downgrade(&shared));
        shared.engine_slot(address.to_string()).await?;
        let warm_timeout_ms = shared.opts.topology_warm_timeout_ms;
        let client = Client { shared };
        // Warm the topology cache once so the first publish spreads across
        // partitions and the first subscription fans in over all of them.
        // Best-effort and bounded: a server that does not answer must not stall
        // connect, and a cold cache simply degrades to single-partition routing.
        if let Some(timeout_ms) = warm_timeout_ms {
            let _ = tokio::time::timeout(
                std::time::Duration::from_millis(timeout_ms),
                client.fetch_topology(),
            )
            .await;
        }
        Ok(client)
    }

    /// Replace the internal engine with a new connection.
    ///
    /// Existing [`Publisher`] handles created from this client use the new
    /// engine after this returns. Existing active [`Subscription`] streams
    /// remain attached to their original engine until subscription
    /// reconciliation is implemented.
    ///
    /// The returned outcome tells whether the broker accepted the previous
    /// resume identity. `ResumeOutcome::Resumed` means the server-side logical
    /// connection was reattached. Any other outcome means the broker treated the
    /// connection as fresh.
    #[tracing::instrument(fields(bootstrap = ?self.shared.bootstrap, opts = ?self.shared.opts))]
    pub async fn reconnect(&mut self) -> FibrilResult<ReconnectOutcome> {
        self.shared.bootstrap_slot().await?.reconnect().await
    }

    /// Reconnect and restore existing handles.
    ///
    /// This is not implemented yet. Use [`reconnect`](Self::reconnect);
    /// existing publishers can continue afterward, but active subscriptions
    /// should be recreated by the application.
    // TODO: try to handle inflight acks etc (resend?)
    #[tracing::instrument(fields(bootstrap = ?self.shared.bootstrap, opts = ?self.shared.opts))]
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
            shared: self.shared.clone(),
            topic: TopicName::parse(topic)?,
            group: None,
            default_message_ttl_ms: None,
            partition_count: None,
            round_robin: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
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
            shared: self.shared.clone(),
            topic: TopicName::parse(topic)?,
            group: GroupName::parse_optional(group)?,
            default_message_ttl_ms: None,
            partition_count: None,
            round_robin: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    /// Subscribe to exclusive consumer-group assignment changes for this client.
    ///
    /// Each [`AssignmentEvent`] reports a cohort the client belongs to (via
    /// [`SubscriptionBuilder::consumer_group`]) whose partition set changed.
    /// Purely informational — the broker enforces exclusivity regardless — but
    /// useful for partition-affinity work, metrics, or app-level drain on
    /// `revoked`. The stream survives reconnects; a slow consumer may miss events
    /// (broadcast lag). Returns a fresh receiver each call.
    pub fn assignment_events(&self) -> broadcast::Receiver<AssignmentEvent> {
        self.shared.assignment_tx.subscribe()
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
            consumer_group: None,
            consumer_target: None,
        })
    }

    /// Declare queue behavior such as retry and dead-letter policy.
    ///
    /// Queue declarations apply to the topic plus optional group. Partition
    /// selection is internal.
    #[tracing::instrument(skip(self), fields(topic = %config.topic, group = ?config.group))]
    pub async fn declare_queue(&self, config: QueueConfig) -> FibrilResult<()> {
        self.shared
            .engine_for_operation()
            .await?
            .declare_queue(config.into_wire())
            .await
    }

    /// Declare a Plexus (fan-out stream) channel.
    ///
    /// Every consumer of a stream sees every record (unlike a queue). See
    /// [`StreamConfig`] for partitioning, durability, and retention.
    #[tracing::instrument(skip(self), fields(topic = %config.topic))]
    pub async fn declare_plexus(&self, config: StreamConfig) -> FibrilResult<()> {
        self.shared
            .engine_for_operation()
            .await?
            .declare_plexus(config.into_wire())
            .await
    }

    /// Begin a stream (Plexus) subscription for a topic.
    ///
    /// A stream subscription reads ALL partitions and fans them in. Name the
    /// subscription with [`StreamSubscriptionBuilder::durable`] to get a
    /// broker-side cursor that resumes after a restart and advances on ack.
    pub fn stream(
        &'_ self,
        topic: impl AsRef<str> + fmt::Debug,
    ) -> FibrilResult<StreamSubscriptionBuilder<'_>> {
        Ok(StreamSubscriptionBuilder {
            client: self,
            topic: TopicName::parse(topic)?,
            partition_count: None,
            durable_name: None,
            start: StreamStart::Latest,
            filter: Vec::new(),
            prefetch: 16,
        })
    }

    /// Fetch the cluster topology from the broker and refresh the routing
    /// cache. Returns the topology snapshot. In standalone mode the broker
    /// returns an empty topology and the client keeps using its direct
    /// connection.
    pub async fn fetch_topology(&self) -> FibrilResult<TopologyOk> {
        let engine = self
            .shared
            .bootstrap_slot()
            .await?
            .engine_for_operation()
            .await?;
        let topology = engine.fetch_topology().await?;
        let now = unix_millis();
        let snapshot = topology.clone();
        self.shared.topology.rcu(|old| {
            let mut updated = (**old).clone();
            updated.replace(snapshot.clone());
            updated.last_refresh_ms = now;
            updated
        });
        // Full refresh -> prune pooled connections to endpoints that are no
        // longer owners (e.g. a demoted/dead owner after failover).
        self.shared.prune_pool_to_topology();
        self.shared.refresh_catalogue(&topology);
        Ok(topology)
    }

    /// The current cluster [`Catalogue`]: every queue and Plexus stream this
    /// client knows about, with partition counts. Read straight from the cached
    /// topology - no round-trip. Empty on a cold or standalone client; warmed by
    /// the connect-time topology fetch and kept live by broker topology pushes.
    pub fn catalogue(&self) -> Catalogue {
        self.shared.catalogue.load().as_ref().clone()
    }

    /// Subscribe to cluster catalogue changes. Each [`Catalogue`] delivered is a
    /// full snapshot, emitted when the set of declared queues or streams changes
    /// (a channel added or removed, or a partition count change). Owner-only
    /// failover churn does not emit. Lossy broadcast - a slow consumer may miss
    /// intermediate snapshots but a fresh receiver still sees the next change.
    /// Returns a fresh receiver each call.
    pub fn catalogue_events(&self) -> broadcast::Receiver<Catalogue> {
        self.shared.catalogue_tx.subscribe()
    }

    /// Subscribe to broker drain notices ([`GoingAwayNotice`]). The broker emits
    /// one when it is draining for a planned shutdown or upgrade, so the app can
    /// stop producing or finish in-flight work before the connection drops. Lossy
    /// broadcast - returns a fresh receiver each call.
    pub fn going_away_events(&self) -> broadcast::Receiver<GoingAwayNotice> {
        self.shared.going_away_tx.subscribe()
    }

    /// Gracefully shut down the client.
    ///
    /// This closes the connection engine and wakes subscription receivers.
    pub async fn shutdown(&self) {
        self.shared.user_shutdown.store(true, Ordering::Release);
        for slot in self.shared.pool.read().values() {
            slot.current().shutdown.notify_waiters();
        }
    }
}

// TODO: Replace serializeable with NewMessage struct, so the user can easily choose form of serialization
// TODO: perhaps use generics so that it defaults to message pack and can be used transparently
impl Publisher {
    /// Spread this publisher's writes across `count` partitions (`0..count`):
    /// round-robin for keyless messages, `hash(key) % count` when a partition key
    /// is set. Mirrors [`StreamSubscriptionBuilder::partitions`] on the read side.
    /// Use it for a multi-partition Plexus stream, whose partitioning the topology
    /// cache does not carry; without it the publisher routes via the topology cache,
    /// which is just partition 0 in standalone or a cold cache.
    pub fn partitions(mut self, count: u32) -> Self {
        self.partition_count = Some(count.max(1));
        self
    }

    /// Resolve the partition for one publish: an explicit local count (set via
    /// [`partitions`](Self::partitions)) wins, otherwise the shared topology cache
    /// routes. The local path stamps partitioning version 0, which the broker does
    /// not fence in standalone (no topology source).
    fn route(&self, key: Option<&[u8]>) -> Route {
        let Some(count) = self.partition_count else {
            let group = self.group.as_ref().map(|group| group.as_str());
            return self.shared.route_partition(self.topic.as_str(), group, key);
        };
        let count = count.max(1);
        let index = match key {
            Some(key) => (fnv1a(key) % count as u64) as u32,
            None => {
                let next = self
                    .round_robin
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (next % count as usize) as u32
            }
        };
        Route {
            partition: Partition::new(index),
            partitioning_version: 0,
        }
    }

    /// Publish without waiting for broker confirmation.
    ///
    /// This is the common path. It only waits for the command to be accepted by
    /// the local engine. Use [`publish_confirmed`](Self::publish_confirmed)
    /// when you need the broker-assigned offset.
    #[tracing::instrument(level = "trace", skip(payload), fields(topic = %self.topic))]
    pub async fn publish<T: Publishable>(&self, payload: T) -> FibrilResult<()> {
        let message = payload.into_message()?;
        let topic = self.topic.as_str();
        let group = self.group.as_ref().map(|group| group.as_str());
        let Route {
            partition,
            partitioning_version,
        } = self.route(message.partition_key.as_deref());
        self.shared
            .engine_for(topic, partition, group)
            .await?
            .publish_unconfirmed(
                topic.to_string(),
                group.map(str::to_string),
                partition,
                partitioning_version,
                message.content_type,
                message.headers,
                message.payload,
                self.default_message_ttl_ms,
            )
            .await
        // TODO: use oneshot channel to wait for when the packet has left(better errors timing)?
    }

    /// Shared retry decision for confirmed-publish paths after one attempt fails.
    /// Returns `Ok(())` to retry the loop, or `Err(e)` to give up with `e`.
    ///
    /// - Redirect: point-update routing to the named owner and retry (bounded by
    ///   `max_redirects`).
    /// - Transient transport failure (owner unreachable mid-failover): refresh
    ///   topology from a reachable node so the next attempt re-resolves the new
    ///   owner, then back off until the deadline. If a refresh lands against a
    ///   populated cluster view and the topic is not declared, fail fast
    ///   (`ERR_NOT_FOUND`) rather than burning the whole budget.
    /// - Anything else: give up immediately.
    async fn after_publish_failure(
        &self,
        topic: &str,
        group: Option<&str>,
        partition: Partition,
        err: FibrilError,
        state: &mut PublishRetryState,
    ) -> FibrilResult<()> {
        match err {
            FibrilError::Redirect(redirect) => {
                if state.redirects >= self.shared.opts.max_redirects {
                    return Err(FibrilError::Failure {
                        code: ERR_NOT_OWNER,
                        msg: format!(
                            "exceeded max redirects ({}) routing {topic}/{partition}",
                            self.shared.opts.max_redirects
                        ),
                    });
                }
                self.shared.apply_redirect(&redirect);
                state.redirects += 1;
                Ok(())
            }
            err if err.is_transient() => {
                let Some(deadline) = state.deadline else {
                    return Err(err);
                };
                if Instant::now() >= deadline {
                    return Err(err);
                }
                if self.shared.refresh_topology_throttled().await {
                    let topo = self.shared.topology.load();
                    if topo.is_populated() && !topo.knows_topic(topic, group) {
                        return Err(FibrilError::Failure {
                            code: ERR_NOT_FOUND,
                            msg: format!("{topic}/{partition} is not declared in the cluster"),
                        });
                    }
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                tokio::time::sleep(publish_retry_nap(state.backoff_ms).min(remaining)).await;
                state.backoff_ms = (state.backoff_ms * 2).min(PUBLISH_RETRY_MAX_BACKOFF_MS);
                Ok(())
            }
            other => Err(other),
        }
    }

    /// Publish and wait for broker confirmation.
    ///
    /// Resolves with the broker-assigned topic offset.
    #[tracing::instrument(level = "trace", skip(payload), fields(topic = %self.topic))]
    pub async fn publish_confirmed<T: Publishable>(&self, payload: T) -> FibrilResult<u64> {
        let message = payload.into_message()?;
        let topic = self.topic.as_str();
        let group = self.group.as_ref().map(|group| group.as_str());
        let Route {
            partition,
            partitioning_version,
        } = self
            .shared
            .route_partition(topic, group, message.partition_key.as_deref());
        // Bound transient (owner-failover) retries by the configured budget. 0
        // disables retry: a transport failure fails fast, the pre-existing behavior.
        let mut state = PublishRetryState::new(self.shared.opts.publish_timeout_ms);
        loop {
            // Resolve the owner, send, and await the confirm as one attempt so a
            // transport failure at any step (dead owner during failover) is caught
            // and retried together. Clone per attempt so a retry re-sends cleanly.
            let attempt = async {
                let engine = self.shared.engine_for(topic, partition, group).await?;
                let confirmation = engine
                    .publish_with_confirmation(
                        topic.to_string(),
                        group.map(str::to_string),
                        partition,
                        partitioning_version,
                        message.content_type.clone(),
                        message.headers.clone(),
                        message.payload.clone(),
                        self.default_message_ttl_ms,
                    )
                    .await?;
                confirmation.confirmed().await
            }
            .await;

            match attempt {
                Ok(offset) => return Ok(offset),
                Err(err) => {
                    self.after_publish_failure(topic, group, partition, err, &mut state)
                        .await?
                }
            }
        }
    }

    /// Publish and return a handle that can be awaited for broker confirmation.
    ///
    /// This sends a confirmed publish request but does not wait for the
    /// confirmation before returning. Keep the returned handle and await
    /// [`PublishConfirmation::confirmed`] later to pipeline multiple publishes.
    ///
    /// The SEND step retries across a transient owner failover (same policy as
    /// [`publish_confirmed`](Self::publish_confirmation), bounded by
    /// `publish_timeout_ms`): because callers await each send before issuing the
    /// next, retrying the send before returning preserves per-partition send order.
    /// A failure of the later confirm step (owner died after accepting the send) is
    /// NOT retried here - re-sending an already-accepted message risks a duplicate
    /// and a reorder under the confirm window, so that is left to the caller (a
    /// future idempotent-producer feature).
    #[tracing::instrument(level = "trace", skip(payload), fields(topic = %self.topic))]
    pub async fn publish_with_confirmation<T: Publishable>(
        &self,
        payload: T,
    ) -> FibrilResult<PublishConfirmation> {
        let message = payload.into_message()?;
        let topic = self.topic.as_str();
        let group = self.group.as_ref().map(|group| group.as_str());
        let Route {
            partition,
            partitioning_version,
        } = self.route(message.partition_key.as_deref());
        let mut state = PublishRetryState::new(self.shared.opts.publish_timeout_ms);
        loop {
            // Send-only attempt: resolve the owner and hand off the publish. Clone
            // per attempt so a retry re-sends cleanly. The returned handle's confirm
            // is awaited by the caller, not here.
            let attempt = async {
                let engine = self.shared.engine_for(topic, partition, group).await?;
                engine
                    .publish_with_confirmation(
                        topic.to_string(),
                        group.map(str::to_string),
                        partition,
                        partitioning_version,
                        message.content_type.clone(),
                        message.headers.clone(),
                        message.payload.clone(),
                        self.default_message_ttl_ms,
                    )
                    .await
            }
            .await;

            match attempt {
                Ok(confirmation) => return Ok(confirmation),
                Err(err) => {
                    self.after_publish_failure(topic, group, partition, err, &mut state)
                        .await?
                }
            }
        }
    }

    /// Publish after a relative delay without waiting for broker confirmation.
    ///
    /// Numeric Rust delays are seconds; use [`std::time::Duration`] for
    /// explicit units.
    #[tracing::instrument(level = "trace", skip(payload), fields(topic = %self.topic))]
    pub async fn publish_delayed<T: Publishable, D: Delayable + Debug>(
        &self,
        payload: T,
        delay: D,
    ) -> FibrilResult<()> {
        let deadline = delay.deadline();
        let message = payload.into_message()?;
        let topic = self.topic.as_str();
        let group = self.group.as_ref().map(|group| group.as_str());
        let Route {
            partition,
            partitioning_version,
        } = self
            .shared
            .route_partition(topic, group, message.partition_key.as_deref());
        self.shared
            .engine_for(topic, partition, group)
            .await?
            .publish_unconfirmed_delayed(
                topic.to_string(),
                group.map(str::to_string),
                partition,
                partitioning_version,
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
    #[tracing::instrument(level = "trace", skip(payload), fields(topic = %self.topic))]
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
    #[tracing::instrument(level = "trace", skip(payload), fields(topic = %self.topic))]
    pub async fn publish_delayed_with_confirmation<T: Publishable, D: Delayable + Debug>(
        &self,
        payload: T,
        delay: D,
    ) -> FibrilResult<PublishConfirmation> {
        let deadline = delay.deadline();
        let message = payload.into_message()?;
        let topic = self.topic.as_str();
        let group = self.group.as_ref().map(|group| group.as_str());
        let Route {
            partition,
            partitioning_version,
        } = self
            .shared
            .route_partition(topic, group, message.partition_key.as_deref());
        self.shared
            .engine_for(topic, partition, group)
            .await?
            .publish_delayed_with_confirmation(
                topic.to_string(),
                group.map(str::to_string),
                partition,
                partitioning_version,
                message.content_type,
                message.headers,
                message.payload,
                deadline,
            )
            .await
    }
}

impl Subscription {
    /// Merge per-partition delivery streams into one subscription. A single
    /// partition is returned as-is (no extra hop); multiple partitions are
    /// forwarded into one merged channel by a task per partition. Each
    /// `InflightMessage` carries its own settle channel, so acks route back to
    /// the delivering partition's owner regardless of the merge. Ordering is
    /// preserved within a partition, interleaved across partitions.
    fn fan_in(mut receivers: Vec<mpsc::Receiver<InflightMessage>>, prefetch: u32) -> Self {
        if receivers.len() == 1 {
            if let Some(rx) = receivers.pop() {
                return Subscription { rx };
            }
        }
        let cap = (prefetch as usize).max(1) * receivers.len().max(1);
        let (tx, rx) = mpsc::channel(cap);
        for mut part_rx in receivers {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(msg) = part_rx.recv().await {
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
            });
        }
        Subscription { rx }
    }

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
    /// Merge per-partition auto-ack streams into one subscription. See
    /// [`Subscription::fan_in`]; auto-ack settles server-side, so there is no
    /// ack to route — this is a plain stream merge.
    fn fan_in(mut receivers: Vec<mpsc::Receiver<Message>>, prefetch: u32) -> Self {
        if receivers.len() == 1 {
            if let Some(rx) = receivers.pop() {
                return AutoAckedSubscription { rx };
            }
        }
        let cap = (prefetch as usize).max(1) * receivers.len().max(1);
        let (tx, rx) = mpsc::channel(cap);
        for mut part_rx in receivers {
            let tx = tx.clone();
            tokio::spawn(async move {
                while let Some(msg) = part_rx.recv().await {
                    if tx.send(msg).await.is_err() {
                        break;
                    }
                }
            });
        }
        AutoAckedSubscription { rx }
    }

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

/// The first owner endpoint to dial, as a connectable `host:port` string. The
/// connection pool resolves it at connect time, so a service name works directly.
/// Returns `None` when the owner advertises no endpoints. Trying each address in
/// the list in order (a true connect-probe) is a later refinement.
fn first_owner_target(addrs: &[AdvertisedAddress]) -> Option<String> {
    addrs.first().map(AdvertisedAddress::target)
}

/// Owner of one queue partition for client routing.
#[derive(Debug, Clone)]
#[allow(dead_code)] // fields consumed once routing is wired
struct OwnerEntry {
    endpoint: String,
    partitioning_version: u64,
}

/// Authoritative partitioning of one logical queue `(topic, group)`: how many
/// partitions it has and the version that count was published under. The client
/// stamps `version` on each publish so the owner can fence stale routing.
#[derive(Debug, Clone, Copy)]
struct PartitioningEntry {
    count: u32,
    version: u64,
}

/// The outcome of routing one publish: which partition to send to, and the
/// partitioning version that choice was made under (stamped on the wire so the
/// owner can fence a stale view).
#[derive(Debug, Clone, Copy)]
struct Route {
    partition: Partition,
    partitioning_version: u64,
}

/// Client-side cache of queue ownership learned from `Op::Topology` /
/// `Op::Redirect`. Empty (e.g. standalone brokers) means "no routing info";
/// callers fall back to the bootstrap connection.
#[derive(Debug, Default, Clone)]
#[allow(dead_code)] // some methods used as routing is wired
struct TopologyCache {
    generation: u64,
    by_queue: HashMap<(String, Partition, Option<String>), OwnerEntry>,
    /// Authoritative partitioning per logical queue `(topic, group)`.
    counts: HashMap<(String, Option<String>), PartitioningEntry>,
    last_refresh_ms: u64,
}

#[allow(dead_code)] // some methods used as routing is wired
impl TopologyCache {
    fn lookup(&self, topic: &str, partition: Partition, group: Option<&str>) -> Option<OwnerEntry> {
        self.by_queue
            .get(&(topic.to_string(), partition, group.map(str::to_string)))
            .cloned()
    }

    /// Authoritative partitioning (count + version) for `(topic, group)`, if known.
    fn partitioning(&self, topic: &str, group: Option<&str>) -> Option<PartitioningEntry> {
        self.counts
            .get(&(topic.to_string(), group.map(str::to_string)))
            .copied()
    }

    /// Whether this `(topic, group)` is declared in the cluster. `counts` is
    /// populated for every declared queue even while its owner is mid-failover
    /// (unresolved), so this stays true through a transition and only goes false
    /// when the topic is genuinely absent (never declared or deleted).
    fn knows_topic(&self, topic: &str, group: Option<&str>) -> bool {
        self.partitioning(topic, group).is_some()
    }

    /// Whether the cache holds a real cluster view (at least one declared queue).
    /// Used to avoid reading an empty standalone/cold cache as "topic not found".
    fn is_populated(&self) -> bool {
        !self.counts.is_empty()
    }

    /// The set of endpoints that currently own at least one partition. Used to
    /// prune pooled connections to endpoints that are no longer owners.
    fn endpoints(&self) -> std::collections::HashSet<String> {
        self.by_queue
            .values()
            .map(|entry| entry.endpoint.clone())
            .collect()
    }

    fn replace(&mut self, topology: TopologyOk) {
        self.generation = topology.generation;
        self.by_queue.clear();
        self.counts.clear();
        for queue in topology.queues {
            self.counts.insert(
                (queue.topic.clone(), queue.group.clone()),
                PartitioningEntry {
                    count: queue.partition_count.max(1),
                    version: queue.partitioning_version,
                },
            );
            let Some(endpoint) = first_owner_target(&queue.owner_endpoints) else {
                continue;
            };
            self.by_queue.insert(
                (queue.topic, queue.partition, queue.group),
                OwnerEntry {
                    endpoint,
                    partitioning_version: queue.partitioning_version,
                },
            );
        }
        // Streams have no group (keyed under group None). They carry per-partition
        // owners just like queues, so a publisher both spreads across partitions
        // and routes each to its owner. A topic is either a stream or a queue, so
        // the group-None cache entries never collide.
        for stream in topology.streams {
            self.counts.insert(
                (stream.topic.clone(), None),
                PartitioningEntry {
                    count: stream.partition_count.max(1),
                    version: stream.partitioning_version,
                },
            );
            let Some(endpoint) = first_owner_target(&stream.owner_endpoints) else {
                continue;
            };
            self.by_queue.insert(
                (stream.topic, stream.partition, None),
                OwnerEntry {
                    endpoint,
                    partitioning_version: stream.partitioning_version,
                },
            );
        }
    }

    fn apply_redirect(&mut self, redirect: &Redirect) {
        if let Some(endpoint) = first_owner_target(&redirect.owner_endpoints) {
            self.by_queue.insert(
                (
                    redirect.topic.clone(),
                    redirect.partition,
                    redirect.group.clone(),
                ),
                OwnerEntry {
                    endpoint,
                    partitioning_version: redirect.partitioning_version,
                },
            );
        }
    }

    fn invalidate(&mut self, topic: &str, partition: Partition, group: Option<&str>) {
        self.by_queue
            .remove(&(topic.to_string(), partition, group.map(str::to_string)));
    }
}

#[derive(Debug)]
struct ClientShared {
    bootstrap: Vec<String>,
    opts: ClientOptions,
    user_shutdown: Arc<AtomicBool>,
    // parking_lot: no poison, faster, and only ever held briefly (get + clone),
    // never across an await — the connect happens outside the lock.
    pool: parking_lot::RwLock<HashMap<String, Arc<EngineSlot>>>,
    // ArcSwap: lock-free read-mostly routing snapshot; readers can hold the
    // snapshot across awaits, writers update via rcu. last_refresh_ms lives
    // inside so it stays consistent with the swapped map.
    topology: ArcSwap<TopologyCache>,
    /// Round-robin cursor for keyless publishes (spread across partitions).
    round_robin: std::sync::atomic::AtomicUsize,
    /// Fan-out of exclusive consumer-group assignment changes to app subscribers.
    /// Shared across reconnects so the stream survives a dropped connection.
    assignment_tx: broadcast::Sender<AssignmentEvent>,
    /// This consumer's cluster cohort member id, minted by the server on the first
    /// exclusive subscribe and then carried on every other exclusive subscribe
    /// (across brokers) and across reconnects, so the cohort sees one member.
    cohort_member_id: Arc<std::sync::OnceLock<Uuid>>,
    /// Latest catalogue snapshot (declared queues + streams), derived from the
    /// topology and refreshed on every full topology replace (push or fetch).
    catalogue: ArcSwap<Catalogue>,
    /// Fan-out of catalogue changes to app subscribers. Lossy broadcast (a slow
    /// consumer may miss intermediate snapshots, never the latest on resubscribe).
    catalogue_tx: broadcast::Sender<Catalogue>,
    /// Fan-out of broker drain notices to app subscribers. Lossy broadcast,
    /// shared across reconnects like the other event streams.
    going_away_tx: broadcast::Sender<GoingAwayNotice>,
    /// Weak self-reference, set once right after construction. Connection reader
    /// loops hold a clone so a broker-pushed `TopologyUpdate` can be applied back
    /// into this routing cache (and the pool pruned) without a strong cycle.
    me: std::sync::OnceLock<std::sync::Weak<ClientShared>>,
}

/// Buffer of assignment events retained per [`Client::assignment_events`]
/// receiver before a slow consumer starts lagging (older events dropped).
const ASSIGNMENT_EVENT_CAPACITY: usize = 256;

/// Buffer of catalogue snapshots retained per [`Client::catalogue_events`]
/// receiver. Catalogue changes are infrequent, and only the latest matters, so a
/// small buffer is plenty.
const CATALOGUE_EVENT_CAPACITY: usize = 16;

/// Buffer of drain notices retained per [`Client::going_away_events`] receiver.
/// Drain is rare and only the latest matters, so a small buffer is plenty.
const GOING_AWAY_EVENT_CAPACITY: usize = 16;

/// Wire cohort id sent for [`SubscriptionBuilder::exclusive`]. A queue has a
/// single exclusive cohort, so the id is a fixed constant — membership is keyed
/// by `(topic, group)` server-side; the string only needs to be stable and
/// shared, never user-chosen.
const DEFAULT_COHORT_ID: &str = "default";

/// Stable FNV-1a hash for partition selection. Must be deterministic across all
/// clients so a given key always maps to the same partition (per-key ordering).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

impl ClientShared {
    /// Select the partition for a publish to `(topic, group)`:
    /// explicit override (none yet) -> `hash(key) % N` if a key is present ->
    /// round-robin over N. N comes from the authoritative topology cache;
    /// unknown N (cache cold / standalone) routes to partition 0. Returns the
    /// chosen partition together with the partitioning version it was computed
    /// under, so the publish can carry that version for the owner-side fence
    /// (unknown => version 0, which matches a single-partition v0 queue).
    fn route_partition(&self, topic: &str, group: Option<&str>, key: Option<&[u8]>) -> Route {
        let partitioning = self.topology.load().partitioning(topic, group);
        let version = partitioning.map(|p| p.version).unwrap_or(0);
        let count = partitioning.map(|p| p.count).unwrap_or(1).max(1);
        if count == 1 {
            return Route {
                partition: Partition::ZERO,
                partitioning_version: version,
            };
        }
        let index = match key {
            Some(key) => (fnv1a(key) % count as u64) as u32,
            None => {
                let next = self
                    .round_robin
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                (next % count as usize) as u32
            }
        };
        Route {
            partition: Partition::new(index),
            partitioning_version: version,
        }
    }

    /// Get-or-create the connection slot for an endpoint, connecting on miss.
    async fn engine_slot(&self, addr: String) -> FibrilResult<Arc<EngineSlot>> {
        if let Some(slot) = self.pool.read().get(&addr).cloned() {
            return Ok(slot);
        }
        let slot = Arc::new(
            EngineSlot::connect(
                addr.clone(),
                self.opts.clone(),
                self.user_shutdown.clone(),
                self.assignment_tx.clone(),
                self.cohort_member_id.clone(),
                self.me.get().cloned().unwrap_or_default(),
            )
            .await?,
        );
        let mut pool = self.pool.write();
        // Another task may have connected the same endpoint concurrently.
        if let Some(existing) = pool.get(&addr).cloned() {
            return Ok(existing);
        }
        pool.insert(addr, slot.clone());
        Ok(slot)
    }

    /// The bootstrap connection slot — the default route until queue-based
    /// routing is enabled (A5.5).
    async fn bootstrap_slot(&self) -> FibrilResult<Arc<EngineSlot>> {
        self.engine_slot(self.bootstrap[0].clone()).await
    }

    async fn engine_for_operation(&self) -> FibrilResult<Arc<EngineHandle>> {
        self.bootstrap_slot().await?.engine_for_operation().await
    }

    /// Resolve the engine that should serve a queue partition. Routing is
    /// REACTIVE: a cache hit routes to the owner's pooled connection; a miss
    /// falls back to the bootstrap connection. The cache is populated by
    /// redirects (the broker tells us the owner on a misroute) and by explicit
    /// `Client::fetch_topology`. We deliberately do NOT fetch topology on the
    /// hot path — that would add a round-trip to every first op (pointless in
    /// standalone, where topology is empty) and the redirect path corrects a
    /// misroute precisely.
    async fn engine_for(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> FibrilResult<Arc<EngineHandle>> {
        if let Some(owner) = self.topology.load().lookup(topic, partition, group) {
            return self
                .engine_slot(owner.endpoint.clone())
                .await?
                .engine_for_operation()
                .await;
        }
        self.engine_for_operation().await
    }

    /// Refresh the whole topology cache from any reachable node, throttled by
    /// `topology_refresh_cooldown_ms` so concurrent retriers do not storm the
    /// cluster. The assignment table is raft-replicated, so any one live node
    /// returns the authoritative current owners - we just need to reach one. Tries
    /// the configured seeds first, then any already-pooled endpoint (a likely
    /// survivor), and stops at the first that answers. Best-effort: on total
    /// failure the cache is left as-is and the caller's retry will try again.
    ///
    /// Returns `true` if a live node answered and the cache was updated (so the
    /// caller may trust the refreshed view, e.g. to fail fast on an unknown
    /// topic), `false` if nothing answered or the refresh was throttled.
    async fn refresh_topology_throttled(&self) -> bool {
        let now = unix_millis();
        let last = self.topology.load().last_refresh_ms;
        if now.saturating_sub(last) < self.opts.topology_refresh_cooldown_ms {
            return false;
        }

        let mut candidates: Vec<String> = self.bootstrap.clone();
        candidates.extend(self.pool.read().keys().cloned());
        let mut seen = std::collections::HashSet::new();
        for addr in candidates {
            if !seen.insert(addr.clone()) {
                continue;
            }
            let Ok(slot) = self.engine_slot(addr.clone()).await else {
                continue;
            };
            let Ok(engine) = slot.engine_for_operation().await else {
                continue;
            };
            let Ok(topology) = engine.fetch_topology().await else {
                continue;
            };
            let refreshed_at = unix_millis();
            self.topology.rcu(|old| {
                let mut updated = (**old).clone();
                updated.replace(topology.clone());
                updated.last_refresh_ms = refreshed_at;
                updated
            });
            // A full refresh gives the complete current owner set, so prune
            // pooled connections to endpoints that are no longer owners (e.g. a
            // demoted/dead owner after failover).
            self.prune_pool_to_topology();
            return true;
        }
        false
    }

    /// Drop pooled connections to endpoints that no longer own any partition
    /// (e.g. a demoted or dead owner after failover), so the pool does not
    /// accumulate stale slots over a long-lived client's lifetime. Kept:
    /// - bootstrap endpoints (the always-available fallback route), and
    /// - any slot that still has active subscriptions (it may be mid-migration;
    ///   it gets pruned on a later refresh once its subscriptions move).
    /// A pruned endpoint that is needed again is simply reconnected on demand.
    fn prune_pool_to_topology(&self) {
        let live = self.topology.load().endpoints();
        let bootstrap: std::collections::HashSet<String> = self.bootstrap.iter().cloned().collect();
        let mut pool = self.pool.write();
        pool.retain(|addr, slot| {
            let has_subscriptions = slot
                .subscriptions
                .read()
                .map(|subs| !subs.is_empty())
                .unwrap_or(true); // keep on a poisoned lock (conservative)
            live.contains(addr) || bootstrap.contains(addr) || has_subscriptions
        });
    }

    /// Apply a broker-pushed topology snapshot to the routing cache and prune the
    /// pool, mirroring the [`refresh_topology_throttled`](Self::refresh_topology_throttled)
    /// apply path. The push carries the full current topology (not a delta), so a
    /// plain replace is correct. A push only moves the cache forward: a stale push
    /// (older generation than the cache already reflects) is ignored so an
    /// out-of-order delivery cannot regress routing. Returns the generation the
    /// cache reflects after the call, which the caller acks back to the broker.
    fn apply_pushed_topology(&self, topology: TopologyOk) -> u64 {
        let applied_at = unix_millis();
        self.topology.rcu(|old| {
            let mut updated = (**old).clone();
            if topology.generation > updated.generation {
                updated.replace(topology.clone());
                updated.last_refresh_ms = applied_at;
            }
            updated
        });
        // A push gives the complete current owner set, so prune pooled
        // connections to endpoints that are no longer owners (same reasoning as a
        // full refresh).
        self.prune_pool_to_topology();
        self.refresh_catalogue(&topology);
        self.topology.load().generation
    }

    /// Refresh the catalogue snapshot from a full topology and broadcast a change
    /// event if the set of declared queues or streams actually changed. Monotonic
    /// and self-guarding: a stale topology (generation not newer than the snapshot
    /// already held) is ignored, so it is safe to call unconditionally. Owner-only
    /// churn (same channels, new owners, bumped generation) updates the stored
    /// generation but emits no event - the catalogue is unchanged.
    fn refresh_catalogue(&self, topology: &TopologyOk) {
        let next = Catalogue::from_topology(topology);
        let prev = self.catalogue.load();
        if next.generation <= prev.generation && prev.generation != 0 {
            return;
        }
        let changed = prev.queues != next.queues || prev.streams != next.streams;
        self.catalogue.store(Arc::new(next.clone()));
        if changed {
            let _ = self.catalogue_tx.send(next);
        }
    }

    /// Update the routing cache from a broker redirect (point-update to the new
    /// owner), so the retry — and subsequent ops — route correctly.
    fn apply_redirect(&self, redirect: &Redirect) {
        self.topology.rcu(|old| {
            let mut updated = (**old).clone();
            updated.apply_redirect(redirect);
            updated
        });
    }
}

/// A single reconnectable connection to one broker endpoint. The connection
/// pool holds one of these per endpoint.
#[derive(Debug)]
struct EngineSlot {
    /// Connect target as a `host:port` string, resolved at connect time so it can
    /// be a service name. Also the connection pool key.
    address: String,
    opts: ClientOptions,
    user_shutdown: Arc<AtomicBool>,
    subscriptions: Arc<RwLock<HashMap<u64, RegisteredSubscription>>>,
    engine: RwLock<Arc<EngineHandle>>,
    reconnect_lock: Mutex<()>,
    /// Shared assignment-event fan-out (survives reconnects).
    assignment_tx: broadcast::Sender<AssignmentEvent>,
    /// Shared cohort member id cache (survives reconnects); the read loop fills it
    /// from the first exclusive SubscribeOk.
    cohort_member_id: Arc<std::sync::OnceLock<Uuid>>,
    /// Weak handle back to the owning shared client, handed to each engine the
    /// slot starts so a pushed `TopologyUpdate` lands in the routing cache.
    /// Defaulted to an empty weak for slots built around a pre-made engine (tests
    /// and the in-memory path), which never run a network reader loop.
    topology_sink: std::sync::Weak<ClientShared>,
}

impl EngineSlot {
    /// Build a slot around an already-connected engine (after the initial
    /// handshake, and for tests using an in-memory engine).
    fn from_engine(
        address: String,
        opts: ClientOptions,
        user_shutdown: Arc<AtomicBool>,
        subscriptions: Arc<RwLock<HashMap<u64, RegisteredSubscription>>>,
        engine: Arc<EngineHandle>,
        assignment_tx: broadcast::Sender<AssignmentEvent>,
        cohort_member_id: Arc<std::sync::OnceLock<Uuid>>,
    ) -> Self {
        Self {
            address,
            opts,
            user_shutdown,
            subscriptions,
            engine: RwLock::new(engine),
            reconnect_lock: Mutex::new(()),
            assignment_tx,
            cohort_member_id,
            topology_sink: std::sync::Weak::new(),
        }
    }

    /// Connect a fresh slot to `address` (handshake included).
    async fn connect(
        address: String,
        opts: ClientOptions,
        user_shutdown: Arc<AtomicBool>,
        assignment_tx: broadcast::Sender<AssignmentEvent>,
        cohort_member_id: Arc<std::sync::OnceLock<Uuid>>,
        topology_sink: std::sync::Weak<ClientShared>,
    ) -> FibrilResult<Self> {
        let subscriptions = Arc::new(RwLock::new(HashMap::new()));
        let stream = establish_stream(address.as_str(), opts.tls.as_ref()).await?;
        let mut framed = Framed::new(stream, ProtoCodec);
        // Raise tokio-util's default 8KB write-buffer flush boundary so it does
        // not force a socket write every few 1KB frames, which would defeat the
        // engine's own publish-flush batching (PUBLISH_FLUSH_MESSAGES / _BYTES).
        framed.set_backpressure_boundary(PUBLISH_FLUSH_BYTES);
        let engine = start_engine(
            framed,
            opts.clone(),
            subscriptions.clone(),
            assignment_tx.clone(),
            cohort_member_id.clone(),
            topology_sink.clone(),
        )
        .await
        .map_err(|err| tls::refine_post_connect_error(err, opts.tls.as_ref(), &address))?;
        let mut slot = Self::from_engine(
            address,
            opts,
            user_shutdown,
            subscriptions,
            engine,
            assignment_tx,
            cohort_member_id,
        );
        slot.topology_sink = topology_sink;
        Ok(slot)
    }

    fn current(&self) -> Arc<EngineHandle> {
        self.engine
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn replace(&self, engine: Arc<EngineHandle>) {
        *self.engine.write().unwrap_or_else(|e| e.into_inner()) = engine;
    }

    async fn reconnect_once(&self) -> FibrilResult<ReconnectOutcome> {
        let old_engine = self.current();
        let mut opts = self.opts.clone();
        opts.resume_identity = Some(old_engine.resume_identity.clone());
        let stream = establish_stream(self.address.as_str(), opts.tls.as_ref()).await?;
        let mut framed = Framed::new(stream, ProtoCodec);
        // Raise tokio-util's default 8KB write-buffer flush boundary so it does
        // not force a socket write every few 1KB frames, which would defeat the
        // engine's own publish-flush batching (PUBLISH_FLUSH_MESSAGES / _BYTES).
        framed.set_backpressure_boundary(PUBLISH_FLUSH_BYTES);
        let tls_opts = opts.tls.clone();
        let new_engine = start_engine(
            framed,
            opts,
            self.subscriptions.clone(),
            self.assignment_tx.clone(),
            self.cohort_member_id.clone(),
            self.topology_sink.clone(),
        )
        .await
        .map_err(|err| tls::refine_post_connect_error(err, tls_opts.as_ref(), &self.address))?;
        let outcome = ReconnectOutcome {
            resume_outcome: new_engine.resume_outcome,
        };

        self.replace(new_engine);
        self.user_shutdown.store(false, Ordering::Release);
        old_engine.shutdown.notify_waiters();

        Ok(outcome)
    }

    /// Explicit reconnect (lock-guarded) for `Client::reconnect`.
    async fn reconnect(&self) -> FibrilResult<ReconnectOutcome> {
        let _guard = self.reconnect_lock.lock().await;
        self.reconnect_once().await
    }

    async fn reconnect_if_closed(&self) -> FibrilResult<()> {
        let current = self.current();
        if !current.is_closed() {
            return Ok(());
        }
        if self.user_shutdown.load(Ordering::Acquire) {
            return Err(FibrilError::BrokenPipe);
        }
        // A non-retryable close (bad credentials, forbidden, malformed request)
        // will fail again on reconnect, so surface it instead of storming the
        // broker with doomed handshakes while the real error never reaches the
        // caller.
        if let Some(reason) = current.close_reason()
            && reason.retry_advice() == RetryAdvice::DoNotRetry
        {
            return Err(reason);
        }

        let attempts = self.opts.auto_reconnect.max_attempts;
        if attempts == 0 {
            return Err(FibrilError::BrokenPipe);
        }

        let _guard = self.reconnect_lock.lock().await;
        if !self.current().is_closed() {
            return Ok(());
        }

        let mut last_err = None;
        for _ in 0..attempts {
            match self.reconnect_once().await {
                Ok(_) => return Ok(()),
                Err(err) => last_err = Some(err),
            }
        }

        Err(last_err.unwrap_or(FibrilError::BrokenPipe))
    }

    async fn engine_for_operation(&self) -> FibrilResult<Arc<EngineHandle>> {
        self.reconnect_if_closed().await?;
        Ok(self.current())
    }
}

#[derive(Debug, Clone)]
struct EngineHandle {
    tx: mpsc::Sender<Command>,
    shutdown: Arc<Notify>,
    // Why this engine's connection ended, set once as it tears down. Read by the
    // reconnect path: a non-retryable close (bad credentials, forbidden) must not
    // be retried, or the client storms the broker reconnecting into the same
    // rejection while the real error never surfaces.
    close_reason: Arc<std::sync::Mutex<Option<FibrilError>>>,
    resume_identity: ResumeIdentity,
    resume_outcome: ResumeOutcome,
}

impl EngineHandle {
    fn close_reason(&self) -> Option<FibrilError> {
        self.close_reason
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[derive(Debug)]
enum Command {
    PublishUnconfirmed {
        topic: String,
        group: Option<String>,
        partition: Partition,
        partitioning_version: u64,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
        ttl_ms: Option<u64>,
    },
    PublishConfirmed {
        topic: String,
        group: Option<String>,
        partition: Partition,
        partitioning_version: u64,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
        ttl_ms: Option<u64>,
        reply: oneshot::Sender<FibrilResult<u64>>,
    },
    PublishDelayedUnconfirmed {
        topic: String,
        group: Option<String>,
        partition: Partition,
        partitioning_version: u64,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        published: u64,
        not_before: u64,
    },
    PublishDelayedConfirmed {
        topic: String,
        group: Option<String>,
        partition: Partition,
        partitioning_version: u64,
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
    SubscribeStream {
        req: SubscribeStream,
        reply: oneshot::Sender<FibrilResult<AckableSubChannel>>,
    },
    SubscribeStreamAutoAcked {
        req: SubscribeStream,
        reply: oneshot::Sender<FibrilResult<AutoAckedSubChannel>>,
    },
    DeclareQueue {
        req: DeclareQueue,
        reply: oneshot::Sender<FibrilResult<()>>,
    },
    DeclarePlexus {
        req: DeclarePlexus,
        reply: oneshot::Sender<FibrilResult<()>>,
    },
    Topology {
        reply: oneshot::Sender<FibrilResult<TopologyOk>>,
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
        not_before: Option<UnixMillis>,
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
    partition: Partition,
    delivery: SubDelivery,
}

#[derive(Debug, Clone)]
struct RegisteredSubscription {
    reconcile: ReconcileSubscription,
    delivery: SubDelivery,
}

fn apply_reconcile_result(
    subscriptions: &Arc<RwLock<HashMap<u64, RegisteredSubscription>>>,
    result: ReconcileResult,
) -> FibrilResult<HashMap<u64, SubState>> {
    let mut registry = subscriptions
        .write()
        .map_err(|_| client_subscription_registry_poisoned())?;
    let mut restored = HashMap::new();

    for item in result.subscriptions {
        let Some(client) = item.client else {
            continue;
        };

        if item.action != ReconcileAction::Keep {
            registry.remove(&client.sub_id);
            continue;
        }

        let Some(mut registered) = registry.remove(&client.sub_id) else {
            continue;
        };
        let server = item.server.unwrap_or(client);
        registered.reconcile = server.clone();
        let delivery = registered.delivery.clone();
        registry.insert(server.sub_id, registered);
        restored.insert(
            server.sub_id,
            SubState {
                topic: server.topic,
                group: server.group,
                partition: server.partition,
                delivery,
            },
        );
    }

    Ok(restored)
}

// DEFAULT_HEARTBEAT_INTERVAL is shared from the protocol crate (single source of
// truth) so the client fallback and the server default cannot drift apart.
const PUBLISH_FLUSH_MESSAGES: usize = 128;
const PUBLISH_FLUSH_BYTES: usize = 1024 * 1024;
const PUBLISH_FLUSH_INTERVAL: std::time::Duration = std::time::Duration::from_micros(250);
// Max commands drained from the outbound channel per event-loop wakeup. Under a
// saturating producer the channel backs up and the writer blocks on send; draining
// a burst per wakeup relieves that backpressure faster than one-command-per-select.
// The bound keeps a fast producer from starving the heartbeat, flush, and shutdown arms.
const ENGINE_CMD_DRAIN_MAX: usize = 256;

// Reserved request ids for the fixed handshake exchanges, one per op. The
// handshake replies are matched by opcode (the request-response task is not
// running yet), and live ops number after the last id used here, so these never
// collide with a later request. A distinct id space from the opcodes, and the
// values match the other clients.
const HELLO_REQUEST_ID: u64 = 1;
const AUTH_REQUEST_ID: u64 = 2;
const RECONCILE_REQUEST_ID: u64 = 3;

async fn feed_protocol_frame<S, T>(
    framed: &mut Framed<S, ProtoCodec>,
    op: Op,
    request_id: u64,
    msg: &T,
) -> FibrilResult<usize>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: Serialize + 'static,
{
    let frame = try_encode(op, request_id, msg).map_err(|err| FibrilError::Unexpected {
        msg: err.to_string(),
    })?;
    let size = size_of::<Frame>() + frame.payload.len();

    framed
        .feed(frame)
        .await
        .map_err(|err| FibrilError::Disconnection {
            msg: err.to_string(),
        })?;

    Ok(size)
}

async fn feed_encoded_frame<S>(
    framed: &mut Framed<S, ProtoCodec>,
    frame: Frame,
) -> FibrilResult<usize>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let size = size_of::<Frame>() + frame.payload.len();

    framed
        .feed(frame)
        .await
        .map_err(|err| FibrilError::Disconnection {
            msg: err.to_string(),
        })?;

    Ok(size)
}

async fn flush_protocol_frames<S>(framed: &mut Framed<S, ProtoCodec>) -> FibrilResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    framed
        .flush()
        .await
        .map_err(|err| FibrilError::Disconnection {
            msg: err.to_string(),
        })
}

async fn send_protocol_frame<S, T>(
    framed: &mut Framed<S, ProtoCodec>,
    op: Op,
    request_id: u64,
    msg: &T,
) -> FibrilResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: Serialize + 'static,
{
    feed_protocol_frame(framed, op, request_id, msg).await?;
    flush_protocol_frames(framed).await
}

// TODO: Further reconnection attempts logic
// TODO: Better handle frame send errors, which currently just get swallowed. These errors indicate a broken connection and should trigger cleanup and reconnection logic.
async fn start_engine<S>(
    mut framed: Framed<S, ProtoCodec>,
    opts: ClientOptions,
    subscriptions: Arc<RwLock<HashMap<u64, RegisteredSubscription>>>,
    assignment_tx: broadcast::Sender<AssignmentEvent>,
    cohort_member_id: Arc<std::sync::OnceLock<Uuid>>,
    topology_sink: std::sync::Weak<ClientShared>,
) -> FibrilResult<Arc<EngineHandle>>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let shutdown = Arc::new(Notify::new());
    // handshake
    send_protocol_frame(
        &mut framed,
        Op::Hello,
        HELLO_REQUEST_ID,
        &Hello {
            client_name: opts.client_name.clone(),
            client_version: opts.client_version.clone(),
            protocol_version: PROTOCOL_V1,
            resume: opts.resume_identity.clone(),
        },
    )
    .await?;

    let frame = framed
        .next()
        .await
        .ok_or(FibrilError::Eof)?
        .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
    let (resume_identity, resume_outcome) = match frame.opcode {
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
            (
                ResumeIdentity {
                    owner_id: ho.owner_id,
                    client_id: ho.client_id,
                    resume_token: ho.resume_token,
                },
                ho.resume_outcome,
            )
        }
        x if x == Op::HelloErr as u16 => {
            let e: ErrorMsg = decode_protocol(&frame)?;
            return Err(FibrilError::Failure {
                code: e.code,
                msg: e.message,
            });
        }
        // A TLS listener answers a plaintext HELLO with a plaintext error
        // frame carrying ERR_TLS_REQUIRED, so the mismatch surfaces as its
        // own typed error rather than a generic failure.
        x if x == Op::Error as u16 => {
            let e: ErrorMsg = decode_protocol(&frame)?;
            if e.code == ERR_TLS_REQUIRED {
                return Err(FibrilError::TlsRequiredByBroker);
            }
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
    };

    if let Some(auth) = opts.auth {
        send_protocol_frame(&mut framed, Op::Auth, AUTH_REQUEST_ID, &auth).await?;
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

    let mut restored_subs = HashMap::<u64, SubState>::new();
    // Re-declare active subscriptions on ANY reconnect that has them, not only a
    // Resumed session. When the owner broker restarts in place (a bounce faster
    // than failure detection, so ownership stays put and there is no failover)
    // the client reconnects to a FRESH session (ResumeRejected/New) that has no
    // memory of the subscriptions. Reconciling lets the broker either restore
    // them (RestoreClientSubscriptions policy) or report them missing so the
    // client drops the now-dead streams and the subscription supervisor
    // re-subscribes. Without this, a bounced owner leaves the consumer's streams
    // open but unfed and it silently stops receiving.
    let reconcile_subs: Vec<_> = subscriptions
        .read()
        .map_err(|_| client_subscription_registry_poisoned())?
        .values()
        .map(|sub| sub.reconcile.clone())
        .collect();
    if !reconcile_subs.is_empty() {
        let reconcile = ReconcileClient {
            policy: opts.reconcile_policy,
            subscriptions: reconcile_subs,
        };
        send_protocol_frame(
            &mut framed,
            Op::ReconcileClient,
            RECONCILE_REQUEST_ID,
            &reconcile,
        )
        .await?;
        let frame = framed
            .next()
            .await
            .ok_or(FibrilError::Eof)?
            .map_err(|e| FibrilError::Disconnection { msg: e.to_string() })?;
        match frame.opcode {
            x if x == Op::ReconcileResult as u16 => {
                let result: ReconcileResult = decode_protocol(&frame)?;
                restored_subs = apply_reconcile_result(&subscriptions, result)?;
            }
            x if x == Op::Error as u16 => {
                let err: ErrorMsg = decode_protocol(&frame)?;
                return Err(FibrilError::Failure {
                    code: err.code,
                    msg: err.message,
                });
            }
            _ => {
                return Err(FibrilError::Unexpected {
                    msg: format!("Unexpected reconcile frame: opcode {}", frame.opcode),
                });
            }
        }
    }

    let (cmd_tx, mut cmd_rx) = mpsc::channel::<Command>(8192);
    let close_reason = Arc::new(std::sync::Mutex::new(None::<FibrilError>));
    let handle = Arc::new(EngineHandle {
        tx: cmd_tx.clone(),
        shutdown: shutdown.clone(),
        close_reason: close_reason.clone(),
        resume_identity,
        resume_outcome,
    });

    let mut subs = restored_subs;
    let subscription_registry = subscriptions.clone();

    let shutdown_engine = shutdown.clone();
    let shutdown_acks = shutdown.clone();

    // heartbeat task
    let heartbeat_secs = opts
        .heartbeat_interval
        .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);

    // writer + reader loop
    tokio::spawn(async move {
        let mut next_req = 4u64;
        let mut waiters: HashMap<u64, Waiter> = HashMap::new();
        let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(heartbeat_secs));
        heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        heartbeat.tick().await; // consume immediate tick

        let timeout = std::time::Duration::from_secs(heartbeat_secs * 3);
        let mut last_seen = tokio::time::Instant::now();
        let mut flush_ticker = tokio::time::interval(PUBLISH_FLUSH_INTERVAL);
        flush_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        flush_ticker.tick().await;
        let mut queued_publish_frames = 0usize;
        let mut queued_publish_bytes = 0usize;

        // In the engine task, before the select! loop:
        let mut fatal_error: Option<FibrilError> = None;

        // Helper closure (or just an inline pattern):
        macro_rules! send_or_die {
            ($framed:expr, $op:expr, $request_id:expr, $msg:expr, $err_slot:expr) => {
                if let Err(e) = send_protocol_frame(&mut $framed, $op, $request_id, $msg).await {
                    $err_slot = Some(e);
                    break;
                } else {
                    queued_publish_frames = 0;
                    queued_publish_bytes = 0;
                }
            };
        }

        macro_rules! flush_publishes_or_die {
            ($framed:expr, $err_slot:expr) => {
                if queued_publish_frames > 0 {
                    if let Err(e) = flush_protocol_frames(&mut $framed).await {
                        $err_slot = Some(e);
                        break;
                    }
                    queued_publish_frames = 0;
                    queued_publish_bytes = 0;
                }
            };
        }

        macro_rules! feed_encoded_publish_or_die {
            ($framed:expr, $frame:expr, $err_slot:expr) => {
                match $frame.map_err(|err| FibrilError::Unexpected {
                    msg: err.to_string(),
                }) {
                    Ok(frame) => match feed_encoded_frame(&mut $framed, frame).await {
                        Ok(size) => {
                            queued_publish_frames += 1;
                            queued_publish_bytes += size;
                            if queued_publish_frames >= PUBLISH_FLUSH_MESSAGES
                                || queued_publish_bytes >= PUBLISH_FLUSH_BYTES
                            {
                                flush_publishes_or_die!($framed, $err_slot);
                            }
                        }
                        Err(e) => {
                            $err_slot = Some(e);
                            break;
                        }
                    },
                    Err(e) => {
                        $err_slot = Some(e);
                        break;
                    }
                }
            };
        }

        macro_rules! send_encoded_or_die {
            ($framed:expr, $frame:expr, $err_slot:expr) => {
                match $frame.map_err(|err| FibrilError::Unexpected {
                    msg: err.to_string(),
                }) {
                    Ok(frame) => {
                        if let Err(e) = feed_encoded_frame(&mut $framed, frame).await {
                            $err_slot = Some(e);
                            break;
                        }
                        if let Err(e) = flush_protocol_frames(&mut $framed).await {
                            $err_slot = Some(e);
                            break;
                        }
                        queued_publish_frames = 0;
                        queued_publish_bytes = 0;
                    }
                    Err(e) => {
                        $err_slot = Some(e);
                        break;
                    }
                }
            };
        }

        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    if last_seen.elapsed() > timeout {
                        tracing::warn!("Heartbeat timout, exiting event loop.");
                        fatal_error = Some(FibrilError::Disconnection {
                            msg: format!(
                                "heartbeat timeout: no response from the broker for over {}s. \
                                 This usually means a network stall or an overloaded or stopped \
                                 broker rather than a client bug - check broker reachability and \
                                 health. The client will attempt to reconnect if auto-reconnect \
                                 is enabled",
                                timeout.as_secs()
                            ),
                        });
                        break;
                    }
                    let req_id = next_req; next_req = next_req.wrapping_add(1);
                    send_or_die!(framed, Op::Ping, req_id, &(), fatal_error)
                }

                _ = flush_ticker.tick(), if queued_publish_frames > 0 => {
                    flush_publishes_or_die!(framed, fatal_error)
                }

                _ = shutdown.notified() => {
                    tracing::info!("Shutting down, exiting event loop.");
                    if queued_publish_frames > 0 {
                        if let Err(e) = flush_protocol_frames(&mut framed).await {
                            fatal_error = Some(e);
                        }
                    }
                    break;
                }

                Some(cmd) = cmd_rx.recv() => {
                  let mut pending = Some(cmd);
                  let mut drained = 0usize;
                  while let Some(cmd) = pending.take() {
                    match cmd {
                    Command::PublishUnconfirmed { topic, group, partition, partitioning_version, content_type, headers, payload, published, ttl_ms } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        let p = Publish {
                            topic,
                            group,
                            partition,
                            require_confirm: false,
                            content_type,
                            headers,
                            payload,
                            published,
                            partition_key: None,
                            partitioning_version,
                            ttl_ms,
                        };
                        feed_encoded_publish_or_die!(framed, wire::encode_publish(req_id, &p), fatal_error)
                    }
                    Command::PublishConfirmed { topic, group, partition, partitioning_version, content_type, headers, payload, published, ttl_ms, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::Publish(reply));
                        let p = Publish {
                            topic,
                            group,
                            partition,
                            require_confirm: true,
                            content_type,
                            headers,
                            payload,
                            published,
                            partition_key: None,
                            partitioning_version,
                            ttl_ms,
                        };
                        feed_encoded_publish_or_die!(framed, wire::encode_publish(req_id, &p), fatal_error)
                    }
                    Command::PublishDelayedUnconfirmed { topic, group, partition, partitioning_version, content_type, headers, payload, published, not_before } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        let p = PublishDelayed {
                            topic,
                            group,
                            partition,
                            require_confirm: false,
                            not_before,
                            content_type,
                            headers,
                            payload,
                            published,
                            partition_key: None,
                            partitioning_version,
                        };
                        feed_encoded_publish_or_die!(framed, wire::encode_publish_delayed(req_id, &p), fatal_error)
                    }
                    Command::PublishDelayedConfirmed { topic, group, partition, partitioning_version, content_type, headers, payload, published, not_before, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::Publish(reply));
                        let p = PublishDelayed {
                            topic,
                            group,
                            partition,
                            require_confirm: true,
                            not_before,
                            content_type,
                            headers,
                            payload,
                            published,
                            partition_key: None,
                            partitioning_version,
                        };
                        feed_encoded_publish_or_die!(framed, wire::encode_publish_delayed(req_id, &p), fatal_error)
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
                    Command::SubscribeStream { req, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::StreamSubscribeManual(reply));
                        send_or_die!(framed, Op::SubscribeStream, req_id, &req, fatal_error)
                    }
                    Command::SubscribeStreamAutoAcked { req, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::StreamSubscribeAuto(reply));
                        send_or_die!(framed, Op::SubscribeStream, req_id, &req, fatal_error)
                    }
                    Command::DeclareQueue { req, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::DeclareQueue(reply));
                        send_or_die!(framed, Op::DeclareQueue, req_id, &req, fatal_error)
                    }
                    Command::DeclarePlexus { req, reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::DeclareQueue(reply));
                        send_or_die!(framed, Op::DeclarePlexus, req_id, &req, fatal_error)
                    }
                    Command::Topology { reply } => {
                        let req_id = next_req; next_req = next_req.wrapping_add(1);
                        waiters.insert(req_id, Waiter::Topology(reply));
                        send_or_die!(framed, Op::Topology, req_id, &TopologyRequest::default(), fatal_error)
                    }
                    Command::Ack { sub_id, delivery_tag, request_id } => {
                        if let Some(sub) = subs.get(&sub_id) {
                            let ack = Ack {
                                topic: sub.topic.clone(),
                                group: sub.group.clone(),
                                partition: sub.partition,
                                tags: vec![delivery_tag],
                            };
                            send_encoded_or_die!(framed, wire::encode_ack(request_id, &ack), fatal_error)
                        }
                    }
                    Command::Nack { sub_id, delivery_tag, requeue, not_before, request_id } => {
                        if let Some(sub) = subs.get(&sub_id) {
                            let nack = Nack {
                                topic: sub.topic.clone(),
                                group: sub.group.clone(),
                                partition: sub.partition,
                                tags: vec![delivery_tag],
                                requeue,
                                not_before,
                            };
                            send_encoded_or_die!(framed, wire::encode_nack(request_id, &nack), fatal_error)
                        }
                    }
                    }
                    drained += 1;
                    if drained >= ENGINE_CMD_DRAIN_MAX {
                        break;
                    }
                    pending = cmd_rx.try_recv().ok();
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
                            let ok: PublishOk = match wire::decode_publish_ok(&frame) {
                                Ok(ok) => ok,
                                Err(err) => {
                                    fatal_error = Some(FibrilError::DeserializationFailure {
                                        msg: err.to_string(),
                                    });
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
                            let d: Deliver = match wire::decode_deliver(&frame) {
                                Ok(deliver) => deliver,
                                Err(err) => {
                                    fatal_error = Some(FibrilError::DeserializationFailure {
                                        msg: err.to_string(),
                                    });
                                    break;
                                }
                            };
                            if let Some(sub) = subs.get(&d.sub_id).cloned() {
                                match sub.delivery {
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
                                                            SettleRequest::Nack { tag, requeue, not_before, request_id, response } => {
                                                                let res = cmd_tx.send(Command::Nack {
                                                                    sub_id,
                                                                    delivery_tag: tag,
                                                                    requeue,
                                                                    not_before,
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
                                        } else {
                                            tracing::warn!("Subscription receiver dropped");
                                            subs.remove(&d.sub_id);
                                            if let Ok(mut registry) = subscription_registry.write() {
                                                registry.remove(&d.sub_id);
                                            }
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
                                            tracing::warn!("Subscription receiver dropped");
                                            subs.remove(&d.sub_id);
                                            if let Ok(mut registry) = subscription_registry.write() {
                                                registry.remove(&d.sub_id);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        x if x == Op::AssignmentChanged as u16 => {
                            let changed: AssignmentChanged = match decode_protocol(&frame) {
                                Ok(changed) => changed,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };
                            // Best-effort fan-out: no subscribers (or a lagging
                            // one) just means the app isn't watching assignments.
                            let _ = assignment_tx.send(AssignmentEvent {
                                topic: changed.topic,
                                group: changed.group,
                                consumer_group: changed.consumer_group,
                                generation: changed.generation,
                                assigned: changed.assigned,
                                added: changed.added,
                                revoked: changed.revoked,
                            });
                        }
                        x if x == Op::SubscribeOk as u16 => {
                            let ok: SubscribeOk = match decode_protocol(&frame) {
                                Ok(ok) => ok,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };

                            // Cache the server-minted cohort member id (write-once)
                            // so subsequent exclusive subscribes — including to
                            // other brokers — carry the same identity.
                            if let Some(member_id) = ok.member_id {
                                let _ = cohort_member_id.set(member_id);
                            }

                            if let Some(waiter) = waiters.remove(&frame.request_id) {
                                match waiter {
                                    Waiter::SubscribeManual(tx) => {
                                        let (txm, rxm) = mpsc::channel(ok.prefetch as usize);
                                        let delivery = SubDelivery::Manual(txm);

                                        subs.insert(ok.sub_id, SubState {
                                            topic: ok.topic.clone(),
                                            group: ok.group.clone(),
                                            partition: ok.partition,
                                            delivery: delivery.clone(),
                                        });
                                        match subscription_registry.write() {
                                            Ok(mut registry) => {
                                                registry.insert(
                                                    ok.sub_id,
                                                    RegisteredSubscription {
                                                        reconcile:
                                                            reconcile_subscription_from_subscribe_ok(
                                                                &ok, false,
                                                            ),
                                                        delivery,
                                                    },
                                                );
                                            }
                                            Err(_) => {
                                                fatal_error =
                                                    Some(client_subscription_registry_poisoned());
                                                break;
                                            }
                                        }

                                        let res = tx.send(Ok(AckableSubChannel { manual: rxm }));

                                        if res.is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }

                                    Waiter::SubscribeAuto(tx) => {
                                        let (txa, rxa) = mpsc::channel(ok.prefetch as usize);
                                        let delivery = SubDelivery::Auto(txa);

                                        subs.insert(ok.sub_id, SubState {
                                            topic: ok.topic.clone(),
                                            group: ok.group.clone(),
                                            partition: ok.partition,
                                            delivery: delivery.clone(),
                                        });
                                        match subscription_registry.write() {
                                            Ok(mut registry) => {
                                                registry.insert(
                                                    ok.sub_id,
                                                    RegisteredSubscription {
                                                        reconcile:
                                                            reconcile_subscription_from_subscribe_ok(
                                                                &ok, true,
                                                            ),
                                                        delivery,
                                                    },
                                                );
                                            }
                                            Err(_) => {
                                                fatal_error =
                                                    Some(client_subscription_registry_poisoned());
                                                break;
                                            }
                                        }

                                        let res = tx.send(Ok(AutoAckedSubChannel { auto: rxa }));

                                        if res.is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }

                                    // Stream subscriptions register delivery state
                                    // (for Deliver routing + cursor-advancing acks)
                                    // but stay OUT of the reconcile registry: the
                                    // stream fan-in supervisor owns re-subscribe and
                                    // the durable cursor owns resume.
                                    Waiter::StreamSubscribeManual(tx) => {
                                        let (txm, rxm) = mpsc::channel(ok.prefetch as usize);
                                        subs.insert(ok.sub_id, SubState {
                                            topic: ok.topic.clone(),
                                            group: ok.group.clone(),
                                            partition: ok.partition,
                                            delivery: SubDelivery::Manual(txm),
                                        });
                                        if tx.send(Ok(AckableSubChannel { manual: rxm })).is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }

                                    Waiter::StreamSubscribeAuto(tx) => {
                                        let (txa, rxa) = mpsc::channel(ok.prefetch as usize);
                                        subs.insert(ok.sub_id, SubState {
                                            topic: ok.topic.clone(),
                                            group: ok.group.clone(),
                                            partition: ok.partition,
                                            delivery: SubDelivery::Auto(txa),
                                        });
                                        if tx.send(Ok(AutoAckedSubChannel { auto: rxa })).is_err() {
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
                        x if x == Op::DeclarePlexusOk as u16 => {
                            let _ok: DeclarePlexusOk = match decode_protocol(&frame) {
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
                                    tracing::error!("Internal error: protocol violation: DeclarePlexusOk for non-declare request_id")
                                }
                                None => {
                                    tracing::error!("Internal error: unexpected DeclarePlexusOk")
                                }
                            }
                        }
                        x if x == Op::TopologyOk as u16 => {
                            let ok: TopologyOk = match decode_protocol(&frame) {
                                Ok(ok) => ok,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };
                            match waiters.remove(&frame.request_id) {
                                Some(Waiter::Topology(tx)) => {
                                    let _ = tx.send(Ok(ok));
                                }
                                Some(_other) => {
                                    tracing::error!("Internal error: protocol violation: TopologyOk for non-topology request_id")
                                }
                                None => {
                                    tracing::error!("Internal error: unexpected TopologyOk")
                                }
                            }
                        }
                        x if x == Op::TopologyUpdate as u16 => {
                            // Broker-pushed routing refresh (generation changed).
                            // Apply it to the shared cache so subsequent ops route
                            // to the new owners, then ack the generation we now
                            // reflect so the broker can fence a cutover on it.
                            let topology: TopologyOk = match decode_protocol(&frame) {
                                Ok(topology) => topology,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };
                            // The shared client outlives its connections; a dropped
                            // upgrade means the client is shutting down, so there is
                            // nothing to update and no point acking.
                            if let Some(shared) = topology_sink.upgrade() {
                                let generation = shared.apply_pushed_topology(topology);
                                let ack = TopologyUpdateAck { generation };
                                let res = send_protocol_frame(
                                    &mut framed,
                                    Op::TopologyUpdateAck,
                                    frame.request_id,
                                    &ack,
                                )
                                .await;
                                if let Err(err) = res {
                                    tracing::warn!("Broken pipe");
                                    fatal_error = Some(err);
                                    break;
                                }
                            }
                        }
                        x if x == Op::Redirect as u16 => {
                            let redirect: Redirect = match decode_protocol(&frame) {
                                Ok(redirect) => redirect,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };
                            // Surface the redirect on the waiting op so the
                            // routing layer can update the cache and retry on
                            // the new owner. Unconfirmed publishes have no
                            // waiter — best-effort, the cache is corrected by a
                            // confirmed op or fetch_topology.
                            match waiters.remove(&frame.request_id) {
                                Some(waiter) => {
                                    fail_waiter(waiter, FibrilError::Redirect(Box::new(redirect)));
                                }
                                None => {
                                    tracing::debug!(
                                        "redirect for uncorrelated request {}; cache unchanged (best-effort path)",
                                        frame.request_id
                                    );
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
                        x if x == Op::GoingAway as u16 => {
                            // The broker is draining for a planned shutdown or
                            // upgrade. Surface it to the app so it can wind down,
                            // then let the existing reconnect-on-close path
                            // redirect to the post-drain owner via topology.
                            let notice: GoingAway = match decode_protocol(&frame) {
                                Ok(notice) => notice,
                                Err(err) => {
                                    fatal_error = Some(err);
                                    break;
                                }
                            };
                            tracing::info!(
                                grace_ms = notice.grace_ms,
                                "broker going away: {}",
                                notice.message
                            );
                            // The shared client outlives its connections; a
                            // dropped upgrade means the client is shutting down,
                            // so there is nothing to notify.
                            if let Some(shared) = topology_sink.upgrade() {
                                let _ = shared.going_away_tx.send(GoingAwayNotice {
                                    grace_ms: notice.grace_ms,
                                    message: notice.message,
                                });
                            }
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
                                    Waiter::StreamSubscribeManual(tx) => {
                                        if tx.send(Err(FibrilError::Failure { code: err.code, msg: err.message })).is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }
                                    Waiter::StreamSubscribeAuto(tx) => {
                                        if tx.send(Err(FibrilError::Failure { code: err.code, msg: err.message })).is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }
                                    Waiter::Topology(tx) => {
                                        let res = tx.send(Err(FibrilError::Failure { code: err.code, msg: err.message }));

                                        if res.is_err() {
                                            tracing::warn!("Broken pipe");
                                        }
                                    }
                                }
                            } else {
                                // Connection-level error (no correlated request).
                                // A non-retryable code (bad credentials, forbidden,
                                // malformed) is kept as a Failure so the reconnect
                                // path surfaces it instead of storming into the same
                                // rejection. A retryable code (owner moved, 5xx)
                                // stays a Disconnection so the existing transient
                                // reconnect/failover path is unchanged.
                                let failure = FibrilError::Failure {
                                    code: err.code,
                                    msg: err.message,
                                };
                                let fatal = if failure.retry_advice() == RetryAdvice::DoNotRetry {
                                    failure
                                } else {
                                    FibrilError::Disconnection {
                                        msg: format!("connection error {}", failure),
                                    }
                                };
                                fatal_error = Some(fatal.clone());
                                for (_, w) in waiters.drain() {
                                    fail_waiter(w, fatal.clone());
                                }

                                // subs cleared
                                subs.clear();

                                // Closing the subscription channels signals the
                                // failover supervisor, which owns re-subscribe and
                                // at-least-once redelivery against the new owner
                                // (see failover.rs). Unsupervised subs are restored
                                // by the reconcile-on-reconnect path.
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
            // A command drained mid-batch can set a fatal error through a
            // die-macro that only breaks the inner drain loop, so exit the
            // engine loop here. The other select arms break it directly.
            if fatal_error.is_some() {
                break;
            }
        }

        // ================================
        // FAIL ALL PENDING WAITERS
        // ================================

        // Record why the connection ended so the reconnect path can tell a
        // retryable transport drop from a fatal rejection (auth, forbidden).
        if let Ok(mut slot) = close_reason.lock() {
            *slot = fatal_error.clone();
        }

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
        Waiter::StreamSubscribeManual(tx) => {
            let _ = tx.send(Err(err));
        }
        Waiter::StreamSubscribeAuto(tx) => {
            let _ = tx.send(Err(err));
        }
        Waiter::Topology(tx) => {
            let _ = tx.send(Err(err));
        }
    }
}

impl EngineHandle {
    fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    async fn publish_unconfirmed(
        &self,
        topic: String,
        group: Option<String>,
        partition: Partition,
        partitioning_version: u64,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> FibrilResult<()> {
        let published = unix_millis();
        self.tx
            .send(Command::PublishUnconfirmed {
                topic,
                group,
                partition,
                partitioning_version,
                content_type,
                headers,
                payload,
                published,
                ttl_ms,
            })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        Ok(())
    }

    async fn publish_with_confirmation(
        &self,
        topic: String,
        group: Option<String>,
        partition: Partition,
        partitioning_version: u64,
        content_type: Option<ContentType>,
        headers: HashMap<String, String>,
        payload: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> FibrilResult<PublishConfirmation> {
        let (tx, rx) = oneshot::channel();
        let published = unix_millis();
        self.tx
            .send(Command::PublishConfirmed {
                topic,
                group,
                partition,
                partitioning_version,
                content_type,
                headers,
                payload,
                published,
                ttl_ms,
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
        partition: Partition,
        partitioning_version: u64,
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
                partition,
                partitioning_version,
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
        partition: Partition,
        partitioning_version: u64,
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
                partition,
                partitioning_version,
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

    async fn declare_plexus(&self, req: DeclarePlexus) -> FibrilResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::DeclarePlexus { req, reply: tx })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        rx.await.map_err(|_e| FibrilError::BrokenPipe)?
    }

    async fn fetch_topology(&self) -> FibrilResult<TopologyOk> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::Topology { reply: tx })
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

    async fn subscribe_stream(
        &self,
        req: SubscribeStream,
    ) -> FibrilResult<mpsc::Receiver<InflightMessage>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SubscribeStream { req, reply: tx })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        let chans = rx.await.map_err(|_e| FibrilError::BrokenPipe)??;
        Ok(chans.manual)
    }

    async fn subscribe_stream_auto_ack(
        &self,
        req: SubscribeStream,
    ) -> FibrilResult<mpsc::Receiver<Message>> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Command::SubscribeStreamAutoAcked { req, reply: tx })
            .await
            .map_err(|_e| FibrilError::BrokenPipe)?;
        let chans = rx.await.map_err(|_e| FibrilError::BrokenPipe)??;
        Ok(chans.auto)
    }
}

// ===== Options ===============================================================

/// Automatic reconnect policy for future operations.
///
/// The default attempts one reconnect before a new publish, subscribe, or
/// declare operation if the previous engine is already known to be closed. It
/// does not replay an operation that was already in flight when the socket
/// failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AutoReconnect {
    /// Maximum reconnect attempts before one new operation.
    pub max_attempts: usize,
}

impl AutoReconnect {
    /// Disable automatic reconnect attempts.
    pub const fn disabled() -> Self {
        Self { max_attempts: 0 }
    }

    /// Attempt reconnect once before a new operation.
    pub const fn once() -> Self {
        Self { max_attempts: 1 }
    }
}

impl Default for AutoReconnect {
    fn default() -> Self {
        Self::once()
    }
}

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
    /// Optional resume identity from a previous connection.
    pub resume_identity: Option<ResumeIdentity>,
    /// Automatic reconnect policy for future operations.
    pub auto_reconnect: AutoReconnect,
    /// How resumed connections reconcile subscriptions after reconnect.
    pub reconcile_policy: ReconcilePolicy,
    /// Max times a single operation will follow `Op::Redirect` before failing.
    pub max_redirects: u32,
    /// Minimum interval between client topology refreshes (anti-storm).
    pub topology_refresh_cooldown_ms: u64,
    /// Warm the topology cache once at connect, bounded by this timeout (ms), so
    /// the very first publish spreads across partitions and the first
    /// subscription fans in over all of them. Without it a cold cache funnels
    /// every publish to partition 0 and leaves other partitions unconsumed.
    /// `None` disables warming (cache then warms lazily via redirects). Warming
    /// is best-effort: on timeout or error the client proceeds with a cold
    /// cache (single-partition behavior).
    pub topology_warm_timeout_ms: Option<u64>,
    /// How often a live subscription re-checks the topology to pick up partitions
    /// added by a live grow (and shed ones removed by a shrink). A consumer-only
    /// client has no other reason to refresh, so the subscription polls on this
    /// interval. `None` disables auto-resubscribe (the fan-in is fixed at
    /// subscribe time). Higher values reduce background traffic at the cost of
    /// slower pickup of new partitions.
    pub partition_resubscribe_interval_ms: Option<u64>,
    /// How long a confirmed publish keeps retrying across a transient owner
    /// failover (ms) before giving up. On a transport failure to the owner the
    /// client refreshes topology from another node and retries with backoff until
    /// this deadline, so a publish issued during a failover blocks for the
    /// reassignment window and then succeeds against the new owner instead of
    /// erroring. `0` disables the retry (fail fast on the first transport error).
    pub publish_timeout_ms: u64,
    /// TLS options. `None` (default) connects plaintext. Applies to every
    /// connection the client opens, including reconnects and owner redirects.
    pub tls: Option<TlsClientOptions>,
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
            resume_identity: None,
            auto_reconnect: AutoReconnect::default(),
            reconcile_policy: ReconcilePolicy::Conservative,
            max_redirects: 3,
            topology_refresh_cooldown_ms: 1_000,
            topology_warm_timeout_ms: Some(5_000),
            partition_resubscribe_interval_ms: Some(5_000),
            publish_timeout_ms: 30_000,
            tls: None,
        }
    }

    /// Return a copy with TLS enabled using the OS trust store, for brokers
    /// with publicly issued certificates.
    pub fn tls(mut self) -> Self {
        self.tls = Some(self.tls.take().unwrap_or_default());
        self
    }

    /// Return a copy with TLS enabled, trusting the CA certificate(s) in a
    /// PEM file, e.g. the broker's generated `<data_dir>/tls/ca.pem`.
    pub fn tls_ca_path(mut self, path: impl Into<std::path::PathBuf>) -> Self {
        let mut tls = self.tls.take().unwrap_or_default();
        tls.ca_path = Some(path.into());
        self.tls = Some(tls);
        self
    }

    /// Return a copy with TLS enabled, pinning the broker certificate by the
    /// SHA-256 fingerprint printed in the broker startup log (colons
    /// optional). Pinning replaces chain-of-trust verification.
    pub fn tls_ca_fingerprint(mut self, fingerprint: impl Into<String>) -> Self {
        let mut tls = self.tls.take().unwrap_or_default();
        tls.ca_fingerprint = Some(fingerprint.into());
        self.tls = Some(tls);
        self
    }

    /// Return a copy with TLS enabled and an explicit certificate name to
    /// verify (and send as SNI), when the connect address is not the name on
    /// the certificate.
    /// Present a client certificate (PEM chain and key) to the broker, for
    /// `tls.client_auth` deployments. Enables TLS if not already enabled.
    pub fn tls_client_cert(
        mut self,
        cert_path: impl Into<std::path::PathBuf>,
        key_path: impl Into<std::path::PathBuf>,
    ) -> Self {
        let tls = self.tls.get_or_insert_with(TlsClientOptions::default);
        tls.client_cert_path = Some(cert_path.into());
        tls.client_key_path = Some(key_path.into());
        self
    }

    pub fn tls_server_name(mut self, name: impl Into<String>) -> Self {
        let mut tls = self.tls.take().unwrap_or_default();
        tls.server_name = Some(name.into());
        self.tls = Some(tls);
        self
    }

    /// Return a copy with a custom confirmed-publish failover retry budget (ms).
    /// `0` disables retry (fail fast on the first transient transport error).
    pub fn publish_timeout_ms(self, timeout_ms: u64) -> Self {
        Self {
            publish_timeout_ms: timeout_ms,
            ..self
        }
    }

    /// Return a copy with a custom connect-time topology warm timeout (ms).
    pub fn topology_warm_timeout_ms(self, timeout_ms: u64) -> Self {
        Self {
            topology_warm_timeout_ms: Some(timeout_ms),
            ..self
        }
    }

    /// Return a copy with connect-time topology warming disabled. The cache then
    /// warms lazily via redirects; a cold cache routes/consumes partition 0 only.
    pub fn disable_topology_warm(self) -> Self {
        Self {
            topology_warm_timeout_ms: None,
            ..self
        }
    }

    /// Return a copy with a custom auto-resubscribe interval (ms): how often a
    /// live subscription re-checks topology to pick up partitions added by a grow.
    pub fn partition_resubscribe_interval_ms(self, interval_ms: u64) -> Self {
        Self {
            partition_resubscribe_interval_ms: Some(interval_ms),
            ..self
        }
    }

    /// Return a copy with auto-resubscribe disabled: the subscription fan-in is
    /// fixed at subscribe time and will not pick up partitions added later.
    pub fn disable_partition_resubscribe(self) -> Self {
        Self {
            partition_resubscribe_interval_ms: None,
            ..self
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

    /// Return a copy configured to attempt resuming a previous connection.
    pub fn resume_identity(self, resume_identity: ResumeIdentity) -> Self {
        Self {
            resume_identity: Some(resume_identity),
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

    /// Return a copy with automatic reconnect disabled.
    pub fn disable_auto_reconnect(self) -> Self {
        Self {
            auto_reconnect: AutoReconnect::disabled(),
            ..self
        }
    }

    /// Return a copy with a custom automatic reconnect attempt limit.
    pub fn auto_reconnect_attempts(self, max_attempts: usize) -> Self {
        Self {
            auto_reconnect: AutoReconnect { max_attempts },
            ..self
        }
    }

    /// Return a copy with a custom reconnect subscription reconciliation policy.
    pub fn reconnect_reconcile_policy(self, reconcile_policy: ReconcilePolicy) -> Self {
        Self {
            reconcile_policy,
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
    use std::time::Duration;

    #[test]
    fn expiring_ttl_follows_delayable_convention() {
        // expiring() stamps `ttl.with_delay().as_millis()`. A bare number is
        // seconds; a Duration is taken as-is. This mirrors the delayed-publish API.
        let from_secs = 60u64.with_delay().as_millis() as u64;
        assert_eq!(from_secs, 60_000);
        let from_duration = Duration::from_millis(1500).with_delay().as_millis() as u64;
        assert_eq!(from_duration, 1500);
    }

    #[test]
    fn topology_cache_distinguishes_declared_from_gone() {
        // A declared topic with no resolved owner (mid-failover) stays "known"
        // (counts present, by_queue absent) -> caller keeps retrying. An absent
        // topic is "gone" -> caller fails fast. An empty cache is not populated.
        let empty = TopologyCache {
            generation: 0,
            by_queue: HashMap::new(),
            counts: HashMap::new(),
            last_refresh_ms: 0,
        };
        assert!(!empty.is_populated());
        assert!(!empty.knows_topic("orders", None));

        let mut counts = HashMap::new();
        counts.insert(
            ("orders".to_string(), None),
            PartitioningEntry {
                count: 4,
                version: 1,
            },
        );
        let transitioning = TopologyCache {
            generation: 1,
            by_queue: HashMap::new(), // owner unresolved during failover
            counts,
            last_refresh_ms: 1,
        };
        assert!(transitioning.is_populated());
        assert!(
            transitioning.knows_topic("orders", None),
            "declared topic stays known while its owner is transitioning"
        );
        assert!(
            !transitioning.knows_topic("ghost", None),
            "an undeclared topic is not known -> fail fast"
        );
    }

    #[test]
    fn replace_resolves_stream_owners_under_group_none() {
        // A stream topology entry must populate both the partitioning count and
        // the per-partition owner (keyed by group None), so a stream publish
        // routes to its owner the same way a queue publish does.
        use fibril_wire::{StreamTopologyEntry, TopologyOk};

        let mut cache = TopologyCache {
            generation: 0,
            by_queue: HashMap::new(),
            counts: HashMap::new(),
            last_refresh_ms: 0,
        };
        cache.replace(TopologyOk {
            generation: 9,
            queues: Vec::new(),
            streams: vec![
                StreamTopologyEntry {
                    topic: "events".into(),
                    partition: Partition::new(0),
                    owner_endpoints: vec![
                        AdvertisedAddress::parse("127.0.0.1:7000")
                            .expect("valid test owner endpoint"),
                    ],
                    partitioning_version: 2,
                    partition_count: 2,
                },
                StreamTopologyEntry {
                    topic: "events".into(),
                    partition: Partition::new(1),
                    owner_endpoints: vec![], // owner unresolved mid-failover
                    partitioning_version: 2,
                    partition_count: 2,
                },
            ],
        });

        let partitioning = cache.partitioning("events", None).expect("count cached");
        assert_eq!(partitioning.count, 2);
        let owner = cache
            .lookup("events", Partition::new(0), None)
            .expect("resolved owner");
        assert_eq!(owner.endpoint, "127.0.0.1:7000");
        assert!(
            cache.lookup("events", Partition::new(1), None).is_none(),
            "an unresolved owner leaves the partition unrouted"
        );
    }

    #[test]
    fn publish_timeout_defaults_on_and_is_configurable() {
        assert_eq!(ClientOptions::new().publish_timeout_ms, 30_000);
        assert_eq!(
            ClientOptions::new()
                .publish_timeout_ms(0)
                .publish_timeout_ms,
            0
        );
        assert_eq!(
            ClientOptions::new()
                .publish_timeout_ms(5_000)
                .publish_timeout_ms,
            5_000
        );
    }

    #[test]
    fn fnv1a_is_deterministic_and_distributes() {
        // Determinism: every client must map a key to the same partition.
        assert_eq!(fnv1a(b"entity-123"), fnv1a(b"entity-123"));
        assert_ne!(fnv1a(b"a"), fnv1a(b"b"));
        // Distribution: distinct keys spread across N partitions (not all one).
        let partitions: std::collections::HashSet<u64> = (0..32)
            .map(|i| fnv1a(format!("k{i}").as_bytes()) % 4)
            .collect();
        assert!(
            partitions.len() > 1,
            "keys should distribute across partitions, got {partitions:?}"
        );
    }

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
        assert!(GroupName::parse_optional("default").unwrap().is_none());
        assert!(matches!(
            GroupName::parse("Workers"),
            Err(FibrilError::InvalidName { kind: "group", .. })
        ));
    }

    fn engine_with_command_rx() -> (Arc<EngineHandle>, mpsc::Receiver<Command>) {
        let (tx, rx) = mpsc::channel(8);
        let engine = Arc::new(EngineHandle {
            tx,
            shutdown: Arc::new(Notify::new()),
            close_reason: Arc::new(std::sync::Mutex::new(None)),
            resume_identity: ResumeIdentity {
                owner_id: uuid::Uuid::nil(),
                client_id: uuid::Uuid::nil(),
                resume_token: uuid::Uuid::nil(),
            },
            resume_outcome: ResumeOutcome::New,
        });

        (engine, rx)
    }

    fn client_with_command_rx() -> (Client, mpsc::Receiver<Command>) {
        client_with_options_and_command_rx(ClientOptions::new())
    }

    fn client_with_options_and_command_rx(
        opts: ClientOptions,
    ) -> (Client, mpsc::Receiver<Command>) {
        let (engine, rx) = engine_with_command_rx();
        let address = "127.0.0.1:0".to_string();
        let user_shutdown = Arc::new(AtomicBool::new(false));
        let (assignment_tx, _) = broadcast::channel(ASSIGNMENT_EVENT_CAPACITY);
        let cohort_member_id = Arc::new(std::sync::OnceLock::new());
        let slot = Arc::new(EngineSlot::from_engine(
            address.clone(),
            opts.clone(),
            user_shutdown.clone(),
            Arc::new(RwLock::new(HashMap::new())),
            engine,
            assignment_tx.clone(),
            cohort_member_id.clone(),
        ));
        let mut pool = HashMap::new();
        pool.insert(address.clone(), slot);
        let shared = Arc::new(ClientShared {
            bootstrap: vec![address],
            opts,
            user_shutdown,
            pool: parking_lot::RwLock::new(pool),
            topology: ArcSwap::from_pointee(TopologyCache::default()),
            round_robin: std::sync::atomic::AtomicUsize::new(0),
            assignment_tx,
            cohort_member_id,
            catalogue: ArcSwap::from_pointee(Catalogue::default()),
            catalogue_tx: broadcast::channel(CATALOGUE_EVENT_CAPACITY).0,
            going_away_tx: broadcast::channel(GOING_AWAY_EVENT_CAPACITY).0,
            me: std::sync::OnceLock::new(),
        });
        let _ = shared.me.set(Arc::downgrade(&shared));
        (Client { shared }, rx)
    }

    #[tokio::test]
    async fn disabled_auto_reconnect_returns_broken_pipe_for_closed_engine() {
        let (client, rx) =
            client_with_options_and_command_rx(ClientOptions::new().disable_auto_reconnect());
        drop(rx);
        let publisher = client.publisher("jobs").unwrap();

        let err = publisher.publish("hello").await.unwrap_err();

        assert!(matches!(err, FibrilError::BrokenPipe));
    }

    #[tokio::test]
    async fn default_auto_reconnect_attempts_before_new_operation() {
        let (client, rx) = client_with_command_rx();
        drop(rx);
        let publisher = client.publisher("jobs").unwrap();

        let err = publisher.publish("hello").await.unwrap_err();

        assert!(matches!(err, FibrilError::Disconnection { .. }));
    }

    #[tokio::test]
    async fn prune_pool_drops_dead_owner_after_failover() {
        let (client, _rx) = client_with_command_rx();
        let bootstrap_addr = client.shared.bootstrap[0].clone();

        let make_slot = |addr: String| -> Arc<EngineSlot> {
            let (engine, _rx) = engine_with_command_rx();
            Arc::new(EngineSlot::from_engine(
                addr,
                ClientOptions::new(),
                Arc::new(AtomicBool::new(false)),
                Arc::new(RwLock::new(HashMap::new())),
                engine,
                broadcast::channel(ASSIGNMENT_EVENT_CAPACITY).0,
                Arc::new(std::sync::OnceLock::new()),
            ))
        };

        let owner_addr = "127.0.0.1:7001".to_string();
        let dead_addr = "127.0.0.1:7002".to_string();
        {
            let mut pool = client.shared.pool.write();
            pool.insert(owner_addr.clone(), make_slot(owner_addr.clone()));
            pool.insert(dead_addr.clone(), make_slot(dead_addr.clone()));
        }

        // New topology after failover: owner_addr owns a partition, dead_addr
        // owns nothing.
        let mut topo = TopologyCache::default();
        topo.by_queue.insert(
            ("jobs".to_string(), Partition::new(0), None),
            OwnerEntry {
                endpoint: owner_addr.clone(),
                partitioning_version: 0,
            },
        );
        client.shared.topology.store(Arc::new(topo));

        client.shared.prune_pool_to_topology();

        let pool = client.shared.pool.read();
        assert!(pool.contains_key(&bootstrap_addr), "bootstrap kept");
        assert!(pool.contains_key(&owner_addr), "current owner kept");
        assert!(
            !pool.contains_key(&dead_addr),
            "dead owner pruned from the pool"
        );
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
    async fn existing_publisher_uses_replaced_engine_slot() {
        let (client, mut old_rx) = client_with_command_rx();
        let publisher = client.publisher("jobs").unwrap();
        let (new_engine, mut new_rx) = engine_with_command_rx();

        let slot = client.shared.pool.read().values().next().unwrap().clone();
        slot.replace(new_engine);

        publisher.publish("hello").await.unwrap();

        match new_rx.recv().await.unwrap() {
            Command::PublishUnconfirmed { topic, group, .. } => {
                assert_eq!(topic, "jobs");
                assert_eq!(group, None);
            }
            other => panic!("expected unconfirmed publish, got {other:?}"),
        }
        assert!(old_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn reconnect_sends_active_subscription_reconciliation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        let server = tokio::spawn(async move {
            let owner_id = uuid::Uuid::new_v4();
            let client_id = uuid::Uuid::new_v4();
            let resume_token = uuid::Uuid::new_v4();

            let (first, _) = listener.accept().await.unwrap();
            let mut first = Framed::new(first, ProtoCodec);
            let hello = first.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            first
                .send(
                    try_encode(
                        Op::HelloOk,
                        hello.request_id,
                        &HelloOk {
                            protocol_version: PROTOCOL_V1,
                            owner_id,
                            client_id,
                            resume_token,
                            resume_outcome: ResumeOutcome::New,
                            server_name: "fake".into(),
                            compliance: COMPLIANCE_STRING.into(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            let subscribe = first.next().await.unwrap().unwrap();
            assert_eq!(subscribe.opcode, Op::Subscribe as u16);
            let req: Subscribe = try_decode(&subscribe).unwrap();
            first
                .send(
                    try_encode(
                        Op::SubscribeOk,
                        subscribe.request_id,
                        &SubscribeOk {
                            sub_id: 77,
                            topic: req.topic,
                            group: req.group,
                            partition: Partition::new(0),
                            prefetch: req.prefetch,
                            consumer_group: None,
                            consumer_target: None,
                            member_id: None,
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            let (second, _) = listener.accept().await.unwrap();
            let mut second = Framed::new(second, ProtoCodec);
            let hello = second.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            second
                .send(
                    try_encode(
                        Op::HelloOk,
                        hello.request_id,
                        &HelloOk {
                            protocol_version: PROTOCOL_V1,
                            owner_id,
                            client_id,
                            resume_token,
                            resume_outcome: ResumeOutcome::Resumed,
                            server_name: "fake".into(),
                            compliance: COMPLIANCE_STRING.into(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            let reconcile = second.next().await.unwrap().unwrap();
            assert_eq!(reconcile.opcode, Op::ReconcileClient as u16);
            let msg: ReconcileClient = try_decode(&reconcile).unwrap();
            tx.send(msg).await.unwrap();
            let client_sub = ReconcileSubscription {
                sub_id: 77,
                topic: "jobs".into(),
                group: None,
                partition: Partition::new(0),
                auto_ack: false,
                prefetch: 1,
                consumer_group: None,
                consumer_target: None,
                member_id: None,
            };
            let restored = ReconcileSubscription {
                sub_id: 88,
                ..client_sub.clone()
            };
            second
                .send(
                    try_encode(
                        Op::ReconcileResult,
                        reconcile.request_id,
                        &ReconcileResult {
                            subscriptions: vec![ReconcileSubscriptionResult {
                                client: Some(client_sub),
                                server: Some(restored.clone()),
                                action: ReconcileAction::Keep,
                                reason: "server_id_changed".into(),
                            }],
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            second
                .send(
                    wire::encode_deliver(
                        88,
                        &Deliver {
                            sub_id: 88,
                            topic: "jobs".into(),
                            group: None,
                            partition: Partition::new(0),
                            offset: 9,
                            delivery_tag: DeliveryTag { epoch: 123 },
                            published: 1,
                            publish_received: 2,
                            content_type: None,
                            headers: HashMap::new(),
                            payload: b"after-reconnect".to_vec(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        });

        let mut client = ClientOptions::new()
            .reconnect_reconcile_policy(ReconcilePolicy::RestoreClientSubscriptions)
            // This test's fake server scripts an exact frame sequence and does
            // not answer a topology warm; keep connect to just the handshake.
            .disable_topology_warm()
            .connect(addr)
            .await
            .unwrap();
        let mut sub = client.subscribe("jobs").unwrap().sub().await.unwrap();
        let outcome = client.reconnect().await.unwrap();

        assert_eq!(outcome.resume_outcome, ResumeOutcome::Resumed);
        let reconcile = rx.recv().await.unwrap();
        assert_eq!(
            reconcile,
            ReconcileClient {
                policy: ReconcilePolicy::RestoreClientSubscriptions,
                subscriptions: vec![ReconcileSubscription {
                    sub_id: 77,
                    topic: "jobs".into(),
                    group: None,
                    partition: Partition::new(0),
                    auto_ack: false,
                    prefetch: 1,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
                }],
            }
        );
        let msg = sub.recv().await.unwrap();
        assert_eq!(msg.payload, b"after-reconnect");
        assert_eq!(msg.delivery_tag, DeliveryTag { epoch: 123 });

        client.shutdown().await;
        server.await.unwrap();
    }

    // A broker-pushed TopologyUpdate (sent when the coordination generation
    // changes) must land in the client's routing cache so subsequent ops route to
    // the new owner, and the client must ack the generation it now reflects so the
    // broker can fence a repartition cutover on it.
    #[tokio::test]
    async fn client_applies_pushed_topology_update_and_acks() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let owner_addr = "127.0.0.1:7123".to_string();

        let server = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(conn, ProtoCodec);
            let hello = framed.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            framed
                .send(
                    try_encode(
                        Op::HelloOk,
                        hello.request_id,
                        &HelloOk {
                            protocol_version: PROTOCOL_V1,
                            owner_id: uuid::Uuid::new_v4(),
                            client_id: uuid::Uuid::new_v4(),
                            resume_token: uuid::Uuid::new_v4(),
                            resume_outcome: ResumeOutcome::New,
                            server_name: "fake".into(),
                            compliance: COMPLIANCE_STRING.into(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            // Push a topology with one queue partition owned by owner_addr.
            let topology = TopologyOk {
                generation: 7,
                queues: vec![QueueTopologyEntry {
                    topic: "jobs".into(),
                    partition: Partition::new(0),
                    group: None,
                    owner_endpoints: vec![
                        AdvertisedAddress::parse(&owner_addr.to_string())
                            .expect("valid test owner endpoint"),
                    ],
                    partitioning_version: 1,
                    partition_count: 1,
                }],
                streams: vec![],
            };
            framed
                .send(wire::encode_topology_update(0, &topology).unwrap())
                .await
                .unwrap();

            // The client must ack the generation it now reflects.
            let ack_frame = framed.next().await.unwrap().unwrap();
            assert_eq!(ack_frame.opcode, Op::TopologyUpdateAck as u16);
            let ack: TopologyUpdateAck = try_decode(&ack_frame).unwrap();
            assert_eq!(ack.generation, 7);
        });

        let client = ClientOptions::new()
            .disable_topology_warm()
            .connect(addr)
            .await
            .unwrap();

        // Wait for the cache to reflect the pushed owner (the reader loop applies
        // the push asynchronously after connect returns).
        let mut applied = None;
        for _ in 0..100 {
            if let Some(owner) =
                client
                    .shared
                    .topology
                    .load()
                    .lookup("jobs", Partition::new(0), None)
            {
                applied = Some(owner);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let owner = applied.expect("pushed topology should populate the routing cache");
        assert_eq!(owner.endpoint, "127.0.0.1:7123");
        assert_eq!(client.shared.topology.load().generation, 7);

        // The push also refreshes the catalogue snapshot.
        let catalogue = client.catalogue();
        assert_eq!(catalogue.generation, 7);
        assert_eq!(catalogue.queues.len(), 1);
        assert_eq!(catalogue.queues[0].topic, "jobs");
        assert_eq!(catalogue.queues[0].partition_count, 1);
        assert!(catalogue.streams.is_empty());

        server.await.unwrap();
        client.shutdown().await;
    }

    // A stale (out-of-order) topology push must not regress the routing cache, and
    // the client must still ack honestly with the generation it actually reflects -
    // never the lower, stale generation.
    #[tokio::test]
    async fn client_ignores_stale_topology_push_and_acks_current() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let owner_new = "127.0.0.1:7201".to_string();
        let owner_stale = "127.0.0.1:7202".to_string();

        let server = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(conn, ProtoCodec);
            let hello = framed.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            framed
                .send(
                    try_encode(
                        Op::HelloOk,
                        hello.request_id,
                        &HelloOk {
                            protocol_version: PROTOCOL_V1,
                            owner_id: uuid::Uuid::new_v4(),
                            client_id: uuid::Uuid::new_v4(),
                            resume_token: uuid::Uuid::new_v4(),
                            resume_outcome: ResumeOutcome::New,
                            server_name: "fake".into(),
                            compliance: COMPLIANCE_STRING.into(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            let push = |generation: u64, owner: String| TopologyOk {
                generation,
                queues: vec![QueueTopologyEntry {
                    topic: "jobs".into(),
                    partition: Partition::new(0),
                    group: None,
                    owner_endpoints: vec![
                        AdvertisedAddress::parse(&owner.to_string())
                            .expect("valid test owner endpoint"),
                    ],
                    partitioning_version: 1,
                    partition_count: 1,
                }],
                streams: vec![],
            };

            // Newer generation first, then a stale (lower) generation.
            framed
                .send(wire::encode_topology_update(0, &push(7, owner_new.clone())).unwrap())
                .await
                .unwrap();
            let ack: TopologyUpdateAck =
                try_decode(&framed.next().await.unwrap().unwrap()).unwrap();
            assert_eq!(ack.generation, 7);

            framed
                .send(wire::encode_topology_update(0, &push(3, owner_stale)).unwrap())
                .await
                .unwrap();
            // The stale push is ignored, but still acked with the CURRENT generation.
            let ack: TopologyUpdateAck =
                try_decode(&framed.next().await.unwrap().unwrap()).unwrap();
            assert_eq!(ack.generation, 7);
        });

        let client = ClientOptions::new()
            .disable_topology_warm()
            .connect(addr)
            .await
            .unwrap();

        // Both acks observed by the server task means both pushes were processed.
        server.await.unwrap();

        // Routing did not regress to the stale owner, and the generation held.
        let owner = client
            .shared
            .topology
            .load()
            .lookup("jobs", Partition::new(0), None)
            .expect("routing cache populated");
        assert_eq!(owner.endpoint, "127.0.0.1:7201");
        assert_eq!(client.shared.topology.load().generation, 7);

        client.shutdown().await;
    }

    // A topology change must surface on the catalogue change feed and in the
    // catalogue snapshot accessor. Driven through fetch_topology for deterministic
    // timing (the push path is covered by the test above).
    #[tokio::test]
    async fn catalogue_events_report_declared_queues_and_streams() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.unwrap();
            let mut framed = Framed::new(conn, ProtoCodec);
            let hello = framed.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            framed
                .send(
                    try_encode(
                        Op::HelloOk,
                        hello.request_id,
                        &HelloOk {
                            protocol_version: PROTOCOL_V1,
                            owner_id: uuid::Uuid::new_v4(),
                            client_id: uuid::Uuid::new_v4(),
                            resume_token: uuid::Uuid::new_v4(),
                            resume_outcome: ResumeOutcome::New,
                            server_name: "fake".into(),
                            compliance: COMPLIANCE_STRING.into(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            // Answer the fetch_topology request with one queue and one stream.
            let req = framed.next().await.unwrap().unwrap();
            assert_eq!(req.opcode, Op::Topology as u16);
            let topology = TopologyOk {
                generation: 5,
                queues: vec![QueueTopologyEntry {
                    topic: "jobs".into(),
                    partition: Partition::new(0),
                    group: Some("workers".into()),
                    owner_endpoints: vec![],
                    partitioning_version: 1,
                    partition_count: 3,
                }],
                streams: vec![StreamTopologyEntry {
                    topic: "events".into(),
                    partition: Partition::new(0),
                    owner_endpoints: vec![],
                    partitioning_version: 1,
                    partition_count: 2,
                }],
            };
            framed
                .send(wire::encode_topology_ok(req.request_id, &topology).unwrap())
                .await
                .unwrap();
        });

        let client = ClientOptions::new()
            .disable_topology_warm()
            .connect(addr)
            .await
            .unwrap();

        let mut events = client.catalogue_events();
        client.fetch_topology().await.unwrap();

        let catalogue = events.recv().await.unwrap();
        assert_eq!(catalogue.generation, 5);
        assert_eq!(catalogue.queues.len(), 1);
        assert_eq!(catalogue.queues[0].topic, "jobs");
        assert_eq!(catalogue.queues[0].group.as_deref(), Some("workers"));
        assert_eq!(catalogue.queues[0].partition_count, 3);
        assert_eq!(catalogue.streams.len(), 1);
        assert_eq!(catalogue.streams[0].topic, "events");
        assert_eq!(catalogue.streams[0].partition_count, 2);

        // The accessor returns the same snapshot.
        assert_eq!(client.catalogue(), catalogue);

        server.await.unwrap();
        client.shutdown().await;
    }

    // Regression: when the owner broker restarts in place (bounce faster than
    // failover, so ownership stays put), the client reconnects to a FRESH session
    // (resume REJECTED). It must still reconcile its active subscriptions so the
    // broker re-establishes delivery - otherwise the consumer's stream stays open
    // but unfed and it silently stops receiving (found by the chaos soak). Uses
    // the DEFAULT (Conservative) policy to prove the common case.
    #[tokio::test]
    async fn reconnect_reconciles_subscriptions_when_resume_rejected() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (tx, mut rx) = mpsc::channel(1);

        let server = tokio::spawn(async move {
            let owner_id = uuid::Uuid::new_v4();
            let client_id = uuid::Uuid::new_v4();
            let resume_token = uuid::Uuid::new_v4();

            let (first, _) = listener.accept().await.unwrap();
            let mut first = Framed::new(first, ProtoCodec);
            let hello = first.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            first
                .send(
                    try_encode(
                        Op::HelloOk,
                        hello.request_id,
                        &HelloOk {
                            protocol_version: PROTOCOL_V1,
                            owner_id,
                            client_id,
                            resume_token,
                            resume_outcome: ResumeOutcome::New,
                            server_name: "fake".into(),
                            compliance: COMPLIANCE_STRING.into(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            let subscribe = first.next().await.unwrap().unwrap();
            assert_eq!(subscribe.opcode, Op::Subscribe as u16);
            let req: Subscribe = try_decode(&subscribe).unwrap();
            first
                .send(
                    try_encode(
                        Op::SubscribeOk,
                        subscribe.request_id,
                        &SubscribeOk {
                            sub_id: 77,
                            topic: req.topic,
                            group: req.group,
                            partition: Partition::new(0),
                            prefetch: req.prefetch,
                            consumer_group: None,
                            consumer_target: None,
                            member_id: None,
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            // Reconnect: the broker came back FRESH, so resume is REJECTED.
            let (second, _) = listener.accept().await.unwrap();
            let mut second = Framed::new(second, ProtoCodec);
            let hello = second.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            second
                .send(
                    try_encode(
                        Op::HelloOk,
                        hello.request_id,
                        &HelloOk {
                            protocol_version: PROTOCOL_V1,
                            owner_id,
                            client_id,
                            resume_token,
                            resume_outcome: ResumeOutcome::ResumeRejected,
                            server_name: "fake".into(),
                            compliance: COMPLIANCE_STRING.into(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();

            // The fix: a reconcile arrives even though resume was rejected.
            let reconcile = second.next().await.unwrap().unwrap();
            assert_eq!(reconcile.opcode, Op::ReconcileClient as u16);
            let msg: ReconcileClient = try_decode(&reconcile).unwrap();
            tx.send(msg).await.unwrap();
            let client_sub = ReconcileSubscription {
                sub_id: 77,
                topic: "jobs".into(),
                group: None,
                partition: Partition::new(0),
                auto_ack: false,
                prefetch: 1,
                consumer_group: None,
                consumer_target: None,
                member_id: None,
            };
            let restored = ReconcileSubscription {
                sub_id: 88,
                ..client_sub.clone()
            };
            second
                .send(
                    try_encode(
                        Op::ReconcileResult,
                        reconcile.request_id,
                        &ReconcileResult {
                            subscriptions: vec![ReconcileSubscriptionResult {
                                client: Some(client_sub),
                                server: Some(restored.clone()),
                                action: ReconcileAction::Keep,
                                reason: "server_restored".into(),
                            }],
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
            second
                .send(
                    wire::encode_deliver(
                        88,
                        &Deliver {
                            sub_id: 88,
                            topic: "jobs".into(),
                            group: None,
                            partition: Partition::new(0),
                            offset: 9,
                            delivery_tag: DeliveryTag { epoch: 123 },
                            published: 1,
                            publish_received: 2,
                            content_type: None,
                            headers: HashMap::new(),
                            payload: b"after-restart".to_vec(),
                        },
                    )
                    .unwrap(),
                )
                .await
                .unwrap();
        });

        let mut client = ClientOptions::new()
            .disable_topology_warm()
            .connect(addr)
            .await
            .unwrap();
        let mut sub = client.subscribe("jobs").unwrap().sub().await.unwrap();
        let outcome = client.reconnect().await.unwrap();

        // Resume was rejected, yet the client still reconciled (the regression).
        assert_eq!(outcome.resume_outcome, ResumeOutcome::ResumeRejected);
        let reconcile = rx.recv().await.unwrap();
        assert_eq!(reconcile.policy, ReconcilePolicy::Conservative);
        assert_eq!(reconcile.subscriptions.len(), 1);
        assert_eq!(reconcile.subscriptions[0].sub_id, 77);
        assert_eq!(reconcile.subscriptions[0].topic, "jobs");
        // And delivery resumes on the re-established subscription.
        let msg = sub.recv().await.unwrap();
        assert_eq!(msg.payload, b"after-restart");

        client.shutdown().await;
        server.await.unwrap();
    }

    #[tokio::test]
    async fn assignment_changed_push_reaches_assignment_events() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (go_tx, go_rx) = oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.unwrap();
            let mut conn = Framed::new(conn, ProtoCodec);
            let hello = conn.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            conn.send(
                try_encode(
                    Op::HelloOk,
                    hello.request_id,
                    &HelloOk {
                        protocol_version: PROTOCOL_V1,
                        owner_id: uuid::Uuid::new_v4(),
                        client_id: uuid::Uuid::new_v4(),
                        resume_token: uuid::Uuid::new_v4(),
                        resume_outcome: ResumeOutcome::New,
                        server_name: "fake".into(),
                        compliance: COMPLIANCE_STRING.into(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

            // Send the push only once the test is listening (broadcast delivers
            // only to receivers that exist at send time).
            go_rx.await.unwrap();
            conn.send(
                try_encode(
                    Op::AssignmentChanged,
                    0,
                    &AssignmentChanged {
                        topic: "jobs".into(),
                        group: None,
                        consumer_group: "g".into(),
                        generation: 7,
                        assigned: vec![Partition::new(0), Partition::new(2)],
                        added: vec![Partition::new(2)],
                        revoked: vec![Partition::new(1)],
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
            // Keep the connection alive until the test finishes.
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = ClientOptions::new()
            .disable_topology_warm()
            .connect(addr)
            .await
            .unwrap();
        let mut events = client.assignment_events();
        go_tx.send(()).unwrap();

        let event = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.topic, "jobs");
        assert_eq!(event.consumer_group, "g");
        assert_eq!(event.generation, 7);
        assert_eq!(event.assigned, vec![Partition::new(0), Partition::new(2)]);
        assert_eq!(event.added, vec![Partition::new(2)]);
        assert_eq!(event.revoked, vec![Partition::new(1)]);

        client.shutdown().await;
        server.abort();
    }

    #[tokio::test]
    async fn going_away_push_reaches_going_away_events() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (go_tx, go_rx) = oneshot::channel::<()>();

        let server = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.unwrap();
            let mut conn = Framed::new(conn, ProtoCodec);
            let hello = conn.next().await.unwrap().unwrap();
            assert_eq!(hello.opcode, Op::Hello as u16);
            conn.send(
                try_encode(
                    Op::HelloOk,
                    hello.request_id,
                    &HelloOk {
                        protocol_version: PROTOCOL_V1,
                        owner_id: uuid::Uuid::new_v4(),
                        client_id: uuid::Uuid::new_v4(),
                        resume_token: uuid::Uuid::new_v4(),
                        resume_outcome: ResumeOutcome::New,
                        server_name: "fake".into(),
                        compliance: COMPLIANCE_STRING.into(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

            // Send the push only once the test is listening (broadcast delivers
            // only to receivers that exist at send time).
            go_rx.await.unwrap();
            conn.send(
                try_encode(
                    Op::GoingAway,
                    0,
                    &GoingAway {
                        grace_ms: 30_000,
                        message: "broker restarting for upgrade".into(),
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let client = ClientOptions::new()
            .disable_topology_warm()
            .connect(addr)
            .await
            .unwrap();
        let mut events = client.going_away_events();
        go_tx.send(()).unwrap();

        let notice = tokio::time::timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(notice.grace_ms, 30_000);
        assert_eq!(notice.message, "broker restarting for upgrade");

        client.shutdown().await;
        server.abort();
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
    async fn reliable_publisher_stamps_ids_and_retries_until_confirmed() {
        let (client, mut rx) = client_with_command_rx();
        let reliable = client.publisher("jobs").unwrap().reliable();
        let pid = reliable.producer_id().to_string();
        assert_eq!(
            reliable.clone().producer_id().to_string(),
            pid,
            "clones share the producer id"
        );

        let task = tokio::spawn(async move { reliable.publish("hello").await });

        // First attempt: producer id/seq stamped under the fibril.client.* carve-out;
        // fail with a retryable (but not transport-transient) error so the outer
        // ReliablePublisher loop re-publishes (inner publish_confirmed returns
        // ERR_NOT_OWNER rather than auto-retrying it).
        match rx.recv().await.unwrap() {
            Command::PublishConfirmed { headers, reply, .. } => {
                assert_eq!(headers.get(HEADER_PRODUCER_ID), Some(&pid));
                assert_eq!(headers.get(HEADER_PRODUCER_SEQ), Some(&"0".to_string()));
                reply
                    .send(Err(FibrilError::Failure {
                        code: ERR_NOT_OWNER,
                        msg: "owner moved".into(),
                    }))
                    .unwrap();
            }
            other => panic!("expected confirmed publish, got {other:?}"),
        }
        // Re-publish keeps the SAME sequence, then confirms.
        match rx.recv().await.unwrap() {
            Command::PublishConfirmed { headers, reply, .. } => {
                assert_eq!(
                    headers.get(HEADER_PRODUCER_SEQ),
                    Some(&"0".to_string()),
                    "a retry re-sends the same seq"
                );
                reply.send(Ok(7)).unwrap();
            }
            other => panic!("expected re-publish, got {other:?}"),
        }
        assert_eq!(task.await.unwrap().unwrap(), 7);
    }

    #[tokio::test]
    async fn declare_plexus_sends_stream_config() {
        let (client, mut rx) = client_with_command_rx();
        let task = tokio::spawn(async move {
            client
                .declare_plexus(
                    StreamConfig::new("events")
                        .unwrap()
                        .partitions(4)
                        .speculative()
                        .retain_records(1_000)
                        .retain_for(60u64),
                )
                .await
        });

        match rx.recv().await.unwrap() {
            Command::DeclarePlexus { req, reply } => {
                assert_eq!(req.topic, "events");
                assert_eq!(req.partition_count, Some(4));
                assert_eq!(req.durability, StreamDurability::Speculative);
                assert_eq!(req.retention.max_records, Some(1_000));
                assert_eq!(req.retention.max_age_ms, Some(60_000));
                assert_eq!(req.retention.max_bytes, None);
                reply.send(Ok(())).unwrap();
            }
            other => panic!("expected declare plexus, got {other:?}"),
        }

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stream_subscribe_sends_durable_filtered_request() {
        let (client, mut rx) = client_with_command_rx();
        let task = tokio::spawn(async move {
            client
                .stream("events")
                .unwrap()
                .durable("analytics")
                .from_earliest()
                .filter("region", "eu-*")
                .filter("kind", "order")
                .prefetch(8)
                .sub()
                .await
        });

        match rx.recv().await.unwrap() {
            Command::SubscribeStream { req, reply } => {
                assert_eq!(req.topic, "events");
                assert_eq!(req.partition, Partition::new(0));
                assert_eq!(req.durable_name.as_deref(), Some("analytics"));
                assert_eq!(req.start, StreamStart::Earliest);
                assert_eq!(
                    req.filter,
                    vec![
                        ("region".to_string(), "eu-*".to_string()),
                        ("kind".to_string(), "order".to_string()),
                    ]
                );
                assert_eq!(req.prefetch, 8);
                assert!(!req.auto_ack);
                let (_tx, manual) = mpsc::channel(1);
                reply.send(Ok(AckableSubChannel { manual })).unwrap();
            }
            other => panic!("expected subscribe stream, got {other:?}"),
        }

        let _subscription = task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn stream_subscribe_fans_in_over_explicit_partitions() {
        let (client, mut rx) = client_with_command_rx();
        let task = tokio::spawn(async move {
            client
                .stream("events")
                .unwrap()
                .partitions(3)
                .sub_auto_ack()
                .await
        });

        // One SubscribeStream per partition, auto-ack, default latest start.
        let mut seen = Vec::new();
        for _ in 0..3 {
            match rx.recv().await.unwrap() {
                Command::SubscribeStreamAutoAcked { req, reply } => {
                    assert_eq!(req.topic, "events");
                    assert!(req.auto_ack);
                    assert_eq!(req.start, StreamStart::Latest);
                    seen.push(req.partition.id());
                    let (_tx, auto) = mpsc::channel(1);
                    reply.send(Ok(AutoAckedSubChannel { auto })).unwrap();
                }
                other => panic!("expected auto-ack subscribe stream, got {other:?}"),
            }
        }
        seen.sort_unstable();
        assert_eq!(seen, vec![0, 1, 2]);

        let _subscription = task.await.unwrap().unwrap();
    }

    #[test]
    fn user_headers_cannot_set_reserved_namespaces() {
        let msg = NewMessage::raw(vec![1])
            .header("trace-id", "abc")
            .header("fibril.client.producer_id", "spoofed")
            .header("fibril.retries", "9")
            .header("stroma.dlq.source_topic", "evil");
        assert_eq!(msg.headers().get("trace-id"), Some(&"abc".to_string()));
        assert!(msg.headers().get("fibril.client.producer_id").is_none());
        assert!(msg.headers().get("fibril.retries").is_none());
        assert!(msg.headers().get("stroma.dlq.source_topic").is_none());
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
    async fn declare_carries_default_message_ttl() {
        let (client, mut rx) = client_with_command_rx();
        let task = tokio::spawn(async move {
            client
                .declare_queue(
                    QueueConfig::new("ephemeral")
                        .unwrap()
                        .default_message_ttl(30u64),
                )
                .await
        });

        match rx.recv().await.unwrap() {
            Command::DeclareQueue { req, reply } => {
                // 30 seconds via the Delayable convention -> 30_000 ms.
                assert_eq!(req.default_message_ttl_ms, Some(30_000));
                reply.send(Ok(())).unwrap();
            }
            other => panic!("expected declare queue, got {other:?}"),
        }

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn default_group_declares_ungrouped_queue() {
        let (client, mut rx) = client_with_command_rx();
        let task = tokio::spawn(async move {
            client
                .declare_queue(QueueConfig::new("jobs").unwrap().group("default").unwrap())
                .await
        });

        match rx.recv().await.unwrap() {
            Command::DeclareQueue { req, reply } => {
                assert_eq!(req.topic, "jobs");
                assert_eq!(req.group, None);
                reply.send(Ok(())).unwrap();
            }
            other => panic!("expected declare queue, got {other:?}"),
        }

        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn default_group_publisher_uses_ungrouped_queue() {
        let (client, mut rx) = client_with_command_rx();
        let publisher = client.publisher_grouped("jobs", "default").unwrap();

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
    async fn subscribe_flags_match_ack_mode() {
        let (client, mut rx) = client_with_command_rx();
        // Hold the mock stream senders open so the per-partition streams stay live;
        // otherwise the supervisor (correctly) sees the stream end and re-subscribes,
        // which would interleave an extra Subscribe with the auto-subscribe below.
        let mut keep_open: Vec<Box<dyn std::any::Any + Send>> = Vec::new();
        let manual_client = client.clone();
        let manual =
            tokio::spawn(async move { manual_client.subscribe("jobs").unwrap().sub().await });

        match rx.recv().await.unwrap() {
            Command::Subscribe { req, reply } => {
                assert!(!req.auto_ack);
                let (tx, manual_rx) = mpsc::channel(1);
                keep_open.push(Box::new(tx));
                reply
                    .send(Ok(AckableSubChannel { manual: manual_rx }))
                    .unwrap();
            }
            other => panic!("expected manual subscribe, got {other:?}"),
        }
        let _manual_sub = manual.await.unwrap().unwrap();

        let auto_client = client.clone();
        let auto =
            tokio::spawn(
                async move { auto_client.subscribe("jobs").unwrap().sub_auto_ack().await },
            );

        match rx.recv().await.unwrap() {
            Command::SubscribeAutoAcked { req, reply } => {
                assert!(req.auto_ack);
                let (tx, auto_rx) = mpsc::channel(1);
                keep_open.push(Box::new(tx));
                reply
                    .send(Ok(AutoAckedSubChannel { auto: auto_rx }))
                    .unwrap();
            }
            other => panic!("expected auto subscribe, got {other:?}"),
        }
        let _auto_sub = auto.await.unwrap().unwrap();
        drop(keep_open);
    }

    #[tokio::test]
    async fn retry_after_sends_delayed_nack() {
        let (settle_tx, settle_rx) = oneshot::channel();
        let msg = InflightMessage {
            delivery_tag: DeliveryTag { epoch: 7 },
            published: 1,
            publish_received: 2,
            headers: HashMap::new(),
            content_type: None,
            payload: b"later".to_vec(),
            request_id: 42,
            settle: settle_tx,
        };

        let before = unix_millis();
        let task =
            tokio::spawn(
                async move { msg.retry_after(std::time::Duration::from_millis(250)).await },
            );
        let req = settle_rx.await.unwrap();
        let after = unix_millis();

        match req {
            SettleRequest::Nack {
                tag,
                requeue,
                not_before,
                request_id,
                response,
            } => {
                assert_eq!(tag, DeliveryTag { epoch: 7 });
                assert!(requeue);
                let not_before = not_before.expect("retry_after should set a deadline");
                assert!(not_before >= before + 250);
                assert!(not_before <= after + 250);
                assert_eq!(request_id, 42);
                response.send(Ok(())).unwrap();
            }
            SettleRequest::Ack { .. } => panic!("expected delayed nack, got ack"),
        }

        let settled = task.await.unwrap().unwrap();
        assert_eq!(settled.payload, b"later".to_vec());
    }

    #[test]
    fn new_message_exposes_headers_and_content_type() {
        let message = NewMessage::text("hello")
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
    fn text_message_sets_text_content_type() {
        let message = NewMessage::text("hello");
        let message = Message {
            delivery_tag: DeliveryTag { epoch: 1 },
            published: 0,
            publish_received: 0,
            content_type: message.content_type,
            headers: message.headers,
            payload: message.payload,
        };

        assert_eq!(message.content_type(), Some("text/plain; charset=utf-8"));
        assert_eq!(message.text().unwrap(), "hello");
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
