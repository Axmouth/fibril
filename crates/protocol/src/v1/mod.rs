pub mod client;
pub mod frame;
pub mod handler;
pub mod helper;
pub mod replication;
pub mod replication_payload;
pub mod wire;

use std::collections::HashMap;

pub use fibril_storage::{DeliveryTag, Partition};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

fn default_prefetch() -> u32 {
    1
}

/// Handshake magic bytes.
/// IMPORTANT: This field MUST be included and preserved unchanged
/// in this protocol implementation. It serves as a protocol-level
/// declaration of the author's objection to use of this software for
/// machine learning training or dataset inclusion.
/// See AI_POLICY.md for details.
/// Identifier: NF-SOVEREIGN-2025-GN-OPT-OUT-TDM
#[doc(alias = "ai_training_prohibited")]
#[doc(alias = "sovereign_protocol_handshake")]
#[doc(alias = "protocol_compliance_marker")]
pub const COMPLIANCE_STRING: &str = "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md";

pub const PROTOCOL_V1: u16 = 1;

#[repr(u16)]
#[derive(Debug, Copy, Clone)]
pub enum Op {
    Hello = 1,
    HelloOk = 2,
    HelloErr = 3,

    Auth = 10,
    AuthOk = 11,
    AuthErr = 12,

    Publish = 20,
    PublishDelayed = 21,
    PublishOk = 25,

    Subscribe = 30,
    SubscribeOk = 31,
    SubscribeErr = 32,

    Deliver = 40,
    Ack = 41,
    Nack = 42,
    /// Server->client push: a member's exclusive consumer-group assignment
    /// changed (informational; the per-partition gate enforces it regardless).
    AssignmentChanged = 43,

    Ping = 50,
    Pong = 51,

    DeclareQueue = 60,
    DeclareQueueOk = 61,

    ReconcileClient = 70,
    ReconcileServer = 71,
    ReconcileResult = 72,

    ReplicationRead = 80,
    ReplicationReadOk = 81,
    ReplicationApply = 82,
    ReplicationApplyOk = 83,
    ReplicationCheckpointExport = 84,
    ReplicationCheckpointExportOk = 85,
    ReplicationCheckpointInstall = 86,
    ReplicationCheckpointInstallOk = 87,

    Topology = 90,
    TopologyOk = 91,
    Redirect = 92,

    Error = 255,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Hello {
    pub client_name: String,
    pub client_version: String,
    pub protocol_version: u16, // client-supported version
    pub resume: Option<ResumeIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloOk {
    pub protocol_version: u16, // negotiated
    pub owner_id: Uuid,
    pub client_id: Uuid,
    pub resume_token: Uuid,
    pub resume_outcome: ResumeOutcome,
    pub server_name: String,
    pub compliance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResumeIdentity {
    pub owner_id: Uuid,
    pub client_id: Uuid,
    pub resume_token: Uuid,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResumeOutcome {
    New,
    Resumed,
    ResumeNotFound,
    ResumeRejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Auth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ContentType {
    MsgPack,
    Json,
    Text,
    Custom(String),
}

impl ContentType {
    pub fn from_header(value: impl Into<String>) -> Self {
        let value = value.into();
        match value.split(';').next().map(str::trim) {
            Some("application/msgpack") => ContentType::MsgPack,
            Some("application/json") => ContentType::Json,
            Some("text/plain") if value == "text/plain; charset=utf-8" => ContentType::Text,
            _ => ContentType::Custom(value),
        }
    }

    pub fn as_header(&self) -> &str {
        match self {
            ContentType::MsgPack => "application/msgpack",
            ContentType::Json => "application/json",
            ContentType::Text => "text/plain; charset=utf-8",
            ContentType::Custom(value) => value,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Publish {
    pub topic: String,
    pub partition: Partition, // explicit partition override; default 0
    pub group: Option<String>,
    pub require_confirm: bool,
    #[serde(default)]
    pub content_type: Option<ContentType>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
    pub published: u64,
    /// Optional partition key: `hash(key) % partition_count` selects the
    /// partition (Kafka-style, for per-key ordering). Absent => round-robin.
    /// Purely partition selection — NOT a RabbitMQ-style routing key.
    #[serde(default)]
    pub partition_key: Option<Vec<u8>>,
    /// The partitioning version the client routed under. The owner fences
    /// against a stale view: if this lags the queue's authoritative version,
    /// the chosen partition may no longer be correct, so the owner redirects
    /// the client to re-fetch topology. `0` is the default/unknown version and
    /// matches a single-partition v0 queue (standalone / non-cluster path).
    #[serde(default)]
    pub partitioning_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishDelayed {
    pub topic: String,
    pub partition: Partition,
    pub group: Option<String>,
    pub require_confirm: bool,
    pub not_before: u64,
    #[serde(default)]
    pub content_type: Option<ContentType>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
    pub published: u64,
    /// Optional partition key: `hash(key) % partition_count` selects the
    /// partition (Kafka-style, for per-key ordering). Absent => round-robin.
    /// Purely partition selection — NOT a RabbitMQ-style routing key.
    #[serde(default)]
    pub partition_key: Option<Vec<u8>>,
    /// The partitioning version the client routed under. See [`Publish`].
    #[serde(default)]
    pub partitioning_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishOk {
    // TODO: use delivery tag?
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum QueueDlqPolicy {
    Discard,
    Global,
    Custom {
        topic: String,
        group: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareQueue {
    pub topic: String,
    pub group: Option<String>,
    pub dlq_policy: Option<QueueDlqPolicy>,
    pub dlq_max_retries: Option<u32>,
    /// Desired partition count for this `(topic, group)` queue. `None` uses the
    /// cluster default (`default_partition_count`). Serde-default keeps older
    /// clients wire-compatible.
    #[serde(default)]
    pub partition_count: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareQueueOk {
    pub status: String,
    /// Effective partition count of the declared queue.
    #[serde(default)]
    pub partition_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscribe {
    pub topic: String,
    /// The partition to subscribe to. A multi-partition subscription opens one
    /// `Subscribe` per partition; single-partition / standalone uses 0. Serde
    /// default keeps older clients (which omit it) on partition 0.
    #[serde(default)]
    pub partition: Partition,
    pub group: Option<String>,
    pub prefetch: u32,
    pub auto_ack: bool,
    /// Opt-in exclusive consumer-group id. `None` (default) = the normal
    /// competing-consumer behavior. `Some(id)` joins an exclusive cohort that
    /// divides the queue's partitions (each partition to one member) for
    /// per-partition ordering; the server assigns partitions and the wire
    /// `partition` is ignored for the join.
    #[serde(default)]
    pub consumer_group: Option<String>,
    /// Soft per-consumer target: the member's desired max partitions within its
    /// exclusive cohort. `None` (default) uses the group default. Only meaningful
    /// alongside `consumer_group`; coverage always wins over the target.
    #[serde(default)]
    pub consumer_target: Option<u32>,
    /// Cluster-scoped cohort member identity. A consumer that spans brokers
    /// carries the SAME id on every exclusive subscribe so the cohort recognizes
    /// it as one member across owners. `None` on the first exclusive subscribe —
    /// the server mints one and returns it in `SubscribeOk`; the client then
    /// echoes it. Ignored without `consumer_group`.
    #[serde(default)]
    pub member_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeOk {
    pub sub_id: u64,
    pub topic: String,
    pub group: Option<String>,
    pub partition: Partition,
    pub prefetch: u32,
    /// Echoes the exclusive cohort id this subscription joined (if any), so the
    /// client can restore exclusive membership on reconnect-reconcile.
    #[serde(default)]
    pub consumer_group: Option<String>,
    /// Echoes the soft per-consumer target (if any).
    #[serde(default)]
    pub consumer_target: Option<u32>,
    /// The cluster-scoped cohort member id in effect for this subscription
    /// (server-minted on the first exclusive subscribe). The client caches it and
    /// echoes it on its other exclusive subscribes and across reconnects.
    #[serde(default)]
    pub member_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileSubscription {
    pub sub_id: u64,
    pub topic: String,
    pub group: Option<String>,
    pub partition: Partition,
    pub auto_ack: bool,
    #[serde(default = "default_prefetch")]
    pub prefetch: u32,
    /// Exclusive cohort id to restore this subscription into (if any). Carried so
    /// a reconnect rejoins the cohort instead of silently falling back to
    /// competing.
    #[serde(default)]
    pub consumer_group: Option<String>,
    /// Soft per-consumer target to restore (if any).
    #[serde(default)]
    pub consumer_target: Option<u32>,
    /// Cohort member id to restore under, so a reconnect keeps the same cluster
    /// identity rather than being minted a new one.
    #[serde(default)]
    pub member_id: Option<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileClient {
    #[serde(default)]
    pub policy: ReconcilePolicy,
    pub subscriptions: Vec<ReconcileSubscription>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReconcilePolicy {
    Conservative,
    RestoreClientSubscriptions,
}

impl Default for ReconcilePolicy {
    fn default() -> Self {
        Self::Conservative
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileServer {
    pub subscriptions: Vec<ReconcileSubscription>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReconcileAction {
    Keep,
    CloseClientSide,
    CloseServerSide,
    RecreateClientSide,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileSubscriptionResult {
    pub client: Option<ReconcileSubscription>,
    pub server: Option<ReconcileSubscription>,
    pub action: ReconcileAction,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileResult {
    pub subscriptions: Vec<ReconcileSubscriptionResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationRead {
    pub topic: String,
    pub group: Option<String>,
    pub partition: Partition,
    pub message_from: u64,
    pub event_from: u64,
    pub max_messages: u32,
    pub max_events: u32,
    /// Approximate byte budget for returned records. One oversized message may
    /// still exceed this so replication can make progress.
    #[serde(default)]
    pub max_bytes: u64,
    /// Optional long-poll budget. Zero means an immediate read. Followers use
    /// this only after they have caught up; ordinary catch-up remains pull.
    #[serde(default)]
    pub max_wait_ms: u32,
    /// Follower identity for owner-side progress tracking (publish-confirm
    /// durability policies). Followers apply durably, so `message_from` /
    /// `event_from` are honest durable progress. Optional: old peers simply
    /// don't report.
    #[serde(default)]
    pub reporter_node_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationMessageRecord {
    pub offset: u64,
    pub flags: u16,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationEventRecord {
    pub offset: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationCheckpointRequired {
    pub epoch: u64,
    pub requested_offset: u64,
    pub head_offset: u64,
    pub next_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReplicationMessageRead {
    Batch {
        epoch: u64,
        requested_offset: u64,
        next_offset: u64,
        records: Vec<ReplicationMessageRecord>,
    },
    CheckpointRequired(ReplicationCheckpointRequired),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReplicationEventRead {
    Batch {
        epoch: u64,
        requested_offset: u64,
        next_offset: u64,
        records: Vec<ReplicationEventRecord>,
    },
    CheckpointRequired(ReplicationCheckpointRequired),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationReadOk {
    pub messages: ReplicationMessageRead,
    pub events: ReplicationEventRead,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationMessageApplyBatch {
    pub epoch: u64,
    pub records: Vec<ReplicationMessageRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationEventApplyBatch {
    pub epoch: u64,
    pub records: Vec<ReplicationEventRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationApply {
    pub topic: String,
    pub group: Option<String>,
    pub partition: Partition,
    pub messages: Option<ReplicationMessageApplyBatch>,
    pub events: Option<ReplicationEventApplyBatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationApplyOk {
    pub messages_applied: bool,
    pub events_applied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationCheckpointExport {
    pub topic: String,
    pub group: Option<String>,
    pub partition: Partition,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationStateCheckpoint {
    pub message_epoch: u64,
    pub event_epoch: u64,
    pub message_checkpoint_offset: u64,
    pub message_next_offset: u64,
    pub event_next_offset: u64,
    pub applied_event_offset: u64,
    pub state_snapshot: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationCheckpointExportOk {
    pub checkpoint: ReplicationStateCheckpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationCheckpointInstall {
    pub topic: String,
    pub group: Option<String>,
    pub partition: Partition,
    pub checkpoint: ReplicationStateCheckpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationCheckpointInstallOk {
    pub message_next_offset: u64,
    pub event_next_offset: u64,
    pub applied_event_offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deliver {
    pub sub_id: u64,
    pub topic: String,
    pub group: Option<String>,
    pub partition: Partition,
    pub offset: u64,
    pub delivery_tag: DeliveryTag,
    pub published: u64,
    pub publish_received: u64,
    #[serde(default)]
    pub content_type: Option<ContentType>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
}

/// Server->client push notifying an exclusive consumer-group member that its
/// partition assignment changed. Purely informational: the broker's per-partition
/// delivery gate enforces exclusivity regardless of whether the client acts on
/// this. `generation` increases per cohort rebalance so stale notifications can be
/// fenced. `assigned` is the member's full current set; `added`/`revoked` are the
/// deltas since its previous assignment.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssignmentChanged {
    pub topic: String,
    pub group: Option<String>,
    pub consumer_group: String,
    pub generation: u64,
    pub assigned: Vec<Partition>,
    pub added: Vec<Partition>,
    pub revoked: Vec<Partition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ack {
    pub topic: String,
    pub group: Option<String>,
    pub partition: Partition,
    pub tags: Vec<DeliveryTag>, // batch
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Nack {
    pub topic: String,
    pub group: Option<String>,
    pub partition: Partition,
    pub tags: Vec<DeliveryTag>, // batch
    pub requeue: bool,
    #[serde(default)]
    pub not_before: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Ping;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Pong;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorMsg {
    pub code: u16,
    pub message: String,
}

pub const ERR_CONFLICT: u16 = 409;
// Not-owner is a topology or state conflict, not an auth failure. Retrying
// against the current owner is valid, so 403-style "forbidden" is misleading.
pub const ERR_NOT_OWNER: u16 = ERR_CONFLICT;
/// Malformed request the client should fix rather than retry (e.g. a nil cohort
/// member id).
pub const ERR_INVALID: u16 = 400;

/// Client request for cluster topology. An empty `topic` filter asks for the
/// full topology; a set `topic` (optionally with `group`) narrows it.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TopologyRequest {
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub group: Option<String>,
}

/// One queue partition's ownership, as seen by clients for routing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueTopologyEntry {
    pub topic: String,
    pub partition: Partition,
    pub group: Option<String>,
    /// Broker endpoint of the owner, if the owner node is known in the registry.
    pub owner_endpoint: Option<String>,
    pub partitioning_version: u64,
    /// Total partition count of this queue `(topic, group)` — authoritative N
    /// for key routing. Repeated across the queue's partition entries.
    #[serde(default = "one")]
    pub partition_count: u32,
}

fn one() -> u32 {
    1
}

/// Topology response: ownership of the requested queue partitions at a given
/// coordination generation. Clients route from this and refresh on redirects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologyOk {
    pub generation: u64,
    pub queues: Vec<QueueTopologyEntry>,
}

/// Control-flow response telling the client to retry against the current owner.
/// Distinct from an error: it is not a failure, it carries a routing target,
/// and it must be retried on a DIFFERENT connection, so the client routing
/// layer (not the per-connection engine) acts on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Redirect {
    pub topic: String,
    pub partition: Partition,
    pub group: Option<String>,
    pub owner_endpoint: String,
    pub partitioning_version: u64,
}
