pub mod client;
pub mod frame;
pub mod handler;
pub mod helper;

use std::collections::HashMap;

use fibril_storage::DeliveryTag;
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
    pub partition: u32, // keep for later, default 0
    pub group: Option<String>,
    pub require_confirm: bool,
    #[serde(default)]
    pub content_type: Option<ContentType>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
    pub published: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublishDelayed {
    pub topic: String,
    pub partition: u32,
    pub group: Option<String>,
    pub require_confirm: bool,
    pub not_before: u64,
    #[serde(default)]
    pub content_type: Option<ContentType>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
    pub published: u64,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclareQueueOk {
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscribe {
    pub topic: String,
    pub group: Option<String>,
    pub prefetch: u32,
    pub auto_ack: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeOk {
    pub sub_id: u64,
    pub topic: String,
    pub group: Option<String>,
    pub partition: u32,
    pub prefetch: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReconcileSubscription {
    pub sub_id: u64,
    pub topic: String,
    pub group: Option<String>,
    pub partition: u32,
    pub auto_ack: bool,
    #[serde(default = "default_prefetch")]
    pub prefetch: u32,
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
    pub partition: u32,
    pub message_from: u64,
    pub event_from: u64,
    pub max_messages: u32,
    pub max_events: u32,
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
    pub partition: u32,
    pub messages: Option<ReplicationMessageApplyBatch>,
    pub events: Option<ReplicationEventApplyBatch>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReplicationApplyOk {
    pub messages_applied: bool,
    pub events_applied: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deliver {
    pub sub_id: u64,
    pub topic: String,
    pub group: Option<String>,
    pub partition: u32,
    pub offset: u64,
    pub delivery_tag: DeliveryTag,
    pub published: u64,
    pub publish_received: u64,
    #[serde(default)]
    pub content_type: Option<ContentType>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ack {
    pub topic: String,
    pub group: Option<String>,
    pub partition: u32,
    pub tags: Vec<DeliveryTag>, // batch
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Nack {
    pub topic: String,
    pub group: Option<String>,
    pub partition: u32,
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
