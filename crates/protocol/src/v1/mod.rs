pub mod client;
pub mod frame;
pub mod handler;
pub mod helper;

use std::collections::HashMap;

use fibril_storage::DeliveryTag;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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
