use async_trait::async_trait;
use std::collections::HashMap;

pub use stroma_core::{
    AppendCompletion, AppendResult, CompletionPair, IoError, MessageContentType,
};

// Shared value types live in dependency-light crates: engine-layer concepts
// (Partition, Topic, Group) in `stroma-common`, fibril-layer concepts
// (DeliveryTag) in `fibril-common`. Re-exported here so existing
// `fibril_storage::{Partition, ...}` paths keep working. `Offset` stays a `u64`
// alias for now (its newtype adoption is the phase after Partition).
pub use fibril_common::DeliveryTag;
pub use stroma_common::{Group, Partition, Topic};

pub type Offset = u64;

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub topic: Topic,
    pub group: Option<Group>,
    pub partition: Partition,
    pub offset: Offset,
    pub published: u64,
    pub publish_received: u64,
    pub retried: u32,
    pub content_type: Option<MessageContentType>,
    pub headers: HashMap<String, String>,
    pub payload: Vec<u8>,
}

/// Returned by poll operations: the message plus its metadata
#[derive(Debug, Clone)]
pub struct DeliverableMessage {
    pub message: StoredMessage,
    pub delivery_tag: DeliveryTag, // unique per (topic,partition)
    pub group: Option<Group>,
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("column family not found: {0}")]
    MissingColumnFamily(&'static str),

    #[error("invalid key encoding: {0}")]
    KeyDecode(String),

    #[error("missing message for offset {offset}")]
    MessageNotFound { offset: u64 },

    #[error("unexpected internal error: {0}")]
    Internal(String),

    #[error("anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

#[async_trait]
pub trait AppendReceiptExt<T> {
    async fn wait(self) -> Result<T, impl Into<StorageError>>;
}

pub struct StorageAppendReceipt<T> {
    pub result_rx: tokio::sync::oneshot::Receiver<Result<T, StorageError>>,
}

#[async_trait]
impl AppendReceiptExt<Offset> for StorageAppendReceipt<Offset> {
    async fn wait(self) -> Result<Offset, StorageError> {
        self.result_rx
            .await
            .unwrap_or_else(|_| Err(StorageError::Internal("writer dropped".into())))
    }
}

pub struct BrokerCompletion {
    tx: tokio::sync::oneshot::Sender<Result<AppendResult, IoError>>,
}

impl AppendCompletion<IoError> for BrokerCompletion {
    fn complete(self: Box<Self>, res: Result<AppendResult, IoError>) {
        let _ = self.tx.send(res);
    }
}

#[derive(Debug, Clone)]
pub struct BrokerCompletionPair;

impl CompletionPair<IoError> for BrokerCompletionPair {
    type Receiver = tokio::sync::oneshot::Receiver<Result<AppendResult, IoError>>;

    fn pair() -> (Box<dyn AppendCompletion<IoError> + Send>, Self::Receiver) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Box::new(BrokerCompletion { tx }), rx)
    }
}
