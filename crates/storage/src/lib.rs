use async_trait::async_trait;
use serde::{Deserialize, Serialize};

pub use stroma_core::{AppendCompletion, AppendResult, CompletionPair, IoError};

pub type Topic = String;
pub type LogId = u32;
pub type Offset = u64;
pub type Group = String;

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub topic: Topic,
    pub group: Option<Group>,
    pub partition: LogId,
    pub offset: Offset,
    pub timestamp: u64,
    pub retried: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DeliveryTag {
    pub epoch: u64,
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

    fn pair() -> (Box<dyn AppendCompletion<IoError>>, Self::Receiver) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        (Box::new(BrokerCompletion { tx }), rx)
    }
}
