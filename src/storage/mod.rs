pub mod rocksdb_store;

use async_trait::async_trait;

pub type Topic = String;
pub type Partition = u32;
pub type Offset = u64;
pub type Group = String;

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub topic: Topic,
    pub partition: Partition,
    pub offset: Offset,
    pub timestamp: u64,
    pub payload: Vec<u8>,
}

/// Returned by poll operations: the message plus its metadata
#[derive(Debug, Clone)]
pub struct DeliverableMessage {
    pub message: StoredMessage,
    pub delivery_tag: Offset, // unique per (topic,partition)
    pub group: Group,
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("column family not found: {0}")]
    MissingColumnFamily(&'static str),

    #[error("rocksdb error: {0}")]
    RocksDb(#[from] rocksdb::Error),

    #[error("invalid key encoding: {0}")]
    KeyDecode(String),

    #[error("missing message for offset {offset}")]
    MessageNotFound { offset: u64 },

    #[error("unexpected internal error: {0}")]
    Internal(String),

    #[error("anyhow error: {0}")]
    Anyhow(#[from] anyhow::Error),
}

pub fn make_rocksdb_store(path: &str) -> Result<rocksdb_store::RocksStorage, StorageError> {
    rocksdb_store::RocksStorage::open(path)
}

/// Defines the persistent storage API for a durable queue system.
#[async_trait]
pub trait Storage: Send + Sync {
    /// Append a message to the end of a topic/partition log.
    async fn append(
        &self,
        topic: &Topic,
        partition: Partition,
        payload: &[u8],
    ) -> Result<Offset, StorageError>;

    /// Append a batch of messages to the end of a topic/partition log.
    async fn append_batch(
        &self,
        topic: &Topic,
        partition: Partition,
        payloads: &[Vec<u8>],
    ) -> Result<Vec<Offset>, StorageError>;

    /// Register a consumer group for a topic/partition.
    async fn register_group(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
    ) -> Result<(), StorageError>;

    /// Fetch messages starting *after* a given offset,
    /// limited to max count, excluding messages currently in-flight.
    async fn fetch_available(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        from_offset: Offset,
        max: usize,
    ) -> Result<Vec<DeliverableMessage>, StorageError>;

    /// Mark a message as "in-flight" for a consumer group with a deadline.
    async fn mark_inflight(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offset: Offset,
        deadline_ts: u64,
    ) -> Result<(), StorageError>;

    /// Remove message from inflight and mark as acknowledged.
    async fn ack(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError>;

    /// Return messages whose deadline expired → need redelivery.
    async fn list_expired(&self, now_ts: u64) -> Result<Vec<DeliverableMessage>, StorageError>;

    /// Get the lowest unacknowledged offset for a consumer group.
    async fn lowest_unacked_offset(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
    ) -> Result<Offset, StorageError>;

    /// Cleanup fully acknowledged messages safely.
    async fn cleanup_topic(&self, topic: &Topic, partition: Partition) -> Result<(), StorageError>;

    async fn clear_inflight(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError>;

    // TODO: Batch append support (Performance)
    // We don't need this now, but all high-performance brokers rely on batching.
    // async fn append_batch(&self, topic, partition, &[payloads]) -> Result<Vec<Offset>>
}
