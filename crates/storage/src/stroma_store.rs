use std::{ops::Deref, sync::{Arc, atomic::AtomicU64}};

use async_trait::async_trait;
use dashmap::DashSet;
use fibril_util::unix_millis;

use crate::{
    BrokerCompletionPair, DeliverableMessage, DeliveryTag, Group, LogId, Offset, Storage,
    StorageError, StoredMessage, Topic, UnixMillis,
};
use stroma_core::{
    AppendCompletion, CompletionPair, Durability, IoError, KeratinConfig, SnapshotConfig, Stroma,
    StromaError,
};

#[derive(Debug, Clone)]
pub struct StromaStorage {
    inner: std::sync::Arc<Stroma>,

    pub(crate) msg_count: Arc<AtomicU64>,

    pub(crate) event_count: Arc<AtomicU64>,

    pub(crate) group_reg: Arc<DashSet<(Topic, LogId, Option<Group>)>>,
}

impl StromaStorage {
    pub async fn open(path: &str, sync_write: bool) -> Result<Self, StorageError> {
        let keratin_cfg = KeratinConfig {
            segment_max_bytes: 256 * 1024 * 1024,
            index_stride_bytes: 64 * 1024,
            max_batch_bytes: 12 * 1024 * 1024,
            max_batch_records: 8192,
            batch_linger_ms: 25,
            fsync_interval_ms: 25,
            flush_target_bytes: 48 * 1024 * 1024,
            default_durability: (if sync_write {
                Durability::AfterFsync
            } else {
                Durability::AfterWrite
            })
            .into(),
        };
        let snap_cfg = SnapshotConfig::default();
        let inner = Arc::new(
            Stroma::open(path, keratin_cfg, snap_cfg)
                .await
                .map_err(Self::map_err)?,
        );
        Ok(Self {
            inner,
            msg_count: Arc::new(AtomicU64::new(0)),
            event_count: Arc::new(AtomicU64::new(0)),
            group_reg: Arc::new(DashSet::new()),
        })
    }

    pub fn new(inner: std::sync::Arc<Stroma>) -> Self {
        Self {
            inner,
            msg_count: Arc::new(AtomicU64::new(0)),
            event_count: Arc::new(AtomicU64::new(0)),
            group_reg: Arc::new(DashSet::new()),
        }
    }

    fn map_err(e: StromaError) -> StorageError {
        // Adjust to your StorageError variants.
        StorageError::Internal(e.to_string())
    }

    fn is_enqueued(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        self.inner
            .is_ready(topic, partition, group.as_deref(), offset)
            .map_err(Self::map_err)
    }
}

#[async_trait]
impl Storage for StromaStorage {
    async fn append(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        payload: &[u8],
    ) -> Result<Offset, StorageError> {
        let (completion, rx) = BrokerCompletionPair::pair();
        self.inner
            .append_message(topic, partition, group.as_deref(), payload, completion)
            .await
            .map_err(Self::map_err)?;

        Ok(rx
            .await
            .map_err(|e| StromaError::Io(e.to_string()))
            .map_err(Self::map_err)?
            .map_err(|e| StromaError::Io(e.to_string()))
            .map_err(Self::map_err)?
            .base_offset)
    }

    async fn append_enqueue(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        payload: &[u8],
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StorageError> {
        self.inner
            .append_message(topic, partition, group.as_deref(), payload, completion)
            .await
            .map_err(Self::map_err)?;
        Ok(())
    }

    async fn append_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        payloads: &[Vec<u8>],
    ) -> Result<Vec<Offset>, StorageError> {
        self.inner
            .append_messages_batch(topic, partition, group.as_deref(), payloads)
            .await
            .map_err(Self::map_err)
    }

    async fn register_group(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
    ) -> Result<(), StorageError> {
        // Stroma creates group state lazily on first event/use.
        // Keep this as a no-op for API compatibility.
        self.group_reg
            .insert((topic.clone(), partition, group.clone()));
        Ok(())
    }

    async fn fetch_by_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
    ) -> Result<StoredMessage, StorageError> {
        let Some(record) = self
            .inner
            .fetch_message_by_offset(topic, partition, group.as_deref(), offset)
            .await
            .map_err(Self::map_err)?
        else {
            return Err(StorageError::MessageNotFound { offset });
        };

        Ok(StoredMessage {
            topic: topic.clone(),
            group: group.clone(),
            partition,
            offset,
            timestamp: unix_millis(),
            payload: record.payload,
        })
    }

    async fn fetch_available(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        from_offset: Offset,
        max: usize,
    ) -> Result<Vec<StoredMessage>, StorageError> {
        let mut out = Vec::with_capacity(max);

        // We scan message log from from_offset, then filter by Stroma state.
        // This is “dumb but correct” and can be optimized later by tighter loops.
        let mut cur = from_offset;

        // Avoid infinite loops if there are many inflight/acked holes.
        // We'll keep scanning until we either collect `max` or we hit tail.
        let tail = self
            .inner
            .current_next_offset(topic, partition, group.as_deref())
            .await
            .map_err(Self::map_err)?;

        let mut candidates = Vec::new();

        while out.len() < max && cur < tail {
            let chunk = self
                .inner
                .scan_messages_from(topic, partition, group.as_deref(), cur, max - out.len())
                .await
                .map_err(Self::map_err)?;

            if chunk.is_empty() {
                break;
            }

            candidates.clear();
            candidates.reserve(chunk.len());

            for (off, payload) in chunk {
                cur = off + 1;
                candidates.push((off, payload));
            }

            self.inner.filter_not_enqueued(topic, partition, group.as_deref(), &mut candidates);

            for (off, payload) in candidates.drain(..) {
                out.push(StoredMessage {
                        topic: topic.clone(),
                        partition,
                        group: group.clone(),
                        offset: off,
                        timestamp: unix_millis(),
                        payload,
                    });
                if out.len() >= max { break; }
            }

            // for (off, payload) in chunk {
            //     cur = off + 1;

            //     let ok = self
            //         .inner
            //         .is_enqueued(topic, partition, group.as_deref(), off)
            //         .map_err(Self::map_err)?;

            //     if !ok {
            //         continue;
            //     }

            //     out.push(StoredMessage {
            //         topic: topic.clone(),
            //         group: group.clone(),
            //         partition,
            //         offset: off,
            //         timestamp: unix_millis(),
            //         payload,
            //     });

            //     if out.len() >= max {
            //         break;
            //     }
            // }
        }

        Ok(out)
    }

    async fn current_next_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
    ) -> Result<Offset, StorageError> {
        self.inner
            .current_next_offset(topic, partition, group.as_deref())
            .await
            .map_err(Self::map_err)
    }

    async fn fetch_available_clamped(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        from_offset: Offset,
        max_offset_exclusive: Offset,
        max: usize,
    ) -> Result<Vec<StoredMessage>, StorageError> {
        if from_offset >= max_offset_exclusive {
            return Ok(vec![]);
        }

        let msgs = self
            .fetch_available(topic, partition, &group, from_offset, max)
            .await?;
        Ok(msgs
            .into_iter()
            .take_while(|m| m.offset < max_offset_exclusive)
            .collect())
    }

    async fn mark_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
        deadline_ts: UnixMillis,
    ) -> Result<(), StorageError> {
        self.inner
            .mark_inflight_one(topic, partition, group.as_deref(), offset, deadline_ts)
            .await
            .map_err(Self::map_err)
    }

    async fn mark_inflight_batch(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        entries: &[(Offset, UnixMillis)],
    ) -> Result<(), StorageError> {
        self.inner
            .mark_inflight_batch(topic, partition, group.as_deref(), entries)
            .await
            .map_err(Self::map_err)
    }

    async fn ack_enqueue(
        &self,
        topic: &str,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StorageError> {
        self.inner
            .ack_enqueue(topic, partition, group.as_deref(), offset, completion)
            .await
            .map_err(Self::map_err)?;

        Ok(())
    }

    async fn ack(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
    ) -> Result<(), StorageError> {
        self.inner
            .ack_one(topic, partition, group.as_deref(), offset)
            .await
            .map_err(Self::map_err)
    }

    async fn ack_batch(
        &self,
        topic: &str,
        partition: LogId,
        group: &Option<Group>,
        offsets: &[Offset],
    ) -> Result<(), StorageError> {
        self.inner
            .ack_batch(topic.into(), partition, group.as_deref(), offsets)
            .await
            .map_err(Self::map_err)
    }

    async fn list_expired(&self, now_ts: u64) -> Result<Vec<StoredMessage>, StorageError> {
        // Rocks implementation returned messages too.
        // Here we:
        // 1) ask Stroma which (tp,part,group,off) expired
        // 2) fetch the message payload from Stroma message log
        let expired = self
            .inner
            .list_expired(now_ts, usize::MAX)
            .map_err(Self::map_err)?;

        let mut out = Vec::with_capacity(expired.len());

        for (tp, part, group, off) in expired {
            let Some(record) = self
                .inner
                .fetch_message_by_offset(&tp, part, group.as_deref(), off)
                .await
                .map_err(Self::map_err)?
            else {
                // Message missing: skip (or return error). Rocks deleted stale inflight keys.
                continue;
            };

            out.push(StoredMessage {
                topic: tp.clone(),
                // TODO: Fix
                group: group.clone(),
                partition: part,
                offset: off,
                timestamp: unix_millis(),
                payload: record.payload,
            });
        }

        Ok(out)
    }

    async fn lowest_unacked_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
    ) -> Result<Offset, StorageError> {
        self.inner
            .lowest_unacked_offset(topic, partition, group.as_deref())
            .map_err(Self::map_err)
    }

    async fn lowest_not_acked_offset(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
    ) -> Result<Offset, StorageError> {
        // In your stroma state model, "lowest_unacked" is the same as "lowest_not_acked".
        self.lowest_unacked_offset(topic, partition, group).await
    }

    async fn cleanup_topic(&self, topic: &Topic, partition: LogId, 
        group: &Option<Group>) -> Result<(), StorageError> {
        self.inner
            .cleanup_topic_partition(topic, partition, group.as_deref())
            .await
            .map_err(Self::map_err)
    }

    async fn clear_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
    ) -> Result<(), StorageError> {
        self.inner
            .clear_inflight(topic, partition, group.as_deref(), offset)
            .await
            .map_err(Self::map_err)
    }

    async fn requeue(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
    ) -> Result<(), StorageError> {
        self.inner
            .requeue(topic, partition, group.as_deref(), offset)
            .await
            .map_err(Self::map_err)
    }

    async fn add_to_redelivery(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
        deadline: UnixMillis,
    ) -> Result<(), StorageError> {
        self.inner
            .add_to_redelivery(topic, partition, group.as_deref(), offset)
            .await
            .map_err(Self::map_err)
    }

    async fn clear_all_inflight(&self) -> Result<(), StorageError> {
        // Stroma currently doesn’t have a “clear all inflight” event.
        // TODO: implement as:
        // - enumerate keys in groups map
        // - emit ResetGroup / Snapshot events or add a ClearAllInflight event
        todo!()
        // Err(StorageError::Unsupported("clear_all_inflight not implemented for Stroma yet".into()))
    }

    async fn count_inflight(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
    ) -> Result<usize, StorageError> {
        self.inner
            .count_inflight(topic, partition, group.as_deref())
            .map_err(Self::map_err)
    }

    async fn dump_meta_keys(&self) {
        // no-op; rocks-specific
    }

    async fn is_inflight_or_acked(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        self.inner
            .is_inflight_or_acked(topic, partition, group.as_deref(), offset)
            .map_err(Self::map_err)
    }
    async fn is_enqueued(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        self.inner
            .is_ready(topic, partition, group.as_deref(), offset)
            .map_err(Self::map_err)
    }

    async fn is_acked(
        &self,
        topic: &Topic,
        partition: LogId,
        group: &Option<Group>,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        self.inner
            .is_acked(topic, partition, group.as_deref(), offset)
            .map_err(Self::map_err)
    }

    async fn list_topics(&self) -> Result<Vec<Topic>, StorageError> {
        Ok(self
            .inner
            .list_topics()
            .into_iter()
            .map(|s| s.into())
            .collect())
    }

    async fn list_groups(&self) -> Result<Vec<(Topic, LogId, Option<Group>)>, StorageError> {
        Ok(self
            .group_reg
            .iter()
            .map(|key| {
                let (topic, part, group) = key.key();
                (topic.clone(), *part, group.clone())
            })
            .collect::<Vec<_>>())
    }

    async fn flush(&self) -> Result<(), StorageError> {
        // Keratin fsync happens on each append because Stroma uses AfterFsync.
        Ok(())
    }

    async fn next_expiry_hint(&self) -> Result<Option<u64>, StorageError> {
        self.inner.next_expiry_hint().map_err(Self::map_err)
    }

    async fn recompute_and_store_next_expiry_hint(&self) -> Result<Option<u64>, StorageError> {
        // Stroma hint is derived from GroupState; we can recompute by querying it.
        self.next_expiry_hint().await
    }

    async fn estimate_disk_used(&self) -> Result<u64, StorageError> {
        let mut total = 0;
        for d in ["events", "snapshots", "messages"] {
            let root = self.inner.root().join(d);
            if root.exists() {
                for e in walkdir::WalkDir::new(root) {
                    let e = e.map_err(|e| StorageError::Internal(e.to_string()))?;
                    if let Ok(m) = e.metadata() {
                        total += m.len();
                    }
                }
            }
        }
        Ok(total)
    }
}
