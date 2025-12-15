use crate::storage::*;
use async_trait::async_trait;
use rocksdb::{
    ColumnFamilyDescriptor, DBWithThreadMode, IteratorMode, MultiThreaded, Options, WriteBatch,
};
use std::{sync::Arc, time::SystemTime};

#[derive(Debug, Clone)]
pub struct RocksStorage {
    db: Arc<DBWithThreadMode<MultiThreaded>>,
}

impl RocksStorage {
    pub fn open(path: &str) -> Result<Self, StorageError> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let cfs = vec![
            ColumnFamilyDescriptor::new("messages", Options::default()),
            ColumnFamilyDescriptor::new("inflight", Options::default()),
            ColumnFamilyDescriptor::new("acked", Options::default()),
            ColumnFamilyDescriptor::new("meta", Options::default()),
            ColumnFamilyDescriptor::new("groups", Options::default()),
        ];

        let db = DBWithThreadMode::open_cf_descriptors(&opts, path, cfs)?;
        let storage = Self { db: Arc::new(db) };

        Ok(storage)
    }

    fn next_offset_key(topic: &Topic, partition: Partition) -> String {
        format!("NEXT_OFFSET:{}:{}", topic, partition)
    }

    fn encode_msg_key(topic: &Topic, partition: Partition, offset: Offset) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(topic.as_bytes());
        v.push(0);
        v.extend_from_slice(&partition.to_be_bytes());
        v.extend_from_slice(&offset.to_be_bytes());
        v
    }

    fn encode_group_key(
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offset: Offset,
    ) -> Vec<u8> {
        let mut v = Vec::new();
        v.extend_from_slice(topic.as_bytes());
        v.push(0);
        v.extend_from_slice(&partition.to_be_bytes());
        v.extend_from_slice(group.as_bytes());
        v.push(0);
        v.extend_from_slice(&offset.to_be_bytes());
        v
    }
}

#[async_trait]
impl Storage for RocksStorage {
    async fn append(
        &self,
        topic: &Topic,
        partition: Partition,
        payload: &[u8],
    ) -> Result<Offset, StorageError> {
        let meta_cf = self
            .db
            .cf_handle("meta")
            .ok_or(StorageError::MissingColumnFamily("meta"))?;
        let messages_cf = self
            .db
            .cf_handle("messages")
            .ok_or(StorageError::MissingColumnFamily("messages"))?;

        // Read next offset, default 0
        let key = Self::next_offset_key(topic, partition);
        let next = self.db.get_cf(&meta_cf, key.as_bytes())?;
        let offset = match &next {
            Some(v) => u64::from_be_bytes(v.as_slice().try_into().map_err(|_| {
                StorageError::KeyDecode(format!("invalid next offset length: {}", v.len()))
            })?),
            None => 0,
        };

        let msg_key = Self::encode_msg_key(topic, partition, offset);

        let mut batch = WriteBatch::default();
        batch.put_cf(&messages_cf, msg_key, payload);
        batch.put_cf(&meta_cf, key.as_bytes(), (offset + 1).to_be_bytes());
        self.db.write(batch)?;

        Ok(offset)
    }

    async fn append_batch(
        &self,
        topic: &Topic,
        partition: Partition,
        payloads: &[Vec<u8>],
    ) -> Result<Vec<Offset>, StorageError> {
        if payloads.is_empty() {
            return Ok(Vec::new());
        }

        let meta_cf = self.db.cf_handle("meta").unwrap();
        let messages_cf = self.db.cf_handle("messages").unwrap();

        // Read next offset
        let next_key = Self::next_offset_key(topic, partition);
        let next = self.db.get_cf(&meta_cf, next_key.as_bytes())?;

        let mut offset = match next {
            Some(v) => u64::from_be_bytes(v.try_into().unwrap()),
            None => 0,
        };

        let start_offset = offset;
        let mut batch = WriteBatch::default();
        let mut out = Vec::with_capacity(payloads.len());

        for payload in payloads {
            let msg_key = Self::encode_msg_key(topic, partition, offset);
            batch.put_cf(&messages_cf, msg_key, payload);
            out.push(offset);
            offset += 1;
        }

        // Update next_offset
        batch.put_cf(&meta_cf, next_key.as_bytes(), offset.to_be_bytes());

        self.db.write(batch)?;

        Ok(out)
    }

    async fn register_group(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
    ) -> Result<(), StorageError> {
        let groups_cf = self.db.cf_handle("groups").unwrap();

        let mut key = Vec::new();
        key.extend_from_slice(topic.as_bytes());
        key.push(0);
        key.extend_from_slice(&partition.to_be_bytes());
        key.extend_from_slice(group.as_bytes());

        self.db.put_cf(&groups_cf, key, [])?;
        Ok(())
    }

    async fn fetch_by_offset(
        &self,
        topic: &Topic,
        partition: Partition,
        offset: Offset,
    ) -> Result<StoredMessage, StorageError> {
        let messages_cf = self
            .db
            .cf_handle("messages")
            .ok_or(StorageError::MissingColumnFamily("messages"))?;

        let msg_key = RocksStorage::encode_msg_key(topic, partition, offset);

        let val = self
            .db
            .get_cf(&messages_cf, msg_key)?
            .ok_or(StorageError::MessageNotFound { offset })?;

        Ok(StoredMessage {
            topic: topic.clone(),
            partition,
            offset,
            timestamp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            payload: val.to_vec(),
        })
    }

    async fn fetch_available(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        from_offset: Offset,
        max: usize,
    ) -> Result<Vec<DeliverableMessage>, StorageError> {
        let messages_cf = self
            .db
            .cf_handle("messages")
            .ok_or(StorageError::MissingColumnFamily("messages"))?;
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let start_key = Self::encode_msg_key(topic, partition, from_offset);

        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        let mut iter = self.db.iterator_cf(
            &messages_cf,
            IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        let mut out = Vec::new();

        for pair in iter.by_ref() {
            let (key, value) = pair?;
            if !key.starts_with(&prefix) {
                break;
            }

            if out.len() >= max {
                break;
            }

            let off = u64::from_be_bytes(key[key.len() - 8..].try_into().map_err(|e| {
                StorageError::KeyDecode(format!("invalid message key length: {}", e))
            })?);

            // Skip if inflight
            let inflight_key = Self::encode_group_key(topic, partition, group, off);
            if self
                .db
                .get_cf(&inflight_cf, inflight_key.clone())?
                .is_some()
            {
                continue;
            }

            // Skip if ACKed
            let acked_key = Self::encode_group_key(topic, partition, group, off);
            if self.db.get_cf(&acked_cf, acked_key.clone())?.is_some() {
                continue;
            }

            debug_assert!(
                self.db.get_cf(&acked_cf, &acked_key)?.is_none(),
                "fetch_available returning ACKed message {}",
                off
            );

            debug_assert!(
                self.db.get_cf(&inflight_cf, &inflight_key)?.is_none(),
                "fetch_available returned inflight message {}",
                off
            );

            out.push(DeliverableMessage {
                message: StoredMessage {
                    topic: topic.clone(),
                    partition,
                    offset: off,
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    payload: value.to_vec(),
                },
                delivery_tag: off,
                group: group.clone(),
            });
        }

        Ok(out)
    }

    async fn current_next_offset(
        &self,
        topic: &Topic,
        partition: Partition,
    ) -> Result<Offset, StorageError> {
        let meta_cf = self
            .db
            .cf_handle("meta")
            .ok_or(StorageError::MissingColumnFamily("meta"))?;

        let key = Self::next_offset_key(topic, partition);

        match self.db.get_cf(&meta_cf, key.as_bytes())? {
            Some(v) => {
                let o = u64::from_be_bytes(v.as_slice().try_into().map_err(|_| {
                    StorageError::KeyDecode(format!("invalid next_offset for {}", key))
                })?);
                Ok(o)
            }
            None => Ok(0),
        }
    }

    async fn fetch_available_clamped(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        from_offset: Offset,
        max_offset_exclusive: Offset,
        max: usize,
    ) -> Result<Vec<DeliverableMessage>, StorageError> {
        if from_offset >= max_offset_exclusive {
            return Ok(vec![]);
        }

        let msgs = self
            .fetch_available(topic, partition, group, from_offset, max)
            .await?;

        // clamp based on next-offset snapshot
        let clamped = msgs
            .into_iter()
            .take_while(|m| m.delivery_tag < max_offset_exclusive)
            .collect();

        Ok(clamped)
    }

    async fn fetch_range(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        from_offset: Offset,
        to_offset: Offset,
        max: usize,
    ) -> Result<Vec<DeliverableMessage>, StorageError> {
        let messages_cf = self
            .db
            .cf_handle("messages")
            .ok_or(StorageError::MissingColumnFamily("messages"))?;
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let start_key = Self::encode_msg_key(topic, partition, from_offset);

        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        let mut iter = self.db.iterator_cf(
            &messages_cf,
            IteratorMode::From(&start_key, rocksdb::Direction::Forward),
        );

        let mut out = Vec::new();

        for pair in iter.by_ref() {
            let (key, value) = pair?;
            if !key.starts_with(&prefix) {
                break;
            }

            if out.len() >= max {
                break;
            }

            let off = u64::from_be_bytes(key[key.len() - 8..].try_into().map_err(|e| {
                StorageError::KeyDecode(format!("invalid message key length: {}", e))
            })?);

            if off >= to_offset {
                break;
            }

            // Skip if inflight
            let inflight_key = Self::encode_group_key(topic, partition, group, off);
            if self
                .db
                .get_cf(&inflight_cf, inflight_key.clone())?
                .is_some()
            {
                continue;
            }

            // Skip if ACKed
            let acked_key = Self::encode_group_key(topic, partition, group, off);
            if self.db.get_cf(&acked_cf, acked_key.clone())?.is_some() {
                continue;
            }

            out.push(DeliverableMessage {
                message: StoredMessage {
                    topic: topic.clone(),
                    partition,
                    offset: off,
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    payload: value.to_vec(),
                },
                delivery_tag: off,
                group: group.clone(),
            });
        }

        Ok(out)
    }

    async fn mark_inflight(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offset: Offset,
        deadline_ts: u64,
    ) -> Result<(), StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let key = Self::encode_group_key(topic, partition, group, offset);

        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;
        debug_assert!(
            self.db.get_cf(&acked_cf, &key)?.is_none(),
            "mark_inflight on ACKed message {}",
            offset
        );

        self.db
            .put_cf(&inflight_cf, key, deadline_ts.to_be_bytes())?;
        Ok(())
    }

    async fn mark_inflight_batch(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        entries: &[(Offset, u64)],
    ) -> Result<(), StorageError> {
        if entries.is_empty() {
            return Ok(());
        }

        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;

        let mut batch = WriteBatch::default();

        for (offset, deadline) in entries {
            let key = Self::encode_group_key(topic, partition, group, *offset);
            batch.put_cf(&inflight_cf, key, deadline.to_be_bytes());
        }

        self.db.write(batch)?;
        Ok(())
    }

    async fn ack(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let key = Self::encode_group_key(topic, partition, group, offset);

        debug_assert!(
            self.db.get_cf(&acked_cf, &key)?.is_none(),
            "double ACK for offset {}",
            offset
        );

        debug_assert!(
            self.db.get_cf(&inflight_cf, &key)?.is_some(),
            "ACK on non-inflight message {}",
            offset
        );

        let key = Self::encode_group_key(topic, partition, group, offset);

        let mut batch = WriteBatch::default();
        batch.delete_cf(&inflight_cf, &key);
        batch.put_cf(&acked_cf, &key, []);
        self.db.write(batch)?;

        Ok(())
    }

    async fn ack_batch(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offsets: &[Offset],
    ) -> Result<(), StorageError> {
        if offsets.is_empty() {
            return Ok(());
        }

        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let mut batch = WriteBatch::default();

        // (Optional) If you ever send duplicates in the same batch, you can de-dupe.
        // For perf we can skip this.
        // let mut v = offsets.to_vec();
        // v.sort_unstable();
        // v.dedup();
        // for off in v { ... }

        for &offset in offsets {
            let key = Self::encode_group_key(topic, partition, group, offset);

            // Idempotent:
            // - delete inflight even if missing
            // - put acked even if already exists
            batch.delete_cf(&inflight_cf, &key);
            batch.put_cf(&acked_cf, &key, []);
        }

        self.db.write(batch)?;
        Ok(())
    }

    async fn list_expired(&self, now_ts: u64) -> Result<Vec<DeliverableMessage>, StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let messages_cf = self
            .db
            .cf_handle("messages")
            .ok_or(StorageError::MissingColumnFamily("messages"))?;

        let mut out = Vec::new();

        let iter = self
            .db
            .iterator_cf(&inflight_cf, rocksdb::IteratorMode::Start);
        for pair in iter {
            let (key, value) = pair?;
            let deadline = u64::from_be_bytes(boxed_slice_to_array(value)?);
            if deadline > now_ts {
                continue;
            }

            // Extract topic, partition, offset by decoding key
            let (topic, partition, group, offset) = decode_inflight_key(&key)?;

            let msg_key = RocksStorage::encode_msg_key(&topic, partition, offset);

            // Fetch the message
            let Some(msg_val) = self.db.get_cf(&messages_cf, msg_key)? else {
                // stale inflight entry pointing to a deleted/missing message
                // don't poison the whole scan
                eprintln!(
                    "[WARN] stale inflight entry: topic={} partition={} group={} offset={} (message missing)",
                    topic, partition, group, offset
                );
                let inflight_cf = self.db.cf_handle("inflight").unwrap();
                self.db.delete_cf(&inflight_cf, &key)?;
                continue;
            };

            out.push(DeliverableMessage {
                message: StoredMessage {
                    topic: topic.clone(),
                    partition,
                    offset,
                    timestamp: SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                    payload: msg_val.to_vec(),
                },
                delivery_tag: offset,
                group,
            });
        }

        Ok(out)
    }

    async fn lowest_unacked_offset(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
    ) -> Result<Offset, StorageError> {
        let messages_cf = self.db.cf_handle("messages").unwrap();
        let acked_cf = self.db.cf_handle("acked").unwrap();
        let inflight_cf = self.db.cf_handle("inflight").unwrap();

        // prefix: topic + 0x00 + partition bytes
        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        let iter = self.db.iterator_cf(
            &messages_cf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        for pair in iter {
            let (key, _) = pair?;

            if !key.starts_with(&prefix) {
                break;
            }

            let offset = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());

            let inflight_key = Self::encode_group_key(topic, partition, group, offset);
            if self.db.get_cf(&inflight_cf, &inflight_key)?.is_some() {
                continue;
            }

            let ack_key = Self::encode_group_key(topic, partition, group, offset);
            if self.db.get_cf(&acked_cf, &ack_key)?.is_some() {
                continue;
            }

            return Ok(offset);
        }

        // No unacked messages remain → everything is acked
        // Need to compute next_offset from meta CF
        let meta_cf = self.db.cf_handle("meta").unwrap();
        let next_key = Self::next_offset_key(topic, partition);

        let next = self.db.get_cf(&meta_cf, next_key.as_bytes())?;
        let next_offset = match next {
            Some(v) => u64::from_be_bytes(v.try_into().unwrap()),
            None => 0,
        };

        Ok(next_offset)
    }

    async fn cleanup_topic(&self, topic: &Topic, partition: Partition) -> Result<(), StorageError> {
        let messages_cf = self.db.cf_handle("messages").unwrap();

        // Find all groups for this topic/partition
        let groups_cf = self.db.cf_handle("groups").unwrap();
        let mut groups = Vec::new();

        // Build prefix for this topic + partition
        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        // Iterate groups CF looking for matching entries
        let iter = self.db.iterator_cf(
            &groups_cf,
            IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        for pair in iter {
            let (key, _) = pair?;

            if !key.starts_with(&prefix) {
                break;
            }

            // group name starts after prefix
            let group_bytes = &key[prefix.len()..];
            let g = String::from_utf8(group_bytes.to_vec())
                .map_err(|_| StorageError::Internal("invalid group".into()))?;

            groups.push(g);
        }

        // If no groups exist → no consumer ever → nothing can be deleted
        if groups.is_empty() {
            return Ok(());
        }

        // Compute min unacked offset across all groups
        let mut min_unacked = u64::MAX;

        for g in groups {
            let unacked = self.lowest_not_acked_offset(topic, partition, &g).await?;
            if unacked < min_unacked {
                min_unacked = unacked;
            }
        }

        if min_unacked == 0 {
            // nothing deletable
            return Ok(());
        }

        // Range delete messages < min_unacked
        let start_key = Self::encode_msg_key(topic, partition, 0);
        let end_key = Self::encode_msg_key(topic, partition, min_unacked);

        self.db.delete_range_cf(&messages_cf, start_key, end_key)?;

        Ok(())
    }

    async fn clear_inflight(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offset: Offset,
    ) -> Result<(), StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let key = Self::encode_group_key(topic, partition, group, offset);

        self.db.delete_cf(&inflight_cf, key)?;
        Ok(())
    }

    async fn clear_all_inflight(&self) -> Result<(), StorageError> {
        // Drop and re-create CF
        self.db.drop_cf("inflight")?;
        self.db.create_cf("inflight", &Options::default())?;
        Ok(())
    }

    async fn count_inflight(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
    ) -> Result<usize, StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;

        // Build prefix = topic + 0 + partition + group + 0
        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());
        prefix.extend_from_slice(group.as_bytes());
        prefix.push(0);

        let iter = self.db.iterator_cf(
            &inflight_cf,
            IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        let mut count = 0usize;

        for pair in iter {
            let (key, _) = pair?;
            if !key.starts_with(&prefix) {
                break;
            }
            count += 1;
        }

        Ok(count)
    }

    async fn dump_meta_keys(&self) {
        let meta_cf = self.db.cf_handle("meta").unwrap(); // however you expose this
        let iter = self.db.iterator_cf(&meta_cf, IteratorMode::Start);
        for pair in iter {
            let (key, _) = pair.unwrap();
            eprintln!("META KEY = {:?}", String::from_utf8_lossy(&key));
        }
    }

    async fn is_inflight_or_acked(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        let inflight_cf = self
            .db
            .cf_handle("inflight")
            .ok_or(StorageError::MissingColumnFamily("inflight"))?;
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let key = Self::encode_group_key(topic, partition, group, offset);

        if self.db.get_cf(&acked_cf, &key)?.is_some() {
            return Ok(true);
        }

        if self.db.get_cf(&inflight_cf, &key)?.is_some() {
            return Ok(true);
        }

        Ok(false)
    }

    async fn is_acked(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
        offset: Offset,
    ) -> Result<bool, StorageError> {
        let acked_cf = self
            .db
            .cf_handle("acked")
            .ok_or(StorageError::MissingColumnFamily("acked"))?;

        let key = Self::encode_group_key(topic, partition, group, offset);

        if self.db.get_cf(&acked_cf, &key)?.is_some() {
            return Ok(true);
        }

        Ok(false)
    }

    async fn list_topics(&self) -> Result<Vec<Topic>, StorageError> {
        let groups_cf = self
            .db
            .cf_handle("groups")
            .ok_or(StorageError::MissingColumnFamily("groups"))?;

        let iter = self.db.iterator_cf(&groups_cf, IteratorMode::Start);

        let mut topics = std::collections::HashSet::new();

        for pair in iter {
            let (key, _) = pair?;

            // key format: topic \0 partition(4) group
            let mut i = 0;
            while i < key.len() && key[i] != 0 {
                i += 1;
            }

            if i == 0 || i >= key.len() {
                continue;
            }

            let topic = String::from_utf8(key[..i].to_vec())
                .map_err(|_| StorageError::KeyDecode("invalid topic".into()))?;

            topics.insert(topic);
        }

        Ok(topics.into_iter().collect())
    }

    async fn list_groups(&self) -> Result<Vec<(Topic, Partition, Group)>, StorageError> {
        let groups_cf = self
            .db
            .cf_handle("groups")
            .ok_or(StorageError::MissingColumnFamily("groups"))?;

        let iter = self.db.iterator_cf(&groups_cf, IteratorMode::Start);

        let mut out = Vec::new();

        for pair in iter {
            let (key, _) = pair?;

            // find topic terminator
            let Some(zero) = key.iter().position(|&b| b == 0) else {
                continue;
            };

            if zero + 1 + 4 > key.len() {
                continue;
            }

            let topic = String::from_utf8(key[..zero].to_vec())
                .map_err(|_| StorageError::KeyDecode("invalid topic".into()))?;

            let partition = u32::from_be_bytes(key[zero + 1..zero + 5].try_into().unwrap());

            let group = String::from_utf8(key[zero + 5..].to_vec())
                .map_err(|_| StorageError::KeyDecode("invalid group".into()))?;

            out.push((topic, partition, group));
        }

        Ok(out)
    }

    async fn lowest_not_acked_offset(
        &self,
        topic: &Topic,
        partition: Partition,
        group: &Group,
    ) -> Result<Offset, StorageError> {
        let messages_cf = self.db.cf_handle("messages").unwrap();
        let acked_cf = self.db.cf_handle("acked").unwrap();

        let mut prefix = Vec::new();
        prefix.extend_from_slice(topic.as_bytes());
        prefix.push(0);
        prefix.extend_from_slice(&partition.to_be_bytes());

        let iter = self.db.iterator_cf(
            &messages_cf,
            rocksdb::IteratorMode::From(&prefix, rocksdb::Direction::Forward),
        );

        for pair in iter {
            let (key, _) = pair?;
            if !key.starts_with(&prefix) {
                break;
            }

            let offset = u64::from_be_bytes(key[key.len() - 8..].try_into().unwrap());
            let ack_key = Self::encode_group_key(topic, partition, group, offset);

            if self.db.get_cf(&acked_cf, &ack_key)?.is_some() {
                continue;
            }

            // first offset that is NOT acked (could be inflight or never delivered)
            return Ok(offset);
        }

        // everything acked => return next_offset
        let meta_cf = self.db.cf_handle("meta").unwrap();
        let next_key = Self::next_offset_key(topic, partition);
        let next = self.db.get_cf(&meta_cf, next_key.as_bytes())?;
        Ok(match next {
            Some(v) => u64::from_be_bytes(v.try_into().unwrap()),
            None => 0,
        })
    }

    async fn flush(&self) -> Result<(), StorageError> {
        self.db.flush()?;
        Ok(())
    }

    // TODO: Better timestamp support
    // Stored messages currently use timestamp: 0.
    // Eventually we want:
    // broker receipt time
    // producer timestamp (optional)
    // Stub: keep as zero or SystemTime::now()

    // TODO: . Message metadata
    // Even just:
    // content_type
    // headers
    // priority
    // ttl

    // TODO: Delete inflight entries on redelivery

    // Storage correctly identifies expired inflight messages but does NOT delete them.
    // Usually the broker layer does that, not the storage layer.
    // Meaning:
    // - list_expired() should not delete
    // - Broker should delete inflight entries and then re-queue for delivery
    // This is correct.

    // TODO: Consistent snapshots or checkpointing (for future clustering)
    // For replication we will eventually need:
    // durable metadata for consumer groups
    // durable write-ahead-log boundaries (segment numbers)
    // support for Raft log entry application
    // Stub: no action needed now, but design broker around explicit "state applies".
}

fn decode_inflight_key(key: &[u8]) -> Result<(Topic, Partition, Group, Offset), anyhow::Error> {
    // topic: up to first 0
    let mut i = 0;
    while i < key.len() && key[i] != 0 {
        i += 1;
    }
    let topic = String::from_utf8(key[..i].to_vec())?;
    i += 1; // skip delimiter

    // partition: 4 bytes
    let partition = Partition::from_be_bytes(key[i..i + 4].try_into()?);
    i += 4;

    // group: up to next 0
    let mut j = i;
    while j < key.len() && key[j] != 0 {
        j += 1;
    }
    let group = String::from_utf8(key[i..j].to_vec())?;
    j += 1;

    // offset: last 8 bytes
    let offset = Offset::from_be_bytes(key[j..j + 8].try_into()?);

    Ok((topic, partition, group, offset))
}

#[derive(Debug)]
pub struct BoxedSliceToArrayError {
    expected: usize,
    found: usize,
}

impl std::fmt::Display for BoxedSliceToArrayError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "expected slice of length {}, got {}",
            self.expected, self.found
        )
    }
}

impl std::error::Error for BoxedSliceToArrayError {}

impl From<BoxedSliceToArrayError> for StorageError {
    fn from(e: BoxedSliceToArrayError) -> Self {
        StorageError::KeyDecode(e.to_string())
    }
}

fn boxed_slice_to_array<const N: usize>(b: Box<[u8]>) -> Result<[u8; N], BoxedSliceToArrayError> {
    if b.len() != N {
        return Err(BoxedSliceToArrayError {
            expected: N,
            found: b.len(),
        });
    }

    let mut arr = [0u8; N];
    arr.copy_from_slice(&b); // length checked above, so no panic
    Ok(arr)
}
