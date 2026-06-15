use std::{collections::HashMap, path::Path, sync::Arc};

use async_trait::async_trait;
use fibril_metrics::QueuesStateSnapshot;
use fibril_storage::Offset;
use fibril_util::{UnixMillis, unix_millis};
use std::collections::HashSet;
use stroma_core::{
    AckEventMeta, CompletionPair, GlobalStore, NackEventMeta, PublishItem, StromaDebugSnapshot,
    StromaMetrics,
};
pub use stroma_core::{
    AppendCompletion, DLQDiscardPolicyWire, DeclareMeta, EvictOutcome,
    FollowerStateCheckpointInstall, FollowerStateCheckpointInstallOutcome, GlobalDLQ,
    GlobalDlqSnapshot, GlobalDlqUpdateOutcome, InspectMode, IoError, KDurability,
    KeratinAppendCompletion, KeratinConfig, Message, MessageContentType, MessageHeaders,
    MessageInspectionPage, MessageInspectionStatus, OwnerReplicationBatch, OwnerReplicationRead,
    OwnerStateCheckpoint, QueueInspectionState, QueuePromotionOutcome, ReplicatedAppendOutcome,
    ReplicatedEventBatch, ReplicatedMessageBatch, ReplicatedQueueApplyOutcome, SnapshotConfig,
    Stroma, StromaError, StromaEvent, StromaKeratinConfig,
};
use tokio::sync::Notify;

pub struct Deliverable {
    pub published: u64,
    pub publish_received: u64,
    pub content_type: Option<MessageContentType>,
    pub extra_headers: HashMap<String, String>,
    pub retries: u32,
    pub offset: Offset,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub enum SettleKind {
    Ack,
    Nack {
        requeue: bool,
        not_before: Option<UnixMillis>,
    },
}

pub struct SettleRequest {
    pub offset: Offset,
    pub kind: SettleKind,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayDeadLettersReport {
    pub requested: usize,
    pub replayed: usize,
    pub items: Vec<ReplayDeadLetterItem>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReplayDeadLetterItem {
    pub offset: Offset,
    pub outcome: ReplayDeadLetterOutcome,
    pub target_topic: Option<String>,
    pub target_group: Option<String>,
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayDeadLetterOutcome {
    Replayed,
    Skipped,
}

#[async_trait]
pub trait QueueEngine {
    async fn poll_ready(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        max: usize,
        lease_deadline: UnixMillis,
    ) -> Result<Vec<Deliverable>, StromaError>;

    async fn ack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn ack_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn nack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        requeue: bool,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn nack_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<NackEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn release_inflight_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn settle(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        req: SettleRequest,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn publish(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        headers: &MessageHeaders,
        payload: Vec<u8>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError>;

    async fn publish_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<PublishItem>,
    ) -> Result<(), StromaError>;

    async fn next_expiry_hint(&self) -> Result<Option<UnixMillis>, StromaError>;

    async fn requeue_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>, StromaError>;

    async fn shutdown(&self) -> Result<(), StromaError>;

    async fn estimate_disk_used(&self) -> Result<u64, StromaError>;

    async fn list_queues(&self) -> Result<Vec<(String, Option<String>)>, StromaError>;

    async fn queue_stats_snapshot(&self) -> Result<QueuesStateSnapshot, StromaError>;

    async fn debug_snapshot(&self) -> Result<StromaDebugSnapshot, StromaError>;

    async fn inspect_messages(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        limit: usize,
        mode: InspectMode,
        include_payload: bool,
        payload_limit_bytes: usize,
    ) -> Result<MessageInspectionPage, StromaError>;

    async fn replay_dead_letters(
        &self,
        dlq_tp: &str,
        dlq_group: Option<&str>,
        offsets: &[Offset],
    ) -> Result<ReplayDeadLettersReport, StromaError>;

    async fn global_dlq(&self) -> Result<GlobalDlqSnapshot, StromaError>;

    async fn set_global_dlq(
        &self,
        target: Option<GlobalDLQ>,
        expected_version: u64,
    ) -> Result<GlobalDlqUpdateOutcome, StromaError>;

    async fn declare_queue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        meta: DeclareMeta,
    ) -> Result<(), StromaError>;

    async fn materialize(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError>;

    async fn unmaterialize(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<EvictOutcome, StromaError>;

    fn is_materialized(&self, tp: &str, part: u32, group: Option<&str>) -> bool;

    async fn has_inflight(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<bool, StromaError>;

    /// The lowest offset not yet settled (acked): every offset below it is
    /// consumed and gone. Used by live repartitioning to tell when a partition
    /// has drained its pre-cutover backlog (settled offset has reached the
    /// cutover boundary).
    async fn lowest_unacked_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset, StromaError>;

    fn metrics(&self) -> Arc<StromaMetrics>;

    fn deadline_awaker(&self) -> Arc<Notify>;
}

#[derive(Debug, Clone)]
pub struct StromaEngine {
    inner: Arc<Stroma>,
}

impl StromaEngine {
    pub async fn open(
        root: impl AsRef<Path>,
        keratin_cfg: StromaKeratinConfig,
        snap_cfg: SnapshotConfig,
    ) -> Result<Self, StromaError> {
        let stroma = Stroma::open(root, keratin_cfg, snap_cfg).await?;
        Ok(Self {
            inner: Arc::new(stroma),
        })
    }

    pub async fn global_store(&self) -> Result<Arc<GlobalStore>, StromaError> {
        self.inner.global_store().await
    }

    pub async fn read_owner_message_records(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
    ) -> Result<OwnerReplicationRead<Message>, StromaError> {
        self.inner
            .read_owner_message_records(tp, part, group, from, max)
            .await
    }

    pub async fn read_owner_event_records(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        max: usize,
    ) -> Result<OwnerReplicationRead<StromaEvent>, StromaError> {
        self.inner
            .read_owner_event_records(tp, part, group, from, max)
            .await
    }

    pub async fn export_owner_state_checkpoint(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<OwnerStateCheckpoint, StromaError> {
        self.inner
            .export_owner_state_checkpoint(tp, part, group)
            .await
    }

    pub async fn install_follower_state_checkpoint(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        install: FollowerStateCheckpointInstall,
    ) -> Result<FollowerStateCheckpointInstallOutcome, StromaError> {
        self.inner
            .install_follower_state_checkpoint(tp, part, group, install)
            .await
    }

    pub async fn become_queue_follower(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner.become_queue_follower(tp, part, group).await
    }

    pub async fn stop_queue_follower_for_transition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner
            .stop_queue_follower_for_transition(tp, part, group)
            .await
    }

    pub async fn freeze_queue_for_transition(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner
            .freeze_queue_for_transition(tp, part, group)
            .await
    }

    pub async fn demote_queue_owner_to_follower(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner
            .demote_queue_owner_to_follower(tp, part, group)
            .await?;
        Ok(())
    }

    pub async fn become_queue_owner(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner.become_queue_owner(tp, part, group).await
    }

    pub async fn apply_replicated_queue_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        messages: Option<ReplicatedMessageBatch>,
        events: Option<ReplicatedEventBatch>,
    ) -> Result<ReplicatedQueueApplyOutcome, StromaError> {
        self.inner
            .apply_replicated_queue_batch(tp, part, group, messages, events)
            .await
    }

    pub async fn promote_queue_follower_if_caught_up(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        expected_message_next_offset: Offset,
        expected_event_next_offset: Offset,
    ) -> Result<QueuePromotionOutcome, StromaError> {
        self.inner
            .promote_queue_follower_if_caught_up(
                tp,
                part,
                group,
                expected_message_next_offset,
                expected_event_next_offset,
            )
            .await
    }

    /// Failover promotion: accept the follower's own tails, fenced at the
    /// assignment epoch (persisted before serving).
    pub async fn promote_queue_follower_to_local_tail(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<QueuePromotionOutcome, StromaError> {
        self.inner
            .promote_queue_follower_to_local_tail(tp, part, group, epoch)
            .await
    }

    /// Fence both queue logs at the assignment epoch (persisted, monotonic).
    pub async fn advance_queue_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<u64, StromaError> {
        self.inner.advance_queue_epoch(tp, part, group, epoch).await
    }

    /// `become_queue_owner` fenced at the assignment epoch.
    pub async fn become_queue_owner_with_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<(), StromaError> {
        self.inner
            .become_queue_owner_with_epoch(tp, part, group, epoch)
            .await
    }

    /// `become_queue_follower` fenced at the assignment epoch: stale-epoch
    /// owners' replicated batches are rejected from here on.
    pub async fn become_queue_follower_with_epoch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        epoch: u64,
    ) -> Result<(), StromaError> {
        self.inner
            .become_queue_follower_with_epoch(tp, part, group, epoch)
            .await
    }

    async fn replay_dead_letter(
        &self,
        dlq_tp: &str,
        dlq_group: Option<&str>,
        offset: Offset,
    ) -> Result<ReplayDeadLetterItem, StromaError> {
        let page = self
            .inner
            .inspect_messages(
                dlq_tp,
                0,
                dlq_group,
                offset,
                1,
                InspectMode::ActiveOnly,
                true,
                usize::MAX,
            )
            .await?;
        let Some(item) = page
            .items
            .into_iter()
            .find(|item| item.state.offset == offset)
        else {
            return Ok(skipped_replay(
                offset,
                "offset is not active in the DLQ queue",
            ));
        };
        let Some(mut headers) = item.headers else {
            return Ok(skipped_replay(
                offset,
                "message headers are missing from the log",
            ));
        };
        let Some(payload) = item.payload else {
            return Ok(skipped_replay(
                offset,
                "message payload is missing from the log",
            ));
        };
        if item.missing_payload {
            return Ok(skipped_replay(
                offset,
                "message payload is missing from the log",
            ));
        }
        if item.payload_truncated {
            return Ok(skipped_replay(
                offset,
                "message payload was truncated during inspection",
            ));
        }

        let Some(target_topic) = headers.extra.get("stroma.dlq.source_topic").cloned() else {
            return Ok(skipped_replay(
                offset,
                "DLQ metadata is missing stroma.dlq.source_topic",
            ));
        };
        let target_group = headers.extra.get("stroma.dlq.source_group").cloned();

        headers.published = unix_millis();
        headers.publish_received = headers.published;
        headers
            .extra
            .retain(|key, _| !key.starts_with("stroma.") && !key.starts_with("fibril."));

        let (completion, receiver) = KeratinAppendCompletion::pair();
        self.inner
            .append_message(
                &target_topic,
                0,
                target_group.as_deref(),
                &headers,
                payload,
                completion,
            )
            .await?;
        match receiver.await {
            Ok(Ok(_)) => Ok(ReplayDeadLetterItem {
                offset,
                outcome: ReplayDeadLetterOutcome::Replayed,
                target_topic: Some(target_topic),
                target_group,
                reason: None,
            }),
            Ok(Err(err)) => Err(StromaError::Io(err.to_string())),
            Err(_) => Err(StromaError::Io("replay append completion dropped".into())),
        }
    }
}

fn skipped_replay(offset: Offset, reason: impl Into<String>) -> ReplayDeadLetterItem {
    ReplayDeadLetterItem {
        offset,
        outcome: ReplayDeadLetterOutcome::Skipped,
        target_topic: None,
        target_group: None,
        reason: Some(reason.into()),
    }
}

// pub fn make_stroma_engine(root: impl AsRef<Path>) -> Result<StromaEngine, StromaError> {
//     let keratin_cfg = KeratinConfig::test_default();
//     let snap_cfg = SnapshotConfig::default();
//     let stroma = futures::executor::block_on(Stroma::open(root, keratin_cfg, snap_cfg))?;
//     Ok(StromaEngine {
//         inner: stroma,
//     })
// }

#[async_trait]
impl QueueEngine for StromaEngine {
    async fn poll_ready(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        max: usize,
        lease_deadline: UnixMillis,
    ) -> Result<Vec<Deliverable>, StromaError> {
        let v = self
            .inner
            .poll_ready(tp, part, group, max, lease_deadline)
            .await?;

        Ok(v.into_iter()
            .map(|(offset, headers, payload, retries)| Deliverable {
                offset,
                payload,
                retries,
                content_type: headers.content_type,
                extra_headers: headers.extra,
                publish_received: headers.publish_received,
                published: headers.published,
            })
            .collect())
    }

    async fn ack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .ack_enqueue(tp, part, group, offset, completion)
            .await
    }

    async fn ack_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .ack_enqueue_many(tp, part, group, items, completion)
            .await
    }

    async fn nack(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        offset: Offset,
        requeue: bool,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .nack_enqueue(tp, part, group, offset, requeue, completion)
            .await?;
        Ok(())
    }

    async fn nack_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<NackEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .nack_enqueue_many(tp, part, group, items, completion)
            .await
    }

    async fn release_inflight_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<AckEventMeta>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .release_inflight_many(tp, part, group, items, completion)
            .await
    }

    async fn settle(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        req: SettleRequest,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        match req.kind {
            SettleKind::Ack => {
                self.inner
                    .ack_enqueue(tp, part, group, req.offset, completion)
                    .await?;
            }
            SettleKind::Nack {
                requeue,
                not_before,
            } => {
                self.inner
                    .nack_enqueue_many(
                        tp,
                        part,
                        group,
                        vec![NackEventMeta {
                            off: req.offset,
                            requeue,
                            not_before,
                        }],
                        completion,
                    )
                    .await?;
            }
        }
        Ok(())
    }

    async fn publish(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        headers: &MessageHeaders,
        payload: Vec<u8>,
        completion: Box<dyn AppendCompletion<IoError>>,
    ) -> Result<(), StromaError> {
        self.inner
            .append_message(tp, part, group, headers, payload, completion)
            .await?;

        Ok(())
    }

    async fn publish_batch(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        items: Vec<PublishItem>,
    ) -> Result<(), StromaError> {
        self.inner
            .append_message_batch(tp, part, group, items)
            .await
    }

    async fn next_expiry_hint(&self) -> Result<Option<UnixMillis>, StromaError> {
        self.inner.next_expiry_hint().await
    }

    async fn requeue_expired(
        &self,
        now: UnixMillis,
        max: usize,
    ) -> Result<HashSet<(String, u32, Option<String>, u64)>, StromaError> {
        self.inner.requeue_expired(now, max).await
    }

    async fn shutdown(&self) -> Result<(), StromaError> {
        self.inner.shutdown().await?;

        Ok(())
    }

    async fn estimate_disk_used(&self) -> Result<u64, StromaError> {
        self.inner.estimate_disk_used().await
    }

    async fn list_queues(&self) -> Result<Vec<(String, Option<String>)>, StromaError> {
        Ok(self
            .inner
            .list_queues()
            .into_iter()
            .map(|(tp, _part, group)| (tp.to_string(), group.map(|s| s.to_string())))
            .collect::<Vec<_>>())
    }

    async fn queue_stats_snapshot(&self) -> Result<QueuesStateSnapshot, StromaError> {
        let stats = self.inner.get_queues_stats().await?;

        let mut snapshot = QueuesStateSnapshot {
            queues: Default::default(),
        };

        for ((tp, group), stat) in stats {
            snapshot.queues.insert(
                fibril_metrics::QueueKey {
                    topic: tp.to_string(),
                    group: group.map(|s| s.to_string()),
                },
                fibril_metrics::QueueStateSnapshot {
                    ready_count: stat.ready_count,
                    inflight_count: stat.inflight_count,
                },
            );
        }

        Ok(snapshot)
    }

    async fn debug_snapshot(&self) -> Result<StromaDebugSnapshot, StromaError> {
        self.inner.debug_snapshot().await
    }

    async fn inspect_messages(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        from: Offset,
        limit: usize,
        mode: InspectMode,
        include_payload: bool,
        payload_limit_bytes: usize,
    ) -> Result<MessageInspectionPage, StromaError> {
        self.inner
            .inspect_messages(
                tp,
                part,
                group,
                from,
                limit,
                mode,
                include_payload,
                payload_limit_bytes,
            )
            .await
    }

    async fn replay_dead_letters(
        &self,
        dlq_tp: &str,
        dlq_group: Option<&str>,
        offsets: &[Offset],
    ) -> Result<ReplayDeadLettersReport, StromaError> {
        let mut items = Vec::with_capacity(offsets.len());
        let mut replayed = 0;

        for &offset in offsets {
            let result = self.replay_dead_letter(dlq_tp, dlq_group, offset).await?;
            if matches!(result.outcome, ReplayDeadLetterOutcome::Replayed) {
                replayed += 1;
            }
            items.push(result);
        }

        Ok(ReplayDeadLettersReport {
            requested: offsets.len(),
            replayed,
            items,
        })
    }

    async fn global_dlq(&self) -> Result<GlobalDlqSnapshot, StromaError> {
        self.inner.global_dlq().await
    }

    async fn set_global_dlq(
        &self,
        target: Option<GlobalDLQ>,
        expected_version: u64,
    ) -> Result<GlobalDlqUpdateOutcome, StromaError> {
        self.inner.set_global_dlq(target, expected_version).await
    }

    async fn declare_queue(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
        meta: DeclareMeta,
    ) -> Result<(), StromaError> {
        self.inner.declare(tp, part, group, meta).await
    }

    async fn materialize(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<(), StromaError> {
        self.inner.materialize(tp, part, group).await
    }

    async fn unmaterialize(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<EvictOutcome, StromaError> {
        self.inner.unmaterialize(tp, part, group).await
    }

    fn is_materialized(&self, tp: &str, part: u32, group: Option<&str>) -> bool {
        self.inner.is_materialized(tp, part, group)
    }

    async fn has_inflight(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<bool, StromaError> {
        self.inner.has_inflight(tp, part, group).await
    }

    async fn lowest_unacked_offset(
        &self,
        tp: &str,
        part: u32,
        group: Option<&str>,
    ) -> Result<Offset, StromaError> {
        self.inner.lowest_unacked_offset(tp, part, group).await
    }

    fn metrics(&self) -> Arc<StromaMetrics> {
        self.inner.metrics()
    }

    fn deadline_awaker(&self) -> Arc<Notify> {
        self.inner.deadline_waker()
    }
}
