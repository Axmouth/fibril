//! Isolated staged-delivery experiment. No assigned/replicated queues.
use super::speculative_metrics::{Fallback, Trace};
use super::*;
use tokio::sync::OwnedSemaphorePermit;

#[derive(Debug)]
pub(super) struct Budget {
    _bytes: OwnedSemaphorePermit,
    _count: OwnedSemaphorePermit,
}

#[derive(Debug)]
pub(super) struct SpeculativePublish {
    trace: Option<Arc<Trace>>,
    reply: Mutex<Option<oneshot::Sender<Result<u64, BrokerError>>>>,
    local: AtomicUsize, // 0 pending, 1 durable/applied, 2 failed
    changed: Notify,
    processed_confirm: bool,
    processed: AtomicBool,
    tag: Mutex<Option<DeliveryTag>>,
    fence: Mutex<Option<(stroma_core::QueueHandle, u64, CancellationToken)>>,
    budget: Mutex<Option<Arc<Budget>>>,
    pub(super) cancel: CancellationToken,
}
impl SpeculativePublish {
    fn send(&self, result: Result<u64, BrokerError>) -> Result<(), Result<u64, BrokerError>> {
        match self.reply.lock().unwrap().take() {
            Some(reply) => {
                if let Some(trace) = &self.trace {
                    if result.is_ok() {
                        trace.record("confirm");
                    } else {
                        trace.fail();
                    }
                }
                reply.send(result)
            }
            None => Ok(()),
        }
    }
    pub(super) fn processed(&self, offset: u64) {
        if self.processed_confirm
            && !self.cancel.is_cancelled()
            && self.local.load(Ordering::Acquire) != 2
        {
            self.processed.store(true, Ordering::Release);
            let _ = self.send(Ok(offset));
        }
    }
    pub(super) async fn wait_local(&self) -> bool {
        loop {
            let notified = self.changed.notified();
            match self.local.load(Ordering::Acquire) {
                1 => return true,
                2 => return false,
                _ => notified.await,
            }
        }
    }
}

pub(super) enum ConfirmReply {
    Direct(oneshot::Sender<Result<u64, BrokerError>>),
    Speculative(Arc<SpeculativePublish>),
}
impl ConfirmReply {
    pub(super) fn send(
        self,
        result: Result<u64, BrokerError>,
    ) -> Result<(), Result<u64, BrokerError>> {
        match self {
            Self::Direct(reply) => reply.send(result),
            Self::Speculative(p) => p.send(result),
        }
    }
    pub(super) fn local_complete<
        E: QueueEngine + StreamStore + Clone + std::fmt::Debug + 'static,
    >(
        &self,
        ok: bool,
        broker: &Broker<E>,
    ) {
        if let Self::Speculative(p) = self {
            p.local.store(if ok { 1 } else { 2 }, Ordering::Release);
            p.changed.notify_waiters();
            p.budget.lock().unwrap().take();
            if !ok {
                p.cancel.cancel();
                if p.processed.load(Ordering::Acquire) {
                    tracing::error!(
                        "SPECULATIVE: storage failed after application ACK confirmed publication"
                    );
                }
                if let Some(tag) = p.tag.lock().unwrap().take() {
                    if let Some((_, rec)) = broker.records_by_tags.remove(&tag) {
                        if let Some(qs) = broker.queues.get(&rec.key) {
                            if let Some(c) = qs.consumers.get(&rec.consumer_id) {
                                c.dec_inflight();
                            }
                            qs.wake();
                        }
                    }
                }
            }
        }
    }
}

struct StagedItem {
    headers: MessageHeaders,
    payload: Vec<u8>,
    pending: Arc<SpeculativePublish>,
}

pub(super) struct StageObserver<E: QueueEngine + StreamStore + Clone + std::fmt::Debug + 'static> {
    broker: Arc<Broker<E>>,
    qs: Arc<QueueLoopState>,
    key: QueueKey,
    mode: u8,
    budget: Option<Arc<Budget>>,
    rejection: Option<Fallback>,
    traces: Mutex<Vec<Arc<Trace>>>,
    items: Mutex<Vec<StagedItem>>,
    cancel: CancellationToken,
}
impl<E: QueueEngine + StreamStore + Clone + std::fmt::Debug + 'static> StageObserver<E> {
    pub(super) fn new(
        broker: Arc<Broker<E>>,
        qs: Arc<QueueLoopState>,
        key: QueueKey,
        batch: &[PublishRequest],
        mode: u8,
    ) -> Arc<Self> {
        let bytes = batch.iter().try_fold(0usize, |sum, r| {
            let headers: usize = r.extra.iter().map(|(k, v)| k.len() + v.len()).sum();
            sum.checked_add(r.payload.len().saturating_add(headers).saturating_add(128))
        });
        let mut rejection = None;
        // Optional preparation guard. Final dispatch still revalidates everything,
        // and every staged offset still enters the actor ordering barrier.
        if broker.speculative_telemetry.cheap {
            rejection = if batch
                .iter()
                .any(|r| r.not_before.is_some() || r.expire_at.is_some())
            {
                Some(Fallback::StoragePolicy)
            } else if !broker.speculation_is_local(&key)
                || qs.owner_runtime_shutdown.is_cancelled()
                || qs.delivery_held.load(Ordering::Acquire)
                || qs.hold_above_offset.load(Ordering::Acquire) != u64::MAX
                || qs.exclusive_assignee().is_some()
            {
                Some(Fallback::QueuePolicy)
            } else if qs.consumers.is_empty() {
                Some(Fallback::NoConsumer)
            } else if !qs.consumers.iter().any(|c| c.can_accept()) {
                Some(Fallback::NoCredit)
            } else {
                None
            };
        }
        let budget = if rejection.is_none() {
            match bytes.and_then(|n| u32::try_from(n).ok()).and_then(|n| {
                broker
                    .speculative_bytes
                    .clone()
                    .try_acquire_many_owned(n)
                    .ok()
            }) {
                None => {
                    rejection = Some(Fallback::ByteBudget);
                    None
                }
                Some(bytes) => match broker
                    .speculative_count
                    .clone()
                    .try_acquire_many_owned(batch.len() as u32)
                {
                    Err(_) => {
                        rejection = Some(Fallback::ItemBudget);
                        None
                    }
                    Ok(count) => Some(Arc::new(Budget {
                        _bytes: bytes,
                        _count: count,
                    })),
                },
            }
        } else {
            None
        };
        let cancel = qs.owner_runtime_shutdown.child_token();
        Arc::new(Self {
            broker,
            qs,
            key,
            mode,
            budget,
            rejection,
            traces: Mutex::new(Vec::new()),
            items: Mutex::new(Vec::new()),
            cancel,
        })
    }
    pub(super) fn push(
        &self,
        headers: &MessageHeaders,
        payload: &[u8],
        reply: oneshot::Sender<Result<u64, BrokerError>>,
        admitted: std::time::Instant,
    ) -> ConfirmReply {
        let trace = headers
            .extra
            .get("fibril.message_id")
            .and_then(|id| self.broker.speculative_telemetry.sample(id, admitted));
        if let Some(trace) = &trace {
            self.traces.lock().unwrap().push(trace.clone());
        }
        let pending = Arc::new(SpeculativePublish {
            trace,
            reply: Mutex::new(Some(reply)),
            local: AtomicUsize::new(0),
            changed: Notify::new(),
            processed_confirm: self.mode == 2,
            processed: AtomicBool::new(false),
            tag: Mutex::new(None),
            fence: Mutex::new(None),
            budget: Mutex::new(self.budget.clone()),
            cancel: self.cancel.clone(),
        });
        if self.budget.is_some() {
            self.broker
                .speculative_telemetry
                .copied_bytes
                .fetch_add(payload.len() as u64, Ordering::Relaxed);
            self.items.lock().unwrap().push(StagedItem {
                headers: headers.clone(),
                payload: payload.to_vec(),
                pending: pending.clone(),
            });
        }
        ConfirmReply::Speculative(pending)
    }
}
#[async_trait::async_trait]
impl<E: QueueEngine + StreamStore + Clone + std::fmt::Debug + 'static>
    stroma_core::ExperimentalStageObserver for StageObserver<E>
{
    fn staging_complete(&self) {
        for trace in self.traces.lock().unwrap().iter() {
            trace.stage();
        }
    }
    fn durable_applied(&self) {
        for trace in self.traces.lock().unwrap().iter() {
            trace.record("durable_applied");
        }
    }
    async fn before_apply(&self) -> bool {
        if let Some(gate) = &self.broker.config_snapshot().experimental_commit_gate {
            if let Ok(permit) = gate.acquire().await {
                permit.forget();
            }
        }
        !self.broker.config_snapshot().experimental_message_failure
    }
    async fn staged(
        &self,
        queue: stroma_core::QueueHandle,
        base: u64,
        count: usize,
        eligible: bool,
    ) -> Result<(), StromaError> {
        for trace in self.traces.lock().unwrap().iter() {
            trace.event();
        }
        let dispatch = match self.qs.speculative_dispatch.try_lock().ok() {
            Some(guard) => Some(guard),
            None if self.broker.speculative_telemetry.handoff
                && eligible
                && self.rejection.is_none()
                && self.broker.speculation_is_local(&self.key)
                && !self.cancel.is_cancelled() =>
            {
                self.broker
                    .speculative_telemetry
                    .handoff_attempts
                    .fetch_add(1, Ordering::Relaxed);
                let guard = bounded_dispatch_handoff(&self.qs.speculative_dispatch).await;
                if guard.is_some() {
                    self.broker
                        .speculative_telemetry
                        .handoff_acquired
                        .fetch_add(1, Ordering::Relaxed);
                }
                guard
            }
            None => None,
        };
        let handle = queue.resolve()?;
        // One reason per fallback message. Precedence is explicit and stable;
        // these counters describe the first failed gate, not all possible causes.
        let mut reason = if !eligible {
            Some(Fallback::StoragePolicy)
        } else if let Some(reason) = self.rejection {
            Some(reason)
        } else if !self.broker.speculation_is_local(&self.key)
            || self.qs.owner_runtime_shutdown.is_cancelled()
            || self.qs.delivery_held.load(Ordering::Acquire)
            || self.qs.hold_above_offset.load(Ordering::Acquire) != u64::MAX
            || self.qs.exclusive_assignee().is_some()
        {
            Some(Fallback::QueuePolicy)
        } else if dispatch.is_none() {
            Some(Fallback::DispatchBusy)
        } else {
            None
        };
        let eligible = reason.is_none();
        let mut choices: Vec<_> = if eligible || !self.broker.speculative_telemetry.cheap {
            self.qs
                .consumers
                .iter()
                .map(|c| c.value().clone())
                .collect()
        } else {
            Vec::new()
        };
        choices.sort_by_key(|c| c.sub_id);
        if eligible {
            reason = Some(if choices.is_empty() {
                Fallback::NoConsumer
            } else {
                Fallback::NoCredit
            });
        }
        let mut picked = None;
        if eligible && !choices.is_empty() {
            let start = self.qs.rr.fetch_add(1, Ordering::Relaxed) as usize;
            for i in 0..choices.len() {
                let c = &choices[(start + i) % choices.len()];
                let free = c
                    .prefetch
                    .load(Ordering::Acquire)
                    .saturating_sub(c.inflight.load(Ordering::Acquire));
                if free == 0 {
                    continue;
                }
                reason = Some(Fallback::ChannelUnavailable);
                if let Some(tx) = c.tx.load_full() {
                    if let Ok(permit) = tx.as_ref().clone().try_reserve_owned() {
                        reason = None;
                        picked = Some((c.clone(), permit, free.min(count)));
                        break;
                    }
                }
            }
        }
        let reserve = picked.as_ref().map_or(0, |(_, _, n)| *n);
        let deadline = unix_millis().saturating_add(self.broker.config_snapshot().inflight_ttl_ms);
        let n = handle
            .work_queue()?
            .experimental_stage(base, count, reserve, deadline, self.cancel.clone())
            .await?;
        self.broker.speculative_telemetry.fallback(
            reason.unwrap_or(if n == 0 {
                Fallback::OlderUndispatched
            } else {
                Fallback::PartialCredit
            }),
            count - n,
        );
        self.broker
            .speculative_fallback
            .fetch_add((count - n) as u64, Ordering::Relaxed);
        if n == 0 {
            return Ok(());
        }
        let (consumer, permit, _) = picked.unwrap();
        let sent = {
            // Serialize send/tag insertion with owner retirement. No await in this section.
            let _lifecycle = self.qs.speculative_lifecycle.lock().unwrap();
            if self.cancel.is_cancelled()
                || consumer.closed.get().is_some()
                || !self.broker.speculation_is_local(&self.key)
            {
                false
            } else {
                let items = std::mem::take(&mut *self.items.lock().unwrap());
                let mut batch = Vec::with_capacity(n);
                for (i, item) in items.into_iter().take(n).enumerate() {
                    let offset = base + i as u64;
                    let tag = DeliveryTag {
                        epoch: self.broker.next_tag_epoch.fetch_add(1, Ordering::Relaxed),
                    };
                    *item.pending.tag.lock().unwrap() = Some(tag);
                    *item.pending.fence.lock().unwrap() = Some((
                        queue.clone(),
                        handle.role_generation(),
                        self.qs.owner_runtime_shutdown.clone(),
                    ));
                    let trace = item
                        .headers
                        .extra
                        .get("fibril.message_id")
                        .and_then(|id| self.broker.speculative_telemetry.take(id));
                    if let Some(trace) = &trace {
                        trace.dispatch(true);
                    }
                    self.broker.records_by_tags.insert(
                        tag,
                        TagRecord {
                            trace,
                            key: self.key.clone(),
                            offset,
                            consumer_id: consumer.sub_id,
                            speculative: Some(item.pending),
                        },
                    );
                    consumer.inc_inflight();
                    let mut headers = item.headers.extra;
                    headers.insert("fibril.speculative".into(), "1".into());
                    batch.push(DeliverableMessage {
                        message: StoredMessage {
                            topic: self.key.tp.clone(),
                            group: self.key.group.clone(),
                            partition: self.key.part,
                            offset,
                            published: item.headers.published,
                            publish_received: item.headers.publish_received,
                            retried: 0,
                            content_type: item.headers.content_type,
                            headers,
                            payload: item.payload,
                        },
                        delivery_tag: tag,
                        group: self.key.group.clone(),
                    });
                }
                permit.send(batch);
                self.broker
                    .speculative_delivered
                    .fetch_add(n as u64, Ordering::Relaxed);
                if let Some(metrics) = &self.broker.metrics {
                    metrics.delivered_many(n as u64);
                }
                true
            }
        };
        if !sent {
            self.broker
                .speculative_telemetry
                .fallback(Fallback::Retired, n);
            self.broker
                .speculative_fallback
                .fetch_add(n as u64, Ordering::Relaxed);
            handle
                .work_queue()?
                .experimental_abandon(base, n, false)
                .await?;
        }
        Ok(())
    }
}

impl<E: QueueEngine + StreamStore + Clone + std::fmt::Debug + 'static> Broker<E> {
    pub(super) fn speculation_is_local(&self, key: &QueueKey) -> bool {
        self.assignment_cache.get(key).is_none_or(|a| {
            a.followers.is_empty() && a.durability_requirement().is_ok_and(|r| r.nodes <= 1)
        })
    }

    pub(super) fn defer_speculative_settles(
        self: &Arc<Self>,
        records: Vec<(TagRecord, SettleType)>,
    ) {
        let broker = self.clone();
        self.task_group.spawn("speculative_settle", async move {
            let total = records.len();
            let mut acks: HashMap<QueueKey, Vec<AckEventMeta>> = HashMap::new();
            let mut nacks: HashMap<QueueKey, Vec<NackEventMeta>> = HashMap::new();
            let mut fences = HashMap::new();
            for (record, kind) in records {
                let p = record.speculative.as_ref().unwrap();
                if !p.wait_local().await || p.cancel.is_cancelled() {
                    continue;
                }
                if let Some(fence) = p.fence.lock().unwrap().clone() {
                    fences.insert(record.key.clone(), fence);
                } else {
                    continue;
                }
                match kind {
                    SettleType::Ack => acks
                        .entry(record.key)
                        .or_default()
                        .push(AckEventMeta { off: record.offset }),
                    SettleType::Nack {
                        requeue,
                        not_before,
                    } => nacks.entry(record.key).or_default().push(NackEventMeta {
                        off: record.offset,
                        requeue: requeue.unwrap_or(true),
                        not_before,
                    }),
                    SettleType::Reject { .. } => {
                        nacks.entry(record.key).or_default().push(NackEventMeta {
                            off: record.offset,
                            requeue: false,
                            not_before: None,
                        })
                    }
                }
            }
            for (key, fence) in fences {
                let acks = acks.remove(&key).unwrap_or_default();
                let nacks = nacks.remove(&key).unwrap_or_default();
                let count = acks.len();
                let result = broker
                    .engine
                    .settle_batch_fenced(
                        &key.tp,
                        key.part.id(),
                        key.group.as_deref(),
                        acks,
                        nacks,
                        fence,
                    )
                    .await;
                if let Err(error) = &result {
                    tracing::warn!(topic = %key.tp, partition = key.part.id(), ?error,
                        "speculative settlement rejected or failed after local completion");
                }
                if result.is_ok() {
                    if let Some(m) = &broker.metrics {
                        m.acked_many(count as u64);
                    }
                }
                if let Some(qs) = broker.queues.get(&key) {
                    qs.wake_with_replication();
                }
            }
            let previous = broker.pending_settles.fetch_sub(total, Ordering::AcqRel);
            if previous <= total {
                broker.settle_drained.notify_waiters();
            }
        });
    }
}

// Only a contended admission uses this experiment. The actor barrier and final
// eligibility checks remain unchanged. Tokio's timer rounds sub-ms waits, so
// 250 us is a requested grace interval, not a real-time scheduling guarantee.
async fn bounded_dispatch_handoff(lock: &AsyncMutex<()>) -> Option<AsyncMutexGuard<'_, ()>> {
    tokio::time::timeout(Duration::from_micros(250), lock.lock())
        .await
        .ok()
}

#[cfg(test)]
mod handoff_tests {
    use super::*;
    #[tokio::test(start_paused = true)]
    async fn dispatch_handoff_observes_unlock_before_deadline() {
        let lock = AsyncMutex::new(());
        let held = lock.lock().await;
        let wait = bounded_dispatch_handoff(&lock);
        tokio::pin!(wait);
        assert!(futures::poll!(&mut wait).is_pending());
        drop(held);
        assert!(wait.await.is_some());
    }
    #[tokio::test(start_paused = true)]
    async fn dispatch_handoff_falls_back_when_dispatch_stays_blocked() {
        let lock = AsyncMutex::new(());
        let _held = lock.lock().await;
        assert!(bounded_dispatch_handoff(&lock).await.is_none());
        assert!(lock.try_lock().is_err());
    }
}
