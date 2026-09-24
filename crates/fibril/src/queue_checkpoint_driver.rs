//! Independent healthy-history worker. One bounded local scan at a time; never
//! waits for replica catch-up while holding an actor or application gate.
use fibril_broker::{
    broker::{Broker, QueueOwnership},
    queue_engine::StromaEngine,
};
use fibril_coordination_ganglion::GanglionCoordination;
use ganglion_core::ResourceIdentity;
use std::{
    collections::{BTreeMap, BTreeSet},
    hash::{Hash, Hasher},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Policy {
    interval_ms: u64,
    events: u64,
    bytes: u64,
}
struct Cadence {
    policy: Policy,
    due: Instant,
    last_start: Option<Instant>,
    certificate: Option<[u8; 32]>,
    byte_base: u64,
    epoch: u64,
}
impl Cadence {
    fn new(policy: Policy, now: Instant, jitter_ms: u64, bytes: u64, epoch: u64) -> Self {
        Self {
            policy,
            due: now + Duration::from_millis(jitter_ms),
            last_start: None,
            certificate: None,
            byte_base: bytes,
            epoch,
        }
    }
    fn observe(
        &mut self,
        policy: Policy,
        now: Instant,
        certificate: Option<[u8; 32]>,
        bytes: u64,
        epoch: u64,
    ) {
        if self.policy != policy {
            self.policy = policy;
            // Runtime interval reductions take effect without retaining the old deadline.
            self.due = now;
        }
        if self.certificate != certificate || self.epoch != epoch || bytes < self.byte_base {
            self.certificate = certificate;
            self.byte_base = bytes;
            self.epoch = epoch;
        }
    }
    fn reason(&self, now: Instant, event_delta: u64, byte_count: u64) -> Option<&'static str> {
        if self.policy.interval_ms == 0
            || self
                .last_start
                .is_some_and(|t| now < t + Duration::from_secs(1))
        {
            return None;
        }
        if self.policy.events != 0 && event_delta >= self.policy.events {
            return Some("events");
        }
        if self.policy.bytes != 0 && byte_count.saturating_sub(self.byte_base) >= self.policy.bytes
        {
            return Some("bytes");
        }
        (now >= self.due).then_some("age")
    }
    fn started(&mut self, now: Instant, jitter_ms: u64) {
        self.last_start = Some(now);
        // Jitter brings work forward, rather than extending the configured age.
        self.due = now
            + Duration::from_millis(self.policy.interval_ms.saturating_sub(jitter_ms).max(1_000));
    }
}
fn jitter(resource: &ResourceIdentity, interval: u64) -> u64 {
    let mut hash = std::collections::hash_map::DefaultHasher::new();
    resource.hash(&mut hash);
    hash.finish() % (interval / 10).clamp(1, 1_000)
}

pub(crate) struct CheckpointWorker(tokio::task::JoinHandle<()>);
impl Drop for CheckpointWorker {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub(crate) fn spawn(
    provider: Arc<GanglionCoordination>,
    broker: Arc<Broker<StromaEngine>>,
) -> CheckpointWorker {
    CheckpointWorker(tokio::spawn(async move {
        let mut cadence: BTreeMap<ResourceIdentity, Cadence> = BTreeMap::new();
        let mut failures: BTreeMap<ResourceIdentity, (Instant, u64, String)> = BTreeMap::new();
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if broker.is_shutting_down() {
                break;
            }
            let policy = match provider.runtime_settings_document() {
                Ok(doc) => {
                    let settings = doc.map(|d| d.settings.replication).unwrap_or_default();
                    Policy {
                        interval_ms: settings.agreed_checkpoint_interval_ms,
                        events: settings.agreed_checkpoint_max_events,
                        bytes: settings.agreed_checkpoint_max_bytes,
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, "cannot read checkpoint policy");
                    continue;
                }
            };
            let work = provider.queue_checkpoint_work(policy.interval_ms != 0);
            let eligible: BTreeSet<_> = work.iter().collect();
            cadence.retain(|r, _| eligible.contains(r));
            failures.retain(|r, _| eligible.contains(r));
            drop(eligible);
            for resource in work {
                if broker.is_shutting_down() {
                    break;
                }
                let now = Instant::now();
                if failures.get(&resource).is_some_and(|(at, _, _)| *at > now) {
                    continue;
                }
                let Ok(partition) = u32::try_from(resource.partition) else {
                    continue;
                };
                let Some(activity) = broker.engine().queue_checkpoint_activity(
                    &resource.name,
                    partition,
                    resource.group.as_deref(),
                ) else {
                    continue;
                };
                let status = match provider.queue_checkpoint_status(&resource) {
                    Ok(status) => status,
                    Err(error) => {
                        tracing::debug!(%error, "checkpoint status unavailable");
                        continue;
                    }
                };
                let jitter_ms = jitter(&resource, policy.interval_ms);
                let schedule = cadence.entry(resource.clone()).or_insert_with(|| {
                    Cadence::new(
                        policy,
                        now,
                        jitter_ms,
                        activity.local_append_bytes,
                        activity.event_epoch,
                    )
                });
                schedule.observe(
                    policy,
                    now,
                    status.certificate,
                    activity.local_append_bytes,
                    activity.event_epoch,
                );
                let event_delta = activity
                    .event_next
                    .saturating_sub(status.event_next.unwrap_or(activity.event_head));
                let reason = schedule.reason(now, event_delta, activity.local_append_bytes);
                let start = provider.replication_node_id() == Some(status.owner.as_str())
                    && !status.in_progress
                    && reason.is_some();
                // No outer timeout: disk work owns lifecycle locks after caller
                // cancellation. A retry must not multiply detached scan jobs.
                match provider
                    .queue_checkpoint_step(&resource, &broker.engine(), start)
                    .await
                {
                    Ok(installed) => {
                        failures.remove(&resource);
                        if start {
                            schedule.started(Instant::now(), jitter_ms);
                            tracing::info!(
                                topic = resource.name,
                                partition = resource.partition,
                                trigger = reason.unwrap(),
                                event_delta,
                                local_append_bytes = activity
                                    .local_append_bytes
                                    .saturating_sub(schedule.byte_base),
                                installed,
                                "background checkpoint cadence evaluated"
                            );
                        }
                    }
                    Err(error) => {
                        let old = failures.get(&resource);
                        let delay = old.map_or(1, |(_, d, _)| (d * 2).min(30));
                        if old.is_none_or(|(_, _, previous)| previous != &error) {
                            tracing::warn!(topic=resource.name, partition=resource.partition, retry_seconds=delay, %error,
                                "background checkpoint deferred; ordinary replication and recovery remain available");
                        }
                        failures.insert(
                            resource,
                            (Instant::now() + Duration::from_secs(delay), delay, error),
                        );
                    }
                }
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn work_triggers_obey_opt_in_and_minimum_spacing() {
        let now = Instant::now();
        let policy = Policy {
            interval_ms: 60_000,
            events: 100,
            bytes: 1024,
        };
        let mut c = Cadence::new(policy, now, 500, 0, 1);
        assert_eq!(c.reason(now, 99, 1023), None);
        assert_eq!(c.reason(now, 100, 0), Some("events"));
        assert_eq!(c.reason(now, 0, 1024), Some("bytes"));
        c.started(now, 500);
        assert_eq!(c.reason(now + Duration::from_millis(999), 1000, 5000), None);
        assert_eq!(
            c.reason(now + Duration::from_secs(1), 1000, 0),
            Some("events")
        );
        c.observe(
            Policy {
                interval_ms: 0,
                ..policy
            },
            now,
            None,
            0,
            1,
        );
        assert_eq!(
            c.reason(now + Duration::from_secs(100), u64::MAX, u64::MAX),
            None
        );
    }
    #[test]
    fn byte_baseline_rebases_on_certificate_epoch_or_counter_reset() {
        let now = Instant::now();
        let p = Policy {
            interval_ms: 60_000,
            events: 0,
            bytes: 100,
        };
        let mut c = Cadence::new(p, now, 500, 1000, 1);
        c.observe(p, now, Some([1; 32]), 1200, 1);
        assert_eq!(c.reason(now, 0, 1299), None);
        assert_eq!(c.reason(now, 0, 1300), Some("bytes"));
        c.observe(p, now, Some([1; 32]), 0, 1);
        assert_eq!(c.reason(now, 0, 99), None);
        c.observe(p, now, Some([1; 32]), 500, 2);
        assert_eq!(c.reason(now, 0, 599), None);
        assert_eq!(
            c.reason(now + Duration::from_millis(500), 0, 599),
            Some("age")
        );
    }
    #[test]
    fn interval_changes_and_queue_staggering_are_bounded() {
        let now = Instant::now();
        let p = Policy {
            interval_ms: 60_000,
            events: 0,
            bytes: 0,
        };
        let mut c = Cadence::new(p, now, 900, 0, 1);
        c.started(now, 900);
        assert_eq!(c.reason(now + Duration::from_secs(10), 0, 0), None);
        c.observe(
            Policy {
                interval_ms: 1000,
                ..p
            },
            now + Duration::from_secs(10),
            None,
            0,
            1,
        );
        assert_eq!(c.reason(now + Duration::from_secs(10), 0, 0), Some("age"));
        let values: BTreeSet<_> = (0..32)
            .map(|n| {
                jitter(
                    &ResourceIdentity::new("fibril/queue", "q", n, None::<String>),
                    60_000,
                )
            })
            .collect();
        assert!(values.len() > 16);
        assert!(values.iter().all(|v| *v < 1000));
    }
}
