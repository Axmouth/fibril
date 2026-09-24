//! Independent healthy-history worker. One bounded local scan at a time; never
//! waits for replica catch-up while holding an actor or application gate.
use fibril_broker::{broker::Broker, queue_engine::StromaEngine};
use fibril_coordination_ganglion::GanglionCoordination;
use ganglion_core::ResourceIdentity;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;

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
        let mut due: BTreeMap<ResourceIdentity, Instant> = BTreeMap::new();
        let mut failures: BTreeMap<ResourceIdentity, (Instant, u64, String)> = BTreeMap::new();
        loop {
            tokio::time::sleep(Duration::from_millis(250)).await;
            if broker.is_shutting_down() {
                break;
            }
            let interval = match provider.runtime_settings_document() {
                Ok(doc) => doc.map_or(0, |d| d.settings.replication.agreed_checkpoint_interval_ms),
                Err(error) => {
                    tracing::warn!(%error, "cannot read checkpoint policy");
                    continue;
                }
            };
            let work = provider.queue_checkpoint_work(interval != 0);
            let eligible: BTreeSet<_> = work.iter().collect();
            due.retain(|r, _| eligible.contains(r));
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
                let start = interval != 0 && due.get(&resource).is_none_or(|at| *at <= now);
                // No outer timeout: disk work owns lifecycle locks after caller
                // cancellation. A retry must not multiply detached scan jobs.
                match provider
                    .queue_checkpoint_step(&resource, &broker.engine(), start)
                    .await
                {
                    Ok(installed) => {
                        failures.remove(&resource);
                        if start || installed && !due.contains_key(&resource) {
                            due.insert(
                                resource.clone(),
                                Instant::now() + Duration::from_millis(interval.max(1_000)),
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
