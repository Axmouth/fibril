//! Background catch-up never changes the serving assignment or counts learner ACKs.
use fibril_broker::{
    Partition,
    broker::{
        Broker, BrokerError, BrokerOwnerReplicationPeer, BrokerOwnerReplicationRecords,
        BrokerReplicationCatchUp, BrokerReplicationCatchUpOptions, ReplicationResourceKind,
    },
    queue_engine::{
        OwnerReplicationBatch, OwnerReplicationRead, OwnerStateCheckpoint, StromaEngine,
    },
};
use fibril_coordination_ganglion::GanglionCoordination;
use fibril_protocol::v1::replication::{
    ProtocolOwnerPeerResolverConfig, connect_protocol_owner_peer,
};
use ganglion_core::ResourceIdentity;

// Bound both streams to the captured cut. Otherwise a busy owner can keep
// supplying newer enqueue events faster than their payload backfill catches up.
struct CutPeer<'a> {
    owner: &'a dyn BrokerOwnerReplicationPeer,
    messages: u64,
    events: u64,
}
fn cap_read<T>(read: OwnerReplicationRead<T>, cut: u64) -> OwnerReplicationRead<T> {
    match read {
        OwnerReplicationRead::Batch(mut batch) => {
            batch.records.retain(|(offset, _)| *offset < cut);
            batch.next_offset = batch.next_offset.min(cut).max(batch.requested_offset);
            OwnerReplicationRead::Batch(batch)
        }
        OwnerReplicationRead::CheckpointRequired {
            epoch,
            requested_offset,
            ..
        } if requested_offset >= cut => {
            // This side is already complete locally. Compaction of newer owner
            // data must not force it to chase a new checkpoint unnecessarily.
            OwnerReplicationRead::Batch(OwnerReplicationBatch {
                epoch,
                requested_offset,
                next_offset: requested_offset,
                records: vec![],
            })
        }
        required => required,
    }
}
impl BrokerOwnerReplicationPeer for CutPeer<'_> {
    fn read_owner_replication_records<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        message_from: u64,
        event_from: u64,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
        max_wait_ms: u64,
    ) -> futures::future::BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        self.read_owner_replication_records_fenced(
            topic,
            partition,
            group,
            message_from,
            event_from,
            max_messages,
            max_events,
            max_bytes,
            max_wait_ms,
            None,
        )
    }
    fn read_owner_replication_records_fenced<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        message_from: u64,
        event_from: u64,
        max_messages: usize,
        max_events: usize,
        max_bytes: usize,
        max_wait_ms: u64,
        epoch: Option<u64>,
    ) -> futures::future::BoxFuture<'a, Result<BrokerOwnerReplicationRecords, BrokerError>> {
        Box::pin(async move {
            let records = self
                .owner
                .read_owner_replication_records_fenced(
                    topic,
                    partition,
                    group,
                    message_from,
                    event_from,
                    max_messages,
                    max_events,
                    max_bytes,
                    max_wait_ms,
                    epoch,
                )
                .await?;
            Ok(BrokerOwnerReplicationRecords {
                messages: cap_read(records.messages, self.messages),
                events: cap_read(records.events, self.events),
            })
        })
    }
    fn export_owner_state_checkpoint<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
    ) -> futures::future::BoxFuture<'a, Result<OwnerStateCheckpoint, BrokerError>> {
        self.owner
            .export_owner_state_checkpoint(topic, partition, group)
    }
}

pub async fn learn_queue_once(
    provider: &GanglionCoordination,
    broker: &Broker<StromaEngine>,
    config: &ProtocolOwnerPeerResolverConfig,
    resource: &ResourceIdentity,
) -> Result<(), String> {
    if broker.is_shutting_down() {
        return Err("broker is shutting down".into());
    }
    let intent = provider
        .begin_queue_learner(resource)
        .await
        .map_err(|e| e.to_string())?;
    let engine = broker.engine();
    let receipt = provider
        .prepare_queue_learner(&intent, &engine)
        .await
        .map_err(|e| e.to_string())?;
    // No reporter is configured: learner reads cannot produce write-quorum ACKs.
    let session = intent.session(&provider.consensus_node().committed_snapshot())?;
    let peer = connect_protocol_owner_peer(
        config
            .nodes
            .get(&intent.assignment.owner)
            .ok_or("learner owner address absent")?
            .clone(),
        config.auth.as_ref(),
        config.tls.as_ref(),
        &config.client_name,
        &config.client_version,
    )
    .await
    .map_err(|e| e.to_string())?
    .with_history_session(session);
    let part = Partition::new(u32::try_from(resource.partition).map_err(|e| e.to_string())?);
    let mut cut = peer
        .export_owner_state_checkpoint(&resource.name, part, resource.group.as_deref())
        .await
        .map_err(|e| e.to_string())?;
    if cut.message_epoch != intent.assignment.epoch || cut.event_epoch != intent.assignment.epoch {
        return Err("learner checkpoint has another epoch".into());
    }
    provider
        .authorize_queue_learner(&intent)
        .await
        .map_err(|e| e.to_string())?;
    engine
        .verify_admitted_storage_history(&receipt)
        .map_err(|e| e.to_string())?;
    engine
        .become_queue_follower_with_epoch(
            &resource.name,
            part.id(),
            resource.group.as_deref(),
            intent.assignment.epoch,
        )
        .await
        .map_err(|e| e.to_string())?;
    // A cancelled append must not be treated as completely applied. A verified
    // current-owner checkpoint can replace this non-voting baseline and resume.
    if engine
        .verify_queue_learner_caught_up(
            &resource.name,
            part.id(),
            resource.group.as_deref(),
            intent.assignment.epoch,
            0,
            0,
        )
        .await
        .is_err()
    {
        engine
            .install_queue_learner_checkpoint(
                receipt.clone(),
                fibril_broker::queue_engine::FollowerStateCheckpointInstall {
                    message_epoch: cut.message_epoch,
                    event_epoch: cut.event_epoch,
                    message_next_offset: cut.message_checkpoint_offset,
                    event_next_offset: cut.event_next_offset,
                    applied_event_offset: cut.applied_event_offset,
                    state_snapshot: cut.state_snapshot.clone(),
                },
            )
            .await
            .map_err(|e| e.to_string())?;
    }
    let mut from = engine
        .queue_replication_next_offsets(&resource.name, part.id(), resource.group.as_deref())
        .await
        .map_err(|e| e.to_string())?;
    loop {
        if broker.is_shutting_down() {
            return Err("broker is shutting down".into());
        }
        // The cut is fixed at the start; publishing does not need an idle gap.
        if from.0 >= cut.message_next_offset && from.1 >= cut.event_next_offset {
            provider
                .admit_queue_learner(
                    &intent,
                    &engine,
                    cut.message_next_offset,
                    cut.event_next_offset,
                )
                .await
                .map_err(|e| e.to_string())?;
            tracing::info!(
                topic = resource.name,
                partition = resource.partition,
                node = intent.node,
                message_next = from.0,
                event_next = from.1,
                "background queue learner admitted without fencing the owner"
            );
            return Ok(());
        }
        let cut_peer = CutPeer {
            owner: &peer,
            messages: cut.message_next_offset,
            events: cut.event_next_offset,
        };
        let outcome = broker
            .catch_up_replication_follower_from_owner(
                &cut_peer,
                &resource.name,
                part,
                resource.group.as_deref(),
                ReplicationResourceKind::Queue,
                BrokerReplicationCatchUpOptions {
                    message_from: from.0,
                    event_from: from.1,
                    max_messages_per_read: 4096,
                    max_events_per_read: 4096,
                    max_bytes_per_read: 16 * 1024 * 1024,
                    max_iterations: 16,
                    max_wait_ms: 0,
                },
            )
            .await
            .map_err(|e| e.to_string())?;
        let progress = match outcome {
            BrokerReplicationCatchUp::CaughtUp(p)
            | BrokerReplicationCatchUp::IterationLimit { progress: p } => p,
            BrokerReplicationCatchUp::CheckpointRequired { .. } => {
                // Compaction can overtake the read cursor while the owner
                // continues serving. Only fresh non-voting authority permits reset.
                provider
                    .authorize_queue_learner(&intent)
                    .await
                    .map_err(|e| e.to_string())?;
                let checkpoint = peer
                    .export_owner_state_checkpoint(&resource.name, part, resource.group.as_deref())
                    .await
                    .map_err(|e| e.to_string())?;
                if checkpoint.message_epoch != intent.assignment.epoch
                    || checkpoint.event_epoch != intent.assignment.epoch
                {
                    return Err("learner checkpoint epoch changed".into());
                }
                engine
                    .install_queue_learner_checkpoint(
                        receipt.clone(),
                        fibril_broker::queue_engine::FollowerStateCheckpointInstall {
                            message_epoch: checkpoint.message_epoch,
                            event_epoch: checkpoint.event_epoch,
                            message_next_offset: checkpoint.message_checkpoint_offset,
                            event_next_offset: checkpoint.event_next_offset,
                            applied_event_offset: checkpoint.applied_event_offset,
                            state_snapshot: checkpoint.state_snapshot.clone(),
                        },
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                cut = checkpoint;
                from = engine
                    .queue_replication_next_offsets(
                        &resource.name,
                        part.id(),
                        resource.group.as_deref(),
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                continue;
            }
        };
        from.0 = progress.message_next_offset;
        from.1 = progress.event_next_offset;
        // Validate the unchanged intent before admitting any further local work.
        intent.session(&provider.consensus_node().committed_snapshot())?;
        tokio::task::yield_now().await;
    }
}

/// The learner loop is independent of recovery: an unavailable owner cannot
/// delay witness collection or installation for another queue on this broker.
pub(crate) struct LearnerWorker(tokio::task::JoinHandle<()>);
impl Drop for LearnerWorker {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub(crate) fn spawn(
    provider: std::sync::Arc<GanglionCoordination>,
    broker: std::sync::Arc<Broker<StromaEngine>>,
    mut config: ProtocolOwnerPeerResolverConfig,
) -> LearnerWorker {
    LearnerWorker(tokio::spawn(async move {
        use std::{collections::HashMap, time::Duration};
        let mut learner_failures: HashMap<ResourceIdentity, (tokio::time::Instant, u64, String)> =
            HashMap::new();
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if broker.is_shutting_down() {
                break;
            }
            config.nodes = provider
                .consensus_node()
                .committed_snapshot()
                .nodes
                .iter()
                .map(|(id, n)| (id.clone(), n.endpoint.clone()))
                .collect();
            match provider.queue_learner_work() {
                Ok(work) => {
                    learner_failures.retain(|r, _| work.contains(r));
                    for resource in work {
                        if learner_failures
                            .get(&resource)
                            .is_some_and(|(at, _, _)| *at > tokio::time::Instant::now())
                        {
                            continue;
                        }
                        let result = tokio::time::timeout(
                            Duration::from_secs(120),
                            crate::queue_learner_driver::learn_queue_once(
                                &provider, &broker, &config, &resource,
                            ),
                        )
                        .await
                        .unwrap_or_else(|_| {
                            Err(
                                "learner catch-up exceeded work budget; partial storage retained"
                                    .into(),
                            )
                        });
                        match result {
                            Ok(()) => {
                                learner_failures.remove(&resource);
                            }
                            Err(e) => {
                                let old = learner_failures.get(&resource);
                                let delay = old.map_or(1, |(_, d, _)| (d * 2).min(30));
                                if old.is_none_or(|(_, _, previous)| previous != &e) {
                                    tracing::warn!(topic=resource.name,partition=resource.partition,retry_seconds=delay,error=%e,"queue learner remains non-voting; serving quorum unchanged");
                                }
                                learner_failures.insert(
                                    resource,
                                    (
                                        tokio::time::Instant::now() + Duration::from_secs(delay),
                                        delay,
                                        e,
                                    ),
                                );
                            }
                        }
                    }
                }
                Err(e) => tracing::warn!(error=%e,"cannot inspect background queue learners"),
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_cut_excludes_later_enqueues_and_tolerates_compaction_of_completed_side() {
        let batch = OwnerReplicationBatch {
            epoch: 7,
            requested_offset: 0,
            next_offset: 100,
            records: vec![(0, ()), (1, ()), (2, ()), (3, ())],
        };
        let OwnerReplicationRead::Batch(bounded) = cap_read(OwnerReplicationRead::Batch(batch), 2)
        else {
            panic!()
        };
        assert_eq!(bounded.records, vec![(0, ()), (1, ())]);
        assert_eq!(bounded.next_offset, 2);
        for cut in [0, 2] {
            let bounded = cap_read::<()>(
                OwnerReplicationRead::CheckpointRequired {
                    epoch: 7,
                    requested_offset: 2,
                    head_offset: 80,
                    next_offset: 100,
                },
                cut,
            );
            assert!(
                matches!(bounded,OwnerReplicationRead::Batch(OwnerReplicationBatch {next_offset:2,records,..}) if records.is_empty())
            );
        }
        assert!(matches!(
            cap_read::<()>(
                OwnerReplicationRead::CheckpointRequired {
                    epoch: 7,
                    requested_offset: 1,
                    head_offset: 80,
                    next_offset: 100
                },
                2
            ),
            OwnerReplicationRead::CheckpointRequired { .. }
        ));
    }
}
