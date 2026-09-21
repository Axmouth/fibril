//! Creation-time preparation and activation for enrolled queues.
use fibril_broker::{broker::Broker, queue_engine::StromaEngine};
use fibril_coordination_ganglion::{
    GanglionCoordination, initial_history::InitialHistoryReceiptSet,
};
use fibril_protocol::v1::{
    initial_history::request_preparation, replication::ProtocolOwnerPeerResolverConfig,
};
use ganglion_core::ResourceIdentity;
use std::time::Duration;

/// One attempt; the caller supplies the overall deadline and retry backoff.
pub async fn prepare_queue_once(
    provider: &GanglionCoordination,
    broker: &Broker<StromaEngine>,
    config: &ProtocolOwnerPeerResolverConfig,
    resource: &ResourceIdentity,
) -> Result<(), String> {
    if broker.is_shutting_down() {
        return Err("broker is shutting down".into());
    }
    let decision = provider
        .resume_initial_history(resource)
        .await
        .map_err(|e| e.to_string())?;
    if decision.assignment.replica_set_size() > 16 {
        return Err("initial preparation exceeds the 16-replica work budget".into());
    }
    let mut receipts = InitialHistoryReceiptSet::new(
        &provider.consensus_node().committed_snapshot(),
        decision.clone(),
    )?;
    for node in std::iter::once(&decision.assignment.owner).chain(&decision.assignment.followers) {
        let command = decision.prepare_command(node)?;
        let outcome = if *node == decision.assignment.owner {
            broker.prepare_initial_history_replica(command).await
        } else {
            request_preparation(config, &command, Duration::from_secs(10)).await
        };
        match outcome {
            Ok(receipt) => receipts.record(
                &provider.consensus_node().committed_snapshot(),
                node,
                receipt,
            )?,
            Err(e) => tracing::debug!(node, error=%e, "initial replica preparation will retry"),
        }
    }
    provider
        .persist_initial_history_quorum(&receipts)
        .await
        .map_err(|e| e.to_string())?;
    provider
        .commit_initial_history_activation(&decision, &broker.engine())
        .await
        .map_err(|e| e.to_string())?;
    provider
        .admit_local_initial_history(&decision, &broker.engine())
        .await
        .map_err(|e| e.to_string())?;
    tracing::info!(
        topic = resource.name,
        partition = resource.partition,
        epoch = decision.assignment.epoch,
        "queue initial history activated"
    );
    Ok(())
}
