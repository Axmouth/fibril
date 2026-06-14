//! Server wiring extracted from the `fibril-server` binary so it can be unit
//! tested. Per the fibril/ganglion split, this holds fibril-specific glue
//! (protocol<->coordination adapter bridges, config->settings mapping); the
//! reusable coordination primitives live in ganglion.

use std::sync::Arc;

use async_trait::async_trait;
use fibril_admin::{RuntimeSettingsClusterStore, RuntimeSettingsClusterUpdateOutcome};
use fibril_broker::runtime_settings::{
    ConnectionRuntimeSettings as BrokerConnectionRuntimeSettings, ConsumerGroupRuntimeSettings,
    DeliveryRuntimeSettings, IdleQueueCleanupRuntimeSettings, PartitioningRuntimeSettings,
    ReplicationRuntimeSettings, RuntimeSettings, RuntimeSettingsSnapshot,
};
use fibril_config::ServerConfig;
use fibril_coordination_ganglion::{
    ClientTopology, ClusterRuntimeSettingsUpdateOutcome, GanglionCoordination,
};
use fibril_protocol::v1::handler::{ClientTopologySource, QueueDeclareCoordinator};
use fibril_protocol::v1::{Partition, QueueTopologyEntry, TopologyOk};
use ganglion_openraft::{
    GanglionRaftConfig,
    openraft::{RaftNetworkFactory, storage::RaftLogStorage},
};

/// Map the startup config's runtime-seed section into the broker's
/// `RuntimeSettings` (the initial cluster document before replicated overrides).
pub fn runtime_seed_from_config(config: &ServerConfig) -> RuntimeSettings {
    RuntimeSettings {
        delivery: DeliveryRuntimeSettings {
            inflight_ttl_ms: config.runtime_seed.delivery.inflight_ttl_ms,
            expiry_poll_min_ms: config.runtime_seed.delivery.expiry_poll_min_ms,
            expiry_batch_max: config.runtime_seed.delivery.expiry_batch_max,
            delivery_poll_max_ms: config.runtime_seed.delivery.delivery_poll_max_ms,
        },
        idle_queue_cleanup: IdleQueueCleanupRuntimeSettings {
            enabled: config.runtime_seed.idle_queue_cleanup.enabled,
            evict_after_ms: config.runtime_seed.idle_queue_cleanup.evict_after_ms,
            sweep_interval_ms: config.runtime_seed.idle_queue_cleanup.sweep_interval_ms,
            publisher_idle_timeout_ms: config
                .runtime_seed
                .idle_queue_cleanup
                .publisher_idle_timeout_ms,
        },
        connection: BrokerConnectionRuntimeSettings {
            reconnect_grace_ms: config.runtime_seed.connection.reconnect_grace_ms,
        },
        replication: ReplicationRuntimeSettings {
            confirm_timeout_ms: config.runtime_seed.replication.confirm_timeout_ms,
            caught_up_poll_ms: config.runtime_seed.replication.caught_up_poll_ms,
            retry_poll_ms: config.runtime_seed.replication.retry_poll_ms,
            checkpoint_retry_poll_ms: config.runtime_seed.replication.checkpoint_retry_poll_ms,
            min_in_sync_replicas: config.runtime_seed.replication.min_in_sync_replicas,
            isr_timeout_ms: config.runtime_seed.replication.isr_timeout_ms,
        },
        partitioning: PartitioningRuntimeSettings {
            default_partition_count: config.runtime_seed.partitioning.default_partition_count,
        },
        consumer_groups: ConsumerGroupRuntimeSettings {
            default_target_per_consumer: config
                .runtime_seed
                .consumer_groups
                .default_target_per_consumer,
        },
    }
}

pub struct GanglionRuntimeSettingsStore<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    coordination: Arc<GanglionCoordination<LS, NF>>,
}

impl<LS, NF> GanglionRuntimeSettingsStore<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    pub fn new(coordination: Arc<GanglionCoordination<LS, NF>>) -> Self {
        Self { coordination }
    }
}

#[async_trait]
impl<LS, NF> RuntimeSettingsClusterStore for GanglionRuntimeSettingsStore<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig> + 'static,
    NF: RaftNetworkFactory<GanglionRaftConfig> + 'static,
{
    async fn current_runtime_settings(&self) -> Result<Option<RuntimeSettingsSnapshot>, String> {
        self.coordination
            .runtime_settings_document()
            .map(|document| {
                document.map(|document| RuntimeSettingsSnapshot {
                    version: document.cluster_version,
                    settings: document.settings,
                })
            })
            .map_err(|error| error.to_string())
    }

    async fn update_runtime_settings(
        &self,
        expected_version: u64,
        settings: RuntimeSettings,
    ) -> Result<RuntimeSettingsClusterUpdateOutcome, String> {
        match self
            .coordination
            .update_runtime_settings(expected_version, &settings)
            .await
            .map_err(|error| error.to_string())?
        {
            ClusterRuntimeSettingsUpdateOutcome::Stored(document) => Ok(
                RuntimeSettingsClusterUpdateOutcome::Stored(RuntimeSettingsSnapshot {
                    version: document.cluster_version,
                    settings: document.settings,
                }),
            ),
            ClusterRuntimeSettingsUpdateOutcome::Conflict(Some(document)) => Ok(
                RuntimeSettingsClusterUpdateOutcome::Conflict(RuntimeSettingsSnapshot {
                    version: document.cluster_version,
                    settings: document.settings,
                }),
            ),
            ClusterRuntimeSettingsUpdateOutcome::Conflict(None) => Ok(
                RuntimeSettingsClusterUpdateOutcome::Conflict(RuntimeSettingsSnapshot {
                    version: 0,
                    settings,
                }),
            ),
        }
    }
}

/// Bridges the coordination provider's client topology into the protocol
/// handler's `ClientTopologySource`, keeping coordination-ganglion free of a
/// protocol dependency. The closure fetches a fresh snapshot each call.
pub struct CoordinationTopologySource {
    pub fetch: Arc<dyn Fn() -> ClientTopology + Send + Sync>,
}

impl ClientTopologySource for CoordinationTopologySource {
    fn topology(&self) -> TopologyOk {
        let topology = (self.fetch)();
        TopologyOk {
            generation: topology.generation,
            queues: topology
                .queues
                .into_iter()
                .map(|queue| QueueTopologyEntry {
                    topic: queue.topic,
                    partition: queue.partition,
                    group: queue.group,
                    owner_endpoint: queue.owner_endpoint,
                    partitioning_version: queue.partitioning_version,
                    partition_count: queue.partition_count,
                })
                .collect(),
        }
    }

    fn owner_endpoint(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Option<(String, u64)> {
        (self.fetch)()
            .queues
            .into_iter()
            .find(|queue| {
                queue.topic == topic
                    && queue.partition == partition
                    && queue.group.as_deref() == group
            })
            .and_then(|queue| {
                queue
                    .owner_endpoint
                    .map(|endpoint| (endpoint, queue.partitioning_version))
            })
    }
}

/// Boxed future returned by the declare bridge.
pub type DeclareFut = futures::future::BoxFuture<'static, Result<u32, String>>;

/// Bridges queue-declare partitioning writes to the coordination provider. The
/// boxed-future closure captures the provider, avoiding naming its generic type
/// and keeping coordination-ganglion free of a protocol dependency.
pub struct CoordinationDeclareCoordinator {
    pub declare: Arc<dyn Fn(String, Option<String>, u32) -> DeclareFut + Send + Sync>,
}

impl QueueDeclareCoordinator for CoordinationDeclareCoordinator {
    fn declare_partitioning<'a>(
        &'a self,
        topic: &'a str,
        group: Option<&'a str>,
        partition_count: u32,
    ) -> futures::future::BoxFuture<'a, Result<u32, String>> {
        (self.declare)(
            topic.to_string(),
            group.map(str::to_string),
            partition_count,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_seed_maps_config_defaults() {
        let config = ServerConfig::default();
        let seed = runtime_seed_from_config(&config);

        // Spot-check each section maps through (this mapping has regressed
        // before when fields were added).
        assert_eq!(
            seed.delivery.inflight_ttl_ms,
            config.runtime_seed.delivery.inflight_ttl_ms
        );
        assert_eq!(
            seed.replication.confirm_timeout_ms,
            config.runtime_seed.replication.confirm_timeout_ms
        );
        assert_eq!(
            seed.replication.min_in_sync_replicas,
            config.runtime_seed.replication.min_in_sync_replicas
        );
        assert_eq!(
            seed.partitioning.default_partition_count,
            config.runtime_seed.partitioning.default_partition_count
        );
        assert_eq!(
            seed.idle_queue_cleanup.sweep_interval_ms,
            config.runtime_seed.idle_queue_cleanup.sweep_interval_ms
        );
    }

    #[test]
    fn runtime_seed_carries_non_default_values() {
        let mut config = ServerConfig::default();
        config.runtime_seed.partitioning.default_partition_count = 7;
        config.runtime_seed.replication.min_in_sync_replicas = 3;
        let seed = runtime_seed_from_config(&config);
        assert_eq!(seed.partitioning.default_partition_count, 7);
        assert_eq!(seed.replication.min_in_sync_replicas, 3);
    }
}
