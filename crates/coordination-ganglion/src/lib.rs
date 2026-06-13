//! Spike: ganglion raft-backed implementation of fibril's [`Coordination`] trait.
//!
//! A `RaftMetadataNode` replicates the coordination snapshot through real
//! consensus; this crate maps between fibril's and ganglion's snapshot models
//! and bridges ganglion's committed-snapshot watch into the
//! `watch::Receiver<CoordinationSnapshot>` fibril consumes. Reads are sync
//! (trait-compatible); proposals are async and leader-only, matching the
//! controller-loop model from `REPLICATION_PLANNING.md`.

use std::collections::HashMap;

use fibril_broker::coordination::{
    Coordination, CoordinationSnapshot, CoordinationStream, NodeInfo, PartitionAssignment,
    PartitionPlacementError, PartitionPlacementInput, PartitionPlacementPolicy, QueueIdentity,
    ReplicationDurabilityPolicy,
};
use ganglion_openraft::openraft::storage::RaftLogStorage;
use ganglion_openraft::openraft::RaftNetworkFactory;
use ganglion_openraft::{
    client_write_remote, GanglionLogStore, GanglionRaftConfig, InProcessRouter,
    MetadataRaftCommand, MetadataRaftResponse, MetadataRejection, OpenraftAdapterError,
    RaftMetadataNode, WireFormat,
};
use tokio::sync::watch;

/// Namespace tag used for fibril queues inside ganglion resource identities.
const QUEUE_NAMESPACE: &str = "fibril/queue";

/// Attribute key carrying the replicated runtime-settings document.
pub const RUNTIME_SETTINGS_ATTRIBUTE: &str = "fibril/runtime_settings";

/// Replicated runtime-settings document: the cluster truth. `cluster_version`
/// is independent of each node's local store version (those differ per node);
/// CAS on the serialized document makes concurrent publishers race-safe.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusterRuntimeSettings {
    pub cluster_version: u64,
    pub settings: fibril_broker::runtime_settings::RuntimeSettings,
}

/// Heartbeat label prefix for per-assignment applied tails:
/// `applied/<topic>/<partition>[/<group>] = "<message_next>:<event_next>"`.
/// Advisory data for failover candidate selection — the broker-side checked
/// promotion remains the safety authority (stale labels can only pick a worse
/// candidate, never an unsafe one).
pub const APPLIED_TAIL_LABEL_PREFIX: &str = "applied/";

/// Label key for one queue's applied tails.
pub fn applied_tail_label(queue: &QueueIdentity) -> String {
    match &queue.group {
        Some(group) => format!(
            "{APPLIED_TAIL_LABEL_PREFIX}{}/{}/{group}",
            queue.topic, queue.partition
        ),
        None => format!(
            "{APPLIED_TAIL_LABEL_PREFIX}{}/{}",
            queue.topic, queue.partition
        ),
    }
}

fn parse_applied_tail(raw: &str) -> Option<(u64, u64)> {
    let (message, event) = raw.split_once(':')?;
    Some((message.parse().ok()?, event.parse().ok()?))
}

/// Node label carrying the broker's last heartbeat (unix milliseconds, broker
/// clock). Liveness compares against the controller's clock — TTLs must
/// absorb clock skew and election gaps (see ganglion FAILURE_MODES.md §4b.6).
pub const HEARTBEAT_LABEL: &str = "heartbeat_unix_ms";

fn unix_millis_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as u64)
        .unwrap_or_default()
}

/// Convert a fibril snapshot into ganglion's model.
///
/// Lossless: queue identity, owner/followers, epoch, durability, and node
/// endpoints all carry over.
pub fn to_ganglion_snapshot(
    snapshot: &CoordinationSnapshot,
) -> ganglion_core::CoordinationSnapshot {
    let nodes = snapshot
        .nodes
        .iter()
        .map(|(node_id, info)| {
            (
                node_id.clone(),
                ganglion_core::NodeInfo::new(
                    info.node_id.clone(),
                    info.broker_addr.to_string(),
                    info.admin_addr.map(|addr| addr.to_string()),
                ),
            )
        })
        .collect();

    let assignments = snapshot
        .assignments
        .iter()
        .map(|(queue, assignment)| {
            let resource = to_ganglion_resource(queue);
            let mut mapped = ganglion_core::PartitionAssignment::new(
                resource.clone(),
                assignment.owner.clone(),
                assignment.followers.clone(),
                assignment.epoch,
            );
            mapped.durability = to_ganglion_durability(assignment.durability);
            (resource, mapped)
        })
        .collect();

    ganglion_core::CoordinationSnapshot {
        nodes,
        assignments,
        // fibril's snapshot type does not model these; writers that must
        // preserve them (control_iteration) copy from the committed snapshot.
        resources: std::collections::BTreeSet::new(),
        attributes: std::collections::BTreeMap::new(),
        generation: snapshot.generation,
    }
}

/// Convert a ganglion snapshot back into fibril's model.
///
/// Entries that cannot be represented on the fibril side (non-queue
/// namespaces, partitions beyond `u32`, unparseable socket addresses) are
/// skipped rather than failing the whole snapshot: a coordination consumer
/// must keep operating on the entries it understands.
pub fn to_fibril_snapshot(snapshot: &ganglion_core::CoordinationSnapshot) -> CoordinationSnapshot {
    let nodes: HashMap<String, NodeInfo> = snapshot
        .nodes
        .iter()
        .filter_map(|(node_id, info)| {
            let broker_addr = info.endpoint.parse().ok()?;
            let admin_addr = info
                .admin_endpoint
                .as_ref()
                .and_then(|endpoint| endpoint.parse().ok());
            Some((
                node_id.clone(),
                NodeInfo {
                    node_id: info.node_id.clone(),
                    broker_addr,
                    admin_addr,
                },
            ))
        })
        .collect();

    let assignments = snapshot
        .assignments
        .iter()
        .filter_map(|(resource, assignment)| {
            let queue = to_fibril_queue(resource)?;
            let mut mapped = PartitionAssignment::new(
                queue.clone(),
                assignment.owner.clone(),
                assignment.followers.clone(),
                assignment.epoch,
            );
            mapped.durability = to_fibril_durability(assignment.durability);
            Some((queue, mapped))
        })
        .collect();

    CoordinationSnapshot {
        nodes,
        assignments,
        generation: snapshot.generation,
    }
}

fn to_ganglion_resource(queue: &QueueIdentity) -> ganglion_core::ResourceIdentity {
    ganglion_core::ResourceIdentity::new(
        QUEUE_NAMESPACE,
        queue.topic.clone(),
        u64::from(queue.partition),
        queue.group.clone(),
    )
}

fn to_fibril_queue(resource: &ganglion_core::ResourceIdentity) -> Option<QueueIdentity> {
    if resource.namespace != QUEUE_NAMESPACE {
        return None;
    }
    let partition = u32::try_from(resource.partition).ok()?;
    Some(QueueIdentity::new(
        resource.name.clone(),
        partition,
        resource.group.as_deref(),
    ))
}

fn to_ganglion_durability(
    durability: ReplicationDurabilityPolicy,
) -> ganglion_core::ReplicationDurabilityPolicy {
    use ganglion_core::ReplicationDurabilityPolicy as G;
    match durability {
        ReplicationDurabilityPolicy::LocalDurable => G::LocalDurable,
        ReplicationDurabilityPolicy::ReplicaAccepted { nodes } => G::ReplicaAccepted { nodes },
        ReplicationDurabilityPolicy::ReplicaDurable { nodes } => G::ReplicaDurable { nodes },
        ReplicationDurabilityPolicy::MajorityDurable => G::MajorityDurable,
    }
}

fn to_fibril_durability(
    durability: ganglion_core::ReplicationDurabilityPolicy,
) -> ReplicationDurabilityPolicy {
    use ganglion_core::ReplicationDurabilityPolicy as G;
    match durability {
        G::LocalDurable => ReplicationDurabilityPolicy::LocalDurable,
        G::ReplicaAccepted { nodes } => ReplicationDurabilityPolicy::ReplicaAccepted { nodes },
        G::ReplicaDurable { nodes } => ReplicationDurabilityPolicy::ReplicaDurable { nodes },
        G::MajorityDurable => ReplicationDurabilityPolicy::MajorityDurable,
    }
}

/// Settings for the embedded controller loop (from server config).
#[derive(Debug, Clone)]
pub struct ControllerConfig {
    pub target_followers: usize,
    /// Bounded tick interval; the loop also wakes on snapshot changes.
    pub tick: std::time::Duration,
    /// Heartbeats older than this mark a broker dead. Must exceed worst-case
    /// election + retry time (ganglion FAILURE_MODES §4b.6).
    pub liveness_ttl: std::time::Duration,
    pub max_cas_retries: usize,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            target_followers: 1,
            tick: std::time::Duration::from_millis(2000),
            liveness_ttl: std::time::Duration::from_millis(9000),
            max_cas_retries: 8,
        }
    }
}

/// Live controller status for observability (topology JSON / admin page).
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ControllerStatus {
    /// Whether the last tick ran as the active controller (raft leader).
    pub active: bool,
    pub last_plan_generation: Option<u64>,
    pub last_error: Option<String>,
}

/// Controller-iteration failure surface.
#[derive(Debug)]
pub enum ControlError {
    /// The pure placement planner refused the input.
    Planning(PartitionPlacementError),
    /// Consensus rejected the proposal (leadership loss, retries exhausted, IO).
    Consensus(OpenraftAdapterError),
}

impl std::fmt::Display for ControlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Planning(error) => write!(f, "planning failed: {error:?}"),
            Self::Consensus(error) => write!(f, "consensus rejected proposal: {error}"),
        }
    }
}

impl std::error::Error for ControlError {}

/// Fibril `Coordination` provider backed by a ganglion raft node.
///
/// Reads serve from a fibril-side watch channel fed by ganglion's
/// committed-snapshot stream; a background task forwards updates. Proposals go
/// through [`GanglionCoordination::propose`], which is leader-only.
pub struct GanglionCoordination<LS = GanglionLogStore, NF = InProcessRouter<LS>>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    node_id: String,
    node: RaftMetadataNode<LS, NF>,
    tx: watch::Sender<CoordinationSnapshot>,
    forwarder: tokio::task::JoinHandle<()>,
    wire_format: WireFormat,
}

impl<LS, NF> std::fmt::Debug for GanglionCoordination<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GanglionCoordination")
            .field("node_id", &self.node_id)
            .field("raft_node_id", &self.node.node_id())
            .finish_non_exhaustive()
    }
}

impl<LS, NF> GanglionCoordination<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    /// Wrap a started raft node as a fibril coordination provider.
    ///
    /// `node_id` is the fibril-side node identity (string); the raft node id
    /// (u64) is an internal transport concern and need not match.
    ///
    /// Must be called from within a tokio runtime (spawns the watch forwarder).
    pub fn new(node_id: impl Into<String>, node: RaftMetadataNode<LS, NF>) -> Self {
        Self::new_with_wire_format(node_id, node, WireFormat::default())
    }

    /// `new` with the wire format used for forwarded writes (registration /
    /// heartbeats sent to the leader). Pass from startup configuration.
    pub fn new_with_wire_format(
        node_id: impl Into<String>,
        node: RaftMetadataNode<LS, NF>,
        wire_format: WireFormat,
    ) -> Self {
        let mut ganglion_rx = node.watch_committed();
        let initial = to_fibril_snapshot(&ganglion_rx.borrow_and_update());
        let (tx, _rx) = watch::channel(initial);

        let forward_tx = tx.clone();
        let forwarder = tokio::spawn(async move {
            while ganglion_rx.changed().await.is_ok() {
                let mapped = to_fibril_snapshot(&ganglion_rx.borrow_and_update());
                forward_tx.send_replace(mapped);
            }
        });

        Self {
            node_id: node_id.into(),
            node,
            tx,
            forwarder,
            wire_format,
        }
    }

    /// Register (or refresh) this broker in the committed snapshot.
    ///
    /// Leader: applies locally. Follower: forwards to the leader's raft
    /// address from topology. No-leader and transient failures surface as
    /// errors the caller should retry with backoff — never crash on them
    /// (brokers must keep serving while coordination heals).
    pub async fn register_self(&self, info: &NodeInfo) -> Result<(), OpenraftAdapterError> {
        self.register_self_with_labels(info, std::collections::BTreeMap::new())
            .await
    }

    /// `register_self` carrying extra advisory labels (e.g. applied tails for
    /// failover candidate selection).
    pub async fn register_self_with_labels(
        &self,
        info: &NodeInfo,
        labels: std::collections::BTreeMap<String, String>,
    ) -> Result<(), OpenraftAdapterError> {
        let mut node = ganglion_core::NodeInfo::new(
            info.node_id.clone(),
            info.broker_addr.to_string(),
            info.admin_addr.map(|addr| addr.to_string()),
        );
        node.labels = labels;
        node.labels
            .insert(HEARTBEAT_LABEL.to_string(), unix_millis_now().to_string());

        self.forward_merge(MetadataRaftCommand::RegisterNode { node })
            .await
    }

    /// Whether the watch-forwarder task is still running. A dead forwarder
    /// means fibril-side reads go silently stale (FAILURE_MODES §5.4) —
    /// include this in broker health checks.
    pub fn forwarder_alive(&self) -> bool {
        !self.forwarder.is_finished()
    }

    /// Coarse coordination health: the forwarder is alive and a raft leader
    /// is currently known. False during elections/partitions (reads still
    /// serve last-committed state; writes will fail until healthy).
    pub fn coordination_healthy(&self) -> bool {
        self.forwarder_alive() && self.node.topology().leader.is_some()
    }

    /// Add a queue to the cluster catalogue (forwarded merge; idempotent).
    pub async fn register_queue(&self, queue: &QueueIdentity) -> Result<(), OpenraftAdapterError> {
        self.forward_merge(MetadataRaftCommand::RegisterResource {
            resource: to_ganglion_resource(queue),
        })
        .await
    }

    /// Remove a queue from the cluster catalogue (forwarded merge).
    pub async fn deregister_queue(
        &self,
        queue: &QueueIdentity,
    ) -> Result<(), OpenraftAdapterError> {
        self.forward_merge(MetadataRaftCommand::DeregisterResource {
            resource: to_ganglion_resource(queue),
        })
        .await
    }

    /// The committed cluster queue catalogue (fibril-representable entries).
    pub fn registered_queues(&self) -> Vec<QueueIdentity> {
        self.node
            .committed_snapshot()
            .resources
            .iter()
            .filter_map(to_fibril_queue)
            .collect()
    }

    /// Read one replicated cluster attribute (e.g. runtime settings).
    pub fn cluster_attribute(&self, key: &str) -> Option<String> {
        self.node.committed_snapshot().attributes.get(key).cloned()
    }

    /// Set one replicated cluster attribute (forwarded merge; same-value
    /// writes are generation no-ops). Consumers own versioning in the value.
    pub async fn set_cluster_attribute(
        &self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), OpenraftAdapterError> {
        self.forward_merge(MetadataRaftCommand::SetAttribute {
            key: key.into(),
            value: value.into(),
        })
        .await
    }

    /// Leader-or-forwarded merge write (registration, catalogue, attributes).
    async fn forward_merge(
        &self,
        command: MetadataRaftCommand,
    ) -> Result<(), OpenraftAdapterError> {
        self.forward_command(command).await.map(|_| ())
    }

    /// Leader-or-forwarded command with state-machine rejections mapped to
    /// errors on both paths (the wire returns rejections in-band).
    async fn forward_command(
        &self,
        command: MetadataRaftCommand,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        if self.node.is_leader().await {
            return self.node.submit_merge(command).await;
        }
        let topology = self.node.topology();
        let leader_addr = topology
            .leader
            .and_then(|leader| topology.nodes.get(&leader).cloned())
            .ok_or(OpenraftAdapterError::NotLeader)?;
        let response = client_write_remote(&leader_addr, command, self.wire_format).await?;
        match response.rejection.clone() {
            None => Ok(response),
            Some(MetadataRejection::StaleGeneration) => Err(OpenraftAdapterError::StaleGeneration),
            Some(MetadataRejection::GenerationMismatch { expected, actual }) => {
                Err(OpenraftAdapterError::GenerationMismatch { expected, actual })
            }
            Some(MetadataRejection::AttributeMismatch { key, actual }) => {
                Err(OpenraftAdapterError::AttributeMismatch { key, actual })
            }
        }
    }

    /// Publish runtime settings as the cluster truth (bounded CAS loop:
    /// concurrent publishers serialize; last committed wins everywhere).
    /// Returns the new cluster version.
    pub async fn publish_runtime_settings(
        &self,
        settings: &fibril_broker::runtime_settings::RuntimeSettings,
    ) -> Result<u64, OpenraftAdapterError> {
        for _ in 0..8 {
            let current_raw = self.cluster_attribute(RUNTIME_SETTINGS_ATTRIBUTE);
            let cluster_version = current_raw
                .as_deref()
                .and_then(|raw| serde_json::from_str::<ClusterRuntimeSettings>(raw).ok())
                .map(|doc| doc.cluster_version)
                .unwrap_or(0);
            let document = ClusterRuntimeSettings {
                cluster_version: cluster_version + 1,
                settings: settings.clone(),
            };
            let value = serde_json::to_string(&document)
                .map_err(|error| OpenraftAdapterError::Storage(error.to_string()))?;

            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttribute {
                    key: RUNTIME_SETTINGS_ATTRIBUTE.to_string(),
                    expected: current_raw,
                    value,
                })
                .await
            {
                Ok(_) => return Ok(document.cluster_version),
                Err(OpenraftAdapterError::AttributeMismatch { .. }) => continue,
                Err(error) => return Err(error),
            }
        }
        Err(OpenraftAdapterError::Storage(
            "runtime settings publish lost the CAS race repeatedly".to_string(),
        ))
    }

    /// Spawn the settings-sync loop: applies replicated runtime-settings
    /// documents to the local manager whenever the cluster document advances.
    /// The local Stroma store stays the node-local cache; the attribute is the
    /// cluster truth. Locked-field rejections are logged loudly (that node
    /// diverges deliberately via its boot locks).
    pub fn spawn_runtime_settings_sync(
        self: &std::sync::Arc<Self>,
        manager: std::sync::Arc<fibril_broker::runtime_settings::RuntimeSettingsManager>,
    ) -> tokio::task::JoinHandle<()>
    where
        LS: 'static,
        NF: 'static,
    {
        let provider = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            let mut watch = provider.node.watch_committed();
            let mut last_applied_cluster_version = 0u64;
            loop {
                let document = watch
                    .borrow_and_update()
                    .attributes
                    .get(RUNTIME_SETTINGS_ATTRIBUTE)
                    .and_then(|raw| serde_json::from_str::<ClusterRuntimeSettings>(raw).ok());

                if let Some(document) = document {
                    if document.cluster_version > last_applied_cluster_version {
                        let current = manager.current();
                        if current.settings == document.settings {
                            // Already effective (e.g. this node published it).
                            last_applied_cluster_version = document.cluster_version;
                        } else {
                            match manager
                                .update(current.version, document.settings.clone())
                                .await
                            {
                                Ok(_) => {
                                    last_applied_cluster_version = document.cluster_version;
                                }
                                Err(error) => {
                                    tracing::warn!(
                                        %error,
                                        cluster_version = document.cluster_version,
                                        "replicated runtime settings rejected locally"
                                    );
                                    // Do not retry a hard rejection (locks);
                                    // mark seen to avoid a hot loop.
                                    last_applied_cluster_version = document.cluster_version;
                                }
                            }
                        }
                    }
                }

                if watch.changed().await.is_err() {
                    return;
                }
            }
        })
    }

    /// Spawn the embedded controller loop: leader-gated placement over the
    /// cluster catalogue and heartbeat-live brokers. Standbys idle. Returns
    /// the task handle plus a shared status cell for observability.
    pub fn spawn_controller(
        self: &std::sync::Arc<Self>,
        planner: std::sync::Arc<dyn PartitionPlacementPolicy>,
        config: ControllerConfig,
    ) -> (
        tokio::task::JoinHandle<()>,
        std::sync::Arc<std::sync::RwLock<ControllerStatus>>,
    )
    where
        LS: 'static,
        NF: 'static,
    {
        let provider = std::sync::Arc::clone(self);
        let status = std::sync::Arc::new(std::sync::RwLock::new(ControllerStatus::default()));
        let shared_status = status.clone();

        let handle = tokio::spawn(async move {
            let mut watch = provider.watch();
            loop {
                let queues = provider.registered_queues();
                let live = provider.live_nodes(config.liveness_ttl);
                let outcome = if queues.is_empty() || live.is_empty() {
                    // Nothing to place (or no live brokers): a normal idle
                    // tick, not an error.
                    Ok(provider.node.is_leader().await.then(|| provider.snapshot()))
                } else {
                    provider
                        .control_iteration(
                            planner.as_ref(),
                            &queues,
                            config.target_followers,
                            &live,
                            config.max_cas_retries,
                        )
                        .await
                };

                if let Ok(mut status) = status.write() {
                    match outcome {
                        Ok(Some(snapshot)) => {
                            status.active = true;
                            status.last_plan_generation = Some(snapshot.generation);
                            status.last_error = None;
                        }
                        Ok(None) => status.active = false,
                        Err(error) => {
                            status.active = true;
                            status.last_error = Some(error.to_string());
                        }
                    }
                }

                tokio::select! {
                    changed = watch.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    _ = tokio::time::sleep(config.tick) => {}
                }
            }
        });
        (handle, shared_status)
    }

    /// Spawn the catalogue-sync loop: every `interval`, list this broker's
    /// local queues and register any missing from the cluster catalogue.
    /// Idempotent diffing keeps idle ticks off the raft log; failures are
    /// retried next tick (coordination outages never kill the broker). Also
    /// covers pre-existing on-disk queues after restarts.
    pub fn spawn_catalogue_sync<ListFn, ListFut>(
        self: &std::sync::Arc<Self>,
        list_local_queues: ListFn,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()>
    where
        ListFn: Fn() -> ListFut + Send + 'static,
        ListFut: std::future::Future<Output = Vec<QueueIdentity>> + Send,
        LS: 'static,
        NF: 'static,
    {
        let provider = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let local = list_local_queues().await;
                if !local.is_empty() {
                    let known: std::collections::HashSet<QueueIdentity> =
                        provider.registered_queues().into_iter().collect();
                    for queue in local {
                        if !known.contains(&queue) {
                            if let Err(error) = provider.register_queue(&queue).await {
                                tracing::debug!(%error, topic = %queue.topic, "catalogue sync deferred");
                            }
                        }
                    }
                }
                tokio::time::sleep(interval).await;
            }
        })
    }

    /// Spawn a heartbeat loop registering `info` every `interval`.
    ///
    /// Failures are logged and retried: coordination outages must never kill
    /// a broker (FAILURE_MODES.md §4b.6). Abort the handle to stop.
    pub fn spawn_heartbeat(
        self: &std::sync::Arc<Self>,
        info: NodeInfo,
        interval: std::time::Duration,
    ) -> tokio::task::JoinHandle<()>
    where
        LS: 'static,
        NF: 'static,
    {
        self.spawn_heartbeat_with_labels(info, interval, || std::collections::BTreeMap::new())
    }

    /// Heartbeat loop with per-tick advisory labels (applied tails etc.).
    pub fn spawn_heartbeat_with_labels<LabelsFn>(
        self: &std::sync::Arc<Self>,
        info: NodeInfo,
        interval: std::time::Duration,
        labels: LabelsFn,
    ) -> tokio::task::JoinHandle<()>
    where
        LabelsFn: Fn() -> std::collections::BTreeMap<String, String> + Send + 'static,
        LS: 'static,
        NF: 'static,
    {
        let provider = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            loop {
                if let Err(error) = provider.register_self_with_labels(&info, labels()).await {
                    tracing::debug!(
                        node_id = %info.node_id,
                        %error,
                        "coordination heartbeat deferred; will retry"
                    );
                }
                tokio::time::sleep(interval).await;
            }
        })
    }

    /// Live brokers: registered nodes whose heartbeat is within `ttl` of this
    /// process's clock. Nodes without a heartbeat label are treated as live
    /// (manually registered/static entries).
    pub fn live_nodes(&self, ttl: std::time::Duration) -> HashMap<String, NodeInfo> {
        let now = unix_millis_now();
        let ttl_ms = ttl.as_millis() as u64;
        self.node
            .committed_snapshot()
            .nodes
            .values()
            .filter(|node| {
                node.labels
                    .get(HEARTBEAT_LABEL)
                    .and_then(|raw| raw.parse::<u64>().ok())
                    .is_none_or(|beat| now.saturating_sub(beat) <= ttl_ms)
            })
            .filter_map(|node| {
                let broker_addr = node.endpoint.parse().ok()?;
                Some((
                    node.node_id.clone(),
                    NodeInfo {
                        node_id: node.node_id.clone(),
                        broker_addr,
                        admin_addr: node
                            .admin_endpoint
                            .as_ref()
                            .and_then(|endpoint| endpoint.parse().ok()),
                    },
                ))
            })
            .collect()
    }

    /// Propose a new coordination snapshot through raft consensus.
    ///
    /// Leader-only (`NotLeader` otherwise); stale generations are rejected
    /// after commit with `StaleGeneration`.
    pub async fn propose(
        &self,
        snapshot: &CoordinationSnapshot,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
        self.node
            .write_snapshot(to_ganglion_snapshot(snapshot))
            .await
    }

    /// Whether this node currently leads the coordination raft group.
    pub async fn is_leader(&self) -> bool {
        self.node.is_leader().await
    }

    /// One embedded-controller iteration, gated on raft leadership.
    ///
    /// Implements the controller-loop shape from `REPLICATION_PLANNING.md`:
    /// read committed state, run the pure placement planner over the given
    /// live-node set, stamp fencing epochs (owner change bumps, follower churn
    /// holds), and propose through a guarded CAS write — retrying when another
    /// proposal commits in between (with raft this only happens across a
    /// leadership change).
    ///
    /// Liveness is the caller's input on purpose: heartbeat/lease mechanisms
    /// live above this layer, so the controller stays a pure function of
    /// (committed state, live nodes, desired queues).
    ///
    /// Returns `Ok(None)` when this node is not the leader (standbys watch).
    pub async fn control_iteration(
        &self,
        planner: &dyn PartitionPlacementPolicy,
        queues: &[QueueIdentity],
        target_followers: usize,
        live_nodes: &HashMap<String, NodeInfo>,
        max_retries: usize,
    ) -> Result<Option<CoordinationSnapshot>, ControlError> {
        if !self.node.is_leader().await {
            return Ok(None);
        }

        let mut attempts = 0;
        loop {
            let committed = self.node.committed_snapshot();
            let fibril_committed = to_fibril_snapshot(&committed);

            let plan = planner
                .plan(PartitionPlacementInput {
                    nodes: live_nodes.clone(),
                    queues: queues.to_vec(),
                    existing: fibril_committed.assignments,
                    target_followers,
                    generation: committed.generation + 1,
                })
                .map_err(ControlError::Planning)?;

            let mut desired = to_ganglion_snapshot(&plan.snapshot);

            // Failover candidate selection: when the committed owner is dead
            // and the planner moved ownership, prefer the most caught-up LIVE
            // committed follower (by heartbeat-label applied event tail).
            // Advisory only — checked promotion on the broker is the safety
            // gate; with one follower this is a no-op.
            for (resource, planned) in desired.assignments.iter_mut() {
                let Some(current) = committed.assignments.get(resource) else {
                    continue;
                };
                let owner_died = !live_nodes.contains_key(&current.owner);
                if !owner_died || planned.owner == current.owner {
                    continue;
                }
                let queue = match to_fibril_queue(resource) {
                    Some(queue) => queue,
                    None => continue,
                };
                let label = applied_tail_label(&queue);
                let best = current
                    .followers
                    .iter()
                    .filter(|follower| live_nodes.contains_key(*follower))
                    .filter_map(|follower| {
                        let tails = committed
                            .nodes
                            .get(follower)
                            .and_then(|node| node.labels.get(&label))
                            .and_then(|raw| parse_applied_tail(raw))?;
                        Some((follower.clone(), tails.1))
                    })
                    .max_by_key(|(_, event_tail)| *event_tail);
                if let Some((best_follower, _)) = best {
                    if planned.owner != best_follower {
                        let displaced =
                            std::mem::replace(&mut planned.owner, best_follower.clone());
                        // Keep the replica set coherent: the chosen follower
                        // leaves the follower list; the planner's displaced
                        // pick joins it (if live and distinct).
                        planned
                            .followers
                            .retain(|follower| *follower != best_follower);
                        if displaced != best_follower
                            && live_nodes.contains_key(&displaced)
                            && !planned.followers.contains(&displaced)
                        {
                            planned.followers.push(displaced);
                        }
                    }
                }
            }

            desired.generation = committed.generation + 1;
            // Snapshot-replace writes must preserve everything the planner
            // does not own: the catalogue, the replicated attribute store,
            // and the node registry (heartbeat labels live there — replacing
            // the nodes map from planner output would erase liveness data).
            desired.resources = committed.resources.clone();
            desired.attributes = committed.attributes.clone();
            desired.nodes = committed.nodes.clone();
            ganglion_core::stamp_assignment_epochs(&committed, &mut desired);

            // Anti-churn: when the plan changes nothing, do not write at all.
            // Keeps idle controller ticks off the raft log entirely.
            if desired.assignments == committed.assignments {
                return Ok(Some(to_fibril_snapshot(&committed)));
            }

            match self
                .node
                .write_snapshot_guarded(committed.generation, desired)
                .await
            {
                Ok(response) => return Ok(Some(to_fibril_snapshot(&response.snapshot))),
                Err(OpenraftAdapterError::GenerationMismatch { .. }) if attempts < max_retries => {
                    attempts += 1;
                }
                Err(error) => return Err(ControlError::Consensus(error)),
            }
        }
    }

    /// Access the underlying raft node (membership changes, waits, shutdown).
    pub fn raft_node(&self) -> &RaftMetadataNode<LS, NF> {
        &self.node
    }
}

impl<LS, NF> Drop for GanglionCoordination<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    fn drop(&mut self) {
        self.forwarder.abort();
    }
}

/// Queue-ownership gate view: in cluster mode brokers serve only queues the
/// committed snapshot assigns to them.
impl<LS, NF> fibril_broker::broker::QueueOwnership for GanglionCoordination<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    fn owns_queue(
        &self,
        topic: &str,
        partition: fibril_storage::LogId,
        group: Option<&str>,
    ) -> bool {
        Coordination::owns_queue(self, topic, partition, group)
    }
}

impl<LS, NF> Coordination for GanglionCoordination<LS, NF>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
    NF: RaftNetworkFactory<GanglionRaftConfig>,
{
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn snapshot(&self) -> CoordinationSnapshot {
        self.tx.borrow().clone()
    }

    fn watch(&self) -> CoordinationStream {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    use ganglion_openraft::{default_raft_config, InProcessRouter};

    fn sample_snapshot(generation: u64, epoch: u64) -> CoordinationSnapshot {
        let mut nodes = HashMap::new();
        nodes.insert(
            "broker-a".to_string(),
            NodeInfo {
                node_id: "broker-a".to_string(),
                broker_addr: "127.0.0.1:9000".parse().expect("addr"),
                admin_addr: None,
            },
        );
        nodes.insert(
            "broker-b".to_string(),
            NodeInfo {
                node_id: "broker-b".to_string(),
                broker_addr: "127.0.0.1:9001".parse().expect("addr"),
                admin_addr: Some("127.0.0.1:9101".parse().expect("addr")),
            },
        );

        let queue = QueueIdentity::new("orders", 0, Some("workers"));
        let mut assignments = HashMap::new();
        assignments.insert(
            queue.clone(),
            PartitionAssignment::new(queue, "broker-a", vec!["broker-b".to_string()], epoch)
                .with_durability(ReplicationDurabilityPolicy::MajorityDurable),
        );

        CoordinationSnapshot {
            nodes,
            assignments,
            generation,
        }
    }

    #[test]
    fn snapshot_mapping_roundtrips_losslessly() {
        let original = sample_snapshot(7, 3);
        let roundtripped = to_fibril_snapshot(&to_ganglion_snapshot(&original));
        assert_eq!(roundtripped, original);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ganglion_backed_coordination_serves_fibril_trait() {
        let router = InProcessRouter::new();
        let config = default_raft_config().expect("config");
        let raft_node = RaftMetadataNode::start(1, config, &router)
            .await
            .expect("raft node should start");

        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            ganglion_openraft::openraft::BasicNode::new("coordinator-1"),
        );
        raft_node.initialize(members).await.expect("initialize");
        raft_node
            .wait_for_leader(1, Duration::from_secs(10))
            .await
            .expect("single node elects itself");

        let coordination = GanglionCoordination::new("broker-a", raft_node);
        let mut stream = coordination.watch();

        // Propose an assignment snapshot through consensus.
        let proposed = sample_snapshot(1, 1);
        let response = coordination
            .propose(&proposed)
            .await
            .expect("leader proposal should commit");
        assert!(response.accepted);

        // The fibril-side watch observes the committed snapshot.
        tokio::time::timeout(Duration::from_secs(10), async {
            while stream.borrow_and_update().generation < 1 {
                stream.changed().await.expect("stream open");
            }
        })
        .await
        .expect("watch should observe the committed snapshot");

        // Trait queries answer from the committed state.
        assert_eq!(coordination.snapshot(), proposed);
        assert!(coordination.owns_queue("orders", 0, Some("workers")));
        assert!(!coordination.follows_queue("orders", 0, Some("workers")));
        let owner = coordination
            .owner_for("orders", 0, Some("workers"))
            .expect("owner resolves");
        assert_eq!(owner.node_id, "broker-a");
        assert_eq!(
            coordination
                .assignment_for("orders", 0, Some("workers"))
                .expect("assignment resolves")
                .epoch,
            1
        );

        // Stale generation is rejected after consensus.
        let stale = sample_snapshot(0, 1);
        let err = coordination
            .propose(&stale)
            .await
            .expect_err("stale generation must be rejected");
        assert!(matches!(err, OpenraftAdapterError::StaleGeneration));

        coordination.raft_node().shutdown().await.expect("shutdown");
    }

    /// Failover choreography: the raft leader IS the controller. The controller
    /// loop assigns a queue from the live-node set, a follower provider
    /// observes it, and when the owner drops out of the live set the next
    /// iteration reassigns ownership with a fencing-epoch bump.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controller_loop_drives_owner_failover_with_epoch_bump() {
        use fibril_broker::coordination::DeterministicPartitionPlacement;

        let router = InProcessRouter::new();
        let config = default_raft_config().expect("config");
        let timeout = Duration::from_secs(10);

        let mut raft_nodes = Vec::new();
        for id in 1..=3u64 {
            raft_nodes.push(
                RaftMetadataNode::start(id, config.clone(), &router)
                    .await
                    .expect("raft node should start"),
            );
        }
        let members: BTreeMap<u64, _> = (1..=3u64)
            .map(|id| {
                (
                    id,
                    ganglion_openraft::openraft::BasicNode::new(format!("broker-{id}")),
                )
            })
            .collect();
        raft_nodes[0].initialize(members).await.expect("initialize");
        let leader_raft_id = raft_nodes[0]
            .wait_for_any_leader(timeout)
            .await
            .expect("election");

        let providers: Vec<GanglionCoordination> = raft_nodes
            .into_iter()
            .enumerate()
            .map(|(index, node)| GanglionCoordination::new(format!("broker-{}", index + 1), node))
            .collect();
        let controller = providers
            .iter()
            .find(|provider| provider.raft_node().node_id() == leader_raft_id)
            .expect("leader provider");
        let standby = providers
            .iter()
            .find(|provider| provider.raft_node().node_id() != leader_raft_id)
            .expect("standby provider");

        // Standbys never act as controller.
        let queue = QueueIdentity::new("orders", 0, Some("workers"));
        let live_node = |id: &str, port: u16| NodeInfo {
            node_id: id.to_string(),
            broker_addr: format!("127.0.0.1:{port}").parse().expect("addr"),
            admin_addr: None,
        };
        let mut live: HashMap<String, NodeInfo> = HashMap::new();
        live.insert("broker-1".into(), live_node("broker-1", 9001));
        live.insert("broker-2".into(), live_node("broker-2", 9002));
        live.insert("broker-3".into(), live_node("broker-3", 9003));

        let standby_result = standby
            .control_iteration(
                &DeterministicPartitionPlacement,
                std::slice::from_ref(&queue),
                1,
                &live,
                8,
            )
            .await
            .expect("standby iteration is a no-op");
        assert!(standby_result.is_none(), "standby must not control");

        // Controller assigns the queue.
        let assigned = controller
            .control_iteration(
                &DeterministicPartitionPlacement,
                std::slice::from_ref(&queue),
                1,
                &live,
                8,
            )
            .await
            .expect("controller iteration should commit")
            .expect("leader runs the iteration");
        let first = assigned
            .assignment_for("orders", 0, Some("workers"))
            .expect("queue assigned")
            .clone();
        assert_eq!(first.epoch, 1, "fresh assignment starts at epoch 1");
        assert_eq!(first.replica_set_size(), 2);

        // A follower provider observes the committed assignment via watch.
        let mut stream = standby.watch();
        tokio::time::timeout(timeout, async {
            while stream
                .borrow_and_update()
                .assignment_for("orders", 0, Some("workers"))
                .is_none()
            {
                stream.changed().await.expect("stream open");
            }
        })
        .await
        .expect("standby should observe the assignment");

        // The owner dies: drop it from the live set; the next iteration must
        // move ownership and bump the fencing epoch.
        live.remove(&first.owner);
        let reassigned = controller
            .control_iteration(
                &DeterministicPartitionPlacement,
                std::slice::from_ref(&queue),
                1,
                &live,
                8,
            )
            .await
            .expect("failover iteration should commit")
            .expect("leader runs the iteration");
        let second = reassigned
            .assignment_for("orders", 0, Some("workers"))
            .expect("queue still assigned")
            .clone();
        assert_ne!(second.owner, first.owner, "ownership must move");
        assert_eq!(second.epoch, first.epoch + 1, "owner change must fence");

        // The standby's watch converges on the failover assignment.
        tokio::time::timeout(timeout, async {
            loop {
                let epoch = stream
                    .borrow_and_update()
                    .assignment_for("orders", 0, Some("workers"))
                    .map(|assignment| assignment.epoch);
                if epoch == Some(second.epoch) {
                    break;
                }
                stream.changed().await.expect("stream open");
            }
        })
        .await
        .expect("standby should observe the failover");

        for provider in &providers {
            provider.raft_node().shutdown().await.expect("shutdown");
        }
    }

    /// Catalogue + attributes: registered queues are cluster-visible, survive
    /// controller snapshot-replace writes, and attributes roundtrip.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalogue_and_attributes_roundtrip_and_survive_controller_writes() {
        use fibril_broker::coordination::DeterministicPartitionPlacement;

        let router = InProcessRouter::new();
        let config = default_raft_config().expect("config");
        let raft_node = RaftMetadataNode::start(1, config, &router)
            .await
            .expect("raft node");
        let mut members = BTreeMap::new();
        members.insert(1u64, ganglion_openraft::openraft::BasicNode::new("n1"));
        raft_node.initialize(members).await.expect("initialize");
        raft_node
            .wait_for_leader(1, Duration::from_secs(10))
            .await
            .expect("election");
        let provider = GanglionCoordination::new("broker-a", raft_node);

        let queue = QueueIdentity::new("orders", 0, Some("workers"));
        provider
            .register_queue(&queue)
            .await
            .expect("register queue");
        provider.register_queue(&queue).await.expect("idempotent");
        assert_eq!(provider.registered_queues(), vec![queue.clone()]);

        provider
            .set_cluster_attribute("fibril/runtime_settings", "{\"v\":1}")
            .await
            .expect("set attribute");
        assert_eq!(
            provider
                .cluster_attribute("fibril/runtime_settings")
                .as_deref(),
            Some("{\"v\":1}")
        );

        // A controller snapshot-replace write must preserve both.
        provider
            .register_self(&NodeInfo {
                node_id: "broker-a".into(),
                broker_addr: "127.0.0.1:9000".parse().expect("addr"),
                admin_addr: None,
            })
            .await
            .expect("register node for placement");
        let live = provider.live_nodes(Duration::from_secs(30));
        provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                0,
                &live,
                8,
            )
            .await
            .expect("controller iteration")
            .expect("leader runs it");

        assert_eq!(provider.registered_queues(), vec![queue.clone()]);
        assert_eq!(
            provider
                .cluster_attribute("fibril/runtime_settings")
                .as_deref(),
            Some("{\"v\":1}"),
            "attributes must survive controller writes"
        );
        let assigned = provider
            .snapshot()
            .assignment_for("orders", 0, Some("workers"))
            .cloned()
            .expect("controller assigned the catalogue queue");
        assert_eq!(assigned.owner, "broker-a");

        provider.deregister_queue(&queue).await.expect("deregister");
        assert!(provider.registered_queues().is_empty());

        provider.raft_node().shutdown().await.expect("shutdown");
    }

    /// Loop-level controller test: assigns catalogue queues for live brokers,
    /// idle ticks never advance the generation (anti-churn), and a dead
    /// broker's queues move with an epoch bump on a later tick.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controller_loop_assigns_idles_and_fails_over() {
        use fibril_broker::coordination::DeterministicPartitionPlacement;

        let router = InProcessRouter::new();
        let config = default_raft_config().expect("config");
        let raft_node = RaftMetadataNode::start(1, config, &router)
            .await
            .expect("raft node");
        let mut members = BTreeMap::new();
        members.insert(1u64, ganglion_openraft::openraft::BasicNode::new("n1"));
        raft_node.initialize(members).await.expect("initialize");
        raft_node
            .wait_for_leader(1, Duration::from_secs(10))
            .await
            .expect("election");
        let provider = std::sync::Arc::new(GanglionCoordination::new("broker-a", raft_node));

        let broker = |id: &str, port: u16| NodeInfo {
            node_id: id.to_string(),
            broker_addr: format!("127.0.0.1:{port}").parse().expect("addr"),
            admin_addr: None,
        };
        provider
            .register_self(&broker("broker-a", 9000))
            .await
            .expect("a");
        provider
            .register_self(&broker("broker-b", 9001))
            .await
            .expect("b");
        let queue = QueueIdentity::new("orders", 0, None);
        provider.register_queue(&queue).await.expect("queue");

        let (controller, status) = provider.spawn_controller(
            std::sync::Arc::new(DeterministicPartitionPlacement),
            ControllerConfig {
                target_followers: 1,
                tick: Duration::from_millis(100),
                // Generous TTL first: both brokers count as live.
                liveness_ttl: Duration::from_secs(30),
                max_cas_retries: 8,
            },
        );

        // Assignment appears.
        let mut stream = provider.watch();
        tokio::time::timeout(Duration::from_secs(10), async {
            while stream
                .borrow_and_update()
                .assignment_for("orders", 0, None)
                .is_none()
            {
                stream.changed().await.expect("stream open");
            }
        })
        .await
        .expect("controller assigns the catalogue queue");
        let first = provider
            .snapshot()
            .assignment_for("orders", 0, None)
            .cloned()
            .expect("assigned");
        assert_eq!(first.epoch, 1);
        assert_eq!(first.replica_set_size(), 2, "owner + follower");
        // Status updates just after the write returns; poll briefly.
        tokio::time::timeout(Duration::from_secs(5), async {
            while !status.read().unwrap().active {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("controller reports active");

        // Anti-churn: several idle ticks later the generation is unchanged.
        let settled = provider.snapshot().generation;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(
            provider.snapshot().generation,
            settled,
            "idle controller ticks must not write"
        );

        // Kill the owner's heartbeats by restarting the controller with a
        // tiny TTL and only refreshing the survivor.
        controller.abort();
        let survivor = if first.owner == "broker-a" {
            "broker-b"
        } else {
            "broker-a"
        };
        let survivor_port = if survivor == "broker-a" { 9000 } else { 9001 };
        tokio::time::sleep(Duration::from_millis(50)).await;
        provider
            .register_self(&broker(survivor, survivor_port))
            .await
            .expect("refresh survivor heartbeat");
        let (controller, _status) = provider.spawn_controller(
            std::sync::Arc::new(DeterministicPartitionPlacement),
            ControllerConfig {
                target_followers: 1,
                tick: Duration::from_millis(100),
                liveness_ttl: Duration::from_millis(900),
                max_cas_retries: 8,
            },
        );

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(assignment) = provider.snapshot().assignment_for("orders", 0, None) {
                    if assignment.owner == survivor {
                        assert_eq!(
                            assignment.epoch,
                            first.epoch + 1,
                            "ownership move must fence"
                        );
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("controller moves ownership off the dead broker");

        controller.abort();
        provider.raft_node().shutdown().await.expect("shutdown");
    }

    /// A runtime-settings update published on one broker becomes
    /// effective on another through the replicated attribute + sync loop,
    /// and concurrent publishers serialize through the attribute CAS.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_settings_replicate_across_brokers() {
        use fibril_broker::queue_engine::{
            KeratinConfig, SnapshotConfig, StromaEngine, StromaKeratinConfig,
        };
        use fibril_broker::runtime_settings::{
            RuntimeSettings, RuntimeSettingsLocks, RuntimeSettingsManager,
        };

        async fn manager(tag: &str) -> std::sync::Arc<RuntimeSettingsManager> {
            let root = std::env::temp_dir().join(format!(
                "fibril-settings-sync-{tag}-{}-{:?}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&root).expect("dir");
            let engine = StromaEngine::open(
                &root,
                StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
                SnapshotConfig::default(),
            )
            .await
            .expect("engine");
            std::sync::Arc::new(
                RuntimeSettingsManager::load_from_stroma_engine(
                    &engine,
                    RuntimeSettings::default(),
                    RuntimeSettingsLocks::default(),
                )
                .await
                .expect("manager"),
            )
        }

        let router = InProcessRouter::new();
        let config = default_raft_config().expect("config");
        let raft_node = RaftMetadataNode::start(1, config, &router)
            .await
            .expect("raft node");
        let mut members = BTreeMap::new();
        members.insert(1u64, ganglion_openraft::openraft::BasicNode::new("n1"));
        raft_node.initialize(members).await.expect("initialize");
        raft_node
            .wait_for_leader(1, Duration::from_secs(10))
            .await
            .expect("election");
        let provider = std::sync::Arc::new(GanglionCoordination::new("broker-a", raft_node));

        let manager_a = manager("a").await;
        let manager_b = manager("b").await;
        provider.spawn_runtime_settings_sync(manager_b.clone());

        // Broker A stores locally (versioned update), then publishes.
        let mut settings = manager_a.current().settings.clone();
        settings.delivery.inflight_ttl_ms = settings.delivery.inflight_ttl_ms.saturating_add(7);
        let stored = manager_a
            .update(manager_a.current().version, settings.clone())
            .await
            .expect("local update");
        assert!(matches!(
            stored,
            fibril_broker::runtime_settings::RuntimeSettingsUpdateOutcome::Stored(_)
        ));
        let version = provider
            .publish_runtime_settings(&settings)
            .await
            .expect("publish");
        assert_eq!(version, 1);

        // Broker B's manager converges via the sync loop.
        tokio::time::timeout(Duration::from_secs(10), async {
            while manager_b.current().settings != settings {
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("replicated settings become effective on broker B");

        // Second publish bumps the cluster version (CAS over the previous doc).
        let version = provider
            .publish_runtime_settings(&settings)
            .await
            .expect("republish");
        assert_eq!(version, 2);

        provider.raft_node().shutdown().await.expect("shutdown");
    }

    /// Candidate selection: with multiple followers, failover prefers the
    /// most caught-up live follower by heartbeat-label applied tails — even
    /// when the planner's sort order would pick another.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failover_prefers_most_caught_up_follower_by_heartbeat_tails() {
        use fibril_broker::coordination::DeterministicPartitionPlacement;

        let router = InProcessRouter::new();
        let config = default_raft_config().expect("config");
        let raft_node = RaftMetadataNode::start(1, config, &router)
            .await
            .expect("raft node");
        let mut members = BTreeMap::new();
        members.insert(1u64, ganglion_openraft::openraft::BasicNode::new("n1"));
        raft_node.initialize(members).await.expect("initialize");
        raft_node
            .wait_for_leader(1, Duration::from_secs(10))
            .await
            .expect("election");
        let provider = GanglionCoordination::new("a-owner", raft_node);

        let queue = QueueIdentity::new("orders", 0, None);
        let info = |id: &str, port: u16| NodeInfo {
            node_id: id.to_string(),
            broker_addr: format!("127.0.0.1:{port}").parse().expect("addr"),
            admin_addr: None,
        };
        let tails = |message: u64, event: u64| {
            let mut labels = std::collections::BTreeMap::new();
            labels.insert(applied_tail_label(&queue), format!("{message}:{event}"));
            labels
        };

        provider
            .register_self(&info("a-owner", 9000))
            .await
            .expect("a");
        // b-slow sorts FIRST among followers but is behind; c-fast is ahead.
        provider
            .register_self_with_labels(&info("b-slow", 9001), tails(5, 5))
            .await
            .expect("b");
        provider
            .register_self_with_labels(&info("c-fast", 9002), tails(9, 9))
            .await
            .expect("c");
        provider.register_queue(&queue).await.expect("queue");

        // Healthy assignment: a owns, b and c follow.
        let all_live = provider.live_nodes(Duration::from_secs(30));
        let committed = provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                2,
                &all_live,
                8,
            )
            .await
            .expect("assign")
            .expect("leader");
        let first = committed
            .assignment_for("orders", 0, None)
            .expect("assigned")
            .clone();
        assert_eq!(first.owner, "a-owner");
        assert_eq!(first.replica_set_size(), 3);

        // The owner dies: only b and c stay live. Sort order says b; tails
        // must say c.
        let mut live = std::collections::HashMap::new();
        live.insert("b-slow".to_string(), info("b-slow", 9001));
        live.insert("c-fast".to_string(), info("c-fast", 9002));
        let committed = provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                2,
                &live,
                8,
            )
            .await
            .expect("failover")
            .expect("leader");
        let moved = committed
            .assignment_for("orders", 0, None)
            .expect("assigned")
            .clone();
        assert_eq!(
            moved.owner, "c-fast",
            "candidate selection must prefer the most caught-up follower"
        );
        assert_eq!(moved.epoch, first.epoch + 1, "the move fences");
        assert!(
            moved.followers.contains(&"b-slow".to_string()),
            "the displaced candidate stays in the replica set: {moved:?}"
        );

        provider.raft_node().shutdown().await.expect("shutdown");
    }

    /// Self-registration merges into the shared node table (no clobbering)
    /// and `live_nodes` filters by heartbeat TTL.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn registration_merges_and_liveness_filters() {
        let router = InProcessRouter::new();
        let config = default_raft_config().expect("config");
        let raft_node = RaftMetadataNode::start(1, config, &router)
            .await
            .expect("raft node");
        let mut members = BTreeMap::new();
        members.insert(1u64, ganglion_openraft::openraft::BasicNode::new("n1"));
        raft_node.initialize(members).await.expect("initialize");
        raft_node
            .wait_for_leader(1, Duration::from_secs(10))
            .await
            .expect("election");
        let provider = GanglionCoordination::new("broker-a", raft_node);

        let broker = |id: &str, port: u16| NodeInfo {
            node_id: id.to_string(),
            broker_addr: format!("127.0.0.1:{port}").parse().expect("addr"),
            admin_addr: None,
        };
        provider
            .register_self(&broker("broker-a", 9000))
            .await
            .expect("register a");
        // Second registration of a different broker must merge, not replace.
        provider
            .register_self(&broker("broker-b", 9001))
            .await
            .expect("register b");

        let live = provider.live_nodes(Duration::from_secs(30));
        assert_eq!(live.len(), 2, "both registrations visible: {live:?}");

        // Zero TTL: heartbeats in the past are dead (allow 5ms of clock step).
        tokio::time::sleep(Duration::from_millis(10)).await;
        let dead = provider.live_nodes(Duration::from_millis(1));
        assert!(dead.is_empty(), "expired heartbeats filtered: {dead:?}");

        // The watch/trait surface sees the registrations too.
        assert_eq!(provider.snapshot().nodes.len(), 2);

        provider.raft_node().shutdown().await.expect("shutdown");
    }

    /// The ganglion provider must satisfy the same contract as every other
    /// `Coordination` implementation (see `fibril_broker::coordination::contract_tests`).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ganglion_provider_passes_provider_contract() {
        let router = InProcessRouter::new();
        let config = default_raft_config().expect("config");
        let raft_node = RaftMetadataNode::start(1, config, &router)
            .await
            .expect("raft node should start");
        let mut members = BTreeMap::new();
        members.insert(
            1u64,
            ganglion_openraft::openraft::BasicNode::new("contract-node"),
        );
        raft_node.initialize(members).await.expect("initialize");
        raft_node
            .wait_for_leader(1, Duration::from_secs(10))
            .await
            .expect("election");

        let coordination = GanglionCoordination::new("contract-node", raft_node);

        // The contract callback is sync; bridge each commit through the async
        // propose + wait-for-visibility path on a separate runtime handle.
        let handle = tokio::runtime::Handle::current();
        let provider = coordination;
        tokio::task::spawn_blocking(move || {
            fibril_broker::coordination::contract_tests::assert_coordination_contract(
                &provider,
                "contract-node",
                |provider, snapshot| {
                    let generation = snapshot.generation;
                    handle.block_on(async {
                        provider
                            .propose(&snapshot)
                            .await
                            .expect("contract commit should succeed");
                        let mut watch = provider.watch();
                        tokio::time::timeout(Duration::from_secs(10), async {
                            while watch.borrow_and_update().generation < generation {
                                watch.changed().await.expect("watch open");
                            }
                        })
                        .await
                        .expect("committed snapshot should become visible");
                    });
                },
            );
            provider
        })
        .await
        .expect("contract suite should pass")
        .raft_node()
        .shutdown()
        .await
        .expect("shutdown");
    }
}
