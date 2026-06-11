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
    QueueIdentity, ReplicationDurabilityPolicy,
};
use ganglion_openraft::openraft::storage::RaftLogStorage;
use ganglion_openraft::{
    GanglionLogStore, GanglionRaftConfig, MetadataRaftResponse, OpenraftAdapterError,
    RaftMetadataNode,
};
use tokio::sync::watch;

/// Namespace tag used for fibril queues inside ganglion resource identities.
const QUEUE_NAMESPACE: &str = "fibril/queue";

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

/// Fibril `Coordination` provider backed by a ganglion raft node.
///
/// Reads serve from a fibril-side watch channel fed by ganglion's
/// committed-snapshot stream; a background task forwards updates. Proposals go
/// through [`GanglionCoordination::propose`], which is leader-only.
pub struct GanglionCoordination<LS = GanglionLogStore>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    node_id: String,
    node: RaftMetadataNode<LS>,
    tx: watch::Sender<CoordinationSnapshot>,
    forwarder: tokio::task::JoinHandle<()>,
}

impl<LS> std::fmt::Debug for GanglionCoordination<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GanglionCoordination")
            .field("node_id", &self.node_id)
            .field("raft_node_id", &self.node.node_id())
            .finish_non_exhaustive()
    }
}

impl<LS> GanglionCoordination<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    /// Wrap a started raft node as a fibril coordination provider.
    ///
    /// `node_id` is the fibril-side node identity (string); the raft node id
    /// (u64) is an internal transport concern and need not match.
    ///
    /// Must be called from within a tokio runtime (spawns the watch forwarder).
    pub fn new(node_id: impl Into<String>, node: RaftMetadataNode<LS>) -> Self {
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
        }
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

    /// Access the underlying raft node (membership changes, waits, shutdown).
    pub fn raft_node(&self) -> &RaftMetadataNode<LS> {
        &self.node
    }
}

impl<LS> Drop for GanglionCoordination<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
{
    fn drop(&mut self) {
        self.forwarder.abort();
    }
}

impl<LS> Coordination for GanglionCoordination<LS>
where
    LS: RaftLogStorage<GanglionRaftConfig>,
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
