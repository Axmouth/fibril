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
    GanglionLogStore, GanglionRaftConfig, InProcessRouter, MetadataRaftResponse,
    OpenraftAdapterError, RaftMetadataNode,
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
            desired.generation = committed.generation + 1;
            ganglion_core::stamp_assignment_epochs(&committed, &mut desired);

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

    /// F2 choreography: the raft leader IS the controller. The controller
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
