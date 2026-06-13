use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use fibril_storage::{Group, LogId, Topic};
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueIdentity {
    pub topic: Topic,
    pub partition: LogId,
    pub group: Option<Group>,
}

impl QueueIdentity {
    pub fn new(topic: impl Into<Topic>, partition: LogId, group: Option<&str>) -> Self {
        Self {
            topic: topic.into(),
            partition,
            group: group.map(str::to_string),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
    pub node_id: String,
    pub broker_addr: SocketAddr,
    pub admin_addr: Option<SocketAddr>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionAssignment {
    pub queue: QueueIdentity,
    pub owner: String,
    pub followers: Vec<String>,
    pub epoch: u64,
    pub durability: ReplicationDurabilityPolicy,
}

impl PartitionAssignment {
    pub fn new(
        queue: QueueIdentity,
        owner: impl Into<String>,
        followers: Vec<String>,
        epoch: u64,
    ) -> Self {
        Self {
            queue,
            owner: owner.into(),
            followers,
            epoch,
            durability: ReplicationDurabilityPolicy::LocalDurable,
        }
    }

    pub fn with_durability(mut self, durability: ReplicationDurabilityPolicy) -> Self {
        self.durability = durability;
        self
    }

    pub fn is_owned_by(&self, node_id: &str) -> bool {
        self.owner == node_id
    }

    pub fn is_followed_by(&self, node_id: &str) -> bool {
        self.followers.iter().any(|follower| follower == node_id)
    }

    pub fn replica_set_size(&self) -> usize {
        1 + self.followers.len()
    }

    pub fn durability_requirement(
        &self,
    ) -> Result<ReplicationDurabilityRequirement, ReplicationDurabilityError> {
        self.durability.resolve(self.replica_set_size())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationDurabilityPolicy {
    /// Confirm after the local owner has durably written the append.
    LocalDurable,
    /// Confirm after this many assigned nodes have accepted the append.
    ///
    /// `nodes` includes the owner. Acceptance is weaker than durable fsync.
    ReplicaAccepted { nodes: usize },
    /// Confirm after this many assigned nodes have durably written the append.
    ///
    /// `nodes` includes the owner.
    ReplicaDurable { nodes: usize },
    /// Confirm after a durable majority of the assigned replica set.
    MajorityDurable,
}

impl Default for ReplicationDurabilityPolicy {
    fn default() -> Self {
        Self::LocalDurable
    }
}

impl ReplicationDurabilityPolicy {
    pub fn resolve(
        self,
        available_nodes: usize,
    ) -> Result<ReplicationDurabilityRequirement, ReplicationDurabilityError> {
        let requirement = match self {
            Self::LocalDurable => ReplicationDurabilityRequirement {
                nodes: 1,
                acknowledgement: ReplicationAcknowledgement::Durable,
            },
            Self::ReplicaAccepted { nodes } => ReplicationDurabilityRequirement {
                nodes,
                acknowledgement: ReplicationAcknowledgement::Accepted,
            },
            Self::ReplicaDurable { nodes } => ReplicationDurabilityRequirement {
                nodes,
                acknowledgement: ReplicationAcknowledgement::Durable,
            },
            Self::MajorityDurable => ReplicationDurabilityRequirement {
                nodes: (available_nodes / 2) + 1,
                acknowledgement: ReplicationAcknowledgement::Durable,
            },
        };

        requirement.validate(available_nodes)?;
        Ok(requirement)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplicationDurabilityRequirement {
    pub nodes: usize,
    pub acknowledgement: ReplicationAcknowledgement,
}

impl ReplicationDurabilityRequirement {
    fn validate(self, available_nodes: usize) -> Result<(), ReplicationDurabilityError> {
        if self.nodes == 0 {
            return Err(ReplicationDurabilityError::ZeroNodes);
        }

        if self.nodes > available_nodes {
            return Err(ReplicationDurabilityError::NotEnoughAssignedNodes {
                required: self.nodes,
                available: available_nodes,
            });
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationAcknowledgement {
    Accepted,
    Durable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicationDurabilityError {
    ZeroNodes,
    NotEnoughAssignedNodes { required: usize, available: usize },
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CoordinationSnapshot {
    pub nodes: HashMap<String, NodeInfo>,
    pub assignments: HashMap<QueueIdentity, PartitionAssignment>,
    pub generation: u64,
}

impl CoordinationSnapshot {
    pub fn assignment_for(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
    ) -> Option<&PartitionAssignment> {
        self.assignments
            .get(&QueueIdentity::new(topic, partition, group))
    }

    pub fn owned_by(&self, node_id: &str) -> Vec<PartitionAssignment> {
        self.assignments
            .values()
            .filter(|assignment| assignment.is_owned_by(node_id))
            .cloned()
            .collect()
    }

    pub fn followed_by(&self, node_id: &str) -> Vec<PartitionAssignment> {
        self.assignments
            .values()
            .filter(|assignment| assignment.is_followed_by(node_id))
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPlacementInput {
    pub nodes: HashMap<String, NodeInfo>,
    pub queues: Vec<QueueIdentity>,
    pub existing: HashMap<QueueIdentity, PartitionAssignment>,
    pub target_followers: usize,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionPlacementPlan {
    pub snapshot: CoordinationSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionPlacementError {
    NoNodesForQueues,
}

pub trait PartitionPlacementPolicy: std::fmt::Debug + Send + Sync {
    fn plan(
        &self,
        input: PartitionPlacementInput,
    ) -> Result<PartitionPlacementPlan, PartitionPlacementError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeterministicPartitionPlacement;

impl PartitionPlacementPolicy for DeterministicPartitionPlacement {
    fn plan(
        &self,
        input: PartitionPlacementInput,
    ) -> Result<PartitionPlacementPlan, PartitionPlacementError> {
        let mut queues = input.queues;
        queues.sort_by_key(queue_sort_key);
        queues.dedup();

        let nodes = input.nodes;
        if queues.is_empty() {
            return Ok(PartitionPlacementPlan {
                snapshot: CoordinationSnapshot {
                    nodes,
                    assignments: HashMap::new(),
                    generation: input.generation,
                },
            });
        }

        let node_ids = sorted_node_ids(&nodes);
        if node_ids.is_empty() {
            return Err(PartitionPlacementError::NoNodesForQueues);
        }

        let mut assignments = HashMap::with_capacity(queues.len());
        for (idx, queue) in queues.into_iter().enumerate() {
            let existing = input.existing.get(&queue);
            let owner = existing
                .and_then(|assignment| {
                    nodes
                        .contains_key(&assignment.owner)
                        .then_some(&assignment.owner)
                })
                .cloned()
                .unwrap_or_else(|| node_ids[idx % node_ids.len()].clone());
            let followers = plan_followers(
                &node_ids,
                &owner,
                existing
                    .map(|assignment| assignment.followers.as_slice())
                    .unwrap_or(&[]),
                input.target_followers,
            );

            let durability = existing
                .map(|assignment| assignment.durability)
                .unwrap_or_default();
            let epoch = existing
                .filter(|assignment| assignment.owner == owner && assignment.followers == followers)
                .map(|assignment| assignment.epoch)
                .unwrap_or(input.generation);

            assignments.insert(
                queue.clone(),
                PartitionAssignment::new(queue, owner, followers, epoch)
                    .with_durability(durability),
            );
        }

        Ok(PartitionPlacementPlan {
            snapshot: CoordinationSnapshot {
                nodes,
                assignments,
                generation: input.generation,
            },
        })
    }
}

fn sorted_node_ids(nodes: &HashMap<String, NodeInfo>) -> Vec<String> {
    let mut node_ids: Vec<_> = nodes.keys().cloned().collect();
    node_ids.sort();
    node_ids
}

fn plan_followers(
    node_ids: &[String],
    owner: &str,
    existing_followers: &[String],
    target_followers: usize,
) -> Vec<String> {
    if target_followers == 0 {
        return Vec::new();
    }

    let mut followers = Vec::new();
    let mut seen = HashSet::new();
    for follower in existing_followers {
        if follower == owner || !node_ids.iter().any(|node| node == follower) {
            continue;
        }
        if seen.insert(follower.clone()) {
            followers.push(follower.clone());
        }
        if followers.len() == target_followers {
            return followers;
        }
    }

    let owner_index = node_ids
        .iter()
        .position(|node| node == owner)
        .unwrap_or_default();
    for step in 1..node_ids.len() {
        if followers.len() == target_followers {
            break;
        }
        let candidate = &node_ids[(owner_index + step) % node_ids.len()];
        if candidate == owner {
            continue;
        }
        if seen.insert(candidate.clone()) {
            followers.push(candidate.clone());
        }
    }

    followers
}

fn queue_sort_key(queue: &QueueIdentity) -> (String, LogId, String) {
    (
        queue.topic.to_string(),
        queue.partition,
        queue.group.as_deref().unwrap_or_default().to_string(),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAssignmentRole {
    Owner,
    Follower,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAssignmentIntent {
    Noop,
    BecomeOwner,
    BecomeFollower,
    PromoteFollowerToOwner,
    DemoteOwnerToFollower,
    FreezeOwner,
    StopFollower,
    RefreshOwner,
    RefreshFollower,
}

impl LocalAssignmentIntent {
    pub fn requires_role_change(self) -> bool {
        !matches!(
            self,
            Self::Noop | Self::RefreshOwner | Self::RefreshFollower
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalAssignmentTransition {
    pub queue: QueueIdentity,
    pub previous_role: Option<LocalAssignmentRole>,
    pub next_role: Option<LocalAssignmentRole>,
    pub previous: Option<PartitionAssignment>,
    pub next: Option<PartitionAssignment>,
    pub intent: LocalAssignmentIntent,
}

pub fn plan_local_assignment_transitions(
    node_id: &str,
    previous: &CoordinationSnapshot,
    next: &CoordinationSnapshot,
) -> Vec<LocalAssignmentTransition> {
    let mut keys: HashSet<QueueIdentity> = previous.assignments.keys().cloned().collect();
    keys.extend(next.assignments.keys().cloned());

    let mut keys: Vec<_> = keys.into_iter().collect();
    keys.sort_by(|a, b| {
        (
            a.topic.to_string(),
            a.partition,
            a.group.as_deref().unwrap_or_default().to_string(),
        )
            .cmp(&(
                b.topic.to_string(),
                b.partition,
                b.group.as_deref().unwrap_or_default().to_string(),
            ))
    });

    keys.into_iter()
        .filter_map(|queue| {
            let previous_assignment = previous.assignments.get(&queue).cloned();
            let next_assignment = next.assignments.get(&queue).cloned();
            let previous_role = previous_assignment
                .as_ref()
                .and_then(|assignment| local_role_for(node_id, assignment));
            let next_role = next_assignment
                .as_ref()
                .and_then(|assignment| local_role_for(node_id, assignment));
            let intent = local_assignment_intent(
                previous_role,
                next_role,
                previous_assignment.as_ref(),
                next_assignment.as_ref(),
            );

            (intent != LocalAssignmentIntent::Noop).then_some(LocalAssignmentTransition {
                queue,
                previous_role,
                next_role,
                previous: previous_assignment,
                next: next_assignment,
                intent,
            })
        })
        .collect()
}

fn local_role_for(node_id: &str, assignment: &PartitionAssignment) -> Option<LocalAssignmentRole> {
    if assignment.is_owned_by(node_id) {
        Some(LocalAssignmentRole::Owner)
    } else if assignment.is_followed_by(node_id) {
        Some(LocalAssignmentRole::Follower)
    } else {
        None
    }
}

fn local_assignment_intent(
    previous_role: Option<LocalAssignmentRole>,
    next_role: Option<LocalAssignmentRole>,
    previous: Option<&PartitionAssignment>,
    next: Option<&PartitionAssignment>,
) -> LocalAssignmentIntent {
    match (previous_role, next_role) {
        (None, None) => LocalAssignmentIntent::Noop,
        (None, Some(LocalAssignmentRole::Owner)) => LocalAssignmentIntent::BecomeOwner,
        (None, Some(LocalAssignmentRole::Follower)) => LocalAssignmentIntent::BecomeFollower,
        (Some(LocalAssignmentRole::Owner), None) => LocalAssignmentIntent::FreezeOwner,
        (Some(LocalAssignmentRole::Follower), None) => LocalAssignmentIntent::StopFollower,
        (Some(LocalAssignmentRole::Owner), Some(LocalAssignmentRole::Follower)) => {
            LocalAssignmentIntent::DemoteOwnerToFollower
        }
        (Some(LocalAssignmentRole::Follower), Some(LocalAssignmentRole::Owner)) => {
            LocalAssignmentIntent::PromoteFollowerToOwner
        }
        (Some(LocalAssignmentRole::Owner), Some(LocalAssignmentRole::Owner)) => {
            if previous == next {
                LocalAssignmentIntent::Noop
            } else {
                LocalAssignmentIntent::RefreshOwner
            }
        }
        (Some(LocalAssignmentRole::Follower), Some(LocalAssignmentRole::Follower)) => {
            if previous == next {
                LocalAssignmentIntent::Noop
            } else {
                LocalAssignmentIntent::RefreshFollower
            }
        }
    }
}

pub type CoordinationStream = watch::Receiver<CoordinationSnapshot>;

pub trait Coordination: std::fmt::Debug + Send + Sync {
    fn node_id(&self) -> &str;
    fn snapshot(&self) -> CoordinationSnapshot;
    fn watch(&self) -> CoordinationStream;

    fn assignment_for(
        &self,
        topic: &str,
        partition: LogId,
        group: Option<&str>,
    ) -> Option<PartitionAssignment> {
        self.snapshot()
            .assignment_for(topic, partition, group)
            .cloned()
    }

    fn owns_queue(&self, topic: &str, partition: LogId, group: Option<&str>) -> bool {
        self.assignment_for(topic, partition, group)
            .is_some_and(|assignment| assignment.is_owned_by(self.node_id()))
    }

    fn follows_queue(&self, topic: &str, partition: LogId, group: Option<&str>) -> bool {
        self.assignment_for(topic, partition, group)
            .is_some_and(|assignment| assignment.is_followed_by(self.node_id()))
    }

    fn owner_for(&self, topic: &str, partition: LogId, group: Option<&str>) -> Option<NodeInfo> {
        let snapshot = self.snapshot();
        let owner = snapshot
            .assignment_for(topic, partition, group)?
            .owner
            .clone();
        snapshot.nodes.get(&owner).cloned()
    }

    fn follower_assignments(&self) -> Vec<PartitionAssignment> {
        self.snapshot().followed_by(self.node_id())
    }

    fn owned_assignments(&self) -> Vec<PartitionAssignment> {
        self.snapshot().owned_by(self.node_id())
    }
}

#[derive(Debug, Clone)]
pub struct StaticCoordination {
    node_id: String,
    snapshot: Arc<RwLock<CoordinationSnapshot>>,
    tx: watch::Sender<CoordinationSnapshot>,
}

impl StaticCoordination {
    pub fn new(node_id: impl Into<String>, snapshot: CoordinationSnapshot) -> Self {
        let (tx, _rx) = watch::channel(snapshot.clone());
        Self {
            node_id: node_id.into(),
            snapshot: Arc::new(RwLock::new(snapshot)),
            tx,
        }
    }

    pub fn single_node(node_id: impl Into<String>, broker_addr: SocketAddr) -> Self {
        let node_id = node_id.into();
        let mut nodes = HashMap::new();
        nodes.insert(
            node_id.clone(),
            NodeInfo {
                node_id: node_id.clone(),
                broker_addr,
                admin_addr: None,
            },
        );
        Self::new(
            node_id,
            CoordinationSnapshot {
                nodes,
                assignments: HashMap::new(),
                generation: 0,
            },
        )
    }

    pub fn update_snapshot(&self, snapshot: CoordinationSnapshot) {
        *self
            .snapshot
            .write()
            .expect("coordination snapshot poisoned") = snapshot.clone();
        let _ = self.tx.send(snapshot);
    }
}

impl Coordination for StaticCoordination {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn snapshot(&self) -> CoordinationSnapshot {
        self.snapshot
            .read()
            .expect("coordination snapshot poisoned")
            .clone()
    }

    fn watch(&self) -> CoordinationStream {
        self.tx.subscribe()
    }
}

/// Single-node default used before cluster coordination is configured.
///
/// This intentionally reports ownership for every queue, matching the current
/// standalone broker behavior.
#[derive(Debug, Clone)]
pub struct NoopCoordination {
    inner: StaticCoordination,
}

impl Default for NoopCoordination {
    fn default() -> Self {
        Self {
            inner: StaticCoordination::single_node(
                "local",
                "127.0.0.1:0".parse().expect("valid loopback socket"),
            ),
        }
    }
}

impl Coordination for NoopCoordination {
    fn node_id(&self) -> &str {
        self.inner.node_id()
    }

    fn snapshot(&self) -> CoordinationSnapshot {
        self.inner.snapshot()
    }

    fn watch(&self) -> CoordinationStream {
        self.inner.watch()
    }

    fn owns_queue(&self, _topic: &str, _partition: LogId, _group: Option<&str>) -> bool {
        true
    }
}

/// Reusable contract assertions every `Coordination` provider must satisfy.
///
/// Run this against each implementation (static, ganglion, later etcd) so
/// providers cannot drift apart. The `commit` callback applies a new snapshot
/// through the provider's own write path and must not return until the
/// provider's reads observe it (e.g. static: `update_snapshot`; ganglion:
/// propose through raft and wait on the watch).
#[cfg(any(test, feature = "provider-contract-tests"))]
pub mod contract_tests {
    use super::*;

    fn sample_snapshot(local: &str, generation: u64, owner: &str) -> CoordinationSnapshot {
        let mut nodes = HashMap::new();
        for (node_id, port) in [(local, 9101u16), (owner, 9102), ("other-node", 9103)] {
            nodes.insert(
                node_id.to_string(),
                NodeInfo {
                    node_id: node_id.to_string(),
                    broker_addr: format!("127.0.0.1:{port}").parse().expect("addr"),
                    admin_addr: None,
                },
            );
        }

        let owned = QueueIdentity::new("contract-owned", 0, Some("workers"));
        let followed = QueueIdentity::new("contract-followed", 1, None);
        let mut assignments = HashMap::new();
        assignments.insert(
            owned.clone(),
            PartitionAssignment::new(owned, owner, vec!["other-node".to_string()], generation),
        );
        assignments.insert(
            followed.clone(),
            PartitionAssignment::new(followed, "other-node", vec![local.to_string()], 1),
        );

        CoordinationSnapshot {
            nodes,
            assignments,
            generation,
        }
    }

    /// Assert the shared provider contract.
    ///
    /// Preconditions: the provider starts on an empty/default snapshot and
    /// `commit(provider, snapshot)` makes `snapshot` the committed state,
    /// visible to reads, before returning.
    pub fn assert_coordination_contract<P, F>(provider: &P, local_node_id: &str, mut commit: F)
    where
        P: Coordination,
        F: FnMut(&P, CoordinationSnapshot),
    {
        // Identity is stable.
        assert_eq!(provider.node_id(), local_node_id);

        // Empty state: queries answer absent, not panic.
        assert!(
            provider
                .assignment_for("contract-owned", 0, Some("workers"))
                .is_none()
        );
        assert!(!provider.owns_queue("contract-owned", 0, Some("workers")));
        assert!(!provider.follows_queue("contract-followed", 1, None));
        assert!(
            provider
                .owner_for("contract-owned", 0, Some("workers"))
                .is_none()
        );
        assert!(provider.owned_assignments().is_empty());
        assert!(provider.follower_assignments().is_empty());

        // Subscribe BEFORE the first commit: the stream must deliver the
        // latest committed value to pre-existing subscribers.
        let mut stream = provider.watch();

        // Commit a snapshot where the local node owns one queue and follows
        // another.
        let first = sample_snapshot(local_node_id, 1, local_node_id);
        commit(provider, first.clone());

        assert_eq!(provider.snapshot(), first);
        assert!(provider.owns_queue("contract-owned", 0, Some("workers")));
        assert!(!provider.follows_queue("contract-owned", 0, Some("workers")));
        assert!(provider.follows_queue("contract-followed", 1, None));
        let owner = provider
            .owner_for("contract-owned", 0, Some("workers"))
            .expect("owner must resolve from the committed snapshot");
        assert_eq!(owner.node_id, local_node_id);
        assert_eq!(provider.owned_assignments().len(), 1);
        assert_eq!(provider.follower_assignments().len(), 1);
        let assignment = provider
            .assignment_for("contract-owned", 0, Some("workers"))
            .expect("assignment must resolve");
        assert_eq!(assignment.epoch, 1);

        // Ownership moves away: role queries must flip with the snapshot.
        let second = sample_snapshot(local_node_id, 2, "other-node");
        commit(provider, second.clone());

        assert_eq!(provider.snapshot(), second);
        assert!(!provider.owns_queue("contract-owned", 0, Some("workers")));
        let moved = provider
            .assignment_for("contract-owned", 0, Some("workers"))
            .expect("assignment still present");
        assert_eq!(moved.owner, "other-node");

        // The pre-commit subscriber observes the latest committed state (the
        // channel may coalesce intermediate values, but never lose the last).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if stream.borrow_and_update().generation >= 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "watch subscriber never observed the committed snapshot"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert_eq!(stream.borrow().clone(), second);

        // A fresh subscriber starts at the committed state immediately.
        let fresh = provider.watch();
        assert_eq!(fresh.borrow().clone(), second);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> CoordinationSnapshot {
        let mut nodes = HashMap::new();
        nodes.insert(
            "node-a".to_string(),
            NodeInfo {
                node_id: "node-a".to_string(),
                broker_addr: "127.0.0.1:1001".parse().unwrap(),
                admin_addr: Some("127.0.0.1:2001".parse().unwrap()),
            },
        );
        nodes.insert(
            "node-b".to_string(),
            NodeInfo {
                node_id: "node-b".to_string(),
                broker_addr: "127.0.0.1:1002".parse().unwrap(),
                admin_addr: None,
            },
        );

        let queue = QueueIdentity::new("emails", 0, Some("workers"));
        let mut assignments = HashMap::new();
        assignments.insert(
            queue.clone(),
            PartitionAssignment::new(queue, "node-a", vec!["node-b".to_string()], 7),
        );

        CoordinationSnapshot {
            nodes,
            assignments,
            generation: 42,
        }
    }

    #[test]
    fn static_coordination_passes_provider_contract() {
        let provider = StaticCoordination::new(
            "contract-node",
            CoordinationSnapshot {
                nodes: HashMap::new(),
                assignments: HashMap::new(),
                generation: 0,
            },
        );
        contract_tests::assert_coordination_contract(
            &provider,
            "contract-node",
            |provider, snapshot| provider.update_snapshot(snapshot),
        );
    }

    #[test]
    fn static_coordination_reports_owner_and_followers() {
        let coordination = StaticCoordination::new("node-a", snapshot());

        assert!(coordination.owns_queue("emails", 0, Some("workers")));
        assert!(!coordination.follows_queue("emails", 0, Some("workers")));

        let owner = coordination
            .owner_for("emails", 0, Some("workers"))
            .expect("owner node");
        assert_eq!(owner.node_id, "node-a");
        assert_eq!(owner.broker_addr, "127.0.0.1:1001".parse().unwrap());

        let owned = coordination.owned_assignments();
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].epoch, 7);
    }

    #[test]
    fn static_coordination_reports_local_follow_assignments() {
        let coordination = StaticCoordination::new("node-b", snapshot());

        assert!(!coordination.owns_queue("emails", 0, Some("workers")));
        assert!(coordination.follows_queue("emails", 0, Some("workers")));

        let followed = coordination.follower_assignments();
        assert_eq!(followed.len(), 1);
        assert_eq!(followed[0].queue.topic.to_string(), "emails");
        assert_eq!(followed[0].epoch, 7);
    }

    #[test]
    fn partition_assignment_defaults_to_local_durable_policy() {
        let assignment = assignment("emails", "node-a", vec!["node-b"], 1);

        assert_eq!(assignment.replica_set_size(), 2);
        assert_eq!(
            assignment.durability_requirement().unwrap(),
            ReplicationDurabilityRequirement {
                nodes: 1,
                acknowledgement: ReplicationAcknowledgement::Durable,
            }
        );
    }

    #[test]
    fn replication_durability_policy_distinguishes_accepted_and_durable() {
        let accepted = assignment("accepted", "node-a", vec!["node-b", "node-c"], 1)
            .with_durability(ReplicationDurabilityPolicy::ReplicaAccepted { nodes: 2 });
        let durable = assignment("durable", "node-a", vec!["node-b", "node-c"], 1)
            .with_durability(ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 });

        assert_eq!(
            accepted.durability_requirement().unwrap(),
            ReplicationDurabilityRequirement {
                nodes: 2,
                acknowledgement: ReplicationAcknowledgement::Accepted,
            }
        );
        assert_eq!(
            durable.durability_requirement().unwrap(),
            ReplicationDurabilityRequirement {
                nodes: 2,
                acknowledgement: ReplicationAcknowledgement::Durable,
            }
        );
    }

    #[test]
    fn majority_durable_uses_assigned_replica_set_size() {
        let one = assignment("one", "node-a", vec![], 1)
            .with_durability(ReplicationDurabilityPolicy::MajorityDurable);
        let two = assignment("two", "node-a", vec!["node-b"], 1)
            .with_durability(ReplicationDurabilityPolicy::MajorityDurable);
        let three = assignment("three", "node-a", vec!["node-b", "node-c"], 1)
            .with_durability(ReplicationDurabilityPolicy::MajorityDurable);
        let four = assignment("four", "node-a", vec!["node-b", "node-c", "node-d"], 1)
            .with_durability(ReplicationDurabilityPolicy::MajorityDurable);

        assert_eq!(one.durability_requirement().unwrap().nodes, 1);
        assert_eq!(two.durability_requirement().unwrap().nodes, 2);
        assert_eq!(three.durability_requirement().unwrap().nodes, 2);
        assert_eq!(four.durability_requirement().unwrap().nodes, 3);
    }

    #[test]
    fn replication_durability_policy_rejects_invalid_requirements() {
        let zero = assignment("zero", "node-a", vec!["node-b"], 1)
            .with_durability(ReplicationDurabilityPolicy::ReplicaAccepted { nodes: 0 });
        let too_many = assignment("too-many", "node-a", vec!["node-b"], 1)
            .with_durability(ReplicationDurabilityPolicy::ReplicaDurable { nodes: 3 });

        assert_eq!(
            zero.durability_requirement().unwrap_err(),
            ReplicationDurabilityError::ZeroNodes
        );
        assert_eq!(
            too_many.durability_requirement().unwrap_err(),
            ReplicationDurabilityError::NotEnoughAssignedNodes {
                required: 3,
                available: 2,
            }
        );
    }

    #[tokio::test]
    async fn static_coordination_watch_observes_snapshot_update() {
        let coordination = StaticCoordination::new("node-a", snapshot());
        let mut watch = coordination.watch();

        let mut next = snapshot();
        next.generation = 43;
        coordination.update_snapshot(next);

        watch.changed().await.unwrap();
        assert_eq!(watch.borrow().generation, 43);
    }

    #[test]
    fn placement_allows_empty_queue_set_without_nodes() {
        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: HashMap::new(),
                queues: Vec::new(),
                existing: HashMap::new(),
                target_followers: 2,
                generation: 9,
            })
            .unwrap();

        assert!(plan.snapshot.nodes.is_empty());
        assert!(plan.snapshot.assignments.is_empty());
        assert_eq!(plan.snapshot.generation, 9);
    }

    #[test]
    fn placement_requires_nodes_when_queues_exist() {
        let err = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: HashMap::new(),
                queues: vec![QueueIdentity::new("emails", 0, None)],
                existing: HashMap::new(),
                target_followers: 1,
                generation: 1,
            })
            .unwrap_err();

        assert_eq!(err, PartitionPlacementError::NoNodesForQueues);
    }

    #[test]
    fn placement_assigns_deterministic_round_robin_owners() {
        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: nodes(["node-b", "node-a"]),
                queues: vec![
                    QueueIdentity::new("orders", 0, None),
                    QueueIdentity::new("emails", 0, None),
                    QueueIdentity::new("tasks", 0, None),
                ],
                existing: HashMap::new(),
                target_followers: 0,
                generation: 3,
            })
            .unwrap();

        assert_eq!(assignment_owner(&plan.snapshot, "emails"), "node-a");
        assert_eq!(assignment_owner(&plan.snapshot, "orders"), "node-b");
        assert_eq!(assignment_owner(&plan.snapshot, "tasks"), "node-a");
    }

    #[test]
    fn placement_caps_followers_and_excludes_owner() {
        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: nodes(["node-a", "node-b", "node-c"]),
                queues: vec![QueueIdentity::new("emails", 0, None)],
                existing: HashMap::new(),
                target_followers: 8,
                generation: 4,
            })
            .unwrap();

        let assignment = plan
            .snapshot
            .assignment_for("emails", 0, None)
            .expect("assignment");
        assert_eq!(assignment.owner, "node-a");
        assert_eq!(assignment.followers, vec!["node-b", "node-c"]);
    }

    #[test]
    fn placement_preserves_existing_owner_and_fills_followers() {
        let queue = QueueIdentity::new("emails", 0, None);
        let existing_assignment =
            PartitionAssignment::new(queue.clone(), "node-b", vec!["node-c".to_string()], 11)
                .with_durability(ReplicationDurabilityPolicy::MajorityDurable);
        let existing = HashMap::from([(queue.clone(), existing_assignment)]);

        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: nodes(["node-a", "node-b", "node-c", "node-d"]),
                queues: vec![queue],
                existing,
                target_followers: 2,
                generation: 12,
            })
            .unwrap();

        let assignment = plan
            .snapshot
            .assignment_for("emails", 0, None)
            .expect("assignment");
        assert_eq!(assignment.owner, "node-b");
        assert_eq!(assignment.followers, vec!["node-c", "node-d"]);
        assert_eq!(assignment.epoch, 12);
        assert_eq!(
            assignment.durability,
            ReplicationDurabilityPolicy::MajorityDurable
        );
    }

    #[test]
    fn placement_preserves_epoch_when_assignment_does_not_change() {
        let queue = QueueIdentity::new("emails", 0, None);
        let existing_assignment =
            PartitionAssignment::new(queue.clone(), "node-a", vec!["node-b".to_string()], 7);
        let existing = HashMap::from([(queue.clone(), existing_assignment)]);

        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: nodes(["node-a", "node-b"]),
                queues: vec![queue],
                existing,
                target_followers: 1,
                generation: 8,
            })
            .unwrap();

        let assignment = plan
            .snapshot
            .assignment_for("emails", 0, None)
            .expect("assignment");
        assert_eq!(assignment.owner, "node-a");
        assert_eq!(assignment.followers, vec!["node-b"]);
        assert_eq!(assignment.epoch, 7);
    }

    fn nodes_n(count: usize) -> HashMap<String, NodeInfo> {
        (0..count)
            .map(|idx| {
                let node_id = format!("node-{idx:04}");
                (
                    node_id.clone(),
                    NodeInfo {
                        node_id,
                        broker_addr: format!("127.0.0.1:{}", 10_000 + idx).parse().unwrap(),
                        admin_addr: None,
                    },
                )
            })
            .collect()
    }

    fn queues_prefixed(prefix: &str, count: usize) -> Vec<QueueIdentity> {
        (0..count)
            .map(|idx| {
                // Spread across topics, partitions, and groups so the sort/dedup
                // and round-robin paths see realistic key variety at scale. The
                // prefix keeps separate batches from colliding.
                let topic = format!("{prefix}-{:03}", idx % 64);
                let partition = (idx % 4) as LogId;
                let group = if idx % 3 == 0 {
                    None
                } else {
                    Some(format!("group-{}", idx % 7))
                };
                QueueIdentity::new(topic, partition, group.as_deref())
            })
            .collect()
    }

    fn queues_n(count: usize) -> Vec<QueueIdentity> {
        queues_prefixed("topic", count)
    }

    /// Shared invariant check: every queue is placed on a live node with a
    /// distinct, owner-excluded follower set capped at the available nodes.
    fn assert_placement_well_formed(
        assignments: &HashMap<QueueIdentity, PartitionAssignment>,
        live: &HashMap<String, NodeInfo>,
        target_followers: usize,
    ) {
        let expected_followers = target_followers.min(live.len().saturating_sub(1));
        for assignment in assignments.values() {
            assert!(live.contains_key(&assignment.owner));
            assert_eq!(assignment.followers.len(), expected_followers);
            let distinct: HashSet<_> = assignment.followers.iter().collect();
            assert_eq!(distinct.len(), expected_followers);
            assert!(!assignment.followers.contains(&assignment.owner));
            for follower in &assignment.followers {
                assert!(live.contains_key(follower));
            }
        }
    }

    /// Sanity check that placement stays correct and balanced at cluster sizes
    /// far beyond the small fixtures: every queue placed on live nodes with the
    /// requested replica count, owner load balanced within one, re-planning is
    /// a no-op, and a mass node failure rebalances only the orphaned queues
    /// while leaving survivors untouched.
    #[test]
    fn placement_scales_to_large_clusters() {
        const NODE_COUNT: usize = 75;
        const TARGET_FOLLOWERS: usize = 2;

        let live = nodes_n(NODE_COUNT);
        let queues = queues_n(600);
        let unique_queues: HashSet<_> = queues.iter().cloned().collect();

        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: live.clone(),
                queues: queues.clone(),
                existing: HashMap::new(),
                target_followers: TARGET_FOLLOWERS,
                generation: 1,
            })
            .unwrap();
        let assignments = plan.snapshot.assignments;

        // Every distinct queue is placed exactly once, on live nodes, with the
        // full replica set: owner excluded from followers, followers distinct.
        assert_eq!(assignments.len(), unique_queues.len());
        let mut owner_load: HashMap<String, usize> = HashMap::new();
        for (queue, assignment) in &assignments {
            assert!(unique_queues.contains(queue));
            assert!(live.contains_key(&assignment.owner));
            assert_eq!(assignment.followers.len(), TARGET_FOLLOWERS);
            let distinct: HashSet<_> = assignment.followers.iter().collect();
            assert_eq!(distinct.len(), TARGET_FOLLOWERS);
            assert!(!assignment.followers.contains(&assignment.owner));
            for follower in &assignment.followers {
                assert!(live.contains_key(follower));
            }
            *owner_load.entry(assignment.owner.clone()).or_default() += 1;
        }

        // Round-robin ownership over sorted queues balances within one queue.
        let max_load = owner_load.values().copied().max().unwrap();
        let min_load = owner_load.values().copied().min().unwrap();
        assert!(
            max_load - min_load <= 1,
            "owner load imbalance {min_load}..={max_load}"
        );

        // Re-planning with the produced assignments as the existing state and
        // an unchanged cluster must be a pure no-op (controller anti-churn
        // depends on this stability).
        let replan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: live.clone(),
                queues: queues.clone(),
                existing: assignments.clone(),
                target_followers: TARGET_FOLLOWERS,
                generation: 2,
            })
            .unwrap();
        assert_eq!(replan.snapshot.assignments, assignments);

        // Kill a third of the cluster and re-plan: every queue stays placed on
        // a live node, and any queue whose owner survived keeps that owner.
        let mut survivors = live.clone();
        let mut dead = HashSet::new();
        for idx in 0..NODE_COUNT {
            if idx % 3 == 0 {
                let node_id = format!("node-{idx:04}");
                survivors.remove(&node_id);
                dead.insert(node_id);
            }
        }
        let rebalanced = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: survivors.clone(),
                queues: queues.clone(),
                existing: assignments.clone(),
                target_followers: TARGET_FOLLOWERS,
                generation: 3,
            })
            .unwrap();
        for (queue, assignment) in &rebalanced.snapshot.assignments {
            assert!(survivors.contains_key(&assignment.owner));
            for follower in &assignment.followers {
                assert!(survivors.contains_key(follower));
                assert!(!dead.contains(follower));
            }
            let previous = &assignments[queue];
            if !dead.contains(&previous.owner) {
                assert_eq!(
                    &assignment.owner, &previous.owner,
                    "surviving owner churned for {queue:?}"
                );
            }
        }
    }

    /// Much larger than the headline scaling test: a thousand nodes carrying
    /// several thousand queues. Pure computation, so it stays fast while
    /// proving the placement math (balance, replica integrity) holds far past
    /// any realistic deployment.
    #[test]
    fn placement_scales_to_very_large_clusters() {
        const NODE_COUNT: usize = 1000;
        const TARGET_FOLLOWERS: usize = 2;

        let live = nodes_n(NODE_COUNT);
        let queues = queues_prefixed("svc", 5000);
        let unique: HashSet<_> = queues.iter().cloned().collect();

        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: live.clone(),
                queues,
                existing: HashMap::new(),
                target_followers: TARGET_FOLLOWERS,
                generation: 1,
            })
            .unwrap();
        let assignments = plan.snapshot.assignments;

        assert_eq!(assignments.len(), unique.len());
        assert_placement_well_formed(&assignments, &live, TARGET_FOLLOWERS);

        let mut owner_load: HashMap<&str, usize> = HashMap::new();
        for assignment in assignments.values() {
            *owner_load.entry(assignment.owner.as_str()).or_default() += 1;
        }
        let max_load = owner_load.values().copied().max().unwrap();
        let min_load = owner_load.values().copied().min().unwrap_or(0);
        assert!(
            max_load - min_load <= 1,
            "owner load imbalance {min_load}..={max_load}"
        );
    }

    /// Scaling up the cluster absorbs newly declared queues onto the expanded
    /// node set without churning the owners or epochs of queues that were
    /// already placed (the deterministic planner is intentionally sticky:
    /// adding capacity does not trigger a rebalance of existing ownership).
    #[test]
    fn placement_scale_up_absorbs_new_queues_without_churn() {
        const TARGET_FOLLOWERS: usize = 2;

        let initial_nodes = nodes_n(50);
        let batch_a = queues_prefixed("a", 400);

        let first = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: initial_nodes.clone(),
                queues: batch_a.clone(),
                existing: HashMap::new(),
                target_followers: TARGET_FOLLOWERS,
                generation: 1,
            })
            .unwrap();
        let placed_a = first.snapshot.assignments;

        // Grow to 75 nodes and declare a fresh batch of queues.
        let expanded_nodes = nodes_n(75);
        let batch_b = queues_prefixed("b", 200);
        let mut all_queues = batch_a.clone();
        all_queues.extend(batch_b.iter().cloned());

        let second = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: expanded_nodes.clone(),
                queues: all_queues,
                existing: placed_a.clone(),
                target_followers: TARGET_FOLLOWERS,
                generation: 2,
            })
            .unwrap();
        let placed_all = second.snapshot.assignments;

        assert_placement_well_formed(&placed_all, &expanded_nodes, TARGET_FOLLOWERS);

        // Existing queues keep their owner and epoch: no churn from scale-up.
        for queue in &batch_a {
            let before = &placed_a[queue];
            let after = &placed_all[queue];
            assert_eq!(before.owner, after.owner, "owner churned for {queue:?}");
            assert_eq!(before.epoch, after.epoch, "epoch churned for {queue:?}");
        }

        // The new nodes are reachable for placement: some replica role lands on
        // the expanded range (owner or follower of a batch-B queue).
        let new_node_ids: HashSet<String> = (50..75).map(|idx| format!("node-{idx:04}")).collect();
        let touches_new_node = batch_b.iter().any(|queue| {
            let assignment = &placed_all[queue];
            new_node_ids.contains(&assignment.owner)
                || assignment
                    .followers
                    .iter()
                    .any(|follower| new_node_ids.contains(follower))
        });
        assert!(
            touches_new_node,
            "new nodes received no placement after scale-up"
        );
    }

    /// Near-total failure: a large cluster collapses to two survivors. Every
    /// queue must still place, with followers transparently capped at the one
    /// remaining peer rather than failing or duplicating the owner.
    #[test]
    fn placement_survives_collapse_to_two_nodes() {
        const TARGET_FOLLOWERS: usize = 3;

        let queues = queues_prefixed("svc", 500);
        let survivors = nodes_n(2);

        // Pretend everything was previously spread across 200 nodes that are
        // now gone; the existing assignments reference dead owners/followers.
        let stale = nodes_n(200);
        let stale_plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: stale,
                queues: queues.clone(),
                existing: HashMap::new(),
                target_followers: TARGET_FOLLOWERS,
                generation: 1,
            })
            .unwrap();

        let recovered = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: survivors.clone(),
                queues: queues.clone(),
                existing: stale_plan.snapshot.assignments,
                target_followers: TARGET_FOLLOWERS,
                generation: 2,
            })
            .unwrap();

        let assignments = recovered.snapshot.assignments;
        assert_eq!(
            assignments.len(),
            queues.iter().cloned().collect::<HashSet<_>>().len()
        );
        // With two nodes, the follower set caps at one (owner excluded).
        assert_placement_well_formed(&assignments, &survivors, TARGET_FOLLOWERS);
    }

    #[test]
    fn placement_adds_missing_queue_without_churning_existing_assignment() {
        let existing_queue = QueueIdentity::new("emails", 0, None);
        let new_queue = QueueIdentity::new("orders", 0, None);
        let existing_assignment = PartitionAssignment::new(
            existing_queue.clone(),
            "node-b",
            vec!["node-c".to_string(), "node-a".to_string()],
            17,
        )
        .with_durability(ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 });
        let existing = HashMap::from([(existing_queue.clone(), existing_assignment.clone())]);

        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: nodes(["node-a", "node-b", "node-c"]),
                queues: vec![new_queue.clone(), existing_queue.clone()],
                existing,
                target_followers: 2,
                generation: 18,
            })
            .unwrap();

        assert_eq!(
            plan.snapshot
                .assignments
                .get(&existing_queue)
                .expect("existing assignment"),
            &existing_assignment
        );
        assert!(plan.snapshot.assignments.contains_key(&new_queue));
    }

    #[test]
    fn placement_repairs_missing_owner_and_missing_followers() {
        let queue = QueueIdentity::new("emails", 0, None);
        let existing_assignment = PartitionAssignment::new(
            queue.clone(),
            "node-z",
            vec![
                "node-b".to_string(),
                "node-missing".to_string(),
                "node-b".to_string(),
            ],
            2,
        );
        let existing = HashMap::from([(queue.clone(), existing_assignment)]);

        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: nodes(["node-a", "node-b", "node-c"]),
                queues: vec![queue],
                existing,
                target_followers: 2,
                generation: 9,
            })
            .unwrap();

        let assignment = plan
            .snapshot
            .assignment_for("emails", 0, None)
            .expect("assignment");
        assert_eq!(assignment.owner, "node-a");
        assert_eq!(assignment.followers, vec!["node-b", "node-c"]);
        assert_eq!(assignment.epoch, 9);
    }

    fn snapshot_with(
        assignments: Vec<PartitionAssignment>,
        generation: u64,
    ) -> CoordinationSnapshot {
        let mut snapshot = snapshot();
        snapshot.assignments = assignments
            .into_iter()
            .map(|assignment| (assignment.queue.clone(), assignment))
            .collect();
        snapshot.generation = generation;
        snapshot
    }

    fn assignment(
        topic: &str,
        owner: &str,
        followers: Vec<&str>,
        epoch: u64,
    ) -> PartitionAssignment {
        PartitionAssignment::new(
            QueueIdentity::new(topic, 0, Some("workers")),
            owner,
            followers.into_iter().map(str::to_string).collect(),
            epoch,
        )
    }

    fn nodes<const N: usize>(node_ids: [&str; N]) -> HashMap<String, NodeInfo> {
        node_ids
            .into_iter()
            .enumerate()
            .map(|(idx, node_id)| {
                (
                    node_id.to_string(),
                    NodeInfo {
                        node_id: node_id.to_string(),
                        broker_addr: format!("127.0.0.1:{}", 10_000 + idx).parse().unwrap(),
                        admin_addr: None,
                    },
                )
            })
            .collect()
    }

    fn assignment_owner(snapshot: &CoordinationSnapshot, topic: &str) -> String {
        snapshot
            .assignment_for(topic, 0, None)
            .expect("assignment")
            .owner
            .clone()
    }

    #[test]
    fn transition_planner_reports_new_local_owner_and_follower_assignments() {
        let previous = snapshot_with(Vec::new(), 1);
        let next = snapshot_with(
            vec![
                assignment("owned", "node-a", vec!["node-b"], 2),
                assignment("followed", "node-b", vec!["node-a"], 2),
                assignment("remote", "node-b", vec![], 2),
            ],
            2,
        );

        let transitions = plan_local_assignment_transitions("node-a", &previous, &next);

        assert_eq!(transitions.len(), 2);
        assert_eq!(transitions[0].queue.topic.to_string(), "followed");
        assert_eq!(transitions[0].intent, LocalAssignmentIntent::BecomeFollower);
        assert_eq!(transitions[1].queue.topic.to_string(), "owned");
        assert_eq!(transitions[1].intent, LocalAssignmentIntent::BecomeOwner);
    }

    #[test]
    fn transition_planner_reports_owner_demote_and_follower_promote() {
        let previous = snapshot_with(
            vec![
                assignment("demote", "node-a", vec!["node-b"], 1),
                assignment("promote", "node-b", vec!["node-a"], 1),
            ],
            1,
        );
        let next = snapshot_with(
            vec![
                assignment("demote", "node-b", vec!["node-a"], 2),
                assignment("promote", "node-a", vec!["node-b"], 2),
            ],
            2,
        );

        let transitions = plan_local_assignment_transitions("node-a", &previous, &next);

        assert_eq!(transitions.len(), 2);
        assert_eq!(transitions[0].queue.topic.to_string(), "demote");
        assert_eq!(
            transitions[0].intent,
            LocalAssignmentIntent::DemoteOwnerToFollower
        );
        assert_eq!(transitions[1].queue.topic.to_string(), "promote");
        assert_eq!(
            transitions[1].intent,
            LocalAssignmentIntent::PromoteFollowerToOwner
        );
    }

    #[test]
    fn transition_planner_reports_removed_local_assignments() {
        let previous = snapshot_with(
            vec![
                assignment("owned", "node-a", vec!["node-b"], 1),
                assignment("followed", "node-b", vec!["node-a"], 1),
            ],
            1,
        );
        let next = snapshot_with(Vec::new(), 2);

        let transitions = plan_local_assignment_transitions("node-a", &previous, &next);

        assert_eq!(transitions.len(), 2);
        assert_eq!(transitions[0].queue.topic.to_string(), "followed");
        assert_eq!(transitions[0].intent, LocalAssignmentIntent::StopFollower);
        assert_eq!(transitions[1].queue.topic.to_string(), "owned");
        assert_eq!(transitions[1].intent, LocalAssignmentIntent::FreezeOwner);
    }

    #[test]
    fn transition_planner_reports_same_role_refresh_without_role_change() {
        let previous = snapshot_with(vec![assignment("owned", "node-a", vec!["node-b"], 1)], 1);
        let next = snapshot_with(vec![assignment("owned", "node-a", vec!["node-b"], 2)], 2);

        let transitions = plan_local_assignment_transitions("node-a", &previous, &next);

        assert_eq!(transitions.len(), 1);
        assert_eq!(transitions[0].intent, LocalAssignmentIntent::RefreshOwner);
        assert!(!transitions[0].intent.requires_role_change());
    }

    #[test]
    fn noop_coordination_preserves_standalone_owner_behavior() {
        let coordination = NoopCoordination::default();

        assert!(coordination.owns_queue("anything", 99, Some("group")));
        assert!(
            coordination
                .owner_for("anything", 99, Some("group"))
                .is_none()
        );
    }
}
