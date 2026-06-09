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
