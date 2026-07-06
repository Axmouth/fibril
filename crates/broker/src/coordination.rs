use std::{
    collections::{BTreeSet, HashMap, HashSet},
    net::SocketAddr,
    sync::{Arc, RwLock},
};

use crate::storage::{Group, Partition, Topic};
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct QueueIdentity {
    pub topic: Topic,
    pub partition: Partition,
    pub group: Option<Group>,
}

impl QueueIdentity {
    pub fn new(topic: impl Into<Topic>, partition: Partition, group: Option<&str>) -> Self {
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
    /// The broker endpoint to advertise to peers and clients, as a connectable
    /// `host:port` string (resolved at connect time). A `String` rather than a
    /// `SocketAddr` so it can carry a service name when `bind` is `0.0.0.0`.
    pub broker_addr: String,
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
    /// Stream (Plexus) partition ownership, kept in a separate map from queue
    /// assignments: a stream partition has a single owner and no follower set
    /// (durability is the local keratin tier, not cross-node replication).
    pub stream_assignments: HashMap<StreamIdentity, StreamAssignment>,
    pub generation: u64,
}

impl CoordinationSnapshot {
    pub fn assignment_for(
        &self,
        topic: &str,
        partition: Partition,
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

    /// The owner of a stream partition, if assigned.
    pub fn stream_assignment_for(
        &self,
        topic: &str,
        partition: Partition,
    ) -> Option<&StreamAssignment> {
        self.stream_assignments
            .get(&StreamIdentity::new(topic, partition))
    }

    /// Stream partitions this node owns.
    pub fn streams_owned_by(&self, node_id: &str) -> Vec<StreamAssignment> {
        self.stream_assignments
            .values()
            .filter(|assignment| assignment.is_owned_by(node_id))
            .cloned()
            .collect()
    }

    /// Stream partitions this node replicates as a follower.
    pub fn streams_followed_by(&self, node_id: &str) -> Vec<StreamAssignment> {
        self.stream_assignments
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
    pub default_durability: ReplicationDurabilityPolicy,
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
                    stream_assignments: HashMap::new(),
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
                .unwrap_or(input.default_durability);
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
                stream_assignments: HashMap::new(),
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

fn queue_sort_key(queue: &QueueIdentity) -> (String, Partition, String) {
    (
        queue.topic.to_string(),
        queue.partition,
        queue.group.as_deref().unwrap_or_default().to_string(),
    )
}

// ===== Stream (Plexus) placement =============================================
//
// Streams are fan-out channels, not work queues. A stream partition needs a
// single OWNER node and nothing else: there is no follower set and no
// replica-durability policy (durability is the local keratin tier, not
// cross-node replication). Placement spreads a stream's partitions across
// distinct nodes first, the same small-cluster balance goal as queue placement,
// but the resulting plan carries only owner + fencing epoch.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StreamIdentity {
    pub topic: Topic,
    pub partition: Partition,
}

impl StreamIdentity {
    pub fn new(topic: impl Into<Topic>, partition: Partition) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamAssignment {
    pub stream: StreamIdentity,
    pub owner: String,
    /// Replica followers for the durable tier (empty for owner-only tiers).
    /// A durable stream replicates its log to these nodes so the partition
    /// survives owner node loss; one of them is promoted on failover.
    pub followers: Vec<String>,
    /// Fencing epoch: bumps when ownership moves so a deposed owner can be
    /// fenced. Holds steady while the owner is unchanged.
    pub epoch: u64,
}

impl StreamAssignment {
    pub fn new(
        stream: StreamIdentity,
        owner: impl Into<String>,
        followers: Vec<String>,
        epoch: u64,
    ) -> Self {
        Self {
            stream,
            owner: owner.into(),
            followers,
            epoch,
        }
    }

    pub fn is_owned_by(&self, node_id: &str) -> bool {
        self.owner == node_id
    }

    pub fn is_followed_by(&self, node_id: &str) -> bool {
        self.followers.iter().any(|follower| follower == node_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StreamPlacementInput {
    pub nodes: HashMap<String, NodeInfo>,
    pub streams: Vec<StreamIdentity>,
    pub existing: HashMap<StreamIdentity, StreamAssignment>,
    /// Replica follower count per stream topic; absent means 0 (owner-only, the
    /// ephemeral/speculative tiers). Durable streams set this to the replication
    /// factor so the planner spreads followers across distinct nodes.
    pub target_followers: HashMap<String, usize>,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamPlacementPlan {
    pub assignments: HashMap<StreamIdentity, StreamAssignment>,
}

pub trait StreamPlacementPolicy: std::fmt::Debug + Send + Sync {
    fn plan(
        &self,
        input: StreamPlacementInput,
    ) -> Result<StreamPlacementPlan, PartitionPlacementError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeterministicStreamPlacement;

impl StreamPlacementPolicy for DeterministicStreamPlacement {
    fn plan(
        &self,
        input: StreamPlacementInput,
    ) -> Result<StreamPlacementPlan, PartitionPlacementError> {
        let mut streams = input.streams;
        streams.sort_by_key(stream_sort_key);
        streams.dedup();

        if streams.is_empty() {
            return Ok(StreamPlacementPlan {
                assignments: HashMap::new(),
            });
        }

        let node_ids = sorted_node_ids(&input.nodes);
        if node_ids.is_empty() {
            return Err(PartitionPlacementError::NoNodesForQueues);
        }

        let mut assignments = HashMap::with_capacity(streams.len());
        for (idx, stream) in streams.into_iter().enumerate() {
            let existing = input.existing.get(&stream);
            // Keep the existing owner if it is still alive (sticky), else spread
            // by round-robin over the sorted node set.
            let owner = existing
                .and_then(|assignment| {
                    input
                        .nodes
                        .contains_key(&assignment.owner)
                        .then_some(&assignment.owner)
                })
                .cloned()
                .unwrap_or_else(|| node_ids[idx % node_ids.len()].clone());
            // Spread replica followers across distinct nodes for the durable
            // tier (0 for owner-only tiers), reusing the queue follower planner.
            let target_followers = input
                .target_followers
                .get(stream.topic.as_str())
                .copied()
                .unwrap_or(0);
            let followers = plan_followers(
                &node_ids,
                &owner,
                existing
                    .map(|assignment| assignment.followers.as_slice())
                    .unwrap_or(&[]),
                target_followers,
            );
            // Reuse the prior epoch when owner and follower set are unchanged;
            // bump to the new generation when ownership moves so a deposed owner
            // is fenced.
            let epoch = existing
                .filter(|assignment| assignment.owner == owner && assignment.followers == followers)
                .map(|assignment| assignment.epoch)
                .unwrap_or(input.generation);

            assignments.insert(
                stream.clone(),
                StreamAssignment::new(stream, owner, followers, epoch),
            );
        }

        Ok(StreamPlacementPlan { assignments })
    }
}

fn stream_sort_key(stream: &StreamIdentity) -> (String, Partition) {
    (stream.topic.to_string(), stream.partition)
}

// ===== Consumer-group partition assignment ===================================
//
// The CONSUMER side: which member of a consumer group reads which partitions of
// a logical queue. Mirrors `PartitionPlacementPolicy` (partitions->nodes) one
// level up (partitions->group members). Coverage-first: every partition is
// assigned to exactly one member whenever the group is non-empty; a per-consumer
// target is a soft capacity signal, never a coverage cap.

/// Inputs for assigning a queue's partitions across a consumer group's members.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConsumerGroupAssignmentInput {
    /// The partitions to cover (one logical queue's partitions).
    pub partitions: Vec<Partition>,
    /// Live group member ids (stable per consumer instance).
    pub members: Vec<String>,
    /// Group-wide soft per-consumer capacity, applied to any member without an
    /// explicit override in `member_targets`. `None` leaves those members
    /// uncapped (pure balanced share).
    pub default_target_per_consumer: Option<usize>,
    /// Per-member soft capacity overrides (a member's desired max partitions).
    /// Takes precedence over `default_target_per_consumer`. Coverage always wins:
    /// when total capacity is below the partition count every partition is still
    /// assigned and `under_provisioned` is set.
    pub member_targets: HashMap<String, usize>,
    /// The assignment in effect before this rebalance (member -> partitions). A
    /// sticky assignor uses it to minimize churn; stateless assignors ignore it.
    /// Empty on first assignment.
    pub current: HashMap<String, Vec<Partition>>,
}

impl ConsumerGroupAssignmentInput {
    /// The effective soft capacity for `member`: its explicit override, else the
    /// group default, else uncapped (`None`).
    fn target_for(&self, member: &str) -> Option<usize> {
        self.member_targets
            .get(member)
            .copied()
            .or(self.default_target_per_consumer)
    }
}

/// The computed assignment of partitions to group members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupAssignment {
    /// Partitions assigned to each member (every member present, possibly empty).
    pub by_member: HashMap<String, Vec<Partition>>,
    /// True when some member's load exceeds its soft target — the group is
    /// under-provisioned for its targets (still fully covered). Feeds
    /// alert/autoscale, never reduces coverage.
    pub under_provisioned: bool,
}

/// Pluggable consumer-group assignment policy (geo/locality- or load-aware
/// variants can slot in later), mirroring [`PartitionPlacementPolicy`].
pub trait ConsumerGroupAssignor: std::fmt::Debug + Send + Sync {
    fn assign(&self, input: ConsumerGroupAssignmentInput) -> ConsumerGroupAssignment;
}

/// Index of the member to receive the next partition: the least-loaded member
/// still under its soft cap (ties broken by earliest index, i.e. sorted member
/// order). When every member is at capacity, overflow to the least-loaded member
/// overall — coverage always wins over the soft cap.
fn least_loaded_member(loads: &[usize], caps: &[Option<usize>]) -> usize {
    let mut pick: Option<usize> = None;
    for index in 0..loads.len() {
        let under_cap = caps[index].is_none_or(|cap| loads[index] < cap);
        if under_cap && pick.is_none_or(|best| loads[index] < loads[best]) {
            pick = Some(index);
        }
    }
    pick.unwrap_or_else(|| {
        let mut best = 0;
        for index in 1..loads.len() {
            if loads[index] < loads[best] {
                best = index;
            }
        }
        best
    })
}

/// The balanced per-member load (counts only) a fresh capacity-aware deal of
/// `num_partitions` would produce — the target each member should reach. Sums to
/// `num_partitions`, so it is both balanced and fully covering.
fn balanced_target_loads(num_partitions: usize, caps: &[Option<usize>]) -> Vec<usize> {
    let mut loads = vec![0usize; caps.len()];
    for _ in 0..num_partitions {
        let index = least_loaded_member(&loads, caps);
        loads[index] += 1;
    }
    loads
}

/// True when any member's final load exceeds its set soft target (coverage
/// forced it over capacity).
fn flag_under_provisioned(loads: &[usize], caps: &[Option<usize>]) -> bool {
    loads
        .iter()
        .zip(caps)
        .any(|(load, cap)| cap.is_some_and(|cap| cap > 0 && *load > cap))
}

/// Deterministic, balanced, coverage-first assignor: deals the queue's
/// partitions one at a time to the eligible member (load below its soft target)
/// with the least load, breaking ties by sorted member order. With no targets
/// this is exactly a round-robin balanced deal (first `N % M` members get
/// `ceil(N/M)`, the rest `floor(N/M)`); with targets it respects each member's
/// soft capacity until capacity runs out, then keeps assigning (coverage wins)
/// and flags `under_provisioned`. Stateless — ignores the prior assignment, so it
/// may reshuffle partitions on every membership change; see
/// [`StickyConsumerGroupAssignor`] to minimize churn.
#[derive(Debug, Clone, Copy, Default)]
pub struct BalancedConsumerGroupAssignor;

impl ConsumerGroupAssignor for BalancedConsumerGroupAssignor {
    fn assign(&self, input: ConsumerGroupAssignmentInput) -> ConsumerGroupAssignment {
        let mut partitions = input.partitions.clone();
        partitions.sort();
        partitions.dedup();

        let mut members = input.members.clone();
        members.sort();
        members.dedup();

        if members.is_empty() {
            // No members => nothing assigned. The queue is unconsumed (backlog
            // waits); not "under-provisioned" — there is no target to miss.
            return ConsumerGroupAssignment {
                by_member: HashMap::new(),
                under_provisioned: false,
            };
        }

        let member_count = members.len();
        let caps: Vec<Option<usize>> = members.iter().map(|m| input.target_for(m)).collect();
        let mut loads = vec![0usize; member_count];
        let mut assigned: Vec<Vec<Partition>> = vec![Vec::new(); member_count];

        for partition in &partitions {
            let index = least_loaded_member(&loads, &caps);
            assigned[index].push(*partition);
            loads[index] += 1;
        }

        let under_provisioned = flag_under_provisioned(&loads, &caps);
        let by_member = members.into_iter().zip(assigned).collect();

        ConsumerGroupAssignment {
            by_member,
            under_provisioned,
        }
    }
}

/// Deterministic, balanced, coverage-first **sticky** assignor: reaches the same
/// per-member loads as [`BalancedConsumerGroupAssignor`] but keeps each member's
/// current partitions wherever possible, so a membership change only moves the
/// minimum needed (each move costs a drain on the old owner + a cold start on the
/// new one). With an empty `current` it is identical to the balanced deal.
///
/// Algorithm: (1) compute each member's balanced target load; (2) retain each
/// member's still-valid current partitions up to that target; (3) deal the
/// remaining (orphaned / new / over-target-trimmed) partitions to members below
/// their target. Because the targets sum to the partition count, step 3 fills
/// every member exactly to its target — balanced, fully covering, max retention.
#[derive(Debug, Clone, Copy, Default)]
pub struct StickyConsumerGroupAssignor;

impl ConsumerGroupAssignor for StickyConsumerGroupAssignor {
    fn assign(&self, input: ConsumerGroupAssignmentInput) -> ConsumerGroupAssignment {
        let mut partitions = input.partitions.clone();
        partitions.sort();
        partitions.dedup();

        let mut members = input.members.clone();
        members.sort();
        members.dedup();

        if members.is_empty() {
            return ConsumerGroupAssignment {
                by_member: HashMap::new(),
                under_provisioned: false,
            };
        }

        let member_count = members.len();
        let caps: Vec<Option<usize>> = members.iter().map(|m| input.target_for(m)).collect();
        let target = balanced_target_loads(partitions.len(), &caps);
        let partition_set: HashSet<Partition> = partitions.iter().copied().collect();

        let mut assigned: Vec<Vec<Partition>> = vec![Vec::new(); member_count];
        let mut taken: HashSet<Partition> = HashSet::new();

        // (2) Retain still-valid current partitions, up to each member's target.
        for (index, member) in members.iter().enumerate() {
            let Some(current) = input.current.get(member) else {
                continue;
            };
            let mut keep: Vec<Partition> = current
                .iter()
                .copied()
                .filter(|partition| partition_set.contains(partition))
                .collect();
            keep.sort();
            keep.dedup();
            for partition in keep {
                if assigned[index].len() >= target[index] {
                    break;
                }
                if taken.insert(partition) {
                    assigned[index].push(partition);
                }
            }
        }

        // (3) Deal the leftover partitions to members still below their target.
        let free: Vec<Partition> = partitions
            .iter()
            .copied()
            .filter(|partition| !taken.contains(partition))
            .collect();
        for partition in free {
            let mut pick: Option<usize> = None;
            for index in 0..member_count {
                if assigned[index].len() < target[index]
                    && pick.is_none_or(|best| assigned[index].len() < assigned[best].len())
                {
                    pick = Some(index);
                }
            }
            // Targets sum to the partition count, so a slot always exists; fall
            // back to least-loaded overall rather than panic if that ever fails.
            let index = pick.unwrap_or_else(|| {
                let loads: Vec<usize> = assigned.iter().map(Vec::len).collect();
                least_loaded_member(&loads, &caps)
            });
            assigned[index].push(partition);
        }

        for partitions in &mut assigned {
            partitions.sort();
        }
        let loads: Vec<usize> = assigned.iter().map(Vec::len).collect();
        let under_provisioned = flag_under_provisioned(&loads, &caps);
        let by_member = members.into_iter().zip(assigned).collect();

        ConsumerGroupAssignment {
            by_member,
            under_provisioned,
        }
    }
}

/// How one member's assignment changed across a rebalance.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MemberAssignmentChange {
    /// The member's full partition set after the rebalance.
    pub assigned: Vec<Partition>,
    /// Partitions newly assigned to the member (start consuming these).
    pub added: Vec<Partition>,
    /// Partitions taken away from the member (drain in-flight, then release —
    /// the new owner must not double-process). See graceful-drain requirement.
    pub revoked: Vec<Partition>,
}

/// The outcome of a membership change: a new generation and the per-member
/// deltas to apply. The `generation` lets stale assignments be fenced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupAssignmentDelta {
    pub generation: u64,
    pub per_member: HashMap<String, MemberAssignmentChange>,
    pub under_provisioned: bool,
}

/// Tracks one consumer group's live members for a logical queue and the current
/// partition assignment, recomputing it (via a [`ConsumerGroupAssignor`]) on each
/// membership change and reporting per-member added/revoked partitions. `added`
/// drives new subscriptions; `revoked` drives graceful drain. Pure state — where
/// this is hosted and how deltas are delivered are separate (later) concerns.
#[derive(Debug)]
pub struct ConsumerGroupState {
    partitions: Vec<Partition>,
    members: BTreeSet<String>,
    assignment: HashMap<String, Vec<Partition>>,
    generation: u64,
    default_target_per_consumer: Option<usize>,
    /// Per-member soft capacity overrides (see [`ConsumerGroupAssignmentInput`]).
    member_targets: HashMap<String, usize>,
    assignor: Arc<dyn ConsumerGroupAssignor>,
}

impl ConsumerGroupState {
    pub fn new(
        partitions: Vec<Partition>,
        default_target_per_consumer: Option<usize>,
        assignor: Arc<dyn ConsumerGroupAssignor>,
    ) -> Self {
        Self {
            partitions,
            members: BTreeSet::new(),
            assignment: HashMap::new(),
            generation: 0,
            default_target_per_consumer,
            member_targets: HashMap::new(),
            assignor,
        }
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The queue's partition set this group is dividing.
    pub fn partitions(&self) -> &[Partition] {
        &self.partitions
    }

    /// True when the group has no members.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// The member's current partitions (empty if unknown / unassigned).
    pub fn assignment_for(&self, member: &str) -> &[Partition] {
        self.assignment
            .get(member)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn add_member(&mut self, member: impl Into<String>) -> GroupAssignmentDelta {
        self.members.insert(member.into());
        self.rebalance()
    }

    pub fn remove_member(&mut self, member: &str) -> GroupAssignmentDelta {
        self.members.remove(member);
        self.rebalance()
    }

    pub fn set_members<I: IntoIterator<Item = String>>(
        &mut self,
        members: I,
    ) -> GroupAssignmentDelta {
        self.members = members.into_iter().collect();
        self.rebalance()
    }

    /// Update the queue's partition set (e.g. repartitioning) and rebalance.
    pub fn set_partitions(&mut self, partitions: Vec<Partition>) -> GroupAssignmentDelta {
        self.partitions = partitions;
        self.rebalance()
    }

    /// Seed the prior assignment used for stickiness, without rebalancing. Only
    /// meaningful right after construction (before the first reconcile): it lets
    /// a freshly elected controller carry a previously published plan forward so
    /// the next reconcile stays sticky to it instead of computing a fresh one.
    pub fn seed_assignment(&mut self, assignment: HashMap<String, Vec<Partition>>) {
        self.assignment = assignment;
    }

    /// Replace both the partition set and the membership in one rebalance. Used
    /// when the caller holds the authoritative view of who is in the cohort and
    /// which partitions they cover (the broker-side router), avoiding a
    /// double-rebalance from separate `set_partitions` + `set_members` calls.
    pub fn reconcile(
        &mut self,
        partitions: Vec<Partition>,
        members: Vec<String>,
        member_targets: HashMap<String, usize>,
    ) -> GroupAssignmentDelta {
        self.partitions = partitions;
        self.members = members.into_iter().collect();
        self.member_targets = member_targets;
        self.rebalance()
    }

    fn rebalance(&mut self) -> GroupAssignmentDelta {
        let result = self.assignor.assign(ConsumerGroupAssignmentInput {
            partitions: self.partitions.clone(),
            members: self.members.iter().cloned().collect(),
            default_target_per_consumer: self.default_target_per_consumer,
            member_targets: self.member_targets.clone(),
            // The assignment in effect before this rebalance drives stickiness.
            current: self.assignment.clone(),
        });
        let new_assignment = result.by_member;

        // Per-member delta over the union of old + new members (a departed member
        // reports everything revoked so its partitions can be reclaimed).
        let mut members: BTreeSet<&String> = new_assignment.keys().collect();
        members.extend(self.assignment.keys());

        let mut per_member = HashMap::with_capacity(members.len());
        for member in members {
            let old = self
                .assignment
                .get(member)
                .map(Vec::as_slice)
                .unwrap_or(&[]);
            let new = new_assignment.get(member).map(Vec::as_slice).unwrap_or(&[]);
            let old_set: HashSet<Partition> = old.iter().copied().collect();
            let new_set: HashSet<Partition> = new.iter().copied().collect();
            per_member.insert(
                member.clone(),
                MemberAssignmentChange {
                    assigned: new.to_vec(),
                    added: new
                        .iter()
                        .copied()
                        .filter(|p| !old_set.contains(p))
                        .collect(),
                    revoked: old
                        .iter()
                        .copied()
                        .filter(|p| !new_set.contains(p))
                        .collect(),
                },
            );
        }

        if new_assignment != self.assignment {
            self.generation += 1;
        }
        self.assignment = new_assignment;

        GroupAssignmentDelta {
            generation: self.generation,
            per_member,
            under_provisioned: result.under_provisioned,
        }
    }
}

/// Identifies one exclusive consumer group: a named cohort of consumers that
/// exclusively divide a logical queue `(topic, group)`'s partitions. The
/// `consumer_group` id is the opt-in handle (consumers that pass the same id on
/// the same queue share an exclusive assignment); plain consumers that pass none
/// keep the default competing-consumer behavior and are not tracked here.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConsumerGroupKey {
    pub topic: Topic,
    pub group: Option<Group>,
    pub consumer_group: String,
}

impl ConsumerGroupKey {
    pub fn new(
        topic: impl Into<Topic>,
        group: Option<&str>,
        consumer_group: impl Into<String>,
    ) -> Self {
        Self {
            topic: topic.into(),
            group: group.map(str::to_string),
            consumer_group: consumer_group.into(),
        }
    }
}

/// Broker-side registry of all live exclusive consumer groups: holds a
/// [`ConsumerGroupState`] per [`ConsumerGroupKey`] and routes membership changes
/// to it. Single-owner scope for now (the owner of a queue's partitions hosts
/// this); a cross-broker coordinator is a later step.
#[derive(Debug)]
pub struct ExclusiveConsumerGroups {
    assignor: Arc<dyn ConsumerGroupAssignor>,
    default_target_per_consumer: Option<usize>,
    groups: HashMap<ConsumerGroupKey, ConsumerGroupState>,
}

impl ExclusiveConsumerGroups {
    pub fn new(
        assignor: Arc<dyn ConsumerGroupAssignor>,
        default_target_per_consumer: Option<usize>,
    ) -> Self {
        Self {
            assignor,
            default_target_per_consumer,
            groups: HashMap::new(),
        }
    }

    /// Add `member` to the group, (re)setting the queue's current partition set,
    /// and return the resulting per-member assignment delta.
    pub fn join(
        &mut self,
        key: ConsumerGroupKey,
        partitions: Vec<Partition>,
        member: impl Into<String>,
    ) -> GroupAssignmentDelta {
        let assignor = self.assignor.clone();
        let target = self.default_target_per_consumer;
        let state = self
            .groups
            .entry(key)
            .or_insert_with(|| ConsumerGroupState::new(partitions.clone(), target, assignor));
        // Keep the partition set current (e.g. repartitioning) before rebalancing.
        if state.partitions() != partitions.as_slice() {
            state.set_partitions(partitions);
        }
        state.add_member(member)
    }

    /// Reconcile a group to an authoritative `(partitions, members, targets)` view
    /// and return the resulting delta. When `members` is empty the group is
    /// dropped (its last member left); the returned delta reports everything
    /// revoked so the caller can release any gates. This is the broker-side entry
    /// point: the router recomputes the union of subscribed partitions, live
    /// members, and their soft targets on every change rather than tracking
    /// incremental join/leave.
    pub fn reconcile(
        &mut self,
        key: ConsumerGroupKey,
        partitions: Vec<Partition>,
        members: Vec<String>,
        member_targets: HashMap<String, usize>,
    ) -> GroupAssignmentDelta {
        if members.is_empty() {
            // Last member gone: tear the group down, reporting full revocation.
            return match self.groups.remove(&key) {
                Some(mut state) => state.set_members(std::iter::empty::<String>()),
                None => GroupAssignmentDelta {
                    generation: 0,
                    per_member: HashMap::new(),
                    under_provisioned: false,
                },
            };
        }
        let assignor = self.assignor.clone();
        let target = self.default_target_per_consumer;
        let state = self
            .groups
            .entry(key)
            .or_insert_with(|| ConsumerGroupState::new(partitions.clone(), target, assignor));
        state.reconcile(partitions, members, member_targets)
    }

    /// Seed a cohort's prior assignment for stickiness, but only if the cohort is
    /// not already tracked (never clobber live state). A freshly elected
    /// controller uses this to adopt the last published plan, so its first
    /// computation keeps the existing assignment rather than churning the cluster.
    pub fn seed_if_absent(
        &mut self,
        key: ConsumerGroupKey,
        assignment: HashMap<String, Vec<Partition>>,
    ) {
        if self.groups.contains_key(&key) {
            return;
        }
        let partitions: Vec<Partition> = assignment.values().flatten().copied().collect();
        let mut state = ConsumerGroupState::new(
            partitions,
            self.default_target_per_consumer,
            self.assignor.clone(),
        );
        state.seed_assignment(assignment);
        self.groups.insert(key, state);
    }

    /// Remove `member`; returns the delta, or `None` if the group was unknown.
    /// Drops the group entry once its last member leaves.
    pub fn leave(&mut self, key: &ConsumerGroupKey, member: &str) -> Option<GroupAssignmentDelta> {
        let state = self.groups.get_mut(key)?;
        let delta = state.remove_member(member);
        if state.is_empty() {
            self.groups.remove(key);
        }
        Some(delta)
    }

    /// The member's currently-assigned partitions (empty if unknown).
    pub fn assignment_for(&self, key: &ConsumerGroupKey, member: &str) -> &[Partition] {
        self.groups
            .get(key)
            .map(|state| state.assignment_for(member))
            .unwrap_or(&[])
    }
}

// ===== Cross-broker cohort coordination ======================================
//
// In a cluster a queue's partitions are owned by different brokers; the gate on
// each owner enforces one-consumer-per-partition locally (correctness), but the
// global ASSIGNMENT (balance, targets, stickiness across owner moves) needs a
// cluster-wide view. The controller aggregates each broker's local cohort
// membership and computes one plan per cohort, distributed back to owners.

/// One exclusive-cohort member as seen by a broker: the cluster member id plus
/// its soft target (if any).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CohortMemberInfo {
    pub member: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<usize>,
}

/// A broker's local view of one exclusive cohort's membership — the members
/// (cluster ids) subscribed to this broker's partitions of the queue. Brokers
/// publish this (e.g. on the heartbeat) for the controller to aggregate.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct LocalCohortMembership {
    pub topic: Topic,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<Group>,
    pub consumer_group: String,
    pub members: Vec<CohortMemberInfo>,
}

impl LocalCohortMembership {
    fn key(&self) -> ConsumerGroupKey {
        ConsumerGroupKey::new(
            self.topic.clone(),
            self.group.as_deref(),
            self.consumer_group.clone(),
        )
    }
}

/// The cluster-wide membership of one cohort, aggregated across all brokers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalCohortMembership {
    pub key: ConsumerGroupKey,
    pub members: Vec<CohortMemberInfo>,
}

/// Aggregate per-broker cohort membership into one global view per cohort: union
/// the members (deduped by cluster id — the same consumer reports the same id on
/// every broker it spans) and keep the largest reported target. Deterministic
/// (cohorts and members sorted).
pub fn aggregate_cohort_membership(
    reports: impl IntoIterator<Item = LocalCohortMembership>,
) -> Vec<GlobalCohortMembership> {
    let mut by_cohort: HashMap<ConsumerGroupKey, HashMap<String, Option<usize>>> = HashMap::new();
    for report in reports {
        let key = report.key();
        let members = by_cohort.entry(key).or_default();
        for info in report.members {
            let slot = members.entry(info.member).or_insert(None);
            // Keep the largest target seen (most capacity); None stays None.
            *slot = match (*slot, info.target) {
                (Some(a), Some(b)) => Some(a.max(b)),
                (existing, incoming) => existing.or(incoming),
            };
        }
    }

    let mut cohorts: Vec<GlobalCohortMembership> = by_cohort
        .into_iter()
        .map(|(key, members)| {
            let mut members: Vec<CohortMemberInfo> = members
                .into_iter()
                .map(|(member, target)| CohortMemberInfo { member, target })
                .collect();
            members.sort_by(|a, b| a.member.cmp(&b.member));
            GlobalCohortMembership { key, members }
        })
        .collect();
    cohorts.sort_by(|a, b| cohort_key_sort(&a.key).cmp(&cohort_key_sort(&b.key)));
    cohorts
}

fn cohort_key_sort(key: &ConsumerGroupKey) -> (String, String, String) {
    (
        key.topic.to_string(),
        key.group.as_deref().unwrap_or_default().to_string(),
        key.consumer_group.clone(),
    )
}

/// One cohort's cluster-wide assignment: `partition -> member id`, ready to
/// distribute to the partitions' owner brokers. Empty when the cohort has no
/// members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CohortPlan {
    pub key: ConsumerGroupKey,
    pub assignment: HashMap<Partition, String>,
}

/// Computes cluster-wide cohort assignments from aggregated membership, reusing
/// the sticky/target assignor. Holds per-cohort state across ticks, so the plan
/// stays balanced AND minimizes churn across membership changes and across
/// partition-owner moves (a partition keeps its member even if its owner moves).
#[derive(Debug)]
pub struct ClusterCohortController {
    groups: ExclusiveConsumerGroups,
}

impl ClusterCohortController {
    pub fn new(
        assignor: Arc<dyn ConsumerGroupAssignor>,
        default_target_per_consumer: Option<usize>,
    ) -> Self {
        Self {
            groups: ExclusiveConsumerGroups::new(assignor, default_target_per_consumer),
        }
    }

    /// Adopt a previously published plan as the sticky prior for a cohort, but
    /// only if the controller has no state for it yet. A freshly elected
    /// controller seeds from the published plans before its first tick so it
    /// keeps the existing assignment instead of recomputing a different balanced
    /// one (which would needlessly churn the cluster on every leader change).
    pub fn seed_published(
        &mut self,
        key: ConsumerGroupKey,
        assignment: HashMap<Partition, String>,
    ) {
        let mut by_member: HashMap<String, Vec<Partition>> = HashMap::new();
        for (partition, member) in assignment {
            by_member.entry(member).or_default().push(partition);
        }
        self.groups.seed_if_absent(key, by_member);
    }

    /// Compute the assignment for every cohort in `cohorts` (already globally
    /// aggregated). `partition_count` resolves the queue's full partition count
    /// for a cohort key (min 1). A cohort with no members yields an empty plan so
    /// owners can reopen its partitions.
    pub fn plan(
        &mut self,
        cohorts: Vec<GlobalCohortMembership>,
        partition_count: impl Fn(&ConsumerGroupKey) -> u32,
    ) -> Vec<CohortPlan> {
        cohorts
            .into_iter()
            .map(|cohort| {
                let count = partition_count(&cohort.key).max(1);
                let partitions: Vec<Partition> = (0..count).map(Partition::new).collect();
                let members: Vec<String> =
                    cohort.members.iter().map(|m| m.member.clone()).collect();
                let member_targets: HashMap<String, usize> = cohort
                    .members
                    .iter()
                    .filter_map(|m| m.target.map(|t| (m.member.clone(), t)))
                    .collect();

                let delta =
                    self.groups
                        .reconcile(cohort.key.clone(), partitions, members, member_targets);
                let mut assignment = HashMap::new();
                for (member, change) in delta.per_member {
                    for partition in change.assigned {
                        assignment.insert(partition, member.clone());
                    }
                }
                CohortPlan {
                    key: cohort.key,
                    assignment,
                }
            })
            .collect()
    }
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
            a.topic.as_str(),
            a.partition,
            a.group.as_deref().unwrap_or_default(),
        )
            .cmp(&(
                b.topic.as_str(),
                b.partition,
                b.group.as_deref().unwrap_or_default(),
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

/// One node's role change for a stream partition across a coordination snapshot
/// diff. The stream analogue of [`LocalAssignmentTransition`], over
/// [`StreamAssignment`] (owner + replica followers, no consumer leases). Reuses
/// the shared [`LocalAssignmentIntent`]/[`LocalAssignmentRole`] vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalStreamAssignmentTransition {
    pub stream: StreamIdentity,
    pub previous_role: Option<LocalAssignmentRole>,
    pub next_role: Option<LocalAssignmentRole>,
    pub previous: Option<StreamAssignment>,
    pub next: Option<StreamAssignment>,
    pub intent: LocalAssignmentIntent,
}

/// Diff `previous` -> `next` stream assignments into this node's role changes,
/// mirroring [`plan_local_assignment_transitions`] for streams. The apply is
/// simpler than queues (no leases/offset release): BecomeOwner opens the
/// channel, BecomeFollower starts a stream-replication worker.
pub fn plan_local_stream_transitions(
    node_id: &str,
    previous: &CoordinationSnapshot,
    next: &CoordinationSnapshot,
) -> Vec<LocalStreamAssignmentTransition> {
    let mut keys: HashSet<StreamIdentity> = previous.stream_assignments.keys().cloned().collect();
    keys.extend(next.stream_assignments.keys().cloned());

    let mut keys: Vec<_> = keys.into_iter().collect();
    keys.sort_by(|a, b| stream_sort_key(a).cmp(&stream_sort_key(b)));

    keys.into_iter()
        .filter_map(|stream| {
            let previous_assignment = previous.stream_assignments.get(&stream).cloned();
            let next_assignment = next.stream_assignments.get(&stream).cloned();
            let previous_role = previous_assignment
                .as_ref()
                .and_then(|assignment| local_stream_role_for(node_id, assignment));
            let next_role = next_assignment
                .as_ref()
                .and_then(|assignment| local_stream_role_for(node_id, assignment));
            let intent = local_stream_assignment_intent(
                previous_role,
                next_role,
                previous_assignment.as_ref(),
                next_assignment.as_ref(),
            );

            (intent != LocalAssignmentIntent::Noop).then_some(LocalStreamAssignmentTransition {
                stream,
                previous_role,
                next_role,
                previous: previous_assignment,
                next: next_assignment,
                intent,
            })
        })
        .collect()
}

fn local_stream_role_for(
    node_id: &str,
    assignment: &StreamAssignment,
) -> Option<LocalAssignmentRole> {
    if assignment.is_owned_by(node_id) {
        Some(LocalAssignmentRole::Owner)
    } else if assignment.is_followed_by(node_id) {
        Some(LocalAssignmentRole::Follower)
    } else {
        None
    }
}

fn local_stream_assignment_intent(
    previous_role: Option<LocalAssignmentRole>,
    next_role: Option<LocalAssignmentRole>,
    previous: Option<&StreamAssignment>,
    next: Option<&StreamAssignment>,
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
        partition: Partition,
        group: Option<&str>,
    ) -> Option<PartitionAssignment> {
        self.snapshot()
            .assignment_for(topic, partition, group)
            .cloned()
    }

    fn owns_queue(&self, topic: &str, partition: Partition, group: Option<&str>) -> bool {
        self.assignment_for(topic, partition, group)
            .is_some_and(|assignment| assignment.is_owned_by(self.node_id()))
    }

    fn follows_queue(&self, topic: &str, partition: Partition, group: Option<&str>) -> bool {
        self.assignment_for(topic, partition, group)
            .is_some_and(|assignment| assignment.is_followed_by(self.node_id()))
    }

    fn owner_for(
        &self,
        topic: &str,
        partition: Partition,
        group: Option<&str>,
    ) -> Option<NodeInfo> {
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

    pub fn single_node(node_id: impl Into<String>, broker_addr: impl Into<String>) -> Self {
        let broker_addr = broker_addr.into();
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
                stream_assignments: HashMap::new(),
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
            inner: StaticCoordination::single_node("local", "127.0.0.1:0"),
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

    fn owns_queue(&self, _topic: &str, _partition: Partition, _group: Option<&str>) -> bool {
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
                    broker_addr: format!("127.0.0.1:{port}"),
                    admin_addr: None,
                },
            );
        }

        let owned = QueueIdentity::new("contract-owned", Partition::new(0), Some("workers"));
        let followed = QueueIdentity::new("contract-followed", Partition::new(1), None);
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
            stream_assignments: HashMap::new(),
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
                .assignment_for("contract-owned", Partition::new(0), Some("workers"))
                .is_none()
        );
        assert!(!provider.owns_queue("contract-owned", Partition::new(0), Some("workers")));
        assert!(!provider.follows_queue("contract-followed", Partition::new(1), None));
        assert!(
            provider
                .owner_for("contract-owned", Partition::new(0), Some("workers"))
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
        assert!(provider.owns_queue("contract-owned", Partition::new(0), Some("workers")));
        assert!(!provider.follows_queue("contract-owned", Partition::new(0), Some("workers")));
        assert!(provider.follows_queue("contract-followed", Partition::new(1), None));
        let owner = provider
            .owner_for("contract-owned", Partition::new(0), Some("workers"))
            .expect("owner must resolve from the committed snapshot");
        assert_eq!(owner.node_id, local_node_id);
        assert_eq!(provider.owned_assignments().len(), 1);
        assert_eq!(provider.follower_assignments().len(), 1);
        let assignment = provider
            .assignment_for("contract-owned", Partition::new(0), Some("workers"))
            .expect("assignment must resolve");
        assert_eq!(assignment.epoch, 1);

        // Ownership moves away: role queries must flip with the snapshot.
        let second = sample_snapshot(local_node_id, 2, "other-node");
        commit(provider, second.clone());

        assert_eq!(provider.snapshot(), second);
        assert!(!provider.owns_queue("contract-owned", Partition::new(0), Some("workers")));
        let moved = provider
            .assignment_for("contract-owned", Partition::new(0), Some("workers"))
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
                broker_addr: "127.0.0.1:1001".to_string(),
                admin_addr: Some("127.0.0.1:2001".parse().unwrap()),
            },
        );
        nodes.insert(
            "node-b".to_string(),
            NodeInfo {
                node_id: "node-b".to_string(),
                broker_addr: "127.0.0.1:1002".to_string(),
                admin_addr: None,
            },
        );

        let queue = QueueIdentity::new("emails", Partition::new(0), Some("workers"));
        let mut assignments = HashMap::new();
        assignments.insert(
            queue.clone(),
            PartitionAssignment::new(queue, "node-a", vec!["node-b".to_string()], 7),
        );

        CoordinationSnapshot {
            nodes,
            assignments,
            stream_assignments: HashMap::new(),
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
                stream_assignments: HashMap::new(),
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

        assert!(coordination.owns_queue("emails", Partition::new(0), Some("workers")));
        assert!(!coordination.follows_queue("emails", Partition::new(0), Some("workers")));

        let owner = coordination
            .owner_for("emails", Partition::new(0), Some("workers"))
            .expect("owner node");
        assert_eq!(owner.node_id, "node-a");
        assert_eq!(owner.broker_addr, "127.0.0.1:1001");

        let owned = coordination.owned_assignments();
        assert_eq!(owned.len(), 1);
        assert_eq!(owned[0].epoch, 7);
    }

    #[test]
    fn static_coordination_reports_local_follow_assignments() {
        let coordination = StaticCoordination::new("node-b", snapshot());

        assert!(!coordination.owns_queue("emails", Partition::new(0), Some("workers")));
        assert!(coordination.follows_queue("emails", Partition::new(0), Some("workers")));

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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
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
                queues: vec![QueueIdentity::new("emails", Partition::new(0), None)],
                existing: HashMap::new(),
                target_followers: 1,
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
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
                    QueueIdentity::new("orders", Partition::new(0), None),
                    QueueIdentity::new("emails", Partition::new(0), None),
                    QueueIdentity::new("tasks", Partition::new(0), None),
                ],
                existing: HashMap::new(),
                target_followers: 0,
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
                generation: 3,
            })
            .unwrap();

        assert_eq!(assignment_owner(&plan.snapshot, "emails"), "node-a");
        assert_eq!(assignment_owner(&plan.snapshot, "orders"), "node-b");
        assert_eq!(assignment_owner(&plan.snapshot, "tasks"), "node-a");
    }

    #[test]
    fn placement_spreads_one_queues_partitions_across_distinct_nodes() {
        // A single queue's partitions must land on distinct nodes (up to the
        // node count) before any node is reused — so small clusters don't clump
        // one queue onto a few nodes. See memory placement-spreads-partitions-first.
        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: nodes(["node-a", "node-b", "node-c"]),
                queues: (0..3)
                    .map(|p| QueueIdentity::new("orders", Partition::new(p), None))
                    .collect(),
                existing: HashMap::new(),
                target_followers: 0,
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
                generation: 1,
            })
            .unwrap();

        let owners: std::collections::HashSet<String> = (0..3)
            .map(|p| {
                plan.snapshot
                    .assignment_for("orders", Partition::new(p), None)
                    .expect("assignment")
                    .owner
                    .clone()
            })
            .collect();
        assert_eq!(
            owners.len(),
            3,
            "3 partitions should occupy 3 distinct nodes"
        );
    }

    fn stream_target_followers(topic: &str, count: usize) -> HashMap<String, usize> {
        let mut map = HashMap::new();
        map.insert(topic.to_string(), count);
        map
    }

    #[test]
    fn stream_placement_allows_empty_set_without_nodes() {
        let plan = DeterministicStreamPlacement
            .plan(StreamPlacementInput {
                streams: Vec::new(),
                generation: 5,
                ..Default::default()
            })
            .unwrap();

        assert!(plan.assignments.is_empty());
    }

    #[test]
    fn stream_placement_requires_nodes_when_streams_exist() {
        let err = DeterministicStreamPlacement
            .plan(StreamPlacementInput {
                streams: vec![StreamIdentity::new("events", Partition::new(0))],
                generation: 1,
                ..Default::default()
            })
            .unwrap_err();

        assert_eq!(err, PartitionPlacementError::NoNodesForQueues);
    }

    #[test]
    fn stream_placement_spreads_partitions_across_distinct_nodes() {
        // One stream's partitions land on distinct nodes before any node is
        // reused, the same small-cluster balance goal as queue placement.
        let plan = DeterministicStreamPlacement
            .plan(StreamPlacementInput {
                nodes: nodes(["node-a", "node-b", "node-c"]),
                streams: (0..3)
                    .map(|p| StreamIdentity::new("events", Partition::new(p)))
                    .collect(),
                generation: 1,
                ..Default::default()
            })
            .unwrap();

        let owners: std::collections::HashSet<String> = (0..3)
            .map(|p| {
                plan.assignments
                    .get(&StreamIdentity::new("events", Partition::new(p)))
                    .expect("assignment")
                    .owner
                    .clone()
            })
            .collect();
        assert_eq!(
            owners.len(),
            3,
            "3 partitions should occupy 3 distinct nodes"
        );
    }

    #[test]
    fn stream_placement_keeps_live_owner_and_holds_epoch() {
        let stream = StreamIdentity::new("events", Partition::new(0));
        let mut existing = HashMap::new();
        existing.insert(
            stream.clone(),
            StreamAssignment::new(stream.clone(), "node-b", Vec::new(), 4),
        );

        let plan = DeterministicStreamPlacement
            .plan(StreamPlacementInput {
                nodes: nodes(["node-a", "node-b"]),
                streams: vec![stream.clone()],
                existing,
                generation: 9,
                ..Default::default()
            })
            .unwrap();

        let assignment = plan.assignments.get(&stream).expect("assignment");
        assert_eq!(assignment.owner, "node-b", "live owner is sticky");
        assert_eq!(assignment.epoch, 4, "unchanged owner holds its epoch");
    }

    #[test]
    fn stream_placement_reassigns_dead_owner_and_bumps_epoch() {
        let stream = StreamIdentity::new("events", Partition::new(0));
        let mut existing = HashMap::new();
        existing.insert(
            stream.clone(),
            StreamAssignment::new(stream.clone(), "node-gone", Vec::new(), 4),
        );

        let plan = DeterministicStreamPlacement
            .plan(StreamPlacementInput {
                nodes: nodes(["node-a", "node-b"]),
                streams: vec![stream.clone()],
                existing,
                generation: 9,
                ..Default::default()
            })
            .unwrap();

        let assignment = plan.assignments.get(&stream).expect("assignment");
        assert_ne!(assignment.owner, "node-gone", "dead owner is replaced");
        assert_eq!(assignment.epoch, 9, "owner change fences via a new epoch");
    }

    #[test]
    fn stream_placement_spreads_followers_for_the_durable_tier() {
        // A durable stream (target_followers > 0) gets distinct-node replicas;
        // owner-only tiers (absent / 0) get none.
        let durable = StreamIdentity::new("durable", Partition::new(0));
        let ephemeral = StreamIdentity::new("ephemeral", Partition::new(0));
        let plan = DeterministicStreamPlacement
            .plan(StreamPlacementInput {
                nodes: nodes(["node-a", "node-b", "node-c"]),
                streams: vec![durable.clone(), ephemeral.clone()],
                target_followers: stream_target_followers("durable", 2),
                generation: 1,
                ..Default::default()
            })
            .unwrap();

        let durable_assignment = plan.assignments.get(&durable).expect("durable assigned");
        assert_eq!(
            durable_assignment.followers.len(),
            2,
            "durable stream replicates to its follower count"
        );
        assert!(
            !durable_assignment
                .followers
                .contains(&durable_assignment.owner),
            "the owner is never its own follower"
        );

        let ephemeral_assignment = plan
            .assignments
            .get(&ephemeral)
            .expect("ephemeral assigned");
        assert!(
            ephemeral_assignment.followers.is_empty(),
            "owner-only tiers get no followers"
        );
    }

    #[test]
    fn stream_placement_bumps_epoch_on_follower_churn() {
        // A follower-set change (e.g. RF grew) re-stamps the epoch even though the
        // owner is unchanged, mirroring queue placement.
        let stream = StreamIdentity::new("durable", Partition::new(0));
        let mut existing = HashMap::new();
        existing.insert(
            stream.clone(),
            StreamAssignment::new(stream.clone(), "node-a", vec!["node-b".to_string()], 4),
        );

        let plan = DeterministicStreamPlacement
            .plan(StreamPlacementInput {
                nodes: nodes(["node-a", "node-b", "node-c"]),
                streams: vec![stream.clone()],
                existing,
                target_followers: stream_target_followers("durable", 2),
                generation: 9,
            })
            .unwrap();

        let assignment = plan.assignments.get(&stream).expect("assignment");
        assert_eq!(assignment.owner, "node-a", "live owner stays");
        assert_eq!(assignment.followers.len(), 2, "follower set grew to RF");
        assert_eq!(assignment.epoch, 9, "follower churn re-stamps the epoch");
    }

    fn stream_snapshot(assignments: Vec<StreamAssignment>) -> CoordinationSnapshot {
        CoordinationSnapshot {
            stream_assignments: assignments
                .into_iter()
                .map(|assignment| (assignment.stream.clone(), assignment))
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn plan_local_stream_transitions_become_owner_and_follower() {
        let stream = StreamIdentity::new("events", Partition::new(0));
        let previous = stream_snapshot(Vec::new());
        let next = stream_snapshot(vec![StreamAssignment::new(
            stream.clone(),
            "node-a",
            vec!["node-b".to_string()],
            1,
        )]);

        let owner = plan_local_stream_transitions("node-a", &previous, &next);
        assert_eq!(owner.len(), 1);
        assert_eq!(owner[0].intent, LocalAssignmentIntent::BecomeOwner);

        let follower = plan_local_stream_transitions("node-b", &previous, &next);
        assert_eq!(follower.len(), 1);
        assert_eq!(follower[0].intent, LocalAssignmentIntent::BecomeFollower);

        // A node mentioned in neither assignment has nothing to do.
        assert!(plan_local_stream_transitions("node-c", &previous, &next).is_empty());
    }

    #[test]
    fn plan_local_stream_transitions_promote_and_demote_on_failover() {
        let stream = StreamIdentity::new("events", Partition::new(0));
        let previous = stream_snapshot(vec![StreamAssignment::new(
            stream.clone(),
            "node-a",
            vec!["node-b".to_string()],
            1,
        )]);
        // Owner moves a -> b (b was the follower); epoch bumps.
        let next = stream_snapshot(vec![StreamAssignment::new(
            stream.clone(),
            "node-b",
            vec!["node-a".to_string()],
            2,
        )]);

        let demoted = plan_local_stream_transitions("node-a", &previous, &next);
        assert_eq!(
            demoted[0].intent,
            LocalAssignmentIntent::DemoteOwnerToFollower
        );

        let promoted = plan_local_stream_transitions("node-b", &previous, &next);
        assert_eq!(
            promoted[0].intent,
            LocalAssignmentIntent::PromoteFollowerToOwner
        );
    }

    #[test]
    fn plan_local_stream_transitions_noop_when_unchanged() {
        let stream = StreamIdentity::new("events", Partition::new(0));
        let assignment = StreamAssignment::new(stream, "node-a", vec!["node-b".to_string()], 1);
        let snapshot = stream_snapshot(vec![assignment]);
        assert!(plan_local_stream_transitions("node-a", &snapshot, &snapshot).is_empty());
        assert!(plan_local_stream_transitions("node-b", &snapshot, &snapshot).is_empty());
    }

    fn assign_balanced(
        partitions: u32,
        members: &[&str],
        target: Option<usize>,
    ) -> ConsumerGroupAssignment {
        BalancedConsumerGroupAssignor.assign(ConsumerGroupAssignmentInput {
            partitions: (0..partitions).map(Partition::new).collect(),
            members: members.iter().map(|m| m.to_string()).collect(),
            default_target_per_consumer: target,
            ..Default::default()
        })
    }

    fn assign_with_targets(
        partitions: u32,
        members: &[&str],
        default_target: Option<usize>,
        member_targets: &[(&str, usize)],
    ) -> ConsumerGroupAssignment {
        BalancedConsumerGroupAssignor.assign(ConsumerGroupAssignmentInput {
            partitions: (0..partitions).map(Partition::new).collect(),
            members: members.iter().map(|m| m.to_string()).collect(),
            default_target_per_consumer: default_target,
            member_targets: member_targets
                .iter()
                .map(|(m, t)| (m.to_string(), *t))
                .collect(),
            ..Default::default()
        })
    }

    fn assign_sticky(
        partitions: u32,
        members: &[&str],
        current: &[(&str, &[u32])],
    ) -> ConsumerGroupAssignment {
        StickyConsumerGroupAssignor.assign(ConsumerGroupAssignmentInput {
            partitions: (0..partitions).map(Partition::new).collect(),
            members: members.iter().map(|m| m.to_string()).collect(),
            current: current
                .iter()
                .map(|(m, parts)| {
                    (
                        m.to_string(),
                        parts.iter().copied().map(Partition::new).collect(),
                    )
                })
                .collect(),
            ..Default::default()
        })
    }

    #[test]
    fn group_assignment_is_balanced_and_covers_all_partitions() {
        let assignment = assign_balanced(9, &["c1", "c2"], None);
        let mut loads: Vec<usize> = assignment.by_member.values().map(Vec::len).collect();
        loads.sort();
        assert_eq!(loads, vec![4, 5], "9 partitions over 2 consumers => 5 + 4");

        // Coverage: every partition assigned exactly once.
        let mut covered: Vec<u32> = assignment
            .by_member
            .values()
            .flatten()
            .map(|p| p.id())
            .collect();
        covered.sort();
        assert_eq!(covered, (0..9).collect::<Vec<_>>());
        assert!(!assignment.under_provisioned);
    }

    #[test]
    fn group_assignment_leaves_extra_members_idle() {
        // More members than partitions => some idle, none over-provisioned.
        let assignment = assign_balanced(2, &["c1", "c2", "c3"], None);
        let idle = assignment
            .by_member
            .values()
            .filter(|partitions| partitions.is_empty())
            .count();
        assert_eq!(idle, 1);
        assert!(!assignment.under_provisioned);
    }

    #[test]
    fn group_assignment_with_no_members_assigns_nothing() {
        let assignment = assign_balanced(4, &[], None);
        assert!(assignment.by_member.is_empty());
        assert!(!assignment.under_provisioned);
    }

    #[test]
    fn group_assignment_covers_then_flags_under_provisioned() {
        // 9 partitions, target 3/consumer, only 2 consumers: still fully covered
        // (5 + 4, exceeding the target) and flagged under-provisioned.
        let assignment = assign_balanced(9, &["c1", "c2"], Some(3));
        let mut covered: Vec<u32> = assignment
            .by_member
            .values()
            .flatten()
            .map(|p| p.id())
            .collect();
        covered.sort();
        assert_eq!(
            covered,
            (0..9).collect::<Vec<_>>(),
            "coverage is never capped"
        );
        assert!(assignment.under_provisioned);
    }

    fn member_load(assignment: &ConsumerGroupAssignment, member: &str) -> usize {
        assignment.by_member.get(member).map_or(0, Vec::len)
    }

    #[test]
    fn group_assignment_member_target_caps_one_member_others_absorb() {
        // c1 caps itself at 1 partition; c2 is uncapped (no default) and absorbs
        // the rest. Coverage is preserved and nobody is over their target.
        let assignment = assign_with_targets(6, &["c1", "c2"], None, &[("c1", 1)]);
        assert_eq!(member_load(&assignment, "c1"), 1, "c1 honors its soft cap");
        assert_eq!(
            member_load(&assignment, "c2"),
            5,
            "c2 absorbs the remainder"
        );
        let mut covered: Vec<u32> = assignment
            .by_member
            .values()
            .flatten()
            .map(|p| p.id())
            .collect();
        covered.sort();
        assert_eq!(covered, (0..6).collect::<Vec<_>>());
        assert!(
            !assignment.under_provisioned,
            "c2 is uncapped, so not under target"
        );
    }

    #[test]
    fn group_assignment_member_target_overrides_group_default() {
        // Default target 2 for everyone, but c1 overrides to 4. With 6 partitions
        // c1 takes up to 4 and c2 the rest; coverage preserved.
        let assignment = assign_with_targets(6, &["c1", "c2"], Some(2), &[("c1", 4)]);
        // Greedy keeps loads balanced until a cap binds: both fill to 2 (c1,c2),
        // then only c1 may continue (c2 at its default cap of 2) up to c1's 4.
        assert_eq!(
            member_load(&assignment, "c1"),
            4,
            "c1's override raises its cap"
        );
        assert_eq!(
            member_load(&assignment, "c2"),
            2,
            "c2 stays at the group default"
        );
        assert!(
            !assignment.under_provisioned,
            "both members land exactly at their cap (4 + 2 == 6), none over target"
        );
    }

    #[test]
    fn group_assignment_all_capped_below_total_overflows_and_flags() {
        // Two members each capped at 1, three partitions: coverage forces one
        // member over its cap and flags under-provisioned.
        let assignment = assign_with_targets(3, &["c1", "c2"], None, &[("c1", 1), ("c2", 1)]);
        let mut covered: Vec<u32> = assignment
            .by_member
            .values()
            .flatten()
            .map(|p| p.id())
            .collect();
        covered.sort();
        assert_eq!(covered, vec![0, 1, 2], "coverage wins over capacity");
        assert!(assignment.under_provisioned);
    }

    #[test]
    fn sticky_first_assignment_matches_balanced() {
        // With no prior assignment, sticky == balanced (same loads + coverage).
        let sticky = assign_sticky(9, &["c1", "c2"], &[]);
        let mut loads: Vec<usize> = sticky.by_member.values().map(Vec::len).collect();
        loads.sort();
        assert_eq!(loads, vec![4, 5]);
        let mut covered: Vec<u32> = sticky
            .by_member
            .values()
            .flatten()
            .map(|p| p.id())
            .collect();
        covered.sort();
        assert_eq!(covered, (0..9).collect::<Vec<_>>());
    }

    #[test]
    fn sticky_join_moves_only_what_balance_requires() {
        // c1={0,2}, c2={1,3}; c3 joins. Balanced target loads are 2,1,1. Sticky
        // keeps c1's two, keeps one of c2's, and moves exactly ONE partition to
        // c3 — versus the stateless deal which would reshuffle more.
        let sticky = assign_sticky(4, &["c1", "c2", "c3"], &[("c1", &[0, 2]), ("c2", &[1, 3])]);
        assert_eq!(
            sorted_ids(&sticky.by_member["c1"]),
            vec![0, 2],
            "c1 keeps both"
        );
        assert_eq!(sticky.by_member["c2"].len(), 1, "c2 keeps one");
        assert_eq!(sticky.by_member["c3"].len(), 1, "c3 gets exactly one");
        // Exactly one partition changed owner (the one c2 gave up went to c3).
        let moved = sticky.by_member["c3"][0].id();
        assert!(
            moved == 1 || moved == 3,
            "c3 took one of c2's old partitions"
        );
    }

    #[test]
    fn sticky_leave_redistributes_orphans_and_keeps_survivors() {
        // c2 leaves; its partitions are reassigned to c1/c3 while c1 and c3 keep
        // every partition they already held (no needless movement).
        let sticky = assign_sticky(
            6,
            &["c1", "c3"],
            &[("c1", &[0, 1]), ("c2", &[2, 3]), ("c3", &[4, 5])],
        );
        // c1 and c3 retain their originals; the orphaned 2,3 are split across them.
        assert!(sticky.by_member["c1"].contains(&Partition::new(0)));
        assert!(sticky.by_member["c1"].contains(&Partition::new(1)));
        assert!(sticky.by_member["c3"].contains(&Partition::new(4)));
        assert!(sticky.by_member["c3"].contains(&Partition::new(5)));
        let mut covered: Vec<u32> = sticky
            .by_member
            .values()
            .flatten()
            .map(|p| p.id())
            .collect();
        covered.sort();
        assert_eq!(
            covered,
            (0..6).collect::<Vec<_>>(),
            "orphans reassigned, full coverage"
        );
        let mut loads: Vec<usize> = sticky.by_member.values().map(Vec::len).collect();
        loads.sort();
        assert_eq!(loads, vec![3, 3], "balanced across survivors");
    }

    #[test]
    fn sticky_trims_over_target_holder_on_rebalance() {
        // c1 currently holds everything; c2 joins. Sticky trims c1 down to the
        // balanced target (2) and gives the rest to c2 — c1 keeps its lowest two.
        let sticky = assign_sticky(4, &["c1", "c2"], &[("c1", &[0, 1, 2, 3])]);
        assert_eq!(
            sorted_ids(&sticky.by_member["c1"]),
            vec![0, 1],
            "c1 trimmed to target, keeps lowest"
        );
        assert_eq!(
            sorted_ids(&sticky.by_member["c2"]),
            vec![2, 3],
            "c2 takes the trimmed remainder"
        );
    }

    fn group_state(partitions: u32) -> ConsumerGroupState {
        ConsumerGroupState::new(
            (0..partitions).map(Partition::new).collect(),
            None,
            Arc::new(BalancedConsumerGroupAssignor),
        )
    }

    fn sticky_state(partitions: u32) -> ConsumerGroupState {
        ConsumerGroupState::new(
            (0..partitions).map(Partition::new).collect(),
            None,
            Arc::new(StickyConsumerGroupAssignor),
        )
    }

    /// Min and max member load across the cohort's current assignment.
    fn load_spread(state: &ConsumerGroupState, members: &[String]) -> (usize, usize) {
        let loads: Vec<usize> = members
            .iter()
            .map(|m| state.assignment_for(m).len())
            .collect();
        (
            loads.iter().copied().min().unwrap_or(0),
            loads.iter().copied().max().unwrap_or(0),
        )
    }

    fn member_ids(prefix: &str, count: usize) -> Vec<String> {
        (0..count).map(|i| format!("{prefix}{i:04}")).collect()
    }

    #[test]
    fn sticky_large_scale_add_member_moves_only_to_newcomer() {
        // 100 members over 1000 partitions, then one joins. Stickiness guarantees
        // the join moves the MINIMUM: only the newcomer gains; every existing
        // member only sheds (never gains), and everything it sheds goes to the
        // newcomer. This is optimal — you cannot add a member without handing it
        // ~N/(M+1) partitions.
        let partitions = 1000u32;
        let mut state = sticky_state(partitions);
        let members = member_ids("c", 100);
        state.set_members(members.clone());

        let delta = state.add_member("c0100");
        let newcomer = &delta.per_member["c0100"];
        assert!(
            newcomer.revoked.is_empty(),
            "the newcomer starts from nothing"
        );
        assert_eq!(
            newcomer.added.len(),
            newcomer.assigned.len(),
            "all of the newcomer's partitions are freshly added"
        );

        let mut total_shed = 0;
        for member in &members {
            let change = &delta.per_member[member];
            assert!(
                change.added.is_empty(),
                "no existing member gains partitions on a join"
            );
            total_shed += change.revoked.len();
        }
        assert_eq!(
            total_shed,
            newcomer.added.len(),
            "everything shed by existing members goes to the newcomer"
        );

        // Still balanced (within one) and fully covered.
        let all_members = member_ids("c", 101);
        let (min, max) = load_spread(&state, &all_members);
        assert!(
            max - min <= 1,
            "balanced within one partition (got {min}..{max})"
        );
    }

    #[test]
    fn sticky_large_scale_remove_member_only_orphans_move() {
        // 100 members over 1000 partitions, then one leaves. Survivors keep ALL
        // their partitions; only the departed member's partitions are
        // redistributed.
        let partitions = 1000u32;
        let mut state = sticky_state(partitions);
        let members = member_ids("c", 100);
        state.set_members(members.clone());

        let departing = "c0050";
        let departed_load = state.assignment_for(departing).len();
        let delta = state.remove_member(departing);

        let departed = &delta.per_member[departing];
        assert!(departed.added.is_empty());
        assert_eq!(
            departed.revoked.len(),
            departed_load,
            "departed loses all it held"
        );

        let mut total_gained = 0;
        for member in &members {
            if member == departing {
                continue;
            }
            let change = &delta.per_member[member];
            assert!(
                change.revoked.is_empty(),
                "survivors keep every partition they held"
            );
            total_gained += change.added.len();
        }
        assert_eq!(
            total_gained, departed_load,
            "the departed member's partitions are fully redistributed"
        );
    }

    #[test]
    fn sticky_scales_to_hundreds_of_members_balanced_and_covered() {
        // 300 members over 5000 partitions: full coverage, balanced within one.
        let partitions = 5000u32;
        let mut state = sticky_state(partitions);
        let members = member_ids("m", 300);
        state.set_members(members.clone());

        let mut covered: HashSet<u32> = HashSet::new();
        for member in &members {
            for p in state.assignment_for(member) {
                assert!(covered.insert(p.id()), "no partition assigned twice");
            }
        }
        assert_eq!(
            covered.len(),
            partitions as usize,
            "every partition covered exactly once"
        );

        let (min, max) = load_spread(&state, &members);
        assert!(
            max - min <= 1,
            "balanced within one partition (got {min}..{max})"
        );
    }

    fn local_report(
        topic: &str,
        consumer_group: &str,
        members: &[(&str, Option<usize>)],
    ) -> LocalCohortMembership {
        LocalCohortMembership {
            topic: topic.to_string(),
            group: None,
            consumer_group: consumer_group.to_string(),
            members: members
                .iter()
                .map(|(m, t)| CohortMemberInfo {
                    member: m.to_string(),
                    target: *t,
                })
                .collect(),
        }
    }

    fn cluster_controller() -> ClusterCohortController {
        ClusterCohortController::new(Arc::new(StickyConsumerGroupAssignor), None)
    }

    #[test]
    fn aggregate_unions_members_across_brokers_and_keeps_largest_target() {
        // Same cohort reported by two brokers: m1 on both (it spans them), m2 on
        // one. Deduped by cluster id; largest target wins.
        let reports = vec![
            local_report("jobs", "default", &[("m1", Some(2)), ("m2", None)]),
            local_report("jobs", "default", &[("m1", Some(5))]),
        ];
        let global = aggregate_cohort_membership(reports);
        assert_eq!(global.len(), 1);
        assert_eq!(
            global[0].members,
            vec![
                CohortMemberInfo {
                    member: "m1".into(),
                    target: Some(5),
                },
                CohortMemberInfo {
                    member: "m2".into(),
                    target: None,
                },
            ]
        );
    }

    #[test]
    fn cluster_plan_balances_globally_and_covers_all_partitions() {
        let mut controller = cluster_controller();
        let global = aggregate_cohort_membership(vec![local_report(
            "jobs",
            "default",
            &[("m1", None), ("m2", None)],
        )]);
        let plans = controller.plan(global, |_| 4);
        assert_eq!(plans.len(), 1);
        let assignment = &plans[0].assignment;
        // Every partition assigned exactly once, balanced 2/2.
        let mut covered: Vec<u32> = assignment.keys().map(|p| p.id()).collect();
        covered.sort();
        assert_eq!(covered, vec![0, 1, 2, 3]);
        let mut loads: HashMap<&str, usize> = HashMap::new();
        for member in assignment.values() {
            *loads.entry(member.as_str()).or_default() += 1;
        }
        assert_eq!(loads["m1"], 2);
        assert_eq!(loads["m2"], 2);
    }

    #[test]
    fn cluster_plan_is_sticky_across_ticks() {
        let mut controller = cluster_controller();
        let key = ConsumerGroupKey::new("jobs", None, "default");

        // Tick 1: m1 alone owns all 4 partitions.
        let first = controller.plan(
            aggregate_cohort_membership(vec![local_report("jobs", "default", &[("m1", None)])]),
            |_| 4,
        );
        assert_eq!(first[0].assignment.len(), 4);

        // Tick 2: m2 joins. m1 keeps the partitions it still holds (sticky); only
        // the minimum move to m2.
        let second = controller.plan(
            aggregate_cohort_membership(vec![local_report(
                "jobs",
                "default",
                &[("m1", None), ("m2", None)],
            )]),
            |_| 4,
        );
        let assignment = &second[0].assignment;
        let m1_kept = assignment.values().filter(|m| *m == "m1").count();
        let m2_got = assignment.values().filter(|m| *m == "m2").count();
        assert_eq!(m1_kept, 2, "m1 keeps half (sticky), not reshuffled");
        assert_eq!(m2_got, 2);
        assert_eq!(plans_key(second), key);
    }

    fn plans_key(plans: Vec<CohortPlan>) -> ConsumerGroupKey {
        plans.into_iter().next().unwrap().key
    }

    #[test]
    fn seeded_controller_keeps_published_plan_across_leader_change() {
        let key = ConsumerGroupKey::new("jobs", None, "default");
        // A previously published plan that a fresh assignor would not necessarily
        // reproduce on its own.
        let published = HashMap::from([
            (Partition::new(0), "m2".to_string()),
            (Partition::new(1), "m2".to_string()),
            (Partition::new(2), "m1".to_string()),
            (Partition::new(3), "m1".to_string()),
        ]);

        // A freshly elected controller seeds from the published plan before its
        // first tick, then plans over the same membership.
        let mut seeded = cluster_controller();
        seeded.seed_published(key.clone(), published.clone());
        let plans = seeded.plan(
            aggregate_cohort_membership(vec![local_report(
                "jobs",
                "default",
                &[("m1", None), ("m2", None)],
            )]),
            |_| 4,
        );
        assert_eq!(
            plans[0].assignment, published,
            "seeded controller keeps the published plan (no leader-change churn)"
        );

        // Seeding is a no-op once the cohort is tracked: a later seed with a
        // different plan must not clobber the live assignment.
        seeded.seed_published(
            key.clone(),
            HashMap::from([(Partition::new(0), "m1".to_string())]),
        );
        let again = seeded.plan(
            aggregate_cohort_membership(vec![local_report(
                "jobs",
                "default",
                &[("m1", None), ("m2", None)],
            )]),
            |_| 4,
        );
        assert_eq!(
            again[0].assignment, published,
            "live state is not clobbered"
        );
    }

    #[test]
    fn cluster_plan_with_no_members_is_empty() {
        let mut controller = cluster_controller();
        let global = aggregate_cohort_membership(vec![local_report("jobs", "default", &[])]);
        // An empty report still names the cohort; plan yields an empty assignment.
        let plans = controller.plan(global, |_| 3);
        assert_eq!(plans.len(), 1);
        assert!(plans[0].assignment.is_empty());
    }

    fn sorted_ids(parts: &[Partition]) -> Vec<u32> {
        let mut ids: Vec<u32> = parts.iter().map(|p| p.id()).collect();
        ids.sort();
        ids
    }

    #[test]
    fn group_state_first_member_gets_all_partitions() {
        let mut state = group_state(4);
        let delta = state.add_member("c1");
        assert_eq!(delta.generation, 1);
        let change = &delta.per_member["c1"];
        assert_eq!(sorted_ids(&change.assigned), vec![0, 1, 2, 3]);
        assert_eq!(sorted_ids(&change.added), vec![0, 1, 2, 3]);
        assert!(change.revoked.is_empty());
    }

    #[test]
    fn group_state_join_revokes_from_existing_and_keeps_coverage() {
        let mut state = group_state(4);
        state.add_member("c1");
        let delta = state.add_member("c2");
        assert_eq!(delta.generation, 2);

        // c1 gives some up; c2 receives them; union still covers all 4.
        let c1 = &delta.per_member["c1"];
        let c2 = &delta.per_member["c2"];
        assert!(!c1.revoked.is_empty(), "c1 should yield partitions to c2");
        assert_eq!(sorted_ids(&c2.added), sorted_ids(&c2.assigned));
        let mut covered = sorted_ids(&c1.assigned);
        covered.extend(sorted_ids(&c2.assigned));
        covered.sort();
        assert_eq!(covered, vec![0, 1, 2, 3]);
    }

    #[test]
    fn group_state_leave_redistributes_revoked_partitions() {
        let mut state = group_state(4);
        state.add_member("c1");
        state.add_member("c2");
        let delta = state.remove_member("c1");

        // c1 loses everything; c2 ends up covering all 4.
        let c1 = &delta.per_member["c1"];
        assert!(c1.assigned.is_empty());
        assert_eq!(sorted_ids(&c1.revoked).len(), 2);
        assert_eq!(
            sorted_ids(&state.assignment_for("c2").to_vec()),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn group_state_noop_rebalance_does_not_bump_generation() {
        let mut state = group_state(4);
        state.add_member("c1");
        let prev_gen = state.generation();
        let delta = state.set_members(["c1".to_string()]);
        assert_eq!(
            delta.generation, prev_gen,
            "re-applying the same membership is a no-op"
        );
    }

    fn exclusive_groups() -> ExclusiveConsumerGroups {
        ExclusiveConsumerGroups::new(Arc::new(BalancedConsumerGroupAssignor), None)
    }

    fn parts(n: u32) -> Vec<Partition> {
        (0..n).map(Partition::new).collect()
    }

    #[test]
    fn exclusive_registry_assigns_and_redistributes_within_a_group() {
        let mut registry = exclusive_groups();
        let key = ConsumerGroupKey::new("jobs", None, "g1");

        registry.join(key.clone(), parts(2), "c1");
        assert_eq!(sorted_ids(registry.assignment_for(&key, "c1")), vec![0, 1]);

        // Second member joins -> partitions split across c1 and c2.
        registry.join(key.clone(), parts(2), "c2");
        let mut covered = sorted_ids(registry.assignment_for(&key, "c1"));
        covered.extend(sorted_ids(registry.assignment_for(&key, "c2")));
        covered.sort();
        assert_eq!(covered, vec![0, 1], "both partitions still covered");
        assert_eq!(registry.assignment_for(&key, "c1").len(), 1);
        assert_eq!(registry.assignment_for(&key, "c2").len(), 1);
    }

    #[test]
    fn exclusive_registry_drops_group_when_last_member_leaves() {
        let mut registry = exclusive_groups();
        let key = ConsumerGroupKey::new("jobs", None, "g1");
        registry.join(key.clone(), parts(2), "c1");

        assert!(registry.leave(&key, "c1").is_some());
        // Group gone: unknown member assignment is empty, and leaving again is None.
        assert!(registry.assignment_for(&key, "c1").is_empty());
        assert!(registry.leave(&key, "c1").is_none());
    }

    #[test]
    fn exclusive_registry_reconcile_recomputes_from_authoritative_view() {
        let mut registry = exclusive_groups();
        let key = ConsumerGroupKey::new("jobs", None, "g1");

        // One member covers everything.
        registry.reconcile(
            key.clone(),
            parts(4),
            vec!["c1".to_string()],
            HashMap::new(),
        );
        assert_eq!(
            sorted_ids(registry.assignment_for(&key, "c1")),
            vec![0, 1, 2, 3]
        );

        // Second member joins -> balanced split, full coverage preserved.
        registry.reconcile(
            key.clone(),
            parts(4),
            vec!["c1".to_string(), "c2".to_string()],
            HashMap::new(),
        );
        let mut covered = sorted_ids(registry.assignment_for(&key, "c1"));
        covered.extend(sorted_ids(registry.assignment_for(&key, "c2")));
        covered.sort();
        assert_eq!(covered, vec![0, 1, 2, 3]);
        assert_eq!(registry.assignment_for(&key, "c1").len(), 2);
        assert_eq!(registry.assignment_for(&key, "c2").len(), 2);
    }

    #[test]
    fn exclusive_registry_reconcile_to_no_members_drops_group() {
        let mut registry = exclusive_groups();
        let key = ConsumerGroupKey::new("jobs", None, "g1");
        registry.reconcile(
            key.clone(),
            parts(2),
            vec!["c1".to_string()],
            HashMap::new(),
        );

        // Emptying membership reports full revocation and drops the group.
        let delta = registry.reconcile(key.clone(), parts(2), Vec::new(), HashMap::new());
        let c1 = &delta.per_member["c1"];
        assert!(c1.assigned.is_empty());
        assert_eq!(sorted_ids(&c1.revoked), vec![0, 1]);
        assert!(registry.assignment_for(&key, "c1").is_empty());
    }

    #[test]
    fn exclusive_registry_groups_are_independent() {
        let mut registry = exclusive_groups();
        let g1 = ConsumerGroupKey::new("jobs", None, "g1");
        let g2 = ConsumerGroupKey::new("jobs", None, "g2");
        registry.join(g1.clone(), parts(2), "c1");
        registry.join(g2.clone(), parts(2), "c2");

        // Each group independently covers all partitions (separate exclusive cohorts).
        assert_eq!(sorted_ids(registry.assignment_for(&g1, "c1")), vec![0, 1]);
        assert_eq!(sorted_ids(registry.assignment_for(&g2, "c2")), vec![0, 1]);
    }

    #[test]
    fn placement_caps_followers_and_excludes_owner() {
        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: nodes(["node-a", "node-b", "node-c"]),
                queues: vec![QueueIdentity::new("emails", Partition::new(0), None)],
                existing: HashMap::new(),
                target_followers: 8,
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
                generation: 4,
            })
            .unwrap();

        let assignment = plan
            .snapshot
            .assignment_for("emails", Partition::new(0), None)
            .expect("assignment");
        assert_eq!(assignment.owner, "node-a");
        assert_eq!(assignment.followers, vec!["node-b", "node-c"]);
    }

    #[test]
    fn placement_preserves_existing_owner_and_fills_followers() {
        let queue = QueueIdentity::new("emails", Partition::new(0), None);
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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
                generation: 12,
            })
            .unwrap();

        let assignment = plan
            .snapshot
            .assignment_for("emails", Partition::new(0), None)
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
        let queue = QueueIdentity::new("emails", Partition::new(0), None);
        let existing_assignment =
            PartitionAssignment::new(queue.clone(), "node-a", vec!["node-b".to_string()], 7);
        let existing = HashMap::from([(queue.clone(), existing_assignment)]);

        let plan = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: nodes(["node-a", "node-b"]),
                queues: vec![queue],
                existing,
                target_followers: 1,
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
                generation: 8,
            })
            .unwrap();

        let assignment = plan
            .snapshot
            .assignment_for("emails", Partition::new(0), None)
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
                        broker_addr: format!("127.0.0.1:{}", 10_000 + idx),
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
                let partition = Partition::new((idx % 4) as u32);
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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
                generation: 1,
            })
            .unwrap();

        let recovered = DeterministicPartitionPlacement
            .plan(PartitionPlacementInput {
                nodes: survivors.clone(),
                queues: queues.clone(),
                existing: stale_plan.snapshot.assignments,
                target_followers: TARGET_FOLLOWERS,
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
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
        let existing_queue = QueueIdentity::new("emails", Partition::new(0), None);
        let new_queue = QueueIdentity::new("orders", Partition::new(0), None);
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
                default_durability: ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 },
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
        let new_assignment = plan
            .snapshot
            .assignments
            .get(&new_queue)
            .expect("new queue assignment");
        assert_eq!(
            new_assignment.durability,
            ReplicationDurabilityPolicy::ReplicaDurable { nodes: 2 }
        );
    }

    #[test]
    fn placement_repairs_missing_owner_and_missing_followers() {
        let queue = QueueIdentity::new("emails", Partition::new(0), None);
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
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
                generation: 9,
            })
            .unwrap();

        let assignment = plan
            .snapshot
            .assignment_for("emails", Partition::new(0), None)
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
            QueueIdentity::new(topic, Partition::new(0), Some("workers")),
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
                        broker_addr: format!("127.0.0.1:{}", 10_000 + idx),
                        admin_addr: None,
                    },
                )
            })
            .collect()
    }

    fn assignment_owner(snapshot: &CoordinationSnapshot, topic: &str) -> String {
        snapshot
            .assignment_for(topic, Partition::new(0), None)
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

        assert!(coordination.owns_queue("anything", Partition::new(99), Some("group")));
        assert!(
            coordination
                .owner_for("anything", Partition::new(99), Some("group"))
                .is_none()
        );
    }
}
