//! Spike: ganglion raft-backed implementation of fibril's [`Coordination`] trait.
//!
//! A `RaftMetadataNode` replicates the coordination snapshot through real
//! consensus; this crate maps between fibril's and ganglion's snapshot models
//! and bridges ganglion's committed-snapshot watch into the
//! `watch::Receiver<CoordinationSnapshot>` fibril consumes. Reads are sync
//! (trait-compatible); proposals are async and leader-only, matching the
//! controller-loop model from `REPLICATION_PLANNING.md`.

use std::{collections::HashMap, time::Duration};

use fibril_broker::coordination::{
    aggregate_cohort_membership, ClusterCohortController, CohortPlan, ConsumerGroupKey,
    Coordination, CoordinationSnapshot, CoordinationStream, GlobalCohortMembership,
    LocalCohortMembership, NodeInfo, PartitionAssignment, PartitionPlacementError,
    PartitionPlacementInput, PartitionPlacementPolicy, QueueIdentity, ReplicationDurabilityPolicy,
};
use fibril_storage::Partition;
use ganglion_openraft::openraft::storage::RaftLogStorage;
use ganglion_openraft::openraft::RaftNetworkFactory;
use ganglion_openraft::{
    client_write_remote_with_hint, GanglionLogStore, GanglionRaftConfig, InProcessRouter,
    MetadataRaftCommand, MetadataRaftResponse, MetadataRejection, OpenraftAdapterError,
    RaftMetadataNode, RemoteWriteError, WireFormat,
};
use tokio::sync::watch;

/// Namespace tag used for fibril queues inside ganglion resource identities.
const QUEUE_NAMESPACE: &str = "fibril/queue";

/// Attribute key carrying the replicated runtime-settings document.
pub const RUNTIME_SETTINGS_ATTRIBUTE: &str = "fibril/runtime_settings";

/// Heartbeat node-label key carrying a broker's local exclusive-cohort
/// membership (JSON `Vec<LocalCohortMembership>`). The controller reads every
/// live node's label, aggregates global membership, and computes one plan per
/// cohort.
pub const COHORT_MEMBERSHIP_LABEL: &str = "fibril/cohort_membership";

/// Serialize a broker's local cohort membership for its heartbeat label.
pub fn encode_cohort_membership(memberships: &[LocalCohortMembership]) -> String {
    serde_json::to_string(memberships).unwrap_or_else(|_| "[]".to_string())
}

/// Parse a broker's cohort-membership label; malformed/absent -> empty (advisory
/// data must never break the controller).
pub fn decode_cohort_membership(raw: &str) -> Vec<LocalCohortMembership> {
    serde_json::from_str(raw).unwrap_or_default()
}

/// Aggregate global cohort membership from every node's membership label.
pub fn aggregate_membership_labels<'a>(
    labels: impl IntoIterator<Item = &'a str>,
) -> Vec<GlobalCohortMembership> {
    let reports = labels.into_iter().flat_map(decode_cohort_membership);
    aggregate_cohort_membership(reports)
}

/// Replicated cluster-attribute key carrying a cohort's computed assignment.
pub fn cohort_assignment_attribute_key(key: &ConsumerGroupKey) -> String {
    format!(
        "fibril/cohort_assignment/{}/{}/{}",
        key.topic,
        key.group.as_deref().unwrap_or(""),
        key.consumer_group,
    )
}

/// Wire form of a cohort's `partition -> member` assignment. Stored as an entry
/// SEQUENCE (not a map) so a `Partition` newtype is never used as a JSON map key
/// (that path has bitten us before — see the v2 WAL pair-sequence fix).
///
/// `generation` is the durable plan version. The controller bumps it only when
/// the assignment content changes, so owners can fence a stale slice and the
/// cluster has one number to watch for convergence. It lives in the document
/// (not an in-memory counter) so it survives a controller leader change.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CohortAssignmentDoc {
    #[serde(default)]
    pub generation: u64,
    pub entries: Vec<CohortAssignmentEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CohortAssignmentEntry {
    pub partition: Partition,
    pub member: String,
}

/// A plan's assignment as a stable entry sequence (sorted by partition), so the
/// same assignment always serializes identically and content comparisons are
/// order-independent.
fn cohort_assignment_entries(plan: &CohortPlan) -> Vec<CohortAssignmentEntry> {
    let mut entries: Vec<CohortAssignmentEntry> = plan
        .assignment
        .iter()
        .map(|(partition, member)| CohortAssignmentEntry {
            partition: *partition,
            member: member.clone(),
        })
        .collect();
    entries.sort_by_key(|entry| entry.partition.id());
    entries
}

/// Serialize a cohort assignment document for publishing.
pub fn encode_cohort_assignment_doc(doc: &CohortAssignmentDoc) -> String {
    serde_json::to_string(doc).unwrap_or_else(|_| {
        // entries are plain data; serialization cannot realistically fail.
        String::from("{\"generation\":0,\"entries\":[]}")
    })
}

/// Serialize a computed plan's assignment at `generation` for publishing.
pub fn encode_cohort_assignment(generation: u64, plan: &CohortPlan) -> String {
    encode_cohort_assignment_doc(&CohortAssignmentDoc {
        generation,
        entries: cohort_assignment_entries(plan),
    })
}

/// Parse a published cohort assignment document (generation + entries).
pub fn decode_cohort_assignment_doc(raw: &str) -> Option<CohortAssignmentDoc> {
    serde_json::from_str::<CohortAssignmentDoc>(raw).ok()
}

/// Parse a published cohort assignment into `partition -> member`.
pub fn decode_cohort_assignment(raw: &str) -> HashMap<Partition, String> {
    decode_cohort_assignment_doc(raw)
        .map(|doc| {
            doc.entries
                .into_iter()
                .map(|entry| (entry.partition, entry.member))
                .collect()
        })
        .unwrap_or_default()
}

/// Attribute-key prefix for an in-progress live-repartition transition.
pub const REPARTITION_TRANSITION_PREFIX: &str = "fibril/repartition/";

/// Heartbeat label key under which an owner reports the old partitions whose
/// pre-cutover backlog it has drained during a repartition.
pub const REPARTITION_DRAINED_LABEL: &str = "fibril/repartition_drained";

/// Replicated marker for an in-progress grow of a queue `(topic, group)`.
/// Present only while the transition runs (the controller clears it once every
/// old partition has drained). `version` scopes drain reports to this grow so a
/// stale report from a prior transition is ignored.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RepartitionTransitionDoc {
    pub version: u64,
    pub n_old: u32,
    pub n_new: u32,
}

/// The old partition `r` a new partition `p` sources from under integer-multiple
/// growth: `p % n_old`. A new partition may deliver once its source has drained.
pub fn repartition_source_partition(p: u32, n_old: u32) -> u32 {
    p % n_old.max(1)
}

/// Attribute key carrying the repartition transition marker for `(topic, group)`.
pub fn repartition_transition_attribute_key(topic: &str, group: Option<&str>) -> String {
    match group {
        Some(group) => format!("{REPARTITION_TRANSITION_PREFIX}{topic}/{group}"),
        None => format!("{REPARTITION_TRANSITION_PREFIX}{topic}"),
    }
}

pub fn encode_repartition_transition(doc: &RepartitionTransitionDoc) -> String {
    serde_json::to_string(doc).unwrap_or_else(|_| String::from("{}"))
}

pub fn decode_repartition_transition(raw: &str) -> Option<RepartitionTransitionDoc> {
    serde_json::from_str(raw).ok()
}

/// One owner's drain report: the old partitions of a queue whose pre-cutover
/// backlog it has fully drained, scoped to a transition `version`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RepartitionDrainedReport {
    pub topic: String,
    #[serde(default)]
    pub group: Option<String>,
    pub version: u64,
    pub drained: Vec<u32>,
}

pub fn encode_repartition_drained(reports: &[RepartitionDrainedReport]) -> String {
    serde_json::to_string(reports).unwrap_or_else(|_| String::from("[]"))
}

pub fn decode_repartition_drained(raw: &str) -> Vec<RepartitionDrainedReport> {
    serde_json::from_str(raw).unwrap_or_default()
}

/// Union the drained old partitions reported across all nodes' labels for one
/// queue `(topic, group)` at transition `version`. Reports for other queues or
/// stale versions contribute nothing.
pub fn aggregate_repartition_drained<'a>(
    labels: impl IntoIterator<Item = &'a str>,
    topic: &str,
    group: Option<&str>,
    version: u64,
) -> std::collections::HashSet<u32> {
    let mut drained = std::collections::HashSet::new();
    for raw in labels {
        for report in decode_repartition_drained(raw) {
            if report.topic == topic
                && report.group.as_deref() == group
                && report.version == version
            {
                drained.extend(report.drained);
            }
        }
    }
    drained
}

/// Replicated runtime-settings document: the cluster truth. `cluster_version`
/// is independent of each node's local store version (those differ per node);
/// CAS on the serialized document makes concurrent publishers race-safe.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ClusterRuntimeSettings {
    pub cluster_version: u64,
    pub settings: fibril_broker::runtime_settings::RuntimeSettings,
}

#[derive(Debug, Clone)]
pub enum ClusterRuntimeSettingsUpdateOutcome {
    Stored(ClusterRuntimeSettings),
    Conflict(Option<ClusterRuntimeSettings>),
}

/// Partitioning version carried by the client topology. Constant while
/// partition counts are fixed-at-create; a future live-repartitioning change
/// bumps it so clients re-fetch routing and owners can fence stale-version
/// traffic. Present from day one to keep the wire forward-compatible.
pub const DEFAULT_PARTITIONING_VERSION: u64 = 0;

/// Attribute-key prefix for per-queue partitioning metadata in the replicated
/// attribute store. A logical queue is identified by `(topic, group)` (group is
/// part of the identity, like a name prefix), so the key is
/// `fibril/partitioning/<topic>` (ungrouped) or
/// `fibril/partitioning/<topic>/<group>`. Topic and group share a validated
/// charset that excludes `/`, so the separator is unambiguous.
pub const QUEUE_PARTITIONING_ATTRIBUTE_PREFIX: &str = "fibril/partitioning/";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForwardedWritePolicy {
    /// Maximum deterministic remote leader redirects for one forwarded write.
    /// Redirects do not sleep; they follow explicit leader hints returned by
    /// contacted raft peers.
    pub redirect_limit: usize,
    /// Number of blind waits after a write sees no leader and no remote leader
    /// hint, the case of a freshly started or churning standby whose topology
    /// has not yet learned the leader. Each wait backs off, giving the local
    /// leader view time to converge before the write gives up. Tests that want
    /// to prove the deterministic leader-hint path can set this to zero.
    pub no_leader_retries: usize,
    pub no_leader_base_backoff: Duration,
    pub no_leader_max_backoff: Duration,
}

impl Default for ForwardedWritePolicy {
    fn default() -> Self {
        Self {
            redirect_limit: 16,
            no_leader_retries: 6,
            no_leader_base_backoff: Duration::from_millis(25),
            no_leader_max_backoff: Duration::from_millis(250),
        }
    }
}

fn queue_partitioning_key(topic: &str, group: Option<&str>) -> String {
    match group {
        Some(group) => format!("{QUEUE_PARTITIONING_ATTRIBUTE_PREFIX}{topic}/{group}"),
        None => format!("{QUEUE_PARTITIONING_ATTRIBUTE_PREFIX}{topic}"),
    }
}

/// Replicated partitioning of one logical queue `(topic, group)`.
/// `partition_count` is fixed at create for now; `partitioning_version` bumps on
/// a future live repartition so routing can be fenced. Stored as a CAS
/// attribute so concurrent declares are race-safe (create-once) and a
/// repartition is a compare-update.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QueuePartitioning {
    pub partition_count: u32,
    pub partitioning_version: u64,
}

/// Outcome of declaring a queue's partitioning.
#[derive(Debug)]
pub enum DeclareQueueError {
    /// The queue already exists with a different partition count. Changing the
    /// count is a repartition, not a re-declare.
    PartitionCountConflict {
        topic: String,
        group: Option<String>,
        existing: u32,
        requested: u32,
    },
    /// A coordination/consensus error (not-leader, storage, repeated CAS loss).
    Coordination(OpenraftAdapterError),
}

impl std::fmt::Display for DeclareQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PartitionCountConflict {
                topic,
                group,
                existing,
                requested,
            } => write!(
                f,
                "queue `{topic}`{} already declared with {existing} partitions (requested {requested}); use repartition to change it",
                group
                    .as_deref()
                    .map(|g| format!("/{g}"))
                    .unwrap_or_default()
            ),
            Self::Coordination(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for DeclareQueueError {}

/// Outcome of repartitioning (growing) a queue's partition count.
#[derive(Debug)]
pub enum RepartitionQueueError {
    /// The queue has not been declared, so there is nothing to repartition.
    NotDeclared {
        topic: String,
        group: Option<String>,
    },
    /// The requested count is not a strictly larger integer multiple of the
    /// current count. v1 grows by an integer multiple only (typically doubling),
    /// which keeps the per-key ordering barrier a pure partition-identity gate.
    NotIntegerMultipleGrowth {
        topic: String,
        group: Option<String>,
        current: u32,
        requested: u32,
    },
    /// A coordination/consensus error (not-leader, storage, repeated CAS loss).
    Coordination(OpenraftAdapterError),
}

impl std::fmt::Display for RepartitionQueueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let queue = |topic: &str, group: &Option<String>| {
            format!(
                "`{topic}`{}",
                group
                    .as_deref()
                    .map(|g| format!("/{g}"))
                    .unwrap_or_default()
            )
        };
        match self {
            Self::NotDeclared { topic, group } => {
                write!(f, "queue {} is not declared", queue(topic, group))
            }
            Self::NotIntegerMultipleGrowth {
                topic,
                group,
                current,
                requested,
            } => write!(
                f,
                "queue {} cannot repartition from {current} to {requested} partitions: \
                 the new count must be a larger integer multiple of the current one",
                queue(topic, group)
            ),
            Self::Coordination(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for RepartitionQueueError {}

/// Client-facing ownership of one queue partition: which node owns it and where
/// to reach that node, plus the partitioning version the routing was computed
/// under.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClientQueueTopology {
    pub topic: String,
    pub partition: fibril_storage::Partition,
    pub group: Option<String>,
    pub owner_node_id: String,
    /// Broker endpoint of the owner, if the node is known in the registry.
    pub owner_endpoint: Option<String>,
    pub partitioning_version: u64,
    /// Total partition count of this queue `(topic, group)` — authoritative N
    /// for key routing.
    pub partition_count: u32,
}

/// Client-facing topology snapshot: the owner/endpoint of every assigned queue
/// partition at a given coordination generation. Clients route to owners from
/// this and refresh it on not-owner / stale-topology errors.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClientTopology {
    pub generation: u64,
    pub queues: Vec<ClientQueueTopology>,
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
        u64::from(queue.partition.id()),
        queue.group.clone(),
    )
}

fn to_fibril_queue(resource: &ganglion_core::ResourceIdentity) -> Option<QueueIdentity> {
    if resource.namespace != QUEUE_NAMESPACE {
        return None;
    }
    let partition = Partition::new(u32::try_from(resource.partition).ok()?);
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
    forwarded_write_policy: ForwardedWritePolicy,
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
        Self::new_with_forwarded_write_policy(
            node_id,
            node,
            wire_format,
            ForwardedWritePolicy::default(),
        )
    }

    pub fn new_with_forwarded_write_policy(
        node_id: impl Into<String>,
        node: RaftMetadataNode<LS, NF>,
        wire_format: WireFormat,
        forwarded_write_policy: ForwardedWritePolicy,
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
            forwarded_write_policy,
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

    /// Aggregate exclusive-cohort membership across the cluster from every node's
    /// heartbeat membership label. The controller computes one plan per cohort
    /// from this. (Advisory: malformed/absent labels just contribute nothing.)
    pub fn global_cohort_membership(&self) -> Vec<GlobalCohortMembership> {
        let snapshot = self.node.committed_snapshot();
        let labels: Vec<&str> = snapshot
            .nodes
            .values()
            .filter_map(|node| node.labels.get(COHORT_MEMBERSHIP_LABEL).map(String::as_str))
            .collect();
        aggregate_membership_labels(labels)
    }

    /// Publish a cohort's computed assignment as a replicated cluster attribute
    /// for owner brokers to read and apply. Leader-or-forwarded.
    ///
    /// The durable plan generation bumps only when the assignment content changes
    /// against what is already published, so re-publishing a stable plan is a
    /// same-value write (a no-op that keeps the generation). Only the leader runs
    /// the controller, so this read-then-write needs no CAS. Reading the
    /// generation back from the committed document also keeps it monotonic across
    /// a leader change.
    pub async fn publish_cohort_assignment(
        &self,
        plan: &CohortPlan,
    ) -> Result<(), OpenraftAdapterError> {
        let current = self.cohort_assignment_doc(&plan.key);
        let entries = cohort_assignment_entries(plan);
        let generation = match &current {
            Some(doc) if doc.entries == entries => doc.generation,
            Some(doc) => doc.generation + 1,
            None => 0,
        };
        self.set_cluster_attribute(
            cohort_assignment_attribute_key(&plan.key),
            encode_cohort_assignment_doc(&CohortAssignmentDoc {
                generation,
                entries,
            }),
        )
        .await
    }

    /// Read a cohort's published `partition -> member` assignment (empty if none).
    pub fn cohort_assignment(&self, key: &ConsumerGroupKey) -> HashMap<Partition, String> {
        self.cluster_attribute(&cohort_assignment_attribute_key(key))
            .map(|raw| decode_cohort_assignment(&raw))
            .unwrap_or_default()
    }

    /// Read a cohort's published assignment document (generation + entries), if
    /// any. Owners use the generation to fence a stale slice and to report which
    /// plan version they have applied.
    pub fn cohort_assignment_doc(&self, key: &ConsumerGroupKey) -> Option<CohortAssignmentDoc> {
        self.cluster_attribute(&cohort_assignment_attribute_key(key))
            .and_then(|raw| decode_cohort_assignment_doc(&raw))
    }

    /// Publish the marker for an in-progress repartition transition (leader-or-
    /// forwarded). New-partition owners read it to learn `n_old` and hold their
    /// partitions until the source old partition drains.
    pub async fn begin_repartition_transition(
        &self,
        topic: &str,
        group: Option<&str>,
        doc: &RepartitionTransitionDoc,
    ) -> Result<(), OpenraftAdapterError> {
        self.set_cluster_attribute(
            repartition_transition_attribute_key(topic, group),
            encode_repartition_transition(doc),
        )
        .await
    }

    /// Read the repartition transition marker for `(topic, group)`, if a grow is
    /// in progress.
    pub fn repartition_transition(
        &self,
        topic: &str,
        group: Option<&str>,
    ) -> Option<RepartitionTransitionDoc> {
        self.cluster_attribute(&repartition_transition_attribute_key(topic, group))
            .and_then(|raw| decode_repartition_transition(&raw))
    }

    /// Clear the repartition transition marker once the grow is complete.
    pub async fn clear_repartition_transition(
        &self,
        topic: &str,
        group: Option<&str>,
    ) -> Result<(), OpenraftAdapterError> {
        // An empty value is the absent state (decode yields None).
        self.set_cluster_attribute(
            repartition_transition_attribute_key(topic, group),
            String::new(),
        )
        .await
    }

    /// The set of old partitions reported drained cluster-wide for a queue's
    /// repartition at `version`, aggregated from every node's heartbeat label.
    pub fn global_repartition_drained(
        &self,
        topic: &str,
        group: Option<&str>,
        version: u64,
    ) -> std::collections::HashSet<u32> {
        let snapshot = self.node.committed_snapshot();
        let labels: Vec<&str> = snapshot
            .nodes
            .values()
            .filter_map(|node| node.labels.get(REPARTITION_DRAINED_LABEL).map(String::as_str))
            .collect();
        aggregate_repartition_drained(labels, topic, group, version)
    }

    /// Every queue with an in-progress repartition transition, read from the
    /// committed attributes. Owners iterate these to hold/drain/lift their
    /// partitions. Cleared markers (empty value) decode to None and are skipped.
    pub fn active_repartition_transitions(
        &self,
    ) -> Vec<(String, Option<String>, RepartitionTransitionDoc)> {
        let snapshot = self.node.committed_snapshot();
        snapshot
            .attributes
            .iter()
            .filter_map(|(key, raw)| {
                let rest = key.strip_prefix(REPARTITION_TRANSITION_PREFIX)?;
                let doc = decode_repartition_transition(raw)?;
                let (topic, group) = match rest.split_once('/') {
                    Some((topic, group)) => (topic.to_string(), Some(group.to_string())),
                    None => (rest.to_string(), None),
                };
                Some((topic, group, doc))
            })
            .collect()
    }

    /// One cohort-controller iteration: as the leader, aggregate cluster-wide
    /// cohort membership, compute each cohort's global plan (`controller` holds
    /// per-cohort state across ticks for stickiness), and publish the plans for
    /// owners to apply. Standbys (non-leaders) no-op. `partition_count` resolves a
    /// queue's full partition count (e.g. from `queue_partitioning`).
    ///
    /// Before planning, a controller with no state for a cohort seeds itself from
    /// that cohort's published plan. A freshly elected leader therefore keeps the
    /// existing assignment instead of recomputing a different balanced one, so a
    /// leader change does not churn the cluster (the seed is a no-op once the
    /// controller already tracks the cohort).
    pub async fn run_cohort_controller_tick(
        &self,
        controller: &mut ClusterCohortController,
        partition_count: impl Fn(&ConsumerGroupKey) -> u32,
    ) -> Result<(), OpenraftAdapterError> {
        if !self.node.is_leader().await {
            return Ok(());
        }
        let membership = self.global_cohort_membership();
        for cohort in &membership {
            let published = self.cohort_assignment(&cohort.key);
            if !published.is_empty() {
                controller.seed_published(cohort.key.clone(), published);
            }
        }
        for plan in controller.plan(membership, partition_count) {
            self.publish_cohort_assignment(&plan).await?;
        }
        Ok(())
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
        let mut next_remote_addr: Option<String> = None;
        let mut redirects = 0usize;
        let mut no_leader_waits = 0usize;

        loop {
            if let Some(addr) = next_remote_addr.take() {
                match client_write_remote_with_hint(&addr, command.clone(), self.wire_format).await
                {
                    Ok(response) => return Self::map_forwarded_response(response),
                    Err(RemoteWriteError::NotLeader {
                        leader_addr: Some(addr),
                    }) => {
                        redirects += 1;
                        if redirects > self.forwarded_write_policy.redirect_limit {
                            return Err(OpenraftAdapterError::NotLeader);
                        }
                        next_remote_addr = Some(addr);
                    }
                    Err(RemoteWriteError::NotLeader { leader_addr: None }) => {
                        if let Some(addr) = self.leader_addr_from_topology() {
                            next_remote_addr = Some(addr);
                        } else if !self.wait_for_no_leader_hint(&mut no_leader_waits).await {
                            return Err(OpenraftAdapterError::NotLeader);
                        }
                    }
                    Err(RemoteWriteError::Other(message)) => {
                        return Err(OpenraftAdapterError::Storage(message));
                    }
                }
                continue;
            }

            if self.node.is_leader().await {
                match self.node.submit_merge(command.clone()).await {
                    Ok(response) => return Self::map_forwarded_response(response),
                    Err(OpenraftAdapterError::NotLeader) => {
                        if let Some(addr) = self.leader_addr_from_topology() {
                            next_remote_addr = Some(addr);
                        } else if !self.wait_for_no_leader_hint(&mut no_leader_waits).await {
                            return Err(OpenraftAdapterError::NotLeader);
                        }
                    }
                    Err(error) => return Err(error),
                }
                continue;
            }

            match self.leader_addr_from_topology() {
                Some(addr) => next_remote_addr = Some(addr),
                None => {
                    if !self.wait_for_no_leader_hint(&mut no_leader_waits).await {
                        return Err(OpenraftAdapterError::NotLeader);
                    }
                }
            }
        }
    }

    async fn wait_for_no_leader_hint(&self, waits: &mut usize) -> bool {
        if *waits >= self.forwarded_write_policy.no_leader_retries {
            return false;
        }
        *waits += 1;
        let delay = self
            .forwarded_write_policy
            .no_leader_base_backoff
            .saturating_mul(*waits as u32)
            .min(self.forwarded_write_policy.no_leader_max_backoff);
        tokio::time::sleep(delay).await;
        true
    }

    fn leader_addr_from_topology(&self) -> Option<String> {
        let topology = self.node.topology();
        topology
            .leader
            .and_then(|leader| topology.nodes.get(&leader).cloned())
    }

    fn map_forwarded_response(
        response: MetadataRaftResponse,
    ) -> Result<MetadataRaftResponse, OpenraftAdapterError> {
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

    /// Committed partitioning for a logical queue `(topic, group)`, if declared.
    pub fn queue_partitioning(
        &self,
        topic: &str,
        group: Option<&str>,
    ) -> Option<QueuePartitioning> {
        self.cluster_attribute(&queue_partitioning_key(topic, group))
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok())
    }

    /// Declare a logical queue's partitioning, race-safe via create-once CAS.
    /// The queue is `(topic, group)` — group is part of its identity, so each
    /// group is partitioned independently.
    ///
    /// - absent -> created with `partition_count` at version 0.
    /// - already declared with the SAME count -> idempotent success.
    /// - already declared with a DIFFERENT count -> `PartitionCountConflict`
    ///   (changing the count is a repartition, a separate operation).
    ///
    /// Concurrent declares serialize through the CAS: the loser re-reads and
    /// either succeeds idempotently or surfaces the conflict.
    pub async fn declare_queue_partitioning(
        &self,
        topic: &str,
        group: Option<&str>,
        partition_count: u32,
    ) -> Result<QueuePartitioning, DeclareQueueError> {
        let partition_count = partition_count.max(1);
        let key = queue_partitioning_key(topic, group);
        for _ in 0..8 {
            let current_raw = self.cluster_attribute(&key);
            if let Some(raw) = &current_raw {
                match serde_json::from_str::<QueuePartitioning>(raw) {
                    Ok(existing) if existing.partition_count == partition_count => {
                        return Ok(existing);
                    }
                    Ok(existing) => {
                        return Err(DeclareQueueError::PartitionCountConflict {
                            topic: topic.to_string(),
                            group: group.map(str::to_string),
                            existing: existing.partition_count,
                            requested: partition_count,
                        });
                    }
                    Err(error) => {
                        return Err(DeclareQueueError::Coordination(
                            OpenraftAdapterError::Storage(format!(
                                "queue `{topic}`/{group:?} partitioning is corrupt: {error}"
                            )),
                        ));
                    }
                }
            }
            let partitioning = QueuePartitioning {
                partition_count,
                partitioning_version: DEFAULT_PARTITIONING_VERSION,
            };
            let value = serde_json::to_string(&partitioning).map_err(|error| {
                DeclareQueueError::Coordination(OpenraftAdapterError::Storage(error.to_string()))
            })?;
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttribute {
                    key: key.clone(),
                    expected: None,
                    value,
                })
                .await
            {
                Ok(_) => return Ok(partitioning),
                // Lost the create race: re-read and resolve (idempotent or conflict).
                Err(OpenraftAdapterError::AttributeMismatch { .. }) => continue,
                Err(error) => return Err(DeclareQueueError::Coordination(error)),
            }
        }
        Err(DeclareQueueError::Coordination(
            OpenraftAdapterError::Storage("queue declare lost the CAS race repeatedly".to_string()),
        ))
    }

    /// Grow a logical queue's partition count to `new_count` and bump its
    /// partitioning version. Race-safe via CAS on the current document.
    ///
    /// v1 grows by an INTEGER MULTIPLE only (`new_count` must be a strictly larger
    /// multiple of the current count, typically doubling). With modulo hashing
    /// that keeps the per-key ordering barrier a pure partition-identity gate
    /// (each new partition sources from exactly one old partition), see
    /// DESIGN_NOTES.md.
    ///
    /// - undeclared queue -> `NotDeclared`.
    /// - already at `new_count` -> idempotent success.
    /// - `new_count` not a larger integer multiple -> `NotIntegerMultipleGrowth`.
    pub async fn repartition_queue(
        &self,
        topic: &str,
        group: Option<&str>,
        new_count: u32,
    ) -> Result<QueuePartitioning, RepartitionQueueError> {
        let key = queue_partitioning_key(topic, group);
        for _ in 0..8 {
            let Some(current_raw) = self.cluster_attribute(&key) else {
                return Err(RepartitionQueueError::NotDeclared {
                    topic: topic.to_string(),
                    group: group.map(str::to_string),
                });
            };
            let existing: QueuePartitioning =
                serde_json::from_str(&current_raw).map_err(|error| {
                    RepartitionQueueError::Coordination(OpenraftAdapterError::Storage(format!(
                        "queue `{topic}`/{group:?} partitioning is corrupt: {error}"
                    )))
                })?;
            // Idempotent: already grown to the requested count.
            if existing.partition_count == new_count {
                return Ok(existing);
            }
            // v1 grows by an integer multiple only.
            if new_count <= existing.partition_count
                || new_count % existing.partition_count != 0
            {
                return Err(RepartitionQueueError::NotIntegerMultipleGrowth {
                    topic: topic.to_string(),
                    group: group.map(str::to_string),
                    current: existing.partition_count,
                    requested: new_count,
                });
            }
            let next = QueuePartitioning {
                partition_count: new_count,
                partitioning_version: existing.partitioning_version + 1,
            };
            let value = serde_json::to_string(&next).map_err(|error| {
                RepartitionQueueError::Coordination(OpenraftAdapterError::Storage(
                    error.to_string(),
                ))
            })?;
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttribute {
                    key: key.clone(),
                    expected: Some(current_raw.clone()),
                    value,
                })
                .await
            {
                Ok(_) => return Ok(next),
                // Lost the race against a concurrent writer: re-read and resolve.
                Err(OpenraftAdapterError::AttributeMismatch { .. }) => continue,
                Err(error) => return Err(RepartitionQueueError::Coordination(error)),
            }
        }
        Err(RepartitionQueueError::Coordination(
            OpenraftAdapterError::Storage(
                "queue repartition lost the CAS race repeatedly".to_string(),
            ),
        ))
    }

    /// Operator entry point for a live grow: publish the transition marker and
    /// register the new partitions BEFORE bumping the partitioning version, so a
    /// new partition is held (and placed) before producers cut over to it. Then
    /// bump the version (the bump is what redirects producers to the new count).
    ///
    /// Order matters for correctness: marker first means any new partition that
    /// materializes starts held, never delivering before its source old partition
    /// has drained.
    pub async fn grow_queue(
        &self,
        topic: &str,
        group: Option<&str>,
        new_count: u32,
    ) -> Result<QueuePartitioning, RepartitionQueueError> {
        let current = self.queue_partitioning(topic, group).ok_or_else(|| {
            RepartitionQueueError::NotDeclared {
                topic: topic.to_string(),
                group: group.map(str::to_string),
            }
        })?;
        let n_old = current.partition_count;
        // Idempotent: already at the target count.
        if n_old == new_count {
            return Ok(current);
        }
        if new_count <= n_old || new_count % n_old.max(1) != 0 {
            return Err(RepartitionQueueError::NotIntegerMultipleGrowth {
                topic: topic.to_string(),
                group: group.map(str::to_string),
                current: n_old,
                requested: new_count,
            });
        }
        let next_version = current.partitioning_version + 1;
        // 1. Marker first, so new partitions are held the moment they appear.
        self.begin_repartition_transition(
            topic,
            group,
            &RepartitionTransitionDoc {
                version: next_version,
                n_old,
                n_new: new_count,
            },
        )
        .await
        .map_err(RepartitionQueueError::Coordination)?;
        // 2. Register the new partitions so the controller places owners.
        for partition in n_old..new_count {
            let queue = QueueIdentity::new(topic, fibril_storage::Partition::new(partition), group);
            self.register_queue(&queue)
                .await
                .map_err(RepartitionQueueError::Coordination)?;
        }
        // 3. Bump the version last: this is the routing cutover.
        self.repartition_queue(topic, group, new_count).await
    }

    pub fn runtime_settings_document(
        &self,
    ) -> Result<Option<ClusterRuntimeSettings>, OpenraftAdapterError> {
        self.cluster_attribute(RUNTIME_SETTINGS_ATTRIBUTE)
            .as_deref()
            .map(|raw| {
                serde_json::from_str::<ClusterRuntimeSettings>(raw).map_err(|error| {
                    OpenraftAdapterError::Storage(format!(
                        "decode runtime settings cluster document: {error}"
                    ))
                })
            })
            .transpose()
    }

    /// Update runtime settings as the cluster truth. `expected_version` is the
    /// cluster document version returned by the admin API, where 0 means no
    /// cluster document exists yet.
    pub async fn update_runtime_settings(
        &self,
        expected_version: u64,
        settings: &fibril_broker::runtime_settings::RuntimeSettings,
    ) -> Result<ClusterRuntimeSettingsUpdateOutcome, OpenraftAdapterError> {
        settings
            .validate()
            .map_err(|error| OpenraftAdapterError::Config(error.to_string()))?;

        for _ in 0..8 {
            let current_raw = self.cluster_attribute(RUNTIME_SETTINGS_ATTRIBUTE);
            let current_document = current_raw
                .as_deref()
                .map(|raw| {
                    serde_json::from_str::<ClusterRuntimeSettings>(raw).map_err(|error| {
                        OpenraftAdapterError::Storage(format!(
                            "decode runtime settings cluster document: {error}"
                        ))
                    })
                })
                .transpose()?;
            let current_version = current_document
                .as_ref()
                .map(|document| document.cluster_version)
                .unwrap_or(0);

            if current_version != expected_version {
                return Ok(ClusterRuntimeSettingsUpdateOutcome::Conflict(
                    current_document,
                ));
            }

            let document = ClusterRuntimeSettings {
                cluster_version: expected_version + 1,
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
                Ok(_) => return Ok(ClusterRuntimeSettingsUpdateOutcome::Stored(document)),
                Err(OpenraftAdapterError::AttributeMismatch { .. }) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(OpenraftAdapterError::Storage(
            "runtime settings update lost the CAS race repeatedly".to_string(),
        ))
    }

    /// Publish runtime settings as the cluster truth using last-committer-wins
    /// semantics. Prefer `update_runtime_settings` for user/admin writes.
    pub async fn publish_runtime_settings(
        &self,
        settings: &fibril_broker::runtime_settings::RuntimeSettings,
    ) -> Result<u64, OpenraftAdapterError> {
        settings
            .validate()
            .map_err(|error| OpenraftAdapterError::Config(error.to_string()))?;

        for _ in 0..8 {
            let current_raw = self.cluster_attribute(RUNTIME_SETTINGS_ATTRIBUTE);
            let cluster_version = current_raw
                .as_deref()
                .map(|raw| {
                    serde_json::from_str::<ClusterRuntimeSettings>(raw).map_err(|error| {
                        OpenraftAdapterError::Storage(format!(
                            "decode runtime settings cluster document: {error}"
                        ))
                    })
                })
                .transpose()?
                .map(|document| document.cluster_version)
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
            let mut last_bad_raw: Option<String> = None;
            loop {
                let raw_document = watch
                    .borrow_and_update()
                    .attributes
                    .get(RUNTIME_SETTINGS_ATTRIBUTE)
                    .cloned();
                let document = match raw_document.as_deref() {
                    Some(raw) => match serde_json::from_str::<ClusterRuntimeSettings>(raw) {
                        Ok(document) => {
                            last_bad_raw = None;
                            Some(document)
                        }
                        Err(error) => {
                            if last_bad_raw.as_deref() != Some(raw) {
                                tracing::warn!(
                                    %error,
                                    "replicated runtime settings document is invalid"
                                );
                                last_bad_raw = Some(raw.to_string());
                            }
                            None
                        }
                    },
                    None => None,
                };

                if let Some(document) = document {
                    if document.cluster_version > last_applied_cluster_version {
                        let current = manager.current();
                        if current.settings == document.settings {
                            // Already effective (e.g. this node published it).
                            last_applied_cluster_version = document.cluster_version;
                        } else {
                            match manager
                                .apply_cluster_settings(document.settings.clone())
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

    /// Client-facing topology: the owner and reachable endpoint of every
    /// assigned queue partition in the committed snapshot. Clients route to
    /// owners from this and refresh on not-owner / stale-topology errors. The
    /// partitioning version is carried per queue for forward-compatibility with
    /// live repartitioning (constant today).
    pub fn client_topology(&self) -> ClientTopology {
        let committed = self.node.committed_snapshot();
        let endpoints: HashMap<String, String> = committed
            .nodes
            .values()
            .map(|node| (node.node_id.clone(), node.endpoint.clone()))
            .collect();
        let generation = committed.generation;
        // Authoritative partitioning (count + version) per (topic, group), read
        // from the committed attributes we already hold.
        let partitioning = |topic: &str, group: Option<&str>| -> QueuePartitioning {
            committed
                .attributes
                .get(&queue_partitioning_key(topic, group))
                .and_then(|raw| serde_json::from_str::<QueuePartitioning>(raw).ok())
                .unwrap_or(QueuePartitioning {
                    partition_count: 1,
                    partitioning_version: DEFAULT_PARTITIONING_VERSION,
                })
        };
        let fibril = to_fibril_snapshot(&committed);
        let mut queues: Vec<ClientQueueTopology> = fibril
            .assignments
            .values()
            .map(|assignment| {
                let group = assignment.queue.group.as_ref().map(ToString::to_string);
                let part = partitioning(&assignment.queue.topic, group.as_deref());
                ClientQueueTopology {
                    topic: assignment.queue.topic.to_string(),
                    partition: assignment.queue.partition,
                    group,
                    owner_node_id: assignment.owner.clone(),
                    owner_endpoint: endpoints.get(&assignment.owner).cloned(),
                    partitioning_version: part.partitioning_version,
                    partition_count: part.partition_count,
                }
            })
            .collect();
        queues.sort_by(|a, b| {
            a.topic
                .cmp(&b.topic)
                .then_with(|| a.group.cmp(&b.group))
                .then_with(|| a.partition.cmp(&b.partition))
        });
        ClientTopology { generation, queues }
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
        partition: fibril_storage::Partition,
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
mod cohort_transport_tests {
    use super::*;
    use fibril_broker::coordination::CohortMemberInfo;

    fn membership(
        consumer_group: &str,
        members: &[(&str, Option<usize>)],
    ) -> LocalCohortMembership {
        LocalCohortMembership {
            topic: "jobs".into(),
            group: None,
            consumer_group: consumer_group.into(),
            members: members
                .iter()
                .map(|(m, t)| CohortMemberInfo {
                    member: m.to_string(),
                    target: *t,
                })
                .collect(),
        }
    }

    #[test]
    fn membership_label_roundtrips() {
        let reports = vec![membership("default", &[("m1", Some(2)), ("m2", None)])];
        let encoded = encode_cohort_membership(&reports);
        assert_eq!(decode_cohort_membership(&encoded), reports);
        // Malformed / empty label decodes to nothing, never panics.
        assert!(decode_cohort_membership("not json").is_empty());
        assert!(decode_cohort_membership("").is_empty());
    }

    #[test]
    fn aggregate_labels_unions_across_nodes() {
        // Two nodes' labels for the same cohort: m1 spans both, m2 on one.
        let node_a = encode_cohort_membership(&[membership("default", &[("m1", Some(2))])]);
        let node_b =
            encode_cohort_membership(&[membership("default", &[("m1", None), ("m2", None)])]);
        let global = aggregate_membership_labels([node_a.as_str(), node_b.as_str()]);
        assert_eq!(global.len(), 1);
        let members: Vec<&str> = global[0]
            .members
            .iter()
            .map(|m| m.member.as_str())
            .collect();
        assert_eq!(members, vec!["m1", "m2"]);
    }

    #[test]
    fn assignment_roundtrips_without_partition_map_keys() {
        let plan = CohortPlan {
            key: ConsumerGroupKey::new("jobs", None, "default"),
            assignment: HashMap::from([
                (Partition::new(0), "m1".to_string()),
                (Partition::new(2), "m2".to_string()),
            ]),
        };
        let encoded = encode_cohort_assignment(7, &plan);
        // Entry sequence, not a map keyed by Partition.
        assert!(encoded.contains("\"entries\""));
        assert_eq!(decode_cohort_assignment(&encoded), plan.assignment);
        assert_eq!(decode_cohort_assignment_doc(&encoded).unwrap().generation, 7);
        assert!(decode_cohort_assignment("garbage").is_empty());
        assert!(decode_cohort_assignment_doc("garbage").is_none());
    }

    #[test]
    fn assignment_doc_defaults_generation_for_old_documents() {
        // A document written before the generation field decodes as generation 0.
        let legacy = r#"{"entries":[{"partition":0,"member":"m1"}]}"#;
        let doc = decode_cohort_assignment_doc(legacy).expect("legacy doc decodes");
        assert_eq!(doc.generation, 0);
        assert_eq!(doc.entries.len(), 1);
    }

    #[test]
    fn repartition_transition_roundtrips_and_rejects_garbage() {
        let doc = RepartitionTransitionDoc {
            version: 3,
            n_old: 4,
            n_new: 8,
        };
        let encoded = encode_repartition_transition(&doc);
        assert_eq!(decode_repartition_transition(&encoded), Some(doc));
        assert_eq!(decode_repartition_transition(""), None);
        assert_eq!(decode_repartition_transition("garbage"), None);
    }

    #[test]
    fn repartition_source_maps_each_new_partition_to_one_old() {
        // Doubling 4 -> 8: new partition p sources from p % 4.
        assert_eq!(repartition_source_partition(5, 4), 1);
        assert_eq!(repartition_source_partition(7, 4), 3);
        assert_eq!(repartition_source_partition(4, 4), 0);
        // Staying partitions map to themselves.
        assert_eq!(repartition_source_partition(1, 4), 1);
    }

    #[test]
    fn aggregate_repartition_drained_unions_and_filters() {
        // Two owners each report a drained old partition for the same grow.
        let a = encode_repartition_drained(&[RepartitionDrainedReport {
            topic: "orders".into(),
            group: None,
            version: 1,
            drained: vec![0],
        }]);
        let b = encode_repartition_drained(&[RepartitionDrainedReport {
            topic: "orders".into(),
            group: None,
            version: 1,
            drained: vec![2],
        }]);
        // A stale-version report and a different-queue report contribute nothing.
        let stale = encode_repartition_drained(&[RepartitionDrainedReport {
            topic: "orders".into(),
            group: None,
            version: 0,
            drained: vec![3],
        }]);
        let other = encode_repartition_drained(&[RepartitionDrainedReport {
            topic: "shipments".into(),
            group: None,
            version: 1,
            drained: vec![1],
        }]);

        let drained = aggregate_repartition_drained(
            [a.as_str(), b.as_str(), stale.as_str(), other.as_str()],
            "orders",
            None,
            1,
        );
        assert_eq!(drained, std::collections::HashSet::from([0, 2]));
    }

    #[test]
    fn assignment_attribute_key_is_distinct_per_cohort() {
        let a = cohort_assignment_attribute_key(&ConsumerGroupKey::new("jobs", None, "g1"));
        let b = cohort_assignment_attribute_key(&ConsumerGroupKey::new("jobs", None, "g2"));
        let c =
            cohort_assignment_attribute_key(&ConsumerGroupKey::new("jobs", Some("workers"), "g1"));
        assert_ne!(a, b);
        assert_ne!(a, c);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::time::Duration;

    use ganglion_openraft::{default_raft_config, InProcessRouter};

    fn unique_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "fibril-ganglion-{tag}-{}-{nanos}",
            std::process::id()
        ))
    }

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

        let queue = QueueIdentity::new("orders", Partition::new(0), Some("workers"));
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
        assert!(coordination.owns_queue("orders", Partition::new(0), Some("workers")));
        assert!(!coordination.follows_queue("orders", Partition::new(0), Some("workers")));
        let owner = coordination
            .owner_for("orders", Partition::new(0), Some("workers"))
            .expect("owner resolves");
        assert_eq!(owner.node_id, "broker-a");
        assert_eq!(
            coordination
                .assignment_for("orders", Partition::new(0), Some("workers"))
                .expect("assignment resolves")
                .epoch,
            1
        );

        // Client-facing topology resolves the owner and its reachable endpoint.
        let topology = coordination.client_topology();
        assert_eq!(topology.generation, 1);
        assert_eq!(topology.queues.len(), 1);
        let queue = &topology.queues[0];
        assert_eq!(queue.topic, "orders");
        assert_eq!(queue.partition, Partition::new(0));
        assert_eq!(queue.group.as_deref(), Some("workers"));
        assert_eq!(queue.owner_node_id, "broker-a");
        assert_eq!(queue.owner_endpoint.as_deref(), Some("127.0.0.1:9000"));
        assert_eq!(queue.partitioning_version, DEFAULT_PARTITIONING_VERSION);

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
        let queue = QueueIdentity::new("orders", Partition::new(0), Some("workers"));
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
            .assignment_for("orders", Partition::new(0), Some("workers"))
            .expect("queue assigned")
            .clone();
        assert_eq!(first.epoch, 1, "fresh assignment starts at epoch 1");
        assert_eq!(first.replica_set_size(), 2);

        // A follower provider observes the committed assignment via watch.
        let mut stream = standby.watch();
        tokio::time::timeout(timeout, async {
            while stream
                .borrow_and_update()
                .assignment_for("orders", Partition::new(0), Some("workers"))
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
            .assignment_for("orders", Partition::new(0), Some("workers"))
            .expect("queue still assigned")
            .clone();
        assert_ne!(second.owner, first.owner, "ownership must move");
        assert_eq!(second.epoch, first.epoch + 1, "owner change must fence");

        // The standby's watch converges on the failover assignment.
        tokio::time::timeout(timeout, async {
            loop {
                let epoch = stream
                    .borrow_and_update()
                    .assignment_for("orders", Partition::new(0), Some("workers"))
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

    /// Queue declare is create-once + idempotent per `(topic, group)`: first
    /// declare creates the partitioning, re-declaring the same count is a no-op
    /// success, a different count conflicts, and groups partition independently
    /// (group is part of the queue identity). Reads return None for undeclared
    /// queues.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn declare_queue_partitioning_is_create_once_and_per_group() {
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

        assert_eq!(provider.queue_partitioning("orders", None), None);

        let created = provider
            .declare_queue_partitioning("orders", None, 4)
            .await
            .expect("declare");
        assert_eq!(created.partition_count, 4);
        assert_eq!(created.partitioning_version, DEFAULT_PARTITIONING_VERSION);
        assert_eq!(
            provider.queue_partitioning("orders", None),
            Some(created.clone())
        );

        // Idempotent re-declare with the same count.
        let again = provider
            .declare_queue_partitioning("orders", None, 4)
            .await
            .expect("idempotent re-declare");
        assert_eq!(again, created);

        // A different count is a conflict, not a silent change.
        let conflict = provider
            .declare_queue_partitioning("orders", None, 8)
            .await
            .expect_err("different partition count must conflict");
        assert!(matches!(
            conflict,
            DeclareQueueError::PartitionCountConflict {
                existing: 4,
                requested: 8,
                ..
            }
        ));
        assert_eq!(
            provider
                .queue_partitioning("orders", None)
                .map(|m| m.partition_count),
            Some(4)
        );

        // A group on the same topic partitions independently (part of identity).
        let grouped = provider
            .declare_queue_partitioning("orders", Some("workers"), 2)
            .await
            .expect("declare grouped queue");
        assert_eq!(grouped.partition_count, 2);
        assert_eq!(
            provider
                .queue_partitioning("orders", None)
                .map(|m| m.partition_count),
            Some(4),
            "the ungrouped queue is unaffected by the grouped declare"
        );

        provider.raft_node().shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repartition_grows_by_integer_multiple_and_bumps_version() {
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

        // Repartitioning an undeclared queue is rejected.
        let undeclared = provider
            .repartition_queue("orders", None, 4)
            .await
            .expect_err("undeclared queue cannot repartition");
        assert!(matches!(
            undeclared,
            RepartitionQueueError::NotDeclared { .. }
        ));

        let created = provider
            .declare_queue_partitioning("orders", None, 2)
            .await
            .expect("declare");
        assert_eq!(created.partition_count, 2);

        // Grow 2 -> 4 (a multiple): count grows, version bumps.
        let grown = provider
            .repartition_queue("orders", None, 4)
            .await
            .expect("grow to a multiple");
        assert_eq!(grown.partition_count, 4);
        assert_eq!(
            grown.partitioning_version,
            created.partitioning_version + 1
        );

        // Idempotent: repartition to the current count is a no-op success.
        let idempotent = provider
            .repartition_queue("orders", None, 4)
            .await
            .expect("idempotent repartition");
        assert_eq!(idempotent, grown);

        // Not a multiple of the current count -> rejected, state unchanged.
        let not_multiple = provider
            .repartition_queue("orders", None, 6)
            .await
            .expect_err("6 is not a multiple of 4");
        assert!(matches!(
            not_multiple,
            RepartitionQueueError::NotIntegerMultipleGrowth {
                current: 4,
                requested: 6,
                ..
            }
        ));

        // Shrinking is rejected in v1.
        let shrink = provider
            .repartition_queue("orders", None, 2)
            .await
            .expect_err("shrink rejected");
        assert!(matches!(
            shrink,
            RepartitionQueueError::NotIntegerMultipleGrowth { .. }
        ));

        // Chained grow 4 -> 8 bumps the version again.
        let grown_again = provider
            .repartition_queue("orders", None, 8)
            .await
            .expect("grow to 8");
        assert_eq!(grown_again.partition_count, 8);
        assert_eq!(
            grown_again.partitioning_version,
            grown.partitioning_version + 1
        );
        assert_eq!(
            provider.queue_partitioning("orders", None),
            Some(grown_again)
        );

        provider.raft_node().shutdown().await.expect("shutdown");
    }

    /// A logical queue can be declared through any broker in the raft group,
    /// not only the current metadata leader. This protects the real operator
    /// flow used by `scripts/cluster-tryout.sh --ganglion`: the CLI may connect
    /// to node 1 while node 2 or 3 currently leads coordination.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn declare_queue_partitioning_forwards_from_tcp_standby() {
        let config = default_raft_config().expect("config");
        let timeout = Duration::from_secs(10);
        let mut raft_nodes = Vec::new();
        let mut servers = Vec::new();
        let mut dirs = Vec::new();

        for id in 1..=3u64 {
            let dir = unique_dir(&format!("declare-forward-{id}"));
            let (node, server) =
                RaftMetadataNode::start_durable_tcp(id, config.clone(), "127.0.0.1:0", &dir)
                    .await
                    .expect("start tcp raft node");
            raft_nodes.push(node);
            servers.push(server);
            dirs.push(dir);
        }

        let members: BTreeMap<u64, _> = servers
            .iter()
            .enumerate()
            .map(|(index, server)| {
                (
                    index as u64 + 1,
                    ganglion_openraft::openraft::BasicNode::new(server.local_addr().to_string()),
                )
            })
            .collect();
        raft_nodes[0].initialize(members).await.expect("initialize");
        let leader_raft_id = raft_nodes[0]
            .wait_for_any_leader(timeout)
            .await
            .expect("election");

        // Run the forwarded write with ZERO blind no-leader retries on purpose.
        // This proves the deterministic path carries the write (forward to the
        // known leader, follow explicit leader hints on redirect) rather than
        // leaning on retry-spam to paper over a stale leader view.
        let zero_retry = ForwardedWritePolicy {
            no_leader_retries: 0,
            ..ForwardedWritePolicy::default()
        };
        let providers: Vec<GanglionCoordination<_, _>> = raft_nodes
            .into_iter()
            .enumerate()
            .map(|(index, node)| {
                GanglionCoordination::new_with_forwarded_write_policy(
                    format!("broker-{}", index + 1),
                    node,
                    WireFormat::default(),
                    zero_retry,
                )
            })
            .collect();
        let standby = providers
            .iter()
            .find(|provider| provider.raft_node().node_id() != leader_raft_id)
            .expect("standby provider");

        // The standby must observe the elected leader before it can forward
        // deterministically, since with zero blind retries there is no fallback
        // wait while its leader view converges.
        standby
            .raft_node()
            .wait_for_any_leader(timeout)
            .await
            .expect("standby observes the leader");

        let declared = standby
            .declare_queue_partitioning("orders", None, 3)
            .await
            .expect("standby declare should forward to leader");
        assert_eq!(declared.partition_count, 3);

        for provider in &providers {
            let mut watch = provider.watch();
            tokio::time::timeout(timeout, async {
                loop {
                    if provider.queue_partitioning("orders", None) == Some(declared.clone()) {
                        break;
                    }
                    watch.changed().await.expect("watch open");
                }
            })
            .await
            .expect("partitioning should replicate to every provider");
        }

        // The forwarded path still preserves create-once semantics.
        let conflict = standby
            .declare_queue_partitioning("orders", None, 4)
            .await
            .expect_err("different partition count must conflict");
        assert!(matches!(
            conflict,
            DeclareQueueError::PartitionCountConflict {
                existing: 3,
                requested: 4,
                ..
            }
        ));

        for provider in &providers {
            provider.raft_node().shutdown().await.expect("shutdown");
        }
        for server in &servers {
            server.shutdown();
        }
        for dir in dirs {
            let _ = std::fs::remove_dir_all(dir);
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

        let queue = QueueIdentity::new("orders", Partition::new(0), Some("workers"));
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
            .assignment_for("orders", Partition::new(0), Some("workers"))
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
        let queue = QueueIdentity::new("orders", Partition::new(0), None);
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
                .assignment_for("orders", Partition::new(0), None)
                .is_none()
            {
                stream.changed().await.expect("stream open");
            }
        })
        .await
        .expect("controller assigns the catalogue queue");
        let first = provider
            .snapshot()
            .assignment_for("orders", Partition::new(0), None)
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
                if let Some(assignment) =
                    provider
                        .snapshot()
                        .assignment_for("orders", Partition::new(0), None)
                {
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

        // Broker A commits through the cluster document first.
        let mut settings = manager_a.current().settings.clone();
        settings.delivery.inflight_ttl_ms = settings.delivery.inflight_ttl_ms.saturating_add(7);
        let stored = provider
            .update_runtime_settings(0, &settings)
            .await
            .expect("cluster update");
        assert!(
            matches!(stored, ClusterRuntimeSettingsUpdateOutcome::Stored(document) if document.cluster_version == 1)
        );
        manager_a
            .apply_cluster_settings(settings.clone())
            .await
            .expect("local cache update");

        // Broker B's manager converges via the sync loop.
        tokio::time::timeout(Duration::from_secs(10), async {
            while manager_b.current().settings != settings {
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await
        .expect("replicated settings become effective on broker B");

        let conflict = provider
            .update_runtime_settings(0, &settings)
            .await
            .expect("stale cluster update returns conflict");
        assert!(matches!(
            conflict,
            ClusterRuntimeSettingsUpdateOutcome::Conflict(Some(document))
                if document.cluster_version == 1
        ));

        let mut invalid = settings.clone();
        invalid.partitioning.default_partition_count = 0;
        let err = provider
            .publish_runtime_settings(&invalid)
            .await
            .expect_err("invalid settings are rejected before cluster publish");
        assert!(matches!(err, OpenraftAdapterError::Config(_)));

        provider.raft_node().shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_settings_document_reports_malformed_cluster_attribute() {
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

        provider
            .set_cluster_attribute(RUNTIME_SETTINGS_ATTRIBUTE, "{bad json")
            .await
            .expect("set malformed settings attribute");

        let err = provider
            .runtime_settings_document()
            .expect_err("malformed settings document must be reported");
        assert!(matches!(
            err,
            OpenraftAdapterError::Storage(message)
                if message.contains("decode runtime settings cluster document")
        ));

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

        let queue = QueueIdentity::new("orders", Partition::new(0), None);
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
            .assignment_for("orders", Partition::new(0), None)
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
            .assignment_for("orders", Partition::new(0), None)
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cohort_assignment_publishes_and_reads_back() {
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
        let key = ConsumerGroupKey::new("jobs", None, "default");
        let plan = CohortPlan {
            key: key.clone(),
            assignment: HashMap::from([
                (Partition::new(0), "m1".to_string()),
                (Partition::new(1), "m2".to_string()),
            ]),
        };

        coordination
            .publish_cohort_assignment(&plan)
            .await
            .expect("publish");

        // Read back the committed attribute (poll briefly for commit visibility).
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if coordination.cohort_assignment(&key) == plan.assignment {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "published cohort assignment never became visible"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        coordination.raft_node().shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grow_queue_bumps_version_registers_new_partitions_and_marks_transition() {
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
        coordination
            .declare_queue_partitioning("orders", None, 2)
            .await
            .expect("declare");

        // Grow 2 -> 4.
        let grown = coordination
            .grow_queue("orders", None, 4)
            .await
            .expect("grow");
        assert_eq!(grown.partition_count, 4);
        assert_eq!(grown.partitioning_version, 1);

        // Version bump, new partitions registered, and a transition marker exist.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let part = coordination.queue_partitioning("orders", None);
            let registered: std::collections::HashSet<u32> = coordination
                .registered_queues()
                .into_iter()
                .filter(|q| q.topic.as_str() == "orders")
                .map(|q| q.partition.id())
                .collect();
            let marker = coordination.repartition_transition("orders", None);
            if part.as_ref().map(|p| p.partition_count) == Some(4)
                && registered.is_superset(&std::collections::HashSet::from([2, 3]))
                && marker
                    == Some(RepartitionTransitionDoc {
                        version: 1,
                        n_old: 2,
                        n_new: 4,
                    })
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "grow never became fully visible"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Idempotent at the target count; non-multiple growth is rejected.
        assert_eq!(
            coordination
                .grow_queue("orders", None, 4)
                .await
                .expect("idempotent grow")
                .partition_count,
            4
        );
        assert!(matches!(
            coordination.grow_queue("orders", None, 6).await,
            Err(RepartitionQueueError::NotIntegerMultipleGrowth { .. })
        ));

        coordination.raft_node().shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn repartition_transition_marker_lifecycle() {
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
        let doc = RepartitionTransitionDoc {
            version: 1,
            n_old: 2,
            n_new: 4,
        };
        coordination
            .begin_repartition_transition("orders", None, &doc)
            .await
            .expect("begin transition");

        // The marker becomes readable and is listed as active.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if coordination.repartition_transition("orders", None) == Some(doc.clone()) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "transition marker never became visible"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let active = coordination.active_repartition_transitions();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].0, "orders");
        assert_eq!(active[0].2, doc);

        // Clearing retires the marker: no longer readable or listed.
        coordination
            .clear_repartition_transition("orders", None)
            .await
            .expect("clear transition");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            if coordination.repartition_transition("orders", None).is_none()
                && coordination.active_repartition_transitions().is_empty()
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "transition marker never cleared"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        coordination.raft_node().shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cohort_assignment_generation_bumps_only_on_change() {
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
        let key = ConsumerGroupKey::new("jobs", None, "default");

        // Wait until a publish becomes visible at the expected generation.
        let await_generation = |expected: u64, assignment: HashMap<Partition, String>| {
            let coordination = &coordination;
            let key = key.clone();
            async move {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);
                loop {
                    if let Some(doc) = coordination.cohort_assignment_doc(&key) {
                        let map: HashMap<Partition, String> = doc
                            .entries
                            .iter()
                            .map(|e| (e.partition, e.member.clone()))
                            .collect();
                        if doc.generation == expected && map == assignment {
                            break;
                        }
                    }
                    assert!(
                        std::time::Instant::now() < deadline,
                        "cohort assignment never reached generation {expected}"
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            }
        };

        let plan_a = CohortPlan {
            key: key.clone(),
            assignment: HashMap::from([
                (Partition::new(0), "m1".to_string()),
                (Partition::new(1), "m2".to_string()),
            ]),
        };
        // First publish: generation 0.
        coordination
            .publish_cohort_assignment(&plan_a)
            .await
            .expect("publish a");
        await_generation(0, plan_a.assignment.clone()).await;

        // Republishing the same assignment is a no-op: generation holds at 0.
        coordination
            .publish_cohort_assignment(&plan_a)
            .await
            .expect("republish a");
        await_generation(0, plan_a.assignment.clone()).await;
        assert_eq!(
            coordination.cohort_assignment_doc(&key).unwrap().generation,
            0,
            "stable plan must not bump the generation"
        );

        // A changed assignment bumps the generation to 1.
        let plan_b = CohortPlan {
            key: key.clone(),
            assignment: HashMap::from([
                (Partition::new(0), "m1".to_string()),
                (Partition::new(1), "m1".to_string()),
            ]),
        };
        coordination
            .publish_cohort_assignment(&plan_b)
            .await
            .expect("publish b");
        await_generation(1, plan_b.assignment.clone()).await;

        coordination.raft_node().shutdown().await.expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cohort_controller_tick_plans_from_membership_label() {
        use fibril_broker::coordination::{CohortMemberInfo, StickyConsumerGroupAssignor};
        use std::sync::Arc;

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

        // Register this node with a cohort-membership label: cohort "default"
        // has members m1 + m2 (as the heartbeat would carry).
        let info = NodeInfo {
            node_id: "broker-a".into(),
            broker_addr: "127.0.0.1:9000".parse().expect("addr"),
            admin_addr: None,
        };
        let membership = vec![LocalCohortMembership {
            topic: "jobs".into(),
            group: None,
            consumer_group: "default".into(),
            members: vec![
                CohortMemberInfo {
                    member: "m1".into(),
                    target: None,
                },
                CohortMemberInfo {
                    member: "m2".into(),
                    target: None,
                },
            ],
        }];
        let mut labels = BTreeMap::new();
        labels.insert(
            COHORT_MEMBERSHIP_LABEL.to_string(),
            encode_cohort_membership(&membership),
        );
        coordination
            .register_self_with_labels(&info, labels)
            .await
            .expect("register");

        // Wait until the membership label is committed/visible to the controller.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while coordination.global_cohort_membership().is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "membership label never became visible"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Run a controller tick over a 4-partition queue.
        let mut controller =
            ClusterCohortController::new(Arc::new(StickyConsumerGroupAssignor), None);
        coordination
            .run_cohort_controller_tick(&mut controller, |_| 4)
            .await
            .expect("tick");

        // The published plan covers all 4 partitions, balanced 2/2 across m1/m2.
        let key = ConsumerGroupKey::new("jobs", None, "default");
        let assignment = coordination.cohort_assignment(&key);
        assert_eq!(assignment.len(), 4, "all partitions assigned");
        let mut loads: HashMap<&str, usize> = HashMap::new();
        for member in assignment.values() {
            *loads.entry(member.as_str()).or_default() += 1;
        }
        assert_eq!(loads.get("m1").copied(), Some(2));
        assert_eq!(loads.get("m2").copied(), Some(2));

        coordination.raft_node().shutdown().await.expect("shutdown");
    }

    /// The end-to-end cross-broker case the single-node label test cannot show:
    /// two separate brokers each report only their LOCAL cohort member via the
    /// heartbeat label. The leader's controller aggregates both labels into one
    /// global membership, plans a globally balanced assignment (the thing a
    /// per-broker-local computation cannot reach), and publishes it. Then one
    /// broker loses its consumer and the next tick rebalances onto the survivor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cohort_controller_aggregates_across_brokers_and_rebalances() {
        use fibril_broker::coordination::{CohortMemberInfo, StickyConsumerGroupAssignor};
        use std::sync::Arc;

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

        // One cohort "default" on topic "jobs", spread across two brokers. Each
        // broker reports only the member connected to it (its local view).
        let register = |node_id: &'static str, port: u16, member: Option<&'static str>| {
            let coordination = &coordination;
            async move {
                let info = NodeInfo {
                    node_id: node_id.into(),
                    broker_addr: format!("127.0.0.1:{port}").parse().expect("addr"),
                    admin_addr: None,
                };
                let mut labels = BTreeMap::new();
                if let Some(member) = member {
                    let membership = vec![LocalCohortMembership {
                        topic: "jobs".into(),
                        group: None,
                        consumer_group: "default".into(),
                        members: vec![CohortMemberInfo {
                            member: member.into(),
                            target: None,
                        }],
                    }];
                    labels.insert(
                        COHORT_MEMBERSHIP_LABEL.to_string(),
                        encode_cohort_membership(&membership),
                    );
                }
                coordination
                    .register_self_with_labels(&info, labels)
                    .await
                    .expect("register");
            }
        };

        register("broker-a", 9000, Some("m1")).await;
        register("broker-b", 9001, Some("m2")).await;

        let key = ConsumerGroupKey::new("jobs", None, "default");
        let loads = |coordination: &GanglionCoordination| {
            let assignment = coordination.cohort_assignment(&key);
            let mut loads: HashMap<String, usize> = HashMap::new();
            for member in assignment.values() {
                *loads.entry(member.clone()).or_default() += 1;
            }
            (assignment.len(), loads)
        };

        // Wait until both brokers' labels are aggregated into the global view.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let global = coordination.global_cohort_membership();
            let combined: usize = global.iter().map(|cohort| cohort.members.len()).sum();
            if combined == 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "both brokers' membership labels never aggregated: {global:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let mut controller =
            ClusterCohortController::new(Arc::new(StickyConsumerGroupAssignor), None);

        // First plan: balanced 2/2 across the two brokers' members. A
        // per-broker-local planner could not produce this, neither broker sees
        // the other's member.
        coordination
            .run_cohort_controller_tick(&mut controller, |_| 4)
            .await
            .expect("tick");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (covered, loads) = loads(&coordination);
            if covered == 4 && loads.get("m1") == Some(&2) && loads.get("m2") == Some(&2) {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "cross-broker plan never became balanced 2/2: {loads:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // The first published plan is generation 0.
        assert_eq!(
            coordination.cohort_assignment_doc(&key).unwrap().generation,
            0,
            "first published plan should be generation 0"
        );

        // broker-b loses its consumer (m2 disconnects), so it re-reports an empty
        // local membership. Re-registration replaces the node's labels.
        register("broker-b", 9001, None).await;
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let global = coordination.global_cohort_membership();
            let combined: usize = global.iter().map(|cohort| cohort.members.len()).sum();
            if combined == 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "dropped member never left the global view: {global:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Next tick rebalances: coverage-first, m1 keeps its two sticky
        // partitions and absorbs the orphaned two, so all four land on m1.
        coordination
            .run_cohort_controller_tick(&mut controller, |_| 4)
            .await
            .expect("rebalance tick");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let (covered, loads) = loads(&coordination);
            if covered == 4 && loads.get("m1") == Some(&4) && !loads.contains_key("m2") {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "plan never rebalanced onto the survivor: {loads:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // The rebalance changed the assignment, so the generation bumped to 1.
        assert_eq!(
            coordination.cohort_assignment_doc(&key).unwrap().generation,
            1,
            "rebalance should bump the plan generation"
        );

        coordination.raft_node().shutdown().await.expect("shutdown");
    }
}
