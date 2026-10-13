//! Spike: ganglion raft-backed implementation of fibril's [`Coordination`] trait.
//!
//! A `RaftMetadataNode` replicates the coordination snapshot through real
//! consensus; this crate maps between fibril's and ganglion's snapshot models
//! and bridges ganglion's committed-snapshot watch into the
//! `watch::Receiver<CoordinationSnapshot>` fibril consumes. Reads are sync
//! (trait-compatible); proposals are async and leader-only, matching the
//! controller-loop model. See the coordination internals dev note:
//! https://fibril.sh/development/coordination-internals/

use std::{collections::HashMap, time::Duration};

use fibril_broker::coordination::{
    aggregate_cohort_membership, ClusterCohortController, CohortPlan, ConsumerGroupKey,
    Coordination, CoordinationSnapshot, CoordinationStream, DeterministicStreamPlacement,
    GlobalCohortMembership, LocalCohortMembership, NodeInfo, PartitionAssignment,
    PartitionPlacementError, PartitionPlacementInput, PartitionPlacementPolicy, QueueIdentity,
    ReplicationDurabilityPolicy, StreamAssignment, StreamIdentity, StreamPlacementInput,
    StreamPlacementPolicy,
};
use fibril_broker::Partition;
use ganglion_openraft::{
    client_write_remote_with_hint, MetadataRaftCommand, MetadataRaftResponse, MetadataRejection,
    OpenraftAdapterError, RaftMetadataNode, RemoteWriteError, WireFormat,
};
use tokio::sync::watch;

pub mod recovery_diagnostics;
pub mod history_identity;
pub mod initial_history;
pub mod history_activation;
pub mod promotion;
pub mod recovery_witnesses;
pub mod recovery_selection;
pub mod recovery_plan;
pub mod recovery_activation;
mod eager_failover;
pub mod operator_override;
mod recovery_candidate;
pub mod queue_learner;
pub mod queue_reseed;
pub mod queue_checkpoint;

/// Namespace tag used for fibril queues inside ganglion resource identities.
const QUEUE_NAMESPACE: &str = "fibril/queue";

/// Namespace tag for fibril streams (Plexus) inside ganglion resource
/// identities. Streams place through the same controller as queues but live in
/// their own namespace: they carry an owner only (no follower set), and the
/// fibril side keeps them in a separate `stream_assignments` map.
const STREAM_NAMESPACE: &str = "fibril/stream";

/// Attribute key carrying the replicated runtime-settings document.
pub const RUNTIME_SETTINGS_ATTRIBUTE: &str = "fibril/runtime_settings";
/// Cluster attribute carrying the authoritative user document (argon2
/// hashes). Parallel to the runtime-settings document, kept separate so a
/// settings write can never clobber users.
pub const AUTH_USERS_ATTRIBUTE: &str = "fibril/auth_users";

/// Heartbeat node-label key carrying a broker's local exclusive-cohort
/// membership (JSON `Vec<LocalCohortMembership>`). The controller reads every
/// live node's label, aggregates global membership, and computes one plan per
/// cohort.
pub const COHORT_MEMBERSHIP_LABEL: &str = "fibril/cohort_membership";

/// Heartbeat label carrying a broker's full advertise list (priority-ordered
/// `host:port` strings) so clients learn every reachable endpoint of an owner, not
/// just the single endpoint registered in the node table.
pub const ADVERTISE_LABEL: &str = "fibril/advertise";

/// Heartbeat label a draining broker sets (any value). The controller treats
/// a live draining node as an evacuation source and stops placing anything
/// new on it. A label rather than a raft attribute so the state expires with
/// the node's heartbeats and a crashed drain leaves nothing behind.
pub const DRAINING_LABEL: &str = "fibril/draining";
/// Heartbeat label a private broker sets to [`PRIVATE_VISIBILITY`]. A private
/// broker is never given to clients, and its client endpoint list is empty.
/// Absence means public, so clusters without the label behave as before. It
/// rides the first registration, so a private node is never visible without
/// it.
pub const VISIBILITY_LABEL: &str = "fibril/visibility";
pub const PRIVATE_VISIBILITY: &str = "private";
/// Advisory label carrying how many upstream sessions a public broker holds
/// open to private owners while forwarding for its clients. Absent when zero.
pub const UPSTREAM_SESSIONS_LABEL: &str = "fibril/upstream_sessions";
/// Advisory label carrying how many forwarded sessions an owner serves for
/// public brokers. Absent when zero.
pub const FORWARDED_SESSIONS_LABEL: &str = "fibril/forwarded_sessions";
/// Advisory label carrying the node's raft id, so observability can correlate
/// broker node ids with consensus membership.
pub const RAFT_ID_LABEL: &str = "fibril/raft-id";
/// Process replacement hint. This may request recovery but never grants admission.
pub const HISTORY_PROCESS_LABEL: &str = "fibril/history-process";
/// Attribute key prefix, followed by a node id, holding the process that node
/// last published. Written once per process start so the change bumps the
/// generation (see `register_self_with_labels`).
pub const NODE_PROCESS_ATTRIBUTE_PREFIX: &str = "fibril/node-process/";
/// Advisory labels carrying a broker's coarse throughput - messages per
/// second since its previous heartbeat - for the admin topology view.
pub const RATE_PUB_LABEL: &str = "fibril/rate-pub";
pub const RATE_DLV_LABEL: &str = "fibril/rate-dlv";
/// Advisory labels carrying broker runtime facts for the admin views:
/// version, process start (unix ms), TLS mode ("off" | "tls" | "mtls"),
/// and - when serving TLS - the leaf certificate's not-after (unix
/// seconds; days-left computes reader-side so the label only changes on
/// rotation).
pub const VERSION_LABEL: &str = "fibril/version";
pub const STARTED_AT_LABEL: &str = "fibril/started-at";
pub const TLS_LABEL: &str = "fibril/tls";
pub const CERT_EXPIRY_LABEL: &str = "fibril/cert-expiry";

/// Serialize a broker's advertise list for its heartbeat label.
pub fn encode_advertise(addrs: &[String]) -> String {
    serde_json::to_string(addrs).unwrap_or_else(|_| "[]".to_string())
}

/// Parse a broker's advertise label; malformed/absent -> empty (the caller falls
/// back to the node's registered endpoint).
pub fn decode_advertise(raw: &str) -> Vec<String> {
    serde_json::from_str(raw).unwrap_or_default()
}

/// A node's advertise list for client routing: the full priority-ordered list
/// from its advertise label when present and non-empty, otherwise the single
/// registered endpoint (empty when that is absent too).
fn node_advertise_endpoints(node: &ganglion_core::NodeInfo) -> Vec<String> {
    if gateway::node_is_private(node) {
        return Vec::new();
    }
    advertise_endpoints(
        &node.endpoint,
        node.labels.get(ADVERTISE_LABEL).map(String::as_str),
    )
}

fn advertise_endpoints(endpoint: &str, advertised: Option<&str>) -> Vec<String> {
    advertised
        .map(decode_advertise)
        .filter(|list| !list.is_empty())
        .unwrap_or_else(|| {
            if endpoint.is_empty() {
                Vec::new()
            } else {
                vec![endpoint.to_owned()]
            }
        })
}

pub mod gateway;
mod targeted_route;

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

/// Heartbeat label key under which a node reports the lowest topology generation
/// its client connections have acked (the minimum across connections that have
/// acked at least once). The repartition controller takes the cluster-wide
/// minimum of these to fence a cutover until clients have adopted the new
/// routing. Absent when the node has no acked client connections.
pub const TOPOLOGY_ADOPTION_LABEL: &str = "fibril/topology_adoption";

/// Replicated marker for an in-progress grow of a queue `(topic, group)`.
/// Present only while the transition runs (the controller clears it once every
/// old partition has drained). `version` scopes drain reports to this grow so a
/// stale report from a prior transition is ignored.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RepartitionTransitionDoc {
    pub version: u64,
    pub n_old: u32,
    pub n_new: u32,
    /// Topology generation at/after which the new partitioning became visible to
    /// clients, captured just after the routing cutover (the version bump). The
    /// controller fences the transition's finalize until clients have acked a
    /// topology generation at least this high (cluster-wide adoption), or an
    /// adoption timeout elapses. `None` on markers written before this field
    /// existed, or before the cutover has stamped it - treated as "no adoption
    /// gate" so an in-flight upgrade never stalls.
    #[serde(default)]
    pub adoption_generation: Option<u64>,
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

/// The cluster-wide minimum topology generation acked by clients, taken across
/// every node's adoption label. Unparseable or absent labels contribute nothing.
/// `None` when no node reports an adoption label (no client has acked anywhere).
pub fn aggregate_topology_adoption<'a>(labels: impl IntoIterator<Item = &'a str>) -> Option<u64> {
    labels
        .into_iter()
        .filter_map(|raw| raw.parse::<u64>().ok())
        .min()
}

/// The cluster-wide minimum acked topology generation across only the LIVE nodes,
/// given each node's `(heartbeat_label, adoption_label)` pair. A dead node's
/// adoption label is frozen at its last value and represents departed clients (the
/// clients that were connected to it are gone), so counting it would wrongly pin
/// the minimum down and stall every cutover on the adoption timeout. A node with
/// no heartbeat label counts as live (static or manually registered). `None` when
/// no live node reports an adoption label.
pub fn live_topology_adoption<'a>(
    nodes: impl IntoIterator<Item = (Option<&'a str>, Option<&'a str>)>,
    now_ms: u64,
    ttl_ms: u64,
) -> Option<u64> {
    let live_labels = nodes.into_iter().filter_map(|(heartbeat, adoption)| {
        let alive = heartbeat
            .and_then(|raw| raw.parse::<u64>().ok())
            .is_none_or(|beat| now_ms.saturating_sub(beat) <= ttl_ms);
        alive.then_some(adoption).flatten()
    });
    aggregate_topology_adoption(live_labels)
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

/// The cluster-authoritative user document plus its CAS version.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClusterUsers {
    pub cluster_version: u64,
    pub document: fibril_broker::auth_store::UserDocument,
}

#[derive(Debug, Clone)]
pub enum ClusterUsersUpdateOutcome {
    Stored(ClusterUsers),
    Conflict(Option<ClusterUsers>),
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

/// Attribute key prefix for a stream's replicated partitioning, kept separate
/// from the queue prefix so a stream and a same-named queue never collide.
/// Streams have no group, so the key is `fibril/stream_config/<topic>`. The doc
/// is the full stream declaration (partitioning + durability + retention), so an
/// owner that did not declare the stream can still open its partitions with the
/// right config. Queues keep config in the replicated log; streams cannot,
/// because the owner-only tiers have no log to ride (see the #73 design note).
pub const STREAM_CONFIG_ATTRIBUTE_PREFIX: &str = "fibril/stream_config/";

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

fn queue_declaration_key(topic: &str, group: Option<&str>) -> String {
    // JSON tuple encoding keeps topics/groups containing separators distinct.
    format!(
        "fibril/queue_declaration/{}",
        serde_json::to_string(&(topic, group)).unwrap()
    )
}

fn stream_config_key(topic: &str) -> String {
    format!("{STREAM_CONFIG_ATTRIBUTE_PREFIX}{topic}")
}

/// A stream's retention bounds, mirrored from the protocol on declare. `None`
/// fields mean unbounded on that axis.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StreamRetentionConfig {
    pub max_age_ms: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_records: Option<u64>,
}

/// Stream durability tier ordinal for the durable tier (matches the protocol
/// `StreamDurability` ordinals: 0 ephemeral, 1 speculative, 2 durable). Only the
/// durable tier replicates across nodes.
pub const STREAM_DURABILITY_DURABLE: u8 = 2;

/// Stream durability tier ordinal for the ephemeral tier, which confirms
/// before a record is durable and so makes no durability promise.
pub const STREAM_DURABILITY_EPHEMERAL: u8 = 0;

/// A stream's full declared configuration, replicated through coordination so any
/// owner can open its partitions with the correct partitioning, durability tier,
/// and retention. `durability` is the tier ordinal (0 ephemeral, 1 speculative,
/// 2 durable) to keep coordination free of a protocol dependency.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct StreamConfig {
    pub partition_count: u32,
    pub partitioning_version: u64,
    pub durability: u8,
    #[serde(default)]
    pub retention: StreamRetentionConfig,
    /// Per-stream durable-tier replication factor (follower count). `None` uses
    /// the controller's cluster default `stream_replication_factor`. Only the
    /// durable tier replicates.
    #[serde(default)]
    pub replication_factor: Option<u32>,
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
    /// The queue already exists with a different durability mode. The mode
    /// is fixed when the queue is first declared.
    DurabilityConflict {
        topic: String,
        group: Option<String>,
        existing: fibril_broker::queue_engine::QueueDurability,
        requested: fibril_broker::queue_engine::QueueDurability,
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
            Self::DurabilityConflict {
                topic,
                group,
                existing,
                requested,
            } => write!(
                f,
                "queue `{topic}`{} is already declared {} (requested {}); the durability mode is fixed when a queue is first declared",
                group
                    .as_deref()
                    .map(|g| format!("/{g}"))
                    .unwrap_or_default(),
                existing.as_str(),
                requested.as_str()
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
    /// The requested count is not a smaller integer factor of the current count.
    /// v1 shrinks by an integer factor only (the new count must divide the
    /// current one), mirroring the integer-multiple grow constraint.
    NotIntegerFactorShrink {
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
            Self::NotIntegerFactorShrink {
                topic,
                group,
                current,
                requested,
            } => write!(
                f,
                "queue {} cannot shrink from {current} to {requested} partitions: \
                 the new count must be a smaller integer factor of the current one",
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
    pub partition: fibril_broker::Partition,
    pub group: Option<String>,
    pub owner_node_id: String,
    /// Client endpoints for the partition in priority order, empty when the
    /// owner is not in the registry, or is private with no live public broker
    /// to serve it. Clients try them in order.
    pub owner_endpoints: Vec<String>,
    /// Set when the owner is private and `owner_endpoints` belong to the
    /// public broker chosen to forward for it.
    #[serde(default)]
    pub via_gateway: bool,
    pub partitioning_version: u64,
    /// Total partition count of this queue `(topic, group)` — authoritative N
    /// for key routing.
    pub partition_count: u32,
}

/// Client-facing ownership of one stream partition: which node owns it and where
/// to reach that node. Streams have no group and no follower set, so this is the
/// queue shape minus those fields.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClientStreamTopology {
    pub topic: String,
    pub partition: fibril_broker::Partition,
    pub owner_node_id: String,
    /// Client endpoints for the partition in priority order, see
    /// [`ClientQueueTopology::owner_endpoints`].
    pub owner_endpoints: Vec<String>,
    /// See [`ClientQueueTopology::via_gateway`].
    #[serde(default)]
    pub via_gateway: bool,
    pub partitioning_version: u64,
    /// Total partition count of this stream topic — authoritative N for key
    /// routing.
    pub partition_count: u32,
}

/// Routing view of one partition for the protocol handler: who owns it, the
/// owner's own client endpoints, and what clients are told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientRoute {
    pub owner_node_id: String,
    /// The owner's own client endpoints. Empty when the owner is private.
    pub owner_endpoints: Vec<String>,
    /// What topology answers and redirects carry: the owner's endpoints, or a
    /// gateway's when the owner is private. Empty with no live gateway.
    pub client_endpoints: Vec<String>,
    pub partitioning_version: u64,
}

/// Client-facing topology snapshot: the owner/endpoint of every assigned queue
/// and stream partition at a given coordination generation. Clients route to
/// owners from this and refresh it on not-owner / stale-topology errors.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClientTopology {
    pub generation: u64,
    pub queues: Vec<ClientQueueTopology>,
    #[serde(default)]
    pub streams: Vec<ClientStreamTopology>,
}

/// Heartbeat label prefix for per-assignment applied tails:
/// `applied/<topic>/<partition>[/<group>]`. A queue publishes `"<next>"`, its
/// position in the single queue log. A stream publishes
/// `"<message_next>:<event_next>"`, its record and cursor-event logs.
/// Advisory data for failover candidate selection. Local promotion checks log
/// and state completeness, but do not prove possession of every previously
/// confirmed batch. Stale labels can select a consistent but lagging candidate.
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

/// Applied-tail label value for a queue follower.
pub fn encode_queue_applied_tail(next: u64) -> String {
    next.to_string()
}

/// Applied-tail label value for a stream follower.
pub fn encode_stream_applied_tail(message_next: u64, event_next: u64) -> String {
    format!("{message_next}:{event_next}")
}

/// Ranking value of an applied-tail label, compared lexicographically. A
/// queue ranks by its log position. A stream ranks by its record tail first,
/// since producer confirms count record progress only, and by its
/// cursor-event tail second: a lagging event log rewinds cursors and causes
/// redelivery, while a lagging record log loses confirmed records. A value in
/// the other kind's format is ignored.
fn parse_applied_tail(raw: &str, stream: bool) -> Option<(u64, u64)> {
    if stream {
        let (message, event) = raw.split_once(':')?;
        Some((message.parse().ok()?, event.parse().ok()?))
    } else {
        Some((raw.parse().ok()?, 0))
    }
}

/// New owner for a partition whose owner is gone: the eligible committed
/// follower with the best applied-tail label. A follower with a label beats
/// one without. Ties go to the follower owning the fewest partitions, then
/// to the smallest node id. None when no follower is eligible.
fn choose_failover_owner(
    current: &ganglion_core::PartitionAssignment,
    committed: &ganglion_core::CoordinationSnapshot,
    label: &str,
    stream: bool,
    owned: &HashMap<String, usize>,
    eligible: impl Fn(&str) -> bool,
) -> Option<String> {
    let owned = |node: &str| owned.get(node).copied().unwrap_or(0);
    current
        .followers
        .iter()
        .filter(|follower| eligible(follower))
        .map(|follower| {
            let tail = committed
                .nodes
                .get(follower)
                .and_then(|node| node.labels.get(label))
                .and_then(|raw| parse_applied_tail(raw, stream));
            (tail, follower)
        })
        .max_by(|(tail_a, a), (tail_b, b)| {
            tail_a
                .cmp(tail_b)
                .then_with(|| owned(b).cmp(&owned(a)))
                .then_with(|| b.cmp(a))
        })
        .map(|(_, follower)| follower.clone())
}

/// Registered nodes whose heartbeat is not fresh. The controller startup
/// grace waits for this list to empty before it moves any owner.
fn startup_grace_missing(
    committed: &ganglion_core::CoordinationSnapshot,
    live: &HashMap<String, NodeInfo>,
) -> Vec<String> {
    let mut missing: Vec<String> = committed
        .nodes
        .keys()
        .filter(|node| !live.contains_key(*node))
        .cloned()
        .collect();
    missing.sort();
    missing
}

/// Keep every existing assignment's owner and followers. A node missing from
/// the live set may still be booting, so nothing moves away from it. Nodes the
/// planner chose, its owner pick included, only fill slots up to the target.
fn hold_existing_placements(
    committed: &ganglion_core::CoordinationSnapshot,
    desired: &mut ganglion_core::CoordinationSnapshot,
    target_followers: impl Fn(&ganglion_core::ResourceIdentity) -> usize,
) {
    for (resource, planned) in desired.assignments.iter_mut() {
        let Some(current) = committed.assignments.get(resource) else {
            continue;
        };
        let target = target_followers(resource);
        let mut followers = current.followers.clone();
        for candidate in planned.followers.iter().chain(std::iter::once(&planned.owner)) {
            if followers.len() >= target {
                break;
            }
            if *candidate != current.owner && !followers.contains(candidate) {
                followers.push(candidate.clone());
            }
        }
        planned.owner = current.owner.clone();
        planned.followers = followers;
    }
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
                    info.broker_addr.clone(),
                    info.admin_addr.clone(),
                ),
            )
        })
        .collect();

    let mut assignments: std::collections::BTreeMap<_, _> = snapshot
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

    // Streams place through the same controller, in their own namespace: an
    // owner with no followers and the default (local) durability — durability is
    // the local keratin tier, not cross-node replication.
    for (stream, assignment) in &snapshot.stream_assignments {
        let resource = to_ganglion_stream_resource(stream);
        let mapped = ganglion_core::PartitionAssignment::new(
            resource.clone(),
            assignment.owner.clone(),
            assignment.followers.clone(),
            assignment.epoch,
        );
        assignments.insert(resource, mapped);
    }

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
    project_snapshot(snapshot, true)
}

/// Placement must retain enrolled resources while serving keeps them fenced.
/// This projection is private so brokers cannot accidentally use it for admission.
fn project_snapshot(
    snapshot: &ganglion_core::CoordinationSnapshot,
    serving: bool,
) -> CoordinationSnapshot {
    let nodes: HashMap<String, NodeInfo> = snapshot
        .nodes
        .iter()
        .map(|(node_id, info)| {
            // The broker endpoint is kept as a connectable string (it may be a
            // service name), so it is carried through verbatim rather than parsed.
            let broker_addr = info.endpoint.clone();
            let admin_addr = info
                .admin_endpoint
                .as_ref()
                .and_then(|endpoint| endpoint.parse().ok());
            (
                node_id.clone(),
                NodeInfo {
                    node_id: info.node_id.clone(),
                    broker_addr,
                    admin_addr,
                },
            )
        })
        .collect();

    let assignments = snapshot
        .assignments
        .iter()
        .filter_map(|(resource, assignment)| {
            let queue = to_fibril_queue(resource)?;
            let history =
                if serving && !history_identity::permits_legacy_serving(snapshot, resource) {
                    Some(std::sync::Arc::new(
                        history_activation::accepted_history(snapshot, resource).ok()?,
                    ))
                } else {
                    None
                };
            let mut mapped = PartitionAssignment::new(
                queue.clone(),
                assignment.owner.clone(),
                assignment.followers.clone(),
                assignment.epoch,
            );
            mapped.durability = to_fibril_durability(assignment.durability);
            mapped.history = history;
            Some((queue, mapped))
        })
        .collect();

    // Stream assignments live in their own namespace and own map (owner + epoch
    // only); the queue filter above skips them.
    let stream_assignments = snapshot
        .assignments
        .iter()
        .filter_map(|(resource, assignment)| {
            let stream = to_fibril_stream(resource)?;
            let history =
                if serving && !history_identity::permits_legacy_serving(snapshot, resource) {
                    Some(std::sync::Arc::new(
                        history_activation::accepted_history(snapshot, resource).ok()?,
                    ))
                } else {
                    None
                };
            let mut mapped = StreamAssignment::new(
                stream.clone(),
                assignment.owner.clone(),
                assignment.followers.clone(),
                assignment.epoch,
            );
            mapped.history = history;
            Some((stream, mapped))
        })
        .collect();

    CoordinationSnapshot {
        nodes,
        assignments,
        stream_assignments,
        generation: snapshot.generation,
    }
}

/// A restarted provider cannot consume an activation for an earlier process.
/// The global topology still retains the full configured replica count.
fn local_serving_snapshot(
    snapshot: &ganglion_core::CoordinationSnapshot,
    node: &str,
    process: [u8; 16],
) -> CoordinationSnapshot {
    let mut mapped = to_fibril_snapshot(snapshot);
    let restrict = |history: &mut Option<std::sync::Arc<fibril_broker::history_replication::AcceptedHistory>>| {
        if let Some(history) = history {
            if !history.replicas.get(node).is_some_and(|replica| replica.process == process) {
                std::sync::Arc::make_mut(history).blocked_local_replica = Some(node.to_owned());
            }
        }
    };
    for assignment in mapped.assignments.values_mut() { restrict(&mut assignment.history); }
    for assignment in mapped.stream_assignments.values_mut() { restrict(&mut assignment.history); }
    mapped
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

fn to_ganglion_stream_resource(stream: &StreamIdentity) -> ganglion_core::ResourceIdentity {
    ganglion_core::ResourceIdentity::new(
        STREAM_NAMESPACE,
        stream.topic.clone(),
        u64::from(stream.partition.id()),
        None,
    )
}

fn to_fibril_stream(resource: &ganglion_core::ResourceIdentity) -> Option<StreamIdentity> {
    if resource.namespace != STREAM_NAMESPACE {
        return None;
    }
    let partition = Partition::new(u32::try_from(resource.partition).ok()?);
    Some(StreamIdentity::new(resource.name.clone(), partition))
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
    /// Boot policy until a cluster runtime-settings document exists.
    pub eager_failover: bool,
    pub eager_failover_grace_ms: u64,
    pub target_followers: usize,
    /// Replica followers for a DURABLE stream partition (the durable tier; the
    /// express tiers stay owner-only). Tuned independently of the queue
    /// `target_followers` so stream and queue fault tolerance can differ.
    pub stream_replication_factor: usize,
    pub default_durability: ReplicationDurabilityPolicy,
    /// Bounded tick interval; the loop also wakes on snapshot changes.
    pub tick: std::time::Duration,
    /// Heartbeats older than this mark a broker dead. Must exceed worst-case
    /// election + retry time (ganglion FAILURE_MODES §4b.6).
    pub liveness_ttl: std::time::Duration,
    pub max_cas_retries: usize,
    /// After winning leadership, move no owner until every registered node
    /// heartbeats fresh or this long has passed. Zero disables the grace.
    pub startup_grace: std::time::Duration,
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            eager_failover: false,
            eager_failover_grace_ms: 1_000,
            target_followers: 1,
            stream_replication_factor: 1,
            default_durability: ReplicationDurabilityPolicy::LocalDurable,
            tick: std::time::Duration::from_millis(2000),
            liveness_ttl: std::time::Duration::from_millis(9000),
            max_cas_retries: 8,
            startup_grace: std::time::Duration::from_secs(30),
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
    /// Assignment changes held until confirmed-history recovery can complete.
    pub pending_recoveries: Vec<promotion::PendingRecovery>,
    /// Holds and pending recoveries that need an operator decision.
    pub attention: Vec<operator_override::RecoveryAttention>,
    pub eager_suspects: Vec<eager_failover::EagerSuspect>,
    /// Set while the startup grace keeps owners in place after this node
    /// became leader: the registered nodes without a fresh heartbeat.
    pub startup_grace_waiting_for: Vec<String>,
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
pub struct GanglionCoordination {
    node_id: String,
    // Private instance identity: a replacement provider cannot inherit an old writer grant.
    history_process: [u8; 16],
    // Set once this process's identity has been published with a generation
    // bump, see register_self_with_labels.
    process_published: std::sync::atomic::AtomicBool,
    checkpoint_operation: tokio::sync::Mutex<()>,
    recovery_diagnostics: recovery_diagnostics::RecoveryDiagnostics,
    node: RaftMetadataNode,
    tx: watch::Sender<CoordinationSnapshot>,
    forwarder: tokio::task::JoinHandle<()>,
    wire_format: WireFormat,
    forwarded_write_policy: ForwardedWritePolicy,
    // Heartbeat age past which a public broker stops being a gateway. Set
    // from the controller's liveness TTL at startup.
    gateway_liveness_ttl_ms: std::sync::atomic::AtomicU64,
    // Mode for queues first declared without one. Buffered when the cluster
    // assignment durability is `replica_buffered`.
    default_queue_buffered: std::sync::atomic::AtomicBool,
    accepted_histories: history_activation::AcceptedHistoryCache,
}

impl std::fmt::Debug for GanglionCoordination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GanglionCoordination")
            .field("node_id", &self.node_id)
            .field("raft_node_id", &self.node.node_id())
            .finish_non_exhaustive()
    }
}

impl GanglionCoordination {
    /// Bounded node-local recovery observations; never used for authority.
    pub fn recovery_diagnostics(&self) -> &recovery_diagnostics::RecoveryDiagnostics {
        &self.recovery_diagnostics
    }

    /// Wrap a started raft node as a fibril coordination provider.
    ///
    /// `node_id` is the fibril-side node identity (string); the raft node id
    /// (u64) is an internal transport concern and need not match.
    ///
    /// Must be called from within a tokio runtime (spawns the watch forwarder).
    pub fn new(node_id: impl Into<String>, node: RaftMetadataNode) -> Self {
        Self::new_with_wire_format(node_id, node, WireFormat::default())
    }

    /// `new` with the wire format used for forwarded writes (registration /
    /// heartbeats sent to the leader). Pass from startup configuration.
    pub fn new_with_wire_format(
        node_id: impl Into<String>,
        node: RaftMetadataNode,
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
        node: RaftMetadataNode,
        wire_format: WireFormat,
        forwarded_write_policy: ForwardedWritePolicy,
    ) -> Self {
        let node_id = node_id.into();
        let history_process = *uuid::Uuid::now_v7().as_bytes();
        let mut ganglion_rx = node.watch_committed();
        let initial = local_serving_snapshot(&ganglion_rx.borrow_and_update(), &node_id, history_process);
        let (tx, _rx) = watch::channel(initial);

        let forward_tx = tx.clone();
        let forward_node = node_id.clone();
        let forwarder = tokio::spawn(async move {
            while ganglion_rx.changed().await.is_ok() {
                let mapped = local_serving_snapshot(&ganglion_rx.borrow_and_update(), &forward_node, history_process);
                forward_tx.send_replace(mapped);
            }
        });

        Self {
            node_id,
            history_process,
            process_published: std::sync::atomic::AtomicBool::new(false),
            checkpoint_operation: tokio::sync::Mutex::new(()),
            recovery_diagnostics: Default::default(),
            node,
            tx,
            forwarder,
            wire_format,
            forwarded_write_policy,
            gateway_liveness_ttl_ms: std::sync::atomic::AtomicU64::new(
                ControllerConfig::default().liveness_ttl.as_millis() as u64,
            ),
            default_queue_buffered: std::sync::atomic::AtomicBool::new(false),
            accepted_histories: Default::default(),
        }
    }

    /// Mode for queues first declared without one. Existing queues keep theirs.
    pub fn set_default_queue_durability(&self, mode: fibril_broker::queue_engine::QueueDurability) {
        self.default_queue_buffered.store(
            mode == fibril_broker::queue_engine::QueueDurability::Buffered,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    fn default_queue_durability(&self) -> fibril_broker::queue_engine::QueueDurability {
        if self
            .default_queue_buffered
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            fibril_broker::queue_engine::QueueDurability::Buffered
        } else {
            fibril_broker::queue_engine::QueueDurability::Durable
        }
    }

    /// Use the controller's liveness TTL when choosing gateways, so a public
    /// broker the controller considers dead is never named to clients.
    pub fn set_gateway_liveness_ttl(&self, ttl: std::time::Duration) {
        self.gateway_liveness_ttl_ms
            .store(ttl.as_millis() as u64, std::sync::atomic::Ordering::Relaxed);
    }

    fn gateway_liveness_ttl_ms(&self) -> u64 {
        self.gateway_liveness_ttl_ms
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The address peers dial for `node_id` (the node table endpoint).
    pub fn node_peer_address(&self, node_id: &str) -> Option<String> {
        self.node.read_committed(|state| {
            state
                .nodes
                .values()
                .rev()
                .find(|node| node.node_id == node_id)
                .map(|node| node.endpoint.clone())
                .filter(|endpoint| !endpoint.is_empty())
        })
    }

    /// Whether the current view holds a private node but no live public
    /// broker that could forward for it. Clients then get empty endpoints for
    /// private-owned partitions.
    pub fn private_nodes_without_gateway(&self) -> bool {
        let state = self.node.committed_snapshot();
        let any_private = state.nodes.values().any(gateway::node_is_private);
        any_private
            && gateway::gateway_candidates(
                state.nodes.values(),
                unix_millis_now(),
                self.gateway_liveness_ttl_ms(),
                node_advertise_endpoints,
            )
            .is_empty()
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
            info.broker_addr.clone(),
            info.admin_addr.clone(),
        );
        node.labels = labels;
        // Never copy an application-supplied session identity. Tests and admin
        // callers can register another node; only this provider can report itself.
        node.labels.remove(HISTORY_PROCESS_LABEL);
        if info.node_id == self.node_id {
            node.labels.insert(HISTORY_PROCESS_LABEL.into(), uuid::Uuid::from_bytes(self.history_process).to_string());
        }
        node.labels
            .insert(HEARTBEAT_LABEL.to_string(), unix_millis_now().to_string());

        self.forward_merge(MetadataRaftCommand::RegisterNode { node })
            .await?;
        if info.node_id == self.node_id
            && !self.process_published.load(std::sync::atomic::Ordering::Acquire)
        {
            // Label-only node updates do not bump the generation or wake
            // watchers, so a restarted process's new identity could stay
            // invisible to every watched snapshot until some unrelated write.
            // Histories built from such a stale view suspend this replica and
            // nothing re-admits it. One attribute write per process forces a
            // published snapshot that carries the new label.
            self.forward_merge(MetadataRaftCommand::SetAttribute {
                key: format!("{NODE_PROCESS_ATTRIBUTE_PREFIX}{}", self.node_id),
                value: uuid::Uuid::from_bytes(self.history_process).to_string(),
            })
            .await?;
            self.process_published
                .store(true, std::sync::atomic::Ordering::Release);
        }
        Ok(())
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
        self.register_initial_history_resource(&to_ganglion_resource(queue))
            .await
            .map(|_| ())
    }

    /// Legacy-only fixture for tests of the pre-enrollment controller surface.
    #[cfg(test)]
    async fn register_legacy_queue(&self, queue: &QueueIdentity) -> Result<(), OpenraftAdapterError> {
        self.forward_merge(history_identity::registration(to_ganglion_resource(queue))?).await
    }

    /// Remove a queue from the cluster catalogue (forwarded merge).
    pub async fn deregister_queue(
        &self,
        queue: &QueueIdentity,
    ) -> Result<(), OpenraftAdapterError> {
        self.deregister_resource_incarnation(to_ganglion_resource(queue))
            .await
    }

    async fn deregister_resource_incarnation(
        &self,
        resource: ganglion_core::ResourceIdentity,
    ) -> Result<(), OpenraftAdapterError> {
        let key = history_identity::key(&resource);
        let (expected, incarnation) = self.node.read_committed(|snapshot| {
            (snapshot.attributes.get(&key).cloned(), history_identity::resource_incarnation(snapshot, &resource))
        });
        let incarnation = incarnation.map_err(OpenraftAdapterError::Storage)?;
        // Keep this observation fixed through forwarding/retry. Retrying a
        // rejected delete with a replacement's identity could erase new work.
        let command = if let Some(mut identity) = incarnation.filter(|identity| identity.version == 2) {
            identity.retired = true;
            MetadataRaftCommand::DeregisterResourceReplacingAttribute {
                resource, key, expected,
                value: serde_json::to_string(&identity).map_err(|e| OpenraftAdapterError::Storage(e.to_string()))?,
            }
        } else {
            MetadataRaftCommand::DeregisterResourceWithAttribute { resource, key, expected }
        };
        self.forward_merge(command).await
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

    /// Add a stream partition to the cluster catalogue (forwarded merge;
    /// idempotent). The controller places catalogued streams onto owners.
    pub async fn register_stream(
        &self,
        stream: &StreamIdentity,
    ) -> Result<(), OpenraftAdapterError> {
        self.forward_merge(history_identity::registration(to_ganglion_stream_resource(stream))?)
            .await
    }

    /// Remove a stream partition from the cluster catalogue (forwarded merge).
    pub async fn deregister_stream(
        &self,
        stream: &StreamIdentity,
    ) -> Result<(), OpenraftAdapterError> {
        self.deregister_resource_incarnation(to_ganglion_stream_resource(stream))
            .await
    }

    /// The committed cluster stream catalogue (fibril-representable entries).
    pub fn registered_streams(&self) -> Vec<StreamIdentity> {
        self.node
            .committed_snapshot()
            .resources
            .iter()
            .filter_map(to_fibril_stream)
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
            .filter_map(|node| {
                node.labels
                    .get(REPARTITION_DRAINED_LABEL)
                    .map(String::as_str)
            })
            .collect();
        aggregate_repartition_drained(labels, topic, group, version)
    }

    /// The cluster-wide minimum topology generation acked by client connections,
    /// taken across only the LIVE nodes' adoption labels (heartbeat within `ttl`).
    /// Dead nodes are excluded: their frozen adoption label represents departed
    /// clients and would otherwise pin the minimum down and stall every cutover on
    /// the adoption timeout. `None` when no live node reports a label (no client
    /// has acked) - the controller reads that as "no adoption yet" and leans on the
    /// adoption timeout. A node with acked clients reports its own minimum; the
    /// cluster minimum is the lowest of those.
    pub fn global_topology_adoption(&self, ttl: std::time::Duration) -> Option<u64> {
        let snapshot = self.node.committed_snapshot();
        let now = unix_millis_now();
        let ttl_ms = ttl.as_millis() as u64;
        let nodes = snapshot.nodes.values().map(|node| {
            (
                node.labels.get(HEARTBEAT_LABEL).map(String::as_str),
                node.labels.get(TOPOLOGY_ADOPTION_LABEL).map(String::as_str),
            )
        });
        live_topology_adoption(nodes, now, ttl_ms)
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

    /// Store queue settings before cataloguing its partitions. Only assigned
    /// owners append Declare events; the receiving broker never creates a log.
    /// Like storage declaration, omitted settings preserve their previous values.
    /// The durability mode is set by the first declare, from the request or
    /// the cluster default, and a later declare naming another mode fails.
    pub async fn declare_queue(
        &self,
        topic: &str,
        group: Option<&str>,
        partition_count: u32,
        declaration: fibril_broker::queue_engine::QueueDeclaration,
    ) -> Result<QueuePartitioning, DeclareQueueError> {
        self.declare_queue_declared(topic, group, partition_count, declaration)
            .await
            .map(|(partitioning, _)| partitioning)
    }

    /// `declare_queue`, also returning the queue's durability mode in effect.
    pub async fn declare_queue_declared(
        &self,
        topic: &str,
        group: Option<&str>,
        partition_count: u32,
        declaration: fibril_broker::queue_engine::QueueDeclaration,
    ) -> Result<(QueuePartitioning, fibril_broker::queue_engine::QueueDurability), DeclareQueueError>
    {
        if group.is_none() && self.stream_config(topic).is_some() {
            return Err(DeclareQueueError::Coordination(
                OpenraftAdapterError::Storage(format!(
                    "{topic} is already declared as a stream, cannot declare it as a queue"
                )),
            ));
        }
        // Refuse a count conflict before the declaration changes anything.
        let existing = self.queue_partitioning(topic, group);
        if let Some(existing) = existing.filter(|p| p.partition_count != partition_count.max(1)) {
            return Err(DeclareQueueError::PartitionCountConflict {
                topic: topic.to_string(),
                group: group.map(str::to_string),
                existing: existing.partition_count,
                requested: partition_count.max(1),
            });
        }
        let mode = self.store_queue_declaration(topic, group, declaration).await?;
        let partitioning = self
            .declare_queue_partitioning(topic, group, partition_count)
            .await?;
        Ok((partitioning, mode))
    }

    /// Owners open a queue once its partitions are catalogued, so the
    /// declaration, mode included, has to commit first. A declare that fails
    /// after this still leaves the mode fixed.
    async fn store_queue_declaration(
        &self,
        topic: &str,
        group: Option<&str>,
        declaration: fibril_broker::queue_engine::QueueDeclaration,
    ) -> Result<fibril_broker::queue_engine::QueueDurability, DeclareQueueError> {
        let meta = declaration.meta;
        let key = queue_declaration_key(topic, group);
        for _ in 0..8 {
            let current = self.cluster_attribute(&key);
            let mut stored: fibril_broker::queue_engine::QueueDeclaration = match &current {
                Some(raw) => serde_json::from_str(raw).map_err(|error| {
                    DeclareQueueError::Coordination(OpenraftAdapterError::Storage(format!(
                        "queue `{topic}`/{group:?} declaration is corrupt: {error}"
                    )))
                })?,
                None => Default::default(),
            };
            // A queue catalogued without a declaration predates modes and
            // has always been durable.
            let legacy = current.is_none() && self.queue_partitioning(topic, group).is_some();
            if legacy {
                stored.durability = Some(fibril_broker::queue_engine::QueueDurability::Durable);
            }
            match (current.is_some() || legacy, declaration.durability) {
                (false, requested) => {
                    stored.durability =
                        Some(requested.unwrap_or_else(|| self.default_queue_durability()));
                }
                (true, Some(requested)) if requested != stored.mode() => {
                    return Err(DeclareQueueError::DurabilityConflict {
                        topic: topic.to_string(),
                        group: group.map(str::to_string),
                        existing: stored.mode(),
                        requested,
                    });
                }
                (true, _) => {}
            }
            let merged = &mut stored.meta;
            if meta.dlq_policy.is_some() {
                merged.dlq_policy = meta.dlq_policy.clone();
            }
            if meta.dlq_max_retries.is_some() {
                merged.dlq_max_retries = meta.dlq_max_retries;
            }
            if meta.default_message_ttl_ms.is_some() {
                merged.default_message_ttl_ms = meta.default_message_ttl_ms;
            }
            let value = serde_json::to_string(&stored).map_err(|error| {
                DeclareQueueError::Coordination(OpenraftAdapterError::Storage(error.to_string()))
            })?;
            let mode = stored.mode();
            if current.as_ref() == Some(&value) {
                return Ok(mode);
            }
            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttribute {
                    key: key.clone(),
                    expected: current,
                    value,
                })
                .await
            {
                Ok(_) => return Ok(mode),
                Err(OpenraftAdapterError::AttributeMismatch { .. }) => continue,
                Err(error) => return Err(DeclareQueueError::Coordination(error)),
            }
        }
        Err(DeclareQueueError::Coordination(
            OpenraftAdapterError::Storage(
                "queue declaration lost the CAS race repeatedly".to_string(),
            ),
        ))
    }

    /// Record a stream's full config (create-once, race-safe via CAS), mirroring
    /// `declare_queue_partitioning` but on the stream-config key and carrying the
    /// durability tier + retention so any owner can open with the right config.
    /// Streams have no group, so the document is keyed by topic alone. A re-declare
    /// with the same partition count is idempotent; a different count conflicts.
    pub async fn declare_stream(
        &self,
        topic: &str,
        partition_count: u32,
        durability: u8,
        retention: StreamRetentionConfig,
        replication_factor: Option<u32>,
    ) -> Result<StreamConfig, DeclareQueueError> {
        let partition_count = partition_count.max(1);
        let key = stream_config_key(topic);
        for _ in 0..8 {
            if let Some(raw) = self.cluster_attribute(&key) {
                match serde_json::from_str::<StreamConfig>(&raw) {
                    Ok(existing) if existing.partition_count == partition_count => {
                        return Ok(existing);
                    }
                    Ok(existing) => {
                        return Err(DeclareQueueError::PartitionCountConflict {
                            topic: topic.to_string(),
                            group: None,
                            existing: existing.partition_count,
                            requested: partition_count,
                        });
                    }
                    Err(error) => {
                        return Err(DeclareQueueError::Coordination(
                            OpenraftAdapterError::Storage(format!(
                                "stream `{topic}` config is corrupt: {error}"
                            )),
                        ));
                    }
                }
            }
            let config = StreamConfig {
                partition_count,
                partitioning_version: DEFAULT_PARTITIONING_VERSION,
                durability,
                retention: retention.clone(),
                replication_factor,
            };
            let value = serde_json::to_string(&config).map_err(|error| {
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
                Ok(_) => return Ok(config),
                // Lost the create race: re-read and resolve (idempotent or conflict).
                Err(OpenraftAdapterError::AttributeMismatch { .. }) => continue,
                Err(error) => return Err(DeclareQueueError::Coordination(error)),
            }
        }
        Err(DeclareQueueError::Coordination(
            OpenraftAdapterError::Storage(
                "stream declare lost the CAS race repeatedly".to_string(),
            ),
        ))
    }

    /// Read a stream's committed config (partitioning + durability + retention),
    /// if declared. Owners read this to open their assigned partitions.
    pub fn stream_config(&self, topic: &str) -> Option<StreamConfig> {
        self.cluster_attribute(&stream_config_key(topic))
            .and_then(|raw| serde_json::from_str::<StreamConfig>(&raw).ok())
    }

    /// Grow a logical queue's partition count to `new_count` and bump its
    /// partitioning version. Race-safe via CAS on the current document.
    ///
    /// v1 grows by an INTEGER MULTIPLE only (`new_count` must be a strictly larger
    /// multiple of the current count, typically doubling). With modulo hashing
    /// that keeps the per-key ordering barrier a pure partition-identity gate
    /// (each new partition sources from exactly one old partition).
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
            if new_count <= existing.partition_count || new_count % existing.partition_count != 0 {
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
        // 1. Marker first, so new partitions are held the moment they appear. The
        //    adoption generation is unknown until the cutover, so it starts None.
        self.begin_repartition_transition(
            topic,
            group,
            &RepartitionTransitionDoc {
                version: next_version,
                n_old,
                n_new: new_count,
                adoption_generation: None,
            },
        )
        .await
        .map_err(RepartitionQueueError::Coordination)?;
        // 2. Register the new partitions so the controller places owners.
        for partition in n_old..new_count {
            let queue = QueueIdentity::new(topic, fibril_broker::Partition::new(partition), group);
            self.register_queue(&queue)
                .await
                .map_err(RepartitionQueueError::Coordination)?;
        }
        // 3. Bump the version: this is the routing cutover.
        let result = self.repartition_queue(topic, group, new_count).await?;
        // 4. Stamp the marker with the topology generation now in effect so the
        //    controller can fence the finalize on client adoption of the new
        //    routing. Done right after the cutover (not at marker creation) because
        //    the generation only exists once the version bump commits.
        self.stamp_repartition_adoption_generation(
            topic,
            group,
            &RepartitionTransitionDoc {
                version: next_version,
                n_old,
                n_new: new_count,
                adoption_generation: None,
            },
        )
        .await
        .map_err(RepartitionQueueError::Coordination)?;
        Ok(result)
    }

    /// Stamp a transition marker with the topology generation that now reflects the
    /// new partitioning, so the controller can fence the finalize on clients
    /// adopting it. Called right after the routing cutover (the generation only
    /// exists once the version bump commits, so it cannot be set at marker
    /// creation). Idempotent: re-stamping with the same fields is a no-op
    /// overwrite. A client that applies the post-cutover topology push acks a
    /// generation at least this high, so the adoption gate stays satisfiable even
    /// though pushes are content-gated and stop firing once the cutover settles.
    pub async fn stamp_repartition_adoption_generation(
        &self,
        topic: &str,
        group: Option<&str>,
        doc: &RepartitionTransitionDoc,
    ) -> Result<(), OpenraftAdapterError> {
        let adoption_generation = self.node.committed_snapshot().generation;
        self.begin_repartition_transition(
            topic,
            group,
            &RepartitionTransitionDoc {
                adoption_generation: Some(adoption_generation),
                ..doc.clone()
            },
        )
        .await
    }

    /// Operator entry point for a live shrink: `new_count` must be a factor of the
    /// current count (new_count divides it, and is smaller). Writes the transition
    /// marker first so surviving partitions are held before producers cut over,
    /// then bumps the version. Removed partitions stay registered so they keep
    /// draining (retirement/deregistration is deferred).
    ///
    /// NOTE: a stricter choreography would wait for owners to confirm survivors
    /// are held before the version bump. v1 relies on the watcher applying the
    /// hold within about one tick of the marker.
    pub async fn shrink_queue(
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
        if n_old == new_count {
            return Ok(current);
        }
        // Shrink only, and only to a factor of the current count.
        if new_count == 0 || new_count >= n_old || n_old % new_count != 0 {
            return Err(RepartitionQueueError::NotIntegerFactorShrink {
                topic: topic.to_string(),
                group: group.map(str::to_string),
                current: n_old,
                requested: new_count,
            });
        }
        let next_version = current.partitioning_version + 1;
        // 1. Marker first: surviving partitions are held before the cutover. The
        //    adoption generation is unknown until the cutover, so it starts None.
        self.begin_repartition_transition(
            topic,
            group,
            &RepartitionTransitionDoc {
                version: next_version,
                n_old,
                n_new: new_count,
                adoption_generation: None,
            },
        )
        .await
        .map_err(RepartitionQueueError::Coordination)?;
        // 2. Bump the version (routing cutover) via a CAS to the smaller count.
        let key = queue_partitioning_key(topic, group);
        let mut applied = None;
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
            if existing.partition_count == new_count {
                applied = Some(existing);
                break;
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
                Ok(_) => {
                    applied = Some(next);
                    break;
                }
                Err(OpenraftAdapterError::AttributeMismatch { .. }) => continue,
                Err(error) => return Err(RepartitionQueueError::Coordination(error)),
            }
        }
        let Some(applied) = applied else {
            return Err(RepartitionQueueError::Coordination(
                OpenraftAdapterError::Storage(
                    "queue shrink lost the CAS race repeatedly".to_string(),
                ),
            ));
        };
        // 3. Stamp the marker with the topology generation now in effect so the
        //    controller can fence the finalize on client adoption of the smaller
        //    routing (only known once the cutover above has committed).
        self.stamp_repartition_adoption_generation(
            topic,
            group,
            &RepartitionTransitionDoc {
                version: next_version,
                n_old,
                n_new: new_count,
                adoption_generation: None,
            },
        )
        .await
        .map_err(RepartitionQueueError::Coordination)?;
        Ok(applied)
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

    pub fn users_document(&self) -> Result<Option<ClusterUsers>, OpenraftAdapterError> {
        self.cluster_attribute(AUTH_USERS_ATTRIBUTE)
            .as_deref()
            .map(|raw| {
                serde_json::from_str::<ClusterUsers>(raw).map_err(|error| {
                    OpenraftAdapterError::Storage(format!(
                        "decode auth users cluster document: {error}"
                    ))
                })
            })
            .transpose()
    }

    /// Update the user document as the cluster truth. `expected_version` 0
    /// means no cluster document exists yet.
    pub async fn update_users(
        &self,
        expected_version: u64,
        document: &fibril_broker::auth_store::UserDocument,
    ) -> Result<ClusterUsersUpdateOutcome, OpenraftAdapterError> {
        for _ in 0..8 {
            let current_raw = self.cluster_attribute(AUTH_USERS_ATTRIBUTE);
            let current_document = current_raw
                .as_deref()
                .map(|raw| {
                    serde_json::from_str::<ClusterUsers>(raw).map_err(|error| {
                        OpenraftAdapterError::Storage(format!(
                            "decode auth users cluster document: {error}"
                        ))
                    })
                })
                .transpose()?;
            let current_version = current_document
                .as_ref()
                .map(|document| document.cluster_version)
                .unwrap_or(0);

            if current_version != expected_version {
                return Ok(ClusterUsersUpdateOutcome::Conflict(current_document));
            }

            let stored = ClusterUsers {
                cluster_version: expected_version + 1,
                document: document.clone(),
            };
            let value = serde_json::to_string(&stored)
                .map_err(|error| OpenraftAdapterError::Storage(error.to_string()))?;

            match self
                .forward_command(MetadataRaftCommand::CompareAndSetAttribute {
                    key: AUTH_USERS_ATTRIBUTE.to_string(),
                    expected: current_raw,
                    value,
                })
                .await
            {
                Ok(_) => return Ok(ClusterUsersUpdateOutcome::Stored(stored)),
                Err(OpenraftAdapterError::AttributeMismatch { .. }) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(OpenraftAdapterError::Storage(
            "auth users update lost the CAS race repeatedly".to_string(),
        ))
    }

    /// Adopt the cluster user document into the local store as it changes.
    /// If no cluster document exists yet and the local store holds seeded
    /// users, publish them once (first node wins, the rest adopt).
    pub fn spawn_user_store_sync(
        self: &std::sync::Arc<Self>,
        manager: std::sync::Arc<fibril_broker::auth_store::UserStoreManager>,
    ) -> tokio::task::JoinHandle<()> {
        let provider = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            if provider.users_document().ok().flatten().is_none() && manager.has_users() {
                let snapshot = manager.snapshot();
                let _ = provider.update_users(0, &snapshot.document).await;
            }

            let mut watch = provider.node.watch_committed();
            let mut last_applied_cluster_version = 0u64;
            let mut last_bad_raw: Option<String> = None;
            loop {
                let raw_document = watch
                    .borrow_and_update()
                    .attributes
                    .get(AUTH_USERS_ATTRIBUTE)
                    .cloned();
                let document = match raw_document.as_deref() {
                    Some(raw) => match serde_json::from_str::<ClusterUsers>(raw) {
                        Ok(document) => {
                            last_bad_raw = None;
                            Some(document)
                        }
                        Err(error) => {
                            if last_bad_raw.as_deref() != Some(raw) {
                                tracing::warn!(
                                    %error,
                                    "replicated auth users document is invalid"
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
                        match manager
                            .adopt_document(document.cluster_version, document.document)
                            .await
                        {
                            Ok(()) => last_applied_cluster_version = document.cluster_version,
                            Err(error) => {
                                tracing::warn!(%error, "failed to adopt replicated auth users");
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
    ) -> tokio::task::JoinHandle<()> {
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
    ) {
        let provider = std::sync::Arc::clone(self);
        let status = std::sync::Arc::new(std::sync::RwLock::new(ControllerStatus::default()));
        let shared_status = status.clone();
        // Streams place owner-only by the deterministic spread policy; there is
        // no pluggable stream policy yet, so the controller owns the instance.
        let stream_planner = DeterministicStreamPlacement;

        let handle = tokio::spawn(async move {
            let mut watch = provider.watch();
            let mut transport = provider.node.peer_transport_failures();
            // Wakes the loop as soon as this node wins an election. Otherwise a
            // new leader could sleep up to a full tick before acting on the
            // transport failures it already saw while electing.
            let mut leader = provider.node.watch_leader();
            let mut detector = eager_failover::Detector::default();
            let mut previous_suspects = std::collections::BTreeSet::new();
            let mut previous_attention = Vec::new();
            let mut previous_expired = std::collections::BTreeMap::<String, u64>::new();
            // When this node won leadership, and whether the startup grace
            // has ended for that term.
            let mut leader_since: Option<std::time::Instant> = None;
            let mut grace_over = false;
            loop {
                let queues = provider.registered_queues();
                let streams = provider.registered_streams();
                let mut live = provider.live_nodes(config.liveness_ttl);
                let policy = match provider.runtime_settings_document() {
                    Ok(Some(document)) if document.settings.validate().is_ok() => {
                        document.settings.replication
                    }
                    Ok(None) => fibril_broker::runtime_settings::ReplicationRuntimeSettings {
                        eager_failover: config.eager_failover,
                        eager_failover_grace_ms: config.eager_failover_grace_ms,
                        ..Default::default()
                    },
                    _ => Default::default(),
                };
                let active = provider.node.is_leader().await;
                if !active {
                    leader_since = None;
                    grace_over = false;
                } else if leader_since.is_none() {
                    leader_since = Some(std::time::Instant::now());
                }
                let waiting_for: Vec<String> = if active && !grace_over {
                    let missing = startup_grace_missing(&provider.node.committed_snapshot(), &live);
                    if missing.is_empty()
                        || leader_since.is_some_and(|since| since.elapsed() >= config.startup_grace)
                    {
                        grace_over = true;
                        if !missing.is_empty() {
                            tracing::warn!(?missing, "controller startup grace expired; owners may move off nodes without a fresh heartbeat");
                        }
                        Vec::new()
                    } else {
                        missing
                    }
                } else {
                    Vec::new()
                };
                // Only the leader reports, once per expiry and once per return.
                // Labels go stale while a whole cluster is down, so wait for the
                // startup grace before reporting, and never report this node.
                let mut expired = if active && grace_over {
                    provider.expired_heartbeats(config.liveness_ttl)
                } else {
                    std::collections::BTreeMap::new()
                };
                expired.remove(&provider.node_id);
                for (node, age_ms) in &expired {
                    if !previous_expired.contains_key(node) {
                        tracing::warn!(
                            node = %node,
                            since_last_heartbeat_ms = age_ms,
                            ttl_ms = config.liveness_ttl.as_millis() as u64,
                            "broker heartbeat expired, node is excluded from placement"
                        );
                    }
                }
                if active {
                    for node in previous_expired.keys().filter(|n| !expired.contains_key(*n)) {
                        if live.contains_key(node) {
                            tracing::info!(node = %node, "broker heartbeat resumed");
                        }
                    }
                }
                previous_expired = expired;
                provider.node.set_monitor_idle_peer_connections(
                    active && policy.eager_failover && policy.eager_failover_grace_ms == 0,
                );
                let suspects = detector.suspects(
                    active && policy.eager_failover,
                    policy.eager_failover_grace_ms,
                    &provider.node.committed_snapshot(),
                    &transport.borrow_and_update(),
                    provider.node.node_id(),
                    std::time::Instant::now(),
                );
                let current_suspects: std::collections::BTreeSet<_> =
                    suspects.iter().map(|s| s.node.clone()).collect();
                for suspect in &suspects {
                    live.remove(&suspect.node);
                    if !previous_suspects.contains(&suspect.node) {
                        tracing::warn!(
                            node = suspect.node,
                            raft_id = suspect.raft_id,
                            failure = suspect.failure,
                            elapsed_ms = suspect.elapsed_ms,
                            reconnect_failures = suspect.reconnect_failures,
                            "eager failover excludes suspected peer from placement; recovery proof still required"
                        );
                    }
                }
                previous_suspects = current_suspects;
                let outcome = if (queues.is_empty() && streams.is_empty()) || live.is_empty() {
                    // Nothing to place (or no live brokers): a normal idle
                    // tick, not an error.
                    Ok(provider.node.is_leader().await.then(|| provider.snapshot()))
                } else {
                    provider
                        .plan_and_commit(
                            planner.as_ref(),
                            &queues,
                            &stream_planner,
                            &streams,
                            config.target_followers,
                            config.stream_replication_factor,
                            config.default_durability,
                            &live,
                            config.max_cas_retries,
                            !waiting_for.is_empty(),
                        )
                        .await
                };

                if let Ok(mut status) = status.write() {
                    status.eager_suspects = suspects;
                    status.startup_grace_waiting_for = waiting_for;
                    match outcome {
                        Ok(Some(snapshot)) => {
                            status.active = true;
                            status.last_plan_generation = Some(snapshot.generation);
                            status.last_error = None;
                            match provider.pending_recoveries() {
                                Ok(requests) => status.pending_recoveries = requests,
                                Err(error) => status.last_error = Some(error.to_string()),
                            }
                            let attention = operator_override::recovery_attention(
                                &provider.node.committed_snapshot(),
                                |node| live.contains_key(node),
                            );
                            for item in attention.iter().filter(|item| !previous_attention.contains(*item)) {
                                tracing::warn!(topic = item.resource.name, partition = item.resource.partition,
                                    namespace = item.resource.namespace, action = item.action, "{}", item.reason);
                            }
                            previous_attention = attention.clone();
                            status.attention = attention;
                        }
                        Ok(None) => status.active = false,
                        Err(error) => {
                            status.active = true;
                            status.last_error = Some(error.to_string());
                        }
                    }
                }

                tokio::select! {
                    changed = leader.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                    changed = transport.changed(), if active && policy.eager_failover => {
                        if changed.is_err() {
                            return;
                        }
                    }
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
    {
        let provider = std::sync::Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let local = list_local_queues().await;
                if !local.is_empty() {
                    let known: std::collections::HashSet<QueueIdentity> =
                        provider.registered_queues().into_iter().collect();
                    for queue in local {
                        // A topic declared as a stream must never enter the
                        // queue catalogue, whatever the local listing said -
                        // a queue assignment for a stream partition sends
                        // queue follower workers at a stream.
                        if queue.group.is_none() && provider.stream_config(&queue.topic).is_some() {
                            continue;
                        }
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
    ) -> tokio::task::JoinHandle<()> {
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
    /// Registered nodes' raft ids, from the advisory heartbeat label. Nodes
    /// that never carried the label are absent.
    pub fn node_raft_ids(&self) -> HashMap<String, u64> {
        self.node
            .committed_snapshot()
            .nodes
            .values()
            .filter_map(|node| {
                let raft_id = node.labels.get(RAFT_ID_LABEL)?.parse().ok()?;
                Some((node.node_id.clone(), raft_id))
            })
            .collect()
    }

    /// Registered nodes' runtime facts from the advisory heartbeat labels,
    /// as loose JSON ready for the topology payload. Nodes that never
    /// carried the version label are absent. Visibility is "public" or
    /// "private", and the session counts read 0 when their label is absent.
    pub fn node_runtime_meta(&self) -> HashMap<String, serde_json::Value> {
        self.node
            .committed_snapshot()
            .nodes
            .values()
            .filter_map(|node| {
                let version = node.labels.get(VERSION_LABEL)?;
                let mut meta = serde_json::json!({ "version": version });
                if let Some(started) = node
                    .labels
                    .get(STARTED_AT_LABEL)
                    .and_then(|raw| raw.parse::<u64>().ok())
                {
                    meta["started_at_ms"] = started.into();
                }
                if let Some(tls) = node.labels.get(TLS_LABEL) {
                    meta["tls"] = serde_json::Value::String(tls.clone());
                }
                if let Some(expiry) = node
                    .labels
                    .get(CERT_EXPIRY_LABEL)
                    .and_then(|raw| raw.parse::<i64>().ok())
                {
                    meta["cert_not_after_unix"] = expiry.into();
                }
                let visibility = if gateway::node_is_private(node) {
                    PRIVATE_VISIBILITY
                } else {
                    "public"
                };
                meta["visibility"] = visibility.into();
                let count = |label: &str| {
                    node.labels
                        .get(label)
                        .and_then(|raw| raw.parse::<u64>().ok())
                        .unwrap_or(0)
                };
                meta["upstream_sessions"] = count(UPSTREAM_SESSIONS_LABEL).into();
                meta["forwarded_sessions"] = count(FORWARDED_SESSIONS_LABEL).into();
                Some((node.node_id.clone(), meta))
            })
            .collect()
    }

    /// Registered nodes' coarse throughput as (published/s, delivered/s),
    /// from the advisory heartbeat labels. Nodes that never carried the
    /// publish-rate label are absent.
    pub fn node_rates(&self) -> HashMap<String, (u64, u64)> {
        self.node
            .committed_snapshot()
            .nodes
            .values()
            .filter_map(|node| {
                let published = node.labels.get(RATE_PUB_LABEL)?.parse().ok()?;
                let delivered = node
                    .labels
                    .get(RATE_DLV_LABEL)
                    .and_then(|raw| raw.parse().ok())
                    .unwrap_or(0);
                Some((node.node_id.clone(), (published, delivered)))
            })
            .collect()
    }

    /// Nodes whose last heartbeat is older than `ttl`, mapped to the heartbeat
    /// age in milliseconds. Nodes without a heartbeat label never expire,
    /// matching `live_nodes`.
    pub fn expired_heartbeats(&self, ttl: std::time::Duration) -> std::collections::BTreeMap<String, u64> {
        let now = unix_millis_now();
        let ttl_ms = ttl.as_millis() as u64;
        self.node
            .committed_snapshot()
            .nodes
            .values()
            .filter_map(|node| {
                let beat = node.labels.get(HEARTBEAT_LABEL)?.parse::<u64>().ok()?;
                let age = now.saturating_sub(beat);
                (age > ttl_ms).then(|| (node.node_id.clone(), age))
            })
            .collect()
    }

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
            .map(|node| {
                let broker_addr = node.endpoint.clone();
                (
                    node.node_id.clone(),
                    NodeInfo {
                        node_id: node.node_id.clone(),
                        broker_addr,
                        admin_addr: node
                            .admin_endpoint
                            .as_ref()
                            .and_then(|endpoint| endpoint.parse().ok()),
                    },
                )
            })
            .collect()
    }

    /// Client-facing topology: the owner and reachable endpoint of every
    /// assigned queue partition in the committed snapshot. Clients route to
    /// owners from this and refresh on not-owner / stale-topology errors. The
    /// partitioning version is carried per queue for forward-compatibility with
    /// live repartitioning (constant today).
    ///
    /// Every catalogued partition is listed, also one that is not served yet
    /// (no placement, or its history not active), with no owner and no
    /// endpoints. Clients read a topic missing from a populated topology as
    /// deleted, so leaving such a partition out would end subscriptions to a
    /// queue that still exists.
    pub fn client_topology(&self) -> ClientTopology {
        let committed = self.node.committed_snapshot();
        // Per-node advertise list: the full priority-ordered list from the node's
        // advertise label when present, else the single registered endpoint.
        let endpoints: HashMap<String, Vec<String>> = committed
            .nodes
            .values()
            .map(|node| (node.node_id.clone(), node_advertise_endpoints(node)))
            .collect();
        let private: std::collections::HashSet<String> = committed
            .nodes
            .values()
            .filter(|node| gateway::node_is_private(node))
            .map(|node| node.node_id.clone())
            .collect();
        let gateways = if private.is_empty() {
            Vec::new()
        } else {
            gateway::gateway_candidates(
                committed.nodes.values(),
                unix_millis_now(),
                self.gateway_liveness_ttl_ms(),
                node_advertise_endpoints,
            )
        };
        // A private owner's partition is served through the gateway the
        // rendezvous choice names. Every route clients see goes through here
        // or through the targeted route, which applies the same rule.
        let client_endpoints = |owner: &str, topic: &str, group: Option<&str>, partition: u32| {
            if private.contains(owner) {
                let chosen = gateway::rendezvous_gateway(&gateways, topic, group, partition)
                    .map(|gateway| gateway.endpoints.clone())
                    .unwrap_or_default();
                (chosen, true)
            } else {
                (endpoints.get(owner).cloned().unwrap_or_default(), false)
            }
        };
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
                let (owner_endpoints, via_gateway) = client_endpoints(
                    &assignment.owner,
                    &assignment.queue.topic,
                    group.as_deref(),
                    assignment.queue.partition.id(),
                );
                ClientQueueTopology {
                    topic: assignment.queue.topic.to_string(),
                    partition: assignment.queue.partition,
                    group,
                    owner_node_id: assignment.owner.clone(),
                    owner_endpoints,
                    via_gateway,
                    partitioning_version: part.partitioning_version,
                    partition_count: part.partition_count,
                }
            })
            .collect();
        for queue in committed.resources.iter().filter_map(to_fibril_queue) {
            if fibril.assignments.contains_key(&queue) {
                continue;
            }
            let group = queue.group.as_ref().map(ToString::to_string);
            let part = partitioning(&queue.topic, group.as_deref());
            queues.push(ClientQueueTopology {
                topic: queue.topic.to_string(),
                partition: queue.partition,
                group,
                owner_node_id: String::new(),
                owner_endpoints: Vec::new(),
                via_gateway: false,
                partitioning_version: part.partitioning_version,
                partition_count: part.partition_count,
            });
        }
        queues.sort_by(|a, b| {
            a.topic
                .cmp(&b.topic)
                .then_with(|| a.group.cmp(&b.group))
                .then_with(|| a.partition.cmp(&b.partition))
        });

        // Authoritative stream partitioning (count + version) per topic, read from
        // the committed stream config.
        let stream_partitioning = |topic: &str| -> (u32, u64) {
            committed
                .attributes
                .get(&stream_config_key(topic))
                .and_then(|raw| serde_json::from_str::<StreamConfig>(raw).ok())
                .map(|config| (config.partition_count, config.partitioning_version))
                .unwrap_or((1, DEFAULT_PARTITIONING_VERSION))
        };
        let mut streams: Vec<ClientStreamTopology> = fibril
            .stream_assignments
            .values()
            .map(|assignment| {
                let (partition_count, partitioning_version) =
                    stream_partitioning(&assignment.stream.topic);
                let (owner_endpoints, via_gateway) = client_endpoints(
                    &assignment.owner,
                    &assignment.stream.topic,
                    None,
                    assignment.stream.partition.id(),
                );
                ClientStreamTopology {
                    topic: assignment.stream.topic.to_string(),
                    partition: assignment.stream.partition,
                    owner_node_id: assignment.owner.clone(),
                    owner_endpoints,
                    via_gateway,
                    partitioning_version,
                    partition_count,
                }
            })
            .collect();
        for stream in committed.resources.iter().filter_map(to_fibril_stream) {
            if fibril.stream_assignments.contains_key(&stream) {
                continue;
            }
            let (partition_count, partitioning_version) = stream_partitioning(&stream.topic);
            streams.push(ClientStreamTopology {
                topic: stream.topic.to_string(),
                partition: stream.partition,
                owner_node_id: String::new(),
                owner_endpoints: Vec::new(),
                via_gateway: false,
                partitioning_version,
                partition_count,
            });
        }
        streams.sort_by(|a, b| {
            a.topic
                .cmp(&b.topic)
                .then_with(|| a.partition.cmp(&b.partition))
        });

        ClientTopology {
            generation,
            queues,
            streams,
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
    /// Implements the controller-loop shape (see the coordination internals dev
    /// note, https://fibril.sh/development/coordination-internals/):
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
        stream_planner: &dyn StreamPlacementPolicy,
        streams: &[StreamIdentity],
        target_followers: usize,
        stream_replication_factor: usize,
        default_durability: ReplicationDurabilityPolicy,
        live_nodes: &HashMap<String, NodeInfo>,
        max_retries: usize,
    ) -> Result<Option<CoordinationSnapshot>, ControlError> {
        self.plan_and_commit(
            planner,
            queues,
            stream_planner,
            streams,
            target_followers,
            stream_replication_factor,
            default_durability,
            live_nodes,
            max_retries,
            false,
        )
        .await
    }

    /// `control_iteration` during the controller startup grace: every
    /// existing assignment keeps its owner and current followers, and only
    /// gains followers for empty slots. New resources are placed normally.
    pub async fn control_iteration_in_startup_grace(
        &self,
        planner: &dyn PartitionPlacementPolicy,
        queues: &[QueueIdentity],
        stream_planner: &dyn StreamPlacementPolicy,
        streams: &[StreamIdentity],
        target_followers: usize,
        stream_replication_factor: usize,
        default_durability: ReplicationDurabilityPolicy,
        live_nodes: &HashMap<String, NodeInfo>,
        max_retries: usize,
    ) -> Result<Option<CoordinationSnapshot>, ControlError> {
        self.plan_and_commit(
            planner,
            queues,
            stream_planner,
            streams,
            target_followers,
            stream_replication_factor,
            default_durability,
            live_nodes,
            max_retries,
            true,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn plan_and_commit(
        &self,
        planner: &dyn PartitionPlacementPolicy,
        queues: &[QueueIdentity],
        stream_planner: &dyn StreamPlacementPolicy,
        streams: &[StreamIdentity],
        target_followers: usize,
        stream_replication_factor: usize,
        default_durability: ReplicationDurabilityPolicy,
        live_nodes: &HashMap<String, NodeInfo>,
        max_retries: usize,
        hold_owners: bool,
    ) -> Result<Option<CoordinationSnapshot>, ControlError> {
        if !self.node.is_leader().await {
            return Ok(None);
        }

        let mut attempts = 0;
        loop {
            let committed = self.node.committed_snapshot();
            let fibril_committed = project_snapshot(&committed, false);

            // Draining nodes leave the placement inputs entirely: nothing new
            // lands on them (which also keeps a concurrent repartition from
            // deadlocking against the drain), and the planner treats their
            // partitions like a dead owner's. The candidate pass below then
            // either evacuates each one to a caught-up follower or, unlike
            // death, reverts it to stay put, because the node is still
            // serving.
            let draining: std::collections::HashSet<String> = committed
                .nodes
                .values()
                .filter(|node| node.labels.contains_key(DRAINING_LABEL))
                .map(|node| node.node_id.clone())
                .filter(|node_id| live_nodes.contains_key(node_id))
                .collect();
            let placement_nodes: HashMap<String, NodeInfo> = live_nodes
                .iter()
                .filter(|(node_id, _)| !draining.contains(*node_id))
                .map(|(node_id, info)| (node_id.clone(), info.clone()))
                .collect();
            // With every placeable node draining there is nothing useful to
            // plan; assignments stay put until a node finishes draining or a
            // non-draining node appears.
            if placement_nodes.is_empty() {
                return Ok(Some(to_fibril_snapshot(&committed)));
            }

            let mut plan = planner
                .plan(PartitionPlacementInput {
                    nodes: placement_nodes.clone(),
                    queues: queues.to_vec(),
                    existing: fibril_committed.assignments,
                    target_followers,
                    default_durability,
                    generation: committed.generation + 1,
                })
                .map_err(ControlError::Planning)?;

            // Streams place through the same controller pass. Their assignments
            // ride into the same ganglion snapshot so epoch stamping, anti-churn,
            // and the guarded write all cover them uniformly. Only the durable
            // tier replicates: it gets `target_followers` replicas; the cheaper
            // tiers stay owner-only (0 followers).
            let stream_target_followers: HashMap<String, usize> = streams
                .iter()
                .map(|stream| stream.topic.to_string())
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .map(|topic| {
                    let followers = match self.stream_config(&topic) {
                        Some(config) if config.durability == STREAM_DURABILITY_DURABLE => {
                            // Per-stream override beats the cluster default.
                            config
                                .replication_factor
                                .map(|rf| rf as usize)
                                .unwrap_or(stream_replication_factor)
                        }
                        _ => 0,
                    };
                    (topic, followers)
                })
                .collect();
            plan.snapshot.stream_assignments = stream_planner
                .plan(StreamPlacementInput {
                    nodes: placement_nodes.clone(),
                    streams: streams.to_vec(),
                    existing: fibril_committed.stream_assignments,
                    target_followers: stream_target_followers.clone(),
                    generation: committed.generation + 1,
                })
                .map_err(ControlError::Planning)?
                .assignments;

            let mut desired = to_ganglion_snapshot(&plan.snapshot);

            // Failover candidate selection: when the committed owner is gone
            // (dead, or live but draining) and the planner moved ownership,
            // hand the partition to a live committed follower, which already
            // holds a copy, instead of the planner's data-blind pick. This is
            // advisory: recovery proof, not this choice, decides safety.
            let failover_target = |resource: &ganglion_core::ResourceIdentity,
                                   planned: &ganglion_core::PartitionAssignment| {
                committed.assignments.get(resource).is_some_and(|current| {
                    (!live_nodes.contains_key(&current.owner) || draining.contains(&current.owner))
                        && planned.owner != current.owner
                })
            };
            // Partitions each node owns outside this failover pass. Choices
            // below add to it, so equally ranked followers share the load
            // instead of one node taking every partition of a failed peer.
            let mut owned: HashMap<String, usize> = HashMap::new();
            for (resource, planned) in &desired.assignments {
                if !failover_target(resource, planned) {
                    *owned.entry(planned.owner.clone()).or_default() += 1;
                }
            }
            for (resource, planned) in desired.assignments.iter_mut() {
                if !failover_target(resource, planned) {
                    continue;
                }
                let current = &committed.assignments[resource];
                let owner_draining = draining.contains(&current.owner);
                // The applied-tail label is keyed by QueueIdentity. A stream
                // follower reuses the queue follower worker (group None), so it
                // already publishes its tail under that same key - derive it the
                // same way for the stream resource.
                let (label, stream) = if let Some(queue) = to_fibril_queue(resource) {
                    (applied_tail_label(&queue), false)
                } else if let Some(stream) = to_fibril_stream(resource) {
                    (
                        applied_tail_label(&QueueIdentity::new(stream.topic, stream.partition, None)),
                        true,
                    )
                } else {
                    continue;
                };
                let best = choose_failover_owner(current, &committed, &label, stream, &owned, |node| {
                    live_nodes.contains_key(node) && !draining.contains(node)
                });
                match best {
                    Some(best_follower) => {
                        let displaced =
                            std::mem::replace(&mut planned.owner, best_follower.clone());
                        planned.followers.retain(|follower| *follower != best_follower);
                        let mut fresh_free = planned.clone();
                        fresh_free
                            .followers
                            .retain(|follower| current.followers.contains(follower));
                        let keeps_writes = promotion::write_requirement(&fresh_free).ok()
                            >= promotion::write_requirement(current).ok();
                        if keeps_writes {
                            // Recovery installs every proposed member before
                            // the new owner serves, so a node without a copy,
                            // the planner's pick or its fill, would put a full
                            // copy on the critical path. Copy holders alone
                            // still meet the write requirement, so normal
                            // planning adds the rest after activation.
                            *planned = fresh_free;
                            if displaced != best_follower
                                && current.followers.contains(&displaced)
                                && !planned.followers.contains(&displaced)
                            {
                                planned.followers.push(displaced);
                            }
                        } else if displaced != best_follower
                            && live_nodes.contains_key(&displaced)
                            && !planned.followers.contains(&displaced)
                        {
                            // Too few copy holders survive to keep the write
                            // requirement, which promotion never lowers on
                            // liveness alone. A fresh node has to join now.
                            planned.followers.push(displaced);
                        }
                    }
                    // A draining owner with no live follower keeps the
                    // partition: the node is still serving, so staying put
                    // beats handing ownership to a node without a copy.
                    // Reactive failover stays the backstop once the process
                    // exits. A dead owner keeps the planner's pick.
                    None if owner_draining => {
                        planned.owner = current.owner.clone();
                        planned.followers = current.followers.clone();
                    }
                    None => {}
                }
                *owned.entry(planned.owner.clone()).or_default() += 1;
            }

            if hold_owners {
                hold_existing_placements(&committed, &mut desired, |resource| {
                    match to_fibril_stream(resource) {
                        Some(stream) => stream_target_followers
                            .get(stream.topic.as_ref() as &str)
                            .copied()
                            .unwrap_or(0),
                        None => target_followers,
                    }
                });
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

            let held = promotion::retain_unproven_assignments(&committed, &mut desired)
                .map_err(|error| ControlError::Consensus(OpenraftAdapterError::Storage(error)))?;
            operator_override::clear_settled_owner_losses(&mut desired, |node| {
                live_nodes.contains_key(node) && !draining.contains(node)
            });
            recovery_candidate::replace_unavailable(&mut desired, |node| {
                live_nodes.contains_key(node) && !draining.contains(node)
            })
            .map_err(|error| ControlError::Consensus(OpenraftAdapterError::Storage(error)))?;
            if held > 0 && desired.attributes != committed.attributes {
                tracing::warn!(held, "replica assignment changes await confirmed-history recovery; previous sources retained");
            }

            // Anti-churn: when the plan changes nothing, do not write at all.
            // Keeps idle controller ticks off the raft log entirely.
            if desired.assignments == committed.assignments
                && desired.attributes == committed.attributes
            {
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
    pub fn consensus_node(&self) -> &RaftMetadataNode {
        &self.node
    }

    /// Persisted blocked transitions, including the old replica configuration.
    /// Reading these requests does not authorize promotion or data replacement.
    pub fn pending_recoveries(
        &self,
    ) -> Result<Vec<promotion::PendingRecovery>, OpenraftAdapterError> {
        self.node.read_committed(|snapshot| {
            snapshot
                .attributes
                .iter()
                .filter(|(key, _)| key.starts_with(promotion::PENDING_RECOVERY_PREFIX))
                .map(|(_, raw)| {
                    serde_json::from_str(raw).map_err(|e| {
                        OpenraftAdapterError::Storage(format!("decode pending recovery: {e}"))
                    })
                })
                .collect()
        })
    }
}

impl Drop for GanglionCoordination {
    fn drop(&mut self) {
        self.forwarder.abort();
    }
}

/// Queue-ownership gate view: in cluster mode brokers serve only queues the
/// committed snapshot assigns to them.
impl fibril_broker::broker::QueueOwnership for GanglionCoordination {
    fn request_queue_reseed<'a>(
        &'a self,
        topic: &'a str,
        partition: Partition,
        group: Option<&'a str>,
        epoch: u64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let resource = ganglion_core::ResourceIdentity::new(
                QUEUE_NAMESPACE,
                topic,
                u64::from(partition.id()),
                group.map(str::to_owned),
            );
            if self
                .node
                .committed_snapshot()
                .assignments
                .get(&resource)
                .is_none_or(|a| a.epoch != epoch)
            {
                return Err("reseed assignment epoch changed".into());
            }
            self.request_queue_reseed(&resource, &self.node_id)
                .await
                .map_err(|e| e.to_string())
        })
    }
    fn replication_node_id(&self) -> Option<&str> {
        Some(&self.node_id)
    }

    fn permits_legacy_replication(&self, topic: &str, partition: Partition, group: Option<&str>) -> bool {
        let group = group.filter(|group| !group.is_empty() && *group != "default");
        self.node.read_committed(|snapshot| {
            let queue = ganglion_core::ResourceIdentity::new(QUEUE_NAMESPACE, topic, u64::from(partition.id()), group.map(str::to_owned));
            let stream = ganglion_core::ResourceIdentity::new(STREAM_NAMESPACE, topic, u64::from(partition.id()), None::<String>);
            history_identity::permits_legacy_serving(snapshot, &queue)
                && (group.is_some() || history_identity::permits_legacy_serving(snapshot, &stream))
        })
    }

    fn authorize_history_replication(&self, session: &fibril_broker::history_replication::HistoryReplicationSession,
        owner_is_receiver: bool) -> Result<(), String> {
        self.validate_history_replication(session, owner_is_receiver)
            .or_else(|_| self.validate_learner_read(session, owner_is_receiver))
            .or_else(|_| self.validate_reseed_read(session, owner_is_receiver))
    }

    fn authorize_initial_history<'a>(
        &'a self,
        command: &'a fibril_broker::initial_history::InitialHistoryPrepareCommand,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<fibril_broker::initial_history::InitialHistoryAuthorization, String>> + Send + 'a>> {
        Box::pin(self.authorize_initial_history_command(command))
    }

    fn authorize_recovery_transfer<'a>(&'a self, command: &'a fibril_broker::recovery_transfer::QueueRecoveryCommand) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<fibril_broker::recovery_transfer::QueueRecoveryAuthorization, String>> + Send + 'a>> {
        Box::pin(self.authorize_transfer_command(command))
    }
    fn record_recovery_installation<'a>(&'a self, command: &'a fibril_broker::recovery_transfer::QueueRecoveryCommand, receipt: &'a fibril_broker::recovery_transfer::QueueRecoveryLocalReceipt) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(self.record_transfer_receipt(command,receipt))
    }
    fn authorize_recovery_admission<'a>(&'a self, command: &'a fibril_broker::recovery_transfer::QueueRecoveryCommand) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<fibril_broker::queue_engine::PreparedQueueRecovery, String>> + Send + 'a>> {
        Box::pin(self.authorize_recovery_admission_command(command))
    }

    fn authorize_recovery_seal<'a>(
        &'a self,
        command: &'a fibril_broker::recovery::RecoverySealCommand,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, String>> + Send + 'a>>
    {
        Box::pin(async move {
            let (key, raw) = self.node.read_committed(|snapshot| {
                promotion::validate_seal_command(snapshot, &self.node_id, command)
            })?;
            // A same-value CAS goes through consensus without changing metadata
            // generation. Its committed response supplies a fresh authority view,
            // including when this broker must forward to the current leader.
            let response = self
                .forward_command(MetadataRaftCommand::CompareAndSetAttribute {
                    key,
                    expected: Some(raw.clone()),
                    value: raw,
                })
                .await
                .map_err(|err| err.to_string())?;
            promotion::validate_seal_command(&response.snapshot, &self.node_id, command)?;
            Ok(self.node_id.clone())
        })
    }

    fn owns_queue(
        &self,
        topic: &str,
        partition: fibril_broker::Partition,
        group: Option<&str>,
    ) -> bool {
        Coordination::owns_queue(self, topic, partition, group)
    }

    fn queue_declaration(
        &self,
        topic: &str,
        group: Option<&str>,
    ) -> Result<Option<fibril_broker::queue_engine::QueueDeclaration>, String> {
        let key = queue_declaration_key(topic, group);
        let raw = self
            .node
            .read_committed(|snapshot| snapshot.attributes.get(&key).cloned());
        raw.map(|raw| {
            serde_json::from_str(&raw).map_err(|error| {
                format!("queue `{topic}`/{group:?} declaration is corrupt: {error}")
            })
        })
        .transpose()
    }
}

/// Stream-ownership gate view: in cluster mode a broker serves only the stream
/// partitions the committed snapshot assigns to it, and reads declared config
/// from coordination so it can open a partition it did not declare.
impl fibril_broker::broker::StreamOwnership for GanglionCoordination {
    fn owns_stream(&self, topic: &str, partition: fibril_broker::Partition) -> bool {
        self.tx
            .borrow()
            .stream_assignment_for(topic, partition)
            .is_some_and(|assignment| assignment.is_owned_by(&self.node_id))
    }

    fn stream_open_config(&self, topic: &str) -> Option<fibril_broker::broker::StreamOpenConfig> {
        let config = self.stream_config(topic)?;
        let retention = if config.retention.max_age_ms.is_none()
            && config.retention.max_bytes.is_none()
            && config.retention.max_records.is_none()
        {
            None
        } else {
            Some(fibril_broker::queue_engine::RetentionConfig {
                max_age_ms: config.retention.max_age_ms,
                max_bytes: config.retention.max_bytes,
                max_records: config.retention.max_records,
            })
        };
        Some(fibril_broker::broker::StreamOpenConfig {
            durability: fibril_broker::stream::StreamDurability::from_u8(config.durability)
                .unwrap_or_default(),
            retention,
        })
    }

    fn topic_has_queue_state(&self, topic: &str) -> Result<bool, String> {
        let in_fibril_view = self
            .tx
            .borrow()
            .assignments
            .keys()
            .any(|queue| queue.topic.as_str() == topic);
        Ok(in_fibril_view
            || self
                .node
                .read_committed(|snapshot| committed_topic_has_queue_state(snapshot, topic)))
    }
}

/// Whether committed coordination state names `topic` as a queue under any
/// group: a catalogue resource, an assignment, partitioning or a declaration.
fn committed_topic_has_queue_state(
    snapshot: &ganglion_core::CoordinationSnapshot,
    topic: &str,
) -> bool {
    let has_prefix = |prefix: &str| {
        snapshot
            .attributes
            .range::<str, _>((
                std::ops::Bound::Included(prefix),
                std::ops::Bound::Unbounded,
            ))
            .next()
            .is_some_and(|(key, _)| key.starts_with(prefix))
    };
    let queue_resource = |resource: &ganglion_core::ResourceIdentity| {
        resource.namespace == QUEUE_NAMESPACE && resource.name == topic
    };
    // Declaration keys are JSON (topic, group) tuples, so the encoded topic
    // followed by a comma matches every group of this topic and no other topic.
    let declaration_prefix = format!(
        "fibril/queue_declaration/[{},",
        serde_json::to_string(topic).unwrap_or_default()
    );
    snapshot.resources.iter().any(queue_resource)
        || snapshot.assignments.keys().any(queue_resource)
        || snapshot
            .attributes
            .contains_key(&queue_partitioning_key(topic, None))
        || has_prefix(&format!("{QUEUE_PARTITIONING_ATTRIBUTE_PREFIX}{topic}/"))
        || has_prefix(&declaration_prefix)
}

impl Coordination for GanglionCoordination {
    fn node_id(&self) -> &str {
        &self.node_id
    }

    fn queue_durability(
        &self,
        topic: &str,
        group: Option<&str>,
    ) -> Option<fibril_broker::queue_engine::QueueDurability> {
        use fibril_broker::broker::QueueOwnership;
        self.queue_declaration(topic, group)
            .ok()
            .flatten()
            .map(|declaration| declaration.mode())
    }

    fn snapshot(&self) -> CoordinationSnapshot {
        self.tx.borrow().clone()
    }

    fn watch(&self) -> CoordinationStream {
        self.tx.subscribe()
    }
}

#[cfg(test)]
mod advertise_tests {
    use super::*;

    fn node_with_label(endpoint: &str, advertise: Option<&str>) -> ganglion_core::NodeInfo {
        let mut node = ganglion_core::NodeInfo::new("n1".to_string(), endpoint.to_string(), None);
        if let Some(raw) = advertise {
            node.labels
                .insert(ADVERTISE_LABEL.to_string(), raw.to_string());
        }
        node
    }

    #[test]
    fn advertise_label_roundtrips() {
        let addrs = vec![
            "broker-1:9876".to_string(),
            "public.example:9876".to_string(),
        ];
        assert_eq!(decode_advertise(&encode_advertise(&addrs)), addrs);
        // Malformed or empty -> empty, never a panic (advisory data).
        assert!(decode_advertise("not json").is_empty());
    }

    #[test]
    fn node_advertise_uses_full_label_list_in_order() {
        let node = node_with_label(
            "10.0.0.1:9876",
            Some(r#"["broker-1:9876","public.example:9876"]"#),
        );
        assert_eq!(
            node_advertise_endpoints(&node),
            vec![
                "broker-1:9876".to_string(),
                "public.example:9876".to_string()
            ]
        );
    }

    #[test]
    fn node_advertise_falls_back_to_registered_endpoint() {
        // No label -> the single registered endpoint.
        assert_eq!(
            node_advertise_endpoints(&node_with_label("10.0.0.1:9876", None)),
            vec!["10.0.0.1:9876".to_string()]
        );
        // Empty label list -> fall back too.
        assert_eq!(
            node_advertise_endpoints(&node_with_label("10.0.0.1:9876", Some("[]"))),
            vec!["10.0.0.1:9876".to_string()]
        );
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
        assert_eq!(
            decode_cohort_assignment_doc(&encoded).unwrap().generation,
            7
        );
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
            adoption_generation: None,
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
    fn aggregate_topology_adoption_takes_cluster_minimum() {
        // No labels -> no adoption signal.
        assert_eq!(aggregate_topology_adoption(std::iter::empty()), None);
        // The minimum across reporting nodes is the cluster adoption floor.
        assert_eq!(aggregate_topology_adoption(["7", "5", "9"]), Some(5));
        // Unparseable labels are ignored, not treated as zero.
        assert_eq!(aggregate_topology_adoption(["", "8", "oops"]), Some(8));
    }

    #[test]
    fn live_topology_adoption_excludes_dead_nodes() {
        let now = 100_000u64;
        let ttl = 5_000u64;
        // A fresh node acked 9; a dead node's frozen label says 3. The dead node
        // must not pin the cluster minimum, so the live minimum is 9.
        let fresh = (Some("99000"), Some("9"));
        let dead = (Some("1000"), Some("3"));
        assert_eq!(live_topology_adoption([fresh, dead], now, ttl), Some(9));

        // Two live nodes: the minimum across them wins.
        let fresh_low = (Some("98000"), Some("4"));
        assert_eq!(
            live_topology_adoption([fresh, fresh_low], now, ttl),
            Some(4)
        );

        // A node with no heartbeat label counts as live (static/manual entry).
        let static_node = (None, Some("2"));
        assert_eq!(
            live_topology_adoption([fresh, static_node], now, ttl),
            Some(2)
        );

        // Only a dead node reports -> no live adoption signal.
        assert_eq!(live_topology_adoption([dead], now, ttl), None);

        // No nodes / no adoption labels -> no signal.
        assert_eq!(
            live_topology_adoption([(Some("99000"), None)], now, ttl),
            None
        );
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

    #[test]
    fn topic_queue_state_covers_every_group_and_no_other_topic() {
        let mut snapshot = ganglion_core::CoordinationSnapshot::default();
        let attr = |snapshot: &mut ganglion_core::CoordinationSnapshot, key: String| {
            snapshot.attributes.insert(key, String::new());
        };
        attr(&mut snapshot, stream_config_key("events"));
        attr(&mut snapshot, queue_declaration_key("events2", None));
        attr(&mut snapshot, queue_declaration_key("other", Some("events")));
        attr(&mut snapshot, queue_partitioning_key("events2", Some("g")));
        attr(&mut snapshot, queue_partitioning_key("event", None));
        snapshot.resources.insert(ganglion_core::ResourceIdentity::new(STREAM_NAMESPACE, "events", 0, None));
        assert!(!committed_topic_has_queue_state(&snapshot, "events"));

        for key in [
            queue_declaration_key("events", None),
            queue_declaration_key("events", Some("g")),
            queue_partitioning_key("events", None),
            queue_partitioning_key("events", Some("g")),
        ] {
            let mut with_queue = snapshot.clone();
            attr(&mut with_queue, key.clone());
            assert!(committed_topic_has_queue_state(&with_queue, "events"), "{key}");
        }
        let queue = ganglion_core::ResourceIdentity::new(QUEUE_NAMESPACE, "events", 1, Some("g".into()));
        let mut catalogued = snapshot.clone();
        catalogued.resources.insert(queue.clone());
        assert!(committed_topic_has_queue_state(&catalogued, "events"));
        let mut assigned = snapshot.clone();
        assigned.assignments.insert(
            queue.clone(),
            ganglion_core::PartitionAssignment::new(queue, "n1", Vec::new(), 1),
        );
        assert!(committed_topic_has_queue_state(&assigned, "events"));
    }

    #[test]
    fn applied_tail_labels_are_parsed_by_resource_kind() {
        assert_eq!(parse_applied_tail(&encode_queue_applied_tail(9), false), Some((9, 0)));
        assert_eq!(parse_applied_tail(&encode_stream_applied_tail(4, 6), true), Some((4, 6)));
        // Records outrank cursor events: ahead on events but behind on
        // records must never win.
        let rank = |m, e| parse_applied_tail(&encode_stream_applied_tail(m, e), true).unwrap();
        assert!(rank(100, 48) > rank(10, 50));
        assert!(rank(100, 50) > rank(90, 60));
        assert!(rank(100, 51) > rank(100, 50));
        // A value in the other kind's format ranks nothing.
        assert_eq!(parse_applied_tail(&encode_stream_applied_tail(4, 6), false), None);
        assert_eq!(parse_applied_tail(&encode_queue_applied_tail(9), true), None);
        assert_eq!(parse_applied_tail("x:6", true), None);
    }

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
                broker_addr: "127.0.0.1:9000".to_string(),
                admin_addr: None,
            },
        );
        nodes.insert(
            "broker-b".to_string(),
            NodeInfo {
                node_id: "broker-b".to_string(),
                broker_addr: "127.0.0.1:9001".to_string(),
                admin_addr: Some("127.0.0.1:9101".to_string()),
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
            stream_assignments: std::collections::HashMap::new(),
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
        assert_eq!(queue.owner_endpoints, vec!["127.0.0.1:9000".to_string()]);
        assert_eq!(queue.partitioning_version, DEFAULT_PARTITIONING_VERSION);

        // Stale generation is rejected after consensus.
        let stale = sample_snapshot(0, 1);
        let err = coordination
            .propose(&stale)
            .await
            .expect_err("stale generation must be rejected");
        assert!(matches!(err, OpenraftAdapterError::StaleGeneration));

        coordination
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
            .find(|provider| provider.consensus_node().node_id() == leader_raft_id)
            .expect("leader provider");
        let standby = providers
            .iter()
            .find(|provider| provider.consensus_node().node_id() != leader_raft_id)
            .expect("standby provider");

        // Standbys never act as controller.
        let queue = QueueIdentity::new("orders", Partition::new(0), Some("workers"));
        // An ephemeral stream makes no durability promise, so it may move on
        // liveness alone. The legacy queue keeps only its owner's copy and
        // must wait for that owner.
        let ticks = StreamIdentity::new("ticks", Partition::new(0));
        controller
            .declare_stream("ticks", 1, STREAM_DURABILITY_EPHEMERAL, Default::default(), None)
            .await
            .expect("declare ephemeral stream");
        let live_node = |id: &str, port: u16| NodeInfo {
            node_id: id.to_string(),
            broker_addr: format!("127.0.0.1:{port}"),
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
                &DeterministicStreamPlacement,
                &[],
                1,
                1,
                ReplicationDurabilityPolicy::LocalDurable,
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
                &DeterministicStreamPlacement,
                std::slice::from_ref(&ticks),
                1,
                1,
                ReplicationDurabilityPolicy::LocalDurable,
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
        let first_stream = assigned
            .stream_assignment_for("ticks", Partition::new(0))
            .expect("stream assigned")
            .clone();
        assert_eq!(first_stream.owner, first.owner, "both land on the first node");

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
                &DeterministicStreamPlacement,
                std::slice::from_ref(&ticks),
                1,
                1,
                ReplicationDurabilityPolicy::LocalDurable,
                &live,
                8,
            )
            .await
            .expect("failover iteration should commit")
            .expect("leader runs the iteration");
        let held = reassigned
            .assignment_for("orders", Partition::new(0), Some("workers"))
            .expect("queue still assigned")
            .clone();
        assert_eq!(held, first, "owner-only durable queue waits for its owner");
        let second = reassigned
            .stream_assignment_for("ticks", Partition::new(0))
            .expect("stream still assigned")
            .clone();
        assert_ne!(second.owner, first_stream.owner, "ownership must move");
        assert_eq!(second.epoch, first_stream.epoch + 1, "owner change must fence");

        // The standby's watch converges on the failover assignment.
        tokio::time::timeout(timeout, async {
            loop {
                let epoch = stream
                    .borrow_and_update()
                    .stream_assignment_for("ticks", Partition::new(0))
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
            provider
                .consensus_node()
                .shutdown()
                .await
                .expect("shutdown");
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

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queue_declarations_merge_settings_and_reject_count_conflicts() {
        use fibril_broker::{
            broker::QueueOwnership,
            queue_engine::{DLQDiscardPolicyWire, DeclareMeta},
        };
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

        let meta = DeclareMeta {
            dlq_policy: Some(DLQDiscardPolicyWire::CustomDQL {
                tp: "dead".into(),
                part: 3,
                group: Some("archive".into()),
            }),
            dlq_max_retries: Some(7),
            default_message_ttl_ms: Some(12345),
        };
        provider
            .declare_queue("orders", Some("workers"), 2, meta.clone().into())
            .await
            .unwrap();
        assert_eq!(
            provider
                .queue_declaration("orders", Some("workers"))
                .unwrap()
                .map(|declaration| declaration.meta),
            Some(meta.clone())
        );
        assert_eq!(provider.queue_declaration("orders", None).unwrap(), None);
        let update = DeclareMeta {
            dlq_max_retries: Some(9),
            ..Default::default()
        };
        provider
            .declare_queue("orders", Some("workers"), 2, update.into())
            .await
            .unwrap();
        let expected = DeclareMeta {
            dlq_max_retries: Some(9),
            ..meta
        };
        provider
            .declare_queue("orders", Some("workers"), 2, Default::default())
            .await
            .unwrap();
        assert_eq!(
            provider
                .queue_declaration("orders", Some("workers"))
                .unwrap()
                .map(|declaration| declaration.meta),
            Some(expected.clone())
        );
        assert!(
            provider
                .declare_queue(
                    "orders",
                    Some("workers"),
                    3,
                    DeclareMeta {
                        default_message_ttl_ms: Some(1),
                        ..Default::default()
                    }
                    .into()
                )
                .await
                .is_err()
        );
        assert_eq!(
            provider
                .queue_declaration("orders", Some("workers"))
                .unwrap()
                .map(|declaration| declaration.meta),
            Some(expected)
        );
        assert_ne!(
            queue_declaration_key("a/b", None),
            queue_declaration_key("a", Some("b"))
        );
        provider.node.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn accepted_history_cache_follows_every_committed_write() {
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
        let resource = ganglion_core::ResourceIdentity::new(QUEUE_NAMESPACE, "q", 0, None::<String>);

        let first = provider.cached_accepted_history(&resource);
        assert!(first.is_err(), "no history is enrolled yet");
        assert!(std::sync::Arc::ptr_eq(&first, &provider.cached_accepted_history(&resource)));
        provider.node.set_attribute("unrelated", "1").await.unwrap();
        let rebuilt = provider.cached_accepted_history(&resource);
        assert!(!std::sync::Arc::ptr_eq(&first, &rebuilt));
        assert_eq!(*first, *rebuilt);
        provider.node.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn queue_mode_is_fixed_before_partitions_and_legacy_queues_stay_durable() {
        use fibril_broker::{
            broker::QueueOwnership,
            queue_engine::{QueueDeclaration, QueueDurability},
        };
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
        let buffered = || QueueDeclaration::new(Default::default(), Some(QueueDurability::Buffered));

        let (_, mode) = provider
            .declare_queue_declared("clicks", None, 1, buffered())
            .await
            .unwrap();
        assert_eq!(mode, QueueDurability::Buffered);
        assert_eq!(
            provider.queue_declaration("clicks", None).unwrap().map(|d| d.mode()),
            Some(QueueDurability::Buffered)
        );

        // A count conflict leaves no declaration behind.
        provider.declare_queue_partitioning("sized", None, 2).await.unwrap();
        assert!(matches!(
            provider.declare_queue_declared("sized", None, 3, buffered()).await,
            Err(DeclareQueueError::PartitionCountConflict { .. })
        ));
        assert_eq!(provider.queue_declaration("sized", None).unwrap(), None);

        // Partitions without a declaration come from before modes existed.
        provider.declare_queue_partitioning("legacy", None, 1).await.unwrap();
        assert!(matches!(
            provider.declare_queue_declared("legacy", None, 1, buffered()).await,
            Err(DeclareQueueError::DurabilityConflict {
                existing: QueueDurability::Durable,
                ..
            })
        ));
        let (_, mode) = provider
            .declare_queue_declared("legacy", None, 1, Default::default())
            .await
            .unwrap();
        assert_eq!(mode, QueueDurability::Durable);
        provider.node.shutdown().await.unwrap();
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
        assert_eq!(grown.partitioning_version, created.partitioning_version + 1);

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

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
        let providers: Vec<GanglionCoordination> = raft_nodes
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
            .find(|provider| provider.consensus_node().node_id() != leader_raft_id)
            .expect("standby provider");

        // The standby must observe the elected leader before it can forward
        // deterministically, since with zero blind retries there is no fallback
        // wait while its leader view converges.
        standby
            .consensus_node()
            .wait_for_any_leader(timeout)
            .await
            .expect("standby observes the leader");

        use fibril_broker::broker::QueueOwnership;
        let meta = fibril_broker::queue_engine::DeclareMeta {
            dlq_max_retries: Some(17), default_message_ttl_ms: Some(12345), ..Default::default()
        };
        let declared = standby
            .declare_queue("orders", None, 3, meta.clone().into())
            .await
            .expect("standby declare should forward to leader");
        assert_eq!(declared.partition_count, 3);

        for provider in &providers {
            let mut watch = provider.watch();
            tokio::time::timeout(timeout, async {
                loop {
                    if provider.queue_partitioning("orders", None) == Some(declared.clone())
                        && provider.queue_declaration("orders", None).unwrap().map(|d| d.meta) == Some(meta.clone()) {
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
            provider
                .consensus_node()
                .shutdown()
                .await
                .expect("shutdown");
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
            .register_legacy_queue(&queue)
            .await
            .expect("register queue");
        let resource = to_ganglion_resource(&queue);
        let incarnation = provider.node.read_committed(|snapshot|
            history_identity::resource_incarnation(snapshot, &resource).unwrap().unwrap());
        let (left, right) = tokio::join!(provider.register_legacy_queue(&queue), provider.register_legacy_queue(&queue));
        left.expect("concurrent registration");
        right.expect("concurrent registration");
        assert_eq!(provider.node.read_committed(|snapshot|
            history_identity::resource_incarnation(snapshot, &resource).unwrap().unwrap()), incarnation);
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
                broker_addr: "127.0.0.1:9000".to_string(),
                admin_addr: None,
            })
            .await
            .expect("register node for placement");
        let live = provider.live_nodes(Duration::from_secs(30));
        provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                0,
                0,
                ReplicationDurabilityPolicy::LocalDurable,
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
        assert_eq!(provider.node.read_committed(|snapshot|
            history_identity::resource_incarnation(snapshot, &resource).unwrap().unwrap()), incarnation);
        // The trait snapshot()/watch() view updates through the spawned watch
        // forwarder, so it lags the committed controller write by a task hop.
        // Wait bounded for the assignment to propagate instead of reading
        // immediately, like the neighboring placement tests.
        let mut watch = provider.watch();
        tokio::time::timeout(Duration::from_secs(10), async {
            while watch
                .borrow_and_update()
                .assignment_for("orders", Partition::new(0), Some("workers"))
                .is_none()
            {
                watch.changed().await.expect("watch open");
            }
        })
        .await
        .expect("controller assigned the catalogue queue");
        let assigned = provider
            .snapshot()
            .assignment_for("orders", Partition::new(0), Some("workers"))
            .cloned()
            .expect("assignment visible once the watch caught up");
        assert_eq!(assigned.owner, "broker-a");

        provider.deregister_queue(&queue).await.expect("deregister");
        assert!(provider.registered_queues().is_empty());

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn client_topology_lists_catalogued_partitions_before_placement() {
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

        // Declared and catalogued, but no controller has placed anything yet.
        provider
            .declare_queue("jobs", None, 2, Default::default())
            .await
            .expect("declare queue");
        for partition in 0..2 {
            provider
                .register_queue(&QueueIdentity::new("jobs", Partition::new(partition), None))
                .await
                .expect("register queue");
        }
        provider
            .declare_stream("events", 1, 2, StreamRetentionConfig::default(), None)
            .await
            .expect("declare stream");
        provider
            .register_stream(&StreamIdentity::new("events", Partition::new(0)))
            .await
            .expect("register stream");

        // Clients see the topics with their counts and no owner, so a
        // populated topology never makes a declared queue look deleted.
        let topology = provider.client_topology();
        assert_eq!(topology.queues.len(), 2);
        for (index, entry) in topology.queues.iter().enumerate() {
            assert_eq!(entry.topic, "jobs");
            assert_eq!(entry.partition, Partition::new(index as u32));
            assert!(entry.owner_node_id.is_empty());
            assert!(entry.owner_endpoints.is_empty());
            assert_eq!(entry.partition_count, 2);
        }
        assert_eq!(topology.streams.len(), 1);
        assert_eq!(topology.streams[0].topic, "events");
        assert!(topology.streams[0].owner_endpoints.is_empty());
        // An unplaced stream partition has no route to forward to.
        assert!(provider
            .client_stream_route("events", Partition::new(0))
            .is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stream_catalogue_places_owner_and_surfaces_in_client_topology() {
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

        // Declare a 2-partition durable stream and catalogue both partitions.
        provider
            .declare_stream("events", 2, 2, StreamRetentionConfig::default(), None)
            .await
            .expect("declare stream");
        for partition in 0..2 {
            provider
                .register_stream(&StreamIdentity::new("events", Partition::new(partition)))
                .await
                .expect("register stream");
        }
        let mut registered = provider.registered_streams();
        registered.sort_by_key(|s| s.partition);
        assert_eq!(
            registered,
            vec![
                StreamIdentity::new("events", Partition::new(0)),
                StreamIdentity::new("events", Partition::new(1)),
            ]
        );
        // The declared config (durability tier + count) round-trips so an owner
        // can open with it.
        let config = provider.stream_config("events").expect("stream config");
        assert_eq!(config.partition_count, 2);
        assert_eq!(config.durability, 2);
        // No per-stream override: the cluster default applies at placement.
        assert_eq!(config.replication_factor, None);

        // A per-stream replication-factor override round-trips through the config,
        // so the controller can prefer it over the cluster default.
        provider
            .declare_stream("crit", 1, 2, StreamRetentionConfig::default(), Some(2))
            .await
            .expect("declare crit");
        assert_eq!(
            provider
                .stream_config("crit")
                .expect("crit config")
                .replication_factor,
            Some(2)
        );

        provider
            .register_self(&NodeInfo {
                node_id: "broker-a".into(),
                broker_addr: "127.0.0.1:9100".to_string(),
                admin_addr: None,
            })
            .await
            .expect("register node for placement");
        let live = provider.live_nodes(Duration::from_secs(30));
        let committed = provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                0,
                0,
                ReplicationDurabilityPolicy::LocalDurable,
                &live,
                8,
            )
            .await
            .expect("controller iteration")
            .expect("leader runs it");

        // Both stream partitions are owned by the only live broker, kept in the
        // stream map (never the queue map). Assert on the snapshot the write
        // returned, then wait for the watch view to catch up before checking
        // the watch-derived surfaces (the watch applies asynchronously).
        assert!(committed.assignments.is_empty(), "no queue assignments");
        for partition in 0..2 {
            let assigned = committed
                .stream_assignment_for("events", Partition::new(partition))
                .expect("stream assigned");
            assert_eq!(assigned.owner, "broker-a");
        }
        let mut watch = provider.watch();
        tokio::time::timeout(Duration::from_secs(10), async {
            while watch
                .borrow_and_update()
                .stream_assignment_for("events", Partition::new(0))
                .is_none()
            {
                watch.changed().await.expect("watch open");
            }
        })
        .await
        .expect("watch view caught up with the committed assignments");

        // The client topology carries the owner endpoint and authoritative count.
        let topology = provider.client_topology();
        assert!(topology.queues.is_empty());
        assert_eq!(topology.streams.len(), 2);
        for entry in &topology.streams {
            assert_eq!(entry.topic, "events");
            assert_eq!(entry.owner_node_id, "broker-a");
            assert_eq!(entry.owner_endpoints, vec!["127.0.0.1:9100".to_string()]);
            assert_eq!(entry.partition_count, 2);
        }

        // The StreamOwnership gate: this node owns the assigned partitions but not
        // an unassigned one, and the declared open-config round-trips for the owner
        // to materialize with.
        use fibril_broker::broker::StreamOwnership as _;
        assert!(provider.owns_stream("events", Partition::new(0)));
        assert!(provider.owns_stream("events", Partition::new(1)));
        assert!(
            !provider.owns_stream("events", Partition::new(5)),
            "an unassigned partition is not owned"
        );
        let open = provider
            .stream_open_config("events")
            .expect("declared open config");
        assert_eq!(
            open.durability,
            fibril_broker::stream::StreamDurability::Durable
        );

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
            broker_addr: format!("127.0.0.1:{port}"),
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
        provider.register_legacy_queue(&queue).await.expect("queue");
        // Only the ephemeral stream may leave a dead owner. The legacy queue
        // keeps confirmed data on its owner alone.
        provider
            .declare_stream("ticks", 1, STREAM_DURABILITY_EPHEMERAL, Default::default(), None)
            .await
            .expect("declare stream");
        provider
            .register_stream(&StreamIdentity::new("ticks", Partition::new(0)))
            .await
            .expect("stream");

        let (controller, status) = provider.spawn_controller(
            std::sync::Arc::new(DeterministicPartitionPlacement),
            ControllerConfig {
                target_followers: 1,
                stream_replication_factor: 1,
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
                tick: Duration::from_millis(100),
                // Generous TTL first: both brokers count as live.
                liveness_ttl: Duration::from_secs(30),
                max_cas_retries: 8,
                ..Default::default()
            },
        );

        // Assignment appears.
        let mut stream = provider.watch();
        tokio::time::timeout(Duration::from_secs(10), async {
            while stream
                .borrow_and_update()
                .stream_assignment_for("ticks", Partition::new(0))
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
        let first_stream = provider
            .snapshot()
            .stream_assignment_for("ticks", Partition::new(0))
            .cloned()
            .expect("stream assigned");
        assert_eq!(first_stream.owner, first.owner);
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
                stream_replication_factor: 1,
                default_durability: ReplicationDurabilityPolicy::LocalDurable,
                tick: Duration::from_millis(100),
                liveness_ttl: Duration::from_millis(900),
                max_cas_retries: 8,
                // The dead owner never returns, so skip the startup wait.
                startup_grace: Duration::ZERO,
                ..Default::default()
            },
        );

        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let snapshot = provider.snapshot();
                if let Some(assignment) = snapshot.stream_assignment_for("ticks", Partition::new(0)) {
                    if assignment.owner == survivor {
                        assert_eq!(
                            assignment.epoch,
                            first_stream.epoch + 1,
                            "ownership move must fence"
                        );
                        assert_eq!(
                            snapshot.assignment_for("orders", Partition::new(0), None),
                            Some(&first),
                            "owner-only durable queue waits for its owner"
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
        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
        settings.replication.eager_failover = true;
        settings.replication.eager_failover_grace_ms = 750;
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

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
            broker_addr: format!("127.0.0.1:{port}"),
            admin_addr: None,
        };
        let tails = |next: u64| {
            let mut labels = std::collections::BTreeMap::new();
            labels.insert(applied_tail_label(&queue), encode_queue_applied_tail(next));
            labels
        };

        provider
            .register_self(&info("a-owner", 9000))
            .await
            .expect("a");
        // b-slow sorts FIRST among followers but is behind; c-fast is ahead.
        provider
            .register_self_with_labels(&info("b-slow", 9001), tails(5))
            .await
            .expect("b");
        provider
            .register_self_with_labels(&info("c-fast", 9002), tails(9))
            .await
            .expect("c");
        provider.register_legacy_queue(&queue).await.expect("queue");

        // Healthy assignment: a owns, b and c follow.
        let all_live = provider.live_nodes(Duration::from_secs(30));
        let committed = provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                2,
                2,
                ReplicationDurabilityPolicy::MajorityDurable,
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
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                2,
                2,
                ReplicationDurabilityPolicy::MajorityDurable,
                &live,
                8,
            )
            .await
            .expect("failover")
            .expect("leader");
        // The change parks as a recovery request. Its proposed owner is the
        // node that drives recovery, and the tails decide it.
        assert_eq!(
            committed.assignment_for("orders", Partition::new(0), None),
            Some(&first)
        );
        let pending = provider.pending_recoveries().expect("pending");
        assert_eq!(pending.len(), 1);
        let moved = &pending[0].proposed;
        assert_eq!(
            moved.owner, "c-fast",
            "candidate selection must prefer the most caught-up follower"
        );
        assert_eq!(moved.epoch, first.epoch + 1, "the move fences");
        assert!(
            moved.followers.contains(&"b-slow".to_string()),
            "the displaced candidate stays in the replica set: {moved:?}"
        );

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
    }

    /// Stale labels cannot authorize a majority-durable history replacement.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn majority_failover_preserves_old_assignment_until_recovery_proof() {
        use fibril_broker::coordination::DeterministicPartitionPlacement;
        let router = InProcessRouter::new();
        let directory = std::env::temp_dir().join(format!(
            "fibril-promotion-controller-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let raft =
            RaftMetadataNode::start_durable(1, default_raft_config().unwrap(), &router, &directory)
                .await
                .unwrap();
        raft.initialize(BTreeMap::from([(
            1,
            ganglion_openraft::openraft::BasicNode::new("n1"),
        )]))
        .await
        .unwrap();
        raft.wait_for_leader(1, Duration::from_secs(10))
            .await
            .unwrap();
        let provider = std::sync::Arc::new(GanglionCoordination::new("c-lagging", raft));
        let queue = QueueIdentity::new("confirmed-history", Partition::new(0), None);
        let info = |id: &str| NodeInfo {
            node_id: id.into(),
            broker_addr: "127.0.0.1:9000".into(),
            admin_addr: None,
        };
        for (id, reported_tail) in [("a-owner", 9), ("b-durable", 0), ("c-lagging", 1)] {
            provider
                .register_self_with_labels(
                    &info(id),
                    BTreeMap::from([(
                        applied_tail_label(&queue),
                        encode_queue_applied_tail(reported_tail),
                    )]),
                )
                .await
                .unwrap();
        }
        provider.register_legacy_queue(&queue).await.unwrap();
        let original = provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                2,
                2,
                ReplicationDurabilityPolicy::MajorityDurable,
                &provider.live_nodes(Duration::from_secs(30)),
                8,
            )
            .await
            .unwrap()
            .unwrap();
        let before = original
            .assignment_for(&queue.topic, queue.partition, None)
            .unwrap()
            .clone();
        assert_eq!(before.owner, "a-owner");
        // B's label predates its durable copy; C advertises the larger label.
        // Neither label is an authoritative recovery witness.
        let survivors = HashMap::from([
            ("b-durable".into(), info("b-durable")),
            ("c-lagging".into(), info("c-lagging")),
        ]);
        let after = provider
            .control_iteration(
                &DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                2,
                2,
                ReplicationDurabilityPolicy::MajorityDurable,
                &survivors,
                8,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            after.assignment_for(&queue.topic, queue.partition, None),
            Some(&before),
            "an unproven candidate must not become owner or replication source"
        );
        let pending = provider.pending_recoveries().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].previous.owner, "a-owner");
        assert_eq!(pending[0].proposed.owner, "c-lagging");
        assert_eq!(pending[0].required_old_witnesses, 2);
        let seal = pending[0].seal_command().unwrap();
        let generation_before = provider.consensus_node().committed_snapshot().generation;
        use fibril_broker::broker::QueueOwnership;
        provider.authorize_recovery_seal(&seal).await.unwrap();
        provider.authorize_recovery_seal(&seal).await.unwrap();
        assert_eq!(
            provider.consensus_node().committed_snapshot().generation,
            generation_before,
            "repeated seal authorization must not change metadata generation"
        );
        let mut stale = seal.clone();
        stale.fence_epoch += 1;
        assert!(provider.authorize_recovery_seal(&stale).await.is_err());

        // A cold candidate must also remain unservable: it has no local role
        // history to make a follower-only promotion check reject admission.
        use fibril_broker::{
            broker::{Broker, BrokerConfig, BrokerError},
            queue_engine::{KeratinConfig, SnapshotConfig, StromaEngine, StromaKeratinConfig},
        };
        let engine = StromaEngine::open(
            directory.join("candidate"),
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let candidate =
            Broker::new_with_ownership(engine, BrokerConfig::default(), None, provider.clone());
        assert!(
            matches!(
                candidate
                    .get_publisher(&queue.topic, queue.partition, &None)
                    .await,
                Err(BrokerError::NotOwner { .. })
            ),
            "a pending cold candidate must not create a new owner history"
        );
        candidate.shutdown().await;
        drop(candidate);
        let stored_generation = provider.consensus_node().committed_snapshot().generation;
        provider.consensus_node().shutdown().await.unwrap();
        drop(provider);
        let restarted = RaftMetadataNode::start_durable(
            1,
            default_raft_config().unwrap(),
            &InProcessRouter::new(),
            &directory,
        )
        .await
        .unwrap();
        restarted
            .wait_for_leader(1, Duration::from_secs(10))
            .await
            .unwrap();
        // Election precedes re-commit/application of the persisted WAL tail.
        // Check recovery after replay reaches the generation we actually stored.
        let mut replay = restarted.watch_committed();
        tokio::time::timeout(Duration::from_secs(10), async {
            while replay.borrow_and_update().generation < stored_generation {
                replay.changed().await.expect("replay watch remains open");
            }
        })
        .await
        .expect("restart must replay the persisted recovery request");
        let recovered = GanglionCoordination::new("a-owner", restarted);
        assert_eq!(recovered.pending_recoveries().unwrap(), pending);
        recovered.authorize_recovery_seal(&seal).await.unwrap();
        let retry = recovered
            .control_iteration(
                &DeterministicPartitionPlacement,
                &recovered.registered_queues(),
                &DeterministicStreamPlacement,
                &recovered.registered_streams(),
                2,
                2,
                ReplicationDurabilityPolicy::MajorityDurable,
                &survivors,
                8,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            retry.assignment_for(&queue.topic, queue.partition, None),
            Some(&before)
        );
        assert_eq!(
            recovered.consensus_node().committed_snapshot().generation,
            stored_generation,
            "a pending transition must not write a fresh proposal every controller tick"
        );
        recovered.consensus_node().shutdown().await.unwrap();
        drop(recovered);
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// Drain evacuation: a live draining owner hands its partition to the
    /// most caught-up follower through the same fenced move as failover, and
    /// receives no new placements while draining.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn draining_owner_evacuates_and_receives_no_new_placements() {
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
            broker_addr: format!("127.0.0.1:{port}"),
            admin_addr: None,
        };
        let tails = |next: u64| {
            let mut labels = std::collections::BTreeMap::new();
            labels.insert(applied_tail_label(&queue), encode_queue_applied_tail(next));
            labels
        };

        provider
            .register_self(&info("a-owner", 9000))
            .await
            .expect("a");
        provider
            .register_self_with_labels(&info("b-slow", 9001), tails(5))
            .await
            .expect("b");
        provider
            .register_self_with_labels(&info("c-fast", 9002), tails(9))
            .await
            .expect("c");
        provider.register_legacy_queue(&queue).await.expect("queue");

        let mut live = std::collections::HashMap::new();
        live.insert("a-owner".to_string(), info("a-owner", 9000));
        live.insert("b-slow".to_string(), info("b-slow", 9001));
        live.insert("c-fast".to_string(), info("c-fast", 9002));
        let iterate = |live: std::collections::HashMap<String, NodeInfo>| {
            let provider = &provider;
            async move {
                provider
                    .control_iteration(
                        &DeterministicPartitionPlacement,
                        &provider.registered_queues(),
                        &DeterministicStreamPlacement,
                        &provider.registered_streams(),
                        2,
                        2,
                        ReplicationDurabilityPolicy::MajorityDurable,
                        &live,
                        8,
                    )
                    .await
                    .expect("iteration")
                    .expect("leader")
            }
        };

        let committed = iterate(live.clone()).await;
        let first = committed
            .assignment_for("orders", Partition::new(0), None)
            .expect("assigned")
            .clone();
        assert_eq!(first.owner, "a-owner");

        // The owner starts draining while staying live: ownership moves to
        // the most caught-up follower with the same fencing as failover.
        let mut draining_labels = std::collections::BTreeMap::new();
        draining_labels.insert(DRAINING_LABEL.to_string(), "1".to_string());
        provider
            .register_self_with_labels(&info("a-owner", 9000), draining_labels)
            .await
            .expect("mark draining");
        iterate(live.clone()).await;
        let pending = provider.pending_recoveries().expect("pending");
        assert_eq!(pending.len(), 1);
        let moved = &pending[0].proposed;
        assert_eq!(
            moved.owner, "c-fast",
            "evacuate to the most caught-up follower"
        );
        assert_eq!(moved.epoch, first.epoch + 1, "the move fences");
        assert!(
            !moved.is_owned_by("a-owner") && !moved.is_followed_by("a-owner"),
            "the draining node leaves the replica set: {moved:?}"
        );

        // A queue declared mid-drain never lands on the draining node.
        let fresh = QueueIdentity::new("fresh", Partition::new(0), None);
        provider.register_legacy_queue(&fresh).await.expect("fresh");
        let committed = iterate(live).await;
        let placed = committed
            .assignment_for("fresh", Partition::new(0), None)
            .expect("placed")
            .clone();
        assert!(
            !placed.is_owned_by("a-owner") && !placed.is_followed_by("a-owner"),
            "no new placement on a draining node: {placed:?}"
        );

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
    }

    /// A draining owner with no live follower keeps its partition: the node
    /// is still serving, so staying put beats moving ownership to a node
    /// without a copy. Reactive failover remains the backstop after the
    /// process exits. An ephemeral owner-only stream shows this rule apart
    /// from the owner-only durability hold.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn draining_owner_without_live_follower_keeps_the_partition() {
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

        let info = |id: &str, port: u16| NodeInfo {
            node_id: id.to_string(),
            broker_addr: format!("127.0.0.1:{port}"),
            admin_addr: None,
        };
        provider
            .register_self(&info("a-owner", 9000))
            .await
            .expect("a");
        provider
            .register_self(&info("b-fresh", 9001))
            .await
            .expect("b");
        provider
            .declare_stream("solo", 1, STREAM_DURABILITY_EPHEMERAL, Default::default(), None)
            .await
            .expect("declare");
        provider
            .register_stream(&StreamIdentity::new("solo", Partition::new(0)))
            .await
            .expect("stream");

        let mut live = std::collections::HashMap::new();
        live.insert("a-owner".to_string(), info("a-owner", 9000));
        live.insert("b-fresh".to_string(), info("b-fresh", 9001));
        let iterate = |live: std::collections::HashMap<String, NodeInfo>| {
            let provider = &provider;
            async move {
                provider
                    .control_iteration(
                        &DeterministicPartitionPlacement,
                        &provider.registered_queues(),
                        &DeterministicStreamPlacement,
                        &provider.registered_streams(),
                        1,
                        1,
                        ReplicationDurabilityPolicy::LocalDurable,
                        &live,
                        8,
                    )
                    .await
                    .expect("iteration")
                    .expect("leader")
            }
        };

        let committed = iterate(live.clone()).await;
        let first = committed
            .stream_assignment_for("solo", Partition::new(0))
            .expect("assigned")
            .clone();
        assert_eq!(first.owner, "a-owner");

        let mut draining_labels = std::collections::BTreeMap::new();
        draining_labels.insert(DRAINING_LABEL.to_string(), "1".to_string());
        provider
            .register_self_with_labels(&info("a-owner", 9000), draining_labels)
            .await
            .expect("mark draining");
        let committed = iterate(live).await;
        let kept = committed
            .stream_assignment_for("solo", Partition::new(0))
            .expect("assigned")
            .clone();
        assert_eq!(kept.owner, "a-owner", "no reporting follower: stay put");
        assert_eq!(kept.epoch, first.epoch, "staying put must not fence");

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
    }

    #[test]
    fn failover_owner_prefers_labelled_then_least_loaded_follower() {
        let resource = ganglion_core::ResourceIdentity::new(QUEUE_NAMESPACE, "q", 0, None);
        let current = ganglion_core::PartitionAssignment::new(
            resource,
            "a",
            vec!["b".into(), "c".into(), "d".into()],
            1,
        );
        let mut committed = ganglion_core::CoordinationSnapshot::default();
        let label = "applied/q/0";
        let any = |_: &str| true;
        let none = HashMap::new();
        assert_eq!(choose_failover_owner(&current, &committed, label, false, &none, any).as_deref(), Some("b"));
        assert_eq!(
            choose_failover_owner(&current, &committed, label, false, &none, |n| n != "b").as_deref(),
            Some("c")
        );
        let mut node = ganglion_core::NodeInfo::new("d", "127.0.0.1:1", None);
        node.labels.insert(label.into(), encode_queue_applied_tail(3));
        committed.nodes.insert("d".into(), node);
        assert_eq!(choose_failover_owner(&current, &committed, label, false, &none, any).as_deref(), Some("d"));
        assert_eq!(choose_failover_owner(&current, &committed, label, false, &none, |_| false), None);

        // Equal labels: the follower owning fewer partitions wins, then the
        // smaller id, whatever the follower order.
        for id in ["b", "c"] {
            let mut node = ganglion_core::NodeInfo::new(id, "127.0.0.1:1", None);
            node.labels.insert(label.into(), encode_queue_applied_tail(3));
            committed.nodes.insert(id.into(), node);
        }
        let mut reversed = current.clone();
        reversed.followers.reverse();
        for assignment in [&current, &reversed] {
            assert_eq!(choose_failover_owner(assignment, &committed, label, false, &none, any).as_deref(), Some("b"));
            let loaded = HashMap::from([("b".to_string(), 2), ("d".to_string(), 1)]);
            assert_eq!(choose_failover_owner(assignment, &committed, label, false, &loaded, any).as_deref(), Some("c"));
        }
    }

    #[test]
    fn stream_failover_ranks_record_tail_before_event_tail() {
        let resource = ganglion_core::ResourceIdentity::new(STREAM_NAMESPACE, "s", 0, None);
        let current = ganglion_core::PartitionAssignment::new(
            resource,
            "a",
            vec!["b-backfilling".into(), "c-caught-up".into()],
            1,
        );
        let label = "applied/s/0";
        let mut committed = ganglion_core::CoordinationSnapshot::default();
        for (id, records, events) in [("b-backfilling", 10_000, 50), ("c-caught-up", 1_000_000, 48)] {
            let mut node = ganglion_core::NodeInfo::new(id, "127.0.0.1:1", None);
            node.labels.insert(label.into(), encode_stream_applied_tail(records, events));
            committed.nodes.insert(id.into(), node);
        }
        assert_eq!(
            choose_failover_owner(&current, &committed, label, true, &HashMap::new(), |_| true).as_deref(),
            Some("c-caught-up")
        );
    }

    /// Partitions of a failed node spread over its equally ranked followers
    /// instead of all landing on one of them.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn failover_spreads_equally_ranked_partitions() {
        let router = InProcessRouter::new();
        let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        raft_node
            .initialize(BTreeMap::from([(1u64, ganglion_openraft::openraft::BasicNode::new("n1"))]))
            .await
            .unwrap();
        raft_node.wait_for_leader(1, Duration::from_secs(10)).await.unwrap();
        let provider = GanglionCoordination::new("b", raft_node);
        let info = |id: &str| NodeInfo {
            node_id: id.into(),
            broker_addr: "127.0.0.1:9000".into(),
            admin_addr: None,
        };
        for id in ["a", "b", "c"] {
            provider.register_self(&info(id)).await.unwrap();
        }
        for partition in 0..6 {
            provider
                .register_legacy_queue(&QueueIdentity::new("orders", Partition::new(partition), None))
                .await
                .unwrap();
        }
        let iterate = |ids: &'static [&'static str]| {
            let provider = &provider;
            async move {
                let live: HashMap<String, NodeInfo> =
                    ids.iter().map(|id| (id.to_string(), info(id))).collect();
                provider
                    .control_iteration(
                        &fibril_broker::coordination::DeterministicPartitionPlacement,
                        &provider.registered_queues(),
                        &DeterministicStreamPlacement,
                        &[],
                        2,
                        2,
                        ReplicationDurabilityPolicy::MajorityDurable,
                        &live,
                        8,
                    )
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        let placed = iterate(&["a", "b", "c"]).await;
        assert_eq!(placed.owned_by("a").len(), 2);
        iterate(&["b", "c"]).await;
        let mut new_owners: Vec<String> = provider
            .pending_recoveries()
            .unwrap()
            .into_iter()
            .filter(|pending| pending.previous.owner == "a")
            .map(|pending| pending.proposed.owner)
            .collect();
        new_owners.sort();
        assert_eq!(new_owners, vec!["b", "c"]);
        provider.consensus_node().shutdown().await.unwrap();
    }

    /// The node the planner picked before the label override chose a
    /// follower does not join the proposed replica set: it holds no copy and
    /// would have to be built before the new owner serves.
    /// A restarted process changes only its node labels, which do not bump
    /// the generation. Its first registration must still reach watched
    /// snapshots, or histories built from them keep the dead process.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restarted_process_identity_reaches_watched_snapshots() {
        let router = InProcessRouter::new();
        let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        raft_node
            .initialize(BTreeMap::from([(1u64, ganglion_openraft::openraft::BasicNode::new("n1"))]))
            .await
            .unwrap();
        raft_node.wait_for_leader(1, Duration::from_secs(10)).await.unwrap();
        let provider = GanglionCoordination::new("b", raft_node);
        let info = NodeInfo {
            node_id: "b".into(),
            broker_addr: "127.0.0.1:9000".into(),
            admin_addr: None,
        };
        // The record the previous process left behind.
        let dead = uuid::Uuid::from_bytes([7; 16]).to_string();
        let mut old = ganglion_core::NodeInfo::new("b", info.broker_addr.clone(), None);
        old.labels.insert(HISTORY_PROCESS_LABEL.into(), dead.clone());
        provider.node.register_node(old).await.unwrap();
        let watched = provider.node.watch_committed();
        assert_eq!(watched.borrow().nodes["b"].labels[HISTORY_PROCESS_LABEL], dead);

        provider.register_self(&info).await.unwrap();
        let current = uuid::Uuid::from_bytes(provider.history_process).to_string();
        assert_eq!(
            watched.borrow().nodes["b"].labels[HISTORY_PROCESS_LABEL],
            current,
            "the watched snapshot must carry the new process"
        );
        // Later heartbeats stay label-only and do not bump the generation.
        let generation = watched.borrow().generation;
        provider.register_self(&info).await.unwrap();
        assert_eq!(provider.node.committed_snapshot().generation, generation);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn displaced_planner_pick_stays_out_of_the_recovery_set() {
        let router = InProcessRouter::new();
        let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        raft_node
            .initialize(BTreeMap::from([(1u64, ganglion_openraft::openraft::BasicNode::new("n1"))]))
            .await
            .unwrap();
        raft_node.wait_for_leader(1, Duration::from_secs(10)).await.unwrap();
        let provider = GanglionCoordination::new("b", raft_node);
        let queue = QueueIdentity::new("orders", Partition::new(0), None);
        let info = |id: &str| NodeInfo {
            node_id: id.into(),
            broker_addr: "127.0.0.1:9000".into(),
            admin_addr: None,
        };
        let tails = |next| BTreeMap::from([(applied_tail_label(&queue), encode_queue_applied_tail(next))]);
        provider.register_legacy_queue(&queue).await.unwrap();
        let iterate = |ids: &'static [&'static str]| {
            let provider = &provider;
            async move {
                let live: HashMap<String, NodeInfo> =
                    ids.iter().map(|id| (id.to_string(), info(id))).collect();
                provider
                    .control_iteration(
                        &fibril_broker::coordination::DeterministicPartitionPlacement,
                        &provider.registered_queues(),
                        &DeterministicStreamPlacement,
                        &[],
                        2,
                        2,
                        ReplicationDurabilityPolicy::MajorityDurable,
                        &live,
                        8,
                    )
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        provider.register_self(&info("b")).await.unwrap();
        provider.register_self_with_labels(&info("c"), tails(5)).await.unwrap();
        provider.register_self_with_labels(&info("d"), tails(9)).await.unwrap();
        let first = iterate(&["b", "c", "d"]).await.assignment_for("orders", Partition::new(0), None).unwrap().clone();
        assert_eq!(first.owner, "b");
        assert_eq!(first.followers, vec!["c", "d"]);
        // "a" joins later and sorts first, so the planner picks it for the
        // dead owner's partition. The labels choose "d".
        provider.register_self(&info("a")).await.unwrap();
        iterate(&["a", "c", "d"]).await;
        let pending = provider.pending_recoveries().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].proposed.owner, "d");
        assert_eq!(pending[0].proposed.followers, vec!["c"]);
        provider.consensus_node().shutdown().await.unwrap();
    }

    /// With no applied-tail labels, a dead owner's partition goes to a live
    /// committed follower rather than the planner's round-robin pick, which
    /// would have to copy the whole history first.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dead_owner_prefers_an_unlabelled_follower_over_a_fresh_node() {
        let router = InProcessRouter::new();
        let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        raft_node
            .initialize(BTreeMap::from([(1u64, ganglion_openraft::openraft::BasicNode::new("n1"))]))
            .await
            .unwrap();
        raft_node.wait_for_leader(1, Duration::from_secs(10)).await.unwrap();
        let provider = GanglionCoordination::new("b", raft_node);
        let info = |id: &str| NodeInfo {
            node_id: id.into(),
            broker_addr: "127.0.0.1:9000".into(),
            admin_addr: None,
        };
        let queue = QueueIdentity::new("orders", Partition::new(0), None);
        provider.register_legacy_queue(&queue).await.unwrap();
        let iterate = |ids: &'static [&'static str]| {
            let provider = &provider;
            async move {
                let live: HashMap<String, NodeInfo> =
                    ids.iter().map(|id| (id.to_string(), info(id))).collect();
                provider
                    .control_iteration(
                        &fibril_broker::coordination::DeterministicPartitionPlacement,
                        &provider.registered_queues(),
                        &DeterministicStreamPlacement,
                        &[],
                        1,
                        1,
                        ReplicationDurabilityPolicy::MajorityDurable,
                        &live,
                        8,
                    )
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        for id in ["b", "c"] {
            provider.register_self(&info(id)).await.unwrap();
        }
        let first = iterate(&["b", "c"]).await.assignment_for("orders", Partition::new(0), None).unwrap().clone();
        assert_eq!((first.owner.as_str(), first.followers.as_slice()), ("b", &["c".to_string()][..]));
        // "a" joins later and sorts first, so round-robin would pick it.
        provider.register_self(&info("a")).await.unwrap();
        iterate(&["a", "c"]).await;
        let pending = provider.pending_recoveries().unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].proposed.owner, "c");
        // "c" alone cannot meet the old write requirement of two copies, so
        // the fresh node joins the recovery set.
        assert_eq!(pending[0].proposed.followers, vec!["a"]);
        provider.consensus_node().shutdown().await.unwrap();
    }

    /// During the startup grace an existing assignment keeps its owner and
    /// followers even when they look dead, followers still fill empty slots,
    /// and new resources are still placed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn startup_grace_keeps_owners_but_fills_and_places() {
        let router = InProcessRouter::new();
        let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        raft_node
            .initialize(BTreeMap::from([(1u64, ganglion_openraft::openraft::BasicNode::new("n1"))]))
            .await
            .unwrap();
        raft_node.wait_for_leader(1, Duration::from_secs(10)).await.unwrap();
        let provider = GanglionCoordination::new("a", raft_node);
        let info = |id: &str| NodeInfo {
            node_id: id.into(),
            broker_addr: "127.0.0.1:9000".into(),
            admin_addr: None,
        };
        for id in ["a", "b", "c"] {
            provider.register_self(&info(id)).await.unwrap();
        }
        provider
            .declare_stream("ticks", 1, STREAM_DURABILITY_EPHEMERAL, Default::default(), None)
            .await
            .unwrap();
        let stream = StreamIdentity::new("ticks", Partition::new(0));
        provider.register_stream(&stream).await.unwrap();
        let queue = QueueIdentity::new("orders", Partition::new(0), None);
        provider.register_legacy_queue(&queue).await.unwrap();
        let live = |ids: &[&str]| -> HashMap<String, NodeInfo> {
            ids.iter().map(|id| (id.to_string(), info(id))).collect()
        };
        let first = provider
            .control_iteration(
                &fibril_broker::coordination::DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                1,
                1,
                ReplicationDurabilityPolicy::LocalDurable,
                &live(&["a", "b", "c"]),
                8,
            )
            .await
            .unwrap()
            .unwrap();
        let stream_before = first.stream_assignment_for("ticks", Partition::new(0)).unwrap().clone();
        let queue_before = first.assignment_for("orders", Partition::new(0), None).unwrap().clone();
        assert_eq!(stream_before.owner, "a");
        assert_eq!(queue_before.followers, vec!["b"]);

        // "a" and "b" look dead while booting. The grace keeps the stream on
        // "a" and the queue's follower "b", adds a second follower slot, and
        // still places a fresh queue.
        let fresh = QueueIdentity::new("fresh", Partition::new(0), None);
        provider.register_legacy_queue(&fresh).await.unwrap();
        let held = provider
            .control_iteration_in_startup_grace(
                &fibril_broker::coordination::DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                2,
                1,
                ReplicationDurabilityPolicy::LocalDurable,
                &live(&["c"]),
                8,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(held.stream_assignment_for("ticks", Partition::new(0)), Some(&stream_before));
        let queue_now = held.assignment_for("orders", Partition::new(0), None).unwrap();
        assert_eq!(queue_now.owner, queue_before.owner);
        assert_eq!(queue_now.followers, vec!["b", "c"]);
        assert_eq!(held.assignment_for("fresh", Partition::new(0), None).unwrap().owner, "c");

        // Without the grace the ephemeral stream leaves its missing owner.
        let moved = provider
            .control_iteration(
                &fibril_broker::coordination::DeterministicPartitionPlacement,
                &provider.registered_queues(),
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                2,
                1,
                ReplicationDurabilityPolicy::LocalDurable,
                &live(&["c"]),
                8,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(moved.stream_assignment_for("ticks", Partition::new(0)).unwrap().owner, "c");
        provider.consensus_node().shutdown().await.unwrap();
    }

    /// The controller loop applies the grace after winning leadership and
    /// ends it once the configured time passes without the missing node.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn controller_waits_out_the_startup_grace_for_a_missing_node() {
        let router = InProcessRouter::new();
        let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        raft_node
            .initialize(BTreeMap::from([(1u64, ganglion_openraft::openraft::BasicNode::new("n1"))]))
            .await
            .unwrap();
        raft_node.wait_for_leader(1, Duration::from_secs(10)).await.unwrap();
        let provider = std::sync::Arc::new(GanglionCoordination::new("broker-b", raft_node));
        let info = |id: &str| NodeInfo {
            node_id: id.into(),
            broker_addr: "127.0.0.1:9000".into(),
            admin_addr: None,
        };
        for id in ["broker-a", "broker-b"] {
            provider.register_self(&info(id)).await.unwrap();
        }
        provider
            .declare_stream("ticks", 1, STREAM_DURABILITY_EPHEMERAL, Default::default(), None)
            .await
            .unwrap();
        provider
            .register_stream(&StreamIdentity::new("ticks", Partition::new(0)))
            .await
            .unwrap();
        let placed = provider
            .control_iteration(
                &fibril_broker::coordination::DeterministicPartitionPlacement,
                &[],
                &DeterministicStreamPlacement,
                &provider.registered_streams(),
                0,
                0,
                ReplicationDurabilityPolicy::LocalDurable,
                &provider.live_nodes(Duration::from_secs(30)),
                8,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(placed.stream_assignment_for("ticks", Partition::new(0)).unwrap().owner, "broker-a");

        // broker-a stops heartbeating, broker-b keeps beating.
        let beat = {
            let provider = provider.clone();
            tokio::spawn(async move {
                loop {
                    let _ = provider.register_self(&info("broker-b")).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            })
        };
        tokio::time::sleep(Duration::from_millis(400)).await;
        let grace = Duration::from_millis(1500);
        let started = std::time::Instant::now();
        let (controller, status) = provider.spawn_controller(
            std::sync::Arc::new(fibril_broker::coordination::DeterministicPartitionPlacement),
            ControllerConfig {
                target_followers: 0,
                stream_replication_factor: 0,
                tick: Duration::from_millis(50),
                liveness_ttl: Duration::from_millis(300),
                startup_grace: grace,
                ..Default::default()
            },
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let owner = provider
                    .snapshot()
                    .stream_assignment_for("ticks", Partition::new(0))
                    .unwrap()
                    .owner
                    .clone();
                if owner == "broker-b" {
                    break;
                }
                if started.elapsed() > Duration::from_millis(300) {
                    assert_eq!(status.read().unwrap().startup_grace_waiting_for, vec!["broker-a"]);
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("owner moves once the grace expires");
        assert!(started.elapsed() >= grace, "moved before the grace ended");
        controller.abort();
        beat.abort();
        provider.consensus_node().shutdown().await.unwrap();
    }

    /// A replicated stream ownership change parks forever because automatic
    /// recovery does not handle streams. The controller flags it, an operator
    /// can withdraw it and then accept losing the old owner.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn operator_resolves_a_stuck_stream_recovery() {
        let router = InProcessRouter::new();
        let raft_node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        raft_node
            .initialize(BTreeMap::from([(1u64, ganglion_openraft::openraft::BasicNode::new("n1"))]))
            .await
            .unwrap();
        raft_node.wait_for_leader(1, Duration::from_secs(10)).await.unwrap();
        let provider = GanglionCoordination::new("a-owner", raft_node);
        let info = |id: &str| NodeInfo {
            node_id: id.into(),
            broker_addr: "127.0.0.1:9000".into(),
            admin_addr: None,
        };
        for id in ["a-owner", "b-follower", "c-spare"] {
            provider.register_self(&info(id)).await.unwrap();
        }
        provider
            .declare_stream("ticks", 1, STREAM_DURABILITY_DURABLE, Default::default(), Some(1))
            .await
            .unwrap();
        let stream = StreamIdentity::new("ticks", Partition::new(0));
        provider.register_stream(&stream).await.unwrap();
        let iterate = |live: HashMap<String, NodeInfo>| {
            let provider = &provider;
            async move {
                provider
                    .control_iteration(
                        &fibril_broker::coordination::DeterministicPartitionPlacement,
                        &[],
                        &DeterministicStreamPlacement,
                        &provider.registered_streams(),
                        1,
                        1,
                        ReplicationDurabilityPolicy::LocalDurable,
                        &live,
                        8,
                    )
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        let all: HashMap<_, _> = ["a-owner", "b-follower", "c-spare"]
            .into_iter()
            .map(|id| (id.to_string(), info(id)))
            .collect();
        let first = iterate(all.clone()).await.stream_assignment_for("ticks", Partition::new(0)).unwrap().clone();
        assert_eq!(first.owner, "a-owner");
        let mut survivors = all.clone();
        survivors.remove("a-owner");
        iterate(survivors.clone()).await;
        let resource = to_ganglion_stream_resource(&stream);
        let snapshot = provider.consensus_node().committed_snapshot();
        assert!(snapshot.attributes.contains_key(&promotion::pending_recovery_key(&resource)));
        let attention = operator_override::recovery_attention(&snapshot, |n| survivors.contains_key(n));
        assert_eq!(attention.len(), 1);
        assert!(attention[0].reason.contains("unsupported for automatic resolution"));

        // A pending request blocks accepting loss until it is withdrawn.
        assert!(provider.accept_owner_loss(&resource).await.is_err());
        provider.withdraw_pending_recovery(&resource).await.unwrap();
        assert!(provider.pending_recoveries().unwrap().is_empty());
        assert!(provider.withdraw_pending_recovery(&resource).await.is_err());
        provider.accept_owner_loss(&resource).await.unwrap();
        let moved = iterate(survivors).await.stream_assignment_for("ticks", Partition::new(0)).unwrap().clone();
        assert_eq!(moved.owner, "b-follower");
        assert_eq!(moved.epoch, first.epoch + 1);
        assert!(provider.pending_recoveries().unwrap().is_empty());
        let snapshot = provider.consensus_node().committed_snapshot();
        assert!(!snapshot.attributes.contains_key(&operator_override::accepted_owner_loss_key(&resource)));

        // A request against an accepted queue history may already have
        // sealed replicas and is never withdrawn by hand.
        let mut sealed = promotion::PendingRecovery {
            version: 1,
            resource_incarnation: None,
            previous_activation: Some([1; 32]),
            requested_generation: snapshot.generation,
            previous: snapshot.assignments[&resource].clone(),
            proposed: snapshot.assignments[&resource].clone(),
            previous_write_nodes: 2,
            proposed_write_nodes: 2,
            required_old_witnesses: 1,
        };
        sealed.proposed.epoch += 1;
        let mut with_sealed = snapshot.clone();
        with_sealed
            .attributes
            .insert(promotion::pending_recovery_key(&resource), serde_json::to_string(&sealed).unwrap());
        provider
            .consensus_node()
            .write_snapshot_guarded(snapshot.generation, with_sealed)
            .await
            .unwrap();
        let refused = provider.withdraw_pending_recovery(&resource).await.unwrap_err();
        assert!(refused.to_string().contains("cannot be withdrawn"), "{refused}");
        assert!(!provider.pending_recoveries().unwrap().is_empty());

        provider.consensus_node().shutdown().await.unwrap();
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
            broker_addr: format!("127.0.0.1:{port}"),
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

        // The watch/trait surface sees the registrations too. It updates
        // through the spawned watch forwarder, so wait bounded rather than
        // assuming instant propagation.
        let mut watch = provider.watch();
        tokio::time::timeout(Duration::from_secs(10), async {
            while watch.borrow_and_update().nodes.len() < 2 {
                watch.changed().await.expect("watch open");
            }
        })
        .await
        .expect("watch view caught up with both registrations");
        assert_eq!(provider.snapshot().nodes.len(), 2);

        provider
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
        .consensus_node()
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

        coordination
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
            // adoption_generation is stamped (to Some) at the cutover, so match the
            // transition shape and require it to be set rather than a fixed value.
            if part.as_ref().map(|p| p.partition_count) == Some(4)
                && registered.is_superset(&std::collections::HashSet::from([2, 3]))
                && marker.as_ref().map(|m| (m.version, m.n_old, m.n_new)) == Some((1, 2, 4))
                && marker
                    .as_ref()
                    .is_some_and(|m| m.adoption_generation.is_some())
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

        coordination
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shrink_queue_bumps_version_and_marks_transition() {
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
        let coordination = GanglionCoordination::new("broker-a", raft_node);
        coordination
            .declare_queue_partitioning("orders", None, 4)
            .await
            .expect("declare");

        // Shrink 4 -> 2 (a factor): count drops, version bumps, marker recorded.
        let shrunk = coordination
            .shrink_queue("orders", None, 2)
            .await
            .expect("shrink");
        assert_eq!(shrunk.partition_count, 2);
        assert_eq!(shrunk.partitioning_version, 1);

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let part = coordination.queue_partitioning("orders", None);
            let marker = coordination.repartition_transition("orders", None);
            // adoption_generation is stamped (to Some) at the cutover, so match the
            // transition shape and require it to be set rather than a fixed value.
            if part.as_ref().map(|p| p.partition_count) == Some(2)
                && marker.as_ref().map(|m| (m.version, m.n_old, m.n_new)) == Some((1, 4, 2))
                && marker
                    .as_ref()
                    .is_some_and(|m| m.adoption_generation.is_some())
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "shrink never became fully visible"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // Idempotent at the target; non-factor and growth are rejected.
        assert_eq!(
            coordination
                .shrink_queue("orders", None, 2)
                .await
                .expect("idempotent shrink")
                .partition_count,
            2
        );
        // 3 does not divide the current count (2); growth via shrink is rejected.
        assert!(matches!(
            coordination.shrink_queue("orders", None, 3).await,
            Err(RepartitionQueueError::NotIntegerFactorShrink { .. })
        ));
        assert!(matches!(
            coordination.shrink_queue("orders", None, 4).await,
            Err(RepartitionQueueError::NotIntegerFactorShrink { .. })
        ));

        coordination
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
            adoption_generation: None,
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
            if coordination
                .repartition_transition("orders", None)
                .is_none()
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

        coordination
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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

        coordination
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
            broker_addr: "127.0.0.1:9000".to_string(),
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

        coordination
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
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
                    broker_addr: format!("127.0.0.1:{port}"),
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

        coordination
            .consensus_node()
            .shutdown()
            .await
            .expect("shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn runtime_meta_carries_visibility_and_forwarding_counts() {
        let router = InProcessRouter::new();
        let node = RaftMetadataNode::start(1, default_raft_config().unwrap(), &router)
            .await
            .unwrap();
        node.initialize(BTreeMap::from([(
            1,
            ganglion_openraft::openraft::BasicNode::new("one"),
        )]))
        .await
        .unwrap();
        node.wait_for_leader(1, Duration::from_secs(10))
            .await
            .unwrap();
        let provider = GanglionCoordination::new("public-a", node);
        let mut state = ganglion_core::CoordinationSnapshot {
            generation: 1,
            ..Default::default()
        };
        let mut private = ganglion_core::NodeInfo::new("private-p", "10.0.0.9:9876", None);
        private.labels.insert(VERSION_LABEL.into(), "0.5.0".into());
        private
            .labels
            .insert(VISIBILITY_LABEL.into(), PRIVATE_VISIBILITY.into());
        private
            .labels
            .insert(FORWARDED_SESSIONS_LABEL.into(), "2".into());
        state.nodes.insert("private-p".into(), private);
        let mut public = ganglion_core::NodeInfo::new("public-a", "10.0.0.1:9876", None);
        public.labels.insert(VERSION_LABEL.into(), "0.5.0".into());
        public
            .labels
            .insert(UPSTREAM_SESSIONS_LABEL.into(), "3".into());
        state.nodes.insert("public-a".into(), public);
        let mut idle = ganglion_core::NodeInfo::new("public-b", "10.0.0.2:9876", None);
        idle.labels.insert(VERSION_LABEL.into(), "0.5.0".into());
        state.nodes.insert("public-b".into(), idle);
        provider
            .consensus_node()
            .write_snapshot(state)
            .await
            .unwrap();

        let meta = provider.node_runtime_meta();
        assert_eq!(meta["private-p"]["visibility"], "private");
        assert_eq!(meta["private-p"]["forwarded_sessions"], 2);
        assert_eq!(meta["private-p"]["upstream_sessions"], 0);
        assert_eq!(meta["public-a"]["visibility"], "public");
        assert_eq!(meta["public-a"]["upstream_sessions"], 3);
        assert_eq!(meta["public-a"]["forwarded_sessions"], 0);
        // An absent label reads as zero, so idle brokers need not send it.
        assert_eq!(meta["public-b"]["visibility"], "public");
        assert_eq!(meta["public-b"]["upstream_sessions"], 0);
        provider.consensus_node().shutdown().await.unwrap();
    }
}
