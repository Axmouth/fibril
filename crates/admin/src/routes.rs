use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use fibril_broker::PartitionKind;
use fibril_broker::coordination::ReplicationDurabilityPolicy;
use fibril_broker::queue_engine::{
    DLQDiscardPolicyWire, DeclareMeta, DestroyOutcome, GlobalDLQ, GlobalDlqSnapshot,
    GlobalDlqUpdateOutcome, InspectMode, MessageHeaders, MessageInspectionStatus,
    QueueInspectionState, RecoveryMismatchPolicy, RetentionConfig, StromaError,
};
use fibril_broker::runtime_settings::{
    RuntimeSettings, RuntimeSettingsError, RuntimeSettingsLoadIssue, RuntimeSettingsLocks,
    RuntimeSettingsSnapshot, RuntimeSettingsUpdateOutcome,
};
use fibril_broker::stream::StreamDurability;
use fibril_metrics::{BrokerStatsSnapshot, StorageStatsSnapshot, SystemSnapshot, TcpStatsSnapshot};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::check_admin_auth;
use crate::server::{AdminServer, RuntimeSettingsClusterUpdateOutcome};

#[derive(Serialize)]
pub struct OverviewResponse {
    pub broker: BrokerStatsSnapshot,
    pub storage: StorageStatsSnapshot,
    pub tcp: TcpStatsSnapshot,
    pub stroma: serde_json::Value,
    pub sys: SystemSnapshot,
    pub storage_used: u64,
    /// Per-queue disk footprint (largest first), the segments behind
    /// `storage_used`.
    pub storage_breakdown: Vec<fibril_broker::queue_engine::DiskUsedBreakdownEntry>,
}

#[derive(Serialize)]
pub struct RuntimeSettingsResponse {
    pub version: u64,
    pub settings: RuntimeSettings,
    pub locks: RuntimeSettingsLocks,
    pub load_issue: Option<RuntimeSettingsLoadIssue>,
}

/// Unknown fields in the ENVELOPE are rejected (a typo should be named, not
/// ignored), but the inner RuntimeSettings stays permissive on purpose: the
/// settings document is cluster-replicated and must tolerate fields written
/// by newer broker versions.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateRuntimeSettingsRequest {
    pub expected_version: u64,
    pub settings: RuntimeSettings,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateGlobalDlqRequest {
    pub expected_version: u64,
    pub target: Option<QueueDlqTargetRequest>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueueDlqPolicyRequest {
    Discard,
    Global,
    Custom,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueDlqTargetRequest {
    pub topic: String,
    pub group: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateQueueDlqRequest {
    pub topic: String,
    pub group: Option<String>,
    pub policy: QueueDlqPolicyRequest,
    pub target: Option<QueueDlqTargetRequest>,
    pub max_retries: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateQueueRequest {
    pub topic: String,
    pub group: Option<String>,
    /// Partition count for the new queue (defaults to 1).
    pub partition_count: Option<u32>,
    /// Optional dead-letter policy. Absent leaves the queue without one.
    pub policy: Option<QueueDlqPolicyRequest>,
    pub target: Option<QueueDlqTargetRequest>,
    pub max_retries: Option<u32>,
    /// Optional per-queue default message TTL (ms): messages without their own
    /// TTL drop after this age. Not queue expiration.
    pub default_message_ttl_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteQueueRequest {
    pub topic: String,
    pub group: Option<String>,
    /// Partition count to tear down (defaults to 1). Partitions 0..count are
    /// destroyed.
    pub partition_count: Option<u32>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReplayDeadLettersRequest {
    pub dlq_topic: String,
    pub dlq_group: Option<String>,
    pub offsets: Vec<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AddCoordinationVotingMemberRequest {
    pub id: u64,
    pub addr: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoveCoordinationVotingMemberRequest {
    pub id: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepartitionQueueRequest {
    pub topic: String,
    #[serde(default)]
    pub group: Option<String>,
    pub partition_count: u32,
}

#[derive(Serialize)]
pub struct QueueDlqResponse {
    pub status: &'static str,
}

/// Admin-API view of the global dead-letter snapshot. The storage layer spells
/// the topic `tp` (a persisted field name); the HTTP API presents it as `topic`
/// like every other admin surface, so this view translates rather than leaking
/// the internal name.
#[derive(Serialize)]
pub struct GlobalDlqTargetView {
    pub topic: String,
    pub part: u32,
    pub group: Option<String>,
}

#[derive(Serialize)]
pub struct GlobalDlqView {
    pub version: u64,
    pub target: Option<GlobalDlqTargetView>,
}

impl From<GlobalDlqSnapshot> for GlobalDlqView {
    fn from(snapshot: GlobalDlqSnapshot) -> Self {
        GlobalDlqView {
            version: snapshot.version,
            target: snapshot.target.map(|t| GlobalDlqTargetView {
                topic: t.tp,
                part: t.part,
                group: t.group,
            }),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InspectMessagesQuery {
    pub topic: String,
    pub group: Option<String>,
    /// Which partition to read (offsets are per partition). Defaults to 0,
    /// which was previously the only partition the endpoint could see.
    #[serde(default)]
    pub partition: u32,
    #[serde(default)]
    pub from: u64,
    pub limit: Option<usize>,
    #[serde(default)]
    pub include_settled: bool,
    #[serde(default)]
    pub include_payload: bool,
    pub payload_limit_bytes: Option<usize>,
    pub status: Option<String>,
}

#[derive(Serialize)]
pub struct InspectMessagesResponse {
    pub next_offset_hint: u64,
    pub limit: usize,
    pub items: Vec<InspectMessageItemResponse>,
}

#[derive(Serialize)]
pub struct InspectMessageItemResponse {
    pub state: QueueInspectionState,
    pub headers: Option<MessageHeaders>,
    pub payload_len: Option<usize>,
    pub payload_base64: Option<String>,
    pub payload_truncated: bool,
}

#[derive(Serialize)]
pub struct AdminErrorResponse {
    pub code: String,
    pub message: String,
}

const MESSAGE_INSPECTION_DEFAULT_LIMIT: usize = 50;
const MESSAGE_INSPECTION_MAX_LIMIT: usize = 5_000;
const MESSAGE_INSPECTION_DEFAULT_PAYLOAD_LIMIT_BYTES: usize = 4096;
const MESSAGE_INSPECTION_MAX_PAYLOAD_LIMIT_BYTES: usize = 1024 * 1024;
const DLQ_REPLAY_MAX_OFFSETS: usize = 100;

fn normalize_group(group: Option<String>) -> Option<String> {
    group.and_then(|group| {
        let group = group.trim().to_string();
        if group.is_empty() || group == "default" {
            None
        } else {
            Some(group)
        }
    })
}

fn parse_status_filter(
    status: Option<String>,
) -> Result<Option<Vec<MessageInspectionStatus>>, String> {
    let Some(status) = status else {
        return Ok(None);
    };
    let mut filters = Vec::new();
    for raw in status.split(',') {
        let raw = raw.trim();
        if raw.is_empty() {
            continue;
        }
        let status = match raw {
            "ready" => MessageInspectionStatus::Ready,
            "inflight" => MessageInspectionStatus::Inflight,
            "delayed" => MessageInspectionStatus::Delayed,
            "pending_dlq" => MessageInspectionStatus::PendingDlq,
            "settled" => MessageInspectionStatus::Settled,
            other => return Err(format!("unknown message status filter: {other}")),
        };
        if !filters.contains(&status) {
            filters.push(status);
        }
    }
    Ok((!filters.is_empty()).then_some(filters))
}

impl RuntimeSettingsResponse {
    fn new(
        snapshot: RuntimeSettingsSnapshot,
        locks: RuntimeSettingsLocks,
        load_issue: Option<RuntimeSettingsLoadIssue>,
    ) -> Self {
        Self {
            version: snapshot.version,
            settings: snapshot.settings,
            locks,
            load_issue,
        }
    }
}

/// Request body for declaring a Plexus stream from the admin UI.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateStreamRequest {
    pub topic: String,
    pub partition_count: Option<u32>,
    /// "ephemeral" | "speculative" | "durable" (defaults to durable).
    pub durability: Option<String>,
    pub max_age_ms: Option<u64>,
    pub max_bytes: Option<u64>,
    pub max_records: Option<u64>,
}

/// GET /admin/api/streams: stream channels this broker hosts, with per-partition
/// position plus declared durability and retention.
pub async fn streams(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_auth(&server, &headers).await?;
    let Some(streams) = &server.streams else {
        return Ok(Json(serde_json::json!({ "streams": [] })));
    };
    let stats = streams.stream_stats().await;
    Ok(Json(
        serde_json::json!({ "streams": serde_json::to_value(stats).unwrap_or_default() }),
    ))
}

/// POST /admin/api/streams: declare a Plexus stream.
pub async fn create_stream(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CreateStreamRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    if request.topic.trim().is_empty() {
        return Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_topic",
            "topic must not be empty",
        ));
    }
    let Some(streams) = &server.streams else {
        return Ok(admin_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "streams_unavailable",
            "this broker does not host streams",
        ));
    };

    let durability = match request.durability.as_deref() {
        None | Some("durable") => StreamDurability::Durable,
        Some("speculative") => StreamDurability::Speculative,
        Some("ephemeral") => StreamDurability::Ephemeral,
        Some(other) => {
            return Ok(admin_error(
                StatusCode::BAD_REQUEST,
                "invalid_durability",
                format!("unknown durability tier `{other}`"),
            ));
        }
    };

    let retention = if request.max_age_ms.is_some()
        || request.max_bytes.is_some()
        || request.max_records.is_some()
    {
        Some(RetentionConfig {
            max_age_ms: request.max_age_ms,
            max_bytes: request.max_bytes,
            max_records: request.max_records,
        })
    } else {
        None
    };

    let partition_count = request.partition_count.unwrap_or(1).max(1);
    // Same two-step as queues: coordination first in cluster mode, so the
    // stream's config and every partition land in the catalogue.
    let partition_count = match &server.declare_stream_coordinator {
        Some(coordinator) => {
            match coordinator(
                request.topic.clone(),
                partition_count,
                durability.as_u8(),
                retention.clone(),
            )
            .await
            {
                Ok(count) => count,
                Err(message) => {
                    return Ok(admin_error(
                        StatusCode::CONFLICT,
                        "declare_conflict",
                        message,
                    ));
                }
            }
        }
        None => partition_count,
    };
    match streams
        .declare_stream(&request.topic, partition_count, durability, retention)
        .await
    {
        Ok(()) => {
            server.audit.record(
                "stream_declared",
                "info",
                request.topic.clone(),
                format!("Stream declared with {partition_count} partitions"),
            );
            Ok(Json(serde_json::json!({ "status": "created" })).into_response())
        }
        Err(StromaError::InvalidArgument(message)) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            message,
        )),
        Err(err) => {
            tracing::error!("create stream failed: {err}");
            Ok(admin_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "create_stream_failed",
                "create stream failed",
            ))
        }
    }
}

fn admin_error(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(AdminErrorResponse {
            code: code.into(),
            message: message.into(),
        }),
    )
        .into_response()
}

fn locked_status() -> StatusCode {
    StatusCode::from_u16(423).unwrap_or(StatusCode::CONFLICT)
}

pub(crate) async fn check_auth(
    server: &AdminServer,
    headers: &axum::http::HeaderMap,
) -> Result<(), StatusCode> {
    check_admin_auth(headers, &server.config.auth, &server.sessions).await
}

fn durability_policy_json(policy: ReplicationDurabilityPolicy) -> serde_json::Value {
    match policy {
        ReplicationDurabilityPolicy::LocalDurable => {
            serde_json::json!({ "mode": "local_durable" })
        }
        ReplicationDurabilityPolicy::ReplicaAccepted { nodes } => {
            serde_json::json!({ "mode": "replica_accepted", "nodes": nodes })
        }
        ReplicationDurabilityPolicy::ReplicaDurable { nodes } => {
            serde_json::json!({ "mode": "replica_durable", "nodes": nodes })
        }
        ReplicationDurabilityPolicy::MajorityDurable => {
            serde_json::json!({ "mode": "majority_durable" })
        }
    }
}

pub async fn overview(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<OverviewResponse>, StatusCode> {
    check_auth(&server, &headers).await?;

    Ok(Json(overview_payload(&server).await))
}

/// The overview data bundle, shared by the GET route and the events stream.
pub(crate) async fn overview_payload(server: &AdminServer) -> OverviewResponse {
    // One walk serves both the total and its per-queue segments.
    let storage_breakdown = server
        .storage
        .estimate_disk_used_breakdown()
        .await
        .unwrap_or_default();
    let storage_used = storage_breakdown
        .iter()
        .map(|entry| entry.message_bytes + entry.event_bytes)
        .sum();

    OverviewResponse {
        broker: server.metrics.broker().snapshot(),
        storage: server.metrics.storage().snapshot(),
        tcp: server.metrics.tcp().snapshot(),
        stroma: stroma_overview(server),
        sys: server.metrics.system().snapshot(),
        storage_used,
        storage_breakdown,
    }
}

fn stroma_overview(server: &AdminServer) -> serde_json::Value {
    let command = server.stroma_metrics.command_snapshot();
    let log = server.stroma_metrics.log_snapshot();
    let snapshot = server.stroma_metrics.snapshot.snapshot();
    let recovery = server.stroma_metrics.recovery.snapshot();
    let depths = server.stroma_metrics.cmd_queue_depths_snapshot();

    serde_json::json!({
        "command": command,
        "log": log,
        "snapshot": snapshot,
        "recovery": recovery,
        "queue_depths": depths,
    })
}

pub async fn connections(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_auth(&server, &headers).await?;

    Ok(Json(server.metrics.connections().snapshot()))
}

pub async fn subscriptions(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_auth(&server, &headers).await?;

    Ok(Json(server.metrics.connections().snapshot_subs()))
}

pub async fn queues(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    let queues = match server.storage.queue_stats_snapshot().await {
        Ok(queues) => queues,
        Err(err) => {
            tracing::error!("Error fetching queue stats: {err}");
            return Ok(admin_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "queue_stats_failed",
                err.to_string(),
            ));
        }
    };

    Ok(Json(serde_json::to_value(queues).unwrap_or_default()).into_response())
}

/// Build the partition debug payload for one channel kind. A stream reuses the
/// queue handle, so [`debug_snapshot`](fibril_broker::queue_engine::QueueEngine::debug_snapshot)
/// returns both kinds; this filters to one (`keep_streams` selects which),
/// recomputes the counts, then merges the same broker observability. Queues and
/// streams get a symmetric shape so each admin page reads exactly its resource.
pub(crate) async fn partition_debug_value(
    server: &AdminServer,
    keep_streams: bool,
) -> Result<serde_json::Value, Response> {
    let snapshot = match server.storage.debug_snapshot().await {
        Ok(snapshot) => snapshot,
        Err(err) => {
            tracing::error!("Error fetching partition debug info: {err}");
            return Err(admin_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "debug_snapshot_failed",
                err.to_string(),
            ));
        }
    };

    let mut snapshot = snapshot;
    snapshot
        .queues
        .retain(|q| matches!(q.kind, PartitionKind::Stream) == keep_streams);
    snapshot.queue_count = snapshot.queues.len();
    snapshot.materialized_queue_count = snapshot.queues.iter().filter(|q| q.materialized).count();

    let mut value = serde_json::to_value(snapshot).unwrap_or_default();
    if let Some(observability) = &server.broker_queue_observability
        && let Some(object) = value.as_object_mut()
    {
        let observability = observability();
        if let Some(queues) = observability.get("queues") {
            object.insert("broker_activity".into(), queues.clone());
        } else {
            object.insert("broker_activity".into(), observability.clone());
        }
        if let Some(summary) = observability.get("summary") {
            object.insert("broker_activity_summary".into(), summary.clone());
        }
        if let Some(replication_followers) = observability.get("replication_followers") {
            object.insert(
                "replication_followers".into(),
                replication_followers.clone(),
            );
        }
        if let Some(replication_summary) = observability.get("replication_summary") {
            object.insert("replication_summary".into(), replication_summary.clone());
        }
        if let Some(owned_replicas) = observability.get("owned_replicas") {
            object.insert("owned_replicas".into(), owned_replicas.clone());
        }
        if let Some(owned_replica_summary) = observability.get("owned_replica_summary") {
            object.insert(
                "owned_replica_summary".into(),
                owned_replica_summary.clone(),
            );
        }
        if let Some(replication_timing) = observability.get("replication_timing") {
            object.insert("replication_timing".into(), replication_timing.clone());
        }
        object.insert(
            "broker_cleanup_metrics".into(),
            serde_json::to_value(server.metrics.broker().snapshot().queue_cleanup)
                .unwrap_or_default(),
        );
    }
    Ok(value)
}

pub async fn queues_debug(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;
    match partition_debug_value(&server, false).await {
        Ok(value) => Ok(Json(value).into_response()),
        Err(response) => Ok(response),
    }
}

/// Stream counterpart to [`queues_debug`]: the same debug snapshot filtered to
/// Plexus stream partitions (role, epoch, applied offsets, replication state).
pub async fn streams_debug(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;
    match partition_debug_value(&server, true).await {
        Ok(value) => Ok(Json(value).into_response()),
        Err(response) => Ok(response),
    }
}

pub async fn inspect_messages(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<InspectMessagesQuery>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;
    let group = normalize_group(query.group);
    let status_filter = match parse_status_filter(query.status) {
        Ok(filter) => filter,
        Err(err) => {
            return Ok(admin_error(
                StatusCode::BAD_REQUEST,
                "invalid_message_status_filter",
                err,
            ));
        }
    };

    let limit = query
        .limit
        .unwrap_or(MESSAGE_INSPECTION_DEFAULT_LIMIT)
        .clamp(1, MESSAGE_INSPECTION_MAX_LIMIT);
    let payload_limit_bytes = query
        .payload_limit_bytes
        .unwrap_or(MESSAGE_INSPECTION_DEFAULT_PAYLOAD_LIMIT_BYTES)
        .min(MESSAGE_INSPECTION_MAX_PAYLOAD_LIMIT_BYTES);
    let mode = if query.include_settled {
        InspectMode::IncludeSettled
    } else {
        InspectMode::ActiveOnly
    };

    match server
        .storage
        .inspect_messages(
            &query.topic,
            query.partition,
            group.as_deref(),
            query.from,
            limit,
            mode,
            query.include_payload,
            payload_limit_bytes,
        )
        .await
    {
        Ok(page) => Ok((
            StatusCode::OK,
            Json(InspectMessagesResponse {
                next_offset_hint: page.next_offset_hint,
                limit,
                items: page
                    .items
                    .into_iter()
                    .filter(|item| {
                        status_filter
                            .as_ref()
                            .is_none_or(|filter| filter.contains(&item.state.status))
                    })
                    .map(|item| InspectMessageItemResponse {
                        state: item.state,
                        headers: item.headers,
                        payload_len: item.payload_len,
                        payload_base64: item.payload.map(|payload| STANDARD.encode(payload)),
                        payload_truncated: item.payload_truncated,
                    })
                    .collect(),
            }),
        )
            .into_response()),
        Err(err @ StromaError::InvalidArgument(_)) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_message_inspection_request",
            err.to_string(),
        )),
        Err(err) => {
            tracing::error!("message inspection failed: {err}");
            Ok(admin_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "message_inspection_failed",
                "message inspection failed",
            ))
        }
    }
}

/// Cluster topology: the committed coordination snapshot (nodes, assignments
/// with owner/followers/epoch, generation) plus an optional consensus-internals
/// block when an embedded provider is active. This JSON is the contract for
/// `fibrilctl topology` and the admin diagram.
pub async fn topology(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_auth(&server, &headers).await?;

    let coordination = match &server.coordination {
        Some(coordination) => {
            let snapshot = coordination.snapshot();
            // Registration survives a crash; the heartbeat does not. The
            // liveness provider separates the two so the diagram can show a
            // dead broker instead of silently pretending it is fine.
            let live = server.liveness.as_ref().map(|provider| provider());
            let raft_ids = server
                .node_raft_ids
                .as_ref()
                .map(|provider| provider())
                .unwrap_or_default();
            let rates = server
                .node_rates
                .as_ref()
                .map(|provider| provider())
                .unwrap_or_default();
            let mut nodes: Vec<serde_json::Value> = snapshot
                .nodes
                .values()
                .map(|node| {
                    serde_json::json!({
                        "node_id": node.node_id,
                        "broker_addr": node.broker_addr.to_string(),
                        "admin_addr": node.admin_addr.map(|addr| addr.to_string()),
                        "live": live
                            .as_ref()
                            .is_none_or(|set| set.contains(&node.node_id)),
                        "raft_id": raft_ids.get(&node.node_id),
                        "rate_pub": rates.get(&node.node_id).map(|rate| rate.0),
                        "rate_dlv": rates.get(&node.node_id).map(|rate| rate.1),
                    })
                })
                .collect();
            nodes.sort_by(|a, b| a["node_id"].as_str().cmp(&b["node_id"].as_str()));

            let mut assignments: Vec<serde_json::Value> = snapshot
                .assignments
                .values()
                .map(|assignment| {
                    serde_json::json!({
                        "topic": assignment.queue.topic,
                        "partition": assignment.queue.partition,
                        "group": assignment.queue.group,
                        "owner": assignment.owner,
                        "followers": assignment.followers,
                        "epoch": assignment.epoch,
                        "durability": durability_policy_json(assignment.durability),
                    })
                })
                .collect();
            assignments.sort_by(|a, b| {
                (a["topic"].as_str(), a["partition"].as_u64())
                    .cmp(&(b["topic"].as_str(), b["partition"].as_u64()))
            });

            let mut stream_assignments: Vec<serde_json::Value> = snapshot
                .stream_assignments
                .values()
                .map(|assignment| {
                    serde_json::json!({
                        "topic": assignment.stream.topic,
                        "partition": assignment.stream.partition,
                        "owner": assignment.owner,
                        "followers": assignment.followers,
                        "epoch": assignment.epoch,
                    })
                })
                .collect();
            stream_assignments.sort_by(|a, b| {
                (a["topic"].as_str(), a["partition"].as_u64())
                    .cmp(&(b["topic"].as_str(), b["partition"].as_u64()))
            });

            serde_json::json!({
                "node_id": coordination.node_id(),
                "generation": snapshot.generation,
                "nodes": nodes,
                "assignments": assignments,
                "stream_assignments": stream_assignments,
            })
        }
        None => serde_json::Value::Null,
    };

    let consensus = match &server.consensus_topology {
        Some(provider) => provider(),
        None => serde_json::Value::Null,
    };

    Ok(Json(serde_json::json!({
        "coordination": coordination,
        "consensus": consensus,
    })))
}

/// This broker's local exclusive-cohort view (per-cohort members and targets).
/// Cohort assignment is broker-local runtime state, so this is node-scoped.
pub async fn cohorts(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_auth(&server, &headers).await?;

    let cohorts = match &server.cohorts {
        Some(provider) => provider(),
        None => serde_json::Value::Null,
    };
    Ok(Json(serde_json::json!({ "cohorts": cohorts })))
}

pub async fn add_coordination_voting_member(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<AddCoordinationVotingMemberRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    if request.addr.parse::<std::net::SocketAddr>().is_err() {
        return Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_coordination_member_addr",
            "addr must be a socket address",
        ));
    }

    let Some(manager) = &server.coordination_membership else {
        return Ok(admin_error(
            StatusCode::NOT_FOUND,
            "coordination_membership_unavailable",
            "coordination membership management is not available",
        ));
    };

    match manager.add_voting_member(request.id, request.addr).await {
        Ok(coordination) => {
            Ok(Json(serde_json::json!({ "coordination": coordination })).into_response())
        }
        Err(error) => Ok(admin_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "coordination_membership_update_failed",
            error,
        )),
    }
}

pub async fn remove_coordination_voting_member(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<RemoveCoordinationVotingMemberRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    let Some(manager) = &server.coordination_membership else {
        return Ok(admin_error(
            StatusCode::NOT_FOUND,
            "coordination_membership_unavailable",
            "coordination membership management is not available",
        ));
    };

    match manager.remove_voting_member(request.id).await {
        Ok(coordination) => {
            Ok(Json(serde_json::json!({ "coordination": coordination })).into_response())
        }
        Err(error) => Ok(admin_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "coordination_membership_update_failed",
            error,
        )),
    }
}

/// Trigger a live repartition (grow or shrink) of a queue to `partition_count`.
pub async fn repartition_queue(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<RepartitionQueueRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    if request.partition_count == 0 {
        return Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_partition_count",
            "partition_count must be at least 1",
        ));
    }

    let Some(manager) = &server.queue_repartition else {
        return Ok(admin_error(
            StatusCode::NOT_FOUND,
            "repartition_unavailable",
            "live repartitioning is not available (requires ganglion coordination)",
        ));
    };

    match manager
        .repartition(
            request.topic,
            normalize_group(request.group),
            request.partition_count,
        )
        .await
    {
        Ok(partitioning) => {
            Ok(Json(serde_json::json!({ "partitioning": partitioning })).into_response())
        }
        Err(error) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "repartition_failed",
            error,
        )),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DrainRequest {
    /// How long the broker will hold sessions before it stops serving, so
    /// clients know their window to settle and reconnect. Defaults to 30s.
    #[serde(default = "default_drain_grace_ms")]
    pub grace_ms: u64,
    /// Human-readable reason surfaced to clients and logs.
    #[serde(default = "default_drain_message")]
    pub message: String,
}

fn default_drain_grace_ms() -> u64 {
    30_000
}

fn default_drain_message() -> String {
    "broker draining for a planned restart".to_string()
}

/// POST /admin/api/drain: announce a planned restart or upgrade to connected
/// clients so they settle in-flight work and reconnect. Does not stop the
/// broker; the operator restarts it after the grace window.
/// Re-read the TLS certificate and key from their configured paths and
/// serve the new pair to subsequent handshakes. Invalid material is
/// rejected with the old material still serving.
pub async fn reload_tls(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;
    let Some(reload) = &server.tls_reload else {
        return Ok(admin_error(
            StatusCode::NOT_FOUND,
            "tls_reload_unavailable",
            "TLS is not enabled on this broker",
        ));
    };
    match reload() {
        Ok(fingerprint) => Ok(Json(serde_json::json!({
            "reloaded": true,
            "leaf_sha256": fingerprint,
        }))
        .into_response()),
        Err(message) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "tls_reload_rejected",
            message,
        )),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestPublishRequest {
    pub topic: String,
    #[serde(default)]
    pub group: Option<String>,
    /// Text body of the test message, published with a text content type.
    pub text: String,
}

pub async fn publish_test_message(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<TestPublishRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    let Some(publisher) = &server.test_publisher else {
        return Ok(admin_error(
            StatusCode::NOT_FOUND,
            "publish_unavailable",
            "test publish is not available on this broker",
        ));
    };

    let topic = request.topic.trim();
    if topic.is_empty() {
        return Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "topic_required",
            "topic must not be empty",
        ));
    }
    let group = request
        .group
        .as_deref()
        .map(str::trim)
        .filter(|g| !g.is_empty());

    match publisher.publish_text(topic, group, request.text).await {
        Ok(outcome) => {
            server.audit.record(
                "test_publish",
                "info",
                topic.to_string(),
                format!("Operator test message confirmed on partition {}", outcome.partition),
            );
            Ok(Json(serde_json::to_value(&outcome).unwrap_or_default()).into_response())
        }
        Err(message) => Ok(admin_error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "publish_failed",
            message,
        )),
    }
}

pub async fn drain(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<DrainRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    let Some(controller) = &server.drain else {
        return Ok(admin_error(
            StatusCode::NOT_FOUND,
            "drain_unavailable",
            "drain is not available on this broker",
        ));
    };

    let message = request.message.clone();
    let outcome = controller
        .announce_drain(request.grace_ms, request.message)
        .await;
    server.audit.record(
        "drain",
        "warning",
        message,
        format!(
            "Drain announced to {} connections, {} partitions still owned",
            outcome.connections_notified, outcome.owned_partitions_remaining
        ),
    );
    Ok(Json(serde_json::to_value(&outcome).unwrap_or_default()).into_response())
}

pub async fn runtime_settings(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    let Some(runtime_settings) = &server.runtime_settings else {
        return Err(StatusCode::NOT_FOUND);
    };

    let snapshot = match &server.runtime_settings_cluster {
        Some(cluster) => match cluster.current_runtime_settings().await {
            Ok(Some(snapshot)) => snapshot,
            Ok(None) => RuntimeSettingsSnapshot {
                version: 0,
                settings: runtime_settings.current().settings,
            },
            Err(error) => {
                tracing::warn!(%error, "cluster runtime settings read failed");
                return Ok(admin_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cluster_runtime_settings_unavailable",
                    "cluster runtime settings are unavailable",
                ));
            }
        },
        None => runtime_settings.current(),
    };

    Ok(Json(RuntimeSettingsResponse::new(
        snapshot,
        runtime_settings.locks().clone(),
        runtime_settings.load_issue(),
    )))
    .map(IntoResponse::into_response)
}

pub async fn startup_config(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<crate::server::StartupConfigSummary>, StatusCode> {
    check_auth(&server, &headers).await?;

    server
        .startup_config
        .clone()
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

pub async fn update_runtime_settings(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<UpdateRuntimeSettingsRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    let Some(runtime_settings) = &server.runtime_settings else {
        return Err(StatusCode::NOT_FOUND);
    };

    let locks = runtime_settings.locks().clone();
    if let Some(cluster) = &server.runtime_settings_cluster {
        if let Err(err) = request.settings.validate() {
            return Ok(match err {
                RuntimeSettingsError::Invalid(message) => {
                    admin_error(StatusCode::BAD_REQUEST, "invalid_runtime_settings", message)
                }
                other => {
                    tracing::error!("runtime settings validation failed unexpectedly: {other}");
                    admin_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "runtime_settings_update_failed",
                        "runtime settings update failed",
                    )
                }
            });
        }

        match cluster
            .update_runtime_settings(request.expected_version, request.settings)
            .await
        {
            Ok(RuntimeSettingsClusterUpdateOutcome::Stored(snapshot)) => {
                if let Err(error) = runtime_settings
                    .apply_cluster_settings(snapshot.settings.clone())
                    .await
                {
                    tracing::warn!(
                        %error,
                        cluster_version = snapshot.version,
                        "cluster runtime settings committed but local cache update failed"
                    );
                }
                return Ok((
                    StatusCode::OK,
                    Json(RuntimeSettingsResponse::new(
                        snapshot,
                        locks,
                        runtime_settings.load_issue(),
                    )),
                )
                    .into_response());
            }
            Ok(RuntimeSettingsClusterUpdateOutcome::Conflict(snapshot)) => {
                return Ok((
                    StatusCode::CONFLICT,
                    Json(RuntimeSettingsResponse::new(
                        snapshot,
                        locks,
                        runtime_settings.load_issue(),
                    )),
                )
                    .into_response());
            }
            Err(error) => {
                tracing::warn!(%error, "cluster runtime settings update failed");
                return Ok(admin_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cluster_runtime_settings_update_failed",
                    "cluster runtime settings update failed",
                ));
            }
        }
    }

    match runtime_settings
        .update(request.expected_version, request.settings)
        .await
    {
        Ok(RuntimeSettingsUpdateOutcome::Stored(snapshot)) => Ok((
            StatusCode::OK,
            Json(RuntimeSettingsResponse::new(
                snapshot,
                locks,
                runtime_settings.load_issue(),
            )),
        )
            .into_response()),
        Ok(RuntimeSettingsUpdateOutcome::Conflict(snapshot)) => Ok((
            StatusCode::CONFLICT,
            Json(RuntimeSettingsResponse::new(
                snapshot,
                locks,
                runtime_settings.load_issue(),
            )),
        )
            .into_response()),
        Err(RuntimeSettingsError::Invalid(err)) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_runtime_settings",
            err,
        )),
        Err(RuntimeSettingsError::Locked(err)) => {
            Ok(admin_error(locked_status(), "setting_locked", err))
        }
        Err(err) => {
            tracing::error!("runtime settings update failed: {err}");
            Ok(admin_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "runtime_settings_update_failed",
                "runtime settings update failed",
            ))
        }
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpsertUserRequest {
    pub username: String,
    pub password: String,
}

pub async fn list_users(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Vec<crate::server::AdminUserInfo>>, StatusCode> {
    check_auth(&server, &headers).await?;
    let Some(users) = &server.users else {
        return Err(StatusCode::NOT_FOUND);
    };
    users.list_users().await.map(Json).map_err(|err| {
        tracing::error!("user listing failed: {err}");
        StatusCode::INTERNAL_SERVER_ERROR
    })
}

pub async fn upsert_user(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<UpsertUserRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;
    let Some(users) = &server.users else {
        return Err(StatusCode::NOT_FOUND);
    };
    if request.password.trim().is_empty() {
        return Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_user",
            "password must not be empty",
        ));
    }
    match users
        .upsert_user(&request.username, &request.password)
        .await
    {
        Ok(()) => Ok((
            StatusCode::OK,
            Json(serde_json::json!({ "status": "ok", "username": request.username })),
        )
            .into_response()),
        Err(message) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "user_update_failed",
            message,
        )),
    }
}

pub async fn remove_user(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    axum::extract::Path(username): axum::extract::Path<String>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;
    let Some(users) = &server.users else {
        return Err(StatusCode::NOT_FOUND);
    };
    match users.remove_user(&username).await {
        Ok(()) => Ok((
            StatusCode::OK,
            Json(serde_json::json!({ "status": "removed", "username": username })),
        )
            .into_response()),
        Err(message) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "user_remove_failed",
            message,
        )),
    }
}

pub async fn global_dlq(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<GlobalDlqView>, StatusCode> {
    check_auth(&server, &headers).await?;

    server
        .storage
        .global_dlq()
        .await
        .map(|snapshot| Json(GlobalDlqView::from(snapshot)))
        .map_err(|err| {
            tracing::error!("global dlq fetch failed: {err}");
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

pub async fn update_global_dlq(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<UpdateGlobalDlqRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    let target = match request.target {
        Some(target) => {
            let group = normalize_group(target.group);
            match GlobalDLQ::new(&target.topic, 0, group.as_deref()).await {
                Ok(target) => Some(target),
                Err(err) => {
                    return Ok(admin_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_global_dlq",
                        err.to_string(),
                    ));
                }
            }
        }
        None => None,
    };

    match server
        .storage
        .set_global_dlq(target, request.expected_version)
        .await
    {
        Ok(GlobalDlqUpdateOutcome::Stored(snapshot)) => {
            Ok((StatusCode::OK, Json(GlobalDlqView::from(snapshot))).into_response())
        }
        Ok(GlobalDlqUpdateOutcome::Conflict(snapshot)) => {
            Ok((StatusCode::CONFLICT, Json(GlobalDlqView::from(snapshot))).into_response())
        }
        Err(err @ StromaError::InvalidArgument(_)) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_global_dlq",
            err.to_string(),
        )),
        Err(err) => {
            tracing::error!("global dlq update failed: {err}");
            Ok(admin_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "global_dlq_update_failed",
                "global dlq update failed",
            ))
        }
    }
}

pub async fn update_queue_dlq(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<UpdateQueueDlqRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    let policy = match request.policy {
        QueueDlqPolicyRequest::Discard => DLQDiscardPolicyWire::Discard,
        QueueDlqPolicyRequest::Global => DLQDiscardPolicyWire::GlobalDQL,
        QueueDlqPolicyRequest::Custom => {
            let Some(target) = request.target else {
                return Ok(admin_error(
                    StatusCode::BAD_REQUEST,
                    "missing_queue_dlq_target",
                    "custom queue DLQ policy requires a target",
                ));
            };
            let target_group = normalize_group(target.group);
            let target = match GlobalDLQ::new(&target.topic, 0, target_group.as_deref()).await {
                Ok(target) => target,
                Err(err) => {
                    return Ok(admin_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_queue_dlq_target",
                        err.to_string(),
                    ));
                }
            };
            DLQDiscardPolicyWire::CustomDQL {
                tp: target.tp.into_boxed_str(),
                part: target.part,
                group: target.group.map(String::into_boxed_str),
            }
        }
    };

    let meta = DeclareMeta {
        dlq_policy: Some(policy),
        dlq_max_retries: request.max_retries,
        default_message_ttl_ms: None,
    };

    let group = normalize_group(request.group);
    match server
        .storage
        .declare_queue(&request.topic, 0, group.as_deref(), meta)
        .await
    {
        Ok(()) => Ok((StatusCode::OK, Json(QueueDlqResponse { status: "stored" })).into_response()),
        Err(err @ StromaError::InvalidArgument(_)) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_queue_dlq",
            err.to_string(),
        )),
        Err(err) => {
            tracing::error!("queue DLQ update failed: {err}");
            Ok(admin_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "queue_dlq_update_failed",
                "queue DLQ update failed",
            ))
        }
    }
}

/// Create a queue from the admin UI. Declares partitions 0..partition_count
/// locally with an optional dead-letter policy. In cluster mode the catalogue
/// sync loop then registers them with coordination and the controller spreads
/// ownership - the same path the client DeclareQueue handler uses (which also
/// declares locally per partition). partition_count defaults to 1.
///
/// First cut: does not yet go through declare_partitioning (authoritative count
/// + conflict detection). See the queue-lifecycle plan in FOLLOWUPS.md.
pub async fn create_queue(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<CreateQueueRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    if request.topic.trim().is_empty() {
        return Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_topic",
            "topic must not be empty",
        ));
    }
    let partition_count = request.partition_count.unwrap_or(1).max(1);

    // Optional dead-letter policy (same mapping as update_queue_dlq).
    let dlq_policy = match &request.policy {
        None => None,
        Some(QueueDlqPolicyRequest::Discard) => Some(DLQDiscardPolicyWire::Discard),
        Some(QueueDlqPolicyRequest::Global) => Some(DLQDiscardPolicyWire::GlobalDQL),
        Some(QueueDlqPolicyRequest::Custom) => {
            let Some(target) = &request.target else {
                return Ok(admin_error(
                    StatusCode::BAD_REQUEST,
                    "missing_queue_dlq_target",
                    "custom queue DLQ policy requires a target",
                ));
            };
            let target_group = normalize_group(target.group.clone());
            let target = match GlobalDLQ::new(&target.topic, 0, target_group.as_deref()).await {
                Ok(target) => target,
                Err(err) => {
                    return Ok(admin_error(
                        StatusCode::BAD_REQUEST,
                        "invalid_queue_dlq_target",
                        err.to_string(),
                    ));
                }
            };
            Some(DLQDiscardPolicyWire::CustomDQL {
                tp: target.tp.into_boxed_str(),
                part: target.part,
                group: target.group.map(String::into_boxed_str),
            })
        }
    };

    let meta = DeclareMeta {
        dlq_policy,
        dlq_max_retries: request.max_retries,
        default_message_ttl_ms: request.default_message_ttl_ms,
    };

    let group = normalize_group(request.group.clone());
    // Cluster mode records the partitioning with coordination first - the
    // returned count is authoritative and the controller places every
    // partition. Standalone materializes the requested count directly.
    let partition_count = match &server.declare_queue_coordinator {
        Some(coordinator) => {
            match coordinator(request.topic.clone(), group.clone(), partition_count).await {
                Ok(count) => count,
                Err(message) => {
                    return Ok(admin_error(
                        StatusCode::CONFLICT,
                        "declare_conflict",
                        message,
                    ));
                }
            }
        }
        None => partition_count,
    };
    for partition in 0..partition_count {
        match server
            .storage
            .declare_queue(&request.topic, partition, group.as_deref(), meta.clone())
            .await
        {
            Ok(()) => {}
            Err(err @ StromaError::InvalidArgument(_)) => {
                return Ok(admin_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_queue",
                    err.to_string(),
                ));
            }
            Err(err) => {
                tracing::error!("create queue failed: {err}");
                return Ok(admin_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "create_queue_failed",
                    "create queue failed",
                ));
            }
        }
    }

    server.audit.record(
        "queue_declared",
        "info",
        request.topic.clone(),
        format!("Queue declared with {partition_count} partitions"),
    );
    Ok((
        StatusCode::CREATED,
        Json(serde_json::json!({
            "status": "created",
            "partition_count": partition_count,
        })),
    )
        .into_response())
}

/// Delete a queue from the admin UI. Destroys partitions 0..partition_count
/// locally (drops each from the registry AND deletes its on-disk storage).
/// partition_count defaults to 1.
///
/// Single-node only. In cluster mode this is refused (501) because a real
/// teardown has to deregister the queue from coordination and destroy every
/// replica, otherwise the catalogue-sync loop re-materializes it. That
/// coordinated path is the multi-node follow-up in FOLLOWUPS.md.
///
/// A partition with inflight (leased, un-acked) work is not destroyed - the
/// whole request returns 409 so callers drain or wait for lease expiry first.
pub async fn delete_queue(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<DeleteQueueRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    if request.topic.trim().is_empty() {
        return Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_topic",
            "topic must not be empty",
        ));
    }

    if server.cluster_mode {
        return Ok(admin_error(
            StatusCode::NOT_IMPLEMENTED,
            "cluster_delete_unsupported",
            "deleting a queue in cluster mode is not supported yet (coordinated teardown is pending)",
        ));
    }

    let partition_count = request.partition_count.unwrap_or(1).max(1);
    let group = normalize_group(request.group.clone());

    for partition in 0..partition_count {
        match server
            .storage
            .destroy_partition(&request.topic, partition, group.as_deref())
            .await
        {
            Ok(DestroyOutcome::Destroyed) => {}
            Ok(DestroyOutcome::HasInflight) => {
                return Ok(admin_error(
                    StatusCode::CONFLICT,
                    "queue_has_inflight",
                    format!(
                        "partition {partition} still has inflight work - drain or wait for lease expiry before deleting"
                    ),
                ));
            }
            Err(err @ StromaError::InvalidArgument(_)) => {
                return Ok(admin_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_queue",
                    err.to_string(),
                ));
            }
            Err(err) => {
                tracing::error!("delete queue failed: {err}");
                return Ok(admin_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "delete_queue_failed",
                    "delete queue failed",
                ));
            }
        }
    }

    server.audit.record(
        "queue_deleted",
        "warning",
        request.topic.clone(),
        format!("Queue deleted ({partition_count} partitions)"),
    );
    Ok((
        StatusCode::OK,
        Json(serde_json::json!({
            "status": "deleted",
            "partition_count": partition_count,
        })),
    )
        .into_response())
}

pub async fn replay_dead_letters(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<ReplayDeadLettersRequest>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    if request.offsets.is_empty() {
        return Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "missing_dlq_replay_offsets",
            "provide at least one DLQ offset to replay",
        ));
    }
    if request.offsets.len() > DLQ_REPLAY_MAX_OFFSETS {
        return Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "too_many_dlq_replay_offsets",
            format!("DLQ replay accepts at most {DLQ_REPLAY_MAX_OFFSETS} offsets per request"),
        ));
    }

    let dlq_group = normalize_group(request.dlq_group);
    match server
        .storage
        .replay_dead_letters(&request.dlq_topic, dlq_group.as_deref(), &request.offsets)
        .await
    {
        Ok(report) => Ok((StatusCode::OK, Json(report)).into_response()),
        Err(err @ StromaError::InvalidArgument(_)) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_dlq_replay_request",
            err.to_string(),
        )),
        Err(err) => {
            tracing::error!("DLQ replay failed: {err}");
            Ok(admin_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "dlq_replay_failed",
                "DLQ replay failed",
            ))
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepairPartitionQuery {
    pub topic: String,
    #[serde(default)]
    pub partition: u32,
    pub group: Option<String>,
}

/// List partitions quarantined by recovery (a dangling event->message reference)
/// plus the active policy, so the admin UI can show "X must be resolved before
/// this queue continues" and offer a repair.
/// `GET /admin/api/tls`: presentation metadata for the served certificate
/// (fingerprint, validity window, subject), or null when TLS is not enabled.
pub async fn tls_info(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_auth(&server, &headers).await?;
    let info = match &server.cert_info {
        Some(provider) => provider(),
        None => serde_json::Value::Null,
    };
    Ok(Json(info))
}

pub async fn quarantine(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_auth(&server, &headers).await?;
    let quarantined = server.storage.quarantined_partitions();
    Ok(Json(serde_json::json!({
        "policy": format!("{:?}", server.storage.recovery_mismatch_policy()),
        "quarantined": quarantined,
    })))
}

/// Repair a quarantined partition (truncate-to-valid) and clear it. The next
/// access re-recovers the valid prefix. Operator-triggered.
pub async fn repair_partition(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<RepairPartitionQuery>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;
    let group = normalize_group(query.group);
    match server
        .storage
        .repair_partition(&query.topic, query.partition, group.as_deref())
        .await
    {
        Ok(()) => Ok(Json(serde_json::json!({
            "repaired": true,
            "topic": query.topic,
            "partition": query.partition,
        }))
        .into_response()),
        Err(err) => Ok(admin_error(
            StatusCode::BAD_REQUEST,
            "repair_failed",
            err.to_string(),
        )),
    }
}

/// Readiness probe. A quarantined partition means something needs an operator.
/// Under the Quarantine policy the broker keeps serving the healthy partitions,
/// so it stays ready (200) but reports `degraded`; under Refuse it fails
/// readiness (503) so the problem cannot be missed.
pub async fn readyz(State(server): State<Arc<AdminServer>>) -> Response {
    let quarantined = server.storage.quarantined_partitions();
    if quarantined.is_empty() {
        return (StatusCode::OK, "ok").into_response();
    }
    let ready = !matches!(
        server.storage.recovery_mismatch_policy(),
        RecoveryMismatchPolicy::Refuse
    );
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(serde_json::json!({
            "ready": ready,
            "degraded": true,
            "quarantined": quarantined,
        })),
    )
        .into_response()
}
