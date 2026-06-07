use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use fibril_broker::queue_engine::{
    DLQDiscardPolicyWire, DeclareMeta, GlobalDLQ, GlobalDlqSnapshot, GlobalDlqUpdateOutcome,
    InspectMode, MessageHeaders, QueueInspectionState, StromaError,
};
use fibril_broker::runtime_settings::{
    RuntimeSettings, RuntimeSettingsError, RuntimeSettingsLoadIssue, RuntimeSettingsLocks,
    RuntimeSettingsSnapshot, RuntimeSettingsUpdateOutcome,
};
use fibril_metrics::{BrokerStatsSnapshot, StorageStatsSnapshot, SystemSnapshot};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::check_admin_auth;
use crate::server::AdminServer;

#[derive(Serialize)]
pub struct OverviewResponse {
    pub broker: BrokerStatsSnapshot,
    pub storage: StorageStatsSnapshot,
    pub sys: SystemSnapshot,
    pub storage_used: u64,
}

#[derive(Serialize)]
pub struct RuntimeSettingsResponse {
    pub version: u64,
    pub settings: RuntimeSettings,
    pub locks: RuntimeSettingsLocks,
    pub load_issue: Option<RuntimeSettingsLoadIssue>,
}

#[derive(Deserialize)]
pub struct UpdateRuntimeSettingsRequest {
    pub expected_version: u64,
    pub settings: RuntimeSettings,
}

#[derive(Deserialize)]
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
pub struct QueueDlqTargetRequest {
    pub tp: String,
    pub group: Option<String>,
}

#[derive(Deserialize)]
pub struct UpdateQueueDlqRequest {
    pub tp: String,
    pub group: Option<String>,
    pub policy: QueueDlqPolicyRequest,
    pub target: Option<QueueDlqTargetRequest>,
    pub max_retries: Option<u32>,
}

#[derive(Serialize)]
pub struct QueueDlqResponse {
    pub status: &'static str,
}

#[derive(Deserialize)]
pub struct InspectMessagesQuery {
    pub topic: String,
    pub group: Option<String>,
    #[serde(default)]
    pub from: u64,
    pub limit: Option<usize>,
    #[serde(default)]
    pub include_settled: bool,
    #[serde(default)]
    pub include_payload: bool,
    pub payload_limit_bytes: Option<usize>,
}

#[derive(Serialize)]
pub struct InspectMessagesResponse {
    pub next_offset_hint: u64,
    pub limit: usize,
    pub warning: &'static str,
    pub items: Vec<InspectMessageItemResponse>,
}

#[derive(Serialize)]
pub struct InspectMessageItemResponse {
    pub state: QueueInspectionState,
    pub headers: Option<MessageHeaders>,
    pub payload_len: Option<usize>,
    pub payload_base64: Option<String>,
    pub payload_truncated: bool,
    pub missing_payload: bool,
}

#[derive(Serialize)]
pub struct AdminErrorResponse {
    pub code: String,
    pub message: String,
}

const MESSAGE_INSPECTION_DEFAULT_LIMIT: usize = 50;
const MESSAGE_INSPECTION_MAX_LIMIT: usize = 500;
const MESSAGE_INSPECTION_DEFAULT_PAYLOAD_LIMIT_BYTES: usize = 4096;
const MESSAGE_INSPECTION_MAX_PAYLOAD_LIMIT_BYTES: usize = 64 * 1024;

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

async fn check_auth(
    server: &AdminServer,
    headers: &axum::http::HeaderMap,
) -> Result<(), StatusCode> {
    check_admin_auth(headers, &server.config.auth, &server.sessions).await
}

pub async fn overview(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<OverviewResponse>, StatusCode> {
    check_auth(&server, &headers).await?;

    Ok(Json(OverviewResponse {
        broker: server.metrics.broker().snapshot(),
        storage: server.metrics.storage().snapshot(),
        sys: server.metrics.system().snapshot(),
        storage_used: server
            .storage
            .estimate_disk_used()
            .await
            .unwrap_or_default(),
    }))
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
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_auth(&server, &headers).await?;

    let queues = server.storage.queue_stats_snapshot().await;

    if let Ok(queues) = queues {
        Ok(Json(serde_json::to_value(queues).unwrap_or_default()))
    } else {
        Ok(Json(serde_json::json!({})))
    }
}

pub async fn queues_debug(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_auth(&server, &headers).await?;

    let queues = server.storage.debug_snapshot().await;

    if let Ok(queues) = queues {
        Ok(Json(serde_json::to_value(queues).unwrap_or_default()))
    } else if let Err(_err) = queues {
        tracing::error!("Error fetching queue debug info: {_err}");
        Err(StatusCode::INTERNAL_SERVER_ERROR)
    } else {
        Ok(Json(serde_json::json!({})))
    }
}

pub async fn inspect_messages(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Query(query): Query<InspectMessagesQuery>,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

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
            0,
            query.group.as_deref(),
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
                warning: "Message inspection reads persisted message data and queue state. Use it for debugging and operations, not as a live polling view.",
                items: page
                    .items
                    .into_iter()
                    .map(|item| InspectMessageItemResponse {
                        state: item.state,
                        headers: item.headers,
                        payload_len: item.payload_len,
                        payload_base64: item.payload.map(|payload| STANDARD.encode(payload)),
                        payload_truncated: item.payload_truncated,
                        missing_payload: item.missing_payload,
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

pub async fn runtime_settings(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<RuntimeSettingsResponse>, StatusCode> {
    check_auth(&server, &headers).await?;

    let Some(runtime_settings) = &server.runtime_settings else {
        return Err(StatusCode::NOT_FOUND);
    };

    Ok(Json(RuntimeSettingsResponse::new(
        runtime_settings.current(),
        runtime_settings.locks().clone(),
        runtime_settings.load_issue(),
    )))
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

pub async fn global_dlq(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<GlobalDlqSnapshot>, StatusCode> {
    check_auth(&server, &headers).await?;

    server.storage.global_dlq().await.map(Json).map_err(|err| {
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
        Some(target) => match GlobalDLQ::new(&target.tp, 0, target.group.as_deref()).await {
            Ok(target) => Some(target),
            Err(err) => {
                return Ok(admin_error(
                    StatusCode::BAD_REQUEST,
                    "invalid_global_dlq",
                    err.to_string(),
                ));
            }
        },
        None => None,
    };

    match server
        .storage
        .set_global_dlq(target, request.expected_version)
        .await
    {
        Ok(GlobalDlqUpdateOutcome::Stored(snapshot)) => {
            Ok((StatusCode::OK, Json(snapshot)).into_response())
        }
        Ok(GlobalDlqUpdateOutcome::Conflict(snapshot)) => {
            Ok((StatusCode::CONFLICT, Json(snapshot)).into_response())
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
            let target = match GlobalDLQ::new(&target.tp, 0, target.group.as_deref()).await {
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
    };

    match server
        .storage
        .declare_queue(&request.tp, 0, request.group.as_deref(), meta)
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
