use axum::{Json, extract::State, http::StatusCode, response::IntoResponse, response::Response};
use fibril_broker::queue_engine::{
    GlobalDLQ, GlobalDlqSnapshot, GlobalDlqUpdateOutcome, StromaError,
};
use fibril_broker::runtime_settings::{
    RuntimeSettings, RuntimeSettingsError, RuntimeSettingsLocks, RuntimeSettingsSnapshot,
    RuntimeSettingsUpdateOutcome,
};
use fibril_metrics::{BrokerStatsSnapshot, StorageStatsSnapshot, SystemSnapshot};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::check_basic_auth;
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
}

#[derive(Deserialize)]
pub struct UpdateRuntimeSettingsRequest {
    pub expected_version: u64,
    pub settings: RuntimeSettings,
}

#[derive(Deserialize)]
pub struct UpdateGlobalDlqRequest {
    pub expected_version: u64,
    pub target: Option<GlobalDLQ>,
}

#[derive(Serialize)]
pub struct AdminErrorResponse {
    pub code: String,
    pub message: String,
}

impl RuntimeSettingsResponse {
    fn new(snapshot: RuntimeSettingsSnapshot, locks: RuntimeSettingsLocks) -> Self {
        Self {
            version: snapshot.version,
            settings: snapshot.settings,
            locks,
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

pub async fn overview(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<OverviewResponse>, StatusCode> {
    check_basic_auth(&headers, &server.config.auth).await?;

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
    check_basic_auth(&headers, &server.config.auth).await?;

    Ok(Json(server.metrics.connections().snapshot()))
}

pub async fn subscriptions(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_basic_auth(&headers, &server.config.auth).await?;

    Ok(Json(server.metrics.connections().snapshot_subs()))
}

pub async fn queues(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    check_basic_auth(&headers, &server.config.auth).await?;

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
    check_basic_auth(&headers, &server.config.auth).await?;

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

pub async fn runtime_settings(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<RuntimeSettingsResponse>, StatusCode> {
    check_basic_auth(&headers, &server.config.auth).await?;

    let Some(runtime_settings) = &server.runtime_settings else {
        return Err(StatusCode::NOT_FOUND);
    };

    Ok(Json(RuntimeSettingsResponse::new(
        runtime_settings.current(),
        runtime_settings.locks().clone(),
    )))
}

pub async fn update_runtime_settings(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
    Json(request): Json<UpdateRuntimeSettingsRequest>,
) -> Result<Response, StatusCode> {
    check_basic_auth(&headers, &server.config.auth).await?;

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
            Json(RuntimeSettingsResponse::new(snapshot, locks)),
        )
            .into_response()),
        Ok(RuntimeSettingsUpdateOutcome::Conflict(snapshot)) => Ok((
            StatusCode::CONFLICT,
            Json(RuntimeSettingsResponse::new(snapshot, locks)),
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
    check_basic_auth(&headers, &server.config.auth).await?;

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
    check_basic_auth(&headers, &server.config.auth).await?;

    match server
        .storage
        .set_global_dlq(request.target, request.expected_version)
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
