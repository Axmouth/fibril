use axum::{Json, extract::State, http::StatusCode, response::IntoResponse, response::Response};
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

#[derive(Serialize)]
pub struct AdminErrorResponse {
    pub error: String,
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
        Err(err @ (RuntimeSettingsError::Invalid(_) | RuntimeSettingsError::Locked(_))) => Ok((
            StatusCode::BAD_REQUEST,
            Json(AdminErrorResponse {
                error: err.to_string(),
            }),
        )
            .into_response()),
        Err(err) => {
            tracing::error!("runtime settings update failed: {err}");
            Ok((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(AdminErrorResponse {
                    error: "runtime settings update failed".into(),
                }),
            )
                .into_response())
        }
    }
}
