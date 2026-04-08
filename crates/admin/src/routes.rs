use axum::{Json, extract::State, http::StatusCode};
use fibril_metrics::{BrokerStatsSnapshot, StorageStatsSnapshot, SystemSnapshot};
use fibril_storage::{AppendReceiptExt, Offset};
use serde::Serialize;
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

    let queues = server.metrics.broker().call_queue_state_callback().await;

    if let Some(queues) = queues {
        Ok(Json(serde_json::to_value(queues).unwrap_or_default()))
    } else {
        Ok(Json(serde_json::json!({})))
    }
}
