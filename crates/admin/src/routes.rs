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

pub async fn overview<O>(
    State(server): State<Arc<AdminServer<O>>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<OverviewResponse>, StatusCode>
where
    O: AppendReceiptExt<Offset> + 'static,
{
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

pub async fn connections<O>(
    State(server): State<Arc<AdminServer<O>>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    O: AppendReceiptExt<Offset> + 'static,
{
    check_basic_auth(&headers, &server.config.auth).await?;

    Ok(Json(server.metrics.connections().snapshot()))
}

pub async fn subscriptions<O>(
    State(server): State<Arc<AdminServer<O>>>,
    headers: axum::http::HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode>
where
    O: AppendReceiptExt<Offset> + 'static,
{
    check_basic_auth(&headers, &server.config.auth).await?;

    Ok(Json(server.metrics.connections().snapshot_subs()))
}
