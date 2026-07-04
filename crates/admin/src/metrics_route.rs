//! The GET /metrics endpoint: Prometheus text exposition of node-level
//! counters and gauges. A scrape reads atomic counters and the same
//! snapshot views the dashboard uses, never a delivery hot path.

use axum::{
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

use crate::prometheus::{MetricFamily, MetricKind, Sample, render};
use crate::routes::check_auth;
use crate::server::AdminServer;

pub async fn metrics(
    State(server): State<Arc<AdminServer>>,
    headers: axum::http::HeaderMap,
) -> Result<Response, StatusCode> {
    check_auth(&server, &headers).await?;

    let body = render(&node_families(&server));
    Ok((
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
        .into_response())
}

/// Node-level families: broker message totals, storage operation totals,
/// transport and session-resume counters, and open connection gauges.
/// Counters are process-lifetime monotonic and reset on restart.
fn node_families(server: &AdminServer) -> Vec<MetricFamily> {
    let broker = server.metrics.broker().snapshot();
    let storage = server.metrics.storage().snapshot();
    let tcp = server.metrics.tcp().snapshot();
    let connections = server.metrics.connections();

    let counter = |name, help, value: u64| {
        MetricFamily::scalar(name, help, MetricKind::Counter, value as f64)
    };
    let gauge = |name, help, value: usize| {
        MetricFamily::scalar(name, help, MetricKind::Gauge, value as f64)
    };

    vec![
        counter(
            "fibril_broker_published_total",
            "Messages accepted for publish since process start.",
            broker.total_published,
        ),
        counter(
            "fibril_broker_delivered_total",
            "Messages delivered to consumers.",
            broker.total_delivered,
        ),
        counter(
            "fibril_broker_acked_total",
            "Messages settled by acknowledgement.",
            broker.total_acked,
        ),
        counter(
            "fibril_broker_redelivered_total",
            "Redeliveries after lease expiry or explicit retry.",
            broker.total_redelivered,
        ),
        counter(
            "fibril_broker_expired_total",
            "Messages dropped by TTL expiry.",
            broker.total_expired,
        ),
        counter(
            "fibril_storage_reads_total",
            "Storage read operations.",
            storage.total_reads,
        ),
        counter(
            "fibril_storage_writes_total",
            "Storage write operations.",
            storage.total_writes,
        ),
        counter(
            "fibril_tcp_connections_total",
            "Broker connections accepted.",
            tcp.total_connections,
        ),
        counter(
            "fibril_tcp_bytes_in_total",
            "Bytes read from broker connections.",
            tcp.total_bytes_in,
        ),
        counter(
            "fibril_tcp_bytes_out_total",
            "Bytes written to broker connections.",
            tcp.total_bytes_out,
        ),
        counter(
            "fibril_tcp_errors_total",
            "Broker connection errors.",
            tcp.errors_total,
        ),
        counter(
            "fibril_tcp_resume_new_total",
            "Sessions started fresh without a resume attempt.",
            tcp.resume_new_total,
        ),
        counter(
            "fibril_tcp_resume_accepted_total",
            "Session resume attempts accepted.",
            tcp.resume_accepted_total,
        ),
        counter(
            "fibril_tcp_resume_rejected_total",
            "Session resume attempts rejected.",
            tcp.resume_rejected_total,
        ),
        counter(
            "fibril_tcp_reconnect_grace_entered_total",
            "Disconnected sessions that entered the reconnect grace window.",
            tcp.reconnect_grace_entered_total,
        ),
        counter(
            "fibril_tcp_reconnect_grace_expired_total",
            "Reconnect grace windows that expired unclaimed.",
            tcp.reconnect_grace_expired_total,
        ),
        counter(
            "fibril_tcp_reconcile_requests_total",
            "Subscription reconcile requests received on session resume.",
            tcp.reconcile_requests_total,
        ),
        MetricFamily {
            name: "fibril_tcp_reconcile_outcomes_total",
            help: "Subscription reconcile outcomes on session resume, by outcome.",
            kind: MetricKind::Counter,
            samples: vec![
                reconcile_outcome("kept", tcp.reconcile_kept_total),
                reconcile_outcome("restored", tcp.reconcile_restored_total),
                reconcile_outcome("client_closed", tcp.reconcile_client_closed_total),
                reconcile_outcome("server_dropped", tcp.reconcile_server_dropped_total),
                reconcile_outcome("mismatched", tcp.reconcile_mismatched_total),
                reconcile_outcome("restore_failed", tcp.reconcile_restore_failed_total),
            ],
        },
        gauge(
            "fibril_tcp_connections_open",
            "Currently open broker connections.",
            connections.open_connections(),
        ),
        gauge(
            "fibril_subscriptions_open",
            "Currently open subscriptions.",
            connections.open_subscriptions(),
        ),
    ]
}

fn reconcile_outcome(outcome: &'static str, value: u64) -> Sample {
    Sample::labeled(vec![("outcome", outcome.to_string())], value as f64)
}
