use askama::Template;
use axum::{
    Router,
    extract::Path,
    response::{Html, IntoResponse},
    routing::get,
};
use fibril_broker::{
    StromaMetrics, queue_engine::QueueEngine, runtime_settings::RuntimeSettingsManager,
};
use fibril_util::StaticAuthHandler;
use http::header;
use rust_embed::RustEmbed;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::services::ServeDir;

use crate::routes;
use fibril_metrics::Metrics;

pub struct AdminConfig {
    // TODO: better type, parse earlier
    pub bind: String,
    pub auth: Option<StaticAuthHandler>,
}

pub struct AdminServer {
    pub metrics: Metrics,
    pub stroma_metrics: Arc<StromaMetrics>,
    pub config: AdminConfig,
    pub storage: Arc<dyn QueueEngine + Send + Sync>,
    pub runtime_settings: Option<Arc<RuntimeSettingsManager>>,
}

fn render<T: Template>(tpl: T) -> Html<String> {
    match tpl.render() {
        Ok(v) => Html(v),
        Err(e) => {
            tracing::error!("template render error: {e}");
            Html("<h1>500 - template error</h1>".into())
        }
    }
}

impl AdminServer {
    pub fn new(
        metrics: Metrics,
        stroma_metrics: Arc<StromaMetrics>,
        config: AdminConfig,
        storage: Arc<dyn QueueEngine + Send + Sync>,
        runtime_settings: Option<Arc<RuntimeSettingsManager>>,
    ) -> Self {
        Self {
            metrics,
            stroma_metrics,
            config,
            storage,
            runtime_settings,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let state = Arc::new(self);

        let app = Self::router(state.clone());

        let listener = TcpListener::bind(&state.config.bind).await?;
        print_admin_banner(&state.config.bind, state.config.auth.is_some());
        tracing::info!("listening on {}", state.config.bind);
        axum::serve(listener, app).await?;
        Ok(())
    }

    pub(crate) fn router(state: Arc<Self>) -> Router {
        let mut app = Router::new();

        #[cfg(debug_assertions)]
        {
            // DEV: serve the whole admin-ui folder from disk
            app = app.nest_service("/static", ServeDir::new("crates/admin/admin-ui"));
        }

        #[cfg(not(debug_assertions))]
        {
            // RELEASE: serve embedded assets
            app = app.route("/static/{*file}", get(admin_static));
        }

        let app = app
            .route("/", get(overview_page))
            .route("/admin/connections", get(connections_page))
            .route("/admin/subscriptions", get(subscriptions_page))
            .route("/admin/queues", get(queues_page))
            .route("/admin/settings", get(settings_page))
            .route("/admin/api/overview", get(routes::overview))
            .route("/admin/api/connections", get(routes::connections))
            .route("/admin/api/subscriptions", get(routes::subscriptions))
            .route("/admin/api/queues", get(routes::queues))
            .route("/admin/api/queues_debug", get(routes::queues_debug))
            .route(
                "/admin/api/runtime-settings",
                get(routes::runtime_settings).put(routes::update_runtime_settings),
            )
            .route("/healthz", get(|| async { "ok" }))
            .fallback(not_found)
            .with_state(state);
        app
    }
}

#[derive(RustEmbed)]
#[folder = "admin-ui"]
struct AdminAssets;

async fn admin_static(Path(path): Path<String>) -> impl IntoResponse {
    let path = path.trim_start_matches('/');
    if let Some(file) = AdminAssets::get(path) {
        let mime = mime_guess::from_path(path).first_or_octet_stream();
        return ([(header::CONTENT_TYPE, mime.as_ref())], file.data).into_response();
    }
    tracing::debug!("admin static not found: {path}");

    not_found().await.into_response()
}

async fn not_found() -> impl IntoResponse {
    render(NotFound {
        page: "404",
        title: "Not Found",
    })
}

async fn overview_page() -> impl IntoResponse {
    render(OverviewPage {
        page: "dashboard",
        title: "Overview",
    })
}

async fn connections_page() -> impl IntoResponse {
    render(Connections {
        page: "connections",
        title: "Connections",
    })
}

async fn subscriptions_page() -> impl IntoResponse {
    render(Subscriptions {
        page: "subscriptions",
        title: "Subscriptions",
    })
}

async fn queues_page() -> impl IntoResponse {
    render(Queues {
        page: "queues",
        title: "Queues",
    })
}

async fn settings_page() -> impl IntoResponse {
    render(Settings {
        page: "settings",
        title: "Settings",
    })
}

#[derive(Template)]
#[template(path = "pages/overview.html")]
struct OverviewPage {
    page: &'static str,
    title: &'static str,
}

#[derive(Template)]
#[template(path = "pages/connections.html")]
struct Connections {
    page: &'static str,
    title: &'static str,
}

#[derive(Template)]
#[template(path = "pages/subscriptions.html")]
struct Subscriptions {
    page: &'static str,
    title: &'static str,
}

#[derive(Template)]
#[template(path = "pages/queues.html")]
struct Queues {
    page: &'static str,
    title: &'static str,
}

#[derive(Template)]
#[template(path = "pages/settings.html")]
struct Settings {
    page: &'static str,
    title: &'static str,
}

#[derive(Template)]
#[template(path = "pages/404.html")]
struct NotFound {
    page: &'static str,
    title: &'static str,
}

pub fn print_admin_banner(bind: &str, auth: bool) {
    let auth = if auth { "enabled " } else { "disabled" };

    tracing::info!(
        r#"
                                                
┌──────────────────────────────────────────────┐
│            Fibril Admin Console              │
├──────────────────────────────────────────────┤
│  Web UI        : http://{bind:<20} │
│  Mode          : internal / operator         │
│  Auth          : {auth:<27} │
└──────────────────────────────────────────────┘
                                                
"#,
        bind = bind
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use fibril_broker::{
        queue_engine::{KeratinConfig, QueueEngine, SnapshotConfig, StromaEngine},
        runtime_settings::{
            RuntimeSettings, RuntimeSettingsLocks, RuntimeSettingsManager,
            RuntimeSettingsUpdateOutcome,
        },
    };
    use fibril_metrics::Metrics;
    use serde_json::json;
    use tower::ServiceExt;

    async fn test_server(locks: RuntimeSettingsLocks) -> Arc<AdminServer> {
        let root = std::env::temp_dir().join(format!("fibril-admin-{}", fastrand::u64(..)));
        std::fs::create_dir_all(&root).unwrap();
        let engine = StromaEngine::open(
            &root,
            KeratinConfig::test_default(),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let runtime_settings = RuntimeSettingsManager::load_from_stroma_engine(
            &engine,
            RuntimeSettings::default(),
            locks,
        )
        .await
        .unwrap();
        Arc::new(AdminServer::new(
            Metrics::new(60),
            engine.metrics(),
            AdminConfig {
                bind: "127.0.0.1:0".into(),
                auth: None,
            },
            Arc::new(engine),
            Some(Arc::new(runtime_settings)),
        ))
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn runtime_settings_get_returns_current_settings() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/runtime-settings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["version"], 1);
        assert_eq!(body["settings"]["delivery"]["inflight_ttl_ms"], 30_000);
        assert_eq!(body["locks"]["idle_queue_cleanup"], false);
    }

    #[tokio::test]
    async fn runtime_settings_put_updates_settings() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/runtime-settings")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_version": 1,
                            "settings": {
                                "delivery": {
                                    "inflight_ttl_ms": 12_000,
                                    "expiry_poll_min_ms": 15_000,
                                    "expiry_batch_max": 8192,
                                    "delivery_poll_max_ms": 5_000
                                },
                                "idle_queue_cleanup": {
                                    "enabled": false,
                                    "evict_after_ms": 600_000,
                                    "sweep_interval_ms": 60_000,
                                    "publisher_idle_timeout_ms": null
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["version"], 2);
        assert_eq!(body["settings"]["delivery"]["inflight_ttl_ms"], 12_000);
    }

    #[tokio::test]
    async fn runtime_settings_put_returns_conflict_for_stale_version() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let runtime_settings = server.runtime_settings.as_ref().unwrap().clone();
        let mut updated = runtime_settings.current().settings;
        updated.delivery.inflight_ttl_ms = 11_000;
        assert!(matches!(
            runtime_settings.update(1, updated).await.unwrap(),
            RuntimeSettingsUpdateOutcome::Stored(_)
        ));
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/runtime-settings")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_version": 1,
                            "settings": {
                                "delivery": {
                                    "inflight_ttl_ms": 12_000,
                                    "expiry_poll_min_ms": 15_000,
                                    "expiry_batch_max": 8192,
                                    "delivery_poll_max_ms": 5_000
                                },
                                "idle_queue_cleanup": {
                                    "enabled": false,
                                    "evict_after_ms": 600_000,
                                    "sweep_interval_ms": 60_000,
                                    "publisher_idle_timeout_ms": null
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let body = response_json(response).await;
        assert_eq!(body["version"], 2);
        assert_eq!(body["settings"]["delivery"]["inflight_ttl_ms"], 11_000);
    }

    #[tokio::test]
    async fn runtime_settings_put_rejects_locked_idle_cleanup() {
        let server = test_server(RuntimeSettingsLocks {
            idle_queue_cleanup: true,
        })
        .await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/runtime-settings")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_version": 1,
                            "settings": {
                                "delivery": {
                                    "inflight_ttl_ms": 30_000,
                                    "expiry_poll_min_ms": 15_000,
                                    "expiry_batch_max": 8192,
                                    "delivery_poll_max_ms": 5_000
                                },
                                "idle_queue_cleanup": {
                                    "enabled": true,
                                    "evict_after_ms": 600_000,
                                    "sweep_interval_ms": 60_000,
                                    "publisher_idle_timeout_ms": null
                                }
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert!(body["error"].as_str().unwrap().contains("locked"));
    }

    #[tokio::test]
    async fn settings_page_renders() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/settings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Runtime Settings"));
        assert!(body.contains("Save settings"));
    }
}
