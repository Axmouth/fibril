use askama::Template;
use axum::{
    Form, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
};
use fibril_broker::{
    StromaMetrics, queue_engine::QueueEngine, runtime_settings::RuntimeSettingsManager,
};
use fibril_util::StaticAuthHandler;
use rust_embed::RustEmbed;
use serde::Deserialize;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::services::ServeDir;

use crate::{
    auth::{
        AdminSessions, check_admin_auth, clear_session_cookie_header, session_cookie,
        session_cookie_header, verify_credentials,
    },
    routes,
};
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
    pub sessions: AdminSessions,
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
            sessions: AdminSessions::default(),
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
            .route("/login", get(login_page).post(login_submit))
            .route("/logout", get(logout))
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
            .route(
                "/admin/api/global-dlq",
                get(routes::global_dlq).put(routes::update_global_dlq),
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

async fn page_auth(server: &AdminServer, headers: &HeaderMap) -> Result<(), Redirect> {
    check_admin_auth(headers, &server.config.auth, &server.sessions)
        .await
        .map_err(|_| Redirect::to("/login"))
}

async fn overview_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(OverviewPage {
        page: "dashboard",
        title: "Overview",
    }))
}

async fn connections_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(Connections {
        page: "connections",
        title: "Connections",
    }))
}

async fn subscriptions_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(Subscriptions {
        page: "subscriptions",
        title: "Subscriptions",
    }))
}

async fn queues_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(Queues {
        page: "queues",
        title: "Queues",
    }))
}

async fn settings_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(Settings {
        page: "settings",
        title: "Settings",
    }))
}

async fn login_page(State(server): State<Arc<AdminServer>>, headers: HeaderMap) -> Response {
    if server.config.auth.is_none() || page_auth(&server, &headers).await.is_ok() {
        return Redirect::to("/").into_response();
    }
    render(Login {
        title: "Login",
        error: None,
    })
    .into_response()
}

#[derive(Deserialize)]
struct LoginForm {
    username: String,
    password: String,
}

async fn login_submit(
    State(server): State<Arc<AdminServer>>,
    Form(form): Form<LoginForm>,
) -> Response {
    if verify_credentials(&server.config.auth, &form.username, &form.password).await {
        let token = server.sessions.create();
        return (
            StatusCode::SEE_OTHER,
            [(header::SET_COOKIE, session_cookie_header(&token))],
            [(header::LOCATION, "/")],
        )
            .into_response();
    }

    (
        StatusCode::UNAUTHORIZED,
        render(Login {
            title: "Login",
            error: Some("Invalid username or password"),
        }),
    )
        .into_response()
}

async fn logout(State(server): State<Arc<AdminServer>>, headers: HeaderMap) -> Response {
    if let Some(token) = session_cookie(&headers) {
        server.sessions.remove(token);
    }
    (
        StatusCode::SEE_OTHER,
        [(header::SET_COOKIE, clear_session_cookie_header())],
        [(header::LOCATION, "/login")],
    )
        .into_response()
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
#[template(path = "pages/login.html")]
struct Login {
    title: &'static str,
    error: Option<&'static str>,
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
    use base64::Engine;
    use fibril_broker::{
        queue_engine::{
            KeratinConfig, QueueEngine, SnapshotConfig, StromaEngine, StromaKeratinConfig,
        },
        runtime_settings::{
            RuntimeSettings, RuntimeSettingsLocks, RuntimeSettingsManager,
            RuntimeSettingsUpdateOutcome,
        },
    };
    use fibril_metrics::Metrics;
    use serde_json::json;
    use tower::ServiceExt;

    async fn test_server(locks: RuntimeSettingsLocks) -> Arc<AdminServer> {
        test_server_with_auth(locks, None).await
    }

    async fn test_server_with_auth(
        locks: RuntimeSettingsLocks,
        auth: Option<StaticAuthHandler>,
    ) -> Arc<AdminServer> {
        let root = std::env::temp_dir().join(format!("fibril-admin-{}", fastrand::u64(..)));
        std::fs::create_dir_all(&root).unwrap();
        let engine = StromaEngine::open(
            &root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
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
                auth,
            },
            Arc::new(engine),
            Some(Arc::new(runtime_settings)),
        ))
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn test_auth() -> StaticAuthHandler {
        StaticAuthHandler::new("fibril".into(), "secret".into())
    }

    fn basic_auth_header(username: &str, password: &str) -> String {
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
        format!("Basic {encoded}")
    }

    fn session_cookie(response: &axum::response::Response) -> String {
        response
            .headers()
            .get(header::SET_COOKIE)
            .unwrap()
            .to_str()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn protected_pages_redirect_to_login_when_auth_enabled() {
        let server =
            test_server_with_auth(RuntimeSettingsLocks::default(), Some(test_auth())).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/login");
    }

    #[tokio::test]
    async fn login_sets_session_cookie_and_allows_page_access() {
        let server =
            test_server_with_auth(RuntimeSettingsLocks::default(), Some(test_auth())).await;
        let app = AdminServer::router(server);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("username=fibril&password=secret"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/");
        let cookie = session_cookie(&response);
        assert!(cookie.starts_with("fibril_admin_session="));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn login_rejects_bad_credentials() {
        let server =
            test_server_with_auth(RuntimeSettingsLocks::default(), Some(test_auth())).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("username=fibril&password=wrong"))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().get(header::SET_COOKIE).is_none());
    }

    #[tokio::test]
    async fn api_accepts_basic_or_session_auth_when_enabled() {
        let server =
            test_server_with_auth(RuntimeSettingsLocks::default(), Some(test_auth())).await;
        let app = AdminServer::router(server);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/api/runtime-settings")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/api/runtime-settings")
                    .header(header::AUTHORIZATION, basic_auth_header("fibril", "secret"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let login = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("username=fibril&password=secret"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let cookie = session_cookie(&login);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/runtime-settings")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn logout_removes_session() {
        let server =
            test_server_with_auth(RuntimeSettingsLocks::default(), Some(test_auth())).await;
        let app = AdminServer::router(server);

        let login = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/login")
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                    .body(Body::from("username=fibril&password=secret"))
                    .unwrap(),
            )
            .await
            .unwrap();
        let cookie = session_cookie(&login);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/logout")
                    .header(header::COOKIE, cookie.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(response.headers().get(header::LOCATION).unwrap(), "/login");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/")
                    .header(header::COOKIE, cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
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

        assert_eq!(response.status().as_u16(), 423);
        let body = response_json(response).await;
        assert_eq!(body["code"], "setting_locked");
        assert!(body["message"].as_str().unwrap().contains("locked"));
    }

    #[tokio::test]
    async fn global_dlq_get_returns_current_setting() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/global-dlq")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["version"], 0);
        assert!(body["target"].is_null());
    }

    #[tokio::test]
    async fn global_dlq_put_updates_and_clears_setting() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/global-dlq")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_version": 0,
                            "target": {
                                "tp": "_dlq.orders",
                                "part": 0,
                                "group": "failed"
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
        assert_eq!(body["version"], 1);
        assert_eq!(body["target"]["tp"], "_dlq.orders");
        assert_eq!(body["target"]["group"], "failed");

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/global-dlq")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_version": 1,
                            "target": null
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
        assert!(body["target"].is_null());
    }

    #[tokio::test]
    async fn global_dlq_put_returns_conflict_for_stale_version() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/global-dlq")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_version": 0,
                            "target": {
                                "tp": "_dlq.orders",
                                "part": 0,
                                "group": null
                            }
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/global-dlq")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_version": 0,
                            "target": {
                                "tp": "_dlq.other",
                                "part": 0,
                                "group": null
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
        assert_eq!(body["version"], 1);
        assert_eq!(body["target"]["tp"], "_dlq.orders");
    }

    #[tokio::test]
    async fn global_dlq_put_rejects_invalid_target() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/global-dlq")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_version": 0,
                            "target": {
                                "tp": "BadTopic",
                                "part": 0,
                                "group": null
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
        assert_eq!(body["code"], "invalid_global_dlq");
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
