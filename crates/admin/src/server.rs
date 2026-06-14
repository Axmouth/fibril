use askama::Template;
use async_trait::async_trait;
use axum::{
    Form, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
};
use fibril_broker::{
    StromaMetrics,
    queue_engine::QueueEngine,
    runtime_settings::{RuntimeSettings, RuntimeSettingsManager, RuntimeSettingsSnapshot},
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

pub type BrokerQueueObservability = dyn Fn() -> serde_json::Value + Send + Sync + 'static;

/// Producer of the optional consensus-internals block in `GET /admin/api/topology`
/// (e.g. ganglion's `RaftTopology` serialized to JSON). Kept as an opaque JSON
/// callback so the admin crate stays independent of the coordination backend.
pub type RaftTopologyProvider = dyn Fn() -> serde_json::Value + Send + Sync + 'static;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeSettingsClusterUpdateOutcome {
    Stored(RuntimeSettingsSnapshot),
    Conflict(RuntimeSettingsSnapshot),
}

#[async_trait]
pub trait RuntimeSettingsClusterStore: Send + Sync + 'static {
    async fn current_runtime_settings(&self) -> Result<Option<RuntimeSettingsSnapshot>, String>;

    async fn update_runtime_settings(
        &self,
        expected_version: u64,
        settings: RuntimeSettings,
    ) -> Result<RuntimeSettingsClusterUpdateOutcome, String>;
}

pub struct AdminConfig {
    // TODO: better type, parse earlier
    pub bind: String,
    pub auth: Option<StaticAuthHandler>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StartupConfigSummary {
    pub data_dir: String,
    pub broker_bind: String,
    pub admin_bind: String,
    pub admin_auth_enabled: bool,
    pub keratin_fsync_interval_ms: u64,
    pub keratin_message_log_segment_max_bytes: u64,
    pub keratin_event_log_segment_max_bytes: u64,
}

pub struct AdminServer {
    pub metrics: Metrics,
    pub stroma_metrics: Arc<StromaMetrics>,
    pub config: AdminConfig,
    pub startup_config: Option<StartupConfigSummary>,
    pub storage: Arc<dyn QueueEngine + Send + Sync>,
    pub broker_queue_observability: Option<Arc<BrokerQueueObservability>>,
    pub runtime_settings: Option<Arc<RuntimeSettingsManager>>,
    pub sessions: AdminSessions,
    pub coordination: Option<Arc<dyn fibril_broker::Coordination>>,
    pub raft_topology: Option<Arc<RaftTopologyProvider>>,
    /// Optional cluster-authoritative runtime-settings store. When absent,
    /// runtime settings are local-only and use the node's Stroma global store.
    pub runtime_settings_cluster: Option<Arc<dyn RuntimeSettingsClusterStore>>,
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
        startup_config: Option<StartupConfigSummary>,
        storage: Arc<dyn QueueEngine + Send + Sync>,
        broker_queue_observability: Option<Arc<BrokerQueueObservability>>,
        runtime_settings: Option<Arc<RuntimeSettingsManager>>,
    ) -> Self {
        Self {
            metrics,
            stroma_metrics,
            config,
            startup_config,
            storage,
            broker_queue_observability,
            runtime_settings,
            sessions: AdminSessions::default(),
            coordination: None,
            raft_topology: None,
            runtime_settings_cluster: None,
        }
    }

    /// Attach the coordination provider serving `GET /admin/api/topology`.
    pub fn with_coordination(mut self, coordination: Arc<dyn fibril_broker::Coordination>) -> Self {
        self.coordination = Some(coordination);
        self
    }

    /// Attach the optional consensus-internals block for the topology endpoint.
    pub fn with_raft_topology(mut self, provider: Arc<RaftTopologyProvider>) -> Self {
        self.raft_topology = Some(provider);
        self
    }

    /// Attach the cluster-authoritative runtime-settings store.
    pub fn with_runtime_settings_cluster(
        mut self,
        store: Arc<dyn RuntimeSettingsClusterStore>,
    ) -> Self {
        self.runtime_settings_cluster = Some(store);
        self
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
            .route("/admin/messages", get(messages_page))
            .route("/admin/diagnostics", get(diagnostics_page))
            .route("/admin/topology", get(topology_page))
            .route("/admin/settings", get(settings_page))
            .route("/admin/api/overview", get(routes::overview))
            .route("/admin/api/connections", get(routes::connections))
            .route("/admin/api/subscriptions", get(routes::subscriptions))
            .route("/admin/api/queues", get(routes::queues))
            .route("/admin/api/queues_debug", get(routes::queues_debug))
            .route("/admin/api/messages", get(routes::inspect_messages))
            .route(
                "/admin/api/runtime-settings",
                get(routes::runtime_settings).put(routes::update_runtime_settings),
            )
            .route("/admin/api/startup-config", get(routes::startup_config))
            .route("/admin/api/topology", get(routes::topology))
            .route(
                "/admin/api/global-dlq",
                get(routes::global_dlq).put(routes::update_global_dlq),
            )
            .route(
                "/admin/api/queue-dlq",
                axum::routing::put(routes::update_queue_dlq),
            )
            .route(
                "/admin/api/dlq/replay",
                axum::routing::post(routes::replay_dead_letters),
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
        auth_enabled: false,
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
        auth_enabled: server.config.auth.is_some(),
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
        auth_enabled: server.config.auth.is_some(),
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
        auth_enabled: server.config.auth.is_some(),
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
        auth_enabled: server.config.auth.is_some(),
    }))
}

async fn messages_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(Messages {
        page: "messages",
        title: "Messages",
        auth_enabled: server.config.auth.is_some(),
    }))
}

async fn diagnostics_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(Diagnostics {
        page: "diagnostics",
        title: "Diagnostics",
        auth_enabled: server.config.auth.is_some(),
    }))
}

async fn topology_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(TopologyPage {
        page: "topology",
        title: "Topology",
        auth_enabled: server.config.auth.is_some(),
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
        auth_enabled: server.config.auth.is_some(),
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
    auth_enabled: bool,
}

#[derive(Template)]
#[template(path = "pages/connections.html")]
struct Connections {
    page: &'static str,
    title: &'static str,
    auth_enabled: bool,
}

#[derive(Template)]
#[template(path = "pages/subscriptions.html")]
struct Subscriptions {
    page: &'static str,
    title: &'static str,
    auth_enabled: bool,
}

#[derive(Template)]
#[template(path = "pages/queues.html")]
struct Queues {
    page: &'static str,
    title: &'static str,
    auth_enabled: bool,
}

#[derive(Template)]
#[template(path = "pages/messages.html")]
struct Messages {
    page: &'static str,
    title: &'static str,
    auth_enabled: bool,
}

#[derive(Template)]
#[template(path = "pages/diagnostics.html")]
struct Diagnostics {
    page: &'static str,
    title: &'static str,
    auth_enabled: bool,
}

#[derive(Template)]
#[template(path = "pages/topology.html")]
struct TopologyPage {
    page: &'static str,
    title: &'static str,
    auth_enabled: bool,
}

#[derive(Template)]
#[template(path = "pages/settings.html")]
struct Settings {
    page: &'static str,
    title: &'static str,
    auth_enabled: bool,
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
    auth_enabled: bool,
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

    use async_trait::async_trait;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode, header},
    };
    use base64::Engine;
    use fibril_broker::{
        CompletionPair,
        broker::{Broker, BrokerConfig},
        queue_engine::{
            KeratinAppendCompletion, KeratinConfig, MessageContentType, MessageHeaders,
            QueueEngine, SnapshotConfig, StromaEngine, StromaKeratinConfig,
        },
        runtime_settings::{
            RuntimeSettings, RuntimeSettingsLocks, RuntimeSettingsManager,
            RuntimeSettingsUpdateOutcome,
        },
    };
    use fibril_metrics::{ConnectionStats, Metrics, TcpStats};
    use fibril_protocol::v1::{
        Deliver, Hello, HelloOk, Nack, Op, PROTOCOL_V1, Partition, Publish, Subscribe,
        frame::{Frame, ProtoCodec},
        handler::{ConnectionSettings, handle_connection},
        helper::{try_decode, try_encode},
    };
    use fibril_util::unix_millis;
    use futures::{SinkExt, StreamExt};
    use serde_json::json;
    use std::{
        sync::{Arc as StdArc, Mutex as StdMutex},
        time::{Duration, Instant},
    };
    use tokio::net::{TcpListener, TcpStream};
    use tokio_util::codec::Framed;
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
        let admin_auth_enabled = auth.is_some();
        Arc::new(AdminServer::new(
            Metrics::new(60),
            engine.metrics(),
            AdminConfig {
                bind: "127.0.0.1:0".into(),
                auth,
            },
            Some(StartupConfigSummary {
                data_dir: root.display().to_string(),
                broker_bind: "127.0.0.1:9876".into(),
                admin_bind: "127.0.0.1:0".into(),
                admin_auth_enabled,
                keratin_fsync_interval_ms: 5,
                keratin_message_log_segment_max_bytes: 16 * 1024 * 1024,
                keratin_event_log_segment_max_bytes: 16 * 1024 * 1024,
            }),
            Arc::new(engine),
            None,
            Some(Arc::new(runtime_settings)),
        ))
    }

    async fn response_json(response: axum::response::Response) -> serde_json::Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[derive(Debug, Default)]
    struct FakeRuntimeSettingsClusterStore {
        current: StdMutex<Option<RuntimeSettingsSnapshot>>,
    }

    #[async_trait]
    impl RuntimeSettingsClusterStore for FakeRuntimeSettingsClusterStore {
        async fn current_runtime_settings(
            &self,
        ) -> Result<Option<RuntimeSettingsSnapshot>, String> {
            Ok(self.current.lock().unwrap().clone())
        }

        async fn update_runtime_settings(
            &self,
            expected_version: u64,
            settings: RuntimeSettings,
        ) -> Result<RuntimeSettingsClusterUpdateOutcome, String> {
            let mut current = self.current.lock().unwrap();
            let current_version = current
                .as_ref()
                .map(|snapshot| snapshot.version)
                .unwrap_or(0);
            if current_version != expected_version {
                return Ok(RuntimeSettingsClusterUpdateOutcome::Conflict(
                    current.clone().unwrap_or(RuntimeSettingsSnapshot {
                        version: 0,
                        settings,
                    }),
                ));
            }
            let snapshot = RuntimeSettingsSnapshot {
                version: current_version + 1,
                settings,
            };
            *current = Some(snapshot.clone());
            Ok(RuntimeSettingsClusterUpdateOutcome::Stored(snapshot))
        }
    }

    async fn open_protocol_connection(
        broker: Arc<Broker<StromaEngine>>,
    ) -> (
        Framed<TcpStream, ProtoCodec>,
        tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client = TcpStream::connect(addr).await.unwrap();
        let (server, peer) = listener.accept().await.unwrap();
        let tcp_stats = TcpStats::new(10);
        let connection_stats = ConnectionStats::new();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);

        let server_task = tokio::spawn(handle_connection(
            server,
            broker,
            tcp_stats,
            connection_stats,
            conn_id,
            None::<StaticAuthHandler>,
            ConnectionSettings::new(Some(60)),
            None,
            None,
        ));

        (Framed::new(client, ProtoCodec), server_task)
    }

    async fn recv_frame(framed: &mut Framed<TcpStream, ProtoCodec>) -> Frame {
        tokio::time::timeout(Duration::from_secs(2), framed.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    }

    async fn handshake(framed: &mut Framed<TcpStream, ProtoCodec>) {
        framed
            .send(
                try_encode(
                    Op::Hello,
                    1,
                    &Hello {
                        client_name: "admin-flow-test".into(),
                        client_version: "0.1.0".into(),
                        protocol_version: PROTOCOL_V1,
                        resume: None,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let frame = recv_frame(framed).await;
        assert_eq!(frame.opcode, Op::HelloOk as u16);
        let hello_ok: HelloOk = try_decode(&frame).unwrap();
        assert_eq!(hello_ok.protocol_version, PROTOCOL_V1);
    }

    async fn recv_delivery_for_topic(
        framed: &mut Framed<TcpStream, ProtoCodec>,
        topic: &str,
    ) -> Deliver {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = recv_frame(framed).await;
                if frame.opcode == Op::Deliver as u16 {
                    let delivered: Deliver = try_decode(&frame).unwrap();
                    if delivered.topic == topic {
                        break delivered;
                    }
                }
            }
        })
        .await
        .unwrap()
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
    async fn topology_endpoint_serves_coordination_and_raft_blocks() {
        use fibril_broker::coordination::{
            CoordinationSnapshot, NodeInfo, PartitionAssignment, QueueIdentity, StaticCoordination,
        };

        let base = test_server(RuntimeSettingsLocks::default()).await;
        let server = Arc::try_unwrap(base).ok().expect("sole owner");

        // Without a provider, both blocks are null.
        let bare = AdminServer::router(Arc::new(AdminServer { ..server }));
        let response = bare
            .oneshot(
                Request::builder()
                    .uri("/admin/api/topology")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert!(body["coordination"].is_null());
        assert!(body["raft"].is_null());

        // With a provider + raft block attached, the endpoint reports both.
        let queue = QueueIdentity::new("orders", Partition::ZERO, Some("workers"));
        let mut nodes = std::collections::HashMap::new();
        nodes.insert(
            "broker-a".to_string(),
            NodeInfo {
                node_id: "broker-a".to_string(),
                broker_addr: "127.0.0.1:9000".parse().unwrap(),
                admin_addr: Some("127.0.0.1:9100".parse().unwrap()),
            },
        );
        let mut assignments = std::collections::HashMap::new();
        assignments.insert(
            queue.clone(),
            PartitionAssignment::new(queue, "broker-a", vec!["broker-b".to_string()], 3),
        );
        let coordination = StaticCoordination::new(
            "broker-a",
            CoordinationSnapshot {
                nodes,
                assignments,
                generation: 7,
            },
        );

        let wired = test_server(RuntimeSettingsLocks::default()).await;
        let wired = Arc::try_unwrap(wired).ok().expect("sole owner");
        let wired = wired
            .with_coordination(Arc::new(coordination))
            .with_raft_topology(Arc::new(|| serde_json::json!({"local_id": 1, "leader": 1})));
        let app = AdminServer::router(Arc::new(wired));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/topology")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;

        assert_eq!(body["coordination"]["node_id"], "broker-a");
        assert_eq!(body["coordination"]["generation"], 7);
        assert_eq!(body["coordination"]["nodes"][0]["node_id"], "broker-a");
        let assignment = &body["coordination"]["assignments"][0];
        assert_eq!(assignment["topic"], "orders");
        assert_eq!(assignment["owner"], "broker-a");
        assert_eq!(assignment["epoch"], 3);
        assert_eq!(assignment["followers"][0], "broker-b");
        assert_eq!(body["raft"]["leader"], 1);
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
    async fn messages_page_renders_inspection_form() {
        let server = test_server_with_auth(RuntimeSettingsLocks::default(), None).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/messages")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("message-inspection-form"));
        assert!(body.contains("/admin/api/messages"));
        assert!(body.contains("message-status-filter"));
    }

    #[tokio::test]
    async fn diagnostics_page_renders_stroma_metrics() {
        let server = test_server_with_auth(RuntimeSettingsLocks::default(), None).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/diagnostics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Command Lanes"));
        assert!(body.contains("Logs And Snapshots"));
        assert!(body.contains("/admin/api/overview"));
    }

    #[tokio::test]
    async fn auth_disabled_pages_show_status_without_logout() {
        let server = test_server_with_auth(RuntimeSettingsLocks::default(), None).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Auth disabled"));
        assert!(!body.contains("href=\"/logout\""));
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
        assert!(body["settings"]["connection"]["reconnect_grace_ms"].is_null());
        assert_eq!(body["locks"]["idle_queue_cleanup"], false);
        assert!(body["load_issue"].is_null());
    }

    #[tokio::test]
    async fn startup_config_get_returns_readonly_summary() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/startup-config")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["broker_bind"], "127.0.0.1:9876");
        assert_eq!(body["admin_auth_enabled"], false);
        assert_eq!(body["keratin_fsync_interval_ms"], 5);
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
                                },
                                "connection": {
                                    "reconnect_grace_ms": 30_000
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
        assert_eq!(body["settings"]["connection"]["reconnect_grace_ms"], 30_000);
    }

    #[tokio::test]
    async fn runtime_settings_put_uses_cluster_store_when_available() {
        let cluster = StdArc::new(FakeRuntimeSettingsClusterStore::default());
        let server = Arc::try_unwrap(test_server(RuntimeSettingsLocks::default()).await)
            .unwrap_or_else(|_| panic!("test server should have one strong reference"))
            .with_runtime_settings_cluster(cluster.clone());
        let runtime_settings = server.runtime_settings.as_ref().unwrap().clone();
        let app = AdminServer::router(Arc::new(server));

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/runtime-settings")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "expected_version": 0,
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
                                },
                                "connection": {
                                    "reconnect_grace_ms": 30_000
                                },
                                "replication": {
                                    "confirm_timeout_ms": 5000,
                                    "caught_up_poll_ms": 1000,
                                    "retry_poll_ms": 100,
                                    "checkpoint_retry_poll_ms": 5000,
                                    "min_in_sync_replicas": 1,
                                    "isr_timeout_ms": 10000
                                },
                                "partitioning": {
                                    "default_partition_count": 1
                                },
                                "consumer_groups": {
                                    "default_target_per_consumer": null
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
        assert_eq!(body["version"], 1);
        assert_eq!(body["settings"]["delivery"]["inflight_ttl_ms"], 12_000);
        assert_eq!(
            cluster
                .current
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .settings
                .delivery
                .inflight_ttl_ms,
            12_000
        );

        // The cluster write is applied back to the local manager as a cache.
        assert_eq!(
            runtime_settings.current().settings.delivery.inflight_ttl_ms,
            12_000
        );
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
                                },
                                "connection": {
                                    "reconnect_grace_ms": 30_000
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
                                },
                                "connection": {
                                    "reconnect_grace_ms": null
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
    async fn message_inspection_returns_active_messages_with_payload_preview() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let headers = MessageHeaders {
            published: 1,
            publish_received: 2,
            content_type: Some(MessageContentType::Text),
            extra: Default::default(),
        };
        let (completion, rx) = KeratinAppendCompletion::pair();
        server
            .storage
            .publish("inspect", 0, None, &headers, b"hello".to_vec(), completion)
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        let app = AdminServer::router(server);
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/admin/api/messages?topic=inspect&include_payload=true&payload_limit_bytes=3")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["next_offset_hint"], 50);
        assert_eq!(body["items"][0]["state"]["offset"], 0);
        assert_eq!(body["items"][0]["state"]["status"], "ready");
        assert_eq!(body["items"][0]["payload_len"], 5);
        assert_eq!(
            body["items"][0]["payload_base64"],
            base64::engine::general_purpose::STANDARD.encode(b"hel")
        );
        assert_eq!(body["items"][0]["payload_truncated"], true);
        assert!(body["items"][0].get("missing_payload").is_none());
        assert!(body.get("warning").is_none());

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/messages?topic=inspect&status=inflight")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert!(body["items"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn message_inspection_rejects_unknown_status_filter() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/messages?topic=inspect&status=unknown")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["code"], "invalid_message_status_filter");
    }

    #[tokio::test]
    async fn dlq_replay_requires_offsets() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/dlq/replay")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "dlq_topic": "_dlq.source",
                            "dlq_group": null,
                            "offsets": []
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["code"], "missing_dlq_replay_offsets");
    }

    #[tokio::test]
    async fn dlq_replay_rejects_too_many_offsets() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);
        let offsets: Vec<u64> = (0..101).collect();

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/dlq/replay")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "dlq_topic": "_dlq.source",
                            "dlq_group": null,
                            "offsets": offsets
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["code"], "too_many_dlq_replay_offsets");
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
    async fn queue_dlq_put_declares_custom_policy() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/queue-dlq")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "tp": "orders.created",
                            "group": null,
                            "policy": "custom",
                            "target": {
                                "tp": "_dlq.orders",
                                "group": "failed"
                            },
                            "max_retries": 7
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["status"], "stored");

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/queues_debug")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        let queue = body["queues"]
            .as_array()
            .unwrap()
            .iter()
            .find(|queue| queue["topic"] == "orders.created")
            .unwrap();
        assert_eq!(queue["state"]["dlq_max_retries"], 7);
        assert!(
            queue["state"]["dlq_policy"]
                .as_str()
                .unwrap()
                .contains("_dlq.orders")
        );
    }

    #[tokio::test]
    async fn queues_debug_includes_broker_activity_when_available() {
        let root = std::env::temp_dir().join(format!("fibril-admin-{}", fastrand::u64(..)));
        std::fs::create_dir_all(&root).unwrap();
        let engine = StromaEngine::open(
            &root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let server = Arc::new(AdminServer::new(
            Metrics::new(60),
            engine.metrics(),
            AdminConfig {
                bind: "127.0.0.1:0".into(),
                auth: None,
            },
            None,
            Arc::new(engine),
            Some(Arc::new(|| {
                json!({
                    "summary": {
                        "tracked_queue_count": 1,
                        "active_queue_count": 0,
                        "idle_queue_count": 1
                    },
                    "queues": [{
                        "topic": "orders.created",
                        "group": null,
                        "active_publishers": 0,
                        "active_subscribers": 0,
                        "idle_since_ms": 10,
                        "idle_for_ms": 5,
                        "last_active_ms": 10,
                        "last_eviction_attempt": {
                            "attempted_at_ms": 15,
                            "kind": "storage",
                            "outcome": "evicted"
                        }
                    }],
                    "replication_summary": {
                        "follower_worker_count": 1,
                        "caught_up_count": 0,
                        "pending_retry_count": 1,
                        "checkpoint_required_count": 0
                    },
                    "replication_followers": [{
                        "topic": "orders.created",
                        "partition": 0,
                        "group": null,
                        "state": {
                            "message_next_offset": 4,
                            "event_next_offset": 6,
                            "status": {
                                "status": "pending_retry"
                            },
                            "last_progress": null,
                            "next_delay_ms": 100
                        },
                        "busy": false
                    }]
                })
            })),
            None,
        ));
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/queues_debug")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["broker_activity"][0]["topic"], "orders.created");
        assert_eq!(
            body["broker_activity"][0]["last_eviction_attempt"]["outcome"],
            "evicted"
        );
        assert_eq!(body["broker_activity_summary"]["tracked_queue_count"], 1);
        assert_eq!(
            body["replication_summary"]["follower_worker_count"],
            serde_json::json!(1)
        );
        assert_eq!(body["replication_followers"][0]["topic"], "orders.created");
        assert_eq!(body["broker_cleanup_metrics"]["attempts_total"], 0);
    }

    #[tokio::test]
    async fn queue_dlq_put_requires_custom_target() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("PUT")
                    .uri("/admin/api/queue-dlq")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "tp": "orders.created",
                            "group": null,
                            "policy": "custom",
                            "target": null,
                            "max_retries": 7
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["code"], "missing_queue_dlq_target");
    }

    #[tokio::test]
    async fn admin_dlq_configuration_routes_failed_message_over_tcp() {
        let root = std::env::temp_dir().join(format!("fibril-admin-flow-{}", fastrand::u64(..)));
        std::fs::create_dir_all(&root).unwrap();
        let engine = StromaEngine::open(
            &root,
            StromaKeratinConfig::from_message_log(KeratinConfig::test_default()),
            SnapshotConfig::default(),
        )
        .await
        .unwrap();
        let server = Arc::new(AdminServer::new(
            Metrics::new(60),
            engine.metrics(),
            AdminConfig {
                bind: "127.0.0.1:0".into(),
                auth: None,
            },
            None,
            Arc::new(engine.clone()),
            None,
            None,
        ));
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
                                "tp": "_dlq.source",
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
                    .uri("/admin/api/queue-dlq")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "tp": "source",
                            "group": null,
                            "policy": "global",
                            "target": null,
                            "max_retries": 0
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let broker = Broker::new(engine, BrokerConfig::default(), None);
        let (mut framed, server_task) = open_protocol_connection(broker).await;
        handshake(&mut framed).await;

        framed
            .send(
                try_encode(
                    Op::Subscribe,
                    2,
                    &Subscribe {
                        topic: "source".into(),
                        partition: Partition::new(0),
                        group: None,
                        prefetch: 1,
                        auto_ack: false,
                        consumer_group: None,
                        consumer_target: None,
                        member_id: None,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

        framed
            .send(
                try_encode(
                    Op::Subscribe,
                    3,
                    &Subscribe {
                        topic: "_dlq.source".into(),
                        partition: Partition::new(0),
                        group: None,
                        prefetch: 1,
                        auto_ack: false,
                        consumer_group: None,
                        consumer_target: None,
                        member_id: None,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(recv_frame(&mut framed).await.opcode, Op::SubscribeOk as u16);

        framed
            .send(
                try_encode(
                    Op::Publish,
                    4,
                    &Publish {
                        topic: "source".into(),
                        partition: Partition::new(0),
                        group: None,
                        require_confirm: true,
                        content_type: None,
                        headers: std::collections::HashMap::from([(
                            "x-trace-id".into(),
                            "admin-dlq-flow".into(),
                        )]),
                        payload: b"poison".to_vec(),
                        published: unix_millis(),
                        partition_key: None,
                        partitioning_version: 0,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let source = recv_delivery_for_topic(&mut framed, "source").await;
        assert_eq!(source.payload, b"poison".to_vec());

        framed
            .send(
                try_encode(
                    Op::Nack,
                    5,
                    &Nack {
                        topic: "source".into(),
                        group: None,
                        partition: Partition::new(0),
                        tags: vec![source.delivery_tag],
                        requeue: true,
                        not_before: None,
                    },
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let dlq = recv_delivery_for_topic(&mut framed, "_dlq.source").await;
        assert_eq!(dlq.payload, b"poison".to_vec());
        assert_eq!(
            dlq.headers.get("x-trace-id").map(String::as_str),
            Some("admin-dlq-flow")
        );
        assert!(!dlq.headers.contains_key("x-dlq-source-tp"));

        drop(framed);
        server_task.await.unwrap().unwrap();
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
        assert!(body.contains("Broker Runtime Settings"));
        assert!(body.contains("Stroma Dead Letter Defaults"));
        assert!(body.contains("Startup Config"));
        assert_eq!(body.matches("<h5>Startup Config</h5>").count(), 1);
        assert!(body.contains("Global Dead Letter Queue"));
        assert!(body.contains("Queue Dead Letter Policy"));
        assert!(body.contains("Optional queue group for the target queue."));
        assert!(!body.contains("Queue partition"));
        assert!(!body.contains("Target partition"));
        assert!(!body.contains("consumer group"));
        assert!(body.contains("Save settings"));
        assert!(!body.contains("Log out"));
        assert!(!body.contains("href=\"/logout\""));
    }

    #[tokio::test]
    async fn settings_page_shows_logout_when_auth_enabled() {
        let server =
            test_server_with_auth(RuntimeSettingsLocks::default(), Some(test_auth())).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/settings")
                    .header(header::AUTHORIZATION, basic_auth_header("fibril", "secret"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Log out"));
        assert!(body.contains("href=\"/logout\""));
    }
}
