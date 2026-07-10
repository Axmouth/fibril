use askama::Template;
use async_trait::async_trait;
use axum::{
    Form, Router,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::get,
};
// Path is only used by the release-only embedded static handler.
#[cfg(not(debug_assertions))]
use axum::extract::Path;
use fibril_broker::{
    StromaMetrics,
    broker::StreamAdmin,
    queue_engine::QueueEngine,
    runtime_settings::{RuntimeSettings, RuntimeSettingsManager, RuntimeSettingsSnapshot},
};
use fibril_util::StaticAuthHandler;
#[cfg(not(debug_assertions))]
use rust_embed::RustEmbed;
use serde::Deserialize;
use std::sync::Arc;
use tokio::net::TcpListener;
// ServeDir backs the debug-only on-disk /static route.
#[cfg(debug_assertions)]
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
/// (the coordination backend's consensus state serialized to JSON). Opaque JSON
/// callback so the admin crate stays independent of the coordination backend.
pub type ConsensusTopologyProvider = dyn Fn() -> serde_json::Value + Send + Sync + 'static;

#[async_trait]
pub trait CoordinationMembershipManager: Send + Sync + 'static {
    async fn add_voting_member(&self, id: u64, addr: String) -> Result<serde_json::Value, String>;

    async fn remove_voting_member(&self, id: u64) -> Result<serde_json::Value, String>;
}

/// Operator-triggered live repartition (grow or shrink) of a queue's partition
/// count. The implementation decides direction from the current count.
#[async_trait]
pub trait QueueRepartitionManager: Send + Sync + 'static {
    async fn repartition(
        &self,
        topic: String,
        group: Option<String>,
        partition_count: u32,
    ) -> Result<serde_json::Value, String>;
}

/// Operator-triggered drain: announce a planned restart or upgrade to connected
/// clients (a `GoingAway` push) so they settle in-flight work and reconnect,
/// redirecting to the post-drain owner. Returns how many clients were notified.
#[async_trait]
pub trait BrokerDrainController: Send + Sync + 'static {
    async fn announce_drain(&self, grace_ms: u64, message: String) -> DrainOutcome;
}

/// What a drain call achieved: connections notified, and in coordinated mode
/// how far the ownership handoff got within its bounded wait.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DrainOutcome {
    pub connections_notified: usize,
    /// Partitions this node still owns when the call returns. Zero means
    /// the handoff completed and stopping the process is gap-free.
    pub owned_partitions_remaining: usize,
    /// Whether ownership fully moved within the wait. `false` after the
    /// timeout (or in standalone mode, where there is nowhere to move to);
    /// reactive failover covers the remainder either way.
    pub handoff_complete: bool,
    pub handoff_waited_ms: u64,
}

/// Re-reads and swaps the broker's TLS material, returning the new leaf
/// certificate fingerprint. Errors leave the old material serving.
pub type TlsReloadHandler = dyn Fn() -> Result<String, String> + Send + Sync;

/// Returns presentation metadata for the certificate the broker currently
/// serves, as JSON (fingerprint, validity window, subject). The broker wires
/// one in when TLS is enabled; feeds the security view and the certificate
/// expiry attention rule.
pub type CertInfoProvider = dyn Fn() -> serde_json::Value + Send + Sync;

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

/// One user for admin listing: identity and timestamps, never a hash.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AdminUserInfo {
    pub username: String,
    pub created_ms: u64,
    pub updated_ms: u64,
}

/// Admin-side user management. The binary implements this over the broker
/// user store (standalone) or the cluster-authoritative document (cluster).
#[async_trait]
pub trait UserAdmin: Send + Sync + 'static {
    async fn list_users(&self) -> Result<Vec<AdminUserInfo>, String>;
    async fn upsert_user(&self, username: &str, password: &str) -> Result<(), String>;
    async fn remove_user(&self, username: &str) -> Result<(), String>;
}

pub struct AdminConfig {
    // TODO: better type, parse earlier
    pub bind: String,
    pub auth: Option<StaticAuthHandler>,
    /// When set, the dashboard serves HTTPS with this rustls config (the
    /// same material as the broker listener).
    pub tls: Option<std::sync::Arc<tokio_rustls::rustls::ServerConfig>>,
    /// Include per-channel series in the /metrics exposition. Node-level
    /// aggregates are always exported.
    pub metrics_per_channel: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum AdminServerError {
    #[error("failed to bind admin listener at {bind}: {source}")]
    Bind {
        bind: String,
        #[source]
        source: std::io::Error,
    },
    #[error("admin listener failed: {0}")]
    Serve(#[source] std::io::Error),
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StartupConfigSummary {
    pub data_dir: String,
    /// Human-readable TLS state: disabled (with the enable guide), or
    /// enabled with the material source and admin coverage.
    pub tls_status: String,
    pub broker_bind: String,
    pub admin_bind: String,
    pub admin_auth_enabled: bool,
    pub keratin_fsync_interval_ms: u64,
    pub keratin_message_log_segment_max_bytes: u64,
    pub keratin_event_log_segment_max_bytes: u64,
    pub coordination_heartbeat_interval_ms: u64,
    pub coordination_liveness_ttl_ms: u64,
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
    pub consensus_topology: Option<Arc<ConsensusTopologyProvider>>,
    /// Optional per-broker exclusive-cohort view (this node's local cohort
    /// membership). Cohort assignment is broker-local runtime state, not in the
    /// committed coordination snapshot, so this is a node-scoped view.
    pub cohorts: Option<Arc<ConsensusTopologyProvider>>,
    pub coordination_membership: Option<Arc<dyn CoordinationMembershipManager>>,
    /// Optional live-repartition trigger (grow/shrink a queue's partition count).
    pub queue_repartition: Option<Arc<dyn QueueRepartitionManager>>,
    /// Optional cluster-authoritative runtime-settings store. When absent,
    /// runtime settings are local-only and use the node's Stroma global store.
    pub runtime_settings_cluster: Option<Arc<dyn RuntimeSettingsClusterStore>>,
    /// Optional Plexus stream observability + declare surface (the hosting broker).
    pub streams: Option<Arc<dyn StreamAdmin>>,
    /// Optional user management surface for `/admin/api/users`.
    pub users: Option<Arc<dyn UserAdmin>>,
    /// Optional drain controller for `POST /admin/api/drain`.
    pub drain: Option<Arc<dyn BrokerDrainController>>,
    /// Optional live TLS reload for `POST /admin/api/tls/reload`.
    pub tls_reload: Option<Arc<TlsReloadHandler>>,
    /// Optional served-certificate metadata (fingerprint, validity) for the
    /// security view and the certificate-expiry attention rule.
    pub cert_info: Option<Arc<CertInfoProvider>>,
    /// Recent throughput and backlog time series, sampled by a background task.
    pub history: crate::history::History,
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
            consensus_topology: None,
            cohorts: None,
            coordination_membership: None,
            queue_repartition: None,
            runtime_settings_cluster: None,
            streams: None,
            users: None,
            drain: None,
            tls_reload: None,
            cert_info: None,
            history: crate::history::History::new(),
        }
    }

    /// Attach the user management surface for `/admin/api/users`.
    pub fn with_users(mut self, users: Arc<dyn UserAdmin>) -> Self {
        self.users = Some(users);
        self
    }

    /// Attach the drain controller for `POST /admin/api/drain`.
    pub fn with_drain(mut self, drain: Arc<dyn BrokerDrainController>) -> Self {
        self.drain = Some(drain);
        self
    }

    /// Attach the live TLS reload for `POST /admin/api/tls/reload`.
    pub fn with_tls_reload(mut self, reload: Arc<TlsReloadHandler>) -> Self {
        self.tls_reload = Some(reload);
        self
    }

    /// Attach served-certificate metadata for `GET /admin/api/tls` and the
    /// certificate-expiry attention rule.
    pub fn with_cert_info(mut self, cert_info: Arc<CertInfoProvider>) -> Self {
        self.cert_info = Some(cert_info);
        self
    }

    /// Attach the Plexus stream surface serving `GET/POST /admin/api/streams`.
    pub fn with_streams(mut self, streams: Arc<dyn StreamAdmin>) -> Self {
        self.streams = Some(streams);
        self
    }

    /// Attach the coordination provider serving `GET /admin/api/topology`.
    pub fn with_coordination(mut self, coordination: Arc<dyn fibril_broker::Coordination>) -> Self {
        self.coordination = Some(coordination);
        self
    }

    /// Attach the optional consensus-internals block for the topology endpoint.
    pub fn with_consensus_topology(mut self, provider: Arc<ConsensusTopologyProvider>) -> Self {
        self.consensus_topology = Some(provider);
        self
    }

    /// Attach the optional per-broker cohort view serving `GET /admin/api/cohorts`.
    pub fn with_cohorts(mut self, provider: Arc<ConsensusTopologyProvider>) -> Self {
        self.cohorts = Some(provider);
        self
    }

    /// Attach the optional coordination membership manager.
    pub fn with_coordination_membership(
        mut self,
        manager: Arc<dyn CoordinationMembershipManager>,
    ) -> Self {
        self.coordination_membership = Some(manager);
        self
    }

    /// Attach the live-repartition trigger for `POST /admin/api/repartition`.
    pub fn with_queue_repartition(mut self, manager: Arc<dyn QueueRepartitionManager>) -> Self {
        self.queue_repartition = Some(manager);
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

    pub async fn run(self) -> Result<(), AdminServerError> {
        let state = Arc::new(self);

        // Background time-series sampler feeding /admin/api/history.
        tokio::spawn(crate::history::run_sampler(state.clone()));

        let app = Self::router(state.clone());

        if let Some(tls) = state.config.tls.clone() {
            let addr = resolve_bind_addr(&state.config.bind)?;
            let rustls_config = axum_server::tls_rustls::RustlsConfig::from_config(tls);
            print_admin_banner(&state.config.bind, state.config.auth.is_some());
            tracing::info!("listening on {} (HTTPS)", state.config.bind);
            axum_server::bind_rustls(addr, rustls_config)
                .serve(app.into_make_service())
                .await
                .map_err(AdminServerError::Serve)?;
            return Ok(());
        }

        let listener = TcpListener::bind(&state.config.bind)
            .await
            .map_err(|source| AdminServerError::Bind {
                bind: state.config.bind.clone(),
                source,
            })?;
        print_admin_banner(&state.config.bind, state.config.auth.is_some());
        tracing::info!("listening on {}", state.config.bind);
        axum::serve(listener, app)
            .await
            .map_err(AdminServerError::Serve)?;
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
            .route("/admin/queue", get(queue_detail_page))
            .route("/admin/dlq", get(dlq_page))
            .route("/admin/security", get(security_page))
            .route("/admin/streams", get(streams_page))
            .route("/admin/messages", get(messages_page))
            .route("/admin/diagnostics", get(diagnostics_page))
            .route("/admin/topology", get(topology_page))
            .route("/admin/settings", get(settings_page))
            .route("/admin/api/overview", get(routes::overview))
            .route("/admin/api/history", get(crate::history::history))
            .route("/admin/api/attention", get(crate::attention::attention))
            .route("/admin/api/tls", get(routes::tls_info))
            .route("/admin/api/connections", get(routes::connections))
            .route("/admin/api/subscriptions", get(routes::subscriptions))
            .route(
                "/admin/api/queues",
                get(routes::queues).post(routes::create_queue),
            )
            .route(
                "/admin/api/queues/delete",
                axum::routing::post(routes::delete_queue),
            )
            .route("/admin/api/queues_debug", get(routes::queues_debug))
            .route("/admin/api/streams_debug", get(routes::streams_debug))
            .route(
                "/admin/api/streams",
                get(routes::streams).post(routes::create_stream),
            )
            .route("/admin/api/messages", get(routes::inspect_messages))
            .route(
                "/admin/api/runtime-settings",
                get(routes::runtime_settings).put(routes::update_runtime_settings),
            )
            .route("/admin/api/startup-config", get(routes::startup_config))
            .route("/admin/api/topology", get(routes::topology))
            .route("/admin/api/cohorts", get(routes::cohorts))
            .route(
                "/admin/api/coordination/membership/add-voting-member",
                axum::routing::post(routes::add_coordination_voting_member),
            )
            .route(
                "/admin/api/coordination/membership/remove-voting-member",
                axum::routing::post(routes::remove_coordination_voting_member),
            )
            .route(
                "/admin/api/repartition",
                axum::routing::post(routes::repartition_queue),
            )
            .route("/admin/api/drain", axum::routing::post(routes::drain))
            .route(
                "/admin/api/tls/reload",
                axum::routing::post(routes::reload_tls),
            )
            .route(
                "/admin/api/users",
                get(routes::list_users).post(routes::upsert_user),
            )
            .route(
                "/admin/api/users/{username}",
                axum::routing::delete(routes::remove_user),
            )
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
            .route("/admin/api/quarantine", get(routes::quarantine))
            .route(
                "/admin/api/quarantine/repair",
                axum::routing::post(routes::repair_partition),
            )
            .route("/metrics", get(crate::metrics_route::metrics))
            .route("/healthz", get(|| async { "ok" }))
            .route("/readyz", get(routes::readyz))
            .fallback(not_found)
            .with_state(state);
        app
    }
}

// Release-only: in debug builds /static is served from disk via ServeDir (see
// router), so the embedded copy and its handler are compiled only for release.
#[cfg(not(debug_assertions))]
#[derive(RustEmbed)]
#[folder = "admin-ui"]
struct AdminAssets;

#[cfg(not(debug_assertions))]
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

/// Transport and access: served-certificate status with live reload, and the
/// admin user store.
async fn security_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(SecurityPage {
        page: "security",
        title: "Security",
        auth_enabled: server.config.auth.is_some(),
    }))
}

/// Dead letters across the broker: the global target, its backlog, and every
/// queue that declares a dead-letter policy.
async fn dlq_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(DlqPage {
        page: "dlq",
        title: "Dead letters",
        auth_enabled: server.config.auth.is_some(),
    }))
}

/// One queue in depth. The topic and group ride in the query string and are
/// read client-side, so the template itself is static like every other page.
async fn queue_detail_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(QueueDetail {
        page: "queues",
        title: "Queue",
        auth_enabled: server.config.auth.is_some(),
    }))
}

async fn streams_page(
    State(server): State<Arc<AdminServer>>,
    headers: HeaderMap,
) -> Result<Html<String>, Redirect> {
    page_auth(&server, &headers).await?;
    Ok(render(Streams {
        page: "streams",
        title: "Streams",
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
#[template(path = "pages/streams.html")]
struct Streams {
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
#[template(path = "pages/security.html")]
struct SecurityPage {
    page: &'static str,
    title: &'static str,
    auth_enabled: bool,
}

#[derive(Template)]
#[template(path = "pages/dlq.html")]
struct DlqPage {
    page: &'static str,
    title: &'static str,
    auth_enabled: bool,
}

#[derive(Template)]
#[template(path = "pages/queue.html")]
struct QueueDetail {
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

/// Resolve the configured bind string to a socket address for the HTTPS
/// server (the plain path binds the string directly).
fn resolve_bind_addr(bind: &str) -> Result<std::net::SocketAddr, AdminServerError> {
    use std::net::ToSocketAddrs;
    bind.to_socket_addrs()
        .map_err(|source| AdminServerError::Bind {
            bind: bind.to_string(),
            source,
        })?
        .next()
        .ok_or_else(|| AdminServerError::Bind {
            bind: bind.to_string(),
            source: std::io::Error::new(
                std::io::ErrorKind::AddrNotAvailable,
                "bind resolved to no addresses",
            ),
        })
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
            DeclareMeta, KeratinAppendCompletion, KeratinConfig, MessageContentType,
            MessageHeaders, QueueEngine, SnapshotConfig, StromaEngine, StromaKeratinConfig,
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
        handler::{ConnectionSettings, ProtocolConnectionError, handle_connection},
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
                tls: None,
                metrics_per_channel: true,
            },
            Some(StartupConfigSummary {
                data_dir: root.display().to_string(),
                tls_status: "disabled".to_string(),
                broker_bind: "127.0.0.1:9876".into(),
                admin_bind: "127.0.0.1:0".into(),
                admin_auth_enabled,
                keratin_fsync_interval_ms: 5,
                keratin_message_log_segment_max_bytes: 16 * 1024 * 1024,
                keratin_event_log_segment_max_bytes: 16 * 1024 * 1024,
                coordination_heartbeat_interval_ms: 3000,
                coordination_liveness_ttl_ms: 9000,
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

    #[derive(Debug, Default)]
    struct FakeCoordinationMembershipManager {
        calls: StdMutex<Vec<String>>,
    }

    #[async_trait]
    impl CoordinationMembershipManager for FakeCoordinationMembershipManager {
        async fn add_voting_member(
            &self,
            id: u64,
            addr: String,
        ) -> Result<serde_json::Value, String> {
            self.calls.lock().unwrap().push(format!("add:{id}:{addr}"));
            Ok(json!({
                "voters": [1, id],
                "added": id,
            }))
        }

        async fn remove_voting_member(&self, id: u64) -> Result<serde_json::Value, String> {
            self.calls.lock().unwrap().push(format!("remove:{id}"));
            Ok(json!({
                "voters": [1],
                "removed": id,
            }))
        }
    }

    async fn open_protocol_connection(
        broker: Arc<Broker<StromaEngine>>,
    ) -> (
        Framed<TcpStream, ProtoCodec>,
        tokio::task::JoinHandle<Result<(), ProtocolConnectionError>>,
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
            Some(peer),
            broker,
            tcp_stats,
            connection_stats,
            conn_id,
            None::<StaticAuthHandler>,
            None,
            ConnectionSettings::new(Some(60)),
            None,
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
    async fn history_endpoint_reports_cadence_and_samples() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        // The sampler runs on its own task in run(); here we record directly to
        // assert the route serves what the ring holds.
        crate::history::run_sampler_once(&server).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/history")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["interval_ms"], 5000);
        let samples = body["samples"].as_array().expect("samples array");
        assert_eq!(samples.len(), 1);
        assert!(samples[0]["at"].as_i64().unwrap() > 0);
        assert!(samples[0]["backlog"].is_number());
    }

    #[tokio::test]
    async fn attention_flags_a_backlog_with_no_consumer() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        // Publish a message but connect no subscriber: the backlog has nobody
        // reading it, which is exactly the condition the rule names.
        let headers = MessageHeaders {
            published: 1,
            publish_received: 2,
            content_type: Some(MessageContentType::Text),
            extra: Default::default(),
        };
        let (completion, rx) = KeratinAppendCompletion::pair();
        server
            .storage
            .publish("orders", 0, None, &headers, b"work".to_vec(), completion)
            .await
            .unwrap();
        rx.await.unwrap().unwrap();

        let app = AdminServer::router(server);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/attention")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        let items = body["items"].as_array().expect("items array");
        let no_consumer = items
            .iter()
            .find(|item| item["kind"] == "no_consumer")
            .expect("a no_consumer item");
        assert_eq!(no_consumer["severity"], "warning");
        assert_eq!(no_consumer["subject"], "orders");
        assert_eq!(no_consumer["action_href"], "/admin/queues");
    }

    #[tokio::test]
    async fn attention_endpoint_is_empty_when_healthy() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/attention")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        // A fresh broker with no queues, good settings, and no TLS has nothing
        // to flag.
        assert_eq!(body["items"].as_array().expect("items array").len(), 0);
    }

    #[tokio::test]
    async fn tls_endpoint_is_null_without_certificate_then_reports_it() {
        let base = test_server(RuntimeSettingsLocks::default()).await;
        let server = Arc::try_unwrap(base).ok().expect("sole owner");

        // No TLS: null.
        let bare = AdminServer::router(Arc::new(AdminServer {
            cert_info: None,
            ..server
        }));
        let response = bare
            .oneshot(
                Request::builder()
                    .uri("/admin/api/tls")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response_json(response).await.is_null());

        // With a certificate-info provider: it reports it verbatim.
        let with_cert = test_server(RuntimeSettingsLocks::default()).await;
        let with_cert = Arc::try_unwrap(with_cert).ok().expect("sole owner");
        let app = AdminServer::router(Arc::new(AdminServer {
            cert_info: Some(Arc::new(
                || json!({ "fingerprint": "AA:BB", "not_after_unix": 4102444800i64 }),
            )),
            ..with_cert
        }));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/tls")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response_json(response).await;
        assert_eq!(body["fingerprint"], "AA:BB");
    }

    #[tokio::test]
    async fn metrics_endpoint_renders_node_families() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        server.metrics.broker().published_many(5);
        server.metrics.tcp().resume_accepted();
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(content_type.starts_with("text/plain"), "{content_type}");

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("fibril_broker_published_total 5\n"), "{body}");
        assert!(
            body.contains("fibril_tcp_resume_accepted_total 1\n"),
            "{body}"
        );
        assert!(body.contains("# TYPE fibril_tcp_connections_open gauge"));
        assert!(body.contains(r#"fibril_tcp_reconcile_outcomes_total{outcome="kept"} 0"#));
        assert!(body.contains("# TYPE fibril_recovery_quarantined gauge"));
        assert!(body.contains("fibril_recovery_quarantines_total 0\n"));

        // Every non-comment line must parse as `name{labels} value`.
        for line in body.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let (name_part, value) = line.rsplit_once(' ').expect("sample line has a value");
            assert!(value.parse::<f64>().is_ok(), "bad value in line: {line}");
            let name = name_part.split('{').next().unwrap();
            assert!(name.starts_with("fibril_"), "bad name in line: {line}");
        }
    }

    #[tokio::test]
    async fn metrics_endpoint_renders_replication_families_from_observability() {
        let observability: Arc<crate::server::BrokerQueueObservability> = Arc::new(|| {
            json!({
                "replication_summary": {
                    "follower_worker_count": 2,
                    "caught_up_count": 1,
                    "pending_retry_count": 0,
                    "checkpoint_required_count": 0,
                },
                "replication_followers": [
                    {
                        "topic": "orders",
                        "partition": 1,
                        "group": "workers",
                        "state": { "message_next_offset": 42, "event_next_offset": 7 },
                        "busy": false,
                    },
                    {
                        "topic": "orders",
                        "partition": 2,
                        "group": null,
                        "state": null,
                        "busy": false,
                    },
                ],
            })
        });

        let base = test_server(RuntimeSettingsLocks::default()).await;
        let mut server = Arc::try_unwrap(base).ok().expect("sole owner");
        server.broker_queue_observability = Some(observability.clone());
        let app = AdminServer::router(Arc::new(server));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(body.contains("fibril_replication_followers 2\n"), "{body}");
        assert!(
            body.contains("fibril_replication_followers_caught_up 1\n"),
            "{body}"
        );
        assert!(
            body.contains(
                r#"fibril_replication_follower_applied_messages{topic="orders",group="workers",partition="1"} 42"#
            ),
            "{body}"
        );
        assert!(
            body.contains(
                r#"fibril_replication_follower_applied_events{topic="orders",group="workers",partition="1"} 7"#
            ),
            "{body}"
        );
        // A follower with no applied state yet contributes no series.
        assert!(!body.contains(r#"partition="2""#), "{body}");

        // With per-channel metrics off the summary stays and the
        // per-partition series drop.
        let base = test_server(RuntimeSettingsLocks::default()).await;
        let mut server = Arc::try_unwrap(base).ok().expect("sole owner");
        server.broker_queue_observability = Some(observability);
        server.config.metrics_per_channel = false;
        let app = AdminServer::router(Arc::new(server));
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("fibril_replication_followers 2\n"), "{body}");
        assert!(
            !body.contains("fibril_replication_follower_applied_messages"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn metrics_endpoint_requires_auth_when_enabled() {
        let server =
            test_server_with_auth(RuntimeSettingsLocks::default(), Some(test_auth())).await;
        let app = AdminServer::router(server);

        let denied = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(denied.status(), StatusCode::UNAUTHORIZED);

        let allowed = app
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header(header::AUTHORIZATION, basic_auth_header("fibril", "secret"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(allowed.status(), StatusCode::OK);
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
        assert!(body["consensus"].is_null());

        // With a provider + consensus block attached, the endpoint reports both.
        let queue = QueueIdentity::new("orders", Partition::ZERO, Some("workers"));
        let mut nodes = std::collections::HashMap::new();
        nodes.insert(
            "broker-a".to_string(),
            NodeInfo {
                node_id: "broker-a".to_string(),
                broker_addr: "127.0.0.1:9000".to_string(),
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
                stream_assignments: std::collections::HashMap::new(),
                generation: 7,
            },
        );

        let wired = test_server(RuntimeSettingsLocks::default()).await;
        let wired = Arc::try_unwrap(wired).ok().expect("sole owner");
        let wired = wired
            .with_coordination(Arc::new(coordination))
            .with_consensus_topology(Arc::new(|| serde_json::json!({"local_id": 1, "leader": 1})));
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
        assert_eq!(body["consensus"]["leader"], 1);
    }

    #[tokio::test]
    async fn coordination_membership_routes_report_unavailable_without_manager() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/coordination/membership/add-voting-member")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "id": 2,
                            "addr": "127.0.0.1:9202"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response_json(response).await;
        assert_eq!(body["code"], "coordination_membership_unavailable");
    }

    #[tokio::test]
    async fn coordination_membership_add_rejects_invalid_addr() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/coordination/membership/add-voting-member")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "id": 2,
                            "addr": "not a socket address"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["code"], "invalid_coordination_member_addr");
    }

    #[tokio::test]
    async fn repartition_rejects_zero_partition_count() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/repartition")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({ "topic": "orders.created", "partition_count": 0 }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["code"], "invalid_partition_count");
    }

    #[tokio::test]
    async fn repartition_reports_unavailable_without_manager() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/repartition")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({ "topic": "orders.created", "partition_count": 4 }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = response_json(response).await;
        assert_eq!(body["code"], "repartition_unavailable");
    }

    #[tokio::test]
    async fn create_queue_rejects_empty_topic() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/queues")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({ "topic": "  " }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["code"], "invalid_topic");
    }

    #[tokio::test]
    async fn delete_queue_rejects_empty_topic() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/queues/delete")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(json!({ "topic": "  " }).to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response_json(response).await;
        assert_eq!(body["code"], "invalid_topic");
    }

    #[tokio::test]
    async fn delete_queue_destroys_declared_partitions() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        server
            .storage
            .declare_queue("orders", 0, None, DeclareMeta::default())
            .await
            .unwrap();
        server
            .storage
            .declare_queue("orders", 1, None, DeclareMeta::default())
            .await
            .unwrap();
        assert!(server.storage.is_materialized("orders", 0, None));
        let app = AdminServer::router(server.clone());

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/queues/delete")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({ "topic": "orders", "partition_count": 2 }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["status"], "deleted");
        assert_eq!(body["partition_count"], 2);
        assert!(!server.storage.is_materialized("orders", 0, None));
        assert!(!server.storage.is_materialized("orders", 1, None));
    }

    #[tokio::test]
    async fn cohorts_endpoint_returns_null_without_provider() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/cohorts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert!(body["cohorts"].is_null());
    }

    #[tokio::test]
    async fn cohorts_endpoint_serves_provider_value() {
        let server = Arc::try_unwrap(test_server(RuntimeSettingsLocks::default()).await)
            .unwrap_or_else(|_| panic!("test server should have one strong reference"))
            .with_cohorts(StdArc::new(|| {
                json!([{ "topic": "orders", "consumer_group": "g1", "members": [{ "member": "c1" }] }])
            }));
        let app = AdminServer::router(Arc::new(server));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/cohorts")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["cohorts"][0]["topic"], "orders");
        assert_eq!(body["cohorts"][0]["members"][0]["member"], "c1");
    }

    #[tokio::test]
    async fn readyz_ok_when_nothing_quarantined() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn quarantine_get_returns_policy_and_empty_list() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/api/quarantine")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert!(body["policy"].is_string());
        assert!(body["quarantined"].as_array().unwrap().is_empty());
    }

    #[tokio::test]
    async fn coordination_membership_routes_call_manager() {
        let manager = StdArc::new(FakeCoordinationMembershipManager::default());
        let server = Arc::try_unwrap(test_server(RuntimeSettingsLocks::default()).await)
            .unwrap_or_else(|_| panic!("test server should have one strong reference"))
            .with_coordination_membership(manager.clone());
        let app = AdminServer::router(Arc::new(server));

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/coordination/membership/add-voting-member")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "id": 2,
                            "addr": "127.0.0.1:9202"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["coordination"]["added"], 2);
        assert_eq!(body["coordination"]["voters"][1], 2);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/admin/api/coordination/membership/remove-voting-member")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        json!({
                            "id": 2
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["coordination"]["removed"], 2);
        assert_eq!(
            manager.calls.lock().unwrap().as_slice(),
            ["add:2:127.0.0.1:9202", "remove:2"]
        );
    }

    #[tokio::test]
    async fn security_page_renders() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/security")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Admin users"), "users card present");
        assert!(body.contains("tls-body"), "transport card present");
    }

    #[tokio::test]
    async fn dlq_page_renders() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/dlq")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("Global dead-letter target"), "global card present");
        assert!(body.contains("dlq-rows"), "policy table present");
    }

    #[tokio::test]
    async fn queue_detail_page_renders() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/queue?topic=orders&group=workers")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        // The page is static chrome; topic and group are read client-side.
        assert!(body.contains("All queues"), "back link present");
        assert!(body.contains("q-partitions"), "partitions grid present");
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
    async fn topology_page_escapes_operator_supplied_labels() {
        let server = test_server_with_auth(RuntimeSettingsLocks::default(), None).await;
        let app = AdminServer::router(server);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/admin/topology")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(body.contains("function escapeHtml"));
        assert!(body.contains("escapeHtml(assignment.topic)"));
        assert!(body.contains("escapeHtml(assignment.group"));
        assert!(body.contains("escapeHtml(coordination.node_id)"));
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
    async fn runtime_settings_put_updates_local_settings_without_cluster_store() {
        let server = test_server(RuntimeSettingsLocks::default()).await;
        assert!(server.runtime_settings_cluster.is_none());
        let runtime_settings = server.runtime_settings.as_ref().unwrap().clone();
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
        assert_eq!(
            runtime_settings.current().settings.delivery.inflight_ttl_ms,
            12_000
        );
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
                                    "max_messages_per_read": 256,
                                    "max_events_per_read": 256,
                                    "max_bytes_per_read": 8388608,
                                    "max_iterations_per_tick": 8,
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
                                "topic": "_dlq.orders",
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
        assert_eq!(body["target"]["topic"], "_dlq.orders");
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
                                "topic": "_dlq.orders",
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
                                "topic": "_dlq.other",
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
        assert_eq!(body["target"]["topic"], "_dlq.orders");
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
                                "topic": "BadTopic",
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
                            "topic": "orders.created",
                            "group": null,
                            "policy": "custom",
                            "target": {
                                "topic": "_dlq.orders",
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
                tls: None,
                metrics_per_channel: true,
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
                            "topic": "orders.created",
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
                tls: None,
                metrics_per_channel: true,
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
                                "topic": "_dlq.source",
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
                            "topic": "source",
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
                        ttl_ms: None,
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
        assert!(body.contains("Default partition count"));
        assert!(body.contains("Target partitions per consumer"));
        assert!(!body.contains("Queue partition"));
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
