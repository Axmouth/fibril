use std::{
    ffi::OsString,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
};

use clap::Parser;
use serde::{Deserialize, Serialize};

pub type ConfigResult<T> = Result<T, ConfigError>;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to parse command line arguments: {0}")]
    Cli(#[from] clap::Error),
    #[error("failed to read config file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("failed to parse config file {path}: {source}")]
    ParseFile {
        path: PathBuf,
        #[source]
        source: toml::de::Error,
    },
    #[error("failed to parse config TOML: {0}")]
    ParseToml(#[from] toml::de::Error),
    #[error("failed to encode config overlay: {0}")]
    EncodeToml(#[from] toml::ser::Error),
    #[error("failed to write {path}: {source}")]
    WriteFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{name} is not valid unicode: {source}")]
    EnvUnicode {
        name: String,
        #[source]
        source: std::env::VarError,
    },
    #[error("{name} has invalid value {value:?}: {message}")]
    EnvParse {
        name: String,
        value: String,
        message: String,
    },
    #[error("FIBRIL_COORDINATION_PEERS entry `{entry}` is not id=addr")]
    CoordinationPeerEntry { entry: String },
    #[error("{0}")]
    Validation(String),
    #[error("unknown coordination mode `{value}` (static|ganglion)")]
    UnknownCoordinationMode { value: String },
    #[error("invalid assignment durability node count `{value}`: {source}")]
    AssignmentDurabilityNodes {
        value: String,
        #[source]
        source: std::num::ParseIntError,
    },
    #[error(
        "unknown assignment durability `{value}` (local_durable|replica_accepted|replica_durable|majority_durable)"
    )]
    UnknownAssignmentDurability { value: String },
    #[error("unknown recovery on_mismatch `{value}` (quarantine|refuse|ignore)")]
    UnknownRecoveryMismatchMode { value: String },
}

impl ConfigError {
    fn validation(message: impl Into<String>) -> Self {
        Self::Validation(message.into())
    }
}

#[derive(Debug, Parser)]
#[command(name = "fibril-server", about = "Run the Fibril broker server")]
struct CliArgs {
    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long)]
    data_dir: Option<PathBuf>,

    #[arg(long)]
    broker_bind: Option<SocketAddr>,

    #[arg(long)]
    admin_bind: Option<SocketAddr>,

    #[arg(long)]
    admin_auth_enabled: Option<bool>,

    #[arg(long)]
    admin_username: Option<String>,

    #[arg(long)]
    admin_password: Option<String>,

    #[arg(long)]
    keratin_fsync_interval_ms: Option<u64>,

    #[arg(long)]
    keratin_min_fsync_interval_ms: Option<u64>,

    #[arg(long)]
    keratin_batch_linger_ms: Option<u64>,

    #[arg(long)]
    keratin_message_log_segment_max_bytes: Option<u64>,

    #[arg(long)]
    keratin_event_log_segment_max_bytes: Option<u64>,

    #[arg(long)]
    queue_idle_evict_after_ms: Option<u64>,

    #[arg(long)]
    queue_idle_sweep_interval_ms: Option<u64>,

    #[arg(long)]
    publisher_idle_timeout_ms: Option<u64>,

    #[arg(long)]
    reconnect_grace_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ServerConfig {
    pub server: ServerSection,
    pub broker: BrokerSection,
    pub admin: AdminSection,
    pub tls: TlsSection,
    pub storage: StorageSection,
    pub runtime_seed: RuntimeSeedSection,
    pub runtime_locks: RuntimeLocksSection,
    pub coordination: CoordinationSection,
    pub recovery: RecoverySection,
    pub setup: SetupSection,
    pub auth: AuthSection,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server: ServerSection::default(),
            broker: BrokerSection::default(),
            admin: AdminSection::default(),
            tls: TlsSection::default(),
            storage: StorageSection::default(),
            runtime_seed: RuntimeSeedSection::default(),
            runtime_locks: RuntimeLocksSection::default(),
            coordination: CoordinationSection::default(),
            recovery: RecoverySection::default(),
            setup: SetupSection::default(),
            auth: AuthSection::default(),
        }
    }
}

/// Startup recovery behavior.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct RecoverySection {
    /// What to do when recovery finds the event log references a message the
    /// message log never durably accepted (a dangling event->message reference).
    pub on_mismatch: RecoveryMismatchMode,
}

/// Policy for a recovery dangling event->message mismatch. Mirrors
/// `stroma_core::RecoveryMismatchPolicy`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryMismatchMode {
    /// Park only the affected partition; the broker stays up. (Default.)
    #[default]
    Quarantine,
    /// Treat as fatal: the broker escalates (readiness goes hard-unhealthy) so it
    /// cannot be missed.
    Refuse,
    /// Auto-apply the truncate-to-valid repair and continue, with a loud warning.
    Ignore,
}

impl std::str::FromStr for RecoveryMismatchMode {
    type Err = ConfigError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "quarantine" => Ok(Self::Quarantine),
            "refuse" => Ok(Self::Refuse),
            "ignore" => Ok(Self::Ignore),
            other => Err(ConfigError::UnknownRecoveryMismatchMode {
                value: other.to_string(),
            }),
        }
    }
}

/// Broker authentication: first-boot user seeds and the default-credential
/// policy. After first boot the persisted user store owns the users
/// (managed via fibrilctl or the dashboard), these seeds do not overwrite it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AuthSection {
    /// Accept the built-in default credentials (fibril/fibril) from loopback
    /// connections. Remote connections never accept them, they require a
    /// real user. Disable to require real users everywhere.
    pub allow_default_loopback: bool,
    /// Users created when the user store is empty (first boot only).
    pub seed_users: Vec<SeedUser>,
}

impl Default for AuthSection {
    fn default() -> Self {
        Self {
            allow_default_loopback: true,
            seed_users: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SeedUser {
    pub username: String,
    pub password: String,
}

/// First-boot setup mode. When `mode` is set and the data dir holds no
/// completed-setup marker, the server starts only a localhost setup page and
/// the broker listener stays down until setup completes. Completion records
/// the marker, so restarts boot normally. Deleting the marker re-arms setup
/// on the next boot. Deployments that configure everything via file or env
/// never enable this and are unaffected.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct SetupSection {
    pub mode: bool,
}

/// File name of the completed-setup marker under the data dir.
pub const SETUP_MARKER_FILE: &str = "setup_complete";
/// File name of the setup-owned config overlay under the data dir.
pub const CONFIG_OVERLAY_FILE: &str = "config-overlay.toml";

/// The subset of config a completed setup may own. Only sections listed here
/// can come from the overlay.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ConfigOverlay {
    pub tls: Option<TlsSection>,
    pub auth: Option<AuthSection>,
}

impl ConfigOverlay {
    /// Persist under the data dir for boot to layer below explicit config.
    pub fn write(&self, data_dir: &Path) -> ConfigResult<()> {
        let path = data_dir.join(CONFIG_OVERLAY_FILE);
        let encoded = toml::to_string_pretty(self)?;
        std::fs::write(&path, encoded).map_err(|source| ConfigError::WriteFile { path, source })
    }
}

impl ServerConfig {
    /// Layer the setup-owned overlay from the data dir below explicit config:
    /// an overlay section applies only when file/env/CLI left that section at
    /// its default, so explicit config always wins. Returns whether the tls
    /// section came from the overlay.
    pub fn apply_setup_overlay(&mut self) -> ConfigResult<bool> {
        let path = self.server.data_dir.join(CONFIG_OVERLAY_FILE);
        let input = match std::fs::read_to_string(&path) {
            Ok(input) => input,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(source) => return Err(ConfigError::ReadFile { path, source }),
        };
        let overlay: ConfigOverlay =
            toml::from_str(&input).map_err(|source| ConfigError::ParseFile { path, source })?;
        let mut applied = false;
        if let Some(tls) = overlay.tls
            && self.tls == TlsSection::default()
        {
            tls.mode()?;
            self.tls = tls;
            applied = true;
        }
        if let Some(auth) = overlay.auth
            && self.auth == AuthSection::default()
        {
            self.auth = auth;
            applied = true;
        }
        Ok(applied)
    }

    /// Whether the completed-setup marker exists under the data dir.
    pub fn setup_marker_exists(&self) -> bool {
        self.server.data_dir.join(SETUP_MARKER_FILE).exists()
    }

    /// Whether this boot should enter first-boot setup mode.
    pub fn setup_pending(&self) -> bool {
        self.setup.mode && !self.setup_marker_exists()
    }

    /// Resolve the cluster shared secret: `FIBRIL_CLUSTER_SECRET` env, then
    /// `coordination.secret_path`, then the `<data_dir>/cluster.secret`
    /// convention file. Trimmed so a trailing newline in a mounted secret
    /// file does not change the value.
    pub fn resolve_cluster_secret(&self) -> ConfigResult<Option<String>> {
        if let Some(value) = optional_string_env("FIBRIL_CLUSTER_SECRET")? {
            return Ok(Some(value.trim().to_string()));
        }
        let candidates = [
            self.coordination.secret_path.clone(),
            Some(self.server.data_dir.join(CLUSTER_SECRET_FILE)),
        ];
        for path in candidates.into_iter().flatten() {
            match std::fs::read_to_string(&path) {
                Ok(raw) => {
                    let trimmed = raw.trim();
                    if trimmed.is_empty() {
                        return Err(ConfigError::validation(format!(
                            "cluster secret file {} is empty",
                            path.display()
                        )));
                    }
                    return Ok(Some(trimmed.to_string()));
                }
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    // An explicitly configured path that is missing is an
                    // error, the convention file is optional.
                    if Some(&path) == self.coordination.secret_path.as_ref() {
                        return Err(ConfigError::ReadFile { path, source: err });
                    }
                }
                Err(source) => return Err(ConfigError::ReadFile { path, source }),
            }
        }
        Ok(None)
    }
}

/// File name of the cluster shared secret under the data dir (the
/// convention location fibrilctl secret generate and setup mode write).
pub const CLUSTER_SECRET_FILE: &str = "cluster.secret";

impl ServerConfig {
    pub fn load() -> ConfigResult<Self> {
        Self::load_from_args(std::env::args_os())
    }

    pub fn load_file_and_env(config_path: Option<PathBuf>) -> ConfigResult<Self> {
        let config_path = match config_path {
            Some(path) => Some(path),
            None => optional_path_env("FIBRIL_CONFIG")?,
        };
        let mut config = match config_path.as_deref() {
            Some(path) => Self::from_toml_file(path)?,
            None => Self::default(),
        };
        config.apply_env()?;
        config.validate()?;
        Ok(config)
    }

    pub fn load_from_args<I, T>(args: I) -> ConfigResult<Self>
    where
        I: IntoIterator<Item = T>,
        T: Into<OsString> + Clone,
    {
        let args = CliArgs::try_parse_from(args)?;
        let config_path = match args.config.as_deref() {
            Some(path) => Some(path.to_path_buf()),
            None => optional_path_env("FIBRIL_CONFIG")?,
        };
        let mut config = match config_path.as_deref() {
            Some(path) => Self::from_toml_file(path)?,
            None => Self::default(),
        };
        config.apply_env()?;
        config.apply_cli(args);
        config.validate()?;
        Ok(config)
    }

    pub fn from_toml_str(input: &str) -> ConfigResult<Self> {
        let mut config: Self = toml::from_str(input)?;
        config.validate()?;
        Ok(config)
    }

    fn apply_env(&mut self) -> ConfigResult<()> {
        self.apply_env_from(|name| match optional_string_env(name) {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        })
    }

    fn apply_env_from<F>(&mut self, mut get: F) -> ConfigResult<()>
    where
        F: FnMut(&str) -> Option<ConfigResult<String>>,
    {
        if let Some(value) = env_value(&mut get, "FIBRIL_DATA_DIR")? {
            self.server.data_dir = PathBuf::from(value);
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_BROKER_BIND")? {
            self.broker.listener.bind = parse_env("FIBRIL_BROKER_BIND", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_BROKER_ADVERTISE")? {
            // Comma-separated `host:port` entries, in priority order. Blank
            // entries (e.g. a trailing comma) are dropped.
            self.broker.listener.advertise = value
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_ADMIN_BIND")? {
            self.admin.listener.bind = parse_env("FIBRIL_ADMIN_BIND", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_ADMIN_ADVERTISE")? {
            // Comma-separated `host:port` entries like FIBRIL_BROKER_ADVERTISE.
            // Only the first is registered with the cluster today.
            self.admin.listener.advertise = value
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect();
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_MODE")? {
            self.coordination.mode = parse_env("FIBRIL_COORDINATION_MODE", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_NODE_ID")? {
            self.coordination.node_id = value;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_RAFT_ID")? {
            self.coordination.ganglion.raft_node_id =
                parse_env("FIBRIL_COORDINATION_RAFT_ID", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_LISTEN")? {
            self.coordination.ganglion.listen = parse_env("FIBRIL_COORDINATION_LISTEN", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_BOOTSTRAP")? {
            self.coordination.ganglion.bootstrap =
                parse_env("FIBRIL_COORDINATION_BOOTSTRAP", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_WIRE_FORMAT")? {
            self.coordination.ganglion.wire_format = value;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_HEARTBEAT_INTERVAL_MS")? {
            self.coordination.ganglion.heartbeat_interval_ms =
                parse_env("FIBRIL_COORDINATION_HEARTBEAT_INTERVAL_MS", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_LIVENESS_TTL_MS")? {
            self.coordination.ganglion.liveness_ttl_ms =
                parse_env("FIBRIL_COORDINATION_LIVENESS_TTL_MS", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_RAFT_HEARTBEAT_INTERVAL_MS")?
        {
            self.coordination.ganglion.raft.heartbeat_interval_ms =
                parse_env("FIBRIL_COORDINATION_RAFT_HEARTBEAT_INTERVAL_MS", &value)?;
        }
        if let Some(value) =
            env_value(&mut get, "FIBRIL_COORDINATION_RAFT_ELECTION_TIMEOUT_MIN_MS")?
        {
            self.coordination.ganglion.raft.election_timeout_min_ms =
                parse_env("FIBRIL_COORDINATION_RAFT_ELECTION_TIMEOUT_MIN_MS", &value)?;
        }
        if let Some(value) =
            env_value(&mut get, "FIBRIL_COORDINATION_RAFT_ELECTION_TIMEOUT_MAX_MS")?
        {
            self.coordination.ganglion.raft.election_timeout_max_ms =
                parse_env("FIBRIL_COORDINATION_RAFT_ELECTION_TIMEOUT_MAX_MS", &value)?;
        }
        if let Some(value) = env_value(
            &mut get,
            "FIBRIL_COORDINATION_FORWARDED_WRITE_REDIRECT_LIMIT",
        )? {
            self.coordination.ganglion.forwarded_write.redirect_limit =
                parse_env("FIBRIL_COORDINATION_FORWARDED_WRITE_REDIRECT_LIMIT", &value)?;
        }
        if let Some(value) = env_value(
            &mut get,
            "FIBRIL_COORDINATION_FORWARDED_WRITE_NO_LEADER_RETRIES",
        )? {
            self.coordination.ganglion.forwarded_write.no_leader_retries = parse_env(
                "FIBRIL_COORDINATION_FORWARDED_WRITE_NO_LEADER_RETRIES",
                &value,
            )?;
        }
        if let Some(value) = env_value(
            &mut get,
            "FIBRIL_COORDINATION_FORWARDED_WRITE_NO_LEADER_BASE_BACKOFF_MS",
        )? {
            self.coordination
                .ganglion
                .forwarded_write
                .no_leader_base_backoff_ms = parse_env(
                "FIBRIL_COORDINATION_FORWARDED_WRITE_NO_LEADER_BASE_BACKOFF_MS",
                &value,
            )?;
        }
        if let Some(value) = env_value(
            &mut get,
            "FIBRIL_COORDINATION_FORWARDED_WRITE_NO_LEADER_MAX_BACKOFF_MS",
        )? {
            self.coordination
                .ganglion
                .forwarded_write
                .no_leader_max_backoff_ms = parse_env(
                "FIBRIL_COORDINATION_FORWARDED_WRITE_NO_LEADER_MAX_BACKOFF_MS",
                &value,
            )?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_PEERS")? {
            // Comma-separated "raft_id=host:port" pairs.
            let mut peers = std::collections::BTreeMap::new();
            for pair in value.split(',').filter(|pair| !pair.trim().is_empty()) {
                let (id, addr) =
                    pair.split_once('=')
                        .ok_or_else(|| ConfigError::CoordinationPeerEntry {
                            entry: pair.to_string(),
                        })?;
                peers.insert(id.trim().to_string(), addr.trim().to_string());
            }
            self.coordination.ganglion.peers = peers;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY")? {
            self.coordination.ganglion.assignment_durability =
                parse_env("FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_RECOVERY_ON_MISMATCH")? {
            self.recovery.on_mismatch = parse_env("FIBRIL_RECOVERY_ON_MISMATCH", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_ADMIN_AUTH_ENABLED")? {
            self.admin.auth.enabled = parse_env("FIBRIL_ADMIN_AUTH_ENABLED", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_ADMIN_USERNAME")? {
            self.admin.auth.username = value;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_ADMIN_PASSWORD")? {
            self.admin.auth.password = Some(value);
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_ADMIN_METRICS_PER_CHANNEL")? {
            self.admin.metrics_per_channel = parse_env("FIBRIL_ADMIN_METRICS_PER_CHANNEL", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_AUTH_USERNAME")? {
            // Paired with FIBRIL_AUTH_PASSWORD below into one seed entry;
            // validation rejects a half-set pair.
            self.auth.seed_users.push(SeedUser {
                username: value,
                password: String::new(),
            });
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_AUTH_PASSWORD")? {
            match self.auth.seed_users.last_mut() {
                Some(seed) if seed.password.is_empty() => seed.password = value,
                _ => {
                    self.auth.seed_users.push(SeedUser {
                        username: String::new(),
                        password: value,
                    });
                }
            }
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_CLUSTER_SECRET_PATH")? {
            self.coordination.secret_path = Some(PathBuf::from(value));
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_SETUP_MODE")? {
            self.setup.mode = parse_env("FIBRIL_SETUP_MODE", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_TLS_ENABLED")? {
            self.tls.enabled = parse_env("FIBRIL_TLS_ENABLED", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_TLS_CERT_PATH")? {
            self.tls.cert_path = Some(PathBuf::from(value));
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_TLS_KEY_PATH")? {
            self.tls.key_path = Some(PathBuf::from(value));
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_TLS_AUTO_SELF_SIGNED")? {
            self.tls.auto_self_signed = parse_env("FIBRIL_TLS_AUTO_SELF_SIGNED", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_TLS_ADMIN_ENABLED")? {
            self.tls.admin_enabled = Some(parse_env("FIBRIL_TLS_ADMIN_ENABLED", &value)?);
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_TLS_INTER_BROKER")? {
            self.tls.inter_broker = Some(parse_env("FIBRIL_TLS_INTER_BROKER", &value)?);
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_TLS_PEER_CA_PATH")? {
            self.tls.peer_ca_path = Some(PathBuf::from(value));
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_TLS_CLIENT_AUTH")? {
            self.tls.client_auth =
                value
                    .parse()
                    .map_err(|message: String| ConfigError::EnvParse {
                        name: "FIBRIL_TLS_CLIENT_AUTH".into(),
                        value: value.clone(),
                        message,
                    })?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_TLS_CLIENT_CA_PATH")? {
            self.tls.client_ca_path = Some(PathBuf::from(value));
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_KERATIN_FSYNC_INTERVAL_MS")? {
            self.storage.keratin.fsync_interval_ms =
                parse_env("FIBRIL_KERATIN_FSYNC_INTERVAL_MS", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_KERATIN_MIN_FSYNC_INTERVAL_MS")? {
            self.storage.keratin.min_fsync_interval_ms =
                parse_env("FIBRIL_KERATIN_MIN_FSYNC_INTERVAL_MS", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_KERATIN_BATCH_LINGER_MS")? {
            self.storage.keratin.batch_linger_ms =
                parse_env("FIBRIL_KERATIN_BATCH_LINGER_MS", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_KERATIN_SEGMENT_PREALLOCATE_BYTES")? {
            self.storage.keratin.segment_preallocate_bytes =
                parse_env("FIBRIL_KERATIN_SEGMENT_PREALLOCATE_BYTES", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_KERATIN_MAX_INFLIGHT_FSYNCS")? {
            self.storage.keratin.max_inflight_fsyncs =
                parse_env("FIBRIL_KERATIN_MAX_INFLIGHT_FSYNCS", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_KERATIN_PIPELINE_COMMIT_RECORDS")? {
            self.storage.keratin.pipeline_commit_records =
                parse_env("FIBRIL_KERATIN_PIPELINE_COMMIT_RECORDS", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_REPLICATION_STREAM_ENABLED")? {
            self.runtime_seed.replication.stream_enabled =
                parse_env("FIBRIL_REPLICATION_STREAM_ENABLED", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_KERATIN_MESSAGE_LOG_SEGMENT_MAX_BYTES")? {
            self.storage.keratin.message_log.segment_max_bytes =
                parse_env("FIBRIL_KERATIN_MESSAGE_LOG_SEGMENT_MAX_BYTES", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_KERATIN_EVENT_LOG_SEGMENT_MAX_BYTES")? {
            self.storage.keratin.event_log.segment_max_bytes =
                parse_env("FIBRIL_KERATIN_EVENT_LOG_SEGMENT_MAX_BYTES", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_QUEUE_IDLE_EVICT_AFTER_MS")? {
            let idle = &mut self.runtime_seed.idle_queue_cleanup;
            idle.enabled = true;
            idle.evict_after_ms = parse_env("FIBRIL_QUEUE_IDLE_EVICT_AFTER_MS", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_QUEUE_IDLE_SWEEP_INTERVAL_MS")? {
            self.runtime_seed.idle_queue_cleanup.sweep_interval_ms =
                parse_env("FIBRIL_QUEUE_IDLE_SWEEP_INTERVAL_MS", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_PUBLISHER_CACHE_IDLE_TIMEOUT_MS")? {
            self.runtime_seed
                .idle_queue_cleanup
                .publisher_idle_timeout_ms =
                Some(parse_env("FIBRIL_PUBLISHER_CACHE_IDLE_TIMEOUT_MS", &value)?);
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_RECONNECT_GRACE_MS")? {
            self.runtime_seed.connection.reconnect_grace_ms =
                Some(parse_env("FIBRIL_RECONNECT_GRACE_MS", &value)?);
        }
        Ok(())
    }

    pub fn from_toml_file(path: impl AsRef<Path>) -> ConfigResult<Self> {
        let path = path.as_ref();
        let input = std::fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;
        let mut config: Self = toml::from_str(&input).map_err(|source| ConfigError::ParseFile {
            path: path.to_path_buf(),
            source,
        })?;
        config.validate()?;
        Ok(config)
    }

    pub fn idle_queue_cleanup_internal(&self) -> InternalIdleQueueCleanup {
        let idle = &self.runtime_seed.idle_queue_cleanup;
        InternalIdleQueueCleanup {
            queue_idle_evict_after_ms: idle.enabled.then_some(idle.evict_after_ms),
            queue_idle_sweep_interval_ms: idle.sweep_interval_ms,
            publisher_idle_timeout_ms: idle.publisher_idle_timeout_ms,
        }
    }

    fn apply_cli(&mut self, args: CliArgs) {
        if let Some(data_dir) = args.data_dir {
            self.server.data_dir = data_dir;
        }
        if let Some(bind) = args.broker_bind {
            self.broker.listener.bind = bind;
        }
        if let Some(bind) = args.admin_bind {
            self.admin.listener.bind = bind;
        }
        if let Some(enabled) = args.admin_auth_enabled {
            self.admin.auth.enabled = enabled;
        }
        if let Some(username) = args.admin_username {
            self.admin.auth.username = username;
        }
        if let Some(password) = args.admin_password {
            self.admin.auth.password = Some(password);
        }
        if let Some(fsync_interval_ms) = args.keratin_fsync_interval_ms {
            self.storage.keratin.fsync_interval_ms = fsync_interval_ms;
        }
        if let Some(min_fsync_interval_ms) = args.keratin_min_fsync_interval_ms {
            self.storage.keratin.min_fsync_interval_ms = min_fsync_interval_ms;
        }
        if let Some(batch_linger_ms) = args.keratin_batch_linger_ms {
            self.storage.keratin.batch_linger_ms = batch_linger_ms;
        }
        if let Some(segment_max_bytes) = args.keratin_message_log_segment_max_bytes {
            self.storage.keratin.message_log.segment_max_bytes = segment_max_bytes;
        }
        if let Some(segment_max_bytes) = args.keratin_event_log_segment_max_bytes {
            self.storage.keratin.event_log.segment_max_bytes = segment_max_bytes;
        }
        if let Some(evict_after_ms) = args.queue_idle_evict_after_ms {
            let idle = &mut self.runtime_seed.idle_queue_cleanup;
            idle.enabled = true;
            idle.evict_after_ms = evict_after_ms;
        }
        if let Some(sweep_interval_ms) = args.queue_idle_sweep_interval_ms {
            self.runtime_seed.idle_queue_cleanup.sweep_interval_ms = sweep_interval_ms;
        }
        if let Some(publisher_idle_timeout_ms) = args.publisher_idle_timeout_ms {
            self.runtime_seed
                .idle_queue_cleanup
                .publisher_idle_timeout_ms = Some(publisher_idle_timeout_ms);
        }
        if let Some(reconnect_grace_ms) = args.reconnect_grace_ms {
            self.runtime_seed.connection.reconnect_grace_ms = Some(reconnect_grace_ms);
        }
    }

    fn validate(&mut self) -> ConfigResult<()> {
        if self.server.data_dir.as_os_str().is_empty() {
            return Err(ConfigError::validation("server.data_dir must not be empty"));
        }
        if self.admin.auth.enabled {
            if self.admin.auth.username.trim().is_empty() {
                return Err(ConfigError::validation(
                    "admin.auth.username must not be empty when admin auth is enabled",
                ));
            }
            if self
                .admin
                .auth
                .password
                .as_deref()
                .map(str::trim)
                .unwrap_or_default()
                .is_empty()
            {
                return Err(ConfigError::validation(
                    "admin.auth.password must be set when admin auth is enabled",
                ));
            }
        }
        for seed in &self.auth.seed_users {
            if seed.username.trim().is_empty() || seed.password.trim().is_empty() {
                return Err(ConfigError::validation(
                    "auth.seed_users entries need both username and password (set \
                     FIBRIL_AUTH_USERNAME and FIBRIL_AUTH_PASSWORD together)",
                ));
            }
        }
        self.tls.mode()?;
        if self.tls.admin_enabled == Some(true) && !self.tls.enabled {
            return Err(ConfigError::validation(
                "tls.admin_enabled = true requires tls.enabled = true. The admin \
                 listener serves TLS from the same tls section material",
            ));
        }
        if self.runtime_seed.delivery.expiry_batch_max == 0 {
            return Err(ConfigError::validation(
                "runtime_seed.delivery.expiry_batch_max must be at least 1",
            ));
        }
        if self.storage.keratin.fsync_interval_ms == 0 {
            return Err(ConfigError::validation(
                "storage.keratin.fsync_interval_ms must be at least 1",
            ));
        }
        if self.storage.keratin.batch_linger_ms == 0 {
            return Err(ConfigError::validation(
                "storage.keratin.batch_linger_ms must be at least 1",
            ));
        }
        if self.storage.keratin.message_log.segment_max_bytes == 0 {
            return Err(ConfigError::validation(
                "storage.keratin.message_log.segment_max_bytes must be at least 1",
            ));
        }
        if self.storage.keratin.event_log.segment_max_bytes == 0 {
            return Err(ConfigError::validation(
                "storage.keratin.event_log.segment_max_bytes must be at least 1",
            ));
        }
        if self.runtime_seed.idle_queue_cleanup.sweep_interval_ms == 0 {
            return Err(ConfigError::validation(
                "runtime_seed.idle_queue_cleanup.sweep_interval_ms must be at least 1",
            ));
        }
        if self.runtime_seed.replication.caught_up_poll_ms == 0
            || self.runtime_seed.replication.retry_poll_ms == 0
            || self.runtime_seed.replication.checkpoint_retry_poll_ms == 0
        {
            return Err(ConfigError::validation(
                "runtime_seed.replication poll intervals must be at least 1ms",
            ));
        }
        if self.runtime_seed.replication.max_messages_per_read == 0
            || self.runtime_seed.replication.max_events_per_read == 0
            || self.runtime_seed.replication.max_bytes_per_read == 0
            || self.runtime_seed.replication.max_iterations_per_tick == 0
        {
            return Err(ConfigError::validation(
                "runtime_seed.replication read limits must be at least 1",
            ));
        }
        if self.runtime_seed.replication.min_in_sync_replicas == 0 {
            return Err(ConfigError::validation(
                "runtime_seed.replication.min_in_sync_replicas must be at least 1",
            ));
        }
        if self.runtime_seed.replication.isr_timeout_ms == 0 {
            return Err(ConfigError::validation(
                "runtime_seed.replication.isr_timeout_ms must be at least 1",
            ));
        }
        if self.runtime_seed.replication.owner_connect_timeout_ms == 0 {
            return Err(ConfigError::validation(
                "runtime_seed.replication.owner_connect_timeout_ms must be at least 1",
            ));
        }
        if self.runtime_seed.partitioning.default_partition_count == 0 {
            return Err(ConfigError::validation(
                "runtime_seed.partitioning.default_partition_count must be at least 1",
            ));
        }
        if self.coordination.mode == CoordinationMode::Ganglion
            && self.runtime_locks.idle_queue_cleanup
        {
            return Err(ConfigError::validation(
                "runtime_locks are standalone-only; in ganglion mode, runtime settings are cluster-authoritative",
            ));
        }
        if self.coordination.mode == CoordinationMode::Ganglion
            && self.coordination.ganglion.liveness_ttl_ms
                < 2 * self.coordination.ganglion.heartbeat_interval_ms
        {
            return Err(ConfigError::validation(
                "coordination.ganglion.liveness_ttl_ms must be at least twice \
                 heartbeat_interval_ms, or healthy nodes will flap dead on a \
                 single missed heartbeat",
            ));
        }
        let raft = &self.coordination.ganglion.raft;
        if raft.heartbeat_interval_ms == 0 {
            return Err(ConfigError::validation(
                "coordination.ganglion.raft.heartbeat_interval_ms must be at least 1",
            ));
        }
        if raft.election_timeout_min_ms <= raft.heartbeat_interval_ms {
            return Err(ConfigError::validation(
                "coordination.ganglion.raft.election_timeout_min_ms must be greater than heartbeat_interval_ms",
            ));
        }
        if raft.election_timeout_min_ms >= raft.election_timeout_max_ms {
            return Err(ConfigError::validation(
                "coordination.ganglion.raft.election_timeout_min_ms must be less than election_timeout_max_ms",
            ));
        }
        let forwarded = &self.coordination.ganglion.forwarded_write;
        if forwarded.redirect_limit == 0 {
            return Err(ConfigError::validation(
                "coordination.ganglion.forwarded_write.redirect_limit must be at least 1",
            ));
        }
        if forwarded.no_leader_base_backoff_ms == 0 || forwarded.no_leader_max_backoff_ms == 0 {
            return Err(ConfigError::validation(
                "coordination.ganglion.forwarded_write backoff values must be at least 1ms",
            ));
        }
        if forwarded.no_leader_base_backoff_ms > forwarded.no_leader_max_backoff_ms {
            return Err(ConfigError::validation(
                "coordination.ganglion.forwarded_write.no_leader_base_backoff_ms must be <= no_leader_max_backoff_ms",
            ));
        }
        let durability = &self.coordination.ganglion.assignment_durability;
        match durability.mode {
            GanglionAssignmentDurabilityMode::ReplicaAccepted
            | GanglionAssignmentDurabilityMode::ReplicaDurable => {
                if durability.nodes.unwrap_or_default() == 0 {
                    return Err(ConfigError::validation(
                        "coordination.ganglion.assignment_durability.nodes must be at least 1 for replica durability",
                    ));
                }
            }
            GanglionAssignmentDurabilityMode::LocalDurable
            | GanglionAssignmentDurabilityMode::MajorityDurable => {
                if durability.nodes.is_some() {
                    return Err(ConfigError::validation(
                        "coordination.ganglion.assignment_durability.nodes is only valid for replica durability",
                    ));
                }
            }
        }
        Ok(())
    }
}

fn env_value<F>(get: &mut F, name: &'static str) -> ConfigResult<Option<String>>
where
    F: FnMut(&str) -> Option<ConfigResult<String>>,
{
    match get(name).transpose()? {
        Some(value) if value.trim().is_empty() => Ok(None),
        value => Ok(value),
    }
}

fn optional_path_env(name: &'static str) -> ConfigResult<Option<PathBuf>> {
    Ok(optional_string_env(name)?.map(PathBuf::from))
}

fn optional_string_env(name: &str) -> ConfigResult<Option<String>> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(source) => Err(ConfigError::EnvUnicode {
            name: name.to_string(),
            source,
        }),
    }
}

fn parse_env<T>(name: &str, value: &str) -> ConfigResult<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.parse::<T>().map_err(|err| ConfigError::EnvParse {
        name: name.to_string(),
        value: value.to_string(),
        message: err.to_string(),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ServerSection {
    pub data_dir: PathBuf,
}

impl Default for ServerSection {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("server_data"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BrokerSection {
    pub listener: ListenerSection,
}

impl Default for BrokerSection {
    fn default() -> Self {
        Self {
            listener: ListenerSection {
                bind: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9876)),
                advertise: Vec::new(),
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AdminSection {
    pub listener: ListenerSection,
    pub auth: AdminAuthSection,
    /// Include per-channel series (queue ready/inflight, stream subscriptions
    /// and lag evictions) in the /metrics exposition. Node-level aggregates
    /// are always exported. Turn off for deployments with many active
    /// channels where the series count would strain the scraper.
    pub metrics_per_channel: bool,
}

impl Default for AdminSection {
    fn default() -> Self {
        Self {
            listener: ListenerSection {
                bind: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8081)),
                advertise: Vec::new(),
            },
            auth: AdminAuthSection::default(),
            metrics_per_channel: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AdminAuthSection {
    pub enabled: bool,
    pub username: String,
    pub password: Option<String>,
}

impl Default for AdminAuthSection {
    fn default() -> Self {
        Self {
            enabled: false,
            username: "fibril".into(),
            password: None,
        }
    }
}

/// TLS for the client-facing listeners. Startup config: material is loaded
/// once at boot, and certificate reload or rotation is a planned follow-up.
/// When enabled, the broker listener and the admin server both serve TLS
/// from the same material unless `admin_enabled` opts the admin listener
/// out (for deployments where a reverse proxy terminates TLS in front of
/// the dashboard while the broker speaks TLS directly).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct TlsSection {
    pub enabled: bool,
    /// PEM certificate chain, leaf first. Set together with `key_path`.
    pub cert_path: Option<PathBuf>,
    /// PEM private key for the certificate.
    pub key_path: Option<PathBuf>,
    /// Generate a per-deployment CA and server certificate under
    /// `<server.data_dir>/tls` on first boot instead of supplying PEMs. The
    /// CA fingerprint is printed at startup so clients can pin or trust it.
    /// Unverified self-signed material defeats passive snooping only.
    pub auto_self_signed: bool,
    /// TLS on the admin listener. Unset follows `enabled`.
    pub admin_enabled: Option<bool>,
    /// TLS on follower-to-owner replication connections. Unset follows
    /// `enabled`, which assumes a homogeneous cluster. Set to `false` when a
    /// service mesh or tunnel already encrypts inter-broker traffic.
    pub inter_broker: Option<bool>,
    /// PEM file with the CA that peer server certificates chain to. Unset
    /// falls back to `<data_dir>/tls/ca.pem` when present, then OS roots.
    pub peer_ca_path: Option<PathBuf>,
    /// Client-certificate policy on the TLS listeners: `off` (no client
    /// certs), `request` (a presented certificate is verified and its
    /// identity can authenticate, certless clients still connect and
    /// password-auth), or `require` (the handshake rejects certless
    /// clients).
    pub client_auth: ClientAuthMode,
    /// PEM file with the CA that client certificates chain to. Unset falls
    /// back to the generated `<data_dir>/tls/ca.pem`.
    pub client_ca_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ClientAuthMode {
    #[default]
    Off,
    Request,
    Require,
}

impl std::str::FromStr for ClientAuthMode {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "off" => Ok(Self::Off),
            "request" => Ok(Self::Request),
            "require" => Ok(Self::Require),
            other => Err(format!(
                "unknown tls.client_auth `{other}`: expected off, request or require"
            )),
        }
    }
}

impl TlsSection {
    /// Whether replication connections to peer brokers use TLS.
    pub fn inter_broker_enabled(&self) -> bool {
        self.inter_broker.unwrap_or(self.enabled)
    }
}

/// The certificate source resolved from a validated [`TlsSection`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TlsMode {
    Disabled,
    /// Operator-supplied PEM files.
    Provided {
        cert_path: PathBuf,
        key_path: PathBuf,
    },
    /// Per-deployment generated material under the data dir.
    AutoSelfSigned,
}

impl TlsSection {
    /// Resolve the certificate source. When disabled, staged paths are
    /// ignored so material can be configured ahead of turning TLS on.
    pub fn mode(&self) -> ConfigResult<TlsMode> {
        if !self.enabled {
            return Ok(TlsMode::Disabled);
        }
        if self.auto_self_signed {
            if self.cert_path.is_some() || self.key_path.is_some() {
                return Err(ConfigError::validation(
                    "tls.auto_self_signed = true conflicts with tls.cert_path and \
                     tls.key_path. Remove the paths to generate per-deployment \
                     material, or drop auto_self_signed to use the supplied files",
                ));
            }
            return Ok(TlsMode::AutoSelfSigned);
        }
        match (self.cert_path.clone(), self.key_path.clone()) {
            (Some(cert_path), Some(key_path)) => Ok(TlsMode::Provided {
                cert_path,
                key_path,
            }),
            (None, None) => Err(ConfigError::validation(
                "tls.enabled = true but no certificate source is configured. \
                 Either set tls.cert_path and tls.key_path to your PEM files, or \
                 set tls.auto_self_signed = true to generate per-deployment \
                 material under <data_dir>/tls on first boot. See \
                 https://fibril.sh/configuration/ for both paths",
            )),
            _ => Err(ConfigError::validation(
                "tls.cert_path and tls.key_path must both be set to serve TLS \
                 from supplied PEM files",
            )),
        }
    }

    /// Whether the admin server serves HTTPS. Follows `enabled` unless
    /// `admin_enabled` overrides it.
    pub fn admin_tls(&self) -> bool {
        self.enabled && self.admin_enabled.unwrap_or(true)
    }
}

/// Cluster coordination provider selection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct CoordinationSection {
    /// `static` (single-node, default) or `ganglion` (embedded raft coordinator).
    pub mode: CoordinationMode,
    /// This broker's identity in coordination snapshots.
    pub node_id: String,
    /// Cluster shared secret file. Every node holds the same secret and
    /// node-to-node connections authenticate with it (never with a user
    /// account). Resolution order: `FIBRIL_CLUSTER_SECRET` env, this path,
    /// then `<data_dir>/cluster.secret` if present. Required in ganglion
    /// mode.
    pub secret_path: Option<PathBuf>,
    pub ganglion: GanglionCoordinationSection,
}

impl Default for CoordinationSection {
    fn default() -> Self {
        Self {
            mode: CoordinationMode::Static,
            node_id: "local".into(),
            secret_path: None,
            ganglion: GanglionCoordinationSection::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CoordinationMode {
    Static,
    Ganglion,
}

impl std::str::FromStr for CoordinationMode {
    type Err = ConfigError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "static" => Ok(Self::Static),
            "ganglion" => Ok(Self::Ganglion),
            other => Err(ConfigError::UnknownCoordinationMode {
                value: other.to_string(),
            }),
        }
    }
}

/// Embedded ganglion raft coordinator settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct GanglionCoordinationSection {
    /// Raft node id (u64, transport-level; distinct from `node_id`).
    pub raft_node_id: u64,
    /// Raft RPC listen address. Must match this node's entry in `peers`.
    pub listen: SocketAddr,
    /// Raft id -> "host:port" for every cluster member, including self.
    /// (String keys because TOML tables require them.)
    pub peers: std::collections::BTreeMap<String, String>,
    /// Initialize the cluster on first boot. Exactly one node sets this;
    /// restarts ignore it once membership exists.
    pub bootstrap: bool,
    /// Raft WAL + snapshot directory. Empty = `<server.data_dir>/coordination`.
    pub data_dir: PathBuf,
    /// Outbound raft frame encoding: `msgpack` (default) or `json` (debugging).
    /// Inbound frames are self-describing, so mixed clusters interoperate.
    pub wire_format: String,
    /// Raft timing. Boot-time only: changing it requires node restart.
    pub raft: GanglionRaftSection,
    /// Policy for forwarding metadata writes from followers to the current
    /// raft leader. Boot-time only.
    pub forwarded_write: GanglionForwardedWriteSection,
    /// Broker self-registration heartbeat interval (milliseconds).
    pub heartbeat_interval_ms: u64,
    /// Controller: desired follower count per queue partition.
    pub target_followers: usize,
    /// Controller: replica follower count for a DURABLE stream partition (the
    /// durable tier; the express tiers stay owner-only). Tuned independently of
    /// `target_followers` so stream and queue fault tolerance can differ.
    pub stream_replication_factor: usize,
    /// Controller: default durability policy for newly assigned partitions.
    pub assignment_durability: GanglionAssignmentDurabilitySection,
    /// Controller: bounded tick interval (also wakes on snapshot changes).
    pub controller_tick_ms: u64,
    /// Controller: heartbeats older than this mark a broker dead. Must exceed
    /// worst-case election + retry time. Default 3x heartbeat interval.
    pub liveness_ttl_ms: u64,
    /// Controller: how long a repartition's finalize (retiring shrunk-away
    /// partitions and clearing the marker) waits for clients to adopt the new
    /// routing once the backlog has drained. Adoption is observed from client
    /// topology acks; this timeout bounds the wait so a silent or stuck client
    /// cannot stall a cutover forever (publish version-fencing remains the
    /// correctness backstop). Default 30s.
    pub repartition_adoption_timeout_ms: u64,
}

impl Default for GanglionCoordinationSection {
    fn default() -> Self {
        Self {
            raft_node_id: 1,
            listen: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 7301)),
            peers: std::collections::BTreeMap::new(),
            bootstrap: false,
            data_dir: PathBuf::new(),
            wire_format: "msgpack".into(),
            raft: GanglionRaftSection::default(),
            forwarded_write: GanglionForwardedWriteSection::default(),
            heartbeat_interval_ms: 3000,
            target_followers: 1,
            stream_replication_factor: 1,
            assignment_durability: GanglionAssignmentDurabilitySection::default(),
            controller_tick_ms: 2000,
            liveness_ttl_ms: 9000,
            repartition_adoption_timeout_ms: 30_000,
        }
    }
}

/// Default durability policy stamped on newly planned queue assignments.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct GanglionAssignmentDurabilitySection {
    pub mode: GanglionAssignmentDurabilityMode,
    pub nodes: Option<usize>,
}

impl Default for GanglionAssignmentDurabilitySection {
    fn default() -> Self {
        Self {
            mode: GanglionAssignmentDurabilityMode::LocalDurable,
            nodes: None,
        }
    }
}

impl std::str::FromStr for GanglionAssignmentDurabilitySection {
    type Err = ConfigError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let (mode, nodes) = match raw.split_once(':') {
            Some((mode, nodes)) => (
                mode,
                Some(nodes.parse::<usize>().map_err(|source| {
                    ConfigError::AssignmentDurabilityNodes {
                        value: nodes.to_string(),
                        source,
                    }
                })?),
            ),
            None => (raw, None),
        };
        Ok(Self {
            mode: mode.parse()?,
            nodes,
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GanglionAssignmentDurabilityMode {
    LocalDurable,
    ReplicaAccepted,
    ReplicaDurable,
    MajorityDurable,
}

impl Default for GanglionAssignmentDurabilityMode {
    fn default() -> Self {
        Self::LocalDurable
    }
}

impl std::str::FromStr for GanglionAssignmentDurabilityMode {
    type Err = ConfigError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "local_durable" | "local" => Ok(Self::LocalDurable),
            "replica_accepted" => Ok(Self::ReplicaAccepted),
            "replica_durable" => Ok(Self::ReplicaDurable),
            "majority_durable" | "majority" => Ok(Self::MajorityDurable),
            other => Err(ConfigError::UnknownAssignmentDurability {
                value: other.to_string(),
            }),
        }
    }
}

/// Embedded raft consensus timing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct GanglionRaftSection {
    pub heartbeat_interval_ms: u64,
    pub election_timeout_min_ms: u64,
    pub election_timeout_max_ms: u64,
}

impl Default for GanglionRaftSection {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 250,
            election_timeout_min_ms: 1500,
            election_timeout_max_ms: 3000,
        }
    }
}

/// Metadata write forwarding policy.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct GanglionForwardedWriteSection {
    pub redirect_limit: usize,
    pub no_leader_retries: usize,
    pub no_leader_base_backoff_ms: u64,
    pub no_leader_max_backoff_ms: u64,
}

impl Default for GanglionForwardedWriteSection {
    fn default() -> Self {
        Self {
            redirect_limit: 16,
            no_leader_retries: 6,
            no_leader_base_backoff_ms: 25,
            no_leader_max_backoff_ms: 250,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct StorageSection {
    pub keratin: KeratinStorageSection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KeratinStorageSection {
    pub fsync_interval_ms: u64,
    /// Floor between commits while the fsync worker is idle. 0 self-clocks
    /// group commit on fsync completions. Raise on storage where a high fsync
    /// rate is expensive.
    #[serde(default)]
    pub min_fsync_interval_ms: u64,
    #[serde(default = "default_batch_linger_ms")]
    pub batch_linger_ms: u64,
    #[serde(default = "default_message_log_section")]
    pub message_log: KeratinLogSection,
    #[serde(default = "default_event_log_section")]
    pub event_log: KeratinLogSection,
    /// In-memory tail-read cache budget in bytes for the message log (`0`
    /// disables, e.g. to keep RAM free). Recent records are served to
    /// tail-following consumers from memory instead of scanning the segment
    /// file under fsync/writeback.
    #[serde(default = "default_tail_cache_bytes")]
    pub tail_cache_bytes: usize,
    /// Bytes preallocated ahead of each active segment write cursor (`0` = off).
    /// In-place writes let fdatasync skip the block-allocation flush an extending
    /// write pays. Disk use tracks written data plus at most one chunk per log.
    #[serde(default)]
    pub segment_preallocate_bytes: usize,
    /// Commits allowed in flight to the fsync worker at once. Above 1 the worker
    /// coalesces the queued commits into one fdatasync, so small batches reach
    /// fat-batch throughput.
    #[serde(default = "default_max_inflight_fsyncs")]
    pub max_inflight_fsyncs: usize,
    /// Records-per-commit below which the writer pipelines fsyncs (fsync-count-bound).
    /// At or above it a single fsync stays in flight (bandwidth-bound).
    #[serde(default = "default_pipeline_commit_records")]
    pub pipeline_commit_records: u64,
}

fn default_max_inflight_fsyncs() -> usize {
    8
}

fn default_pipeline_commit_records() -> u64 {
    2048
}
fn default_tail_cache_bytes() -> usize {
    64 * 1024 * 1024
}

fn default_batch_linger_ms() -> u64 {
    5
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KeratinLogSection {
    pub segment_max_bytes: u64,
}

impl KeratinLogSection {
    const fn new(segment_max_bytes: u64) -> Self {
        Self { segment_max_bytes }
    }
}

impl Default for KeratinStorageSection {
    fn default() -> Self {
        Self {
            fsync_interval_ms: 5,
            min_fsync_interval_ms: 0,
            batch_linger_ms: default_batch_linger_ms(),
            message_log: default_message_log_section(),
            event_log: default_event_log_section(),
            tail_cache_bytes: default_tail_cache_bytes(),
            segment_preallocate_bytes: 0,
            max_inflight_fsyncs: default_max_inflight_fsyncs(),
            pipeline_commit_records: default_pipeline_commit_records(),
        }
    }
}

impl Default for KeratinLogSection {
    fn default() -> Self {
        Self::new(256 * 1024 * 1024)
    }
}

fn default_message_log_section() -> KeratinLogSection {
    KeratinLogSection::new(256 * 1024 * 1024)
}

fn default_event_log_section() -> KeratinLogSection {
    KeratinLogSection::new(32 * 1024 * 1024)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ListenerSection {
    pub bind: SocketAddr,
    /// Addresses to advertise instead of `bind`, in priority order (clients try
    /// them in turn). Each is a `host:port` others can actually dial - e.g. a
    /// service name when `bind` is `0.0.0.0`. Empty means derive from the
    /// coordination peer host (in ganglion mode) or fall back to `bind`. On the
    /// admin listener the first entry is what the cluster registers for the
    /// dashboard's cross-broker links. Held as raw `host:port` strings here;
    /// resolved to routable addresses at startup.
    pub advertise: Vec<String>,
}

impl Default for ListenerSection {
    fn default() -> Self {
        Self {
            bind: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
            advertise: Vec::new(),
        }
    }
}

/// Derive an advertise `host:port` from this node's own coordination peer entry
/// (its host) combined with the broker port. Returns `None` when this node has no
/// peer entry or the host is empty. The peer entry is `host:coord_port`; the host
/// is everything before the last `:` (so a bracketed IPv6 literal stays intact),
/// recombined with the broker port.
pub fn derive_advertise_from_peers(
    peers: &std::collections::BTreeMap<String, String>,
    raft_node_id: u64,
    broker_port: u16,
) -> Option<String> {
    let entry = peers.get(&raft_node_id.to_string())?;
    let host = entry
        .rsplit_once(':')
        .map(|(h, _)| h)
        .unwrap_or(entry)
        .trim();
    if host.is_empty() {
        return None;
    }
    Some(format!("{host}:{broker_port}"))
}

impl ServerConfig {
    /// The broker addresses to advertise to peers and clients, in priority order:
    /// explicitly configured entries first, then the peer-derived address (ganglion
    /// only), deduplicated. Holds only addresses we actually know - it never pads
    /// with a placeholder or the (possibly unroutable) bind address. An empty
    /// result tells the caller to fall back to the bind address.
    pub fn broker_advertise_addresses(&self) -> Vec<String> {
        let mut out: Vec<String> = Vec::new();
        for entry in &self.broker.listener.advertise {
            let entry = entry.trim();
            if !entry.is_empty() && !out.iter().any(|existing| existing == entry) {
                out.push(entry.to_string());
            }
        }
        if self.coordination.mode == CoordinationMode::Ganglion
            && let Some(derived) = derive_advertise_from_peers(
                &self.coordination.ganglion.peers,
                self.coordination.ganglion.raft_node_id,
                self.broker.listener.bind.port(),
            )
            && !out.contains(&derived)
        {
            out.push(derived);
        }
        out
    }

    /// The admin address this node registers with the cluster, feeding the
    /// dashboard's cross-broker links (the Cluster page's "open its admin" and
    /// the broker switcher). Priority: the first explicit admin advertise
    /// entry, else - when the admin bind host is unspecified and so useless to
    /// a browser - the primary broker advertise host recombined with the admin
    /// port, else the admin bind itself.
    pub fn admin_advertise_address(&self) -> String {
        if let Some(entry) = self
            .admin
            .listener
            .advertise
            .iter()
            .map(|entry| entry.trim())
            .find(|entry| !entry.is_empty())
        {
            return entry.to_string();
        }
        let bind = self.admin.listener.bind;
        if bind.ip().is_unspecified()
            && let Some(primary) = self.broker_advertise_addresses().into_iter().next()
        {
            let host = primary
                .rsplit_once(':')
                .map(|(host, _)| host)
                .unwrap_or(primary.as_str())
                .trim();
            if !host.is_empty() {
                return format!("{host}:{}", bind.port());
            }
        }
        bind.to_string()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct RuntimeSeedSection {
    pub delivery: DeliverySettings,
    pub idle_queue_cleanup: IdleQueueCleanupSettings,
    pub connection: ConnectionSettings,
    pub replication: ReplicationSettings,
    pub partitioning: PartitioningSettings,
    pub consumer_groups: ConsumerGroupSettings,
}

/// Seed values for the partitioning runtime settings.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PartitioningSettings {
    pub default_partition_count: u32,
}

impl Default for PartitioningSettings {
    fn default() -> Self {
        Self {
            default_partition_count: 1,
        }
    }
}

/// Seed values for exclusive consumer-group runtime settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConsumerGroupSettings {
    /// Soft target partitions-per-consumer for an exclusive cohort (alert/
    /// autoscale signal; never reduces coverage). `None` disables it.
    pub default_target_per_consumer: Option<usize>,
}

/// Seed values for the replication runtime settings (cluster-replicated
/// after first boot; this section only sets the initial document).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ReplicationSettings {
    pub confirm_timeout_ms: u64,
    pub caught_up_poll_ms: u64,
    pub retry_poll_ms: u64,
    pub checkpoint_retry_poll_ms: u64,
    pub max_messages_per_read: usize,
    pub max_events_per_read: usize,
    pub max_bytes_per_read: usize,
    pub max_iterations_per_tick: usize,
    pub min_in_sync_replicas: usize,
    pub isr_timeout_ms: u64,
    /// Slack added to a follower read's long-poll window before the read is
    /// abandoned and the connection dropped (a read waits `max_wait_ms + this`).
    #[serde(default = "default_read_timeout_slack_ms")]
    pub read_timeout_slack_ms: u64,
    /// Upper bound on establishing a follower-to-owner connection (TCP connect
    /// plus the HELLO/AUTH handshake) before it is abandoned and retried.
    #[serde(default = "default_owner_connect_timeout_ms")]
    pub owner_connect_timeout_ms: u64,
    /// Use credit-based streaming replication on the follower (default true after
    /// the microbatch fold + failover-under-load validation; pull stays the
    /// automatic fallback on checkpoint/error).
    #[serde(default = "default_stream_enabled")]
    pub stream_enabled: bool,
    /// How long the streaming follower lingers to gather more contiguous frames
    /// before applying (and fsyncing) them as one batch. The applier always drains
    /// frames already queued (free coalescing under backlog); this bounds the extra
    /// wait it spends gathering more when nearly caught up. Higher = better fsync
    /// amortization (throughput) at the cost of apply latency; 0 = drain-only.
    #[serde(default = "default_stream_apply_linger_us")]
    pub stream_apply_linger_us: u64,

    /// Byte cap on a single coalesced streaming-apply: the applier folds
    /// contiguous same-epoch frames into one apply (one follower fsync), but
    /// stops growing one apply past this so a deep backlog still applies in
    /// reasonable chunks. Higher = better fsync amortization at the cost of peak
    /// memory per apply. Pairs with `stream_apply_linger_us`.
    #[serde(default = "default_stream_apply_max_merge_bytes")]
    pub stream_apply_max_merge_bytes: u64,

    /// How many replicated batches the streaming follower buffers in flight (the
    /// credit window depth). Read at stream establish (setup-time), so a change
    /// takes effect on the next stream, not mid-stream.
    #[serde(default = "default_stream_buffer_batches")]
    pub stream_buffer_batches: usize,
}

fn default_read_timeout_slack_ms() -> u64 {
    10_000
}

fn default_owner_connect_timeout_ms() -> u64 {
    5_000
}

fn default_stream_apply_linger_us() -> u64 {
    2_000
}

fn default_stream_apply_max_merge_bytes() -> u64 {
    16 * 1024 * 1024
}

fn default_stream_buffer_batches() -> usize {
    8
}

impl Default for ReplicationSettings {
    fn default() -> Self {
        Self {
            confirm_timeout_ms: 5_000,
            caught_up_poll_ms: 1_000,
            retry_poll_ms: 100,
            checkpoint_retry_poll_ms: 5_000,
            // The follower does one fsync per replicated append call (per log),
            // so each fsync amortizes over at most this many records. At 256 the
            // fsync rate (msg + event logs, x iterations) saturates a contended
            // disk well below useful replica-durable throughput. 2048 cuts the
            // fsync rate ~8x; max_bytes_per_read still bounds per-batch memory for
            // large payloads.
            max_messages_per_read: 2048,
            max_events_per_read: 2048,
            max_bytes_per_read: 8 * 1024 * 1024,
            max_iterations_per_tick: 8,
            min_in_sync_replicas: 1,
            isr_timeout_ms: 10_000,
            read_timeout_slack_ms: default_read_timeout_slack_ms(),
            owner_connect_timeout_ms: default_owner_connect_timeout_ms(),
            stream_enabled: default_stream_enabled(),
            stream_apply_linger_us: default_stream_apply_linger_us(),
            stream_apply_max_merge_bytes: default_stream_apply_max_merge_bytes(),
            stream_buffer_batches: default_stream_buffer_batches(),
        }
    }
}

fn default_stream_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct DeliverySettings {
    pub inflight_ttl_ms: u64,
    pub expiry_poll_min_ms: u64,
    pub expiry_batch_max: usize,
    pub delivery_poll_max_ms: u64,
}

impl Default for DeliverySettings {
    fn default() -> Self {
        Self {
            inflight_ttl_ms: 30_000,
            expiry_poll_min_ms: 15_000,
            expiry_batch_max: 8192,
            delivery_poll_max_ms: 5_000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct IdleQueueCleanupSettings {
    pub enabled: bool,
    pub evict_after_ms: u64,
    pub sweep_interval_ms: u64,
    pub publisher_idle_timeout_ms: Option<u64>,
}

impl Default for IdleQueueCleanupSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            evict_after_ms: 600_000,
            sweep_interval_ms: 60_000,
            publisher_idle_timeout_ms: None,
        }
    }
}

/// Default reconnect grace window for the server seed. Grace is on by default so
/// a transient client blip resumes transparently instead of churning redelivery
/// and resubscribe. Operators opt out by setting `reconnect_grace_ms = 0`.
pub const DEFAULT_RECONNECT_GRACE_MS: u64 = 5_000;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectionSettings {
    pub reconnect_grace_ms: Option<u64>,
}

impl Default for ConnectionSettings {
    fn default() -> Self {
        Self {
            reconnect_grace_ms: Some(DEFAULT_RECONNECT_GRACE_MS),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct RuntimeLocksSection {
    pub idle_queue_cleanup: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InternalIdleQueueCleanup {
    pub queue_idle_evict_after_ms: Option<u64>,
    pub queue_idle_sweep_interval_ms: u64,
    pub publisher_idle_timeout_ms: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derive_advertise_uses_own_peer_host_with_broker_port() {
        let mut peers = std::collections::BTreeMap::new();
        peers.insert("1".to_string(), "broker-1:7000".to_string());
        peers.insert("2".to_string(), "broker-2:7000".to_string());
        // Our raft id is 2, broker port 9876 -> advertise broker-2:9876.
        assert_eq!(
            derive_advertise_from_peers(&peers, 2, 9876),
            Some("broker-2:9876".to_string())
        );
        // No entry for this node -> nothing derived.
        assert_eq!(derive_advertise_from_peers(&peers, 9, 9876), None);
    }

    #[test]
    fn derive_advertise_keeps_ipv6_host_intact() {
        let mut peers = std::collections::BTreeMap::new();
        peers.insert("1".to_string(), "[::1]:7000".to_string());
        assert_eq!(
            derive_advertise_from_peers(&peers, 1, 9876),
            Some("[::1]:9876".to_string())
        );
    }

    #[test]
    fn broker_advertise_addresses_explicit_then_derived_deduped() {
        let mut config = ServerConfig::default();
        config.coordination.mode = CoordinationMode::Ganglion;
        config.coordination.ganglion.raft_node_id = 1;
        config
            .coordination
            .ganglion
            .peers
            .insert("1".to_string(), "broker-1:7000".to_string());
        config.broker.listener.bind = "0.0.0.0:9876".parse().unwrap();
        // Explicit entry first, then the peer-derived one.
        config.broker.listener.advertise = vec!["public.example:9876".to_string()];
        assert_eq!(
            config.broker_advertise_addresses(),
            vec![
                "public.example:9876".to_string(),
                "broker-1:9876".to_string()
            ]
        );
        // The derived entry is not duplicated if already listed explicitly.
        config.broker.listener.advertise = vec!["broker-1:9876".to_string()];
        assert_eq!(
            config.broker_advertise_addresses(),
            vec!["broker-1:9876".to_string()]
        );
    }

    #[test]
    fn broker_advertise_addresses_empty_when_static_and_unset() {
        let config = ServerConfig::default();
        // Static mode, no explicit advertise -> nothing known, caller uses bind.
        assert!(config.broker_advertise_addresses().is_empty());
    }

    #[test]
    fn admin_advertise_explicit_entry_wins() {
        let mut config = ServerConfig::default();
        config.admin.listener.bind = "0.0.0.0:8081".parse().unwrap();
        config.admin.listener.advertise =
            vec!["".to_string(), "dash.example:8081".to_string()];
        assert_eq!(config.admin_advertise_address(), "dash.example:8081");
    }

    #[test]
    fn admin_advertise_substitutes_broker_host_when_bind_unspecified() {
        let mut config = ServerConfig::default();
        config.admin.listener.bind = "0.0.0.0:8081".parse().unwrap();
        config.broker.listener.advertise = vec!["broker-2.example:9876".to_string()];
        // Unspecified admin host is useless to a browser - reuse the broker
        // advertise host with the admin port.
        assert_eq!(config.admin_advertise_address(), "broker-2.example:8081");

        // A bracketed IPv6 broker host stays intact.
        config.broker.listener.advertise = vec!["[2001:db8::7]:9876".to_string()];
        assert_eq!(config.admin_advertise_address(), "[2001:db8::7]:8081");
    }

    #[test]
    fn admin_advertise_falls_back_to_the_bind() {
        let mut config = ServerConfig::default();
        // A specific bind host is already reachable - registered as is.
        config.admin.listener.bind = "192.0.2.9:8081".parse().unwrap();
        config.broker.listener.advertise = vec!["broker-2.example:9876".to_string()];
        assert_eq!(config.admin_advertise_address(), "192.0.2.9:8081");

        // Unspecified bind with nothing to derive from still registers the
        // bind - the dashboard suppresses the link for unspecified hosts.
        config.admin.listener.bind = "0.0.0.0:8081".parse().unwrap();
        config.broker.listener.advertise = Vec::new();
        assert_eq!(config.admin_advertise_address(), "0.0.0.0:8081");
    }

    #[test]
    fn admin_advertise_env_override_applies() {
        let mut config = ServerConfig::default();
        config
            .apply_env_from(|name| match name {
                "FIBRIL_ADMIN_ADVERTISE" => Some(Ok("127.0.0.1:8082, ".to_string())),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            config.admin.listener.advertise,
            vec!["127.0.0.1:8082".to_string()]
        );
        assert_eq!(config.admin_advertise_address(), "127.0.0.1:8082");
    }

    #[test]
    fn defaults_match_current_server_behavior() {
        let config = ServerConfig::default();

        assert_eq!(config.server.data_dir, PathBuf::from("server_data"));
        assert_eq!(
            config.broker.listener.bind,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 9876))
        );
        assert_eq!(
            config.admin.listener.bind,
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8081))
        );
        assert!(!config.admin.auth.enabled);
        assert_eq!(config.admin.auth.username, "fibril");
        assert_eq!(config.admin.auth.password, None);
        assert_eq!(config.tls, TlsSection::default());
        assert_eq!(config.tls.mode().unwrap(), TlsMode::Disabled);
        assert!(!config.tls.admin_tls());
        assert_eq!(config.storage.keratin.fsync_interval_ms, 5);
        assert_eq!(
            config.storage.keratin.message_log.segment_max_bytes,
            256 * 1024 * 1024
        );
        assert_eq!(
            config.storage.keratin.event_log.segment_max_bytes,
            32 * 1024 * 1024
        );
        assert_eq!(config.runtime_seed.delivery.inflight_ttl_ms, 30_000);
        assert_eq!(config.runtime_seed.delivery.expiry_poll_min_ms, 15_000);
        assert_eq!(config.runtime_seed.delivery.expiry_batch_max, 8192);
        assert_eq!(config.runtime_seed.delivery.delivery_poll_max_ms, 5_000);
        assert_eq!(
            config.coordination.ganglion.raft,
            GanglionRaftSection {
                heartbeat_interval_ms: 250,
                election_timeout_min_ms: 1500,
                election_timeout_max_ms: 3000,
            }
        );
        assert_eq!(
            config.coordination.ganglion.forwarded_write,
            GanglionForwardedWriteSection {
                redirect_limit: 16,
                no_leader_retries: 6,
                no_leader_base_backoff_ms: 25,
                no_leader_max_backoff_ms: 250,
            }
        );
        assert_eq!(
            config.coordination.ganglion.assignment_durability,
            GanglionAssignmentDurabilitySection {
                mode: GanglionAssignmentDurabilityMode::LocalDurable,
                nodes: None,
            }
        );
        assert_eq!(
            config.runtime_seed.connection.reconnect_grace_ms,
            Some(DEFAULT_RECONNECT_GRACE_MS)
        );
        assert_eq!(
            config.idle_queue_cleanup_internal(),
            InternalIdleQueueCleanup {
                queue_idle_evict_after_ms: None,
                queue_idle_sweep_interval_ms: 60_000,
                publisher_idle_timeout_ms: None,
            }
        );
    }

    #[test]
    fn toml_overrides_nested_sections() {
        let config = ServerConfig::from_toml_str(
            r#"
            [server]
            data_dir = "data"

            [broker.listener]
            bind = "127.0.0.1:9000"

            [admin.listener]
            bind = "127.0.0.1:9001"

            [admin.auth]
            enabled = true
            username = "admin"
            password = "secret"

            [storage.keratin]
            fsync_interval_ms = 20

            [storage.keratin.message_log]
            segment_max_bytes = 134217728

            [storage.keratin.event_log]
            segment_max_bytes = 16777216

            [runtime_seed.delivery]
            inflight_ttl_ms = 10
            expiry_poll_min_ms = 11
            expiry_batch_max = 12
            delivery_poll_max_ms = 13

            [runtime_seed.idle_queue_cleanup]
            enabled = true
            evict_after_ms = 14
            sweep_interval_ms = 15
            publisher_idle_timeout_ms = 16

            [runtime_seed.connection]
            reconnect_grace_ms = 17

            [runtime_seed.replication]
            confirm_timeout_ms = 18
            caught_up_poll_ms = 19
            retry_poll_ms = 20
            checkpoint_retry_poll_ms = 21
            max_messages_per_read = 22
            max_events_per_read = 23
            max_bytes_per_read = 24
            max_iterations_per_tick = 25
            min_in_sync_replicas = 2
            isr_timeout_ms = 26

            [coordination.ganglion]
            heartbeat_interval_ms = 3000
            liveness_ttl_ms = 12000

            [coordination.ganglion.raft]
            heartbeat_interval_ms = 300
            election_timeout_min_ms = 1800
            election_timeout_max_ms = 3600

            [coordination.ganglion.forwarded_write]
            redirect_limit = 20
            no_leader_retries = 2
            no_leader_base_backoff_ms = 30
            no_leader_max_backoff_ms = 300

            [coordination.ganglion.assignment_durability]
            mode = "replica_durable"
            nodes = 2

            [runtime_locks]
            idle_queue_cleanup = true
            "#,
        )
        .unwrap();

        assert_eq!(config.server.data_dir, PathBuf::from("data"));
        assert_eq!(
            config.broker.listener.bind,
            "127.0.0.1:9000".parse().unwrap()
        );
        assert_eq!(
            config.admin.listener.bind,
            "127.0.0.1:9001".parse().unwrap()
        );
        assert!(config.admin.auth.enabled);
        assert_eq!(config.admin.auth.username, "admin");
        assert_eq!(config.admin.auth.password.as_deref(), Some("secret"));
        assert_eq!(config.storage.keratin.fsync_interval_ms, 20);
        assert_eq!(
            config.storage.keratin.message_log.segment_max_bytes,
            134_217_728
        );
        assert_eq!(
            config.storage.keratin.event_log.segment_max_bytes,
            16_777_216
        );
        assert_eq!(
            config.coordination.ganglion.raft,
            GanglionRaftSection {
                heartbeat_interval_ms: 300,
                election_timeout_min_ms: 1800,
                election_timeout_max_ms: 3600,
            }
        );
        assert_eq!(
            config.coordination.ganglion.forwarded_write,
            GanglionForwardedWriteSection {
                redirect_limit: 20,
                no_leader_retries: 2,
                no_leader_base_backoff_ms: 30,
                no_leader_max_backoff_ms: 300,
            }
        );
        assert_eq!(
            config.coordination.ganglion.assignment_durability,
            GanglionAssignmentDurabilitySection {
                mode: GanglionAssignmentDurabilityMode::ReplicaDurable,
                nodes: Some(2),
            }
        );
        assert_eq!(config.runtime_seed.delivery.inflight_ttl_ms, 10);
        assert_eq!(config.runtime_seed.delivery.expiry_poll_min_ms, 11);
        assert_eq!(config.runtime_seed.delivery.expiry_batch_max, 12);
        assert_eq!(config.runtime_seed.delivery.delivery_poll_max_ms, 13);
        assert_eq!(
            config.idle_queue_cleanup_internal(),
            InternalIdleQueueCleanup {
                queue_idle_evict_after_ms: Some(14),
                queue_idle_sweep_interval_ms: 15,
                publisher_idle_timeout_ms: Some(16),
            }
        );
        assert_eq!(config.runtime_seed.connection.reconnect_grace_ms, Some(17));
        assert_eq!(config.runtime_seed.replication.confirm_timeout_ms, 18);
        assert_eq!(config.runtime_seed.replication.caught_up_poll_ms, 19);
        assert_eq!(config.runtime_seed.replication.retry_poll_ms, 20);
        assert_eq!(config.runtime_seed.replication.checkpoint_retry_poll_ms, 21);
        assert_eq!(config.runtime_seed.replication.max_messages_per_read, 22);
        assert_eq!(config.runtime_seed.replication.max_events_per_read, 23);
        assert_eq!(config.runtime_seed.replication.max_bytes_per_read, 24);
        assert_eq!(config.runtime_seed.replication.max_iterations_per_tick, 25);
        assert_eq!(config.runtime_seed.replication.min_in_sync_replicas, 2);
        assert_eq!(config.runtime_seed.replication.isr_timeout_ms, 26);
        assert_eq!(config.coordination.ganglion.heartbeat_interval_ms, 3000);
        assert_eq!(config.coordination.ganglion.liveness_ttl_ms, 12000);
        assert!(config.runtime_locks.idle_queue_cleanup);
    }

    #[test]
    fn ganglion_mode_rejects_node_local_runtime_locks() {
        let err = ServerConfig::from_toml_str(
            r#"
            [server]
            data_dir = "data"

            [coordination]
            mode = "ganglion"

            [runtime_locks]
            idle_queue_cleanup = true
            "#,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("runtime_locks are standalone-only")
        );
    }

    #[test]
    fn static_mode_allows_node_local_runtime_locks() {
        let config = ServerConfig::from_toml_str(
            r#"
            [server]
            data_dir = "data"

            [coordination]
            mode = "static"

            [runtime_locks]
            idle_queue_cleanup = true
            "#,
        )
        .unwrap();

        assert_eq!(config.coordination.mode, CoordinationMode::Static);
        assert!(config.runtime_locks.idle_queue_cleanup);
    }

    #[test]
    fn runtime_seed_validation_matches_runtime_settings_rules() {
        let err = ServerConfig::from_toml_str(
            r#"
            [server]
            data_dir = "data"

            [runtime_seed.partitioning]
            default_partition_count = 0
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("default_partition_count"));
    }

    #[test]
    fn ganglion_raft_timing_validation_rejects_unstable_ordering() {
        let err = ServerConfig::from_toml_str(
            r#"
            [server]
            data_dir = "data"

            [coordination.ganglion.raft]
            heartbeat_interval_ms = 300
            election_timeout_min_ms = 300
            election_timeout_max_ms = 400
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("election_timeout_min_ms"));
    }

    #[test]
    fn forwarded_write_validation_rejects_zero_redirect_limit() {
        let err = ServerConfig::from_toml_str(
            r#"
            [server]
            data_dir = "data"

            [coordination.ganglion.forwarded_write]
            redirect_limit = 0
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("redirect_limit"));
    }

    #[test]
    fn cli_overrides_toml_values() {
        let dir = tempfile_path("fibril-config-cli.toml");
        std::fs::write(
            &dir,
            r#"
            [server]
            data_dir = "from-file"

            [broker.listener]
            bind = "127.0.0.1:9000"
            "#,
        )
        .unwrap();

        let config = ServerConfig::load_from_args([
            OsString::from("fibril-server"),
            OsString::from("--config"),
            dir.into_os_string(),
            OsString::from("--data-dir"),
            OsString::from("from-cli"),
            OsString::from("--broker-bind"),
            OsString::from("127.0.0.1:7777"),
            OsString::from("--admin-auth-enabled"),
            OsString::from("true"),
            OsString::from("--admin-username"),
            OsString::from("cli-admin"),
            OsString::from("--admin-password"),
            OsString::from("cli-secret"),
            OsString::from("--keratin-fsync-interval-ms"),
            OsString::from("25"),
            OsString::from("--keratin-message-log-segment-max-bytes"),
            OsString::from("67108864"),
            OsString::from("--keratin-event-log-segment-max-bytes"),
            OsString::from("8388608"),
            OsString::from("--queue-idle-evict-after-ms"),
            OsString::from("123"),
            OsString::from("--reconnect-grace-ms"),
            OsString::from("456"),
        ])
        .unwrap();

        assert_eq!(config.server.data_dir, PathBuf::from("from-cli"));
        assert_eq!(
            config.broker.listener.bind,
            "127.0.0.1:7777".parse().unwrap()
        );
        assert!(config.admin.auth.enabled);
        assert_eq!(config.admin.auth.username, "cli-admin");
        assert_eq!(config.admin.auth.password.as_deref(), Some("cli-secret"));
        assert_eq!(config.storage.keratin.fsync_interval_ms, 25);
        assert_eq!(
            config.storage.keratin.message_log.segment_max_bytes,
            67_108_864
        );
        assert_eq!(
            config.storage.keratin.event_log.segment_max_bytes,
            8_388_608
        );
        assert_eq!(
            config
                .idle_queue_cleanup_internal()
                .queue_idle_evict_after_ms,
            Some(123)
        );
        assert_eq!(config.runtime_seed.connection.reconnect_grace_ms, Some(456));
    }

    #[test]
    fn load_file_and_env_uses_explicit_config_without_server_cli_args() {
        let dir = tempfile_path("fibril-config-tool.toml");
        std::fs::write(
            &dir,
            r#"
            [server]
            data_dir = "from-tool-file"

            [broker.listener]
            bind = "127.0.0.1:9123"
            "#,
        )
        .unwrap();

        let config = ServerConfig::load_file_and_env(Some(dir.clone())).unwrap();

        assert_eq!(config.server.data_dir, PathBuf::from("from-tool-file"));
        assert_eq!(
            config.broker.listener.bind,
            "127.0.0.1:9123".parse().unwrap()
        );
        let _ = std::fs::remove_file(dir);
    }

    #[test]
    fn env_overrides_toml_and_empty_values_are_ignored() {
        let mut config = ServerConfig::from_toml_str(
            r#"
            [server]
            data_dir = "from-file"

            [runtime_seed.idle_queue_cleanup]
            enabled = false
            evict_after_ms = 100
            sweep_interval_ms = 200
            "#,
        )
        .unwrap();

        config
            .apply_env_from(|name| match name {
                "FIBRIL_DATA_DIR" => Some(Ok("from-env".to_string())),
                "FIBRIL_KERATIN_FSYNC_INTERVAL_MS" => Some(Ok("30".to_string())),
                "FIBRIL_KERATIN_MESSAGE_LOG_SEGMENT_MAX_BYTES" => Some(Ok("33554432".to_string())),
                "FIBRIL_KERATIN_EVENT_LOG_SEGMENT_MAX_BYTES" => Some(Ok("4194304".to_string())),
                "FIBRIL_ADMIN_AUTH_ENABLED" => Some(Ok("true".to_string())),
                "FIBRIL_ADMIN_USERNAME" => Some(Ok("env-admin".to_string())),
                "FIBRIL_ADMIN_PASSWORD" => Some(Ok("env-secret".to_string())),
                "FIBRIL_ADMIN_METRICS_PER_CHANNEL" => Some(Ok("false".to_string())),
                "FIBRIL_QUEUE_IDLE_EVICT_AFTER_MS" => Some(Ok("123".to_string())),
                "FIBRIL_QUEUE_IDLE_SWEEP_INTERVAL_MS" => Some(Ok("".to_string())),
                "FIBRIL_RECONNECT_GRACE_MS" => Some(Ok("789".to_string())),
                "FIBRIL_COORDINATION_HEARTBEAT_INTERVAL_MS" => Some(Ok("1000".to_string())),
                "FIBRIL_COORDINATION_LIVENESS_TTL_MS" => Some(Ok("20000".to_string())),
                "FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY" => {
                    Some(Ok("replica_durable:2".to_string()))
                }
                "FIBRIL_RECOVERY_ON_MISMATCH" => Some(Ok("refuse".to_string())),
                _ => None,
            })
            .unwrap();

        assert_eq!(config.server.data_dir, PathBuf::from("from-env"));
        assert!(config.admin.auth.enabled);
        assert_eq!(config.admin.auth.username, "env-admin");
        assert_eq!(config.admin.auth.password.as_deref(), Some("env-secret"));
        assert!(!config.admin.metrics_per_channel);
        assert_eq!(config.storage.keratin.fsync_interval_ms, 30);
        assert_eq!(
            config.storage.keratin.message_log.segment_max_bytes,
            33_554_432
        );
        assert_eq!(
            config.storage.keratin.event_log.segment_max_bytes,
            4_194_304
        );
        assert_eq!(
            config.idle_queue_cleanup_internal(),
            InternalIdleQueueCleanup {
                queue_idle_evict_after_ms: Some(123),
                queue_idle_sweep_interval_ms: 200,
                publisher_idle_timeout_ms: None,
            }
        );
        assert_eq!(config.runtime_seed.connection.reconnect_grace_ms, Some(789));
        assert_eq!(config.coordination.ganglion.heartbeat_interval_ms, 1000);
        assert_eq!(config.coordination.ganglion.liveness_ttl_ms, 20000);
        assert_eq!(
            config.coordination.ganglion.assignment_durability,
            GanglionAssignmentDurabilitySection {
                mode: GanglionAssignmentDurabilityMode::ReplicaDurable,
                nodes: Some(2),
            }
        );
        assert_eq!(config.recovery.on_mismatch, RecoveryMismatchMode::Refuse);
    }

    #[test]
    fn rejects_invalid_zero_values() {
        let err = ServerConfig::from_toml_str(
            r#"
            [runtime_seed.idle_queue_cleanup]
            sweep_interval_ms = 0
            "#,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("runtime_seed.idle_queue_cleanup.sweep_interval_ms")
        );
    }

    #[test]
    fn rejects_invalid_storage_values() {
        let err = ServerConfig::from_toml_str(
            r#"
            [storage.keratin]
            fsync_interval_ms = 0
            "#,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("storage.keratin.fsync_interval_ms")
        );

        let err = ServerConfig::from_toml_str(
            r#"
            [storage.keratin.message_log]
            segment_max_bytes = 0
            "#,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("storage.keratin.message_log.segment_max_bytes")
        );

        let err = ServerConfig::from_toml_str(
            r#"
            [storage.keratin.event_log]
            segment_max_bytes = 0
            "#,
        )
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("storage.keratin.event_log.segment_max_bytes")
        );
    }

    #[test]
    fn tls_provided_paths_resolve_and_cover_admin_by_default() {
        let config = ServerConfig::from_toml_str(
            r#"
            [tls]
            enabled = true
            cert_path = "/etc/fibril/tls/server.pem"
            key_path = "/etc/fibril/tls/server.key"
            "#,
        )
        .unwrap();

        assert_eq!(
            config.tls.mode().unwrap(),
            TlsMode::Provided {
                cert_path: PathBuf::from("/etc/fibril/tls/server.pem"),
                key_path: PathBuf::from("/etc/fibril/tls/server.key"),
            }
        );
        assert!(config.tls.admin_tls());
    }

    #[test]
    fn tls_admin_override_opts_the_admin_listener_out() {
        let config = ServerConfig::from_toml_str(
            r#"
            [tls]
            enabled = true
            auto_self_signed = true
            admin_enabled = false
            "#,
        )
        .unwrap();

        assert_eq!(config.tls.mode().unwrap(), TlsMode::AutoSelfSigned);
        assert!(!config.tls.admin_tls());
    }

    #[test]
    fn tls_enabled_without_a_source_names_both_fixes() {
        let err = ServerConfig::from_toml_str(
            r#"
            [tls]
            enabled = true
            "#,
        )
        .unwrap_err();

        let message = err.to_string();
        assert!(message.contains("tls.cert_path and tls.key_path"));
        assert!(message.contains("tls.auto_self_signed"));
    }

    #[test]
    fn tls_rejects_auto_self_signed_combined_with_paths() {
        let err = ServerConfig::from_toml_str(
            r#"
            [tls]
            enabled = true
            auto_self_signed = true
            cert_path = "/etc/fibril/tls/server.pem"
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("conflicts"));
    }

    #[test]
    fn tls_rejects_a_lone_cert_or_key_path() {
        let err = ServerConfig::from_toml_str(
            r#"
            [tls]
            enabled = true
            cert_path = "/etc/fibril/tls/server.pem"
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("must both be set"));
    }

    #[test]
    fn tls_rejects_admin_enabled_without_tls_enabled() {
        let err = ServerConfig::from_toml_str(
            r#"
            [tls]
            admin_enabled = true
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("tls.enabled = true"));
    }

    #[test]
    fn tls_disabled_ignores_staged_material() {
        let config = ServerConfig::from_toml_str(
            r#"
            [tls]
            enabled = false
            cert_path = "/etc/fibril/tls/server.pem"
            key_path = "/etc/fibril/tls/server.key"
            "#,
        )
        .unwrap();

        assert_eq!(config.tls.mode().unwrap(), TlsMode::Disabled);
        assert!(!config.tls.admin_tls());
    }

    #[test]
    fn tls_env_overrides_apply() {
        let mut config = ServerConfig::default();
        config
            .apply_env_from(|name| match name {
                "FIBRIL_TLS_ENABLED" => Some(Ok("true".to_string())),
                "FIBRIL_TLS_CERT_PATH" => Some(Ok("/env/server.pem".to_string())),
                "FIBRIL_TLS_KEY_PATH" => Some(Ok("/env/server.key".to_string())),
                "FIBRIL_TLS_ADMIN_ENABLED" => Some(Ok("false".to_string())),
                "FIBRIL_TLS_INTER_BROKER" => Some(Ok("false".to_string())),
                "FIBRIL_TLS_PEER_CA_PATH" => Some(Ok("/env/peer-ca.pem".to_string())),
                _ => None,
            })
            .unwrap();

        assert_eq!(
            config.tls.mode().unwrap(),
            TlsMode::Provided {
                cert_path: PathBuf::from("/env/server.pem"),
                key_path: PathBuf::from("/env/server.key"),
            }
        );
        assert!(!config.tls.admin_tls());
        assert!(!config.tls.inter_broker_enabled());
        assert_eq!(
            config.tls.peer_ca_path.as_deref(),
            Some(std::path::Path::new("/env/peer-ca.pem"))
        );
    }

    #[test]
    fn inter_broker_tls_follows_enabled_when_unset() {
        let mut config = ServerConfig::default();
        assert!(!config.tls.inter_broker_enabled());
        config.tls.enabled = true;
        assert!(config.tls.inter_broker_enabled());
        config.tls.inter_broker = Some(false);
        assert!(!config.tls.inter_broker_enabled());
    }

    #[test]
    fn setup_overlay_applies_only_when_tls_is_unset() {
        let dir = std::env::temp_dir().join(format!(
            "fibril-overlay-{}-{}",
            std::process::id(),
            fastrand_like_seed()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join(CONFIG_OVERLAY_FILE),
            "[tls]\nenabled = true\nauto_self_signed = true\n",
        )
        .unwrap();

        // Unset tls -> the overlay applies.
        let mut config = ServerConfig::default();
        config.server.data_dir = dir.clone();
        assert!(config.apply_setup_overlay().unwrap());
        assert!(config.tls.enabled);
        assert_eq!(config.tls.mode().unwrap(), TlsMode::AutoSelfSigned);

        // Explicit tls in config -> the overlay is ignored.
        let mut config = ServerConfig::default();
        config.server.data_dir = dir.clone();
        config.tls.enabled = true;
        config.tls.cert_path = Some(PathBuf::from("/etc/fibril/server.pem"));
        config.tls.key_path = Some(PathBuf::from("/etc/fibril/server.key"));
        assert!(!config.apply_setup_overlay().unwrap());
        assert!(config.tls.cert_path.is_some());

        // No overlay file -> a no-op.
        let mut config = ServerConfig::default();
        config.server.data_dir = dir.join("missing");
        assert!(!config.apply_setup_overlay().unwrap());

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn setup_pending_requires_mode_and_no_marker() {
        let dir = std::env::temp_dir().join(format!(
            "fibril-setup-pending-{}-{}",
            std::process::id(),
            fastrand_like_seed()
        ));
        std::fs::create_dir_all(&dir).unwrap();

        let mut config = ServerConfig::default();
        config.server.data_dir = dir.clone();
        assert!(!config.setup_pending());
        config.setup.mode = true;
        assert!(config.setup_pending());
        std::fs::write(dir.join(SETUP_MARKER_FILE), "").unwrap();
        assert!(!config.setup_pending());

        let _ = std::fs::remove_dir_all(dir);
    }

    fn fastrand_like_seed() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default()
    }

    #[test]
    fn rejects_enabled_admin_auth_without_credentials() {
        let err = ServerConfig::from_toml_str(
            r#"
            [admin.auth]
            enabled = true
            username = ""
            password = "secret"
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("admin.auth.username"));

        let err = ServerConfig::from_toml_str(
            r#"
            [admin.auth]
            enabled = true
            username = "admin"
            "#,
        )
        .unwrap_err();

        assert!(err.to_string().contains("admin.auth.password"));
    }

    fn tempfile_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("{name}-{}", std::process::id()));
        path
    }
}
