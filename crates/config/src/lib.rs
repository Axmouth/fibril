use std::{
    ffi::OsString,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
};

use clap::Parser;
use serde::{Deserialize, Serialize};

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
    pub storage: StorageSection,
    pub runtime_seed: RuntimeSeedSection,
    pub runtime_locks: RuntimeLocksSection,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            server: ServerSection::default(),
            broker: BrokerSection::default(),
            admin: AdminSection::default(),
            storage: StorageSection::default(),
            runtime_seed: RuntimeSeedSection::default(),
            runtime_locks: RuntimeLocksSection::default(),
        }
    }
}

impl ServerConfig {
    pub fn load() -> anyhow::Result<Self> {
        Self::load_from_args(std::env::args_os())
    }

    pub fn load_file_and_env(config_path: Option<PathBuf>) -> anyhow::Result<Self> {
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

    pub fn load_from_args<I, T>(args: I) -> anyhow::Result<Self>
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

    pub fn from_toml_str(input: &str) -> anyhow::Result<Self> {
        let mut config: Self = toml::from_str(input)?;
        config.validate()?;
        Ok(config)
    }

    fn apply_env(&mut self) -> anyhow::Result<()> {
        self.apply_env_from(|name| match optional_string_env(name) {
            Ok(Some(value)) => Some(Ok(value)),
            Ok(None) => None,
            Err(err) => Some(Err(err)),
        })
    }

    fn apply_env_from<F>(&mut self, mut get: F) -> anyhow::Result<()>
    where
        F: FnMut(&str) -> Option<anyhow::Result<String>>,
    {
        if let Some(value) = env_value(&mut get, "FIBRIL_DATA_DIR")? {
            self.server.data_dir = PathBuf::from(value);
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_BROKER_BIND")? {
            self.broker.listener.bind = parse_env("FIBRIL_BROKER_BIND", &value)?;
        }
        if let Some(value) = env_value(&mut get, "FIBRIL_ADMIN_BIND")? {
            self.admin.listener.bind = parse_env("FIBRIL_ADMIN_BIND", &value)?;
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
        if let Some(value) = env_value(&mut get, "FIBRIL_KERATIN_FSYNC_INTERVAL_MS")? {
            self.storage.keratin.fsync_interval_ms =
                parse_env("FIBRIL_KERATIN_FSYNC_INTERVAL_MS", &value)?;
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

    pub fn from_toml_file(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let input = std::fs::read_to_string(path).map_err(|err| {
            anyhow::anyhow!("failed to read config file {}: {err}", path.display())
        })?;
        let mut config: Self = toml::from_str(&input).map_err(|err| {
            anyhow::anyhow!("failed to parse config file {}: {err}", path.display())
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

    fn validate(&mut self) -> anyhow::Result<()> {
        if self.server.data_dir.as_os_str().is_empty() {
            anyhow::bail!("server.data_dir must not be empty");
        }
        if self.admin.auth.enabled {
            if self.admin.auth.username.trim().is_empty() {
                anyhow::bail!("admin.auth.username must not be empty when admin auth is enabled");
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
                anyhow::bail!("admin.auth.password must be set when admin auth is enabled");
            }
        }
        if self.runtime_seed.delivery.expiry_batch_max == 0 {
            anyhow::bail!("runtime_seed.delivery.expiry_batch_max must be at least 1");
        }
        if self.storage.keratin.fsync_interval_ms == 0 {
            anyhow::bail!("storage.keratin.fsync_interval_ms must be at least 1");
        }
        if self.storage.keratin.message_log.segment_max_bytes == 0 {
            anyhow::bail!("storage.keratin.message_log.segment_max_bytes must be at least 1");
        }
        if self.storage.keratin.event_log.segment_max_bytes == 0 {
            anyhow::bail!("storage.keratin.event_log.segment_max_bytes must be at least 1");
        }
        if self.runtime_seed.idle_queue_cleanup.sweep_interval_ms == 0 {
            anyhow::bail!("runtime_seed.idle_queue_cleanup.sweep_interval_ms must be at least 1");
        }
        Ok(())
    }
}

fn env_value<F>(get: &mut F, name: &str) -> anyhow::Result<Option<String>>
where
    F: FnMut(&str) -> Option<anyhow::Result<String>>,
{
    match get(name).transpose()? {
        Some(value) if value.trim().is_empty() => Ok(None),
        value => Ok(value),
    }
}

fn optional_path_env(name: &str) -> anyhow::Result<Option<PathBuf>> {
    Ok(optional_string_env(name)?.map(PathBuf::from))
}

fn optional_string_env(name: &str) -> anyhow::Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if value.trim().is_empty() => Ok(None),
        Ok(value) => Ok(Some(value)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(err) => Err(anyhow::anyhow!("{name} is not valid unicode: {err}")),
    }
}

fn parse_env<T>(name: &str, value: &str) -> anyhow::Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse::<T>()
        .map_err(|err| anyhow::anyhow!("{name} has invalid value {value:?}: {err}"))
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
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct AdminSection {
    pub listener: ListenerSection,
    pub auth: AdminAuthSection,
}

impl Default for AdminSection {
    fn default() -> Self {
        Self {
            listener: ListenerSection {
                bind: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 8081)),
            },
            auth: AdminAuthSection::default(),
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct StorageSection {
    pub keratin: KeratinStorageSection,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KeratinStorageSection {
    pub fsync_interval_ms: u64,
    #[serde(default = "default_message_log_section")]
    pub message_log: KeratinLogSection,
    #[serde(default = "default_event_log_section")]
    pub event_log: KeratinLogSection,
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
            message_log: default_message_log_section(),
            event_log: default_event_log_section(),
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
}

impl Default for ListenerSection {
    fn default() -> Self {
        Self {
            bind: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(default)]
pub struct RuntimeSeedSection {
    pub delivery: DeliverySettings,
    pub idle_queue_cleanup: IdleQueueCleanupSettings,
    pub connection: ConnectionSettings,
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct ConnectionSettings {
    pub reconnect_grace_ms: Option<u64>,
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
        assert_eq!(config.runtime_seed.connection.reconnect_grace_ms, None);
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
        assert!(config.runtime_locks.idle_queue_cleanup);
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
                "FIBRIL_QUEUE_IDLE_EVICT_AFTER_MS" => Some(Ok("123".to_string())),
                "FIBRIL_QUEUE_IDLE_SWEEP_INTERVAL_MS" => Some(Ok("".to_string())),
                "FIBRIL_RECONNECT_GRACE_MS" => Some(Ok("789".to_string())),
                _ => None,
            })
            .unwrap();

        assert_eq!(config.server.data_dir, PathBuf::from("from-env"));
        assert!(config.admin.auth.enabled);
        assert_eq!(config.admin.auth.username, "env-admin");
        assert_eq!(config.admin.auth.password.as_deref(), Some("env-secret"));
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
