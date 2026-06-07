use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum};
use fibril_client::{ClientOptions, QueueConfig};
use fibril_config::ServerConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(name = "fibrilctl", about = "Operate a Fibril broker")]
struct Cli {
    /// Path to fibril.toml. Defaults to FIBRIL_CONFIG, then built-in defaults.
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Broker address to connect to. Defaults to the configured broker bind.
    #[arg(long, global = true)]
    broker: Option<SocketAddr>,

    /// Broker username.
    #[arg(long, global = true, default_value = "fibril")]
    username: String,

    /// Broker password.
    #[arg(long, global = true, default_value = "fibril")]
    password: String,

    /// Admin HTTP address. Defaults to the configured admin bind.
    #[arg(long, global = true)]
    admin: Option<SocketAddr>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Queue operations.
    Queue {
        #[command(subcommand)]
        command: QueueCommand,
    },
    /// Admin API operations.
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
}

#[derive(Debug, Subcommand)]
enum QueueCommand {
    /// Declare or update queue settings.
    Declare(DeclareQueueArgs),
}

#[derive(Debug, Subcommand)]
enum AdminCommand {
    /// Global dead-letter queue target operations.
    GlobalDlq {
        #[command(subcommand)]
        command: GlobalDlqCommand,
    },
    /// Dead-letter queue operations.
    Dlq {
        #[command(subcommand)]
        command: DlqCommand,
    },
    /// Inspect persisted queue messages through the admin API.
    Messages(InspectMessagesArgs),
}

#[derive(Debug, Subcommand)]
enum GlobalDlqCommand {
    /// Show the current global DLQ target.
    Get,
    /// Set the global DLQ target.
    Set(SetGlobalDlqArgs),
    /// Clear the global DLQ target.
    Clear(ClearGlobalDlqArgs),
}

#[derive(Debug, Subcommand)]
enum DlqCommand {
    /// Replay active dead-letter messages to their recorded source queue.
    Replay(ReplayDlqArgs),
}

#[derive(Debug, Parser)]
struct SetGlobalDlqArgs {
    /// Target DLQ topic.
    topic: String,

    /// Optional target DLQ group.
    #[arg(long)]
    group: Option<String>,

    /// Expected global DLQ settings version. Defaults to the current version.
    #[arg(long)]
    expected_version: Option<u64>,
}

#[derive(Debug, Parser)]
struct ReplayDlqArgs {
    /// DLQ topic to replay from.
    dlq_topic: String,

    /// Optional DLQ group.
    #[arg(long)]
    dlq_group: Option<String>,

    /// DLQ offset to replay. Repeat for multiple offsets.
    #[arg(long, required = true)]
    offset: Vec<u64>,
}

#[derive(Debug, Parser)]
struct ClearGlobalDlqArgs {
    /// Expected global DLQ settings version. Defaults to the current version.
    #[arg(long)]
    expected_version: Option<u64>,
}

#[derive(Debug, Parser)]
struct InspectMessagesArgs {
    /// Queue topic to inspect.
    topic: String,

    /// Optional queue group namespace.
    #[arg(long)]
    group: Option<String>,

    /// First offset to inspect.
    #[arg(long, default_value_t = 0)]
    from: u64,

    /// Maximum messages to inspect. The server applies its own hard cap.
    #[arg(long)]
    limit: Option<usize>,

    /// Include settled log records that are no longer active in queue state.
    #[arg(long)]
    include_settled: bool,

    /// Include base64 payload previews.
    #[arg(long)]
    include_payload: bool,

    /// Maximum payload preview bytes per message.
    #[arg(long)]
    payload_limit_bytes: Option<usize>,
}

#[derive(Debug, Parser)]
struct DeclareQueueArgs {
    /// Queue topic to declare.
    topic: String,

    /// Optional queue group namespace.
    #[arg(long)]
    group: Option<String>,

    /// Retry count before dead-letter behavior applies.
    #[arg(long)]
    max_retries: Option<u32>,

    /// Dead-letter policy for this queue.
    #[arg(long, value_enum)]
    dlq: Option<DlqPolicyArg>,

    /// Custom DLQ topic. Requires --dlq custom.
    #[arg(long)]
    dlq_topic: Option<String>,

    /// Custom DLQ group. Requires --dlq custom.
    #[arg(long)]
    dlq_group: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum DlqPolicyArg {
    Discard,
    Global,
    Custom,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    run(cli).await
}

async fn run(cli: Cli) -> anyhow::Result<()> {
    let config = ServerConfig::load_file_and_env(cli.config)?;

    match cli.command {
        Command::Queue { command } => {
            let broker_addr = cli
                .broker
                .unwrap_or_else(|| connect_addr_for_bind(config.broker.listener.bind));
            let client = ClientOptions::new()
                .auth(cli.username, cli.password)
                .connect(broker_addr)
                .await
                .with_context(|| format!("failed to connect to broker at {broker_addr}"))?;

            match command {
                QueueCommand::Declare(args) => declare_queue(&client, args).await?,
            }

            client.shutdown().await;
        }
        Command::Admin { command } => {
            let admin_addr = cli
                .admin
                .unwrap_or_else(|| connect_addr_for_bind(config.admin.listener.bind));
            let admin = AdminClient {
                addr: admin_addr,
                username: cli.username,
                password: cli.password,
                http: reqwest::Client::new(),
            };

            match command {
                AdminCommand::GlobalDlq { command } => match command {
                    GlobalDlqCommand::Get => print_global_dlq(admin.get_global_dlq().await?)?,
                    GlobalDlqCommand::Set(args) => {
                        let current = admin.get_global_dlq().await?;
                        let version = args.expected_version.unwrap_or(current.version);
                        let updated = admin
                            .set_global_dlq(
                                version,
                                Some(GlobalDlqTarget {
                                    tp: args.topic,
                                    group: normalize_group_arg(args.group.as_deref())
                                        .map(str::to_string),
                                    part: None,
                                }),
                            )
                            .await?;
                        print_global_dlq(updated)?;
                    }
                    GlobalDlqCommand::Clear(args) => {
                        let current = admin.get_global_dlq().await?;
                        let version = args.expected_version.unwrap_or(current.version);
                        let updated = admin.set_global_dlq(version, None).await?;
                        print_global_dlq(updated)?;
                    }
                },
                AdminCommand::Dlq { command } => match command {
                    DlqCommand::Replay(args) => print_json(admin.replay_dlq(args).await?)?,
                },
                AdminCommand::Messages(args) => print_json(admin.inspect_messages(args).await?)?,
            }
        }
    }

    Ok(())
}

async fn declare_queue(
    client: &fibril_client::Client,
    args: DeclareQueueArgs,
) -> anyhow::Result<()> {
    let topic = args.topic.clone();
    let mut config = QueueConfig::new(&args.topic)?;

    if let Some(group) = normalize_group_arg(args.group.as_deref()) {
        config = config.group(group)?;
    }
    if let Some(max_retries) = args.max_retries {
        config = config.max_retries(max_retries);
    }

    config = match args.dlq {
        Some(DlqPolicyArg::Discard) => {
            reject_custom_dlq_args(&args)?;
            config.discard_dead_letters()
        }
        Some(DlqPolicyArg::Global) => {
            reject_custom_dlq_args(&args)?;
            config.use_global_dead_letter_queue()
        }
        Some(DlqPolicyArg::Custom) => {
            let dlq_topic = args
                .dlq_topic
                .as_deref()
                .context("--dlq custom requires --dlq-topic")?;
            match normalize_group_arg(args.dlq_group.as_deref()) {
                Some(group) => config.custom_dead_letter_queue_grouped(dlq_topic, group)?,
                None => config.custom_dead_letter_queue(dlq_topic)?,
            }
        }
        None => {
            if args.dlq_topic.is_some() || args.dlq_group.is_some() {
                bail!("--dlq-topic and --dlq-group require --dlq custom");
            }
            config
        }
    };

    client.declare_queue(config).await?;
    println!("declared queue {topic}");
    Ok(())
}

fn reject_custom_dlq_args(args: &DeclareQueueArgs) -> anyhow::Result<()> {
    if args.dlq_topic.is_some() || args.dlq_group.is_some() {
        bail!("--dlq-topic and --dlq-group require --dlq custom");
    }
    Ok(())
}

fn connect_addr_for_bind(bind: SocketAddr) -> SocketAddr {
    match bind {
        SocketAddr::V4(addr) if addr.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), addr.port())
        }
        SocketAddr::V6(addr) if addr.ip().is_unspecified() => {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), addr.port())
        }
        other => other,
    }
}

fn normalize_group_arg(group: Option<&str>) -> Option<&str> {
    group.and_then(|group| {
        let group = group.trim();
        if group.is_empty() || group == "default" {
            None
        } else {
            Some(group)
        }
    })
}

#[derive(Debug, Clone)]
struct AdminClient {
    addr: SocketAddr,
    username: String,
    password: String,
    http: reqwest::Client,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct GlobalDlqSnapshot {
    version: u64,
    target: Option<GlobalDlqTarget>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct GlobalDlqTarget {
    tp: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    part: Option<u32>,
    group: Option<String>,
}

#[derive(Debug, Serialize)]
struct UpdateGlobalDlqRequest {
    expected_version: u64,
    target: Option<GlobalDlqTarget>,
}

#[derive(Debug, Serialize)]
struct ReplayDlqRequest {
    dlq_topic: String,
    dlq_group: Option<String>,
    offsets: Vec<u64>,
}

impl AdminClient {
    async fn get_global_dlq(&self) -> anyhow::Result<GlobalDlqSnapshot> {
        self.get_json("/admin/api/global-dlq", &[]).await
    }

    async fn set_global_dlq(
        &self,
        expected_version: u64,
        target: Option<GlobalDlqTarget>,
    ) -> anyhow::Result<GlobalDlqSnapshot> {
        self.put_json(
            "/admin/api/global-dlq",
            &UpdateGlobalDlqRequest {
                expected_version,
                target,
            },
        )
        .await
    }

    async fn inspect_messages(
        &self,
        args: InspectMessagesArgs,
    ) -> anyhow::Result<serde_json::Value> {
        let mut query = vec![
            ("topic".to_string(), args.topic),
            ("from".to_string(), args.from.to_string()),
        ];
        if let Some(group) = normalize_group_arg(args.group.as_deref()) {
            query.push(("group".to_string(), group.to_string()));
        }
        if let Some(limit) = args.limit {
            query.push(("limit".to_string(), limit.to_string()));
        }
        if args.include_settled {
            query.push(("include_settled".to_string(), "true".to_string()));
        }
        if args.include_payload {
            query.push(("include_payload".to_string(), "true".to_string()));
        }
        if let Some(limit) = args.payload_limit_bytes {
            query.push(("payload_limit_bytes".to_string(), limit.to_string()));
        }

        self.get_json("/admin/api/messages", &query).await
    }

    async fn replay_dlq(&self, args: ReplayDlqArgs) -> anyhow::Result<serde_json::Value> {
        self.post_json(
            "/admin/api/dlq/replay",
            &ReplayDlqRequest {
                dlq_topic: args.dlq_topic,
                dlq_group: normalize_group_arg(args.dlq_group.as_deref()).map(str::to_string),
                offsets: args.offset,
            },
        )
        .await
    }

    async fn get_json<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        query: &[(String, String)],
    ) -> anyhow::Result<T> {
        let response = self
            .http
            .get(self.url(path))
            .basic_auth(&self.username, Some(&self.password))
            .query(query)
            .send()
            .await
            .with_context(|| format!("failed to connect to admin API at {}", self.addr))?;
        decode_admin_response(response).await
    }

    async fn put_json<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> anyhow::Result<T> {
        let response = self
            .http
            .put(self.url(path))
            .basic_auth(&self.username, Some(&self.password))
            .json(body)
            .send()
            .await
            .with_context(|| format!("failed to connect to admin API at {}", self.addr))?;
        decode_admin_response(response).await
    }

    async fn post_json<T: for<'de> Deserialize<'de>>(
        &self,
        path: &str,
        body: &impl Serialize,
    ) -> anyhow::Result<T> {
        let response = self
            .http
            .post(self.url(path))
            .basic_auth(&self.username, Some(&self.password))
            .json(body)
            .send()
            .await
            .with_context(|| format!("failed to connect to admin API at {}", self.addr))?;
        decode_admin_response(response).await
    }

    fn url(&self, path: &str) -> String {
        format!("http://{}{}", self.addr, path)
    }
}

async fn decode_admin_response<T: for<'de> Deserialize<'de>>(
    response: reqwest::Response,
) -> anyhow::Result<T> {
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read admin API response")?;
    if !status.is_success() {
        bail!("admin API returned HTTP {status}: {body}");
    }
    serde_json::from_str(&body).context("failed to decode admin API response")
}

fn print_global_dlq(snapshot: GlobalDlqSnapshot) -> anyhow::Result<()> {
    print_json(snapshot)
}

fn print_json(value: impl Serialize) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string_pretty(&value)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unspecified_bind_connects_to_loopback() {
        assert_eq!(
            connect_addr_for_bind("0.0.0.0:9876".parse().unwrap()),
            "127.0.0.1:9876".parse().unwrap()
        );
        assert_eq!(
            connect_addr_for_bind("[::]:9876".parse().unwrap()),
            "[::1]:9876".parse().unwrap()
        );
    }

    #[test]
    fn custom_dlq_args_require_custom_policy() {
        let args = DeclareQueueArgs {
            topic: "jobs".to_string(),
            group: None,
            max_retries: None,
            dlq: Some(DlqPolicyArg::Global),
            dlq_topic: Some("jobs.dlq".to_string()),
            dlq_group: None,
        };

        assert!(reject_custom_dlq_args(&args).is_err());
    }

    #[test]
    fn default_group_arg_normalizes_to_ungrouped() {
        assert_eq!(normalize_group_arg(None), None);
        assert_eq!(normalize_group_arg(Some("")), None);
        assert_eq!(normalize_group_arg(Some("default")), None);
        assert_eq!(normalize_group_arg(Some(" workers ")), Some("workers"));
    }

    #[test]
    fn admin_url_uses_configured_address() {
        let admin = AdminClient {
            addr: "127.0.0.1:9090".parse().unwrap(),
            username: "fibril".to_string(),
            password: "fibril".to_string(),
            http: reqwest::Client::new(),
        };

        assert_eq!(
            admin.url("/admin/api/messages"),
            "http://127.0.0.1:9090/admin/api/messages"
        );
    }

    #[test]
    fn parses_dlq_replay_offsets() {
        let cli = Cli::try_parse_from([
            "fibrilctl",
            "admin",
            "dlq",
            "replay",
            "_dlq.orders",
            "--offset",
            "0",
            "--offset",
            "3",
        ])
        .unwrap();

        let Command::Admin {
            command:
                AdminCommand::Dlq {
                    command: DlqCommand::Replay(args),
                },
        } = cli.command
        else {
            panic!("expected dlq replay command");
        };

        assert_eq!(args.dlq_topic, "_dlq.orders");
        assert_eq!(args.offset, vec![0, 3]);
    }
}
