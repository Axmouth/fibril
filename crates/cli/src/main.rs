use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum};
use fibril_client::{ClientOptions, NewMessage, QueueConfig};
use fibril_config::ServerConfig;
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(name = "fibrilctl", about = "Operate a Fibril broker")]
struct Cli {
    /// Path to fibril.toml. Defaults to FIBRIL_CONFIG, then built-in defaults.
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    /// Broker address to connect to (host:port; hostnames are resolved). Defaults
    /// to the configured broker bind.
    #[arg(long, global = true)]
    broker: Option<String>,

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
    /// TLS certificate operations.
    Cert {
        #[command(subcommand)]
        command: CertCommand,
    },
    /// Broker user management through the admin API.
    User {
        #[command(subcommand)]
        command: UserCommand,
    },
    /// Cluster shared secret operations.
    Secret {
        #[command(subcommand)]
        command: SecretCommand,
    },
}

#[derive(Debug, Subcommand)]
enum UserCommand {
    /// Create a user or replace an existing user's password.
    Add(UserAddArgs),
    /// Change a user's password (same operation as add, named for scripts).
    Passwd(UserAddArgs),
    /// Remove a user.
    Remove(UserRemoveArgs),
    /// List users (names and timestamps, never hashes).
    List,
}

#[derive(Debug, clap::Args)]
struct UserAddArgs {
    /// Username to create or update.
    user: String,

    /// Password for the user.
    #[arg(long)]
    user_password: String,
}

#[derive(Debug, clap::Args)]
struct UserRemoveArgs {
    /// Username to remove.
    user: String,
}

#[derive(Debug, Subcommand)]
enum SecretCommand {
    /// Generate the cluster shared secret at <data_dir>/cluster.secret (kept
    /// as is when it already exists). Every node holds the same secret, so
    /// copy this file (or its value) to the other nodes.
    Generate(SecretGenerateArgs),
}

#[derive(Debug, clap::Args)]
struct SecretGenerateArgs {
    /// Data directory to write under. Defaults to the configured
    /// server.data_dir.
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,

    /// Print the secret value (for pasting into FIBRIL_CLUSTER_SECRET on
    /// other nodes). Off by default to keep it out of terminal scrollback.
    #[arg(long)]
    show: bool,
}

#[derive(Debug, Subcommand)]
enum CertCommand {
    /// Generate per-deployment TLS material (CA + server certificate) under
    /// <data_dir>/tls, the same material tls.auto_self_signed generates on
    /// first boot. Existing complete material is kept as is; a directory
    /// holding only the CA pair (copied from another node, or with the
    /// server pair removed for rotation) gets a server certificate minted
    /// from that CA.
    Generate(CertGenerateArgs),
    /// Print the SHA-256 fingerprint of the first certificate in a PEM file.
    Fingerprint(CertFingerprintArgs),
    /// Issue a client certificate from the deployment CA under
    /// <data_dir>/tls. The identity becomes the certificate's name and, on
    /// brokers with tls.client_auth, authenticates connections as the user
    /// of the same name without a password.
    Issue(CertIssueArgs),
}

#[derive(Debug, clap::Args)]
struct CertIssueArgs {
    /// Identity the certificate asserts (must name an existing broker user
    /// to authenticate; @ names are reserved for nodes).
    identity: String,

    /// Data directory holding the deployment CA (material under
    /// <data_dir>/tls). Defaults to the configured server.data_dir.
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,

    /// Directory the certificate and key files are written to. Defaults to
    /// the current directory.
    #[arg(long)]
    out_dir: Option<std::path::PathBuf>,
}

#[derive(Debug, clap::Args)]
struct CertGenerateArgs {
    /// Data directory to generate under (material goes to <data_dir>/tls).
    /// Defaults to the configured server.data_dir.
    #[arg(long)]
    data_dir: Option<std::path::PathBuf>,

    /// Extra subject alternative name (hostname or IP) for the server
    /// certificate, on top of the localhost set. Repeatable.
    #[arg(long = "san")]
    sans: Vec<String>,
}

#[derive(Debug, clap::Args)]
struct CertFingerprintArgs {
    /// PEM file holding the certificate (e.g. <data_dir>/tls/ca.pem).
    path: std::path::PathBuf,
}

#[derive(Debug, Subcommand)]
enum QueueCommand {
    /// Declare or update queue settings.
    Declare(DeclareQueueArgs),
    /// Publish one UTF-8 text message and wait for confirmation.
    Publish(PublishMessageArgs),
    /// Consume and acknowledge messages from a queue.
    Consume(ConsumeMessagesArgs),
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
    /// Show queue state and sparse queue activity from the admin API.
    Queues,
    /// Show cluster topology: nodes, queue assignments, and (when an embedded
    /// coordinator is active) consensus state.
    Topology(TopologyArgs),
    /// Coordination membership operations.
    Coordination {
        #[command(subcommand)]
        command: CoordinationCommand,
    },
    /// Live-repartition a queue: grow (to a multiple) or shrink (to a factor) of
    /// its current partition count.
    Repartition(RepartitionArgs),
    /// Re-read the broker's TLS certificate and key and serve the new pair to
    /// subsequent handshakes (same-CA leaf rotation without a restart).
    ReloadTls,
}

#[derive(Debug, Parser)]
struct RepartitionArgs {
    /// Topic to repartition.
    topic: String,

    /// New partition count: a larger integer multiple to grow, a smaller integer
    /// factor to shrink.
    partition_count: u32,

    /// Optional group (part of the queue identity).
    #[arg(long)]
    group: Option<String>,
}

#[derive(Debug, clap::Args)]
struct TopologyArgs {
    /// Print the raw topology JSON instead of tables.
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum CoordinationCommand {
    /// Add a node as a voting coordination member.
    AddVotingMember(CoordinationAddVotingMemberArgs),
    /// Remove a node from the voting coordination member set.
    RemoveVotingMember(CoordinationRemoveVotingMemberArgs),
}

#[derive(Debug, Parser)]
struct CoordinationAddVotingMemberArgs {
    /// Coordination node id to add.
    id: u64,

    /// Coordination TCP address for the new node.
    addr: SocketAddr,
}

#[derive(Debug, Parser)]
struct CoordinationRemoveVotingMemberArgs {
    /// Coordination node id to remove.
    id: u64,
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

    /// Partition count for the queue (spread across owners in a cluster).
    #[arg(long)]
    partitions: Option<u32>,

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

#[derive(Debug, Parser)]
struct PublishMessageArgs {
    /// Queue topic to publish to.
    topic: String,

    /// Optional queue group namespace.
    #[arg(long)]
    group: Option<String>,

    /// UTF-8 text payload.
    #[arg(long)]
    message: String,

    /// Optional partition key for partitioned queues.
    #[arg(long)]
    partition_key: Option<String>,
}

#[derive(Debug, Parser)]
struct ConsumeMessagesArgs {
    /// Queue topic to consume from.
    topic: String,

    /// Optional queue group namespace.
    #[arg(long)]
    group: Option<String>,

    /// Number of messages to consume and acknowledge.
    #[arg(long, default_value_t = 1)]
    count: usize,

    /// Subscription prefetch.
    #[arg(long, default_value_t = 1)]
    prefetch: u32,

    /// Per-message wait timeout in milliseconds.
    #[arg(long, default_value_t = 5_000)]
    timeout_ms: u64,

    /// Fail if any consumed message payload does not equal this text.
    #[arg(long)]
    expect: Option<String>,
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
                .clone()
                .unwrap_or_else(|| connect_addr_for_bind(config.broker.listener.bind).to_string());
            let client = ClientOptions::new()
                .auth(cli.username, cli.password)
                .connect(broker_addr.clone())
                .await
                .with_context(|| format!("failed to connect to broker at {broker_addr}"))?;

            match command {
                QueueCommand::Declare(args) => declare_queue(&client, args).await?,
                QueueCommand::Publish(args) => publish_message(&client, args).await?,
                QueueCommand::Consume(args) => consume_messages(&client, args).await?,
            }

            client.shutdown().await;
        }
        Command::User { command } => {
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
                UserCommand::Add(args) | UserCommand::Passwd(args) => {
                    print_json(admin.upsert_user(&args.user, &args.user_password).await?)?
                }
                UserCommand::Remove(args) => print_json(admin.remove_user(&args.user).await?)?,
                UserCommand::List => print_json(admin.list_users().await?)?,
            }
        }
        Command::Secret { command } => match command {
            SecretCommand::Generate(args) => {
                let data_dir = args.data_dir.unwrap_or(config.server.data_dir);
                std::fs::create_dir_all(&data_dir)
                    .with_context(|| format!("failed to create {}", data_dir.display()))?;
                let path = data_dir.join(fibril_config::CLUSTER_SECRET_FILE);
                let secret = if path.exists() {
                    println!(
                        "Cluster secret already exists at {}, keeping it.",
                        path.display()
                    );
                    std::fs::read_to_string(&path)
                        .with_context(|| format!("failed to read {}", path.display()))?
                        .trim()
                        .to_string()
                } else {
                    let mut bytes = [0u8; 32];
                    getrandom::getrandom(&mut bytes)
                        .map_err(|err| anyhow::anyhow!("failed to gather entropy: {err}"))?;
                    let secret: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
                    std::fs::write(&path, format!("{secret}\n"))
                        .with_context(|| format!("failed to write {}", path.display()))?;
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
                            .with_context(|| format!("failed to chmod {}", path.display()))?;
                    }
                    println!("Cluster secret written to {} (0600).", path.display());
                    secret
                };
                println!();
                println!("Give every node the same secret:");
                println!("  copy this file to each node's data dir, or set");
                println!("  FIBRIL_CLUSTER_SECRET / coordination.secret_path there.");
                if args.show {
                    println!();
                    println!("{secret}");
                } else {
                    println!("Run with --show to print the value.");
                }
            }
        },
        Command::Cert { command } => match command {
            CertCommand::Generate(args) => {
                let data_dir = args.data_dir.unwrap_or(config.server.data_dir);
                let tls = fibril_tls::build_server_tls(
                    &fibril_config::TlsMode::AutoSelfSigned,
                    &data_dir,
                    &args.sans,
                )?
                .context("auto_self_signed mode always yields material")?;
                let fibril_tls::TlsMaterialSource::Generated {
                    dir,
                    ca_fingerprint,
                } = tls.source
                else {
                    anyhow::bail!("generation unexpectedly reported operator-supplied material");
                };
                println!("TLS material ready under {}", dir.display());
                println!("  ca.pem / ca.key / server.pem / server.key (keys are 0600)");
                println!("CA SHA-256 fingerprint: {ca_fingerprint}");
                println!();
                println!("Broker config to serve it:");
                println!("  [tls]");
                println!("  enabled = true");
                println!("  auto_self_signed = true");
                println!();
                println!(
                    "Clients trust {}/ca.pem or pin the fingerprint above.",
                    dir.display()
                );
            }
            CertCommand::Issue(args) => {
                let data_dir = args.data_dir.unwrap_or(config.server.data_dir);
                let tls_dir = data_dir.join(fibril_tls::GENERATED_TLS_DIR);
                let (cert_pem, key_pem) =
                    fibril_tls::issue_client_certificate(&tls_dir, &args.identity)?;
                let out_dir = args
                    .out_dir
                    .unwrap_or_else(|| std::path::PathBuf::from("."));
                std::fs::create_dir_all(&out_dir)?;
                let cert_path = out_dir.join(format!("{}.pem", args.identity));
                let key_path = out_dir.join(format!("{}.key", args.identity));
                std::fs::write(&cert_path, cert_pem)?;
                std::fs::write(&key_path, key_pem)?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
                }
                println!("Issued client certificate for `{}`:", args.identity);
                println!("  {}", cert_path.display());
                println!("  {} (0600)", key_path.display());
                println!();
                println!(
                    "It authenticates as the broker user `{}` (create it with \
                     fibrilctl user add). Clients present it via their TLS \
                     client-certificate options.",
                    args.identity
                );
            }
            CertCommand::Fingerprint(args) => {
                println!("{}", fibril_tls::ca_fingerprint(&args.path)?);
            }
        },
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
                AdminCommand::Queues => print_json(admin.queues_debug().await?)?,
                AdminCommand::Topology(args) => {
                    let topology = admin.topology().await?;
                    if args.json {
                        print_json(topology)?;
                    } else {
                        print_topology(&topology);
                    }
                }
                AdminCommand::Coordination { command } => match command {
                    CoordinationCommand::AddVotingMember(args) => {
                        print_json(admin.add_coordination_voting_member(args).await?)?
                    }
                    CoordinationCommand::RemoveVotingMember(args) => {
                        print_json(admin.remove_coordination_voting_member(args).await?)?
                    }
                },
                AdminCommand::Repartition(args) => print_json(admin.repartition(args).await?)?,
                AdminCommand::ReloadTls => print_json(admin.reload_tls().await?)?,
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
    if let Some(partitions) = args.partitions {
        config = config.partitions(partitions);
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

async fn publish_message(
    client: &fibril_client::Client,
    args: PublishMessageArgs,
) -> anyhow::Result<()> {
    let publisher = match normalize_group_arg(args.group.as_deref()) {
        Some(group) => client.publisher_grouped(&args.topic, group)?,
        None => client.publisher(&args.topic)?,
    };
    let mut message = NewMessage::content(args.message);
    if let Some(partition_key) = args.partition_key {
        message = message.partition_key(partition_key.into_bytes());
    }
    let offset = publisher.publish_confirmed(message).await?;
    println!("published {} at offset {offset}", args.topic);
    Ok(())
}

async fn consume_messages(
    client: &fibril_client::Client,
    args: ConsumeMessagesArgs,
) -> anyhow::Result<()> {
    if args.count == 0 {
        bail!("--count must be greater than zero");
    }

    let mut builder = client.subscribe(&args.topic)?.prefetch(args.prefetch);
    if let Some(group) = normalize_group_arg(args.group.as_deref()) {
        builder = builder.group(group)?;
    }
    let mut subscription = builder.sub().await?;

    for index in 0..args.count {
        let message = tokio::time::timeout(
            std::time::Duration::from_millis(args.timeout_ms),
            subscription.recv(),
        )
        .await
        .with_context(|| {
            format!(
                "timed out after {}ms waiting for message {}",
                args.timeout_ms,
                index + 1
            )
        })?
        .with_context(|| format!("subscription closed before message {}", index + 1))?;

        let payload = String::from_utf8_lossy(&message.payload).to_string();
        if let Some(expected) = &args.expect {
            if &payload != expected {
                bail!("expected payload {expected:?}, got {payload:?}");
            }
        }

        message.complete().await?;
        println!("{payload}");
    }

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

#[derive(Debug, Serialize)]
struct AddCoordinationVotingMemberRequest {
    id: u64,
    addr: String,
}

#[derive(Debug, Serialize)]
struct RemoveCoordinationVotingMemberRequest {
    id: u64,
}

#[derive(Debug, Serialize)]
struct RepartitionRequest {
    topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<String>,
    partition_count: u32,
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

    async fn queues_debug(&self) -> anyhow::Result<serde_json::Value> {
        self.get_json("/admin/api/queues_debug", &[]).await
    }

    async fn topology(&self) -> anyhow::Result<serde_json::Value> {
        self.get_json("/admin/api/topology", &[]).await
    }

    async fn add_coordination_voting_member(
        &self,
        args: CoordinationAddVotingMemberArgs,
    ) -> anyhow::Result<serde_json::Value> {
        self.post_json(
            "/admin/api/coordination/membership/add-voting-member",
            &AddCoordinationVotingMemberRequest {
                id: args.id,
                addr: args.addr.to_string(),
            },
        )
        .await
    }

    async fn remove_coordination_voting_member(
        &self,
        args: CoordinationRemoveVotingMemberArgs,
    ) -> anyhow::Result<serde_json::Value> {
        self.post_json(
            "/admin/api/coordination/membership/remove-voting-member",
            &RemoveCoordinationVotingMemberRequest { id: args.id },
        )
        .await
    }

    async fn list_users(&self) -> anyhow::Result<serde_json::Value> {
        self.get_json("/admin/api/users", &[]).await
    }

    async fn upsert_user(
        &self,
        username: &str,
        password: &str,
    ) -> anyhow::Result<serde_json::Value> {
        self.post_json(
            "/admin/api/users",
            &serde_json::json!({ "username": username, "password": password }),
        )
        .await
    }

    async fn remove_user(&self, username: &str) -> anyhow::Result<serde_json::Value> {
        let response = self
            .http
            .delete(self.url(&format!("/admin/api/users/{username}")))
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .with_context(|| format!("failed to connect to admin API at {}", self.addr))?;
        decode_admin_response(response).await
    }

    async fn repartition(&self, args: RepartitionArgs) -> anyhow::Result<serde_json::Value> {
        self.post_json(
            "/admin/api/repartition",
            &RepartitionRequest {
                topic: args.topic,
                group: normalize_group_arg(args.group.as_deref()).map(str::to_string),
                partition_count: args.partition_count,
            },
        )
        .await
    }

    async fn reload_tls(&self) -> anyhow::Result<serde_json::Value> {
        self.post_json("/admin/api/tls/reload", &serde_json::json!({}))
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

fn print_topology(topology: &serde_json::Value) {
    let coordination = &topology["coordination"];
    if coordination.is_null() {
        println!("no coordination provider attached (single-node / static mode)");
    } else {
        println!(
            "cluster: reporting node {} | snapshot generation {}",
            coordination["node_id"].as_str().unwrap_or("?"),
            coordination["generation"].as_u64().unwrap_or(0),
        );

        println!("\nnodes:");
        println!("  {:<16} {:<22} {:<22}", "NODE", "BROKER", "ADMIN");
        for node in coordination["nodes"].as_array().into_iter().flatten() {
            println!(
                "  {:<16} {:<22} {:<22}",
                node["node_id"].as_str().unwrap_or("?"),
                node["broker_addr"].as_str().unwrap_or("?"),
                node["admin_addr"].as_str().unwrap_or("-"),
            );
        }

        println!("\nassignments:");
        println!(
            "  {:<20} {:<5} {:<10} {:<16} {:<6} FOLLOWERS",
            "TOPIC", "PART", "GROUP", "OWNER", "EPOCH"
        );
        for assignment in coordination["assignments"].as_array().into_iter().flatten() {
            let followers = assignment["followers"]
                .as_array()
                .map(|followers| {
                    followers
                        .iter()
                        .filter_map(|follower| follower.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default();
            println!(
                "  {:<20} {:<5} {:<10} {:<16} {:<6} {}",
                assignment["topic"].as_str().unwrap_or("?"),
                assignment["partition"].as_u64().unwrap_or(0),
                assignment["group"].as_str().unwrap_or("-"),
                assignment["owner"].as_str().unwrap_or("?"),
                assignment["epoch"].as_u64().unwrap_or(0),
                followers,
            );
        }
    }

    let consensus = &topology["consensus"];
    if !consensus.is_null() {
        let render_ids = |ids: &serde_json::Value| {
            ids.as_array()
                .map(|ids| {
                    ids.iter()
                        .filter_map(|id| id.as_u64())
                        .map(|id| id.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .unwrap_or_default()
        };
        println!("\nraft (embedded coordinator):");
        println!(
            "  local={} leader={} voters=[{}] learners=[{}] applied={} committed_generation={}",
            consensus["local_id"].as_u64().unwrap_or(0),
            consensus["leader"]
                .as_u64()
                .map(|id| id.to_string())
                .unwrap_or_else(|| "none".to_string()),
            render_ids(&consensus["voters"]),
            render_ids(&consensus["learners"]),
            consensus["last_applied_index"]
                .as_u64()
                .map(|index| index.to_string())
                .unwrap_or_else(|| "-".to_string()),
            consensus["committed_generation"].as_u64().unwrap_or(0),
        );
    }
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
            partitions: None,
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

    #[test]
    fn parses_admin_queues_command() {
        let cli = Cli::try_parse_from(["fibrilctl", "admin", "queues"]).unwrap();

        let Command::Admin {
            command: AdminCommand::Queues,
        } = cli.command
        else {
            panic!("expected admin queues command");
        };
    }

    #[test]
    fn parses_queue_publish_command() {
        let cli = Cli::try_parse_from([
            "fibrilctl",
            "queue",
            "publish",
            "orders",
            "--group",
            "workers",
            "--message",
            "hello",
            "--partition-key",
            "customer-7",
        ])
        .unwrap();

        let Command::Queue {
            command: QueueCommand::Publish(args),
        } = cli.command
        else {
            panic!("expected queue publish command");
        };

        assert_eq!(args.topic, "orders");
        assert_eq!(args.group.as_deref(), Some("workers"));
        assert_eq!(args.message, "hello");
        assert_eq!(args.partition_key.as_deref(), Some("customer-7"));
    }

    #[test]
    fn parses_queue_consume_command() {
        let cli = Cli::try_parse_from([
            "fibrilctl",
            "queue",
            "consume",
            "orders",
            "--count",
            "2",
            "--prefetch",
            "4",
            "--timeout-ms",
            "250",
            "--expect",
            "hello",
        ])
        .unwrap();

        let Command::Queue {
            command: QueueCommand::Consume(args),
        } = cli.command
        else {
            panic!("expected queue consume command");
        };

        assert_eq!(args.topic, "orders");
        assert_eq!(args.count, 2);
        assert_eq!(args.prefetch, 4);
        assert_eq!(args.timeout_ms, 250);
        assert_eq!(args.expect.as_deref(), Some("hello"));
    }
}
