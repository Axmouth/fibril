use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::PathBuf,
};

use anyhow::{Context, bail};
use clap::{Parser, Subcommand, ValueEnum};
use fibril_client::{ClientOptions, QueueConfig};
use fibril_config::ServerConfig;

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
}

#[derive(Debug, Subcommand)]
enum QueueCommand {
    /// Declare or update queue settings.
    Declare(DeclareQueueArgs),
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
    let broker_addr = cli
        .broker
        .unwrap_or_else(|| connect_addr_for_bind(config.broker.listener.bind));
    let client = ClientOptions::new()
        .auth(cli.username, cli.password)
        .connect(broker_addr)
        .await
        .with_context(|| format!("failed to connect to broker at {broker_addr}"))?;

    match cli.command {
        Command::Queue { command } => match command {
            QueueCommand::Declare(args) => declare_queue(&client, args).await?,
        },
    }

    client.shutdown().await;
    Ok(())
}

async fn declare_queue(
    client: &fibril_client::Client,
    args: DeclareQueueArgs,
) -> anyhow::Result<()> {
    let topic = args.topic.clone();
    let mut config = QueueConfig::new(&args.topic)?;

    if let Some(group) = &args.group {
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
            match args.dlq_group.as_deref() {
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
}
