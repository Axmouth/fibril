//! fibril-demo: a small living world of realistic fake traffic, built to make
//! every corner of the admin dashboard light up. Three businesses share one
//! broker - a delivery bistro, a freight desk, and a hotel group - each with
//! its own rhythm on a compressed day clock, its own payload shapes (JSON,
//! XML, msgpack, plain text), and consumers with personalities: steady,
//! sluggish (naps every afternoon, so backlog attention raises and resolves),
//! and flaky (feeds the dead-letter flow). Traffic is deliberately gentle -
//! a handful of messages per second - because the point is a readable story,
//! not load.
//!
//! Run a broker, then:
//!
//!   cargo run --release -p fibril-demo
//!   cargo run --release -p fibril-demo -- --addr 127.0.0.1:19876 --day-secs 180
//!
//! Ctrl-C stops everything. Restarts are safe: declares are idempotent and
//! topic names are stable.

mod bistro;
mod clock;
mod data;
mod freight;
mod hotels;
mod pace;
mod telemetry;

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use fibril_client::{Client, ClientOptions, QueueConfig, StreamConfig};

use clock::DayClock;

#[derive(Parser)]
#[command(about = "A living demo world of realistic fake traffic for the Fibril dashboard")]
struct Args {
    /// Broker address.
    #[arg(long, default_value = "127.0.0.1:9876")]
    addr: String,

    /// Broker credentials as user:pass.
    #[arg(long, default_value = "fibril:fibril")]
    auth: String,

    /// Seed for the fake-data randomness, for reproducible runs.
    #[arg(long)]
    seed: Option<u64>,

    /// Traffic multiplier over the gentle default.
    #[arg(long, default_value_t = 1.0)]
    scale: f64,

    /// Chance the payment processor requeues a delivery (its retry budget is
    /// 2, so roughly flakiness^3 of payments dead-letter).
    #[arg(long, default_value_t = 0.25)]
    flakiness: f64,

    /// Length of one demo "day" in real seconds. Rush hours, naps, and
    /// reader outages all scale with it.
    #[arg(long, default_value_t = 240)]
    day_secs: u64,

    /// Keep the support desk awake (no afternoon nap, so no backlog drama).
    #[arg(long)]
    no_naps: bool,
}

/// Shared demo context handed to every role.
pub struct Demo {
    addr: String,
    user: String,
    pass: String,
    pub clock: DayClock,
    pub scale: f64,
    pub flakiness: f64,
    pub naps: bool,
}

impl Demo {
    /// A fresh connection: every role holds its own, so the Connections page
    /// shows the whole cast instead of one multiplexed line.
    pub async fn client(&self) -> anyhow::Result<Client> {
        Ok(Client::connect(
            self.addr.as_str(),
            ClientOptions::new().auth(&self.user, &self.pass),
        )
        .await?)
    }
}

/// Keep a role alive forever: any error (broker restart, network blip) logs
/// and retries with a short pause, which also demos client recovery.
fn spawn_role<F, Fut>(name: &'static str, demo: Arc<Demo>, role: F)
where
    F: Fn(Arc<Demo>) -> Fut + Send + 'static,
    Fut: Future<Output = anyhow::Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        loop {
            match role(demo.clone()).await {
                Ok(()) => return,
                Err(error) => {
                    eprintln!("[{name}] {error:#} - retrying in 3s");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            }
        }
    });
}

async fn declare_world(demo: &Demo) -> anyhow::Result<()> {
    let client = demo.client().await?;
    client
        .declare_queue(QueueConfig::new("orders")?.partitions(4))
        .await?;
    client
        .declare_queue(QueueConfig::new("deliveries")?.partitions(2))
        .await?;
    client
        .declare_queue(
            QueueConfig::new("payments")?
                .partitions(2)
                .max_retries(2)
                .custom_dead_letter_queue("payments.dead")?,
        )
        .await?;
    client
        .declare_queue(QueueConfig::new("payments.dead")?.partitions(1))
        .await?;
    client
        .declare_queue(QueueConfig::new("shipments")?.partitions(2))
        .await?;
    client
        .declare_queue(QueueConfig::new("shipments.customs")?.partitions(1))
        .await?;
    client
        .declare_queue(QueueConfig::new("bookings")?.partitions(2))
        .await?;
    client
        .declare_queue(QueueConfig::new("bookings.cancellations")?.partitions(1))
        .await?;
    client
        .declare_queue(QueueConfig::new("support.mail")?.partitions(1))
        .await?;
    client
        .declare_plexus(StreamConfig::new("courier.location")?.partitions(3).durable())
        .await?;
    client
        .declare_plexus(StreamConfig::new("kitchen.telemetry")?.partitions(2))
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let seed = args.seed.unwrap_or_else(|| fastrand::u64(..));
    fastrand::seed(seed);
    let (user, pass) = args
        .auth
        .split_once(':')
        .map(|(u, p)| (u.to_string(), p.to_string()))
        .unwrap_or_else(|| (args.auth.clone(), String::new()));

    let demo = Arc::new(Demo {
        addr: args.addr.clone(),
        user,
        pass,
        clock: DayClock::new(args.day_secs),
        scale: args.scale.max(0.05),
        flakiness: args.flakiness.clamp(0.0, 0.95),
        naps: !args.no_naps,
    });

    // Retry the initial declares until the broker answers, so the demo can
    // be started before or after the broker without ceremony.
    loop {
        match declare_world(&demo).await {
            Ok(()) => break,
            Err(error) => {
                eprintln!("waiting for the broker at {}: {error:#}", demo.addr);
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
    }

    println!("fibril-demo: world is open for business");
    println!("  broker    {}", demo.addr);
    println!(
        "  day       {}s (rushes, naps, and outages all ride this clock)",
        args.day_secs
    );
    println!("  seed      {seed}");
    println!("  scale     {}  flakiness {}", demo.scale, demo.flakiness);
    println!("  stop with Ctrl-C - restarts are safe");

    // The bistro pipeline: customers -> kitchen -> couriers + payments.
    let (courier_tx, courier_rx) = tokio::sync::mpsc::channel(64);
    spawn_role("bistro.customers", demo.clone(), bistro::run_customers);
    {
        let tx = courier_tx.clone();
        spawn_role("bistro.kitchen", demo.clone(), move |d| {
            bistro::run_kitchen(d, tx.clone())
        });
    }
    {
        // The courier receiver is single-consumer, so this role does not use
        // spawn_role's retry loop - a courier-pool crash ends its pings only.
        let demo_couriers = demo.clone();
        tokio::spawn(async move {
            if let Err(error) = bistro::run_couriers(demo_couriers, courier_rx).await {
                eprintln!("[bistro.couriers] {error:#} - courier pings stopped");
            }
        });
    }
    spawn_role("bistro.tracker", demo.clone(), bistro::run_delivery_tracker);
    spawn_role("bistro.payments", demo.clone(), bistro::run_payment_processor);

    // The freight desk.
    spawn_role("freight.dispatch", demo.clone(), freight::run_dispatch);
    spawn_role("freight.desk", demo.clone(), freight::run_desk);
    spawn_role("freight.customs", demo.clone(), freight::run_customs_officer);

    // The hotel group, sharing one bookings ledger.
    let ledger = hotels::ledger();
    {
        let l = ledger.clone();
        spawn_role("hotels.bookings", demo.clone(), move |d| {
            hotels::run_booking_desk(d, l.clone())
        });
    }
    {
        let l = ledger.clone();
        spawn_role("hotels.cancellations", demo.clone(), move |d| {
            hotels::run_cancellations(d, l.clone())
        });
    }
    {
        let l = ledger.clone();
        spawn_role("hotels.guest-mail", demo.clone(), move |d| {
            hotels::run_guest_mail(d, l.clone())
        });
    }
    spawn_role("hotels.booking-ops", demo.clone(), hotels::run_booking_ops);
    spawn_role(
        "hotels.cancellation-ops",
        demo.clone(),
        hotels::run_cancellation_ops,
    );
    spawn_role("hotels.support", demo.clone(), hotels::run_support_desk);

    // Streams and their readers.
    spawn_role("telemetry.sensors", demo.clone(), telemetry::run_sensors);
    spawn_role(
        "telemetry.dashboard",
        demo.clone(),
        telemetry::run_dashboard_reader,
    );
    spawn_role(
        "telemetry.analytics",
        demo.clone(),
        telemetry::run_analytics_reader,
    );
    spawn_role(
        "telemetry.dispatch-screen",
        demo.clone(),
        telemetry::run_dispatch_screen,
    );

    tokio::signal::ctrl_c().await?;
    println!("\nfibril-demo: closing time");
    Ok(())
}
