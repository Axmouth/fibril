use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use clap::Parser;
use fibril_client::{InflightMessage, ClientOptions};
use tokio::{sync::oneshot, time::Instant};

async fn run_load_test(
    num_clients: usize,
    msgs_per_client: usize,
    start_reader: bool,
    start_writer: bool,
    txb: oneshot::Sender<()>,
) {
    let start = Instant::now();

    let mut handles: Vec<tokio::task::JoinHandle<()>> = vec![];

    for j in 0..num_clients {
        handles.push(tokio::spawn(async move {
            let payload = vec![8u8; 1024];
            let address = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::from([127, 0, 0, 1]), 9876));
            let client = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(address)
                .await
                .unwrap();

            let (tx_acker, mut rx_acker) = tokio::sync::mpsc::unbounded_channel::<InflightMessage>();
            let client_reader = client.clone();
            let reader = tokio::spawn(async move {
                if !start_reader {
                    return;
                }
                let mut sub = client_reader
                    .subscribe("Topic1")
                    .prefetch(1024 * 8)
                    .sub_manual_ack()
                    .await
                    .unwrap();
                let start_inner = Instant::now();
                let mut i: usize = 0;

                while let Some(msg) = sub.recv().await {
                    // just drain
                    i += 1;
                    if i.is_multiple_of(1000) {
                        let elapsed = start_inner.elapsed();
                        println!(
                            "Client {j}: received {i}, after {:.5} secs",
                            elapsed.as_secs_f64()
                        );
                    }
                    tx_acker.send(msg).unwrap();
                    if i >= msgs_per_client - 1 {
                        break;
                    }
                }
            });

            let client_pub = ClientOptions::new()
                .auth("fibril", "fibril")
                .connect(address)
                .await
                .unwrap();

            let pubber = tokio::spawn(async move {
                if !start_writer {
                    return;
                }
                let start_inner = Instant::now();
                let publisher = client_pub.publisher("Topic1");

                for i in 1..=msgs_per_client {
                    publisher.publish_unconfirmed(&payload).await.unwrap();

                    if i.is_multiple_of(1000) {
                        let elapsed = start_inner.elapsed();
                        println!(
                            "Client {j}: sent {i}, after {:.5} secs",
                            elapsed.as_secs_f64()
                        );
                    }
                }
            });

            let acker = tokio::spawn(async move {
                while let Some(msg) = rx_acker.recv().await {
                    msg.complete().await.unwrap();
                }
            });

            tokio::join!(reader, pubber, acker);
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();

    println!(
        "Throughput: {} msgs/sec",
        (num_clients * msgs_per_client) as f64 / elapsed.as_secs_f64()
    );

    txb.send(()).unwrap();
}

#[derive(Parser, Debug)]
#[command(author, version, about)]
struct Args {
    /// Number of concurrent clients
    #[arg(short, long, default_value_t = 10)]
    clients: usize,

    /// Messages per client
    #[arg(short, long, default_value_t = 500_000)]
    messages: usize,

    /// Enable reader
    #[arg(long, default_value_t = false)]
    reader: bool,

    /// Enable writer
    #[arg(long, default_value_t = false)]
    writer: bool,
}

#[tokio::main]
async fn main() {
    fibril_util::init_tracing();

    let args = Args::parse();

    let (txb, rxb) = oneshot::channel::<()>();

    tokio::spawn(async move {
        run_load_test(args.clients, args.messages, args.reader, args.writer, txb).await;
    });

    rxb.await.unwrap();
}
