use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use fibril_client::{AckableMessage, ClientOptions};
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

            let client_pub = client.clone();
            let pubber = tokio::spawn(async move {
                if !start_writer {
                    return;
                }
                let start_inner = Instant::now();
                let publisher = client_pub.publisher("Topic1");

                for i in 1..=msgs_per_client {
                    publisher.publish(&payload).await.unwrap();

                    if i.is_multiple_of(1000) {
                        let elapsed = start_inner.elapsed();
                        println!("Client {j}: sent {i}, after {} secs", elapsed.as_secs_f64());
                    }
                }
            });

            let (tx_acker, mut rx_acker) = tokio::sync::mpsc::unbounded_channel::<AckableMessage>();
            let client_reader = client.clone();
            let reader = tokio::spawn(async move {
                if !start_reader {
                    return;
                }
                let mut sub = client_reader
                    .subscribe("Topic1")
                    .prefetch(1024)
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
                            "Client {j}: received {i}, after {} secs",
                            elapsed.as_secs_f64()
                        );
                    }
                    tx_acker.send(msg).unwrap();
                    if i >= msgs_per_client - 1 {
                        break;
                    }
                }
            });

            let _acker = tokio::spawn(async move {
                while let Some(msg) = rx_acker.recv().await {
                    msg.ack().await.unwrap();
                }
            });

            tokio::join!(reader, pubber);
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

#[tokio::main]
async fn main() {
    // init_tracing();
    let (txb, rxb) = oneshot::channel::<()>();
    let start_reader = true;
    let start_writer = true;
    tokio::spawn(async move {
        run_load_test(10, 50000, start_reader, start_writer, txb).await;
    });

    rxb.await.unwrap();
}
