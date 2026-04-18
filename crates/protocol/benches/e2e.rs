use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    time::Duration,
};

use fibril_broker::{
    broker::{Broker, BrokerConfig},
    queue_engine::{KeratinConfig, SnapshotConfig, StromaEngine},
};
use fibril_metrics::Metrics;
use fibril_protocol::v1::{
    Auth, Hello, Op, PROTOCOL_V1, Publish, Subscribe, frame::ProtoCodec, handler::run_server,
    helper::encode,
};
use fibril_util::{StaticAuthHandler, init_tracing};
use futures::{SinkExt, StreamExt};
use tokio::{net::TcpStream, sync::oneshot, time::Instant};
use tokio_util::codec::Framed;

async fn run_load_test(num_clients: usize, msgs_per_client: usize, txb: oneshot::Sender<()>) {
    let start = Instant::now();

    let mut handles = vec![];

    for j in 0..num_clients {
        handles.push(tokio::spawn(async move {
            let payload = vec![8u8; 1024];
            let publ = Publish {
                group: None,
                topic: "Topic1".into(),
                payload: payload.clone(),
                partition: 1,
                require_confirm: true,
            };
            let stream = TcpStream::connect(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from([127, 0, 0, 1]),
                9876,
            )))
            .await
            .unwrap();
            let framed = Framed::new(stream, ProtoCodec);

            let (mut sink, mut stream) = framed.split();

            let writer = tokio::spawn(async move {
                let start_inner = Instant::now();
                sink.send(encode(
                    Op::Hello,
                    1,
                    &Hello {
                        client_name: "Client1".into(),
                        client_version: "0.1".into(),
                        protocol_version: PROTOCOL_V1,
                    },
                ))
                .await
                .unwrap();
                sink.send(fibril_protocol::v1::helper::encode(
                    Op::Auth,
                    2,
                    &Auth {
                        username: "fibril".to_string(),
                        password: "fibril".to_string(),
                    },
                ))
                .await
                .unwrap();
                sink.send(encode(
                    Op::Subscribe,
                    3,
                    &Subscribe {
                        auto_ack: true,
                        group: None,
                        topic: "Topic1".into(),
                        prefetch: 100,
                    },
                ))
                .await
                .unwrap();

                for i in 0..msgs_per_client {
                    sink.send(encode(Op::Publish, i as u64, &publ))
                        .await
                        .unwrap();
                    if i % 1000 == 0 {
                        let elapsed = start_inner.elapsed();
                        println!("Client {j}: sent {i}, after {} secs", elapsed.as_secs_f64());
                    }
                }
            });

            let reader = tokio::spawn(async move {
                let start_inner = Instant::now();
                let mut i = 0;
                while let Some(frame) = stream.next().await {
                    // just drain
                    if frame.unwrap().opcode == Op::Deliver as u16 {
                        i += 1;
                        if i % 1000 == 0 {
                            let elapsed = start_inner.elapsed();
                            println!(
                                "Client {j}: received {i}, after {} secs",
                                elapsed.as_secs_f64()
                            );
                        }
                    }
                    if i >= msgs_per_client - 1 {
                        break;
                    }
                }
            });

            tokio::join!(reader, writer);
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
    tokio::spawn(async move {
        run_load_test(1, 50000, txb).await;
    });

    rxb.await.unwrap();
}
