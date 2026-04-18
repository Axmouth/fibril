use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use fibril_protocol::v1::{
    Ack, Auth, Deliver, Hello, Op, PROTOCOL_V1, Publish, Subscribe,
    frame::ProtoCodec,
    helper::{decode, encode},
};
use fibril_storage::DeliveryTag;
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

            let (tx_writer, mut rx_writer) = tokio::sync::mpsc::channel(4096);
            let tx_writer_clone = tx_writer.clone();
            let pubber = tokio::spawn(async move {
                let start_inner = Instant::now();

                for i in 1..=msgs_per_client {
                    tx_writer
                        .send(encode(Op::Publish, i as u64, &publ))
                        .await
                        .unwrap();

                    if i.is_multiple_of(1000) {
                        let elapsed = start_inner.elapsed();
                        println!("Client {j}: sent {i}, after {} secs", elapsed.as_secs_f64());
                    }
                }
            });

            let (tx_acker, mut rx_acker) = tokio::sync::mpsc::channel::<(DeliveryTag, u64)>(4096);
            let reader = tokio::spawn(async move {
                let start_inner = Instant::now();
                let mut i: usize = 0;
                while let Some(frame) = stream.next().await {
                    // just drain
                    let frame = frame.unwrap();
                    if frame.opcode == Op::Deliver as u16 {
                        i += 1;
                        if i.is_multiple_of(1000) {
                            let elapsed = start_inner.elapsed();
                            println!(
                                "Client {j}: received {i}, after {} secs",
                                elapsed.as_secs_f64()
                            );
                        }
                        let msg: Deliver = decode(&frame);
                        tx_acker
                            .send((msg.delivery_tag, frame.request_id))
                            .await
                            .unwrap();
                    }
                    if i >= msgs_per_client - 1 {
                        break;
                    }
                }
            });

            let acker = tokio::spawn(async move {
                while let Some((tag, req_id)) = rx_acker.recv().await {
                    let ack_msg = encode(
                        Op::Ack,
                        req_id,
                        &Ack {
                            group: None,
                            topic: "Topic1".into(),
                            partition: 1,
                            tags: vec![tag],
                        },
                    );

                    tx_writer_clone.send(ack_msg).await.unwrap();
                }
            });

            let sink_task = tokio::spawn(async move {
                // initial handshake
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

                sink.send(encode(
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
                        auto_ack: false,
                        group: None,
                        topic: "Topic1".into(),
                        prefetch: 1024,
                    },
                ))
                .await
                .unwrap();
                while let Some(msg) = rx_writer.recv().await {
                    sink.send(msg).await.unwrap();
                }
            });

            tokio::join!(reader, pubber, acker, sink_task);
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
        run_load_test(15, 50000, txb).await;
    });

    rxb.await.unwrap();
}
