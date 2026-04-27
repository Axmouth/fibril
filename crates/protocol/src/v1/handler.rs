use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, atomic::AtomicU64},
    time::Instant,
};

use crate::v1::{
    frame::{Frame, ProtoCodec},
    helper::{decode, encode},
    *,
};
use anyhow::Context;
use fibril_broker::{
    broker::{
        Broker, BrokerError, ConsumerConfig, ConsumerHandle, PublisherHandle, SettleRequest,
        SettleType,
    },
    queue_engine::StromaEngine,
};
use fibril_metrics::{ConnectionStats, TcpStats};
use fibril_storage::{Group, Topic};
use fibril_util::{AuthHandler, unix_millis};
use futures::{SinkExt, StreamExt};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_util::codec::Framed;
use uuid::Uuid;

type SubKey = (Topic, Option<Group>); // (topic, group)

struct ConnState {
    authenticated: bool,
    subs: HashMap<SubKey, SubState>,
}

struct SubState {
    sub_id: u64,
    auto_ack: bool,
    state_settler: tokio::sync::mpsc::Sender<SettleRequest>,
    task: tokio::task::JoinHandle<()>,
}

pub struct ConnectionSettings {
    pub heartbeat_interval: Option<u64>,
}

impl ConnectionSettings {
    pub fn new(heartbeat_interval: Option<u64>) -> Self {
        Self { heartbeat_interval }
    }
}

#[derive(Debug)]
struct ReqIdGenerator {
    current_id: AtomicU64,
}

impl ReqIdGenerator {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            current_id: AtomicU64::from(0),
        })
    }

    pub fn next_id(&self) -> u64 {
        self.current_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }
}

pub async fn run_server(
    addr: SocketAddr,
    broker: Arc<Broker<StromaEngine>>,
    tcp_stats: Arc<TcpStats>,
    connection_stats: Arc<ConnectionStats>,
    auth: Option<impl AuthHandler + Send + Sync + Clone + 'static>,
) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    print_banner(&addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        tcp_stats.connection_opened();
        let conn_id = connection_stats.add_connection(peer, Instant::now(), false);
        let broker = broker.clone();

        let auth = auth.clone();
        let tcp_stats = tcp_stats.clone();
        let connection_stats = connection_stats.clone();
        let connection_settings = ConnectionSettings::new(None);
        tokio::spawn(async move {
            tracing::info!("Connection {conn_id} opening..");
            if let Err(e) = handle_connection(
                socket,
                broker,
                tcp_stats.clone(),
                connection_stats.clone(),
                conn_id,
                auth,
                connection_settings,
            )
            .await
            {
                tracing::error!("conn {} error: {:?}", peer, e);
            }

            connection_stats.remove_connection(&conn_id);

            tracing::info!("Connection {conn_id} closed..");
        });
    }
}

pub const DEFAULT_HEARTBEAT_INTERVAL: u64 = 5; // seconds

pub enum LoopEvent {
    Heartbeat,
    Frame(Frame),
    Disconnect,
    Timeout,
}

// TODO: Resolve publish drowning out delivery

pub async fn handle_connection(
    socket: tokio::net::TcpStream,
    broker: Arc<Broker<StromaEngine>>,
    tcp_stats: Arc<TcpStats>,
    connection_stats: Arc<ConnectionStats>,
    conn_id: Uuid,
    auth_handler: Option<impl AuthHandler + Send + Sync>,
    connection_settings: ConnectionSettings,
) -> anyhow::Result<()> {
    let heartbeat_interval = connection_settings
        .heartbeat_interval
        .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL);
    let timeout = tokio::time::Duration::from_secs(heartbeat_interval * 3);
    // TODO: Consider sharing it between connections?
    let req_id_gen = ReqIdGenerator::new();

    let mut last_seen = Instant::now();
    let mut heartbeat = tokio::time::interval(tokio::time::Duration::from_secs(heartbeat_interval));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // consume first tick
    // ---- Framed socket -----------------------------------------------------
    let peer_addr = socket.peer_addr().ok();
    let framed = Framed::new(socket, ProtoCodec);
    let (mut writer, mut reader) = framed.split();

    // ---- Write fan-in channel ---------------------------------------------
    let (frame_tx_high_prio, mut frame_rx_high_prio) = mpsc::channel::<Frame>(2048);
    let (frame_tx_low_prio, mut frame_rx_low_prio) = mpsc::channel::<Frame>(16);

    let metrics_clone = tcp_stats.clone();
    // ---- Writer task -------------------------------------------------------
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let req_id_gen_clone = req_id_gen.clone();
    let writer_task = tokio::spawn(async move {
        tracing::debug!("[writer] START");

        let metrics = metrics_clone.clone();
        let mut ticker = tokio::time::interval(std::time::Duration::from_millis(2));
        ticker.tick().await;

        let mut non_flushed_messages: usize = 0;
        let mut last_flush = Instant::now();
        let mut bytes_queued: usize = 0;
        loop {
            tokio::select! {
                biased;

                // ---- Shutdown signal -----------------------------------------
                _ = &mut shutdown_rx => {
                    tracing::debug!("[writer] Received shutdown signal");
                    break;
                }

                // ---- Normal write path ---------------------------------------
                Some(frame) = frame_rx_high_prio.recv() => {
                    tracing::debug!(
                        "[writer] Writing Frame to tcp socket.. code={}",
                        frame.opcode
                    );

                    let size = size_of_val(&frame) + frame.payload.len();

                    if let Err(err) = writer.feed(frame).await {
                        metrics.error();
                        tracing::warn!("[writer] Error writing to tcp socket : {err}");
                        break;
                    } else {
                        metrics.bytes_out(size as u64);
                        non_flushed_messages += 1;
                        bytes_queued += size;

                        if non_flushed_messages >= 32 {
                            let _ = writer.flush().await;
                            non_flushed_messages = 0;
                            last_flush = Instant::now();
                            bytes_queued = 0;
                        }
                    }
                }

                Some(frame) = frame_rx_low_prio.recv() => {
                    tracing::debug!(
                        "[writer] Writing Frame to tcp socket.. code={}",
                        frame.opcode
                    );

                    let size = size_of_val(&frame) + frame.payload.len();

                    if let Err(err) = writer.feed(frame).await {
                        metrics.error();
                        tracing::error!("[writer] Error writing to tcp socket : {err}");
                        break;
                    } else {
                        metrics.bytes_out(size as u64);
                        non_flushed_messages += 1;
                        bytes_queued += size;
                    }
                }

                _ = ticker.tick() => {
                    // pass
                }

                // ---- Channel closed ------------------------------------------
                else => break,
            }

            // Basic batching logic, limited by message number, time or total bytes to send
            if (non_flushed_messages > 0)
                && (non_flushed_messages >= 128
                    || bytes_queued >= 1024 * 1024
                    || last_flush.elapsed().as_millis() >= 5)
            {
                if let Err(err) = writer.flush().await {
                    metrics.error();
                    tracing::warn!("[writer] Error writing to tcp socket : {err}");
                    break;
                } else {
                    non_flushed_messages = 0;
                    last_flush = Instant::now();
                    bytes_queued = 0;
                }
            }
        }

        tracing::debug!("[writer] EXIT");
    });

    // ---- Connection state --------------------------------------------------
    let mut state = ConnState {
        authenticated: false,
        subs: HashMap::new(),
    };

    // ---- HELLO handshake ---------------------------------------------------
    let frame = reader
        .next()
        .await
        .context("connection closed before HELLO")??;

    if frame.opcode != Op::Hello as u16 {
        frame_tx_high_prio
            .send(encode(
                Op::Error,
                frame.request_id,
                &ErrorMsg {
                    code: 400,
                    message: "expected HELLO".into(),
                },
            ))
            .await
            .ok();
        tcp_stats.error();
        return Ok(());
    }

    let hello: Hello = decode(&frame);

    if hello.protocol_version != PROTOCOL_V1 {
        frame_tx_high_prio
            .send(encode(
                Op::HelloErr,
                frame.request_id,
                &ErrorMsg {
                    code: 1,
                    message: "unsupported protocol version".into(),
                },
            ))
            .await
            .ok();
        tcp_stats.error();
        return Ok(());
    }

    let hello_ok = &HelloOk {
        protocol_version: PROTOCOL_V1,
        server_name: "rust-broker".into(),
        compliance: "v=1;license=MIT;ai_train=disallowed;policy=AI_POLICY.md".to_owned(),
    };

    // TODO: Two ways handshake?
    if hello_ok.compliance != COMPLIANCE_STRING {
        tracing::warn!(
            id = "NF-SOVEREIGN-2025-GN-OPT-OUT-TDM",
            expected = COMPLIANCE_STRING,
            got = %hello_ok.compliance,
            "Invariant violated: compliance marker altered or missing"
        );
        anyhow::bail!("Protocol compliance marker mismatch");
    }

    frame_tx_high_prio
        .send(encode(Op::HelloOk, frame.request_id, &hello_ok))
        .await?;

    let (pub_tx, mut pub_rx) = tokio::sync::mpsc::channel::<(
        Result<oneshot::Receiver<Result<u64, BrokerError>>, fibril_broker::broker::BrokerError>,
        bool,
        u64,
    )>(8192);

    let metrics_pub = tcp_stats.clone();
    let frame_tx_pub = frame_tx_low_prio.clone();

    let pub_queue_handle = tokio::spawn(async move {
        while let Some((published, require_confirm, request_id)) = pub_rx.recv().await {
            match published {
                Ok(offset_rx) => {
                    let offset = match offset_rx.await {
                        Ok(Ok(offset)) => offset,
                        Ok(Err(err)) => {
                            if require_confirm {
                                let send_res = frame_tx_pub
                                    .send(encode(
                                        Op::Error,
                                        request_id,
                                        &ErrorMsg {
                                            code: 500,
                                            message: err.to_string(),
                                        },
                                    ))
                                    .await;

                                if let Err(err) = send_res {
                                    tracing::error!("Error sending Publish Confirm: {err}");
                                }
                                metrics_pub.error();
                            }
                            continue;
                        }
                        Err(_err) => {
                            if require_confirm {
                                let send_res = frame_tx_pub
                                    .send(encode(
                                        Op::Error,
                                        request_id,
                                        &ErrorMsg {
                                            code: 500,
                                            message: "Channel closed".into(),
                                        },
                                    ))
                                    .await;

                                if let Err(err) = send_res {
                                    tracing::error!("Error sending Publish Confirm: {err}");
                                }
                                metrics_pub.error();
                            }
                            continue;
                        }
                    };
                    if require_confirm {
                        let send_res = frame_tx_pub
                            .send(encode(Op::PublishOk, request_id, &PublishOk { offset }))
                            .await;

                        if let Err(err) = send_res {
                            tracing::error!("Error sending Publish Confirm: {err}");
                        }
                    }
                }
                Err(err) => {
                    let send_res = frame_tx_pub
                        .send(encode(
                            Op::Error,
                            request_id,
                            &ErrorMsg {
                                code: 500,
                                message: err.to_string(),
                            },
                        ))
                        .await;

                    if let Err(err) = send_res {
                        tracing::error!("Error sending Publish Confirm: {err}");
                    }
                    metrics_pub.error();
                }
            }
        }
    });

    let mut publishers = HashMap::<(Topic, Option<Group>), PublisherHandle>::new();

    // ---- Main reader loop --------------------------------------------------
    // TODO: Make handling more async? Spawn task per frame, or have a task pool
    loop {
        let loop_event = tokio::select! {
            // ---- Heartbeat tick ----
            _ = heartbeat.tick() => {
                if last_seen.elapsed() > timeout {
                    tracing::warn!("Heartbeat timeout, closing connection");
                    LoopEvent::Timeout
                }
                else if frame_tx_high_prio.send(encode(Op::Ping, req_id_gen_clone.next_id(), &())).await.is_err() {
                    // channel full or closed -> treat as disconnect
                    tracing::error!("Failed to send heartbeat ping, closing connection");
                    LoopEvent::Timeout
                }
                else {
                    LoopEvent::Heartbeat
                }
            }

            // ---- Incoming frame ----
            frame = reader.next() => {
                match frame {
                    Some(Ok(f)) => {
                        last_seen = Instant::now();

                        LoopEvent::Frame(f)
                    },
                    Some(Err(err)) => {
                        tracing::warn!("[reader] Error reading from tcp socket: {err}");
                        LoopEvent::Disconnect
                    },
                    None => {
                        tracing::info!("[reader] Disconnected from tcp socket");
                        LoopEvent::Disconnect
                    },
                }
            }
        };

        let frame = match loop_event {
            LoopEvent::Frame(f) => f,
            LoopEvent::Timeout => break,
            LoopEvent::Disconnect => break,
            LoopEvent::Heartbeat => continue,
        };

        let metrics = tcp_stats.clone();
        let size = size_of_val(&frame) + frame.payload.len();
        metrics.bytes_in(size as u64);

        let auth_required = auth_handler.is_some();
        let is_auth = frame.opcode == Op::Auth as u16;
        let is_ping = frame.opcode == Op::Ping as u16;
        let is_pong = frame.opcode == Op::Pong as u16;

        if auth_required && !state.authenticated && !(is_auth || is_ping || is_pong) {
            frame_tx_high_prio
                .send(encode(
                    Op::Error,
                    frame.request_id,
                    &ErrorMsg {
                        code: 401,
                        message: "authentication required".into(),
                    },
                ))
                .await?;
            metrics.error();

            break; // close connection
        }

        match frame.opcode {
            // -------- AUTH ---------------------------------------------------
            x if x == Op::Auth as u16 => {
                if state.authenticated {
                    frame_tx_high_prio
                        .send(encode(
                            Op::AuthErr,
                            frame.request_id,
                            &ErrorMsg {
                                code: 401,
                                message: "already authenticated".into(),
                            },
                        ))
                        .await?;
                    metrics.error();
                } else if auth_handler.is_none() {
                    frame_tx_high_prio
                        .send(encode(
                            Op::AuthErr,
                            frame.request_id,
                            &ErrorMsg {
                                code: 400,
                                message: "authentication not applicable".into(),
                            },
                        ))
                        .await?;
                    metrics.error();
                } else {
                    // ---- AUTH handshake -------------------------------------
                    if let Some(auth_handler) = &auth_handler {
                        let auth_frame: Auth = decode(&frame);

                        let verified = auth_handler
                            .verify(&auth_frame.username, &auth_frame.password)
                            .await;

                        if verified {
                            state.authenticated = true;
                            frame_tx_high_prio
                                .send(encode(Op::AuthOk, frame.request_id, &()))
                                .await?;
                            connection_stats.set_connection_auth(&conn_id, true);
                        } else {
                            frame_tx_high_prio
                                .send(encode(
                                    Op::AuthErr,
                                    frame.request_id,
                                    &ErrorMsg {
                                        code: 401,
                                        message: "invalid credentials".into(),
                                    },
                                ))
                                .await?;
                            metrics.error();

                            break; // close connection
                        }
                    }
                }
            }

            // -------- SUBSCRIBE ---------------------------------------------
            x if x == Op::Subscribe as u16 => {
                let sub: Subscribe = decode(&frame);

                let sub_key: SubKey = (sub.topic.clone(), sub.group.clone());

                if state.subs.contains_key(&sub_key) {
                    frame_tx_high_prio
                        .send(encode(
                            Op::SubscribeErr,
                            frame.request_id,
                            &ErrorMsg {
                                code: 409,
                                message: "already subscribed".into(),
                            },
                        ))
                        .await?;
                    metrics.error();
                    continue;
                }

                let consumer = broker
                    .subscribe(
                        &sub.topic,
                        sub.group.as_deref(),
                        ConsumerConfig {
                            prefetch: sub.prefetch as usize,
                        },
                    )
                    .await
                    .context("subscribe failed")?;

                let ConsumerHandle {
                    messages,
                    settler,
                    sub_id,
                    ..
                } = consumer;

                let auto_ack = sub.auto_ack;
                let frame_tx_clone = frame_tx_high_prio.clone();

                connection_stats.add_sub(
                    &conn_id,
                    sub.topic.clone(),
                    sub.group.clone(),
                    Instant::now(),
                    auto_ack,
                );

                let settler_clone = settler.clone();
                let req_id_gen_clone = req_id_gen.clone();
                let handle = tokio::spawn(async move {
                    let mut rx = messages;

                    while let Some(msg) = rx.recv().await {
                        let deliver = Deliver {
                            sub_id,
                            topic: msg.message.topic.clone(),
                            group: msg.group.clone(),
                            partition: msg.message.partition,
                            offset: msg.message.offset,
                            delivery_tag: msg.delivery_tag,
                            published: msg.message.published,
                            publish_received: msg.message.publish_received,
                            payload: msg.message.payload.clone(),
                        };

                        tracing::debug!("Sending Deliver");

                        // 1. Try to write to socket
                        if let Err(err) = frame_tx_clone
                            .send(encode(Op::Deliver, req_id_gen_clone.next_id(), &deliver))
                            .await
                        {
                            // Socket dead -> do NOT auto-ack
                            tracing::warn!("Failed to send to socket writer : {err}");
                            metrics.error();
                            break;
                        }

                        // 2. Auto-ack ONLY after successful send
                        if auto_ack {
                            let _ = settler
                                .send(SettleRequest {
                                    settle_type: SettleType::Ack,
                                    delivery_tag: msg.delivery_tag,
                                })
                                .await;
                        }
                    }
                });

                state.subs.insert(
                    sub_key,
                    SubState {
                        sub_id,
                        task: handle,
                        auto_ack: sub.auto_ack,
                        state_settler: settler_clone,
                    },
                );

                frame_tx_high_prio
                    .send(encode(
                        Op::SubscribeOk,
                        frame.request_id,
                        &SubscribeOk {
                            sub_id,
                            topic: sub.topic,
                            group: sub.group,
                            partition: consumer.partition,
                            prefetch: sub.prefetch,
                        },
                    ))
                    .await?;
            }

            // -------- ACK ----------------------------------------------------
            x if x == Op::Ack as u16 => {
                // TODO: Decline ack when auto ack? Log?
                let ack: Ack = decode(&frame);

                let key: SubKey = (ack.topic.clone(), ack.group.clone());

                if let Some(sub) = state.subs.get(&key)
                    && !sub.auto_ack
                {
                    let state_settler = sub.state_settler.clone();
                    tokio::spawn(async move {
                        for tag in ack.tags {
                            let req = SettleRequest {
                                delivery_tag: tag,
                                settle_type: SettleType::Ack,
                            };
                            let _ = state_settler.send(req).await;
                        }
                    });
                }
                // Unknown subscription: ignore (idempotent)
            }

            // -------- NACK ----------------------------------------------------
            x if x == Op::Nack as u16 => {
                // TODO: Decline ack when auto ack? Log?
                let nack: Nack = decode(&frame);

                let key: SubKey = (nack.topic.clone(), nack.group.clone());

                if let Some(sub) = state.subs.get(&key)
                    && !sub.auto_ack
                {
                    let state_settler = sub.state_settler.clone();
                    tokio::spawn(async move {
                        for tag in nack.tags {
                            let req = SettleRequest {
                                delivery_tag: tag,
                                settle_type: SettleType::Nack {
                                    requeue: Some(nack.requeue),
                                },
                            };
                            let _ = state_settler.send(req).await;
                        }
                    });
                }
                // Unknown subscription: ignore (idempotent)
            }

            // -------- REJECT ----------------------------------------------------
            x if x == Op::Reject as u16 => {
                // TODO: Decline ack when auto ack? Log?
                let reject: Reject = decode(&frame);

                let key: SubKey = (reject.topic.clone(), reject.group.clone());

                if let Some(sub) = state.subs.get(&key)
                    && !sub.auto_ack
                {
                    let state_settler = sub.state_settler.clone();
                    tokio::spawn(async move {
                        for tag in reject.tags {
                            let req = SettleRequest {
                                delivery_tag: tag,
                                settle_type: SettleType::Reject {
                                    requeue: Some(reject.requeue),
                                },
                            };
                            let _ = state_settler.send(req).await;
                        }
                    });
                }
                // Unknown subscription: ignore (idempotent)
            }

            // -------- PUBLISH ------------------------------------------------
            x if x == Op::Publish as u16 => {
                let publish_received = unix_millis();
                let pubreq: Publish = decode(&frame);

                // TODO: Handle confirm stream properly
                let key = &(pubreq.topic.clone(), pubreq.group.clone());
                let publisher = if let Some(pubh) = publishers.get(key) {
                    pubh.clone()
                } else {
                    let (pubh, mut conf_stream) = broker
                        .get_publisher(&pubreq.topic, &pubreq.group)
                        .await
                        .context("get publisher failed")?;

                    // let conf_sink = frame_tx_low_prio.clone();
                    // let req_id_gen_clone = req_id_gen.clone();
                    tokio::spawn(async move {
                        while let Some(offset) = conf_stream.recv().await {
                            // let res = conf_sink.send(encode(Op::PublishOk, req_id_gen_clone.next_id(), &PublishOk {offset})).await;

                            // if let Err(_) = res {
                            //     tracing::warn!("Error sending confirm for offset {offset}");
                            // }

                            // TODO: confirms are handled elsewhere, see if there's a cleaner way than this
                            let _ = offset;
                        }
                    });
                    publishers.insert(key.clone(), pubh.clone());
                    pubh
                };
                let pub_tx = pub_tx.clone();
                let frame_tx_pub = frame_tx_low_prio.clone();
                tokio::spawn(async move {
                    let published: Result<
                        tokio::sync::oneshot::Receiver<
                            Result<u64, fibril_broker::broker::BrokerError>,
                        >,
                        fibril_broker::broker::BrokerError,
                    > = if pubreq.require_confirm {
                        publisher
                            .publish(
                                &pubreq.payload,
                                pubreq.published,
                                publish_received,
                                HashMap::new(),
                            )
                            .await
                    } else {
                        publisher
                            .publish_no_confirm(
                                &pubreq.payload,
                                pubreq.published,
                                publish_received,
                                HashMap::new(),
                            )
                            .await
                    };
                    let res = pub_tx
                        .send((published, pubreq.require_confirm, frame.request_id))
                        .await;
                    if let Err(_err) = res {
                        let _ = frame_tx_pub
                            .send(encode(
                                Op::Error,
                                frame.request_id,
                                &ErrorMsg {
                                    code: 500,
                                    message: "Broken pipe".into(),
                                },
                            ))
                            .await;
                        tracing::error!("Error sending published to queue");
                    }
                });
            }

            // -------- PING ---------------------------------------------------
            x if x == Op::Ping as u16 => {
                frame_tx_high_prio
                    .send(encode(Op::Pong, frame.request_id, &()))
                    .await?;
            }

            // -------- PONG ---------------------------------------------------
            x if x == Op::Pong as u16 => {
                // pass
            }

            // -------- UNKNOWN -----------------------------------------------
            _ => {
                frame_tx_high_prio
                    .send(encode(
                        Op::Error,
                        frame.request_id,
                        &ErrorMsg {
                            code: 400,
                            message: "unknown opcode".into(),
                        },
                    ))
                    .await?;
                metrics.error();
            }
        }
    }

    // ---- Connection closing ------------------------------------------------
    // closes writer channel
    drop(frame_tx_high_prio);
    drop(frame_tx_low_prio);

    let _ = shutdown_tx.send(());
    writer_task.abort();
    pub_queue_handle.await?;

    for (_, sub) in state.subs.drain() {
        sub.task.abort();
    }

    tracing::debug!("[conn] EXIT handle_connection peer={:?}", peer_addr);

    tcp_stats.connection_closed();
    Ok(())
}

pub fn print_banner(bind: &SocketAddr) {
    let ts = Instant::now().elapsed().as_nanos();
    let idx = (ts % (ASCII_ARTS.len() as u128)) as usize;

    let art = ASCII_ARTS[idx];

    tracing::info!("\n{art}\nListening on {bind}\n");
}

const ASCII_ARTS: &[&str] = &[
    r#"
███████╗██╗██████╗ ██████╗ ██╗██╗     
██╔════╝██║██╔══██╗██╔══██╗██║██║     
█████╗  ██║██████╔╝██████╔╝██║██║     
██╔══╝  ██║██╔══██╗██╔══██╗██║██║     
██║     ██║██████╔╝██║  ██║██║███████╗
╚═╝     ╚═╝╚═════╝ ╚═╝  ╚═╝╚═╝╚══════╝
                                         
                                         
"#,
    r#"
                                                  
88888888888  88  88                       88  88  
88           ""  88                       ""  88  
88               88                           88  
88aaaaa      88  88,dPPYba,   8b,dPPYba,  88  88  
88"""""      88  88P'    "8a  88P'   "Y8  88  88  
88           88  88       d8  88          88  88  
88           88  88b,   ,a8"  88          88  88  
88           88  8Y"Ybbd8"'   88          88  88  
                                                  
                                                  
"#,
    r#"
'||''''|  ||  '||               ||  '||  
 ||  .   ...   || ...  ... ..  ...   ||  
 ||''|    ||   ||'  ||  ||' ''  ||   ||  
 ||       ||   ||    |  ||      ||   ||  
.||.     .||.  '|...'  .||.    .||. .||. 
                                         
                                         
"#,
    r#"
'||''''|      '||                 '||` 
 ||  .    ''   ||             ''   ||  
 ||''|    ||   ||''|, '||''|  ||   ||  
 ||       ||   ||  ||  ||     ||   ||  
.||.     .||. .||..|' .||.   .||. .||. 
                                       
                                       
"#,
    r#"
 ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄   ▄▄▄▄▄▄▄▄▄▄▄  ▄▄▄▄▄▄▄▄▄▄▄  ▄           
▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░▌ ▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌▐░▌          
▐░█▀▀▀▀▀▀▀▀▀  ▀▀▀▀█░█▀▀▀▀ ▐░█▀▀▀▀▀▀▀█░▌▐░█▀▀▀▀▀▀▀█░▌ ▀▀▀▀█░█▀▀▀▀ ▐░▌          
▐░▌               ▐░▌     ▐░▌       ▐░▌▐░▌       ▐░▌     ▐░▌     ▐░▌          
▐░█▄▄▄▄▄▄▄▄▄      ▐░▌     ▐░█▄▄▄▄▄▄▄█░▌▐░█▄▄▄▄▄▄▄█░▌     ▐░▌     ▐░▌          
▐░░░░░░░░░░░▌     ▐░▌     ▐░░░░░░░░░░▌ ▐░░░░░░░░░░░▌     ▐░▌     ▐░▌          
▐░█▀▀▀▀▀▀▀▀▀      ▐░▌     ▐░█▀▀▀▀▀▀▀█░▌▐░█▀▀▀▀█░█▀▀      ▐░▌     ▐░▌          
▐░▌               ▐░▌     ▐░▌       ▐░▌▐░▌     ▐░▌       ▐░▌     ▐░▌          
▐░▌           ▄▄▄▄█░█▄▄▄▄ ▐░█▄▄▄▄▄▄▄█░▌▐░▌      ▐░▌  ▄▄▄▄█░█▄▄▄▄ ▐░█▄▄▄▄▄▄▄▄▄ 
▐░▌          ▐░░░░░░░░░░░▌▐░░░░░░░░░░▌ ▐░▌       ▐░▌▐░░░░░░░░░░░▌▐░░░░░░░░░░░▌
 ▀            ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀   ▀         ▀  ▀▀▀▀▀▀▀▀▀▀▀  ▀▀▀▀▀▀▀▀▀▀▀ 
                                                                              
"#,
    r#"
                                                                                    
                             bbbbbbbb                                               
FFFFFFFFFFFFFFFFFFFFFF  iiii b::::::b                                 iiii  lllllll 
F::::::::::::::::::::F i::::ib::::::b                                i::::i l:::::l 
F::::::::::::::::::::F  iiii b::::::b                                 iiii  l:::::l 
FF::::::FFFFFFFFF::::F        b:::::b                                       l:::::l 
  F:::::F       FFFFFFiiiiiii b:::::bbbbbbbbb    rrrrr   rrrrrrrrr  iiiiiii  l::::l 
  F:::::F             i:::::i b::::::::::::::bb  r::::rrr:::::::::r i:::::i  l::::l 
  F::::::FFFFFFFFFF    i::::i b::::::::::::::::b r:::::::::::::::::r i::::i  l::::l 
  F:::::::::::::::F    i::::i b:::::bbbbb:::::::brr::::::rrrrr::::::ri::::i  l::::l 
  F:::::::::::::::F    i::::i b:::::b    b::::::b r:::::r     r:::::ri::::i  l::::l 
  F::::::FFFFFFFFFF    i::::i b:::::b     b:::::b r:::::r     rrrrrrri::::i  l::::l 
  F:::::F              i::::i b:::::b     b:::::b r:::::r            i::::i  l::::l 
  F:::::F              i::::i b:::::b     b:::::b r:::::r            i::::i  l::::l 
FF:::::::FF           i::::::ib:::::bbbbbb::::::b r:::::r           i::::::il::::::l
F::::::::FF           i::::::ib::::::::::::::::b  r:::::r           i::::::il::::::l
F::::::::FF           i::::::ib:::::::::::::::b   r:::::r           i::::::il::::::l
FFFFFFFFFFF           iiiiiiiibbbbbbbbbbbbbbbb    rrrrrrr           iiiiiiiillllllll
                                                                                    
                                                                                    
                                                                                    
                                                                                    
                                                                                    
                                                                                    
                                                                                    
"#,
];
