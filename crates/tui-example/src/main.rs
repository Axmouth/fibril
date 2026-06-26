//! Live terminal visualizer of real Fibril wire traffic against a running broker.
//!
//! It drives the protocol at the frame level (so the handshake, subscribe,
//! publish, confirm, deliver, ping/pong and errors are all visible) and animates
//! every frame as a moving dot between client nodes and the broker's partition
//! lanes. Runtime-configurable, with small illustrative defaults. Point it at a live
//! broker with `--addr` (e.g. the one-command cluster demo).

use std::collections::VecDeque;
use std::io::stdout;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use crossterm::{
    cursor, execute,
    terminal::{Clear, ClearType, disable_raw_mode, enable_raw_mode},
};
use fibril_protocol::v1::{
    Auth, DeclareQueue, Deliver, ErrorMsg, Hello, Op, PROTOCOL_V1, Partition, Pong, Publish,
    Subscribe,
    frame::ProtoCodec,
    helper::{Conn, try_decode, try_encode},
};
use fibril_util::unix_millis;
use futures::{SinkExt, StreamExt};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::Rect,
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

/// Live visualizer of real Fibril traffic and partition routing.
#[derive(Debug, Clone, Parser)]
#[command(name = "fibril-tui", about = "Live Fibril traffic + partition visualizer")]
struct Args {
    /// Broker address to connect to.
    #[arg(long, default_value = "127.0.0.1:9876")]
    addr: String,

    /// Number of client connections (each both publishes and consumes).
    #[arg(long, default_value_t = 4)]
    clients: usize,

    /// Partition count to declare for the demo topic.
    #[arg(long, default_value_t = 4)]
    partitions: u32,

    /// Topic to publish to and consume from.
    #[arg(long, default_value = "demo")]
    topic: String,

    /// Optional queue group namespace.
    #[arg(long)]
    group: Option<String>,

    /// Publish rate per client, in messages per second.
    #[arg(long, default_value_t = 6.0)]
    rate: f64,

    /// Payload size in bytes per message.
    #[arg(long, default_value_t = 64)]
    payload_size: usize,

    /// Require a publish confirm (shows the confirm return path).
    #[arg(long)]
    confirm: bool,

    /// Broker credentials as `user:pass`.
    #[arg(long, default_value = "fibril:fibril")]
    auth: String,

    /// How keyed publishes pick a partition.
    #[arg(long, value_enum, default_value_t = KeyMode::Keyed)]
    key_mode: KeyMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum KeyMode {
    /// Route by `hash(key) % partitions` (per-key ordering is visible).
    Keyed,
    /// Spread round-robin across partitions, no key.
    RoundRobin,
}

/// Operation class of a frame, for coloring and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum VizOp {
    Hello,
    Auth,
    Subscribe,
    Declare,
    Publish,
    Confirm,
    Deliver,
    Ping,
    Pong,
    Error,
}

impl VizOp {
    fn color(self) -> Color {
        match self {
            VizOp::Hello | VizOp::Auth | VizOp::Subscribe | VizOp::Declare => Color::DarkGray,
            VizOp::Publish => Color::Blue,
            VizOp::Confirm => Color::Cyan,
            VizOp::Deliver => Color::Green,
            VizOp::Ping | VizOp::Pong => Color::DarkGray,
            VizOp::Error => Color::Red,
        }
    }
}

/// A frame observed on a client connection, sent to the UI to animate.
#[derive(Debug, Clone)]
struct VisualEvent {
    client: usize,
    op: VizOp,
    /// Direction: true = client -> broker, false = broker -> client.
    to_broker: bool,
    /// Partition lane this frame belongs to, when known.
    partition: Option<u32>,
    /// Publish -> deliver latency in ms, on a Deliver.
    latency_ms: Option<u64>,
}

/// Logical endpoint of an animated dot, resolved to a cell against the current
/// (responsive) layout each frame so resizes do not strand in-flight dots.
#[derive(Debug, Clone, Copy)]
enum Endpoint {
    Client(usize),
    Lane(u32),
}

#[derive(Debug, Clone)]
struct Ball {
    from: Endpoint,
    to: Endpoint,
    progress: f32,
    color: Color,
}

#[derive(Default)]
struct Metrics {
    published: u64,
    confirmed: u64,
    delivered: u64,
    errors: u64,
    latencies: VecDeque<u64>,
    // Rate sampling.
    last_sample: Option<(u64, u64, u64, Instant)>,
    pub_rate: f64,
    deliver_rate: f64,
    confirm_rate: f64,
}

impl Metrics {
    fn record_latency(&mut self, ms: u64) {
        self.latencies.push_back(ms);
        while self.latencies.len() > 512 {
            self.latencies.pop_front();
        }
    }

    fn percentile(&self, pct: f64) -> Option<u64> {
        if self.latencies.is_empty() {
            return None;
        }
        let mut sorted: Vec<u64> = self.latencies.iter().copied().collect();
        sorted.sort_unstable();
        let idx = ((pct / 100.0) * (sorted.len() as f64 - 1.0)).round() as usize;
        sorted.get(idx).copied()
    }

    /// Refresh per-second rates from cumulative counters. Call about once a second.
    fn refresh_rates(&mut self) {
        let now = Instant::now();
        if let Some((p0, d0, c0, t0)) = self.last_sample {
            let dt = now.duration_since(t0).as_secs_f64();
            if dt >= 0.25 {
                self.pub_rate = (self.published - p0) as f64 / dt;
                self.deliver_rate = (self.delivered - d0) as f64 / dt;
                self.confirm_rate = (self.confirmed - c0) as f64 / dt;
                self.last_sample = Some((self.published, self.delivered, self.confirmed, now));
            }
        } else {
            self.last_sample = Some((self.published, self.delivered, self.confirmed, now));
        }
    }
}

struct App {
    clients: usize,
    partitions: u32,
    topic: String,
    addr: String,
    balls: Vec<Ball>,
    metrics: Metrics,
}

static REQ: AtomicU64 = AtomicU64::new(1);

fn next_req_id() -> u64 {
    REQ.fetch_add(1, Ordering::Relaxed)
}

/// FNV-1a, matching the client/broker so keyed routing lands on the same lane.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for &b in bytes {
        hash ^= b as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

// ===== Layout =================================================================

struct Layout {
    client_boxes: Vec<Rect>,
    lane_boxes: Vec<Rect>,
    hud: Rect,
}

fn compute_layout(area: Rect, clients: usize, partitions: u32) -> Layout {
    let box_w: u16 = 16;
    let hud_h: u16 = 8;
    let top = area.y + 1;
    let usable_h = area.height.saturating_sub(hud_h + 2).max(1);

    let client_x = area.x + 1;
    let lane_x = area.x + area.width.saturating_sub(box_w + 1);

    let lay = |n: usize, x: u16| -> Vec<Rect> {
        let n = n.max(1);
        let step = (usable_h / n as u16).max(1);
        let h = step.saturating_sub(1).clamp(1, 3);
        (0..n)
            .map(|i| Rect::new(x, top + i as u16 * step, box_w, h))
            .collect()
    };

    Layout {
        client_boxes: lay(clients, client_x),
        lane_boxes: lay(partitions as usize, lane_x),
        hud: Rect::new(area.x + 1, area.y + area.height.saturating_sub(hud_h), area.width.saturating_sub(2), hud_h),
    }
}

fn endpoint_cell(layout: &Layout, ep: Endpoint) -> Option<(u16, u16)> {
    match ep {
        Endpoint::Client(i) => layout
            .client_boxes
            .get(i)
            .map(|r| (r.x + r.width, r.y + r.height / 2)),
        Endpoint::Lane(p) => layout
            .lane_boxes
            .get(p as usize)
            .map(|r| (r.x.saturating_sub(1), r.y + r.height / 2)),
    }
}

fn draw_box(f: &mut ratatui::Frame, rect: Rect, title: &str, color: Color) {
    if rect.height == 0 || rect.width == 0 {
        return;
    }
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(color))
        .title(Span::styled(title.to_string(), Style::default().fg(color)));
    f.render_widget(block, rect);
}

fn draw_ui(f: &mut ratatui::Frame, app: &App) {
    let area = f.area();
    if area.width < 48 || area.height < 16 {
        f.render_widget(
            Paragraph::new("Terminal too small - resize me").style(Style::default().fg(Color::Red)),
            area,
        );
        return;
    }
    let layout = compute_layout(area, app.clients, app.partitions);

    for (i, rect) in layout.client_boxes.iter().enumerate() {
        draw_box(f, *rect, &format!("client {i}"), Color::Gray);
    }
    for (p, rect) in layout.lane_boxes.iter().enumerate() {
        draw_box(f, *rect, &format!("part {p}"), Color::Yellow);
    }

    for ball in &app.balls {
        let (Some((x0, y0)), Some((x1, y1))) = (
            endpoint_cell(&layout, ball.from),
            endpoint_cell(&layout, ball.to),
        ) else {
            continue;
        };
        let t = ease(ball.progress);
        let x = lerp(x0, x1, t);
        let y = lerp(y0, y1, t);
        if x < area.x || y < area.y || x >= area.x + area.width || y >= area.y + area.height {
            continue;
        }
        let glyph = if ball.progress < 0.25 || ball.progress > 0.75 {
            "·"
        } else {
            "●"
        };
        f.render_widget(
            Paragraph::new(glyph).style(Style::default().fg(ball.color)),
            Rect::new(x, y, 1, 1),
        );
    }

    draw_hud(f, app, layout.hud);
}

fn draw_hud(f: &mut ratatui::Frame, app: &App, area: Rect) {
    let m = &app.metrics;
    let p50 = m.percentile(50.0).map(|v| format!("{v}ms")).unwrap_or_else(|| "-".into());
    let p99 = m.percentile(99.0).map(|v| format!("{v}ms")).unwrap_or_else(|| "-".into());
    let lines = vec![
        Line::from(vec![
            Span::styled("fibril viz  ", Style::default().fg(Color::Cyan)),
            Span::raw(format!(
                "{}  clients {}  partitions {}  topic {}",
                app.addr, app.clients, app.partitions, app.topic
            )),
        ]),
        Line::from(format!(
            "published {} ({:.0}/s)   confirmed {} ({:.0}/s)   delivered {} ({:.0}/s)   errors {}",
            m.published, m.pub_rate, m.confirmed, m.confirm_rate, m.delivered, m.deliver_rate, m.errors
        )),
        Line::from(format!(
            "in-flight (animating) {}   publish->deliver p50 {}  p99 {}",
            app.balls.len(),
            p50,
            p99
        )),
        Line::from(Span::styled(
            "publish=blue  confirm=cyan  deliver=green  setup/ping=gray  error=red    (q / Esc to quit)",
            Style::default().fg(Color::DarkGray),
        )),
    ];
    f.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("status")),
        area,
    );
}

fn ease(t: f32) -> f32 {
    let t = t.clamp(0.0, 1.0);
    t * t * (3.0 - 2.0 * t)
}

fn lerp(a: u16, b: u16, t: f32) -> u16 {
    let a = a as f32;
    let b = b as f32;
    (a + (b - a) * t).round() as u16
}

fn handle_event(app: &mut App, ev: VisualEvent) {
    match ev.op {
        VizOp::Publish => app.metrics.published += 1,
        VizOp::Confirm => app.metrics.confirmed += 1,
        VizOp::Deliver => {
            app.metrics.delivered += 1;
            if let Some(ms) = ev.latency_ms {
                app.metrics.record_latency(ms);
            }
        }
        VizOp::Error => app.metrics.errors += 1,
        _ => {}
    }

    // Frames with no partition (setup, confirm, ping, pong) animate to and from
    // the first lane, which reads as the broker hub.
    let lane = ev.partition.unwrap_or(0).min(app.partitions.saturating_sub(1));
    let (from, to) = if ev.to_broker {
        (Endpoint::Client(ev.client), Endpoint::Lane(lane))
    } else {
        (Endpoint::Lane(lane), Endpoint::Client(ev.client))
    };
    app.balls.push(Ball {
        from,
        to,
        progress: 0.0,
        color: ev.op.color(),
    });
}

fn update_balls(app: &mut App, dt: f32) {
    for b in &mut app.balls {
        b.progress = (b.progress + dt * 1.1).min(1.0);
    }
    app.balls.retain(|b| b.progress < 1.0);
    // Bound the animation set so a burst cannot grow it without limit.
    if app.balls.len() > 4000 {
        let excess = app.balls.len() - 4000;
        app.balls.drain(0..excess);
    }
}

async fn run_ui(app: &mut App, rx: &mut mpsc::Receiver<VisualEvent>) -> anyhow::Result<()> {
    use crossterm::event::{self, Event, KeyCode};

    let mut out = stdout();
    enable_raw_mode()?;
    execute!(out, Clear(ClearType::All), cursor::Hide)?;
    let mut terminal = Terminal::new(CrosstermBackend::new(out))?;

    let mut last = Instant::now();
    let mut last_rate = Instant::now();
    let result = loop {
        if event::poll(Duration::from_millis(1))?
            && let Event::Key(key) = event::read()?
            && matches!(key.code, KeyCode::Char('q') | KeyCode::Esc)
        {
            break Ok(());
        }

        let mut drained = 0;
        while let Ok(ev) = rx.try_recv() {
            handle_event(app, ev);
            drained += 1;
            if drained > 4096 {
                break;
            }
        }

        let dt = last.elapsed().as_secs_f32();
        last = Instant::now();
        update_balls(app, dt);
        if last_rate.elapsed() >= Duration::from_millis(500) {
            app.metrics.refresh_rates();
            last_rate = Instant::now();
        }

        terminal.draw(|f| draw_ui(f, app))?;
        tokio::time::sleep(Duration::from_millis(33)).await;
    };

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), cursor::Show, Clear(ClearType::All))?;
    terminal.show_cursor()?;
    result
}

// ===== Protocol driving =======================================================

fn parse_auth(auth: &str) -> Option<(String, String)> {
    auth.split_once(':')
        .map(|(u, p)| (u.to_string(), p.to_string()))
}

async fn connect(addr: &str) -> anyhow::Result<Conn> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(Framed::new(stream, ProtoCodec))
}

/// Handshake (hello + optional auth). Emits the setup frames for `client`.
async fn handshake(
    conn: &mut Conn,
    client: usize,
    auth: &Option<(String, String)>,
    vis: &mpsc::Sender<VisualEvent>,
) -> anyhow::Result<()> {
    let setup = |op: VizOp, to_broker: bool| VisualEvent {
        client,
        op,
        to_broker,
        partition: None,
        latency_ms: None,
    };

    conn.send(try_encode(
        Op::Hello,
        next_req_id(),
        &Hello {
            client_name: format!("tui-{client}"),
            client_version: "0.1".into(),
            protocol_version: PROTOCOL_V1,
            resume: None,
        },
    )?)
    .await?;
    let _ = vis.send(setup(VizOp::Hello, true)).await;
    expect(conn, Op::HelloOk).await?;
    let _ = vis.send(setup(VizOp::Hello, false)).await;

    if let Some((user, pass)) = auth {
        conn.send(try_encode(
            Op::Auth,
            next_req_id(),
            &Auth {
                username: user.clone(),
                password: pass.clone(),
            },
        )?)
        .await?;
        let _ = vis.send(setup(VizOp::Auth, true)).await;
        expect(conn, Op::AuthOk).await?;
        let _ = vis.send(setup(VizOp::Auth, false)).await;
    }
    Ok(())
}

/// Read frames until one with `op` arrives (answering pings on the way).
async fn expect(conn: &mut Conn, op: Op) -> anyhow::Result<()> {
    while let Some(frame) = conn.next().await {
        let frame = frame?;
        if frame.opcode == op as u16 {
            return Ok(());
        }
        if frame.opcode == Op::Ping as u16 {
            conn.send(try_encode(Op::Pong, frame.request_id, &Pong)?).await?;
            continue;
        }
        if frame.opcode == Op::Error as u16
            || frame.opcode == Op::HelloErr as u16
            || frame.opcode == Op::AuthErr as u16
        {
            let err: ErrorMsg = try_decode(&frame)?;
            anyhow::bail!("server error {} waiting for {:?}: {}", err.code, op, err.message);
        }
    }
    anyhow::bail!("connection closed waiting for {op:?}")
}

/// Declare the demo topic with the requested partition count (one-shot).
async fn declare_topic(args: &Args, vis: &mpsc::Sender<VisualEvent>) -> anyhow::Result<()> {
    let auth = parse_auth(&args.auth);
    let mut conn = connect(&args.addr).await?;
    handshake(&mut conn, 0, &auth, vis).await?;
    conn.send(try_encode(
        Op::DeclareQueue,
        next_req_id(),
        &DeclareQueue {
            topic: args.topic.clone(),
            group: args.group.clone(),
            dlq_policy: None,
            dlq_max_retries: None,
            partition_count: Some(args.partitions.max(1)),
            default_message_ttl_ms: None,
        },
    )?)
    .await?;
    let _ = vis
        .send(VisualEvent {
            client: 0,
            op: VizOp::Declare,
            to_broker: true,
            partition: None,
            latency_ms: None,
        })
        .await;
    expect(&mut conn, Op::DeclareQueueOk).await?;
    Ok(())
}

/// One client: handshake, subscribe to its assigned partitions, then publish at
/// the configured rate while consuming. Emits a VisualEvent per frame in/out.
async fn run_client(args: Args, client: usize, vis: mpsc::Sender<VisualEvent>) -> anyhow::Result<()> {
    let auth = parse_auth(&args.auth);
    let mut conn = connect(&args.addr).await?;
    handshake(&mut conn, client, &auth, &vis).await?;

    // Cover the partitions: client i owns partitions where p % clients == i.
    let owned: Vec<u32> = (0..args.partitions)
        .filter(|p| (*p as usize) % args.clients.max(1) == client)
        .collect();
    for &p in &owned {
        conn.send(try_encode(
            Op::Subscribe,
            next_req_id(),
            &Subscribe {
                topic: args.topic.clone(),
                partition: Partition::new(p),
                group: args.group.clone(),
                prefetch: 64,
                auto_ack: true,
                consumer_group: None,
                consumer_target: None,
                member_id: None,
            },
        )?)
        .await?;
        let _ = vis
            .send(VisualEvent {
                client,
                op: VizOp::Subscribe,
                to_broker: true,
                partition: Some(p),
                latency_ms: None,
            })
            .await;
        expect(&mut conn, Op::SubscribeOk).await?;
    }

    // Single task: publish on a steady ticker and read frames in one select, so
    // the same connection answers broker pings inline (no split, no lost pongs).
    let period = if args.rate > 0.0 {
        Duration::from_secs_f64(1.0 / args.rate)
    } else {
        Duration::from_millis(200)
    };
    let payload = vec![b'x'; args.payload_size];
    let key_space = (args.partitions as u64 * 3).max(1);
    let mut ticker = tokio::time::interval(period);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut seq: u64 = 0;

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let key = format!("key-{}", seq % key_space);
                let partition = match args.key_mode {
                    KeyMode::Keyed => (fnv1a(key.as_bytes()) % args.partitions.max(1) as u64) as u32,
                    KeyMode::RoundRobin => (seq % args.partitions.max(1) as u64) as u32,
                };
                seq += 1;
                let msg = Publish {
                    topic: args.topic.clone(),
                    group: args.group.clone(),
                    partition: Partition::new(partition),
                    require_confirm: args.confirm,
                    content_type: None,
                    headers: std::collections::HashMap::new(),
                    published: unix_millis(),
                    payload: payload.clone(),
                    partition_key: matches!(args.key_mode, KeyMode::Keyed)
                        .then(|| key.into_bytes()),
                    partitioning_version: 0,
                    ttl_ms: None,
                };
                conn.send(try_encode(Op::Publish, next_req_id(), &msg)?).await?;
                let _ = vis
                    .send(VisualEvent {
                        client,
                        op: VizOp::Publish,
                        to_broker: true,
                        partition: Some(partition),
                        latency_ms: None,
                    })
                    .await;
            }
            frame = conn.next() => {
                let Some(frame) = frame else { break };
                let frame = frame?;
                match frame.opcode {
                    x if x == Op::Deliver as u16 => {
                        let d: Deliver = try_decode(&frame)?;
                        let latency = unix_millis().saturating_sub(d.published);
                        let _ = vis
                            .send(VisualEvent {
                                client,
                                op: VizOp::Deliver,
                                to_broker: false,
                                partition: Some(d.partition.id()),
                                latency_ms: Some(latency),
                            })
                            .await;
                    }
                    x if x == Op::PublishOk as u16 => {
                        let _ = vis
                            .send(VisualEvent {
                                client,
                                op: VizOp::Confirm,
                                to_broker: false,
                                partition: None,
                                latency_ms: None,
                            })
                            .await;
                    }
                    x if x == Op::Ping as u16 => {
                        let _ = vis
                            .send(VisualEvent {
                                client,
                                op: VizOp::Ping,
                                to_broker: false,
                                partition: None,
                                latency_ms: None,
                            })
                            .await;
                        conn.send(try_encode(Op::Pong, frame.request_id, &Pong)?).await?;
                        let _ = vis
                            .send(VisualEvent {
                                client,
                                op: VizOp::Pong,
                                to_broker: true,
                                partition: None,
                                latency_ms: None,
                            })
                            .await;
                    }
                    x if x == Op::Error as u16 => {
                        let e: ErrorMsg = try_decode(&frame)?;
                        tracing::debug!("client {client} error {}: {}", e.code, e.message);
                        let _ = vis
                            .send(VisualEvent {
                                client,
                                op: VizOp::Error,
                                to_broker: false,
                                partition: None,
                                latency_ms: None,
                            })
                            .await;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let (tx, mut rx) = mpsc::channel(8192);

    // Declare the topic up front so the partitions exist before clients route to
    // them. Best-effort: a broker that rejects re-declare is fine.
    if let Err(err) = declare_topic(&args, &tx).await {
        eprintln!("fibril-tui: could not declare `{}`: {err}", args.topic);
        eprintln!("fibril-tui: is a broker listening at {}?", args.addr);
        return Err(err);
    }

    for client in 0..args.clients {
        let args = args.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            if let Err(err) = run_client(args, client, tx).await {
                tracing::debug!("client {client} ended: {err}");
            }
        });
    }
    drop(tx);

    let mut app = App {
        clients: args.clients,
        partitions: args.partitions.max(1),
        topic: args.topic.clone(),
        addr: args.addr.clone(),
        balls: Vec::new(),
        metrics: Metrics::default(),
    };

    run_ui(&mut app, &mut rx).await
}
