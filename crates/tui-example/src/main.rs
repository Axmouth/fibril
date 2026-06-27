//! Live terminal visualizer of real Fibril wire traffic against a running broker.
//!
//! It drives the protocol at the frame level (so the handshake, subscribe,
//! publish, confirm, deliver, ping/pong and errors are all visible) and animates
//! every frame as a moving dot between client nodes and the broker's partition
//! lanes. Runtime-configurable, with small illustrative defaults. Point it at a live
//! broker with `--addr` (e.g. the one-command cluster demo).

use std::collections::VecDeque;
use std::io::stdout;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use clap::{Parser, ValueEnum};
use crossterm::{
    cursor, execute,
    terminal::{Clear, ClearType, disable_raw_mode, enable_raw_mode},
};
use fibril_protocol::v1::{
    Ack, AssignmentChanged, Auth, DeclareQueue, DeliveryTag, Deliver, ErrorMsg, Hello, Nack, Op,
    PROTOCOL_V1, Partition, Pong, Publish, Subscribe, SubscribeOk, TopologyOk, TopologyRequest,
    TopologyUpdateAck,
    frame::{Frame, ProtoCodec},
    helper::{Conn, try_decode, try_encode},
};
use fibril_util::unix_millis;
use futures::{SinkExt, StreamExt, stream::select_all};
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

    /// Admin API address for live repartition (the `+` / `-` keys). Repartition
    /// is coordination-only, so this has an effect only against a `--ganglion`
    /// cluster. Defaults to the standard admin port on the bootstrap host.
    #[arg(long, default_value = "127.0.0.1:8081")]
    admin_addr: String,

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

    /// Join an exclusive consumer group (cohort) of this name: the broker divides
    /// the partitions across the clients, and a killed client's partitions move to
    /// a surviving peer. Without it, clients consume fixed partitions.
    #[arg(long)]
    consumer_group: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum KeyMode {
    /// Route by `hash(key) % partitions` (per-key ordering is visible).
    Keyed,
    /// Spread round-robin across partitions, no key.
    RoundRobin,
}

/// Live, lock-free demo controls shared between the UI (which mutates them on key
/// presses) and the client drivers (which read them each publish). Interactive
/// keys steer the running demo without restarting anything.
struct Control {
    /// Microseconds between publishes, per client.
    period_us: AtomicU64,
    confirm: AtomicBool,
    /// true = keyed routing, false = round-robin.
    keyed: AtomicBool,
    /// Per-client publish pause.
    paused: Vec<AtomicBool>,
    /// Per-client liveness: cleared by `k` (the client drops its connections),
    /// set by `r` (it reconnects and re-subscribes).
    alive: Vec<AtomicBool>,
    /// Per-client count of deliveries to nack (requeue) instead of ack next.
    nack_next: Vec<AtomicU64>,
}

fn rate_to_period_us(rate: f64) -> u64 {
    if rate <= 0.0 {
        200_000
    } else {
        (1_000_000.0 / rate).clamp(1_000.0, 2_000_000.0) as u64
    }
}

impl Control {
    fn new(rate: f64, confirm: bool, keyed: bool, clients: usize) -> Self {
        let clients = clients.max(1);
        Self {
            period_us: AtomicU64::new(rate_to_period_us(rate)),
            confirm: AtomicBool::new(confirm),
            keyed: AtomicBool::new(keyed),
            paused: (0..clients).map(|_| AtomicBool::new(false)).collect(),
            alive: (0..clients).map(|_| AtomicBool::new(true)).collect(),
            nack_next: (0..clients).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn period(&self) -> Duration {
        Duration::from_micros(self.period_us.load(Ordering::Relaxed).max(1))
    }

    fn rate_per_sec(&self) -> f64 {
        1_000_000.0 / self.period_us.load(Ordering::Relaxed).max(1) as f64
    }

    fn confirm(&self) -> bool {
        self.confirm.load(Ordering::Relaxed)
    }

    fn keyed(&self) -> bool {
        self.keyed.load(Ordering::Relaxed)
    }

    fn is_paused(&self, client: usize) -> bool {
        self.paused
            .get(client)
            .is_some_and(|p| p.load(Ordering::Relaxed))
    }

    fn toggle_pause(&self, client: usize) {
        if let Some(p) = self.paused.get(client) {
            p.fetch_xor(true, Ordering::Relaxed);
        }
    }

    fn toggle_confirm(&self) {
        self.confirm.fetch_xor(true, Ordering::Relaxed);
    }

    fn toggle_keyed(&self) {
        self.keyed.fetch_xor(true, Ordering::Relaxed);
    }

    fn paused_count(&self) -> usize {
        self.paused
            .iter()
            .filter(|p| p.load(Ordering::Relaxed))
            .count()
    }

    fn is_alive(&self, client: usize) -> bool {
        self.alive.get(client).is_none_or(|a| a.load(Ordering::Relaxed))
    }

    fn set_alive(&self, client: usize, alive: bool) {
        if let Some(a) = self.alive.get(client) {
            a.store(alive, Ordering::Relaxed);
        }
    }

    /// Request that the client nack (requeue) its next delivery.
    fn request_nack(&self, client: usize) {
        if let Some(n) = self.nack_next.get(client) {
            n.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Consume one pending nack request for the client, returning whether the
    /// next delivery should be nacked rather than acked.
    fn take_nack(&self, client: usize) -> bool {
        let Some(n) = self.nack_next.get(client) else {
            return false;
        };
        n.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1))
            .is_ok()
    }

    /// Scale the publish rate. `factor > 1` speeds up (shorter period).
    fn scale_rate(&self, factor: f64) {
        let current = self.period_us.load(Ordering::Relaxed).max(1) as f64;
        let next = (current / factor).clamp(1_000.0, 2_000_000.0) as u64;
        self.period_us.store(next, Ordering::Relaxed);
    }
}

/// Best-effort visual emit: never block the protocol on the UI draining events.
fn emit(vis: &mpsc::Sender<VisualEvent>, ev: VisualEvent) {
    let _ = vis.try_send(ev);
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
    Ack,
    Nack,
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
            VizOp::Ack => Color::LightGreen,
            VizOp::Nack => Color::LightRed,
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
    /// Client index the interactive keys act on.
    focus: usize,
    control: Arc<Control>,
    /// partition -> owning broker index, for coloring lanes by owner.
    partition_owner: Vec<usize>,
    /// Distinct brokers, so single-broker (standalone) skips owner coloring.
    broker_count: usize,
    /// Exclusive consumer group name, when running in cohort mode.
    cohort: Option<String>,
    /// partition -> client currently assigned it, in cohort mode. Updated from
    /// the broker's `AssignmentChanged` pushes so failover is visible on screen.
    partition_client: Vec<Option<usize>>,
    /// Topology generation the UI has applied. Bumps on a live repartition, which
    /// the UI then mirrors into the partition count, lanes, and ownership.
    topology_gen: u64,
    /// Whether a repartition request is currently in flight (for the HUD).
    repartitioning: bool,
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
        let focused = i == app.focus;
        let down = !app.control.is_alive(i);
        let paused = app.control.is_paused(i);
        let color = if down {
            Color::Red
        } else if focused {
            Color::White
        } else if paused {
            Color::DarkGray
        } else {
            Color::Gray
        };
        let label = format!(
            "client {i}{}{}{}",
            if focused { " *" } else { "" },
            if down { " down" } else { "" },
            if paused { " ||" } else { "" }
        );
        draw_box(f, *rect, &label, color);
    }
    for (p, rect) in layout.lane_boxes.iter().enumerate() {
        // In a cluster, color each partition lane by its owning broker so the
        // ownership spread is visible. A single broker stays uniform.
        let (color, mut label) = if app.broker_count > 1 {
            let owner = app.partition_owner.get(p).copied().unwrap_or(0);
            (broker_color(owner), format!("part {p} @b{owner}"))
        } else {
            (Color::Yellow, format!("part {p}"))
        };
        // In cohort mode, append the client currently assigned this partition, so
        // the per-partition exclusive ownership (and its failover) is visible.
        if app.cohort.is_some() {
            match app.partition_client.get(p).copied().flatten() {
                Some(c) => label.push_str(&format!(" c{c}")),
                None => label.push_str(" c-"),
            }
        }
        draw_box(f, *rect, &label, color);
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
                "{}  clients {}  brokers {}  partitions {}{}  topic {}{}",
                app.addr,
                app.clients,
                app.broker_count,
                app.partitions,
                if app.repartitioning { " (repartitioning...)" } else { "" },
                app.topic,
                match &app.cohort {
                    Some(g) => format!("  cohort {g}"),
                    None => String::new(),
                },
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
        Line::from(format!(
            "rate {:.0}/s   confirm {}   routing {}   focus client {}   paused {}/{}",
            app.control.rate_per_sec(),
            if app.control.confirm() { "on" } else { "off" },
            if app.control.keyed() { "keyed" } else { "round-robin" },
            app.focus,
            app.control.paused_count(),
            app.clients,
        )),
        Line::from(Span::styled(
            "publish=blue confirm=cyan deliver=green ack=lgreen nack=lred setup/ping=gray error=red",
            Style::default().fg(Color::DarkGray),
        )),
        Line::from(Span::styled(
            "Tab focus  space pause  k/r kill/restart  n nack  [ ] rate  c confirm  g routing  +/- partitions  q quit",
            Style::default().fg(Color::DarkGray),
        )),
    ];
    f.render_widget(
        Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title("status")),
        area,
    );
}

/// A stable color per broker index, for tinting partition lanes by owner.
fn broker_color(broker: usize) -> Color {
    const PALETTE: [Color; 6] = [
        Color::Yellow,
        Color::Magenta,
        Color::Cyan,
        Color::LightGreen,
        Color::LightRed,
        Color::LightBlue,
    ];
    PALETTE[broker % PALETTE.len()]
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

/// Upper bound on the lanes a live repartition will grow to, so the demo stays
/// readable and does not march off the screen.
const MAX_PARTITIONS: u32 = 32;

/// Kick off a live repartition in the background: double (grow) or halve (shrink)
/// the partition count via the admin API. The broker then pushes the new topology
/// to the sessions, which apply it (restarting against the new layout) and ack the
/// cutover. A no-op while one is already in flight, or when growing or shrinking
/// would not change the count.
fn trigger_repartition(app: &App, args: &Args, topo: &Arc<Topology>, grow: bool) {
    if topo.repartitioning.load(Ordering::Relaxed) {
        return;
    }
    let current = app.partitions.max(1);
    let target = if grow {
        (current * 2).min(MAX_PARTITIONS)
    } else {
        (current / 2).max(1)
    };
    if target == current {
        return;
    }
    topo.repartitioning.store(true, Ordering::Relaxed);
    let args = args.clone();
    let topo = topo.clone();
    tokio::spawn(async move {
        if let Err(err) =
            admin_repartition(&args.admin_addr, &args.topic, &args.group, target).await
        {
            tracing::debug!("repartition to {target} failed: {err}");
        }
        // Leave the in-flight flag set briefly so the HUD shows the cutover while
        // the broker pushes the new topology and the sessions apply and ack it.
        tokio::time::sleep(Duration::from_millis(1200)).await;
        topo.repartitioning.store(false, Ordering::Relaxed);
    });
}

async fn run_ui(
    app: &mut App,
    args: &Args,
    topo: &Arc<Topology>,
    rx: &mut mpsc::Receiver<VisualEvent>,
    assign_rx: &mut mpsc::Receiver<(usize, Vec<u32>)>,
) -> anyhow::Result<()> {
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
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => break Ok(()),
                KeyCode::Tab => app.focus = (app.focus + 1) % app.clients.max(1),
                KeyCode::Char(' ') => app.control.toggle_pause(app.focus),
                // `]` speeds up (shorter period), `[` slows down.
                KeyCode::Char(']') => app.control.scale_rate(1.3),
                KeyCode::Char('[') => app.control.scale_rate(1.0 / 1.3),
                KeyCode::Char('c') => app.control.toggle_confirm(),
                KeyCode::Char('g') => app.control.toggle_keyed(),
                KeyCode::Char('k') => app.control.set_alive(app.focus, false),
                KeyCode::Char('r') => app.control.set_alive(app.focus, true),
                KeyCode::Char('n') => app.control.request_nack(app.focus),
                // `+` doubles the partition count, `-` halves it (the repartition
                // API grows to a multiple and shrinks to a factor). Accept the
                // unshifted `=` / `_` too. Coordination-only: a no-op standalone.
                KeyCode::Char('+') | KeyCode::Char('=') => {
                    trigger_repartition(app, args, topo, true)
                }
                KeyCode::Char('-') | KeyCode::Char('_') => {
                    trigger_repartition(app, args, topo, false)
                }
                _ => {}
            }
        }

        let mut drained = 0;
        while let Ok(ev) = rx.try_recv() {
            handle_event(app, ev);
            drained += 1;
            if drained > 4096 {
                break;
            }
        }

        // Apply cohort assignment pushes: clear the client's old lanes, then claim
        // its current set, so a killed member's partitions visibly move to a peer.
        while let Ok((client, assigned)) = assign_rx.try_recv() {
            for slot in app.partition_client.iter_mut() {
                if *slot == Some(client) {
                    *slot = None;
                }
            }
            for p in assigned {
                if let Some(slot) = app.partition_client.get_mut(p as usize) {
                    *slot = Some(client);
                }
            }
        }

        // Mirror a live repartition into the UI: pick up the new partition count,
        // ownership, and lane set when the topology generation has advanced.
        app.repartitioning = topo.repartitioning.load(Ordering::Relaxed);
        if topo.generation() != app.topology_gen {
            let snap = topo.snapshot();
            app.partitions = snap.cluster.partitions() as u32;
            app.broker_count = snap.cluster.brokers.len();
            app.partition_owner = snap.cluster.partition_owner.clone();
            // Reset cohort ownership: members re-push their assignments after they
            // resubscribe against the new layout.
            app.partition_client = vec![None; snap.cluster.partitions()];
            app.topology_gen = snap.generation;
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

/// Send a subscribe and return the `SubscribeOk` (answering pings on the way), so
/// the caller can read the server-minted cohort member id to reuse on the rest of
/// an exclusive member's subscribes.
async fn subscribe(conn: &mut Conn, sub: &Subscribe) -> anyhow::Result<SubscribeOk> {
    conn.send(try_encode(Op::Subscribe, next_req_id(), sub)?).await?;
    while let Some(frame) = conn.next().await {
        let frame = frame?;
        match frame.opcode {
            x if x == Op::SubscribeOk as u16 => return Ok(try_decode(&frame)?),
            x if x == Op::Ping as u16 => {
                conn.send(try_encode(Op::Pong, frame.request_id, &Pong)?).await?;
            }
            x if x == Op::Error as u16 || x == Op::SubscribeErr as u16 => {
                let err: ErrorMsg = try_decode(&frame)?;
                anyhow::bail!("subscribe rejected {}: {}", err.code, err.message);
            }
            _ => {}
        }
    }
    anyhow::bail!("connection closed during subscribe")
}

/// The brokers to talk to and which one owns each partition.
#[derive(Clone)]
struct Cluster {
    /// Broker endpoints. `brokers[0]` is the bootstrap address.
    brokers: Vec<String>,
    /// partition -> index into `brokers`. Defaults to 0 (bootstrap) for a
    /// partition whose owner is unknown (e.g. standalone, where queue
    /// partitioning is not carried in the client topology).
    partition_owner: Vec<usize>,
}

impl Cluster {
    fn partitions(&self) -> usize {
        self.partition_owner.len().max(1)
    }
}

/// A cluster snapshot paired with the generation it was taken at, so a session can
/// detect when a live repartition has superseded it.
struct ClusterSnapshot {
    cluster: Cluster,
    generation: u64,
}

/// Shared, live view of the cluster, so a live repartition can swap in a new
/// partition count and ownership while clients are running. `generation` bumps on
/// every swap; sessions capture the generation they started with and return when
/// it changes, so their supervisor reconnects against the fresh topology.
struct Topology {
    cluster: std::sync::RwLock<Cluster>,
    /// Internal swap counter: bumps on every applied change so sessions restart.
    generation: AtomicU64,
    /// Highest coordination generation applied from a broker `TopologyUpdate`
    /// push, so concurrent sessions apply each push exactly once.
    last_coord_gen: AtomicU64,
    /// Set while a repartition request is in flight, for the HUD.
    repartitioning: AtomicBool,
}

impl Topology {
    fn new(cluster: Cluster, coord_gen: u64) -> Self {
        Self {
            cluster: std::sync::RwLock::new(cluster),
            generation: AtomicU64::new(0),
            last_coord_gen: AtomicU64::new(coord_gen),
            repartitioning: AtomicBool::new(false),
        }
    }

    /// A consistent cluster snapshot plus its generation, both read under the lock
    /// so they cannot tear against a concurrent swap. Recovers a poisoned lock
    /// rather than panicking.
    fn snapshot(&self) -> ClusterSnapshot {
        let cluster = self.cluster.read().unwrap_or_else(|p| p.into_inner());
        ClusterSnapshot {
            cluster: cluster.clone(),
            generation: self.generation.load(Ordering::Relaxed),
        }
    }

    /// Current generation, for the cheap per-tick staleness check.
    fn generation(&self) -> u64 {
        self.generation.load(Ordering::Relaxed)
    }

    /// Install a new cluster and bump the generation, so running sessions restart.
    /// The bump happens under the write lock, so `snapshot` never pairs a new
    /// generation with the old cluster.
    fn swap(&self, cluster: Cluster) {
        let mut guard = self.cluster.write().unwrap_or_else(|p| p.into_inner());
        *guard = cluster;
        self.generation.fetch_add(1, Ordering::Relaxed);
    }

    /// Apply a broker-pushed topology at coordination generation `coord_gen`, but
    /// only once across all sessions: the first to see a newer generation swaps in
    /// the new cluster (which restarts every session against it). Returns whether
    /// this call performed the swap. Acking the push is the caller's job and
    /// happens regardless, so the broker can finalize the cutover fence.
    fn apply_pushed(&self, cluster: Cluster, coord_gen: u64) -> bool {
        let prev = self.last_coord_gen.fetch_max(coord_gen, Ordering::Relaxed);
        if coord_gen <= prev {
            return false;
        }
        self.swap(cluster);
        true
    }
}

/// Read frames until a `TopologyOk`, answering pings on the way.
async fn recv_topology(conn: &mut Conn) -> anyhow::Result<TopologyOk> {
    while let Some(frame) = conn.next().await {
        let frame = frame?;
        if frame.opcode == Op::TopologyOk as u16 {
            return Ok(try_decode(&frame)?);
        }
        if frame.opcode == Op::Ping as u16 {
            conn.send(try_encode(Op::Pong, frame.request_id, &Pong)?).await?;
        }
    }
    anyhow::bail!("connection closed waiting for topology")
}

/// Declare the demo topic and learn which broker owns each partition, so clients
/// can route publishes and subscribes to the owner. In a cluster the controller
/// assigns owners shortly after declare, so poll briefly until every partition
/// has one. Standalone brokers do not carry queue ownership in the topology, so
/// owners stay unknown and default to the bootstrap broker (which owns all).
async fn discover(args: &Args, vis: &mpsc::Sender<VisualEvent>) -> anyhow::Result<(Cluster, u64)> {
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
    emit(
        vis,
        VisualEvent {
            client: 0,
            op: VizOp::Declare,
            to_broker: true,
            partition: None,
            latency_ms: None,
        },
    );
    expect(&mut conn, Op::DeclareQueueOk).await?;

    let (owners, generation) = poll_owners(&mut conn, args, args.partitions.max(1) as usize).await?;
    Ok((build_cluster(args, &owners), generation))
}

/// Poll the topology until every partition in `0..parts` has a known owner (or a
/// brief timeout elapses), returning the owner endpoint per partition along with
/// the coordination generation last seen (so pushes at the same generation are
/// not reapplied).
async fn poll_owners(
    conn: &mut Conn,
    args: &Args,
    parts: usize,
) -> anyhow::Result<(Vec<Option<String>>, u64)> {
    let mut owners: Vec<Option<String>> = vec![None; parts];
    let mut generation = 0;
    for _ in 0..20 {
        conn.send(try_encode(
            Op::Topology,
            next_req_id(),
            &TopologyRequest {
                topic: Some(args.topic.clone()),
                group: args.group.clone(),
            },
        )?)
        .await?;
        let topo = recv_topology(conn).await?;
        generation = topo.generation;
        for q in &topo.queues {
            if q.topic == args.topic
                && q.group == args.group
                && let (Some(ep), Some(slot)) =
                    (q.owner_endpoint.as_ref(), owners.get_mut(q.partition.id() as usize))
            {
                *slot = Some(ep.clone());
            }
        }
        if owners.iter().all(Option::is_some) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
    }
    Ok((owners, generation))
}

/// Turn per-partition owner endpoints into a `Cluster`: the bootstrap address is
/// broker 0, and each distinct owner endpoint gets the next broker index.
fn build_cluster(args: &Args, owners: &[Option<String>]) -> Cluster {
    let mut brokers = vec![args.addr.clone()];
    let mut partition_owner = vec![0usize; owners.len()];
    for (p, owner) in owners.iter().enumerate() {
        if let Some(ep) = owner {
            let idx = brokers.iter().position(|b| b == ep).unwrap_or_else(|| {
                brokers.push(ep.clone());
                brokers.len() - 1
            });
            partition_owner[p] = idx;
        }
    }
    Cluster {
        brokers,
        partition_owner,
    }
}

/// Build a `Cluster` from a broker-pushed topology, sizing the partition set to
/// the highest partition id present for the topic so a grow or shrink is picked up.
fn cluster_from_topology(args: &Args, topology: &TopologyOk) -> Cluster {
    let count = topology
        .queues
        .iter()
        .filter(|q| q.topic == args.topic && q.group == args.group)
        .map(|q| q.partition.id() as usize + 1)
        .max()
        .unwrap_or(1);
    let mut owners: Vec<Option<String>> = vec![None; count];
    for q in &topology.queues {
        if q.topic == args.topic
            && q.group == args.group
            && let Some(slot) = owners.get_mut(q.partition.id() as usize)
        {
            *slot = q.owner_endpoint.clone();
        }
    }
    build_cluster(args, &owners)
}

/// Minimal HTTP POST to the admin repartition endpoint. Kept dependency-free (a
/// hand-written request over a raw socket) so the visualizer does not pull in an
/// HTTP client. Assumes the demo default of admin auth disabled.
async fn admin_repartition(
    admin_addr: &str,
    topic: &str,
    group: &Option<String>,
    partition_count: u32,
) -> anyhow::Result<()> {
    let group_field = match group {
        Some(g) => format!(",\"group\":\"{}\"", json_escape(g)),
        None => String::new(),
    };
    let body = format!(
        "{{\"topic\":\"{}\"{},\"partition_count\":{}}}",
        json_escape(topic),
        group_field,
        partition_count
    );
    let request = format!(
        "POST /admin/api/repartition HTTP/1.1\r\nHost: {admin_addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    let mut stream = TcpStream::connect(admin_addr).await?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    stream.write_all(request.as_bytes()).await?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await?;
    let head = String::from_utf8_lossy(&response);
    let status = head.lines().next().unwrap_or_default();
    if status.contains(" 200") {
        Ok(())
    } else {
        anyhow::bail!("admin repartition rejected: {}", status.trim())
    }
}

/// Escape a string for embedding in a JSON string literal (the few cases that
/// matter for a topic or group name).
fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Hold each delivery this long before acking, so there is always a small window
/// of unacked in-flight to redeliver when a consumer is killed.
const ACK_DELAY: Duration = Duration::from_millis(450);

/// Supervise one client: run a session while the client is alive, and reconnect
/// after `r` brings it back (or after an error). On `k` the session returns and
/// the connections drop, so the broker reclaims the client's unacked in-flight
/// and redelivers it when the client reconnects.
async fn run_client(
    args: Args,
    client: usize,
    topo: Arc<Topology>,
    control: Arc<Control>,
    vis: mpsc::Sender<VisualEvent>,
    assign: mpsc::Sender<(usize, Vec<u32>)>,
) -> anyhow::Result<()> {
    loop {
        if !control.is_alive(client) {
            tokio::time::sleep(Duration::from_millis(150)).await;
            continue;
        }
        // Snapshot the topology this session runs against. The session returns
        // when the generation bumps (a live repartition), so the next pass picks
        // up the new partition count and ownership.
        let snap = topo.snapshot();
        if let Err(err) = run_session(&args, client, &snap, &topo, &control, &vis, &assign).await {
            tracing::debug!("client {client} session ended: {err}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// One session: connect to every broker, subscribe (manual ack) to this client's
/// owned partitions on each partition's owner, then publish (routed to the owner)
/// while consuming. Deliveries are acked after a short delay (so the ack return
/// path is visible and there is unacked in-flight to redeliver on a kill), unless
/// a nack was requested. Returns when the client is killed or a connection fails.
async fn run_session(
    args: &Args,
    client: usize,
    snap: &ClusterSnapshot,
    topo: &Topology,
    control: &Control,
    vis: &mpsc::Sender<VisualEvent>,
    assign: &mpsc::Sender<(usize, Vec<u32>)>,
) -> anyhow::Result<()> {
    let auth = parse_auth(&args.auth);
    let cluster = &snap.cluster;
    // Partition count comes from the snapshot, not the CLI, so a live repartition
    // grows or shrinks the lanes this session drives.
    let part_count = cluster.partitions() as u32;

    // Connect to every broker (bootstrap plus each partition owner) and handshake,
    // so a publish or subscribe can be routed to the partition's owner.
    let mut conns: Vec<Conn> = Vec::with_capacity(cluster.brokers.len());
    for endpoint in &cluster.brokers {
        let mut conn = connect(endpoint).await?;
        handshake(&mut conn, client, &auth, vis).await?;
        conns.push(conn);
    }

    // Report topology adoption for these fresh connections so a repartition
    // cutover fence is satisfied by client adoption, not just the 30s timeout.
    // The broker pushes a TopologyUpdate only on a change, and a reconnect seeds
    // at the current topology, so a session that reconnected after a cutover would
    // otherwise never ack the post-cutover generation. Ask for the current
    // generation (no deliveries are flowing yet) and ack it on every connection.
    {
        conns[0]
            .send(try_encode(
                Op::Topology,
                next_req_id(),
                &TopologyRequest {
                    topic: Some(args.topic.clone()),
                    group: args.group.clone(),
                },
            )?)
            .await?;
        let coord_gen = recv_topology(&mut conns[0]).await?.generation;
        for conn in conns.iter_mut() {
            conn.send(try_encode(
                Op::TopologyUpdateAck,
                next_req_id(),
                &TopologyUpdateAck {
                    generation: coord_gen,
                },
            )?)
            .await?;
        }
    }

    let owner_of = |partition: u32| -> usize {
        cluster
            .partition_owner
            .get(partition as usize)
            .copied()
            .unwrap_or(0)
    };

    if let Some(group_name) = &args.consumer_group {
        // Exclusive cohort: every client subscribes to EVERY partition (on its
        // owner) with the same cohort id, and the broker's per-partition gate
        // delivers each partition to exactly one member. Killing a member moves
        // its partitions to a surviving subscriber. The first subscribe mints a
        // cluster-scoped member id; reuse it so every broker sees one member.
        let mut member_id = None;
        for p in 0..part_count {
            let ok = subscribe(
                &mut conns[owner_of(p)],
                &Subscribe {
                    topic: args.topic.clone(),
                    partition: Partition::new(p),
                    group: args.group.clone(),
                    prefetch: 64,
                    auto_ack: false,
                    consumer_group: Some(group_name.clone()),
                    consumer_target: None,
                    member_id,
                },
            )
            .await?;
            if member_id.is_none() {
                member_id = ok.member_id;
            }
            emit(
                vis,
                VisualEvent {
                    client,
                    op: VizOp::Subscribe,
                    to_broker: true,
                    partition: Some(p),
                    latency_ms: None,
                },
            );
        }
    } else {
        // Competing consumers: client i owns partitions where p % clients == i,
        // and subscribes to each on that partition's owner connection.
        let owned: Vec<u32> = (0..part_count)
            .filter(|p| (*p as usize) % args.clients.max(1) == client)
            .collect();
        for &p in &owned {
            subscribe(
                &mut conns[owner_of(p)],
                &Subscribe {
                    topic: args.topic.clone(),
                    partition: Partition::new(p),
                    group: args.group.clone(),
                    prefetch: 64,
                    auto_ack: false,
                    consumer_group: None,
                    consumer_target: None,
                    member_id: None,
                },
            )
            .await?;
            emit(
                vis,
                VisualEvent {
                    client,
                    op: VizOp::Subscribe,
                    to_broker: true,
                    partition: Some(p),
                    latency_ms: None,
                },
            );
        }
    }

    // Split each connection: sinks stay indexed by broker for routed sends, and
    // the read halves merge into one tagged stream so any broker's frames are
    // handled in the same select loop (and answered on the right connection).
    let mut sinks: Vec<futures::stream::SplitSink<Conn, Frame>> = Vec::new();
    let mut reads = Vec::new();
    for (idx, conn) in conns.into_iter().enumerate() {
        let (sink, stream) = conn.split();
        sinks.push(sink);
        reads.push(stream.map(move |r| (idx, r)).boxed());
    }
    let mut reads = select_all(reads);

    let payload = vec![b'x'; args.payload_size];
    let parts = part_count.max(1) as u64;
    let key_space = (parts * 3).max(1);
    let mut seq: u64 = 0;
    let mut next_pub = Instant::now();
    // Deliveries awaiting their delayed ack: (deadline, broker, partition, tag).
    let mut pending_acks: VecDeque<(Instant, usize, Partition, DeliveryTag)> = VecDeque::new();
    // Stateful so a busy loop cannot starve the kill check.
    let mut alive_check = tokio::time::interval(Duration::from_millis(150));
    alive_check.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let ack_deadline = pending_acks.front().map(|(d, ..)| *d);
        tokio::select! {
            _ = alive_check.tick() => {
                if !control.is_alive(client) {
                    // Drop the connections: the broker reclaims the unacked
                    // in-flight and redelivers it when this client reconnects.
                    return Ok(());
                }
                if topo.generation() != snap.generation {
                    // A live repartition swapped the topology: end this session so
                    // the supervisor reconnects against the new partition layout.
                    return Ok(());
                }
            }
            _ = tokio::time::sleep_until(next_pub.into()) => {
                next_pub = Instant::now() + control.period();
                if control.is_paused(client) {
                    continue;
                }
                let key = format!("key-{}", seq % key_space);
                let partition = if control.keyed() {
                    (fnv1a(key.as_bytes()) % parts) as u32
                } else {
                    (seq % parts) as u32
                };
                seq += 1;
                let msg = Publish {
                    topic: args.topic.clone(),
                    group: args.group.clone(),
                    partition: Partition::new(partition),
                    require_confirm: control.confirm(),
                    content_type: None,
                    headers: std::collections::HashMap::new(),
                    published: unix_millis(),
                    payload: payload.clone(),
                    partition_key: control.keyed().then(|| key.into_bytes()),
                    partitioning_version: 0,
                    ttl_ms: None,
                };
                sinks[owner_of(partition)]
                    .send(try_encode(Op::Publish, next_req_id(), &msg)?)
                    .await?;
                emit(
                    vis,
                    VisualEvent {
                        client,
                        op: VizOp::Publish,
                        to_broker: true,
                        partition: Some(partition),
                        latency_ms: None,
                    },
                );
            }
            _ = async { match ack_deadline {
                Some(d) => tokio::time::sleep_until(d.into()).await,
                None => std::future::pending().await,
            } } => {
                let now = Instant::now();
                while let Some(&(deadline, broker, partition, tag)) = pending_acks.front() {
                    if deadline > now {
                        break;
                    }
                    pending_acks.pop_front();
                    sinks[broker]
                        .send(try_encode(Op::Ack, next_req_id(), &Ack {
                            topic: args.topic.clone(),
                            group: args.group.clone(),
                            partition,
                            tags: vec![tag],
                        })?)
                        .await?;
                    emit(
                        vis,
                        VisualEvent {
                            client,
                            op: VizOp::Ack,
                            to_broker: true,
                            partition: Some(partition.id()),
                            latency_ms: None,
                        },
                    );
                }
            }
            Some((broker, frame)) = reads.next() => {
                let frame = frame?;
                match frame.opcode {
                    x if x == Op::Deliver as u16 => {
                        let d: Deliver = try_decode(&frame)?;
                        let latency = unix_millis().saturating_sub(d.published);
                        emit(
                            vis,
                            VisualEvent {
                                client,
                                op: VizOp::Deliver,
                                to_broker: false,
                                partition: Some(d.partition.id()),
                                latency_ms: Some(latency),
                            },
                        );
                        if control.take_nack(client) {
                            // Requeue now -> the broker redelivers it.
                            sinks[broker]
                                .send(try_encode(Op::Nack, next_req_id(), &Nack {
                                    topic: args.topic.clone(),
                                    group: args.group.clone(),
                                    partition: d.partition,
                                    tags: vec![d.delivery_tag],
                                    requeue: true,
                                    not_before: None,
                                })?)
                                .await?;
                            emit(
                                vis,
                                VisualEvent {
                                    client,
                                    op: VizOp::Nack,
                                    to_broker: true,
                                    partition: Some(d.partition.id()),
                                    latency_ms: None,
                                },
                            );
                        } else {
                            pending_acks.push_back((
                                Instant::now() + ACK_DELAY,
                                broker,
                                d.partition,
                                d.delivery_tag,
                            ));
                        }
                    }
                    x if x == Op::PublishOk as u16 => {
                        emit(
                            vis,
                            VisualEvent {
                                client,
                                op: VizOp::Confirm,
                                to_broker: false,
                                partition: None,
                                latency_ms: None,
                            },
                        );
                    }
                    x if x == Op::Ping as u16 => {
                        // Answer on the broker that pinged, then emit the visuals.
                        sinks[broker]
                            .send(try_encode(Op::Pong, frame.request_id, &Pong)?)
                            .await?;
                        emit(
                            vis,
                            VisualEvent {
                                client,
                                op: VizOp::Ping,
                                to_broker: false,
                                partition: None,
                                latency_ms: None,
                            },
                        );
                        emit(
                            vis,
                            VisualEvent {
                                client,
                                op: VizOp::Pong,
                                to_broker: true,
                                partition: None,
                                latency_ms: None,
                            },
                        );
                    }
                    x if x == Op::AssignmentChanged as u16 => {
                        // Cohort rebalance: the broker pushes this member's full
                        // current partition set. Forward it so the UI can show
                        // which client owns each lane (and watch failover move them).
                        let a: AssignmentChanged = try_decode(&frame)?;
                        let assigned = a.assigned.iter().map(|p| p.id()).collect();
                        let _ = assign.try_send((client, assigned));
                    }
                    x if x == Op::TopologyUpdate as u16 => {
                        // Broker-pushed routing refresh (a live repartition). Apply
                        // it once across sessions so the lanes and ownership follow
                        // the new layout, then ack the generation so the broker can
                        // finalize the cutover fence. Without this ack the
                        // repartition would stay pending forever.
                        let pushed: TopologyOk = try_decode(&frame)?;
                        let coord_gen = pushed.generation;
                        topo.apply_pushed(cluster_from_topology(args, &pushed), coord_gen);
                        sinks[broker]
                            .send(try_encode(
                                Op::TopologyUpdateAck,
                                frame.request_id,
                                &TopologyUpdateAck {
                                    generation: coord_gen,
                                },
                            )?)
                            .await?;
                    }
                    x if x == Op::Error as u16 => {
                        let e: ErrorMsg = try_decode(&frame)?;
                        tracing::debug!("client {client} error {}: {}", e.code, e.message);
                        emit(
                            vis,
                            VisualEvent {
                                client,
                                op: VizOp::Error,
                                to_broker: false,
                                partition: None,
                                latency_ms: None,
                            },
                        );
                    }
                    _ => {}
                }
            }
            else => break,
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let (tx, mut rx) = mpsc::channel(8192);
    // Separate channel for cohort assignment pushes (client -> its full current
    // partition set), so the UI can show which client owns each partition.
    let (assign_tx, mut assign_rx) = mpsc::channel::<(usize, Vec<u32>)>(256);

    // Declare the topic and learn which broker owns each partition, so clients can
    // route to owners. Falls back to the bootstrap broker for unknown owners.
    let (cluster, coord_gen) = match discover(&args, &tx).await {
        Ok(found) => found,
        Err(err) => {
            eprintln!("fibril-tui: could not set up `{}`: {err}", args.topic);
            eprintln!("fibril-tui: is a broker listening at {}?", args.addr);
            return Err(err);
        }
    };

    let control = Arc::new(Control::new(
        args.rate,
        args.confirm,
        matches!(args.key_mode, KeyMode::Keyed),
        args.clients,
    ));

    let init_partitions = cluster.partitions() as u32;
    let init_partition_owner = cluster.partition_owner.clone();
    let init_broker_count = cluster.brokers.len();
    let topo = Arc::new(Topology::new(cluster, coord_gen));

    for client in 0..args.clients {
        let args = args.clone();
        let tx = tx.clone();
        let assign_tx = assign_tx.clone();
        let control = control.clone();
        let topo = topo.clone();
        tokio::spawn(async move {
            if let Err(err) = run_client(args, client, topo, control, tx, assign_tx).await {
                tracing::debug!("client {client} ended: {err}");
            }
        });
    }
    drop(assign_tx);
    drop(tx);

    let mut app = App {
        clients: args.clients,
        partitions: init_partitions,
        topic: args.topic.clone(),
        addr: args.addr.clone(),
        balls: Vec::new(),
        metrics: Metrics::default(),
        focus: 0,
        control,
        partition_owner: init_partition_owner,
        broker_count: init_broker_count,
        cohort: args.consumer_group.clone(),
        partition_client: vec![None; init_partitions as usize],
        topology_gen: topo.generation.load(Ordering::Relaxed),
        repartitioning: false,
    };

    run_ui(&mut app, &args, &topo, &mut rx, &mut assign_rx).await
}
