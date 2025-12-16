use crossterm::{
    cursor,
    event::{self, Event, KeyCode},
    execute,
    terminal::{Clear, ClearType, disable_raw_mode},
};
use fibril_protocol::v1::{
    Deliver, ErrorMsg, Hello, HelloOk, Op, PROTOCOL_V1, Publish, Subscribe, SubscribeOk,
    frame::ProtoCodec, helper::Conn,
};
use futures::{SinkExt, StreamExt};
use ratatui::{
    Terminal,
    backend::CrosstermBackend,
    layout::Rect,
    style::Color,
    widgets::{Block, Borders, Paragraph},
};
use std::io::stdout;
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};
use tokio::{net::TcpStream, sync::mpsc};
use tokio_util::codec::Framed;

#[derive(Debug)]
pub enum VisualEvent {
    Hello { pub_id: usize },
    HelloOk { sub_id: usize },
    Subscribe { sub_id: usize },
    SubscribeOk { sub_id: usize },
    Publish { pub_id: usize, topic: String },
    Deliver { sub_id: usize, offset: u64 },
    ErrorMsg { sub_id: usize, code: u16 },
}

#[derive(Clone)]
struct Node {
    id: usize,
    label: String,
    x: u16,
    y: u16,
}

struct Link {
    from: usize,
    to: usize,
}

struct InFlight {
    from: usize,
    to: usize,
    progress: f32,
    color: Color,
}

struct App {
    pubs: Vec<Node>,
    subs: Vec<Node>,
    broker: Node,
    links: Vec<Link>,
    inflight: Vec<InFlight>,
}

static REQ: AtomicU64 = AtomicU64::new(1);

fn next_req_id() -> u64 {
    REQ.fetch_add(1, Ordering::Relaxed)
}

fn random_idle_duration() -> Duration {
    // 200ms – 3s idle
    Duration::from_millis(fastrand::u64(200..3000))
}

fn random_burst_size() -> usize {
    // 1–8 messages per burst
    fastrand::usize(1..=8)
}

fn random_inter_message_delay() -> Duration {
    // 20–200ms between messages in a burst
    Duration::from_millis(fastrand::u64(20..200))
}

fn init_app() -> App {
    let pubs = (0..3)
        .map(|i| Node {
            id: i,
            label: format!("PUB {}", i),
            x: 2,
            y: 2 + i as u16 * 4,
        })
        .collect();

    let subs = (0..3)
        .map(|i| Node {
            id: 100 + i,
            label: format!("SUB {}", i),
            x: 60,
            y: 2 + i as u16 * 4,
        })
        .collect();

    let mut links = Vec::new();

    let broker = Node {
        id: 50,
        label: "BROKER".into(),
        x: 30,
        y: 8,
    };

    // pubs -> broker
    for p in &pubs {
        links.push(Link {
            from: (p as &Node).id,
            to: broker.id,
        });
    }

    // broker -> subs
    for s in &subs {
        links.push(Link {
            from: broker.id,
            to: (s as &Node).id,
        });
    }

    App {
        pubs,
        subs,
        broker,
        links,
        inflight: Vec::new(),
    }
}

fn find_node(app: &App, id: usize) -> Option<&Node> {
    app.pubs
        .iter()
        .chain(app.subs.iter())
        .chain(std::iter::once(&app.broker))
        .find(|n| n.id == id)
}

fn node_center(n: &Node) -> (f32, f32) {
    (n.x as f32 + 6.0, n.y as f32 + 1.5)
}

fn node_rect(n: &Node) -> Rect {
    Rect::new(n.x, n.y, 12, 3)
}

fn is_inside_any_node(app: &App, x: u16, y: u16) -> bool {
    app.pubs
        .iter()
        .chain(app.subs.iter())
        .chain(std::iter::once(&app.broker))
        .any(|n| {
            let r = node_rect(n);
            x >= r.x && x < r.x + r.width && y >= r.y && y < r.y + r.height
        })
}

fn out_port(n: &Node) -> (u16, u16) {
    (n.x + 12, n.y + 1) // right middle
}

fn in_port(n: &Node) -> (u16, u16) {
    (n.x, n.y + 1) // left middle
}

fn clipped_endpoints(from: &Node, to: &Node) -> ((f32, f32), (f32, f32)) {
    let (fx, fy) = node_center(from);
    let (tx, ty) = node_center(to);

    let dx = tx - fx;
    let dy = ty - fy;
    let len = (dx * dx + dy * dy).sqrt();

    // how far from center to edge of node
    let margin = 7.0;

    let ux = dx / len;
    let uy = dy / len;

    (
        (fx + ux * margin, fy + uy * margin),
        (tx - ux * margin, ty - uy * margin),
    )
}

fn draw_link(f: &mut ratatui::Frame, app: &App, link: &Link) {
    let (from, to) = match (find_node(app, link.from), find_node(app, link.to)) {
        (Some(f), Some(t)) => (f, t),
        _ => return,
    };

    let steps = 20; // higher = smoother line

    for i in 0..=steps {
        let t = i as f32 / steps as f32;
        let x = from.x as f32 + (to.x as f32 - from.x as f32) * t;
        let y = from.y as f32 + (to.y as f32 - from.y as f32) * t;

        let rect = Rect::new(x as u16 + 5, y as u16 + 1, 1, 1);

        let dot = Paragraph::new("·").style(ratatui::style::Style::default().fg(Color::DarkGray));

        f.render_widget(dot, rect);
    }
}

fn draw_hline(f: &mut ratatui::Frame, app: &App, x1: u16, x2: u16, y: u16, color: Color) {
    let (a, b) = if x1 <= x2 { (x1, x2) } else { (x2, x1) };
    for x in a..=b {
        if is_near_any_node(app, x, y, 1) {
            continue;
        }
        f.render_widget(
            Paragraph::new("·").style(ratatui::style::Style::default().fg(color)),
            Rect::new(x, y, 1, 1),
        );
    }
}

fn draw_vline(f: &mut ratatui::Frame, app: &App, y1: u16, y2: u16, x: u16, color: Color) {
    let (a, b) = if y1 <= y2 { (y1, y2) } else { (y2, y1) };
    for y in a..=b {
        if is_near_any_node(app, x, y, 1) {
            continue;
        }
        f.render_widget(
            Paragraph::new("·").style(ratatui::style::Style::default().fg(color)),
            Rect::new(x, y, 1, 1),
        );
    }
}

fn draw_path(f: &mut ratatui::Frame, app: &App, from: &Node, to: &Node, color: Color) {
    let ((sx, sy), (ex, ey), mid_x) = routed_path(from, to);

    draw_hline(f, app, sx, mid_x, sy, color);

    // Corner dot at (mid_x, sy)
    if !is_inside_any_node(app, mid_x, sy) {
        f.render_widget(
            Paragraph::new("·").style(ratatui::style::Style::default().fg(color)),
            Rect::new(mid_x, sy, 1, 1),
        );
    }

    draw_vline(f, app, sy, ey, mid_x, color);

    // Corner dot at (mid_x, ey)
    if !is_inside_any_node(app, mid_x, ey) {
        f.render_widget(
            Paragraph::new("·").style(ratatui::style::Style::default().fg(color)),
            Rect::new(mid_x, ey, 1, 1),
        );
    }

    draw_hline(f, app, mid_x, ex, ey, color);
}

fn is_near_any_node(app: &App, x: u16, y: u16, pad: u16) -> bool {
    app.pubs
        .iter()
        .chain(app.subs.iter())
        .chain(std::iter::once(&app.broker))
        .any(|n| {
            let r = node_rect(n);
            let x0 = r.x.saturating_sub(pad);
            let y0 = r.y.saturating_sub(pad);
            let x1 = r.x + r.width + pad;
            let y1 = r.y + r.height + pad;
            x >= x0 && x < x1 && y >= y0 && y < y1
        })
}

fn draw_node(f: &mut ratatui::Frame, n: &Node) {
    let block = Block::default()
        .borders(Borders::ALL)
        .title(n.label.as_str());

    f.render_widget(block, Rect::new(n.x, n.y, 12, 3));
}

fn draw_ui(f: &mut ratatui::Frame, app: &App) {
    // Draw nodes
    draw_node(f, &app.broker);

    for p in &app.pubs {
        draw_node(f, p);
    }

    for s in &app.subs {
        draw_node(f, s);
    }

    // PUB -> BROKER paths
    for p in &app.pubs {
        draw_path(f, app, p, &app.broker, Color::DarkGray);
    }

    // BROKER -> SUB paths
    for s in &app.subs {
        draw_path(f, app, &app.broker, s, Color::DarkGray);
    }

    // Draw inflight messages
    for m in &app.inflight {
        let (x, y) = interpolate(app, m);
        if x > 0 && y > 0 {
            let (x, y) = interpolate(app, m);
            if x == 0 && y == 0 {
                continue;
            }
            if is_near_any_node(app, x, y, 0) {
                continue;
            } // don't draw inside/border

            f.set_cursor_position((x, y));

            let glyph = if m.progress < 0.2 || m.progress > 0.8 {
                "•" // small near ends
            } else if m.progress < 0.4 || m.progress > 0.6 {
                "●"
            } else {
                "⬤" // fattest in the middle
            };

            let dot = Paragraph::new(glyph).style(ratatui::style::Style::default().fg(m.color));

            f.render_widget(dot, Rect::new(x, y, 1, 1));
        }
    }
}

pub async fn run_ui(mut rx: mpsc::Receiver<VisualEvent>) -> anyhow::Result<()> {
    use crossterm::{
        cursor, execute,
        terminal::{Clear, ClearType, disable_raw_mode, enable_raw_mode},
    };

    let mut stdout = stdout();
    enable_raw_mode()?;
    execute!(stdout, Clear(ClearType::All), cursor::Hide)?;

    let mut terminal = Terminal::new(CrosstermBackend::new(stdout))?;

    let mut app = init_app();
    let mut last_tick = Instant::now();

    'ui: loop {
        // 1. Handle keyboard input (non-blocking)
        if event::poll(Duration::from_millis(1))?
            && let Event::Key(key) = event::read()?
        {
            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => {
                    break 'ui;
                }
                _ => {}
            }
        }

        // 2. Drain visual events
        while let Ok(ev) = rx.try_recv() {
            handle_event(&mut app, ev);
        }

        // 3. Animate
        let dt = last_tick.elapsed().as_secs_f32();
        last_tick = Instant::now();
        update_inflight(&mut app, dt);

        // 4. Draw
        terminal.draw(|f| draw_ui(f, &app))?;

        tokio::time::sleep(Duration::from_millis(33)).await;
    }

    // ---- CLEANUP (IMPORTANT) ----------------------------------------------
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), cursor::Show, Clear(ClearType::All))?;
    terminal.show_cursor()?;

    Ok(())
}

fn update_inflight(app: &mut App, dt: f32) {
    for m in &mut app.inflight {
        m.progress += dt * 0.4;
    }
    app.inflight.retain(|m| m.progress < 1.0);
}

fn routed_path(from: &Node, to: &Node) -> ((u16, u16), (u16, u16), u16) {
    let (sx, sy) = out_port(from);
    let (ex, ey) = in_port(to);
    let mid_x = (sx + ex) / 2;
    ((sx, sy), (ex, ey), mid_x)
}

fn interpolate(app: &App, m: &InFlight) -> (u16, u16) {
    let (from, to) = match (find_node(app, m.from), find_node(app, m.to)) {
        (Some(f), Some(t)) => (f, t),
        _ => return (0, 0),
    };

    let ((sx, sy), (ex, ey), mid_x) = routed_path(from, to);

    // segment lengths (Manhattan)
    let l1 = sx.abs_diff(mid_x) as f32;
    let l2 = sy.abs_diff(ey) as f32;
    let l3 = mid_x.abs_diff(ex) as f32;
    let total = (l1 + l2 + l3).max(1.0);

    let mut d = (m.progress.clamp(0.0, 1.0)) * total;

    // Walk segment 1: (sx,sy) -> (mid_x,sy)
    if d <= l1 {
        let x = if mid_x >= sx {
            sx + d as u16
        } else {
            sx - d as u16
        };
        return (x, sy);
    }
    d -= l1;

    // Walk segment 2: (mid_x,sy) -> (mid_x,ey)
    if d <= l2 {
        let y = if ey >= sy {
            sy + d as u16
        } else {
            sy - d as u16
        };
        return (mid_x, y);
    }
    d -= l2;

    // Walk segment 3: (mid_x,ey) -> (ex,ey)
    let x = if ex >= mid_x {
        mid_x + d as u16
    } else {
        mid_x - d as u16
    };
    (x, ey)
}

fn handle_event(app: &mut App, ev: VisualEvent) {
    match ev {
        VisualEvent::Publish { pub_id, .. } => {
            app.inflight.push(InFlight {
                from: pub_id,
                to: app.broker.id,
                progress: 0.0,
                color: Color::Blue,
            });
        }
        VisualEvent::Deliver { sub_id, .. } => {
            app.inflight.push(InFlight {
                from: app.broker.id,
                to: sub_id,
                progress: 0.0,
                color: Color::Green,
            });
        }
        VisualEvent::ErrorMsg { sub_id, .. } => {
            app.inflight.push(InFlight {
                from: app.broker.id,
                to: sub_id,
                progress: 0.0,
                color: Color::Red,
            });
        }
        VisualEvent::Hello { pub_id, .. } => {
            app.inflight.push(InFlight {
                from: pub_id,
                to: app.broker.id,
                progress: 0.0,
                color: Color::White,
            });
        }
        VisualEvent::HelloOk { sub_id, .. } => {
            app.inflight.push(InFlight {
                from: app.broker.id,
                to: sub_id,
                progress: 0.0,
                color: Color::LightBlue,
            });
        }
        VisualEvent::Subscribe { sub_id, .. } => {
            app.inflight.push(InFlight {
                from: sub_id,
                to: app.broker.id,
                progress: 0.0,
                color: Color::Cyan,
            });
        }
        VisualEvent::SubscribeOk { sub_id, .. } => {
            app.inflight.push(InFlight {
                from: app.broker.id,
                to: sub_id,
                progress: 0.0,
                color: Color::Magenta,
            });
        }
    }
}

pub async fn visual_client(
    mut conn: Conn,
    pub_id: usize,
    sub_id: usize,
    vis_tx: mpsc::Sender<VisualEvent>,
) -> anyhow::Result<()> {
    // ---- HELLO handshake -------------------------------------------------

    // Visual: client initiating hello
    let _ = vis_tx.send(VisualEvent::Hello { pub_id }).await;

    // Send HELLO
    conn.send(fibril_protocol::v1::helper::encode(
        Op::Hello,
        next_req_id(),
        &Hello {
            client_name: format!("tui-client-{}", pub_id),
            client_version: "0.1".into(),
            protocol_version: PROTOCOL_V1,
        },
    ))
    .await?;
    let _ = vis_tx.send(VisualEvent::Hello { pub_id }).await;

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    // Wait for HELLOOK / HELLOERR
    let frame = conn
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("connection closed during hello"))??;

    match frame.opcode {
        x if x == Op::HelloOk as u16 => {
            let _ok: HelloOk = fibril_protocol::v1::helper::decode(&frame);
            // println!("HELLO OK, negotiated protocol {}", ok.protocol_version);

            let _ = vis_tx.send(VisualEvent::HelloOk { sub_id }).await;
        }
        x if x == Op::HelloErr as u16 => {
            let err: ErrorMsg = fibril_protocol::v1::helper::decode(&frame);
            let _ = vis_tx
                .send(VisualEvent::ErrorMsg {
                    sub_id,
                    code: err.code,
                })
                .await;
            // anyhow::bail!("HELLO rejected: {}", err.message);
        }
        _ => {
            // anyhow::bail!("unexpected frame during HELLO: {}", frame.opcode);
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    // ---- SUBSCRIBE -------------------------------------------------------
    conn.send(fibril_protocol::v1::helper::encode(
        Op::Subscribe,
        next_req_id(),
        &Subscribe {
            topic: "t1".into(),
            group: "g1".into(),
            prefetch: 100,
            auto_ack: true,
        },
    ))
    .await?;
    let _ = vis_tx.send(VisualEvent::Subscribe { sub_id }).await;

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    // ⬅️ READ THE RESPONSE
    let frame = conn
        .next()
        .await
        .ok_or_else(|| anyhow::anyhow!("connection closed during subscribe"))??;

    match frame.opcode {
        x if x == Op::SubscribeOk as u16 => {
            let _ok: SubscribeOk = fibril_protocol::v1::helper::decode(&frame);

            // println!(
            //     "SUBSCRIBE OK topic={} group={} partition={}",
            //     ok.topic, ok.group, ok.partition
            // );

            let _ = vis_tx.send(VisualEvent::SubscribeOk { sub_id }).await;
        }

        x if x == Op::Error as u16 => {
            let err: ErrorMsg = fibril_protocol::v1::helper::decode(&frame);
            anyhow::bail!(
                "SUBSCRIBE rejected: code={} msg='{}'",
                err.code,
                err.message
            );
        }

        _ => {
            anyhow::bail!("unexpected frame during SUBSCRIBE: {}", frame.opcode);
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(250)).await;

    let (mut sink, mut stream) = conn.split();
    let (pub_tx, mut pub_rx) = mpsc::channel::<Publish>(64);
    let publish_tx = vis_tx.clone();

    tokio::spawn(async move {
        loop {
            tokio::time::sleep(random_idle_duration()).await;

            let burst = random_burst_size();

            for _ in 0..burst {
                // 🔵 Visual: intent to publish
                let _ = publish_tx
                    .send(VisualEvent::Publish {
                        pub_id,
                        topic: "t1".into(),
                    })
                    .await;

                // Enqueue actual publish
                if pub_tx
                    .send(Publish {
                        topic: "t1".into(),
                        partition: 0,
                        require_confirm: false,
                        payload: b"hello".to_vec(),
                    })
                    .await
                    .is_err()
                {
                    break;
                }

                tokio::time::sleep(random_inter_message_delay()).await;
            }
        }
    });

    tokio::spawn(async move {
        while let Some(p) = pub_rx.recv().await {
            let _ = sink
                .send(fibril_protocol::v1::helper::encode(
                    Op::Publish,
                    next_req_id(),
                    &p,
                ))
                .await;
        }
    });

    while let Some(frame) = stream.next().await {
        let frame = frame?;
        // println!("Received frame with code {:?}", frame.opcode);
        match frame.opcode {
            x if x == Op::Deliver as u16 => {
                let d: Deliver = fibril_protocol::v1::helper::decode(&frame);
                // println!("CLIENT got DELIVER offset={}", d.offset);
                let _ = vis_tx
                    .send(VisualEvent::Deliver {
                        sub_id,
                        offset: d.offset,
                    })
                    .await;
            }
            x if x == Op::Error as u16 => {
                let e: ErrorMsg = fibril_protocol::v1::helper::decode(&frame);
                // println!("CLIENT got ErrorMsg code={} msg='{}'", e.code, e.message);
                let _ = vis_tx
                    .send(VisualEvent::ErrorMsg {
                        sub_id,
                        code: e.code,
                    })
                    .await;
            }
            _ => {}
        }
    }

    Ok(())
}

async fn connect_to_server() -> anyhow::Result<Conn> {
    let stream = TcpStream::connect("127.0.0.1:9876").await?;
    Ok(Framed::new(stream, ProtoCodec))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (tx, rx) = mpsc::channel(1000);

    execute!(stdout(), Clear(ClearType::All))?;
    execute!(stdout(), cursor::Hide)?;
    for i in 0..3 {
        let conn = connect_to_server().await?;
        let tx = tx.clone();
        tokio::spawn(visual_client(conn, i, 100 + i, tx));
    }
    tokio::spawn(run_ui(rx)).await??;

    execute!(stdout(), cursor::Show)?;
    disable_raw_mode()?;

    Ok(())
}
