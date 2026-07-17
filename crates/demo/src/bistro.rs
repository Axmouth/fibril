//! The bistro: a delivery-food pipeline of interacting queues. Customers place
//! orders, the kitchen consumes them and hands each to a courier (deliveries)
//! while charging the card (payments), couriers ping their position onto a
//! stream while riding, and a flaky payment processor feeds the dead-letter
//! flow. Rush hours come from the shared day clock.

use std::sync::Arc;

use fibril_client::NewMessage;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::clock::rhythm;
use crate::data::{person, pick, reference};
use crate::pace::{producer_gap, service_time};
use crate::Demo;

/// Breakfast bump, lunch rush, dinner peak, dead night.
const RUSH: &[(f64, f64)] = &[
    (0.00, 0.05),
    (0.08, 0.40),
    (0.13, 0.15),
    (0.42, 0.30),
    (0.48, 3.00),
    (0.58, 0.50),
    (0.74, 1.00),
    (0.82, 3.60),
    (0.92, 0.40),
];

const MENU: &[(&str, u32)] = &[
    ("halloumi burger", 1150),
    ("lamb gyros plate", 1380),
    ("wood-fired margherita", 1050),
    ("truffle rigatoni", 1490),
    ("smash burger", 990),
    ("grilled octopus", 1720),
    ("caesar salad", 890),
    ("pumpkin risotto", 1240),
    ("bao trio", 1080),
    ("seabass fillet", 1690),
    ("wild mushroom pie", 1140),
    ("baklava cheesecake", 680),
    ("charred cauliflower steak", 1090),
    ("short rib ragu pappardelle", 1560),
    ("crispy chicken souvlaki", 1180),
    ("burrata and blood orange", 950),
];

const ADD_ONS: &[(&str, u32)] = &[
    ("extra halloumi", 250),
    ("garlic flatbread", 320),
    ("truffle fries", 450),
    ("side tzatziki", 180),
    ("vegan cheese", 200),
    ("chili honey", 120),
    ("smoked paprika aioli", 150),
    ("pickled red onion", 100),
];

const NOTES: &[&str] = &[
    "no onions please",
    "ring the bell twice",
    "extra napkins",
    "leave at the door",
    "cutlery not needed",
    "nut allergy - please flag the kitchen",
];

const COURIERS: &[&str] = &[
    "kostas", "mira", "jonas", "amara", "petros", "yuki", "sofia", "dario",
];

#[derive(Serialize)]
struct OrderItem {
    item: &'static str,
    qty: u32,
    add_ons: Vec<&'static str>,
    cents: u32,
}

#[derive(Serialize)]
struct Order {
    order_id: String,
    customer: String,
    items: Vec<OrderItem>,
    total_cents: u32,
    note: Option<&'static str>,
    placed_unix_ms: u64,
}

/// The part of an order the downstream stages need back out of the payload.
#[derive(Deserialize)]
struct OrderRef {
    order_id: String,
    customer: String,
    total_cents: u32,
}

#[derive(Serialize)]
struct Delivery {
    order_id: String,
    courier: &'static str,
    distance_km: f64,
    eta_min: u32,
}

#[derive(Serialize)]
struct Payment {
    order_id: String,
    customer: String,
    amount_cents: u32,
    method: &'static str,
}

#[derive(Serialize)]
struct CourierPing {
    courier: &'static str,
    order_id: String,
    lat: f64,
    lng: f64,
    ts_unix_ms: u64,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn build_order() -> Order {
    let customer = person();
    let count = 1 + fastrand::usize(..3);
    let mut total = 0;
    let items = (0..count)
        .map(|_| {
            let (item, cents) = *pick(MENU);
            let qty = 1 + fastrand::u32(..2);
            // A chain, not independent rolls: 25% chance of one add-on,
            // then 25% of a second given the first, and so on - most items
            // arrive plain, a few arrive loaded.
            let mut add_ons: Vec<&'static str> = Vec::new();
            while add_ons.len() < 3 && fastrand::f64() < 0.25 {
                let (name, _) = *pick(ADD_ONS);
                if add_ons.contains(&name) {
                    break;
                }
                add_ons.push(name);
            }
            let add_on_cents: u32 = ADD_ONS
                .iter()
                .filter(|(name, _)| add_ons.contains(name))
                .map(|(_, c)| c)
                .sum();
            total += qty * cents + add_on_cents;
            OrderItem {
                item,
                qty,
                add_ons,
                cents,
            }
        })
        .collect();
    Order {
        order_id: reference("ORD"),
        customer,
        items,
        total_cents: total,
        note: (fastrand::f64() < 0.3).then(|| *pick(NOTES)),
        placed_unix_ms: now_ms(),
    }
}

/// Customers: publish orders on the rush curve, keyed by customer so one
/// person's orders keep their relative order.
pub async fn run_customers(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let publisher = client.publisher("orders")?;
    loop {
        producer_gap(0.9 * demo.scale, rhythm(RUSH, demo.clock.time_of_day())).await;
        let order = build_order();
        let key = order.customer.clone();
        publisher
            .publish_confirmed(NewMessage::json(&order)?.partition_key(key))
            .await?;
    }
}

/// A courier assignment handed from the kitchen to the courier pool.
pub struct CourierJob {
    order_id: String,
    courier: &'static str,
    pings: u32,
}

/// The kitchen: consumes orders at a bounded prep rate, then charges the card
/// (payments) and hands the food to a courier (deliveries plus location pings).
pub async fn run_kitchen(demo: Arc<Demo>, couriers: mpsc::Sender<CourierJob>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let deliveries = client.publisher("deliveries")?;
    let payments = client.publisher("payments")?;
    let mut sub = client.subscribe("orders")?.prefetch(8).sub().await?;
    while let Some(msg) = sub.recv().await {
        service_time(30, 90).await;
        let Ok(order) = serde_json::from_slice::<OrderRef>(&msg.payload) else {
            msg.complete().await?;
            continue;
        };
        let courier = *pick(COURIERS);
        let distance_km = 0.5 + fastrand::f64() * 6.0;
        let delivery = Delivery {
            order_id: order.order_id.clone(),
            courier,
            distance_km: (distance_km * 10.0).round() / 10.0,
            eta_min: 8 + (distance_km * 3.0) as u32,
        };
        deliveries
            .publish_confirmed(NewMessage::json(&delivery)?.partition_key(courier))
            .await?;
        let payment = Payment {
            order_id: order.order_id.clone(),
            customer: order.customer,
            amount_cents: order.total_cents,
            method: *pick(&["card", "app", "cash"]),
        };
        payments
            .publish_confirmed(NewMessage::json(&payment)?)
            .await?;
        let _ = couriers
            .send(CourierJob {
                order_id: order.order_id,
                courier,
                pings: 6 + fastrand::u32(..8),
            })
            .await;
        msg.complete().await?;
    }
    Ok(())
}

/// The courier pool: while a delivery rides, its position pings onto the
/// courier.location stream every second or so, keyed by courier.
pub async fn run_couriers(
    demo: Arc<Demo>,
    mut jobs: mpsc::Receiver<CourierJob>,
) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let pings = client.publisher("courier.location")?;
    let mut active: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    while let Some(job) = jobs.recv().await {
        // Bound the concurrently riding couriers, oldest ride finishes first.
        while active.len() >= 6 {
            active.join_next().await;
        }
        let publisher = pings.clone();
        active.spawn(async move {
            let (mut lat, mut lng) = (37.98 + fastrand::f64() * 0.05, 23.72 + fastrand::f64() * 0.05);
            for _ in 0..job.pings {
                lat += (fastrand::f64() - 0.5) * 0.004;
                lng += (fastrand::f64() - 0.5) * 0.004;
                let ping = CourierPing {
                    courier: job.courier,
                    order_id: job.order_id.clone(),
                    lat: (lat * 1e5).round() / 1e5,
                    lng: (lng * 1e5).round() / 1e5,
                    ts_unix_ms: now_ms(),
                };
                let message = match NewMessage::msg_pack(&ping) {
                    Ok(message) => message.partition_key(job.courier),
                    Err(_) => break,
                };
                if publisher.publish_confirmed(message).await.is_err() {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(700 + fastrand::u64(..600)))
                    .await;
            }
        });
    }
    Ok(())
}

/// Delivery tracking: a steady consumer confirming hand-offs.
pub async fn run_delivery_tracker(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let mut sub = client.subscribe("deliveries")?.prefetch(8).sub().await?;
    while let Some(msg) = sub.recv().await {
        service_time(40, 120).await;
        msg.complete().await?;
    }
    Ok(())
}

/// The payment processor: the flaky personality. Every delivery has a
/// `flakiness` chance of being requeued, so a slice of payments burns its
/// retry budget and lands in payments.dead for the dashboard's DLQ story.
pub async fn run_payment_processor(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let mut sub = client.subscribe("payments")?.prefetch(8).sub().await?;
    while let Some(msg) = sub.recv().await {
        service_time(30, 80).await;
        if fastrand::f64() < demo.flakiness {
            msg.retry().await?;
        } else {
            msg.complete().await?;
        }
    }
    Ok(())
}
