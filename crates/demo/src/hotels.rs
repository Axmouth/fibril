//! The hotel group: bookings flow in on an evening-heavy rhythm and live in a
//! bounded in-memory ledger, from which much rarer cancellations are drawn -
//! with a refund computed from a days-before-arrival tier policy. A wordy
//! support mailbox rounds it out, staffed by a sluggish consumer who takes an
//! afternoon nap long enough for the dashboard to notice the backlog.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use fibril_client::{NewMessage, SubEvent};
use serde::Serialize;

use crate::clock::rhythm;
use crate::data::{person, pick};
use crate::pace::{producer_gap, service_time};
use crate::Demo;

/// Bookings trickle at lunch and surge in the evening.
const BOOKING_RHYTHM: &[(f64, f64)] = &[
    (0.00, 0.05),
    (0.35, 0.40),
    (0.50, 0.90),
    (0.62, 0.50),
    (0.78, 2.40),
    (0.92, 0.30),
];

const HOTELS: &[(&str, &str)] = &[
    ("The Gilded Fern", "Athens"),
    ("Hotel Meridian", "Lisbon"),
    ("Casa Azul", "Valencia"),
    ("The Harbour House", "Rotterdam"),
    ("Villa Selene", "Thessaloniki"),
    ("Grand Bastion", "Hamburg"),
    ("The Lighthouse Inn", "Marseille"),
    ("Palazzo Vento", "Genoa"),
    ("Amber Court", "Gdansk"),
    ("The Old Mill Lodge", "Bilbao"),
    ("Seaglass Suites", "Antwerp"),
    ("Fortezza Grand", "Trieste"),
];

const BOARDS: &[&str] = &["RO", "BB", "HB", "FB", "AI"];

const CANCEL_REASONS: &[&str] = &[
    "change of plans",
    "found a better rate",
    "flight cancelled",
    "illness",
    "double booking",
    "work emergency",
    "visa not approved in time",
    "booked the wrong week",
];

#[derive(Clone, Serialize)]
pub struct Booking {
    booking_ref: String,
    hotel: &'static str,
    city: &'static str,
    guest: String,
    adults: u32,
    kids: u32,
    nights: u32,
    days_until_arrival: u32,
    board: &'static str,
    total_eur: u32,
}

#[derive(Serialize)]
struct Cancellation {
    booking_ref: String,
    hotel: &'static str,
    guest: String,
    reason: &'static str,
    days_before_arrival: u32,
    refund_pct: u32,
    refund_eur: u32,
}

/// Bounded ledger of recent bookings, shared between the booking desk (push)
/// and the cancellation line (draw). Old entries fall off the front.
pub type Ledger = Arc<Mutex<VecDeque<Booking>>>;

pub fn ledger() -> Ledger {
    Arc::new(Mutex::new(VecDeque::with_capacity(300)))
}

fn build_booking() -> Booking {
    let (hotel, city) = *pick(HOTELS);
    let nights = 1 + fastrand::u32(..9);
    let adults = 1 + fastrand::u32(..3);
    let kids = if fastrand::f64() < 0.35 {
        1 + fastrand::u32(..3)
    } else {
        0
    };
    let per_night = 70 + fastrand::u32(..260);
    Booking {
        booking_ref: crate::data::reference("BK"),
        hotel,
        city,
        guest: person(),
        adults,
        kids,
        nights,
        days_until_arrival: 1 + fastrand::u32(..60),
        board: *pick(BOARDS),
        total_eur: per_night * nights,
    }
}

/// The refund tiers: generous far out, half close in, nothing last-minute.
fn refund_pct(days_before_arrival: u32) -> u32 {
    match days_before_arrival {
        d if d >= 14 => 100,
        d if d >= 3 => 50,
        _ => 0,
    }
}

/// The booking desk: publishes bookings and remembers them in the ledger.
pub async fn run_booking_desk(demo: Arc<Demo>, ledger: Ledger) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let publisher = client.publisher("bookings")?;
    loop {
        producer_gap(
            0.5 * demo.scale,
            rhythm(BOOKING_RHYTHM, demo.clock.time_of_day()),
        )
        .await;
        let booking = build_booking();
        publisher
            .publish_confirmed(NewMessage::json(&booking)?.partition_key(booking.hotel))
            .await?;
        let mut held = ledger.lock().unwrap_or_else(|p| p.into_inner());
        if held.len() >= 300 {
            held.pop_front();
        }
        held.push_back(booking);
    }
}

/// The cancellation line: much rarer, always about a real remembered booking,
/// stating the refund the tier policy grants.
pub async fn run_cancellations(demo: Arc<Demo>, ledger: Ledger) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let publisher = client.publisher("bookings.cancellations")?;
    loop {
        producer_gap(0.05 * demo.scale, 1.0).await;
        let Some(booking) = ({
            let mut held = ledger.lock().unwrap_or_else(|p| p.into_inner());
            if held.is_empty() {
                None
            } else {
                let index = fastrand::usize(..held.len());
                held.remove(index)
            }
        }) else {
            continue;
        };
        let pct = refund_pct(booking.days_until_arrival);
        let cancellation = Cancellation {
            booking_ref: booking.booking_ref,
            hotel: booking.hotel,
            guest: booking.guest,
            reason: *pick(CANCEL_REASONS),
            days_before_arrival: booking.days_until_arrival,
            refund_pct: pct,
            refund_eur: booking.total_eur * pct / 100,
        };
        publisher
            .publish_confirmed(NewMessage::json(&cancellation)?)
            .await?;
    }
}

/// Guests write in about real bookings, in prose.
pub async fn run_guest_mail(demo: Arc<Demo>, ledger: Ledger) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let publisher = client.publisher("support.mail")?;
    loop {
        producer_gap(0.03 * demo.scale, 1.0).await;
        let subject = {
            let held = ledger.lock().unwrap_or_else(|p| p.into_inner());
            held.get(fastrand::usize(..held.len().max(1))).cloned()
        };
        let Some(booking) = subject else { continue };
        let text = format!(
            "Subject: question about booking {}\n\nHello,\n\nwe arrive at {} ({}) in {} days \
             and wanted to ask whether a late check-out is possible, and if the {} board \
             can still be upgraded. Party of {} adults{}.\n\nThanks,\n{}",
            booking.booking_ref,
            booking.hotel,
            booking.city,
            booking.days_until_arrival,
            booking.board,
            booking.adults,
            if booking.kids > 0 {
                format!(" and {} kids", booking.kids)
            } else {
                String::new()
            },
            booking.guest,
        );
        publisher.publish_confirmed(NewMessage::text(text)).await?;
    }
}

/// Steady back-office consumers for bookings and cancellations.
pub async fn run_booking_ops(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let mut sub = client.subscribe("bookings")?.prefetch(8).sub().await?;
    while let SubEvent::Delivery(msg) = sub.recv().await {
        service_time(30, 80).await;
        msg.complete().await?;
    }
    Ok(())
}

pub async fn run_cancellation_ops(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let mut sub = client
        .subscribe("bookings.cancellations")?
        .prefetch(4)
        .sub()
        .await?;
    while let SubEvent::Delivery(msg) = sub.recv().await {
        service_time(100, 250).await;
        msg.complete().await?;
    }
    Ok(())
}

/// The support desk: the sluggish personality. Slow on a good day, and every
/// afternoon it walks away entirely - the connection drops, mail piles up,
/// the dashboard's attention panel notices, and when the nap ends the backlog
/// drains and the condition resolves.
pub async fn run_support_desk(demo: Arc<Demo>) -> anyhow::Result<()> {
    const NAP: (f64, f64) = (0.55, 0.70);
    loop {
        if demo.naps && demo.clock.within(NAP.0, NAP.1) {
            // Sleep out the remainder of the nap window with no connection.
            while demo.clock.within(NAP.0, NAP.1) {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
        }
        let client = demo.client().await?;
        let mut sub = client.subscribe("support.mail")?.prefetch(2).sub().await?;
        loop {
            if demo.naps && demo.clock.within(NAP.0, NAP.1) {
                // Walk away: dropping the subscription and client closes the
                // connection, so the queue genuinely has no consumer.
                break;
            }
            let event = tokio::select! {
                event = sub.recv() => event,
                _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => continue,
            };
            let SubEvent::Delivery(msg) = event else {
                return Ok(());
            };
            service_time(200, 500).await;
            msg.complete().await?;
        }
    }
}
