//! The freight desk: shipment manifests as XML (the enterprise classic),
//! with cargo tags, weights, and optional insurance. A small slice gets
//! flagged for customs, where a single very slow officer keeps a lazy
//! backlog alive on the shipments.customs queue.

use std::sync::Arc;

use fibril_client::{NewMessage, SubEvent};

use crate::clock::rhythm;
use crate::data::{pick, reference, CITIES};
use crate::pace::{producer_gap, service_time};
use crate::Demo;

/// Business hours plateau, nothing at night.
const OFFICE_HOURS: &[(f64, f64)] = &[
    (0.00, 0.02),
    (0.30, 0.10),
    (0.38, 1.00),
    (0.55, 1.20),
    (0.70, 0.80),
    (0.78, 0.10),
];

const TAGS: &[&str] = &[
    "fragile",
    "cold-chain",
    "hazmat",
    "oversize",
    "high-value",
    "stackable",
    "this-side-up",
    "live-animals",
    "keep-dry",
];

const CARRIERS: &[&str] = &[
    "NordCargo",
    "Meltemi Lines",
    "TransAdria",
    "Baltic Star",
    "Ionian Freight Co",
];

fn build_shipment_xml() -> (String, bool) {
    let id = reference("SHP");
    let origin = pick(CITIES);
    let mut destination = pick(CITIES);
    while destination == origin {
        destination = pick(CITIES);
    }
    let weight_kg = 2.0 + fastrand::f64() * 1200.0;
    let tags: Vec<&str> = TAGS
        .iter()
        .filter(|_| fastrand::f64() < 0.3)
        .copied()
        .collect();
    let insured = fastrand::f64() < 0.55;
    let insurance = if insured {
        format!(
            "<insured currency=\"EUR\">{}</insured>",
            (weight_kg * (8.0 + fastrand::f64() * 40.0)) as u64
        )
    } else {
        "<uninsured/>".to_string()
    };
    let tag_xml: String = tags
        .iter()
        .map(|t| format!("<tag>{t}</tag>"))
        .collect::<Vec<_>>()
        .join("");
    let customs = fastrand::f64() < 0.06;
    let xml = format!(
        "<shipment id=\"{id}\">\
         <carrier>{}</carrier>\
         <origin>{origin}</origin>\
         <destination>{destination}</destination>\
         <weight unit=\"kg\">{:.1}</weight>\
         <tags>{tag_xml}</tags>\
         {insurance}\
         {}\
         </shipment>",
        pick(CARRIERS),
        weight_kg,
        if customs { "<customs-inspection/>" } else { "" },
    );
    (xml, customs)
}

/// The dispatch office: shipments on office hours, occasionally flagging one
/// for customs on a second, slower queue.
pub async fn run_dispatch(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let shipments = client.publisher("shipments")?;
    let customs = client.publisher("shipments.customs")?;
    loop {
        producer_gap(0.4 * demo.scale, rhythm(OFFICE_HOURS, demo.clock.time_of_day())).await;
        let (xml, inspect) = build_shipment_xml();
        let bytes = xml.clone().into_bytes();
        shipments
            .publish_confirmed(NewMessage::raw(bytes).content_type("application/xml"))
            .await?;
        if inspect {
            customs
                .publish_confirmed(
                    NewMessage::raw(xml.into_bytes()).content_type("application/xml"),
                )
                .await?;
        }
    }
}

/// The freight desk clerk: a steady consumer.
pub async fn run_desk(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let mut sub = client.subscribe("shipments")?.prefetch(8).sub().await?;
    while let SubEvent::Delivery(msg) = sub.recv().await {
        service_time(40, 100).await;
        msg.complete().await?;
    }
    Ok(())
}

/// The customs officer: takes seconds per manifest, so the customs queue
/// holds a small honest backlog whenever inspections cluster.
pub async fn run_customs_officer(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let mut sub = client
        .subscribe("shipments.customs")?
        .prefetch(2)
        .sub()
        .await?;
    while let SubEvent::Delivery(msg) = sub.recv().await {
        service_time(2000, 5000).await;
        msg.complete().await?;
    }
    Ok(())
}
