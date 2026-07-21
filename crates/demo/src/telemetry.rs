//! Streams with readers: kitchen fridge sensors publish a slow steady feed,
//! and two durable-cursor readers follow it - one glued to the tail, one
//! that disconnects for a stretch every day and then catches its cursor back
//! up, so the Streams page shows both cursor states.

use std::sync::Arc;
use std::time::Duration;

use fibril_client::{NewMessage, SubEvent};
use serde::Serialize;

use crate::Demo;

#[derive(Serialize)]
struct SensorReading {
    sensor: &'static str,
    temp_c: f64,
    door_open: bool,
    ts_unix_ms: u64,
}

const SENSORS: &[(&str, f64)] = &[
    ("walk-in-fridge", 4.0),
    ("freezer", -18.0),
    ("wine-cellar", 12.0),
];

/// The sensors: a reading every couple of seconds per sensor, temperature
/// drifting on the day cycle with noise and the occasional door-open spike.
pub async fn run_sensors(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let publisher = client.publisher("kitchen.telemetry")?;
    loop {
        for (sensor, base) in SENSORS {
            let day_drift = (demo.clock.time_of_day() * std::f64::consts::TAU).sin() * 0.6;
            let door_open = fastrand::f64() < 0.04;
            let spike = if door_open { 2.5 } else { 0.0 };
            let reading = SensorReading {
                sensor,
                temp_c: ((base + day_drift + fastrand::f64() * 0.4 + spike) * 10.0).round() / 10.0,
                door_open,
                ts_unix_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0),
            };
            publisher
                .publish_confirmed(NewMessage::msg_pack(&reading)?.partition_key(*sensor))
                .await?;
        }
        tokio::time::sleep(Duration::from_millis(1500 + fastrand::u64(..700))).await;
    }
}

/// The dashboard reader: a durable cursor that never leaves the tail.
pub async fn run_dashboard_reader(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let mut sub = client
        .stream("kitchen.telemetry")?
        .durable("demo-dashboard")
        .prefetch(64)
        .sub_auto_ack()
        .await?;
    while let SubEvent::Delivery(_) = sub.recv().await {}
    Ok(())
}

/// The analytics reader: disconnects for a stretch of every day, so its
/// durable cursor visibly falls behind and then catches up on return.
pub async fn run_analytics_reader(demo: Arc<Demo>) -> anyhow::Result<()> {
    const OFFLINE: (f64, f64) = (0.30, 0.45);
    loop {
        if demo.clock.within(OFFLINE.0, OFFLINE.1) {
            while demo.clock.within(OFFLINE.0, OFFLINE.1) {
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        }
        let client = demo.client().await?;
        let mut sub = client
            .stream("kitchen.telemetry")?
            .durable("demo-analytics")
            .prefetch(64)
            .sub_auto_ack()
            .await?;
        loop {
            if demo.clock.within(OFFLINE.0, OFFLINE.1) {
                break;
            }
            tokio::select! {
                event = sub.recv() => {
                    if matches!(event, SubEvent::Closed(_)) {
                        return Ok(());
                    }
                }
                _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            }
        }
    }
}

/// The dispatch screen: follows courier positions at the tail.
pub async fn run_dispatch_screen(demo: Arc<Demo>) -> anyhow::Result<()> {
    let client = demo.client().await?;
    let mut sub = client
        .stream("courier.location")?
        .durable("demo-dispatch")
        .prefetch(64)
        .sub_auto_ack()
        .await?;
    while let SubEvent::Delivery(_) = sub.recv().await {}
    Ok(())
}
