//! Storage back-compat fixture generator.
//!
//! Drives a running broker through a fixed sequence of durable operations that
//! together exercise the keratin on-disk formats a later broker must keep
//! reading: queue enqueue/ack/inflight/delayed events, a dead-letter path, and
//! a durable stream log with a committed cursor. The broker writes the data
//! dir; this tool only issues client requests, so the bytes on disk belong to
//! whichever broker version is listening. It prints a manifest describing what
//! the data dir should contain so the reopen test can verify the numbers.
//!
//! Usage: compat-fixture-gen --broker <addr> --tag <version> [--out <path>]
//! The manifest is written to `--out` (or stdout when omitted).

use std::error::Error;
use std::time::Duration;

use fibril_client::{ClientOptions, NewMessage, QueueConfig, StreamConfig};
use tokio::time::timeout;

const QUEUE_TOPIC: &str = "orders";
const QUEUE_GROUP: &str = "workers";
const DLQ_TOPIC: &str = "orders.dlq";
const QUEUE_PARTITIONS: u32 = 4;
const QUEUE_MAX_RETRIES: u32 = 3;

const READY_COUNT: usize = 16;
const DELAYED_COUNT: usize = 4;
const RECEIVE_COUNT: usize = 8;
const ACK_COUNT: usize = 3;
const DLQ_COUNT: usize = 2;
// The remainder of the received batch (RECEIVE_COUNT - ACK_COUNT - DLQ_COUNT) is
// left inflight: received but never settled. Dropping an InflightMessage does
// not settle it, so the broker's MarkInflight record stays on disk.

const STREAM_TOPIC: &str = "events";
const STREAM_PARTITIONS: u32 = 2;
const STREAM_CURSOR: &str = "archiver";
const STREAM_COUNT: usize = 10;

// A far-future delay keeps the delayed messages parked as EnqueueDelayed rather
// than becoming ready during generation (Delayable bare number is seconds).
const DELAY_SECONDS: u64 = 86_400;

struct Args {
    broker: String,
    tag: String,
    out: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut broker = "127.0.0.1:9876".to_string();
    let mut tag = None;
    let mut out = None;
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        match flag.as_str() {
            "--broker" => broker = it.next().ok_or("--broker needs a value")?,
            "--tag" => tag = Some(it.next().ok_or("--tag needs a value")?),
            "--out" => out = Some(it.next().ok_or("--out needs a value")?),
            other => return Err(format!("unknown argument `{other}`")),
        }
    }
    Ok(Args {
        broker,
        tag: tag.ok_or("--tag is required")?,
        out,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = parse_args()?;
    // Built-in fibril/fibril credentials, accepted from loopback on a broker
    // with no user store configured (the default the fixture generates against).
    let client = ClientOptions::new()
        .auth("fibril", "fibril")
        .connect(args.broker.as_str())
        .await?;

    // The dead-letter target is a plain queue the failed messages land in.
    client
        .declare_queue(QueueConfig::new(DLQ_TOPIC)?)
        .await?;

    client
        .declare_queue(
            QueueConfig::new(QUEUE_TOPIC)?
                .group(QUEUE_GROUP)?
                .partitions(QUEUE_PARTITIONS)
                .max_retries(QUEUE_MAX_RETRIES)
                .custom_dead_letter_queue(DLQ_TOPIC)?,
        )
        .await?;

    let publisher = client.publisher_grouped(QUEUE_TOPIC, QUEUE_GROUP)?;
    for i in 0..READY_COUNT {
        publisher
            .publish_confirmed(NewMessage::raw(format!("orders-ready-{i:03}").into_bytes()))
            .await?;
    }
    for i in 0..DELAYED_COUNT {
        publisher
            .publish_delayed(
                NewMessage::raw(format!("orders-delayed-{i:03}").into_bytes()),
                DELAY_SECONDS,
            )
            .await?;
    }

    // Receive a batch and split it across settle outcomes: some acked (Ack
    // records), some failed to the dead-letter queue (DeadLetter records), the
    // rest held inflight (MarkInflight records that survive on disk).
    let mut sub = client
        .subscribe(QUEUE_TOPIC)?
        .group(QUEUE_GROUP)?
        .prefetch(RECEIVE_COUNT as u32)
        .sub()
        .await?;

    let mut received = Vec::new();
    for _ in 0..RECEIVE_COUNT {
        match timeout(Duration::from_secs(5), sub.recv()).await {
            Ok(Some(msg)) => received.push(msg),
            Ok(None) => break,
            Err(_) => break,
        }
    }

    let mut acked = 0;
    let mut dead_lettered = 0;
    let mut held_inflight = Vec::new();
    for msg in received {
        if acked < ACK_COUNT {
            msg.complete().await?;
            acked += 1;
        } else if dead_lettered < DLQ_COUNT {
            // fail = negative-ack without requeue, which routes to the
            // dead-letter queue when a policy is set.
            msg.fail().await?;
            dead_lettered += 1;
        } else {
            held_inflight.push(msg);
        }
    }

    // Durable stream: the log plus a committed cursor.
    client
        .declare_plexus(
            StreamConfig::new(STREAM_TOPIC)?
                .durable()
                .partitions(STREAM_PARTITIONS),
        )
        .await?;
    let stream_publisher = client.publisher(STREAM_TOPIC)?;
    for i in 0..STREAM_COUNT {
        stream_publisher
            .publish_confirmed(NewMessage::raw(format!("events-{i:03}").into_bytes()))
            .await?;
    }

    let mut stream_consumed = 0;
    let mut stream_sub = client
        .stream(STREAM_TOPIC)?
        .durable(STREAM_CURSOR)
        .sub()
        .await?;
    for _ in 0..STREAM_COUNT {
        match timeout(Duration::from_secs(5), stream_sub.recv()).await {
            Ok(Some(_)) => stream_consumed += 1,
            Ok(None) => break,
            Err(_) => break,
        }
    }

    // Ready messages the subscription never received always redeliver on a cold
    // reopen. Inflight and delayed timing is wall-clock dependent, so this floor
    // is the robust lower bound the reopen test asserts.
    let recoverable_floor = READY_COUNT.saturating_sub(acked + dead_lettered + held_inflight.len());

    let manifest = serde_json::json!({
        "tag": args.tag,
        "payload_prefix": "orders-",
        "queue": {
            "topic": QUEUE_TOPIC,
            "group": QUEUE_GROUP,
            "partitions": QUEUE_PARTITIONS,
            "dlq_topic": DLQ_TOPIC,
            "ready_published": READY_COUNT,
            "delayed_published": DELAYED_COUNT,
            "acked": acked,
            "dead_lettered": dead_lettered,
            "left_inflight": held_inflight.len(),
        },
        "stream": {
            "topic": STREAM_TOPIC,
            "partitions": STREAM_PARTITIONS,
            "durable_cursor": STREAM_CURSOR,
            "published": STREAM_COUNT,
            "consumed": stream_consumed,
        },
        "recoverable_floor": recoverable_floor,
    });
    let rendered = serde_json::to_string_pretty(&manifest)?;

    // Hold the inflight messages until every write is issued so they are never
    // settled, then let a clean broker shutdown (driven by the caller) flush.
    drop(held_inflight);

    match args.out {
        Some(path) => std::fs::write(path, rendered)?,
        None => println!("{rendered}"),
    }
    Ok(())
}
