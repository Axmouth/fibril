//! Process-failure probes use payload correlation IDs and permit documented redelivery.
use crate::{Args, adapter, metrics::Stamp};
use anyhow::{Context, Result, ensure};
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
fn now() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
fn phase(args: &Args, value: &Value) -> Result<()> {
    let path = args
        .phase_file
        .as_ref()
        .context("fault phase-file required")?;
    let pending = path.with_extension("pending");
    std::fs::write(&pending, serde_json::to_vec_pretty(value)?)?;
    std::fs::rename(pending, path)?;
    Ok(())
}
pub async fn run(args: &Args) -> Result<Value> {
    let started = now();
    tokio::time::timeout(
        Duration::from_secs(args.drain_secs),
        run_inner(args, started),
    )
    .await?
}
async fn run_inner(args: &Args, started: u128) -> Result<Value> {
    let role = args.fault_role.as_deref().unwrap();
    let mut a = args.clone();
    a.allow_existing = true;
    a.connections = 1;
    a.setup_only = role == "publish";
    let mut prepared = adapter::prepare(&a).await?;
    let mut deliveries = BTreeMap::<u64, u64>::new();
    let mut confirmed = Vec::new();
    let mut acked = 0;
    let mut first_received_ns = None;
    match role {
        "publish" => {
            let mut pending = futures::stream::FuturesOrdered::new();
            for id in args.fault_start..args.fault_start + args.fault_count {
                let confirmation = prepared
                    .publisher
                    .send_response(
                        Stamp {
                            id,
                            intended: 0,
                            admitted: 0,
                        }
                        .encode(args.payload_bytes),
                    )
                    .await?;
                pending.push_back(async move {
                    confirmation.await?;
                    Ok::<u64, anyhow::Error>(id)
                });
                if pending.len() >= args.confirm_window {
                    confirmed.push(
                        pending
                            .next()
                            .await
                            .context("confirmation window empty")??,
                    );
                }
            }
            while let Some(result) = pending.next().await {
                confirmed.push(result?);
            }
        }
        "worker" | "worker-pause" => {
            let mut reply = a.clone();
            reply.queue = format!("{}_reply", args.queue);
            reply.setup_only = true;
            let mut replies = adapter::prepare(&reply).await?;
            for _ in 0..args.fault_count {
                let message = prepared
                    .deliveries
                    .next()
                    .await
                    .context("request stream ended")??;
                let stamp = Stamp::decode(message.payload(), args.payload_bytes)?;
                first_received_ns.get_or_insert(now());
                *deliveries.entry(stamp.id).or_default() += 1;
                replies
                    .publisher
                    .send_response(stamp.encode(args.reply_bytes))
                    .await?
                    .await?;
                confirmed.push(stamp.id);
                if role == "worker-pause" {
                    phase(
                        args,
                        &json!({"phase":"reply_confirmed_request_unacked", "id":stamp.id,"unix_ns":now()}),
                    )?;
                    futures::future::pending::<()>().await;
                }
                message.ack().await?;
                acked += 1;
            }
            replies.connections.close_only().await?;
        }
        "consume" | "requester-pause" => {
            for _ in 0..args.fault_count {
                let message = prepared
                    .deliveries
                    .next()
                    .await
                    .context("reply/data stream ended")??;
                let stamp = Stamp::decode(message.payload(), args.payload_bytes)?;
                first_received_ns.get_or_insert(now());
                *deliveries.entry(stamp.id).or_default() += 1;
                if role == "requester-pause" {
                    phase(
                        args,
                        &json!({"phase":"reply_received_unacked","id":stamp.id,"unix_ns":now()}),
                    )?;
                    futures::future::pending::<()>().await;
                }
                message.ack().await?;
                acked += 1;
            }
        }
        _ => anyhow::bail!("unknown fault role: {role}"),
    }
    ensure!(args.fault_count > 0, "empty fault workload");
    prepared.connections.close_only().await?;
    Ok(
        json!({"status":"fault_client_complete","role":role,"config":args,"started_unix_ns":started,
        "first_received_unix_ns":first_received_ns,"finished_unix_ns":now(),"confirmed_ids":confirmed,
        "delivery_counts":deliveries,"acked":acked}),
    )
}
