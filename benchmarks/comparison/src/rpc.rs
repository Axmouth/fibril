//! Two durable queues, application correlation IDs, reply-confirm-before-request-ACK.
use super::*;

fn delivered(
    stats: &mut Stats,
    stamp: Stamp,
    observed: u64,
    early: bool,
    args: &Args,
) -> Result<()> {
    stats.total += 1;
    stats.last_ns = stats.last_ns.max(observed);
    if args.measured(observed) {
        stats.completed_in_window += 1;
    }
    if early {
        stats.early_total += 1;
    }
    if args.measured(stamp.intended) {
        stats.measured += 1;
        stats.latency.record(observed - stamp.admitted)?;
        stats.scheduled.record(observed - stamp.intended)?;
        if early {
            stats.early_measured += 1;
        }
    }
    Ok(())
}

pub(super) async fn run(
    args: Arc<Args>,
    progress: Arc<Progress>,
    origin: Instant,
) -> Result<Value> {
    let mut requests =
        tokio::time::timeout(Duration::from_secs(60), adapter::prepare(&args)).await??;
    let mut reply_args = (*args).clone();
    reply_args.queue.push_str("_reply");
    let mut replies =
        tokio::time::timeout(Duration::from_secs(60), adapter::prepare(&reply_args)).await??;
    let settings = json!({"request":requests.settings,"reply":replies.settings});
    if args.setup_only {
        let request = requests.connections.flush().await?;
        let reply = replies.connections.flush().await?;
        return Ok(
            json!({"status":"setup_validated","config":*args,"adapter_settings":settings,
            "settlement":{"request":request,"reply":reply}}),
        );
    }
    let request_publisher = Arc::new(requests.publisher);
    let response_publisher = Arc::new(replies.publisher);
    let start = Instant::now() + Duration::from_millis(100);
    let finish = start + Duration::from_secs(args.warmup_secs + args.duration_secs);
    let request_credits = Arc::new(Semaphore::new(args.request_window));
    let (tx, rx) = mpsc::channel(args.confirm_window);
    let (done_tx, done_rx) = watch::channel(None::<u64>);
    let producer = tokio::spawn(issue(
        args.clone(),
        progress.clone(),
        request_publisher.clone(),
        start,
        finish,
        tx,
        done_tx,
        Arc::new(Semaphore::new(args.confirm_window)),
        Some(request_credits.clone()),
    ));
    let confirmer = tokio::spawn(collect_confirmations(
        args.clone(),
        progress.clone(),
        start,
        rx,
    ));
    let a = args.clone();
    let publisher = response_publisher.clone();
    let mut done = done_rx.clone();
    let service = tokio::spawn(async move {
        let mut processing = FuturesUnordered::new();
        let mut pending = FuturesUnordered::new();
        let outstanding_limit = if a.pipeline_replies {
            a.confirm_window
        } else {
            a.service_workers
        };
        let mut seen = Ids::default();
        let mut request_stats = Stats::default();
        let mut response_confirms = Stats::default();
        let mut expected = None;
        let mut acked = 0;
        let mut replies_issued = 0u64;
        let mut max_pending_replies = 0usize;
        let mut max_processing = 0usize;
        loop {
            if expected == Some(acked) {
                break;
            }
            tokio::select! {
                item = requests.deliveries.next(), if processing.len() < a.service_workers
                    && processing.len() + pending.len() < outstanding_limit
                    && expected != Some(request_stats.total) => {
                    let message = item.context("request consumer ended")??;
                    let stamp = Stamp::decode(message.payload(), a.payload_bytes)?;
                    seen.insert(stamp.id, a.max_messages)?;
                    message.check_offset(if a.connections == 1 { Some(stamp.id) } else { None })?;
                    if a.connections == 1 && matches!(a.broker, Broker::Fibril) {
                        ensure!(stamp.id == request_stats.total, "request delivery reordered: expected {}, got {}", request_stats.total, stamp.id);
                    }
                    delivered(&mut request_stats, stamp, ns(start), message.speculative(), &a)?;
                    let publisher = publisher.clone();
                    let a = a.clone();
                    processing.push(async move {
                        if a.processing_us > 0 {
                            tokio::time::sleep(Duration::from_micros(a.processing_us)).await;
                        }
                        let sent = ns(start);
                        // Responses can finish in a different order from requests.
                        // Their payload retains the application correlation ID.
                        let confirmation = publisher.send_response(stamp.encode(a.reply_bytes)).await?;
                        Ok::<_, anyhow::Error>((stamp, sent, confirmation, message))
                    });
                    max_processing = max_processing.max(processing.len());
                }
                Some(result) = processing.next(), if !processing.is_empty() => {
                    let (stamp, sent, confirmation, message) = result?;
                    // Retain each request until its own reply confirms. Other
                    // workers can prepare/send replies during that wait.
                    pending.push(async move {
                        confirmation.await?;
                        let confirmed = ns(start);
                        message.ack().await?;
                        Ok::<_, anyhow::Error>((stamp, sent, confirmed))
                    });
                    replies_issued += 1;
                    max_pending_replies = max_pending_replies.max(pending.len());
                }
                Some(result) = pending.next(), if !pending.is_empty() => {
                    let (stamp, sent, confirmed) = result?;
                    delivered(&mut response_confirms, Stamp { admitted: sent, ..stamp }, confirmed, false, &a)?;
                    acked += 1;
                }
                changed = done.changed(), if expected.is_none() => {
                    changed.context("request producer ended without count")?;
                    expected = *done.borrow_and_update();
                }
            }
        }
        seen.finish(expected.context("missing request count")?)?;
        ensure!(
            replies_issued == acked,
            "reply publication/ACK count mismatch"
        );
        Ok::<_, anyhow::Error>((
            request_stats,
            response_confirms,
            acked,
            json!({"enabled":a.pipeline_replies,"responses_issued":replies_issued,
                "max_pending_reply_confirm_and_ack":max_pending_replies,
                "max_processing":max_processing,"outstanding_limit":outstanding_limit}),
            requests.connections,
        ))
    });
    let a = args.clone();
    let p = progress.clone();
    let mut done = done_rx;
    let requester = tokio::spawn(async move {
        let mut stats = Stats::default();
        let mut seen = Ids::default();
        let mut expected = None;
        loop {
            if expected == Some(stats.total) {
                break;
            }
            tokio::select! {
                item = replies.deliveries.next() => {
                    let message = item.context("response consumer ended")??;
                    let stamp = Stamp::decode(message.payload(), a.reply_bytes)?;
                    seen.insert(stamp.id, a.max_messages)?;
                    delivered(&mut stats, stamp, ns(start), message.speculative(), &a)?;
                    p.delivered.store(stats.total, Ordering::Relaxed);
                    message.ack().await?;
                    p.ack_sent.store(stats.total, Ordering::Relaxed);
                    request_credits.add_permits(1);
                }
                changed = done.changed(), if expected.is_none() => {
                    changed.context("request producer ended without count")?;
                    expected = *done.borrow_and_update();
                }
            }
        }
        seen.finish(expected.context("missing response count")?)?;
        Ok::<_, anyhow::Error>((stats, replies.connections))
    });
    let _abort = AbortTasks(vec![
        producer.abort_handle(),
        confirmer.abort_handle(),
        service.abort_handle(),
        requester.abort_handle(),
    ]);
    let (
        producer,
        confirmer,
        (requests, response_confirms, request_acks, pipeline, mut request_connections),
        (roundtrip, mut reply_connections),
    ) = tokio::time::timeout_at(finish + Duration::from_secs(args.drain_secs), async {
        tokio::try_join!(
            async { producer.await? },
            async { confirmer.await? },
            async { service.await? },
            async { requester.await? }
        )
    })
    .await
    .context("RPC workload exceeded duration + drain timeout")??;
    for stats in [&confirmer, &requests, &response_confirms, &roundtrip] {
        ensure!(
            stats.total == producer.total && stats.measured == producer.measured,
            "RPC count/cohort mismatch"
        );
    }
    ensure!(
        producer.measured > 0 && request_acks == producer.total,
        "RPC requests not fully acknowledged"
    );
    let workload_done_ns = ns(start);
    let request_settlement =
        tokio::time::timeout(Duration::from_secs(30), request_connections.flush()).await??;
    let reply_settlement =
        tokio::time::timeout(Duration::from_secs(30), reply_connections.flush()).await??;
    drop(request_publisher);
    drop(response_publisher);
    let elapsed = ((confirmer
        .last_ns
        .max(roundtrip.last_ns)
        .max(response_confirms.last_ns) as f64
        / 1e9)
        .max((args.warmup_secs + args.duration_secs) as f64)
        - args.warmup_secs as f64)
        .max(1e-9);
    Ok(
        json!({"schema_version":1,"status":"client_validated","workload":"request_response","config":*args,
        "adapter_settings":settings,"publish":producer.json(),"confirm":confirmer.json(),"delivery":roundtrip.json(),
        "request_delivery":requests.json(),"response_confirmation":response_confirms.json(),
        "counts":progress.snapshot(),"rpc_counts":{"requests_acked":request_acks,"responses_confirmed":response_confirms.total},
        "rpc_pipeline":pipeline,
        "delivery_path":{"early_total":roundtrip.early_total,"early_measured":roundtrip.early_measured,
            "request_early_total":requests.early_total,"request_early_measured":requests.early_measured},
        "workload_start_since_setup_secs":start.duration_since(origin).as_secs_f64(),
        "workload_done_ns":workload_done_ns,"cohort_elapsed_including_drain_secs":elapsed,
        "cohort_completed_per_sec":producer.measured as f64/elapsed,
        "observed_delivery_per_sec":roundtrip.completed_in_window as f64/args.duration_secs as f64,
        "observed_confirm_per_sec":confirmer.completed_in_window as f64/args.duration_secs as f64,
        "settlement":{"request":request_settlement,"reply":reply_settlement},
        "note":"Delivery measures the complete request/reply round trip. Request ACK follows confirmed response publication. Runner must verify both queues."}),
    )
}
