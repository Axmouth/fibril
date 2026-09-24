/* Synthetic examples only. These values are not measurements or performance claims. */
(() => {
  'use strict';
  const now = Date.now();
  const topics = ['orders.created', 'payments.pending', 'notifications.email', '_dlq.payments'];
  const queue = (topic, partition, ready, inflight) => ({
    topic, partition, group: null, role: 'owner', materialized: true,
    exists_on_disk: true, evicting: false, applied_upto: 42100 + partition,
    state: { ready_count: ready, inflight_count: inflight, delayed_count: 0,
      settled_until: 40000, next_offset: 40000 + ready + inflight, dlq_policy: topic.startsWith('_dlq') ? 'discard' : 'global',
      dlq_max_retries: 5, oldest_ready_age_ms: ready ? 1200 : 0 },
  });
  const queues = [queue(topics[0], 0, 48, 12), queue(topics[0], 1, 32, 8),
    queue(topics[1], 0, 120, 6), queue(topics[2], 0, 680, 0), queue(topics[3], 0, 3, 0)];
  const subscriptions = [topics[0], topics[0], topics[1]].map((topic, i) => ({
    topic, group: null, sub_id: `worker-${i + 1}`, conn_id: `client-${i + 1}`, auto_ack: false, uptime: 1800 + i * 60,
  }));
  const samples = Array.from({ length: 60 }, (_, i) => ({
    at: (now - (59 - i) * 30000) / 1000,
    published_per_sec: Math.round(700 + 140 * Math.sin(i / 5) + i * 3),
    delivered_per_sec: Math.round(690 + 120 * Math.sin(i / 5) + i * 3),
    backlog: Math.round(650 + 4 * i + 90 * Math.sin(i / 8)), inflight: 26,
    connections: 6, subscriptions: 3, rss_mb: 112 + i / 10,
    cpu: 8 + 3 * Math.sin(i / 4), disk_used: 96 * 1024 * 1024 + i * 65536,
  }));
  const assignments = queues.map(q => ({
    topic: q.topic, group: q.group, partition: q.partition, owner: 'broker-1',
    followers: ['broker-2', 'broker-3'], epoch: 12,
  }));
  const nodes = [1, 2, 3].map(n => ({ node_id: `broker-${n}`, raft_id: n,
    broker_addr: `broker-${n}.example:9876`, admin_addr: null, live: true,
    rate_pub: n === 1 ? 823 : 0, rate_dlv: n === 1 ? 810 : 0, draining: false,
    runtime: { version: 'demo', tls: 'off', started_at_ms: now - 7200000 },
  }));
  const runtime = {
    delivery: { inflight_ttl_ms: 30000, expiry_poll_min_ms: 100, expiry_batch_max: 1024, delivery_poll_max_ms: 1000 },
    idle_queue_cleanup: { enabled: true, evict_after_ms: 60000, sweep_interval_ms: 10000, publisher_idle_timeout_ms: 30000 },
    connection: { reconnect_grace_ms: 10000, drain_handoff_timeout_ms: null, resume_session_restart_ttl_ms: 60000 },
    replication: { agreed_checkpoint_interval_ms: 0, eager_failover: false, eager_failover_grace_ms: 1000, read_timeout_slack_ms: 1000, owner_connect_timeout_ms: 5000, confirm_timeout_ms: 10000, caught_up_poll_ms: 5, retry_poll_ms: 100,
      checkpoint_retry_poll_ms: 1000, max_messages_per_read: 4096, max_events_per_read: 8192,
      max_bytes_per_read: 4194304, max_iterations_per_tick: 16, min_in_sync_replicas: 2,
      isr_timeout_ms: 10000, stream_enabled: true, stream_apply_linger_us: 100,
      stream_apply_max_merge_bytes: 1048576, stream_buffer_batches: 64 },
    stream: { cursor_commit_window_us: 100, cursor_commit_max_batch: 1024, idle_evict_enabled: false, idle_evict_after_ms: 600000, idle_sweep_interval_ms: 60000 },
    partitioning: { default_partition_count: 1 }, consumer_groups: { default_target_per_consumer: null },
  };
  const fixtures = {
    overview: { broker: { published_per_sec_1m: 823, delivered_per_sec_1m: 810, acked_per_sec_1m: 806 },
      storage: {}, tcp: { resume_accepted_total: 4, resume_rejected_total: 0 },
      sys: { rss_mb: 118, cpu: 8.4 }, storage_used: 100 * 1024 * 1024,
      storage_breakdown: topics.map((topic, i) => ({ topic, group: null, partition: 0,
        message_bytes: (40 - i * 10) * 1024 * 1024, event_bytes: 0 })),
      stroma: { command: { per_lane: { high: { current_depth: 0, max_wait_ms: 0.12, max_process_ms: 0.08 },
        normal: { current_depth: 2, max_wait_ms: 0.24, max_process_ms: 0.13 } }, per_kind: {} },
        log: { event_log: { avg_append_latency_ms: 0.08, appends_per_sec_1m: 300, total_bytes: 1048576, total_appends: 12000, total_items: 42000 } },
        snapshot: { avg_total_ms: 0.9, attempts_total: 12, last_snapshot_size_bytes: 16384 },
        recovery: { queues_recovered: 5, total_events_replayed: 250, avg_startup_ms: 12 } } },
    history: { interval_ms: 30000, samples,
      queues: topics.map((topic, n) => ({ topic, group: null, samples: samples.map((s, i) => ({ at: s.at,
        depth: n === 2 ? 90 + 10 * i : Math.round(40 + 30 * (1 + Math.sin(i / 6 + n))),
        leased: n === 2 ? 0 : 8 + n, published: s.published_per_sec / 4, delivered: s.delivered_per_sec / 4 })) })) },
    attention: { items: [{ kind: 'queue_no_consumers', severity: 'warning', subject: 'notifications.email',
      headline: 'Messages waiting without a consumer', action_href: '/dashboard-demo/admin/queue/?topic=notifications.email' }] },
    queues_debug: { queues, broker_activity: topics.map(topic => ({ topic, group: null, active_publishers: 1, active_subscribers: subscriptions.filter(s => s.topic === topic).length, idle_for_ms: null, last_active_ms: now })), broker_activity_summary: { tracked_queue_count: 4, active_queue_count: 4, idle_queue_count: 0 }, replication_followers: [] },
    queues: { queues: Object.fromEntries(topics.map(topic => [topic, {}])) },
    subscriptions,
    cohorts: { cohorts: [{ topic: topics[0], group: null, consumer_group: 'order-workers', members: [
      { member: 'worker-1', partitions: [0], target: 1 }, { member: 'worker-2', partitions: [1], target: 1 },
    ] }] },
    topology: { coordination: { node_id: 'broker-1', generation: 12, nodes, assignments, stream_assignments: [
      { topic: 'orders.events', partition: 0, owner: 'broker-1', followers: ['broker-2', 'broker-3'], epoch: 12 },
    ] }, consensus: { leader: 1, voters: [1, 2, 3], term: 4, last_applied_index: 246, current_term: 4 } },
    streams: { streams: [{ topic: 'orders.events', partition: 0, head: 1000, tail: 8500,
      durability: 'durable', max_age_ms: 86400000, max_bytes: 268435456,
      live_subscriptions: 2, lag_evictions: 0, cursors: [['billing-projection', 8500], ['analytics', 8120]] }] },
    streams_debug: { queues: [{ topic: 'orders.events', partition: 0, role: 'owner', materialized: true, applied_upto: 8500 }], replication_followers: [] },
    connections: Array.from({ length: 6 }, (_, i) => ({ id: `client-${i + 1}`,
      peer: `worker-${i + 1}.example:45000`, authenticated: true, published: 42000 + i * 100,
      subs: subscriptions.filter(s => s.conn_id === `client-${i + 1}`).length, uptime: 1800 + i * 60 })),
    audit: { entries: [
      { at_ms: now - 60000, severity: 'warning', kind: 'attention_raised', subject: 'notifications.email', detail: 'Example: messages waiting while the notification worker is paused.' },
      { at_ms: now - 180000, severity: 'info', kind: 'queue_declared', subject: 'orders.created', detail: 'Example: two queue partitions declared.' },
    ] },
    quarantine: { quarantined: [], policy: 'fail' },
    'runtime-settings': { version: 3, settings: runtime, locks: {}, load_issue: null },
    'local-storage-settings': { node_id: 'broker-1', version: 2, startup_preallocate_bytes: 0,
      override_preallocate_bytes: 1048576, requested_preallocate_bytes: 1048576, pending_logs: 1, failed_logs: 0,
      logs: [{ path: 'messages/orders.created/0', applied_revision: 2, active_segment_base: 40000,
        configured_preallocate_bytes: 1048576, effective_preallocate_bytes: 1048576, allocation_error: null },
        { path: 'events/orders.created/0', applied_revision: 1, active_segment_base: 0,
          configured_preallocate_bytes: 0, effective_preallocate_bytes: 0, allocation_error: null }] },
    'startup-config': { data_dir: '/example/fibril-data', broker_bind: '0.0.0.0:9876', admin_bind: '0.0.0.0:8081',
      tls_status: 'disabled (sample configuration)', admin_auth_enabled: false,
      keratin_fsync_interval_ms: 5, keratin_min_fsync_interval_ms: 0, keratin_batch_linger_ms: 5, keratin_tail_cache_bytes: 67108864, keratin_segment_preallocate_bytes: 0,
      keratin_max_inflight_fsyncs: 8, keratin_pipeline_commit_records: 2048,
      keratin_writer_buffer_factor: 16, keratin_adaptive_staging: true,
      keratin_staging_decay_secs: 10, keratin_staging_idle_release_secs: 60,
      keratin_message_log_segment_max_bytes: 268435456, keratin_event_log_segment_max_bytes: 33554432,
      coordination_heartbeat_interval_ms: 3000, coordination_liveness_ttl_ms: 9000 },
    'global-dlq': { version: 1, target: { topic: '_dlq.payments', group: null } },
    users: [{ username: 'demo-operator', created_ms: now - 86400000, updated_ms: now - 86400000 }],
    tls: null,
  };
  const latest = samples.at(-1);
  latest.backlog = queues.reduce((sum, q) => sum + q.state.ready_count, 0);
  latest.inflight = queues.reduce((sum, q) => sum + q.state.inflight_count, 0);
  latest.published_per_sec = fixtures.overview.broker.published_per_sec_1m;
  latest.delivered_per_sec = fixtures.overview.broker.delivered_per_sec_1m;
  latest.rss_mb = fixtures.overview.sys.rss_mb;
  latest.cpu = fixtures.overview.sys.cpu;
  latest.disk_used = fixtures.overview.storage_used;
  for (const series of fixtures.history.queues) {
    const parts = queues.filter(q => q.topic === series.topic);
    series.samples.at(-1).depth = parts.reduce((sum, q) => sum + q.state.ready_count, 0);
    series.samples.at(-1).leased = parts.reduce((sum, q) => sum + q.state.inflight_count, 0);
  }
  for (let i = 0; i < samples.length; i++) {
    samples[i].backlog = fixtures.history.queues.reduce((sum, q) => sum + q.samples[i].depth, 0);
    samples[i].inflight = fixtures.history.queues.reduce((sum, q) => sum + q.samples[i].leased, 0);
  }
  window.fibrilDemoResponse = url => {
    const key = url.pathname.slice('/admin/api/'.length);
    if (key === 'messages') {
      const from = Math.max(0, Number(url.searchParams.get('from') || 0));
      const limit = Math.min(5000, Math.max(1, Number(url.searchParams.get('limit') || 50)));
      const topic = url.searchParams.get('topic'), partition = Number(url.searchParams.get('partition') || 0);
      const group = url.searchParams.get('group') || null;
      const q = queues.find(q => q.topic === topic && q.partition === partition && q.group === group);
      const statuses = (url.searchParams.get('status') || '').split(',').filter(Boolean);
      const includeSettled = url.searchParams.get('include_settled') === 'true';
      const includePayload = url.searchParams.get('include_payload') === 'true';
      const payloadLimit = Math.max(1, Number(url.searchParams.get('payload_limit_bytes') || 4096));
      const items = [];
      let offset = Math.max(from, includeSettled ? 0 : (q?.state.settled_until || 0));
      const end = q?.state.next_offset || 0;
      for (; offset < end && items.length < limit; offset++) {
        const status = offset < q.state.settled_until ? 'settled'
          : offset < q.state.settled_until + q.state.inflight_count ? 'inflight' : 'ready';
        if (statuses.length && !statuses.includes(status)) continue;
        const payload = JSON.stringify({ order_id: `sample-${offset}`, status: 'received', example: true });
        items.push({ state: { offset, status, retry_count: 0 }, payload_len: payload.length,
          headers: includePayload ? { content_type: 'Json', extra: { example: 'true' } } : null,
          payload_base64: includePayload ? btoa(payload.slice(0, payloadLimit)) : null,
          payload_truncated: includePayload && payload.length > payloadLimit });
      }
      return { items, next_offset_hint: offset < end ? offset : null, topic, partition, group };

    }
    return fixtures[key] === undefined ? undefined : structuredClone(fixtures[key]);
  };
})();
