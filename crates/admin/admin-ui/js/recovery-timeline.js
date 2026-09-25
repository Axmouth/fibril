/* Shared by the live dashboard and its read-only demo. Durations use the
 * recovery worker's monotonic clock; bars can overlap and must not be summed. */
(() => {
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;',
  })[c]);
  const number = value => Number.isFinite(Number(value)) ? Math.max(0, Number(value)) : 0;
  const duration = us => number(us) < 1000 ? `${Math.round(number(us))} µs`
    : number(us) < 1000000 ? `${(number(us) / 1000).toFixed(1)} ms` : `${(number(us) / 1000000).toFixed(2)} s`;
  const statuses = { running: 'Running', ok: 'Completed', error: 'Failed', cancelled: 'Cancelled' };
  const status = value => Object.hasOwn(statuses, value) ? value : 'cancelled';
  const labels = {
    witness_seal: 'Seal witness', inspect_source: 'Inspect source', compare_sources: 'Compare histories', compare_pair: 'Compare histories',
    commit_plan: 'Commit recovery plan', probe_snapshot: 'Probe snapshot', source_snapshot: 'Read snapshot',
    prepare_target: 'Prepare target', target_seal: 'Seal target', begin_target: 'Begin transfer',
    copy_pages: 'Copy suffix', finish_target: 'Finish transfer', install_target: 'Install state',
    activate: 'Activate assignment', admit: 'Admit replica',
  };
  const phases = [
    ['Witnesses', ['witness_seal']],
    ['Source', ['inspect_source', 'compare_sources', 'compare_pair', 'probe_snapshot', 'source_snapshot']],
    ['Plan', ['commit_plan']],
    ['Transfer', ['prepare_target', 'target_seal', 'begin_target', 'copy_pages', 'finish_target']],
    ['Install', ['install_target']],
    ['Activate', ['activate']],
    ['Admit', ['admit']],
  ];
  function phaseSummary(stages) {
    return '<ol class="recovery-phases" aria-label="Recovery phase observations">' + phases.map(([label, names]) => {
      const seen = stages.filter(s => names.includes(s.name));
      const kind = ['error', 'cancelled', 'running', 'ok'].find(k => seen.some(s => status(s.outcome) === k));
      return `<li class="recovery-${kind || 'unobserved'}"><strong>${label}</strong><span>${kind ? statuses[kind] : 'Not observed'}</span></li>`;
    }).join('') + '</ol>';
  }
  const resource = a => `${a.topic} / ${a.partition}${a.group ? ` / ${a.group}` : ''}`;
  const when = ms => {
    const date = new Date(Number(ms));
    return Number.isFinite(date.getTime()) ? date.toLocaleString() : 'Unknown time';
  };
  function budgetDetails(b) {
    if (!b) return '';
    const names = { inspection_pages: 'Inspection pages', inspection_records: 'Inspection records',
      inspection_bytes: 'Inspection bytes', snapshot_bytes: 'Snapshot bytes', replay_operations: 'Replay operations',
      artifact_bytes: 'Recovery artifact bytes', stage_records: 'Staging records', stage_bytes: 'Staging bytes',
      record_bytes: 'Record bytes per page', replica_count: 'Replica count' };
    return `<p><strong>${escape(names[b.budget] || b.budget)} limit reached.</strong> Limit ${escape(b.limit)} · accepted before this check ${escape(b.completed)} · next work refused ${escape(b.requested)}.</p>`
      + `<p>${b.unchanged_retry_can_help === false ? 'An unchanged retry cannot clear this limit. The history, available source or supported budget must change.' : 'Whether a retry can help is unknown.'} Counts describe this operation and do not certify complete recovery verification.</p>`;
  }
  function issues(a, stages) {
    const interrupted = stages.filter(s => ['error', 'cancelled'].includes(s.outcome));
    const rows = interrupted.map(s => `<p><strong class="recovery-${status(s.outcome)}">${statuses[status(s.outcome)]}:</strong> ${escape(labels[s.name] || s.name)}${s.peer ? ` on ${escape(s.peer)}` : ''} at +${duration(number(s.start_us) + number(s.elapsed_us))}.</p>${budgetDetails(s.budget)}`).join('');
    const terminal = ['error', 'cancelled'].includes(a.outcome);
    if (!rows && !terminal) return '';
    return `<div class="recovery-issues" role="note">${rows || `<p>Attempt ${statuses[status(a.outcome)].toLowerCase()}. No corresponding failed or cancelled stage was recorded.</p>`}${stages.some(s => s.budget) ? '' : budgetDetails(a.budget)}<p>Use the attempt identity to find detailed recovery logs.</p></div>`;
  }
  function detail(a) {
    const stages = Array.isArray(a.stages) ? a.stages : [];
    const extent = Math.max(1, number(a.elapsed_us), ...stages.map(s => number(s.start_us) + number(s.elapsed_us)));
    const rows = stages.map(s => {
      const start = Math.min(100, number(s.start_us) / extent * 100);
      const width = Math.min(100 - start, number(s.elapsed_us) / extent * 100);
      const kind = status(s.outcome);
      return `<div class="recovery-row"><div class="recovery-stage"><strong>${escape(labels[s.name] || s.name)}</strong><span>${escape(s.peer || 'controller')}</span></div>`
        + `<div class="recovery-track" aria-hidden="true"><span class="recovery-bar recovery-${kind}" style="left:${start}%;width:${width}%"></span></div>`
        + `<div class="recovery-measure"><strong>${duration(s.elapsed_us)}</strong><span>${statuses[kind]} · +${duration(s.start_us)}</span></div></div>`;
    }).join('');
    return `<div class="recovery-heading"><div><strong>${escape(resource(a))}</strong><p>Epoch ${escape(a.epoch)} · ${escape(when(a.started_at_ms))}</p></div>`
      + `<div class="recovery-total recovery-${status(a.outcome)}"><strong>${duration(a.elapsed_us)}</strong><span>${statuses[status(a.outcome)]}</span></div></div>`
      + phaseSummary(stages)
      + '<p class="recovery-legend">Phase order is a guide; work can overlap. Grey phases have no recorded stages: a retry may reuse work, or stop before reaching them.</p>'
      + issues(a, stages)
      + `<div class="recovery-axis"><span>Worker start · 0</span><span>${duration(extent)}</span></div>`
      + (rows || `<p class="settings-note">${a.outcome === 'running' ? 'Waiting for the first stage.' : 'No stages recorded for this attempt.'}</p>`)
      + (a.omitted_stages ? `<p class="settings-note">${escape(a.omitted_stages)} additional stages omitted by the recording limit.</p>` : '')
      + (a.labels_truncated ? '<p class="settings-note">Long resource or peer labels were shortened.</p>' : '')
      + `<details class="recovery-identity"><summary>Attempt identity</summary><p>Local attempt ${escape(a.id)} · transition <code>${escape(a.transition)}</code></p></details>`;
  }
  function render(root, snapshot, node) {
    if (!root) return;
    const attempts = snapshot?.attempts || [];
    if (!attempts.length) {
      root.textContent = snapshot ? 'No recovery attempts recorded on this node since it started.' : 'Recovery timelines are unavailable on this node.';
      root._recoveryReady = false;
      return;
    }
    if (!root._recoveryReady) {
      root.innerHTML = '<div class="recovery-controls"><label>Recent attempt <select aria-label="Recent recovery attempt"></select></label><span class="settings-note recovery-retention"></span></div><div class="recovery-detail"></div>';
      root._recoveryReady = true;
      root.querySelector('select').addEventListener('change', event => {
        root._recoverySelected = event.target.value;
        render(root, root._recoverySnapshot, root._recoveryNode);
      });
    }
    root._recoverySnapshot = snapshot;
    root._recoveryNode = node;
    const selected = attempts.find(a => String(a.id) === root._recoverySelected) || attempts[0];
    root._recoverySelected = String(selected.id);
    const select = root.querySelector('select');
    const options = attempts.map(a => `<option value="${escape(a.id)}">${escape(resource(a))} · ${escape(when(a.started_at_ms))} · ${statuses[status(a.outcome)]}</option>`).join('');
    if (select._options !== options) { select.innerHTML = options; select._options = options; }
    select.value = root._recoverySelected;
    root.querySelector('.recovery-retention').textContent = `Node ${node ?? 'local'} · ${attempts.length}/${snapshot.capacity} attempts · ${snapshot.stage_capacity} stages per attempt` + (snapshot.evicted_attempts ? ` · ${snapshot.evicted_attempts} older attempts evicted` : '');
    const target = root.querySelector('.recovery-detail');
    const expanded = target.querySelector('details')?.open;
    const html = detail(selected);
    if (target._html !== html) {
      target.innerHTML = html;
      target._html = html;
      target.querySelector('details').open = !!expanded;
    }
  }
  window.FibrilRecoveryTimeline = { render, detail, duration };
})();
