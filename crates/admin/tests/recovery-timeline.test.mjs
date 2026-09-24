import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
const context = vm.createContext({ window: {}, structuredClone, btoa });
vm.runInContext(await readFile(new URL('../admin-ui/js/recovery-timeline.js', import.meta.url), 'utf8'), context);
vm.runInContext(await readFile(new URL('../../../website/demo/fixtures.js', import.meta.url), 'utf8'), context);
const { detail, duration, render } = context.window.FibrilRecoveryTimeline;
const snapshot = context.window.fibrilDemoResponse(new URL('http://demo.test/admin/api/topology')).consensus.recovery_timeline;

test('shared demo timelines have bounded stages, valid intervals and retry identity', () => {
  assert.equal(snapshot.scope, 'local_process');
  assert(snapshot.attempts.length <= snapshot.capacity);
  assert.equal(snapshot.attempts[0].transition, snapshot.attempts[1].transition);
  assert.notEqual(snapshot.attempts[0].id, snapshot.attempts[1].id);
  for (const a of snapshot.attempts) {
    assert(a.stages.length <= snapshot.stage_capacity);
    assert.equal(new Set(a.stages.map(s => s.sequence)).size, a.stages.length);
    for (const s of a.stages) assert(s.start_us + s.elapsed_us <= a.elapsed_us);
    const html = detail(a);
    assert(!html.includes('NaN'));
    assert(html.includes(a.outcome === 'ok' ? 'Completed' : a.outcome === 'error' ? 'Failed' : 'Cancelled'));
    assert(html.includes('Attempt identity'));
  }
});

test('overlapping stages use elapsed position rather than cumulative durations', () => {
  const html = detail({ ...snapshot.attempts[0], elapsed_us: 1000, stages: [
    { name: 'prepare_target', peer: 'a', start_us: 0, elapsed_us: 1000, outcome: 'ok' },
    { name: 'copy_pages', peer: 'a', start_us: 250, elapsed_us: 500, outcome: 'error' },
    { name: 'install_target', peer: 'b', start_us: 750, elapsed_us: 0, outcome: 'cancelled' },
  ] });
  assert(html.includes('left:0%;width:100%'));
  assert(html.includes('left:25%;width:50%'));
  assert(html.includes('left:75%;width:0%'));
  assert(html.includes('Failed · +250 µs'));
  assert(html.includes('Cancelled'));
});

test('resource labels, identities and unknown stage names are escaped', () => {
  const attack = '<img src=x onerror="alert(1)">';
  const html = detail({ ...snapshot.attempts[0], id: attack, topic: attack, group: attack,
    epoch: attack, transition: attack, outcome: attack, omitted_stages: 9, labels_truncated: true,
    stages: [{ name: attack, peer: attack, start_us: 'NaN', elapsed_us: Infinity, outcome: attack }] });
  assert(!html.includes('<img'));
  assert(!html.includes('NaN'));
  assert(!html.includes('Infinity'));
  assert(html.includes('&lt;img'));
  assert(html.includes('9 additional stages omitted'));
  assert(html.includes('labels were shortened'));
  assert.equal(duration(-1), '0 µs');
  assert.equal(duration(1200000), '1.20 s');
});

// DOM-shaped controls exercise state across polling without a browser dependency.
function root() {
  const select = { innerHTML: '', addEventListener(type, fn) { this.change = fn; } };
  const identity = { open: false };
  const body = { innerHTML: '', querySelector: () => identity };
  const retention = {};
  return { innerHTML: '', textContent: '', select, body, identity, retention,
    querySelector: key => key === 'select' ? select : key === '.recovery-detail' ? body : retention };
}
test('selection and expanded identity survive polling; eviction falls back to latest', () => {
  const r = root();
  render(r, snapshot, 'broker-1');
  r.select.change({ target: { value: '1' } });
  r.identity.open = true;
  render(r, structuredClone(snapshot), 'broker-1');
  assert.equal(r.select.value, '1');
  assert.equal(r.identity.open, true);
  assert(r.body.innerHTML.includes('Failed'));
  render(r, { ...snapshot, attempts: [snapshot.attempts[0]], evicted_attempts: 1 }, 'broker-1');
  assert.equal(r.select.value, '2');
  assert(r.retention.textContent.includes('1 older attempts evicted'));
  render(r, { ...snapshot, attempts: [] }, 'broker-1');
  assert(r.textContent.includes('since it started'));
  render(r, null, 'broker-1');
  assert(r.textContent.includes('unavailable'));
});

test('failed and cancelled attempts distinguish observed failures from absent phases', () => {
  const failed = detail(snapshot.attempts[1]);
  assert(failed.includes('Failed:</strong> Seal witness on broker-3'));
  assert(failed.includes('recovery-unobserved"><strong>Activate</strong><span>Not observed'));
  assert(!failed.includes('Skipped'));
  const cancelled = detail(snapshot.attempts[2]);
  assert(cancelled.includes('Cancelled:</strong> Inspect source on broker-2'));
  assert(cancelled.includes('recovery-cancelled'));
  assert(cancelled.includes('retry may reuse work'));
});

test('failure outside a recorded stage stays explicit without inventing a failed step', () => {
  const html = detail({ ...snapshot.attempts[1], stages: [] });
  assert(html.includes('Attempt failed; no corresponding failed or cancelled stage was recorded.'));
  assert(html.includes('No stages recorded for this attempt.'));
  assert(!html.includes('Waiting for the first stage.'));
});
