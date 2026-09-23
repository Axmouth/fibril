import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';

const template = await readFile(new URL('../templates/pages/settings.html', import.meta.url), 'utf8');
const fixture = await readFile(new URL('../../../website/demo/fixtures.js', import.meta.url), 'utf8');

// Execute the production form functions with DOM-shaped controls. Startup
// network requests and unrelated user/DLQ forms are outside these unit tests.
function form() {
  const controls = new Map();
  for (const match of template.matchAll(/<(input|select|span|div)[^>]*\bid="([^"]+)"[^>]*>/g)) {
    const tag = match[0];
    controls.set(match[2], { id: match[2], _value: '', get value() { return this._value; },
      set value(value) { this._value = String(value); }, checked: false, disabled: false,
      min: /min="([^"]+)"/.exec(tag)?.[1] || '',
      type: /type="([^"]+)"/.exec(tag)?.[1] || '', dataset: {}, textContent: '',
      runtime: tag.includes('data-runtime-setting'), optional: tag.includes('data-runtime-optional'),
      hasAttribute(name) { return name === 'data-runtime-optional' && this.optional; },
      addEventListener() {},
    });
  }
  const context = vm.createContext({ structuredClone, btoa, window: {},
    document: { getElementById: id => controls.get(id),
      querySelectorAll: selector => selector === '[data-runtime-setting]'
        ? [...controls.values()].filter(el => el.runtime)
        : selector === '#idle-section input, #idle-section select'
          ? [...controls.values()].filter(el => el.id.startsWith('idle_queue_cleanup.')) : [] },
  });
  vm.runInContext(fixture, context);
  const data = context.window.fibrilDemoResponse(new URL('http://demo.test/admin/api/runtime-settings'));
  Object.assign(data.settings.connection, { drain_handoff_timeout_ms: 12345, resume_session_restart_ttl_ms: 87654 });
  Object.assign(data.settings.replication, { read_timeout_slack_ms: 321, owner_connect_timeout_ms: 456 });
  data.settings.stream = { cursor_commit_window_us: 17, cursor_commit_max_batch: 23,
    idle_evict_enabled: true, idle_evict_after_ms: 42000, idle_sweep_interval_ms: 1234 };
  const script = /<script>([\s\S]*?)<\/script>/.exec(template)[1];
  vm.runInContext(script.split('field("settings-form").addEventListener')[0], context);
  return { context, data, controls, collect: () => JSON.parse(JSON.stringify(context.collectSettings())) };
}

test('editing one setting preserves every other setting, including unrepresented fields', () => {
  const f = form();
  f.data.settings.connection.future_timeout_ms = 555;
  f.data.settings.future_group = { enabled: true };
  f.context.renderSettings(f.data);
  f.controls.get('delivery.expiry_batch_max').value = '19';
  const expected = structuredClone(f.data.settings);
  expected.delivery.expiry_batch_max = 19;
  assert.deepEqual(f.collect(), expected);
  assert.equal(f.data.settings.delivery.expiry_batch_max, 1024, 'editing must not mutate the loaded snapshot');
});

test('reload/conflict replaces the base snapshot without retaining a stale hidden value', () => {
  const f = form();
  f.context.renderSettings(f.data);
  f.data.settings.connection.future_timeout_ms = 987;
  f.data.version++;
  f.context.renderSettings(f.data);
  assert.deepEqual(f.collect(), f.data.settings);
});

test('optional fields preserve null and explicit zero; duration edits use selected units', () => {
  const f = form();
  f.data.settings.connection.reconnect_grace_ms = null;
  f.context.renderSettings(f.data);
  assert.equal(f.collect().connection.reconnect_grace_ms, null);
  f.controls.get('connection.reconnect_grace_ms').value = '0';
  assert.equal(f.collect().connection.reconnect_grace_ms, 0);
  f.controls.get('delivery.inflight_ttl_ms').value = '1.5';
  f.controls.get('delivery.inflight_ttl_ms.unit').value = 's';
  assert.equal(f.collect().delivery.inflight_ttl_ms, 1500);
});

test('unsupported and locked controls cannot overwrite the loaded document', () => {
  const f = form();
  delete f.data.settings.stream;
  f.data.locks.idle_queue_cleanup = true;
  f.context.renderSettings(f.data);
  assert.equal(f.controls.get('stream.cursor_commit_max_batch').disabled, true);
  f.controls.get('stream.cursor_commit_max_batch').value = '99';
  // Locked controls retain their original values even if modified programmatically.
  assert.equal(f.controls.get('idle_queue_cleanup.enabled').disabled, true);
  f.controls.get('idle_queue_cleanup.enabled').checked = false;
  assert.deepEqual(f.collect(), f.data.settings);
});

test('all newly exposed controls save edits using their actual form types', () => {
  const f = form();
  f.context.renderSettings(f.data);
  for (const key of ['connection.drain_handoff_timeout_ms', 'connection.resume_session_restart_ttl_ms',
    'replication.read_timeout_slack_ms', 'replication.owner_connect_timeout_ms',
    'stream.idle_evict_after_ms', 'stream.idle_sweep_interval_ms']) {
    f.controls.get(key).value = '2';
    f.controls.get(`${key}.unit`).value = 's';
  }
  f.controls.get('stream.idle_evict_enabled').checked = false;
  f.controls.get('stream.cursor_commit_window_us').value = '0';
  f.controls.get('stream.cursor_commit_max_batch').value = '11';
  const saved = f.collect();
  assert.deepEqual(saved.stream, { cursor_commit_window_us: 0, cursor_commit_max_batch: 11,
    idle_evict_enabled: false, idle_evict_after_ms: 2000, idle_sweep_interval_ms: 2000 });
  assert.equal(saved.connection.drain_handoff_timeout_ms, 2000);
  assert.equal(saved.connection.resume_session_restart_ttl_ms, 2000);
  assert.equal(saved.replication.read_timeout_slack_ms, 2000);
  assert.equal(saved.replication.owner_connect_timeout_ms, 2000);
});

test('saving before a successful load is rejected', () => {
  assert.throws(() => form().collect(), /Load settings before saving/);
});

test('changing duration units retains millisecond precision and validation limits', () => {
  const f = form();
  f.context.renderSettings(f.data);
  f.context.renderDuration('replication.owner_connect_timeout_ms', 5, 's');
  const input = f.controls.get('replication.owner_connect_timeout_ms');
  assert.equal(input.value, '0.005');
  assert.equal(input.min, '0.001');
  assert.equal(input.step, '0.001');
  assert.equal(f.collect().replication.owner_connect_timeout_ms, 5);
});

test('save submits the loaded version and full document; conflict reloads without an automatic retry', async () => {
  const f = form();
  f.context.renderSettings(f.data);
  const requests = [];
  const conflict = structuredClone(f.data);
  conflict.version++;
  conflict.settings.replication.owner_connect_timeout_ms = 777;
  f.context.toast = () => {};
  f.context.fetch = async (url, options) => {
    requests.push({ url, body: JSON.parse(options.body) });
    return { status: 409, ok: false, json: async () => conflict };
  };
  await f.context.saveSettings({ preventDefault() {} });
  assert.equal(requests.length, 1);
  assert.equal(requests[0].body.expected_version, f.data.version);
  assert.deepEqual(requests[0].body.settings, f.data.settings);
  assert.equal(f.controls.get('version').value, String(conflict.version));
  assert.deepEqual(f.collect(), conflict.settings);
});
