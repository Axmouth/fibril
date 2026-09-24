import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
import { pages, renderTemplate } from './build.mjs';

const root = new URL('../public/dashboard-demo/', import.meta.url);
test('every generated view uses shared templates and has no unresolved syntax', async () => {
  const layout = await readFile(new URL('../../crates/admin/templates/layout.html', import.meta.url), 'utf8');
  for (const page of pages) {
    const body = await readFile(new URL(`../../crates/admin/templates/pages/${page}.html`, import.meta.url), 'utf8');
    const html = await readFile(new URL(page === 'overview' ? 'index.html' : `admin/${page}/index.html`, root), 'utf8');
    assert.equal(html, renderTemplate(layout, body, page));
    assert(!/\{%|\{\{/.test(html));
    assert(html.indexOf('transport.js') < html.indexOf('window.localStorage'));
    assert(!html.includes('href="/admin/'));
  }
  assert.throws(() => renderTemplate('{{ unknown }}', '{% extends "layout.html" %}{% block content %}x{% endblock %}', 'overview'));
});

test('fixture transport rejects writes, unknown APIs and external requests without network fallback', async () => {
  const requested = [];
  const context = vm.createContext({ URL, Response, structuredClone, btoa, console,
    location: new URL('http://demo.test/dashboard-demo/'),
    document: { addEventListener() {} },
    window: { addEventListener() {}, fetch: async (...args) => { requested.push(args); return new Response('static'); } },
  });
  vm.runInContext(await readFile(new URL('fixtures.js', import.meta.url), 'utf8'), context);
  vm.runInContext(await readFile(new URL('transport.js', import.meta.url), 'utf8'), context);
  const fetch = context.window.fetch;
  for (const method of ['POST', 'PUT', 'DELETE', 'PATCH']) {
    assert.equal((await fetch('/admin/api/queues', { method })).status, 403);
  }
  assert.equal((await fetch('https://broker.example/admin/api/overview')).status, 403);
  const before = await (await fetch('/admin/api/queues_debug')).json();
  assert(before.queues.length > 0);
  assert.equal((await fetch('/admin/api/unknown')).status, 404);
  assert.equal((await fetch('/admin/queues')).status, 403);
  assert.equal(requested.length, 0);
  await fetch('/dashboard-demo/static/css/admin.css');
  assert.equal(requested.length, 1);
  assert.equal(context.window.EventSource, undefined);
  assert.equal((await (await fetch('/admin/api/startup-config')).json()).keratin_adaptive_staging, true);
  const messages = await (await fetch('/admin/api/messages?topic=orders.created&from=40012&limit=1')).json();
  assert.equal(messages.items.length, 1); assert.equal(messages.items[0].state.offset, 40012);
});

test('sample inventory, history, subscription and topology views agree', async () => {
  const context = vm.createContext({ window: {}, structuredClone, btoa });
  vm.runInContext(await readFile(new URL('fixtures.js', import.meta.url), 'utf8'), context);
  const get = name => context.window.fibrilDemoResponse(new URL(`http://demo.test/admin/api/${name}`));
  const queues = get('queues_debug').queues, history = get('history'), subs = get('subscriptions');
  const conns = get('connections'), topology = get('topology'), overview = get('overview');
  assert.equal(history.samples.at(-1).backlog, queues.reduce((n, q) => n + q.state.ready_count, 0));
  assert.equal(history.samples.at(-1).connections, conns.length);
  assert.equal(history.samples.at(-1).subscriptions, subs.length);
  assert.equal(overview.storage_used, overview.storage_breakdown.reduce((n, q) => n + q.message_bytes + q.event_bytes, 0));
  for (const [i, sample] of history.samples.entries()) {
    assert.equal(sample.backlog, history.queues.reduce((n, q) => n + q.samples[i].depth, 0));
    assert.equal(sample.inflight, history.queues.reduce((n, q) => n + q.samples[i].leased, 0));
  }
  for (const stream of get('streams_debug').queues) {
    const assignment = topology.coordination.stream_assignments.find(a => a.topic === stream.topic && a.partition === stream.partition);
    assert.equal(assignment.owner, topology.coordination.node_id);
  }
  for (const sub of subs) assert(conns.some(c => c.id === sub.conn_id));
  for (const conn of conns) assert.equal(conn.subs, subs.filter(s => s.conn_id === conn.id).length);
  for (const q of queues) {
    const assignment = topology.coordination.assignments.find(a => a.topic === q.topic && a.partition === q.partition);
    assert.equal(assignment.owner, topology.coordination.node_id);
    assert(assignment.followers.every(id => topology.coordination.nodes.some(n => n.node_id === id)));
  }
});


test('inspection respects partition, status, pagination and payload selection', async () => {
  const context = vm.createContext({ window: {}, structuredClone, btoa });
  vm.runInContext(await readFile(new URL('fixtures.js', import.meta.url), 'utf8'), context);
  const get = query => context.window.fibrilDemoResponse(new URL(`http://demo.test/admin/api/messages?topic=orders.created&${query}`));
  const first = get('limit=2&status=ready');
  assert.equal(first.items.length, 2);
  assert.equal(first.items[0].state.offset, 40012);
  assert.equal(first.items[0].payload_base64, null);
  const next = get(`limit=2&status=ready&from=${first.next_offset_hint}`);
  assert(next.items[0].state.offset > first.items.at(-1).state.offset);
  assert.equal(get('partition=99').items.length, 0);
  assert.equal(get('status=delayed').items.length, 0);
  const preview = get('limit=1&include_payload=true&payload_limit_bytes=8');
  assert.equal(preview.items[0].payload_truncated, true);
  assert.equal(atob(preview.items[0].payload_base64).length, 8);
  assert.equal(get('limit=1&include_settled=true').items[0].state.status, 'settled');
});

test('embedded views disable navigation while keeping data reads and local controls', async () => {
  const listeners = new Map(), attributes = new Map([['href', '/dashboard-demo/admin/streams/']]);
  const anchor = { dataset: {}, getAttribute: key => attributes.get(key),
    removeAttribute: key => attributes.delete(key), setAttribute: (key, value) => attributes.set(key, value) };
  const document = { documentElement: { dataset: {} },
    addEventListener: (type, handler) => listeners.set(type, handler),
    querySelectorAll: selector => selector === 'a[href]' && attributes.has('href') ? [anchor] : [],
    getElementById: () => null };
  const requested = [];
  const context = vm.createContext({ URL, Response, structuredClone, btoa, console, document,
    location: new URL('http://demo.test/dashboard-demo/admin/queue/?topic=orders.created&embed=1'),
    MutationObserver: class { observe() {} disconnect() {} },
    window: { addEventListener() {}, fetch: async (...args) => { requested.push(args); return new Response('static'); } } });
  vm.runInContext(await readFile(new URL('fixtures.js', import.meta.url), 'utf8'), context);
  vm.runInContext(await readFile(new URL('transport.js', import.meta.url), 'utf8'), context);
  listeners.get('DOMContentLoaded')();
  assert.equal(document.documentElement.dataset.demoEmbed, 'true');
  assert.equal(attributes.has('href'), false);
  assert.equal(attributes.get('aria-disabled'), 'true');
  assert.equal(attributes.get('tabindex'), '-1');
  for (const type of ['click', 'auxclick', 'keydown']) {
    let prevented = false, stopped = false;
    listeners.get(type)({ target: { closest: selector => selector === 'a' ? anchor : null },
      ctrlKey: true, key: 'k', preventDefault: () => { prevented = true; },
      stopImmediatePropagation: () => { stopped = true; } });
    assert(prevented && stopped, type);
  }
  let blocked = false;
  listeners.get('click')({ target: { closest: () => null },
    preventDefault: () => { blocked = true; }, stopImmediatePropagation() {} });
  assert.equal(blocked, false, 'ordinary local controls remain interactive');
  assert.equal((await context.window.fetch('/dashboard-demo/admin/streams/')).status, 403);
  assert.equal(requested.length, 0);
  assert.equal((await context.window.fetch('/admin/api/queues_debug')).status, 200);
  await context.window.fetch('/dashboard-demo/static/js/admin.js');
  assert.equal(requested.length, 1);
});

test('focused recovery embeds are opt-in and restricted to the topology page', async () => {
  const source = await readFile(new URL('transport.js', import.meta.url), 'utf8');
  for (const [path, panel] of [
    ['/admin/topology/?embed=1&panel=recovery', 'recovery'],
    ['/admin/topology/?panel=recovery', undefined],
    ['/admin/messages/?embed=1&panel=recovery', undefined],
    ['/admin/topology/?embed=1&panel=unknown', undefined],
  ]) {
    const context = vm.createContext({ URL, location: new URL(`http://demo.test/dashboard-demo${path}`),
      document: { documentElement: { dataset: {} }, addEventListener() {} },
      window: { fetch() {} } });
    vm.runInContext(source, context);
    assert.equal(context.document.documentElement.dataset.demoPanel, panel);
  }
});
