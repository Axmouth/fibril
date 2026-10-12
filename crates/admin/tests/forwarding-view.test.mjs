import test from 'node:test';
import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import vm from 'node:vm';
const context = vm.createContext({ window: {} });
vm.runInContext(await readFile(new URL('../admin-ui/js/forwarding-view.js', import.meta.url), 'utf8'), context);
const view = context.window.FibrilForwarding;

const privateNode = { node_id: 'p', runtime: { visibility: 'private', upstream_sessions: 0, forwarded_sessions: 2 } };
const publicNode = { node_id: 'a', runtime: { visibility: 'public', upstream_sessions: 3, forwarded_sessions: 0 } };
const oldNode = { node_id: 'old' };

test('forwarded connections name the origin node and acting user', () => {
  assert.equal(view.forwardedVia({ forwarded: { origin_node: 'pub-1', acting_user: 'alice' } }), 'via node pub-1 for user alice');
  assert.equal(view.forwardedVia({ forwarded: { origin_node: 'pub-1', acting_user: '' } }), 'via node pub-1');
  assert.equal(view.forwardedVia({ forwarded: null }), '');
  assert.equal(view.forwardedVia({}), '');
});

test('private brokers get a badge, an internal admin link and no upstream count', () => {
  assert.match(view.badge(privateNode), />private</);
  assert.equal(view.badge(publicNode), '');
  assert.equal(view.badge(oldNode), '');
  assert.equal(view.adminLinkText(privateNode), 'open its admin (internal address)');
  assert.equal(view.adminLinkText(publicNode), 'open its admin');
  assert.equal(view.upstreamSessions(privateNode), null);
  assert.equal(view.upstreamSessions(publicNode), 3);
  assert.equal(view.upstreamSessions(oldNode), 0);
});

test('list rows show upstream sessions on public brokers and served sessions when present', () => {
  const pub = view.nodeRows(publicNode);
  assert.match(pub, /upstream sessions<\/span><b>3</);
  assert.doesNotMatch(pub, /forwarded sessions served/);
  const priv = view.nodeRows(privateNode);
  assert.doesNotMatch(priv, /upstream sessions/);
  assert.match(priv, /forwarded sessions served<\/span><b>2</);
  assert.deepEqual([...view.captionTags(privateNode)], ['private']);
  assert.deepEqual([...view.captionTags(publicNode)], ['3 upstream']);
  assert.deepEqual([...view.captionTags({ runtime: { upstream_sessions: 0 } })], []);
});

test('the gateway notice shows only while no public broker is live', () => {
  const html = view.gatewayNotice({ no_live_public_broker: true });
  assert.match(html, /No public broker is live/);
  assert.match(html, /empty owner endpoints/);
  assert.equal(view.gatewayNotice({ no_live_public_broker: false }), '');
  assert.equal(view.gatewayNotice(null), '');
});

test('counts are sanitized and markup is escaped', () => {
  assert.equal(view.upstreamSessions({ runtime: { upstream_sessions: 'junk' } }), 0);
  assert.equal(view.forwardedSessions({ runtime: { forwarded_sessions: -4 } }), 0);
  assert.doesNotMatch(view.nodeRows({ runtime: { upstream_sessions: '<b>' } }), /<b><b>/);
});
