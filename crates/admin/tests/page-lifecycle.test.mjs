import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFile } from 'node:fs/promises';
const lifecycle = await readFile(new URL('../admin-ui/js/page-lifecycle.js', import.meta.url), 'utf8');
const admin = await readFile(new URL('../admin-ui/js/admin.js', import.meta.url), 'utf8');
function setup() {
  let id = 0;
  const timers = new Map();
  const listeners = new Map();
  const document = { hidden: false, activeElement: null,
    addEventListener(name, fn) { if (!listeners.has(name)) listeners.set(name, new Set()); listeners.get(name).add(fn); },
    removeEventListener(name, fn) { listeners.get(name)?.delete(fn); } };
  const window = { scrollX: 0, scrollY: 0, scrollTo() { throw new Error('disposed refresher scrolled the new page'); },
    setTimeout(fn) { timers.set(++id, fn); return id; }, clearTimeout(id) { timers.delete(id); },
    setInterval(fn) { timers.set(++id, fn); return id; }, clearInterval(id) { timers.delete(id); } };
  const ctx = vm.createContext({ window, document, Date, console, URLSearchParams });
  vm.runInContext(lifecycle, ctx);
  ctx.setInterval = window.setInterval;
  ctx.setTimeout = window.setTimeout;
  vm.runInContext(admin, ctx);
  return { ctx, window, timers, listeners };
}
test('page swaps release interaction listeners, pollers, streams and visibility callbacks', () => {
  const f = setup();
  let closes = 0;
  for (let page = 0; page < 10; page++) {
    f.ctx.autoRefresh(() => {}, 1000);
    f.window.__spaEventSources.add({ close() { closes++; } });
    assert.equal(f.listeners.get('pointerdown').size, 1);
    assert.equal(f.window.__spaVisibilityKicks.size, 1);
    f.window.__disposePage();
    assert.equal(f.listeners.get('pointerdown').size, 0);
    assert.equal(f.timers.size, 0);
    assert.equal(f.window.__spaVisibilityKicks.size, 0);
  }
  assert.equal(closes, 10);
  assert.equal(f.listeners.get('visibilitychange').size, 1, 'shell listener stays singular');
});
test('completed and cancelled timers leave the resource registry', () => {
  const f = setup(); let value;
  const done = f.window.setTimeout(arg => { value = arg; }, 1, 42);
  const callback = f.timers.get(done); f.timers.delete(done); callback();
  assert.equal(value, 42);
  assert.equal(f.window.__spaTimeouts.size, 0);
  const cancelled = f.window.setTimeout(() => {}, 1);
  f.window.clearTimeout(cancelled);
  const interval = f.window.setInterval(() => {}, 1);
  f.window.clearInterval(interval);
  assert.equal(f.window.__spaTimeouts.size + f.window.__spaIntervals.size, 0);
  assert.equal(f.timers.size, 0);
});
test('a refresh finishing after navigation cannot scroll or restart the old poller', async () => {
  const f = setup(); let finish; let calls = 0;
  f.ctx.autoRefresh(() => { calls++; return new Promise(resolve => { finish = resolve; }); }, 1000);
  const tick = [...f.window.__spaVisibilityKicks][0];
  const pending = tick();
  f.window.__disposePage();
  f.window.scrollY = 500;
  finish(); await pending; await tick();
  assert.equal(calls, 1);
});

test('queued stream callbacks after navigation cannot repaint or restart polling', async () => {
  const f = setup(); let stream; let paints = 0; let polls = 0;
  f.ctx.EventSource = class {
    static CLOSED = 2;
    constructor() { stream = this; this.readyState = 2; }
    addEventListener(name, fn) { if (name === 'tick') this.tick = fn; }
    close() {}
  };
  f.ctx.liveData(['overview'], () => paints++, () => polls++);
  f.window.__disposePage();
  await Promise.resolve();
  stream.tick({ data: '{}' });
  stream.onerror();
  assert.equal(paints, 0);
  assert.equal(polls, 0);
  assert.equal(f.timers.size, 0);
});
