import test from 'node:test';
import assert from 'node:assert/strict';
import vm from 'node:vm';
import { readFile } from 'node:fs/promises';
const source = await readFile(new URL('../admin-ui/js/navigation.js', import.meta.url), 'utf8');
function deferred() {
  let resolve, reject;
  const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}
const tick = () => new Promise(resolve => setImmediate(resolve));
function setup() {
  const requests = [], history = [], scripts = [], ran = [], listeners = {};
  let disposed = 0, scrolled = 0;
  const root = { innerHTML: '', classList: { add() {}, remove() {} },
    querySelectorAll() { return pages[this.innerHTML]?.scripts || []; } };
  const pages = {};
  const document = { head: { append(script) { scripts.push(script); } },
    querySelectorAll() { return []; }, querySelector() { return null; },
    getElementById() { return root; }, addEventListener(name, fn) { listeners[name] = fn; },
    createElement() { return { setAttribute(name, value) { this[name] = value; } }; } };
  const window = { location: { href: 'http://test/admin/', origin: 'http://test' },
    history: { pushState(_, __, url) { history.push(url); window.location.href = url; } },
    __disposePage() { disposed++; }, scrollTo() { scrolled++; },
    addEventListener(name, fn) { listeners[name] = fn; } };
  vm.runInNewContext(source, { window, document, URL, AbortController,
    DOMParser: class { parseFromString(body) { return { getElementById: () => ({ innerHTML: body }), querySelector: () => ({ textContent: body }) }; } },
    fetch(url, options) { const d = deferred(); requests.push({ url, options, ...d }); return d.promise; } });
  function click(path) {
    const anchor = { href: 'http://test'+path, dataset: {}, hasAttribute: () => false };
    listeners.click({ button: 0, target: { closest: () => anchor }, preventDefault() {} });
  }
  function response(body, text = async () => body) {
    return { ok: true, headers: { get: () => 'text/html' }, text };
  }
  function page(name, external) {
    const inline = { attributes: [], textContent: name, replaceWith() { ran.push(name); } };
    pages[name] = { scripts: external ? [{ src: external, attributes: [{ name: 'src', value: external }], remove() {} }, inline] : [inline] };
  }
  return { click, response, page, requests, history, root, scripts, ran, window,
    counts: () => ({ disposed, scrolled }) };
}

test('latest navigation wins even if an aborted request returns later', async () => {
  const f = setup(); f.click('/admin/old'); f.click('/admin/new');
  assert.equal(f.requests[0].options.signal.aborted, true);
  f.requests[1].resolve(f.response('new')); await tick();
  f.requests[0].resolve(f.response('old')); await tick();
  assert.equal(f.root.innerHTML, 'new');
  assert.deepEqual(f.history, ['http://test/admin/new']);
  assert.deepEqual(f.counts(), { disposed: 1, scrolled: 1 });
});

test('a superseded body or transport failure cannot swap or redirect', async () => {
  for (const body of [true, false]) {
    const f = setup(), delayed = deferred(); f.click('/admin/old');
    if (body) { f.requests[0].resolve(f.response('old', () => delayed.promise)); await tick(); }
    f.click('/admin/new'); f.requests[1].resolve(f.response('new')); await tick();
    if (body) delayed.resolve('old'); else f.requests[0].reject(new Error('offline'));
    await tick();
    assert.equal(f.root.innerHTML, 'new');
    assert.equal(f.window.location.href, 'http://test/admin/new');
  }
});

test('overlapping page initialization shares dependencies and skips old inline scripts', async () => {
  const f = setup(); f.page('old', '/shared.js'); f.page('new', '/shared.js');
  f.click('/admin/old'); f.requests[0].resolve(f.response('old')); await tick();
  f.click('/admin/new'); f.requests[1].resolve(f.response('new')); await tick();
  assert.equal(f.scripts.length, 1);
  f.scripts[0].onload(); await tick();
  assert.deepEqual(f.ran, ['new']);
  assert.equal(f.counts().scrolled, 1);
});

test('current script failures fall back to a full load', async () => {
  const f = setup(); f.page('new', '/broken.js');
  f.click('/admin/new'); f.requests[0].resolve(f.response('new')); await tick();
  f.window.location.href = 'before-error';
  f.scripts[0].onerror(); await tick();
  assert.equal(f.window.location.href, 'http://test/admin/new');
  assert.deepEqual(f.ran, []);
});
