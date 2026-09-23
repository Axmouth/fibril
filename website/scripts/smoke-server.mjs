import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { request } from 'node:http';
import { fileURLToPath } from 'node:url';
import { pages } from '../demo/build.mjs';

// Exercise the production server behind a simulated TLS-terminating proxy.
// Static-file and Astro preview checks cannot catch Nginx-generated redirects.
const docker = (...args) => execFileSync('docker', args, { encoding: 'utf8' }).trim();
const dist = fileURLToPath(new URL('../dist', import.meta.url));
const config = fileURLToPath(new URL('../docker/nginx.conf', import.meta.url));
const image = process.env.WEBSITE_SMOKE_IMAGE || 'nginx:1.28-alpine';
let container;

function get(port, path) {
  return new Promise((resolve, reject) => {
    const req = request({ host: '127.0.0.1', port, path, headers: {
      Host: 'docs.example.test',
      'X-Forwarded-Proto': 'https',
      'X-Requested-With': 'fetch',
    } }, res => {
      let body = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { body += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body }));
      res.on('error', reject);
    });
    req.setTimeout(3000, () => req.destroy(new Error(`Timeout: ${path}`)));
    req.on('error', reject);
    req.end();
  });
}

try {
  container = docker('run', '--detach', '--rm', '--publish', '127.0.0.1::8080',
    '--mount', `type=bind,src=${dist},dst=/usr/share/nginx/html,readonly`,
    '--mount', `type=bind,src=${config},dst=/etc/nginx/conf.d/default.conf,readonly`, image);
  const port = Number(docker('port', container, '8080/tcp').split(':').at(-1));
  for (let attempt = 0; ; attempt++) {
    try {
      assert.equal((await get(port, '/healthz')).status, 200);
      break;
    } catch (error) {
      if (attempt === 39) throw error;
      await new Promise(resolve => setTimeout(resolve, 100));
    }
  }

  for (const page of pages) {
    const path = page === 'overview' ? '/dashboard-demo' : `/dashboard-demo/admin/${page}`;
    for (const suffix of ['', '/', '/index.html', '?topic=orders.created&partition=0', '/?embed=1']) {
      const res = await get(port, path + suffix);
      assert.equal(res.status, 200, `${path + suffix}: ${res.status}, Location=${res.headers.location}`);
      assert.equal(res.headers.location, undefined);
      assert.match(res.headers['content-type'], /text\/html/);
      assert(res.body.includes(`Fibril demo · ${page}`), path + suffix);
      assert(res.body.includes("connect-src 'self'"), 'Keep the demo CSP intact');
    }
  }
  for (const path of ['/quickstart', '/reliability/replication']) {
    const res = await get(port, path);
    assert.equal(res.status, 301, path);
    assert.equal(res.headers.location, path + '/', 'Redirect must preserve the external scheme/host/port');
    assert.equal((await get(port, res.headers.location)).status, 200);
  }
  for (const path of ['/dashboard-demo/fixtures.js', '/dashboard-demo/transport.js',
    '/dashboard-demo/static/js/admin.js', '/dashboard-demo/static/css/admin.css']) {
    assert.equal((await get(port, path)).status, 200, path);
  }
  assert.equal((await get(port, '/dashboard-demo/admin/missing')).status, 404);
  assert.equal((await get(port, '/dashboard-demo/admin/api/queues')).status, 404);
  console.log(`Nginx smoke passed: ${pages.length} demo views, query/embed URLs, assets and proxy-safe redirects`);
} finally {
  if (container) docker('rm', '--force', container);
}
