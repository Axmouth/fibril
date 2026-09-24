import { createHash } from 'node:crypto';
import { readFile, writeFile, mkdir, cp, rm } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';

const admin = new URL('../../crates/admin/', import.meta.url);
const output = new URL('../public/dashboard-demo/', import.meta.url);
const base = '/dashboard-demo';
export const pages = ['overview', 'queues', 'queue', 'streams', 'messages', 'dlq',
  'connections', 'subscriptions', 'activity', 'topology', 'diagnostics', 'security', 'settings'];

// The shared Askama templates currently use only these shell substitutions.
// Fail on new syntax instead of silently producing a stale or partial demo.
export function renderTemplate(layout, pageBody, page) {
  const content = pageBody.match(/^\{% extends "layout.html" %\}\s*\{% block content %\}([\s\S]*)\{% endblock %\}\s*$/);
  if (!content) throw new Error(`Unsupported page template: ${page}`);
  let html = layout
    .replace(/\{% if page == "([a-z]+)" %\}([\s\S]*?)\{% endif %\}/g,
      (_, name, text) => name === (page === 'overview' ? 'dashboard' : page) ? text : '')
    .replace(/\{% if auth_enabled %\}[\s\S]*?\{% else %\}([\s\S]*?)\{% endif %\}/g, '$1')
    .replace(/\{\{\s*title\s*\}\}/g, `Fibril demo · ${page}`)
    .replace('{% block content %}{% endblock %}', content[1]);
  if (/\{%|\{\{/.test(html)) throw new Error(`Unrendered template expression: ${page}`);
  html = html.replaceAll('/static/', `${base}/static/`)
    .replace(/\/admin\/(?!api)/g, `${base}/admin/`)
    .replace(/href="\/"/g, `href="${base}/"`)
    .replace(/href: "\/"/g, `href: "${base}/"`);
  // Install the fixture transport before any production page or layout script.
  html = html.replace('<head>', `<head>\n  <meta name="robots" content="noindex">\n  <meta http-equiv="Content-Security-Policy" content="default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'none'">\n  <script src="${base}/fixtures.js"></script>\n  <script src="${base}/transport.js"></script>\n  <link rel="stylesheet" href="${base}/demo.css">`);
  return html.replace('<body class="admin-body">', `<body class="admin-body">\n  <div class="demo-banner" role="note"><strong>Read-only demo</strong><span>Illustrative sample data · no broker connected</span><a href="/admin-dashboard/" target="_top" data-no-boost>Dashboard docs ↗</a></div>`);
}

export async function buildDemo() {
  const layout = await readFile(new URL('templates/layout.html', admin), 'utf8');
  const hash = createHash('sha256').update(layout);
  await rm(output, { recursive: true, force: true });
  await mkdir(output, { recursive: true });
  await cp(new URL('admin-ui/', admin), new URL('static/', output), { recursive: true });
  // Rebase shared script asset and navigation paths in generated output only.
  for (const name of ['api.js', 'admin.js', 'tendrils.js', 'recovery-timeline.js']) {
    const p = new URL(`static/js/${name}`, output);
    const source = await readFile(p, 'utf8'); hash.update(source);
    await writeFile(p, source.replaceAll('/static/', `${base}/static/`).replace(/\/admin\/(?!api)/g, `${base}/admin/`));
  }
  for (const page of pages) {
    const source = await readFile(new URL(`templates/pages/${page}.html`, admin), 'utf8');
    hash.update(source);
    const dest = new URL(page === 'overview' ? './' : `admin/${page}/`, output);
    await mkdir(dest, { recursive: true });
    await writeFile(new URL('index.html', dest), renderTemplate(layout, source, page));
  }
  for (const name of ['fixtures.js', 'transport.js', 'demo.css']) {
    const source = await readFile(new URL(name, import.meta.url)); hash.update(source);
    await writeFile(new URL(name, output), source);
  }
  hash.update(await readFile(new URL('admin-ui/css/admin.css', admin)));
  await writeFile(new URL('source.json', output), JSON.stringify({
    schema: 1, source: 'crates/admin/templates + crates/admin/admin-ui',
    sha256: hash.digest('hex'), pages, data: 'Synthetic, illustrative; not benchmark results',
  }, null, 2));
  console.log(`Built ${pages.length} shared dashboard views in ${fileURLToPath(output)}`);
}
if (process.argv[1] === fileURLToPath(import.meta.url)) await buildDemo();
