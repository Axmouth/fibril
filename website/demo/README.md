# Read-only dashboard demo

`npm run demo:build` renders the real `crates/admin/templates` pages and copies the
real `admin-ui` assets into generated `public/dashboard-demo`. Normal website
`dev`, `build` and `verify` commands build it automatically. No Rust build, broker,
credentials or remote service is needed. Generated files are ignored by Git.

The small renderer supports only the shell's current Askama constructs and fails
on unknown template syntax. `demo.test.mjs` checks every output against its source
and checks the transport's read-only boundary. Run `npm run verify` for the
website build and static smoke checks. A source fingerprint is emitted as
`dashboard-demo/source.json`.

After building, `npm run smoke:server` uses Docker to check the production Nginx
configuration with an external Host and forwarded HTTPS header. It covers all
demo pages with slashless, trailing-slash, explicit-index and query/embed URLs,
static assets, missing routes and relative documentation redirects. CI runs this
in addition to the build checks. `WEBSITE_SMOKE_IMAGE` can select a locally cached
compatible Nginx image; the default matches the website Dockerfile's runtime.
The server resolves demo index files directly because shared broker navigation
uses slashless paths. Other directory redirects remain relative behind the TLS
proxy, preserving the public origin and the demo's same-origin CSP.

`fixtures.js` supplies deterministic synthetic relationships and time series;
timestamps are anchored to page load. Values illustrate the UI, not broker
performance. `transport.js` installs before the shared scripts, serves only known
fixture API reads and blocks writes, unknown API calls and external fetches. It
uses the production polling fallback instead of emulating SSE. Demo-only UI
protection disables known mutations, while transport rejection remains the
backstop. Keep fixture additions in step with API changes; do not capture live
credentials, user payloads or host details into these public assets.

`DashboardDemo.astro` provides a lazy, titled 860px-tall iframe and a full-view
link, with a wider documentation layout. Embedded URLs use `embed=1`: navigation
chrome is hidden, link destinations removed, click/middle-click and palette
shortcuts blocked, and page-navigation fetches refused. Filters, charts, themes,
inspection and view toggles stay interactive. The iframe does not allow popups
or top navigation; the full-view link lives in the parent documentation and opens
the ordinary navigable demo. This is a focused presentation mode, not a security
boundary against code running on the same origin. Styling
and scripts stay isolated from the docs. Current unversioned docs and the demo
are built together from the same checkout. Existing archived docs do not embed
the demo. Before archiving a version that does, preserve its matching generated
demo under a versioned URL and rewrite those embeds; never point frozen docs at
an incompatible current demo.

Production broker code never loads the fixture transport or demo styling.

Design reference: [Axmouth/natsui](https://github.com/Axmouth/natsui), particularly
`scripts/export-demo.mjs`, `web/demo.js` and `scripts/test-demo.mjs`: share the real
UI, intercept the API, label simulated evidence, and test relationships across
views. This first Fibril fixture is a stable inspectable scenario; Natsui's timed
workload cycles are a possible later extension.
