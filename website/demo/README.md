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

`fixtures.js` supplies deterministic synthetic relationships and time series;
timestamps are anchored to page load. Values illustrate the UI, not broker
performance. `transport.js` installs before the shared scripts, serves only known
fixture API reads and blocks writes, unknown API calls and external fetches. It
uses the production polling fallback instead of emulating SSE. Demo-only UI
protection disables known mutations, while transport rejection remains the
backstop. Keep fixture additions in step with API changes; do not capture live
credentials, user payloads or host details into these public assets.

`DashboardDemo.astro` provides a lazy, titled iframe and a full-view link. Styling
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
