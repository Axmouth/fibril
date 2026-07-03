import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

const root = new URL("../dist/", import.meta.url).pathname;

const checks = [
  {
    path: "index.html",
    includes: [
      "<title>Fibril | Durable messaging without broker ceremony</title>",
      "Useful messaging without broker ceremony.",
      "queue lifecycle",
      // The release badge renders a real version from the workspace manifest,
      // never the 0.x fallback.
      'release-tag">v',
    ],
  },
  {
    path: "overview/index.html",
    includes: ["<title>Fibril documentation | Fibril</title>", "/quickstart/"],
  },
  {
    path: "404.html",
    includes: ["<title>Page not found | Fibril</title>", "Useful places to land"],
  },
  {
    path: "status/index.html",
    includes: ["<title>Project status | Fibril</title>", "Explicit settlement", "Dead lettering"],
  },
  {
    path: "implemented-surface/index.html",
    includes: [
      "<title>Implemented surface | Fibril</title>",
      "reverse roadmap",
      "Conditions for a queue to be unloaded from memory",
    ],
  },
  {
    path: "reliability/retries-delays/index.html",
    includes: ["<title>Retries and delays | Fibril</title>", "retry_after"],
  },
  {
    path: "reliability/dead-lettering/index.html",
    includes: ["<title>Dead Lettering | Fibril</title>", "global dead-letter queue target"],
  },
  {
    path: "concepts/backpressure/index.html",
    includes: ["<title>Backpressure | Fibril</title>", "prefetch"],
  },
  // The frozen 0.2 snapshot proves versioning is wired: the overview page exists
  // under the version slug and its intra-doc links are rewritten to /0.2/.
  {
    path: "0.2/index.html",
    includes: ["<title>Fibril documentation | Fibril</title>", "/0.2/quickstart/"],
  },
  {
    path: "0.2/status/index.html",
    includes: ["<title>Project status | Fibril</title>"],
  },
  {
    path: "sitemap-index.xml",
    includes: ["sitemap"],
  },
];

let failures = 0;

for (const check of checks) {
  const file = join(root, check.path);
  if (!existsSync(file)) {
    console.error(`missing built file: ${check.path}`);
    failures += 1;
    continue;
  }

  const body = readFileSync(file, "utf8");
  for (const expected of check.includes) {
    if (!body.includes(expected)) {
      console.error(`missing expected content in ${check.path}: ${expected}`);
      failures += 1;
    }
  }
}

if (failures > 0) {
  console.error(`static smoke failed with ${failures} issue(s)`);
  process.exit(1);
}

console.log(`static smoke passed for ${checks.length} built artifacts`);
