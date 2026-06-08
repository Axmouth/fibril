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
    ],
  },
  {
    path: "latest/index.html",
    includes: ["<title>Fibril documentation | Fibril</title>"],
  },
  {
    path: "latest/status/index.html",
    includes: ["<title>Project status | Fibril</title>", "Explicit settlement", "Dead lettering"],
  },
  {
    path: "latest/implemented-surface/index.html",
    includes: [
      "<title>Implemented surface | Fibril</title>",
      "reverse roadmap",
      "Conditions for a queue to be unloaded from memory",
    ],
  },
  {
    path: "latest/reliability/retries-delays/index.html",
    includes: ["<title>Retries and delays | Fibril</title>", "retry_after"],
  },
  {
    path: "latest/reliability/dead-lettering/index.html",
    includes: ["<title>Dead Lettering | Fibril</title>", "global dead-letter queue target"],
  },
  {
    path: "latest/concepts/backpressure/index.html",
    includes: ["<title>Backpressure | Fibril</title>", "prefetch"],
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
