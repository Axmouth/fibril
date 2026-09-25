import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import { sidebar } from "../src/navigation.mjs";

const docs = new URL("../src/content/docs/", import.meta.url);
const versions = new Set(
  (await readdir(new URL("../src/content/versions/", import.meta.url)))
    .filter((file) => file.endsWith(".json"))
    .map((file) => file.slice(0, -5)),
);
const pages = new Set();
for (const file of await readdir(docs, { recursive: true })) {
  if (!/\.mdx?$/.test(file) || versions.has(file.split("/")[0])) continue;
  // The error page is intentionally absent from navigation.
  if (file === "404.md") continue;
  const body = await readFile(new URL(file, docs), "utf8");
  const frontmatter = body.match(/^---\r?\n([\s\S]*?)\r?\n---/);
  const explicit = frontmatter?.[1].match(/^slug:\s*(.+)$/m)?.[1].trim();
  const slug =
    explicit?.replace(/^['"]|['"]$/g, "") ?? file.replace(/\.mdx?$/, "");
  assert(!pages.has(slug), `Duplicate current page slug: ${slug}`);
  pages.add(slug);
}

const listed = new Set();
function check(items) {
  for (const item of items) {
    if ("items" in item) {
      check(item.items);
    } else if ("slug" in item) {
      assert(
        pages.has(item.slug),
        `Sidebar references an unknown current page: ${item.slug}`,
      );
      assert(
        !listed.has(item.slug),
        `Sidebar lists a page twice: ${item.slug}`,
      );
      listed.add(item.slug);
    } else {
      // The static dashboard is generated separately from the docs collection.
      assert.equal(
        item.link,
        "/dashboard-demo/",
        `Unrecognized non-doc sidebar destination: ${item.link}`,
      );
    }
  }
}
check(sidebar);
const missing = [...pages].filter((slug) => !listed.has(slug));
assert.equal(
  missing.length,
  0,
  `Current pages missing from the sidebar: ${missing.join(", ")}`,
);
console.log(`Navigation covers all ${pages.size} current documentation pages.`);
