#!/usr/bin/env bash
# Syntax-check the inline JavaScript embedded in the admin HTML templates.
#
# askama compiles the templates as opaque text and the Rust tests assert on
# rendered HTML, so a broken inline <script> never fails the build: it only
# surfaces when the page runs in a browser. This extracts each inline
# (attribute-less) <script> block and compiles it with node, turning a syntax
# error into a check-time failure.
#
# Inline template scripts must therefore be plain JavaScript. Keep askama
# expressions (the {{ }} / {% %} forms) out of <script> bodies, or this check
# will read them as syntax errors.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
templates_dir="$repo_root/crates/admin/templates"

if [[ ! -d "$templates_dir" ]]; then
  echo "skip: $templates_dir not found"
  exit 0
fi

if ! command -v node >/dev/null 2>&1; then
  echo "skip: node not found, cannot syntax-check template JS" >&2
  exit 0
fi

node - "$templates_dir" <<'NODE'
const fs = require("fs");
const path = require("path");
const vm = require("vm");
const root = process.argv[2];

function walk(dir) {
  let out = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, entry.name);
    if (entry.isDirectory()) out = out.concat(walk(p));
    else if (entry.name.endsWith(".html")) out.push(p);
  }
  return out;
}

let failures = 0;
let checked = 0;
// Attribute-less <script> blocks only: <script src=...> are first-party files
// checked elsewhere, and other attributes (type=module and so on) are not used.
const re = /<script>([\s\S]*?)<\/script>/g;

for (const file of walk(root).sort()) {
  const src = fs.readFileSync(file, "utf8");
  let m;
  let index = 0;
  while ((m = re.exec(src)) !== null) {
    index++;
    checked++;
    try {
      // Compile only (never run): this is a parse-time syntax check.
      new vm.Script(m[1], { filename: `${path.relative(root, file)}#script-${index}` });
    } catch (e) {
      failures++;
      console.error(`SYNTAX ERROR in ${path.relative(root, file)} (inline script #${index}): ${e.message}`);
    }
  }
}

console.log(`checked ${checked} inline template script(s)`);
if (failures > 0) {
  console.error(`${failures} inline template script(s) failed to parse`);
  process.exit(1);
}
NODE
