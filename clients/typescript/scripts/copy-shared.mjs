// Copies the repo-root LICENSE and AI_POLICY.md into the package directory for
// npm pack, so the tarball carries them without keeping a second copy in git.
// The copies are gitignored. Run with --clean to remove them after packing.
import { copyFileSync, rmSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const pkgDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = join(pkgDir, "..", "..");
const shared = ["LICENSE", "AI_POLICY.md"];
const clean = process.argv.includes("--clean");

for (const name of shared) {
  const target = join(pkgDir, name);
  if (clean) {
    rmSync(target, { force: true });
  } else {
    copyFileSync(join(repoRoot, name), target);
  }
}
