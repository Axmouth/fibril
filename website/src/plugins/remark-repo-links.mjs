// Lets docs pages link to repository files with ordinary relative paths, such
// as ../../../../benchmarks/comparison, so the same link works when the source
// is read on GitHub. The site only serves the docs, so at build time any
// relative link that resolves to an existing file or folder outside the docs
// tree is rewritten to its GitHub URL on main. Links to other docs pages and
// links that resolve to nothing are left alone.
import { existsSync, statSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const DOCS_ROOT = path.join(REPO_ROOT, "website/src/content/docs");
const GITHUB = "https://github.com/Axmouth/fibril";
const VERSION_DIR = /^\d+\.\d+$/;

// Versioned snapshots sit one folder deeper than the page they were copied
// from, so resolve their links as if they were still at the docs root.
function sourceDir(file) {
  const rel = path.relative(DOCS_ROOT, path.dirname(file));
  const parts = rel.split(path.sep);
  if (VERSION_DIR.test(parts[0])) parts.shift();
  return path.join(DOCS_ROOT, ...parts);
}

function githubUrl(file, url) {
  if (/^[a-z][a-z0-9+.-]*:|^[/#]/i.test(url)) return null;
  const [target, hash = ""] = url.split(/(?=#)/);
  const resolved = path.resolve(sourceDir(file), decodeURI(target));
  if (resolved.startsWith(DOCS_ROOT + path.sep) || !resolved.startsWith(REPO_ROOT + path.sep)) return null;
  if (!existsSync(resolved)) return null;
  const kind = statSync(resolved).isDirectory() ? "tree" : "blob";
  const repoPath = path.relative(REPO_ROOT, resolved).split(path.sep).join("/");
  return `${GITHUB}/${kind}/main/${repoPath}${hash}`;
}

export default function remarkRepoLinks() {
  return (tree, vfile) => {
    const file = vfile.path ?? vfile.history?.[0];
    if (!file || !file.startsWith(DOCS_ROOT)) return;
    const visit = (node) => {
      if ((node.type === "link" || node.type === "definition") && node.url) {
        const rewritten = githubUrl(file, node.url);
        if (rewritten) node.url = rewritten;
      }
      for (const child of node.children ?? []) visit(child);
    };
    visit(tree);
  };
}
