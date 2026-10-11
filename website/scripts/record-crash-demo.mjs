// Records the landing page crash test scenarios against real fibril-server and
// fibrilctl binaries, then checks the transcripts in
// src/components/crash-terminal/transcripts.mjs still match what they print.
//
// Usage: node scripts/record-crash-demo.mjs [--bin-dir DIR] [--out DIR] [--only single|failover]
//
// Build the binaries first (cargo build --bin fibril-server --bin fibrilctl).
// Brokers listen on 9876 to 9878 as in compose.cluster.example.yaml, so stop
// any local broker first.
// The default bin dir is the cargo target dir's debug folder. Trimmed logs land
// in the out dir (a fresh temp dir by default) so new lines can be copied into
// the transcripts. Exits non-zero when a transcript line no longer matches.

import { spawn, execFileSync } from "node:child_process";
import { mkdtempSync, writeFileSync, appendFileSync, mkdirSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { randomBytes } from "node:crypto";
import { terminalTabs } from "../src/components/crash-terminal/transcripts.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const repo = join(here, "../..");
const arg = (name, fallback) => {
  const i = process.argv.indexOf(name);
  return i > 0 ? process.argv[i + 1] : fallback;
};
const targetDir = () =>
  JSON.parse(execFileSync("cargo", ["metadata", "--format-version", "1", "--no-deps"], { cwd: repo, encoding: "utf8" })).target_directory;
const binDir = arg("--bin-dir") ?? join(targetDir(), "debug");
const out = arg("--out") ?? mkdtempSync(join(tmpdir(), "fibril-crash-demo-"));
const only = arg("--only");
const SERVER = join(binDir, "fibril-server");
const CTL = join(binDir, "fibrilctl");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Matches the transcript trimming: no timestamp, thread id or file path.
// "2026-...Z  INFO ThreadId(3) fibril_broker::broker: crates/x.rs:12: msg k=v"
// becomes "INFO fibril_broker::broker: msg k=v".
const trim = (line) =>
  line
    .replace(/^\S+Z\s+/, "")
    .replace(/^(\w+)\s+ThreadId\(\d+\)\s+/, "$1 ")
    .replace(/^(\w+ [\w:]+:) [\w/.-]+\.rs:\d+:/, "$1");

class Node {
  constructor(name, env) {
    this.name = name;
    this.env = env;
    this.log = join(out, `${name}.log`);
    this.started = [];
  }
  start() {
    const proc = spawn(SERVER, [], { env: { ...process.env, NO_COLOR: "1", RUST_LOG_STYLE: "never", ...this.env } });
    const write = (chunk) => appendFileSync(this.log, chunk);
    proc.stdout.on("data", write);
    proc.stderr.on("data", write);
    this.proc = proc;
    this.started.push(Date.now());
  }
  kill() {
    this.proc.kill("SIGKILL");
    this.killedAt = Date.now();
  }
  // Waits for the process to exit and its last output to reach the log file.
  async stop(signal = "SIGTERM") {
    const proc = this.proc;
    if (!proc || proc.exitCode !== null || proc.signalCode !== null) return;
    const closed = new Promise((r) => proc.once("close", r));
    proc.kill(signal);
    await closed;
  }
  async healthy() {
    for (let i = 0; i < 150; i++) {
      try {
        if ((await fetch(`http://${this.env.FIBRIL_ADMIN_BIND}/healthz`)).ok) return;
      } catch {}
      await sleep(200);
    }
    throw new Error(`${this.name} did not become healthy, see ${this.log}`);
  }
}

function ctl(args, { allowFail = false } = {}) {
  try {
    return execFileSync(CTL, args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }).trim();
  } catch (e) {
    if (allowFail) return null;
    throw new Error(`fibrilctl ${args.join(" ")} failed: ${e.stderr || e.message}`);
  }
}

const payload = '{"to":"ana@example.com"}';
const results = {};

async function single() {
  const dir = join(out, "single-data");
  mkdirSync(dir, { recursive: true });
  const node = new Node("single", { FIBRIL_DATA_DIR: dir, FIBRIL_BROKER_BIND: "127.0.0.1:9876", FIBRIL_ADMIN_BIND: "127.0.0.1:8081" });
  node.start();
  await node.healthy();
  const broker = ["--broker", node.env.FIBRIL_BROKER_BIND];
  const published = ctl([...broker, "queue", "publish", "email.send", "--message", payload]);
  node.kill();
  await sleep(500);
  node.start();
  await node.healthy();
  const consumed = ctl([...broker, "queue", "consume", "email.send"]);
  await node.stop();
  results.single = { out: [published, consumed], logs: { shell: node.log } };
}

async function failover() {
  const secret = randomBytes(32).toString("hex");
  const peers = [1, 2, 3].map((i) => `${i}=127.0.0.1:${7000 + i}`).join(",");
  const nodes = [1, 2, 3].map((i) => {
    const dir = join(out, `node-${i}-data`);
    mkdirSync(dir, { recursive: true });
    return new Node(`node-${i}`, {
      FIBRIL_DATA_DIR: dir,
      FIBRIL_BROKER_BIND: `127.0.0.1:${9875 + i}`,
      FIBRIL_ADMIN_BIND: `127.0.0.1:${8080 + i}`,
      FIBRIL_CLUSTER_SECRET: secret,
      FIBRIL_COORDINATION_MODE: "ganglion",
      FIBRIL_COORDINATION_NODE_ID: `node-${i}`,
      FIBRIL_COORDINATION_RAFT_ID: String(i),
      FIBRIL_COORDINATION_LISTEN: `127.0.0.1:${7000 + i}`,
      FIBRIL_COORDINATION_PEERS: peers,
      FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY: "replica_durable:2",
      ...(i === 1 ? { FIBRIL_COORDINATION_BOOTSTRAP: "true" } : {}),
    });
  });
  try {
    nodes.forEach((n) => n.start());
    for (const n of nodes) await n.healthy();
    await sleep(5000);
    const byName = Object.fromEntries(nodes.map((n) => [n.name, n]));
    ctl(["--broker", nodes[0].env.FIBRIL_BROKER_BIND, "queue", "declare", "email.send"]);
    const ownerOf = (via) => {
      const topo = ctl(["--admin", via.env.FIBRIL_ADMIN_BIND, "admin", "topology", "--json"], { allowFail: true });
      const a = topo && JSON.parse(topo).coordination?.assignments?.find((x) => x.topic === "email.send" && x.partition === 0);
      return a?.owner ?? null;
    };
    let owner = null;
    for (let i = 0; i < 100 && !owner; i++, await sleep(300)) owner = ownerOf(nodes[0]);
    if (!owner) throw new Error("no owner for email.send");
    const survivor = nodes.find((n) => n.name !== owner);
    await sleep(3000);
    const published = ctl(["--broker", survivor.env.FIBRIL_BROKER_BIND, "queue", "publish", "email.send", "--message", payload]);
    await sleep(1000);
    byName[owner].kill();
    let next = null;
    for (let i = 0; i < 120; i++, await sleep(500)) {
      const o = ownerOf(survivor);
      if (o && o !== owner) { next = o; break; }
    }
    if (!next) throw new Error("no failover owner");
    const activatedAfter = ((Date.now() - byName[owner].killedAt) / 1000).toFixed(1);
    let consumed = null;
    for (let i = 0; i < 10 && consumed === null; i++) {
      consumed = ctl(["--broker", byName[next].env.FIBRIL_BROKER_BIND, "queue", "consume", "email.send"], { allowFail: true });
      if (consumed === null) await sleep(2000);
    }
    results.failover = {
      out: [published, consumed],
      killedAt: byName[owner].killedAt,
      note: `${owner} killed, ${next} became owner, seen ${activatedAfter}s after the kill`,
      logs: Object.fromEntries(nodes.map((n) => [n.name, n.log])),
    };
  } finally {
    for (const n of nodes) await n.stop("SIGKILL");
  }
}

// The message part of a log line: everything before the first key=value field.
const message = (text) => text.replace(/^(INFO|WARN|ERROR) [\w:]+: /, "").split(/ [a-z_]+=/)[0];

function check(tab, run) {
  const problems = [];
  // Raw lines keep their timestamp so matches can report time since the kill.
  const logs = Object.fromEntries(
    Object.entries(run.logs).map(([pane, file]) => [
      pane,
      readFileSync(file, "utf8").split("\n").filter(Boolean).map((raw) => ({ raw, text: trim(raw) })),
    ]),
  );
  const since = (raw) => {
    const at = Date.parse(raw.split(/\s/)[0]);
    return run.killedAt && !Number.isNaN(at) ? ` (${at >= run.killedAt ? "+" : ""}${((at - run.killedAt) / 1000).toFixed(1)}s)` : "";
  };
  for (const l of tab.lines) {
    if (l.kind === "log" && !l.proposed) {
      // Single node logs belong to the shell pane. Cluster panes use node names.
      const pane = logs[l.pane] ? l.pane : Object.keys(logs)[0];
      const want = message(l.text);
      const found = logs[pane].find((line) => message(line.text) === want);
      if (found) {
        console.log(`  ${tab.id}/${pane}${since(found.raw)}: ${found.text}`);
        continue;
      }
      // Which node wins an election or leads recovery varies between runs.
      const elsewhere = Object.entries(logs).find(([, lines]) => lines.some((line) => message(line.text) === want));
      if (elsewhere) console.log(`  note: "${want}" came from ${elsewhere[0]} this run, not ${l.pane}`);
      else problems.push(`${tab.id}/${l.pane}: no log line "${want}"`);
    }
    if (l.kind === "out" && !run.out.includes(l.text)) {
      problems.push(`${tab.id}/${l.pane}: output "${l.text}" not printed, got ${JSON.stringify(run.out)}`);
    }
  }
  return problems;
}
console.log(`binaries: ${binDir}\nlogs: ${out}`);
if (!only || only === "single") await single();
if (!only || only === "failover") await failover();

let problems = [];
for (const tab of terminalTabs) {
  const run = results[tab.id];
  if (!run) continue;
  console.log(`${tab.id}: ${run.note ?? "recorded"}`);
  // Trimmed copies sit next to the raw logs for copying into transcripts.
  for (const [name, file] of Object.entries(run.logs)) {
    const lines = readFileSync(file, "utf8").split("\n").filter(Boolean).map(trim);
    writeFileSync(file.replace(/\.log$/, ".trimmed.log"), lines.join("\n") + "\n");
  }
  problems = problems.concat(check(tab, run));
}
if (problems.length) {
  console.error("\nTranscript lines that no longer match:\n" + problems.map((p) => `  ${p}`).join("\n"));
  process.exit(1);
}
console.log("All transcript lines match.");
process.exit(0);
