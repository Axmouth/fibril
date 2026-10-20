#!/usr/bin/env python3
"""Run one Fibril benchmark case on fresh local brokers.

Starts one broker or a three-node Ganglion cluster from a release binary,
declares the queues, runs the shared comparison client (benchmarks/comparison)
against each queue's owner, validates counts and settlement, and writes a
summary with latency, CPU, syscalls, context switches and batching counters.

Every node keeps its data under --storage. Results stay under --output, failed
runs included. See README.md for the presets and suites built on top of this.
"""
import argparse
import base64
import hashlib
import json
import os
import platform
import re
import resource
import secrets
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
CREDENTIAL = "bench"


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def http(base, path, body=None, method=None, timeout=3):
    headers = {"Content-Type": "application/json",
               "Authorization": "Basic " + base64.b64encode(f"{CREDENTIAL}:{CREDENTIAL}".encode()).decode()}
    request = urllib.request.Request(base + path, data=None if body is None else json.dumps(body).encode(),
                                     headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.load(response)


def wait_for(check, seconds=90):
    deadline = time.monotonic() + seconds
    error = None
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except (urllib.error.URLError, TimeoutError, ConnectionError) as exc:
            error = str(exc)
        time.sleep(0.1)
    raise TimeoutError(f"readiness deadline exceeded: {error}")


def free_ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def process_sample(process):
    """CPU, RSS, syscalls and context switches of one process, from /proc."""
    try:
        proc = Path(f"/proc/{process.pid}")
        stat = (proc / "stat").read_text().split(")", 1)[1].split()
        rss = re.search(r"^VmRSS:\s+(\d+)", (proc / "status").read_text(), re.M)
        io = dict(line.split(": ") for line in (proc / "io").read_text().splitlines())
        hz = os.sysconf("SC_CLK_TCK")
        switches = [0, 0]
        by_thread = {}
        for task in (proc / "task").iterdir():
            try:
                name = (task / "comm").read_text().strip()
                for line in (task / "status").read_text().splitlines():
                    if line.startswith("voluntary_ctxt_switches"):
                        switches[0] += int(line.split()[1])
                        by_thread[name] = by_thread.get(name, 0) + int(line.split()[1])
                    elif line.startswith("nonvoluntary_ctxt_switches"):
                        switches[1] += int(line.split()[1])
            except (FileNotFoundError, ProcessLookupError):
                pass
        return {"pid": process.pid, "rss_kib": int(rss[1]) if rss else 0,
                "threads": sum(1 for _ in (proc / "task").iterdir()),
                "cpu_seconds": (int(stat[11]) + int(stat[12])) / hz,
                "voluntary_switches": switches[0], "involuntary_switches": switches[1],
                "voluntary_switches_by_thread": by_thread,
                "syscr": int(io["syscr"]), "syscw": int(io["syscw"]), "write_bytes": int(io["write_bytes"])}
    except (FileNotFoundError, ProcessLookupError, PermissionError):
        # A process that just exited denies /proc reads.
        return {"pid": process.pid, "exited": True}


def meminfo():
    fields = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines())
    return {k: int(fields[k].split()[0]) for k in ("Dirty", "Writeback")}


def provenance(args):
    binaries = {}
    for path in (args.bin_dir / "fibril-server", args.compare_bin):
        with path.open("rb") as binary:
            binaries[path.name] = {"path": str(path), "sha256": hashlib.file_digest(binary, "sha256").hexdigest()}
    repositories = {}
    for name, path in (("fibril", REPO), ("keratin", REPO.parent / "keratin"), ("ganglion", REPO.parent / "ganglion")):
        try:
            repositories[name] = {
                "revision": subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip(),
                "dirty": bool(subprocess.check_output(["git", "-C", str(path), "status", "--porcelain"], text=True)),
            }
        except (subprocess.CalledProcessError, FileNotFoundError):
            repositories[name] = {"unavailable": True}
    return {"binaries": binaries, "repositories": repositories,
            "note": "Repository state does not prove how prebuilt binaries were built."}


def expected_policy(args):
    kind, _, nodes = args.assignment.partition(":")
    want = {"mode": kind.rsplit("_", 1)[0] + f"_{args.durability}"}
    if nodes:
        want["nodes"] = int(nodes)
    return want


def seed_toml(args):
    lines = ["[runtime_seed.replication]",
             f"stream_apply_linger_us = {args.linger_us}",
             f"early_replication = {str(bool(args.early_replication)).lower()}"]
    return "\n".join(lines) + "\n"


def run(args):
    case = args.output
    copies = args.copies
    queues = ["topic1"] + [f"topic{k}" for k in range(2, args.queues + 1)]
    data = args.storage / case.name
    data.mkdir(parents=True, exist_ok=False)
    node_data = [(args.node_storage[i] / case.name if args.node_storage else data) / f"node-{i}"
                 for i in range(copies)]
    ports = free_ports(copies * 3)
    broker_port = lambda i: ports[i * 3]
    endpoints = [f"http://127.0.0.1:{ports[i * 3 + 1]}" for i in range(copies)]
    secret = secrets.token_hex(16)
    env = {k: v for k, v in os.environ.items() if not k.startswith(("FIBRIL_", "KERATIN_", "STROMA_"))}
    env.update(RUST_LOG=args.rust_log, FIBRIL_AUTH_USERNAME=CREDENTIAL, FIBRIL_AUTH_PASSWORD=CREDENTIAL)
    processes, logs, servers, clients, samples, profilers = [], [], [], [], [], []

    def launch(command, name, environment, cwd):
        log = (case / f"{name}.log").open("w")
        logs.append(log)
        process = subprocess.Popen(command, cwd=cwd, env=environment, stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        save(case / f"{name}-command.json", command)
        return process

    def queue_ready(queue, tops, debug, settled):
        states = [next((q for q in d["queues"] if q["topic"] == queue), None) for d in debug]
        if not all(states):
            return False
        if copies == 3:
            assignments = [[a for a in t["coordination"]["assignments"] if a["topic"] == queue] for t in tops]
            if not all(len(a) == 1 and len(a[0]["followers"]) == 2 for a in assignments):
                return False
            if not all(a == assignments[0] for a in assignments):
                return False
            assignment = assignments[0][0]
            if assignment["durability"] != expected_policy(args):
                raise RuntimeError(f"unexpected durability policy: {assignment['durability']}")
            for t, state, d in zip(tops, states, debug):
                owner = t["coordination"]["node_id"] == assignment["owner"]
                if state["role"] != ("owner" if owner else "follower") or not state["materialized"]:
                    return False
                if owner:
                    progress = next((r for r in d["owned_replicas"] if r["topic"] == queue), None)
                    if not progress or len(progress["followers"]) != 2 or not all(f["in_sync"] for f in progress["followers"]):
                        return False
        if settled and not all(q["state"]["settled_until"] == q["applied_upto"] + 1
                               and q["state"]["ready_count"] == q["state"]["inflight_count"] == 0 for q in states):
            return False
        return True

    def topology(settled=False):
        tops = [http(base, "/admin/api/topology") for base in endpoints]
        debug = [http(base, "/admin/api/queues_debug") for base in endpoints]
        if all(queue_ready(q, tops, debug, settled) for q in queues):
            return {"topology": tops, "queues": debug}
        return None

    def owner_index(queue):
        if copies == 1:
            return 0
        top = json.loads((case / "before.json").read_text())["topology"][0]["coordination"]["assignments"]
        return int(next(a for a in top if a["topic"] == queue)["owner"].split("-")[1]) - 1

    def settings_applied(base):
        response = http(base, "/admin/api/runtime-settings")
        settings = response["settings"]
        if settings["replication"]["stream_apply_linger_us"] != args.linger_us:
            return None
        if args.sync_interval_ms is not None and settings["storage"]["background_sync_interval_ms"] != args.sync_interval_ms:
            return None
        if not (response.get("local_application") or {}).get("broker_matches_saved"):
            return None
        return response

    try:
        cpusets = args.broker_cpusets.split(";") if args.broker_cpusets else []
        for i in range(copies):
            node_data[i].mkdir(parents=True)
            node_env = dict(env, KERATIN_DRAIN_COUNTERS_PATH=str(case / f"node-{i}-counters.json"),
                            FIBRIL_TASK_POLLS_PATH=str(case / f"node-{i}-polls.json"))
            node_env.update(item.split("=", 1) for item in args.node_env)
            if args.preallocate_bytes is not None:
                node_env["FIBRIL_KERATIN_SEGMENT_PREALLOCATE_BYTES"] = str(args.preallocate_bytes)
            config = case / f"node-{i}.toml"
            config.write_text(("[coordination.ganglion]\ntarget_followers = 2\n\n" if copies == 3 else "") + seed_toml(args))
            pin = ["taskset", "-c", cpusets[i]] if i < len(cpusets) and cpusets[i] else []
            command = pin + [str(args.bin_dir / "fibril-server"), "--data-dir", str(node_data[i] / "data"),
                             "--broker-bind", f"127.0.0.1:{broker_port(i)}",
                             "--admin-bind", f"127.0.0.1:{ports[i * 3 + 1]}", "--config", str(config)]
            if copies == 3:
                node_env.update(
                    FIBRIL_COORDINATION_MODE="ganglion", FIBRIL_CLUSTER_SECRET=secret,
                    FIBRIL_COORDINATION_NODE_ID=f"broker-{i + 1}", FIBRIL_COORDINATION_RAFT_ID=str(i + 1),
                    FIBRIL_COORDINATION_LISTEN=f"127.0.0.1:{ports[i * 3 + 2]}",
                    FIBRIL_COORDINATION_BOOTSTRAP=str(i == 0).lower(),
                    FIBRIL_COORDINATION_PEERS=",".join(f"{j + 1}=127.0.0.1:{ports[j * 3 + 2]}" for j in range(copies)),
                    FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY=args.assignment)
            servers.append(launch(command, f"node-{i}", node_env, node_data[i]))
        for i, endpoint in enumerate(endpoints):
            save(case / f"node-{i}-settings.json", wait_for(lambda: http(endpoint, "/admin/api/startup-config")))
        if copies == 3:
            wait_for(lambda: all(len(http(b, "/admin/api/topology")["coordination"]["nodes"]) == 3 for b in endpoints))
        for queue in queues:
            body = {"topic": queue, "partition_count": 1, "durability": args.durability}
            save(case / f"declare-{queue}.json", http(endpoints[0], "/admin/api/queues", body))
        save(case / "before.json", wait_for(topology))
        if args.idle_queues:
            for k in range(args.idle_queues):
                http(endpoints[0], "/admin/api/queues",
                     {"topic": f"idle{k}", "partition_count": 1, "durability": args.durability}, timeout=30)
            # Every node opens a log for each queue it owns or follows.
            per_node = args.idle_queues + args.queues
            opened = lambda: all(sum(1 for q in http(b, "/admin/api/queues_debug", timeout=30)["queues"]
                                     if q.get("materialized")) >= per_node for b in endpoints)
            wait_for(opened, 600)
            time.sleep(5)
            idle = [process_sample(p) for p in servers]
            save(case / "idle.json", idle)
        if args.sync_interval_ms is not None:
            current = http(endpoints[0], "/admin/api/runtime-settings")
            current["settings"]["storage"]["background_sync_interval_ms"] = args.sync_interval_ms
            http(endpoints[0], "/admin/api/runtime-settings", method="PUT", timeout=10,
                 body={"expected_version": current["version"], "settings": current["settings"]})
        for i, endpoint in enumerate(endpoints):
            save(case / f"node-{i}-runtime-before.json", wait_for(lambda e=endpoint: settings_applied(e)))

        # Task poll counts, written once a second by a task-polls build.
        read_polls = lambda: [json.loads(f.read_text()) if f.exists() else {}
                              for f in (case / f"node-{i}-polls.json" for i in range(copies))]
        time.sleep(1.2)
        polls_before = read_polls()
        before = [process_sample(p) for p in servers]
        if args.perf:
            # One perf process per broker, since perf's per-user locked memory
            # rarely covers more. --perf-role picks which broker.
            owner = owner_index("topic1")
            chosen = [owner] if copies == 1 or args.perf_role == "owner" else [(owner + 1) % copies]
            save(case / "perf-nodes.json", {"role": args.perf_role, "nodes": chosen})
            mode = {"flat": ["-F", "999"], "fp": ["-g", "-F", "499"],
                    "switches": ["-g", "-e", "context-switches"]}[args.perf]
            raise_fds = lambda: resource.setrlimit(resource.RLIMIT_NOFILE, (resource.getrlimit(resource.RLIMIT_NOFILE)[1],) * 2)
            for n in chosen:
                profilers.append(subprocess.Popen(
                    ["perf", "record", *mode, "-o", str(case / f"perf-node-{n}.data"), "-p", str(servers[n].pid)],
                    stdout=subprocess.DEVNULL, stderr=(case / f"perf-node-{n}.err").open("w"), preexec_fn=raise_fds))

        for queue in queues:
            command = [str(args.compare_bin), "--broker", "fibril", "--fibril-sparse-offsets", "--queue", queue,
                       "--endpoint", f"127.0.0.1:{broker_port(owner_index(queue))}", "--copies", str(copies),
                       "--connections", str(args.connections), "--payload-bytes", str(args.size),
                       "--prefetch", str(args.prefetch),
                       "--confirm-window", str(args.confirm_window), "--delivery-window", str(args.confirm_window),
                       "--workers", "4", "--warmup-secs", str(args.warmup_secs),
                       "--duration-secs", str(args.duration_secs), "--drain-secs", "15",
                       "--output", str(case / f"client-{queue}.json")]
            if args.unconfirmed:
                command += ["--fibril-unconfirmed"]
            if args.burst_pattern:
                command += ["--burst-pattern", args.burst_pattern]
            command += ["--rate", str(args.rate)] if args.rate else ["--saturation"]
            clients.append(launch(command, f"client-{queue}", env, case))

        started = time.monotonic()
        while any(c.poll() is None for c in clients):
            samples.append({"elapsed_s": time.monotonic() - started, "meminfo": meminfo(),
                            "servers": [process_sample(p) for p in servers]})
            if any(p.poll() is not None for p in servers):
                raise RuntimeError("a broker exited during the workload")
            if any(c.poll() not in (None, 0) for c in clients):
                raise RuntimeError("a client failed, see the client logs")
            if time.monotonic() - started > args.duration_secs + args.warmup_secs + 60:
                raise TimeoutError("workload deadline exceeded")
            time.sleep(1)
        for profiler in profilers:
            profiler.send_signal(2)
        for profiler in profilers:
            profiler.wait(timeout=120)

        results = []
        for queue in queues:
            one = json.loads((case / f"client-{queue}.json").read_text())
            counts = one["counts"]
            confirms = args.connections if args.unconfirmed else counts["issued"]
            if (one["status"] != "client_validated" or counts["issued"] <= args.connections
                    or counts["confirmed"] != confirms
                    or counts["delivered"] != counts["issued"] or counts["ack_sent"] != counts["issued"]):
                raise RuntimeError(f"client counts failed validation on {queue}: {counts}")
            results.append(one)
        # Delivery counts alone cannot show settlement or catch duplicates.
        save(case / "settled.json", wait_for(lambda: topology(settled=True), 120))
        after = [process_sample(p) for p in servers]
        time.sleep(1.2)
        save(case / "polls.json", {"before": polls_before, "after": read_polls()})
        for i, endpoint in enumerate(endpoints):
            save(case / f"node-{i}-runtime-after.json", settings_applied(endpoint))
        save(case / "summary.json", summarize(args, case, results, before, after, samples))
        print(f"passed {case.name}", flush=True)
    except BaseException as error:
        save(case / "summary.json", {"status": "failed", "error": str(error)})
        for i, endpoint in enumerate(endpoints):
            try:
                save(case / f"node-{i}-failure-state.json", http(endpoint, "/admin/api/queues_debug"))
            except (urllib.error.URLError, TimeoutError, ConnectionError):
                pass
        raise
    finally:
        save(case / "samples.json", samples)
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
        for process in processes:
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for log in logs:
            log.close()
        if not args.keep_data:
            for parent in {data, *(d.parent for d in node_data)}:
                shutil.rmtree(parent, ignore_errors=True)


def summarize(args, case, results, before, after, samples):
    issued = sum(r["counts"]["issued"] for r in results)
    delta = lambda key: sum(a.get(key, 0) for a in after) - sum(b.get(key, 0) for b in before)
    counters = [json.loads(f.read_text()) for f in sorted(case.glob("node-*-counters.json"))]
    polls = json.loads((case / "polls.json").read_text()) if (case / "polls.json").exists() else None
    polls_by_site = {}
    if polls:
        for b, a in zip(polls["before"], polls["after"]):
            for site, n in a.items():
                polls_by_site[site] = polls_by_site.get(site, 0) + n - b.get(site, 0)
    idle = json.loads((case / "idle.json").read_text()) if (case / "idle.json").exists() else None
    return {
        "status": "passed",
        "idle": idle and {"queues": args.idle_queues,
                          "rss_mib_per_node": [i.get("rss_kib", 0) / 1024 for i in idle],
                          "threads_per_node": [i.get("threads") for i in idle]},
        "label": {"nodes": args.copies, "policy": args.assignment if args.copies == 3 else "standalone",
                  "queue_mode": args.durability, "queues": args.queues, "payload_bytes": args.size,
                  "rate_per_queue": args.rate or "saturation", "burst_pattern": args.burst_pattern,
                  "confirm_window": args.confirm_window, "prefetch": args.prefetch,
                  "connections": args.connections, "idle_queues": args.idle_queues,
                  "early_replication": bool(args.early_replication), "linger_us": args.linger_us,
                  "sync_interval_ms": args.sync_interval_ms, "unconfirmed": args.unconfirmed,
                  "node_env": args.node_env,
                  "broker_cpusets": args.broker_cpusets},
        "issued": issued,
        "delivered_per_sec": sum(r.get("observed_delivery_per_sec") or 0 for r in results),
        "latency_ms": [r["delivery"]["from_schedule"] for r in results],
        "cpu_seconds_per_million": delta("cpu_seconds") * 1e6 / issued,
        "switches_per_message": (delta("voluntary_switches") + delta("involuntary_switches")) / issued,
        "write_syscalls_per_message": delta("syscw") / issued,
        # A voluntary switch is a thread going to sleep until something wakes
        # it, so this maps wakeups by thread role. Threads that exited during
        # the run are missing from the end sample.
        "sleeps_per_message_by_thread": {
            name: (sum(a.get("voluntary_switches_by_thread", {}).get(name, 0) for a in after)
                   - sum(b.get("voluntary_switches_by_thread", {}).get(name, 0) for b in before)) / issued
            for name in sorted({n for a in after for n in a.get("voluntary_switches_by_thread", {})})},
        # Only from a task-polls build. Polls by spawn site, summed over nodes.
        "task_polls_per_message": {site: n / issued for site, n in
                                   sorted(polls_by_site.items(), key=lambda kv: -kv[1]) if n > 0},
        "staged_batches_per_message": [c["staged_batches"] / issued for c in counters],
        "fsync_jobs_per_message": sum(c["worker_fsync_jobs"] + c["direct_fsync_jobs"] for c in counters) / issued,
        "peak_rss_mib": max((sum(s.get("rss_kib", 0) for s in sample["servers"]) for sample in samples), default=0) / 1024,
        "peak_dirty_mib": max((s["meminfo"]["Dirty"] for s in samples), default=0) / 1024,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--output", type=Path, required=True, help="Result directory for this run, must not exist")
    parser.add_argument("--storage", type=Path, required=True, help="Filesystem root for broker data")
    parser.add_argument("--node-storage", type=Path, nargs=3, help="Separate storage roots per node, for three nodes")
    parser.add_argument("--bin-dir", type=Path, default=REPO / "target/release", help="Directory holding fibril-server")
    parser.add_argument("--compare-bin", type=Path,
                        default=REPO / "benchmarks/comparison/target/release/broker-compare")
    parser.add_argument("--copies", type=int, choices=[1, 3], default=3, help="Standalone broker or three-node cluster")
    parser.add_argument("--assignment", default="majority_durable",
                        help="Cluster assignment durability, for example majority_durable or replica_durable:3")
    parser.add_argument("--durability", choices=["durable", "buffered"], default="durable", help="Queue mode")
    parser.add_argument("--queues", type=int, default=1, help="Independent queues, one client each")
    parser.add_argument("--rate", type=int, default=0, help="Messages per second per queue, 0 for saturation")
    parser.add_argument("--burst-pattern", help="Repeating burst sizes at the average --rate, for example 200,100")
    parser.add_argument("--size", type=int, default=1024, help="Payload bytes")
    parser.add_argument("--connections", type=int, default=1,
                        help="Publisher and consumer connection pairs per queue, sharing the total rate and credit")
    parser.add_argument("--idle-queues", type=int, default=0,
                        help="Extra queues declared and left without traffic, to measure idle cost")
    parser.add_argument("--confirm-window", type=int, default=32768)
    parser.add_argument("--prefetch", type=int, default=2000)
    parser.add_argument("--warmup-secs", type=int, default=3)
    parser.add_argument("--duration-secs", type=int, default=12)
    parser.add_argument("--unconfirmed", action="store_true", help="Publish without confirms, as an upper bound")
    parser.add_argument("--early-replication", type=int, choices=[0, 1], default=1)
    parser.add_argument("--linger-us", type=int, default=2000, help="Follower apply linger")
    parser.add_argument("--sync-interval-ms", type=int, help="Background sync interval for buffered queues")
    parser.add_argument("--preallocate-bytes", type=int)
    parser.add_argument("--broker-cpusets", help="Per-node CPU lists for taskset, for example '0-3;4-7;8-11'")
    parser.add_argument("--perf", choices=["flat", "fp", "switches"],
                        help="Record one broker with perf: flat samples, frame-pointer stacks or context switches")
    parser.add_argument("--perf-role", choices=["owner", "follower"], default="owner")
    parser.add_argument("--node-env", action="append", default=[], metavar="KEY=VALUE",
                        help="Environment for every broker, for experiment switches. Repeatable")
    parser.add_argument("--rust-log", default="warn")
    parser.add_argument("--keep-data", action="store_true", help="Keep broker data after the run")
    args = parser.parse_args()
    if args.burst_pattern and not args.rate:
        parser.error("--burst-pattern needs --rate")
    if args.burst_pattern:
        cycle = sum(int(n) for n in args.burst_pattern.split(",")) / args.rate
        if args.warmup_secs % cycle or args.duration_secs % cycle:
            parser.error(f"warmup and duration must be whole burst cycles of {cycle:g}s")
    if not 1 <= args.prefetch <= 2000:
        parser.error("the comparison client takes --prefetch from 1 to 2000")
    if not 1 <= args.connections <= 16 or args.prefetch % args.connections:
        parser.error("--connections must be 1 to 16 and divide --prefetch")
    if args.node_storage and args.copies != 3:
        parser.error("--node-storage needs --copies 3")
    for name in ("output", "storage", "bin_dir", "compare_bin"):
        setattr(args, name, getattr(args, name).resolve())
    args.node_storage = [p.resolve() for p in args.node_storage] if args.node_storage else None
    args.output.mkdir(parents=True, exist_ok=False)
    args.storage.mkdir(parents=True, exist_ok=True)
    save(args.output / "environment.json", {
        "platform": platform.platform(), "argv": {k: str(v) for k, v in vars(args).items()},
        "provenance": provenance(args),
        "storage": json.loads(subprocess.check_output(["findmnt", "--json", "--target", str(args.storage)], text=True)),
        "thp": Path("/sys/kernel/mm/transparent_hugepage/enabled").read_text().strip()})
    run(args)


if __name__ == "__main__":
    main()
