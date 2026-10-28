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
# Length of the traffic-free window that measures idle queue cost.
IDLE_WINDOW_SECS = 10
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
                # Fields 10 and 12 of /proc/<pid>/stat, counted after the
                # command name.
                "minor_faults": int(stat[7]), "major_faults": int(stat[9]),
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
    lines += [f"{key} = {value}" for key, value in (item.split("=", 1) for item in args.replication)]
    return "\n".join(lines) + "\n"


def publish_settings(args):
    # Numeric values are gaps, the rest are mode names.
    return {k: int(v) if v.isdigit() else v for k, v in (item.split("=", 1) for item in args.publish)}


def run(args):
    case = args.output
    copies = args.copies
    publish = publish_settings(args)
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
        if any(settings.get("publish", {}).get(k) != v for k, v in publish.items()):
            return None
        if not (response.get("local_application") or {}).get("broker_matches_saved"):
            return None
        return response

    try:
        cpusets = args.broker_cpusets.split(";") if args.broker_cpusets else []
        for i in range(copies):
            node_data[i].mkdir(parents=True)
            node_env = dict(env, FIBRIL_TASK_POLLS_PATH=str(case / f"node-{i}-polls.json"),
                            FIBRIL_TASK_BUSY_PATH=str(case / f"node-{i}-busy.json"),
                            FIBRIL_LONG_POLLS_PATH=str(case / f"node-{i}-long-polls.json"))
            if args.stage_trace:
                node_env["KERATIN_WRITER_STAGE_TRACE"] = str(case / f"node-{i}-stage.jsonl")
            if not args.no_diagnostics:
                node_env.update(KERATIN_DRAIN_COUNTERS_PATH=str(case / f"node-{i}-counters.json"),
                                FIBRIL_PIPELINE_COUNTERS_PATH=str(case / f"node-{i}-pipeline.json"))
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
        if args.idle_queues is not None:
            for k in range(args.idle_queues):
                http(endpoints[0], "/admin/api/queues",
                     {"topic": f"idle{k}", "partition_count": 1, "durability": args.durability}, timeout=30)
            # Every node opens a log for each queue it owns or follows.
            per_node = args.idle_queues + args.queues
            opened = lambda: all(sum(1 for q in http(b, "/admin/api/queues_debug", timeout=30)["queues"]
                                     if q.get("materialized")) >= per_node for b in endpoints)
            wait_for(opened, 600)
            time.sleep(5)
            # A quiet window with no traffic at all: what idle queues cost.
            idle_polls = lambda kind="polls": [json.loads(f.read_text()) if f.exists() else {}
                                               for f in (case / f"node-{i}-{kind}.json" for i in range(copies))]
            polls_start, busy_start = idle_polls(), idle_polls("busy")
            idle_start = [process_sample(p) for p in servers]
            time.sleep(args.idle_window_secs)
            idle = [process_sample(p) for p in servers]
            save(case / "idle-polls.json", {"before": polls_start, "after": idle_polls(),
                                            "secs": args.idle_window_secs})
            save(case / "idle-busy.json", {"before": busy_start, "after": idle_polls("busy"),
                                           "secs": args.idle_window_secs})
            save(case / "idle.json", idle)
            save(case / "idle-start.json", idle_start)
        if args.sync_interval_ms is not None or publish:
            current = http(endpoints[0], "/admin/api/runtime-settings")
            if args.sync_interval_ms is not None:
                current["settings"]["storage"]["background_sync_interval_ms"] = args.sync_interval_ms
            current["settings"].setdefault("publish", {}).update(publish)
            http(endpoints[0], "/admin/api/runtime-settings", method="PUT", timeout=10,
                 body={"expected_version": current["version"], "settings": current["settings"]})
        for i, endpoint in enumerate(endpoints):
            save(case / f"node-{i}-runtime-before.json", wait_for(lambda e=endpoint: settings_applied(e)))

        # Task poll counts, written once a second by a task-polls build.
        read_polls = lambda: [json.loads(f.read_text()) if f.exists() else {}
                              for f in (case / f"node-{i}-polls.json" for i in range(copies))]
        time.sleep(1.2)
        polls_before = read_polls()
        read_pipeline = lambda: [json.loads(f.read_text()) if f.exists() else {}
                                 for f in (case / f"node-{i}-pipeline.json" for i in range(copies))]
        pipeline_before = read_pipeline()
        read_long_polls = lambda: [json.loads(f.read_text()) if f.exists() else {}
                                   for f in (case / f"node-{i}-long-polls.json" for i in range(copies))]
        long_polls_before = read_long_polls()
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
                       "--duration-secs", str(args.duration_secs),
                       # Redeliveries after a failover can take a lease timeout to arrive.
                       "--drain-secs", "45" if args.survive_broker_loss else "15",
                       "--output", str(case / f"client-{queue}.json")]
            if args.unconfirmed:
                command += ["--fibril-unconfirmed"]
            if args.burst_pattern:
                command += ["--burst-pattern", args.burst_pattern]
            if args.survive_broker_loss:
                command += ["--survive-broker-loss", "--fibril-discovery",
                            ",".join(f"127.0.0.1:{broker_port(i)}" for i in range(copies))]
            command += ["--rate", str(args.rate)] if args.rate else ["--saturation"]
            if args.rate:
                # The client refuses a paced plan above its message safety
                # limit (50M by default), which long soaks exceed.
                planned = args.rate * (args.warmup_secs + args.duration_secs)
                command += ["--max-messages", str(max(50_000_000, planned + planned // 10 + 1000))]
            clients.append(launch(command, f"client-{queue}", env, case))

        started = time.monotonic()
        lost_brokers = {}
        killed = False
        while any(c.poll() is None for c in clients):
            elapsed = time.monotonic() - started
            samples.append({"elapsed_s": elapsed, "meminfo": meminfo(),
                            "servers": [process_sample(p) for p in servers]})
            if args.kill_owner_at_secs is not None and not killed and elapsed >= args.kill_owner_at_secs:
                servers[owner_index(queues[0])].kill()
                killed = True
            for i, p in enumerate(servers):
                if p.poll() is not None and i not in lost_brokers:
                    if not args.survive_broker_loss:
                        raise RuntimeError("a broker exited during the workload")
                    lost_brokers[i] = {"node": i, "elapsed_s": round(elapsed, 1), "returncode": p.returncode}
                    print(f"broker {i} exited at {elapsed:.0f} s with {p.returncode}, continuing", flush=True)
            if any(c.poll() not in (None, 0) for c in clients):
                raise RuntimeError("a client failed, see the client logs")
            if time.monotonic() - started > args.duration_secs + args.warmup_secs + (90 if args.survive_broker_loss else 60):
                raise TimeoutError("workload deadline exceeded")
            time.sleep(1)
        for profiler in profilers:
            profiler.send_signal(2)
        for profiler in profilers:
            profiler.wait(timeout=120)

        if lost_brokers:
            survived = []
            for queue in queues:
                one = json.loads((case / f"client-{queue}.json").read_text())
                if not one.get("failover"):
                    raise RuntimeError(f"client on {queue} did not finish after the broker loss, see its log")
                survived.append({"queue": queue, **one["failover"],
                                 "latency_ms": one["delivery"]["from_schedule"]})
            verdict = "passed" if all(r["verdict"] == "pass" for r in survived) else "failed"
            save(case / "summary.json", {"status": verdict, "broker_loss": list(lost_brokers.values()),
                                         "failover": survived,
                                         "latency_ms": [r["latency_ms"] for r in survived]})
            for r in survived:
                print(f"  {r['queue']}: confirmed {r['confirmed']}, delivered {r['delivered_unique']}, "
                      f"lost {r['lost']}, phantom {r['phantom']}, unknown outcome {r['uncertain']}, "
                      f"repeats {r['repeats']}", flush=True)
            if verdict != "passed":
                raise RuntimeError(f"broker loss check failed: {survived}")
            print(f"passed {case.name} with broker loss", flush=True)
            return

        results = []
        for queue in queues:
            one = json.loads((case / f"client-{queue}.json").read_text())
            counts = one["counts"]
            confirms = args.connections if args.unconfirmed else counts["issued"]
            if (one["status"] not in ("client_validated", "client_validated_with_failover")
                    or (one.get("failover") or {}).get("verdict", "pass") != "pass"
                    or counts["issued"] <= args.connections
                    or counts["confirmed"] != confirms
                    or counts["delivered"] != counts["issued"] or counts["ack_sent"] != counts["issued"]):
                raise RuntimeError(f"client counts failed validation on {queue}: {counts}")
            results.append(one)
        # Delivery counts alone cannot show settlement or catch duplicates.
        save(case / "settled.json", wait_for(lambda: topology(settled=True), 120))
        after = [process_sample(p) for p in servers]
        time.sleep(1.2)
        save(case / "polls.json", {"before": polls_before, "after": read_polls()})
        save(case / "pipeline.json", {"before": pipeline_before, "after": read_pipeline()})
        save(case / "long-polls.json", {"before": long_polls_before, "after": read_long_polls()})
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


def histogram_delta(before, after):
    """Mean, bucket upper bounds for p50 and p99, and long waits, in microseconds."""
    if not after:
        return None
    count = after["count"] - (before or {}).get("count", 0)
    buckets = [a - b for a, b in zip(after["buckets"], (before or {}).get("buckets", [0] * len(after["buckets"])))]
    if count <= 0:
        return {"count": 0}

    def upper(q):
        seen = 0
        for i, n in enumerate(buckets):
            seen += n
            if seen >= q * count:
                return 2 ** (i + 1)
        return None
    return {"count": count, "mean_us": (after["total_us"] - (before or {}).get("total_us", 0)) / count,
            "p50_under_us": upper(0.5), "p99_under_us": upper(0.99),
            "over_1ms": sum(buckets[10:]), "over_4ms": sum(buckets[12:]), "run_max_us": after["max_us"]}


def pipeline_summary(case):
    """Replication pipeline counters per node, over the measured window."""
    path = case / "pipeline.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    nodes = []
    for b, a in zip(data["before"], data["after"]):
        if not a:
            nodes.append(None)
            continue
        d = lambda key: a.get(key, 0) - b.get(key, 0)
        per = lambda num, den: d(num) / d(den) if d(den) else None
        nodes.append({
            "owner_reads": d("owner_reads"),
            "owner_credit_capped_share": per("owner_credit_capped_reads", "owner_reads"),
            "owner_records_per_frame": per("owner_frame_records", "owner_frames"),
            "owner_kib_per_frame": per("owner_frame_bytes", "owner_frames") and per("owner_frame_bytes", "owner_frames") / 1024,
            "follower_applies": d("follower_applies"),
            "follower_frames_per_apply": per("follower_frames", "follower_applies"),
            "follower_records_per_apply": per("follower_records", "follower_applies"),
            "follower_kib_per_apply": per("follower_bytes", "follower_applies") and per("follower_bytes", "follower_applies") / 1024,
            "follower_backlogged_share": per("follower_backlogged_applies", "follower_applies"),
            "sink_messages_per_batch": per("sink_messages", "sink_batches"),
            **{name: histogram_delta(b.get(name), a.get(name)) for name in (
                "owner_read", "owner_send", "owner_credit_park", "follower_idle", "follower_collect",
                "follower_apply", "follower_progress_send", "sink_wait", "sink_gather", "sink_append",
                "sink_confirm_send", "confirm_local", "confirm_replicated", "probe_sleep_late", "probe_yield")},
        })
    return nodes


def long_polls_summary(case):
    path = case / "long-polls.json"
    if not path.exists():
        return None
    data = json.loads(path.read_text())
    nodes = []
    for b, a in zip(data["before"], data["after"]):
        rows = {}
        for site, (n1, n4, total, longest) in a.items():
            p1, p4, ptotal, _ = b.get(site, [0, 0, 0, 0])
            if n1 - p1:
                rows[site] = [n1 - p1, n4 - p4, round((total - ptotal) / 1000, 1), round(longest / 1000, 1)]
        nodes.append(dict(sorted(rows.items(), key=lambda kv: -kv[1][2])))
    return nodes if any(nodes) else None


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
    idle_start = json.loads((case / "idle-start.json").read_text()) if (case / "idle-start.json").exists() else None
    if idle and idle_start:
        for start, end in zip(idle_start, idle):
            end["idle_cpu_ms_per_sec"] = (end["cpu_seconds"] - start["cpu_seconds"]) * 1000 / args.idle_window_secs
            end["idle_wakeups_per_sec"] = (end["voluntary_switches"] - start["voluntary_switches"]) / args.idle_window_secs
            by_thread = end.get("voluntary_switches_by_thread", {})
            start_by_thread = start.get("voluntary_switches_by_thread", {})
            end["idle_wakeups_per_sec_by_thread"] = {
                name: (n - start_by_thread.get(name, 0)) / args.idle_window_secs for name, n in by_thread.items()}
    return {
        "status": "passed",
        "idle": idle and {"queues": args.idle_queues,
                          "rss_mib_per_node": [i.get("rss_kib", 0) / 1024 for i in idle],
                          "threads_per_node": [i.get("threads") for i in idle],
                          "cpu_ms_per_sec_per_node": [i.get("idle_cpu_ms_per_sec") for i in idle],
                          "wakeups_per_sec_per_node": [i.get("idle_wakeups_per_sec") for i in idle],
                          "wakeups_per_sec_by_thread": [i.get("idle_wakeups_per_sec_by_thread") for i in idle],
                          # Only from a task-polls build. Polls per second by
                          # spawn site over the quiet window, per node.
                          "polls_per_sec_by_site": idle_polls_summary(case),
                          # Busy microseconds per second by spawn site.
                          "busy_us_per_sec_by_site": idle_polls_summary(case, "idle-busy.json")},
        "label": {"nodes": args.copies, "policy": args.assignment if args.copies == 3 else "standalone",
                  "queue_mode": args.durability, "queues": args.queues, "payload_bytes": args.size,
                  "rate_per_queue": args.rate or "saturation", "burst_pattern": args.burst_pattern,
                  "confirm_window": args.confirm_window, "prefetch": args.prefetch,
                  "connections": args.connections, "idle_queues": args.idle_queues,
                  "early_replication": bool(args.early_replication), "linger_us": args.linger_us,
                  "sync_interval_ms": args.sync_interval_ms, "publish": publish_settings(args) or None,
                  "replication": args.replication or None, "unconfirmed": args.unconfirmed,
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
        "pipeline": pipeline_summary(case),
        # Only from a task-polls build. Polls of 1 ms or more by spawn site, per
        # node: over 1 ms, over 4 ms, total ms and the run's longest in ms.
        "long_polls_by_node": long_polls_summary(case),
        "peak_rss_mib": max((sum(s.get("rss_kib", 0) for s in sample["servers"]) for sample in samples), default=0) / 1024,
        "peak_dirty_mib": max((s["meminfo"]["Dirty"] for s in samples), default=0) / 1024,
    }


def idle_polls_summary(case, name="idle-polls.json"):
    path = case / name
    if not path.exists():
        return None
    polls = json.loads(path.read_text())
    out = []
    for b, a in zip(polls["before"], polls["after"]):
        rates = {site: (n - b.get(site, 0)) / polls["secs"] for site, n in a.items()}
        out.append({site: round(r, 2) for site, r in sorted(rates.items(), key=lambda kv: -kv[1]) if r > 0})
    return out


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
    parser.add_argument("--idle-queues", type=int,
                        help="Extra queues declared and left without traffic, then a quiet window measures their cost. 0 measures the baseline")
    parser.add_argument("--confirm-window", type=int, default=32768)
    parser.add_argument("--prefetch", type=int, default=2000)
    parser.add_argument("--warmup-secs", type=int, default=3)
    parser.add_argument("--duration-secs", type=int, default=12)
    parser.add_argument("--unconfirmed", action="store_true", help="Publish without confirms, as an upper bound")
    parser.add_argument("--early-replication", type=int, choices=[0, 1], default=1)
    parser.add_argument("--linger-us", type=int, default=2000, help="Follower apply linger")
    parser.add_argument("--sync-interval-ms", type=int, help="Background sync interval for buffered queues")
    parser.add_argument("--publish", action="append", default=[], metavar="KEY=VALUE",
                        help="Publish runtime setting, for example buffered_coalesce=window or durable_gap_us=50")
    parser.add_argument("--replication", action="append", default=[], metavar="KEY=VALUE",
                        help="Replication setting seeded at first boot, as TOML, for example "
                             "stream_initial_credit_bytes=33554432")
    parser.add_argument("--preallocate-bytes", type=int)
    parser.add_argument("--broker-cpusets", help="Per-node CPU lists for taskset, for example '0-3;4-7;8-11'")
    parser.add_argument("--perf", choices=["flat", "fp", "switches"],
                        help="Record one broker with perf: flat samples, frame-pointer stacks or context switches")
    parser.add_argument("--perf-role", choices=["owner", "follower"], default="owner")
    parser.add_argument("--node-env", action="append", default=[], metavar="KEY=VALUE",
                        help="Environment for every broker, for experiment switches. Repeatable")
    parser.add_argument("--rust-log", default="warn")
    parser.add_argument("--no-diagnostics", action="store_true",
                        help="Leave the pipeline and Keratin counter files off. The pipeline counters' runtime probe "
                             "wakes a worker every 1 ms, so idle wakeups and CPU then match a production broker. "
                             "Task polls from a task-polls build are still recorded")
    parser.add_argument("--stage-trace", action="store_true",
                        help="Write each broker's Keratin writer stage trace to node-N-stage.jsonl. "
                             "Needs a build with the writer-stage-trace feature")
    parser.add_argument("--idle-window-secs", type=int, default=IDLE_WINDOW_SECS,
                        help="Length of the quiet window measured after --idle-queues are open")
    parser.add_argument("--keep-data", action="store_true", help="Keep broker data after the run")
    parser.add_argument("--survive-broker-loss", action="store_true",
                        help="Keep running when a broker dies, then check that every confirmed message "
                             "was delivered and nothing unsent arrived")
    parser.add_argument("--kill-owner-at-secs", type=float, default=None,
                        help="SIGKILL the first queue's owner this many seconds into the workload "
                             "(implies --survive-broker-loss)")
    args = parser.parse_args()
    if args.kill_owner_at_secs is not None:
        args.survive_broker_loss = True
    if args.burst_pattern and not args.rate:
        parser.error("--burst-pattern needs --rate")
    if args.burst_pattern:
        # In whole messages: a float cycle like 0.7 s does not divide 7 s exactly.
        total = sum(int(n) for n in args.burst_pattern.split(","))
        if (args.warmup_secs * args.rate) % total or (args.duration_secs * args.rate) % total:
            parser.error(f"warmup and duration must be whole burst cycles of {total / args.rate:g}s")
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
