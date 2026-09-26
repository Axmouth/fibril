#!/usr/bin/env python3
"""Repeat the original two-process e2e_c workload on fresh solo/three-copy nodes.

Build first with cargo build --release --bin fibril-server --bin e2e_c.
This preserves the historical unconfirmed writer and latency-tracking reader.
Results and data remain under explicit persistent directories, including failures.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import socket
import subprocess
import time
import urllib.error
import urllib.request


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def provenance(bin_dir):
    binaries = {}
    for name in ("fibril-server", "e2e_c"):
        path = bin_dir / name
        with path.open("rb") as binary:
            binaries[name] = {"path": str(path.resolve()),
                              "sha256": hashlib.file_digest(binary, "sha256").hexdigest()}
    source = Path(__file__).resolve().parents[1]
    repositories = {}
    for name, path in (("fibril", source), ("keratin", source.parent / "keratin"),
                       ("ganglion", source.parent / "ganglion")):
        try:
            repositories[name] = {
                "revision": subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip(),
                "status": subprocess.check_output(["git", "-C", str(path), "status", "--porcelain"], text=True),
            }
        except subprocess.CalledProcessError:
            repositories[name] = {"unavailable": True}
    return {"binaries": binaries, "repositories_at_run_time": repositories,
            "note": "Repository state does not prove the build provenance of prebuilt binaries."}


def http(base, path, body=None):
    request = urllib.request.Request(
        base + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=3) as response:
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
    raise TimeoutError(f"Readiness deadline exceeded: {error}")


def ports(count):
    sockets = [socket.socket() for _ in range(count)]
    try:
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [sock.getsockname()[1] for sock in sockets]
    finally:
        for sock in sockets:
            sock.close()


def process_sample(process):
    try:
        stat = Path(f"/proc/{process.pid}/stat").read_text().split(")", 1)[1].split()
        status = Path(f"/proc/{process.pid}/status").read_text()
        rss = re.search(r"^VmRSS:\s+(\d+)", status, re.M)
        return {"pid": process.pid, "rss_kib": int(rss[1]) if rss else 0,
                "cpu_seconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK")}
    except (FileNotFoundError, ProcessLookupError):
        return {"pid": process.pid, "exited": True}


def panic_in_log(path):
    with path.open("rb") as log:
        log.seek(max(0, path.stat().st_size - 16384))
        return b"panicked at" in log.read()


def run(args, copies):
    case = args.output / f"copies-{copies}"
    data = args.storage / f"{args.output.name}-copies-{copies}"
    case.mkdir(parents=True, exist_ok=False)
    data.mkdir(parents=True, exist_ok=False)
    allocated = ports(copies * 3)
    endpoints = [f"http://127.0.0.1:{allocated[i * 3 + 1]}" for i in range(copies)]
    processes, logs, servers = [], [], []
    secret = secrets.token_hex(16)
    samples = []
    env = {key: value for key, value in os.environ.items()
           if not key.startswith("FIBRIL_")}
    env.update(RUST_LOG="warn", FIBRIL_AUTH_USERNAME="fibril", FIBRIL_AUTH_PASSWORD="fibril")

    def launch(command, name, environment=env, cwd=case):
        log = (case / f"{name}.log").open("w")
        logs.append(log)
        process = subprocess.Popen(command, cwd=cwd, env=environment, stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        save(case / f"{name}-command.json", command)
        return process

    def topology(settled=None):
        tops = [http(base, "/admin/api/topology") for base in endpoints]
        debug = [http(base, "/admin/api/queues_debug") for base in endpoints]
        states = [next((q for q in d["queues"] if q["topic"] == "topic1"), None) for d in debug]
        if not all(states):
            return None
        if copies == 3:
            assignments = [[a for a in t["coordination"]["assignments"] if a["topic"] == "topic1"] for t in tops]
            if not all(len(a) == 1 and len(a[0]["followers"]) == 2 for a in assignments):
                return None
            if not all(a == assignments[0] for a in assignments):
                return None
            assignment = assignments[0][0]
            if assignment["durability"] != {"mode": "majority_durable"}:
                raise RuntimeError(f"Unexpected durability: {assignment}")
            for t, state, d in zip(tops, states, debug):
                owner = t["coordination"]["node_id"] == assignment["owner"]
                if state["role"] != ("owner" if owner else "follower") or not state["materialized"]:
                    return None
                if owner:
                    progress = next((r for r in d["owned_replicas"] if r["topic"] == "topic1"), None)
                    if not progress or len(progress["followers"]) != 2 or not all(f["in_sync"] for f in progress["followers"]):
                        return None
        if settled is not None and not all(q["state"]["settled_until"] == settled and
                q["state"]["ready_count"] == q["state"]["inflight_count"] == 0 for q in states):
            return None
        return {"topology": tops, "queues": debug}

    try:
        for i in range(copies):
            node_dir = data / f"node-{i}"
            node_dir.mkdir()
            node_env = env.copy()
            command = [str(args.bin_dir / "fibril-server"), "--data-dir", str(node_dir / "data"),
                       "--broker-bind", f"127.0.0.1:{allocated[i * 3]}",
                       "--admin-bind", f"127.0.0.1:{allocated[i * 3 + 1]}"]
            if copies == 3:
                config = case / f"node-{i}.toml"
                config.write_text("[coordination.ganglion]\ntarget_followers = 2\n")
                command += ["--config", str(config)]
                node_env.update(FIBRIL_COORDINATION_MODE="ganglion", FIBRIL_CLUSTER_SECRET=secret,
                    FIBRIL_COORDINATION_NODE_ID=f"broker-{i + 1}", FIBRIL_COORDINATION_RAFT_ID=str(i + 1),
                    FIBRIL_COORDINATION_LISTEN=f"127.0.0.1:{allocated[i * 3 + 2]}",
                    FIBRIL_COORDINATION_BOOTSTRAP=str(i == 0).lower(),
                    FIBRIL_COORDINATION_PEERS=",".join(f"{j + 1}=127.0.0.1:{allocated[j * 3 + 2]}" for j in range(copies)),
                    FIBRIL_COORDINATION_ASSIGNMENT_DURABILITY="majority_durable")
            servers.append(launch(command, f"node-{i}", node_env, node_dir))
        for i, endpoint in enumerate(endpoints):
            save(case / f"node-{i}-settings.json", wait_for(lambda: http(endpoint, "/admin/api/startup-config")))
        if copies == 3:
            wait_for(lambda: all(len(http(base, "/admin/api/topology")["coordination"]["nodes"]) == 3 for base in endpoints))
        save(case / "declare.json", http(endpoints[0], "/admin/api/queues", {"topic": "topic1", "partition_count": 1}))
        save(case / "before.json", wait_for(topology))
        ready = case / "ready"
        ready.mkdir()
        common = [str(args.bin_dir / "e2e_c"), "-m", str(args.messages), "-c", str(args.clients),
                  "--addr", f"127.0.0.1:{allocated[0]}"]
        reader = launch(common + ["--reader", "--ready-dir", str(ready),
                                  "--prefetch", str(args.prefetch)], "reader")
        wait_for(lambda: len(list(ready.glob("*.ready"))) == args.clients)
        time.sleep(0.5)
        writer = launch(common + ["--writer", "--size", str(args.size)], "writer")
        started = time.monotonic()
        while reader.poll() is None or writer.poll() is None:
            samples.append({"elapsed_s": time.monotonic() - started,
                            "servers": [process_sample(p) for p in servers],
                            "reader": process_sample(reader), "writer": process_sample(writer)})
            if any(p.poll() is not None for p in servers):
                raise RuntimeError("A broker exited during the workload")
            if any(p.poll() not in (None, 0) for p in (reader, writer)) or any(
                    panic_in_log(case / f"{name}.log") for name in ("reader", "writer")):
                raise RuntimeError("Client failure during workload, see reader/writer logs")
            if time.monotonic() - started > args.timeout:
                raise TimeoutError("Workload deadline exceeded")
            time.sleep(1)
        if reader.returncode or writer.returncode:
            raise RuntimeError(f"Client failed: reader={reader.returncode}, writer={writer.returncode}")
        text = (case / "reader.log").read_text()
        expected = args.messages * args.clients
        if f"Expected receive count: {expected}, missing: 0" not in text:
            raise RuntimeError("Reader did not report the expected receive count")
        if f"Sent: {expected}, received: 0" not in (case / "writer.log").read_text():
            raise RuntimeError("Writer did not report the expected send count")
        # Delivery counts alone cannot establish settlement or catch duplicates.
        save(case / "settled.json", wait_for(lambda: topology(expected), 120))
        save(case / "result.json", {"status": "passed", "copies": copies, "messages": expected,
            "payload_bytes": args.size, "clients": args.clients, "prefetch": args.prefetch, "data": str(data),
            "writer_confirms": False, "warmup": False,
            "sampled_peak_broker_rss_kib": max(sum(p.get("rss_kib", 0) for p in s["servers"]) for s in samples)})
        print(f"copies={copies}: complete, results={case}", flush=True)
    except BaseException as error:
        save(case / "result.json", {"status": "failed", "error": str(error), "data": str(data)})
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, default=Path("target/release"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--storage", type=Path, required=True)
    parser.add_argument("--copies", type=int, choices=[1, 3], nargs="+", default=[1, 3])
    parser.add_argument("--messages", type=int, default=500_000, help="Messages per connection")
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--size", type=int, default=1024)
    parser.add_argument("--prefetch", type=int, default=16384)
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()
    for name in ("messages", "clients", "size", "prefetch", "timeout"):
        if getattr(args, name) < 1:
            parser.error(f"{name} must be positive")
    for name in ("bin_dir", "output", "storage"):
        setattr(args, name, getattr(args, name).resolve())
    args.output.mkdir(parents=True, exist_ok=False)
    args.storage.mkdir(parents=True, exist_ok=True)
    save(args.output / "environment.json", {"platform": platform.platform(),
        "argv": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "provenance": provenance(args.bin_dir),
        "thp": Path("/sys/kernel/mm/transparent_hugepage/enabled").read_text().strip()})
    for copies in args.copies:
        run(args, copies)


if __name__ == "__main__":
    main()
