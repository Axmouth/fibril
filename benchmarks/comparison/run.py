#!/usr/bin/env python3
"""Provision one isolated broker at a time; keep workload logic in Rust."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parent
REPO = ROOT.parent.parent
IMAGES = {
    "fibril": "ghcr.io/axmouth/fibril-server@sha256:8c5a722c6b134141f9d0aced286ecf5c9ca1627de4d325652d287bc72aadca7b",
    "nats": "nats@sha256:ac8f88a6494bffc2c2a5289a0ca61cb28a9145c11ba5677cf24265d07f46d8d4",
    "rabbitmq": "rabbitmq@sha256:a7faa436fbb827d5554ef3213de39bb0d858a8e7caf184179db568630b21c683",
}

def call(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()

def require(condition, message):
    if not condition:
        raise RuntimeError(message)

def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")

def sha(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024*1024), b""):
            h.update(block)
    return h.hexdigest()

def http(url, rabbit=False):
    request = urllib.request.Request(url)
    if rabbit:
        request.add_header("Authorization", "Basic " + base64.b64encode(b"bench:bench").decode())
    with urllib.request.urlopen(request, timeout=3) as response:
        return json.load(response)

def wait_for(fn, seconds=60):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result is not None and result is not False:
                return result
        except (OSError, ValueError, KeyError) as e:
            last = e
        time.sleep(0.2)
    raise TimeoutError(f"readiness/settlement deadline: {last}")

def cgroup_path(pid):
    entries = Path(f"/proc/{pid}/cgroup").read_text().splitlines()
    relative = next((line[3:] for line in entries if line.startswith("0::")), None)
    if relative is None:
        return None
    return Path("/sys/fs/cgroup") / relative.lstrip("/")

def cgroup_sample(path):
    if path is None:
        return {"unavailable": "requires Linux cgroup v2"}
    try:
        pairs = lambda name: dict(line.split() for line in (path/name).read_text().splitlines())
        cpu, mem = pairs("cpu.stat"), pairs("memory.stat")
        return {"cpu_usec": int(cpu["usage_usec"]), "memory_current_bytes": int((path/"memory.current").read_text()),
                "anon_bytes": int(mem["anon"]), "file_bytes": int(mem["file"]),
                "pids": int((path/"pids.current").read_text())}
    except OSError as e:
        return {"unavailable": str(e)}

def summarize_resources(samples):
    rows = [r["broker"] for r in samples if "cpu_usec" in r["broker"]]
    if not rows:
        return {"available": False}
    return {"available": True, "scope": "container cgroup, client setup through final settlement",
            "cpu_seconds": (rows[-1]["cpu_usec"]-rows[0]["cpu_usec"])/1e6,
            **{f"sampled_peak_{key}": max(r[key] for r in rows)
               for key in ["memory_current_bytes", "anon_bytes", "file_bytes", "pids"]}}

def verify_server(kind, base, queue, result):
    count = result["counts"]["issued"]
    if kind == "fibril":
        data = http(base + "/admin/api/queues_debug")
        queues = [q for q in data["queues"] if q["topic"] == queue]
        if len(queues) != 1:
            return None
        state = queues[0]["state"]
        if not (state["ready_count"] == state["inflight_count"] == 0 and state["settled_until"] == count):
            return None
        return data
    if kind == "rabbitmq":
        data = http(base + "/api/queues/%2F/" + queue, True)
        require(data["type"] == "quorum" and data["durable"], "Rabbit queue contract mismatch")
        require(len(data["members"]) == 1, "Rabbit replica count mismatch")
        if data.get("messages_ready") != 0 or data.get("messages_unacknowledged") != 0:
            return None
        # Updated queue statistics are eventually sampled. Both delivery and ACK
        # counters must cover the run, avoiding a stale pre-workload zero snapshot.
        stats = data.get("message_stats", {})
        if stats.get("ack", 0) < count or stats.get("deliver", 0) < count:
            return None
        return data
    data = http(base + "/jsz?streams=true&consumers=true&config=true")
    streams = [s for a in data.get("account_details", []) for s in a.get("stream_detail", []) if s["name"] == queue]
    if len(streams) != 1:
        return None
    stream = streams[0]
    if stream["state"]["messages"] != 0 or stream["state"]["last_seq"] != count:
        return None
    return data

def run_case(args, kind, repeat, binary, images, out):
    case = out / f"{repeat:02d}-{kind}"
    case.mkdir()
    project = "brokerbench-" + os.urandom(6).hex()
    data = Path(tempfile.mkdtemp(prefix=project+"-", dir=args.data_root))
    config = case/"nats.conf"
    config.write_text(f'port: 4222\nhttp_port: 8222\njetstream {{\n store_dir: /data\n sync_interval: "{args.nats_sync}"\n}}\n')
    env = dict(os.environ, FIBRIL_IMAGE=images["fibril"], NATS_IMAGE=images["nats"], RABBITMQ_IMAGE=images["rabbitmq"],
               BENCH_UID=str(os.getuid()), BENCH_GID=str(os.getgid()), BENCH_DATA=str(data), NATS_CONFIG=str(config))
    compose = ["docker", "compose", "-f", str(ROOT/"docker/compose.yaml"), "-p", project]
    def dc(*parts):
        return call(*compose, *parts, env=env)
    samples, proc, cid = [], None, None
    start = time.monotonic()
    log = (case/"client.log").open("w")
    try:
        (case/"compose.yaml").write_text(dc("config"))
        save(case/"placement.json", {"data": str(data), "mount": json.loads(call("findmnt", "-J", "-T", str(data)))})
        print(f"Starting {case.name}", flush=True)
        dc("up", "-d", "--no-deps", kind)
        cid = dc("ps", "-q", kind)
        inspected = json.loads(call("docker", "inspect", cid))[0]
        save(case/"container.json", inspected)
        if not inspected["State"]["Running"]:
            raise RuntimeError("broker exited during startup; see server.log")
        ports = inspected["NetworkSettings"]["Ports"]
        port = lambda p: ports[f"{p}/tcp"][0]["HostPort"]
        base = "http://127.0.0.1:" + port({"fibril":8081, "nats":8222, "rabbitmq":15672}[kind])
        endpoint = {"fibril":lambda:"127.0.0.1:"+port(9876),
                    "nats":lambda:"nats://127.0.0.1:"+port(4222),
                    "rabbitmq":lambda:"amqp://bench:bench@127.0.0.1:"+port(5672)+"/%2f"}[kind]()
        ready_path = {"fibril":"/admin/api/startup-config", "nats":"/varz", "rabbitmq":"/api/overview"}[kind]
        ready = wait_for(lambda:http(base+ready_path, kind=="rabbitmq"))
        save(case/"server-settings.json", ready)
        if kind == "nats":
            actual = ready["jetstream"]["config"]
            require(bool(actual.get("sync_always",False)) == (args.nats_sync == "always"), f"NATS sync policy mismatch: {actual}")
            if args.nats_sync == "2m":
                require(actual["sync_interval"] == 120_000_000_000, f"NATS sync interval mismatch: {actual}")
        cg = cgroup_path(inspected["State"]["Pid"])
        save(case/"cgroup-limits.json", {name:(cg/name).read_text().strip() if cg and (cg/name).exists() else None
            for name in ["cpu.max","cpuset.cpus.effective","memory.max","memory.swap.max"]})
        sample = lambda: {"elapsed_secs":time.monotonic()-start, "broker":cgroup_sample(cg)}
        samples.append(sample())
        cmd = [str(binary), "--broker", kind, "--endpoint", endpoint, "--queue", "bench",
               "--payload-bytes", str(args.payload_bytes), "--warmup-secs", str(args.warmup_secs),
               "--duration-secs", str(args.duration_secs), "--drain-secs", str(args.drain_secs),
               "--confirm-window", str(args.confirm_window), "--prefetch", str(args.prefetch),
               "--pull-batch", str(args.pull_batch), "--workers", str(args.workers), "--max-messages", str(args.max_messages),
               "--nats-sync", args.nats_sync, "--output", str(case/"client.json")]
        cmd += ["--rate", str(args.rate)] if args.rate is not None else ["--saturation"]
        save(case/"command.json", cmd)
        proc = subprocess.Popen(cmd, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.monotonic()+args.warmup_secs+args.duration_secs+args.drain_secs+100
        while proc.poll() is None:
            if time.monotonic() > deadline:
                raise TimeoutError("client process timeout")
            samples.append(sample())
            time.sleep(.1)
        if proc.returncode != 0:
            raise RuntimeError(f"client failed; see {case/'client.log'}")
        result = json.loads((case/"client.json").read_text())
        require(result["status"] == "client_validated", "client did not validate")
        final = wait_for(lambda:verify_server(kind, base, "bench", result), 30)
        save(case/"settlement.json", final)
        samples.append(sample())
        result["status"] = "validated"
        result["broker_resources"] = summarize_resources(samples)
        result["contract"] = {"workload":"queue", "topology":"single node, single queue, one stored copy",
            "durability_group": "timed-sync" if kind=="nats" and args.nats_sync!="always" else "sync-before-publish-confirm",
            "sync_policy": args.nats_sync if kind=="nats" else "local_durable" if kind=="fibril" else "quorum-r1",
            "transport":"Docker bridge, loopback published ports", "image":images[kind]}
        save(case/"result.json",result)
        print(f"Validated {case.name}: {result['cohort_completed_per_sec']:,.0f}/s", flush=True)
        return result
    except Exception as error:
        save(case/"failure.json", {"error":str(error)})
        raise
    finally:
        if proc and proc.poll() is None:
            proc.kill(); proc.wait()
        log.close()
        save(case/"resources.json",samples)
        if cid:
            with (case/"server.log").open("w") as f:
                subprocess.run(["docker","logs",cid], stdout=f, stderr=subprocess.STDOUT, check=False)
        dc("down", "--volumes", "--remove-orphans", "--timeout", "10")
        shutil.rmtree(data)
        save(case/"cleanup.json", {"removed_data":not data.exists(), "compose_project":project})

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--broker", choices=["all", *IMAGES], default="all")
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--rate",type=int); mode.add_argument("--saturation",action="store_true")
    for name, default in [("payload-bytes",1024),("warmup-secs",5),("duration-secs",30),("drain-secs",120),
                          ("confirm-window",4096),("prefetch",1024),("pull-batch",1024),("workers",4),
                          ("max-messages",50_000_000),("repeats",1)]:
        p.add_argument("--"+name,type=int,default=default)
    p.add_argument("--nats-sync",choices=["always","2m"],default="always")
    p.add_argument("--data-root",type=Path,default=Path(tempfile.gettempdir()))
    p.add_argument("--output",type=Path)
    p.add_argument("--binary",type=Path,help="Use a prebuilt shared Rust harness")
    p.add_argument("--fibril-bin",type=Path,help="Build a runtime image with this local INLINE server binary")
    for kind in IMAGES:
        p.add_argument("--"+kind+"-image",default=IMAGES[kind])
    args = p.parse_args()
    if args.rate is not None and args.rate <= 0:
        p.error("--rate must be positive")
    if args.repeats < 1:
        p.error("--repeats must be positive")
    args.data_root = args.data_root.resolve()
    args.data_root.mkdir(parents=True, exist_ok=True)
    out = (args.output or ROOT/"results"/time.strftime("%Y%m%d-%H%M%S")).resolve()
    out.mkdir(parents=True,exist_ok=False)
    images = {kind:getattr(args,kind+"_image") for kind in IMAGES}
    if args.binary:
        binary = args.binary.resolve()
    else:
        subprocess.run(["cargo","build","--release","--locked","--manifest-path",str(ROOT/"Cargo.toml"),
                        "--target-dir",str(ROOT/"target")],check=True)
        binary = ROOT/"target/release/broker-compare"
    if args.fibril_bin:
        with tempfile.TemporaryDirectory(prefix="brokerbench-image-") as temp:
            context = Path(temp)
            rootfs = context/"rootfs"
            binary_path = args.fibril_bin.resolve()
            ldd = call("ldd", str(binary_path))
            files = {binary_path:Path("/usr/local/bin/fibril-server")}
            files.update({Path(lib):Path(lib) for lib in re.findall(r"(/[^\s]+)", ldd)})
            records = []
            for source, target in files.items():
                dest = rootfs / str(target).lstrip("/")
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, dest)
                records.append({"source":str(source), "container_path":str(target), "sha256":sha(dest)})
            (rootfs/"tmp").mkdir(); (rootfs/"tmp").chmod(0o1777)
            save(out/"fibril-image-inputs.json", {"ldd":ldd,"files":records})
            shutil.copy2(ROOT/"docker/Dockerfile.binary",context/"Dockerfile")
            image = "fibril-benchmark:"+sha(binary_path)[:16]
            subprocess.run(["docker","build","--network=none","-t",image,str(context)],check=True)
            images["fibril"] = image
    kinds = list(IMAGES) if args.broker=="all" else [args.broker]
    image_info = {}
    for kind in kinds:
        if subprocess.run(["docker","image","inspect",images[kind]],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode:
            subprocess.run(["docker","pull",images[kind]],check=True)
        image_info[kind] = json.loads(call("docker","image","inspect",images[kind]))[0]
        # Run by immutable local ID even when the caller supplied a mutable tag.
        images[kind] = image_info[kind]["Id"]
    source_files = [ROOT/"README.md",ROOT/"Cargo.toml",ROOT/"Cargo.lock",ROOT/"run.py",ROOT/"run.sh",ROOT/"table.py",*sorted((ROOT/"src").glob("*.rs")),*sorted((ROOT/"docker").glob("*"))]
    save(out/"provenance.json", {"args":{k:str(v) if isinstance(v,Path) else v for k,v in vars(args).items()},
        "host":platform.uname()._asdict(),"cpu":call("lscpu"),"clock_ticks_per_second":os.sysconf("SC_CLK_TCK"),
        "git_head":call("git","-C",str(REPO),"rev-parse","HEAD"),"git_status":call("git","-C",str(REPO),"status","--short"),
        "tracked_diff_sha256":hashlib.sha256(call("git","-C",str(REPO),"diff","HEAD").encode()).hexdigest(),
        "rustc":call("rustc","--version"),"docker":call("docker","version","--format","{{.Server.Version}}"),
        "harness_sha256":sha(binary),"fibril_binary_sha256":sha(args.fibril_bin) if args.fibril_bin else None,
        "files":{str(f.relative_to(ROOT)):sha(f) for f in source_files},"images":image_info})
    shutil.copy2(ROOT/"Cargo.lock",out/"Cargo.lock")
    (out/"fibril-working-tree.patch").write_text(call("git","-C",str(REPO),"diff","HEAD","--binary"))
    for source in source_files:
        dest = out/"harness-source"/source.relative_to(ROOT)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, dest)
    for sibling in ["keratin", "ganglion"]:
        path = REPO.parent/sibling
        if (path/".git").exists():
            save(out/(sibling+"-source.json"), {"head":call("git","-C",str(path),"rev-parse","HEAD"),
                "status":call("git","-C",str(path),"status","--short")})
            (out/(sibling+"-working-tree.patch")).write_text(call("git","-C",str(path),"diff","HEAD","--binary"))
    results = []
    try:
        for repeat in range(args.repeats):
            # Rotate startup order to avoid giving one broker a fixed order advantage.
            order = kinds[repeat%len(kinds):]+kinds[:repeat%len(kinds)]
            for kind in order:
                results.append(run_case(args,kind,repeat,binary,images,out))
                save(out/"results.json",results)
    finally:
        subprocess.run(["python3",str(ROOT/"table.py"),str(out)],check=True)
    print(f"Evidence: {out}")

if __name__ == "__main__":
    main()
