"""Plexus (fan-out stream) throughput benchmark.

A saturating publisher feeds a stream while CONSUMERS independent stream
subscribers each receive every record (fan-out), so deliver throughput is
aggregated across all of them. Mirrors bench_pubsub, over a stream instead of a
work queue.

Run a broker on 127.0.0.1:9876, then:

    uv run python examples/bench_plexus.py

MODE selects what this process runs:
  both (default) - publisher and consumers share one event loop (one core)
  pub            - publisher only
  sub            - consumers only

Env: FIBRIL_ADDR, FIBRIL_USER, FIBRIL_PASS, SIZE (payload bytes), DURATION_S,
WARMUP_S, TOPIC, PARTITIONS, PREFETCH, MODE (both|pub|sub), CONSUMERS,
DURABILITY (ephemeral|speculative|durable).
"""

from __future__ import annotations

import asyncio
import os
import time

from fibril import ClientOptions, StreamConfig


async def main() -> None:
    addr = os.environ.get("FIBRIL_ADDR", "127.0.0.1:9876")
    user = os.environ.get("FIBRIL_USER", "fibril")
    password = os.environ.get("FIBRIL_PASS", "fibril")
    size = int(os.environ.get("SIZE", "1024"))
    duration = float(os.environ.get("DURATION_S", "8"))
    warmup = float(os.environ.get("WARMUP_S", "2"))
    topic = os.environ.get("TOPIC", "benchplexus")
    partitions = int(os.environ.get("PARTITIONS", "1"))
    prefetch = int(os.environ.get("PREFETCH", "4096"))
    mode = os.environ.get("MODE", "both")
    consumers = int(os.environ.get("CONSUMERS", "1"))
    durability = os.environ.get("DURABILITY", "durable")
    do_pub = mode in ("both", "pub")
    do_sub = mode in ("both", "sub")

    opts = ClientOptions().with_auth(user, password)

    # Declare the stream once up front on a throwaway connection.
    admin = await opts.connect(addr)
    cfg = StreamConfig(topic).partitions(partitions)
    cfg = {"ephemeral": cfg.ephemeral, "speculative": cfg.speculative}.get(
        durability, cfg.durable
    )()
    await admin.declare_plexus(cfg)
    await admin.shutdown()

    published = 0
    delivered = 0
    running = True

    producer = await opts.connect(addr) if do_pub else None
    consumer_clients = [await opts.connect(addr) for _ in range(consumers)] if do_sub else []

    async def publish_loop() -> None:
        nonlocal published
        assert producer is not None
        pub = producer.publisher(topic)
        payload = bytes(size)
        while running:
            for _ in range(256):
                await pub.publish_bytes(payload)
                published += 1
            await asyncio.sleep(0)

    async def consume_loop(client) -> None:
        nonlocal delivered
        sub = await client.stream(topic).from_latest().prefetch(prefetch).sub_auto_ack()
        async for _msg in sub:
            delivered += 1
            if not running:
                break

    tasks = []
    for client in consumer_clients:
        tasks.append(asyncio.create_task(consume_loop(client)))
    if do_pub:
        tasks.append(asyncio.create_task(publish_loop()))

    await asyncio.sleep(warmup)
    p0, d0, t0 = published, delivered, time.monotonic()
    await asyncio.sleep(duration)
    elapsed = time.monotonic() - t0
    p1, d1 = published, delivered
    running = False

    parts = [f"mode={mode}", f"durability={durability}"]
    if do_pub:
        parts.append(f"publish={round((p1 - p0) / elapsed)}")
    if do_sub:
        per = round((d1 - d0) / elapsed / max(consumers, 1))
        parts.append(f"deliver={round((d1 - d0) / elapsed)} (fanout={consumers}, per_consumer={per})")
    parts.append(f"msgs/s  size={size}  elapsed={elapsed:.1f}s")
    print("  ".join(parts))

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if producer is not None:
        await producer.shutdown()
    for client in consumer_clients:
        await client.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
