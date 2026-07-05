"""Combined publish + deliver throughput benchmark.

A saturating publisher and a consumer run at the same time over separate
connections to one broker, so this measures publish and deliver throughput
together (and whether deliver keeps up with publish, i.e. the backlog).

Run a broker on 127.0.0.1:9876, then:

    uv run python examples/bench_pubsub.py

MODE selects what this process runs:
  both (default) - publisher and consumer share one event loop (one core)
  pub            - publisher only (run a separate MODE=sub process to consume)
  sub            - consumer only (run a separate MODE=pub process to feed it)

Env: FIBRIL_ADDR, FIBRIL_USER, FIBRIL_PASS, SIZE (payload bytes), DURATION_S,
WARMUP_S, TOPIC, GROUP, PREFETCH, ACK (auto|manual), MODE (both|pub|sub).
"""

from __future__ import annotations

import asyncio
import os
import time

from fibril import ClientOptions


async def main() -> None:
    addr = os.environ.get("FIBRIL_ADDR", "127.0.0.1:9876")
    user = os.environ.get("FIBRIL_USER", "fibril")
    password = os.environ.get("FIBRIL_PASS", "fibril")
    size = int(os.environ.get("SIZE", "1024"))
    duration = float(os.environ.get("DURATION_S", "8"))
    warmup = float(os.environ.get("WARMUP_S", "2"))
    topic = os.environ.get("TOPIC", "benchpubsub")
    group = os.environ.get("GROUP", "workers")
    prefetch = int(os.environ.get("PREFETCH", "4096"))
    ack = os.environ.get("ACK", "auto")
    mode = os.environ.get("MODE", "both")
    do_pub = mode in ("both", "pub")
    do_sub = mode in ("both", "sub")

    opts = ClientOptions().with_auth(user, password)
    producer = await opts.connect(addr) if do_pub else None
    consumer = await opts.connect(addr) if do_sub else None

    published = 0
    delivered = 0
    running = True

    async def publish_loop() -> None:
        nonlocal published
        assert producer is not None
        pub = producer.publisher_grouped(topic, group)
        payload = bytes(size)
        while running:
            for _ in range(256):
                await pub.publish_bytes(payload)
                published += 1
            # A fire-and-forget publish never suspends, so yield each batch or the
            # publisher monopolizes the loop and the consumer (and timers) starve.
            await asyncio.sleep(0)

    async def consume_loop() -> None:
        nonlocal delivered
        assert consumer is not None
        builder = consumer.subscribe(topic).group(group).prefetch(prefetch)
        if ack == "manual":
            sub = await builder.sub()
            async for msg in sub:
                await msg.complete()
                delivered += 1
                if not running:
                    break
        else:
            sub = await builder.sub_auto_ack()
            async for _msg in sub:
                delivered += 1
                if not running:
                    break

    tasks = []
    if do_sub:
        tasks.append(asyncio.create_task(consume_loop()))
    if do_pub:
        tasks.append(asyncio.create_task(publish_loop()))

    # Warm up, then sample the counters at both ends of the measurement window.
    await asyncio.sleep(warmup)
    p0, d0, t0 = published, delivered, time.monotonic()
    await asyncio.sleep(duration)
    elapsed = time.monotonic() - t0
    p1, d1 = published, delivered
    running = False

    parts = [f"mode={mode}"]
    if do_pub:
        parts.append(f"publish={round((p1 - p0) / elapsed)}")
    if do_sub:
        parts.append(f"deliver={round((d1 - d0) / elapsed)}")
    parts.append(f"msgs/s  ack={ack} size={size}")
    if do_pub:
        parts.append(f"published={p1}")
    if do_sub:
        parts.append(f"delivered={d1}")
    if do_pub and do_sub:
        parts.append(f"backlog={p1 - d1}")
    parts.append(f"elapsed={elapsed:.1f}s")
    print("  ".join(parts))

    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    if producer is not None:
        await producer.shutdown()
    if consumer is not None:
        await consumer.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
