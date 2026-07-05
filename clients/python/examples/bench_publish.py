"""Single-client saturating publish benchmark (unconfirmed).

Run a broker on 127.0.0.1:9876, then:

    uv run python examples/bench_publish.py

Env: FIBRIL_ADDR, SIZE (payload bytes), DURATION_S, WARMUP_S.
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

    async with await ClientOptions().with_auth(user, password).connect(addr) as client:
        pub = client.publisher("benchtopic")
        payload = bytes(size)

        async def loop_until(deadline: float) -> int:
            count = 0
            # Inner burst so time.monotonic() is not read every message.
            while time.monotonic() < deadline:
                for _ in range(512):
                    await pub.publish_bytes(payload)
                    count += 1
            return count

        await loop_until(time.monotonic() + warmup)
        start = time.monotonic()
        count = await loop_until(start + duration)
        elapsed = time.monotonic() - start
        print(f"rate={round(count / elapsed)} msgs/s count={count} elapsed={elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())
