"""Consume messages from a Fibril broker with manual acknowledgement.

Run a broker on 127.0.0.1:9876, then:

    uv run python examples/subscribe.py
"""

from __future__ import annotations

import asyncio
import os

from fibril import ClientOptions


async def main() -> None:
    addr = os.environ.get("FIBRIL_ADDR", "127.0.0.1:9876")
    opts = ClientOptions().with_auth("fibril", "fibril")
    async with await opts.connect(addr) as client:
        sub = await client.subscribe("jobs").group("workers").prefetch(32).sub()
        async for msg in sub:
            try:
                job = msg.deserialize()
                print(f"processing {job!r} at offset {msg.offset}")
                await msg.complete()
            except Exception:
                # Requeue for another attempt after 5 seconds.
                await msg.retry_after(5)


if __name__ == "__main__":
    asyncio.run(main())
