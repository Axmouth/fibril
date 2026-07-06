"""Manual acknowledgement with a retry.

A manually-acked delivery that is nacked with requeue comes back for another
attempt. Once it is completed (acked), the broker settles it and no further copy
arrives.

Run a broker on 127.0.0.1:9876, then:

    uv run python examples/manual_ack_retry.py
"""

from __future__ import annotations

import asyncio
import os

from fibril import ClientOptions


async def main() -> None:
    addr = os.environ.get("FIBRIL_ADDR", "127.0.0.1:9876")
    opts = ClientOptions().with_auth("fibril", "fibril")
    async with await opts.connect(addr) as client:
        topic = "tasks"  # distinct from other examples so a shared broker cannot cross-feed
        sub = await client.subscribe(topic).prefetch(4).sub()  # manual ack
        await client.publisher(topic).publish_confirmed({"task": "resize-image"})

        seen = 0
        async for msg in sub:
            job = msg.deserialize()
            seen += 1
            if seen == 1:
                print(f"first delivery of {job!r}: requeuing for another attempt")
                await msg.retry()
            else:
                print(f"redelivery of {job!r}: completing")
                await msg.complete()
                break


if __name__ == "__main__":
    asyncio.run(main())
