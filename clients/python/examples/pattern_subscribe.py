"""Pattern subscribe: fan in across every work queue whose topic matches a glob.

New matching queues attach automatically. Run a broker on 127.0.0.1:9876, then:

    uv run python examples/pattern_subscribe.py
"""

from __future__ import annotations

import asyncio
import os

from fibril import ClientOptions


async def main() -> None:
    addr = os.environ.get("FIBRIL_ADDR", "127.0.0.1:9876")
    opts = ClientOptions().with_auth("fibril", "fibril")
    async with await opts.connect(addr) as client:
        # Opt in to the routing surface and fan in across every "events.*" queue.
        sub = await client.routing().subscribe_pattern("events.*").sub()
        print("listening for events.* (new matching queues attach automatically)")
        async for item in sub:
            print(f"{item.source.topic}: {item.message.text()}")
            await item.message.complete()


if __name__ == "__main__":
    asyncio.run(main())
