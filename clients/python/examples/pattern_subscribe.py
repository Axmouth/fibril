"""Pattern subscribe: fan in across every work queue whose topic matches a glob.

New matching queues attach automatically. Run a broker on 127.0.0.1:9876, then:

    uv run python examples/pattern_subscribe.py
"""

from __future__ import annotations

import asyncio

from fibril import Client


async def main() -> None:
    async with await Client.connect("127.0.0.1:9876") as client:
        # Opt in to the routing surface and fan in across every "events.*" queue.
        sub = await client.routing().subscribe_pattern("events.*").sub()
        print("listening for events.* (new matching queues attach automatically)")
        async for item in sub:
            print(f"{item.source.topic}: {item.message.text()}")
            await item.message.complete()


if __name__ == "__main__":
    asyncio.run(main())
