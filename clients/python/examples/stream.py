"""Plexus (fan-out stream): every subscriber sees every record.

Unlike a work queue, where each message goes to one consumer, a Plexus stream
fans every record out to all subscribers. The subscription reads all partitions
and merges them; a durable name would track an independent cursor per partition.

Run a broker on 127.0.0.1:9876, then:

    uv run python examples/stream.py
"""

from __future__ import annotations

import asyncio
import os

from fibril import ClientOptions, StreamConfig


async def main() -> None:
    addr = os.environ.get("FIBRIL_ADDR", "127.0.0.1:9876")
    opts = ClientOptions().with_auth("fibril", "fibril")
    async with await opts.connect(addr) as client:
        topic = "events"
        await client.declare_plexus(StreamConfig(topic).partitions(1).durable())

        # Read only records published from now on (from_latest); auto-ack advances
        # past each record as it is delivered.
        sub = await client.stream(topic).from_latest().prefetch(64).sub_auto_ack()
        pub = client.publisher(topic)

        for seq in range(5):
            await pub.publish_confirmed({"seq": seq})

        received = 0
        async for msg in sub:
            print(f"stream record {msg.deserialize()!r} at offset {msg.offset}")
            received += 1
            if received == 5:
                break


if __name__ == "__main__":
    asyncio.run(main())
