"""Confirmed and delayed publishing.

A confirmed publish returns the broker-assigned offset, which increases across
publishes to a partition. A delayed publish is withheld until its deadline, then
delivered like any other message.

Run a broker on 127.0.0.1:9876, then:

    uv run python examples/confirmed_delayed.py
"""

from __future__ import annotations

import asyncio
import os

from fibril import ClientOptions


async def main() -> None:
    addr = os.environ.get("FIBRIL_ADDR", "127.0.0.1:9876")
    opts = ClientOptions().with_auth("fibril", "fibril")
    async with await opts.connect(addr) as client:
        topic = "orders"
        sub = await client.subscribe(topic).prefetch(8).sub_auto_ack()
        pub = client.publisher(topic)

        first = await pub.publish_confirmed({"note": "now"})
        second = await pub.publish_confirmed({"note": "also now"})
        print(f"confirmed offsets {first} then {second} (increasing: {second > first})")

        # Withheld until the delay elapses (1 second), then delivered normally.
        await pub.publish_delayed_confirmed({"note": "later"}, 1.0)
        print("published one message delayed by 1s")

        received = 0
        async for msg in sub:
            print(f"received {msg.deserialize()!r} at offset {msg.offset}")
            received += 1
            if received == 3:
                break


if __name__ == "__main__":
    asyncio.run(main())
