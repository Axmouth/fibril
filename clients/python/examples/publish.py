"""Publish a few messages to a Fibril broker.

Run a broker on 127.0.0.1:9876, then:

    uv run python examples/publish.py
"""

from __future__ import annotations

import asyncio
import os

from fibril import ClientOptions, NewMessage


async def main() -> None:
    addr = os.environ.get("FIBRIL_ADDR", "127.0.0.1:9876")
    opts = ClientOptions().with_auth("fibril", "fibril")
    async with await opts.connect(addr) as client:
        publisher = client.publisher("jobs")

        # Fire-and-forget (msgpack-encoded by default).
        await publisher.publish({"id": 1, "task": "resize-image"})

        # Confirmed publish returns the broker-assigned offset.
        offset = await publisher.publish_confirmed({"id": 2, "task": "send-email"})
        print(f"confirmed at offset {offset}")

        # Explicit content type, custom header, and a partition key for ordering.
        await publisher.publish(
            NewMessage.json({"id": 3}).header("x-trace-id", "abc123").partition_key("user-7")
        )

        # A message that the broker drops if not consumed within 30 seconds.
        await publisher.expiring(30).publish({"id": 4, "task": "ephemeral"})


if __name__ == "__main__":
    asyncio.run(main())
