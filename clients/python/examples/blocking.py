"""Synchronous publish and consume using the blocking facade.

For callers that are not built on asyncio. Run a broker on 127.0.0.1:9876, then:

    uv run python examples/blocking.py
"""

from __future__ import annotations

from fibril.blocking import BlockingClient


def main() -> None:
    with BlockingClient.connect("127.0.0.1:9876") as client:
        client.publisher("jobs").publish_confirmed({"id": 1, "task": "report"})

        sub = client.subscribe("jobs").group("workers").sub()
        for msg in sub:
            print(f"got {msg.deserialize()!r} at offset {msg.offset}")
            msg.complete()
            break  # one message for the demo
        sub.close()


if __name__ == "__main__":
    main()
