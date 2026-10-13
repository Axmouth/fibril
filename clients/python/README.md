# fibril (Python client)

Async-first Python client for the [Fibril](../../README.md) message broker, with a
thin blocking facade for synchronous callers.

It ports the Rust reference client (`crates/client`) and the TypeScript client
(`clients/typescript`) to full feature parity. See `clients/ARCHITECTURE.md` for
the shared layering and invariants, `clients/FEATURE_MATRIX.md` for the parity
grid, and `clients/PYTHON_CLIENT_PLAN.md` for the Python-specific decisions.

## Status

Feature-complete against the `FEATURE_MATRIX` (async core plus the blocking
facade). Pre-alpha and not yet published. The API tracks the source tree.

msgpack is optional: the client works with raw bytes, text, and JSON without it,
and only the msgpack payload path (the default for plain published values) needs
it. Install with `pip install 'fibril[msgpack]'` to enable it.

## Queue durability

`QueueConfig` picks how a queue confirms and delivers. `durable()` waits until
each required copy has synced a message to disk. `buffered()` waits only until
each required copy has written it to the OS, before fsync, so power loss or a
storage failure on every required node at once can lose recent confirmed
messages. Leaving both out keeps the broker default.

```python
ok = await client.declare_queue(QueueConfig("jobs").buffered())
print(ok.durability, ok.warnings)
```

Only the first declare of a queue sets its mode. A later declare naming another
mode fails with error code 409. Brokers that predate buffered queues refuse a
declare that names a mode.

## Development

The dev environment is managed with [uv](https://docs.astral.sh/uv/):

```sh
cd clients/python
uv run pytest
```

Python 3.11 is the minimum. The code is kept 3.10-portable (3.11-only constructs
are isolated behind small helpers) so a future 3.10 backport stays localized.
