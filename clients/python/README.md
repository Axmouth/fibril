# fibril (Python client)

Async-first Python client for the [Fibril](https://fibril.sh) message broker, with a
thin blocking facade for synchronous callers.

It ports the Rust reference client (`crates/client`) and the TypeScript client
(`clients/typescript`) to full feature parity. See `clients/ARCHITECTURE.md` for
the shared layering and invariants, `clients/FEATURE_MATRIX.md` for the parity
grid. The original plan with the Python-specific decisions is archived at
`archive/implementation-notes/PYTHON_CLIENT_PLAN.md`.

## Status

Feature-complete against the `FEATURE_MATRIX` (async core plus the blocking
facade). Pre-alpha and not yet published. The API tracks the source tree.

msgpack is optional: the client works with raw bytes, text, and JSON without it,
and only the msgpack payload path (the default for plain published values) needs
it. Install with `pip install 'fibril[msgpack]'` to enable it.

Not yet on PyPI. Install from a checkout with `pip install ./clients/python`
(or `uv pip install ./clients/python`). Once published, it will install with:

```sh
pip install fibril
```

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

## More

- [Client architecture](https://github.com/Axmouth/fibril/blob/main/clients/ARCHITECTURE.md), the layering and invariants shared by every client
- [Feature matrix](https://github.com/Axmouth/fibril/blob/main/clients/FEATURE_MATRIX.md), what each client supports
- [Documentation](https://fibril.sh)

## License

MIT. See [AI_POLICY.md](https://github.com/Axmouth/fibril/blob/main/AI_POLICY.md)
for the text and data mining reservation that comes with this package.
