---
title: Metadata policy
description: Development policy for reserved metadata namespaces and message headers.
---

This is a development note. User-facing client docs should simply say which header prefixes are reserved and what application code should avoid.

## Reserved Namespaces

Fibril reserves these message header prefixes:

- `fibril.*` for broker and protocol metadata
- `stroma.*` for storage and queue-state metadata

Clients may receive and display these headers, but user code should not set them. The broker rejects published messages that include reserved headers so system metadata cannot be spoofed by application payloads.

## What Belongs In Headers

Headers are for sparse metadata: values that are useful on some messages, but should not be paid for by every message.

Examples that fit:

- user trace ids
- application content hints
- uncommon system annotations that are not part of the hot path

Examples that should stay out of the user header map:

- always-present broker metadata such as published timestamps
- retry counts when the common case is zero
- fields that are better represented positionally in the wire/storage format

## Content Type

`content-type` is currently a normal user header because it is an application-level decoding hint.

A future wire/storage format may move content type out of the header map into a compact enum for common values, with an escape hatch for custom strings. That would keep the common JSON/msgpack/text cases cheaper without removing user-defined content types.

## DLQ Metadata

Dead-letter source details should not be added as ad-hoc user headers.

If replay or message inspection needs source information, add it under a reserved namespace after deciding the stable shape. Until then, DLQ copies should preserve user headers but avoid inventing public metadata fields accidentally.
