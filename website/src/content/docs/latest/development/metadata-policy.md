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
- application content hints that are not already represented by a protocol field
- uncommon system annotations that are not part of the hot path

Examples that should stay out of the user header map:

- always-present broker metadata such as published timestamps
- retry counts when the common case is zero
- content type, which has a compact protocol/storage representation
- fields that are better represented positionally in the wire/storage format

## Content Type

`content-type` is application-level decoding metadata, but it is not stored in the user header map.

The wire/storage representation uses explicit common variants for msgpack, JSON, and UTF-8 text, plus a custom string variant for other content types. Clients may expose a convenience method that accepts `content-type` as a header name, but that should normalize into the explicit content-type field before publish.

## DLQ Metadata

Dead-letter source details should not be added as ad-hoc user headers.

If replay or message inspection needs source information, add it under a reserved namespace after deciding the stable shape. Until then, DLQ copies should preserve user headers but avoid inventing public metadata fields accidentally.
