---
title: Idle queue internals
description: Implementation notes for lazy loading and idle queue cleanup.
---

This is a development note. The user-facing behavior is described in [many idle queues](/latest/concepts/many-idle-queues/).

The feature has two layers:

- storage materialization: the Stroma queue handle, queue actor, open logs, snapshot task, and recovered queue state
- broker tracking: lightweight per-queue loop state, activity counters, delivery tags, and publisher/subscriber leases

Current cleanup mainly targets storage materialization. An idle queue can stop having a live Stroma handle while the broker still remembers small in-process bookkeeping for that topic and group.

The broker records activity through publisher and subscriber leases. A queue becomes a cleanup candidate after the last active lease drops and the configured idle window has elapsed. Cleanup is skipped if the broker sees active delivery tags, pending settlements, or storage-reported inflight messages.

Connection-lifetime publisher caching is the default. It avoids create/destroy publisher protocol messages that can desync if clients create and drop publisher objects frequently. `FIBRIL_PUBLISHER_CACHE_IDLE_TIMEOUT_MS` is a server-side backstop for long-lived connections that publish to many queues over time.

Future explicit publisher destroy commands should use server-issued or otherwise unique-enough publisher ids, and should still keep timeout-based cleanup as a backstop.
