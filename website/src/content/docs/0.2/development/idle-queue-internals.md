---
title: Idle queue internals
description: Implementation notes for lazy loading and idle queue cleanup.
---

This is a development note. The user-facing behavior is described in [many idle queues](/latest/concepts/many-idle-queues/).

The feature has two layers:

- storage materialization: the Stroma queue handle, queue actor, open logs, snapshot task, and recovered queue state
- broker tracking: lightweight per-queue loop state, activity counters, delivery tags, and publisher/subscriber leases

Current cleanup mainly targets storage materialization. An idle queue can stop having a live Stroma handle while the broker still remembers small in-process bookkeeping for that topic and group.

The broker records activity through publisher and subscriber leases. Creating either kind of lease is serialized with cleanup and materializes the Stroma queue before the broker returns a usable handle. A queue becomes a cleanup candidate after the last active lease drops and the configured idle window has elapsed. Cleanup is skipped if the broker sees active delivery tags, pending settlements, or storage-reported inflight messages.

Connection-lifetime publisher caching is the default. It avoids create/destroy publisher protocol messages that can desync if clients create and drop publisher objects frequently. `FIBRIL_PUBLISHER_CACHE_IDLE_TIMEOUT_MS` is a server-side backstop for long-lived connections that publish to many queues over time.

Publisher and subscriber lease creation is serialized with queue cleanup. This prevents the broker from checking "no active publishers or subscribers", starting storage unmaterialization, and then accepting a new lease for the same queue in the middle of that cleanup.

Automated cleanup suppresses repeated storage eviction calls after the previous storage outcome already unloaded the queue. Direct `try_evict_inactive_queue` calls still report the storage state truthfully, including `NotMaterialized`, because they are diagnostic/manual operations.

Broker `PublisherHandle` is intentionally not cloneable. A handle owns one publisher sink task and one active-publisher lease. If clone only copied the sender, multiple logical handles would still count as one active publisher. If clone added another lease, that lease would not have a clear sink lifetime. Creating another independently tracked publisher should call `get_publisher` again.

Future explicit publisher destroy commands should use server-issued or otherwise unique-enough publisher ids, and should still keep timeout-based cleanup as a backstop.
