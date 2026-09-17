# Repartition retirement and late cleanup

Native live grow/shrink testing exposed two independent races after a drained
partition was removed. Both reproduced with full-topology and targeted routing.

First, the assignment watcher could process `StopFollower` after the shrink
reclaimer had deleted the logs. Stroma's stop operation called `queue_handle`,
which materialized a new, empty owner and then rejected it with `WrongQueueRole`.
The equivalent owner-freeze path could also recreate deleted storage.

Second, closing the retired owner's subscription triggered a client reconnect
before the local coordination view had observed assignment removal. A native
diagnostic backtrace identified `install_subscription → materialize_owned_queue`
as the remaining source of empty retired partitions.

The correction spans Fibril and its sibling Keratin checkout:

- Keratin's role-cleanup methods only freeze existing handles. They leave cold
  or deleted partitions alone and hold the partition lifecycle lock across
  lookup and freezing, serializing with destruction and eviction.
- Fibril fences the retired key before closing consumers, drains its follower
  worker, cancels its owner runtime, then deletes storage. This local fence
  rejects stale reconnects and replication reads while assignment propagation
  catches up.
- Owner materialization rechecks ownership while holding the same queue
  eviction guard that retirement takes, covering requests admitted before the
  fence was installed.
- A fresh assignment releases the fence. A later grow also releases it for
  reintroduced indices, even when assignment-watch notifications coalesce the
  intervening removal and re-addition.

Permanent regressions cover cleanup after deletion, worker removal, stale
publisher/subscriber reconnects, fresh assignment reuse, and a grow whose
assignment watch coalesces. The native acceptance harness additionally checks
confirmed-message identities, absence of role errors, and absence of retired
partition handles on every node after live grow/shrink.

Run the relevant suites from the Fibril workspace:

```sh
cargo test --locked --release -p stroma-core --lib --test roles
cargo test --locked --release -p fibril-broker --lib --test broker_tests
cargo test --locked --release -p fibril-benches --bin failover_verify
```

## Confirmation errors during topology changes

The identity verifier now reports the error behind every failed confirmation.
The investigated live repartition failures were explicit redirects, not durable
confirmation timeouts. `publish_with_confirmation` retries the send step, but
its later `PublishConfirmation::confirmed` result is returned to the caller.
That pipelined API deliberately does not automatically retry confirmation
failures, which could duplicate or reorder a previously accepted publication.
This change reports those errors without changing the client's retry semantics;
callers using a confirmation window must handle its individual errors. Identity
verification continues to require delivery of every successfully confirmed ID,
rather than treating every send attempt as durable.
