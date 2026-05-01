Maybe find way to better linearly read from Keratin, faster

More pipelining in Keratin writer: Batch -> encode and stage buffer -> write file -> fsync -> notify awaiters (estimated possible 40%-60% gain in throughput from not waiting encoding and fsync for large payloads)

explore a cache trying to keep in memory x mb of next deliverable messages(we always know which messages are next with ready set)

try to nest main loop in tcp client and server and put more generous reconnect logic there, trying to keep the state and not redo, have a handshake that shows it was reconnected with same id to differentiate from connection dying on other side. also immediately mark ready again once connection(cause messages are stuck in prefetch inflight etc)

keep track off consumer id per message and extend lease for alive consumers instead of expiring

opportunistic batching (do not wait if the socket is writable now, but if you would block, accumulate)

more lazy init queues

run instructions:
cargo run --bin fibril

simple cli:
# publish
fibril-cli pub topic "hello"

# consume
fibril-cli sub topic

direct delivery/express lane (enqueue and inflight immediately whole also sending to delivery loop shortcut)
"Fibril utilizes speculative delivery: messages are streamed to available consumers immediately upon receipt, while persistence to Keratin happens in parallel. Producer acknowledgments are strictly deferred until disk synchronization is confirmed, ensuring zero-compromise durability with sub-millisecond egress."

deny topic etc names beyond simple fs compatible setups


Add display names to topics/groups for logging/ui

add events for changing queue settings (ttl, max inflight, etc)
add global event log for stroma setting changes

Finish dead letter queue implementation, untangle any fallible/storage bits to outside state methods and on event log application
Perhaps use completions to apply the events in memory only after persisting them

clean up group related tests

test big payloads

Investigate single log(storage message log only used for messages beyond a certain size?) for stroma topic state? Message Offsets are now enqueue offsets, requeue event to avoid payload duplication

TODO:
Broker should no longer:
loop compute_start_offset() by calling is_inflight_or_acked() repeatedly.
Instead:
start = stroma.next_deliverable(tp,g, current_cursor, upper)
Also: redelivery queue should remain bounded by inflight cap (it mostly is already), but don’t let it accumulate unbounded offsets from repeated failures—Stroma can own "expired offsets" listing.

consider interning topics etc to save some memory

ui login

reorganize for structs/models in a common crate, to avoid circles between metrics and storage too

cleanup leftover inflight without message (or better figure why it happens)

config

Handshake two way

ack correct matching(sub id?), in tcp layer

partitions internal generation and assignment

delivery not just with roundrobin bout account for prefetch capacity etc

max unconfirmed per publisher

slow ingress on memory/storage pressure

unwrap/expect cleanup

experiment with spreading delivered messages by making some consumers slower and see what happens

better handling of batching slowdown when confirms not drained

better error handling(return louder errors when failing to publish)

diagnostics queue/channel to allow sending up non fatal issues and tallying/observing them better?

multiple brokers on same storage tests (must fail)

shutdown stops publishing immediately, tries to drain inflight, ensure no late ack flushes race, ensure batchers drain once

Eval persisting inflight map, or delivery tags for inflight, so inflight state can be recovered on startup after crash

clusters (leader through shared networked storage initially, raft replication later?)

cli

metadata(content type, redelivered, etc)

RabidMQ easter egg(--version during April 1st? rabbitmq compatibility layer?)

routing layer? (in order to keep invariants stable, maybe we'd need a separate layer, broker delivers to itself, runs script, acks onces derived message completes, effectively)

dead letter queue? (expired, nacked too much, etc) (part of routing?)

queues with ttl to discard(not just resend)

define ops/infra story for easy convenient deployments and handling(not error prone by default, don't assume user does things right)

write an operator runbook

rabbitmq compatible endpoint?

replace epoch in delivery tag with gen and seq. Seq is simple monotonic counter, gen is increased per instance(process? task?) created

dls or embedded language to script transformations, routing, etc?
