---
title: Dead-letter replay
description: Design for previewing a dead-letter replay, replaying in paced canary steps that stop when messages fail again, and replaying into a scratch queue.
---

This is a proposed design. Nothing here is implemented yet. It expands the
roadmap item on bulk dead-letter replay.

## Problem

Replay today takes an explicit list of up to a capped number of dead-letter
offsets and sends them back to their source queues at once. That is safe for a
handful of messages. For thousands, replaying before the consumer bug is fixed
sends every one of them through the same failure again, which can overload the
consumer, flood the dead-letter queue a second time and hide the messages that
were fine.

An operator needs to see what a replay will do before it happens, start small,
and stop automatically if the problem is still there.

## Preview

A preview takes the same selection as a replay and changes nothing. For each
message, and summarized per target queue, it reports:

- the target queue and whether it still exists,
- whether the target has live consumers right now, and how many,
- the dead-letter reason and the attempt count,
- whether the message was already replayed, and when.

The summary leads with what would go wrong: targets that no longer exist,
targets with no consumers, and messages already replayed. The preview returns
a token that the replay can quote, so a replay applies exactly the previewed
selection. This matches how [MCP actions](/development/mcp-design/) work.

## Canary replay

A canary replay sends messages in steps and checks each step before the next.

1. Send the first step, 10 messages by default.
2. Wait until those messages are settled or the step timeout passes, 60 s by
   default.
3. If any came back to the dead-letter queue, or none were settled before the
   timeout, stop and report which messages failed again and why.
4. Otherwise send the next step, doubling its size up to a cap, at a paced
   rate.

The replay runs on the broker that owns the dead-letter queue and reports its
progress to the dashboard, the API and the activity feed. It can be paused or
cancelled at any time. Messages not yet sent stay in the dead-letter queue
untouched. A replay survives a dashboard reload. It does not survive an owner
failover, and it reports as interrupted afterwards, with the messages sent so
far listed, so the operator can start a new one for the rest.

## Replay to another queue

A replay can name a different target queue instead of each message's source,
for example a scratch queue read by a test consumer. The messages keep their
headers, and a `stroma.dlq.replayed_from` header records the original source,
so a test consumer can see where each one came from. The scratch queue can be
deleted afterwards without touching the dead-letter queue.

## Permissions

Under [access control](/development/access-control-design/), previews need
`view` on the dead-letter queue. Replays need `purge` on the dead-letter queue,
which covers moving dead-lettered messages, and `publish` on every target.

## Implementation order

1. Preview, with the token.
2. Replay to another queue, which reuses the current replay path with a
   different target.
3. Canary replay with steps, checks, pause and cancel, and its progress in the
   dashboard and activity feed.
4. Selection by filter, such as reason, source queue or time range, instead
   of only explicit offsets.

## Test plan

- A preview reports missing targets, targets without consumers and repeat
  replays, and changes nothing.
- A replay quoting a preview token applies exactly the previewed messages,
  even after more messages were dead-lettered.
- A canary replay against a consumer that rejects everything stops after the
  first step and leaves the rest in the dead-letter queue.
- A canary replay against a healthy consumer finishes with every message
  settled.
- Owner failover during a canary replay reports it as interrupted with the
  sent messages listed.
- Replay to a scratch queue keeps the original headers and adds the source
  header.
