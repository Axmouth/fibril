---
title: MCP server
description: Design for a Model Context Protocol server that lets an agent diagnose a Fibril cluster, read its documentation and, later, take scoped actions.
---

This is a proposed design. Nothing here is implemented yet. It expands the
roadmap entry
[MCP server and agent integration](/roadmap/#mcp-server-and-agent-integration).
Access goes through [access control](/development/access-control-design/), and
trends come from the persistent history in
[monitoring presets and history](/development/monitoring-design/).

## Problem

When a cluster misbehaves, the answer is usually spread across the dashboard's
attention panel, queue and replication pages, recovery evidence, settings,
recent activity and the docs. An operator who asks an agent "why is
`billing.invoices` slow?" today has to copy all of that by hand.

An MCP server gives the agent the same view the dashboard has, as structured
tools, plus the documentation for the running version. The first goal is
diagnosis, for both performance and faults. Actions come later and stay
narrow.

## Principles

- **Same data as the dashboard.** Tools call the operations behind the admin
  API, never a separate path, so the agent and the dashboard cannot disagree.
- **Bounded answers.** Every result has a size cap, an observation time and
  the node it came from. Listings are paged and sorted by what matters most.
- **Explicit unknowns.** A field that could not be read says so and why, for
  example "node b did not answer within 2 s", rather than being left out.
- **Links back.** Each result carries the dashboard URL that shows the same
  thing, so a person can check the agent's reading.
- **Read-only first.** The first version changes nothing.

## Tools

### Overview

- `cluster_overview`: nodes, roles, health, versions, coordination leader,
  configuration source states and resource levels.
- `attention`: the attention feed, with each item's rule, severity, subject,
  since when it has been raised, and a link.
- `recent_activity`: the audit feed for a time window: declares, deletes,
  drains, settings changes, raised and resolved attention items, membership
  changes.

### Queues and streams

- `list_queues`: paged, sorted by backlog, growth or redeliveries, filtered
  by pattern using the access control syntax.
- `queue_detail`: partitions, owners and replicas, backlog, inflight,
  consumers and their prefetch, publish and delivery rates, redeliveries,
  dead-letter counts and recent backlog trend.
- `stream_detail`: the same for streams, with consumer cursors and lag.
- `inspect_messages`: message metadata for a queue, with payloads only when
  asked and allowed (see Safety).

### Replication and recovery

- `replication_status`: per partition, the in-sync set, follower progress and
  lag, and partitions below their in-sync minimum.
- `recovery_status`: quarantined partitions, recovery evidence and pending
  withdrawals.
- `topology`: partition ownership and routing, with private nodes.

### Performance

- `node_performance`: throughput, publish confirm and delivery latency
  percentiles, fsync timings, CPU, memory, threads, disk use and free space,
  over a chosen window from persistent history.
- `compare_to_reference`: the node's recent numbers beside the published
  reference results for a similar workload, so the agent can say whether a
  number is normal for this hardware class or an outlier. The reference data
  ships with the docs.

### Configuration

- `settings`: runtime settings with their source, version and what differs
  from defaults.
- `config_sources`: per-document source, adopted fingerprint and per-node
  agreement.
- `access_check`: the access control explanation for a user, action and
  resource.

### Documentation

- `docs_search`: keyword search over the documentation of the running
  version, returning titles, headings and short excerpts.
- `docs_read`: one page or section as Markdown.

Pages are also exposed as MCP resources, so clients that browse resources can
attach them directly.

## Prompts

A few prompts give agents a starting procedure:

- `diagnose_queue`: backlog trend, consumers, redeliveries, replication, then
  relevant attention items and docs.
- `diagnose_node`: resource levels, latency against reference, recent
  activity, then storage and replication.
- `explain_attention`: each raised item with the docs section that covers it.

## Documentation source

The docs come from the website content at build time, as one generated
bundle embedded in the broker binary. The website stays the only maintained
copy, and the bundle always matches the running version. The bundle holds
Markdown with frontmatter and a small prebuilt keyword index. Animated
stories and other interactive pages are reduced to their text transcripts.

## Transport and access

- **HTTP.** The admin listener serves MCP at `/mcp` using Streamable HTTP,
  with the same TLS and the same authentication as the admin API.
- **Local.** `fibrilctl mcp` runs a stdio server that forwards to a broker's
  `/mcp`, for agents that only launch local processes.
- **Permissions.** Each tool maps to access control actions. A built-in
  `diagnostics` role allows `view` on everything and `settings.view`, which
  covers every first-version tool except message payloads. Connecting an
  agent with that role cannot change the cluster.

Before access control exists, MCP is available only to users who may use the
admin API, which today means every user.

## Safety

- Message payloads are off by default. `inspect_messages` returns metadata
  unless the call asks for payloads and the server setting
  `mcp.message_payloads` allows them. Payloads are truncated to a configured
  size.
- Header values under the reserved `fibril.*` and `stroma.*` namespaces are
  shown, and other header values follow the same switch as payloads.
- Every MCP call is recorded in the activity feed with the user and the tool,
  marked as coming through MCP.
- Results never include password hashes, the cluster secret or private keys.

## Later: actions

Actions come after access control, each as its own tool mapped to one action
and recorded in the activity feed:

- drain a node, rebalance or repartition a queue,
- replay or move dead-lettered messages,
- change a runtime setting with an expected version.

Each action first returns a preview of its effect, and applies only when
called again with the preview's token, so an agent cannot change the cluster
in one unconsidered step. Clients that support MCP elicitation show the
preview to the person for confirmation.

## Implementation order

1. Shared typed operations behind the admin API for the overview, attention,
   queue, replication and recovery reads, with unknowns and observation times.
2. The MCP server on `/mcp` with those tools, and `fibrilctl mcp`.
3. The documentation bundle from the website build, with search and read.
4. Performance tools on persistent history, and reference comparison.
5. Configuration and access tools as those features land.
6. Prompts, then actions after access control.

## Test plan

- Every tool's output matches the corresponding admin API response for the
  same state.
- Results stay within their caps on a cluster with thousands of queues.
- A node that does not answer produces an explicit unknown, not a missing
  field.
- The `diagnostics` role can call every read tool and nothing else, and
  payloads stay hidden unless both the call and the setting allow them.
- The documentation bundle builds from the website source and its version
  matches the binary.
- An agent-driven check against the test cluster: given a queue with no
  consumers and a slow follower, the prompts lead to both causes.

## Open questions

- Whether the stdio proxy should also work against several brokers at once
  for clusters without a shared admin address.
- How far reference comparison should go beyond the published benchmark
  shapes.
