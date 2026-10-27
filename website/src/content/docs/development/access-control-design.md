---
title: Access control
description: Design for roles that allow or deny actions on topics, groups and administration, kept in one cluster document with a simple file format.
---

This is a proposed design. Nothing here is implemented yet. The policy is a
cluster document whose source follows
[configuration sources](/development/config-sources/). Identity for forwarded
sessions comes from the [private nodes](/development/private-nodes-design/)
design.

## Problem

Any authenticated user can do anything today: publish and consume on every
queue, delete and purge them, change settings and manage users. Teams sharing
a cluster need to limit that, for example an application that may only publish
to its own topics, a support account that can look but not touch, or an
operator who sees everything except payroll queues.

The policy has to stay easy to read and change. The aim is a format a person
can review in a pull request or edit in the dashboard and predict the outcome,
with no regular expressions and no ordering rules to remember.

## Precedents

| System | Model | Lesson |
| --- | --- | --- |
| NATS | Per-user publish and subscribe allow and deny lists with subject wildcards | Deny lists beside allow lists cover most needs. Wildcards on dotted names read well |
| AWS IAM | Statements with actions and resources, explicit deny wins | One evaluation rule makes mixed policies predictable |
| Kafka | ACLs per resource type with literal or prefixed names, deny wins | Prefix matching covers team namespaces |
| RabbitMQ | Configure, write and read regular expressions per virtual host | Regular expressions are hard to review and a common source of mistakes |

## Model

- **Users** authenticate as today, by password or by a certificate identity
  that names a user.
- **Roles** hold rules. A user has any number of roles.
- **Rules** allow or deny a set of actions on a set of resources.

A request is allowed when some rule in the user's roles allows it and no rule
in any of the user's roles denies it. Everything else is denied. Deny always
wins, so a role that denies payroll access removes it even from a user who
also holds an allow-everything role.

### Actions

Data actions apply to topics and groups:

| Action | Covers |
| --- | --- |
| `publish` | Publish and delayed publish |
| `consume` | Subscribe, and settle the deliveries of that subscription |
| `declare` | Declare queues and streams, change their settings |
| `delete` | Delete queues and streams |
| `purge` | Purge, replay and move dead-lettered messages |
| `view` | See the topic in listings, metrics and message inspection |

Administration actions have no resource:

| Action | Covers |
| --- | --- |
| `settings.view`, `settings.edit` | Runtime settings and the global dead-letter target |
| `users.edit` | Create, change and remove users |
| `access.edit` | Change roles and their assignment |
| `cluster.manage` | Drain, membership, rebalancing and recovery actions |

`*` alone means every action, data and administration.

### Resources

Topic and group names use lowercase letters, digits, `.`, `_` and `-`, with
dots as separators by convention. Patterns follow the same shape:

- `billing.invoices` names one topic and every group under it.
- `billing.invoices/workers` names one group of that topic. The default group
  is `default`.
- `*` matches one dot-separated segment: `billing.*` matches
  `billing.invoices` but not `billing.eu.invoices`.
- `**` matches any number of segments: `billing.**` matches both, and `**`
  alone matches everything.
- Both work in the group part too: `billing.**/workers`.

`/` never appears in a name, so a pattern always reads unambiguously as topic
and group. The order follows the client API and the wire format, where a group
is a namespace under its topic.

| Pattern | Matches |
| --- | --- |
| `billing.invoices` | That topic and every group under it |
| `billing.*` | `billing.invoices` and its groups, not `billing.eu.invoices` |
| `billing.**` | Every topic under `billing` at any depth, with all their groups |
| `billing.invoices/workers` | Only the `workers` group of that topic |
| `billing.invoices/default` | Only the default group of that topic |
| `billing.invoices/*` | Every group of that topic, the same as leaving the group off |
| `billing.**/workers` | The `workers` group of every topic under `billing` |
| `**/workers` | The `workers` group of every topic |
| `**` | Everything |

## File format

Rules are written as one line each, actions then a colon then resources.

```toml
[access]
source = "seed"

[access.roles.ops]
allow = ["*"]

[access.roles.billing-app]
allow = ["publish, consume: billing.**"]
deny = ["delete, purge: **"]

[access.roles.support]
allow = ["view: **", "settings.view"]
deny = ["view: payroll.**"]

[access.roles.payments-team]
allow = [
  "publish, consume, declare, view: payments.**",
  "consume: billing.invoices/payments",
]

[access.users]
alice = ["ops"]
bob = ["support"]
billing-svc = ["billing-app"]
payments-svc = ["payments-team"]
```

- A rule with several resources separates them with commas:
  `"view: billing.**, payments.**"`.
- Each line also has a table form for tools that generate policy:
  `{ actions = ["publish"], resources = ["billing.**"] }`.
- User credentials stay in the user document. `[access.users]` only assigns
  roles, so a policy file holds no secrets and can be shared freely.
- The dashboard edits the same structure and the API returns it as JSON with
  the same fields, so a policy copied from one to the other needs no changes.

Unknown actions, malformed patterns and roles that are assigned but not
defined are errors that name the line, both at startup and in the dashboard.

## Default behavior

- With no policy document, every authenticated user may do everything, as
  today. Upgrading changes nothing until a policy is written.
- Once a policy exists, a user with no roles can authenticate and do nothing.
- The node principal used between brokers is outside the policy.
- The loopback default user, which exists only while no users are defined, has
  every action.
- The admin API refuses a policy change that would leave no user with
  `access.edit`, so the cluster cannot be locked out of its own policy. File
  mode reports the same condition as an error at startup.

## Enforcement

One check function serves the protocol, the admin API, the CLI and the planned
MCP server.

- **Protocol.** The broker checks when a connection first publishes to a topic
  and group, subscribes, declares, deletes or purges. Results are cached per
  connection and keyed by the policy version, so the cost is one map lookup per
  publish batch. Settling a delivery needs no separate check, since only the
  subscription that received it can settle it.
- **Policy changes.** A new policy version clears every connection's cache.
  Subscriptions the user may no longer consume close with a new reason code,
  `Forbidden`, and later requests fail with a new error code,
  `ERR_FORBIDDEN` (403).
- **Forwarded sessions.** The public broker checks before forwarding, to reject
  early, and the owner checks again against the acting user, as the
  authoritative decision. A 403 from the owner is relayed unchanged.
- **Admin API and dashboard.** Each route maps to an action. Listings include
  only topics the user may `view`, and the dashboard hides controls the user
  cannot use.

## Making it easy to manage

- **Explain a decision.** `fibrilctl access check bob view payroll.salaries`
  answers "denied by role support, rule deny 1 (`view: payroll.**`)". The
  dashboard shows the same next to every user, with a test box for any action
  and topic.
- **Effective access.** Each user's page lists what they can do, grouped by
  topic pattern, so reviewing a role does not require evaluating rules by hand.
- **Audit.** Policy and user changes are logged with who made them. Denied
  requests are logged once per connection, user, action and resource, so a
  misbehaving client cannot flood the log.

## Storage

The policy is a cluster document beside the user document, replicated through
coordination and copied into each node's global store. It has its own version
for compare-and-swap edits. Its source is `cluster`, `seed` or `file`, as for
the other documents.

## Implementation order

1. The policy model, parser, pattern matching and the check function, with
   the explanation output, as a library with unit tests.
2. The cluster document, its configuration source and the admin API to read
   and write it.
3. Protocol enforcement with per-connection caching, `ERR_FORBIDDEN` and the
   `Forbidden` close reason, in the broker and all clients.
4. Admin API and dashboard enforcement, filtered listings, and the policy
   editor with the check box.
5. Forwarded session checks on both ends.
6. `fibrilctl access check` and the effective access view.

## Test plan

- Pattern matching for `*`, `**`, groups and the default group, including names
  at the 128 byte limit.
- Evaluation: allow only, deny only, mixed, multiple roles where one denies,
  and no roles at all.
- Every protocol operation and admin route refuses a user without the action
  and accepts one with it.
- A policy change closes subscriptions that lost `consume` and leaves others
  running.
- Forwarded publishes are refused at the public broker and, with a deliberately
  stale entry policy, at the owner.
- The lockout guard refuses a policy without `access.edit`.
- A file policy round-trips through the API and the dashboard unchanged.

## Open questions

- Whether roles should include other roles. Multiple roles per user cover the
  common cases without it.
- Whether services should get API tokens as an alternative to passwords and
  certificates.
- Whether per-user publish rate quotas belong in roles or with
  [resource limits](/development/resource-limits-design/).
- Single sign-on for the dashboard, which would map external groups to roles.
