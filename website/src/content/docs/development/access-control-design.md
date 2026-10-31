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
| `consume` | Subscribe to an existing queue, group or stream, and settle the deliveries of that subscription |
| `declare` | Create queues, groups and streams, and change their settings |
| `delete` | Delete queues and streams |
| `purge` | Empty a queue. It destroys data, so it is granted rarely |
| `replay` | Replay or move dead-lettered messages. The target also needs `publish` |
| `view` | See the topic's name, settings and metrics in listings and the dashboard |
| `read` | Read stored payloads outside a subscription: message inspection, dead-letter previews and the MCP payload tools |

Seeing that a topic exists and reading what is in it are different trust
levels, so `view` never shows payloads. A subscriber already receives payloads
through `consume` and needs no `read` for that. Replay re-publishes data while
purge destroys it, and an operator who may retry failed work usually should
not be able to empty a queue, so they are separate actions. `declare` covers
both creating and reconfiguring, since fewer actions keep policies readable.
A separate action for reconfiguring can be added if someone needs it.

Administration actions have no resource:

| Action | Covers |
| --- | --- |
| `settings.view`, `settings.edit` | Runtime settings and the global dead-letter target |
| `users.edit` | Create, change and remove users |
| `access.edit` | Change roles and their assignment. Without a resource it is root, see delegated administration below |
| `cluster.manage` | Drain, membership, rebalancing and recovery actions |
| `attention.note` | Add notes to attention items and snooze them |

`*` alone means every action, data and administration.

### Resources

Topic and group names use lowercase letters, digits, `.`, `_` and `-`, with
dots as separators by convention. Patterns follow the same shape:

- `billing.invoices` names one topic and every group under it.
- `billing.invoices/workers` names one group of that topic. The default group
  is `default`.
- `*` as a whole segment matches exactly one segment, anywhere in the
  pattern: `billing.*` matches `billing.invoices` but not
  `billing.eu.invoices`, and `billing.*.invoices` matches `billing.eu.invoices`.
- `**` as a whole segment matches any number of segments, including none, and
  may appear once per pattern, anywhere: `billing.**` matches every topic
  under `billing`, `**.dlq` matches every topic ending in `dlq`, and
  `billing.**.audit` matches `billing.audit` and `billing.eu.west.audit`.
  `**` alone matches everything.
- Inside a segment, one `*` matches any run of characters within that
  segment, never crossing a dot: `team-*`, `*-prod` and `orders-*-v2`.
- Both work in the group part too: `billing.**/workers`.

These limits are deliberate. One `*` per segment and one `**` per pattern keep
matching linear and make the check that one pattern lies inside another exact,
which delegated administration needs. They also keep every pattern readable at
a glance.

### Delegated administration

`access.edit` may carry resources, as in `"access.edit: payments.**"`. A
delegated administrator can then create and change roles whose every rule
names only resources inside their own patterns, and assign those roles to
users. They cannot grant administration actions, including `access.edit`
itself, and cannot change a role that names anything outside their patterns.
Bare `access.edit`, with no resource, is root: whoever holds it can grant
themselves anything, and the dashboard labels it that way.

Checking that a rule stays inside a scope compares patterns segment by
segment: `payments.eu.*` is inside `payments.**`, while `payments.*` is not
inside `payments.eu.**`. The check function exposes this so the dashboard can
explain a refusal.

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
| `billing.inv*` | `billing.invoices` and `billing.inventory`, not `billing.inv.eu` |
| `orders/team-*` | Every group of `orders` whose name starts with `team-` |
| `*-prod.**` | Everything under a first segment ending in `-prod`, such as `billing-prod.invoices` |
| `orders.*-v2` | `orders.eu-v2` and `orders.us-v2`, not `orders.eu.v2` |
| `billing.*.invoices` | `billing.eu.invoices`, not `billing.invoices` or `billing.eu.west.invoices` |
| `**.dlq` | `dlq`, `orders.dlq` and `billing.eu.dlq` |
| `billing.**.audit` | `billing.audit` and `billing.eu.west.audit` |
| `**` | Everything |

Refused at load time, with the line and the reason:

| Pattern | Why |
| --- | --- |
| `team-*-*` | More than one `*` inside a segment |
| `billing.**.eu.**` | More than one `**` in a pattern |
| `billing**` | `**` must be a whole segment |
| `billing..invoices` | Empty segment |

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
  and group, subscribes, declares, deletes or purges.
- **Creating by use.** A publish or subscribe that would create a queue, group
  or stream, as standalone brokers do for undeclared queues, needs `declare` on
  it as well. Using one that already exists needs only `publish` or `consume`. Results are cached per
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
- **Audit.** Policy, user and token changes are logged with who made them, and
  so are allowed destructive and administrative actions: delete, purge,
  replay, settings changes and cluster actions, each with the actor, the
  resource and the outcome. This needs no configuration. Denied requests are
  logged once per connection, user, action and resource, so a misbehaving
  client cannot flood the log.
- **Check a policy file.** `fibrilctl access check-policy policy.toml`
  validates a file with the same parser and line-numbered errors as the
  broker, and `--fmt` rewrites it in a normalized order, so typos fail before
  they reach a cluster and reviews stay clean.

## Service tokens

Services authenticate with scoped, expiring API tokens instead of shared
passwords or certificates.

- A token belongs to a user, usually a service user with no password, and
  carries a subset of that user's roles. It can never do more than its user.
- Tokens are created through the admin API, `fibrilctl` or the dashboard, are
  shown once, and are stored only as a hash in the user document.
- Each token has an expiry, an optional label and a last-used time, and can be
  revoked at once. Rotation is creating a new token, deploying it and revoking
  the old one, all admin API operations.
- On the wire a token authenticates like a password, so every client gets it
  without a protocol change.

## Storage

The policy is a cluster document beside the user document, replicated through
coordination and copied into each node's global store. It has its own version
for compare-and-swap edits. Its source is `cluster`, `seed` or `file`, as for
the other documents.

## Implementation order

1. The policy model, parser, pattern matching, scope containment and the
   check function, with the explanation output, as a library with unit tests.
2. The cluster document, its configuration source and the admin API to read
   and write it.
3. Protocol enforcement with per-connection caching, `ERR_FORBIDDEN` and the
   `Forbidden` close reason, in the broker and all clients.
4. Admin API and dashboard enforcement, filtered listings, and the policy
   editor with the check box.
5. Forwarded session checks on both ends.
6. `fibrilctl access check`, `check-policy` and the effective access view.
7. The audit of allowed destructive and administrative actions.
8. Service tokens.

## Test plan

- Pattern matching for `*`, `**`, trailing `*` within a segment, groups and the
  default group, including names at the 128 byte limit, and rejection of a `*`
  in the middle of a segment.
- Evaluation: allow only, deny only, mixed, multiple roles where one denies,
  and no roles at all.
- Every protocol operation and admin route refuses a user without the action
  and accepts one with it.
- A policy change closes subscriptions that lost `consume` and leaves others
  running.
- Forwarded publishes are refused at the public broker and, with a deliberately
  stale entry policy, at the owner.
- The lockout guard refuses a policy without `access.edit`.
- A delegated administrator scoped to `payments.**` can create and assign a
  role on `payments.eu.**`, and is refused a role naming `billing.**`, an
  administration action or `access.edit`.
- `view` alone lists a topic but refuses message inspection, and `read` allows
  it. `replay` without `purge` replays but cannot empty the queue.
- A publish or subscribe that would create a queue or group is refused without
  `declare`.
- Allowed deletes, purges and replays appear in the audit with their actor.
- An expired or revoked token is refused, and a token never exceeds its user.
- A file policy round-trips through the API and the dashboard unchanged.

## Open questions

- Whether roles should include other roles. Multiple roles per user cover the
  common cases without it.
- Whether per-user publish rate quotas belong in roles or with
  [resource limits](/development/resource-limits-design/).
- Single sign-on for the dashboard, which would map external groups to roles.
