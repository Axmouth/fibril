---
title: Stream filters
description: Design for richer Plexus stream filters, using the access control pattern rules for header values, groups of clauses joined by OR, and fixed bounds.
---

This is a proposed design. Today a stream subscription can carry an AND of
`header = pattern` clauses, where `*` matches any run of characters (see
[Plexus streams](/concepts/plexus-streams/#filtering)). This note sets where
filters should end up and where they stop.

## Goals

- Select records inside one stream by their headers, including NATS-style
  subjects carried in a header.
- One pattern language across the product: access control resources, stream
  filters and the clients' wildcard subscribe.
- Stay declarative and bounded, so every record costs a small, predictable
  amount to check and the broker keeps no routing state.

Out of scope: regex, matching on the message body, scripting, arbitrary
nesting, and filtering on queues. Queue filtering stays out for the reasons in
the [queue-side filtering note](../../../../../archive/implementation-notes/FOLLOWUPS_COMPLETED.md):
records no consumer matches, head-of-line blocking and unfair scans.

## Patterns

Header value patterns follow the [access control pattern rules](/development/access-control-design/#resources),
with dots as segment separators:

- A pattern with no `*` matches that exact value.
- `*` as a whole segment matches one segment: `orders.*.created` matches
  `orders.eu.created` but not `orders.eu.west.created`.
- `**` matches any number of segments: `orders.**` matches every value under
  `orders`, and `**` alone matches any value.
- A `*` at the end of a segment matches values starting with the text before
  it, within that segment: `eu-*` matches `eu-west` and `eu-central`.
- A `*` anywhere else inside a segment is an error at subscribe time.

A value without dots is a single segment, so filters like `region = eu-*` keep
working. The one change from today is that `*` no longer crosses a dot, so
`orders.*` stops matching `orders.eu.created`. `orders.**` is the new spelling
for that. A leading or inner `*` such as `*-west` becomes an error with a
message pointing at the new rules.

The rules live in one matcher shared by the broker's filters and access
control. The clients' wildcard subscribe follows the same rules, checked
against a shared set of test patterns in `clients/` that every client and the
broker run, the same way the wire vectors keep the clients in step.

## Subjects

There is no subject feature beyond a convention. A publisher puts the subject
in a header and subscribers filter on it:

```rust
publisher.publish(NewMessage::text("...").header("subject", "orders.eu.created")).await?;

client.stream("events")?
    .filter("subject", "orders.*.created")
    .sub_auto_ack()
    .await?;
```

A per-subject index, last-record-per-subject retention and per-subject limits
are deliberately not planned. Filtered backfill scans the retained range and
skips non-matching records. A consumer that needs cheap, separately retained
subjects uses one stream per subject with the clients' wildcard subscribe,
which already attaches new matching streams and backfills them. A subject index
would be that same pattern subscribe with extra steps inside one stream.

## Filter shape

A filter is a list of groups. A record matches when any group matches, and a
group matches when all of its clauses match. Every boolean condition can be
written this way, and keeping it flat keeps evaluation simple.

Clauses:

| Clause | Matches when |
| --- | --- |
| `header = pattern` | The header is present and its value matches the pattern |
| `header != pattern` | The header is missing, or its value does not match |
| `header in [a, b, c]` | The header's value is one of the listed patterns |
| `header exists` | The header is present with any value |
| `header missing` | The header is absent |
| `header < n`, `<=`, `>`, `>=` | The value parses as a number and compares true. A value that does not parse never matches |

Today's filter is one group of `=` clauses, so existing subscriptions keep
their meaning, apart from the dot rule above.

Bounds, checked at subscribe time with a clear error:

| Limit | Value |
| --- | --- |
| Groups per filter | 8 |
| Clauses per group | 8 |
| Values per `in` list | 32 |
| Pattern length | 256 bytes |

## Cost

Measured on 2026-06-29 (Ryzen 5950X, `crates/broker/benches/stream_filter.rs`):
no filter costs about 3 ns per record, one clause about 22 ns and three clauses
about 57 ns, mostly the header key lookup rather than the pattern match.
Checking each subscriber separately costs about 39 ns per subscriber, so 4096
subscribers sharing one two-clause filter spend about 160 µs per record.

So:

- Patterns compile once at subscribe time, as today.
- Identical filters are interned and checked once per record, with the result
  shared by every subscriber holding that filter.
- The header lookup moves off the general-purpose hash where it shows up in
  the bench.
- The bench grows cases for groups, `in` lists and numeric clauses, and stays
  the regression baseline.

## Wire and clients

The subscribe frame gains an optional filter field holding groups of
clauses, each an operator, a header name and its values. Brokers that do not
know the field keep reading the existing clause list, and a client sends the
old list when its filter is a single group of `=` clauses, so either side can
upgrade first. Clients keep today's builder, where chained `.filter(header, pattern)` calls
form one group. An `.or()` call starts the next group, and typed helpers add
the other clauses, for example `.filter_not`, `.filter_in`, `.has_header` and
`.filter_range`. A string form exists only for the CLI and the dashboard.

## Order of work

1. The shared matcher with the access control rules, the shared test patterns
   and the dot change for existing filters.
2. Filter interning, checked once per distinct filter per record.
3. Groups and the new clauses, the wire field and the client helpers.
4. A subject example in the stream docs.
