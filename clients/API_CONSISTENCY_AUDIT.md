# Client API Consistency Audit + Action Plan

Source: two external design-review passes over the client-usage + reconnect docs
(2026-07-08), plus in-repo verification of the specific claims. Pre-alpha, no
stability contract yet — the right moment to unify vocabulary before it hardens.

The behaviors mirror (golden `wire_vectors.json` + `error_guides.json` + a
same-shaped engine under each idiomatic skin prove wire/error parity). The
*vocabulary* drifts. Golden vectors validate bytes and error identity but never
see the surface spelling, so `msg.complete()` vs `msg.Ack()` both pass — vocabulary
is unpinned by construction. This doc is the to-do for that gap.

## Organizing principle (the useful lens from the review)

Classify every divergence into exactly one of:
- **✅ pinned** — fixed by a golden vector / error guide (wire codec, error identity,
  reserved `fibril.*`/`stroma.*` headers). Already provably uniform. Nothing to do.
- **🟢 idiomatic** — the language forces it (Go `ctx`-first, C#
  `CancellationToken`/`IAsyncDisposable`, Rust `Duration`, Python blocking facade,
  casing like `CAFile`/`CaFile`). Making these uniform would be *worse*. Leave.
- **🟠 free drift** — a free word-choice the engine does not constrain
  (`complete`/`Ack`, `content`/`text`, enum spelling, config type name). **Unify.**
- **🔴 internal defect** — trips a *single-language* user regardless of consistency
  (Go positional-boolean subscribe, Python content/text write-read split). Fix
  first, even if Fibril shipped one language.

Decision rule per row: *does the language make me do this?* If no, it is drift
wearing an idiom costume.

## What is genuinely good (protect these)

- Confirmed-publish trio (`publish` → `publish_confirmed` → `publish_with_confirmation`
  / `.confirmed()`), pipelining via deferred confirmation handles.
- Typed TLS error taxonomy (distinct types because the fix differs; none retried
  automatically). The gold standard the rest of the error surface should reach.
- Client-cert-as-identity auth (verified cert names an existing user, no password).
- Conservative reconnect reconciliation with opt-in restore (safety-first default).
- No replay of in-flight operations on reconnect (refuses to guess → no dup work).
- Explicit reconnect outcome (`Resumed`/`FreshConnection`) instead of `Ok(())`.
- Stream start-position set (`from_latest`/`from_earliest`/`from_offset`/`from_last`
  /`from_time`); pattern-subscribe attaching new channels without reconnect.
- One conceptual API, idiomatic per language; Rust as the declared reference.
- Golden vectors + error guide + shared engine (behavior parity, provable).

## Canonical vocabulary + coverage table

Columns: current name per language, proposed canonical, class. Adjust only for
casing idioms (Go initialisms, C# PascalCase) — the *word* is what must not drift.

| Concept | Rust | TS | Python | Go | C# | Proposed | Class |
|---|---|---|---|---|---|---|---|
| Positive settle | `complete()` | `complete()` | `complete()` | `Ack()` | `Ack()` | `ack()`/`Ack()` (matches section title + convention) | 🟠 |
| Text body (read) | `content()` | `content()` | `text()` | `Text()` | `Text()` | one word, matches write | 🟠 (Python 🔴 write/read split RESOLVED: write is now `text()`) |
| Text body (write) | `NewMessage.content()` | `.content()` | `.text()` (was `.content()`) | `Text()` | `Text()` | match read accessor | 🟠 |
| Raw bytes (read) | `raw()` | `raw()` | `raw()` | `.Payload` | `.Payload` | one name | 🟠 (🔴 Go: constructs `Raw()`, reads `.Payload`) |
| Decode (ct-dispatch) | `deserialize()` | `deserialize()` | `deserialize()` | `JSON(&v)` only | `Json<T>()` only | give Go/C# a ct-dispatcher OR reword the section intro (currently over-promises) | 🟠 |
| Reconcile policy value | `Restore` (was `RestoreClientSubscriptions`) | `"restore"` | `"restore"` | `ReconcileRestore` (value `"restore"`) | `Restore` | `restore` | 🟠 RESOLVED |
| Queue config type | `QueueConfig` | `QueueConfig` | `QueueConfig` | `DeclareQueue` | `QueueDeclareOptions` | one scheme | 🟠 |
| Stream config type | `StreamConfig` | `StreamConfig` | `StreamConfig` | `DeclarePlexus` | `PlexusDeclareOptions` | one scheme | 🟠 |
| Retention field | `retain_records` | `retainRecords` | `retain_records` | `MaxRecords` | `MaxRecords` | one word (`retain_records`) | 🟠 |
| Auth struct | — | — | — | `Auth` | `Credentials` | one word | 🟠 |
| Work-queue subscribe | builder | builder | builder | `SubscribeTopic(ctx, t, TopicSubscribeOptions{...})` (was positional `&group, 32, false`) | named args | Go → options struct (match its own `SubscribeStreamTopic`) | 🔴 Go RESOLVED |
| Go optional declare fields | — | — | — | pointer-to-local (`&group`, `&maxRetries`) | — | option funcs / builder | 🔴 Go |
| Delay/retry duration | `Duration` + bare `30`=sec | `Date`/ms + bare `30_000`=ms | `timedelta` | `time.Duration` | `TimeSpan` | drop bare-number overload OR unit in name (`publishDelayedSecs`) | ⚠ hazard |
| Subscribe terminal | `.sub()` after `subscribe()` | `.sub()` | `.sub()` | n/a | n/a | `.open()`/`.consume()` (near-homonym) | · nit |
| Shutdown | `shutdown()` | `shutdown()` | `shutdown()` | `Shutdown()` | `DisposeAsync()` | add C# `ShutdownAsync()` alias (keep `IAsyncDisposable`) | 🟢+alias |
| Wire codec / headers / error identity | — | — | — | — | — | pinned by vectors + error guide | ✅ |

## Glossary decisions (2026-07-08, locked)

The canonical word for each genuine fork, decided with the user. The reference
client (Rust) moves where it is the outlier.

- **Positive settle → `complete`.** Keep the work-queue framing (complete/fail/
  retry as job outcomes). Flip Go/C# `Ack()` → `Complete()`; align the family so
  the negative is `Fail()` not `Nack()` (public surface only; wire op stays `Ack`).
- **Text body → `text`.** Flip Rust + TS `content()` (read and write) → `text()`.
  Python + Go + C# already there. (Content-type accessors keep `content_type`.)
- **Declare config type → `QueueConfig` / `StreamConfig`.** Flip Go `DeclareQueue`/
  `DeclarePlexus` and C# `QueueDeclareOptions`/`PlexusDeclareOptions` to the config
  word (and `Plexus` → `Stream` in the type name). The client *method* stays
  `DeclareQueue()`/`DeclarePlexus()` (method verb + config-type noun, as in Rust).
- **Retention count → `retain_records`.** Flip Go/C# `MaxRecords` → `RetainRecords`
  (and TS's internal `maxRecords` retention field, so method and field agree).
- **Mechanical defaults (no user call needed):** reconcile policy value → `restore`
  (`ReconcilePolicy::Restore` / Go `ReconcileRestore` / TS+Py `"restore"`); auth
  struct → `Credentials` (flip Go `Auth`); raw-bytes read left idiomatic
  (`raw()` where a method exists, `.Payload` field access in Go/C#); Go/C# decode
  entry points documented rather than forced into a content-type dispatcher.

Then a CI naming-lint fails when one concept is spelled two ways, so the surface
cannot drift back (the real lever, per Tier 2 below).

### Execution status (2026-07-08)

DONE across all five clients (code + examples + tests + doc tabs, each client's
suite green): settle verb `complete` (Go/C# `Ack`->`Complete`, raw public `Nack`
dropped), text body `text` (Rust + TS `content`->`text`; Python did it in the
Tier-1 pass), declare config type `QueueConfig`/`StreamConfig` (Go and C# renamed),
retention `retain_records` on the records axis (Go/C# fields + the TS retention
field aligned with the existing builder), Go auth struct + field -> `Credentials`.

Reconcile policy value -> `restore` is now DONE across the stack too. It was not a
client-local rename: `ReconcilePolicy` lives in the shared `crates/wire` crate and
the broker (`crates/protocol`) matches on it, encoded as a numeric byte, so the
rename is wire-compatible (the golden `reconcile_client` vector, which stores the
byte not the name, is unchanged) but reached into the broker. Rust variant
`RestoreClientSubscriptions` -> `Restore`; TS/Python/Go string
`"restore_client_subscriptions"` -> `"restore"` (Go's constant name `ReconcileRestore`
and C# `Restore` were already idiomatic-canonical). Broker + all client suites green.

The naming-lint ships as `clients/vocab-lint.sh` (+ a `client-vocab-lint` CI
workflow). It only bans spellings with no legitimate use left, so it never fires on
internal wire DTOs. It guards the text-body accessor, the settle verb, the declare
config type, and the reconcile value. It does NOT cover retention (the `max_records`
wire field name is still legitimate on the wire).

## Action plan by tier

**Tier 1 — internal defects (fix first; standalone bugs). VERIFIED in-repo.**
- Go `SubscribeTopic` (`clients/go/fanin.go:176`) → options struct like
  `SubscribeStreamTopic` (`:219`). Kills positional-boolean + pointer-to-local. Low.
- Python `content()` write (`message.py:115`) vs `text()` read
  (`subscription.py:74`) → one word. Low.
- Go pointer-to-local optional declare fields → option funcs/builder. Med.

**Tier 2 — glossary pass (free drift) + naming-lint.** Apply the table verbatim,
casing-idioms only. Add a CI naming-lint that fails when one concept is spelled two
ways, so drift cannot reintroduce (the real 8→9 move: not "renamed" but "can't
drift again"). Touches all five clients — do as one coordinated pass. Highest lever
(this is the axis separating Fibril from NATS).

**Tier 3 — latent hazards.**
- Bare-number durations, per-language units (Rust sec vs TS ms) → drop bare overload
  or bake unit into name. A unit mismatch that still type-checks. Do it.
- Python 3.13 `tls_ca_fingerprint` silently pins whole chain vs leaf on older →
  fail loud on older Pythons rather than checking less (it is a security control).

**Tier 4 — docs.**
- DONE - `reliability/reconnects.md` under-claim fixed. Verified the true support
  set in-repo first: resume identity + one-attempt auto-reconnect + reconcile +
  opt-in restore + no-replay + closed-stream-as-end-of-stream are ALL FIVE; the only
  uneven bits are explicit user `reconnect()` returning the handshake outcome and
  disabling auto-reconnect (Rust/TS/Python only - Go/C# reconnect automatically with
  no explicit call or disable knob). Replaced the "Rust and TypeScript" lines with
  the accurate set + per-language closed-stream shapes.
- DONE - support matrix. Added a per-client matrix in `reconnects.md` for the uneven
  bits and two matching rows in `clients/FEATURE_MATRIX.md`, and stopped inline
  client enumeration for the uniform features ("the clients").
- DONE - central entry + cross-link. `reconnects.md` (user-facing) links to the
  `reconnection-grace` dev note and vice-versa.
- DONE - defined what "known to be closed" means (socket read/write fail, EOF, missed
  heartbeat, fatal TLS alert).
- DONE - admin HTTP API topic field `tp` -> `topic`. Broader than just DLQ: the
  admin request DTOs (create/delete queue, create stream, per-queue and global
  dead-letter target) all spelled it `tp`, and the global-DLQ response leaked the
  storage `GlobalDLQ.tp`. Renamed the admin request DTOs (admin-owned, not
  persisted) and added a thin response view DTO, leaving the PERSISTED
  `GlobalDLQ.tp` (msgpack `to_vec_named`) and the broker-wire `DLQDiscardPolicyWire`
  field untouched - those are internal formats, same principle as the client wire
  DTOs. fibrilctl updated in lockstep; the vocab-lint now guards `"tp"` / `pub tp:`
  across the admin + cli surface. `expected_version` kept.
- DONE - remaining doc cleanups: mental-model diagram (TCP -> logical connection
  -> subscriptions -> settlement) added to `reconnects.md`; the reserved `stroma.*`
  namespace explained on the clients page (Stroma storage/queue-state metadata, e.g.
  `stroma.dlq.*`, alongside `fibril.*`); the decode-dispatch intro reworded so it no
  longer implies Go/C# have a content-type `deserialize()` (they decode explicitly).

Tier 4 is now fully cleared.

**Tier 5 — design deepening (differentiators, bigger, mature over time; 9→10).**
- Typed stream-close reason (`End`/`ReconciliationFailed`/`ConnectionLost`/
  `PermissionRevoked`) instead of bare `None`. Already flagged as planned; top item.
- Reconnect outcome as a first-class typed state machine (`Disconnected`/
  `Reconnecting`/`Resumed`/`FreshConnection`/`ReconciliationFailed`).
- Extend the TLS-grade taxonomy to the *whole* error surface (typed,
  distinct-because-fix-differs, explicit retryability, uniform identity pinned by the
  error guide). One gold corner → the whole room.
- Footgun-resistance: make an unsafe-resume return a distinctly-typed value you
  cannot accidentally treat as live.
- Check the error guide keys on stable *identifiers* (shared identity, only the
  language binding differs) — not per-language identifiers.
- Ensure message-shaped decode errors (content-type present but body undecodable;
  type mismatch) are in the golden set, since Go/C# explicit decode entry points
  differ from the `deserialize()` dispatch in the other three.

**Tier 6 — nits.** `subscribe`/`.sub()` terminal; `publish_confirmed` vs
`publish_with_confirmation` legibility; long enum names; C# `ShutdownAsync()` alias.

**Leave alone (assessed).** Idiomatic divergences (ctx-first, CancellationToken/
IAsyncDisposable/DisposeAsync, Duration types, casing, Python blocking facade) —
uniformity would be worse. Auto-reconnect "explicit default": the *dangerous* part
(restore) is already opt-in, so this drops to "document the default clearly."

## Per-client standalone read (vacuum ratings, from the review)

Removing the cross-language penalty (a solo user of one client never sees the drift):
Rust ~8.5 (reference; only `sub`/`subscribe` + confirm-naming nits), C# ~8 (named
args, idiomatic .NET), TS ~8 (idiomatic; ms-delay hazard shrinks solo), Python ~7.5
(async/blocking facade elegant, but the content/text seam is internal), Go ~7
(problems are Go-native: the subscribe signature + pointer-to-local, and its own
stream API shows the better pattern). Diagnostic: vacuum-judging *lifts* the clients
whose only sin was consistency (C#/TS/Rust) and *does not rescue* the two with
genuine internal defects (Go subscribe, Python content/text) — so those two are the
"fix regardless" set.

## External comparison (design axis only; maturity separate)

Reviewer put Fibril client-API *design* in the upper tier: above mainstream
AMQP/Kafka clients, roughly level with the better modern families (Redis go-redis/
ioredis, Azure Service Bus .NET), a notch below **NATS** (~9) which it most
resembles and should study. NATS's edge is cross-language naming discipline (the
exact gap here) + maturity. Fibril's TLS taxonomy is arguably cleaner than NATS's
error surface. gRPC buys uniformity by codegen at the cost of idiom; Fibril buys
idiom (hand-written skins) at the risk of drift — the golden-vector + shared-engine
approach aims for both. The telling praise: few "API scars" (accreted flags from
years of back-com/back-compat) — the surface still feels coherent. 8→9 is finishing
work (this doc); 9→10 needs the failure-mode seams above *plus* a stability track
record only time provides.

## Verified in-repo (2026-07-08)
- Python content/text: `clients/python/src/fibril/message.py:115` (`content`) vs
  `subscription.py:74` (`text`). REAL.
- Go subscribe: `clients/go/fanin.go:176` positional-boolean; `:219`
  `SubscribeStreamTopic(ctx, topic, opts StreamSubscribeOptions)`. REAL.
- Reconcile enum: C# `ReconcilePolicy.Restore`, Go `ReconcileRestore`, TS/Py
  `"restore_client_subscriptions"`, Rust `RestoreClientSubscriptions`. REAL.
- Doc staleness: `reliability/reconnects.md` "Rust and TypeScript" x4;
  `development/reconnection-grace.md` already all-five. REAL.
</content>
