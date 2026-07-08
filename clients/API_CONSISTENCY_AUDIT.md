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
| Text body (read) | `content()` | `content()` | `text()` | `Text()` | `Text()` | one word, matches write | 🟠 (🔴 Python: writes `content`, reads `text`) |
| Text body (write) | `NewMessage.content()` | `.content()` | `.content()` | `Text()` | `Text()` | match read accessor | 🟠 |
| Raw bytes (read) | `raw()` | `raw()` | `raw()` | `.Payload` | `.Payload` | one name | 🟠 (🔴 Go: constructs `Raw()`, reads `.Payload`) |
| Decode (ct-dispatch) | `deserialize()` | `deserialize()` | `deserialize()` | `JSON(&v)` only | `Json<T>()` only | give Go/C# a ct-dispatcher OR reword the section intro (currently over-promises) | 🟠 |
| Reconcile policy value | `RestoreClientSubscriptions` | `"restore_client_subscriptions"` | `"restore_client_subscriptions"` | `ReconcileRestore` | `Restore` | one canonical member name | 🟠 |
| Queue config type | `QueueConfig` | `QueueConfig` | `QueueConfig` | `DeclareQueue` | `QueueDeclareOptions` | one scheme | 🟠 |
| Stream config type | `StreamConfig` | `StreamConfig` | `StreamConfig` | `DeclarePlexus` | `PlexusDeclareOptions` | one scheme | 🟠 |
| Retention field | `retain_records` | `retainRecords` | `retain_records` | `MaxRecords` | `MaxRecords` | one word (`retain_records`) | 🟠 |
| Auth struct | — | — | — | `Auth` | `Credentials` | one word | 🟠 |
| Work-queue subscribe | builder | builder | builder | `SubscribeTopic(ctx, t, &group, 32, false)` | named args | Go → options struct (match its own `SubscribeStreamTopic`) | 🔴 Go |
| Go optional declare fields | — | — | — | pointer-to-local (`&group`, `&maxRetries`) | — | option funcs / builder | 🔴 Go |
| Delay/retry duration | `Duration` + bare `30`=sec | `Date`/ms + bare `30_000`=ms | `timedelta` | `time.Duration` | `TimeSpan` | drop bare-number overload OR unit in name (`publishDelayedSecs`) | ⚠ hazard |
| Subscribe terminal | `.sub()` after `subscribe()` | `.sub()` | `.sub()` | n/a | n/a | `.open()`/`.consume()` (near-homonym) | · nit |
| Shutdown | `shutdown()` | `shutdown()` | `shutdown()` | `Shutdown()` | `DisposeAsync()` | add C# `ShutdownAsync()` alias (keep `IAsyncDisposable`) | 🟢+alias |
| Wire codec / headers / error identity | — | — | — | — | — | pinned by vectors + error guide | ✅ |

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
- **`website/src/content/docs/reliability/reconnects.md` says "Rust and TypeScript"
  in 4 places** (lines 31, 40, 71, 103 — incl. the handshake-outcome line), while
  `development/reconnection-grace.md` was already updated to all five. This
  *under-claims a safety surface* → correct each line to the true support set (keep
  genuine per-client gaps explicit). HIGHEST-STAKES single item.
- Structural: assert parity once + a per-client support matrix for the uneven bits
  (handshake-outcome accessor, reconcile-close reason, closed-stream signal shape).
  Stop inline-enumerating client names (an allowlist silently becomes a denylist).
- Central reconnect entry point (there are TWO pages: `reconnection-grace` +
  `reconnects`) — one canonical entry + cross-link.
- Mental-model diagram (TCP → logical connection → subscriptions → settlement).
- Explain the reserved `stroma.*` namespace on the clients page.
- Admin DLQ `"tp"` field → `"topic"` (keep `expected_version`, it is a nice touch).
- Define what "known to be closed" triggers reconnect (read/write fail, heartbeat,
  EOF, TLS alert).

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
