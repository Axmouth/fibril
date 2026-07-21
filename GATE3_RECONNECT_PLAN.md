# Gate 3 plan: reconnect reconciliation family (#102-#105)

Working plan for the 0.5 clarity release. Expands the FOLLOWUPS brief with
verified as-is findings and precise acceptance criteria. Design decisions
marked PROPOSED await ratification before implementation starts. The typed
surface designed here is the one #111 later freezes, so shape choices are
made freeze-grade now.

## Goal

A client always KNOWS what happened to its subscriptions and inflight work
across a reconnect or broker restart, as typed surface rather than silence
or a generic disconnect.

## Invariants (from the brief, binding)

1. At-least-once is never weakened. Ambiguity resolves to redelivery plus a
   typed signal, never silent drop.
2. Every subscription termination has exactly one typed terminal event. No
   path may end a stream silently. Enforced test-side by an exhaustiveness
   sweep: every pump exit in the delivery paths maps to a reason.
3. Auto-resubscribe never changes settlement semantics. Unsettled
   deliveries from the old incarnation stay governed by the #104 rules.
4. Wire changes are additive only (serde-default trailing fields, new
   opcodes, no re-tagging of existing enum values). This family lands
   before the #110 wire freeze and must not force a version bump. One
   deliberate pre-1.0 exception is recorded in D6: ResumeOutcome gains
   a variant, breaking pre-0.5 clients on the restart-resume path only,
   accepted while nothing is frozen.
5. Single-node guardrail: everything here works on a standalone broker.
   Restart reconciliation (#105) especially, since single-node restart is
   its most common case.

## As-is findings (verified 2026-07-18, HEAD 26d52f1)

Broker and wire:

- There is no per-subscription close signal in the protocol. No
  SubscriptionClosed frame, no Unsubscribe op. Every delivery pump ends by
  silent stream cessation (queue pump handler.rs:1274/1305/1314, stream
  pump handler.rs:1438/1459/1468, assignment forwarder handler.rs:1081).
  Owner moves, queue deletion, and stream teardown all surface to the
  client as Deliver frames simply stopping.
- ReconcileSubscriptionResult.reason is a free string (wire lib.rs:552)
  with seven hardcoded literals in the handler, and metrics are keyed by
  string equality on those literals (handler.rs:1766-1778).
- ReconcileAction::RecreateClientSide is defined and wire-encodable but no
  broker path ever emits it (handler.rs:1787 no-op arm).
- Settles on unknown subs or dead tags are silently ignored as idempotent
  (handler.rs:3748, 3792). There is no settle confirmation on the wire at
  all, so a stale settle is invisible to the client.
- Resume sessions live only in process memory (ResumeSessionRegistry,
  handler.rs:1815-1954). The registry owner_id is minted fresh per process
  start, so a resume after broker restart fails as ResumeRejected via the
  owner_id mismatch branch, indistinguishable from a bad token.
- Session lifetime equals the reconnect grace window (reconnect_grace_ms,
  default 5000). There is no separate resume session TTL setting.
- Error frames carry (u16 code, free string). No machine-readable tagged
  code enum exists anywhere on the wire.

Clients:

- Rust: subscription end is a bare recv() -> None with no reason surface.
  apply_reconcile_result (client lib.rs:3518-3556) drops every non-Keep
  action identically: registry entry removed, sender dropped, consumer
  sees None. No resubscribe, no event.
- The five clients diverge on RecreateClientSide: Rust silently drops,
  TypeScript and Python close the consumer with a typed
  DisconnectionError carrying the reason string, Go and C# treat it the
  same as Keep and restore the channel. Latent (never emitted) but must
  converge in this arc.
- The subscription supervisor (failover.rs supervise_forward) already
  re-subscribes when its per-partition receiver closes, but only in
  dynamic fan-in mode (partition_resubscribe_interval_ms set). Static
  fan-in hands the raw engine receiver to the user with no supervisor in
  between.
- Settlement is fire-and-forget: complete() returns Ok(()) once the
  command is queued locally, a settle against a vanished sub_id no-ops
  silently, and nothing marks in-hand deliveries stale on a non-resumed
  reconnect. FibrilError has no stale or unknown tag variant.
- ResumeOutcome is captured and surfaced through reconnect() but no client
  logic branches on it.

Groundwork confirmed: #101 cold-start orphan handling and #108
owner-scoped resume identity are landed, member_id rides the exclusive
rejoin path end to end, the v0.4.0 golden fixture exists and runs in CI,
and persisted inflight state is a pure offset-to-deadline map with no
consumer identity (keratin stroma event.rs:131-135, state.rs:333).
Stream lag close and notification policies are explicitly deferred to
this typed surface (STREAM_PARITY_AUDIT.md:69,92).

## Design decisions

### D1 (PROPOSED): one close-reason taxonomy, used in three places

A single tagged code set, defined once in the wire crate, carried as:

- a new `code` field beside the existing free-string `reason` on
  ReconcileSubscriptionResult (additive, string kept for humans, metrics
  re-keyed onto the code),
- the payload of the new SubscriptionClosed frame (D3),
- the client-side terminal event surfaced to user code (D2).

Merged variant set (unifying the gate-3 brief draft and the #111 freeze
bundle draft):

| Variant | Emitted when | Origin |
| --- | --- | --- |
| Unsubscribed | user closed or dropped the subscription locally | client |
| QueueDeleted | topic or queue removed while subscribed | server |
| OwnerMoved | partition ownership moved away and the sub was not (or could not be) migrated | server |
| Reconciled { action, code } | reconcile verdict closed the sub (server_missing, server_mismatch, restore_failed, client_missing) | server |
| Recreated | reconcile recreated the sub and auto-resubscribe is off, so the old handle ends | client |
| Disconnected { source } | connection lost and not recovered (reconnect disabled, retries exhausted, or non-retryable close) | client |
| BrokerShutdown | broker announced drain or shutdown and the sub could not be handed off | server |
| Lagged | reserved for the deferred stream close-on-lag policy, not emitted in 0.5 | server |
| ServerError { code } | server closed the sub for an error not covered above | server |

Notes: the freeze bundle's PermissionRevoked folds into ServerError with a
dedicated code until per-topic authz exists. BrokerRestarted is not a
close reason, it is a resume outcome (D6): after a restart the
subscription is either restored or closed as Reconciled, and the restart
fact travels on the HelloOk.

### D2 (DECIDED 2026-07-18): in-band terminal event

Honors the recorded 2026-06-29 decision that the reason travels with the
stream. recv() returns a two-armed event instead of Option:

```rust
pub enum SubEvent {
    Delivery(InflightMessage),
    Closed(SubscriptionClosed),
}
```

The deciding observation: the common loop keeps its exact shape, with
only the pattern word changing (`while let SubEvent::Delivery(msg) =
sub.recv().await`), while careful code matches both arms and gets the
reason in-band. After termination recv() keeps returning the same
Closed(reason) (fused), so code that exited a while-let can still
recover the reason with one more recv or the close_reason() convenience
accessor.

Mechanics: the delivery channel stays an mpsc of messages. Every close
path stamps a per-subscription reason slot (set-once) before dropping
the sender. The recv() wrapper maps a received message to Delivery and
channel completion to Closed with the slot value. An mpsc receiver
drains buffered messages before reporting closure, so the terminal
event arrives exactly once, after all deliveries. A path that drops the
sender without stamping the slot yields Closed(Disconnected) as the
safety default, and the invariant-2 exhaustiveness sweep keeps that
default unreachable.

Per-client idiom mapping: Rust gets the in-band event enum. TypeScript
and Python keep typed-exception termination (their iterator idiom),
upgraded from DisconnectionError-with-string to the typed reason. Go and
C# cannot carry a value in a channel close or enumeration end, so they
keep bare channel/enumeration termination plus a CloseReason() accessor
that is guaranteed set before the close is observable.

### D3 (PROPOSED): a SubscriptionClosed frame, and the exhaustiveness rule

New additive opcode SubscriptionClosed { sub_id, code, message }. The
broker sends it whenever a server-side subscription ends while the
transport is live outside a reconcile response: queue deletion, ownership
moved away, stream teardown, cohort removal. The pump exit paths listed
in the as-is section each either send it or are covered by connection
loss (where the client-side Disconnected reason applies). The invariant-2
test asserts the mapping is total.

### D4 (PROPOSED): RecreateClientSide semantics and cross-client convergence

The broker starts emitting RecreateClientSide from reconcile when the
subscription is safely recreatable: manual-ack (redelivery covers the
gap; auto-ack cannot recreate honestly because in-flight deliveries were
already settled at send) and either no exclusive cohort or a rejoin
carried by member_id. Under Conservative policy today those cases return
CloseClientSide server_missing, which stays the verdict for the unsafe
cases.

Client behavior converges across all five: on RecreateClientSide, if the
subscription is supervised, re-subscribe through the existing supervisor
with no user-visible close. If unsupervised, surface the Recreated
terminal event. Opt-out knob (auto_resubscribe, default on) per the
product philosophy. Go and C# stop aliasing Recreate to Keep, Rust stops
silently dropping, TypeScript and Python route through the same policy
instead of unconditionally erroring.

### D5 (PROPOSED): #104 stale-tag surface is client-side, no wire change

Semantics: within reconnect grace and a Resumed outcome, delivery tags
remain settleable (already true, same logical session). On any
non-Resumed reconnect, the client marks every in-hand delivery from the
old incarnation stale. Settling a stale delivery returns a new typed
error (FibrilError::StaleDelivery and per-client equivalents) immediately
and locally, and the messages redeliver server-side as they already do.
The broker's silent idempotent ignore of unknown settles remains as the
safety net, unchanged. No settle confirmation is added to the wire (that
would tax the hot path for a case the client can detect itself).
Implementation: deliveries carry the engine incarnation they were
received on, and the settle path checks it against the current one.

### D6 (DECIDED, ratifies the reserved #105 scope): durable session skeletons

Scope stays redelivery-only, exactly as the brief recorded: a fast broker
restart lets clients resume with reconcile outcomes instead of a bare
rejection, messages redeliver per at-least-once, delivery tags die with
the process and #104 covers that honestly. No inflight lease reclaim, no
consumer identity added to storage (the golden fixture pins today's
encoding either way).

Mechanics:

- Persist on the broker's own durable store (works standalone, satisfies
  invariant 5): the registry owner_id plus per-session skeletons of
  client_id, resume_token, and the subscription set as
  ReconcileSubscription records. Sessions are owner-scoped state, so this
  is deliberately NOT coordination-replicated: another node cannot honor
  them and does not need them (topology-driven resubscribe already covers
  moved partitions).
- Skeletons are written on session creation and subscription change,
  dropped on clean session end, and expire on load past a bounded TTL.
- New setting resume_session_restart_ttl_ms (config crate plus runtime
  settings, per the settings discipline), bounding how stale a skeleton a
  restarted broker will honor. Grace (reconnect_grace_ms) keeps governing
  live-process dormancy exactly as today.
- Auth is NOT restored from a skeleton. The reconnecting client
  authenticates per the normal handshake rules for a fresh connection,
  and only then does reconcile restore the subscription set.
- Wire (DECIDED): ResumeOutcome gains a dedicated variant,
  ResumedAfterRestart, rather than shoehorning the restart fact into a
  side field. This is the deliberate pre-1.0 break recorded in
  invariant 4: a pre-0.5 client that resumes against a restarted 0.5
  broker fails decode on that one path, accepted because the honest
  enum is worth it while breaking is still free and impossible after
  #110. Semantics: ResumedAfterRestart restores the session skeleton
  and reconciles subscriptions like Resumed, but counts as non-Resumed
  for #104, so held tags go stale. The registry owner_id persists
  durably (not TTL bound), so a restarted broker keeps its owner
  identity: a resume with an expired or missing skeleton reports
  ResumeNotFound, and ResumeRejected narrows to genuine identity
  mismatches.

### D7 (PROPOSED): settlement of the reason string coupling

Metrics and log summaries re-key from string equality onto the tagged
codes. The strings stay on the wire for humans but nothing machine-reads
them anymore.

## Acceptance criteria

#102 typed close reason:

- A1. The wire defines one tagged close-code set. ReconcileSubscriptionResult
  carries it beside the retained reason string as an additive field.
  wire_vectors.json gains vectors proving old-format frames (no code)
  still decode and new frames round-trip byte-exact.
- A2. A SubscriptionClosed frame exists and the broker emits it on every
  server-side subscription end with a live transport that is not already
  reported through a reconcile response. Specifically covered: queue
  deletion, ownership moved without handoff, stream subscription
  teardown, cohort membership removal.
- A3. Every client surfaces a typed terminal reason on subscription end
  through the D2 shape: Rust recv() yields the SubEvent enum with a
  fused Closed terminal, TypeScript and Python terminate iteration with
  the typed reason, Go and C# guarantee CloseReason() is set before the
  channel close or enumeration end is observable. Termination without
  an available reason no longer exists in any client.
- A4. Exhaustiveness: a test (or lint-level sweep recorded in the test)
  enumerates every delivery-pump exit path in handler.rs and asserts each
  maps to either a SubscriptionClosed emission or a connection-loss
  reason. New exit paths fail the sweep by default.
- A5. Reconcile metrics key off tagged codes, not reason strings, with
  values unchanged for the existing seven cases.

#103 auto-resubscribe:

- B1. The broker emits RecreateClientSide exactly for the safe set:
  manual-ack subscriptions whose topic still exists where the server-side
  sub is gone, including exclusive-cohort subs (rejoin via member_id).
  Auto-ack and mismatch cases keep their current Close verdicts.
- B2. All five clients implement the same policy: supervised plus
  auto_resubscribe on (the default) resubscribes with no terminal event,
  otherwise the Recreated terminal event surfaces. The Go and C#
  Recreate-as-Keep aliasing and the Rust silent drop are gone.
- B3. Continuity: under an owner restart with auto-resubscribe on, a
  consumer observes no terminal event, no message loss, and no double
  settle. Unsettled deliveries from before the recreate follow #104.
- B4. The opt-out knob exists in every client with the same name family
  and default, with FEATURE_MATRIX rows added in the same change.

#104 inflight reconciliation:

- C1. After a reconnect with a Resumed outcome inside grace, previously
  delivered messages settle successfully. Covered by a test that holds a
  delivery across a connection drop and settles after resume.
- C2. After any non-Resumed reconnect, settling a held delivery returns
  the typed stale error in every client, immediately and locally, and
  the message redelivers exactly once more (per at-least-once) to the
  recreated or new subscription.
- C3. Staleness is marked at reconnect time, not discovered at settle
  time: a client can query or observe that in-hand deliveries went stale
  as soon as the handshake completes (the terminal surface or delivery
  handle exposes it), so user code learns before wasting work.
- C4. The broker settle path is unchanged (still idempotent-ignores
  unknown settles) and hot-path settle throughput is unaffected
  (existing perf suites within noise).

#105 durable restart reconciliation:

- D1. A broker restarted within resume_session_restart_ttl_ms honors a
  resume: the client reconnects, authenticates normally, reconcile
  restores or recreates its subscription set per policy, and unsettled
  messages redeliver. The client-visible outcome distinguishes a broker
  restart from a bad identity (no more ResumeRejected for a mere
  restart).
- D2. A restart past the TTL, or a skeleton for a different owner store,
  cleanly reports the existing not-found or rejected outcomes.
- D3. Delivery tags never survive a restart: held deliveries from before
  the restart are stale per #104 in every client.
- D4. Standalone single-node broker passes the whole restart matrix with
  no coordination configured. Cluster mode passes it on the owning node.
- D5. Skeleton persistence adds no wire-visible or storage-format change
  covered by the golden fixture (the fixture suite still passes
  unmodified), and skeleton load failures degrade to today's behavior
  (fresh sessions), never a boot failure.
- D6. Settings: the TTL is configurable at startup and runtime per the
  settings discipline, documented with its default.

Cross-cutting:

- E1. Wire additivity proven: previous-release client binaries (or their
  recorded vectors) still parse every touched frame. No existing enum tag
  value changes meaning. Single recorded exception: the new ResumeOutcome
  variant (D6), which old clients meet only on the restart-resume path.
- E2. FEATURE_MATRIX gains rows for: typed close reason, auto-resubscribe
  policy, stale-delivery marking, restart resume. Marked per client in
  the same change as each client lands.
- E3. The reconnects docs page is rewritten around the typed lifecycle
  (its current "not yet" section retires), and the CHANGELOG carries the
  user-visible surface in the same change.
- E4. Soak suite asserts no silent subscription end across a
  kill-connection matrix of policy x outcome x supervision mode.

## Bricks (implementation order)

1. Wire: close-code set, code field on ReconcileSubscriptionResult,
   SubscriptionClosed opcode, HelloOk restart field, vectors.
2. Broker: emit SubscriptionClosed at the pump/teardown sites, emit
   RecreateClientSide for the safe set, re-key metrics, exhaustiveness
   sweep test.
3. Rust client: terminal surface (per D2 ratification), incarnation
   stamping + StaleDelivery, auto-resubscribe through the supervisor,
   kill-connection matrix tests.
4. Broker: durable session skeletons + TTL setting + restart reconcile
   path + restart matrix tests (standalone first).
5. Client parity: TypeScript, Python, Go, C# to the same surface,
   FEATURE_MATRIX rows, per-client reconnect tests.
6. Docs: reconnects page rewrite, CHANGELOG, glossary touch-ups.

Bricks 1-3 are one reviewable arc (the typed surface), 4 is #105, 5-6
close the family. Each brick keeps the suite green on its own.

## Ratification record

All decisions ratified 2026-07-21:

- D2: in-band terminal event enum (the while-let-preserving shape).
- D6: broker-local durable skeletons, ResumedAfterRestart enum variant
  (deliberate pre-1.0 break), durable owner identity, auth never
  restored from a skeleton.
- B1: RecreateClientSide safe set is manual-ack only.
- D1: variant table stands as written. It gets formally re-ratified at
  the #111 freeze like the rest of the typed surface.

## Future assessments (recorded, not in this arc)

- Cross-node session transfer: resuming a session on a DIFFERENT node
  after node loss, beyond what topology-driven resubscribe already
  covers. A whole feature of its own (replicating session identity,
  deciding what a foreign node can honestly honor). Assess separately
  if ever wanted.
- Auto-ack auto-recreate: widen the B1 safe set to auto-ack
  subscriptions. Needs an honest story for deliveries settled at send
  but never processed across the gap (for example surfacing a
  possible-loss signal, or revisiting when auto-ack settles) before
  silent recreation stops hiding loss.
