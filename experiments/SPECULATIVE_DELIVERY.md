# Speculative queue delivery prototype

Status: experimental branch, disabled by default, not approved for production adoption. This branch preserves staged early delivery and the selected bounded dispatch handoff. Compio and the asynchronous storage backend are excluded; storage uses the ordinary inline writer path.

## Build and run

Use matching sibling checkouts of Fibril and Keratin at `experiment/speculative-delivery`, plus the normal sibling Ganglion checkout. Fibril's workspace patches resolve those local dependencies. Build the server with `cargo build --release -p fibril --bin fibril-server` and run the existing broker configuration.

The development-only environment setting `FIBRIL_EXPERIMENTAL_QUEUE_SPECULATION` selects:

- `0` (default): ordinary durable delivery and confirmation.
- `1`: eligible staged messages can be delivered early; producer confirmation still requires durability.
- `2`: eligible staged messages can be delivered early; producer confirmation can follow durability or a valid consumer processing ACK. This is a different confirmation contract and still needs targeted crash validation.

The retained low-overhead admission/handoff variant uses `FIBRIL_EXPERIMENTAL_SPEC_ADMISSION=cheap` and `FIBRIL_EXPERIMENTAL_SPEC_HANDOFF=1`. Optional sampled internal traces use `FIBRIL_EXPERIMENTAL_SPEC_TRACE=1`; leave them off for clean timing comparisons.

## Boundaries

Eligible queues are local-only, without follower assignments. Replicated queues use the ordinary path regardless of their configured confirmation policy. TTL, delayed messages, exclusive-consumer constraints, holds, admission pressure and unavailable consumer credit fall back to ordinary delivery. Admission is bounded by byte and item budgets (32 MiB estimated bytes and 4096 items); the dispatch handoff requests about 250 microseconds and is subject to scheduler delay.

Messages are staged and assigned real offsets before early dispatch. Both storage logs still persist them. Persistent `fibril.message_id` metadata identifies a message across redelivery, while the delivery tag identifies a particular delivery attempt. `fibril.speculative` marks early delivery. The broker reserves its internal metadata namespace; SDK automatic deduplication is not implemented. Ordinary and early paths preserve partition delivery order through their registration/fallback coordination.

## Adoption gates

1. Force actual early delivery and consumer ACK before durability, then inject process loss or persistence failure at confirmation and settlement seams. Verify the processing-ACK contract, stable redelivery identity and producer outcomes after restart. Earlier broad worker/requester loss probes mostly exercised durable fallback and do not close this gate.
2. Exercise prolonged pressure on both admission budgets, slow/disconnected consumers, randomized early/fallback transitions and expiry-related races.
3. Define production configuration and API documentation for the separate confirmation contracts, metadata and application-owned deduplication.
4. Repeat ordinary and speculative regressions and representative physical-storage measurements after integration with current main. Keep replicated speculation excluded until separately designed.

The branch includes focused speculative lifecycle tests, ordinary ordering coverage and the ACK ownership/accounting fixes. SIGKILL tests exercise process loss and do not simulate sudden power loss. The prototype's test-only commit gate and injected failure controls are research seams, not deployment settings.

## Preserved evidence

The local handoff contains the original staged-delivery, dispatch-handoff, physical SATA/NVMe, service-RPC and fault experiments. This branch originates from the service-cluster prototype, which includes the later durable application-order fix; it excludes the subsequent replication timing/ablation experiments. No competitor benchmark results are published with it. New validation results for this extracted branch are recorded below.

Git-sourced CI/Docker builds pin Keratin `48af39327795726b661ab5aef5f4053954a13e90` from the matching experimental branch. Local workspace patches still use the sibling checkout.

Extraction validation: all 117 broker unit tests, 134 ordinary broker integration tests and 16 speculative integration tests passed. The matching Keratin branch passed 292 Stroma tests with one existing ignored test. These checks preserve the tested prototype and do not close the crash-safety gates above.

The complete server also passed `cargo check --offline --locked -p fibril --bin fibril-server` after extraction.
