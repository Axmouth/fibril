# Implementation safety

Prefer production code without `unsafe`, using safe Rust and safe library
interfaces. Introduce a production unsafe boundary only for a strong, documented
reason: explain why safe alternatives are inadequate, keep the boundary minimal,
and document and validate its exact safety invariants.

Isolated allocator-specific tests and diagnostic experiments may use necessary
unsafe FFI calls, with documented invariants. Keep those hooks out of ordinary
production builds. Experimental performance results alone do not justify moving
an unsafe hook into production.
