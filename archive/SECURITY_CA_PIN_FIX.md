# Security fix: CA-fingerprint TLS pinning is MITM-bypassable

RESOLVED 2026-07-09 - shipped across all five clients, all of AC1-AC8 met. This is
the archived design record. Outcome summary in FOLLOWUPS.md ("SECURITY (HIGH) ...
RESOLVED"). One design note vs the plan below: the Rust fix used rustls's own
`verify_server_cert_signed_by_trust_anchor` rather than a direct webpki dependency,
and all five stacks turned out to be lenient on an absent EKU (so the broker's
no-EKU server cert validates unchanged). Original working doc follows.

## The vulnerability (confirmed 2026-07-09)

Fingerprint pinning accepts the presented chain if ANY certificate in it matches the
pinned SHA-256, while only the TLS handshake SIGNATURE is verified. The handshake
signature proves possession of the LEAF key, not the pinned cert's key. So:

- LEAF-fingerprint pinning is SOUND (an attacker cannot hold the real leaf's key).
- CA-fingerprint pinning is BYPASSABLE. The broker prints the CA fingerprint and
  includes the CA in the chain (crates/tls/src/lib.rs - the design sells "survives
  leaf rotation"). A network MITM presents `[rogue_leaf (attacker key), real_CA_cert]`.
  `real_CA_cert` matches the pin; the handshake is signed by `rogue_leaf`; the client
  accepts the attacker. The real CA cert is public (in every legitimate chain), so
  this is trivially exploitable.

Root cause: "the chain CONTAINS the pinned fingerprint" is NOT "the leaf is SIGNED BY
the pinned cert". Leaf-only pinning would be sound + simple but breaks the advertised
rotation survival AND the broker prints the CA fp (not the leaf) - so the real fix is
required.

## The sound fix

Find the presented cert C whose SHA-256 matches the pin, then PATH-VALIDATE the leaf
against C as the SOLE trust anchor (real signature + validity + chain checks), instead
of merely confirming C appears in the chain. Skip SNI/hostname verification (broker
material is self-signed; hostname is not the trust basis here - the pin is). If C is
itself the leaf, that is leaf-pinning and the existing handshake-signature check is
already sufficient, but running it through the same path validation is fine and
uniform.

## Per-client state + fix (file:location, current impl, approach)

- **C# - clients/csharp/Fibril/Tls.cs** (`Validate` ~74, `MatchesPin` ~122). ALREADY
  builds an `X509Chain` with `ChainPolicy.TrustMode = CustomRootTrust` (~101). CHECK
  FIRST: does it path-validate the leaf against the pin-matched cert as the custom
  root, or does `MatchesPin` (leaf ~124 OR any chain element ~132) still short-circuit
  acceptance without validating leaf-signed-by-C? If it already validates, C# is the
  SOUND REFERENCE - mirror its shape in the others. If not, fix it too.
- **Rust - crates/client/src/tls.rs** `FingerprintVerifier::verify_server_cert`
  (~363-380): loops end_entity+intermediates, returns `assertion()` on first pin match.
  Doc comment ~354-356 ("a peer must hold the pinned certificate's key") is FALSE for
  the CA case - fix it. rustls 0.23 / tokio-rustls 0.26. Fix: on match of cert C, build
  a trust anchor from C (`webpki::anchor_from_trusted_cert`) and verify the leaf via
  `webpki::EndEntityCert::try_from(end_entity)?.verify_for_usage(algs, &[anchor],
  intermediates, now, KeyUsage::server_auth(), None, None)`. Confirm the exact
  rustls-0.23-re-exported webpki API (may need the `webpki`/`rustls-webpki` types;
  check what tokio-rustls 0.26 exposes). Keep verify_tls13/12_signature as-is.
- **Python - clients/python/src/fibril/client.py** `_chain_matches_pin` (~421-436),
  `_build_ssl_context` (`verify_mode = CERT_NONE` ~398), pin check in `_open_connection`
  (~485-491). stdlib `ssl` cannot path-validate against an arbitrary presented cert.
  Options: (a) use the `cryptography` package to do path validation of the leaf against
  C; (b) re-verify by loading C as the only CA into a fresh SSLContext (verify_mode
  REQUIRED) and re-handshaking. (a) is cleaner but adds a dep - discuss which. Also fix
  the docstring/comment.
- **Go - clients/go/** (TLS setup with `CAFingerprint`; tests tlsconn_test.go:64/89,
  mtls_test.go). Find the `VerifyPeerCertificate`/`VerifyConnection` that checks the
  fingerprint. Fix: on match of C, `x509.CertPool` with C as root, then
  `leaf.Verify(x509.VerifyOptions{Roots: pool, ...})` (no DNSName - skip hostname).
- **TS - clients/typescript/src/client.ts** (`createHash` import ~8, `caFingerprint`
  ~86/287; the cert check is in the connect path). node `tls` with
  `rejectUnauthorized:false` + a post-connect cert check today. Fix: validate the leaf
  chains to C. node's built-in verification against a custom CA is the clean path -
  set `ca:[C]` and re-check, or use `checkServerIdentity`/`peerCertificate` chain
  walking. Find the current check first.
- **Broker - crates/tls/src/lib.rs**: prints the CA fingerprint, includes CA in chain.
  NO change expected (AC7) - confirm.

## Acceptance criteria (verify EACH, per client, before "done")

- AC1 (the point): pinning the CA fingerprint REJECTS `[rogue_leaf (attacker key),
  real_CA_cert]` - handshake fails. Automated adversarial test per client.
- AC2 (no regression): the legitimate `[real_leaf, real_CA]` chain with the CA
  fingerprint pinned is ACCEPTED.
- AC3 (feature preserved): after leaf rotation under the same CA, the CA-fp pin
  accepts the new leaf.
- AC4: leaf-fingerprint pinning still works and stays sound.
- AC5: automated AC1 + AC2 tests in EVERY client (Rust, TS, Python, Go, C#).
- AC6: docs + the false Rust doc comment corrected; no "any cert in the chain" wording.
- AC7: broker unchanged (confirm) - still prints the CA fp.
- AC8: CHANGELOG Fixed entry (real vuln on the released Rust/TS/Python clients; Go/C#
  are new this cycle so their part is subsumed - see the changelog within-cycle rule).

## Execution order

1. Inspect C# Tls.cs - decide if it is the sound reference. Write the AC1 adversarial
   test first (a helper that builds a self-signed CA, a real leaf signed by it, and a
   rogue leaf with its own key; presents `[rogue_leaf, real_CA]`).
2. Do each client: fix + AC1/AC2 tests, verify green. Rust and Python are the confirmed-
   broken ones; C#/Go/TS verify-then-fix.
3. AC3/AC4 coverage, docs (AC6), confirm broker (AC7), CHANGELOG (AC8).
4. Walk every AC and report pass/fail. Then archive this doc + FOLLOWUPS security item.

## Gotchas

- Do NOT check SNI/hostname during pin validation - the pin is the trust basis, and
  broker certs are self-signed. Verifying hostname would break legitimate connections.
- The adversarial test must sign rogue_leaf with a DIFFERENT key than real_CA, else it
  is not testing the attack. real_CA must be a real signer of real_leaf.
- Leaf-pin (C == leaf) must keep working - do not require an issuer above the leaf.
- Full context of how this was found + the audit it came from: archive/API_CONSISTENCY_
  AUDIT.md (Tier 3 / Tier 5) and the FOLLOWUPS.md security brief.
