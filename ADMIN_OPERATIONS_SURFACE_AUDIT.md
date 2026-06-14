# Admin Operations Surface Audit

This audit reviews the admin API and UI from an operator perspective: what an
operator can safely do during setup, debugging, and incidents, and where the
surface still needs clearer behavior or stronger guardrails.

## Goal

The admin surface should make operational state and actions obvious without
requiring the operator to understand internal broker machinery.

- Settings writes should have a clear authority and version contract.
- Auth behavior should be predictable when enabled or disabled.
- Message inspection should be useful for debugging without looking like a live
  query interface.
- DLQ replay should be explicit, bounded, and non-destructive.
- Cluster topology should show ownership, followers, epochs, and coordination
  health without requiring Raft knowledge for routine use.

## Findings

### 1. Runtime Settings Authority Is Clear

Status: Addressed

Standalone mode uses the local runtime settings manager. Ganglion mode uses the
cluster runtime settings store first and only reports success after the cluster
document commits. Stale writes return `409 Conflict`, node-local runtime locks
return `423 Locked` in standalone mode and are rejected for Ganglion mode at
startup.

Coverage:

- Local `GET` and `PUT` runtime settings tests.
- Cluster-store `PUT` test.
- Local conflict and lock tests.
- Runtime settings Ganglion audit covers authority and validation details.

### 2. Auth Disabled/Enabled UI Is Consistent

Status: Addressed

When auth is disabled, protected pages and APIs are available and the layout
shows `Auth disabled` without a logout link. When auth is enabled, pages redirect
to `/login`, APIs require Basic auth or a session cookie, login creates a
session, and logout removes it.

Coverage:

- Protected page redirect test.
- Login/session/logout tests.
- Basic auth and session API test.
- Auth-disabled layout test.

Follow-up:

- Harden cookies with `Secure` when the admin UI is served behind HTTPS. This is
  deployment-sensitive and not a blocker for the current local/internal admin
  model.

### 3. Message Inspection Status Filtering Is Applied After Page Read

Status: Audited

Severity: Medium

Current behavior:

`GET /admin/api/messages` asks storage for a page by `from` and `limit`, then
filters the returned items by `status` in the admin route.

Impact:

An operator can request `status=ready` and get an empty page even if ready
messages exist after the current page window. Pagination still moves by
`next_offset_hint`, so this is not data loss, but it is a confusing query shape
for an operations tool.

Preferred fix:

Keep the storage inspection API offset-window shaped. If operators need pages
that contain up to N matching rows, the admin route can do a bounded scan-fill:
read successive inspection windows, filter in admin, stop when the requested
page is full or a scan budget is reached, and report how far it scanned. The
lighter alternative is an explicit `filtered_after_read` response field and UI
note so the operator knows the page was filtered after reading a bounded window.

Coverage now:

- Message inspection returns active messages with payload preview.
- Unknown status filters return `400 Bad Request`.

### 4. Message Inspection Is Properly Bounded

Status: Addressed

The admin route caps page size and payload preview size. The UI warns before
large inspections and defaults to no payload preview. Returned rows omit fields
that are not available rather than displaying misleading placeholder data.

Coverage:

- Payload preview truncation test.
- UI smoke test for the inspection form and status filter.

Follow-up:

- Add a backend test for max limit clamping if this endpoint becomes part of a
  stable external admin API.

### 5. DLQ Replay Is Bounded And Non-Destructive

Status: Addressed

DLQ replay requires explicit offsets, caps each request at 100 offsets, reads
active DLQ messages with full payload, strips reserved `stroma.` and `fibril.`
metadata before replay, and copies messages back to their recorded source queue.
The DLQ messages are not removed.

Coverage:

- Missing offsets return `400 Bad Request`.
- Too many offsets return `400 Bad Request`.
- End-to-end admin DLQ config test routes a failed message to a DLQ over TCP.
- Broker tests cover DLQ replay behavior.

Follow-up:

- Add a direct admin route test for successful replay once the test helper can
  create DLQ metadata without driving a full poison-message flow.

### 6. Topology Page Needed Escaped Browser Rendering

Status: Addressed

Severity: Medium

Current behavior:

The topology API returns node ids, topics, groups, owners, and followers. The
page rendered parts of that data through template strings assigned to
`innerHTML`.

Impact:

Those values are normally internal or validated, but topics and groups ultimately
come from operator/client input. The page should not rely on upstream validation
to keep browser rendering safe.

Resolution:

The topology page now escapes values rendered through HTML strings. SVG labels
were already assigned via `textContent`.

Coverage:

- Topology API test covers coordination and consensus blocks.
- Topology page render test locks in the escaping helper and escaped assignment
  rendering path.

### 7. Coordination Membership Operations Are Deliberately Narrow

Status: Addressed

The admin surface exposes add/remove voting member operations behind a
coordination-membership manager. In standalone mode or when the manager is not
installed, routes return a clear `coordination_membership_unavailable` error.
Invalid addresses are rejected before reaching the manager.

Coverage:

- Unavailable manager test.
- Invalid address test.
- Add/remove manager routing test.
- `scripts/cluster-tryout.sh --dynamic-membership` exercises this against live
  clustered processes.

## Current Recommendation

The admin operations surface is usable for the current clustered prototype. The
main remaining behavior issue is message inspection status filtering after page
read. That should either become a bounded admin-side scan-fill operation, or be
made explicit in the response/UI as post-page filtering.
