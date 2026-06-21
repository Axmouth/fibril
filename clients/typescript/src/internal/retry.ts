// Confirmed-publish failover retry state and backoff. Mirrors the Rust client
// (crates/client/src/failover.rs): a transient transport failure during an
// owner failover is retried against a refreshed owner, bounded by a deadline,
// with jittered exponential backoff so many publishers do not resynchronize
// into a retry storm.

export const PUBLISH_RETRY_INITIAL_BACKOFF_MS = 10;
export const PUBLISH_RETRY_MAX_BACKOFF_MS = 500;

/** One backoff nap: base plus up to base of jitter. */
export function publishRetryNap(baseMs: number): number {
  const span = Math.max(baseMs, 1);
  return baseMs + Math.floor(Math.random() * span);
}

/** Per-call state for the confirmed-publish failover retry loop. */
export interface PublishRetryState {
  // Give-up time (epoch ms) for transient retries. Null disables retry so the
  // first transient error fails fast.
  deadline: number | null;
  redirects: number;
  backoffMs: number;
}

export function newPublishRetryState(publishTimeoutMs: number): PublishRetryState {
  return {
    deadline: publishTimeoutMs > 0 ? Date.now() + publishTimeoutMs : null,
    redirects: 0,
    backoffMs: PUBLISH_RETRY_INITIAL_BACKOFF_MS,
  };
}

/** Advance backoff after one transient retry. */
export function bumpBackoff(state: PublishRetryState): void {
  state.backoffMs = Math.min(state.backoffMs * 2, PUBLISH_RETRY_MAX_BACKOFF_MS);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
