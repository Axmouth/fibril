"""Confirmed-publish failover retry state and backoff.

Mirrors the Rust client (``crates/client/src/failover.rs``): a transient
transport failure during an owner failover is retried against a refreshed owner,
bounded by a deadline, with jittered exponential backoff so many publishers do
not resynchronize into a retry storm.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Optional

PUBLISH_RETRY_INITIAL_BACKOFF_MS = 10
PUBLISH_RETRY_MAX_BACKOFF_MS = 500


def publish_retry_nap_ms(base_ms: int) -> int:
    """One backoff nap: base plus up to base of jitter, in milliseconds."""
    span = max(base_ms, 1)
    return base_ms + random.randint(0, span - 1)


@dataclass
class PublishRetryState:
    """Per-call state for the confirmed-publish failover retry loop."""

    # Give-up time (monotonic ms) for transient retries. None disables retry so
    # the first transient error fails fast.
    deadline_ms: Optional[float]
    redirects: int = 0
    backoff_ms: int = PUBLISH_RETRY_INITIAL_BACKOFF_MS


def now_ms() -> float:
    return asyncio.get_running_loop().time() * 1000


def new_publish_retry_state(publish_timeout_ms: int) -> PublishRetryState:
    return PublishRetryState(
        deadline_ms=(now_ms() + publish_timeout_ms) if publish_timeout_ms > 0 else None,
        redirects=0,
        backoff_ms=PUBLISH_RETRY_INITIAL_BACKOFF_MS,
    )


def bump_backoff(state: PublishRetryState) -> None:
    """Advance backoff after one transient retry."""
    state.backoff_ms = min(state.backoff_ms * 2, PUBLISH_RETRY_MAX_BACKOFF_MS)


async def sleep_ms(ms: float) -> None:
    await asyncio.sleep(max(ms, 0) / 1000)
