import { test } from "node:test";
import assert from "node:assert/strict";

import {
  PUBLISH_RETRY_INITIAL_BACKOFF_MS,
  PUBLISH_RETRY_MAX_BACKOFF_MS,
  bumpBackoff,
  newPublishRetryState,
  publishRetryNap,
} from "../src/internal/retry.js";

test("retry state deadline tracks the publish timeout", () => {
  const disabled = newPublishRetryState(0);
  assert.equal(disabled.deadline, null);
  assert.equal(disabled.redirects, 0);
  assert.equal(disabled.backoffMs, PUBLISH_RETRY_INITIAL_BACKOFF_MS);

  const before = Date.now();
  const enabled = newPublishRetryState(5_000);
  assert.ok(enabled.deadline !== null);
  assert.ok(enabled.deadline! >= before + 5_000);
});

test("backoff doubles and saturates at the cap", () => {
  const state = newPublishRetryState(1_000);
  const seen: number[] = [state.backoffMs];
  for (let i = 0; i < 10; i += 1) {
    bumpBackoff(state);
    seen.push(state.backoffMs);
  }
  assert.equal(seen[1], 20);
  assert.equal(seen[2], 40);
  assert.equal(seen.at(-1), PUBLISH_RETRY_MAX_BACKOFF_MS);
  assert.ok(seen.every((ms) => ms <= PUBLISH_RETRY_MAX_BACKOFF_MS));
});

test("nap stays within base and double base", () => {
  for (const base of [1, 10, 100, 500]) {
    for (let i = 0; i < 50; i += 1) {
      const nap = publishRetryNap(base);
      assert.ok(nap >= base, `nap ${nap} below base ${base}`);
      assert.ok(nap < base * 2, `nap ${nap} not below 2x base ${base}`);
    }
  }
});