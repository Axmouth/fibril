import { test } from "node:test";
import assert from "node:assert/strict";

import { durationToMs, deadlineFromDelay } from "../src/publisher.js";

// A bare number stays milliseconds (JS-native); the tagged object forms make the
// unit explicit so a value is never mistaken for the wrong unit.
test("durationToMs resolves every DurationInput form to milliseconds", () => {
  assert.equal(durationToMs(30_000), 30_000);
  assert.equal(durationToMs({ ms: 250 }), 250);
  assert.equal(durationToMs({ seconds: 30 }), 30_000);
  assert.equal(durationToMs({ minutes: 2 }), 120_000);
  // Fractional milliseconds are truncated to whole ms.
  assert.equal(durationToMs({ seconds: 1.5 }), 1500);
});

test("durationToMs rejects negative or non-finite durations", () => {
  assert.throws(() => durationToMs(-1));
  assert.throws(() => durationToMs({ seconds: -1 }));
  assert.throws(() => durationToMs({ ms: Number.POSITIVE_INFINITY }));
});

test("deadlineFromDelay adds a relative duration to now, and passes a Date through", () => {
  const before = Date.now();
  const deadline = Number(deadlineFromDelay({ seconds: 30 }));
  assert.ok(deadline >= before + 30_000);
  assert.ok(deadline <= Date.now() + 30_000);

  const absolute = new Date(Date.now() + 5_000);
  assert.equal(Number(deadlineFromDelay(absolute)), absolute.getTime());
});
