import { test } from "node:test";
import assert from "node:assert/strict";

import { _TopicGlobForTests as TopicGlob } from "../src/routing.js";

test("topic glob matches prefix, suffix, and middle wildcards", () => {
  assert.ok(new TopicGlob("events.*").matches("events.click"));
  assert.ok(new TopicGlob("events.*").matches("events."));
  assert.ok(!new TopicGlob("events.*").matches("orders.new"));

  assert.ok(new TopicGlob("*.dead").matches("orders.dead"));
  assert.ok(!new TopicGlob("*.dead").matches("orders.live"));

  assert.ok(new TopicGlob("a*c").matches("abc"));
  assert.ok(new TopicGlob("a*c").matches("ac"));
  assert.ok(!new TopicGlob("a*c").matches("ab"));

  // No wildcard is an exact match; "*" matches everything.
  assert.ok(new TopicGlob("orders").matches("orders"));
  assert.ok(!new TopicGlob("orders").matches("orders.new"));
  assert.ok(new TopicGlob("*").matches("anything.at.all"));
});
