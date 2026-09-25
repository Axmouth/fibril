import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import {
  model,
  failoverSteps,
  checkpointSteps,
  assignments,
  delivery,
  scenes,
} from "../src/components/broker-story/scenarios.mjs";

test("the replacement stays fenced through verification and installation; old owner returns as learner", () => {
  const steps = failoverSteps("recover");
  for (let i = 1; i < 6; i++) {
    assert(steps[i].dead.includes("a"));
    assert(!steps[i].roles[1].includes("Owner"));
    if (i > 1)
      assert(
        steps[i].roles[1].includes("fenced") ||
          steps[i].title.includes("boundary"),
      );
  }
  assert(steps[6].roles[1].includes("Owner"));
  assert(steps[8].roles[0].includes("Learner"));
  assert(steps[9].roles[0].includes("Follower"));
  const blocked = failoverSteps("fenced").at(-1);
  assert(blocked.blocked);
  assert(blocked.roles[1].includes("fenced"));
  assert(!blocked.roles.some((r) => r.includes("Owner")));
});

test("durable modes gate delivery and confirm on both persistence windows and the progress response", () => {
  for (const id of ["durable", "early", "replicated-speculation"]) {
    const v = delivery.variants.find((v) => v.id === id);
    const durable = Math.max(
      ...v.segments.filter((s) => s.lane < 2).map((s) => s.end),
    );
    const progress = v.events.find((e) => e.label === "durable progress");
    const confirm = v.events.find((e) => e.label === "publish confirm");
    assert(progress.start >= durable);
    assert(confirm.start >= progress.end);
    if (id !== "replicated-speculation")
      assert(
        v.events.find((e) => e.to === "d" && e.kind === "delivery").start >=
          progress.end,
      );
  }
  const early = delivery.variants.find((v) => v.id === "early").segments;
  assert(
    early[1].start < early[0].end,
    "early replica persistence overlaps local persistence",
  );
});

test("local speculation excludes replica work and identifies the ACK-path contract", () => {
  const v = delivery.variants.find((v) => v.id === "speculative");
  assert(v.badge.includes("no replication"));
  assert(!v.events.some((e) => e.from === "c" || e.to === "c"));
  assert(!v.segments.some((s) => s.lane === 1));
  assert(
    v.events.find((e) => e.label === "publish confirm").start >=
      v.events.find((e) => e.label === "processing ACK").end,
  );
  assert(model("delivery", "speculative", 6).inactive.includes("c"));
  assert(
    model("delivery", "replicated-speculation", 5).badge.includes("Proposed"),
  );
});

test("scrubbing is deterministic and bounded across modes, including final and negative positions", () => {
  for (const [scene, config] of Object.entries(scenes))
    for (const variant of config.variants) {
      for (const time of [-1, 0, 0.5, 3, 7, 12, 30, 1000]) {
        const a = model(scene, variant.id, time),
          b = model(scene, variant.id, time);
        assert.deepEqual(a, b);
        assert(a.index >= 0 && a.index < a.count);
        assert(a.progress >= 0 && a.progress <= 1);
        for (const e of [...(a.events || []), ...(a.segments || [])])
          assert(e.progress >= 0 && e.progress <= 1);
      }
    }
});

test("each partition has exactly one owner and two distinct followers", () => {
  for (const q of assignments) {
    assert.equal(q.followers.length, 2);
    assert.equal(new Set([q.owner, ...q.followers]).size, 3);
  }
  assert.equal(new Set(assignments.map((q) => q.owner)).size, 3);
});

test("published sprite frames are exact build copies of the shared dashboard artwork", async () => {
  for (const frame of ["open-a", "open-b", "open", "half", "closed", "dead"]) {
    const name = `ring-${frame}-128.png`;
    assert.deepEqual(
      await readFile(
        new URL(`../public/brand/sprites/${name}`, import.meta.url),
      ),
      await readFile(
        new URL(
          `../../crates/admin/admin-ui/img/sprites/${name}`,
          import.meta.url,
        ),
      ),
    );
  }
});


test("checkpoint agreement never skips live payload verification or grants ownership", () => {
  const steps = checkpointSteps();
  assert.equal(steps[0].history.accepted, false);
  assert.equal(steps[1].history.accepted, false);
  assert.equal(steps[2].history.accepted, true);
  assert.equal(steps[3].history.suffix, true);
  assert.equal(steps[4].history.verified, false);
  assert.equal(steps[5].history.verified, true);
  for (const step of steps.slice(4)) {
    assert(step.dead.includes("a"));
    assert(step.roles[1].includes("fenced"));
  }
  assert.match(steps[5].text, /large live backlog still requires reads/);
});


test("a missing checkpoint participant delays agreement until all verify on retry", () => {
  const steps = checkpointSteps("retry");
  assert(steps[1].dead.includes("c"));
  assert.equal(steps[1].history.status, "waiting");
  const accepted = steps.findIndex(s => s.history.accepted);
  assert(accepted > steps.findIndex(s => s.title.includes("Every admitted replica")));
  assert(steps.slice(0, accepted).every(s => !s.history.accepted));
  assert(!steps[2].dead?.includes("c"));
  assert(steps.at(-1).history.verified);
});

test("conflicting checkpoint evidence cannot become an accepted base or authorize serving", () => {
  const steps = checkpointSteps("conflict");
  assert(steps.every(s => !s.history.accepted && !s.history.verified));
  assert(steps.at(-1).blocked);
  assert.equal(steps.at(-1).history.status, "conflict");
  assert.match(steps.at(-1).text, /previous accepted checkpoint/);
  assert.equal(model("checkpoint", "conflict", 1000).history.accepted, false);
  assert.equal(model("checkpoint", "retry", 1000).history.accepted, true);
});
