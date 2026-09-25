// @ts-check
// Educational choreography, not measured time. Each scene explicitly states its
// authority and confirmation boundary. The renderer only visualizes this model.
/** @param {number} n */
export const clamp = (n) => Math.max(0, Math.min(1, n));
/**
 * @param {string} title
 * @param {string} text
 * @param {string[]} roles
 * @param {import('./types').Flow[]} flows
 * @param {import('./types').StepExtras} extra
 * @returns {import('./types').Step}
 */
const step = (title, text, roles, flows = [], extra = {}) => ({
  title,
  text,
  roles,
  flows,
  ...extra,
});
/**
 * @param {import('./types').Endpoint} from
 * @param {import('./types').Endpoint} to
 * @param {string} label
 * @param {import('./types').Ink} kind
 * @returns {import('./types').Flow}
 */
const flow = (from, to, label, kind = "data") => ({ from, to, label, kind });
const stable = ["Owner · epoch 8", "Follower", "Follower · controller"];
/** @type {import("./types").FailoverScene} */
export const failover = {
  title: "An owner falls. The queue finds its way.",
  badge: "Implemented · experimental clustering",
  note: "One enrolled queue, three replicas, majority-durable confirms. The metadata leader survives. Steps are explanatory. Their duration is not a latency prediction.",
  nodes: ["Broker A", "Broker B", "Broker C"],
  layout: "cluster",
  variants: [
    { id: "recover", label: "Recover and rejoin" },
    { id: "fenced", label: "Evidence unavailable" },
  ],
};
/** @param {string} variant @returns {import("./types").Step[]} */
export function failoverSteps(variant) {
  const prefix = [
    step(
      "01 / Every copy has a role",
      "Clients use A for this queue. Followers pull its durable payload and enqueue-event history. C also hosts the metadata controller: a different role from queue ownership.",
      stable,
      [
        flow("client", "a", "publish"),
        flow("a", "b", "durable records"),
        flow("a", "c", "durable records"),
      ],
    ),
    step(
      "02 / A stops responding",
      "A stops moving. The client loses its connection, but B has not become the owner yet. A transport failure is evidence to investigate. It is not permission to serve.",
      ["Offline", "Follower", "Follower · controller"],
      [],
      { dead: ["a"] },
    ),
    step(
      "03 / The controller proposes a replacement",
      "With eager detection enabled, explicit connection loss and failed reconnect verification can start placement before heartbeat expiry. Silent failures still use expiry. The new assignment remains fenced.",
      ["Offline", "Candidate · fenced", "Follower · controller"],
      [
        flow("c", "a", "verify contact", "failure"),
        flow("c", "b", "proposed epoch 9", "control"),
      ],
      { dead: ["a"] },
    ),
    step(
      "04 / Freeze the evidence",
      "Surviving replicas authorize the exact transition and seal their history. Each source can be inspected after its own seal completes. The worker collects the required evidence before selecting a source.",
      ["Offline", "Sealed · fenced", "Sealed · controller"],
      [
        flow("b", "c", "seal / inspect", "control"),
        flow("c", "b", "history evidence", "control"),
      ],
      { dead: ["a"] },
    ),
  ];
  if (variant === "fenced")
    return [
      ...prefix,
      step(
        "05 / Evidence is insufficient",
        "A is down and C cannot provide the required evidence. B stays fenced. The worker can retry transient failures. Contradictory authoritative history needs investigation. There is no unsafe promotion to keep the picture moving.",
        ["Offline", "Waiting · fenced", "Evidence unavailable"],
        [flow("b", "c", "retry evidence", "failure")],
        { dead: ["a"], blocked: true },
      ),
    ];
  return [
    ...prefix,
    step(
      "05 / Find an agreed boundary",
      "An accepted checkpoint can provide a verified base. Recovery compares the required suffix and validates live payloads. A large live backlog still costs work. The diagram does not hide that behind a checkpoint.",
      ["Offline", "Verify base + suffix", "Verify live payloads"],
      [flow("c", "b", "verified history", "control")],
      { dead: ["a"], checkpoint: true },
    ),
    step(
      "06 / Prepare an installed quorum",
      "Compatible retained bytes can be reused after verification. Missing data is copied, state is installed durably, and exact process/storage receipts are collected. B is still fenced.",
      ["Offline", "Install · fenced", "Installed replica"],
      [flow("c", "b", "missing suffix, if needed")],
      { dead: ["a"], checkpoint: true },
    ),
    step(
      "07 / Activate the exact assignment",
      "Consensus commits the validated replacement. The required quorum and process/storage identities bind this activation. Local admission enables B to serve under epoch 9.",
      ["Offline", "Owner · epoch 9", "Follower · controller"],
      [flow("c", "b", "activation + admission", "control")],
      { dead: ["a"] },
    ),
    step(
      "08 / Clients find the new owner",
      "The client refreshes topology and reattaches to B. Delivery and new confirmations resume. Failure detection, worker recovery and client reconnection are separate parts of the outage.",
      ["Offline", "Owner · serving", "Follower · controller"],
      [
        flow("client", "b", "reconnect / publish"),
        flow("b", "client", "delivery / confirms", "ack"),
      ],
      { dead: ["a"] },
    ),
    step(
      "09 / A returns as a learner",
      "A comes back with its old history. It cannot reclaim ownership. It catches up in the background while B continues serving. Learner progress does not count as admitted quorum evidence.",
      ["Learner · catching up", "Owner · serving", "Follower · controller"],
      [
        flow("b", "a", "background catch-up"),
        flow("client", "b", "ordinary traffic"),
      ],
    ),
    step(
      "10 / A earns its place again",
      "After verified catch-up and admission, A is a follower. B remains the owner. The recovered topology preserves the confirmation contract rather than silently reducing the required copies.",
      ["Follower · admitted", "Owner · epoch 9", "Follower · controller"],
      [
        flow("b", "a", "durable records"),
        flow("b", "c", "durable records"),
        flow("client", "b", "publish"),
      ],
    ),
  ];
}
const lanes = [
  "Owner: payload + enqueue event",
  "Follower: payload + enqueue event",
  "Delivery to worker",
  "Publisher confirmation",
];
/**
 * @param {import('./types').Segment['lane']} lane
 * @param {number} start
 * @param {number} end
 * @param {string} label
 * @param {import('./types').Ink} kind
 * @returns {import('./types').Segment}
 */
const segment = (lane, start, end, label, kind = "data") => ({
  lane,
  start,
  end,
  label,
  kind,
});
/** @type {import("./types").DeliveryScene} */
export const delivery = {
  title: "Follow one message. Watch the waits overlap.",
  badge: "Current main",
  layout: "delivery",
  nodes: ["Publisher", "Owner", "Follower", "Worker"],
  note: "Illustrative time units, not benchmark results. The replicated examples require owner + one durable follower. Payload and enqueue-event dependencies both matter. Polling/wakeup and batching delays are simplified.",
  variants: [
    {
      id: "durable",
      label: "Durable replication",
      badge: "Current main",
      text: "The follower reads durable owner history. Delivery and publisher confirmation wait for the required durable replica progress. Worker ACK is separate.",
      segments: [
        segment(0, 1, 4, "append + fsync"),
        segment(1, 5, 8, "append + fsync"),
        segment(2, 9, 10, "deliver", "delivery"),
        segment(3, 9, 10, "confirm", "ack"),
      ],
      events: [
        { start: 0, end: 1, ...flow("a", "b", "publish") },
        { start: 4, end: 5, ...flow("b", "c", "follower pull") },
        { start: 8, end: 9, ...flow("c", "b", "durable progress", "ack") },
        { start: 9, end: 10, ...flow("b", "d", "delivery", "delivery") },
        { start: 9, end: 10, ...flow("b", "a", "publish confirm", "ack") },
        { start: 11, end: 12, ...flow("d", "b", "worker ACK", "ack") },
      ],
      milestones: [
        [
          0,
          "A publish enters the owner",
          "The owner must preserve both the payload and its enqueue event.",
        ],
        [
          1,
          "Local persistence is in progress",
          "Appending and fsyncing establish the owner’s durable boundary.",
        ],
        [
          4,
          "The follower can read the durable batch",
          "This animation shows the data response. The current transport is follower-pull.",
        ],
        [
          5,
          "The follower persists its own copy",
          "Both logs must complete and apply safely before reporting durable progress.",
        ],
        [
          8,
          "Durable progress reaches the owner",
          "The exact payload/enqueue dependency and assignment epoch are checked.",
        ],
        [
          9,
          "The durability requirement is satisfied",
          "Delivery and publisher confirmation can proceed independently.",
        ],
        [
          11,
          "The worker acknowledges processing",
          "Processing ACK and publisher confirmation are different signals.",
        ],
      ],
    },
    {
      id: "early",
      label: "Early replication",
      badge: "Unmerged experiment",
      text: "Expose written batches to followers before owner fsync completes. Local and follower persistence can overlap. The publisher still waits for the required durable evidence.",
      segments: [
        segment(0, 1, 5, "append + fsync"),
        segment(1, 3, 7, "append + fsync"),
        segment(2, 8, 9, "deliver", "delivery"),
        segment(3, 8, 9, "confirm", "ack"),
      ],
      events: [
        { start: 0, end: 1, ...flow("a", "b", "publish") },
        { start: 2, end: 3, ...flow("b", "c", "written batch") },
        { start: 7, end: 8, ...flow("c", "b", "durable progress", "ack") },
        { start: 8, end: 9, ...flow("b", "d", "delivery", "delivery") },
        { start: 8, end: 9, ...flow("b", "a", "publish confirm", "ack") },
        { start: 10, end: 11, ...flow("d", "b", "worker ACK", "ack") },
      ],
      milestones: [
        [
          0,
          "A publish enters the owner",
          "The durable confirmation contract stays the same.",
        ],
        [1, "Local I/O starts", "The owner’s fsync is still pending."],
        [
          2,
          "A written batch becomes readable early",
          "The experimental path can begin replica work before local fsync finishes.",
        ],
        [
          3,
          "Two persistence windows overlap",
          "Overlap can remove serial waiting. Network, batching and progress-report costs remain.",
        ],
        [
          7,
          "The follower has durable evidence",
          "The owner still verifies both dependencies and its own durability.",
        ],
        [
          8,
          "Confirm and deliver after the durable boundary",
          "Early replication changes scheduling, not the required durable copies.",
        ],
        [
          10,
          "The worker sends a processing ACK",
          "This signal does not retroactively replace the durability policy.",
        ],
      ],
    },
    {
      id: "speculative",
      label: "Local speculation",
      badge: "Unmerged prototype · no replication",
      text: "When ordering and receiver-slot checks permit, deliver before fsync. The experimental contract allows confirmation after a processing ACK or required durability. In this example, processing ACK wins the race.",
      segments: [
        segment(0, 1, 8, "append + fsync"),
        segment(2, 2, 3, "early delivery", "delivery"),
        segment(3, 5, 6, "ACK-path confirm", "ack"),
      ],
      events: [
        { start: 0, end: 1, ...flow("a", "b", "publish") },
        {
          start: 2,
          end: 3,
          ...flow("b", "d", "speculative delivery", "delivery"),
        },
        { start: 4, end: 5, ...flow("d", "b", "processing ACK", "ack") },
        { start: 5, end: 6, ...flow("b", "a", "publish confirm", "ack") },
      ],
      milestones: [
        [
          0,
          "A receiver has an open slot",
          "The prototype preserves delivery ordering and has bounded speculative capacity.",
        ],
        [
          1,
          "Local persistence starts",
          "The data has not crossed the fsync boundary yet.",
        ],
        [
          2,
          "The worker gets an early delivery",
          "Internal speculative identity can help identify repeats. It is not exactly-once processing.",
        ],
        [
          4,
          "Processing finishes before fsync",
          "A processing ACK may satisfy the experimental confirmation contract.",
        ],
        [
          5,
          "The publisher receives its confirmation",
          "This path promises processing acknowledgement, not a durable surviving copy.",
        ],
        [
          8,
          "The durability path completes later",
          "Crash/error, fallback, expiry and duplicate cases remain adoption gates.",
        ],
      ],
    },
    {
      id: "replicated-speculation",
      label: "Replicated speculation",
      badge: "Proposed · correctness work pending",
      text: "A proposed path combines early delivery with overlapping replication. This scene deliberately keeps confirmation behind required durability. An ACK-based shortcut needs its own recovery and dependency proof.",
      segments: [
        segment(0, 1, 5, "append + fsync"),
        segment(1, 3, 7, "append + fsync"),
        segment(2, 2, 3, "early delivery", "delivery"),
        segment(3, 8, 9, "durable confirm", "ack"),
      ],
      events: [
        { start: 0, end: 1, ...flow("a", "b", "publish") },
        { start: 2, end: 3, ...flow("b", "c", "written batch") },
        {
          start: 2,
          end: 3,
          ...flow("b", "d", "speculative delivery", "delivery"),
        },
        { start: 4, end: 5, ...flow("d", "b", "processing ACK", "ack") },
        { start: 7, end: 8, ...flow("c", "b", "durable progress", "ack") },
        { start: 8, end: 9, ...flow("b", "a", "publish confirm", "ack") },
      ],
      milestones: [
        [
          0,
          "A possible replicated path",
          "This is design exploration, not available production behavior.",
        ],
        [
          2,
          "Delivery and replica transfer begin early",
          "An open receiver slot and ordering checks remain necessary.",
        ],
        [
          3,
          "Persistence overlaps with processing",
          "Early work increases concurrency without proving a safe confirmation shortcut.",
        ],
        [
          4,
          "An ACK can precede the replica’s enqueue",
          "Failover must preserve ACK/enqueue dependencies, identity and producer outcomes.",
        ],
        [
          7,
          "Durable replica progress arrives",
          "This conservative illustrated path still uses required durability.",
        ],
        [
          8,
          "The publisher can be confirmed",
          "A different ACK-or-durability contract requires separate proof before adoption.",
        ],
      ],
    },
  ],
  lanes,
};
/** @type {import("./types").PlacementScene} */
export const placement = {
  title: "One cluster. Many independent owners.",
  badge: "Implemented · partitioned queues",
  layout: "placement",
  note: "Illustrative assignments: four brokers, three queue partitions, one owner + two followers each. The metadata controller plans placement. Queue ownership is per partition.",
  nodes: ["Broker A", "Broker B", "Broker C", "Broker D"],
  variants: [{ id: "spread", label: "Partition placement" }],
};
/** @type {import("./types").Assignment[]} */
export const assignments = [
  { name: "orders / 0", owner: "a", followers: ["b", "d"], color: "data" },
  { name: "orders / 1", owner: "b", followers: ["a", "c"], color: "delivery" },
  { name: "billing / 0", owner: "c", followers: ["b", "d"], color: "ack" },
];
/** @type {import("./types").NodeId[]} */
const nodeIds = ["a", "b", "c", "d"];
export function placementSteps() {
  return assignments.map((q, i) =>
    step(
      `0${i + 1} / ${q.name}`,
      `Broker ${q.owner.toUpperCase()} owns this partition. ${q.followers.map((x) => x.toUpperCase()).join(" and ")} hold follower copies. Another partition can have a different owner on the same machines. Ordering is per partition. It does not establish a global order across partitions.`,
      nodeIds.map((id) =>
        id === q.owner
          ? "Owner · " + q.name
          : q.followers.includes(id)
            ? "Follower · " + q.name
            : "Other partitions",
      ),
      q.followers.map((to) => flow(q.owner, to, "replica copy", q.color)),
      { focus: i },
    ),
  );
}
/** @type {import("./types").CheckpointScene} */
export const checkpoint = {
  title: "Agree on a base. Verify what comes next.",
  badge: "Implemented · opt-in checkpoints",
  layout: "cluster",
  note: "One queue, three admitted replicas. Event cut 120 is an illustrative exclusive boundary. Event positions and message offsets are different coordinates. This story does not measure time or storage size.",
  nodes: ["Broker A", "Broker B", "Broker C"],
  variants: [{ id: "agreement", label: "Checkpoint agreement" }],
};
/** @returns {import("./types").Step[]} */
export function checkpointSteps() {
  return [
    step(
      "01 / Capture an exact applied cut",
      "A captures queue state after applying events before 120. The snapshot describes ready, inflight, delayed and settled state at that exact boundary. A snapshot file alone is not an accepted recovery checkpoint.",
      ["Owner · cut 120", "Follower", "Follower"],
      [
        flow("a", "b", "checkpoint proposal", "control"),
        flow("a", "c", "checkpoint proposal", "control"),
      ],
      { history: { accepted: false, suffix: false, verified: false } },
    ),
    step(
      "02 / Every admitted replica verifies",
      "B and C reconstruct the same applied cut and verify the required evidence. Each persists its checkpoint and returns a bound receipt. A missing participant delays agreement while ordinary replication continues.",
      ["Collect receipts", "Verify + persist", "Verify + persist"],
      [
        flow("b", "a", "durable receipt", "ack"),
        flow("c", "a", "durable receipt", "ack"),
      ],
      { history: { accepted: false, suffix: false, verified: false } },
    ),
    step(
      "03 / Consensus accepts the checkpoint",
      "Acceptance binds the verified base to the queue history and assignment. The accepted checkpoint stays pinned until a replacement is accepted. The agreed cut covers events before 120.",
      ["Owner · accepted base", "Pinned base", "Pinned base"],
      [
        flow("a", "b", "accepted checkpoint", "control"),
        flow("a", "c", "accepted checkpoint", "control"),
      ],
      { history: { accepted: true, suffix: false, verified: false } },
    ),
    step(
      "04 / New events form a suffix",
      "Publishing, acknowledgements and timer transitions add events from 120 onward. Some messages created before the cut remain live. Their payloads are still needed, regardless of how old the checkpoint is.",
      ["Owner · new traffic", "Replicate suffix", "Replicate suffix"],
      [flow("a", "b", "new records"), flow("a", "c", "new records")],
      { history: { accepted: true, suffix: true, verified: false } },
    ),
    step(
      "05 / The owner disappears",
      "The replacement remains fenced. Recovery obtains sealed evidence from the surviving replicas and checks that the accepted base is available and bound to the relevant history.",
      ["Offline", "Candidate · fenced", "Sealed evidence"],
      [flow("c", "b", "checkpoint evidence", "control")],
      {
        dead: ["a"],
        history: { accepted: true, suffix: true, verified: false },
      },
    ),
    step(
      "06 / Replay the suffix and validate live payloads",
      "Recovery verifies the snapshot, reconstructs the later event suffix and checks required live payload identities and bytes. A large settled history can become cheap. A large live backlog still requires reads. Missing or incompatible evidence keeps the queue fenced.",
      ["Offline", "Verify · fenced", "Verify payloads"],
      [flow("c", "b", "suffix + live payload proof", "control")],
      {
        dead: ["a"],
        history: { accepted: true, suffix: true, verified: true },
      },
    ),
    step(
      "07 / Evidence feeds the normal recovery barriers",
      "The verified base and suffix reduce reconstruction work. They do not grant ownership. Installation, exact quorum receipts, consensus activation and local admission still have to succeed before the replacement serves.",
      ["Offline", "Install · fenced", "Required replica"],
      [flow("b", "c", "prepare installation", "control")],
      {
        dead: ["a"],
        history: { accepted: true, suffix: true, verified: true },
      },
    ),
  ];
}
export const scenes = { failover, delivery, placement, checkpoint };
/**
 * @param {import('./types').SceneId} scene
 * @param {string} variant
 * @param {number} time
 * @returns {import('./types').Frame}
 */
export function model(scene, variant, time) {
  if (scene === "delivery") {
    const v =
      delivery.variants.find((v) => v.id === variant) || delivery.variants[0];
    const t = clamp(time / 12) * 12;
    const milestone =
      [...v.milestones].reverse().find((m) => t >= m[0]) || v.milestones[0];
    return {
      title: milestone[1],
      text: milestone[2],
      roles: [
        "Publish + confirm",
        "Queue owner",
        variant === "speculative" ? "Not used" : "Required replica",
        "Process + ACK",
      ],
      inactive: variant === "speculative" ? ["c"] : [],
      badge: v.badge,
      progress: t / 12,
      events: v.events.map((e) => ({
        ...e,
        progress: clamp((t - e.start) / (e.end - e.start)),
        active: t >= e.start && t <= e.end,
      })),
      segments: v.segments.map((s) => ({
        ...s,
        progress: clamp((t - s.start) / (s.end - s.start)),
      })),
      index: v.milestones.indexOf(milestone),
      count: v.milestones.length,
      duration: 12,
      steps: v.milestones.map((m) => m[0]),
    };
  }
  const steps =
    scene === "placement"
      ? placementSteps()
      : scene === "checkpoint"
        ? checkpointSteps()
        : failoverSteps(variant);
  const i = Math.min(steps.length - 1, Math.floor(Math.max(0, time) / 3));
  return {
    ...steps[i],
    badge: scenes[scene].badge,
    index: i,
    count: steps.length,
    duration: steps.length * 3,
    progress: clamp(time / (steps.length * 3)),
    stepProgress: clamp((time - i * 3) / 2),
    steps: steps.map((_, i) => i * 3),
  };
}
