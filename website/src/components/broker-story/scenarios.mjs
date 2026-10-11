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
          "The payload-bearing queue log must be durable and applied before reporting progress.",
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
      badge: "Experimental · opt-in setting",
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
      // Hidden until the prototype lands. A page can still list it explicitly.
      hidden: true,
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
      hidden: true,
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
  variants: [
    { id: "agreement", label: "Checkpoint agreement" },
    { id: "retry", label: "Missing replica and retry" },
    { id: "conflict", label: "Conflicting evidence" },
  ],
};
/** @param {string} [variant]
 * @returns {import("./types").Step[]} */
export function checkpointSteps(variant = "agreement") {
  const agreement = [
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
      { history: { accepted: true, suffix: true, verified: false, growing: true } },
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
  const pending = { accepted: false, suffix: false, verified: false };
  if (variant === "conflict") return [
    agreement[0],
    step("02 / Evidence does not match",
      "C returns evidence that does not match the proposed applied cut. The new checkpoint cannot be accepted. A receipt from B cannot substitute for C's required verification.",
      ["Owner · proposal blocked", "Matching receipt", "Evidence mismatch"],
      [flow("b", "a", "matching receipt", "ack"), flow("c", "a", "mismatched evidence", "failure")],
      { blocked: true, history: { ...pending, status: "conflict" } }),
    step("03 / Preserve the previous recovery base",
      "The previous accepted checkpoint, if any, remains in force. Repeating the same contradictory evidence cannot make this proposal valid. Investigate the mismatch or repair the replica through an authorized path. This failed checkpoint attempt does not itself activate a replacement or imply that ordinary queue traffic has stopped.",
      ["Do not accept proposal", "Preserve old base", "Investigate mismatch"], [],
      { blocked: true, history: { ...pending, status: "conflict" } }),
  ];
  if (variant === "retry") return [
    agreement[0],
    step("02 / A required replica is unavailable",
      "B has returned a durable receipt, but C is unavailable. The proposal waits without becoming the accepted base. Existing replication and queue traffic can continue when their own durability requirements are satisfied.",
      ["Owner · awaiting C", "Receipt ready", "Unavailable"],
      [flow("b", "a", "one receipt is insufficient", "ack")],
      { dead: ["c"], history: { ...pending, status: "waiting" } }),
    step("03 / Retry with fresh verification",
      "C returns. A later attempt obtains verification for the exact proposed history, cut and current assignment. Old receipts are not assumed valid across changed identity or authority. Every required participant must pass before consensus can accept a checkpoint.",
      ["Owner · retry proposal", "Verify current attempt", "Returned · verify"],
      [flow("a", "b", "verify attempt", "control"), flow("a", "c", "verify attempt", "control")],
      { history: pending }),
    ...agreement.slice(1),
  ].map((frame, i) => ({ ...frame, title: `${String(i + 1).padStart(2, "0")} / ${frame.title.split(" / ")[1]}` }));
  return agreement;
}
/** @type {import("./types").RebalanceScene} */
export const rebalance = {
  title: "A new broker joins. A copy moves without a fence.",
  badge: "Proposed · not implemented",
  layout: "rebalance",
  note: "Proposed design, not implemented. One queue partition, replication factor 3 with a majority policy, so W = 2 counted copies including the owner. Position 500 stands for the switch boundary. Load bars and step lengths are illustrative.",
  nodes: ["Broker A", "Broker B", "Broker C", "Broker D"],
  variants: [
    { id: "grow", label: "Move a follower to D" },
    { id: "abort", label: "Recovery cancels the move" },
    { id: "handoff", label: "Planned owner handoff" },
  ],
};
/**
 * Every rebalance step is proposed work. The flag drives the on-stage label.
 * @param {string} title
 * @param {string} text
 * @param {string[]} roles
 * @param {import('./types').Flow[]} flows
 * @param {Partial<import('./types').MoveState>} state
 * @param {import('./types').StepExtras} extra
 * @returns {import('./types').Step}
 */
const proposed = (title, text, roles, flows, state, extra = {}) =>
  step(title, text, roles, flows, {
    proposed: true,
    move: {
      counting: ["a", "b", "c"],
      learners: [],
      leaving: [],
      eligible: ["a", "b", "c"],
      removed: [],
      load: [0.85, 0.75, 0.75, 0.05],
      ...state,
    },
    ...extra,
  });
/** @param {string} [variant]
 * @returns {import("./types").Step[]} */
export function rebalanceSteps(variant = "grow") {
  const joined = proposed(
    "01 / A new broker joins empty",
    "D registers with the cluster and holds no partitions. The load score adds up owned and followed partitions, retained history, traffic and transfers in progress. A, B and C sit above the cluster mean. D sits far below it.",
    ["Owner · epoch 8", "Follower", "Follower", "New · empty"],
    [
      flow("client", "a", "publish"),
      flow("a", "b", "durable records"),
      flow("a", "c", "durable records"),
    ],
    {},
  );
  const learner = proposed(
    "02 / Add D as a learner",
    "One guarded coordination write adds D to the learners list. It needs no pending recovery, a matching assignment and room in the sync budget. W stays 2 because it comes from the configured replication factor, not from the number of assigned copies. The queue keeps serving.",
    ["Owner · epoch 8", "Follower", "Follower", "Learner · not counted"],
    [
      flow("b", "a", "durable progress", "ack"),
      flow("a", "client", "confirm: A + B or C", "ack"),
      flow("a", "d", "learner assigned", "control"),
    ],
    { learners: ["d"] },
  );
  const catchUp = proposed(
    "03 / D catches up in the background",
    "D copies a checkpoint if the owner has truncated, then follows the log tail. The sync budget caps transfers per node and per cluster, and recovery always comes first. Publishes keep confirming from A plus B or C. D's progress is not confirm evidence.",
    ["Owner · serving", "Follower", "Follower", "Learner · catching up"],
    [
      flow("a", "d", "checkpoint + log tail"),
      flow("client", "a", "publish"),
      flow("c", "a", "durable progress", "ack"),
    ],
    { learners: ["d"], sync: true, load: [0.85, 0.75, 0.75, 0.3] },
  );
  if (variant === "abort")
    return [
      joined,
      learner,
      catchUp,
      proposed(
        "04 / A fails while D is still a learner",
        "A stops responding before D is admitted. The controller opens a pending recovery for this partition. A pending recovery cancels any move in progress and pauses rebalancing.",
        ["Offline", "Follower", "Follower", "Learner · not counted"],
        [flow("c", "a", "verify contact", "failure")],
        {
          learners: ["d"],
          recovery: "pending",
          load: [0.85, 0.75, 0.75, 0.3],
        },
        { dead: ["a"] },
      ),
      proposed(
        "05 / The move is cancelled",
        "D is dropped from the learners here. It could also stay a learner. Either way it never counted toward a confirm, so the confirm contract never changed. No copy is removed. The queue is fenced, as in any failover, until recovery activates a replacement.",
        ["Offline", "Candidate · fenced", "Follower", "Move cancelled"],
        [flow("c", "b", "proposed epoch 9", "control")],
        { recovery: "pending", load: [0.85, 0.75, 0.75, 0.1] },
        { dead: ["a"] },
      ),
      proposed(
        "06 / Recovery uses the eligible set",
        "B and C seal their history and provide evidence. D was never admitted, so it is not in the eligible set and recovery does not wait for it. Installation, quorum receipts, consensus activation and local admission still have to succeed before B serves.",
        ["Offline", "Install · fenced", "Sealed evidence", "Not assigned"],
        [
          flow("b", "c", "seal / inspect", "control"),
          flow("c", "b", "history evidence", "control"),
        ],
        { recovery: "pending", load: [0.85, 0.75, 0.75, 0.1] },
        { dead: ["a"] },
      ),
      proposed(
        "07 / B activates at epoch 9",
        "Consensus commits the replacement and B serves under epoch 9. Clients refresh topology and reattach. Rebalancing stayed paused the whole time the recovery was pending.",
        ["Offline", "Owner · epoch 9", "Follower", "Not assigned"],
        [flow("client", "b", "publish"), flow("b", "c", "durable records")],
        {
          counting: ["b", "c"],
          eligible: ["b", "c"],
          load: [0.1, 0.9, 0.75, 0.1],
        },
        { dead: ["a"] },
      ),
      proposed(
        "08 / The move retries after activation",
        "With no recovery pending, the planner adds D as a learner again under epoch 9 and runs the same steps. The same mechanism can also replace the copy lost with A. Which copy leaves is chosen again from fresh load scores.",
        ["Offline", "Owner · epoch 9", "Follower", "Learner · not counted"],
        [
          flow("b", "d", "learner catch-up"),
          flow("client", "b", "publish"),
          flow("c", "b", "durable progress", "ack"),
        ],
        {
          counting: ["b", "c"],
          eligible: ["b", "c"],
          learners: ["d"],
          sync: true,
          load: [0.1, 0.9, 0.75, 0.3],
        },
        { dead: ["a"] },
      ),
    ];
  /** @type {import('./types').NodeId[]} */
  const swapped = ["a", "b", "d"];
  /** @type {import('./types').NodeId[]} */
  const all = ["a", "b", "c", "d"];
  if (variant === "handoff") {
    const after = { counting: swapped, eligible: swapped };
    return [
      proposed(
        "01 / The owner carries the most load",
        "After the follower move, A still owns the partition and serves its traffic. D is an admitted, caught-up follower. Owners move only by a planned handoff to a replica like that.",
        ["Owner · epoch 8", "Follower", "Other partitions", "Follower · admitted"],
        [
          flow("client", "a", "publish"),
          flow("a", "b", "durable records"),
          flow("a", "d", "durable records"),
        ],
        { ...after, load: [0.85, 0.7, 0.5, 0.5] },
      ),
      proposed(
        "02 / Wait for a quiet point and close admission",
        "At a quiet point A stops accepting state-changing work for this partition. New publishes wait. W, the counted copies and the eligible set stay the same.",
        ["Owner · admission closed", "Follower", "Other partitions", "Follower · admitted"],
        [flow("client", "a", "publish waits", "delivery")],
        { ...after, admission: "closed", load: [0.85, 0.7, 0.5, 0.5] },
      ),
      proposed(
        "03 / Drain and report a final boundary",
        "A drains work already in flight and reports its final boundary. B and D report that they are durable at it. A failure before the next write cancels the handoff and A reopens admission.",
        ["Owner · draining", "Follower", "Other partitions", "Follower · admitted"],
        [
          flow("a", "d", "final records"),
          flow("b", "a", "durable at final", "ack"),
          flow("d", "a", "durable at final", "ack"),
        ],
        {
          ...after,
          admission: "closed",
          boundary: "final",
          load: [0.85, 0.7, 0.5, 0.5],
        },
      ),
      proposed(
        "04 / Commit D as owner at epoch 9",
        "One guarded write commits D as owner at epoch 9 with a continuation certificate. The final boundary is known and every counted copy holds it, so D activates without seal or inspection. A failure after this write falls back to ordinary recovery.",
        ["Handing off", "Follower", "Other partitions", "Owner · epoch 9"],
        [flow("a", "d", "continuation certificate", "control")],
        {
          ...after,
          admission: "closed",
          boundary: "final",
          load: [0.75, 0.7, 0.5, 0.6],
        },
      ),
      proposed(
        "05 / Clients follow the new owner",
        "D opens admission and serves. Clients refresh topology and reattach to D. A stays on as a follower, so the replica count and W never changed. The owner work now sits on D.",
        ["Follower", "Follower", "Other partitions", "Owner · serving"],
        [
          flow("client", "d", "publish"),
          flow("d", "a", "durable records"),
          flow("d", "b", "durable records"),
        ],
        { ...after, load: [0.6, 0.65, 0.5, 0.65] },
      ),
    ];
  }
  return [
    joined,
    learner,
    catchUp,
    proposed(
      "04 / D is admitted to the history",
      "D has caught up. The existing admission path adds D's process and storage instance to the accepted history. Owner, epoch and W do not change, and D still does not count. The eligible set grows to four, so a recovery would need more seals. That is the safe direction.",
      ["Owner · epoch 8", "Follower", "Follower", "Learner · admitted"],
      [
        flow("a", "d", "admitted", "control"),
        flow("a", "b", "durable records"),
        flow("a", "c", "durable records"),
      ],
      { learners: ["d"], eligible: all, load: [0.85, 0.75, 0.75, 0.4] },
    ),
    proposed(
      "05 / Swap who counts",
      "One guarded write moves D into the followers and marks C as leaving. From here A counts confirms only from A, B and D. A records the switch boundary, its confirmed tail at that moment, here position 500. Everything below 500 was confirmed on 2 copies of the old set, at most one of them on C. C keeps replicating and stays eligible.",
      ["Owner · counts A, B, D", "Follower", "Leaving · not counted", "Follower · counted"],
      [
        flow("a", "d", "durable records"),
        flow("a", "c", "still replicating"),
        flow("d", "a", "durable progress", "ack"),
      ],
      {
        counting: swapped,
        leaving: ["c"],
        eligible: all,
        boundary: "recorded",
        load: [0.85, 0.75, 0.7, 0.55],
      },
    ),
    proposed(
      "06 / Removal waits for D to cover 500",
      "D is durable only up to 470. A message below 500 might be confirmed only on A and C. Remove C before D holds it, lose A, and that message is gone. C stays assigned and eligible until D is durable at or beyond 500.",
      ["Owner · counts A, B, D", "Follower", "Leaving · kept", "Follower · durable 470"],
      [
        flow("a", "d", "records 470 to 500"),
        flow("a", "c", "still replicating"),
        flow("d", "a", "durable 470", "ack"),
      ],
      {
        counting: swapped,
        leaving: ["c"],
        eligible: all,
        boundary: "waiting",
        load: [0.85, 0.75, 0.7, 0.55],
      },
    ),
    proposed(
      "07 / D covers the boundary",
      "D reports durable progress at 500 or beyond. Every message confirmed before the swap now sits on at least 2 copies without C: one surviving old copy plus D. Every later message was counted only from A, B and D.",
      ["Owner · counts A, B, D", "Follower", "Leaving · kept", "Follower · durable 500"],
      [flow("d", "a", "durable ≥ 500", "ack")],
      {
        counting: swapped,
        leaving: ["c"],
        eligible: all,
        boundary: "covered",
        load: [0.85, 0.75, 0.7, 0.55],
      },
    ),
    proposed(
      "08 / Remove C, keep its data for a while",
      "One guarded write removes C from the assignment and the eligible set. It needs no pending recovery and no other admission or removal in flight. C stops following but keeps its retained copy for a grace period, so an abandoned move, a recovery that starts right after, or a later move back can still use it.",
      ["Owner · epoch 8", "Follower", "Removed · data kept", "Follower"],
      [
        flow("client", "a", "publish"),
        flow("a", "b", "durable records"),
        flow("a", "d", "durable records"),
      ],
      {
        counting: swapped,
        eligible: swapped,
        removed: ["c"],
        boundary: "covered",
        load: [0.85, 0.75, 0.45, 0.55],
      },
    ),
    proposed(
      "09 / Repeat until the load evens out",
      "The planner repeats follower moves under the sync budget, then hands off some owners. A move must improve the spread by a margin. Cooldowns stop a node that just received partitions from being drained again. The cluster settles near the mean.",
      ["Owner · epoch 8", "Follower", "Other partitions", "Follower"],
      [
        flow("client", "a", "publish"),
        flow("a", "b", "durable records"),
        flow("a", "d", "durable records"),
      ],
      {
        counting: swapped,
        eligible: swapped,
        removed: ["c"],
        load: [0.62, 0.6, 0.58, 0.56],
      },
    ),
  ];
}
export const scenes = { failover, delivery, placement, checkpoint, rebalance };
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
        ? checkpointSteps(variant)
        : scene === "rebalance"
          ? rebalanceSteps(variant)
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
