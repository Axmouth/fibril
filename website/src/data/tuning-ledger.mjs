// Entries for the tuning ledger page. Each summarizes one or more rows of the
// optimization log, which stays the full record with commits and conditions.
// m: [label, unit, before, after, better], where before and after are a
// number or a [low, high] range, and better is "lower" or "higher".
export const outcomes = { kept: "kept", setting: "setting", parked: "not merged", removed: "removed", pending: "pending" };

export const items = [
  { v: "kept", date: "2026-10-25", title: "Zero-ahead for preallocated segments",
    what: "A shared worker writes and syncs zeros a little ahead of each writer, so syncs no longer wait for the filesystem to record new blocks.",
    m: [["owner fsync p50", "ms", 4.9, 0.73, "lower"], ["1k msg/s p50", "ms", [9.4, 9.5], [2.6, 3.6], "lower"], ["1k msg/s CPU per message", "% of before", 100, 135, "lower"]],
    trade: "More and smaller fsyncs raised CPU per message at 1k msg/s by about 35%. Zeroing in 1 MiB chunks pushed p99 to 34–38 ms, so it zeroes in 4 MiB chunks. Its writes share the drive, so mid-size bursts gained about 3 ms of p99.",
    why: "Merged. The low-rate floor was the biggest latency cost left, and the CPU rise is partly won back by loaded fsync spacing." },
  { v: "kept", date: "2026-10-25", title: "Rollover off the writer, truncate off the writer, spare reuse",
    what: "Rollover syncs only the outgoing data and hands the header, directory and index syncs to the next fsync. Truncation runs on the manifest worker, and up to 4 freed segments per log are reused.",
    m: [["10k msg/s p50", "ms", 30, 9, "lower"], ["10k msg/s p99", "ms", [83, 95], [28, 33], "lower"], ["CPU per million messages", "s", 54, 68, "lower"]],
    trade: "Cheaper syncs made fsync groups smaller, about 3.3 times more fsync jobs per message, so CPU per message rose about 25%.",
    why: "Merged. Tail latency fell to a third, and later spacing work targets the CPU." },
  { v: "kept", date: "2026-10-25", title: "Loaded fsync spacing",
    what: "Under load, a new fsync waits up to 2 ms after the previous one so more records share it. Idle logs sync at once. It is the runtime setting storage.loaded_fsync_spacing_ms.",
    m: [["fixed 3 ms floor, 10k msg/s CPU per million", "s", [60, 63], [47, 49], "lower"], ["fixed 3 ms floor, 100 msg/s p50", "ms", [5.8, 7.4], [8.0, 8.1], "lower"]],
    trade: "A fixed floor saved CPU under load but added latency at low rates, as the second chart shows. Applying it only under load keeps the saving and drops that cost.",
    why: "Merged as a runtime setting, 2 ms by default. With it Fibril led RabbitMQ at 100 and 1k msg/s in the same harness." },
  { v: "kept", date: "2026-10-25", title: "Raw replication bytes",
    what: "The owner sends stored records to followers as they are on disk, and the follower decodes once, instead of decoding and re-encoding on the owner.",
    m: [["1 KiB throughput", "% of before", 100, 104.5, "higher"], ["4 KiB CPU per message", "% of before", 100, 92, "lower"]],
    trade: "None measured. It changes the replication frame contents, which both sides of a rolling upgrade must understand.",
    why: "Merged." },
  { v: "kept", date: "2026-10-26", title: "Tail cache on the owner only, 16 MiB",
    what: "Followers stopped filling a read cache they never read from, and the owner's budget dropped to 16 MiB.",
    m: [["follower memory at 10k msg/s", "MiB", [184, 187], [105, 109], "lower"], ["saturation throughput", "k msg/s", [141.9, 142.8], 141.7, "higher"]],
    trade: "A consumer more than 16 MiB behind reads from disk instead of memory. 4 MiB measured the same, and 16 MiB keeps headroom for lagging readers.",
    why: "Merged." },
  { v: "kept", date: "2026-10-26", title: "Smaller writer buffers",
    what: "The default writer buffer factor dropped from 128 to 32, which still leaves 2,048 channel slots per log for bursts.",
    m: [["memory per node, 200 idle queues", "MiB", [940, 959], [744, 774], "lower"]],
    trade: "Factor 8 saved another 80 MiB, at a quarter of the burst headroom. Saturation and p99 did not move between 8 and 128.",
    why: "Merged at 32, a balance between idle memory and bursts." },
  { v: "kept", date: "2026-10-26", title: "Fsync and notify threads on demand",
    what: "Each log starts its fsync and notify threads when it needs them and retires them after 10 s unused.",
    m: [["threads per node, 200 idle queues", "", 644, 240, "lower"], ["10k msg/s p99", "ms", [35.9, 37.7], [35.4, 35.9], "lower"]],
    trade: "None measured in memory, CPU or latency. Reading the fsync loop for this change also found that merged fsync groups skipped a roll's deferred syncs, now fixed.",
    why: "Merged." },
  { v: "kept", date: "2026-10-20", title: "One publisher sink per partition",
    what: "Every publisher handle of a partition shares one sink and confirm loop instead of one per connection.",
    m: [["CPU per million, 16 conns, durable 10k/s", "s", 214, 113, "lower"], ["durable saturation", "k msg/s", 114, 120, "higher"]],
    trade: "Tighter coalescing timers cut buffered 10k msg/s p50 from about 2.5 to 1.4–1.6 ms, for 12–25% more CPU, and plain yields lost 7% durable saturation. Those variants stayed on a branch.",
    why: "Merged with the ordinary Tokio timer." },
  { v: "kept", date: "2026-10-27", title: "mimalloc v2 instead of v3",
    what: "Moved off mimalloc 3.3.2, which has open reports of rare multi-threaded crashes, to the long-stable 2.3.2.",
    m: [["memory per node, 200 idle queues", "MiB", [831, 885], [439, 494], "lower"], ["saturation throughput", "k msg/s", [134.8, 137.8], [131.8, 132.4], "higher"]],
    trade: "About 2–3% less peak throughput and a few ms more p99 at 10k msg/s.",
    why: "Merged for reliability, with a large idle memory gain." },
  { v: "setting", date: "2026-10-24", title: "Publish batching gap mode",
    what: "A batch of publishes closes after 100 µs without a new request, on a microsecond timer.",
    m: [["buffered p50 at 10k msg/s, 16 connections", "ms", 0.99, 0.37, "lower"], ["buffered CPU per message", "% of before", 100, 171, "lower"]],
    trade: "Durable publishes gained nothing and paid up to 31% more CPU at 10k msg/s, with a wider p99 at 50k msg/s.",
    why: "A per-durability setting, on for buffered publishes only." },
  { v: "setting", date: "2026-09-27", title: "Separate follower push credit",
    what: "Gives the follower push stream its own credit window instead of the per-read byte budget.",
    m: [["4 KiB saturation", "% of before", 100, 133, "higher"]],
    trade: "No gain at 1 KiB, and more CPU and memory per message at 4 KiB.",
    why: "An off-by-default runtime setting, to retest on single-log queues." },
  { v: "parked", date: "2026-10-26", title: "Push frames built from smaller reads",
    what: "Fill each owner push frame from 256 KiB or 1 MiB reads with a yield between them, to shorten long task polls.",
    m: [["1 KiB long-poll time per 60 s", "s", 7.6, 1.5, "lower"], ["4 KiB throughput", "k msg/s", [42.4, 42.7], [41.0, 41.1], "higher"]],
    trade: "At 4 KiB the follower waits on the owner, so a slower frame build costs throughput.",
    why: "Not merged. The poll gain did not show up in latency." },
  { v: "parked", date: "2026-10-26", title: "Pipelined push frames",
    what: "Read and encode the next push frame while waiting for credit.",
    m: [["1 KiB throughput", "k msg/s", [141.2, 141.7], [136.7, 139.4], "higher"]],
    trade: "It removed most of the follower's idle gap at 4 KiB without raising 4 KiB throughput, and the read-ahead cost the owner at 1 KiB.",
    why: "Not merged. The 4 KiB limit sits in the credit window and the follower apply." },
  { v: "parked", date: "2026-09-26", title: "Batched follower apply",
    what: "Apply several replicated batches per follower actor step.",
    m: [["split-drive saturation", "% of before", 100, 109.5, "higher"]],
    trade: "Paced latency got worse.",
    why: "Not merged. A throughput gain that taxes every paced workload is not a default." },
  { v: "parked", date: "2026-09-20", title: "Speculative delivery",
    what: "Deliver before durability under an explicit contract, confirming once the message is durable or processed.",
    m: [["tmpfs saturation", "% of before", 100, [90, 92], "higher"]],
    trade: "Confirm and delivery p99 fell, but it needs its correctness gates on single-log queues, and benchmarks with instant consumers overstate it.",
    why: "Kept on its branch." },
  { v: "setting", date: "2026-10-13", title: "Buffered queues (AfterFlush)",
    what: "A queue can confirm and deliver once every required copy has written the message to the operating system, before the fsync. A background sync makes it durable, every 100 ms by default.",
    m: [["10k msg/s p99, no sync vs 100 ms sync", "ms", [96.6, 110.2], [11.2, 11.7], "lower"]],
    trade: "A weaker guarantee: power loss or a storage failure on every required node at once can lose confirmed messages from the last sync window. The first broker attempt in September stalled, because replication and delivery read only durable records, and without a background sync it had a repeatable tail and CPU regression.",
    why: "Merged on 13 October as an experimental per-queue mode, once Keratin tracked written and durable progress separately and replication carried both. Durable stays the default, and a queue's mode cannot be changed after its first declare yet." },
  { v: "parked", date: "2026-09-22", title: "A much larger tail cache",
    what: "Raised the read cache from 64 MiB to 4 GiB.",
    m: [["broker memory", "GiB", 0.44, 4.87, "lower"], ["delivery throughput", "k msg/s", [419, 422], [419, 422], "higher"]],
    trade: "Cache hits reached 100% and delivery did not move.",
    why: "Default unchanged. Reads were never the limit." },
  { v: "removed", date: "2026-10-25", title: "Stage-then-commit",
    what: "Stage writer input immediately in bounded chunks and decide fsync submission after staging.",
    m: [["1k msg/s CPU per million", "s", [439, 477], [965, 1057], "lower"], ["1k msg/s p99", "ms", [16, 17], [25, 31], "lower"]],
    trade: "It had won on an older build. Against current main it cost about twice the CPU and a worse low-rate p99 for about 1% at saturation.",
    why: "Removed, along with its switch." },
  { v: "removed", date: "2026-09-27", title: "Close a batch when writer input drains",
    what: "End the pending append batch as soon as the writer's input queue is empty.",
    m: [["8 KiB median delivery", "ms", [86, 87], [72, 73], "lower"], ["fsync jobs", "% of before", 100, 118, "lower"]],
    trade: "About 11% more CPU, no early closures at 1 KiB, and later no effect at 100 or 1k msg/s.",
    why: "Removed with the other storage switches." },
  { v: "pending", date: "2026-10-27", title: "jemalloc instead of mimalloc v2",
    what: "An allocator comparison of mimalloc v2, jemalloc, snmalloc and tcmalloc on the same build.",
    m: [["memory per node, 200 idle queues", "MiB", [395, 513], [296, 322], "lower"], ["saturation throughput", "k msg/s", [130.8, 135.4], [138.6, 139.4], "higher"]],
    trade: "tcmalloc used the least memory but more idle CPU and comes from a small wrapper. snmalloc used the most idle memory.",
    why: "On a branch, waiting on longer stability soaks before it replaces mimalloc v2." },
];

const mid = (x) => Array.isArray(x) ? (x[0] + x[1]) / 2 : x;
const lo = (x) => Array.isArray(x) ? x[0] : x;
const hi = (x) => Array.isArray(x) ? x[1] : x;
const fmt = (x) => {
  const f = (n) => (n >= 100 ? Math.round(n).toLocaleString("en-US") : n >= 10 ? (Math.round(n * 10) / 10).toString() : (Math.round(n * 100) / 100).toString());
  return Array.isArray(x) ? `${f(x[0])}–${f(x[1])}` : f(x);
};

// One before/after row drawn on a shared scale that starts at zero.
/** @param {any[]} row One `m` entry: label, unit, before, after and which way is better. */
export function dumbbell(row) {
  const [label, unit, b, a, better] = row;
  // Drawn at about a card's width, label and values on their own lines so
  // long labels never run into the numbers.
  const W = 340, L = 6, R = 334, max = Math.max(hi(b), hi(a)) * 1.08;
  const sx = (v) => L + (v / max) * (R - L);
  const improved = better === "lower" ? mid(a) < mid(b) : mid(a) > mid(b);
  const same = Math.abs(mid(a) - mid(b)) / Math.max(mid(b), 1e-9) < 0.005;
  const tone = same ? "var(--muted)" : improved ? "var(--good)" : "var(--bad)";
  const u = unit ? ` ${unit}` : "";
  const y = 44;
  return `<svg viewBox="0 0 ${W} 62" role="img" aria-label="${label}: ${fmt(b)} to ${fmt(a)}${u}">
    <text x="${L}" y="13" class="tm" font-size="12">${label}</text>
    <text x="${L}" y="29" class="tl" font-size="12.5">${fmt(b)} → ${fmt(a)}${u}</text>
    <line x1="${L}" y1="${y}" x2="${R}" y2="${y}" stroke="var(--line)" stroke-width="1"/>
    <line x1="${L}" y1="${y - 4}" x2="${L}" y2="${y + 4}" stroke="var(--line)"/>
    <line x1="${sx(mid(b))}" y1="${y}" x2="${sx(mid(a))}" y2="${y}" stroke="${tone}" stroke-width="2" opacity="0.6"/>
    <rect x="${sx(lo(b))}" y="${y - 5}" width="${Math.max(sx(hi(b)) - sx(lo(b)), 0)}" height="10" rx="5" fill="var(--muted)" opacity="0.35"/>
    <circle cx="${sx(mid(b))}" cy="${y}" r="5" fill="var(--muted)"/>
    <rect x="${sx(lo(a))}" y="${y - 5}" width="${Math.max(sx(hi(a)) - sx(lo(a)), 0)}" height="10" rx="5" fill="${tone}" opacity="0.35"/>
    <circle cx="${sx(mid(a))}" cy="${y}" r="5.5" fill="${tone}"/>
    <text x="${L}" y="60" class="tm" font-size="10">0</text>
  </svg>`;
}
