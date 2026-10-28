// Scenes for the storage lifecycle story. Each draw(t) returns SVG markup for
// time t in seconds, so the same code renders the static first frame at build
// time and the animation in the browser. Colours come from CSS variables set on
// the .storage-lifecycle root.
const W = 210, H = 22, GAP = 18, X0 = 34, LANE = 120;
const slotX = (i) => X0 + i * (W + GAP);
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const lerp = (a, b, k) => a + (b - a) * k;
const ease = (k) => { k = clamp(k, 0, 1); return k < 0.5 ? 2 * k * k : 1 - Math.pow(-2 * k + 2, 2) / 2; };
const seg = (t, a, b) => clamp((t - a) / (b - a), 0, 1);

export const defs = `<defs>
  <pattern id="sl-pz" width="6" height="6" patternUnits="userSpaceOnUse" patternTransform="rotate(135)"><rect width="2.2" height="6" fill="var(--zeros)"/></pattern>
  <pattern id="sl-po" width="7" height="7" patternUnits="userSpaceOnUse" patternTransform="rotate(45)"><rect width="3" height="7" fill="var(--old)"/></pattern>
</defs>`;

// One segment file. Fractions run 0..1 of the segment's full size.
function slab(x, y, o) {
  const w = o.w || W, h = o.h || H;
  const fill = (a, b, paint, op = 1) => b > a ? `<rect x="${x + a * w}" y="${y}" width="${(b - a) * w}" height="${h}" fill="${paint}" opacity="${op}"/>` : "";
  let s = `<g opacity="${o.opacity ?? 1}" transform="translate(${o.dx || 0} ${o.dy || 0})">`;
  if (o.reserved) s += `<rect x="${x}" y="${y}" width="${w}" height="${h}" rx="3" fill="none" stroke="var(--muted)" stroke-dasharray="5 4" stroke-width="1.4"/>`;
  s += fill(o.written ?? 0, o.oldTo ?? 0, "url(#sl-po)");
  s += fill(o.written ?? 0, o.zeroTo ?? 0, "url(#sl-pz)");
  s += fill(o.synced ?? 0, o.written ?? 0, "var(--written)");
  s += fill(0, o.synced ?? 0, o.spare ? "var(--spare)" : "var(--synced)");
  const extent = o.reserved || o.full ? 1 : Math.max(o.file ?? o.written ?? 0, 0.012);
  s += `<rect x="${x}" y="${y}" width="${extent * w}" height="${h}" rx="3" fill="none" stroke="var(--text)" stroke-opacity="${o.reserved ? 0 : 0.55}" stroke-width="1.4"/>`;
  if (o.frontiers && (o.written ?? 0) > 0 && (o.written ?? 0) < 1) {
    const wx = x + o.written * w;
    // Frontier tags sit on two rows under the slab so they never collide.
    s += `<line x1="${wx}" y1="${y - 5}" x2="${wx}" y2="${y + h + 6}" stroke="var(--written)" stroke-width="2"/>`;
    s += `<text x="${wx}" y="${y + h + 19}" text-anchor="middle" class="tm" font-size="13">written</text>`;
    if ((o.synced ?? 0) > 0.02) {
      const sx = x + o.synced * w;
      s += `<line x1="${sx}" y1="${y - 5}" x2="${sx}" y2="${y + h + 24}" stroke="var(--synced)" stroke-width="2"/>`;
      s += `<text x="${sx}" y="${y + h + 36}" text-anchor="middle" class="tm" font-size="13" fill="var(--synced)">synced</text>`;
    }
  }
  if (o.label) s += `<text x="${x}" y="${y - 12}" class="tm" font-size="14.5">${o.label}</text>`;
  if (o.note) s += `<text x="${x}" y="${y + h + 58}" class="tm" font-size="14.5">${o.note}</text>`;
  return s + "</g>";
}

// Zero-ahead lead for a segment written by wr(t) from time `from`: it starts a
// window ahead and refills in half-window chunks once the lead drops below
// half, never past the window or the end of the file. This is a sawtooth, not
// a fixed distance.
const WIN = 0.3;
function zeroAhead(t, wr, from = 0, fillSecs = 0.35) {
  if (t < from) return 0;
  let zt = Math.min(wr(from) + WIN, 1), due = -1;
  for (let tt = from; tt <= t; tt += 0.02) {
    if (due < 0 && zt - wr(tt) < WIN / 2 && zt < 1) due = tt + fillSecs;
    if (due >= 0 && tt >= due) { zt = Math.min(zt + WIN / 2, wr(tt) + WIN, 1); due = -1; }
  }
  return Math.max(zt, wr(t));
}

// A cost bubble that rises and fades after an fsync completes.
function bubble(x, y, text, age, tone = "var(--synced)") {
  // A bubble lives 1 s, shorter than the gap between fsyncs, so two never overlap.
  if (age < 0 || age > 1.0) return "";
  const k = age / 1.0, op = age < 0.15 ? age / 0.15 : 1 - Math.max(0, (k - 0.55) / 0.45);
  const yy = y - 34 - k * 24, wide = text.length * 8.4 + 20;
  x = clamp(x, wide / 2 + 6, 960 - wide / 2 - 6);
  return `<g opacity="${clamp(op, 0, 1)}"><rect x="${x - wide / 2}" y="${yy - 17}" width="${wide}" height="24" rx="12" fill="var(--panel)" stroke="${tone}" stroke-width="1.2"/>
    <text x="${x}" y="${yy}" text-anchor="middle" class="tl" font-size="15">${text}</text></g>`;
}

// fsyncs every `every` seconds from `first`; returns synced fraction and bubbles.
// fsyncs stop starting after `until` (a log that rolled away), but their bubbles
// keep ageing with the real time so they fade out.
function syncs(t, written, first, every, costs, x, _w = W, y = LANE, label = (c) => `fsync ${c.toFixed(1)} ms`, until = Infinity) {
  let synced = 0, out = "";
  for (let k = 0, ts = first; ts <= Math.min(t, until) + 0.001 && k < 60; k++, ts += every) {
    const done = ts + 0.3;
    if (done <= t) synced = written(ts);
    const c = costs[k % costs.length];
    out += bubble(x(synced) , y, label(c), t - done);
  }
  return { synced, out };
}

function chips(y, title, items, t) {
  let s = `<text x="${X0}" y="${y + 17}" class="tm" font-size="14.5">${title}</text>`;
  let x = X0 + 80;
  for (const it of items) {
    const on = t >= it.at, active = on && t < it.at + (it.hold ?? 0.6);
    const w = it.text.length * 8.3 + 18;
    // Wrap to a second row rather than run past the drawing.
    if (x + w > 940) { x = X0 + 80; y += 30; }
    s += `<g opacity="${on ? 1 : 0.28}"><rect x="${x}" y="${y}" width="${w}" height="22" rx="5" fill="${active ? "var(--written)" : "var(--panel-2)"}" stroke="${it.tone || "var(--line)"}"/>
      <text x="${x + w / 2}" y="${y + 15}" text-anchor="middle" class="tl" font-size="14.5">${it.text}</text></g>`;
    x += w + 8;
  }
  return s;
}

const shelf = (label) => `<rect x="${X0}" y="300" width="${4 * W + 3 * GAP}" height="86" rx="8" fill="var(--panel-2)" stroke="var(--line)"/>
  <text x="${X0 + 12}" y="322" class="tm" font-size="14.5">${label}</text>`;

const before = [4.6, 5.1, 4.8, 4.9, 5.3, 4.7, 5.0, 4.8];
const zeroed = [0.7, 0.8, 0.7, 0.6, 0.9, 0.7, 0.8];
const reused = [0.7, 0.6, 0.8, 0.7, 0.7, 0.6];

export const scenes = [
  {
    name: "Append and fsync", dur: 10,
    title: "Appends move the written frontier, an fsync moves the synced one",
    text: "The writer appends records and the file grows with them. An fsync makes everything written before it durable, so one sync covers every append since the last one. That group commit is why the synced shade jumps in steps. A confirm waits for the sync that covers its record. Every append here extends the file, so each sync also waits for the filesystem to record the new blocks in its journal.",
    fig: "Measured at 1k msg/s: each owner fsync took a flat <b>4.9 ms</b> and carried about <b>4 records</b>, set by those journal commits.",
    draw(t) {
      const wr = (tt) => clamp(0.05 + 0.08 * tt, 0, 0.86);
      const s = syncs(t, wr, 0.9, 1.25, before, (f) => slotX(0) + f * W);
      return slab(slotX(0), LANE, { written: wr(t), synced: s.synced, frontiers: true, label: "segment 7", note: "file grows as it is written" }) + s.out
        + `<text x="${slotX(1) + 20}" y="${LANE + H / 2 + 4}" class="tm" font-size="15.5">records →</text>`;
    },
  },
  {
    name: "Rollover, before", dur: 11,
    title: "Rolling to a new file used to stall the writer",
    text: "When a segment fills, the writer opens the next one. It used to do every step inline: sync the outgoing segment, create the file, write and sync its header, sync the directory so the new name is durable, then write and sync the sealed index. Appends waited behind all of it.",
    fig: "Measured: owner rollover held the writer for about <b>18 ms</b>.",
    draw(t) {
      const r = 2.2, steps = ["sync segment 7", "create file", "write header", "sync header", "sync directory", "sealed index", "sync index"];
      const wr0 = (tt) => clamp(0.82 + 0.09 * tt, 0, 1);
      const s0 = syncs(t, wr0, 0.5, 0.9, before, (f) => slotX(0) + f * W, W, LANE, undefined, r);
      const syncedA = t >= r + 0.3 ? 1 : s0.synced;
      const after = r + steps.length * 0.6 + 0.4;
      const wr1 = (tt) => clamp((tt - after) * 0.07, 0, 0.32);
      const s1 = syncs(t, wr1, after + 0.9, 1.2, before, (f) => slotX(1) + f * W);
      let out = slab(slotX(0), LANE, { written: wr0(Math.min(t, r)), synced: syncedA, frontiers: t < r, label: "segment 7" }) + s0.out;
      if (t >= after) out += slab(slotX(1), LANE, { written: wr1(t), synced: t > after + 1.2 ? s1.synced : 0, frontiers: true, label: "segment 8" }) + s1.out;
      out += chips(250, "writer", steps.map((x, i) => ({ text: x, at: r + i * 0.6, tone: "var(--warn)" })), t);
      if (t >= r && t < after) {
        const waiting = Math.floor((t - r) * 9);
        out += `<text x="${slotX(1)}" y="${LANE + H / 2 + 4}" class="t" font-size="17" fill="var(--warn)">appends waiting: ${waiting}</text>`;
      }
      if (t >= after) out += `<text x="${X0 + 80}" y="330" class="tm" font-size="14.5">writer was blocked for the whole roll</text>`;
      return out;
    },
  },
  {
    name: "Preallocation alone", dur: 9,
    title: "Reserving the file up front did not make syncs cheaper",
    text: "Preallocating the segment reserves its full size at once, so the file stops growing. The space is reserved but never written, and the filesystem still records each block the first time it is written, with a journal commit. The slab is full size but hollow, and the fsync cost stays where it was.",
    fig: "fallocate alone left the per-sync floor unchanged.",
    draw(t) {
      const wr = (tt) => clamp(0.03 + 0.065 * tt, 0, 0.6);
      const s = syncs(t, wr, 0.9, 1.15, before, (f) => slotX(1) + f * W);
      return slab(slotX(0), LANE, { written: 1, synced: 1, full: true, label: "segment 7" })
        + slab(slotX(1), LANE, { reserved: true, written: wr(t), synced: s.synced, frontiers: true, label: "segment 8", note: "reserved, not written" }) + s.out;
    },
  },
  {
    name: "Zero-ahead", dur: 10,
    title: "A background worker writes zeros ahead of the writer",
    text: "One shared worker writes and syncs zeros into the reserved space ahead of each log's writer. When the writer gets there, the blocks already exist on disk, so its fsync only flushes data. The lead is a sawtooth: when it drops below half the window, the worker zeroes 4 MiB chunks until it is a full window ahead again, never further. The window is the preallocation size, 8 MiB by default, so the lead stays between about 4 and 8 MiB. Zeroing starts once a segment holds 64 KiB, all segments share a 64 MiB/s cap, and a writer that overtakes its zeros turns zeroing off for that segment, since at that rate it would only double the disk traffic.",
    fig: "Measured: owner fsync p50 <b>4.9 → 0.73 ms</b>. At 1k msg/s end to end, p50 <b>9.5 → 2.6–3.6 ms</b> and p99 <b>28–30 → 13–16 ms</b>.",
    draw(t) {
      const wr = (tt) => clamp(0.03 + 0.06 * tt, 0, 0.6);
      const win = WIN, zt = zeroAhead(t, wr);
      const s = syncs(t, wr, 0.9, 1.1, zeroed, (f) => slotX(1) + f * W, W, LANE, (c) => `fsync ${c.toFixed(1)} ms`);
      return slab(slotX(0), LANE, { written: 1, synced: 1, full: true, label: "segment 7" })
        + slab(slotX(1), LANE, { reserved: true, zeroTo: Math.max(zt, wr(t)), written: wr(t), synced: s.synced, frontiers: true, label: "segment 8", note: "zeros run ahead" }) + s.out
        + `<text x="${slotX(1) + Math.max(zt, wr(t)) * W + 6}" y="${LANE - 3}" class="tm" font-size="13" fill="var(--zeros)">lead ${((zt - wr(t)) / win * 8).toFixed(1)} MiB</text>`;
    },
  },
  {
    name: "Deferred seal", dur: 10,
    title: "Rollover hands its syncs to the next fsync",
    text: "The writer now only switches files and keeps appending. The outgoing segment's last data, the new file's header and the directory entry are synced by the next fsync, together with the new appends. No record in the new segment counts as durable before that sync, and recovery drops a new segment whose header never made it to disk.",
    fig: "Measured: owner roll time <b>18 → 1 ms</b>, throughput <b>+2–5%</b>.",
    draw(t) {
      const r = 1.8;
      const wrA = (tt) => clamp(0.86 + 0.08 * tt, 0, 1);
      const wrB = (tt) => clamp((tt - r) * 0.075, 0, 0.55);
      const firstJoint = r + 1.1;
      let synA = t < 0.8 ? 0.84 : 0.9;
      if (t >= firstJoint + 0.3) synA = 1;
      const sB = syncs(t, wrB, firstJoint, 1.2, zeroed, (f) => slotX(1) + f * W, W, LANE,
        (c) => `fsync ${c.toFixed(1)} ms`);
      let out = slab(slotX(0), LANE, { written: wrA(Math.min(t, r)), synced: synA, frontiers: t < firstJoint, label: "segment 7" });
      if (t >= r) out += slab(slotX(1), LANE, { reserved: true, zeroTo: zeroAhead(t, wrB, r), written: wrB(t), synced: sB.synced, frontiers: true, label: "segment 8" }) + sB.out;
      out += bubble(slotX(0) + W, LANE, "one fsync: segment 7 tail + segment 8 header + directory", t - firstJoint - 0.3);
      out += chips(250, "writer", [{ text: "switch files, about 1 ms", at: r, hold: 0.4, tone: "var(--ok)" }, { text: "keep appending", at: r + 0.3, hold: 99 }], t);
      out += chips(282, "fsync", [{ text: "segment 7 tail", at: firstJoint, hold: 0.5 }, { text: "segment 8 header", at: firstJoint, hold: 0.5 }, { text: "directory entry", at: firstJoint, hold: 0.5 }, { text: "new appends", at: firstJoint, hold: 0.5 }], t);
      return out;
    },
  },
  {
    name: "Truncate and reuse", dur: 12,
    title: "Consumed segments become spares and come back at the next roll",
    text: "Once every message in a segment is acknowledged, the log truncates it off the writer's path. A freed file is kept as a spare, up to 4 per log, instead of being deleted. At the next roll the writer renames a spare into place. Its blocks were all written before, so syncs stay cheap and the zero worker only has to run past the file's old end.",
    fig: "Reused files skip both the journal growth and the zeroing for the length they already had.",
    draw(t) {
      const shed0 = 1.4, fade = 2.2, toPool = 3.0, inPool = 4.3, roll = 7, back = 8.2;
      const wr2 = (tt) => clamp(0.55 + 0.065 * tt, 0, 1);
      const s2 = syncs(t, wr2, 0.6, 1.1, zeroed, (f) => slotX(2) + f * W, W, LANE, undefined, roll);
      let out = shelf("spares for this log, up to 4");
      const spareK = seg(t, fade, toPool);
      // Slab 0 sheds to the pool, then returns as segment 10.
      const moveDown = ease(seg(t, toPool, inPool)), moveBack = ease(seg(t, roll, back));
      const px = lerp(slotX(0), X0 + 330, moveDown), py = lerp(LANE, 350, moveDown);
      const fx = lerp(px, slotX(3), moveBack), fy = lerp(py, LANE, moveBack);
      const wr3 = (tt) => clamp((tt - back) * 0.08, 0, 0.32);
      const s3 = syncs(t, wr3, back + 0.8, 1.1, reused, (f) => slotX(3) + f * W);
      if (t < back) {
        out += slab(fx, fy, { written: 1, synced: 1, full: true, spare: spareK > 0.5, opacity: t < fade ? 1 : lerp(1, 0.85, spareK),
          label: t < fade ? "segment 7, consumed" : "spare file", note: t < shed0 ? "" : (t < fade ? "all acked" : "") });
      } else {
        out += slab(slotX(3), LANE, { full: true, oldTo: 1, written: wr3(t), synced: s3.synced, frontiers: true, label: "segment 10 (reused file)" }) + s3.out;
      }
      out += slab(slotX(1), LANE, { written: 1, synced: 1, full: true, label: "segment 8" });
      out += slab(slotX(2), LANE, { written: wr2(Math.min(t, roll)), synced: t >= roll ? 1 : s2.synced, reserved: true, zeroTo: zeroAhead(Math.min(t, roll), wr2), frontiers: t < roll, label: "segment 9" }) + s2.out;
      if (t >= roll && t < back) out += `<text x="${slotX(3)}" y="${LANE + H / 2 + 4}" class="t" font-size="17">roll: rename a spare into place</text>`;
      return out;
    },
  },
  {
    name: "Shared pool", dur: 13, tag: "proposed",
    title: "One spare pool per node, safe across logs",
    text: "Spares would move between logs through one pool per filesystem. A reused file can hold old records from another queue whose offsets happen to fit, so each file carries a generation number in its header that grows on every use, and every record is stamped with it. Recovery accepts only records that match the header. When a file enters the pool, its header is rewritten with the next generation and synced in the background, before any log can take it.",
    fig: "Proposed design, part of the resource limits work. No zeroing and no extra syncs on the write path.",
    draw(t) {
      const y1 = 100, y2 = 230, h = 18, w = 190;
      const sx = (i) => X0 + i * (w + 22);
      const rel = 1.0, down = 1.8, inPool = 3.0, stamped = 3.6, take = 7.0, arrive = 8.4;
      let out = `<text x="${X0}" y="${y1 - 46}" class="t" font-size="17" font-weight="650">queue orders</text>
        <text x="${X0}" y="${y2 - 46}" class="t" font-size="17" font-weight="650">queue billing</text>
        <rect x="${sx(3) - 6}" y="${y1 - 62}" width="${w + 50}" height="${y2 + h - y1 + 100}" rx="10" fill="var(--panel-2)" stroke="var(--line)"/>
        <text x="${sx(3) + 6}" y="${y1 - 44}" class="tm" font-size="14.5">node pool</text>`;
      out += slab(sx(1), y1, { w, h, written: 1, synced: 1, full: true, label: "segment 12" });
      out += slab(sx(2), y1, { w, h, reserved: true, zeroTo: 0.7, written: 0.45, synced: 0.38, label: "segment 13" });
      out += slab(sx(0), y2, { w, h, written: 1, synced: 1, full: true, label: "segment 40" });
      const md = ease(seg(t, down, inPool)), mb = ease(seg(t, take, arrive));
      const gx = lerp(lerp(sx(0), sx(3) + 14, md), sx(1), mb);
      const gy = lerp(lerp(y1, (y1 + y2) / 2 - 10, md), y2, mb);
      if (t < arrive) {
        const gen = t < stamped ? "gen 6" : "gen 7 · spare header synced";
        out += slab(gx, gy, { w, h, written: 1, synced: 1, full: true, spare: t >= down, label: t < rel ? "segment 11, consumed" : gen });
      } else {
        const wr = clamp((t - arrive) * 0.07, 0, 0.3);
        out += slab(sx(1), y2, { w, h, full: true, oldTo: 1, written: wr, synced: Math.max(0, wr - 0.06), frontiers: false, label: "segment 41 · gen 7" });
        out += `<text x="${sx(1)}" y="${y2 + h + 18}" class="tm" font-size="13" fill="var(--synced)">records stamped gen 7</text>`;
        out += `<text x="${sx(1)}" y="${y2 + h + 33}" class="tm" font-size="13">gen 6 leftovers past them: rejected by recovery</text>`;
      }
      out += `<text x="${X0 + 3 * W + 3 * GAP + 46}" y="21" text-anchor="middle" class="tm" font-size="14.5" fill="var(--organic)">proposed</text>`;
      return out;
    },
  },
];

/** Markup for one scene at time t, including the shared pattern definitions. */
export function frame(index, t) {
  const scene = scenes[index];
  return defs + scene.draw(clamp(t, 0, scene.dur));
}
