// Shared procedural fiber grammar for the admin diagrams (the Cluster page
// and the Connections page). This file owns the pure drawing primitives -
// outlined two-tone strands, organic branch growth, snaking links, tips,
// and traveling pulses. Each page composes its own layout on top (batches,
// fusing, welds, and node placement stay page-local).
window.FibrilTendrils = (() => {
  function hashStr(s) {
    let h = 2166136261;
    for (const c of String(s)) { h ^= c.charCodeAt(0); h = Math.imul(h, 16777619); }
    return h >>> 0;
  }
  const rnd = (seed) => { const x = Math.sin(seed) * 43758.5453; return x - Math.floor(x); };

  const spriteUrl = (frame, size) => `/static/img/sprites/ring-${frame}-${size}.png`;

  // Two-tone strands: dark border carries the pixel look, a thin offset
  // highlight breaks the uniformity like the art's lit fiber edge.
  const HIGHLIGHT = {
    "#6db5f2": "#abd6f9", "#3d7bf0": "#82aaf6",
    "#8a4be8": "#b78ef2", "#e8b65b": "#f6d795",
  };

  // Pulse cadence per load bucket: idle strands shimmer rarely, flat-out
  // ones fire almost continuously.
  const PULSE_CADENCE = [3.5, 1.4, 0.7, 0.35];

  const fmtRate = (rate) => (rate >= 10000 ? Math.round(rate / 1000) + "k"
    : rate >= 1000 ? (rate / 1000).toFixed(1) + "k"
    : String(Math.round(rate))) + " msg/s";

  // Binds one drawn frame's context: the diagram-to-canvas transforms and
  // the animation clocks. Pages call this once per frame.
  function makeKit({ px, py, sway, swayT, reducedMotion, pulseHidden }) {
    // Optional stops ([[t, color], ...]) stroke the path with a gradient
    // laid between its endpoints - used to melt a link's color into the
    // color of the fiber it fuses onto, so junctions never show a seam.
    const drawPathOn = (ctx, pts, color, alpha, width, stops) => {
      if (pts.length < 2) return;
      const trace = (dx, dy) => {
        ctx.beginPath();
        ctx.moveTo(px(pts[0][0] + dx), py(pts[0][1] + dy));
        for (let k = 1; k < pts.length - 1; k++) {
          const mx = (pts[k][0] + pts[k + 1][0]) / 2 + dx;
          const my = (pts[k][1] + pts[k + 1][1]) / 2 + dy;
          ctx.quadraticCurveTo(px(pts[k][0] + dx), py(pts[k][1] + dy), px(mx), py(my));
        }
        const last = pts[pts.length - 1];
        ctx.lineTo(px(last[0] + dx), py(last[1] + dy));
        ctx.stroke();
      };
      const grad = (mapColor) => {
        const a = pts[0], b = pts[pts.length - 1];
        const g = ctx.createLinearGradient(px(a[0]), py(a[1]), px(b[0]), py(b[1]));
        for (const [t, c] of stops) g.addColorStop(t, mapColor(c));
        return g;
      };
      ctx.globalAlpha = alpha;
      ctx.strokeStyle = "#0b0e14";
      ctx.lineWidth = width + 2.6;
      trace(0, 0);
      ctx.strokeStyle = stops ? grad((c) => c) : color;
      ctx.lineWidth = width;
      trace(0, 0);
      const hl = HIGHLIGHT[color];
      if (hl) {
        ctx.globalAlpha = alpha * 0.8;
        ctx.strokeStyle = stops ? grad((c) => HIGHLIGHT[c] || c) : hl;
        ctx.lineWidth = Math.max(1, width * 0.4);
        trace(-0.9, -0.9);
      }
    };

    const drawTip = (ctx, x, y, color, alpha) => {
      ctx.globalAlpha = alpha;
      ctx.fillStyle = color;
      ctx.beginPath();
      ctx.arc(px(x), py(y), 2.1, 0, Math.PI * 2);
      ctx.fill();
      ctx.globalAlpha = alpha * 0.35;
      ctx.beginPath();
      ctx.arc(px(x), py(y), 4, 0, Math.PI * 2);
      ctx.fill();
    };

    const growPath = (x, y, angle, len, seed) => {
      const steps = 7;
      const pts = [[x, y]];
      let a = angle;
      for (let k = 1; k <= steps; k++) {
        const settle = Math.min(1, (k - 1) / 2);
        a += ((rnd(seed + k) - 0.5) * 0.55
          + Math.sin(sway * 0.8 + seed + k * 1.7) * 0.045) * settle;
        const step = (len / steps) * (0.8 + rnd(seed + 40 + k) * 0.4);
        x += Math.cos(a) * step;
        y += Math.sin(a) * step;
        pts.push([x, y]);
      }
      return pts;
    };

    const tangentAt = (pts, k) => {
      const a = pts[Math.max(0, k - 1)], b = pts[Math.min(pts.length - 1, k + 1)];
      return Math.atan2(b[1] - a[1], b[0] - a[0]);
    };

    const pathPoint = (pts, t) => {
      const lens = [];
      let total = 0;
      for (let k = 1; k < pts.length; k++) {
        const l = Math.hypot(pts[k][0] - pts[k - 1][0], pts[k][1] - pts[k - 1][1]);
        lens.push(l); total += l;
      }
      let want = t * total;
      for (let k = 0; k < lens.length; k++) {
        if (want <= lens[k]) {
          const f = lens[k] ? want / lens[k] : 0;
          return [pts[k][0] + (pts[k + 1][0] - pts[k][0]) * f,
                  pts[k][1] + (pts[k + 1][1] - pts[k][1]) * f];
        }
        want -= lens[k];
      }
      return pts[pts.length - 1];
    };

    // Frequent, quick pulses. The cadence multiplier scales the period, so
    // loaded strands fire more often. Direction is seeded per strand
    // unless `dir` forces it (+1 start->end, -1 end->start).
    const drawPulse = (ctx, pts, seed, color, cadence, dir) => {
      if (reducedMotion) return;
      const period = (2.6 + rnd(seed + 300) * 3) * (cadence ?? 1);
      const phase = ((swayT * 2.4 + rnd(seed + 301) * period) % period);
      if (phase > 1.1) return;
      let t = phase / 1.1;
      const backwards = dir != null ? dir < 0 : (hashStr("dir" + seed) % 2 === 1);
      if (backwards) t = 1 - t;
      const [x, y] = pathPoint(pts, t);
      if (pulseHidden && pulseHidden(x, y)) return;
      drawTip(ctx, x, y, color, 0.95);
    };

    const linkPts = (x1, y1, x2, y2, seed) => {
      const dx = x2 - x1, dy = y2 - y1;
      const len = Math.hypot(dx, dy) || 1;
      const nx = -dy / len, ny = dx / len;
      const amp = Math.min(16, 6 + len * 0.035) * (0.7 + rnd(seed) * 0.5)
        * Math.min(1, len / 180);
      const waves = 1.2 + rnd(seed + 4);
      const phase = rnd(seed + 5) * Math.PI * 2;
      // Fewer samples on short spans: a close link gets one long gentle
      // curve instead of a stair-step of tight wiggles.
      const SAMPLES = Math.max(4, Math.min(11, Math.round(len / 24)));
      const pts = [];
      for (let k = 0; k <= SAMPLES; k++) {
        const t = k / SAMPLES;
        const taper = Math.sin(Math.PI * t);
        const wob = Math.sin(t * Math.PI * 2 * waves + phase + sway * 0.9)
          + 0.35 * Math.sin(t * Math.PI * 4.3 + phase * 2 - sway * 1.1);
        const off = wob * amp * taper;
        pts.push([x1 + dx * t + nx * off, y1 + dy * t + ny * off]);
      }
      return pts;
    };

    return { drawPathOn, drawTip, growPath, tangentAt, pathPoint, drawPulse, linkPts };
  }

  return { hashStr, rnd, spriteUrl, HIGHLIGHT, PULSE_CADENCE, fmtRate, makeKit };
})();
