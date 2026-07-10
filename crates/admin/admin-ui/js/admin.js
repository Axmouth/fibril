function fmtUptime(seconds) {
  seconds = Math.floor(seconds);
  const d = Math.floor(seconds / 86400);
  seconds %= 86400;
  const h = Math.floor(seconds / 3600);
  seconds %= 3600;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;

  if (d > 0) return `${d}d ${h}h`;
  if (h > 0) return `${h}h ${m}m`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

function fmtAgo(ts) {
  return fmtUptime((Date.now() - ts*1000)/1000) + " ago";
}

// Shared polling driver that does not stomp on what the operator is doing.
// Replaces a bare `setInterval(refresh, ms)`: it skips a tick while the tab is
// hidden, while a form control is focused, while text is selected, or within one
// interval of the last interaction (pointer, key, wheel, focus), and it never
// overlaps refreshes. It also restores the window scroll position if a rebuild
// shifted it. The page's own event-driven refresh (search, expand) still runs.
function autoRefresh(refreshFn, intervalMs) {
  let lastInteraction = 0;
  let running = false;
  const mark = () => {
    lastInteraction = Date.now();
  };
  for (const ev of ["pointerdown", "keydown", "wheel", "focusin"]) {
    document.addEventListener(ev, mark, { passive: true });
  }

  const busy = () => {
    if (document.hidden) return true;
    if (running) return true;
    if (Date.now() - lastInteraction < intervalMs) return true;
    const el = document.activeElement;
    if (el && /^(INPUT|SELECT|TEXTAREA)$/.test(el.tagName)) return true;
    const selection = window.getSelection && window.getSelection();
    if (selection && !selection.isCollapsed && String(selection).length > 0) return true;
    return false;
  };

  const tick = async () => {
    if (busy()) return;
    const x = window.scrollX;
    const y = window.scrollY;
    running = true;
    try {
      await refreshFn();
    } catch (_err) {
      // Swallow: the next tick retries. The page's own error handling, if any,
      // applies inside refreshFn.
    } finally {
      running = false;
      if (window.scrollX !== x || window.scrollY !== y) {
        window.scrollTo(x, y);
      }
    }
  };

  setInterval(tick, intervalMs);
}

// ---- tiny SVG charts, shared by every page ----
// All charts read their size from the svg's width/height attributes and draw in
// that coordinate space (preserveAspectRatio="none" makes them stretch). Values
// are plain number arrays; missing/NaN entries break the line.

function chartScale(values, height, pad) {
  const finite = values.filter(Number.isFinite);
  const max = finite.length ? Math.max(...finite) : 1;
  const min = finite.length ? Math.min(...finite, 0) : 0;
  const span = max - min || 1;
  return (v) => height - pad - ((v - min) / span) * (height - 2 * pad);
}

function chartPath(values, width, yOf) {
  const n = values.length;
  if (n === 0) return "";
  if (n === 1) return `M0,${yOf(values[0])}L${width},${yOf(values[0])}`;
  let d = "";
  for (let i = 0; i < n; i++) {
    const v = values[i];
    if (!Number.isFinite(v)) continue;
    const x = (i / (n - 1)) * width;
    d += `${d && Number.isFinite(values[i - 1]) ? "L" : "M"}${x.toFixed(1)},${yOf(v).toFixed(1)}`;
  }
  return d;
}

// A row-sized trend line with a soft area fill and an endpoint dot.
function renderSparkline(svg, values, color) {
  const w = Number(svg.getAttribute("width")) || 110;
  const h = Number(svg.getAttribute("height")) || 26;
  svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
  svg.setAttribute("preserveAspectRatio", "none");
  if (!values || values.length === 0) {
    svg.innerHTML = "";
    return;
  }
  const yOf = chartScale(values, h, 2);
  const d = chartPath(values, w, yOf);
  const last = values[values.length - 1];
  const endY = Number.isFinite(last) ? yOf(last).toFixed(1) : h / 2;
  svg.innerHTML =
    `<path d="${d}" fill="none" stroke="${color}" stroke-width="1.4" vector-effect="non-scaling-stroke"/>` +
    `<path d="${d}L${w},${h}L0,${h}Z" fill="${color}" opacity=".12" stroke="none"/>` +
    `<circle cx="${w - 1}" cy="${endY}" r="2.2" fill="${color}"/>`;
}

// A panel chart: faint gridlines, one or more series, oldest left. Series:
// { values, color, fill } - fill draws a soft area under that series.
function renderChart(svg, seriesList) {
  const w = Number(svg.getAttribute("width")) || 720;
  const h = Number(svg.getAttribute("height")) || 180;
  svg.setAttribute("viewBox", `0 0 ${w} ${h}`);
  svg.setAttribute("preserveAspectRatio", "none");
  const all = seriesList.flatMap((s) => s.values);
  if (all.filter(Number.isFinite).length === 0) {
    svg.innerHTML = "";
    return;
  }
  const yOf = chartScale(all, h, 6);
  let out = "";
  for (let i = 1; i < 4; i++) {
    const y = ((h * i) / 4).toFixed(1);
    out += `<line x1="0" y1="${y}" x2="${w}" y2="${y}" stroke="var(--line)" stroke-width="1"/>`;
  }
  for (const series of seriesList) {
    const d = chartPath(series.values, w, yOf);
    if (!d) continue;
    if (series.fill) {
      out += `<path d="${d}L${w},${h}L0,${h}Z" fill="${series.color}" opacity=".10" stroke="none"/>`;
    }
    out += `<path d="${d}" fill="none" stroke="${series.color}" stroke-width="1.6" vector-effect="non-scaling-stroke"/>`;
  }
  svg.innerHTML = out;
}

// One word for where a series is heading, comparing now against a moment ago.
function trendWord(values, lookback = 12) {
  if (!values || values.length < 2) return "";
  const last = values[values.length - 1];
  const past = values[Math.max(0, values.length - 1 - lookback)];
  if (!Number.isFinite(last) || !Number.isFinite(past)) return "";
  const base = Math.max(Math.abs(past), 1);
  const change = (last - past) / base;
  if (change > 0.05) return "rising";
  if (change < -0.05) return "falling";
  return "steady";
}

// Shared HTML escaping for pages that build rows from API data.
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, (c) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[c]);
}

function fmtCount(n) {
  return Number.isFinite(n) ? Number(n).toLocaleString("en-US") : "--";
}
