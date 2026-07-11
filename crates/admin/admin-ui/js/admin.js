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
// Shared "is the user in the middle of something" gate: rebuilding the DOM
// under a click, a focused form field, or a text selection is what the
// refresh machinery must never do, whether the data arrives by poll or push.
function interactionGuard(quietMs) {
  let lastInteraction = 0;
  const mark = () => {
    lastInteraction = Date.now();
  };
  for (const ev of ["pointerdown", "keydown", "wheel", "focusin"]) {
    document.addEventListener(ev, mark, { passive: true });
  }
  return () => {
    if (document.hidden) return true;
    if (Date.now() - lastInteraction < quietMs) return true;
    const el = document.activeElement;
    if (el && /^(INPUT|SELECT|TEXTAREA)$/.test(el.tagName)) return true;
    const selection = window.getSelection && window.getSelection();
    if (selection && !selection.isCollapsed && String(selection).length > 0) return true;
    return false;
  };
}

function autoRefresh(refreshFn, intervalMs) {
  let running = false;
  const guardBusy = interactionGuard(intervalMs);
  const busy = () => running || guardBusy();

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
function renderChart(svg, seriesList, opts) {
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
  const prevIdx = svg.__chart ? svg.__chart.idx : null;
  svg.__chart = { series: seriesList, times: opts && opts.times, idx: prevIdx };
  if (opts && opts.hover) {
    chartHoverWire(svg);
    chartHoverDraw(svg);
  }
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

// The queue debug snapshot reports the storage dead-letter policy enum via its
// Debug string (Discard, GlobalDQL, CustomDQL(..)). Normalize to the admin
// vocabulary before display.
function dlqPolicyKind(raw) {
  const s = String(raw || "");
  if (s.startsWith("CustomDQL")) return "custom";
  if (s.startsWith("GlobalDQL")) return "global";
  if (s.startsWith("Discard")) return "discard";
  return s ? s.toLowerCase() : "none";
}

// Shared toast: a short-lived confirmation for quick actions. Durable results
// keep using inline op-message spans.
let __toastTimer;
function toast(text) {
  let el = document.getElementById("shared-toast");
  if (!el) {
    el = document.createElement("div");
    el.id = "shared-toast";
    el.className = "toast";
    document.body.appendChild(el);
  }
  el.textContent = text;
  el.classList.add("on");
  clearTimeout(__toastTimer);
  __toastTimer = setTimeout(() => el.classList.remove("on"), 2400);
}

// Compact display for large counts: 12.4k, 5.08M. Pair with a title attribute
// carrying the exact value.
function fmtCompact(n) {
  if (!Number.isFinite(n)) return "--";
  const abs = Math.abs(n);
  if (abs < 10000) return Number(n).toLocaleString("en-US");
  const units = [[1e9, "B"], [1e6, "M"], [1e3, "k"]];
  for (const [div, suffix] of units) {
    if (abs >= div) {
      const v = n / div;
      return (v >= 100 ? v.toFixed(0) : v >= 10 ? v.toFixed(1) : v.toFixed(2)) + suffix;
    }
  }
  return String(n);
}

// Set a stat element to the compact form with the exact value on hover.
function setCompact(el, n) {
  el.textContent = fmtCompact(n);
  el.title = Number.isFinite(n) ? Number(n).toLocaleString("en-US") : "";
}

function timeAgoShort(unixSec) {
  const d = Math.max(0, Date.now() / 1000 - unixSec);
  if (d < 90) return Math.round(d) + "s ago";
  if (d < 5400) return Math.round(d / 60) + "m ago";
  return (d / 3600).toFixed(1) + "h ago";
}

// ---- chart hover readout ----
// renderChart(svg, series, { hover: true, times }) attaches a cursor line and
// a floating readout naming each series' value at the pointer. State lives on
// the svg so the 2s re-render keeps the cursor in place.

function chartHoverEnsureTip(svg) {
  let tip = svg.__chartTip;
  if (tip) return tip;
  const parent = svg.parentElement;
  parent.classList.add("chart-host");
  tip = document.createElement("div");
  tip.className = "chart-tip";
  tip.hidden = true;
  parent.appendChild(tip);
  svg.__chartTip = tip;
  return tip;
}

function chartHoverDraw(svg) {
  const state = svg.__chart;
  const tip = svg.__chartTip;
  if (!state || !tip) return;
  const idx = state.idx;
  const n = Math.max(...state.series.map((s) => s.values.length), 0);
  if (idx == null || n < 2) {
    tip.hidden = true;
    return;
  }
  const w = Number(svg.getAttribute("width")) || 720;
  const h = Number(svg.getAttribute("height")) || 180;
  const x = ((idx / (n - 1)) * w).toFixed(1);
  svg.insertAdjacentHTML(
    "beforeend",
    `<line class="chart-cursor" x1="${x}" y1="0" x2="${x}" y2="${h}" stroke="var(--dim)" stroke-width="1" vector-effect="non-scaling-stroke" opacity=".7"/>`,
  );
  const parts = state.series
    .filter((s) => Number.isFinite(s.values[idx]))
    .map((s) => {
      const v = s.values[idx];
      const shown = Number.isInteger(v) ? fmtCompact(v) : v.toFixed(1);
      return `<span class="tip-dot" style="background:${s.color}"></span>${s.label ? escapeHtml(s.label) + " " : ""}${shown}`;
    });
  const at = state.times && state.times[idx] ? ` · ${timeAgoShort(state.times[idx])}` : "";
  tip.innerHTML = parts.join("&ensp;") + `<span class="tip-when">${at}</span>`;
  const svgRect = svg.getBoundingClientRect();
  const hostRect = svg.parentElement.getBoundingClientRect();
  const px = svgRect.left - hostRect.left + (idx / (n - 1)) * svgRect.width;
  tip.hidden = false;
  const clamped = Math.max(4, Math.min(hostRect.width - tip.offsetWidth - 4, px - tip.offsetWidth / 2));
  tip.style.left = clamped + "px";
  tip.style.top = (svgRect.top - hostRect.top - tip.offsetHeight - 6) + "px";
}

function chartHoverWire(svg) {
  if (svg.__hoverWired) return;
  svg.__hoverWired = true;
  chartHoverEnsureTip(svg);
  svg.addEventListener("mousemove", (event) => {
    const state = svg.__chart;
    if (!state) return;
    const n = Math.max(...state.series.map((s) => s.values.length), 0);
    if (n < 2) return;
    const rect = svg.getBoundingClientRect();
    const frac = Math.min(1, Math.max(0, (event.clientX - rect.left) / rect.width));
    state.idx = Math.round(frac * (n - 1));
    svg.querySelectorAll(".chart-cursor").forEach((el) => el.remove());
    chartHoverDraw(svg);
  });
  svg.addEventListener("mouseleave", () => {
    if (svg.__chart) svg.__chart.idx = null;
    svg.querySelectorAll(".chart-cursor").forEach((el) => el.remove());
    if (svg.__chartTip) svg.__chartTip.hidden = true;
  });
}

// Mirror filter controls into the URL query string so a filtered view can be
// reloaded or shared by copying the address. replaceState keeps typing from
// piling entries onto the back button.
function urlParamGet(name) {
  return new URLSearchParams(location.search).get(name) || "";
}

function urlParamSet(name, value) {
  const url = new URL(location.href);
  if (value) url.searchParams.set(name, value);
  else url.searchParams.delete(name);
  history.replaceState(history.state, "", url);
}

// Seed a text filter from the URL, then mirror edits back and re-render.
function wireUrlFilter(input, param, onChange) {
  input.value = urlParamGet(param);
  input.addEventListener("input", () => {
    urlParamSet(param, input.value.trim());
    onChange();
  });
}

// ---- live data over server-sent events ----
// One EventSource per page, multiplexing every data family the page shows as
// a single "tick" event (a JSON object keyed by family). Registers with the
// layout's __spaEventSources so boosted navigation closes it. When SSE is
// unavailable or the stream dies for good, the page falls back to polling its
// old fetch-based refresh. Renders hold off while the user interacts, exactly
// like autoRefresh, and the deferred bundle paints once the page is quiet.
function liveData(families, onTick, fallbackRefresh) {
  const fallBack = () => {
    fallbackRefresh();
    autoRefresh(fallbackRefresh, 2000);
  };
  if (typeof EventSource === "undefined") {
    fallBack();
    return;
  }

  const busy = interactionGuard(1000);
  let pending = null;
  let flushTimer = null;
  const paint = (bundle) => {
    const x = window.scrollX;
    const y = window.scrollY;
    try {
      onTick(bundle);
    } catch (_err) {
      // Swallow: the next tick repaints. Matches autoRefresh.
    }
    if (window.scrollX !== x || window.scrollY !== y) {
      window.scrollTo(x, y);
    }
  };
  const render = (bundle) => {
    if (!busy()) {
      pending = null;
      paint(bundle);
      return;
    }
    pending = bundle;
    if (flushTimer) return;
    flushTimer = setInterval(() => {
      if (!pending || busy()) return;
      const bundle = pending;
      pending = null;
      paint(bundle);
    }, 500);
  };

  const es = new EventSource(`/admin/api/events?families=${families.join(",")}`);
  window.__spaEventSources?.add(es);
  es.addEventListener("tick", (event) => {
    // The live pill reads this stamp: a healthy stream keeps it fresh, a
    // reconnecting one lets it age into stale, then dead.
    window.__fibrilLastOk = Date.now();
    let bundle;
    try {
      bundle = JSON.parse(event.data);
    } catch (_err) {
      return;
    }
    render(bundle);
  });
  es.onerror = () => {
    // Transient errors auto-reconnect. CLOSED is final (e.g. an auth
    // redirect): fall back to polling for the rest of this page view.
    if (es.readyState === EventSource.CLOSED) {
      window.__spaEventSources?.delete(es);
      fallBack();
    }
  };
}
