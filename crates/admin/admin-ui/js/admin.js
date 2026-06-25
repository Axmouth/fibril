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
