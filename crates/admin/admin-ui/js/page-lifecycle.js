// Shared page resources for the live dashboard and generated demo. The shell
// survives boosted navigation. Page resources are disposed before its next swap.
(() => {
  window.__nativeSetInterval = window.setInterval.bind(window);
  window.__nativeClearInterval = window.clearInterval.bind(window);
  window.__nativeSetTimeout = window.setTimeout.bind(window);
  window.__nativeClearTimeout = window.clearTimeout.bind(window);
  window.__spaIntervals = new Set();
  window.__spaTimeouts = new Set();
  window.__spaEventSources = new Set();
  window.__spaVisibilityKicks = new Set();
  window.__spaDisposers = new Set();

  window.__disposePage = () => {
    for (const dispose of window.__spaDisposers) dispose();
    window.__spaDisposers.clear();
    for (const id of window.__spaIntervals) window.__nativeClearInterval(id);
    window.__spaIntervals.clear();
    for (const id of window.__spaTimeouts) window.__nativeClearTimeout(id);
    window.__spaTimeouts.clear();
    for (const source of window.__spaEventSources) source.close();
    window.__spaEventSources.clear();
    window.__spaVisibilityKicks.clear();
  };
  document.addEventListener('visibilitychange', () => {
    if (document.hidden) return;
    for (const kick of window.__spaVisibilityKicks) kick();
  });
  window.setInterval = (fn, ms, ...args) => {
    const id = window.__nativeSetInterval(fn, ms, ...args);
    window.__spaIntervals.add(id);
    return id;
  };
  window.clearInterval = id => {
    window.__spaIntervals.delete(id);
    window.__nativeClearInterval(id);
  };
  window.setTimeout = (fn, ms, ...args) => {
    // Browser timers also accept strings. Preserve that behavior without adding
    // an eval boundary here. Page scripts use function callbacks.
    if (typeof fn !== 'function') {
      const id = window.__nativeSetTimeout(fn, ms, ...args);
      window.__spaTimeouts.add(id);
      return id;
    }
    const id = window.__nativeSetTimeout(() => {
      window.__spaTimeouts.delete(id);
      fn.apply(window, args);
    }, ms);
    window.__spaTimeouts.add(id);
    return id;
  };
  window.clearTimeout = id => {
    window.__spaTimeouts.delete(id);
    window.__nativeClearTimeout(id);
  };
})();
