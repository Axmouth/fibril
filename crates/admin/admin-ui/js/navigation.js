(() => {
  if (!window.history || !window.history.pushState) return;

  const loadedSrcs = new Set(
    [...document.querySelectorAll("script[src]")].map((s) => s.src),
  );


  let navigation = 0;
  let pendingRequest;
  const loadingScripts = new Map();

  function setActiveNav(pathname) {
    document.querySelectorAll(".nav-link").forEach((link) => {
      link.classList.toggle("active", link.getAttribute("href") === pathname);
    });
  }

  // innerHTML never runs <script> tags, so recreate them. External scripts
  // are loaded once and awaited so their globals are ready; inline scripts
  // run inside an IIFE so a page can be revisited without redeclaring its
  // top-level bindings.
  async function runScripts(container, isCurrent) {
    for (const old of [...container.querySelectorAll("script")]) {
      if (!isCurrent()) return;
      const fresh = document.createElement("script");
      for (const attr of old.attributes) fresh.setAttribute(attr.name, attr.value);
      if (old.src) {
        if (loadedSrcs.has(fresh.src)) {
          old.remove();
          continue;
        }
        let loading = loadingScripts.get(fresh.src);
        if (!loading) {
          loading = new Promise((resolve, reject) => {
            fresh.onload = () => { loadedSrcs.add(fresh.src); resolve(); };
            fresh.onerror = () => reject(new Error(`Could not load ${fresh.src}`));
            // Keep shared dependencies outside the replaceable page while loading.
            document.head.append(fresh);
          });
          loadingScripts.set(fresh.src, loading);
        }
        old.remove();
        try { await loading; }
        finally { loadingScripts.delete(fresh.src); }
      } else {
        fresh.textContent = `(function(){\n${old.textContent}\n})();`;
        old.replaceWith(fresh);
      }
    }
  }

  async function navigate(url, push) {
    const id = ++navigation;
    const isCurrent = () => id === navigation;
    pendingRequest?.abort();
    pendingRequest = new AbortController();
    const current = document.getElementById("page-root");
    if (!current) { window.location.href = url; return; }
    current.classList.add("swapping");
    try {
      const res = await fetch(url, {
        headers: { "X-Requested-With": "fetch" }, signal: pendingRequest.signal,
      });
      if (!isCurrent()) return;
      const type = res.headers.get("content-type") || "";
      if (!res.ok || res.redirected || !type.includes("text/html")) {
        window.location.href = res.redirected ? res.url : url;
        return;
      }
      const html = await res.text();
      if (!isCurrent()) return;
      const doc = new DOMParser().parseFromString(html, "text/html");
      const next = doc.getElementById("page-root");
      if (!next) { window.location.href = url; return; }
      window.__disposePage();
      const title = doc.querySelector("title");
      if (title) {
        document.title = title.textContent;
        const crumb = document.querySelector(".crumb");
        if (crumb) crumb.textContent = title.textContent;
      }
      current.innerHTML = next.innerHTML;
      // Page initializers read query parameters, so update the URL first.
      if (push) window.history.pushState({ spa: true }, "", url);
      await runScripts(current, isCurrent);
      if (!isCurrent()) return;
      current.classList.remove("swapping");
      setActiveNav(new URL(url, window.location.origin).pathname);
      window.scrollTo(0, 0);
    } catch {
      // A superseded request must never redirect the newer page, even if abort
      // arrives too late. Current transport or script failures use a full load.
      if (isCurrent()) window.location.href = url;
    }
  }

  function boostable(anchor) {
    if (!anchor) return false;
    if (anchor.target && anchor.target !== "_self") return false;
    if (anchor.hasAttribute("download") || anchor.dataset.noBoost !== undefined) return false;
    const url = new URL(anchor.href, window.location.href);
    if (url.origin !== window.location.origin) return false;
    if (url.pathname === "/logout") return false;
    return true;
  }

  document.addEventListener("click", (event) => {
    if (event.defaultPrevented || event.button !== 0) return;
    if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
    const anchor = event.target.closest("a[href]");
    if (!boostable(anchor)) return;
    event.preventDefault();
    navigate(anchor.href, true);
  });

  window.addEventListener("popstate", () => navigate(window.location.href, false));
})();
