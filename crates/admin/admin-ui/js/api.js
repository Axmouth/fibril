async function api(path) {
  let r;
  try {
    r = await fetch(path);
  } catch (err) {
    // A network-level failure is the signal that the broker itself is
    // unreachable - the live pill separates this from polling merely
    // being paused.
    window.__fibrilLastFail = Date.now();
    throw err;
  }
  // Any HTTP response proves the broker is reachable, error statuses
  // included - liveness and per-request success are different questions.
  window.__fibrilLastOk = Date.now();
  if (!r.ok) {
    let body = "";
    try {
      body = await r.text();
    } catch {
      body = "";
    }
    throw new Error(body || `API error: HTTP ${r.status}`);
  }
  return r.json();
}

// POST JSON and return { ok, data } without throwing, so callers can show a
// message either way. Mirrors the per-page postJson used on the topology page.
async function apiPost(path, body) {
  let r;
  try {
    r = await fetch(path, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch {
    window.__fibrilLastFail = Date.now();
    return { ok: false, data: null };
  }
  window.__fibrilLastOk = Date.now();
  let data = null;
  try {
    data = await r.json();
  } catch {
    data = null;
  }
  return { ok: r.ok, data };
}
