async function api(path) {
  const r = await fetch(path);
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
  const r = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  let data = null;
  try {
    data = await r.json();
  } catch {
    data = null;
  }
  return { ok: r.ok, data };
}
