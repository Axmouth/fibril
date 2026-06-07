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
