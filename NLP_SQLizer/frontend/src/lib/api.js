const API = import.meta.env.VITE_API_URL;

// Call backend. If POST /... with URL is unsupported, gracefully fall back to plain GET.
async function tryPostOrGet(path, body) {
  // Prefer POST with JSON body
  try {
    const r = await fetch(`${API}${path}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
    if (r.ok) return await r.json();
  } catch {
    console.error("POST failed, falling back to GET", path, body);
  }
  // Fallback to GET (uses server's current engine)
  const r2 = await fetch(`${API}${path}`);
  return await r2.json();
}

export async function health() {
  const r = await fetch(`${API}/healthz`);
  return r.json();
}

/**
 * Test a connection. Accepts { url } or { parts }.
 * Returns: { ok, dialect, url?, error? }
 */
export async function connectTest(payload) {
  return tryPostOrGet("/connect/test", payload);
}

/**
 * Get schema overview for a connection. Accepts same payload.
 * Returns: { ok, dialect, tables: [{table, columns:[{name,type,nullable}]}] }
 */
export async function schemaOverview(payload) {
  return tryPostOrGet("/schema/overview", payload);
}
