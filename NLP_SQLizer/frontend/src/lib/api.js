const API = import.meta.env.VITE_API_URL || "http://127.0.0.1:8000";

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

export async function aiAsk(question, connection, opts = {}) {
  const body = { question, connection, ...opts };
  const r = await fetch(`${API}/ai/ask`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.json()).detail || "Request failed");
  return r.json();
}

/**
 * Get schema ID from connection
 */
export async function getSchemaId(connection) {
  const r = await fetch(`${API}/models/schema-id`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ connection }),
  });
  if (!r.ok) throw new Error("Failed to get schema ID");
  const data = await r.json();
  return data.schema_id;
}

/**
 * Train a model for a schema
 */
export async function trainModel(connection, opts = {}) {
  const body = { connection, ...opts };
  const r = await fetch(`${API}/models/train`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error((await r.json()).detail || "Training failed");
  return r.json();
}

/**
 * Get model status
 */
export async function getModelStatus(schemaId) {
  const r = await fetch(`${API}/models/${schemaId}/status`);
  if (!r.ok) return { ok: false, status: "error" };
  return r.json();
}

/**
 * Get training progress
 */
export async function getTrainingProgress(schemaId) {
  const r = await fetch(`${API}/models/${schemaId}/progress`);
  if (!r.ok) return { ok: false, status: "error" };
  return r.json();
}
