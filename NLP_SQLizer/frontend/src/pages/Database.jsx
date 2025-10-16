import { useEffect, useState } from "react";
import { useNavigate, useParams, Link } from "react-router-dom";
import { getCurrent } from "../lib/storage.js";
import { schemaOverview } from "../lib/api.js";
import { buildUrl } from "../lib/dburl.js";
import ProgressStrip from "../components/ProgressStrip.jsx";

export default function Database() {
  const { id } = useParams();
  const nav = useNavigate();
  const [conn, setConn] = useState(getCurrent());
  const [schema, setSchema] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");

  useEffect(() => {
    if (!conn || conn.id !== decodeURIComponent(id)) {
      setErr("No active connection. Returning home…");
      const t = setTimeout(() => nav("/"), 1200);
      return () => clearTimeout(t);
    }

    const payload = conn.partsSansPass?.DB_PASSWORD
      ? { parts: conn.partsSansPass }
      : conn.urlMasked
      ? { url: conn.urlMasked } // best effort; backend may ignore password
      : null;

    setLoading(true);
    schemaOverview(payload)
      .then((res) => {
        if (res?.ok) setSchema(res);
        else setErr(res?.error || "Failed to load schema");
      })
      .catch((e) => setErr(String(e)))
      .finally(() => setLoading(false));
  }, [id]);

  if (!conn) return null;

  return (
    <div
      className="grid"
      style={{ gridTemplateColumns: "1.2fr 0.8fr", gap: 18, padding: 24 }}
    >
      <div>
        <div className="card">
          <h2 style={{ marginTop: 0 }}>
            {conn.name}
            <span className="badge">{conn.type?.split("+")[0] || "DB"}</span>
          </h2>
          <div className="kv">
            <div>Host</div>
            <div className="mono">{conn.host}</div>
            <div>Database</div>
            <div className="mono">{conn.database}</div>
          </div>
        </div>

        <div className="card">
          <h3>Tables & Columns</h3>
          {loading && <div className="small">Loading schema…</div>}
          {err && (
            <div className="small" style={{ color: "#fca5a5" }}>
              {err}
            </div>
          )}
          {!loading && schema?.tables?.length === 0 && (
            <div className="small">No tables found.</div>
          )}
          {!loading && schema?.tables?.length > 0 && (
            <div className="grid">
              {schema.tables.map((t) => (
                <details key={t.table} className="details card" open>
                  <summary>
                    {t.table}{" "}
                    <span className="badge">{t.columns?.length || 0} cols</span>
                  </summary>
                  <div style={{ marginTop: 10 }}>
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Column</th>
                          <th>Type</th>
                          <th>Nullable</th>
                        </tr>
                      </thead>
                      <tbody>
                        {t.columns.map((c) => (
                          <tr key={c.name}>
                            <td className="mono">{c.name}</td>
                            <td>{c.type}</td>
                            <td>{String(c.nullable)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </details>
              ))}
            </div>
          )}
        </div>
      </div>

      <div className="rightStick">
        <div className="card">
          <h3>Indexing & Readiness</h3>
          <ProgressStrip value={conn.indexPct ?? 0} label="Indexing progress" />
          <div style={{ height: 10 }} />
          <ProgressStrip value={conn.trainPct ?? 0} label="Training progress" />
          <div className="note" style={{ marginTop: 10 }}>
            Placeholder metrics. Real indexing/learning will light this up.
          </div>
        </div>

        <div className="card">
          <h3>Chat</h3>
          <div className="note">
            Start a chat once the database is ready. For now, this navigates to
            a placeholder.
          </div>
          <button
            onClick={() => nav(`/chat/${encodeURIComponent(conn.id)}`)}
            style={{ marginTop: 8 }}
          >
            Start Chat
          </button>
        </div>

        <div className="card">
          <h3>Actions</h3>
          <div className="grid two">
            <button className="secondary" onClick={() => nav("/")}>
              Switch Database
            </button>
            <button className="secondary" onClick={() => location.reload()}>
              Refresh Schema
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}
