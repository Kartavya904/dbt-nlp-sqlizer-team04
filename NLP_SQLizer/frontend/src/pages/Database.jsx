import { useEffect, useState, useRef } from "react";
import { useNavigate, useParams, Link } from "react-router-dom";
import { getCurrent, setCurrent } from "../lib/storage.js";
import { schemaOverview, getSchemaId, getModelStatus, getTrainingProgress, trainModel } from "../lib/api.js";
import { buildUrl } from "../lib/dburl.js";
import ProgressStrip from "../components/ProgressStrip.jsx";

export default function Database() {
  const { id } = useParams();
  const nav = useNavigate();
  const [conn, setConn] = useState(getCurrent());
  const [schema, setSchema] = useState(null);
  const [loading, setLoading] = useState(true);
  const [err, setErr] = useState("");
  const [schemaId, setSchemaId] = useState(null);
  const [modelStatus, setModelStatus] = useState(null);
  const [trainingProgress, setTrainingProgress] = useState(null);
  const [indexProgress, setIndexProgress] = useState(0);
  const [trainProgress, setTrainProgress] = useState(0);
  const progressIntervalRef = useRef(null);

  // Check model status and auto-train
  useEffect(() => {
    const decodedId = decodeURIComponent(id);
    
    // Reset state when switching databases
    setSchema(null);
    setSchemaId(null);
    setModelStatus(null);
    setTrainingProgress(null);
    setIndexProgress(0);
    setTrainProgress(0);
    setErr("");
    
    if (!conn || conn.id !== decodedId) {
      setErr("No active connection. Returning home…");
      const t = setTimeout(() => nav("/"), 1200);
      return () => clearTimeout(t);
    }

    // Try to get full URL from sessionStorage (has password)
    const fullUrl = sessionStorage.getItem(`db_url_${conn.id}`);
    
    // Build payload: prefer full URL from sessionStorage, then parts, then masked URL
    let payload = null;
    if (fullUrl) {
      // Use full URL with password from sessionStorage
      payload = { url: fullUrl };
    } else if (conn.partsSansPass) {
      // Use parts (but password will be missing - may fail if DB requires it)
      payload = { parts: conn.partsSansPass };
    } else if (conn.urlMasked) {
      // Masked URL won't work (has ***** instead of password)
      // This will likely fail, but we try it anyway
      payload = { url: conn.urlMasked };
    }

    if (!payload) {
      setErr("Invalid connection - missing connection details");
      setLoading(false);
      return;
    }

    setLoading(true);
    
    // Load schema
    schemaOverview(payload)
      .then((res) => {
        if (res?.ok) {
          setSchema(res);
          setIndexProgress(100); // Schema loaded = indexing complete
          
          // Update connection name if we got database info from schema
          // For MongoDB, use the database name from the response
          if (res.dialect === "mongodb") {
            const dbName = res.database;
            if (dbName && dbName !== "admin") {
              // Update the connection name if it's better than current
              const currentName = conn.name || "";
              if (!currentName || currentName === conn.host || currentName.includes("mongodb") || currentName.toLowerCase() === "mongodb") {
                setConn({
                  ...conn,
                  name: dbName,
                  database: dbName,
                });
              }
            } else if (res.tables && res.tables.length > 0) {
              // Fallback: Try to extract database name from table names (format: "dbname.collection")
              const firstTable = res.tables[0]?.table || "";
              if (firstTable.includes(".")) {
                const extractedDbName = firstTable.split(".")[0];
                if (extractedDbName && extractedDbName !== "admin") {
                  const currentName = conn.name || "";
                  if (!currentName || currentName === conn.host || currentName.includes("mongodb") || currentName.toLowerCase() === "mongodb") {
                    setConn({
                      ...conn,
                      name: extractedDbName,
                      database: extractedDbName,
                    });
                  }
                }
              }
            }
          }
        } else {
          setErr(res?.error || "Failed to load schema");
        }
      })
      .catch((e) => setErr(String(e)))
      .finally(() => setLoading(false));

    // Get schema ID and check model status
    getSchemaId(payload)
      .then((sid) => {
        setSchemaId(sid);
        return getModelStatus(sid);
      })
      .then((status) => {
        setModelStatus(status);
        
        if (status.status === "not_found") {
          // Auto-train if model doesn't exist
          return trainModel(payload, { use_llm_for_training: true })
            .then((trainRes) => {
              if (trainRes.status === "training") {
                const sid = trainRes.schema_id || schemaId;
                setSchemaId(sid);
                // Start polling for progress
                startProgressPolling(sid);
              }
            });
        } else if (status.status === "training") {
          // Start polling for progress
          startProgressPolling(status.schema_id);
        } else if (status.status === "ready") {
          setTrainProgress(100);
        }
      })
      .catch((e) => {
        console.error("Model status check failed:", e);
      });
  }, [id]);

  // Poll for training progress
  const startProgressPolling = (sid) => {
    if (!sid) return;
    if (progressIntervalRef.current) {
      clearInterval(progressIntervalRef.current);
    }
    
    const poll = async () => {
      try {
        const progress = await getTrainingProgress(sid);
        if (progress.ok) {
          setTrainingProgress(progress);
          
          if (progress.status === "training") {
            const overall = progress.overall_progress || 0;
            setTrainProgress(overall);
          } else if (progress.status === "completed") {
            setTrainProgress(100);
            setModelStatus({ status: "ready", schema_id: sid });
            if (progressIntervalRef.current) {
              clearInterval(progressIntervalRef.current);
              progressIntervalRef.current = null;
            }
          }
        }
      } catch (e) {
        console.error("Progress poll error:", e);
      }
    };
    
    poll(); // Immediate poll
    progressIntervalRef.current = setInterval(poll, 1000); // Poll every second
  };

  useEffect(() => {
    return () => {
      if (progressIntervalRef.current) {
        clearInterval(progressIntervalRef.current);
      }
    };
  }, []);

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
          <ProgressStrip value={indexProgress} label="Schema indexing" />
          <div style={{ height: 10 }} />
          <ProgressStrip value={trainProgress} label="AI model training" />
          <div className="note" style={{ marginTop: 10 }}>
            {modelStatus?.status === "training" && (
              <div>
                <div style={{ color: "#93c5fd", marginBottom: 4 }}>
                  ⚙️ Training in progress...
                </div>
                {trainingProgress?.stages && (
                  <div style={{ fontSize: "11px", opacity: 0.8 }}>
                    {Object.entries(trainingProgress.stages).map(([stage, info]) => (
                      <div key={stage} style={{ marginTop: 4 }}>
                        {stage.replace(/_/g, " ")}: {Math.round(info.progress || 0)}%
                        {info.message && ` - ${info.message}`}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
            {modelStatus?.status === "ready" && (
              <div style={{ color: "#86efac" }}>
                ✓ Model ready for queries
              </div>
            )}
            {modelStatus?.status === "not_found" && (
              <div style={{ color: "#fbbf24" }}>
                ⏳ Initializing training...
              </div>
            )}
          </div>
        </div>

        <div className="card">
          <h3>Chat</h3>
          {modelStatus?.status === "training" ? (
            <>
              <div className="note" style={{ color: "#fbbf24" }}>
                ⏳ AI model is being trained. Chat will be available once training completes.
              </div>
              <button disabled style={{ marginTop: 8, opacity: 0.5 }}>
                Training in Progress...
              </button>
            </>
          ) : modelStatus?.status === "ready" ? (
            <>
              <div className="note">
                Start asking questions about your database in natural language.
              </div>
              <button
                onClick={() => nav(`/chat/${encodeURIComponent(conn.id)}`)}
                style={{ marginTop: 8 }}
              >
                Start Chat
              </button>
            </>
          ) : (
            <>
              <div className="note">
                Preparing database connection...
              </div>
              <button disabled style={{ marginTop: 8, opacity: 0.5 }}>
                Loading...
              </button>
            </>
          )}
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
