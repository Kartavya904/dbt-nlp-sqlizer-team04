import { useEffect, useState } from "react";
import {
  buildUrl,
  parseUrl,
  connectionId,
  driverPretty,
} from "../lib/dburl.js";
import { connectTest } from "../lib/api.js";

export default function ConnectModal({ open, onClose, onConnected }) {
  const [tab, setTab] = useState("url"); // "url" | "form"
  const [url, setUrl] = useState(
    "postgresql+psycopg://postgres:@localhost:5432/postgres"
  );
  const [parts, setParts] = useState({
    DB_DRIVER: "postgresql+psycopg",
    DB_HOST: "localhost",
    DB_PORT: 5432,
    DB_NAME: "postgres",
    DB_USER: "postgres",
    DB_PASSWORD: "",
  });
  
  // Update default port when driver changes
  const handleDriverChange = (driver) => {
    const defaultPorts = {
      "postgresql+psycopg": 5432,
      "mysql+pymysql": 3306,
      "mongodb": 27017,
      "sqlite+pysqlite": "",
      "mssql+pyodbc": 1433,
      "oracle+cx_oracle": 1521,
    };
    setParts({
      ...parts,
      DB_DRIVER: driver,
      DB_PORT: defaultPorts[driver] || parts.DB_PORT,
    });
  };
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("");

  useEffect(() => {
    if (!open) setMsg("");
  }, [open]);

  const currentUrl = tab === "url" ? url : buildUrl(parts);

  async function handleConnect() {
    setBusy(true);
    setMsg("Testing connection…");
    const payload = tab === "url" ? { url: currentUrl } : { parts };
    try {
      const res = await connectTest(payload);
      if (res?.ok) {
        setMsg("Connected ✔");
        const parsed = parseUrl(currentUrl);
        const id = connectionId(parsed);
        // Store full URL with password in sessionStorage for this session
        sessionStorage.setItem(`db_url_${id}`, currentUrl);
        
        // Create a better display name
        let displayName = parsed?.DB_NAME;
        if (!displayName || displayName === "") {
          // For MongoDB or when DB_NAME is empty, use host/cluster name
          if (parsed?.DB_DRIVER?.includes("mongodb")) {
            // Extract cluster name from MongoDB host (e.g., "cluster.ivz1b.mongodb.net" -> "cluster")
            const host = parsed?.DB_HOST || "";
            const clusterName = host.split(".")[0] || host;
            displayName = clusterName || "MongoDB";
          } else {
            // For other databases, use host:database or just host
            displayName = parsed?.DB_HOST || "Database";
          }
        }
        
        const entry = {
          id,
          name: displayName,
          type: parsed?.DB_DRIVER,
          host: parsed?.DB_HOST,
          database: parsed?.DB_NAME || displayName,
          urlMasked: res.url || currentUrl.replace(/:\/\/[^@]+@/, "://*****@"),
          partsSansPass: { ...parsed, DB_PASSWORD: undefined },
          indexPct: 0,
          trainPct: 0,
          ts: Date.now(),
        };
        onConnected?.(entry);
      } else {
        setMsg(res?.error || res?.detail || "Connection failed");
      }
    } catch (e) {
      // Extract error message from response if available
      const errorMsg = e.message || String(e);
      setMsg(errorMsg);
    } finally {
      setBusy(false);
    }
  }

  if (!open) return null;

  return (
    <div className="modalBack" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <h2 style={{ margin: "4px 0 10px" }}>Connect to a Database</h2>
        <div className="small" style={{ marginBottom: 12 }}>
          Two methods: paste a full connection URL or fill the form.
        </div>

        <div className="tabs">
          <button
            className={tab === "url" ? "" : "secondary"}
            onClick={() => setTab("url")}
          >
            Method 1 — URL
          </button>
          <button
            className={tab === "form" ? "" : "secondary"}
            onClick={() => setTab("form")}
          >
            Method 2 — Form
          </button>
        </div>

        {tab === "url" ? (
          <div className="grid">
            <label className="small">Database URL</label>
            <input
              className="input mono"
              value={url}
              onChange={(e) => setUrl(e.target.value)}
            />
            <div className="small">
              Examples:{" "}
              <span className="mono">
                postgresql+psycopg://user:pass@localhost:5432/yourdb
                <br />
                mysql+pymysql://user:pass@localhost:3306/yourdb
                <br />
                mongodb://user:pass@localhost:27017/dbname
                <br />
                mongodb+srv://user:pass@cluster.mongodb.net/dbname
                <br />
                sqlite:///path/to/database.db
              </span>
            </div>
            {/* If URL lacks password, show a password field to collect it */}
            {(() => {
              const p = parseUrl(url);
              if (p && !p.DB_PASSWORD) {
                return (
                  <div className="grid">
                    <label className="small">
                      Password (URL has no password)
                    </label>
                    <input
                      className="input"
                      type="password"
                      onChange={(e) => {
                        const withPass = { ...p, DB_PASSWORD: e.target.value };
                        setUrl(buildUrl(withPass));
                      }}
                    />
                  </div>
                );
              }
              return null;
            })()}
          </div>
        ) : (
          <div className="grid">
            <div className="row">
              <div>
                <label className="small">Driver</label>
                <select
                  className="select"
                  value={parts.DB_DRIVER}
                  onChange={(e) => handleDriverChange(e.target.value)}
                >
                  <option value="postgresql+psycopg">
                    PostgreSQL (psycopg)
                  </option>
                  <option value="mysql+pymysql">MySQL (PyMySQL)</option>
                  <option value="mongodb">MongoDB</option>
                  <option value="mssql+pyodbc">SQL Server (pyodbc)</option>
                  <option value="oracle+cx_oracle">Oracle (cx_Oracle)</option>
                  <option value="sqlite+pysqlite">SQLite</option>
                </select>
              </div>
              <div>
                <label className="small">Host</label>
                <input
                  className="input"
                  value={parts.DB_HOST}
                  placeholder={parts.DB_DRIVER.includes("sqlite") ? "N/A for SQLite" : ""}
                  disabled={parts.DB_DRIVER.includes("sqlite")}
                  onChange={(e) =>
                    setParts({ ...parts, DB_HOST: e.target.value })
                  }
                />
              </div>
            </div>
            <div className="row">
              <div>
                <label className="small">Port</label>
                <input
                  className="input"
                  type="number"
                  value={parts.DB_PORT}
                  placeholder={parts.DB_DRIVER.includes("sqlite") ? "N/A for SQLite" : ""}
                  disabled={parts.DB_DRIVER.includes("sqlite")}
                  onChange={(e) =>
                    setParts({ ...parts, DB_PORT: e.target.value ? Number(e.target.value) : "" })
                  }
                />
              </div>
              <div>
                <label className="small">
                  {parts.DB_DRIVER.includes("sqlite") ? "Database Path" : "Database"}
                </label>
                <input
                  className="input"
                  value={parts.DB_NAME}
                  placeholder={parts.DB_DRIVER.includes("sqlite") ? "/path/to/database.db" : ""}
                  onChange={(e) =>
                    setParts({ ...parts, DB_NAME: e.target.value })
                  }
                />
              </div>
            </div>
            {!parts.DB_DRIVER.includes("sqlite") && (
              <div className="row">
                <div>
                  <label className="small">User</label>
                  <input
                    className="input"
                    value={parts.DB_USER}
                    onChange={(e) =>
                      setParts({ ...parts, DB_USER: e.target.value })
                    }
                  />
                </div>
                <div>
                  <label className="small">Password</label>
                  <input
                    className="input"
                    type="password"
                    value={parts.DB_PASSWORD}
                    onChange={(e) =>
                      setParts({ ...parts, DB_PASSWORD: e.target.value })
                    }
                  />
                </div>
              </div>
            )}
            <div className="small mono">URL preview: {buildUrl(parts)}</div>
          </div>
        )}

        {msg && (
          <div className="small" style={{ color: "#93c5fd", marginTop: 8 }}>
            {msg}
          </div>
        )}

        <div className="actions">
          <button className="secondary" onClick={onClose}>
            Close
          </button>
          <button onClick={handleConnect} disabled={busy || !currentUrl}>
            {busy ? "Connecting…" : "Connect to Database"}
          </button>
        </div>

        <div className="small" style={{ marginTop: 8 }}>
          Note: you can save up to <b>3</b> databases locally (browser cache).
          Remove one to add another.
        </div>
      </div>
    </div>
  );
}
