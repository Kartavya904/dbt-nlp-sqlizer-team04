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
        const entry = {
          id,
          name: parsed?.DB_NAME || id,
          type: parsed?.DB_DRIVER,
          host: parsed?.DB_HOST,
          database: parsed?.DB_NAME,
          urlMasked: res.url || currentUrl.replace(/:\/\/[^@]+@/, "://*****@"),
          partsSansPass: { ...parsed, DB_PASSWORD: undefined },
          indexPct: 0,
          trainPct: 0,
          ts: Date.now(),
        };
        onConnected?.(entry);
      } else {
        setMsg(res?.error || "Connection failed");
      }
    } catch (e) {
      setMsg(String(e));
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
              Example:{" "}
              <span className="mono">
                postgresql+psycopg://user:pass@localhost:5432/yourdb
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
                  onChange={(e) =>
                    setParts({ ...parts, DB_DRIVER: e.target.value })
                  }
                >
                  <option value="postgresql+psycopg">
                    PostgreSQL (psycopg)
                  </option>
                  <option value="mysql+pymysql">MySQL (PyMySQL)</option>
                  <option value="sqlite">SQLite</option>
                </select>
              </div>
              <div>
                <label className="small">Host</label>
                <input
                  className="input"
                  value={parts.DB_HOST}
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
                  onChange={(e) =>
                    setParts({ ...parts, DB_PORT: Number(e.target.value) })
                  }
                />
              </div>
              <div>
                <label className="small">Database</label>
                <input
                  className="input"
                  value={parts.DB_NAME}
                  onChange={(e) =>
                    setParts({ ...parts, DB_NAME: e.target.value })
                  }
                />
              </div>
            </div>
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
