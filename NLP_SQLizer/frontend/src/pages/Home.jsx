import { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import ConnectModal from "../components/ConnectModal.jsx";
import DBCard from "../components/DBCard.jsx";
import {
  getRecent,
  saveRecent,
  setCurrent,
  deleteRecent,
} from "../lib/storage.js";
import { health } from "../lib/api.js";
import { connectionId } from "../lib/dburl.js";

export default function Home() {
  const nav = useNavigate();
  const [showConnect, setShowConnect] = useState(false);
  const [recent, setRecent] = useState(getRecent());
  const [apiHealth, setApiHealth] = useState(null);

  useEffect(() => {
    health()
      .then(setApiHealth)
      .catch(() => {});
  }, []);

  const team = useMemo(
    () => [
      {
        name: "Kartavya Singh [Leader]",
        role: "Backend / Infra",
        email: "singhk6@mail.uc.edu",
      },
      {
        name: "Saarthak Sinha",
        role: "Frontend / Data Viz",
        email: "sinhas6@mail.uc.edu",
      },
      {
        name: "Kanav Shetty",
        role: "Product / QA",
        email: "shettykv@mail.uc.edu",
      },
    ],
    []
  );

  function handleConnected(entry) {
    const updated = saveRecent(entry);
    setRecent(updated);
    setCurrent(entry); // one active connection at a time
    setShowConnect(false);
    nav(`/db/${encodeURIComponent(entry.id)}`);
  }

  function handleRecentClick(db) {
    // If we didn't store a password, we simply route; backend side can prompt/deny later.
    setCurrent(db);
    nav(`/db/${encodeURIComponent(db.id)}`);
  }

  function handleDelete(db) {
    const ok = confirm(`Remove saved connection:\n${db.name || db.id}?`);
    if (!ok) return;
    const updated = deleteRecent(db.id);
    setRecent(updated);
  }

  return (
    <>
      <section className="hero">
        {/* LEFT */}
        <div className="left">
          <h1 className="title">Query your database in plain English.</h1>
          <p className="subtitle">
            NLP_SQLizer turns questions into safe, explainable SQL. Connect a
            DB, see schema insights, and get ready for chat-assisted analysis.
          </p>

          <div className="card" style={{ marginTop: 16 }}>
            <h3>Created by</h3>
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Role</th>
                  <th>Email</th>
                </tr>
              </thead>
              <tbody>
                {team.map((t) => (
                  <tr key={t.email}>
                    <td>{t.name}</td>
                    <td>{t.role}</td>
                    <td>
                      <span className="mono">{t.email}</span>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            <div className="note">
              You can store up to <b>3</b> databases locally. To add a new one,
              delete an older entry.
            </div>
          </div>
        </div>

        {/* RIGHT */}
        <div className="rightStick">
          <div className="card tryBox">
            <div className="bigArrow">↓</div>
            <div style={{ marginBottom: 10, fontWeight: 700 }}>Try it now</div>
            <button onClick={() => setShowConnect(true)}>Get Started</button>
            <div className="note" style={{ marginTop: 10 }}>
              Max 3 saved databases. Active connections:{" "}
              <span className="badge">{recent.length}</span>
            </div>
            {apiHealth && (
              <div className="small" style={{ marginTop: 8, color: "#a7f3d0" }}>
                API: {apiHealth?.ok ? "healthy" : "unavailable"}
              </div>
            )}
          </div>

          <div className="card">
            <h3>Previously connected</h3>
            {recent.length === 0 ? (
              <div className="small">
                Nothing yet. Connect a database to see it here.
              </div>
            ) : (
              <div className="cards">
                {recent.map((db) => (
                  <DBCard
                    key={db.id}
                    db={db}
                    onClick={handleRecentClick}
                    onDelete={handleDelete}
                  />
                ))}
              </div>
            )}
            {recent.length > 0 && (
              <div className="small" style={{ marginTop: 10 }}>
                Only the 3 latest are kept. You’re connected to{" "}
                <span className="mono">
                  {recent[0]?.name || connectionId(recent[0])}
                </span>{" "}
                unless you switch.
              </div>
            )}
          </div>
        </div>
      </section>

      {showConnect && (
        <ConnectModal
          open={showConnect}
          onClose={() => setShowConnect(false)}
          onConnected={handleConnected}
        />
      )}
    </>
  );
}
