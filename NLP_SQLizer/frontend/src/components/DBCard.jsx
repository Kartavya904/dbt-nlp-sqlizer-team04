import ProgressStrip from "./ProgressStrip.jsx";
import { driverPretty } from "../lib/dburl.js";

export default function DBCard({ db, onClick, onDelete }) {
  function handleDelete(e) {
    e.stopPropagation(); // don't open card
    if (onDelete) onDelete(db);
  }

  return (
    <div
      className="card"
      style={{
        display: "grid",
        gridTemplateColumns: "1fr auto",
        gap: 12,
        cursor: "pointer",
      }}
      onClick={() => onClick?.(db)}
    >
      {/* X button */}
      <button
        className="iconBtn"
        title="Remove from saved"
        aria-label="Remove saved connection"
        onClick={handleDelete}
      >
        ×
      </button>

      <div>
        <div style={{ fontSize: 18, fontWeight: 800 }}>{db.name || db.id}</div>
        <div className="small">
          {driverPretty(db.type)} • {db.host || "localhost"}
        </div>
        {db.database && <div className="small mono">{db.database}</div>}
      </div>

      <div style={{ width: "auto" }}>
        <ProgressStrip value={db.indexPct ?? 0} label="Indexing" />
        <div style={{ height: 8 }} />
        <ProgressStrip value={db.trainPct ?? 0} label="Training" />
      </div>
    </div>
  );
}
