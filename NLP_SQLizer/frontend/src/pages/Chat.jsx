import { useParams, Link } from "react-router-dom";

export default function Chat() {
  const { id } = useParams();
  return (
    <div style={{ padding: 24 }}>
      <div className="card">
        <h2 style={{ marginTop: 0 }}>Chat — coming soon</h2>
        <div className="note">
          Connected DB ID:{" "}
          <span className="mono">{decodeURIComponent(id)}</span>
        </div>
        <div style={{ height: 10 }} />
        <Link to="/" className="small">
          ← Back to Home
        </Link>
      </div>
    </div>
  );
}
