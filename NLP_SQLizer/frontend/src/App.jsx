import { useEffect, useState } from "react";

const API = import.meta.env.VITE_API_URL;

export default function App() {
  const [health, setHealth] = useState("checking…");
  const [connect, setConnect] = useState(null);
  const [schema, setSchema] = useState(null);

  useEffect(() => {
    if (!API) {
      setHealth("VITE_API_URL missing");
      return;
    }
    fetch(`${API}/healthz`)
      .then((r) => r.json())
      .then((d) => setHealth(JSON.stringify(d)));
  }, []);

  const testConnect = async () => {
    const d = await fetch(`${API}/connect/test`).then((r) => r.json());
    setConnect(d);
  };

  const loadSchema = async () => {
    const d = await fetch(`${API}/schema/overview`).then((r) => r.json());
    setSchema(d);
  };

  return (
    <div style={{ fontFamily: "system-ui", padding: 24, maxWidth: 900 }}>
      <h1>NLP_SQLizer — Frontend</h1>

      <p>
        <b>Backend /healthz →</b>
      </p>
      <pre>{health}</pre>

      <div style={{ display: "flex", gap: 12, margin: "12px 0" }}>
        <button onClick={testConnect}>Test DB connection</button>
        <button onClick={loadSchema}>Load schema overview</button>
      </div>

      {connect && (
        <>
          <h3>Connect result</h3>
          <pre>{JSON.stringify(connect, null, 2)}</pre>
        </>
      )}

      {schema && (
        <>
          <h3>Schema overview</h3>
          <pre>{JSON.stringify(schema, null, 2)}</pre>
        </>
      )}
    </div>
  );
}
