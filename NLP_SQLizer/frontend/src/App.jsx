import { useEffect, useState } from "react";

function App() {
  const [status, setStatus] = useState("checking…");

  useEffect(() => {
    const url = `${import.meta.env.VITE_API_URL}/healthz`;
    fetch(url)
      .then((r) => r.json())
      .then((data) => setStatus(JSON.stringify(data)))
      .catch((e) => setStatus(`error: ${e}`));
  }, []);

  return (
    <div style={{ fontFamily: "system-ui", padding: 24 }}>
      <h1>NLP_SQLizer — Frontend</h1>
      <p>Backend /healthz →</p>
      <pre>{status}</pre>
    </div>
  );
}

export default App;
