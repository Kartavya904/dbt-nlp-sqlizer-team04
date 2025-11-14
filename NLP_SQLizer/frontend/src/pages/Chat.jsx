import { useEffect, useMemo, useRef, useState } from "react";
import { useParams, Link } from "react-router-dom";

// Simple API helper (uses env if present, otherwise same-origin dev default)
const API =
  import.meta?.env?.VITE_API_BASE?.replace(/\/$/, "") ||
  "http://127.0.0.1:8000";

function classNames(...xs) {
  return xs.filter(Boolean).join(" ");
}

function MessageBubble({ m }) {
  return (
    <div
      className={classNames(
        "bubble",
        m.role === "user" ? "bubble-user" : "bubble-assistant"
      )}
    >
      {m.role === "assistant" && m.sql ? (
        <>
          <div className="sql-label">Proposed SQL</div>
          <pre className="sql">{m.sql}</pre>
          {m.explain && (
            <>
              <div className="sql-label">Plan (EXPLAIN)</div>
              <pre className="explain">{m.explain}</pre>
            </>
          )}
          {m.columns?.length ? (
            <>
              <div className="sql-label">Results</div>
              <div className="result-table-wrap">
                <table className="result-table">
                  <thead>
                    <tr>
                      {m.columns.map((c) => (
                        <th key={c}>{c}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {m.rows.map((row, i) => (
                      <tr key={i}>
                        {row.map((v, j) => (
                          <td key={j}>
                            {v === null || v === undefined ? "—" : String(v)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </>
          ) : null}
        </>
      ) : (
        <div style={{ whiteSpace: "pre-wrap" }}>{m.content}</div>
      )}
    </div>
  );
}

export default function Chat() {
  const { id } = useParams(); // encoded DB connection ID
  const connectionId = decodeURIComponent(id || "");
  
  // Get full URL from sessionStorage (has password) or fall back to connection ID
  const connectionUrl = useMemo(() => {
    const fullUrl = sessionStorage.getItem(`db_url_${connectionId}`);
    return fullUrl || connectionId;
  }, [connectionId]);

  // Chat state
  const [messages, setMessages] = useState([
    {
      role: "assistant",
      content:
        "Ask me things like:\n• Top 5 categories by total revenue\n• Count customers by gender\n• Average age of Electronics buyers\nI’ll propose safe SQL with LIMIT and run it after a quick plan check.",
    },
  ]);
  const [input, setInput] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const scrollRef = useRef(null);

  // Mic state (Web Speech API)
  const [listening, setListening] = useState(false);
  const recognitionRef = useRef(null);
  const supportsSpeech =
    typeof window !== "undefined" &&
    (window.SpeechRecognition || window.webkitSpeechRecognition);

  // Auto-scroll on new messages
  useEffect(() => {
    scrollRef.current?.scrollTo({ top: 999999, behavior: "smooth" });
  }, [messages, busy]);

  function addUserMessage(text) {
    setMessages((ms) => [...ms, { role: "user", content: text }]);
  }

  function addAssistantSQL(payload) {
    const { sql, explain, columns, rows } = payload;
    setMessages((ms) => [
      ...ms,
      { role: "assistant", sql, explain, columns, rows },
    ]);
  }

  async function ask(question) {
    setError("");
    setBusy(true);
    try {
      // 1) show user message
      addUserMessage(question);

      // 2) call all-in-one endpoint (draft → validate → EXPLAIN → execute)
      const res = await fetch(`${API}/ai/ask`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question,
          connection: { url: connectionUrl },
          limit: 100,
          timeout_ms: 5000,
        }),
      });

      if (!res.ok) {
        const err = await safeJson(res);
        throw new Error(err?.detail || `Request failed (${res.status})`);
      }

      const data = await res.json();
      addAssistantSQL(data);
    } catch (e) {
      const msg =
        (e && e.message) ||
        "Something went wrong running your query. Check server logs.";
      setError(msg);
      setMessages((ms) => [...ms, { role: "assistant", content: `⚠️ ${msg}` }]);
    } finally {
      setBusy(false);
      setInput("");
    }
  }

  // Speech: start/stop
  const startListening = () => {
    if (!supportsSpeech || listening) return;

    const SR =
      window.SpeechRecognition || window.webkitSpeechRecognition || null;
    const rec = new SR();
    recognitionRef.current = rec;
    rec.lang = "en-US";
    rec.continuous = false; // single utterance
    rec.interimResults = false;

    rec.onresult = (evt) => {
      const transcript = Array.from(evt.results)
        .map((r) => r[0]?.transcript || "")
        .join(" ")
        .trim();
      if (transcript) {
        setInput(transcript);
        // tiny delay so UI paints the text before sending
        setTimeout(() => ask(transcript), 50);
      } else {
        setListening(false);
      }
    };
    rec.onerror = () => {
      setListening(false);
      setError("Mic error. Check browser permissions.");
    };
    rec.onend = () => setListening(false);

    setListening(true);
    rec.start();
  };

  const stopListening = () => {
    recognitionRef.current?.stop();
    setListening(false);
  };

  // UI handlers
  const onSubmit = (e) => {
    e.preventDefault();
    if (!input.trim() || busy) return;
    ask(input.trim());
  };

  const onKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      if (!input.trim() || busy) return;
      ask(input.trim());
    }
  };

  return (
    <div className="page chat">
      <div className="header">
        <div className="title">
          <Link to="/" className="small back">
            ← Home
          </Link>
          <h2>NLP Chat</h2>
          <div className="note">
            Connected DB: <span className="mono">{connectionUrl}</span>
          </div>
        </div>

        <div className="actions">
          {supportsSpeech ? (
            <button
              className={classNames("btn", listening ? "btn-danger" : "btn")}
              onClick={listening ? stopListening : startListening}
              disabled={busy}
              title={
                listening
                  ? "Stop recording"
                  : "Speak your question (browser mic)"
              }
            >
              {listening ? "■ Stop" : "🎤 Speak"}
            </button>
          ) : (
            <div className="note small">
              🎤 Mic not supported in this browser
            </div>
          )}
        </div>
      </div>

      <div className="chat-body" ref={scrollRef}>
        {messages.map((m, i) => (
          <MessageBubble key={i} m={m} />
        ))}
        {busy && (
          <div className="bubble bubble-assistant">
            <div className="typing">Thinking…</div>
          </div>
        )}
      </div>

      <form className="composer" onSubmit={onSubmit}>
        <textarea
          className="input"
          placeholder="Ask a question about your data…  (Shift+Enter for newline)"
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={onKeyDown}
          rows={2}
          disabled={busy || listening}
        />
        <button className="btn btn-primary" disabled={busy || !input.trim()}>
          Send
        </button>
      </form>

      {error ? <div className="error">{error}</div> : null}

      <style>{`
        .page.chat {
          padding: 24px;
          height: 100%;
          display: grid;
          grid-template-rows: auto 1fr auto;
          gap: 12px;
        }
        .header { display:flex; align-items:center; justify-content:space-between; }
        .title h2 { margin: 0 0 6px 0; }
        .note { opacity: .7; }
        .note.small { font-size: 12px; }
        .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }

        .actions .btn { min-width: 120px; }

        .chat-body {
          overflow: auto;
          padding: 8px;
          border-radius: 12px;
          background: rgba(255,255,255,0.02);
          box-shadow: inset 0 0 0 1px rgba(255,255,255,0.06);
        }

        .bubble {
          max-width: 100%;
          margin: 10px 0;
          padding: 14px 16px;
          border-radius: 14px;
          line-height: 1.4;
          box-shadow: 0 1px 0 rgba(0,0,0,.15);
        }
        .bubble-user {
          background: rgba(99, 102, 241, .18);
          border: 1px solid rgba(99, 102, 241, .35);
        }
        .bubble-assistant {
          background: rgba(255,255,255,.04);
          border: 1px solid rgba(255,255,255,.08);
        }
        .typing { opacity: .7; font-style: italic; }

        .sql-label { opacity: .7; margin-top: 8px; margin-bottom: 4px; font-size: 12px; }
        .sql, .explain {
          overflow: auto;
          background: rgba(0,0,0,.35);
          border: 1px solid rgba(255,255,255,.08);
          border-radius: 10px;
          padding: 10px;
          font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
          font-size: 12.5px;
          max-height: 240px;
        }

        .result-table-wrap { overflow:auto; }
        .result-table {
          width: 100%;
          border-collapse: collapse;
          font-size: 14px;
        }
        .result-table th, .result-table td {
          border-bottom: 1px solid rgba(255,255,255,.08);
          padding: 8px 10px;
          text-align: left;
        }
        .composer {
          display: grid;
          grid-template-columns: 1fr auto;
          gap: 8px;
          align-items: end;
        }
        .input {
          width: 100%;
          resize: vertical;
          min-height: 44px;
          max-height: 160px;
          padding: 10px 12px;
          border-radius: 12px;
          border: 1px solid rgba(255,255,255,.12);
          background: rgba(255,255,255,.03);
          color: inherit;
        }
        .btn {
          height: 44px;
          padding: 0 16px;
          border-radius: 12px;
          border: 1px solid rgba(255,255,255,.12);
          background: rgba(255,255,255,.06);
          color: inherit;
        }
        .btn:hover { background: rgba(255,255,255,.10); }
        .btn-primary {
          background: rgb(99,102,241);
          border-color: rgba(99,102,241,.8);
          color: #fff;
        }
        .btn-primary:hover { filter: brightness(.98); }
        .btn-danger {
          background: rgba(239, 68, 68, .25);
          border-color: rgba(239, 68, 68, .6);
        }
        .back { margin-right: 10px; }
        .error {
          color: #ffb4b4;
          background: rgba(239,68,68,.15);
          border: 1px solid rgba(239,68,68,.35);
          padding: 8px 10px;
          border-radius: 10px;
          font-size: 13px;
        }
      `}</style>
    </div>
  );
}

// Safe JSON parse helper when errors are not JSON
async function safeJson(res) {
  try {
    return await res.json();
  } catch {
    return null;
  }
}
