# NLP_SQLizer — Team 04 (CS 5151/6051 Database Theory)

Natural language (and voice) to SQL for your own database — safe, explainable, and self-hostable.

> Course: Database Theory (DBT) — Team 04  
> Semester Project: "NLP_SQLizer" (a lightweight AI copilot for querying existing SQL databases)

---

## Team Members

| Name                      | Major            | Email                    | Focus/Role (tentative)                                                                                                                                           |
| ------------------------- | ---------------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Kartavya Singh (Lead)** | Computer Science | **singhk6@mail.uc.edu**  | **Lead & Backend/AI** — FastAPI architecture; schema crawler; NL→SQL planner & candidate ranking; safety/EXPLAIN gates; end-to-end integration & demo stability. |
| **Saarthak Sinha**        | Computer Science | **sinhas6@mail.uc.edu**  | **Backend & Docs** — API contracts; connection manager; `/schema/overview`; evaluation set & scripts; course deliverables and reporting.                         |
| **Kanav Shetty**          | Computer Science | **shettykv@mail.uc.edu** | **Frontend (React) & DX** — Connect flow; chat UI; voice input; telemetry & UX polish; assists on schema-prompt context/AI as needed.                            |

## 1) Overview

NLP_SQLizer lets a user connect to an existing SQL database and ask questions in plain English (or via voice).
The backend converts the question into safe SQL, executes it in read-only mode, and returns (a) the generated SQL, (b) a results grid (CSV export), and (c) a short explanation of how the query answers the question.

v1 focuses on PostgreSQL. The design keeps adapters thin so MySQL/SQLite can be added later.

---

## 2) Features (v1)

- Two-step connect flow
  - Paste a database URL or enter host/port/db/user; password is collected separately when missing in the URL.
  - No credentials are persisted. We keep a non-secret "last connection profile" in a cookie/localStorage (e.g., host, port, db, user; no password).
- Schema crawl & explorer
  - Read tables, columns, types, PK/FK graph, light stats, and sample values.
  - Expose `/schema/overview` so the frontend can render a small explorer.
- Natural-language to SQL
  - Candidate generation (3–5) using FK-aware join search and predicate mapping.
  - Ranking by intent coverage, join plausibility, and EXPLAIN estimates.
  - Returns SQL + results + a short explanation.
- Safety by default
  - Read-only role, LIMIT injection, statement_timeout, denylist for DDL/WRITE.
  - EXPLAIN gate before execution; fall back to a cheaper candidate if needed.
- Chat UX with voice input
  - Web Speech API (Chrome) for dictation; copy-SQL, CSV export, and "why this query?" hints.

---

## 3) Architecture

```
PROJECT_TEAM_04/
├─ Deliverables/          # Course reports, slides, screenshots
└─ NLP_SQLizer/
   ├─ frontend/           # React (JavaScript, Vite)
   │  └─ src/
   └─ backend/            # FastAPI (Python)
      └─ app/
         ├─ api/          # /connect, /schema, /chat
         ├─ db/           # connection manager, DSN assembly
         ├─ schema/       # crawler, sampling, synonyms
         ├─ nlsql/        # planner, candidate gen, ranking
         ├─ safety/       # static checks, EXPLAIN gate
         └─ exec/         # read-only executor, CSV export
```

### Data-flow (connect -> ask)

1. Frontend collects DSN or host/port/db/user (+ password if needed) -> POST /connect/test.
2. Backend assembles DSN in memory only, tests connectivity with a read-only role, sets server-side statement_timeout.
3. On success, backend builds a schema context pack; frontend routes to /chat and can show the Schema Explorer.
4. User asks a question. Backend generates multiple SQL candidates, runs static checks and an EXPLAIN gate, executes the best safe plan, and returns results + explanation.

---

## 4) Security model

- Use a read-only database role (GRANT SELECT only). Reject any query that contains DDL/INSERT/UPDATE/DELETE/TRUNCATE, etc.
- Enforce LIMIT when missing; set statement_timeout per session.
- Do not log full DSNs or passwords. Do not persist credentials to disk.
- Cookie/localStorage stores only non-secrets to enable "Reconnect to last database."

---

## 5) Quickstart (Development)

### Backend (FastAPI + Python 3.11+)

```bash
# Linux/macOS
cd NLP_SQLizer/backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

```powershell
# Windows (PowerShell)
cd NLP_SQLizer/backend
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

**Environment variables**

```
READ_ONLY=true
MAX_ROWS=500
STATEMENT_TIMEOUT_MS=8000
ALLOWED_ORIGIN=http://localhost:5173
```

### Frontend (React + Vite, Node LTS + npm)

```bash
cd NLP_SQLizer/frontend
npm install
npm run dev
```

Open the printed local URL (typically http://localhost:5173).

---

## 6) Connecting to a database

- URL mode (recommended):  
  `postgresql://USER[:PASSWORD]@HOST:PORT/DBNAME?sslmode=require`  
  If the pasted URL lacks a password, the UI prompts for it before sending to the backend.
- Manual mode: host, port, db, user, password fields. The backend assembles the DSN in memory.

Note: Most drivers cannot prompt for a password. If a password is required and not provided, auth fails. That is why the UI collects it when the URL omits it.

---

## 7) API (initial surface)

- GET /healthz — service liveness
- POST /connect/test — body: { dsn? | host, port, db, user, password } -> { ok: true }
- GET /schema/overview — returns tables/columns, PK/FK edges, samples, synonyms
- POST /chat/ask — body: { question: string } -> { sql, rows, columns, explanation, diagnostics }
- POST /chat/ask/dryrun — like above but runs EXPLAIN only

---

## 8) NL->SQL planner (v1)

1. Parsing & intent — identify entities, filters, aggregations, and ordering from the question.
2. Grounding — map phrases to tables/columns using the schema context + synonyms (e.g., "pupils" -> students).
3. Candidate generation — 3-5 SQL variants via FK join paths and alternative predicates.
4. Safety checks — SQL parse/AST validation, denylist, LIMIT injection.
5. EXPLAIN gate — reject plans that are too costly; try the next candidate.
6. Execution — run read-only; cap rows (MAX_ROWS); return CSV on demand.
7. Explanation — short description of joins/filters/aggregations used.

Example

> "List students who received a C but have a numeric score above 75."

```sql
SELECT s.student_id, s.name, e.course_id, e.term, e.year, e.grade_letter, e.numeric_score
FROM enrollments e
JOIN students s ON s.student_id = e.student_id
WHERE e.grade_letter = 'C'
  AND e.numeric_score > 75
ORDER BY s.student_id;
```

---

## 9) Evaluation (for the report)

- Build a test set (~50 NL<->SQL pairs) across 2 schemas (e.g., school, store).
- Metrics: execution success rate, result-set F1 vs gold, unsafe query rate (target 0), time-to-first-correct.
- Brief user study: task completion and perceived clarity of the explanation.

---

## 10) Roadmap

- v1: Postgres only, read-only, small schemas.
- v2: MySQL/SQLite adapters, better synonym mining, provenance (which joins/rows mattered most), caching of schema context, optional semantic reranker.

---

## 11) Team — Roles & Responsibilities

- Kartavya (Team Lead): Backend architecture, schema crawler, NL->SQL planner & ranking, safety/execution; integration and demo stability.
- Kanav: Frontend React UX (connect flow, chat, voice input), telemetry & usability test; assists with schema prompt context.
- Sarthak: Backend API contracts, connection manager, evaluation set & scripts, documentation and course deliverables.

---

## 12) Milestones (suggested weekly pacing)

1. Connect flow (URL/manual) + connection test; start schema crawler.
2. Schema explorer UI; NL->SQL v1 (template/rules) with safe execution.
3. Candidate generation & ranking; EXPLAIN gate.
4. Chat UX polish; voice input; CSV export.
5. Evaluation harness + small user test; error copy & edge cases.
6. Final polish, slides, and demo script.

---

## 13) Repository hygiene

- Branches: feat/frontend-connect, feat/backend-schema, feat/planner, feat/safety-exec, etc.
- PRs require lint + tests to pass.
- Dependabot on for JS/Python; secret scanning enabled.
- Add LICENSE (MIT), CONTRIBUTING.md, and docs/architecture.md with sequence diagrams.

---

## 14) License

MIT — see LICENSE.
