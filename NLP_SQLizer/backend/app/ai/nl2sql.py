# app/ai/nl2sql.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple
from rapidfuzz import fuzz, process
import sqlglot as sg
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine, Connection
from .llm import chat_complete, LLMNotConfigured

# ---------- Schema utilities ----------

def load_schema(engine: Engine) -> Dict[str, List[Dict[str, Any]]]:
    insp = inspect(engine)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for t in insp.get_table_names():
        cols = [{"name": c["name"], "type": str(c["type"]), "nullable": bool(c.get("nullable", True))}
                for c in insp.get_columns(t)]
        out[t] = cols
    return out

def select_relevant(schema: Dict[str, List[Dict[str, Any]]], question: str, k_tables: int = 4) -> Dict[str, List[str]]:
    """
    Fuzzy-score tables/columns against the question; return a pruned view: { table: [columns...] }.
    """
    tables = list(schema.keys())
    table_scores = [(t, max(
        fuzz.partial_ratio(t, question),
        max((fuzz.partial_ratio(c["name"], question) for c in schema[t]), default=0)
    )) for t in tables]
    table_scores.sort(key=lambda x: x[1], reverse=True)
    chosen = [t for t, _ in table_scores[:k_tables]]

    out: Dict[str, List[str]] = {}
    for t in chosen:
        cols = [c["name"] for c in schema[t]]
        # keep top columns plus primary key-ish names
        col_scores = [(c, fuzz.partial_ratio(c, question)) for c in cols]
        col_scores.sort(key=lambda x: x[1], reverse=True)
        best = [c for c, _ in col_scores[:8]]
        for fallback in ("id", f"{t}_id"):
            if fallback in cols and fallback not in best:
                best.append(fallback)
        out[t] = best
    return out

# ---------- Prompting ----------

SYS = """You generate a single, safe, READ-ONLY SQL query.
Rules:
- Return ONE SQL statement only, no backticks, no prose.
- SELECT only. Never use INSERT/UPDATE/DELETE/DDL.
- Use only the tables and columns given.
- If joins are needed, use explicit JOIN ... ON.
- Prefer COUNT, SUM, AVG with GROUP BY when appropriate.
- Always include LIMIT 100 unless the query is an aggregate that returns <= 100 rows naturally.
- Use standard SQL that works on PostgreSQL.
"""

def render_context(slice_: Dict[str, List[str]]) -> str:
    lines = []
    for t, cols in slice_.items():
        col_list = ", ".join(cols)
        lines.append(f"- {t}({col_list})")
    return "\n".join(lines)

def ask_llm(question: str, slice_: Dict[str, List[str]]) -> str:
    ctx = render_context(slice_)
    user = f"""Question: {question}

Tables and columns you may use:
{ctx}

Respond with ONE SQL SELECT only."""
    return chat_complete(SYS, user)

# ---------- Validation & safety ----------

class SQLSafetyError(Exception):
    pass

BLOCK_KINDS = {"Insert", "Update", "Delete", "Create", "Drop", "Alter", "Truncate", "Merge"}

def ensure_select_only(sql: str) -> sg.Expression:
    try:
        parsed = sg.parse_one(sql, read="postgres")
    except Exception as e:
        raise SQLSafetyError(f"SQL parse error: {e}")
    if parsed.__class__.__name__ in BLOCK_KINDS:
        raise SQLSafetyError("Only SELECT statements are allowed.")
    # Unions etc. are okay if they are SELECT-derived
    if not any(isinstance(parsed, k) for k in (sg.exp.Select, sg.exp.Subquery, sg.exp.Union, sg.exp.With)):
        raise SQLSafetyError("Statement must be a SELECT.")
    return parsed

def referenced_tables(expr: sg.Expression) -> List[str]:
    names = []
    for t in expr.find_all(sg.exp.Table):
        names.append(t.this and t.this.name or "")
    # dedupe
    return sorted(set(n for n in names if n))

def enforce_limit(expr: sg.Expression, max_rows: int = 100) -> sg.Expression:
    # If top-level is a SELECT without limit, add one
    target = expr
    if isinstance(target, sg.exp.Subquery):
        target = target.this
    if isinstance(target, sg.exp.Select) and not target.args.get("limit"):
        target.set("limit", sg.exp.Limit(this=sg.exp.Literal.number(max_rows)))
    return expr

def ensure_tables_allowed(expr: sg.Expression, allowed: Dict[str, List[str]]):
    used = referenced_tables(expr)
    allowed_tables = set(allowed.keys())
    for t in used:
        if t not in allowed_tables:
            raise SQLSafetyError(f"Table not allowed in context: {t}")

def finalize_sql(expr: sg.Expression) -> str:
    return expr.sql(dialect="postgres")

# ---------- Execution ----------

# app/ai/nl2sql.py (or wherever execute_readonly lives)
from decimal import Decimal
from datetime import date, datetime, time
from sqlalchemy.engine import Connection

def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)  # or str(v) if you need exactness
    if isinstance(v, (date, datetime, time)):
        return v.isoformat()
    return v

def execute_readonly(conn: Connection, sql: str, timeout_ms: int = 5000):
    # Keep queries safe/fast
    try:
        # Postgres: short statement timeout
        conn.exec_driver_sql(f"SET LOCAL statement_timeout = {timeout_ms}")
    except Exception:
        pass

    res = conn.exec_driver_sql(sql)

    # Columns
    cols = []
    if getattr(res, "cursor", None) and res.cursor.description:
        cols = [c[0] for c in res.cursor.description]

    # Rows → plain lists of JSON-safe values
    rows = []
    if cols:
        for row in res.fetchall():            # row is a tuple/Row object
            rows.append([_jsonable(v) for v in row])

    return cols, rows


def explain(conn: Connection, sql: str) -> str:
    try:
        txt = conn.exec_driver_sql(f"EXPLAIN {sql}").fetchall()
        # EXPLAIN (FORMAT TEXT) returns rows of text in PG
        return "\n".join(r[0] for r in txt)
    except Exception:
        return ""
