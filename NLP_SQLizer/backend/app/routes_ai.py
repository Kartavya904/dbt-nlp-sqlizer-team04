# app/routes_ai.py  (add below your other routes)
from fastapi import APIRouter, HTTPException, Body
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from .ai.nl2sql import (
    load_schema, select_relevant, ask_llm,
    ensure_select_only, ensure_tables_allowed, enforce_limit, finalize_sql,
    execute_readonly, explain, SQLSafetyError
)

router = APIRouter(prefix="/ai", tags=["ai"])

def _engine(url: str):
    if not url: raise HTTPException(400, "Missing connection.url")
    try: return create_engine(url, pool_pre_ping=True)
    except Exception as e: raise HTTPException(400, f"Invalid connection.url: {e}")

@router.post("/ask")
def ai_ask(payload: dict = Body(...)):
    q = (payload.get("question") or "").strip()
    if not q: raise HTTPException(400, "Missing 'question'")
    url = (payload.get("connection") or {}).get("url") or ""
    eng = _engine(url)
    limit = int(payload.get("limit") or 100)
    timeout_ms = int(payload.get("timeout_ms") or 5000)

    schema = load_schema(eng)
    allowed = select_relevant(schema, q)  # {table:[cols]}
    if not allowed: raise HTTPException(400, "No relevant tables/columns found")

    try:
        draft = ask_llm(q, allowed)
        expr = ensure_select_only(draft)
        ensure_tables_allowed(expr, allowed)
        expr = enforce_limit(expr, limit)
        sql_final = finalize_sql(expr)
    except SQLSafetyError as e:
        raise HTTPException(400, f"Validation failed: {e}")

    # EXPLAIN gate (simple but effective)
    with eng.connect() as conn:
        plan = explain(conn, sql_final) or ""
        # Block obviously large plans
        import re
        m = re.search(r"rows=(\d+)", plan)
        if m and int(m.group(1)) > 100_000:
            raise HTTPException(400, "Plan too large; refine filters or narrow scope.")
        cols, rows = execute_readonly(conn, sql_final, timeout_ms)
    return {"ok": True, "sql": sql_final, "columns": cols, "rows": rows, "rowcount": len(rows), "explain": plan, "context": allowed}
