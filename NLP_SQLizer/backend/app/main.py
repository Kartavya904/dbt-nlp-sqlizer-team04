# app/main.py
from typing import Optional, Dict, Any

from fastapi import FastAPI, Body, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from sqlalchemy import create_engine, inspect
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import SQLAlchemyError

from .db import engine as default_engine
from .settings import settings


app = FastAPI(title="NLP_SQLizer Backend", version="0.1.0")

# Allow localhost and 127.0.0.1 variants (helps during dev)
origins = {settings.ALLOWED_ORIGIN}
try:
    if "localhost" in settings.ALLOWED_ORIGIN:
        origins.add(settings.ALLOWED_ORIGIN.replace("localhost", "127.0.0.1"))
    if "127.0.0.1" in settings.ALLOWED_ORIGIN:
        origins.add(settings.ALLOWED_ORIGIN.replace("127.0.0.1", "localhost"))
except Exception:
    pass

app.add_middleware(
    CORSMiddleware,
    allow_origins=list(origins),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "backend", "message": "healthy"}


# ---------- Helpers ----------

def _engine_from_payload(payload: Optional[Dict[str, Any]]):
    """
    If a payload with connection details is provided, build a temporary Engine.
    Otherwise return None so we fall back to the default Engine.
    """
    if not payload:
        return None

    try:
        # Method 1: full URL
        url_str = (payload.get("url") or "").strip()
        if url_str:
            # pool_pre_ping keeps long-lived servers from holding dead sockets
            return create_engine(url_str, future=True, pool_pre_ping=True)

        # Method 2: discrete parts
        parts = payload.get("parts") or {}
        if parts:
            url = URL.create(
                drivername=parts.get("DB_DRIVER", "postgresql+psycopg"),
                username=parts.get("DB_USER") or None,
                password=parts.get("DB_PASSWORD") or None,  # safely quoted by SQLAlchemy
                host=parts.get("DB_HOST", "localhost"),
                port=int(parts["DB_PORT"]) if parts.get("DB_PORT") not in (None, "") else None,
                database=parts.get("DB_NAME") or None,
            )
            return create_engine(url, future=True, pool_pre_ping=True)
    except Exception as e:
        # Bad URL or malformed parts → 400
        raise HTTPException(status_code=400, detail=f"Invalid connection details: {e}")

    return None


def _pick_engine(payload: Optional[Dict[str, Any]]):
    return _engine_from_payload(payload) or default_engine


def _safe_url_str(eng) -> str:
    try:
        return make_url(str(eng.url)).render_as_string(hide_password=True)
    except Exception:
        return ""


# ---------- Routes ----------

@app.api_route("/connect/test", methods=["GET", "POST"])
def connect_test(payload: Optional[Dict[str, Any]] = Body(default=None)):
    """
    Test a connection. Accepts either:
      - POST body { "url": "<driver://user:pass@host:port/db>" }
      - POST body { "parts": { DB_DRIVER, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD } }
    or falls back to the server's default engine on GET.
    """
    eng = _pick_engine(payload)
    try:
        with eng.connect() as conn:
            _ = conn.exec_driver_sql("SELECT 1").scalar()
            return {
                "ok": True,
                "dialect": conn.dialect.name,
                "url": _safe_url_str(conn.engine),  # password hidden
            }
    except SQLAlchemyError as e:
        # Surface a clean 400 with the DB error string
        raise HTTPException(status_code=400, detail=f"Connection failed: {e}")


@app.api_route("/schema/overview", methods=["GET", "POST"])
def schema_overview(payload: Optional[Dict[str, Any]] = Body(default=None)):
    """
    Return a lightweight schema map:
      { ok, dialect, tables: [ { table, columns: [ { name, type, nullable } ] } ] }
    Accepts the same payload as /connect/test or falls back to the default engine.
    """
    eng = _pick_engine(payload)
    try:
        insp = inspect(eng)
        tables = []
        for tname in insp.get_table_names():
            cols = []
            for c in insp.get_columns(tname):
                cols.append({
                    "name": c.get("name"),
                    "type": str(c.get("type")),
                    "nullable": bool(c.get("nullable", True)),
                })
            tables.append({"table": tname, "columns": cols})
        return {
            "ok": True,
            "dialect": eng.dialect.name,
            "tables": tables,
        }
    except SQLAlchemyError as e:
        raise HTTPException(status_code=400, detail=f"Schema inspection failed: {e}")