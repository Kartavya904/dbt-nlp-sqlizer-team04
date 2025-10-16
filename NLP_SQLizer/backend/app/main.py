from fastapi import FastAPI, Body
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import URL, make_url
from .db import engine
from .settings import settings

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.ALLOWED_ORIGIN],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/healthz")
def healthz():
    return {"ok": True, "service": "backend", "message": "healthy"}



@app.get("/connect/test")
def connect_test():
    with engine.connect() as conn:
        _ = conn.execute(text("SELECT 1")).scalar()  # portable ping
        dialect = conn.dialect.name
    return {"ok": True, "dialect": dialect, "url": settings.DATABASE_URL}

@app.get("/schema/overview")
def schema_overview():
    insp = inspect(engine)
    tables = []
    for t in insp.get_table_names():
        cols = [{"name": c["name"], "type": str(c["type"]), "nullable": c.get("nullable", True)}
                for c in insp.get_columns(t)]
        tables.append({"table": t, "columns": cols})
    return {"ok": True, "dialect": engine.dialect.name, "tables": tables}
