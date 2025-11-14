# app/routes_ai.py  (add below your other routes)
from fastapi import APIRouter, HTTPException, Body
from sqlalchemy import create_engine, URL
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
from typing import Optional, Dict, Any
from .ai.nl2sql import (
    load_schema, select_relevant, ask_llm,
    ensure_select_only, ensure_tables_allowed, enforce_limit, finalize_sql,
    execute_readonly, explain, SQLSafetyError
)
from .models import ModelTrainer, SchemaQueryGenerator
from .schema import crawl_schema
from .mongodb_adapter import is_mongodb_url

router = APIRouter(prefix="/ai", tags=["ai"])

_trainer = ModelTrainer(models_dir=Path("models"))

def _engine_from_connection(connection: Optional[Dict[str, Any]]):
    """Create engine from connection object (supports both url and parts)"""
    if not connection:
        raise HTTPException(400, "Missing connection")
    
    try:
        # Method 1: full URL
        url_str = (connection.get("url") or "").strip()
        if url_str:
            # Check for MongoDB - AI query generation for MongoDB is not yet implemented
            url_lower = url_str.lower()
            if url_lower.startswith("mongodb"):
                raise HTTPException(
                    status_code=400,
                    detail="MongoDB is supported for schema inspection, but AI query generation is currently only available for SQL databases. Please use SQL databases (PostgreSQL, MySQL, SQLite, SQL Server, Oracle) for AI-powered queries."
                )
            
            # Handle SQLite specially
            kwargs = {}
            if url_str.startswith("sqlite"):
                kwargs["connect_args"] = {"check_same_thread": False}
            else:
                kwargs["pool_pre_ping"] = True
            return create_engine(url_str, **kwargs)
        
        # Method 2: discrete parts
        parts = connection.get("parts") or {}
        if parts:
            driver = parts.get("DB_DRIVER", "postgresql+psycopg")
            
            # Handle SQLite specially
            if driver and "sqlite" in driver.lower():
                db_name = parts.get("DB_NAME") or ""
                url_str = f"{driver}:///{db_name}"
                return create_engine(url_str, connect_args={"check_same_thread": False})
            
            # For other databases, build URL from parts
            url = URL.create(
                drivername=driver,
                username=parts.get("DB_USER") or None,
                password=parts.get("DB_PASSWORD") or None,
                host=parts.get("DB_HOST", "localhost"),
                port=int(parts["DB_PORT"]) if parts.get("DB_PORT") not in (None, "") else None,
                database=parts.get("DB_NAME") or None,
            )
            return create_engine(url, pool_pre_ping=True)
        
        raise HTTPException(400, "Connection must have either 'url' or 'parts'")
    except HTTPException:
        raise
    except Exception as e:
        # Check for SQLAlchemy dialect errors
        error_str = str(e)
        if "NoSuchModuleError" in error_str or "Can't load plugin" in error_str:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported database type. This application supports SQL databases (PostgreSQL, MySQL, SQLite, SQL Server, Oracle) and MongoDB (schema inspection only). Error: {error_str}"
            )
        raise HTTPException(400, f"Invalid connection details: {e}")

@router.post("/ask")
def ai_ask(payload: dict = Body(...)):
    """
    Generate SQL query. Optionally uses trained model if available.
    
    Body: {
        "question": "...",
        "connection": { "url": "..." },
        "use_trained_model": true,  # Try to use trained model first
        "limit": 100,
        "timeout_ms": 5000
    }
    """
    q = (payload.get("question") or "").strip()
    if not q: raise HTTPException(400, "Missing 'question'")
    connection = payload.get("connection")
    eng = _engine_from_connection(connection)
    limit = int(payload.get("limit") or 100)
    timeout_ms = int(payload.get("timeout_ms") or 5000)
    use_trained = payload.get("use_trained_model", True)

    # Try to use trained model if available
    if use_trained:
        try:
            # Crawl schema to get schema_id
            metadata = crawl_schema(eng, sample_size=50)  # Smaller sample for speed
            schema_id = _trainer.generate_schema_id(metadata)
            model = _trainer.load_model(schema_id)
            
            if model:
                # Use trained model
                generator = SchemaQueryGenerator(model)
                sql_final, gen_metadata = generator.generate_query(q)
                explanation = generator.explain_query(sql_final, q)
                
                # Validate and execute
                expr = ensure_select_only(sql_final)
                expr = enforce_limit(expr, limit)
                sql_final = finalize_sql(expr)
                
                with eng.connect() as conn:
                    plan = explain(conn, sql_final) or ""
                    import re
                    m = re.search(r"rows=(\d+)", plan)
                    if m and int(m.group(1)) > 100_000:
                        raise HTTPException(400, "Plan too large; refine filters or narrow scope.")
                    cols, rows = execute_readonly(conn, sql_final, timeout_ms)
                
                return {
                    "ok": True,
                    "sql": sql_final,
                    "columns": cols,
                    "rows": rows,
                    "rowcount": len(rows),
                    "explain": plan,
                    "explanation": explanation,
                    "confidence": gen_metadata.get("confidence", 0.5),
                    "method": "trained_model",
                }
        except Exception as e:
            # Fall through to LLM-based generation
            print(f"Trained model failed, falling back to LLM: {e}")

    # Fallback to original LLM-based approach
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
    return {
        "ok": True,
        "sql": sql_final,
        "columns": cols,
        "rows": rows,
        "rowcount": len(rows),
        "explain": plan,
        "context": allowed,
        "method": "llm",
    }
