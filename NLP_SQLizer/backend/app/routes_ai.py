# app/routes_ai.py  (add below your other routes)
from fastapi import APIRouter, HTTPException, Body
from sqlalchemy import create_engine, URL
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
from typing import Optional, Dict, Any
import json
from .ai.nl2sql import (
    load_schema, select_relevant, ask_llm,
    ensure_select_only, ensure_tables_allowed, enforce_limit, finalize_sql,
    execute_readonly, explain, SQLSafetyError, _validate_aggregation_requirements,
    _validate_query_structure
)
from .ai.nl2mongo import (
    load_mongodb_schema, select_relevant_mongo, ask_llm_mongo,
    execute_mongodb_query, explain_mongodb_query
)
from .ai.llm import LLMNotConfigured
from .models import ModelTrainer, SchemaQueryGenerator
from .schema import crawl_schema
from .mongodb_adapter import is_mongodb_url, crawl_mongodb_schema
from .settings import settings

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
            # MongoDB is handled separately in the /ask endpoint
            # No need to block it here
            
            # Check for unencoded special characters in password (multiple @ symbols)
            if url_str.count("@") > 1:
                raise HTTPException(
                    status_code=400,
                    detail="Password contains '@' symbol which must be URL-encoded as '%40'. "
                           "Example: postgresql://user:pass%40word@host/db"
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
    Generate SQL or MongoDB query. Optionally uses trained model if available.
    Supports both SQL databases and MongoDB.
    
    Always returns a response (never raises unhandled errors).
    
    Body: {
        "question": "...",
        "connection": { "url": "..." },
        "use_trained_model": true,  # Try to use trained model first (SQL only)
        "limit": 100,
        "timeout_ms": 5000
    }
    """
    import logging
    import traceback
    logger = logging.getLogger(__name__)
    
    try:
        q = (payload.get("question") or "").strip()
        if not q:
            return {
                "ok": False,
                "error": "Missing question",
                "message": "Please provide a question to query the database.",
                "sql": None,
                "columns": [],
                "rows": [],
                "rowcount": 0,
            }
        connection = payload.get("connection")
        if not connection:
            return {
                "ok": False,
                "error": "Missing connection",
                "message": "Database connection information is required.",
                "sql": None,
                "columns": [],
                "rows": [],
                "rowcount": 0,
            }
        limit = int(payload.get("limit") or 100)
        timeout_ms = int(payload.get("timeout_ms") or 5000)
        use_trained = payload.get("use_trained_model", True)
    except Exception as e:
        logger.error(f"Invalid request payload: {e}")
        return {
            "ok": False,
            "error": "Invalid request",
            "message": f"Request validation failed: {str(e)}",
            "sql": None,
            "columns": [],
            "rows": [],
            "rowcount": 0,
        }
    
    try:
        # Check if this is a MongoDB connection
        url_str = (connection.get("url") or "").strip() if connection else ""
        is_mongo = url_str and is_mongodb_url(url_str)
        
        if is_mongo:
            # Handle MongoDB query generation
            return _handle_mongodb_query(q, url_str, limit, timeout_ms)
        
        # Handle SQL query generation
        eng = _engine_from_connection(connection)
    except Exception as e:
        logger.error(f"Connection setup failed: {e}", exc_info=True)
        return {
            "ok": False,
            "error": "Connection failed",
            "message": f"Failed to establish database connection: {str(e)}",
            "sql": None,
            "columns": [],
            "rows": [],
            "rowcount": 0,
        }

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
                _validate_aggregation_requirements(q, sql_final)
                expr = ensure_select_only(sql_final)
                expr = enforce_limit(expr, limit)
                sql_final = finalize_sql(expr)
                _validate_aggregation_requirements(q, sql_final)
                
                # Use separate connections to avoid transaction conflicts
                with eng.connect() as conn_explain:
                    plan = explain(conn_explain, sql_final) or ""
                    import re
                    m = re.search(r"rows=(\d+)", plan)
                    if m and int(m.group(1)) > 100_000:
                        raise HTTPException(400, "Plan too large; refine filters or narrow scope.")
                
                # Execute query in a separate connection
                try:
                    with eng.connect() as conn_exec:
                        cols, rows = execute_readonly(conn_exec, sql_final, timeout_ms)
                except Exception as e:
                    # Execution failed with trained model - fall through to LLM
                    logger.warning(f"Trained model execution failed, falling back to LLM: {e}")
                    raise  # Re-raise to trigger fallback to LLM
                
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
        except SQLSafetyError as e:
            # Validation error - fall through to LLM-based generation
            logger.warning(f"Trained model validation failed, falling back to LLM: {e}")
        except Exception as e:
            # Other errors - fall through to LLM-based generation
            logger.warning(f"Trained model failed, falling back to LLM: {e}")

    # Fallback to original LLM-based approach
    try:
        schema = load_schema(eng)
        allowed = select_relevant(schema, q)  # {table:[cols]}
        if not allowed:
            return {
                "ok": False,
                "error": "No relevant tables found",
                "message": "Could not find any tables or columns relevant to your question. Please rephrase or check the database schema.",
                "sql": None,
                "columns": [],
                "rows": [],
                "rowcount": 0,
            }

        draft = ask_llm(q, allowed, use_intent_analysis=True)
        # Validate aggregation requirements before parsing
        _validate_aggregation_requirements(q, draft)
        _validate_query_structure(q, draft, allowed)
        expr = ensure_select_only(draft)
        ensure_tables_allowed(expr, allowed)
        expr = enforce_limit(expr, limit)
        sql_final = finalize_sql(expr)
        # Validate again after finalization (in case parsing changed something)
        _validate_aggregation_requirements(q, sql_final)
        _validate_query_structure(q, sql_final, allowed)
    except SQLSafetyError as e:
        logger.error(f"Validation failed: {e}")
        return {
            "ok": False,
            "error": "Query validation failed",
            "message": f"The generated query failed safety validation: {str(e)}",
            "sql": None,
            "columns": [],
            "rows": [],
            "rowcount": 0,
        }
    except Exception as e:
        logger.error(f"Query generation failed: {e}", exc_info=True)
        return {
            "ok": False,
            "error": "Query generation failed",
            "message": f"Failed to generate query: {str(e)}",
            "sql": None,
            "columns": [],
            "rows": [],
            "rowcount": 0,
        }

    # EXPLAIN gate (simple but effective)
    # Use separate connections to avoid transaction conflicts
    with eng.connect() as conn_explain:
        plan = explain(conn_explain, sql_final) or ""
        # Block obviously large plans
        import re
        m = re.search(r"rows=(\d+)", plan)
        if m and int(m.group(1)) > 100_000:
            raise HTTPException(400, "Plan too large; refine filters or narrow scope.")
    
    # Execute query in a separate connection
    try:
        with eng.connect() as conn_exec:
            cols, rows = execute_readonly(conn_exec, sql_final, timeout_ms)
    except Exception as e:
        # Database execution error - return user-friendly message
        error_msg = str(e)
        logger.error(f"Query execution failed: {error_msg}", exc_info=True)
        return {
            "ok": False,
            "error": "Query execution failed",
            "message": f"The generated query failed to execute: {error_msg}",
            "sql": sql_final,
            "columns": [],
            "rows": [],
            "rowcount": 0,
            "explain": plan,
            "context": allowed,
            "method": "llm",
        }
    
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


def _handle_mongodb_query(question: str, connection_url: str, limit: int, timeout_ms: int) -> Dict[str, Any]:
    """
    Handle MongoDB query generation and execution.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Load MongoDB schema
        schema = load_mongodb_schema(connection_url)
        if not schema:
            raise HTTPException(400, "No collections found in MongoDB database")
        
        # Select relevant collections and fields
        allowed = select_relevant_mongo(schema, question, k_collections=4)
        if not allowed:
            raise HTTPException(400, "No relevant collections/fields found")
        
        # Generate MongoDB query using LLM
        try:
            query_dict = ask_llm_mongo(question, allowed)
        except LLMNotConfigured as e:
            error_msg = str(e)
            logger.error(f"LLM not configured or unavailable: {error_msg}")
            raise HTTPException(
                status_code=503,
                detail=f"LLM service is not available. {error_msg} Please configure LLM_BASE_URL and LLM_MODEL in your .env file or ensure the LLM service is running."
            )
        except Exception as e:
            logger.error(f"LLM query generation failed: {e}", exc_info=True)
            error_msg = str(e)
            # Check if it's a connection error
            if "connection" in error_msg.lower() or "refused" in error_msg.lower():
                raise HTTPException(
                    status_code=503,
                    detail=f"Could not connect to LLM service. Please ensure the LLM service is running at {settings.LLM_BASE_URL} and check your .env configuration."
                )
            raise HTTPException(400, f"Failed to generate MongoDB query: {error_msg}")
        
        # Ensure limit is set
        query_dict["limit"] = min(limit, query_dict.get("limit", 100))
        
        # Execute query
        try:
            columns, rows, rowcount = execute_mongodb_query(connection_url, query_dict, timeout_ms)
        except Exception as e:
            logger.error(f"MongoDB query execution failed: {e}", exc_info=True)
            raise HTTPException(400, f"Query execution failed: {e}")
        
        # Get query explanation
        try:
            explain_text = explain_mongodb_query(connection_url, query_dict)
        except Exception as e:
            logger.warning(f"Query explanation failed: {e}")
            explain_text = ""
        
        # Format query for display
        query_display = json.dumps(query_dict, indent=2)
        
        return {
            "ok": True,
            "sql": query_display,  # Using "sql" field for compatibility with frontend
            "mongo_query": query_dict,  # Also include as mongo_query
            "columns": columns,
            "rows": rows,
            "rowcount": rowcount,
            "explain": explain_text,
            "context": allowed,
            "method": "llm_mongo",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"MongoDB query generation error: {e}", exc_info=True)
        raise HTTPException(500, f"Internal error: {e}")
