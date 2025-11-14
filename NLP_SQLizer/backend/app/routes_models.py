# app/routes_models.py
"""
API routes for model training and management.
"""
from fastapi import APIRouter, HTTPException, Body, BackgroundTasks
from typing import Optional, Dict, Any, List
from pathlib import Path
from sqlalchemy import create_engine, URL
from sqlalchemy.exc import SQLAlchemyError

from .models import ModelTrainer, SchemaModel, SchemaQueryGenerator
from .models.progress import get_progress, is_training, set_progress
from .schema import crawl_schema
from .mongodb_adapter import is_mongodb_url, crawl_mongodb_schema

router = APIRouter(prefix="/models", tags=["models"])

# Global trainer instance
_trainer = ModelTrainer(models_dir=Path("models"))


def _engine_from_connection(connection: Optional[Dict[str, Any]]):
    """Create engine from connection object (supports both url and parts)"""
    import logging
    logger = logging.getLogger(__name__)
    
    if not connection:
        logger.error("Connection object is None or empty")
        raise HTTPException(400, "Missing connection")
    
    try:
        # Method 1: full URL
        url_str = (connection.get("url") or "").strip()
        if url_str:
            # MongoDB is handled separately in the train endpoint
            # No need to block it here
            
            logger.info(f"Creating engine from URL: {url_str[:50]}...")
            
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
            logger.info(f"Creating engine from parts: {list(parts.keys())}")
            driver = parts.get("DB_DRIVER", "postgresql+psycopg")
            
            # Handle SQLite specially
            if driver and "sqlite" in driver.lower():
                db_name = parts.get("DB_NAME") or ""
                url_str = f"{driver}:///{db_name}"
                logger.info(f"SQLite URL: {url_str}")
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
            logger.info(f"URL created from parts: {url.render_as_string(hide_password=True)}")
            return create_engine(url, pool_pre_ping=True)
        
        logger.error(f"Connection object missing both 'url' and 'parts': {connection}")
        raise HTTPException(400, f"Connection must have either 'url' or 'parts'. Received: {list(connection.keys()) if connection else 'None'}")
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
        logger.error(f"Error creating engine: {e}", exc_info=True)
        raise HTTPException(400, f"Invalid connection details: {e}")


@router.post("/schema-id")
def get_schema_id(payload: dict = Body(...)):
    """
    Get schema ID for a connection without training.
    Useful for checking if model exists.
    Supports both SQL databases and MongoDB.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info(f"Received payload for /schema-id: {payload}")
        connection = payload.get("connection")
        logger.info(f"Extracted connection: {connection}")
        
        if not connection:
            logger.error("Missing 'connection' in payload")
            raise HTTPException(400, "Missing 'connection' in payload")
        
        # Check for MongoDB
        url_str = (connection.get("url") or "").strip()
        if url_str and is_mongodb_url(url_str):
            logger.info("MongoDB detected, using MongoDB schema crawler")
            metadata = crawl_mongodb_schema(url_str, sample_size=50)
        else:
            # SQL database
            eng = _engine_from_connection(connection)
            logger.info(f"Engine created successfully")
            metadata = crawl_schema(eng, sample_size=50)  # Quick crawl
        
        schema_id = _trainer.generate_schema_id(metadata)
        logger.info(f"Schema ID generated: {schema_id}")
        
        return {
            "ok": True,
            "schema_id": schema_id,
            "tables_count": len(metadata.tables),
        }
    except HTTPException:
        raise
    except SQLAlchemyError as e:
        logger.error(f"Schema crawl failed: {e}", exc_info=True)
        raise HTTPException(400, f"Schema crawl failed: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in /schema-id: {e}", exc_info=True)
        raise HTTPException(500, f"Internal error: {e}")


@router.post("/train")
def train_model(
    payload: dict = Body(...),
    background_tasks: BackgroundTasks = None
):
    """
    Train a model for a schema.
    
    Body: {
        "connection": { "url": "..." },
        "force_retrain": false,  # If true, retrain even if model exists
        "use_llm_for_training": true  # Generate synthetic training data
    }
    
    Returns: { ok, schema_id, status, message }
    Supports both SQL databases and MongoDB.
    """
    import logging
    logger = logging.getLogger(__name__)
    
    connection = payload.get("connection")
    force_retrain = payload.get("force_retrain", False)
    use_llm = payload.get("use_llm_for_training", True)
    
    try:
        # Check for MongoDB
        url_str = (connection.get("url") or "").strip() if connection else ""
        if url_str and is_mongodb_url(url_str):
            logger.info("MongoDB detected for model training, using MongoDB schema crawler")
            metadata = crawl_mongodb_schema(url_str, sample_size=100)
        else:
            # SQL database
            eng = _engine_from_connection(connection)
            # Crawl schema
            metadata = crawl_schema(eng, sample_size=100)
        
        # Check if model already exists
        schema_id = _trainer.generate_schema_id(metadata)
        existing_model = _trainer.load_model(schema_id)
        
        if existing_model and not force_retrain:
            return {
                "ok": True,
                "schema_id": schema_id,
                "status": "exists",
                "message": f"Model already exists for schema {schema_id}",
                "model_info": existing_model.to_dict(),
            }
        
        # Check if already training
        if is_training(schema_id):
            return {
                "ok": True,
                "schema_id": schema_id,
                "status": "training",
                "message": "Training already in progress",
            }
        
        # Train model in background
        # Capture metadata and schema_id in closure for MongoDB support
        def train_task(metadata_to_use, sid):
            try:
                model = _trainer.train(metadata_to_use, use_llm_for_training=use_llm, track_progress=True)
                _trainer.save_model(model)
            except Exception as e:
                from .models.progress import set_error
                set_error(sid, str(e))
                logger.error(f"Training error: {e}", exc_info=True)
        
        background_tasks.add_task(train_task, metadata, schema_id)
        return {
            "ok": True,
            "schema_id": schema_id,
            "status": "training",
            "message": "Training started in background",
        }
            
    except SQLAlchemyError as e:
        raise HTTPException(400, f"Schema crawl failed: {e}")
    except Exception as e:
        raise HTTPException(500, f"Training failed: {e}")


@router.get("/{schema_id}/progress")
def get_training_progress(schema_id: str):
    """Get training progress for a schema"""
    progress = get_progress(schema_id)
    if not progress:
        # Check if model exists (training completed)
        model = _trainer.load_model(schema_id)
        if model:
            return {
                "ok": True,
                "schema_id": schema_id,
                "status": "completed",
                "overall_progress": 100,
                "model_info": model.to_dict(),
            }
        return {
            "ok": False,
            "schema_id": schema_id,
            "status": "not_found",
            "message": "No training in progress and no model found",
        }
    return {"ok": True, **progress}


@router.get("/{schema_id}/status")
def get_model_status(schema_id: str):
    """Get model status (exists, training, or not found)"""
    # Check if training
    if is_training(schema_id):
        progress = get_progress(schema_id)
        return {
            "ok": True,
            "schema_id": schema_id,
            "status": "training",
            "progress": progress,
        }
    
    # Check if model exists
    model = _trainer.load_model(schema_id)
    if model:
        return {
            "ok": True,
            "schema_id": schema_id,
            "status": "ready",
            "model_info": model.to_dict(),
        }
    
    return {
        "ok": False,
        "schema_id": schema_id,
        "status": "not_found",
        "message": "Model not found. Training required.",
    }


@router.get("/list")
def list_models():
    """List all trained models"""
    try:
        models = _trainer.list_models()
        return {"ok": True, "models": models}
    except Exception as e:
        raise HTTPException(500, f"Failed to list models: {e}")


@router.get("/{schema_id}")
def get_model(schema_id: str):
    """Get information about a specific model"""
    try:
        model = _trainer.load_model(schema_id)
        if not model:
            raise HTTPException(404, f"Model {schema_id} not found")
        return {"ok": True, "model": model.to_dict()}
    except Exception as e:
        raise HTTPException(500, f"Failed to load model: {e}")


@router.delete("/{schema_id}")
def delete_model(schema_id: str):
    """Delete a trained model"""
    try:
        model_path = _trainer.get_model_path(schema_id)
        if not model_path.exists():
            raise HTTPException(404, f"Model {schema_id} not found")
        
        import shutil
        shutil.rmtree(model_path)
        return {"ok": True, "message": f"Model {schema_id} deleted"}
    except Exception as e:
        raise HTTPException(500, f"Failed to delete model: {e}")


@router.post("/{schema_id}/query")
def generate_query_with_model(
    schema_id: str,
    payload: dict = Body(...)
):
    """
    Generate SQL query using a trained model.
    
    Body: {
        "question": "natural language question",
        "connection": { "url": "..." }  # Optional, for validation
    }
    
    Returns: { ok, sql, explanation, confidence, metadata }
    """
    question = (payload.get("question") or "").strip()
    if not question:
        raise HTTPException(400, "Missing 'question'")
    
    try:
        # Load model
        model = _trainer.load_model(schema_id)
        if not model:
            raise HTTPException(404, f"Model {schema_id} not found")
        
        # Generate query
        generator = SchemaQueryGenerator(model)
        sql, metadata = generator.generate_query(question)
        
        # Generate explanation
        explanation = generator.explain_query(sql, question)
        
        return {
            "ok": True,
            "sql": sql,
            "explanation": explanation,
            "confidence": metadata.get("confidence", 0.5),
            "metadata": metadata,
        }
    except Exception as e:
        raise HTTPException(500, f"Query generation failed: {e}")
