# app/models/progress.py
"""
Progress tracking for model training.
Uses in-memory storage for training progress (can be extended to Redis/database).
"""
from typing import Dict, Optional
from threading import Lock
from datetime import datetime

# In-memory progress storage
_training_progress: Dict[str, Dict] = {}
_progress_lock = Lock()


def set_progress(schema_id: str, stage: str, progress: float, message: str = ""):
    """Update training progress for a schema"""
    with _progress_lock:
        if schema_id not in _training_progress:
            _training_progress[schema_id] = {
                "schema_id": schema_id,
                "status": "training",
                "started_at": datetime.now().isoformat(),
                "stages": {},
            }
        _training_progress[schema_id]["stages"][stage] = {
            "progress": progress,
            "message": message,
            "updated_at": datetime.now().isoformat(),
        }
        # Calculate overall progress
        stages = _training_progress[schema_id]["stages"]
        if stages:
            overall = sum(s.get("progress", 0) for s in stages.values()) / len(stages)
            _training_progress[schema_id]["overall_progress"] = overall
        _training_progress[schema_id]["updated_at"] = datetime.now().isoformat()


def get_progress(schema_id: str) -> Optional[Dict]:
    """Get current training progress for a schema"""
    with _progress_lock:
        return _training_progress.get(schema_id)


def clear_progress(schema_id: str):
    """Clear progress after training completes"""
    with _progress_lock:
        if schema_id in _training_progress:
            _training_progress[schema_id]["status"] = "completed"
            _training_progress[schema_id]["completed_at"] = datetime.now().isoformat()
            # Keep for a while, then can be cleaned up
            # For now, we keep it for status checking


def set_error(schema_id: str, error: str):
    """Mark training as failed"""
    with _progress_lock:
        if schema_id not in _training_progress:
            _training_progress[schema_id] = {"schema_id": schema_id}
        _training_progress[schema_id]["status"] = "error"
        _training_progress[schema_id]["error"] = error
        _training_progress[schema_id]["updated_at"] = datetime.now().isoformat()


def is_training(schema_id: str) -> bool:
    """Check if training is in progress"""
    with _progress_lock:
        progress = _training_progress.get(schema_id)
        return progress and progress.get("status") == "training"

