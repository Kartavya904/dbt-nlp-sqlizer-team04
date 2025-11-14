# app/models/trainer.py
"""
Model training pipeline for schema-specific NL->SQL models.
Creates embeddings and fine-tuned models that understand the schema deeply.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import json
import os
import hashlib
from pathlib import Path
import numpy as np
from datetime import datetime

try:
    from sentence_transformers import SentenceTransformer
    import torch
    HAS_ML_DEPS = True
except ImportError:
    HAS_ML_DEPS = False
    SentenceTransformer = None
    torch = None

from ..schema.crawler import SchemaMetadata
from ..ai.llm import chat_complete, LLMNotConfigured
from .progress import set_progress, clear_progress, set_error


class SchemaModel:
    """A trained model for a specific database schema"""
    def __init__(self, schema_id: str, metadata: SchemaMetadata):
        self.schema_id = schema_id
        self.metadata = metadata
        self.embeddings: Optional[Dict[str, np.ndarray]] = None
        self.encoder: Optional[SentenceTransformer] = None
        self.trained_at: Optional[datetime] = None
        self.training_samples: int = 0
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_id": self.schema_id,
            "trained_at": self.trained_at.isoformat() if self.trained_at else None,
            "training_samples": self.training_samples,
            "tables_count": len(self.metadata.tables),
            "relationships_count": len(self.metadata.relationships),
        }
    
    def save(self, model_dir: Path):
        """Save model to disk"""
        model_dir.mkdir(parents=True, exist_ok=True)
        
        # Save metadata
        with open(model_dir / "metadata.json", "w") as f:
            json.dump(self.metadata.to_dict(), f, indent=2, default=str)
        
        # Save model info
        with open(model_dir / "model_info.json", "w") as f:
            json.dump(self.to_dict(), f, indent=2, default=str)
        
        # Save embeddings if available
        if self.embeddings:
            embeddings_file = model_dir / "embeddings.npz"
            np.savez_compressed(embeddings_file, **{k: v for k, v in self.embeddings.items()})
        
        # Save encoder if available (sentence-transformers can save itself)
        if self.encoder:
            encoder_dir = model_dir / "encoder"
            self.encoder.save(str(encoder_dir))
    
    @classmethod
    def load(cls, model_dir: Path) -> SchemaModel:
        """Load model from disk"""
        # Load model info
        with open(model_dir / "model_info.json", "r") as f:
            info = json.load(f)
        
        # Load metadata
        with open(model_dir / "metadata.json", "r") as f:
            metadata_dict = json.load(f)
        
        metadata = SchemaMetadata.from_dict(metadata_dict)
        model = cls(info["schema_id"], metadata)
        model.trained_at = datetime.fromisoformat(info["trained_at"]) if info.get("trained_at") else None
        model.training_samples = info.get("training_samples", 0)
        
        # Load embeddings
        embeddings_file = model_dir / "embeddings.npz"
        if embeddings_file.exists():
            loaded = np.load(embeddings_file, allow_pickle=True)
            model.embeddings = {k: loaded[k] for k in loaded.files}
        
        # Load encoder
        encoder_dir = model_dir / "encoder"
        if encoder_dir.exists() and HAS_ML_DEPS:
            model.encoder = SentenceTransformer(str(encoder_dir))
        
        return model


class ModelTrainer:
    """Trains schema-specific models"""
    
    def __init__(self, models_dir: Path = Path("models")):
        self.models_dir = Path(models_dir)
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        if not HAS_ML_DEPS:
            print("Warning: ML dependencies not installed. Install with: pip install sentence-transformers torch")
    
    def generate_schema_id(self, metadata: SchemaMetadata) -> str:
        """Generate a unique ID for a schema based on its structure"""
        # Create a hash from table names and column names
        schema_str = json.dumps({
            "tables": sorted(metadata.tables.keys()),
            "columns": {t: [c.name for c in info.columns] for t, info in metadata.tables.items()}
        }, sort_keys=True)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]
    
    def train(
        self,
        metadata: SchemaMetadata,
        training_samples: Optional[List[Tuple[str, str]]] = None,
        use_llm_for_training: bool = True,
        track_progress: bool = True
    ) -> SchemaModel:
        """
        Train a model for the given schema.
        
        Args:
            metadata: Schema metadata from crawler
            training_samples: Optional list of (question, sql) pairs for training
            use_llm_for_training: If True, generate synthetic training data using LLM
        """
        if not HAS_ML_DEPS:
            raise RuntimeError("ML dependencies not installed. Install: pip install sentence-transformers torch")
        
        schema_id = self.generate_schema_id(metadata)
        model = SchemaModel(schema_id, metadata)
        model.trained_at = datetime.now()
        
        try:
            # Stage 1: Generate training data
            if track_progress:
                set_progress(schema_id, "data_generation", 0, "Generating training data...")
            
            if training_samples is None and use_llm_for_training:
                training_samples = self._generate_training_data(metadata)
            
            model.training_samples = len(training_samples) if training_samples else 0
            
            if track_progress:
                set_progress(schema_id, "data_generation", 100, f"Generated {model.training_samples} training samples")
            
            # Stage 2: Create embeddings
            if track_progress:
                set_progress(schema_id, "embeddings", 0, "Creating schema embeddings...")
            
            total_elements = len(metadata.tables) + sum(len(t.columns) for t in metadata.tables.values()) + len(metadata.relationships)
            model.embeddings = self._create_schema_embeddings(metadata, schema_id if track_progress else None, total_elements)
            
            if track_progress:
                set_progress(schema_id, "embeddings", 100, f"Created embeddings for {total_elements} schema elements")
            
            # Stage 3: Fine-tune encoder
            if track_progress:
                set_progress(schema_id, "fine_tuning", 0, "Fine-tuning encoder...")
            
            if training_samples:
                model.encoder = self._fine_tune_encoder(metadata, training_samples)
            else:
                # Use base model if no training data
                model.encoder = SentenceTransformer('all-MiniLM-L6-v2')
            
            if track_progress:
                set_progress(schema_id, "fine_tuning", 100, "Encoder ready")
                clear_progress(schema_id)
            
            return model
        except Exception as e:
            if track_progress:
                set_error(schema_id, str(e))
            raise
    
    def _create_schema_embeddings(
        self,
        metadata: SchemaMetadata,
        schema_id: Optional[str] = None,
        total_elements: Optional[int] = None
    ) -> Dict[str, np.ndarray]:
        """Create embeddings for all schema elements (tables, columns, relationships)"""
        if not HAS_ML_DEPS:
            return {}
        
        encoder = SentenceTransformer('all-MiniLM-L6-v2')
        embeddings = {}
        
        processed = 0
        
        # Embed table names
        for table_name, table_info in metadata.tables.items():
            # Table name embedding
            table_text = f"table {table_name}"
            embeddings[f"table:{table_name}"] = encoder.encode(table_text)
            processed += 1
            
            if schema_id and total_elements:
                progress = (processed / total_elements) * 100
                set_progress(schema_id, "embeddings", progress, f"Processing {table_name}...")
            
            # Column embeddings
            for col in table_info.columns:
                col_text = f"column {table_name}.{col.name} type {col.type_str}"
                if col.is_primary_key:
                    col_text += " primary key"
                if col.is_foreign_key:
                    col_text += " foreign key"
                embeddings[f"column:{table_name}.{col.name}"] = encoder.encode(col_text)
                processed += 1
                
                if schema_id and total_elements and processed % 10 == 0:
                    progress = (processed / total_elements) * 100
                    set_progress(schema_id, "embeddings", progress, f"Processed {processed}/{total_elements} elements...")
        
        # Embed relationships
        for rel in metadata.relationships:
            rel_text = f"relationship {rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}"
            embeddings[f"fk:{rel.from_table}.{rel.from_column}"] = encoder.encode(rel_text)
            processed += 1
        
        return embeddings
    
    def _generate_training_data(self, metadata: SchemaMetadata) -> List[Tuple[str, str]]:
        """
        Generate synthetic training data by asking LLM to create NL->SQL pairs
        based on the schema.
        """
        try:
            # Build schema description
            schema_desc = self._describe_schema(metadata)
            
            prompt = f"""You are generating training examples for a natural language to SQL system.

Schema:
{schema_desc}

Generate 20 diverse natural language questions and their corresponding SQL queries.
Format as JSON array: [{{"question": "...", "sql": "..."}}]

Rules:
- Only SELECT queries
- Use explicit JOINs
- Include WHERE, GROUP BY, ORDER BY as appropriate
- Questions should be realistic and diverse
- SQL should be valid PostgreSQL
"""
            
            response = chat_complete(
                "You are a helpful assistant that generates training data.",
                prompt
            )
            
            # Parse JSON response
            import re
            json_match = re.search(r'\[.*\]', response, re.DOTALL)
            if json_match:
                examples = json.loads(json_match.group(0))
                return [(ex["question"], ex["sql"]) for ex in examples if "question" in ex and "sql" in ex]
            
        except (LLMNotConfigured, Exception) as e:
            print(f"Could not generate training data with LLM: {e}")
        
        # Fallback: generate simple examples from schema structure
        return self._generate_simple_examples(metadata)
    
    def _generate_simple_examples(self, metadata: SchemaMetadata) -> List[Tuple[str, str]]:
        """Generate simple training examples from schema structure"""
        examples = []
        
        for table_name, table_info in metadata.tables.items():
            # Simple SELECT examples
            if table_info.columns:
                col_names = [c.name for c in table_info.columns[:5]]  # First 5 columns
                sql = f'SELECT {", ".join(col_names)} FROM "{table_name}" LIMIT 10'
                question = f"Show me {table_name}"
                examples.append((question, sql))
                
                # COUNT example
                sql = f'SELECT COUNT(*) FROM "{table_name}"'
                question = f"How many records are in {table_name}?"
                examples.append((question, sql))
        
        # JOIN examples
        if len(metadata.relationships) > 0:
            rel = metadata.relationships[0]
            sql = f'''
                SELECT * FROM "{rel.from_table}" f
                JOIN "{rel.to_table}" t ON f."{rel.from_column}" = t."{rel.to_column}"
                LIMIT 10
            '''
            question = f"Join {rel.from_table} with {rel.to_table}"
            examples.append((question, sql))
        
        return examples[:20]  # Limit to 20 examples
    
    def _describe_schema(self, metadata: SchemaMetadata) -> str:
        """Create a text description of the schema"""
        lines = []
        for table_name, table_info in metadata.tables.items():
            lines.append(f"\nTable: {table_name}")
            if table_info.primary_key:
                lines.append(f"  Primary Key: {', '.join(table_info.primary_key)}")
            for col in table_info.columns:
                pk_marker = " (PK)" if col.is_primary_key else ""
                fk_marker = " (FK)" if col.is_foreign_key else ""
                lines.append(f"  - {col.name}: {col.type_str}{pk_marker}{fk_marker}")
        
        if metadata.relationships:
            lines.append("\nRelationships:")
            for rel in metadata.relationships:
                lines.append(f"  {rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}")
        
        return "\n".join(lines)
    
    def _fine_tune_encoder(
        self,
        metadata: SchemaMetadata,
        training_samples: List[Tuple[str, str]]
    ) -> SentenceTransformer:
        """
        Fine-tune the encoder on schema-specific training data.
        For now, we use the base model. In a full implementation, you'd fine-tune here.
        """
        # For v1, we'll use the base model
        # In a full implementation, you could fine-tune using the training samples
        encoder = SentenceTransformer('all-MiniLM-L6-v2')
        
        # TODO: Implement actual fine-tuning using training_samples
        # This would involve:
        # 1. Creating positive/negative pairs from training data
        # 2. Training with contrastive loss
        # 3. Saving the fine-tuned model
        
        return encoder
    
    def get_model_path(self, schema_id: str) -> Path:
        """Get the path where a model should be stored"""
        return self.models_dir / schema_id
    
    def save_model(self, model: SchemaModel):
        """Save a trained model"""
        model_path = self.get_model_path(model.schema_id)
        model.save(model_path)
    
    def load_model(self, schema_id: str) -> Optional[SchemaModel]:
        """Load a trained model"""
        model_path = self.get_model_path(schema_id)
        if not model_path.exists():
            return None
        return SchemaModel.load(model_path)
    
    def list_models(self) -> List[Dict[str, Any]]:
        """List all trained models"""
        models = []
        for model_dir in self.models_dir.iterdir():
            if model_dir.is_dir() and (model_dir / "model_info.json").exists():
                try:
                    model = SchemaModel.load(model_dir)
                    models.append(model.to_dict())
                except Exception as e:
                    print(f"Error loading model {model_dir}: {e}")
        return models

