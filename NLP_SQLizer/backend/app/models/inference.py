# app/models/inference.py
"""
Inference system that uses trained schema-specific models to generate SQL queries.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
import numpy as np

try:
    from sentence_transformers import SentenceTransformer
    import torch
    HAS_ML_DEPS = True
except ImportError:
    HAS_ML_DEPS = False
    SentenceTransformer = None
    torch = None

from ..schema.crawler import SchemaMetadata
from .trainer import SchemaModel


class SchemaQueryGenerator:
    """Uses trained schema models to generate SQL queries from natural language"""
    
    def __init__(self, model: SchemaModel):
        self.model = model
        self.metadata = model.metadata
        self.encoder = model.encoder or (SentenceTransformer('all-MiniLM-L6-v2') if HAS_ML_DEPS else None)
        self.embeddings = model.embeddings or {}
    
    def generate_query(
        self,
        question: str,
        max_candidates: int = 5,
        use_llm_fallback: bool = True
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Generate SQL query from natural language question.
        
        Returns:
            (sql_query, metadata) where metadata contains:
            - confidence: float
            - relevant_tables: List[str]
            - relevant_columns: Dict[str, List[str]]
            - explanation: str
        """
        if not HAS_ML_DEPS:
            raise RuntimeError("ML dependencies not installed")
        
        # Step 1: Find relevant schema elements using embeddings
        relevant = self._find_relevant_schema_elements(question)
        
        # Step 2: Generate SQL candidates using schema context
        candidates = self._generate_candidates(question, relevant, max_candidates)
        
        # Step 3: Rank candidates
        ranked = self._rank_candidates(question, candidates, relevant)
        
        # Step 4: Select best candidate
        if ranked:
            best_sql, best_metadata = ranked[0]
            return best_sql, best_metadata
        
        # Fallback to LLM if no candidates
        if use_llm_fallback:
            return self._llm_fallback(question, relevant)
        
        raise ValueError("Could not generate query")
    
    def _find_relevant_schema_elements(self, question: str) -> Dict[str, Any]:
        """Find relevant tables and columns using semantic similarity"""
        if not self.encoder or not self.embeddings:
            # Fallback to simple keyword matching
            return self._keyword_match(question)
        
        # Encode question
        question_embedding = self.encoder.encode(question)
        
        # Find similar schema elements
        similarities = {}
        for key, embedding in self.embeddings.items():
            similarity = np.dot(question_embedding, embedding) / (
                np.linalg.norm(question_embedding) * np.linalg.norm(embedding)
            )
            similarities[key] = float(similarity)
        
        # Extract relevant tables and columns
        relevant_tables = set()
        relevant_columns = {}
        
        # Sort by similarity
        sorted_items = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
        
        for key, score in sorted_items[:20]:  # Top 20 matches
            if score < 0.3:  # Threshold
                break
            
            if key.startswith("table:"):
                table_name = key.split(":", 1)[1]
                relevant_tables.add(table_name)
            elif key.startswith("column:"):
                parts = key.split(":", 1)[1].split(".", 1)
                if len(parts) == 2:
                    table_name, col_name = parts
                    if table_name not in relevant_columns:
                        relevant_columns[table_name] = []
                    relevant_columns[table_name].append(col_name)
        
        # Ensure we have at least some tables
        if not relevant_tables and self.metadata.tables:
            # Fallback: use all tables
            relevant_tables = set(self.metadata.tables.keys())
        
        return {
            "tables": list(relevant_tables),
            "columns": relevant_columns,
            "similarities": similarities,
        }
    
    def _keyword_match(self, question: str) -> Dict[str, Any]:
        """Fallback keyword matching when embeddings aren't available"""
        question_lower = question.lower()
        relevant_tables = []
        relevant_columns = {}
        
        for table_name, table_info in self.metadata.tables.items():
            if table_name.lower() in question_lower:
                relevant_tables.append(table_name)
                relevant_columns[table_name] = [c.name for c in table_info.columns[:10]]
            else:
                # Check column names
                for col in table_info.columns:
                    if col.name.lower() in question_lower:
                        if table_name not in relevant_tables:
                            relevant_tables.append(table_name)
                        if table_name not in relevant_columns:
                            relevant_columns[table_name] = []
                        relevant_columns[table_name].append(col.name)
        
        if not relevant_tables:
            # Use all tables as fallback
            relevant_tables = list(self.metadata.tables.keys())
            for table_name in relevant_tables:
                relevant_columns[table_name] = [c.name for c in self.metadata.tables[table_name].columns[:10]]
        
        return {
            "tables": relevant_tables[:5],  # Limit to 5 tables
            "columns": relevant_columns,
        }
    
    def _generate_candidates(
        self,
        question: str,
        relevant: Dict[str, Any],
        max_candidates: int
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """Generate multiple SQL query candidates"""
        candidates = []
        
        # Use LLM to generate candidates with rich schema context
        try:
            from ..ai.llm import chat_complete
            
            schema_context = self._build_schema_context(relevant)
            
            prompt = f"""Generate {max_candidates} different SQL queries for this question: "{question}"

Schema context:
{schema_context}

Rules:
- Only SELECT queries
- Use explicit JOINs based on foreign key relationships
- Include appropriate WHERE, GROUP BY, ORDER BY clauses
- Always include LIMIT 100
- Return {max_candidates} queries, one per line, prefixed with "QUERY:"
"""
            
            response = chat_complete(
                "You are a SQL expert that generates safe, read-only queries.",
                prompt
            )
            
            # Parse queries from response
            for line in response.split("\n"):
                if line.strip().startswith("QUERY:") or line.strip().startswith("SELECT"):
                    sql = line.replace("QUERY:", "").strip()
                    if sql.startswith("SELECT"):
                        candidates.append((sql, {"method": "llm", "confidence": 0.8}))
            
        except Exception as e:
            print(f"Error generating candidates with LLM: {e}")
        
        # Generate template-based candidates as fallback
        if not candidates:
            candidates = self._generate_template_candidates(question, relevant)
        
        return candidates[:max_candidates]
    
    def _generate_template_candidates(
        self,
        question: str,
        relevant: Dict[str, Any]
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """Generate candidates using templates"""
        candidates = []
        tables = relevant.get("tables", [])
        
        if not tables:
            return candidates
        
        # Simple SELECT from first table
        table_name = tables[0]
        table_info = self.metadata.tables.get(table_name)
        if table_info and table_info.columns:
            cols = [c.name for c in table_info.columns[:5]]
            sql = f'SELECT {", ".join(cols)} FROM "{table_name}" LIMIT 100'
            candidates.append((sql, {"method": "template", "confidence": 0.5}))
        
        # JOIN candidate if relationships exist
        if len(tables) >= 2 and self.metadata.relationships:
            rel = self.metadata.relationships[0]
            if rel.from_table in tables and rel.to_table in tables:
                sql = f'''
                    SELECT * FROM "{rel.from_table}" f
                    JOIN "{rel.to_table}" t ON f."{rel.from_column}" = t."{rel.to_column}"
                    LIMIT 100
                '''
                candidates.append((sql, {"method": "template_join", "confidence": 0.6}))
        
        return candidates
    
    def _build_schema_context(self, relevant: Dict[str, Any]) -> str:
        """Build a detailed schema context string"""
        lines = []
        tables = relevant.get("tables", [])
        
        for table_name in tables[:5]:  # Limit to 5 tables
            table_info = self.metadata.tables.get(table_name)
            if not table_info:
                continue
            
            lines.append(f"\nTable: {table_name}")
            if table_info.primary_key:
                lines.append(f"  PK: {', '.join(table_info.primary_key)}")
            
            cols = relevant.get("columns", {}).get(table_name, [])
            if not cols:
                cols = [c.name for c in table_info.columns[:10]]
            
            for col_name in cols:
                col = next((c for c in table_info.columns if c.name == col_name), None)
                if col:
                    pk_marker = " (PK)" if col.is_primary_key else ""
                    fk_marker = " (FK)" if col.is_foreign_key else ""
                    lines.append(f"  - {col.name}: {col.type_str}{pk_marker}{fk_marker}")
        
        # Add relationships
        rels = [r for r in self.metadata.relationships 
                if r.from_table in tables or r.to_table in tables]
        if rels:
            lines.append("\nRelationships:")
            for rel in rels[:5]:
                lines.append(f"  {rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column}")
        
        return "\n".join(lines)
    
    def _rank_candidates(
        self,
        question: str,
        candidates: List[Tuple[str, Dict[str, Any]]],
        relevant: Dict[str, Any]
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """Rank SQL candidates by relevance and quality"""
        if not candidates:
            return []
        
        ranked = []
        for sql, metadata in candidates:
            score = metadata.get("confidence", 0.5)
            
            # Boost score if SQL uses relevant tables
            sql_lower = sql.lower()
            relevant_tables = relevant.get("tables", [])
            for table in relevant_tables:
                if f'"{table}"' in sql or f" {table} " in sql_lower:
                    score += 0.1
            
            # Boost if uses JOINs (more sophisticated)
            if "join" in sql_lower:
                score += 0.1
            
            metadata["confidence"] = min(score, 1.0)
            ranked.append((sql, metadata))
        
        # Sort by confidence
        ranked.sort(key=lambda x: x[1].get("confidence", 0), reverse=True)
        return ranked
    
    def _llm_fallback(self, question: str, relevant: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """Fallback to LLM-based generation"""
        from ..ai.nl2sql import ask_llm
        
        # Build simple context
        context = {}
        for table_name in relevant.get("tables", [])[:4]:
            table_info = self.metadata.tables.get(table_name)
            if table_info:
                cols = relevant.get("columns", {}).get(table_name, [])
                if not cols:
                    cols = [c.name for c in table_info.columns[:8]]
                context[table_name] = cols
        
        sql = ask_llm(question, context)
        return sql, {
            "confidence": 0.7,
            "method": "llm_fallback",
            "relevant_tables": relevant.get("tables", []),
            "relevant_columns": relevant.get("columns", {}),
        }
    
    def explain_query(self, sql: str, question: str) -> str:
        """Generate a human-readable explanation of the query"""
        # Simple explanation based on SQL structure
        explanation_parts = []
        
        sql_lower = sql.lower()
        
        # Extract tables
        for table_name in self.metadata.tables.keys():
            if f'"{table_name}"' in sql or f" {table_name} " in sql_lower:
                explanation_parts.append(f"queries the {table_name} table")
        
        # Check for JOINs
        if "join" in sql_lower:
            explanation_parts.append("joins related tables")
        
        # Check for aggregations
        if any(op in sql_lower for op in ["count(", "sum(", "avg(", "max(", "min("]):
            explanation_parts.append("performs aggregations")
        
        # Check for filters
        if "where" in sql_lower:
            explanation_parts.append("applies filters")
        
        if "group by" in sql_lower:
            explanation_parts.append("groups results")
        
        if "order by" in sql_lower:
            explanation_parts.append("sorts results")
        
        if explanation_parts:
            return "This query " + ", ".join(explanation_parts) + "."
        return "This query retrieves data from the database."

