# app/schema/crawler.py
"""
Enhanced schema crawler that extracts comprehensive metadata:
- Tables, columns, types, constraints
- Primary keys and foreign key relationships
- Data samples and statistics
- Column value distributions
- Common patterns and synonyms
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Set, Tuple
from sqlalchemy import inspect, text, MetaData, Table
from sqlalchemy.engine import Engine, Connection
from collections import defaultdict
import json


class SchemaMetadata:
    """Rich schema metadata container"""
    def __init__(self):
        self.tables: Dict[str, TableInfo] = {}
        self.relationships: List[FKRelationship] = []
        self.synonyms: Dict[str, List[str]] = {}  # column_name -> [synonyms]
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "tables": {name: t.to_dict() for name, t in self.tables.items()},
            "relationships": [r.to_dict() for r in self.relationships],
            "synonyms": self.synonyms,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> SchemaMetadata:
        sm = cls()
        sm.tables = {name: TableInfo.from_dict(t) for name, t in data.get("tables", {}).items()}
        sm.relationships = [FKRelationship.from_dict(r) for r in data.get("relationships", [])]
        sm.synonyms = data.get("synonyms", {})
        return sm


class TableInfo:
    """Information about a database table"""
    def __init__(self, name: str):
        self.name = name
        self.columns: List[ColumnInfo] = []
        self.primary_key: List[str] = []
        self.foreign_keys: List[str] = []  # column names that are FKs
        self.sample_rows: List[Dict[str, Any]] = []
        self.row_count: Optional[int] = None
        self.indexes: List[str] = []
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "primary_key": self.primary_key,
            "foreign_keys": self.foreign_keys,
            "sample_rows": self.sample_rows[:10],  # limit samples
            "row_count": self.row_count,
            "indexes": self.indexes,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TableInfo:
        ti = TableInfo(data["name"])
        ti.columns = [ColumnInfo.from_dict(c) for c in data.get("columns", [])]
        ti.primary_key = data.get("primary_key", [])
        ti.foreign_keys = data.get("foreign_keys", [])
        ti.sample_rows = data.get("sample_rows", [])
        ti.row_count = data.get("row_count")
        ti.indexes = data.get("indexes", [])
        return ti


class ColumnInfo:
    """Information about a database column"""
    def __init__(self, name: str, type_str: str, nullable: bool = True):
        self.name = name
        self.type_str = type_str
        self.nullable = nullable
        self.is_primary_key = False
        self.is_foreign_key = False
        self.unique_values: Optional[List[Any]] = None  # for categorical columns
        self.min_value: Optional[Any] = None
        self.max_value: Optional[Any] = None
        self.avg_value: Optional[float] = None
        self.distinct_count: Optional[int] = None
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "type": self.type_str,
            "nullable": self.nullable,
            "is_primary_key": self.is_primary_key,
            "is_foreign_key": self.is_foreign_key,
            "unique_values": self.unique_values[:50] if self.unique_values else None,  # limit
            "min_value": self.min_value,
            "max_value": self.max_value,
            "avg_value": self.avg_value,
            "distinct_count": self.distinct_count,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ColumnInfo:
        ci = ColumnInfo(data["name"], data["type"], data.get("nullable", True))
        ci.is_primary_key = data.get("is_primary_key", False)
        ci.is_foreign_key = data.get("is_foreign_key", False)
        ci.unique_values = data.get("unique_values")
        ci.min_value = data.get("min_value")
        ci.max_value = data.get("max_value")
        ci.avg_value = data.get("avg_value")
        ci.distinct_count = data.get("distinct_count")
        return ci


class FKRelationship:
    """Foreign key relationship between tables"""
    def __init__(self, from_table: str, from_column: str, to_table: str, to_column: str):
        self.from_table = from_table
        self.from_column = from_column
        self.to_table = to_table
        self.to_column = to_column
        
    def to_dict(self) -> Dict[str, Any]:
        return {
            "from_table": self.from_table,
            "from_column": self.from_column,
            "to_table": self.to_table,
            "to_column": self.to_column,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> FKRelationship:
        return FKRelationship(
            data["from_table"],
            data["from_column"],
            data["to_table"],
            data["to_column"],
        )


def crawl_schema(engine: Engine, sample_size: int = 100) -> SchemaMetadata:
    """
    Comprehensive schema crawler that extracts:
    - Table and column metadata
    - Primary and foreign key relationships
    - Data samples and statistics
    - Value distributions for categorical columns
    """
    metadata = SchemaMetadata()
    insp = inspect(engine)
    
    # Get all tables
    table_names = insp.get_table_names()
    
    for table_name in table_names:
        table_info = TableInfo(table_name)
        
        # Get columns
        columns = insp.get_columns(table_name)
        pk_constraint = insp.get_pk_constraint(table_name)
        pk_columns = set(pk_constraint.get("constrained_columns", []) if pk_constraint else [])
        
        # Get foreign keys
        fk_constraints = insp.get_foreign_keys(table_name)
        fk_columns = set()
        for fk in fk_constraints:
            from_col = fk["constrained_columns"][0] if fk["constrained_columns"] else None
            to_table = fk["referred_table"]
            to_col = fk["referred_columns"][0] if fk["referred_columns"] else None
            if from_col and to_table and to_col:
                fk_columns.add(from_col)
                metadata.relationships.append(
                    FKRelationship(table_name, from_col, to_table, to_col)
                )
        
        # Get indexes
        indexes = insp.get_indexes(table_name)
        table_info.indexes = [idx["name"] for idx in indexes]
        
        # Process each column
        for col in columns:
            col_name = col["name"]
            col_type = str(col["type"])
            col_nullable = col.get("nullable", True)
            
            col_info = ColumnInfo(col_name, col_type, col_nullable)
            col_info.is_primary_key = col_name in pk_columns
            col_info.is_foreign_key = col_name in fk_columns
            
            table_info.columns.append(col_info)
        
        table_info.primary_key = list(pk_columns)
        table_info.foreign_keys = list(fk_columns)
        
        # Get sample data and statistics
        try:
            with engine.connect() as conn:
                # Row count
                count_result = conn.exec_driver_sql(f'SELECT COUNT(*) FROM "{table_name}"').scalar()
                table_info.row_count = int(count_result) if count_result else 0
                
                # Sample rows (limit to avoid memory issues)
                if table_info.row_count > 0:
                    sample_query = f'SELECT * FROM "{table_name}" LIMIT {sample_size}'
                    sample_result = conn.exec_driver_sql(sample_query)
                    rows = sample_result.fetchall()
                    
                    # Get column names from cursor description
                    if rows:
                        cursor = getattr(sample_result, 'cursor', None)
                        if cursor and hasattr(cursor, 'description') and cursor.description:
                            col_names = [desc[0] for desc in cursor.description]
                            for row in rows:
                                table_info.sample_rows.append(dict(zip(col_names, row)))
                        else:
                            # Fallback: use column metadata if cursor.description not available
                            col_names = [col.name for col in table_info.columns]
                            for row in rows:
                                table_info.sample_rows.append(dict(zip(col_names, row)))
                    
                    # Column statistics
                    for col_info in table_info.columns:
                        _analyze_column(conn, table_name, col_info, table_info.row_count)
        except Exception as e:
            # If we can't sample, continue with metadata only
            print(f"Warning: Could not sample table {table_name}: {e}")
        
        metadata.tables[table_name] = table_info
    
    # Extract synonyms from column names and sample data
    metadata.synonyms = _extract_synonyms(metadata)
    
    return metadata


def _analyze_column(conn: Connection, table_name: str, col_info: ColumnInfo, total_rows: int):
    """Analyze a column to extract statistics and value distributions"""
    try:
        col_name = col_info.name
        col_type_lower = col_info.type_str.lower()
        
        # Distinct count
        distinct_query = f'SELECT COUNT(DISTINCT "{col_name}") FROM "{table_name}"'
        distinct_result = conn.exec_driver_sql(distinct_query).scalar()
        col_info.distinct_count = int(distinct_result) if distinct_result else 0
        
        # For categorical columns (low distinct count), get unique values
        if col_info.distinct_count > 0 and col_info.distinct_count <= 100:
            unique_query = f'SELECT DISTINCT "{col_name}" FROM "{table_name}" ORDER BY "{col_name}" LIMIT 100'
            unique_result = conn.exec_driver_sql(unique_query)
            col_info.unique_values = [row[0] for row in unique_result.fetchall()]
        
        # For numeric columns, get min/max/avg
        if any(t in col_type_lower for t in ["int", "numeric", "decimal", "float", "double", "real"]):
            try:
                stats_query = f'''
                    SELECT 
                        MIN("{col_name}") as min_val,
                        MAX("{col_name}") as max_val,
                        AVG("{col_name}") as avg_val
                    FROM "{table_name}"
                    WHERE "{col_name}" IS NOT NULL
                '''
                stats_result = conn.exec_driver_sql(stats_query).fetchone()
                if stats_result:
                    col_info.min_value = stats_result[0]
                    col_info.max_value = stats_result[1]
                    col_info.avg_value = float(stats_result[2]) if stats_result[2] else None
            except Exception:
                pass  # Skip if column type doesn't support these operations
                
    except Exception as e:
        # Skip analysis if it fails
        pass


def _extract_synonyms(metadata: SchemaMetadata) -> Dict[str, List[str]]:
    """
    Extract potential synonyms from:
    - Column names (e.g., "student_id" -> ["student", "pupil", "learner"])
    - Table names
    - Sample data values
    """
    synonyms: Dict[str, List[str]] = defaultdict(list)
    
    # Simple heuristic: split camelCase/snake_case and generate variations
    for table_name, table_info in metadata.tables.items():
        # Table name variations
        base = table_name.lower().replace("_", " ").replace("-", " ")
        if base not in synonyms[table_name]:
            synonyms[table_name].append(base)
        
        # Singular/plural variations
        if table_name.endswith("s") and len(table_name) > 1:
            singular = table_name[:-1]
            synonyms[table_name].append(singular)
            synonyms[singular].append(table_name)
        
        # Column name variations
        for col in table_info.columns:
            col_base = col.name.lower().replace("_", " ").replace("-", " ")
            if col_base not in synonyms[col.name]:
                synonyms[col.name].append(col_base)
            
            # Common patterns
            if col.name.endswith("_id"):
                base_name = col.name[:-3]
                synonyms[col.name].append(base_name)
    
    return dict(synonyms)

