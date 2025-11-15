# app/ai/nl2mongo.py
"""
Natural Language to MongoDB Query Generator
Converts natural language questions to MongoDB aggregation pipelines or find queries.
"""
from __future__ import annotations
from typing import Any, Dict, List, Tuple, Optional
from rapidfuzz import fuzz
import json
from .llm import chat_complete, LLMNotConfigured


# ---------- Schema utilities ----------

def load_mongodb_schema(connection_url: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Load MongoDB schema from connection.
    Returns: { collection_name: [{"name": field_name, "type": type_str, ...}] }
    """
    from ..mongodb_adapter import MongoDBConnection, get_mongodb_schema
    
    schema_response = get_mongodb_schema(connection_url)
    schema: Dict[str, List[Dict[str, Any]]] = {}
    
    for table_info in schema_response.get("tables", []):
        collection_name = table_info["table"]
        # Remove database prefix if present (e.g., "dbname.collection" -> "collection")
        if "." in collection_name:
            collection_name = collection_name.split(".", 1)[1]
        
        columns = table_info.get("columns", [])
        schema[collection_name] = columns
    
    return schema


def select_relevant_mongo(schema: Dict[str, List[Dict[str, Any]]], question: str, k_collections: int = 4) -> Dict[str, List[str]]:
    """
    Fuzzy-score collections/fields against the question; return a pruned view: { collection: [fields...] }.
    """
    collections = list(schema.keys())
    collection_scores = [(c, max(
        fuzz.partial_ratio(c, question),
        max((fuzz.partial_ratio(f["name"], question) for f in schema[c]), default=0)
    )) for c in collections]
    collection_scores.sort(key=lambda x: x[1], reverse=True)
    chosen = [c for c, _ in collection_scores[:k_collections]]

    out: Dict[str, List[str]] = {}
    for c in chosen:
        fields = [f["name"] for f in schema[c]]
        # keep top fields plus _id
        field_scores = [(f, fuzz.partial_ratio(f, question)) for f in fields]
        field_scores.sort(key=lambda x: x[1], reverse=True)
        best = [f for f, _ in field_scores[:8]]
        if "_id" in fields and "_id" not in best:
            best.append("_id")
        out[c] = best
    return out


# ---------- Prompting ----------

MONGO_SYS = """Generate ONLY MongoDB query JSON. NO explanations, NO markdown, NO backticks.

Format: {"collection": "name", "pipeline": [...]} OR {"collection": "name", "find": {...}, "limit": 100}
Use pipeline for aggregations/grouping. Use find for simple queries. READ-only. LIMIT 100."""

def render_mongo_context(slice_: Dict[str, List[str]]) -> str:
    lines = []
    for coll, fields in slice_.items():
        field_list = ", ".join(fields)
        lines.append(f"- {coll}({field_list})")
    return "\n".join(lines)


def ask_llm_mongo(question: str, slice_: Dict[str, List[str]]) -> Dict[str, Any]:
    """
    Ask LLM to generate a MongoDB query.
    Returns a dictionary with collection, pipeline/find, etc.
    """
    ctx = render_mongo_context(slice_)
    user = f"""Q: {question}
Schema: {ctx}
Generate MongoDB query JSON only."""
    
    response = chat_complete(MONGO_SYS, user)
    
    # Try to extract JSON from response (might have markdown or extra text)
    response = response.strip()
    if response.startswith("```"):
        # Remove markdown code blocks
        lines = response.split("\n")
        response = "\n".join(lines[1:-1]) if len(lines) > 2 else response
    elif response.startswith("```json"):
        lines = response.split("\n")
        response = "\n".join(lines[1:-1]) if len(lines) > 2 else response
    
    # Parse JSON
    try:
        query_dict = json.loads(response)
        return query_dict
    except json.JSONDecodeError as e:
        # Try to find JSON object in the response
        import re
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', response, re.DOTALL)
        if json_match:
            try:
                query_dict = json.loads(json_match.group(0))
                return query_dict
            except json.JSONDecodeError:
                pass
        raise ValueError(f"Failed to parse MongoDB query from LLM response: {e}. Response: {response}")


# ---------- Query Execution ----------

def execute_mongodb_query(connection_url: str, query_dict: Dict[str, Any], timeout_ms: int = 5000) -> Tuple[List[str], List[List[Any]], int]:
    """
    Execute a MongoDB query and return (columns, rows, rowcount).
    
    Args:
        connection_url: MongoDB connection URL
        query_dict: Query dictionary with collection, pipeline/find, etc.
    
    Returns:
        (columns, rows, rowcount)
    """
    from ..mongodb_adapter import MongoDBConnection, get_mongodb_schema
    
    collection_name = query_dict.get("collection")
    if not collection_name:
        raise ValueError("Query must specify a collection")
    
    # Remove database prefix if present
    if "." in collection_name:
        db_name, collection_name = collection_name.split(".", 1)
    else:
        db_name = None
    
    with MongoDBConnection(connection_url) as mongo:
        # Determine the correct database to use
        if not db_name:
            # First, try to find a database with collections (most reliable)
            all_db_collections = mongo.get_all_databases_with_collections()
            if all_db_collections:
                db_name = list(all_db_collections.keys())[0]
            elif mongo.db_name and mongo.db_name != "admin":
                db_name = mongo.db_name
            else:
                # Last resort: try to get from schema
                try:
                    schema_response = get_mongodb_schema(connection_url)
                    actual_db_name = schema_response.get("database")
                    if actual_db_name and actual_db_name != "admin":
                        db_name = actual_db_name
                    else:
                        db_name = "admin"  # Final fallback
                except Exception:
                    db_name = "admin"  # Final fallback
        
        # Use the determined database
        db = mongo.client[db_name]
        
        collection = db[collection_name]
        
        # Execute query
        if "pipeline" in query_dict:
            # Aggregation pipeline
            pipeline = query_dict["pipeline"]
            # Ensure limit is applied
            has_limit = any("$limit" in stage for stage in pipeline if isinstance(stage, dict))
            if not has_limit:
                pipeline.append({"$limit": query_dict.get("limit", 100)})
            
            cursor = collection.aggregate(pipeline)
            results = list(cursor)
        elif "find" in query_dict:
            # Find query
            find_filter = query_dict["find"]
            projection = query_dict.get("projection")
            sort = query_dict.get("sort")
            limit = query_dict.get("limit", 100)
            
            cursor = collection.find(find_filter, projection)
            if sort:
                cursor = cursor.sort(list(sort.items()))
            cursor = cursor.limit(limit)
            results = list(cursor)
        else:
            raise ValueError("Query must have either 'pipeline' or 'find'")
        
        # Convert results to rows and columns
        if not results:
            return ([], [], 0)
        
        # Get all unique field names from results
        all_fields = set()
        for doc in results:
            all_fields.update(doc.keys())
        
        # Remove _id from fields if present (we'll add it back if needed)
        columns = sorted([f for f in all_fields if f != "_id"])
        if "_id" in all_fields:
            columns.insert(0, "_id")
        
        # Convert documents to rows
        rows = []
        for doc in results:
            row = []
            for col in columns:
                value = doc.get(col)
                # Convert ObjectId and other MongoDB types to strings
                if hasattr(value, '__str__'):
                    # For ObjectId, datetime, etc., convert to string
                    if hasattr(value, 'isoformat'):  # datetime
                        value = value.isoformat()
                    else:
                        value = str(value)
                elif isinstance(value, (dict, list)):
                    # Convert nested objects/arrays to JSON strings
                    value = json.dumps(value)
                row.append(value)
            rows.append(row)
        
        return (columns, rows, len(rows))


def explain_mongodb_query(connection_url: str, query_dict: Dict[str, Any]) -> str:
    """
    Explain a MongoDB query (returns query plan info).
    """
    collection_name = query_dict.get("collection")
    if not collection_name:
        return "No collection specified"
    
    # Remove database prefix if present
    if "." in collection_name:
        db_name, collection_name = collection_name.split(".", 1)
    else:
        db_name = None
    
    try:
        from ..mongodb_adapter import MongoDBConnection, get_mongodb_schema
        
        with MongoDBConnection(connection_url) as mongo:
            # Determine the correct database to use
            if not db_name:
                # First, try to find a database with collections (most reliable)
                all_db_collections = mongo.get_all_databases_with_collections()
                if all_db_collections:
                    db_name = list(all_db_collections.keys())[0]
                elif mongo.db_name and mongo.db_name != "admin":
                    db_name = mongo.db_name
                else:
                    # Last resort: try to get from schema
                    try:
                        schema_response = get_mongodb_schema(connection_url)
                        actual_db_name = schema_response.get("database")
                        if actual_db_name and actual_db_name != "admin":
                            db_name = actual_db_name
                        else:
                            db_name = "admin"  # Final fallback
                    except Exception:
                        db_name = "admin"  # Final fallback
            
            # Use the determined database
            db = mongo.client[db_name]
            
            collection = db[collection_name]
            
            if "pipeline" in query_dict:
                # Explain aggregation pipeline using Database.command
                pipeline = query_dict["pipeline"]
                explain_result = db.command("explain", {"aggregate": collection_name, "pipeline": pipeline, "cursor": {}})
                return json.dumps(explain_result, indent=2, default=str)
            elif "find" in query_dict:
                # Explain find query using Database.command
                find_filter = query_dict["find"]
                projection = query_dict.get("projection")
                sort = query_dict.get("sort")
                limit = query_dict.get("limit", 100)
                
                explain_cmd = {"find": collection_name, "filter": find_filter}
                if projection:
                    explain_cmd["projection"] = projection
                if sort:
                    explain_cmd["sort"] = sort
                if limit:
                    explain_cmd["limit"] = limit
                
                explain_result = db.command("explain", explain_cmd)
                return json.dumps(explain_result, indent=2, default=str)
            else:
                return "Query type not supported for explanation"
    except Exception as e:
        return f"Explain failed: {str(e)}"

