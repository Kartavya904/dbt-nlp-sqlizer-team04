"""
MongoDB adapter for NLP_SQLizer
Handles MongoDB connections and schema inspection
"""
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, parse_qs
import logging

logger = logging.getLogger(__name__)

try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ConfigurationError
    PYMONGO_AVAILABLE = True
except ImportError:
    PYMONGO_AVAILABLE = False
    logger.warning("pymongo not installed. MongoDB support will be limited.")


class MongoDBConnection:
    """Wrapper for MongoDB connection"""
    def __init__(self, url: str):
        if not PYMONGO_AVAILABLE:
            raise ImportError("pymongo is required for MongoDB support. Install it with: pip install pymongo")
        
        self.url = url
        self.client = None
        self.db = None
        self.db_name = None
        self._connect()
    
    def _connect(self):
        """Establish MongoDB connection"""
        try:
            self.client = MongoClient(self.url, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            
            # Extract database name from URL
            parsed = urlparse(self.url)
            # Extract database name from path (e.g., mongodb://host/dbname)
            path_parts = parsed.path.lstrip('/').split('/')
            db_name = path_parts[0] if path_parts and path_parts[0] else None
            
            # If no database in path, check query params for defaultDatabase or authSource
            if not db_name:
                query_params = parse_qs(parsed.query)
                # Try defaultDatabase first (MongoDB Atlas uses this)
                if 'defaultDatabase' in query_params:
                    db_name = query_params['defaultDatabase'][0]
                # Then try authSource
                elif 'authSource' in query_params:
                    db_name = query_params['authSource'][0]
                # Default to 'admin' if nothing specified
                else:
                    db_name = 'admin'
            
            self.db = self.client[db_name]
            self.db_name = db_name
            logger.info(f"MongoDB connected to database: {db_name}")
        except (ConnectionFailure, ConfigurationError) as e:
            logger.error(f"MongoDB connection failed: {e}")
            raise ConnectionError(f"MongoDB connection failed: {e}")
    
    def get_collections(self) -> List[str]:
        """Get list of collection names"""
        if self.db is None:
            return []
        try:
            return self.db.list_collection_names()
        except Exception as e:
            logger.error(f"Error listing collections: {e}")
            return []
    
    def get_all_databases_with_collections(self) -> Dict[str, List[str]]:
        """Get all databases and their collections"""
        if self.client is None:
            return {}
        
        db_collections = {}
        try:
            # List all databases
            db_names = self.client.list_database_names()
            for db_name in db_names:
                # Skip system databases
                if db_name in ['admin', 'local', 'config']:
                    continue
                db = self.client[db_name]
                collections = db.list_collection_names()
                if collections:  # Only include databases with collections
                    db_collections[db_name] = collections
        except Exception as e:
            logger.error(f"Error listing databases: {e}")
        
        return db_collections
    
    def get_collection_schema(self, collection_name: str, sample_size: int = 100) -> Dict[str, Any]:
        """Get schema information for a collection"""
        if self.db is None:
            return {}
        
        collection = self.db[collection_name]
        
        # Sample documents to infer schema
        sample_docs = list(collection.find().limit(sample_size))
        
        if not sample_docs:
            return {
                "name": collection_name,
                "columns": [],
                "row_count": collection.count_documents({})
            }
        
        # Infer fields from sample documents
        all_fields = set()
        field_types = {}
        field_nullable = {}
        
        for doc in sample_docs:
            for key, value in doc.items():
                all_fields.add(key)
                # Track types
                value_type = type(value).__name__
                if key not in field_types:
                    field_types[key] = set()
                field_types[key].add(value_type)
                # Track nullable
                if value is None:
                    field_nullable[key] = True
        
        # Build columns list
        columns = []
        for field in sorted(all_fields):
            types = field_types.get(field, set())
            type_str = ", ".join(sorted(types)) if types else "unknown"
            nullable = field_nullable.get(field, False)
            
            columns.append({
                "name": field,
                "type": type_str,
                "nullable": nullable
            })
        
        return {
            "name": collection_name,
            "columns": columns,
            "row_count": collection.count_documents({})
        }
    
    def close(self):
        """Close MongoDB connection"""
        if self.client is not None:
            self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def is_mongodb_url(url: str) -> bool:
    """Check if URL is a MongoDB connection string"""
    url_lower = url.lower().strip()
    return url_lower.startswith("mongodb://") or url_lower.startswith("mongodb+srv://")


def get_mongodb_schema(url: str) -> Dict[str, Any]:
    """Get MongoDB schema overview (collections as tables)"""
    with MongoDBConnection(url) as mongo:
        # First try to get collections from the specified database
        collections = mongo.get_collections()
        
        # If no collections in the specified database, try all databases
        if not collections:
            logger.info("No collections found in specified database, checking all databases...")
            all_db_collections = mongo.get_all_databases_with_collections()
            
            if all_db_collections:
                # Use the first database that has collections
                db_name = list(all_db_collections.keys())[0]
                logger.info(f"Found collections in database: {db_name}")
                mongo.db = mongo.client[db_name]
                mongo.db_name = db_name
                collections = all_db_collections[db_name]
            else:
                logger.warning("No collections found in any database")
        
        # Get the actual database name being used
        actual_db_name = mongo.db_name or (mongo.db.name if mongo.db else None)
        
        tables = []
        for coll_name in collections:
            try:
                schema = mongo.get_collection_schema(coll_name, sample_size=50)
                # Include database name in table name if we're using a different database
                if actual_db_name and actual_db_name != 'admin':
                    table_name = f"{actual_db_name}.{coll_name}"
                else:
                    table_name = coll_name
                tables.append({
                    "table": table_name,
                    "columns": schema["columns"]
                })
            except Exception as e:
                logger.error(f"Error getting schema for collection {coll_name}: {e}")
                # Still add the collection even if we can't get its schema
                table_name = f"{actual_db_name}.{coll_name}" if actual_db_name and actual_db_name != 'admin' else coll_name
                tables.append({
                    "table": table_name,
                    "columns": []
                })
        
        result = {
            "ok": True,
            "dialect": "mongodb",
            "tables": tables
        }
        
        # Include database name in response for frontend display
        if actual_db_name:
            result["database"] = actual_db_name
        
        return result


def crawl_mongodb_schema(url: str, sample_size: int = 100):
    """
    Crawl MongoDB schema and return SchemaMetadata compatible with SQL schema crawler.
    This allows MongoDB to work with the model training system.
    """
    from .schema.crawler import SchemaMetadata, TableInfo, ColumnInfo
    
    metadata = SchemaMetadata()
    
    with MongoDBConnection(url) as mongo:
        # Get collections from specified database
        collections = mongo.get_collections()
        
        # If no collections, try all databases
        if not collections:
            all_db_collections = mongo.get_all_databases_with_collections()
            if all_db_collections:
                db_name = list(all_db_collections.keys())[0]
                mongo.db = mongo.client[db_name]
                mongo.db_name = db_name
                collections = all_db_collections[db_name]
        
        for coll_name in collections:
            try:
                # Get collection schema
                coll_schema = mongo.get_collection_schema(coll_name, sample_size=sample_size)
                
                # Create TableInfo (treating collection as table)
                table_info = TableInfo(coll_name)
                table_info.row_count = coll_schema.get("row_count", 0)
                
                # Convert columns
                for col_data in coll_schema.get("columns", []):
                    col_info = ColumnInfo(
                        name=col_data["name"],
                        type_str=col_data["type"],
                        nullable=col_data.get("nullable", True)
                    )
                    # MongoDB doesn't have explicit PKs/FKs, but _id is typically the primary key
                    if col_data["name"] == "_id":
                        col_info.is_primary_key = True
                        table_info.primary_key = ["_id"]
                    table_info.columns.append(col_info)
                
                # Get sample rows
                collection = mongo.db[coll_name]
                sample_docs = list(collection.find().limit(sample_size))
                for doc in sample_docs:
                    # Convert ObjectId and other MongoDB types to JSON-serializable
                    sample_row = {}
                    for key, value in doc.items():
                        if hasattr(value, '__dict__'):
                            sample_row[key] = str(value)
                        else:
                            sample_row[key] = value
                    table_info.sample_rows.append(sample_row)
                
                metadata.tables[coll_name] = table_info
                
            except Exception as e:
                logger.error(f"Error crawling collection {coll_name}: {e}", exc_info=True)
                # Still add the collection with minimal info
                table_info = TableInfo(coll_name)
                metadata.tables[coll_name] = table_info
    
    # Extract synonyms (same as SQL version)
    metadata.synonyms = _extract_synonyms_mongodb(metadata)
    
    return metadata


def _extract_synonyms_mongodb(metadata):
    """Extract synonyms from MongoDB schema metadata"""
    from collections import defaultdict
    synonyms = defaultdict(list)
    
    # Similar logic to SQL version - extract from column names and sample data
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
        for col_info in table_info.columns:
            col_base = col_info.name.lower().replace("_", " ").replace("-", " ")
            if col_base not in synonyms[col_info.name]:
                synonyms[col_info.name].append(col_base)
            
            # Common patterns
            if col_info.name.endswith("_id"):
                base_name = col_info.name[:-3]
                synonyms[col_info.name].append(base_name)
    
    return dict(synonyms)


def test_mongodb_connection(url: str) -> Dict[str, Any]:
    """Test MongoDB connection"""
    try:
        with MongoDBConnection(url) as mongo:
            # Connection successful if we get here
            # Mask password in URL for response
            parsed = urlparse(url)
            if parsed.password:
                # Replace password with *****
                masked_url = url.replace(f":{parsed.password}@", ":*****@")
            else:
                masked_url = url
            return {
                "ok": True,
                "dialect": "mongodb",
                "url": masked_url
            }
    except Exception as e:
        raise ConnectionError(f"MongoDB connection failed: {e}")

