# app/schema/__init__.py
from .crawler import crawl_schema, SchemaMetadata

__all__ = ["crawl_schema", "SchemaMetadata"]

# MongoDB crawler is imported separately when needed

