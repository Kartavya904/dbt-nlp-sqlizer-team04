# app/ai/nl2sql.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple
from rapidfuzz import fuzz, process
import sqlglot as sg
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine, Connection
from .llm import chat_complete, LLMNotConfigured
from .query_intent import analyze_query_intent, build_enhanced_prompt
import re

# ---------- Schema utilities ----------

def load_schema(engine: Engine) -> Dict[str, List[Dict[str, Any]]]:
    insp = inspect(engine)
    out: Dict[str, List[Dict[str, Any]]] = {}
    for t in insp.get_table_names():
        cols = [{"name": c["name"], "type": str(c["type"]), "nullable": bool(c.get("nullable", True))}
                for c in insp.get_columns(t)]
        out[t] = cols
    return out

def select_relevant(schema: Dict[str, List[Dict[str, Any]]], question: str, k_tables: int = 4) -> Dict[str, List[str]]:
    """
    Fuzzy-score tables/columns against the question; return a pruned view: { table: [columns...] }.
    """
    tables = list(schema.keys())
    table_scores = [(t, max(
        fuzz.partial_ratio(t, question),
        max((fuzz.partial_ratio(c["name"], question) for c in schema[t]), default=0)
    )) for t in tables]
    table_scores.sort(key=lambda x: x[1], reverse=True)
    chosen = [t for t, _ in table_scores[:k_tables]]

    out: Dict[str, List[str]] = {}
    for t in chosen:
        cols = [c["name"] for c in schema[t]]
        # keep top columns plus primary key-ish names
        col_scores = [(c, fuzz.partial_ratio(c, question)) for c in cols]
        col_scores.sort(key=lambda x: x[1], reverse=True)
        best = [c for c, _ in col_scores[:8]]
        for fallback in ("id", f"{t}_id"):
            if fallback in cols and fallback not in best:
                best.append(fallback)
        out[t] = best
    return out

# ---------- Prompting ----------

def _detect_aggregation_needed(question: str) -> bool:
    """Detect if the question requires aggregations (GROUP BY, AVG, COUNT, SUM, etc.)"""
    question_lower = question.lower()
    aggregation_keywords = [
        "average", "avg", "mean",
        "count", "how many", "number of",
        "sum", "total",
        "max", "maximum", "min", "minimum",
        "grouped by", "group by", "per", "for each",
        "aggregate", "aggregation",
        "along with the average", "along with the", "with the average",
        "compared to", "compared with", "same as the average"
    ]
    return any(keyword in question_lower for keyword in aggregation_keywords)

SYS = """Generate ONLY SQL SELECT queries. NO explanations, NO markdown, NO backticks. Start with SELECT.

Rules:
- SELECT only (no INSERT/UPDATE/DELETE/DDL)
- Use explicit JOIN ... ON for joins
- Aggregations: Use AVG/COUNT/SUM with GROUP BY when asked. For "along with" use window functions: AVG() OVER (PARTITION BY ...)
- WHERE for filters, ORDER BY for sorting, LIMIT 100 (or specified number)
- DISTINCT for unique values
- Use only provided tables/columns
- PostgreSQL syntax"""

def render_context(slice_: Dict[str, List[str]]) -> str:
    lines = []
    for t, cols in slice_.items():
        col_list = ", ".join(cols)
        lines.append(f"- {t}({col_list})")
    return "\n".join(lines)

def _extract_sql_from_response(response: str) -> str:
    """
    Extract SQL query from LLM response, handling cases where the model
    includes explanations or markdown formatting.
    """
    response = response.strip()
    
    # Remove markdown code blocks if present
    if response.startswith("```"):
        # Find the closing ```
        lines = response.split("\n")
        # Remove first line (```sql or ```)
        if len(lines) > 1:
            lines = lines[1:]
        # Remove last line if it's ```
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        response = "\n".join(lines)
    
    # Try to find SQL statement (starts with SELECT)
    lines = response.split("\n")
    sql_lines = []
    in_sql = False
    
    for line in lines:
        line_stripped = line.strip()
        # Start collecting when we see SELECT
        if line_stripped.upper().startswith("SELECT"):
            in_sql = True
            sql_lines.append(line_stripped)
        elif in_sql:
            # Stop if we hit a line that looks like prose (starts with capital letter, no SQL keywords)
            if line_stripped and not any(
                keyword in line_stripped.upper() 
                for keyword in ["SELECT", "FROM", "WHERE", "JOIN", "GROUP", "ORDER", "LIMIT", "HAVING", "UNION", "WITH", "AS", "ON", "AND", "OR", "IN", "EXISTS", "CASE", "WHEN", "THEN", "ELSE", "END", "(", ")", ",", "=", "<", ">", "!=", "<=", ">=", "IS", "NULL", "NOT", "LIKE", "ILIKE", "AVG", "COUNT", "SUM", "MAX", "MIN", "OVER", "PARTITION", "BY", "DISTINCT"]
            ) and line_stripped[0].isupper() and not line_stripped.startswith("--"):
                # Might be prose, but check if it's part of a string literal
                if '"' not in line_stripped and "'" not in line_stripped:
                    break
            sql_lines.append(line_stripped)
            # Stop at semicolon if present
            if line_stripped.endswith(";"):
                break
    
    if sql_lines:
        sql = " ".join(sql_lines).strip()
        # Remove trailing semicolon if present (we'll add it back if needed)
        sql = sql.rstrip(";").strip()
        return sql
    
    # Fallback: return the whole response if no SQL found
    return response.strip()


def ask_llm(question: str, slice_: Dict[str, List[str]], use_intent_analysis: bool = True) -> str:
    """
    Ask LLM to generate SQL query.
    
    Args:
        question: Natural language question
        slice_: Schema context {table: [columns]}
        use_intent_analysis: If True, use comprehensive intent analysis for better prompts
    """
    if use_intent_analysis:
        # Use comprehensive intent analysis
        intent_analysis = analyze_query_intent(question, slice_)
        user = build_enhanced_prompt(question, slice_, intent_analysis)
    else:
        # Fallback to original simple prompt
        ctx = render_context(slice_)
        
        # Detect if aggregation is needed and emphasize it
        needs_aggregation = _detect_aggregation_needed(question)
        aggregation_reminder = "\n⚠️ Use AVG/COUNT/SUM with GROUP BY or window functions." if needs_aggregation else ""
        
        user = f"""Q: {question}{aggregation_reminder}
Schema: {ctx}
Generate SQL only, start with SELECT."""
    
    response = chat_complete(SYS, user)
    # Extract SQL from response (handles markdown, explanations, etc.)
    sql = _extract_sql_from_response(response)
    return sql

# ---------- Validation & safety ----------

class SQLSafetyError(Exception):
    pass

BLOCK_KINDS = {"Insert", "Update", "Delete", "Create", "Drop", "Alter", "Truncate", "Merge"}

def _validate_aggregation_requirements(question: str, sql: str) -> None:
    """Validate that SQL includes required aggregations if the question asks for them"""
    if not _detect_aggregation_needed(question):
        return  # No aggregation needed
    
    sql_lower = sql.lower()
    
    # Check for aggregation keywords in question
    question_lower = question.lower()
    needs_avg = any(kw in question_lower for kw in ["average", "avg", "mean", "along with the average"])
    needs_count = any(kw in question_lower for kw in ["count", "how many", "number of"])
    needs_sum = any(kw in question_lower for kw in ["sum", "total"])
    needs_group_by = any(kw in question_lower for kw in ["grouped by", "group by", "per", "for each"])
    
    # Check if SQL has the required aggregations
    has_avg = "avg(" in sql_lower or "average(" in sql_lower
    has_count = "count(" in sql_lower
    has_sum = "sum(" in sql_lower
    has_group_by = "group by" in sql_lower
    has_window = "over (" in sql_lower or "partition by" in sql_lower
    
    # If question asks for aggregation but SQL doesn't have it, warn
    if needs_avg and not has_avg:
        raise SQLSafetyError(
            f"Question asks for average/mean but SQL doesn't include AVG(). "
            f"Use AVG() with GROUP BY or window functions (AVG() OVER (PARTITION BY ...))."
        )
    if needs_count and not has_count:
        raise SQLSafetyError(
            f"Question asks for count but SQL doesn't include COUNT(). "
            f"Use COUNT() with GROUP BY if needed."
        )
    if needs_sum and not has_sum:
        raise SQLSafetyError(
            f"Question asks for sum/total but SQL doesn't include SUM(). "
            f"Use SUM() with GROUP BY if needed."
        )
    if needs_group_by and not (has_group_by or has_window):
        raise SQLSafetyError(
            f"Question asks for grouping but SQL doesn't include GROUP BY or window functions. "
            f"Use GROUP BY or window functions (OVER (PARTITION BY ...)) to group data."
        )


def _validate_query_structure(question: str, sql: str, schema_context: Optional[Dict[str, List[str]]] = None) -> None:
    """
    Comprehensive validation of query structure based on question intent.
    Validates JOINs, WHERE clauses, ORDER BY, etc.
    """
    question_lower = question.lower()
    sql_lower = sql.lower()
    
    # Validate JOIN requirements
    if schema_context and len(schema_context) >= 2:
        # Multiple tables in context - check if question implies JOIN
        join_indicators = ["and their", "with their", "together", "and", "join"]
        if any(indicator in question_lower for indicator in join_indicators):
            # Check if SQL has JOIN
            has_join = "join" in sql_lower
            # Count table references in SQL
            table_refs = sum(1 for table in schema_context.keys() if f'"{table}"' in sql_lower or f" {table} " in sql_lower)
            if table_refs >= 2 and not has_join:
                # Might be using implicit join (comma-separated) - that's okay but warn
                if "," not in sql_lower.split("from")[1].split("where")[0] if "from" in sql_lower else "":
                    raise SQLSafetyError(
                        f"Question mentions multiple tables but SQL doesn't use JOIN. "
                        f"Use explicit JOIN syntax: SELECT ... FROM table1 JOIN table2 ON ..."
                    )
    
    # Validate WHERE clause for filter keywords
    filter_keywords = ["where", "with", "that have", "that are", "greater than", "less than", "above", "below"]
    has_filter_keywords = any(kw in question_lower for kw in filter_keywords)
    has_where = "where" in sql_lower
    
    if has_filter_keywords and not has_where:
        # Not always an error - might be in JOIN condition
        if "on" not in sql_lower or not any(kw in question_lower for kw in ["join", "and their", "with their"]):
            # Likely missing WHERE clause
            pass  # Don't raise error, just note it
    
    # Validate ORDER BY for sort keywords
    sort_keywords = ["sorted by", "ordered by", "top", "first", "last", "newest", "oldest", "highest", "lowest"]
    has_sort_keywords = any(kw in question_lower for kw in sort_keywords)
    has_order_by = "order by" in sql_lower
    
    if has_sort_keywords and not has_order_by:
        # Check if it's a "top N" query that might use LIMIT only
        if not re.search(r'\b(top|first|last)\s+\d+', question_lower):
            # Missing ORDER BY
            pass  # Don't raise error, LIMIT might be sufficient for some cases
    
    # Validate DISTINCT
    distinct_keywords = ["unique", "distinct", "no duplicates", "different values"]
    has_distinct_keywords = any(kw in question_lower for kw in distinct_keywords)
    has_distinct = "distinct" in sql_lower
    
    if has_distinct_keywords and not has_distinct:
        raise SQLSafetyError(
            f"Question asks for unique/distinct values but SQL doesn't include DISTINCT. "
            f"Add DISTINCT to SELECT clause."
        )

def ensure_select_only(sql: str) -> sg.Expression:
    try:
        parsed = sg.parse_one(sql, read="postgres")
    except Exception as e:
        raise SQLSafetyError(f"SQL parse error: {e}")
    if parsed.__class__.__name__ in BLOCK_KINDS:
        raise SQLSafetyError("Only SELECT statements are allowed.")
    # Unions etc. are okay if they are SELECT-derived
    if not any(isinstance(parsed, k) for k in (sg.exp.Select, sg.exp.Subquery, sg.exp.Union, sg.exp.With)):
        raise SQLSafetyError("Statement must be a SELECT.")
    return parsed

def referenced_tables(expr: sg.Expression) -> List[str]:
    names = []
    for t in expr.find_all(sg.exp.Table):
        names.append(t.this and t.this.name or "")
    # dedupe
    return sorted(set(n for n in names if n))

def enforce_limit(expr: sg.Expression, max_rows: int = 100) -> sg.Expression:
    # If top-level is a SELECT without limit, add one
    target = expr
    if isinstance(target, sg.exp.Subquery):
        target = target.this
    if isinstance(target, sg.exp.Select) and not target.args.get("limit"):
        target.set("limit", sg.exp.Limit(this=sg.exp.Literal.number(max_rows)))
    return expr

def ensure_tables_allowed(expr: sg.Expression, allowed: Dict[str, List[str]]):
    used = referenced_tables(expr)
    allowed_tables = set(allowed.keys())
    for t in used:
        if t not in allowed_tables:
            raise SQLSafetyError(f"Table not allowed in context: {t}")

def finalize_sql(expr: sg.Expression) -> str:
    return expr.sql(dialect="postgres")

# ---------- Execution ----------

# app/ai/nl2sql.py (or wherever execute_readonly lives)
from decimal import Decimal
from datetime import date, datetime, time
from sqlalchemy.engine import Connection

def _jsonable(v):
    if isinstance(v, Decimal):
        return float(v)  # or str(v) if you need exactness
    if isinstance(v, (date, datetime, time)):
        return v.isoformat()
    return v

def execute_readonly(conn: Connection, sql: str, timeout_ms: int = 5000):
    # Keep queries safe/fast
    try:
        # Postgres: short statement timeout
        conn.exec_driver_sql(f"SET LOCAL statement_timeout = {timeout_ms}")
    except Exception:
        pass

    res = conn.exec_driver_sql(sql)

    # Columns
    cols = []
    if getattr(res, "cursor", None) and res.cursor.description:
        cols = [c[0] for c in res.cursor.description]

    # Rows → plain lists of JSON-safe values
    rows = []
    if cols:
        for row in res.fetchall():            # row is a tuple/Row object
            rows.append([_jsonable(v) for v in row])

    return cols, rows


def explain(conn: Connection, sql: str) -> str:
    try:
        txt = conn.exec_driver_sql(f"EXPLAIN {sql}").fetchall()
        # EXPLAIN (FORMAT TEXT) returns rows of text in PG
        return "\n".join(r[0] for r in txt)
    except Exception:
        return ""
