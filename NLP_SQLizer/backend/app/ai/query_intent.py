# app/ai/query_intent.py
"""
Comprehensive query intent detection and analysis.
Detects what type of query the user wants and provides guidance for generation.
"""
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import re


class QueryIntent(Enum):
    """Types of query intents"""
    SIMPLE_SELECT = "simple_select"
    FILTERED = "filtered"
    AGGREGATION = "aggregation"
    GROUPED_AGGREGATION = "grouped_aggregation"
    WINDOW_FUNCTION = "window_function"
    JOIN = "join"
    SORTED = "sorted"
    LIMITED = "limited"
    COMPLEX_FILTER = "complex_filter"
    DATE_TIME = "date_time"
    TEXT_SEARCH = "text_search"
    COMPARISON = "comparison"
    RANKING = "ranking"
    DISTINCT = "distinct"
    NULL_HANDLING = "null_handling"
    CONDITIONAL = "conditional"
    UNION = "union"
    SUBQUERY = "subquery"


@dataclass
class QueryIntentAnalysis:
    """Analysis of query intent"""
    intent: QueryIntent
    confidence: float
    required_clauses: List[str]  # e.g., ["WHERE", "GROUP BY", "ORDER BY"]
    required_functions: List[str]  # e.g., ["AVG", "COUNT", "JOIN"]
    hints: List[str]  # Guidance for query generation
    example_sql_pattern: str  # Example SQL pattern


def analyze_query_intent(question: str, schema_context: Optional[Dict[str, List[str]]] = None) -> QueryIntentAnalysis:
    """
    Analyze the user's question to determine query intent.
    
    Returns QueryIntentAnalysis with detected intent, required clauses, and hints.
    """
    question_lower = question.lower()
    
    # Detect multiple intents (can overlap)
    intents = []
    required_clauses = []
    required_functions = []
    hints = []
    
    # 1. AGGREGATION detection
    aggregation_keywords = {
        "avg": ["average", "avg", "mean", "mean age", "mean price"],
        "count": ["count", "how many", "number of", "total number", "quantity"],
        "sum": ["sum", "total", "total amount", "total price", "total sales"],
        "max": ["max", "maximum", "highest", "largest", "most", "top"],
        "min": ["min", "minimum", "lowest", "smallest", "least", "bottom"]
    }
    
    detected_agg = None
    for agg_func, keywords in aggregation_keywords.items():
        if any(kw in question_lower for kw in keywords):
            detected_agg = agg_func.upper()
            required_functions.append(detected_agg)
            intents.append(QueryIntent.AGGREGATION)
            break
    
    # 2. GROUP BY detection
    group_keywords = [
        "grouped by", "group by", "per", "for each", "by company", "by category",
        "by month", "by year", "by department", "by type", "by status"
    ]
    needs_group_by = any(kw in question_lower for kw in group_keywords)
    
    if needs_group_by and detected_agg:
        intents.append(QueryIntent.GROUPED_AGGREGATION)
        required_clauses.append("GROUP BY")
        hints.append("Use GROUP BY with the aggregation function")
    elif needs_group_by:
        intents.append(QueryIntent.GROUPED_AGGREGATION)
        required_clauses.append("GROUP BY")
        hints.append("Question asks for grouping but no aggregation specified - may need COUNT(*) or similar")
    
    # 3. WINDOW FUNCTION detection (individual rows + aggregates)
    window_keywords = [
        "along with", "with their", "with the average", "with the total",
        "compared to", "compared with", "same as the average",
        "alongside", "including the", "plus the average"
    ]
    needs_window = any(kw in question_lower for kw in window_keywords)
    
    if needs_window and detected_agg:
        intents.append(QueryIntent.WINDOW_FUNCTION)
        required_functions.append("OVER (PARTITION BY ...)")
        hints.append("Use window functions (AVG() OVER (PARTITION BY ...)) to show individual rows with aggregated values")
    
    # 4. JOIN detection
    join_keywords = [
        "and their", "with their", "and", "join", "together",
        "users and orders", "products and categories", "employees and departments"
    ]
    # More sophisticated: detect multiple table mentions
    if schema_context:
        table_mentions = sum(1 for table in schema_context.keys() if table.lower() in question_lower)
        if table_mentions >= 2:
            intents.append(QueryIntent.JOIN)
            required_clauses.append("JOIN")
            hints.append("Multiple tables mentioned - use JOIN to combine data")
    elif any(kw in question_lower for kw in ["and their", "with their", "together"]):
        intents.append(QueryIntent.JOIN)
        required_clauses.append("JOIN")
        hints.append("Question implies joining related data")
    
    # 5. FILTER detection (WHERE)
    filter_keywords = [
        "where", "with", "that have", "that are", "which", "whose",
        "greater than", "less than", "equal to", "not equal",
        "above", "below", "over", "under", "between", "in range"
    ]
    comparison_ops = [">", "<", ">=", "<=", "=", "!=", "between", "in", "like"]
    
    has_filters = any(kw in question_lower for kw in filter_keywords) or \
                  any(re.search(rf'\b{op}\b', question_lower) for op in ["greater", "less", "equal", "not"])
    
    if has_filters:
        intents.append(QueryIntent.FILTERED)
        required_clauses.append("WHERE")
        hints.append("Question contains filtering conditions - use WHERE clause")
    
    # 6. COMPLEX FILTER detection
    complex_filter_indicators = [
        "and", "or", "both", "either", "neither", "not only", "but also",
        "as well as", "in addition to"
    ]
    if has_filters and sum(1 for kw in complex_filter_indicators if kw in question_lower) >= 2:
        intents.append(QueryIntent.COMPLEX_FILTER)
        hints.append("Multiple filter conditions - use AND/OR in WHERE clause")
    
    # 7. SORTING detection (ORDER BY)
    sort_keywords = [
        "sorted by", "ordered by", "order by", "sort by",
        "ascending", "descending", "asc", "desc",
        "newest", "oldest", "latest", "earliest", "first", "last",
        "top", "bottom", "highest", "lowest"
    ]
    needs_sort = any(kw in question_lower for kw in sort_keywords)
    
    if needs_sort:
        intents.append(QueryIntent.SORTED)
        required_clauses.append("ORDER BY")
        # Detect sort direction
        if any(kw in question_lower for kw in ["descending", "desc", "newest", "latest", "highest", "top"]):
            hints.append("Use ORDER BY ... DESC for descending order")
        else:
            hints.append("Use ORDER BY for sorting")
    
    # 8. LIMIT/TOP N detection
    limit_keywords = [
        "first", "last", "top", "bottom", "limit", "only",
        "first 10", "top 5", "last 20", "first few", "only show"
    ]
    needs_limit = any(kw in question_lower for kw in limit_keywords) or \
                  re.search(r'\b(top|first|last)\s+\d+', question_lower)
    
    if needs_limit:
        intents.append(QueryIntent.LIMITED)
        hints.append("Question specifies a limit - use LIMIT clause")
        # Extract number if present
        limit_match = re.search(r'\b(top|first|last)\s+(\d+)', question_lower)
        if limit_match:
            hints.append(f"Limit to {limit_match.group(2)} rows")
    
    # 9. DATE/TIME detection
    date_keywords = [
        "today", "yesterday", "tomorrow", "this week", "this month", "this year",
        "last week", "last month", "last year", "next week", "next month",
        "recent", "recently", "latest", "oldest", "date", "time", "when",
        "from", "to", "between", "after", "before", "since", "until"
    ]
    needs_date = any(kw in question_lower for kw in date_keywords) or \
                 re.search(r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}', question_lower)
    
    if needs_date:
        intents.append(QueryIntent.DATE_TIME)
        required_clauses.append("WHERE")
        hints.append("Question involves dates/times - use date functions and comparisons in WHERE")
    
    # 10. TEXT SEARCH detection
    text_search_keywords = [
        "containing", "contains", "like", "matching", "starts with", "ends with",
        "includes", "including", "search", "find", "look for"
    ]
    needs_text_search = any(kw in question_lower for kw in text_search_keywords)
    
    if needs_text_search:
        intents.append(QueryIntent.TEXT_SEARCH)
        required_functions.append("LIKE or ILIKE")
        hints.append("Text search needed - use LIKE or ILIKE with wildcards (%)")
    
    # 11. COMPARISON detection
    comparison_keywords = [
        "compare", "comparison", "versus", "vs", "difference", "different",
        "same", "similar", "equal", "greater than", "less than"
    ]
    needs_comparison = any(kw in question_lower for kw in comparison_keywords)
    
    if needs_comparison:
        intents.append(QueryIntent.COMPARISON)
        hints.append("Comparison query - may need subqueries or self-joins")
    
    # 12. RANKING detection
    ranking_keywords = [
        "rank", "ranking", "ranked", "position", "nth", "first place",
        "second place", "top performer", "best", "worst"
    ]
    needs_ranking = any(kw in question_lower for kw in ranking_keywords)
    
    if needs_ranking:
        intents.append(QueryIntent.RANKING)
        required_functions.append("ROW_NUMBER() or RANK()")
        hints.append("Ranking needed - use ROW_NUMBER() or RANK() window functions")
    
    # 13. DISTINCT detection
    distinct_keywords = [
        "unique", "distinct", "different", "unique values", "no duplicates",
        "only show unique", "list all unique"
    ]
    needs_distinct = any(kw in question_lower for kw in distinct_keywords)
    
    if needs_distinct:
        intents.append(QueryIntent.DISTINCT)
        required_functions.append("DISTINCT")
        hints.append("Use DISTINCT to remove duplicates")
    
    # 14. NULL handling
    null_keywords = [
        "null", "empty", "missing", "not set", "no value", "blank",
        "is null", "is not null", "has no", "without"
    ]
    needs_null_check = any(kw in question_lower for kw in null_keywords)
    
    if needs_null_check:
        intents.append(QueryIntent.NULL_HANDLING)
        required_clauses.append("WHERE")
        hints.append("Check for NULL values using IS NULL or IS NOT NULL")
    
    # 15. UNION detection
    union_keywords = [
        "or", "either", "both", "combine", "union", "together",
        "all from", "all records from"
    ]
    # This is tricky - "or" could be in WHERE. Look for multiple table mentions with "or"
    if "or" in question_lower and schema_context:
        # Check if "or" appears between table names
        table_names = list(schema_context.keys())
        for i, table1 in enumerate(table_names):
            for table2 in table_names[i+1:]:
                pattern = rf'\b{re.escape(table1)}\b.*\bor\b.*\b{re.escape(table2)}\b'
                if re.search(pattern, question_lower, re.IGNORECASE):
                    intents.append(QueryIntent.UNION)
                    required_functions.append("UNION")
                    hints.append("Multiple tables with OR - may need UNION")
                    break
    
    # 16. SUBQUERY detection
    subquery_indicators = [
        "that have", "which have", "whose", "where there exists",
        "that are in", "that are not in", "in the list of"
    ]
    needs_subquery = any(kw in question_lower for kw in subquery_indicators) and has_filters
    
    if needs_subquery:
        intents.append(QueryIntent.SUBQUERY)
        hints.append("Complex condition - may need subquery in WHERE clause")
    
    # Determine primary intent
    if not intents:
        primary_intent = QueryIntent.SIMPLE_SELECT
        confidence = 0.8
    else:
        # Prioritize more specific intents
        priority_order = [
            QueryIntent.WINDOW_FUNCTION,
            QueryIntent.GROUPED_AGGREGATION,
            QueryIntent.AGGREGATION,
            QueryIntent.JOIN,
            QueryIntent.RANKING,
            QueryIntent.COMPLEX_FILTER,
            QueryIntent.FILTERED,
            QueryIntent.SORTED,
            QueryIntent.LIMITED,
        ]
        
        primary_intent = intents[0]
        for priority in priority_order:
            if priority in intents:
                primary_intent = priority
                break
        
        confidence = min(0.95, 0.7 + (len(intents) * 0.05))
    
    # Generate example SQL pattern
    example_pattern = _generate_example_pattern(primary_intent, required_functions, required_clauses)
    
    return QueryIntentAnalysis(
        intent=primary_intent,
        confidence=confidence,
        required_clauses=required_clauses,
        required_functions=required_functions,
        hints=hints,
        example_sql_pattern=example_pattern
    )


def _generate_example_pattern(intent: QueryIntent, functions: List[str], clauses: List[str]) -> str:
    """Generate an example SQL pattern based on intent"""
    patterns = {
        QueryIntent.SIMPLE_SELECT: "SELECT * FROM table_name LIMIT 100",
        QueryIntent.FILTERED: "SELECT * FROM table_name WHERE condition LIMIT 100",
        QueryIntent.AGGREGATION: "SELECT {func}(column) FROM table_name LIMIT 100",
        QueryIntent.GROUPED_AGGREGATION: "SELECT group_column, {func}(column) FROM table_name GROUP BY group_column LIMIT 100",
        QueryIntent.WINDOW_FUNCTION: "SELECT column1, column2, {func}(column2) OVER (PARTITION BY group_column) FROM table_name LIMIT 100",
        QueryIntent.JOIN: "SELECT * FROM table1 JOIN table2 ON table1.id = table2.foreign_id LIMIT 100",
        QueryIntent.SORTED: "SELECT * FROM table_name ORDER BY column DESC LIMIT 100",
        QueryIntent.LIMITED: "SELECT * FROM table_name LIMIT 10",
        QueryIntent.COMPLEX_FILTER: "SELECT * FROM table_name WHERE condition1 AND condition2 LIMIT 100",
        QueryIntent.DATE_TIME: "SELECT * FROM table_name WHERE date_column >= '2024-01-01' LIMIT 100",
        QueryIntent.TEXT_SEARCH: "SELECT * FROM table_name WHERE column LIKE '%pattern%' LIMIT 100",
        QueryIntent.COMPARISON: "SELECT * FROM table1 WHERE column > (SELECT AVG(column) FROM table1) LIMIT 100",
        QueryIntent.RANKING: "SELECT *, ROW_NUMBER() OVER (ORDER BY column DESC) as rank FROM table_name LIMIT 100",
        QueryIntent.DISTINCT: "SELECT DISTINCT column FROM table_name LIMIT 100",
        QueryIntent.NULL_HANDLING: "SELECT * FROM table_name WHERE column IS NOT NULL LIMIT 100",
        QueryIntent.UNION: "SELECT * FROM table1 UNION SELECT * FROM table2 LIMIT 100",
        QueryIntent.SUBQUERY: "SELECT * FROM table_name WHERE id IN (SELECT id FROM other_table WHERE condition) LIMIT 100",
    }
    
    pattern = patterns.get(intent, "SELECT * FROM table_name LIMIT 100")
    
    # Replace function placeholders
    if functions and "{func}" in pattern:
        func = functions[0] if functions else "AVG"
        pattern = pattern.replace("{func}", func)
    
    return pattern


def build_enhanced_prompt(question: str, schema_context: Dict[str, List[str]], intent_analysis: QueryIntentAnalysis) -> str:
    """Build an enhanced prompt with intent-specific guidance"""
    
    # Build schema context with emphasis on exact column names
    schema_lines = ["EXACT column names (use these exactly):"]
    for t, cols in schema_context.items():
        schema_lines.append(f"  {t}: {', '.join(cols)}")
    context_str = "\n".join(schema_lines)
    
    # Concise intent guidance for faster processing
    clauses_str = ', '.join(intent_analysis.required_clauses) if intent_analysis.required_clauses else 'None'
    funcs_str = ', '.join(intent_analysis.required_functions) if intent_analysis.required_functions else 'None'
    hints_str = '; '.join(intent_analysis.hints[:3]) if intent_analysis.hints else 'Standard SELECT'
    
    intent_guidance = f"""Intent: {intent_analysis.intent.value}. Required: {clauses_str}. Functions: {funcs_str}. {hints_str}. Example: {intent_analysis.example_sql_pattern}"""
    
    prompt = f"""Q: {question}
{intent_guidance}
Schema:
{context_str}
⚠️ CRITICAL: Use exact column names above (e.g., "fcity" NOT "fromCity", "fprice" NOT "price").
Generate SQL only, start with SELECT."""
    
    return prompt

