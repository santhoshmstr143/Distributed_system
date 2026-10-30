"""
Query engine module for Sharded-Docstore — Module 5.
"""

from __future__ import annotations

from docstore.query.evaluator import evaluate_filter, get_dotted_field, type_safe_equal
from docstore.query.shard_key import (
    ShardKeyConstraint,
    detect_shard_key_constraint,
)
from docstore.query.sorter import extract_sort_key, sort_documents
from docstore.query.validator import validate_query_filter

__all__ = [
    "ShardKeyConstraint",
    "detect_shard_key_constraint",
    "evaluate_filter",
    "extract_sort_key",
    "get_dotted_field",
    "sort_documents",
    "type_safe_equal",
    "validate_query_filter",
]
