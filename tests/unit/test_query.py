"""
Unit tests for Query Engine — Module 5.1.

Covers:
- Grammar validation & whitelist operators ($eq, $gt, $lt, $in, $and, $or)
- Limits: max depth (5), max clauses (50), max $in size (1000)
- Unknown $ operators and malformed AST rejection
- Evaluator: pure Python, no eval/regex
- Dotted-path navigation
- Type safety: booleans are not numbers
- Missing-field and null rules: missing never matches, null only matches explicit null
- Sorter: total order across types (missing < null < bool < number < string), doc_id tie-breaker
- Shard-key constraint detector: exact, in, range, unconstrained
"""

from __future__ import annotations

import pytest

from docstore.common.errors import InvalidQuery
from docstore.query import (
    detect_shard_key_constraint,
    evaluate_filter,
    get_dotted_field,
    sort_documents,
    type_safe_equal,
    validate_query_filter,
)

pytestmark = pytest.mark.unit


# ═══════════════════════════════════════════════════════════
# 1. Validator Tests
# ═══════════════════════════════════════════════════════════


class TestQueryValidator:
    def test_empty_filter_valid(self) -> None:
        validate_query_filter({})

    def test_valid_basic_filter(self) -> None:
        validate_query_filter({"age": {"$gt": 18, "$lt": 65}, "status": "active"})

    def test_valid_in_filter(self) -> None:
        validate_query_filter({"tags": {"$in": ["a", "b", "c"]}})

    def test_valid_logical_and_or(self) -> None:
        validate_query_filter(
            {
                "$or": [
                    {"status": "pending"},
                    {"$and": [{"score": {"$gt": 80}}, {"verified": True}]},
                ]
            }
        )

    def test_unknown_operator_rejected(self) -> None:
        with pytest.raises(InvalidQuery, match="Unknown or misplaced operator"):
            validate_query_filter({"$where": "true"})

        with pytest.raises(InvalidQuery, match="Unknown operator"):
            validate_query_filter({"age": {"$regex": ".*"}})

    def test_depth_cap_exceeded(self) -> None:
        # Nest 6 levels deep (> 5)
        deep_filter = {"$and": [{"$or": [{"$and": [{"$or": [{"$and": [{"a": 1}]}]}]}]}]}
        with pytest.raises(InvalidQuery, match=r"depth.*exceeds maximum"):
            validate_query_filter(deep_filter)

    def test_clause_cap_exceeded(self) -> None:
        # 51 clauses (> 50)
        clauses = {f"field_{i}": i for i in range(51)}
        with pytest.raises(InvalidQuery, match=r"Clause count.*exceeds maximum"):
            validate_query_filter(clauses)

    def test_in_size_cap_exceeded(self) -> None:
        large_in = {"x": {"$in": list(range(1001))}}
        with pytest.raises(InvalidQuery, match=r"array size.*exceeds max"):
            validate_query_filter(large_in)

    def test_in_not_a_list_rejected(self) -> None:
        with pytest.raises(InvalidQuery, match="requires a list"):
            validate_query_filter({"x": {"$in": "not-a-list"}})

    def test_logical_not_a_list_rejected(self) -> None:
        with pytest.raises(InvalidQuery, match="requires a non-empty list"):
            validate_query_filter({"$and": "not-a-list"})

        with pytest.raises(InvalidQuery, match="requires a non-empty list"):
            validate_query_filter({"$or": []})


# ═══════════════════════════════════════════════════════════
# 2. Evaluator & Type Safety Tests
# ═══════════════════════════════════════════════════════════


class TestQueryEvaluator:
    def test_dotted_path_access(self) -> None:
        doc = {
            "doc_id": 1,
            "user": {
                "profile": {
                    "age": 30,
                    "nickname": None,
                }
            },
        }
        found, val = get_dotted_field(doc, "user.profile.age")
        assert found is True
        assert val == 30

        found, val = get_dotted_field(doc, "user.profile.nickname")
        assert found is True
        assert val is None

        found, _ = get_dotted_field(doc, "user.profile.missing")
        assert found is False

        found, _ = get_dotted_field(doc, "user.non_dict.field")
        assert found is False

    def test_type_safe_equal_bool_vs_number(self) -> None:
        # In Python: True == 1 is True. Here it MUST be False!
        assert type_safe_equal(True, 1) is False
        assert type_safe_equal(1, True) is False
        assert type_safe_equal(False, 0) is False
        assert type_safe_equal(0, False) is False

        assert type_safe_equal(True, True) is True
        assert type_safe_equal(1, 1.0) is True

    def test_missing_field_never_matches(self) -> None:
        doc = {"doc_id": 1}
        # $eq
        assert evaluate_filter(doc, {"missing": 5}) is False
        # $eq: None should NOT match missing field!
        assert evaluate_filter(doc, {"missing": None}) is False
        assert evaluate_filter(doc, {"missing": {"$eq": None}}) is False
        # $gt / $lt
        assert evaluate_filter(doc, {"missing": {"$gt": 0}}) is False
        assert evaluate_filter(doc, {"missing": {"$lt": 100}}) is False
        # $in
        assert evaluate_filter(doc, {"missing": {"$in": [None, 1, 2]}}) is False

    def test_explicit_null_matches_only_null(self) -> None:
        doc_null = {"doc_id": 1, "field": None}
        assert evaluate_filter(doc_null, {"field": None}) is True
        assert evaluate_filter(doc_null, {"field": {"$eq": None}}) is True
        assert evaluate_filter(doc_null, {"field": {"$in": [1, None, 2]}}) is True

        assert evaluate_filter(doc_null, {"field": 0}) is False
        assert evaluate_filter(doc_null, {"field": {"$gt": 0}}) is False
        assert evaluate_filter(doc_null, {"field": {"$lt": 0}}) is False

    def test_gt_lt_scalar_type_safety(self) -> None:
        doc = {"doc_id": 1, "num": 50, "str": "hello", "flag": True}

        # Numeric compare
        assert evaluate_filter(doc, {"num": {"$gt": 40, "$lt": 60}}) is True
        assert evaluate_filter(doc, {"num": {"$gt": 50}}) is False

        # String compare
        assert evaluate_filter(doc, {"str": {"$gt": "abc", "$lt": "world"}}) is True
        assert evaluate_filter(doc, {"str": {"$gt": "hello"}}) is False

        # Mixed types: string vs number -> no match
        assert evaluate_filter(doc, {"num": {"$gt": "10"}}) is False
        assert evaluate_filter(doc, {"str": {"$lt": 100}}) is False

        # Boolean vs number -> no match
        assert evaluate_filter(doc, {"flag": {"$gt": 0}}) is False
        assert evaluate_filter(doc, {"flag": {"$lt": 2}}) is False

    def test_nested_and_or(self) -> None:
        doc = {"doc_id": 1, "tier": "gold", "score": 95, "active": True}

        f1 = {
            "$and": [
                {"tier": "gold"},
                {"$or": [{"score": {"$lt": 50}}, {"active": True}]},
            ]
        }
        assert evaluate_filter(doc, f1) is True

        f2 = {
            "$or": [
                {"tier": "silver"},
                {"score": {"$lt": 90}},
            ]
        }
        assert evaluate_filter(doc, f2) is False


# ═══════════════════════════════════════════════════════════
# 3. Sorter & Total Order Tests
# ═══════════════════════════════════════════════════════════


class TestQuerySorter:
    def test_total_order_across_types(self) -> None:
        # Contract rank: missing < null < bool < number < string
        docs = [
            {"doc_id": 1, "val": "apple"},
            {"doc_id": 2, "val": 100},
            {"doc_id": 3, "val": None},
            {"doc_id": 4, "val": True},
            {"doc_id": 5, "val": False},
            {"doc_id": 6},  # missing val
            {"doc_id": 7, "val": -10},
            {"doc_id": 8, "val": "zebra"},
        ]

        sorted_asc = sort_documents(docs, sort_field="val", sort_dir="asc")
        # Expected:
        # missing (6)
        # null (3)
        # bool: False (5) < True (4)
        # number: -10 (7) < 100 (2)
        # string: "apple" (1) < "zebra" (8)
        assert [d["doc_id"] for d in sorted_asc] == [6, 3, 5, 4, 7, 2, 1, 8]

        sorted_desc = sort_documents(docs, sort_field="val", sort_dir="desc")
        assert [d["doc_id"] for d in sorted_desc] == [8, 1, 2, 7, 4, 5, 3, 6]

    def test_doc_id_tie_breaker(self) -> None:
        docs = [
            {"doc_id": 40, "score": 100},
            {"doc_id": 10, "score": 100},
            {"doc_id": 30, "score": 100},
            {"doc_id": 20, "score": 100},
        ]
        sorted_asc = sort_documents(docs, sort_field="score", sort_dir="asc")
        assert [d["doc_id"] for d in sorted_asc] == [10, 20, 30, 40]

        sorted_desc = sort_documents(docs, sort_field="score", sort_dir="desc")
        # In desc with identical values, doc_id is still the deterministic tie-breaker
        assert [d["doc_id"] for d in sorted_desc] == [10, 20, 30, 40]


# ═══════════════════════════════════════════════════════════
# 4. Shard-Key Constraint Detector Tests
# ═══════════════════════════════════════════════════════════


class TestShardKeyDetector:
    def test_detect_exact_doc_id(self) -> None:
        c1 = detect_shard_key_constraint({"doc_id": 42})
        assert c1.kind == "exact"
        assert c1.doc_id == 42

        c2 = detect_shard_key_constraint({"doc_id": {"$eq": 99}})
        assert c2.kind == "exact"
        assert c2.doc_id == 99

    def test_detect_in_doc_ids(self) -> None:
        c = detect_shard_key_constraint({"doc_id": {"$in": [1, 2, 3]}})
        assert c.kind == "in"
        assert c.doc_ids == [1, 2, 3]

    def test_detect_range_doc_id(self) -> None:
        c = detect_shard_key_constraint({"doc_id": {"$gt": 100, "$lt": 200}})
        assert c.kind == "range"
        assert c.range_start == 101
        assert c.range_end == 200

    def test_detect_in_and_clause(self) -> None:
        c = detect_shard_key_constraint(
            {
                "$and": [
                    {"status": "active"},
                    {"doc_id": 500},
                ]
            }
        )
        assert c.kind == "exact"
        assert c.doc_id == 500

    def test_detect_unconstrained(self) -> None:
        assert detect_shard_key_constraint({}).kind == "all"
        assert detect_shard_key_constraint({"name": "alice"}).kind == "all"
        assert detect_shard_key_constraint({"$or": [{"doc_id": 1}, {"name": "bob"}]}).kind == "all"
