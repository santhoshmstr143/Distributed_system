"""
Unit tests for docstore.common — Module 1.

Covers:
- models.py : Document doc_id validation (bool, float, negative, out-of-range, valid)
- models.py : WriteRequest, ShardRange, ShardMap, QueryFilter
- config.py : Timing constraint validation (H<L<D and T+M<L-H)
- auth.py   : HMAC signature generation, verification, constant-time compare
- errors.py : HTTP status mappings, to_http_exception()
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest
from pydantic import ValidationError as PydanticValidationError

if TYPE_CHECKING:
    from docstore.common.config import Settings

pytestmark = pytest.mark.unit

# ── helpers ───────────────────────────────────────────────────────────────────


def _make_settings(**overrides: Any) -> Settings:
    """Create a Settings instance without a real .env file."""
    defaults: dict[str, Any] = {
        "CLUSTER_SECRET": "a" * 32,
        "ADMIN_TOKEN": "b" * 32,
        "CLIENT_API_KEY": "c" * 16,
        "HEARTBEAT_INTERVAL_S": 1.0,
        "LEASE_DURATION_S": 3.0,
        "FAILURE_TIMEOUT_S": 5.0,
        "SAFETY_MARGIN_S": 0.5,
        "REPLICATION_TIMEOUT_S": 1.0,
    }
    defaults.update(overrides)
    from docstore.common.config import Settings

    return Settings.model_validate(defaults)


# ═════════════════════════════════════════════════════════════════════════════
# 1. Document / doc_id validation
# ═════════════════════════════════════════════════════════════════════════════


class TestDocId:
    """Validate doc_id accepts only non-negative 64-bit integers."""

    def test_valid_zero(self) -> None:
        from docstore.common.models import Document

        doc = Document.model_validate({"doc_id": 0, "name": "Alice"})
        assert doc.doc_id == 0

    def test_valid_large(self) -> None:
        from docstore.common.models import DOC_ID_MAX, Document

        doc = Document.model_validate({"doc_id": DOC_ID_MAX})
        assert doc.doc_id == DOC_ID_MAX

    def test_valid_arbitrary(self) -> None:
        from docstore.common.models import Document

        doc = Document.model_validate({"doc_id": 999_999})
        assert doc.doc_id == 999_999

    def test_extra_fields_preserved(self) -> None:
        from docstore.common.models import Document

        doc = Document.model_validate({"doc_id": 1, "x": "hello", "y": 42})
        assert doc.extra_fields() == {"x": "hello", "y": 42}

    # ── Rejection cases ───────────────────────────────────────────────────────

    @pytest.mark.parametrize("bad_id", [True, False])
    def test_bool_rejected(self, bad_id: bool) -> None:
        from docstore.common.models import Document

        with pytest.raises(PydanticValidationError, match="boolean"):
            Document.model_validate({"doc_id": bad_id})

    def test_float_rejected(self) -> None:
        from docstore.common.models import Document

        with pytest.raises(PydanticValidationError):
            Document.model_validate({"doc_id": 1.0})

    def test_string_rejected(self) -> None:
        from docstore.common.models import Document

        with pytest.raises(PydanticValidationError):
            Document.model_validate({"doc_id": "42"})

    def test_negative_rejected(self) -> None:
        from docstore.common.models import Document

        with pytest.raises(PydanticValidationError, match="non-negative"):
            Document.model_validate({"doc_id": -1})

    def test_too_large_rejected(self) -> None:
        from docstore.common.models import DOC_ID_MAX, Document

        with pytest.raises(PydanticValidationError):
            Document.model_validate({"doc_id": DOC_ID_MAX + 1})

    def test_none_rejected(self) -> None:
        from docstore.common.models import Document

        with pytest.raises(PydanticValidationError):
            Document.model_validate({"doc_id": None})

    def test_missing_doc_id_rejected(self) -> None:
        from docstore.common.models import Document

        with pytest.raises(PydanticValidationError):
            Document.model_validate({"name": "no-id"})


# ═════════════════════════════════════════════════════════════════════════════
# 2. WriteRequest validation
# ═════════════════════════════════════════════════════════════════════════════


class TestWriteRequest:
    def test_insert_valid(self) -> None:
        from docstore.common.models import Document, WriteOp, WriteRequest

        wr = WriteRequest(
            request_id="req-123",
            op=WriteOp.INSERT,
            document=Document.model_validate({"doc_id": 10}),
        )
        assert wr.op == WriteOp.INSERT

    def test_delete_without_document_valid(self) -> None:
        from docstore.common.models import WriteOp, WriteRequest

        wr = WriteRequest(request_id="req-del", op=WriteOp.DELETE)
        assert wr.document is None

    def test_insert_without_document_rejected(self) -> None:
        from docstore.common.models import WriteOp, WriteRequest

        with pytest.raises(PydanticValidationError, match=r"document.*required"):
            WriteRequest(request_id="req-x", op=WriteOp.INSERT)

    def test_update_without_document_rejected(self) -> None:
        from docstore.common.models import WriteOp, WriteRequest

        with pytest.raises(PydanticValidationError, match=r"document.*required"):
            WriteRequest(request_id="req-y", op=WriteOp.UPDATE)


# ═════════════════════════════════════════════════════════════════════════════
# 3. ShardRange and ShardMap validation
# ═════════════════════════════════════════════════════════════════════════════


class TestShardRange:
    def test_valid_range(self) -> None:
        from docstore.common.models import ShardRange

        r = ShardRange(start=0, end=100)
        assert r.start == 0 and r.end == 100

    def test_start_ge_end_rejected(self) -> None:
        from docstore.common.models import ShardRange

        with pytest.raises(PydanticValidationError, match="start < end"):
            ShardRange(start=100, end=100)

    def test_start_gt_end_rejected(self) -> None:
        from docstore.common.models import ShardRange

        with pytest.raises(PydanticValidationError):
            ShardRange(start=200, end=100)


class TestShardMap:
    def _full_coverage_entries(self) -> list[dict]:
        """3 shards covering [0, 2^63) exactly."""
        third = 2**63 // 3
        return [
            {
                "shard_id": "s0",
                "range": {"start": 0, "end": third},
                "primary": "host:8200",
                "ownership_epoch": 1,
                "map_version": 1,
            },
            {
                "shard_id": "s1",
                "range": {"start": third, "end": 2 * third},
                "primary": "host:8210",
                "ownership_epoch": 1,
                "map_version": 1,
            },
            {
                "shard_id": "s2",
                "range": {"start": 2 * third, "end": 2**63},
                "primary": "host:8220",
                "ownership_epoch": 1,
                "map_version": 1,
            },
        ]

    def test_valid_full_coverage(self) -> None:
        from docstore.common.models import ShardMap

        sm = ShardMap(map_version=1, entries=self._full_coverage_entries())
        assert len(sm.entries) == 3

    def test_gap_rejected(self) -> None:
        from docstore.common.models import ShardMap, ShardMapEntry, ShardRange

        # Gap: s0 ends at 100, s1 starts at 200
        entries = [
            ShardMapEntry(
                shard_id="s0",
                range=ShardRange(start=0, end=100),
                primary="h:1",
                ownership_epoch=1,
                map_version=1,
            ),
            ShardMapEntry(
                shard_id="s1",
                range=ShardRange(start=200, end=2**63),
                primary="h:2",
                ownership_epoch=1,
                map_version=1,
            ),
        ]
        with pytest.raises(PydanticValidationError, match="Gap or overlap"):
            ShardMap(map_version=1, entries=entries)

    def test_incomplete_coverage_rejected(self) -> None:
        from docstore.common.models import ShardMap, ShardMapEntry, ShardRange

        entries = [
            ShardMapEntry(
                shard_id="s0",
                range=ShardRange(start=0, end=1000),
                primary="h:1",
                ownership_epoch=1,
                map_version=1,
            )
        ]
        with pytest.raises(PydanticValidationError, match="full key space"):
            ShardMap(map_version=1, entries=entries)


# ═════════════════════════════════════════════════════════════════════════════
# 4. Config timing validation
# ═════════════════════════════════════════════════════════════════════════════


class TestConfigTimingConstraints:
    def test_valid_defaults(self) -> None:
        s = _make_settings()
        assert s.HEARTBEAT_INTERVAL_S == 1.0
        assert s.LEASE_DURATION_S == 3.0
        assert s.FAILURE_TIMEOUT_S == 5.0

    @pytest.mark.parametrize(
        ("H", "L", "D"),
        [
            (3.0, 1.0, 5.0),  # H >= L
            (1.0, 5.0, 3.0),  # L >= D
            (1.0, 1.0, 5.0),  # H == L
            (1.0, 3.0, 3.0),  # L == D
        ],
    )
    def test_H_L_D_constraint_violated(self, H: float, L: float, D: float) -> None:
        with pytest.raises((PydanticValidationError, ValueError), match="H < L < D"):
            _make_settings(
                HEARTBEAT_INTERVAL_S=H,
                LEASE_DURATION_S=L,
                FAILURE_TIMEOUT_S=D,
            )

    def test_T_plus_M_constraint_violated(self) -> None:
        # T=2, M=1, L=3, H=1 → T+M=3, L-H=2 → 3 >= 2 → violated
        with pytest.raises((PydanticValidationError, ValueError), match="T \\+ M"):
            _make_settings(
                HEARTBEAT_INTERVAL_S=1.0,
                LEASE_DURATION_S=3.0,
                FAILURE_TIMEOUT_S=5.0,
                REPLICATION_TIMEOUT_S=2.0,
                SAFETY_MARGIN_S=1.0,
            )

    def test_secret_min_length_enforced(self) -> None:
        with pytest.raises((PydanticValidationError, ValueError)):
            _make_settings(CLUSTER_SECRET="short")  # < 32 chars

    def test_admin_token_min_length_enforced(self) -> None:
        with pytest.raises((PydanticValidationError, ValueError)):
            _make_settings(ADMIN_TOKEN="tooshort")


# ═════════════════════════════════════════════════════════════════════════════
# 5. Auth: HMAC signature verification
# ═════════════════════════════════════════════════════════════════════════════


class TestAuth:
    SECRET = "my-test-secret-that-is-long-enough-32x"
    BODY = b'{"doc_id": 42, "name": "Alice"}'

    def test_compute_and_verify_roundtrip(self) -> None:
        from docstore.common.auth import compute_signature, verify_signature

        sig = compute_signature(self.SECRET, self.BODY)
        assert verify_signature(self.SECRET, self.BODY, sig) is True

    def test_wrong_secret_rejected(self) -> None:
        from docstore.common.auth import compute_signature, verify_signature

        sig = compute_signature(self.SECRET, self.BODY)
        assert verify_signature("wrong-secret-xxxxxxxxxxxxxxxxx", self.BODY, sig) is False

    def test_tampered_body_rejected(self) -> None:
        from docstore.common.auth import compute_signature, verify_signature

        sig = compute_signature(self.SECRET, self.BODY)
        assert verify_signature(self.SECRET, b'{"doc_id": 99}', sig) is False

    def test_empty_signature_rejected(self) -> None:
        from docstore.common.auth import verify_signature

        assert verify_signature(self.SECRET, self.BODY, "") is False

    def test_case_insensitive_hex(self) -> None:
        """Signature comparison should be case-insensitive for hex."""
        from docstore.common.auth import compute_signature, verify_signature

        sig = compute_signature(self.SECRET, self.BODY).upper()
        assert verify_signature(self.SECRET, self.BODY, sig) is True

    def test_verify_token_correct(self) -> None:
        from docstore.common.auth import verify_token

        assert verify_token("my-admin-token", "my-admin-token") is True

    def test_verify_token_wrong(self) -> None:
        from docstore.common.auth import verify_token

        assert verify_token("my-admin-token", "wrong-token") is False

    def test_verify_token_empty(self) -> None:
        from docstore.common.auth import verify_token

        assert verify_token("my-admin-token", "") is False


# ═════════════════════════════════════════════════════════════════════════════
# 6. Error model: HTTP status codes and error_code strings
# ═════════════════════════════════════════════════════════════════════════════


class TestErrors:
    @pytest.mark.parametrize(
        ("exc_cls", "expected_status", "expected_code"),
        [
            ("StaleEpoch", 409, "STALE_EPOCH"),
            ("StaleTerm", 409, "STALE_TERM"),
            ("NotPrimary", 409, "NOT_PRIMARY"),
            ("LeaseExpired", 409, "LEASE_EXPIRED"),
            ("DuplicateKey", 409, "DUPLICATE_KEY"),
            ("NotFound", 404, "NOT_FOUND"),
            ("OutOfRange", 400, "OUT_OF_RANGE"),
            ("InvalidQuery", 400, "INVALID_QUERY"),
            ("AuthError", 401, "AUTH_FAILED"),
            ("BodyTooLarge", 413, "BODY_TOO_LARGE"),
            ("RangePaused", 503, "RANGE_PAUSED"),
            ("ReplicationUnavailable", 503, "REPLICATION_UNAVAILABLE"),
            ("OutcomeUnknown", 202, "OUTCOME_UNKNOWN"),
        ],
    )
    def test_http_status_and_code(
        self, exc_cls: str, expected_status: int, expected_code: str
    ) -> None:
        import docstore.common.errors as errs

        cls = getattr(errs, exc_cls)
        exc = cls("test message")
        http_exc = exc.to_http_exception()
        assert http_exc.status_code == expected_status
        assert http_exc.detail["error_code"] == expected_code
        assert "traceback" not in str(http_exc.detail).lower()
        assert "stack" not in str(http_exc.detail).lower()

    def test_error_message_preserved(self) -> None:
        from docstore.common.errors import NotFound

        exc = NotFound("doc 42 not found")
        http_exc = exc.to_http_exception()
        assert "42" in http_exc.detail["error_message"]


# ═════════════════════════════════════════════════════════════════════════════
# 7. QueryFilter size caps
# ═════════════════════════════════════════════════════════════════════════════


class TestQueryFilter:
    def test_defaults(self) -> None:
        from docstore.common.models import QueryFilter

        qf = QueryFilter()
        assert qf.limit == 100
        assert qf.skip == 0

    def test_limit_cap(self) -> None:
        from docstore.common.models import QueryFilter

        with pytest.raises(PydanticValidationError):
            QueryFilter(limit=1001)

    def test_skip_cap(self) -> None:
        from docstore.common.models import QueryFilter

        with pytest.raises(PydanticValidationError):
            QueryFilter(skip=10_001)

    def test_valid_custom(self) -> None:
        from docstore.common.models import QueryFilter

        qf = QueryFilter(skip=500, limit=250)
        assert qf.skip == 500
        assert qf.limit == 250
