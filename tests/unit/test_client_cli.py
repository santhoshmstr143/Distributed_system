"""Unit and integration tests for Module 10: Client SDK and CLI."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
from click.testing import CliRunner

from docstore.client.cli import cli
from docstore.client.client import (
    AuthenticationError,
    DocStoreClient,
    DuplicateKeyError,
    InvalidQueryError,
    NotFoundError,
    OutcomeUnknownError,
    ReplicationUnavailableError,
)


def _mock_handler(request: httpx.Request) -> httpx.Response:
    url_str = str(request.url)
    path = request.url.path
    method = request.method

    # 1. Router /doc [POST]
    if path == "/doc" and method == "POST":
        body = json.loads(request.content)
        rid = body.get("request_id")
        doc = body.get("document", {})
        did = doc.get("doc_id")

        if did == 999:  # simulate duplicate
            return httpx.Response(
                409,
                json={"error_code": "DUPLICATE_KEY", "error_message": "Duplicate key 999"},
            )
        if did == 503:  # simulate transient error
            return httpx.Response(
                503,
                json={
                    "error_code": "REPLICATION_UNAVAILABLE",
                    "error_message": "Follower not synced",
                },
            )
        if did == 202:  # simulate outcome unknown
            return httpx.Response(
                202,
                json={
                    "error_code": "OUTCOME_UNKNOWN",
                    "error_message": "Append timed out",
                },
            )

        return httpx.Response(
            201,
            json={
                "outcome": "COMMITTED",
                "request_id": rid,
                "seq": 1,
                "meta": {"served_by": "primary", "applied_seq": 1, "replication_lag": None},
            },
        )

    # 2. Router /doc/{id} [GET]
    if path.startswith("/doc/") and method == "GET":
        did_str = path.split("/")[-1]
        try:
            did_int = int(did_str)
        except ValueError:
            return httpx.Response(
                400,
                json={"error_code": "VALIDATION_ERROR", "error_message": "Bad ID"},
            )

        if did_int == 404:
            return httpx.Response(
                404,
                json={"error_code": "NOT_FOUND", "error_message": f"Doc {did_int} not found"},
            )

        read_mode = request.url.params.get("read_mode", "primary")
        served = "follower" if read_mode == "follower" else "primary"
        lag = 0 if read_mode == "follower" else None

        return httpx.Response(
            200,
            json={
                "document": {"doc_id": did_int, "title": "Test Doc"},
                "meta": {"served_by": served, "applied_seq": 1, "replication_lag": lag},
            },
        )

    # 3. Router /doc/{id} [PUT]
    if path.startswith("/doc/") and method == "PUT":
        did_str = path.split("/")[-1]
        body = json.loads(request.content)
        rid = body.get("request_id")
        return httpx.Response(
            200,
            json={
                "outcome": "COMMITTED",
                "request_id": rid,
                "meta": {"served_by": "primary", "applied_seq": 2},
            },
        )

    # 4. Router /doc/{id} [DELETE]
    if path.startswith("/doc/") and method == "DELETE":
        body = json.loads(request.content)
        rid = body.get("request_id")
        return httpx.Response(
            200,
            json={
                "outcome": "COMMITTED",
                "request_id": rid,
                "meta": {"served_by": "primary", "applied_seq": 3},
            },
        )

    # 5. Router /query [POST]
    if path == "/query" and method == "POST":
        body = json.loads(request.content)
        filt = body.get("filter", {})
        if "$malicious" in filt:
            return httpx.Response(
                400,
                json={
                    "error_code": "INVALID_QUERY",
                    "error_message": "Invalid operator $malicious",
                },
            )

        read_mode = body.get("read_mode", "primary")
        return httpx.Response(
            200,
            json={
                "documents": [{"doc_id": 1, "name": "A"}, {"doc_id": 2, "name": "B"}],
                "total": 2,
                "meta": {"served_by": read_mode, "applied_seq": 3},
            },
        )

    # 6. Coordinator /shardmap [GET]
    if path == "/shardmap" and method == "GET":
        return httpx.Response(
            200,
            json={
                "map_version": 1,
                "entries": [
                    {
                        "shard_id": "shard0",
                        "range": {"start": 0, "end": 1000},
                        "primary": "127.0.0.1:8200",
                        "ownership_epoch": 1,
                    }
                ],
            },
        )

    # 7. Coordinator /admin/status [GET]
    if path == "/admin/status" and method == "GET":
        token = request.headers.get("X-Admin-Token")
        if not token or token == "bad-token":
            return httpx.Response(
                401,
                json={"error_code": "AUTH_FAILED", "error_message": "Bad admin token"},
            )
        return httpx.Response(
            200,
            json={
                "status": "healthy",
                "map_version": 1,
                "shards": {"shard0": {"primary": "node0", "replica_state": "SYNCED"}},
            },
        )

    # 8. Coordinator /admin/migrate [POST]
    if path == "/admin/migrate" and method == "POST":
        token = request.headers.get("X-Admin-Token")
        if not token or token == "bad-token":
            return httpx.Response(
                401,
                json={"error_code": "AUTH_FAILED", "error_message": "Bad admin token"},
            )
        body = json.loads(request.content)
        return httpx.Response(
            200,
            json={
                "migration_id": "mig-12345",
                "state": "DONE",
                "from_shard": body.get("from_shard"),
                "to_shard": body.get("to_shard"),
                "docs_copied": 50,
            },
        )

    return httpx.Response(
        404,
        json={"error_code": "NOT_FOUND", "error_message": f"Path {url_str} not found"},
    )


# ═══════════════════════════════════════════════════════════════════════════
# 1. Python Client Library Unit Tests
# ═══════════════════════════════════════════════════════════════════════════


def test_client_crud_and_query_success() -> None:
    transport = httpx.MockTransport(_mock_handler)
    http_client = httpx.Client(transport=transport)

    with DocStoreClient(http_client=http_client, api_key="secret-key") as client:
        # 1. Insert generates request_id
        res_ins = client.insert({"doc_id": 42, "title": "First"})
        assert res_ins["outcome"] == "COMMITTED"
        assert res_ins["request_id"] is not None

        # 2. Get with primary read mode
        res_get = client.get(42, read_mode="primary")
        assert res_get["document"]["doc_id"] == 42
        assert res_get["meta"]["served_by"] == "primary"

        # 3. Get with follower read mode
        res_get_fol = client.get(42, read_mode="follower")
        assert res_get_fol["meta"]["served_by"] == "follower"
        assert res_get_fol["meta"]["replication_lag"] == 0

        # 4. Update
        res_upd = client.update(42, {"doc_id": 42, "title": "Updated"})
        assert res_upd["outcome"] == "COMMITTED"

        # 5. Delete
        res_del = client.delete(42)
        assert res_del["outcome"] == "COMMITTED"

        # 6. Query
        res_query = client.query(filter={"tags": "test"}, limit=10)
        assert res_query["total"] == 2
        assert len(res_query["documents"]) == 2

        # 7. Shard map
        res_map = client.shard_map()
        assert res_map["map_version"] == 1

        # 8. Admin status with valid token
        client.admin_token = "valid-token"
        res_status = client.status()
        assert res_status["status"] == "healthy"

        # 9. Live migration
        res_mig = client.migrate("shard0", "shard1", 100, 200)
        assert res_mig["migration_id"] == "mig-12345"


def test_client_typed_errors() -> None:
    transport = httpx.MockTransport(_mock_handler)
    http_client = httpx.Client(transport=transport)

    with DocStoreClient(http_client=http_client, max_retries=1) as client:
        # Not found raises NotFoundError
        with pytest.raises(NotFoundError):
            client.get(404)

        # Duplicate key raises DuplicateKeyError
        with pytest.raises(DuplicateKeyError):
            client.insert({"doc_id": 999})

        # Invalid query raises InvalidQueryError
        with pytest.raises(InvalidQueryError):
            client.query(filter={"$malicious": True})

        # Bad admin token raises AuthenticationError
        client.admin_token = "bad-token"
        with pytest.raises(AuthenticationError):
            client.status()

        # Transient 503 retries and eventually raises ReplicationUnavailableError
        with pytest.raises(ReplicationUnavailableError):
            client.insert({"doc_id": 503})

        # Transient 202 retries and eventually raises OutcomeUnknownError
        with pytest.raises(OutcomeUnknownError):
            client.insert({"doc_id": 202})


def test_client_idempotent_retry_reuses_same_request_id() -> None:
    """Verifies that transient retry logic reuses the exact same request_id."""
    seen_request_ids: list[str] = []

    def flaky_handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        rid = body["request_id"]
        seen_request_ids.append(rid)

        if len(seen_request_ids) < 3:
            return httpx.Response(
                503,
                json={"error_code": "REPLICATION_UNAVAILABLE", "error_message": "Follower lag"},
            )
        return httpx.Response(
            201,
            json={"outcome": "COMMITTED", "request_id": rid, "seq": 10},
        )

    transport = httpx.MockTransport(flaky_handler)
    http_client = httpx.Client(transport=transport)

    with DocStoreClient(http_client=http_client, max_retries=4, backoff_base_s=0.01) as client:
        res = client.insert({"doc_id": 100})
        assert res["outcome"] == "COMMITTED"
        assert len(seen_request_ids) == 3
        # Crucial invariant: all attempts used the exact same request_id!
        assert seen_request_ids[0] == seen_request_ids[1] == seen_request_ids[2]


def test_client_bulk_load() -> None:
    transport = httpx.MockTransport(_mock_handler)
    http_client = httpx.Client(transport=transport)

    with DocStoreClient(http_client=http_client) as client:
        docs = [
            {"doc_id": 1, "val": "a"},
            {"doc_id": 999, "val": "b"},  # fails with duplicate key
            {"doc_id": 3, "val": "c"},
        ]
        summary = client.bulk_load(docs)
        assert summary["total"] == 3
        assert summary["succeeded"] == 2
        assert summary["failed"] == 1
        assert len(summary["errors"]) == 1
        assert summary["errors"][0]["doc_id"] == 999


# ═══════════════════════════════════════════════════════════════════════════
# 2. CLI Tool Tests (Happy Paths and Error Exit Codes)
# ═══════════════════════════════════════════════════════════════════════════


def test_cli_help() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Sharded-Docstore CLI tool" in result.output
    assert "insert" in result.output
    assert "query" in result.output
    assert "migrate" in result.output


def test_cli_insert_and_get_success(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    runner = CliRunner()
    transport = httpx.MockTransport(_mock_handler)
    client = DocStoreClient(http_client=httpx.Client(transport=transport))

    # 1. Insert via --doc JSON string
    res_ins = runner.invoke(
        cli, ["insert", "--doc", '{"doc_id": 10, "k": "v"}'], obj={"client": client}
    )
    assert res_ins.exit_code == 0
    out_ins = res_ins.stdout or res_ins.output or capsys.readouterr().out
    data_ins = json.loads(out_ins)
    assert data_ins["outcome"] == "COMMITTED"

    # 2. Insert via --file
    doc_file = tmp_path / "test_doc.json"
    doc_file.write_text(json.dumps({"doc_id": 11, "k": "v2"}), encoding="utf-8")
    res_file = runner.invoke(cli, ["insert", "-f", str(doc_file)], obj={"client": client})
    assert res_file.exit_code == 0
    capsys.readouterr()

    # 3. Get document
    res_get = runner.invoke(cli, ["get", "10", "--read-mode", "follower"], obj={"client": client})
    assert res_get.exit_code == 0
    out_get = res_get.stdout or res_get.output or capsys.readouterr().out
    data_get = json.loads(out_get)
    assert data_get["document"]["doc_id"] == 10
    assert data_get["meta"]["served_by"] == "follower"


def test_cli_update_delete_query(capsys: pytest.CaptureFixture[str]) -> None:
    runner = CliRunner()
    transport = httpx.MockTransport(_mock_handler)
    client = DocStoreClient(http_client=httpx.Client(transport=transport))

    # Update
    res_upd = runner.invoke(
        cli,
        ["update", "10", "--doc", '{"doc_id": 10, "name": "new"}'],
        obj={"client": client},
    )
    assert res_upd.exit_code == 0
    out_upd = res_upd.stdout or res_upd.output or capsys.readouterr().out
    assert json.loads(out_upd)["outcome"] == "COMMITTED"

    # Delete
    res_del = runner.invoke(cli, ["delete", "10"], obj={"client": client})
    assert res_del.exit_code == 0
    out_del = res_del.stdout or res_del.output or capsys.readouterr().out
    assert json.loads(out_del)["outcome"] == "COMMITTED"

    # Query
    res_q = runner.invoke(
        cli,
        ["query", "--filter", '{"name": "A"}', "--limit", "5"],
        obj={"client": client},
    )
    assert res_q.exit_code == 0
    out_q = res_q.stdout or res_q.output or capsys.readouterr().out
    data_q = json.loads(out_q)
    assert data_q["total"] == 2


def test_cli_admin_and_bulk_load(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    runner = CliRunner()
    transport = httpx.MockTransport(_mock_handler)
    client = DocStoreClient(
        admin_token="valid-token",
        http_client=httpx.Client(transport=transport),
    )

    # Status (with admin token)
    res_status = runner.invoke(cli, ["status"], obj={"client": client})
    assert res_status.exit_code == 0
    out_status = res_status.stdout or res_status.output or capsys.readouterr().out
    assert json.loads(out_status)["status"] == "healthy"

    # Shardmap
    res_map = runner.invoke(cli, ["shardmap"], obj={"client": client})
    assert res_map.exit_code == 0
    out_map = res_map.stdout or res_map.output or capsys.readouterr().out
    assert json.loads(out_map)["map_version"] == 1

    # Migrate
    res_mig = runner.invoke(
        cli,
        [
            "migrate",
            "--from-shard",
            "shard0",
            "--to-shard",
            "shard1",
            "--start",
            "0",
            "--end",
            "100",
        ],
        obj={"client": client},
    )
    assert res_mig.exit_code == 0
    out_mig = res_mig.stdout or res_mig.output or capsys.readouterr().out
    assert json.loads(out_mig)["migration_id"] == "mig-12345"

    # Bulk-load
    bulk_file = tmp_path / "bulk.json"
    bulk_file.write_text(json.dumps([{"doc_id": 1}, {"doc_id": 2}]), encoding="utf-8")
    res_bulk = runner.invoke(cli, ["bulk-load", str(bulk_file)], obj={"client": client})
    assert res_bulk.exit_code == 0
    out_bulk = res_bulk.stdout or res_bulk.output or capsys.readouterr().out
    assert json.loads(out_bulk)["succeeded"] == 2


def test_cli_error_exit_codes(capsys: pytest.CaptureFixture[str]) -> None:
    runner = CliRunner()
    transport = httpx.MockTransport(_mock_handler)
    client = DocStoreClient(
        admin_token="bad-token",
        http_client=httpx.Client(transport=transport),
        max_retries=0,
    )

    # Not found -> exit code 2
    res_nf = runner.invoke(cli, ["get", "404"], obj={"client": client})
    assert res_nf.exit_code == 2
    err_nf = res_nf.output + (res_nf.stderr or "") + capsys.readouterr().err
    assert "NOT_FOUND" in err_nf

    # Duplicate key -> exit code 3
    res_dup = runner.invoke(cli, ["insert", "--doc", '{"doc_id": 999}'], obj={"client": client})
    assert res_dup.exit_code == 3
    err_dup = res_dup.output + (res_dup.stderr or "") + capsys.readouterr().err
    assert "DUPLICATE_KEY" in err_dup

    # Auth failed -> exit code 4
    res_auth = runner.invoke(cli, ["status"], obj={"client": client})
    assert res_auth.exit_code == 4
    err_auth = res_auth.output + (res_auth.stderr or "") + capsys.readouterr().err
    assert "Authentication failed" in err_auth

    # Invalid query -> exit code 5
    res_iq = runner.invoke(cli, ["query", "--filter", '{"$malicious": 1}'], obj={"client": client})
    assert res_iq.exit_code == 5
    err_iq = res_iq.output + (res_iq.stderr or "") + capsys.readouterr().err
    assert "INVALID_QUERY" in err_iq
