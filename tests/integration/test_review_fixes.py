"""
Regression tests for review findings:
- Issue 1: Docker shard ownership ranges matching shard map without overlapping full keyspace
- Issue 2: Durable term and epoch persistence across crash-restart; old primary restarts as fenced follower
- Issue 3: Pending-write reconciliation:
    1. Follower contains op but ack was lost -> commits on retry
    2. Follower is behind -> stays unresolved, subsequent writes blocked
    3. Follower unavailable -> stays unresolved, subsequent writes blocked
    4. Follower has conflicting operation at same sequence -> authoritative reconciliation resolves as uncommitted
    5. Authoritative log reconciliation proves op absent -> resolves as uncommitted
    6. Retry does not execute duplicate operation
- Issue 4: Migration snapshot consistency with applied state
- Issue 5: Migration recovery after coordinator restart:
    1. Failed cutover / cleanup RPCs keep migration in recoverable CLEANUP state, record error, NOT marked DONE
    2. Failed unpause / abort RPCs keep migration in recoverable pre-cutover state, record error, NOT marked ABORTED
    3. Successful RPCs roll forward to DONE / abort to ABORTED idempotently
- Issue 6: Scatter-gather pagination for large skip + limit windows exceeding QUERY_LIMIT_MAX
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest
import yaml
from asgi_lifespan import LifespanManager
from httpx import ASGITransport, AsyncClient

from docstore.common.auth import CLIENT_KEY_HEADER, CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.models import NodeRole, QueryFilter, ReplicaState
from docstore.coordinator.coordinator import CoordinatorService
from docstore.coordinator.migration import MigrationManager, MigrationState
from docstore.router.scatter import execute_scatter_query
from docstore.router.shard_map import ShardMapManager, create_default_3_shard_map
from docstore.shard.app import ShardRangeConfig, build_app, resync_with_primary

pytestmark = pytest.mark.integration

SECRET = "a" * 32
ADMIN = "b" * 32
CLIENT_KEY = "c" * 16


def _make_settings(data_dir: Path | str) -> Settings:
    return Settings(
        CLUSTER_SECRET=SECRET,
        ADMIN_TOKEN=ADMIN,
        CLIENT_API_KEY=CLIENT_KEY,
        DATA_DIR=str(data_dir),
        REPLICATION_TIMEOUT_S=0.5,
        SAFETY_MARGIN_S=0.1,
    )


def _cluster_headers(body_bytes: bytes) -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        CLUSTER_SIG_HEADER: compute_signature(SECRET, body_bytes),
    }


def test_issue1_docker_compose_shard_ranges():
    """Verify docker-compose specifies distinct ranges for all shards matching shard_map.json."""
    compose_path = Path("docker/docker-compose.yml")
    shard_map_path = Path("docker/shard_map.json")
    assert compose_path.exists()
    assert shard_map_path.exists()

    with shard_map_path.open("r", encoding="utf-8") as f:
        smap = json.load(f)

    expected_ranges = {
        e["shard_id"]: (e["range"]["start"], e["range"]["end"]) for e in smap["entries"]
    }

    with compose_path.open("r", encoding="utf-8") as f:
        compose = yaml.safe_load(f)

    services = compose.get("services", {})
    for shard_id, (expected_start, expected_end) in expected_ranges.items():
        for role in ("primary", "follower"):
            svc_name = f"{shard_id}-{role}"
            assert svc_name in services, f"Service {svc_name} missing from docker-compose.yml"
            env = services[svc_name].get("environment", [])
            env_dict = {}
            for item in env:
                if "=" in item:
                    k, v = item.split("=", 1)
                    env_dict[k] = v
            assert "SHARD_RANGE_START" in env_dict, f"SHARD_RANGE_START missing in {svc_name}"
            assert "SHARD_RANGE_END" in env_dict, f"SHARD_RANGE_END missing in {svc_name}"
            assert int(env_dict["SHARD_RANGE_START"]) == expected_start
            assert int(env_dict["SHARD_RANGE_END"]) == expected_end
            assert int(env_dict.get("SHARD_OWNERSHIP_EPOCH", 1)) == 1


@pytest.mark.asyncio
async def test_issue2_durable_term_and_epoch_persistence(tmp_path: Path):
    """Verify promotion persists term and epoch, but a restarted primary does NOT regain authority."""
    settings = _make_settings(tmp_path)
    shard_id = "shard0"

    # 1. Start node in term 1
    app1 = build_app(
        settings=settings,
        node_id="shard0-node1",
        shard_id=shard_id,
        role=NodeRole.FOLLOWER,
    )
    async with LifespanManager(app1):
        transport = ASGITransport(app=app1)
        async with AsyncClient(transport=transport, base_url="http://node") as client:
            # Promote to term 5 with new epoch
            promote_body = {
                "term": 5,
                "role": "primary",
                "ownership_epochs": {"0-9223372036854775808": 42},
            }
            body_bytes = json.dumps(promote_body).encode("utf-8")
            resp = await client.post(
                "/internal/promote", content=body_bytes, headers=_cluster_headers(body_bytes)
            )
            assert resp.status_code == 200
            assert resp.json()["term"] == 5

    # 2. Verify metadata file exists durably on disk
    meta_path = tmp_path / shard_id / "node_metadata.json"
    assert meta_path.exists()
    with meta_path.open("r", encoding="utf-8") as f:
        meta = json.load(f)
    assert meta["term"] == 5
    assert any(r["ownership_epoch"] == 42 for r in meta["ranges"])

    # 3. Simulate crash and restart without any snapshot taken
    app2 = build_app(
        settings=settings,
        node_id="shard0-node1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,  # Passed primary role in config/args
    )
    async with LifespanManager(app2):
        node_state = app2.state.node
        assert node_state.term == 5
        # Must NOT regain primary authority! Recovered node must start in fenced FOLLOWER state
        assert node_state.role == NodeRole.FOLLOWER
        assert node_state.lease_expires_at == 0.0
        assert any(r.ownership_epoch == 42 for r in node_state.ranges)

        # Verify that write attempts to the restarted node are rejected with 409 NOT_PRIMARY
        transport2 = ASGITransport(app=app2)
        async with AsyncClient(transport=transport2, base_url="http://node") as client2:
            resp2 = await client2.post(
                "/doc",
                json={"request_id": "req-restart", "document": {"doc_id": 100, "val": "x"}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert resp2.status_code == 409


@pytest.mark.asyncio
async def test_restarted_old_primary_cannot_regain_authority_and_rejoins_as_follower(
    tmp_path: Path,
):
    """Regression test for Issue 1:
    1. Start primary A and follower B.
    2. Promote B after A fails.
    3. Restart A using its old metadata.
    4. Verify A cannot serve writes or act as primary.
    5. Verify A can rejoin only as a synchronized follower after reconciliation.
    """
    settings_a = _make_settings(tmp_path / "node_a")
    settings_b = _make_settings(tmp_path / "node_b")
    ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

    # 1. Start primary A and follower B
    app_a = build_app(
        settings=settings_a,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
        ranges=ranges,
    )
    app_b = build_app(
        settings=settings_b,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
        ranges=ranges,
    )

    async with LifespanManager(app_a), LifespanManager(app_b):
        trans_b = ASGITransport(app=app_b)
        async with AsyncClient(transport=trans_b, base_url="http://node_b") as client_b:
            app_a.state.node.follower_url = "node_b"
            app_a.state.node.http_client = client_b

            trans_a = ASGITransport(app=app_a)
            async with AsyncClient(transport=trans_a, base_url="http://node_a") as client_a:
                # Write doc 10 in term 1
                resp = await client_a.post(
                    "/doc",
                    json={"request_id": "req-1", "document": {"doc_id": 10, "val": "v1"}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert resp.status_code == 201

    # 2. A crashes. Promote B to term 2.
    async with LifespanManager(app_b):
        trans_b = ASGITransport(app=app_b)
        async with AsyncClient(transport=trans_b, base_url="http://node_b") as client_b:
            promote_body = {
                "term": 2,
                "role": "primary",
                "ownership_epochs": {"0-1000": 2},
            }
            body_bytes = json.dumps(promote_body).encode("utf-8")
            resp = await client_b.post(
                "/internal/promote",
                content=body_bytes,
                headers=_cluster_headers(body_bytes),
            )
            assert resp.status_code == 200
            assert app_b.state.node.role == NodeRole.PRIMARY
            assert app_b.state.node.term == 2

            # 3. Restart A using its old metadata
            app_a_restarted = build_app(
                settings=settings_a,
                node_id="shard0-primary",
                shard_id="shard0",
                role=NodeRole.PRIMARY,  # Even if role argument says primary
            )
            async with LifespanManager(app_a_restarted):
                state_a = app_a_restarted.state.node
                # 4. Verify A cannot serve writes or act as primary
                assert state_a.role == NodeRole.FOLLOWER
                assert state_a.lease_expires_at == 0.0

                trans_a_restarted = ASGITransport(app=app_a_restarted)
                async with AsyncClient(
                    transport=trans_a_restarted, base_url="http://node_a"
                ) as client_a_restarted:
                    w_resp = await client_a_restarted.post(
                        "/doc",
                        json={"request_id": "req-split", "document": {"doc_id": 20, "val": "v2"}},
                        headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                    )
                    assert w_resp.status_code == 409

                # 5. Verify A can rejoin only as a synchronized follower after reconciliation
                rejoin_client = AsyncClient(mounts={"http://node_b": trans_b}, timeout=5.0)
                resync_ok = await resync_with_primary(
                    state_a,
                    "node_b",
                    http_client=rejoin_client,
                )
                assert resync_ok is True
                assert state_a.role == NodeRole.FOLLOWER
                assert app_b.state.node.follower_state == ReplicaState.SYNCED


@pytest.mark.asyncio
async def test_issue3_pending_write_reconciliation_follower_has_op(tmp_path: Path):
    """Test pending write reconciliation when follower has durably fsynced the operation."""
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")

    follower_app = build_app(
        settings=f_settings,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
    )
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
    )

    async with LifespanManager(follower_app), LifespanManager(primary_app):
        f_transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=f_transport, base_url="http://follower") as f_client:
            primary_app.state.node.follower_url = "follower"
            primary_app.state.node.http_client = f_client
            primary_app.state.node.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
            follower_app.state.node.ranges = [
                ShardRangeConfig(start=0, end=1000, ownership_epoch=1)
            ]

            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # 1. Normal write
                doc_payload = {"request_id": "req-1", "document": {"doc_id": 10, "title": "Doc1"}}
                resp = await p_client.post(
                    "/doc", json=doc_payload, headers={CLIENT_KEY_HEADER: CLIENT_KEY}
                )
                assert resp.status_code == 201

                # 2. Simulate replication failure where follower stored it but ack was lost
                f_state = follower_app.state.node
                f_seq = f_state.wal.next_seq()
                f_state.wal.append(
                    seq=f_seq,
                    term=f_state.term,
                    op="insert",
                    doc_id=20,
                    request_id="req-2",
                    payload={"doc_id": 20, "title": "Doc2"},
                )
                f_state.engine.apply(
                    {
                        "seq": f_seq,
                        "term": f_state.term,
                        "op": "insert",
                        "doc_id": 20,
                        "request_id": "req-2",
                        "payload": {"doc_id": 20, "title": "Doc2"},
                    }
                )

                p_state = primary_app.state.node
                p_seq = p_state.wal.next_seq()
                p_state.wal.append(
                    seq=p_seq,
                    term=p_state.term,
                    op="insert",
                    doc_id=20,
                    request_id="req-2",
                    payload={"doc_id": 20, "title": "Doc2"},
                )
                p_state.follower_state = ReplicaState.DOWN
                p_state.pending_unresolved_writes["req-2"] = {
                    "seq": p_seq,
                    "term": p_state.term,
                    "op": "insert",
                    "doc_id": 20,
                    "payload": {"doc_id": 20, "title": "Doc2"},
                }

                # Attempting unrelated write while req-2 is unresolved should be rejected
                unrelated_resp = await p_client.post(
                    "/doc",
                    json={
                        "request_id": "req-unrelated",
                        "document": {"doc_id": 30, "title": "Doc3"},
                    },
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert unrelated_resp.status_code == 503

                # Retry req-2: primary reconciles with follower via /internal/check-op!
                retry_resp = await p_client.post(
                    "/doc",
                    json={"request_id": "req-2", "document": {"doc_id": 20, "title": "Doc2"}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert retry_resp.status_code in (200, 201)
                assert retry_resp.json()["outcome"] == "COMMITTED"
                assert "req-2" not in p_state.pending_unresolved_writes
                assert p_state.engine.get(20) is not None


@pytest.mark.asyncio
async def test_pending_write_reconciliation_follower_behind(tmp_path: Path):
    """Test that if follower is behind (wal_last_seq < seq), absence is NOT proof of failure;
    outcome remains unresolved and subsequent writes remain blocked."""
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")

    follower_app = build_app(
        settings=f_settings,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
    )
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
    )

    async with LifespanManager(follower_app), LifespanManager(primary_app):
        f_transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=f_transport, base_url="http://follower") as f_client:
            p_state = primary_app.state.node
            f_state = follower_app.state.node
            p_state.follower_url = "follower"
            p_state.http_client = f_client
            p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
            f_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # Follower is at seq=5, but primary has pending op at seq=10
            f_state.wal.append(
                seq=5,
                term=f_state.term,
                op="insert",
                doc_id=5,
                request_id="req-5",
                payload={"doc_id": 5},
            )
            assert f_state.wal.last_seq == 5
            p_state.follower_state = ReplicaState.DOWN
            p_state.pending_unresolved_writes["req-lag"] = {
                "seq": 10,
                "term": p_state.term,
                "op": "insert",
                "doc_id": 50,
                "payload": {"doc_id": 50, "v": 1},
            }

            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # 1. Retrying req-lag when follower is behind reports 202 OUTCOME_UNKNOWN
                retry_resp = await p_client.post(
                    "/doc",
                    json={"request_id": "req-lag", "document": {"doc_id": 50, "v": 1}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert retry_resp.status_code == 202
                # Must NOT remove req-lag from pending writes!
                assert "req-lag" in p_state.pending_unresolved_writes

                # 2. Subsequent writes MUST remain blocked with 503 REPLICATION_UNAVAILABLE
                subsequent_resp = await p_client.post(
                    "/doc",
                    json={"request_id": "req-subsequent", "document": {"doc_id": 60, "v": 2}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert subsequent_resp.status_code == 503
                assert "earlier write has an unresolved replication outcome" in subsequent_resp.text


@pytest.mark.asyncio
async def test_pending_write_reconciliation_follower_unavailable(tmp_path: Path):
    """Test that if follower is unavailable, write remains unresolved and subsequent writes are blocked."""
    p_settings = _make_settings(tmp_path / "primary")
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
    )

    async with LifespanManager(primary_app):
        p_state = primary_app.state.node
        p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
        p_state.follower_url = "nonexistent-follower:9999"
        p_state.follower_state = ReplicaState.DOWN
        p_state.pending_unresolved_writes["req-unavail"] = {
            "seq": 10,
            "term": p_state.term,
            "op": "insert",
            "doc_id": 70,
            "payload": {"doc_id": 70},
        }

        p_transport = ASGITransport(app=primary_app)
        async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
            # Retrying req-unavail yields 202 OUTCOME_UNKNOWN and write stays in pending
            r_resp = await p_client.post(
                "/doc",
                json={"request_id": "req-unavail", "document": {"doc_id": 70}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert r_resp.status_code == 202
            assert "req-unavail" in p_state.pending_unresolved_writes

            # Subsequent write is blocked
            sub_resp = await p_client.post(
                "/doc",
                json={"request_id": "req-blocked", "document": {"doc_id": 80}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert sub_resp.status_code == 503


@pytest.mark.asyncio
async def test_pending_write_reconciliation_conflicting_op(tmp_path: Path):
    """Test that if follower has a conflicting operation at seq (wal_last_seq >= seq but different op),
    authoritative log reconciliation proves it absent -> resolved as uncommitted, client retry can execute."""
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")

    follower_app = build_app(
        settings=f_settings,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
    )
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
    )

    async with LifespanManager(follower_app), LifespanManager(primary_app):
        f_transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=f_transport, base_url="http://follower") as f_client:
            p_state = primary_app.state.node
            f_state = follower_app.state.node
            p_state.follower_url = "follower"
            p_state.http_client = f_client
            p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
            f_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # Follower has a DIFFERENT entry at seq 10 (different request_id "req-other")
            f_state.wal.append(
                seq=10,
                term=f_state.term,
                op="insert",
                doc_id=99,
                request_id="req-other",
                payload={"doc_id": 99},
            )
            assert f_state.wal.last_seq >= 10

            # Primary has pending "req-conflict" at seq 10
            p_state.pending_unresolved_writes["req-conflict"] = {
                "seq": 10,
                "term": p_state.term,
                "op": "insert",
                "doc_id": 100,
                "payload": {"doc_id": 100},
            }

            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # Retrying req-conflict triggers reconciliation: follower is at seq 10 with conflicting op.
                # Authoritative log proves req-conflict was absent/not committed!
                await p_client.post(
                    "/doc",
                    json={"request_id": "req-conflict", "document": {"doc_id": 100}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                # Resolved as uncommitted, so it gets removed from pending!
                assert "req-conflict" not in p_state.pending_unresolved_writes


@pytest.mark.asyncio
async def test_pending_write_reconciliation_authoritative_absence(tmp_path: Path):
    """Test authoritative absence when follower is caught up (wal_last_seq >= seq) and op is absent."""
    f_settings = _make_settings(tmp_path / "follower")
    p_settings = _make_settings(tmp_path / "primary")

    follower_app = build_app(
        settings=f_settings,
        node_id="shard0-follower",
        shard_id="shard0",
        role=NodeRole.FOLLOWER,
    )
    primary_app = build_app(
        settings=p_settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
    )

    async with LifespanManager(follower_app), LifespanManager(primary_app):
        f_transport = ASGITransport(app=follower_app)
        async with AsyncClient(transport=f_transport, base_url="http://follower") as f_client:
            p_state = primary_app.state.node
            f_state = follower_app.state.node
            p_state.follower_url = "follower"
            p_state.http_client = f_client
            p_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]
            f_state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # Follower has logged up to seq 15
            f_state.wal.append(
                seq=15,
                term=f_state.term,
                op="insert",
                doc_id=15,
                request_id="req-15",
                payload={"doc_id": 15},
            )
            assert f_state.wal.last_seq == 15

            # Primary has pending write at seq 10
            p_state.pending_unresolved_writes["req-absent"] = {
                "seq": 10,
                "term": p_state.term,
                "op": "insert",
                "doc_id": 110,
                "payload": {"doc_id": 110},
            }

            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                # Primary checks follower: follower wal_last_seq is 15 >= 10, and entry 10 is absent
                await p_client.post(
                    "/doc",
                    json={"request_id": "req-absent", "document": {"doc_id": 110}},
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert "req-absent" not in p_state.pending_unresolved_writes


@pytest.mark.asyncio
async def test_pending_write_retry_no_duplicate(tmp_path: Path):
    """Test that once an operation commits, a retried request returns COMMITTED without re-execution."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings=settings,
        node_id="shard0-standalone",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
    )
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://standalone") as client:
            state = app.state.node
            state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # First write succeeds
            resp1 = await client.post(
                "/doc",
                json={"request_id": "req-once", "document": {"doc_id": 200, "val": 1}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert resp1.status_code == 201
            seq1 = resp1.json()["seq"]
            assert state.engine.doc_count() == 1

            # Duplicate retry returns original committed result
            resp2 = await client.post(
                "/doc",
                json={"request_id": "req-once", "document": {"doc_id": 200, "val": 2}},
                headers={CLIENT_KEY_HEADER: CLIENT_KEY},
            )
            assert resp2.status_code in (200, 201)
            assert resp2.json()["outcome"] == "COMMITTED"
            assert resp2.json()["seq"] == seq1
            assert state.engine.get(200)["val"] == 1  # Not overwritten
            assert state.engine.doc_count() == 1


@pytest.mark.asyncio
async def test_issue4_migration_snapshot_consistency(tmp_path: Path):
    """Verify migration snapshot uses engine.last_applied_seq and blocks if pending writes unresolved."""
    settings = _make_settings(tmp_path)
    app = build_app(
        settings=settings,
        node_id="shard0-primary",
        shard_id="shard0",
        role=NodeRole.PRIMARY,
    )
    async with LifespanManager(app):
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://node") as client:
            state = app.state.node
            state.ranges = [ShardRangeConfig(start=0, end=1000, ownership_epoch=1)]

            # Insert applied document at seq 1
            state.engine.apply(
                {
                    "seq": 1,
                    "term": 1,
                    "op": "insert",
                    "doc_id": 50,
                    "request_id": "r1",
                    "payload": {"doc_id": 50},
                }
            )
            state.wal.next_seq()
            state.wal.append(
                seq=1, term=1, op="insert", doc_id=50, request_id="r1", payload={"doc_id": 50}
            )

            # Append unapplied WAL entry at seq 2
            state.wal.next_seq()
            state.wal.append(
                seq=2, term=1, op="insert", doc_id=60, request_id="r2", payload={"doc_id": 60}
            )

            # Take snapshot
            body = {"start": 0, "end": 1000}
            b_bytes = json.dumps(body).encode("utf-8")
            resp = await client.post(
                "/internal/migration/snapshot", content=b_bytes, headers=_cluster_headers(b_bytes)
            )
            assert resp.status_code == 200
            data = resp.json()
            # Must return snapshot_seq == 1 (applied seq), NOT wal.last_seq == 2!
            assert data["snapshot_seq"] == 1
            assert len(data["documents"]) == 1
            assert data["documents"][0]["doc_id"] == 50


@pytest.mark.asyncio
async def test_issue5_migration_recovery_on_coordinator_restart(tmp_path: Path):
    """Verify coordinator startup rolls forward post-cutover migrations and aborts pre-cutover ones
    when RPCs succeed."""
    settings = _make_settings(tmp_path)
    map_mgr = ShardMapManager(tmp_path / "shard_map.json")
    default_map = create_default_3_shard_map()
    map_mgr.save(default_map)

    coord_init = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_init.replica_states[s_id] = ReplicaState.SYNCED

    # 1. Post-cutover migration in CUTOVER state
    mgr = MigrationManager(str(tmp_path), SECRET)
    rec_cutover = mgr.create_migration(
        from_shard="shard0",
        to_shard="shard1",
        start=0,
        end=100,
        coord=coord_init,
    )
    rec_cutover.state = MigrationState.CUTOVER
    rec_cutover.final_cutover_epoch = 10
    mgr.save_record(rec_cutover)

    # 2. Pre-cutover migration in CATCHUP state
    rec_pre = mgr.create_migration(
        from_shard="shard1",
        to_shard="shard2",
        start=4000000000000000000,
        end=4500000000000000000,
        coord=coord_init,
    )
    rec_pre.state = MigrationState.CATCHUP
    mgr.save_record(rec_pre)

    # Mock client where all recovery RPCs succeed
    async def ok_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    mock_client = AsyncClient(transport=httpx.MockTransport(ok_handler))

    # 3. Simulate coordinator restart
    coord_svc = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_svc.replica_states[s_id] = ReplicaState.SYNCED
    await coord_svc.resume_unfinished_migrations(http_client=mock_client)

    # Post-cutover migration must roll forward to DONE
    saved_cutover = coord_svc.migration_mgr.migrations[rec_cutover.migration_id]
    assert saved_cutover.state == MigrationState.DONE

    # Pre-cutover migration must abort cleanly
    saved_pre = coord_svc.migration_mgr.migrations[rec_pre.migration_id]
    assert saved_pre.state == MigrationState.ABORTED


@pytest.mark.asyncio
async def test_migration_recovery_rpc_failures_prevent_false_success(tmp_path: Path):
    """Regression test for Issue 3:
    Verify that if cutover, cleanup, unpause, or abort RPCs fail:
    - Post-cutover migration is NOT marked DONE; retained in CLEANUP with error.
    - Pre-cutover migration is NOT marked ABORTED; retained in pre-cutover state with error.
    - Subsequent retry when RPCs succeed completes the recovery idempotently.
    """
    settings = _make_settings(tmp_path)
    map_mgr = ShardMapManager(tmp_path / "shard_map.json")
    default_map = create_default_3_shard_map()
    map_mgr.save(default_map)

    coord_init = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_init.replica_states[s_id] = ReplicaState.SYNCED

    mgr = MigrationManager(str(tmp_path), SECRET)

    # 1. Post-cutover migration
    rec_post = mgr.create_migration(
        from_shard="shard0", to_shard="shard1", start=0, end=100, coord=coord_init
    )
    rec_post.state = MigrationState.CUTOVER
    rec_post.final_cutover_epoch = 10
    mgr.save_record(rec_post)

    # 2. Pre-cutover migration
    rec_pre = mgr.create_migration(
        from_shard="shard1",
        to_shard="shard2",
        start=4000000000000000000,
        end=4500000000000000000,
        coord=coord_init,
    )
    rec_pre.state = MigrationState.PAUSE
    mgr.save_record(rec_pre)

    # A mock client that fails cutover, cleanup, unpause, and abort RPCs
    async def fail_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "RPC connection failed"})

    fail_client = AsyncClient(transport=httpx.MockTransport(fail_handler))

    coord_svc = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_svc.replica_states[s_id] = ReplicaState.SYNCED

    # Recovery attempt with failing RPCs
    recovered = await coord_svc.resume_unfinished_migrations(http_client=fail_client)
    assert len(recovered) == 0  # Neither migration succeeded recovery!

    # Verify post-cutover migration is NOT marked DONE
    saved_post = coord_svc.migration_mgr.migrations[rec_post.migration_id]
    assert saved_post.state == MigrationState.CLEANUP
    assert saved_post.error is not None
    assert "failed" in saved_post.error

    # Verify pre-cutover migration is NOT marked ABORTED
    saved_pre = coord_svc.migration_mgr.migrations[rec_pre.migration_id]
    assert saved_pre.state != MigrationState.ABORTED
    assert saved_pre.state == MigrationState.PAUSE
    assert saved_pre.error is not None
    assert "failed" in saved_pre.error

    # 3. Now simulate subsequent recovery attempt where RPCs succeed
    async def ok_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok"})

    ok_client = AsyncClient(transport=httpx.MockTransport(ok_handler))
    recovered_retry = await coord_svc.resume_unfinished_migrations(http_client=ok_client)
    assert len(recovered_retry) == 2

    # Now they are safely marked DONE and ABORTED
    assert coord_svc.migration_mgr.migrations[rec_post.migration_id].state == MigrationState.DONE
    assert coord_svc.migration_mgr.migrations[rec_pre.migration_id].state == MigrationState.ABORTED


@pytest.mark.asyncio
async def test_issue6_scatter_gather_large_pagination(tmp_path: Path):
    """Verify scatter-gather retrieves candidates via internal pagination when skip + limit > 1000."""
    settings = _make_settings(tmp_path)
    map_mgr = ShardMapManager(tmp_path / "shard_map.json")
    default_map = create_default_3_shard_map()
    map_mgr.save(default_map)

    # Build mock shard responding with 1000 docs per chunk
    queries_made = []

    async def mock_handler(request: httpx.Request) -> httpx.Response:
        data = json.loads(request.content.decode("utf-8"))
        queries_made.append(data)
        skip = data.get("skip", 0)
        limit = data.get("limit", 1000)
        # Return docs numbered skip .. skip+limit (up to 1500 total in shard0)
        total_available = 1500
        count = max(0, min(limit, total_available - skip))
        docs = [{"doc_id": i, "val": i} for i in range(skip, skip + count)]
        return httpx.Response(200, json={"documents": docs, "total": total_available})

    mock_client = AsyncClient(transport=httpx.MockTransport(mock_handler))

    # Request skip=900, limit=300 (total candidates needed: 1200 > 1000)
    q = QueryFilter(
        filter={"val": {"$gte": 0}}, sort_field="doc_id", sort_dir="asc", skip=900, limit=300
    )
    resp = await execute_scatter_query(q, map_mgr, mock_client, settings)

    assert len(resp.documents) == 300
    assert resp.documents[0]["doc_id"] == 900
    assert resp.documents[-1]["doc_id"] == 1199

    # Verify that multiple chunked queries were executed to fetch beyond 1000
    assert len(queries_made) >= 2
    assert queries_made[0]["limit"] == 1000
    assert queries_made[1]["limit"] == 200
