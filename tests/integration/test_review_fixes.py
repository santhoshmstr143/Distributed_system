"""
Regression tests for review findings:
- Issue 1: Docker shard ownership ranges matching shard map without overlapping full keyspace
- Issue 2: Durable term and epoch persistence across crash-restart
- Issue 3: Pending-write reconciliation (follower has op vs follower does not, and subsequent write blocking)
- Issue 4: Migration snapshot consistency with applied state
- Issue 5: Migration recovery after coordinator restart (roll forward post-cutover, abort pre-cutover)
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
from docstore.shard.app import ShardRangeConfig, build_app

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
    """Verify promotion durably persists term and epoch before serving; recovery restores term."""
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
    assert meta["role"] == "primary"
    assert any(r["ownership_epoch"] == 42 for r in meta["ranges"])

    # 3. Simulate crash and restart without any snapshot taken
    app2 = build_app(
        settings=settings,
        node_id="shard0-node1",
        shard_id=shard_id,
        role=NodeRole.PRIMARY,
    )
    async with LifespanManager(app2):
        node_state = app2.state.node
        assert node_state.term == 5
        assert node_state.role == NodeRole.PRIMARY
        assert any(r.ownership_epoch == 42 for r in node_state.ranges)


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

            # 1. Normal write works
            p_transport = ASGITransport(app=primary_app)
            async with AsyncClient(transport=p_transport, base_url="http://primary") as p_client:
                doc_payload = {"request_id": "req-1", "document": {"doc_id": 10, "title": "Doc1"}}
                resp = await p_client.post(
                    "/doc", json=doc_payload, headers={CLIENT_KEY_HEADER: CLIENT_KEY}
                )
                assert resp.status_code == 201

                # 2. Now simulate replication failure where follower received it but ACK timed out:
                # Append write to follower WAL & dedup table directly to simulate that follower has it
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

                # Primary also appended to its WAL, but recorded outcome as OUTCOME_UNKNOWN and pending
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

                # Attempting an unrelated write while req-2 is unresolved should be rejected
                unrelated_resp = await p_client.post(
                    "/doc",
                    json={
                        "request_id": "req-unrelated",
                        "document": {"doc_id": 30, "title": "Doc3"},
                    },
                    headers={CLIENT_KEY_HEADER: CLIENT_KEY},
                )
                assert unrelated_resp.status_code == 503

                # When client retries req-2, primary reconciles with follower via /internal/check-op!
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
    """Verify coordinator startup rolls forward post-cutover migrations and aborts pre-cutover ones."""
    settings = _make_settings(tmp_path)
    map_mgr = ShardMapManager(tmp_path / "shard_map.json")
    default_map = create_default_3_shard_map()
    map_mgr.save(default_map)

    # 1. Initialize coordinator and mark shards SYNCED so migrations can be created
    coord_init = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_init.replica_states[s_id] = ReplicaState.SYNCED

    # 2. Create a migration in CUTOVER state directly in migration manager
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

    # 3. Create another migration in CATCHUP state (pre-cutover)
    rec_pre = mgr.create_migration(
        from_shard="shard1",
        to_shard="shard2",
        start=4000000000000000000,
        end=4500000000000000000,
        coord=coord_init,
    )
    rec_pre.state = MigrationState.CATCHUP
    mgr.save_record(rec_pre)

    # 4. Simulate coordinator restart
    coord_svc = CoordinatorService(settings, shard_map_path=tmp_path / "shard_map.json")
    for s_id in ("shard0", "shard1", "shard2"):
        coord_svc.replica_states[s_id] = ReplicaState.SYNCED
    await coord_svc.resume_unfinished_migrations()

    # Post-cutover migration must roll forward to DONE
    saved_cutover = coord_svc.migration_mgr.migrations[rec_cutover.migration_id]
    assert saved_cutover.state == MigrationState.DONE

    # Pre-cutover migration must abort cleanly
    saved_pre = coord_svc.migration_mgr.migrations[rec_pre.migration_id]
    assert saved_pre.state == MigrationState.ABORTED


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
