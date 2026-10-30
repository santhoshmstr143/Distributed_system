# Sharded-Docstore

A MongoDB-inspired, sharded JSON document store built as a university
distributed-systems project (Python 3.11, asyncio, FastAPI).

## Architecture

```
Clients
  └─► Router  (stateless, shard-key routing + scatter-gather)
        ├─► Shard 0: Primary  ↔  Follower
        ├─► Shard 1: Primary  ↔  Follower
        └─► Shard 2: Primary  ↔  Follower
               ▲
        Coordinator  (shard map, leases, epochs, failover)
```

**Key properties:**
- Range-partitioned by `doc_id` (non-negative 64-bit integer) across 3 shards
- Synchronous replication: writes ack'd only after fsync on **both** primary and follower WAL
- Three write outcomes: `COMMITTED` · `REJECTED` · `outcome_unknown`
- Automatic failover with epoch/term fencing; single-primary failure only
- Live key-range migration with snapshot copy + oplog catch-up

## Quickstart

```bash
# 1. Setup (once)
make setup
source .venv/bin/activate

# 2. Copy and fill in environment
cp .env.example .env
# Edit .env — set CLUSTER_SECRET, ADMIN_TOKEN, CLIENT_API_KEY

# 3. Run all tests
make test

# 4. Start the full cluster (Docker)
make up

# 5. Run the scripted demo
make demo
```

> **Note:** Docker Compose and the demo script are implemented in Module 11 and 14 respectively. See [CHECKLIST.md](CHECKLIST.md) for the milestone map.

## Running a single shard node (M1 milestone)

```bash
# After make setup
source .venv/bin/activate

# Option A: via Python module
python -m docstore.shard

# Option B: via uvicorn with factory
uvicorn "docstore.shard.app:create_default_app" --factory \
  --host 127.0.0.1 \
  --port 8200 \
  --env-file .env
```

## Development workflow

```bash
make lint        # ruff lint + format (auto-fix)
make typecheck   # mypy strict
make test-fast   # unit + integration tests
make check       # full gate: lint + types + security + audit + tests
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for branch naming and commit conventions.

## Module map

| Module | Status | Description |
|--------|--------|-------------|
| 0  | ✅ Done | Project setup & repo hygiene |
| 1  | ✅ Done | Contracts & common foundations |
| 2  | ✅ Done | Storage engine (WAL, snapshots, index) |
| 3  | ✅ Done | Basic shard node API |
| 4  | ⬜ | Static shard map & router |
| 5  | ⬜ | Query engine & scatter-gather |
| 6  | ⬜ | Coordinator, epochs, leases |
| 7  | ⬜ | Replication (primary → follower) |
| 8  | ⬜ | Failover & fencing |
| 9  | ⬜ | Live range migration |
| 10–16 | ⬜ | Client, Docker, hardening, benchmarks, demo, report, release |

## Limitations (documented honestly)

- Single-node failure only: coordinator is **not** fault-tolerant
- Writes unavailable after primary failure until the old primary rejoins and is `SYNCED`
- Single-host Docker Compose; no cross-machine clock comparison
- Shared-secret auth without replay protection (see `docs/security.md`)

## Security

Security rules are enforced at every layer. Before each commit:
```bash
make security   # bandit static analysis
make audit      # pip-audit for known CVEs
```

See [docs/security.md](docs/security.md) for the threat model.

## License

MIT — see [LICENSE](LICENSE).