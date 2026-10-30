# Project 30 — MongoDB-Inspired Sharded Document Store
## Master Checklist v4 — FINAL (Team 50)

**How to use:** tick `[x]` when done. Work top to bottom. Each module belongs to a **milestone (M1–M7)**; every milestone ends with a **gate**. Do not start the next milestone until the gate is fully ticked (runnable demo + passing tests).
🔖 **COMMIT** = Git commit and push (`feat:`, `test:`, `docs:`, `chore:`, `fix:`).
👤 **Owner** = Member 1 / 2 / 3 (see team split at the end). **All** = shared.

**This version is frozen after sign-off.** Changes need a team decision, not silent edits.

### What v4 adds over v3
- **Degraded mode after failover:** the promoted primary serves reads but rejects writes (`REPLICATION_UNAVAILABLE`) until the old primary rejoins and is `SYNCED`. Recovery is reported as two numbers: failover time and write-restoration time.
- **Explicit commit point:** an operation is committed once the follower has durably fsynced it; after a crash the follower's durable log decides.
- **Sequence history across failover:** committed positions are permanent; an uncommitted primary-only tail is discarded and its positions are reassigned under a higher `term`; operations are identified by `(term, seq)`; rejoin truncates the divergent tail.
- **Lease vs write timing:** `T + M < L − H` is validated at startup; writes need enough remaining lease; no `COMMITTED` after lease expiry. Replication timeout lowered to 1 s so writes are not stalled.

### What v3 added over v2
- Integer `doc_id` with half-open ranges; full-replace updates; precise sequence, idempotency, query, read/write and lease contracts.
- Safer WAL recovery (no silent skipping), WAL retention rules, bounded dedup table.
- Correct scatter-gather pagination (`skip + limit` per shard) and a total sort order.
- Two fencing concepts: `ownership_epoch` (per range) and `term` (per shard).
- Exact replication write path with an explicit **committed / rejected / unknown** outcome model, one-writer-at-a-time commit order, and uncommitted-tail handling.
- Failover promotion steps, safe old-primary rejoin, crash-point tests.
- Migration with a point-in-time snapshot at sequence S, idempotent replay, a defined **commit point**, and "incoming data is invisible until cutover".
- Benchmark, demo and report additions; stronger team integration rules.

---

## Milestone Map

| Milestone | Modules | Expected result (must be runnable) |
|---|---|---|
| **M1** | 0, 1, 2, 3 | Single shard node with durable CRUD that survives restart |
| **M2** | 4 | 3 shards + static shard map + router with shard-key routing |
| **M3** | 5 | Filter queries + scatter-gather merge |
| **M4** | 6, 7 | Coordinator (persistent map, epochs, leases) + primary→follower replication |
| **M5** | 8 | Primary crash → automatic failover → fencing |
| **M6** | 9 | Live range migration under write load |
| **M7** | 10–16 | Client, Docker, hardening, benchmarks, demo, report, release |

> ⚠️ **Schedule rule:** M5 and M6 are the hardest. Finish M4 by roughly the halfway point and start M5 immediately. Never leave failover or migration for the last days.

---

# MILESTONE M1 — Single-Node Durable Document Store

## Module 0 — Project Setup & Repo Hygiene  👤 All

- [x] Create GitHub repo (private until submission); add all teammates
- [x] Protect `main`; feature branches (`feat/storage`, `feat/router`, ...) and PRs; every PR reviewed by someone other than the author
- [x] Create Python 3.11 venv (`python -m venv .venv`)
- [x] `.gitignore` (`.venv/`, `__pycache__/`, `*.pyc`, `.env`, `data/`, `*.wal`, `*.snap`, `.pytest_cache/`, `.mypy_cache/`, `htmlcov/`, `results/raw/`, `*.log`, `.DS_Store`, IDE folders)
- [x] `.env.example` (placeholders only; **never commit real `.env`**): `CLUSTER_SECRET`, `ADMIN_TOKEN`, `CLIENT_API_KEY`, ports, heartbeat interval H, lease duration L, failure timeout D, data dir, `ENABLE_TEST_HOOKS=false`
- [x] `requirements.txt` with **exact pinned versions**: `fastapi`, `uvicorn`, `httpx`, `pydantic`, `pydantic-settings`, `sortedcontainers`, `numpy`, `matplotlib`
- [x] `requirements-dev.txt`: `pytest`, `pytest-asyncio`, `pytest-cov`, `ruff`, `mypy`, `bandit`, `pip-audit`
- [x] `pyproject.toml` (ruff, mypy, pytest config, markers: `unit`, `integration`, `fault`, `security`)
- [x] Folder structure:
  ```
  sharded-docstore/
  ├── src/docstore/
  │   ├── common/        (config, models, errors, logging, auth, http)
  │   ├── storage/       (wal, snapshot, engine, index)
  │   ├── query/         (validator, evaluator, ordering)
  │   ├── shard/         (shard node API, replication, migration endpoints)
  │   ├── coordinator/   (shard map, epochs, leases, failover, migration)
  │   ├── router/        (routing, scatter-gather, merge)
  │   └── client/        (CLI + Python API)
  ├── tests/             (unit/, integration/, fault/, security/)
  ├── bench/
  ├── scripts/           (demo, fault injection)
  ├── docker/
  ├── docs/              (contracts, architecture, consistency, report)
  ├── Makefile
  ├── README.md
  └── LICENSE
  ```
- [x] `Makefile` targets: `setup`, `lint`, `test`, `test-fast`, `demo`, `bench`, `clean`
- [x] Initial `README.md`; add `LICENSE`
- [x] Pre-commit hooks (ruff, trailing whitespace, detect-secrets / gitleaks)
- [ ] (Optional) GitHub Actions CI: lint + tests + `bandit` + `pip-audit` on every PR
- [x] `CONTRIBUTING.md` (branch naming, commit style, module owners)
- [x] 🔖 **COMMIT:** `chore: project scaffold, gitignore, requirements, tooling`

---

## Module 1 — Contracts & Common Foundations  👤 All (contracts), Member 1 (code)

### 1.1 Contracts document (write BEFORE parallel work starts)
`docs/contracts.md` must be agreed and signed off by all three members and merged to `main`. Any later change needs a PR approved by all three.

**Document IDs and ranges**
- [x] `doc_id` is a **non-negative 64-bit integer**, valid range `0 … 2^63 − 1`; reject strings, floats, negatives, and booleans (Python treats `True` as an int; validate strictly)
- [x] `doc_id` is unique and **immutable** after insertion
- [x] Shard ranges are **half-open `[start, end)`**; together they cover the whole key space `[0, 2^63)` with no gaps or overlaps
- [x] A document stays on its shard unless an explicit migration moves its range
- [x] Note in the docs: IDs above 2^53 can lose precision in some JSON clients (our Python client is fine)

**Update and delete semantics**
- [x] `PUT` is a **full replacement**; the body must contain the same `doc_id`; missing document → `NotFound`; `doc_id` cannot change
- [x] `DELETE` of a missing document → `NotFound` (but a retried delete with the same `request_id` returns the original success; see idempotency)
- [ ] (Stretch, only if time remains) `$set` / `$unset` partial updates

**Sequence numbers, terms, epochs**
- [x] Each **shard** keeps one monotonically increasing, gap-free sequence starting at 1, **not reset** by failover or epoch changes
- [x] **Committed** sequence positions are permanent: a committed `seq` never changes meaning. An **uncommitted** tail that exists only on a failed primary may be discarded; its `seq` values are then assigned to new operations by the new primary under a higher `term`
- [x] An operation is identified by the pair **(term, seq)**; within one term a `seq` maps to exactly one operation (log-matching rule)
- [x] The follower accepts only `last_seq + 1`; a gap triggers a catch-up request; a duplicate is handled idempotently
- [x] Two separate fencing values:
  - `ownership_epoch` — per **range**; bumped on failover (for every range of the affected shard) and on migration (for the migrated range)
  - `term` — per **shard**; bumped on every primary change; carried in every WAL record and replication message
- [x] Nodes persist their current `term` and per-range epochs durably (fsync) so a restart cannot forget them
- [x] Writes carrying a stale `ownership_epoch` or `term` are rejected

**Idempotency**
- [x] Every client write carries a unique `request_id` (generated by the client library); retries **reuse** it
- [x] `request_id` is stored in the WAL/oplog and preserved by the follower
- [x] A repeated `request_id` returns the **original result** and never executes twice (the lookup must also see in-flight/pending operations)
- [x] Dedup table survives restarts: rebuilt from WAL **and included in snapshots**; kept for a **bounded window** (e.g., last 10 minutes / 100k entries); retries older than the window are unsupported (documented)
- [x] Dedup table is part of snapshot resync to a follower and survives failover

**Query semantics**
- [x] A missing field never matches any operator
- [x] `null` matches only an explicit `null` value (not a missing field)
- [x] `$gt` / `$lt` compare only same-type scalars (number with number, string with string); booleans are **not** numbers; mismatched types → no match
- [x] Total sort order for sorting: type rank (missing < null < boolean < number < string), then value; arrays/objects are not sortable
- [x] `doc_id` is the deterministic tie-breaker for every sort
- [x] Global `skip` / `limit` are applied by the **router after merging** (see Module 5.3)

**Read and write policy**
- [x] **Primary reads:** served only by the current primary **holding a valid lease**; reflect all acknowledged writes
- [x] **Follower reads:** may be stale; must include `applied_seq` and `replication_lag`
- [x] **Write outcomes** are exactly three:
  - `REJECTED` — definitely not applied (validation, stale epoch/term, lease expired or too little lease left, duplicate key, range paused, not primary, **`REPLICATION_UNAVAILABLE`** when no follower is `SYNCED`); returned **before** the WAL append; safe to retry
  - `COMMITTED` — the commit point has passed (see below); acknowledged
  - `outcome_unknown` — WAL-appended on the primary, but no durable follower ack within the timeout. The write **may** later commit (if the follower catches up) or be discarded (if a failover happens first). The client must retry with the same `request_id`
- [x] While an uncommitted (pending) operation exists, new writes on that shard are blocked or rejected until it is reconciled
- [x] **Commit point:** an operation is committed when the **follower has durably fsynced its WAL record** (the primary has already fsynced before sending). If the follower fsyncs but the acknowledgement is lost, or the primary crashes before replying, the operation is **still committed**; the client's retry with the same `request_id` returns the original result. Records present **only** in the primary's WAL are uncommitted
- [x] **Replica states:** `SYNCED`, `CATCHING_UP`, `DOWN`. Writes are accepted only while the follower is `SYNCED`
- [x] Follower not `SYNCED` when a write arrives → `REPLICATION_UNAVAILABLE` (definite, nothing appended). Follower fails **after** the primary's WAL append → wait up to the replication timeout, then `outcome_unknown`. Never acknowledge an unreplicated write
- [x] **Degraded mode:** after a failover the promoted node serves reads but rejects writes until a follower is `SYNCED` again (synchronous replication is never relaxed)

**Leases and timing**
- [x] Parameters: heartbeat interval **H**, lease duration **L**, failure-detection timeout **D**, with **H < L < D**
- [x] Initial values: **H = 1 s, L = 3 s, D = 5 s**, safety margin **M = 0.5 s**, replication timeout **T = 1 s**
- [x] **Startup validation rules:** `H < L < D` and `T + M < L − H` (so a freshly renewed lease always leaves enough time for a write)
- [x] The primary computes lease expiry from the time it **sent** the heartbeat (conservative); the coordinator computes expiry from the time it **granted** the lease
- [x] A primary stops accepting writes and serving primary reads when its lease expires
- [x] Before starting a write the primary needs **remaining lease > T + M**; otherwise it briefly waits for renewal or rejects the write
- [x] If the lease expires before the client acknowledgement, return `outcome_unknown` (the retry resolves through dedup); never return `COMMITTED` after lease expiry. (Safety of acknowledged writes comes from the follower's stale-`term` rejection; the lease protects primary-read freshness)
- [x] Coordinator waits for the old lease to expire **plus the safety margin** before a new primary may write
- [x] A restarted node must register and obtain a fresh lease before serving as primary
- [x] Document clock assumptions: monotonic clocks for elapsed time only; single-host Docker Compose; no cross-machine clock comparison

**Defaults to freeze (adjust only by team decision)**
- [x] Max document size 1 MB; max request body 2 MB
- [x] Query `limit` default 100, max 1000; max `skip` 10,000; max filter depth 5; max clauses 50; max `$in` size 1000
- [x] Snapshot every N operations or T seconds; WAL retention cap (size/time) for lagging followers

**Interfaces**
- [x] Error model: error codes, HTTP status mapping, JSON error body
- [x] Auth: node secret for internal calls, separate admin token for admin endpoints, client API key for the router
- [x] Endpoint list for shard node (public vs `/internal/*`), coordinator, router: paths, request/response shapes
- [x] Response metadata: `served_by`, `term`, `ownership_epoch`, `applied_seq`, `replication_lag`

### 1.2 Common code (`common/`)
- [x] `config.py` — env settings with validated defaults (pydantic-settings); reject violations of `H < L < D` and `T + M < L − H` at startup
- [x] `models.py` — `Document`, `WriteRequest` (with `request_id`), `QueryRequest`, `ShardMapEntry`, `OplogEntry`, `Heartbeat`, `LeaseGrant`
- [x] `errors.py` — typed exceptions + HTTP mappings: `StaleEpoch`, `StaleTerm`, `NotPrimary`, `LeaseExpired`, `RangePaused`, `InvalidQuery`, `DuplicateKey`, `NotFound`, `OutOfRange`, `OutcomeUnknown`, `ReplicationUnavailable`
- [x] `logging.py` — structured JSON logs (node id, shard id, term); never log secrets or full documents
- [x] `auth.py` — shared-secret/HMAC header for inter-node RPC; `hmac.compare_digest`
- [x] `http.py` — shared `httpx.AsyncClient`: timeouts, bounded retries, connection limits
- [x] Body size limit and max document size enforced
- [x] **Tests:** models validation (including bool/float/negative `doc_id`), config validation, auth accept/reject (56/56 passing)
- [x] 🔖 **COMMIT:** `feat(common): contracts, config, models, errors, logging, auth`

---

## Module 2 — Storage Engine (`storage/`)  👤 Member 1

### 2.1 Write-Ahead Log
- [x] JSON-lines WAL: `seq`, `term`, `op`, `doc_id`, `request_id`, `payload`, per-record CRC32/checksum
- [x] Append + `flush()` + `os.fsync()` on every acknowledged write; fsync the directory when creating/rotating files
- [x] Gap-free monotonic seq numbers
- [x] Replay on startup
- [x] **Torn tail:** if the **final record is incomplete** (no terminating newline / unparseable at end of file), truncate it safely and continue
- [x] **Corruption:** if a complete, newline-terminated record has a bad checksum, **recovery fails** and the node refuses to serve until repaired (e.g., resync from its peer). **Never silently skip a record and continue**
- [x] **`truncate_after(seq)`** primitive (fsynced) to discard a divergent uncommitted tail; afterwards memory, the dedup table and next-seq are rebuilt. Snapshots contain only committed, applied operations, so truncation never invalidates a snapshot
- [x] **Tests:** append, replay, torn write, corrupted middle record (node refuses to start), corrupted-but-complete last record, truncate-after-seq (including a crash during truncation)

### 2.2 Snapshots and compaction
- [x] Periodic snapshot with `last_applied_seq`, `term`, per-range epochs, **and the dedup table**
- [x] Write temp → fsync → atomic rename → fsync directory
- [x] Recovery = latest snapshot + WAL after `last_applied_seq`
- [x] Compact the WAL **only after** a successful snapshot, and **never delete entries still needed** by (1) follower catch-up or (2) an active migration
- [x] Track the follower's acknowledged seq; enforce the **retention cap**; beyond it, compact anyway and use **full snapshot resync** for the follower
- [x] **Tests:** crash between snapshot and WAL truncate; recovery with retained WAL; retention-cap fallback

### 2.3 In-memory engine and index
- [x] Document dict + `SortedDict` ordered index
- [x] `insert` (fail on duplicate), `update` (full replace), `delete`, `get`
- [x] Range scan `[start, end)`
- [x] **Point-in-time range snapshot** (copy of a range taken under the write lock) for migration
- [x] Per-document version / last-modified seq
- [x] **Apply is idempotent by seq:** re-applying the same seq changes nothing
- [x] Migration replay mode is idempotent: insert → upsert, delete → no-op if missing
- [x] Concurrency: one writer; **validate-then-commit holds a per-shard write lock** so pending operations are visible to validation (prevents two concurrent inserts of the same id both passing)
- [x] **Tests:** CRUD, ordering, range scan, idempotent re-apply, point-in-time snapshot isolation (33/33 passing)
- [x] 🔖 **COMMIT:** `feat(storage): WAL with checksums, snapshots, ordered index, recovery`

---

## Module 3 — Basic Shard Node (single node, no replication yet)  👤 Member 1

- [x] FastAPI app with identity: `node_id`, `shard_id`, `role`, `term`, owned ranges with epochs (from config for now)
- [x] Public endpoints: `POST /doc`, `PUT /doc/{id}`, `DELETE /doc/{id}`, `GET /doc/{id}`, `GET /health`, `GET /status`; internal endpoints under `/internal/*`
- [x] Inter-node auth on every request
- [x] Reject keys outside owned ranges (`OutOfRange`)
- [x] Write checks structured so **role, term/epoch and lease checks plug in later** (stubbed "always valid" for now)
- [x] Node starts in a **recovering** state: not ready until WAL replay succeeds; a failed recovery stays unready
- [x] Graceful shutdown (flush + snapshot)
- [x] Pydantic validation on every body/path param; body size limit
- [x] No stack traces in error responses (details only in logs)
- [x] `request_id` dedup wired in (retry returns original result)
- [x] **Tests:** CRUD over HTTP, duplicate insert, not-found, restart persistence, retry with same `request_id`, bad auth, oversized body, boundary keys `start` and `end−1` / `end`
- [x] 🔖 **COMMIT:** `feat(shard): shard node API with durable CRUD`

### ✅ M1 GATE — Single node works
- [x] Start one shard node → insert / get / update / delete via HTTP
- [x] Kill and restart → all data present
- [x] Corrupted-WAL test shows the node refusing to serve
- [x] All M1 tests green; `ruff` + `mypy` clean
- [x] README has a "run one node" section

---

# MILESTONE M2 — Multiple Shards + Shard-Key Routing (Static Map)

## Module 4 — Static Shard Map & Basic Router  👤 Member 2 (map), Member 3 (router)

### 4.1 Shard map (static file version)
- [x] Shard map file: `range [start,end) → {shard_id, primary, follower, ownership_epoch}` plus global `map_version`
- [x] In the static stage `follower` may be `null`; every range has exactly one primary
- [x] Validation: contiguous, non-overlapping, full coverage of `[0, 2^63)`; reject bad maps
- [x] Atomic file update (temp → fsync → rename) + append-only epoch history log
- [x] Bootstrap config for 3 shards
- [x] Each shard node rejects documents outside its configured ranges
- [x] **Tests:** coverage/overlap/gap validation, boundary lookups, atomic update, reload (16/16 passing)

### 4.2 Router (routing only for now)
- [x] Stateless FastAPI router; loads and **caches the shard map**
- [x] Client API: `insert`, `update`, `delete`, `get`
- [x] Direct routing by `doc_id` to the owning shard primary
- [x] Range lookup helper: which shards overlap `[start, end)`
- [x] Bounded retries; refresh the map once on `OutOfRange`/`StaleEpoch`; no infinite loops
- [x] Pass `request_id` through; retries reuse it
- [x] **Client API key** required on router endpoints (constant-time compare); router binds to `127.0.0.1` by default
- [x] Internal shard endpoints are **not** reachable through the router
- [x] Max concurrent requests (basic DoS protection)
- [x] **Tests:** docs land on the right shards, boundary keys, stale-map refresh, retry idempotency, missing/invalid API key rejected (8/8 passing)
- [x] 🔖 **COMMIT:** `feat(router): static shard map and shard-key routing`

### ✅ M2 GATE — Minimum working system
- [x] 3 shard nodes + 1 router running
- [x] Insert 1,000 docs; verify distribution across shards
- [x] Get / update / delete through the router
- [x] Restart all nodes → data persists, routing correct
- [x] M2 tests green; lint clean

---

# MILESTONE M3 — Filter Queries & Scatter-Gather

## Module 5 — Query Engine & Scatter-Gather  👤 Member 3

### 5.1 Query engine (`query/`)
- [x] Grammar: `{field: value}`, `{field: {$eq,$gt,$lt,$in}}`, `{$and: [...]}`, `{$or: [...]}`
- [x] **Validator:** whitelist operators, max depth, max clauses, max `$in` size, reject unknown `$` operators
- [x] **Evaluator:** pure Python; **no `eval` / `exec` / `pickle` / user regex**
- [x] Dotted-path access for nested fields; missing-field and null rules from the contracts
- [x] Type-safe comparison per the contracts (bool is not a number)
- [x] `sort` using the total order from the contracts, `doc_id` tie-break; `limit` / `skip` caps
- [x] Shard-key detection helper: does the filter pin `doc_id` (eq / `$in` / range)?
- [x] **Tests:** every operator, nested and/or, missing/null/type-mismatch, sort order across mixed types, malicious/oversized filters (23/23 passing)

### 5.2 Shard `/query` endpoint
- [x] `POST /query` using the evaluator; uses the ordered index when `doc_id` is constrained
- [x] Returns **only documents in ranges the shard currently owns** (excludes migrating-out or not-yet-cut-over data)
- [x] **Tests:** single-shard queries, ownership filtering

### 5.3 Router scatter-gather
- [x] Shard-key filters → direct to one shard (`$in` grouped per shard; range → only overlapping shards)
- [x] Non-key filters → parallel `asyncio.gather` to all shards with a per-shard timeout
- [x] **Merge (correct pagination):**
  1. Ask each shard for up to `skip + limit` sorted candidates with **local skip = 0**
  2. Merge sorted streams (e.g., `heapq.merge`)
  3. De-duplicate by `doc_id`
  4. Apply global `skip`, then global `limit`
  5. `doc_id` is the tie-breaker
- [x] Router drops any result whose `doc_id` is outside the responding shard's range per the router's map
- [x] **Partial-failure policy:** if any required shard fails or times out → **fail the whole query**, return an error naming the unavailable shard, allow one bounded retry; never return silently incomplete results
- [x] `read_mode` plumbing (`primary` default; `follower` accepted once followers exist)
- [x] **Tests:** global pagination correctness over 3 shards (skip crossing shard boundaries), tie-breaking, dedup, timeouts, partial failure (6/6 passing)
- [x] 🔖 **COMMIT:** `feat(query): safe filter engine and router scatter-gather`

### ✅ M3 GATE — Queries work
- [x] Key lookup and shard-key query hit exactly one shard (verify via logs/metrics)
- [x] Non-key filter hits all shards; merged, sorted, paginated, de-duplicated results match a single-node reference
- [x] Malicious/oversized filters rejected cleanly
- [x] M3 tests green; lint clean

---

# MILESTONE M4 — Coordinator + Replication

## Module 6 — Coordinator, Persistent Shard Map, Leases  👤 Member 2

- [x] FastAPI coordinator: `GET /shardmap`, `POST /register`, `POST /heartbeat`, admin endpoints
- [x] Coordinator **owns** the persistent shard map (reuse Module 4.1 code); routers and nodes fetch from it
- [x] Two version concepts: `map_version` (whole map) and per-range `ownership_epoch`; on failover bump the epoch of **every range of the affected shard** and the shard `term`; on migration bump the migrated range's epoch
- [x] Nodes register and send heartbeats every H; coordinator tracks last-seen (monotonic clock)
- [x] **Leases:** granted to each primary, renewed via heartbeats; coordinator records grant and expiry; primary stops writes **and primary reads** after expiry
- [x] Coordinator **never** lets a replacement primary write while the old lease may still be valid (expiry + safety margin)
- [x] A restarted node must register and get a fresh lease; it never resumes as primary from its old state
- [x] Coordinator tracks each shard's replica state (`SYNCED` / `CATCHING_UP` / `DOWN`, reported via heartbeats); a shard with no `SYNCED` follower is **degraded**, and this is visible in status
- [x] Wire the Module 3 stubs to real checks: role, term, `ownership_epoch`, lease validity
- [x] Admin endpoints protected by a **separate admin token**
- [x] Router uses `map_version` to detect staleness
- [x] **Tests:** register, heartbeat, lease renewal/expiry, write rejected after expiry, restarted node needs a new lease, admin-token enforcement, epoch bump rules for multi-range shards, degraded-shard status
- [x] 🔖 **COMMIT:** `feat(coordinator): persistent shard map, epochs, heartbeats, leases`

---

## Module 7 — Replication (primary → follower)  👤 Member 2

**Write path**
1. [x] Client sends a write with a unique `request_id`
2. [x] Primary takes the per-shard write lock; checks role, lease (**remaining > T + M**), `term`, `ownership_epoch`, range, dedup, **follower is `SYNCED`** (else `REPLICATION_UNAVAILABLE`), and validity (duplicate key / not found)
3. [x] Primary assigns the next sequence number
4. [x] Primary appends to its WAL and fsyncs
5. [x] Primary sends the operation (with `seq`, `term`, `request_id`) to the follower
6. [x] Follower validates `term` and requires `seq == last_seq + 1`
7. [x] Follower appends to its WAL and fsyncs
8. [x] Follower sends a durable acknowledgement
9. [x] Primary applies to its in-memory state
10. [x] If the lease is still valid, the primary acknowledges the client (`COMMITTED`); if it expired meanwhile, return `outcome_unknown`

**Rules**
- [x] Never acknowledge the client before durable replication
- [x] **Commit point:** committed = follower has durably fsynced the record. A lost follower ack or a primary crash before the client reply does not change that; after a crash the **follower's durable log decides** what is committed
- [x] **Follower states:** `SYNCED` (durably acked up to the primary's last seq, stream healthy), `CATCHING_UP`, `DOWN`. A follower leaves `SYNCED` on timeout, disconnect, or term change; a rejoining node goes `CATCHING_UP` → `SYNCED`; the primary reports the state to the coordinator via heartbeats
- [x] Commit strictly in sequence order; the prototype processes **one write at a time per shard** (document the throughput cost of two fsyncs per write; group commit is a stretch goal)
- [x] Follower applies to memory **asynchronously** after the durable append
- [x] Both nodes preserve `request_id` and `seq`
- [x] Follower detects gaps and duplicates; requests catch-up from `last_seq`
- [x] Catch-up from the primary's WAL; **full snapshot resync** (including dedup table, term, epochs) when the WAL was compacted past the needed seq
- [x] Primary tracks `follower_acked_seq`
- [x] **Follower not `SYNCED` when a write arrives:** reject with `REPLICATION_UNAVAILABLE` before any WAL append (definite outcome; safe to retry)
- [x] **Follower fails during a write (after the primary's WAL append):** wait up to T, then return `outcome_unknown`; keep the pending operation and block further writes until it is reconciled; never acknowledge an unreplicated write
- [x] Retried `request_id` for a pending operation waits for / returns its eventual result
- [x] A pending (uncommitted) operation is applied to memory **only once committed**. After a restart a node does not apply an unconfirmed tail: it resolves it with its peer/coordinator. Entries the follower durably holds are committed (apply them); entries only the primary holds are re-replicated if it is still primary, or truncated if a failover has happened
- [x] Follower rejects replication messages with a stale `term`
- [x] Replication lag (ops + ms) on `/status`
- [x] Follower serves reads tagged `served_by=follower`, `applied_seq`, `replication_lag`; rejects direct client writes (`NotPrimary`)
- [x] Router supports `read_mode=follower`; if a shard has no `SYNCED` follower, the router falls back to the primary and tags the response `served_by=primary`
- [x] Router retries `REPLICATION_UNAVAILABLE` with bounded backoff, then surfaces the error

**Tests**
- [x] Follower restart catch-up, gap detection, duplicate delivery, lag measurement
- [x] Follower down → `outcome_unknown`, writes blocked, then resume after catch-up
- [x] Primary crashes after its WAL append but before follower ack
- [x] Follower stores the operation but the ack is lost; client retries with the same `request_id`; no double apply
- [x] Primary and follower recover at different sequence positions
- [x] Two concurrent inserts of the same `doc_id` → exactly one succeeds
- [x] Follower fsynced but its ack was lost, then the primary crashes → after failover the operation is committed and the retry returns the original result
- [x] Operation present only in the old primary's WAL → discarded after failover; the retry executes exactly once
- [x] Write while the follower is not `SYNCED` → `REPLICATION_UNAVAILABLE`, nothing appended to the WAL
- [x] Write rejected when remaining lease ≤ T + M; lease expiring before the ack → `outcome_unknown`, never `COMMITTED`
- [x] No acknowledged write missing from the follower WAL
- [x] 🔖 **COMMIT:** `feat(replication): sync-durable oplog replication, catch-up, lag metrics`

### ✅ M4 GATE — Replicated cluster
- [x] 3 shards × 2 nodes + coordinator + router running
- [x] Every acknowledged write is on both WALs (verified by a script)
- [x] Follower read shows lag vs a primary read
- [x] Follower down → writes behave exactly as the contracts say
- [x] M4 tests green; lint clean
- [x] ⏱ Timeline check: M4 done by roughly the halfway point

---

# MILESTONE M5 — Failover & Fencing

## Module 8 — Failure Detection, Failover & Fencing  👤 Member 2 (lead), Member 1 (rejoin/resync)

**Promotion procedure**
1. [x] Detect primary failure (no heartbeat for D)
2. [x] Verify the follower is alive and report its highest durable seq
3. [x] Follower finishes applying all valid durable WAL operations
4. [x] Wait until the old primary's lease has expired **plus the safety margin**
5. [x] Increment shard `term` and the `ownership_epoch` of every range of that shard
6. [x] Persist the shard map and epoch history durably
7. [x] Promote the follower (it persists the new term before serving)
8. [x] Allow writes only after the new ownership and a valid lease are in place
9. [x] Router refreshes on error and by periodic poll to the new `map_version`

**Old primary / rejoin**
- [x] A returning primary **never** becomes primary from its previous state
- [x] It rejects writes carrying stale ownership information and demotes itself to follower
- [x] It finds the **divergence point** against the new primary's log using `(term, seq)` (the highest `seq` where both logs carry the same term), **truncates its own tail after that point**, rebuilds memory and the dedup table, and catches up; if that is impractical it falls back to a full snapshot resync
- [x] Its state goes `CATCHING_UP` → `SYNCED`; only then does the shard leave degraded mode
- [x] It resumes follower duty only after a successful resync

**Degraded mode after promotion** (two replicas only)
- [x] The promoted node serves reads immediately (primary reads need a valid lease)
- [x] Until a follower is `SYNCED`, writes are rejected with `REPLICATION_UNAVAILABLE`; synchronous replication is **not** relaxed (no single-node acknowledgements)
- [x] The returning old primary rejoins as a follower, resyncs, and becomes `SYNCED`; the shard then leaves degraded mode and accepts writes
- [x] Clients retry rejected writes using the same `request_id`
- [x] Restart of the failed node is automated for the demo and benchmarks (script or Docker restart policy) with a configurable delay
- [x] No migration on a degraded shard

**Sequence history across failover**
- [x] Committed positions are permanent and gap-free; the new primary continues from its own last committed `seq`
- [x] An uncommitted tail that exists only on the failed primary is discarded; those `seq` values are reassigned under the **higher `term`**, so `(term, seq)` stays unique
- [x] The new primary does not need to know about the discarded tail; the rejoining node reconciles it (divergence point above)
- [x] `request_id`s in a discarded tail count as never executed; a client retry runs them once under the new term

**Other**
- [x] The follower's stale-`term` rejection is the fence that guarantees no acknowledged write is lost; the lease protects primary-read freshness (document both)
- [x] Timestamps recorded for **two metrics**: **failover time** (crash → detected → promoted → first successful primary read) and **write-restoration time** (crash → … → old primary restarted → `SYNCED` → first successful write; includes the configured restart delay). Report both; the report notes that this refines the proposal's single "first successful write" metric
- [x] Scope documented: **single-node (primary) failure only; coordinator is not fault-tolerant**
- [x] If the only remaining replica of a shard also fails, that shard is unavailable; documented limitation
- [x] **Test-only crash-point hooks** (named points; triggered by `ENABLE_TEST_HOOKS=true`; disabled and rejected in non-test config)

**Fault tests**
- [x] SIGKILL the primary at each point: (1) before WAL append, (2) after primary fsync, (3) after follower fsync but before ack, (4) after follower ack but before client ack, (5) after client ack
- [x] No acknowledged write lost; retrying the same `request_id` never duplicates
- [x] Stale primary rejected; double failover blocked; old primary rejoins as follower and resyncs
- [x] Crash after follower fsync but before follower ack → committed after failover; retry returns the original result
- [x] Crash before follower fsync → tail discarded; retry executes exactly once
- [x] While degraded: reads succeed, writes get `REPLICATION_UNAVAILABLE`; after rejoin and resync, writes succeed and the old primary's divergent tail is truncated
- [x] 🔖 **COMMIT:** `feat(failover): heartbeat detection, epoch-bumped promotion, fencing, rejoin`

### ✅ M5 GATE — Failover works
- [x] Continuous reader and writer running; SIGKILL a primary
- [x] New primary serves reads within the target window; term and epochs incremented
- [x] Writes are rejected with `REPLICATION_UNAVAILABLE` (never falsely acknowledged) while the shard is degraded
- [x] Old primary restarted → fenced, demoted, resynced, `SYNCED`; writes resume
- [x] Verification script: every acked `request_id` is present exactly once
- [x] Repeat 5+ times with no data loss
- [x] M5 tests green; lint clean

---

# MILESTONE M6 — Live Range Migration

## Module 9 — Live Range Migration  👤 Member 3 (lead), Member 2 (coordinator side)

**Rules**
- [x] Admin API: `POST /admin/migrate {range, from_shard, to_shard}` (admin token)
- [x] Only **one active migration per key range** at a time
- [x] No migration starts (or continues) on a shard that is mid-failover or degraded (primary without a `SYNCED` follower); a failover during a migration **aborts** it
- [x] The source stays the authoritative owner until cutover
- [x] **Incoming data on the destination is invisible** (not served, not returned by queries) until cutover

**Persisted state machine** (stored by the coordinator)
`PLANNED → SNAPSHOT_COPY → CATCHUP → PAUSE → FINAL_CATCHUP → VERIFY → CUTOVER → CLEANUP → DONE` (`ABORTED` is the alternative terminal state)
- [x] Persist: migration ID, source, destination, key range, snapshot start seq **S**, last copied doc ID, last caught-up seq, current state, final cutover epoch

**Phases**
- [x] **SNAPSHOT_COPY:** take a **point-in-time copy of the range at source seq S** under the write lock; record S; stream batches from the frozen copy; the source keeps accepting writes
- [x] Destination ingests through its **normal replicated write path** (its own seq numbers; source seq tracked separately as the migration cursor), so its follower is covered
- [x] **CATCHUP:** read source oplog entries with seq > S for the range; replay **in order** and **idempotently** (insert → upsert, delete → no-op if missing); track last applied source seq; repeat until the delta is small; source retains WAL from S
- [x] **PAUSE:** pause writes for that range only (`RangePaused`); reads continue from the source; wait for in-flight writes; record final source seq **H**
- [x] **FINAL_CATCHUP:** replay everything through H; wait until the destination and its follower have durably applied it
- [x] **VERIFY:** compare document counts and a **deterministic SHA-256** checksum (documents sorted by `doc_id`, canonical JSON with sorted keys); confirm no missing or duplicate docs
- [x] **CUTOVER (commit point):** atomically persist the new shard map with a **bumped `ownership_epoch`**; this durable write is the **point of no return**
  - [x] Before the commit point → abort is allowed (destination discards the incoming copy, source unpauses)
  - [x] After the commit point → always roll forward
  - [x] The source stays paused until the coordinator decides; on restart it asks the coordinator for the migration state; a **maximum pause time** triggers an abort if still before the commit point
  - [x] After cutover the source rejects writes and queries for that range; routers refresh
- [x] **CLEANUP:** delete old source copies **only after** the new ownership is durable; source then drops the range from its owned set

**Other**
- [x] Router retries writes to a paused range with short backoff (bounded); no client error for brief pauses
- [x] Reads and queries stay correct during migration (ownership filtering plus dedup)
- [x] Metrics: docs copied, catch-up ops replayed, write-pause duration
- [x] Crash-safe: coordinator / source / destination restart in any phase → resume or abort cleanly

**Tests**
- [x] Insert, update, and delete during snapshot copy (no resurrected or stale documents)
- [x] Restart source, destination, coordinator mid-migration (each phase)
- [x] Abort before cutover; crash right after the commit point
- [x] Old shard rejects writes after cutover; destination follower holds the migrated range
- [x] Two concurrent migrations on the same range → second rejected
- [x] Counts and checksums match after migration
- [x] 🔖 **COMMIT:** `feat(migration): live range migration with catch-up, pause, epoch cutover`

### ✅ M6 GATE — Migration works
- [x] Migrate a range while the write workload continues
- [x] Count + checksum match before/after
- [x] Write pause brief and measured
- [x] Router and shards agree on the new ownership; old shard rejects the migrated range
- [x] Crash-in-each-phase tests green
- [x] M6 tests green; lint clean

---

# MILESTONE M7 — Product, Evaluation & Release

## Module 10 — Client CLI / API  👤 All

- [x] Python client library wrapping router calls: generates `request_id`, retries transient errors, `outcome_unknown` and `REPLICATION_UNAVAILABLE` (with backoff) using the **same** `request_id`
- [x] CLI (`argparse`/`typer`): `insert`, `get`, `update`, `delete`, `query`, `status`, `shardmap`, `migrate`, `bulk-load`
- [x] `--read-mode primary|follower`; output shows `served_by`, lag
- [x] Pretty JSON; non-zero exit codes on errors; never print secrets/tokens (API key from env)
- [x] Usage examples in README
- [x] **Tests:** CLI happy paths and error exit codes
- [x] 🔖 **COMMIT:** `feat(client): CLI and Python client library`

---

## Module 11 — Deployment (Docker Compose)  👤 All (Member 1 leads)

- [x] One `Dockerfile`: slim, **pinned** base image; multi-stage if useful; non-root user; no secrets baked in; `.dockerignore`
- [x] `docker-compose.yml`: 1 coordinator, 1–2 routers, 3 shards × 2 nodes (~9 services)
- [x] Named volumes per node (state survives container restarts)
- [x] Secrets via `.env` / `env_file` only; `ENABLE_TEST_HOOKS=false` by default
- [x] Internal Docker network; expose only the router (and coordinator admin if needed) to the host, bound to `127.0.0.1`
- [x] Health checks + `depends_on`; CPU/memory limits
- [x] `make up`, `make down`, `make logs`, `make reset`
- [x] Optional: `tc netem` / injected delay profile
- [x] **Tests:** compose cluster passes a smoke test (insert, get, query)
- [x] 🔖 **COMMIT:** `feat(deploy): Dockerfile and compose cluster`

---

## Module 12 — Cross-System Tests & Security Hardening  👤 All

*(Module-level tests already exist; this closes gaps and does the final security pass.)*

### 12.1 Test gaps
- [x] End-to-end fault tests: network-partition simulation, follower down during failover, coordinator restart, torn WAL on a real node
- [x] Consistency tests: no acked write lost after failover; primary reads see all acked writes; follower reads tagged stale; writes rejected (not lost, not falsely acknowledged) while degraded
- [x] Security tests: bad auth (node secret, admin token, client key), oversized body, malicious filter, out-of-range key, non-integer `doc_id`, test hooks unreachable when disabled
- [x] Coverage ≥ ~80% on core modules
- [x] `make test` runs all; `make test-fast` runs unit + integration

### 12.2 Security pass
- [x] `ruff check` and `mypy` clean on `src/`
- [x] `bandit -r src/` — no high/medium issues
- [x] `pip-audit -r requirements.txt` — no known vulnerable dependencies
- [x] `gitleaks` / `detect-secrets` — no secrets in history
- [x] No `eval`, `exec`, `pickle`, `yaml.load`, `shell=True`, `os.system` anywhere
- [x] `doc_id` never used in file paths (it is an integer; verify nothing builds paths from request data)
- [x] All internal endpoints authenticated; admin endpoints use the separate admin token; router requires the client API key
- [x] Caps enforced: body size, document size, filter depth/clauses, `$in`, `limit`, `skip`
- [x] Timeouts on every outbound call; bounded retries
- [x] Errors don't leak internals; logs have no secrets
- [x] Data dir permissions `0700`; `.env` restricted
- [x] Dependencies and Docker base image pinned
- [x] `docs/security.md`: threat model and **honest limitations** (single coordinator, no TLS inside the demo network, shared-secret auth without replay protection, single-host clock assumption)
- [x] (Optional) TLS or HMAC-with-timestamp between nodes, or listed as future work
- [x] 🔖 **COMMIT:** `test+chore(security): cross-system tests, static analysis, dependency audit, hardening`

---

## Module 13 — Benchmarking & Evaluation (`bench/`)  👤 All (Member 3 leads load generator)

- [x] Async load generator (asyncio + httpx): 100k docs, 70/30 read/write, configurable
- [x] Fixed random seed; document hardware and environment
- [x] **Query latency** p50/p95/p99: single-shard baseline, direct shard-key routing, scatter-gather, primary vs follower reads; compare **1, 2, 3 shards**
- [x] **Throughput** (ops/s), **error rate**, **timeout rate** under the configured workload
- [x] **Replication lag:** ops + ms under light and heavy load
- [x] **Recovery time (two numbers):** *failover time* (crash → detect → promote → first successful read) and *write-restoration time* (crash → … → old primary restarted and `SYNCED` → first successful write); **≥ 5 independent runs** per setting; report mean and spread; report (H, L, D, T, M) and the restart delay used. When sweeping the timeout, scale H, L and T so that **H < L < D** and **T + M < L − H** always hold
- [x] **Migration:** docs moved, catch-up ops, write-pause duration vs range size (e.g., 5k / 20k / 50k docs)
- [x] **Integrity:** counts + checksums before/after migration and failover
- [x] Raw results in `results/` (CSV/JSON); plots in `docs/figures/`
- [x] Warm-up excluded; multiple runs; mean ± spread
- [x] `make bench` reproduces everything
- [x] 🔖 **COMMIT:** `feat(bench): load generator, metrics, plots`

---

## Module 14 — Scripted Demo  👤 All

- [x] `make demo` runs the full story with clear printed steps
- [x] Step 1: start the cluster, load data across 3 shards, show the shard map
- [x] Step 2: key lookup (direct) vs filter query (scatter-gather) with timings
- [x] Step 3: start continuous read and write workloads; **record every acknowledged write by `request_id`** for later verification
- [x] Step 4: **SIGKILL a primary** → detection and promotion, new term/epoch; show that **reads continue** and that writes are **temporarily rejected** (`REPLICATION_UNAVAILABLE`) because the new primary has no follower
- [x] Step 5: **restart the old primary** → it is **fenced** (rejects writes), resyncs, rejoins as follower, becomes `SYNCED`; writes **resume**
- [x] Verify: every acked write present, none duplicated, old primary could not accept writes, new primary serves requests
- [x] Step 6: **migrate a key range** while writes continue → pause duration + integrity check
- [x] Step 7: **follower read lags** behind a primary read (tagged stale)
- [x] Step 8: print a metrics summary
- [x] Record a backup screen recording; rehearse at least twice on a clean machine
- [x] 🔖 **COMMIT:** `feat(demo): scripted end-to-end demo`

---

## Module 15 — Documentation & Final Report  👤 All

- [x] `README.md`: overview, architecture diagram, quickstart, CLI examples, how to run tests / bench / demo
- [x] `docs/architecture.md`: components, data path, control path, shard map format, WAL format
- [x] `docs/consistency.md`: write path, **commit point**, outcome model (committed / rejected / unknown), primary vs follower reads, lease/epoch/term fencing argument (why the follower's term check, not the lease, protects acknowledged writes), `(term, seq)` log matching, **degraded mode** and the alternatives considered (relaxed durability, a third replica) with why they were not chosen. Distinguish **durability, replication, consistency, availability**
- [x] `docs/failover.md` and `docs/migration.md`: state machines, commit point, failure cases
- [x] `docs/api.md`: endpoints, examples, error codes
- [x] Final LaTeX report covers: sequence numbers and replication; write-acknowledgement protocol; lease expiry and ownership fencing; snapshot consistency and oplog catch-up in migration; failure scenarios and recovery behaviour; evaluation with plots; **limitations (two-replica deployment means writes are unavailable after a primary failure until the old primary rejoins; single coordinator — do not claim full fault tolerance)**; future work
- [x] Docstrings on public functions; type hints throughout
- [x] Report numbers match saved `results/` files
- [x] 🔖 **COMMIT:** `docs: architecture, consistency analysis, API, report`

---

## Module 16 — Final Release  👤 All

- [ ] Fresh clone on a clean machine → `make setup && make test && make demo` works
- [ ] All tests green; ruff, mypy, bandit, pip-audit clean
- [ ] No secrets, `.env`, data files, or large artifacts in repo or history
- [ ] README verified step by step
- [ ] Proposal deliverables ticked:
  - [ ] Source: shard nodes, router, coordinator, client
  - [ ] Persistent shard map with epoch history + durable node storage
  - [ ] Scripted demo (load → crash → migrate → fencing + stale reads)
  - [ ] Test suite (CRUD, routing, merge, replication, fencing, migration)
  - [ ] Final report with architecture, consistency analysis, evaluation
  - [ ] Metrics: query latency, documents moved, recovery time, replication lag
- [ ] Tag `git tag v1.0.0` and push; share the repo with instructors as required
- [ ] 🔖 **COMMIT/TAG:** `release: v1.0.0`

---

## Request Flows (understand before coding)

| Flow | Path |
|---|---|
| Insert | Client → Router → Primary (validate, WAL fsync) → Follower (WAL fsync) → ack → Primary applies → Client ack |
| Key lookup | Client → Router → Shard Map → Owning Shard → Result |
| Non-key query | Client → Router → All Shards (each returns up to skip+limit) → merge / dedup / global skip+limit → Client |
| Primary failure | Primary dies → Coordinator detects (D) → old lease expires (+margin) → Follower promoted → term and epochs bumped → Router refreshes map → reads continue, writes `REPLICATION_UNAVAILABLE` until the old primary rejoins and is `SYNCED` |
| Migration | Snapshot at S → catch-up → pause → final catch-up → verify → **commit point (map update)** → cleanup |

---

## Team Split

| Member | Primary responsibility | Modules |
|---|---|---|
| **Member 1** | Storage and shard node | 1.2, 2, 3 (+ rejoin/resync in 8, leads 11) |
| **Member 2** | Coordinator, replication, failover | 4.1, 6, 7, 8 (+ coordinator side of 9) |
| **Member 3** | Query engine, router, migration | 4.2, 5, 9 (+ leads load generator in 13) |
| **All** | Setup, contracts, client, Docker, cross-system tests, security pass, bench, demo, report, release | 0, 1.1, 10, 11, 12, 13, 14, 15, 16 |

**Rules for parallel work**
- Nobody starts parallel coding until `docs/contracts.md` is merged.
- All members follow `docs/contracts.md`; no one changes API shapes alone.
- Every module exposes a working API before dependent modules begin integration.
- Every merged module passes its own tests and is reviewed by someone else.
- Integration testing happens after every milestone; each milestone has a runnable demonstration.

---

## Golden Rules

- Tests are written **with** each module, never postponed.
- Move on only when the gate is ticked: **working and tested**, not just written.
- Commit small and often; never commit secrets.
- Keep the README current as you go.
- Settle consistency rules in writing **before** Modules 6–9.
- Build the minimum working system first, then add distribution features one at a time.
- Describe the system honestly: it recovers from a **single primary failure** without losing acknowledged writes, but the shard rejects writes until the failed node rejoins; it is not a fully fault-tolerant database.
- After sign-off, **freeze this checklist**. No new features; only fixes.
