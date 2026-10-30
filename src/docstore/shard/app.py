"""
Shard Node FastAPI application — Modules 3 & 7.

Identity: node_id, shard_id, role, term, owned ranges with ownership_epochs.

Public endpoints:
  POST   /doc                  — insert (primary only, sync replicated)
  PUT    /doc/{doc_id}         — update (full replace)
  DELETE /doc/{doc_id}         — delete
  GET    /doc/{doc_id}         — get (primary with valid lease, or follower with lag)
  POST   /query                — query engine evaluation
  GET    /health               — liveness probe
  GET    /status               — detailed node status (replication lag, lease, etc.)

Internal endpoints (inter-node RPC, cluster auth required):
  POST   /internal/replicate       — primary -> follower synchronous WAL append
  POST   /internal/catchup         — follower catch-up from primary WAL
  GET    /internal/snapshot        — full snapshot resync
  POST   /internal/snapshot/apply  — apply snapshot on follower
  POST   /internal/fence           — bump term

Write path (Module 7):
  1. Client sends write with unique request_id
  2. Primary takes write lock; checks:
     - role == PRIMARY (else NotPrimary)
     - lease remaining > T + M (else LeaseExpired)
     - range ownership (owns doc_id, else OutOfRange)
     - dedup check (cached -> return original result)
     - follower is SYNCED if replicated (else ReplicationUnavailable)
     - pre-WAL validity (insert: DuplicateKey; update/delete: NotFound)
  3. Primary assigns next sequence number
  4. Primary appends to its WAL and fsyncs
  5. Primary sends to follower via /internal/replicate (timeout T=1.0s)
  6. Follower fsyncs to WAL and sends durable acknowledgement
  7. Primary applies to in-memory state and dedup table
  8. If lease is still valid, returns COMMITTED; if expired meanwhile, return outcome_unknown
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from docstore.common.auth import (
    CLUSTER_SIG_HEADER,
    compute_signature,
    make_cluster_auth_dependency,
)
from docstore.common.config import Settings
from docstore.common.errors import (
    DocstoreError,
    DuplicateKey,
    InvalidQuery,
    LeaseExpired,
    NotFound,
    NotPrimary,
    OutcomeUnknown,
    OutOfRange,
    ReplicationUnavailable,
    StaleTerm,
)
from docstore.common.logging import get_logger
from docstore.common.models import (
    DOC_ID_MIN,
    CatchupRequest,
    CatchupResponse,
    Document,
    NodeRole,
    QueryFilter,
    QueryResponse,
    ReadResponse,
    ReplicaState,
    ReplicationRequest,
    ReplicationResponse,
    ResponseMeta,
    WriteOp,
    WriteOutcome,
    WriteResponse,
)
from docstore.query import (
    detect_shard_key_constraint,
    evaluate_filter,
    sort_documents,
    validate_query_filter,
)
from docstore.storage.engine import DedupEntry, StorageEngine
from docstore.storage.snapshot import SnapshotManager
from docstore.storage.wal import WAL, WALCorruptionError

logger = logging.getLogger(__name__)


# ── Module-level request body models ──────────────────────────────────────────


class InsertBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Must contain doc_id.")


class UpdateBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Full replacement document.")


class DeleteBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)


# ── Node state ────────────────────────────────────────────────────────────────


class ShardRangeConfig(BaseModel):
    """One owned range with its epoch."""

    start: int = Field(ge=DOC_ID_MIN)
    end: int = Field(ge=1)
    ownership_epoch: int = Field(ge=0)


class NodeState:
    """Mutable runtime state for a single shard node instance."""

    def __init__(self, settings: Settings, node_id: str, shard_id: str) -> None:
        self.settings = settings
        self.node_id = node_id
        self.shard_id = shard_id
        self.role: NodeRole = NodeRole.PRIMARY
        self.term: int = 1
        self.ranges: list[ShardRangeConfig] = []
        self.ready: bool = False
        self.engine: StorageEngine = StorageEngine()
        self.wal: WAL | None = None
        self.snapshot_mgr: SnapshotManager | None = None

        # Lease & replication fields (Module 6 & 7)
        self.lease_expires_at: float = float("inf")
        self.follower_url: str | None = None
        self.follower_state: ReplicaState = ReplicaState.SYNCED
        self.follower_acked_seq: int = 0
        self.primary_seq: int = 0  # tracked by follower for lag measurement
        self.http_client: httpx.AsyncClient | None = None

    def owns(self, doc_id: int) -> bool:
        return any(r.start <= doc_id < r.end for r in self.ranges)

    def epoch_for(self, doc_id: int) -> int:
        for r in self.ranges:
            if r.start <= doc_id < r.end:
                return r.ownership_epoch
        return 0

    def is_lease_valid(self) -> bool:
        if self.role != NodeRole.PRIMARY:
            return False
        return time.monotonic() < self.lease_expires_at

    def remaining_lease(self) -> float:
        if self.role != NodeRole.PRIMARY:
            return 0.0
        if self.lease_expires_at == float("inf"):
            return float("inf")
        return max(0.0, self.lease_expires_at - time.monotonic())

    def response_meta(self, doc_id: int | None = None) -> ResponseMeta:
        epoch = self.epoch_for(doc_id) if doc_id is not None else 0
        lag = None
        if self.role == NodeRole.FOLLOWER:
            lag = max(0, self.primary_seq - self.engine.last_applied_seq)

        return ResponseMeta(
            served_by=self.role.value,
            term=self.term,
            ownership_epoch=epoch,
            applied_seq=self.engine.last_applied_seq,
            replication_lag=lag,
        )

    def write_lock(self) -> asyncio.Lock:
        return self.engine.write_lock()


# ── Recovery ──────────────────────────────────────────────────────────────────


def _configure_ranges(state: NodeState) -> None:
    """Set default range from env vars if none configured."""
    if state.ranges:
        return
    start = int(os.environ.get("SHARD_RANGE_START", 0))
    end = int(os.environ.get("SHARD_RANGE_END", 2**63))
    epoch = int(os.environ.get("SHARD_OWNERSHIP_EPOCH", 1))
    state.ranges.append(ShardRangeConfig(start=start, end=end, ownership_epoch=epoch))


async def _recover(state: NodeState) -> None:
    """Load snapshot then replay WAL. Raises WALCorruptionError on bad WAL."""
    data_dir = Path(state.settings.DATA_DIR) / state.shard_id
    data_dir.mkdir(parents=True, exist_ok=True)

    snap_mgr = SnapshotManager(data_dir)
    state.snapshot_mgr = snap_mgr
    after_seq = 0

    snap = snap_mgr.load()
    if snap:
        for doc_id, doc in snap["documents"].items():
            state.engine._docs[doc_id] = doc
            state.engine._index[doc_id] = None
        state.engine.last_applied_seq = snap["last_applied_seq"]
        state.term = snap.get("term", 1)
        state.engine.dedup_import(snap.get("dedup_table", {}))
        after_seq = snap["last_applied_seq"]

        epoch_map: dict[str, int] = snap.get("ownership_epochs", {})
        if epoch_map and not state.ranges:
            for range_key, epoch in epoch_map.items():
                s, e = range_key.split("-", 1)
                state.ranges.append(
                    ShardRangeConfig(start=int(s), end=int(e), ownership_epoch=epoch)
                )

    _configure_ranges(state)

    wal = WAL(data_dir / "shard.wal")
    wal.open()
    state.wal = wal

    wal_entries = list(wal.replay(after_seq=after_seq))
    for entry in wal_entries:
        state.engine.apply(entry)
        rid = entry.get("request_id")
        if rid:
            state.engine.dedup_store(
                rid,
                DedupEntry(
                    seq=entry["seq"],
                    outcome=WriteOutcome.COMMITTED,
                    result={"outcome": WriteOutcome.COMMITTED, "seq": entry["seq"]},
                ),
            )

    logger.info(
        "Recovery complete: shard=%s last_applied_seq=%d docs=%d",
        state.shard_id,
        state.engine.last_applied_seq,
        state.engine.doc_count(),
    )


# ── Replication Helper ────────────────────────────────────────────────────────


async def _replicate_to_follower(
    state: NodeState,
    seq: int,
    op: WriteOp,
    doc_id: int,
    request_id: str,
    payload: dict[str, Any] | None,
) -> bool:
    """Primary sends operation to follower and awaits fsynced acknowledgement."""
    if not state.follower_url:
        return True

    client = state.http_client
    owns_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=state.settings.REPLICATION_TIMEOUT_S)
        owns_client = True

    try:
        clean_url = state.follower_url.removeprefix("http://").removeprefix("https://").rstrip("/")
        url = f"http://{clean_url}/internal/replicate"
        req_body = {
            "shard_id": state.shard_id,
            "term": state.term,
            "seq": seq,
            "op": op.value,
            "doc_id": doc_id,
            "request_id": request_id,
            "payload": payload,
        }
        body_bytes = json.dumps(req_body, separators=(",", ":")).encode("utf-8")
        sig = compute_signature(state.settings.CLUSTER_SECRET, body_bytes)
        headers = {
            "Content-Type": "application/json",
            CLUSTER_SIG_HEADER: sig,
        }

        resp = await client.post(
            url,
            content=body_bytes,
            headers=headers,
            timeout=state.settings.REPLICATION_TIMEOUT_S,
        )
        if resp.status_code == 200:
            data = resp.json()
            return bool(data.get("status") == "ok")
        logger.warning(
            "Follower replication returned status=%d body=%s", resp.status_code, resp.text
        )
        return False
    except Exception as exc:
        logger.warning("Follower replication call failed: %s", exc)
        return False
    finally:
        if owns_client:
            await client.aclose()


# ── Write Helper ──────────────────────────────────────────────────────────────


async def _execute_write(
    state: NodeState,
    request_id: str,
    op: WriteOp,
    doc_id: int,
    payload: dict[str, Any] | None,
) -> WriteResponse:
    """Run a write under the per-shard write lock with replication and lease fencing."""
    async with state.write_lock():
        # 1. Role check: only primary accepts writes
        if state.role != NodeRole.PRIMARY:
            raise NotPrimary(f"Node {state.node_id} is {state.role.value}, not primary.")

        # 2. Lease check: remaining lease > T + M
        min_lease = state.settings.REPLICATION_TIMEOUT_S + state.settings.SAFETY_MARGIN_S
        if state.remaining_lease() <= min_lease:
            rem = state.remaining_lease()
            raise LeaseExpired(f"Primary lease remaining ({rem:.2f}s) <= T+M ({min_lease:.2f}s).")

        # 3. Range ownership
        if not state.owns(doc_id):
            raise OutOfRange(
                f"doc_id={doc_id} is outside all ranges owned by shard {state.shard_id}."
            )

        # 4. Dedup check: return original result for retried request_id
        existing = state.engine.dedup_check(request_id)
        if existing is not None:
            return WriteResponse(
                outcome=WriteOutcome(existing.outcome),
                request_id=request_id,
                seq=existing.seq,
                meta=state.response_meta(doc_id),
            )

        # 5. Follower SYNCED check (if replicated)
        if state.follower_url:
            if state.follower_state != ReplicaState.SYNCED:
                raise ReplicationUnavailable(
                    f"Follower is {state.follower_state.value}; write rejected before WAL append."
                )

        # 6. Pre-WAL business logic validity check
        if op == WriteOp.INSERT:
            if doc_id in state.engine._docs:
                raise DuplicateKey(f"doc_id={doc_id} already exists.")
        elif op in (WriteOp.UPDATE, WriteOp.DELETE):
            if doc_id not in state.engine._docs:
                raise NotFound(f"doc_id={doc_id} not found.")

        # 7. Assign sequence number & append to primary WAL (durable)
        assert state.wal is not None
        seq = state.wal.next_seq()
        state.wal.append(
            seq=seq,
            term=state.term,
            op=op.value,
            doc_id=doc_id,
            request_id=request_id,
            payload=payload,
        )

        # 8. Synchronous replication to follower
        if state.follower_url:
            replicated = await _replicate_to_follower(state, seq, op, doc_id, request_id, payload)
            if not replicated:
                # Follower failed after primary WAL append: outcome unknown
                state.follower_state = ReplicaState.DOWN
                state.engine.dedup_store(
                    request_id,
                    DedupEntry(
                        seq=seq,
                        outcome=WriteOutcome.OUTCOME_UNKNOWN,
                        result={"outcome": WriteOutcome.OUTCOME_UNKNOWN, "seq": seq},
                    ),
                )
                raise OutcomeUnknown(
                    f"Write appended to primary WAL (seq={seq}), but follower replication failed."
                )
            state.follower_acked_seq = seq

        # 9. Apply to memory state via engine.apply() — unified idempotent codepath (M3)
        apply_entry = {
            "seq": seq,
            "term": state.term,
            "op": op.value,
            "doc_id": doc_id,
            "request_id": request_id,
            "payload": payload,
        }
        state.engine.apply(apply_entry)
        result = {"outcome": WriteOutcome.COMMITTED, "seq": seq}
        state.engine.dedup_store(
            request_id,
            DedupEntry(seq=seq, outcome=WriteOutcome.COMMITTED, result=result),
        )

        # 10. Final lease check before client reply
        if state.lease_expires_at != float("inf") and not state.is_lease_valid():
            raise OutcomeUnknown("Primary lease expired before client acknowledgement.")

    return WriteResponse(
        outcome=WriteOutcome.COMMITTED,
        request_id=request_id,
        seq=seq,
        meta=state.response_meta(doc_id),
    )


# ── App factory ───────────────────────────────────────────────────────────────


def build_app(
    settings: Settings,
    *,
    node_id: str,
    shard_id: str,
    ranges: list[ShardRangeConfig] | None = None,
    role: NodeRole = NodeRole.PRIMARY,
    term: int = 1,
    lease_expires_at: float | None = None,
    follower_url: str | None = None,
    follower_state: ReplicaState = ReplicaState.SYNCED,
    follower_transport: httpx.AsyncBaseTransport | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> FastAPI:
    """Factory: create and return the configured FastAPI shard-node app."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        state = NodeState(settings, node_id=node_id, shard_id=shard_id)
        if ranges:
            state.ranges = list(ranges)
        state.role = role
        state.term = term
        state.follower_url = follower_url
        state.follower_state = follower_state

        if lease_expires_at is not None:
            state.lease_expires_at = lease_expires_at
        elif role == NodeRole.PRIMARY:
            # Standalone default to inf so unit tests not testing leases pass
            state.lease_expires_at = float("inf")
        else:
            state.lease_expires_at = 0.0

        # Configure HTTP client for outbound replication
        owns_client = False
        if http_client is not None:
            state.http_client = http_client
        elif follower_transport and follower_url:
            clean_url = follower_url.removeprefix("http://").removeprefix("https://")
            mounts = {f"http://{clean_url}": follower_transport}
            state.http_client = httpx.AsyncClient(
                mounts=mounts, timeout=settings.REPLICATION_TIMEOUT_S
            )
            owns_client = True

        app.state.node = state
        node_logger = get_logger(__name__, node_id=node_id, shard_id=shard_id)
        node_logger.info("Shard node starting: %s shard=%s role=%s", node_id, shard_id, role.value)

        try:
            await _recover(state)
            state.ready = True
            node_logger.info("Node ready: %s", node_id)
        except WALCorruptionError as exc:
            node_logger.error("WAL corruption — node refusing to serve: %s", exc)
        except Exception as exc:
            node_logger.error("Recovery failed: %s", exc, exc_info=True)

        yield

        if owns_client and state.http_client:
            await state.http_client.aclose()

        if state.ready and state.wal and state.snapshot_mgr:
            node_logger.info("Shutdown: saving snapshot")
            try:
                state.snapshot_mgr.save(
                    documents=state.engine.all_docs(),
                    last_applied_seq=state.engine.last_applied_seq,
                    term=state.term,
                    ownership_epochs={
                        f"{r.start}-{r.end}": r.ownership_epoch for r in state.ranges
                    },
                    dedup_table=state.engine.dedup_export(),
                )
            except Exception as exc:
                node_logger.error("Snapshot on shutdown failed: %s", exc)
            state.wal.close()

    app = FastAPI(title=f"Shard Node — {shard_id}", version="0.1.0", lifespan=lifespan)

    # ── Middleware ────────────────────────────────────────────────────────────

    @app.middleware("http")
    async def _body_size_limit(request: Request, call_next: Any) -> Response:
        cl = request.headers.get("content-length")
        if cl and int(cl) > settings.MAX_BODY_SIZE_BYTES:
            return JSONResponse(
                status_code=413,
                content={
                    "error_code": "BODY_TOO_LARGE",
                    "error_message": "Request body exceeds 2 MB.",
                },
            )
        response: Response = await call_next(request)
        return response

    cluster_auth = make_cluster_auth_dependency(lambda: settings.CLUSTER_SECRET)

    def get_node(request: Request) -> NodeState:
        state: NodeState = request.app.state.node
        if not state.ready:
            raise HTTPException(
                status_code=503,
                detail={"error_code": "NODE_NOT_READY", "error_message": "Node is recovering."},
            )
        return state

    # ── Public Endpoints ──────────────────────────────────────────────────────

    @app.post("/doc", status_code=201)
    async def insert_doc(body: InsertBody, request: Request) -> WriteResponse:
        state = get_node(request)
        if state.role != NodeRole.PRIMARY:
            raise NotPrimary(
                f"Node {state.node_id} is a follower; cannot accept writes."
            ).to_http_exception()

        try:
            doc = Document.model_validate(body.document)
        except Exception as exc:
            raise HTTPException(
                status_code=422,
                detail={"error_code": "VALIDATION_ERROR", "error_message": str(exc)},
            ) from exc

        doc_dict = doc.model_dump()
        doc_id = doc_dict["doc_id"]
        try:
            return await _execute_write(state, body.request_id, WriteOp.INSERT, doc_id, doc_dict)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.put("/doc/{doc_id}")
    async def update_doc(doc_id: int, body: UpdateBody, request: Request) -> WriteResponse:
        state = get_node(request)
        if state.role != NodeRole.PRIMARY:
            raise NotPrimary(
                f"Node {state.node_id} is a follower; cannot accept writes."
            ).to_http_exception()

        try:
            doc = Document.model_validate(body.document)
        except Exception as exc:
            raise HTTPException(
                status_code=422,
                detail={"error_code": "VALIDATION_ERROR", "error_message": str(exc)},
            ) from exc

        if doc.doc_id != doc_id:
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "VALIDATION_ERROR",
                    "error_message": "doc_id in path and body must match.",
                },
            )

        doc_dict = doc.model_dump()
        try:
            return await _execute_write(state, body.request_id, WriteOp.UPDATE, doc_id, doc_dict)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.delete("/doc/{doc_id}")
    async def delete_doc(doc_id: int, body: DeleteBody, request: Request) -> WriteResponse:
        state = get_node(request)
        if state.role != NodeRole.PRIMARY:
            raise NotPrimary(
                f"Node {state.node_id} is a follower; cannot accept writes."
            ).to_http_exception()

        try:
            return await _execute_write(state, body.request_id, WriteOp.DELETE, doc_id, None)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.get("/doc/{doc_id}")
    async def get_doc(doc_id: int, request: Request) -> ReadResponse:
        state = get_node(request)
        if not state.owns(doc_id):
            raise OutOfRange(f"doc_id={doc_id} outside owned ranges.").to_http_exception()

        if state.role == NodeRole.PRIMARY and not state.is_lease_valid():
            raise LeaseExpired("Primary lease has expired.").to_http_exception()

        doc = state.engine.get(doc_id)
        if doc is None:
            raise NotFound(f"doc_id={doc_id} not found.").to_http_exception()

        return ReadResponse(
            document=Document.model_validate(doc),
            meta=state.response_meta(doc_id),
        )

    @app.post("/query")
    async def query_docs(body: QueryFilter, request: Request) -> QueryResponse:
        state = get_node(request)
        try:
            validate_query_filter(body.filter)
        except InvalidQuery as exc:
            raise exc.to_http_exception() from exc

        if state.role == NodeRole.PRIMARY and not state.is_lease_valid():
            raise LeaseExpired("Primary lease has expired.").to_http_exception()

        # Collect candidate documents according to shard-key constraints & ownership
        constraint = detect_shard_key_constraint(body.filter)
        candidates: list[dict[str, Any]] = []

        if constraint.kind == "exact":
            assert constraint.doc_id is not None
            if state.owns(constraint.doc_id):
                doc = state.engine.get(constraint.doc_id)
                if doc is not None:
                    candidates.append(doc)
        elif constraint.kind == "in":
            for doc_id in constraint.doc_ids or []:
                if state.owns(doc_id):
                    doc = state.engine.get(doc_id)
                    if doc is not None:
                        candidates.append(doc)
        elif constraint.kind == "range":
            assert constraint.range_start is not None and constraint.range_end is not None
            for r in state.ranges:
                s = max(constraint.range_start, r.start)
                e = min(constraint.range_end, r.end)
                if s < e:
                    candidates.extend(state.engine.range_scan(s, e))
        else:
            for r in state.ranges:
                candidates.extend(state.engine.range_scan(r.start, r.end))

        matched = [doc for doc in candidates if evaluate_filter(doc, body.filter)]
        sorted_docs = sort_documents(matched, sort_field=body.sort_field, sort_dir=body.sort_dir)
        total = len(sorted_docs)
        paginated = sorted_docs[body.skip : body.skip + body.limit]

        return QueryResponse(
            documents=paginated,
            total=total,
            meta=state.response_meta(),
        )

    @app.get("/health")
    async def health(request: Request) -> dict[str, str]:
        state: NodeState = request.app.state.node
        if not state.ready:
            raise HTTPException(
                status_code=503,
                detail={"error_code": "NODE_NOT_READY", "error_message": "Recovering."},
            )
        return {"status": "ok"}

    @app.get("/status")
    async def node_status(request: Request) -> dict[str, Any]:
        state: NodeState = request.app.state.node
        lag = None
        if state.role == NodeRole.FOLLOWER:
            lag = max(0, state.primary_seq - state.engine.last_applied_seq)

        return {
            "node_id": state.node_id,
            "shard_id": state.shard_id,
            "role": state.role.value,
            "term": state.term,
            "ready": state.ready,
            "last_applied_seq": state.engine.last_applied_seq,
            "doc_count": state.engine.doc_count(),
            "lease_remaining_s": round(state.remaining_lease(), 3),
            "follower_state": state.follower_state.value,
            "follower_acked_seq": state.follower_acked_seq,
            "replication_lag": lag,
            "ranges": [
                {"start": r.start, "end": r.end, "epoch": r.ownership_epoch} for r in state.ranges
            ],
        }

    # ── Internal Replication & Catchup Endpoints ──────────────────────────────

    @app.post("/internal/replicate", dependencies=[Depends(cluster_auth)])
    async def replicate(body: ReplicationRequest, request: Request) -> ReplicationResponse:
        state = get_node(request)
        if body.term < state.term:
            raise StaleTerm(
                f"Replication term {body.term} < node term {state.term}"
            ).to_http_exception()
        if body.term > state.term:
            state.term = body.term

        assert state.wal is not None
        expected_seq = state.wal.next_seq()

        if body.seq == expected_seq:
            # 1. Append to follower WAL and fsync
            state.wal.append(
                seq=body.seq,
                term=body.term,
                op=body.op.value,
                doc_id=body.doc_id,
                request_id=body.request_id,
                payload=body.payload,
            )
            # 2. Apply via engine.apply() — idempotent; safe after crash-restart (C3)
            repl_entry = {
                "seq": body.seq,
                "term": body.term,
                "op": body.op.value,
                "doc_id": body.doc_id,
                "request_id": body.request_id,
                "payload": body.payload,
            }
            state.engine.apply(repl_entry)
            state.primary_seq = body.seq
            state.engine.dedup_store(
                body.request_id,
                DedupEntry(
                    seq=body.seq,
                    outcome=WriteOutcome.COMMITTED,
                    result={"outcome": WriteOutcome.COMMITTED, "seq": body.seq},
                ),
            )
            return ReplicationResponse(status="ok", seq=body.seq, last_seq=body.seq)

        if body.seq <= (state.wal.last_seq if state.wal else 0):
            # Duplicate delivery: already committed
            return ReplicationResponse(
                status="ok",
                seq=body.seq,
                last_seq=state.wal.last_seq if state.wal else 0,
            )

        # Gap detected
        return ReplicationResponse(
            status="gap",
            seq=body.seq,
            last_seq=state.wal.last_seq if state.wal else 0,
            error_message=f"Gap detected: expected {expected_seq}, got {body.seq}",
        )

    @app.post("/internal/catchup", dependencies=[Depends(cluster_auth)])
    async def catchup(body: CatchupRequest, request: Request) -> CatchupResponse:
        state = get_node(request)
        assert state.wal is not None

        earliest = state.wal.earliest_seq()
        if earliest is not None and earliest > body.after_seq + 1:
            return CatchupResponse(
                entries=[],
                needs_snapshot=True,
                last_seq=state.wal.last_seq,
            )

        entries = list(state.wal.replay(after_seq=body.after_seq))
        return CatchupResponse(
            entries=entries,
            needs_snapshot=False,
            last_seq=state.wal.last_seq,
        )

    @app.get("/internal/snapshot", dependencies=[Depends(cluster_auth)])
    async def get_snapshot(request: Request) -> dict[str, Any]:
        state = get_node(request)
        return {
            "documents": state.engine.all_docs(),
            "last_applied_seq": state.engine.last_applied_seq,
            "term": state.term,
            "ownership_epochs": {f"{r.start}-{r.end}": r.ownership_epoch for r in state.ranges},
            "dedup_table": state.engine.dedup_export(),
        }

    @app.post("/internal/snapshot/apply", dependencies=[Depends(cluster_auth)])
    async def apply_snapshot(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        async with state.write_lock():
            # C2: all_docs() returns dict[int, dict]; iterating the dict yields keys,
            # not documents.  Handle both the dict format (current) and a list format
            # (future-proofing) so the follower resync never crashes.
            docs = body.get("documents", {})
            state.engine._docs.clear()
            state.engine._index.clear()
            if isinstance(docs, dict):
                for doc_id_str, doc in docs.items():
                    doc_id = int(doc_id_str)
                    state.engine._docs[doc_id] = doc
                    state.engine._index[doc_id] = None
            else:  # list format (future-proofing)
                for doc in docs:
                    doc_id = doc["doc_id"]
                    state.engine._docs[doc_id] = doc
                    state.engine._index[doc_id] = None
            state.engine.last_applied_seq = body.get("last_applied_seq", 0)
            state.primary_seq = state.engine.last_applied_seq
            state.term = body.get("term", state.term)
            state.engine.dedup_import(body.get("dedup_table", {}))

            if state.wal is not None:
                state.wal.truncate_after(state.engine.last_applied_seq)

            return {"status": "ok", "applied_seq": state.engine.last_applied_seq}

    @app.post("/internal/fence", dependencies=[Depends(cluster_auth)])
    async def fence(body: dict[str, Any], request: Request) -> dict[str, Any]:
        state = get_node(request)
        new_term = body.get("term")
        # M6: Only accept exactly current_term+1 to prevent a misbehaving peer
        # from jumping the term to max-int and permanently blocking replication.
        if new_term is not None:
            if new_term == state.term + 1:
                state.term = new_term
            elif new_term > state.term + 1:
                logger.warning(
                    "Fence rejected: term jump %d -> %d exceeds +1; ignoring.",
                    state.term,
                    new_term,
                )
        return {"status": "ok", "term": state.term}

    @app.get("/internal/status", dependencies=[Depends(cluster_auth)])
    async def internal_status(request: Request) -> dict[str, Any]:
        state = get_node(request)
        return {
            "node_id": state.node_id,
            "shard_id": state.shard_id,
            "role": state.role.value,
            "term": state.term,
            "last_applied_seq": state.engine.last_applied_seq,
            "doc_count": state.engine.doc_count(),
        }

    # ── Error handler ─────────────────────────────────────────────────────────

    @app.exception_handler(HTTPException)
    async def _http_exc_handler(request: Request, exc: HTTPException) -> JSONResponse:
        if isinstance(exc.detail, dict):
            detail = exc.detail
        else:
            detail = {"error_code": "ERROR", "error_message": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=detail)

    return app


def create_default_app() -> FastAPI:
    """Factory for uvicorn (--factory) using settings from environment."""
    from docstore.common.config import get_settings

    settings = get_settings()
    node_id = os.environ.get("NODE_ID", "shard-node-default")
    shard_id = os.environ.get("SHARD_ID", "shard0")
    return build_app(settings, node_id=node_id, shard_id=shard_id)
