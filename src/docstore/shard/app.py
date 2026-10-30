"""
Shard Node FastAPI application — Module 3.

Identity: node_id, shard_id, role, term, owned ranges with ownership_epochs.

Public endpoints:
  POST   /doc                  — insert
  PUT    /doc/{doc_id}         — update (full replace)
  DELETE /doc/{doc_id}         — delete
  GET    /doc/{doc_id}         — get
  GET    /health               — liveness probe
  GET    /status               — detailed node status

Internal endpoints (inter-node RPC, cluster auth required):
  POST   /internal/replicate   — (stub for M4)
  GET    /internal/status      — machine-readable status

Design notes for this milestone:
  - Role, term, epoch, and lease checks are **stubbed** ("always valid") —
    they will be wired in Module 4 (coordinator) and Module 5 (failover).
  - WAL recovery must succeed before the node accepts requests.
  - request_id dedup is fully wired: retries return the original result.
  - Body size is enforced via middleware.
  - No stack traces in error responses.
  - State is stored in app.state.node (per-app) so tests can run
    multiple app instances without interference.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from docstore.common.auth import make_cluster_auth_dependency
from docstore.common.config import Settings
from docstore.common.errors import DuplicateKey, NotFound, OutOfRange
from docstore.common.logging import get_logger
from docstore.common.models import (
    DOC_ID_MIN,
    Document,
    NodeRole,
    ReadResponse,
    ResponseMeta,
    WriteOp,
    WriteOutcome,
    WriteResponse,
)
from docstore.storage.engine import DedupEntry, StorageEngine
from docstore.storage.snapshot import SnapshotManager
from docstore.storage.wal import WAL, WALCorruptionError

logger = logging.getLogger(__name__)


# ── Module-level request body models (must be at module scope for Pydantic v2) ─


class InsertBody(BaseModel):
    """Request body for POST /doc."""

    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any]


class UpdateBody(BaseModel):
    """Request body for PUT /doc/{doc_id}."""

    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any]


class DeleteBody(BaseModel):
    """Request body for DELETE /doc/{doc_id}."""

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
        self.role: NodeRole = NodeRole.PRIMARY  # stubbed; set by coordinator in M4
        self.term: int = 1  # stubbed; set by coordinator in M4
        self.ranges: list[ShardRangeConfig] = []
        self.ready: bool = False
        self.engine: StorageEngine = StorageEngine()
        self.wal: WAL | None = None
        self.snapshot_mgr: SnapshotManager | None = None

    def owns(self, doc_id: int) -> bool:
        return any(r.start <= doc_id < r.end for r in self.ranges)

    def epoch_for(self, doc_id: int) -> int:
        for r in self.ranges:
            if r.start <= doc_id < r.end:
                return r.ownership_epoch
        return 0

    def response_meta(self, doc_id: int | None = None) -> ResponseMeta:
        epoch = self.epoch_for(doc_id) if doc_id is not None else 0
        return ResponseMeta(
            served_by=self.node_id,
            term=self.term,
            ownership_epoch=epoch,
            applied_seq=self.engine.last_applied_seq,
            replication_lag=None,
        )

    def write_lock(self) -> asyncio.Lock:
        return self.engine.write_lock()


# ── Recovery ──────────────────────────────────────────────────────────────────


def _configure_ranges(state: NodeState) -> None:
    """Set default range from env vars (coordinator overrides this in M4)."""
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

    # Apply env-based default range if still none
    _configure_ranges(state)

    wal = WAL(data_dir / "shard.wal")
    wal.open()
    state.wal = wal

    # Raises WALCorruptionError if any complete record has a bad CRC
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


# ── Write helper (module-level) ───────────────────────────────────────────────


async def _execute_write(
    state: NodeState,
    request_id: str,
    op: WriteOp,
    doc_id: int,
    payload: dict[str, Any] | None,
) -> WriteResponse:
    """Run a write under the write lock with dedup."""
    async with state.write_lock():
        # 1. Dedup check
        existing = state.engine.dedup_check(request_id)
        if existing is not None:
            return WriteResponse(
                outcome=WriteOutcome(existing.outcome),
                request_id=request_id,
                seq=existing.seq,
                meta=state.response_meta(doc_id),
            )

        # 2. Range ownership
        if not state.owns(doc_id):
            raise OutOfRange(
                f"doc_id={doc_id} is outside all ranges owned by shard {state.shard_id}."
            )

        # 3. Allocate seq
        assert state.wal is not None
        seq = state.wal.next_seq()

        # 4. Apply to engine (raises on business-logic errors)
        if op == WriteOp.INSERT:
            state.engine.insert(payload or {}, seq=seq)
        elif op == WriteOp.UPDATE:
            state.engine.update(payload or {}, seq=seq)
        elif op == WriteOp.DELETE:
            state.engine.delete(doc_id, seq=seq)

        # 5. Append to WAL (durable)
        state.wal.append(
            seq=seq,
            term=state.term,
            op=op.value,
            doc_id=doc_id,
            request_id=request_id,
            payload=payload,
        )
        state.engine.last_applied_seq = seq

        # 6. Store in dedup
        result: dict[str, Any] = {"outcome": WriteOutcome.COMMITTED, "seq": seq}
        state.engine.dedup_store(
            request_id,
            DedupEntry(seq=seq, outcome=WriteOutcome.COMMITTED, result=result),
        )

    return WriteResponse(
        outcome=WriteOutcome.COMMITTED,
        request_id=request_id,
        seq=seq,
        meta=state.response_meta(doc_id),
    )


# ── App factory ───────────────────────────────────────────────────────────────


def build_app(settings: Settings, *, node_id: str, shard_id: str) -> FastAPI:
    """Factory: create and return the configured FastAPI shard-node app."""

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        state = NodeState(settings, node_id=node_id, shard_id=shard_id)
        app.state.node = state  # per-app state, not a global

        node_logger = get_logger(__name__, node_id=node_id, shard_id=shard_id)
        node_logger.info("Shard node starting: %s shard=%s", node_id, shard_id)

        try:
            await _recover(state)
            state.ready = True
            node_logger.info("Node ready: %s", node_id)
        except WALCorruptionError as exc:
            node_logger.error("WAL corruption — node refusing to serve: %s", exc)
        except Exception as exc:
            node_logger.error("Recovery failed: %s", exc, exc_info=True)

        yield  # ← app serves requests here

        # Graceful shutdown: take snapshot + close WAL
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

    app = FastAPI(
        title=f"Shard Node — {shard_id}",
        version="0.1.0",
        lifespan=lifespan,
    )

    # ── Body-size middleware ───────────────────────────────────────────────────

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

    # ── Auth ──────────────────────────────────────────────────────────────────

    cluster_auth = make_cluster_auth_dependency(lambda: settings.CLUSTER_SECRET)

    # ── State dependency ──────────────────────────────────────────────────────

    def get_node(request: Request) -> NodeState:
        state: NodeState = request.app.state.node
        if not state.ready:
            raise HTTPException(
                status_code=503,
                detail={"error_code": "NODE_NOT_READY", "error_message": "Node is recovering."},
            )
        return state

    # ── Public endpoints ──────────────────────────────────────────────────────

    @app.post("/doc", status_code=201)
    async def insert_doc(body: InsertBody, request: Request) -> WriteResponse:
        state = get_node(request)
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
        except OutOfRange as exc:
            raise exc.to_http_exception() from exc
        except DuplicateKey as exc:
            raise exc.to_http_exception() from exc

    @app.put("/doc/{doc_id}")
    async def update_doc(doc_id: int, body: UpdateBody, request: Request) -> WriteResponse:
        state = get_node(request)
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
        except OutOfRange as exc:
            raise exc.to_http_exception() from exc
        except NotFound as exc:
            raise exc.to_http_exception() from exc

    @app.delete("/doc/{doc_id}")
    async def delete_doc(doc_id: int, body: DeleteBody, request: Request) -> WriteResponse:
        state = get_node(request)
        try:
            return await _execute_write(state, body.request_id, WriteOp.DELETE, doc_id, None)
        except OutOfRange as exc:
            raise exc.to_http_exception() from exc
        except NotFound as exc:
            raise exc.to_http_exception() from exc

    @app.get("/doc/{doc_id}")
    async def get_doc(doc_id: int, request: Request) -> ReadResponse:
        state = get_node(request)
        if not state.owns(doc_id):
            raise OutOfRange(f"doc_id={doc_id} outside owned ranges.").to_http_exception()
        doc = state.engine.get(doc_id)
        if doc is None:
            raise NotFound(f"doc_id={doc_id} not found.").to_http_exception()
        return ReadResponse(
            document=Document.model_validate(doc),
            meta=state.response_meta(doc_id),
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
        return {
            "node_id": state.node_id,
            "shard_id": state.shard_id,
            "role": state.role.value,
            "term": state.term,
            "ready": state.ready,
            "last_applied_seq": state.engine.last_applied_seq,
            "doc_count": state.engine.doc_count(),
            "ranges": [
                {"start": r.start, "end": r.end, "epoch": r.ownership_epoch} for r in state.ranges
            ],
        }

    # ── Internal endpoints ────────────────────────────────────────────────────

    @app.post("/internal/replicate", dependencies=[Depends(cluster_auth)])
    async def replicate_stub() -> dict[str, str]:
        """Placeholder — implemented in Module 4."""
        return {"status": "stub"}

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

    # ── Error handler (no stack traces) ──────────────────────────────────────

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
