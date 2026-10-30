"""
Stateless Router FastAPI application — Module 4.2.

Key responsibilities:
- Client authentication via X-Api-Key (constant-time comparison).
- Cached ShardMap lookup: routes client requests to the owning primary shard node.
- Preserves client request_id for end-to-end idempotency.
- Bounded retry (max 1) on stale map (OutOfRange / StaleEpoch / NotPrimary).
- Concurrency limiter (DoS protection via semaphore).
- Internal shard endpoints (/internal/*) are NOT exposed.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from docstore.common.auth import (
    CLIENT_KEY_HEADER,
    CLUSTER_SIG_HEADER,
    compute_signature,
    verify_token,
)
from docstore.common.config import Settings
from docstore.common.errors import OutOfRange
from docstore.common.logging import get_logger
from docstore.common.models import (
    DOC_ID_MAX,
    DOC_ID_MIN,
    Document,
    ShardMapEntry,
)
from docstore.router.shard_map import ShardMapManager

logger = get_logger(__name__)


# ── Request / Response models ─────────────────────────────────────────────────


class RouterInsertBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Document containing doc_id.")


class RouterUpdateBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)
    document: dict[str, Any] = Field(description="Full replacement document.")


class RouterDeleteBody(BaseModel):
    request_id: str = Field(min_length=1, max_length=128)


# ── App factory ───────────────────────────────────────────────────────────────


def build_router_app(
    settings: Settings,
    map_mgr: ShardMapManager,
    *,
    shard_transports: dict[str, httpx.AsyncBaseTransport] | None = None,
    http_client: httpx.AsyncClient | None = None,
    max_concurrent_requests: int | None = None,
) -> FastAPI:
    """Create and return the stateless FastAPI router application.

    Parameters
    ----------
    settings : Settings
        Cluster settings including secrets and timeouts.
    map_mgr : ShardMapManager
        Shard map manager holding the routing table.
    shard_transports : dict[str, httpx.BaseTransport], optional
        Mapping of 'host:port' -> ASGITransport for in-memory integration testing.
    http_client : httpx.AsyncClient, optional
        Pre-configured HTTP client (overrides shard_transports if provided).
    max_concurrent_requests : int, optional
        Max concurrent in-flight requests (defaults to settings.MAX_CONCURRENT_REQUESTS).
    """
    concurrency_limit = max_concurrent_requests or settings.MAX_CONCURRENT_REQUESTS
    semaphore = asyncio.Semaphore(concurrency_limit)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        app.state.map_mgr = map_mgr
        app.state.settings = settings
        app.state.semaphore = semaphore

        # Set up HTTP client for forwarding
        if http_client is not None:
            client = http_client
            owns_client = False
        elif shard_transports:
            # Mount transports by host:port
            mounts: dict[str, httpx.AsyncBaseTransport | None] = {}
            for target, transport in shard_transports.items():
                target_clean = target.removeprefix("http://").removeprefix("https://")
                mounts[f"http://{target_clean}"] = transport
            client = httpx.AsyncClient(mounts=mounts, timeout=settings.REPLICATION_TIMEOUT_S + 2.0)
            owns_client = True
        else:
            client = httpx.AsyncClient(timeout=settings.REPLICATION_TIMEOUT_S + 2.0)
            owns_client = True

        app.state.http_client = client
        logger.info("Router started with map version %d", map_mgr.shard_map.map_version)
        yield
        if owns_client:
            await client.aclose()
        logger.info("Router shut down")

    app = FastAPI(title="DocStore Router", version="0.1.0", lifespan=lifespan)

    # ── Concurrency & Body Size Middleware ─────────────────────────────────────

    @app.middleware("http")
    async def _router_middleware(request: Request, call_next: Any) -> Response:
        cl = request.headers.get("content-length")
        if cl and int(cl) > settings.MAX_BODY_SIZE_BYTES:
            return JSONResponse(
                status_code=413,
                content={
                    "error_code": "BODY_TOO_LARGE",
                    "error_message": "Request body exceeds 2 MB.",
                },
            )

        sem: asyncio.Semaphore = request.app.state.semaphore
        try:
            # Try to acquire semaphore without long stall
            await asyncio.wait_for(sem.acquire(), timeout=5.0)
        except TimeoutError:
            return JSONResponse(
                status_code=503,
                content={
                    "error_code": "SERVER_BUSY",
                    "error_message": "Too many concurrent requests.",
                },
            )

        try:
            response: Response = await call_next(request)
            return response
        finally:
            sem.release()

    # ── Client Auth Dependency ────────────────────────────────────────────────

    def _verify_client_auth(
        x_api_key: str | None = Header(default=None, alias=CLIENT_KEY_HEADER),
    ) -> None:
        if not x_api_key or not verify_token(x_api_key, settings.CLIENT_API_KEY):
            raise HTTPException(
                status_code=401,
                detail={
                    "error_code": "AUTH_FAILED",
                    "error_message": "Invalid or missing API key.",
                },
            )

    # ── Forwarding helper with bounded retry ──────────────────────────────────

    async def _forward_request(
        req: Request,
        doc_id: int,
        method: str,
        path: str,
        body_dict: dict[str, Any] | None,
    ) -> JSONResponse:
        """Route request to owning primary, refreshing map once if node reports stale map."""
        client: httpx.AsyncClient = req.app.state.http_client
        mgr: ShardMapManager = req.app.state.map_mgr
        cluster_secret: str = req.app.state.settings.CLUSTER_SECRET

        # Attempt up to 2 times (initial + 1 retry on stale map)
        for attempt in range(2):
            try:
                entry: ShardMapEntry = mgr.get_shard_for_doc_id(doc_id)
            except OutOfRange as exc:
                if attempt == 0:
                    try:
                        mgr.reload()
                        continue
                    except Exception as reload_exc:
                        logger.debug("Map reload failed: %s", reload_exc)
                raise exc.to_http_exception() from exc

            primary = entry.primary.removeprefix("http://").removeprefix("https://")
            url = f"http://{primary}{path}"

            # Prepare body and signature
            if body_dict is not None:
                body_bytes = json.dumps(body_dict, separators=(",", ":")).encode("utf-8")
                sig = compute_signature(cluster_secret, body_bytes)
                headers = {
                    "Content-Type": "application/json",
                    CLUSTER_SIG_HEADER: sig,
                }
                content = body_bytes
            else:
                sig = compute_signature(cluster_secret, b"")
                headers = {
                    CLUSTER_SIG_HEADER: sig,
                }
                content = b""

            try:
                resp = await client.request(
                    method=method,
                    url=url,
                    content=content,
                    headers=headers,
                )
            except (httpx.ConnectError, httpx.TimeoutException) as exc:
                if attempt == 0:
                    try:
                        mgr.reload()
                        continue
                    except Exception as reload_exc:
                        logger.debug("Map reload failed: %s", reload_exc)
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error_code": "SHARD_UNAVAILABLE",
                        "error_message": (
                            f"Shard {entry.shard_id} ({primary}) is unavailable: {exc}"
                        ),
                    },
                ) from exc

            # Check if shard reports stale routing (OutOfRange or StaleEpoch or NotPrimary)
            if resp.status_code in (400, 409) and attempt == 0:
                try:
                    data = resp.json()
                    err_code = data.get("detail", {}).get("error_code") or data.get("error_code")
                    if err_code in ("OUT_OF_RANGE", "STALE_EPOCH", "NOT_PRIMARY"):
                        logger.warning(
                            "Shard %s returned %s on doc_id=%d; reloading map and retrying once",
                            entry.shard_id,
                            err_code,
                            doc_id,
                        )
                        mgr.reload()
                        continue
                except Exception as parse_exc:
                    logger.debug("Failed parsing error body from shard: %s", parse_exc)

            # Return response from shard directly
            try:
                resp_json = resp.json()
            except Exception:
                resp_json = {"detail": resp.text}

            return JSONResponse(status_code=resp.status_code, content=resp_json)

        raise HTTPException(
            status_code=500,
            detail={
                "error_code": "ROUTING_FAILED",
                "error_message": "Exceeded routing retry attempts.",
            },
        )

    # ── Public Router API ─────────────────────────────────────────────────────

    @app.post("/doc", status_code=201)
    async def insert_doc(
        body: RouterInsertBody,
        request: Request,
        _auth: None = Header(default=None),
    ) -> JSONResponse:
        _verify_client_auth(request.headers.get(CLIENT_KEY_HEADER))
        try:
            doc = Document.model_validate(body.document)
        except Exception as exc:
            raise HTTPException(
                status_code=422,
                detail={"error_code": "VALIDATION_ERROR", "error_message": str(exc)},
            ) from exc

        doc_dict = doc.model_dump()
        doc_id = doc_dict["doc_id"]
        forward_body = {"request_id": body.request_id, "document": doc_dict}

        return await _forward_request(
            request,
            doc_id=doc_id,
            method="POST",
            path="/doc",
            body_dict=forward_body,
        )

    @app.put("/doc/{doc_id}")
    async def update_doc(
        doc_id: int,
        body: RouterUpdateBody,
        request: Request,
    ) -> JSONResponse:
        _verify_client_auth(request.headers.get(CLIENT_KEY_HEADER))
        if isinstance(doc_id, bool) or doc_id < DOC_ID_MIN or doc_id > DOC_ID_MAX:
            raise OutOfRange(f"doc_id={doc_id} out of range").to_http_exception()

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
        forward_body = {"request_id": body.request_id, "document": doc_dict}

        return await _forward_request(
            request,
            doc_id=doc_id,
            method="PUT",
            path=f"/doc/{doc_id}",
            body_dict=forward_body,
        )

    @app.delete("/doc/{doc_id}")
    async def delete_doc(
        doc_id: int,
        body: RouterDeleteBody,
        request: Request,
    ) -> JSONResponse:
        _verify_client_auth(request.headers.get(CLIENT_KEY_HEADER))
        if isinstance(doc_id, bool) or doc_id < DOC_ID_MIN or doc_id > DOC_ID_MAX:
            raise OutOfRange(f"doc_id={doc_id} out of range").to_http_exception()

        forward_body = {"request_id": body.request_id}
        return await _forward_request(
            request,
            doc_id=doc_id,
            method="DELETE",
            path=f"/doc/{doc_id}",
            body_dict=forward_body,
        )

    @app.get("/doc/{doc_id}")
    async def get_doc(
        doc_id: int,
        request: Request,
    ) -> JSONResponse:
        _verify_client_auth(request.headers.get(CLIENT_KEY_HEADER))
        if isinstance(doc_id, bool) or doc_id < DOC_ID_MIN or doc_id > DOC_ID_MAX:
            raise OutOfRange(f"doc_id={doc_id} out of range").to_http_exception()

        return await _forward_request(
            request,
            doc_id=doc_id,
            method="GET",
            path=f"/doc/{doc_id}",
            body_dict=None,
        )

    @app.get("/health")
    async def health(request: Request) -> dict[str, Any]:
        mgr: ShardMapManager = request.app.state.map_mgr
        return {
            "status": "ok",
            "map_version": mgr.shard_map.map_version,
            "shard_count": len(mgr.shard_map.entries),
        }

    @app.get("/status")
    async def status(request: Request) -> dict[str, Any]:
        mgr: ShardMapManager = request.app.state.map_mgr
        return {
            "status": "ok",
            "map_version": mgr.shard_map.map_version,
            "shards": [
                {
                    "shard_id": e.shard_id,
                    "range": {"start": e.range.start, "end": e.range.end},
                    "primary": e.primary,
                    "follower": e.follower,
                    "ownership_epoch": e.ownership_epoch,
                }
                for e in mgr.shard_map.entries
            ],
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
