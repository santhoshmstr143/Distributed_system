"""
FastAPI application for the Coordinator — Module 6.

Public endpoints:
- GET  /shardmap   — fetch current cluster shard map
- POST /register   — node startup registration (cluster auth required)
- POST /heartbeat  — node periodic heartbeats & primary lease renewals (cluster auth required)
- GET  /health     — liveness probe

Admin endpoints (X-Admin-Token required):
- GET  /admin/status   — detailed cluster status (replica states, leases, terms)
- POST /admin/migrate  — trigger range migration (stub for Module 9)
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from docstore.common.auth import make_admin_auth_dependency, make_cluster_auth_dependency
from docstore.common.config import Settings
from docstore.common.errors import DocstoreError
from docstore.common.logging import get_logger
from docstore.common.models import (
    Heartbeat,
    LeaseGrant,
    NodeRegistration,
    RegistrationResponse,
    ShardMap,
)
from docstore.coordinator.coordinator import CoordinatorService

logger = get_logger(__name__)


def build_coordinator_app(
    settings: Settings,
    coordinator_svc: CoordinatorService | None = None,
) -> FastAPI:
    """Create and return the FastAPI Coordinator application."""
    svc = coordinator_svc or CoordinatorService(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        app.state.coordinator = svc
        app.state.settings = settings
        logger.info(
            "Coordinator started with map version %d",
            svc.shard_map.map_version,
        )

        # H5 / Module 8: Background health monitor — checks for timed-out nodes and auto-failovers.
        async def _health_monitor() -> None:
            while True:
                await asyncio.sleep(settings.HEARTBEAT_INTERVAL_S)
                try:
                    await svc.check_and_trigger_failover()
                except Exception as exc:
                    logger.warning("Health monitor failover check error: %s", exc)

        monitor_task = asyncio.create_task(_health_monitor())

        yield

        monitor_task.cancel()
        try:
            await monitor_task
        except asyncio.CancelledError:
            pass

        logger.info("Coordinator shut down")

    app = FastAPI(title="DocStore Coordinator", version="0.1.0", lifespan=lifespan)
    app.state.coordinator = svc
    app.state.settings = settings

    # ── Auth dependencies ─────────────────────────────────────────────────────

    admin_auth = make_admin_auth_dependency(lambda: settings.ADMIN_TOKEN)
    # C4: cluster_auth guards /register and /heartbeat (contracts §9)
    cluster_auth = make_cluster_auth_dependency(lambda: settings.CLUSTER_SECRET)

    def get_service(request: Request) -> CoordinatorService:
        return request.app.state.coordinator  # type: ignore[no-any-return]

    # ── Public Endpoints ──────────────────────────────────────────────────────

    @app.get("/shardmap")
    async def get_shard_map(request: Request) -> ShardMap:
        service = get_service(request)
        return service.shard_map

    @app.post("/register", dependencies=[Depends(cluster_auth)])
    async def register(
        body: NodeRegistration,
        request: Request,
    ) -> RegistrationResponse:
        service = get_service(request)
        try:
            return service.register_node(body)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.post("/heartbeat", dependencies=[Depends(cluster_auth)])
    async def heartbeat(
        body: Heartbeat,
        request: Request,
    ) -> LeaseGrant | None:
        service = get_service(request)
        try:
            return service.handle_heartbeat(body)
        except DocstoreError as exc:
            raise exc.to_http_exception() from exc

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    # ── Admin Endpoints ───────────────────────────────────────────────────────

    @app.get("/admin/status", dependencies=[Depends(admin_auth)])
    async def admin_status(request: Request) -> dict[str, Any]:
        service = get_service(request)
        return service.get_cluster_status()

    @app.post("/admin/failover/{shard_id}", dependencies=[Depends(admin_auth)])
    async def admin_failover(shard_id: str, request: Request) -> dict[str, Any]:
        """Manually trigger failover for a shard."""
        service = get_service(request)
        promoted = await service.trigger_failover(shard_id)
        if not promoted:
            raise HTTPException(
                status_code=400,
                detail={
                    "error_code": "FAILOVER_FAILED",
                    "error_message": f"Failover failed or blocked for shard {shard_id}.",
                },
            )
        return {"status": "ok", "shard_id": shard_id, "promoted": True}

    @app.post("/admin/migrate", dependencies=[Depends(admin_auth)])
    async def admin_migrate() -> dict[str, str]:
        """Placeholder for Module 9."""
        return {"status": "stub", "message": "Migration not yet implemented"}

    # ── Error handler (no stack traces) ───────────────────────────────────────

    @app.exception_handler(HTTPException)
    async def _http_exc_handler(request: Request, exc: HTTPException) -> JSONResponse:
        if isinstance(exc.detail, dict):
            detail = exc.detail
        else:
            detail = {"error_code": "ERROR", "error_message": str(exc.detail)}
        return JSONResponse(status_code=exc.status_code, content=detail)

    return app
