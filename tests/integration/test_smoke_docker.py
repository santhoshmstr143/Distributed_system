"""Smoke test for the Docker Compose cluster or live router deployment."""

from __future__ import annotations

import os

import httpx
import pytest

from docstore.common.auth import CLIENT_KEY_HEADER
from docstore.common.config import get_settings

pytestmark = pytest.mark.integration


def test_docker_cluster_smoke() -> None:
    """Smoke test: insert, get, and query against a live cluster router."""
    settings = get_settings()
    router_url = os.environ.get("ROUTER_URL", "http://127.0.0.1:8000")
    client_key = settings.CLIENT_API_KEY
    headers = {"Content-Type": "application/json", CLIENT_KEY_HEADER: client_key}

    try:
        with httpx.Client(base_url=router_url, timeout=5.0) as client:
            resp = client.get("/health")
            if resp.status_code != 200:
                pytest.skip(
                    f"Live cluster router at {router_url} not healthy (HTTP {resp.status_code})"
                )

            # 1. Insert test document
            req_id = "smoke-test-req-001"
            doc_id = 42000
            insert_payload = {
                "request_id": req_id,
                "document": {
                    "doc_id": doc_id,
                    "title": "Docker Smoke Document",
                    "category": "smoke",
                    "rating": 5,
                },
            }
            ins_res = client.post("/doc", json=insert_payload, headers=headers)
            assert ins_res.status_code in (201, 200), f"Insert failed: {ins_res.text}"

            # 2. Get document
            get_res = client.get(f"/doc/{doc_id}", headers=headers)
            assert get_res.status_code == 200, f"Get failed: {get_res.text}"
            data = get_res.json()
            assert data["document"]["doc_id"] == doc_id
            assert data["document"]["title"] == "Docker Smoke Document"

            # 3. Query document
            q_res = client.post(
                "/query",
                json={"filter": {"category": "smoke"}, "limit": 10},
                headers=headers,
            )
            assert q_res.status_code == 200, f"Query failed: {q_res.text}"
            q_data = q_res.json()
            assert any(d["doc_id"] == doc_id for d in q_data["documents"])

    except httpx.ConnectError:
        pytest.skip(f"Live cluster router not reachable at {router_url} (skip when docker is down)")
