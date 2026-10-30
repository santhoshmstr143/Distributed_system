"""Unit tests for benchmark LoadGenerator (bench/load_generator.py)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from bench.load_generator import LoadConfig, LoadGenerator, compute_percentiles


@pytest.mark.asyncio
async def test_load_generator_reads_target_existing_keys() -> None:
    """Prove that read operations target keys that exist in seeded / existing_keys."""
    mock_client = AsyncMock()
    mock_client.get.return_value = {"doc_id": 100, "val": "test"}

    seeded_keys = {100, 200, 300, 400, 500}
    cfg = LoadConfig(
        total_ops=50,
        concurrency=2,
        read_ratio=1.0,  # 100% reads
        warmup_ops=0,
        seed=123,
    )

    gen = LoadGenerator(mock_client, cfg, existing_keys=seeded_keys)
    res = await gen.run()

    assert res.total_ops == 50
    assert res.read_ops == 50
    assert res.write_ops == 0
    assert res.error_ops == 0

    # Verify every call to client.get targeted an ID within seeded_keys
    for call in mock_client.get.call_args_list:
        called_doc_id = call[0][0]
        assert called_doc_id in seeded_keys


@pytest.mark.asyncio
async def test_load_generator_seeding_fails_does_not_add_unverified_keys() -> None:
    """Verify that if insert fails and document is missing, it is NOT added to existing_keys."""
    mock_client = AsyncMock()
    # insert fails and get returns None (or raises)
    mock_client.insert.side_effect = RuntimeError("Storage write failed")
    mock_client.get.return_value = None

    cfg = LoadConfig(key_min=10, key_max=100)
    gen = LoadGenerator(mock_client, cfg)

    seeded = await gen.seed_data(5)
    assert seeded == 0
    assert len(gen._existing_keys) == 0


@pytest.mark.asyncio
async def test_load_generator_error_rate_matches_failed_operations() -> None:
    """Verify that reported error_rate accurately matches the ratio of failed operations."""
    mock_client = AsyncMock()

    # Fail 2 out of 10 reads
    call_count = 0

    async def mock_get(doc_id: int) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        if call_count in (3, 7):
            raise RuntimeError("Injected transient read failure")
        return {"doc_id": doc_id, "data": "ok"}

    mock_client.get.side_effect = mock_get

    cfg = LoadConfig(
        total_ops=10,
        concurrency=1,
        read_ratio=1.0,
        warmup_ops=0,
        seed=42,
    )

    gen = LoadGenerator(mock_client, cfg, existing_keys={1, 2, 3})
    res = await gen.run()

    assert res.total_ops == 10
    assert res.read_ops == 8
    assert res.error_ops == 2
    assert res.error_ops / res.total_ops == 0.2
    assert round(res.error_ops / max(1, res.total_ops), 4) == 0.2


def test_compute_percentiles_accuracy() -> None:
    """Verify percentiles calculation on known distributions."""
    latencies = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    pcts = compute_percentiles(latencies)

    assert pcts["min"] == 10.0
    assert pcts["max"] == 100.0
    assert pcts["mean"] == 55.0
    assert pcts["p50"] == 50.0 or pcts["p50"] == 60.0
