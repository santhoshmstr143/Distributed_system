"""Unit tests for benchmarking load generator, statistics, and plot utilities."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from bench.load_generator import LoadResult, compute_percentiles
from bench.plot import (
    plot_failover_recovery,
    plot_migration_pause,
    plot_query_comparison,
    plot_throughput_latency,
)

pytestmark = pytest.mark.unit


def test_compute_percentiles_empty() -> None:
    res = compute_percentiles([])
    assert res["min"] == 0.0
    assert res["p50"] == 0.0
    assert res["p99"] == 0.0


def test_compute_percentiles_known_values() -> None:
    values = [10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0]
    res = compute_percentiles(values)
    assert res["min"] == 10.0
    assert res["max"] == 100.0
    assert res["mean"] == 55.0
    assert res["p50"] == 60.0 or res["p50"] == 50.0
    assert res["p90"] == 90.0
    assert res["p99"] == 100.0


def test_load_result_to_dict() -> None:
    res = LoadResult(
        total_ops=100,
        duration_s=2.5,
        throughput_ops_sec=40.0,
        read_ops=70,
        write_ops=30,
        error_ops=0,
        timeout_ops=0,
        latency_p50_ms=2.1,
        latency_p90_ms=3.4,
        latency_p95_ms=4.0,
        latency_p99_ms=5.2,
        latency_min_ms=1.0,
        latency_max_ms=6.0,
        latency_mean_ms=2.5,
        latencies_ms=[2.1, 3.4],
    )
    d = res.to_dict(include_raw_latencies=False)
    assert d["total_ops"] == 100
    assert "latencies_ms" not in d

    d_raw = res.to_dict(include_raw_latencies=True)
    assert "latencies_ms" in d_raw


def test_plots_generation_mock_data(tmp_path: Path) -> None:
    results_dir = tmp_path / "results"
    output_dir = tmp_path / "figures"
    results_dir.mkdir()
    output_dir.mkdir()

    # 1. throughput_latency.json
    tput_data = [
        {
            "concurrency": 1,
            "throughput_ops_sec": 120.0,
            "latency_p50_ms": 1.2,
            "latency_p95_ms": 2.1,
            "latency_p99_ms": 3.0,
        },
        {
            "concurrency": 10,
            "throughput_ops_sec": 650.0,
            "latency_p50_ms": 2.4,
            "latency_p95_ms": 4.5,
            "latency_p99_ms": 6.8,
        },
    ]
    with open(results_dir / "throughput_latency.json", "w") as f:
        json.dump(tput_data, f)

    # 2. query_comparison.json
    query_data = [
        {
            "query_mode": "direct_primary",
            "latency_p50_ms": 1.1,
            "latency_p95_ms": 1.8,
            "latency_p99_ms": 2.4,
        },
        {
            "query_mode": "scatter_gather_3_shards",
            "latency_p50_ms": 3.2,
            "latency_p95_ms": 5.1,
            "latency_p99_ms": 7.0,
        },
    ]
    with open(results_dir / "query_comparison.json", "w") as f:
        json.dump(query_data, f)

    # 3. failover_recovery.json
    failover_data = {
        "runs": [
            {"run": 1, "failover_time_ms": 650.0, "write_restoration_time_ms": 820.0},
            {"run": 2, "failover_time_ms": 640.0, "write_restoration_time_ms": 810.0},
        ]
    }
    with open(results_dir / "failover_recovery.json", "w") as f:
        json.dump(failover_data, f)

    # 4. migration_pause.json
    mig_data = [
        {"docs_migrated": 500, "pause_duration_ms": 4.5, "catchup_ops": 2},
        {"docs_migrated": 2000, "pause_duration_ms": 6.2, "catchup_ops": 8},
    ]
    with open(results_dir / "migration_pause.json", "w") as f:
        json.dump(mig_data, f)

    plot_throughput_latency(results_dir, output_dir)
    plot_query_comparison(results_dir, output_dir)
    plot_failover_recovery(results_dir, output_dir)
    plot_migration_pause(results_dir, output_dir)

    assert (output_dir / "throughput_latency.png").exists()
    assert (output_dir / "query_comparison.png").exists()
    assert (output_dir / "failover_recovery.png").exists()
    assert (output_dir / "migration_pause.png").exists()
