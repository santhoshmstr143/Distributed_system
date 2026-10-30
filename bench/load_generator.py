"""High-throughput asynchronous load generator for Sharded-Docstore benchmarking.

Evaluates throughput (ops/sec), latency percentiles (p50, p90, p95, p99),
error rates, and timeout rates under configurable read/write ratios.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from docstore.client.client import AsyncDocStoreClient


@dataclass
class LoadConfig:
    """Configuration for benchmark load generation."""

    total_ops: int = 2000
    concurrency: int = 10
    read_ratio: float = 0.7  # 70% reads, 30% writes
    seed: int = 42
    warmup_ops: int = 200
    key_min: int = 0
    key_max: int = 100000
    value_size_bytes: int = 128


@dataclass
class LoadResult:
    """Summary of benchmark run metrics."""

    total_ops: int
    duration_s: float
    throughput_ops_sec: float
    read_ops: int
    write_ops: int
    error_ops: int
    timeout_ops: int
    latency_p50_ms: float
    latency_p90_ms: float
    latency_p95_ms: float
    latency_p99_ms: float
    latency_min_ms: float
    latency_max_ms: float
    latency_mean_ms: float
    latencies_ms: list[float] = field(default_factory=list)

    def to_dict(self, include_raw_latencies: bool = False) -> dict[str, Any]:
        data = asdict(self)
        if not include_raw_latencies:
            data.pop("latencies_ms", None)
        return data


def compute_percentiles(values: list[float]) -> dict[str, float]:
    """Compute min, max, mean, p50, p90, p95, p99 without external dependencies."""
    if not values:
        return {
            "min": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "p50": 0.0,
            "p90": 0.0,
            "p95": 0.0,
            "p99": 0.0,
        }
    sorted_vals = sorted(values)
    n = len(sorted_vals)

    def p(pct: float) -> float:
        idx = round((pct / 100.0) * (n - 1))
        idx = max(0, min(n - 1, idx))
        return sorted_vals[idx]

    return {
        "min": round(sorted_vals[0], 3),
        "max": round(sorted_vals[-1], 3),
        "mean": round(sum(sorted_vals) / n, 3),
        "p50": round(p(50.0), 3),
        "p90": round(p(90.0), 3),
        "p95": round(p(95.0), 3),
        "p99": round(p(99.0), 3),
    }


class LoadGenerator:
    """Orchestrates concurrent read/write load against a Docstore cluster."""

    def __init__(self, client: AsyncDocStoreClient, config: LoadConfig | None = None) -> None:
        self.client = client
        self.config = config or LoadConfig()
        self._rng = random.Random(self.config.seed)
        self._existing_keys: set[int] = set()

    def _generate_payload(self, doc_id: int) -> dict[str, Any]:
        padding = "x" * max(1, self.config.value_size_bytes - 32)
        return {
            "doc_id": doc_id,
            "timestamp": time.time(),
            "payload": padding,
            "counter": doc_id % 100,
        }

    async def seed_data(self, count: int) -> int:
        """Seed the cluster with an initial dataset for read workloads."""
        seeded = 0
        step = max(1, (self.config.key_max - self.config.key_min) // max(count, 1))
        curr = self.config.key_min

        for _ in range(count):
            doc = self._generate_payload(curr)
            try:
                await self.client.insert(doc)
                self._existing_keys.add(curr)
                seeded += 1
            except Exception:
                # May already exist, still record as existing key
                self._existing_keys.add(curr)
            curr += step
            if curr >= self.config.key_max:
                break
        return seeded

    async def run(self) -> LoadResult:
        """Execute warmup and measurement phases, returning aggregated metrics."""
        # 1. Warm-up phase (excluded from reported latency/throughput)
        if self.config.warmup_ops > 0:
            await self._run_phase(self.config.warmup_ops, record_metrics=False)

        # 2. Measurement phase
        latencies, read_ops, write_ops, error_ops, timeout_ops, duration = await self._run_phase(
            self.config.total_ops, record_metrics=True
        )

        pcts = compute_percentiles(latencies)
        throughput = round(len(latencies) / duration, 2) if duration > 0 else 0.0

        return LoadResult(
            total_ops=len(latencies) + error_ops,
            duration_s=round(duration, 4),
            throughput_ops_sec=throughput,
            read_ops=read_ops,
            write_ops=write_ops,
            error_ops=error_ops,
            timeout_ops=timeout_ops,
            latency_p50_ms=pcts["p50"],
            latency_p90_ms=pcts["p90"],
            latency_p95_ms=pcts["p95"],
            latency_p99_ms=pcts["p99"],
            latency_min_ms=pcts["min"],
            latency_max_ms=pcts["max"],
            latency_mean_ms=pcts["mean"],
            latencies_ms=latencies,
        )

    async def _run_phase(
        self, num_ops: int, record_metrics: bool
    ) -> tuple[list[float], int, int, int, int, float]:
        queue: asyncio.Queue[tuple[bool, int]] = asyncio.Queue()

        # Pre-generate planned operations deterministically
        for _ in range(num_ops):
            is_read = self._rng.random() < self.config.read_ratio
            if is_read and self._existing_keys:
                key = self._rng.choice(list(self._existing_keys))
            else:
                key = self._rng.randint(self.config.key_min, self.config.key_max)
            queue.put_nowait((is_read, key))

        latencies: list[float] = []
        read_ops = 0
        write_ops = 0
        error_ops = 0
        timeout_ops = 0

        lock = asyncio.Lock()
        start_time = time.perf_counter()

        async def worker() -> None:
            nonlocal read_ops, write_ops, error_ops, timeout_ops
            while not queue.empty():
                try:
                    is_read, key = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                t0 = time.perf_counter()
                try:
                    if is_read:
                        await self.client.get(key)
                        elapsed_ms = (time.perf_counter() - t0) * 1000.0
                        if record_metrics:
                            async with lock:
                                read_ops += 1
                                latencies.append(elapsed_ms)
                    else:
                        doc = self._generate_payload(key)
                        try:
                            await self.client.insert(doc)
                        except Exception:
                            # Update if already exists
                            await self.client.update(key, doc)
                        elapsed_ms = (time.perf_counter() - t0) * 1000.0
                        if record_metrics:
                            async with lock:
                                write_ops += 1
                                self._existing_keys.add(key)
                                latencies.append(elapsed_ms)
                except TimeoutError:
                    if record_metrics:
                        async with lock:
                            timeout_ops += 1
                            error_ops += 1
                except Exception:
                    if record_metrics:
                        async with lock:
                            error_ops += 1
                finally:
                    queue.task_done()

        num_workers = min(self.config.concurrency, num_ops)
        workers = [asyncio.create_task(worker()) for _ in range(num_workers)]
        await asyncio.gather(*workers)
        duration = time.perf_counter() - start_time

        return latencies, read_ops, write_ops, error_ops, timeout_ops, duration


async def main() -> None:
    parser = argparse.ArgumentParser(description="DocStore Load Generator")
    parser.add_argument("--router-url", default=os.environ.get("ROUTER_URL", "http://127.0.0.1:8000"))
    parser.add_argument("--api-key", default=os.environ.get("CLIENT_API_KEY", "c" * 16))
    parser.add_argument("--ops", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--read-ratio", type=float, default=0.7)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    client = AsyncDocStoreClient(router_url=args.router_url, api_key=args.api_key)
    cfg = LoadConfig(
        total_ops=args.ops,
        concurrency=args.concurrency,
        read_ratio=args.read_ratio,
        seed=args.seed,
    )
    generator = LoadGenerator(client, cfg)
    print(f"Seeding 200 documents with seed={args.seed}...")
    await generator.seed_data(200)

    pct_read = int(args.read_ratio * 100)
    print(
        f"Running load generator ({args.ops} ops, {args.concurrency} workers, {pct_read}% reads)..."
    )
    res = await generator.run()
    print("--- Benchmark Results ---")
    print(f"Throughput: {res.throughput_ops_sec:.2f} ops/sec")
    print(f"Latency p50: {res.latency_p50_ms:.2f} ms")
    print(f"Latency p95: {res.latency_p95_ms:.2f} ms")
    print(f"Latency p99: {res.latency_p99_ms:.2f} ms")
    print(f"Errors: {res.error_ops} (Timeouts: {res.timeout_ops})")


if __name__ == "__main__":
    asyncio.run(main())
