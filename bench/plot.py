"""Generate evaluation figures and plots from benchmark results — Module 13.

Reads results from results/*.json and results/raw/*.csv and renders
publication-quality charts to docs/figures/.
"""

from __future__ import annotations

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # Non-interactive backend
import matplotlib.pyplot as plt


def plot_throughput_latency(results_dir: Path, output_dir: Path) -> None:
    json_path = results_dir / "throughput_latency.json"
    if not json_path.exists():
        print(f"Skipping throughput_latency plot: {json_path} not found.")
        return

    with open(json_path) as f:
        data = json.load(f)

    concurrency = [d["concurrency"] for d in data]
    throughput = [d["throughput_ops_sec"] for d in data]
    p50 = [d["latency_p50_ms"] for d in data]
    p95 = [d["latency_p95_ms"] for d in data]
    p99 = [d["latency_p99_ms"] for d in data]

    _fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # Subplot 1: Throughput vs Concurrency
    ax1.plot(concurrency, throughput, marker="o", color="#1f77b4", linewidth=2.5, markersize=8)
    ax1.set_title(
        "Cluster Throughput vs Concurrency (70% Read / 30% Write)",
        fontsize=11,
        fontweight="bold",
    )
    ax1.set_xlabel("Client Concurrency", fontsize=10)
    ax1.set_ylabel("Successful Throughput (ops/sec)", fontsize=10)
    ax1.grid(True, linestyle="--", alpha=0.6)
    for x_val, y_val in zip(concurrency, throughput, strict=False):
        ax1.annotate(
            f"{y_val:.0f}",
            (x_val, y_val),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=8,
        )

    # Subplot 2: Latency percentiles vs Concurrency
    ax2.plot(concurrency, p50, marker="s", label="p50 (Median)", color="#2ca02c", linewidth=2)
    ax2.plot(concurrency, p95, marker="^", label="p95", color="#ff7f0e", linewidth=2)
    ax2.plot(concurrency, p99, marker="d", label="p99", color="#d62728", linewidth=2)
    ax2.set_title("Latency Percentiles vs Concurrency", fontsize=11, fontweight="bold")
    ax2.set_xlabel("Client Concurrency", fontsize=10)
    ax2.set_ylabel("Latency (ms)", fontsize=10)
    ax2.grid(True, linestyle="--", alpha=0.6)
    ax2.legend(loc="upper left")

    plt.tight_layout()
    out_file = output_dir / "throughput_latency.png"
    plt.savefig(out_file, dpi=300)
    plt.close()
    print(f"Generated: {out_file}")


def plot_query_comparison(results_dir: Path, output_dir: Path) -> None:
    json_path = results_dir / "query_comparison.json"
    if not json_path.exists():
        print(f"Skipping query_comparison plot: {json_path} not found.")
        return

    with open(json_path) as f:
        data = json.load(f)

    labels = [d["query_mode"].replace("_", " ").title() for d in data]
    p50 = [d["latency_p50_ms"] for d in data]
    p95 = [d["latency_p95_ms"] for d in data]
    p99 = [d["latency_p99_ms"] for d in data]

    x = range(len(labels))
    width = 0.25

    _fig, ax = plt.subplots(figsize=(11, 5))
    rects1 = ax.bar([i - width for i in x], p50, width, label="p50", color="#2ca02c", alpha=0.9)
    rects2 = ax.bar(list(x), p95, width, label="p95", color="#ff7f0e", alpha=0.9)
    rects3 = ax.bar([i + width for i in x], p99, width, label="p99", color="#d62728", alpha=0.9)

    ax.set_title(
        "Direct Shard-Key Routing vs Scatter-Gather Query Latencies",
        fontsize=12,
        fontweight="bold",
    )
    ax.set_ylabel("Latency (ms)", fontsize=10)
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, fontsize=9)
    ax.grid(True, axis="y", linestyle="--", alpha=0.6)
    ax.legend()

    for rects in (rects1, rects2, rects3):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(
                f"{height:.1f}",
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
            )

    plt.tight_layout()
    out_file = output_dir / "query_comparison.png"
    plt.savefig(out_file, dpi=300)
    plt.close()
    print(f"Generated: {out_file}")


def plot_failover_recovery(results_dir: Path, output_dir: Path) -> None:
    json_path = results_dir / "failover_recovery.json"
    if not json_path.exists():
        print(f"Skipping failover_recovery plot: {json_path} not found.")
        return

    with open(json_path) as f:
        data = json.load(f)

    runs = data.get("runs", [])
    if not runs:
        return

    run_labels = [f"Run {r['run']}" for r in runs]
    failover_ms = [r["failover_time_ms"] for r in runs]
    write_restoration_ms = [r["write_restoration_time_ms"] for r in runs]

    x = range(len(runs))
    width = 0.35

    _fig, ax = plt.subplots(figsize=(10, 5))
    r1 = ax.bar(
        [i - width / 2 for i in x],
        failover_ms,
        width,
        label="Failover Time (Crash → Read)",
        color="#1f77b4",
    )
    r2 = ax.bar(
        [i + width / 2 for i in x],
        write_restoration_ms,
        width,
        label="Write-Restoration Time (Crash → Synced Write)",
        color="#ff7f0e",
    )

    ax.set_title(
        f"Failover Recovery & Write-Restoration Times ({len(runs)} Independent Runs)",
        fontsize=12,
        fontweight="bold",
    )
    ax.set_ylabel("Time (ms)", fontsize=10)
    ax.set_xticks(list(x))
    ax.set_xticklabels(run_labels, fontsize=10)
    ax.grid(True, axis="y", linestyle="--", alpha=0.6)
    ax.legend()

    for rects in (r1, r2):
        for rect in rects:
            height = rect.get_height()
            ax.annotate(
                f"{height:.0f}ms",
                xy=(rect.get_x() + rect.get_width() / 2, height),
                xytext=(0, 3),
                textcoords="offset points",
                ha="center",
                va="bottom",
                fontsize=8,
            )

    plt.tight_layout()
    out_file = output_dir / "failover_recovery.png"
    plt.savefig(out_file, dpi=300)
    plt.close()
    print(f"Generated: {out_file}")


def plot_migration_pause(results_dir: Path, output_dir: Path) -> None:
    json_path = results_dir / "migration_pause.json"
    if not json_path.exists():
        print(f"Skipping migration_pause plot: {json_path} not found.")
        return

    with open(json_path) as f:
        data = json.load(f)

    docs = [d["docs_migrated"] for d in data]
    pause_ms = [d["pause_duration_ms"] for d in data]
    catchup = [d["catchup_ops"] for d in data]

    _fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.8))

    # Subplot 1: Write Pause Duration vs Range Size
    ax1.plot(docs, pause_ms, marker="o", color="#d62728", linewidth=2.2, markersize=8)
    ax1.set_title(
        "Live Migration Write Pause Duration vs Range Size",
        fontsize=11,
        fontweight="bold",
    )
    ax1.set_xlabel("Number of Documents Migrated", fontsize=10)
    ax1.set_ylabel("Write Pause Duration (ms)", fontsize=10)
    ax1.grid(True, linestyle="--", alpha=0.6)
    for x_val, y_val in zip(docs, pause_ms, strict=False):
        ax1.annotate(
            f"{y_val:.1f} ms",
            (x_val, y_val),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=9,
        )

    # Subplot 2: Catch-up Operations vs Range Size
    ax2.plot(docs, catchup, marker="^", color="#9467bd", linewidth=2.2, markersize=8)
    ax2.set_title("Oplog Catch-Up Operations vs Range Size", fontsize=11, fontweight="bold")
    ax2.set_xlabel("Number of Documents Migrated", fontsize=10)
    ax2.set_ylabel("Catch-Up Operations", fontsize=10)
    ax2.grid(True, linestyle="--", alpha=0.6)
    for x_val, y_val in zip(docs, catchup, strict=False):
        ax2.annotate(
            f"{y_val}",
            (x_val, y_val),
            textcoords="offset points",
            xytext=(0, 8),
            ha="center",
            fontsize=9,
        )

    plt.tight_layout()
    out_file = output_dir / "migration_pause.png"
    plt.savefig(out_file, dpi=300)
    plt.close()
    print(f"Generated: {out_file}")


def main() -> None:
    results_dir = Path("results")
    output_dir = Path("docs/figures")
    output_dir.mkdir(parents=True, exist_ok=True)

    print("Generating evaluation figures in docs/figures/...")
    plot_throughput_latency(results_dir, output_dir)
    plot_query_comparison(results_dir, output_dir)
    plot_failover_recovery(results_dir, output_dir)
    plot_migration_pause(results_dir, output_dir)
    print("✅ All plots generated successfully.")


if __name__ == "__main__":
    main()
