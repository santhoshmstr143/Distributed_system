"""
WAL compaction helper for Sharded-Docstore.

Compaction removes WAL entries that are no longer needed:
  - Already snapshotted (seq <= last_applied_seq)
  - Not needed for follower catch-up (seq <= follower_acked_seq)
  - Not needed for an active migration

Rules from contracts:
  - Never compact entries still needed by the follower catch-up.
  - If follower is too far behind (lagging beyond the retention cap),
    compact anyway — the follower must do a full snapshot resync.
  - Never compact during an active migration window.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from docstore.storage.wal import _fsync_dir

logger = logging.getLogger(__name__)

WAL_RETENTION_CAP_ENTRIES: int = 50_000  # compact beyond this even if follower lags


def compact_wal(
    wal_path: Path,
    *,
    snapshot_seq: int,
    follower_acked_seq: int,
    migration_active: bool = False,
) -> int:
    """Remove WAL entries that are safe to delete; return the number removed.

    Safe to delete = seq <= min(snapshot_seq, follower_acked_seq),
    unless the gap is beyond the retention cap, in which case compact up to
    snapshot_seq and flag the follower for a full resync.

    Args:
        wal_path:           Path to the WAL file.
        snapshot_seq:       ``last_applied_seq`` of the latest snapshot.
        follower_acked_seq: Highest seq the follower has durably acked.
        migration_active:   If True, skip compaction entirely (migration safety).
    """
    if migration_active:
        logger.info("WAL compaction skipped: migration active")
        return 0

    if not wal_path.exists():
        return 0

    # Decide the safe-to-delete threshold
    # Cap at 0 so a follower ahead of the snapshot yields gap=0 (no retention issue).
    gap = max(0, snapshot_seq - follower_acked_seq)
    if gap > WAL_RETENTION_CAP_ENTRIES:
        logger.warning(
            "Follower is %d entries behind (cap=%d); compacting to snapshot_seq=%d "
            "and marking follower for full resync",
            gap,
            WAL_RETENTION_CAP_ENTRIES,
            snapshot_seq,
        )
        cutoff_seq = snapshot_seq
    else:
        cutoff_seq = min(snapshot_seq, follower_acked_seq)

    if cutoff_seq <= 0:
        return 0

    # Read current WAL
    raw = wal_path.read_bytes()
    lines = [line_bytes for line_bytes in raw.splitlines() if line_bytes.strip()]

    keep: list[bytes] = []
    removed = 0
    for line in lines:
        try:
            record = json.loads(line)
            seq = record.get("seq", 0)
        except Exception:
            keep.append(line + b"\n")
            continue
        if seq <= cutoff_seq:
            removed += 1
        else:
            keep.append(line + b"\n")

    if removed == 0:
        return 0

    # Write compacted WAL atomically
    tmp_path = wal_path.with_suffix(".wal.compact.tmp")
    tmp_path.write_bytes(b"".join(keep))
    # Use r+b (read-write) so that write-buffer flush is guaranteed on all
    # platforms, and the `with` block ensures the FD is closed after fsync.
    with open(tmp_path, "r+b") as _f:
        os.fsync(_f.fileno())
    os.replace(tmp_path, wal_path)
    _fsync_dir(wal_path.parent)

    logger.info("WAL compacted: removed %d entries up to seq=%d", removed, cutoff_seq)
    return removed
