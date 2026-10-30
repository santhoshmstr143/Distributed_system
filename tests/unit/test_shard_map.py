"""
Unit tests for ShardMapManager — Module 4.1.

Covers:
- Validation: contiguous, non-overlapping, full coverage of [0, 2^63), empty primaries.
- Boundary lookups: doc_id=0, boundary-1, boundary, 2^63-1, invalid keys.
- Range lookups: sub-range, cross-shard, full coverage.
- Atomic persistence and epoch history log.
- Map reload.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from docstore.common.errors import OutOfRange
from docstore.common.models import ShardMap, ShardMapEntry, ShardRange
from docstore.router.shard_map import (
    KEY_SPACE_END,
    KEY_SPACE_START,
    ShardMapManager,
    create_default_3_shard_map,
    validate_shard_map,
)

pytestmark = pytest.mark.unit


# ═══════════════════════════════════════════════════════════
# Validation Tests
# ═══════════════════════════════════════════════════════════


class TestShardMapValidation:
    def test_valid_default_3_shard_map(self) -> None:
        m = create_default_3_shard_map()
        validate_shard_map(m)
        assert len(m.entries) == 3
        assert m.entries[0].range.start == 0
        assert m.entries[-1].range.end == KEY_SPACE_END

    def test_empty_entries_rejected(self) -> None:
        with pytest.raises(ValueError, match="at least one entry"):
            ShardMap(map_version=1, entries=[])

    def test_gap_in_ranges_rejected(self) -> None:
        entries = [
            ShardMapEntry(
                shard_id="s0",
                range=ShardRange(start=0, end=1000),
                primary="127.0.0.1:8200",
                ownership_epoch=1,
                map_version=1,
            ),
            # Gap between 1000 and 2000
            ShardMapEntry(
                shard_id="s1",
                range=ShardRange(start=2000, end=KEY_SPACE_END),
                primary="127.0.0.1:8201",
                ownership_epoch=1,
                map_version=1,
            ),
        ]
        with pytest.raises(ValueError, match="Gap or overlap"):
            ShardMap(map_version=1, entries=entries)

    def test_overlap_in_ranges_rejected(self) -> None:
        entries = [
            ShardMapEntry(
                shard_id="s0",
                range=ShardRange(start=0, end=2000),
                primary="127.0.0.1:8200",
                ownership_epoch=1,
                map_version=1,
            ),
            # Overlap: starts at 1500 < 2000
            ShardMapEntry(
                shard_id="s1",
                range=ShardRange(start=1500, end=KEY_SPACE_END),
                primary="127.0.0.1:8201",
                ownership_epoch=1,
                map_version=1,
            ),
        ]
        with pytest.raises(ValueError, match="Gap or overlap"):
            ShardMap(map_version=1, entries=entries)

    def test_start_not_zero_rejected(self) -> None:
        entries = [
            ShardMapEntry(
                shard_id="s0",
                range=ShardRange(start=10, end=KEY_SPACE_END),
                primary="127.0.0.1:8200",
                ownership_epoch=1,
                map_version=1,
            ),
        ]
        with pytest.raises(ValueError, match="Gap or overlap"):
            ShardMap(map_version=1, entries=entries)

    def test_end_not_keyspace_max_rejected(self) -> None:
        entries = [
            ShardMapEntry(
                shard_id="s0",
                range=ShardRange(start=0, end=KEY_SPACE_END - 1),
                primary="127.0.0.1:8200",
                ownership_epoch=1,
                map_version=1,
            ),
        ]
        with pytest.raises(ValueError, match="does not cover full key space"):
            ShardMap(map_version=1, entries=entries)

    def test_empty_primary_rejected(self) -> None:
        m = create_default_3_shard_map()
        m.entries[1].primary = ""
        with pytest.raises(ValueError, match="empty primary"):
            validate_shard_map(m)


# ═══════════════════════════════════════════════════════════
# Lookup Tests
# ═══════════════════════════════════════════════════════════


class TestShardMapLookups:
    @pytest.fixture
    def mgr(self) -> ShardMapManager:
        m = create_default_3_shard_map()
        mgr = ShardMapManager()
        mgr.set_map(m)
        return mgr

    def test_lookup_boundaries(self, mgr: ShardMapManager) -> None:
        e0 = mgr.shard_map.entries[0]
        e1 = mgr.shard_map.entries[1]
        e2 = mgr.shard_map.entries[2]

        # First key of shard 0
        assert mgr.get_shard_for_doc_id(0).shard_id == "shard0"
        # Last key of shard 0
        assert mgr.get_shard_for_doc_id(e0.range.end - 1).shard_id == "shard0"

        # First key of shard 1
        assert mgr.get_shard_for_doc_id(e1.range.start).shard_id == "shard1"
        # Last key of shard 1
        assert mgr.get_shard_for_doc_id(e1.range.end - 1).shard_id == "shard1"

        # First key of shard 2
        assert mgr.get_shard_for_doc_id(e2.range.start).shard_id == "shard2"
        # Absolute maximum key (2^63 - 1)
        assert mgr.get_shard_for_doc_id(KEY_SPACE_END - 1).shard_id == "shard2"

    def test_lookup_invalid_keys(self, mgr: ShardMapManager) -> None:
        with pytest.raises(OutOfRange):
            mgr.get_shard_for_doc_id(-1)

        with pytest.raises(OutOfRange):
            mgr.get_shard_for_doc_id(KEY_SPACE_END)

        with pytest.raises(OutOfRange):
            mgr.get_shard_for_doc_id(True)  # type: ignore[arg-type]

    def test_range_lookup_single_shard(self, mgr: ShardMapManager) -> None:
        shards = mgr.get_shards_for_range(100, 500)
        assert len(shards) == 1
        assert shards[0].shard_id == "shard0"

    def test_range_lookup_spanning_shards(self, mgr: ShardMapManager) -> None:
        e0 = mgr.shard_map.entries[0]
        # Range spanning e0.range.end - 10 to e0.range.end + 10
        shards = mgr.get_shards_for_range(e0.range.end - 10, e0.range.end + 10)
        assert len(shards) == 2
        assert [s.shard_id for s in shards] == ["shard0", "shard1"]

    def test_range_lookup_full_keyspace(self, mgr: ShardMapManager) -> None:
        shards = mgr.get_shards_for_range(KEY_SPACE_START, KEY_SPACE_END)
        assert len(shards) == 3
        assert [s.shard_id for s in shards] == ["shard0", "shard1", "shard2"]

    def test_range_lookup_invalid_range(self, mgr: ShardMapManager) -> None:
        with pytest.raises(OutOfRange):
            mgr.get_shards_for_range(500, 100)

        with pytest.raises(OutOfRange):
            mgr.get_shards_for_range(100, 100)


# ═══════════════════════════════════════════════════════════
# Persistence and Reload Tests
# ═══════════════════════════════════════════════════════════


class TestShardMapPersistence:
    def test_atomic_save_and_load(self, tmp_path: Path) -> None:
        map_path = tmp_path / "shard_map.json"
        hist_path = tmp_path / "epoch_history.log"

        mgr = ShardMapManager(path=map_path, history_log_path=hist_path)
        m = create_default_3_shard_map(map_version=1)

        mgr.save(m)
        assert map_path.exists()
        assert hist_path.exists()

        # Load fresh in another manager
        mgr2 = ShardMapManager(path=map_path)
        loaded = mgr2.load()
        assert loaded.map_version == 1
        assert len(loaded.entries) == 3

    def test_epoch_history_logged(self, tmp_path: Path) -> None:
        map_path = tmp_path / "shard_map.json"
        hist_path = tmp_path / "history.log"
        mgr = ShardMapManager(path=map_path, history_log_path=hist_path)

        m1 = create_default_3_shard_map(map_version=1)
        mgr.save(m1)

        m2 = create_default_3_shard_map(map_version=2)
        m2.entries[0].ownership_epoch = 2
        mgr.save(m2)

        lines = [line.strip() for line in hist_path.read_text().splitlines() if line.strip()]
        assert len(lines) == 2
        rec1 = json.loads(lines[0])
        rec2 = json.loads(lines[1])
        assert rec1["map_version"] == 1
        assert rec2["map_version"] == 2
        assert rec2["entries"][0]["ownership_epoch"] == 2

    def test_reload_reflects_disk_changes(self, tmp_path: Path) -> None:
        map_path = tmp_path / "shard_map.json"
        mgr = ShardMapManager(path=map_path)

        m1 = create_default_3_shard_map(map_version=1)
        mgr.save(m1)

        # External change written to disk
        m2 = create_default_3_shard_map(map_version=5)
        map_path.write_text(m2.model_dump_json(indent=2), encoding="utf-8")

        # Reload
        reloaded = mgr.reload()
        assert reloaded.map_version == 5
        assert mgr.shard_map.map_version == 5
