"""
Coordinator service logic — Module 6.

Responsibilities:
- Owns the persistent shard map and epoch history log
- Two version concepts: global map_version and per-range ownership_epoch
- Bump rules: on failover bump epoch of every range of the affected shard + shard term;
  on migration bump the migrated range's epoch
- Node registration: assigns primary / follower role and initial lease
- Heartbeat tracking with monotonic clock: failure detection (D=5.0s)
- Lease management: granted to primary for L=3.0s, renewed every H=1.0s
- Safety margin: replacement primary cannot write while old lease may be valid (expiry + M=0.5s)
- Shard degraded status: degraded if shard has no SYNCED follower
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from docstore.common.config import Settings
from docstore.common.errors import InvalidQuery, StaleTerm
from docstore.common.logging import get_logger
from docstore.common.models import (
    Heartbeat,
    LeaseGrant,
    NodeRegistration,
    NodeRole,
    RegistrationResponse,
    ReplicaState,
    ShardMap,
    ShardMapEntry,
)
from docstore.router.shard_map import ShardMapManager, create_default_3_shard_map

logger = get_logger(__name__)


@dataclass
class RegisteredNode:
    node_id: str
    shard_id: str
    address: str
    role: NodeRole
    term: int
    last_seen_monotonic: float = field(default_factory=time.monotonic)


@dataclass
class ActiveLease:
    node_id: str
    shard_id: str
    term: int
    granted_at_wall: float
    expires_at_wall: float
    expires_at_monotonic: float

    def remaining(self, now: float) -> float:
        return max(0.0, self.expires_at_monotonic - now)


class CoordinatorService:
    """Core state and business logic of the Coordinator node."""

    def __init__(self, settings: Settings, map_path: Path | str | None = None) -> None:
        self.settings = settings
        target_path = Path(map_path) if map_path else Path(settings.SHARD_MAP_PATH)
        self.map_mgr = ShardMapManager(target_path)

        # Initialize shard map if not present
        if not target_path.exists():
            default_map = create_default_3_shard_map()
            self.map_mgr.save(default_map)
        else:
            self.map_mgr.reload()

        self.nodes: dict[str, RegisteredNode] = {}
        self.leases: dict[str, ActiveLease] = {}  # shard_id -> ActiveLease
        self.shard_terms: dict[str, int] = {}  # shard_id -> current term
        self.replica_states: dict[str, ReplicaState] = {}  # shard_id -> ReplicaState

        # Initialize shard terms
        for entry in self.map_mgr.shard_map.entries:
            self.shard_terms[entry.shard_id] = 1
            self.replica_states[entry.shard_id] = ReplicaState.DOWN

    def is_shard_degraded(self, shard_id: str) -> bool:
        """A shard is degraded if its follower is not in SYNCED state."""
        return self.replica_states.get(shard_id, ReplicaState.DOWN) != ReplicaState.SYNCED

    # ── Shard Map & Epoch Management ──────────────────────────────────────────

    @property
    def shard_map(self) -> ShardMap:
        return self.map_mgr.shard_map

    def bump_shard_failover(self, shard_id: str, new_primary_url: str | None = None) -> int:
        """Bump ownership_epoch of EVERY range of shard_id and bump shard term.

        Returns the new term.
        """
        current_map = self.map_mgr.shard_map
        new_entries = []
        found = False

        for entry in current_map.entries:
            if entry.shard_id == shard_id:
                found = True
                update_dict: dict[str, Any] = {
                    "ownership_epoch": entry.ownership_epoch + 1,
                    "map_version": current_map.map_version + 1,
                }
                if new_primary_url is not None:
                    update_dict["primary"] = new_primary_url
                new_entries.append(entry.model_copy(update=update_dict))
            else:
                new_entries.append(
                    entry.model_copy(update={"map_version": current_map.map_version + 1})
                )

        if not found:
            raise ValueError(f"Unknown shard {shard_id!r}")

        self.shard_terms[shard_id] = self.shard_terms.get(shard_id, 1) + 1
        new_map = ShardMap(map_version=current_map.map_version + 1, entries=new_entries)
        self.map_mgr.save(new_map)

        logger.info(
            "Bumped failover epochs for shard %s: term=%d map_v=%d",
            shard_id,
            self.shard_terms[shard_id],
            new_map.map_version,
        )
        return self.shard_terms[shard_id]

    def bump_range_migration(self, shard_id: str, start: int, end: int) -> None:
        """Bump ownership_epoch of the specific migrated range only."""
        current_map = self.map_mgr.shard_map
        new_entries = []
        found = False

        for entry in current_map.entries:
            if entry.shard_id == shard_id and entry.range.start == start and entry.range.end == end:
                found = True
                new_entries.append(
                    entry.model_copy(
                        update={
                            "ownership_epoch": entry.ownership_epoch + 1,
                            "map_version": current_map.map_version + 1,
                        }
                    )
                )
            else:
                new_entries.append(
                    entry.model_copy(update={"map_version": current_map.map_version + 1})
                )

        if not found:
            raise ValueError(f"Range [{start}, {end}) not found for shard {shard_id!r}")

        new_map = ShardMap(map_version=current_map.map_version + 1, entries=new_entries)
        self.map_mgr.save(new_map)

    # ── Node Registration & Role Assignment ───────────────────────────────────

    def register_node(self, reg: NodeRegistration) -> RegistrationResponse:
        """Register a restarted or newly started node.

        A restarted node must obtain a fresh lease and never resumes from stale state.
        """
        now_mono = time.monotonic()
        now_wall = time.time()
        shard_id = reg.shard_id
        current_map = self.map_mgr.shard_map

        # Find matching shard entry
        entries = [e for e in current_map.entries if e.shard_id == shard_id]
        if not entries:
            raise InvalidQuery(f"Unknown shard_id {shard_id!r}")

        ranges = [e.range for e in entries]
        term = self.shard_terms.get(shard_id, 1)

        # Determine role
        if reg.requested_role:
            role = reg.requested_role
        else:
            # Check addresses in shard map
            primary_addr = entries[0].primary
            follower_addr = entries[0].follower
            if reg.address == primary_addr or not primary_addr:
                role = NodeRole.PRIMARY
            elif reg.address == follower_addr:
                role = NodeRole.FOLLOWER
            else:
                # Default first registered node to primary, second to follower
                existing_for_shard = [
                    n
                    for n in self.nodes.values()
                    if n.shard_id == shard_id and n.node_id != reg.node_id
                ]
                has_primary = any(n.role == NodeRole.PRIMARY for n in existing_for_shard)
                has_follower = any(n.role == NodeRole.FOLLOWER for n in existing_for_shard)
                if has_primary and has_follower:
                    raise ValueError(f"Shard {shard_id!r} already has primary and follower.")
                role = NodeRole.FOLLOWER if has_primary else NodeRole.PRIMARY

        # Update address in shard map if needed
        updated_entries = []
        map_changed = False
        for entry in current_map.entries:
            if entry.shard_id == shard_id:
                if role == NodeRole.PRIMARY and entry.primary != reg.address:
                    entry = entry.model_copy(update={"primary": reg.address})
                    map_changed = True
                elif role == NodeRole.FOLLOWER and entry.follower != reg.address:
                    entry = entry.model_copy(update={"follower": reg.address})
                    map_changed = True
            updated_entries.append(entry)

        if map_changed:
            current_map = ShardMap(
                map_version=current_map.map_version + 1,
                entries=updated_entries,
            )
            self.map_mgr.save(current_map)

        # Record registered node
        self.nodes[reg.node_id] = RegisteredNode(
            node_id=reg.node_id,
            shard_id=shard_id,
            address=reg.address,
            role=role,
            term=term,
            last_seen_monotonic=now_mono,
        )

        lease_grant: LeaseGrant | None = None
        if role == NodeRole.PRIMARY:
            # Check if an old lease from a DIFFERENT primary is still within expiry + M
            existing_lease = self.leases.get(shard_id)
            if existing_lease is not None and existing_lease.node_id != reg.node_id:
                safety_cutoff = existing_lease.expires_at_monotonic + self.settings.SAFETY_MARGIN_S
                if now_mono < safety_cutoff:
                    # Old lease from another node still potentially active:
                    # cannot grant write lease yet
                    logger.warning(
                        "Registration: cannot grant lease to replacement primary %s: "
                        "old lease for %s still active until %.2f (mono=%.2f)",
                        reg.node_id,
                        existing_lease.node_id,
                        safety_cutoff,
                        now_mono,
                    )
                    return RegistrationResponse(
                        node_id=reg.node_id,
                        shard_id=shard_id,
                        role=role,
                        term=term,
                        ranges=ranges,
                        lease=None,
                        map_version=current_map.map_version,
                    )

            # Grant fresh lease
            expires_wall = now_wall + self.settings.LEASE_DURATION_S
            expires_mono = now_mono + self.settings.LEASE_DURATION_S
            self.leases[shard_id] = ActiveLease(
                node_id=reg.node_id,
                shard_id=shard_id,
                term=term,
                granted_at_wall=now_wall,
                expires_at_wall=expires_wall,
                expires_at_monotonic=expires_mono,
            )
            lease_grant = LeaseGrant(
                node_id=reg.node_id,
                shard_id=shard_id,
                term=term,
                granted_at=now_wall,
                expires_at=expires_wall,
                map_version=current_map.map_version,
            )

        logger.info(
            "Registered node %s as %s for shard %s (lease=%s)",
            reg.node_id,
            role.value,
            shard_id,
            lease_grant is not None,
        )

        return RegistrationResponse(
            node_id=reg.node_id,
            shard_id=shard_id,
            role=role,
            term=term,
            ranges=ranges,
            lease=lease_grant,
            map_version=current_map.map_version,
        )

    # ── Heartbeats & Lease Renewal ────────────────────────────────────────────

    def handle_heartbeat(self, hb: Heartbeat) -> LeaseGrant | None:
        """Process heartbeat from a node.

        Primary heartbeats renew the primary's lease and report follower state.
        """
        now_mono = time.monotonic()
        now_wall = time.time()
        shard_id = hb.shard_id
        current_term = self.shard_terms.get(shard_id, 1)

        if hb.term < current_term:
            raise StaleTerm(f"Heartbeat carries stale term {hb.term} < current {current_term}")

        # Update last seen
        if hb.node_id in self.nodes:
            self.nodes[hb.node_id].last_seen_monotonic = now_mono
            self.nodes[hb.node_id].term = hb.term
        else:
            self.nodes[hb.node_id] = RegisteredNode(
                node_id=hb.node_id,
                shard_id=shard_id,
                address="",
                role=hb.role,
                term=hb.term,
                last_seen_monotonic=now_mono,
            )

        if hb.role == NodeRole.PRIMARY:
            # Update follower state as seen by primary
            self.replica_states[shard_id] = hb.follower_state

            # Renew lease
            expires_wall = now_wall + self.settings.LEASE_DURATION_S
            expires_mono = now_mono + self.settings.LEASE_DURATION_S
            self.leases[shard_id] = ActiveLease(
                node_id=hb.node_id,
                shard_id=shard_id,
                term=hb.term,
                granted_at_wall=now_wall,
                expires_at_wall=expires_wall,
                expires_at_monotonic=expires_mono,
            )
            return LeaseGrant(
                node_id=hb.node_id,
                shard_id=shard_id,
                term=hb.term,
                granted_at=now_wall,
                expires_at=expires_wall,
                map_version=self.map_mgr.shard_map.map_version,
            )

        return None

    # ── Status Inspection ─────────────────────────────────────────────────────

    def get_cluster_status(self) -> dict[str, Any]:
        """Return comprehensive cluster and shard health status."""
        now_mono = time.monotonic()
        shards_info: dict[str, Any] = {}

        # Group map entries by shard_id
        entries_by_shard: dict[str, list[ShardMapEntry]] = {}
        for entry in self.map_mgr.shard_map.entries:
            entries_by_shard.setdefault(entry.shard_id, []).append(entry)

        for shard_id, entries in entries_by_shard.items():
            rep_state = self.replica_states.get(shard_id, ReplicaState.DOWN)
            is_degraded = rep_state != ReplicaState.SYNCED
            active_lease = self.leases.get(shard_id)
            lease_remaining = 0.0
            lease_holder = None
            if active_lease and active_lease.expires_at_monotonic > now_mono:
                lease_remaining = round(active_lease.expires_at_monotonic - now_mono, 3)
                lease_holder = active_lease.node_id

            shards_info[shard_id] = {
                "primary": entries[0].primary,
                "follower": entries[0].follower,
                "replica_state": rep_state.value,
                "degraded": is_degraded,
                "term": self.shard_terms.get(shard_id, 1),
                "lease_holder": lease_holder,
                "lease_remaining_s": lease_remaining,
                "ranges": [
                    {"start": e.range.start, "end": e.range.end, "epoch": e.ownership_epoch}
                    for e in entries
                ],
            }

        return {
            "status": "ok",
            "map_version": self.map_mgr.shard_map.map_version,
            "shards": shards_info,
            "nodes": [
                {
                    "node_id": n.node_id,
                    "shard_id": n.shard_id,
                    "address": n.address,
                    "role": n.role.value,
                    "term": n.term,
                    "alive": (now_mono - n.last_seen_monotonic) < self.settings.FAILURE_TIMEOUT_S,
                    "last_seen_seconds_ago": round(now_mono - n.last_seen_monotonic, 2),
                }
                for n in self.nodes.values()
            ],
        }
