"""
Router scatter-gather query execution — Module 5.3.

Responsibilities:
- Inspect filter for doc_id constraints (exact, $in, range, all)
- Route to minimal required shards (1 shard for exact doc_id; overlapping shards for range/$in)
- Query shards concurrently with local skip=0 and local limit = skip + limit
- Enforce partial-failure policy: whole query fails if any required shard fails or times out
- Filter out documents outside the responding shard's range
- Merge sorted streams using the total order comparator and doc_id tie-breaker
- Deduplicate by doc_id and apply global skip and limit
"""

from __future__ import annotations

import asyncio
import heapq
import json
from typing import Any

import httpx
from fastapi import HTTPException

from docstore.common.auth import CLUSTER_SIG_HEADER, compute_signature
from docstore.common.config import Settings
from docstore.common.errors import OutOfRange
from docstore.common.logging import get_logger
from docstore.common.models import (
    QUERY_LIMIT_MAX,
    QueryFilter,
    QueryResponse,
    ShardMapEntry,
)
from docstore.query.evaluator import is_number
from docstore.query.shard_key import (
    ShardKeyConstraint,
    detect_shard_key_constraint,
)
from docstore.query.sorter import extract_sort_key
from docstore.router.shard_map import ShardMapManager

logger = get_logger(__name__)


def _specialize_in_filter(
    filter_dict: dict[str, Any],
    shard_doc_ids: list[int],
) -> dict[str, Any]:
    """Create a copy of filter_dict with doc_id.$in restricted to shard_doc_ids."""
    new_filter = dict(filter_dict)
    if "doc_id" in new_filter and isinstance(new_filter["doc_id"], dict):
        d = dict(new_filter["doc_id"])
        d["$in"] = shard_doc_ids
        new_filter["doc_id"] = d
        return new_filter

    if "$and" in new_filter and isinstance(new_filter["$and"], list):
        new_and = []
        for sub in new_filter["$and"]:
            if isinstance(sub, dict) and "doc_id" in sub:
                new_and.append(_specialize_in_filter(sub, shard_doc_ids))
            else:
                new_and.append(sub)
        new_filter["$and"] = new_and
        return new_filter

    return new_filter


def resolve_target_shards(
    map_mgr: ShardMapManager,
    filter_dict: dict[str, Any],
) -> list[tuple[ShardMapEntry, dict[str, Any]]]:
    """Determine which shards must be queried and the specialized filter for each.

    Returns
    -------
    list[tuple[ShardMapEntry, dict[str, Any]]]
        List of (shard_entry, filter_for_shard).
    """
    constraint: ShardKeyConstraint = detect_shard_key_constraint(filter_dict)

    if constraint.kind == "exact":
        assert constraint.doc_id is not None
        try:
            entry = map_mgr.get_shard_for_doc_id(constraint.doc_id)
            return [(entry, filter_dict)]
        except OutOfRange:
            # If doc_id is outside the valid keyspace, no shard owns it
            return []

    if constraint.kind == "in":
        assert constraint.doc_ids is not None
        grouped: dict[str, list[int]] = {}
        entry_map: dict[str, ShardMapEntry] = {}

        for did in constraint.doc_ids:
            try:
                entry = map_mgr.get_shard_for_doc_id(did)
                grouped.setdefault(entry.shard_id, []).append(did)
                entry_map[entry.shard_id] = entry
            except OutOfRange:
                continue

        targets = []
        for shard_id, ids in grouped.items():
            entry = entry_map[shard_id]
            specialized = _specialize_in_filter(filter_dict, ids)
            targets.append((entry, specialized))
        return targets

    if constraint.kind == "range":
        assert constraint.range_start is not None and constraint.range_end is not None
        if constraint.range_start >= constraint.range_end:
            return []
        entries = map_mgr.get_shards_for_range(constraint.range_start, constraint.range_end)
        return [(entry, filter_dict) for entry in entries]

    # Non-key filter: scatter to all shards
    return [(entry, filter_dict) for entry in map_mgr.shard_map.entries]


async def _query_single_shard(
    client: httpx.AsyncClient,
    entry: ShardMapEntry,
    payload: dict[str, Any],
    cluster_secret: str,
    timeout_s: float,
) -> list[dict[str, Any]]:
    """Send query to a single shard primary and validate response range."""
    target = entry.primary
    if payload.get("read_mode") == "follower" and entry.follower:
        target = entry.follower

    clean_target = target.removeprefix("http://").removeprefix("https://")
    url = f"http://{clean_target}/query"

    body_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sig = compute_signature(cluster_secret, body_bytes)
    headers = {
        "Content-Type": "application/json",
        CLUSTER_SIG_HEADER: sig,
    }

    try:
        resp = await client.post(url, content=body_bytes, headers=headers, timeout=timeout_s)
    except Exception as exc:
        if target != entry.primary:
            primary = entry.primary.removeprefix("http://").removeprefix("https://")
            resp = await client.post(
                f"http://{primary}/query", content=body_bytes, headers=headers, timeout=timeout_s
            )
        else:
            raise exc
    if resp.status_code != 200:
        try:
            data = resp.json()
            err_code = data.get("detail", {}).get("error_code") or data.get("error_code")
            err_msg = data.get("detail", {}).get("error_message") or data.get("error_message")
        except Exception:
            err_code = "SHARD_ERROR"
            err_msg = resp.text
        raise RuntimeError(
            f"Shard {entry.shard_id} returned HTTP {resp.status_code} ({err_code}): {err_msg}"
        )

    data = resp.json()
    docs = data.get("documents", [])

    # Filter out any document outside the responding shard's range
    valid_docs: list[dict[str, Any]] = []
    for doc in docs:
        if isinstance(doc, dict) and "doc_id" in doc and is_number(doc["doc_id"]):
            did = int(doc["doc_id"])
            if entry.range.start <= did < entry.range.end:
                valid_docs.append(doc)
            else:
                logger.warning(
                    "Dropping out-of-range document %d returned by shard %s [%d, %d)",
                    did,
                    entry.shard_id,
                    entry.range.start,
                    entry.range.end,
                )

    return valid_docs


async def execute_scatter_query(
    query: QueryFilter,
    map_mgr: ShardMapManager,
    http_client: httpx.AsyncClient,
    settings: Settings,
) -> QueryResponse:
    """Execute scatter-gather query with retries and stream merging.

    Raises :class:`fastapi.HTTPException` with 503 SHARD_UNAVAILABLE on partial failure.
    """
    # Candidate depth: local skip=0, local limit = skip + limit
    local_limit = min(query.skip + query.limit, QUERY_LIMIT_MAX)
    timeout_s = min(settings.REPLICATION_TIMEOUT_S + 2.0, 5.0)

    # Allow up to 2 attempts (initial + 1 bounded retry on shard failure)
    last_error: Exception | None = None
    failed_shard_desc: str = "unknown"

    for attempt in range(2):
        targets = resolve_target_shards(map_mgr, query.filter)
        if not targets:
            return QueryResponse(documents=[], total=0)

        tasks = []
        for entry, target_filter in targets:
            shard_payload = {
                "filter": target_filter,
                "sort_field": query.sort_field,
                "sort_dir": query.sort_dir,
                "skip": 0,
                "limit": local_limit,
                "read_mode": query.read_mode.value,
            }
            tasks.append(
                _query_single_shard(
                    http_client,
                    entry,
                    shard_payload,
                    settings.CLUSTER_SECRET,
                    timeout_s,
                )
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        failed_entry: ShardMapEntry | None = None
        shard_lists: list[list[dict[str, Any]]] = []

        for (entry, _), res in zip(targets, results, strict=False):
            if isinstance(res, BaseException):
                failed_entry = entry
                last_error = res if isinstance(res, Exception) else RuntimeError(str(res))
                failed_shard_desc = f"{entry.shard_id} ({entry.primary})"
                break
            shard_lists.append(res)

        if failed_entry is not None:
            if attempt == 0:
                logger.warning(
                    "Scatter-gather query failed on shard %s (%s); reloading map and retrying once",
                    failed_shard_desc,
                    last_error,
                )
                try:
                    map_mgr.reload()
                except Exception as exc:
                    logger.debug("Map reload failed: %s", exc)
                continue
            # Second attempt also failed: partial failure policy
            raise HTTPException(
                status_code=503,
                detail={
                    "error_code": "SHARD_UNAVAILABLE",
                    "error_message": f"Shard {failed_shard_desc} is unavailable: {last_error}",
                },
            )

        # Merge sorted candidate streams using total order comparator
        merged_stream = heapq.merge(
            *shard_lists,
            key=lambda d: extract_sort_key(d, query.sort_field, query.sort_dir),
        )

        # Deduplicate by doc_id
        seen_ids: set[int] = set()
        deduped: list[dict[str, Any]] = []
        for doc in merged_stream:
            did = int(doc["doc_id"])
            if did not in seen_ids:
                seen_ids.add(did)
                deduped.append(doc)

        # Global pagination: apply skip then limit
        paginated = deduped[query.skip : query.skip + query.limit]
        return QueryResponse(documents=paginated, total=len(paginated))

    raise HTTPException(
        status_code=503,
        detail={
            "error_code": "SHARD_UNAVAILABLE",
            "error_message": f"Shard {failed_shard_desc} is unavailable: {last_error}",
        },
    )
