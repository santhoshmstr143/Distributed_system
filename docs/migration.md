# Live Range Migration Specification — Sharded-Docstore

This document specifies the protocol for live range migration across shards without downtime.

---

## 1. Migration State Machine

The migration of a key range $[R_{start}, R_{end})$ from Shard A (Source) to Shard B (Destination) follows a deterministic multi-phase state machine:

```
┌─────────┐
│ PLANNED │
└────┬────┘
     │ 1. Snapshot taken at seq S on Source
     ▼
┌───────────────┐
│ SNAPSHOT_COPY │ Source streams snapshot data to Destination
└────┬──────────┘
     │ 2. Destination applies snapshot (invisible to queries)
     ▼
┌─────────┐
│ CATCHUP │ Destination replays oplog entries from S to S_current
└────┬────┘
     │ 3. Lag falls below catchup threshold
     ▼
┌───────┐
│ PAUSE │ Source pauses writes on [R_start, R_end) (503 RANGE_PAUSED)
└────┬──┘
     │ 4. Drain remaining pending ops
     ▼
┌───────────────┐
│ FINAL_CATCHUP │ Destination replicates remaining oplog entries
└────┬──────────┘
     │ 5. Verify byte count and record count
     ▼
┌────────┐
│ VERIFY │ Consistency verification between Source and Destination
└────┬───┘
     │ 6. Commit Point: Coordinator bumps ownership_epoch & map_version
     ▼
┌─────────┐
│ CUTOVER │ Destination begins serving writes & reads for range
└────┬────┘
     │ 7. Asynchronous deletion of range data on Source
     ▼
┌─────────┐
│ CLEANUP │
└────┬────┘
     │ 8. Migration marked complete
     ▼
┌──────┐
│ DONE │
└──────┘
```

---

## 2. Invisibility Invariant & The Commit Point

### 2.1 Invisibility Invariant
> **Invariant:** During `SNAPSHOT_COPY`, `CATCHUP`, and `FINAL_CATCHUP`, all data replicated to Destination is **completely invisible** to client queries on Destination until the `CUTOVER` phase.

This prevents dirty reads, partial reads, or duplicate results across shards while migration is in progress.

### 2.2 The Migration Commit Point
The atomic commit point of a migration occurs when:
1. Destination verifies identical state with Source.
2. The **Coordinator atomically persists the updated shard map**, incrementing:
   - The `ownership_epoch` of the migrated range $[R_{start}, R_{end})$.
   - The cluster `map_version`.
3. If a failure occurs before this atomic update, the migration **aborts safely**, and the Source unpauses the range.

---

## 3. Router Behavior During Migration

1. **Routing during `PAUSE`:** When a client issues a write to the migrating range during the brief `PAUSE` phase, the source returns HTTP 503 with error code `RANGE_PAUSED`.
2. **Router Retry:** The router intercepts `RANGE_PAUSED`, applies bounded exponential backoff, queries the coordinator for a newer `map_version`, and reroutes the request to the new owner once cutover finishes.
