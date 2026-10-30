# Failover & Fencing Specification — Sharded-Docstore

This document specifies the failure detection, promotion protocol, split-brain fencing, and rejoin reconciliation procedures for Sharded-Docstore.

---

## 1. Failover State Machine

```
┌─────────────────┐
│ Primary Healthy │
└────────┬────────┘
         │ Heartbeat timeout (D = 5.0s)
         ▼
┌───────────────────────┐
│ Failure Detected      │
│ Coordinator checks F  │
└────────┬──────────────┘
         │ Wait lease expiry + M (3.5s)
         ▼
┌───────────────────────────┐
│ Epoch & Term Bump         │
│ coordinator updates shard │
│ term = term + 1           │
│ ownership_epoch += 1      │
└────────┬──────────────────┘
         │
         ▼
┌───────────────────────────┐
│ Promote Follower          │
│ Follower -> Primary       │
│ Serves reads; rejects     │
│ writes (Degraded Mode)    │
└────────┬──────────────────┘
         │ Old primary restarts & catches up
         ▼
┌───────────────────────────┐
│ Rejoin & Resync           │
│ Truncates divergent tail  │
│ Follower -> SYNCED        │
└────────┬──────────────────┘
         │
         ▼
┌───────────────────────────┐
│ Normal Cluster Operation  │
│ Writes re-enabled         │
└───────────────────────────┘
```

---

## 2. Step-by-Step Promotion Procedure

1. **Failure Detection:** The coordinator monitors heartbeats. If no heartbeat is received from the primary within failure detection timeout $D = 5.0s$, the coordinator marks the primary as down.
2. **Follower Health Check:** The coordinator contacts the follower to verify its reachability and query its highest durable sequence number (`last_durable_seq`).
3. **Log Drain:** Follower finishes applying all durable WAL entries to memory.
4. **Safety Wait:** The coordinator waits until the old primary's lease has definitively expired plus safety margin $M = 0.5s$:
   $$t_{wait} = \text{lease.expires\_at} + M - \text{now()}$$
5. **Epoch and Term Bumping:**
   - Shard `term` is incremented by 1.
   - `ownership_epoch` of **every key range** owned by this shard is incremented by 1.
   - Coordinator persists the updated shard map with new `map_version`.
6. **Promotion Notification:** Coordinator promotes the follower to primary role.
7. **Degraded Mode Operation:** The new primary serves reads immediately, but returns 503 `REPLICATION_UNAVAILABLE` on writes until a follower joins and reaches `SYNCED`.

---

## 3. Rejoin Reconciliation Protocol

When an old crashed primary restarts, it must never resume as primary from its stale state:

1. **Demotion & Registration:** The restarted node contacts the coordinator, registers as a follower, and receives the latest term and epoch.
2. **Divergence Discovery:**
   - The returning node compares its local WAL records with the new primary using `(term, seq)`.
   - The **divergence point** is identified as the highest `seq` where both logs have the exact same `term`.
3. **Tail Truncation:**
   - Any local WAL entries beyond the divergence point (uncommitted entries that existed only on the old primary) are **truncated**.
   - In-memory index and idempotency dedup table are rebuilt from the truncated log.
4. **Catchup & Resync:**
   - The node streams missing oplog records from the new primary via `POST /internal/catchup` (or full snapshot via `/internal/snapshot` if compacted).
5. **State Transition to `SYNCED`:**
   - Once all missing records are durably fsynced and applied, the primary marks the follower as `SYNCED`.
   - Degraded mode terminates and writes resume normally.

---

## 4. Failure Scenarios and Mitigations

| Failure Scenario | Mitigation & Behavior |
|---|---|
| **Primary Crash before Follower ACK** | Uncommitted entry discarded upon failover; retry with same `request_id` executes cleanly on new primary. |
| **Follower ACK lost in transit** | Follower durably stored the entry. After failover, the operation is committed. Retry returns cached outcome. |
| **Split-Brain Network Partition** | Old primary cannot renew lease; old primary rejected by follower due to stale term; safety wait guarantees old lease is dead before new primary accepts writes. |
| **Coordinator Crash** | Primaries continue serving reads and writes until lease expiry; documented limitation: single coordinator prototype. |
