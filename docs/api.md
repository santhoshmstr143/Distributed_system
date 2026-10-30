# API Reference — Sharded-Docstore

This document provides the complete REST API specification for Sharded-Docstore across the **Router**, **Shard Nodes**, and **Coordinator**.

---

## 1. Authentication & Headers

| Header | Description | Target Component | Required On |
|---|---|---|---|
| `X-Api-Key` | Client authentication key (`CLIENT_API_KEY`) | Router | All client document and query requests |
| `X-Admin-Token` | Cluster administrator token (`ADMIN_TOKEN`) | Coordinator | Admin endpoints (`/admin/*`) |
| `X-Cluster-Signature` | HMAC-SHA256 hex digest of raw request body signed with `CLUSTER_SECRET` | Coordinator, Shard Nodes | Inter-node RPCs (`/register`, `/heartbeat`, `/internal/*`) |

---

## 2. Response Metadata & Error Model

### 2.1 Response Metadata (`meta`)

Every operational response contains a `meta` block:

```json
{
  "served_by": "primary",
  "term": 1,
  "ownership_epoch": 0,
  "applied_seq": 42,
  "replication_lag": null
}
```

- `served_by`: `"primary"` or `"follower"` (or node identifier).
- `term`: Monotonic term for the shard.
- `ownership_epoch`: Current ownership epoch of the target key range.
- `applied_seq`: Sequence number applied to in-memory state.
- `replication_lag`: `null` for primary reads; integer (operations behind primary) for follower reads.

### 2.2 Standard Error Response

Errors return JSON and **never expose stack traces**:

```json
{
  "error_code": "NOT_FOUND",
  "error_message": "Document with doc_id 100 was not found on this shard."
}
```

#### Error Codes Summary

| Code | HTTP Status | Description |
|---|---|---|
| `AUTH_FAILED` | 401 | Missing or invalid API key, admin token, or HMAC signature |
| `NOT_FOUND` | 404 | Document with specified `doc_id` does not exist |
| `OUT_OF_RANGE` | 400 | `doc_id` outside the key range owned by this shard |
| `INVALID_QUERY` | 400 | Query filter exceeds depth, size, or violates grammar |
| `BODY_TOO_LARGE` | 413 | Request body exceeds 2 MB or document exceeds 1 MB |
| `VALIDATION_ERROR` | 422 | Pydantic payload schema validation failure |
| `DUPLICATE_KEY` | 409 | Document with specified `doc_id` already exists |
| `STALE_EPOCH` | 409 | Request ownership epoch does not match node's current epoch |
| `STALE_TERM` | 409 | Request term is behind node's current term |
| `NOT_PRIMARY` | 409 | Write or primary read routed to non-primary replica |
| `LEASE_EXPIRED` | 409 | Primary lease expired; cannot serve writes or fresh reads |
| `OUTCOME_UNKNOWN` | 202 | Write appended to primary WAL, but follower ACK not received in time |
| `RANGE_PAUSED` | 503 | Target key range is currently paused for migration |
| `REPLICATION_UNAVAILABLE`| 503 | Shard has no `SYNCED` follower; write rejected before WAL append |
| `SHARD_UNAVAILABLE` | 503 | Target shard primary is down or unreachable |
| `SERVER_BUSY` | 503 | Router reached maximum concurrent request capacity |
| `ROUTING_FAILED` | 500 | Router exceeded retry limit during map recovery |

---

## 3. Router API (Client Endpoints)

The Router listens by default on port `8080`. All document and query endpoints require `X-Api-Key: test-api-key`.

### 3.1 Insert Document (`POST /doc`)

Inserts a new document. `doc_id` must be an integer in `[0, 2^63)`.

- **Headers:** `X-Api-Key: test-api-key`, `Content-Type: application/json`
- **Request Body (`WriteRequest`):**
  ```json
  {
    "request_id": "c0a80101-0000-0000-0000-000000000001",
    "op": "insert",
    "document": {
      "doc_id": 100,
      "user": "alice",
      "email": "alice@example.com",
      "status": "active"
    }
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "outcome": "COMMITTED",
    "request_id": "c0a80101-0000-0000-0000-000000000001",
    "seq": 1,
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 1,
      "replication_lag": null
    },
    "error_code": null,
    "error_message": null
  }
  ```

### 3.2 Get Document (`GET /doc/{id}`)

Fetches a document by its 64-bit integer ID.

- **Query Parameters:**
  - `read_mode`: `"primary"` (default) or `"follower"`.
- **Curl Example:**
  ```bash
  curl -s -H "X-Api-Key: test-api-key" \
    "http://127.0.0.1:8080/doc/100?read_mode=primary"
  ```
- **Response (200 OK):**
  ```json
  {
    "document": {
      "doc_id": 100,
      "user": "alice",
      "email": "alice@example.com",
      "status": "active"
    },
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 1,
      "replication_lag": null
    }
  }
  ```

### 3.3 Update Document (`PUT /doc/{id}`)

Performs full replacement of an existing document. Missing document returns 404 `NOT_FOUND`.

- **Request Body:**
  ```json
  {
    "request_id": "c0a80101-0000-0000-0000-000000000002",
    "op": "update",
    "document": {
      "doc_id": 100,
      "user": "alice",
      "email": "alice_updated@example.com",
      "status": "verified"
    }
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "outcome": "COMMITTED",
    "request_id": "c0a80101-0000-0000-0000-000000000002",
    "seq": 2,
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 2,
      "replication_lag": null
    }
  }
  ```

### 3.4 Delete Document (`DELETE /doc/{id}`)

Deletes a document. Retrying with the same `request_id` returns the cached outcome idempotently.

- **Request Body:**
  ```json
  {
    "request_id": "c0a80101-0000-0000-0000-000000000003",
    "op": "delete"
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "outcome": "COMMITTED",
    "request_id": "c0a80101-0000-0000-0000-000000000003",
    "seq": 3,
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 3,
      "replication_lag": null
    }
  }
  ```

### 3.5 Query Documents (`POST /query`)

Executes MongoDB-inspired filter queries with scatter-gather parallel execution across shards.

- **Request Body (`QueryFilter`):**
  ```json
  {
    "filter": {
      "status": { "$eq": "active" }
    },
    "sort_field": "user",
    "sort_dir": "asc",
    "skip": 0,
    "limit": 10,
    "read_mode": "primary"
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "documents": [
      { "doc_id": 100, "user": "alice", "status": "active" },
      { "doc_id": 250, "user": "bob", "status": "active" }
    ],
    "total": 2,
    "meta": {
      "served_by": "primary",
      "term": 1,
      "ownership_epoch": 0,
      "applied_seq": 3,
      "replication_lag": null
    }
  }
  ```

### 3.6 Router Health & Status (`GET /health`, `GET /status`)

- `GET /health`: Returns `{"status": "ok"}`.
- `GET /status`: Returns router cache version, active shard connections, and latency metrics.

---

## 4. Shard Node Internal RPCs (`/internal/*`)

Used strictly for inter-replica communication. Protected by `X-Cluster-Signature` (HMAC-SHA256).

### 4.1 Replicate Operation (`POST /internal/replicate`)

Primary sends new oplog entries to follower. Follower fsyncs to its WAL before replying.

- **Request:**
  ```json
  {
    "shard_id": "shard0",
    "term": 1,
    "seq": 42,
    "op": "insert",
    "doc_id": 100,
    "request_id": "c0a80101-0000-0000-0000-000000000001",
    "payload": { "doc_id": 100, "user": "alice" }
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "status": "ok",
    "seq": 42,
    "last_seq": 42,
    "error_message": null
  }
  ```

### 4.2 Catchup Oplog (`POST /internal/catchup`)

Follower requests missing entries after restart or network blip.

- **Request:**
  ```json
  {
    "shard_id": "shard0",
    "after_seq": 40
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "entries": [
      {
        "seq": 41,
        "term": 1,
        "op": "insert",
        "doc_id": 99,
        "request_id": "req-99",
        "payload": { "doc_id": 99, "data": "val" },
        "crc32": 12345678
      },
      {
        "seq": 42,
        "term": 1,
        "op": "insert",
        "doc_id": 100,
        "request_id": "c0a80101-0000-0000-0000-000000000001",
        "payload": { "doc_id": 100, "user": "alice" },
        "crc32": 87654321
      }
    ],
    "needs_snapshot": false,
    "last_seq": 42
  }
  ```

### 4.3 Snapshot Resync (`GET /internal/snapshot`, `POST /internal/snapshot/apply`)

- `GET /internal/snapshot`: Exports full point-in-time state of the primary (documents, term, epoch, applied seq, dedup table).
- `POST /internal/snapshot/apply`: Follower applies snapshot atomically when WAL compaction prevents incremental catchup.

---

## 5. Coordinator API

The coordinator listens by default on port `8200`.

### 5.1 Get Cluster Shard Map (`GET /shardmap`)

Public endpoint returning active cluster topology, primary/follower endpoints, and epochs.

- **Curl:**
  ```bash
  curl -s http://127.0.0.1:8200/shardmap
  ```
- **Response (200 OK):**
  ```json
  {
    "map_version": 1,
    "entries": [
      {
        "shard_id": "shard0",
        "range": { "start": 0, "end": 3074457345618258602 },
        "primary": "127.0.0.1:8000",
        "follower": "127.0.0.1:8100",
        "ownership_epoch": 0,
        "map_version": 1
      },
      {
        "shard_id": "shard1",
        "range": { "start": 3074457345618258602, "end": 6148914691236517204 },
        "primary": "127.0.0.1:8001",
        "follower": "127.0.0.1:8101",
        "ownership_epoch": 0,
        "map_version": 1
      },
      {
        "shard_id": "shard2",
        "range": { "start": 6148914691236517204, "end": 9223372036854775808 },
        "primary": "127.0.0.1:8002",
        "follower": "127.0.0.1:8102",
        "ownership_epoch": 0,
        "map_version": 1
      }
    ]
  }
  ```

### 5.2 Node Registration (`POST /register`)

Nodes register on startup. Protected by HMAC signature.

- **Request:**
  ```json
  {
    "node_id": "shard0-primary",
    "shard_id": "shard0",
    "address": "127.0.0.1:8000",
    "requested_role": "primary"
  }
  ```
- **Response (200 OK):**
  ```json
  {
    "node_id": "shard0-primary",
    "shard_id": "shard0",
    "role": "primary",
    "term": 1,
    "ranges": [
      { "start": 0, "end": 3074457345618258602 }
    ],
    "lease": {
      "node_id": "shard0-primary",
      "shard_id": "shard0",
      "term": 1,
      "granted_at": 100.0,
      "expires_at": 103.0,
      "map_version": 1
    },
    "map_version": 1
  }
  ```

### 5.3 Heartbeat (`POST /heartbeat`)

Primary sends periodic heartbeat every `H=1.0s`. Protected by HMAC signature.

- **Request:**
  ```json
  {
    "node_id": "shard0-primary",
    "shard_id": "shard0",
    "role": "primary",
    "term": 1,
    "last_applied_seq": 42,
    "follower_acked_seq": 42,
    "follower_state": "SYNCED",
    "timestamp_monotonic": 1234.56
  }
  ```
- **Response (200 OK):** Returns renewed `LeaseGrant`.

### 5.4 Coordinator Admin Status (`GET /admin/status`)

Protected by `X-Admin-Token: test-admin-token`.

- **Curl:**
  ```bash
  curl -s -H "X-Admin-Token: test-admin-token" \
    http://127.0.0.1:8200/admin/status
  ```
- **Response (200 OK):**
  ```json
  {
    "map_version": 1,
    "nodes": {
      "shard0-primary": {
        "node_id": "shard0-primary",
        "shard_id": "shard0",
        "address": "127.0.0.1:8000",
        "role": "primary",
        "last_heartbeat": 1234.56
      }
    },
    "leases": {
      "shard0": {
        "node_id": "shard0-primary",
        "shard_id": "shard0",
        "term": 1,
        "granted_at": 100.0,
        "expires_at": 103.0,
        "map_version": 1
      }
    },
    "degraded_shards": []
  }
  ```
