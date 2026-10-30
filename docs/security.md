# Security — Sharded-Docstore

> **Status:** Placeholder. Full threat model is written in Module 12.2.

## Limitations (documented honestly)

- Single coordinator: not fault-tolerant (documented scope)
- No TLS inside the demo Docker network (uses shared-secret HMAC only)
- Shared-secret auth without replay protection (timestamp not included in HMAC)
- Single-host Docker Compose; no cross-machine clock comparison
- See Module 12.2 for the full security pass and mitigations
