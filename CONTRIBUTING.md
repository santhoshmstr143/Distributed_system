# Contributing to Sharded-Docstore

Thank you for contributing! Follow these rules strictly to keep the project
consistent and reviewable.

---

## Branch Naming

All work happens on feature branches, never directly on `main`.

| Type | Pattern | Example |
|------|---------|---------|
| Module implementation | `feat/<module>` | `feat/module-0-setup`, `feat/storage` |
| Bug fix | `fix/<topic>` | `fix/wal-torn-tail` |
| Test-only change | `test/<topic>` | `test/replication-fault` |
| Documentation | `docs/<topic>` | `docs/consistency-model` |
| Chore / tooling | `chore/<topic>` | `chore/update-deps` |

## Commit Style (Conventional Commits)

```
<type>(<scope>): <short imperative summary>

[optional body — explain *why*, not *what*]

[optional footer: Closes #issue]
```

| Type | When to use |
|------|-------------|
| `feat` | New feature or module |
| `fix` | Bug fix |
| `test` | Tests only (no production code change) |
| `docs` | Documentation only |
| `chore` | Build, CI, tooling, dependency updates |
| `refactor` | Code restructure without behaviour change |
| `perf` | Performance improvement |

**Scopes** (use the component name):
`common`, `storage`, `shard`, `router`, `coordinator`, `replication`,
`failover`, `migration`, `client`, `deploy`, `bench`, `demo`, `security`

**Examples:**
```
feat(storage): add CRC32-checksummed JSON-lines WAL
fix(replication): handle lost follower ack before client reply
test(failover): add SIGKILL-at-each-crash-point fault tests
chore: bump ruff to 0.17.3
```

## Pull Request Rules

1. Every PR must be reviewed by **at least one teammate who did not author it**.
2. PR title = the commit message you intend to squash-merge with.
3. PRs must pass the full CI gate: lint + typecheck + security + audit + tests.
4. No PR may touch a module that hasn't been approved by the team yet.
5. No `.env`, `data/`, `*.wal`, `*.snap`, or large binary files.

## Module Ownership

| Module(s) | Primary Owner |
|-----------|--------------|
| 0, 1.1, 10, 11, 12, 13, 14, 15, 16 | All |
| 1.2, 2, 3, 8 (rejoin/resync), 11 (lead) | Member 1 |
| 4.1, 6, 7, 8 (lead), 9 (coordinator side) | Member 2 |
| 4.2, 5, 9 (lead), 13 (load generator lead) | Member 3 |

## Code Standards

- **Type hints** on every function signature (`strict` mypy).
- **Docstrings** on every public function and class.
- **No dead code.** List open issues in PRs instead of leaving TODOs.
- **No `eval`, `exec`, `pickle`, `yaml.load`, `shell=True`, `os.system`.**
- Secrets via environment variables only. Never hardcode or log them.
- All outbound HTTP calls must have an explicit timeout.

## Running Quality Gates Locally

```bash
make lint        # ruff lint + format
make typecheck   # mypy
make security    # bandit
make audit       # pip-audit
make test-fast   # unit + integration
make check       # all of the above (run before every PR)
```
