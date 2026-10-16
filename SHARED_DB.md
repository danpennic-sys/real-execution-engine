# Shared SQLite contract (Rust engine \u2194 Python MCP)

## Path (identical precedence)

1. `OPERATORPAGES_DB` (preferred)
2. `SCHEDULE_STORE_PATH`
3. **No relative default** \u2014 fail if unset

Must be **absolute**. Relative paths resolve against each process cwd and silently diverge.

## Connection PRAGMAs (both sides)

| PRAGMA | Value |
|--------|-------|
| `journal_mode` | `WAL` |
| `busy_timeout` | `5000` ms |
| `foreign_keys` | `ON` |
| `user_version` | `1` |

Writes use `BEGIN IMMEDIATE` (or equivalent) to avoid `SQLITE_BUSY` upgrade deadlocks.

## Schema version

`PRAGMA user_version = 1`. Mismatch at open \u2192 hard error on both sides.

## Tables

`schedules`, `generations`, `dependencies`, `consumed` \u2014 identical column names/types.

Timestamps: UTC ISO-8601 strings (Rust chrono RFC3339 / Python `...Z`).

## Snapshot

- Read all tables in **one** transaction.
- `apply_snapshot(replace=true)` is **one** transaction.
- File write: temp \u2192 fsync \u2192 rename.

## Deployment

Same host, local filesystem only (WAL + multi-process). Not for NFS without careful testing.
