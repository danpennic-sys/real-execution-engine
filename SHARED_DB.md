# Shared SQLite contract

## Backend (canonical)

| Side | Library |
|------|--------|
| Rust | rusqlite 0.29 (bundled), sync \u2014 use spawn_blocking under tokio |
| Python MCP | stdlib sqlite3 |

## Path

1. `OPERATORPAGES_DB` (preferred)
2. `SCHEDULE_STORE_PATH`
3. No relative default \u2014 must be absolute

## PRAGMAs

WAL, busy_timeout=5000, foreign_keys=ON, user_version=1

## Schema owner

One migrator only (MCP or migrate job). Engine checks user_version.

## Writers

- schedules / dependencies: MCP or engine
- generations / consumed: engine only in production
