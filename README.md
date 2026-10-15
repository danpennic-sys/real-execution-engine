# Real Execution Engine

Deterministic agents that plug into a RealExecutionEngine.

Agents only emit structured **Intents**.  
The engine is the sole gate for capability checks, policy evaluation, execution, verification, and ledgering.

## Core ideas

- **Separation of reasoning and execution**  
  Probabilistic planning stays outside the engine. Once an Intent is accepted, execution is deterministic.

- **Capability grants**  
  Agents can only perform actions they have been explicitly granted.

- **Coordinated schedules**  
  Cron / interval / one-shot schedules with resource locks, mutual-exclusion groups, and generation-based dependencies.

- **Persistent schedule store**  
  SQLite-backed store for schedules, completion generations, and dependencies. Survives restarts.

- **Verification + ledger**  
  Every material action is verified against expected effects and recorded with a content-addressed receipt.

## Layout

```
src/
  intent.rs           # Intent, RiskLevel, Decision, ExecutionReceipt, RunOutcome
  agent_scheduler.rs  # Coordinated scheduling + concurrency + dependencies
  schedule_store.rs   # Durable SQLite persistence for schedules & generations
  lib.rs              # Re-exports
```

## Status

Early scaffolding. Intent types, coordinated scheduler, and persistent store are in place.
Next: full RealExecutionEngine, adapters, and integration of the store into the scheduler.

## License

MIT (or proprietary — update as needed)
