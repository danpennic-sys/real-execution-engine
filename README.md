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

- **Verification + ledger**  
  Every material action is verified against expected effects and recorded with a content-addressed receipt.

## Layout

```
src/
  intent.rs          # Intent, RiskLevel, Decision, ExecutionReceipt, RunOutcome
  agent_scheduler.rs # Coordinated scheduling + concurrency + dependencies
  lib.rs             # Re-exports and engine trait surface
```

## Status

Early scaffolding. The scheduler and intent types are the current focus.

## License

MIT (or proprietary — update as needed)
