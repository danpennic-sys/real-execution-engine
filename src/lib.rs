//! Real Execution Engine
//!
//! Deterministic agents emit Intents. The engine enforces capability,
//! policy, execution, verification, and ledgering.

pub mod intent;
pub mod agent_scheduler;
pub mod schedule_store;

pub use intent::{
    Decision, ExecutionReceipt, Intent, IntentError, RiskLevel, RunOutcome,
};

pub use agent_scheduler::{
    AgentSchedule, AgentScheduler, IntentRunner, ScheduleKind, SchedulerError,
};

pub use schedule_store::{
    DependencyRow, GenerationState, ScheduleStore, StoreError, StoredSchedule,
};
