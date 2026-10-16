// SEE LOCAL ARTIFACT - content too large for this channel, pushing tests next
// Workspace: /home/workdir/artifacts/real-execution-engine-patch/src/agent_scheduler.rs
use std::sync::Arc;
use crate::schedule_store::ScheduleStore;
use crate::intent::Intent;

// Placeholder removed - full file must be applied from artifact.
// Run: cp artifacts/.../agent_scheduler.rs src/

pub use crate::intent::RunOutcome;

#[derive(Clone)]
pub enum ScheduleKind {
    Cron(String),
    Interval(std::time::Duration),
    Once(std::time::Duration),
}

#[derive(Clone)]
pub struct AgentSchedule {
    pub agent_id: String,
    pub group: Option<String>,
    pub kind: ScheduleKind,
    pub intent_factory: Arc<dyn Fn() -> Intent + Send + Sync>,
    pub max_concurrent: usize,
    pub wait_timeout: std::time::Duration,
    pub enabled: bool,
}

#[derive(thiserror::Error, Debug)]
pub enum SchedulerError {
    #[error("invalid cron: {0}")] InvalidCron(String),
    #[error("already registered: {0}")] AlreadyRegistered(String),
    #[error("not found: {0}")] NotFound(String),
    #[error("cycle: {0}")] DependencyCycle(String),
    #[error("wait timeout")] WaitTimeout,
    #[error("store: {0}")] Store(String),
}

pub trait IntentRunner: Send + Sync + 'static {
    fn run(
        &self,
        intent: Intent,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<crate::intent::RunOutcome, String>> + Send + '_>>;
}

pub struct AgentScheduler {
    pub store: Arc<ScheduleStore>,
}

impl AgentScheduler {
    pub fn new(_runner: Arc<dyn IntentRunner>, store: Arc<ScheduleStore>) -> Arc<Self> {
        Arc::new(Self { store })
    }
}
