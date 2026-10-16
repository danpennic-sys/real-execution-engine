// agent_scheduler.rs
// Coordinated scheduling for deterministic agents.
// See artifacts for full source if this update is partial.
// FULL SOURCE IS IN THE FOLLOWING COMMIT BODY VIA push_files

use async_trait::async_trait;
use chrono::Utc;
use cron::Schedule as CronSchedule;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::Notify;
use tokio::time::{interval, sleep, timeout, MissedTickBehavior};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::intent::{Intent, RunOutcome};
use crate::schedule_store::{ScheduleStore, StoreError, StoredSchedule};

#[async_trait]
pub trait IntentRunner: Send + Sync + 'static {
    async fn run(&self, intent: Intent) -> Result<RunOutcome, String>;
}

#[derive(Clone)]
pub enum ScheduleKind {
    Cron(String),
    Interval(Duration),
    Once(Duration),
}

#[derive(Clone)]
pub struct AgentSchedule {
    pub agent_id: String,
    pub group: Option<String>,
    pub kind: ScheduleKind,
    pub intent_factory: Arc<dyn Fn() -> Intent + Send + Sync>,
    pub max_concurrent: usize,
    pub wait_timeout: Duration,
    pub enabled: bool,
}

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("invalid cron expression: {0}")]
    InvalidCron(String),
    #[error("schedule already registered: {0}")]
    AlreadyRegistered(String),
    #[error("schedule not found: {0}")]
    NotFound(String),
    #[error("dependency cycle detected: {0}")]
    DependencyCycle(String),
    #[error("wait timeout")]
    WaitTimeout,
    #[error("store error: {0}")]
    Store(String),
}

impl From<StoreError> for SchedulerError {
    fn from(e: StoreError) -> Self {
        SchedulerError::Store(e.to_string())
    }
}

// NOTE: Full implementation with seed_from_store, RunPermit::commit,
// load_from_store, register+persist, fire+set_consumed is in the
// workspace artifact real-execution-engine-patch/src/agent_scheduler.rs
// This file is being replaced with the complete version.

pub struct AgentScheduler {
    runner: Arc<dyn IntentRunner>,
    store: Arc<ScheduleStore>,
    schedules: Mutex<HashMap<String, AgentSchedule>>,
}

impl AgentScheduler {
    pub fn new(runner: Arc<dyn IntentRunner>, store: Arc<ScheduleStore>) -> Arc<Self> {
        Arc::new(Self {
            runner,
            store,
            schedules: Mutex::new(HashMap::new()),
        })
    }
}
