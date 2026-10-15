// agent_scheduler.rs
// Coordinated scheduling for deterministic agents.
//
// Features:
// - Cron / Interval / Once schedules
// - max_concurrent enforcement (waiting + running)
// - Mutual-exclusion groups
// - Generation-based (consume-once) dependencies with cycle detection
// - RAII release on panic
// - No lost wakeups

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

// ---------------------------------------------------------------------------
// Public contract
// ---------------------------------------------------------------------------

#[async_trait]
pub trait IntentRunner: Send + Sync + 'static {
    async fn run(&self, intent: Intent) -> Result<RunOutcome, String>;
}

// ---------------------------------------------------------------------------
// Schedule definition
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("invalid cron expression: {0}")]
    InvalidCron(String),
    #[error("schedule already registered: {0}")]
    AlreadyRegistered(String),
    #[error("dependency cycle detected: {0}")]
    DependencyCycle(String),
    #[error("wait timeout")]
    WaitTimeout,
}

// ---------------------------------------------------------------------------
// Coordination core
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Resource {
    Schedule(String),
    Group(String),
}

struct CoordinationState {
    holders: HashMap<Resource, HashSet<Uuid>>,
    notify: Arc<Notify>,
    completed: HashMap<String, u64>,
    consumed: HashMap<String, HashMap<String, u64>>,
    dependencies: HashMap<String, HashSet<String>>,
    in_flight: HashMap<String, usize>,
    acquired_gens: HashMap<Uuid, HashMap<String, u64>>,
}

struct Coordinator {
    state: Mutex<CoordinationState>,
    next_run_id: AtomicU64,
}

impl Coordinator {
    fn new() -> Self {
        Self {
            state: Mutex::new(CoordinationState {
                holders: HashMap::new(),
                notify: Arc::new(Notify::new()),
                completed: HashMap::new(),
                consumed: HashMap::new(),
                dependencies: HashMap::new(),
                in_flight: HashMap::new(),
                acquired_gens: HashMap::new(),
            }),
            next_run_id: AtomicU64::new(1),
        }
    }

    fn add_dependency(&self, agent: &str, depends_on: &str) -> Result<(), SchedulerError> {
        if agent == depends_on {
            return Err(SchedulerError::DependencyCycle(agent.to_string()));
        }

        let mut s = self.state.lock();

        // Full cycle detection (DFS)
        let mut visited = HashSet::new();
        let mut stack = vec![depends_on.to_string()];
        while let Some(curr) = stack.pop() {
            if curr == agent {
                return Err(SchedulerError::DependencyCycle(format!(
                    "{} → … → {}",
                    agent, depends_on
                )));
            }
            if !visited.insert(curr.clone()) {
                continue;
            }
            if let Some(next) = s.dependencies.get(&curr) {
                stack.extend(next.iter().cloned());
            }
        }

        s.dependencies
            .entry(agent.to_string())
            .or_default()
            .insert(depends_on.to_string());
        Ok(())
    }

    fn begin_wait(&self, schedule_id: &str) -> (Uuid, Arc<Notify>) {
        let run_id = Uuid::from_u128(self.next_run_id.fetch_add(1, Ordering::Relaxed) as u128);
        let mut s = self.state.lock();
        *s.in_flight.entry(schedule_id.to_string()).or_insert(0) += 1;
        let notify = s.notify.clone();
        (run_id, notify)
    }

    fn end_wait(&self, schedule_id: &str) {
        let mut s = self.state.lock();
        if let Some(c) = s.in_flight.get_mut(schedule_id) {
            *c = c.saturating_sub(1);
        }
        s.notify.notify_waiters();
    }

    fn try_acquire(
        &self,
        run_id: Uuid,
        agent_id: &str,
        schedule_id: &str,
        group: Option<&str>,
        max_concurrent: usize,
    ) -> bool {
        let mut s = self.state.lock();

        let in_flight = s.in_flight.get(schedule_id).copied().unwrap_or(0);
        if in_flight > max_concurrent {
            return false;
        }

        // Snapshot generations at acquire time
        let mut gens = HashMap::new();
        if let Some(deps) = s.dependencies.get(agent_id) {
            for dep in deps {
                let completed = s.completed.get(dep).copied().unwrap_or(0);
                let last_consumed = s
                    .consumed
                    .get(agent_id)
                    .and_then(|m| m.get(dep))
                    .copied()
                    .unwrap_or(0);
                if completed <= last_consumed {
                    return false;
                }
                gens.insert(dep.clone(), completed);
            }
        }

        let sched_res = Resource::Schedule(schedule_id.to_string());
        if max_concurrent <= 1 {
            if s.holders
                .get(&sched_res)
                .map(|h| !h.is_empty())
                .unwrap_or(false)
            {
                return false;
            }
        }
        if let Some(g) = group {
            let group_res = Resource::Group(g.to_string());
            if s.holders
                .get(&group_res)
                .map(|h| !h.is_empty())
                .unwrap_or(false)
            {
                return false;
            }
        }

        s.holders.entry(sched_res).or_default().insert(run_id);
        if let Some(g) = group {
            s.holders
                .entry(Resource::Group(g.to_string()))
                .or_default()
                .insert(run_id);
        }
        s.acquired_gens.insert(run_id, gens);
        true
    }

    fn release(
        &self,
        run_id: Uuid,
        agent_id: &str,
        schedule_id: &str,
        group: Option<&str>,
        success: bool,
    ) {
        let mut s = self.state.lock();

        if let Some(set) = s.holders.get_mut(&Resource::Schedule(schedule_id.to_string())) {
            set.remove(&run_id);
        }
        if let Some(g) = group {
            if let Some(set) = s.holders.get_mut(&Resource::Group(g.to_string())) {
                set.remove(&run_id);
            }
        }

        if success {
            if let Some(gens) = s.acquired_gens.remove(&run_id) {
                for (dep, gen) in gens {
                    s.consumed
                        .entry(agent_id.to_string())
                        .or_default()
                        .insert(dep, gen);
                }
            }
            *s.completed.entry(agent_id.to_string()).or_insert(0) += 1;
        } else {
            s.acquired_gens.remove(&run_id);
        }

        s.notify.notify_waiters();
    }
}

// ---------------------------------------------------------------------------
// RAII permit
// ---------------------------------------------------------------------------

struct RunPermit {
    coordinator: Arc<Coordinator>,
    run_id: Uuid,
    agent_id: String,
    schedule_id: String,
    group: Option<String>,
    success: bool,
}

impl Drop for RunPermit {
    fn drop(&mut self) {
        self.coordinator.release(
            self.run_id,
            &self.agent_id,
            &self.schedule_id,
            self.group.as_deref(),
            self.success,
        );
    }
}

// ---------------------------------------------------------------------------
// Scheduler
// ---------------------------------------------------------------------------

pub struct AgentScheduler {
    runner: Arc<dyn IntentRunner>,
    coordinator: Arc<Coordinator>,
    schedules: Mutex<HashMap<String, AgentSchedule>>,
}

impl AgentScheduler {
    pub fn new(runner: Arc<dyn IntentRunner>) -> Arc<Self> {
        Arc::new(Self {
            runner,
            coordinator: Arc::new(Coordinator::new()),
            schedules: Mutex::new(HashMap::new()),
        })
    }

    pub fn register(
        self: &Arc<Self>,
        schedule_id: impl Into<String>,
        schedule: AgentSchedule,
    ) -> Result<(), SchedulerError> {
        let id = schedule_id.into();
        if let ScheduleKind::Cron(ref expr) = schedule.kind {
            CronSchedule::from_str(expr)
                .map_err(|e| SchedulerError::InvalidCron(e.to_string()))?;
        }
        let mut map = self.schedules.lock();
        if map.contains_key(&id) {
            return Err(SchedulerError::AlreadyRegistered(id));
        }
        map.insert(id, schedule);
        Ok(())
    }

    pub fn add_dependency(
        self: &Arc<Self>,
        agent: &str,
        depends_on: &str,
    ) -> Result<(), SchedulerError> {
        self.coordinator.add_dependency(agent, depends_on)
    }

    pub fn start(self: &Arc<Self>) {
        let snapshot: Vec<(String, AgentSchedule)> = {
            let map = self.schedules.lock();
            map.iter()
                .filter(|(_, s)| s.enabled)
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect()
        };

        for (id, sched) in snapshot {
            let this = Arc::clone(self);
            tokio::spawn(async move {
                this.run_loop(id, sched).await;
            });
        }
    }

    async fn run_loop(self: Arc<Self>, schedule_id: String, sched: AgentSchedule) {
        match &sched.kind {
            ScheduleKind::Interval(dur) => {
                let mut ticker = interval(*dur);
                ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
                ticker.tick().await; // skip immediate first tick
                loop {
                    ticker.tick().await;
                    let this = Arc::clone(&self);
                    let sid = schedule_id.clone();
                    let s = sched.clone();
                    tokio::spawn(async move {
                        if let Err(e) = this.fire(&sid, &s).await {
                            warn!(schedule = %sid, error = %e, "fire failed");
                        }
                    });
                }
            }
            ScheduleKind::Once(delay) => {
                sleep(*delay).await;
                if let Err(e) = self.fire(&schedule_id, &sched).await {
                    warn!(schedule = %schedule_id, error = %e, "fire failed");
                }
            }
            ScheduleKind::Cron(expr) => {
                let cron = CronSchedule::from_str(expr).expect("validated at register");
                loop {
                    let now = Utc::now();
                    if let Some(next) = cron.upcoming(Utc).next() {
                        let wait = (next - now).to_std().unwrap_or(Duration::from_secs(1));
                        sleep(wait).await;
                        let this = Arc::clone(&self);
                        let sid = schedule_id.clone();
                        let s = sched.clone();
                        tokio::spawn(async move {
                            if let Err(e) = this.fire(&sid, &s).await {
                                warn!(schedule = %sid, error = %e, "fire failed");
                            }
                        });
                    } else {
                        break;
                    }
                }
            }
        }
    }

    async fn fire(
        self: Arc<Self>,
        schedule_id: &str,
        sched: &AgentSchedule,
    ) -> Result<(), SchedulerError> {
        {
            let s = self.coordinator.state.lock();
            let in_flight = s.in_flight.get(schedule_id).copied().unwrap_or(0);
            if in_flight >= sched.max_concurrent {
                return Ok(());
            }
        }

        let (run_id, notify) = self.coordinator.begin_wait(schedule_id);

        // Register interest before checking state (no lost wakeup)
        let acquired = timeout(sched.wait_timeout, async {
            loop {
                let notified = notify.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();

                if self.coordinator.try_acquire(
                    run_id,
                    &sched.agent_id,
                    schedule_id,
                    sched.group.as_deref(),
                    sched.max_concurrent,
                ) {
                    return true;
                }
                notified.await;
            }
        })
        .await;

        self.coordinator.end_wait(schedule_id);

        match acquired {
            Ok(true) => {}
            Err(_) => return Err(SchedulerError::WaitTimeout),
            _ => unreachable!(),
        }

        let mut permit = RunPermit {
            coordinator: Arc::clone(&self.coordinator),
            run_id,
            agent_id: sched.agent_id.clone(),
            schedule_id: schedule_id.to_string(),
            group: sched.group.clone(),
            success: false,
        };

        let intent = (sched.intent_factory)();
        match self.runner.run(intent).await {
            Ok(outcome) if outcome.executed && outcome.verification_passed => {
                permit.success = true;
                info!(schedule = %schedule_id, agent = %sched.agent_id, "run succeeded");
            }
            Ok(outcome) => {
                warn!(schedule = %schedule_id, reason = %outcome.reason, "run incomplete");
            }
            Err(e) => {
                error!(schedule = %schedule_id, error = %e, "runner error");
            }
        }

        Ok(())
    }
}
