// intent.rs
// Core intent and execution types for deterministic agents.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

/// Hint supplied by the agent about how dangerous the requested action is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskLevel {
    Low,
    Medium,
    High,
    Critical,
}

impl Default for RiskLevel {
    fn default() -> Self {
        RiskLevel::Medium
    }
}

/// Final decision returned by the execution engine after policy evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    Allow,
    Deny,
    RequireApproval,
    Escalate,
}

/// Pure data structure describing what an agent wants to do.
/// Agents never execute side effects; they only emit Intents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Intent {
    pub id: Uuid,
    pub agent_id: String,
    pub goal: String,
    pub action: String,
    pub parameters: serde_json::Value,
    pub expected_effects: Vec<String>,
    pub risk_hint: RiskLevel,
    pub idempotency_key: Option<String>,
    pub metadata: HashMap<String, serde_json::Value>,
    pub created_at: DateTime<Utc>,
}

impl Intent {
    pub fn new(
        agent_id: impl Into<String>,
        goal: impl Into<String>,
        action: impl Into<String>,
        parameters: serde_json::Value,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            agent_id: agent_id.into(),
            goal: goal.into(),
            action: action.into(),
            parameters,
            expected_effects: Vec::new(),
            risk_hint: RiskLevel::Medium,
            idempotency_key: None,
            metadata: HashMap::new(),
            created_at: Utc::now(),
        }
    }

    pub fn with_expected_effects(
        mut self,
        effects: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.expected_effects = effects.into_iter().map(Into::into).collect();
        self
    }

    pub fn with_risk(mut self, risk: RiskLevel) -> Self {
        self.risk_hint = risk;
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Generate a stable idempotency key from agent + action + parameters.
    pub fn with_auto_idempotency_key(mut self) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(self.agent_id.as_bytes());
        hasher.update(b"|");
        hasher.update(self.action.as_bytes());
        hasher.update(b"|");
        hasher.update(self.parameters.to_string().as_bytes());
        let digest = hasher.finalize();
        self.idempotency_key = Some(format!("{:x}", digest)[..32].to_string());
        self
    }
}

/// Detailed receipt produced after processing an Intent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReceipt {
    pub intent_id: Uuid,
    pub decision: Decision,
    pub reason: String,
    pub executed: bool,
    pub result: Option<serde_json::Value>,
    pub error: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub verification_passed: Option<bool>,
    pub ledger_hash: String,
}

impl ExecutionReceipt {
    pub fn duration_ms(&self) -> i64 {
        (self.finished_at - self.started_at).num_milliseconds()
    }

    pub fn is_success(&self) -> bool {
        self.executed
            && self.verification_passed.unwrap_or(false)
            && self.decision == Decision::Allow
    }
}

/// Lightweight outcome used by the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunOutcome {
    pub executed: bool,
    pub verification_passed: bool,
    pub reason: String,
}

impl From<&ExecutionReceipt> for RunOutcome {
    fn from(r: &ExecutionReceipt) -> Self {
        Self {
            executed: r.executed,
            verification_passed: r.verification_passed.unwrap_or(false),
            reason: r.reason.clone(),
        }
    }
}

#[derive(Error, Debug)]
pub enum IntentError {
    #[error("invalid action name: {0}")]
    InvalidAction(String),

    #[error("missing required parameter: {0}")]
    MissingParameter(String),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("idempotency key collision")]
    IdempotencyCollision,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn builder_and_idempotency() {
        let intent = Intent::new("agent-1", "test goal", "echo", json!({"msg": "hi"}))
            .with_expected_effects(["message echoed"])
            .with_risk(RiskLevel::Low)
            .with_auto_idempotency_key();

        assert_eq!(intent.agent_id, "agent-1");
        assert_eq!(intent.action, "echo");
        assert_eq!(intent.risk_hint, RiskLevel::Low);
        assert!(intent.idempotency_key.is_some());
        assert_eq!(intent.expected_effects, vec!["message echoed"]);
    }

    #[test]
    fn receipt_success() {
        let now = Utc::now();
        let receipt = ExecutionReceipt {
            intent_id: Uuid::new_v4(),
            decision: Decision::Allow,
            reason: "ok".into(),
            executed: true,
            result: Some(json!({"echoed": "hi"})),
            error: None,
            started_at: now,
            finished_at: now,
            verification_passed: Some(true),
            ledger_hash: "abc".into(),
        };
        assert!(receipt.is_success());
    }
}
